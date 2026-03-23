"""
Ozon Placement Cost Service — Real storage costs per SKU from Ozon reports.

API endpoints used:
    - POST /v1/report/placement/by-products/create  (create report)
    - POST /v1/report/info                           (check status, get download URL)
    - GET  <download_url>                            (download Excel file)

Data flow:
    1. create_report(from, to) → report code
    2. wait_for_report(code) → download URL
    3. download_and_parse(url) → list of per-SKU-per-day-per-warehouse rows
    4. OzonPlacementLoader → ClickHouse fact_ozon_placement_cost

Excel structure (Ozon API):
    Row 0: Дата | SKU | Артикул | Категория товара | Описательный тип |
           Склад | Признак товара | Суммарный объем мл | Кол-во экземпляров |
           Платный объем мл | Кол-во платных экземпляров |
           Начисленная стоимость размещения

    Each row = one day × one SKU × one warehouse.
    To get total cost per SKU, SUM(placement_cost) GROUP BY offer_id.

Limits:
    - Max period: 31 days per report
    - Rate limit: 5 reports per day per seller
    - Report generation: typically 10-60 seconds
"""

import asyncio
import io
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

CH_BATCH_SIZE = 2000
REPORT_POLL_INTERVAL = 5  # seconds between status checks
REPORT_POLL_MAX_ATTEMPTS = 60  # max 5 minutes of polling


# ── Service ────────────────────────────────────────────────


class OzonPlacementService:
    """
    Fetch placement cost reports from Ozon Seller API.

    Process:
        1. Request report generation (async on Ozon side)
        2. Poll report status until ready
        3. Download Excel file
        4. Parse into structured rows (per-day × per-SKU × per-warehouse)
    """

    def __init__(self, db, shop_id: int, api_key: str, client_id: str):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key
        self.client_id = client_id

    def _make_client(self):
        return MarketplaceClient(
            db=self.db, shop_id=self.shop_id, marketplace="ozon",
            api_key=self.api_key, client_id=self.client_id,
        )

    async def create_report(
        self, date_from: str, date_to: str,
    ) -> Optional[str]:
        """
        Request placement cost report generation.

        Note: MarketplaceClient.request() already handles 429 with retries
        and backoff, so we don't add our own retry loop here.

        Args:
            date_from: 'YYYY-MM-DD' start date
            date_to: 'YYYY-MM-DD' end date

        Returns:
            Report code (UUID string) or None on failure
        """
        async with self._make_client() as client:
            response = await client.post(
                "/v1/report/placement/by-products/create",
                json={
                    "date_from": date_from,
                    "date_to": date_to,
                },
            )

        if response.status_code == 429:
            logger.error(
                "Rate limit 429 on create_report for shop %s (client_id=%s). "
                "Response body: %s",
                self.shop_id, self.client_id, response.data,
            )
            return None

        if not response.is_success:
            logger.error(
                "Placement report create failed: %s %s",
                response.status_code, response.data,
            )
            return None

        code = response.data.get("result", {}).get("code", "")
        if not code:
            code = response.data.get("code", "")

        if code:
            logger.info(
                "Placement report requested: code=%s, period=%s→%s, shop=%s",
                code, date_from, date_to, self.shop_id,
            )
        else:
            logger.error("No report code in response: %s", response.data)

        return code or None

    async def check_report(self, code: str) -> Tuple[str, Optional[str]]:
        """
        Check report generation status.

        Returns:
            Tuple of (status, download_url).
            Status: 'waiting', 'processing', 'success', 'failed'
        """
        async with self._make_client() as client:
            response = await client.post(
                "/v1/report/info",
                json={"code": code},
            )

        if not response.is_success:
            logger.error(
                "Report info request failed: %s %s",
                response.status_code, response.data,
            )
            return "failed", None

        result = response.data.get("result", {})
        status = result.get("status", "unknown")
        file_url = result.get("file", "")

        return status, file_url if file_url else None

    async def wait_for_report(self, code: str) -> Optional[str]:
        """
        Poll report status until ready, then return download URL.
        """
        for attempt in range(REPORT_POLL_MAX_ATTEMPTS):
            status, url = await self.check_report(code)

            if status == "success" and url:
                logger.info("Report ready: %s (attempt %d)", code, attempt + 1)
                return url

            if status == "failed":
                logger.error("Report generation failed: %s", code)
                return None

            if status in ("waiting", "processing"):
                logger.debug(
                    "Report %s status: %s (attempt %d/%d)",
                    code, status, attempt + 1, REPORT_POLL_MAX_ATTEMPTS,
                )
                await asyncio.sleep(REPORT_POLL_INTERVAL)
            else:
                logger.warning("Unknown report status: %s", status)
                await asyncio.sleep(REPORT_POLL_INTERVAL)

        logger.error("Report poll timeout after %d attempts: %s", REPORT_POLL_MAX_ATTEMPTS, code)
        return None

    async def download_report(self, url: str) -> Optional[bytes]:
        """Download Excel report file by URL."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=60) as http:
                resp = await http.get(url)
                if resp.status_code == 200:
                    logger.info("Downloaded report: %d bytes", len(resp.content))
                    return resp.content
                else:
                    logger.error("Download failed: %s %s", resp.status_code, resp.text[:200])
                    return None
        except Exception as e:
            logger.error("Download error: %s", e)
            return None

    async def fetch_placement_costs(
        self,
        date_from: str,
        date_to: str,
        offer_to_sku: Optional[Dict[str, int]] = None,
    ) -> List[dict]:
        """
        Full pipeline: create report → wait → download → parse.

        Returns:
            List of parsed placement cost rows (per-day × per-SKU × per-warehouse)
        """
        # Step 1: Create report
        code = await self.create_report(date_from, date_to)
        if not code:
            return []

        # Step 2: Wait for report
        url = await self.wait_for_report(code)
        if not url:
            return []

        # Step 3: Download Excel
        excel_bytes = await self.download_report(url)
        if not excel_bytes:
            return []

        # Step 4: Parse
        rows = parse_placement_excel(excel_bytes, offer_to_sku)
        logger.info(
            "Parsed %d placement cost rows (period %s → %s)",
            len(rows), date_from, date_to,
        )
        return rows


# ── Excel Parser ───────────────────────────────────────────


# Fixed column indices based on actual Ozon report structure:
# 0=Дата, 1=SKU, 2=Артикул, 3=Категория товара, 4=Описательный тип,
# 5=Склад, 6=Признак товара, 7=Суммарный объем мл, 8=Кол-во экземпляров,
# 9=Платный объем мл, 10=Кол-во платных экземпляров,
# 11=Начисленная стоимость размещения
COL_DATE = 0
COL_SKU = 1
COL_OFFER_ID = 2
COL_CATEGORY = 3
COL_PRODUCT_TYPE = 4
COL_WAREHOUSE = 5
COL_ITEM_TAG = 6
COL_VOLUME_ML = 7
COL_ITEM_COUNT = 8
COL_PAID_VOLUME_ML = 9
COL_PAID_ITEM_COUNT = 10
COL_COST = 11


def parse_placement_excel(
    data: bytes,
    offer_to_sku: Optional[Dict[str, int]] = None,
) -> List[dict]:
    """
    Parse Ozon placement cost Excel report.

    Each row in the report = one day × one SKU × one warehouse.
    We keep this granularity in ClickHouse and aggregate at query time.

    Args:
        data: Raw Excel file bytes
        offer_to_sku: Optional mapping offer_id → sku (from PostgreSQL)

    Returns:
        List of dicts with parsed placement data (per-day rows)
    """
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    ws = wb.active

    if ws is None:
        logger.error("No active worksheet in Excel file")
        return []

    rows_iter = ws.iter_rows(values_only=True)

    # Skip header row
    header = next(rows_iter, None)
    if header is None:
        wb.close()
        return []

    # Validate header structure
    header_strs = [str(c or "").strip().lower() for c in header]
    if not any("артикул" in h for h in header_strs):
        logger.error("Header does not contain 'Артикул': %s", header_strs)
        wb.close()
        return []

    logger.info("Excel header: %s", [str(c or "")[:30] for c in header])

    # Parse data rows
    result = []
    for row in rows_iter:
        if not row or all(c is None for c in row):
            continue

        # Parse date
        date_val = row[COL_DATE]
        if date_val is None:
            continue

        if isinstance(date_val, datetime):
            dt_str = date_val.strftime("%Y-%m-%d")
        else:
            dt_str = str(date_val).split(" ")[0]  # "2026-03-01 00:00:00" → "2026-03-01"

        offer_id = str(row[COL_OFFER_ID] or "").strip()
        if not offer_id:
            continue

        sku_raw = row[COL_SKU]
        sku = int(sku_raw) if sku_raw else 0

        # If we have a mapping and sku is 0, try to look up from offer_id
        if sku == 0 and offer_to_sku and offer_id in offer_to_sku:
            sku = offer_to_sku[offer_id]

        result.append({
            "dt": dt_str,
            "sku": sku,
            "offer_id": offer_id,
            "category": str(row[COL_CATEGORY] or "").strip(),
            "product_type": str(row[COL_PRODUCT_TYPE] or "").strip(),
            "warehouse_name": str(row[COL_WAREHOUSE] or "").strip(),
            "item_tag": str(row[COL_ITEM_TAG] or "").strip(),
            "volume_ml": _safe_float(row[COL_VOLUME_ML]),
            "item_count": int(_safe_float(row[COL_ITEM_COUNT])),
            "paid_volume_ml": _safe_float(row[COL_PAID_VOLUME_ML]),
            "paid_item_count": int(_safe_float(row[COL_PAID_ITEM_COUNT])),
            "placement_cost": round(_safe_float(row[COL_COST]), 2),
        })

    wb.close()
    logger.info("Parsed %d rows from placement Excel", len(result))
    return result


def _safe_float(val) -> float:
    if val is None or val == "" or val == "-":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ── ClickHouse Loader ──────────────────────────────────────


CH_TABLE = "mms_analytics.fact_ozon_placement_cost"
CH_COLUMNS = [
    "dt", "period_from", "period_to", "shop_id",
    "sku", "offer_id", "product_name", "warehouse_name",
    "product_type", "item_tag",
    "volume_ml", "item_count", "paid_volume_ml", "paid_item_count",
    "placement_cost", "updated_at",
]


class OzonPlacementLoader:
    """Insert parsed placement cost rows into ClickHouse."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_costs(
        self,
        shop_id: int,
        rows: List[dict],
        date_from: str,
        date_to: str,
    ) -> int:
        """
        Insert placement cost rows into ClickHouse.

        Each row = one day × one SKU × one warehouse.

        Args:
            shop_id: Shop identifier
            rows: Parsed placement cost rows (from parse_placement_excel)
            date_from: Period start (YYYY-MM-DD) — the requested period
            date_to: Period end (YYYY-MM-DD)

        Returns:
            Number of rows inserted
        """
        if not rows or not self._client:
            return 0

        now = datetime.utcnow()
        period_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        period_to = datetime.strptime(date_to, "%Y-%m-%d").date()

        ch_rows = []
        for r in rows:
            # Parse row date
            dt = datetime.strptime(r["dt"], "%Y-%m-%d").date()
            ch_rows.append([
                dt,                         # dt (per-row date)
                period_from,                # period_from
                period_to,                  # period_to
                shop_id,                    # shop_id
                r["sku"],                   # sku
                r["offer_id"],              # offer_id
                r["category"],              # product_name (= category)
                r["warehouse_name"],        # warehouse_name
                r["product_type"],          # product_type
                r["item_tag"],              # item_tag
                r["volume_ml"],             # volume_ml
                r["item_count"],            # item_count
                r["paid_volume_ml"],        # paid_volume_ml
                r["paid_item_count"],       # paid_item_count
                Decimal(str(r["placement_cost"])),  # placement_cost
                now,                        # updated_at
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d placement cost rows into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get placement cost stats from ClickHouse (aggregated by offer_id)."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                min(dt) as min_date,
                max(dt) as max_date,
                sum(placement_cost) as total_cost,
                uniq(offer_id) as unique_skus
            FROM fact_ozon_placement_cost FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "min_date": str(r[1]),
                "max_date": str(r[2]),
                "total_cost": float(r[3]),
                "unique_skus": r[4],
            }
        return {}
