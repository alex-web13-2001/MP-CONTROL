"""
WB Sales Funnel Service — load funnel analytics from WB Seller Analytics API.

Endpoints used:
  - POST /api/analytics/v3/sales-funnel/products          — aggregate stats (up to 365 days)
  - POST /api/analytics/v3/sales-funnel/products/history   — daily stats (max 7 days, max 20 nmIds)
  - POST /api/v2/nm-report/downloads                       — async CSV report (any period)

Data is stored in ClickHouse: mms_analytics.fact_sales_funnel (MergeTree, append-only).
Every sync INSERTs new rows with fetched_at timestamp to track how metrics
change throughout the day. Use fact_sales_funnel_latest view for latest values.
"""

import asyncio
import csv
import io
import json
import logging
import os
import uuid
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
HISTORY_MAX_DAYS = 7       # API limit for history endpoint
HISTORY_MAX_NMIDS = 20     # API limit: max nmIds per request
PRODUCTS_MAX_LIMIT = 1000  # Pagination limit for products endpoint
RATE_LIMIT_PAUSE = 21      # seconds between requests (3 req / 60 sec)
CSV_POLL_INTERVAL = 30     # seconds between CSV status checks
CSV_POLL_MAX_ATTEMPTS = 60 # max attempts (~30 min)

TABLE = "mms_analytics.fact_sales_funnel"
COLUMNS = [
    "fetched_at",
    "event_date", "shop_id", "nm_id",
    "open_count", "cart_count",
    "order_count", "order_sum",
    "buyout_count", "buyout_sum",
    "cancel_count", "cancel_sum",
    "add_to_cart_pct", "cart_to_order_pct", "buyout_pct",
    "avg_price", "add_to_wishlist",
]


def _chunks(lst: list, n: int) -> list[list]:
    """Split list into chunks of size n."""
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def _week_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split date range into 7-day windows."""
    windows = []
    cur = start
    while cur <= end:
        window_end = min(cur + timedelta(days=6), end)
        windows.append((cur, window_end))
        cur = window_end + timedelta(days=1)
    return windows


# ── ClickHouse Loader ──────────────────────────────────────
class SalesFunnelLoader:
    """Batch INSERT funnel data into ClickHouse (append-only, no dedup)."""

    BATCH_SIZE = 500

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
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
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

    def get_existing_count(self, shop_id: int, date_from: date, date_to: date) -> int:
        """Check how many rows exist for date range."""
        if not self._client:
            raise RuntimeError("Not connected")
        result = self._client.query(
            """
            SELECT count()
            FROM mms_analytics.fact_sales_funnel
            WHERE shop_id = {shop_id:UInt32}
              AND event_date >= {d1:Date}
              AND event_date <= {d2:Date}
            """,
            parameters={"shop_id": shop_id, "d1": date_from, "d2": date_to},
        )
        return result.first_row[0] if result.first_row else 0

    def insert_rows(self, rows: list[dict]) -> int:
        """INSERT rows into fact_sales_funnel (append-only). Returns count."""
        if not rows or not self._client:
            return 0

        now = datetime.now()
        data = []
        for r in rows:
            data.append([
                now,  # fetched_at — snapshot timestamp
                r["event_date"],
                r["shop_id"],
                r["nm_id"],
                r.get("open_count", 0),
                r.get("cart_count", 0),
                r.get("order_count", 0),
                float(r.get("order_sum", 0)),
                r.get("buyout_count", 0),
                float(r.get("buyout_sum", 0)),
                r.get("cancel_count", 0),
                float(r.get("cancel_sum", 0)),
                float(r.get("add_to_cart_pct", 0)),
                float(r.get("cart_to_order_pct", 0)),
                float(r.get("buyout_pct", 0)),
                float(r.get("avg_price", 0)),
                r.get("add_to_wishlist", 0),
            ])

        total = 0
        for i in range(0, len(data), self.BATCH_SIZE):
            batch = data[i:i + self.BATCH_SIZE]
            self._client.insert(TABLE, batch, column_names=COLUMNS)
            total += len(batch)

        return total


# ── API Service ────────────────────────────────────────────
class WBSalesFunnelService:
    """
    Fetch sales funnel data from WB Seller Analytics API.

    Usage:
        async with WBSalesFunnelService(db, shop_id, api_key) as svc:
            rows = await svc.fetch_history_by_days(nm_ids, start, end)
    """

    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        api_key: str,
    ):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key
        self._client: Optional[MarketplaceClient] = None

    async def __aenter__(self):
        self._client = MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_analytics",
            api_key=self.api_key,
        )
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.__aexit__(*args)

    # ── Daily History (max 7 days, max 20 nmIds) ──────────

    async def fetch_history_by_days(
        self,
        nm_ids: List[int],
        start: date,
        end: date,
        progress_callback=None,
    ) -> List[dict]:
        """
        Fetch daily funnel stats.

        Automatically splits:
          - nm_ids into chunks of 20
          - date range into 7-day windows

        Returns list of dicts ready for ClickHouse insert.
        """
        windows = _week_windows(start, end)
        chunks = _chunks(nm_ids, HISTORY_MAX_NMIDS)
        all_rows = []
        total_requests = len(windows) * len(chunks)
        done = 0

        for w_start, w_end in windows:
            for chunk in chunks:
                try:
                    resp = await self._client.post(
                        "/api/analytics/v3/sales-funnel/products/history",
                        json={
                            "selectedPeriod": {
                                "start": w_start.isoformat(),
                                "end": w_end.isoformat(),
                            },
                            "nmIds": chunk,
                            "skipDeletedNm": False,
                            "aggregationLevel": "day",
                        },
                    )
                    done += 1

                    if resp.is_success and resp.data:
                        items = resp.data if isinstance(resp.data, list) else []
                        for item in items:
                            product = item.get("product", {})
                            nm_id = product.get("nmId", 0)
                            for h in item.get("history", []):
                                all_rows.append(self._map_history_row(nm_id, h))
                    elif resp.is_rate_limited:
                        logger.warning("Rate limited, sleeping 60s")
                        await asyncio.sleep(60)
                    else:
                        logger.error(
                            "History API error: %s (status %s)",
                            resp.error,
                            resp.status_code,
                        )

                    if progress_callback:
                        progress_callback(done, total_requests)

                except Exception as e:
                    logger.error("fetch_history error: %s", e)

                # Respect rate limit: 3 req/min
                await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows

    def _map_history_row(self, nm_id: int, h: dict) -> dict:
        """Map a single history entry to ClickHouse row."""
        event_date_str = h.get("date", "")
        try:
            event_date = datetime.strptime(event_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            event_date = date.today()

        return {
            "event_date": event_date,
            "shop_id": self.shop_id,
            "nm_id": nm_id,
            "open_count": h.get("openCount", 0),
            "cart_count": h.get("cartCount", 0),
            "order_count": h.get("orderCount", 0),
            "order_sum": h.get("orderSum", 0),
            "buyout_count": h.get("buyoutCount", 0),
            "buyout_sum": h.get("buyoutSum", 0),
            "cancel_count": h.get("cancelCount", 0),
            "cancel_sum": h.get("cancelSum", 0),
            "add_to_cart_pct": h.get("addToCartConversion", 0),
            "cart_to_order_pct": h.get("cartToOrderConversion", 0),
            "buyout_pct": h.get("buyoutPercent", 0),
            "avg_price": h.get("avgPrice", 0),
            "add_to_wishlist": h.get("addToWishlistCount", 0),
        }

    # ── Aggregate (up to 365 days) ────────────────────────

    async def fetch_aggregate(
        self,
        start: date,
        end: date,
    ) -> List[dict]:
        """
        Fetch aggregated funnel stats for ALL products.
        Uses pagination (limit/offset).

        Returns raw product items from API.
        """
        all_products = []
        offset = 0
        limit = PRODUCTS_MAX_LIMIT

        while True:
            resp = await self._client.post(
                "/api/analytics/v3/sales-funnel/products",
                json={
                    "selectedPeriod": {
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    },
                    "pastPeriod": {
                        "start": (start - (end - start)).isoformat(),
                        "end": start.isoformat(),
                    },
                    "nmIds": [],
                    "brandNames": [],
                    "subjectIds": [],
                    "tagIds": [],
                    "skipDeletedNm": False,
                    "orderBy": {"field": "openCard", "mode": "desc"},
                    "limit": limit,
                    "offset": offset,
                },
            )

            if resp.is_rate_limited:
                logger.warning("Rate limited on aggregate, sleeping 60s")
                await asyncio.sleep(60)
                continue

            if not resp.is_success or not resp.data:
                break

            data = resp.data
            products = data.get("products", []) if isinstance(data, dict) else []
            if not products:
                break

            all_products.extend(products)
            offset += limit

            if len(products) < limit:
                break

            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_products

    # ── CSV Report (for backfill) ─────────────────────────

    async def create_csv_report(
        self,
        start: date,
        end: date,
        aggregation: str = "day",
    ) -> str:
        """
        Create async CSV report for sales funnel.
        Returns report UUID.
        """
        report_id = str(uuid.uuid4())

        resp = await self._client.post(
            "/api/v2/nm-report/downloads",
            json={
                "id": report_id,
                "reportType": "DETAIL_HISTORY_REPORT",
                "userReportName": f"Funnel {start} - {end}",
                "params": {
                    "nmIDs": [],
                    "subjectIds": [],
                    "brandNames": [],
                    "tagIds": [],
                    "startDate": start.isoformat(),
                    "endDate": end.isoformat(),
                    "timezone": "Europe/Moscow",
                    "aggregationLevel": aggregation,
                    "skipDeletedNm": False,
                },
            },
        )

        if resp.is_success:
            logger.info("CSV report created: %s", report_id)
            return report_id
        else:
            raise RuntimeError(
                f"Failed to create CSV report: {resp.status_code} {resp.error}"
            )

    async def poll_csv_report(self, report_id: str) -> str:
        """
        Poll CSV report status until ready.
        Returns status: 'SUCCESS', 'FAILED', 'TIMEOUT'.
        """
        for attempt in range(CSV_POLL_MAX_ATTEMPTS):
            resp = await self._client.get(
                "/api/v2/nm-report/downloads",
                params={"filter[downloadIds]": report_id},
            )

            if resp.is_success and resp.data:
                data = resp.data
                reports = data.get("data", []) if isinstance(data, dict) else data
                if isinstance(reports, list):
                    for r in reports:
                        if r.get("id") == report_id:
                            status = r.get("status", "PENDING")
                            logger.info(
                                "CSV report %s: %s (attempt %d/%d)",
                                report_id, status, attempt + 1, CSV_POLL_MAX_ATTEMPTS,
                            )
                            if status in ("SUCCESS", "COMPLETED"):
                                return "SUCCESS"
                            elif status == "FAILED":
                                return "FAILED"

            await asyncio.sleep(CSV_POLL_INTERVAL)

        return "TIMEOUT"

    async def download_csv_report(self, report_id: str) -> bytes:
        """Download CSV report ZIP file."""
        resp = await self._client.get(
            f"/api/v2/nm-report/downloads/file/{report_id}",
        )

        if resp.is_success and resp.data:
            # Response is the ZIP file content
            return resp.data
        else:
            raise RuntimeError(
                f"Failed to download CSV report: {resp.status_code} {resp.error}"
            )

    def parse_csv_report(self, zip_data: bytes) -> List[dict]:
        """
        Parse downloaded ZIP with CSV into rows for ClickHouse.
        Returns list of dicts.
        """
        rows = []
        try:
            with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
                for name in zf.namelist():
                    if name.endswith(".csv"):
                        with zf.open(name) as f:
                            reader = csv.DictReader(
                                io.TextIOWrapper(f, encoding="utf-8")
                            )
                            for row in reader:
                                mapped = self._map_csv_row(row)
                                if mapped:
                                    rows.append(mapped)
        except Exception as e:
            logger.error("CSV parse error: %s", e)
        return rows

    def _map_csv_row(self, row: dict) -> Optional[dict]:
        """Map a CSV row to ClickHouse row format.

        Real CSV columns from WB nm-report:
          nmID, dt, openCardCount, addToCartCount, ordersCount, ordersSumRub,
          buyoutsCount, buyoutsSumRub, cancelCount, cancelSumRub,
          addToCartConversion, cartToOrderConversion, buyoutPercent,
          addToWishlist, currency
        """
        try:
            # Date: CSV uses 'dt', History API uses 'date'
            date_str = (
                row.get("dt", "")
                or row.get("date", "")
                or row.get("Дата", "")
            )
            if not date_str:
                return None

            event_date = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()

            # nmId: CSV uses 'nmID', History API uses 'nmId'
            nm_id = int(
                row.get("nmID", 0)
                or row.get("nmId", 0)
                or row.get("nm_id", 0)
                or row.get("Артикул WB", 0)
                or 0
            )
            if not nm_id:
                return None

            return {
                "event_date": event_date,
                "shop_id": self.shop_id,
                "nm_id": nm_id,
                "open_count": int(row.get("openCardCount", 0) or row.get("openCount", 0) or 0),
                "cart_count": int(row.get("addToCartCount", 0) or row.get("cartCount", 0) or 0),
                "order_count": int(row.get("ordersCount", 0) or row.get("orderCount", 0) or 0),
                "order_sum": float(row.get("ordersSumRub", 0) or row.get("orderSum", 0) or 0),
                "buyout_count": int(row.get("buyoutsCount", 0) or row.get("buyoutCount", 0) or 0),
                "buyout_sum": float(row.get("buyoutsSumRub", 0) or row.get("buyoutSum", 0) or 0),
                "cancel_count": int(row.get("cancelCount", 0) or 0),
                "cancel_sum": float(row.get("cancelSumRub", 0) or row.get("cancelSum", 0) or 0),
                "add_to_cart_pct": float(row.get("addToCartConversion", 0) or 0),
                "cart_to_order_pct": float(row.get("cartToOrderConversion", 0) or 0),
                "buyout_pct": float(row.get("buyoutPercent", 0) or 0),
                "avg_price": float(row.get("avgPriceRub", 0) or row.get("avgPrice", 0) or 0),
                "add_to_wishlist": int(row.get("addToWishlist", 0) or row.get("addToWishlistCount", 0) or 0),
            }
        except Exception as e:
            logger.warning("CSV row parse error: %s (row: %s)", e, row)
            return None

    # ── Helper: get nm_ids from dim_products ──────────────

    async def get_product_nm_ids(self) -> List[int]:
        """Get all nm_ids for this shop from dim_products."""
        from sqlalchemy import text
        result = await self.db.execute(
            text("SELECT nm_id FROM dim_products WHERE shop_id = :shop_id"),
            {"shop_id": self.shop_id},
        )
        return [row[0] for row in result.fetchall()]
