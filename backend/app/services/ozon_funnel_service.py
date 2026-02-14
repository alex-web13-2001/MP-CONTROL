"""
Ozon Funnel Service — Sales analytics from Ozon Seller API.

Collects daily per-SKU metrics: ordered_units + revenue.
API: POST /v1/analytics/data

NOTE (2026-02-15): Most /v1/analytics/data metrics are DEPRECATED by Ozon:
  - All hits_view_*, session_*, hits_tocart_*, conv_*, position_category,
    delivered_units, returns, cancellations, adv_view_*, adv_sum_*
  - ONLY ordered_units and revenue still work.
  - If Ozon re-enables funnel metrics, add them back here.

Data flow:
    1. fetch_funnel_data(from, to) → raw sku × day rows
    2. normalize → flat dicts for ClickHouse
    3. OzonFunnelLoader → insert into fact_ozon_funnel
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Dict

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

API_LIMIT = 1000
RATE_LIMIT_PAUSE = 1.0  # aggressive rate limit on this endpoint
CH_BATCH_SIZE = 500

# The only working metrics as of 2026-02-15
WORKING_METRICS = ["ordered_units", "revenue"]


def _safe_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except Exception:
        return 0.0


def _safe_int(val) -> int:
    if val is None or val == "":
        return 0
    try:
        return int(float(val))
    except Exception:
        return 0


def _parse_date(val: str) -> datetime:
    if not val:
        return datetime(1970, 1, 1)
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d")
    except Exception:
        return datetime(1970, 1, 1)


# ── Service ────────────────────────────────────────────────


class OzonFunnelService:
    """
    Fetch sales analytics from Ozon Seller API.

    Uses POST /v1/analytics/data with dimension=[sku, day].
    Handles pagination and rate limiting.
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

    async def fetch_funnel_data(
        self, date_from: str, date_to: str,
    ) -> List[dict]:
        """
        Fetch sales metrics for all SKUs for the given date range.

        Returns list of raw API rows with sku × day × [ordered_units, revenue].
        Max 1000 rows per page, paginates automatically.
        """
        all_rows = []
        offset = 0

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v1/analytics/data",
                    json={
                        "date_from": date_from,
                        "date_to": date_to,
                        "metrics": WORKING_METRICS,
                        "dimension": ["sku", "day"],
                        "filters": [],
                        "sort": [{"key": "revenue", "order": "DESC"}],
                        "limit": API_LIMIT,
                        "offset": offset,
                    },
                )

            if not response.is_success:
                logger.error("Funnel API error: %s %s",
                             response.status_code, response.data)
                break

            data = response.data.get("result", {}).get("data", [])
            if not data:
                break

            all_rows.extend(data)
            logger.info("Funnel page offset=%d: %d rows (total %d)",
                        offset, len(data), len(all_rows))

            if len(data) < API_LIMIT:
                break

            offset += len(data)
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows

    async def fetch_all_funnel(
        self, date_from: str, date_to: str,
    ) -> List[dict]:
        """
        Fetch funnel data, chunking by 90 days for long periods.

        Returns normalized rows ready for ClickHouse.
        """
        dt_from = _parse_date(date_from)
        dt_to = _parse_date(date_to)

        all_raw = []
        chunk_start = dt_from

        while chunk_start < dt_to:
            chunk_end = min(chunk_start + timedelta(days=89), dt_to)
            f = chunk_start.strftime("%Y-%m-%d")
            t = chunk_end.strftime("%Y-%m-%d")

            # Ensure from < to (API requires strict inequality)
            if f >= t:
                chunk_start = chunk_end + timedelta(days=1)
                continue

            logger.info("Funnel chunk: %s → %s", f, t)
            raw = await self.fetch_funnel_data(f, t)
            all_raw.extend(raw)

            chunk_start = chunk_end + timedelta(days=1)
            if chunk_start < dt_to:
                await asyncio.sleep(RATE_LIMIT_PAUSE)

        normalized = _normalize_rows(all_raw)
        logger.info("Funnel total: %d raw → %d normalized", len(all_raw), len(normalized))
        return normalized


def _normalize_rows(raw_rows: List[dict]) -> List[dict]:
    """Convert raw analytics/data rows into flat dicts for ClickHouse."""
    result = []
    for row in raw_rows:
        dims = row.get("dimensions", [])
        metrics = row.get("metrics", [])

        if len(dims) < 2:
            continue

        sku_str = dims[0].get("id", "0")
        sku_name = dims[0].get("name", "")
        day_str = dims[1].get("id", "")

        # Metrics order follows WORKING_METRICS: [ordered_units, revenue]
        m = metrics + [0] * (2 - len(metrics))

        result.append({
            "dt": _parse_date(day_str),
            "sku": _safe_int(sku_str),
            "sku_name": sku_name,
            "ordered_units": _safe_int(m[0]),
            "revenue": Decimal(str(_safe_float(m[1]))),
        })

    return result


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_funnel"
CH_COLUMNS = [
    "dt", "shop_id", "sku", "sku_name",
    "ordered_units", "revenue",
    "updated_at",
]


class OzonFunnelLoader:
    """Insert funnel analytics into ClickHouse."""

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

    def insert_rows(self, shop_id: int, rows: List[dict]) -> int:
        if not rows or not self._client:
            return 0

        now = datetime.utcnow()
        ch_rows = []
        for r in rows:
            ch_rows.append([
                r["dt"], shop_id, r["sku"], r["sku_name"],
                r["ordered_units"], r["revenue"],
                now,
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d funnel rows into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(sku) as unique_skus,
                min(dt) as min_date,
                max(dt) as max_date,
                sum(ordered_units) as total_orders,
                sum(revenue) as total_revenue
            FROM fact_ozon_funnel
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0],
                "unique_skus": row[1],
                "min_date": str(row[2]),
                "max_date": str(row[3]),
                "total_orders": row[4],
                "total_revenue": float(row[5]),
            }
        return {}
