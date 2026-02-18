"""
WB Orders Service — Fetch raw orders from WB Statistics API.

API: GET /api/v1/supplier/orders?dateFrom=...&flag=...
Domain: statistics-api.wildberries.ru (wildberries_stats)

Uses MarketplaceClient for:
    - Proxy rotation (sticky sessions)
    - Rate limiting (Redis-synced)
    - Circuit breaker (auto-disable on auth errors)
    - JA3 fingerprint spoofing
    - Request logging

Pagination (flag=0, default):
    - Returns orders where lastChangeDate >= dateFrom
    - Max ~80,000 rows per response
    - For next page: dateFrom = lastChangeDate of last row
    - Empty array [] = all orders loaded
    - Rate limit: 1 request per minute

Daily snapshot (flag=1):
    - Returns ALL orders for the calendar date in dateFrom
    - Time part is ignored

Data is stored in ClickHouse: mms_analytics.fact_orders_raw (ReplacingMergeTree).
Orders are deduplicated by g_number — if WB updates an order's status
(e.g. cancellation), the newer version replaces the old one.

Usage:
    - sync_orders: every 10 min, flag=0, dateFrom=last synced timestamp
    - backfill_orders: one-time, flag=0, dateFrom=90 days ago (paginated)
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
ENDPOINT = "/api/v1/supplier/orders"
TABLE = "mms_analytics.fact_orders_raw"
COLUMNS = [
    "date", "last_change_date", "shop_id", "nm_id", "g_number", "srid",
    "supplier_article", "barcode", "category", "subject", "brand", "tech_size",
    "warehouse_name", "warehouse_type", "country_name", "oblast_okrug_name", "region_name",
    "total_price", "discount_percent", "spp", "finished_price", "price_with_disc",
    "is_cancel", "cancel_date", "sticker", "income_id", "is_supply", "is_realization",
]
BATCH_SIZE = 500
RATE_LIMIT_PAUSE = 63  # 1 req/min + safety margin

_EPOCH_MIN = datetime(1970, 1, 2)  # ClickHouse DateTime min (epoch > 0)


def _parse_datetime(val: str) -> datetime:
    """Parse WB datetime string to Python datetime.

    WB returns '0001-01-01T00:00:00' for empty dates (e.g. cancelDate).
    ClickHouse DateTime is UInt32 epoch — cannot store dates before 1970.
    """
    if not val or val.startswith("0001") or val.startswith("0000"):
        return _EPOCH_MIN
    try:
        dt = datetime.fromisoformat(val.replace("Z", "+00:00").replace("+00:00", ""))
        return dt if dt >= _EPOCH_MIN else _EPOCH_MIN
    except (ValueError, TypeError):
        return _EPOCH_MIN


def _parse_order_row(item: dict, shop_id: int) -> list:
    """Map an API order item to a ClickHouse row."""
    return [
        _parse_datetime(item.get("date", "")),
        _parse_datetime(item.get("lastChangeDate", "")),
        shop_id,
        int(item.get("nmId", 0) or 0),
        str(item.get("gNumber", "")),
        str(item.get("srid", "")),
        str(item.get("supplierArticle", "")),
        str(item.get("barcode", "")),
        str(item.get("category", "")),
        str(item.get("subject", "")),
        str(item.get("brand", "")),
        str(item.get("techSize", "0")),
        str(item.get("warehouseName", "")),
        str(item.get("warehouseType", "")),
        str(item.get("countryName", "")),
        str(item.get("oblastOkrugName", "")),
        str(item.get("regionName", "")),
        float(item.get("totalPrice", 0) or 0),
        int(item.get("discountPercent", 0) or 0),
        float(item.get("spp", 0) or 0),
        float(item.get("finishedPrice", 0) or 0),
        float(item.get("priceWithDisc", 0) or 0),
        1 if item.get("isCancel") else 0,
        _parse_datetime(item.get("cancelDate", "")),
        str(item.get("sticker", "")),
        int(item.get("incomeID", 0) or 0),
        1 if item.get("isSupply") else 0,
        1 if item.get("isRealization") else 0,
    ]


# ── ClickHouse Loader ──────────────────────────────────────
class OrdersLoader:
    """Batch insert orders into ClickHouse fact_orders_raw."""

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

    def insert_rows(self, rows: List[list]) -> int:
        """Insert rows into fact_orders_raw. Returns count."""
        if not rows or not self._client:
            return 0

        total = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            self._client.insert(TABLE, batch, column_names=COLUMNS)
            total += len(batch)

        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get current stats from fact_orders_raw."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(g_number) as unique_orders,
                uniq(nm_id) as unique_products,
                min(date) as min_date,
                max(date) as max_date,
                sum(price_with_disc) as total_revenue,
                countIf(is_cancel = 1) as cancel_count
            FROM fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_orders": r[1],
                "unique_products": r[2],
                "min_date": str(r[3]),
                "max_date": str(r[4]),
                "total_revenue": float(r[5]),
                "cancel_count": r[6],
            }
        return {}


# ── WB Orders Service (async, via MarketplaceClient) ───────
class WBOrdersService:
    """
    Fetch orders from WB Statistics API via MarketplaceClient.

    Uses proxy rotation, rate limiting, and circuit breaker
    from the shared MarketplaceClient infrastructure.
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

    async def _fetch_single_page(
        self,
        date_from_str: str,
        flag: int = 0,
    ) -> Optional[List[dict]]:
        """
        Single API call via MarketplaceClient.

        Returns:
            list of dicts — success
            empty list [] — no data or API error
            None — rate limited (caller should retry)
        """
        params = {"dateFrom": date_from_str}
        if flag:
            params["flag"] = flag

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_stats",
            api_key=self.api_key,
        ) as client:
            response = await client.get(ENDPOINT, params=params)

            if response.is_success:
                data = response.data
                if isinstance(data, list):
                    return data
                logger.error(
                    "Orders API returned unexpected format: %s",
                    type(data),
                )
                return []

            if response.is_rate_limited:
                logger.warning("Orders API rate limited (429), will retry")
                return None  # signal to retry

            if response.is_auth_error:
                logger.error(
                    "Orders API auth error (shop=%s): %s",
                    self.shop_id,
                    response.error,
                )
                return []

            logger.error(
                "Orders API error: status=%s error=%s",
                response.status_code,
                response.error,
            )
            return []

    async def fetch_all_orders(
        self,
        date_from: datetime,
        flag: int = 0,
        on_progress=None,
    ) -> List[dict]:
        """
        Fetch ALL orders with automatic pagination via MarketplaceClient.

        Pagination (flag=0):
            Uses lastChangeDate from the last row of each response
            as dateFrom for the next request. Stops on empty response [].
            Rate limit: 1 request per minute.

        Args:
            date_from: Start datetime
            flag: 0 = paginated by lastChangeDate, 1 = all for that date
            on_progress: optional callback(page, total_so_far)

        Returns:
            List of raw order dicts from API
        """
        all_orders = []
        current_date_from = date_from.strftime("%Y-%m-%dT%H:%M:%S")
        page = 0

        while True:
            page += 1
            logger.info(
                "Orders API page %d: dateFrom=%s, total_so_far=%d",
                page, current_date_from, len(all_orders),
            )

            result = await self._fetch_single_page(
                current_date_from, flag=flag,
            )

            # Rate limited — wait and retry same page
            if result is None:
                wait = RATE_LIMIT_PAUSE * 2
                logger.warning(
                    "Rate limited, waiting %ds before retry...", wait,
                )
                await asyncio.sleep(wait)
                continue

            # Empty response — all orders loaded
            if not result:
                logger.info("Orders API: empty response, all orders loaded")
                break

            all_orders.extend(result)

            if on_progress:
                on_progress(page, len(all_orders))

            # For flag=1 there is no pagination — one request returns all
            if flag == 1:
                break

            # Get lastChangeDate from the last item for next page
            last_item = result[-1]
            last_change_date = last_item.get("lastChangeDate", "")

            if not last_change_date or last_change_date <= current_date_from:
                logger.warning(
                    "Orders API: lastChangeDate=%s <= dateFrom=%s, stopping",
                    last_change_date, current_date_from,
                )
                break

            current_date_from = last_change_date

            # If got less than 70K — likely last page
            if len(result) < 70000:
                logger.info(
                    "Orders API: got %d rows (< 70K), likely last page",
                    len(result),
                )
                break

            # Rate limit: wait 1 minute between requests
            logger.info(
                "Orders API: got %d rows, waiting %ds for next page...",
                len(result), RATE_LIMIT_PAUSE,
            )
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        logger.info(
            "Orders API: total %d orders in %d pages",
            len(all_orders), page,
        )
        return all_orders
