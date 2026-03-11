"""
Ozon Turnover Service — FBO item turnover analytics.

API: POST /v1/analytics/item_turnover
     Requires date_from/date_to (max 31 days).
     Returns per-SKU turnover: days_on_site, stock, avg_daily_sales, days_of_supply.

Data: daily snapshots → ClickHouse fact_ozon_turnover.
"""

import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

API_LIMIT = 500
RATE_LIMIT_PAUSE = 0.5
CH_BATCH_SIZE = 500


class OzonTurnoverService:
    """Fetch FBO item turnover data from Ozon API."""

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

    async def fetch_turnover(self, date_from: str = None, date_to: str = None) -> List[dict]:
        """
        Fetch item turnover via POST /v1/analytics/item_turnover.

        API returns per-SKU turnover metrics:
        - days_on_site: how long the item has been on Ozon
        - fbo_stock: current FBO stock
        - avg_daily_sales: average daily sales (Ozon calculation)
        - days_of_supply: how many days current stock will last

        Returns normalized rows ready for ClickHouse.
        """
        if not date_from:
            d = date.today() - timedelta(days=30)
            date_from = d.strftime("%Y-%m-%d")
        if not date_to:
            date_to = date.today().strftime("%Y-%m-%d")

        all_rows = []
        offset = 0

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v1/analytics/item_turnover",
                    json={
                        "date_from": date_from,
                        "date_to": date_to,
                        "limit": API_LIMIT,
                        "offset": offset,
                    },
                )

            if not response.is_success:
                logger.error("Turnover API error: %s %s",
                             response.status_code, response.data)
                break

            result = response.data.get("result", {})
            items = result.get("items", [])

            if not items:
                # Try alternative response structure
                items = response.data.get("items", [])

            if not items:
                logger.info("Turnover API: no items returned (offset=%d)", offset)
                break

            now = date.today()
            for item in items:
                sku = int(item.get("sku", 0))
                if sku == 0:
                    continue

                all_rows.append({
                    "dt": now,
                    "sku": sku,
                    "product_id": int(item.get("product_id", 0)),
                    "offer_id": item.get("offer_id", ""),
                    "product_name": item.get("name", item.get("item_name", "")),
                    "days_on_site": int(item.get("days_on_site", 0)),
                    "stock_fbo": int(item.get("fbo_stock", item.get("stock_fbo", 0))),
                    "avg_daily_sales": float(item.get("avg_daily_sales",
                                                       item.get("average_daily_quantity", 0))),
                    "days_of_supply": float(item.get("days_of_supply",
                                                      item.get("forecast_days", 0))),
                    "turnover_category": item.get("turnover_category",
                                                   item.get("category", "")),
                    "revenue_30d": float(item.get("revenue_30d",
                                                   item.get("revenue", 0))),
                    "sold_30d": int(item.get("sold_30d",
                                              item.get("ordered_units", 0))),
                })

            logger.info("Turnover offset=%d: %d items (total %d)",
                        offset, len(items), len(all_rows))

            if len(items) < API_LIMIT:
                break

            offset += len(items)
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows

    async def fetch_turnover_stocks(self) -> List[dict]:
        """
        Alternative: POST /v1/analytics/turnover/stocks (beta).
        Returns forecast days_of_supply per SKU.
        Falls back gracefully if endpoint unavailable.
        """
        try:
            async with self._make_client() as client:
                response = await client.post(
                    "/v1/analytics/turnover/stocks",
                    json={"limit": 1000, "offset": 0},
                )

            if not response.is_success:
                logger.warning("turnover/stocks (beta) unavailable: %s",
                               response.status_code)
                return []

            items = response.data.get("items", [])
            now = date.today()
            rows = []
            for item in items:
                sku = int(item.get("sku", 0))
                if sku == 0:
                    continue
                rows.append({
                    "dt": now,
                    "sku": sku,
                    "product_id": int(item.get("product_id", 0)),
                    "offer_id": item.get("offer_id", ""),
                    "product_name": item.get("name", ""),
                    "days_on_site": 0,
                    "stock_fbo": int(item.get("present", item.get("stock", 0))),
                    "avg_daily_sales": float(item.get("avg_speed", 0)),
                    "days_of_supply": float(item.get("forecast_days",
                                                      item.get("days_of_supply", 0))),
                    "turnover_category": "",
                    "revenue_30d": 0,
                    "sold_30d": 0,
                })
            return rows
        except Exception as e:
            logger.warning("turnover/stocks (beta) error: %s", e)
            return []


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_turnover"
CH_COLUMNS = [
    "dt", "shop_id", "sku", "product_id", "offer_id", "product_name",
    "days_on_site", "stock_fbo", "avg_daily_sales", "days_of_supply",
    "turnover_category", "revenue_30d", "sold_30d", "updated_at",
]


class OzonTurnoverLoader:
    """Insert turnover data into ClickHouse."""

    def __init__(self, host="clickhouse", port=8123,
                 username="default", password="", database="mms_analytics"):
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
                r["dt"], shop_id, r["sku"], r["product_id"], r["offer_id"],
                r["product_name"], r["days_on_site"], r["stock_fbo"],
                r["avg_daily_sales"], r["days_of_supply"],
                r["turnover_category"], r["revenue_30d"], r["sold_30d"],
                now,
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d turnover rows for shop %d", total, shop_id)
        return total

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT count(), uniq(sku),
                   avg(days_of_supply), avg(avg_daily_sales)
            FROM fact_ozon_turnover FINAL
            WHERE shop_id = {shop_id:UInt32} AND dt = today()
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0], "unique_skus": row[1],
                "avg_days_supply": round(float(row[2]), 1),
                "avg_daily_sales": round(float(row[3]), 2),
            }
        return {}
