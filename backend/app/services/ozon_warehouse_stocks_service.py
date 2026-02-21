"""
Ozon Warehouse Stocks Service — per-warehouse stock levels.

API: POST /v2/analytics/stock_on_warehouses
     POST /v4/product/info/stocks (alternative)

Data: SKU × warehouse snapshots with free_to_sell, promised, reserved.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

API_LIMIT = 500
RATE_LIMIT_PAUSE = 0.5
CH_BATCH_SIZE = 500


class OzonWarehouseStocksService:
    """Fetch per-warehouse stock levels from Ozon API."""

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

    async def fetch_warehouse_stocks(self) -> List[dict]:
        """
        Fetch stock levels per warehouse via /v2/analytics/stock_on_warehouses.

        Returns normalized rows ready for ClickHouse.
        """
        all_rows = []
        offset = 0

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v2/analytics/stock_on_warehouses",
                    json={
                        "limit": API_LIMIT,
                        "offset": offset,
                    },
                )

            if not response.is_success:
                logger.error("Warehouse stocks API error: %s %s",
                             response.status_code, response.data)
                break

            result = response.data.get("result", {})
            rows = result.get("rows", [])

            if not rows:
                break

            now = datetime.utcnow().date()
            for row in rows:
                all_rows.append({
                    "dt": now,
                    "sku": int(row.get("sku", 0)),
                    "product_name": row.get("item_name", ""),
                    "offer_id": row.get("item_code", ""),
                    "warehouse_name": row.get("warehouse_name", ""),
                    "warehouse_type": "fbo",  # endpoint is FBO-focused
                    "free_to_sell": int(row.get("free_to_sell_amount", 0)),
                    "promised": int(row.get("promised_amount", 0)),
                    "reserved": int(row.get("reserved_amount", 0)),
                })

            logger.info("Warehouse stocks offset=%d: %d rows (total %d)",
                        offset, len(rows), len(all_rows))

            if len(rows) < API_LIMIT:
                break

            offset += len(rows)
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows

    async def fetch_product_stocks(self) -> List[dict]:
        """
        Alternative: /v4/product/info/stocks for FBO+FBS stocks.
        """
        all_rows = []
        last_id = ""

        while True:
            body = {"filter": {"visibility": "ALL"}, "limit": API_LIMIT}
            if last_id:
                body["last_id"] = last_id

            async with self._make_client() as client:
                response = await client.post(
                    "/v4/product/info/stocks",
                    json=body,
                )

            if not response.is_success:
                logger.error("Product stocks API error: %s", response.status_code)
                break

            items = response.data.get("items", [])
            new_last_id = response.data.get("last_id", "")

            if not items:
                break

            now = datetime.utcnow().date()
            for item in items:
                sku = item.get("product_id", 0)
                offer_id = item.get("offer_id", "")
                for stock in item.get("stocks", []):
                    all_rows.append({
                        "dt": now,
                        "sku": sku,
                        "product_name": "",  # not in this endpoint
                        "offer_id": offer_id,
                        "warehouse_name": stock.get("warehouse_name", ""),
                        "warehouse_type": stock.get("type", ""),
                        "free_to_sell": int(stock.get("present", 0)),
                        "promised": int(stock.get("promised_amount", 0)),
                        "reserved": int(stock.get("reserved", 0)),
                    })

            if not new_last_id or new_last_id == last_id:
                break
            last_id = new_last_id
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_warehouse_stocks"
CH_COLUMNS = [
    "dt", "shop_id", "sku", "product_name", "offer_id",
    "warehouse_name", "warehouse_type",
    "free_to_sell", "promised", "reserved",
    "updated_at",
]


class OzonWarehouseStocksLoader:
    """Insert warehouse stocks into ClickHouse."""

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
                r["dt"], shop_id, r["sku"], r["product_name"], r["offer_id"],
                r["warehouse_name"], r["warehouse_type"],
                r["free_to_sell"], r["promised"], r["reserved"],
                now,
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d warehouse stock rows", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT count(), uniq(sku), uniq(warehouse_name),
                   sum(free_to_sell), sum(reserved)
            FROM fact_ozon_warehouse_stocks FINAL
            WHERE shop_id = {shop_id:UInt32} AND dt = today()
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0], "unique_skus": row[1],
                "unique_warehouses": row[2],
                "total_free": row[3], "total_reserved": row[4],
            }
        return {}
