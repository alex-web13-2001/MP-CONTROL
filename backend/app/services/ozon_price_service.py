"""
Ozon Price Tracker Service — pricing + commission snapshots.

API: POST /v5/product/info/prices
     Returns prices, commissions, acquiring fees per product.

Captures daily snapshots to track pricing dynamics.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

API_LIMIT = 1000
RATE_LIMIT_PAUSE = 0.5
CH_BATCH_SIZE = 500


def _safe_dec(val) -> Decimal:
    try:
        return Decimal(str(val)) if val else Decimal("0")
    except Exception:
        return Decimal("0")


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except Exception:
        return 0.0


class OzonPriceService:
    """Fetch product prices and commissions from Ozon API."""

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

    async def fetch_prices(self) -> List[dict]:
        """
        Fetch all product prices via /v5/product/info/prices.

        Paginates with cursor (last_id).
        Returns normalized rows for ClickHouse.
        """
        all_rows = []
        last_id = ""

        while True:
            body = {
                "filter": {"visibility": "ALL"},
                "limit": API_LIMIT,
            }
            if last_id:
                body["last_id"] = last_id

            async with self._make_client() as client:
                response = await client.post(
                    "/v5/product/info/prices",
                    json=body,
                )

            if not response.is_success:
                logger.error("Prices API error: %s %s",
                             response.status_code, response.data)
                break

            items = response.data.get("items", [])
            new_last_id = response.data.get("last_id", "")

            if not items:
                break

            now = datetime.utcnow().date()
            for item in items:
                price_obj = item.get("price", {})
                comms = item.get("commissions", {})
                acquiring = item.get("acquiring", 0)
                mkt_price_val = price_obj.get("marketing_seller_price",
                                              price_obj.get("marketing_price", 0))

                # v5 API: "sku" is None, use product_id as SKU
                pid = int(item.get("product_id", 0) or 0)
                sku = int(item.get("sku", 0) or 0) or pid

                all_rows.append({
                    "dt": now,
                    "sku": sku,
                    "product_id": pid,
                    "offer_id": item.get("offer_id", ""),
                    "product_name": "",  # not in v5 response
                    "price": _safe_dec(price_obj.get("price")),
                    "old_price": _safe_dec(price_obj.get("old_price")),
                    "min_price": _safe_dec(price_obj.get("min_price")),
                    "marketing_price": _safe_dec(mkt_price_val),
                    "sales_percent": _safe_float(
                        comms.get("sales_percent_fbo", 0)),
                    "fbo_commission_percent": _safe_float(
                        comms.get("sales_percent_fbo", 0)),
                    "fbs_commission_percent": _safe_float(
                        comms.get("sales_percent_fbs", 0)),
                    "fbo_commission_value": _safe_dec(
                        comms.get("fbo_direct_flow_trans_min_amount", 0)),
                    "fbs_commission_value": _safe_dec(
                        comms.get("fbs_direct_flow_trans_min_amount", 0)),
                    "acquiring_percent": _safe_float(acquiring),
                })

            logger.info("Prices page: %d items (total %d)",
                        len(items), len(all_rows))

            # v5 may return empty last_id when all items fit in one page
            if not new_last_id or new_last_id == last_id:
                break
            last_id = new_last_id
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_rows


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_prices"
CH_COLUMNS = [
    "dt", "shop_id", "sku", "product_id", "offer_id", "product_name",
    "price", "old_price", "min_price", "marketing_price",
    "sales_percent", "fbo_commission_percent", "fbs_commission_percent",
    "fbo_commission_value", "fbs_commission_value",
    "acquiring_percent",
    "updated_at",
]


class OzonPriceLoader:
    """Insert price snapshots into ClickHouse."""

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
                r["dt"], shop_id, r["sku"], r["product_id"],
                r["offer_id"], r["product_name"],
                r["price"], r["old_price"], r["min_price"], r["marketing_price"],
                r["sales_percent"], r["fbo_commission_percent"],
                r["fbs_commission_percent"],
                r["fbo_commission_value"], r["fbs_commission_value"],
                r["acquiring_percent"],
                now,
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d price rows", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT count(), uniq(sku),
                   avg(price), avg(sales_percent),
                   avg(fbo_commission_percent), avg(acquiring_percent)
            FROM fact_ozon_prices
            WHERE shop_id = {shop_id:UInt32} AND dt = today()
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0], "unique_skus": row[1],
                "avg_price": float(row[2]),
                "avg_sales_percent": float(row[3]),
                "avg_fbo_commission": float(row[4]),
                "avg_acquiring": float(row[5]),
            }
        return {}
