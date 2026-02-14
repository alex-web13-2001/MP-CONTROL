"""
Ozon Seller Rating Service — account health metrics.

API: POST /v1/rating/summary
     Returns seller rating groups with individual metrics.

Captures daily snapshots for monitoring account health.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

CH_BATCH_SIZE = 500


class OzonSellerRatingService:
    """Fetch seller rating from Ozon API."""

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

    async def fetch_rating(self) -> List[dict]:
        """
        Fetch seller rating summary.

        Returns normalized rows (one per rating metric).
        """
        async with self._make_client() as client:
            response = await client.post(
                "/v1/rating/summary",
                json={},
            )

        if not response.is_success:
            logger.error("Rating API error: %s %s",
                         response.status_code, response.data)
            return []

        groups = response.data.get("groups", [])
        now = datetime.utcnow().date()
        rows = []

        for group in groups:
            group_name = group.get("group_name", "")
            for idx, item in enumerate(group.get("items", [])):
                # API uses 'name' not 'rating_name' (rating_name=None)
                name = (item.get("name") or item.get("rating_name")
                        or f"{group_name}_{idx}")
                status = item.get("status", "")
                if isinstance(status, dict):
                    status = status.get("key", "")
                rows.append({
                    "dt": now,
                    "group_name": group_name,
                    "rating_name": name,
                    "rating_value": float(item.get("current_value", 0) or 0),
                    "rating_status": str(status),
                    "penalty_score": float(item.get("penalty_score_down", 0) or 0),
                })

        logger.info("Rating: %d metrics from %d groups", len(rows), len(groups))
        return rows


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_seller_rating"
CH_COLUMNS = [
    "dt", "shop_id", "group_name", "rating_name",
    "rating_value", "rating_status", "penalty_score",
    "updated_at",
]


class OzonSellerRatingLoader:
    """Insert seller rating into ClickHouse."""

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
                r["dt"], shop_id, r["group_name"], r["rating_name"],
                r["rating_value"], r["rating_status"], r["penalty_score"],
                now,
            ])

        self._client.insert(CH_TABLE, ch_rows, column_names=CH_COLUMNS)
        logger.info("Inserted %d rating rows", len(ch_rows))
        return len(ch_rows)

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT count(), uniq(rating_name), uniq(group_name)
            FROM fact_ozon_seller_rating
            WHERE shop_id = {shop_id:UInt32} AND dt = today()
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0],
                "unique_metrics": row[1],
                "unique_groups": row[2],
            }
        return {}
