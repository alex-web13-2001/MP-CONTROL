"""
Ozon Returns Service — Detailed returns and cancellations from Ozon Seller API.

Collects per-return records with reasons, logistics info, product details.
API: POST /v1/returns/list

PAGINATION WORKAROUND (2026-02-15):
    API returns last_id=0 which causes infinite loop.
    Solution: use max `id` from current page as the next `last_id` cursor.

Data flow:
    1. fetch_returns(time_from, time_to) → raw returns
    2. normalize → flat dicts for ClickHouse
    3. OzonReturnsLoader → insert into fact_ozon_returns
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

API_LIMIT = 500
RATE_LIMIT_PAUSE = 0.5
MAX_PAGES = 200  # safety limit
CH_BATCH_SIZE = 500


def _safe_float(val) -> float:
    try:
        return float(val) if val else 0.0
    except Exception:
        return 0.0


def _safe_int(val) -> int:
    try:
        return int(val) if val else 0
    except Exception:
        return 0


def _parse_dt(val) -> datetime:
    if not val:
        return datetime(1970, 1, 1)
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(val.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        try:
            return datetime.strptime(val[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return datetime(1970, 1, 1)


# ── Service ────────────────────────────────────────────────


class OzonReturnsService:
    """
    Fetch returns and cancellations from Ozon Seller API.

    Uses POST /v1/returns/list with cursor-based pagination.
    Workaround: API last_id=0 bug → use max(id) from page as cursor.
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

    async def fetch_returns(
        self, time_from: str, time_to: str,
    ) -> List[dict]:
        """
        Fetch all returns for period.

        Uses last_free_waiting_day filter + cursor pagination.
        Deduplicates by `id` to prevent duplicates from buggy pagination.
        """
        all_returns = []
        seen_ids = set()
        last_id = 0
        page = 0

        while page < MAX_PAGES:
            async with self._make_client() as client:
                response = await client.post(
                    "/v1/returns/list",
                    json={
                        "filter": {
                            "last_free_waiting_day": {
                                "time_from": time_from,
                                "time_to": time_to,
                            },
                        },
                        "limit": API_LIMIT,
                        "last_id": last_id,
                    },
                )

            if not response.is_success:
                logger.error("Returns API error: %s %s",
                             response.status_code, response.data)
                break

            returns = response.data.get("returns", [])
            has_next = response.data.get("has_next", False)

            if not returns:
                break

            # Workaround: API returns last_id=0, use max id from page
            new_items = []
            for r in returns:
                rid = r.get("id", 0)
                if rid not in seen_ids:
                    seen_ids.add(rid)
                    new_items.append(r)

            # If no new items, we're looping — stop
            if not new_items:
                logger.info("Returns: no new items on page %d, stopping", page)
                break

            all_returns.extend(new_items)
            page += 1

            # Use max id from page as cursor
            max_id = max(r.get("id", 0) for r in returns)
            if max_id <= last_id:
                # Cursor not advancing, stop
                logger.info("Returns: cursor stuck at %d, stopping", max_id)
                break
            last_id = max_id

            logger.info("Returns page %d: %d items (total %d, cursor=%d)",
                        page, len(new_items), len(all_returns), last_id)

            if not has_next:
                break

            await asyncio.sleep(RATE_LIMIT_PAUSE)

        logger.info("Returns: fetched %d total (%d pages)", len(all_returns), page)
        return all_returns


def normalize_returns(raw_returns: List[dict]) -> List[dict]:
    """Convert raw /v1/returns/list items into flat dicts for ClickHouse."""
    result = []
    for r in raw_returns:
        product = r.get("product", {})
        logistic = r.get("logistic", {})
        place = r.get("place", {})
        target = r.get("target_place", {})

        # Determine date: prefer logistic.return_date, fallback to final_moment
        return_date = logistic.get("return_date") or logistic.get("final_moment")
        accepted_at = logistic.get("final_moment") or return_date

        result.append({
            "dt": _parse_dt(return_date).date() if return_date else datetime(1970, 1, 1).date(),
            "return_id": _safe_int(r.get("id")),
            "order_id": _safe_int(r.get("order_id")),
            "order_number": r.get("order_number", ""),
            "posting_number": r.get("posting_number", ""),
            "return_type": r.get("type", ""),
            "return_schema": r.get("schema", ""),
            "return_reason": r.get("return_reason_name", ""),
            "sku": _safe_int(product.get("sku")),
            "offer_id": product.get("offer_id", ""),
            "product_name": product.get("name", ""),
            "quantity": _safe_int(product.get("quantity", 1)),
            "price": Decimal(str(_safe_float(
                (product.get("price") or {}).get("price", 0)
            ))),
            "place_name": place.get("name", ""),
            "target_place": target.get("name", ""),
            "compensation_status": str(r.get("compensation_status") or ""),
            "accepted_at": _parse_dt(accepted_at),
            "returned_at": _parse_dt(return_date),
        })

    return result


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_returns"
CH_COLUMNS = [
    "dt", "shop_id", "return_id", "order_id", "order_number", "posting_number",
    "return_type", "return_schema", "return_reason",
    "sku", "offer_id", "product_name", "quantity", "price",
    "place_name", "target_place",
    "compensation_status",
    "accepted_at", "returned_at",
    "updated_at",
]


class OzonReturnsLoader:
    """Insert returns data into ClickHouse."""

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
                r["dt"], shop_id, r["return_id"], r["order_id"],
                r["order_number"], r["posting_number"],
                r["return_type"], r["return_schema"], r["return_reason"],
                r["sku"], r["offer_id"], r["product_name"],
                r["quantity"], r["price"],
                r["place_name"], r["target_place"],
                r["compensation_status"],
                r["accepted_at"], r["returned_at"],
                now,
            ])

        total = 0
        for i in range(0, len(ch_rows), CH_BATCH_SIZE):
            batch = ch_rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d returns into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        if not self._client:
            return {}
        r = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(return_id) as unique_returns,
                min(dt) as min_date,
                max(dt) as max_date,
                countIf(return_type = 'Cancellation') as cancellations,
                countIf(return_type = 'Return') as returns,
                uniq(sku) as unique_skus,
                sum(price * quantity) as total_value
            FROM fact_ozon_returns FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        row = r.first_row
        if row:
            return {
                "total_rows": row[0],
                "unique_returns": row[1],
                "min_date": str(row[2]),
                "max_date": str(row[3]),
                "cancellations": row[4],
                "returns": row[5],
                "unique_skus": row[6],
                "total_value": float(row[7]),
            }
        return {}
