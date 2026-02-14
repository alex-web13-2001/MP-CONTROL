"""
Ozon Orders Service — Fetch FBO & FBS postings from Ozon Seller API.

All requests go through MarketplaceClient (proxy, rate limiting, circuit breaker).

API Endpoints:
    POST /v2/posting/fbo/list — FBO orders (Ozon warehouse)
    POST /v3/posting/fbs/list — FBS orders (seller warehouse)

Data flow:
    1. fetch_fbo_postings → paginated list of FBO postings
    2. fetch_fbs_postings → paginated list of FBS postings
    3. normalize → unified rows (1 row per product per posting)
    4. OzonOrdersLoader → ClickHouse fact_ozon_orders
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

PAGE_SIZE = 1000
CH_BATCH_SIZE = 500


def _safe_decimal(val) -> Decimal:
    """Convert any numeric (or string-numeric) value to Decimal, default 0."""
    if val is None or val == "":
        return Decimal("0")
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal("0")


def _parse_dt(val) -> datetime:
    """Parse an ISO-ish datetime string, return epoch-zero on failure."""
    if not val:
        return datetime(1970, 1, 1)
    try:
        # "2026-01-15T03:07:47.471475Z" → strip tz suffix
        s = str(val).replace("Z", "+00:00")
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
        return datetime(1970, 1, 1)


# ── Service ────────────────────────────────────────────────


class OzonOrdersService:
    """
    Fetch FBO & FBS postings from Ozon Seller API.

    Usage:
        async with OzonOrdersService(db, shop_id, api_key, client_id) as svc:
            orders = await svc.fetch_all_orders(since, to)
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

    async def fetch_fbo_postings(
        self, since: str, to: str, *, limit: int = PAGE_SIZE,
    ) -> List[dict]:
        """
        Fetch ALL FBO postings for the given period (paginated via offset).

        Args:
            since: ISO datetime string for period start
            to: ISO datetime string for period end

        Returns:
            List of raw posting dicts from API
        """
        all_items = []
        offset = 0

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v2/posting/fbo/list",
                    json={
                        "dir": "ASC",
                        "filter": {
                            "since": since,
                            "to": to,
                            "status": "",
                        },
                        "limit": limit,
                        "offset": offset,
                        "with": {
                            "analytics_data": True,
                            "financial_data": True,
                        },
                    },
                )

            if not response.is_success:
                logger.error(
                    "FBO list failed: %s %s",
                    response.status_code, response.data,
                )
                break

            items = response.data.get("result", [])
            if not items:
                break

            all_items.extend(items)
            logger.info(
                "FBO page offset=%d → %d items (total %d)",
                offset, len(items), len(all_items),
            )

            if len(items) < limit:
                break
            offset += limit
            await asyncio.sleep(0.3)  # rate limit

        logger.info("FBO total: %d postings", len(all_items))
        return all_items

    async def fetch_fbs_postings(
        self, since: str, to: str, *, limit: int = PAGE_SIZE,
    ) -> List[dict]:
        """
        Fetch ALL FBS postings for the given period (paginated via offset + has_next).
        """
        all_items = []
        offset = 0

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v3/posting/fbs/list",
                    json={
                        "dir": "ASC",
                        "filter": {
                            "since": since,
                            "to": to,
                            "status": "",
                        },
                        "limit": limit,
                        "offset": offset,
                        "with": {
                            "analytics_data": True,
                            "financial_data": True,
                        },
                    },
                )

            if not response.is_success:
                logger.error(
                    "FBS list failed: %s %s",
                    response.status_code, response.data,
                )
                break

            result = response.data.get("result", {})
            postings = result.get("postings", [])
            has_next = result.get("has_next", False)

            if not postings:
                break

            all_items.extend(postings)
            logger.info(
                "FBS page offset=%d → %d items (total %d, has_next=%s)",
                offset, len(postings), len(all_items), has_next,
            )

            if not has_next:
                break
            offset += limit
            await asyncio.sleep(0.3)

        logger.info("FBS total: %d postings", len(all_items))
        return all_items

    async def fetch_all_orders(
        self, since: str, to: str,
    ) -> List[dict]:
        """
        Fetch FBO + FBS and normalize into unified rows.

        FBS has a PERIOD_IS_TOO_LONG limit (~30 days), so we chunk it.
        FBO does not have this limit.

        Returns list of flat dicts, 1 row per product per posting.
        """
        fbo = await self.fetch_fbo_postings(since, to)

        # FBS: chunk into 30-day windows to avoid PERIOD_IS_TOO_LONG
        fbs = await self._fetch_fbs_chunked(since, to)

        rows = []
        rows.extend(_normalize_postings(fbo, "FBO"))
        rows.extend(_normalize_postings(fbs, "FBS"))

        logger.info(
            "Total normalized rows: %d (FBO=%d raw, FBS=%d raw)",
            len(rows), len(fbo), len(fbs),
        )
        return rows

    async def _fetch_fbs_chunked(
        self, since: str, to: str, chunk_days: int = 28,
    ) -> List[dict]:
        """
        Fetch FBS postings in time chunks to avoid PERIOD_IS_TOO_LONG error.

        Ozon limits FBS queries to ~30 days. We use 28 days for safety.
        """
        from datetime import datetime, timedelta

        dt_since = datetime.fromisoformat(since.replace("Z", "+00:00")).replace(tzinfo=None)
        dt_to = datetime.fromisoformat(to.replace("Z", "+00:00")).replace(tzinfo=None)

        all_fbs = []
        chunk_start = dt_since

        while chunk_start < dt_to:
            chunk_end = min(chunk_start + timedelta(days=chunk_days), dt_to)
            s = chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            e = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            logger.info("FBS chunk: %s → %s", s[:10], e[:10])
            items = await self.fetch_fbs_postings(s, e)
            all_fbs.extend(items)

            chunk_start = chunk_end

        logger.info("FBS chunked total: %d postings", len(all_fbs))
        return all_fbs


# ── Normalization ──────────────────────────────────────────


def _normalize_postings(postings: List[dict], mode: str) -> List[dict]:
    """
    Flatten postings → 1 row per product per posting.

    Handles both FBO and FBS response structures.
    """
    rows = []

    for p in postings:
        posting_number = p.get("posting_number", "")
        order_id = p.get("order_id", 0)
        order_number = p.get("order_number", "")
        status = p.get("status", "")
        substatus = p.get("substatus", "")

        # Date: prefer created_at, fallback to in_process_at
        created_at = p.get("created_at")
        in_process_at = p.get("in_process_at")
        order_date = _parse_dt(created_at or in_process_at)
        in_process_dt = _parse_dt(in_process_at)

        # Analytics
        analytics = p.get("analytics_data") or {}
        city = analytics.get("city", "") or ""
        region = analytics.get("region", "") or ""
        delivery_type = analytics.get("delivery_type", "") or ""
        warehouse_name = analytics.get("warehouse_name", "") or analytics.get("warehouse", "") or ""

        # Financial (posting-level)
        financial = p.get("financial_data") or {}
        cluster_from = financial.get("cluster_from", "") or ""
        cluster_to = financial.get("cluster_to", "") or ""
        fin_products = financial.get("products", []) or []

        # Build product_id → financial map
        fin_map: Dict[int, dict] = {}
        for fp in fin_products:
            fpid = fp.get("product_id", 0)
            if fpid:
                fin_map[fpid] = fp

        # Cancellation (FBS only)
        cancellation = p.get("cancellation") or {}
        cancel_reason = cancellation.get("cancel_reason", "") or ""

        # Shipment date (FBS only)
        shipment_date = _parse_dt(p.get("shipment_date"))

        # Products
        products = p.get("products", [])
        for prod in products:
            sku = prod.get("sku", 0)
            product_id = prod.get("product_id", 0) or sku
            offer_id = prod.get("offer_id", "")
            product_name = prod.get("name", "")
            quantity = prod.get("quantity", 1)
            price = _safe_decimal(prod.get("price"))

            # Match financial data by product_id or sku
            fin = fin_map.get(product_id, fin_map.get(sku, {}))
            old_price = _safe_decimal(fin.get("old_price", prod.get("old_price", 0)))
            commission_amount = _safe_decimal(fin.get("commission_amount", 0))
            commission_percent = _safe_decimal(fin.get("commission_percent", 0))
            payout = _safe_decimal(fin.get("payout", 0))
            total_discount_percent = _safe_decimal(fin.get("total_discount_percent", 0))
            total_discount_value = _safe_decimal(fin.get("total_discount_value", 0))

            rows.append({
                "posting_number": posting_number,
                "order_id": order_id,
                "order_number": order_number,
                "order_date": order_date,
                "in_process_at": in_process_dt,
                "status": status,
                "substatus": substatus,
                "sku": sku,
                "product_id": product_id,
                "offer_id": offer_id,
                "product_name": product_name,
                "quantity": quantity,
                "warehouse_mode": mode,
                "price": price,
                "old_price": old_price,
                "commission_amount": commission_amount,
                "commission_percent": commission_percent,
                "payout": payout,
                "total_discount_percent": total_discount_percent,
                "total_discount_value": total_discount_value,
                "city": city,
                "region": region,
                "cluster_from": cluster_from,
                "cluster_to": cluster_to,
                "delivery_type": delivery_type,
                "warehouse_name": warehouse_name,
                "cancel_reason": cancel_reason,
                "shipment_date": shipment_date,
            })

    return rows


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_orders"
CH_COLUMNS = [
    "posting_number", "order_id", "order_number",
    "order_date", "in_process_at", "status", "substatus",
    "sku", "product_id", "offer_id", "product_name", "quantity",
    "warehouse_mode",
    "price", "old_price",
    "commission_amount", "commission_percent", "payout",
    "total_discount_percent", "total_discount_value",
    "city", "region", "cluster_from", "cluster_to",
    "delivery_type", "warehouse_name",
    "cancel_reason", "shipment_date",
    "shop_id", "updated_at",
]


class OzonOrdersLoader:
    """Insert normalized order rows into ClickHouse fact_ozon_orders."""

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

    def insert_orders(self, shop_id: int, orders: List[dict]) -> int:
        """
        Insert normalized order rows into ClickHouse.

        Args:
            shop_id: shop identifier
            orders: list of normalized order dicts from _normalize_postings()

        Returns:
            number of rows inserted
        """
        if not orders or not self._client:
            return 0

        now = datetime.utcnow()
        rows = []

        for o in orders:
            rows.append([
                o["posting_number"],
                o["order_id"],
                o["order_number"],
                o["order_date"],
                o["in_process_at"],
                o["status"],
                o["substatus"],
                o["sku"],
                o["product_id"],
                o["offer_id"],
                o["product_name"],
                o["quantity"],
                o["warehouse_mode"],
                o["price"],
                o["old_price"],
                o["commission_amount"],
                o["commission_percent"],
                o["payout"],
                o["total_discount_percent"],
                o["total_discount_value"],
                o["city"],
                o["region"],
                o["cluster_from"],
                o["cluster_to"],
                o["delivery_type"],
                o["warehouse_name"],
                o["cancel_reason"],
                o["shipment_date"],
                shop_id,
                now,
            ])

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d order rows into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get order stats from ClickHouse."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(posting_number) as unique_postings,
                uniq(order_id) as unique_orders,
                countIf(warehouse_mode = 'FBO') as fbo_rows,
                countIf(warehouse_mode = 'FBS') as fbs_rows,
                countIf(status = 'delivered') as delivered,
                countIf(status = 'cancelled') as cancelled,
                sum(payout) as total_payout,
                sum(commission_amount) as total_commission,
                min(order_date) as min_date,
                max(order_date) as max_date
            FROM fact_ozon_orders
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_postings": r[1],
                "unique_orders": r[2],
                "fbo_rows": r[3],
                "fbs_rows": r[4],
                "delivered": r[5],
                "cancelled": r[6],
                "total_payout": float(r[7]),
                "total_commission": float(r[8]),
                "min_date": str(r[9]),
                "max_date": str(r[10]),
            }
        return {}
