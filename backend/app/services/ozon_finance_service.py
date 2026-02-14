"""
Ozon Finance Service — Transaction Stream from Ozon Seller API.

Fetches all financial transactions (debits & credits) for P&L,
bank reconciliation, and per-item cost accounting.

API: POST /v3/finance/transaction/list
    - Period limit: max 1 month per request
    - Pagination: page + page_size (max 1000)
    - Rate limit: ~1.5s between pages

Data flow:
    1. fetch_transactions(from, to) → paginated list for one month
    2. fetch_all_transactions(since, to) → chunked by month
    3. _normalize_transaction() → flat rows
    4. OzonTransactionsLoader → ClickHouse fact_ozon_transactions

Маппинг operation_type → category:
    Revenue, Refund, Logistics, Marketing, Storage,
    Penalty, Acquiring, Compensation, Other
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
RATE_LIMIT_PAUSE = 1.5
CH_BATCH_SIZE = 500


# ── Operation Type → Category Mapping ─────────────────────

OPERATION_CATEGORY_MAP: Dict[str, str] = {
    # Revenue (продажи)
    "OperationAgentDeliveredToCustomer": "Revenue",

    # Refund (возвраты)
    "OperationItemReturn": "Refund",
    "OperationReturnGoodsFBSofRMS": "Refund",
    "ClientReturnAgent": "Refund",
    "ClientReturnAgentFBS": "Refund",
    "OperationItemReturnFBO": "Refund",

    # Logistics (логистика, кросс-докинг, поставки)
    "MarketplaceServiceItemCrossdocking": "Logistics",
    "OperationMarketplaceSupplyAdditional": "Logistics",
    "OperationMarketplaceSupplyExpirationDateProcessing": "Logistics",
    "OperationMarketplaceServiceSupplyInboundCargoShortage": "Logistics",
    "OperationMarketplaceServiceSupplyInboundSupplyShortage": "Logistics",
    "MarketplaceServiceItemDirectFlowLogistic": "Logistics",
    "MarketplaceServiceItemDelivToCustomer": "Logistics",
    "MarketplaceServiceItemDropoff": "Logistics",
    "SellerReturnsDeliveryToPickupPoint": "Logistics",

    # Marketing (реклама, отзывы)
    "OperationMarketplaceCostPerClick": "Marketing",
    "OperationMarketPlaceItemPinReview": "Marketing",
    "OperationPointsForReviews": "Marketing",
    "ServiceAdvertising": "Marketing",

    # Storage (хранение)
    "OperationMarketplaceServiceStorage": "Storage",
    "OperationMarketplaceItemTemporaryStorageRedistribution": "Storage",

    # Penalty (штрафы, ошибки продавца)
    "DefectRateDetailed": "Penalty",
    "DefectRateCancellation": "Penalty",
    "OperationSellerReturnsCargoAssortmentInvalid": "Penalty",
    "DisposalReasonDamagedPackaging": "Penalty",

    # Acquiring (эквайринг)
    "MarketplaceRedistributionOfAcquiringOperation": "Acquiring",

    # Compensation (компенсации от Ozon)
    "AccrualInternalClaim": "Compensation",
}


def _get_category(operation_type: str) -> str:
    """Map operation_type to our category. Unknown types → 'Other'."""
    return OPERATION_CATEGORY_MAP.get(operation_type, "Other")


def _safe_decimal(val) -> Decimal:
    if val is None or val == "":
        return Decimal("0")
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal("0")


def _parse_dt(val) -> datetime:
    """Parse Ozon finance datetime 'YYYY-MM-DD HH:MM:SS'."""
    if not val:
        return datetime(1970, 1, 1)
    try:
        return datetime.strptime(str(val).strip(), "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            s = str(val).replace("Z", "+00:00")
            return datetime.fromisoformat(s).replace(tzinfo=None)
        except Exception:
            return datetime(1970, 1, 1)


# ── Service ────────────────────────────────────────────────


class OzonFinanceService:
    """
    Fetch financial transactions from Ozon Seller API.

    Handles:
        - Pagination (page + page_size)
        - Monthly chunking (API limit: max 1 month per request)
        - Rate limiting (1.5s between pages)
        - Proxy rotation via MarketplaceClient
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

    async def fetch_transactions(
        self, from_dt: str, to_dt: str,
    ) -> List[dict]:
        """
        Fetch transactions for a SINGLE period (max 1 month).

        Paginates through all pages until empty response.

        Args:
            from_dt: ISO datetime string (start of period)
            to_dt: ISO datetime string (end of period)

        Returns:
            List of raw operation dicts from API
        """
        all_ops = []
        page = 1

        while True:
            async with self._make_client() as client:
                response = await client.post(
                    "/v3/finance/transaction/list",
                    json={
                        "filter": {
                            "date": {
                                "from": from_dt,
                                "to": to_dt,
                            },
                            "transaction_type": "all",
                        },
                        "page": page,
                        "page_size": PAGE_SIZE,
                    },
                )

            if not response.is_success:
                logger.error(
                    "Finance list failed: %s %s",
                    response.status_code, response.data,
                )
                break

            operations = response.data.get("result", {}).get("operations", [])
            if not operations:
                break

            all_ops.extend(operations)
            logger.info(
                "Finance page %d: %d ops (total %d) [%s → %s]",
                page, len(operations), len(all_ops),
                from_dt[:10], to_dt[:10],
            )

            if len(operations) < PAGE_SIZE:
                break

            page += 1
            await asyncio.sleep(RATE_LIMIT_PAUSE)

        return all_ops

    async def fetch_all_transactions(
        self, since: str, to: str,
    ) -> List[dict]:
        """
        Fetch transactions for any period, chunking by calendar months.

        Ozon limits each request to max 1 month. This method automatically
        generates monthly chunks: [Jan 1-31], [Feb 1-28], etc.

        Args:
            since: ISO datetime string (overall start)
            to: ISO datetime string (overall end)

        Returns:
            List of raw operation dicts, all months combined
        """
        dt_since = _parse_dt(since)
        dt_to = _parse_dt(to)

        all_ops = []
        chunk_start = dt_since

        while chunk_start < dt_to:
            # End of current month
            if chunk_start.month == 12:
                next_month = chunk_start.replace(year=chunk_start.year + 1, month=1, day=1)
            else:
                next_month = chunk_start.replace(month=chunk_start.month + 1, day=1)

            chunk_end = min(next_month, dt_to)

            from_str = chunk_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            to_str = chunk_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            logger.info("Finance chunk: %s → %s", from_str[:10], to_str[:10])
            ops = await self.fetch_transactions(from_str, to_str)
            all_ops.extend(ops)

            chunk_start = next_month

        logger.info("Finance total: %d operations", len(all_ops))
        return all_ops


# ── Normalization ──────────────────────────────────────────


def _normalize_transaction(op: dict) -> dict:
    """
    Flatten a single API operation into a row for ClickHouse.

    Extracts:
        - Core fields (operation_id, date, type, amount)
        - Category from our mapping
        - First item's SKU and name
        - Posting info (posting_number, delivery_schema)
        - Sum of all services prices
    """
    posting = op.get("posting") or {}
    items = op.get("items") or []
    services = op.get("services") or []

    # First item (most transactions have exactly 1 item)
    item = items[0] if items else {}
    sku = item.get("sku", 0) or 0
    item_name = item.get("name", "") or ""

    # Sum all service prices
    services_total = sum(s.get("price", 0) or 0 for s in services)

    operation_type = op.get("operation_type", "")

    return {
        "operation_id": op.get("operation_id", 0),
        "operation_date": _parse_dt(op.get("operation_date")),
        "operation_type": operation_type,
        "operation_type_name": op.get("operation_type_name", ""),
        "category": _get_category(operation_type),
        "posting_number": posting.get("posting_number", "") or "",
        "delivery_schema": posting.get("delivery_schema", "") or "",
        "sku": sku,
        "item_name": item_name,
        "amount": _safe_decimal(op.get("amount", 0)),
        "accruals_for_sale": _safe_decimal(op.get("accruals_for_sale", 0)),
        "sale_commission": _safe_decimal(op.get("sale_commission", 0)),
        "services_total": _safe_decimal(services_total),
        "type": op.get("type", ""),
    }


def normalize_transactions(operations: List[dict]) -> List[dict]:
    """Normalize a list of raw API operations into flat rows."""
    return [_normalize_transaction(op) for op in operations]


# ── ClickHouse Loader ──────────────────────────────────────

CH_TABLE = "mms_analytics.fact_ozon_transactions"
CH_COLUMNS = [
    "operation_id", "operation_date", "operation_type", "operation_type_name",
    "category", "posting_number", "delivery_schema",
    "sku", "item_name",
    "amount", "accruals_for_sale", "sale_commission", "services_total",
    "type", "shop_id", "updated_at",
]


class OzonTransactionsLoader:
    """Insert normalized transaction rows into ClickHouse."""

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

    def insert_transactions(
        self, shop_id: int, transactions: List[dict],
    ) -> int:
        """
        Insert normalized transaction rows into ClickHouse.

        Args:
            shop_id: shop identifier
            transactions: list of normalized transaction dicts

        Returns:
            number of rows inserted
        """
        if not transactions or not self._client:
            return 0

        now = datetime.utcnow()
        rows = []

        for t in transactions:
            rows.append([
                t["operation_id"],
                t["operation_date"],
                t["operation_type"],
                t["operation_type_name"],
                t["category"],
                t["posting_number"],
                t["delivery_schema"],
                t["sku"],
                t["item_name"],
                t["amount"],
                t["accruals_for_sale"],
                t["sale_commission"],
                t["services_total"],
                t["type"],
                shop_id,
                now,
            ])

        total = 0
        for i in range(0, len(rows), CH_BATCH_SIZE):
            batch = rows[i:i + CH_BATCH_SIZE]
            self._client.insert(CH_TABLE, batch, column_names=CH_COLUMNS)
            total += len(batch)

        logger.info("Inserted %d transaction rows into ClickHouse", total)
        return total

    def get_stats(self, shop_id: int) -> dict:
        """Get transaction stats from ClickHouse."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(operation_id) as unique_ops,
                min(operation_date) as min_date,
                max(operation_date) as max_date,
                sum(amount) as total_amount,
                sumIf(amount, category = 'Revenue') as revenue,
                sumIf(amount, category = 'Refund') as refunds,
                sumIf(amount, category = 'Logistics') as logistics,
                sumIf(amount, category = 'Marketing') as marketing,
                sumIf(amount, category = 'Storage') as storage,
                sumIf(amount, category = 'Penalty') as penalties,
                sumIf(amount, category = 'Acquiring') as acquiring,
                sumIf(amount, category = 'Compensation') as compensation,
                sum(accruals_for_sale) as total_sales,
                sum(sale_commission) as total_commission,
                sum(services_total) as total_services
            FROM fact_ozon_transactions
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0],
                "unique_ops": r[1],
                "min_date": str(r[2]),
                "max_date": str(r[3]),
                "total_amount": float(r[4]),
                "revenue": float(r[5]),
                "refunds": float(r[6]),
                "logistics": float(r[7]),
                "marketing": float(r[8]),
                "storage": float(r[9]),
                "penalties": float(r[10]),
                "acquiring": float(r[11]),
                "compensation": float(r[12]),
                "total_sales": float(r[13]),
                "total_commission": float(r[14]),
                "total_services": float(r[15]),
            }
        return {}

    def get_pnl(self, shop_id: int, month: str = None) -> dict:
        """
        Get P&L breakdown.

        Args:
            shop_id: shop identifier
            month: optional 'YYYY-MM' filter

        Returns:
            dict with P&L metrics
        """
        if not self._client:
            return {}

        where = f"shop_id = {{shop_id:UInt32}}"
        if month:
            where += f" AND toYYYYMM(operation_date) = {{month:UInt32}}"

        params = {"shop_id": shop_id}
        if month:
            params["month"] = int(month.replace("-", ""))

        result = self._client.query(f"""
            SELECT
                sumIf(amount, category = 'Revenue') as revenue,
                sum(sale_commission) as commission,
                sumIf(amount, category = 'Logistics') as logistics,
                sumIf(amount, category = 'Marketing') as marketing,
                sumIf(amount, category = 'Storage') as storage,
                sumIf(amount, category = 'Refund') as refunds,
                sumIf(amount, category = 'Penalty') as penalties,
                sumIf(amount, category = 'Acquiring') as acquiring,
                sumIf(amount, category = 'Compensation') as compensation,
                sum(amount) as net_payout
            FROM fact_ozon_transactions
            WHERE {where}
        """, parameters=params)
        r = result.first_row
        if r:
            return {
                "revenue": float(r[0]),
                "commission": float(r[1]),
                "logistics": float(r[2]),
                "marketing": float(r[3]),
                "storage": float(r[4]),
                "refunds": float(r[5]),
                "penalties": float(r[6]),
                "acquiring": float(r[7]),
                "compensation": float(r[8]),
                "net_payout": float(r[9]),
            }
        return {}
