"""
WB Stocks Service — Fetch FBO inventory by warehouse.

API: GET /api/v1/supplier/stocks?dateFrom=...
Domain: statistics-api.wildberries.ru (wildberries_stats)

Response format per item:
    {
        "lastChangeDate": "2026-02-01T21:28:15",
        "warehouseName": "Волгоград",
        "supplierArticle": "B_ACTIV2",
        "nmId": 467259691,
        "barcode": "4657800130268",
        "quantity": 8,
        "inWayToClient": 0,
        "inWayFromClient": 0,
        "quantityFull": 8,
        "category": "...",
        "subject": "...",
        "brand": "...",
        "techSize": "0",
        "Price": 3500,
        "Discount": 57,
        "isSupply": true,
        "isRealization": false,
        "SCCode": "Tech"
    }

Updates:
  - Redis: state:stock:{shop_id}:{nm_id}:{warehouse}
  - Auto-creates unknown warehouses in dim_warehouses
  - Returns data for ClickHouse fact_inventory_snapshot
"""
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Set

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient
from app.core.redis_state import RedisStateManager

logger = logging.getLogger(__name__)


class WBStocksService:
    """
    Fetches FBO stock quantities per warehouse from WB.

    Uses statistics-api GET /api/v1/supplier/stocks?dateFrom=...
    Returns all stocks in one request (no need for chunking).
    """

    ENDPOINT = "/api/v1/supplier/stocks"

    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        api_key: str,
        redis_url: str = "redis://redis:6379/0",
    ):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key
        self.state_manager = RedisStateManager(redis_url)

    async def get_product_nm_ids(self) -> List[int]:
        """Get all nmIds for this shop from dim_products."""
        result = await self.db.execute(
            text("SELECT nm_id FROM dim_products WHERE shop_id = :shop_id"),
            {"shop_id": self.shop_id},
        )
        return [row[0] for row in result.fetchall()]

    async def fetch_stocks(self, nm_ids: List[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch stock data from WB Statistics API.

        This API returns all stocks at once (no need for nm_id chunking).
        We filter by nm_ids from dim_products if provided.

        Returns:
            List of dicts: {nm_id, warehouse_name, amount, supplier_article,
                           price, discount, in_way_to_client, in_way_from_client}
        """
        all_stocks = []

        # dateFrom = 1 day ago to get recent data
        date_from = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_stats",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                self.ENDPOINT,
                params={"dateFrom": date_from},
            )

            if not response.is_success:
                logger.error(
                    f"Stocks API error: status={response.status_code}, "
                    f"error={response.error}"
                )
                return []

            data = response.data
            if not isinstance(data, list):
                logger.error(f"Stocks API returned unexpected format: {type(data)}")
                return []

            # nm_ids filter set (if provided — keep only known products)
            nm_ids_set = set(nm_ids) if nm_ids else None

            for stock_item in data:
                nm_id = stock_item.get("nmId")
                if not nm_id:
                    continue

                # Filter by known products if we have them
                if nm_ids_set and nm_id not in nm_ids_set:
                    continue

                warehouse_name = stock_item.get("warehouseName", "Unknown")
                quantity = stock_item.get("quantity", 0)

                all_stocks.append({
                    "nm_id": nm_id,
                    "warehouse_name": warehouse_name,
                    "amount": quantity,
                    "supplier_article": stock_item.get("supplierArticle", ""),
                    "price": stock_item.get("Price", 0),
                    "discount": stock_item.get("Discount", 0),
                    "in_way_to_client": stock_item.get("inWayToClient", 0),
                    "in_way_from_client": stock_item.get("inWayFromClient", 0),
                    "quantity_full": stock_item.get("quantityFull", quantity),
                })

        logger.info(f"Total stocks fetched: {len(all_stocks)} for shop {self.shop_id}")
        return all_stocks

    async def ensure_warehouses(self, stocks_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        Ensure all warehouse names exist in dim_warehouses.
        Auto-creates unverified entries for unknown warehouses.

        Returns:
            Mapping of warehouse_name -> warehouse_id
        """
        # Get unique warehouse names
        warehouse_names: Set[str] = {s["warehouse_name"] for s in stocks_data}

        # Get existing warehouses
        result = await self.db.execute(
            text("SELECT warehouse_id, name FROM dim_warehouses"),
        )
        existing = {row[1]: row[0] for row in result.fetchall()}

        # Find and create missing warehouses
        name_to_id = {}
        next_id = max(existing.values(), default=0) + 1

        for name in warehouse_names:
            if name in existing:
                name_to_id[name] = existing[name]
            else:
                # Auto-create with temporary ID, is_verified=false
                temp_id = next_id
                next_id += 1
                try:
                    await self.db.execute(
                        text("""
                            INSERT INTO dim_warehouses (warehouse_id, name, is_verified)
                            VALUES (:wh_id, :name, false)
                            ON CONFLICT (warehouse_id) DO NOTHING
                        """),
                        {"wh_id": temp_id, "name": name},
                    )
                    name_to_id[name] = temp_id
                    logger.info(f"Auto-created warehouse: {name} (id={temp_id}, unverified)")
                except Exception as e:
                    logger.warning(f"Failed to create warehouse {name}: {e}")
                    name_to_id[name] = 0

        await self.db.commit()
        return name_to_id

    def update_redis_state(self, stocks_data: List[Dict[str, Any]]) -> None:
        """Update Redis stock state for event detection."""
        for item in stocks_data:
            self.state_manager.set_stock(
                self.shop_id,
                item["nm_id"],
                item["warehouse_name"],
                item["amount"],
            )
        logger.info(f"Updated {len(stocks_data)} stock states in Redis")

    def prepare_snapshot_rows(
        self,
        stocks_data: List[Dict[str, Any]],
        warehouse_map: Dict[str, int],
        prices_map: Dict[int, Dict],
        fetched_at: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Prepare rows for ClickHouse fact_inventory_snapshot.

        Uses price from stocks API if available (field Price),
        falls back to prices_map from prices service.
        """
        rows = []
        for item in stocks_data:
            nm_id = item["nm_id"]
            price_info = prices_map.get(nm_id, {})

            # Prefer price from stocks API, fallback to prices service
            price = item.get("price", 0) or price_info.get("converted_price", 0)
            discount = item.get("discount", 0) or price_info.get("discount", 0)

            rows.append({
                "fetched_at": fetched_at,
                "shop_id": self.shop_id,
                "nm_id": nm_id,
                "warehouse_name": item["warehouse_name"],
                "warehouse_id": warehouse_map.get(item["warehouse_name"], 0),
                "quantity": item["amount"],
                "price": Decimal(str(price)),
                "discount": discount,
            })
        return rows
