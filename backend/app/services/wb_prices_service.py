"""
WB Prices Service — Fetch product prices and discounts.

API: GET /api/v2/list/goods/filter
Domain: discounts-prices-api.wildberries.ru

Updates:
  - PostgreSQL dim_products: current_price, current_discount, vendor_code
  - Redis: state:price:{shop_id}:{nm_id}
  - Returns data for ClickHouse fact_inventory_snapshot
"""
import logging
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient
from app.core.redis_state import RedisStateManager

logger = logging.getLogger(__name__)


class WBPricesService:
    """
    Fetches prices and discounts from WB discounts-prices-api.
    
    Flow:
        1. Paginate through /api/v2/list/goods/filter (limit=1000)
        2. For each product: update dim_products, Redis state
        3. Return price data for ClickHouse snapshot insertion
    """

    ENDPOINT = "/api/v2/list/goods/filter"
    PAGE_SIZE = 1000

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

    async def fetch_all_prices(self) -> List[Dict[str, Any]]:
        """
        Fetch all product prices with pagination.
        
        Returns:
            List of dicts with keys: nm_id, vendor_code, price, discount, converted_price
        """
        all_goods = []
        offset = 0

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_prices",
            api_key=self.api_key,
        ) as client:
            while True:
                params = {
                    "limit": self.PAGE_SIZE,
                    "offset": offset,
                }
                response = await client.get(self.ENDPOINT, params=params)

                if not response.is_success:
                    logger.error(
                        f"Prices API error: status={response.status_code}, "
                        f"error={response.error}"
                    )
                    break

                data = response.data
                if not data:
                    break

                # Extract listGoods from response
                list_goods = []
                if isinstance(data, dict):
                    list_goods = data.get("data", {}).get("listGoods", [])
                
                if not list_goods:
                    break

                for item in list_goods:
                    nm_id = item.get("nmID")
                    vendor_code = item.get("vendorCode", "")
                    sizes = item.get("sizes", [])
                    # discount is at item level, not in sizes
                    discount = item.get("discount", 0)

                    if not nm_id or not sizes:
                        continue

                    # Take first size (primary)
                    size = sizes[0]
                    price = size.get("price", 0)
                    discounted_price = size.get("discountedPrice", 0)

                    all_goods.append({
                        "nm_id": nm_id,
                        "vendor_code": vendor_code,
                        "price": price,
                        "discount": discount,
                        "converted_price": discounted_price,  # actual selling price
                    })

                logger.info(
                    f"Fetched {len(list_goods)} prices (offset={offset}), "
                    f"total so far: {len(all_goods)}"
                )

                # Check if there are more pages
                if len(list_goods) < self.PAGE_SIZE:
                    break

                offset += self.PAGE_SIZE

        logger.info(f"Total prices fetched: {len(all_goods)} for shop {self.shop_id}")
        return all_goods

    async def update_products_db(self, prices_data: List[Dict[str, Any]]) -> int:
        """
        Upsert product prices into dim_products.
        
        Returns:
            Number of products updated.
        """
        updated = 0
        for item in prices_data:
            try:
                await self.db.execute(
                    text("""
                        INSERT INTO dim_products (shop_id, nm_id, vendor_code, current_price, current_discount)
                        VALUES (:shop_id, :nm_id, :vendor_code, :price, :discount)
                        ON CONFLICT (shop_id, nm_id)
                        DO UPDATE SET
                            vendor_code = COALESCE(EXCLUDED.vendor_code, dim_products.vendor_code),
                            current_price = EXCLUDED.current_price,
                            current_discount = EXCLUDED.current_discount,
                            updated_at = NOW()
                    """),
                    {
                        "shop_id": self.shop_id,
                        "nm_id": item["nm_id"],
                        "vendor_code": item["vendor_code"],
                        "price": item["converted_price"],
                        "discount": item["discount"],
                    },
                )
                updated += 1
            except Exception as e:
                logger.warning(f"Failed to upsert product {item['nm_id']}: {e}")
                continue

        await self.db.commit()
        logger.info(f"Updated {updated} products in dim_products")
        return updated

    def update_redis_state(self, prices_data: List[Dict[str, Any]]) -> None:
        """Update Redis price state for event detection."""
        for item in prices_data:
            self.state_manager.set_price(
                self.shop_id,
                item["nm_id"],
                float(item["converted_price"]),
            )
        logger.info(f"Updated {len(prices_data)} price states in Redis")

    def prepare_snapshot_rows(
        self, prices_data: List[Dict[str, Any]], fetched_at: datetime
    ) -> List[Dict[str, Any]]:
        """
        Prepare rows for ClickHouse fact_inventory_snapshot (price portion).
        
        NOTE: These rows have no warehouse/quantity info — that comes from stocks.
        Price-only rows use warehouse_name='' and quantity=0.
        """
        rows = []
        for item in prices_data:
            rows.append({
                "fetched_at": fetched_at,
                "shop_id": self.shop_id,
                "nm_id": item["nm_id"],
                "warehouse_name": "",  # No warehouse for price-only rows
                "warehouse_id": 0,
                "quantity": 0,
                "price": Decimal(str(item["converted_price"])),
                "discount": item["discount"],
            })
        return rows
