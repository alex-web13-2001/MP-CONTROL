"""
WB Stocks Service — Fetch FBO + FBS inventory.

FBO (WB warehouses): GET /api/v1/supplier/stocks?dateFrom=...
  Domain: statistics-api.wildberries.ru (wildberries_stats)
FBS (seller warehouses): POST /api/v3/stocks/{warehouseId}
  Domain: marketplace-api.wildberries.ru (wildberries)

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
    Fetches FBO + FBS stock quantities per warehouse from WB.

    FBO: statistics-api GET /api/v1/supplier/stocks?dateFrom=...
    FBS: marketplace-api POST /api/v3/stocks/{warehouseId} with chrtIds
    """

    ENDPOINT_FBO = "/api/v1/supplier/stocks"
    ENDPOINT_FBS_WAREHOUSES = "/api/v3/warehouses"
    ENDPOINT_FBS_STOCKS = "/api/v3/stocks"  # + /{warehouseId}
    ENDPOINT_CARDS = "/content/v2/get/cards/list"

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

        # dateFrom filters by lastChangeDate — use far-past date to get ALL stocks
        date_from = "2019-06-20T00:00:00"

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_stats",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                self.ENDPOINT_FBO,
                params={"dateFrom": date_from},
            )
            data = response.data

            if not isinstance(data, list):
                logger.warning(
                    f"Unexpected stocks response: {type(data)} — {str(data)[:200]}"
                )
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
                # Use quantityFull (includes inWayToClient + inWayFromClient)
                quantity_full = stock_item.get("quantityFull", stock_item.get("quantity", 0))

                all_stocks.append({
                    "nm_id": nm_id,
                    "warehouse_name": warehouse_name,
                    "amount": quantity_full,
                    "supplier_article": stock_item.get("supplierArticle", ""),
                    "price": stock_item.get("Price", 0),
                    "discount": stock_item.get("Discount", 0),
                    "in_way_to_client": stock_item.get("inWayToClient", 0),
                    "in_way_from_client": stock_item.get("inWayFromClient", 0),
                    "quantity_full": quantity_full,
                })

        logger.info(f"Total stocks fetched: {len(all_stocks)} for shop {self.shop_id}")
        return all_stocks

    async def _get_chrt_to_nm_mapping(self) -> Dict[int, int]:
        """
        Build chrtId → nmId mapping from WB Content API.

        Fetches all product cards and extracts chrtId from each size variant.
        Returns: {chrtId: nmId}
        """
        chrt_to_nm: Dict[int, int] = {}

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_content",
            api_key=self.api_key,
        ) as client:
            cursor = {"limit": 100}
            total_fetched = 0

            while True:
                body = {
                    "settings": {
                        "cursor": cursor,
                        "filter": {"withPhoto": -1},
                    }
                }
                response = await client.post(self.ENDPOINT_CARDS, json=body)

                if not response.is_success:
                    logger.error(
                        f"Content API cards error: status={response.status_code}, "
                        f"error={response.error}"
                    )
                    break

                data = response.data
                cards = data.get("cards", []) if isinstance(data, dict) else []
                if not cards:
                    break

                for card in cards:
                    nm_id = card.get("nmID")
                    if not nm_id:
                        continue
                    for size in card.get("sizes", []):
                        chrt_id = size.get("chrtID")
                        if chrt_id:
                            chrt_to_nm[chrt_id] = nm_id

                total_fetched += len(cards)

                # Pagination: check cursor for next page
                cursor_data = data.get("cursor", {})
                if not cursor_data.get("total", 0) or len(cards) < 100:
                    break
                cursor = {
                    "limit": 100,
                    "updatedAt": cursor_data.get("updatedAt", ""),
                    "nmID": cursor_data.get("nmID", 0),
                }

        logger.info(
            f"Built chrtId→nmId mapping: {len(chrt_to_nm)} chrtIds "
            f"for shop {self.shop_id}"
        )
        return chrt_to_nm

    async def fetch_fbs_stocks(self, nm_ids: List[int] = None) -> List[Dict[str, Any]]:
        """
        Fetch FBS stock data from WB Marketplace API.

        Flow:
          1. Get chrtId → nmId mapping from Content API
          2. Get seller warehouses (GET /api/v3/warehouses)
          3. For each warehouse: POST /api/v3/stocks/{warehouseId} with chrtIds
          4. Map chrtId back to nmId and aggregate per nm_id per warehouse

        FBS warehouse names are prefixed with "FBS:" to distinguish from FBO.
        """
        all_fbs_stocks: List[Dict[str, Any]] = []
        nm_ids_set = set(nm_ids) if nm_ids else None

        # Step 1: Get chrtId → nmId mapping
        chrt_to_nm = await self._get_chrt_to_nm_mapping()
        if not chrt_to_nm:
            logger.warning("No chrtId→nmId mapping available, skipping FBS stocks")
            return []

        # Filter chrtIds to only known nm_ids
        if nm_ids_set:
            chrt_ids = [c for c, n in chrt_to_nm.items() if n in nm_ids_set]
        else:
            chrt_ids = list(chrt_to_nm.keys())

        if not chrt_ids:
            logger.info("No chrtIds to query for FBS stocks")
            return []

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_marketplace",
            api_key=self.api_key,
        ) as client:
            # Step 2: Get seller warehouses
            resp_wh = await client.get(self.ENDPOINT_FBS_WAREHOUSES)
            if not resp_wh.is_success:
                logger.error(
                    f"FBS warehouses error: {resp_wh.status_code} {resp_wh.error}"
                )
                return []

            warehouses = resp_wh.data
            if not isinstance(warehouses, list) or not warehouses:
                logger.info("No FBS warehouses found")
                return []

            logger.info(
                f"Found {len(warehouses)} FBS warehouses, "
                f"querying {len(chrt_ids)} chrtIds"
            )

            # Step 3: For each warehouse, fetch stocks
            for wh in warehouses:
                wh_id = wh.get("id")
                wh_name = wh.get("name", "FBS")
                if not wh_id:
                    continue

                # Chunk chrtIds (API might have payload limits)
                chunk_size = 1000
                for i in range(0, len(chrt_ids), chunk_size):
                    chunk = chrt_ids[i:i + chunk_size]
                    resp = await client.post(
                        f"{self.ENDPOINT_FBS_STOCKS}/{wh_id}",
                        json={"chrtIds": chunk},
                    )

                    if not resp.is_success:
                        logger.error(
                            f"FBS stocks error wh={wh_id}: "
                            f"{resp.status_code} {resp.error}"
                        )
                        continue

                    data = resp.data
                    # Response: {"stocks": [{"chrtId": X, "amount": Y, "sku": "..."}]}
                    stocks_list = data.get("stocks", []) if isinstance(data, dict) else []

                    # Aggregate by nm_id (sum amounts for all sizes of same product)
                    nm_amounts: Dict[int, int] = {}
                    for item in stocks_list:
                        chrt_id = item.get("chrtId")
                        amount = item.get("amount", 0)
                        nm_id = chrt_to_nm.get(chrt_id)
                        if nm_id and amount > 0:
                            nm_amounts[nm_id] = nm_amounts.get(nm_id, 0) + amount

                    for nm_id, total_amount in nm_amounts.items():
                        all_fbs_stocks.append({
                            "nm_id": nm_id,
                            "warehouse_name": f"FBS:{wh_name}",
                            "amount": total_amount,
                            "supplier_article": "",
                            "price": 0,
                            "discount": 0,
                            "in_way_to_client": 0,
                            "in_way_from_client": 0,
                            "quantity_full": total_amount,
                        })

        logger.info(
            f"Total FBS stocks: {len(all_fbs_stocks)} items, "
            f"total qty={sum(s['amount'] for s in all_fbs_stocks)} "
            f"for shop {self.shop_id}"
        )
        return all_fbs_stocks

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
