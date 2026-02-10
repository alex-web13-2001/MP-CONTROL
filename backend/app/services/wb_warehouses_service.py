"""
WB Warehouses Service — Sync warehouse dictionary.

API: GET /api/v3/warehouses
Domain: marketplace-api.wildberries.ru (wildberries_marketplace)

Response format per item:
    {
        "name": "Берита",
        "officeId": 218,
        "id": 1126831,
        "cargoType": 1,
        "deliveryType": 1,
        "isDeleting": false,
        "isProcessing": false
    }

Updates PostgreSQL dim_warehouses with verified warehouse data.
Frequency: once per day.
"""
import logging
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)


class WBWarehousesService:
    """
    Syncs seller warehouses from marketplace-api /api/v3/warehouses.

    Marks all warehouses as is_verified=true (as opposed to
    auto-created warehouses from stocks which are unverified).
    """

    ENDPOINT = "/api/v3/warehouses"

    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        api_key: str,
    ):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key

    async def sync_warehouses(self) -> int:
        """
        Fetch all seller warehouses from WB and upsert into dim_warehouses.

        Returns:
            Number of warehouses synced.
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_marketplace",
            api_key=self.api_key,
        ) as client:
            response = await client.get(self.ENDPOINT)

            if not response.is_success:
                logger.error(
                    f"Warehouses API error: status={response.status_code}, "
                    f"error={response.error}"
                )
                return 0

            warehouses = response.data
            if not isinstance(warehouses, list):
                logger.error(f"Unexpected response format: {type(warehouses)}")
                return 0

        synced = 0
        for wh in warehouses:
            # Use 'id' as the primary warehouse_id
            wh_id = wh.get("id")
            name = wh.get("name", "")
            office_id = wh.get("officeId")

            if not wh_id or not name:
                continue

            try:
                await self.db.execute(
                    text("""
                        INSERT INTO dim_warehouses (warehouse_id, name, address, city, is_verified)
                        VALUES (:wh_id, :name, :address, :city, true)
                        ON CONFLICT (warehouse_id)
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            address = EXCLUDED.address,
                            is_verified = true,
                            updated_at = NOW()
                    """),
                    {
                        "wh_id": wh_id,
                        "name": name,
                        "address": f"officeId={office_id}" if office_id else "",
                        "city": "",
                    },
                )
                synced += 1
            except Exception as e:
                logger.warning(f"Failed to upsert warehouse {wh_id} ({name}): {e}")
                continue

        await self.db.commit()
        logger.info(f"Synced {synced} seller warehouses from WB marketplace API")
        return synced
