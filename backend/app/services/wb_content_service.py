"""
WB Content Service — Fetch product cards (title, photos, dimensions).

API: POST /content/v2/get/cards/list
Domain: content-api.wildberries.ru

Updates PostgreSQL dim_products: name, main_image_url, dimensions, category.
Frequency: once per day (content rarely changes).
"""
import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient
from app.core.redis_state import RedisStateManager

logger = logging.getLogger(__name__)


class WBContentService:
    """
    Fetches product content cards from WB Content API.
    
    Uses cursor-based pagination (limit=100 per request).
    Extracts: title, main photo URL, dimensions (L/W/H), category.
    """

    ENDPOINT = "/content/v2/get/cards/list"
    PAGE_SIZE = 100

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

    async def fetch_all_cards(self) -> List[Dict[str, Any]]:
        """
        Fetch all product cards with cursor pagination.
        
        Returns:
            List of dicts with: nm_id, title, main_image_url, 
            length, width, height, category
        """
        all_cards = []
        cursor = {"limit": self.PAGE_SIZE}
        total_cursor = None

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_content",
            api_key=self.api_key,
        ) as client:
            while True:
                payload = {
                    "settings": {
                        "cursor": cursor,
                        "filter": {"withPhoto": -1},  # All products
                    }
                }

                if total_cursor:
                    payload["settings"]["cursor"]["updatedAt"] = total_cursor.get("updatedAt")
                    payload["settings"]["cursor"]["nmID"] = total_cursor.get("nmID")

                response = await client.post(self.ENDPOINT, json=payload)

                if not response.is_success:
                    logger.error(
                        f"Content API error: status={response.status_code}, "
                        f"error={response.error}"
                    )
                    break

                data = response.data
                if not isinstance(data, dict):
                    break

                cards = data.get("cards", [])
                if not cards:
                    break

                for card in cards:
                    nm_id = card.get("nmID")
                    if not nm_id:
                        continue

                    # Extract main photo
                    photos = card.get("photos", [])
                    main_image_url = ""
                    if photos and isinstance(photos[0], dict):
                        main_image_url = photos[0].get("big", "") or photos[0].get("tm", "")

                    # Extract dimensions
                    dimensions = card.get("dimensions", {})
                    length = dimensions.get("length", 0) if isinstance(dimensions, dict) else 0
                    width = dimensions.get("width", 0) if isinstance(dimensions, dict) else 0
                    height = dimensions.get("height", 0) if isinstance(dimensions, dict) else 0

                    # Extract category from characteristics
                    category = ""
                    characteristics = card.get("characteristics", [])
                    for char in characteristics:
                        if isinstance(char, dict) and char.get("id") == "Предмет":
                            category = str(char.get("value", ""))
                            break
                    # Fallback: subjectName
                    if not category:
                        category = card.get("subjectName", "")

                    all_cards.append({
                        "nm_id": nm_id,
                        "title": card.get("title", ""),
                        "main_image_url": main_image_url,
                        "length": length,
                        "width": width,
                        "height": height,
                        "category": category,
                    })

                logger.info(
                    f"Fetched {len(cards)} content cards, "
                    f"total so far: {len(all_cards)}"
                )

                # Check cursor for next page
                cursor_resp = data.get("cursor", {})
                if not cursor_resp.get("updatedAt") and not cursor_resp.get("nmID"):
                    break

                total_cursor = cursor_resp
                total = cursor_resp.get("total", 0)
                if len(all_cards) >= total and total > 0:
                    break

        logger.info(f"Total content cards fetched: {len(all_cards)} for shop {self.shop_id}")
        return all_cards

    async def update_products_db(self, cards_data: List[Dict[str, Any]]) -> int:
        """
        Update dim_products with content data.
        
        Returns:
            Number of products updated.
        """
        updated = 0
        for card in cards_data:
            try:
                await self.db.execute(
                    text("""
                        INSERT INTO dim_products (shop_id, nm_id, name, main_image_url, length, width, height, category)
                        VALUES (:shop_id, :nm_id, :name, :image_url, :length, :width, :height, :category)
                        ON CONFLICT (shop_id, nm_id)
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            main_image_url = EXCLUDED.main_image_url,
                            length = CASE WHEN EXCLUDED.length > 0 THEN EXCLUDED.length ELSE dim_products.length END,
                            width = CASE WHEN EXCLUDED.width > 0 THEN EXCLUDED.width ELSE dim_products.width END,
                            height = CASE WHEN EXCLUDED.height > 0 THEN EXCLUDED.height ELSE dim_products.height END,
                            category = CASE WHEN EXCLUDED.category != '' THEN EXCLUDED.category ELSE dim_products.category END,
                            updated_at = NOW()
                    """),
                    {
                        "shop_id": self.shop_id,
                        "nm_id": card["nm_id"],
                        "name": card["title"],
                        "image_url": card["main_image_url"],
                        "length": card["length"],
                        "width": card["width"],
                        "height": card["height"],
                        "category": card["category"],
                    },
                )
                updated += 1
            except Exception as e:
                logger.warning(f"Failed to update product content {card['nm_id']}: {e}")
                continue

        await self.db.commit()
        logger.info(f"Updated {updated} product content entries in dim_products")
        return updated

    def update_redis_image_state(self, cards_data: List[Dict[str, Any]]) -> None:
        """Update Redis image state for CONTENT_CHANGE detection."""
        for card in cards_data:
            if card["main_image_url"]:
                self.state_manager.set_image_url(
                    self.shop_id,
                    card["nm_id"],
                    card["main_image_url"],
                )
        logger.info(f"Updated {len(cards_data)} image URL states in Redis")
