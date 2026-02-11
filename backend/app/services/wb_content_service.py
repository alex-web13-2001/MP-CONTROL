"""
WB Content Service — Fetch product cards (title, photos, dimensions).

API: POST /content/v2/get/cards/list
Domain: content-api.wildberries.ru

Updates PostgreSQL dim_products: name, main_image_url, dimensions, category.
Manages dim_product_content: title/desc/photos hashes for SEO audit.
Frequency: once per day (content rarely changes).
"""
import hashlib
import json
import logging
import re
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient
from app.core.redis_state import RedisStateManager

logger = logging.getLogger(__name__)


def extract_photo_id(url: str) -> str:
    """
    Extract stable file identifier from WB CDN URL.
    
    WB photo URLs look like:
      https://basket-12.wbbasket.ru/vol1856/part185600/185600123/images/c246x328/1.webp
      https://basket-12.wbbasket.ru/vol1856/part185600/185600123/images/big/1.webp
    
    We extract: "vol1856/part185600/185600123/1" as the stable ID.
    CDN domain and size suffix (/c246x328/ vs /big/) can change without actual photo change.
    """
    # Try to extract vol/part/nm/N pattern
    match = re.search(r'(vol\d+/part\d+/\d+)/images/[^/]+/(\d+)', url)
    if match:
        return f"{match.group(1)}/{match.group(2)}"
    
    # Fallback: use last 2 path segments (photo number + parent folder)
    parts = url.rstrip('/').split('/')
    if len(parts) >= 2:
        return '/'.join(parts[-2:])
    
    return url


def compute_hash(value: str) -> str:
    """Compute MD5 hash of a string. Returns 32-char hex digest."""
    return hashlib.md5(value.encode('utf-8')).hexdigest()


class WBContentService:
    """
    Fetches product content cards from WB Content API.
    
    Uses cursor-based pagination (limit=100 per request).
    Extracts: title, description, photos, dimensions, category.
    Computes content hashes for SEO change detection.
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
            List of dicts with: nm_id, title, description, main_image_url,
            photos (full list), photo_ids, length, width, height, category,
            title_hash, description_hash, main_photo_id, photos_hash, photos_count
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

                    # === Extract photos ===
                    photos_raw = card.get("photos", [])
                    photo_urls = []
                    photo_ids = []
                    main_image_url = ""
                    main_photo_id = ""

                    for photo in photos_raw:
                        if isinstance(photo, dict):
                            url = photo.get("big", "") or photo.get("tm", "")
                            if url:
                                photo_urls.append(url)
                                photo_ids.append(extract_photo_id(url))

                    if photo_urls:
                        main_image_url = photo_urls[0]
                        main_photo_id = photo_ids[0]

                    # === Extract text content ===
                    title = card.get("title", "")
                    description = card.get("description", "")

                    # === Extract dimensions ===
                    dimensions = card.get("dimensions", {})
                    length = dimensions.get("length", 0) if isinstance(dimensions, dict) else 0
                    width = dimensions.get("width", 0) if isinstance(dimensions, dict) else 0
                    height = dimensions.get("height", 0) if isinstance(dimensions, dict) else 0

                    # === Extract category ===
                    category = ""
                    characteristics = card.get("characteristics", [])
                    for char in characteristics:
                        if isinstance(char, dict) and char.get("id") == "Предмет":
                            category = str(char.get("value", ""))
                            break
                    if not category:
                        category = card.get("subjectName", "")

                    # === Compute hashes ===
                    title_hash = compute_hash(title) if title else ""
                    desc_hash = compute_hash(description) if description else ""
                    # Sort photo_ids for stable hash (order matters for order change detection)
                    photos_hash = compute_hash(json.dumps(photo_ids)) if photo_ids else ""

                    all_cards.append({
                        "nm_id": nm_id,
                        "title": title,
                        "description": description,
                        "main_image_url": main_image_url,
                        "photo_ids": photo_ids,
                        "photos_count": len(photo_ids),
                        "length": length,
                        "width": width,
                        "height": height,
                        "category": category,
                        # Hashes for change detection
                        "title_hash": title_hash,
                        "description_hash": desc_hash,
                        "main_photo_id": main_photo_id,
                        "photos_hash": photos_hash,
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

    async def upsert_content_hashes(self, cards_data: List[Dict[str, Any]]) -> int:
        """
        Upsert content hashes into dim_product_content.
        
        This creates/updates the "reference snapshot" used for next comparison.
        
        Returns:
            Number of content records upserted.
        """
        upserted = 0
        for card in cards_data:
            try:
                await self.db.execute(
                    text("""
                        INSERT INTO dim_product_content 
                            (shop_id, nm_id, title_hash, description_hash, 
                             main_photo_id, photos_hash, photos_count)
                        VALUES 
                            (:shop_id, :nm_id, :title_hash, :desc_hash,
                             :main_photo_id, :photos_hash, :photos_count)
                        ON CONFLICT (shop_id, nm_id)
                        DO UPDATE SET
                            title_hash = EXCLUDED.title_hash,
                            description_hash = EXCLUDED.description_hash,
                            main_photo_id = EXCLUDED.main_photo_id,
                            photos_hash = EXCLUDED.photos_hash,
                            photos_count = EXCLUDED.photos_count,
                            updated_at = NOW()
                    """),
                    {
                        "shop_id": self.shop_id,
                        "nm_id": card["nm_id"],
                        "title_hash": card["title_hash"],
                        "desc_hash": card["description_hash"],
                        "main_photo_id": card["main_photo_id"],
                        "photos_hash": card["photos_hash"],
                        "photos_count": card["photos_count"],
                    },
                )
                upserted += 1
            except Exception as e:
                logger.warning(f"Failed to upsert content hash for {card['nm_id']}: {e}")
                continue

        await self.db.commit()
        logger.info(f"Upserted {upserted} content hashes in dim_product_content")
        return upserted

    def update_redis_image_state(self, cards_data: List[Dict[str, Any]]) -> None:
        """Update Redis image state for CONTENT_CHANGE detection."""
        for card in cards_data:
            if card["main_image_url"]:
                self.state_manager.set_image_url(
                    self.shop_id,
                    card["nm_id"],
                    card["main_image_url"],
                )
            # Also store content hashes in Redis
            self.state_manager.set_content_hash(
                self.shop_id,
                card["nm_id"],
                title_hash=card["title_hash"],
                desc_hash=card["description_hash"],
                photos_hash=card["photos_hash"],
                main_photo_id=card["main_photo_id"],
            )
        logger.info(f"Updated {len(cards_data)} content states in Redis")
