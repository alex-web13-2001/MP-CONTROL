"""
Event Detection Service for Advertising Module.

Detects changes in campaign settings and logs them to PostgreSQL event_log.

CRITICAL: Implements event debouncing to avoid garbage events from API "storms".
"""
import logging
from typing import Dict, Any, List, Optional, Tuple, Set
from decimal import Decimal
from datetime import datetime

from app.core.redis_state import RedisStateManager
from app.models.event_log import EventLog

logger = logging.getLogger(__name__)


class EventDetector:
    """
    Detects advertising events by comparing current API state with Redis cache.
    
    Events detected:
    - BID_CHANGE: CPM/CPC changed (with debouncing)
    - STATUS_CHANGE: Campaign paused/started
    - ITEM_ADD: New item added to campaign
    - ITEM_REMOVE: Item removed from campaign
    - ITEM_INACTIVE: Item stopped receiving views (still in campaign but inactive)
    
    DEBOUNCING RULES:
    - BID_CHANGE: Only if new_value is valid number > 0
    - STATUS_CHANGE: Only if new status is valid (not None)
    - ITEM events: Only on actual set difference
    """
    
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.state_manager = RedisStateManager(redis_url)
    
    def detect_changes(
        self,
        shop_id: int,
        campaign_settings: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Detect changes for multiple campaigns with debouncing.
        
        Args:
            shop_id: Shop ID
            campaign_settings: Response from POST /adv/v1/promotion/adverts
            
        Returns:
            List of event dicts ready for PostgreSQL insertion
        """
        events = []
        
        for campaign in campaign_settings:
            try:
                advert_id = int(campaign.get("advertId", 0))
                if not advert_id:
                    continue
                
                # Parse current values with validation
                raw_status = campaign.get("status")
                campaign_type = campaign.get("type")
                
                # Extract CPM from unitedParams (new format as of Oct 2025)
                raw_cpm = self._extract_cpm(campaign)
                
                # DEBOUNCING: Skip if API returned garbage (None, empty, or 0 for CPM)
                current_cpm = None
                if raw_cpm is not None and raw_cpm > 0:
                    current_cpm = Decimal(str(raw_cpm))
                
                current_status = None
                if raw_status is not None and raw_status != "":
                    current_status = int(raw_status)
                
                # Extract item list from params
                current_items = self._extract_items(campaign)
                
                # Get last state from Redis (single HGETALL)
                old_state = self.state_manager.get_state(shop_id, advert_id)
                old_cpm = old_state.get("cpm")
                old_status = old_state.get("status")
                
                # ===== Detect BID_CHANGE (with debouncing) =====
                if current_cpm is not None:  # Only if we got valid CPM
                    if old_cpm is not None and float(current_cpm) != old_cpm:
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": advert_id,
                            "nm_id": None,
                            "event_type": "BID_CHANGE",
                            "old_value": str(old_cpm),
                            "new_value": str(current_cpm),
                            "event_metadata": {"campaign_type": campaign_type}
                        })
                        logger.info(f"Detected BID_CHANGE: advert={advert_id} {old_cpm} -> {current_cpm}")
                
                # ===== Detect STATUS_CHANGE =====
                if current_status is not None:  # Only if we got valid status
                    if old_status is not None and current_status != old_status:
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": advert_id,
                            "nm_id": None,
                            "event_type": "STATUS_CHANGE",
                            "old_value": str(old_status),
                            "new_value": str(current_status),
                            "event_metadata": None
                        })
                        logger.info(f"Detected STATUS_CHANGE: advert={advert_id} {old_status} -> {current_status}")
                
                # ===== Detect ITEM_ADD / ITEM_REMOVE =====
                old_items = set(old_state.get("items") or [])
                current_items_set = set(current_items)
                
                added_items = current_items_set - old_items
                removed_items = old_items - current_items_set
                
                for nm_id in added_items:
                    events.append({
                        "shop_id": shop_id,
                        "advert_id": advert_id,
                        "nm_id": nm_id,
                        "event_type": "ITEM_ADD",
                        "old_value": None,
                        "new_value": str(nm_id),
                        "event_metadata": None
                    })
                    logger.info(f"Detected ITEM_ADD: advert={advert_id} nm={nm_id}")
                
                for nm_id in removed_items:
                    events.append({
                        "shop_id": shop_id,
                        "advert_id": advert_id,
                        "nm_id": nm_id,
                        "event_type": "ITEM_REMOVE",
                        "old_value": str(nm_id),
                        "new_value": None,
                        "event_metadata": None
                    })
                    logger.info(f"Detected ITEM_REMOVE: advert={advert_id} nm={nm_id}")
                
                # ===== Update Redis state (only with valid values) =====
                self.state_manager.set_state(
                    shop_id, advert_id,
                    cpm=float(current_cpm) if current_cpm is not None else None,
                    status=current_status,
                    items=current_items if current_items else None,
                    campaign_type=campaign_type
                )
            
            except Exception as e:
                logger.warning(f"Error processing campaign {campaign.get('advertId')}: {e}")
                continue
        
        logger.info(f"Detected {len(events)} events total")
        return events
    
    def detect_inactive_items(
        self,
        shop_id: int,
        advert_id: int,
        campaign_status: int,
        official_items: Set[int],
        stats_items: Dict[int, int]  # nm_id -> views
    ) -> List[Dict[str, Any]]:
        """
        Detect ITEM_INACTIVE events.
        
        An item is INACTIVE when:
        1. Campaign is active (status in [9, 11])
        2. Item is in official campaign list
        3. Item had views before but now has 0 views
        """
        events = []
        
        # Only check for active campaigns
        if campaign_status not in [9, 11]:
            return events
        
        for nm_id in official_items:
            current_views = stats_items.get(nm_id, 0)
            old_views = self.state_manager.get_last_views(shop_id, advert_id, nm_id)
            
            # ITEM_INACTIVE: Had views before, now has 0
            if old_views is not None and old_views > 0 and current_views == 0:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": advert_id,
                    "nm_id": nm_id,
                    "event_type": "ITEM_INACTIVE",
                    "old_value": str(old_views),
                    "new_value": "0",
                    "event_metadata": {"reason": "views_dropped_to_zero"}
                })
                logger.info(f"Detected ITEM_INACTIVE: advert={advert_id} nm={nm_id} views {old_views} -> 0")
            
            # Update last views
            self.state_manager.set_last_views(shop_id, advert_id, nm_id, current_views)
        
        return events
    
    def _extract_items(self, campaign: Dict[str, Any]) -> List[int]:
        """Extract list of nm_ids from campaign params/unitedParams.
        
        Ensures all returned items are integers, not dicts.
        """
        items = []
        
        # New format (Oct 2025): unitedParams array
        united_params = campaign.get("unitedParams", [])
        if united_params:
            for up in united_params:
                if isinstance(up, dict):
                    nms = up.get("nms", [])
                    for nm in nms:
                        if isinstance(nm, int):
                            items.append(nm)
                        elif isinstance(nm, dict) and "nm" in nm:
                            items.append(int(nm["nm"]))
            return items
        
        # Legacy format: params array
        params = campaign.get("params", [])
        for param in params:
            if not isinstance(param, dict):
                continue
            nms = param.get("nms", [])
            for nm in nms:
                if isinstance(nm, int):
                    items.append(nm)
            menus = param.get("menus", [])
            for menu in menus:
                if isinstance(menu, dict):
                    for nm in menu.get("nms", []):
                        if isinstance(nm, int):
                            items.append(nm)
        
        return items
    
    def _extract_cpm(self, campaign: Dict[str, Any]) -> Optional[float]:
        """Extract CPM/bid from campaign settings."""
        # New format: unitedParams[0].searchCPM or catalogCPM
        united_params = campaign.get("unitedParams", [])
        if united_params and isinstance(united_params[0], dict):
            up = united_params[0]
            search_cpm = up.get("searchCPM", 0)
            catalog_cpm = up.get("catalogCPM", 0)
            # Return the higher bid as the active one
            return max(search_cpm, catalog_cpm)
        
        # Legacy format: top-level cpm
        return campaign.get("cpm")
    
    def extract_all_campaign_data(
        self,
        campaign_settings: List[Dict[str, Any]]
    ) -> Tuple[Dict[int, List[int]], Dict[int, Decimal], Dict[int, int]]:
        """
        Extract campaign items, CPM values, and types for history parsing.
        
        Returns:
            Tuple of (campaign_items, cpm_values, campaign_types)
        """
        campaign_items = {}
        cpm_values = {}
        campaign_types = {}
        
        for campaign in campaign_settings:
            try:
                advert_id = int(campaign.get("advertId", 0))
                campaign_items[advert_id] = self._extract_items(campaign)
                
                raw_cpm = self._extract_cpm(campaign) or 0
                cpm_values[advert_id] = Decimal(str(raw_cpm))
                
                campaign_types[advert_id] = int(campaign.get("type", 0))
            except Exception as e:
                logger.warning(f"Error extracting campaign data: {e}")
                continue
        
        return campaign_items, cpm_values, campaign_types


class CommercialEventDetector:
    """
    Detects commercial events by comparing current data with Redis cache.
    
    Events detected:
    - PRICE_CHANGE: Product price changed
    - STOCK_OUT: Stock dropped to 0 at a warehouse (was > 0)
    - STOCK_REPLENISH: Stock increased by 50+ units in one jump
    - CONTENT_CHANGE: Main product image URL changed
    - ITEM_INACTIVE: All stocks = 0 but ad campaign is active
    
    IMPORTANT: Separated from EventDetector to keep ad logic independent.
    """

    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.state_manager = RedisStateManager(redis_url)

    def detect_price_changes(
        self,
        shop_id: int,
        prices_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Detect PRICE_CHANGE events.
        
        Compares current convertedPrice with Redis state:price:{shop_id}:{nm_id}.
        """
        events = []

        for item in prices_data:
            nm_id = item["nm_id"]
            current_price = float(item["converted_price"])

            if current_price <= 0:
                continue

            old_price = self.state_manager.get_price(shop_id, nm_id)

            if old_price is not None and old_price != current_price:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,  # Not ad-related
                    "nm_id": nm_id,
                    "event_type": "PRICE_CHANGE",
                    "old_value": str(old_price),
                    "new_value": str(current_price),
                    "event_metadata": {
                        "vendor_code": item.get("vendor_code", ""),
                        "discount": item.get("discount", 0),
                    },
                })
                logger.info(
                    f"Detected PRICE_CHANGE: nm={nm_id} "
                    f"{old_price} -> {current_price}"
                )

        logger.info(f"Detected {len(events)} PRICE_CHANGE events")
        return events

    def detect_stock_events(
        self,
        shop_id: int,
        stocks_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Detect STOCK_OUT and STOCK_REPLENISH events.
        
        STOCK_OUT:      old > 0  AND  new == 0
        STOCK_REPLENISH: new - old >= 50  (large restock jump)
        """
        events = []
        REPLENISH_THRESHOLD = 50

        for item in stocks_data:
            nm_id = item["nm_id"]
            warehouse = item["warehouse_name"]
            current_qty = item["amount"]

            old_qty = self.state_manager.get_stock(shop_id, nm_id, warehouse)

            if old_qty is None:
                # First data point — skip comparison
                continue

            # STOCK_OUT: was in stock, now gone
            if old_qty > 0 and current_qty == 0:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "STOCK_OUT",
                    "old_value": str(old_qty),
                    "new_value": "0",
                    "event_metadata": {"warehouse_name": warehouse},
                })
                logger.info(
                    f"Detected STOCK_OUT: nm={nm_id} "
                    f"warehouse={warehouse} ({old_qty} -> 0)"
                )

            # STOCK_REPLENISH: large restock jump
            elif current_qty - old_qty >= REPLENISH_THRESHOLD:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "STOCK_REPLENISH",
                    "old_value": str(old_qty),
                    "new_value": str(current_qty),
                    "event_metadata": {
                        "warehouse_name": warehouse,
                        "delta": current_qty - old_qty,
                    },
                })
                logger.info(
                    f"Detected STOCK_REPLENISH: nm={nm_id} "
                    f"warehouse={warehouse} ({old_qty} -> {current_qty})"
                )

        logger.info(
            f"Detected {len([e for e in events if e['event_type'] == 'STOCK_OUT'])} STOCK_OUT "
            f"and {len([e for e in events if e['event_type'] == 'STOCK_REPLENISH'])} STOCK_REPLENISH events"
        )
        return events

    def detect_content_changes(
        self,
        shop_id: int,
        cards_data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Detect CONTENT_CHANGE events (main image URL changed).
        """
        events = []

        for card in cards_data:
            nm_id = card["nm_id"]
            current_url = card.get("main_image_url", "")

            if not current_url:
                continue

            old_url = self.state_manager.get_image_url(shop_id, nm_id)

            if old_url is not None and old_url != current_url:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "CONTENT_CHANGE",
                    "old_value": old_url,
                    "new_value": current_url,
                    "event_metadata": {"title": card.get("title", "")},
                })
                logger.info(f"Detected CONTENT_CHANGE: nm={nm_id}")

        logger.info(f"Detected {len(events)} CONTENT_CHANGE events")
        return events

    def detect_inactive_ads_by_stock(
        self,
        shop_id: int,
        stocks_data: List[Dict[str, Any]],
        active_campaign_items: Dict[int, List[int]],
    ) -> List[Dict[str, Any]]:
        """
        Detect ITEM_INACTIVE events: all warehouse stocks = 0 but ad is running.
        
        Args:
            stocks_data: Current stock data
            active_campaign_items: {advert_id: [nm_id, ...]} for active campaigns
        """
        events = []

        # Build total stock per nm_id across all warehouses
        total_stock: Dict[int, int] = {}
        for item in stocks_data:
            nm_id = item["nm_id"]
            total_stock[nm_id] = total_stock.get(nm_id, 0) + item["amount"]

        # Check each active campaign's items
        for advert_id, nm_ids in active_campaign_items.items():
            for nm_id in nm_ids:
                stock = total_stock.get(nm_id, 0)
                if stock == 0:
                    events.append({
                        "shop_id": shop_id,
                        "advert_id": advert_id,
                        "nm_id": nm_id,
                        "event_type": "ITEM_INACTIVE",
                        "old_value": None,
                        "new_value": "0",
                        "event_metadata": {
                            "reason": "zero_stock_all_warehouses",
                        },
                    })
                    logger.info(
                        f"Detected ITEM_INACTIVE (zero stock): "
                        f"advert={advert_id} nm={nm_id}"
                    )

        logger.info(f"Detected {len(events)} ITEM_INACTIVE events (zero stock)")
        return events


class ContentEventDetector:
    """
    Detects content/SEO events by comparing current card data with
    the reference hashes stored in dim_product_content (PostgreSQL).
    
    Events detected:
    - CONTENT_TITLE_CHANGED: Title text changed (affects SEO/organic)
    - CONTENT_DESC_CHANGED: Description changed (affects SEO)
    - CONTENT_MAIN_PHOTO_CHANGED: Main photo replaced (affects CTR)
    - CONTENT_PHOTO_ORDER_CHANGED: Photos added/removed/reordered (affects CR)
    
    IMPORTANT: Comparison uses PostgreSQL (not Redis) because content
    is checked once per day. Redis is used as supplementary cache.
    """

    def detect_content_events(
        self,
        shop_id: int,
        cards_data: List[Dict[str, Any]],
        existing_hashes: Dict[int, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Compare current card hashes with stored reference hashes.
        
        Args:
            shop_id: Shop ID
            cards_data: Fresh cards from WBContentService.fetch_all_cards()
            existing_hashes: {nm_id: {title_hash, description_hash, 
                             main_photo_id, photos_hash, photos_count}}
                             from dim_product_content
        
        Returns:
            List of event dicts ready for event_log insertion.
        """
        events = []

        for card in cards_data:
            nm_id = card["nm_id"]
            old = existing_hashes.get(nm_id)

            if not old:
                # First time seeing this product — no comparison possible
                continue

            # === CONTENT_TITLE_CHANGED ===
            if (
                card["title_hash"]
                and old.get("title_hash")
                and card["title_hash"] != old["title_hash"]
            ):
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "CONTENT_TITLE_CHANGED",
                    "old_value": old["title_hash"],
                    "new_value": card["title_hash"],
                    "event_metadata": {
                        "new_title": card.get("title", "")[:200],  # Truncate for metadata
                    },
                })
                logger.info(f"Detected CONTENT_TITLE_CHANGED: nm={nm_id}")

            # === CONTENT_DESC_CHANGED ===
            if (
                card["description_hash"]
                and old.get("description_hash")
                and card["description_hash"] != old["description_hash"]
            ):
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "CONTENT_DESC_CHANGED",
                    "old_value": old["description_hash"],
                    "new_value": card["description_hash"],
                    "event_metadata": None,
                })
                logger.info(f"Detected CONTENT_DESC_CHANGED: nm={nm_id}")

            # === CONTENT_MAIN_PHOTO_CHANGED ===
            # Most important for CTR! Uses photo_id (not full URL)
            if (
                card["main_photo_id"]
                and old.get("main_photo_id")
                and card["main_photo_id"] != old["main_photo_id"]
            ):
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "CONTENT_MAIN_PHOTO_CHANGED",
                    "old_value": old["main_photo_id"],
                    "new_value": card["main_photo_id"],
                    "event_metadata": {
                        "old_count": old.get("photos_count", 0),
                        "new_count": card["photos_count"],
                    },
                })
                logger.info(f"Detected CONTENT_MAIN_PHOTO_CHANGED: nm={nm_id}")

            # === CONTENT_PHOTO_ORDER_CHANGED ===
            # Detects added/removed/reordered secondary photos (affects CR)
            # Only fires if main photo is unchanged (otherwise MAIN_PHOTO_CHANGED covers it)
            elif (
                card["photos_hash"]
                and old.get("photos_hash")
                and card["photos_hash"] != old["photos_hash"]
            ):
                events.append({
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": nm_id,
                    "event_type": "CONTENT_PHOTO_ORDER_CHANGED",
                    "old_value": old["photos_hash"],
                    "new_value": card["photos_hash"],
                    "event_metadata": {
                        "old_count": old.get("photos_count", 0),
                        "new_count": card["photos_count"],
                    },
                })
                logger.info(f"Detected CONTENT_PHOTO_ORDER_CHANGED: nm={nm_id}")

        logger.info(
            f"Content audit: {len(events)} events detected "
            f"({len(cards_data)} products checked)"
        )
        return events
