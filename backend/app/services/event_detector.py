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
        [LEGACY] Detect changes using V1 format (/adv/v1/promotion/adverts).
        
        DEPRECATED: Use detect_changes_v2() with /api/advert/v2/adverts data instead.
        
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
    
    def _extract_cpm_separate(self, campaign: Dict[str, Any]) -> Tuple[float, float]:
        """Extract search_cpm and catalog_cpm separately."""
        united_params = campaign.get("unitedParams", [])
        if united_params and isinstance(united_params[0], dict):
            up = united_params[0]
            search_cpm = float(up.get("searchCPM", 0) or 0)
            catalog_cpm = float(up.get("catalogCPM", 0) or 0)
            return search_cpm, catalog_cpm
        
        # Legacy: single cpm value → treat as search
        legacy_cpm = float(campaign.get("cpm", 0) or 0)
        return legacy_cpm, 0.0
    
    def _extract_cpc_price(self, campaign: Dict[str, Any]) -> float:
        """
        Extract CPC bid (price) from campaign settings.
        
        WB stores CPC bid in unitedParams[].subject.price for CPC campaigns,
        or in params[].price for legacy format.
        """
        united_params = campaign.get("unitedParams", [])
        if united_params:
            for up in united_params:
                if isinstance(up, dict):
                    # subject contains {id, name, price} for CPC
                    subject = up.get("subject", {})
                    if isinstance(subject, dict):
                        price = subject.get("price", 0)
                        if price:
                            return float(price)
                    # Also try direct price field
                    price = up.get("price", 0)
                    if price:
                        return float(price)
        
        # Legacy format
        params = campaign.get("params", [])
        for param in params:
            if isinstance(param, dict):
                price = param.get("price", 0)
                if price:
                    return float(price)
        
        return 0.0
    
    def extract_all_campaign_data(
        self,
        campaign_settings: List[Dict[str, Any]]
    ) -> Tuple[Dict[int, List[int]], Dict[int, Decimal], Dict[int, int]]:
        """
        [LEGACY] Extract campaign data from V1 format (/adv/v1/promotion/adverts).
        
        DEPRECATED: Use extract_all_campaign_data_v2() with V2 API data instead.
        
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
    
    def extract_bid_snapshot_v2(
        self,
        shop_id: int,
        adverts_v2: List[Dict[str, Any]]
    ) -> List[tuple]:
        """
        Extract bid snapshot rows from V2 API response for log_wb_bids.
        
        V2 format: each advert has nm_settings[] with bids_kopecks per nm_id.
        All fields are None-safe (API may return None for any field).
        
        Returns list of tuples:
            (shop_id, advert_id, nm_id, bid_type, payment_type,
             bid_search, bid_recommendations, search_enabled,
             recommendations_enabled, status)
        """
        rows = []
        
        for advert in adverts_v2:
            try:
                advert_id = int(advert.get("id") or 0)
                if not advert_id:
                    continue
                
                bid_type = str(advert.get("bid_type") or "")
                status = int(advert.get("status") or 0)
                
                settings = advert.get("settings") or {}
                payment_type = str(settings.get("payment_type") or "")
                placements = settings.get("placements") or {}
                search_enabled = 1 if placements.get("search") else 0
                recommendations_enabled = 1 if placements.get("recommendations") else 0
                
                nm_settings = advert.get("nm_settings") or []
                for nm_setting in nm_settings:
                    if not isinstance(nm_setting, dict):
                        continue
                    nm_id = int(nm_setting.get("nm_id") or 0)
                    if not nm_id:
                        continue
                    
                    bids = nm_setting.get("bids_kopecks") or {}
                    bid_search = int(bids.get("search") or 0)
                    bid_recommendations = int(bids.get("recommendations") or 0)
                    
                    rows.append((
                        shop_id,
                        advert_id,
                        nm_id,
                        bid_type,
                        payment_type,
                        bid_search,
                        bid_recommendations,
                        search_enabled,
                        recommendations_enabled,
                        status,
                    ))
            except Exception as e:
                logger.warning(f"Error extracting V2 bid snapshot for advert {advert.get('id')}: {e}")
                continue
        
        return rows

    def detect_changes_v2(
        self,
        shop_id: int,
        adverts_v2: List[Dict[str, Any]],
        campaign_type_map: Dict[int, int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Detect changes using V2 API format (/api/advert/v2/adverts).
        
        Improvements over V1:
        - BID_CHANGE detected per nm_id (V1 was per campaign CPM)
        - Uses bids_kopecks (search/recommendations) instead of CPM
        - Direct access to payment_type, bid_type, placements
        
        Args:
            shop_id: Shop ID
            adverts_v2: Response from GET /api/advert/v2/adverts
            campaign_type_map: advert_id -> type (from /adv/v1/promotion/count)
            
        Returns:
            List of event dicts ready for PostgreSQL insertion
        """
        events = []
        type_map = campaign_type_map or {}
        
        for advert in adverts_v2:
            try:
                advert_id = int(advert.get("id") or 0)
                if not advert_id:
                    continue
                
                # Parse current values
                status = int(advert.get("status") or 0)
                campaign_type = type_map.get(advert_id, 0)
                
                # ===== STATUS_CHANGE =====
                old_state = self.state_manager.get_state(shop_id, advert_id)
                old_status = old_state.get("status")
                
                if old_status is not None and status != old_status:
                    events.append({
                        "shop_id": shop_id,
                        "advert_id": advert_id,
                        "nm_id": None,
                        "event_type": "STATUS_CHANGE",
                        "old_value": str(old_status),
                        "new_value": str(status),
                        "event_metadata": None
                    })
                    logger.info(f"Detected STATUS_CHANGE: advert={advert_id} {old_status} -> {status}")
                
                # ===== BID_CHANGE per nm_id =====
                nm_settings = advert.get("nm_settings") or []
                current_items = []
                
                for nm_setting in nm_settings:
                    if not isinstance(nm_setting, dict):
                        continue
                    nm_id = int(nm_setting.get("nm_id") or 0)
                    if not nm_id:
                        continue
                    
                    current_items.append(nm_id)
                    
                    bids = nm_setting.get("bids_kopecks") or {}
                    bid_search = int(bids.get("search") or 0)
                    bid_reco = int(bids.get("recommendations") or 0)
                    
                    # Compare with Redis: per-nm_id bids
                    old_bid_search = self.state_manager.get_bid(shop_id, advert_id, nm_id, "search")
                    old_bid_reco = self.state_manager.get_bid(shop_id, advert_id, nm_id, "recommendations")
                    
                    if old_bid_search is not None and bid_search != old_bid_search:
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": advert_id,
                            "nm_id": nm_id,
                            "event_type": "BID_CHANGE",
                            "old_value": str(old_bid_search),
                            "new_value": str(bid_search),
                            "event_metadata": {
                                "bid_field": "search",
                                "campaign_type": campaign_type,
                                "unit": "kopecks",
                            }
                        })
                        logger.info(
                            f"Detected BID_CHANGE (search): advert={advert_id} "
                            f"nm={nm_id} {old_bid_search} -> {bid_search} kopecks"
                        )
                    
                    if old_bid_reco is not None and bid_reco != old_bid_reco:
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": advert_id,
                            "nm_id": nm_id,
                            "event_type": "BID_CHANGE",
                            "old_value": str(old_bid_reco),
                            "new_value": str(bid_reco),
                            "event_metadata": {
                                "bid_field": "recommendations",
                                "campaign_type": campaign_type,
                                "unit": "kopecks",
                            }
                        })
                        logger.info(
                            f"Detected BID_CHANGE (recommendations): advert={advert_id} "
                            f"nm={nm_id} {old_bid_reco} -> {bid_reco} kopecks"
                        )
                    
                    # Update per-nm_id bid state in Redis
                    self.state_manager.set_bid(shop_id, advert_id, nm_id, "search", bid_search)
                    self.state_manager.set_bid(shop_id, advert_id, nm_id, "recommendations", bid_reco)
                
                # ===== ITEM_ADD / ITEM_REMOVE =====
                old_items = set(old_state.get("items") or [])
                current_items_set = set(current_items)
                
                for nm_id in current_items_set - old_items:
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
                
                for nm_id in old_items - current_items_set:
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
                
                # ===== Update Redis state =====
                # Use max bid as CPM equivalent for backward compat
                max_bid = Decimal(0)
                for nm_s in nm_settings:
                    if isinstance(nm_s, dict):
                        b = nm_s.get("bids_kopecks") or {}
                        max_bid = max(max_bid, Decimal(str(int(b.get("search") or 0))))
                
                self.state_manager.set_state(
                    shop_id, advert_id,
                    cpm=float(max_bid) if max_bid > 0 else None,
                    status=status,
                    items=current_items if current_items else None,
                    campaign_type=campaign_type
                )
            
            except Exception as e:
                logger.warning(f"Error processing V2 advert {advert.get('id')}: {e}")
                continue
        
        logger.info(f"Detected {len(events)} events total (V2)")
        return events

    def extract_all_campaign_data_v2(
        self,
        adverts_v2: List[Dict[str, Any]],
        campaign_type_map: Dict[int, int] = None,
    ) -> Tuple[Dict[int, List[int]], Dict[int, Decimal], Dict[int, int]]:
        """
        Extract campaign items, bid values, and types from V2 API response.
        
        Replaces extract_all_campaign_data() for V2 format.
        
        Args:
            adverts_v2: Response from GET /api/advert/v2/adverts
            campaign_type_map: advert_id -> type (from /adv/v1/promotion/count)
        
        Returns:
            Tuple of (campaign_items, cpm_values, campaign_types)
            cpm_values uses max(bid_search, bid_recommendations) in kopecks
            as CPM equivalent for backward compat with history parser.
        """
        campaign_items = {}
        cpm_values = {}
        campaign_types = {}
        type_map = campaign_type_map or {}
        
        for advert in adverts_v2:
            try:
                advert_id = int(advert.get("id") or 0)
                if not advert_id:
                    continue
                
                # Items from nm_settings
                nm_settings = advert.get("nm_settings") or []
                items = []
                max_bid = 0
                
                for nm_s in nm_settings:
                    if not isinstance(nm_s, dict):
                        continue
                    nm_id = int(nm_s.get("nm_id") or 0)
                    if nm_id:
                        items.append(nm_id)
                    
                    bids = nm_s.get("bids_kopecks") or {}
                    bid_search = int(bids.get("search") or 0)
                    bid_reco = int(bids.get("recommendations") or 0)
                    max_bid = max(max_bid, bid_search, bid_reco)
                
                campaign_items[advert_id] = items
                cpm_values[advert_id] = Decimal(str(max_bid))
                campaign_types[advert_id] = type_map.get(advert_id, 0)
            except Exception as e:
                logger.warning(f"Error extracting V2 campaign data for {advert.get('id')}: {e}")
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
