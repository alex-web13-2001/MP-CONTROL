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
