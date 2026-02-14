"""
Ozon Ads Event Detector — detects campaign changes and logs to event_log.

Mirrors WB EventDetector pattern:
    1. Fetch current state from API
    2. Compare with Redis last state
    3. Generate events (BID_CHANGE, STATUS_CHANGE, BUDGET_CHANGE, ITEM_ADD/REMOVE)
    4. Update Redis with current state

Event types:
    OZON_BID_CHANGE:    SKU bid changed (old_value=35.0, new_value=65.0)
    OZON_STATUS_CHANGE: Campaign state changed (RUNNING → STOPPED)
    OZON_BUDGET_CHANGE: Daily budget changed (5000 → 10000)
    OZON_ITEM_ADD:      SKU added to campaign
    OZON_ITEM_REMOVE:   SKU removed from campaign
"""

import logging
from typing import Dict, Any, List, Optional, Set

from app.core.redis_state import RedisStateManager

logger = logging.getLogger(__name__)

# Bid values from API are in microroubles
MICROROUBLES = 1_000_000


class OzonAdsEventDetector:
    """
    Detects Ozon advertising events by comparing current API state
    with Redis cache. Same pattern as WB EventDetector.

    Usage:
        detector = OzonAdsEventDetector(redis_url)
        events = detector.detect_all(shop_id, campaigns, products_by_campaign)
        # → insert events into event_log
    """

    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.state_manager = RedisStateManager(redis_url)

    def detect_all(
        self,
        shop_id: int,
        campaigns: List[Dict[str, Any]],
        products_by_campaign: Dict[int, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        """
        Full detection pipeline.

        Args:
            shop_id: Shop ID
            campaigns: List from GET /api/client/campaign
            products_by_campaign: {campaign_id: [products from /v2/products]}

        Returns:
            List of event dicts ready for event_log insertion.
        """
        events = []

        # 1. Campaign-level: STATUS_CHANGE + BUDGET_CHANGE
        events.extend(self.detect_campaign_changes(shop_id, campaigns))

        # 2. Product-level: BID_CHANGE + ITEM_ADD/REMOVE
        for campaign_id, products in products_by_campaign.items():
            events.extend(
                self.detect_product_changes(shop_id, campaign_id, products)
            )

        logger.info("Ozon EventDetector: %d events detected", len(events))
        return events

    def detect_campaign_changes(
        self,
        shop_id: int,
        campaigns: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Detect STATUS_CHANGE and BUDGET_CHANGE from campaign list.

        Campaign dict from API:
            {id, title, state, advObjectType, dailyBudget, ...}
        """
        events = []

        for camp in campaigns:
            try:
                campaign_id = int(camp.get("id", 0))
                if not campaign_id:
                    continue

                current_status = camp.get("state")
                current_budget = camp.get("dailyBudget")

                # Convert budget from microroubles if numeric
                if current_budget is not None:
                    try:
                        current_budget = float(current_budget) / MICROROUBLES
                    except (ValueError, TypeError):
                        current_budget = None

                # Get last state from Redis
                old_state = self.state_manager.get_ozon_campaign_state(
                    shop_id, campaign_id
                )
                old_status = old_state.get("status")
                old_budget = old_state.get("budget")

                # ── STATUS_CHANGE ──
                if current_status is not None and old_status is not None:
                    if str(current_status) != str(old_status):
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": campaign_id,
                            "nm_id": None,
                            "event_type": "OZON_STATUS_CHANGE",
                            "old_value": str(old_status),
                            "new_value": str(current_status),
                            "event_metadata": {
                                "title": camp.get("title", ""),
                                "type": camp.get("advObjectType", ""),
                            },
                        })
                        logger.info(
                            "OZON_STATUS_CHANGE: campaign=%d %s → %s",
                            campaign_id, old_status, current_status,
                        )

                # ── BUDGET_CHANGE ──
                if current_budget is not None and old_budget is not None:
                    if abs(current_budget - old_budget) > 0.01:
                        events.append({
                            "shop_id": shop_id,
                            "advert_id": campaign_id,
                            "nm_id": None,
                            "event_type": "OZON_BUDGET_CHANGE",
                            "old_value": str(old_budget),
                            "new_value": str(current_budget),
                            "event_metadata": {
                                "title": camp.get("title", ""),
                            },
                        })
                        logger.info(
                            "OZON_BUDGET_CHANGE: campaign=%d %.2f → %.2f",
                            campaign_id, old_budget, current_budget,
                        )

                # Update campaign-level state in Redis
                self.state_manager.set_ozon_campaign_state(
                    shop_id, campaign_id,
                    status=str(current_status) if current_status else None,
                    budget=current_budget,
                )

            except Exception as e:
                logger.warning(
                    "Error processing campaign %s: %s",
                    camp.get("id"), e,
                )
                continue

        return events

    def detect_product_changes(
        self,
        shop_id: int,
        campaign_id: int,
        products: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Detect BID_CHANGE, ITEM_ADD, ITEM_REMOVE for a campaign's products.

        Product dict from API (/v2/products):
            {sku, bid, title, ...}
            bid is in microroubles.
        """
        events = []

        # Get last state
        old_state = self.state_manager.get_ozon_campaign_state(
            shop_id, campaign_id
        )
        old_bids = old_state.get("bids", {})  # {str(sku): bid_rub}
        old_items = set(int(x) for x in old_state.get("items", []))

        # Build current state
        current_bids = {}
        current_items = set()

        for p in products:
            sku = int(p.get("sku", 0))
            if not sku:
                continue

            current_items.add(sku)

            # Convert bid from microroubles to roubles
            raw_bid = p.get("bid", 0)
            try:
                bid_rub = int(raw_bid) / MICROROUBLES
            except (ValueError, TypeError):
                bid_rub = 0.0

            current_bids[str(sku)] = bid_rub

            # ── BID_CHANGE ──
            old_bid = old_bids.get(str(sku))
            if old_bid is not None and abs(old_bid - bid_rub) > 0.01:
                events.append({
                    "shop_id": shop_id,
                    "advert_id": campaign_id,
                    "nm_id": sku,
                    "event_type": "OZON_BID_CHANGE",
                    "old_value": str(old_bid),
                    "new_value": str(bid_rub),
                    "event_metadata": {
                        "title": p.get("title", ""),
                    },
                })
                logger.info(
                    "OZON_BID_CHANGE: campaign=%d sku=%d %.2f → %.2f",
                    campaign_id, sku, old_bid, bid_rub,
                )

        # ── ITEM_ADD ──
        added = current_items - old_items
        for sku in added:
            events.append({
                "shop_id": shop_id,
                "advert_id": campaign_id,
                "nm_id": sku,
                "event_type": "OZON_ITEM_ADD",
                "old_value": None,
                "new_value": str(sku),
                "event_metadata": None,
            })
            logger.info(
                "OZON_ITEM_ADD: campaign=%d sku=%d", campaign_id, sku,
            )

        # ── ITEM_REMOVE ──
        removed = old_items - current_items
        for sku in removed:
            events.append({
                "shop_id": shop_id,
                "advert_id": campaign_id,
                "nm_id": sku,
                "event_type": "OZON_ITEM_REMOVE",
                "old_value": str(sku),
                "new_value": None,
                "event_metadata": None,
            })
            logger.info(
                "OZON_ITEM_REMOVE: campaign=%d sku=%d", campaign_id, sku,
            )

        # Update product-level state in Redis
        self.state_manager.set_ozon_campaign_state(
            shop_id, campaign_id,
            bids=current_bids,
            items=list(current_items),
        )

        return events
