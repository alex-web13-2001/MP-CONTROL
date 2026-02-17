"""
Redis State Manager for advertising Last State tracking.

Stores the last known state of campaigns to detect changes:
- CPM/CPC bids
- Campaign status
- List of items in campaign

Uses Redis Hashes (HSET/HGETALL) for efficient state retrieval.
"""
import json
import logging
from typing import Optional, List, Dict, Any
import redis

logger = logging.getLogger(__name__)


class RedisStateManager:
    """
    Manages Last State for advertising campaigns in Redis using Hashes.
    
    Key format (using HSET):
    - ads:state:{shop_id}:{advert_id} -> {cpm: "500", status: "9", items: "[...]", type: "8"}
    
    Benefits:
    - Single HGETALL to fetch entire campaign state
    - Atomic updates with HSET
    - Better memory efficiency
    """
    
    PREFIX = "ads:state"
    TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days
    
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.redis_url = redis_url
        self._client: Optional[redis.Redis] = None
    
    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.from_url(self.redis_url, decode_responses=True)
        return self._client
    
    def _key(self, shop_id: int, advert_id: int) -> str:
        return f"{self.PREFIX}:{shop_id}:{advert_id}"
    
    # ============ Bulk Operations (Primary API) ============
    
    def get_state(self, shop_id: int, advert_id: int) -> Dict[str, Any]:
        """
        Get full last state for a campaign using HGETALL.
        Returns dict with cpm, status, items, campaign_type.
        """
        key = self._key(shop_id, advert_id)
        raw = self.client.hgetall(key)
        
        if not raw:
            return {"cpm": None, "status": None, "items": [], "campaign_type": None}
        
        # Parse stored values
        cpm = float(raw["cpm"]) if raw.get("cpm") else None
        status = int(raw["status"]) if raw.get("status") else None
        campaign_type = int(raw["campaign_type"]) if raw.get("campaign_type") else None
        
        items = []
        if raw.get("items"):
            try:
                items = json.loads(raw["items"])
            except json.JSONDecodeError:
                items = []
        
        return {
            "cpm": cpm,
            "status": status,
            "items": items,
            "campaign_type": campaign_type
        }
    
    def set_state(
        self, 
        shop_id: int, 
        advert_id: int,
        cpm: Optional[float] = None,
        status: Optional[int] = None,
        items: Optional[List[int]] = None,
        campaign_type: Optional[int] = None
    ) -> None:
        """
        Update state fields using HSET.
        Only updates fields that are not None.
        """
        key = self._key(shop_id, advert_id)
        
        mapping = {}
        if cpm is not None:
            mapping["cpm"] = str(cpm)
        if status is not None:
            mapping["status"] = str(status)
        if items is not None:
            mapping["items"] = json.dumps(items)
        if campaign_type is not None:
            mapping["campaign_type"] = str(campaign_type)
        
        if mapping:
            self.client.hset(key, mapping=mapping)
            self.client.expire(key, self.TTL_SECONDS)
    
    # ============ Individual Field Access (Convenience) ============
    
    def get_cpm(self, shop_id: int, advert_id: int) -> Optional[float]:
        """Get last known CPM for campaign."""
        val = self.client.hget(self._key(shop_id, advert_id), "cpm")
        return float(val) if val else None
    
    def get_status(self, shop_id: int, advert_id: int) -> Optional[int]:
        """Get last known status for campaign."""
        val = self.client.hget(self._key(shop_id, advert_id), "status")
        return int(val) if val else None
    
    def get_items(self, shop_id: int, advert_id: int) -> List[int]:
        """Get last known list of nm_ids in campaign."""
        val = self.client.hget(self._key(shop_id, advert_id), "items")
        if val:
            try:
                return json.loads(val)
            except json.JSONDecodeError:
                return []
        return []
    
    def get_campaign_type(self, shop_id: int, advert_id: int) -> Optional[int]:
        """Get campaign type (for CPC vs CPM differentiation)."""
        val = self.client.hget(self._key(shop_id, advert_id), "campaign_type")
        return int(val) if val else None
    
    # ============ Last Views Tracking (for ITEM_INACTIVE detection) ============
    
    def get_last_views(self, shop_id: int, advert_id: int, nm_id: int) -> Optional[int]:
        """Get last known views count for specific item."""
        key = f"{self.PREFIX}:views:{shop_id}:{advert_id}:{nm_id}"
        val = self.client.get(key)
        return int(val) if val else None
    
    def set_last_views(self, shop_id: int, advert_id: int, nm_id: int, views: int) -> None:
        """Store last views count for specific item."""
        key = f"{self.PREFIX}:views:{shop_id}:{advert_id}:{nm_id}"
        self.client.setex(key, self.TTL_SECONDS, str(views))

    # ============ Per-NM Bid Tracking (V2 API) ============

    def get_bid(self, shop_id: int, advert_id: int, nm_id: int, field: str) -> Optional[int]:
        """Get last known bid in kopecks for specific nm_id and field (search/recommendations)."""
        key = f"{self.PREFIX}:bid:{shop_id}:{advert_id}:{nm_id}:{field}"
        val = self.client.get(key)
        return int(val) if val else None

    def set_bid(self, shop_id: int, advert_id: int, nm_id: int, field: str, value: int) -> None:
        """Store bid in kopecks for specific nm_id and field (search/recommendations)."""
        key = f"{self.PREFIX}:bid:{shop_id}:{advert_id}:{nm_id}:{field}"
        self.client.setex(key, self.TTL_SECONDS, str(value))

    COMMERCIAL_TTL = 2 * 24 * 60 * 60  # 2 days (enough for 30-min intervals)

    def get_price(self, shop_id: int, nm_id: int) -> Optional[float]:
        """Get last known converted price for a product."""
        key = f"state:price:{shop_id}:{nm_id}"
        val = self.client.get(key)
        return float(val) if val else None

    def set_price(self, shop_id: int, nm_id: int, price: float) -> None:
        """Store current converted price for a product."""
        key = f"state:price:{shop_id}:{nm_id}"
        self.client.setex(key, self.COMMERCIAL_TTL, str(price))

    def get_stock(self, shop_id: int, nm_id: int, warehouse: str) -> Optional[int]:
        """Get last known stock quantity for a product at a specific warehouse."""
        key = f"state:stock:{shop_id}:{nm_id}:{warehouse}"
        val = self.client.get(key)
        return int(val) if val else None

    def set_stock(self, shop_id: int, nm_id: int, warehouse: str, quantity: int) -> None:
        """Store current stock quantity for a product at a specific warehouse."""
        key = f"state:stock:{shop_id}:{nm_id}:{warehouse}"
        self.client.setex(key, self.COMMERCIAL_TTL, str(quantity))

    def get_image_url(self, shop_id: int, nm_id: int) -> Optional[str]:
        """Get last known main image URL for a product."""
        key = f"state:image:{shop_id}:{nm_id}"
        return self.client.get(key)

    def set_image_url(self, shop_id: int, nm_id: int, url: str) -> None:
        """Store current main image URL for a product."""
        key = f"state:image:{shop_id}:{nm_id}"
        self.client.setex(key, self.COMMERCIAL_TTL, url)

    # ============ Content Monitoring State (SEO audit) ============

    CONTENT_TTL = 3 * 24 * 60 * 60  # 3 days (checked once per day)

    def get_content_hash(self, shop_id: int, nm_id: int) -> Optional[Dict[str, str]]:
        """Get last known content hashes for a product."""
        key = f"state:content:{shop_id}:{nm_id}"
        raw = self.client.hgetall(key)
        if not raw:
            return None
        return raw

    def set_content_hash(
        self,
        shop_id: int,
        nm_id: int,
        title_hash: str = "",
        desc_hash: str = "",
        photos_hash: str = "",
        main_photo_id: str = "",
    ) -> None:
        """Store content hashes for a product."""
        key = f"state:content:{shop_id}:{nm_id}"
        mapping = {}
        if title_hash:
            mapping["title_hash"] = title_hash
        if desc_hash:
            mapping["desc_hash"] = desc_hash
        if photos_hash:
            mapping["photos_hash"] = photos_hash
        if main_photo_id:
            mapping["main_photo_id"] = main_photo_id
        if mapping:
            self.client.hset(key, mapping=mapping)
            self.client.expire(key, self.CONTENT_TTL)

    # ============ Ozon Ads State ============

    OZON_ADS_PREFIX = "ozon_ads:state"
    OZON_ADS_TTL = 7 * 24 * 60 * 60  # 7 days

    def _ozon_key(self, shop_id: int, campaign_id: int) -> str:
        return f"{self.OZON_ADS_PREFIX}:{shop_id}:{campaign_id}"

    def get_ozon_campaign_state(self, shop_id: int, campaign_id: int) -> Dict[str, Any]:
        """
        Get last state for an Ozon campaign.
        Returns dict with bids (sku→bid_rub), status, budget, items.
        """
        key = self._ozon_key(shop_id, campaign_id)
        raw = self.client.hgetall(key)

        if not raw:
            return {"bids": {}, "status": None, "budget": None, "items": []}

        bids = {}
        if raw.get("bids"):
            try:
                bids = json.loads(raw["bids"])
            except json.JSONDecodeError:
                bids = {}

        status = raw.get("status")
        budget = float(raw["budget"]) if raw.get("budget") else None

        items = []
        if raw.get("items"):
            try:
                items = json.loads(raw["items"])
            except json.JSONDecodeError:
                items = []

        return {
            "bids": bids,
            "status": status,
            "budget": budget,
            "items": items,
        }

    def set_ozon_campaign_state(
        self,
        shop_id: int,
        campaign_id: int,
        bids: Optional[Dict[str, float]] = None,
        status: Optional[str] = None,
        budget: Optional[float] = None,
        items: Optional[List[int]] = None,
    ) -> None:
        """
        Update Ozon campaign state in Redis.
        Only updates fields that are not None.
        """
        key = self._ozon_key(shop_id, campaign_id)

        mapping = {}
        if bids is not None:
            mapping["bids"] = json.dumps(bids)
        if status is not None:
            mapping["status"] = str(status)
        if budget is not None:
            mapping["budget"] = str(budget)
        if items is not None:
            mapping["items"] = json.dumps(items)

        if mapping:
            self.client.hset(key, mapping=mapping)
            self.client.expire(key, self.OZON_ADS_TTL)
