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
