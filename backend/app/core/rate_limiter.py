"""
Redis-based Rate Limiter for distributed Celery workers.

This module provides rate limiting synchronized across all workers via Redis.
Critical for when you have 4+ celery-fast workers sharing limits for the same shop.

ARCHITECTURE:
    - Uses Redis for atomic counters
    - Sliding window algorithm via sorted sets
    - Exponential backoff stored in Redis
    - Works across all Celery workers
"""

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import redis.asyncio as aioredis


@dataclass
class RateLimitConfig:
    """Rate limit configuration for a marketplace."""
    
    requests_per_second: float = 3.0
    requests_per_minute: int = 100
    requests_per_hour: int = 3000
    
    # Backoff configuration
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    backoff_multiplier: float = 2.0


# Default rate limits per marketplace
MARKETPLACE_LIMITS: Dict[str, RateLimitConfig] = {
    "wildberries": RateLimitConfig(
        requests_per_second=3.0,
        requests_per_minute=100,
        requests_per_hour=3000,
    ),
    "ozon": RateLimitConfig(
        requests_per_second=10.0,
        requests_per_minute=300,
        requests_per_hour=10000,
    ),
}


class RedisRateLimiter:
    """
    Distributed rate limiter using Redis for synchronization.
    
    All Celery workers share the same rate limits through Redis.
    Uses sliding window algorithm for accuracy.
    
    Redis Keys:
        - mms:ratelimit:{shop_id}:window - Sorted set for sliding window
        - mms:ratelimit:{shop_id}:backoff - Backoff state
    """
    
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.redis_url = redis_url
        self._redis: Optional[aioredis.Redis] = None
        self._key_prefix = "mms:ratelimit"
    
    async def _get_redis(self) -> aioredis.Redis:
        """Get or create Redis connection."""
        if self._redis is None:
            self._redis = await aioredis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self._redis
    
    def _get_key(self, shop_id: int, suffix: str) -> str:
        """Generate Redis key."""
        return f"{self._key_prefix}:{shop_id}:{suffix}"
    
    async def can_request(self, shop_id: int, marketplace: str = "wildberries") -> bool:
        """Check if a request can be made (non-blocking)."""
        redis = await self._get_redis()
        config = MARKETPLACE_LIMITS.get(marketplace, MARKETPLACE_LIMITS["wildberries"])
        
        # Check backoff first
        backoff_key = self._get_key(shop_id, "backoff")
        backoff_until = await redis.get(backoff_key)
        if backoff_until and float(backoff_until) > time.time():
            return False
        
        # Check sliding window
        window_key = self._get_key(shop_id, "window")
        now = time.time()
        window_start = now - 1.0  # 1 second window
        
        # Count requests in window
        count = await redis.zcount(window_key, window_start, now)
        return count < config.requests_per_second
    
    async def acquire(
        self,
        shop_id: int,
        marketplace: str = "wildberries",
        timeout: float = 30.0,
    ) -> bool:
        """
        Acquire permission to make a request (blocking).
        
        Uses sliding window algorithm with Redis sorted sets.
        Blocks until rate limit allows or timeout.
        """
        redis = await self._get_redis()
        config = MARKETPLACE_LIMITS.get(marketplace, MARKETPLACE_LIMITS["wildberries"])
        start_time = time.time()
        
        window_key = self._get_key(shop_id, "window")
        backoff_key = self._get_key(shop_id, "backoff")
        
        while True:
            now = time.time()
            
            # Check timeout
            if now - start_time > timeout:
                return False
            
            # Check backoff
            backoff_until = await redis.get(backoff_key)
            if backoff_until and float(backoff_until) > now:
                wait_time = min(float(backoff_until) - now, 0.5)
                await asyncio.sleep(wait_time)
                continue
            
            # Clean old entries from sliding window
            window_start = now - 1.0
            await redis.zremrangebyscore(window_key, 0, window_start)
            
            # Try to acquire slot atomically
            async with redis.pipeline(transaction=True) as pipe:
                try:
                    # Watch the key for changes
                    await pipe.watch(window_key)
                    
                    # Count current requests
                    count = await redis.zcount(window_key, window_start, now)
                    
                    if count < config.requests_per_second:
                        # Acquire slot
                        pipe.multi()
                        pipe.zadd(window_key, {str(now): now})
                        pipe.expire(window_key, 60)  # TTL for cleanup
                        await pipe.execute()
                        return True
                    else:
                        await pipe.unwatch()
                        
                except aioredis.WatchError:
                    # Another worker modified, retry
                    pass
            
            # Need to wait
            await asyncio.sleep(0.1)
    
    async def report_rate_limit(self, shop_id: int, marketplace: str = "wildberries"):
        """
        Report 429 error and set exponential backoff with jitter.
        
        JITTER: Adds ±10-30 seconds randomness to prevent Thundering Herd.
        When 50 shops all get 429 at the same time, they won't all wake up
        at exactly the same moment.
        """
        import random
        
        redis = await self._get_redis()
        config = MARKETPLACE_LIMITS.get(marketplace, MARKETPLACE_LIMITS["wildberries"])
        
        backoff_key = self._get_key(shop_id, "backoff")
        count_key = self._get_key(shop_id, "429_count")
        
        # Increment 429 counter
        count = await redis.incr(count_key)
        await redis.expire(count_key, 3600)  # Reset after 1 hour
        
        # Calculate base backoff
        base_backoff = min(
            config.initial_backoff_seconds * (config.backoff_multiplier ** count),
            config.max_backoff_seconds,
        )
        
        # Add jitter: ±10-30 seconds (scale with backoff)
        # More backoff = more jitter to spread load
        jitter_range = min(30, max(10, base_backoff * 0.5))
        jitter = random.uniform(-jitter_range, jitter_range)
        
        # Final backoff (minimum 1 second)
        backoff = max(1.0, base_backoff + jitter)
        
        # Set backoff until
        backoff_until = time.time() + backoff
        await redis.set(backoff_key, str(backoff_until), ex=int(backoff) + 60)
    
    async def report_success(self, shop_id: int, marketplace: str = "wildberries"):
        """Report successful request (resets 429 counter)."""
        redis = await self._get_redis()
        count_key = self._get_key(shop_id, "429_count")
        await redis.delete(count_key)
    
    async def get_wait_time(self, shop_id: int, marketplace: str = "wildberries") -> float:
        """Get time until next request is allowed."""
        redis = await self._get_redis()
        config = MARKETPLACE_LIMITS.get(marketplace, MARKETPLACE_LIMITS["wildberries"])
        
        # Check backoff
        backoff_key = self._get_key(shop_id, "backoff")
        backoff_until = await redis.get(backoff_key)
        if backoff_until and float(backoff_until) > time.time():
            return float(backoff_until) - time.time()
        
        # Check sliding window
        window_key = self._get_key(shop_id, "window")
        now = time.time()
        window_start = now - 1.0
        
        # Get oldest entry in window
        oldest = await redis.zrange(window_key, 0, 0, withscores=True)
        if not oldest:
            return 0.0
        
        count = await redis.zcount(window_key, window_start, now)
        if count < config.requests_per_second:
            return 0.0
        
        # Wait until oldest entry expires
        oldest_time = oldest[0][1]
        return max(0.0, oldest_time + 1.0 - now)
    
    async def get_status(self, shop_id: int) -> dict:
        """Get rate limit status for monitoring."""
        redis = await self._get_redis()
        now = time.time()
        
        window_key = self._get_key(shop_id, "window")
        backoff_key = self._get_key(shop_id, "backoff")
        count_key = self._get_key(shop_id, "429_count")
        
        window_start = now - 1.0
        count = await redis.zcount(window_key, window_start, now)
        backoff_until = await redis.get(backoff_key)
        error_count = await redis.get(count_key)
        
        return {
            "shop_id": shop_id,
            "current_requests_per_second": count,
            "in_backoff": backoff_until and float(backoff_until) > now,
            "backoff_remaining": max(0.0, float(backoff_until or 0) - now),
            "consecutive_429_count": int(error_count or 0),
        }
    
    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None


# ===================
# Global instance
# ===================

_rate_limiter: Optional[RedisRateLimiter] = None


def get_rate_limiter(redis_url: Optional[str] = None) -> RedisRateLimiter:
    """Get global rate limiter instance."""
    global _rate_limiter
    if _rate_limiter is None:
        from app.config import get_settings
        settings = get_settings()
        url = redis_url or settings.redis_url
        _rate_limiter = RedisRateLimiter(url)
    return _rate_limiter


# ===================
# Convenience functions for tasks
# ===================

async def wait_for_rate_limit(shop_id: int, marketplace: str = "wildberries") -> bool:
    """Wait until rate limit allows a request."""
    limiter = get_rate_limiter()
    return await limiter.acquire(shop_id, marketplace)


async def report_429_error(shop_id: int, marketplace: str = "wildberries"):
    """Report a 429 error for backoff."""
    limiter = get_rate_limiter()
    await limiter.report_rate_limit(shop_id, marketplace)


async def report_request_success(shop_id: int, marketplace: str = "wildberries"):
    """Report successful request."""
    limiter = get_rate_limiter()
    await limiter.report_success(shop_id, marketplace)
