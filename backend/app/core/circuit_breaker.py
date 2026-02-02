"""
Circuit Breaker for shop API health.

PROBLEM: If a shop's API key becomes invalid (revoked, expired), the system
will keep trying different proxies thinking they're the issue. This wastes
resources and can trigger rate limits.

SOLUTION: Track failures per shop. After 10 consecutive failures on different
proxies, mark shop as `auth_error` and stop sync until user fixes the key.

STATES:
    - CLOSED: Normal operation
    - OPEN: Shop is broken, stop all requests
    - HALF_OPEN: Testing if shop recovered

Redis Keys:
    - mms:circuit:{shop_id}:failures - Consecutive failure count
    - mms:circuit:{shop_id}:state - Current state
    - mms:circuit:{shop_id}:last_proxy - Last proxy used (to detect proxy rotation)
"""

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import redis.asyncio as aioredis
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Broken, stop requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitConfig:
    """Circuit breaker configuration."""
    
    # Number of consecutive failures to open circuit
    failure_threshold: int = 10
    
    # Time to wait before testing recovery (seconds)
    recovery_timeout: float = 3600.0  # 1 hour
    
    # Number of successful requests to close circuit
    success_threshold: int = 3


# Default config
DEFAULT_CONFIG = CircuitConfig()


class CircuitBreaker:
    """
    Circuit breaker for shop API health.
    
    Prevents wasted requests when a shop's API key is invalid.
    Automatically marks shop for user intervention.
    
    Usage:
        breaker = CircuitBreaker(redis_url)
        
        # Before making request
        if not await breaker.can_request(shop_id):
            raise ShopDisabledError("Shop requires auth fix")
        
        # After request
        if response.status_code == 401:
            await breaker.record_auth_failure(shop_id, proxy_id)
        else:
            await breaker.record_success(shop_id)
    """
    
    def __init__(
        self,
        redis_url: str = "redis://redis:6379/0",
        config: Optional[CircuitConfig] = None,
    ):
        self.redis_url = redis_url
        self.config = config or DEFAULT_CONFIG
        self._redis: Optional[aioredis.Redis] = None
        self._key_prefix = "mms:circuit"
    
    async def _get_redis(self) -> aioredis.Redis:
        """Get or create Redis connection, recreating if loop changed."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self._redis:
            try:
                if self._redis.connection_pool.connection_kwargs.get("loop") != current_loop:
                    await self._redis.close()
                    self._redis = None
            except Exception:
                self._redis = None
        
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
    
    async def can_request(self, shop_id: int) -> bool:
        """
        Check if requests to this shop are allowed.
        
        Returns False if circuit is OPEN (shop needs auth fix).
        """
        redis = await self._get_redis()
        state_key = self._get_key(shop_id, "state")
        opened_at_key = self._get_key(shop_id, "opened_at")
        
        state = await redis.get(state_key)
        
        if state == CircuitState.CLOSED.value or state is None:
            return True
        
        if state == CircuitState.OPEN.value:
            # Check if we should try half-open
            opened_at = await redis.get(opened_at_key)
            if opened_at:
                elapsed = time.time() - float(opened_at)
                if elapsed > self.config.recovery_timeout:
                    # Move to half-open for testing
                    await redis.set(state_key, CircuitState.HALF_OPEN.value)
                    await redis.set(self._get_key(shop_id, "half_open_successes"), "0")
                    return True
            return False
        
        # HALF_OPEN - allow limited requests for testing
        return True
    
    async def record_auth_failure(
        self,
        shop_id: int,
        proxy_id: Optional[int] = None,
        db: Optional[AsyncSession] = None,
    ):
        """
        Record an authentication failure (401/403).
        
        Tracks failures across different proxies to distinguish
        auth issues from proxy issues.
        """
        redis = await self._get_redis()
        
        failures_key = self._get_key(shop_id, "failures")
        proxies_key = self._get_key(shop_id, "failed_proxies")
        state_key = self._get_key(shop_id, "state")
        
        # Get current state
        state = await redis.get(state_key) or CircuitState.CLOSED.value
        
        if state == CircuitState.HALF_OPEN.value:
            # Failed during recovery test - reopen circuit
            await self._open_circuit(shop_id, db)
            return
        
        # Increment failure count
        failures = await redis.incr(failures_key)
        await redis.expire(failures_key, 3600)  # Reset after 1 hour of no failures
        
        # Track which proxies failed
        if proxy_id:
            await redis.sadd(proxies_key, str(proxy_id))
            await redis.expire(proxies_key, 3600)
            failed_proxy_count = await redis.scard(proxies_key)
        else:
            failed_proxy_count = 1
        
        # Check if we should open circuit
        # Open if: failures >= threshold AND failed on multiple proxies
        # This ensures it's not just a proxy issue
        if failures >= self.config.failure_threshold and failed_proxy_count >= 2:
            await self._open_circuit(shop_id, db)
    
    async def _open_circuit(
        self,
        shop_id: int,
        db: Optional[AsyncSession] = None,
    ):
        """Open the circuit and mark shop as needing auth fix."""
        redis = await self._get_redis()
        
        state_key = self._get_key(shop_id, "state")
        opened_at_key = self._get_key(shop_id, "opened_at")
        
        await redis.set(state_key, CircuitState.OPEN.value)
        await redis.set(opened_at_key, str(time.time()))
        
        # Update shop status in PostgreSQL
        if db:
            await db.execute(
                update(Shop)
                .where(Shop.id == shop_id)
                .values(
                    status="auth_error",
                    status_message="API key appears invalid. Please update.",
                    is_active=False,
                )
            )
            await db.commit()
    
    async def record_success(self, shop_id: int, db: Optional[AsyncSession] = None):
        """
        Record a successful request.
        
        Resets failure count and handles half-open recovery.
        """
        redis = await self._get_redis()
        
        failures_key = self._get_key(shop_id, "failures")
        proxies_key = self._get_key(shop_id, "failed_proxies")
        state_key = self._get_key(shop_id, "state")
        half_open_key = self._get_key(shop_id, "half_open_successes")
        
        state = await redis.get(state_key) or CircuitState.CLOSED.value
        
        if state == CircuitState.HALF_OPEN.value:
            # Increment success counter
            successes = await redis.incr(half_open_key)
            
            if successes >= self.config.success_threshold:
                # Close the circuit - shop recovered!
                await self._close_circuit(shop_id, db)
        else:
            # Regular success - reset failure counters
            await redis.delete(failures_key, proxies_key)
    
    async def _close_circuit(
        self,
        shop_id: int,
        db: Optional[AsyncSession] = None,
    ):
        """Close the circuit - shop is healthy again."""
        redis = await self._get_redis()
        
        # Clean up all keys
        keys_to_delete = [
            self._get_key(shop_id, "state"),
            self._get_key(shop_id, "opened_at"),
            self._get_key(shop_id, "failures"),
            self._get_key(shop_id, "failed_proxies"),
            self._get_key(shop_id, "half_open_successes"),
        ]
        await redis.delete(*keys_to_delete)
        
        # Update shop status in PostgreSQL
        if db:
            await db.execute(
                update(Shop)
                .where(Shop.id == shop_id)
                .values(
                    status="active",
                    status_message=None,
                    is_active=True,
                )
            )
            await db.commit()
    
    async def get_status(self, shop_id: int) -> dict:
        """Get circuit breaker status for monitoring."""
        redis = await self._get_redis()
        
        state = await redis.get(self._get_key(shop_id, "state"))
        failures = await redis.get(self._get_key(shop_id, "failures"))
        failed_proxies = await redis.scard(self._get_key(shop_id, "failed_proxies"))
        opened_at = await redis.get(self._get_key(shop_id, "opened_at"))
        
        return {
            "shop_id": shop_id,
            "state": state or CircuitState.CLOSED.value,
            "consecutive_failures": int(failures or 0),
            "failed_proxy_count": failed_proxies,
            "opened_at": float(opened_at) if opened_at else None,
            "failure_threshold": self.config.failure_threshold,
        }
    
    async def reset(self, shop_id: int, db: Optional[AsyncSession] = None):
        """
        Manually reset circuit (e.g., after user updates API key).
        
        Call this from API when user submits new API key.
        """
        await self._close_circuit(shop_id, db)
    
    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            self._redis = None


# Lazy import to avoid circular dependency
Shop = None


def _get_shop_model():
    """Lazy import Shop model."""
    global Shop
    if Shop is None:
        from app.models.shop import Shop as ShopModel
        Shop = ShopModel
    return Shop


# ===================
# Global instance
# ===================

_circuit_breaker: Optional[CircuitBreaker] = None


def get_circuit_breaker(redis_url: Optional[str] = None) -> CircuitBreaker:
    """Get global circuit breaker instance."""
    global _circuit_breaker
    if _circuit_breaker is None:
        from app.config import get_settings
        settings = get_settings()
        url = redis_url or settings.redis_url
        _circuit_breaker = CircuitBreaker(url)
    return _circuit_breaker


# ===================
# Convenience functions
# ===================

async def check_shop_health(shop_id: int) -> bool:
    """Check if shop is healthy (circuit closed)."""
    breaker = get_circuit_breaker()
    return await breaker.can_request(shop_id)


async def report_shop_auth_error(
    shop_id: int,
    proxy_id: Optional[int] = None,
    db: Optional[AsyncSession] = None,
):
    """Report auth error for shop."""
    breaker = get_circuit_breaker()
    await breaker.record_auth_failure(shop_id, proxy_id, db)


async def report_shop_success(shop_id: int, db: Optional[AsyncSession] = None):
    """Report successful request for shop."""
    breaker = get_circuit_breaker()
    await breaker.record_success(shop_id, db)


async def reset_shop_circuit(shop_id: int, db: Optional[AsyncSession] = None):
    """Reset shop circuit after API key update."""
    breaker = get_circuit_breaker()
    await breaker.reset(shop_id, db)
