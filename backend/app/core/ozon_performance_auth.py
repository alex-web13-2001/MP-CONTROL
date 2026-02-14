"""
OAuth2 client for Ozon Performance API.

Ozon Performance API uses a SEPARATE auth from Seller API:
- Seller API: Client-Id + Api-Key headers
- Performance API: OAuth2 client_credentials → Bearer token (TTL 30 min)

Token is cached in Redis to share across workers.

Usage:
    auth = OzonPerformanceAuth(client_id="...", client_secret="...")
    token = await auth.get_token()
    headers = {"Authorization": f"Bearer {token}"}
"""

import asyncio
import json
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Token URL
TOKEN_URL = "https://api-performance.ozon.ru/api/client/token"
# Refresh token 5 min before expiry
TOKEN_REFRESH_MARGIN = 300
# Default TTL from Ozon: 1800s (30 min)
DEFAULT_TTL = 1800


class OzonPerformanceAuth:
    """
    OAuth2 client_credentials auth for Ozon Performance API.

    Manages access_token lifecycle:
    - Fetches token from Ozon
    - Caches in-memory with expiry tracking
    - Auto-refreshes before expiry
    - Optional Redis caching for multi-worker setups
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redis_client=None,
        redis_key_prefix: str = "ozon_perf_token",
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self._redis = redis_client
        self._redis_key = f"{redis_key_prefix}:{client_id}"

        # In-memory cache
        self._token: Optional[str] = None
        self._expires_at: float = 0

    def _is_expired(self) -> bool:
        """Check if token is expired or about to expire."""
        return time.time() >= (self._expires_at - TOKEN_REFRESH_MARGIN)

    async def _fetch_token(self) -> dict:
        """Fetch new token from Ozon Performance API."""
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                TOKEN_URL,
                json={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )

        if response.status_code != 200:
            logger.error(
                "Ozon Performance token error: status=%s body=%s",
                response.status_code, response.text[:200],
            )
            raise Exception(
                f"Failed to get Ozon Performance token: {response.status_code} {response.text[:200]}"
            )

        data = response.json()
        logger.info(
            "Ozon Performance token obtained: expires_in=%s type=%s",
            data.get("expires_in"), data.get("token_type"),
        )
        return data

    async def _try_redis_cache(self) -> Optional[str]:
        """Try to get token from Redis cache."""
        if not self._redis:
            return None
        try:
            cached = await self._redis.get(self._redis_key)
            if cached:
                data = json.loads(cached)
                if time.time() < data.get("expires_at", 0) - TOKEN_REFRESH_MARGIN:
                    logger.debug("Ozon Performance token from Redis cache")
                    return data["access_token"]
        except Exception as e:
            logger.warning("Redis cache read error: %s", e)
        return None

    async def _save_redis_cache(self, token: str, expires_at: float):
        """Save token to Redis cache."""
        if not self._redis:
            return
        try:
            ttl = int(expires_at - time.time())
            if ttl > 0:
                await self._redis.setex(
                    self._redis_key,
                    ttl,
                    json.dumps({"access_token": token, "expires_at": expires_at}),
                )
        except Exception as e:
            logger.warning("Redis cache write error: %s", e)

    async def get_token(self) -> str:
        """
        Get a valid access_token.

        Returns cached token if still valid, otherwise fetches a new one.
        """
        # 1. Check in-memory cache
        if self._token and not self._is_expired():
            return self._token

        # 2. Check Redis cache
        cached = await self._try_redis_cache()
        if cached:
            self._token = cached
            return cached

        # 3. Fetch new token
        data = await self._fetch_token()
        self._token = data["access_token"]
        expires_in = data.get("expires_in", DEFAULT_TTL)
        self._expires_at = time.time() + expires_in

        # 4. Save to Redis
        await self._save_redis_cache(self._token, self._expires_at)

        return self._token

    def get_headers(self, token: str) -> dict:
        """Build request headers with Bearer auth."""
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
