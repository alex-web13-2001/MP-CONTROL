"""
Marketplace HTTP Client with built-in proxy rotation and rate limiting.

This is the ONLY allowed way to make marketplace API requests.
It combines ProxyProvider, RateLimiter, and logging.

CRITICAL RULES (enforced in code):
1. Sticky Sessions: Same proxy for same shop during a task
2. Rate Limiting: Redis-based, shared across all workers
3. Quarantine: Bad proxies auto-disabled for 15-30 min
4. Logging: All requests logged with response_time
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from curl_cffi import requests as curl_requests
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.encryption import decrypt_api_key
from app.core.proxy_provider import ProxyConfig, ProxyProvider
from app.core.rate_limiter import (
    RedisRateLimiter,
    get_rate_limiter,
    report_429_error,
    report_request_success,
    wait_for_rate_limit,
)
from app.core.circuit_breaker import (
    CircuitBreaker,
    get_circuit_breaker,
    check_shop_health,
    report_shop_auth_error,
    report_shop_success,
)


# Marketplace base URLs
MARKETPLACE_URLS = {
    "wildberries": "https://suppliers-api.wildberries.ru",
    "wildberries_adv": "https://advert-api.wb.ru",
    "wildberries_stats": "https://statistics-api.wildberries.ru",
    "ozon": "https://api-seller.ozon.ru",
}


class ShopDisabledError(Exception):
    """
    Raised when shop is disabled due to auth errors.
    
    The shop's API key is likely invalid. User must update it
    before sync can resume.
    """
    pass


@dataclass
class MarketplaceResponse:
    """Response from marketplace API."""
    
    status_code: int
    data: Any
    response_time_ms: int
    proxy_used: Optional[str] = None
    error: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300
    
    @property
    def is_rate_limited(self) -> bool:
        return self.status_code == 429
    
    @property
    def is_banned(self) -> bool:
        return self.status_code == 403
    
    @property
    def is_auth_error(self) -> bool:
        """401 or API key issues."""
        return self.status_code == 401


class MarketplaceClient:
    """
    HTTP client for marketplace APIs.
    
    Features:
        - JA3 fingerprint spoofing via curl_cffi
        - Sticky sessions (same proxy for same shop)
        - Redis-based rate limiting (shared across workers)
        - Proxy quarantine on failures
        - Full request logging with response_time
    
    Usage:
        async with MarketplaceClient(db, shop_id=1) as client:
            response = await client.get("/api/v1/supplier/orders")
            if response.is_success:
                process_orders(response.data)
    """
    
    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        marketplace: str = "wildberries",
        api_key: Optional[str] = None,
        api_key_encrypted: Optional[bytes] = None,
        max_retries: int = 3,
        use_proxy: bool = True,
    ):
        self.db = db
        self.shop_id = shop_id
        self.marketplace = marketplace
        self.max_retries = max_retries
        self.use_proxy = use_proxy
        
        # Decrypt API key if encrypted
        if api_key_encrypted:
            self._api_key = decrypt_api_key(api_key_encrypted)
        else:
            self._api_key = api_key
        
        # Get base URL
        self.base_url = MARKETPLACE_URLS.get(marketplace, MARKETPLACE_URLS["wildberries"])
        
        # Components (initialized in __aenter__)
        self._proxy_provider: Optional[ProxyProvider] = None
        self._rate_limiter: Optional[RedisRateLimiter] = None
        self._circuit_breaker: Optional[CircuitBreaker] = None
        self._current_proxy: Optional[ProxyConfig] = None
    
    async def __aenter__(self):
        """Initialize components and get sticky proxy."""
        self._proxy_provider = ProxyProvider(self.db)
        self._rate_limiter = get_rate_limiter()
        self._circuit_breaker = get_circuit_breaker()
        
        # Check circuit breaker FIRST
        if not await self._circuit_breaker.can_request(self.shop_id):
            raise ShopDisabledError(
                f"Shop {self.shop_id} is disabled due to auth errors. "
                "Please update the API key."
            )
        
        # Get sticky proxy for this session
        if self.use_proxy:
            self._current_proxy = await self._proxy_provider.get_proxy(
                shop_id=self.shop_id,
                strategy="weighted",
                sticky=True,
            )
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup: clear sticky session."""
        if self._proxy_provider:
            self._proxy_provider.clear_sticky_session(self.shop_id)
    
    def _get_headers(self, extra_headers: Optional[Dict] = None) -> Dict[str, str]:
        """Build request headers with API key."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        
        if self._api_key:
            if "wildberries" in self.marketplace:
                headers["Authorization"] = self._api_key
            elif "ozon" in self.marketplace:
                headers["Api-Key"] = self._api_key
        
        if extra_headers:
            headers.update(extra_headers)
        
        return headers
    
    async def _log_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        response_time_ms: int,
        is_success: bool,
        error_message: Optional[str] = None,
    ):
        """Log request to proxy_usage_log for analytics."""
        proxy_id = self._current_proxy.id if self._current_proxy else None
        
        await self.db.execute(
            text("""
                INSERT INTO proxy_usage_log 
                (proxy_id, shop_id, endpoint, method, status_code, response_time_ms, is_success, error_message)
                VALUES (:proxy_id, :shop_id, :endpoint, :method, :status_code, :response_time_ms, :is_success, :error_message)
            """),
            {
                "proxy_id": proxy_id,
                "shop_id": self.shop_id,
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code,
                "response_time_ms": response_time_ms,
                "is_success": is_success,
                "error_message": error_message,
            }
        )
        await self.db.commit()
    
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> MarketplaceResponse:
        """Make a single HTTP request with current proxy."""
        url = f"{self.base_url}{endpoint}"
        start_time = time.time()
        proxy_used = None
        
        try:
            # Setup proxy
            proxies = None
            if self._current_proxy:
                proxies = self._current_proxy.to_curl_cffi_proxy()
                proxy_used = self._current_proxy.url_masked
            
            # Build headers
            headers = self._get_headers(kwargs.pop("headers", None))
            
            # Make request with curl_cffi (JA3 fingerprint spoofing)
            response = curl_requests.request(
                method=method,
                url=url,
                headers=headers,
                proxies=proxies,
                timeout=30,
                impersonate="chrome110",
                **kwargs,
            )
            
            response_time_ms = int((time.time() - start_time) * 1000)
            
            # Parse response
            try:
                data = response.json()
            except Exception:
                data = response.text
            
            result = MarketplaceResponse(
                status_code=response.status_code,
                data=data,
                response_time_ms=response_time_ms,
                proxy_used=proxy_used,
            )
            
            # Handle rate limiting and proxy feedback
            if result.is_rate_limited:
                await report_429_error(self.shop_id, self.marketplace)
                if self._current_proxy and self._proxy_provider:
                    await self._proxy_provider.report_failure(
                        self._current_proxy,
                        status_code=429,
                        error_message="Rate limited",
                        shop_id=self.shop_id,
                        endpoint=endpoint,
                    )
            elif result.is_banned:
                # 403 = IP banned, quarantine proxy
                if self._current_proxy and self._proxy_provider:
                    await self._proxy_provider.report_failure(
                        self._current_proxy,
                        status_code=403,
                        error_message="IP banned",
                        shop_id=self.shop_id,
                        endpoint=endpoint,
                    )
                    # Get new proxy for remaining requests
                    self._current_proxy = await self._proxy_provider.get_proxy(
                        shop_id=self.shop_id,
                        sticky=True,
                    )
            elif result.is_auth_error:
                # 401 = API key invalid, report to circuit breaker
                proxy_id = self._current_proxy.id if self._current_proxy else None
                await report_shop_auth_error(self.shop_id, proxy_id, self.db)
            elif result.is_success:
                await report_request_success(self.shop_id, self.marketplace)
                await report_shop_success(self.shop_id, self.db)
                if self._current_proxy and self._proxy_provider:
                    await self._proxy_provider.report_success(
                        self._current_proxy,
                        response_time_ms=response_time_ms,
                        shop_id=self.shop_id,
                        endpoint=endpoint,
                    )
            
            # Log request
            await self._log_request(
                endpoint=endpoint,
                method=method,
                status_code=result.status_code,
                response_time_ms=response_time_ms,
                is_success=result.is_success,
            )
            
            return result
            
        except Exception as e:
            response_time_ms = int((time.time() - start_time) * 1000)
            error_msg = str(e)
            
            # Report connection error to proxy
            if self._current_proxy and self._proxy_provider:
                await self._proxy_provider.report_failure(
                    self._current_proxy,
                    status_code=0,
                    error_message=error_msg,
                    shop_id=self.shop_id,
                    endpoint=endpoint,
                )
            
            # Log error
            await self._log_request(
                endpoint=endpoint,
                method=method,
                status_code=0,
                response_time_ms=response_time_ms,
                is_success=False,
                error_message=error_msg,
            )
            
            return MarketplaceResponse(
                status_code=0,
                data=None,
                response_time_ms=response_time_ms,
                proxy_used=proxy_used,
                error=error_msg,
            )
    
    async def request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> MarketplaceResponse:
        """
        Make HTTP request with rate limiting and retries.
        
        Handles:
            - Waits for rate limit (Redis-synced)
            - Makes request with sticky proxy
            - Retries on failure with backoff
            - Logs everything
        """
        last_response = None
        
        for attempt in range(self.max_retries):
            # Wait for rate limit (blocks until allowed)
            acquired = await wait_for_rate_limit(self.shop_id, self.marketplace)
            if not acquired:
                return MarketplaceResponse(
                    status_code=0,
                    data=None,
                    response_time_ms=0,
                    error="Rate limit timeout",
                )
            
            # Make request
            response = await self._make_request(method, endpoint, **kwargs)
            last_response = response
            
            # Success
            if response.is_success:
                return response
            
            # Rate limited - already backed off in report_429_error
            if response.is_rate_limited:
                wait_time = await self._rate_limiter.get_wait_time(
                    self.shop_id, self.marketplace
                )
                await asyncio.sleep(wait_time)
                continue
            
            # Banned - got new proxy, retry
            if response.is_banned:
                continue
            
            # Server error - retry with backoff
            if response.status_code >= 500:
                await asyncio.sleep(1.0 * (attempt + 1))
                continue
            
            # Client error (4xx except 429/403) - don't retry
            if 400 <= response.status_code < 500:
                return response
        
        return last_response or MarketplaceResponse(
            status_code=0,
            data=None,
            response_time_ms=0,
            error="Max retries exceeded",
        )
    
    # Convenience methods
    async def get(self, endpoint: str, **kwargs) -> MarketplaceResponse:
        return await self.request("GET", endpoint, **kwargs)
    
    async def post(self, endpoint: str, **kwargs) -> MarketplaceResponse:
        return await self.request("POST", endpoint, **kwargs)
    
    async def put(self, endpoint: str, **kwargs) -> MarketplaceResponse:
        return await self.request("PUT", endpoint, **kwargs)
    
    async def delete(self, endpoint: str, **kwargs) -> MarketplaceResponse:
        return await self.request("DELETE", endpoint, **kwargs)
