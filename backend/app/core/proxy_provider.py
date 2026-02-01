"""
Proxy Provider with Quarantine and Sticky Sessions.

IMPROVEMENTS:
1. Quarantine: Proxies get 15-30 min timeout on 403/429
2. Sticky Sessions: Same proxy for same shop during a task
3. Health tracking: Immediate success_rate updates
"""

import asyncio
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.encryption import decrypt_api_key


# Quarantine settings
QUARANTINE_DURATION_403 = timedelta(minutes=30)  # Banned
QUARANTINE_DURATION_429 = timedelta(minutes=15)  # Rate limited
QUARANTINE_DURATION_ERROR = timedelta(minutes=5)  # Connection errors


@dataclass
class ProxyConfig:
    """Proxy configuration for HTTP requests."""
    
    id: int  # Database ID for tracking
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    protocol: str = "http"
    
    @property
    def url(self) -> str:
        """Get proxy URL for requests library."""
        auth = ""
        if self.username and self.password:
            auth = f"{self.username}:{self.password}@"
        return f"{self.protocol}://{auth}{self.host}:{self.port}"
    
    @property
    def url_masked(self) -> str:
        """Get proxy URL with masked password for logging."""
        auth = ""
        if self.username:
            auth = f"{self.username}:***@"
        return f"{self.protocol}://{auth}{self.host}:{self.port}"
    
    def to_curl_cffi_proxy(self) -> dict:
        """Get proxy dict for curl_cffi."""
        return {
            "http": self.url,
            "https": self.url,
        }


class ProxyProvider:
    """
    Provider for rotating proxies with quarantine and sticky sessions.
    
    Features:
        - Quarantine: Bad proxies go to timeout
        - Sticky Sessions: Same proxy for same shop in a session
        - Weighted Selection: Prefer high success_rate proxies
    """
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self._round_robin_index = 0
        
        # Sticky sessions: shop_id -> (proxy_id, assigned_at)
        self._sticky_sessions: Dict[int, tuple[int, float]] = {}
        self._sticky_session_ttl = 300  # 5 minutes
        
        # In-memory quarantine cache (synced with DB)
        self._quarantine_until: Dict[int, float] = {}
    
    async def get_proxy(
        self,
        shop_id: Optional[int] = None,
        strategy: str = "weighted",
        proxy_type: Optional[str] = None,
        country: Optional[str] = None,
        sticky: bool = True,
    ) -> Optional[ProxyConfig]:
        """
        Get a proxy for making requests.
        
        Args:
            shop_id: If provided and sticky=True, returns same proxy for this shop
            strategy: Selection strategy ('weighted', 'round_robin', 'random')
            proxy_type: Filter by type ('datacenter', 'residential', 'mobile')
            country: Filter by country code
            sticky: If True, use sticky sessions for shop_id
            
        Returns:
            ProxyConfig if proxy available, None otherwise
        """
        # Check sticky session first
        if sticky and shop_id:
            sticky_proxy = await self._get_sticky_proxy(shop_id)
            if sticky_proxy:
                return sticky_proxy
        
        # Import here to avoid circular imports
        from app.models.proxy import Proxy
        
        # Build query for active proxies not in quarantine
        now = datetime.utcnow()
        query = (
            select(Proxy)
            .where(Proxy.status == "active")
            .where(
                (Proxy.last_failure_at.is_(None)) |
                (Proxy.last_failure_at < now - timedelta(minutes=5))
            )
        )
        
        if proxy_type:
            query = query.where(Proxy.proxy_type == proxy_type)
        if country:
            query = query.where(Proxy.country == country)
        
        # Order by strategy
        if strategy == "weighted":
            query = query.order_by(Proxy.success_rate.desc())
        elif strategy == "random":
            pass
        else:  # round_robin
            query = query.order_by(Proxy.id)
        
        result = await self.db.execute(query)
        proxies = result.scalars().all()
        
        # Filter out quarantined proxies
        now_ts = time.time()
        available_proxies = [
            p for p in proxies
            if self._quarantine_until.get(p.id, 0) < now_ts
        ]
        
        if not available_proxies:
            return None
        
        # Select proxy
        if strategy == "random":
            proxy = random.choice(available_proxies)
        elif strategy == "round_robin":
            proxy = available_proxies[self._round_robin_index % len(available_proxies)]
            self._round_robin_index += 1
        else:  # weighted
            top_proxies = [p for p in available_proxies if p.success_rate >= 0.9]
            if top_proxies:
                proxy = random.choice(top_proxies)
            else:
                proxy = available_proxies[0]
        
        # Decrypt password
        password = None
        if proxy.password_encrypted:
            password = decrypt_api_key(proxy.password_encrypted)
        
        proxy_config = ProxyConfig(
            id=proxy.id,
            host=proxy.host,
            port=proxy.port,
            username=proxy.username,
            password=password,
            protocol=proxy.protocol,
        )
        
        # Set sticky session
        if sticky and shop_id:
            self._sticky_sessions[shop_id] = (proxy.id, time.time())
        
        return proxy_config
    
    async def _get_sticky_proxy(self, shop_id: int) -> Optional[ProxyConfig]:
        """Get sticky session proxy if still valid."""
        if shop_id not in self._sticky_sessions:
            return None
        
        proxy_id, assigned_at = self._sticky_sessions[shop_id]
        
        # Check TTL
        if time.time() - assigned_at > self._sticky_session_ttl:
            del self._sticky_sessions[shop_id]
            return None
        
        # Check quarantine
        if self._quarantine_until.get(proxy_id, 0) > time.time():
            del self._sticky_sessions[shop_id]
            return None
        
        # Fetch proxy from DB
        from app.models.proxy import Proxy
        query = select(Proxy).where(Proxy.id == proxy_id, Proxy.status == "active")
        result = await self.db.execute(query)
        proxy = result.scalar_one_or_none()
        
        if not proxy:
            del self._sticky_sessions[shop_id]
            return None
        
        password = None
        if proxy.password_encrypted:
            password = decrypt_api_key(proxy.password_encrypted)
        
        return ProxyConfig(
            id=proxy.id,
            host=proxy.host,
            port=proxy.port,
            username=proxy.username,
            password=password,
            protocol=proxy.protocol,
        )
    
    def clear_sticky_session(self, shop_id: int):
        """Clear sticky session for a shop (call when task ends)."""
        self._sticky_sessions.pop(shop_id, None)
    
    async def report_success(
        self,
        proxy_config: ProxyConfig,
        response_time_ms: int,
        shop_id: Optional[int] = None,
        endpoint: Optional[str] = None,
    ):
        """Report successful request through proxy."""
        await self._update_proxy_stats(
            proxy_id=proxy_config.id,
            success=True,
            response_time_ms=response_time_ms,
            shop_id=shop_id,
            endpoint=endpoint,
        )
    
    async def report_failure(
        self,
        proxy_config: ProxyConfig,
        status_code: int,
        error_message: Optional[str] = None,
        shop_id: Optional[int] = None,
        endpoint: Optional[str] = None,
    ):
        """
        Report failed request and quarantine if needed.
        
        Status codes:
            - 403: Banned → 30 min quarantine
            - 429: Rate limited → 15 min quarantine
            - 5xx: Server error → 5 min quarantine
            - 0: Connection error → 5 min quarantine
        """
        # Determine quarantine duration
        quarantine_duration = None
        is_ban = False
        
        if status_code == 403:
            quarantine_duration = QUARANTINE_DURATION_403
            is_ban = True
        elif status_code == 429:
            quarantine_duration = QUARANTINE_DURATION_429
        elif status_code >= 500 or status_code == 0:
            quarantine_duration = QUARANTINE_DURATION_ERROR
        
        # Set quarantine
        if quarantine_duration:
            quarantine_until = time.time() + quarantine_duration.total_seconds()
            self._quarantine_until[proxy_config.id] = quarantine_until
            
            # Clear sticky session if this proxy was assigned
            for shop, (pid, _) in list(self._sticky_sessions.items()):
                if pid == proxy_config.id:
                    del self._sticky_sessions[shop]
        
        await self._update_proxy_stats(
            proxy_id=proxy_config.id,
            success=False,
            status_code=status_code,
            error_message=error_message,
            is_ban=is_ban,
            shop_id=shop_id,
            endpoint=endpoint,
        )
    
    async def _update_proxy_stats(
        self,
        proxy_id: int,
        success: bool,
        response_time_ms: int = 0,
        status_code: int = 0,
        error_message: Optional[str] = None,
        is_ban: bool = False,
        shop_id: Optional[int] = None,
        endpoint: Optional[str] = None,
    ):
        """Update proxy statistics and log usage."""
        from app.models.proxy import Proxy
        
        now = datetime.utcnow()
        
        # Update proxy record
        query = select(Proxy).where(Proxy.id == proxy_id)
        result = await self.db.execute(query)
        proxy = result.scalar_one_or_none()
        
        if proxy:
            if success:
                proxy.success_count += 1
                proxy.last_success_at = now
            else:
                proxy.failure_count += 1
                proxy.last_failure_at = now
                if is_ban:
                    proxy.status = "banned"
            
            # Recalculate success rate
            total = proxy.success_count + proxy.failure_count
            if total > 0:
                proxy.success_rate = proxy.success_count / total
            
            proxy.last_checked_at = now
        
        # Log to proxy_usage_log
        await self.db.execute(
            """
            INSERT INTO proxy_usage_log 
            (proxy_id, shop_id, endpoint, method, status_code, response_time_ms, is_success, error_message)
            VALUES (:proxy_id, :shop_id, :endpoint, :method, :status_code, :response_time_ms, :is_success, :error_message)
            """,
            {
                "proxy_id": proxy_id,
                "shop_id": shop_id,
                "endpoint": endpoint,
                "method": "GET",  # Will be passed properly later
                "status_code": status_code if not success else 200,
                "response_time_ms": response_time_ms,
                "is_success": success,
                "error_message": error_message,
            }
        )
        
        await self.db.commit()
    
    async def get_active_count(self) -> int:
        """Get count of active, non-quarantined proxies."""
        from app.models.proxy import Proxy
        
        query = select(Proxy).where(Proxy.status == "active")
        result = await self.db.execute(query)
        proxies = result.scalars().all()
        
        now_ts = time.time()
        return len([p for p in proxies if self._quarantine_until.get(p.id, 0) < now_ts])
    
    async def get_quarantine_status(self) -> Dict[int, float]:
        """Get quarantine status for monitoring."""
        now_ts = time.time()
        return {
            pid: remaining
            for pid, until in self._quarantine_until.items()
            if (remaining := until - now_ts) > 0
        }


# ===================
# Task-level helpers
# ===================

async def get_proxy_for_task(
    db: AsyncSession,
    shop_id: int,
    prefer_type: str = "residential",
) -> tuple[ProxyProvider, Optional[ProxyConfig]]:
    """
    Get a proxy for a Celery task with sticky session.
    
    Returns provider to report results and clear session.
    
    Usage in task:
        provider, proxy = await get_proxy_for_task(db, shop_id=1)
        try:
            # use proxy...
            await provider.report_success(proxy, response_time_ms)
        except Exception as e:
            await provider.report_failure(proxy, status_code)
        finally:
            provider.clear_sticky_session(shop_id)
    """
    provider = ProxyProvider(db)
    proxy = await provider.get_proxy(
        shop_id=shop_id,
        strategy="weighted",
        proxy_type=prefer_type,
        sticky=True,
    )
    return provider, proxy
