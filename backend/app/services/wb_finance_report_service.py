"""
WB Finance Report Service - Download weekly realization reports from Wildberries.

This service implements the full workflow:
1. Get report IDs via statistics-api (reportDetailByPeriod)
2. Request file generation via common-api
3. Poll for status until done
4. Download the resulting CSV file
"""

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient, MarketplaceResponse


@dataclass
class ReportInfo:
    """Information about a WB weekly report."""
    report_id: int
    date_from: str
    date_to: str
    create_dt: Optional[str] = None


@dataclass
class ReportTaskStatus:
    """Status of a report generation task."""
    task_id: str
    status: str  # 'pending', 'processing', 'done', 'error'
    url: Optional[str] = None
    error: Optional[str] = None


class WBFinanceReportService:
    """
    Service for downloading WB weekly finance reports.
    
    Usage:
        async with WBFinanceReportService(db, shop_id, api_key) as service:
            data = await service.get_report_data("2025-01-01", "2025-01-31")
            # process data...
    """
    
    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        api_key: Optional[str] = None,
        api_key_encrypted: Optional[bytes] = None,
    ):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key
        self.api_key_encrypted = api_key_encrypted
        
        # Client for statistics-api
        self._stats_client: Optional[MarketplaceClient] = None
    
    async def __aenter__(self):
        """Initialize marketplace clients."""
        # Client for statistics-api (get report data)
        self._stats_client = MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_stats",
            api_key=self.api_key,
            api_key_encrypted=self.api_key_encrypted,
        )
        await self._stats_client.__aenter__()
        
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup clients."""
        if self._stats_client:
            await self._stats_client.__aexit__(exc_type, exc_val, exc_tb)
    
    async def get_report_data(
        self,
        date_from: str,
        date_to: str,
        limit: int = 100_000,
    ) -> List[Dict[str, Any]]:
        """
        Get finance report data for a date range.
        
        Uses statistics-api endpoint:
        GET /api/v5/supplier/reportDetailByPeriod
        
        Args:
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            limit: Max rows to fetch
        
        Returns:
            List of report data rows (dicts)
        """
        if not self._stats_client:
            raise RuntimeError("Service not initialized. Use async with.")
        
        endpoint = f"/api/v5/supplier/reportDetailByPeriod?dateFrom={date_from}&dateTo={date_to}&limit={limit}"
        response = await self._stats_client.get(endpoint)
        
        if not response.is_success:
            # 429 errors are handled by MarketplaceClient (retries), 
            # but if we get here it failed completely.
            raise Exception(f"Failed to get report data: {response.status_code} - {response.data}")
        
        # V5 returns data straight away (or wrapped in list/dict)
        data = response.data
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
             # Try common wrapper keys just in case
            return data.get("data", []) or data.get("report", []) or []
            
        return []

    # Alias for compatibility if needed, but better to update calls
    async def sync_reports_for_period(
        self,
        date_from: str,
        date_to: str,
        progress_callback: Optional[callable] = None,
    ) -> List[Dict[str, Any]]:
        """Deprecated: Use get_report_data directly."""
        raise NotImplementedError("Use get_report_data directly with new JSON ingestion flow.")
