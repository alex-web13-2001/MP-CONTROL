"""
WB Paid Storage Service — Fetch actual paid storage costs per SKU.

API: 3-step async report:
  1. GET /api/v1/paid_storage?dateFrom=...&dateTo=...  → taskId
  2. GET /api/v1/paid_storage/tasks/{taskId}/status    → done/processing
  3. GET /api/v1/paid_storage/tasks/{taskId}/download   → list[dict]

Limit: max 8 days per request.
Reports expire after 2 hours.

Target: ClickHouse fact_wb_paid_storage
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Any

import httpx

logger = logging.getLogger(__name__)

WB_BASE_URL = "https://seller-analytics-api.wildberries.ru"
MAX_DAYS_PER_REQUEST = 7  # API limit is 8, use 7 for safety


class WBPaidStorageService:
    """Fetch WB paid storage reports via async report API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Authorization": api_key}

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        max_retries: int = 5,
        **kwargs,
    ) -> httpx.Response:
        """
        HTTP request with retry + exponential backoff for 429 rate limiting
        and transient network errors (ConnectError, timeout, etc.).
        """
        last_exc = None
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=60) as client:
                    resp = await getattr(client, method)(url, headers=self.headers, **kwargs)
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout, OSError) as e:
                wait = min(10 * (2 ** attempt), 120)
                logger.warning("WBPaidStorage: network error %s, waiting %ds (attempt %d/%d)",
                               type(e).__name__, wait, attempt + 1, max_retries)
                last_exc = e
                await asyncio.sleep(wait)
                continue

            if resp.status_code == 429:
                wait = min(10 * (2 ** attempt), 120)  # 10, 20, 40, 80, 120 sec
                logger.warning("WBPaidStorage: 429 rate limit, waiting %ds (attempt %d/%d)",
                               wait, attempt + 1, max_retries)
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        if last_exc:
            raise last_exc
        raise httpx.HTTPStatusError(
            "429 rate limit exceeded after retries",
            request=httpx.Request(method.upper(), url),
            response=resp,
        )

    async def create_report(self, date_from: date, date_to: date) -> str:
        """
        Create a paid storage report task.
        """
        params = {
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
        }

        resp = await self._request_with_retry("get", f"{WB_BASE_URL}/api/v1/paid_storage", params=params)
        data = resp.json()

        task_id = data.get("data", {}).get("taskId", "")
        if not task_id:
            raise ValueError(f"No taskId in response: {data}")

        logger.info("WBPaidStorage: created report task %s for %s — %s",
                     task_id, date_from, date_to)
        return task_id

    async def check_status(self, task_id: str) -> str:
        """
        Check report task status.
        """
        resp = await self._request_with_retry(
            "get", f"{WB_BASE_URL}/api/v1/paid_storage/tasks/{task_id}/status"
        )
        data = resp.json()
        status = data.get("data", {}).get("status", "unknown")
        return status

    async def download_report(self, task_id: str) -> list[dict[str, Any]]:
        """
        Download completed report data with retry on 429.
        """
        resp = await self._request_with_retry(
            "get", f"{WB_BASE_URL}/api/v1/paid_storage/tasks/{task_id}/download"
        )
        data = resp.json()

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data", [])
            if isinstance(items, dict):
                items = []
        else:
            items = []

        logger.info("WBPaidStorage: downloaded %d items for task %s", len(items), task_id)
        return items

    async def fetch_period(
        self,
        date_from: date,
        date_to: date,
        poll_interval: float = 10.0,
        max_poll_attempts: int = 30,
    ) -> list[dict[str, Any]]:
        """
        Full cycle: create → poll → download for a single period (max 8 days).

        Args:
            date_from: Start date
            date_to: End date
            poll_interval: Seconds between status checks
            max_poll_attempts: Max polling attempts before giving up

        Returns:
            List of storage data items
        """
        task_id = await self.create_report(date_from, date_to)

        # Poll until done
        for attempt in range(max_poll_attempts):
            await asyncio.sleep(poll_interval)
            status = await self.check_status(task_id)
            logger.debug("WBPaidStorage: task %s status=%s (attempt %d)",
                         task_id, status, attempt + 1)

            if status == "done":
                return await self.download_report(task_id)
            elif status in ("purged", "canceled"):
                logger.warning("WBPaidStorage: task %s status=%s, aborting", task_id, status)
                return []

        logger.error("WBPaidStorage: task %s timed out after %d attempts",
                     task_id, max_poll_attempts)
        return []

    async def fetch_date_range(
        self,
        date_from: date,
        date_to: date,
        on_progress: callable = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch paid storage for an arbitrary date range by splitting into 7-day chunks.

        Args:
            date_from: Start date
            date_to: End date
            on_progress: Optional callback(chunk_idx, total_chunks, items_so_far)

        Returns:
            Combined list of all storage data items
        """
        all_items = []
        chunks = []

        # Split into 7-day chunks
        current = date_from
        while current <= date_to:
            chunk_end = min(current + timedelta(days=MAX_DAYS_PER_REQUEST - 1), date_to)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)

        logger.info("WBPaidStorage: fetching %s — %s in %d chunks",
                     date_from, date_to, len(chunks))

        for i, (chunk_start, chunk_end) in enumerate(chunks):
            items = await self.fetch_period(chunk_start, chunk_end)
            all_items.extend(items)

            if on_progress:
                on_progress(i + 1, len(chunks), len(all_items))

            # Delay between chunks to respect WB rate limits
            if i < len(chunks) - 1:
                await asyncio.sleep(5)

        logger.info("WBPaidStorage: total %d items for %s — %s",
                     len(all_items), date_from, date_to)
        return all_items

    @staticmethod
    def prepare_ch_rows(
        items: list[dict[str, Any]],
        shop_id: int,
    ) -> list[tuple]:
        """
        Convert API response to ClickHouse insert rows.

        Returns list of tuples matching fact_wb_paid_storage columns.
        """
        rows = []
        now = datetime.utcnow()

        for item in items:
            try:
                date_str = item.get("date", "")
                if not date_str:
                    continue

                dt = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).date()

                # Parse original_date
                orig_str = item.get("originalDate", "")
                if orig_str:
                    orig_date = datetime.fromisoformat(
                        orig_str.replace("Z", "+00:00")
                    ).date()
                else:
                    orig_date = dt

                nm_id = item.get("nmId", 0)
                if not nm_id:
                    continue

                rows.append((
                    dt,                                          # dt
                    shop_id,                                     # shop_id
                    str(item.get("vendorCode", "")),             # vendor_code
                    int(nm_id),                                  # nm_id
                    str(item.get("warehouse", "")),              # warehouse
                    int(item.get("officeId", 0)),                # office_id
                    float(item.get("warehouseCoef", 0)),         # warehouse_coef
                    float(item.get("logWarehouseCoef", 0)),      # log_warehouse_coef
                    float(item.get("volume", 0)),                # volume_liters
                    str(item.get("calcType", "")),               # calc_type
                    float(item.get("warehousePrice", 0)),        # warehouse_price
                    int(item.get("barcodesCount", 0)),           # barcodes_count
                    int(item.get("palletPlaceCode", 0)),         # pallet_place_code
                    float(item.get("palletCount", 0)),           # pallet_count
                    orig_date,                                   # original_date
                    float(item.get("loyaltyDiscount", 0)),       # loyalty_discount
                    str(item.get("tariffFixDate", "")),          # tariff_fix_date
                    str(item.get("tariffLowerDate", "")),        # tariff_lower_date
                    int(item.get("giId", 0)),                    # gi_id
                    str(item.get("barcode", "")),                # barcode
                    str(item.get("brand", "")),                  # brand
                    str(item.get("subject", "")),                # subject
                    now,                                         # updated_at
                ))
            except Exception as e:
                logger.warning("WBPaidStorage: skip item: %s — %s",
                               item.get("vendorCode", "?"), e)

        logger.info("WBPaidStorage: prepared %d CH rows for shop_id=%d",
                     len(rows), shop_id)
        return rows
