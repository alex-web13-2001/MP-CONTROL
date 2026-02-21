"""
Ozon Ads Service — Campaigns, Bids & Statistics from Ozon Performance API.

Authentication: OAuth2 client_credentials (SEPARATE from Seller API).
Base URL: https://api-performance.ozon.ru

Tested endpoints (live):
    POST /api/client/token                               → access_token (TTL 30 min)
    GET  /api/client/campaign                             → 64 campaigns
    GET  /api/client/campaign/{id}/v2/products            → SKU + bid (real-time!)
    GET  /api/client/campaign/{id}/products/bids/competitive → market benchmark bids
    PUT  /api/client/campaign/{id}/products               → update bids
    POST /api/client/statistics                           → UUID (async report)
    GET  /api/client/statistics/{UUID}                    → report status
    GET  /api/client/statistics/report?UUID=              → CSV data

Bid format: microroubles (14000000 = 14 RUB).

Data flow:
    1. monitor_ozon_bids (15 min): /v2/products → log_ozon_bids (ClickHouse)
    2. sync_ozon_ad_stats (60 min): /statistics → UUID → CSV → fact_ozon_ad_daily
    3. backfill_ozon_ads (one-time): same as #2, week by week
"""

import asyncio
import csv
import io
import logging
import os
import re
import time
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx
import clickhouse_connect
from clickhouse_connect.driver.client import Client as ClickHouseClient

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ozon_performance_auth import OzonPerformanceAuth
from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────
MICROROUBLES = 1_000_000  # bid=14000000 → 14 RUB

# ClickHouse tables
CH_BIDS_TABLE = "mms_analytics.log_ozon_bids"
CH_STATS_TABLE = "mms_analytics.fact_ozon_ad_daily"

# Report poll settings
REPORT_POLL_INTERVAL = 10   # seconds between status checks (reduced API load)
REPORT_POLL_MAX_WAIT = 300  # max seconds to wait for report (Ozon generates slowly)

# Retry settings for 429 / transient errors
RETRY_MAX_ATTEMPTS = 3     # max retries per batch
RETRY_PAUSE_SECONDS = 60   # pause between retries
BATCH_PAUSE_SECONDS = 30   # pause between successful batches (Ozon: 1 concurrent download/account)


def _bid_to_rub(bid_micro: str) -> float:
    """Convert microroubles bid to roubles."""
    try:
        return int(bid_micro) / MICROROUBLES
    except (ValueError, TypeError):
        return 0.0


def _safe_float(val) -> float:
    """Safe convert to float."""
    if val is None or val == "":
        return 0.0
    try:
        return float(str(val).replace(",", ".").replace("\xa0", ""))
    except (ValueError, TypeError):
        return 0.0


def _safe_int(val) -> int:
    """Safe convert to int."""
    if val is None or val == "":
        return 0
    try:
        return int(float(str(val).replace(",", ".").replace("\xa0", "")))
    except (ValueError, TypeError):
        return 0


# ── Ozon Ads Service ──────────────────────────────────────
class OzonAdsService:
    """
    Fetch campaigns, bids, and statistics from Ozon Performance API.

    Uses MarketplaceClient for:
        - Proxy rotation (sticky sessions)
        - Rate limiting (Redis-synced)
        - Circuit breaker (auto-disable on auth errors)
        - JA3 fingerprint spoofing

    Auth: OAuth2 Bearer (separate from Seller API, handled by OzonPerformanceAuth).
    Base URL: https://api-performance.ozon.ru (marketplace='ozon_performance')
    """

    def __init__(
        self,
        db: AsyncSession,
        shop_id: int,
        perf_client_id: str,
        perf_client_secret: str,
        redis_client=None,
    ):
        self.db = db
        self.shop_id = shop_id
        self.perf_client_id = perf_client_id
        self.perf_client_secret = perf_client_secret
        self.redis_client = redis_client
        self.auth = OzonPerformanceAuth(
            client_id=perf_client_id,
            client_secret=perf_client_secret,
            redis_client=redis_client,
        )

    async def _reset_rate_limiter_backoff(self):
        """Reset internal rate limiter backoff to break the vicious cycle.

        Without this, a single 429 from Ozon causes our MarketplaceClient
        rate limiter to set exponential backoff. Subsequent retry attempts
        get blocked by our OWN backoff (not Ozon's), which counts as
        another 429, increasing backoff further — a death spiral.

        We reset before each retry so the request actually reaches Ozon.
        """
        if not self.redis_client:
            return
        backoff_key = f"mms:ratelimit:{self.shop_id}:ozon_performance:backoff"
        count_key = f"mms:ratelimit:{self.shop_id}:ozon_performance:429_count"
        deleted = await self.redis_client.delete(backoff_key, count_key)
        if deleted:
            logger.info("Reset %d rate-limiter keys before retry", deleted)

    async def _request(
        self,
        method: str,
        path: str,
        json: dict = None,
        params: dict = None,
        timeout: int = 20,
    ):
        """
        Make authenticated request via MarketplaceClient (proxy + rate limit).

        OAuth2 Bearer token is passed via headers kwarg, which
        MarketplaceClient._make_request pops and merges into final headers.

        Returns MarketplaceResponse with .status_code, .data, .is_success, .error.
        """
        token = await self.auth.get_token()
        bearer_headers = {"Authorization": f"Bearer {token}"}

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="ozon_performance",
            max_retries=1,  # No retry inside client — 429 handled in fetch_statistics()
        ) as client:
            response = await client.request(
                method,
                path,
                json=json,
                params=params,
                headers=bearer_headers,
            )

        return response

    # ── Campaigns ──────────────────────────────────────────

    async def get_campaigns(
        self,
        state: str = None,
        adv_object_type: str = None,
    ) -> List[dict]:
        """
        Get all campaigns.

        Args:
            state: Filter by state (CAMPAIGN_STATE_RUNNING, CAMPAIGN_STATE_INACTIVE, etc.)
            adv_object_type: Filter by type (SKU, BANNER, SEARCH_PROMO)

        Returns list of campaign dicts with id, title, state, advObjectType, dailyBudget.
        """
        params = {}
        if state:
            params["state"] = state
        if adv_object_type:
            params["advObjectType"] = adv_object_type

        response = await self._request("GET", "/api/client/campaign", params=params)

        if not response.is_success:
            logger.error(
                "Ozon campaigns error: status=%s error=%s",
                response.status_code, response.error,
            )
            return []

        data = response.data if isinstance(response.data, dict) else {}
        campaigns = data.get("list", [])
        logger.info("Ozon: found %d campaigns", len(campaigns))
        return campaigns

    # ── Bids (Real-Time) ───────────────────────────────────

    async def get_campaign_products(
        self,
        campaign_id: int,
    ) -> List[dict]:
        """
        Get products with their current bids.

        GET /api/client/campaign/{id}/v2/products

        Returns: [{sku, bid, title}, ...]
        bid is in microroubles (14000000 = 14 RUB).
        """
        response = await self._request(
            "GET",
            f"/api/client/campaign/{campaign_id}/v2/products",
        )

        if not response.is_success:
            logger.warning(
                "Ozon products error for campaign %d: %s %s",
                campaign_id, response.status_code, response.error,
            )
            return []

        data = response.data if isinstance(response.data, dict) else {}
        products = data.get("products", [])
        logger.debug(
            "Campaign %d: %d products", campaign_id, len(products),
        )
        return products

    async def get_competitive_bids(
        self,
        campaign_id: int,
        skus: List[str],
    ) -> List[dict]:
        """
        Get competitive (market benchmark) bids for SKUs.

        GET /api/client/campaign/{id}/products/bids/competitive?skus=...

        Returns: [{sku, bid}, ...] — bid in microroubles.
        """
        params = {"skus": ",".join(str(s) for s in skus)}
        response = await self._request(
            "GET",
            f"/api/client/campaign/{campaign_id}/products/bids/competitive",
            params=params,
        )

        if not response.is_success:
            logger.warning(
                "Ozon competitive bids error: %s %s",
                response.status_code, response.error,
            )
            return []

        data = response.data if isinstance(response.data, dict) else {}
        return data.get("bids", [])

    async def get_all_bids(self) -> List[dict]:
        """
        Get current bids for ALL running campaigns.

        Returns list of {campaign_id, sku, bid_rub, bid_micro, title}
        """
        campaigns = await self.get_campaigns(state="CAMPAIGN_STATE_RUNNING")
        all_bids = []

        for camp in campaigns:
            campaign_id = camp.get("id")
            if not campaign_id:
                continue

            products = await self.get_campaign_products(campaign_id)
            for p in products:
                all_bids.append({
                    "campaign_id": int(campaign_id),
                    "sku": int(p.get("sku", 0)),
                    "bid_micro": int(p.get("bid", 0)),
                    "bid_rub": _bid_to_rub(p.get("bid", "0")),
                    "title": p.get("title", ""),
                })

            await asyncio.sleep(0.3)  # rate limit safety

        logger.info("Fetched bids for %d products across %d campaigns",
                     len(all_bids), len(campaigns))
        return all_bids

    # ── Statistics (Async CSV) ─────────────────────────────

    async def order_report(
        self,
        campaign_ids: List[int],
        date_from: str,
        date_to: str,
        group_by: str = "DATE",
    ) -> Optional[str]:
        """
        Order async statistics report.

        POST /api/client/statistics

        Args:
            campaign_ids: List of campaign IDs
            date_from: "YYYY-MM-DD"
            date_to: "YYYY-MM-DD"
            group_by: "DATE", "MONTH", "NO_GROUP_BY"

        Returns UUID of the report, or None on error.
        """
        response = await self._request(
            "POST",
            "/api/client/statistics",
            json={
                "campaigns": [str(c) for c in campaign_ids],
                "dateFrom": date_from,
                "dateTo": date_to,
                "groupBy": group_by,
            },
        )

        if not response.is_success:
            status = getattr(response, 'status_code', 0)
            logger.error(
                "Ozon statistics order error: %s %s",
                status, response.error,
            )
            # Return status code so caller can distinguish 429 from other errors
            return None

        data = response.data if isinstance(response.data, dict) else {}
        uuid = data.get("UUID")
        logger.info("Ozon report ordered: UUID=%s", uuid)
        return uuid

    async def wait_for_report(self, uuid: str) -> Optional[str]:
        """
        Poll report status until ready.

        GET /api/client/statistics/{UUID}

        Uses raw httpx (not MarketplaceClient) to avoid rate limiter overhead
        during polling. Polling is lightweight GET, doesn't need proxy/JA3.

        Returns download link when state=OK, None on timeout/error.
        """
        start = time.time()
        token = await self.auth.get_token()
        url = f"https://api-performance.ozon.ru/api/client/statistics/{uuid}"

        while time.time() - start < REPORT_POLL_MAX_WAIT:
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(
                        url,
                        headers={"Authorization": f"Bearer {token}"},
                    )

                if resp.status_code != 200:
                    logger.warning("Ozon report status error: %s", resp.status_code)
                    # Refresh token on 401
                    if resp.status_code == 401:
                        token = await self.auth.get_token()
                    await asyncio.sleep(REPORT_POLL_INTERVAL)
                    continue

                data = resp.json()
                state = data.get("state")

                if state == "OK":
                    link = data.get("link", f"/api/client/statistics/report?UUID={uuid}")
                    logger.info("Ozon report ready: UUID=%s", uuid)
                    return link

                if state in ("ERROR", "FAILED"):
                    logger.error("Ozon report failed: UUID=%s state=%s", uuid, state)
                    return None

                logger.debug("Ozon report pending: UUID=%s state=%s", uuid, state)
            except Exception as e:
                logger.warning("Ozon report poll error: %s", e)

            await asyncio.sleep(REPORT_POLL_INTERVAL)

        logger.error("Ozon report timeout: UUID=%s (waited %ds)", uuid, REPORT_POLL_MAX_WAIT)
        return None

    async def download_report(self, link: str) -> str:
        """
        Download report CSV content.

        GET /api/client/statistics/report?UUID=...

        Ozon returns:
        - Plain CSV for single-campaign reports
        - ZIP archive with multiple CSVs for batch reports (10+ campaigns)

        We detect the format and handle both cases.
        Returns concatenated CSV string.
        """
        import zipfile
        import io

        # Use httpx directly (not MarketplaceClient) because we need raw bytes
        # for ZIP detection. MarketplaceClient tries to json-parse everything.
        token = await self.auth.get_token()
        url = f"https://api-performance.ozon.ru{link}"

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}", "Accept": "*/*"},
            )

        if response.status_code != 200:
            logger.error(
                "Ozon report download error: status=%s body=%s",
                response.status_code, response.text[:200],
            )
            return ""

        raw_bytes = response.content

        # Detect ZIP (starts with PK\x03\x04)
        if raw_bytes[:4] == b'PK\x03\x04':
            logger.info("Report is ZIP archive (%d bytes), extracting CSVs...", len(raw_bytes))
            csv_parts = []
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                for name in zf.namelist():
                    if name.endswith('.csv'):
                        csv_content = zf.read(name).decode('utf-8-sig')
                        csv_parts.append(csv_content)
                        logger.debug("Extracted %s (%d chars)", name, len(csv_content))
            logger.info("Extracted %d CSV files from ZIP", len(csv_parts))
            return "\n".join(csv_parts)

        # Plain CSV (single campaign)
        return raw_bytes.decode('utf-8-sig')

    @staticmethod
    def parse_csv_report(csv_text: str, shop_id: int) -> List[dict]:
        """
        Parse Ozon Performance CSV report into structured dicts.

        CSV format (semicolon-separated, BOM-prefixed):
            \ufeff;Кампания по продвижению товаров № XXXXX, период ...
            День;sku;Название;Цена₽;Показы;Клики;CTR%;В корзину;
            Ср.стоимость клика₽;Расход₽;Заказы;Продажи₽;
            Заказы модели;Продажи модели₽;ДРР%;Заказано на₽;Общий ДРР;Дата добавления
            dd.mm.yyyy;SKU;...
            Всего;...

        For multi-campaign ZIP reports, multiple CSVs are concatenated.
        Each CSV starts with its own header containing "№ XXXXX".

        Returns list of dicts ready for ClickHouse insert.
        """
        rows = []

        # Strip BOM and whitespace
        csv_text = csv_text.strip().lstrip("\ufeff")
        lines = csv_text.split("\n")

        if not lines:
            return rows

        # campaign_id is updated dynamically as we encounter new headers
        campaign_id = 0

        # Parse data lines
        for line in lines:
            line = line.strip().lstrip("\ufeff")
            if not line:
                continue

            # Update campaign_id when we encounter a new campaign header
            if "Кампания" in line and "№" in line:
                match = re.search(r"№\s*(\d+)", line)
                if match:
                    campaign_id = int(match.group(1))
                continue

            # Skip non-data rows
            if line.startswith("День;") or line.startswith("Всего"):
                continue

            parts = line.split(";")
            if len(parts) < 14:
                continue

            # Parse date (dd.mm.yyyy)
            date_str = parts[0].strip()
            try:
                dt = datetime.strptime(date_str, "%d.%m.%Y").date()
            except (ValueError, IndexError):
                continue

            sku = _safe_int(parts[1])
            if not sku:
                continue

            rows.append({
                "dt": dt,
                "shop_id": shop_id,
                "campaign_id": campaign_id,
                "sku": sku,
                "views": _safe_int(parts[4]),
                "clicks": _safe_int(parts[5]),
                "ctr": _safe_float(parts[6]),
                "add_to_cart": _safe_int(parts[7]),
                "avg_cpc": _safe_float(parts[8]),
                "money_spent": _safe_float(parts[9]),
                "orders": _safe_int(parts[10]),
                "revenue": _safe_float(parts[11]),
                "model_orders": _safe_int(parts[12]),
                "model_revenue": _safe_float(parts[13]),
                "drr": _safe_float(parts[14]) if len(parts) > 14 else 0.0,
            })

        logger.info("Parsed %d rows from Ozon CSV (%d campaigns)", len(rows),
                     len(set(r["campaign_id"] for r in rows)) if rows else 0)
        return rows

    async def fetch_statistics(
        self,
        shop_id: int,
        campaign_ids: List[int],
        date_from: str,
        date_to: str,
        batch_size: int = 10,
    ) -> List[dict]:
        """
        Full pipeline: order → wait → download → parse.

        Ozon API limit: max 10 campaigns per report.
        So we batch campaign_ids into groups of 10.

        On 429 / transient errors, retries up to RETRY_MAX_ATTEMPTS times
        with RETRY_PAUSE_SECONDS pause between attempts.

        Returns parsed rows ready for ClickHouse.
        """
        all_rows = []

        # Batch campaign IDs (API limit: 10 per request)
        batches = [
            campaign_ids[i:i + batch_size]
            for i in range(0, len(campaign_ids), batch_size)
        ]

        logger.info(
            "Fetching stats: %d campaigns in %d batches (%s → %s)",
            len(campaign_ids), len(batches), date_from, date_to,
        )

        for batch_idx, batch in enumerate(batches):
            logger.info("Stats batch %d/%d: campaigns %s", batch_idx + 1, len(batches), batch)

            batch_success = False
            for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                # Step 1: Order report
                uuid = await self.order_report(batch, date_from, date_to)
                if not uuid:
                    if attempt < RETRY_MAX_ATTEMPTS:
                        logger.warning(
                            "Batch %d/%d: order_report failed (attempt %d/%d), "
                            "pausing %ds before retry...",
                            batch_idx + 1, len(batches),
                            attempt, RETRY_MAX_ATTEMPTS, RETRY_PAUSE_SECONDS,
                        )
                        await asyncio.sleep(RETRY_PAUSE_SECONDS)
                        # Reset internal backoff so retry actually reaches Ozon
                        await self._reset_rate_limiter_backoff()
                        continue
                    else:
                        logger.error(
                            "Batch %d/%d: order_report failed after %d attempts, skipping",
                            batch_idx + 1, len(batches), RETRY_MAX_ATTEMPTS,
                        )
                        break

                # Step 2: Wait for report
                link = await self.wait_for_report(uuid)
                if not link:
                    if attempt < RETRY_MAX_ATTEMPTS:
                        logger.warning(
                            "Batch %d/%d: wait_for_report failed (attempt %d/%d), "
                            "pausing %ds before retry...",
                            batch_idx + 1, len(batches),
                            attempt, RETRY_MAX_ATTEMPTS, RETRY_PAUSE_SECONDS,
                        )
                        await asyncio.sleep(RETRY_PAUSE_SECONDS)
                        await self._reset_rate_limiter_backoff()
                        continue
                    else:
                        logger.error(
                            "Batch %d/%d: wait_for_report failed after %d attempts, skipping",
                            batch_idx + 1, len(batches), RETRY_MAX_ATTEMPTS,
                        )
                        break

                # Step 3: Download report
                csv_text = await self.download_report(link)
                if not csv_text:
                    if attempt < RETRY_MAX_ATTEMPTS:
                        logger.warning(
                            "Batch %d/%d: download_report failed (attempt %d/%d), "
                            "pausing %ds before retry...",
                            batch_idx + 1, len(batches),
                            attempt, RETRY_MAX_ATTEMPTS, RETRY_PAUSE_SECONDS,
                        )
                        await asyncio.sleep(RETRY_PAUSE_SECONDS)
                        await self._reset_rate_limiter_backoff()
                        continue
                    else:
                        logger.error(
                            "Batch %d/%d: download_report failed after %d attempts, skipping",
                            batch_idx + 1, len(batches), RETRY_MAX_ATTEMPTS,
                        )
                        break

                # Step 4: Parse — success!
                rows = self.parse_csv_report(csv_text, shop_id)
                all_rows.extend(rows)
                batch_success = True
                if attempt > 1:
                    logger.info(
                        "Batch %d/%d: succeeded on attempt %d",
                        batch_idx + 1, len(batches), attempt,
                    )
                break

            # Rate limit between batches (Ozon: 1 concurrent report per account)
            if batch_idx < len(batches) - 1:
                pause = BATCH_PAUSE_SECONDS if batch_success else RETRY_PAUSE_SECONDS
                logger.info("Pausing %ds before next batch...", pause)
                await asyncio.sleep(pause)

        logger.info("Total stats rows from all batches: %d", len(all_rows))
        return all_rows



# ── ClickHouse Loaders ─────────────────────────────────────

class OzonBidsLoader:
    """Insert bid snapshots into ClickHouse log_ozon_bids."""

    def __init__(
        self,
        host: str = "clickhouse",
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "mms_analytics",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self._client: Optional[ClickHouseClient] = None

    def connect(self):
        self._client = clickhouse_connect.get_client(
            host=self.host, port=self.port,
            username=self.username, password=self.password,
            database=self.database,
        )

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    def insert_bids(self, shop_id: int, bids: List[dict]) -> int:
        """Insert bid snapshot rows."""
        if not bids or not self._client:
            return 0

        now = datetime.utcnow()
        rows = []
        for b in bids:
            rows.append([
                now,
                shop_id,
                b["campaign_id"],
                b["sku"],
                b["bid_rub"],
                0.0,  # price — will be enriched from dim_ozon_products later
            ])

        self._client.insert(
            CH_BIDS_TABLE, rows,
            column_names=["timestamp", "shop_id", "campaign_id", "sku", "avg_cpc", "price"],
        )
        logger.info("Inserted %d bid snapshots into ClickHouse", len(rows))
        return len(rows)

    def insert_stats(self, rows: List[dict]) -> int:
        """Insert statistics rows into fact_ozon_ad_daily."""
        if not rows or not self._client:
            return 0

        now = datetime.utcnow()
        ch_rows = []

        # Deduplicate by (shop_id, campaign_id, sku, dt)
        seen = set()
        for r in rows:
            key = (r["shop_id"], r["campaign_id"], r["sku"], str(r["dt"]))
            if key in seen:
                continue
            seen.add(key)

            ch_rows.append([
                r["dt"],
                now,
                r["shop_id"],
                r["campaign_id"],
                r["sku"],
                r["views"],
                r["clicks"],
                r["ctr"],
                r["add_to_cart"],
                r["avg_cpc"],
                r["money_spent"],
                r["orders"],
                r["revenue"],
                r["model_orders"],
                r["model_revenue"],
                r["drr"],
            ])

        columns = [
            "dt", "updated_at", "shop_id", "campaign_id", "sku",
            "views", "clicks", "ctr", "add_to_cart", "avg_cpc",
            "money_spent", "orders", "revenue", "model_orders",
            "model_revenue", "drr",
        ]
        self._client.insert(CH_STATS_TABLE, ch_rows, column_names=columns)

        # Force merge to collapse duplicates immediately
        # (ReplacingMergeTree only deduplicates on background merges or FINAL queries)
        try:
            self._client.command(f"OPTIMIZE TABLE {CH_STATS_TABLE} FINAL")
            logger.info("OPTIMIZE TABLE %s FINAL completed", CH_STATS_TABLE)
        except Exception as e:
            logger.warning("OPTIMIZE TABLE failed (non-critical): %s", e)

        logger.info(
            "Inserted %d stats rows into ClickHouse (deduplicated from %d)",
            len(ch_rows), len(rows),
        )
        return len(ch_rows)

    def get_stats_summary(self, shop_id: int) -> dict:
        """Get summary statistics."""
        if not self._client:
            return {}
        result = self._client.query("""
            SELECT
                count() as total_rows,
                uniq(campaign_id) as unique_campaigns,
                uniq(sku) as unique_skus,
                min(dt) as min_date,
                max(dt) as max_date,
                sum(money_spent) as total_spend,
                sum(orders) as total_orders,
                sum(revenue) as total_revenue
            FROM fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
        """, parameters={"shop_id": shop_id})
        r = result.first_row
        if r:
            return {
                "total_rows": r[0], "unique_campaigns": r[1],
                "unique_skus": r[2], "min_date": str(r[3]),
                "max_date": str(r[4]), "total_spend": float(r[5]),
                "total_orders": r[6], "total_revenue": float(r[7]),
            }
        return {}
