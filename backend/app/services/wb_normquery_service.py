"""
WB Normquery (Search Cluster) Service — READ operations for search cluster analytics.

Provides methods for retrieving:
- Normquery statistics (aggregated and daily)
- Current bids per cluster
- Active/excluded cluster lists
- Minus phrases
- Recommended bids

All calls go through MarketplaceClient (proxy, rate limiting, circuit breaker).

IMPORTANT:
- Normquery endpoints are only available for campaigns with payment_type=cpm
- Cluster bids are only available for bid_type=manual campaigns
- Bids are returned in KOPECKS (÷100 for rubles)
"""

import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)


class WBNormqueryService:
    """
    Service for WB search cluster (normquery) analytics.

    READ-only operations for UWB (manual bid) campaigns:
    - Stats per cluster (aggregated + daily)
    - Bids per cluster
    - Active/excluded lists
    - Minus phrases
    - Bid recommendations

    Base URL: https://advert-api.wildberries.ru (marketplace='wildberries_adv')
    """

    def __init__(self, db: AsyncSession, shop_id: int, api_key: str):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key

    # ══════════════════════════════════════════════════════════════
    # Statistics
    # ══════════════════════════════════════════════════════════════

    async def get_normquery_stats(
        self,
        items: List[Dict[str, int]],
        date_from: str,
        date_to: str,
    ) -> Dict[str, Any]:
        """
        Get aggregated normquery statistics for a period.

        WB API: POST /adv/v0/normquery/stats
        Only for campaigns with payment_type=cpm.

        Args:
            items: List of {"advert_id": int, "nm_id": int}
            date_from: "YYYY-MM-DD"
            date_to: "YYYY-MM-DD"

        Returns:
            {
              "stats": [{
                "advert_id": 123,
                "nm_id": 456,
                "stats": [{
                  "norm_query": "...",
                  "views": N, "clicks": N, "atbs": N, "orders": N,
                  "avg_pos": F, "cpc": N, "cpm": N, "ctr": F
                }]
              }]
            }
        """
        payload = {
            "from": date_from,
            "to": date_to,
            "items": [
                {"advert_id": item["advert_id"], "nm_id": item["nm_id"]}
                for item in items
            ],
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/stats",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Got stats for {len(items)} items "
                    f"({date_from}→{date_to}) shop={self.shop_id}"
                )
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get stats: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    async def get_normquery_daily_stats(
        self,
        items: List[Dict[str, int]],
        date_from: str,
        date_to: str,
    ) -> Dict[str, Any]:
        """
        Get normquery statistics with daily breakdown.

        WB API: POST /adv/v1/normquery/stats
        Extended version with per-day granularity.

        Args:
            items: List of {"advert_id": int, "nm_id": int}
            date_from: "YYYY-MM-DD"
            date_to: "YYYY-MM-DD"

        Returns similar structure to v0 but with daily breakdowns.
        """
        payload = {
            "from": date_from,
            "to": date_to,
            "items": [
                {"advert_id": item["advert_id"], "nm_id": item["nm_id"]}
                for item in items
            ],
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v1/normquery/stats",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Got daily stats for {len(items)} items "
                    f"({date_from}→{date_to}) shop={self.shop_id}"
                )
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get daily stats: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    # ══════════════════════════════════════════════════════════════
    # Bids per Cluster
    # ══════════════════════════════════════════════════════════════

    async def get_normquery_bids(
        self,
        items: List[Dict[str, int]],
    ) -> Dict[str, Any]:
        """
        Get current bids for search clusters.

        WB API: POST /adv/v0/normquery/get-bids

        Args:
            items: List of {"advert_id": int, "nm_id": int}

        Returns:
            {
              "bids": [{
                "advert_id": 123,
                "nm_id": 456,
                "norm_query": "...",
                "bid": 9000  // kopecks
              }]
            }
        """
        payload = {
            "items": [
                {"advert_id": item["advert_id"], "nm_id": item["nm_id"]}
                for item in items
            ],
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/get-bids",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Got bids for {len(items)} items, "
                    f"shop={self.shop_id}"
                )
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get bids: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    # ══════════════════════════════════════════════════════════════
    # Active/Excluded Clusters
    # ══════════════════════════════════════════════════════════════

    async def get_normquery_list(
        self,
        items: List[Dict[str, int]],
    ) -> Dict[str, Any]:
        """
        Get lists of active and excluded search clusters.

        WB API: POST /adv/v0/normquery/list
        Returns clusters that had at least 100 impressions.

        Args:
            items: List of {"advertId": int, "nmId": int}
                   (NOTE: WB uses camelCase here, unlike other endpoints!)

        Returns:
            {
              "items": [{
                "advertId": 123, "nmId": 456,
                "normQueries": {
                  "active": ["кластер1", ...] or null,
                  "excluded": ["кластер2", ...]
                }
              }]
            }
        """
        payload = {
            "items": [
                {"advertId": item["advert_id"], "nmId": item["nm_id"]}
                for item in items
            ],
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/list",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Got cluster lists for {len(items)} items, "
                    f"shop={self.shop_id}"
                )
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get cluster lists: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    # ══════════════════════════════════════════════════════════════
    # Minus Phrases
    # ══════════════════════════════════════════════════════════════

    async def get_normquery_minus(
        self,
        items: List[Dict[str, int]],
    ) -> Dict[str, Any]:
        """
        Get minus phrases for campaigns.

        WB API: POST /adv/v0/normquery/get-minus

        Args:
            items: List of {"advert_id": int, "nm_id": int}

        Returns:
            {
              "items": [{
                "advert_id": 123, "nm_id": 456,
                "norm_queries": ["фраза1", ...]
              }]
            }
        """
        payload = {
            "items": [
                {"advert_id": item["advert_id"], "nm_id": item["nm_id"]}
                for item in items
            ],
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/get-minus",
                json=payload,
            )

            if response.is_success:
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get minus phrases: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    # ══════════════════════════════════════════════════════════════
    # Bid Recommendations
    # ══════════════════════════════════════════════════════════════

    async def get_bid_recommendations(
        self,
        advert_id: int,
        nm_id: int,
    ) -> Dict[str, Any]:
        """
        Get recommended bids for a specific nm_id in a campaign.

        WB API: GET /api/advert/v0/bids/recommendations
        Only for campaigns with payment_type=cpm.

        Args:
            advert_id: Campaign ID
            nm_id: Product article (nm)

        Returns:
            {
              "advertId": 123,
              "nmId": 456,
              "base": {
                "competitiveBid": {"bidKopecks": 39500},
                "leadersBid": {"bidKopecks": 66900},
                "top2": {"bidKopecks": 0}
              },
              "normQueries": [{
                "normQuery": "футболка",
                "reachMax": {"bidKopecks": 50500, "bidKopecksMin": 49500},
                "reachMedium": {"bidKopecks": 32000},
                "reachMin": {"bidKopecks": 32000}
              }]
            }
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/api/advert/v0/bids/recommendations",
                params={
                    "advertId": advert_id,
                    "nmId": nm_id,
                },
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Got bid recommendations: "
                    f"advert={advert_id} nm={nm_id} shop={self.shop_id}"
                )
                return response.data or {}

            logger.warning(
                f"[normquery] Failed to get recommendations: "
                f"status={response.status_code}, error={response.error}"
            )
            return {}

    # ══════════════════════════════════════════════════════════════
    # Write Operations (for future Phase 3)
    # ══════════════════════════════════════════════════════════════

    async def set_normquery_bids(
        self,
        bids: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Set bids for search clusters.
        Only for manual bid campaigns with payment_type=cpm.

        WB API: POST /adv/v0/normquery/bids

        Args:
            bids: List of {
                "advert_id": int,
                "nm_id": int,
                "norm_query": str,
                "bid": int  // kopecks
            }
        """
        payload = {"bids": bids}

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/bids",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Set {len(bids)} cluster bids, "
                    f"shop={self.shop_id}"
                )
                return {"success": True, "message": f"Установлено {len(bids)} ставок"}

            error_msg = response.error or "Unknown error"
            logger.warning(
                f"[normquery] Failed to set bids: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {"success": False, "message": error_msg}

    async def set_minus_phrases(
        self,
        advert_id: int,
        nm_id: int,
        norm_queries: List[str],
    ) -> Dict[str, Any]:
        """
        Set minus phrases for a campaign.

        WB API: POST /adv/v0/normquery/set-minus
        Works for both manual and unified bid campaigns.

        Args:
            advert_id: Campaign ID
            nm_id: Product nm_id
            norm_queries: List of phrases to exclude
        """
        payload = {
            "advert_id": advert_id,
            "nm_id": nm_id,
            "norm_queries": norm_queries,
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v0/normquery/set-minus",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[normquery] Set {len(norm_queries)} minus phrases: "
                    f"advert={advert_id} nm={nm_id} shop={self.shop_id}"
                )
                return {
                    "success": True,
                    "message": f"Установлено {len(norm_queries)} минус-фраз",
                }

            error_msg = response.error or "Unknown error"
            logger.warning(
                f"[normquery] Failed to set minus phrases: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {"success": False, "message": error_msg}
