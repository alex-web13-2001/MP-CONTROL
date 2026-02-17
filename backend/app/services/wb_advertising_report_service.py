
import asyncio
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)


class WBAdvertisingReportService:
    """
    Service for interacting with WB Advertising API V3.

    Uses MarketplaceClient for:
        - Proxy rotation (sticky sessions)
        - Rate limiting (Redis-synced)
        - Circuit breaker (auto-disable on auth errors)
        - JA3 fingerprint spoofing

    Base URL: https://advert-api.wildberries.ru (marketplace='wildberries_adv')
    """

    def __init__(self, db: AsyncSession, shop_id: int, api_key: str):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key

    async def get_campaigns(self) -> List[Dict[str, Any]]:
        """
        Get list of campaigns from Count endpoint.

        The Count endpoint returns all campaign IDs in advert_list,
        so we extract them directly without needing separate adverts call.
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            try:
                response = await client.get("/adv/v1/promotion/count")

                if not response.is_success:
                    logger.error(f"WB API Count Error: status={response.status_code}, error={response.error}")
                    return []

                count_data = response.data
                logger.info(f"WB Count Response: all={count_data.get('all', 0)} campaigns")

                total_count = count_data.get("all", 0)
                if total_count == 0:
                    logger.info("Seller has no advertising campaigns")
                    return []

                # Extract campaigns directly from count response
                campaigns = []
                if "adverts" in count_data:
                    for group in count_data["adverts"]:
                        adv_type = group.get("type")
                        status = group.get("status")
                        advert_list = group.get("advert_list", [])

                        for advert in advert_list:
                            campaigns.append({
                                "advertId": advert["advertId"],
                                "type": adv_type,
                                "status": status,
                                "changeTime": advert.get("changeTime"),
                            })

                logger.info(f"Extracted {len(campaigns)} campaigns from count response")
                return campaigns

            except Exception as e:
                logger.error(f"Failed to fetch campaign counts: {e}")
                raise e

    async def get_full_stats_v3(self, campaign_ids: List[int], begin_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get full statistics for campaigns using V3 API.

        Method: GET /adv/v3/fullstats
        Query params:
            - ids: comma-separated campaign IDs (max 50)
            - beginDate: YYYY-MM-DD
            - endDate: YYYY-MM-DD

        Max period: 31 days
        Rate Limit: 1 request per minute (handled by MarketplaceClient).

        Response includes advertId at root level and full funnel metrics:
        - views, clicks, atbs (carts), orders, shks (items), sum (spend), sum_price (revenue)
        """
        # Convert campaign IDs to comma-separated string
        ids_str = ",".join(str(cid) for cid in campaign_ids)

        params = {
            "ids": ids_str,
            "beginDate": begin_date,
            "endDate": end_date
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get("/adv/v3/fullstats", params=params)

            if response.is_rate_limited:
                logger.warning("Rate limit hit on /adv/v3/fullstats")
                raise Exception("Rate Limit on /adv/v3/fullstats")

            if response.status_code == 400:
                error_text = response.error or ""
                logger.warning(f"WB API v3/fullstats 400: {error_text}")
                if "no companies" in error_text.lower() or "Invalid" in error_text:
                    return []

            if not response.is_success:
                logger.error(f"WB API v3/fullstats Error: status={response.status_code}, error={response.error}")
                return []

            return response.data

    async def get_campaign_settings(self, campaign_ids: List[int]) -> List[Dict[str, Any]]:
        """
        [DEPRECATED] Get campaign settings including CPM/CPC bids and item lists.

        Method: POST /adv/v1/promotion/adverts (updated Oct 2025)
        
        WARNING: This endpoint is not in current WB API docs and may be removed.
        Use get_adverts_v2() instead, which provides per-nm_id bids and placements.

        Response includes:
        - advertId: campaign ID
        - type: campaign type (9 = unified, formerly 8)
        - status: campaign status
        - unitedParams: bid settings and item lists

        This is used for:
        1. Bid change detection (comparing CPM with Redis state)
        2. Identifying associated items (items in fullstats but not in params)
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v1/promotion/adverts",
                json=campaign_ids,
            )

            if response.is_rate_limited:
                logger.warning("Rate limit on /adv/v1/promotion/adverts")
                raise Exception("Rate Limit on /adv/v1/promotion/adverts")

            if not response.is_success:
                logger.error(f"WB API v1/promotion/adverts Error: status={response.status_code}, error={response.error}")
                return []

            return response.data

    async def get_adverts_v2(
        self,
        campaign_ids: List[int] = None,
        statuses: List[int] = None,
        payment_type: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Get detailed campaign info including bids per nm_id.

        Method: GET /api/advert/v2/adverts
        
        Returns bids_kopecks (search, recommendations) per nm_id,
        bid_type (manual/auto), payment_type (cpm/cpc), placements.
        
        Query params:
            - id: campaign IDs (max 50)
            - status: campaign statuses (-1,4,7,8,9,11)
            - payment_type: 'cpm' or 'cpc'
        """
        params = {}
        if campaign_ids:
            params["id"] = ",".join(str(cid) for cid in campaign_ids[:50])
        if statuses:
            params["status"] = ",".join(str(s) for s in statuses)
        if payment_type:
            params["payment_type"] = payment_type

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/api/advert/v2/adverts",
                params=params,
            )

            if response.is_rate_limited:
                logger.warning("Rate limit on /api/advert/v2/adverts")
                raise Exception("Rate Limit on /api/advert/v2/adverts")

            if not response.is_success:
                logger.error(
                    f"WB API v2/adverts Error: status={response.status_code}, "
                    f"error={response.error}"
                )
                return []

            data = response.data
            # Response wraps in {"adverts": [...]}
            if isinstance(data, dict) and "adverts" in data:
                return data["adverts"]
            # Or could be a list directly
            if isinstance(data, list):
                return data
            return []

    async def get_balance(self) -> Dict[str, Any]:
        """
        Get current advertising balance.

        Method: GET /adv/v1/balance
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get("/adv/v1/balance")

            if not response.is_success:
                logger.error(f"WB API balance Error: status={response.status_code}, error={response.error}")
                return {}

            return response.data
