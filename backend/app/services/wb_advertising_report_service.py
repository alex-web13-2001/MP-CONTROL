
import asyncio
import logging
import httpx
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class WBAdvertisingReportService:
    """
    Service for interacting with WB Advertising API V3.
    
    Base URL: https://advert-api.wildberries.ru
    """
    
    BASE_URL = "https://advert-api.wildberries.ru"
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json",
        }
        
    async def get_campaigns(self) -> List[Dict[str, Any]]:
        """
        Get list of campaigns from Count endpoint.
        
        The Count endpoint returns all campaign IDs in advert_list,
        so we extract them directly without needing separate adverts call.
        """
        count_url = f"{self.BASE_URL}/adv/v1/promotion/count"
        
        async with httpx.AsyncClient() as client:
            try:
                count_resp = await client.get(count_url, headers=self.headers)
                if count_resp.status_code != 200:
                    logger.error(f"WB API Count Error: {count_resp.text}")
                    count_resp.raise_for_status()
                
                count_data = count_resp.json()
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
        Rate Limit: 1 request per minute (handled by caller).
        
        Response includes advertId at root level and full funnel metrics:
        - views, clicks, atbs (carts), orders, shks (items), sum (spend), sum_price (revenue)
        """
        url = f"{self.BASE_URL}/adv/v3/fullstats"
        
        # Convert campaign IDs to comma-separated string
        ids_str = ",".join(str(cid) for cid in campaign_ids)
        
        params = {
            "ids": ids_str,
            "beginDate": begin_date,
            "endDate": end_date
        }
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url, headers=self.headers, params=params)
            
            if response.status_code == 429:
                logger.warning("Rate limit hit on /adv/v3/fullstats")
                raise httpx.HTTPStatusError("Rate Limit", request=response.request, response=response)
            
            if response.status_code == 400:
                error_text = response.text
                logger.warning(f"WB API v3/fullstats 400: {error_text}")
                # "no companies with correct intervals" means campaigns don't have data for this period
                if "no companies" in error_text.lower() or "Invalid" in error_text:
                    return []
                    
            if response.status_code != 200:
                logger.error(f"WB API v3/fullstats Error: {response.text}")
                response.raise_for_status()
                
            return response.json()

    async def get_balance(self) -> Dict[str, Any]:
        """
        Get current advertising balance.
        
        Method: GET /adv/v1/balance
        """
        url = f"{self.BASE_URL}/adv/v1/balance"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
