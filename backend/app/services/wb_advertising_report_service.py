
import asyncio
import logging
import httpx
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class WBAdvertisingReportService:
    """
    Service for interacting with WB Advertising API.
    
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
        so we extract them directly without needing POST /adverts.
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

    async def get_full_stats(self, campaign_ids: List[int], date_from: str, date_to: str) -> List[Dict[str, Any]]:
        """
        Get full statistics for campaigns.
        
        Method: POST /adv/v2/fullstats
        Limit: Max 50 campaigns per request.
        Rate Limit: 1 request per minute (handled by caller).
        """
        url = f"{self.BASE_URL}/adv/v2/fullstats"
        
        payload = [
            {
                "id": cid,
                "interval": {
                    "begin": date_from,
                    "end": date_to
                }
            }
            for cid in campaign_ids
        ]
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(url, headers=self.headers, json=payload)
            
            if response.status_code == 429:
                logger.warning("Rate limit hit on /adv/v2/fullstats")
                raise httpx.HTTPStatusError("Rate Limit", request=response.request, response=response)
            
            if response.status_code != 200:
                logger.error(f"WB API fullstats Error: {response.text}")
                
            response.raise_for_status()
            return response.json()
