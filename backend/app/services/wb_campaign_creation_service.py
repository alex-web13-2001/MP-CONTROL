"""
WB Campaign Creation Service — creates new advertising campaigns via WB API.

Endpoints used:
- GET  /adv/v1/supplier/subjects  — available subjects (categories)
- POST /adv/v2/supplier/nms       — product cards by subject IDs
- POST /adv/v2/seacat/save-ad     — create campaign

All calls go through MarketplaceClient (proxy, rate limiting, circuit breaker).
"""

import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)


class WBCampaignCreationService:
    """
    Service for creating new WB advertising campaigns.

    Flow:
    1. get_subjects() → user picks category
    2. get_products() → user picks products (nm_ids)
    3. create_campaign() → creates campaign (status=4 "Готова к запуску")
    4. After creation: deposit budget + start via existing services

    Base URL: https://advert-api.wildberries.ru (marketplace='wildberries_adv')
    """

    def __init__(self, db: AsyncSession, shop_id: int, api_key: str):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key

    async def get_subjects(self, payment_type: str = "cpm") -> Dict[str, Any]:
        """
        Get available subjects (categories) for campaign creation.

        WB API: GET /adv/v1/supplier/subjects?payment_type={cpm|cpc}

        Returns:
            {"success": True, "subjects": [{"name": "...", "id": int, "count": int}]}
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/adv/v1/supplier/subjects",
                params={"payment_type": payment_type},
            )

            if response.is_success:
                data = response.data
                subjects = data if isinstance(data, list) else []
                logger.info(
                    f"[campaign-create] Got {len(subjects)} subjects "
                    f"(payment_type={payment_type}, shop={self.shop_id})"
                )
                return {"success": True, "subjects": subjects}

            error_msg = self._parse_error(response)
            logger.warning(
                f"[campaign-create] Failed to get subjects: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {"success": False, "message": error_msg, "subjects": []}

    async def get_products(self, subject_ids: List[int]) -> Dict[str, Any]:
        """
        Get product cards available for advertising by subject IDs.

        WB API: POST /adv/v2/supplier/nms
        Body: [subjectId1, subjectId2, ...]

        Returns:
            {"success": True, "products": [{"title": "...", "nm": int, "subjectId": int}]}
        """
        if not subject_ids:
            return {"success": True, "products": []}

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v2/supplier/nms",
                json=subject_ids,
            )

            if response.is_success:
                data = response.data
                products = data if isinstance(data, list) else []
                logger.info(
                    f"[campaign-create] Got {len(products)} products "
                    f"for subjects {subject_ids} (shop={self.shop_id})"
                )
                return {"success": True, "products": products}

            error_msg = self._parse_error(response)
            logger.warning(
                f"[campaign-create] Failed to get products: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {"success": False, "message": error_msg, "products": []}

    async def create_campaign(
        self,
        name: str,
        nms: List[int],
        bid_type: str = "unified",
        payment_type: Optional[str] = None,
        placement_types: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new advertising campaign.

        WB API: POST /adv/v2/seacat/save-ad

        Args:
            name: Campaign name
            nms: List of nm_ids (product IDs), max 50
            bid_type: "unified" or "manual"
            payment_type: "cpm" or "cpc" (optional, may not be required by API)
            placement_types: ["search", "recommendations"] — only for manual bid_type

        Returns:
            {"success": True, "advert_id": int} on success
        """
        if not nms:
            return {"success": False, "message": "Не выбраны товары"}

        if len(nms) > 50:
            return {"success": False, "message": "Максимум 50 товаров"}

        # Build request payload
        payload: Dict[str, Any] = {
            "name": name,
            "nms": nms,
            "bid_type": bid_type,
        }

        # Add payment_type if specified
        if payment_type:
            payload["payment_type"] = payment_type

        # Add placement_types for manual bid campaigns
        if bid_type == "manual" and placement_types:
            payload["placement_types"] = placement_types

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v2/seacat/save-ad",
                json=payload,
            )

            if response.is_success:
                advert_id = response.data
                logger.info(
                    f"[campaign-create] Campaign created: id={advert_id} "
                    f"name='{name}' bid_type={bid_type} nms={len(nms)} "
                    f"(shop={self.shop_id})"
                )
                return {
                    "success": True,
                    "advert_id": advert_id,
                    "message": f"Кампания '{name}' создана (ID: {advert_id})",
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[campaign-create] Failed to create campaign: "
                f"status={response.status_code}, error={error_msg}, "
                f"payload={payload}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    def _parse_error(self, response) -> str:
        """Extract user-friendly error message from WB API response."""
        if response.error:
            return response.error

        if isinstance(response.data, dict):
            # WB returns {"errors": [{"detail": "...", "field": "..."}], ...}
            errors = response.data.get("errors", [])
            if errors:
                return "; ".join(e.get("detail", "") for e in errors)
            return response.data.get("message", "") or response.data.get("title", "")
        if isinstance(response.data, str):
            return response.data

        status_errors = {
            422: "Некорректные параметры запроса",
            429: "Слишком частые запросы. Подождите",
            403: "Нет доступа. Проверьте API-ключ",
            400: "Ошибка в данных запроса",
        }
        return status_errors.get(
            response.status_code,
            f"Ошибка WB API (код {response.status_code})",
        )
