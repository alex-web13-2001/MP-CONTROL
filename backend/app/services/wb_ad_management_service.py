"""
WB Ad Management Service — WRITE operations for Wildberries Advertising API.

Supports:
- Campaign status control (start/pause/stop)
- Bid management (change bids per nm_id, get min bids)
- Balance checking

All calls go through MarketplaceClient (proxy, rate limiting, circuit breaker).

IMPORTANT: Bids in WB API are in KOPECKS (×100 from rubles).
"""

import logging
from typing import List, Dict, Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.marketplace_client import MarketplaceClient

logger = logging.getLogger(__name__)


# ── Campaign Statuses ─────────────────────────────────────────────
# WB campaign status codes:
#  -1  = В процессе удаления
#   4  = Готова к запуску
#   7  = Завершена
#   8  = Отказана
#   9  = Активна (идут показы)
#  11  = На паузе

WB_STATUS_LABELS = {
    -1: "Удаляется",
    4: "Готова к запуску",
    7: "Завершена",
    8: "Отказана",
    9: "Активна",
    11: "На паузе",
}


class WBAdManagementService:
    """
    Service for managing WB advertising campaigns.

    WRITE operations:
    - start/pause/stop campaigns
    - change bids (per nm_id)
    - get minimum bids

    All methods use MarketplaceClient for:
    - Proxy rotation (sticky sessions)
    - Redis rate limiting
    - Circuit breaker
    - JA3 fingerprint spoofing

    Base URL: https://advert-api.wildberries.ru (marketplace='wildberries_adv')
    """

    def __init__(self, db: AsyncSession, shop_id: int, api_key: str):
        self.db = db
        self.shop_id = shop_id
        self.api_key = api_key

    # ══════════════════════════════════════════════════════════════
    # Campaign Status Control
    # ══════════════════════════════════════════════════════════════

    async def start_campaign(self, advert_id: int) -> Dict[str, Any]:
        """
        Start (resume) a paused/ready campaign.

        WB API: GET /adv/v0/start?id={advertId}
        Rate limit: ~1 req/min per campaign.

        Returns:
            {"success": True/False, "message": "...", "status_code": int}
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/adv/v0/start",
                params={"id": advert_id},
            )

            if response.is_success:
                logger.info(
                    f"[ad-mgmt] Campaign {advert_id} STARTED (shop={self.shop_id})"
                )
                return {
                    "success": True,
                    "message": f"Кампания {advert_id} запущена",
                    "status_code": response.status_code,
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[ad-mgmt] Failed to start campaign {advert_id}: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    async def pause_campaign(self, advert_id: int) -> Dict[str, Any]:
        """
        Pause an active campaign.

        WB API: GET /adv/v0/pause?id={advertId}
        Rate limit: ~1 req/min per campaign.
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/adv/v0/pause",
                params={"id": advert_id},
            )

            if response.is_success:
                logger.info(
                    f"[ad-mgmt] Campaign {advert_id} PAUSED (shop={self.shop_id})"
                )
                return {
                    "success": True,
                    "message": f"Кампания {advert_id} поставлена на паузу",
                    "status_code": response.status_code,
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[ad-mgmt] Failed to pause campaign {advert_id}: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    async def stop_campaign(self, advert_id: int) -> Dict[str, Any]:
        """
        STOP a campaign (IRREVERSIBLE!).

        WB API: GET /adv/v0/stop?id={advertId}

        WARNING: Cannot be undone! Campaign will be permanently stopped.
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/adv/v0/stop",
                params={"id": advert_id},
            )

            if response.is_success:
                logger.info(
                    f"[ad-mgmt] Campaign {advert_id} STOPPED (shop={self.shop_id}) ⚠️ IRREVERSIBLE"
                )
                return {
                    "success": True,
                    "message": f"Кампания {advert_id} завершена (необратимо)",
                    "status_code": response.status_code,
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[ad-mgmt] Failed to stop campaign {advert_id}: "
                f"status={response.status_code}, error={error_msg}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    # ══════════════════════════════════════════════════════════════
    # Bid Management
    # ══════════════════════════════════════════════════════════════

    async def change_bids(
        self,
        advert_id: int,
        placement: str,
        bids: List[Dict[str, int]],
    ) -> Dict[str, Any]:
        """
        Change bids for nm_ids in a campaign.

        WB API: PATCH /api/advert/v1/bids
        Rate limit: 1 request / 30 seconds per campaign.

        Args:
            advert_id: Campaign ID
            placement: 'search' or 'recommendations'
            bids: List of {"nm_id": int, "bid": int} (bid in KOPECKS)

        Request body format:
        {
            "advertId": 12345,
            "placement": "search",
            "bids": [
                {"nmId": 67890, "bid": 15000}  // 150 rubles
            ]
        }
        """
        # Convert field names to WB API format
        api_bids = [
            {"nmId": b["nm_id"], "bid": b["bid"]}
            for b in bids
        ]

        payload = {
            "advertId": advert_id,
            "placement": placement,
            "bids": api_bids,
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.request(
                "PATCH",
                "/api/advert/v1/bids",
                json=payload,
            )

            if response.is_success:
                bid_summary = ", ".join(
                    f"nm={b['nm_id']}: {b['bid'] / 100:.0f}₽"
                    for b in bids
                )
                logger.info(
                    f"[ad-mgmt] Bids changed: advert={advert_id} "
                    f"placement={placement} [{bid_summary}] (shop={self.shop_id})"
                )
                return {
                    "success": True,
                    "message": f"Ставки обновлены ({placement})",
                    "status_code": response.status_code,
                    "bids_applied": len(bids),
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[ad-mgmt] Failed to change bids: advert={advert_id} "
                f"status={response.status_code}, error={error_msg}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    async def get_min_bids(
        self,
        payment_type: str = "cpm",
        position: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get minimum allowed bids.

        WB API: POST /api/advert/v1/bids/min
        Returns minimum bids in kopecks by placement.

        Args:
            payment_type: 'cpm' or 'cpc'
            position: Optional position for bid calculation
        """
        payload: Dict[str, Any] = {"paymentType": payment_type}
        if position is not None:
            payload["position"] = position

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/api/advert/v1/bids/min",
                json=payload,
            )

            if response.is_success:
                return {
                    "success": True,
                    "data": response.data,
                }

            error_msg = self._parse_error(response)
            return {
                "success": False,
                "message": error_msg,
            }

    # ══════════════════════════════════════════════════════════════
    # Balance
    # ══════════════════════════════════════════════════════════════

    async def get_balance(self) -> Dict[str, Any]:
        """
        Get current advertising balance.

        WB API: GET /adv/v1/balance
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get("/adv/v1/balance")

            if response.is_success:
                return {
                    "success": True,
                    "data": response.data,
                }

            return {
                "success": False,
                "message": self._parse_error(response),
            }

    # ══════════════════════════════════════════════════════════════
    # Campaigns List (enriched with current bids)
    # ══════════════════════════════════════════════════════════════

    async def get_campaigns_with_bids(self) -> List[Dict[str, Any]]:
        """
        Fetch all campaigns with current bids (V2 API).

        Combines:
        1. GET /adv/v1/promotion/count → campaign IDs + statuses
        2. GET /api/advert/v2/adverts → detailed bids per nm_id

        Returns enriched campaign list ready for frontend.
        """
        # Step 1: Get campaign list
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            count_response = await client.get("/adv/v1/promotion/count")

            if not count_response.is_success:
                logger.error(f"Failed to get campaigns: {count_response.error}")
                return []

            count_data = count_response.data
            campaign_ids = []

            if "adverts" in count_data:
                for group in count_data["adverts"]:
                    adv_type = group.get("type")
                    group_status = group.get("status")
                    for advert in group.get("advert_list", []):
                        aid = advert.get("advertId")
                        if aid:
                            campaign_ids.append({
                                "id": aid,
                                "type": adv_type,
                                "status": group_status,
                                "changeTime": advert.get("changeTime"),
                            })

            if not campaign_ids:
                return []

            # Step 2: Get V2 details in batches of 50
            all_v2 = []
            ids_list = [c["id"] for c in campaign_ids]

            for i in range(0, len(ids_list), 50):
                batch = ids_list[i:i+50]
                v2_response = await client.get(
                    "/api/advert/v2/adverts",
                    params={"id": ",".join(str(x) for x in batch)},
                )

                if v2_response.is_success:
                    data = v2_response.data
                    if isinstance(data, dict) and "adverts" in data:
                        all_v2.extend(data["adverts"])
                    elif isinstance(data, list):
                        all_v2.extend(data)

            # Step 3: Merge data
            v2_map = {int(a.get("id") or 0): a for a in all_v2}
            result = []

            for campaign_info in campaign_ids:
                cid = campaign_info["id"]
                v2 = v2_map.get(cid, {})

                settings = v2.get("settings") or {}
                name = settings.get("name", "") or v2.get("name", "")
                payment_type = settings.get("payment_type", "")
                bid_type = v2.get("bid_type", "")
                placements = settings.get("placements") or {}

                # Extract nm_settings with bids
                nm_settings = []
                for ns in v2.get("nm_settings") or []:
                    if not isinstance(ns, dict):
                        continue
                    nm_id = ns.get("nm_id")
                    bids = ns.get("bids_kopecks") or {}
                    subject = ns.get("subject") or {}
                    nm_settings.append({
                        "nm_id": nm_id,
                        "bid_search": bids.get("search", 0),
                        "bid_recommendations": bids.get("recommendations", 0),
                        "subject_name": subject.get("name", ""),
                    })

                result.append({
                    "advert_id": cid,
                    "name": name,
                    "type": campaign_info.get("type", 9),
                    "status": campaign_info.get("status", 0),
                    "status_label": WB_STATUS_LABELS.get(campaign_info.get("status", 0), "Неизвестно"),
                    "payment_type": payment_type,
                    "bid_type": bid_type,
                    "search_enabled": bool(placements.get("search")),
                    "recommendations_enabled": bool(placements.get("recommendations")),
                    "change_time": campaign_info.get("changeTime"),
                    "nm_settings": nm_settings,
                })

            logger.info(
                f"[ad-mgmt] Loaded {len(result)} campaigns with bids "
                f"(shop={self.shop_id})"
            )
            return result

    async def get_campaign_budget(self, advert_id: int) -> Dict[str, Any]:
        """
        Get budget info for a specific campaign.

        WB API: GET /adv/v1/budget?id={advertId}
        Returns: {"total": int, "daily": int, "booster": bool, ...}
        """
        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.get(
                "/adv/v1/budget",
                params={"id": advert_id},
            )

            if response.is_success:
                return {
                    "success": True,
                    "data": response.data,
                }

            return {
                "success": False,
                "message": self._parse_error(response),
            }

    async def deposit_budget(
        self, advert_id: int, amount: int, budget_type: str = "sum"
    ) -> Dict[str, Any]:
        """
        Deposit (top-up) budget for a campaign.

        WB API: POST /adv/v1/budget/deposit
        Args:
            advert_id: Campaign ID
            amount: Amount in RUBLES (integer)
            budget_type: 'sum' for total budget, 'dly' for daily limit
        """
        payload = {
            "advertId": advert_id,
            "type": budget_type,
            "sum": amount,
        }

        async with MarketplaceClient(
            db=self.db,
            shop_id=self.shop_id,
            marketplace="wildberries_adv",
            api_key=self.api_key,
        ) as client:
            response = await client.post(
                "/adv/v1/budget/deposit",
                json=payload,
            )

            if response.is_success:
                logger.info(
                    f"[ad-mgmt] Budget deposited: advert={advert_id} "
                    f"amount={amount}₽ type={budget_type} (shop={self.shop_id})"
                )
                return {
                    "success": True,
                    "message": f"Бюджет пополнен на {amount} ₽",
                    "status_code": response.status_code,
                }

            error_msg = self._parse_error(response)
            logger.warning(
                f"[ad-mgmt] Failed to deposit budget: advert={advert_id} "
                f"status={response.status_code}, error={error_msg}"
            )
            return {
                "success": False,
                "message": error_msg,
                "status_code": response.status_code,
            }

    async def get_campaigns_budgets(
        self, advert_ids: List[int]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Fetch budgets for multiple campaigns in parallel (batched).
        Returns dict of advert_id -> budget data.
        Rate-limited: max 5 concurrent.
        """
        import asyncio

        budgets: Dict[int, Dict[str, Any]] = {}
        semaphore = asyncio.Semaphore(5)

        async def fetch_one(aid: int):
            async with semaphore:
                result = await self.get_campaign_budget(aid)
                if result.get("success"):
                    budgets[aid] = result["data"]

        await asyncio.gather(
            *[fetch_one(aid) for aid in advert_ids],
            return_exceptions=True,
        )
        return budgets

    # ══════════════════════════════════════════════════════════════
    # Helpers
    # ══════════════════════════════════════════════════════════════

    def _parse_error(self, response) -> str:
        """Extract user-friendly error message from WB API response."""
        if response.error:
            return response.error

        if isinstance(response.data, dict):
            return response.data.get("message", "") or response.data.get("error", "")
        if isinstance(response.data, str):
            return response.data

        # Status-specific messages
        status_errors = {
            422: "Некорректные параметры запроса",
            429: "Слишком частые запросы. Подождите 30 секунд",
            403: "Нет доступа. Проверьте API-ключ",
            400: "Ошибка в данных запроса",
        }
        return status_errors.get(
            response.status_code,
            f"Ошибка WB API (код {response.status_code})",
        )
