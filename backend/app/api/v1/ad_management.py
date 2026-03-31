"""
Ad Management API — WRITE operations for Wildberries advertising campaigns.

Endpoints:
- GET  /ad-management/wb/campaigns       — List campaigns with current bids
- POST /ad-management/wb/campaigns/start  — Start a campaign
- POST /ad-management/wb/campaigns/pause  — Pause a campaign
- POST /ad-management/wb/campaigns/stop   — Stop a campaign (IRREVERSIBLE)
- POST /ad-management/wb/bids/change      — Change bids per nm_id
- POST /ad-management/wb/bids/batch       — Batch bid change
- POST /ad-management/wb/status/batch     — Batch start/pause
- GET  /ad-management/wb/balance          — Advertising balance
- GET  /ad-management/wb/audit-log        — Audit log of user actions

All endpoints require authentication and shop ownership verification.
All WRITE operations are logged to ad_audit_log (PostgreSQL).
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.encryption import decrypt_api_key
from app.core.security import get_current_user
from app.models.ad_audit_log import AdAuditLog
from app.models.shop import Shop
from app.models.user import User
from app.schemas.ad_management import (
    AuditLogEntry,
    AuditLogResponse,
    BalanceResponse,
    BatchBidChangeRequest,
    BatchStatusRequest,
    BatchStatusResponse,
    BatchStatusResult,
    CampaignsListResponse,
    CampaignWithBidsResponse,
    ChangeBidsRequest,
    ChangeBidsResponse,
    NmSettingResponse,
    StatusChangeRequest,
    StatusChangeResponse,
)
from app.services.wb_ad_management_service import WBAdManagementService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/ad-management/wb",
    tags=["Ad Management (WB)"],
)


# ══════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════


async def _verify_wb_shop(
    shop_id: int,
    user: User,
    db: AsyncSession,
) -> Shop:
    """Verify shop exists, belongs to user, and is Wildberries."""
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == user.id,
        )
    )
    shop = result.scalar_one_or_none()

    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )

    if shop.marketplace != "wildberries":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Этот раздел доступен только для Wildberries",
        )

    return shop


async def _get_api_key(shop: Shop) -> str:
    """Decrypt shop API key."""
    try:
        return decrypt_api_key(shop.api_key_encrypted)
    except Exception as e:
        logger.error(f"Failed to decrypt API key for shop {shop.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка расшифровки API-ключа",
        )


async def _log_audit(
    db: AsyncSession,
    user: User,
    shop_id: int,
    action: str,
    advert_id: Optional[int],
    details: Optional[dict],
    success: bool,
    error_message: Optional[str] = None,
):
    """Write an audit log entry to PostgreSQL."""
    entry = AdAuditLog(
        user_id=user.id,
        shop_id=shop_id,
        action=action,
        advert_id=advert_id,
        details=details,
        success="true" if success else "false",
        error_message=error_message,
    )
    db.add(entry)
    await db.commit()


# ══════════════════════════════════════════════════════════════════
# Campaigns List
# ══════════════════════════════════════════════════════════════════


@router.get("/campaigns", response_model=CampaignsListResponse)
async def get_campaigns(
    shop_id: int = Query(..., description="Shop ID"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all advertising campaigns with current bids.

    Returns enriched campaign list from WB API (count + v2/adverts).
    """
    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)

    # Fetch campaigns and balance in parallel
    campaigns_raw, balance_raw = await asyncio.gather(
        service.get_campaigns_with_bids(),
        service.get_balance(),
    )

    # Convert to response models
    campaigns = [
        CampaignWithBidsResponse(
            advert_id=c["advert_id"],
            name=c["name"],
            type=c.get("type", 9),
            status=c["status"],
            status_label=c["status_label"],
            payment_type=c.get("payment_type", ""),
            bid_type=c.get("bid_type", ""),
            search_enabled=c.get("search_enabled", False),
            recommendations_enabled=c.get("recommendations_enabled", False),
            change_time=c.get("change_time"),
            nm_settings=[
                NmSettingResponse(**ns) for ns in c.get("nm_settings", [])
            ],
        )
        for c in campaigns_raw
    ]

    balance_data = None
    if balance_raw.get("success"):
        balance_data = balance_raw.get("data")

    return CampaignsListResponse(
        campaigns=campaigns,
        total=len(campaigns),
        balance=balance_data,
    )


# ══════════════════════════════════════════════════════════════════
# Campaign Status Control
# ══════════════════════════════════════════════════════════════════


@router.post("/campaigns/start", response_model=StatusChangeResponse)
async def start_campaign(
    request: StatusChangeRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Start (resume) a paused campaign."""
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.start_campaign(request.advert_id)

    await _log_audit(
        db, current_user, shop.id,
        action="campaign_start",
        advert_id=request.advert_id,
        details={"advert_id": request.advert_id},
        success=result["success"],
        error_message=result.get("message") if not result["success"] else None,
    )

    return StatusChangeResponse(**result)


@router.post("/campaigns/pause", response_model=StatusChangeResponse)
async def pause_campaign(
    request: StatusChangeRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Pause an active campaign."""
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.pause_campaign(request.advert_id)

    await _log_audit(
        db, current_user, shop.id,
        action="campaign_pause",
        advert_id=request.advert_id,
        details={"advert_id": request.advert_id},
        success=result["success"],
        error_message=result.get("message") if not result["success"] else None,
    )

    return StatusChangeResponse(**result)


@router.post("/campaigns/stop", response_model=StatusChangeResponse)
async def stop_campaign(
    request: StatusChangeRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Stop a campaign PERMANENTLY.

    ⚠️ This action is IRREVERSIBLE. The campaign cannot be restarted.
    """
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.stop_campaign(request.advert_id)

    await _log_audit(
        db, current_user, shop.id,
        action="campaign_stop",
        advert_id=request.advert_id,
        details={"advert_id": request.advert_id, "irreversible": True},
        success=result["success"],
        error_message=result.get("message") if not result["success"] else None,
    )

    return StatusChangeResponse(**result)


# ══════════════════════════════════════════════════════════════════
# Bid Management
# ══════════════════════════════════════════════════════════════════


@router.post("/bids/change", response_model=ChangeBidsResponse)
async def change_bids(
    request: ChangeBidsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Change bids for nm_ids in a campaign.

    Bids are in KOPECKS (e.g. 15000 = 150₽).
    Placement: 'search' or 'recommendations'.
    """
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    bids_dicts = [{"nm_id": b.nm_id, "bid": b.bid} for b in request.bids]

    result = await service.change_bids(
        advert_id=request.advert_id,
        placement=request.placement,
        bids=bids_dicts,
    )

    await _log_audit(
        db, current_user, shop.id,
        action="bid_change",
        advert_id=request.advert_id,
        details={
            "placement": request.placement,
            "bids": [
                {"nm_id": b.nm_id, "bid_kopecks": b.bid, "bid_rub": b.bid / 100}
                for b in request.bids
            ],
        },
        success=result["success"],
        error_message=result.get("message") if not result["success"] else None,
    )

    return ChangeBidsResponse(**result)


# ══════════════════════════════════════════════════════════════════
# Batch Operations
# ══════════════════════════════════════════════════════════════════


@router.post("/status/batch", response_model=BatchStatusResponse)
async def batch_status_change(
    request: BatchStatusRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Batch start/pause for multiple campaigns.

    Max 50 campaigns per request.
    Processes sequentially with 2-second delay between calls.
    """
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)

    results = []
    success_count = 0
    failed_count = 0

    for i, advert_id in enumerate(request.advert_ids):
        if request.action == "start":
            result = await service.start_campaign(advert_id)
        else:
            result = await service.pause_campaign(advert_id)

        results.append(BatchStatusResult(
            advert_id=advert_id,
            success=result["success"],
            message=result["message"],
        ))

        if result["success"]:
            success_count += 1
        else:
            failed_count += 1

        # Rate limit: WB allows ~1 status change per minute
        if i < len(request.advert_ids) - 1:
            await asyncio.sleep(2)

    # Log batch operation
    await _log_audit(
        db, current_user, shop.id,
        action="batch_status",
        advert_id=None,
        details={
            "action": request.action,
            "advert_ids": request.advert_ids,
            "success_count": success_count,
            "failed_count": failed_count,
        },
        success=failed_count == 0,
    )

    return BatchStatusResponse(
        results=results,
        total=len(results),
        success_count=success_count,
        failed_count=failed_count,
    )


# ══════════════════════════════════════════════════════════════════
# Balance
# ══════════════════════════════════════════════════════════════════


@router.get("/balance", response_model=BalanceResponse)
async def get_balance(
    shop_id: int = Query(..., description="Shop ID"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get current advertising balance."""
    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.get_balance()

    if result.get("success"):
        data = result.get("data", {})
        return BalanceResponse(
            success=True,
            balance=data.get("balance"),
            bonus=data.get("bonus"),
        )

    return BalanceResponse(
        success=False,
        message=result.get("message", "Ошибка получения баланса"),
    )


# ══════════════════════════════════════════════════════════════════
# Audit Log
# ══════════════════════════════════════════════════════════════════


@router.get("/audit-log", response_model=AuditLogResponse)
async def get_audit_log(
    shop_id: int = Query(..., description="Shop ID"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get audit log of advertising management actions."""
    shop = await _verify_wb_shop(shop_id, current_user, db)

    # Count total
    count_result = await db.execute(
        select(func.count(AdAuditLog.id)).where(
            AdAuditLog.shop_id == shop.id,
        )
    )
    total = count_result.scalar() or 0

    # Fetch entries
    result = await db.execute(
        select(AdAuditLog, User.full_name)
        .outerjoin(User, AdAuditLog.user_id == User.id)
        .where(AdAuditLog.shop_id == shop.id)
        .order_by(desc(AdAuditLog.created_at))
        .offset(offset)
        .limit(limit)
    )
    rows = result.all()

    entries = [
        AuditLogEntry(
            id=row[0].id,
            action=row[0].action,
            advert_id=row[0].advert_id,
            details=row[0].details,
            created_at=row[0].created_at.isoformat(),
            user_name=row[1],
        )
        for row in rows
    ]

    return AuditLogResponse(entries=entries, total=total)


# ══════════════════════════════════════════════════════════════════
# Enriched Campaigns (management data + ClickHouse stats)
# ══════════════════════════════════════════════════════════════════


@router.get("/campaigns/enriched")
async def get_enriched_campaigns(
    shop_id: int = Query(...),
    period: str = Query("30d", description="Period: today, 7d, 14d, 30d, 90d"),
    date_from: Optional[str] = Query(None, description="Custom start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Custom end date YYYY-MM-DD"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get campaigns with management data + ClickHouse statistics for the period.
    Merges WB API (status, bids) with fact_advert_stats_v3 (spend, views, etc.)
    """
    from datetime import date as date_type, timedelta
    from app.core.clickhouse import get_clickhouse_client

    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    # Parse period
    PERIOD_DAYS = {"today": 1, "7d": 7, "14d": 14, "30d": 30, "90d": 90}
    today = date_type.today()

    if date_from and date_to:
        try:
            d_start = date_type.fromisoformat(date_from)
            d_end = date_type.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format")
        days = (d_end - d_start).days + 1
    else:
        days = PERIOD_DAYS.get(period, 30)
        if period == "today":
            d_start = today
            d_end = today
        else:
            d_end = today
            d_start = today - timedelta(days=days - 1)

    # Prev period for deltas
    prev_end = d_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)

    # 1. Get campaigns from WB API + balance
    campaigns_raw, balance_raw = await asyncio.gather(
        service.get_campaigns_with_bids(),
        service.get_balance(),
    )

    # 2. Get stats from ClickHouse
    ch = get_clickhouse_client()

    # Current period stats per campaign
    stats_rows = ch.query("""
        SELECT
            advert_id AS cid,
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(atbs) AS t_cart,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {start:Date}
          AND date <= {end:Date}
        GROUP BY cid
    """, parameters={
        "shop_id": shop.id,
        "start": d_start,
        "end": d_end,
    }).result_rows

    stats_map: dict = {}
    for row in stats_rows:
        cid = int(row[0])
        spend = float(row[1])
        views = int(row[2])
        clicks = int(row[3])
        cart = int(row[4])
        orders = int(row[5])
        revenue = float(row[6])
        stats_map[cid] = {
            "spend": round(spend, 2),
            "views": views,
            "clicks": clicks,
            "cart": cart,
            "orders": orders,
            "revenue": round(revenue, 2),
            "ctr": round(clicks / views * 100, 2) if views > 0 else 0,
            "drr": round(spend / revenue * 100, 1) if revenue > 0 else 0,
            "cpc": round(spend / clicks, 2) if clicks > 0 else 0,
            "cpm": round(spend / views * 1000, 2) if views > 0 else 0,
            "cpa_cart": round(spend / cart, 2) if cart > 0 else 0,
            "cpo": round(spend / orders, 2) if orders > 0 else 0,
        }

    # Prev period stats (aggregated for KPI deltas)
    prev_rows = ch.query("""
        SELECT
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(atbs) AS t_cart,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {start:Date}
          AND date <= {end:Date}
    """, parameters={
        "shop_id": shop.id,
        "start": prev_start,
        "end": prev_end,
    }).result_rows

    prev = {}
    if prev_rows and prev_rows[0]:
        r = prev_rows[0]
        prev = {
            "spend": float(r[0]),
            "views": int(r[1]),
            "clicks": int(r[2]),
            "cart": int(r[3]),
            "orders": int(r[4]),
            "revenue": float(r[5]),
        }

    # Aggregate KPI for current period
    kpi = {
        "spend": sum(s["spend"] for s in stats_map.values()),
        "views": sum(s["views"] for s in stats_map.values()),
        "clicks": sum(s["clicks"] for s in stats_map.values()),
        "cart": sum(s["cart"] for s in stats_map.values()),
        "orders": sum(s["orders"] for s in stats_map.values()),
        "revenue": sum(s["revenue"] for s in stats_map.values()),
    }
    kpi["ctr"] = round(kpi["clicks"] / kpi["views"] * 100, 2) if kpi["views"] > 0 else 0
    kpi["drr"] = round(kpi["spend"] / kpi["revenue"] * 100, 1) if kpi["revenue"] > 0 else 0

    # Deltas
    def _delta(cur, prv):
        if prv == 0:
            return 100.0 if cur > 0 else 0.0
        return round((cur - prv) / prv * 100, 1)

    kpi_deltas = {
        "spend": _delta(kpi["spend"], prev.get("spend", 0)),
        "views": _delta(kpi["views"], prev.get("views", 0)),
        "clicks": _delta(kpi["clicks"], prev.get("clicks", 0)),
        "cart": _delta(kpi["cart"], prev.get("cart", 0)),
        "orders": _delta(kpi["orders"], prev.get("orders", 0)),
        "revenue": _delta(kpi["revenue"], prev.get("revenue", 0)),
    }
    prev_ctr = round(prev["clicks"] / prev["views"] * 100, 2) if prev.get("views", 0) > 0 else 0
    kpi_deltas["ctr"] = _delta(kpi["ctr"], prev_ctr)
    prev_drr = round(prev["spend"] / prev["revenue"] * 100, 1) if prev.get("revenue", 0) > 0 else 0
    kpi_deltas["drr"] = _delta(kpi["drr"], prev_drr)

    # 3. Merge management + stats
    campaigns = []
    for c in campaigns_raw:
        cid = c["advert_id"]
        st = stats_map.get(cid, {})
        campaigns.append({
            **c,
            "spend": st.get("spend", 0),
            "views": st.get("views", 0),
            "clicks": st.get("clicks", 0),
            "cart": st.get("cart", 0),
            "orders": st.get("orders", 0),
            "revenue": st.get("revenue", 0),
            "ctr": st.get("ctr", 0),
            "drr": st.get("drr", 0),
            "cpc": st.get("cpc", 0),
            "cpm": st.get("cpm", 0),
            "cpa_cart": st.get("cpa_cart", 0),
            "cpo": st.get("cpo", 0),
        })

    balance_data = None
    if balance_raw.get("success"):
        balance_data = balance_raw.get("data")

    return {
        "campaigns": campaigns,
        "total": len(campaigns),
        "balance": balance_data,
        "kpi": kpi,
        "kpi_deltas": kpi_deltas,
        "period": {"start": str(d_start), "end": str(d_end)},
    }


# ══════════════════════════════════════════════════════════════════
# Campaign Budget
# ══════════════════════════════════════════════════════════════════


@router.get("/budget")
async def get_campaign_budget(
    shop_id: int = Query(...),
    advert_id: int = Query(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get budget for a specific campaign."""
    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.get_campaign_budget(advert_id)

    if not result.get("success"):
        raise HTTPException(status_code=502, detail=result.get("message", "Ошибка"))

    return result["data"]


@router.post("/budgets/batch")
async def get_campaigns_budgets_batch(
    request: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Batch-fetch budgets for multiple campaigns with Redis caching (60s TTL).
    
    Request body: {"shop_id": int, "advert_ids": [int, ...]}
    Response: {"budgets": {advert_id: {total, daily}, ...}}
    
    Gracefully degrades: if Redis is down, fetches all from WB API.
    """
    from app.config import get_settings
    
    shop_id = request.get("shop_id")
    advert_ids = request.get("advert_ids", [])
    
    if not shop_id or not advert_ids:
        raise HTTPException(status_code=400, detail="shop_id and advert_ids required")
    
    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)
    
    # Try Redis cache first, but degrade gracefully if Redis is unavailable
    budgets = {}
    uncached_ids = list(advert_ids)
    redis_client = None
    
    settings = get_settings()
    try:
        import redis.asyncio as aioredis
        redis_client = await aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
            retry_on_timeout=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        await redis_client.ping()
        
        # Check cache
        uncached_ids = []
        for aid in advert_ids:
            cache_key = f"budget:{shop_id}:{aid}"
            cached = await redis_client.get(cache_key)
            if cached:
                budgets[aid] = json.loads(cached)
            else:
                uncached_ids.append(aid)
    except Exception as e:
        logger.warning(f"[budgets/batch] Redis unavailable, fetching all from API: {e}")
        uncached_ids = list(advert_ids)
    
    # Fetch uncached from WB API
    if uncached_ids:
        service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
        try:
            fresh = await service.get_campaigns_budgets(uncached_ids)
            
            for aid, data in fresh.items():
                budgets[aid] = data
                # Cache in Redis if available
                if redis_client:
                    try:
                        cache_key = f"budget:{shop_id}:{aid}"
                        await redis_client.set(cache_key, json.dumps(data), ex=60)
                    except Exception:
                        pass  # Don't fail on cache write errors
        except Exception as e:
            logger.error(f"[budgets/batch] WB API error: {e}")
    
    # Cleanup
    if redis_client:
        try:
            await redis_client.aclose()
        except Exception:
            pass
    
    return {"budgets": budgets}


from pydantic import BaseModel, Field as PField


class DepositBudgetRequest(BaseModel):
    shop_id: int
    advert_id: int
    amount: int = PField(..., gt=0, description="Amount in rubles")
    budget_type: str = PField("sum", description="'sum' for total, 'dly' for daily limit")


@router.post("/budget/deposit")
async def deposit_budget(
    request: DepositBudgetRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Deposit (top-up) budget for a campaign."""
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.deposit_budget(
        request.advert_id, request.amount, request.budget_type,
    )

    await _log_audit(
        db, current_user, shop.id,
        action="budget_deposit",
        advert_id=request.advert_id,
        details={
            "advert_id": request.advert_id,
            "amount": request.amount,
            "budget_type": request.budget_type,
        },
        success=result["success"],
        error_message=result.get("message") if not result["success"] else None,
    )

    if not result["success"]:
        raise HTTPException(
            status_code=result.get("status_code", 502),
            detail=result["message"],
        )

    return result
