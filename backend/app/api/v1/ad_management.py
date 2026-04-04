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
from app.services.wb_campaign_creation_service import WBCampaignCreationService
from pydantic import BaseModel, Field
from typing import List as TypingList

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


async def _update_campaign_status_in_ch(shop_id: int, advert_id: int, new_status: int):
    """Update campaign status in ClickHouse immediately after WB API confirms change."""
    try:
        from app.core.clickhouse import get_clickhouse_client
        from datetime import datetime
        ch = get_clickhouse_client()
        # Insert new row with updated status — ReplacingMergeTree(updated_at) will
        # keep only the latest row per (shop_id, advert_id) on FINAL reads
        ch.command("""
            INSERT INTO mms_analytics.dim_advert_campaigns
                (shop_id, advert_id, name, type, status, updated_at,
                 payment_type, bid_type, search_enabled, recommendations_enabled)
            SELECT
                shop_id, advert_id, 
                argMax(name, updated_at),
                argMax(type, updated_at),
                {new_status:Int8},
                {now:DateTime},
                argMax(payment_type, updated_at),
                argMax(bid_type, updated_at),
                argMax(search_enabled, updated_at),
                argMax(recommendations_enabled, updated_at)
            FROM mms_analytics.dim_advert_campaigns
            WHERE shop_id = {shop_id:UInt32} AND advert_id = {advert_id:UInt64}
            GROUP BY shop_id, advert_id
        """, parameters={
            "shop_id": shop_id,
            "advert_id": advert_id,
            "new_status": new_status,
            "now": datetime.utcnow(),
        })
        logger.info(f"[ad-mgmt] Updated CH status: advert={advert_id} → {new_status}")
    except Exception as e:
        logger.warning(f"[ad-mgmt] Failed to update CH status: {e}")


async def _insert_new_campaign_to_ch(
    shop_id: int,
    advert_id: int,
    name: str,
    status: int = 4,
    payment_type: str = "cpm",
    bid_type: str = "auto",
    search_enabled: int = 1,
    recommendations_enabled: int = 1,
):
    """Insert a brand-new campaign into ClickHouse so it appears immediately in from-db list."""
    try:
        from app.core.clickhouse import get_clickhouse_client
        from datetime import datetime
        ch = get_clickhouse_client()
        ch.command("""
            INSERT INTO mms_analytics.dim_advert_campaigns
                (shop_id, advert_id, name, type, status, updated_at,
                 payment_type, bid_type, search_enabled, recommendations_enabled)
            VALUES (
                {shop_id:UInt32}, {advert_id:UInt64}, {name:String},
                9, {status:Int8}, {now:DateTime},
                {payment_type:String}, {bid_type:String},
                {search_enabled:UInt8}, {reco_enabled:UInt8}
            )
        """, parameters={
            "shop_id": shop_id,
            "advert_id": advert_id,
            "name": name,
            "status": status,
            "now": datetime.utcnow(),
            "payment_type": payment_type,
            "bid_type": bid_type,
            "search_enabled": search_enabled,
            "reco_enabled": recommendations_enabled,
        })
        logger.info(
            f"[ad-mgmt] Inserted new campaign in CH: "
            f"advert={advert_id} name='{name}' status={status}"
        )
    except Exception as e:
        logger.warning(f"[ad-mgmt] Failed to insert new campaign in CH: {e}")


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

    if result["success"]:
        await _update_campaign_status_in_ch(shop.id, request.advert_id, 9)  # 9 = Active

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

    if result["success"]:
        await _update_campaign_status_in_ch(shop.id, request.advert_id, 11)  # 11 = Paused

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
        bid_type=request.bid_type,
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
            # Persist status change to ClickHouse immediately
            new_status = 9 if request.action == "start" else 11
            await _update_campaign_status_in_ch(shop.id, advert_id, new_status)
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
# Campaigns from DB (0 WB API calls — all from ClickHouse + Redis)
# ══════════════════════════════════════════════════════════════════

WB_STATUS_LABELS = {
    -1: "Удалена", 4: "Готова к запуску", 7: "Завершена",
    8: "Отказ", 9: "Активна", 11: "На паузе",
}


@router.get("/campaigns/from-db")
async def get_campaigns_from_db(
    shop_id: int = Query(...),
    period: str = Query("7d", description="Period: today, 7d, 14d, 30d, 90d"),
    date_from: Optional[str] = Query(None, description="Custom start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Custom end date YYYY-MM-DD"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get campaigns entirely from our database — 0 WB API calls.

    Sources:
    - dim_advert_campaigns (ClickHouse) → names, statuses, types, placements
    - log_wb_bids (ClickHouse) → latest bids per nm_id
    - fact_advert_stats_v3 (ClickHouse) → spend, views, clicks, etc.
    - Redis → cached budgets + balance (synced every 15 min by Celery)
    """
    from datetime import date as date_type, timedelta
    from app.core.clickhouse import get_clickhouse_client

    shop = await _verify_wb_shop(shop_id, current_user, db)

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
        days = PERIOD_DAYS.get(period, 7)
        if period == "today":
            d_start = today
            d_end = today
        else:
            d_end = today
            d_start = today - timedelta(days=days - 1)

    prev_end = d_start - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days - 1)

    ch = get_clickhouse_client()

    # 1. Campaigns from dim_advert_campaigns
    campaign_rows = ch.query("""
        SELECT
            advert_id,
            argMax(name, updated_at) AS name,
            argMax(type, updated_at) AS type,
            argMax(status, updated_at) AS status,
            argMax(payment_type, updated_at) AS payment_type,
            argMax(bid_type, updated_at) AS bid_type,
            argMax(search_enabled, updated_at) AS search_enabled,
            argMax(recommendations_enabled, updated_at) AS recommendations_enabled
        FROM mms_analytics.dim_advert_campaigns
        WHERE shop_id = {shop_id:UInt32}
        GROUP BY advert_id
    """, parameters={"shop_id": shop.id}).result_rows

    # 2. Latest bids from log_wb_bids (last snapshot per campaign+nm)
    # Use argMaxIf to ignore zero-bid garbage rows from WB API storms
    bid_rows = ch.query("""
        SELECT
            advert_id,
            nm_id,
            argMaxIf(bid_search, timestamp, bid_search > 0 OR bid_recommendations > 0) AS bs,
            argMaxIf(bid_recommendations, timestamp, bid_search > 0 OR bid_recommendations > 0) AS br
        FROM mms_analytics.log_wb_bids
        WHERE shop_id = {shop_id:UInt32}
        GROUP BY advert_id, nm_id
        HAVING bs > 0 OR br > 0
    """, parameters={"shop_id": shop.id}).result_rows

    # Build bid map: advert_id → [{nm_id, bid_search, bid_recommendations}]
    bid_map: dict = {}
    all_nm_ids: set = set()
    for row in bid_rows:
        aid = int(row[0])
        nm_id = int(row[1])
        if aid not in bid_map:
            bid_map[aid] = []
        bid_map[aid].append({
            "nm_id": nm_id,
            "bid_search": int(row[2]),
            "bid_recommendations": int(row[3]),
            "subject_name": "",
            "product_name": "",
            "vendor_code": "",
        })
        all_nm_ids.add(nm_id)

    # Enrich with product names from PostgreSQL dim_products
    if all_nm_ids:
        from sqlalchemy import text
        product_rows = await db.execute(
            text("SELECT nm_id, vendor_code, name FROM dim_products WHERE nm_id = ANY(:ids)"),
            {"ids": list(all_nm_ids)},
        )
        product_map = {int(r[0]): {"vendor_code": r[1] or "", "name": r[2] or ""} for r in product_rows.fetchall()}

        for entries in bid_map.values():
            for entry in entries:
                prod = product_map.get(entry["nm_id"], {})
                entry["product_name"] = prod.get("name", "")
                entry["vendor_code"] = prod.get("vendor_code", "")

    # 3. Stats from ClickHouse (same as /campaigns/stats)
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

    # 4. Prev period stats for KPI deltas
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

    # 5. Budgets + Balance from Redis cache
    budgets_cache: dict = {}
    balance_data = None
    try:
        import redis.asyncio as aioredis
        from app.config import get_settings
        settings = get_settings()
        redis_client = await aioredis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )

        # Read all budget keys for this shop
        # Use pipeline for batch read
        active_ids = [int(row[0]) for row in campaign_rows]
        if active_ids:
            pipeline = redis_client.pipeline()
            for aid in active_ids:
                pipeline.get(f"budget:{shop.id}:{aid}")
            results = await pipeline.execute()

            for aid, cached in zip(active_ids, results):
                if cached:
                    budgets_cache[aid] = json.loads(cached)

        # Read balance
        balance_cached = await redis_client.get(f"balance:{shop.id}")
        if balance_cached:
            balance_data = json.loads(balance_cached)

        await redis_client.aclose()
    except Exception as e:
        logger.warning(f"[from-db] Redis cache unavailable: {e}")

    # 6. Build campaign list
    campaigns = []
    # Map ClickHouse Enum8 type names → WB type codes
    CH_TYPE_MAP = {
        "search": 1, "carousel": 2, "card": 4, "recommend": 5,
        "auto": 7, "search_plus_catalog": 8, "recommend_plus_carousel": 9,
    }

    for row in campaign_rows:
        cid = int(row[0])
        st = stats_map.get(cid, {})
        budget = budgets_cache.get(cid, {})

        status_code = int(row[3])
        raw_type = str(row[2])
        type_code = CH_TYPE_MAP.get(raw_type, 9)  # default to 9 (unified)
        try:
            type_code = int(raw_type)
        except (ValueError, TypeError):
            pass  # keep mapped value

        campaigns.append({
            "advert_id": cid,
            "name": str(row[1]),
            "type": type_code,
            "status": status_code,
            "status_label": WB_STATUS_LABELS.get(status_code, "Неизвестно"),
            "payment_type": str(row[4]),
            "bid_type": str(row[5]),
            "search_enabled": bool(int(row[6])),
            "recommendations_enabled": bool(int(row[7])),
            "change_time": None,
            "nm_settings": bid_map.get(cid, []),
            # Stats
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
            # Budget from Redis cache
            "budget_total": budget.get("total", 0),
            "budget_daily": budget.get("daily", 0),
        })

    # KPI
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

    return {
        "campaigns": campaigns,
        "total": len(campaigns),
        "balance": balance_data,
        "kpi": kpi,
        "kpi_deltas": kpi_deltas,
        "period": {"start": str(d_start), "end": str(d_end)},
    }


# ══════════════════════════════════════════════════════════════════
# Enriched Campaigns (management data + ClickHouse stats) — LEGACY
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


# ══════════════════════════════════════════════════════════════════
# Normquery Write Operations (UWB campaigns — bid_type=manual, payment_type=cpm)
# ══════════════════════════════════════════════════════════════════


class NormqueryBidItem(BaseModel):
    norm_query: str
    bid: int = PField(..., gt=0, description="Bid in kopecks")


class SetNormqueryBidsRequest(BaseModel):
    shop_id: int
    advert_id: int
    nm_id: int
    bids: list[NormqueryBidItem] = PField(..., min_length=1, max_length=200)


class SetMinusPhrasesRequest(BaseModel):
    shop_id: int
    advert_id: int
    nm_id: int
    norm_queries: list[str] = PField(..., description="Full list of minus phrases to set")


@router.post("/normquery/set-bids")
async def set_normquery_bids(
    request: SetNormqueryBidsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Set bids per search cluster (normquery) for a UWB campaign.

    Only for campaigns with bid_type=manual & payment_type=cpm.
    Bids are in KOPECKS (e.g. 30000 = 300₽).
    """
    from app.services.wb_normquery_service import WBNormqueryService

    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)

    bids_payload = [
        {
            "advert_id": request.advert_id,
            "nm_id": request.nm_id,
            "norm_query": b.norm_query,
            "bid": b.bid,
        }
        for b in request.bids
    ]

    result = await svc.set_normquery_bids(bids_payload)

    await _log_audit(
        db, current_user, shop.id,
        action="normquery_bid_change",
        advert_id=request.advert_id,
        details={
            "nm_id": request.nm_id,
            "bids_count": len(request.bids),
            "bids": [
                {"norm_query": b.norm_query, "bid_kopecks": b.bid, "bid_rub": b.bid / 100}
                for b in request.bids
            ],
        },
        success=result.get("success", False),
        error_message=result.get("message") if not result.get("success") else None,
    )

    if not result.get("success"):
        raise HTTPException(
            status_code=502,
            detail=result.get("message", "Ошибка установки ставок кластеров"),
        )

    return result


@router.post("/normquery/set-minus")
async def set_normquery_minus_phrases(
    request: SetMinusPhrasesRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Set minus phrases for a campaign (replaces the full list).

    Works for both manual and unified bid campaigns.
    Pass an empty array to clear all minus phrases.
    """
    from app.services.wb_normquery_service import WBNormqueryService

    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)

    result = await svc.set_minus_phrases(
        advert_id=request.advert_id,
        nm_id=request.nm_id,
        norm_queries=request.norm_queries,
    )

    await _log_audit(
        db, current_user, shop.id,
        action="normquery_minus_phrases",
        advert_id=request.advert_id,
        details={
            "nm_id": request.nm_id,
            "phrases_count": len(request.norm_queries),
            "phrases": request.norm_queries[:20],  # Log first 20 to avoid huge entries
        },
        success=result.get("success", False),
        error_message=result.get("message") if not result.get("success") else None,
    )

    if not result.get("success"):
        raise HTTPException(
            status_code=502,
            detail=result.get("message", "Ошибка установки минус-фраз"),
        )

    return result


class DepositBudgetRequest(BaseModel):
    shop_id: int
    advert_id: int
    amount: int = PField(..., gt=0, description="Amount in rubles")
    budget_type: int = PField(1, description="Fund source: 0=Account, 1=Balance, 3=Bonuses")


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

    # After successful deposit — update Redis cache with new budget
    new_budget_total = None
    try:
        import redis.asyncio as aioredis
        from app.config import get_settings
        settings = get_settings()

        redis_client = await aioredis.from_url(
            settings.redis_url, encoding="utf-8", decode_responses=True,
            socket_connect_timeout=3, socket_timeout=3,
        )
        cache_key = f"budget:{shop.id}:{request.advert_id}"

        # Step 1: Read old cached budget
        old_total = 0
        try:
            old_cached = await redis_client.get(cache_key)
            if old_cached:
                old_data = json.loads(old_cached)
                old_total = old_data.get("total", 0)
        except Exception:
            pass

        # Step 2: IMMEDIATELY write optimistic budget to Redis
        # Don't wait for WB API — this ensures any page reload shows correct value
        optimistic_total = old_total + request.amount
        optimistic_data = {"total": optimistic_total, "daily": 0, "currency": "RUB"}
        await redis_client.set(cache_key, json.dumps(optimistic_data), ex=1200)
        new_budget_total = optimistic_total
        logger.info(
            f"[deposit] Optimistic Redis update: budget:{shop.id}:{request.advert_id} "
            f"old={old_total} + {request.amount} = {optimistic_total}"
        )

        # Step 3: Try to get REAL budget from WB API (2s delay for eventual consistency)
        # This is best-effort — if it fails, we already have optimistic value
        try:
            import asyncio
            await asyncio.sleep(2.0)
            budget_result = await service.get_campaign_budget(request.advert_id)
            if budget_result.get("success"):
                api_data = budget_result["data"]
                api_total = api_data.get("total", 0)
                # Use WB API value only if it's higher than our optimistic
                # (WB API may return stale data that's lower)
                if api_total >= optimistic_total:
                    new_budget_total = api_total
                    await redis_client.set(cache_key, json.dumps(api_data), ex=1200)
                    logger.info(
                        f"[deposit] WB API confirms: budget:{shop.id}:{request.advert_id} "
                        f"total={api_total} (>= optimistic {optimistic_total})"
                    )
                else:
                    logger.info(
                        f"[deposit] WB API stale: {api_total} < optimistic {optimistic_total}, "
                        f"keeping optimistic value"
                    )
        except Exception as e2:
            logger.warning(f"[deposit] WB API budget fetch failed (keeping optimistic): {e2}")

        await redis_client.aclose()
    except Exception as e:
        # Redis itself failed — still try optimistic as return value
        new_budget_total = request.amount  # at minimum, we just deposited this
        logger.error(f"[deposit] Redis budget update FAILED: {e}")

    # Return result WITH the new budget total so frontend can show correct value
    result["new_budget_total"] = new_budget_total
    return result


# ══════════════════════════════════════════════════════════════════
# Stats Only (ClickHouse only — NO WB API calls)
# ══════════════════════════════════════════════════════════════════


@router.get("/campaigns/stats")
async def get_campaigns_stats(
    shop_id: int = Query(...),
    period: str = Query("30d", description="Period: today, 7d, 14d, 30d, 90d"),
    date_from: Optional[str] = Query(None, description="Custom start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Custom end date YYYY-MM-DD"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get ONLY ClickHouse statistics for campaigns — NO WB API calls.
    
    Use this endpoint when changing periods to avoid redundant WB API requests.
    The frontend should merge these stats with cached WB campaign data.
    
    Returns: stats per advert_id + KPI + deltas for the requested period.
    """
    from datetime import date as date_type, timedelta
    from app.core.clickhouse import get_clickhouse_client

    shop = await _verify_wb_shop(shop_id, current_user, db)

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

    # Aggregate KPI
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

    return {
        "stats": stats_map,
        "kpi": kpi,
        "kpi_deltas": kpi_deltas,
        "period": {"start": str(d_start), "end": str(d_end)},
    }


# ══════════════════════════════════════════════════════════════════
# Normquery — Unified Cluster List (combined list + stats + bids)
# ══════════════════════════════════════════════════════════════════


class ClusterListRequest(BaseModel):
    shop_id: int
    advert_id: int
    nm_id: int
    start_date: str = PField(..., description="YYYY-MM-DD")
    end_date: str = PField(..., description="YYYY-MM-DD")


@router.post("/normquery/cluster-list")
async def get_cluster_list(
    request: ClusterListRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get unified cluster list with status, bids, and stats.

    Combines 4 WB API calls in parallel:
    1. POST /adv/v0/normquery/list → active/excluded clusters
    2. POST /adv/v0/normquery/get-bids → current bids per cluster
    3. POST /adv/v0/normquery/stats → performance stats
    4. GET /api/advert/v0/bids/recommendations → recommended bids

    Returns unified array where each cluster has: status, bid, stats, recommendations.
    """
    from app.services.wb_normquery_service import WBNormqueryService

    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)

    items = [{"advert_id": request.advert_id, "nm_id": request.nm_id}]

    # Parallel fetch: cluster lists, bids, stats, recommendations
    list_result, bids_result, stats_result, recommendations = await asyncio.gather(
        svc.get_normquery_list(items),
        svc.get_normquery_bids(items),
        svc.get_normquery_stats(items, request.start_date, request.end_date),
        svc.get_bid_recommendations(request.advert_id, request.nm_id),
        return_exceptions=True,
    )

    # Parse cluster list (active/excluded)
    active_clusters: list = []
    excluded_clusters: list = []
    if isinstance(list_result, dict):
        list_items = list_result.get("items", [])
        if isinstance(list_result, list):
            list_items = list_result
        for item in list_items:
            if isinstance(item, dict):
                nq = item.get("normQueries", {}) or {}
                active_clusters = nq.get("active", []) or []
                excluded_clusters = nq.get("excluded", []) or []
                break

    # Parse bids: norm_query → bid (RUBLES! WB API returns bid in rubles, not kopecks)
    bid_map: dict = {}
    if isinstance(bids_result, dict):
        bids_list = bids_result.get("bids", [])
        if isinstance(bids_result, list):
            bids_list = bids_result
        for b in bids_list:
            if isinstance(b, dict):
                bid_map[b.get("norm_query", "")] = b.get("bid", 0)

    # Parse stats: norm_query → stats dict
    cluster_stats_map: dict = {}


    # WB API can return dict {"stats": [...]} or list [{...}]
    stats_items = []
    if isinstance(stats_result, dict):
        stats_items = stats_result.get("stats", [])
    elif isinstance(stats_result, list):
        stats_items = stats_result

    for si in stats_items:
        if isinstance(si, dict):
            for stat in si.get("stats", []):
                if isinstance(stat, dict):
                    nq = stat.get("norm_query", "")
                    cluster_stats_map[nq] = stat

    # Parse recommendations: norm_query → reach bids
    reach_map: dict = {}
    base_bids = {}
    if isinstance(recommendations, dict):
        base = recommendations.get("base", {})
        if base:
            competitive = base.get("competitiveBid", {}).get("bidKopecks", 0)
            leaders = base.get("leadersBid", {}).get("bidKopecks", 0)
            base_bids = {
                "competitive_kopecks": competitive,
                "leaders_kopecks": leaders,
                "competitive_rub": round(competitive / 100, 2) if competitive else 0,
                "leaders_rub": round(leaders / 100, 2) if leaders else 0,
            }
        for nq_rec in recommendations.get("normQueries", []):
            if isinstance(nq_rec, dict):
                nq_name = nq_rec.get("normQuery", "")
                reach_map[nq_name] = {
                    "max": nq_rec.get("reachMax", {}).get("bidKopecks", 0),
                    "med": nq_rec.get("reachMedium", {}).get("bidKopecks", 0),
                    "min": nq_rec.get("reachMin", {}).get("bidKopecks", 0),
                }

    # Build unified cluster list
    all_clusters = []
    seen = set()

    def _build_cluster(nq: str, status: str):
        if nq in seen or not nq:
            return None
        seen.add(nq)
        stat = cluster_stats_map.get(nq, {})
        bid_rub = bid_map.get(nq, 0)  # WB API returns bid in RUBLES
        reach = reach_map.get(nq, {})

        views = stat.get("views", 0)
        clicks = stat.get("clicks", 0)
        atbs = stat.get("atbs", 0)
        orders = stat.get("orders", 0)
        shks = stat.get("shks", 0)  # ordered items (from API)
        cpc = stat.get("cpc", 0)  # RUBLES (currency: RUB)
        cpm = stat.get("cpm", 0)  # RUBLES (currency: RUB)
        ctr = stat.get("ctr", 0)
        avg_pos = stat.get("avg_pos", 0)
        spend = stat.get("spend", 0)  # RUBLES (currency: RUB)

        # ALL monetary values from normquery/stats are already in RUBLES
        spend_rub = round(spend, 2) if spend else 0
        cpc_rub = round(cpc, 2) if cpc else 0
        cpm_rub = round(cpm, 2) if cpm else 0

        return {
            "norm_query": nq,
            "status": status,
            "views": views,
            "clicks": clicks,
            "atbs": atbs,
            "orders": orders,
            "shks": shks,
            "ctr": round(ctr, 2) if isinstance(ctr, float) else ctr,
            "avg_pos": round(avg_pos, 1) if isinstance(avg_pos, float) else avg_pos,
            "spend_rub": spend_rub,
            "cpc_kopecks": cpc,
            "cpc_rub": cpc_rub,
            "cpm_kopecks": cpm,
            "cpm_rub": cpm_rub,
            "current_bid_kopecks": bid_rub * 100,  # convert to kopecks for frontend compat
            "current_bid_rub": round(bid_rub, 2) if bid_rub else 0,
            "reach_max_bid": reach.get("max", 0),
            "reach_med_bid": reach.get("med", 0),
            "reach_min_bid": reach.get("min", 0),
            "cr_click_to_cart": round(atbs / clicks * 100, 1) if clicks > 0 else 0,
            "cr_click_to_order": round(orders / clicks * 100, 1) if clicks > 0 else 0,
        }

    for nq in active_clusters:
        c = _build_cluster(nq, "active")
        if c:
            all_clusters.append(c)

    for nq in excluded_clusters:
        c = _build_cluster(nq, "excluded")
        if c:
            all_clusters.append(c)

    # Also add clusters that appear in stats but not in active/excluded lists
    for nq in cluster_stats_map:
        c = _build_cluster(nq, "active")
        if c:
            all_clusters.append(c)

    return {
        "clusters": all_clusters,
        "total_active": len([c for c in all_clusters if c["status"] == "active"]),
        "total_excluded": len([c for c in all_clusters if c["status"] == "excluded"]),
        "total_clusters": len(all_clusters),
        "base_bids": base_bids,
    }


# ══════════════════════════════════════════════════════════════════
# Normquery — Toggle Cluster Exclusion (atomic)
# ══════════════════════════════════════════════════════════════════


class ToggleClusterRequest(BaseModel):
    shop_id: int
    advert_id: int
    nm_id: int
    norm_query: str
    action: str = PField(..., description="'exclude' or 'include'")


@router.post("/normquery/toggle-exclude")
async def toggle_cluster_exclusion(
    request: ToggleClusterRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Toggle a cluster's exclusion status (exclude ↔ include).

    Atomically: get-minus → modify list → set-minus.
    """
    from app.services.wb_normquery_service import WBNormqueryService

    if request.action not in ("exclude", "include"):
        raise HTTPException(
            status_code=400,
            detail="action must be 'exclude' or 'include'",
        )

    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)
    result = await svc.toggle_cluster_exclusion(
        advert_id=request.advert_id,
        nm_id=request.nm_id,
        norm_query=request.norm_query,
        action=request.action,
    )

    await _log_audit(
        db, current_user, shop.id,
        action="normquery_toggle_exclude",
        advert_id=request.advert_id,
        details={
            "nm_id": request.nm_id,
            "norm_query": request.norm_query,
            "action": request.action,
        },
        success=result.get("success", False),
        error_message=result.get("message") if not result.get("success") else None,
    )

    if not result.get("success"):
        raise HTTPException(
            status_code=502,
            detail=result.get("message", "Ошибка переключения кластера"),
        )

    return result


# ══════════════════════════════════════════════════════════════════
# Campaign Product (NM) Management
# ══════════════════════════════════════════════════════════════════


class ManageNmsRequest(BaseModel):
    shop_id: int
    advert_id: int
    add: list[int] = PField(default_factory=list, description="NM IDs to add")
    delete: list[int] = PField(default_factory=list, description="NM IDs to remove")


@router.patch("/campaigns/nms")
async def manage_campaign_nms(
    request: ManageNmsRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Add/remove products (nm_ids) from a campaign.

    WB API: PATCH /adv/v0/auction/nms
    """
    if not request.add and not request.delete:
        raise HTTPException(status_code=400, detail="Укажите add или delete")

    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBAdManagementService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.manage_campaign_nms(
        advert_id=request.advert_id,
        add=request.add,
        delete=request.delete,
    )

    await _log_audit(
        db, current_user, shop.id,
        action="campaign_nms_manage",
        advert_id=request.advert_id,
        details={
            "add": request.add,
            "delete": request.delete,
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


# ══════════════════════════════════════════════════════════════════
# Normquery — Cached Cluster List (from ClickHouse — instant!)
# ══════════════════════════════════════════════════════════════════


@router.post("/normquery/cluster-list-cached")
async def get_cluster_list_cached(
    request: ClusterListRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get cluster list from ClickHouse (pre-collected by sync_normquery_data).

    ZERO WB API calls — data comes from:
    1. fact_normquery_stats_daily → aggregated stats for the period
    2. log_normquery_bids → latest bid snapshot + recommendations

    Falls back to live API if no cached data exists.
    """
    from app.core.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()

    # 1. Get aggregated stats from ClickHouse for the period
    stats_rows = ch.query(
        """
        SELECT
            norm_query,
            sum(views) AS total_views,
            sum(clicks) AS total_clicks,
            sum(atbs) AS total_atbs,
            sum(orders) AS total_orders,
            sum(shks) AS total_shks,
            sum(spend) AS total_spend,
            -- Weighted avg position (by views)
            if(sum(views) > 0,
               sum(toFloat64(avg_pos) * views) / sum(views),
               0) AS w_avg_pos,
            -- Weighted CPC (spend / clicks)
            if(sum(clicks) > 0,
               sum(spend) / sum(clicks),
               0) AS w_cpc_rub,
            -- Weighted CPM (spend / views * 1000)
            if(sum(views) > 0,
               sum(spend) / sum(views) * 1000,
               0) AS w_cpm_rub,
            -- Weighted CTR
            if(sum(views) > 0,
               sum(clicks) / sum(views) * 100,
               0) AS w_ctr
        FROM mms_analytics.fact_normquery_stats_daily FINAL
        WHERE shop_id = {sid:UInt32}
          AND advert_id = {cid:UInt64}
          AND dt >= {dt_from:String}
          AND dt <= {dt_to:String}
        GROUP BY norm_query
        HAVING total_views > 0 OR total_clicks > 0 OR total_spend > 0
        ORDER BY total_views DESC
        """,
        parameters={
            "sid": request.shop_id,
            "cid": request.advert_id,
            "dt_from": request.start_date,
            "dt_to": request.end_date,
        },
    ).result_rows

    # If no cached data — fall back to live API
    if not stats_rows:
        logger.info(f"[normquery-cached] No CH data for shop={request.shop_id} "
                     f"campaign={request.advert_id}, falling back to live API")
        return await get_cluster_list(request, current_user, db)

    # 2. Get latest bid snapshot from log_normquery_bids
    bid_rows = ch.query(
        """
        SELECT
            norm_query,
            argMax(bid_kopecks, timestamp) AS bid_kopecks,
            argMax(reach_max_bid, timestamp) AS reach_max,
            argMax(reach_med_bid, timestamp) AS reach_med,
            argMax(reach_min_bid, timestamp) AS reach_min,
            argMax(competitive_bid, timestamp) AS competitive,
            argMax(leaders_bid, timestamp) AS leaders
        FROM mms_analytics.log_normquery_bids
        WHERE shop_id = {sid:UInt32}
          AND advert_id = {cid:UInt64}
        GROUP BY norm_query
        """,
        parameters={
            "sid": request.shop_id,
            "cid": request.advert_id,
        },
    ).result_rows

    bid_map = {}
    competitive_kopecks = 0
    leaders_kopecks = 0
    for row in bid_rows:
        nq, bid_k, r_max, r_med, r_min, comp, lead = row
        bid_map[nq] = {
            "bid_kopecks": int(bid_k),
            "reach_max": int(r_max),
            "reach_med": int(r_med),
            "reach_min": int(r_min),
        }
        if int(comp) > competitive_kopecks:
            competitive_kopecks = int(comp)
        if int(lead) > leaders_kopecks:
            leaders_kopecks = int(lead)

    # 3. Get active/excluded status from live API (lightweight call)
    excluded_set = set()
    try:
        from app.services.wb_normquery_service import WBNormqueryService
        shop = await _verify_wb_shop(request.shop_id, current_user, db)
        api_key = await _get_api_key(shop)
        svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)
        items = [{"advert_id": request.advert_id, "nm_id": request.nm_id}]
        list_result = await svc.get_normquery_list(items)
        if isinstance(list_result, dict):
            for item in list_result.get("items", []):
                if isinstance(item, dict):
                    nq_data = item.get("normQueries", {}) or {}
                    excluded_set = set(nq_data.get("excluded", []) or [])
                    break
    except Exception as e:
        logger.warning(f"[normquery-cached] Could not get cluster status: {e}")

    # 4. Build response (same format as live endpoint)
    clusters = []
    for row in stats_rows:
        nq, views, clicks, atbs, orders, shks, spend, avg_pos, cpc_rub, cpm_rub, ctr = row
        views = int(views)
        clicks = int(clicks)
        atbs = int(atbs)
        orders = int(orders)
        shks = int(shks)
        spend = float(spend)
        avg_pos = float(avg_pos)
        cpc_rub = float(cpc_rub)
        cpm_rub = float(cpm_rub)
        ctr = float(ctr)

        status = "excluded" if nq in excluded_set else "active"
        bid_info = bid_map.get(nq, {})
        bid_kopecks = bid_info.get("bid_kopecks", 0)

        clusters.append({
            "norm_query": nq,
            "status": status,
            "views": views,
            "clicks": clicks,
            "atbs": atbs,
            "orders": orders,
            "shks": shks,
            "ctr": round(ctr, 2),
            "avg_pos": round(avg_pos, 1),
            "spend_rub": round(spend, 2),
            "cpc_kopecks": int(cpc_rub * 100),  # backward compat
            "cpc_rub": round(cpc_rub, 2),
            "cpm_kopecks": int(cpm_rub * 100),  # backward compat
            "cpm_rub": round(cpm_rub, 2),
            "current_bid_kopecks": bid_kopecks,
            "current_bid_rub": round(bid_kopecks / 100, 2) if bid_kopecks else 0,
            "reach_max_bid": bid_info.get("reach_max", 0),
            "reach_med_bid": bid_info.get("reach_med", 0),
            "reach_min_bid": bid_info.get("reach_min", 0),
            "cr_click_to_cart": round(atbs / clicks * 100, 1) if clicks > 0 else 0,
            "cr_click_to_order": round(orders / clicks * 100, 1) if clicks > 0 else 0,
        })

    base_bids = {
        "competitive_kopecks": competitive_kopecks,
        "leaders_kopecks": leaders_kopecks,
        "competitive_rub": round(competitive_kopecks / 100, 2) if competitive_kopecks else 0,
        "leaders_rub": round(leaders_kopecks / 100, 2) if leaders_kopecks else 0,
    }

    return {
        "clusters": clusters,
        "total_active": len([c for c in clusters if c["status"] == "active"]),
        "total_excluded": len([c for c in clusters if c["status"] == "excluded"]),
        "total_clusters": len(clusters),
        "base_bids": base_bids,
        "source": "clickhouse",  # indicator for frontend
    }


# ══════════════════════════════════════════════════════════════════
# Campaign Creation
# ══════════════════════════════════════════════════════════════════


@router.get("/creation/subjects")
async def get_creation_subjects(
    shop_id: int = Query(...),
    payment_type: str = Query("cpm"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Get available subjects (categories) for campaign creation."""
    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBCampaignCreationService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.get_subjects(payment_type=payment_type)

    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message", "Ошибка"))

    return result


@router.post("/creation/products")
async def get_creation_products(
    request: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get products available for advertising by subject IDs.

    WB API only returns title/nm/subjectId.
    We enrich each product with vendor_code from PostgreSQL dim_products.
    """
    shop_id = request.get("shop_id")
    subject_ids = request.get("subject_ids", [])

    if not shop_id:
        raise HTTPException(status_code=400, detail="shop_id is required")

    shop = await _verify_wb_shop(shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    service = WBCampaignCreationService(db=db, shop_id=shop.id, api_key=api_key)
    result = await service.get_products(subject_ids=subject_ids)

    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message", "Ошибка"))

    # Enrich products with vendor_code from PostgreSQL
    products = result.get("products", [])
    if products:
        nm_ids = [p.get("nm") for p in products if isinstance(p, dict) and p.get("nm")]
        if nm_ids:
            from sqlalchemy import text
            product_rows = await db.execute(
                text("SELECT nm_id, vendor_code FROM dim_products WHERE nm_id = ANY(:ids)"),
                {"ids": nm_ids},
            )
            vendor_map = {
                int(r[0]): r[1] or ""
                for r in product_rows.fetchall()
            }
            for p in products:
                if isinstance(p, dict):
                    p["vendor_code"] = vendor_map.get(p.get("nm", 0), "")

    return result


class InitialBidItem(BaseModel):
    nm_id: int
    bid_kopecks: int = Field(..., gt=0, description="Bid in kopecks (e.g. 15000 = 150₽)")


class CreateCampaignRequest(BaseModel):
    shop_id: int
    name: str = Field(..., min_length=1, max_length=128)
    nms: TypingList[int] = Field(..., min_length=1, max_length=50)
    bid_type: str = Field("unified", pattern="^(unified|manual)$")
    payment_type: str = Field("cpm", pattern="^(cpm|cpc)$")
    placement_types: TypingList[str] = Field(default=[])
    budget: int = Field(0, ge=0, description="Initial budget in rubles (0 = no deposit)")
    auto_start: bool = Field(False, description="Auto-start campaign after creation")
    initial_bids: TypingList[InitialBidItem] = Field(default=[], description="Initial bids per nm_id in kopecks")


@router.post("/creation/create")
async def create_campaign(
    request: CreateCampaignRequest,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Create a new WB advertising campaign.

    Flow:
    1. POST /adv/v2/seacat/save-ad → get advert_id
    2. (optional) POST /adv/v1/budget/deposit → fund the campaign
    3. (optional) GET /adv/v0/start → start the campaign

    Returns combined result with advert_id, budget status, and start status.
    """
    shop = await _verify_wb_shop(request.shop_id, current_user, db)
    api_key = await _get_api_key(shop)

    creation_svc = WBCampaignCreationService(
        db=db, shop_id=shop.id, api_key=api_key,
    )
    mgmt_svc = WBAdManagementService(
        db=db, shop_id=shop.id, api_key=api_key,
    )

    # Step 1: Create campaign
    create_result = await creation_svc.create_campaign(
        name=request.name,
        nms=request.nms,
        bid_type=request.bid_type,
        payment_type=request.payment_type,
        placement_types=request.placement_types if request.bid_type == "manual" else None,
    )

    if not create_result.get("success"):
        await _log_audit(
            db, current_user, shop.id,
            action="campaign_create",
            advert_id=None,
            details={
                "name": request.name,
                "nms_count": len(request.nms),
                "bid_type": request.bid_type,
            },
            success=False,
            error_message=create_result.get("message"),
        )
        raise HTTPException(
            status_code=400,
            detail=create_result.get("message", "Ошибка создания кампании"),
        )

    advert_id = create_result["advert_id"]
    response = {
        "success": True,
        "advert_id": advert_id,
        "message": create_result["message"],
        "budget_deposited": False,
        "campaign_started": False,
        "bids_applied": False,
    }

    # Immediately insert the new campaign into ClickHouse so it appears in the list
    # Status 4 = "Готова к запуску" (initial status after save-ad)
    await _insert_new_campaign_to_ch(
        shop_id=shop.id,
        advert_id=advert_id,
        name=request.name,
        status=4,
        payment_type=request.payment_type,
        bid_type=request.bid_type,  # 'manual' or 'unified' — used by frontend to determine column layout
        search_enabled=1,
        recommendations_enabled=1 if request.bid_type == "unified" else 0,
    )

    # Immediately insert nm_ids into log_wb_bids so products appear in management modal
    # (otherwise they only appear after the next Celery sync, ~15 min later)
    await _insert_initial_nm_settings_to_ch(
        shop_id=shop.id,
        advert_id=advert_id,
        nms=request.nms,
        initial_bids={b.nm_id: b.bid_kopecks for b in request.initial_bids},
    )

    # Step 2: Deposit budget (if requested)
    if request.budget > 0:
        # Small delay to let WB register the campaign
        await asyncio.sleep(1.5)
        deposit_result = await mgmt_svc.deposit_budget(
            advert_id=advert_id,
            amount=request.budget,
            budget_type=1,  # Balance
        )
        response["budget_deposited"] = deposit_result.get("success", False)
        if deposit_result.get("success"):
            # Immediately cache budget in Redis so from-db shows it
            try:
                import redis.asyncio as aioredis
                from app.config import get_settings
                settings = get_settings()
                redis_client = await aioredis.from_url(
                    settings.redis_url, encoding="utf-8", decode_responses=True,
                    socket_connect_timeout=2, socket_timeout=2,
                )
                cache_key = f"budget:{shop.id}:{advert_id}"
                budget_data = {"total": request.budget, "daily": 0, "currency": "RUB"}
                await redis_client.set(cache_key, json.dumps(budget_data), ex=1200)
                await redis_client.aclose()
                logger.info(
                    f"[campaign-create] Cached budget in Redis: {cache_key} = {request.budget}₽"
                )
            except Exception as e:
                logger.warning(f"[campaign-create] Failed to cache budget: {e}")
        else:
            response["budget_message"] = deposit_result.get("message", "")
            logger.warning(
                f"[campaign-create] Budget deposit failed for new campaign {advert_id}: "
                f"{deposit_result.get('message')}"
            )

    # Step 3: Set initial bids BEFORE start
    # WB sets default minimum bids on save-ad, so we need to override them.
    # Doing this BEFORE start ensures the campaign runs with correct bids from the first second.
    bids_applied_ok = False
    if request.initial_bids:
        await asyncio.sleep(3)  # WB needs time to register the campaign
        placement = "combined" if request.bid_type == "unified" else "search"
        bids_dicts = [{"nm_id": b.nm_id, "bid": b.bid_kopecks} for b in request.initial_bids]
        try:
            logger.info(
                f"[campaign-create] Step 3: Setting initial bids for campaign {advert_id} "
                f"(status=4, before start). "
                f"Placement={placement}, bid_type={request.bid_type}, "
                f"bids={[{'nm_id': b.nm_id, 'bid_kopecks': b.bid_kopecks} for b in request.initial_bids]}"
            )
            bids_result = await mgmt_svc.change_bids(
                advert_id=advert_id,
                placement=placement,
                bids=bids_dicts,
                bid_type=request.bid_type,
            )
            bids_applied_ok = bids_result.get("success", False)
            response["bids_applied"] = bids_applied_ok
            if bids_applied_ok:
                logger.info(
                    f"[campaign-create] ✅ Initial bids set for campaign {advert_id}: "
                    f"{len(request.initial_bids)} nm_ids, placement={placement}"
                )
            else:
                logger.warning(
                    f"[campaign-create] ⚠ Failed to set initial bids (pre-start) for {advert_id}: "
                    f"status_code={bids_result.get('status_code')}, "
                    f"message={bids_result.get('message')}"
                )
        except Exception as e:
            logger.warning(
                f"[campaign-create] ❌ Error setting initial bids (pre-start) for {advert_id}: {e}"
            )

    # Step 4: Auto-start (if requested and budget was deposited)
    if request.auto_start and response["budget_deposited"]:
        await asyncio.sleep(1)
        start_result = await mgmt_svc.start_campaign(advert_id)
        response["campaign_started"] = start_result.get("success", False)
        if response["campaign_started"]:
            await _update_campaign_status_in_ch(shop.id, advert_id, 9)

    # Step 5: Retry bids after start (if pre-start attempt failed)
    # Some WB campaigns may only accept bid changes in status=9 (active)
    if request.initial_bids and not bids_applied_ok and response["campaign_started"]:
        await asyncio.sleep(3)
        placement = "combined" if request.bid_type == "unified" else "search"
        bids_dicts = [{"nm_id": b.nm_id, "bid": b.bid_kopecks} for b in request.initial_bids]
        try:
            logger.info(
                f"[campaign-create] Step 5: Retrying bids for campaign {advert_id} "
                f"(status=9, after start). Attempt 2."
            )
            bids_result = await mgmt_svc.change_bids(
                advert_id=advert_id,
                placement=placement,
                bids=bids_dicts,
                bid_type=request.bid_type,
            )
            response["bids_applied"] = bids_result.get("success", False)
            if bids_result.get("success"):
                logger.info(
                    f"[campaign-create] ✅ Initial bids set (post-start retry) for {advert_id}"
                )
            else:
                logger.warning(
                    f"[campaign-create] ⚠ Failed to set bids (post-start retry) for {advert_id}: "
                    f"status_code={bids_result.get('status_code')}, "
                    f"message={bids_result.get('message')}"
                )
        except Exception as e:
            logger.warning(
                f"[campaign-create] ❌ Error setting bids (post-start retry) for {advert_id}: {e}"
            )

    # Audit log
    await _log_audit(
        db, current_user, shop.id,
        action="campaign_create",
        advert_id=advert_id,
        details={
            "name": request.name,
            "nms": request.nms,
            "bid_type": request.bid_type,
            "payment_type": request.payment_type,
            "budget": request.budget,
            "auto_start": request.auto_start,
            "budget_deposited": response["budget_deposited"],
            "campaign_started": response["campaign_started"],
            "bids_applied": response["bids_applied"],
            "initial_bids_count": len(request.initial_bids),
        },
        success=True,
    )

    return response


async def _insert_initial_nm_settings_to_ch(
    shop_id: int,
    advert_id: int,
    nms: TypingList[int],
    initial_bids: dict = None,
):
    """
    Insert initial nm_ids into log_wb_bids so that the management modal
    shows products immediately after campaign creation (without waiting for Celery sync).

    Args:
        shop_id: Shop ID
        advert_id: Campaign ID
        nms: List of nm_ids added to the campaign
        initial_bids: Optional dict {nm_id: bid_kopecks} with requested bids
    """
    if not nms:
        return

    try:
        from app.core.clickhouse import get_clickhouse_client
        from datetime import datetime
        ch = get_clickhouse_client()

        bids = initial_bids or {}
        now = datetime.utcnow()

        # Build batch insert: each nm_id gets a row in log_wb_bids
        rows = []
        for nm_id in nms:
            bid_kopecks = max(bids.get(nm_id, 0), 1)  # min 1 kopeck to ensure HAVING clause passes
            rows.append({
                "shop_id": shop_id,
                "advert_id": advert_id,
                "nm_id": nm_id,
                "bid_search": bid_kopecks,
                "bid_recommendations": bid_kopecks,
                "timestamp": now,
            })

        if rows:
            ch.command(
                """
                INSERT INTO mms_analytics.log_wb_bids
                    (shop_id, advert_id, nm_id, bid_search, bid_recommendations, timestamp)
                VALUES
                """
                + ", ".join(
                    f"({r['shop_id']}, {r['advert_id']}, {r['nm_id']}, "
                    f"{r['bid_search']}, {r['bid_recommendations']}, "
                    f"'{r['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}')"
                    for r in rows
                )
            )
            logger.info(
                f"[campaign-create] Inserted {len(rows)} nm_settings into log_wb_bids "
                f"for campaign {advert_id} (shop={shop_id})"
            )
    except Exception as e:
        logger.warning(
            f"[campaign-create] Failed to insert nm_settings into log_wb_bids: {e}"
        )

