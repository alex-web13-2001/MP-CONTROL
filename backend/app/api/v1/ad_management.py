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

    # After successful deposit — refresh Redis cache for this campaign's budget
    try:
        import redis.asyncio as aioredis
        from app.config import get_settings
        settings = get_settings()

        # Fetch fresh budget from WB API
        budget_result = await service.get_campaign_budget(request.advert_id)
        if budget_result.get("success"):
            redis_client = await aioredis.from_url(
                settings.redis_url, encoding="utf-8", decode_responses=True,
                socket_connect_timeout=2, socket_timeout=2,
            )
            cache_key = f"budget:{shop.id}:{request.advert_id}"
            await redis_client.set(cache_key, json.dumps(budget_result["data"]), ex=1200)
            await redis_client.aclose()
            logger.info(f"[deposit] Refreshed Redis cache for budget:{shop.id}:{request.advert_id}")
    except Exception as e:
        logger.warning(f"[deposit] Failed to refresh Redis budget cache: {e}")

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

