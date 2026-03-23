from fastapi import APIRouter, Depends, Query, HTTPException
from typing import List, Optional
from datetime import date, timedelta, datetime as dt_datetime
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select

from app.core.database import get_db
from app.core.clickhouse import get_clickhouse_client
from app.core.security import get_current_user
from app.models.user import User

router = APIRouter(prefix="/campaign-details", tags=["Campaign Details"])


def _normalize_mp(marketplace: str) -> str:
    """Normalize marketplace string: 'wildberries' → 'wb', 'ozon' stays 'ozon'."""
    mp = marketplace.lower().strip()
    if mp in ("wildberries", "wb"):
        return "wb"
    return mp

# --- Models ---

class CampaignStatsRow(BaseModel):
    dt: date
    views: int
    clicks: int
    orders: int
    cart: int
    revenue: float
    spend: float
    ctr: float
    drr: float
    product_revenue: float = 0

class CampaignEventRow(BaseModel):
    id: int
    timestamp: str
    event_type: str
    product_id: Optional[str]
    product_name: Optional[str] = None
    offer_id: Optional[str] = None
    old_value: Optional[str]
    new_value: Optional[str]

class CampaignPhraseRow(BaseModel):
    phrase: str
    views: int
    clicks: int
    ctr: float
    spend: float
    orders: int
    revenue: float

class CampaignHeatmapRow(BaseModel):
    day_of_week: int
    hour: int
    orders: int

class KpiPeriod(BaseModel):
    spend: float = 0
    ad_revenue: float = 0
    product_revenue: float = 0
    orders: int = 0
    cart: int = 0
    clicks: int = 0
    views: int = 0
    ctr: float = 0
    drr_ad: float = 0
    drr_product: float = 0
    cpo: float = 0

class CampaignKpiResponse(BaseModel):
    current: KpiPeriod
    previous: KpiPeriod
    first_date: Optional[str] = None  # earliest date with stats (campaign launch date)

# --- Endpoints ---

@router.get("/{marketplace}/{campaign_id}/kpi", response_model=CampaignKpiResponse)
async def get_campaign_kpi(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(...),
    end_date: date = Query(...),
    sku: Optional[int] = Query(None),
    current_user: User = Depends(get_current_user),
):
    """
    KPI aggregates for current period + previous period (same length) for delta comparison.
    Also includes total product revenue from orders tables.
    """
    marketplace = _normalize_mp(marketplace)
    from datetime import timedelta
    ch = get_clickhouse_client()
    
    period_days = (end_date - start_date).days + 1
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=period_days - 1)
    
    def build_ad_query(mp: str, sd: date, ed: date) -> tuple:
        params = {"campaign_id": campaign_id, "start_date": sd, "end_date": ed}
        if mp == "ozon":
            sku_f = "AND sku = {sku:UInt64}" if sku else ""
            if sku: params["sku"] = sku
            q = f"""
                SELECT
                    sum(money_spent), sum(revenue), sum(orders), sum(add_to_cart),
                    sum(clicks), sum(views)
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE campaign_id = {{campaign_id:UInt64}}
                  AND dt BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f}
            """
        else:
            sku_f = "AND nm_id = {sku:UInt64}" if sku else ""
            if sku: params["sku"] = sku
            q = f"""
                SELECT
                    sum(spend), sum(revenue), sum(orders), sum(atbs),
                    sum(clicks), sum(views)
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE advert_id = {{campaign_id:UInt64}}
                  AND date BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f}
            """
        return q, params
    
    def build_product_rev_query(mp: str, skus: list, shop_id: int, sd: date, ed: date) -> tuple:
        if not skus:
            return None, {}
        if mp == "ozon":
            q = """
                SELECT sum(price * quantity) 
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND sku IN {skus:Array(UInt64)}
                  AND order_date BETWEEN {start_date:Date} AND {end_date:Date}
            """
            return q, {"shop_id": shop_id, "skus": skus, "start_date": sd, "end_date": ed}
        else:
            # WB: get total product revenue from orders table
            q = """
                SELECT sum(finished_price) 
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nm_id IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
                  AND is_cancel = 0
            """
            return q, {"shop_id": shop_id, "skus": skus, "start_date": sd, "end_date": ed}
    
    # Get campaign SKUs + shop_id
    if marketplace.lower() == "ozon":
        skus_r = ch.query(
            "SELECT DISTINCT sku, shop_id FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64}",
            parameters={"cid": campaign_id}
        ).result_rows
    else:
        skus_r = ch.query(
            "SELECT DISTINCT nm_id, shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND (views > 0 OR clicks > 0 OR spend > 0)",
            parameters={"cid": campaign_id}
        ).result_rows
    
    campaign_skus = [int(r[0]) for r in skus_r]
    shop_id = int(skus_r[0][1]) if skus_r else 0
    if sku:
        campaign_skus = [sku]
    
    def make_kpi(ad_row, prod_rev: float) -> KpiPeriod:
        spend = float(ad_row[0] or 0)
        ad_rev = float(ad_row[1] or 0)
        orders = int(ad_row[2] or 0)
        cart = int(ad_row[3] or 0)
        clicks = int(ad_row[4] or 0)
        views = int(ad_row[5] or 0)
        # For WB: product_revenue = ad_revenue (from fact_advert_stats_v3)
        effective_prod_rev = prod_rev if prod_rev > 0 else ad_rev
        ctr = (clicks / views * 100) if views > 0 else 0
        drr_ad = (spend / ad_rev * 100) if ad_rev > 0 else 0
        drr_prod = (spend / effective_prod_rev * 100) if effective_prod_rev > 0 else 0
        cpo = (spend / orders) if orders > 0 else 0
        return KpiPeriod(
            spend=round(spend, 2), ad_revenue=round(ad_rev, 2),
            product_revenue=round(effective_prod_rev, 2),
            orders=orders, cart=cart, clicks=clicks, views=views,
            ctr=round(ctr, 2), drr_ad=round(drr_ad, 2),
            drr_product=round(drr_prod, 2), cpo=round(cpo, 2),
        )
    
    # Current period
    q_cur, p_cur = build_ad_query(marketplace.lower(), start_date, end_date)
    ad_cur = ch.query(q_cur, parameters=p_cur).result_rows
    ad_cur_row = ad_cur[0] if ad_cur else (0, 0, 0, 0, 0, 0)
    
    # Previous period
    q_prev, p_prev = build_ad_query(marketplace.lower(), prev_start, prev_end)
    ad_prev = ch.query(q_prev, parameters=p_prev).result_rows
    ad_prev_row = ad_prev[0] if ad_prev else (0, 0, 0, 0, 0, 0)
    
    # Product revenue (only for Ozon where we have separate orders data)
    prod_rev_cur = 0.0
    prod_rev_prev = 0.0
    if campaign_skus and shop_id:
        pq_cur, pp_cur = build_product_rev_query(marketplace.lower(), campaign_skus, shop_id, start_date, end_date)
        if pq_cur:
            pr = ch.query(pq_cur, parameters=pp_cur).result_rows
            prod_rev_cur = float(pr[0][0] or 0) if pr else 0
        
        pq_prev, pp_prev = build_product_rev_query(marketplace.lower(), campaign_skus, shop_id, prev_start, prev_end)
        if pq_prev:
            pr = ch.query(pq_prev, parameters=pp_prev).result_rows
            prod_rev_prev = float(pr[0][0] or 0) if pr else 0
    
    # First date (campaign launch date)
    first_date_val = None
    if marketplace.lower() == "ozon":
        fd_rows = ch.query(
            "SELECT min(dt) FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64}",
            parameters={"cid": campaign_id}
        ).result_rows
    else:
        fd_rows = ch.query(
            "SELECT min(date) FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64}",
            parameters={"cid": campaign_id}
        ).result_rows
    if fd_rows and fd_rows[0][0]:
        fd_val = fd_rows[0][0]
        if isinstance(fd_val, dt_datetime):
            first_date_val = fd_val.strftime("%Y-%m-%d")
        elif hasattr(fd_val, 'isoformat'):
            first_date_val = fd_val.isoformat()
        else:
            first_date_val = str(fd_val)

    return CampaignKpiResponse(
        current=make_kpi(ad_cur_row, prod_rev_cur),
        previous=make_kpi(ad_prev_row, prod_rev_prev),
        first_date=first_date_val,
    )

@router.get("/{marketplace}/{campaign_id}/stats", response_model=List[CampaignStatsRow])
async def get_campaign_stats(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    sku: Optional[int] = Query(None, description="Filter by specific SKU/nmId inside campaign"),
    current_user: User = Depends(get_current_user),
):
    """
    Get time-series statistics for a specific campaign (Spend, Views, Clicks, Orders, DRR).
    Works for both Ozon and WB.
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    
    query = ""
    params = {
        "campaign_id": campaign_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    
    if marketplace.lower() == "ozon":
        sku_filter = "AND sku = {sku:UInt64}" if sku else ""
        if sku:
            params["sku"] = sku
        
        query = f"""
            SELECT
                dt,
                sum(views) as t_views,
                sum(clicks) as t_clicks,
                sum(orders) as t_orders,
                sum(add_to_cart) as t_cart,
                sum(revenue) as t_revenue,
                sum(money_spent) as t_spend,
                if(sum(views)>0, round(sum(clicks)/sum(views)*100, 2), 0) as t_ctr,
                if(sum(revenue)>0, round(sum(money_spent)/sum(revenue)*100, 2), 0) as t_drr
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE campaign_id = {{campaign_id:UInt64}}
              AND dt BETWEEN {{start_date:Date}} AND {{end_date:Date}}
              {sku_filter}
            GROUP BY dt
            ORDER BY dt ASC
        """
    elif marketplace.lower() == "wb":
        sku_filter = "AND nm_id = {sku:UInt64}" if sku else ""
        if sku:
            params["sku"] = sku
            
        query = f"""
            SELECT
                date as dt,
                sum(views) as t_views,
                sum(clicks) as t_clicks,
                sum(orders) as t_orders,
                sum(atbs) as t_cart,
                sum(revenue) as t_revenue,
                sum(spend) as t_spend,
                if(sum(views)>0, round(sum(clicks)/sum(views)*100, 2), 0) as t_ctr,
                if(sum(revenue)>0, round(sum(spend)/sum(revenue)*100, 2), 0) as t_drr
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE advert_id = {{campaign_id:UInt64}}
              AND date BETWEEN {{start_date:Date}} AND {{end_date:Date}}
              {sku_filter}
            GROUP BY date
            ORDER BY date ASC
        """
    else:
        raise HTTPException(status_code=400, detail="Invalid marketplace")

    rows = ch.query(query, parameters=params).result_rows
    
    # Build result directly — all data comes from the ad stats table
    def _to_date(v):
        return v.date() if isinstance(v, dt_datetime) else v
    
    result = []
    for r in rows:
        result.append(CampaignStatsRow(
            dt=_to_date(r[0]),
            views=int(r[1]), clicks=int(r[2]), orders=int(r[3]),
            cart=int(r[4]), revenue=float(r[5]), spend=float(r[6]),
            ctr=float(r[7]), drr=float(r[8]),
            product_revenue=0,
        ))
    
    # Get daily product revenue from actual orders (total, not just ad-attributed)
    if marketplace == "wb":
        skus_for_rev = []
        shop_id_for_rev = 0
        if sku:
            skus_for_rev = [sku]
        else:
            skus_q = ch.query(
                "SELECT DISTINCT nm_id, shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND (views > 0 OR clicks > 0 OR spend > 0)",
                parameters={"cid": campaign_id}
            ).result_rows
            skus_for_rev = [int(r[0]) for r in skus_q]
            if skus_q:
                shop_id_for_rev = int(skus_q[0][1])
        if not shop_id_for_rev and skus_for_rev:
            sh = ch.query(
                "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
                parameters={"cid": campaign_id}
            ).result_rows
            shop_id_for_rev = int(sh[0][0]) if sh else 0
        if sku:
            sh = ch.query(
                "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
                parameters={"cid": campaign_id}
            ).result_rows
            shop_id_for_rev = int(sh[0][0]) if sh else 0
        
        prod_rev_by_date = {}
        if skus_for_rev and shop_id_for_rev:
            pr_rows = ch.query(
                """
                SELECT toDate(date) as d, sum(finished_price)
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nm_id IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
                  AND is_cancel = 0
                GROUP BY d
                """,
                parameters={"shop_id": shop_id_for_rev, "skus": skus_for_rev, "start_date": start_date, "end_date": end_date}
            ).result_rows
            for pr in pr_rows:
                prod_rev_by_date[_to_date(pr[0])] = float(pr[1] or 0)
        
        # Merge product_revenue into results
        for row in result:
            row.product_revenue = round(prod_rev_by_date.get(row.dt, 0), 2)
    
    return result

@router.get("/{marketplace}/{campaign_id}/events", response_model=List[CampaignEventRow])
async def get_campaign_events(
    marketplace: str,
    campaign_id: int,
    sku: Optional[int] = Query(None, description="Filter events for specific SKU inside campaign"),
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """
    Get history log of events (bids changes, stock changes, content updates) for products in this campaign.
    For Ozon: ad stats store 'sku', but event_log stores nm_id = product_id.
    We must convert sku -> product_id via dim_ozon_products.
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    ad_skus = []
    
    if sku:
        ad_skus = [sku]
    else:
        if marketplace.lower() == "ozon":
            res = ch.query(
                "SELECT DISTINCT sku FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} AND dt >= today()-90",
                parameters={"cid": campaign_id}
            ).result_rows
        else:
            res = ch.query(
                "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND date >= today()-90 AND (views > 0 OR clicks > 0 OR spend > 0)",
                parameters={"cid": campaign_id}
            ).result_rows
        ad_skus = [int(r[0]) for r in res]
        
    if not ad_skus:
        return []

    # Get shop_id
    if marketplace.lower() == "ozon":
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
    else:
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
    
    if not shop_row:
        return []
    
    shop_id = int(shop_row[0][0])
    
    # For Ozon: convert sku -> product_id (event_log uses nm_id = product_id, NOT sku!)
    if marketplace.lower() == "ozon":
        pg_sku_q = text("SELECT product_id, sku FROM dim_ozon_products WHERE shop_id = :sid AND sku = ANY(:skus)")
        pg_sku_res = await db.execute(pg_sku_q, {"sid": shop_id, "skus": ad_skus})
        sku_to_pid = {}
        for r in pg_sku_res:
            sku_to_pid[int(r[1])] = int(r[0])
        
        # Use product_ids for event_log query, PLUS original skus (some events like OZON_BID_CHANGE may use sku)
        event_nm_ids = list(set(list(sku_to_pid.values()) + [int(s) for s in ad_skus]))
        nm_ids_str = [str(x) for x in event_nm_ids]
    else:
        # WB: nm_id in event_log = nm_id in ad stats
        nm_ids_str = [str(s) for s in ad_skus]
        sku_to_pid = {}
    
    # Exclude individual warehouse stock events — only show full FBO/FBS stockouts
    excluded_events = ['STOCK_OUT', 'STOCK_REPLENISH', 'OZON_STOCK_OUT', 'OZON_STOCK_REPLENISH']
    
    # Campaign-level events (BID_CHANGE, STATUS_CHANGE, etc.) must be filtered by advert_id
    # to avoid showing events from OTHER campaigns that advertise the same product.
    # Product-level events (STOCK, PRICE, CONTENT) are filtered by nm_id only.
    campaign_event_types = [
        'BID_CHANGE', 'STATUS_CHANGE', 'ITEM_ADD', 'ITEM_REMOVE', 'ITEM_INACTIVE',
        'CAMPAIGN_CREATED', 'BUDGET_CHANGE',
        'OZON_BID_CHANGE', 'OZON_STATUS_CHANGE', 'OZON_BUDGET_CHANGE',
        'OZON_ITEM_ADD', 'OZON_ITEM_REMOVE', 'OZON_CAMPAIGN_CREATED',
    ]
    query = """
        (
            SELECT id, created_at, event_type, nm_id::text, old_value, new_value
            FROM event_log
            WHERE advert_id = :advert_id
              AND shop_id = :shop_id
              AND event_type = ANY(:campaign_types)
              AND NOT (event_type = ANY(:excluded))
        )
        UNION ALL
        (
            SELECT id, created_at, event_type, nm_id::text, old_value, new_value
            FROM event_log
            WHERE nm_id::text = ANY(:skus)
              AND shop_id = :shop_id
              AND NOT (event_type = ANY(:campaign_types))
              AND NOT (event_type = ANY(:excluded))
        )
        ORDER BY created_at DESC
        LIMIT :limit
    """
    
    result = await db.execute(text(query), {
        "skus": nm_ids_str, "limit": limit, "shop_id": shop_id,
        "excluded": excluded_events, "advert_id": campaign_id,
        "campaign_types": campaign_event_types,
    })
    events = result.fetchall()
    
    # Enrich with product names from PostgreSQL
    nm_ids = list(set(int(e[3]) for e in events if e[3]))
    product_map = {}
    if nm_ids:
        if marketplace.lower() == "ozon":
            pg_q = text("SELECT product_id, name, offer_id FROM dim_ozon_products WHERE shop_id = :sid AND product_id = ANY(:ids)")
            pg_res = await db.execute(pg_q, {"sid": shop_id, "ids": nm_ids})
            product_map = {int(r[0]): {"name": r[1] or "", "offer_id": r[2] or ""} for r in pg_res}
        else:
            pg_q = text("SELECT nm_id, name, vendor_code FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)")
            pg_res = await db.execute(pg_q, {"sid": shop_id, "ids": nm_ids})
            product_map = {int(r[0]): {"name": r[1] or "", "offer_id": r[2] or ""} for r in pg_res}

    # WB campaign status codes → human-readable labels
    WB_STATUS_MAP = {
        "4": "Готова к запуску", "7": "Завершена", "8": "Отклонена",
        "9": "Пауза", "11": "Активна",
    }
    # Ozon campaign status labels
    OZON_STATUS_MAP = {
        "CAMPAIGN_STATE_RUNNING": "Активна",
        "CAMPAIGN_STATE_STOPPED": "Остановлена",
        "CAMPAIGN_STATE_INACTIVE": "Неактивна",
        "CAMPAIGN_STATE_ARCHIVED": "В архиве",
        "CAMPAIGN_STATE_MODERATION": "Модерация",
    }

    result = []
    for e in events:
        nm_id = int(e[3]) if e[3] else None
        prod_info = product_map.get(nm_id, {}) if nm_id else {}
        old_val = e[4]
        new_val = e[5]
        
        # Translate status codes to readable labels
        if e[2] in ('STATUS_CHANGE',):
            old_val = WB_STATUS_MAP.get(str(old_val), old_val)
            new_val = WB_STATUS_MAP.get(str(new_val), new_val)
        elif e[2] in ('OZON_STATUS_CHANGE',):
            old_val = OZON_STATUS_MAP.get(str(old_val), old_val)
            new_val = OZON_STATUS_MAP.get(str(new_val), new_val)
        
        result.append(CampaignEventRow(
            id=e[0],
            timestamp=e[1].isoformat() if hasattr(e[1], 'isoformat') else str(e[1]),
            event_type=e[2],
            product_id=e[3],
            product_name=prod_info.get("name") or None,
            offer_id=prod_info.get("offer_id") or None,
            old_value=old_val,
            new_value=new_val,
        ))
    return result

@router.get("/{marketplace}/{campaign_id}/phrases", response_model=List[CampaignPhraseRow])
async def get_campaign_phrases(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    current_user: User = Depends(get_current_user),
):
    """
    Get aggregated search phrases statistics for a campaign within date range.
    Uses the new fact_advert_phrases_daily ClickHouse table.
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    mk_enum = 2 if marketplace.lower() == "ozon" else 1
    
    query = """
        SELECT
            phrase,
            sum(views) as t_views,
            sum(clicks) as t_clicks,
            if(sum(views)>0, round(sum(clicks)/sum(views)*100, 2), 0) as t_ctr,
            sum(spend) as t_spend,
            sum(orders) as t_orders,
            sum(revenue) as t_revenue
        FROM mms_analytics.fact_advert_phrases_daily FINAL
        WHERE marketplace = {mk:UInt8}
          AND campaign_id = {cid:UInt64}
          AND dt BETWEEN {sd:Date} AND {ed:Date}
        GROUP BY phrase
        ORDER BY t_spend DESC, t_views DESC
        LIMIT 500
    """
    params = {
        "mk": mk_enum,
        "cid": campaign_id,
        "sd": start_date,
        "ed": end_date
    }
    
    rows = ch.query(query, parameters=params).result_rows
    
    return [
        CampaignPhraseRow(
            phrase=r[0], views=int(r[1]), clicks=int(r[2]), ctr=float(r[3]),
            spend=float(r[4]), orders=int(r[5]), revenue=float(r[6])
        ) for r in rows
    ]

@router.get("/{marketplace}/{campaign_id}/heatmap", response_model=List[CampaignHeatmapRow])
async def get_campaign_heatmap(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    sku: Optional[int] = Query(None, description="Filter by specific SKU/nmId"),
    current_user: User = Depends(get_current_user),
):
    """
    Get order heatmap (Hour of Day vs Day of Week) for products in this campaign.
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    skus = []
    
    if sku:
        skus = [sku]
    else:
        if marketplace.lower() == "ozon":
            res = ch.query(
                "SELECT DISTINCT sku FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} AND dt BETWEEN {sd:Date} AND {ed:Date}",
                parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
            ).result_rows
        else:
            res = ch.query(
                "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND date BETWEEN {sd:Date} AND {ed:Date} AND (views > 0 OR clicks > 0 OR spend > 0)",
                parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
            ).result_rows
        skus = [int(r[0]) for r in res]
        
    if not skus:
        return []
        
    # Need shop_id for WB orders table
    if marketplace == "wb":
        shop_q = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
        wb_shop_id = int(shop_q[0][0]) if shop_q else 0
    
    if marketplace == "ozon":
        table = "mms_analytics.fact_ozon_orders FINAL"
        sku_col = "sku"
        date_col = "order_date"
        shop_filter = ""
    else:
        table = "mms_analytics.fact_orders_raw FINAL"
        sku_col = "nm_id"
        date_col = "date"
        shop_filter = f"AND shop_id = {{shop_id:UInt32}}" if marketplace == "wb" else ""
        
    # In ClickHouse array/tuple substitution is {skus:Array(UInt64)}
    if marketplace == "ozon":
        cancel_filter = "AND status NOT IN ('cancelled')"
    else:
        cancel_filter = "AND is_cancel = 0"
    
    query = f"""
        SELECT
            toDayOfWeek({date_col}) AS day_of_week,
            toHour({date_col}) AS hour,
            count() AS orders
        FROM {table}
        WHERE {sku_col} IN {{skus:Array(UInt64)}}
          AND toDate({date_col}) BETWEEN {{sd:Date}} AND {{ed:Date}}
          {cancel_filter}
          {shop_filter}
        GROUP BY day_of_week, hour
        ORDER BY day_of_week, hour
    """
    
    params = {
        "skus": skus,
        "sd": start_date,
        "ed": end_date,
    }
    if marketplace == "wb" and wb_shop_id:
        params["shop_id"] = wb_shop_id
    
    rows = ch.query(query, parameters=params).result_rows
    
    return [
        CampaignHeatmapRow(day_of_week=int(r[0]), hour=int(r[1]), orders=int(r[2]))
        for r in rows
    ]


class CampaignPurchaseRow(BaseModel):
    sku: int
    product_name: str
    offer_id: str
    quantity: int
    revenue: float
    avg_price: float

@router.get("/{marketplace}/{campaign_id}/purchases", response_model=List[CampaignPurchaseRow])
async def get_campaign_purchases(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get products purchased through this campaign (which SKUs are being bought).
    Uses orders data joined with campaign SKU list.
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    
    # Get SKUs from the campaign
    if marketplace.lower() == "ozon":
        sku_res = ch.query(
            "SELECT DISTINCT sku FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} AND dt BETWEEN {sd:Date} AND {ed:Date}",
            parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
        ).result_rows
    else:
        sku_res = ch.query(
            "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND date BETWEEN {sd:Date} AND {ed:Date} AND (views > 0 OR clicks > 0 OR spend > 0)",
            parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
        ).result_rows
    
    skus = [int(r[0]) for r in sku_res]
    if not skus:
        return []
    
    # Get actual orders for these SKUs
    if marketplace.lower() == "ozon":
        orders_query = """
            SELECT
                sku,
                any(product_name) as t_name,
                any(offer_id) as t_offer,
                sum(quantity) as t_qty,
                sum(price * quantity) as t_revenue,
                if(sum(quantity) > 0, round(sum(price * quantity) / sum(quantity), 2), 0) as t_avg_price
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND sku IN {skus:Array(UInt64)}
              AND toDate(in_process_at) BETWEEN {sd:Date} AND {ed:Date}
              AND status NOT IN ('cancelled')
            GROUP BY sku
            ORDER BY t_revenue DESC
        """
    else:
        orders_query = """
            SELECT
                nm_id as sku,
                '' as t_name,
                '' as t_offer,
                count() as t_qty,
                sum(finished_price) as t_revenue,
                if(count() > 0, round(sum(finished_price) / count(), 2), 0) as t_avg_price
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND nm_id IN {skus:Array(UInt64)}
              AND toDate(date) BETWEEN {sd:Date} AND {ed:Date}
              AND is_cancel = 0
            GROUP BY nm_id
            ORDER BY t_revenue DESC
        """
    
    # Get shop_id from campaign data
    if marketplace.lower() == "ozon":
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
    else:
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
        
    if not shop_row:
        return []
    
    shop_id = int(shop_row[0][0])
    
    rows = ch.query(orders_query, parameters={
        "shop_id": shop_id,
        "skus": skus,
        "sd": start_date,
        "ed": end_date,
    }).result_rows
    
    result = []
    for r in rows:
        result.append(CampaignPurchaseRow(
            sku=int(r[0]),
            product_name=str(r[1]) if r[1] else f"SKU {r[0]}",
            offer_id=str(r[2]) if r[2] else "",
            quantity=int(r[3]),
            revenue=float(r[4]),
            avg_price=float(r[5]),
        ))
    
    # Enrich WB names from PostgreSQL
    if marketplace.lower() != "ozon" and result:
        from sqlalchemy import text as sa_text
        sku_ids = [r.sku for r in result]
        pg_res = await db.execute(
            sa_text("SELECT nm_id, name, vendor_code FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
            {"sid": shop_id, "ids": sku_ids}
        )
        pg_map = {int(row[0]): {"name": row[1] or "", "offer_id": row[2] or ""} for row in pg_res}
        for r in result:
            info = pg_map.get(r.sku, {})
            if info.get("name"):
                r.product_name = info["name"]
            if info.get("offer_id"):
                r.offer_id = info["offer_id"]
    
    # Enrich Ozon names if empty
    if marketplace.lower() == "ozon" and result:
        empty_names = [r for r in result if not r.product_name or r.product_name.startswith("SKU")]
        if empty_names:
            from sqlalchemy import text as sa_text
            sku_ids = [r.sku for r in empty_names]
            pg_res = await db.execute(
                sa_text("SELECT product_id, name, offer_id FROM dim_ozon_products WHERE shop_id = :sid AND product_id = ANY(:ids)"),
                {"sid": shop_id, "ids": sku_ids}
            )
            pg_map = {int(row[0]): {"name": row[1] or "", "offer_id": row[2] or ""} for row in pg_res}
            for r in empty_names:
                info = pg_map.get(r.sku, {})
                if info.get("name"):
                    r.product_name = info["name"]
                if info.get("offer_id"):
                    r.offer_id = info["offer_id"]
    
    return result
