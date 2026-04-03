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


def _build_wb_scope_filter(scope: str) -> str:
    """Build SQL filter for WB scope: main/cross/all.
    all/main = directly advertised SKUs (have views/clicks/spend)
              'all' behaves same as 'main' for KPI — shows only direct products
    cross    = associated conversions only (have orders but no views/clicks/spend)
    """
    if scope in ("main", "all"):
        return "AND (views > 0 OR clicks > 0 OR spend > 0)"
    elif scope == "cross":
        return "AND views = 0 AND clicks = 0 AND spend = 0"
    return "AND (views > 0 OR clicks > 0 OR spend > 0)"  # default = main

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
    direct_revenue: float = 0
    model_revenue: float = 0
    associated_revenue: float = 0

class CampaignEventRow(BaseModel):
    id: int
    timestamp: str
    event_type: str
    product_id: Optional[str]
    product_name: Optional[str] = None
    offer_id: Optional[str] = None
    old_value: Optional[str]
    new_value: Optional[str]
    event_metadata: Optional[dict] = None

class CampaignPhraseRow(BaseModel):
    phrase: str
    views: int
    clicks: int
    ctr: float
    spend: float
    orders: int
    revenue: float
    # WB normquery extras (optional — filled for WB UWB campaigns)
    atbs: int = 0          # add-to-basket (корзины)
    avg_pos: float = 0     # средняя позиция показа
    cpc: float = 0         # стоимость клика (руб)

class CampaignHeatmapRow(BaseModel):
    day_of_week: int
    hour: int
    orders: int

class KpiPeriod(BaseModel):
    spend: float = 0
    ad_revenue: float = 0
    product_revenue: float = 0  # total product sales (organic+ad) from fact_orders_raw
    orders: int = 0
    cart: int = 0
    clicks: int = 0
    views: int = 0
    ctr: float = 0
    drr_ad: float = 0
    drr_product: float = 0
    cpo: float = 0
    # Breakdown by sale type (ad-attributed from fact_advert_stats_v3)
    direct_revenue: float = 0
    direct_orders: int = 0
    model_revenue: float = 0
    model_orders: int = 0
    associated_revenue: float = 0
    associated_orders: int = 0

class CampaignKpiResponse(BaseModel):
    current: KpiPeriod
    previous: KpiPeriod
    first_date: Optional[str] = None  # earliest date with stats (campaign launch date)

# --- Helpers ---

async def _compute_sale_type_breakdown(
    ch, campaign_id: int, sd: date, ed: date, db
) -> dict:
    """
    Compute ad-attributed revenue/orders breakdown by sale type (direct/model/associated)
    using imt_id from dim_products for WB campaigns.
    """
    from sqlalchemy import text as sa_text
    
    # Get per-SKU stats from fact_advert_stats_v3
    rows = ch.query(
        "SELECT nm_id, SUM(orders), SUM(revenue), SUM(views), SUM(clicks), SUM(spend) "
        "FROM mms_analytics.fact_advert_stats_v3 FINAL "
        "WHERE advert_id = {cid:UInt64} AND date BETWEEN {sd:Date} AND {ed:Date} "
        "GROUP BY nm_id",
        parameters={"cid": campaign_id, "sd": sd, "ed": ed}
    ).result_rows
    
    if not rows:
        return {}
    
    # Identify main (direct) SKUs: those with views/clicks/spend > 0
    main_skus = set()
    all_nm_ids = []
    for r in rows:
        nm = int(r[0])
        all_nm_ids.append(nm)
        views, clicks, spend = int(r[3]), int(r[4]), float(r[5])
        if views > 0 or clicks > 0 or spend > 0:
            main_skus.add(nm)
    
    # Get imt_ids from PostgreSQL
    sku_imt = {}
    if all_nm_ids:
        # Get shop_id from first row in campaign
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
        if shop_row:
            shop_id = int(shop_row[0][0])
            pg_res = await db.execute(
                sa_text("SELECT nm_id, imt_id FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                {"sid": shop_id, "ids": all_nm_ids}
            )
            for row in pg_res:
                sku_imt[int(row[0])] = row[1]
    
    main_imt_ids = set()
    for ms in main_skus:
        imt = sku_imt.get(ms)
        if imt:
            main_imt_ids.add(imt)
    
    result = {
        'direct_revenue': 0.0, 'direct_orders': 0,
        'model_revenue': 0.0, 'model_orders': 0,
        'associated_revenue': 0.0, 'associated_orders': 0,
    }
    for r in rows:
        nm = int(r[0])
        orders = int(r[1])
        revenue = float(r[2])
        if nm in main_skus:
            result['direct_revenue'] += revenue
            result['direct_orders'] += orders
        else:
            imt = sku_imt.get(nm)
            if imt and imt in main_imt_ids:
                result['model_revenue'] += revenue
                result['model_orders'] += orders
            else:
                result['associated_revenue'] += revenue
                result['associated_orders'] += orders
    
    for k in result:
        if isinstance(result[k], float):
            result[k] = round(result[k], 2)
    return result

# --- Endpoints ---

@router.get("/{marketplace}/{campaign_id}/kpi", response_model=CampaignKpiResponse)
async def get_campaign_kpi(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(...),
    end_date: date = Query(...),
    sku: Optional[int] = Query(None),
    scope: str = Query("all", description="main=advertised SKUs, cross=associated conversions, all=everything"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
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
                    sum(money_spent),
                    sum(revenue) + sum(model_revenue),
                    sum(orders) + sum(model_orders),
                    sum(add_to_cart),
                    sum(clicks), sum(views)
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE campaign_id = {{campaign_id:UInt64}}
                  AND dt BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f}
            """
        else:
            sku_f = "AND nm_id = {sku:UInt64}" if sku else ""
            if sku: params["sku"] = sku
            scope_f = _build_wb_scope_filter(scope) if not sku else ""
            q = f"""
                SELECT
                    sum(spend), sum(revenue), sum(orders), sum(atbs),
                    sum(clicks), sum(views)
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE advert_id = {{campaign_id:UInt64}}
                  AND date BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f} {scope_f}
            """
        return q, params
    
    def build_product_rev_query(mp: str, skus: list, shop_id: int, sd: date, ed: date) -> tuple:
        """Total product revenue from orders tables (organic + ad, ALL sales of these SKUs)."""
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
            # WB: total product revenue from fact_orders_raw (organic + ad sales)
            # Uses price_with_disc (order price) for ALL incoming orders,
            # NOT finished_price with is_cancel=0 (only fulfilled).
            # This matches WB ad attribution which counts all placed orders.
            q = """
                SELECT sum(price_with_disc) 
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nm_id IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
            """
            return q, {"shop_id": shop_id, "skus": skus, "start_date": sd, "end_date": ed}
    
    # Get campaign SKUs + shop_id
    if marketplace.lower() == "ozon":
        skus_r = ch.query(
            "SELECT DISTINCT sku, shop_id FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64}",
            parameters={"cid": campaign_id}
        ).result_rows
    else:
        wb_scope_f = _build_wb_scope_filter(scope)
        skus_r = ch.query(
            f"SELECT DISTINCT nm_id, shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {{cid:UInt64}} {wb_scope_f}",
            parameters={"cid": campaign_id}
        ).result_rows
    
    campaign_skus = [int(r[0]) for r in skus_r]
    shop_id = int(skus_r[0][1]) if skus_r else 0
    if sku:
        campaign_skus = [sku]
    
    def make_kpi(ad_row, prod_rev: float, breakdown: dict = None) -> KpiPeriod:
        spend = float(ad_row[0] or 0)
        ad_rev = float(ad_row[1] or 0)
        orders = int(ad_row[2] or 0)
        cart = int(ad_row[3] or 0)
        clicks = int(ad_row[4] or 0)
        views = int(ad_row[5] or 0)
        effective_prod_rev = prod_rev if prod_rev > 0 else ad_rev
        ctr = (clicks / views * 100) if views > 0 else 0
        drr_ad = (spend / ad_rev * 100) if ad_rev > 0 else 0
        drr_prod = (spend / effective_prod_rev * 100) if effective_prod_rev > 0 else 0
        cpo = (spend / orders) if orders > 0 else 0
        bd = breakdown or {}
        return KpiPeriod(
            spend=round(spend, 2), ad_revenue=round(ad_rev, 2),
            product_revenue=round(effective_prod_rev, 2),
            orders=orders, cart=cart, clicks=clicks, views=views,
            ctr=round(ctr, 2), drr_ad=round(drr_ad, 2),
            drr_product=round(drr_prod, 2), cpo=round(cpo, 2),
            direct_revenue=bd.get('direct_revenue', 0),
            direct_orders=bd.get('direct_orders', 0),
            model_revenue=bd.get('model_revenue', 0),
            model_orders=bd.get('model_orders', 0),
            associated_revenue=bd.get('associated_revenue', 0),
            associated_orders=bd.get('associated_orders', 0),
        )
    
    # Current period
    q_cur, p_cur = build_ad_query(marketplace.lower(), start_date, end_date)
    ad_cur = ch.query(q_cur, parameters=p_cur).result_rows
    ad_cur_row = ad_cur[0] if ad_cur else (0, 0, 0, 0, 0, 0)
    
    # Previous period
    q_prev, p_prev = build_ad_query(marketplace.lower(), prev_start, prev_end)
    ad_prev = ch.query(q_prev, parameters=p_prev).result_rows
    ad_prev_row = ad_prev[0] if ad_prev else (0, 0, 0, 0, 0, 0)
    
    # Product revenue — total product sales (organic + ad) from fact_orders_raw
    # For scope=all/main: uses DIRECT SKUs only (views>0/clicks>0/spend>0)
    # For scope=cross: uses cross-sell SKUs (model + associated)
    prod_rev_cur = 0.0
    prod_rev_prev = 0.0
    
    if marketplace.lower() != "ozon" and shop_id:
        if scope == "cross":
            # For cross scope: use campaign_skus which are already cross-filtered
            skus_for_prod_rev = campaign_skus
        else:
            # For all/main: get DIRECT SKUs
            main_skus_r = ch.query(
                "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL "
                "WHERE advert_id = {cid:UInt64} AND (views > 0 OR clicks > 0 OR spend > 0)",
                parameters={"cid": campaign_id}
            ).result_rows
            skus_for_prod_rev = [int(r[0]) for r in main_skus_r]
        if sku:
            skus_for_prod_rev = [sku]  # single SKU filter overrides
        
        if skus_for_prod_rev:
            pq_cur, pp_cur = build_product_rev_query(marketplace.lower(), skus_for_prod_rev, shop_id, start_date, end_date)
            if pq_cur:
                pr = ch.query(pq_cur, parameters=pp_cur).result_rows
                prod_rev_cur = float(pr[0][0] or 0) if pr else 0
            
            pq_prev, pp_prev = build_product_rev_query(marketplace.lower(), skus_for_prod_rev, shop_id, prev_start, prev_end)
            if pq_prev:
                pr = ch.query(pq_prev, parameters=pp_prev).result_rows
                prod_rev_prev = float(pr[0][0] or 0) if pr else 0
    elif marketplace.lower() == "ozon" and campaign_skus and shop_id:
        pq_cur, pp_cur = build_product_rev_query(marketplace.lower(), campaign_skus, shop_id, start_date, end_date)
        if pq_cur:
            pr = ch.query(pq_cur, parameters=pp_cur).result_rows
            prod_rev_cur = float(pr[0][0] or 0) if pr else 0
        pq_prev, pp_prev = build_product_rev_query(marketplace.lower(), campaign_skus, shop_id, prev_start, prev_end)
        if pq_prev:
            pr = ch.query(pq_prev, parameters=pp_prev).result_rows
            prod_rev_prev = float(pr[0][0] or 0) if pr else 0
    
    # Breakdown by sale type (direct/model/associated) — from fact_advert_stats_v3
    breakdown_cur = {}
    breakdown_prev = {}
    if marketplace.lower() != "ozon" and not sku:
        breakdown_cur = await _compute_sale_type_breakdown(
            ch, campaign_id, start_date, end_date, db
        )
        breakdown_prev = await _compute_sale_type_breakdown(
            ch, campaign_id, prev_start, prev_end, db
        )
    
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
        current=make_kpi(ad_cur_row, prod_rev_cur, breakdown_cur),
        previous=make_kpi(ad_prev_row, prod_rev_prev, breakdown_prev),
        first_date=first_date_val,
    )

@router.get("/{marketplace}/{campaign_id}/stats", response_model=List[CampaignStatsRow])
async def get_campaign_stats(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    sku: Optional[int] = Query(None, description="Filter by specific SKU/nmId inside campaign"),
    scope: str = Query("all", description="main=advertised SKUs, cross=associated conversions, all=everything"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
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
                sum(orders) + sum(model_orders) as t_orders,
                sum(add_to_cart) as t_cart,
                sum(revenue) + sum(model_revenue) as t_revenue,
                sum(money_spent) as t_spend,
                if(sum(views)>0, round(sum(clicks)/sum(views)*100, 2), 0) as t_ctr,
                if((sum(revenue)+sum(model_revenue))>0, round(sum(money_spent)/(sum(revenue)+sum(model_revenue))*100, 2), 0) as t_drr
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
        scope_f = _build_wb_scope_filter(scope) if not sku else ""
            
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
              {sku_filter} {scope_f}
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
    
    # For WB: get daily total product revenue from fact_orders_raw (organic + ad)
    # This is DIFFERENT from "revenue" in the chart (which = ad-attributed only)
    if marketplace == "wb":
        skus_for_rev = []
        shop_id_for_rev = 0
        if sku:
            skus_for_rev = [sku]
        else:
            wb_scope_f = _build_wb_scope_filter(scope)
            skus_q = ch.query(
                f"SELECT DISTINCT nm_id, shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {{cid:UInt64}} {wb_scope_f}",
                parameters={"cid": campaign_id}
            ).result_rows
            skus_for_rev = [int(r[0]) for r in skus_q]
            if skus_q:
                shop_id_for_rev = int(skus_q[0][1])
        if not shop_id_for_rev:
            sh = ch.query(
                "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} LIMIT 1",
                parameters={"cid": campaign_id}
            ).result_rows
            shop_id_for_rev = int(sh[0][0]) if sh else 0
        
        if skus_for_rev and shop_id_for_rev:
            pr_rows = ch.query(
                """
                SELECT toDate(date) as d, sum(price_with_disc)
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nm_id IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY d
                """,
                parameters={"shop_id": shop_id_for_rev, "skus": skus_for_rev, "start_date": start_date, "end_date": end_date}
            ).result_rows
            prod_rev_by_date = {_to_date(pr[0]): float(pr[1] or 0) for pr in pr_rows}
            for row in result:
                row.product_revenue = round(prod_rev_by_date.get(row.dt, 0), 2)
        
        # --- Daily breakdown: direct/model/associated from fact_advert_stats_v3 ---
        if not sku:
            from sqlalchemy import text as sa_text
            # Get per-day per-nm_id revenue
            bd_rows = ch.query(
                "SELECT toDate(date) as d, nm_id, SUM(orders) as o, SUM(revenue) as r, "
                "SUM(views) as v, SUM(clicks) as c, SUM(spend) as s "
                "FROM mms_analytics.fact_advert_stats_v3 FINAL "
                "WHERE advert_id = {cid:UInt64} AND date BETWEEN {sd:Date} AND {ed:Date} "
                "GROUP BY d, nm_id",
                parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
            ).result_rows
            if bd_rows:
                # Identify direct SKUs
                direct_skus = set()
                all_nms = set()
                for r in bd_rows:
                    nm = int(r[1])
                    all_nms.add(nm)
                    if int(r[4]) > 0 or int(r[5]) > 0 or float(r[6]) > 0:
                        direct_skus.add(nm)
                
                # Get imt_ids from PostgreSQL for model classification
                sku_imt = {}
                if all_nms and shop_id_for_rev:
                    pg_res = await db.execute(
                        sa_text("SELECT nm_id, imt_id FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                        {"sid": shop_id_for_rev, "ids": list(all_nms)}
                    )
                    for pgr in pg_res:
                        sku_imt[int(pgr[0])] = pgr[1]
                
                main_imt_ids = {sku_imt[s] for s in direct_skus if s in sku_imt and sku_imt[s]}
                
                # Aggregate per day
                daily_bd: dict = {}  # date -> {direct, model, assoc}
                for r in bd_rows:
                    d = _to_date(r[0])
                    nm = int(r[1])
                    rev = float(r[3])
                    if d not in daily_bd:
                        daily_bd[d] = {'d': 0.0, 'm': 0.0, 'a': 0.0}
                    if nm in direct_skus:
                        daily_bd[d]['d'] += rev
                    else:
                        imt = sku_imt.get(nm)
                        if imt and imt in main_imt_ids:
                            daily_bd[d]['m'] += rev
                        else:
                            daily_bd[d]['a'] += rev
                
                for row in result:
                    bd = daily_bd.get(row.dt)
                    if bd:
                        row.direct_revenue = round(bd['d'], 2)
                        row.model_revenue = round(bd['m'], 2)
                        row.associated_revenue = round(bd['a'], 2)
    
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
    
    # Exclude noisy events from popup:
    # - Individual warehouse stock events (keep only FBO/FBS total stockouts)
    # - STATUS_CHANGE (constant pause/unpause cycles are noise, user sees status directly)
    excluded_events = [
        'STOCK_OUT', 'STOCK_REPLENISH', 'OZON_STOCK_OUT', 'OZON_STOCK_REPLENISH',
        'STATUS_CHANGE', 'OZON_STATUS_CHANGE',
    ]
    
    # Campaign-level events (BID_CHANGE, STATUS_CHANGE, etc.) must be filtered by advert_id
    # to avoid showing events from OTHER campaigns that advertise the same product.
    # Product-level events (STOCK, PRICE, CONTENT) are filtered by nm_id only.
    campaign_event_types = [
        'BID_CHANGE', 'ITEM_ADD', 'ITEM_REMOVE', 'ITEM_INACTIVE',
        'CAMPAIGN_CREATED', 'BUDGET_CHANGE',
        'OZON_BID_CHANGE', 'OZON_BUDGET_CHANGE',
        'OZON_ITEM_ADD', 'OZON_ITEM_REMOVE', 'OZON_CAMPAIGN_CREATED',
    ]
    query = """
        (
            SELECT id, created_at, event_type, nm_id::text, old_value, new_value, event_metadata
            FROM event_log
            WHERE advert_id = :advert_id
              AND shop_id = :shop_id
              AND event_type = ANY(:campaign_types)
              AND NOT (event_type = ANY(:excluded))
        )
        UNION ALL
        (
            SELECT id, created_at, event_type, nm_id::text, old_value, new_value, event_metadata
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
            pg_q = text("SELECT product_id, name, offer_id, main_image_url FROM dim_ozon_products WHERE shop_id = :sid AND product_id = ANY(:ids)")
            pg_res = await db.execute(pg_q, {"sid": shop_id, "ids": nm_ids})
            product_map = {int(r[0]): {"name": r[1] or "", "offer_id": r[2] or "", "main_image_url": r[3] or ""} for r in pg_res}
        else:
            pg_q = text("SELECT nm_id, name, vendor_code, main_image_url FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)")
            pg_res = await db.execute(pg_q, {"sid": shop_id, "ids": nm_ids})
            product_map = {int(r[0]): {"name": r[1] or "", "offer_id": r[2] or "", "main_image_url": r[3] or ""} for r in pg_res}

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
        metadata = e[6] if len(e) > 6 else None
        
        # Translate status codes to readable labels
        if e[2] in ('STATUS_CHANGE',):
            old_val = WB_STATUS_MAP.get(str(old_val), old_val)
            new_val = WB_STATUS_MAP.get(str(new_val), new_val)
        elif e[2] in ('OZON_STATUS_CHANGE',):
            old_val = OZON_STATUS_MAP.get(str(old_val), old_val)
            new_val = OZON_STATUS_MAP.get(str(new_val), new_val)
        
        # For photo events without image URL in metadata,
        # try to get current image from product_map (dim_products)
        if e[2] in ('CONTENT_MAIN_PHOTO_CHANGED', 'CONTENT_PHOTO_ORDER_CHANGED',
                     'CONTENT_PHOTO_ADDED', 'CONTENT_PHOTO_REMOVED') and metadata:
            if not metadata.get('main_image_url'):
                img_url = prod_info.get('main_image_url', '')
                if img_url:
                    metadata['main_image_url'] = img_url
        
        result.append(CampaignEventRow(
            id=e[0],
            timestamp=e[1].isoformat() if hasattr(e[1], 'isoformat') else str(e[1]),
            event_type=e[2],
            product_id=e[3],
            product_name=prod_info.get("name") or None,
            offer_id=prod_info.get("offer_id") or None,
            old_value=old_val,
            new_value=new_val,
            event_metadata=metadata,
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
    For Ozon: uses fact_advert_phrases_daily.
    For WB: tries fact_advert_phrases_daily first, then falls back to
            fact_normquery_stats_daily (normquery clusters from UWB sync).
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
    
    # WB fallback: if no data in fact_advert_phrases_daily, try normquery clusters
    if not rows and marketplace.lower() == "wb":
        # Determine shop_id from campaign data
        shop_row = ch.query(
            "SELECT DISTINCT shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL "
            "WHERE advert_id = {cid:UInt64} LIMIT 1",
            parameters={"cid": campaign_id}
        ).result_rows
        
        if shop_row:
            shop_id = int(shop_row[0][0])
            nq_query = """
                SELECT
                    norm_query,
                    sum(views) as t_views,
                    sum(clicks) as t_clicks,
                    if(sum(views)>0, round(sum(clicks)/sum(views)*100, 2), 0) as t_ctr,
                    round(sum(cpc * clicks) / 100, 2) as t_spend,
                    sum(orders) as t_orders,
                    0 as t_revenue,
                    sum(atbs) as t_atbs,
                    if(sum(views)>0, round(sum(avg_pos * views) / sum(views), 1), 0) as t_avg_pos,
                    if(sum(clicks)>0, round(sum(cpc * clicks) / sum(clicks) / 100, 2), 0) as t_cpc
                FROM mms_analytics.fact_normquery_stats_daily
                WHERE shop_id = {sid:UInt32}
                  AND advert_id = {cid:UInt64}
                  AND dt BETWEEN {sd:Date} AND {ed:Date}
                GROUP BY norm_query
                ORDER BY t_views DESC
                LIMIT 500
            """
            nq_rows = ch.query(nq_query, parameters={
                "sid": shop_id, "cid": campaign_id,
                "sd": start_date, "ed": end_date,
            }).result_rows
            
            return [
                CampaignPhraseRow(
                    phrase=r[0], views=int(r[1]), clicks=int(r[2]), ctr=float(r[3]),
                    spend=float(r[4]), orders=int(r[5]), revenue=float(r[6]),
                    atbs=int(r[7]), avg_pos=float(r[8]), cpc=float(r[9]),
                ) for r in nq_rows
            ]
    
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
    sale_type: str = "direct"  # direct / model / associated

@router.get("/{marketplace}/{campaign_id}/purchases", response_model=List[CampaignPurchaseRow])
async def get_campaign_purchases(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    scope: str = Query("all", description="main=advertised SKUs, cross=associated conversions, all=everything"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get products purchased through this campaign (which SKUs are being bought).
    Uses orders data joined with campaign SKU list.
    sale_type: direct = directly advertised, model = same unified card, associated = different card
    """
    marketplace = _normalize_mp(marketplace)
    ch = get_clickhouse_client()
    
    # Get SKUs from the campaign based on scope
    if marketplace.lower() == "ozon":
        sku_res = ch.query(
            "SELECT DISTINCT sku FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64} AND dt BETWEEN {sd:Date} AND {ed:Date}",
            parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
        ).result_rows
    else:
        wb_scope_f = _build_wb_scope_filter(scope)
        sku_res = ch.query(
            f"SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {{cid:UInt64}} AND date BETWEEN {{sd:Date}} AND {{ed:Date}} {wb_scope_f}",
            parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
        ).result_rows
    
    skus = [int(r[0]) for r in sku_res]
    if not skus:
        return []
    
    # Get main SKUs (directly advertised) for sale_type classification
    main_skus_set = set()
    if marketplace.lower() != "ozon":
        main_res = ch.query(
            "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64} AND date BETWEEN {sd:Date} AND {ed:Date} AND (views > 0 OR clicks > 0 OR spend > 0)",
            parameters={"cid": campaign_id, "sd": start_date, "ed": end_date}
        ).result_rows
        main_skus_set = {int(r[0]) for r in main_res}
    
    # Get actual orders for these SKUs
    # For WB: use fact_advert_stats_v3 which has CAMPAIGN-ATTRIBUTED orders (not all orders!)
    # fact_orders_raw has ALL orders including organic — would inflate numbers ~4x
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
                SUM(orders) as t_qty,
                SUM(revenue) as t_revenue,
                if(SUM(orders) > 0, round(SUM(revenue) / SUM(orders), 2), 0) as t_avg_price
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE advert_id = {cid:UInt64}
              AND date BETWEEN {sd:Date} AND {ed:Date}
              AND (orders > 0 OR revenue > 0)
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
    
    query_params = {
        "sd": start_date,
        "ed": end_date,
    }
    if marketplace.lower() == "ozon":
        query_params["shop_id"] = shop_id
        query_params["skus"] = skus
    else:
        query_params["cid"] = campaign_id
    
    rows = ch.query(orders_query, parameters=query_params).result_rows
    
    # --- Determine sale_type using imt_id ---
    # Get imt_ids for main SKUs to identify model sales (same unified card)
    main_imt_ids = set()
    sku_imt_map = {}
    if marketplace.lower() != "ozon" and main_skus_set:
        from sqlalchemy import text as sa_text
        all_sku_ids = [int(r[0]) for r in rows]
        if all_sku_ids:
            pg_res = await db.execute(
                sa_text("SELECT nm_id, imt_id, name, vendor_code FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                {"sid": shop_id, "ids": list(main_skus_set | set(all_sku_ids))}
            )
            for row in pg_res:
                sku_imt_map[int(row[0])] = {"imt_id": row[1], "name": row[2] or "", "vendor_code": row[3] or ""}
            
            # Collect imt_ids of main (directly advertised) SKUs
            for ms in main_skus_set:
                imt = sku_imt_map.get(ms, {}).get("imt_id")
                if imt:
                    main_imt_ids.add(imt)
    
    def classify_sale(sku_val: int) -> str:
        """Classify sale type: direct / model / associated."""
        if sku_val in main_skus_set:
            return "direct"
        if not main_skus_set:
            return "direct"  # No main SKUs info → assume direct
        # Check if same unified card (model sale) using imt_id
        sku_info = sku_imt_map.get(sku_val, {})
        sku_imt = sku_info.get("imt_id")
        if sku_imt and sku_imt in main_imt_ids:
            return "model"
        # No imt_id data → fall back to "associated" (conservative)
        return "associated"
    
    result = []
    for r in rows:
        sku_val = int(r[0])
        result.append(CampaignPurchaseRow(
            sku=sku_val,
            product_name=str(r[1]) if r[1] else f"SKU {r[0]}",
            offer_id=str(r[2]) if r[2] else "",
            quantity=int(r[3]),
            revenue=float(r[4]),
            avg_price=float(r[5]),
            sale_type=classify_sale(sku_val),
        ))
    
    # Enrich WB names from PostgreSQL (use data already fetched via sku_imt_map)
    if marketplace.lower() != "ozon" and result:
        if not sku_imt_map:
            from sqlalchemy import text as sa_text
            sku_ids = [r.sku for r in result]
            pg_res = await db.execute(
                sa_text("SELECT nm_id, name, vendor_code FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                {"sid": shop_id, "ids": sku_ids}
            )
            sku_imt_map = {int(row[0]): {"name": row[1] or "", "vendor_code": row[2] or ""} for row in pg_res}
        
        for r in result:
            info = sku_imt_map.get(r.sku, {})
            if info.get("name"):
                r.product_name = info["name"]
            if info.get("vendor_code"):
                r.offer_id = info["vendor_code"]
    
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


# ══════════════════════════════════════════════════════════════
# Normquery (Search Cluster) Analytics — UWB campaigns only
# ══════════════════════════════════════════════════════════════

class NormqueryClusterStat(BaseModel):
    norm_query: str
    views: int = 0
    clicks: int = 0
    atbs: int = 0
    orders: int = 0
    avg_pos: float = 0
    cpc_kopecks: int = 0
    cpm_kopecks: int = 0
    ctr: float = 0
    # Bid info
    current_bid_kopecks: int = 0
    current_bid_rub: float = 0
    # Recommended bids (kopecks)
    reach_max_bid: int = 0
    reach_med_bid: int = 0
    reach_min_bid: int = 0
    # Computed
    cr_click_to_order: float = 0
    cr_click_to_cart: float = 0
    cpc_rub: float = 0

class NormqueryAnalyticsResponse(BaseModel):
    clusters: list[NormqueryClusterStat] = []
    excluded_clusters: list[str] = []
    minus_phrases: list[str] = []
    base_bids: dict = {}
    total_clusters: int = 0


@router.get("/wb/{campaign_id}/normquery-analytics", response_model=NormqueryAnalyticsResponse)
async def get_normquery_analytics(
    campaign_id: int,
    shop_id: int = Query(..., description="Shop ID"),
    start_date: date = Query(..., description="Start Date"),
    end_date: date = Query(..., description="End Date"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get search cluster (normquery) analytics for a UWB campaign.

    Returns per-cluster performance + current bids + recommendations.
    Only for WB campaigns with bid_type=manual & payment_type=cpm.

    Combines:
    1. Live normquery stats from WB API (/adv/v0/normquery/stats)
    2. Current cluster bids from WB API (/adv/v0/normquery/get-bids)
    3. Minus phrases from WB API (/adv/v0/normquery/get-minus)
    4. Active/excluded clusters from WB API (/adv/v0/normquery/list)
    5. Bid recommendations from WB API (/api/advert/v0/bids/recommendations)
    """
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop
    from app.services.wb_normquery_service import WBNormqueryService

    # Verify shop access
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()
    if not shop or shop.marketplace != "wildberries":
        raise HTTPException(status_code=404, detail="WB магазин не найден")

    try:
        api_key = decrypt_api_key(shop.api_key_encrypted)
    except Exception:
        raise HTTPException(status_code=500, detail="Ошибка расшифровки API-ключа")

    # Get nm_ids for this campaign from ClickHouse
    ch = get_clickhouse_client()
    nm_rows = ch.query(
        "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL "
        "WHERE advert_id = {cid:UInt64} AND (views > 0 OR clicks > 0 OR spend > 0)",
        parameters={"cid": campaign_id}
    ).result_rows

    if not nm_rows:
        return NormqueryAnalyticsResponse()

    nm_ids = [int(r[0]) for r in nm_rows]
    items = [{"advert_id": campaign_id, "nm_id": nm} for nm in nm_ids]

    svc = WBNormqueryService(db=db, shop_id=shop.id, api_key=api_key)

    # Fetch data from WB API (parallel where possible)
    import asyncio
    stats_data, bids_data, minus_data, list_data = await asyncio.gather(
        svc.get_normquery_stats(
            items=items,
            date_from=start_date.isoformat(),
            date_to=end_date.isoformat(),
        ),
        svc.get_normquery_bids(items=items),
        svc.get_normquery_minus(items=items),
        svc.get_normquery_list(items=items),
        return_exceptions=True,
    )

    # Handle exceptions gracefully
    if isinstance(stats_data, Exception):
        stats_data = {}
    if isinstance(bids_data, Exception):
        bids_data = {}
    if isinstance(minus_data, Exception):
        minus_data = {}
    if isinstance(list_data, Exception):
        list_data = {}

    # Build bid map: norm_query → bid_kopecks
    bid_map = {}
    if isinstance(bids_data, dict):
        for bid in bids_data.get("bids", []):
            nq = bid.get("norm_query", "")
            bid_map[nq] = int(bid.get("bid", 0))

    # Build recommendations map (per first nm_id only for now)
    rec_map = {}
    base_bids = {}
    if nm_ids:
        try:
            rec_data = await svc.get_bid_recommendations(campaign_id, nm_ids[0])
            if isinstance(rec_data, dict):
                base = rec_data.get("base", {})
                base_bids = {
                    "competitive_kopecks": base.get("competitiveBid", {}).get("bidKopecks", 0),
                    "leaders_kopecks": base.get("leadersBid", {}).get("bidKopecks", 0),
                    "competitive_rub": round(base.get("competitiveBid", {}).get("bidKopecks", 0) / 100, 2),
                    "leaders_rub": round(base.get("leadersBid", {}).get("bidKopecks", 0) / 100, 2),
                }
                for nq_rec in rec_data.get("normQueries", []):
                    nq_name = nq_rec.get("normQuery", "")
                    rec_map[nq_name] = {
                        "reach_max": nq_rec.get("reachMax", {}).get("bidKopecks", 0),
                        "reach_med": nq_rec.get("reachMedium", {}).get("bidKopecks", 0),
                        "reach_min": nq_rec.get("reachMin", {}).get("bidKopecks", 0),
                    }
        except Exception:
            pass

    # Parse clusters from stats
    clusters = []
    if isinstance(stats_data, dict):
        for stat_item in stats_data.get("stats", []):
            for s in stat_item.get("stats", []):
                nq = s.get("norm_query", "")
                views = int(s.get("views", 0))
                clicks = int(s.get("clicks", 0))
                orders = int(s.get("orders", 0))
                atbs = int(s.get("atbs", 0))
                bid_k = bid_map.get(nq, 0)
                rec = rec_map.get(nq, {})

                clusters.append(NormqueryClusterStat(
                    norm_query=nq,
                    views=views,
                    clicks=clicks,
                    atbs=atbs,
                    orders=orders,
                    avg_pos=float(s.get("avg_pos", 0)),
                    cpc_kopecks=int(s.get("cpc", 0)),
                    cpm_kopecks=int(s.get("cpm", 0)),
                    ctr=float(s.get("ctr", 0)),
                    current_bid_kopecks=bid_k,
                    current_bid_rub=round(bid_k / 100, 2) if bid_k else 0,
                    reach_max_bid=rec.get("reach_max", 0),
                    reach_med_bid=rec.get("reach_med", 0),
                    reach_min_bid=rec.get("reach_min", 0),
                    cr_click_to_order=round(orders / clicks * 100, 2) if clicks > 0 else 0,
                    cr_click_to_cart=round(atbs / clicks * 100, 2) if clicks > 0 else 0,
                    cpc_rub=round(int(s.get("cpc", 0)) / 100, 2),
                ))

    # Sort by views desc (most important clusters first)
    clusters.sort(key=lambda c: c.views, reverse=True)

    # Parse excluded clusters + minus phrases
    excluded = []
    if isinstance(list_data, dict):
        for item in list_data.get("items", []):
            nq = item.get("normQueries", {})
            excluded.extend(nq.get("excluded", []) or [])

    minus = []
    if isinstance(minus_data, dict):
        for item in minus_data.get("items", []):
            minus.extend(item.get("norm_queries", []) or [])

    return NormqueryAnalyticsResponse(
        clusters=clusters,
        excluded_clusters=list(set(excluded)),
        minus_phrases=list(set(minus)),
        base_bids=base_bids,
        total_clusters=len(clusters),
    )

