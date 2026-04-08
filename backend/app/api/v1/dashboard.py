"""
Dashboard API endpoints.

GET /dashboard/ozon?shop_id=X&period=7d  — Aggregated Ozon dashboard data
GET /dashboard/wb?shop_id=X&period=7d    — Aggregated Wildberries dashboard data
"""
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)


def _wb_basket_host(vol: int) -> str:
    """Determine WB CDN basket host number from vol. Based on known WB CDN mapping."""
    if vol <= 143:
        return "01"
    elif vol <= 287:
        return "02"
    elif vol <= 431:
        return "03"
    elif vol <= 719:
        return "04"
    elif vol <= 1007:
        return "05"
    elif vol <= 1061:
        return "06"
    elif vol <= 1115:
        return "07"
    elif vol <= 1169:
        return "08"
    elif vol <= 1313:
        return "09"
    elif vol <= 1601:
        return "10"
    elif vol <= 1655:
        return "11"
    elif vol <= 1919:
        return "12"
    elif vol <= 2045:
        return "13"
    elif vol <= 2189:
        return "14"
    elif vol <= 2405:
        return "15"
    elif vol <= 2621:
        return "16"
    elif vol <= 2837:
        return "17"
    elif vol <= 3053:
        return "18"
    elif vol <= 3269:
        return "19"
    elif vol <= 3485:
        return "20"
    elif vol <= 3701:
        return "21"
    elif vol <= 3917:
        return "22"
    elif vol <= 4133:
        return "23"
    elif vol <= 4349:
        return "24"
    elif vol <= 4565:
        return "25"
    elif vol <= 4781:
        return "26"
    elif vol <= 4997:
        return "27"
    elif vol <= 5213:
        return "28"
    elif vol <= 5429:
        return "29"
    elif vol <= 5645:
        return "30"
    elif vol <= 5861:
        return "31"
    elif vol <= 6077:
        return "32"
    elif vol <= 6293:
        return "33"
    elif vol <= 6509:
        return "34"
    elif vol <= 6725:
        return "35"
    elif vol <= 6941:
        return "36"
    elif vol <= 7157:
        return "37"
    else:
        return "38"


def wb_image_url(nm_id: int) -> str:
    """Generate correct WB CDN image URL from nm_id using basket algorithm."""
    vol = nm_id // 100000
    part = nm_id // 1000
    basket = _wb_basket_host(vol)
    return f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/big/1.webp"

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])

PERIOD_DAYS = {
    "today": 1,
    "7d": 7,
    "30d": 30,
}


MSK = timezone(timedelta(hours=3))


def _parse_period(period: str) -> tuple[date, date, date, date]:
    """Return (current_start, current_end, prev_start, prev_end) dates in Moscow time."""
    days = PERIOD_DAYS.get(period, 7)
    today = datetime.now(MSK).date()
    if period == "today":
        current_start = today
        current_end = today
        prev_start = today - timedelta(days=1)
        prev_end = today - timedelta(days=1)
    else:
        current_end = today
        current_start = today - timedelta(days=days - 1)
        prev_end = current_start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
    return current_start, current_end, prev_start, prev_end


def _safe_delta(current: float, previous: float) -> float:
    """Calculate percentage change, safe for zero division."""
    if previous == 0:
        return 0.0 if current == 0 else 100.0
    return round((current - previous) / abs(previous) * 100, 1)


@router.get("/ozon")
async def get_ozon_dashboard(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("7d", description="Period: today, 7d, 30d"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregated Ozon dashboard data.

    Returns KPIs (orders, revenue, ad spend, views, clicks, DRR),
    sales chart, top products — all in one call.
    """
    # ── Verify shop ownership ─────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "ozon",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Ozon магазин не найден",
        )

    # ── Dates ─────────────────────────────────────────
    cur_start, cur_end, prev_start, prev_end = _parse_period(period)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()

        # ══════════════════════════════════════════════
        # 1. KPI — Orders (fact_ozon_orders)
        # ══════════════════════════════════════════════
        orders_kpi = ch.query("""
            SELECT
                period,
                count() AS orders_count,
                sum(price * quantity) AS revenue,
                sum(price * quantity) / nullIf(count(), 0) AS avg_check
            FROM (
                SELECT
                    CASE
                        WHEN toDate(addHours(in_process_at, 3)) >= {cur_start:Date} AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(addHours(in_process_at, 3)) >= {prev_start:Date} AND toDate(addHours(in_process_at, 3)) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price, quantity
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {prev_start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start,
            "cur_end": cur_end,
            "prev_start": prev_start,
            "prev_end": prev_end,
        }).result_rows

        orders_map = {row[0]: {"count": int(row[1]), "revenue": float(row[2]), "avg_check": float(row[3] or 0)} for row in orders_kpi}
        cur_orders = orders_map.get("current", {"count": 0, "revenue": 0, "avg_check": 0})
        prev_orders = orders_map.get("previous", {"count": 0, "revenue": 0, "avg_check": 0})

        # ══════════════════════════════════════════════
        # 2. KPI — Advertising: spend, views, clicks, DRR (fact_ozon_ad_daily)
        # ══════════════════════════════════════════════
        ads_kpi = ch.query("""
            SELECT
                period,
                sum(money_spent) AS total_spend,
                sum(revenue) AS total_revenue,
                sum(views) AS total_views,
                sum(clicks) AS total_clicks
            FROM (
                SELECT
                    CASE
                        WHEN dt >= {cur_start:Date} AND dt <= {cur_end:Date} THEN 'current'
                        WHEN dt >= {prev_start:Date} AND dt <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    money_spent, revenue, views, clicks
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {prev_start:Date}
                  AND dt <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
            "prev_start": prev_start, "prev_end": prev_end,
        }).result_rows

        ads_map = {}
        for row in ads_kpi:
            spend = float(row[1])
            ads_map[row[0]] = {
                "spend": spend,
                "views": int(row[3]),
                "clicks": int(row[4]),
            }
        cur_ads = ads_map.get("current", {"spend": 0, "views": 0, "clicks": 0})
        prev_ads = ads_map.get("previous", {"spend": 0, "views": 0, "clicks": 0})

        # DRR = ad_spend / orders_revenue (NOT ad_revenue!)
        cur_drr = round(cur_ads["spend"] / cur_orders["revenue"] * 100, 1) if cur_orders["revenue"] > 0 else 0
        prev_drr = round(prev_ads["spend"] / prev_orders["revenue"] * 100, 1) if prev_orders["revenue"] > 0 else 0

        # ══════════════════════════════════════════════
        # 3. Charts — Sales daily (fact_ozon_orders)
        # ══════════════════════════════════════════════
        chart_start = cur_start if period != "today" else cur_start - timedelta(days=29)
        sales_daily = ch.query("""
            SELECT
                toDate(addHours(in_process_at, 3)) AS day,
                count() AS orders_count,
                sum(price * quantity) AS revenue
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {end:Date}
            GROUP BY day
            ORDER BY day
        """, parameters={
            "shop_id": shop_id,
            "start": chart_start,
            "end": cur_end,
        }).result_rows

        charts_sales = [
            {"date": str(row[0]), "orders": int(row[1]), "revenue": float(row[2])}
            for row in sales_daily
        ]

        # ══════════════════════════════════════════════
        # 4. Top products (with ad spend joined in CH)
        # ══════════════════════════════════════════════
        top_products_rows = ch.query("""
            WITH
                orders_cur AS (
                    SELECT
                        sku,
                        anyLast(offer_id) AS offer_id,
                        count() AS orders_count,
                        sum(price * quantity) AS revenue
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
                      AND sku > 0
                    GROUP BY sku
                ),
                orders_prev AS (
                    SELECT
                        sku,
                        count() AS orders_count
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {prev_start:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {prev_end:Date}
                      AND sku > 0
                    GROUP BY sku
                ),
                latest_stocks AS (
                    SELECT
                        offer_id,
                        sumIf(free_to_sell, warehouse_type = 'fbo') AS stock_fbo,
                        sumIf(free_to_sell, warehouse_type = 'fbs') AS stock_fbs
                    FROM mms_analytics.fact_ozon_warehouse_stocks FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND dt = (
                          SELECT max(dt)
                          FROM mms_analytics.fact_ozon_warehouse_stocks
                          WHERE shop_id = {shop_id:UInt32}
                      )
                    GROUP BY offer_id
                ),
                latest_prices AS (
                    SELECT offer_id, price
                    FROM mms_analytics.fact_ozon_prices FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND dt = (
                          SELECT max(dt)
                          FROM mms_analytics.fact_ozon_prices
                          WHERE shop_id = {shop_id:UInt32}
                      )
                ),
                ad_per_sku AS (
                    SELECT
                        sku,
                        sum(money_spent) AS ad_spend
                    FROM mms_analytics.fact_ozon_ad_daily FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND dt >= {cur_start:Date}
                      AND dt <= {cur_end:Date}
                    GROUP BY sku
                )
            SELECT
                oc.offer_id,
                oc.orders_count,
                oc.revenue,
                CASE WHEN op.orders_count > 0
                    THEN round((oc.orders_count - op.orders_count) / op.orders_count * 100, 1)
                    ELSE 0
                END AS delta_pct,
                coalesce(ls.stock_fbo, 0) AS stock_fbo,
                coalesce(ls.stock_fbs, 0) AS stock_fbs,
                coalesce(lp.price, 0) AS price,
                coalesce(aps.ad_spend, 0) AS ad_spend,
                CASE WHEN oc.revenue > 0
                    THEN round(coalesce(aps.ad_spend, 0) / oc.revenue * 100, 1)
                    ELSE 0
                END AS drr
            FROM orders_cur oc
            LEFT JOIN orders_prev op ON oc.sku = op.sku
            LEFT JOIN latest_stocks ls ON oc.offer_id = ls.offer_id
            LEFT JOIN latest_prices lp ON oc.offer_id = lp.offer_id
            LEFT JOIN ad_per_sku aps ON oc.sku = aps.sku
            ORDER BY oc.revenue DESC
            LIMIT 50
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
            "prev_start": prev_start, "prev_end": prev_end,
        }).result_rows

        top_products = []
        for row in top_products_rows:
            top_products.append({
                "offer_id": row[0],
                "name": "",
                "image_url": "",
                "orders": int(row[1]),
                "revenue": float(row[2]),
                "delta_pct": float(row[3]),
                "stock_fbo": int(row[4]),
                "stock_fbs": int(row[5]),
                "price": float(row[6]),
                "ad_spend": float(row[7]),
                "drr": float(row[8]),
            })

        # Enrich with product names & images from PostgreSQL
        if top_products:
            offer_ids = [p["offer_id"] for p in top_products]
            pg_result = await db.execute(
                text("""
                    SELECT offer_id, name,
                           COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :shop_id
                      AND offer_id = ANY(:offer_ids)
                """),
                {"shop_id": shop_id, "offer_ids": offer_ids},
            )
            pg_map = {}
            for row in pg_result:
                pg_map[row[0]] = {"name": row[1], "image_url": row[2]}

            for p in top_products:
                info = pg_map.get(p["offer_id"], {})
                p["name"] = info.get("name", p["offer_id"])
                p["image_url"] = info.get("image_url", "")

        # ══════════════════════════════════════════════
        # 5. Charts — Ads daily (fact_ozon_ad_daily)
        # ══════════════════════════════════════════════
        ads_daily = ch.query("""
            WITH ads AS (
                SELECT
                    dt AS day,
                    sum(money_spent) AS spend,
                    sum(views) AS total_views,
                    sum(clicks) AS total_clicks,
                    sum(add_to_cart) AS cart,
                    sum(orders) AS total_orders,
                    sum(revenue) AS ad_revenue
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {start:Date}
                  AND dt <= {end:Date}
                GROUP BY day
            ),
            daily_orders AS (
                SELECT
                    toDate(addHours(in_process_at, 3)) AS day,
                    sum(price * quantity) AS total_revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {end:Date}
                GROUP BY day
            )
            SELECT
                a.day,
                a.spend,
                a.total_views,
                a.total_clicks,
                a.cart,
                a.total_orders,
                CASE WHEN a.ad_revenue > 0
                    THEN round(a.spend / a.ad_revenue * 100, 1) ELSE 0
                END AS drr_ad,
                CASE WHEN o.total_revenue > 0
                    THEN round(a.spend / o.total_revenue * 100, 1) ELSE 0
                END AS drr_total
            FROM ads a
            LEFT JOIN daily_orders o ON a.day = o.day
            ORDER BY a.day
        """, parameters={
            "shop_id": shop_id,
            "start": chart_start,
            "end": cur_end,
        }).result_rows

        charts_ads = [
            {
                "date": str(row[0]),
                "spend": round(float(row[1]), 2),
                "views": int(row[2]),
                "clicks": int(row[3]),
                "cart": int(row[4]),
                "orders": int(row[5]),
                "drr_ad": float(row[6]),
                "drr_total": float(row[7]),
            }
            for row in ads_daily
        ]

        ch.close()

        # ══════════════════════════════════════════════
        # Build response
        # ══════════════════════════════════════════════
        return {
            "shop_id": shop_id,
            "period": period,
            "kpi": {
                "orders_count": cur_orders["count"],
                "orders_delta": _safe_delta(cur_orders["count"], prev_orders["count"]),
                "revenue": cur_orders["revenue"],
                "revenue_delta": _safe_delta(cur_orders["revenue"], prev_orders["revenue"]),
                "avg_check": round(cur_orders["avg_check"], 0),
                "ad_spend": cur_ads["spend"],
                "ad_spend_delta": _safe_delta(cur_ads["spend"], prev_ads["spend"]),
                "views": cur_ads["views"],
                "views_delta": _safe_delta(cur_ads["views"], prev_ads["views"]),
                "clicks": cur_ads["clicks"],
                "clicks_delta": _safe_delta(cur_ads["clicks"], prev_ads["clicks"]),
                "drr": cur_drr,
                "drr_delta": round(cur_drr - prev_drr, 1),
            },
            "charts": {
                "sales_daily": charts_sales,
                "ads_daily": charts_ads,
            },
            "top_products": top_products,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Dashboard query failed for shop %s: %s", shop_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки дашборда: {str(e)}",
        )


# ══════════════════════════════════════════════════════════════════
# Wildberries Dashboard — Full Analytics
# ══════════════════════════════════════════════════════════════════

@router.get("/wb")
async def get_wb_dashboard(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("7d", description="Period: today, 7d, 30d"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Full WB analytics dashboard.

    Returns: KPI cards, multi-metric chart, alerts (5 tabs),
    orders feed, weekly finance summary — all in one call.
    """
    # ── Verify shop ownership ─────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
            Shop.marketplace == "wildberries",
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Wildberries магазин не найден",
        )

    # ── Dates ─────────────────────────────────────────
    cur_start, cur_end, prev_start, prev_end = _parse_period(period)
    chart_start = cur_start if period != "today" else cur_start - timedelta(days=29)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()
    except Exception as e:
        logger.error("ClickHouse connection error: %s", e)
        raise HTTPException(status_code=500, detail="Analytics unavailable")

    cost_map: dict[str, float] = {}

    try:
        # ══════════════════════════════════════════════
        # 1. KPI — Sales (fact_orders_raw) + Cancellations
        # ══════════════════════════════════════════════
        orders_kpi = ch.query("""
            SELECT
                period,
                countIf(is_cancel = 0) AS orders_count,
                sumIf(price_with_disc, is_cancel = 0) AS revenue,
                sumIf(price_with_disc, is_cancel = 0) / nullIf(countIf(is_cancel = 0), 0) AS avg_check,
                countIf(is_cancel = 1) AS cancels
            FROM (
                SELECT
                    CASE
                        WHEN toDate(addHours(date, 3)) >= {cur_start:Date} AND toDate(addHours(date, 3)) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(addHours(date, 3)) >= {prev_start:Date} AND toDate(addHours(date, 3)) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price_with_disc, is_cancel
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(date, 3)) >= {prev_start:Date}
                  AND toDate(addHours(date, 3)) <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
            "prev_start": prev_start, "prev_end": prev_end,
        }).result_rows

        orders_map = {
            row[0]: {"count": int(row[1]), "revenue": float(row[2]), "avg_check": float(row[3] or 0), "cancels": int(row[4])}
            for row in orders_kpi
        }
        cur_orders = orders_map.get("current", {"count": 0, "revenue": 0, "avg_check": 0, "cancels": 0})
        prev_orders = orders_map.get("previous", {"count": 0, "revenue": 0, "avg_check": 0, "cancels": 0})

        # ══════════════════════════════════════════════
        # 2. KPI — Advertising / Funnel
        # ══════════════════════════════════════════════
        ads_kpi = ch.query("""
            SELECT
                period,
                sum(spend) AS total_spend,
                sum(views) AS total_views,
                sum(clicks) AS total_clicks,
                sum(atbs) AS total_cart,
                sum(orders) AS total_orders,
                sum(revenue) AS total_ad_revenue
            FROM (
                SELECT
                    CASE
                        WHEN date >= {cur_start:Date} AND date <= {cur_end:Date} THEN 'current'
                        WHEN date >= {prev_start:Date} AND date <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    spend, views, clicks, atbs, orders, revenue
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND date >= {prev_start:Date}
                  AND date <= {cur_end:Date}
            )
            WHERE period != ''
            GROUP BY period
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
            "prev_start": prev_start, "prev_end": prev_end,
        }).result_rows

        ads_map = {}
        for row in ads_kpi:
            ads_map[row[0]] = {
                "spend": float(row[1]), "views": int(row[2]),
                "clicks": int(row[3]), "cart": int(row[4]),
                "orders": int(row[5]), "ad_revenue": float(row[6]),
            }
        _zero_ads = {"spend": 0, "views": 0, "clicks": 0, "cart": 0, "orders": 0, "ad_revenue": 0}
        cur_ads = ads_map.get("current", _zero_ads)
        prev_ads = ads_map.get("previous", _zero_ads)

        # DRR / CTR / Conversion / Cancel rate
        cur_drr_total = round(cur_ads["spend"] / cur_orders["revenue"] * 100, 1) if cur_orders["revenue"] > 0 else 0
        prev_drr_total = round(prev_ads["spend"] / prev_orders["revenue"] * 100, 1) if prev_orders["revenue"] > 0 else 0
        cur_drr_ad = round(cur_ads["spend"] / cur_ads["ad_revenue"] * 100, 1) if cur_ads["ad_revenue"] > 0 else 0
        prev_drr_ad = round(prev_ads["spend"] / prev_ads["ad_revenue"] * 100, 1) if prev_ads["ad_revenue"] > 0 else 0
        cur_ctr = round(cur_ads["clicks"] / cur_ads["views"] * 100, 1) if cur_ads["views"] > 0 else 0
        prev_ctr = round(prev_ads["clicks"] / prev_ads["views"] * 100, 1) if prev_ads["views"] > 0 else 0
        cur_conv = round(cur_ads["orders"] / cur_ads["clicks"] * 100, 1) if cur_ads["clicks"] > 0 else 0
        prev_conv = round(prev_ads["orders"] / prev_ads["clicks"] * 100, 1) if prev_ads["clicks"] > 0 else 0
        # click → cart conversion
        cur_click_to_cart = round(cur_ads["cart"] / cur_ads["clicks"] * 100, 1) if cur_ads["clicks"] > 0 else 0
        prev_click_to_cart = round(prev_ads["cart"] / prev_ads["clicks"] * 100, 1) if prev_ads["clicks"] > 0 else 0
        # cancel rate
        cur_cancel_rate = round(cur_orders["cancels"] / (cur_orders["count"] + cur_orders["cancels"]) * 100, 1) if (cur_orders["count"] + cur_orders["cancels"]) > 0 else 0
        prev_cancel_rate = round(prev_orders["cancels"] / (prev_orders["count"] + prev_orders["cancels"]) * 100, 1) if (prev_orders["count"] + prev_orders["cancels"]) > 0 else 0

        # ══════════════════════════════════════════════
        # 3. Profit via unit economics (orders × unit profit)
        #    Unit profit = avg_payout_per_unit - avg_logistics_per_unit - COGS
        #    Same approach as wb_prices.py — accurate per-product economics
        # ══════════════════════════════════════════════
        profit_cur = 0.0
        profit_prev = 0.0
        profit_pct = 0.0
        finance_week_start = None
        finance_week_end = None
        # finance_week_start/end initialized below

        # 3a. Load COGS from PostgreSQL
        try:
            cost_result = await db.execute(
                text("SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost FROM product_costs WHERE shop_id = :sid AND (cost_price > 0 OR packaging_cost > 0)"),
                {"sid": shop_id},
            )
            cost_map = {r[0].lower(): float(r[1]) for r in cost_result.fetchall()}
        except Exception as e:
            logger.warning("WB COGS load failed: %s", e)

        # 3b. Get per-nm_id avg payout/logistics from fact_finances (30d rolling)
        unit_profit_map: dict[int, float] = {}  # nm_id → profit_per_unit (payout - logistics - COGS)
        try:
            finance_unit_data = ch.query("""
                SELECT
                    toUInt64(external_id) AS nm_id,
                    sum(payout_amount) / nullIf(
                        sumIf(quantity, operation_type = 'Продажа' AND quantity > 0), 0
                    ) AS avg_payout_per_unit,
                    sum(wb_delivery_rub) / nullIf(
                        sumIf(quantity, operation_type = 'Продажа' AND quantity > 0), 0
                    ) AS avg_logistics_per_unit
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND marketplace = 1
                  AND event_date >= today() - 30
                GROUP BY nm_id
                HAVING sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) > 0
            """, parameters={"shop_id": shop_id}).result_rows

            # Map nm_id → vendor_code for COGS lookup
            nm_ids_finance = [int(r[0]) for r in finance_unit_data]
            vc_by_nm: dict[int, str] = {}
            if nm_ids_finance:
                try:
                    vc_result = await db.execute(
                        text("SELECT nm_id, COALESCE(vendor_code, '') FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                        {"sid": shop_id, "ids": nm_ids_finance},
                    )
                    vc_by_nm = {int(r[0]): str(r[1]).lower() for r in vc_result.fetchall()}
                except Exception:
                    pass

            for r in finance_unit_data:
                nm = int(r[0])
                avg_payout = float(r[1] or 0)
                avg_logistics = abs(float(r[2] or 0))
                vc = vc_by_nm.get(nm, "")
                cogs = cost_map.get(vc, 0)
                unit_profit = avg_payout - avg_logistics - cogs
                unit_profit_map[nm] = round(unit_profit, 2)
        except Exception as e:
            logger.warning("WB unit profit map failed: %s", e)

        # 3c. Calculate total profit for current and previous periods
        #     profit = sum(unit_profit[nm_id] * qty) for each ordered nm_id
        try:
            orders_by_nm = ch.query("""
                SELECT
                    period, nm_id, count() AS qty
                FROM (
                    SELECT
                        CASE
                            WHEN toDate(addHours(date, 3)) >= {cur_start:Date} AND toDate(addHours(date, 3)) <= {cur_end:Date} THEN 'current'
                            WHEN toDate(addHours(date, 3)) >= {prev_start:Date} AND toDate(addHours(date, 3)) <= {prev_end:Date} THEN 'previous'
                        END AS period,
                        nm_id
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(date, 3)) >= {prev_start:Date}
                      AND toDate(addHours(date, 3)) <= {cur_end:Date}
                )
                WHERE period != ''
                GROUP BY period, nm_id
            """, parameters={
                "shop_id": shop_id,
                "cur_start": cur_start, "cur_end": cur_end,
                "prev_start": prev_start, "prev_end": prev_end,
            }).result_rows

            for row in orders_by_nm:
                period_name = row[0]
                nm = int(row[1])
                qty = int(row[2])
                up = unit_profit_map.get(nm)
                if up is not None:
                    if period_name == "current":
                        profit_cur += up * qty
                    else:
                        profit_prev += up * qty

            profit_cur = round(profit_cur, 2)
            profit_prev = round(profit_prev, 2)
            profit_pct = round(profit_cur / cur_orders["revenue"] * 100, 1) if cur_orders["revenue"] > 0 else 0
        except Exception as e:
            logger.warning("WB unit-economics profit failed: %s", e)

        # 3d. finance_week_start/end — still needed for finance_summary P&L card
        try:
            week_range = ch.query("""
                SELECT min(event_date), max(event_date)
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32} AND marketplace = 1
                  AND event_date >= today() - 14
            """, parameters={"shop_id": shop_id}).result_rows
            if week_range and week_range[0][0] and str(week_range[0][0]) != '1970-01-01':
                finance_week_end = week_range[0][1]
                finance_week_start = finance_week_end - timedelta(days=6)
        except Exception as e:
            logger.warning("WB finance week range failed: %s", e)

        # ══════════════════════════════════════════════
        # 4. Chart data (daily)
        # ══════════════════════════════════════════════
        sales_daily = ch.query("""
            SELECT toDate(addHours(date, 3)) AS day,
                   count() AS orders_count,
                   sum(price_with_disc) AS revenue
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(date, 3)) >= {start:Date}
              AND toDate(addHours(date, 3)) <= {end:Date}
            GROUP BY day ORDER BY day
        """, parameters={"shop_id": shop_id, "start": chart_start, "end": cur_end}).result_rows
        sales_by_day = {str(r[0]): {"orders": int(r[1]), "revenue": float(r[2])} for r in sales_daily}

        ads_daily = ch.query("""
            SELECT date AS day,
                   sum(spend) AS spend, sum(views) AS views,
                   sum(clicks) AS clicks, sum(atbs) AS cart,
                   sum(orders) AS orders, sum(revenue) AS ad_revenue
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {start:Date} AND date <= {end:Date}
            GROUP BY day ORDER BY day
        """, parameters={"shop_id": shop_id, "start": chart_start, "end": cur_end}).result_rows
        ads_by_day = {str(r[0]): {"spend": float(r[1]), "views": int(r[2]), "clicks": int(r[3]),
                                   "cart": int(r[4]), "orders": int(r[5]), "ad_revenue": float(r[6])} for r in ads_daily}

        all_days = sorted(set(list(sales_by_day.keys()) + list(ads_by_day.keys())))
        chart_data = []
        for day in all_days:
            s = sales_by_day.get(day, {"orders": 0, "revenue": 0})
            a = ads_by_day.get(day, {"spend": 0, "views": 0, "clicks": 0, "cart": 0, "orders": 0, "ad_revenue": 0})
            chart_data.append({
                "date": day,
                "revenue": round(s["revenue"], 2),
                "orders": s["orders"],
                "ad_spend": round(a["spend"], 2),
                "views": a["views"],
                "clicks": a["clicks"],
                "cart": a["cart"],
                "ad_orders": a["orders"],
                "drr_ad": round(a["spend"] / a["ad_revenue"] * 100, 1) if a["ad_revenue"] > 0 else 0,
                "drr_total": round(a["spend"] / s["revenue"] * 100, 1) if s["revenue"] > 0 else 0,
                "ctr": round(a["clicks"] / a["views"] * 100, 1) if a["views"] > 0 else 0,
            })

        # ══════════════════════════════════════════════
        # 5. ALERTS — Warehouses
        # ══════════════════════════════════════════════
        alerts_warehouses = []
        try:
            wh_alerts = ch.query("""
                WITH latest AS (
                    SELECT nm_id, sum(quantity) AS stock
                    FROM mms_analytics.fact_inventory_snapshot FINAL
                    WHERE shop_id = {shop_id:UInt32}
                    GROUP BY nm_id
                ),
                avg_sales AS (
                    SELECT nm_id, count() / 14.0 AS avg_daily
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(date, 3)) >= today() - 14
                    GROUP BY nm_id
                )
                SELECT l.nm_id, l.stock,
                       coalesce(a.avg_daily, 0) AS avg_daily,
                       CASE WHEN a.avg_daily > 0 THEN round(l.stock / a.avg_daily, 1) ELSE 999 END AS days_left
                FROM latest l
                LEFT JOIN avg_sales a ON l.nm_id = a.nm_id
                WHERE l.stock <= 0 OR (a.avg_daily > 0 AND l.stock / a.avg_daily <= 10)
                ORDER BY days_left ASC
                LIMIT 30
            """, parameters={"shop_id": shop_id}).result_rows
            for r in wh_alerts:
                alerts_warehouses.append({
                    "nm_id": int(r[0]), "stock": int(r[1]),
                    "avg_daily_sales": round(float(r[2]), 1),
                    "days_left": round(float(r[3]), 1) if float(r[3]) < 999 else None,
                    "severity": "critical" if int(r[1]) <= 0 else "warning",
                })
        except Exception as e:
            logger.warning("WB warehouse alerts failed: %s", e)

        # ══════════════════════════════════════════════
        # 6. ALERTS — Sales (no sales 7d)
        # ══════════════════════════════════════════════
        alerts_sales = []
        try:
            no_sales = ch.query("""
                WITH stocked AS (
                    SELECT nm_id, sum(quantity) AS stock
                    FROM mms_analytics.fact_inventory_snapshot FINAL
                    WHERE shop_id = {shop_id:UInt32}
                    GROUP BY nm_id HAVING stock > 0
                ),
                recent_sales AS (
                    SELECT nm_id, max(toDate(addHours(date, 3))) AS last_sale
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(date, 3)) >= toDate('2020-01-01')
                    GROUP BY nm_id
                )
                SELECT s.nm_id, s.stock,
                       r.last_sale,
                       dateDiff('day', coalesce(r.last_sale, today() - 9999), today()) AS days_without
                FROM stocked s
                LEFT JOIN recent_sales r ON s.nm_id = r.nm_id
                WHERE coalesce(r.last_sale, toDate('2000-01-01')) < today() - 6
                ORDER BY days_without DESC
                LIMIT 30
            """, parameters={"shop_id": shop_id}).result_rows
            for r in no_sales:
                last_sale = str(r[2]) if r[2] and str(r[2]) > '2020-01-01' else None
                days_w = int(r[3])
                alerts_sales.append({
                    "nm_id": int(r[0]), "stock": int(r[1]),
                    "last_sale_date": last_sale,
                    "days_without_sales": min(days_w, 9999),
                })
        except Exception as e:
            logger.warning("WB sales alerts failed: %s", e)

        # ══════════════════════════════════════════════
        # 7. ALERTS — Paid Storage
        # ══════════════════════════════════════════════
        alerts_storage = []
        storage_total_cost = 0.0
        try:
            storage_data = ch.query("""
                SELECT nm_id, vendor_code, warehouse,
                       sum(warehouse_price) AS total_cost,
                       avg(volume_liters) AS avg_volume
                FROM mms_analytics.fact_wb_paid_storage
                WHERE shop_id = {shop_id:UInt32} AND dt >= today() - 7
                GROUP BY nm_id, vendor_code, warehouse
                HAVING total_cost > 0
                ORDER BY total_cost DESC
                LIMIT 20
            """, parameters={"shop_id": shop_id}).result_rows
            for r in storage_data:
                cost = float(r[3])
                storage_total_cost += cost
                alerts_storage.append({
                    "nm_id": int(r[0]), "vendor_code": str(r[1] or ""),
                    "warehouse": str(r[2] or ""),
                    "cost_7d": round(cost, 2),
                    "volume_liters": round(float(r[4] or 0), 2),
                })
        except Exception as e:
            logger.warning("WB storage alerts failed: %s", e)

        # ══════════════════════════════════════════════
        # 8. ALERTS — Advertising (Budget-aware, 7 problem types)
        # ══════════════════════════════════════════════
        alerts_advertising = []
        try:
            # ── 1. Grab all campaigns + statuses ──
            all_campaigns_rows = ch.query("""
                SELECT advert_id, name, status
                FROM mms_analytics.dim_advert_campaigns FINAL
                WHERE shop_id = {shop_id:UInt32}
            """, parameters={"shop_id": shop_id}).result_rows
            camp_map: dict[int, dict] = {}
            for cr in all_campaigns_rows:
                camp_map[int(cr[0])] = {"name": str(cr[1] or ""), "status": int(cr[2])}

            # ── 2. Grab budgets from Redis ──
            import redis as _redis, json as _json, os as _os
            budgets: dict[int, int] = {}
            try:
                _redis_url = _os.environ.get("REDIS_URL", "redis://redis:6379/0")
                _r = _redis.from_url(_redis_url)
                _pipe = _r.pipeline()
                for aid in camp_map:
                    _pipe.get(f"budget:{shop_id}:{aid}")
                results = _pipe.execute()
                for aid_key, raw in zip(camp_map.keys(), results):
                    if raw:
                        budgets[aid_key] = _json.loads(raw).get("total", -1)
                    else:
                        budgets[aid_key] = -1  # No data in Redis
                _r.close()
            except Exception as e:
                logger.warning("Redis budget read failed: %s", e)

            # ── 3. Compute real_status: budget=0 on "active" → budget_depleted ──
            for aid, info in camp_map.items():
                b = budgets.get(aid, -1)
                if info["status"] in (9, 11) and b == 0:
                    info["real_status"] = "budget_depleted"
                elif info["status"] in (9, 11) and b > 0:
                    info["real_status"] = "active"
                elif info["status"] in (9, 11) and b == -1:
                    info["real_status"] = "active"  # No budget data — assume active
                elif info["status"] == 7:
                    info["real_status"] = "paused"
                else:
                    info["real_status"] = "stopped"

            # ── 4. 7-day stats ──
            stats_7d_rows = ch.query("""
                SELECT advert_id,
                       sum(views) AS s_views, sum(clicks) AS s_clicks,
                       sum(spend) AS s_spend, sum(revenue) AS s_rev,
                       sum(orders) AS s_orders,
                       max(date) AS last_date
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32} AND date >= today() - 7
                GROUP BY advert_id
            """, parameters={"shop_id": shop_id}).result_rows
            stats_7d: dict[int, dict] = {}
            for sr in stats_7d_rows:
                stats_7d[int(sr[0])] = {
                    "views": int(sr[1]), "clicks": int(sr[2]),
                    "spend": float(sr[3]), "revenue": float(sr[4]),
                    "orders": int(sr[5]), "last_date": str(sr[6]),
                }

            # ── 5. 14-day stats (for paused/budget analysis) ──
            stats_14d_rows = ch.query("""
                SELECT advert_id,
                       sum(spend) AS s_spend, sum(revenue) AS s_rev,
                       sum(orders) AS s_orders, max(date) AS last_date
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32} AND date >= today() - 14
                GROUP BY advert_id
            """, parameters={"shop_id": shop_id}).result_rows
            stats_14d: dict[int, dict] = {}
            for sr in stats_14d_rows:
                stats_14d[int(sr[0])] = {
                    "spend": float(sr[1]), "revenue": float(sr[2]),
                    "orders": int(sr[3]), "last_date": str(sr[4]),
                }

            # ── 6. 3-day views (for no_views / low_views) ──
            stats_3d_rows = ch.query("""
                SELECT advert_id, sum(views) AS s_views, sum(clicks) AS s_clicks
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32} AND date >= today() - 3
                GROUP BY advert_id
            """, parameters={"shop_id": shop_id}).result_rows
            views_3d: dict[int, dict] = {}
            for r in stats_3d_rows:
                views_3d[int(r[0])] = {"views": int(r[1]), "clicks": int(r[2])}

            # Shop avg DRR (for context)
            drr_values = []
            for s in stats_7d.values():
                if s["revenue"] > 0 and s["spend"] > 0:
                    d = s["spend"] / s["revenue"] * 100
                    if d < 200:
                        drr_values.append(d)
            shop_avg_drr = round(sum(drr_values) / len(drr_values), 1) if drr_values else 0

            seen_ids: set[int] = set()

            def _status_label(info):
                rs = info.get("real_status", "active")
                if rs == "budget_depleted":
                    return "no_budget"
                elif rs == "paused":
                    return "paused"
                return "active"

            def _budget_val(aid):
                b = budgets.get(aid, -1)
                return b if b >= 0 else None

            # ═══════════════════════════════════════════
            # P1: Budget depleted — was working, now stopped
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if info.get("real_status") != "budget_depleted":
                    continue
                s14 = stats_14d.get(aid)
                if not s14 or s14["orders"] == 0:
                    continue  # Never had orders — not interesting
                drr_val = round(s14["spend"] / s14["revenue"] * 100, 1) if s14["revenue"] > 0 else None
                seen_ids.add(aid)
                alerts_advertising.append({
                    "advert_id": aid, "name": info["name"],
                    "problem": "budget_depleted", "priority": 1,
                    "status": _status_label(info), "budget_total": 0,
                    "reason": f"Бюджет 0 ₽ — было {s14['orders']} заказов, пополните бюджет",
                    "spend": round(s14["spend"], 2), "revenue": round(s14["revenue"], 2),
                    "orders": s14["orders"], "views": 0, "clicks": 0,
                    "drr": drr_val,
                })

            # ═══════════════════════════════════════════
            # P2: Active, spending money with ZERO revenue
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "active":
                    continue
                s = stats_7d.get(aid)
                if not s or s["spend"] < 300:
                    continue
                if s["revenue"] == 0 and s["views"] > 0:
                    seen_ids.add(aid)
                    alerts_advertising.append({
                        "advert_id": aid, "name": info["name"],
                        "problem": "spending_no_revenue", "priority": 2,
                        "status": _status_label(info), "budget_total": _budget_val(aid),
                        "reason": f"Потрачено {round(s['spend']):,} ₽, 0 заказов за 7 дн.".replace(",", " "),
                        "spend": round(s["spend"], 2), "revenue": 0,
                        "orders": 0, "views": s["views"], "clicks": s["clicks"],
                        "drr": None,
                    })

            # ═══════════════════════════════════════════
            # P3: Active, high DRR (> 30%, spend > 200₽)
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "active":
                    continue
                s = stats_7d.get(aid)
                if not s or s["spend"] < 200:
                    continue
                if s["revenue"] > 0:
                    drr_val = round(s["spend"] / s["revenue"] * 100, 1)
                else:
                    continue
                if drr_val > 30:
                    seen_ids.add(aid)
                    alerts_advertising.append({
                        "advert_id": aid, "name": info["name"],
                        "problem": "high_drr", "priority": 3,
                        "status": _status_label(info), "budget_total": _budget_val(aid),
                        "reason": f"Расход {round(s['spend']):,} ₽ → выручка {round(s['revenue']):,} ₽".replace(",", " "),
                        "spend": round(s["spend"], 2), "revenue": round(s["revenue"], 2),
                        "orders": s["orders"], "views": s["views"], "clicks": s["clicks"],
                        "drr": drr_val, "avg_drr": shop_avg_drr,
                    })

            # ═══════════════════════════════════════════
            # P4: Active, low views — need to raise bid
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "active":
                    continue
                v3 = views_3d.get(aid)
                s7 = stats_7d.get(aid)
                if not v3 or not s7:
                    continue
                # Low views: < 200 views in 3 days but has spent money or had meaningful views before
                views_3 = v3["views"]
                if views_3 == 0:
                    continue  # Handled separately below (no_views)
                # Only flag if views are genuinely low AND campaign was running
                if views_3 < 200 and s7["spend"] > 50:
                    seen_ids.add(aid)
                    drr_val = round(s7["spend"] / s7["revenue"] * 100, 1) if s7["revenue"] > 0 else None
                    alerts_advertising.append({
                        "advert_id": aid, "name": info["name"],
                        "problem": "low_views", "priority": 4,
                        "status": _status_label(info), "budget_total": _budget_val(aid),
                        "reason": f"{views_3} показов за 3 дн. — повысьте ставку",
                        "spend": round(s7["spend"], 2), "revenue": round(s7["revenue"], 2),
                        "orders": s7["orders"], "views": views_3, "clicks": v3["clicks"],
                        "drr": drr_val,
                    })

            # ═══════════════════════════════════════════
            # P5: Active, no views 3 days (only real fresh campaigns)
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "active":
                    continue
                v3 = views_3d.get(aid, {})
                if v3.get("views", 0) > 0:
                    continue
                s7 = stats_7d.get(aid)
                s14 = stats_14d.get(aid)
                if not s7 and not s14:
                    continue  # Dead campaign
                if s14 and s14["last_date"] < str(cur_start):
                    continue  # Old campaign
                seen_ids.add(aid)
                alerts_advertising.append({
                    "advert_id": aid, "name": info["name"],
                    "problem": "no_views", "priority": 5,
                    "status": _status_label(info), "budget_total": _budget_val(aid),
                    "reason": "0 показов за 3 дня — проверьте ставку и бюджет",
                    "spend": round(s7["spend"], 2) if s7 else 0,
                    "revenue": round(s7["revenue"], 2) if s7 else 0,
                    "orders": s7["orders"] if s7 else 0,
                    "views": 0, "clicks": 0, "drr": None,
                })

            # ═══════════════════════════════════════════
            # P6: Paused profitable campaigns (worth restarting)
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "paused":
                    continue
                s14 = stats_14d.get(aid)
                if not s14 or s14["orders"] == 0:
                    continue
                if s14["last_date"] < str(cur_start):
                    continue
                drr_val = round(s14["spend"] / s14["revenue"] * 100, 1) if s14["revenue"] > 0 else 999
                if drr_val < 30:
                    seen_ids.add(aid)
                    from datetime import datetime as _dt
                    try:
                        days_ago = (cur_end - _dt.strptime(s14["last_date"], "%Y-%m-%d").date()).days
                    except Exception:
                        days_ago = 0
                    alerts_advertising.append({
                        "advert_id": aid, "name": info["name"],
                        "problem": "stopped_profitable", "priority": 6,
                        "status": _status_label(info), "budget_total": _budget_val(aid),
                        "reason": f"ДРР {drr_val}%, {s14['orders']} заказов — стоит запустить",
                        "spend": round(s14["spend"], 2), "revenue": round(s14["revenue"], 2),
                        "orders": s14["orders"], "views": 0, "clicks": 0,
                        "drr": drr_val,
                    })

            # ═══════════════════════════════════════════
            # P7: Active, clicks but zero orders
            # ═══════════════════════════════════════════
            for aid, info in camp_map.items():
                if aid in seen_ids or info.get("real_status") != "active":
                    continue
                s = stats_7d.get(aid)
                if not s or s["clicks"] < 50:
                    continue
                if s["orders"] == 0 and s["revenue"] == 0:
                    seen_ids.add(aid)
                    alerts_advertising.append({
                        "advert_id": aid, "name": info["name"],
                        "problem": "clicks_no_orders", "priority": 7,
                        "status": _status_label(info), "budget_total": _budget_val(aid),
                        "reason": f"{s['clicks']} кликов, 0 заказов — проверьте карточку",
                        "spend": round(s["spend"], 2), "revenue": 0,
                        "orders": 0, "views": s["views"], "clicks": s["clicks"],
                        "drr": None,
                    })

            # Sort by priority, then by spend desc
            alerts_advertising.sort(key=lambda x: (x.get("priority", 9), -(x.get("spend") or 0)))
        except Exception as e:
            logger.warning("WB advertising alerts failed: %s", e)
            import traceback
            logger.warning(traceback.format_exc())

        # ══════════════════════════════════════════════
        # 9. ALERTS — Finances (loss-making SKUs)
        # ══════════════════════════════════════════════
        alerts_finances = []
        try:
            if finance_week_start:
                loss_skus = ch.query("""
                    SELECT vendor_code,
                           sum(CASE WHEN operation_type = 'Продажа' THEN JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub') ELSE 0 END)
                           - sum(CASE WHEN operation_type = 'Возврат' THEN JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub') ELSE 0 END) AS revenue,
                           sum(payout_amount) AS payout,
                           sum(abs(wb_delivery_rub)) AS logistics,
                           sum(abs(storage_fee)) AS storage,
                           sum(abs(JSONExtractFloat(raw_payload, 'deduction'))) AS deductions,
                           sumIf(quantity, operation_type = 'Продажа') AS qty
                    FROM mms_analytics.fact_finances FINAL
                    WHERE shop_id = {shop_id:UInt32} AND marketplace = 1
                      AND event_date >= {ws:Date} AND event_date <= {we:Date}
                    GROUP BY vendor_code HAVING qty > 0
                """, parameters={"shop_id": shop_id, "ws": finance_week_start, "we": finance_week_end}).result_rows

                for r in loss_skus:
                    rev = float(r[1] or 0)
                    pay = float(r[2] or 0)
                    comm = max(rev - pay, 0)
                    log_v = float(r[3] or 0)
                    stor_v = float(r[4] or 0)
                    ded_v = float(r[5] or 0)
                    qty = int(r[6] or 0)
                    vc = str(r[0] or "")
                    cogs_v = cost_map.get(vc.lower(), 0) * qty
                    profit_v = rev - comm - log_v - stor_v - ded_v - cogs_v

                    if profit_v < 0:
                        # Determine reason
                        reason = "Совокупные расходы превышают выручку"
                        if rev > 0:
                            if log_v > rev * 0.35:
                                reason = f"Логистика {round(log_v/rev*100)}% от выручки"
                            elif ded_v > rev * 0.2:
                                reason = f"Высокие удержания ({round(ded_v):,} ₽)".replace(",", " ")
                            elif cogs_v > 0 and (rev - cogs_v) < 0:
                                reason = "Себестоимость выше выручки"
                            elif stor_v > abs(profit_v) * 0.3:
                                reason = f"Хранение съедает маржу ({round(stor_v):,} ₽)".replace(",", " ")
                        alerts_finances.append({
                            "vendor_code": vc,
                            "revenue": round(rev, 2),
                            "expenses": round(comm + log_v + stor_v + ded_v + cogs_v, 2),
                            "profit": round(profit_v, 2),
                            "profit_pct": round(profit_v / rev * 100, 1) if rev > 0 else 0,
                            "qty": qty,
                            "reason": reason,
                        })
                alerts_finances.sort(key=lambda x: x["profit"])
                alerts_finances = alerts_finances[:20]
        except Exception as e:
            logger.warning("WB finance alerts failed: %s", e)

        # ══════════════════════════════════════════════
        # 10. Orders Feed
        # ══════════════════════════════════════════════
        orders_feed = []
        try:
            feed_rows = ch.query("""
                SELECT nm_id, anyLast(supplier_article) AS article,
                       count() AS orders, sum(price_with_disc) AS revenue,
                       max(toDate(addHours(date, 3))) AS last_order
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(date, 3)) >= {start:Date}
                  AND toDate(addHours(date, 3)) <= {end:Date}
                GROUP BY nm_id
                ORDER BY orders DESC
                LIMIT 50
            """, parameters={"shop_id": shop_id, "start": cur_start, "end": cur_end}).result_rows
            for r in feed_rows:
                orders_feed.append({
                    "nm_id": int(r[0]), "supplier_article": str(r[1] or ""),
                    "orders": int(r[2]), "revenue": round(float(r[3]), 2),
                    "last_order": str(r[4]), "image_url": "",
                })
        except Exception as e:
            logger.warning("WB orders feed failed: %s", e)

        # ══════════════════════════════════════════════
        # 11. Finance Summary (P&L — full breakdown, Mon-Sun week)
        # ══════════════════════════════════════════════
        finance_summary = None
        try:
            # Determine the last *complete* Monday-Sunday week with data
            # toMonday() in ClickHouse gives the Monday of that ISO week
            fw_range = ch.query("""
                SELECT toMonday(max(event_date)) AS last_monday
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32} AND marketplace = 1
                  AND event_date >= today() - 21
            """, parameters={"shop_id": shop_id}).result_rows

            if fw_range and fw_range[0][0] and str(fw_range[0][0]) != '1970-01-01':
                from datetime import date as date_type
                last_monday = fw_range[0][0]
                if isinstance(last_monday, str):
                    last_monday = date_type.fromisoformat(last_monday)

                # Current week: Monday → Sunday
                fw_start = last_monday
                fw_end = last_monday + timedelta(days=6)

                # If the week is not yet complete (today is within it), use previous complete week
                today_d = date_type.today()
                if fw_end >= today_d:
                    fw_start = fw_start - timedelta(days=7)
                    fw_end = fw_end - timedelta(days=7)

                # Previous week (same length, for comparison)
                prev_fw_start = fw_start - timedelta(days=7)
                prev_fw_end = fw_end - timedelta(days=7)

                fs_data = ch.query("""
                    SELECT
                        -- Revenue (sales - returns)
                        sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Продажа' AND event_date >= {ws:Date} AND event_date <= {we:Date})
                         - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Возврат' AND event_date >= {ws:Date} AND event_date <= {we:Date}) AS rev_cur,
                        sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Продажа' AND event_date >= {pws:Date} AND event_date <= {pwe:Date})
                         - sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Возврат' AND event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS rev_prev,

                        -- Payout
                        sumIf(payout_amount, operation_type = 'Продажа' AND event_date >= {ws:Date} AND event_date <= {we:Date})
                         - sumIf(payout_amount, operation_type = 'Возврат' AND event_date >= {ws:Date} AND event_date <= {we:Date}) AS pay_cur,
                        sumIf(payout_amount, operation_type = 'Продажа' AND event_date >= {pws:Date} AND event_date <= {pwe:Date})
                         - sumIf(payout_amount, operation_type = 'Возврат' AND event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS pay_prev,

                        -- Logistics
                        sumIf(wb_delivery_rub, event_date >= {ws:Date} AND event_date <= {we:Date}) AS log_cur,
                        sumIf(wb_delivery_rub, event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS log_prev,

                        -- Storage
                        sumIf(storage_fee, event_date >= {ws:Date} AND event_date <= {we:Date}) AS stor_cur,
                        sumIf(storage_fee, event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS stor_prev,

                        -- Deductions (excluding ads promo)
                        sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {ws:Date} AND event_date <= {we:Date}
                            AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload,'bonus_type_name'),'продвижение')=0) AS ded_cur,
                        sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {pws:Date} AND event_date <= {pwe:Date}
                            AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload,'bonus_type_name'),'продвижение')=0) AS ded_prev,

                        -- Ad deductions (ВБ Промо)
                        sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {ws:Date} AND event_date <= {we:Date}
                            AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload,'bonus_type_name'),'продвижение')>0) AS ded_ads_cur,
                        sumIf(JSONExtractFloat(raw_payload, 'deduction'), event_date >= {pws:Date} AND event_date <= {pwe:Date}
                            AND positionCaseInsensitiveUTF8(JSONExtractString(raw_payload,'bonus_type_name'),'продвижение')>0) AS ded_ads_prev,

                        -- Acceptance
                        sumIf(acceptance_fee, event_date >= {ws:Date} AND event_date <= {we:Date}) AS acc_cur,
                        sumIf(acceptance_fee, event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS acc_prev,

                        -- Penalties (excluding 'Удержание' duplicates)
                        sumIf(penalty_total, event_date >= {ws:Date} AND event_date <= {we:Date}
                            AND operation_type != 'Удержание') AS pen_cur,
                        sumIf(penalty_total, event_date >= {pws:Date} AND event_date <= {pwe:Date}
                            AND operation_type != 'Удержание') AS pen_prev,

                        -- Returns (absolute)
                        sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Возврат' AND event_date >= {ws:Date} AND event_date <= {we:Date}) AS ret_cur,
                        sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                              operation_type = 'Возврат' AND event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS ret_prev,

                        -- Orders count
                        sumIf(quantity, operation_type='Продажа' AND quantity>0
                            AND event_date >= {ws:Date} AND event_date <= {we:Date}) AS ord_cur,
                        sumIf(quantity, operation_type='Продажа' AND quantity>0
                            AND event_date >= {pws:Date} AND event_date <= {pwe:Date}) AS ord_prev

                    FROM mms_analytics.fact_finances FINAL
                    WHERE shop_id = {shop_id:UInt32} AND marketplace = 1
                      AND event_date >= {pws:Date} AND event_date <= {we:Date}
                """, parameters={
                    "shop_id": shop_id,
                    "ws": fw_start, "we": fw_end,
                    "pws": prev_fw_start, "pwe": prev_fw_end,
                }).result_rows

                # Ad spend from fact_advert_stats_v3 (separate source, MAX-reconciliation)
                ads_week = ch.query("""
                    SELECT
                        sumIf(spend, date >= {ws:Date} AND date <= {we:Date}) AS ads_cur,
                        sumIf(spend, date >= {pws:Date} AND date <= {pwe:Date}) AS ads_prev
                    FROM mms_analytics.fact_advert_stats_v3 FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND date >= {pws:Date} AND date <= {we:Date}
                """, parameters={
                    "shop_id": shop_id,
                    "ws": fw_start, "we": fw_end,
                    "pws": prev_fw_start, "pwe": prev_fw_end,
                }).result_rows

                if fs_data:
                    r = fs_data[0]
                    rev_c = float(r[0] or 0)
                    rev_p = float(r[1] or 0)
                    pay_c = float(r[2] or 0)
                    pay_p = float(r[3] or 0)
                    log_c = abs(float(r[4] or 0))
                    log_p = abs(float(r[5] or 0))
                    stor_c = abs(float(r[6] or 0))
                    stor_p = abs(float(r[7] or 0))
                    ded_c = abs(float(r[8] or 0))
                    ded_p = abs(float(r[9] or 0))
                    ded_ads_c = abs(float(r[10] or 0))
                    ded_ads_p = abs(float(r[11] or 0))
                    acc_c = abs(float(r[12] or 0))
                    acc_p = abs(float(r[13] or 0))
                    pen_c = abs(float(r[14] or 0))
                    pen_p = abs(float(r[15] or 0))
                    ret_c = abs(float(r[16] or 0))
                    ret_p = abs(float(r[17] or 0))
                    ord_c = int(r[18] or 0)
                    ord_p = int(r[19] or 0)

                    comm_c = max(rev_c - pay_c, 0)
                    comm_p = max(rev_p - pay_p, 0)

                    # Ad spend: MAX(fact_finances promo deduction, fact_advert_stats)
                    ext_ads_c = float(ads_week[0][0] or 0) if ads_week else 0
                    ext_ads_p = float(ads_week[0][1] or 0) if ads_week else 0
                    ad_c = max(ded_ads_c, ext_ads_c)
                    ad_p = max(ded_ads_p, ext_ads_p)

                    # Profit = payout - logistics - storage - acceptance - deductions - ads - penalties
                    profit_c = pay_c - log_c - stor_c - acc_c - ded_c - ad_c - pen_c
                    profit_p = pay_p - log_p - stor_p - acc_p - ded_p - ad_p - pen_p
                    margin_c = round(profit_c / rev_c * 100, 1) if rev_c > 0 else 0

                    finance_summary = {
                        "week_start": str(fw_start),
                        "week_end": str(fw_end),
                        # Main rows with cur/prev for delta calculation
                        "revenue": round(rev_c, 2),
                        "revenue_prev": round(rev_p, 2),
                        "commission": round(comm_c, 2),
                        "commission_prev": round(comm_p, 2),
                        "logistics": round(log_c, 2),
                        "logistics_prev": round(log_p, 2),
                        "storage": round(stor_c, 2),
                        "storage_prev": round(stor_p, 2),
                        "ad_spend": round(ad_c, 2),
                        "ad_spend_prev": round(ad_p, 2),
                        "deductions": round(ded_c, 2),
                        "deductions_prev": round(ded_p, 2),
                        "acceptance": round(acc_c, 2),
                        "acceptance_prev": round(acc_p, 2),
                        "penalties": round(pen_c, 2),
                        "penalties_prev": round(pen_p, 2),
                        "returns": round(ret_c, 2),
                        "returns_prev": round(ret_p, 2),
                        "orders": ord_c,
                        "orders_prev": ord_p,
                        # Profit
                        "profit": round(profit_c, 2),
                        "profit_prev": round(profit_p, 2),
                        "profit_pct": margin_c,
                        # Deltas
                        "revenue_delta": _safe_delta(rev_c, rev_p),
                        "profit_delta": _safe_delta(profit_c, profit_p),
                    }
        except Exception as e:
            logger.warning("WB finance summary failed: %s", e)

        # ── Enrich alerts with product names ──
        all_nm_ids: set[int] = set()
        for item in alerts_warehouses + alerts_sales + orders_feed:
            all_nm_ids.add(item.get("nm_id", 0))
        for item in alerts_storage:
            all_nm_ids.add(item.get("nm_id", 0))

        vc_to_nm: dict[str, dict] = {}
        if alerts_finances:
            vcs = [a["vendor_code"] for a in alerts_finances]
            vcs_lower = [v.lower() for v in vcs]
            try:
                vc_nm = await db.execute(
                    text("SELECT vendor_code, nm_id, name, COALESCE(main_image_url, '') FROM dim_products WHERE shop_id = :sid AND lower(vendor_code) = ANY(:vcs)"),
                    {"sid": shop_id, "vcs": vcs_lower},
                )
                for row in vc_nm:
                    vc_to_nm[row[0].lower()] = {"nm_id": row[1], "name": row[2] or row[0], "vendor_code": row[0], "image_url": row[3]}
                    all_nm_ids.add(row[1])
            except Exception as e:
                logger.warning("Finance alerts enrichment failed: %s", e)

        for a in alerts_finances:
            info = vc_to_nm.get(a["vendor_code"].lower(), {})
            a["nm_id"] = info.get("nm_id", 0)
            a["name"] = info.get("name", a["vendor_code"])
            a["vendor_code"] = info.get("vendor_code", a["vendor_code"])
            a["image_url"] = info.get("image_url", "")

        nm_names: dict[int, str] = {}
        nm_vcodes: dict[int, str] = {}
        nm_images: dict[int, str] = {}
        nm_list = [n for n in all_nm_ids if n > 0]
        if nm_list:
            try:
                pg_result = await db.execute(
                    text("SELECT nm_id, COALESCE(name, vendor_code, '') AS name, COALESCE(vendor_code, '') AS vc, COALESCE(main_image_url, '') AS img FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:ids)"),
                    {"sid": shop_id, "ids": nm_list},
                )
                for row in pg_result:
                    nm_names[int(row[0])] = row[1]
                    nm_vcodes[int(row[0])] = row[2]
                    nm_images[int(row[0])] = row[3]
            except Exception:
                pass

        for item in alerts_warehouses + alerts_sales:
            nm = item.get("nm_id", 0)
            item["name"] = nm_names.get(nm, str(nm))
            item["vendor_code"] = nm_vcodes.get(nm, "")
            item["image_url"] = nm_images.get(nm, "")

        for item in orders_feed:
            nm = item.get("nm_id", 0)
            item["name"] = nm_names.get(nm, item.get("supplier_article", str(nm)))
            item["image_url"] = nm_images.get(nm, "")
            item["vendor_code"] = nm_vcodes.get(nm, item.get("supplier_article", ""))

        for item in alerts_storage:
            nm = item.get("nm_id", 0)
            item["name"] = nm_names.get(nm, item.get("vendor_code", str(nm)))
            item["vendor_code"] = nm_vcodes.get(nm, item.get("vendor_code", ""))
            item["image_url"] = nm_images.get(nm, "")

        ch.close()

        # ══════════════════════════════════════════════
        # Build response
        # ══════════════════════════════════════════════
        return {
            "shop_id": shop_id,
            "period": period,
            "date_from": str(cur_start),
            "date_to": str(cur_end),
            "kpi": {
                "sales": {
                    "revenue": round(cur_orders["revenue"], 2),
                    "revenue_delta": _safe_delta(cur_orders["revenue"], prev_orders["revenue"]),
                    "profit": round(profit_cur, 2),
                    "profit_delta": _safe_delta(profit_cur, profit_prev),
                    "profit_pct": profit_pct,
                    "orders": cur_orders["count"],
                    "orders_delta": _safe_delta(cur_orders["count"], prev_orders["count"]),
                    "cancels": cur_orders["cancels"],
                    "cancels_delta": _safe_delta(cur_orders["cancels"], prev_orders["cancels"]),
                    "cancel_rate": cur_cancel_rate,
                    "cancel_rate_delta": round(cur_cancel_rate - prev_cancel_rate, 1),
                },
                "advertising": {
                    "ad_spend": round(cur_ads["spend"], 2),
                    "ad_spend_delta": _safe_delta(cur_ads["spend"], prev_ads["spend"]),
                    "drr_total": cur_drr_total,
                    "drr_total_delta": round(cur_drr_total - prev_drr_total, 1),
                    "drr_ad": cur_drr_ad,
                    "drr_ad_delta": round(cur_drr_ad - prev_drr_ad, 1),
                },
                "funnel": {
                    "views": cur_ads["views"],
                    "views_delta": _safe_delta(cur_ads["views"], prev_ads["views"]),
                    "clicks": cur_ads["clicks"],
                    "clicks_delta": _safe_delta(cur_ads["clicks"], prev_ads["clicks"]),
                    "ctr": cur_ctr,
                    "ctr_delta": round(cur_ctr - prev_ctr, 1),
                    "cart": cur_ads["cart"],
                    "cart_delta": _safe_delta(cur_ads["cart"], prev_ads["cart"]),
                    "click_to_cart": cur_click_to_cart,
                    "click_to_cart_delta": round(cur_click_to_cart - prev_click_to_cart, 1),
                    "cart_conversion": round(cur_ads["orders"] / cur_ads["cart"] * 100, 1) if cur_ads["cart"] > 0 else 0,
                    "cart_conversion_delta": round(
                        (round(cur_ads["orders"] / cur_ads["cart"] * 100, 1) if cur_ads["cart"] > 0 else 0) -
                        (round(prev_ads["orders"] / prev_ads["cart"] * 100, 1) if prev_ads["cart"] > 0 else 0), 1),
                    "orders": cur_ads["orders"],
                    "orders_delta": _safe_delta(cur_ads["orders"], prev_ads["orders"]),
                    "conversion": cur_conv,
                    "conversion_delta": round(cur_conv - prev_conv, 1),
                },
            },
            "chart": chart_data,
            "alerts": {
                "warehouses": {"count": len(alerts_warehouses), "items": alerts_warehouses},
                "sales": {"count": len(alerts_sales), "items": alerts_sales},
                "storage": {"count": len(alerts_storage), "total_cost": round(storage_total_cost, 2), "items": alerts_storage},
                "advertising": {"count": len(alerts_advertising), "items": alerts_advertising},
                "finances": {"count": len(alerts_finances), "items": alerts_finances},
            },
            "orders_feed": orders_feed,
            "finance_summary": finance_summary,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("WB Dashboard query failed for shop %s: %s", shop_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки дашборда WB: {str(e)}",
        )

