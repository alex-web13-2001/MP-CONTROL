"""
Dashboard API endpoints.

GET /dashboard/ozon?shop_id=X&period=7d  — Aggregated Ozon dashboard data
GET /dashboard/wb?shop_id=X&period=7d    — Aggregated Wildberries dashboard data
"""
import logging
from datetime import date, timedelta
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
    else:
        return "27"


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


def _parse_period(period: str) -> tuple[date, date, date, date]:
    """Return (current_start, current_end, prev_start, prev_end) dates."""
    days = PERIOD_DAYS.get(period, 7)
    today = date.today()
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
                        WHEN toDate(in_process_at) >= {cur_start:Date} AND toDate(in_process_at) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(in_process_at) >= {prev_start:Date} AND toDate(in_process_at) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price, quantity
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(in_process_at) >= {prev_start:Date}
                  AND toDate(in_process_at) <= {cur_end:Date}
                  AND status NOT IN ('cancelled')
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
                toDate(in_process_at) AS day,
                count() AS orders_count,
                sum(price * quantity) AS revenue
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(in_process_at) >= {start:Date}
              AND toDate(in_process_at) <= {end:Date}
              AND status NOT IN ('cancelled')
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
                      AND toDate(in_process_at) >= {cur_start:Date}
                      AND toDate(in_process_at) <= {cur_end:Date}
                      AND status NOT IN ('cancelled')
                      AND sku > 0
                    GROUP BY sku
                ),
                orders_prev AS (
                    SELECT
                        sku,
                        count() AS orders_count
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(in_process_at) >= {prev_start:Date}
                      AND toDate(in_process_at) <= {prev_end:Date}
                      AND status NOT IN ('cancelled')
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
                           COALESCE(main_image_url, '') AS image_url
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
                    toDate(in_process_at) AS day,
                    sum(price * quantity) AS total_revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(in_process_at) >= {start:Date}
                  AND toDate(in_process_at) <= {end:Date}
                  AND status NOT IN ('cancelled')
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
# Wildberries Dashboard
# ══════════════════════════════════════════════════════════════════

@router.get("/wb")
async def get_wb_dashboard(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("7d", description="Period: today, 7d, 30d"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregated Wildberries dashboard data.

    Returns KPIs (orders, revenue, ad spend, views, clicks, DRR),
    sales chart, ads chart, top products — all in one call.

    Uses the same response format as /dashboard/ozon for frontend reuse.
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

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()

        # ══════════════════════════════════════════════
        # 1. KPI — Orders (fact_orders_raw)
        # ══════════════════════════════════════════════
        orders_kpi = ch.query("""
            SELECT
                period,
                count() AS orders_count,
                sum(price_with_disc) AS revenue,
                sum(price_with_disc) / nullIf(count(), 0) AS avg_check
            FROM (
                SELECT
                    CASE
                        WHEN toDate(date) >= {cur_start:Date} AND toDate(date) <= {cur_end:Date} THEN 'current'
                        WHEN toDate(date) >= {prev_start:Date} AND toDate(date) <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    price_with_disc
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(date) >= {prev_start:Date}
                  AND toDate(date) <= {cur_end:Date}
                  AND is_cancel = 0
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

        orders_map = {
            row[0]: {"count": int(row[1]), "revenue": float(row[2]), "avg_check": float(row[3] or 0)}
            for row in orders_kpi
        }
        cur_orders = orders_map.get("current", {"count": 0, "revenue": 0, "avg_check": 0})
        prev_orders = orders_map.get("previous", {"count": 0, "revenue": 0, "avg_check": 0})

        # ══════════════════════════════════════════════
        # 2. KPI — Advertising (fact_advert_stats_v3)
        # ══════════════════════════════════════════════
        ads_kpi = ch.query("""
            SELECT
                period,
                sum(spend) AS total_spend,
                sum(views) AS total_views,
                sum(clicks) AS total_clicks
            FROM (
                SELECT
                    CASE
                        WHEN date >= {cur_start:Date} AND date <= {cur_end:Date} THEN 'current'
                        WHEN date >= {prev_start:Date} AND date <= {prev_end:Date} THEN 'previous'
                    END AS period,
                    spend, views, clicks
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
                "spend": float(row[1]),
                "views": int(row[2]),
                "clicks": int(row[3]),
            }
        cur_ads = ads_map.get("current", {"spend": 0, "views": 0, "clicks": 0})
        prev_ads = ads_map.get("previous", {"spend": 0, "views": 0, "clicks": 0})

        # DRR = ad_spend / orders_revenue * 100
        cur_drr = round(cur_ads["spend"] / cur_orders["revenue"] * 100, 1) if cur_orders["revenue"] > 0 else 0
        prev_drr = round(prev_ads["spend"] / prev_orders["revenue"] * 100, 1) if prev_orders["revenue"] > 0 else 0

        # ══════════════════════════════════════════════
        # 3. Charts — Sales daily (fact_orders_raw)
        # ══════════════════════════════════════════════
        chart_start = cur_start if period != "today" else cur_start - timedelta(days=29)
        sales_daily = ch.query("""
            SELECT
                toDate(date) AS day,
                count() AS orders_count,
                sum(price_with_disc) AS revenue
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(date) >= {start:Date}
              AND toDate(date) <= {end:Date}
              AND is_cancel = 0
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
        # 4. Charts — Ads daily (fact_advert_stats_v3 + fact_orders_raw)
        # ══════════════════════════════════════════════
        ads_daily = ch.query("""
            WITH ads AS (
                SELECT
                    date AS day,
                    sum(spend) AS spend,
                    sum(views) AS total_views,
                    sum(clicks) AS total_clicks,
                    sum(atbs) AS cart,
                    sum(orders) AS total_orders,
                    sum(revenue) AS ad_revenue
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND date >= {start:Date}
                  AND date <= {end:Date}
                GROUP BY day
            ),
            daily_orders AS (
                SELECT
                    toDate(date) AS day,
                    sum(price_with_disc) AS total_revenue
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(date) >= {start:Date}
                  AND toDate(date) <= {end:Date}
                  AND is_cancel = 0
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

        # ══════════════════════════════════════════════
        # 5. Top products (with ad spend joined in CH)
        # ══════════════════════════════════════════════
        top_products_rows = ch.query("""
            WITH
                orders_cur AS (
                    SELECT
                        nm_id,
                        supplier_article,
                        count() AS orders_count,
                        sum(price_with_disc) AS revenue
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(date) >= {cur_start:Date}
                      AND toDate(date) <= {cur_end:Date}
                      AND is_cancel = 0
                    GROUP BY nm_id, supplier_article
                ),
                orders_prev AS (
                    SELECT
                        nm_id,
                        count() AS orders_count
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(date) >= {prev_start:Date}
                      AND toDate(date) <= {prev_end:Date}
                      AND is_cancel = 0
                    GROUP BY nm_id
                ),
                latest_stocks AS (
                    SELECT
                        nm_id,
                        sum(CASE WHEN NOT startsWith(warehouse_name, 'FBS:') THEN quantity ELSE 0 END) AS stock_fbo,
                        sum(CASE WHEN startsWith(warehouse_name, 'FBS:') THEN quantity ELSE 0 END) AS stock_fbs
                    FROM mms_analytics.fact_inventory_snapshot FINAL
                    WHERE shop_id = {shop_id:UInt32}
                    GROUP BY nm_id
                ),
                ad_per_nm AS (
                    SELECT
                        nm_id,
                        sum(spend) AS ad_spend
                    FROM mms_analytics.fact_advert_stats_v3 FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND date >= {cur_start:Date}
                      AND date <= {cur_end:Date}
                    GROUP BY nm_id
                )
            SELECT
                oc.nm_id,
                oc.supplier_article,
                oc.orders_count,
                oc.revenue,
                CASE WHEN op.orders_count > 0
                    THEN round((oc.orders_count - op.orders_count) / op.orders_count * 100, 1)
                    ELSE 0
                END AS delta_pct,
                coalesce(ls.stock_fbo, 0) AS stock_fbo,
                coalesce(ls.stock_fbs, 0) AS stock_fbs,
                coalesce(apn.ad_spend, 0) AS ad_spend,
                CASE WHEN oc.revenue > 0
                    THEN round(coalesce(apn.ad_spend, 0) / oc.revenue * 100, 1)
                    ELSE 0
                END AS drr
            FROM orders_cur oc
            LEFT JOIN orders_prev op ON oc.nm_id = op.nm_id
            LEFT JOIN latest_stocks ls ON oc.nm_id = ls.nm_id
            LEFT JOIN ad_per_nm apn ON oc.nm_id = apn.nm_id
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
                "offer_id": str(int(row[0])),  # nm_id as offer_id for unified format
                "supplier_article": str(row[1] or ""),
                "name": "",
                "image_url": "",
                "orders": int(row[2]),
                "revenue": float(row[3]),
                "delta_pct": float(row[4]),
                "stock_fbo": int(row[5]),
                "stock_fbs": int(row[6]),
                "price": 0.0,
                "ad_spend": float(row[7]),
                "drr": float(row[8]),
            })

        # Enrich with product names, images & prices from PostgreSQL
        if top_products:
            nm_ids = [int(p["offer_id"]) for p in top_products]
            pg_result = await db.execute(
                text("""
                    SELECT nm_id, name,
                           COALESCE(main_image_url, '') AS image_url,
                           COALESCE(current_price, 0) AS price,
                           COALESCE(vendor_code, '') AS vendor_code
                    FROM dim_products
                    WHERE shop_id = :shop_id
                      AND nm_id = ANY(:nm_ids)
                """),
                {"shop_id": shop_id, "nm_ids": nm_ids},
            )
            pg_map = {}
            for row in pg_result:
                pg_map[int(row[0])] = {
                    "name": row[1] or "",
                    "image_url": row[2],
                    "price": float(row[3]),
                    "vendor_code": row[4],
                }

            for p in top_products:
                nm_id = int(p["offer_id"])
                info = pg_map.get(nm_id, {})
                pg_name = info.get("name") or ""
                # Fallback: use vendor_code or supplier_article if name is empty
                if not pg_name:
                    pg_name = info.get("vendor_code") or p.get("supplier_article") or p["offer_id"]
                p["name"] = pg_name
                p["image_url"] = wb_image_url(nm_id)
                p["price"] = info.get("price", 0.0)
                # Use PG vendor_code as canonical supplier_article if available
                if info.get("vendor_code"):
                    p["supplier_article"] = info["vendor_code"]

        ch.close()

        # ══════════════════════════════════════════════
        # Build response (same format as Ozon)
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
        logger.exception("WB Dashboard query failed for shop %s: %s", shop_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки дашборда WB: {str(e)}",
        )
