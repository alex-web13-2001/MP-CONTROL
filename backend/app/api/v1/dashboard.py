"""
Dashboard API endpoints.

GET /dashboard/ozon?shop_id=X&period=7d  — Aggregated Ozon dashboard data
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
        # 4. Top products
        # ══════════════════════════════════════════════
        top_products_rows = ch.query("""
            WITH
                orders_cur AS (
                    SELECT
                        offer_id,
                        count() AS orders_count,
                        sum(price * quantity) AS revenue
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(in_process_at) >= {cur_start:Date}
                      AND toDate(in_process_at) <= {cur_end:Date}
                      AND status NOT IN ('cancelled')
                    GROUP BY offer_id
                ),
                orders_prev AS (
                    SELECT
                        offer_id,
                        count() AS orders_count,
                        sum(price * quantity) AS revenue
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(in_process_at) >= {prev_start:Date}
                      AND toDate(in_process_at) <= {prev_end:Date}
                      AND status NOT IN ('cancelled')
                    GROUP BY offer_id
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
                coalesce(lp.price, 0) AS price
            FROM orders_cur oc
            LEFT JOIN orders_prev op ON oc.offer_id = op.offer_id
            LEFT JOIN latest_stocks ls ON oc.offer_id = ls.offer_id
            LEFT JOIN latest_prices lp ON oc.offer_id = lp.offer_id
            ORDER BY oc.revenue DESC
            LIMIT 10
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
            "prev_start": prev_start, "prev_end": prev_end,
        }).result_rows

        # Ad spend per SKU
        ad_spend_rows = ch.query("""
            SELECT
                toString(sku) AS sku_str,
                sum(money_spent) AS ad_spend,
                sum(revenue) AS ad_revenue
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {cur_start:Date}
              AND dt <= {cur_end:Date}
            GROUP BY sku_str
        """, parameters={
            "shop_id": shop_id,
            "cur_start": cur_start, "cur_end": cur_end,
        }).result_rows
        ad_spend_map = {row[0]: {"spend": float(row[1]), "revenue": float(row[2])} for row in ad_spend_rows}

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
                "ad_spend": 0.0,
                "drr": 0.0,
            })

        # Enrich with product names, images & ad spend from PostgreSQL
        if top_products:
            offer_ids = [p["offer_id"] for p in top_products]
            pg_result = await db.execute(
                text("""
                    SELECT offer_id, name, sku,
                           COALESCE(main_image_url, '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :shop_id
                      AND offer_id = ANY(:offer_ids)
                """),
                {"shop_id": shop_id, "offer_ids": offer_ids},
            )
            pg_map = {}
            for row in pg_result:
                pg_map[row[0]] = {"name": row[1], "sku": str(row[2]) if row[2] else "", "image_url": row[3]}

            for p in top_products:
                info = pg_map.get(p["offer_id"], {})
                p["name"] = info.get("name", p["offer_id"])
                p["image_url"] = info.get("image_url", "")
                sku_str = info.get("sku", "")
                if sku_str and sku_str in ad_spend_map:
                    ad = ad_spend_map[sku_str]
                    p["ad_spend"] = ad["spend"]
                    p["drr"] = round(ad["spend"] / p["revenue"] * 100, 1) if p["revenue"] > 0 else 0

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
