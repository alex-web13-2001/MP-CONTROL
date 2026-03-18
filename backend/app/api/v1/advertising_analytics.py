"""
Advertising Analytics API endpoints.

GET /advertising-analytics?shop_id=X&period=7d  — Aggregated advertising analytics

Auto-detects marketplace (Ozon / WB) from shop record
and queries the appropriate ClickHouse tables.
"""
import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/advertising-analytics", tags=["Advertising Analytics"])

PERIOD_DAYS = {
    "today": 1,
    "7d": 7,
    "14d": 14,
    "30d": 30,
    "90d": 90,
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


# ══════════════════════════════════════════════════════════════════
# Main endpoint
# ══════════════════════════════════════════════════════════════════

@router.get("")
async def get_advertising_analytics(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("7d", description="Period: today, 7d, 14d, 30d, 90d"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get aggregated advertising analytics.

    Auto-detects marketplace from shop record.
    Returns KPIs, daily chart, campaigns table, and top SKUs.
    """
    # ── Verify shop ownership ─────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )

    marketplace = shop.marketplace  # "ozon" or "wildberries"
    cur_start, cur_end, prev_start, prev_end = _parse_period(period)

    from app.core.clickhouse import get_clickhouse_client

    try:
        ch = get_clickhouse_client()

        if marketplace == "ozon":
            response = await _build_ozon_analytics(
                ch, db, shop_id, cur_start, cur_end, prev_start, prev_end, period,
            )
        else:
            response = await _build_wb_analytics(
                ch, db, shop_id, cur_start, cur_end, prev_start, prev_end, period,
            )

        ch.close()

        response["shop_id"] = shop_id
        response["marketplace"] = marketplace
        response["period"] = period
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Advertising analytics failed for shop %s: %s", shop_id, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка загрузки аналитики рекламы: {str(e)}",
        )


# ══════════════════════════════════════════════════════════════════
# Ozon Analytics (fact_ozon_ad_daily)
# ══════════════════════════════════════════════════════════════════

async def _build_ozon_analytics(
    ch, db: AsyncSession,
    shop_id: int,
    cur_start: date, cur_end: date,
    prev_start: date, prev_end: date,
    period: str,
) -> dict:
    params = {
        "shop_id": shop_id,
        "cur_start": cur_start, "cur_end": cur_end,
        "prev_start": prev_start, "prev_end": prev_end,
    }

    # ── 1. KPI ────────────────────────────────────────
    kpi_rows = ch.query("""
        SELECT
            period,
            sum(money_spent) AS t_spend,
            sum(t_orders) AS t_orders_sum,
            sum(t_revenue) AS t_revenue_sum,
            sum(t_clicks) AS t_clicks_sum,
            sum(t_views) AS t_views_sum,
            sum(add_to_cart) AS t_cart
        FROM (
            SELECT
                CASE
                    WHEN dt >= {cur_start:Date} AND dt <= {cur_end:Date} THEN 'current'
                    WHEN dt >= {prev_start:Date} AND dt <= {prev_end:Date} THEN 'previous'
                END AS period,
                money_spent,
                orders AS t_orders,
                revenue AS t_revenue,
                clicks AS t_clicks,
                views AS t_views,
                add_to_cart
            FROM mms_analytics.fact_ozon_ad_daily FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {prev_start:Date}
              AND dt <= {cur_end:Date}
        )
        WHERE period != ''
        GROUP BY period
    """, parameters=params).result_rows

    kpi_map = {}
    for row in kpi_rows:
        kpi_map[row[0]] = {
            "spend": float(row[1]),
            "orders": int(row[2]),
            "revenue": float(row[3]),
            "clicks": int(row[4]),
            "views": int(row[5]),
            "cart": int(row[6]),
        }
    cur = kpi_map.get("current", {"spend": 0, "orders": 0, "revenue": 0, "clicks": 0, "views": 0, "cart": 0})
    prev = kpi_map.get("previous", {"spend": 0, "orders": 0, "revenue": 0, "clicks": 0, "views": 0, "cart": 0})

    cur_ctr = round(cur["clicks"] / cur["views"] * 100, 2) if cur["views"] > 0 else 0
    prev_ctr = round(prev["clicks"] / prev["views"] * 100, 2) if prev["views"] > 0 else 0
    cur_cpc = round(cur["spend"] / cur["clicks"], 2) if cur["clicks"] > 0 else 0
    prev_cpc = round(prev["spend"] / prev["clicks"], 2) if prev["clicks"] > 0 else 0
    cur_drr = round(cur["spend"] / cur["revenue"] * 100, 1) if cur["revenue"] > 0 else 0
    prev_drr = round(prev["spend"] / prev["revenue"] * 100, 1) if prev["revenue"] > 0 else 0
    cur_roas = round(cur["revenue"] / cur["spend"], 2) if cur["spend"] > 0 else 0
    prev_roas = round(prev["revenue"] / prev["spend"], 2) if prev["spend"] > 0 else 0
    # Конверсия корзина → заказ
    cur_cr = round(cur["orders"] / cur["cart"] * 100, 1) if cur["cart"] > 0 else 0
    prev_cr = round(prev["orders"] / prev["cart"] * 100, 1) if prev["cart"] > 0 else 0
    # CPO — стоимость заказа
    cur_cpo = round(cur["spend"] / cur["orders"], 2) if cur["orders"] > 0 else 0
    prev_cpo = round(prev["spend"] / prev["orders"], 2) if prev["orders"] > 0 else 0
    # ROMI — (revenue - spend) / spend
    cur_romi = round((cur["revenue"] - cur["spend"]) / cur["spend"] * 100, 1) if cur["spend"] > 0 else 0
    prev_romi = round((prev["revenue"] - prev["spend"]) / prev["spend"] * 100, 1) if prev["spend"] > 0 else 0

    # ── Total DRR: ad_spend / total_revenue (from fact_ozon_orders) ──
    total_rev_rows = ch.query("""
        SELECT
            period,
            sum(t_rev) AS total_revenue
        FROM (
            SELECT
                CASE
                    WHEN dt >= {cur_start:Date} AND dt <= {cur_end:Date} THEN 'current'
                    WHEN dt >= {prev_start:Date} AND dt <= {prev_end:Date} THEN 'previous'
                END AS period,
                price * quantity AS t_rev
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND dt >= {prev_start:Date}
              AND dt <= {cur_end:Date}
              AND status NOT IN ('cancelled')
        )
        WHERE period != ''
        GROUP BY period
    """, parameters=params).result_rows
    total_rev_map = {row[0]: float(row[1]) for row in total_rev_rows}
    cur_total_rev = total_rev_map.get("current", 0)
    prev_total_rev = total_rev_map.get("previous", 0)
    cur_total_drr = round(cur["spend"] / cur_total_rev * 100, 1) if cur_total_rev > 0 else 0
    prev_total_drr = round(prev["spend"] / prev_total_rev * 100, 1) if prev_total_rev > 0 else 0

    kpi = {
        "spend": cur["spend"],
        "spend_delta": _safe_delta(cur["spend"], prev["spend"]),
        "views": cur["views"],
        "views_delta": _safe_delta(cur["views"], prev["views"]),
        "clicks": cur["clicks"],
        "clicks_delta": _safe_delta(cur["clicks"], prev["clicks"]),
        "ctr": cur_ctr,
        "ctr_delta": round(cur_ctr - prev_ctr, 2),
        "cart": cur["cart"],
        "cart_delta": _safe_delta(cur["cart"], prev["cart"]),
        "orders": cur["orders"],
        "orders_delta": _safe_delta(cur["orders"], prev["orders"]),
        "conversion_rate": cur_cr,
        "conversion_rate_delta": round(cur_cr - prev_cr, 1),
        "cpo": cur_cpo,
        "cpo_delta": round(cur_cpo - prev_cpo, 2),
        "drr": cur_drr,
        "drr_delta": round(cur_drr - prev_drr, 1),
        "total_drr": cur_total_drr,
        "total_drr_delta": round(cur_total_drr - prev_total_drr, 1),
        "romi": cur_romi,
        "romi_delta": round(cur_romi - prev_romi, 1),
        "revenue": cur["revenue"],
        "revenue_delta": _safe_delta(cur["revenue"], prev["revenue"]),
        "avg_cpc": cur_cpc,
        "avg_cpc_delta": round(cur_cpc - prev_cpc, 2),
        "roas": cur_roas,
        "roas_delta": round(cur_roas - prev_roas, 2),
    }

    # ── 2. Daily Chart ────────────────────────────────
    chart_start = cur_start if period != "today" else cur_start - timedelta(days=29)
    chart_rows = ch.query("""
        SELECT
            dt AS day,
            sum(money_spent) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(add_to_cart) AS t_cart,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(money_spent) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {start:Date}
          AND dt <= {end:Date}
        GROUP BY day
        ORDER BY day
    """, parameters={
        "shop_id": shop_id,
        "start": chart_start,
        "end": cur_end,
    }).result_rows

    chart_daily = [
        {
            "date": str(row[0]),
            "spend": round(float(row[1]), 2),
            "views": int(row[2]),
            "clicks": int(row[3]),
            "cart": int(row[4]),
            "orders": int(row[5]),
            "revenue": round(float(row[6]), 2),
            "ctr": float(row[7]),
            "drr": float(row[8]),
        }
        for row in chart_rows
    ]

    # ── 3. Campaigns Table ────────────────────────────
    campaigns_rows = ch.query("""
        SELECT
            campaign_id,
            sum(money_spent) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN sum(clicks) > 0
                THEN round(sum(money_spent) / sum(clicks), 2) ELSE 0
            END AS t_avg_cpc,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(money_spent) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {cur_start:Date}
          AND dt <= {cur_end:Date}
        GROUP BY campaign_id
        ORDER BY t_spend DESC
    """, parameters=params).result_rows

    campaigns_table = [
        {
            "campaign_id": int(row[0]),
            "spend": round(float(row[1]), 2),
            "views": int(row[2]),
            "clicks": int(row[3]),
            "orders": int(row[4]),
            "revenue": round(float(row[5]), 2),
            "ctr": float(row[6]),
            "avg_cpc": float(row[7]),
            "drr": float(row[8]),
        }
        for row in campaigns_rows
    ]

    # ── 4. Top SKUs ───────────────────────────────────
    top_skus_rows = ch.query("""
        SELECT
            sku,
            sum(money_spent) AS t_spend,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(money_spent) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {cur_start:Date}
          AND dt <= {cur_end:Date}
        GROUP BY sku
        ORDER BY t_spend DESC
        LIMIT 30
    """, parameters=params).result_rows

    top_skus = []
    for row in top_skus_rows:
        top_skus.append({
            "sku": int(row[0]),
            "offer_id": "",
            "name": "",
            "image_url": "",
            "spend": round(float(row[1]), 2),
            "orders": int(row[2]),
            "revenue": round(float(row[3]), 2),
            "drr": float(row[4]),
        })

    # Enrich from PostgreSQL dim_ozon_products
    if top_skus:
        from sqlalchemy import text
        sku_list = [s["sku"] for s in top_skus]
        pg_result = await db.execute(
            text("""
                SELECT product_id, offer_id, name,
                       COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
                FROM dim_ozon_products
                WHERE shop_id = :shop_id
                  AND product_id = ANY(:product_ids)
            """),
            {"shop_id": shop_id, "product_ids": sku_list},
        )
        pg_map = {}
        for row in pg_result:
            pg_map[int(row[0])] = {
                "offer_id": row[1] or "",
                "name": row[2] or "",
                "image_url": row[3] or "",
            }
        for s in top_skus:
            info = pg_map.get(s["sku"], {})
            s["offer_id"] = info.get("offer_id", str(s["sku"]))
            s["name"] = info.get("name", "")
            s["image_url"] = info.get("image_url", "")

    return {
        "kpi": kpi,
        "chart_daily": chart_daily,
        "campaigns_table": campaigns_table,
        "top_skus": top_skus,
    }


# ══════════════════════════════════════════════════════════════════
# Wildberries Analytics (fact_advert_stats_v3)
# ══════════════════════════════════════════════════════════════════

async def _build_wb_analytics(
    ch, db: AsyncSession,
    shop_id: int,
    cur_start: date, cur_end: date,
    prev_start: date, prev_end: date,
    period: str,
) -> dict:
    params = {
        "shop_id": shop_id,
        "cur_start": cur_start, "cur_end": cur_end,
        "prev_start": prev_start, "prev_end": prev_end,
    }

    # ── 1. KPI ────────────────────────────────────────
    kpi_rows = ch.query("""
        SELECT
            period,
            sum(t_spend) AS t_spend_sum,
            sum(t_orders) AS t_orders_sum,
            sum(t_revenue) AS t_revenue_sum,
            sum(t_clicks) AS t_clicks_sum,
            sum(t_views) AS t_views_sum,
            sum(t_atbs) AS t_cart
        FROM (
            SELECT
                CASE
                    WHEN date >= {cur_start:Date} AND date <= {cur_end:Date} THEN 'current'
                    WHEN date >= {prev_start:Date} AND date <= {prev_end:Date} THEN 'previous'
                END AS period,
                spend AS t_spend,
                orders AS t_orders,
                revenue AS t_revenue,
                clicks AS t_clicks,
                views AS t_views,
                atbs AS t_atbs
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {prev_start:Date}
              AND date <= {cur_end:Date}
        )
        WHERE period != ''
        GROUP BY period
    """, parameters=params).result_rows

    kpi_map = {}
    for row in kpi_rows:
        kpi_map[row[0]] = {
            "spend": float(row[1]),
            "orders": int(row[2]),
            "revenue": float(row[3]),
            "clicks": int(row[4]),
            "views": int(row[5]),
            "cart": int(row[6]),
        }
    cur = kpi_map.get("current", {"spend": 0, "orders": 0, "revenue": 0, "clicks": 0, "views": 0, "cart": 0})
    prev = kpi_map.get("previous", {"spend": 0, "orders": 0, "revenue": 0, "clicks": 0, "views": 0, "cart": 0})

    cur_ctr = round(cur["clicks"] / cur["views"] * 100, 2) if cur["views"] > 0 else 0
    prev_ctr = round(prev["clicks"] / prev["views"] * 100, 2) if prev["views"] > 0 else 0
    cur_cpc = round(cur["spend"] / cur["clicks"], 2) if cur["clicks"] > 0 else 0
    prev_cpc = round(prev["spend"] / prev["clicks"], 2) if prev["clicks"] > 0 else 0
    cur_drr = round(cur["spend"] / cur["revenue"] * 100, 1) if cur["revenue"] > 0 else 0
    prev_drr = round(prev["spend"] / prev["revenue"] * 100, 1) if prev["revenue"] > 0 else 0
    cur_roas = round(cur["revenue"] / cur["spend"], 2) if cur["spend"] > 0 else 0
    prev_roas = round(prev["revenue"] / prev["spend"], 2) if prev["spend"] > 0 else 0
    # Конверсия корзина → заказ
    cur_cr = round(cur["orders"] / cur["cart"] * 100, 1) if cur["cart"] > 0 else 0
    prev_cr = round(prev["orders"] / prev["cart"] * 100, 1) if prev["cart"] > 0 else 0
    # CPO — стоимость заказа
    cur_cpo = round(cur["spend"] / cur["orders"], 2) if cur["orders"] > 0 else 0
    prev_cpo = round(prev["spend"] / prev["orders"], 2) if prev["orders"] > 0 else 0
    # ROMI — (revenue - spend) / spend
    cur_romi = round((cur["revenue"] - cur["spend"]) / cur["spend"] * 100, 1) if cur["spend"] > 0 else 0
    prev_romi = round((prev["revenue"] - prev["spend"]) / prev["spend"] * 100, 1) if prev["spend"] > 0 else 0

    # ── Total DRR: ad_spend / total_revenue (from fact_orders_raw) ──
    total_rev_rows = ch.query("""
        SELECT
            period,
            sum(t_rev) AS total_revenue
        FROM (
            SELECT
                CASE
                    WHEN date >= {cur_start:Date} AND date <= {cur_end:Date} THEN 'current'
                    WHEN date >= {prev_start:Date} AND date <= {prev_end:Date} THEN 'previous'
                END AS period,
                finishedPrice * 100 AS t_rev
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {prev_start:Date}
              AND date <= {cur_end:Date}
              AND isCancel = 0
        )
        WHERE period != ''
        GROUP BY period
    """, parameters=params).result_rows
    total_rev_map = {row[0]: float(row[1]) for row in total_rev_rows}
    cur_total_rev = total_rev_map.get("current", 0)
    prev_total_rev = total_rev_map.get("previous", 0)
    # WB spend is in kopecks, revenue in kopecks — need consistent units
    # fact_advert_stats_v3.spend is in rubles, finishedPrice is in rubles
    cur_total_drr = round(cur["spend"] / cur_total_rev * 100, 1) if cur_total_rev > 0 else 0
    prev_total_drr = round(prev["spend"] / prev_total_rev * 100, 1) if prev_total_rev > 0 else 0

    kpi = {
        "spend": cur["spend"],
        "spend_delta": _safe_delta(cur["spend"], prev["spend"]),
        "views": cur["views"],
        "views_delta": _safe_delta(cur["views"], prev["views"]),
        "clicks": cur["clicks"],
        "clicks_delta": _safe_delta(cur["clicks"], prev["clicks"]),
        "ctr": cur_ctr,
        "ctr_delta": round(cur_ctr - prev_ctr, 2),
        "cart": cur["cart"],
        "cart_delta": _safe_delta(cur["cart"], prev["cart"]),
        "orders": cur["orders"],
        "orders_delta": _safe_delta(cur["orders"], prev["orders"]),
        "conversion_rate": cur_cr,
        "conversion_rate_delta": round(cur_cr - prev_cr, 1),
        "cpo": cur_cpo,
        "cpo_delta": round(cur_cpo - prev_cpo, 2),
        "drr": cur_drr,
        "drr_delta": round(cur_drr - prev_drr, 1),
        "total_drr": cur_total_drr,
        "total_drr_delta": round(cur_total_drr - prev_total_drr, 1),
        "romi": cur_romi,
        "romi_delta": round(cur_romi - prev_romi, 1),
        "revenue": cur["revenue"],
        "revenue_delta": _safe_delta(cur["revenue"], prev["revenue"]),
        "avg_cpc": cur_cpc,
        "avg_cpc_delta": round(cur_cpc - prev_cpc, 2),
        "roas": cur_roas,
        "roas_delta": round(cur_roas - prev_roas, 2),
    }

    # ── 2. Daily Chart ────────────────────────────────
    chart_start = cur_start if period != "today" else cur_start - timedelta(days=29)
    chart_rows = ch.query("""
        SELECT
            date AS day,
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(atbs) AS t_cart,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(spend) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {start:Date}
          AND date <= {end:Date}
        GROUP BY day
        ORDER BY day
    """, parameters={
        "shop_id": shop_id,
        "start": chart_start,
        "end": cur_end,
    }).result_rows

    chart_daily = [
        {
            "date": str(row[0]),
            "spend": round(float(row[1]), 2),
            "views": int(row[2]),
            "clicks": int(row[3]),
            "cart": int(row[4]),
            "orders": int(row[5]),
            "revenue": round(float(row[6]), 2),
            "ctr": float(row[7]),
            "drr": float(row[8]),
        }
        for row in chart_rows
    ]

    # ── 3. Campaigns Table ────────────────────────────
    campaigns_rows = ch.query("""
        SELECT
            campaign_id,
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN sum(clicks) > 0
                THEN round(sum(spend) / sum(clicks), 2) ELSE 0
            END AS t_avg_cpc,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(spend) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {cur_start:Date}
          AND date <= {cur_end:Date}
        GROUP BY campaign_id
        ORDER BY t_spend DESC
    """, parameters=params).result_rows

    campaigns_table = [
        {
            "campaign_id": int(row[0]),
            "spend": round(float(row[1]), 2),
            "views": int(row[2]),
            "clicks": int(row[3]),
            "orders": int(row[4]),
            "revenue": round(float(row[5]), 2),
            "ctr": float(row[6]),
            "avg_cpc": float(row[7]),
            "drr": float(row[8]),
        }
        for row in campaigns_rows
    ]

    # ── 4. Top SKUs (nm_id) ───────────────────────────
    top_skus_rows = ch.query("""
        SELECT
            nm_id,
            sum(spend) AS t_spend,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue,
            CASE WHEN sum(revenue) > 0
                THEN round(sum(spend) / sum(revenue) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {cur_start:Date}
          AND date <= {cur_end:Date}
        GROUP BY nm_id
        ORDER BY t_spend DESC
        LIMIT 30
    """, parameters=params).result_rows

    top_skus = []
    for row in top_skus_rows:
        nm_id = int(row[0])
        top_skus.append({
            "sku": nm_id,
            "offer_id": str(nm_id),
            "name": "",
            "image_url": "",
            "spend": round(float(row[1]), 2),
            "orders": int(row[2]),
            "revenue": round(float(row[3]), 2),
            "drr": float(row[4]),
        })

    # Enrich from PostgreSQL dim_products
    if top_skus:
        from sqlalchemy import text
        nm_ids = [s["sku"] for s in top_skus]
        pg_result = await db.execute(
            text("""
                SELECT nm_id, supplier_article, name
                FROM dim_products
                WHERE shop_id = :shop_id
                  AND nm_id = ANY(:nm_ids)
            """),
            {"shop_id": shop_id, "nm_ids": nm_ids},
        )
        pg_map = {}
        for row in pg_result:
            pg_map[int(row[0])] = {
                "supplier_article": row[1] or "",
                "name": row[2] or "",
            }

        # WB image URLs
        for s in top_skus:
            info = pg_map.get(s["sku"], {})
            s["name"] = info.get("name", "")
            s["offer_id"] = info.get("supplier_article", str(s["sku"]))
            # Generate WB CDN image URL
            s["image_url"] = _wb_image_url(s["sku"])

    return {
        "kpi": kpi,
        "chart_daily": chart_daily,
        "campaigns_table": campaigns_table,
        "top_skus": top_skus,
    }


def _wb_basket_host(vol: int) -> str:
    """Determine WB CDN basket host number from vol."""
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


def _wb_image_url(nm_id: int) -> str:
    """Generate WB CDN image URL from nm_id."""
    vol = nm_id // 100000
    part = nm_id // 1000
    basket = _wb_basket_host(vol)
    return f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/big/1.webp"
