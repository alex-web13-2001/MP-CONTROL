"""
Advertising Analytics API endpoints.

GET /advertising-analytics?shop_id=X&period=7d  — Aggregated advertising analytics

Auto-detects marketplace (Ozon / WB) from shop record
and queries the appropriate ClickHouse tables.
"""
import json
import logging
from collections import defaultdict
from datetime import date, datetime as dt_datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, text as sa_text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.shop import Shop
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/advertising-analytics", tags=["Advertising Analytics"])

# Relevant event types for the ad chart overlay
AD_CHART_EVENT_TYPES = [
    # Advertising
    "BID_CHANGE", "STATUS_CHANGE", "ITEM_ADD", "ITEM_REMOVE", "ITEM_INACTIVE",
    "CAMPAIGN_CREATED", "OZON_BID_CHANGE", "OZON_STATUS_CHANGE",
    "OZON_BUDGET_CHANGE", "OZON_ITEM_ADD", "OZON_ITEM_REMOVE", "OZON_CAMPAIGN_CREATED",
    # Content
    "OZON_SEO_CHANGE", "OZON_PHOTO_CHANGE", "OZON_CONTENT_CHANGE",
    "CONTENT_CHANGE", "CONTENT_TITLE_CHANGED", "CONTENT_DESC_CHANGED",
    "CONTENT_MAIN_PHOTO_CHANGED", "CONTENT_PHOTO_ADDED", "CONTENT_PHOTO_REMOVED",
    # Price
    "PRICE_CHANGE", "OZON_PRICE_CHANGE",
    # Stock
    "OZON_STOCK_OUT", "OZON_STOCK_REPLENISH", "STOCK_OUT", "STOCK_REPLENISH",
    "STOCK_OUT_FBO_TOTAL", "STOCK_OUT_FBS_TOTAL",
]

EVENT_TYPE_TO_CATEGORY = {
    "BID_CHANGE": "advertising", "STATUS_CHANGE": "advertising",
    "ITEM_ADD": "advertising", "ITEM_REMOVE": "advertising",
    "ITEM_INACTIVE": "advertising", "CAMPAIGN_CREATED": "advertising",
    "OZON_BID_CHANGE": "advertising", "OZON_STATUS_CHANGE": "advertising",
    "OZON_BUDGET_CHANGE": "advertising", "OZON_ITEM_ADD": "advertising",
    "OZON_ITEM_REMOVE": "advertising", "OZON_CAMPAIGN_CREATED": "advertising",
    "OZON_SEO_CHANGE": "content", "OZON_PHOTO_CHANGE": "content",
    "OZON_CONTENT_CHANGE": "content", "CONTENT_CHANGE": "content",
    "CONTENT_TITLE_CHANGED": "content", "CONTENT_DESC_CHANGED": "content",
    "CONTENT_MAIN_PHOTO_CHANGED": "content", "CONTENT_PHOTO_ADDED": "content",
    "CONTENT_PHOTO_REMOVED": "content",
    "PRICE_CHANGE": "price", "OZON_PRICE_CHANGE": "price",
    "OZON_STOCK_OUT": "stock", "OZON_STOCK_REPLENISH": "stock",
    "STOCK_OUT": "stock", "STOCK_REPLENISH": "stock",
    "STOCK_OUT_FBO_TOTAL": "stock", "STOCK_OUT_FBS_TOTAL": "stock",
}

PERIOD_DAYS = {
    "today": 1,
    "7d": 7,
    "14d": 14,
    "30d": 30,
    "90d": 90,
}


def _parse_period(period: str, date_from: date | None = None, date_to: date | None = None) -> tuple[date, date, date, date]:
    """Return (current_start, current_end, prev_start, prev_end) dates."""
    if date_from and date_to:
        # Custom date range
        days = (date_to - date_from).days + 1
        current_start = date_from
        current_end = date_to
        prev_end = current_start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days - 1)
        return current_start, current_end, prev_start, prev_end
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
    date_from: Optional[str] = Query(None, description="Custom start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Custom end date YYYY-MM-DD"),
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
    # Parse custom dates if provided
    _date_from = None
    _date_to = None
    if date_from and date_to:
        try:
            _date_from = date.fromisoformat(date_from)
            _date_to = date.fromisoformat(date_to)
            period = "custom"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format, use YYYY-MM-DD")
    cur_start, cur_end, prev_start, prev_end = _parse_period(period, _date_from, _date_to)

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
# Events Detail endpoint (for modal drill-down by day)
# ══════════════════════════════════════════════════════════════════

EVENT_LABELS = {
    "OZON_BID_CHANGE": "Изменение ставки",
    "OZON_STATUS_CHANGE": "Статус кампании изменён",
    "OZON_BUDGET_CHANGE": "Бюджет кампании изменён",
    "OZON_ITEM_ADD": "Товар добавлен в кампанию",
    "OZON_ITEM_REMOVE": "Товар удалён из кампании",
    "OZON_CAMPAIGN_CREATED": "Новая кампания",
    "OZON_SEO_CHANGE": "SEO-контент изменён",
    "OZON_PHOTO_CHANGE": "Изменение фото",
    "OZON_CONTENT_CHANGE": "Контент изменён",
    "OZON_PRICE_CHANGE": "Цена изменена",
    "OZON_STOCK_OUT": "Товар закончился",
    "OZON_STOCK_REPLENISH": "Поступление на склад",
    "BID_CHANGE": "Изменение ставки",
    "STATUS_CHANGE": "Статус кампании изменён",
    "ITEM_ADD": "Товар добавлен в кампанию",
    "ITEM_REMOVE": "Товар удалён из кампании",
    "CAMPAIGN_CREATED": "Новая кампания",
    "CONTENT_CHANGE": "Изменение контента",
    "CONTENT_TITLE_CHANGED": "Заголовок изменён",
    "CONTENT_DESC_CHANGED": "Описание изменено",
    "CONTENT_MAIN_PHOTO_CHANGED": "Главное фото изменено",
    "CONTENT_PHOTO_ADDED": "Фото добавлено",
    "CONTENT_PHOTO_REMOVED": "Фото удалено",
    "PRICE_CHANGE": "Цена изменена",
    "STOCK_OUT": "Товар закончился",
    "STOCK_REPLENISH": "Поступление на склад",
    "STOCK_OUT_FBO_TOTAL": "Нет остатков ФБО",
    "STOCK_OUT_FBS_TOTAL": "Нет остатков ФБС",
}


@router.get("/events-detail")
async def get_events_detail(
    shop_id: int = Query(..., description="Shop ID"),
    event_date: str = Query(..., description="Date YYYY-MM-DD"),
    category: Optional[str] = Query(None, description="Category filter"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get detailed events for a specific day — used for modal drill-down on chart.
    Returns events enriched with product data and campaign titles.
    """
    # Verify shop ownership
    shop_result = await db.execute(
        sa_text("SELECT id, marketplace FROM shops WHERE id = :shop_id AND user_id = :user_id"),
        {"shop_id": shop_id, "user_id": str(current_user.id)},
    )
    shop_row = shop_result.fetchone()
    if not shop_row:
        raise HTTPException(status_code=404, detail="Магазин не найден")
    marketplace = shop_row[1]

    try:
        target_date = date.fromisoformat(event_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    date_from = dt_datetime.combine(target_date, dt_datetime.min.time())
    date_to = dt_datetime.combine(target_date + timedelta(days=1), dt_datetime.min.time())

    # Build type filter
    type_filter = AD_CHART_EVENT_TYPES
    if category and category in EVENT_TYPE_TO_CATEGORY.values():
        type_filter = [k for k, v in EVENT_TYPE_TO_CATEGORY.items() if v == category]

    # Fetch events
    events_result = await db.execute(
        sa_text("""
            SELECT id, created_at, event_type, advert_id, nm_id,
                   old_value, new_value, event_metadata
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at >= :date_from
              AND created_at < :date_to
              AND event_type = ANY(:event_types)
            ORDER BY created_at DESC
        """),
        {"shop_id": shop_id, "date_from": date_from, "date_to": date_to,
         "event_types": type_filter},
    )
    raw_events = events_result.fetchall()

    if not raw_events:
        return {"date": event_date, "events": [], "total": 0}

    # Collect product IDs for enrichment
    nm_ids = set()
    for ev in raw_events:
        if ev[4]:
            nm_ids.add(int(ev[4]))

    # Enrich with product data
    product_map = {}
    if nm_ids:
        if marketplace == "ozon":
            pg_result = await db.execute(
                sa_text("""
                    SELECT product_id, sku, name, offer_id,
                           COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :shop_id
                      AND (sku = ANY(:nm_ids) OR product_id = ANY(:nm_ids))
                """),
                {"shop_id": shop_id, "nm_ids": list(nm_ids)},
            )
            for row in pg_result:
                info = {"name": row[2] or "", "offer_id": row[3] or "", "image_url": row[4] or ""}
                product_map[int(row[0])] = info
                if row[1]:
                    product_map[int(row[1])] = info
        else:
            pg_result = await db.execute(
                sa_text("""
                    SELECT nm_id, name, vendor_code, main_image_url
                    FROM dim_products
                    WHERE shop_id = :shop_id AND nm_id = ANY(:nm_ids)
                """),
                {"shop_id": shop_id, "nm_ids": list(nm_ids)},
            )
            for row in pg_result:
                product_map[int(row[0])] = {
                    "name": row[1] or "", "offer_id": row[2] or "", "image_url": row[3] or "",
                }

    # Build response
    events_list = []
    for ev in raw_events:
        ev_id, created_at, event_type, advert_id, nm_id, old_value, new_value, metadata = ev

        meta = {}
        if metadata:
            if isinstance(metadata, str):
                try:
                    meta = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(metadata, dict):
                meta = metadata

        cat = EVENT_TYPE_TO_CATEGORY.get(event_type, "other")
        label = EVENT_LABELS.get(event_type, event_type)

        product = None
        if nm_id:
            nm_int = int(nm_id)
            prod_info = product_map.get(nm_int, {})
            product = {
                "nm_id": nm_int,
                "name": prod_info.get("name", ""),
                "offer_id": prod_info.get("offer_id", ""),
                "image_url": prod_info.get("image_url", ""),
            }

        # Format detail string
        detail = ""
        if event_type in ("OZON_BID_CHANGE", "BID_CHANGE"):
            old_fmt = f"{float(old_value):.2f} ₽" if old_value else ""
            new_fmt = f"{float(new_value):.2f} ₽" if new_value else ""
            bid_field = meta.get("bid_field", "")
            prefix = {"search": "Поиск", "recommendation": "Рекомендации"}.get(bid_field, "")
            detail = f"{prefix}: {old_fmt} → {new_fmt}" if prefix else f"{old_fmt} → {new_fmt}"
        elif event_type in ("OZON_BUDGET_CHANGE",):
            old_fmt = f"{float(old_value):,.0f} ₽" if old_value else ""
            new_fmt = f"{float(new_value):,.0f} ₽" if new_value else ""
            detail = f"{old_fmt} → {new_fmt}"
        elif event_type in ("OZON_STATUS_CHANGE", "STATUS_CHANGE"):
            status_labels = {
                "CAMPAIGN_STATE_RUNNING": "Активна", "CAMPAIGN_STATE_STOPPED": "Остановлена",
                "CAMPAIGN_STATE_INACTIVE": "Неактивна", "9": "Активна", "11": "Остановлена",
            }
            detail = f"{status_labels.get(old_value, old_value or '')} → {status_labels.get(new_value, new_value or '')}"
        elif event_type in ("OZON_PRICE_CHANGE", "PRICE_CHANGE"):
            old_fmt = f"{float(old_value):,.0f} ₽" if old_value else ""
            new_fmt = f"{float(new_value):,.0f} ₽" if new_value else ""
            detail = f"{old_fmt} → {new_fmt}"
        elif event_type in ("OZON_STOCK_OUT", "STOCK_OUT"):
            wh = meta.get("warehouse_name", "")
            detail = f"Остаток: {old_value} → 0" + (f" ({wh})" if wh else "")
        elif event_type in ("OZON_STOCK_REPLENISH", "STOCK_REPLENISH"):
            wh = meta.get("warehouse_name", "")
            delta = meta.get("delta", new_value or "")
            detail = f"+{delta} шт." + (f" ({wh})" if wh else "")
        elif event_type in ("STOCK_OUT_FBO_TOTAL", "STOCK_OUT_FBS_TOTAL"):
            detail = f"Было {old_value} шт. → 0"
        elif "CONTENT" in event_type or "SEO" in event_type or "PHOTO" in event_type:
            field = meta.get("field", "")
            detail = meta.get("detail", field or "Изменение контента")

        campaign_title = meta.get("campaign_title", "") or meta.get("title", "")

        events_list.append({
            "id": ev_id,
            "time": created_at.strftime("%H:%M") if created_at else "",
            "event_type": event_type,
            "category": cat,
            "label": label,
            "detail": detail,
            "campaign_id": int(advert_id) if advert_id else None,
            "campaign_title": campaign_title,
            "product": product,
        })

    return {
        "date": event_date,
        "total": len(events_list),
        "events": events_list,
    }


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
                orders + model_orders AS t_orders,
                revenue + model_revenue AS t_revenue,
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
    # Конверсия клик → корзина
    cur_cart_rate = round(cur["cart"] / cur["clicks"] * 100, 1) if cur["clicks"] > 0 else 0
    prev_cart_rate = round(prev["cart"] / prev["clicks"] * 100, 1) if prev["clicks"] > 0 else 0
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
                    WHEN toDate(addHours(in_process_at, 3)) >= {cur_start:Date} AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date} THEN 'current'
                    WHEN toDate(addHours(in_process_at, 3)) >= {prev_start:Date} AND toDate(addHours(in_process_at, 3)) <= {prev_end:Date} THEN 'previous'
                END AS period,
                price * quantity AS t_rev
            FROM mms_analytics.fact_ozon_orders FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(in_process_at, 3)) >= {prev_start:Date}
              AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
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
        "cart_rate": cur_cart_rate,
        "cart_rate_delta": round(cur_cart_rate - prev_cart_rate, 1),
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
            sum(orders) + sum(model_orders) AS t_orders,
            sum(revenue) + sum(model_revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN (sum(revenue) + sum(model_revenue)) > 0
                THEN round(sum(money_spent) / (sum(revenue) + sum(model_revenue)) * 100, 1) ELSE 0
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
            "total_drr": 0.0,
        }
        for row in chart_rows
    ]

    # Fetch daily total revenue from fact_ozon_orders for total_drr
    total_rev_daily_rows = ch.query("""
        SELECT
            toDate(addHours(in_process_at, 3)) AS day,
            sum(price * quantity) AS total_revenue
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
    total_rev_by_day = {str(row[0]): float(row[1]) for row in total_rev_daily_rows}
    for point in chart_daily:
        total_rev = total_rev_by_day.get(point["date"], 0)
        if total_rev > 0 and point["spend"] > 0:
            point["total_drr"] = round(point["spend"] / total_rev * 100, 1)

    # ── 3. Campaigns Table ────────────────────────────
    campaigns_rows = ch.query("""
        SELECT
            campaign_id,
            sum(money_spent) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(add_to_cart) AS t_cart,
            sum(orders) AS t_direct_orders,
            sum(model_orders) AS t_model_orders,
            sum(revenue) AS t_direct_revenue,
            sum(model_revenue) AS t_model_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN sum(clicks) > 0
                THEN round(sum(money_spent) / sum(clicks), 2) ELSE 0
            END AS t_avg_cpc,
            CASE WHEN (sum(revenue) + sum(model_revenue)) > 0
                THEN round(sum(money_spent) / (sum(revenue) + sum(model_revenue)) * 100, 1) ELSE 0
            END AS t_drr,
            uniqExact(sku) AS t_sku_count
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {cur_start:Date}
          AND dt <= {cur_end:Date}
        GROUP BY campaign_id
        ORDER BY t_spend DESC
    """, parameters=params).result_rows

    campaign_ids = [int(row[0]) for row in campaigns_rows]

    # Enrich campaign info
    campaign_info_map: dict = {}
    
    if campaign_ids:
        pg_result = await db.execute(
            sa_text("""
                SELECT c.campaign_id, c.title, c.state AS status, c.campaign_type,
                       p.sku, p.bid
                FROM dim_ozon_campaigns c
                LEFT JOIN dim_ozon_campaign_products p 
                       ON c.shop_id = p.shop_id AND c.campaign_id = p.campaign_id
                WHERE c.shop_id = :shop_id AND c.campaign_id = ANY(:cids)
            """),
            {"shop_id": shop_id, "cids": campaign_ids}
        )
        for row in pg_result:
            cid = row[0]
            if cid not in campaign_info_map:
                campaign_info_map[cid] = {
                    "title": row[1] or "",
                    "status": row[2] or "",
                    "campaign_type": row[3] or "",
                    "bids": {},
                }
            sku = row[4]
            bid = row[5]
            if sku and bid is not None:
                campaign_info_map[cid]["bids"][str(sku)] = float(bid)

    # Per-SKU breakdown within each campaign
    sku_stats_rows = ch.query("""
        SELECT
            campaign_id,
            sku,
            sum(money_spent) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(add_to_cart) AS t_cart,
            sum(orders) AS t_direct_orders,
            sum(model_orders) AS t_model_orders,
            sum(revenue) AS t_direct_revenue,
            sum(model_revenue) AS t_model_revenue
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {cur_start:Date}
          AND dt <= {cur_end:Date}
          AND sku > 0
        GROUP BY campaign_id, sku
        ORDER BY campaign_id, t_spend DESC
    """, parameters=params).result_rows

    # Collect all SKUs for name enrichment
    all_skus: set = set()
    sku_stats_by_campaign: dict = {}
    for row in sku_stats_rows:
        cid = int(row[0])
        sku = int(row[1])
        all_skus.add(sku)
        sku_stats_by_campaign.setdefault(cid, []).append({
            "sku": sku,
            "spend": round(float(row[2]), 2),
            "views": int(row[3]),
            "clicks": int(row[4]),
            "cart": int(row[5]),
            "direct_orders": int(row[6]),
            "model_orders": int(row[7]),
            "direct_revenue": round(float(row[8]), 2),
            "model_revenue": round(float(row[9]), 2),
        })

    # Enrich SKU names from PostgreSQL
    sku_name_map: dict = {}
    if all_skus:
        try:
            sku_list = [int(s) for s in all_skus]
            sku_result = await db.execute(
                sa_text("""
                    SELECT product_id, sku, offer_id, name,
                           COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :shop_id
                      AND (product_id = ANY(:skus) OR sku = ANY(:skus))
                """),
                {"shop_id": shop_id, "skus": sku_list},
            )
            for row in sku_result:
                pid = int(row[0])
                s = int(row[1]) if row[1] else pid
                info = {"product_id": pid, "offer_id": row[2] or "", "name": row[3] or "", "image_url": row[4] or ""}
                sku_name_map[pid] = info
                if s != pid:
                    sku_name_map[s] = info
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("SKU enrichment failed: %s", e)

    # Per-SKU total revenue from fact_ozon_orders for total_drr
    sku_total_rev_map: dict = {}
    if all_skus:
        try:
            sku_rev_rows = ch.query("""
                SELECT
                    sku,
                    sum(price * quantity) AS total_revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(in_process_at, 3)) >= {cur_start:Date}
                  AND toDate(addHours(in_process_at, 3)) <= {cur_end:Date}
                  AND sku IN ({skus:Array(UInt64)})
                GROUP BY sku
            """, parameters={**params, "skus": list(all_skus)}).result_rows
            for row in sku_rev_rows:
                sku_total_rev_map[int(row[0])] = float(row[1])
        except Exception:
            pass

    def build_sku_item(s: dict, bids_map: dict = None) -> dict:
        sku = s["sku"]
        info = sku_name_map.get(sku, {})
        orders = s["direct_orders"] + s["model_orders"]
        revenue = round(s["direct_revenue"] + s["model_revenue"], 2)
        halo_pct = round(s["model_orders"] / orders * 100, 1) if orders > 0 else 0
        ad_drr = round(s["spend"] / revenue * 100, 1) if revenue > 0 else 0
        total_rev = sku_total_rev_map.get(sku, 0)
        total_drr = round(s["spend"] / total_rev * 100, 1) if total_rev > 0 else 0
        cart_conv = round(s["cart"] / s["clicks"] * 100, 1) if s["clicks"] > 0 else 0
        order_conv = round(orders / s["cart"] * 100, 1) if s["cart"] > 0 else 0
        return {
            "sku": sku,
            "product_id": info.get("product_id", sku),
            "offer_id": info.get("offer_id", ""),
            "name": info.get("name", ""),
            "image_url": info.get("image_url", ""),
            "spend": s["spend"],
            "views": s["views"],
            "clicks": s["clicks"],
            "cart": s["cart"],
            "cart_conv": cart_conv,
            "orders": orders,
            "order_conv": order_conv,
            "direct_orders": s["direct_orders"],
            "model_orders": s["model_orders"],
            "revenue": revenue,
            "direct_revenue": s["direct_revenue"],
            "model_revenue": s["model_revenue"],
            "halo_pct": halo_pct,
            "ctr": round(s["clicks"] / s["views"] * 100, 2) if s["views"] > 0 else 0,
            "avg_cpc": round(s["spend"] / s["clicks"], 2) if s["clicks"] > 0 else 0,
            "drr": ad_drr,
            "total_revenue": round(total_rev, 2),
            "total_drr": total_drr,
            "bid": float(bids_map.get(str(sku), 0)) if bids_map else 0,
        }

    campaigns_table = []
    for row in campaigns_rows:
        cid = int(row[0])
        direct_orders = int(row[5])
        model_orders = int(row[6])
        total_orders = direct_orders + model_orders
        direct_revenue = round(float(row[7]), 2)
        model_revenue = round(float(row[8]), 2)
        total_revenue = round(direct_revenue + model_revenue, 2)
        halo_pct = round(model_orders / total_orders * 100, 1) if total_orders > 0 else 0
        clicks = int(row[3])
        cart = int(row[4])
        cart_conv = round(cart / clicks * 100, 1) if clicks > 0 else 0
        order_conv = round(total_orders / cart * 100, 1) if cart > 0 else 0

        # Build per-SKU items with bids from Redis
        info = campaign_info_map.get(cid, {})
        campaign_bids = info.get("bids", {})
        items = [build_sku_item(s, campaign_bids) for s in sku_stats_by_campaign.get(cid, [])]
        
        # Calculate total revenue by summing sku_total_rev for this campaign's SKUs
        campaign_total_rev = 0.0
        for s in sku_stats_by_campaign.get(cid, []):
            campaign_total_rev += sku_total_rev_map.get(s["sku"], 0)
        campaign_total_drr = round(float(row[1]) / campaign_total_rev * 100, 1) if campaign_total_rev > 0 else 0

        campaigns_table.append({
            "campaign_id": cid,
            "title": info.get("title", ""),
            "status": info.get("status", ""),
            "campaign_type": info.get("campaign_type", ""),
            "sku_count": int(row[12]),
            "items": items,
            "spend": round(float(row[1]), 2),
            "views": int(row[2]),
            "clicks": clicks,
            "cart": cart,
            "cart_conv": cart_conv,
            "orders": total_orders,
            "order_conv": order_conv,
            "direct_orders": direct_orders,
            "model_orders": model_orders,
            "revenue": total_revenue,
            "direct_revenue": direct_revenue,
            "model_revenue": model_revenue,
            "halo_pct": halo_pct,
            "ctr": float(row[9]),
            "avg_cpc": float(row[10]),
            "drr": float(row[11]),
            "total_revenue": round(campaign_total_rev, 2),
            "total_drr": campaign_total_drr,
        })

    # ── 4. Top SKUs ───────────────────────────────────
    top_skus_rows = ch.query("""
        SELECT
            sku,
            sum(money_spent) AS t_spend,
            sum(orders) + sum(model_orders) AS t_orders,
            sum(revenue) + sum(model_revenue) AS t_revenue,
            CASE WHEN (sum(revenue) + sum(model_revenue)) > 0
                THEN round(sum(money_spent) / (sum(revenue) + sum(model_revenue)) * 100, 1) ELSE 0
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
            sa_text("""
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

    # ── 5. Events overlay (from PostgreSQL event_log) ──────────
    chart_start_dt = dt_datetime.combine(chart_start, dt_datetime.min.time())
    chart_end_dt = dt_datetime.combine(cur_end + timedelta(days=1), dt_datetime.min.time())
    events_agg_result = await db.execute(
        sa_text("""
            SELECT
                date_trunc('day', created_at)::date AS day,
                event_type,
                count(*) AS cnt
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at >= :date_from
              AND created_at < :date_to
              AND event_type = ANY(:event_types)
            GROUP BY day, event_type
            ORDER BY day
        """),
        {"shop_id": shop_id, "date_from": chart_start_dt, "date_to": chart_end_dt,
         "event_types": AD_CHART_EVENT_TYPES},
    )
    events_by_day: dict = {}
    for row in events_agg_result:
        day_str = str(row[0])
        evt_type = row[1]
        cnt = int(row[2])
        cat = EVENT_TYPE_TO_CATEGORY.get(evt_type, "other")
        if day_str not in events_by_day:
            events_by_day[day_str] = {"advertising": 0, "content": 0, "price": 0, "stock": 0, "total": 0}
        events_by_day[day_str][cat] = events_by_day[day_str].get(cat, 0) + cnt
        events_by_day[day_str]["total"] += cnt

    return {
        "date_from": cur_start.isoformat(),
        "date_to": cur_end.isoformat(),
        "kpi": kpi,
        "chart_daily": chart_daily,
        "campaigns_table": campaigns_table,
        "top_skus": top_skus,
        "events_by_day": events_by_day,
    }


# ══════════════════════════════════════════════════════════════════
# Per-campaign daily stats for before/after event analysis
# ══════════════════════════════════════════════════════════════════

@router.get("/campaign-daily-stats")
async def campaign_daily_stats(
    shop_id: int = Query(...),
    date_from: str = Query(...),
    date_to: str = Query(...),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Per-campaign daily metrics from fact_ozon_ad_daily.
    Returns daily spend/views/clicks/cart/orders/revenue/ctr/drr per campaign.
    Also returns events matched to campaigns (via campaign_id and product nm_id).
    """
    shop_result = await db.execute(
        sa_text("SELECT id, marketplace FROM shops WHERE id = :shop_id AND user_id = :user_id"),
        {"shop_id": shop_id, "user_id": str(current_user.id)},
    )
    shop_row = shop_result.fetchone()
    if not shop_row:
        raise HTTPException(status_code=404, detail="Магазин не найден")
    marketplace = shop_row[1]

    if marketplace not in ("ozon", "wildberries"):
        return {"campaigns_daily": {}, "events_by_campaign": {}, "campaign_total_revenue": {}}

    try:
        start_date = date.fromisoformat(date_from)
        end_date = date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")

    from app.core.clickhouse import get_clickhouse_client
    ch = get_clickhouse_client()

    if marketplace == "wildberries":
        # ── WB: per-campaign daily stats from fact_advert_stats_v3 ──
        rows = ch.query("""
            SELECT
                advert_id AS campaign_id,
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
            GROUP BY campaign_id, day
            ORDER BY campaign_id, day
        """, parameters={
            "shop_id": shop_id,
            "start": start_date,
            "end": end_date,
        }).result_rows

        campaigns_daily: dict = {}
        for row in rows:
            cid = int(row[0])
            if cid not in campaigns_daily:
                campaigns_daily[cid] = []
            campaigns_daily[cid].append({
                "date": str(row[1]),
                "spend": round(float(row[2]), 2),
                "views": int(row[3]),
                "clicks": int(row[4]),
                "cart": int(row[5]),
                "orders": int(row[6]),
                "revenue": round(float(row[7]), 2),
                "ctr": float(row[8]),
                "drr": float(row[9]),
            })

        # Build campaign_id -> nm_id mapping
        sku_map_rows = ch.query("""
            SELECT DISTINCT advert_id AS campaign_id, nm_id
            FROM mms_analytics.fact_advert_stats_v3 FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND date >= {start:Date}
              AND date <= {end:Date}
              AND nm_id > 0
        """, parameters={
            "shop_id": shop_id,
            "start": start_date,
            "end": end_date,
        }).result_rows

        campaign_skus: dict = {}
        nm_to_campaigns: dict = {}
        for row in sku_map_rows:
            cid, nm_id = int(row[0]), int(row[1])
            campaign_skus.setdefault(cid, []).append(nm_id)
            nm_to_campaigns.setdefault(nm_id, set()).add(cid)

        # Total revenue per nm_id per day from fact_orders_raw
        campaign_total_rev_agg: dict = {}
        all_camp_nms = list({nm for nms in campaign_skus.values() for nm in nms})
        if all_camp_nms:
            try:
                total_rev_rows = ch.query("""
                    SELECT
                        nm_id,
                        toDate(addHours(date, 3)) AS day,
                        sum(price_with_disc) AS total_revenue
                    FROM mms_analytics.fact_orders_raw FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(date, 3)) >= {start:Date}
                      AND toDate(addHours(date, 3)) <= {end:Date}
                      AND nm_id IN ({nm_ids:Array(UInt64)})
                    GROUP BY nm_id, day
                    ORDER BY nm_id, day
                """, parameters={
                    "shop_id": shop_id,
                    "start": start_date,
                    "end": end_date,
                    "nm_ids": all_camp_nms,
                }).result_rows

                nm_day_rev: dict = {}
                for row in total_rev_rows:
                    key = (int(row[0]), str(row[1]))
                    nm_day_rev[key] = float(row[2])

                # Aggregate per campaign per day
                campaign_total_rev_daily: dict = {}
                for cid, nms in campaign_skus.items():
                    daily_rev: dict = {}
                    for nm in nms:
                        for (n, day), rev in nm_day_rev.items():
                            if n == nm:
                                daily_rev[day] = daily_rev.get(day, 0) + rev
                    if daily_rev:
                        campaign_total_rev_daily[cid] = daily_rev

                # Merge total_revenue into campaigns_daily
                for cid, days_data in campaigns_daily.items():
                    total_rev_for_campaign = campaign_total_rev_daily.get(cid, {})
                    for dp in days_data:
                        dp["total_revenue"] = round(total_rev_for_campaign.get(dp["date"], 0), 2)
                        dp["total_drr"] = round(dp["spend"] / dp["total_revenue"] * 100, 1) if dp["total_revenue"] > 0 else 0
            except Exception:
                pass

        # Compute total_revenue aggregate per campaign
        for cid, days_data in campaigns_daily.items():
            total = sum(dp.get("total_revenue", 0) for dp in days_data)
            if total > 0:
                campaign_total_rev_agg[cid] = round(total, 2)

        # Fetch events for WB
        events_result = await db.execute(
            sa_text("""
                SELECT id, created_at, event_type, advert_id, nm_id,
                       old_value, new_value, event_metadata
                FROM event_log
                WHERE shop_id = :shop_id
                  AND created_at >= :date_from
                  AND created_at < :date_to_excl
                  AND event_type = ANY(:event_types)
                ORDER BY created_at
            """),
            {
                "shop_id": shop_id,
                "date_from": start_date,
                "date_to_excl": end_date + timedelta(days=1),
                "event_types": AD_CHART_EVENT_TYPES,
            },
        )
        raw_events = events_result.fetchall()

        events_by_campaign: dict = {}
        for ev in raw_events:
            ev_id, created_at, event_type, advert_id, nm_id, old_val, new_val, metadata = ev

            cat = EVENT_TYPE_TO_CATEGORY.get(event_type, "other")
            label = EVENT_LABELS.get(event_type, event_type)
            ev_date = created_at.strftime("%Y-%m-%d") if created_at else ""
            ev_time = created_at.strftime("%H:%M") if created_at else ""

            meta = {}
            if metadata:
                if isinstance(metadata, str):
                    try:
                        meta = json.loads(metadata)
                    except (json.JSONDecodeError, TypeError):
                        pass
                elif isinstance(metadata, dict):
                    meta = metadata

            detail = ""
            if event_type in ("BID_CHANGE",):
                old_fmt = f"{float(old_val):.2f} ₽" if old_val else ""
                new_fmt = f"{float(new_val):.2f} ₽" if new_val else ""
                bid_field = meta.get("bid_field", "")
                prefix = {"search": "Поиск", "recommendation": "Рекомендации"}.get(bid_field, "")
                detail = f"{prefix}: {old_fmt} → {new_fmt}" if prefix else f"{old_fmt} → {new_fmt}"
            elif event_type in ("STATUS_CHANGE",):
                sl = {"9": "Активна", "11": "Приостановлена", "7": "Завершена"}
                detail = f"{sl.get(old_val, old_val or '')} → {sl.get(new_val, new_val or '')}"
            elif event_type in ("PRICE_CHANGE",):
                old_fmt = f"{float(old_val):,.0f} ₽" if old_val else ""
                new_fmt = f"{float(new_val):,.0f} ₽" if new_val else ""
                detail = f"{old_fmt} → {new_fmt}"

            campaign_title = meta.get("campaign_title", "") or meta.get("title", "")

            event_data = {
                "id": ev_id,
                "date": ev_date,
                "time": ev_time,
                "event_type": event_type,
                "category": cat,
                "label": label,
                "detail": detail,
                "campaign_title": campaign_title,
                "nm_id": int(nm_id) if nm_id else None,
                "offer_id": "",
            }

            # Match event to campaigns
            matched_campaigns = set()
            if advert_id:
                matched_campaigns.add(int(advert_id))
            if nm_id:
                nm_int = int(nm_id)
                if nm_int in nm_to_campaigns:
                    matched_campaigns.update(nm_to_campaigns[nm_int])

            for cid in matched_campaigns:
                events_by_campaign.setdefault(cid, []).append(event_data)

        ch.close()
        return {
            "campaigns_daily": campaigns_daily,
            "events_by_campaign": events_by_campaign,
            "campaign_total_revenue": campaign_total_rev_agg,
        }

    # ── Ozon path (existing) ──────────────────────────
    rows = ch.query("""
        SELECT
            campaign_id,
            dt AS day,
            sum(money_spent) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(add_to_cart) AS t_cart,
            sum(orders) + sum(model_orders) AS t_orders,
            sum(revenue) + sum(model_revenue) AS t_revenue,
            CASE WHEN sum(views) > 0
                THEN round(sum(clicks) / sum(views) * 100, 2) ELSE 0
            END AS t_ctr,
            CASE WHEN (sum(revenue) + sum(model_revenue)) > 0
                THEN round(sum(money_spent) / (sum(revenue) + sum(model_revenue)) * 100, 1) ELSE 0
            END AS t_drr
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {start:Date}
          AND dt <= {end:Date}
        GROUP BY campaign_id, day
        ORDER BY campaign_id, day
    """, parameters={
        "shop_id": shop_id,
        "start": start_date,
        "end": end_date,
    }).result_rows

    # Build per-campaign daily map: {campaign_id: [{date, metrics...}]}
    campaigns_daily: dict = {}
    for row in rows:
        cid = int(row[0])
        if cid not in campaigns_daily:
            campaigns_daily[cid] = []
        campaigns_daily[cid].append({
            "date": str(row[1]),
            "spend": round(float(row[2]), 2),
            "views": int(row[3]),
            "clicks": int(row[4]),
            "cart": int(row[5]),
            "orders": int(row[6]),
            "revenue": round(float(row[7]), 2),
            "ctr": float(row[8]),
            "drr": float(row[9]),
        })

    # Build campaign_id -> SKU mapping from fact_ozon_ad_daily for product event matching
    sku_map_rows = ch.query("""
        SELECT DISTINCT campaign_id, sku
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND dt >= {start:Date}
          AND dt <= {end:Date}
    """, parameters={
        "shop_id": shop_id,
        "start": start_date,
        "end": end_date,
    }).result_rows
    
    sku_to_product = {}
    product_to_campaigns: dict = {}  # product_id -> [campaign_ids]
    campaign_skus: dict = {}  # campaign_id -> [skus]
    
    for row in sku_map_rows:
        cid, sku = int(row[0]), int(row[1])
        if cid not in campaign_skus:
            campaign_skus[cid] = []
        campaign_skus[cid].append(sku)
    
    # Get product_id for all SKUs
    all_skus = list({s for sks in campaign_skus.values() for s in sks})
    product_to_offer: dict = {}  # product_id -> offer_id
    if all_skus:
        prod_result = await db.execute(
            sa_text("""
                SELECT sku, product_id, offer_id FROM dim_ozon_products
                WHERE shop_id = :shop_id AND sku = ANY(:skus)
            """),
            {"shop_id": shop_id, "skus": all_skus},
        )
        for row in prod_result:
            sku_to_product[int(row[0])] = int(row[1])
            if row[1] and row[2]:
                product_to_offer[int(row[1])] = str(row[2])
    
    # Build product_id -> campaign_ids mapping
    for cid, skus in campaign_skus.items():
        for sku in skus:
            pid = sku_to_product.get(sku)
            if pid:
                if pid not in product_to_campaigns:
                    product_to_campaigns[pid] = set()
                product_to_campaigns[pid].add(cid)

    # Fetch total revenue per campaign per day from fact_ozon_orders
    campaign_total_rev_agg: dict = {}
    if campaign_skus:
        all_camp_skus = list({s for sks in campaign_skus.values() for s in sks})
        if all_camp_skus:
            try:
                total_rev_rows = ch.query("""
                    SELECT
                        sku,
                        toDate(addHours(in_process_at, 3)) AS day,
                        sum(price * quantity) AS total_revenue
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND toDate(addHours(in_process_at, 3)) >= {start:Date}
                      AND toDate(addHours(in_process_at, 3)) <= {end:Date}
                      AND sku IN ({skus:Array(UInt64)})
                    GROUP BY sku, day
                    ORDER BY sku, day
                """, parameters={
                    "shop_id": shop_id,
                    "start": start_date,
                    "end": end_date,
                    "skus": all_camp_skus,
                }).result_rows

                # Build sku,day -> revenue map
                sku_day_rev: dict = {}
                for row in total_rev_rows:
                    key = (int(row[0]), str(row[1]))
                    sku_day_rev[key] = float(row[2])

                # Aggregate per campaign per day
                campaign_total_rev_daily: dict = {}
                for cid, skus in campaign_skus.items():
                    daily_rev: dict = {}
                    for sku in skus:
                        for (sk, day), rev in sku_day_rev.items():
                            if sk == sku:
                                daily_rev[day] = daily_rev.get(day, 0) + rev
                    if daily_rev:
                        campaign_total_rev_daily[cid] = daily_rev

                # Merge total_revenue into campaigns_daily
                for cid, days_data in campaigns_daily.items():
                    total_rev_for_campaign = campaign_total_rev_daily.get(cid, {})
                    for dp in days_data:
                        dp["total_revenue"] = round(total_rev_for_campaign.get(dp["date"], 0), 2)
                        dp["total_drr"] = round(dp["spend"] / dp["total_revenue"] * 100, 1) if dp["total_revenue"] > 0 else 0
            except Exception:
                pass
    
    # Compute total_revenue aggregate per campaign  
    for cid, days_data in campaigns_daily.items():
        total = sum(dp.get("total_revenue", 0) for dp in days_data)
        if total > 0:
            campaign_total_rev_agg[cid] = round(total, 2)

    # Fetch events for this period
    events_result = await db.execute(
        sa_text("""
            SELECT id, created_at, event_type, advert_id, nm_id,
                   old_value, new_value, event_metadata
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at >= :date_from
              AND created_at < :date_to_excl
              AND event_type = ANY(:event_types)
            ORDER BY created_at
        """),
        {
            "shop_id": shop_id,
            "date_from": start_date,
            "date_to_excl": end_date + timedelta(days=1),
            "event_types": AD_CHART_EVENT_TYPES,
        },
    )
    raw_events = events_result.fetchall()

    # Process events and match to campaigns
    events_by_campaign: dict = {}  # campaign_id -> [{date, type, detail}]
    
    for ev in raw_events:
        ev_id, created_at, event_type, advert_id, nm_id, old_val, new_val, metadata = ev
        
        cat = EVENT_TYPE_TO_CATEGORY.get(event_type, "other")
        label = EVENT_LABELS.get(event_type, event_type)
        ev_date = created_at.strftime("%Y-%m-%d") if created_at else ""
        ev_time = created_at.strftime("%H:%M") if created_at else ""
        
        meta = {}
        if metadata:
            if isinstance(metadata, str):
                try:
                    meta = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    pass
            elif isinstance(metadata, dict):
                meta = metadata
        
        # Build detail string
        detail = ""
        if event_type in ("OZON_BID_CHANGE", "BID_CHANGE"):
            old_fmt = f"{float(old_val):.2f} ₽" if old_val else ""
            new_fmt = f"{float(new_val):.2f} ₽" if new_val else ""
            detail = f"{old_fmt} → {new_fmt}"
        elif event_type in ("OZON_BUDGET_CHANGE",):
            old_fmt = f"{float(old_val):,.0f} ₽" if old_val else ""
            new_fmt = f"{float(new_val):,.0f} ₽" if new_val else ""
            detail = f"{old_fmt} → {new_fmt}"
        elif event_type in ("OZON_STATUS_CHANGE", "STATUS_CHANGE"):
            sl = {"CAMPAIGN_STATE_RUNNING": "Активна", "CAMPAIGN_STATE_STOPPED": "Остановлена", "CAMPAIGN_STATE_INACTIVE": "Неактивна"}
            detail = f"{sl.get(old_val, old_val or '')} → {sl.get(new_val, new_val or '')}"
        elif event_type in ("OZON_PRICE_CHANGE", "PRICE_CHANGE"):
            old_fmt = f"{float(old_val):,.0f} ₽" if old_val else ""
            new_fmt = f"{float(new_val):,.0f} ₽" if new_val else ""
            detail = f"{old_fmt} → {new_fmt}"
        
        campaign_title = meta.get("campaign_title", "") or meta.get("title", "")
        
        # Build offer_id from nm_id
        event_offer_id = ""
        if nm_id:
            nm_int = int(nm_id)
            event_offer_id = product_to_offer.get(nm_int, "")
            if not event_offer_id:
                # nm_id might be a SKU, look up product_id first
                pid = sku_to_product.get(nm_int)
                if pid:
                    event_offer_id = product_to_offer.get(pid, "")
        
        event_data = {
            "id": ev_id,
            "date": ev_date,
            "time": ev_time,
            "event_type": event_type,
            "category": cat,
            "label": label,
            "detail": detail,
            "campaign_title": campaign_title,
            "nm_id": int(nm_id) if nm_id else None,
            "offer_id": event_offer_id,
        }
        
        # Match event to campaigns
        matched_campaigns = set()
        
        # Direct match via advert_id (advertising events)
        if advert_id:
            matched_campaigns.add(int(advert_id))
        
        # Match via product_id (price, content, stock events)
        if nm_id:
            nm_int = int(nm_id)
            # nm_id could be product_id directly
            if nm_int in product_to_campaigns:
                matched_campaigns.update(product_to_campaigns[nm_int])
            # or it could be a SKU
            pid = sku_to_product.get(nm_int)
            if pid and pid in product_to_campaigns:
                matched_campaigns.update(product_to_campaigns[pid])
        
        for cid in matched_campaigns:
            if cid not in events_by_campaign:
                events_by_campaign[cid] = []
            events_by_campaign[cid].append(event_data)

    return {
        "campaigns_daily": campaigns_daily,
        "events_by_campaign": events_by_campaign,
        "campaign_total_revenue": campaign_total_rev_agg,
    }



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
    # Конверсия клик → корзина
    cur_cart_rate = round(cur["cart"] / cur["clicks"] * 100, 1) if cur["clicks"] > 0 else 0
    prev_cart_rate = round(prev["cart"] / prev["clicks"] * 100, 1) if prev["clicks"] > 0 else 0
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
                    WHEN toDate(addHours(date, 3)) >= {cur_start:Date} AND toDate(addHours(date, 3)) <= {cur_end:Date} THEN 'current'
                    WHEN toDate(addHours(date, 3)) >= {prev_start:Date} AND toDate(addHours(date, 3)) <= {prev_end:Date} THEN 'previous'
                END AS period,
                price_with_disc AS t_rev
            FROM mms_analytics.fact_orders_raw FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND toDate(addHours(date, 3)) >= {prev_start:Date}
              AND toDate(addHours(date, 3)) <= {cur_end:Date}
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
        "cart_rate": cur_cart_rate,
        "cart_rate_delta": round(cur_cart_rate - prev_cart_rate, 1),
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

    # ── 2. Daily Chart (with total_drr from fact_orders_raw) ──────
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

    # Get daily total revenue from fact_orders_raw for total_drr
    daily_total_rev_rows = ch.query("""
        SELECT
            toDate(addHours(date, 3)) AS day,
            sum(price_with_disc) AS total_rev
        FROM mms_analytics.fact_orders_raw FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND toDate(addHours(date, 3)) >= {start:Date}
          AND toDate(addHours(date, 3)) <= {end:Date}
        GROUP BY day
        ORDER BY day
    """, parameters={
        "shop_id": shop_id,
        "start": chart_start,
        "end": cur_end,
    }).result_rows
    daily_total_rev = {str(row[0]): float(row[1]) for row in daily_total_rev_rows}

    chart_daily = []
    for row in chart_rows:
        day_str = str(row[0])
        spend = round(float(row[1]), 2)
        total_rev = daily_total_rev.get(day_str, 0)
        chart_daily.append({
            "date": day_str,
            "spend": spend,
            "views": int(row[2]),
            "clicks": int(row[3]),
            "cart": int(row[4]),
            "orders": int(row[5]),
            "revenue": round(float(row[6]), 2),
            "ctr": float(row[7]),
            "drr": float(row[8]),
            "total_drr": round(spend / total_rev * 100, 1) if total_rev > 0 else 0,
        })

    # ── 3. Campaigns Table (enriched) ─────────────────
    campaigns_rows = ch.query("""
        SELECT
            advert_id AS campaign_id,
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(atbs) AS t_cart,
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
            END AS t_drr,
            uniqExact(nm_id) AS t_sku_count
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {cur_start:Date}
          AND date <= {cur_end:Date}
        GROUP BY campaign_id
        ORDER BY t_spend DESC
    """, parameters=params).result_rows

    campaign_ids = [int(row[0]) for row in campaigns_rows]

    # Enrich campaign info from dim_advert_campaigns
    campaign_info_map: dict = {}
    WB_CAMPAIGN_TYPES = {
        0: "", 1: "Поиск", 2: "Каталог", 4: "Карточка",
        5: "Рекомендации", 7: "Авто", 8: "Поиск + Каталог", 9: "Единая",
    }
    WB_CAMPAIGN_STATUSES = {
        -1: "Удалена", 4: "Готова", 7: "Завершена",
        8: "Отказ", 9: "Активна", 11: "Приостановлена",
    }

    if campaign_ids:
        try:
            ch_result = ch.query("""
                SELECT advert_id, name, type, status
                FROM mms_analytics.dim_advert_campaigns FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND advert_id IN ({cids:Array(UInt64)})
            """, parameters={
                "shop_id": shop_id,
                "cids": [int(c) for c in campaign_ids],
            }).result_rows
            for row in ch_result:
                cid = int(row[0])
                # Safe int parsing — type can be UInt8 or string
                try:
                    raw_type = int(row[2]) if row[2] is not None else 0
                except (ValueError, TypeError):
                    raw_type = 0
                try:
                    raw_status = int(row[3]) if row[3] is not None else 0
                except (ValueError, TypeError):
                    raw_status = 0
                campaign_info_map[cid] = {
                    "title": row[1] or "",
                    "campaign_type": WB_CAMPAIGN_TYPES.get(raw_type, str(row[2] or "")),
                    "status": WB_CAMPAIGN_STATUSES.get(raw_status, str(row[3] or "")),
                }
        except Exception as e:
            logger.warning("WB campaign enrichment failed: %s", e)

    # Per-SKU breakdown within each campaign
    sku_stats_rows = ch.query("""
        SELECT
            advert_id AS campaign_id,
            nm_id,
            sum(spend) AS t_spend,
            sum(views) AS t_views,
            sum(clicks) AS t_clicks,
            sum(atbs) AS t_cart,
            sum(orders) AS t_orders,
            sum(revenue) AS t_revenue
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {shop_id:UInt32}
          AND date >= {cur_start:Date}
          AND date <= {cur_end:Date}
          AND nm_id > 0
        GROUP BY campaign_id, nm_id
        ORDER BY campaign_id, t_spend DESC
    """, parameters=params).result_rows

    # Collect all nm_ids for enrichment
    all_nm_ids: set = set()
    sku_stats_by_campaign: dict = {}
    for row in sku_stats_rows:
        cid = int(row[0])
        nm_id = int(row[1])
        all_nm_ids.add(nm_id)
        sku_stats_by_campaign.setdefault(cid, []).append({
            "nm_id": nm_id,
            "spend": round(float(row[2]), 2),
            "views": int(row[3]),
            "clicks": int(row[4]),
            "cart": int(row[5]),
            "orders": int(row[6]),
            "revenue": round(float(row[7]), 2),
        })

    # Enrich SKU names from dim_products
    sku_name_map: dict = {}
    if all_nm_ids:
        try:
            nm_list = list(all_nm_ids)
            sku_result = await db.execute(
                sa_text("""
                    SELECT nm_id, vendor_code, name
                    FROM dim_products
                    WHERE shop_id = :shop_id AND nm_id = ANY(:nm_ids)
                """),
                {"shop_id": shop_id, "nm_ids": nm_list},
            )
            for row in sku_result:
                sku_name_map[int(row[0])] = {
                    "vendor_code": row[1] or "",
                    "name": row[2] or "",
                }
        except Exception as e:
            logger.warning("WB SKU enrichment failed: %s", e)

    # Per-SKU total revenue from fact_orders_raw
    sku_total_rev_map: dict = {}
    if all_nm_ids:
        try:
            sku_rev_rows = ch.query("""
                SELECT
                    nm_id,
                    sum(price_with_disc) AS total_revenue
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND toDate(addHours(date, 3)) >= {cur_start:Date}
                  AND toDate(addHours(date, 3)) <= {cur_end:Date}
                  AND nm_id IN ({nm_ids:Array(UInt64)})
                GROUP BY nm_id
            """, parameters={**params, "nm_ids": list(all_nm_ids)}).result_rows
            for row in sku_rev_rows:
                sku_total_rev_map[int(row[0])] = float(row[1])
        except Exception:
            pass

    # WB bids from ClickHouse log_wb_bids (latest bid per nm_id per advert_id)
    wb_bids_map: dict = {}  # (advert_id, nm_id) -> bid_search
    if campaign_ids and all_nm_ids:
        try:
            bid_rows = ch.query("""
                SELECT advert_id, nm_id,
                       argMax(bid_search, timestamp) AS last_bid
                FROM mms_analytics.log_wb_bids
                WHERE shop_id = {shop_id:UInt32}
                  AND advert_id IN ({cids:Array(UInt64)})
                GROUP BY advert_id, nm_id
            """, parameters={
                "shop_id": shop_id,
                "cids": [int(c) for c in campaign_ids],
            }).result_rows
            for row in bid_rows:
                wb_bids_map[(int(row[0]), int(row[1]))] = int(row[2])
        except Exception:
            pass

    def build_wb_sku_item(s: dict, campaign_id: int) -> dict:
        nm_id = s["nm_id"]
        info = sku_name_map.get(nm_id, {})
        orders = s["orders"]
        revenue = s["revenue"]
        ad_drr = round(s["spend"] / revenue * 100, 1) if revenue > 0 else 0
        total_rev = sku_total_rev_map.get(nm_id, 0)
        total_drr = round(s["spend"] / total_rev * 100, 1) if total_rev > 0 else 0
        cart_conv = round(s["cart"] / s["clicks"] * 100, 1) if s["clicks"] > 0 else 0
        order_conv = round(orders / s["cart"] * 100, 1) if s["cart"] > 0 else 0
        # WB bid in kopecks, convert to rubles for display
        bid_kopecks = wb_bids_map.get((campaign_id, nm_id), 0)
        bid_rub = round(bid_kopecks / 100, 2) if bid_kopecks else 0
        return {
            "sku": nm_id,
            "product_id": nm_id,
            "offer_id": info.get("vendor_code", str(nm_id)),
            "name": info.get("name", ""),
            "image_url": _wb_image_url(nm_id),
            "spend": s["spend"],
            "views": s["views"],
            "clicks": s["clicks"],
            "cart": s["cart"],
            "cart_conv": cart_conv,
            "orders": orders,
            "order_conv": order_conv,
            "direct_orders": orders,
            "model_orders": 0,   # WB has no model (associated) orders
            "revenue": revenue,
            "direct_revenue": revenue,
            "model_revenue": 0,
            "halo_pct": 0,       # WB has no halo effect tracking
            "ctr": round(s["clicks"] / s["views"] * 100, 2) if s["views"] > 0 else 0,
            "avg_cpc": round(s["spend"] / s["clicks"], 2) if s["clicks"] > 0 else 0,
            "drr": ad_drr,
            "total_revenue": round(total_rev, 2),
            "total_drr": total_drr,
            "bid": bid_rub,
        }

    campaigns_table = []
    for row in campaigns_rows:
        cid = int(row[0])
        spend = round(float(row[1]), 2)
        clicks = int(row[3])
        cart = int(row[4])
        orders = int(row[5])
        revenue = round(float(row[6]), 2)
        cart_conv = round(cart / clicks * 100, 1) if clicks > 0 else 0
        order_conv = round(orders / cart * 100, 1) if cart > 0 else 0

        info = campaign_info_map.get(cid, {})
        items = [build_wb_sku_item(s, cid) for s in sku_stats_by_campaign.get(cid, [])]

        # Aggregate total revenue from per-SKU map
        campaign_total_rev = sum(
            sku_total_rev_map.get(s["nm_id"], 0)
            for s in sku_stats_by_campaign.get(cid, [])
        )
        campaign_total_drr = round(spend / campaign_total_rev * 100, 1) if campaign_total_rev > 0 else 0

        campaigns_table.append({
            "campaign_id": cid,
            "title": info.get("title", ""),
            "status": info.get("status", ""),
            "campaign_type": info.get("campaign_type", ""),
            "sku_count": int(row[10]),
            "items": items,
            "spend": spend,
            "views": int(row[2]),
            "clicks": clicks,
            "cart": cart,
            "cart_conv": cart_conv,
            "orders": orders,
            "order_conv": order_conv,
            "direct_orders": orders,
            "model_orders": 0,
            "revenue": revenue,
            "direct_revenue": revenue,
            "model_revenue": 0,
            "halo_pct": 0,
            "ctr": float(row[7]),
            "avg_cpc": float(row[8]),
            "drr": float(row[9]),
            "total_revenue": round(campaign_total_rev, 2),
            "total_drr": campaign_total_drr,
        })

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
        info = sku_name_map.get(nm_id, {})
        top_skus.append({
            "sku": nm_id,
            "offer_id": info.get("vendor_code", str(nm_id)),
            "name": info.get("name", ""),
            "image_url": _wb_image_url(nm_id),
            "spend": round(float(row[1]), 2),
            "orders": int(row[2]),
            "revenue": round(float(row[3]), 2),
            "drr": float(row[4]),
        })

    # Enrich any missing SKU names (for nm_ids not already in sku_name_map)
    missing_nm = [s["sku"] for s in top_skus if not sku_name_map.get(s["sku"])]
    if missing_nm:
        try:
            pg_result = await db.execute(
                sa_text("""
                    SELECT nm_id, vendor_code, name
                    FROM dim_products
                    WHERE shop_id = :shop_id AND nm_id = ANY(:nm_ids)
                """),
                {"shop_id": shop_id, "nm_ids": missing_nm},
            )
            for row in pg_result:
                nm = int(row[0])
                sku_name_map[nm] = {
                    "vendor_code": row[1] or "",
                    "name": row[2] or "",
                }
            for s in top_skus:
                info = sku_name_map.get(s["sku"], {})
                if info:
                    s["name"] = info.get("name", s["name"])
                    s["offer_id"] = info.get("vendor_code", s["offer_id"])
        except Exception:
            pass

    # ── 5. Events overlay (from PostgreSQL event_log) ──────────
    chart_start_dt = dt_datetime.combine(chart_start, dt_datetime.min.time())
    chart_end_dt = dt_datetime.combine(cur_end + timedelta(days=1), dt_datetime.min.time())
    events_agg_result = await db.execute(
        sa_text("""
            SELECT
                date_trunc('day', created_at)::date AS day,
                event_type,
                count(*) AS cnt
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at >= :date_from
              AND created_at < :date_to
              AND event_type = ANY(:event_types)
            GROUP BY day, event_type
            ORDER BY day
        """),
        {"shop_id": shop_id, "date_from": chart_start_dt, "date_to": chart_end_dt,
         "event_types": AD_CHART_EVENT_TYPES},
    )
    events_by_day: dict = {}
    for row in events_agg_result:
        day_str = str(row[0])
        evt_type = row[1]
        cnt = int(row[2])
        cat = EVENT_TYPE_TO_CATEGORY.get(evt_type, "other")
        if day_str not in events_by_day:
            events_by_day[day_str] = {"advertising": 0, "content": 0, "price": 0, "stock": 0, "total": 0}
        events_by_day[day_str][cat] = events_by_day[day_str].get(cat, 0) + cnt
        events_by_day[day_str]["total"] += cnt

    return {
        "date_from": cur_start.isoformat(),
        "date_to": cur_end.isoformat(),
        "kpi": kpi,
        "chart_daily": chart_daily,
        "campaigns_table": campaigns_table,
        "top_skus": top_skus,
        "events_by_day": events_by_day,
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
