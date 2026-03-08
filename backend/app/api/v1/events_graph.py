"""
Events Graph API — События + KPI на временной шкале.

GET /events/graph?shop_id=X&period=30d&group_by=day
"""
import json
import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["Events Graph"])

# ── Constants ────────────────────────────────────────────────────

PERIOD_DAYS = {"7d": 7, "30d": 30, "90d": 90}

EVENT_CATEGORIES = {
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
    "CONTENT_PHOTO_REMOVED": "content", "CONTENT_PHOTO_ORDER_CHANGED": "content",
    "PRICE_CHANGE": "commercial", "OZON_PRICE_CHANGE": "commercial",
    "OZON_STOCK_OUT": "stock", "OZON_STOCK_REPLENISH": "stock",
    "STOCK_OUT": "stock", "STOCK_REPLENISH": "stock",
    "STOCK_OUT_FBO_TOTAL": "stock", "STOCK_OUT_FBS_TOTAL": "stock",
}

EVENT_LABELS = {
    "OZON_BID_CHANGE": "Ставка изменена", "OZON_STATUS_CHANGE": "Статус кампании",
    "OZON_BUDGET_CHANGE": "Бюджет кампании", "OZON_ITEM_ADD": "Товар добавлен",
    "OZON_ITEM_REMOVE": "Товар удалён", "OZON_CAMPAIGN_CREATED": "Новая кампания",
    "OZON_SEO_CHANGE": "SEO изменён", "OZON_PHOTO_CHANGE": "Фото изменено",
    "OZON_CONTENT_CHANGE": "Контент изменён",
    "BID_CHANGE": "Ставка изменена", "STATUS_CHANGE": "Статус кампании",
    "ITEM_ADD": "Товар добавлен", "ITEM_REMOVE": "Товар удалён",
    "ITEM_INACTIVE": "Товар неактивен", "CAMPAIGN_CREATED": "Новая кампания",
    "CONTENT_CHANGE": "Контент изменён", "CONTENT_TITLE_CHANGED": "Заголовок",
    "CONTENT_DESC_CHANGED": "Описание", "CONTENT_MAIN_PHOTO_CHANGED": "Главное фото",
    "CONTENT_PHOTO_ADDED": "Фото добавлено", "CONTENT_PHOTO_REMOVED": "Фото удалено",
    "CONTENT_PHOTO_ORDER_CHANGED": "Галерея изменена",
    "PRICE_CHANGE": "Цена изменена", "OZON_PRICE_CHANGE": "Цена изменена",
    "OZON_STOCK_OUT": "Нет остатков", "OZON_STOCK_REPLENISH": "Поступление",
    "STOCK_OUT": "Нет остатков", "STOCK_REPLENISH": "Поступление",
    "STOCK_OUT_FBO_TOTAL": "Нет остатков ФБО", "STOCK_OUT_FBS_TOTAL": "Нет остатков ФБС",
}

CATEGORY_LABELS = {
    "advertising": "Реклама",
    "content": "Контент",
    "commercial": "Коммерция",
    "stock": "Склад",
}

MAX_BRIEF_PER_CATEGORY = 2


def _event_brief_text(event_type: str, old_value: str | None, new_value: str | None,
                       metadata: dict | None) -> str:
    """Build short human-readable text for tooltip."""
    meta = metadata or {}

    # ── Status changes ──
    if event_type in ("OZON_STATUS_CHANGE", "STATUS_CHANGE"):
        status_labels = {
            "CAMPAIGN_STATE_RUNNING": "Активна",
            "CAMPAIGN_STATE_STOPPED": "Остановлена",
            "CAMPAIGN_STATE_INACTIVE": "Неактивна",
            "CAMPAIGN_STATE_ARCHIVED": "В архиве",
            "9": "Активна", "11": "Остановлена", "7": "В архиве",
        }
        new_label = status_labels.get(new_value or "", new_value or "")
        return f"Кампания → {new_label}"

    # ── Campaign created ──
    if event_type in ("CAMPAIGN_CREATED", "OZON_CAMPAIGN_CREATED"):
        items = meta.get("items", [])
        n = len(items)
        return f"Новая кампания · {n} товар." if n else "Новая кампания"

    # ── Bid changes ──
    if event_type == "OZON_BID_CHANGE":
        try:
            old_f = f"{float(old_value):.0f}" if old_value else "?"
            new_f = f"{float(new_value):.0f}" if new_value else "?"
            bid_field = meta.get("bid_field", "")
            prefix = "Поиск" if bid_field == "search" else "Рек." if bid_field else ""
            return f"Ставка {prefix} {old_f}→{new_f} ₽".strip()
        except (ValueError, TypeError):
            return "Ставка изменена"

    if event_type == "BID_CHANGE":
        try:
            old_r = int(old_value) // 100 if old_value else "?"
            new_r = int(new_value) // 100 if new_value else "?"
            return f"Ставка {old_r}→{new_r} ₽"
        except (ValueError, TypeError):
            return "Ставка изменена"

    # ── Budget changes ──
    if event_type == "OZON_BUDGET_CHANGE":
        try:
            old_f = f"{float(old_value):,.0f}".replace(",", " ") if old_value else "?"
            new_f = f"{float(new_value):,.0f}".replace(",", " ") if new_value else "?"
            return f"Бюджет {old_f}→{new_f} ₽"
        except (ValueError, TypeError):
            return "Бюджет изменён"

    # ── Price changes ──
    if event_type in ("PRICE_CHANGE", "OZON_PRICE_CHANGE"):
        try:
            old_f = f"{float(old_value):,.0f}".replace(",", " ") if old_value else "?"
            new_f = f"{float(new_value):,.0f}".replace(",", " ") if new_value else "?"
            arrow = "↑" if old_value and new_value and float(new_value) > float(old_value) else "↓"
            return f"Цена {arrow} {old_f}→{new_f} ₽"
        except (ValueError, TypeError):
            return "Цена изменена"

    # ── Item add/remove ──
    if event_type in ("OZON_ITEM_ADD", "ITEM_ADD"):
        return "Товар добавлен в кампанию"
    if event_type in ("OZON_ITEM_REMOVE", "ITEM_REMOVE", "ITEM_INACTIVE"):
        return "Товар удалён из кампании"

    # ── Content changes — hide hashes/IDs ──
    if event_type == "OZON_SEO_CHANGE":
        field = meta.get("field", "")
        return {"title": "SEO: заголовок", "description": "SEO: описание"}.get(field, "SEO изменён")

    if event_type == "OZON_PHOTO_CHANGE":
        field = meta.get("field", "")
        if field == "main_image":
            return "Главное фото заменено"
        if field in ("gallery", "images_order", "images"):
            return "Галерея изменена"
        return "Фото изменено"

    if event_type == "OZON_CONTENT_CHANGE":
        return "Контент изменён"
    if event_type == "CONTENT_CHANGE":
        return "Контент изменён"
    if event_type == "CONTENT_TITLE_CHANGED":
        return "Заголовок изменён"
    if event_type == "CONTENT_DESC_CHANGED":
        return "Описание изменено"
    if event_type == "CONTENT_MAIN_PHOTO_CHANGED":
        return "Главное фото заменено"
    if event_type == "CONTENT_PHOTO_ADDED":
        old_c = meta.get("old_count", "?")
        new_c = meta.get("new_count", "?")
        return f"Фото добавлено ({old_c}→{new_c} шт.)"
    if event_type == "CONTENT_PHOTO_REMOVED":
        return "Фото удалено"
    if event_type == "CONTENT_PHOTO_ORDER_CHANGED":
        return "Галерея изменена"

    # ── Stock events ──
    if event_type in ("OZON_STOCK_OUT", "STOCK_OUT"):
        warehouse = meta.get("warehouse_name", "")
        return f"Нет остатков" + (f" ({warehouse})" if warehouse else "")
    if event_type in ("OZON_STOCK_REPLENISH", "STOCK_REPLENISH"):
        delta = meta.get("delta", "")
        return f"Поступление +{delta} шт." if delta else "Поступление на склад"
    if event_type in ("STOCK_OUT_FBO_TOTAL", "STOCK_OUT_FBS_TOTAL"):
        supply = meta.get("supply_type", "")
        return f"⚠️ Нет остатков {supply}"

    # Fallback — just label, no raw values
    return EVENT_LABELS.get(event_type, event_type)


def _group_key(d: date, group_by: str) -> str:
    """Return grouping key for a date."""
    if group_by == "week":
        # Monday of the week
        monday = d - timedelta(days=d.weekday())
        return monday.isoformat()
    elif group_by == "month":
        return f"{d.year}-{d.month:02d}-01"
    return d.isoformat()


# ── Endpoint ────────────────────────────────────────────────────

@router.get("/graph")
async def get_events_graph(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("30d", description="Period: 7d, 30d, 90d"),
    group_by: str = Query("day", description="Grouping: day, week, month"),
    date_from: Optional[date] = Query(None, description="Custom start date"),
    date_to: Optional[date] = Query(None, description="Custom end date"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Events timeline + KPI metrics for chart visualization.
    Returns array of date-bucketed data points with events count,
    brief descriptions, and business metrics (orders, revenue, views, etc.)
    """
    from app.core.clickhouse import get_clickhouse_client
    from sqlalchemy import select
    from app.models.shop import Shop

    # ── Verify shop ownership ────────────────────────
    result = await db.execute(
        select(Shop).where(
            Shop.id == shop_id,
            Shop.user_id == current_user.id,
        )
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Магазин не найден")

    marketplace = shop.marketplace  # 'ozon' or 'wildberries'

    # ── Dates ────────────────────────────────────────
    if date_from and date_to:
        start_date, end_date = date_from, date_to
    else:
        days = PERIOD_DAYS.get(period, 30)
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

    if group_by not in ("day", "week", "month"):
        group_by = "day"

    # ── 1. Events from PostgreSQL ────────────────────
    events_raw = await db.execute(
        text("""
            SELECT
                created_at::date AS event_date,
                event_type,
                old_value,
                new_value,
                event_metadata::text
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at::date >= :start_date
              AND created_at::date <= :end_date
            ORDER BY created_at
        """),
        {"shop_id": shop_id, "start_date": start_date, "end_date": end_date},
    )
    events_rows = events_raw.fetchall()

    # Group events by bucketed date
    events_by_bucket: dict[str, list] = defaultdict(list)
    for row in events_rows:
        event_date = row[0]
        if isinstance(event_date, datetime):
            event_date = event_date.date()
        bucket = _group_key(event_date, group_by)
        events_by_bucket[bucket].append({
            "event_type": row[1],
            "old_value": row[2],
            "new_value": row[3],
            "metadata": json.loads(row[4]) if row[4] else None,
        })

    # ── 2. KPI metrics from ClickHouse ───────────────
    try:
        ch = get_clickhouse_client()

        if marketplace == "ozon":
            metrics_map = _query_ozon_metrics(ch, shop_id, start_date, end_date, group_by)
        else:
            metrics_map = _query_wb_metrics(ch, shop_id, start_date, end_date, group_by)

        ch.close()
    except Exception as e:
        logger.warning("CH query failed for events/graph shop %s: %s", shop_id, e)
        metrics_map = {}

    # ── 3. Build response ────────────────────────────
    # Generate all date buckets in range
    all_buckets = set()
    d = start_date
    while d <= end_date:
        all_buckets.add(_group_key(d, group_by))
        d += timedelta(days=1)

    all_buckets.update(events_by_bucket.keys())
    all_buckets.update(metrics_map.keys())

    data = []
    for bucket in sorted(all_buckets):
        # Events for this bucket
        bucket_events = events_by_bucket.get(bucket, [])

        # Category counts + brief
        by_cat: dict[str, list] = defaultdict(list)
        for ev in bucket_events:
            cat = EVENT_CATEGORIES.get(ev["event_type"], "other")
            brief = _event_brief_text(ev["event_type"], ev["old_value"], ev["new_value"], ev["metadata"])
            by_cat[cat].append(brief)

        events_by_category = {cat: len(briefs) for cat, briefs in by_cat.items()}
        events_total = len(bucket_events)

        # Build brief: up to MAX_BRIEF_PER_CATEGORY per category
        events_brief = []
        for cat in ["advertising", "content", "commercial", "stock"]:
            briefs = by_cat.get(cat, [])
            if not briefs:
                continue
            shown = briefs[:MAX_BRIEF_PER_CATEGORY]
            remaining = len(briefs) - len(shown)
            for b in shown:
                events_brief.append({"category": cat, "text": b})
            if remaining > 0:
                events_brief.append({"category": cat, "text": f"ещё {remaining}"})

        # Metrics
        m = metrics_map.get(bucket, {})
        orders = m.get("orders", 0)
        revenue = m.get("revenue", 0)
        ad_spend = m.get("ad_spend", 0)
        ad_orders = m.get("ad_orders", 0)
        views = m.get("views", 0)
        clicks = m.get("clicks", 0)
        carts = m.get("carts", 0)

        drr = round(ad_spend / revenue * 100, 1) if revenue > 0 else 0
        cpo = round(ad_spend / ad_orders, 0) if ad_orders > 0 else 0

        data.append({
            "date": bucket,
            "events_total": events_total,
            "events_by_category": events_by_category,
            "events_brief": events_brief,
            "orders": orders,
            "revenue": round(revenue, 2),
            "views": views,
            "clicks": clicks,
            "carts": carts,
            "ad_spend": round(ad_spend, 2),
            "ad_orders": ad_orders,
            "drr": drr,
            "cpo": cpo,
        })

    return {
        "shop_id": shop_id,
        "marketplace": marketplace,
        "group_by": group_by,
        "period": period,
        "date_from": start_date.isoformat(),
        "date_to": end_date.isoformat(),
        "data": data,
    }


# ── ClickHouse helpers ──────────────────────────────────────────

def _ch_group_expr(group_by: str, date_col: str) -> str:
    """Return CH expression for date grouping."""
    if group_by == "week":
        return f"toMonday({date_col})"
    elif group_by == "month":
        return f"toStartOfMonth({date_col})"
    return date_col


def _query_ozon_metrics(ch, shop_id: int, start: date, end: date,
                        group_by: str) -> dict:
    """
    Query Ozon KPI metrics from ClickHouse.
    - Orders/Revenue from fact_ozon_orders
    - Ads views/clicks/carts/spend from fact_ozon_ad_daily
    """
    grp_orders = _ch_group_expr(group_by, "toDate(addHours(in_process_at, 3))")
    grp_ads = _ch_group_expr(group_by, "dt")

    # Orders
    orders_rows = ch.query(f"""
        SELECT
            {grp_orders} AS bucket,
            count() AS orders_count,
            sum(price * quantity) AS revenue
        FROM mms_analytics.fact_ozon_orders FINAL
        WHERE shop_id = {{shop_id:UInt32}}
          AND toDate(addHours(in_process_at, 3)) >= {{start:Date}}
          AND toDate(addHours(in_process_at, 3)) <= {{end:Date}}
        GROUP BY bucket
        ORDER BY bucket
    """, parameters={"shop_id": shop_id, "start": start, "end": end}).result_rows

    result: dict[str, dict] = {}
    for row in orders_rows:
        key = str(row[0])
        result[key] = {
            "orders": int(row[1]),
            "revenue": float(row[2]),
            "views": 0, "clicks": 0, "carts": 0,
            "ad_spend": 0, "ad_orders": 0,
        }

    # Ads
    ads_rows = ch.query(f"""
        SELECT
            {grp_ads} AS bucket,
            sum(views) AS total_views,
            sum(clicks) AS total_clicks,
            sum(add_to_cart) AS total_carts,
            sum(money_spent) AS total_spend,
            sum(orders) AS total_orders
        FROM mms_analytics.fact_ozon_ad_daily FINAL
        WHERE shop_id = {{shop_id:UInt32}}
          AND dt >= {{start:Date}}
          AND dt <= {{end:Date}}
        GROUP BY bucket
        ORDER BY bucket
    """, parameters={"shop_id": shop_id, "start": start, "end": end}).result_rows

    for row in ads_rows:
        key = str(row[0])
        if key not in result:
            result[key] = {"orders": 0, "revenue": 0, "views": 0, "clicks": 0,
                           "carts": 0, "ad_spend": 0, "ad_orders": 0}
        result[key]["views"] = int(row[1])
        result[key]["clicks"] = int(row[2])
        result[key]["carts"] = int(row[3])
        result[key]["ad_spend"] = float(row[4])
        result[key]["ad_orders"] = int(row[5])

    return result


def _query_wb_metrics(ch, shop_id: int, start: date, end: date,
                      group_by: str) -> dict:
    """
    Query WB KPI metrics from ClickHouse.
    - Orders/Revenue from fact_orders_raw
    - Ads views/clicks/carts/spend from fact_advert_stats_v3
    """
    grp_orders = _ch_group_expr(group_by, "toDate(addHours(date, 3))")
    grp_ads = _ch_group_expr(group_by, "date")

    # Orders
    orders_rows = ch.query(f"""
        SELECT
            {grp_orders} AS bucket,
            count() AS orders_count,
            sum(price_with_disc) AS revenue
        FROM mms_analytics.fact_orders_raw FINAL
        WHERE shop_id = {{shop_id:UInt32}}
          AND toDate(addHours(date, 3)) >= {{start:Date}}
          AND toDate(addHours(date, 3)) <= {{end:Date}}
        GROUP BY bucket
        ORDER BY bucket
    """, parameters={"shop_id": shop_id, "start": start, "end": end}).result_rows

    result: dict[str, dict] = {}
    for row in orders_rows:
        key = str(row[0])
        result[key] = {
            "orders": int(row[1]),
            "revenue": float(row[2]),
            "views": 0, "clicks": 0, "carts": 0,
            "ad_spend": 0, "ad_orders": 0,
        }

    # Ads
    ads_rows = ch.query(f"""
        SELECT
            {grp_ads} AS bucket,
            sum(views) AS total_views,
            sum(clicks) AS total_clicks,
            sum(atbs) AS total_carts,
            sum(spend) AS total_spend,
            sum(orders) AS total_orders
        FROM mms_analytics.fact_advert_stats_v3 FINAL
        WHERE shop_id = {{shop_id:UInt32}}
          AND date >= {{start:Date}}
          AND date <= {{end:Date}}
        GROUP BY bucket
        ORDER BY bucket
    """, parameters={"shop_id": shop_id, "start": start, "end": end}).result_rows

    for row in ads_rows:
        key = str(row[0])
        if key not in result:
            result[key] = {"orders": 0, "revenue": 0, "views": 0, "clicks": 0,
                           "carts": 0, "ad_spend": 0, "ad_orders": 0}
        result[key]["views"] = int(row[1])
        result[key]["clicks"] = int(row[2])
        result[key]["carts"] = int(row[3])
        result[key]["ad_spend"] = float(row[4])
        result[key]["ad_orders"] = int(row[5])

    return result
