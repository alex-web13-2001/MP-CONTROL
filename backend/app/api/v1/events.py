"""
Events API — Лента событий.

GET /events/feed?shop_id=X&period=7d  — Event feed grouped by day
"""
import json
import logging
from collections import defaultdict
from datetime import date, datetime as dt_datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.core.redis_state import RedisStateManager
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["Events"])

# ── Event category mapping ────────────────────────────────────────

EVENT_CATEGORIES = {
    # Advertising
    "BID_CHANGE": "advertising",
    "STATUS_CHANGE": "advertising",
    "ITEM_ADD": "advertising",
    "ITEM_REMOVE": "advertising",
    "ITEM_INACTIVE": "advertising",
    "CAMPAIGN_CREATED": "advertising",
    "OZON_BID_CHANGE": "advertising",
    "OZON_STATUS_CHANGE": "advertising",
    "OZON_BUDGET_CHANGE": "advertising",
    "OZON_ITEM_ADD": "advertising",
    "OZON_ITEM_REMOVE": "advertising",
    "OZON_CAMPAIGN_CREATED": "advertising",
    # Content
    "OZON_SEO_CHANGE": "content",
    "OZON_PHOTO_CHANGE": "content",
    "OZON_CONTENT_CHANGE": "content",
    "CONTENT_CHANGE": "content",
    "CONTENT_TITLE_CHANGED": "content",
    "CONTENT_DESC_CHANGED": "content",
    "CONTENT_MAIN_PHOTO_CHANGED": "content",
    "CONTENT_PHOTO_ADDED": "content",
    "CONTENT_PHOTO_REMOVED": "content",
    "CONTENT_PHOTO_ORDER_CHANGED": "content",
    # Commercial
    "PRICE_CHANGE": "commercial",
    "OZON_PRICE_CHANGE": "commercial",
    # Stock / Warehouse
    "OZON_STOCK_OUT": "stock",
    "OZON_STOCK_REPLENISH": "stock",
    "STOCK_OUT": "stock",
    "STOCK_REPLENISH": "stock",
    "STOCK_OUT_FBO_TOTAL": "stock",
    "STOCK_OUT_FBS_TOTAL": "stock",
}

# Human-readable event descriptions (Russian)
EVENT_LABELS = {
    "OZON_BID_CHANGE": "Изменение ставки",
    "OZON_STATUS_CHANGE": "Статус кампании изменён",
    "OZON_BUDGET_CHANGE": "Бюджет кампании изменён",
    "OZON_ITEM_ADD": "Товар добавлен в кампанию",
    "OZON_ITEM_REMOVE": "Товар удалён из кампании",
    "OZON_CAMPAIGN_CREATED": "🚀 Новая кампания",
    "OZON_SEO_CHANGE": "SEO-контент изменён",
    "OZON_PHOTO_CHANGE": "Изменение фото",
    "OZON_CONTENT_CHANGE": "Контент изменён",
    "BID_CHANGE": "Изменение ставки",
    "STATUS_CHANGE": "Статус кампании изменён",
    "ITEM_ADD": "Товар добавлен в кампанию",
    "ITEM_REMOVE": "Товар удалён из кампании",
    "ITEM_INACTIVE": "Товар неактивен",
    "CAMPAIGN_CREATED": "🚀 Новая кампания",
    "CONTENT_CHANGE": "Изменение контента",
    "CONTENT_TITLE_CHANGED": "Заголовок изменён",
    "CONTENT_DESC_CHANGED": "Описание изменено",
    "CONTENT_MAIN_PHOTO_CHANGED": "Главное фото изменено",
    "CONTENT_PHOTO_ADDED": "Фото добавлено в галерею",
    "CONTENT_PHOTO_REMOVED": "Фото удалено из галереи",
    "CONTENT_PHOTO_ORDER_CHANGED": "Фото галереи изменено",
    "PRICE_CHANGE": "Цена изменена",
    "OZON_PRICE_CHANGE": "Цена изменена",
    "OZON_STOCK_OUT": "Товар закончился",
    "OZON_STOCK_REPLENISH": "Поступление на склад",
    "STOCK_OUT": "Товар закончился",
    "STOCK_REPLENISH": "Поступление на склад",
    "STOCK_OUT_FBO_TOTAL": "⚠️ Нет остатков ФБО",
    "STOCK_OUT_FBS_TOTAL": "⚠️ Нет остатков ФБС",
}

PERIOD_DAYS = {
    "today": 1,
    "7d": 7,
    "30d": 30,
    "90d": 90,
}

def _pluralize(n: int) -> str:
    """Russian plural for товар"""
    if 11 <= n % 100 <= 19:
        return "ов"
    r = n % 10
    if r == 1:
        return ""
    if 2 <= r <= 4:
        return "а"
    return "ов"


def _format_value(event_type: str, value: Optional[str], metadata: Optional[dict] = None) -> str:
    """Format event value for display."""
    if value is None:
        return ""
    # Bid changes: show in roubles
    if event_type in ("OZON_BID_CHANGE",):
        try:
            return f"{float(value):.2f} ₽"
        except (ValueError, TypeError):
            return value
    if event_type == "BID_CHANGE":
        # WB bids stored in kopecks — convert to rubles for display
        try:
            kopecks = int(value)
            rubles = kopecks / 100
            if rubles == int(rubles):
                return f"{int(rubles)} ₽"
            return f"{rubles:.2f} ₽"
        except (ValueError, TypeError):
            return value
    if event_type in ("OZON_BUDGET_CHANGE",):
        try:
            return f"{float(value):,.0f} ₽"
        except (ValueError, TypeError):
            return value
    if event_type in ("PRICE_CHANGE", "OZON_PRICE_CHANGE"):
        try:
            return f"{float(value):,.0f} ₽"
        except (ValueError, TypeError):
            return value
    return value


@router.get("/feed")
async def get_events_feed(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("7d", description="Period: today, 7d, 30d, 90d"),
    date_from: Optional[str] = Query(None, description="Custom start date YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="Custom end date YYYY-MM-DD"),
    event_types: Optional[str] = Query(
        None, description="Comma-separated event types filter"
    ),
    category: Optional[str] = Query(
        None, description="Category filter: advertising, content, commercial, stock"
    ),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=10, le=200, description="Events per page"),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get event feed grouped by day.

    Returns events from event_log enriched with product data
    (name, image) from dim_ozon_products / dim_products.
    """
    # ── Verify shop ownership ──────────────────────────
    shop_result = await db.execute(
        text("""
            SELECT id, marketplace FROM shops
            WHERE id = :shop_id AND user_id = :user_id
        """),
        {"shop_id": shop_id, "user_id": str(current_user.id)},
    )
    shop_row = shop_result.fetchone()
    if not shop_row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Магазин не найден",
        )
    marketplace = shop_row[1]

    # ── Date range ─────────────────────────────────────
    today = date.today()
    if date_from and date_to:
        # Custom range from calendar
        try:
            d_from = date.fromisoformat(date_from)
            d_to = date.fromisoformat(date_to)
        except ValueError:
            raise HTTPException(400, "Invalid date format, use YYYY-MM-DD")
    else:
        # Quick period
        days = PERIOD_DAYS.get(period, 7)
        d_from = today - timedelta(days=days - 1)
        d_to = today

    # ── Build filters ──────────────────────────────────
    filters = ["e.shop_id = :shop_id", "e.created_at >= :date_from", "e.created_at < :date_to_exclusive"]
    params: dict = {
        "shop_id": shop_id,
        "date_from": dt_datetime.combine(d_from, dt_datetime.min.time()),
        "date_to_exclusive": dt_datetime.combine(d_to + timedelta(days=1), dt_datetime.min.time()),
    }

    # Filter by event types
    if event_types:
        type_list = [t.strip() for t in event_types.split(",") if t.strip()]
        if type_list:
            filters.append("e.event_type = ANY(:type_list)")
            params["type_list"] = type_list

    # Filter by category
    if category and category in ("advertising", "content", "commercial", "stock"):
        cat_types = [k for k, v in EVENT_CATEGORIES.items() if v == category]
        if cat_types:
            filters.append("e.event_type = ANY(:cat_types)")
            params["cat_types"] = cat_types

    where_clause = " AND ".join(filters)
    offset = (page - 1) * page_size

    # ── Count total ────────────────────────────────────
    count_result = await db.execute(
        text(f"SELECT count(*) FROM event_log e WHERE {where_clause}"),
        params,
    )
    total = count_result.scalar() or 0

    # ── Fetch events ───────────────────────────────────
    events_result = await db.execute(
        text(f"""
            SELECT
                e.id,
                e.created_at,
                e.event_type,
                e.shop_id,
                e.advert_id,
                e.nm_id,
                e.old_value,
                e.new_value,
                e.event_metadata
            FROM event_log e
            WHERE {where_clause}
            ORDER BY e.created_at DESC
            LIMIT :limit OFFSET :offset
        """),
        {**params, "limit": page_size, "offset": offset},
    )
    raw_events = events_result.fetchall()

    if not raw_events:
        return {
            "shop_id": shop_id,
            "marketplace": marketplace,
            "total": 0,
            "page": page,
            "page_size": page_size,
            "days": [],
        }

    # ── Collect product IDs and advert IDs ──────────────
    nm_ids = set()
    advert_ids = set()
    for ev in raw_events:
        if ev[5]:  # nm_id
            nm_ids.add(int(ev[5]))
        if ev[4]:  # advert_id
            advert_ids.add(int(ev[4]))
        # Extract nm_ids from CAMPAIGN_CREATED metadata items
        event_type = ev[2]
        if event_type in ("CAMPAIGN_CREATED", "OZON_CAMPAIGN_CREATED"):
            meta_raw = ev[8]
            if meta_raw:
                try:
                    m = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
                    for item in (m.get("items") or []):
                        item_id = item.get("nm_id") or item.get("sku")
                        if item_id:
                            nm_ids.add(int(item_id))
                except (json.JSONDecodeError, ValueError, TypeError):
                    pass

    # ── Enrich with product data ───────────────────────
    product_map = {}
    if nm_ids:
        if marketplace == "ozon":
            # nm_id can be either SKU or product_id depending on event source:
            #   - Ad events (BID_CHANGE, ITEM_ADD) use SKU
            #   - Price events (PRICE_CHANGE) use product_id
            # Search by both fields to cover all cases
            pg_result = await db.execute(
                text("""
                    SELECT product_id, sku, name, offer_id,
                           COALESCE(NULLIF(primary_image_url, ''), main_image_url, '') AS image_url
                    FROM dim_ozon_products
                    WHERE shop_id = :shop_id
                      AND (sku = ANY(:nm_ids) OR product_id = ANY(:nm_ids))
                """),
                {"shop_id": shop_id, "nm_ids": list(nm_ids)},
            )
            for row in pg_result:
                product_id, sku, name, offer_id, image_url = row
                info = {
                    "name": name or "",
                    "offer_id": offer_id or "",
                    "image_url": image_url or "",
                }
                # Map both product_id and sku so lookup works regardless
                product_map[int(product_id)] = info
                if sku:
                    product_map[int(sku)] = info
        else:
            # WB: nm_id maps to nm_id in dim_products
            pg_result = await db.execute(
                text("""
                    SELECT nm_id, name, vendor_code, main_image_url
                    FROM dim_products
                    WHERE shop_id = :shop_id
                      AND nm_id = ANY(:nm_ids)
                """),
                {"shop_id": shop_id, "nm_ids": list(nm_ids)},
            )
            for row in pg_result:
                product_map[int(row[0])] = {
                    "name": row[1] or "",
                    "offer_id": row[2] or "",
                    "image_url": row[3] or "",
                }

    # ── Collect campaign titles ──────────────────────────
    # For campaign-level events (STATUS_CHANGE, BUDGET_CHANGE),
    #   meta["title"] IS the campaign title.
    # For product-level events (BID_CHANGE, ITEM_ADD, ITEM_REMOVE),
    #   meta["title"] is the PRODUCT title (not campaign!),
    #   meta["campaign_title"] is the campaign title (new field).
    campaign_title_map: dict[int, str] = {}
    for ev in raw_events:
        adv_id = ev[4]
        ev_type = ev[2]
        metadata = ev[8]
        if adv_id:
            adv_id_int = int(adv_id)
            if adv_id_int not in campaign_title_map:
                meta = {}
                if metadata:
                    if isinstance(metadata, str):
                        try:
                            meta = json.loads(metadata)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    elif isinstance(metadata, dict):
                        meta = metadata

                # campaign_title field (new, explicit)
                ct = meta.get("campaign_title", "")
                if ct:
                    campaign_title_map[adv_id_int] = ct
                # For STATUS_CHANGE / BUDGET_CHANGE, meta["title"] IS the campaign title
                elif ev_type in ("OZON_STATUS_CHANGE", "OZON_BUDGET_CHANGE", "STATUS_CHANGE"):
                    title = meta.get("title", "")
                    if title:
                        campaign_title_map[adv_id_int] = title

    # For advert_ids still without titles, look up from STATUS_CHANGE events
    missing_advert_ids = [a for a in advert_ids if a not in campaign_title_map and a != 0]
    if missing_advert_ids:
        title_result = await db.execute(
            text("""
                SELECT DISTINCT ON (advert_id) advert_id, event_metadata->>'title'
                FROM event_log
                WHERE shop_id = :shop_id
                  AND advert_id = ANY(:advert_ids)
                  AND event_type IN ('OZON_STATUS_CHANGE', 'OZON_BUDGET_CHANGE', 'STATUS_CHANGE')
                  AND event_metadata->>'title' IS NOT NULL
                  AND event_metadata->>'title' != ''
                ORDER BY advert_id, created_at DESC
            """),
            {"shop_id": shop_id, "advert_ids": missing_advert_ids},
        )
        for row in title_result:
            if row[0] and row[1]:
                campaign_title_map[int(row[0])] = row[1]

    # Last fallback: meta["campaign_title"] from ANY events
    still_missing = [a for a in advert_ids if a not in campaign_title_map and a != 0]
    if still_missing:
        ct_result = await db.execute(
            text("""
                SELECT DISTINCT ON (advert_id) advert_id, event_metadata->>'campaign_title'
                FROM event_log
                WHERE shop_id = :shop_id
                  AND advert_id = ANY(:advert_ids)
                  AND event_metadata->>'campaign_title' IS NOT NULL
                  AND event_metadata->>'campaign_title' != ''
                ORDER BY advert_id, created_at DESC
            """),
            {"shop_id": shop_id, "advert_ids": still_missing},
        )
        for row in ct_result:
            if row[0] and row[1]:
                campaign_title_map[int(row[0])] = row[1]

    # Redis fallback: campaign titles stored by tracker
    still_missing2 = [a for a in advert_ids if a not in campaign_title_map and a != 0]
    if still_missing2:
        try:
            redis_state = RedisStateManager()
            for adv_id in still_missing2:
                # Try Ozon state first
                ozon_state = redis_state.get_ozon_campaign_state(shop_id, adv_id)
                title = ozon_state.get("title", "")
                if title:
                    campaign_title_map[adv_id] = title
                    continue
                # Try WB state
                wb_state = redis_state.get_state(shop_id, adv_id)
                wb_name = wb_state.get("campaign_name", "")
                if wb_name:
                    campaign_title_map[adv_id] = wb_name
        except Exception as e:
            logger.warning("Redis campaign title lookup failed: %s", e)

    # ── Build response grouped by day ──────────────────
    days_map = defaultdict(list)

    for ev in raw_events:
        ev_id, created_at, event_type, ev_shop_id, advert_id, nm_id, old_value, new_value, metadata = ev

        # Parse metadata
        meta = {}
        if metadata:
            if isinstance(metadata, str):
                try:
                    meta = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    meta = {}
            elif isinstance(metadata, dict):
                meta = metadata

        event_category = EVENT_CATEGORIES.get(event_type, "other")
        event_label = EVENT_LABELS.get(event_type, event_type)

        # Product info
        product = None
        if nm_id:
            nm_id_int = int(nm_id)
            prod_info = product_map.get(nm_id_int, {})
            product = {
                "nm_id": nm_id_int,
                "name": prod_info.get("name", ""),
                "offer_id": prod_info.get("offer_id", ""),
                "image_url": prod_info.get("image_url", ""),
            }

        # Campaign title
        campaign_title = ""
        adv_id_int = int(advert_id) if advert_id else 0
        if adv_id_int:
            # 1. From pre-built map (STATUS_CHANGE titles + campaign_title fields)
            campaign_title = campaign_title_map.get(adv_id_int, "")
            # 2. Fallback: campaign_title from this event's metadata
            if not campaign_title:
                campaign_title = meta.get("campaign_title", "")
            # 3. For campaign-level events, meta["title"] IS the campaign title
            if not campaign_title and event_type in ("OZON_STATUS_CHANGE", "OZON_BUDGET_CHANGE", "STATUS_CHANGE"):
                campaign_title = meta.get("title", "")

        # Format values for display
        old_display = _format_value(event_type, old_value, meta)
        new_display = _format_value(event_type, new_value, meta)

        # Build detail string
        detail = ""
        campaign_items_list = []
        items = meta.get("items", [])
        if event_type in ("OZON_BID_CHANGE", "BID_CHANGE", "PRICE_CHANGE", "OZON_PRICE_CHANGE"):
            detail = f"{old_display} → {new_display}"
            bid_field = meta.get("bid_field", "")
            if bid_field:
                field_label = "Поиск" if bid_field == "search" else "Рекомендации"
                detail = f"{field_label}: {detail}"
        elif event_type in ("OZON_BUDGET_CHANGE",):
            detail = f"{old_display} → {new_display}"
        elif event_type in ("OZON_STATUS_CHANGE", "STATUS_CHANGE"):
            # Translate campaign states to Russian
            status_labels = {
                "CAMPAIGN_STATE_RUNNING": "Активна",
                "CAMPAIGN_STATE_STOPPED": "Остановлена",
                "CAMPAIGN_STATE_INACTIVE": "Неактивна",
                "CAMPAIGN_STATE_ARCHIVED": "В архиве",
                "9": "Активна", "11": "Остановлена",  # WB status codes
            }
            old_label = status_labels.get(old_value, old_value or "")
            new_label = status_labels.get(new_value, new_value or "")
            detail = f"{old_label} → {new_label}"
        elif event_type in ("OZON_ITEM_ADD", "ITEM_ADD"):
            detail = "Товар добавлен в кампанию"
        elif event_type in ("OZON_ITEM_REMOVE", "ITEM_REMOVE", "ITEM_INACTIVE"):
            detail = "Товар удалён из кампании"
        elif event_type in ("CAMPAIGN_CREATED", "OZON_CAMPAIGN_CREATED"):
            status_labels = {
                "CAMPAIGN_STATE_RUNNING": "Активна",
                "CAMPAIGN_STATE_STOPPED": "Остановлена",
                "CAMPAIGN_STATE_INACTIVE": "Неактивна",
                "9": "Активна", "11": "Остановлена", "7": "В архиве",
            }
            status_label = status_labels.get(new_value, "")
            detail = f"Создана кампания" + (f" · {status_label}" if status_label else "")
            
            # Build structured items list for frontend
            campaign_items_list = []
            if items:
                for it in items:
                    nm_id = it.get("nm_id") or it.get("sku", "")
                    prod_info = product_map.get(int(nm_id)) if nm_id else None
                    if prod_info:
                        campaign_items_list.append({
                            "offer_id": prod_info.get("offer_id", ""),
                            "nm_id": str(nm_id),
                            "name": prod_info.get("name", ""),
                        })
                    else:
                        campaign_items_list.append({
                            "offer_id": it.get("offer_id", ""),
                            "nm_id": str(nm_id),
                            "name": it.get("title", "") or it.get("subject", ""),
                        })
                detail += f" · {len(campaign_items_list)} товар{_pluralize(len(campaign_items_list))}"
        elif event_type in ("STOCK_OUT",):
            warehouse = meta.get("warehouse_name", "")
            detail = f"Остаток: {old_value} → 0" + (f" ({warehouse})" if warehouse else "")
        elif event_type in ("STOCK_REPLENISH",):
            warehouse = meta.get("warehouse_name", "")
            delta = meta.get("delta", "")
            detail = f"+{delta} шт." + (f" ({warehouse})" if warehouse else "")
        elif event_type in ("STOCK_OUT_FBO_TOTAL", "STOCK_OUT_FBS_TOTAL"):
            supply = meta.get("supply_type", "")
            detail = f"Товар полностью закончился на всех складах {supply} (было {old_value} шт.)"
        elif event_type in ("OZON_SEO_CHANGE",):
            field = meta.get("field", "")
            field_label = {"title": "Заголовок", "description": "Описание"}.get(field, field)
            detail = f"{field_label} изменён"
        elif event_type in ("OZON_PHOTO_CHANGE",):
            field = meta.get("field", "")
            if field == "main_image":
                detail = "Главное фото изменено"
            elif field in ("gallery", "images_order"):
                detail = "Галерея изображений изменена"
            elif field == "images":
                detail = "Изображения изменены"
            else:
                detail = "Фото изменено"
        elif event_type in ("CONTENT_CHANGE",):
            detail = "Контент товара изменён"
        elif event_type in ("CONTENT_TITLE_CHANGED",):
            new_title = meta.get("new_title", "")
            detail = f"Новый заголовок: {new_title}" if new_title else "Заголовок изменён"
        elif event_type in ("CONTENT_DESC_CHANGED",):
            detail = "Описание товара изменено"
        elif event_type == "CONTENT_MAIN_PHOTO_CHANGED":
            old_count = meta.get("old_count", "?")
            new_count = meta.get("new_count", "?")
            if old_count != new_count:
                detail = f"Главное фото заменено. Фото: {old_count} → {new_count} шт."
            else:
                detail = "Главное фото заменено"
        elif event_type == "CONTENT_PHOTO_ADDED":
            old_count = meta.get("old_count", "?")
            new_count = meta.get("new_count", "?")
            diff = (new_count - old_count) if isinstance(new_count, int) and isinstance(old_count, int) else '?'
            detail = f"+{diff} фото ({old_count} → {new_count} шт.)"
        elif event_type == "CONTENT_PHOTO_REMOVED":
            old_count = meta.get("old_count", "?")
            new_count = meta.get("new_count", "?")
            diff = (old_count - new_count) if isinstance(new_count, int) and isinstance(old_count, int) else '?'
            detail = f"−{diff} фото ({old_count} → {new_count} шт.)"
        elif event_type == "CONTENT_PHOTO_ORDER_CHANGED":
            count = meta.get("new_count", "?")
            detail = f"Фото галереи заменены ({count} шт.)"

        day_key = created_at.strftime("%Y-%m-%d") if created_at else str(date.today())

        days_map[day_key].append({
            "id": ev_id,
            "created_at": created_at.isoformat() if created_at else None,
            "event_type": event_type,
            "category": event_category,
            "label": event_label,
            "detail": detail,
            "advert_id": adv_id_int if adv_id_int else None,
            "campaign_title": campaign_title,
            "old_value": old_value,
            "new_value": new_value,
            "product": product,
            "campaign_items": campaign_items_list if event_type in ("CAMPAIGN_CREATED", "OZON_CAMPAIGN_CREATED") else None,
        })

    # Sort days descending
    sorted_days = sorted(days_map.keys(), reverse=True)
    days_list = [
        {"date": day, "events": days_map[day]}
        for day in sorted_days
    ]

    return {
        "shop_id": shop_id,
        "marketplace": marketplace,
        "total": total,
        "page": page,
        "page_size": page_size,
        "days": days_list,
    }
