"""
Events AI Analysis — ИИ-анализ событий с помощью Gemini 2.5 Flash.

POST /events/analysis  →  SSE streaming response
"""
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["Events Analysis"])

KIE_AI_URL = "https://api.kie.ai/gemini-2.5-flash/v1/chat/completions"

PERIOD_DAYS = {"7d": 7, "30d": 30, "90d": 90}

EVENT_CATEGORIES = {
    "BID_CHANGE": "Реклама", "STATUS_CHANGE": "Реклама",
    "ITEM_ADD": "Реклама", "ITEM_REMOVE": "Реклама",
    "ITEM_INACTIVE": "Реклама", "CAMPAIGN_CREATED": "Реклама",
    "OZON_BID_CHANGE": "Реклама", "OZON_STATUS_CHANGE": "Реклама",
    "OZON_BUDGET_CHANGE": "Реклама", "OZON_ITEM_ADD": "Реклама",
    "OZON_ITEM_REMOVE": "Реклама", "OZON_CAMPAIGN_CREATED": "Реклама",
    "OZON_SEO_CHANGE": "Контент", "OZON_PHOTO_CHANGE": "Контент",
    "OZON_CONTENT_CHANGE": "Контент", "CONTENT_CHANGE": "Контент",
    "CONTENT_TITLE_CHANGED": "Контент", "CONTENT_DESC_CHANGED": "Контент",
    "CONTENT_MAIN_PHOTO_CHANGED": "Контент", "CONTENT_PHOTO_ADDED": "Контент",
    "CONTENT_PHOTO_REMOVED": "Контент", "CONTENT_PHOTO_ORDER_CHANGED": "Контент",
    "PRICE_CHANGE": "Коммерция", "OZON_PRICE_CHANGE": "Коммерция",
    "OZON_STOCK_OUT": "Склад", "OZON_STOCK_REPLENISH": "Склад",
    "STOCK_OUT": "Склад", "STOCK_REPLENISH": "Склад",
    "STOCK_OUT_FBO_TOTAL": "Склад", "STOCK_OUT_FBS_TOTAL": "Склад",
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


SYSTEM_PROMPT = """Ты — эксперт-аналитик маркетплейсов (Ozon, Wildberries). 
Тебе придут данные о событиях, KPI-метриках, каталоге товаров и рекламной воронке магазина.

ДАННЫЕ:
- КАТАЛОГ ТОВАРОВ — список товаров с артикулами продавца и текущими ценами
- СОБЫТИЯ — действия над товарами (смена ставок, цен, фото, остатки), привязанные к конкретным товарам
- KPI — заказы, выручка, показы, клики, корзины, рекламный расход, DRR
- ВОРОНКА ПО ТОВАРАМ — рекламные показы, клики, корзины, заказы, расход, CTR, CR для каждого товара

Твоя задача:
1. Проанализировать ВСЕ данные включая воронку по каждому товару
2. Найти причинно-следственные связи между событиями и изменениями KPI
3. Оценить эффективность рекламы по каждому товару (CTR, CR, DRR)
4. Выделить ключевые инсайты
5. Дать конкретные рекомендации по каждому проблемному товару

ФОРМАТ ОТВЕТА (используй именно такой формат с emoji):

## 🔍 Ключевые находки
(Что произошло, какие корреляции ты видишь. Называй товары по именам!)

## 📦 Анализ по товарам
(Для ключевых товаров — как изменились CTR, конверсия, заказы после событий)

## ⚠️ Предупреждения  
(На что обратить внимание: высокий DRR, низкая конверсия, отсутствие остатков при работающей рекламе)

## 💡 Рекомендации
(Конкретные действия ПО КАЖДОМУ проблемному товару: увеличить/уменьшить ставки, изменить цену, пополнить склад)

## 📊 Общая оценка периода
(Краткое резюме: как дела у магазина за этот период)

ПРАВИЛА:
- НАЗЫВАЙ ТОВАРЫ ПО ИМЕНАМ/АРТИКУЛАМ, а не по ID
- Анализируй воронку: если CTR < 1% — проблема с контентом/ключевыми словами
- Если CR в корзину < 5% — проблема с ценой или карточкой
- Если CR в заказ < 10% — проблема с ценой/конкурентами
- Анализируй КОРРЕЛЯЦИИ: если после события X через 1-3 дня метрика Y изменилась — укажи это
- Будь конкретен: называй даты, числа, процент изменений
- Если после смены фото/заголовка выросли просмотры — это инсайт
- Если после роста ставки продажи не выросли — это предупреждение  
- Если товар закончился, а реклама продолжает крутиться — это проблема
- Если DRR > 15% — обрати на это внимание
- Пиши на русском языке
- Будь кратким но конкретным, не лей воду
"""


def _format_event_for_prompt(event_type: str, old_val: str | None,
                              new_val: str | None, event_date: str,
                              product_name: str | None = None) -> str:
    """Format single event for LLM prompt."""
    label = EVENT_LABELS.get(event_type, event_type)
    cat = EVENT_CATEGORIES.get(event_type, "Другое")
    parts = [f"[{event_date}] [{cat}] {label}"]
    if product_name:
        parts.append(f"товар: {product_name}")
    if old_val and new_val:
        parts.append(f"{old_val} → {new_val}")
    elif new_val:
        parts.append(new_val)
    return " | ".join(parts)


def _ch_group_expr(group_by: str, date_col: str) -> str:
    if group_by == "week":
        return f"toMonday({date_col})"
    elif group_by == "month":
        return f"toStartOfMonth({date_col})"
    return date_col


# ── Endpoint ────────────────────────────────────────────────────

@router.post("/analysis")
async def analyze_events(
    shop_id: int = Query(..., description="Shop ID"),
    period: str = Query("30d", description="Period: 7d, 30d, 90d"),
    group_by: str = Query("day", description="Grouping: day, week, month"),
    date_from: Optional[date] = Query(None),
    date_to: Optional[date] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    AI-powered analysis of events and KPI metrics.
    Streams response from Gemini 2.5 Flash as SSE.
    """
    from app.core.clickhouse import get_clickhouse_client
    from sqlalchemy import select
    from app.models.shop import Shop

    api_key = os.getenv("KIE_AI_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="KIE_AI_API_KEY not configured")

    # ── Verify shop ──
    result = await db.execute(
        select(Shop).where(Shop.id == shop_id, Shop.user_id == current_user.id)
    )
    shop = result.scalar_one_or_none()
    if not shop:
        raise HTTPException(status_code=404, detail="Магазин не найден")

    marketplace = shop.marketplace
    mp_label = "Ozon" if marketplace == "ozon" else "Wildberries"

    # ── Date range ──
    if date_from and date_to:
        start_date, end_date = date_from, date_to
    else:
        days = PERIOD_DAYS.get(period, 30)
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

    if group_by not in ("day", "week", "month"):
        group_by = "day"

    # ── 1. Fetch product catalog ──
    product_names: dict[int, str] = {}  # nm_id/product_id → "name (vendor_code)"
    if marketplace == "ozon":
        prods_raw = await db.execute(
            text("SELECT product_id, name, offer_id FROM dim_ozon_products WHERE shop_id = :sid"),
            {"sid": shop_id},
        )
        for row in prods_raw.fetchall():
            pid, name, offer = row[0], row[1] or "", row[2] or ""
            label = name[:60] if name else offer
            if offer and name:
                label = f"{name[:50]} ({offer})"
            product_names[pid] = label
    else:
        prods_raw = await db.execute(
            text("SELECT nm_id, name, vendor_code FROM dim_products WHERE shop_id = :sid"),
            {"sid": shop_id},
        )
        for row in prods_raw.fetchall():
            nmid, name, vc = row[0], row[1] or "", row[2] or ""
            label = name[:60] if name else str(nmid)
            if vc and name:
                label = f"{name[:50]} ({vc})"
            product_names[nmid] = label

    # Build product catalog text
    catalog_lines = ["Артикул | Название | ID"]
    catalog_lines.append("---|---|---")
    for pid, label in sorted(product_names.items(), key=lambda x: x[1]):
        if marketplace == "ozon":
            # Find offer_id
            parts = label.rsplit(" (", 1)
            art = parts[1].rstrip(")") if len(parts) > 1 else ""
            name = parts[0]
        else:
            parts = label.rsplit(" (", 1)
            art = parts[1].rstrip(")") if len(parts) > 1 else ""
            name = parts[0]
        catalog_lines.append(f"{art} | {name} | {pid}")
    # Limit catalog to 100 products
    if len(catalog_lines) > 102:
        catalog_lines = catalog_lines[:52] + ["..."] + catalog_lines[-50:]

    # ── 2. Fetch events (with product names) ──
    events_raw = await db.execute(
        text("""
            SELECT
                created_at::date AS event_date,
                event_type,
                old_value,
                new_value,
                nm_id
            FROM event_log
            WHERE shop_id = :shop_id
              AND created_at::date >= :start_date
              AND created_at::date <= :end_date
            ORDER BY created_at
        """),
        {"shop_id": shop_id, "start_date": start_date, "end_date": end_date},
    )
    events_rows = events_raw.fetchall()

    # Format events for prompt — with product names
    events_text_lines = []
    for row in events_rows:
        ev_date = row[0]
        if isinstance(ev_date, datetime):
            ev_date = ev_date.date()
        nm_id = row[4]
        pname = product_names.get(nm_id) if nm_id else None
        events_text_lines.append(
            _format_event_for_prompt(row[1], row[2], row[3], str(ev_date), pname)
        )

    events_summary = f"Всего событий: {len(events_text_lines)}"
    # Limit to ~300 events to not exceed context
    if len(events_text_lines) > 300:
        events_text_lines = events_text_lines[:150] + ["... (пропущено) ..."] + events_text_lines[-150:]

    # ── 3. Fetch KPI + per-product funnel ──
    funnel_rows: list = []
    try:
        ch = get_clickhouse_client()

        grp_key = _ch_group_expr(group_by, "toDate(addHours(in_process_at, 3))" if marketplace == "ozon" else "toDate(addHours(date, 3))")
        grp_ads = _ch_group_expr(group_by, "dt" if marketplace == "ozon" else "date")

        if marketplace == "ozon":
            orders_rows = ch.query(f"""
                SELECT
                    {grp_key} AS bucket,
                    count() AS orders_count,
                    sum(price * quantity) AS revenue
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND toDate(addHours(in_process_at, 3)) >= {{start:Date}}
                  AND toDate(addHours(in_process_at, 3)) <= {{end:Date}}
                GROUP BY bucket ORDER BY bucket
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows

            ads_rows = ch.query(f"""
                SELECT
                    {grp_ads} AS bucket,
                    sum(views) AS views,
                    sum(clicks) AS clicks,
                    sum(add_to_cart) AS carts,
                    sum(money_spent) AS ad_spend,
                    sum(orders) AS ad_orders
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND dt >= {{start:Date}} AND dt <= {{end:Date}}
                GROUP BY bucket ORDER BY bucket
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows
        else:
            orders_rows = ch.query(f"""
                SELECT
                    {grp_key} AS bucket,
                    count() AS orders_count,
                    sum(price_with_disc) AS revenue
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND toDate(addHours(date, 3)) >= {{start:Date}}
                  AND toDate(addHours(date, 3)) <= {{end:Date}}
                GROUP BY bucket ORDER BY bucket
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows

            ads_rows = ch.query(f"""
                SELECT
                    {grp_ads} AS bucket,
                    sum(views) AS views,
                    sum(clicks) AS clicks,
                    sum(atbs) AS carts,
                    sum(spend) AS ad_spend,
                    sum(orders) AS ad_orders
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {{shop_id:UInt32}}
                  AND date >= {{start:Date}} AND date <= {{end:Date}}
                GROUP BY bucket ORDER BY bucket
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows

            # Per-product funnel (WB: nm_id)
            funnel_rows = ch.query("""
                SELECT
                    nm_id AS sku_id,
                    sum(views) AS views,
                    sum(clicks) AS clicks,
                    sum(atbs) AS carts,
                    sum(orders) AS orders,
                    sum(spend) AS spend
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND date >= {start:Date} AND date <= {end:Date}
                GROUP BY sku_id
                HAVING views > 0
                ORDER BY spend DESC
                LIMIT 50
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows

        # Per-product funnel for Ozon (add after ozon ads_rows)
        if marketplace == "ozon" and not funnel_rows:
            funnel_rows = ch.query("""
                SELECT
                    sku AS sku_id,
                    sum(views) AS views,
                    sum(clicks) AS clicks,
                    sum(add_to_cart) AS carts,
                    sum(orders) AS orders,
                    sum(money_spent) AS spend
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND dt >= {start:Date} AND dt <= {end:Date}
                GROUP BY sku_id
                HAVING views > 0
                ORDER BY spend DESC
                LIMIT 50
            """, parameters={"shop_id": shop_id, "start": start_date, "end": end_date}).result_rows

        ch.close()
    except Exception as e:
        logger.warning("CH query failed for analysis: %s", e)
        orders_rows = []
        ads_rows = []
        funnel_rows = []

    # Format KPI table
    kpi_lines = ["Дата | Заказы | Выручка | Показы | Клики | Корзины | Расход рекл. | DRR%"]
    kpi_lines.append("---|---|---|---|---|---|---|---")

    kpi_map: dict[str, dict] = {}
    for row in orders_rows:
        key = str(row[0])
        kpi_map[key] = {"orders": int(row[1]), "revenue": float(row[2]),
                        "views": 0, "clicks": 0, "carts": 0, "ad_spend": 0}
    for row in ads_rows:
        key = str(row[0])
        if key not in kpi_map:
            kpi_map[key] = {"orders": 0, "revenue": 0, "views": 0, "clicks": 0,
                            "carts": 0, "ad_spend": 0}
        kpi_map[key]["views"] = int(row[1])
        kpi_map[key]["clicks"] = int(row[2])
        kpi_map[key]["carts"] = int(row[3])
        kpi_map[key]["ad_spend"] = float(row[4])

    for key in sorted(kpi_map.keys()):
        m = kpi_map[key]
        drr = round(m["ad_spend"] / m["revenue"] * 100, 1) if m["revenue"] > 0 else 0
        kpi_lines.append(
            f"{key} | {m['orders']} | {m['revenue']:.0f}₽ | {m['views']} | "
            f"{m['clicks']} | {m['carts']} | {m['ad_spend']:.0f}₽ | {drr}%"
        )

    # Format per-product funnel table
    funnel_lines = ["Товар | Показы | Клики | CTR% | Корзины | CR→корз% | Заказы | CR→заказ% | Расход"]
    funnel_lines.append("---|---|---|---|---|---|---|---|---")

    # For Ozon, resolve sku → product_id
    sku_to_product: dict[int, int] = {}
    if marketplace == "ozon" and funnel_rows:
        try:
            sku_ids = [int(r[0]) for r in funnel_rows]
            sku_map_raw = await db.execute(
                text("SELECT sku, product_id FROM dim_ozon_products WHERE shop_id = :sid AND sku = ANY(:skus)"),
                {"sid": shop_id, "skus": sku_ids},
            )
            for r in sku_map_raw.fetchall():
                sku_to_product[r[0]] = r[1]
        except Exception:
            pass

    for row in funnel_rows:
        sku_id = int(row[0])
        views, clicks, carts, orders_cnt, spend = int(row[1]), int(row[2]), int(row[3]), int(row[4]), float(row[5])
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        cr_cart = round(carts / clicks * 100, 1) if clicks > 0 else 0
        cr_order = round(orders_cnt / carts * 100, 1) if carts > 0 else 0

        # Resolve product name
        if marketplace == "ozon":
            pid = sku_to_product.get(sku_id, sku_id)
            pname = product_names.get(pid, f"SKU {sku_id}")
        else:
            pname = product_names.get(sku_id, f"nm_id {sku_id}")

        funnel_lines.append(
            f"{pname[:45]} | {views} | {clicks} | {ctr}% | {carts} | {cr_cart}% | "
            f"{orders_cnt} | {cr_order}% | {spend:.0f}₽"
        )

    # ── 4. Build prompt ──
    user_message = f"""Магазин: {shop.name} ({mp_label})
Период: {start_date} — {end_date}
{events_summary}
Товаров в каталоге: {len(product_names)}

### КАТАЛОГ ТОВАРОВ:
{chr(10).join(catalog_lines)}

### СОБЫТИЯ (привязаны к товарам):
{chr(10).join(events_text_lines)}

### KPI МЕТРИКИ МАГАЗИНА (по дням/неделям/месяцам):
{chr(10).join(kpi_lines)}

### РЕКЛАМНАЯ ВОРОНКА ПО ТОВАРАМ (за весь период):
{chr(10).join(funnel_lines)}

Проанализируй ВСЕ данные, включая воронку по каждому товару. Называй товары по именам!"""

    # ── 5. Stream from Gemini ──
    async def generate():
        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                async with client.stream(
                    "POST",
                    KIE_AI_URL,
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {api_key}",
                    },
                    json={
                        "messages": [
                            {"role": "system", "content": [{"type": "text", "text": SYSTEM_PROMPT}]},
                            {"role": "user", "content": [{"type": "text", "text": user_message}]},
                        ],
                        "stream": True,
                        "include_thoughts": False,
                    },
                ) as response:
                    if response.status_code != 200:
                        body = await response.aread()
                        logger.error("Gemini API error %s: %s", response.status_code, body[:500])
                        yield f"data: {json.dumps({'error': f'API error {response.status_code}'})}\n\n"
                        return

                    async for line in response.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        payload = line[6:]
                        if payload.strip() == "[DONE]":
                            yield "data: [DONE]\n\n"
                            break
                        try:
                            chunk = json.loads(payload)
                            choices = chunk.get("choices", [])
                            if not choices:
                                continue
                            delta = choices[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield f"data: {json.dumps({'content': content})}\n\n"
                        except json.JSONDecodeError:
                            continue
            except httpx.ReadTimeout:
                yield f"data: {json.dumps({'error': 'Timeout — модель думает слишком долго'})}\n\n"
            except Exception as e:
                logger.exception("Gemini streaming error")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
