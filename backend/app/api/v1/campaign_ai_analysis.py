"""
Campaign AI Analysis — ИИ-анализ конкретной рекламной кампании с помощью Gemini 2.5 Flash.

POST /campaign-details/{marketplace}/{campaign_id}/ai-analysis  →  SSE streaming response

Анализирует:
- Прямой эффект рекламы (трафик → конверсии)
- Косвенный эффект (Halo) — влияние рекламы на органические продажи
- Влияние событий (ставки, цены, контент) на показатели
"""
import json
import logging
import os
from datetime import date, timedelta, datetime as dt_datetime
from typing import Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.clickhouse import get_clickhouse_client
from app.core.security import get_current_user
from app.models.user import User

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/campaign-details", tags=["Campaign AI Analysis"])

KIE_AI_URL = "https://api.kie.ai/gemini-2.5-flash/v1/chat/completions"

EVENT_LABELS = {
    "OZON_BID_CHANGE": "Ставка изменена", "OZON_STATUS_CHANGE": "Статус кампании",
    "OZON_BUDGET_CHANGE": "Бюджет кампании", "OZON_ITEM_ADD": "Товар добавлен",
    "OZON_ITEM_REMOVE": "Товар удалён", "OZON_CAMPAIGN_CREATED": "Кампания создана",
    "OZON_SEO_CHANGE": "SEO изменён", "OZON_PHOTO_CHANGE": "Фото изменено",
    "OZON_CONTENT_CHANGE": "Контент изменён", "OZON_PRICE_CHANGE": "Цена изменена",
    "OZON_STOCK_OUT": "Нет остатков", "OZON_STOCK_REPLENISH": "Поступление",
    "BID_CHANGE": "Ставка изменена", "STATUS_CHANGE": "Статус кампании",
    "ITEM_ADD": "Товар добавлен", "ITEM_REMOVE": "Товар удалён",
    "ITEM_INACTIVE": "Товар неактивен", "CAMPAIGN_CREATED": "Кампания создана",
    "CONTENT_CHANGE": "Контент изменён", "CONTENT_TITLE_CHANGED": "Заголовок",
    "CONTENT_DESC_CHANGED": "Описание", "CONTENT_MAIN_PHOTO_CHANGED": "Главное фото",
    "CONTENT_PHOTO_ADDED": "Фото добавлено", "CONTENT_PHOTO_REMOVED": "Фото удалено",
    "CONTENT_PHOTO_ORDER_CHANGED": "Галерея изменена",
    "PRICE_CHANGE": "Цена изменена",
    "STOCK_OUT": "Нет остатков", "STOCK_REPLENISH": "Поступление",
    "STOCK_OUT_FBO_TOTAL": "Нет остатков ФБО", "STOCK_OUT_FBS_TOTAL": "Нет остатков ФБС",
}

SYSTEM_PROMPT = """Ты — эксперт-аналитик рекламных кампаний на маркетплейсах (Ozon, Wildberries).

Тебе придут данные ОДНОЙ конкретной рекламной кампании: статистика по дням, события (изменения ставок/цен/контента), общие продажи товаров.

ГЛАВНАЯ ЗАДАЧА: Определить реальную ценность кампании для бизнеса.

АНАЛИЗ ПРЯМОГО ЭФФЕКТА:
- Как рекламный трафик конвертируется в заказы
- Тренды CTR, CPC, CPO, DRR по дням
- Как изменения ставок повлияли на объём трафика и конверсию
- Средние значения и аномалии

АНАЛИЗ КОСВЕННОГО ЭФФЕКТА (HALO EFFECT) — САМОЕ ВАЖНОЕ:
- Сравни рекламную выручку (ad_revenue) и общую выручку товара (product_revenue) по дням
- Если product_revenue значительно больше ad_revenue — реклама разогревает органику
- Ищи корреляции: больше показов/кликов → больше органических продаж через 1-3 дня
- Посчитай "мультипликатор": product_revenue / ad_revenue — сколько рублей общих продаж на каждый рубль рекламной выручки
- Даже если CPO высокий, но общие продажи растут с рекламой — кампания полезна
- Сравни дни с высоким расходом vs дни с низким расходом — отличается ли product_revenue?

АНАЛИЗ СОБЫТИЙ:
- Для каждого значимого события: что произошло → как изменились метрики в следующие 1-5 дней
- Изменение ставки → показы/клики/заказы
- Изменение цены товара → конверсия в заказ (CR)
- Изменение контента (фото/описание) → CTR
- Out-of-stock при работающей рекламе = слив бюджета

ФОРМАТ ОТВЕТА (используй emoji и markdown):

## 🎯 Вердикт
(Одно-два предложения: эффективна / частично эффективна / неэффективна. Почему.)

## 📊 Прямой эффект рекламы
(Средние CTR, CPC, CPO, DRR. Тренды — что растёт, что падает. Ключевые дни.)

## 🔄 Косвенный эффект (Halo)
(Мультипликатор: product_revenue / ad_revenue. Корреляции с лагом. Вывод: разогревает ли реклама органику.)

## ⚡ Влияние событий
(Для каждого ключевого события: дата → что изменилось → результат через N дней → вывод)

## 💡 Рекомендации
(Конкретные действия: изменить ставку до X₽, оптимальный расход Y₽/день, изменить цену, обновить контент)

## 📈 Оптимальные показатели
(При каком расходе/ставке лучший результат. Точка diminishing returns если видна.)

ПРАВИЛА:
- Называй товары по именам/артикулам, а не по ID
- Указывай конкретные даты и числа
- Ищи time-lagged корреляции (событие сегодня → эффект через 1-3 дня)
- Если данных мало — честно скажи, не придумывай корреляции
- Пиши на русском языке
- Будь кратким но содержательным
"""


def _to_date(v):
    """Normalize datetime→date."""
    return v.date() if isinstance(v, dt_datetime) else v


@router.post("/{marketplace}/{campaign_id}/ai-analysis")
async def analyze_campaign_ai(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(...),
    end_date: date = Query(...),
    sku: Optional[int] = Query(None),
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    AI-powered analysis of a specific campaign.
    Streams response from Gemini 2.5 Flash as SSE.
    """
    api_key = os.getenv("KIE_AI_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="KIE_AI_API_KEY not configured")

    mp = marketplace.lower()
    mp_label = "Ozon" if mp == "ozon" else "Wildberries"
    ch = get_clickhouse_client()

    try:
        # ── 1. Get campaign SKUs and shop_id ──
        if mp == "ozon":
            skus_r = ch.query(
                "SELECT DISTINCT sku, shop_id FROM mms_analytics.fact_ozon_ad_daily FINAL WHERE campaign_id = {cid:UInt64}",
                parameters={"cid": campaign_id}
            ).result_rows
        else:
            skus_r = ch.query(
                "SELECT DISTINCT nm_id, shop_id FROM mms_analytics.fact_advert_stats_v3 FINAL WHERE advert_id = {cid:UInt64}",
                parameters={"cid": campaign_id}
            ).result_rows

        if not skus_r:
            raise HTTPException(status_code=404, detail="Кампания не найдена в статистике")

        campaign_skus = [int(r[0]) for r in skus_r]
        shop_id = int(skus_r[0][1])
        filter_skus = [sku] if sku else campaign_skus

        # ── 2. Campaign stats by day (ad metrics) ──
        if mp == "ozon":
            sku_f = "AND sku = {sku:UInt64}" if sku else ""
            params = {"campaign_id": campaign_id, "start_date": start_date, "end_date": end_date}
            if sku:
                params["sku"] = sku
            stats_rows = ch.query(f"""
                SELECT dt, sum(views), sum(clicks), sum(orders), sum(add_to_cart),
                       sum(revenue), sum(money_spent)
                FROM mms_analytics.fact_ozon_ad_daily FINAL
                WHERE campaign_id = {{campaign_id:UInt64}}
                  AND dt BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f}
                GROUP BY dt ORDER BY dt
            """, parameters=params).result_rows
        else:
            sku_f = "AND nm_id = {sku:UInt64}" if sku else ""
            params = {"campaign_id": campaign_id, "start_date": start_date, "end_date": end_date}
            if sku:
                params["sku"] = sku
            stats_rows = ch.query(f"""
                SELECT date, sum(views), sum(clicks), sum(orders), 0,
                       sum(revenue), sum(spend)
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE advert_id = {{campaign_id:UInt64}}
                  AND date BETWEEN {{start_date:Date}} AND {{end_date:Date}} {sku_f}
                GROUP BY date ORDER BY date
            """, parameters=params).result_rows

        # ── 3. Product revenue by day (total orders, not just ad-attributed) ──
        if mp == "ozon":
            prod_rows = ch.query("""
                SELECT toDate(order_date) AS d, sum(price * quantity) AS rev, count() AS cnt
                FROM mms_analytics.fact_ozon_orders FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND sku IN {skus:Array(UInt64)}
                  AND toDate(order_date) BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY d ORDER BY d
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
                "start_date": start_date, "end_date": end_date
            }).result_rows
        else:
            prod_rows = ch.query("""
                SELECT toDate(date) AS d, sum(finishedPrice * quantity) AS rev, count() AS cnt
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nmId IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
                  AND isCancel = 0
                GROUP BY d ORDER BY d
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
                "start_date": start_date, "end_date": end_date
            }).result_rows

        ch.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("CH query failed for campaign AI analysis")
        raise HTTPException(status_code=500, detail=f"Ошибка запроса данных: {e}")

    # ── 4. Product names ──
    product_names: dict[int, str] = {}
    if mp == "ozon":
        # For Ozon, also map sku → product_id
        pg_res = await db.execute(
            text("SELECT product_id, sku, name, offer_id FROM dim_ozon_products WHERE shop_id = :sid AND sku = ANY(:skus)"),
            {"sid": shop_id, "skus": campaign_skus}
        )
        for r in pg_res.fetchall():
            pid, sku_val, name, offer = r[0], r[1], r[2] or "", r[3] or ""
            label = f"{name[:50]} ({offer})" if name and offer else name[:60] or offer or str(sku_val)
            product_names[sku_val] = label
            product_names[pid] = label
    else:
        pg_res = await db.execute(
            text("SELECT nm_id, imt_name, vendor_code FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:skus)"),
            {"sid": shop_id, "skus": campaign_skus}
        )
        for r in pg_res.fetchall():
            nmid, name, vc = r[0], r[1] or "", r[2] or ""
            label = f"{name[:50]} ({vc})" if name and vc else name[:60] or str(nmid)
            product_names[nmid] = label

    product_list = ", ".join(product_names[s] for s in campaign_skus if s in product_names)

    # ── 5. Events ──
    # For Ozon: convert sku → product_id for event_log
    if mp == "ozon":
        pg_sku_res = await db.execute(
            text("SELECT product_id, sku FROM dim_ozon_products WHERE shop_id = :sid AND sku = ANY(:skus)"),
            {"sid": shop_id, "skus": campaign_skus}
        )
        event_nm_ids = list(set(
            [int(r[0]) for r in pg_sku_res.fetchall()] + campaign_skus
        ))
    else:
        event_nm_ids = campaign_skus

    nm_ids_str = [str(x) for x in event_nm_ids]
    events_result = await db.execute(
        text("""
            SELECT created_at, event_type, nm_id, old_value, new_value
            FROM event_log
            WHERE nm_id::text = ANY(:skus)
              AND shop_id = :shop_id
              AND created_at::date >= :start_date
              AND created_at::date <= :end_date
            ORDER BY created_at
        """),
        {"skus": nm_ids_str, "shop_id": shop_id,
         "start_date": start_date, "end_date": end_date}
    )
    events_raw = events_result.fetchall()

    # Format events
    events_lines = []
    for ev in events_raw:
        created, etype, nm_id, old_val, new_val = ev
        ev_date = created.strftime("%Y-%m-%d") if created else ""
        label = EVENT_LABELS.get(etype, etype)
        pname = product_names.get(nm_id, str(nm_id)) if nm_id else ""
        parts = [f"[{ev_date}] {label}"]
        if pname:
            parts.append(f"товар: {pname}")
        if old_val and new_val:
            parts.append(f"{old_val} → {new_val}")
        elif new_val:
            parts.append(new_val)
        events_lines.append(" | ".join(parts))

    if len(events_lines) > 200:
        events_lines = events_lines[:100] + ["... (пропущено) ..."] + events_lines[-100:]

    # ── 6. Build stats table ──
    ad_by_date = {}
    for r in stats_rows:
        dt_val = _to_date(r[0])
        ad_by_date[str(dt_val)] = {
            "views": int(r[1]), "clicks": int(r[2]), "ad_orders": int(r[3]),
            "cart": int(r[4]), "ad_revenue": float(r[5]), "spend": float(r[6]),
        }

    prod_by_date = {}
    for r in prod_rows:
        dt_val = _to_date(r[0])
        prod_by_date[str(dt_val)] = {"prod_revenue": float(r[1]), "prod_orders": int(r[2])}

    all_dates = sorted(set(list(ad_by_date.keys()) + list(prod_by_date.keys())))

    stats_header = "Дата | Показы | Клики | CTR% | Рекл.заказы | Корзины | Рекл.выручка | Расход | DRR% | CPC | Общ.заказы | Общ.выручка | Мульт."
    stats_sep = "---|---|---|---|---|---|---|---|---|---|---|---|---"
    stats_lines = [stats_header, stats_sep]

    total_spend = 0
    total_ad_rev = 0
    total_prod_rev = 0

    for dt_str in all_dates:
        ad = ad_by_date.get(dt_str, {"views": 0, "clicks": 0, "ad_orders": 0, "cart": 0, "ad_revenue": 0, "spend": 0})
        pr = prod_by_date.get(dt_str, {"prod_revenue": 0, "prod_orders": 0})

        views, clicks = ad["views"], ad["clicks"]
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        cpc = round(ad["spend"] / clicks, 1) if clicks > 0 else 0
        drr = round(ad["spend"] / ad["ad_revenue"] * 100, 1) if ad["ad_revenue"] > 0 else 0
        mult = round(pr["prod_revenue"] / ad["ad_revenue"], 1) if ad["ad_revenue"] > 0 else 0

        total_spend += ad["spend"]
        total_ad_rev += ad["ad_revenue"]
        total_prod_rev += pr["prod_revenue"]

        stats_lines.append(
            f"{dt_str} | {views} | {clicks} | {ctr}% | {ad['ad_orders']} | {ad['cart']} | "
            f"{ad['ad_revenue']:.0f}₽ | {ad['spend']:.0f}₽ | {drr}% | {cpc}₽ | "
            f"{pr['prod_orders']} | {pr['prod_revenue']:.0f}₽ | x{mult}"
        )

    # Totals
    total_mult = round(total_prod_rev / total_ad_rev, 1) if total_ad_rev > 0 else 0
    total_drr = round(total_spend / total_ad_rev * 100, 1) if total_ad_rev > 0 else 0

    # ── 7. Build user prompt ──
    user_message = f"""Кампания: ID {campaign_id}
Маркетплейс: {mp_label}
Период: {start_date} — {end_date} ({len(all_dates)} дней с данными)
Товары в кампании: {product_list}

ИТОГО за период:
- Расход: {total_spend:.0f}₽
- Рекламная выручка: {total_ad_rev:.0f}₽
- Общая выручка товаров: {total_prod_rev:.0f}₽
- DRR (рекл.): {total_drr}%
- Мультипликатор (общая/рекл. выручка): x{total_mult}

### СТАТИСТИКА ПО ДНЯМ:
{chr(10).join(stats_lines)}

### СОБЫТИЯ ({len(events_raw)} шт.):
{chr(10).join(events_lines) if events_lines else "Событий не найдено"}

Проанализируй ВСЕ данные. Особое внимание — косвенному эффекту: как рекламный трафик влияет на общие продажи товара."""

    # ── 8. Stream from Gemini ──
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
                logger.exception("Gemini streaming error for campaign analysis")
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
