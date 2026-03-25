"""
Campaign AI Analysis — ИИ-анализ конкретной рекламной кампании с помощью Gemini 2.5 Flash.

POST /campaign-details/{marketplace}/{campaign_id}/ai-analysis  →  SSE streaming response

Анализирует:
- Полную юнит-экономику (себестоимость, комиссия, логистика, payout)
- Прямой эффект рекламы (трафик → конверсии) 
- Конверсионную модель (CR vs цена, порог алгоритма)
- Price Index (GREEN/YELLOW/RED) и его влияние на алгоритм
- Ключевые фразы (качество таргетинга)
- Retention / повторные покупки vs Halo Effect
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

SYSTEM_PROMPT = """Ты — эксперт-аналитик рекламных кампаний на Ozon и Wildberries. Анализируешь данные одной кампании и даёшь конкретную стратегию.

## ОГРАНИЧЕНИЯ РЕКЛАМЫ НА OZON (СТРОГО СОБЛЮДАЙ!)

### Что МОЖНО настроить:
- Ставка CPC (одна на всю кампанию)
- Недельный бюджет (ТОЛЬКО недельный, НЕ дневной!)
- Включить/выключить кампанию
- Цена товара (base_price / marketing_price)
- Стратегия (поиск / рекомендации)
- БОЛЬШЕ НИЧЕГО! Никаких минус-фраз, никакой фильтрации фраз!

### Что НЕЛЬЗЯ сделать:
- НЕЛЬЗЯ вручную подбирать ключевые фразы! Ozon Performance Max подбирает автоматически
- НЕЛЬЗЯ задать дневной бюджет — только НЕДЕЛЬНЫЙ
- НЕЛЬЗЯ создавать группы объявлений
- НЕЛЬЗЯ отключить Performance Max или переключить тип кампании
- НЕЛЬЗЯ добавлять минус-фразы — такого функционала НЕТ на Ozon!
- НЕЛЬЗЯ фильтровать или исключать фразы — НЕВОЗМОЖНО!
- НЕЛЬЗЯ таргетировать по конкретным фразам

### Как управлять качеством фраз (ЕДИНСТВЕННЫЙ рычаг!):
1. Снизить ставку CPC → алгоритм не покупает дорогие общие запросы → остаются точные
2. Получить GREEN price_index → алгоритм перестаёт подмешивать мусор

## АЛГОРИТМ OZON — КЛЮЧЕВЫЕ ЗНАНИЯ

### Цены на Ozon (ВАЖНО — используй правильные названия!)
- «Ваша цена» (price) — сколько продавец получит на руки. Это payout (после комиссий и логистики). Это НЕ то, что видит покупатель!
- «Цена до скидки» (old_price) — маркетинговая цена, base_price для расчёта комиссии
- «Минимальная цена» (min_price) — минимальный порог для участия в автоакциях Ozon. НЕ путай с ценой конкурента!
- «Цена для покупателя» (marketing_price) — ВНИМАНИЕ: покупатель часто видит ДРУГУЮ, ещё более низкую цену!
- КРИТИЧЕСКИ ВАЖНО: Ozon применяет свои субсидии (аналог СПП на Wildberries). Покупатель может видеть цену на 30-50% ниже «Цены до скидки». Например: «Цена до скидки» = 6000₽, а покупатель видит 3050₽ или 2761₽ с Ozon Картой. Ozon субсидирует разницу!
- Мы НЕ можем получить реальную цену для покупателя через API. Она видна только на сайте Ozon.
- Комиссия = % от «Цена до скидки» (old_price), логистика фиксированная
- Price Index считается от реальной цены покупателя (с субсидиями Ozon), а НЕ от «Ваша цена»

### Price Index (КРИТИЧЕСКИ ВАЖНО)
- Формула: реальная_цена_покупателя / min_цена_конкурента (по всем площадкам)
- GREEN/SUPER (≤0.95): бейдж «Супервыгодный индекс», буст выдачи
- YELLOW (0.95-1.10): нейтрально
- RED (>1.10): пессимизация
- ВАЖНО! min_цена_конкурента часто = цена СВОЕГО ЖЕ товара на WB! Не настоящий конкурент. Решение: поднять цену на WB
- min_price из API — это НЕ цена конкурента, а порог для автоакций Ozon!

### ПРАВИЛА ЛОГИКИ ПРИ АНАЛИЗЕ (ОБЯЗАТЕЛЬНО!)
- Снижение цены НЕ МОЖЕТ убивать продажи. Если цена снизилась и продажи упали — это СОВПАДЕНИЕ (корреляция ≠ причинность). Ищи другие причины: сезонность, конкуренция, стоки, изменения в рекламе
- При малом количестве данных (менее 30 заказов за период) — делай оговорку о статистической незначимости
- НЕ делай ложных причинно-следственных связей. Если два события совпали по времени — это НЕ значит что одно вызвало другое
- Повышение цены МОЖЕТ снижать конверсию (ценовая эластичность). Но снижение цены ВСЕГДА нейтрально или положительно для спроса

### Стратегии рекламы на Ozon
- Поиск: товар показывается в результатах поиска
- Рекомендации: товар показывается в карточках конкурентов/похожих
- Выбор стратегии влияет на тип трафика и конверсию
- При высоком Price Index лучше работают рекомендации (менее ценозависимые)

### Retention ≠ Halo
- Часть «органических» = повторные покупки (retention) — им реклама НЕ нужна
- Halo = НОВЫЕ покупатели через органику благодаря рекламной видимости
- При расчёте эффективности учитывай LTV (если retention высокий, допустимый CAC выше)

## ФОРМАТ ОТВЕТА — JSON

Верни ответ СТРОГО в JSON формате. Каждая секция — отдельный объект. Массив секций.
Не добавляй текст ВНЕ JSON. Не оборачивай в ```json```. Только чистый JSON-массив.

[
  {
    "id": "verdict",
    "title": "🎯 Вердикт",
    "content": "2-3 предложения. P&L одной цифрой. Эффективна/убыточна/потенциальна.",
    "type": "verdict",
    "status": "negative|neutral|positive"
  },
  {
    "id": "unit_economics",
    "title": "💰 Юнит-экономика",
    "content": "Если несколько товаров — делай ОТДЕЛЬНУЮ таблицу для КАЖДОГО товара! Не усредняй!\nДля каждого товара используй markdown-таблицу:\nПараметр | Сумма (₽) | Доля от выручки (%)\n---|---|---\nBase price (выручка) | X | 100\nКомиссия Ozon | -X | -X%\nЛогистика+Обработка | -X | -X%\nЭквайринг | -X | -X%\nБонусы продавца | -X | -X%\nЧистый Payout | X | X%\nСебестоимость | -X | -X%\n**Прибыль до рекламы** | **X** | **X%**\nДопустимый CAC (безубыт.) | X | —\nРеальный CAC (прямой) | X | —\nCAC эффективный (Halo) | X | —",
    "type": "section"
  },
  {
    "id": "conversion_model",
    "title": "📊 Конверсия vs Цена",
    "content": "Краткий текст: при какой цене CR работает, при какой обрывается. Порог.",
    "type": "section"
  },
  {
    "id": "price_index",
    "title": "🔍 Price Index",
    "content": "Текущий индекс. Самоконкуренция с WB? Как получить GREEN — конкретная цена на WB.",
    "type": "section"
  },
  {
    "id": "keywords",
    "title": "🔑 Ключевые фразы",
    "content": "% мусора (по CTR и релевантности). 3-5 примеров нерелевантных фраз. ВАЖНО: мы НЕ знаем заказы/конверсию по конкретным фразам! Только показы, клики, CTR. Не придумывай данные по заказам!",
    "type": "section"
  },
  {
    "id": "ad_effect",
    "title": "📈 Реклама",
    "content": "Средние CTR, CPC, CR, DRR. Ключевые дни.",
    "type": "section"
  },
  {
    "id": "halo_retention",
    "title": "🔄 Halo vs Retention",
    "content": "Используй ДАННЫЕ ПО РЕТЕНШЕНУ из запроса! Укажи: % повторных покупателей, среднее число заказов на покупателя, средний LTV повторного клиента, дни между покупками. Рассчитай эффективный CAC с учётом повторных покупок: если клиент покупает X раз, то CAC за привлечение делится на X. Чем выше retention — тем более оправданы высокие расходы на рекламу.",
    "type": "section"
  },
  {
    "id": "events",
    "title": "⚡ События",
    "content": "Для каждого ключевого: дата → изменение → результат. Кратко.",
    "type": "section"
  },
  {
    "id": "strategy",
    "title": "💡 Стратегия",
    "content": "КОНКРЕТНЫЕ цифры. Цена: X₽ base_price, Y₽ на WB. Ставка: Z₽. Недельный бюджет: W₽. Тактика: постоянная/импульсная. P&L: заказов/мес → прибыль/мес → %.",
    "type": "strategy",
    "actions": [
      {"action": "Описание действия", "value": "конкретное значение", "priority": "high|medium|low"}
    ]
  }
]

ПРАВИЛА:
- Каждая секция = КРАТКИЙ текст (3-7 предложений, НЕ портянка)
- Называй товары по именам/артикулам
- Конкретные даты и числа из данных
- НЕ рекомендуй ручной подбор фраз — это НЕВОЗМОЖНО на Ozon!
- НЕ рекомендуй минус-фразы — такого функционала НЕТ на Ozon!
- НЕ рекомендуй фильтровать фразы — НЕЛЬЗЯ!
- Рекомендуй ТОЛЬКО: ставку CPC, недельный бюджет, цену товара, стратегию (поиск/рекомендации), вкл/выкл кампанию. БОЛЬШЕ НИЧЕГО!
- Цель — НЕ фиксированные 20%. Цель — максимальная прибыль при масштабировании! Найди оптимальный баланс цены/ставки/бюджета для макс. прибыли
- DRR считай и от рекл.выручки, и от ОБЩЕЙ выручки (с учётом органики/Halo)
- КРИТИЧЕСКИ ВАЖНО — ЗАКАЗЫ: рекламные заказы УЖЕ ВКЛЮЧЕНЫ в общие! Если общих 31, из них 10 рекламных — значит органических 21. НЕ СКЛАДЫВАЙ 31+10! Итого заказов = 31, не 41!
- Себестоимость: ИСПОЛЬЗУЙ ТОЛЬКО число из данных, НЕ придумывай. Считай: прибыль до рекламы = payout - себестоимость
- Общая себестоимость за период = себестоимость/шт × ВСЕГО заказов (НЕ × рекламных!)
- По ключевым фразам: мы НЕ знаем заказы/конверсию по конкретным фразам! Только показы, клики, CTR. НЕ ПРИДУМЫВАЙ!
- Используй правильные названия цен Ozon. Помни: покупатель видит ДРУГУЮ цену (после субсидий Ozon), мы её не знаем из API!
- НЕ ПУТАЙ min_price с ценой конкурента! min_price — это порог автоакций Ozon
- НЕ ДЕЛАЙ ложных выводов из корреляций. Снижение цены НЕ может снижать продажи!
- Пиши на русском языке
- Будь КРАТКИМ. Бизнесу нужны цифры и действия, не рассуждения.
"""

SYSTEM_PROMPT_WB = """Ты — эксперт-аналитик рекламных кампаний на Wildberries. Анализируешь данные одной кампании и даёшь конкретную стратегию.

## ОГРАНИЧЕНИЯ И ВОЗМОЖНОСТИ РЕКЛАМЫ НА WILDBERRIES

### Что МОЖНО настроить:
- Ставка CPM (в КОПЕЙКАХ! 1000 коп = 10₽). Минимум 125 коп в Поиске, 100 коп в Каталоге
- Бюджет кампании (общий, не дневной/недельный)
- Места размещения: Поиск (search) и/или Рекомендации (catalog/recommendations) — можно включать оба или один
- Минус-фразы (до 1000 шт.) — ИСКЛЮЧИТЬ нерелевантные запросы
- Фиксированные фразы — ЗАКРЕПИТЬ важные запросы
- Тип кампании: Единая (автоматическая), Поиск, Каталог, Рекомендации, Карточка
- Товары в кампании — добавить/убрать
- Включить/выключить/приостановить кампанию

### WB-специфика:
- **СПП (Скидка Постоянного Покупателя)**: WB автоматически применяет скидку, покупатель видит цену НИЖЕ установленной. СПП = 0-30%, мы не контролируем. Цена для покупателя ≈ цена_на_сайте × (1 - СПП%)
- **Ставки CPM в копейках**: WB API принимает ставки в копейках. 1000 коп = 10₽ CPM
- **revenue из fact_advert_stats**: это выручка по заказам, атрибутированным к рекламе WB (модель последнего касания)
- **Нет Price Index**: на WB нет аналога Ozon Price Index
- **3 типа продаж**:
  - **Прямые (direct)** — рекламируемые SKU (views/clicks/spend > 0)
  - **Модель (model)** — другие товары из той же объединённой карточки (imt_id). Покупатель видит рекламу одного товара, но покупает другой размер/цвет
  - **Ассоциированные (associated)** — товары из других карточек (сross-sell)
- **Объединённая карточка (imt_id)**: WB объединяет товары (размеры, цвета) в одну карточку. Реклама одного nm_id показывает всю карточку

### ПРАВИЛА ЛОГИКИ ПРИ АНАЛИЗЕ:
- Снижение цены НЕ МОЖЕТ убивать продажи. Если цена снизилась и продажи упали — ищи другие причины
- При малом количестве данных (менее 30 заказов) — оговорка о статистической незначимости
- НЕ делай ложных причинно-следственных связей
- revenue WB (retail_price_withdisc_rub) — это цена со скидкой БЕЗ СПП. Реальная цена покупателя ниже
- payout (ppvz_for_pay) — сумма к перечислению продавцу после комиссии и логистики
- Комиссия WB = revenue - payout (включает SPP + комиссию площадки)

## ФОРМАТ ОТВЕТА — JSON

Верни ответ СТРОГО в JSON формате. Каждая секция — отдельный объект. Массив секций.
Не добавляй текст ВНЕ JSON. Не оборачивай в ```json```. Только чистый JSON-массив.

[
  {
    "id": "verdict",
    "title": "🎯 Вердикт",
    "content": "2-3 предложения. P&L одной цифрой. Эффективна/убыточна/потенциальна.",
    "type": "verdict",
    "status": "negative|neutral|positive"
  },
  {
    "id": "unit_economics",
    "title": "💰 Юнит-экономика",
    "content": "Если несколько товаров — ОТДЕЛЬНУЮ таблицу для КАЖДОГО!\nПараметр | Сумма (₽) | Доля от выручки (%)\n---|---|---\nВыручка (retail_price_withdisc_rub) | X | 100\nКомиссия WB (вкл. СПП) | -X | -X%\nЛогистика | -X | -X%\nХранение | -X | -X%\nPayout (к перечислению) | X | X%\nСебестоимость | -X | -X%\n**Прибыль до рекламы** | **X** | **X%**\nРасход на рекламу | -X | -X%\n**Чистая прибыль** | **X** | **X%**\nДопустимый CAC (безубыт.) | X | —\nРеальный CAC | X | —",
    "type": "section"
  },
  {
    "id": "sales_breakdown",
    "title": "📦 Структура продаж",
    "content": "Разбивка заказов: прямые / модель (та же карточка imt_id) / ассоциированные. Влияет ли реклама на продажи всей объединённой карточки?",
    "type": "section"
  },
  {
    "id": "keywords",
    "title": "🔑 Ключевые фразы",
    "content": "Топ фразы по кликам/показам. Какие добавить в минус-фразы? Какие закрепить? Только показы, клики, CTR — заказы по фразам неизвестны!",
    "type": "section"
  },
  {
    "id": "ad_effect",
    "title": "📈 Реклама",
    "content": "Средние CTR, CPC, CR, DRR. Тренды. Ключевые дни.",
    "type": "section"
  },
  {
    "id": "events",
    "title": "⚡ События",
    "content": "Для каждого ключевого: дата → изменение → результат. Кратко.",
    "type": "section"
  },
  {
    "id": "strategy",
    "title": "💡 Стратегия",
    "content": "КОНКРЕТНЫЕ цифры. Ставка CPM: X коп (Поиск) / Y коп (Каталог). Бюджет: Z₽. Цена товара: W₽. Минус-фразы: список. P&L прогноз.",
    "type": "strategy",
    "actions": [
      {"action": "Описание действия", "value": "конкретное значение", "priority": "high|medium|low"}
    ]
  }
]

ПРАВИЛА:
- Каждая секция = КРАТКИЙ текст (3-7 предложений, НЕ портянка)
- Называй товары по именам/артикулам
- Конкретные даты и числа из данных
- Рекомендуй КОНКРЕТНЫЕ минус-фразы на основе данных по фразам
- Ставки указывай в КОПЕЙКАХ (1₽ = 100 коп)
- Рекомендуй ТОЛЬКО: ставку CPM, бюджет, цену товара, минус-фразы, места размещения, вкл/выкл
- DRR считай и от рекл.выручки, и от ОБЩЕЙ выручки
- КРИТИЧЕСКИ ВАЖНО: рекламные заказы УЖЕ ВКЛЮЧЕНЫ в общие! НЕ складывай!
- Себестоимость: ТОЛЬКО из данных, НЕ придумывай
- Учитывай imt_id при анализе: модельные продажи — это НЕ кросс, это та же карточка
- Пиши на русском. Будь КРАТКИМ.
"""


def _to_date(v):
    """Normalize datetime→date."""
    return v.date() if isinstance(v, dt_datetime) else v


from pydantic import BaseModel as _PydanticBaseModel

class _AiAnalysisBody(_PydanticBaseModel):
    previous_analysis: Optional[str] = None


@router.post("/{marketplace}/{campaign_id}/ai-analysis")
async def analyze_campaign_ai(
    marketplace: str,
    campaign_id: int,
    start_date: date = Query(...),
    end_date: date = Query(...),
    sku: Optional[int] = Query(None),
    body: Optional[_AiAnalysisBody] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """
    AI-powered analysis of a specific campaign.
    Streams response from Gemini 2.5 Flash as SSE.
    """
    previous_analysis = body.previous_analysis if body else None
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
        prod_orders_by_sku: dict[int, int] = {}
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

            # Per-SKU order count from fact_ozon_orders (for P&L per product)
            prod_orders_by_sku: dict[int, int] = {}
            if len(filter_skus) > 1:
                sku_orders_rows = ch.query("""
                    SELECT sku, count() AS cnt
                    FROM mms_analytics.fact_ozon_orders FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND sku IN {skus:Array(UInt64)}
                      AND toDate(order_date) BETWEEN {start_date:Date} AND {end_date:Date}
                    GROUP BY sku
                """, parameters={
                    "shop_id": shop_id, "skus": filter_skus,
                    "start_date": start_date, "end_date": end_date
                }).result_rows
                for r in sku_orders_rows:
                    prod_orders_by_sku[int(r[0])] = int(r[1])
        else:
            prod_rows = ch.query("""
                SELECT toDate(date) AS d, sum(finished_price) AS rev, count() AS cnt
                FROM mms_analytics.fact_orders_raw FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND nm_id IN {skus:Array(UInt64)}
                  AND toDate(date) BETWEEN {start_date:Date} AND {end_date:Date}
                  AND is_cancel = 0
                GROUP BY d ORDER BY d
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
                "start_date": start_date, "end_date": end_date
            }).result_rows

        # ── 4. Financial transactions (payout, commission, logistics) ──
        finance_data = {}
        order_fin_rows = []
        if mp == "ozon":
            fin_rows = ch.query("""
                SELECT 
                    type,
                    operation_type_name,
                    round(sum(amount), 0) as total,
                    count() as cnt,
                    round(avg(accruals_for_sale), 0) as avg_sale_price,
                    round(avg(sale_commission), 0) as avg_commission,
                    round(avg(services_total), 0) as avg_logistics
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND sku IN {skus:Array(UInt64)}
                  AND toDate(operation_date) BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY type, operation_type_name
                ORDER BY total DESC
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
                "start_date": start_date, "end_date": end_date
            }).result_rows
            
            for r in fin_rows:
                typ, op_name = r[0], r[1]
                finance_data[f"{typ}:{op_name}"] = {
                    "total": int(r[2]), "count": int(r[3]),
                    "avg_sale_price": int(r[4]), "avg_commission": int(r[5]),
                    "avg_logistics": int(r[6])
                }

            # Get per-SKU detail for payout calculation
            order_fin_rows = ch.query("""
                SELECT 
                    sku,
                    round(avg(accruals_for_sale), 2) as avg_base_price,
                    round(avg(sale_commission), 2) as avg_commission,
                    round(avg(services_total), 2) as avg_logistics,
                    round(avg(amount), 2) as avg_payout,
                    count() as order_count,
                    round(avg(sale_commission / nullIf(accruals_for_sale, 0) * 100), 1) as commission_pct,
                    round(avg(services_total / nullIf(accruals_for_sale, 0) * 100), 1) as logistics_pct,
                    round(avg(amount / nullIf(accruals_for_sale, 0) * 100), 1) as payout_pct
                FROM mms_analytics.fact_ozon_transactions FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND sku IN {skus:Array(UInt64)}
                  AND type = 'orders'
                  AND toDate(operation_date) BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY sku
                ORDER BY count() DESC
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
                "start_date": start_date, "end_date": end_date
            }).result_rows

        # ── 5. Top keyword phrases ──
        phrases_lines = []
        mp_code = 2 if mp == "ozon" else 1
        try:
            phrases_rows = ch.query("""
                SELECT phrase, sum(views) as vw, sum(clicks) as cl,
                       round(if(sum(views)>0, sum(clicks)/sum(views)*100, 0), 2) as ctr
                FROM mms_analytics.fact_advert_phrases_daily FINAL
                WHERE marketplace = {mp_code:UInt8} AND campaign_id = {cid:UInt64}
                  AND dt BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY phrase
                ORDER BY cl DESC, vw DESC
                LIMIT 40
            """, parameters={
                "mp_code": mp_code, "cid": campaign_id,
                "start_date": start_date, "end_date": end_date
            }).result_rows
            
            for r in phrases_rows:
                phrases_lines.append(
                    f"«{r[0]}» — {r[1]} показов, {r[2]} кликов, CTR {r[3]}%"
                )
        except Exception as e:
            logger.warning("Failed to get phrases: %s", e)

        # ── 6. Repeat buyers (posting_number appears 2+ times = retention) ──
        repeat_info = ""
        if mp == "ozon":
            try:
                repeat_rows = ch.query("""
                    SELECT 
                        countDistinct(posting_number) as total_orders,
                        count() as tx_count
                    FROM mms_analytics.fact_ozon_transactions FINAL
                    WHERE shop_id = {shop_id:UInt32}
                      AND sku IN {skus:Array(UInt64)}
                      AND type = 'orders'
                      AND toDate(operation_date) BETWEEN {start_date:Date} AND {end_date:Date}
                """, parameters={
                    "shop_id": shop_id, "skus": filter_skus,
                    "start_date": start_date, "end_date": end_date,
                }).result_rows
                if repeat_rows:
                    total_orders = int(repeat_rows[0][0])
                    repeat_info = f"Уникальных заказов (posting): {total_orders}"
            except Exception as e:
                logger.warning("Failed to get repeat data: %s", e)

        ch.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("CH query failed for campaign AI analysis")
        raise HTTPException(status_code=500, detail=f"Ошибка запроса данных: {e}")

    # ── 7. Product info from PostgreSQL (price_index, cost_price, competitor) ──
    product_names: dict[int, str] = {}
    product_info_lines = []
    cost_price_info = ""
    cost_prices: dict[str, float] = {}  # offer_id -> total cost
    finance_summary = ""

    if mp == "ozon":
        pg_res = await db.execute(
            text("""
                SELECT product_id, sku, name, offer_id, 
                       price, old_price, min_price, marketing_price,
                       price_index_color, price_index_value, competitor_min_price,
                       stocks_fbo, stocks_fbs, vat
                FROM dim_ozon_products 
                WHERE shop_id = :sid AND sku = ANY(:skus)
            """),
            {"sid": shop_id, "skus": campaign_skus}
        )
        for r in pg_res.fetchall():
            pid, sku_val, name, offer = r[0], r[1], r[2] or "", r[3] or ""
            price, old_price, min_price, mkt_price = r[4], r[5], r[6], r[7]
            pi_color, pi_value, comp_min = r[8], r[9], r[10]
            stocks_fbo, stocks_fbs, vat = r[11], r[12], r[13]

            label = f"{name[:50]} ({offer})" if name and offer else name[:60] or offer or str(sku_val)
            product_names[sku_val] = label
            product_names[pid] = label

            # Build product info block
            pi_str = str(pi_color).upper() if pi_color else ""
            pi_val = float(pi_value) if pi_value else None
            # Map Ozon color constants correctly
            if "SUPER" in pi_str or "GREEN" in pi_str or (pi_val and pi_val <= 0.95):
                pi_display = "GREEN / SUPER (выгодный)"
            elif "RED" in pi_str or (pi_val and pi_val > 1.10):
                pi_display = "RED (невыгодный)"
            elif pi_str:
                pi_display = "YELLOW (умеренный)"
            else:
                pi_display = "Нет данных"
            
            info = f"""Товар: {label}
  SKU: {sku_val}, Product ID: {pid}
  «Ваша цена» (на руки продавцу): {price}₽
  «Цена до скидки» (маркетинговая/base price): {old_price}₽
  «Минимальная цена» (порог для автоакций): {min_price}₽
  «Цена для покупателя» (после скидок Ozon): {mkt_price}₽
  Price Index: {pi_display}, значение: {pi_value}
  Competitor min price: {comp_min}₽ (ПРОВЕРЬ: это может быть свой же товар на WB!)
  Остатки FBO: {stocks_fbo} шт, FBS: {stocks_fbs} шт
  НДС: {vat}"""
            product_info_lines.append(info)

        # ── Get cost_price from product_costs table (per-SKU as numbers) ──
        try:
            cp_res = await db.execute(
                text("""
                    SELECT p.sku, pc.offer_id, pc.cost_price, pc.packaging_cost
                    FROM product_costs pc
                    JOIN dim_ozon_products p ON p.shop_id = pc.shop_id AND p.offer_id = pc.offer_id
                    WHERE pc.shop_id = :sid AND p.sku = ANY(:skus)
                """),
                {"sid": shop_id, "skus": campaign_skus}
            )
            for cp_row in cp_res.fetchall():
                sku_val = int(cp_row[0])
                offer_id = cp_row[1]
                cost = float(cp_row[2] or 0)
                packaging = float(cp_row[3] or 0)
                total_cost = cost + packaging
                cost_prices[str(sku_val)] = total_cost
                cost_price_info += f"\n  Себестоимость ({offer_id}, SKU {sku_val}): {cost}₽"
                if packaging > 0:
                    cost_price_info += f" + упаковка {packaging}₽ = {total_cost:.0f}₽"
        except Exception as e:
            logger.warning("Could not get cost_price: %s", e)
            try:
                await db.rollback()
            except Exception:
                pass

        # ── Get retention/LTV data per SKU from ClickHouse ──
        retention_info = ""
        try:
            retention_rows = ch.query("""
                WITH
                    sku_clients AS (
                        SELECT
                            sku,
                            splitByChar('-', posting_number)[1] AS client_id,
                            order_number,
                            min(toDate(addHours(in_process_at, 3))) AS order_date,
                            sum(price * quantity) AS order_revenue,
                            sum(quantity) AS qty
                        FROM mms_analytics.fact_ozon_orders FINAL
                        WHERE shop_id = {shop_id:UInt32}
                          AND sku IN {skus:Array(UInt64)}
                        GROUP BY sku, client_id, order_number
                    ),
                    sku_client_agg AS (
                        SELECT
                            sku,
                            client_id,
                            count() AS purchases,
                            sum(order_revenue) AS client_revenue,
                            min(order_date) AS first_buy,
                            max(order_date) AS last_buy
                        FROM sku_clients
                        GROUP BY sku, client_id
                    )
                SELECT
                    sku,
                    count() AS total_buyers,
                    countIf(purchases >= 2) AS repeat_buyers,
                    round(countIf(purchases >= 2) / nullIf(count(), 0) * 100, 1) AS repeat_rate,
                    round(avg(if(purchases >= 2, dateDiff('day', first_buy, last_buy) / (purchases - 1), 0)), 0) AS avg_days_between,
                    round(avgIf(client_revenue, purchases >= 2), 0) AS avg_ltv_repeat,
                    round(avg(purchases), 2) AS avg_orders_per_buyer
                FROM sku_client_agg
                GROUP BY sku
            """, parameters={
                "shop_id": shop_id, "skus": filter_skus,
            }).result_rows

            if retention_rows:
                retention_info = "\n### ДАННЫЕ ПО РЕТЕНШЕНУ И ПОВТОРНЫМ ПОКУПКАМ (за всё время):\n"
                for rr in retention_rows:
                    r_sku = int(rr[0])
                    r_name = product_names.get(r_sku, str(r_sku))
                    total_buyers = int(rr[1])
                    repeat_buyers = int(rr[2])
                    repeat_rate = float(rr[3])
                    avg_days = int(rr[4]) if rr[4] else 0
                    avg_ltv_repeat = float(rr[5]) if rr[5] else 0
                    avg_orders = float(rr[6])
                    retention_info += f"""--- {r_name} (SKU {r_sku}) ---
- Всего покупателей: {total_buyers}
- Повторных покупателей: {repeat_buyers} ({repeat_rate}%)
- Среднее кол-во заказов на покупателя: {avg_orders}
- Среднее дней между повторными покупками: {avg_days} дней
- Средний LTV повторного покупателя: {avg_ltv_repeat:.0f}₽
"""
                retention_info += "ВАЖНО: учитывай ретеншен при оценке CAC! Если покупатель возвращается — CAC окупается за несколько покупок.\n"
        except Exception as e:
            logger.warning("Could not get retention data: %s", e)

    else:
        # WB — full product info, financials, cost_price, sale_type breakdown
        retention_info = ""
        pg_res = await db.execute(
            text("SELECT nm_id, name, vendor_code, imt_id FROM dim_products WHERE shop_id = :sid AND nm_id = ANY(:skus)"),
            {"sid": shop_id, "skus": campaign_skus}
        )
        wb_imt_ids: dict[int, int] = {}  # nm_id -> imt_id
        wb_vendor_codes: dict[int, str] = {}  # nm_id -> vendor_code
        for r in pg_res.fetchall():
            nmid, name, vc = r[0], r[1] or "", r[2] or ""
            imt_id = r[3]
            label = f"{name[:50]} ({vc})" if name and vc else name[:60] or str(nmid)
            product_names[nmid] = label
            if imt_id:
                wb_imt_ids[nmid] = int(imt_id)
            if vc:
                wb_vendor_codes[nmid] = vc
                product_info_lines.append(
                    f"Товар: {label}\n  nm_id: {nmid}, Артикул: {vc}"
                    + (f", imt_id (объединённая карточка): {imt_id}" if imt_id else "")
                )

        # ── WB: Sale type breakdown from fact_advert_stats_v3 (CAMPAIGN-ATTRIBUTED ONLY!) ──
        sale_type_info = ""
        wb_fin_rows = []
        try:
            # Get per-SKU ad stats from fact_advert_stats_v3
            ad_sku_rows = ch.query("""
                SELECT nm_id,
                       sum(orders) AS orders, sum(revenue) AS revenue,
                       sum(views) AS views, sum(clicks) AS clicks, sum(spend) AS spend
                FROM mms_analytics.fact_advert_stats_v3 FINAL
                WHERE advert_id = {cid:UInt64}
                  AND date BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY nm_id
            """, parameters={
                "cid": campaign_id,
                "start_date": start_date, "end_date": end_date
            }).result_rows

            # Classify: direct = has views/clicks/spend, model = same imt_id, associated = other
            direct_skus = set()
            for r in ad_sku_rows:
                nm = int(r[0])
                if int(r[3]) > 0 or int(r[4]) > 0 or float(r[5]) > 0:
                    direct_skus.add(nm)

            direct_imt_ids = {wb_imt_ids[s] for s in direct_skus if s in wb_imt_ids and wb_imt_ids[s]}

            direct_orders, direct_rev = 0, 0.0
            model_orders, model_rev = 0, 0.0
            assoc_orders, assoc_rev = 0, 0.0
            for r in ad_sku_rows:
                nm = int(r[0])
                o_cnt = int(r[1])
                o_rev = float(r[2] or 0)
                if nm in direct_skus:
                    direct_orders += o_cnt
                    direct_rev += o_rev
                else:
                    imt = wb_imt_ids.get(nm)
                    if imt and imt in direct_imt_ids:
                        model_orders += o_cnt
                        model_rev += o_rev
                    else:
                        assoc_orders += o_cnt
                        assoc_rev += o_rev

            total_camp_orders = direct_orders + model_orders + assoc_orders
            total_camp_rev = direct_rev + model_rev + assoc_rev

            sale_type_info = f"""\n### СТРУКТУРА ПРОДАЖ КАМПАНИИ (из рекламной статистики, fact_advert_stats_v3):
ПРЯМЫЕ (direct, рекламируемые SKU): {direct_orders} заказов, {direct_rev:.0f}₽
МОДЕЛЬ (model, та же карточка imt_id, другой размер/цвет): {model_orders} заказов, {model_rev:.0f}₽
АССОЦИИРОВАННЫЕ (associated, кросс-продажи, другие карточки): {assoc_orders} заказов, {assoc_rev:.0f}₽
ИТОГО по кампании: {total_camp_orders} заказов, {total_camp_rev:.0f}₽
ВАЖНО: это ТОЛЬКО рекламно-атрибутированные заказы (модель последнего касания WB), НЕ все продажи магазина!
\"модельные\" продажи — товары из той же объединённой карточки (тот же imt_id). Реклама одного размера/цвета продаёт всю карточку.
\"ассоциированные\" — покупатель увидел рекламу одного товара, но купил другой (кросс-продажа)."""
        except Exception as e:
            logger.warning("WB sale type breakdown failed: %s", e)

        # ── WB: Per-unit financial data from fact_finances (DIRECT products only) ──
        try:
            direct_skus_list = list(direct_skus) if direct_skus else filter_skus
            wb_fin_rows = ch.query("""
                SELECT
                    JSONExtractUInt(raw_payload, 'nm_id') AS nm_id,
                    sumIf(JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub'),
                        operation_type = 'Продажа') AS total_revenue,
                    sumIf(payout_amount, operation_type = 'Продажа') AS total_payout,
                    sumIf(abs(wb_delivery_rub), operation_type = 'Продажа') AS total_logistics,
                    sumIf(quantity, operation_type = 'Продажа' AND quantity > 0) AS total_sales
                FROM mms_analytics.fact_finances FINAL
                WHERE shop_id = {shop_id:UInt32}
                  AND marketplace = 1
                  AND JSONExtractUInt(raw_payload, 'nm_id') IN {skus:Array(UInt64)}
                  AND event_date BETWEEN {start_date:Date} AND {end_date:Date}
                GROUP BY nm_id
            """, parameters={
                "shop_id": shop_id, "skus": direct_skus_list,
                "start_date": start_date, "end_date": end_date
            }).result_rows

            if wb_fin_rows:
                finance_summary = "\n### ЮНИТ-ЭКОНОМИКА WB ПО ПРЯМЫМ ТОВАРАМ (средние значения на 1 продажу из отчётов реализации):\n"
                finance_summary += "ВАЖНО: payout уже ПОСЛЕ вычета комиссии WB и логистики! НЕ вычитай повторно!\n\n"
                for wfr in wb_fin_rows:
                    fn_nm = int(wfr[0])
                    fn_name = product_names.get(fn_nm, str(fn_nm))
                    fn_rev = float(wfr[1] or 0)
                    fn_pay = float(wfr[2] or 0)
                    fn_log = float(wfr[3] or 0)
                    fn_sales = int(wfr[4] or 0)

                    if fn_sales > 0:
                        rev_per_unit = round(fn_rev / fn_sales, 2)
                        pay_per_unit = round(fn_pay / fn_sales, 2)
                        log_per_unit = round(fn_log / fn_sales, 2)
                        commission_per_unit = round(rev_per_unit - pay_per_unit, 2)
                        commission_pct = round(commission_per_unit / rev_per_unit * 100, 1) if rev_per_unit > 0 else 0
                        vc = wb_vendor_codes.get(fn_nm, "")
                        unit_cost = cost_prices.get(vc, 0)

                        finance_summary += f"""--- {fn_name} (nm_id {fn_nm}) ---
Средние на 1 продажу (из {fn_sales} продаж за период):
- Выручка (розничная цена со скидкой): {rev_per_unit}₽
- Комиссия WB (вкл. СПП): -{commission_per_unit}₽ ({commission_pct}%)
- Логистика: -{log_per_unit}₽
- Payout (к перечислению): {pay_per_unit}₽
- Себестоимость: -{unit_cost}₽
- Прибыль на 1 шт до рекламы: {round(pay_per_unit - unit_cost, 2)}₽
- Допустимый CAC (безубыточный): {round(pay_per_unit - unit_cost, 2)}₽
"""
        except Exception as e:
            logger.warning("WB finance query failed: %s", e)

        # ── WB Cost price from product_costs ──
        try:
            vc_list = [vc for vc in wb_vendor_codes.values() if vc]
            if vc_list:
                cp_res = await db.execute(
                    text("""
                        SELECT offer_id, COALESCE(cost_price, 0) + COALESCE(packaging_cost, 0) AS total_cost,
                               cost_price, packaging_cost
                        FROM product_costs
                        WHERE shop_id = :sid AND offer_id = ANY(:vcs)
                    """),
                    {"sid": shop_id, "vcs": vc_list}
                )
                for cp_row in cp_res.fetchall():
                    offer_id = cp_row[0]
                    total_cost = float(cp_row[1] or 0)
                    cost = float(cp_row[2] or 0)
                    packaging = float(cp_row[3] or 0)
                    cost_prices[offer_id] = total_cost
                    cost_price_info += f"\n  Себестоимость ({offer_id}): {cost}₽"
                    if packaging > 0:
                        cost_price_info += f" + упаковка {packaging}₽ = {total_cost:.0f}₽"
        except Exception as e:
            logger.warning("WB cost_price query failed: %s", e)
            try:
                await db.rollback()
            except Exception:
                pass

        # NOTE: WB P&L summary is computed AFTER total_spend is known (see below)

    product_list = ", ".join(product_names[s] for s in campaign_skus if s in product_names)

    # ── 8. Events ──
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

    # ── 9. Build stats table ──
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

    stats_header = "Дата | Показы | Клики | CTR% | Рекл.заказы | Корзины | Рекл.выручка | Расход | CPC | CR% | Общ.заказы | Общ.выручка"
    stats_sep = "---|---|---|---|---|---|---|---|---|---|---|---"
    stats_lines = [stats_header, stats_sep]

    total_spend = 0
    total_ad_rev = 0
    total_prod_rev = 0
    total_ad_orders = 0
    total_prod_orders = 0
    total_clicks = 0

    for dt_str in all_dates:
        ad = ad_by_date.get(dt_str, {"views": 0, "clicks": 0, "ad_orders": 0, "cart": 0, "ad_revenue": 0, "spend": 0})
        pr = prod_by_date.get(dt_str, {"prod_revenue": 0, "prod_orders": 0})

        views, clicks = ad["views"], ad["clicks"]
        ctr = round(clicks / views * 100, 2) if views > 0 else 0
        cpc = round(ad["spend"] / clicks, 1) if clicks > 0 else 0
        cr = round(ad["ad_orders"] / clicks * 100, 2) if clicks > 0 else 0

        total_spend += ad["spend"]
        total_ad_rev += ad["ad_revenue"]
        total_prod_rev += pr["prod_revenue"]
        total_ad_orders += ad["ad_orders"]
        total_prod_orders += pr["prod_orders"]
        total_clicks += clicks

        stats_lines.append(
            f"{dt_str} | {views} | {clicks} | {ctr}% | {ad['ad_orders']} | {ad['cart']} | "
            f"{ad['ad_revenue']:.0f}₽ | {ad['spend']:.0f}₽ | {cpc}₽ | {cr}% | "
            f"{pr['prod_orders']} | {pr['prod_revenue']:.0f}₽"
        )

    # Totals
    total_drr = round(total_spend / total_ad_rev * 100, 1) if total_ad_rev > 0 else 0
    total_drr_overall = round(total_spend / total_prod_rev * 100, 1) if total_prod_rev > 0 else 0
    total_cac = round(total_spend / total_ad_orders, 0) if total_ad_orders > 0 else 0
    avg_cr = round(total_ad_orders / total_clicks * 100, 2) if total_clicks > 0 else 0
    organic_orders = total_prod_orders - total_ad_orders
    halo_pct = round(organic_orders / total_prod_orders * 100, 1) if total_prod_orders > 0 else 0

    # ── WB deferred P&L summary (needs total_spend) ──
    if mp != "ozon":
        try:
            wb_fin_local = locals().get('wb_fin_rows', [])
            wb_vc = locals().get('wb_vendor_codes', {})
            st_info = locals().get('sale_type_info', '')
            d_orders = locals().get('direct_orders', 0)
            
            if wb_fin_local and d_orders > 0:
                # Calculate weighted average per-unit profit across direct products
                total_profit_per_unit = 0.0
                sku_count = 0
                for wfr in wb_fin_local:
                    fn_nm = int(wfr[0])
                    fn_rev = float(wfr[1] or 0)
                    fn_pay = float(wfr[2] or 0)
                    fn_sales = int(wfr[4] or 0)
                    if fn_sales > 0:
                        pay_pu = fn_pay / fn_sales
                        vc_k = wb_vc.get(fn_nm, "")
                        uc = cost_prices.get(vc_k, 0)
                        total_profit_per_unit += (pay_pu - uc)
                        sku_count += 1

                if sku_count > 0:
                    avg_profit_per_unit = total_profit_per_unit / sku_count
                    est_profit_before_ads = avg_profit_per_unit * d_orders
                    est_profit_after_ads = est_profit_before_ads - total_spend

                    finance_summary += f"""
### ОЦЕНОЧНЫЙ P&L КАМПАНИИ (прямые рекламные заказы × средняя прибыль на 1 шт):
- Прямых рекламных заказов: {d_orders} шт
- Средняя прибыль на 1 шт (до рекламы): {avg_profit_per_unit:.0f}₽
- ПРИБЫЛЬ ДО РЕКЛАМЫ (оценка): {est_profit_before_ads:.0f}₽
- Расход на рекламу: -{total_spend:.0f}₽
- ЧИСТАЯ ПРИБЫЛЬ (оценка): {est_profit_after_ads:.0f}₽
{cost_price_info}
{st_info}
"""
        except Exception as e:
            logger.warning("WB P&L summary failed: %s", e)

    # ── 10. Build finance summary (per-SKU with cost) — Ozon only ──
    if mp == "ozon":
        finance_summary = ""
    if order_fin_rows and mp == "ozon":
        if len(order_fin_rows) == 1:
            # Single SKU
            ofr = order_fin_rows[0]
            sku_id = int(ofr[0])
            sku_name = product_names.get(sku_id, str(sku_id))
            cost_per_unit = cost_prices.get(str(sku_id), 0)
            avg_payout = float(ofr[4])
            # P&L считаем по total_prod_orders из fact_ozon_orders!
            total_cost = cost_per_unit * total_prod_orders
            total_payout_sum = avg_payout * total_prod_orders
            profit_per_unit = avg_payout - cost_per_unit if cost_per_unit > 0 else None
            profit_before_ads = total_payout_sum - total_cost if cost_per_unit > 0 else None
            profit_after_ads = (total_payout_sum - total_cost - total_spend) if cost_per_unit > 0 else None
            finance_summary = f"""
### ФИНАНСОВЫЕ ДАННЫЕ ({sku_name}):
- Средняя «Цена до скидки» (accruals_for_sale): {ofr[1]}₽
- Средняя комиссия Ozon: {ofr[2]}₽ ({ofr[6]}% от base_price)
- Средняя логистика+обработка: {ofr[3]}₽ ({ofr[7]}% от base_price)
- Средний payout (на руки после комиссий и логистики): {ofr[4]}₽ ({ofr[8]}% от base_price)
- Себестоимость: {cost_per_unit}₽/шт

### ПРЕДРАССЧИТАННЫЙ P&L (не пересчитывай, используй эти числа!):
- Заказов за период: {total_prod_orders} шт
- Общий payout: {total_payout_sum:.0f}₽ ({total_prod_orders} × {avg_payout:.0f}₽)
- Общая себестоимость: -{total_cost:.0f}₽ ({total_prod_orders} × {cost_per_unit}₽)
- ПРИБЫЛЬ ДО РЕКЛАМЫ: {f'{profit_before_ads:.0f}₽' if profit_before_ads is not None else 'нет данных'}
- Расход на рекламу: -{total_spend:.0f}₽
- ЧИСТАЯ ПРИБЫЛЬ: {f'{profit_after_ads:.0f}₽' if profit_after_ads is not None else 'нет данных'}
- Маржинальность: {f'{round(profit_after_ads / total_payout_sum * 100, 1)}%' if profit_after_ads is not None and total_payout_sum > 0 else 'нет данных'}
ВАЖНО: payout — это уже ПОСЛЕ комиссии и логистики! НЕ вычитай их повторно!
{cost_price_info}
"""
        else:
            # Multiple SKUs — per-SKU breakdown
            finance_summary = "\n### ФИНАНСОВЫЕ ДАННЫЕ ПО КАЖДОМУ ТОВАРУ:\n"
            finance_summary += "ВАЖНО: юнит-экономику считай ОТДЕЛЬНО для КАЖДОГО товара!\n\n"
            combined_payout = 0
            combined_cost = 0
            for ofr in order_fin_rows:
                sku_id = int(ofr[0])
                sku_name = product_names.get(sku_id, str(sku_id))
                cost_per_unit = cost_prices.get(str(sku_id), 0)
                avg_payout = float(ofr[4])
                # Берём заказы из fact_ozon_orders, не из transactions
                sku_orders = prod_orders_by_sku.get(sku_id, int(ofr[5]))
                total_cost = cost_per_unit * sku_orders
                sku_total_payout = avg_payout * sku_orders
                profit_per_unit = avg_payout - cost_per_unit if cost_per_unit > 0 else None
                combined_payout += sku_total_payout
                combined_cost += total_cost
                finance_summary += f"""--- Товар: {sku_name} (SKU {sku_id}) ---
- «Цена до скидки»: {ofr[1]}₽
- Комиссия: {ofr[2]}₽ ({ofr[6]}%)
- Логистика: {ofr[3]}₽ ({ofr[7]}%)
- Payout (на руки, после комиссий!): {ofr[4]}₽
- Заказов: {sku_orders} шт
- Себестоимость: {cost_per_unit}₽/шт
- Пр.до рекл./шт: {f'{profit_per_unit:.0f}₽' if profit_per_unit is not None else 'нет'}
"""
            combined_profit_before_ads = combined_payout - combined_cost
            combined_profit_after_ads = combined_profit_before_ads - total_spend
            finance_summary += f"""
### ПРЕДРАССЧИТАННЫЙ P&L ПО ВСЕЙ КАМПАНИИ:
- Заказов: {total_prod_orders} шт
- Общий payout: {combined_payout:.0f}₽
- Общая себестоимость: -{combined_cost:.0f}₽
- ПРИБЫЛЬ ДО РЕКЛАМЫ: {combined_profit_before_ads:.0f}₽
- Расход на рекламу: -{total_spend:.0f}₽
- ЧИСТАЯ ПРИБЫЛЬ: {combined_profit_after_ads:.0f}₽
ВАЖНО: payout — это уже ПОСЛЕ комиссии и логистики! НЕ вычитай их повторно!
"""
            finance_summary += cost_price_info + "\n"

        # Add other charges
        for key, val in finance_data.items():
            if key.startswith("orders:"):
                continue
            finance_summary += f"- {key}: {val['total']}₽ ({val['count']} шт)\n"

    # ── 11. Build user prompt ──
    user_message = f"""Кампания: ID {campaign_id}
Маркетплейс: {mp_label}
Период: {start_date} — {end_date} ({len(all_dates)} дней с данными)
Товары в кампании: {product_list}

### ИНФОРМАЦИЯ О ТОВАРАХ:
{chr(10).join(product_info_lines) if product_info_lines else "Нет данных"}

ИТОГО за период:
- Расход на рекламу: {total_spend:.0f}₽
- Рекл. выручка (только рекл.заказы): {total_ad_rev:.0f}₽
- ОБЩАЯ выручка (ВСЕ заказы, включая рекламные!): {total_prod_rev:.0f}₽
- ВСЕГО заказов (включая рекламные!): {total_prod_orders}
- Из них рекламных: {total_ad_orders} (ВНИМАНИЕ: это часть общих, НЕ складывать! {total_prod_orders} уже включает {total_ad_orders} рекламных)
- Органических (Halo + повторы): {organic_orders} ({halo_pct}% от общих)
- DRR от рекл.выручки: {total_drr}%
- DRR от ОБЩЕЙ выручки: {total_drr_overall}% (учитывает Halo-эффект)
- Средний CR (заказ/клик): {avg_cr}%
- CAC (расход/рекламный заказ): {total_cac}₽
- CAC эффективный (расход/ВСЕ заказы): {round(total_spend/total_prod_orders) if total_prod_orders > 0 else 0}₽
{repeat_info}
{finance_summary}
{retention_info}

### СТАТИСТИКА ПО ДНЯМ:
{chr(10).join(stats_lines)}

### TOP КЛЮЧЕВЫЕ ФРАЗЫ ({len(phrases_lines)} шт.):
{chr(10).join(phrases_lines) if phrases_lines else "Данные по фразам не найдены"}

### СОБЫТИЯ ({len(events_raw)} шт.):
{chr(10).join(events_lines) if events_lines else "Событий не найдено"}

Проанализируй ВСЕ данные. Рассчитай полную юнит-экономику. Найди оптимальную стратегию масштабирования с МАКСИМАЛЬНОЙ прибылью! Не зацикливайся на 20% — ищи реальный оптимум цена/ставка/бюджет. Рекомендации по бюджету — в НЕДЕЛЬНЫХ суммах."""

    # ── 12. Stream from Gemini ──
    system_prompt = SYSTEM_PROMPT_WB if mp == "wb" else SYSTEM_PROMPT
    messages = [
        {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
    ]
    if previous_analysis:
        messages.append({"role": "user", "content": [{"type": "text", "text": f"Предыдущий анализ этой кампании (для сравнения):\n{previous_analysis[:3000]}"}]})
        messages.append({"role": "assistant", "content": [{"type": "text", "text": "Понял, учту предыдущий анализ для сравнения."}]})
    messages.append({"role": "user", "content": [{"type": "text", "text": user_message}]})

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
                        "messages": messages,
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
