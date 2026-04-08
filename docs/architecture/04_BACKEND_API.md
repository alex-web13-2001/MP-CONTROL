# MP-CONTROL — Backend API

> REST API на FastAPI. Все endpoints начинаются с `/api/v1/`.  
> Файлы: `backend/app/api/v1/` (16 роутеров), `backend/app/schemas/auth.py`

---

## Роутинг

```python
# backend/app/api/v1/router.py
router.include_router(auth_router)              # /api/v1/auth/*
router.include_router(shops_router)             # /api/v1/shops/*
router.include_router(finance_reports_router)    # /api/v1/finance-reports/*
router.include_router(advertising_router)       # /api/v1/advertising/*
router.include_router(commercial_router)        # /api/v1/commercial/*
router.include_router(dashboard_router)         # /api/v1/dashboard/*
router.include_router(products_router)          # /api/v1/products/*
router.include_router(wb_products_router)       # /api/v1/products/wb/*
router.include_router(finances_router)          # /api/v1/finances/*
router.include_router(sales_router)             # /api/v1/sales/*
router.include_router(ltv_router)               # /api/v1/sales/ozon/ltv*
router.include_router(wb_ltv_router)            # /api/v1/sales/wb/ltv*
router.include_router(events_router)            # /api/v1/events/*
router.include_router(warehouses_router)         # /api/v1/warehouses/*
router.include_router(campaign_ai_router)        # /api/v1/campaign-ai/*
router.include_router(campaign_details_router)    # /api/v1/campaign-details/*
```

---

## Аутентификация — `/api/v1/auth`

### Endpoints

| Метод  | Path             | Описание                                 | Auth   |
| ------ | ---------------- | ---------------------------------------- | ------ |
| `POST` | `/auth/register` | Регистрация нового пользователя          | —      |
| `POST` | `/auth/login`    | Авторизация, возврат JWT                 | —      |
| `POST` | `/auth/refresh`  | Обновление access token                  | —      |
| `GET`  | `/auth/me`       | Профиль текущего пользователя + магазины | Bearer |

### Schemas

```
RegisterRequest { email: EmailStr, password: str[6-128], name: str[1-255] }
LoginRequest    { email: EmailStr, password: str }
RefreshRequest  { refresh_token: str }

TokenResponse {
    access_token: str
    refresh_token: str
    token_type: "bearer"
    user: UserResponse
}

UserResponse {
    id: str (UUID)
    email: str
    name: str
    is_active: bool
    shops: ShopResponse[]
}

ShopResponse {
    id: int, name: str, marketplace: str,
    is_active: bool, status: str
}
```

### Логика

- `register`: проверка уникальности email → bcrypt hash → создание User → JWT пара
- `login`: поиск по email → bcrypt verify → JWT пара (access 120 мин, refresh 7 дней)
- `refresh`: decode refresh token → проверка type="refresh" → новая JWT пара
- `me`: `Depends(get_current_user)` → UserResponse с shops

---

## Управление магазинами — `/api/v1/shops`

### Endpoints

| Метод    | Path                           | Описание                                 | Auth   |
| -------- | ------------------------------ | ---------------------------------------- | ------ |
| `GET`    | `/shops`                       | Список магазинов текущего пользователя   | Bearer |
| `POST`   | `/shops`                       | Добавить новый магазин (ключи шифруются) | Bearer |
| `POST`   | `/shops/validate-key`          | Валидация API ключа маркетплейса         | Bearer |
| `GET`    | `/shops/{shop_id}/sync-status` | Статус первичной синхронизации           | Bearer |
| `PATCH`  | `/shops/{shop_id}/keys`        | Обновить API ключи                       | Bearer |
| `DELETE` | `/shops/{shop_id}`             | Удалить магазин и все данные             | Bearer |

### Schemas

```
ShopCreate {
    name: str, marketplace: "wildberries"|"ozon",
    api_key: str,
    client_id?: str,           // Ozon Seller Client-Id
    perf_client_id?: str,      // Ozon Performance Client-Id
    perf_client_secret?: str   // Ozon Performance Client-Secret
}

ValidateKeyRequest {
    marketplace: "wildberries"|"ozon",
    api_key: str,
    client_id?: str,
    perf_client_id?: str,
    perf_client_secret?: str
}

ValidateKeyResponse {
    valid: bool,
    seller_valid?: bool,     // Ozon seller check
    perf_valid?: bool,       // Ozon performance check
    message: str,
    shop_name?: str,         // Auto-detected
    warnings?: str[]         // Missing WB permissions
}
```

### Ключевая логика

- **create_shop**: Fernet-шифрование api_key → `api_key_encrypted`, perf_secret → `perf_client_secret_encrypted`. После создания → `load_historical_data.delay(shop_id)`.
- **validate_key (WB)**: проверяет `/ping` на 7 доменах API → warnings для отсутствующих прав.
- **validate_key (Ozon)**: проверяет Seller API + Performance API OAuth2 раздельно.
- **get_sync_status**: читает Redis `sync_status:{shop_id}` → progress (step, total, eta). Fallback: PostgreSQL `shop.status`.
- **delete_shop**: удаляет данные из ClickHouse (6 таблиц) + PostgreSQL (каскад) + Redis state.

---

## Коммерческий мониторинг — `/api/v1/commercial`

### Endpoints

| Метод  | Path                           | Описание                         | Auth |
| ------ | ------------------------------ | -------------------------------- | ---- |
| `POST` | `/commercial/sync`             | Старт синхронизации цен+остатков | —    |
| `POST` | `/commercial/sync-warehouses`  | Синхронизация складов            | —    |
| `POST` | `/commercial/sync-content`     | Синхронизация контента карточек  | —    |
| `GET`  | `/commercial/status/{task_id}` | Статус задачи Celery             | —    |
| `POST` | `/commercial/turnover`         | Расчёт оборачиваемости on demand | —    |

### Turnover API

```
POST /commercial/turnover
{
    shop_id: 1,
    nm_ids: [123, 456],  // optional
    days: 30
}

→ {
    products: [
        { nm_id, current_quantity, avg_daily_sales, turnover_days }
    ],
    total_products: int
}
```

**Формула:** `turnover_days = current_quantity / avg_daily_sales`  
**Источники:** ClickHouse `fact_inventory_snapshot` (остатки) + `fact_finances` (продажи)

---

## Дашборд Ozon — `/api/v1/dashboard`

### Endpoints

| Метод | Path              | Описание                          | Auth   |
| ----- | ----------------- | --------------------------------- | ------ |
| `GET` | `/dashboard/ozon` | Агрегированные KPI + графики Ozon | Bearer |
| `GET` | `/dashboard/wb`   | Агрегированные KPI + графики WB   | Bearer |

### Query Parameters

```
shop_id: int (required)  — ID магазина
period: "today" | "7d" | "30d"  — период (default: "7d")
```

### Response Schema (WB — `/dashboard/wb`)

```
{
  shop_id: int,
  period: str,
  kpi: {
    sales: { orders, orders_delta, revenue, revenue_delta, avg_check, cancels, cancel_rate, cancel_delta },
    funnel: { views, views_delta, clicks, clicks_delta, ctr, ctr_delta,
              carts, carts_delta, cart_conversion, cart_conversion_delta,
              ad_orders, click_to_cart },
    ads: { ad_spend, ad_spend_delta, drr, drr_delta, drr_ad, drr_ad_delta },
    profit: { profit, profit_delta, profit_pct }
  },
  charts: {
    sales_daily: [{ date, orders, revenue }],
    ads_daily: [{ date, spend, views, clicks, cart, orders, drr_ad, drr_total }]
  },
  top_products: [{ ... }],
  alerts: { ... },
  orders_feed: [{
    nm_id, supplier_article, orders, revenue,
    orders_prev, revenue_prev,        // ← предыдущий период для дельт
    last_order, image_url
  }],
  finance_summary: {
    week_start, week_end,             // ISO dates (Mon→Mon)
    revenue, revenue_prev, revenue_delta,
    commission, commission_prev,
    logistics, logistics_prev,
    storage, storage_prev,
    ad_spend, ad_spend_prev,
    deductions, deductions_prev,
    acceptance, acceptance_prev,
    penalties, penalties_prev,
    returns, returns_prev,
    orders, orders_prev,
    profit, profit_prev, profit_pct, profit_delta
  }
}
```

### Ключевая логика

- 10+ SQL-запросов к ClickHouse: KPI (заказы/реклама), графики, ТОП товаров, алерты, заказы за период, финансы за неделю
- **Все заказы:** фильтры на cancelled/is_cancel убраны — учитываются все статусы (совпадение с ЛК)
- **Timezone:** группировка по дате в МСК (UTC+3): `toDate(addHours(in_process_at, 3))` (Ozon), `toDate(addHours(date, 3))` (WB)
- **DRR** = `ad_spend / orders_revenue × 100` (НЕ ad_revenue)
- Delta = процент изменения к предыдущему аналогичному периоду
- Обогащение товаров именами/изображениями из PostgreSQL
- **Ozon images:** `COALESCE(NULLIF(primary_image_url, ''), main_image_url, '')` — приоритет primary_image
- **WB images:** динамическая генерация CDN URL через `wb_image_url(nm_id)`
- Проверка ownership магазина через `get_current_user`

#### Finance Summary (WB)

- **Период:** Понедельник → Понедельник (Mon-Mon, 8 дней включительно)
- `toMonday(max(event_date))` → если текущая неделя не завершена → предыдущая полная
- **Источник:** `fact_finances FINAL` (WB) — единственный source of truth
- **Revenue:** `retail_price_withdisc_rub` по `operation_type = 'Продажа'` минус возвраты
- **Ad spend:** MAX-reconciliation: `max(promo_deductions, fact_advert_stats_v3.spend)` — берётся бо́льшее из финансового отчёта и рекламной статистики
- **Profit:** `payout − all_expenses` (commission, logistics, storage, deductions, acceptance, penalties, ad_extra)
- Каждая строка P&L содержит текущее значение + значение за предыдущую неделю для расчёта процентных дельт

#### Orders Feed (WB)

- **Источник:** `fact_orders_raw FINAL` — заказы за текущий и предыдущий период
- **Два периода в одном запросе:** `countIf` / `sumIf` с CASE по датам — `orders_cur`, `orders_prev`, `revenue_cur`, `revenue_prev`
- Сортировка по `orders_cur DESC`, лимит 50
- Обогащение `name`, `vendor_code`, `image_url` из PostgreSQL `dim_products`

---

## Товары — `/api/v1/products`

### Endpoints

| Метод   | Path                           | Описание                                   | Auth   |
| ------- | ------------------------------ | ------------------------------------------ | ------ |
| `GET`   | `/products/ozon`               | Агрегированные данные по всем товарам Ozon | Bearer |
| `PATCH` | `/products/ozon/cost`          | Обновить себестоимость товара (single)     | Bearer |
| `POST`  | `/products/ozon/cost/bulk`     | Загрузить себестоимость из Excel (.xlsx)   | Bearer |
| `GET`   | `/products/ozon/cost/template` | Скачать Excel-шаблон для заполнения        | Bearer |

### GET `/products/ozon` — Query Parameters

```
shop_id: int (required)
page: int (default: 1)
per_page: int (default: 50)
sort: "revenue_7d" | "orders_7d" | "stocks" | "price" | "gross_profit" | "drr" | "returns" | "name" | "content_rating"
order: "desc" | "asc" (default: "desc")
filter: "all" | "profitable" | "unprofitable" | "zero_cost" | "no_sales"
search: string
period: "7d" | "14d" | "30d" (default: "7d")
```

### Ключевая логика

- 9 источников данных: PG каталог + product_costs → CH транзакции/реклама/возвраты/комиссии/контент-рейтинг/промоакции → PG events
- **Стабильная сортировка**: composite key `(primary_value, offer_id)` — гарантирует детерминированную пагинацию
- **Формула прибыли Ozon** (revenue = accruals_for_sale из transactions):
  - `revenue` = `sum(accruals_for_sale)` из `fact_ozon_transactions` — реальные начисления продавцу (НЕ цена покупателя)
  - `mp_fees` = `sale_commission + services_total` из `fact_ozon_transactions` — чёткая детализация удержаний
  - Bulk charges (Acquiring, Storage, etc.) — распределяются пропорционально revenue по товарам
  - `gross_profit` = `revenue − COGS − mp_fees − bulk_charges − ad_spend`
  - `gross_profit_percent` = `gross_profit / revenue × 100` — % от реальной выручки
  - ⚠ Ранее: `revenue = price × quantity` (цена покупателя) — завышала выручку ~2.5x из-за скидок Ozon
  - ⚠ Ранее: `mp_fees = revenue − txn_payout` — неинформативно, смешивало комиссию и логистику
- **`margin_percent`** = `cost / price × 100` — доля себестоимости в цене из ЛК (всегда положительный)
- **marketing_price**: реальная «Ваша цена» из `/v5/product/info/prices` (с учётом скидок Ozon)
- **Серверные `totals`**: итоги по ВСЕМ товарам до пагинации, включают `payout`, `avg_price`, `profit`, `mp_fees` и их детализацию

### PATCH `/products/ozon/cost` — защита

- `offer_id.strip()` — предотвращение дублей с пробелами
- Warning если `cost_price > price` продажи

### POST `/products/ozon/cost/bulk` — Excel upload

- Формат: колонка A = артикул, колонка B = себестоимость
- `offer_id.strip()` при импорте
- Возвращает `warnings[]` если с/с > цена продажи

---

## Товары WB — `/api/v1/products/wb`

### Endpoints

| Метод   | Path                         | Описание                                       | Auth   |
| ------- | ---------------------------- | ---------------------------------------------- | ------ |
| `GET`   | `/products/wb`               | Агрегированные данные по всем товарам WB       | Bearer |
| `PATCH` | `/products/wb/cost`          | Обновить себестоимость товара (by vendor_code) | Bearer |
| `POST`  | `/products/wb/cost/bulk`     | Загрузить себестоимость из Excel (.xlsx)       | Bearer |
| `GET`   | `/products/wb/cost/template` | Скачать Excel-шаблон для заполнения            | Bearer |

### GET `/products/wb` — Query Parameters

```
shop_id: int (required)
page: int (default: 1)
per_page: int (default: 25, 10-100)
sort: "revenue_7d" | "orders_7d" | "stocks" | "price" | "gross_profit" | "drr" | "name"
order: "desc" | "asc" (default: "desc")
filter: "all" | "with_ads" | "no_ads" | "leaders" | "falling" | "problems"
search: string
period: "7d" | "14d" | "30d" (default: "7d")
date_from / date_to: date (optional) — кастомный диапазон
```

### Ключевая логика

- 8 источников: PG каталог + product_costs → CH финансы (primary) / заказы (fallback) / остатки / реклама
- **Формула прибыли WB** (единый источник `fact_finances FINAL`):
  - `revenue` = `sum(retail_price_withdisc_rub)` — розничная цена (source of truth)
  - `payout` = `sum(ppvz_for_pay)` — к выплате
  - `mp_fees` = `revenue − payout` — все удержания
  - `profit` = `payout − COGS − ads` — чистая прибыль
  - Удержания: только product-specific (с vendor_code). Общие (отзывы за баллы, авансы) — исключены
  - Хранение: распределяется пропорционально revenue (нет product ID)
  - `fact_orders_raw` → только fallback для товаров без реализации (`fees_source='estimated'`)
- WB CDN: `wb_image_url(nm_id)` — динамическая генерация URL фото
- Серверные `totals`: итоги по всем товарам до пагинации (product-only экономика, без general deductions)
- Фильтры: `with_ads` (ad_spend > 0), `leaders` (top 20% revenue), `falling` (delta < -20%), `problems` (stock = 0)

---

## Финансовые отчёты — `/api/v1/finance-reports`

| Метод  | Path                                | Описание                         | Auth |
| ------ | ----------------------------------- | -------------------------------- | ---- |
| `POST` | `/finance-reports/sync`             | Старт синхронизации за N месяцев | —    |
| `GET`  | `/finance-reports/status/{task_id}` | Статус задачи                    | —    |
| `GET`  | `/finance-reports/list`             | Список загруженных файлов        | —    |

---

## Реклама — `/api/v1/advertising`

| Метод  | Path                            | Описание                      | Auth |
| ------ | ------------------------------- | ----------------------------- | ---- |
| `POST` | `/advertising/sync`             | Старт рекламной синхронизации | —    |
| `GET`  | `/advertising/status/{task_id}` | Статус задачи                 | —    |

### Рекламная аналитика — `/api/v1/advertising/analytics`

> **Файл:** `backend/app/api/v1/advertising_analytics.py` (~2230 строк)  
> **Универсальный:** Ozon + WB в одном endpoint, выбор по `marketplace` магазина

| Метод | Path                         | Описание                                          | Auth   |
| ----- | ---------------------------- | ------------------------------------------------- | ------ |
| `GET` | `/advertising/analytics`     | Полная аналитика: KPI, графики, кампании, ТОП SKU | Bearer |

#### Query Parameters

```
shop_id: int (required)
period: "today" | "7d" | "14d" | "30d" | "custom" (default: "7d")
date_from: date (optional) — начало кастомного диапазона
date_to: date (optional) — конец кастомного диапазона
```

#### Response Schema

```
{
  kpi: {
    spend, spend_delta,
    views, views_delta,
    clicks, clicks_delta,
    ctr, ctr_delta,
    carts, carts_delta,
    orders, orders_delta,
    revenue, revenue_delta,
    drr, drr_delta,
    avg_cpc, avg_cpc_delta
  },
  charts: {
    daily: [{ date, spend, views, clicks, carts, orders, revenue, ctr, drr, total_drr }]
  },
  campaigns_table: [{
    campaign_id, title, status, status_code, campaign_type,
    payment_type, bid_type, placements,
    sku_count,
    items: [{ sku, product_id, offer_id, name, image_url,
              spend, views, clicks, cart, cart_conv, orders, order_conv,
              direct_orders, model_orders, revenue, direct_revenue, model_revenue,
              halo_pct, ctr, avg_cpc, drr, total_revenue, total_drr, bid }],
    associated_items: [...],  // WB: кросс-продажи (is_associated=1)
    spend, views, clicks, cart, cart_conv, orders, order_conv,
    direct_orders, model_orders, revenue, direct_revenue, model_revenue,
    halo_pct, ctr, avg_cpc, drr, total_revenue, total_drr
  }],
  top_skus: [{
    nm_id, vendor_code, name, image_url,
    spend, orders, revenue, drr
  }]
}
```

#### Ключевая логика

**Ozon** (`_build_ozon_analytics`):
- Stats из `fact_ozon_ad_daily FINAL`, orders из `fact_ozon_orders`
- Campaigns enrichment из PostgreSQL `dim_ozon_campaigns` + `dim_ozon_campaign_products`
- **Zero-stat campaigns:** добавляются из `dim_ozon_campaigns` с нулевой статистикой (кроме `CAMPAIGN_STATE_ARCHIVED`)
- SKU enrichment из `dim_ozon_products`, images из `primary_image_url`

**WB** (`_build_wb_analytics`):
- Stats из `fact_advert_stats_v3 FINAL`, orders из `fact_orders_raw`
- **Advertised vs Associated SKU:** `ads_raw_history.is_associated` → раздельные списки `items` и `associated_items`
- Campaigns enrichment из ClickHouse `dim_advert_campaigns FINAL` (name, type Enum8, status, payment_type, bid_type, placements)
- **Zero-stat campaigns:** добавляются из `dim_advert_campaigns` с нулевой статистикой (кроме status=-1 «Удалена»)
- **Ставки WB:** из `log_wb_bids` (последняя ставка per nm_id per advert_id), конвертация из копеек в рубли
- SKU enrichment из `dim_products`, images через `_wb_image_url(nm_id)` (CDN basket calculation)

**WB Type Enum Map:**

| Enum value               | Отображение         |
| ------------------------ | ------------------- |
| `search`                 | Поиск               |
| `carousel`               | Каталог             |
| `card`                   | Карточка            |
| `recommend`              | Рекомендации        |
| `auto`                   | Авто                |
| `search_plus_catalog`    | Поиск + Каталог     |
| `recommend_plus_carousel`| Единая              |

**WB Status Map:** `-1`=Удалена, `4`=Готова, `7`=Завершена, `8`=Отменена, `9`=Активна, `11`=На паузе

---

## Детали кампаний — `/api/v1/campaign-details`

> **Файл:** `backend/app/api/v1/campaign_details.py` (~850 строк)  
> **Универсальный:** работает и для Ozon (`marketplace=ozon`), и для WB (`marketplace=wb`)

### Endpoints

| Метод | Path                                            | Описание                                          | Auth |
| ----- | ----------------------------------------------- | ------------------------------------------------- | ---- |
| `GET` | `/campaign-details/{mp}/{campaign_id}/kpi`      | KPI agрегаты (текущий + предыдущий период, дельты) | JWT  |
| `GET` | `/campaign-details/{mp}/{campaign_id}/stats`    | Time-series (views, clicks, orders, spend, DRR)    | JWT  |
| `GET` | `/campaign-details/{mp}/{campaign_id}/events`   | История событий (ставки, статус, контент)           | JWT  |
| `GET` | `/campaign-details/{mp}/{campaign_id}/phrases`  | Поисковые фразы + normquery fallback для WB        | JWT  |
| `GET` | `/campaign-details/{mp}/{campaign_id}/heatmap`  | Heatmap заказов (час × день недели)                 | JWT  |
| `GET` | `/campaign-details/{mp}/{campaign_id}/purchases`| SKU, купленные через кампанию                       | JWT  |

### Query Parameters (общие)

| Параметр     | Тип    | Описание                                |
| ------------ | ------ | --------------------------------------- |
| `start_date` | date   | Начало периода                          |
| `end_date`   | date   | Конец периода                           |
| `sku`        | int?   | Фильтр по конкретному SKU внутри кампании |

### Ключевая логика

- **KPI:** ad stats + product revenue (из `fact_ozon_orders` / `fact_orders_raw`), вычисление дельт с предыдущим периодом той же длины
- **Stats:** merge рекламных данных (`fact_ozon_ad_daily` / `fact_advert_stats_v3`) с product revenue per day
- **Events:** конвертация Ozon SKU → product_id для поиска в `event_log` (sku и product_id — разные!), enrichment с названиями из `dim_ozon_products`; фильтрация `STATUS_CHANGE` событий
- **Phrases:** двухуровневый источник данных:
  1. Основной: `fact_advert_phrases_daily` (marketplace Enum8: 1=WB, 2=Ozon)
  2. **Fallback (WB):** если основная таблица пуста — `fact_normquery_stats_daily` (кластеры поисковых запросов). Обогащает ответ полями `atbs`, `avg_pos`, `cpc` для CPM-кампаний
- **Heatmap:** группировка заказов по `toDayOfWeek()` × `toHour()` из таблиц заказов
- **Purchases:** фактические покупки по SKU кампании с enrichment названий из PostgreSQL

#### Phrases — формулы normquery (WB fallback)

| Метрика | Формула | Единицы |
| ------- | ------- | ------- |
| `spend` | `Σ(cpc × clicks) / 100` | Рубли (cpc хранится в копейках) |
| `avg_pos` | `Σ(avg_pos × views) / Σ(views)` | Позиция (взвешенное по показам) |
| `cpc` | `Σ(cpc × clicks) / Σ(clicks) / 100` | Рубли (взвешенный средний) |

> **Важно:** CPC для CPM-кампаний WB — производная метрика (0.09–1.34₽), т.к. оплата за показы, не за клики. Расхождение normquery vs основная статистика < 2% (мелкие кластеры < 100 показов отсекаются WB API).

### Response Schemas

| Schema              | Поля                                                                     |
| ------------------- | ------------------------------------------------------------------------ |
| `CampaignKpiResponse` | `current`/`previous` KpiPeriod + `first_date` (дата запуска)            |
| `CampaignStatsRow`    | dt, views, clicks, orders, cart, revenue, spend, ctr, drr, product_revenue |
| `CampaignEventRow`    | id, timestamp, event_type, product_id, product_name, old/new_value        |
| `CampaignPhraseRow`   | phrase, views, clicks, ctr, spend, orders, revenue, **atbs?**, **avg_pos?**, **cpc?** |
| `CampaignHeatmapRow`  | day_of_week, hour, orders                                                 |
| `CampaignPurchaseRow` | sku, product_name, offer_id, quantity, revenue, avg_price                  |

---

## ИИ-анализ кампании — `/api/v1/campaign-ai`

### Endpoints

| Метод  | Path                        | Описание                                      | Auth   |
| ------ | --------------------------- | --------------------------------------------- | ------ |
| `POST` | `/campaign-ai/analyze`      | SSE streaming ИИ-анализ кампании (Gemini 2.5) | Bearer |

### Request Body

```
{
  shop_id: int,
  campaign_id: int,
  days: int (default: 30),            // период анализа
  date_from?: str (YYYY-MM-DD),       // кастомная начальная дата
  date_to?: str (YYYY-MM-DD),         // кастомная конечная дата
  previous_analysis?: str             // предыдущий анализ для сравнения
}
```

### Response (SSE stream)

```
Content-Type: text/event-stream

data: {"type": "sections", "data": [...]}      // структура секций для UI
data: {"type": "chunk", "data": "текст..."}     // streaming текст от Gemini
data: {"type": "done"}                           // конец стрима
data: {"type": "error", "data": "..."}           // ошибка
```

### Структура секций (JSON)

```
[
  { id: "unit_economics", title: "📊 Юнит-экономика", type: "section" },
  { id: "price_conversion", title: "🎯 Конверсия vs Цена", type: "section" },
  { id: "price_index", title: "🔍 Price Index", type: "section" },
  { id: "keywords", title: "🔑 Ключевые фразы", type: "section" },
  { id: "ad_effect", title: "📈 Реклама", type: "section" },
  { id: "halo_retention", title: "🔄 Halo vs Retention", type: "section" },
  { id: "events", title: "⚡ События", type: "section" },
  {
    id: "strategy", title: "💡 Стратегия", type: "strategy",
    actions: [
      { action: "...", value: "...", priority: "high|medium|low" }
    ]
  }
]
```

### Источники данных

**Ozon (11 запросов):**

| # | Источник | Что запрашивается |
|---|----------|-------------------|
| 1 | ClickHouse `fact_ozon_ad_daily` | Ежедневная статистика кампании: views, clicks, spend, orders, revenue, carts |
| 2 | ClickHouse `fact_ozon_orders` | Ежедневные заказы и выручка по SKU кампании (total orders) |
| 3 | ClickHouse `fact_ozon_orders` | Per-SKU order counts для multi-product P&L |
| 4 | ClickHouse `fact_ozon_ad_daily` | Топ ключевые фразы: views, clicks, CTR (НЕ orders — недоступно на Ozon) |
| 5 | ClickHouse `fact_ozon_transactions` | Per-SKU unit economics: base_price, commission, logistics, payout |
| 6 | PostgreSQL `dim_ozon_products` | Каталог: name, offer_id, image, marketing_price, min_price, old_price, price_index |
| 7 | PostgreSQL `dim_ozon_campaigns` | Информация о кампании: бюджет, ставка, стратегия, дата создания |
| 8 | PostgreSQL `product_costs` + `dim_ozon_products` | Себестоимость per-SKU (cost_price + packaging) |
| 9 | ClickHouse `fact_ozon_orders` | **Retention per-SKU**: total_buyers, repeat_buyers, repeat_rate, avg_days_between, avg_ltv_repeat |
| 10 | PostgreSQL `event_log` | События за период: изменения ставок, цен, контента |
| 11 | Gemini 2.5 Flash | Streaming ИИ-анализ через kie.ai API (SSE) |

**WB (11 запросов):**

| # | Источник | Что запрашивается |
|---|----------|-------------------|
| 1 | ClickHouse `fact_advert_stats_v3` | Ежедневная статистика: views, clicks, spend, orders, carts |
| 2 | ClickHouse `fact_orders_raw` | Ежедневные заказы (direct + model + associated), выручка |
| 3 | ClickHouse `fact_finances` | Per-SKU unit economics: revenue, payout, комиссия, логистика, хранение, эквайринг |
| 4 | ClickHouse `fact_advert_phrases_daily` | Ключевые фразы (marketplace=1): views, clicks, spend, revenue |
| 5 | PostgreSQL `dim_products` | Каталог: name, vendor_code, imt_id, main_image_url |
| 6 | ClickHouse `dim_advert_campaigns` | Информация: название, тип, статус, payment_type |
| 7 | PostgreSQL `product_costs` + `dim_products` | Себестоимость per-SKU (cost_price + packaging) |
| 8 | PostgreSQL `event_log` | События за период |
| 9 | ClickHouse `fact_orders_raw` | **Fallback unit economics** — при пустом `fact_finances` |
| 10 | PostgreSQL `dim_products` | imt_id для 3-уровневой классификации продаж |
| 11 | Gemini 2.5 Flash | Streaming ИИ-анализ через kie.ai API (SSE) |

### Ключевая логика

**P&L расчёт (pre-calculated, не AI):**
- `total_prod_orders` из таблицы заказов (все заказы, не только рекламные)
- `avg_payout` из финансовых таблиц (per-unit данные)
- `total_payout = total_prod_orders × avg_payout`
- `total_cost = total_prod_orders × cost_per_unit` (из `product_costs`)
- `profit_after_ads = total_payout − total_cost − ad_spend`
- Для multi-SKU: раздельный P&L по каждому товару через `prod_orders_by_sku`

**WB-специфика:**
- **Классификация продаж** (3 типа): direct (рекламируемые SKU) / model (тот же imt_id) / associated (другие)
- **Ставки** хранятся в копейках → конвертация в рубли (÷100) в данных для ИИ
- **Промпт WB** (`SYSTEM_PROMPT_WB`): CPM (оплата за показы), минус-фразы, размещения Поиск/Рекомендации, СПП (Скидка Постоянного Покупателя)
- **Fallback юнит-экономика**: при пустом `fact_finances` (новый товар, нет отчётов) — операционные данные из `fact_orders_raw`: revenue per unit, estimated commission (27%), estimated logistics (15%), aggregate P&L

**Retention/LTV (per-SKU, за всё время):**
- `client_id = splitByChar('-', posting_number)[1]` (Ozon) / `substring(srid, ...)` (WB)
- `repeat_rate = repeat_buyers / total_buyers × 100`
- `avg_days_between` — средний интервал повторных покупок
- `avg_ltv_repeat` — средний LTV повторного покупателя
- Используется для расчёта эффективного CAC: `CAC / avg_orders_per_buyer`

**Retry / Timeout (Gemini API):**
- **Timeout**: connect=15s, read=170s (decoupled для long-thinking models)
- **Retry**: до 2 retries с exponential backoff (2с→4с) на HTTP 429 (Rate Limit), 503 (Server Overload), ReadTimeout, ConnectTimeout
- **Async sleep**: `asyncio.sleep()` для non-blocking retry delays

**Системный промпт — ключевые правила:**
- **Ozon**: нет минус-фраз, только CPC и бюджет, субсидии (аналог СПП WB), `min_price` ≠ цена конкурента
- **WB**: CPM (оплата за показы), минус-фразы, 3 типа размещения, СПП, 3 типа продаж
- Снижение цены НЕ может убивать продажи (корреляция ≠ причинность)
- При <30 заказах — оговорка о статистической незначимости
- Рекламные заказы уже включены в общие (не складывать)

---

## Продажи — `/api/v1/sales`

### Endpoints

| Метод | Path                        | Описание                                  | Auth   |
| ----- | --------------------------- | ----------------------------------------- | ------ |
| `GET` | `/sales/ozon`               | KPI + графики + ТОП товаров Ozon          | Bearer |
| `GET` | `/sales/ozon/product-daily` | Дневная динамика по конкретным SKU (Ozon) | Bearer |
| `GET` | `/sales/ozon/abc-xyz`       | ABC/XYZ анализ товаров Ozon               | Bearer |
| `GET` | `/sales/ozon/abc-xyz/xlsx`  | Excel экспорт ABC/XYZ Ozon (3 листа)      | Bearer |
| `GET` | `/sales/ozon/forecast`      | Прогноз продаж Ozon (LightGBM)            | Bearer |
| `GET` | `/sales/wb`                 | KPI + графики + ТОП товаров WB            | Bearer |
| `GET` | `/sales/wb/product-daily`   | Дневная динамика по конкретным SKU (WB)   | Bearer |
| `GET` | `/sales/wb/abc-xyz`         | ABC/XYZ анализ товаров WB                 | Bearer |
| `GET` | `/sales/wb/abc-xyz/xlsx`    | Excel экспорт ABC/XYZ WB (3 листа)        | Bearer |
| `GET` | `/sales/wb/forecast`        | Прогноз продаж WB (LightGBM)              | Bearer |

### Query Parameters (общие для ozon/wb)

```
shop_id: int (required)
period: int (default: 7, 1-366) — период в днях
date_from: date (optional) — начало кастомного диапазона
date_to: date (optional) — конец кастомного диапазона
```

### Response Schema (GET /sales/ozon и /sales/wb)

```
{
  kpi: {
    orders_count, orders_delta,
    revenue, revenue_delta, avg_check,
    views, views_delta,
    clicks, clicks_delta,
    ad_spend, ad_spend_delta,
    carts, carts_delta,
    drr, drr_delta
  },
  charts: {
    daily: [{ date, orders, revenue, views, clicks, carts, ad_spend, drr }]
  },
  top_products: [{
    sku, name, image_url, orders, revenue, delta_pct, ad_spend, drr
  }]
}
```

### ABC/XYZ анализ (GET /sales/ozon/abc-xyz, /sales/wb/abc-xyz)

```
shop_id: int (required)
period: int (default: 90, 14-365)
use_profit: bool (default: false) — ABC по чистой прибыли вместо выручки
```

**Формула ABC:**

- Сортировка товаров по убыванию выручки (или прибыли)
- Кумулятивный % → A (≤80%), B (80-95%), C (>95%)

**Формула XYZ:**

- CV = std(daily_orders) / mean(daily_orders) × 100
- X (CV<10%), Y (CV 10-25%), Z (CV>25%)

**Формула чистой прибыли:**
`profit = revenue − commission − logistics − storage − acquiring − ad_spend − cogs`

---

## Клиентская аналитика (LTV) — `/api/v1/sales`

### Endpoints

| Метод | Path                    | Описание                                                | Auth   |
| ----- | ----------------------- | ------------------------------------------------------- | ------ |
| `GET` | `/sales/ozon/ltv`       | KPI + когорты + SKU повторы + distrib. + monthly_buyers | Bearer |
| `GET` | `/sales/ozon/ltv/chain` | Цепочка покупок L1→L5 по SKU (Ozon)                     | Bearer |
| `GET` | `/sales/ozon/ltv/xlsx`  | Excel отчёт LTV (7 листов, .xlsx)                       | Bearer |
| `GET` | `/sales/wb/ltv`         | KPI + когорты + SKU повторы + distrib. + monthly_buyers | Bearer |
| `GET` | `/sales/wb/ltv/chain`   | Цепочка покупок L1→L5 по SKU (WB)                       | Bearer |
| `GET` | `/sales/wb/ltv/xlsx`    | Excel отчёт LTV (7 листов, .xlsx)                       | Bearer |

### Query Parameters

```
shop_id: int (required)
period: "30d" | "90d" | "6m" | "1y" | "all" (default: "6m")
date_from: date (optional)
date_to: date (optional)
sku: int (required для /chain) — offer_id (Ozon) или nm_id (WB)
```

### Response Schema (GET /sales/{mp}/ltv)

```
{
  kpi: {
    total_clients, repeat_clients, repeat_rate,
    avg_ltv, avg_check, avg_orders_per_client, total_revenue
  },
  monthly_buyers: [{
    month: "2025-09",
    new_buyers: 150, repeat_buyers: 42,
    new_revenue: 45000, repeat_revenue: 38000
  }],
  cohort_matrix: [{
    cohort: "2025-09",
    size: 1234,
    months: { "0": { clients, rate }, "1": { clients, rate }, ... }
  }],
  sku_table: [{
    sku, offer_id, name, image_url,
    total_buyers, total_qty, total_revenue,
    repeat_buyers, buyers_3plus,
    conv_to_2, conv_to_3,
    avg_days_between, avg_ltv_repeat
  }],
  time_distribution: [{ bucket: "0-7", count, avg_days }]
}
```

### Response Schema (GET /sales/{mp}/ltv/chain)

```
{
  l1: { sku, offer_id, name, total_buyers, total_qty, total_revenue, avg_price },
  chain: [{
    level: 1..5,
    total_buyers, conversion_from_prev, conversion_from_l1,
    products: [{ sku, offer_id, name, buyers, pct_of_l1, pct_of_level, avg_revenue }]
  }],
  avg_days_between: { l1_to_l2, l2_to_l3, l3_to_l4, l4_to_l5 }
}
```

### Excel экспорт (GET /sales/{mp}/ltv/xlsx)

7 листов в .xlsx файле:

| Лист | Название            | Содержимое                                                             |
| ---- | ------------------- | ---------------------------------------------------------------------- |
| 1    | 📊 KPI              | Основные метрики (total/repeat clients, avg LTV/check, revenue)        |
| 2    | 📅 Месяцы           | Новые/повторные покупатели + выручка по месяцам (% доли каждой группы) |
| 3    | 🗺️ Retention по SKU | Карта удержания: покупка 1→5, % переходов, heatmap                     |
| 4    | 📦 Товары           | Таблица повторных: покупатели, % повтора, ср. дней, ср. чек, выручка   |
| 5    | 🔄 Когорты          | Когортная матрица с % удержания                                        |
| 6    | ⏱ Время             | Дистрибуция дней между покупками                                       |
| 7    | 🔀 Переходы         | Кросс-SKU цепочки: top-15 SKU → товары перехода на уровнях 2–5 (top-3) |

**Лист «Переходы»** — дополнительный ClickHouse запрос:

- Для топ-15 SKU по повторным: dense_rank по клиентам, reindex от target SKU, GROUP BY (target_sku, level, sku)
- Столбцы: исходный товар, покупка №, товар перехода, покупателей, % от исходных
- ⭐ маркировка повторной покупки того же товара

### Ключевая логика

**Ozon** (`ltv.py`):

- Client ID = `splitByChar('-', posting_number)[1]` — первый сегмент posting_number
- Источник: `fact_ozon_orders FINAL`
- Обогащение: `dim_ozon_products` (PostgreSQL) → name, image_url

**WB** (`wb_ltv.py`):

- Buyer ID = `substring(splitByChar('.', srid)[1], 1, 8)` — первые 8 цифр числовой части srid
- Фильтр: числовые srid длиной 16-19 символов (покрытие ~95%, точность 97%)
- Источник: `fact_orders_raw FINAL`
- Обогащение: `dim_products` (PostgreSQL) → name, vendor_code + CDN `wb_image_url(nm_id)`

**Retention по SKU** (ClickHouse запрос):

- window функции: `row_number`, `min`, `max`, `count` по (sku, client_id)
- `b1 = countDistinct(client_id)` — все покупатели
- `b2..b5 = countDistinctIf(purchase_num >= N)` — повторные
- `avg_days = avgIf(dateDiff / greatest(total-1, 1), total>=2 AND purchase_num=1)` — фильтр по purchase_num=1 избегает дублирования
- Порог: `b1 >= 3` (минимум 3 покупателя), лимит 100 SKU

---

## Финансы — `/api/v1/finances`

### Endpoints

| Метод | Path                         | Описание                           | Auth   |
| ----- | ---------------------------- | ---------------------------------- | ------ |
| `GET` | `/finances/ozon`             | P&L Ozon (waterfall)               | Bearer |
| `GET` | `/finances/wb`               | P&L WB (waterfall)                 | Bearer |
| `GET` | `/finances/ozon/products`    | Товарная прибыль Ozon              | Bearer |
| `GET` | `/finances/wb/products`      | Товарная прибыль WB                | Bearer |
| `GET` | `/finances/ozon/excel`       | Excel отчёт Ozon (6 листов)       | Bearer |
| `GET` | `/finances/wb/excel`         | Excel отчёт WB (6 листов)         | Bearer |
| `GET` | `/finances/ozon/weekly-report` | Понедельный отчёт Ozon           | Bearer |
| `GET` | `/finances/wb/weekly-report`   | Понедельный отчёт WB             | Bearer |

### Excel экспорт (GET /finances/{mp}/excel)

Параметры: `shop_id`, `date_from`, `date_to`

**Ozon — 6 листов в .xlsx файле:**

| Лист | Название         | Содержимое                                                     |
| ---- | ---------------- | -------------------------------------------------------------- |
| 1    | Сводка           | KPI текущий/предыдущий период, waterfall + изменение %         |
| 2    | По дням          | Дневная динамика: заказы, выручка, пр. расходов                |
| 3    | По неделям       | Полная ретроспектива с момента создания магазина                |
| 4    | По месяцам       | Полная ретроспектива помесячно                                 |
| 5    | По товарам       | SKU P&L: выручка, логистика, реклама, COGS, прибыль, маржа%   |
| 6    | Расходы детально | Разбивка по типу операции и бонуса (без Продажа/Возврат)       |

**WB — 5 листов в .xlsx файле:**

| Лист | Название         | Содержимое                                                     |
| ---- | ---------------- | -------------------------------------------------------------- |
| 1    | Сводка           | KPI текущий/предыдущий период, P&L waterfall + % от выручки    |
| 2    | По неделям       | Полная ретроспектива + Δ-колонки (Комис%, Логист%, ВБПромо%, С/С%, Приб%) |
| 3    | По месяцам       | Полная ретроспектива помесячно + Δ-колонки                     |
| 4    | По товарам       | SKU P&L: выручка, логистика, ВБ Промо, COGS, прибыль, ДРР%   |
| 5    | Расходы детально | Разбивка по типу операции и бонуса + Итого + % от выручки      |

> **Лист «По дням» удалён для WB** (v15) — избыточен, данные есть в понедельном/помесячном.

**WB-специфика Excel:**

- **Реклама = ВБ Промо** (из `fact_finances` deductions с типом «продвижение»). Единственный источник — НЕ дублируется с `fact_advert_stats_v3`
- **SKU ad spend:** маппинг `nm_id → vendor_code` через `fact_finances` → join `fact_advert_stats_v3` по `nm_id`
- **Per-SKU хранение:** из `fact_wb_paid_storage FINAL` (v14), перезаписывает нулевые значения из `fact_finances`
- **penalty_total:** фильтруется `operation_type != 'Удержание'` (дублирует deduction для Удержание-операций)
- **Прибыль:** `Payout − Логистика − Хранение − Приёмка − Удержания − ВБ Промо − Штрафы − COGS`

## Общий паттерн async task endpoints

Все sync-endpoints возвращают `task_id` → клиент полит через GET `/status/{task_id}`:

```
1. POST /sync → Celery .delay() → { task_id: "uuid", message: "Started..." }
2. GET /status/{task_id} → AsyncResult → { status, progress?, result?, error? }
   - PENDING → STARTED → PROGRESS { current, total } → SUCCESS { rows_inserted }
   - FAILURE { error: "..." }
```

---

## Dependency Injection

```python
get_db()            → AsyncSession (PostgreSQL)
get_current_user()  → User (JWT decode → SELECT user + shops)
```

`get_current_user` используется как `Depends()` в auth/shops/dashboard/products/sales/finances/warehouses endpoints.

---

## Склады — `/api/v1/warehouses`

### Ozon Supply — `/api/v1/warehouses/ozon/supply`

| Метод | Path                           | Описание                        | Auth   |
| ----- | ------------------------------ | ------------------------------- | ------ |
| `GET` | `/warehouses/ozon/supply`      | Рекомендации по поставке (JSON) | Bearer |
| `GET` | `/warehouses/ozon/supply/xlsx` | Excel-экспорт (5 листов)        | Bearer |

### Query Parameters

```
shop_id: int (required)
sales_period: int (default: 30)      — период анализа продаж (дни)
target_days: int (default: 60)       — на сколько дней формировать запас
safety: float (default: 1.15)        — коэффициент безопасности
use_ad_boost: bool (default: true)   — учитывать рекламный буст
```

### Response Schema (GET /supply)

```
{
  items: [{
    offer_id, sku, name, image_url,
    fbo_stock, days_supply, total_sold, total_need,
    boost, status: "critical" | "attention" | "ok",
    ad_spend_7d, ad_views_7d, ad_clicks_7d, ad_carts_7d, ad_orders_7d,
    clusters: [{
      cluster, sold, share, daily, daily_boosted,
      est_stock,               // backward compat (= wh_stock)
      wh_stock,                // реальный сток на РФЦ кластера
      need, revenue,
      hub, hub_hours,          // приоритетный склад отгрузки
      warehouse,               // название РФЦ
      warehouses: ["WH1 (qty)", ...]  // все склады с остатками
    }]
  }],
  hubs: [{
    hub: str,                     // склад отгрузки
    total_need: int,
    total_revenue: float,
    items: [{
      offer_id, name, image_url, cluster,
      hub_hours, daily_boosted, need, revenue,
      wh_stock                 // реальный сток на РФЦ
    }]
  }],
  summary: { total_skus, total_need, critical, attention }
}
```

### Маппинг РФЦ → Кластеры (WAREHOUSE_TO_CLUSTER)

34 Ozon РФЦ привязаны к кластерам доставки. Примеры:

| РФЦ (warehouse_name)         | Кластер доставки                |
| ---------------------------- | ------------------------------- |
| ГРИВНО_РФЦ, НОВАЯ_РИГА_РФЦ  | Москва, МО и Дальние регионы    |
| ХОРУГВИНО_КРУПНОГАБАРИТ_РФЦ  | Москва, МО и Дальние регионы    |
| СПБ_БУГРЫ_РФЦ, СпБ_РФЦ      | Санкт-Петербург и СЗО           |
| ЕКАТЕРИНБУРГ_ХАУС_РФЦ        | Екатеринбург                    |
| НОВОСИБИРСК_РФЦ              | Новосибирск                     |
| РОСТОВ_РФЦ                   | Ростов                          |
| КАЗАНЬ_ЗЕЛЕНОДОЛЬСК_РФЦ      | Казань                          |

Обратный маппинг `CLUSTER_TO_WAREHOUSES_OZON` — для агрегации стоков: `{cluster → [warehouse_name, ...]}`

### Матрица доставки (DELIVERY_HOURS)

25×25 кластеров Ozon. Источник: «Нормативное время доставки 01/2026».

- Собственный кластер = 28ч, ближайшие = 45ч, далёкие = 60–75ч
- Используется для упорядочивания хабов по приоритету доставки

### WB Supply — `/api/v1/warehouses/wb/supply`

Аналогичная структура, но адаптирована под WB:

| Метод | Path                       | Описание                        | Auth   |
| ----- | -------------------------- | ------------------------------- | ------ |
| `GET` | `/warehouses/wb/supply`      | Рекомендации по поставке (JSON) | Bearer |
| `GET` | `/warehouses/wb/supply/xlsx` | Excel-экспорт (4 листа)        | Bearer |

**WB-специфика:**

- food/SGT: привязка к `:Питание` складам, парный stock/daily
- Storage: платное с 1-го дня (нет free period)
- cross-drain re-balance: `need` центрального склада уменьшается на долю кросс-drain
- global cap: `sum(needs) ≤ boosted_daily × target_days × safety − total_stock`

### WB Analytics — `/api/v1/warehouses/wb/analytics`

| Метод | Path                                     | Описание                   | Auth   |
| ----- | ---------------------------------------- | -------------------------- | ------ |
| `GET` | `/warehouses/wb/analytics`               | Обзор складов + кросс      | Bearer |
| `GET` | `/warehouses/wb/analytics/stock-report/excel` | Excel остатков (2 листа)  | Bearer |

---

## События — `/api/v1/events`

### Endpoints

| Метод | Path             | Описание                         | Auth   |
| ----- | ---------------- | -------------------------------- | ------ |
| `GET` | `/events`        | Лента событий                    | Bearer |
| `GET` | `/events/stats`  | График событий (по дням)         | Bearer |

### Query Parameters

```
shop_id: int (required)
period: "7d" | "14d" | "30d" | "custom" (default: "7d")
date_from: date (optional)
date_to: date (optional)
category: "all" | "advertising" | "content" | "commercial"  — фильтр по категории
```

### Response Schema (GET /events)

```
{
  events: [{
    id: int,
    event_type: str,
    nm_id: int,
    old_value: str?,
    new_value: str?,
    event_metadata: jsonb,
    created_at: datetime,
    product: {
      vendor_code, name, image_url
    }
  }],
  total: int
}
```

### Event Types

**Ozon:**

| Тип                     | Категория   | Описание                                              |
| ----------------------- | ----------- | ----------------------------------------------------- |
| `OZON_BID_CHANGE`       | advertising | Изменение ставки                                  |
| `OZON_STATUS_CHANGE`    | advertising | Статус кампании изменён                           |
| `OZON_BUDGET_CHANGE`    | advertising | Бюджет кампании изменён                           |
| `OZON_ITEM_ADD`         | advertising | Товар добавлен в кампанию                         |
| `OZON_ITEM_REMOVE`      | advertising | Товар удалён из кампании                          |
| `OZON_CAMPAIGN_CREATED` | advertising | Новая кампания Ozon (с `campaign_items`)          |
| `OZON_PRICE_CHANGE`     | commercial  | Цена товара изменена                              |
| `OZON_STOCK_OUT`        | commercial  | Товар ушёл в out-of-stock                         |
| `OZON_STOCK_REPLENISH`  | commercial  | Товар вернулся на склад                           |
| `OZON_CONTENT_CHANGE`   | content     | Изменение контента карточки                       |
| `OZON_PHOTO_CHANGE`     | content     | Изменение фото (main_image или gallery)           |

**WB:**

| Тип                     | Категория   | Описание                                              |
| ----------------------- | ----------- | ----------------------------------------------------- |
| `ITEM_INACTIVE`         | advertising | Товар деактивирован (WB)                          |
| `BID_CHANGE`            | advertising | Изменение ставки (WB)                             |
| `STATUS_CHANGE`         | advertising | Статус кампании изменён (WB)                      |
| `ITEM_ADD`              | advertising | Товар добавлен (WB)                               |
| `ITEM_REMOVE`           | advertising | Товар удалён (WB)                                 |
| `CAMPAIGN_CREATED`      | advertising | Новая кампания WB (с `campaign_items`)            |
| `PRICE_CHANGE`          | commercial  | Цена товара изменена (WB)                         |
| `STOCK_OUT`             | commercial  | Out-of-stock (WB)                                 |
| `STOCK_REPLENISH`       | commercial  | Товар пополнен (WB)                               |
| `CONTENT_CHANGE`        | content     | Изменение контента карточки (WB)                  |
| `PHOTO_CHANGE`          | content     | Изменение фото (WB)                               |

---

## Управление рекламой WB — `/api/v1/ad-management`

> **Файл:** `backend/app/api/v1/ad_management.py` (~2600 строк)  
> **Описание:** Полное управление WB-рекламой: статусы, ставки, бюджеты, создание кампаний, авто-пополнение

### Endpoints (автопополнение бюджета)

| Метод  | Path                              | Описание                                         | Auth   |
| ------ | --------------------------------- | ------------------------------------------------ | ------ |
| `GET`  | `/ad-management/wb/auto-budget`   | Настройки автопополнения для кампании             | Bearer |
| `POST` | `/ad-management/wb/auto-budget`   | Сохранение/обновление настроек (UPSERT)          | Bearer |

### Query Parameters (GET)

```
shop_id: int (required)
advert_id: int (required)
```

### Request Body (POST)

```
{
  shop_id: int,
  advert_id: int,
  enabled: bool,
  threshold: int,          // ₽ — порог для пополнения (default: 500)
  amount: int,             // ₽ — сумма пополнения (default: 1000)
  max_per_day: int         // Макс. пополнений в день (default: 5)
}
```

### Response Schema (GET)

```
{
  enabled: bool,
  threshold: int,
  amount: int,
  max_per_day: int,
  deposits_today: int,
  last_deposit_at: str?    // ISO datetime
}
```

### Ключевая логика

- **GET:** если настройки не найдены → дефолт: `enabled=false, threshold=500, amount=1000, max_per_day=5`
- **POST:** UPSERT через `SELECT ... WHERE advert_id=X` → update или insert. Audit log: `action=auto_budget_config`
- **Потребитель:** Celery task `sync_wb_budgets` (каждые 15 мин) читает `ad_auto_budget WHERE enabled=true` и автоматически вызывает `deposit_budget()` при `budget < threshold`

---

## Changelog

### 2026-02-19

- **CRUD шифрование ключей** при создании магазина
- **validate_key**: проверка 7 WB API доменов + Ozon seller+perf раздельно
- **delete_shop**: ClickHouse (6 таблиц) + PG каскад + Redis

### 2026-02-21

- **Dashboard WB**: поддержка WB в dashboard endpoint
- **WB CDN**: `wb_image_url()` для динамической генерации URL фото

### 2026-02-22

- **Ozon marketing_seller_price**: `/v5/product/info/prices` интеграция
- **WB acquiring_fee**: маппинг в `fact_finances`

### 2026-03-09

- **WB Supply**: рекомендации по поставке FBO с кластерной разбивкой

### 2026-03-10

- **WB Supply**: единый интерфейс Ozon + WB, Excel экспорт
- **ABC/XYZ Excel**: 3 листа, цветовое кодирование
- **LTV Excel**: 7 листов, цепочки покупок

### 2026-03-12

- **Ozon Supply per-warehouse stocks**: реальные остатки на РФЦ

### 2026-03-14

- **Ozon/WB Supply — cross-drain, real_days_supply, food/SGT paired warehouses**
- **WB Supply Excel**: 4 листа (Рекомендации, По складам, Поставка по складам, Риск перезатаривания)
  - Excel: колонки «Реал.зап, дн» + «Кросс» в листе Рекомендации
- **WB Supply — food/SGT paired warehouse fix:**
  - `need = 0` на обычных складах для food-товаров (поставка только на `:Питание`)
  - `paired_stock`: food-вариант учитывает stock парного обычного склада
  - `paired_daily`: food-вариант агрегирует продажи парного склада

### 2026-03-16

- **Ozon Geography** (3 JSON endpoints + AI): кластеры, города, стабильность, товары, drill-down
- **Ozon Storage**: аналитика хранения FBO, зонирование free/warning/paid, placement cost sync/backfill
- **Ozon Cross**: кросс-матрица `warehouse × cluster`, per-SKU cross_pct, AI-анализ V4 (обзорный формат)
- **Ozon Overview**: обзорный дашборд складов (~380 строк), 9 типов расходов, KPI с трендами, OOS, per-warehouse status
- **Ozon Placement Cost**: `fact_ozon_placement_cost`, Celery tasks `sync/backfill_ozon_placement_cost`
- **WB Supply — global cap**: `sum(needs)` ≤ `boosted_daily × target_days × safety − total_stock`

### 2026-03-17

- **WB Supply — cross-drain re-balance**: при поставке в регион → уменьшение `effective_daily` центрального склада на долю кросс-drain; food/SGT: не вычитается если нет food-совместимого склада в регионе
- **WB Excel «Поставка по складам»**: `paired_orders`/`paired_revenue` для food/SGT
- **WB Excel «Риск перезатаривания»**: фильтр `turnover_days > target_days` (не хардкод 45), `storage_per_month`, `excess_qty`
- **Ozon Overview — расширение расходов**: 9 типов вместо 5, `cross_problem_warehouses[]`
- Обновлена документация всех Ozon warehouse endpoints

### 2026-03-18

- **Excel экспорт остатков (WB + Ozon)**: `GET /wb/analytics/stock-report/excel`, `GET /ozon/overview/stock-report/excel` — 2 листа (По складам, По товарам), подсветка OOS/дефицит/излишек
- **Общая функция** `_build_stock_report_excel()` — shared логика для обоих маркетплейсов
- **Fix**: `dim_products` и `dim_ozon_products` — запросы через PostgreSQL (SQLAlchemy), не ClickHouse
- **WB `products_summary`**: OOS-товары включены в ответ `/wb/analytics` для фильтрации на фронтенде

### 2026-03-19

- **Новый роутер** `campaign_details_router` (16-й роутер): 6 endpoints для детальной аналитики кампаний (kpi, stats, events, phrases, heatmap, purchases)
  - Универсальный: Ozon (`fact_ozon_ad_daily`/`fact_ozon_orders`) и WB (`fact_advert_stats_v3`/`fact_orders_raw`)
  - KPI с дельтами (текущий vs предыдущий период), product_revenue из таблиц заказов
  - Events: конвертация Ozon `sku` → `product_id` для `event_log`, enrichment с названиями из PG
  - Phrases: агрегация из `fact_advert_phrases_daily` (Enum8 marketplace)
- **Новая таблица ClickHouse:** `fact_advert_phrases_daily` — поисковые фразы рекламы (миграция 008)
- **Новые таблицы PostgreSQL:** `dim_ozon_campaigns` + `dim_ozon_campaign_products` (миграция Alembic `b81f3ce45f30`)
- **Новый сервис:** `ozon_campaigns_loader.py` — синхронизация справочника кампаний Ozon → PostgreSQL
- **Новый сервис:** `ozon_ads_service.py` расширен: `order_phrases_report()` + `parse_phrases_csv_report()`, ZIP detection, улучшенный retry

### 2026-03-20

- **Рекламная аналитика**: `product_revenue` в `CampaignStatsRow`, поиск по кампаниям, улучшения UI

### 2026-03-22

- **ИИ-анализ кампании** (`campaign_ai_analysis.py`): SSE streaming endpoint, Gemini 2.5 Flash, JSON-секции, pre-calculated P&L
- **Retention per-SKU**: запрос повторных покупок из `fact_ozon_orders` (total_buyers, repeat_rate, avg_ltv_repeat)
- **Системный промпт**: правила логики (корреляция ≠ причинность), субсидии Ozon (СПП аналог), min_price ≠ цена конкурента
- Добавлена секция `campaign_ai_router` в роутинг

### 2026-03-25

- **`advertising_analytics.py`** — полная WB-рекламная аналитика:
  - Advertised vs Associated SKU разделение через `ads_raw_history.is_associated`
  - WB campaign enrichment: `dim_advert_campaigns` (ClickHouse) — name, type Enum8, status, payment_type, bid_type, placements
  - WB ставки из `log_wb_bids` (kopecks→rub), per-SKU total_drr из `fact_orders_raw`
  - WB CDN: `_wb_image_url()` — динамический расчёт basket host по vol диапазонам (vol>4781 → basket-18)
- **Zero-stat campaigns:** кампании без данных за выбранный период теперь отображаются с нулевой статистикой
  - WB: дополнительный запрос `dim_advert_campaigns FINAL` (исключая status=-1)
  - Ozon: запрос `dim_ozon_campaigns` (PostgreSQL) (исключая CAMPAIGN_STATE_ARCHIVED)
- **`campaign_details.py`** — фильтрация `STATUS_CHANGE` из событий popup, фикс `advert_id` фильтрации
- **`event_detector.py`** — debounce zero-bid API storm (WB BID_CHANGE)
- Добавлена секция «Рекламная аналитика» с полными response schemas

### 2026-03-26

- **WB ИИ-анализ кампаний** (`campaign_ai_analysis.py`) — полноценная WB-ветка:
  - **ROOT CAUSE** fix: `marketplace == "wildberries"` → `marketplace == "wb"` — WB промпт никогда не применялся, все кампании анализировались Ozon-промптом
  - `SYSTEM_PROMPT_WB`: CPM в копейках, минус-фразы, размещения (Поиск/Рекомендации), СПП, типы продаж (direct/model/associated через imt_id)
  - Финансы WB per-SKU: revenue, payout, комиссия, логистика, хранение, эквайринг из `fact_finances`
  - Себестоимость из `product_costs` по vendor_code
  - 3-уровневая классификация: direct / model (imt_id) / associated
  - Ключевые фразы из `fact_advert_phrases_daily` (marketplace=1)
  - P&L summary: payout - COGS - реклама = чистая прибыль
  - Campaign-attributed данные вместо глобальных продаж магазина
  - Имя кампании + ставки в рублях (из копеек) в ответе
  - Таблицы JSON (`unit_economics_table`, `pl_summary_table`) для структурированного отображения

### 2026-03-27

- **Fallback юнит-экономика** (`campaign_ai_analysis.py`): при пустом `fact_finances` — fallback на операционные заказы из `fact_orders_raw` с расчётом revenue per unit, estimated commission/logistics, сводный P&L
- **Retry логика Gemini API** (`campaign_ai_analysis.py`): до 2 retries с exponential backoff (2с→4с) на 429/503/ReadTimeout/ConnectTimeout; timeout разделён на connect=15s и read=170s
- **Ставки WB в events-detail** (`advertising_analytics.py`): `BID_CHANGE` old/new_value конвертируются из копеек в рубли (÷100); `OZON_BID_CHANGE` оставлен как есть (рубли)


### 2026-04-01

- **`campaign_details.py`** — WB normquery phrases fallback:
  - `CampaignPhraseRow` расширен: `+atbs` (корзины), `+avg_pos` (позиция), `+cpc` (CPC в ₽)
  - Phrases endpoint: если `fact_advert_phrases_daily` пуста для WB → fallback на `fact_normquery_stats_daily`
  - Формулы: `spend = Σ(cpc×clicks)/100`, `cpc = Σ(cpc×clicks)/Σ(clicks)/100`, `avg_pos` взвешенный по показам
  - Валидация данных: CPC в копейках из WB API (подтверждено), расхождение с основной статистикой < 2%

### 2026-04-05

- **Новые endpoints:** `GET/POST /ad-management/wb/auto-budget` — настройки автопополнения бюджета WB-кампаний
  - GET: возвращает настройки (дефолт: disabled, threshold=500₽, amount=1000₽, max_per_day=5)
  - POST: UPSERT настроек + audit log (`auto_budget_config`)
  - Модель: `AdAutoBudget` (PostgreSQL), миграция `abe1a57ab0a1`
  - Потребитель: Celery `sync_wb_budgets` — проверяет `WHERE enabled=true` каждые 15 мин

### 2026-04-06

- **Создание кампании — retry автозапуска:** `POST /creation/create` теперь выполняет до 3 попыток старта с exponential backoff (3с, 5с, 8с) при ошибке `"Low Budget"` (race condition WB API между deposit и start). Response: `+start_error` при неудачном автозапуске
- **Рекомендации по запуску рекламы:** `GET /advertising-analytics/ad-launch-recommendations?shop_id=N` — анализ товаров без активных кампаний (3 категории: selling_no_ads, ads_paused, stagnant). Ozon: `dim_ozon_products` + `fact_ozon_ad_daily` + `fact_ozon_orders`. WB: `dim_products` + `fact_advert_stats_v3` + `fact_orders_raw` + `fact_inventory_snapshot`

### 2026-04-07

- **WB Products P&L refactor** (`wb_products.py`): единый источник `fact_finances FINAL` вместо смешения `fact_orders_raw` + `fact_finances`. Fallback на orders только для товаров без реализации (`fees_source='estimated'`)
- **General deductions fix** (`finances.py`, `wb_products.py`): непривязанные удержания (отзывы за баллы, авансы) больше не распределяются по товарам. Products page показывает только product-only экономику
- **Ozon revenue fix** (`products.py`): `accruals_for_sale` из transactions вместо `price × quantity` (завышение ~2.5x). `mp_fees = sale_commission + services_total`. Bulk charges (Acquiring, Storage) пропорционально revenue
- **Finance products enrichment** (`finances.py`): WB products — `name` + `image_url` из `dim_products`; Ozon products — `image_url` + `product_id` из `dim_ozon_products`

### 2026-04-09

- **Dashboard WB** (`dashboard.py`): `finance_summary` — P&L за неделю (Mon→Mon, 8 дней), 10+ строк расходов с % дельтами. Revenue из `fact_finances FINAL`, ad spend из MAX-reconciliation
- **Dashboard WB** (`dashboard.py`): `orders_feed` — два периода в одном ClickHouse запросе через `countIf/sumIf`, обогащение из `dim_products`
- Response schema `/dashboard/wb` обновлена: `+finance_summary`, `+orders_feed`, `+alerts`, KPI разбит на 4 группы (sales, funnel, ads, profit)
