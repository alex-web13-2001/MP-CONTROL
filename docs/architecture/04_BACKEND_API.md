# MP-CONTROL — Backend API

> REST API на FastAPI. Все endpoints начинаются с `/api/v1/`.  
> Файлы: `backend/app/api/v1/` (12 роутеров), `backend/app/schemas/auth.py`

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

### Response Schema

```
{
  shop_id: int,
  period: str,
  kpi: {
    orders_count, orders_delta,         // Заказы
    revenue, revenue_delta, avg_check,  // Продажи (сумма заказов × цена)
    views, views_delta,                 // Показы рекламы
    clicks, clicks_delta,               // Клики рекламы
    ad_spend, ad_spend_delta,           // Расход рекламы
    drr, drr_delta                      // DRR = ad_spend / revenue × 100
  },
  charts: {
    sales_daily: [{ date, orders, revenue }],
    ads_daily: [{ date, spend, views, clicks, cart, orders, drr_ad, drr_total }]
  },
  top_products: [{
    offer_id, supplier_article, name, image_url,
    orders, revenue, delta_pct,
    stock_fbo, stock_fbs, price,
    ad_spend, drr
  }]
}
```

### Ключевая логика

- 5 SQL-запросов к ClickHouse: заказы, реклама, график продаж, график рекламы, ТОП товаров
- **Все заказы:** фильтры на cancelled/is_cancel убраны — учитываются все статусы (совпадение с ЛК)
- **Timezone:** группировка по дате в МСК (UTC+3): `toDate(addHours(in_process_at, 3))` (Ozon), `toDate(addHours(date, 3))` (WB)
- **DRR** = `ad_spend / orders_revenue × 100` (НЕ ad_revenue)
- Delta = процент изменения к предыдущему аналогичному периоду
- Обогащение товаров именами/изображениями из PostgreSQL
- **Ozon images:** `COALESCE(NULLIF(primary_image_url, ''), main_image_url, '')` — приоритет primary_image
- **WB images:** динамическая генерация CDN URL через `wb_image_url(nm_id)`
- Проверка ownership магазина через `get_current_user`

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

- 9 источников данных: PG каталог + product_costs → CH заказы/транзакции/реклама/возвраты/комиссии/контент-рейтинг/промоакции → PG events
- **Стабильная сортировка**: composite key `(primary_value, offer_id)` — гарантирует детерминированную пагинацию
- **Формула прибыли Ozon** (revenue-based, привязана к дате заказа):
  - `revenue_7d` = `sum(price × quantity)` из `fact_ozon_orders` — выручка по цене из ЛК
  - `txn_payout` = `sum(amount)` из `fact_ozon_transactions` — для расчёта удержаний
  - `mp_fees` = `revenue_7d − txn_payout` — ВСЕ удержания Ozon (SPP + комиссия + логистика + хранение)
  - `gross_profit` = `revenue_7d − COGS − mp_fees − ad_spend` — чистая прибыль
  - `gross_profit_percent` = `gross_profit / revenue_7d × 100` — % от выручки
  - ⚠ Ранее: `gross_profit = txn_payout − COGS − ad` — некорректно, т.к. `txn_payout` не привязан к периоду заказов (включает расчёты за старые заказы)
  - Детализация `mp_fees`: `mp_fees_commission` (скидки+комиссия+эквайринг), `mp_fees_logistics` (логистика+хранение+возвраты)
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

- 8 источников: PG каталог + product_costs → CH заказы/остатки/реклама/финансы
- Формула прибыли: `payout - COGS - ads`, где `payout = revenue - mp_fees`
- WB CDN: `wb_image_url(nm_id)` — динамическая генерация URL фото
- Серверные `totals`: итоги по всем товарам до пагинации
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

---

## Продажи — `/api/v1/sales`

### Endpoints

| Метод | Path                        | Описание                                  | Auth   |
| ----- | --------------------------- | ----------------------------------------- | ------ |
| `GET` | `/sales/ozon`               | KPI + графики + ТОП товаров Ozon          | Bearer |
| `GET` | `/sales/ozon/product-daily` | Дневная динамика по конкретным SKU (Ozon) | Bearer |
| `GET` | `/sales/ozon/abc-xyz`       | ABC/XYZ анализ товаров Ozon               | Bearer |
| `GET` | `/sales/ozon/forecast`      | Прогноз продаж Ozon (LightGBM)            | Bearer |
| `GET` | `/sales/wb`                 | KPI + графики + ТОП товаров WB            | Bearer |
| `GET` | `/sales/wb/product-daily`   | Дневная динамика по конкретным SKU (WB)   | Bearer |
| `GET` | `/sales/wb/abc-xyz`         | ABC/XYZ анализ товаров WB                 | Bearer |
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

| Метод | Path                    | Описание                               | Auth   |
| ----- | ----------------------- | -------------------------------------- | ------ |
| `GET` | `/sales/ozon/ltv`       | KPI + когорты + SKU повторы + distrib. | Bearer |
| `GET` | `/sales/ozon/ltv/chain` | Цепочка покупок L1→L5 по SKU (Ozon)    | Bearer |
| `GET` | `/sales/wb/ltv`         | KPI + когорты + SKU повторы + distrib. | Bearer |
| `GET` | `/sales/wb/ltv/chain`   | Цепочка покупок L1→L5 по SKU (WB)      | Bearer |

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
- Когортная матрица, SKU repeat table, time distribution, purchase chain — идентичная структура с Ozon

---

## Финансы — `/api/v1/finances`

### Endpoints

| Метод | Path                      | Описание              | Auth   |
| ----- | ------------------------- | --------------------- | ------ |
| `GET` | `/finances/ozon`          | P&L Ozon (waterfall)  | Bearer |
| `GET` | `/finances/wb`            | P&L WB (waterfall)    | Bearer |
| `GET` | `/finances/ozon/products` | Товарная прибыль Ozon | Bearer |
| `GET` | `/finances/wb/products`   | Товарная прибыль WB   | Bearer |

---

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

`get_current_user` используется как `Depends()` в auth/shops/dashboard/products/sales/finances endpoints.

---

### 2026-02-19

- Добавлена секция `Дашборд Ozon — /api/v1/dashboard` с endpoint, response schema и логикой
- Обновлён список роутеров (6 вместо 5)

### 2026-02-21

- Добавлен WB dashboard endpoint `GET /dashboard/wb`
- Обновлена response schema: добавлен `ads_daily[]` в charts, `supplier_article` в top_products
- Документировано использование `primary_image_url` вместо `main_image_url` для Ozon
- Добавлена динамическая генерация CDN URL для WB (`wb_image_url(nm_id)`)

### 2026-02-22

- Добавлена полная секция «Товары — /api/v1/products» с 4 endpoints
- PATCH `/ozon/cost`: trim() offer_id, warning при cost > price
- POST `/ozon/cost/bulk`: warnings[] при cost > price
- Стабильная сортировка с composite key для пагинации без дублей

### 2026-02-24

- **Ozon**: Гибридная формула прибыли: revenue из orders (price×qty), profit из transactions (txn_payout)
- **Ozon**: `mp_fees` теперь = revenue − txn_payout (ВСЕ удержания Ozon), с детализацией: `mp_fees_commission`, `mp_fees_logistics`
- **Ozon + WB**: Серверные `totals` в ответе GET `/products/ozon` и `/products/wb` — итоги по всем товарам до пагинации
- **WB**: `mp_fees` из `fact_finances` за текущий период (ранее — пропорция за 90д), фактические суммы
- **WB**: Детализация fees: `mp_fees_commission`, `mp_fees_logistics`, `mp_fees_storage`, `mp_fees_other`
- **WB**: Формула прибыли: `payout - COGS - ads`, где `payout = revenue - mp_fees`

### 2026-02-25

- **Ozon**: Формула прибыли переписана: `revenue − COGS − mp_fees − ad_spend` (ранее: `txn_payout − COGS − ad` — давала абсурд при отрицательном txn_payout за период)
- **Ozon**: `totals` теперь содержит `payout` и `avg_price` (ранее: фронт показывал 0₽ / пустую ячейку)
- **Ozon**: `margin_percent` = `cost / price × 100` (доля С/с в цене, всегда положительный; ранее: маржа прибыли)
- **WB**: `margin` в backend = `gross_profit / sales_amount × 100` (ранее: / payout — % от выплаты)
- **WB**: Frontend `wbToOzon`: `margin_percent` = `(cost + packaging) / price × 100`, `grossProfitPct` = `profit / sales_amount × 100`

### 2026-02-26

- **WB Финансы — полный пересмотр P&L расчёта:**
  - **Source of truth:** `fact_finances FINAL` — единственный источник для P&L WB
  - **Revenue:** `retail_price_withdisc_rub` из `raw_payload` (розничная цена до скидок, ранее: `retail_amount`)
  - **Commission:** `revenue − payout` (включает SPP скидку + комиссию WB, ранее: только `commission_amount` = двойной счёт)
  - **Deductions:** извлекаются из `raw_payload.deduction` (ранее: не учитывались, потеря до 25K/нед)
  - **Operating expenses:** `logistics + storage + penalties + acquiring + acceptance + deductions`
  - **Формула прибыли:** `payout − operating − ads − cogs` (ранее: `payout − mp_fees`, где mp_fees включал комиссию → двойной счёт)
  - **Marketplace filter:** `marketplace = 1` (Enum8, ранее: `marketplace = 'wildberries'` — не работало)

### 2026-02-27

- **Ozon Финансы:** Расчёт товарной прибыли полностью переведён на транзакции (`fact_ozon_transactions`) для точного совпадения с балансом, устранено смешивание данных с `fact_ozon_orders`.
- **Ozon Финансы:** В ответ `GET /api/v1/finances/ozon/products` добавлено поле `name`. Извлекается JOIN-ом из PostgreSQL `dim_ozon_products` по `offer_id` для замены технических артикулов вроде `01-0001055`.
- **Ozon Dashboard:** Удалён фильтр `status NOT IN ('cancelled')` — теперь на дашборде учитываются ВСЕ заказы, включая отменённые, для совпадения с ЛК Ozon Seller.
- **WB Dashboard:** Удалён фильтр `is_cancel = 0` — аналогично, все заказы включены.
- **Ozon Dashboard:** Группировка по дате переведена на МСК (UTC+3): `toDate(addHours(in_process_at, 3))` вместо `toDate(in_process_at)`.
- **WB Dashboard:** Группировка по дате переведена на МСК (UTC+3): `toDate(addHours(date, 3))` вместо `toDate(date)`.
- **Ozon Финансы (COGS):** Маппинг `sku → offer_id` для товарной прибыли переведён на `dim_ozon_products` (PG) вместо `fact_ozon_orders` (CH). В CH offer_id содержал артефакты (лишние пробелы), что ломало lookup в `product_costs`.
- **Ozon + WB Финансы (COGS):** Расчёт себестоимости использует `net_qty = sales - returns` вместо `sales` — корректный учёт возвратов.

### 2026-03-01

- **Добавлена секция «Продажи — /api/v1/sales»** — 6 endpoints: Ozon/WB sales overview, product-daily, ABC/XYZ анализ
- **Добавлена секция «Финансы — /api/v1/finances»** — 4 endpoints: P&L и товарная прибыль для Ozon/WB
- **Роутинг обновлён:** 10 роутеров вместо 6 (добавлены products, wb_products, finances, sales)

### 2026-03-02

- **Добавлена секция «Товары WB — /api/v1/products/wb»** — 4 endpoints: GET list, PATCH cost, POST bulk, GET template
- **Sales endpoints:** добавлены `/sales/ozon/forecast` и `/sales/wb/forecast` (LightGBM, SKU-уровень)
- **Sales:** итого 8 endpoints в секции (+ forecast)
- **Forecast engine:** `forecast_engine.py` — внутренняя утилита для SKU-рекомендаций, не отдельный роутер

### 2026-03-03

- **Добавлена секция «Клиентская аналитика (LTV)»** — 4 endpoints: Ozon/WB LTV + Purchase Chain
- **Роутинг обновлён:** 12 роутеров (добавлены `ltv_router`, `wb_ltv_router`)
- **WB LTV** (`wb_ltv.py`): buyer_id из srid, когорты, SKU repeat, chain L1→L5
- **WB LTV обогащение:** `dim_products` (name, vendor_code) + `wb_image_url(nm_id)` CDN (ранее: несуществующая `dim_wb_products`)

### 2026-03-06

- **Добавлена секция «События — /api/v1/events»** — endpoint: GET `/events/feed` (лента событий)
- **Роутинг обновлён:** 13 роутеров (добавлен `events_router`)
- **Events feed:** обогащение товаров (фото, имя, артикул) из PG `dim_ozon_products`, campaign_title из 5-уровневого fallback (meta → STATUS_CHANGE → DB → Redis)

---

## Лента событий — `/api/v1/events`

### Endpoints

| Метод | Path           | Описание                               | Auth   |
| ----- | -------------- | -------------------------------------- | ------ |
| `GET` | `/events/feed` | Лента событий, сгруппированных по дням | Bearer |

### Query Parameters

```
shop_id: int (required)  — ID магазина
period: "today" | "7d" | "30d" | "90d"  — период (default: "7d")
category: "all" | "advertising" | "content" | "commercial"  — фильтр по категории
```

### Response Schema

```json
{
  "total": 41,
  "period": "7d",
  "days": [
    {
      "date": "2026-03-02",
      "label": "2 марта — понедельник",
      "events": [
        {
          "id": 1234,
          "created_at": "2026-03-02T13:33:34",
          "event_type": "OZON_BID_CHANGE",
          "category": "advertising",
          "label": "Изменение ставки",
          "advert_id": 19642583,
          "campaign_title": "АМ-СОБ-МЕЛ-ЯГ-1 — Поиск",
          "product": {
            "nm_id": 2064330323,
            "name": "Amare Сухой полнорационный корм...",
            "offer_id": "АМ-СОБ-МЕЛ-ЯГ-1",
            "image_url": "https://..."
          },
          "old_value": "12.00 ₽",
          "new_value": "18.00 ₽",
          "detail": "12.00 ₽ → 18.00 ₽"
        }
      ]
    }
  ]
}
```

### Типы событий

| event_type             | category    | Описание                       |
| ---------------------- | ----------- | ------------------------------ |
| `OZON_BID_CHANGE`      | advertising | Изменение ставки               |
| `OZON_STATUS_CHANGE`   | advertising | Статус кампании изменён        |
| `OZON_BUDGET_CHANGE`   | advertising | Бюджет кампании изменён        |
| `OZON_ITEM_ADD`        | advertising | Товар добавлен в кампанию      |
| `OZON_ITEM_REMOVE`     | advertising | Товар удалён из кампании       |
| `ITEM_INACTIVE`        | advertising | Товар деактивирован в кампании |
| `BID_CHANGE`           | advertising | Изменение ставки (WB)          |
| `STATUS_CHANGE`        | advertising | Статус кампании изменён (WB)   |
| `ITEM_ADD`             | advertising | Товар добавлен (WB)            |
| `ITEM_REMOVE`          | advertising | Товар удалён (WB)              |
| `CONTENT_CHANGE`       | content     | Контент изменён                |
| `CONTENT_DESC_CHANGED` | content     | Описание товара изменено       |
| `PRICE_CHANGE`         | commercial  | Цена изменена                  |

### Обогащение данных

- **Товар:** JOIN `event_log.nm_id` → `dim_ozon_products.sku` → name, offer_id, image_url
- **Campaign title:** 5-уровневый fallback:
  1. `event_metadata.campaign_title` (новые события)
  2. `event_metadata.title` только для STATUS_CHANGE/BUDGET_CHANGE
  3. STATUS_CHANGE из БД (того же advert_id)
  4. `campaign_title` из любых событий в БД
  5. Redis-кеш (записывается трекером)
