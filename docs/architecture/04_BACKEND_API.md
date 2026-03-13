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
router.include_router(warehouses_router)         # /api/v1/warehouses/*
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

**6 листов в .xlsx файле:**

| Лист | Название         | Содержимое                                                     |
| ---- | ---------------- | -------------------------------------------------------------- |
| 1    | Сводка           | KPI текущий/предыдущий период, waterfall + изменение %         |
| 2    | По дням          | Дневная динамика: заказы, выручка, пр. расходов                |
| 3    | По неделям       | Полная ретроспектива с момента создания магазина                |
| 4    | По месяцам       | Полная ретроспектива помесячно                                 |
| 5    | По товарам       | SKU P&L: выручка, логистика, реклама, COGS, прибыль, маржа%   |
| 6    | Расходы детально | Разбивка по типу операции и бонуса (без Продажа/Возврат)       |

**WB-специфика Excel:**

- **Реклама = ВБ Промо** (из deductions): `fact_advert_stats_v3` и `deduction` с продвижением — один источник. НЕ дублируется.
- **SKU ad spend:** маппинг `nm_id → vendor_code` через `fact_finances` → join `fact_advert_stats_v3` по `nm_id`
- **Хранение/удержания по SKU = 0:** WB не привязывает записи хранения/удержания к vendor_code
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

## Склады (Поставки FBO) — `/api/v1/warehouses`

### Endpoints

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
- `_resolve_hub(cluster)` — ищет кластер-источник с min часов доставки

### Объединённые группы (CONSOLIDATED_GROUPS)

9 хабов вместо 25 отдельных точек поставки:

| Хаб          | Обслуживает                                |
| ------------ | ------------------------------------------ |
| Москва       | Москва, Тверь, Ярославль, Беларусь         |
| СПб          | СПб                                        |
| Казань       | Казань, Самара, Уфа                        |
| Екатеринбург | Екатеринбург, Пермь, Тюмень, Оренбург      |
| Воронеж      | Воронеж, Саратов                           |
| Ростов       | Ростов, Краснодар, Невинномысск, Махачкала |
| Красноярск   | Красноярск, Новосибирск, Омск, ДВ          |
| Калининград  | Калининград                                |
| Астана       | Астана, Алматы                             |

### Excel-экспорт (5 листов)

| Лист | Название              | Содержимое                                                                    |
| ---- | --------------------- | ----------------------------------------------------------------------------- |
| 1    | Рекомендации          | SKU × кластер: status, sold, daily, need, **Сток РФЦ**, **Склады**, hub, hours |
| 2    | Сводка                | 1 строка на SKU: итого need, status, кол-во кластеров                         |
| 3    | Параметры             | Период, target_days, safety, boost — мета-информация + формула per-warehouse  |
| 4    | Поставка по кластерам | hub → SKU с need > 0 + колонка **Сток РФЦ**                                  |
| 5    | Объединённые кластеры | 9 хабов → SKU × кластер спроса + колонка **Сток РФЦ**, ∑need, время доставки |

### Ключевая логика

1. `fact_ozon_warehouse_stocks` (ClickHouse) → FBO остатки **по каждому складу** (`GROUP BY offer_id, warehouse_name`)
2. `WAREHOUSE_TO_CLUSTER` → агрегация `fbo_by_cluster: {offer_id: {cluster: qty}}`
3. `fact_ozon_orders` (ClickHouse) → продажи по кластерам за `sales_period`
4. `fact_ozon_ad_daily` (ClickHouse) → рекламная статистика 7д
5. `dim_ozon_products` (PostgreSQL) → имена, изображения
6. `need = max(0, ceil(daily_boosted × target_days × safety − wh_stock))` — **wh_stock = реальный сток на РФЦ кластера**
7. `boost = (views > 0) ? max(1.0, min(5.0, (ad_carts × 3) / daily)) : 1.0`

> **Важно:** до v2026-03-12 использовалась пропорциональная оценка `est_stock = fbo_total × share%`. Теперь сток берётся реальный по складам РФЦ, обслуживающим кластер.

---

### WB Supply — `/api/v1/warehouses/wb/supply`

| Метод | Path                         | Описание                        | Auth   |
| ----- | ---------------------------- | ------------------------------- | ------ |
| `GET` | `/warehouses/wb/supply`      | Рекомендации по поставке (JSON) | Bearer |
| `GET` | `/warehouses/wb/supply/xlsx` | Excel-экспорт (4 листа)         | Bearer |

### Query Parameters

```
shop_id: int (required)
sales_period: int (default: 30)      — период анализа продаж (дни)
target_days: int (default: 45)       — на сколько дней формировать запас
safety: float (default: 1.15)        — коэффициент безопасности
```

> **Важно**: WB не поддерживает Ad Boost (в отличие от Ozon).

### Response Schema (GET /wb/supply)

```
{
  kpi: {
    total_need, critical_count, attention_count, overstock_count,
    avg_days_supply, total_stock, total_sku, total_storage_month
  },
  items: [{
    nm_id, vendor_code, name, image_url, vol_liters,
    total_sold, total_stock, daily_avg, turnover_days,
    effective_days,              // реальный запас с учётом кросс-складского расхода (null если нет данных)
    total_need,
    status: "critical" | "attention" | "ok" | "overstock",
    storage_cost_month,
    product_type: "food" | "sgt" | "normal",
    warehouses: [{
      warehouse, stock, orders, daily, turnover_days,
      effective_days,            // реальный запас на этом складе (с учётом кросс-расхода)
      cross_daily,               // чужих заказов/день (из других округов)
      cross_okrugs: [{           // детализация кросс-расхода
        okrug, qty, daily
      }],
      need, storage_per_day, storage_per_month,
      storage_coef, acceptance_coef, acceptance, revenue,
      regional_orders, regional_daily, demand_regions,
      daily_boosted
    }]
  }],
  warehouse_summary: [{
    warehouse, items_count, total_stock, total_orders,
    total_need, total_revenue, storage_coef, acceptance
  }]
}
```

### Маппинг складов → округа (WAREHOUSE_TO_OKRUG)

50+ складов WB привязаны к 8 федеральным округам (включая варианты `:Питание` и `СГТ`):

| Склад (warehouse_name)                | Федеральный округ   |
| -------------------------------------- | ------------------- |
| Котовск, Подольск 4, Домодедово 2      | Центральный         |
| Шушары, СПб Шушары                     | Северо-Западный     |
| Казань, Самара (Новосемейкино)         | Приволжский         |
| Екатеринбург, Челябинск                | Уральский           |
| Краснодар (Тихорецкая), Волгоград      | Южный               |
| Новосибирск                            | Сибирский           |
| Хабаровск                              | Дальневосточный     |
| Ростов, Минеральные Воды               | Северо-Кавказский   |

### Кросс-складской анализ

Анализ фактического расхода стока по `warehouse_name × nm_id × oblast_okrug_name`:

1. SQL: `GROUP BY nm_id, warehouse_name, oblast_okrug_name` из `fact_orders_raw`
2. Если `oblast_okrug_name != WAREHOUSE_TO_OKRUG[warehouse_name]` → **кросс-слив**
3. `effective_days = stock / actual_daily` (включая кросс-заказы)
4. Пересчёт статуса: `effective_days < 14` → `critical`, `< target_days` → `attention`

### Food / SGT логика

- **Классификация**: категория `Товары для животных`, `Продукты питания` и т.д. → тип `food`
- **Ограничение**: food-товары принимают ТОЛЬКО склады с суффиксом `: Питание`
- **Парные склады**: «Котовск» и «Котовск: Питание» — одна физическая локация
- **need = 0** на обычном складе для food-товара (поставка только на `:Питание`)
- **paired_stock**: food-вариант учитывает stock парного обычного склада
- **paired_daily**: food-вариант агрегирует продажи парного склада (WB записывает под обычным именем)

### WB-специфика

- **Хранение платное с 1-го дня** — нет бесплатного периода
- **Фиксация коэффициентов**: 60 дней (большинство категорий), 90 дней (одежда/обувь)
- **Overstock**: `turnover_days > target_days` (настраиваемый порог)
- **Acceptance**: коэффициент приёмки склада (`"Без коэфф."` или `"x{N}"`)
- **storage_cost_month**: `vol_liters × tariff_per_liter × storage_coef × stock × 30`
- **helper**: `_build_wb_supply_data()` — общая логика для JSON и Excel endpoints

### Источники данных

1. `fact_inventory_snapshot` (ClickHouse) → остатки по складам
2. `fact_orders_raw` (ClickHouse) → заказы за `sales_period` (включая `warehouse_name`, `oblast_okrug_name`)
3. `dim_products` (PostgreSQL) → габариты, имена, vendor_code
4. `fact_wb_acceptance_tariffs` (ClickHouse) → тарифы приёмки/хранения/логистики
5. Redis (`state:image_url:{shop_id}:{nm_id}`) → URL изображений

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
- **WB LTV обогащение:** `dim_products` (name, vendor_code) + `wb_image_url(nm_id)` CDN

### 2026-03-10

- **LTV Excel экспорт:** 2 новых endpoint'a `/sales/ozon/ltv/xlsx` и `/sales/wb/ltv/xlsx` — 7-листовый Excel
- **`monthly_buyers`:** новый блок в LTV response — помесячные новые/повторные покупатели + выручка
- **Лист «Переходы»:** кросс-SKU цепочки (dense_rank, reindex, top-3 на уровнях 2–5)
- **Retention фикс:** `countDistinct` для b1 (все покупатели), `avgIf(purchase_num=1)` для avg_days
- **Лист «Товары»:** понятные заголовки («Ср. чек повторных ₽», «Повтор в 2-ю покупку»)

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
date_from: date (optional) — начало кастомного диапазона (переопределяет period)
date_to: date (optional) — конец кастомного диапазона (переопределяет period)
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
          "detail": "12.00 ₽ → 18.00 ₽",
          "campaign_items": null
        },
        {
          "id": 1235,
          "event_type": "OZON_CAMPAIGN_CREATED",
          "category": "advertising",
          "label": "Новая кампания",
          "campaign_title": "тест 111",
          "detail": "Создана кампания · Активна · 8 товаров",
          "campaign_items": [
            {
              "offer_id": "АМ-КШ-СТЕР-КР-10",
              "nm_id": "2947854852",
              "name": "Amare Корм..."
            },
            { "offer_id": "", "nm_id": "2946710642", "name": "Amare Корм..." }
          ]
        }
      ]
    }
  ]
}
```

### Типы событий

| event_type              | category    | Описание                                          |
| ----------------------- | ----------- | ------------------------------------------------- |
| `OZON_BID_CHANGE`       | advertising | Изменение ставки                                  |
| `OZON_STATUS_CHANGE`    | advertising | Статус кампании изменён                           |
| `OZON_BUDGET_CHANGE`    | advertising | Бюджет кампании изменён                           |
| `OZON_ITEM_ADD`         | advertising | Товар добавлен в кампанию                         |
| `OZON_ITEM_REMOVE`      | advertising | Товар удалён из кампании                          |
| `OZON_CAMPAIGN_CREATED` | advertising | Новая кампания Ozon (с `campaign_items`)          |
| `OZON_SEO_CHANGE`       | content     | SEO-контент изменён                               |
| `OZON_PHOTO_CHANGE`     | content     | Фото изменено (`field`: `main_image` / `gallery`) |
| `OZON_CONTENT_CHANGE`   | content     | Контент (название) изменён                        |
| `OZON_PRICE_CHANGE`     | commercial  | Цена изменена                                     |
| `OZON_STOCK_OUT`        | commercial  | Товар закончился (остатки → 0)                    |
| `OZON_STOCK_REPLENISH`  | commercial  | Поступление на склад (0 → N)                      |
| `ITEM_INACTIVE`         | advertising | Товар деактивирован (WB)                          |
| `BID_CHANGE`            | advertising | Изменение ставки (WB)                             |
| `STATUS_CHANGE`         | advertising | Статус кампании изменён (WB)                      |
| `ITEM_ADD`              | advertising | Товар добавлен (WB)                               |
| `ITEM_REMOVE`           | advertising | Товар удалён (WB)                                 |
| `CAMPAIGN_CREATED`      | advertising | Новая кампания WB (с `campaign_items`)            |
| `CONTENT_CHANGE`        | content     | Контент изменён (WB)                              |
| `CONTENT_DESC_CHANGED`  | content     | Описание товара изменено (WB)                     |
| `PRICE_CHANGE`          | commercial  | Цена изменена (WB)                                |
| `STOCK_OUT`             | commercial  | Товар закончился (WB)                             |
| `STOCK_REPLENISH`       | commercial  | Поступление на склад (WB)                         |

### Обогащение данных

- **Товар:** JOIN `event_log.nm_id` → `dim_ozon_products` по `sku` ИЛИ `product_id` → name, offer_id, image_url
- **Товары кампании (CAMPAIGN_CREATED):** nm_ids из `metadata.items` тоже добавляются в product_map lookup → offer_id обогащается из БД
- **Campaign title:** 5-уровневый fallback:
  1. `event_metadata.campaign_title` (новые события)
  2. `event_metadata.title` только для STATUS_CHANGE/BUDGET_CHANGE
  3. STATUS_CHANGE из БД (того же advert_id)
  4. `campaign_title` из любых событий в БД
  5. Redis-кеш: Ozon → `get_ozon_campaign_state().title`, WB → `get_state().campaign_name`

### Changelog

#### 2026-03-06

- Добавлены типы: OZON_PRICE_CHANGE, OZON_STOCK_OUT, OZON_STOCK_REPLENISH, OZON_CONTENT_CHANGE
- Product lookup: поиск по `product_id` OR `sku` (ценовые события используют product_id)
- `_format_value`: OZON_PRICE_CHANGE форматируется как `X ₽`

#### 2026-03-07

- OZON_PHOTO_CHANGE: поддержка `field: gallery` (ранее только `images_order`) в detail-тексте
- `advert_id` больше не обязателен — контентные события (OZON_PHOTO_CHANGE и др.) корректно записываются с `advert_id = NULL`

#### 2026-03-09

- **`date_from`/`date_to`**: добавлены query parameters для кастомного диапазона дат (переопределяют `period`)
- **`campaign_items`**: новое поле в response — структурированный список товаров для `CAMPAIGN_CREATED`/`OZON_CAMPAIGN_CREATED` (`[{offer_id, nm_id, name}]`)
- **`_pluralize()`**: helper для русского склонения «товар/товара/товаров» в detail-строке
- **Product enrichment**: nm_ids из `CAMPAIGN_CREATED` metadata.items добавляются в `product_map` lookup → offer_id обогащается из `dim_products`/`dim_ozon_products`
- **WB Redis fallback**: `get_state().campaign_name` используется для резолва названий WB кампаний (ранее — только Ozon через `get_ozon_campaign_state().title`)
- **Emoji cleanup**: убран дублирующий 🚀 из лейблов CAMPAIGN_CREATED/OZON_CAMPAIGN_CREATED (иконка уже в EVENT_STYLE)

---

## ИИ-анализ событий — `/api/v1/events/analysis`

### Endpoint

| Метод  | Path               | Описание                                        | Auth   |
| ------ | ------------------ | ----------------------------------------------- | ------ |
| `POST` | `/events/analysis` | SSE-стрим ИИ-анализа событий (Gemini 2.5 Flash) | Bearer |

### Request Body

```json
{
  "shop_id": 18,
  "period": "30d",
  "group_by": "day"
}
```

### Механизм

1. **Каталог товаров** (PostgreSQL):
   - WB: `dim_products` → `nm_id`, `name`, `vendor_code`
   - Ozon: `dim_ozon_products` → `product_id`, `name`, `offer_id`

2. **События** (PostgreSQL `event_log`):
   - Все события за period → формат: `[дата] [категория] тип | товар: Название (артикул) | old → new`
   - Привязка к товарам через `nm_id` → lookup в каталоге

3. **KPI-метрики** (ClickHouse):
   - Заказы + выручка из `fact_ozon_orders` / `fact_orders_raw`
   - Реклама из `fact_ozon_ad_daily` / `fact_advert_stats_v3`
   - Группировка по bucket (день/неделя)

4. **Per-product funnel** (ClickHouse):
   - TOP-50 товаров по рекламному расходу
   - Ozon: `fact_ozon_ad_daily` → `sku`, views, clicks, add_to_cart, orders, money_spent
   - WB: `fact_advert_stats_v3` → `nm_id`, views, clicks, atbs, orders, spend
   - Расчёт: CTR%, CR→корзину%, CR→заказ%

5. **Промпт → Gemini 2.5 Flash** (через `api.kie.ai`):
   - System prompt с инструкциями анализа
   - User prompt: каталог + события + KPI таблица + per-product funnel таблица

6. **SSE streaming**: ответ стримится клиенту chunk-by-chunk

### Response (Server-Sent Events)

```
data: {"content": "## 🔍 Ключевые находки\n\n"}
data: {"content": "За период наблюдалось..."}
...
data: [DONE]
```

### Зависимости

- **API ключ**: `KIE_AI_API_KEY` в `.env`
- **Модель**: `gemini-2.5-flash` через `https://api.kie.ai/gemini-2.5-flash/v1/chat/completions`

### Changelog

#### 2026-03-09

- Создан endpoint `POST /events/analysis`
- Обогащение: каталог товаров (имена + артикулы), события привязаны к товарам, per-product funnel (CTR, CR, DRR)

### 2026-03-09 (Склады)

- **Новый роутер:** `warehouses_router` — `/api/v1/warehouses/*`
- **2 endpoints:** `GET /ozon/supply` (JSON) + `GET /ozon/supply/xlsx` (Excel 5 листов)
- **DELIVERY_HOURS** 25×25 — матрица нормативного времени доставки между кластерами Ozon
- **CONSOLIDATED_GROUPS** — 9 объединённых групп кластеров для минимизации точек поставки
- **`_resolve_hub()`** — определение приоритетного склада отгрузки для кластера спроса
- **Excel Sheet 1:** добавлены колонки «Склад отгрузки» + «Доставка, ч» с цветовой индикацией
- **Excel Sheet 5:** «Объединённые кластеры» — сводная поставка по 9 хабам

### 2026-03-10

- **Ozon comparison:** Добавлен ключ `operating` в `comparison.current` / `comparison.previous` — строка «Расходы МП (ОПЕКС)» ранее показывала 0₽
- **Ozon comparison:** Добавлены ключи `penalties`, `refunds` (ранее отсутствовали)
- **Формула:** `operating = services + bulk_charges (excl. marketing)`

### 2026-03-10 (WB Supply)

- **Новый endpoint:** `GET /warehouses/wb/supply/xlsx` — рекомендации поставок WB с учётом платного хранения
- **Параметры:** `shop_id`, `sales_period` (7-90, def 30), `target_days` (14-60, def 45), `safety` (1.0-2.0, def 1.15)
- **4 листа Excel:**
  1. **Рекомендации по складам** — SKU × склад: продажи, остатки, потребность, хранение руб/день и руб/мес, коэфф. приёмки
  2. **Сводка по товарам** — 1 строка на SKU: оборачиваемость, прогноз расходов хранения, текстовая рекомендация
  3. **Тарифы складов WB** — все 144 склада: коэффициенты хранения/логистики/приёмки, базовые и доп тарифы
  4. **Риск перезатаривания** — SKU с оборачиваемостью > 45 дней, оценка доп расходов на хранение
- **Источники данных:** `fact_inventory_snapshot`, `fact_orders_raw_latest`, `dim_products`, `fact_wb_acceptance_tariffs`
- **Ключевая логика:** хранение платное с 1-го дня, формула: `base_tariff × vol + add_tariff × (vol - 1)`
- **Исправление:** `product_name` → `name` в запросе к `dim_products`

### 2026-03-10 (v2)

- Добавлена секция «WB Supply» с JSON endpoint `GET /warehouses/wb/supply`
- Response schema: `kpi` + `items[]` (с `warehouses[]`) + `warehouse_summary[]`
- WB-специфика: хранение платное с 1-го дня, фиксация коэффициентов 60/90 дней
- Overstock = `turnover_days > target_days` (не > 60)
- Acceptance: «Без коэфф.» вместо «Бесплатно»
- 5 источников данных: CH (stocks, orders, tariffs), PG (products), Redis (images)

### 2026-03-10 (v3)

- Добавлены endpoints `GET /sales/ozon/abc-xyz/xlsx` и `GET /sales/wb/abc-xyz/xlsx`
- Excel: 3 листа (товары 19 колонок + матрица 3×3 + сводка), openpyxl
- Цветовое кодирование ABC/XYZ групп, условное форматирование маржи/прибыли
- RFC 5987 `filename*=UTF-8''` для кириллицы в Content-Disposition
- Функция `_build_abc_xyz_xlsx()` — единая для Ozon и WB

### 2026-03-12

- **Ozon Supply — per-warehouse stocks:**
  - Маппинг `WAREHOUSE_TO_CLUSTER` (34 РФЦ → кластеры) + обратный `CLUSTER_TO_WAREHOUSES_OZON`
  - `_compute_supply()` переписан: стоки из `fact_ozon_warehouse_stocks` по каждому складу
  - Формула: `need = max(0, daily×target×safety − реальный_сток_на_РФЦ)` (не пропорциональная оценка)
  - Response: новые поля `wh_stock`, `warehouse`, `warehouses[]` в clusters и hubs
  - Excel: «Сток РФЦ» + «Склады» во всех листах, обновлённая методология

### 2026-03-13

- **WB Supply — кросс-складской анализ:**
  - `WAREHOUSE_TO_OKRUG`: маппинг 50+ складов WB → 8 федеральных округов (вкл. `:Питание`, `СГТ`)
  - Новый SQL: `warehouse_name × nm_id × oblast_okrug_name` → `wh_consumption`
  - Response: новые поля `effective_days`, `cross_daily`, `cross_okrugs[]`, `product_type`
  - Пересчёт `status` по `effective_days` (ok→attention→critical)
  - Excel: колонки «Реал.зап, дн» + «Кросс» в листе Рекомендации
- **WB Supply — food/SGT paired warehouse fix:**
  - `need = 0` на обычных складах для food-товаров (поставка только на `:Питание`)
  - `paired_stock`: food-вариант учитывает stock парного обычного склада
  - `paired_daily`: food-вариант агрегирует продажи парного склада
