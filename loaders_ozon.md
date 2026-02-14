# Ozon — Подробное описание загрузчиков

> Все модули расположены в `backend/app/services/ozon_*.py`

---

## 1. Ozon Products (`ozon_products_service.py`, 1220 строк)

### Назначение

Монолитный модуль — каталог товаров, инвентарь, комиссии, контент, рейтинг контента. Один файл содержит 5 логических подмодулей.

### API Endpoints

| Endpoint                       | Метод | Назначение                                           |
| ------------------------------ | ----- | ---------------------------------------------------- |
| `/v3/product/list`             | POST  | Список product_id + offer_id (paginated via last_id) |
| `/v3/product/info/list`        | POST  | Детальная информация (цены, стоки, комиссии)         |
| `/v1/product/info/description` | POST  | Описание товара (HTML)                               |
| `/v1/product/rating-by-sku`    | POST  | Content rating (0-100) + group breakdown             |

**Base URL:** `api-seller.ozon.ru`

### Константы

```
PAGE_SIZE = 100         # max items per /v3/product/list
INFO_BATCH_SIZE = 100   # max product_ids per /v3/product/info/list
CH_BATCH_SIZE = 500     # ClickHouse insert batch
```

### Подмодуль 1: sync_ozon_products

**Pipeline:** list → info → upsert `dim_ozon_products` (PostgreSQL)

### Подмодуль 2: sync_ozon_content

**Pipeline:** info + description → MD5 хэши → `dim_ozon_product_content` (PostgreSQL)

### Подмодуль 3: OzonInventoryLoader (строки 626-728)

**Pipeline:** info → prices + stocks → `fact_ozon_inventory` (ClickHouse)

| Метод                                 | Описание                                                    |
| ------------------------------------- | ----------------------------------------------------------- |
| `insert_inventory(shop_id, products)` | Extract prices/stocks из product info, batch INSERT         |
| `get_stats(shop_id)`                  | Агрегированная статистика (avg_price, total_fbo, total_fbs) |

**Поля → CH колонки:**

| Источник                      | CH колонка        | Тип           |
| ----------------------------- | ----------------- | ------------- |
| `item["id"]`                  | `product_id`      | UInt64        |
| `item["offer_id"]`            | `offer_id`        | String        |
| `item["price"]`               | `price`           | Decimal(18,2) |
| `item["old_price"]`           | `old_price`       | Decimal(18,2) |
| `item["min_price"]`           | `min_price`       | Decimal(18,2) |
| `item["marketing_price"]`     | `marketing_price` | Decimal(18,2) |
| `_extract_stocks(item)` → fbo | `stocks_fbo`      | UInt32        |
| `_extract_stocks(item)` → fbs | `stocks_fbs`      | UInt32        |

**CH таблица:** `fact_ozon_inventory` — **ReplacingMergeTree(fetched_at)**, ORDER BY: `(shop_id, product_id)`, TTL: 1y

### Подмодуль 4: OzonCommissionsLoader (строки 730-870)

**Pipeline:** info → commissions → `fact_ozon_commissions` (ClickHouse)

**CH колонки:** `dt`, `updated_at`, `shop_id`, `product_id`, `offer_id`, `sku`, `sales_percent`, `fbo_fulfillment_amount`, `fbo_direct_flow_trans_min/max`, `fbo_deliv_to_customer`, `fbo_return_flow`, `fbs_direct_flow_trans_min/max`, `fbs_deliv_to_customer`, `fbs_first_mile_min/max`, `fbs_return_flow`

### Подмодуль 5: sync_ozon_content_rating

**Pipeline:** SKUs → `/v1/product/rating-by-sku` → `fact_ozon_content_rating` (ClickHouse)

### Celery Tasks

- `sync_ozon_products` — каталог → PG
- `sync_ozon_product_snapshots` — 4-in-1: promotions + availability + commissions + inventory
- `sync_ozon_inventory` — snapshot цен+стоков
- `sync_ozon_commissions` — snapshot комиссий (daily 06:00)
- `sync_ozon_content_rating` — snapshot рейтинга (daily 06:30)
- `sync_ozon_content` — MD5 хэши + event detection

---

## 2. Ozon Orders (`ozon_orders_service.py`, ~510 строк)

### Назначение

FBO и FBS заказы с нормализацией до 1 строки на продукт.

### API

| Endpoint               | Метод | Назначение                        |
| ---------------------- | ----- | --------------------------------- |
| `/v2/posting/fbo/list` | POST  | FBO постинги (со склада Ozon)     |
| `/v3/posting/fbs/list` | POST  | FBS постинги (со склада продавца) |

**Пагинация:** Offset-based, `limit=1000`, chunked по месяцам.
**Rate Limit:** 0.5s между страницами.

### Нормализация

`_normalize_postings()` — flattens 1 posting с N products → N строк.

### Поля → `fact_ozon_orders`

| Источник                                      | CH колонка                      |
| --------------------------------------------- | ------------------------------- |
| `posting["posting_number"]`                   | `posting_number` (ORDER BY key) |
| `posting["order_id"]`                         | `order_id`                      |
| `posting["status"]`                           | `status`                        |
| `posting["in_process_at"]`                    | `order_date`                    |
| `product["sku"]`                              | `sku`                           |
| `product["offer_id"]`                         | `offer_id`                      |
| `product["name"]`                             | `product_name`                  |
| `product["quantity"]`                         | `quantity`                      |
| `product["price"]`                            | `price`                         |
| `posting["analytics_data"]["region"]`         | `region`                        |
| `posting["analytics_data"]["city"]`           | `city`                          |
| `posting["analytics_data"]["warehouse_name"]` | `warehouse_name`                |

**CH таблица:** `fact_ozon_orders` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, posting_number, sku)`

### Celery Tasks

- `sync_ozon_orders` (tasks.py:1663) — 14-day window, overlap для status changes
- `backfill_ozon_orders` (tasks.py:1744) — 365 дней, time_limit: 3600s

---

## 3. Ozon Finance (`ozon_finance_service.py`, ~495 строк)

### Назначение

Финансовые транзакции: продажи, комиссии, возвраты, штрафы, доставка.

### API

| Endpoint                       | Метод |
| ------------------------------ | ----- |
| `/v3/finance/transaction/list` | POST  |

**Пагинация:** Offset-based, `limit=1000`
**Ограничение API:** Max 1 месяц на запрос → chunked по месяцам
**Rate Limit:** 1.5s между страницами

### Нормализация

`_normalize_transaction()` — flattens transaction + items + services.

### Поля → `fact_ozon_transactions`

| Источник                           | CH колонка                    |
| ---------------------------------- | ----------------------------- |
| `txn["operation_id"]`              | `operation_id` (ORDER BY key) |
| `txn["operation_type"]`            | `operation_type`              |
| `txn["operation_date"]`            | `operation_date`              |
| `txn["posting"]["posting_number"]` | `posting_number`              |
| `txn["amount"]`                    | `amount`                      |
| `item["sku"]`                      | `sku`                         |
| `item["name"]`                     | `product_name`                |
| `service["name"]`                  | → маппинг в категории         |

**CH таблица:** `fact_ozon_transactions` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, operation_id)`

### Celery Tasks

- `sync_ozon_finance` (tasks.py:1822) — 2-day window
- `backfill_ozon_finance` (tasks.py:1903) — by calendar months, 12 месяцев

---

## 4. Ozon Funnel (`ozon_funnel_service.py`, ~298 строк)

### Назначение

Воронка аналитики: просмотры → корзина → заказы.

### API

| Endpoint             | Метод |
| -------------------- | ----- |
| `/v1/analytics/data` | POST  |

**Пагинация:** `limit=1000`, `offset`
**Rate Limit:** 0.5s
**Chunking:** Max 90 дней на запрос

> ⚠️ **ВАЖНО:** Из 15 метрик API рабочие только 2: `ordered_units` и `revenue`. Остальные deprecated — возвращают 0.

### Метрики

| Метрика                                   | Статус              |
| ----------------------------------------- | ------------------- |
| `ordered_units`                           | ✅ Работает         |
| `revenue`                                 | ✅ Работает         |
| `hits_view_search`, `hits_view_pdp`, etc. | ❌ Deprecated (→ 0) |

### Поля → `fact_ozon_funnel`

`sku`, `dt`, `ordered_units`, `revenue` + 12 deprecated полей

**CH таблица:** `fact_ozon_funnel` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, sku, dt)`

### Celery Tasks

- `sync_ozon_funnel` (tasks.py:1984) — yesterday
- `backfill_ozon_funnel` (tasks.py:2039) — 365d, chunks by 90d

---

## 5. Ozon Returns (`ozon_returns_service.py`, 311 строк)

### Назначение

Возвраты и отмены — детальная информация с причинами.

### API

| Endpoint           | Метод |
| ------------------ | ----- |
| `/v1/returns/list` | POST  |

**Пагинация:** Cursor-based (last_id)
**Rate Limit:** 0.5s, `API_LIMIT=500`, `MAX_PAGES=200`

> ⚠️ **BUG (2026-02-15):** API всегда возвращает `last_id=0` вместо правильного cursor.
> **Workaround:** Используем `max(id)` из текущей страницы как cursor.
> Дедупликация по `id` в коде для предотвращения дублей.

### Классы

**`OzonReturnsService`**
| Метод | Описание |
|-------|----------|
| `fetch_returns(time_from, time_to)` | Cursor pagination с workaround |

**`OzonReturnsLoader`**
| Метод | Описание |
|-------|----------|
| `insert_rows(shop_id, rows)` | Batch INSERT |
| `get_stats(shop_id)` | Статистика |

**Нормализация:** `normalize_returns()` — flat dict per return.

### Поля → `fact_ozon_returns`

`return_id`, `return_date`, `posting_number`, `return_type`, `return_schema`, `return_reason`, `sku`, `offer_id`, `product_name`, `quantity`, `price`, `place_name`, `target_place`, `compensation_status`, `accepted_at`, `returned_at`

**CH таблица:** `fact_ozon_returns` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, return_id)`

### Celery Tasks

- `sync_ozon_returns` (tasks.py:2098) — last 30 days
- `backfill_ozon_returns` (tasks.py:2154) — 180 days

---

## 6. Ozon Warehouse Stocks (`ozon_warehouse_stocks_service.py`, 232 строки)

### Назначение

Складские остатки per SKU × warehouse: free_to_sell, promised, reserved.

### API — 2 endpoint'а

| Endpoint                            | Метод | Данные                         |
| ----------------------------------- | ----- | ------------------------------ |
| `/v2/analytics/stock_on_warehouses` | POST  | FBO-focused, offset pagination |
| `/v4/product/info/stocks`           | POST  | FBO+FBS, cursor (last_id)      |

**Rate Limit:** 0.5s, `API_LIMIT=500`

### Методы Service

| Метод                      | API                                 | Особенность                     |
| -------------------------- | ----------------------------------- | ------------------------------- |
| `fetch_warehouse_stocks()` | `/v2/analytics/stock_on_warehouses` | warehouse_type hardcoded "fbo"  |
| `fetch_product_stocks()`   | `/v4/product/info/stocks`           | Dynamic warehouse_type from API |

### Поля → `fact_ozon_warehouse_stocks`

| CH колонка       | Тип              |
| ---------------- | ---------------- |
| `dt`             | Date             |
| `shop_id`        | UInt32           |
| `sku`            | UInt64           |
| `product_name`   | String           |
| `offer_id`       | String           |
| `warehouse_name` | String           |
| `warehouse_type` | String (fbo/fbs) |
| `free_to_sell`   | UInt32           |
| `promised`       | UInt32           |
| `reserved`       | UInt32           |
| `updated_at`     | DateTime         |

**CH таблица:** `fact_ozon_warehouse_stocks` — **ReplacingMergeTree(fetched_at)**, ORDER BY: `(shop_id, sku, warehouse_name)`

### Celery Task

`sync_ozon_warehouse_stocks` (tasks.py:2212) — twice daily

---

## 7. Ozon Price Tracker (`ozon_price_service.py`, 230 строк)

### Назначение

Ежедневные snapshot цен и комиссий для отслеживания ценовой динамики.

### API

| Endpoint                  | Метод |
| ------------------------- | ----- |
| `/v5/product/info/prices` | POST  |

**Пагинация:** Cursor (last_id), `API_LIMIT=1000`
**Rate Limit:** 0.5s

**Особенность v5:** `sku` = None в ответе → используем `product_id` как SKU.

### Поля → `fact_ozon_prices`

| CH колонка               | Источник                                     |
| ------------------------ | -------------------------------------------- |
| `sku`                    | product_id (v5 fallback)                     |
| `product_id`             | item.product_id                              |
| `offer_id`               | item.offer_id                                |
| `price`                  | price.price                                  |
| `old_price`              | price.old_price                              |
| `min_price`              | price.min_price                              |
| `marketing_price`        | price.marketing_seller_price                 |
| `sales_percent`          | commissions.sales_percent_fbo                |
| `fbo_commission_percent` | commissions.sales_percent_fbo                |
| `fbs_commission_percent` | commissions.sales_percent_fbs                |
| `fbo_commission_value`   | commissions.fbo_direct_flow_trans_min_amount |
| `fbs_commission_value`   | commissions.fbs_direct_flow_trans_min_amount |
| `acquiring_percent`      | acquiring                                    |

**CH таблица:** `fact_ozon_prices` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, sku)`

### Celery Task

`sync_ozon_prices` (tasks.py:2260) — daily/twice daily

---

## 8. Ozon Seller Rating (`ozon_seller_rating_service.py`, 157 строк)

### Назначение

Здоровье аккаунта — рейтинг продавца по группам метрик.

### API

| Endpoint             | Метод |
| -------------------- | ----- |
| `/v1/rating/summary` | POST  |

**Особенность:** Один запрос = все метрики. Нет пагинации.
**`status` field:** Может быть string или dict → обрабатывается `status.get("key", "")`.

### Поля → `fact_ozon_seller_rating`

| CH колонка      | Тип                                  |
| --------------- | ------------------------------------ |
| `dt`            | Date                                 |
| `shop_id`       | UInt32                               |
| `group_name`    | String (e.g. "logistics", "content") |
| `rating_name`   | String (e.g. "on_time_delivery")     |
| `rating_value`  | Float64                              |
| `rating_status` | String (e.g. "at_risk", "good")      |
| `penalty_score` | Float64                              |
| `updated_at`    | DateTime                             |

**CH таблица:** `fact_ozon_seller_rating` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, rating_name)`

### Celery Task

`sync_ozon_seller_rating` (tasks.py:2306) — daily

---

## 9. Ozon Ads (`ozon_ads_service.py`)

### Назначение

Рекламная статистика Ozon Performance API: кампании, ставки, показы, клики, расходы.

### API

| Endpoint                                | Метод | Base             |
| --------------------------------------- | ----- | ---------------- | ------------------- |
| `/api/client/token`                     | POST  | ozon_performance | OAuth2 token        |
| `GET /api/client/campaign`              | GET   | ozon_performance | Список кампаний     |
| `/api/client/campaign/{id}/v2/products` | GET   | ozon_performance | Продукты кампании   |
| `/api/client/statistics/json`           | POST  | ozon_performance | Статистика кампании |

**Аутентификация:** OAuth2 client_credentials → Bearer token (cached в Redis, TTL = expires_in).

### ClickHouse таблицы

| Таблица              | Engine                         | Назначение             |
| -------------------- | ------------------------------ | ---------------------- |
| `log_ozon_bids`      | MergeTree                      | Append-only лог ставок |
| `fact_ozon_ad_daily` | ReplacingMergeTree(updated_at) | Дневная статистика     |

### Celery Tasks

- `monitor_ozon_bids` (tasks.py:2733) — 15 мин, FAST queue, event detection
- `sync_ozon_ad_stats` (tasks.py:2930) — 60 мин, HEAVY queue, 3-day sliding window
- `backfill_ozon_ads` (tasks.py:3044) — 180 дней, week-by-week chunks

### Event Detection

`OzonAdsEventDetector` (266 строк):

- `detect_campaign_changes()` → `OZON_STATUS_CHANGE`, `OZON_BUDGET_CHANGE`
- `detect_product_changes()` → `OZON_BID_CHANGE`, `OZON_ITEM_ADD`, `OZON_ITEM_REMOVE`
- Bids в microroubles: `1 RUB = 1,000,000 micro`

---

## Сводная таблица всех Ozon модулей

| Модуль    | Файл                             | Строк | API         | CH таблица                                                           | Engine   |
| --------- | -------------------------------- | ----- | ----------- | -------------------------------------------------------------------- | -------- |
| Products  | ozon_products_service.py         | 1220  | 4 endpoints | fact_ozon_inventory, fact_ozon_commissions, fact_ozon_content_rating | RMT      |
| Orders    | ozon_orders_service.py           | 510   | 2 endpoints | fact_ozon_orders                                                     | RMT      |
| Finance   | ozon_finance_service.py          | 495   | 1 endpoint  | fact_ozon_transactions                                               | RMT      |
| Funnel    | ozon_funnel_service.py           | 298   | 1 endpoint  | fact_ozon_funnel                                                     | RMT      |
| Returns   | ozon_returns_service.py          | 311   | 1 endpoint  | fact_ozon_returns                                                    | RMT      |
| WH Stocks | ozon_warehouse_stocks_service.py | 232   | 2 endpoints | fact_ozon_warehouse_stocks                                           | RMT      |
| Prices    | ozon_price_service.py            | 230   | 1 endpoint  | fact_ozon_prices                                                     | RMT      |
| Rating    | ozon_seller_rating_service.py    | 157   | 1 endpoint  | fact_ozon_seller_rating                                              | RMT      |
| Ads       | ozon_ads_service.py              | ~400  | 4 endpoints | log_ozon_bids, fact_ozon_ad_daily                                    | MT + RMT |

**RMT** = ReplacingMergeTree, **MT** = MergeTree
