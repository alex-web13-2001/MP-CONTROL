# MP-CONTROL — Services Layer

> Полное описание всех 21 сервисов: API endpoints, трансформация данных, целевые таблицы.  
> Директория: `backend/app/services/`

---

## Архитектурный паттерн

Каждый сервис следует единой структуре:

```
Service (async, MarketplaceClient)    →    Loader (sync, ClickHouse/PostgreSQL)
├── __init__(db, shop_id, api_key)         ├── __init__(host, port, ...)
├── _make_client()                         ├── connect() / close()
├── fetch_*()   ← API calls               ├── insert_rows() / insert_batch()
└── __aenter__/__aexit__                   └── get_stats() / get_count()
```

Все HTTP-запросы идут через `MarketplaceClient` → proxy rotation, rate limiting, circuit breaker, JA3 spoofing.

---

## Wildberries Services (11 файлов)

### `wb_finance_loader.py` (463 строки)

| Компонент            | Описание                                                     |
| -------------------- | ------------------------------------------------------------ |
| **WBReportParser**   | Парсит JSON V5 API (`/api/v5/supplier/reportDetailByPeriod`) |
| **ClickHouseLoader** | Batch INSERT в `fact_finances` (BATCH_SIZE=1000)             |
| **FactFinancesRow**  | Dataclass — единая строка для fact_finances                  |

**API → fact_finances маппинг:**

| API поле                    | →   | DB поле                              |
| --------------------------- | --- | ------------------------------------ |
| `rr_dt` / `sale_dt`         | →   | `event_date`                         |
| `srid`                      | →   | `order_id`                           |
| `nm_id`                     | →   | `external_id`                        |
| `sa_name`                   | →   | `vendor_code`                        |
| `rrd_id`                    | →   | `rrd_id` (дедуп)                     |
| `supplier_oper_name`        | →   | `operation_type`                     |
| `retail_amount`             | →   | `retail_amount`                      |
| `ppvz_for_pay`              | →   | `payout_amount`, `wb_ppvz_for_pay`   |
| `delivery_rub`              | →   | `logistics_total`, `wb_delivery_rub` |
| `penalty`                   | →   | `penalty_total`                      |
| `storage_fee` + `deduction` | →   | `storage_fee`                        |
| `acceptance`                | →   | `acceptance_fee`                     |
| `bonus_type_name` → amount  | →   | `bonus_amount`                       |

---

### `wb_orders_service.py` (362 строки)

| Компонент           | Описание                                    |
| ------------------- | ------------------------------------------- |
| **WBOrdersService** | Fetch заказов через MarketplaceClient       |
| **OrdersLoader**    | INSERT в `fact_orders_raw` (BATCH_SIZE=500) |

**API endpoint:** `GET /api/v1/supplier/orders?dateFrom=...&flag=0`  
**Domain:** `statistics-api.wildberries.ru`  
**Пагинация:** `lastChangeDate` из последней строки → `dateFrom` для следующей страницы  
**Rate limit:** 63 сек между запросами (1 req/мин + margin)

---

### `wb_sales_funnel_service.py` (556 строк)

| Компонент                | Описание                                        |
| ------------------------ | ----------------------------------------------- |
| **WBSalesFunnelService** | 3 стратегии: History API, CSV Report, Aggregate |
| **SalesFunnelLoader**    | INSERT в `fact_sales_funnel` (BATCH_SIZE=500)   |

**API endpoints:**

- `POST /api/analytics/v3/sales-funnel/products` — агрегированные данные (до 365 дней)
- `POST /api/analytics/v3/sales-funnel/products/history` — ежедневная история (max 7 дней, 20 nm_ids)
- `POST /api/analytics/v3/sales-funnel/products/csv-report` — создание CSV отчёта (async)

**Ограничения:** max 20 nm_ids/запрос, max 7 дней/запрос, 3 req/мин  
**Автоматическое разбиение:** chunks of 20 nm_ids × 7-day windows

---

### `wb_advertising_loader.py` (17 КБ)

Загружает данные рекламной статистики из V3 API в ClickHouse.

**Таблицы назначения:**

- `ads_raw_history` (MergeTree, append)
- `fact_advert_stats_v3` (ReplacingMergeTree)

---

### `wb_advertising_report_service.py` (8.8 КБ)

Выгрузка рекламных отчётов для аналитики. Формирование отчётов по кампаниям.

---

### `wb_content_service.py` (12.9 КБ)

Мониторинг контента карточек WB.

**API endpoint:** `POST /content/v2/get/cards/list` (Content API)

**Пайплайн:**

1. Fetch карточек → titles, descriptions, photos, dimensions
2. MD5 хеши: `title_hash`, `description_hash`, `photos_hash`, `main_photo_id`
3. Compare с `dim_product_content` → детектирование изменений
4. Upsert → `dim_product_content`, `dim_products`
5. Redis state update: `image_url`, `content_hash`

---

### `wb_prices_service.py` (6.9 КБ)

Синхронизация цен товаров WB.

**API endpoint:** `GET /public/api/v1/info` (Discounts & Prices API)

**Выход:**

- Redis: `state:price:{shop_id}:{nm_id}`
- PostgreSQL: `dim_products.current_price`

---

### `wb_stocks_service.py` (8.3 КБ)

Синхронизация остатков по складам WB.

**API endpoint:** `POST /api/v3/stocks/{warehouseId}` (Marketplace API)

**Выход:**

- Redis: `state:stock:{shop_id}:{nm_id}:{warehouse}`
- ClickHouse: `fact_inventory_snapshot`

---

### `wb_warehouses_service.py` (3.5 КБ)

Справочник складов WB.

**API endpoint:** `GET /api/v3/offices` (Marketplace API)

**Выход:** PostgreSQL `dim_warehouses` (UPSERT by warehouse_id)

---

### `wb_finance_report_service.py` (4.2 КБ)

API для фронтенда — запрос финансовых данных из ClickHouse `fact_finances`.

---

### `event_detector.py` (953 строки)

| Класс                       | Описание                                                               |
| --------------------------- | ---------------------------------------------------------------------- |
| **EventDetector**           | Рекламные события: BID_CHANGE, STATUS_CHANGE, ITEM_ADD/REMOVE/INACTIVE |
| **CommercialEventDetector** | Коммерческие: PRICE_CHANGE, STOCK_OUT, STOCK_REPLENISH                 |

**Debouncing:** предотвращает мусорные события от "штормов" API.  
**V1/V2 API поддержка:** `detect_changes()` (legacy V1) + `detect_changes_v2()` (текущий).

---

## Ozon Services (10 файлов)

### `ozon_products_service.py` (1220 строк — самый большой сервис)

| Компонент                  | Описание                                    |
| -------------------------- | ------------------------------------------- |
| **OzonProductsService**    | Каталог товаров через Seller API            |
| **upsert_ozon_products()** | UPSERT → `dim_ozon_products` (PostgreSQL)   |
| **upsert_ozon_content()**  | UPSERT → `dim_ozon_product_content`         |
| **OzonInventoryLoader**    | INSERT → `fact_ozon_inventory` (ClickHouse) |

**API endpoints:**

- `POST /v3/product/list` — все product_ids (пагинация через last_id)
- `POST /v3/product/info/list` — детальная инфа (batch 100)
- `POST /v1/product/info/description` — описания (sequential)
- `POST /v1/product/rating-by-sku` — рейтинг контента (batch 100)

**Дополнительные функции:**

- Извлечение комиссий: `_extract_commissions()` — sales_percent, FBO/FBS logistics
- MD5 хеши контента для change detection
- Извлечение FBO/FBS стоков: `_extract_stocks()`

---

### `ozon_orders_service.py` (510 строк)

| Компонент             | Описание                                 |
| --------------------- | ---------------------------------------- |
| **OzonOrdersService** | FBO + FBS заказы через Seller API        |
| **OzonOrdersLoader**  | INSERT → `fact_ozon_orders` (ClickHouse) |

**API endpoints:**

- `POST /v2/posting/fbo/list` — FBO заказы (без лимита по периоду)
- `POST /v3/posting/fbs/list` — FBS заказы (max ~30 дней, chunking по 28 дней)

**Нормализация:** postings → 1 строка per product per posting. Поля: posting_number, sku, offer_id, name, qty, price, commission, city, region, warehouse_name.

---

### `ozon_finance_service.py` (495 строк)

| Компонент                  | Описание                                       |
| -------------------------- | ---------------------------------------------- |
| **OzonFinanceService**     | Финансовые транзакции P&L                      |
| **OzonTransactionsLoader** | INSERT → `fact_ozon_transactions` (ClickHouse) |

**API endpoint:** `POST /v3/finance/transaction/list`  
**Ограничение:** max 1 месяц per запрос → автоматическое разбиение по calendar months

**Категоризация операций (20+):**

| Категория    | Примеры операций                                |
| ------------ | ----------------------------------------------- |
| Revenue      | `OperationAgentDeliveredToCustomer`             |
| Refund       | `OperationItemReturn`, `ClientReturnAgent`      |
| Commission   | `OperationMarketplaceServiceItemFee`            |
| Logistics    | `OperationMarketplaceServiceDelivery*`          |
| Advertising  | `OperationMarketplaceWithHoldingForPromo*`      |
| Storage      | `OperationMarketplaceServiceStorage`            |
| Penalty      | `DefectRateDetailed`, `DefectRateCancellation`  |
| Acquiring    | `MarketplaceRedistributionOfAcquiringOperation` |
| Compensation | `AccrualInternalClaim`                          |

---

### `ozon_ads_service.py` (770 строк)

| Компонент          | Описание                                        |
| ------------------ | ----------------------------------------------- |
| **OzonAdsService** | Performance API (OAuth2, отдельная авторизация) |

**Аутентификация:** OAuth2 client_credentials → token cached in Redis.

**API endpoints:**

- `GET /api/client/campaign` — список кампаний
- `GET /api/client/campaign/{id}/v2/products` — товары с текущими ставками
- `GET /api/client/campaign/{id}/products/bids/competitive` — рыночные ставки
- `POST /api/client/statistics` — заказ CSV отчёта (async)
- `GET /api/client/statistics/{UUID}` — статус отчёта
- `GET /api/client/statistics/report?UUID=...` — скачивание CSV/ZIP

**CSV парсинг:** BOM-prefixed, semicolon-separated, campaign IDs в заголовках.
**Retry:** 5 попыток, 300 сек пауза (strict Ozon Performance limits).

---

### `ozon_ads_event_detector.py` (9.3 КБ)

Аналог `EventDetector` для Ozon. Детектирует: `OZON_BID_CHANGE`, `OZON_STATUS_CHANGE`, `OZON_ITEM_ADD`, `OZON_ITEM_REMOVE`.

---

### `ozon_funnel_service.py` (8.9 КБ)

Воронка продаж Ozon.

**API endpoint:** `POST /v1/analytics/data` — 14 метрик per SKU per day.

---

### `ozon_returns_service.py` (10.2 КБ)

FBO + FBS возвраты Ozon.

**API endpoints:**

- `POST /v3/returns/company/fbo` — FBO возвраты
- `POST /v1/returns/company/fbs` — FBS возвраты

---

### `ozon_warehouse_stocks_service.py` (7.5 КБ)

Остатки FBO + FBS по складам.

**API endpoints:**

- `POST /v2/analytics/stock_on_warehouses` — FBO
- `POST /v1/report/stock/v2` — FBS

---

### `ozon_price_service.py` (7.7 КБ)

Цены и комиссии.

**API endpoint:** `POST /v5/product/info/prices` (пагинация через last_id)

---

### `ozon_seller_rating_service.py` (4.9 КБ)

Рейтинг продавца на Ozon.

**API endpoint:** `POST /v1/rating/summary`

---

## Сводная таблица: Service → API → Storage

| Service                         | Marketplace | API Domain      | Target Storage                                     |
| ------------------------------- | ----------- | --------------- | -------------------------------------------------- |
| `wb_finance_loader`             | WB          | Supplier API v5 | CH: `fact_finances`                                |
| `wb_orders_service`             | WB          | Statistics API  | CH: `fact_orders_raw`                              |
| `wb_sales_funnel_service`       | WB          | Analytics API   | CH: `fact_sales_funnel`                            |
| `wb_advertising_loader`         | WB          | Advert API v3   | CH: `ads_raw_history`, `fact_advert_stats_v3`      |
| `wb_content_service`            | WB          | Content API     | PG: `dim_products`, `dim_product_content`          |
| `wb_prices_service`             | WB          | Prices API      | PG: `dim_products`, Redis                          |
| `wb_stocks_service`             | WB          | Marketplace API | CH: `fact_inventory_snapshot`, Redis               |
| `wb_warehouses_service`         | WB          | Marketplace API | PG: `dim_warehouses`                               |
| `wb_finance_report_service`     | WB          | — (internal)    | ClickHouse queries                                 |
| `event_detector`                | WB          | — (stateful)    | PG: `event_log`, Redis                             |
| `ozon_products_service`         | Ozon        | Seller API      | PG: `dim_ozon_products`, CH: `fact_ozon_inventory` |
| `ozon_orders_service`           | Ozon        | Seller API      | CH: `fact_ozon_orders`                             |
| `ozon_finance_service`          | Ozon        | Seller API      | CH: `fact_ozon_transactions`                       |
| `ozon_ads_service`              | Ozon        | Performance API | CH: `fact_ozon_ad_daily`, `log_ozon_bids`          |
| `ozon_ads_event_detector`       | Ozon        | — (stateful)    | PG: `event_log`, Redis                             |
| `ozon_funnel_service`           | Ozon        | Seller API      | CH: funnel tables                                  |
| `ozon_returns_service`          | Ozon        | Seller API      | CH: returns tables                                 |
| `ozon_warehouse_stocks_service` | Ozon        | Seller API      | CH: stock tables                                   |
| `ozon_price_service`            | Ozon        | Seller API      | CH: price snapshots                                |
| `ozon_seller_rating_service`    | Ozon        | Seller API      | CH: rating snapshots                               |
