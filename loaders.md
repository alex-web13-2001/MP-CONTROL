# Документация загрузчиков данных — MP-CONTROL

> Версия: 2026-02-15 | System Architect Audit
> Подробные описания модулей: [loaders_wb.md](loaders_wb.md) | [loaders_ozon.md](loaders_ozon.md)

---

## 1. Архитектура

```
Marketplace API (WB / Ozon)
      │
      ▼
┌─────────────────────────┐
│   MarketplaceClient      │ ← curl_cffi + JA3 fingerprint (Chrome 120)
│   Proxy rotation         │ ← Sticky sessions per shop_id
│   Rate limiting (Redis)  │ ← Shared across Celery workers
│   Circuit breaker        │ ← Auto-disable on 3× auth errors
└──────────┬──────────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
  Service      Service        ← Fetch API + Normalize data
  (WB/Ozon)    (WB/Ozon)
     │           │
     ▼           ▼
  Loader       Loader         ← Batch INSERT (500-1000 rows)
     │           │
     ▼           ▼
┌──────────┐ ┌──────────┐
│ClickHouse│ │PostgreSQL│    ← Fact tables / Dim tables
│(analytics)│ │(metadata)│
└──────────┘ └──────────┘
     │           │
     ▼           ▼
   Redis ◄──────┘            ← State for Event Detection
     │
     ▼
  EventDetector → event_log (PostgreSQL)
```

### Типы загрузки

| Тип             | Описание                                  | Примеры модулей                 |
| --------------- | ----------------------------------------- | ------------------------------- |
| **Incremental** | Загрузка за период (since→to), дозагрузка | Orders, Finance, Returns        |
| **Snapshot**    | Полный срез на текущий момент             | Stocks, Prices, Rating, Content |
| **Append-only** | Лог событий, каждая запись уникальна      | Bids log, Ads raw history       |
| **Backfill**    | Историческая загрузка за N дней           | Funnel 365d, Orders 90d         |

### Стратегии дедупликации ClickHouse

| Engine                    | Поведение                            | Применение                                |
| ------------------------- | ------------------------------------ | ----------------------------------------- |
| `ReplacingMergeTree(ver)` | Хранит строку с max(ver) по ORDER BY | Основные fact-таблицы                     |
| `MergeTree`               | Append-only, без дедупликации        | Логи (`ads_raw_history`, `log_ozon_bids`) |
| `SummingMergeTree`        | Суммирует числовые колонки           | `sales_hourly_mv`                         |

---

## 2. Core-инфраструктура

### 2.1 MarketplaceClient (`marketplace_client.py`, 435 строк)

Единый HTTP-клиент для всех API запросов.

| Свойство        | Описание                                             |
| --------------- | ---------------------------------------------------- |
| HTTP Engine     | `curl_cffi` с JA3 fingerprint spoofing (Chrome 120)  |
| Proxy           | Sticky sessions — один прокси на `shop_id` на сессию |
| Rate Limit      | Redis-synced, shared across workers                  |
| Circuit Breaker | Auto-disable при 3+ auth errors (401/403)            |
| Retries         | Exponential backoff, max 3                           |
| Auth WB         | `Authorization: {api_key}`                           |
| Auth Ozon       | `Api-Key: {api_key}` + `Client-Id: {client_id}`      |

**Base URLs (10 доменов):**

| Ключ                      | URL                                           |
| ------------------------- | --------------------------------------------- |
| `wildberries`             | `https://supplier-api.wildberries.ru`         |
| `wildberries_stats`       | `https://statistics-api.wildberries.ru`       |
| `wildberries_adv`         | `https://advert-api.wildberries.ru`           |
| `wildberries_prices`      | `https://discounts-prices-api.wildberries.ru` |
| `wildberries_content`     | `https://content-api.wildberries.ru`          |
| `wildberries_common`      | `https://common-api.wildberries.ru`           |
| `wildberries_marketplace` | `https://marketplace-api.wildberries.ru`      |
| `wildberries_analytics`   | `https://seller-analytics-api.wildberries.ru` |
| `ozon`                    | `https://api-seller.ozon.ru`                  |
| `ozon_performance`        | `https://api-performance.ozon.ru`             |

### 2.2 Остальные core-модули

| Файл                       | Назначение                                              |
| -------------------------- | ------------------------------------------------------- |
| `proxy_provider.py`        | Пул прокси, sticky sessions, rotation                   |
| `rate_limiter.py`          | Redis-based rate limiter, shared across Celery workers  |
| `circuit_breaker.py`       | Авто-блокировка магазинов с невалидными API ключами     |
| `redis_state.py`           | State manager для event detection (цены, стоки, ставки) |
| `ozon_performance_auth.py` | OAuth2 client_credentials для Ozon Performance API      |
| `clickhouse.py`            | Фабрика подключений к ClickHouse                        |
| `database.py`              | Async SQLAlchemy engine для PostgreSQL                  |
| `encryption.py`            | Шифрование API ключей at rest                           |

---

## 3. Celery — Оркестрация

### 3.1 Очереди (`celery.py`)

| Очередь   | Назначение                       | Concurrency | Примеры                                           |
| --------- | -------------------------------- | ----------- | ------------------------------------------------- |
| `fast`    | Time-critical (autobidder, bids) | 4 workers   | `update_all_bids`, `monitor_ozon_bids`            |
| `heavy`   | Long-running (sync, backfill)    | 2 workers   | `sync_wb_finance_history`, `backfill_ozon_orders` |
| `default` | General purpose                  | 4 workers   | `example_task`, `send_notification`               |

**Конфигурация:**

- `task_time_limit` = 14400s (4 часа max, heavy)
- `task_soft_time_limit` = 14100s (3ч 55мин)
- `task_acks_late` = True (acknowledge после завершения)
- `task_reject_on_worker_lost` = True

### 3.2 Beat Schedule (активные)

| Задача                | Интервал | Очередь | Priority |
| --------------------- | -------- | ------- | -------- |
| `update_all_bids`     | 60 сек   | fast    | 9        |
| `check_all_positions` | 300 сек  | fast    | 7        |

> ⚠️ Остальные 12 задач закомментированы — требуют shop_id/api_key. Активируются координатором при деплое.

### 3.3 Закомментированные Beat-задачи

| Задача                     | Интервал         | Очередь |
| -------------------------- | ---------------- | ------- |
| `sync_marketplace_data`    | crontab(3:00)    | heavy   |
| `sync_commercial_data`     | 30 мин           | heavy   |
| `sync_warehouses`          | crontab(4:00)    | heavy   |
| `sync_product_content`     | crontab(4:30)    | heavy   |
| `sync_sales_funnel`        | 30 мин           | heavy   |
| `sync_orders`              | 10 мин           | heavy   |
| `monitor_ozon_bids`        | 15 мин           | fast    |
| `sync_ozon_ad_stats`       | 60 мин           | heavy   |
| `sync_ozon_commissions`    | crontab(6:00)    | heavy   |
| `sync_ozon_content_rating` | crontab(6:30)    | heavy   |
| `sync_ozon_inventory`      | crontab(\*/4:15) | heavy   |

### 3.4 Полный реестр Celery Tasks (32 задачи)

#### WB Tasks (14)

| Task                          | Файл          | Time Limit | Тип                         |
| ----------------------------- | ------------- | ---------- | --------------------------- |
| `update_bids`                 | tasks.py:11   | —          | Per-campaign bid update     |
| `update_all_bids`             | tasks.py:29   | —          | Periodic: spawn update_bids |
| `check_positions`             | tasks.py:42   | —          | Position tracking           |
| `check_all_positions`         | tasks.py:59   | —          | Periodic: all positions     |
| `load_historical_data`        | tasks.py:76   | —          | Initial shop setup          |
| `sync_full_history`           | tasks.py:97   | —          | Full order history          |
| `sync_marketplace_data`       | tasks.py:110  | —          | Daily sync                  |
| `download_wb_finance_reports` | tasks.py:150  | —          | Finance CSV download        |
| `sync_wb_finance_history`     | tasks.py:235  | 7200s      | Finance backfill            |
| `sync_wb_advert_history`      | tasks.py:391  | 14400s     | Ad history + events         |
| `sync_commercial_data`        | tasks.py:645  | 3600s      | Prices+Stocks+Events        |
| `sync_warehouses`             | tasks.py:837  | 600s       | Warehouse dict              |
| `sync_product_content`        | tasks.py:876  | 3600s      | Content+SEO audit           |
| `sync_sales_funnel`           | tasks.py:1044 | 600s       | Funnel 2-day sync           |
| `backfill_sales_funnel`       | tasks.py:1146 | 7200s      | Funnel backfill             |
| `sync_orders`                 | tasks.py:1285 | 600s       | Orders 10-min sync          |
| `backfill_orders`             | tasks.py:1379 | 7200s      | Orders backfill             |

#### Ozon Tasks (18)

| Task                          | Файл          | Time Limit | Тип                   |
| ----------------------------- | ------------- | ---------- | --------------------- |
| `sync_ozon_products`          | tasks.py:1484 | 600s       | Product catalog → PG  |
| `sync_ozon_product_snapshots` | tasks.py:1566 | 600s       | 4-in-1 CH snapshot    |
| `sync_ozon_orders`            | tasks.py:1663 | 600s       | FBO+FBS orders        |
| `backfill_ozon_orders`        | tasks.py:1744 | 3600s      | Orders 365d           |
| `sync_ozon_finance`           | tasks.py:1822 | 600s       | Transactions 2-day    |
| `backfill_ozon_finance`       | tasks.py:1903 | 3600s      | Finance by months     |
| `sync_ozon_funnel`            | tasks.py:1984 | 600s       | Analytics/data daily  |
| `backfill_ozon_funnel`        | tasks.py:2039 | 3600s      | Funnel 365d           |
| `sync_ozon_returns`           | tasks.py:2098 | 600s       | Returns 30d           |
| `backfill_ozon_returns`       | tasks.py:2154 | 3600s      | Returns 180d          |
| `sync_ozon_warehouse_stocks`  | tasks.py:2212 | 300s       | FBO stocks snapshot   |
| `sync_ozon_prices`            | tasks.py:2260 | 300s       | Prices+commissions    |
| `sync_ozon_seller_rating`     | tasks.py:2306 | 120s       | Account health        |
| `sync_ozon_content`           | tasks.py:2353 | 300s       | Content hashes+events |
| `sync_ozon_inventory`         | tasks.py:2472 | 300s       | Inventory snapshot    |
| `sync_ozon_commissions`       | tasks.py:2550 | 300s       | Commissions snapshot  |
| `sync_ozon_content_rating`    | tasks.py:2627 | 300s       | Content rating        |
| `monitor_ozon_bids`           | tasks.py:2733 | 600s       | Bids+events 15min     |
| `sync_ozon_ad_stats`          | tasks.py:2930 | 600s       | Ad stats hourly       |
| `backfill_ozon_ads`           | tasks.py:3044 | 3600s      | Ad stats 180d         |

---

## 4. PostgreSQL — Dimension-модели

### 4.1 DimProduct (`product.py`)

```
dim_products (UniqueConstraint: shop_id + nm_id)
├── id             INTEGER PK AUTO
├── shop_id        INTEGER NOT NULL (indexed)
├── nm_id          BIGINT NOT NULL (indexed)
├── vendor_code    VARCHAR(100)
├── name           VARCHAR(500)
├── main_image_url TEXT
├── length/width/height  NUMERIC(8,2)  ← габариты для логистики
├── current_price  NUMERIC(12,2)       ← обновляется каждые 30 мин
├── current_discount INTEGER            ← обновляется каждые 30 мин
├── category       VARCHAR(255)
├── created_at     DATETIME
└── updated_at     DATETIME (auto-update)
```

**Обновляется из:** `wb_prices_service` (30 мин), `wb_content_service` (daily)

### 4.2 DimWarehouse (`warehouse.py`)

```
dim_warehouses
├── warehouse_id   INTEGER PK (НЕ auto!)
├── name           VARCHAR(255) NOT NULL (indexed)
├── address        TEXT
├── city           VARCHAR(150)
├── is_verified    BOOLEAN (false = auto-created by stocks)
├── created_at     DATETIME
└── updated_at     DATETIME (auto-update)
```

**Обновляется из:** `wb_warehouses_service` (daily, verified), `wb_stocks_service` (on-the-fly, unverified)

### 4.3 EventLog (`event_log.py`)

```
event_log
├── id             INTEGER PK AUTO
├── created_at     DATETIME NOT NULL
├── shop_id        INTEGER NOT NULL (indexed)
├── advert_id      BIGINT NOT NULL (indexed)
├── nm_id          BIGINT NULLABLE     ← NULL для campaign-level events
├── event_type     VARCHAR(50) NOT NULL
├── old_value      TEXT NULLABLE
├── new_value      TEXT NULLABLE
└── event_metadata JSON NULLABLE
```

---

## 5. ClickHouse — Полная карта таблиц

### 5.1 Fact-таблицы (ReplacingMergeTree)

| Таблица                      | Ver          | ORDER BY                                                            | Partition                  | TTL |
| ---------------------------- | ------------ | ------------------------------------------------------------------- | -------------------------- | --- |
| `fact_finances`              | `updated_at` | `(shop_id, marketplace, event_date, order_id, external_id, rrd_id)` | `toYYYYMM(event_date)`     | —   |
| `fact_orders_raw`            | `synced_at`  | `(shop_id, g_number)`                                               | `toYYYYMM(date)`           | 2y  |
| `fact_advert_stats_v3`       | `updated_at` | `(shop_id, nm_id, date, advert_id)`                                 | `toYYYYMM(date)`           | —   |
| `fact_sales_funnel`          | `fetched_at` | `(shop_id, nm_id, event_date)`                                      | `toYYYYMM(event_date)`     | 2y  |
| `fact_inventory_snapshot`    | `fetched_at` | `(shop_id, nm_id, warehouse_name)`                                  | `toYYYYMM(fetched_at)`     | 1y  |
| `fact_ozon_orders`           | `updated_at` | `(shop_id, posting_number, sku)`                                    | `toYYYYMM(order_date)`     | —   |
| `fact_ozon_transactions`     | `updated_at` | `(shop_id, operation_id)`                                           | `toYYYYMM(operation_date)` | —   |
| `fact_ozon_funnel`           | `updated_at` | `(shop_id, sku, dt)`                                                | `toYYYYMM(dt)`             | —   |
| `fact_ozon_inventory`        | `fetched_at` | `(shop_id, product_id)`                                             | `toYYYYMM(fetched_at)`     | 1y  |
| `fact_ozon_warehouse_stocks` | `fetched_at` | `(shop_id, sku, warehouse_name)`                                    | —                          | —   |
| `fact_ozon_ad_daily`         | `updated_at` | `(shop_id, campaign_id, sku, dt)`                                   | —                          | —   |
| `fact_ozon_returns`          | `updated_at` | `(shop_id, return_id)`                                              | —                          | —   |
| `fact_ozon_prices`           | `updated_at` | `(shop_id, sku)`                                                    | —                          | —   |
| `fact_ozon_commissions`      | `updated_at` | —                                                                   | —                          | —   |
| `fact_ozon_content_rating`   | `updated_at` | —                                                                   | —                          | —   |
| `fact_ozon_seller_rating`    | `updated_at` | `(shop_id, rating_name)`                                            | —                          | —   |
| `dim_advert_campaigns`       | `updated_at` | `(shop_id, advert_id)`                                              | —                          | —   |
| `fact_advert_stats`          | `updated_at` | `(shop_id, nm_id, date, advert_id)`                                 | `toYYYYMM(date)`           | —   |
| `orders`                     | `updated_at` | `(shop_id, order_id, sku)`                                          | `toYYYYMM(order_date)`     | 2y  |
| `sales_daily`                | `updated_at` | `(date, shop_id, sku)`                                              | `toYYYYMM(date)`           | —   |
| `ad_stats`                   | `updated_at` | `(date, shop_id, campaign_id)`                                      | `toYYYYMM(date)`           | 1y  |
| `positions`                  | `updated_at` | `(shop_id, sku, keyword, toStartOfMinute(checked_at))`              | `toYYYYMM(checked_at)`     | 3m  |

### 5.2 Log-таблицы (MergeTree, append-only)

| Таблица           | ORDER BY                                  | TTL     |
| ----------------- | ----------------------------------------- | ------- |
| `ads_raw_history` | `(shop_id, advert_id, nm_id, fetched_at)` | 6 month |
| `log_ozon_bids`   | `(shop_id, campaign_id, sku, timestamp)`  | —       |

### 5.3 Views и Materialized Views

| View                       | Тип                     | Источник            | Назначение           |
| -------------------------- | ----------------------- | ------------------- | -------------------- |
| `fact_sales_funnel_latest` | View                    | `fact_sales_funnel` | argMax дедупликация  |
| `fact_orders_raw_latest`   | View                    | `fact_orders_raw`   | argMax дедупликация  |
| `fact_finances_latest`     | View                    | `fact_finances`     | argMax дедупликация  |
| `orders_latest`            | View                    | `orders`            | argMax дедупликация  |
| `ad_stats_latest`          | View                    | `ad_stats`          | argMax дедупликация  |
| `positions_latest`         | View                    | `positions`         | argMax дедупликация  |
| `ads_daily_mv`             | MV (ReplacingMergeTree) | `ads_raw_history`   | Daily MAX агрегация  |
| `ads_hourly_mv`            | MV (ReplacingMergeTree) | `ads_raw_history`   | Hourly MAX агрегация |
| `sales_hourly_mv`          | MV (SummingMergeTree)   | `orders`            | Hourly SUM агрегация |

---

## 6. Event Detection

### 6.1 EventDetector (`event_detector.py`, 620 строк) — WB Advertising

| Класс                     | Event Types                                                                       | Redis ключи                                                                                                  |
| ------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `EventDetector`           | `BID_CHANGE`, `STATUS_CHANGE`, `ITEM_ADD`, `ITEM_REMOVE`, `ITEM_INACTIVE`         | `state:bid:{shop_id}:{advert_id}`, `state:status:{shop_id}:{advert_id}`, `state:items:{shop_id}:{advert_id}` |
| `CommercialEventDetector` | `PRICE_CHANGE`, `STOCK_OUT`, `STOCK_REPLENISH`, `CONTENT_CHANGE`, `ITEM_INACTIVE` | `state:price:{shop_id}:{nm_id}`, `state:stock:{shop_id}:{nm_id}:{wh}`, `state:images:{shop_id}:{nm_id}`      |
| `ContentEventDetector`    | `TITLE_CHANGE`, `DESCRIPTION_CHANGE`, `PHOTO_CHANGE`, `PHOTO_COUNT_CHANGE`        | Сравнение MD5 хэшей из `dim_product_content` (PG)                                                            |

**Debouncing:** `EventDetector` реализует debouncing чтобы избежать «мусорных» событий от API storms.

**STOCK_OUT условие:** `old > 0 AND new == 0`
**STOCK_REPLENISH условие:** `new - old >= 50` (большой jump)

### 6.2 OzonAdsEventDetector (`ozon_ads_event_detector.py`, 266 строк)

| Метод                       | Event Types                                            |
| --------------------------- | ------------------------------------------------------ |
| `detect_campaign_changes()` | `OZON_STATUS_CHANGE`, `OZON_BUDGET_CHANGE`             |
| `detect_product_changes()`  | `OZON_BID_CHANGE`, `OZON_ITEM_ADD`, `OZON_ITEM_REMOVE` |

**Особенности:** Bid values в microroubles (1 RUB = 1,000,000 micro), конвертация через `MICROROUBLES = 1_000_000`.

**Redis ключи:** `state:ozon_campaign:{shop_id}:{campaign_id}`, `state:ozon_bid:{shop_id}:{campaign_id}:{sku}`

---

## 7. Подробные описания модулей

Детальное описание каждого модуля (API endpoints, все поля данных, маппинги, баги, workarounds) см. в файлах:

- **[loaders_wb.md](loaders_wb.md)** — 8 модулей Wildberries
- **[loaders_ozon.md](loaders_ozon.md)** — 9 модулей Ozon
