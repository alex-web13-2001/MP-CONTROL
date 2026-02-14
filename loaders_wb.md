# Wildberries — Подробное описание загрузчиков

> Все модули расположены в `backend/app/services/wb_*.py`

---

## 1. WB Prices (`wb_prices_service.py`, ~200 строк)

### Назначение

Загрузка текущих цен и скидок продуктов для коммерческого мониторинга.

### API

| Параметр   | Значение                                                     |
| ---------- | ------------------------------------------------------------ |
| Endpoint   | `GET /api/v2/list/goods/filter`                              |
| Base       | `wildberries_prices` = `discounts-prices-api.wildberries.ru` |
| Пагинация  | Offset-based (`limit=1000`, `offset+=1000`)                  |
| Rate Limit | 0.5s между запросами                                         |

### Классы

**`WBPricesService`**
| Метод | Описание |
|-------|----------|
| `fetch_prices(nm_ids)` | Получить текущие цены для nm_ids |
| `_make_client()` | Фабрика MarketplaceClient |

### Поля из API → маппинг

| API поле                   | Назначение                            |
| -------------------------- | ------------------------------------- |
| `nmID`                     | nm_id товара                          |
| `vendorCode`               | Артикул                               |
| `sizes[0].price`           | Розничная цена (до скидки)            |
| `sizes[0].discountedPrice` | Цена после скидки → `converted_price` |
| `discount`                 | % скидки                              |

### Куда пишет

| Хранилище  | Таблица/ключ                    | Деталь                                      |
| ---------- | ------------------------------- | ------------------------------------------- |
| PostgreSQL | `dim_products`                  | Upsert: `current_price`, `current_discount` |
| Redis      | `state:price:{shop_id}:{nm_id}` | Предыдущая цена для event detection         |
| ClickHouse | `fact_inventory_snapshot`       | Часть snapshot (price fields)               |

### Celery Task

`sync_commercial_data` (tasks.py:645) — Step 1 из 5-шагового pipeline.

---

## 2. WB Stocks (`wb_stocks_service.py`, 145 строк)

### Назначение

FBO остатки по складам — количество товара на каждом складе WB.

### API

| Параметр      | Значение                                                  |
| ------------- | --------------------------------------------------------- |
| Endpoint      | `GET /api/v1/supplier/stocks`                             |
| Base          | `wildberries_stats` = `statistics-api.wildberries.ru`     |
| Query param   | `dateFrom=2019-06-20` (обязательный, но игнорируется API) |
| Формат ответа | Flat array: один элемент = один SKU × один склад          |

### Классы

**`WBStocksService`**
| Метод | Описание |
|-------|----------|
| `fetch_stocks(nm_ids)` | Получить остатки, фильтруя по nm_ids |
| `_parse_stock_row(row)` | Маппинг API → внутренний формат |

### Поля из API → маппинг

| API поле          | Назначение                   |
| ----------------- | ---------------------------- |
| `nmId`            | nm_id товара                 |
| `warehouseName`   | Название склада              |
| `quantity`        | Общее кол-во                 |
| `quantityFull`    | Полное кол-во (вкл. в пути)  |
| `inWayToClient`   | В пути к клиенту             |
| `inWayFromClient` | В пути от клиента (возвраты) |

### Куда пишет

| Хранилище  | Таблица/ключ                         | Деталь                              |
| ---------- | ------------------------------------ | ----------------------------------- |
| Redis      | `state:stock:{shop_id}:{nm_id}:{wh}` | Предыдущий сток для event detection |
| ClickHouse | `fact_inventory_snapshot`            | Часть snapshot (stock fields)       |

### Celery Task

`sync_commercial_data` (tasks.py:645) — Step 2 из 5. После prices.

---

## 3. WB Content (`wb_content_service.py`, 323 строки)

### Назначение

Загрузка карточек товаров: название, описание, фото, габариты, категория.

### API

| Параметр   | Значение                                             |
| ---------- | ---------------------------------------------------- |
| Endpoint   | `POST /content/v2/get/cards/list`                    |
| Base       | `wildberries_content` = `content-api.wildberries.ru` |
| Пагинация  | Cursor-based (`cursor.updatedAt` + `cursor.nmID`)    |
| Batch size | 100 карточек                                         |
| Rate Limit | 0.5s                                                 |

### Классы

**`WBContentService`**
| Метод | Описание |
|-------|----------|
| `fetch_all_cards()` | Получить все карточки (автопагинация) |
| `_extract_photo_ids(card)` | Извлечь ID фото из URL |
| `_compute_hash(data)` | MD5 хэш для change detection |

### Поля из API → маппинг

| API поле                         | Назначение         |
| -------------------------------- | ------------------ |
| `nmID`                           | nm_id              |
| `vendorCode`                     | Артикул            |
| `title`                          | Название           |
| `description`                    | Описание           |
| `photos[].big`                   | URL основного фото |
| `dimensions.length/width/height` | Габариты (см)      |
| `subjectName`                    | Категория          |

### Куда пишет

| Хранилище  | Таблица/ключ                                                |
| ---------- | ----------------------------------------------------------- |
| PostgreSQL | `dim_products` (name, main_image_url, dimensions, category) |
| PostgreSQL | `dim_product_content` (title_hash, desc_hash, photos_hash)  |
| Redis      | `state:images:{shop_id}:{nm_id}` (MD5 хэш фото)             |

### Celery Task

`sync_product_content` (tasks.py:876) — Daily, с event detection.

---

## 4. WB Warehouses (`wb_warehouses_service.py`)

### Назначение

Справочник складов WB (название, адрес, город).

### API

| Параметр  | Значение                       |
| --------- | ------------------------------ |
| Endpoint  | `GET /api/v1/offices`          |
| Base      | `wildberries_marketplace`      |
| Пагинация | Нет (один запрос = все склады) |

### Куда пишет

PostgreSQL → `dim_warehouses` (upsert, `is_verified=True`).

### Celery Task

`sync_warehouses` (tasks.py:837) — Daily at 4:00 AM.

---

## 5. WB Sales Funnel (`wb_sales_funnel_service.py`, ~295 строк)

### Назначение

Воронка продаж: просмотры → корзина → заказы → выкупы с конверсиями.

### API

| Параметр               | Значение                                                        |
| ---------------------- | --------------------------------------------------------------- |
| Endpoint (report)      | `GET /api/v2/nm-report/detail`                                  |
| Endpoint (history)     | `GET /api/v2/nm-report/detail/history`                          |
| Base                   | `wildberries_analytics` = `seller-analytics-api.wildberries.ru` |
| Пагинация              | `page` based                                                    |
| Max nm_ids per request | 20                                                              |
| Rate Limit             | 0.5s между страницами                                           |

### Классы

**`WbSalesFunnelService`**
| Метод | Описание |
|-------|----------|
| `fetch_funnel(nm_ids, begin_dt, end_dt)` | Report: метрики за период |
| `fetch_history_by_days(nm_ids, begin_dt, end_dt)` | History: daily breakdown |
| `_map_history_row(row, nm, shop_id)` | Маппинг → ClickHouse dict |

**`WbSalesFunnelLoader`**
| Метод | Описание |
|-------|----------|
| `insert_rows(rows)` | Batch INSERT в ClickHouse |
| `get_stats(shop_id)` | Статистика по таблице |

### Поля из API → `fact_sales_funnel`

| API поле             | CH колонка          | Тип           |
| -------------------- | ------------------- | ------------- |
| `openCount`          | `open_count`        | UInt32        |
| `cartCount`          | `cart_count`        | UInt32        |
| `orderCount`         | `order_count`       | UInt32        |
| `orderSumRub`        | `order_sum`         | Decimal(18,2) |
| `buyoutCount`        | `buyout_count`      | UInt32        |
| `buyoutSumRub`       | `buyout_sum`        | Decimal(18,2) |
| `cancelCount`        | `cancel_count`      | UInt32        |
| `cancelSumRub`       | `cancel_sum`        | Decimal(18,2) |
| `addToCartPercent`   | `add_to_cart_pct`   | Float32       |
| `cartToOrderPercent` | `cart_to_order_pct` | Float32       |
| `buyoutPercent`      | `buyout_pct`        | Float32       |
| `avgPriceRub`        | `avg_price`         | Decimal(18,2) |
| `addToWishlistCount` | `add_to_wishlist`   | UInt32        |

### ClickHouse таблица

`fact_sales_funnel` — **ReplacingMergeTree(fetched_at)**

- ORDER BY: `(shop_id, nm_id, event_date)`
- PARTITION: `toYYYYMM(event_date)`
- TTL: 2 года

### Celery Tasks

- `sync_sales_funnel` (30 мин) — 2-day window (yesterday + today)
- `backfill_sales_funnel` (one-time) — стратегия: CSV report → fallback History API

---

## 6. WB Orders (`wb_orders_service.py`, ~110 строк)

### Назначение

Оперативные заказы: каждый заказ с логистикой.

### API

| Параметр     | Значение                                          |
| ------------ | ------------------------------------------------- |
| Endpoint     | `GET /api/v1/supplier/orders`                     |
| Base         | `wildberries_stats`                               |
| Query params | `dateFrom`, `flag=0` (lastChangeDate >= dateFrom) |
| Rate Limit   | **63 секунды** между запросами!                   |
| Max rows     | ~80,000 per response                              |

### Поля из API → `fact_orders_raw`

| API поле          | CH колонка         | Тип                   |
| ----------------- | ------------------ | --------------------- |
| `gNumber`         | `g_number`         | String (ORDER BY key) |
| `nmId`            | `nm_id`            | UInt64                |
| `barcode`         | `barcode`          | String                |
| `warehouse`       | `warehouse_name`   | String                |
| `warehouseType`   | `warehouse_type`   | String                |
| `totalPrice`      | `total_price`      | Decimal(18,2)         |
| `discountPercent` | `discount_percent` | Int32                 |
| `finishedPrice`   | `finished_price`   | Decimal(18,2)         |
| `isCancel`        | `is_cancel`        | UInt8                 |
| `regionName`      | `region`           | String                |
| `srid`            | `srid`             | String                |

### ClickHouse

`fact_orders_raw` — **ReplacingMergeTree(synced_at)**, ORDER BY: `(shop_id, g_number)`, TTL: 2y

### Celery Tasks

- `sync_orders` — 10 мин (dateFrom = max in CH, fallback 1h ago)
- `backfill_orders` — 90 дней, 63s rate limit = hours!

---

## 7. WB Advertising (`wb_advert_service.py`)

### Назначение

Рекламная статистика V3: показы, клики, расходы, CPC, CTR per nm_id per day.

### API

| Параметр                  | Значение                                        |
| ------------------------- | ----------------------------------------------- |
| Endpoint (list)           | `POST /adv/v1/promotion/adverts`                |
| Endpoint (stats)          | `POST /adv/v2/fullstats`                        |
| Base                      | `wildberries_adv` = `advert-api.wildberries.ru` |
| Max campaigns per request | 50                                              |
| Rate Limit                | ~1 req/min                                      |

### ClickHouse таблицы

- `ads_raw_history` — **MergeTree** (append-only лог)
- `fact_advert_stats_v3` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, nm_id, date, advert_id)`
- `ads_daily_mv` — **MaterializedView** → daily MAX агрегация
- `ads_hourly_mv` — **MaterializedView** → hourly MAX агрегация

### Celery Task

`sync_wb_advert_history` (tasks.py:391) — с event detection через `EventDetector`.

---

## 8. WB Finance (`wb_finance_service.py`)

### Назначение

Еженедельные финансовые отчёты (реализация): суммы продаж, комиссии, логистика, штрафы.

### API

| Параметр      | Значение                                              |
| ------------- | ----------------------------------------------------- |
| Create report | `GET /api/v5/supplier/reportDetailByPeriod`           |
| Poll status   | `GET /api/v1/supplier/reportDetailByPeriod/{id}`      |
| Download CSV  | `GET /api/v1/supplier/reportDetailByPeriod/{id}/file` |
| Base          | `wildberries_stats`                                   |
| Особенность   | Async: create → poll → download CSV                   |

### ClickHouse

`fact_finances` — **ReplacingMergeTree(updated_at)**, ORDER BY: `(shop_id, marketplace, event_date, order_id, external_id, rrd_id)`

### Celery Tasks

- `download_wb_finance_reports` — загрузка за период
- `sync_wb_finance_history` — backfill 180 дней
