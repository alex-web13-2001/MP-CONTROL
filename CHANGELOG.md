# Changelog

Все изменения в проекте документируются в этом файле.

## [Unreleased] - 2026-02-14

### Added — Модуль «Ozon Ads & Bids Tracking» (Performance API)

- **ozon_performance_auth.py:** OAuth2 client_credentials авторизация для Ozon Performance API. Кэширование токена в памяти + Redis (TTL 25 мин из 30).
- **ozon_ads_service.py:** Сервис для работы с Ozon Performance API:
  - Получение кампаний (`GET /api/client/campaign`) — 64 кампании, 24 активных
  - Real-time ставки (`GET /v2/products`) — bid в микрорублях, 35 активных продуктов
  - Конкурентные ставки (`GET /products/bids/competitive`)
  - Async CSV-отчёты (`POST /statistics → UUID → GET /report`) с батчированием по 10 кампаний
  - Retry с exponential backoff (3 попытки) для устойчивости к timeout'ам
  - CSV-парсер с BOM-фиксом для Ozon отчётов
- **OzonBidsLoader:** ClickHouse loader для `log_ozon_bids` и `fact_ozon_ad_daily` с дедупликацией
- **ClickHouse DDL:** `log_ozon_bids` (MergeTree), `fact_ozon_ad_daily` (ReplacingMergeTree)
- **Celery Tasks:**
  - `monitor_ozon_bids` (15 мин) — мониторинг ставок, delta-check через Redis, запись изменений
  - `sync_ozon_ad_stats` (60 мин) — скользящее окно 3 дня для покрытия атрибуции Ozon
  - `backfill_ozon_ads` (одноразовая) — загрузка истории неделя за неделей

### E2E verified: OAuth2 → 35 bids → ClickHouse, CSV report → 4 rows → fact_ozon_ad_daily (spend=23.82₽, revenue=591₽)

### Changed — Миграция Ozon Ads на MarketplaceClient (прокси)

- **marketplace_client.py:** Добавлен `ozon_performance` в `MARKETPLACE_URLS` (`https://api-performance.ozon.ru`)
- **ozon_ads_service.py:** `_request()` переписан: `httpx.AsyncClient` → `MarketplaceClient(marketplace="ozon_performance")` с proxy rotation, rate limiting, circuit breaker, JA3 spoofing. OAuth2 Bearer передаётся через `headers` kwarg.
- **tasks.py:** Все 3 задачи обновлены — `AsyncSession` + `engine.dispose()` + `MarketplaceClient` (как WB)
- **celery.py:** Task routes (fast/heavy) + beat schedule шаблон для Ozon Ads

### Added — Event Tracking для Ozon Ads (как WB)

- **ozon_ads_event_detector.py [NEW]:** `OzonAdsEventDetector` — детектит 5 типов событий: `OZON_BID_CHANGE`, `OZON_STATUS_CHANGE`, `OZON_BUDGET_CHANGE`, `OZON_ITEM_ADD`, `OZON_ITEM_REMOVE`. Использует `RedisStateManager` для сравнения с last state.
- **redis_state.py:** Добавлены `get_ozon_campaign_state` / `set_ozon_campaign_state` — хранение last state кампаний (bids, status, budget, items).
- **tasks.py → monitor_ozon_bids:** Интегрирован `OzonAdsEventDetector` — события сохраняются в PostgreSQL `event_log` (единая таблица с WB).

### Fixed

- **tasks.py → monitor_ozon_bids:** Исправлен `::jsonb` cast → `CAST(:event_metadata AS jsonb)` — asyncpg не поддерживает native PostgreSQL `::` cast syntax.
- **ozon_ads_service.py → download_report:** Ozon возвращает ZIP-архив для batch-отчётов (10+ campaigns) — добавлена автоматическая распаковка через `zipfile`.
- **ozon_ads_service.py → parse_csv_report:** `campaign_id` теперь обновляется при каждом CSV header `"№ XXXXX"` — критично для multi-campaign ZIP-отчётов.

## [Unreleased] - 2026-02-12

### Changed — Миграция рекламного модуля на MarketplaceClient

- **wb_advertising_report_service.py:** 4 вызова `httpx.AsyncClient()` → `MarketplaceClient(wildberries_adv)` с proxy rotation, rate limiting, circuit breaker, JA3 spoofing.
- **Celery:** `sync_wb_advert_history` обновлён — `create_async_engine` + `AsyncSession` для передачи `db` в `WBAdvertisingReportService(db, shop_id, api_key)`.
- **Итого:** 0 модулей с прямыми httpx/requests вызовами. Все API запросы через MarketplaceClient.

### Added — Модуль «Ozon Core — Товары, Контент и История»

- **MarketplaceClient:** Расширен — добавлен `client_id` для Ozon API (Client-Id + Api-Key headers).
- **Ozon Products Service:** `ozon_products_service.py` — async `OzonProductsService` через `MarketplaceClient(ozon)` с proxy rotation, rate limiting, circuit breaker.
- **API Endpoints:** `POST /v3/product/list` (пагинация через last_id), `POST /v3/product/info/list` (batches of 100), `POST /v1/product/info/description`.
- **PostgreSQL:** Таблица `dim_ozon_products` (40 товаров — offer_id, SKU, prices, stocks, images, barcodes, volume_weight).
- **PostgreSQL:** Таблица `dim_ozon_product_content` (MD5 хеши title, description, images для детекции изменений).
- **ClickHouse:** Таблица `fact_ozon_inventory` (MergeTree, TTL 1 год) — снимки цен и остатков каждые 30 мин.
- **Event Detection:** `OZON_PHOTO_CHANGE`, `OZON_SEO_CHANGE` — сравнение MD5 хешей контента.
- **Celery Tasks:** 3 задачи — `sync_ozon_products` (24h), `sync_ozon_content` (24h), `sync_ozon_inventory` (30 мин).
- **Данные (E2E):** 40 товаров, avg_price 5,367₽, FBO 2,495 шт, FBS 15 шт.

### Added — Модуль «Коммерческий мониторинг»

- **Цены и скидки:** Сервис `wb_prices_service.py` — загрузка цен через `GET /api/v2/list/goods/filter` (discounts-prices-api), пагинация, upsert в `dim_products` (PostgreSQL), кэш в Redis.
- **Остатки FBO:** Сервис `wb_stocks_service.py` — загрузка остатков через `GET /api/v1/supplier/stocks` (statistics-api), авто-создание складов в `dim_warehouses`.
- **Справочник складов:** Сервис `wb_warehouses_service.py` — синхронизация через `GET /api/v3/warehouses` (marketplace-api), ежесуточно.
- **Контент товаров:** Сервис `wb_content_service.py` — загрузка карточек через `POST /content/v2/get/cards/list` (content-api), курсорная пагинация, обновление названий, фото, габаритов, категорий.
- **Event Detector (коммерческий):** Класс `CommercialEventDetector` — детекция `PRICE_CHANGE`, `STOCK_OUT`, `STOCK_REPLENISH`, `CONTENT_CHANGE`, `ITEM_INACTIVE` (реклама на товар с нулевым остатком).
- **Celery Tasks:** 3 новые задачи — `sync_commercial_data` (30 мин), `sync_warehouses` (4:00), `sync_product_content` (4:30).
- **API Endpoints:** Router `/commercial` с 5 эндпоинтами — sync, sync-warehouses, sync-content, status, turnover.
- **PostgreSQL:** Таблицы `dim_products` (справочник товаров) и `dim_warehouses` (справочник складов).
- **ClickHouse:** Таблица `fact_inventory_snapshot` (MergeTree, TTL 1 год) для хранения снимков остатков и цен.
- **Redis State:** Методы `get/set_price`, `get/set_stock`, `get/set_image_url` для кэширования состояний.
- **WB Domains:** Добавлены `wildberries_prices`, `wildberries_content` и `wildberries_marketplace` в `MARKETPLACE_URLS`.

### Added — Модуль «Контент-мониторинг и SEO-аудит»

- **Content Hashing:** Расширен `wb_content_service.py` — MD5-хеширование title, description, фото. Извлечение stable photo_id из WB CDN URL (защита от ложных срабатываний CDN-смены).
- **ContentEventDetector:** Новый класс в `event_detector.py` — 4 типа событий: `CONTENT_TITLE_CHANGED`, `CONTENT_DESC_CHANGED`, `CONTENT_MAIN_PHOTO_CHANGED`, `CONTENT_PHOTO_ORDER_CHANGED`.
- **PostgreSQL:** Таблица `dim_product_content` (хеши: title_hash, description_hash, main_photo_id, photos_hash, photos_count).
- **Redis State:** Методы `get/set_content_hash` для кэширования хешей контента (TTL 3 дня).
- **Celery Task:** Расширен `sync_product_content` — 5-шаговый pipeline: fetch → load hashes → detect events → upsert hashes → update products.

### Added — Модуль «Оперативные заказы и Логистика»

- **API Сервис:** `wb_orders_service.py` — async `WBOrdersService` через `MarketplaceClient(wildberries_stats)` с proxy rotation, rate limiting, circuit breaker. Пагинация через `lastChangeDate` (flag=0), до 80K строк/страница.
- **ClickHouse:** Таблица `fact_orders_raw` (ReplacingMergeTree по g_number, TTL 2 года) — дедупликация по synced_at, view `fact_orders_raw_latest`.
- **Celery Tasks:** `sync_orders` (каждые 10 мин, dateFrom=1 час) и `backfill_orders` (однократно, N дней).
- **Beat Schedule:** `sync-orders-10min` каждые 600 сек (закомментирован, готов к активации).
- **Данные:** 17,541 заказ за 4+ мес (окт 2025 — фев 2026), 47 продуктов, 951 отмена, 58.8M RUB выручки. Пагинация через lastChangeDate.

### Added — Модуль «Воронка продаж WB»

- **API Сервис:** `wb_sales_funnel_service.py` — класс `WBSalesFunnelService` для загрузки данных воронки продаж с автоматическим разбиением на чанки (max 20 nmIds, 7 дней).
- **Три метода загрузки:** `fetch_history_by_days` (подневная история), `fetch_aggregate` (агрегат за 365 дней), CSV-отчёт (create → poll → download → parse ZIP).
- **ClickHouse:** Таблица `fact_sales_funnel` (MergeTree append-only, TTL 2 года) — 14 метрик + `fetched_at` для хранения истории изменений каждые 30 мин.
- **ClickHouse View:** `fact_sales_funnel_latest` — дедупликация через argMax для быстрых запросов (последний снимок).
- **Celery Tasks:** `sync_sales_funnel` (каждые 30 мин, append) и `backfill_sales_funnel` (однократно, 6 мес через CSV nm-report → 7,366 rows, 52 продукта).
- **Beat Schedule:** `sync-sales-funnel-30min` каждые 30 минут (закомментирован, готов к активации).
- **WB Domain:** Добавлен `wildberries_analytics` = `seller-analytics-api.wildberries.ru` в `MARKETPLACE_URLS`.

### Fixed — Коммерческий модуль (тестирование с реальным API)

- **DNS:** Домен `advert-api.wb.ru` → `advert-api.wildberries.ru` (не резолвился из Docker).
- **Stocks API:** Endpoint `/api/v3/stocks` (advert-api, 404) → `/api/v1/supplier/stocks` (statistics-api, 200).
- **Warehouses API:** Endpoint `/api/v1/offices` (common-api, 404) → `/api/v3/warehouses` (marketplace-api, 200).
- **Prices mapping:** Поле `convertedPrice` → `discountedPrice`, `discount` перенесён с уровня sizes на уровень товара.
- **Зависимость:** Добавлен `psycopg2-binary==2.9.9` для записи events в PostgreSQL из Celery.
- **Event Loop:** `sync_wb_advert_history` — заменён deprecated `asyncio.get_event_loop().run_until_complete()` на `asyncio.run()` (ошибка "There is no current event loop in thread 'MainThread'").

## [Unreleased] - 2026-02-02

### Added

- **Direct JSON Ingestion:** Добавлен метод `parse_json_rows` в `WBReportParser` для прямой обработки данных из API V5, минуя CSV.
- **Sequential Sync Logic:** Реализован строгий последовательный порядок запросов ("Правило одной руки") с паузами 5с между неделями и 60с перед первым опросом (хотя опрос больше не нужен).
- **Safe Logging:** Внедрена вложенная транзакция (`begin_nested`) для логирования в `MarketplaceClient`, чтобы ошибки вставки логов (например, FK violation) не прерывали основной бизнес-процесс.
- **Troubleshooting Guide:** Добавлен раздел в `walkthrough.md` по решению проблем с API лимитами и 429 ошибками.

### Changed

- **WB Finance Sync:** Полностью переписан механизм синхронизации фин. отчетов (`WBFinanceReportService`).
  - **Отключено:** Генерация отчетов через `/api/v1/reports/financial/generate` (API возвращал 404).
  - **Включено:** Получение данных напрямую из метода `/api/v5/supplier/reportDetailByPeriod`.
- **Infrastructure:**
  - Обновлен `docker-compose.yml`: добавлены healthchecks, исправлены env var для ClickHouse (`CLICKHOUSE_DB` вместо `CLICKHOUSE_DATABASE`).
  - Исправлен `init.sql`: восстановлено создание таблицы `fact_finances` и `fact_finances_latest`.
- **Celery Tasks:**
  - `sync_wb_finance_3months`: переведена на использование нового метода `get_report_data`.
  - Добавлен автоматический реконнект к Redis в `RedisRateLimiter` и `CircuitBreaker` для исправления ошибки `RuntimeError: Event loop is closed`.

### Fixed

- **API 404 Error:** Устранена ошибка при попытке генерации отчетов по старому методу.
- **Event Loop Error:** Исправлено падение воркеров Celery из-за закрытия event loop при использовании `asyncio.run`.
- **Auth Error:** Исправлена проблема с пользователем `default` в ClickHouse (удален конфликтный `default-user.xml`).
