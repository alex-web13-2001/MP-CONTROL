# Changelog

Все изменения в проекте документируются в этом файле.

## [Unreleased] - 2026-02-11

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
