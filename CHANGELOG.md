# Changelog

Все изменения в проекте документируются в этом файле.

## [Unreleased] - 2026-02-18

### Fixed — TypeError в координаторах sync_all_frequent / sync_all_ads

- **Проблема:** `TypeError: _dedup_dispatch() got multiple values for argument 'shop_id'` — `shop_id` передавался и как позиционный аргумент `_dedup_dispatch`, и внутри `**kwargs`.
- **Решение:** `_dedup_dispatch` теперь автоматически инжектит `shop_id` в task kwargs (`task_kwargs = {"shop_id": shop_id, **kwargs}`). Убраны дубликаты `shop_id` из 9 мест вызовов в координаторах.
- **Файлы:** `tasks.py` (`_dedup_dispatch`, `sync_all_daily`, `sync_all_frequent`, `sync_all_ads`)

### Improved — UI прогресса загрузки данных

- **Проблема:** Прогресс-бар показывал 100% при незавершённой загрузке (прогресс рассчитывался в начале шага, а не после). Текст «несколько минут» нереалистичен (WB ~30 мин, Ozon ~15 мин).
- **Решение:**
  - Формула прогресса `(step-1)/total` вместо `step/total` — 100% только после завершения
  - Фронтенд ограничивает процент до 99% пока статус не `done`
  - ETA на основе маркетплейса и текущего шага: «Осталось ≈ 15 минут»
  - Sub-progress для долгих шагов: «Неделя 14 из 27» (финансы WB), «Период 3 из 6» (реклама Ozon)
  - Elapsed time: «Прошло: 5 мин 32 сек»
- **Файлы:** `tasks.py` (`_set_progress`, `sync_wb_finance_history`, `backfill_ozon_ads`), `shops.py`, `auth.ts`, `ShopWizard.tsx`, `OnboardingPage.tsx`

### Added — Обновление API-токенов магазинов

- **Проблема:** Токены маркетплейсов имеют время жизни. При истечении/смене ключа приходилось удалять и заново создавать магазин, теряя историю.
- **Backend:** Новый endpoint `PATCH /shops/{id}/keys` — валидирует ключ через API маркетплейса, шифрует, сохраняет. Сбрасывает статус `error` → `active`.
- **Frontend:** Кнопка 🔑 на каждой карточке магазина → inline-форма:
  - WB: 1 поле (API-ключ)
  - Ozon: 4 поля (API-ключ, Client-Id, Perf Client-Id, Perf Secret)
- **Файлы:** `auth.py` (схема `ShopUpdateKeys`), `shops.py` (endpoint), `SettingsPage.tsx` (UI)

### Changed — Реструктуризация очередей Celery для масштабирования

- **Проблема:** Все задачи синхронизации шли в одну очередь `heavy` с concurrency=2. При 50+ магазинах задачи разных магазинов блокировали друг друга, хотя API лимиты привязаны к ключу, а не IP.
- **Решение:**
  - Очередь `heavy` разделена на `sync` (c=8, регулярная синхронизация) и `backfill` (c=2, начальная загрузка)
  - Добавлена Redis-based deduplication: координаторы не создают дубликаты задач если предыдущая ещё в очереди
  - Signal handler `_cleanup_dedup_key` автоматически освобождает dedup-блокировку после завершения задачи
- **Файлы:** `docker-compose.yml`, `celery.py` (38 routes, 5 очередей), `tasks.py` (3 координатора + helper)

### Fixed — Дублирование данных в ClickHouse (ReplacingMergeTree без FINAL)

- **Root cause:** Все SELECT-запросы к `ReplacingMergeTree` таблицам выполнялись без модификатора `FINAL`, из-за чего ClickHouse возвращал несхлопнутые дубли. Расхождение с Ozon Performance: **25 468₽ (Ozon) vs 35 607₽ (наша БД)** — завышение на 40%.
- **10 файлов, 15 запросов исправлены:**
  - `ozon_finance_service.py` — `get_stats()`, `get_pnl()` (fact_ozon_transactions)
  - `ozon_orders_service.py` — `get_stats()` (fact_ozon_orders)
  - `ozon_products_service.py` — 5 Loader'ов: inventory, commissions, content_rating, promotions, availability
  - `ozon_seller_rating_service.py` — `get_stats()` (fact_ozon_seller_rating)
  - `ozon_price_service.py` — `get_stats()` (fact_ozon_prices)
  - `ozon_funnel_service.py` — `get_stats()` (fact_ozon_funnel)
  - `ozon_returns_service.py` — `get_stats()` (fact_ozon_returns)
  - `ozon_warehouse_stocks_service.py` — `get_stats()` (fact_ozon_warehouse_stocks)
  - `wb_orders_service.py` — `get_stats()` (fact_orders_raw)
- **`ozon_ads_service.py`:** Добавлен `OPTIMIZE TABLE FINAL` после INSERT в `insert_stats()` — дубли схлопываются сразу, не дожидаясь фонового мержа.
- **Верификация:** После `OPTIMIZE TABLE FINAL` запросы с/без `FINAL` возвращают идентичные данные: **25 546.73₽** (расхождение с Ozon < 0.3%).

### Improved — Каскадное удаление магазина

- **Backend / `shops.py` (`DELETE /shops/{id}`):** Ранее удалялась только запись из таблицы `shops`. Теперь полная очистка:
  - **ClickHouse:** 27 таблиц (`fact_ozon_ad_daily`, `fact_ozon_orders`, `fact_orders_raw`, `fact_sales_funnel` и др.)
  - **PostgreSQL:** 7 таблиц (`dim_ozon_products`, `dim_ozon_product_content`, `dim_products`, `dim_product_content`, `event_log`, `autobidder_settings`, `dim_warehouses`)
  - **Redis:** 10 паттернов ключей (state кампаний, цены, остатки, контент-хеши, sync progress, locks)
- Ошибки очистки CH/Redis логируются, но не блокируют удаление из PostgreSQL.

## [Unreleased] - 2026-02-17

### Added — Лёгкая задача сбора данных рекламных кампаний WB (каждые 30 мин)

- **Backend / `tasks.py`:** Новая задача `sync_wb_campaign_snapshot` — использует только 2 API вызова (`/adv/v1/promotion/count` + `/api/advert/v2/adverts`), выполняется за ~4 сек. Сохраняет ставки (CPM/CPC в копейках), имена кампаний, payment_type, bid_type, placements.
- **Backend / `tasks.py`:** Новый dispatcher `sync_all_campaign_snapshots` — запускает snapshot для всех активных WB магазинов.
- **Backend / `wb_advertising_loader.py`:** Новый метод `load_campaigns_v2()` — обновляет `dim_advert_campaigns` с полными данными из V2 API (имена, payment_type, bid_type, search_enabled, recommendations_enabled).
- **Backend / `celery.py`:** Routing + scheduler для новых задач (каждые 30 минут).

### Fixed — Ставки WB кампаний не сохранялись в `log_wb_bids`

- **Root cause:** В `sync_wb_advert_history` переменная `service` использовалась вне `async with async_session()` — сессия БД была закрыта, API вызов падал молча.
- **Backend / `tasks.py` (line ~1220):** Каждый batch V2 API теперь создаёт свою сессию.
- **ClickHouse:** `log_wb_bids.status` UInt8→Int8 (WB API возвращает -1 для удалённых кампаний).
- **ClickHouse:** Добавлены колонки в `dim_advert_campaigns`: `payment_type`, `bid_type`, `search_enabled`, `recommendations_enabled`.

### Fixed — Удаление магазинов: ошибки больше не глотаются молча

- **Frontend / `SettingsPage.tsx`:** Ошибки удаления теперь показываются пользователю. При 404 (магазин не найден в БД) список автоматически обновляется — «призрачные» магазины исчезают.

### Fixed — Воронка продаж WB: ошибка 400 "excess limit on days"

- **Root cause:** WB Seller Analytics `/history` API принимает start date только за последние **7 дней**. `backfill_sales_funnel` запрашивал 365 дней → 51 из 52 weekly windows возвращали 400.
- **Backend / `tasks.py` (`backfill_sales_funnel`):** При fallback на History API (когда CSV report недоступен) диапазон дат ограничен до 7 дней.
- **Backend / `rate_limiter.py`:** Добавлен конфиг `wildberries_analytics` (21-сек окно, 1 req/window). Redis-ключи теперь scoped по marketplace — изоляция sliding windows между API-доменами. Добавлены `window_seconds`/`max_requests_in_window`.
- **Backend / `wb_sales_funnel_service.py`:** Отключен прокси (`use_proxy=False`) для Analytics API. Улучшено логирование ошибок (включен `detail` из тела ответа).

### Added — Предупреждения о недостающих правах WB API-ключа

- **Frontend / `ShopWizard.tsx`:** При валидации ключа WB показываются предупреждения в amber-стиле, если часть сервисов (`statistics-api`, `advert-api` и т.д.) недоступна. Пользователь видит какие именно права отсутствуют и может продолжить подключение.
- **Frontend / `auth.ts`:** Добавлен тип `warnings` в `ValidateKeyResponse`.
- **Backend / `shops.py`:** Добавлен домен `finance-api` в список проверяемых WB-сервисов (всего теперь 6 доменов).

## [Unreleased] - 2026-02-16

### Fixed — Мерцание темы (dark ↔ light) при загрузке

- **Frontend / `index.html`:** Добавлен синхронный инлайн-скрипт, который читает тему из `localStorage` **до** загрузки React и применяет/удаляет класс `.light` на `<html>`. Устраняет «вспышку» неправильной темы (FOIT).
- **Frontend / `appStore.ts` / `onRehydrateStorage`:** Исправлена логика — теперь при dark теме `.light` класс **удаляется** (раньше только добавлялся для light, но никогда не удалялся).

### Fixed — Невидимый текст кнопки «Войти»

- **Frontend / `index.css`:** Добавлен `color: inherit` в глобальный button reset. Без этого текст кнопки мог стать невидимым (белый на белом в light theme).

### Added — Авто-выбор магазина при входе

- **Frontend / `OnboardingGuard.tsx`:** Если `currentShop === null` и есть active магазины — автоматически выбирает первый.
- **Frontend / `LoginPage.tsx`:** После логина сразу ставит первый active магазин в appStore.
- **Frontend / `Header.tsx`:** Убран placeholder «Выберите магазин» — если магазины есть, один всегда выбран.

### Added — Страница «Настройки» с управлением магазинами

- **Frontend / `SettingsPage.tsx` [NEW]:** Полноценная страница с двумя секциями:
  - **Магазины** — список подключённых магазинов с бейджами маркетплейса/статуса, кнопки: «Добавить магазин», «Выбрать», «Удалить» (с подтверждением).
  - **Профиль** — имя, email, кнопка «Выйти».
- **Frontend / `ShopWizard.tsx` [NEW]:** Извлечённый из OnboardingPage wizard (marketplace → API keys → validation → sync progress). Переиспользуется в Onboarding и Settings.
- **Frontend / `OnboardingPage.tsx` [REFACTORED]:** С 765 строк до ~150. Использует ShopWizard.
- **Frontend / `App.tsx`:** Подключён route `/settings → SettingsPage`.

---

## [Unreleased] - 2026-02-15

### Added — Redis distributed lock для дедупликации задач

- **Backend / `tasks.py` / `load_historical_data`:**
  - Добавлен Redis lock (`SET lock:load_historical_data:{shop_id} NX EX 14400`) при старте задачи.
  - Дубликаты мгновенно возвращают `{status: 'skipped', reason: 'already_running'}` вместо полного прогона.
  - Lock автоматически освобождается в `finally` блоке (или по TTL=4ч при crash).
  - Решает проблему дублей после `revoke(terminate=True)` и повторных dispatch'ей.

### Optimization — Early exit для рекламной статистики при пустых данных

- **Ozon / `backfill_ozon_ads`:**
  - Если **3 подряд** недельных чанка возвращают 0 строк — прекращаем загрузку. Кампании не существовали так далеко в прошлом.
  - Ошибки API также считаются как пустые чанки (инкрементируют empty_streak).
  - Экономит ~15-20 мин бессмысленных запросов через Ozon 429 rate limit.

- **WB / `sync_wb_advert_history`:**
  - Если **2 подряд** 30-дневных интервала возвращают 0 строк — прекращаем.
  - Каждый пустой интервал = ~50+ запросов × 65 сек rate limit = часы пустой работы.
  - Стрик сбрасывается при обнаружении данных.

### Fixed — Login/Refresh возвращает 422 (NameError: ShopResponse)

- **Backend / `auth.py`:**
  - 🔴 **КРИТИЧЕСКИЙ БАГ:** `_user_to_response()` использовала `ShopResponse.model_validate()`, но `ShopResponse` не был импортирован из `app.schemas.auth`. Результат: `NameError` → 500/422 на каждый `/auth/login` и `/auth/refresh`. Исправлено добавлением импорта.

### Fixed — Сессия рвётся при выходе из онбординга во время синхронизации

- **Backend / `auth.py`:**
  - 🔴 **КРИТИЧЕСКИЙ БАГ:** Refresh endpoint использовал `int(user_id)`, но `User.id` — UUID. `int()` на UUID-строке всегда бросает `ValueError` → 500 → клиент считает refresh failed → logout. **Refresh токенов не работал вообще.** Исправлено на `uuid.UUID(user_id)`.
- **Backend / `config.py`:**
  - `access_token_expire_minutes` увеличен с 30 до 120 минут. WB pipeline ~23 мин; с 30-минутным окном пользователь мог разлогиниться просто подождав результат.
- **Frontend / `client.ts`:**
  - Axios interceptor теперь обновляет токены через Zustand `updateTokens()` вместо прямой записи в localStorage. Это гарантирует реактивное обновление React-компонентов.
  - При неудаче refresh — мягкий logout через Zustand (`AuthGuard` → `/login`), без `window.location.href` и очистки localStorage.
- **Frontend / `authStore.ts`:**
  - Добавлен метод `updateTokens(accessToken, refreshToken)` для использования в interceptor.
- **Frontend / `LoginPage.tsx`:**
  - Smart redirect после логина: если нет магазинов или все в `syncing` → `/onboarding`, иначе → dashboard.

### Fixed — CSV парсер воронки продаж (backfill_sales_funnel)

- **Backend / MarketplaceClient:**
  - `marketplace_client.py` — добавлено поле `response_bytes` в `MarketplaceResponse` для сохранения raw binary ответов (`response.content`). Ранее `curl_cffi response.text` необратимо повреждал бинарные данные (ZIP) при UTF-8 декодировании.
  - `wb_sales_funnel_service.py` — `download_csv_report()` использует `resp.response_bytes` вместо `resp.data` (str). `parse_csv_report()` принимает оба типа (str/bytes) с автоконверсией.
  - Добавлено логирование: ZIP файлов, количества распарсенных строк, полный traceback при ошибках.
  - **Результат:** 7,414 строк воронки продаж загружены в ClickHouse (ранее 0 из-за бага).

### Added — Загрузка данных после подключения магазина (Phase 2)

- **Backend:**
  - `celery_app/tasks/tasks.py` — `load_historical_data` оркестратор: читает credentials из PG, запускает 5 Ozon / 4 WB subtasks последовательно, пишет прогресс в Redis (`sync_progress:{shop_id}`)
  - `api/v1/shops.py` — `GET /shops/{id}/sync-status` (polling прогресса из Redis); `create_shop` теперь ставит `status='syncing'` и вызывает `load_historical_data.delay()`
- **Frontend:**
  - `pages/OnboardingPage.tsx` — `StepSyncing` компонент: progress bar + polling каждые 3 сек + return-visit handling (если пользователь вернулся — видит прогресс)
  - `components/OnboardingGuard.tsx` — блокирует Dashboard пока все магазины в `syncing`
  - `stores/authStore.ts` — `status` field в `Shop` interface
  - `api/auth.ts` — `SyncStatusResponse` type + `getSyncStatusApi()`

### Added — Onboarding Wizard (подключение магазина)

- **Backend:**
  - `models/shop.py` — новые поля `perf_client_id`, `perf_client_secret_encrypted` для Ozon Performance API
  - `schemas/auth.py` — расширен `ShopCreate` для perf credentials, добавлены `ValidateKeyRequest/Response`
  - `api/v1/shops.py` — `POST /shops/validate-key` (live-проверка API ключей: WB, Ozon Seller, Ozon Performance OAuth2)
  - `docker/postgres/init.sql` — новые колонки в таблице `shops`
- **Frontend:**
  - `pages/OnboardingPage.tsx` — 4-шаговый wizard (маркетплейс → API ключи с правами → валидация → готово)
  - `components/OnboardingGuard.tsx` — redirect на `/onboarding` если нет магазинов
  - `App.tsx` — route `/onboarding` + OnboardingGuard для protected routes
  - `api/auth.ts` — `validateKeyApi()`, расширены типы для perf credentials

### Added — Реальная система авторизации и регистрации

- **Backend:**
  - `models/user.py`, `models/shop.py` — PostgreSQL таблицы `users`, `shops` (SQLAlchemy)
  - `core/security.py` — JWT access/refresh tokens (python-jose), bcrypt пароли (passlib)
  - `schemas/auth.py` — Pydantic schemas (Register, Login, Token, User, Shop)
  - `api/v1/auth.py` — 4 endpoints: `/register` (201), `/login`, `/refresh`, `/me`
  - `api/v1/shops.py` — CRUD: `GET /shops`, `POST /shops` (API key encrypted), `DELETE /shops/{id}`
  - `main.py` — auto-create tables через `metadata.create_all` при старте
- **Frontend:**
  - `api/auth.ts` — API wrapper (register, login, refresh, getMe, shops CRUD)
  - `api/client.ts` — baseURL /api/v1, auto-refresh token на 401, request queue
  - `authStore.ts` — JWT tokens (access+refresh) в zustand persist, shops из API
  - `RegisterPage.tsx` — страница регистрации (premium design)
  - `LoginPage.tsx` — реальный API вызов (заменён mock)
  - `App.tsx` — route `/register`
  - `Header.tsx` — shops из authStore, кнопка выхода
  - `vite.config.ts` — Vite proxy на localhost:8000

### Fixed — UI Layout: KPI карточки обрезались справа (Tailwind v4 CSS Cascade)

- **index.css:** `* { padding: 0; margin: 0 }` находился вне `@layer` — перезаписывал **все** Tailwind v4 padding/margin utilities (которые живут в `@layer utilities`). Перемещено в `@layer base` для корректного cascade.
- **AppLayout.tsx:** `marginLeft` → `paddingLeft` для sidebar offset (box-model fix с `position: fixed` sidebar).
- **DashboardPage.tsx:** KPI grid `lg:grid-cols-3` → `xl:grid-cols-3` + `min-w-0` для robust responsive layout.
- **card.tsx:** Добавлены `shadow-md` и `ring-1` для визуальной глубины.
- **Результат:** Все 6 KPI карточек полностью видны, `hasHorizontalOverflow: false`, padding 32px (lg:px-8).

### Documentation — Расширенная документация загрузчиков

- **loaders.md** — Главный файл: архитектура, Celery (32 tasks, 3 очереди, beat schedule), ClickHouse (22+ таблиц), PostgreSQL модели, Event Detection (4 класса)
- **loaders_wb.md** — 8 модулей Wildberries: API endpoints, маппинги полей, CH таблицы, Celery tasks, constants, bugs
- **loaders_ozon.md** — 9 модулей Ozon: API endpoints, маппинги полей, CH таблицы, Celery tasks, workarounds (Returns API bug)

### Fixed — Миграция MergeTree → ReplacingMergeTree (Audit Fix)

- **fact_sales_funnel:** MergeTree → ReplacingMergeTree(fetched_at), ORDER BY (shop_id, nm_id, event_date)
  - Устранено дублирование при повторных sync. 7,366 rows сохранены.
- **fact_ozon_inventory:** MergeTree → ReplacingMergeTree(fetched_at), ORDER BY (shop_id, product_id)
  - Дубли удалены: 120 → 40 rows.
- **fact_inventory_snapshot:** MergeTree → ReplacingMergeTree(fetched_at), ORDER BY (shop_id, nm_id, warehouse_name)
  - Дубли удалены: 856 → 428 rows.
- DDL в `docker/clickhouse/init.sql` обновлён.

### Added — 5 новых Ozon модулей (API Audit)

- **ozon_funnel_service.py:** Sales analytics via `/v1/analytics/data`
  - Метрики: ordered_units, revenue (13 метрик deprecated Ozon → Premium)
  - Backfill 365 дней: 3,634 rows, 1,743 заказа, 3.25M₽
  - ClickHouse: `fact_ozon_funnel` (ReplacingMergeTree)
  - Tasks: `sync_ozon_funnel`, `backfill_ozon_funnel`

- **ozon_returns_service.py:** Returns/cancellations via `/v1/returns/list`
  - Workaround: API баг last_id=0 → cursor через max(id) + dedup
  - Backfill: 229 returns (225 cancellations, 4 client), 427K₽
  - Top причина: «Покупатель отменил заказ» (60 из 229)
  - ClickHouse: `fact_ozon_returns`
  - Tasks: `sync_ozon_returns`, `backfill_ozon_returns`

- **ozon_warehouse_stocks_service.py:** Stock per warehouse via `/v2/analytics/stock_on_warehouses`
  - Snapshot: 266 rows, 38 SKUs, 23 склада, 2,481 шт. free-to-sell
  - ClickHouse: `fact_ozon_warehouse_stocks`
  - Task: `sync_ozon_warehouse_stocks`

- **ozon_price_service.py:** Prices + commissions via `/v5/product/info/prices`
  - 40 товаров: FBO 34%, FBS 37%, эквайринг 4.38%, маркетинговые акции
  - ClickHouse: `fact_ozon_prices`
  - Task: `sync_ozon_prices`

- **ozon_seller_rating_service.py:** Account health via `/v1/rating/summary`
  - 10 метрик: Доставка, Жалобы, Индекс цен, Оценка 4.78
  - ClickHouse: `fact_ozon_seller_rating`
  - Task: `sync_ozon_seller_rating`

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

### Added — Расширение хранения данных товаров

- **dim_ozon_products (PostgreSQL):** +18 колонок — `model_id`, `model_count`, `price_index_color`, `price_index_value`, `competitor_min_price`, `vat`, `type_id`, `status`, `moderate_status`, `status_name`, `all_images_json`, `images_hash`, `primary_image_url`, `availability`, `availability_source`, `created_at_ozon`, `updated_at_ozon`, `is_kgt`
- **upsert_ozon_products:** 36 полей INSERT + images_hash change detection → `OZON_PHOTO_CHANGE` events
- **fact_ozon_promotions (ClickHouse):** ежедневные снэпшоты акций (promo_type + is_enabled)
- **fact_ozon_availability (ClickHouse):** ежедневные снэпшоты доступности (source + availability)
- **OzonPromotionsLoader + OzonAvailabilityLoader:** ClickHouse loaders
- **sync_ozon_product_snapshots:** единый Celery task → 1 API → 4 ClickHouse insert

### Added — Ozon Orders Loader (FBO & FBS)

- **ozon_orders_service.py:** `OzonOrdersService` — загрузка заказов FBO (`/v2/posting/fbo/list`) + FBS (`/v3/posting/fbs/list`) с пагинацией
- **fact_ozon_orders (ClickHouse):** `ReplacingMergeTree` — 30 колонок (posting_number, order_id, status, sku, price, commission, payout, city, cluster, warehouse_mode и тд)
- **OzonOrdersLoader:** ClickHouse загрузчик с batch insert + stats
- **\_normalize_postings():** нормализация FBO/FBS → unified rows (1 row per product per posting), обработка различий форматов
- **sync_ozon_orders:** Celery task — синхронизация за последние 14 дней (overlap window для отлова смен статусов)
- **backfill_ozon_orders:** Celery task — историческая загрузка до 365 дней
- **Live тест:** 657 FBO + 11 FBS = 668 rows, payout 711K₽, dedup ✅

### Added — Ozon Finance Service (Transaction Stream)

- **ozon_finance_service.py:** `OzonFinanceService` — загрузка транзакций `POST /v3/finance/transaction/list` с пагинацией + auto-chunking по месяцам
- **fact_ozon_transactions (ClickHouse):** `ReplacingMergeTree` — 16 колонок (operation_id, type, amount, accruals_for_sale, sale_commission, services_total, category и тд)
- **OPERATION_CATEGORY_MAP:** маппинг 19 operation_type → 9 категорий (Revenue, Refund, Logistics, Marketing, Storage, Penalty, Acquiring, Compensation, Other)
- **OzonTransactionsLoader:** ClickHouse загрузчик + stats + `get_pnl()` для P&L отчёта
- **sync_ozon_finance:** Celery task — daily sync (2-дневное окно)
- **backfill_ozon_finance:** Celery task — историческая загрузка до 12 месяцев (по месяцам)
- **Live тест:** 13,384 операций за 10 месяцев, Revenue 4.4M₽, Net payout 2.75M₽

### Added — Комиссии + Контент-рейтинг (daily)

- **ozon_products_service.py:** `_extract_commissions()` — парсинг commissions из `/v3/product/info/list` → flat dict (sales_percent, FBO/FBS logistics fees)
- **ozon_products_service.py:** `OzonCommissionsLoader` → ClickHouse `fact_ozon_commissions` (ReplacingMergeTree, daily snapshots)
- **ozon_products_service.py:** `fetch_content_ratings(skus)` — POST `/v1/product/rating-by-sku` (контент-рейтинг 0-100 + группы media/text/attributes)
- **ozon_products_service.py:** `OzonContentRatingLoader` → ClickHouse `fact_ozon_content_rating` (ReplacingMergeTree, daily snapshots)
- **tasks.py:** `sync_ozon_commissions` — Celery task, раз в сутки (06:00)
- **tasks.py:** `sync_ozon_content_rating` — Celery task, раз в сутки (06:30)
- **celery.py:** Beat schedule шаблоны для комиссий, рейтинга и inventory (каждые 4ч)

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
