## 2026-04-01 (v17.26.0)

### feat(normquery): UWB Search Cluster Analytics — полный backend pipeline

**Новая аналитика для ручных рекламных кампаний WB (UWB)** — поисковые кластеры (normquery), ставки по кластерам, рекомендации.

**ClickHouse** (`009_normquery_stats.sql`):
- `fact_normquery_stats_daily` — ежедневная статистика по кластерам (views, clicks, orders, CTR, CPC, avg_pos)
- `log_normquery_bids` — лог ставок по кластерам с рекомендованными ставками рынка

**Backend — новый сервис** (`wb_normquery_service.py`):
- WBNormqueryService — все READ/WRITE методы для normquery API WB:
  - `get_normquery_stats()` / `get_normquery_daily_stats()` — статистика кластеров (агрегированная / дневная)
  - `get_normquery_bids()` — текущие ставки по кластерам
  - `get_normquery_list()` — списки активных/исключённых кластеров
  - `get_normquery_minus()` — минус-фразы
  - `get_bid_recommendations()` — рекомендованные ставки рынка (fix: camelCase params `advertId`/`nmId`)
  - `set_normquery_bids()` / `set_minus_phrases()` — WRITE операции (Phase 3)

**Backend — новый endpoint** (`campaign_details.py`):
- `GET /campaign-details/wb/{campaign_id}/normquery-analytics` — полная аналитика по кластерам:
  - Параллельный запрос 4 WB API (stats + bids + minus + list) + bid recommendations
  - Per-cluster: views, clicks, orders, CTR, CPC, avg_pos, CR → order/cart
  - Текущая ставка (копейки + рубли) + рекомендованные ставки (reach max/med/min)
  - Исключённые кластеры + минус-фразы
  - Base bids (competitive + leaders)

**Celery** (`tasks.py`):
- `sync_normquery_data` — фоновый сбор normquery данных в ClickHouse:
  - Автоматически находит UWB кампании (bid_type=manual, payment_type=cpm, status IN 9,11)
  - Собирает daily stats за 3 дня + текущие ставки + рекомендации
  - Записывает в `fact_normquery_stats_daily` + `log_normquery_bids`
  - Интегрирован в `sync_all_frequent` с 6h TTL dedup
  - 2с пауза между кампаниями для rate limiting

**Верификация на живых данных:**
- Campaign 35063966: 9 кластеров, 3 excluded, base competitive=1500₽, leaders=3022₽
- Campaign 34609330: 17 кластеров, 20 excluded, 20 минус-фраз
- Время ответа: ~5.7с (4 параллельных WB API)

**Файлы:**
- `docker/clickhouse/migrations/009_normquery_stats.sql` — 2 новые таблицы
- `backend/app/services/wb_normquery_service.py` — сервис (~500 строк)
- `backend/app/api/v1/campaign_details.py` — endpoint (+200 строк)
- `backend/celery_app/tasks/tasks.py` — sync task + coordinator integration (+250 строк)

---

## 2026-03-31 (v17.25.2)

### fix(bids): Ставки пропадали из-за нулевых записей WB API

**Проблема**: В раскрытых строках кампаний ставки показывались как 0 (бейджи не отображались), хотя в ClickHouse реальные ставки были записаны ранее.

**Причина — WB API «storm garbage»:**
- WB API периодически возвращает `bids_kopecks: {search: 0, recommendations: 0}` — мусорные данные
- `extract_bid_snapshot_v2()` записывал эти нули в `log_wb_bids` без фильтрации
- SQL-запрос `argMax(bid_search, timestamp)` брал последнюю по времени строку → 0

**Решение — два уровня защиты:**
1. **Запись (event_detector.py):** `extract_bid_snapshot_v2()` теперь пропускает строки с `bid_search=0 AND bid_recommendations=0`
2. **Чтение (ad_management.py):** SQL-запрос использует `argMaxIf(..., bid_search > 0 OR bid_recommendations > 0)` + `HAVING` — игнорирует уже записанный мусор

**Результат:** 95 из 96 кампаний теперь отображают ставки (было: 0).

**Файлы:**
- `backend/app/services/event_detector.py` — фильтр нулей при записи
- `backend/app/api/v1/ad_management.py` — `argMaxIf` при чтении

---

## 2026-03-31 (v17.25.1)

### feat(ad-management): Двухстрочная карточка товара + умное отображение ставок

**Блок «Товары и ставки» в раскрытой строке кампании — UI/UX оптимизация:**

**Двухстрочная карточка:**
- **Строка 1**: Название товара + ставка → ключевая информация сразу видна
- **Строка 2**: Артикул (vendor_code бейдж) + #nm_id → технические идентификаторы
- Ранее: всё в одну строку, артикул и ID раздували ширину

**Умное отображение ставок (unified vs manual):**
- `bid_type=unified` (автоматическая кампания) → **одна фиолетовая ставка** (search == recommendations)
- `bid_type=manual` (ручная кампания) → **две раздельные ставки**: 🔍 Поиск + 📦 Полки
- Если ставок нет (0) → бейджи не показываются
- Ранее: всегда показывались две одинаковые ставки для unified кампаний

**Обогащение данных (из предыдущего коммита):**
- Backend: PostgreSQL JOIN с `dim_products` → `product_name` и `vendor_code` в каждом nm_setting
- Ранее: только числовой `nm_id` без понятного названия товара

**Ограничение ширины:**
- `max-w-[800px]` — карточки не раздуваются на всю ширину горизонтально скроллируемой таблицы

**Файлы:**
- `backend/app/api/v1/ad_management.py` — PostgreSQL enrichment nm_settings
- `backend/app/schemas/ad_management.py` — +product_name, +vendor_code в NmSettingResponse
- `frontend/src/api/ad-management.ts` — обновлён NmSetting интерфейс
- `frontend/src/pages/AdManagementPage.tsx` — двухстрочная карточка + smart bid display

---

## 2026-03-31 (v17.25.0)

### perf(ad-management): 0 WB API запросов при загрузке страницы — полный переход на БД

**Проблема**: Страница «Управление рекламой» при каждом открытии дёргала 4-6 WB API запросов (campaigns, bids, balance, budgets×N), из-за чего:
- Загрузка занимала **15-20 секунд**
- WB API возвращал 429 Rate Limit → бюджеты = 0
- При переключении магазина — ещё 15с ожидания

**Решение — полный переход на данные из БД:**

| Данные | Было | Стало |
|--------|------|-------|
| Имена/статусы/типы | WB API (2+ запроса) | `dim_advert_campaigns` (ClickHouse) |
| Ставки (bids) | WB API | `log_wb_bids` (ClickHouse) |
| Статистика | ClickHouse ✅ | Без изменений |
| Бюджеты | WB API (N запросов) | Redis кеш (Celery каждые 15 мин) |
| Баланс | WB API | Redis кеш (Celery каждые 15 мин) |

**Backend**:
- Новый endpoint `GET /campaigns/from-db` — **0 WB API запросов**, всё из ClickHouse + Redis
- Celery-задача `sync_wb_budgets` — синхронизирует бюджеты + баланс в Redis (TTL 20 мин)
- Celery-задача `sync_all_budgets` — диспатчер для всех WB-магазинов
- Beat schedule: каждые 15 минут
- Fix: корректная обработка ClickHouse Enum8 типов кампаний (строка → int mapping)

**Frontend**:
- `getCampaignsFromDB()` заменяет `getEnrichedCampaigns()` — один endpoint с полными данными
- Удалён `loadBudgets()` — бюджеты приходят в основном ответе из Redis-кеша
- Попап пополнения бюджета по-прежнему дёргает WB API напрямую (актуальные данные)

**Результат**: Страница грузится за **< 1 секунду** вместо 15-20с. **100% WB API запросов** убрано из загрузки.

**Файлы**:
- `backend/app/api/v1/ad_management.py` — новый endpoint `/from-db`
- `backend/celery_app/tasks/tasks.py` — `sync_wb_budgets`, `sync_all_budgets`
- `backend/celery_app/celery.py` — routing + schedule
- `frontend/src/api/ad-management.ts` — `getCampaignsFromDB()`
- `frontend/src/pages/AdManagementPage.tsx` — рефакторинг загрузки

---

## 2026-03-31 (v17.24.3)

### perf(ad-management): Мгновенное переключение периодов — разделение WB API и ClickHouse

**Проблема**: Переключение периода (7д↔30д) занимало **15-20 секунд**, потому что КАЖДЫЙ раз дёргались 4+ WB API endpoints (count, v2/adverts×N, balance), хотя названия/статусы/ставки кампаний **не зависят** от выбранного периода. Бюджеты также каждый раз перезапрашивались, часто возвращались нулями из-за rate-limit'а.

**Решение — разделение потоков данных:**

| Поток | Источник | Когда | Скорость |
|-------|----------|-------|----------|
| Первичная загрузка | WB API + ClickHouse | Вход на страницу/смена магазина | 10-15с |
| Смена периода | **Только ClickHouse** | Клик на 7д/30д/даты | **<1с** |
| Бюджеты | WB API + Redis cache | **Только при первичной загрузке** | ~3с |

**Backend**: новый endpoint `GET /campaigns/stats` — возвращает статистику per campaign_id + KPI + deltas для периода. **0 запросов к WB API**.

**Frontend**: `loadData()` разделена на:
- `loadFullData()` — первичная (WB API enriched + бюджеты)  
- `refreshStats()` — при смене периода (ClickHouse stats only, мерж с кешированными WB-данными)

**Результат**: WB API запросов снижено на ~90% при активной работе пользователя.

**Файлы**: `ad_management.py` (+endpoint), `ad-management.ts` (+getCampaignStats), `AdManagementPage.tsx` (рефакторинг загрузки).

---



### fix(marketplace-client): Graceful degradation при падении Redis-соединения

**Проблема**: Раздел «Управление рекламой» (WB) периодически показывал «Ошибка загрузки данных» вместо данных. Работало после 1-3 обновлений страницы.

**Причина**: `MarketplaceClient.request()` вызывал `wait_for_rate_limit()` → Redis `ping()` → `RedisError: Buffer is closed.` / `ConnectionError: Connection closed by server`. Исключение не перехватывалось → 500 → фронтенд показывал ошибку.

**Решение**: Graceful degradation в `MarketplaceClient`:
- `__aenter__` (circuit breaker check): try/except → если Redis упал, пропускаем проверку
- `request()` (rate limiter): try/except → если Redis упал, запрос идёт без лимита
- `request()` (get_wait_time при 429): try/except → fallback на 2s ожидание  
- `_make_request()` (report_429/report_success): try/except → метрики не пишутся, запрос не ломается

**Логика**: лучше сделать запрос к API без rate limiting, чем вернуть 500 пользователю.

**Файлы**: `marketplace_client.py` (строки 153-165, 285-330, 380-410).

---

## 2026-03-31 (v17.24.1)

### fix(campaign-details): Ozon-попап игнорировал model-заказы и model-выручку

**Проблема**: В модальном попапе кампании (клик по кампании → блок «Вся кампания») для Ozon показывались **заниженные** метрики:
- Заказы: **5** вместо **9** (не учитывались `model_orders`)
- Выручка: **32 900 ₽** вместо **54 715 ₽** (не учитывалась `model_revenue`)
- ДРР: **67.6%** вместо **40.6%** (считался от прямой выручки)

При этом в таблице кампаний и в Ozon Seller данные были корректными.

**Причина (2 уровня)**:
1. **Backend** (`campaign_details.py`): SQL-запросы KPI и stats для Ozon использовали `sum(orders)` и `sum(revenue)` — только прямые продажи. Колонки `model_orders` и `model_revenue` из `fact_ozon_ad_daily` игнорировались.
2. **Frontend** (`CampaignDetailModal.tsx`): блок «Вся кампания» брал `direct_orders + model_orders + associated_orders` из breakdown, который для Ozon не вычисляется (всегда 0).

**Решение**:
- **Backend**: KPI-запрос (`build_ad_query`) и stats-запрос — `sum(orders) + sum(model_orders)`, `sum(revenue) + sum(model_revenue)`. ДРР считается от суммарной выручки.
- **Frontend**: fallback на `cur.orders` / `cur.ad_revenue` когда breakdown-поля = 0 (Ozon без breakdown).

**Верификация** (campaign 19641608, 25-31.03):
| Метрика | Ozon Seller | Наша таблица | Попап (после fix) |
|---------|------------|-------------|-------------------|
| Заказы  | —          | 9           | **9** ✅           |
| Выручка | 54 715 ₽   | 54 715 ₽    | **54 715 ₽** ✅    |
| ДРР     | 42,5%      | 40,0%       | **40,6%** ✅       |
| Расход  | 23 274 ₽   | 22 237 ₽    | **22 237 ₽**      |

> Расхождения Ozon Seller vs наша система (1 037 ₽ расход, 539 показов) — задержка синхронизации данных за 31.03.

**Файлы**: `campaign_details.py` (строки 215-222, 429-446), `CampaignDetailModal.tsx` (строки 550-553).


---

## 2026-03-31 (v17.24.0)

### feat(ad-management): Sticky колонки + batch-бюджеты + Redis resilience

**Sticky Header + Columns (горизонтальный/вертикальный скролл):**
- **Шапка таблицы** закреплена сверху при вертикальном скролле (`position: sticky; top: 0; z-index: 20`)
- **3 первых столбца** закреплены слева при горизонтальном скролле:
  - Чекбокс (`left: 0`) → Кнопка действия (`left: 40px`) → Название кампании (`left: 80px`)
- Непрозрачный фон `bg-[hsl(var(--card))]` + `group-hover` для корректного hover-эффекта
- Тень-разделитель `boxShadow: 2px 0 8px -2px rgba(0,0,0,0.15)` на последнем sticky-столбце
- z-index иерархия: обычные sticky (10) → header sticky (20) → угловые sticky (30)

**Batch-загрузка бюджетов:**
- Новый эндпоинт `POST /ad-management/wb/budgets/batch` — один запрос для всех кампаний
- Redis-кеш 60s TTL для предотвращения Thundering Herd
- `asyncio.Semaphore(5)` для rate-limiting запросов к WB API
- Frontend: `getBudgetsBatch()` → единый вызов вместо N отдельных запросов

**Redis Resilience (graceful degradation):**
- Batch endpoint: `ping()` перед использованием, `socket_connect_timeout=3`, `retry_on_timeout=True`
- **Graceful degradation**: если Redis недоступен → все данные загружаются с WB API напрямую
- `aclose()` вместо `close()` для корректного закрытия async-клиента
- Ошибки кеширования не блокируют ответ — пользователь всегда получает данные

**Файлы:**
- `frontend/src/pages/AdManagementPage.tsx` — sticky columns/header, batch budgets UI
- `frontend/src/api/ad-management.ts` — `getBudgetsBatch()` функция
- `backend/app/api/v1/ad_management.py` — batch endpoint с Redis resilience
- `backend/app/core/rate_limiter.py` — `ping()` health check

---





### feat(ad-management): Полная переработка интерфейса управления рекламой WB (v3)

**Перенос и логика действий:**
- **Play/Pause перенесены в первый столбец** (после чекбокса) — сразу доступны
- **Одна кнопка**: Активна → ⏸ Пауза, На паузе / Готова к запуску → ▶ Play. Кнопка ⏹ Stop убрана из строк

**Устранение дублирования типа:**
- Тип кампании показывается **только бейджем** (CPM/CPC/CPA) рядом с названием
- Удалён текст "CPM ручная" из подстроки — было двойное отображение

**Расширение названия кампании:**
- Убрана колонка бюджета (всегда 0, бесполезна без batch-fetch) → место для названия
- Название теперь **не обрезается**, показывается полностью
- Подстрока: ID · 🔍 Поиск · 📦 Полки

**PeriodSelector (стандартный компонент):**
- Заменены 5 кнопок периодов (Сегодня/7/14/30/90) на стандартный `PeriodSelector` с `7д | 30д | 📅 Даты`
- Добавлен **календарь произвольных дат** с поддержкой custom date range

**Стилизация фильтров:**
- Фильтр типов (CPM/CPC/CPA) — кастомный styled dropdown вместо нативного `<select>`
- Фильтр статусов — с цветовыми точками и количеством кампаний

**Затраты и данные:**
- Колонка "Затраты" теперь корректно показывает данные: 650K, 172K, 139K ₽ etc
- Все столбцы сортируются по клику на заголовок

**Файл:** `frontend/src/pages/AdManagementPage.tsx` (полная перезапись ~560 строк)

---



### fix(ad-management): Светлая тема, названия кампаний, массовое пополнение бюджета

**Исправления UX/UI:**
- **Светлая тема** — все элементы страницы (KPI карточки, таблица, фильтры, модалы, expanded rows) теперь используют CSS-переменные (`--card`, `--border`, `--foreground`, `--muted-foreground`) вместо жёстких zinc-цветов
- **Названия кампаний** — первая строка: НАЗВАНИЕ, вторая строка: ID · тип (вместо "ID — CPM авто" было)
- **Массовое пополнение бюджета** — при выборе нескольких кампаний в batch actions bar добавлена кнопка «Пополнить бюджет» → инлайн-модал с полем суммы
- **"Ошибка загрузки"** — ошибка теперь показывается ТОЛЬКО если данные не загрузились (`error && !data`), а не после временного сбоя при наличии данных; ошибка сбрасывается при успешной загрузке

---



### feat(ad-management): Редизайн управления рекламой WB — Итерация 2

**Полное обновление интерфейса управления рекламными кампаниями Wildberries** — данные управления объединены со статистикой из ClickHouse.

**Backend**:
- **Новый endpoint** `GET /ad-management/wb/campaigns/enriched`:
  - Объединяет данные WB API (статус, ставки) с ClickHouse (`fact_advert_stats_v3`)
  - Агрегированные KPI за выбранный период: показы, клики, CTR, расход, продажи, ДРР
  - KPI дельты — сравнение с предыдущим аналогичным периодом
  - Расчёт: CPC, CPM, CPA (корзина), CPO на уровне кампании
  - Поддержка периодов: today, 7d, 14d, 30d, 90d + custom date_from/date_to
- **Новый endpoint** `GET /ad-management/wb/budget` — бюджет конкретной кампании (WB API `/adv/v1/budget`)
- **Новый endpoint** `POST /ad-management/wb/budget/deposit` — пополнение бюджета кампании (WB API `/adv/v1/budget/deposit`)
- `WBAdManagementService` расширен:
  - `get_campaign_budget()` — получение бюджета
  - `deposit_budget()` — пополнение с аудитом
  - `get_campaigns_budgets()` — пакетное получение (семафор: max 5)

**Frontend** (`AdManagementPage.tsx` — полный редизайн):
- **6 KPI карточек**: Показы, Клики, CTR, Расход, Продажи, ДРР — с дельтами (%, ↗/↘)
- **Панель фильтров**: поиск (ID/название/артикул), выбор периода, мультиселект статусов, фильтр типа (CPM/CPC/CPA)
- **Сортируемая таблица**: 16 колонок — Кампания, Статус, Бюджет, Затраты, Продажи, Показы, Клики, CTR, Корзины, Заказы, ДРР, CPC, CPM, CPA Корзина, CPO, Действия
- **Бейджи**: CPM авто/ручн., CPC ручн., 🔍 Поиск, 📦 Полки
- **Попап пополнения бюджета**: клик по «+ 0 ₽» → модал с текущим бюджетом, единым счётом, полем суммы, кнопка "Пополнить"
- **Batch actions**: выбор нескольких кампаний → массовый запуск/пауза
- **ДРР цветовая индикация**: зелёный (<10%), жёлтый (10-20%), красный (>20%)
- **Раскрытие строки**: ставки по артикулам (nm_id → Поиск/Полки в ₽)

**API клиент** (`ad-management.ts`):
- `getEnrichedCampaigns()` — enriched endpoint
- `getCampaignBudget()` / `depositBudget()` — работа с бюджетом
- `formatMoney()` / `formatNum()` — хелперы форматирования

**Файлы изменены**:
- `backend/app/services/wb_ad_management_service.py` — +3 метода (budget)
- `backend/app/api/v1/ad_management.py` — +3 endpoint (enriched, budget, deposit)
- `frontend/src/pages/AdManagementPage.tsx` — полный редизайн (~940 строк)
- `frontend/src/api/ad-management.ts` — расширен (enriched, budget, helpers)

---



### feat(ad-management): Управление рекламными кампаниями WB — Итерация 1

**Новый раздел «Управление рекламой»** — полноценный CRUD для рекламных кампаний Wildberries.

**Backend (уже существовал, подключён)**:
- `WBAdManagementService` (`wb_ad_management_service.py`) — обёртка WB API через MarketplaceClient:
  - Start/Pause/Stop кампаний (`/adv/v0/start|pause|stop`)
  - Change bids per nm_id (`PATCH /api/advert/v1/bids`) — ставки в копейках
  - Get campaigns with bids (count + v2/adverts)
  - Get balance (`/adv/v1/balance`)
- `ad_management.py` роутер — 9 endpoints:
  - `GET /ad-management/wb/campaigns` — список кампаний с текущими ставками
  - `POST /campaigns/start|pause|stop` — управление статусом
  - `POST /bids/change` — изменение ставок
  - `POST /status/batch` — массовые old/pause (до 50, с 2с задержкой)
  - `GET /balance` — баланс рекламного кабинета
  - `GET /audit-log` — журнал действий
- `AdAuditLog` — SQLAlchemy модель + миграция Alembic
- **FIX: роутер зарегистрирован в `router.py`** (ранее код существовал, но endpoints были 404)

**Frontend**:
- `AdManagementPage.tsx` — полная страница управления:
  - Balance strip: Счёт, Баланс (нетт.), кол-во активных кампаний
  - Фильтры статуса: Все / Активные / На паузе / Готовы / Завершённые
  - Поиск по названию, ID или артикулу
  - Таблица кампаний: название, статус (бейджи), тип (Ручная/Единая + CPM/CPC), размещение (🔍Поиск / 📦Полки), товары, ставки
  - Раскрытие строки → детальные ставки по nm_id с категориями
  - Кнопки действий: ▶️ Старт, ⏸ Пауза, ⏹ Стоп (с confirm)
  - Чекбоксы + массовые действия (batch start/pause)
  - Error/Loading/Empty states
  - Non-WB shop warning
- `ad-management.ts` — API клиент (уже существовал)
- Route: `/advertising/management` в `App.tsx`
- Sidebar: «Управление → Реклама → Управление» → `/advertising/management`

**Файлы**: `router.py`, `AdManagementPage.tsx`, `App.tsx`, `Sidebar.tsx`.

---



### feat(storage): Excel-экспорт хранения для Ozon

**Задача**: На странице «Хранение» кнопка Excel работала только для WB, у Ozon выгрузки не было.

**Решение**:
- **Backend** (`warehouses.py`): новый endpoint `GET /ozon/storage/export` — генерирует Excel из 3 листов:
  - «Хранение по SKU» — артикул, остаток, оборачиваемость, зона хранения, прогноз 30д
  - «Детализация по складам» — разбивка по складам с остатком и резервом
  - «ИИ-рекомендации» — если есть кеш ИИ-анализа
- **Frontend** (`warehouses.ts`): `downloadStorageExcel()` теперь универсальная — принимает `marketplace` и маршрутизирует на правильный endpoint
- **Frontend** (`WarehousesStoragePage.tsx`): кнопка Excel видна для обоих маркетплейсов

**Файлы**: `warehouses.py`, `warehouses.ts`, `WarehousesStoragePage.tsx`.

---

## 2026-03-30 (v17.20.5)

### fix(finances): Строка «Итого к оплате» в WB Excel-отчёте

**Проблема**: В водопаде на UI показывалась строка «Итого к оплате» (сумма, которую ВБ перечисляет на р/с продавца), но в Excel-сводке эта строка отсутствовала.

**Решение**: Значение `bank_cur` (payout − логистика − хранение − приёмка − удержания − ВБ промо − штрафы) уже рассчитывалось для прибыли, но не отображалось. Добавлена строка «Итого к оплате (на р/с)» в секцию ИТОГИ между «К перечислению» и «Себестоимость».

**Файлы**: `finances_export.py` (1 строка).

---

## 2026-03-30 (v17.20.4)

### fix(storage): Таблица SKU и Excel показывают хранение за выбранный период

**Проблема**: KPI-карточка показывала факт хранения за выбранный период (7/14/30д), но таблица по SKU и Excel-отчёт всегда экстраполировали на 30 дней (`est_cost_month`).

**Решение**:
- **Backend** (`warehouses.py`): добавлено поле `est_cost_period` — фактическая стоимость хранения за выбранный период (пропорционально `est_cost_month * period / 30`)
- **Frontend** (`WBWarehouseAnalyticsContent.tsx`): таблица показывает `est_cost_period`, заголовок колонки динамический («Хранение/7д», «Хранение/30д» и т.д.)
- **Frontend** (`WarehousesStoragePage.tsx`): передаёт `periodDays={period}` в таблицу
- **Excel**: заголовок колонки и данные соответствуют выбранному периоду

**Файлы**: `warehouses.py`, `WBWarehouseAnalyticsContent.tsx`, `WarehousesStoragePage.tsx`.

---



### fix(frontend): ИИ-анализ в попапе кампании скрыт по умолчанию

**Проблема**: При открытии попапа кампании с кешированным ИИ-анализом панель автоматически раскрывалась и занимала место, мешая просмотру данных.

**Решение**: Убран `setShowAiPanel(true)` из эффекта загрузки localStorage кеша. Кешированный текст загружается, но панель остаётся свёрнутой — кнопка «Показать анализ» готова к раскрытию по клику.

**Файлы**: `CampaignDetailModal.tsx` (1 строка).

---

## 2026-03-30 (v17.20.2)

### feat(frontend): KPI-карточки кампаний — дельты и новые метрики

**Было**: 4 карточки (Расход, Показы, Заказы, ДРР) без сравнения.

**Стало**: 6 карточек (+ Корзины, Выручка) с дельтами к предыдущему периоду:
- Зелёный = позитивно (рост показов/заказов/выручки, снижение ДРР)
- Красный = негативно
- ДРР инвертирован: снижение = хорошо (зелёный)

**Файлы**: `AdvertisingCampaignsPage.tsx` (SummaryStrip).

---

## 2026-03-30 (v17.20.1)

### fix(frontend): Фильтр статусов кампаний per-marketplace + скрытие нулевых

**Проблема 1**: localStorage-ключ `ad_status_filter` был общий для всех магазинов. Фильтр WB-статусов (`Активна`) не находился среди Ozon-статусов (`CAMPAIGN_STATE_RUNNING`), поэтому Ozon-кампании не отображались.

**Решение**: Ключ теперь `ad_status_filter_${marketplace}` — у WB и Ozon раздельные фильтры. При смене магазина фильтр сбрасывается/загружается из правильного ключа.

**Проблема 2**: Кампании с нулевыми показателями (spend=0, views=0, clicks=0, orders=0) засоряли список.

**Решение**: Добавлен фильтр — кампании без активности за выбранный период автоматически скрываются.

**Файлы**: `AdvertisingAnalyticsPage.tsx` (CampaignsTable).

---

## 2026-03-30 (v17.20.0)

### refactor(frontend): Вынос таблицы кампаний на отдельную страницу

**Проблема**: При фильтрации/поиске на странице обзора рекламы (`/advertising/analytics`) экран «прыгал» — scroll сбрасывался вверх, потому что таблица кампаний (800+ строк кода) и графики были на одной странице, а при `loading` DOM заменялся на skeleton.

**Решение — вариант Б (отдельная страница)**:

**Новая страница** (`/advertising/campaigns` → `AdvertisingCampaignsPage.tsx`):
- Полностью самостоятельная страница с собственным API-вызовом (`getAdvertisingAnalytics`)
- Собственный PeriodSelector, KPI-полоска, полная CampaignsTable
- Overlay loader вместо Skeleton (DOM не пересоздаётся → scroll не сбрасывается)
- Кнопка «назад» (←) ведёт на обзорную страницу

**Обзорная страница** (`/advertising/analytics` → `AdvertisingAnalyticsPage.tsx`):
- Удалена секция CampaignsTable — вместо неё карточка-ссылка «Кампании за период» со счётчиком
- `if (loading)` → `if (loading && !data)` — при повторной загрузке данные не исчезают
- Overlay loader поверх контента при обновлении данных
- Экспортированы `CampaignsTable`, `PeriodSelector`, `formatMoney`, `formatNumber` для реюзабилити

**Sidebar** (`Sidebar.tsx`):
- Секция «Аналитика → Реклама» теперь содержит: **Обзор** + **Кампании**
- Секция «Управление → Реклама» осталась: **Управление** + **Автобиддер**

---

## 2026-03-27 (v17.19.7)

### fix(events): Ставки WB в попапе событий — копейки→рубли

**Проблема**: В popup событий на странице рекламной аналитики (`/advertising-analytics/events-detail`) ставки WB показывались в копейках (например, «110000 ₽ → 150000 ₽» вместо «1100 ₽ → 1500 ₽»).

**Backend** (`advertising_analytics.py`):
- `events-detail` endpoint: для `BID_CHANGE` (WB) — деление old/new_value на 100 перед форматированием
- `OZON_BID_CHANGE` — оставлен как есть (значения уже в рублях)
- Формат ставок изменён с `:.2f` на `:.0f` (целые рубли)
- Добавлен префикс «Ставки:» для событий без bid_field

> **Примечание**: В ленте событий (`events.py` → `_format_value()`) конвертация уже была реализована корректно.

---

## 2026-03-27 (v17.19.6)

### fix(ai): Retry логика для Gemini API + увеличен read timeout

**Проблема**: Периодические 429/503 ошибки и таймауты при ИИ-анализе кампаний — первый запрос часто фейлился, повторный — проходил.

**Backend** (`campaign_ai_analysis.py`):
- **Retry**: до 2 retries с exponential backoff (2с→4с) на HTTP 429 (Rate Limit), 503 (Server Overload), ReadTimeout, ConnectTimeout
- **Timeout**: decoupled — `connect=15s`, `read=170s` (было единый `timeout=120s`). Read timeout увеличен для long-thinking моделей (Gemini может «думать» до 120с перед первым chunk)
- **Async**: `asyncio.sleep()` для non-blocking retry delays в async SSE generator
- **Логирование**: retry attempt с причиной ошибки

---

## 2026-03-27 (v17.19.5)

### feat(ai): Fallback юнит-экономика из fact_orders_raw

**Проблема**: Для новых товаров или товаров без финансового отчёта WB `fact_finances` пуст — ИИ-анализ не получал данные по юнит-экономике.

**Backend** (`campaign_ai_analysis.py`):
- При пустом `fact_finances` → fallback на операционные заказы из `fact_orders_raw`
- Расчёт: `revenue_per_unit`, `estimated_commission` (27% от цены), `estimated_logistics` (15% от цены)
- Сводный P&L: `total_payout_est − total_cost − ad_spend = profit_after_ads`
- Данные помечены как «оценочные» в промпте для ИИ

---

## 2026-03-26 (v17.19.4)

### fix(ai): Себестоимость явно в P&L, стокауты помечены как региональные

**Backend** (`campaign_ai_analysis.py`):
- Себестоимость (COGS) теперь **явно** передаётся как отдельная строка в P&L для ИИ
- Стокауты в событиях помечаются как `[региональный]` для складских событий (не товар полностью закончился)
- Ставки WB конвертируются из копеек в рубли в данных для ИИ-промпта

---

## 2026-03-26 (v17.19.3)

### feat(ai): Глубокая переработка WB AI-анализа — таблицы, имя кампании, рубли

**Backend** (`campaign_ai_analysis.py`):
- Имя кампании включено в данные для ИИ (из `dim_advert_campaigns`)
- Ставки: конвертация копейки → рубли перед передачей в промпт
- **JSON-таблицы**: `unit_economics_table` (per-SKU экономика) и `pl_summary_table` (P&L) — структурированные данные вместо текста
- Убраны Ozon-специфичные термины из WB ветки (prod_orders, Halo, Price Index)
- Campaign-attributed данные: только direct/model/associated заказы кампании, не все продажи магазина

---

## 2026-03-26 (v17.19.2)

### fix(ai): ROOT CAUSE — WB промпт никогда не применялся

**Проблема**: При ИИ-анализе WB кампаний использовался Ozon-промпт. Причина: сравнение `marketplace == "wildberries"` при фактическом значении `"wb"`.

**Backend** (`campaign_ai_analysis.py`):
- Fix: `marketplace == "wildberries"` → `marketplace == "wb"`
- WB-кампании теперь анализируются с корректным `SYSTEM_PROMPT_WB`

---

## 2026-03-26 (v17.19.1)

### feat(ai): WB AI-анализ — campaign-attributed данные

**Backend** (`campaign_ai_analysis.py`):
- Данные кампании: только заказы, атрибутированные к рекламе (direct SKU + model + associated), вместо всех продаж магазина
- 3-уровневая классификация: direct (рекламируемые) / model (тот же imt_id) / associated (другие)
- Финансы WB per-SKU: revenue, payout, комиссия, логистика, хранение, эквайринг из `fact_finances`
- Ключевые фразы из `fact_advert_phrases_daily` (marketplace=1)

---

## 2026-03-26 (v17.19.0)

### feat(backend): Полноценный ИИ-анализ WB кампаний

**campaign_ai_analysis.py** — расширена WB-ветка (ранее пустая) для полноценного AI-анализа:

- **SYSTEM_PROMPT_WB**: WB-специфичные правила для Gemini — CPM в копейках, минус-фразы, типы размещения (Поиск/Рекомендации), СПП, 3 типа продаж (direct/model/associated через imt_id)
- **Финансы WB**: per-SKU revenue, payout, комиссия, логистика, хранение, эквайринг из `fact_finances`
- **Себестоимость**: из `product_costs` по vendor_code
- **3-уровневая классификация продаж**: direct (рекламируемые) / model (та же карточка imt_id) / associated
- **Ключевые фразы**: из `fact_advert_phrases_daily` (marketplace=1)
- **P&L summary**: payout - COGS - реклама = чистая прибыль
- **Промпт по маркетплейсу**: автоматический выбор SYSTEM_PROMPT vs SYSTEM_PROMPT_WB

---

## 2026-03-26 (v17.18.5)

### feat(frontend): Улучшения UI рекламной аналитики

**CampaignDetailModal.tsx — маркеры событий:**
- Маркеры событий (PRICE_CHANGE и др.) теперь видны на графике **для дат без рекламных данных** — создаётся пустая точка с нулевыми метриками + маркер 📌
- Ранее: маркеры показывались только для дат с `stats` (рекламная статистика), из-за чего свежие события были невидимы на графике

**CampaignDetailModal.tsx — z-index popup событий:**
- Клик по backdrop popup «События за дату» теперь закрывает **только popup событий**, не popup кампании
- Добавлен `e.stopPropagation()` в overlay (z-200) чтобы клик не проваливался к overlay кампании (z-100)

**AdvertisingAnalyticsPage.tsx — фильтр статуса кампании:**
- Новый **select-dropdown** «Статус» с чекбоксами для выбора отображаемых статусов (Активна, На паузе, Завершена, Готова и др.)
- **По умолчанию** скрыты: Завершена, Удалена, Отменена, CAMPAIGN_STATE_ARCHIVED
- Выбранные статусы **сохраняются в localStorage** (`ad_status_filter`)
- Кнопка сброса внутри дропдауна

**AdvertisingAnalyticsPage.tsx — первый столбец кампании:**
- **Тип кампании** увеличен: 13px `font-medium` (было 12px regular)
- **Плейсменты** (Поиск · Рекомендации) объединены с типом кампании в одну строку: `Единая · CPM · Поиск · Рекомендации`
- Убраны из строки ID/статус — логически сгруппированы с типом

---



### feat(advertising): Все кампании в таблице (включая без статистики)

**Проблема**: Таблица кампаний показывала только кампании с данными в `fact_*` таблицах за выбранный период. Кампании на паузе, новые или с нулевым расходом — не отображались.

**Решение** (`advertising_analytics.py`):
- WB: дополнительный запрос всех кампаний из `dim_advert_campaigns` (ClickHouse), кроме удалённых (status ≠ -1)
- Ozon: запрос из `dim_ozon_campaigns` (PostgreSQL), кроме архивных
- Кампании без данных за период добавляются с нулевой статистикой, но с полными метаданными

---

## 2026-03-25 (v17.18.3)

### feat(events): Тизер событий в тултипе + попап деталей по клику

**Проблема**: Тултип на графике показывал сырые хэши (Content-Length) для фото-событий. При большом количестве событий за день тултип переполнялся.

**Тултип — тизер** (`CampaignDetailModal.tsx`):
- Фото-события больше не показывают raw old_value → new_value (хэши)
- Только числовые события (ставка, цена) показывают компактно `500 → 700`
- Добавлена подсказка «Кликните для подробностей»
- Макс. 4 события в тизере (было 5)

**Попап деталей дня** (`CampaignDetailModal.tsx`):
- Клик на дату графика с событиями → модальный попап (z-200)
- Полные карточки событий: иконка, время, товар, тип изменения
- Фото-превью 64×64 для photo-событий
- Числовые изменения с дельтой %
- Текстовые old→new для статусов
- Скроллируемый список, backdrop blur, закрытие по X или клику за пределы

---

## 2026-03-24 (v17.18.2)

### feat(events): Превью фото в событиях кампании

**Проблема**: В попапе кампании события «Фото изменено» / «Фото добавлено» показывали сырые хэши (Content-Length fingerprint) вместо реальных фотографий.

**Backend** (`event_detector.py`):
- `detect_content_events()`: добавлен `main_image_url` в `event_metadata` для всех фото-событий (CONTENT_MAIN_PHOTO_CHANGED, CONTENT_PHOTO_ADDED/REMOVED/ORDER_CHANGED)

**Backend** (`campaign_details.py`):
- `CampaignEventRow` schema: новое поле `event_metadata: Optional[dict]`
- SQL-запрос events: добавлена колонка `event_metadata`
- `product_map`: обогащён `main_image_url` из `dim_products`/`dim_ozon_products`
- Fallback: если `event_metadata` не содержит `main_image_url`, берётся из `product_map`

**Frontend** (`CampaignDetailModal.tsx`):
- Фото-события показывают **thumbnail 64×64** текущего фото товара + описательный текст:
  - «Главное фото заменено»
  - «Добавлено фото: 5 → 6 шт.»
  - «Удалено фото: 6 → 5 шт.»
  - «Порядок/состав фото изменён (6 шт.)»
- Fallback placeholder (иконка Image) при отсутствии URL
- `event_metadata` добавлен в TS-интерфейс `CampaignEventRow`

---

## 2026-03-24 (v17.18.1)

### feat(advertising): KPI редизайн — 3 блока + CPM/CPC/CR

**UI полностью переделан**: вместо 6 отдельных карточек + scope toggle (Все/Прямые/Кросс) — **3 визуальных блока**:

**Блок 1 «Вся кампания»** (как в WB админке):
- 4 колонки: Показы/CPM, Клики/CTR/CPC, Заказы/CR/CPO, ДРР/Выручка/Расход
- Новые метрики: **CPM**, **CPC**, **CR** (вычисляются на frontend)

**Блок 2 «Прямой товар»** (зелёная рамка):
- 6 колонок: Выр. рекл. / Выр. общая / Заказы-Корзины / ДРР рекл. / ДРР общий / CPO
- Данные только по прямым SKU (views>0/clicks>0/spend>0)
- product_revenue = общая выручка прямых SKU из `fact_orders_raw`

**Блок 3 «Кросс-продажи»** (инфо):
- Модель (шт + выручка) / Ассоц. конверсии (шт + выручка)
- Без ДРР/CPO — расход не атрибутируется

**Backend** (`campaign_details.py`):
- `_build_wb_scope_filter()`: scope='all' = 'main' (direct SKU only)
- `_compute_sale_type_breakdown()` — imt_id-based классификация
- 6 breakdown полей в `KpiPeriod`
- `product_revenue` из `fact_orders_raw` для direct SKU

**Frontend** (`CampaignDetailModal.tsx`):
- Scope toggle UI удалён
- scope = const 'all' (= main на backend)
- 3-блочный лейаут KPI с CPM/CPC/CR

---

## 2026-03-23 (v17.18)

### feat(advertising): 3-уровневая классификация продаж в кампаниях (imt_id)

**Цель**: Разделить продажи в рекламных кампаниях WB на 3 типа:
- **direct** (Прямые) — непосредственно рекламируемые товары
- **model** (Модель) — товары из той же объединённой карточки (по `imt_id`)
- **associated** (Ассоц.) — ассоциированные конверсии (другие товары)

**Database** (`dim_products`):
- Новая колонка `imt_id` (BigInteger, nullable) — ID объединённой карточки WB
- Миграция Alembic: `2e105ad65fd8_add_imt_id_to_dim_products.py`
- Индекс: `ix_dim_products_imt_id`

**Backend** (`wb_content_service.py`):
- `fetch_all_cards()`: извлекает `imtID` из WB Content API
- `update_products_db()`: сохраняет `imt_id` в `dim_products`

**Backend** (`campaign_details.py`):
- `CampaignPurchaseRow.is_cross: bool` → `sale_type: str` (direct/model/associated)
- Функция `classify_sale()`: использует `imt_id` из `dim_products` для классификации
  - main SKU (views>0/clicks>0/spend>0) → `direct`
  - тот же `imt_id` что у main → `model`
  - другой `imt_id` или нет data → `associated`
- Graceful fallback: если `imt_id` не заполнен → `associated` (консервативно)
- Обогащение имён через `sku_imt_map` (один запрос вместо двух)

**Frontend** (`campaignDetails.ts`):
- `CampaignPurchaseRow.is_cross` → `sale_type: 'direct' | 'model' | 'associated'`

**Frontend** (`CampaignDetailModal.tsx`):
- Scope toggle: «Все продажи / Товары кампании / Кросс» → «Все / Прямые / Кросс»
- Таблица Покупки: новая колонка «Тип» с цветными бейджами:
  - 🟢 Прямые (emerald) — `direct`
  - 🔵 Модель (blue) — `model`
  - 🟡 Ассоц. (amber) — `associated`
- Summary breakdown: «Прямые 23 шт. • 64 039 ₽ | Ассоц. 856 шт. • 473 421 ₽»
- Колонка «Тип» показывается только при наличии нескольких типов

**Заполнение imt_id**: происходит автоматически при ежедневном `sync_product_content` (Celery heavy queue).

---

## 2026-03-23 (v17.17)

### feat(geography): Excel отчёт географии продаж + фикс AI timeout

**Backend** (`warehouses.py`):
- `_build_geo_excel()` — Excel-отчёт географии (4 листа):
  - **Сводка**: KPI + округа/кластеры с выручкой, заказами, стабильностью
  - **Регионы**: детализация по регионам/городам внутри округов
  - **Топ товары**: продукты с охватом округов/регионов
  - **ИИ-анализ**: diagnosis, рекомендации, инсайты (из кеша)
- `GET /wb/geography/excel` — скачивание WB
- `GET /ozon/geography/excel` — скачивание Ozon + нормализация данных (clusters→regions)
- **AI timeout**: 60с → 120с для всех geography AI endpoints (WB, Ozon, warehouse)

**Frontend** (`warehouses.ts`, `WarehousesGeographyPage.tsx`, `OzonGeographyPage.tsx`):
- `downloadGeoExcel()` — API функция скачивания
- Кнопка «📥 Скачать Excel» на страницах WB и Ozon географии

---

## 2026-03-23 (v17.16)

### feat(cross-ai-wb): ИИ-анализ кросс-логистики Wildberries

**Backend:**
- `POST /warehouses/wb/cross/ai-analysis` — Gemini 2.5 Flash
- Данные: `fact_orders_raw` × `WAREHOUSE_TO_OKRUG` × `fact_wb_warehouse_stocks` × `dim_products`
- Товары кормов/питания помечены `[КОРМ/ПИТАНИЕ]` в промпте
- ИИ знает про склады `: Питание` и рекомендует именно их для пищевой продукции
- Кэш 30 мин (`_ai_cache`)

**Frontend:**
- `getWbCrossAIAnalysis()` — native fetch (180с таймаут)
- `CrossAIInsight` — универсальный компонент (заменяет `OzonCrossAIInsight`)
- AI-анализ теперь показывается и для Ozon, и для WB

### fix(cross-map): Полные данные Ozon + WB

- Кросс-карта теперь включает склады с заказами без FBO-остатков
- Сортировка складов по убыванию заказов

## 2026-03-23 (v17.15)

### feat(cross-ai): Таймауты, auto-retry, таймер загрузки ИИ

**Frontend:**
- `getOzonCrossAIAnalysis()` — timeout увеличен до 150 сек (Gemini ~120с)
- Auto-retry до 2 попыток при timeout или 500-ошибке сервера
- Таймер ожидания с прогресс-баром: «N сек • анализ данных • формирование рекомендаций»

### feat(cross-excel): Улучшенная кросс-карта + ИИ-лист

**Backend:**
- Кросс-карта: добавлен столбец «Дом. регион», итого Локал./Кросс, итого по столбцам
- ИИ-анализ в Excel читается из кэша (фикс MissingGreenlet)
- 6 листов: Сводка, Склады, Кросс-карта, SKU, География, ИИ-анализ

## 2026-03-22 (v17.14)

### feat(cross): Подробный Excel экспорт кросс-логистики (WB + Ozon)

**Backend** (`warehouses.py`):
- `GET /warehouses/wb/cross/excel` и `GET /warehouses/ozon/cross/excel`
- `_build_cross_excel()` — нормализация данных Ozon (orders_period, stock_free, costs.crossdocking, cluster→region)
- **5 листов Excel:**
  - **Сводка** — KPI: кросс%, логистика, кросс-стоимость, проблемные SKU
  - **По складам** — все склады: заказы, локальные/кросс, логистика, выручка, оборачиваемость
  - **Кросс-карта** — матрица Склад × Регион с цветовой индикацией СВОЙ/КРОСС
  - **По товарам (SKU)** — артикул, остаток, кросс%, потери, откуда→куда, рекомендации по довозу
  - **География складов** — все направления доставки: склад → регион с типом и долей

**Frontend** (`warehouses.ts`, `WarehousesCrossPage.tsx`):
- `downloadCrossExcel()` — API функция скачивания
- Кнопка «📥 Скачать Excel» на странице кросс-логистики

### fix(ozon): storage-only rows — реальные названия товаров + себестоимость

- Названия товаров теперь берутся из `dim_ozon_products.name` (ранее: только категория из `fact_ozon_placement_cost`)
- Себестоимость storage-only строк подставляется из `product_costs`

---

## 2026-03-22 (v17.13.1)

### fix(ozon): Excel экспорт хранения — корректный per-SKU + storage-only строки

**Backend** (`ozon_finance_queries.py`):
- `get_placement_costs_by_sku()`: фильтр `dt BETWEEN d_start AND d_end` вместо `period_to = max(period_to)` — суммирует фактические дневные затраты за выбранный период с fallback на последний отчёт
- `get_sku_to_offer_map_ch()`: новая функция — маппинг `sku → offer_id` из `fact_ozon_orders` (ClickHouse) как fallback для `dim_ozon_products` (PostgreSQL). Исправлен SQL alias конфликт (`any(offer_id) AS oid` + `HAVING`)

**Backend** (`finances_export.py`):
- Per-SKU sheet: CH fallback маппинг обогащает `sku_to_offer` перед распределением хранения — покрытие ~88% вместо ~0%
- Storage-only rows: товары с хранением но без продаж — полноценные строки с названием товара (`fact_ozon_placement_cost`), SKU и штрихкодом (`dim_ozon_products`), все финансовые колонки отформатированы
- ИТОГО включает ВСЕ затраты на хранение (ранее: 20K → теперь: 117K)

---

## 2026-03-22 (v17.13)

### feat(campaign-ai): ИИ-анализ кампаний — retention, P&L fix, промпт Ozon

**Backend** (`campaign_ai_analysis.py`):
- **Retention per-SKU** из `fact_ozon_orders` за всё время: total_buyers, repeat_buyers, repeat_rate, avg_days_between, avg_ltv_repeat, avg_orders_per_buyer
- Данные передаются ИИ в секции «ДАННЫЕ ПО РЕТЕНШЕНУ И ПОВТОРНЫМ ПОКУПКАМ» для расчёта эффективного CAC
- **P&L**: переход на `fact_ozon_orders` для подсчёта заказов (было `fact_ozon_transactions`). Per-unit финансовые метрики — по-прежнему из `fact_ozon_transactions` (финансовая точность)
- **Per-SKU order counts** из `fact_ozon_orders` для multi-product кампаний
- **Pre-calculated P&L**: бэкенд считает total_payout, total_cost, profit before/after ads — ИИ использует готовые числа

**Системный промпт — критические исправления:**
- **Субсидии Ozon (СПП аналог)**: покупатель видит цену на 30-50% ниже «Цены до скидки». Пример: old_price=6000₽, покупатель видит 3050₽/2761₽ с Ozon Картой
- **min_price**: это порог автоакций Ozon, НЕ цена конкурента
- **Price Index**: считается от реальной цены покупателя (с субсидиями), не от «Ваша цена»
- **Правила логики**: снижение цены НЕ может убивать продажи (корреляция ≠ причинность)
- **Малая выборка**: при <30 заказах — оговорка о статистической незначимости
- **Минус-фразы**: полностью убраны из промпта (нет на Ozon)
- **Данные по фразам**: только views/clicks/CTR, НЕ orders (нет на Ozon)
- **Ozon цены**: правильная терминология — «Ваша цена» (payout), «Цена до скидки» (old_price), «Минимальная цена» (порог автоакций), «Цена для покупателя» (marketing_price)
- **division by zero**: `nullIf()` в ClickHouse для `accruals_for_sale`

**Frontend** (`CampaignDetailModal.tsx`):
- **UI действий стратегии**: исправлены нечитаемые цвета — `text-yellow-400` → `text-gray-800` на `bg-amber-100`. Тёмный текст на светлых цветных фонах
- Убрано `opacity-80` с value-текста
- Цветовые индикаторы кружков: `bg-amber-500` (medium), `bg-red-500` (high), `bg-blue-500` (low)

**Frontend** (`campaignDetails.ts`):
- `streamCampaignAiAnalysis()` — SSE-клиент для streaming AI analysis с обработкой chunks

---

## 2026-03-20 (v17.12)

### feat(advertising): Общая выручка + реальная дата запуска + DateRangePicker

**Backend** (`campaign_details.py`):
- Новое поле `product_revenue` в `CampaignStatsRow` — общая выручка товаров кампании
- Запрос ежедневных заказов из `fact_ozon_orders` / `fact_orders_raw` по SKU кампании
- SQL: `toDate(order_date)` для корректной группировки по дням
- Нормализация `datetime → date` через helper `_to_date()`

**Frontend** (`CampaignDetailModal.tsx`):
- Новая метрика **«Выручка общая»** на графике: голубая area-линия (`#06b6d4`) с gradient fill
- «Выручка» переименована в **«Выручка рекл.»**
- **Реальная дата запуска кампании** из `dim_ozon_campaigns` вместо первой даты периода
- **Стилизованный DateRangePicker** (inline ModalDatePicker) для произвольных дат
- **Вкладка «Ставки»**: визуализация изменений ставок CPC
- Увеличены шрифты дельт с 10px до 13px на KPI карточках
- Поиск по кампаниям (название, ID, SKU, product_id, артикул, название товара)
- Кампании в столбик, ID крупнее жирный, названия обрезаны

**Frontend** (`AdvertisingAnalyticsPage.tsx`):
- Кнопка «Статистика»: крупная иконка 📊 (32×32) справа от заголовка
- Режим «По товарам» в таблице кампаний

---

## 2026-03-19 (v17.11.5)

### feat(campaign-details): Новый роутер детальной аналитики + поисковые фразы + справочник кампаний

**Backend — Новый роутер** (`campaign_details.py`, 758 строк):
- 6 endpoint'ов: `/kpi`, `/stats`, `/events`, `/phrases`, `/heatmap`, `/purchases`
- Универсальный для Ozon и WB через `{marketplace}` параметр
- **KPI**: ad stats + product revenue из `fact_ozon_orders`/`fact_orders_raw`, дельты с предыдущим периодом
- **Stats**: merge рекламных данных + product revenue per day, `toDate()` группировка
- **Events**: конвертация Ozon `sku` → `product_id` для `event_log`, enrichment с названиями из PG
- **Phrases**: агрегация из `fact_advert_phrases_daily` (Enum8: 1=WB, 2=Ozon)
- **Heatmap**: заказы по `toDayOfWeek()` × `toHour()`
- **Purchases**: фактические покупки SKU кампании с enrichment

**Backend — Новые таблицы PostgreSQL:**
- `dim_ozon_campaigns`: справочник кампаний Ozon (title, campaign_type, state, daily_budget, payment_type)
- `dim_ozon_campaign_products`: товары и ставки per-SKU в кампаниях (bid в рублях)
- Миграция Alembic: `b81f3ce45f30_add_dim_ozon_campaign.py`
- Модель: `app/models/dim_ozon_campaigns.py` (DimOzonCampaign + DimOzonCampaignProduct)
- `campaign_type` расшифровывается из `advObjectType` + `placement` + `productCampaignMode` + `PaymentType`

**Backend — Новая таблица ClickHouse:**
- `fact_advert_phrases_daily`: поисковые фразы рекламы (универсальная WB + Ozon)
- Миграция: `docker/clickhouse/migrations/008_fact_advert_phrases_daily.sql`
- ORDER BY: (shop_id, marketplace, campaign_id, dt, phrase), TTL 1 год

**Backend — Новые/обновлённые сервисы:**
- `ozon_campaigns_loader.py` (202 строки) — `OzonCampaignsLoader`: UPSERT кампаний + товаров из API, cleanup удалённых SKU
- `ozon_ads_service.py` (770 → 1056 строк): `order_phrases_report()` + `parse_phrases_csv_report()`, ZIP detection (PK\x03\x04), retry 3×60сек + rate limiter backoff reset
- Celery: `sync_ozon_campaigns_task` — синхронизация справочника при sync_all_daily + initial sync

**Frontend:**
- `CampaignDetailModal.tsx`: 6 вкладок (KPI, Stats, Events, Phrases, Heatmap, Purchases)
- `CampaignInsights.tsx`: новый компонент визуализации инсайтов
- `campaignDetails.ts`: API-клиент для campaign-details endpoints

---

## 2026-03-20 (v17.11)

### feat(advertising): Общая выручка + поиск по кампаниям + UI улучшения

**Backend** (`campaign_details.py`):
- Новое поле `product_revenue` в `CampaignStatsRow` — общая выручка товаров кампании (не рекламная)
- `get_campaign_stats`: запрос ежедневных заказов из `fact_ozon_orders` / `fact_orders_raw` по SKU кампании
- SQL: `toDate(order_date)` для корректной группировки по дням (было `GROUP BY order_date` — DateTime группировал посекундно)
- Нормализация `datetime → date` через helper `_to_date()` для совместимости ключей словарей
- При фильтре по SKU — показывает только выручку конкретного товара

**Frontend** (`CampaignDetailModal.tsx`):
- Новая метрика **«Выручка общая»** на графике: голубая area-линия (`#06b6d4`) с gradient fill
- Gradient `gProdRevM` + Area компонент для `product_revenue`
- `moneyActive` теперь учитывает `product_revenue` (ось Y не скрывается)
- «Выручка» переименована в **«Выручка рекл.»** для ясности
- KPI карточка «Выручка»: два значения — Рекламная + Товаров
- **Увеличены шрифты дельт** с 10px до 13px на всех KPI карточках (лейблы и проценты)

**Frontend** (`AdvertisingAnalyticsPage.tsx`):
- **Универсальный поиск** в таблице «Кампании за период»:
  - Ищет по: названию кампании, ID кампании, SKU, product_id, артикулу (offer_id), названию товара
  - Иконка 🔍, кнопка очистки ×, счётчик «Найдено: X из Y кампаний»
- **Кнопка «Статистика»**: убрана надпись, заменена на крупную иконку-кнопку 📊 (32×32) справа от заголовка кампании
- **Убрана двойная подложка** таблицы кампаний (внутренняя обёртка с border/bg удалена — используется только внешний Card)

**Frontend** (`campaignDetails.ts`):
- Добавлено поле `product_revenue: number` в тип `CampaignStatsRow`

---



### feat(advertising): Справочник рекламных кампаний Ozon в PostgreSQL

**Backend**:
- Созданы SQLAlchemy модели `dim_ozon_campaigns` и `dim_ozon_campaign_products` для кеширования информации о кампаниях и ставках.
- Добавлен сервис `OzonCampaignsLoader` (`app/services/ozon_campaigns_loader.py`) для загрузки и маппинга кампаний из Performance API (включая детальный разбор стратегий и мест размещения: Трафареты, Поиск и рекомендации).
- Добавлена фоновая задача Celery `sync_ozon_campaigns` с периодичностью запуска каждые 15 минут.
- Вызов метода первичной загрузки интегрирован в роутер `/shops` (при добавлении/проверке API-ключа Ozon).
- В `app/api/v1/advertising_analytics.py` данные о кампаниях (названия, типы, статусы) теперь берутся из PostgreSQL (JOIN `dim_ozon_campaigns`), вместо нестабильного Redis.

**Frontend** (`AdvertisingAnalyticsPage.tsx`):
- Переработан UI первой колонки таблицы (колонка "Кампания"):
  - Увеличена ширина с 220px до 300px, убраны обрывы текста.
  - Увеличены шрифты названий кампаний (14px) и товаров (13px), а также свойств (12px).
  - Свойства товаров (Артикул, ID, Текущая ставка) выстроены в читаемый вертикальный список.
  - Цвет ставки изменен на контрастный изумрудный (`teal-600`/`teal-400`).
  - Типы кампаний теперь выводятся напрямую текстом из базы (например, "Средняя стоимость клика (Поиск и рекомендации)").

---

## 2026-03-19 (v17.9)

**Backend** (`advertising_analytics.py`):
- `events_by_day` — агрегация event_log по дням (advertising/content/price/stock) в ответе `/advertising-analytics`
- `GET /advertising-analytics/events-detail` — подробные события за день с product enrichment и campaign titles
- Хало-заказы: все запросы (KPI, chart, campaigns, top_skus) теперь суммируют `orders + model_orders`, `revenue + model_revenue`
- Исправлены колонки ClickHouse: `dt` → `in_process_at` (Ozon), `finishedPrice` → `price_with_disc` (WB)
- Добавлена KPI «Конверсия в корзину» (cart/clicks)
- Карточка «Заказы»: сумма крупно, количество мелко
- Убрана кнопка RefreshCw

**Frontend** (`AdvertisingAnalyticsPage.tsx`):
- 12 KPI карточек (+ конверсия в корзину)
- Фильтр-кнопки категорий: ⚡ События → 📣 Реклама / 📝 Контент / 💰 Цена / 📦 Склад
- ReferenceLine маркеры на графике (пунктирные линии + ⚡N)
- EventsDetailModal: группировка по категориям, product images, campaign titles, время, old→new

**API** (`advertising.ts`):
- Типы: EventDaySummary, EventDetail, EventDetailResponse
- Функция: `getEventsDetail(shopId, date, category?)`

---

## 2026-03-19 (v17.8)

### feat(advertising): Редизайн KPI карточек — 11 метрик + default «Сегодня»

**Backend** (`advertising_analytics.py`):
- Добавлены KPI: cart, conversion_rate, CPO, total_drr (через fact_ozon_orders/fact_orders_raw), ROMI
- Total DRR = ad_spend / total_revenue (все заказы, не только рекламные)
- ROMI = (revenue - spend) / spend × 100

**Frontend** (`AdvertisingAnalyticsPage.tsx`):
- 11 KPI карточек: Расход → Показы → Клики → CTR → Корзины → Заказы → Конверсия → CPO → ДРР рекламы → Общий ДРР → ROMI
- Период по умолчанию: «Сегодня» (было 7 дней)
- Grid: 4 колонки на 2xl экранах (было 3)

**API** (`advertising.ts`):
- Добавлены типы: cart, conversion_rate, cpo, total_drr, romi

---

## 2026-03-19 (v17.7)

### refactor(nav): Реструктуризация навигации — Воронка → Реклама

**Sidebar.tsx:**
- АНАЛИТИКА: убран пункт «Воронка», добавлен collapsible «Реклама → Обзор» (path: `/advertising/analytics`)
- УПРАВЛЕНИЕ → Реклама: убран подпункт «Аналитика» (перенесён в АНАЛИТИКА), оставлены «Управление» и «Автобиддер»

---

## 2026-03-18 (v17.6)

### feat(advertising): Рекламный модуль — аналитика кампаний (Ozon + WB)

**Новый раздел «Реклама»** — аналитика рекламных кампаний для Ozon и Wildberries.

**Backend** (`advertising_analytics.py`):
- `GET /advertising-analytics?shop_id=X&period=7d` — единый endpoint, авто-определяет marketplace
- 4 ClickHouse-запроса: KPI, ежедневный график, таблица кампаний, топ SKU
- Ozon: `fact_ozon_ad_daily FINAL` + `dim_ozon_products` (PostgreSQL)
- WB: `fact_advert_stats_v3 FINAL` + `dim_products` (PostgreSQL)
- 9 KPI с delta vs предыдущий период: расход, заказы, выручка, показы, клики, CTR, CPC, ДРР, ROAS
- Зарегистрирован в `router.py`

**Frontend** (`AdvertisingAnalyticsPage.tsx` ~700 строк):
- 9 KPI-карточек с цветодённой индикацией изменений
- Переключаемый график (8 метрик: расход, показы, клики, корзины, заказы, выручка, CTR%, ДРР%)
- Таблица кампаний за период (ID, расход, показы, клики, CTR, CPC, заказы, выручка, ДРР)
- Таблица топ SKU по рекламному расходу (с картинками, hover-превью)
- Period selector: today / 7d / 14d / 30d

**Frontend** (навигация):
- `Sidebar.tsx`: Реклама → collapsible group (Аналитика, Управление, Автобиддер)
- `App.tsx`: 3 новых роута `/advertising/analytics`, `/campaigns`, `/autobidder`
- Заглушки: `AdvertisingCampaignsPage.tsx`, `AdvertisingAutobidderPage.tsx` — «В разработке»

**API-клиент** (`advertising.ts`):
- Типы: `AdvertisingKpi`, `AdvertisingDailyPoint`, `CampaignRow`, `TopSkuRow`, `AdvertisingAnalyticsResponse`
- Функция: `getAdvertisingAnalytics(shopId, period)`

---

## 2026-03-18 (v17.5)

### fix(finances): WB — MAX-reconciliation рекламных расходов (ручное пополнение баланса)

**Root cause**: При ручном пополнении рекламного баланса WB, расходы на рекламу не попадают в `fact_finances` (нет записи удержания), но фактически тратятся и отражаются в `fact_advert_stats_v3`. Результат: Excel и JSON API занижали рекламные расходы и завышали прибыль.

**Решение**: `MAX(fact_finances_ded_ads, fact_advert_stats_v3_spend)` для каждого периода.

**Excel** (`finances_export.py`):
- **Summary (P&L)**: `deductions_ads_cur = max(deductions_ads_cur, ad_spend_cur)` — прибыль теперь учитывает реальные рекл. расходы
- **По неделям / По месяцам**: 2 доп. SQL-запроса к `fact_advert_stats_v3` (`toMonday(date)`, `toYYYYMM(date)`), MAX в row writers
- **Daily**: доп. SQL-запрос по дням к `fact_advert_stats_v3`
- **Секция РЕКЛАМА**: переименована «Итого реклама» + подстрока «Факт. расход (API кампаний)» при расхождении

**JSON API** (`finances.py`):
- **Summary**: `total_deductions += max(0, ad_spend - deductions_ads_raw)` перед расчётом operating/profit
- **Daily dynamics** (grouped + ungrouped): аналогичная корректировка `tded_d` для дневных данных

**Примечание**: «Списание за отзыв» (bonus_type_name не содержит «продвижение») корректно учитывается в прочих удержаниях (`ded`) и не смешивается с рекламой.

---

## 2026-03-18 (v17.4)

### feat(warehouses): Excel экспорт остатков по складам и товарам (WB + Ozon)

**Backend** (`warehouses.py`):
- `GET /wb/analytics/stock-report/excel` — Excel экспорт остатков WB (2 листа)
- `GET /ozon/overview/stock-report/excel` — Excel экспорт остатков Ozon (2 листа)
- Общая функция `_build_stock_report_excel()` — shared логика для обоих маркетплейсов
- Лист «По складам»: склад × SKU матрица с подсветкой OOS (красный), дефицит (жёлтый), излишек (фиолетовый)
- Лист «По товарам»: сводка по SKU × склад, итоги, статусы OOS/LOW/OVER
- OOS-товары: инъекция из `fact_orders_raw` (WB) / `fact_ozon_orders` (Ozon) — SKU с заказами но без стока
- Имена OOS-товаров: fallback-запрос к `dim_products` / `dim_ozon_products` (PostgreSQL через SQLAlchemy)

**Frontend** (`WarehousesOverviewPage.tsx`, `warehouses.ts`):
- Кнопка «📥 Excel» в header таблицы складов (WB и Ozon)
- API функция `downloadStockReportExcel(shopId, period, marketplace)` — blob download
- Типы: `downloadStockReportExcel` в `api/warehouses.ts`

### fix(warehouses): dim_products/dim_ozon_products — запросы через PostgreSQL

**Root cause**: OOS-fallback запросы к `dim_products` и `dim_ozon_products` ошибочно шли через ClickHouse-клиент с префиксом `mms_analytics`. Эти таблицы существуют только в PostgreSQL.

**Backend** (`warehouses.py`):
- WB: `ch.query("mms_analytics.dim_products")` → `db.execute(sa_text("SELECT ... FROM dim_products"))`
- Ozon: `ch.query("dim_ozon_products FINAL")` → `db.execute(sa_text("SELECT ... FROM dim_ozon_products"))`

---

## 2026-03-18 (v17.3)

### feat(warehouses): режим «Товары» — обратная перспектива таблицы складов (WB + Ozon)

**Frontend** (`WarehousesOverviewPage.tsx`):
- Toggle «Склады / Товары» в заголовке таблицы складов
- Режим «Товары»: строки = все товары, колонки = per-warehouse stock
- Данные из `warehouses[].skus` + `products_summary` пивотятся в матрицу `ProductRow[]`
- OOS-товары (stock=0, orders>0) подсвечены красным фоном + бейдж `OOS`
- Товары с дефицитом (<14 дней) подсвечены amber + бейдж `LOW`
- Ячейки складов: красный если 0 stock + есть заказы, серый если нет данных
- Развернутые строки: per-warehouse детали (склад, остаток, заказы, дн.запаса)
- В развёрнутом виде видно список складов где товара нет — «Нет на складах: [список]»
- Sticky first column (Товар) при горизонтальном скролле
- Легенда OOS/LOW над таблицей с количеством
- Сортировка: OOS → LOW → по заказам

---

## 2026-03-18 (v17.2)

### feat(warehouses): products_summary — OOS-товары в поиске по таблице складов

**Backend** (`warehouses.py`):
- Новое поле `products_summary` в ответе `/wb/analytics` — агрегированная статистика по всем товарам (stock, orders, daily, days_supply) из `global_sku_agg`
- Включает OOS-товары (stock=0, orders>0), которые раньше не попадали в таблицу складов

**Frontend** (`warehouses.ts`):
- Новый интерфейс `WBProductSummary` (nm_id, vendor_code, name, stock, orders, daily, days_supply)
- Поле `products_summary: WBProductSummary[]` в `WBWarehouseAnalyticsResponse`

**Frontend** (`WarehousesOverviewPage.tsx`):
- `WarehousesTable` принимает `productsSummary` prop
- `allSkus` мерж: warehouse SKUs + products_summary → OOS-товары доступны в поиске/фильтре

**Frontend** (`WarehousesCrossPage.tsx`):
- `normalizeOzonToCrossData()` — добавлен `products_summary: []` для совместимости типов

---

## 2026-03-18 (v17.1)

### fix(diagnostics): OOS-товары без vendor_code и name в диагностике

**Root cause**: `all_nm_ids` (строка 5601-5603 в `warehouses.py`) формировался **только** из `wh_stocks` — товаров с остатком > 0. OOS-товары (stock=0, но есть заказы) не попадали в запрос к `dim_products`, и `products_map` для них оставался пустым → пустые `vendor_code` и `name` в ответе API.

**Backend** (`warehouses.py`):
- `all_nm_ids` теперь дополняется nm_id из `wh_sku_orders` — товаров с заказами за период
- Все OOS-товары получают полные данные (vendor_code, name) из `dim_products`
- Блок «Диагностика проблем → Out-of-stock» на Overview теперь корректно показывает артикулы и названия

---

## 2026-03-18 (v17)

### fix(diagnostics): OOS — товары с нулевым остатком не отображались в диагностике

**Root cause**: Логика OOS-агрегации фильтровала товары с `stock > 0` и `free_to_sell > 0`, исключая самые критичные товары — уже распроданные (stock = 0).

**Backend** (`warehouses.py`):
- **Ozon OOS** (строки 3482-3510): добавлен цикл по `wh_orders` для SKU с заказами, но без стока. Доп-запрос к `fact_ozon_warehouse_stocks` для resolve `offer_id` / `name` нулевых SKU
- **AI-диагностика** (строка 7499): условие `s["stock"] > 0 and (s["stock"] / s["daily"]) < 14` → `s["stock"] == 0 or (s["stock"] / s["daily"]) < 14` — теперь включает товары с полностью нулевым остатком
- **WB OOS** (строки 6018-6053): проверен — уже содержал корректный цикл для товаров без стока

---

## 2026-03-18 (v16)

### fix(stocks): WB остатки — товары «в пути» учитывались как доступные на складе

**Root cause**: `wb_stocks_service.py:131` использовал `quantityFull` (включает `inWayToClient` + `inWayFromClient`) вместо `quantity` (фактический остаток на складе). Это приводило к завышенным остаткам, отсутствию предупреждений STOCK_OUT и некорректному расчёту поставок.

**Backend** (`wb_stocks_service.py`):
- `quantity_full = stock_item.get("quantityFull", ...)` → `quantity = stock_item.get("quantity", 0)`
- `amount` в данных теперь содержит реальный остаток на складе
- `quantity_full` сохранён отдельно как `stock_item.get("quantityFull", 0)` для информации
- Все downstream-потребители (event detector, Redis state, ClickHouse snapshot, dashboard, products, supply, turnover) автоматически получают корректные данные

---

## 2026-03-18 (v15)

### fix(finances): устранение задвоения рекламы в WB отчётах

**Root cause**: WB Продвижение приходит как `deduction` с типом «продвижение» в `fact_finances`. Колонка «Реклама (внеш.)» из `fact_advert_stats_v3` дублировала ту же сумму — это один и тот же расход.

**Excel** (`finances_export.py`):
- **Удалён лист «По дням»** — избыточен, данные есть в понедельном/помесячном листах
- **Убрана колонка «Реклама (внеш.)»** из листов «По неделям» и «По месяцам»
- Удалены запросы `ads_by_week` и `ads_by_month` к `fact_advert_stats_v3` (экономия 2 SQL-запроса)
- WB Excel теперь **5 листов** (было 6): Сводка, По неделям, По месяцам, По товарам, Расходы детально

**Frontend — сравнение периодов** (`FinancesPage.tsx`):
- Убрана строка **«Реклама (внешняя)»** из `WB_ROWS` — осталась только «ВБ Продвижение»

**Frontend — PDF отчёт** (`generatePnlReport.ts`):
- Убрана строка «Реклама (внешняя)» из WB comparison table
- Убрана колонка «Реклама» из WB weekly table (header, body, totals)
- Скорректированы индексы `profitCol` и `marginCol` (было 13/14, стало 12/13)

### fix(finances): WB график прибыли — столбцы висели в воздухе

**Frontend** (`FinancesPage.tsx`):
- Y-ось метрики `profit` имела `domain: ['auto', 'auto']` — Recharts не привязывал к нулю
- Теперь `domain: [min(dataMin, 0), max(dataMax, 0)]` — столбцы корректно растут от нулевой линии
- Остальные метрики (линии) по-прежнему используют `auto` для лучшего масштабирования

---

## 2026-03-18 (v14)

### fix(finances): WB Excel — per-SKU хранение из fact_wb_paid_storage

**Root cause**: WB не привязывает `storage_fee` к конкретному `vendor_code` в `fact_finances` (AGENTS.md: «хранение/удержания по SKU: WB не привязывает записи хранения к конкретному vendor_code — по товарам эти поля = 0»).

**Фикс** (`finances_export.py`):
- Новый запрос к `fact_wb_paid_storage FINAL` для per-SKU хранения (аналогично разделу «Хранение»)
- `SUM(warehouse_price)` по `vendor_code` за выбранный период
- Перезаписывает нулевые значения `storage` из `fact_finances`
- SKU с хранением, но без продаж — добавляются в отчёт

### refactor(finances): WB Excel — обновление формата до уровня Ozon

**Лист «Сводка»** (`finances_export.py`):
- Переписан на **секционный P&L формат** (6 секций: Выручка, Комиссия, Логистика, Операционные, Реклама, Итоги)
- Добавлена **5-я колонка «% выр.»** — доля каждой статьи от выручки
- Новые метрики: **Средний чек**, **ДРР** (доля рекл. расходов), **Маржинальность**, **Всего удержано WB**, **Всего расходов**
- Блок **«СТРУКТУРА РАСХОДОВ»** — 8 статей с суммами + % от выручки за оба периода
- Purple-тема с секционными заголовками (Violet-800)

**Листы «По неделям» и «По месяцам»** (`finances_export.py`):
- Добавлены **парные Δ-колонки** (изменение в п.п. vs предыдущий период) к 5 процентным показателям (Комис%, Логист%, ВБПромо%, С/С%, Приб%)
- Цветовая индикация дельт: 🟢 улучшение, 🔴 ухудшение, серый при нуле
- Добавлена **итоговая строка «ИТОГО»** с суммами и средними %
- По неделям: 24 колонки, по месяцам: 23 колонки

**Лист «По товарам»** (`finances_export.py`):
- Новая колонка **«ДРР%»** — доля рекламных расходов от выручки per-SKU
- Цветовая индикация: 🔴 >30%, 🟢 <15%
- Добавлен **auto_filter** для сортировки и фильтрации в Excel
- 14 колонок (было 13)

**Лист «Расходы детально»** (`finances_export.py`):
- Новая колонка **«Итого»** — сумма всех расходов по строке
- Новая колонка **«% выр.»** — доля от выручки (🔴 >5%)
- Добавлена **итоговая строка** с общей суммой и %
- Добавлен **auto_filter**
- 11 колонок (было 9)

---



### fix(finances): Ozon Excel — % логистики, дельта-столбцы, фильтр пустых SKU

**Понедельный лист «По неделям»** (`finances_export.py`):
- Добавлен **Дост%** — доля логистики от продаж (ранее отсутствовал)
- Теперь 6 процентных столбцов: Комис%, **Дост%**, Рекл%, Приём%, С/С%, Приб%
- **Δ-столбцы** рядом с каждым %: серая заливка, показывают изменение в п.п. vs предыдущая неделя
- Цветовая индикация дельт: 🟢 улучшение (расходы ↓ / прибыль ↑), 🔴 ухудшение, серый при нулевом изменении
- «—» для первой недели (нет предыдущего периода)
- 30 столбцов (было 23)

**Помесячный лист «По месяцам»** (`finances_export.py`):
- Добавлены 6 процентных столбцов: Комис%, Дост%, Рекл%, Приём%, С/С%, Приб% (ранее были только Δ Выручка% и Δ Прибыль%)
- **Δ-столбцы** рядом с каждым % — аналогично понедельному
- 29 столбцов (было 19)

**Лист «По товарам (SKU)»** (`finances_export.py`):
- **Фикс пустого SKU**: отфильтрованы записи с qty=0 (SKU без Revenue-транзакций, только возвраты/прочее)
- Итоговая строка корректно считает только отфильтрованные SKU

**Стили** (`finances_export.py`):
- Новые константы: `DELTA_FILL` (серый-200), `DELTA_FILL_ALT` (серый-300), `DELTA_HDR_FILL` (серый-600)
- Шрифты для дельт: `GREEN_FONT_SM`, `RED_FONT_SM`, `GREY_FONT_SM` (размер 9)

---

## 2026-03-18 (v12)

### refactor(finances): Ozon Excel — полный редизайн сводного листа (P&L формат)

**Root cause проблем**: `services_total` (Revenue) = стоимость доставки покупателям, `category=Logistics` = FBO-услуги (кроссдокинг/приёмка). Старый отчёт путал эти понятия.

**Новый SQL** (`finances_export.py`):
- Добавлен запрос `detail_breakdown` — per-operation_type breakdown для обоих периодов
- Парсинг в именованные бакеты: FBO (crossdocking, intake), Marketing (CPC, reviews), Other (cashback, fines, packaging)

**Сводный лист переписан** — P&L формат с 6 секциями:
1. **ВЫРУЧКА И ЗАКАЗЫ**: + средний чек
2. **КОМИССИЯ OZON**: отдельная секция
3. **ДОСТАВКА И ЛОГИСТИКА**: «Доставка покупателям» (services_total) + sub-items FBO (кроссдокинг, приёмка). Итого логистика = delivery + FBO
4. **ОПЕРАЦИОННЫЕ РАСХОДЫ**: эквайринг, возвраты, хранение, штрафы, кэшбек, упаковка — каждая статья отдельно
5. **РЕКЛАМА**: CPC + баллы за отзывы + ДРР%
6. **ИТОГИ**: всего удержано, к перечислению, COGS, всего расходов, чистая прибыль, маржинальность

**Убраны** непонятные агрегации: «Сервисные услуги», «Расходы МП (ОПЕКС)», «Удержания МП (всё)»

**Структура расходов** — теперь с обоими периодами (тек. + пред.) и % от выручки

**5-я колонка** «% выр.» добавлена в основную таблицу KPI

---

## 2026-03-18 (v11)

### fix(finances): Ozon Excel — per-SKU поля + корректная логистика

**Per-SKU лист «По товарам (SKU)»** (`finances_export.py`):
- Новая колонка **«Артикул»** — `offer_id` из `dim_ozon_products`
- Новая колонка **«Штрих-код»** — `barcode` (EAN-13) из `dim_ozon_products`
- Новая колонка **«ДРР %»** — доля рекламных расходов от выручки (`ad_spend / revenue × 100`), цветовая индикация: 🟢 <15%, ⚪ 15-30%, 🔴 >30%
- Загрузка barcode через новый SQL запрос к `dim_ozon_products`

**Понедельный + помесячный отчёты** — исправлена разбивка логистики:
- **Root cause**: Ozon транзакции category=`Logistics` содержат только FBO-сервисы (кроссдокинг, приёмка). Реальная логистика (доставка) скрыта в `services_total` у Revenue-записей (`OperationAgentDeliveredToCustomer`). Колонка «Доставка» ≈ 0 (единичные `SellerReturnsDeliveryToPickupPoint`)
- **«Сервисы» → «Доставка»** — переименовано, показывает реальную стоимость доставки
- **«FBO» → «Приёмка/FBO»** — переименовано для ясности, включает merged `delivery_services`
- **Убрана бесполезная колонка «Доставка»** (была ≈ 0₽) и «Дост%»
- Формулы прибыли и процентные колонки пересчитаны

---

## 2026-03-17 (v10)

### fix(finances): Ozon — пустая колонка «Хранение» в Per-SKU финансах

**Проблема**: `get_placement_costs_by_sku()` возвращала пустой словарь для ~95% товаров.
- SQL-запрос использовал `period_end` (переименован в `period_to` миграцией 006)
- GROUP BY `sku` + фильтр `sku > 0` отбрасывал данные (`fact_ozon_placement_cost` хранит `sku=0` для большинства записей — Ozon Placement Report привязывает данные к `offer_id`)

**Backend** (`ozon_finance_queries.py`):
- `get_placement_costs_by_sku()`: возвращает `Dict[str, float]` (offer_id → cost) вместо `Dict[int, float]` (sku → cost)
- SQL: `GROUP BY offer_id`, фильтр по `max(period_to)`, убран `sku > 0`

**Backend** (`finances.py`):
- `placement_costs` теперь по `offer_id` → напрямую обновляет `products[oid]` (products dict тоже по offer_id)

**Backend** (`finances_export.py`):
- `offer_storage_map` (было `sku_storage_map`) → маппинг через `sku_to_offer.get(sku)` для получения offer_id

---

## 2026-03-17 (v9)

### refactor(finances): Ozon — 6 улучшений финансовых отчётов

**Новый модуль** (`ozon_finance_queries.py`):
- Shared SQL модуль для переиспользования между API и Excel
- Функции: `get_sku_to_offer_map`, `get_cost_map`, `build_sku_cost_map`, `calc_cogs_from_ch`, `get_daily_category_breakdown`, `get_placement_costs_by_sku`, `get_ad_costs_by_sku`

**ClickHouse** (`007_add_items_count_to_transactions.sql`):
- Новая колонка `items_count UInt32 DEFAULT 1` в `fact_ozon_transactions` — защита от multi-item транзакций

**Backend** (`ozon_finance_service.py`):
- `_normalize_transaction`: добавлен `items_count = max(len(items), 1)`
- `CH_COLUMNS` и loader обновлены для `items_count`

**Backend** (`finances.py`):
- **Формула прибыли**: unified `revenue - mp_fees - ads - cogs` (было `payout - cogs`)
- **Ad spend per-SKU**: `fact_ozon_ad_daily` (Ozon Performance API) вместо `fact_ozon_transactions` Marketing (где `sku=0`)
  - `GET /finances/ozon/products` — ad spend из `fact_ozon_ad_daily FINAL` с группировкой по SKU
- **Daily dynamics**: точный breakdown по категориям через `get_daily_category_breakdown()` (было `mp_d = max(0, rev - txn_d - ads_d)`)
- **COGS**: `sum(items_count)` вместо `count()` через shared модуль
- **Per-product storage**: данные из `fact_ozon_placement_cost` через `get_placement_costs_by_sku()`
- **Per-product profit**: `revenue - commission - logistics - storage - acquiring - ads - cogs`

**Backend** (`finances_export.py`):
- Weekly profit: `sales - |all expenses| - cogs` (было `payout - cogs`)
- Monthly profit: аналогичное обновление
- **Per-SKU лист «По товарам (SKU)»** — исправлены нулевые значения:
  - **Реклама**: из `fact_ozon_ad_daily` через `get_ad_costs_by_sku()` (было: `fact_ozon_transactions` Marketing с `sku=0` → всегда 0)
  - **Хранение**: новая колонка, из `fact_ozon_placement_cost` через `get_placement_costs_by_sku()` (было: отсутствовала)
  - **Логистика**: пропорциональное распределение по выручке SKU (было: `fact_ozon_transactions` Logistics с `sku=0` → всегда 0)
  - **Прибыль**: `revenue - |comm| - |svcs| - logistics - ads - storage - cogs`

---

## 2026-03-17 (v8)

### fix(supply): WB — cross-drain re-balance, Excel нули, Риск перезатаривания

**Backend** (`warehouses.py`):
- **Cross-drain re-balance**: при поставке в региональный склад, `need` центрального склада уменьшается на долю кросс-drain, обслуживающую этот регион
  - Для food/SGT: кросс-drain НЕ вычитается если в регионе нет food-совместимого склада (кросс неизбежен)
  - Используется `wh_consumption` (фактические отгрузки warehouse→okrug) + `REGION_TO_WAREHOUSES` маппинг
  - Re-balance срабатывает ПОСЛЕ начального расчёта need и ПЕРЕД global cap
- **Excel «Поставка по складам»**: для food/SGT складов добавлены `paired_orders` и `paired_revenue` — WB бронирует продажи под базовым именем (Котовск), а не «Котовск: Питание»
  - Новые поля в warehouse dict: `paired_orders`, `paired_revenue`
  - `effective_orders = orders + paired_orders`, `effective_revenue = revenue + paired_revenue`
- **Excel «Риск перезатаривания»**: 
  - Фильтр: `turnover_days > target_days` вместо хардкода `> 45`
  - `extra_cost` → `storage_per_month` (ежемесячная стоимость, не total за excess_days)
  - Добавлено `excess_qty` — количество излишков в рекомендацию
  - Колонка «Превышение»: `{excess_days} дн / ~{excess_qty} шт`

---

## 2026-03-17 (v7)

### fix(supply): WB — global cap на поставки, устранение перезатарки

**Backend** (`warehouses.py`):
- Добавлен **global cap** в `_build_wb_supply_data()`: `sum(needs)` по складам не может превышать `boosted_daily × target_days × safety − total_stock`
- Ранее каждый склад считал `need` независимо через `MAX(actual, regional, paired)`, и сумма превышала адекватный target в 2-3 раза
- При превышении cap — пропорциональное уменьшение `need` каждого склада (распределение сохраняется)
- Пример: товар 36 прод./мес, остаток 10 → было 104 шт, стало **30 шт** (без буста) / **71 шт** (с бустом ×2)

---

## 2026-03-17 (v6)

### feat(warehouses): Ozon Overview — расширение расходов и диагностики

**Backend** (`warehouses.py`):
- Расширен `cost_type_map` — 9 новых типов расходов вместо 5:
  - `OperationAgentDeliveredToCustomer` → Логистика (из `services_total`)
  - `MarketplaceRedistributionOfAcquiringOperation` → Эквайринг (из `services_total`)
  - `OperationItemReturn` + `OperationReturnGoodsFBSofRMS` → Возвраты (из `services_total`)
  - `DefectFineShipmentDelay*`, `DefectFineCancellation` → Штрафы (из `amount`)
  - `OperationMarketplaceServiceSupplyInboundCargoSurplus` → Излишки
- SQL-запросы разделены на `amount`-based и `services_total`-based расходы
- KPI: `total_expenses`, `total_logistics`, `total_returns`, `total_acquiring` с трендами prev period
- `cross_problem_warehouses[]` — массив ВСЕХ складов с кросс > 30% (было: только worst)

**Frontend** (`WarehousesOverviewPage.tsx`, `warehouses.ts`):
- `OzonOverviewKpi` — расширен: `total_expenses`, `total_logistics`, `total_returns`, `total_acquiring`, `cross_problem_warehouses`
- KPI: 6 карточек (Расходы, Логистика с ₽/заказ, Кроссдокинг, Хранение, Заказы, Кросс-кластер)
- Диагностика кросс: список ВСЕХ проблемных складов (Пермь 100%, Дедовск 83%, УФА 78%...)
- Новый блок «Возвраты/невыкупы» при total_returns > 1000₽
- Расходы за период: + Логистика, Эквайринг, Возвраты

---

## 2026-03-17 (v5)

### feat(warehouses): Ozon — раздел «Обзор» (по аналогии с WB)

**Backend** (`warehouses.py`):
- `GET /ozon/overview` — lightweight endpoint обзорного дашборда (~380 строк):
  - Стоки по `fact_ozon_warehouse_stocks` (warehouse_name × offer_id)
  - Заказы из `fact_ozon_orders` (текущий + предыдущий период для трендов)
  - Расходы: кроссдокинг, хранение, FBO из `fact_ozon_transactions`
  - Фактическое хранение из `fact_ozon_placement_cost` (приоритет над расчётным)
  - Out-of-stock SKU: агрегация stock/daily по всем складам, top-10 с days_left < 14
  - KPI с prev period: `total_crossdocking`, `total_storage`, `total_fbo`, `total_orders`, `cross_pct`
  - Per-warehouse: status (critical/empty/attention/overstocked/ok), daily_sales, turnover_days, cross_pct, top-50 SKU
  - Warehouse-to-cluster mapping (`WAREHOUSE_TO_CLUSTER`)

**Frontend** (`WarehousesOverviewPage.tsx`, `warehouses.ts`):
- Убран redirect Ozon → `/warehouses/analytics`
- Ozon KPI: 5 карточек (Общие расходы, Кроссдокинг, Хранение, Заказы, Кросс-кластер) с трендами vs prev period
- Диагностика: кросс-кластер (worst warehouse), затоваривание, out-of-stock, география
- Расходы за период: анимированные бары (кроссдокинг, приёмка, хранение, недостача, ФБО)
- `OzonWarehousesTable` (~150 строк): раскрываемые строки → per-SKU (offer_id, sku, stock, orders, days_supply, cross%)
- API типы: `OzonOverviewKpi`, `OzonOverviewWarehouse`, `OzonOverviewSku`, `OzonOverviewCostItem`, `OzonOverviewResponse`
- API функция `getOzonOverview()`

---

## 2026-03-17 (v4)

### refactor(warehouses): Ozon — кросс-логистика ИИ V4: обзорный формат

**Backend** (`warehouses.py`):
- **Промпт переписан** → обзорный формат вместо конкретных qty рекомендаций
- **Новые секции**: `warehouse_assessments` (оценка каждого склада с status critical/warning/ok, cross_pct, main_cross_destinations), `priority_actions` (текстовые действия с impact и link_to_supply)
- **Убраны**: `transfers`, `supply_recommendations`, расчёты `cross_demand`, `deficit`, `cluster_stock`, каталог стоков по складам
- **Упрощены данные промпта**: только кросс-маршруты per-SKU, стоки per-warehouse, общая статистика — без deficit-калькуляций
- **ИИ запрещено**: считать конкретные qty, давать формулы перемещений, `cross_demand`, `deficit`

**Frontend** (`WarehousesCrossPage.tsx`, `warehouses.ts`):
- **Новые типы**: `OzonCrossAIWarehouseAssessment`, `OzonCrossAIPriorityAction`
- **Удалены типы**: `OzonCrossAITransfer*`, `OzonCrossAISupply*`
- **Модалка переписана**: 4 метрики + «Что делать» (нумерованные действия с кнопками «Поставки») + карточки складов (2-col grid, статус/кросс%/кросс-направления/оценка) + проблемные SKU + рекомендации
- **Убраны**: таблицы перемещений и поставок с конкретными qty

---

## 2026-03-17 (v3)

### fix(warehouses): Ozon — кросс-логистика ИИ V3: deficit-based рекомендации

**Backend** (`warehouses.py`):
- **Промпт V3**: формула `qty = cross_demand - stock_at_dest` вместо `MAX(daily_sales × 30, 40)`
  - `cross_demand = ceil(cross_orders / period × 30 × 1.5)` — 30-дневная потребность с буфером 1.5
  - `deficit = max(cross_demand - stock_at_dest, 0)` — реальный дефицит с учётом стока получателя
- **Обогащение данных**: каждый кросс-маршрут теперь содержит `cross_demand`, `stock_at_dest`, `deficit`
- **Построена `cluster_stock` карта** (sku → cluster → total_stock) для проверки стока получателя
- **Убран минимум 40 шт** для поставок — qty = deficit (может быть 3, 5, 10 шт)
- **Запрещён «добор» SKU** с 0 кросс-заказами в трансферы
- **Результат**: 8 шт вместо 135, поставки 1-3 шт вместо 40, каждая рекомендация привязана к реальному кросс-спросу

---

## 2026-03-17 (v2)

### fix(warehouses): Ozon — переписка промпта кросс-логистики ИИ (V2)

**Backend** (`warehouses.py`):
- Полная переработка промпта `_AI_PROMPT_OZON_CROSS` — 3-шаговый алгоритм:
  1. **ПРОБЛЕМЫ** — SKU с cross_pct > 25% и ≥ 3 кросс-заказами
  2. **ПЕРЕМЕЩЕНИЯ** — сборка трансферов ≥ 40 шт из полного КАТАЛОГА СТОКОВ
  3. **ПОСТАВКИ** — для оставшихся проблем: qty = MAX(daily_sales × 30, 40)
- Запрещены фразы: «например», «допустим», «альтернативно», «рассмотреть», «доберите до 40 единиц»
- Каждый трансфер — конкретные offer_id, qty, stock_at_source, reason
- **Product metadata** теперь запрашивается для ВСЕХ SKU со стоками (не только проблемных)
- Новая секция промпта «КАТАЛОГ СТОКОВ ПО СКЛАДАМ» — полный инвентарь на каждом складе (offer_id, stock, daily_sales, buffer_days, метка [КРОСС])
- HTTP таймаут увеличен с 60с до 120с для обработки увеличенного объёма данных
- Результат: ИИ даёт конкретные трансферы (напр. «ДОМОДЕДОВО_РФЦ → САМАРА_РФЦ: АМ-КШ-ЯГ-0,4 — 41 шт»), конкретные поставки (напр. «Поставить 45 шт на Екатеринбург_РФЦ_НОВЫЙ»)

---

## 2026-03-17

### feat(warehouses): Ozon — ИИ-анализ кросс-логистики (Gemini 2.5 Flash)

**Backend** (`warehouses.py`):
- `POST /ozon/cross/ai-analysis` — endpoint ИИ-анализа кросс-доставок Ozon
  - Prompt `_AI_PROMPT_OZON_CROSS`: правило ≥40 ед. на перемещение (Ozon кратность), JSON-схема с transfers/problem_skus/supply_recommendations/general_tips
  - 4 ClickHouse запроса: кросс-матрица (sku×warehouse×cluster_to), per-SKU cross summary, стоки по складам, общая статистика
  - PostgreSQL: metadata товаров (offer_id, name)
  - Кеширование 6ч в Redis, force-refresh параметр
  - severity: critical/warning/ok, key_metrics, context

**Frontend** (`WarehousesCrossPage.tsx`, `warehouses.ts`):
- Компонент `OzonCrossAIInsight` (~450 строк):
  - Баннер: severity 🔴/🟡/🟢 + diagnosis + badge перемещений + кнопка «Прочитать»
  - Модалка: 4 метрики (кросс%, пробл. SKU, складов с кроссом, перемещений)
  - Секции: Перемещения (from→to с таблицей товаров), Проблемные SKU (стоки+кросс маршруты), Поставки, Рекомендации
- API типы: `OzonCrossAIAnalysis`, `OzonCrossAITransfer`, `OzonCrossAIProblemSku`, `OzonCrossAISupply`
- API функция `getOzonCrossAIAnalysis()`
- Отображается между KPI и TopProblemSkus (только для Ozon)

---

## 2026-03-16 (v7)

### feat(warehouses): Ozon — кросс-логистика (по аналогии с WB)

**Backend** (`warehouses.py`):
- Query 3c: `sku × warehouse_name × cluster_to` из `fact_ozon_orders` для per-SKU cross анализа
- Per-warehouse: `cross_pct`, `cross_orders`, `local_orders` — через `_get_cluster_for_warehouse`
- Per-SKU: `cross_pct`, `cross_orders`, `geography[]` с `is_local` флагом
- `cross_map`: матрица `warehouse × cluster` (аналог WB `okrug_list`/`cross_map`)
- `cluster_list`: список всех кластеров для заголовков матрицы
- KPI: `cross_pct` — общий % кросс-заказов по магазину
- `clusters_served`: добавлен `is_local` флаг (свой кластер vs кросс)

**Frontend** (`WarehousesCrossPage.tsx`, `warehouses.ts`):
- Убран Ozon redirect → `/warehouses/analytics`
- Adapter `normalizeOzonToCrossData()`: нормализует Ozon ответ в WB формат (cluster→okrug, sku→nm_id, offer_id→vendor_code, crossdocking→logistics_cost)
- Все компоненты (KPI, кросс-карта, склады, SKU) работают для обоих маркетплейсов
- Универсальный заголовок «Склад ↓ / Регион →»
- API типы: `OzonCrossMapRow`, `cross_pct` в KPI, `cross_map`/`cluster_list` в response

---

## 2026-03-16 (v6)

### feat(warehouses): Ozon — фактические данные хранения по SKU (Placement Cost API)

**ClickHouse** (`docker/clickhouse/migrations/005_add_ozon_placement_cost.sql`):
- Новая таблица `fact_ozon_placement_cost` — фактическая стоимость размещения per-SKU
- Поля: offer_id, sku, product_id, name, volume, avg_stock, placement_cost, dt, period_end
- `ReplacingMergeTree ORDER BY (shop_id, dt, offer_id)` — дедупликация по дню и товару

**Backend** (`ozon_placement_service.py`):
- `OzonPlacementService` — полный pipeline: create report → poll status → download Excel → parse
- Использует Ozon API `/v1/report/placement/by-products/create` + `/v1/report/info`
- `OzonPlacementLoader` — вставка parsed данных в ClickHouse
- Лимит: 5 отчётов/день, макс. 31 день в периоде

**Celery** (`tasks.py`):
- `sync_ozon_placement_cost` — задача с offer_id→sku маппингом из PostgreSQL
  - Queue: `heavy`, time_limit: 300с (отчёт генерируется 30-60 сек)
- `backfill_ozon_placement_cost` — бэкфилл за N месяцев (по аналогии с WB)
  - Разбивка на 30-дневные чанки (Ozon API лимит: 31 день/отчёт)
  - 3 месяца = 3 чанка = 3 отчёта (в рамках 5 отчётов/день)
  - Queue: `heavy`, time_limit: 1800с
  - Добавлен в Ozon startup pipeline (dispatch_initial_sync)

**Backend API** (`warehouses.py`):
- `POST /ozon/sync-placement-cost` — trigger endpoint для запуска sync из UI
- `POST /ozon/backfill-placement-cost` — trigger бэкфилла за N месяцев (параметр `months`, default=3)
- `GET /ozon/storage` обогащён: запрос `fact_ozon_placement_cost` для actual costs
  - `storage_source: "actual" | "estimated"` per-SKU
  - KPI: `has_actual_data`, `actual_period` (from/to)
  - `total_storage` берёт actual если есть, иначе estimated

**Frontend** (`WarehousesStoragePage.tsx`, `WBWarehouseAnalyticsContent.tsx`, `warehouses.ts`):
- `OzonStorageKpi` → 4 карточки (было 3): Хранение факт/расчёт, Оборачиваемость, Бесплатное, Риск
- Кнопка «Обновить данные» → запускает sync через Celery
- Кнопка «Загрузить за 3 мес» → backfill 90 дней (показывается если нет фактических данных)
- Динамический disclaimer: ✅ факт / ⚠️ расчёт
- Badge «факт» (зелёный) в StorageSkusTable рядом с суммой хранения
- `isEstimate` теперь динамический: `!(kpi.has_actual_data)`
- API типы: `has_actual_data`, `actual_period` в `OzonStorageKpi`
- API функции: `syncOzonPlacementCost()`, `backfillOzonPlacementCost()`

---

## 2026-03-16 (v5)

### feat(warehouses): Ozon — раздел «Хранение» (по аналогии с WB)

**Backend** (`warehouses.py`):
- `GET /ozon/storage` — endpoint аналитики хранения FBO (~270 строк):
  - Per-SKU оборачиваемость: приоритетно из `fact_ozon_turnover` (Ozon API), fallback на расчёт stock/daily_sales
  - Зонирование: free (<120д), warning (120-160д), paid (>160д) — тарифы Ozon
  - **Объём**: `volume_weight × 2.87` (коэффициент из реверс-инжиниринга Ozon кабинета), fallback на `depth×height×width`
  - **Тариф**: 0.14 ₽/л/день (медиана из реверс-инжиниринга Ozon seller dashboard, было 0.07)
  - Прогноз 30д: с учётом убывания стока при продажах
  - Данные рекламы из `fact_ozon_ad_daily` (has_active_ads)
  - Формат ответа совместим с WB `StorageSkusTable`

**Frontend** (`WarehousesStoragePage.tsx`, `warehouses.ts`):
- Убран redirect Ozon → /warehouses/analytics
- `OzonStorageKpi` — 3 KPI карточки: Хранение (оценка), Ср. оборачиваемость, Проблемные зоны
- Переиспользование `StorageSkusTable` компонента (WB → Ozon)
- Подпись «Тариф ~0.14 ₽/л/день» (исправлена с 0.07)
- API типы: `OzonStorageKpi`, `OzonStorageSku`, `OzonStorageResponse`
- API функция `getOzonStorage()`

---

## 2026-03-16 (v4)

### feat(warehouses): Ozon — ИИ-анализ географии продаж (Gemini 2.5 Flash)

**Backend** (`warehouses.py`):
- `POST /ozon/geography/ai-analysis` — endpoint ИИ-анализа для Ozon кластеров
  - Prompt `_AI_PROMPT_GEO_OZON`: 7 шагов сбора данных (кластеры+города, топ товары, per-cluster products, warehouse stocks, cross-delivery stats)
  - Маппинг `WAREHOUSE_TO_CLUSTER` для привязки стоков РФЦ к кластерам
  - Кеширование 6ч в Redis, force-refresh параметр
  - Gemini 2.5 Flash через kie.ai API

**Frontend** (`OzonGeographyPage.tsx`, `warehouses.ts`):
- Компонент `OzonGeographyAIInsight` (~400 строк):
  - Баннер: severity 🔴/🟡/🟢 + diagnosis + кнопка «Прочитать»
  - Модалка: 4 метрики (концентрация, кластеры, недообслуженные, заказы)
  - Секции: Концентрация продаж, Инсайты по товарам, Логистическое соответствие, Рекомендации
- API функция `getOzonGeographyAIAnalysis()`

**Bugfix**: `free_to_sell_amount` → `free_to_sell` в запросе к `fact_ozon_warehouse_stocks`

---

## 2026-03-16 (v3)

### feat(warehouses): Ozon — география продаж (кластеры → города → товары)

**Backend** (`warehouses.py`):
- `GET /ozon/geography` — основной endpoint: кластеры доставки + города + стабильность + топ товары
  - SQL: `fact_ozon_orders FINAL` GROUP BY `cluster_to`, `city`
  - Стабильность: `count(DISTINCT toDate(order_date)) / period * 100`
  - Топ товары per-cluster (top-5) и общие (top-50) с metadata из `dim_ozon_products`
  - Фильтр по массиву SKU
- `GET /ozon/geography/city-products` — drill-down товаров по конкретному городу
  - Стабильность по неделям: `uniqExact(toMonday(order_date)) / total_weeks`
- `GET /ozon/geography/products-search` — autocomplete для фильтра (по name, offer_id, sku)

**Frontend** (`OzonGeographyPage.tsx` ~530 строк, `warehouses.ts`):
- `OzonGeographyPage` — полная страница:
  - 4 KPI: заказы, выручка, ср. чек, охват (X кл. · Y гор.)
  - `ClustersTable` — 2-уровневая: кластеры → города → товары
  - `TopProductsTable` — сортируемая таблица с колонками: кластеры, города, доля
  - `OzonProductCombobox` — мульти-фильтр по SKU (autocomplete с debounce)
  - Period selector (7/14/30/60/90 дн), skeleton loading, анимации
- `WarehousesGeographyPage.tsx` — redirect `→ /warehouses/analytics` заменён на `<OzonGeographyPage />`
- API типы: `OzonGeographyResponse`, `OzonGeographyCluster`, `OzonGeographyProduct`

---

## 2026-03-16 (v2)

### fix(supply): синхронизация расчёта хранения между Поставками и Хранением

**Backend** (`warehouses.py`):
- При наличии фактических данных `fact_wb_paid_storage` для магазина, SKU/склад-комбинации без записей теперь получают `storage_per_day=0` вместо завышенной тарифной оценки
- Логика синхронизирована с аналитическим endpoint (раздел Хранение)
- Было: Поставки 31K₽ vs Хранение 15K₽ (2x расхождение из-за tariff fallback)
- Стало: Поставки ~46K₽ vs Хранение прогноз ~42K₽ (~10%, разница из-за разного окна данных 14д vs 30д)

### fix(overview): OOS диагностика — серверная агрегация по всем складам

**Backend** (`warehouses.py`):
- Новый блок «Out-of-stock aggregation» — серверный расчёт OOS из полных `wh_stocks` + `wh_sku_orders` (без лимита `[:50]` per warehouse)
- Суммирует stock/orders по nm_id **по всем складам**, фильтрует `stock/daily < 14`, возвращает top-10
- Новое поле `kpi.out_of_stock_skus` в ответе analytics API

**Frontend** (`WarehousesOverviewPage.tsx`, `warehouses.ts`):
- OOS карточка диагностики теперь берёт данные из `kpi.out_of_stock_skus` (бэкенд) вместо фронтенд-агрегации из обрезанных `w.skus[:50]`
- Добавлен тип `out_of_stock_skus` в `WBAnalyticsKpi`
- Было: диагностика показывала 5шт/1/день (только SKU попавшие в top-50 по заказам на складе)
- Стало: корректная агрегация 37шт/20/день (все склады)

## 2026-03-16

### feat(warehouses): реструктуризация раздела «Склады» — 5 подстраниц

**Frontend** (новые страницы + рефакторинг навигации):
- **`WarehousesOverviewPage`** (~1130 строк) — единый дашборд складов WB:
  - 4 KPI: оборачиваемость, заказы/день, остатки, SKU
  - Блок «Диагностика проблем»: кросс, хранение, out-of-stock, штрафы (ProblemCard)
  - Блок «Расходы за период»: горизонтальные бары (логистика, ↳кросс, хранение, возмещение, приёмка, удержания)
  - Таблица 20+ складов с сортировкой (статус, кросс%, оборач., хранение₽)
  - ИИ-анализ (AIDiagnosticsBlock): сценарии по SKU + перераспределение через Gemini
- **`WarehousesCrossPage`** (~980 строк) — кросс-логистика WB:
  - 4 KPI: кросс-стоимость (≈), средний кросс%, проблемных SKU, складов с кроссом
  - Топ-проблемные SKU с потерями и рекомендациями «куда довезти»
  - Кросс-карта: матрица «склад × округ» (зелёный=свой, красный=кросс)
  - Кросс-анализ по складам: раскрываемые строки → SKU детализация
  - SkuGeographyPanel: «где лежит» + «куда продаётся» при клике на SKU
- **`WarehousesStoragePage`** — хранение WB (WBWarehouseAnalyticsContent)
- **`WarehousesGeographyPage`** — география продаж WB с ИИ-анализом
- Sidebar: «Склады» → 5 подпунктов (Обзор, Кросс-логистика, Хранение, География, Поставки)
- Routing: `/warehouses/overview`, `/cross`, `/storage`, `/geography`, `/supply`

### fix(warehouses): унификация расчёта кросс-стоимости

**Frontend** (`WarehousesCrossPage.tsx`):
- Убрана фиктивная константа `CROSS_COST_PER_ORDER = 33₽` — выдуманная цифра
- Формула: `Σ(wh.logistics_cost × wh.cross_orders / wh.orders)` — из `fact_finances`
- Пометка `≈` — WB не разделяет логистику на кросс/обычную в отчётах
- Единый расчёт на Overview (↳ Кросс) и Cross (KPI) — цифры совпадают
- Обновлены: KPI, TopProblemSkus, SkuGeographyPanel, CrossWarehousesTable

### fix(warehouses): удалён блок «Нужна поставка» из Overview

**Frontend** (`WarehousesOverviewPage.tsx`):
- Удалена карточка ProblemCard key="supply" — дублировала «Скоро out-of-stock» и страницу Поставки
- Показывала бессмысленные «2 скл.» без объяснения что и зачем поставлять
- SKU с `daily_sales=0` получали `+0 шт` — вводило в заблуждение
- Очищены unused: import PackagePlus, переменная criticalWhs

### docs: обновление архитектурной документации

- `06_FRONTEND.md`: добавлены секции WarehousesOverviewPage и WarehousesCrossPage, обновлены Routing (5 складских маршрутов) и Sidebar (5 подпунктов)

---

## 2026-03-15

### feat(warehouses): ИИ-анализ географии продаж

**Backend** (`warehouses.py`):
- Новый endpoint `POST /warehouses/wb/geography/ai-analysis`
- Системный промпт `_AI_PROMPT_GEOGRAPHY` с JSON-схемой ответа
- Сбор данных: округа/регионы, стабильность, топ-товары, запасы по складам, кросс-доставки, реклама
- Вызов Gemini 2.5 Flash с группировкой всех данных в один промпт
- Кеширование ответов на 6 часов, force-refresh параметр

**Frontend** (`warehouses.ts`, `WarehousesGeographyPage.tsx`):
- Новые типы: `GeoAIAnalysis`, `GeoAIConcentration`, `GeoAIProductInsight`, `GeoAILogisticsMatch`
- API функция `getGeographyAIAnalysis()`
- Компонент `GeographyAIInsight` — 5 визуальных блоков:
  1. **Header**: severity 🔴/🟡/🟢 + диагноз
  2. **4 метрики**: концентрация %, регионы, недообслуженные округа, заказы
  3. **Концентрация продаж**: таблица топ-регионов + risk level + рекомендация
  4. **Инсайты по товарам**: артикул + тип (лидер/нестабильный/кросс-проблема) + действие + эффект в ₽
  5. **Логистическое соответствие**: округа с кросс-доставкой + склад + рекомендация
  6. **Общие рекомендации**: конкретные советы с числами
- Отображается между KPI и таблицей округов (только без SKU-фильтра)

---

## 2026-03-14 (v6)


### feat(warehouses): Полная таблица хранения SKU с поиском и сортировкой

**Backend** (`warehouses.py`):
- Убран лимит `[:20]` — таблица теперь отдаёт **все SKU** (не TOP-20)
- KPI и таблица показывают согласованные суммы

**Frontend** (`WBWarehouseAnalyticsContent.tsx`):
- Переименовано: «ТОП по стоимости хранения» → «Хранение по SKU»
- **Поиск** по названию, артикулу или nm_id с live-фильтрацией
- **Сортировка** по всем столбцам (клик на заголовок) с индикаторами ▲▼
- Артикул под названием выделен **жирным** шрифтом (text-[11px] font-bold)
- Полное название товара в **2 строки** (line-clamp-2, max-w-300px)
- Hover-эффект на строках таблицы
- Удалена колонка «Источник», убрана легенда Факт/Оценка
- Заголовок: «Хранение за 30д» (left) + «Прогноз 30д» (right)
- Итого пересчитывается с учётом фильтрации поиска

**Bugfix** (`warehouses.py`):
- Исправлена аномально большая сумма «Хранение за 30д» (435 844₽ вместо 4 540₽) для магазинов с фактическими данными paid storage
- Причина: tariff-fallback завышал оценку для SKU без записей в `fact_wb_paid_storage`, хотя магазин уже имел фактические данные
- Теперь при наличии actual paid storage, SKU без записей получают `est_cost_month=0` вместо тарифной оценки
- Исправлен `total_penalties`: «Списание за отзыв» (192K) выделено в отдельную категорию расходов, больше не маппится как «Штрафы». Native WB `operation_type='Штраф'` (4 540₽) — единственный источник для KPI штрафов

**feat(supply)**: Реальные данные paid storage в Поставках WB:
- `_build_wb_supply_data()` теперь загружает фактические данные из `fact_wb_paid_storage` (avg за 14 дней)
- Приоритет: реальная стоимость → тарифный fallback
- Добавлен `storage_source` (actual/tariff) для каждого склада
- Excel экспорт автоматически получает обновлённые данные через тот же data builder

---

## 2026-03-14 (v5)
### feat(warehouses): Прогноз хранения WB на 30 дней

**Backend** (`warehouses.py`):
- Запрос per-SKU daily cost из `fact_wb_paid_storage` (avg за последние 7 дней)
- Агрегация per-SKU orders и stock для расчёта daily_sales
- Формула прогноза: `Σ(cost_per_unit_per_day × max(0, stock − daily_sales × i))` для i=0..29
- Учитывает убывание стока: чем быстрее товар продаётся, тем меньше прогноз
- Новые поля: `forecast_30d`, `daily_sales`, `daily_cost`, `days_to_sell` для каждого SKU
- KPI: `forecast_30d` — суммарный прогноз по всем SKU

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- KPI карточка «Прогноз 30д» со статусом (сравнение с текущим хранением)
- 3 новых колонки в StorageSkusTable: «Прод/д», «Дней», «Прогноз 30д»
- Цветовая индикация: дней до распродажи (🟢 <90, 🟡 <180, 🔴 >180, ∞ нет продаж)
- Суммарный прогноз в заголовке таблицы (amber) рядом с хранением/мес (red)

---

## 2026-03-14 (v4)

### feat(warehouses): WB аналитика складов — фактические данные хранения

**Backend** (`warehouses.py`):
- Запрос к `fact_wb_paid_storage` для фактической стоимости хранения per-SKU per-warehouse
- Fallback на тарифную оценку (storBase × vol × qty × коэф) если нет данных paid storage
- Per-warehouse `storage_cost_actual` и `storage_cost_month` в объекте склада
- KPI: `total_storage_actual` (факт из paid storage) + `has_actual_storage` флаг
- `storage_source: "actual" | "estimated"` для каждого SKU и склада

**Bugfix** (`warehouses.py`):
- Fix `UnboundLocalError: wh_list` в `/wb/ai-analysis` — переменная определялась только в ветке tariff-fallback, но использовалась безусловно

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- KPI карточка «Хранение (факт)» с подписью «По отчётам WB • за Nд»
- Новая колонка «Хранение ₽» в таблице складов (фактическая стоимость per-warehouse)
- Бэйджи 📊 Факт / 📐 Оценка в таблице «ТОП по стоимости хранения»
- Легенда: «📊 Факт — из отчётов WB API | 📐 Оценка — по тарифам»

---

## 2026-03-14 (v3)

### feat(warehouses): WB Paid Storage API — фактические данные хранения по SKU

**ClickHouse** (`docker/clickhouse/migrations/004_add_wb_paid_storage.sql`):
- Новая таблица `fact_wb_paid_storage` — ежедневные данные хранения по SKU × склад × тип расчёта
- `shop_id` в ORDER BY — привязка к магазину как везде в системе
- `warehouse_price` — фактическая сумма (может быть отрицательной для скидок WB)
- `calc_type` — различает основное хранение, скидки на остаток/период поставки

**Backend** (`wb_paid_storage_service.py`):
- `WBPaidStorageService` — create→poll→download цикл для WB Report API
- Автоматическая разбивка по 7-дневным чанкам (лимит API = 8 дней)
- `prepare_ch_rows()` — конвертация API-ответа в CH rows с `shop_id`

**Celery** (`tasks.py`):
- `sync_wb_paid_storage` — ежедневная задача, 7 дней назад, queue=sync
- `backfill_wb_paid_storage` — одноразовый бэкфилл за N месяцев
- `sync_all_daily` обновлён — `sync_wb_paid_storage` включён для всех WB-магазинов

**ИИ-анализ** (`warehouses.py`):
- Гибридная логика `est_storage_month`: приоритетно фактические данные из `fact_wb_paid_storage`, fallback на тарифную оценку
- `storage_source: "actual" | "estimated"` — источник данных для каждого SKU
- `has_actual_storage` + `actual_storage_skus` — метаданные в AI response
- Prompt обновлён: «факт» vs «оценка» для прозрачности

---

## 2026-03-14 (v2)

### feat(warehouses): ИИ-анализ складов v2 — P&L + перераспределение + параллельные запросы

**Backend** (`warehouses.py`):
- Два специализированных Gemini-промпта вместо одного:
  - **Промпт 1 (SKU problems)**: сценарный анализ по каждому проблемному SKU с вариантами: `discount`, `launch_ads` (для оборачиваемости), `withdraw`, `do_nothing`, `reduce_supply`
  - **Промпт 2 (Redistribution)**: конкретные рекомендации по перемещению товаров между складами (`transfers[]` с `from_warehouse`, `destinations[]`, `keep_at_source`, `expected_effect`)
- **P&L обогащение**: для каждого SKU подтягиваются данные из `fact_finances`: revenue, payout, logistics, storage_fact, deductions, cogs → `net_profit` (чистая прибыль)
- **Параллельные Gemini API вызовы** через `asyncio.gather()` — ~2x быстрее
- **Исправлен ClickHouse SQL**: разбит сложный JOIN-запрос warehouse summary на 4 простых запроса (stock per warehouse, orders per warehouse, stock per nm_id, orders per nm_id) → мержатся в Python. Устранена ошибка `Correlated subqueries are not supported in JOINs yet`
- Кеширование с TTL 6ч для обоих промптов
- Новый формат ответа: `sku_actions[]` (с `net_profit_month`), `transfers[]` (с `destinations[]`), `general_tips[]`, `supply_tip`

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- Новые TypeScript интерфейсы: `AITransfer`, `AITransferDestination`, обновлён `AISkuAction` (+`net_profit_month`), `AISkuOption` (`launch_ads` вместо `advertise`)
- Компонент `WarehouseAIInsight` переписан на 3 визуальных блока:
  1. **Проблемные товары**: expandable карточки SKU с P&L данными (хранение, чист. прибыль), grid сценариев (discount/ads/withdraw/do_nothing)
  2. **Перераспределение**: таблица трансферов (склад-источник → склады-получатели с qty, причиной, эффектом)
  3. **Supply tip**: рекомендация по поставке + кнопка-ссылка на раздел Поставки
- Увеличены шрифты: 13-14px основной текст, 14px заголовки сценариев
- Удалён `advertise` action (заменён на `launch_ads` — контекст оборачиваемости)
- 3 KPI карточки: Кросс-логистика, Избыточное хранение, Потенциал экономии

---

## 2026-03-14

### feat(warehouses): ИИ-анализ складов → сценарный формат по SKU


**Backend** (`warehouses.py`):
- Полностью переписан AI system prompt: вместо общих рекомендаций → детальный сценарный анализ по каждому проблемному SKU
- Новый формат ответа: `sku_actions[]` (vendor_code, problem, options[]), `warehouse_tips[]`, `supply_tip`
- Каждый SKU получает 2-3 опции: `discount`, `advertise`, `redistribute`, `withdraw`, `do_nothing`, `reduce_supply`
- Опции включают: label, detail (с расчётами DRR, маржи, прогнозом продаж), expected_savings, risk (low/medium/high)
- ИИ указывает `recommended_option` — индекс лучшего варианта
- Новый SQL запрос: per-warehouse stock distribution (nm_id × warehouse_name из fact_inventory_snapshot)
- Обогащённый SKU-контекст: цена, себестоимость, маржа (руб/%), estimated_storage_cost/month, warehouse_distribution{}
- Запрещены рекомендации по рекламе/ценам вне складского контекста

**Frontend** (`warehouses.ts`, `WBWarehouseAnalyticsContent.tsx`):
- Новые TypeScript интерфейсы: `AISkuAction`, `AISkuOption`, `AIWarehouseTip`
- Компонент `WarehouseAIInsight` переписан:
  - Expandable карточки SKU с vendor_code, problem, storage_cost/month
  - Grid сценариев внутри каждого SKU: иконка + label + detail + risk badge + savings
  - Рекомендуемый вариант выделен зелёной рамкой + бейджем «✓ Рекомендуем»
  - Блок «По складам» с конкретными советами по перераспределению
  - Supply tip с кнопкой-ссылкой на раздел Поставки (корректный shop_id)
  - Auto-expand первых 2 SKU карточек

---

## 2026-03-13

### feat(warehouses): WB — кросс-складской анализ расхода стока

**Backend** (`warehouses.py`):
- Новый маппинг `WAREHOUSE_TO_OKRUG` — 50+ складов WB → 8 федеральных округов (вкл. `:Питание` и `СГТ`)
- Новый SQL: фактический расход `warehouse_name × nm_id × oblast_okrug_name` из `fact_orders_raw`
- `wh_consumption` — маппинг {nm_id: {warehouse: {okrug: qty}}}
- `effective_days` — реальный запас с учётом кросс-нагрузки (Воронеж→ЮФО, Самара→Сибирь и т.д.)
- `cross_daily` / `cross_okrugs` — детализация паразитной нагрузки по округам
- Пересчёт статуса по `effective_days` (ok→attention→critical)
- Корректная работа с food-товарами: кросс-слив неизбежен (нет складов Питания в Сибири/ДВ)

**Excel экспорт**:
- Лист «Рекомендации по складам»: колонки «Реал.зап, дн» (col 8) и «Кросс» (col 9) — сдвинуты остальные на +2

---

## 2026-03-12 (v2)

### feat(warehouses): Ozon — кросс-кластерный анализ расхода стока

**Backend** (`warehouses.py`):
- Новый SQL: фактический расход по `warehouse_name × offer_id × cluster_to` из `fact_ozon_orders`
- `wh_consumption` — маппинг {offer_id: {source_cluster: {dest_cluster: qty}}} через `WAREHOUSE_TO_CLUSTER`
- `effective_days` — реальный запас: `stock / фактический_расход` (включая кросс-нагрузку от соседних пустых кластеров)
- `post_restock_days` — прогноз после закрытия дефицитов: `stock / собственный_расход`
- `cross_consumption` / `cross_clusters` — детализация паразитной нагрузки (какой кластер сколько шт/день)
- Пересчёт статуса товара по `effective_days` (attention → critical если реальный запас <14д)
- Сортировка по min(days_supply, effective_days)

**Excel экспорт**:
- Лист «Рекомендации»: колонки «Реал.зап» (col 15) и «Кросс» (col 16) — сдвинуты ПОСТАВИТЬ и далее на +2
- Лист «Методология»: секция «КРОСС-КЛАСТЕРНЫЙ АНАЛИЗ» — объяснение warehouse_name анализа

**Frontend** (`warehouses.ts`):
- `CrossClusterDrain` — новый тип
- `SupplyCluster` + `effective_days`, `post_restock_days`, `cross_consumption`, `cross_clusters`
- `SupplyItem` + `effective_days`

## 2026-03-12

### feat(warehouses): Ozon поставки — реальные стоки по РФЦ

**Backend** (`warehouses.py`):
- Маппинг `WAREHOUSE_TO_CLUSTER` (34 РФЦ → кластеры доставки) + обратный `CLUSTER_TO_WAREHOUSES_OZON`
- `_compute_supply` переписан: стоки берутся **по каждому складу** из `fact_ozon_warehouse_stocks` (`GROUP BY offer_id, warehouse_name`)
- Формула: `need = max(0, daily×target×safety − РЕАЛЬНЫЙ_сток_на_РФЦ)` вместо пропорциональной оценки
- Новые поля в JSON: `wh_stock`, `warehouses` (список РФЦ с остатками)
- Excel: колонка «Оц.стока» → «Сток РФЦ» + новая колонка «Склады» (Лист 1)
- Excel: Sheet 4 «Поставка по кластерам» + Sheet 5 «Объединённые кластеры» — добавлена колонка «Сток РФЦ»
- Excel: Методология обновлена — формула с real stock, описание маппинга складов

**Frontend** (`WarehouseSupplyPage.tsx`, `warehouses.ts`):
- `SupplyCluster` + `wh_stock`, `warehouses` поля
- `HubItem` + `wh_stock`
- Колонка «Оц. стока» → «Сток РФЦ»

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: Response Schema Ozon Supply обновлена (wh_stock, warehouses, warehouse), маппинг WAREHOUSE_TO_CLUSTER, формула per-warehouse stock, Excel описание обновлено
- `06_FRONTEND.md`: типы SupplyCluster/HubItem + колонка «Сток РФЦ»

## 2026-03-11 (v10)

### fix(warehouses): исключение FBS складов из плана кроссдокинга

**Backend** (`warehouses.py`):
- Crossdocking analysis теперь фильтрует заказы через FBO whitelist (`warehouse_type='fbo'`)
- FBS склады (ООО "ТЕЙЛОРД" и т.п.) больше не попадают в план распределения
- Ранее FBS заказы ошибочно показывали «поставить на свой же склад»

## 2026-03-11 (v9)

### feat(warehouses): Excel экспорт плана распределения

**Backend** (`warehouses.py`):
- Новый endpoint `GET /ozon/analytics/distribution-plan/excel`
- Лист 1 «Сводный план» — все SKU со складами, действиями, причинами, выгодой, городами спроса
- Лист 2 «Поставки по складам» — supply items сгруппированы по складу с merge cells и итогами
- Лист 3 «Перемещения» — transfer items сгруппированы по источнику, с окупаемостью и итогами

**Frontend** (`WarehouseAnalyticsPage.tsx`, `warehouses.ts`):
- `downloadDistributionPlanExcel()` в API
- Зеленая кнопка «Скачать Excel» на табе Кроссдокинг со стейтом загрузки

## 2026-03-11 (v8)

### feat(warehouses): кроссдокинг — контекст «почему» и «что даст»

**Backend** (`warehouses.py`):
- SQL запрос географии CD-заказов: `city`, `cluster_from` по SKU × warehouse из `fact_ozon_orders`
- Каждый distribution_plan item обогащён: `demand_cities` (top-5 городов спроса), `shipped_from` (откуда отгружается), `reason` (текстовое объяснение), `benefit` (ожидаемый эффект с окупаемостью)
- Warehouse-level: `top_demand_cities` (top-5 городов по всем SKU склада), `total_orders_cd`

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Warehouse header: строка «📍 Спрос: Москва (8), Домодедово (4)...» — top-4 города
- SKU sub-rows: контекстный блок с причиной (📍) и выгодой (💡) под каждым товаром
- `React.Fragment` для корректного рендера пар строк (данные + контекст)

**Types** (`warehouses.ts`):
- `DistributionPlanItem` + `demand_cities`, `shipped_from`, `reason`, `benefit`
- `DistributionPlanWarehouse` + `top_demand_cities`, `total_orders_cd`

## 2026-03-11 (v7)

### feat(warehouses): Кроссдокинг → План распределения по складам

**Backend** (`warehouses.py`):
- Новая агрегация `distribution_plan`: группировка crossdocking SKU по складу-получателю
- Для каждого склада: список SKU с action (`transfer`/`supply`), qty, source_warehouse, transfer_cost
- Алгоритм: ищет склад-донор с избытком → если есть, action=transfer (со стоимостью), иначе supply

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- `CrossdockingTable` заменён на `DistributionPlanTable` — складоцентричный вид
- 4 KPI-карточки: складов с CD, расход/мес, переместить, поставить
- Раскрываемые блоки складов с таблицей SKU: действие, кол-во, прод/д, CD/мес, источник
- Фильтры (Все/Переместить/Поставить) и поиск работают по distribution_plan

**Types** (`warehouses.ts`):
- `DistributionPlanItem`, `DistributionPlanWarehouse` + поле `distribution_plan` в response

## 2026-03-11 (v6)

### feat(warehouses): столбец Реклама + компактные бейджи + умные рекомендации

**Backend** (`warehouses.py`):
- Добавлен ClickHouse запрос к `fact_ozon_ad_daily` для SKU из зоны риска (spend_30d, orders_30d, has_active_ads)
- Рекомендации теперь ad-aware: учитывают наличие/отсутствие рекламы в каждом SKU
- Добавлено поле `ad_info` в `storage_risk_skus`

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Бейдж «Зона» стал компактным (без emoji, fixed-width, меньше padding)
- Колонка «Сверх лимита» заменена на «Реклама» (Да/Нет)
- Рекомендации различают ситуации: «Запустить рекламу + скидка» vs «Скидка 30-50% или вывоз»

**Types** (`warehouses.ts`):
- `StorageRiskSku` + `ad_info: { has_ads, spend_30d, orders_30d }`

## 2026-03-11 (v5)

### refactor(warehouses): per-SKU рекомендации вместо per-warehouse

**Backend** (`warehouses.py`):
- Удалены `paid_storage` рекомендации по складам (одинаковый текст для всех)
- Добавлено поле `recommendation` в `storage_risk_skus` — индивидуальная рекомендация для каждого SKU:
  - `critical`: «Вывезти или списать» (0 продаж, мёртвый сток)
  - `high`: «Скидка 30-50% или вывоз» (низкие продажи) / «Скидка 20-30% + промо» (оборач. >300д)
  - `medium`: «Ускорить продажи» (рекламой + акции) / «Снизить цену 15-20%» (warning)
  - `low`: «Не поставлять новые» (есть продажи, но избыточный запас)

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Карточки по складам → одна сводка-баннер (кол-во SKU + прогноз расходов + кол-во мёртвого стока)
- Колонка «Складов» → «Рекомендация» с action + reason для каждого SKU

**Types** (`warehouses.ts`):
- `StorageRiskSku` + `recommendation: { action, reason, severity }`

## 2026-03-11 (v4)

### fix(warehouses): читаемость рекомендаций + логика распределения по складам

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Шрифты увеличены (14px заголовки, 13px impact), контрастные `dark:`/light модификаторы
- `bg-orange-50`/`bg-blue-50` + `text-orange-800`/`text-blue-800` для light theme

**Backend** (`warehouses.py`):
- `move_stock` полностью переписан: распределение по **нескольким складам**
- Расчёт capacity каждого склада-получателя (90 дней запаса)
- Проекция оборачиваемости: `[склад: Xд → Yд запаса]` — видно что будет после перемещения
- Лимит: каждый склад получает только столько, сколько может продать за 90 дней

## 2026-03-11 (v3)

### fix(warehouses): фидбек — автокомплит, география продаж, рекомендации

**Backend** (`warehouses.py`):
- SQL запрос `sku_sales_geo_data` — продажи по SKU × cluster_to (fact_ozon_orders)
- `sales_clusters` в `sku_geography` — кластеры с заказами, шт, выручкой

**Frontend** (`WarehouseAnalyticsPage.tsx`):
- Кастомный автокомплит вместо нативного select (поиск по артикулу/название/SKU)
- **«Где лежит» + «Куда продаётся»** — два side-by-side блока в географии товара
- Inline рекомендации: добавлен impact, action items, потенциал экономии в ₽/мес

**API types** (`warehouses.ts`): добавлен `SalesCluster`

## 2026-03-11 (v2)

### feat(warehouses): редизайн «Аналитика складов» — табовый дашборд

**Backend** (`warehouses.py`):

- Автогенерация текстового `summary` — сводка проблем (платное хранение, перезатарка, кроссдокинг, потенциал экономии, скорость доставки)
- `sku_geography` — инвертированный вид warehouse→SKU для географии по конкретному товару

**Frontend** (`WarehouseAnalyticsPage.tsx`):

- **4 вкладки** вместо простыни ~5000px:
  - **Обзор**: текстовое саммари + KPI с бенчмарками/цветовой индикацией + расходы + top-3 проблемных SKU
  - **Хранение**: StorageRiskTable + фильтры (Платное/Скоро/Все) + поиск + inline рекомендации
  - **Кроссдокинг**: CrossdockingTable + фильтры (Переместить/Поставить/Все) + поиск + inline рекомендации
  - **Склады и география**: WarehouseTable + SKU geography selector + фильтры по статусу
- KPI карточки: цветовая индикация (🔴 Критично /🟡 Внимание / 🟢 Норма) + бенчмарки (160дн, 48ч)
- Кнопки навигации из саммари → конкретная вкладка
- Убран отдельный блок «Рекомендации» — встроены inline в контексте каждой вкладки

**API types** (`warehouses.ts`): добавлены `SkuGeography`, `SkuGeographyWarehouse`, обновлен `WarehouseAnalyticsResponse`

## 2026-03-11

### feat(finances): WB Excel финансовый отчёт (6 листов)

**Новый endpoint** `GET /api/v1/finances/wb/excel`:

- 6 листов Excel: Сводка, По дням, По неделям, По месяцам, По товарам, Расходы детально
- Полная ретроспектива по неделям и месяцам (вся история магазина)
- KPI waterfall: выручка → комиссия → payout → расходы → прибыль
- Форматирование: цветовые % (зелёный/красный), формат дат «2 март. — 8 март.»

**Ключевые исправления:**

- **Двойной учёт рекламы устранён:** `fact_advert_stats_v3` и `deduction` с «продвижение» — один и тот же расход. Оставлен только «ВБ Промо» (из удержаний), убрана дублирующая «Реклама (внешн.)» из сводки, недельного и месячного отчётов
- **penalty_total fix:** для `operation_type='Удержание'` поле `penalty_total` дублирует `deduction`. Теперь фильтруется `operation_type!='Удержание'`
- **SKU ad spend fix:** маппинг `nm_id → vendor_code` через `fact_finances` → `fact_advert_stats_v3` по `nm_id` (ранее join по vendor_code давал нули)
- **Формула прибыли:** `Payout − Логистика − Хранение − Приёмка − Удержания − ВБ Промо − Штрафы − COGS`
- **Расходы детально:** исключены Продажа/Возврат (выручка, не расходы)
- **Content-Disposition:** RFC 5987 для кириллических имён файлов

**Frontend:**

- Кнопка «Скачать → Excel отчёт» теперь работает для обоих маркетплейсов (Ozon и WB)
- `downloadWbExcelReport()` в `finances.ts`

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: секция Финансов — 8 endpoints (добавлены excel, weekly-report), описание 6 листов Excel, WB-специфика

## 2026-03-10 (v2)

### feat(warehouses): WB Supply Frontend — единый интерфейс поставок

**Frontend** (`WarehouseSupplyPage.tsx`):

- Единый `WarehouseSupplyPage` — авто-переключение Ozon/WB по `currentShop.marketplace`
- WB-компоненты: `WBContent`, `WBSupplyTable`, `WBWarehouseSummaryTable`, `WBSettingsPanel`, `WBStatusBadge`
- 4 KPI: Итого поставить, Критические SKU, Перезатарка, Хранение/мес (₽)
- Табы: «По товарам» (expandable → склады) / «По складам» (сводка)

**Backend** (`warehouses.py`):

- `GET /warehouses/wb/supply` — JSON endpoint (рекомендации по поставке WB)
- `_build_wb_supply_data()` — общий helper для JSON и Excel endpoints
- Fix: `decimal.Decimal / float` TypeError, `ModuleNotFoundError` для Redis

### fix(warehouses): исправлена логика хранения WB

- Хранение WB **платное с 1-го дня** (не «60 дней бесплатно»)
- 60/90 дней — срок фиксации коэффициентов поставки (60 — большинство, 90 — одежда/обувь)
- Overstock = `turnover_days > target_days` (настраиваемый), не `> 60`
- Acceptance: «Без коэфф.» вместо «Бесплатно»

### docs: обновление архитектурной документации

- `04_BACKEND_API.md`: секция WB Supply (endpoints, response schema, WB-специфика, источники данных)
- `06_FRONTEND.md`: единый `WarehouseSupplyPage` (Ozon + WB ветки, WB-компоненты)

## 2026-03-10

### feat(warehouses): WB Supply — поставки с учётом платного хранения

**Новый endpoint** `GET /api/v1/warehouses/wb/supply/xlsx`:

- 4 листа Excel: Рекомендации, Сводка, Тарифы складов, Риск хранения
- Данные из: `fact_inventory_snapshot`, `fact_orders_raw`, `dim_products`, `fact_wb_acceptance_tariffs`
- Оборачиваемость по складам, расчёт стоимости хранения (объём × тариф × коэффициент)
- Рекомендации: `need = max(0, ceil(daily × target_days × safety − stock))`

**Новая инфраструктура:**

- Таблица CH `fact_wb_acceptance_tariffs` — тарифы хранения/логистики по складам
- WB API `/api/tariffs/v1/acceptance/coefficients` — 144 склада, 14 дней
- `WBTariffsService` + Celery task `sync_wb_tariffs` (ежедневно)

### docs: обновление архитектурной документации

- `02_DATA_MODEL.md`: добавлена таблица `fact_wb_acceptance_tariffs` (14 колонок, ReplacingMergeTree)
- `03_CELERY_PIPELINE.md`: добавлена задача `sync_wb_tariffs`, обновлён `sync_all_daily` dispatch
- `04_BACKEND_API.md`: добавлен endpoint `GET /warehouses/wb/supply/xlsx` (4 листа Excel)
- `05_SERVICES.md`: добавлен `WBTariffsService` (22 сервиса), обновлена сводная таблица

### fix(warehouses): исправлены подписи Excel на русский язык

- Все заголовки столбцов переведены на русский (полные названия с единицами измерения)
- Приёмка: «Бесплатно» / «Платно x20» вместо «Free» / «0.0»
- Рекомендации: развёрнутые русскоязычные тексты вместо «CRIT», «OK»
- Отгрузка: «Да» / «Нет» вместо «OK» / «X»
- Оборачиваемость: «Нет продаж» вместо числа 999
- Исправлено: `product_name` → `name` в запросе к `dim_products`

### feat(warehouses): Excel лист «Анализ логистики» (Ozon) — доля влияния маршрутов

- 6-й лист в Excel-экспорте Ozon: «Анализ логистики» по объединённым группам
- Формула: `объём × часы` для маршрутов >29ч (методика Ozon)
- Автогенерация рекомендаций по сплиту поставки
- Цветовая индикация: красный ≥40% влияния, оранжевый ≥20%

## 2026-03-09 (v2)

### feat(finances): Понедельный финансовый отчёт (Ozon)

**Backend** (`backend/app/api/v1/finances.py`):

- Новый endpoint `GET /finances/ozon/weekly-report` — понедельная агрегация P&L за всю историю магазина
- SQL-запросы к `fact_ozon_transactions` с `GROUP BY toMonday()` + рекламные расходы из `fact_ozon_ad_daily`
- COGS-расчёт через `product_costs` (PG) + SKU→offer_id маппинг из `dim_ozon_products`
- 20 колонок: qty, продажи, возвраты, комиссия, компенсации, услуги, продвижение, прочие, ФБО, эквайринг, доставка, к перечислению, себестоимость, ВАЛ + 6 процентных

**Frontend**:

- Новый компонент `WeeklyReportTable.tsx` — Excel-style таблица с:
  - Sticky-колонки (год, неделя, период)
  - Процентные колонки с зелёным фоном
  - Сортировка по всем колонкам, строка «Итого»
  - Экспорт в CSV
- Табы **«P&L»** / **«Пон. отчёт»** на странице Финансов (для Ozon и WB магазинов)
- Lazy-загрузка данных при переключении на таб

### feat(finances): Понедельный финансовый отчёт (WB)

**Backend** (`backend/app/api/v1/finances.py`):

- Новый endpoint `GET /finances/wb/weekly-report` — понедельная агрегация P&L для WB
- SQL-запросы к `fact_finances FINAL` с `GROUP BY toMonday(event_date)` + рекламные расходы из `fact_advert_stats`
- COGS-расчёт через `product_costs` (PG) по `vendor_code`
- 18 колонок: qty, цена розн., реализовано, комиссия WB, возвраты (шт/сум), логистика, хранение, приёмка, удержания, компенсации, реклама, к перечислению, себестоимость, ВАЛ + 4 процентных

**Frontend**:

- `WeeklyReportTable.tsx` теперь поддерживает оба маркетплейса через prop `marketplace`
- Отдельные наборы колонок: OZON_VALUE_COLS / WB_VALUE_COLS
- Таб «Пон. отчёт» показывается и для WB магазинов

### fix(finances): Ozon Logistics split

- Исправлена разбивка Logistics по `operation_type` вместо `delivery_schema` (который пуст для bulk-операций)
- Добавлена колонка «Хранение» (category='Storage')
- Переименованы колонки: «Др. услуги» → «Логистика», «Усл. ФБО» → «ФБО/Поставки»

---

## 2026-03-09

### feat(warehouses): Консолидация поставок по приоритетным кластерам Ozon

**Backend** (`backend/app/api/v1/warehouses.py`):

- Матрица `DELIVERY_HOURS` 25×25 — нормативное время доставки между кластерами (из Ozon 01/2026)
- `_resolve_hub()` — определение оптимального склада отгрузки для кластера спроса
- `hub` / `hub_hours` поля в `SupplyCluster` response
- `hubs` — агрегация поставки по складам отгрузки (HubSummary)
- Excel-экспорт: новый 4-й лист «Поставка по кластерам»
- Excel-экспорт: новый 5-й лист **«Объединённые кластеры»** — 9 хабов вместо 25, сводная поставка
- Excel лист 1: добавлены колонки «Склад отгрузки» и «Доставка, ч» с цветовой индикацией
- `CONSOLIDATED_GROUPS` — 9 объединённых групп кластеров (Москва+Тверь+Ярославль+Беларусь и т.д.)

**Frontend**:

- Новые типы `HubItem`, `HubSummary` в `warehouses.ts`
- Табы «По SKU» / «По кластерам» — переключение между таблицами
- Компонент `HubTable` — collapsible список складов → SKU с need > 0
- Цветовая индикация времени доставки (28ч зелёный, 45ч жёлтый)

---

### feat(warehouses): Раздел «Поставки FBO» — рекомендации по SKU × кластер

**Backend** (`backend/app/api/v1/warehouses.py`):

- `GET /warehouses/ozon/supply` — JSON-рекомендации по SKU × кластер
  - 4 запроса ClickHouse: FBO stocks, sales × cluster, ad boost (7d vs prev 7d), ad metrics
  - Product info из PostgreSQL (dim_ozon_products)
  - Параметры: `sales_period` (14-90), `target_days` (14-90), `safety` (1.0-2.0), `use_ad_boost`
  - Формула: `daily × target_days × safety − FBO_stock × доля`
  - Ad boost: `min(рост_7д / prev_7д, 2.0)`, для новых SKU с рекламой = 1.3x
- `GET /warehouses/ozon/supply/export` — Excel (3 листа: Рекомендации, Сводка, Методология)
- Зарегистрирован `warehouses_router` в `router.py`

**Frontend**:

- `warehouses.ts` — API клиент + TypeScript типы (SupplyItem, SupplyCluster, SupplyResponse)
- `WarehouseSupplyPage.tsx` — полная страница:
  - 4 KPI карточки: Итого поставить, Критические SKU, На внимании, Средний запас
  - Панель настроек: горизонт (14/30/45/60/90), период продаж, safety (0-30%), ad boost toggle
  - Таблица SKU с сортировкой, статус-бейджами, раскрытием кластеров по клику
  - Кнопка «Скачать Excel» (blob download)
- Sidebar: «Склады» → collapsible с children «Поставки»
- App.tsx: роут `/warehouses/supply`

---

### feat: ИИ-анализ событий — Gemini 2.5 Flash (kie.ai)

**Backend** (`events_analysis.py`):

- `POST /events/analysis` — SSE streaming endpoint
- Собирает события из PostgreSQL + KPI из ClickHouse
- **Каталог товаров**: названия + артикулы продавца из `dim_products` / `dim_ozon_products`
- **Привязка событий к товарам**: каждое событие содержит название товара по nm_id
- **Per-product funnel**: показы, клики, корзины, заказы, CTR%, CR→корз%, CR→заказ%
- Формирует промпт с данными, отправляет в Gemini 2.5 Flash через kie.ai API
- Стримит ответ обратно клиенту как Server-Sent Events
- Поддержка Ozon и WB (таблицы заказов/рекламы per marketplace)

**Frontend** (`EventsGraphPage.tsx`, `events_graph.ts`):

- Кнопка «Запустить анализ» в блоке «ИИ-анализ событий»
- SSE-клиент на базе `fetch` + `ReadableStream`
- Стриминг текста с markdown-рендерингом (заголовки, списки, bold, таблицы)
- Loading state, кнопка «Остановить», перезапуск анализа
- **Сброс анализа при смене магазина**: abort stream + reset state в useEffect по `[shop, period, groupBy]`

**Конфигурация**:

- `KIE_AI_API_KEY` в `.env`
- `router.py`: подключён `events_analysis_router`

**Документация**:

- `04_BACKEND_API.md`: секция «ИИ-анализ событий — /api/v1/events/analysis» — endpoint, request, механизм, SSE response
- `06_FRONTEND.md`: секция EventsGraphPage — AI analysis card, SSE streaming, сброс при смене магазина

---

## 2026-03-08

### feat(wb): критические события полного отсутствия на складах

- `STOCK_OUT_FBO_TOTAL` — товар закончился на **всех** складах ФБО
- `STOCK_OUT_FBS_TOTAL` — товар закончился на **всех** складах ФБС
- Агрегация: суммирует остатки по всем складам каждого типа (FBO/FBS)
- Redis: `state:stock_total:{shop_id}:{nm_id}:{fbo|fbs}` — хранит предыдущий агрегат
- Фронтенд: ⚠️ AlertTriangle, красная рамка + ring, визуально отличается от обычного STOCK_OUT

### fix(wb-ads): названия кампаний + ставки в рублях

**Проблема 1**: Рекламные события показывали только ID кампании (напр. «Кампания #34293797»)
без названия.

**Решение**: `event_detector.py` V1/V2 — добавлен `campaign_title` из `campaign.name`
в `event_metadata` для всех `BID_CHANGE` и `STATUS_CHANGE` событий.

**Проблема 2**: Ставки WB хранятся в копейках (100000 = 1000₽), но отображались как
«100 000 ₽» (без деления на 100).

**Решение**: `events.py` — `_format_value` конвертирует kopecks ÷ 100 → «1000 ₽».

### feat(wb): Content-Length based photo change detection

**Проблема**: WB CDN не меняет URL при замене фото — файл перезаписывается по тому же пути.
`extract_photo_id` извлекал `vol/part/nmID/N` — идентичный до и после замены.
ETag и Last-Modified нестабильны (round-robin между CDN нодами).

**Решение**: `Content-Length` из HTTP HEAD запросов — единственный стабильный заголовок
между CDN нодами, меняется только при реальной замене файла.

- `wb_content_service.py`: async HEAD запросы к CDN (`_fetch_photo_fingerprints`)
  - 20 параллельных запросов, timeout 5с, fallback на path-based ID
  - `main_photo_id` = Content-Length главного фото
  - `photos_hash` = MD5 от JSON массива Content-Length всех фото
- `event_detector.py`: `elif` → `if` — main photo и gallery детектируются независимо
  - `CONTENT_PHOTO_ADDED` — фото добавлено (count↑)
  - `CONTENT_PHOTO_REMOVED` — фото удалено (count↓)
  - `CONTENT_PHOTO_ORDER_CHANGED` — фото заменено (count=, hash≠)
- `events.py`: категории, лейблы, детализация для новых типов
- `EventsPage.tsx`: иконки — зелёный Plus (добавлено), красный Minus (удалено)

---

## 2026-03-03

### feat: WB LTV — полный модуль анализа повторных покупок

**Открытие**: обнаружен buyer_id в поле `srid` заказов WB.  
Формула: `substring(splitByChar('.', srid)[1], 1, 8)` для числовых srid 16-19 символов.  
Точность: 97.1% (подтверждена на 2 магазинах, cross-validated с Ozon).

**Backend** (`backend/app/api/v1/wb_ltv.py`):

- `GET /sales/wb/ltv` — KPI, когортная матрица, SKU repeat table, time distribution
- `GET /sales/wb/ltv/chain` — цепочка покупок L1→L5 для конкретного nm_id
- Buyer ID extraction из srid, фильтр числовых srid 16-19 символов
- Зарегистрирован в `router.py`

**Frontend**:

- `frontend/src/api/wb_ltv.ts` — API модуль с типами
- `LtvPage.tsx` расширена для WB: автовыбор API (Ozon/WB) по marketplace магазина

### fix: WB LTV — артикулы и картинки из dim_products

- **Было**: искал в несуществующей `dim_wb_products` → числовые артикулы `01-0001034`
- **Стало**: `dim_products` (name, vendor_code) + CDN `wb_image_url(nm_id)` — как dashboard WB

### docs: архитектурная документация LTV

- `04_BACKEND_API.md`: секция «Клиентская аналитика (LTV)» — 4 endpoints, response schemas, логика Ozon/WB
- `06_FRONTEND.md`: секция `LtvPage` (~710 строк), routing, sidebar «КЛИЕНТЫ»
- Роутинг обновлён: 12 роутеров (добавлены ltv_router, wb_ltv_router)

---

## 2026-02-28

### fix(sales): sticky header/column + обрезка названий

- Первый столбец «Товар» закрепляется при горизонтальном скроле
- Строка заголовков закрепляется при вертикальном скроле (max-h 600px)
- Названия товаров обрезаны до 220px вместо полных длинных строк
- Непрозрачный фон sticky-ячеек через inline styles

## 2026-02-28

### ui(sales): полный редизайн ABC/XYZ

- Карточки: 2 группы (ABC + XYZ) бок о бок, градиенты, крупные цифры
- Матрица: увеличенные ячейки, эмодзи-индикаторы, подписи, hover-эффекты
- Таблица: +Маржа%, +МП расх., +Реклама — полная финансовая картина
- Склонение: товар/товара/товаров
- Названия товаров: шире (min-w-320px), zebra-striping

## 2026-02-28

### fix(sales): ABC/XYZ — чистая прибыль вместо валовой

- **Было**: `profit = revenue - cogs` (только себестоимость)
- **Стало**: `profit = revenue - commission - logistics - storage - acquiring - ad_spend - cogs`
- Источники: `fact_ozon_transactions`, `fact_ozon_ad_daily`, `product_costs`
- Bulk charges (Storage/Acquiring) распределяются пропорционально выручке

## 2026-02-28

### feat(sales): подменю «Продажи» + ABC/XYZ анализ

- **Sidebar рефакторинг**: «Продажи» → collapsible группа с подпунктами:
  - «Обзор продаж» (`/sales`) — прежняя страница
  - «ABC/XYZ анализ» (`/sales/abc-xyz`) — новая страница
- **Backend**: `GET /sales/ozon/abc-xyz` (period, use_profit)
  - ABC: кумулятивная доля выручки/прибыли (A≤80%, B≤95%, C>95%)
  - XYZ: CV понедельных продаж (X<10%, Y<25%, Z≥25%)
- **Frontend**: AbcXyzPage — 6 summary cards, матрица 3×3, сортируемая таблица
- **API**: `abc-xyz.ts` — TypeScript интерфейсы + fetch

### feat(sales): сортировка столбцов таблицы товаров

- SortKey/SortDir state с дефолтной сортировкой по выручке
- Клик по заголовку → ▲/▼ индикатор + сортировка ASC/DESC

### feat(sales): колонка «Цена» с дельтой периода

- Средняя цена товара + изменение в % относительно предыдущего периода

## 2026-02-26

### feat(finances): товарная P&L таблица — детализация по товарам

- `GET /finances/wb/products` — WB P&L по vendor_code (fact_finances + ads + COGS)
- `GET /finances/ozon/products` — Ozon P&L по offer_id (orders + transactions + ads + COGS)
- Фронтенд: `ProductFinanceTable` — сортировка, delta %, цвет маржи, sticky итого
- Колонки WB: Продажи | Выручка | Логистика | Хранение | Реклама | С/с | Прибыль | Маржа
- Колонки Ozon: Продажи | Выручка | Комиссия | Логистика | Реклама | С/с | Прибыль | Маржа

### fix(finances): Ozon — правильные таблицы и поля

- `fact_ozon_finances` (не существует) → `fact_ozon_transactions`
- `sku_id` → `sku`, `spend` → `money_spent` в fact_ozon_ad_daily
- Bulk charges (Логистика/Хранение/Эквайринг) — распределение пропорционально выручке

### fix(finances): WB — двойной учёт рекламы

- Исключены рекламные услуги ("ВБ.Продвижение") из поля `deduction` (удержания) в отчётах реализации, так как расходы на рекламу отдельно запрашиваются из `fact_advert_stats_v3`.

## 2026-02-24

### feat(wb): P&L водопад — новые столбцы

- `revenue_7d` = `retail_amount` (продажи для покупателя)
- `sales_amount` = `current_price × qty` (продажи по цене из админки)
- `payout` = `ppvz_for_pay` (к перечислению от WB)
- `avg_price` = `retail_amount / qty` (средняя цена покупателя)
- `profit` = `payout - mp_fees - COGS - ads`

### fix(frontend): баг таймзоны

- `toISOString().slice(0,10)` → локальное форматирование
- Даты больше не сдвигаются на -1 день при UTC+5

### ui(table): редизайн таблицы товаров

- 11→9 столбцов (убраны Ср. цена, Возвр., События)
- DRR: цветной текст вместо бейджа
- Остатки: одно число + тултип FBO/FBS
- Дельта: инлайн после штук
- Единый 2-строчный формат ячеек
- Фиксированные ширины (940px), без горизонтального скролла

### fix(wb): UnboundLocalError sales_amount — пустая таблица

- `sales_amount` определялся ПОСЛЕ использования в формуле DRR
- Python выбрасывал `UnboundLocalError`, API `/products/wb` возвращал 500
- Перемещён `current_price` + `sales_amount` перед расчётом DRR

### feat(tooltip): подробная разблюдовка удержаний Услуги МП

- Тултип при наведении: Комиссия / Логистика / Хранение / Прочее
- Каждая статья: сумма + % от продаж
- Нулевые статьи скрываются автоматически

### fix(drr): ДРР считается от продаж (не от выплаты)

- Было: ДРР = ad_spend / payout (ppvz_for_pay)
- Стало: ДРР = ad_spend / sales_amount (цена × шт)

### fix(ux): тултипы маркетплейс-нейтральные + баннер 7 дней

**Тултипы заголовков:**

- Убраны все упоминания «WB», «ppvz_for_pay», «СПП»
- Тексты универсальны для WB и Озон: «маркетплейс» вместо «WB»
- Понятный язык: «расчётный счёт» вместо «р/с (ppvz_for_pay)»

**Баннер 7 дней:**

- Amber-предупреждение перед таблицей
- «Данные за 7 дней могут быть неполными»
- Объясняет задержку финансового отчёта маркетплейса 1-3 дня
- Скрывается при 30 днях или пользовательских датах

### feat(ozon): avg_price — реальная цена покупателя после скидок Ozon

**Проблема:** Ср. цена для Озон = цена из ЛК (до скидок площадки). На самом деле покупатель платит меньше.

**Решение:** SQL-запрос к `fact_ozon_orders`:

- `buyer_revenue = (price - total_discount_value) × qty`
- `avg_price = buyer_revenue / orders`
- `sales_amount = avg_price × orders`

**Пример:** `АМ-СОБ-МЕЛ-ЯГ-1` — цена ЛК 953₽, реальная 634₽ (скидка ~34%)

### fix: С/с процент = доля себестоимости в цене

**Проблема:** Тултип говорил «изменение за период» — бессмыслица для ручного поля.
**Решение:** `margin_percent = cost / price × 100` (доля С/с в цене из ЛК).
Цвета: зелёный <30%, жёлтый 30-50%, красный >50%.

## 2026-02-25

### fix(wb): С/с% = cost/price, Прибыль% от цены продажи

- **С/с%**: было `grossProfitPct` (от выплаты, мог быть отрицательный) → `(cost + packaging) / price × 100` (всегда положительный)
- **Прибыль%**: было `profit / payout` → `profit / sales_amount` (от цены продажи)
- Backend WB: `margin = gross_profit / sales_amount` (ранее: / payout)

### fix(ozon): формула прибыли — revenue вместо txn_payout

- Было: `profit = txn_payout - COGS - ad_spend` (txn_payout не привязан к периоду заказов, за w08 = -33k!)
- Стало: `profit = revenue - COGS - mp_fees - ad_spend` (mp_fees = revenue - txn_payout)
- Результат: прибыль математически идентична, но привязана к дате заказа

### fix(ozon): totals — Выплата и Ср. выплата

- `totals.payout` добавлен в API (было 0₽)
- `totals.avg_price` = `payout / orders` (было пусто)
- Фронтенд: ячейка «Ср. выпл.» в итоговой строке (было пустой `<td/>`)

### docs: обновлена архитектурная документация

- `04_BACKEND_API.md`: формула прибыли Ozon, totals schema, margin_percent
- `06_FRONTEND.md`: столбцы таблицы, wbToOzon маппинг, тултипы С/с

## 2026-02-28 — WB Sales Overview (Обзор продаж для Wildberries)

### Backend

- `GET /api/v1/sales/wb` — KPI (заказы, выручка, ср. чек, отмены), дневной график, география (top-20 регионов), top-20 товаров с органической воронкой из `fact_sales_funnel`
- `GET /api/v1/sales/wb/product-daily` — дневная динамика по конкретным товарам (для оверлея на графике)

### Frontend

- `sales.ts`: добавлены `getWbSalesApi()` и `getWbProductDailyApi()`
- `SalesPage.tsx`: адаптирован для обоих маркетплейсов:
  - Автоопределение маркетплейса → вызов нужного API
  - KPI: «Отмены» (XCircle) для WB, «Возвраты» (RotateCcw) для Ozon
  - График: легенда показывает «Отмены» для WB
  - Таблица: «Воронка» для WB (органическая), «Рекл. воронка» для Ozon
  - Блок «Причины возвратов» скрыт для WB (нет данных)

### Исправление: Рекламная воронка WB (fix)

- Заменён источник данных воронки: `fact_sales_funnel` (органическая) → `fact_advert_stats_v3 FINAL` (рекламная)
- Теперь корректные значения: views (рекл. показы), clicks (клики), atbs (корзины), orders (заказы), spend (расходы)
- CTR = clicks/views, CR→корз. = atbs/clicks, CR→заказ = orders/atbs
- Столбцы «Клики» и «CTR» отображаются для WB (ранее ошибочно скрывались)

## 2026-02-28 — WB ABC/XYZ анализ

### Backend

- `GET /api/v1/sales/wb/abc-xyz` — полный ABC/XYZ анализ для товаров WB
  - Revenue/costs: `fact_finances FINAL` (per vendor_code/nm_id)
  - Ad spend: `fact_advert_stats_v3 FINAL` (per nm_id)
  - COGS: `product_costs` (PG, per vendor_code)
  - Product info: `dim_products` (PG, per nm_id)
  - Weekly: `fact_orders_raw FINAL` для XYZ (per nm_id)
  - Commission = revenue - payout (модель WB)

### Frontend

- `abc-xyz.ts`: добавлена `fetchWbAbcXyz()`
- `AbcXyzPage.tsx`: авто-определение маркетплейса → вызов нужного API

## 2026-02-28 — Раздел «Прогноз продаж» для Ozon

### Backend

- `GET /api/v1/sales/ozon/forecast` — трендовый прогноз + unit economics
  - History: дневная выручка/заказы из `fact_ozon_orders`
  - Forecast: линейная регрессия на Python (без numpy) + доверительный коридор ±σ
  - Per-product: CPO, CPC, CTR, CR, ROI из `fact_ozon_ad_daily` + `fact_ozon_transactions`
  - COGS из `product_costs`, info из `dim_ozon_products`

### Frontend

- `ForecastPage.tsx` — комбо-раздел:
  - Трендовый график (Recharts AreaChart + Line, история + прогноз с доверительным коридором)
  - 4 KPI карточки: прогноз выручки/заказов, тренд, направление
  - Симулятор «Что если?» — ползунки бюджета рекламы (×0.5—3.0) и цены (×0.8—1.2) per-product
  - Мгновенный JS-пересчёт заказов/выручки/прибыли при перемещении ползунков
- `forecast.ts` — API клиент с TypeScript типами
- Роут `/sales/forecast`, навигация в Sidebar (иконка Sparkles)

## 2026-02-28 — NeuralProphet вместо линейной регрессии

### Backend

- Заменена линейная регрессия на **NeuralProphet 0.9.0** (PyTorch)
  - Мультипликативная недельная сезонность
  - Авторегрессия AR(7) — последние 7 дней влияют на прогноз
  - `n_forecasts=horizon` + диагональное извлечение yhat
  - Доверительный интервал из исторической волатильности
  - Fallback на 7-дневное скользящее среднее при ошибке модели

### Зависимости

- `neuralprophet==0.9.0`, `torch==2.2.0` (CPU-only), `pandas<3.0`
- Dockerfile обновлён: `--index-url https://download.pytorch.org/whl/cpu`
- `.gitignore`: добавлен `backend/lightning_logs/`

### Устранённые проблемы

- pandas 3.0 → ошибка `Series.view()` (фикс: pandas<3.0)
- torch 2.10 → несовместимость API (фикс: torch==2.2.0)
- `n_forecasts` не был задан → только 1 день прогноза
- Диагональное извлечение: yhat1 для дня 1, yhat2 для дня 2, и т.д.

## 2026-02-28 — LightGBM per-SKU прогноз с воронкой

### Backend

- Добавлен **LightGBM** per-SKU прогноз с 21 lagged фичей (показы, клики, корзины, рекл. расход за 1-3 дня)
- Новый endpoint `GET /sales/ozon/forecast/sku` — прогноз заказов, выручки, прибыли по каждому SKU
- Feature importance — показывает какой фактор больше всего влияет на продажи каждого SKU
- Формула прибыли: выручка − комиссия − логистика − реклама − себестоимость
- Зависимости: `lightgbm>=4.0`, `scikit-learn`

### Frontend

- Новая секция «Прогноз по SKU (LightGBM)» на странице прогноза
- Кнопки выбора SKU (топ-5 по выручке), KPI карточки, график заказов с confidence band
- Панель «Влияние факторов» — визуальные бары feature importance
- Блок «Структура прибыли» — разбивка: выручка, комиссия, логистика, реклама, чистая прибыль

### Инфраструктура

- Nginx proxy timeout увеличен с 60с до 180с для ML-эндпоинтов
- Dockerfile: torch незафиксирован (2.2.0 удалён с CDN PyTorch)

## 2026-03-01 — Очистка ветки: удалён ML-код для деплоя

### Удалено из feature/sales-page

- NeuralProphet + LightGBM эндпоинты и хелперы из `sales.py` (−942 строки)
- `ForecastPage.tsx`, `forecast.ts` (удалены)
- ML-зависимости: `torch`, `neuralprophet`, `lightgbm`, `scikit-learn`, `pandas`
- Torch из Dockerfile / Dockerfile.prod
- Роут `/sales/forecast` из App.tsx и Sidebar.tsx
- Nginx timeout возвращён с 180с на 60с

### Сохранено

- Весь ML-код сохранён в ветке `feature/forecast-ml`

### Исправлено

- docker-compose.yml: nginx переключён с prod-конфига (SSL) на dev-конфиг
- Docker build теперь 60с вместо 5+ минут

## 2026-03-01 — Обновление архитектурной документации

### 04_BACKEND_API.md

- Добавлена секция «Продажи — /api/v1/sales» (6 endpoints)
- Добавлена секция «Финансы — /api/v1/finances» (4 endpoints)
- Роутинг обновлён: 10 роутеров вместо 6
- Удалены forecast endpoints (код в feature/forecast-ml)

### 06_FRONTEND.md

- Добавлены секции SalesPage (1019 строк) и AbcXyzPage (489 строк)
- Routing обновлён: 6 активных роутов
- Убран «Прогноз» из sidebar

### 07_INFRASTRUCTURE.md

- Документировано различие nginx.conf (dev) и nginx.prod.conf (prod)
- ML зависимости удалены, Docker build ~60с

## 2026-03-01

### fix(forecast): WB profit calculation — real mp_fees from fact_finances FINAL

- **Проблема**: forecast использовал `fact_finances_latest.retail_amount` для расчёта mp_fees,
  но у некоторых магазинов (shop_id=18) `retail_amount` всегда = 0.
  Fallback 5.5% давал нереально высокую прибыль (маржа 51% вместо 13.4%)
- **Решение**: перешли на `fact_finances FINAL` + `JSONExtractFloat(raw_payload, 'retail_price_withdisc_rub')` —
  тот же подход что и раздел Финансов (`finances.py`)
- **Компоненты mp_fees**: Commission(rev-payout) + Logistics(wb_delivery_rub) + Storage + Acquiring + Penalties + Deductions
- **Fallback**: обновлён с 5.5% до 40%
- **Файл**: `backend/app/api/v1/sales.py` (endpoint `/sales/wb/forecast`)

## 2026-03-01

### ui(sales+finances): единый стиль таблиц — sticky + scroll

**SalesPage.tsx → TopProductsTable:**

- Обёрнут в `rounded-2xl` контейнер с title bar (убрана дублирующая Card)
- `max-h-[600px]` + `overflow-auto` — вертикальный скролл
- Sticky header (`sticky top-0 z-20`)
- Sticky первый столбец «Товар» (`stickyBase` + box-shadow)
- Zebra striping: `idx % 2 === 0 ? bg-card : bg-muted/0.06`
- Ячейки: `px-4 py-3.5`, `text-[13px]`, `font-semibold`
- Sort indicator: `text-[hsl(var(--primary))]` вместо opacity

**ProductFinanceTable.tsx:**

- `rounded-2xl` контейнер + title bar c счётчиком товаров
- `max-h-[600px]` + `overflow-auto`
- Sticky header (`sticky top-0 z-20`)
- Sticky footer «Итого» (`sticky bottom-0 z-20`) — всегда виден
- Sticky первый столбец с `stickyCol` + box-shadow
- Zebra striping, единые размеры ячеек

## 2026-03-01

### ui(sales+finances): unified table style — matching ABC/XYZ

**TopProductsTable** (SalesPage.tsx):

- Rounded-2xl card container с title bar
- `max-h-[600px]` scrollable area
- Sticky header (vertical z-20) + sticky first column 'Товар' (horizontal, box-shadow)
- Zebra striping (odd rows bg-muted/0.06)
- Убрана дублирующая Card-обёртка
- Единые padding/font-size ячеек

**ProductFinanceTable** (ProductFinanceTable.tsx):

- Тот же rounded-2xl card + title bar + счётчик товаров
- `max-h-[600px]` vertical scroll
- Sticky header (z-20) + sticky first column + **sticky ИТОГО footer** (bottom-0)
- Zebra striping, единые шрифты через HSL vars

## 2026-03-02

### docs: полный аудит и обновление архитектурной документации

**06_FRONTEND.md:**

- Добавлены секции ForecastPage (387 строк), WBProductsPage (644 строки), FinancesPage (1009 строк)
- Routing diagram: 7 активных + 4 placeholder маршрута (ранее неполный)
- Sidebar навигация: полная таблица с секциями, вложенным меню и статусами
- Unified Table Design: единый стиль таблиц (sticky header/column/footer, max-h-[600px], zebra)
- Компоненты: добавлены ProductFinanceTable, DateRangePicker, Badge
- API Layer: 9 модулей (добавлены forecast.ts, wb-products.ts)

**01_OVERVIEW.md:**

- Frontend routing diagram: 7 активных + 4 placeholder (ранее: 2 страницы)
- API Layer: 9 модулей с перечислением

**04_BACKEND_API.md:**

- Добавлена секция «Товары WB» — 4 endpoints (GET list, PATCH cost, POST bulk, GET template)
- Sales: +2 forecast endpoints (/ozon/forecast, /wb/forecast), итого 8
- forecast_engine.py — внутренняя утилита (не роутер)

## 2026-03-02

### feat: LTV клиентов Ozon — полный раздел анализа повторных покупок

**Backend (app/api/v1/ltv.py):**

- 2 API endpoints: `GET /sales/ozon/ltv` и `GET /sales/ozon/ltv/chain`
- KPI метрики: уникальные клиенты, повторные, retention rate, средний LTV, avg check
- Когортная retention матрица (месячные когорты с % удержания до +6 мес)
- Таблица SKU с повторными покупками (конверсии →2/→3, avg days between, LTV repeat)
- Распределение времени до повторной покупки (гистограмма по бакетам)
- Цепочка продаж L1→L5 (кросс-продажи после покупки конкретного SKU)
- ClickHouse-совместимые запросы (window functions, safe NaN handling)

**Frontend:**

- `ltv.ts` — API модуль с TypeScript типами
- `LtvPage.tsx` (673 строки) — KPI карточки, cohort heatmap, interactive chain visualization, SKU table с сортировкой/поиском, histogram
- Route `/customers/ltv` в App.tsx
- Sidebar: пункт «Клиенты → LTV анализ»
