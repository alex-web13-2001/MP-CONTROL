# MP-CONTROL — Agent Onboarding

> **Прочитай этот файл ПЕРВЫМ при начале работы с проектом.**  
> Затем изучи архитектурную документацию по ссылкам ниже.

---

## Что это за проект

**MP-CONTROL** — SaaS-платформа аналитики для маркетплейсов (Ozon + Wildberries).  
Мультимагазинная архитектура: один пользователь → несколько магазинов → единый интерфейс.

**Стек:** FastAPI (Python 3.11) + React 18 (Vite/TypeScript) + PostgreSQL + ClickHouse + Redis + Celery + Docker.

---

## Обязательное чтение перед началом работы

**Прочитай эти документы через `view_file` в указанном порядке:**

| Приоритет | Документ                                  | Что узнаешь                                               |
| --------- | ----------------------------------------- | --------------------------------------------------------- |
| 🔴 1      | `docs/architecture/01_OVERVIEW.md`        | Архитектура, стек, потоки данных, структура проекта       |
| 🔴 2      | `docs/architecture/02_DATA_MODEL.md`      | Все таблицы PostgreSQL и ClickHouse, связи, ключевые поля |
| 🟡 3      | `docs/architecture/04_BACKEND_API.md`     | REST API endpoints, request/response schemas              |
| 🟡 4      | `docs/architecture/03_CELERY_PIPELINE.md` | Фоновые задачи, расписание, потоки синхронизации          |
| 🟢 5      | `docs/architecture/05_SERVICES.md`        | Бизнес-логика: event_detector, content_service, etc.      |
| 🟢 6      | `docs/architecture/06_FRONTEND.md`        | Страницы, компоненты, routing, stores                     |
| 🟢 7      | `docs/architecture/07_INFRASTRUCTURE.md`  | Docker, Nginx, деплой, мониторинг                         |
| 📝 8      | `docs/CHANGELOG.md`                       | История ВСЕХ изменений — читай ПЕРВЫЕ ~100 строк          |

> **Совет:** если задача касается конкретного модуля — читай только релевантные доки (приоритет 1-2 всегда, остальное по необходимости).

---

## Ключевая архитектура (краткая справка)

```
┌───────────────────────────────────────────────────────────────┐
│                         Frontend (React)                       │
│  localhost:80 → Nginx → /api/* → backend:8000                  │
│                       → /*    → static SPA                     │
└────────────────────────┬──────────────────────────────────────┘
                         │
┌────────────────────────▼──────────────────────────────────────┐
│                    Backend (FastAPI :8000)                      │
│  13 роутеров: auth, shops, products, dashboard, events,        │
│  finance-reports, sales, ltv, commercial, advertising,         │
│  events_analysis                                                │
└──────┬──────────────────┬──────────────────┬──────────────────┘
       │                  │                  │
  PostgreSQL          ClickHouse           Redis
  (dim_*, event_log,  (fact_*, log_*)      (state:*, кеш,
   shops, users)      orders, ads, stats    очереди Celery)
```

### Базы данных

- **PostgreSQL** — каталог товаров (`dim_products`, `dim_ozon_products`), события (`event_log`), пользователи, магазины, себестоимость
- **ClickHouse** — аналитические данные: заказы (`fact_orders_raw`, `fact_ozon_orders`), реклама (`fact_advert_stats_v3`, `fact_ozon_ad_daily`), финансы (`fact_wb_reports`, `fact_ozon_transactions`), прогнозы
- **Redis** — состояние для event detection (`state:bid:*`, `state:content:*`, `state:price:*`), кеш кампаний, очереди Celery

### Маркетплейсы

Два маркетплейса с разными API, но **единый интерфейс**:

| Аспект         | Wildberries                                                       | Ozon                                        |
| -------------- | ----------------------------------------------------------------- | ------------------------------------------- |
| API домены     | `advert-api`, `content-api`, `statistics-api`, `seller-analytics` | `api-seller.ozon.ru`, `performance.ozon.ru` |
| ID товара      | `nm_id` (число)                                                   | `product_id` + `sku` + `offer_id`           |
| ID в event_log | `nm_id`                                                           | `nm_id` = `product_id`                      |
| Валюта ставок  | Копейки (`bids_kopecks`)                                          | Рубли                                       |
| Prefix событий | без префикса (`BID_CHANGE`)                                       | `OZON_` (`OZON_BID_CHANGE`)                 |

---

## Важные правила проекта

1. **Порт:** проект запускается на **порту 80** (`localhost`), НЕ на 5173/3000
2. **Docker:** `docker compose up -d` запускает все сервисы. Backend mount: `./backend:/app`
3. **Rebuild backend:** `docker compose up -d --build backend` (при изменении requirements.txt или Dockerfile)
4. **Frontend dev:** `cd frontend && npm run dev` — Vite HMR, проксирует API через Nginx
5. **Документация:** после КАЖДОГО изменения кода — обновлять доки по workflow `/update-docs` (`.agent/workflows/update-docs.md`)
6. **Общение:** всегда на **русском языке**
7. **Деплой:** НИКОГДА не деплой без явного запроса. Для деплоя используй workflow `/deploy`

---

## Структура директорий

```
MP-CONTROL/
├── backend/
│   ├── app/
│   │   ├── api/v1/          # 13 FastAPI роутеров
│   │   ├── core/            # redis_state, marketplace_client, security
│   │   ├── models/          # SQLAlchemy модели (PostgreSQL)
│   │   ├── schemas/         # Pydantic schemas
│   │   └── services/        # Бизнес-логика
│   ├── celery_app/tasks/    # Celery задачи (tasks.py ~4300 строк)
│   └── alembic/             # Миграции PostgreSQL
├── frontend/
│   └── src/
│       ├── api/             # Axios клиенты
│       ├── pages/           # React страницы
│       ├── components/      # Переиспользуемые компоненты
│       └── stores/          # Zustand stores
├── docker/
│   ├── clickhouse/init.sql  # Схема ClickHouse (~950 строк)
│   └── nginx/               # Nginx конфиг
├── docs/
│   ├── architecture/        # 7 архитектурных документов
│   └── CHANGELOG.md         # История изменений
└── .agent/
    ├── rules/               # Правила для агента
    └── workflows/           # deploy, update-docs
```

---

## Частые задачи — где искать код

| Задача                  | Файлы                                                                             |
| ----------------------- | --------------------------------------------------------------------------------- |
| Новый API endpoint      | `backend/app/api/v1/` + `router.py`                                               |
| Изменить БД PostgreSQL  | `backend/app/models/` + alembic migration                                         |
| Изменить БД ClickHouse  | `docker/clickhouse/init.sql`                                                      |
| Новый event type        | `backend/app/services/event_detector.py` + `backend/app/api/v1/events.py`         |
| Event detection логика  | `backend/app/services/event_detector.py` (EventDetector, CommercialEventDetector) |
| Celery задача           | `backend/celery_app/tasks/tasks.py`                                               |
| Redis state             | `backend/app/core/redis_state.py`                                                 |
| Frontend страница       | `frontend/src/pages/`                                                             |
| API клиент фронтенда    | `frontend/src/api/`                                                               |
| ИИ-анализ событий       | `backend/app/api/v1/events_analysis.py`                                           |
| Контент-мониторинг WB   | `backend/app/services/wb_content_service.py`                                      |
| Контент-мониторинг Ozon | `backend/app/services/ozon_content_service.py`                                    |

---

## Env переменные (.env)

Ключевые переменные окружения (файл `.env` в корне):

- `POSTGRES_*` — подключение к PostgreSQL
- `CLICKHOUSE_*` — подключение к ClickHouse
- `REDIS_URL` — Redis для state + Celery
- `JWT_SECRET_KEY` — JWT авторизация
- `KIE_AI_API_KEY` — API ключ для Gemini 2.5 Flash (ИИ-анализ)
