# MP-CONTROL — Архитектурный обзор

> **MMS (Marketplace Management System)** — платформа для автоматизации аналитики и управления рекламой на маркетплейсах Wildberries и Ozon.

## Назначение системы

MP-CONTROL решает три ключевые задачи:

1. **Сбор данных** — периодическая синхронизация через API маркетплейсов (продажи, финансы, реклама, остатки, воронка)
2. **Аналитика** — хранение и агрегация данных в ClickHouse для быстрых OLAP-запросов
3. **Управление** — автоматический биддинг рекламы (autobidder), мониторинг событий, управление магазинами

---

## Стек технологий

| Слой                 | Технология                   | Назначение                                                  |
| -------------------- | ---------------------------- | ----------------------------------------------------------- |
| **Backend API**      | FastAPI + asyncpg            | REST API, валидация, аутентификация                         |
| **Task Queue**       | Celery + Redis (broker)      | Фоновые задачи: синхронизация, backfill, autobidder         |
| **OLTP Database**    | PostgreSQL 15                | Пользователи, магазины, настройки, справочники              |
| **OLAP Database**    | ClickHouse                   | Аналитика: заказы, финансы, реклама, воронка, остатки       |
| **State/Cache**      | Redis 7                      | Celery broker, rate limiting, deduplication, campaign state |
| **Frontend**         | React 18 + Vite + TypeScript | SPA с dark/light темой                                      |
| **State Mgmt**       | Zustand (persist)            | authStore (JWT + shops), appStore (тема, sidebar, shop)     |
| **HTTP Client**      | Axios + interceptors         | Auto-refresh JWT, auto-token injection                      |
| **Reverse Proxy**    | Nginx                        | Роутинг /api → backend, / → frontend                        |
| **Containerization** | Docker Compose               | 12 сервисов в единой сети                                   |

---

## Высокоуровневая архитектура

```mermaid
graph TB
    subgraph "Клиент"
        Browser["🌐 Browser<br/>React SPA"]
    end

    subgraph "Reverse Proxy"
        Nginx["Nginx :80"]
    end

    subgraph "Application Layer"
        Backend["FastAPI :8000<br/>REST API"]
        CeleryFast["Celery Fast<br/>Q: fast, C: 4<br/>Autobidder"]
        CelerySync["Celery Sync<br/>Q: sync,heavy,default, C: 8<br/>Синхронизация"]
        CeleryBackfill["Celery Backfill<br/>Q: backfill, C: 2<br/>Историческая загрузка"]
        CeleryBeat["Celery Beat<br/>Планировщик"]
        Frontend["Vite Dev Server :3000"]
    end

    subgraph "Data Layer"
        PG["PostgreSQL :5432<br/>OLTP"]
        CH["ClickHouse :8123<br/>OLAP"]
        Redis["Redis :6379<br/>Broker + State"]
    end

    subgraph "External APIs"
        WB_API["Wildberries API<br/>7 доменов"]
        OZ_API["Ozon API<br/>Seller + Performance"]
    end

    subgraph "Admin Tools"
        PGAdmin["pgAdmin :5050"]
        Tabix["Tabix :8080"]
    end

    Browser -->|HTTP :80| Nginx
    Nginx -->|/api/*| Backend
    Nginx -->|/*| Frontend

    Backend --> PG
    Backend --> CH
    Backend --> Redis

    CeleryFast --> PG
    CeleryFast --> CH
    CeleryFast --> Redis
    CeleryFast --> WB_API
    CeleryFast --> OZ_API

    CelerySync --> PG
    CelerySync --> CH
    CelerySync --> Redis
    CelerySync --> WB_API
    CelerySync --> OZ_API

    CeleryBackfill --> PG
    CeleryBackfill --> CH
    CeleryBackfill --> Redis
    CeleryBackfill --> WB_API
    CeleryBackfill --> OZ_API

    CeleryBeat -->|dispatch| Redis

    PGAdmin --> PG
    Tabix --> CH
```

---

## Docker-сервисы (12 контейнеров)

| Контейнер             | Image                    | Порт       | Назначение                                       |
| --------------------- | ------------------------ | ---------- | ------------------------------------------------ |
| `mms-backend`         | Custom (FastAPI)         | 8000       | REST API                                         |
| `mms-celery-fast`     | Custom                   | —          | Autobidder, позиции (каждую минуту)              |
| `mms-celery-sync`     | Custom                   | —          | Синхронизация данных (каждые 15–60 мин)          |
| `mms-celery-backfill` | Custom                   | —          | Историческая загрузка (при подключении магазина) |
| `mms-celery-beat`     | Custom                   | —          | Планировщик периодических задач                  |
| `mms-frontend`        | Custom (Vite)            | 3000       | React SPA (dev mode)                             |
| `mms-nginx`           | Custom                   | **80**     | Reverse proxy (единая точка входа)               |
| `mms-postgres`        | postgres:15-alpine       | 5455       | Транзакционная БД                                |
| `mms-clickhouse`      | clickhouse-server:latest | 8123, 9000 | Аналитическая БД                                 |
| `mms-redis`           | redis:7-alpine           | 6379       | Broker + кэш + state                             |
| `mms-pgadmin`         | pgadmin4:latest          | 5050       | Web-GUI для PostgreSQL                           |
| `mms-tabix`           | tabix:stable             | 8080       | Web-GUI для ClickHouse                           |

---

## Celery — очереди и расписание

### 3 очереди (3 воркера)

```mermaid
graph LR
    Beat["Celery Beat<br/>Планировщик"]

    subgraph "Queue: fast"
        F1["update_all_bids<br/>⏱ каждые 60 сек"]
    end

    subgraph "Queue: sync"
        S1["sync_all_frequent<br/>⏱ каждые 30 мин"]
        S2["sync_all_ads<br/>⏱ каждые 60 мин"]
        S3["sync_all_campaign_snapshots<br/>⏱ каждые 30 мин"]
    end

    subgraph "Queue: backfill"
        B1["sync_full_history<br/>🔧 по запросу"]
        B2["backfill_ozon_ads<br/>🔧 по запросу"]
        B3["backfill_orders<br/>🔧 по запросу"]
    end

    Beat --> F1
    Beat --> S1
    Beat --> S2
    Beat --> S3
```

| Очередь    | Воркер          | Concurrency | Задачи                                                                              |
| ---------- | --------------- | ----------- | ----------------------------------------------------------------------------------- |
| `fast`     | celery-fast     | 4           | autobidder (update_bids) — каждые 60 сек                                            |
| `sync`     | celery-sync     | 8           | sync_all_frequent (30 мин), sync_all_ads (60 мин), sync_campaign_snapshots (30 мин) |
| `backfill` | celery-backfill | 2           | sync_full_history, backfill_ozon_ads, backfill_orders — при подключении магазина    |

### Дедупликация задач

Используется Redis-based dedup (`_dedup_dispatch`): перед отправкой задачи ставится NX-ключ с TTL. Если ключ существует — задача уже в очереди/выполняется, повторная не ставится.

---

## Anti-Ban система

Три уровня защиты от блокировок API:

```mermaid
graph TB
    Request["API Request"]
    RL["Rate Limiter<br/>Redis: token bucket per shop"]
    PP["Proxy Provider<br/>Sticky sessions per shop"]
    CB["Circuit Breaker<br/>auto-disable shop on 401"]
    MC["MarketplaceClient<br/>JA3 fingerprint spoofing<br/>curl_cffi"]
    API["Marketplace API"]

    Request --> RL
    RL -->|"wait if <br/>rate limited"| PP
    PP -->|"assign proxy"| MC
    MC -->|"retry with backoff"| CB
    CB -->|"check health"| API
```

| Модуль                | Файл                         | Что делает                                            |
| --------------------- | ---------------------------- | ----------------------------------------------------- |
| **Rate Limiter**      | `core/rate_limiter.py`       | Token bucket в Redis, лимит req/мин на магазин        |
| **Proxy Provider**    | `core/proxy_provider.py`     | Ротация прокси, sticky sessions (один прокси на shop) |
| **Circuit Breaker**   | `core/circuit_breaker.py`    | Автоотключение магазина при 401 (невалидный ключ)     |
| **MarketplaceClient** | `core/marketplace_client.py` | Единый HTTP-клиент: JA3 spoofing, retry, logging      |

---

## Аутентификация

```mermaid
sequenceDiagram
    participant Browser
    participant Nginx
    participant FastAPI
    participant PostgreSQL

    Browser->>Nginx: POST /api/v1/auth/login
    Nginx->>FastAPI: proxy
    FastAPI->>PostgreSQL: SELECT user WHERE email
    FastAPI->>FastAPI: bcrypt.verify(password)
    FastAPI-->>Browser: {access_token, refresh_token, user}

    Note over Browser: Zustand persist → localStorage

    Browser->>Nginx: GET /api/v1/shops (Bearer token)
    Nginx->>FastAPI: proxy
    FastAPI->>FastAPI: decode_token(JWT)
    FastAPI->>PostgreSQL: SELECT user + shops
    FastAPI-->>Browser: shops[]

    Note over Browser: При 401 → auto-refresh через interceptor
```

| Механизм     | Деталь                                                  |
| ------------ | ------------------------------------------------------- |
| Хеширование  | bcrypt (прямой, без passlib)                            |
| JWT          | HS256, access: 120 мин, refresh: 7 дней                 |
| Frontend     | Zustand с `persist` → `localStorage`                    |
| Auto-refresh | Axios interceptor: при 401 → POST /auth/refresh → retry |

---

## Шифрование API-ключей

API-ключи маркетплейсов хранятся в PostgreSQL в зашифрованном виде:

- **Модуль:** `core/encryption.py`
- **Алгоритм:** Fernet (симметричное шифрование, AES-128-CBC)
- **Ключ:** Выводится из `SECRET_KEY` через PBKDF2
- **Поля:** `Shop.api_key_encrypted`, `Shop.perf_client_secret_encrypted`

---

## Redis — роли

Redis используется для 5 различных целей:

| Роль               | Ключи                             | Описание                                    |
| ------------------ | --------------------------------- | ------------------------------------------- |
| **Celery Broker**  | `celery-task-meta-*`              | Очереди задач                               |
| **Task Dedup**     | `task_lock:{task}:{shop_id}`      | NX-ключи с TTL для предотвращения дублей    |
| **Rate Limiting**  | `rate_limit:{shop_id}`            | Token bucket: остаток токенов + timestamp   |
| **Campaign State** | `ads:state:{shop_id}:{advert_id}` | Хеши: cpm, status, items, type              |
| **Content State**  | `content:{shop_id}:{nm_id}`       | Хеши: price, stock, image_url, content_hash |

---

## Frontend — структура

```mermaid
graph TB
    subgraph "Routing (BrowserRouter)"
        Login["/login<br/>LoginPage"]
        Register["/register<br/>RegisterPage"]
        Onboarding["/onboarding<br/>OnboardingPage"]

        subgraph "AuthGuard + OnboardingGuard + AppLayout"
            Dashboard["/<br/>DashboardPage"]
            Settings["/settings<br/>SettingsPage"]
        end
    end

    subgraph "Stores (Zustand)"
        AuthStore["authStore<br/>user, token, shops"]
        AppStore["appStore<br/>theme, sidebar, currentShop"]
    end

    subgraph "API Layer"
        ApiClient["axios client<br/>+ interceptors"]
    end

    Login --> AuthStore
    Dashboard --> AppStore
    Dashboard --> ApiClient
    Settings --> ApiClient
```

| Файл                  | Назначение                                                              |
| --------------------- | ----------------------------------------------------------------------- |
| `App.tsx`             | Роутинг: публичные (login, register) + защищённые (dashboard, settings) |
| `AuthGuard.tsx`       | HOC: редирект на /login если нет токена                                 |
| `OnboardingGuard.tsx` | HOC: редирект на /onboarding если нет магазинов                         |
| `AppLayout.tsx`       | Sidebar + Header + content area                                         |
| `authStore.ts`        | JWT, user, shops — persist в localStorage                               |
| `appStore.ts`         | Тема (dark/light), sidebar, текущий магазин                             |
| `client.ts`           | Axios + auto-Bearer + auto-refresh при 401                              |

---

## Wildberries API — 7 доменов

| Домен           | Base URL                              | Сервисы                   |
| --------------- | ------------------------------------- | ------------------------- |
| Content API     | `content-api.wildberries.ru`          | Карточки товаров, фото    |
| Statistics API  | `statistics-api.wildberries.ru`       | Заказы, продажи           |
| Marketplace API | `marketplace-api.wildberries.ru`      | Остатки, склады           |
| Advert API      | `advert-api.wildberries.ru`           | Реклама: кампании, ставки |
| Prices API      | `discounts-prices-api.wildberries.ru` | Цены, скидки              |
| Analytics API   | `seller-analytics-api.wildberries.ru` | Воронка продаж            |
| Supplier API    | `supplier-api.wildberries.ru`         | Финансовые отчёты         |

## Ozon API — 2 домена

| Домен           | Base URL                  | Сервисы                          |
| --------------- | ------------------------- | -------------------------------- |
| Seller API      | `api-seller.ozon.ru`      | Товары, заказы, финансы, остатки |
| Performance API | `api-performance.ozon.ru` | Рекламные кампании (OAuth2)      |

---

## Поток данных (высокий уровень)

```mermaid
graph LR
    subgraph "Маркетплейсы"
        WB["WB API"]
        OZ["Ozon API"]
    end

    subgraph "Celery Workers"
        Sync["sync tasks"]
    end

    subgraph "Services Layer"
        WBS["WB Services (11)"]
        OZS["Ozon Services (10)"]
    end

    subgraph "Storage"
        PG["PostgreSQL<br/>dim_products<br/>dim_warehouses<br/>event_log"]
        CH["ClickHouse<br/>fact_finances<br/>fact_orders_raw<br/>fact_sales_funnel<br/>fact_advert_stats<br/>fact_inventory_snapshot"]
    end

    subgraph "Frontend"
        FE["React SPA"]
    end

    WB --> WBS
    OZ --> OZS
    Sync --> WBS
    Sync --> OZS
    WBS --> PG
    WBS --> CH
    OZS --> PG
    OZS --> CH
    FE -->|"REST API"| PG
    FE -->|"REST API"| CH
```

---

## Дальнейшие документы

| Документ                                                                                            | Содержание                                             |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| [02_DATA_MODEL.md](file:///Users/alex/Documents/Репы/MP-CONTROL/docs/architecture/02_DATA_MODEL.md) | Все таблицы PostgreSQL + ClickHouse: поля, типы, связи |
| 03_CELERY_PIPELINE.md                                                                               | Все задачи, координаторы, потоки данных                |
| 04_BACKEND_API.md                                                                                   | REST endpoints, request/response schemas               |
| 05_SERVICES.md                                                                                      | 21 сервис: API endpoints, transformation, storage      |
| 06_FRONTEND.md                                                                                      | Компоненты, stores, UI-система                         |
| 07_INFRASTRUCTURE.md                                                                                | Docker, env, nginx, деплой                             |
