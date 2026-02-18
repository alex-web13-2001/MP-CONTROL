# MP-CONTROL — Инфраструктура

> Docker, переменные окружения, Nginx, база данных, деплой.

---

## Docker Compose

Вся система запускается одной командой:

```bash
docker-compose up -d
```

### 12 контейнеров

```mermaid
graph TB
    subgraph "Reverse Proxy"
        Nginx["mms-nginx<br/>:80"]
    end

    subgraph "Application"
        Backend["mms-backend<br/>:8000<br/>FastAPI + Uvicorn"]
        Frontend["mms-frontend<br/>:3000<br/>Vite Dev Server"]
        CeleryFast["mms-celery-fast<br/>Q: fast, C: 4"]
        CelerySync["mms-celery-sync<br/>Q: sync,heavy,default, C: 8"]
        CeleryBackfill["mms-celery-backfill<br/>Q: backfill, C: 2"]
        CeleryBeat["mms-celery-beat"]
    end

    subgraph "Data"
        PG["mms-postgres<br/>:5432 (ext :5455)"]
        CH["mms-clickhouse<br/>:8123, :9000"]
        Redis["mms-redis<br/>:6379"]
    end

    subgraph "Admin"
        PGAdmin["mms-pgadmin<br/>:5050"]
        Tabix["mms-tabix<br/>:8080"]
    end

    Nginx --> Backend
    Nginx --> Frontend
```

### Зависимости (depends_on)

```
backend     → postgres, redis, clickhouse
celery-*    → postgres, redis, clickhouse, backend
celery-beat → celery-sync
frontend    → backend
nginx       → backend, frontend
pgadmin     → postgres
tabix       → clickhouse
```

---

## Переменные окружения

Файл: `.env` (не коммитится, есть `.env.example`)

| Переменная              | Default                | Описание                                          |
| ----------------------- | ---------------------- | ------------------------------------------------- |
| **Application**         |                        |                                                   |
| `DEBUG`                 | `false`                | Debug mode                                        |
| `SECRET_KEY`            | —                      | **ОБЯЗАТЕЛЬНО!** Ключ для JWT + Fernet шифрования |
| **PostgreSQL**          |                        |                                                   |
| `POSTGRES_HOST`         | `postgres`             | Hostname (в docker-сети)                          |
| `POSTGRES_PORT`         | `5432`                 | Порт                                              |
| `POSTGRES_USER`         | `mms`                  | Пользователь                                      |
| `POSTGRES_PASSWORD`     | `mms_secret`           | Пароль                                            |
| `POSTGRES_DB`           | `mms`                  | База данных                                       |
| **ClickHouse**          |                        |                                                   |
| `CLICKHOUSE_HOST`       | `clickhouse`           | Hostname                                          |
| `CLICKHOUSE_PORT`       | `8123`                 | HTTP порт                                         |
| `CLICKHOUSE_USER`       | `default`              | Пользователь                                      |
| `CLICKHOUSE_PASSWORD`   | —                      | Пароль (пустой по умолчанию)                      |
| `CLICKHOUSE_DB`         | `mms_analytics`        | БД                                                |
| **Redis**               |                        |                                                   |
| `REDIS_HOST`            | `redis`                | Hostname                                          |
| `REDIS_PORT`            | `6379`                 | Порт                                              |
| `REDIS_DB`              | `0`                    | Номер БД                                          |
| **Celery**              |                        |                                                   |
| `CELERY_BROKER_URL`     | `redis://redis:6379/0` | Брокер                                            |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/0` | Бэкенд результатов                                |
| **pgAdmin**             |                        |                                                   |
| `PGADMIN_EMAIL`         | `admin@mms.local`      | Логин pgAdmin                                     |
| `PGADMIN_PASSWORD`      | `admin`                | Пароль pgAdmin                                    |

### Генерация SECRET_KEY

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

> [!CAUTION]
> `SECRET_KEY` используется для JWT подписи и Fernet шифрования API-ключей маркетплейсов. При смене ключа — все сессии и зашифрованные ключи станут невалидными.

---

## Nginx

Файл: `nginx/nginx.conf`

```nginx
upstream backend  { server backend:8000; }
upstream frontend { server frontend:3000; }

server {
    listen 80;

    # API → backend
    location /api {
        proxy_pass http://backend;
        proxy_set_header Upgrade $http_upgrade;   # WebSocket
        proxy_connect_timeout 60s;
    }

    # Всё остальное → frontend (SPA)
    location / {
        proxy_pass http://frontend;
        proxy_set_header Upgrade $http_upgrade;   # HMR WebSocket
    }

    # Health check
    location /health {
        return 200 "healthy\n";
    }
}
```

**Gzip:** включён для text/css, JSON, JavaScript.

---

## Инициализация БД

### PostgreSQL

Файл: `docker/postgres/init.sql` — выполняется при первом запуске контейнера.

Создаёт: extensions (`uuid-ossp`, `pgcrypto`) → 12 таблиц → индексы → triggers (`update_updated_at`).

### ClickHouse

Файл: `docker/clickhouse/init.sql` — выполняется при первом запуске.

Создаёт: database `mms_analytics` → 16 таблиц → 3 Materialized Views → 6 Views.

---

## Volumes (persistent data)

| Volume            | Mount                      | Назначение        |
| ----------------- | -------------------------- | ----------------- |
| `postgres-data`   | `/var/lib/postgresql/data` | Данные PostgreSQL |
| `clickhouse-data` | `/var/lib/clickhouse`      | Данные ClickHouse |
| `redis-data`      | `/data`                    | Persistence Redis |

---

## Порты (exposed)

| Порт   | Контейнер  | Назначение                           |
| ------ | ---------- | ------------------------------------ |
| **80** | nginx      | Единая точка входа                   |
| 8000   | backend    | FastAPI (прямой доступ для отладки)  |
| 3000   | frontend   | Vite dev server                      |
| 5455   | postgres   | PostgreSQL (внешний, mapped из 5432) |
| 8123   | clickhouse | ClickHouse HTTP                      |
| 9000   | clickhouse | ClickHouse Native                    |
| 5050   | pgadmin    | pgAdmin Web UI                       |
| 8080   | tabix      | Tabix (ClickHouse GUI)               |
| 6379   | redis      | Redis                                |

---

## Celery конфигурация

Файл: `backend/celery_app/celery.py`

```python
celery_app = Celery('mms')
celery_app.conf.update(
    broker_url = CELERY_BROKER_URL,
    result_backend = CELERY_RESULT_BACKEND,
    task_routes = {
        'celery_app.tasks.*fast*':    {'queue': 'fast'},
        'celery_app.tasks.*sync*':    {'queue': 'sync'},
        'celery_app.tasks.*backfill*': {'queue': 'backfill'},
    },
)
```

### Beat schedule (периодические задачи)

| Task                          | Interval      | Queue |
| ----------------------------- | ------------- | ----- |
| `update_all_bids`             | 60 сек        | fast  |
| `check_all_positions`         | 5 мин         | fast  |
| `sync_all_frequent`           | 30 мин        | sync  |
| `sync_all_ads`                | 60 мин        | sync  |
| `sync_all_campaign_snapshots` | 30 мин        | sync  |
| `sync_all_daily`              | Cron 3:00 UTC | sync  |

---

## Backend Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### Ключевые зависимости (requirements.txt)

| Пакет                             | Назначение                               |
| --------------------------------- | ---------------------------------------- |
| `fastapi` + `uvicorn`             | Web framework + ASGI server              |
| `sqlalchemy[asyncio]` + `asyncpg` | ORM + async PostgreSQL                   |
| `celery[redis]`                   | Task queue                               |
| `redis`                           | Python Redis client                      |
| `clickhouse-connect`              | ClickHouse client                        |
| `curl-cffi`                       | HTTP с JA3 fingerprint spoofing          |
| `cryptography`                    | Fernet шифрование                        |
| `bcrypt`                          | Password hashing                         |
| `pyjwt`                           | JWT tokens                               |
| `httpx`                           | Async HTTP client (Ozon Performance API) |

---

## Локальная разработка

```bash
# 1. Создать .env из примера
cp .env.example .env
# Заполнить SECRET_KEY!

# 2. Поднять всю инфраструктуру
docker-compose up -d

# 3. Проверка
curl http://localhost/health        # nginx health
curl http://localhost:8000/docs     # FastAPI Swagger UI
open http://localhost:5050          # pgAdmin
open http://localhost:8080          # Tabix (ClickHouse)
```

### Ребилд после изменений

```bash
# Backend (Python)
docker-compose restart backend celery-fast celery-sync celery-backfill

# Frontend
docker-compose restart frontend

# Полный ребилд
docker-compose up -d --build
```

---

## Gitignore

Важные исключения:

- `.env` — секреты
- `__pycache__/` — Python cache
- `node_modules/` — npm зависимости
- `celerybeat-schedule` — Celery Beat state
- `*.pyc`, `*.xlsx`, `*.csv` — временные файлы
