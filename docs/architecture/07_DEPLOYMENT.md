# 07. Деплой и Инфраструктура

## Доступ к серверу

| Параметр     | Значение                                           |
| ------------ | -------------------------------------------------- |
| **IP**       | `5.42.98.106`                                      |
| **User**     | `root`                                             |
| **SSH ключ** | `~/.ssh/id_rsa` (на маке разработчика)             |
| **Пароль**   | `k-n7*-FmhJA3Yj` (fallback, если ключ не добавлен) |

```bash
# Подключение (без пароля):
ssh -i ~/.ssh/id_rsa root@5.42.98.106

# Если новый агент — SSH ключ уже добавлен в authorized_keys на сервере.
# Нужен лишь файл ~/.ssh/id_rsa на локальной машине.
```

## Быстрый деплой

```bash
# Из корня репозитория:
./deploy.sh               # Быстрый деплой (~30 сек) — рекомендуется
./deploy.sh --full-rebuild # Полная пересборка Docker (~10 мин) — только при изменении pip-пакетов или Dockerfile
```

### Когда что использовать

| Изменение                                       | Команда                                 |
| ----------------------------------------------- | --------------------------------------- |
| Изменения Python/TS кода                        | `./deploy.sh`                           |
| Добавлены новые pip-пакеты в `requirements.txt` | `./deploy.sh --full-rebuild`            |
| Изменён `Dockerfile.prod`                       | `./deploy.sh --full-rebuild`            |
| Изменён `nginx.prod.conf`                       | `docker exec mms-nginx nginx -s reload` |

## Структура Docker контейнеров

```
mms-backend        # FastAPI (порт 8000 внутри)
mms-celery-worker  # Celery worker (очереди: fast,sync,heavy,default,backfill)
mms-celery-beat    # Celery scheduler (cron-задачи)
mms-nginx          # Nginx reverse proxy + static files (порт 80/443)
mms-clickhouse     # ClickHouse аналитика
mms-redis          # Redis (брокер Celery + кэш)
mms-postgres       # ВНЕШНИЙ PostgreSQL (TWC1 облако)
```

> **Важно:** PostgreSQL — внешний managed-сервис, не в Docker. URL в `.env`

## Конфигурация Nginx (prod)

Файл: `nginx/nginx.prod.conf`

```nginx
location / {
    root /usr/share/nginx/html;  # Смонтирован из ./frontend/dist
    try_files $uri $uri/ /index.html;  # SPA routing
}
location /api/ {
    proxy_pass http://backend:8000;  # FastAPI backend
}
```

> **Типичная ошибка:** если `proxy_pass http://frontend` вместо `try_files` — сервируется dev-сборка!

## Типичные проблемы и решения

### 1. Blank screen на проде, но локально работает

**Причина:** Nginx отдаёт dev-сборку вместо `dist/`  
**Проверка:** `curl https://mp-control.ru/ | grep '/@react-refresh'` — если есть → dev-сборка  
**Решение:** Проверить `nginx.prod.conf`, должно быть `try_files`, а не `proxy_pass http://frontend`

### 2. Данные на проде не совпадают с локалом (реклама=0, mp_fees=0)

**Причина:** Backend-контейнер работает со СТАРЫМ Docker образом (git pull обновил файлы, но не контейнер)  
**Проверка:** `docker logs mms-backend | grep "Unknown expression identifier"`  
**Решение:** `./deploy.sh` (docker cp + restart)

### 3. Загрузка Excel возвращает 400

**Причина:** `openpyxl` не установлен в образе (старый образ)  
**Быстрое решение:** `docker exec mms-backend pip install openpyxl`  
**Постоянное решение:** `./deploy.sh --full-rebuild`

### 4. Docker compose build завис / занимает 10+ минут

**Норма:** Build 3 сервисов из одного Dockerfile = 5-15 мин (скачивание слоёв, pip install)  
**Если завис:** Нехватка памяти (сервер 72% RAM). Собирай по одному: `docker compose build backend`  
**Подсказка:** Используй `./deploy.sh` (docker cp) — не требует rebuild для Python-изменений

### 5. SSH expect-скрипт зависает на вводе пароля

**Причина:** expect не получает пароль при некоторых конфигурациях  
**Решение:** SSH ключ добавлен в `~/.ssh/authorized_keys` на сервере → используй `-i ~/.ssh/id_rsa`

## Переменные окружения (prod)

Файл на сервере: `/opt/mp-control/.env`

| Переменная            | Назначение                         |
| --------------------- | ---------------------------------- |
| `POSTGRES_URL`        | External PostgreSQL (TWC1)         |
| `CLICKHOUSE_HOST`     | `clickhouse` (docker network)      |
| `CLICKHOUSE_PASSWORD` | `d06927bfe3c2a3287abf089cedd4e1ef` |
| `CLICKHOUSE_USER`     | `default`                          |
| `REDIS_HOST`          | `redis`                            |
| `SECRET_KEY`          | JWT signing key                    |

## История изменений

| Дата       | Что                                                                     | Коммит    |
| ---------- | ----------------------------------------------------------------------- | --------- |
| 2026-02-22 | fix: nginx try_files вместо proxy_pass (blank screen)                   | `1f94ceb` |
| 2026-02-22 | fix: fmtMoney/fmtNum null-safe (TypeError на проде)                     | `10852af` |
| 2026-02-22 | fix: isFinite check для gross_profit_delta (NaN%)                       | `d133ab0` |
| 2026-02-22 | fast-deploy: docker cp products.py + restart backend (offer_id→sku fix) | —         |
| 2026-02-22 | fix: установлен openpyxl в контейнер (bulk upload 400)                  | —         |
