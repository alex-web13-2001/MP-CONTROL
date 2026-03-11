---
description: Deploy MP-CONTROL to production server
---

# Deploy MP-CONTROL

## Предварительно

1. Убедиться что все изменения закоммичены и запушены в `main`
2. Документация обновлена по workflow `/update-docs`

## Деплой

// turbo-all

3. Push в main:
```bash
cd /Users/alex/Documents/Репы/MP-CONTROL && git push origin main
```

4. Запустить серверный деплой-скрипт:
```bash
ssh -i ~/.ssh/id_rsa root@5.42.98.106 "cd /opt/mp-control && bash deploy/deploy.sh"
```

Скрипт автоматически выполняет 6 шагов:
1. `git fetch + reset --hard origin/main` — sync с GitHub
2. **ClickHouse миграции** — `docker/clickhouse/migrations/*.sql` применяются ДО рестарта контейнеров
3. `docker compose build + up` — backend, celery (Alembic/PostgreSQL миграции через entrypoint.sh автоматически)
4. `npm ci && npm run build` — frontend
5. `docker compose restart nginx` — обновление bind mount для frontend/dist
6. Health check

5. Проверить что всё работает:
```bash
curl -s -o /dev/null -w '%{http_code}' https://mp-control.ru/api/v1/auth/health
```

## Миграции БД

### PostgreSQL (Alembic)
- Применяются **автоматически** при рестарте backend через `entrypoint.sh`
- Новые миграции: `cd backend && alembic revision --autogenerate -m "описание"`

### ClickHouse
- SQL файлы: `docker/clickhouse/migrations/NNN_описание.sql`
- Применяются **автоматически** при деплое скриптом `backend/scripts/run_ch_migrations.py`
- Формат: `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` (идемпотентно)

## Важно

- **Сервер:** `5.42.98.106` (Timeweb Cloud VPS)
- **Путь на сервере:** `/opt/mp-control`
- **Git credentials** на сервере настроены (HTTPS без пароля)
- **НЕ** делать `rm -rf dist` на сервере — ломает Docker bind mount nginx
- **НЕ** делать `git commit` на сервере — создаёт divergent branches
- **НЕ** запускать корневой `deploy.sh` — он удалён, используй `deploy/deploy.sh`
