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

Скрипт автоматически:
- `git fetch + reset --hard origin/main` (sync с GitHub)
- `npm ci && npm run build` (frontend)
- `docker compose build + up` (backend + celery)
- `docker compose restart nginx` (обновление bind mount)
- Health check

5. Проверить что всё работает:
```bash
curl -s -o /dev/null -w '%{http_code}' https://mp-control.ru/api/v1/auth/health
```

## Важно

- **Сервер:** `5.42.98.106` (Timeweb Cloud VPS)
- **Путь на сервере:** `/opt/mp-control`
- **Git credentials** на сервере уже настроены (HTTPS без пароля)
- **Alembic** миграции применяются автоматически через `entrypoint.sh` при рестарте backend
- **НЕ** делать `rm -rf dist` на сервере — это ломает Docker bind mount nginx
- **НЕ** делать `git commit` на сервере — это создаёт divergent branches
