#!/bin/bash
set -e

# =============================================
# MP-CONTROL — Deploy / Update Script
# Запускать НА СЕРВЕРЕ из корня проекта:
#   cd /opt/mp-control && bash deploy/deploy.sh
#
# Или с локала через SSH:
#   ssh root@5.42.98.106 "cd /opt/mp-control && bash deploy/deploy.sh"
#
# Steps:
#   1. git fetch + reset (sync с origin/main)
#   2. ClickHouse миграции (до рестарта контейнеров)
#   3. docker compose build + up (backend, celery)
#      → entrypoint.sh автоматически применяет Alembic (PostgreSQL)
#   4. npm build (frontend)
#   5. Restart nginx (bind mount refresh)
#   6. Verify
# =============================================

COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE=".env"
BACKEND_CONTAINER="mms-backend"

echo "══════════════════════════════════════"
echo "  MP-CONTROL Deploy"
echo "  $(date)"
echo "══════════════════════════════════════"

# 1. Sync with origin (force reset — сервер не должен иметь local changes)
echo ""
echo "→ [1/6] Syncing with origin/main..."
git fetch origin main
git reset --hard origin/main
echo "  ✅ $(git log --oneline -1)"

# 2. ClickHouse миграции (ПЕРЕД рестартом — новый код может зависеть от новых колонок)
echo ""
echo "→ [2/6] ClickHouse migrations..."
docker cp docker/clickhouse/migrations $BACKEND_CONTAINER:/app/clickhouse_migrations/ 2>/dev/null || true
docker cp backend/scripts $BACKEND_CONTAINER:/app/scripts/ 2>/dev/null || true
docker exec $BACKEND_CONTAINER python3 /app/scripts/run_ch_migrations.py 2>&1 | tail -10
echo "  ✅ CH migrations done"

# 3. Build and restart backend + celery
#    Alembic (PostgreSQL) миграции применяются автоматически через entrypoint.sh
echo ""
echo "→ [3/6] Building & restarting backend..."
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE build 2>&1 | tail -5
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d 2>&1 | tail -5
sleep 3
echo "  ✅ Backend + Celery restarted (Alembic auto-applied via entrypoint.sh)"

# 4. Build frontend
echo ""
echo "→ [4/6] Building frontend..."
cd frontend
npm ci --production=false 2>&1 | tail -3
npm run build 2>&1 | grep -E 'built in|error TS|Error'
cd ..

# 5. Restart nginx (bind mount обновляется только при restart, не reload)
echo ""
echo "→ [5/6] Restarting nginx..."
docker compose -f $COMPOSE_FILE restart nginx 2>&1 | tail -2
echo "  ✅ Nginx restarted"

# 6. Verify
echo ""
echo "→ [6/6] Verifying..."
sleep 2
docker compose -f $COMPOSE_FILE ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "══════════════════════════════════════"
echo "  ✅ DEPLOY COMPLETE!"
echo "  Commit: $(git log --oneline -1)"
echo "══════════════════════════════════════"
echo ""
echo "  Logs:  docker compose -f $COMPOSE_FILE logs -f"
