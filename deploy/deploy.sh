#!/bin/bash
set -e

# =============================================
# MP-CONTROL — Deploy / Update Script
# Запускать НА СЕРВЕРЕ из корня проекта:
#   cd /opt/mp-control && bash deploy/deploy.sh
#
# Или с локала:
#   ssh root@5.42.98.106 "cd /opt/mp-control && bash deploy/deploy.sh"
#
# Steps:
#   1. git fetch + reset (гарантированно синхронизирует с origin)
#   2. npm build (frontend)
#   3. docker compose build + up (backend)
#   4. Restart nginx (bind mount refresh)
#   5. Verify
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
echo "→ [1/5] Syncing with origin/main..."
git fetch origin main
git reset --hard origin/main
echo "  ✅ $(git log --oneline -1)"

# 2. Build frontend
echo ""
echo "→ [2/5] Building frontend..."
cd frontend
npm ci --production=false 2>&1 | tail -3
npm run build 2>&1 | grep -E 'built in|error TS|Error'
cd ..

# 3. Build and restart containers
echo ""
echo "→ [3/5] Building Docker images & starting services..."
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE build 2>&1 | tail -5
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d 2>&1 | tail -5

# 4. Restart nginx (bind mount обновляется только при restart, не reload)
echo ""
echo "→ [4/5] Restarting nginx..."
docker compose -f $COMPOSE_FILE restart nginx 2>&1 | tail -2
echo "  ✅ Nginx restarted"

# 5. Verify
echo ""
echo "→ [5/5] Verifying..."
sleep 3
docker compose -f $COMPOSE_FILE ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "══════════════════════════════════════"
echo "  ✅ DEPLOY COMPLETE!"
echo "  Commit: $(git log --oneline -1)"
echo "══════════════════════════════════════"
echo ""
echo "  Logs:  docker compose -f $COMPOSE_FILE logs -f"
