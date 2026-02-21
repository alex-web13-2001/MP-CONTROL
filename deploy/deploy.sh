#!/bin/bash
set -e

# =============================================
# MP-CONTROL — Deploy / Update Script
# Run from project root on the server
#
# Steps:
#   1. git pull
#   2. npm build (frontend)
#   3. docker compose build + up
#   4. Alembic migrate (PostgreSQL)
#   5. Verify
# =============================================

COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE=".env"
BACKEND_CONTAINER="mms-backend"

echo "══════════════════════════════════════"
echo "  MP-CONTROL Deploy"
echo "  $(date)"
echo "══════════════════════════════════════"

# 1. Pull latest code
echo ""
echo "→ [1/5] Pulling latest code..."
git pull

# 2. Build frontend
echo ""
echo "→ [2/5] Building frontend..."
cd frontend
npm ci --production=false
npm run build
cd ..

# 3. Build and restart containers
echo ""
echo "→ [3/5] Building Docker images & starting services..."
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE build
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d

# 4. Run database migrations
echo ""
echo "→ [4/5] Running Alembic migrations..."
sleep 3  # Wait for backend container to be ready
docker exec $BACKEND_CONTAINER python -m alembic upgrade head
echo "  ✅ Migrations applied"

# 5. Verify
echo ""
echo "→ [5/5] Verifying..."
sleep 2
docker compose -f $COMPOSE_FILE ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "══════════════════════════════════════"
echo "  ✅ DEPLOY COMPLETE!"
echo "  Commit: $(git log --oneline -1)"
echo "══════════════════════════════════════"
echo ""
echo "  Logs:  docker compose -f $COMPOSE_FILE logs -f"

