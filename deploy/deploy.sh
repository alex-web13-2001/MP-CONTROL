#!/bin/bash
set -e

# =============================================
# MP-CONTROL — Deploy / Update Script
# Run from project root on the server
# =============================================

COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE=".env"

echo "══════════════════════════════════════"
echo "  MP-CONTROL Deploy"
echo "══════════════════════════════════════"

# 1. Pull latest code
echo "→ Pulling latest code..."
git pull origin main

# 2. Build frontend
echo "→ Building frontend..."
cd frontend
npm ci --production=false
npm run build
cd ..

# 3. Build and restart containers
echo "→ Building Docker images..."
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE build

echo "→ Starting services..."
docker compose -f $COMPOSE_FILE --env-file $ENV_FILE up -d

echo ""
echo "✅ Deploy complete!"
echo "→ Check: docker compose -f $COMPOSE_FILE ps"
echo "→ Logs:  docker compose -f $COMPOSE_FILE logs -f"
