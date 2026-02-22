#!/bin/bash
# ============================================================
# MP-CONTROL PRODUCTION DEPLOY SCRIPT
# Usage: ./deploy.sh [--full-rebuild]
# Default: fast deploy (git pull + docker cp + restart) ~30s
# --full-rebuild: пересобрать Docker образы (~10 min)
# ============================================================

SERVER="root@5.42.98.106"
SSH="ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa $SERVER"
SCP="scp -o StrictHostKeyChecking=no -i ~/.ssh/id_rsa"
REMOTE_DIR="/opt/mp-control"

set -e

echo "🚀 MP-CONTROL Deploy → $SERVER"
echo ""

# ── 1. Push local changes ────────────────────────────────
echo "📦 Pushing changes to git..."
git add -A && git diff --cached --quiet || git commit -m "deploy: $(date '+%Y-%m-%d %H:%M')"
git push origin main
echo "   ✓ Git push done"
echo ""

# ── 2. Pull on server ────────────────────────────────────
echo "⬇️  Pulling on server..."
$SSH "cd $REMOTE_DIR && git pull origin main 2>&1 | tail -3"
echo ""

if [ "$1" == "--full-rebuild" ]; then
    # ── FULL REBUILD (slow, needed when: new pip packages, Dockerfile changes) ──
    echo "🔨 Full rebuild (this takes ~10 minutes)..."
    $SSH "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml build backend celery-worker celery-beat 2>&1 | tail -5"
    $SSH "cd $REMOTE_DIR && docker compose -f docker-compose.prod.yml up -d --no-deps backend celery-worker celery-beat 2>&1 | tail -3"
else
    # ── FAST DEPLOY (copy Python files + restart) ──
    echo "⚡ Fast deploy (docker cp + restart)..."
    
    # Copy updated backend Python files
    $SSH "docker cp $REMOTE_DIR/backend/app mms-backend:/app/ 2>&1"
    $SSH "docker cp $REMOTE_DIR/backend/celery_app mms-celery-worker:/app/ 2>&1"
    $SSH "docker cp $REMOTE_DIR/backend/celery_app mms-celery-beat:/app/ 2>&1"
    $SSH "docker cp $REMOTE_DIR/backend/scripts mms-backend:/app/ 2>&1"
    $SSH "docker cp $REMOTE_DIR/docker/clickhouse/migrations mms-backend:/app/clickhouse_migrations/ 2>&1" 2>/dev/null || true
    echo "   ✓ Backend files copied"
    
    # ── Apply ClickHouse migrations ──
    echo ""
    echo "🗄️  Applying ClickHouse migrations..."
    $SSH "docker exec mms-backend python3 /app/scripts/run_ch_migrations.py 2>&1 | tail -10"
    echo "   ✓ CH migrations done"

    # Restart containers
    $SSH "docker restart mms-backend && echo '   ✓ backend restarted'"
    $SSH "docker restart mms-celery-worker && echo '   ✓ celery-worker restarted'"
    $SSH "docker restart mms-celery-beat && echo '   ✓ celery-beat restarted'"
fi

# ── 3. Build & serve Frontend ────────────────────────────
echo ""
echo "🎨 Building frontend..."
$SSH "cd $REMOTE_DIR/frontend && npm run build 2>&1 | grep -E 'built in|error TS|Error'"
$SSH "docker exec mms-nginx nginx -s reload && echo '   ✓ nginx reloaded'"
echo ""

# ── 4. Health check ─────────────────────────────────────
echo "✅ Health check..."
sleep 3
STATUS=$($SSH "curl -s -o /dev/null -w '%{http_code}' http://localhost/api/v1/auth/health" 2>/dev/null)
if [ "$STATUS" == "200" ]; then
    echo "   ✓ API is healthy (HTTP $STATUS)"
else
    echo "   ⚠ API status: HTTP $STATUS"
fi

echo ""
echo "🎉 Deploy complete!"
