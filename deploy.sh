#!/bin/bash
set -e

echo "========================================"
echo "  MP-CONTROL DEPLOY"
echo "  $(date)"
echo "========================================"

cd /opt/mp-control

# 1. Pull latest code
echo ""
echo ">>> Step 1: git pull"
git pull

# 2. Build frontend
echo ""
echo ">>> Step 2: npm install + build"
cd frontend
npm ci --silent
npm run build
cd ..

# 3. Rebuild Docker images
echo ""
echo ">>> Step 3: docker compose build"
docker compose -f docker-compose.prod.yml build

# 4. Restart services
echo ""
echo ">>> Step 4: restart services"
docker compose -f docker-compose.prod.yml up -d

# 5. Verify
echo ""
echo ">>> Step 5: verify"
sleep 5
docker compose -f docker-compose.prod.yml ps --format "table {{.Name}}\t{{.Status}}"

echo ""
echo "========================================"
echo "  DEPLOY DONE!"
echo "  Commit: $(git log --oneline -1)"
echo "========================================"
