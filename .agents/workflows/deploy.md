---
description: Deploy MP-CONTROL to production server
---

# Deploy to Production

// turbo-all

## Prerequisites

SSH key is already added to the server. Connect without password:

```bash
ssh -i ~/.ssh/id_rsa root@5.42.98.106
```

Server docs: `docs/architecture/07_DEPLOYMENT.md`

## Quick deploy (Python/TS changes only, ~30 sec)

```bash
chmod +x deploy.sh && ./deploy.sh
```

## Full rebuild (new pip packages or Dockerfile changes, ~10 min)

```bash
./deploy.sh --full-rebuild
```

## Manual steps (if deploy.sh not available)

1. Push code:

```bash
git add -A && git commit -m "deploy" && git push
```

2. Pull on server:

```bash
ssh -i ~/.ssh/id_rsa root@5.42.98.106 "cd /opt/mp-control && git pull"
```

3. Copy updated Python files (fast, no rebuild needed):

```bash
ssh -i ~/.ssh/id_rsa root@5.42.98.106 "docker cp /opt/mp-control/backend/app mms-backend:/app/"
ssh -i ~/.ssh/id_rsa root@5.42.98.106 "docker restart mms-backend mms-celery-worker mms-celery-beat"
```

4. Build frontend:

```bash
ssh -i ~/.ssh/id_rsa root@5.42.98.106 "cd /opt/mp-control/frontend && npm run build && docker exec mms-nginx nginx -s reload"
```

## Troubleshooting

- **Blank screen on prod**: Check nginx.prod.conf — must use `try_files`, not `proxy_pass http://frontend`
- **Data wrong on prod (реклама=0)**: Backend running old Docker image → run `./deploy.sh`
- **Excel upload 400 error**: `docker exec mms-backend pip install openpyxl`
- **Docker build hangs**: Server RAM is limited (72%). Build one container at a time.
- **SSH hangs on password**: Use `-i ~/.ssh/id_rsa` — key is added to server's authorized_keys
