#!/bin/bash
# Entrypoint for backend container
# Runs Alembic migrations only for the main backend process (uvicorn)

set -e

# Only run migrations for the main backend service, not celery workers
if [[ "$1" == "uvicorn" ]]; then
    echo "=== Running Alembic migrations ==="
    alembic upgrade head 2>&1 || echo "WARNING: Alembic migration failed, continuing..."
    echo "=== Migrations complete ==="
fi

exec "$@"
