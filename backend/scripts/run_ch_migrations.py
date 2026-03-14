#!/usr/bin/env python3
"""
ClickHouse Migration Runner
Применяет файлы миграций из docker/clickhouse/migrations/ в порядке нумерации.
Идемпотентный — использует IF NOT EXISTS / попускает уже выполненные команды.

Использование:
  python3 scripts/run_ch_migrations.py
"""

import os
import sys
import glob
import logging

sys.path.insert(0, "/app")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_migrations():
    from app.core.clickhouse import get_clickhouse_client

    ch = get_clickhouse_client()

    migrations_dir = os.environ.get(
        "CH_MIGRATIONS_DIR",
        "/app/clickhouse_migrations"
    )

    if not os.path.isdir(migrations_dir):
        logger.warning("migrations dir not found: %s", migrations_dir)
        return

    sql_files = sorted(glob.glob(os.path.join(migrations_dir, "*.sql")))

    if not sql_files:
        logger.info("No CH migrations found in %s", migrations_dir)
        return

    logger.info("Found %d migration(s) in %s", len(sql_files), migrations_dir)

    for filepath in sql_files:
        filename = os.path.basename(filepath)
        logger.info("Applying migration: %s", filename)

        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        # Убираем однострочные комментарии (-- ...) из содержимого,
        # сохраняя строки, которые содержат SQL код
        lines = []
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped.startswith("--"):
                continue  # пропускаем строки-комментарии
            lines.append(line)
        clean_content = "\n".join(lines)

        # Разбиваем на отдельные команды по ';'
        statements = [s.strip() for s in clean_content.split(";") if s.strip()]

        for i, stmt in enumerate(statements):
            if not stmt:
                continue
            try:
                ch.command(stmt)
                logger.info("  [%d/%d] OK: %.80s...", i + 1, len(statements), stmt.replace("\n", " "))
            except Exception as e:
                err = str(e)
                # Игнорируем "уже существует" ошибки
                if any(x in err for x in ["already exists", "DUPLICATE_COLUMN", "Column already exists",
                                           "TABLE_ALREADY_EXISTS"]):
                    logger.info("  [%d/%d] Skipped (already applied): %.60s", i + 1, len(statements), stmt[:60])
                else:
                    logger.error("  [%d/%d] FAILED: %s\n  SQL: %s", i + 1, len(statements), err, stmt[:100])
                    raise

        logger.info("Migration %s — done", filename)

    ch.close()
    logger.info("All CH migrations applied successfully")


if __name__ == "__main__":
    run_migrations()
