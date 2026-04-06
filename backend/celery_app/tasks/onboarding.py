"""
Shop onboarding tasks — Initial historical data loading.

Contains load_historical_data (full shop onboarding pipeline)
and sync_full_history (manual re-sync).
"""

from celery_app.celery import celery_app


@celery_app.task(bind=True, time_limit=14400, soft_time_limit=14100)
def load_historical_data(self, shop_id: int, months: int = 6):
    """
    Orchestrator: load historical data for a newly connected shop.

    1. Read credentials from PostgreSQL (decrypt)
    2. Determine marketplace (ozon / wb)
    3. Run sub-tasks sequentially via .apply(), track progress in Redis
    4. Update shop.status on completion / error

    Progress is stored in Redis key ``sync_progress:{shop_id}``
    so the frontend can poll ``GET /shops/{id}/sync-status``.

    Routed to HEAVY queue (can take hours for 6 months of data).
    """
    import asyncio
    import json
    import logging
    import os
    import time
    import redis
    import traceback

    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select, update as sa_update

    from app.config import get_settings
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop

    logger = logging.getLogger(__name__)
    settings = get_settings()

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    r = redis.from_url(redis_url)
    progress_key = f"sync_progress:{shop_id}"

    # ── Distributed lock: only ONE load_historical_data per shop ──
    lock_key = f"lock:load_historical_data:{shop_id}"
    lock_ttl = 14400  # 4 hours — matches task time_limit
    if not r.set(lock_key, self.request.id or "1", nx=True, ex=lock_ttl):
        existing = r.get(lock_key)
        logger.info(
            "shop %s load_historical_data SKIPPED — already running (lock holder: %s)",
            shop_id, existing.decode() if existing else "unknown",
        )
        return {"shop_id": shop_id, "status": "skipped", "reason": "already_running"}

    errors_list: list[str] = []
    start_time = time.time()
    _state = {"marketplace": ""}  # mutable dict to avoid nonlocal with annotations

    # ── ETA estimates per marketplace (seconds) ──────────────
    # Based on real measurements: WB ~32 min, Ozon ~15 min
    _ETA_MAP = {
        "wildberries": {
            # step_idx → estimated remaining seconds at START of that step
            1: 2100, 2: 2000, 3: 1950, 4: 1900,  # finance is step 4, ~15 min
            5: 600, 6: 420, 7: 330, 8: 30,  # step 8: paid storage backfill ~5 min
        },
        "ozon": {
            1: 900, 2: 850, 3: 800, 4: 650, 5: 600, 6: 550,
            7: 500, 8: 450, 9: 400, 10: 350, 11: 300, 12: 200,  # ads backfill ~10 min
        },
    }

    def _format_eta(seconds: int) -> str:
        """Human-readable ETA string."""
        if seconds <= 60:
            return "меньше минуты"
        minutes = seconds // 60
        if minutes == 1:
            return "≈ 1 минута"
        elif minutes < 5:
            return f"≈ {minutes} минуты"
        elif minutes < 21 or minutes % 10 >= 5 or minutes % 10 == 0:
            return f"≈ {minutes} минут"
        elif minutes % 10 == 1:
            return f"≈ {minutes} минута"
        else:
            return f"≈ {minutes} минуты"

    # ── helpers ──────────────────────────────────────────────
    def _set_progress(
        current_step: int,
        total_steps: int,
        step_name: str,
        status: str = "loading",
        error: str | None = None,
    ):
        """Write progress to Redis for frontend polling."""
        # Percent: (step-1)/total — so step 7/7 shows 85%, 100% only on "done"
        if status in ("done", "done_with_errors"):
            percent = 100
        elif total_steps:
            percent = int((current_step - 1) / total_steps * 100)
        else:
            percent = 0

        elapsed = int(time.time() - start_time)

        # ETA based on marketplace-specific estimates
        eta_msg = None
        if status == "loading" and _state["marketplace"]:
            eta_map = _ETA_MAP.get(_state["marketplace"], {})
            remaining = eta_map.get(current_step)
            if remaining:
                eta_msg = _format_eta(remaining)

        # Read sub-progress from subtask (if any) then clear it.
        # Each _set_progress call marks the START of a new step,
        # so any leftover sub-progress from the previous step must be wiped.
        sub_key = f"sync_sub_progress:{shop_id}"
        sub_raw = r.get(sub_key)
        sub_progress = sub_raw.decode() if sub_raw else None
        r.delete(sub_key)  # always clear — subtask will re-set if needed

        payload = {
            "status": status,
            "current_step": current_step,
            "total_steps": total_steps,
            "step_name": step_name,
            "percent": percent,
            "error": error,
            "elapsed_sec": elapsed,
            "started_at": start_time,  # epoch timestamp for real-time elapsed calc
            "eta_message": eta_msg,
            "sub_progress": sub_progress,
        }
        r.setex(progress_key, 86400, json.dumps(payload, ensure_ascii=False))
        self.update_state(state="PROGRESS", meta=payload)
        logger.info("shop %s sync progress: step %s/%s — %s", shop_id, current_step, total_steps, step_name)

    def _run_subtask(task_ref, **kwargs):
        """
        Run a Celery task synchronously with a proper task context.

        Uses .apply() which creates a full Celery task execution context
        (with task_id, request, etc.) so self.update_state() works inside subtasks.
        This runs in the SAME process, NOT via broker.
        """
        result = task_ref.apply(kwargs=kwargs)
        if result.failed():
            raise result.result  # re-raise the exception
        return result.result

    # ── Read credentials ─────────────────────────────────────
    async def _load():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            result = await db.execute(select(Shop).where(Shop.id == shop_id))
            shop = result.scalar_one_or_none()

        if not shop:
            _set_progress(0, 0, "Магазин не найден", status="error", error="Shop not found")
            await engine.dispose()
            return None

        marketplace = shop.marketplace
        api_key = decrypt_api_key(shop.api_key_encrypted)
        client_id = shop.client_id or ""

        # Performance API credentials (Ozon ads)
        perf_client_id = shop.perf_client_id or ""
        perf_client_secret = ""
        if shop.perf_client_secret_encrypted:
            perf_client_secret = decrypt_api_key(shop.perf_client_secret_encrypted)

        # Update status to syncing
        async with async_session() as db:
            await db.execute(
                sa_update(Shop).where(Shop.id == shop_id).values(status="syncing")
            )
            await db.commit()

        await engine.dispose()
        return {
            "marketplace": marketplace,
            "api_key": api_key,
            "client_id": client_id,
            "perf_client_id": perf_client_id,
            "perf_client_secret": perf_client_secret,
        }

    try:  # ── outer try/finally to ALWAYS release lock ──

        try:
            creds = asyncio.run(_load())
            if creds is None:
                return {"shop_id": shop_id, "status": "error", "error": "Shop not found"}
        except Exception as e:
            _set_progress(0, 0, "Ошибка чтения credentials", status="error", error=str(e))
            raise

        marketplace = creds["marketplace"]
        api_key = creds["api_key"]
        client_id = creds["client_id"]
        perf_client_id = creds["perf_client_id"]
        perf_client_secret = creds["perf_client_secret"]

        _state["marketplace"] = marketplace

        # ── Ozon pipeline (11 steps) ─────────────────────────────
        if marketplace == "ozon":
            from celery_app.tasks.ozon_sync import (
                sync_ozon_products,
                sync_ozon_product_snapshots,
                backfill_ozon_orders,
                backfill_ozon_finance,
                backfill_ozon_funnel,
                backfill_ozon_returns,
                sync_ozon_warehouse_stocks,
                sync_ozon_prices,
                sync_ozon_seller_rating,
                sync_ozon_content_rating,
                sync_ozon_content,
                sync_ozon_turnover,
                backfill_ozon_placement_cost,
            )
            from celery_app.tasks.ozon_advertising import (
                backfill_ozon_ads,
                sync_ozon_campaigns_task,
            )

            seller_kwargs = dict(shop_id=shop_id, api_key=api_key, client_id=client_id)

            steps = [
                ("Загрузка каталога товаров",          sync_ozon_products,          seller_kwargs),
                ("Снимок данных (inventory/commissions)", sync_ozon_product_snapshots, seller_kwargs),
                ("Загрузка заказов (365 дней)",         backfill_ozon_orders,        {**seller_kwargs, "days_back": months * 30}),
                ("Загрузка финансов (12 месяцев)",      backfill_ozon_finance,       seller_kwargs),
                ("Загрузка воронки продаж (365 дней)",  backfill_ozon_funnel,        seller_kwargs),
                ("Загрузка возвратов (180 дней)",       backfill_ozon_returns,       seller_kwargs),
                ("Загрузка остатков на складах",        sync_ozon_warehouse_stocks,  seller_kwargs),
                ("Загрузка цен и комиссий",            sync_ozon_prices,            seller_kwargs),
                ("Загрузка рейтинга продавца",         sync_ozon_seller_rating,     seller_kwargs),
                ("Загрузка рейтинга контента",         sync_ozon_content_rating,    seller_kwargs),
                ("Синхронизация контента (хэши)",       sync_ozon_content,           seller_kwargs),
                ("Оборачиваемость товаров FBO",         sync_ozon_turnover,          seller_kwargs),
                ("Загрузка хранения (90 дней)",          backfill_ozon_placement_cost, {**seller_kwargs, "months": 3}),
            ]

            # Add ads backfill only if Performance API credentials exist
            # NOTE: backfill_ozon_ads runs via .apply() (sync) to guarantee
            # data is loaded before shop is marked active.
            # We set a Redis lock to prevent periodic sync_ozon_ad_stats
            # from competing for the same Ozon API rate limit.
            if perf_client_id and perf_client_secret:
                steps.append((
                    "Загрузка справочника кампаний",
                    sync_ozon_campaigns_task,
                    dict(shop_id=shop_id, perf_client_id=perf_client_id, perf_client_secret=perf_client_secret),
                ))
                steps.append((
                    "Загрузка рекламной статистики (180 дней)",
                    backfill_ozon_ads,
                    dict(shop_id=shop_id, perf_client_id=perf_client_id, perf_client_secret=perf_client_secret),
                ))

            total = len(steps)

            for idx, (step_name, task_ref, kwargs) in enumerate(steps, 1):
                _set_progress(idx, total, step_name)
                try:
                    _run_subtask(task_ref, **kwargs)
                    logger.info("shop %s step '%s' completed OK", shop_id, step_name)
                except Exception as e:
                    err_msg = f"{step_name}: {e}"
                    errors_list.append(err_msg)
                    logger.error("shop %s step '%s' failed: %s", shop_id, step_name, traceback.format_exc())
                    # Continue to next step — partial data is better than nothing
                    continue

        # ── WB pipeline ──────────────────────────────────────────
        elif marketplace == "wildberries":
            from celery_app.tasks.wb_sync import (
                sync_product_content,
                backfill_orders,
                backfill_sales_funnel,
                sync_wb_finance_history,
                sync_commercial_data,
                sync_warehouses,
                backfill_wb_paid_storage,
            )
            from celery_app.tasks.wb_advertising import (
                sync_wb_advert_history,
                backfill_normquery_data,
            )

            steps = [
                ("Загрузка контента товаров", sync_product_content, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка заказов (90 дней)", backfill_orders, dict(shop_id=shop_id, api_key=api_key, days=months * 30)),
                ("Загрузка воронки продаж (365 дней)", backfill_sales_funnel, dict(shop_id=shop_id, api_key=api_key, months=min(months, 12))),
                ("Загрузка финансовых отчётов", sync_wb_finance_history, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка рекламной истории", sync_wb_advert_history, dict(shop_id=shop_id, api_key=api_key, days_back=months * 30)),
                ("Загрузка цен и остатков", sync_commercial_data, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка складов", sync_warehouses, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка платного хранения (90 дней)", backfill_wb_paid_storage, dict(shop_id=shop_id, api_key=api_key, months=3)),
                ("Загрузка кластерных данных (30 дней)", backfill_normquery_data, dict(shop_id=shop_id, api_key=api_key, days_back=30)),
            ]
            total = len(steps)

            for idx, (step_name, task_ref, kwargs) in enumerate(steps, 1):
                _set_progress(idx, total, step_name)
                try:
                    _run_subtask(task_ref, **kwargs)
                    logger.info("shop %s step '%s' completed OK", shop_id, step_name)
                except Exception as e:
                    err_msg = f"{step_name}: {e}"
                    errors_list.append(err_msg)
                    logger.error("shop %s step '%s' failed: %s", shop_id, step_name, traceback.format_exc())
                    continue

        # ── Finalize ─────────────────────────────────────────────
        final_status = "active" if not errors_list else "active"  # still active, data is partial
        status_message = "; ".join(errors_list) if errors_list else None

        async def _finalize():
            engine = create_async_engine(settings.database_url)
            sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            from datetime import datetime, timezone
            async with sf() as db:
                await db.execute(
                    sa_update(Shop).where(Shop.id == shop_id).values(
                        status=final_status,
                        status_message=status_message,
                        last_sync_at=datetime.now(timezone.utc),
                    )
                )
                await db.commit()
            await engine.dispose()

        asyncio.run(_finalize())

        done_status = "done" if not errors_list else "done_with_errors"
        error_summary = "; ".join(errors_list) if errors_list else None
        _set_progress(total, total, "Готово!", status=done_status, error=error_summary)

        logger.info(
            "shop %s load_historical_data finished: %s (%d errors)",
            shop_id, done_status, len(errors_list),
        )

        return {"shop_id": shop_id, "marketplace": marketplace, "status": done_status, "errors": errors_list}

    finally:
        # ── Always release lock, even on crash ──
        r.delete(lock_key)
        logger.info("shop %s lock released (key=%s)", shop_id, lock_key)



@celery_app.task(bind=True, time_limit=14400, soft_time_limit=14100)
def sync_full_history(self, shop_id: int, start_date: str, end_date: str):
    """
    Sync full order history between dates.
    
    Long-running task for HEAVY queue.
    Uses ReplacingMergeTree in ClickHouse for idempotency.
    """
    # TODO: Implement full history sync
    # Uses ReplacingMergeTree, so duplicates are handled automatically
    return {"shop_id": shop_id, "start_date": start_date, "end_date": end_date}


# ===================
# SYNC COORDINATORS
# Multi-tenant: read all active shops, dispatch sync tasks with proper credentials
# ===================

