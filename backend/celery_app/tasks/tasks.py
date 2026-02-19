"""Celery tasks module with queue separation and deduplication."""

from celery_app.celery import celery_app


# ===================
# DEDUPLICATION HELPER
# ===================
# Prevents duplicate tasks from accumulating in the queue.
# Before dispatching a task, we set a Redis key with NX (only if not exists).
# If the key already exists, the task is already queued/running — skip it.

def _dedup_dispatch(task_ref, redis_client, shop_id: int, ttl: int = 1800, queue: str = "sync", **kwargs):
    """
    Dispatch a task with Redis-based deduplication.

    Args:
        task_ref: Celery task reference
        redis_client: Redis client instance
        shop_id: Shop ID for dedup scoping (auto-injected into task kwargs)
        ttl: Lock TTL in seconds (default: 30 min — matches frequent sync interval)
        queue: Target queue name
        **kwargs: Task keyword arguments (shop_id will be added automatically)

    Returns:
        True if dispatched, False if deduplicated (skipped)
    """
    task_name = task_ref.name.rsplit(".", 1)[-1]  # e.g. "sync_ozon_products"
    dedup_key = f"dedup:{queue}:{task_name}:{shop_id}"

    # SET NX — only set if key doesn't exist
    if not redis_client.set(dedup_key, "1", nx=True, ex=ttl):
        return False  # Task already in queue/running

    # Auto-inject shop_id into kwargs so callers don't need to duplicate it
    task_kwargs = {"shop_id": shop_id, **kwargs}

    # Dispatch with a callback to clear the dedup key on completion
    task_ref.apply_async(
        kwargs=task_kwargs,
        queue=queue,
        headers={"dedup_key": dedup_key},
    )
    return True


# Signal handler: clean up dedup key after task completes
from celery.signals import task_postrun

@task_postrun.connect
def _cleanup_dedup_key(sender=None, headers=None, request=None, **kwargs):
    """Remove dedup key from Redis after task finishes (success or failure)."""
    import os
    try:
        dedup_key = None
        if request and hasattr(request, 'headers') and request.headers:
            dedup_key = request.headers.get('dedup_key')
        if not dedup_key and hasattr(sender, 'request') and hasattr(sender.request, 'headers'):
            headers_dict = sender.request.headers or {}
            dedup_key = headers_dict.get('dedup_key')
        if dedup_key:
            import redis
            redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
            r = redis.from_url(redis_url)
            r.delete(dedup_key)
    except Exception:
        pass  # Best effort — don't break the task


# ===================
# FAST QUEUE TASKS
# Autobidder and position monitoring (time-critical)
# ===================

@celery_app.task(bind=True, max_retries=3, priority=9)
def update_bids(self, shop_id: int, campaign_id: str):
    """
    Update bids for a specific campaign.
    
    This is a time-critical task that runs every minute.
    Routed to FAST queue automatically.
    """
    try:
        # TODO: Implement bid update logic
        # 1. Get current position from ClickHouse
        # 2. Calculate optimal bid based on autobidder settings
        # 3. Send new bid to WB API
        return {"shop_id": shop_id, "campaign_id": campaign_id, "status": "updated"}
    except Exception as exc:
        self.retry(exc=exc, countdown=10)


@celery_app.task(bind=True)
def update_all_bids(self):
    """
    Periodic task to update all active autobidder campaigns.
    
    Runs every minute via Celery Beat.
    Spawns individual update_bids tasks for each campaign.
    """
    # TODO: Get all active autobidder campaigns from PostgreSQL
    # For each campaign, spawn update_bids.delay(shop_id, campaign_id)
    return {"status": "dispatched", "campaigns": 0}


@celery_app.task(bind=True, max_retries=3, priority=7)
def check_positions(self, shop_id: int, sku: str, keywords: list[str]):
    """
    Check product positions for given keywords.
    
    Routed to FAST queue for responsive position tracking.
    """
    try:
        # TODO: Implement position checking via curl_cffi
        # 1. For each keyword, search on WB
        # 2. Find product position
        # 3. Store in ClickHouse positions table
        return {"shop_id": shop_id, "sku": sku, "keywords_checked": len(keywords)}
    except Exception as exc:
        self.retry(exc=exc, countdown=30)


@celery_app.task(bind=True)
def check_all_positions(self):
    """
    Periodic task to check positions for all tracked products.
    
    Runs every 5 minutes via Celery Beat.
    """
    # TODO: Get all products with position tracking enabled
    # Spawn check_positions tasks
    return {"status": "dispatched", "products": 0}


# ===================
# HEAVY QUEUE TASKS
# Historical data loading (long-running)
# ===================

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
            1: 1800, 2: 1700, 3: 1650, 4: 1600,  # finance is step 4, ~15 min
            5: 300, 6: 120, 7: 30,
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
            from celery_app.tasks.tasks import (
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
                backfill_ozon_ads,
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
            ]

            # Add ads backfill only if Performance API credentials exist
            # NOTE: backfill_ozon_ads runs via .apply() (sync) to guarantee
            # data is loaded before shop is marked active.
            # We set a Redis lock to prevent periodic sync_ozon_ad_stats
            # from competing for the same Ozon API rate limit.
            if perf_client_id and perf_client_secret:
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
            from celery_app.tasks.tasks import (
                sync_product_content,
                backfill_orders,
                backfill_sales_funnel,
                sync_wb_finance_history,
                sync_wb_advert_history,
                sync_commercial_data,
                sync_warehouses,
            )

            steps = [
                ("Загрузка контента товаров", sync_product_content, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка заказов (90 дней)", backfill_orders, dict(shop_id=shop_id, api_key=api_key, days=months * 30)),
                ("Загрузка воронки продаж (365 дней)", backfill_sales_funnel, dict(shop_id=shop_id, api_key=api_key, months=min(months, 12))),
                ("Загрузка финансовых отчётов", sync_wb_finance_history, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка рекламной истории", sync_wb_advert_history, dict(shop_id=shop_id, api_key=api_key, days_back=months * 30)),
                ("Загрузка цен и остатков", sync_commercial_data, dict(shop_id=shop_id, api_key=api_key)),
                ("Загрузка складов", sync_warehouses, dict(shop_id=shop_id, api_key=api_key)),
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

@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_all_daily(self):
    """
    Daily coordinator: dispatch daily sync tasks for ALL active shops.

    Runs at 3:00 UTC via Celery Beat.
    Reads shops from PostgreSQL, decrypts credentials,
    dispatches tasks to SYNC queue with deduplication.

    Ozon shops get: products, snapshots, finance, funnel, returns,
                    seller_rating, content_rating, content hashes
    WB shops get:   warehouses, product_content
    """
    import asyncio
    import os
    import logging
    import redis
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from app.config import get_settings
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop

    logger = logging.getLogger(__name__)
    settings = get_settings()
    r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))

    async def _dispatch():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with sf() as db:
            result = await db.execute(
                select(Shop).where(Shop.status == "active")
            )
            shops = result.scalars().all()

        await engine.dispose()
        return shops

    shops = asyncio.run(_dispatch())

    if not shops:
        logger.info("sync_all_daily: no active shops found, skipping")
        return {"dispatched": 0, "skipped": 0}

    dispatched = 0
    skipped = 0

    for shop in shops:
        try:
            api_key = decrypt_api_key(shop.api_key_encrypted)
            client_id = shop.client_id or ""
        except Exception as e:
            logger.error("sync_all_daily: shop %s decrypt failed: %s", shop.id, e)
            continue

        if shop.marketplace == "ozon":
            from celery_app.tasks.tasks import (
                sync_ozon_products,
                sync_ozon_product_snapshots,
                sync_ozon_finance,
                sync_ozon_funnel,
                sync_ozon_returns,
                sync_ozon_seller_rating,
                sync_ozon_content_rating,
                sync_ozon_content,
            )

            kwargs = dict(api_key=api_key, client_id=client_id)

            for task_ref in [
                sync_ozon_products, sync_ozon_product_snapshots,
                sync_ozon_finance, sync_ozon_funnel, sync_ozon_returns,
                sync_ozon_seller_rating, sync_ozon_content_rating, sync_ozon_content,
            ]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=82800, **kwargs):  # 23h TTL for daily
                    dispatched += 1
                else:
                    skipped += 1

            logger.info("sync_all_daily: Ozon shop %s (%s) — dispatched/skipped", shop.id, shop.name)

        elif shop.marketplace == "wildberries":
            from celery_app.tasks.tasks import (
                sync_warehouses,
                sync_product_content,
            )

            for task_ref in [sync_warehouses, sync_product_content]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=82800, api_key=api_key):
                    dispatched += 1
                else:
                    skipped += 1

            logger.info("sync_all_daily: WB shop %s (%s) — dispatched/skipped", shop.id, shop.name)

    logger.info("sync_all_daily: dispatched=%d skipped=%d shops=%d", dispatched, skipped, len(shops))
    return {"dispatched": dispatched, "skipped": skipped, "shops": len(shops)}


@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_all_frequent(self):
    """
    Frequent coordinator: dispatch high-frequency sync tasks for ALL active shops.

    Runs every 30 minutes via Celery Beat.
    Uses Redis deduplication to prevent duplicate tasks.
    Covers: orders, warehouse stocks, prices (Ozon)
            orders, commercial data, sales funnel, ads (WB)
    """
    import asyncio
    import os
    import logging
    import redis
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from app.config import get_settings
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop

    logger = logging.getLogger(__name__)
    settings = get_settings()
    r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))

    async def _dispatch():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with sf() as db:
            result = await db.execute(
                select(Shop).where(Shop.status == "active")
            )
            shops = result.scalars().all()

        await engine.dispose()
        return shops

    shops = asyncio.run(_dispatch())

    if not shops:
        logger.info("sync_all_frequent: no active shops found, skipping")
        return {"dispatched": 0, "skipped": 0}

    dispatched = 0
    skipped = 0

    for shop in shops:
        try:
            api_key = decrypt_api_key(shop.api_key_encrypted)
            client_id = shop.client_id or ""
        except Exception as e:
            logger.error("sync_all_frequent: shop %s decrypt failed: %s", shop.id, e)
            continue

        if shop.marketplace == "ozon":
            from celery_app.tasks.tasks import (
                sync_ozon_orders,
                sync_ozon_warehouse_stocks,
                sync_ozon_prices,
                sync_ozon_ad_stats,
            )

            kwargs = dict(api_key=api_key, client_id=client_id)

            for task_ref in [sync_ozon_orders, sync_ozon_warehouse_stocks, sync_ozon_prices]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=1800, **kwargs):  # 30min TTL
                    dispatched += 1
                else:
                    skipped += 1

            # Ozon ad stats (requires perf credentials)
            if shop.perf_client_id and shop.perf_client_secret_encrypted:
                try:
                    perf_secret = decrypt_api_key(shop.perf_client_secret_encrypted)
                    if _dedup_dispatch(
                        sync_ozon_ad_stats, r, shop.id, ttl=1800,
                        perf_client_id=shop.perf_client_id,
                        perf_client_secret=perf_secret,
                        lookback_days=3,
                    ):
                        dispatched += 1
                    else:
                        skipped += 1
                except Exception as e:
                    logger.warning("sync_all_frequent: shop %s ozon ad decrypt failed: %s", shop.id, e)

            logger.info("sync_all_frequent: Ozon shop %s — dispatched/skipped", shop.id)

        elif shop.marketplace == "wildberries":
            from celery_app.tasks.tasks import (
                sync_orders,
                sync_commercial_data,
                sync_sales_funnel,
                sync_wb_advert_history,
            )

            wb_kwargs = dict(api_key=api_key)
            for task_ref in [sync_orders, sync_commercial_data, sync_sales_funnel]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=1800, **wb_kwargs):
                    dispatched += 1
                else:
                    skipped += 1

            # Ad stats: 3-day lookback
            if _dedup_dispatch(
                sync_wb_advert_history, r, shop.id, ttl=1800,
                api_key=api_key, days_back=3,
            ):
                dispatched += 1
            else:
                skipped += 1

            logger.info("sync_all_frequent: WB shop %s — dispatched/skipped", shop.id)

    logger.info("sync_all_frequent: dispatched=%d skipped=%d shops=%d", dispatched, skipped, len(shops))
    return {"dispatched": dispatched, "skipped": skipped, "shops": len(shops)}


@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_all_ads(self):
    """
    Ads coordinator: dispatch ad-related sync tasks for ALL active shops.

    Runs every 60 minutes via Celery Beat.
    Uses Redis deduplication to prevent duplicate tasks.
    Ozon: ad stats (perf API) + bid monitoring
    WB:   ad history sync
    """
    import asyncio
    import os
    import logging
    import redis
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from app.config import get_settings
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop

    logger = logging.getLogger(__name__)
    settings = get_settings()
    r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))

    async def _dispatch():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with sf() as db:
            result = await db.execute(
                select(Shop).where(Shop.status == "active")
            )
            shops = result.scalars().all()

        await engine.dispose()
        return shops

    shops = asyncio.run(_dispatch())

    if not shops:
        logger.info("sync_all_ads: no active shops found, skipping")
        return {"dispatched": 0, "skipped": 0}

    dispatched = 0
    skipped = 0

    for shop in shops:
        if shop.marketplace == "ozon":
            # Performance API credentials required
            perf_client_id = shop.perf_client_id or ""
            perf_client_secret = ""
            if shop.perf_client_secret_encrypted:
                try:
                    perf_client_secret = decrypt_api_key(shop.perf_client_secret_encrypted)
                except Exception as e:
                    logger.error("sync_all_ads: shop %s perf decrypt failed: %s", shop.id, e)
                    continue

            if not perf_client_id or not perf_client_secret:
                logger.info("sync_all_ads: shop %s has no perf credentials, skipping ads", shop.id)
                continue

            from celery_app.tasks.tasks import (
                sync_ozon_ad_stats,
                monitor_ozon_bids,
            )

            ozon_ad_kwargs = dict(
                perf_client_id=perf_client_id,
                perf_client_secret=perf_client_secret,
            )

            for task_ref in [sync_ozon_ad_stats, monitor_ozon_bids]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=3600, **ozon_ad_kwargs):  # 1h TTL
                    dispatched += 1
                else:
                    skipped += 1

            logger.info("sync_all_ads: Ozon shop %s — dispatched/skipped", shop.id)

        elif shop.marketplace == "wildberries":
            try:
                api_key = decrypt_api_key(shop.api_key_encrypted)
            except Exception as e:
                logger.error("sync_all_ads: shop %s decrypt failed: %s", shop.id, e)
                continue

            from celery_app.tasks.tasks import sync_wb_advert_history

            if _dedup_dispatch(
                sync_wb_advert_history, r, shop.id, ttl=3600,
                api_key=api_key,
            ):
                dispatched += 1
            else:
                skipped += 1

            logger.info("sync_all_ads: WB shop %s — dispatched/skipped", shop.id)

    logger.info("sync_all_ads: dispatched=%d skipped=%d shops=%d", dispatched, skipped, len(shops))
    return {"dispatched": dispatched, "skipped": skipped, "shops": len(shops)}


@celery_app.task(bind=True, time_limit=7200, soft_time_limit=7000)
def sync_marketplace_data(self, shop_id: int):
    """
    Daily sync of marketplace data.
    
    Scheduled to run at 3 AM via Celery Beat.
    """
    # TODO: Implement daily sync
    # 1. Sync yesterday's orders
    # 2. Sync advertising stats
    # 3. Update aggregated tables
    return {"shop_id": shop_id, "status": "synced"}


# ===================
# DEFAULT QUEUE TASKS
# General purpose tasks
# ===================

@celery_app.task(bind=True, max_retries=3)
def example_task(self, data: dict):
    """Example task for demonstration."""
    try:
        return {"status": "completed", "data": data}
    except Exception as exc:
        self.retry(exc=exc, countdown=60)


@celery_app.task(bind=True)
def send_notification(self, user_id: int, message: str):
    """Send notification to user (email, telegram, etc.)."""
    # TODO: Implement notification sending
    return {"user_id": user_id, "sent": True}



@celery_app.task(bind=True, time_limit=7200, soft_time_limit=7000)
def sync_wb_finance_history(
    self,
    shop_id: int,
    api_key: str,
    days_back: int = 180,  # Default 6 months
):
    """
    Historical Sync: Download WB finance reports for the last N days.
    
    This task:
    1. Generates weekly date ranges for the past days_back
    2. For each week: downloads the finance report
    3. Parses CSV/JSON data and inserts into fact_finances
    4. Reports progress throughout
    
    Routed to HEAVY queue - can run for 1-2 hours.
    
    Args:
        shop_id: Shop ID in our system
        api_key: WB API key (decrypted)
        days_back: Number of days to look back (default: 180 ~ 6 months)
    
    Returns:
        Dict with sync statistics
    """
    import asyncio
    import os
    from datetime import date
    import logging
    import redis as redis_lib
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_finance_report_service import WBFinanceReportService
    logger = logging.getLogger(__name__)
    from app.services.wb_finance_loader import (
        WBReportParser,
        ClickHouseLoader,
        generate_week_ranges,
    )
    
    settings = get_settings()
    _r = redis_lib.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
    _sub_key = f"sync_sub_progress:{shop_id}"
    
    # Generate week ranges based on days_back
    months = max(1, days_back // 30)
    week_ranges = generate_week_ranges(months)
    total_weeks = len(week_ranges)
    
    stats = {
        "shop_id": shop_id,
        "days_back": days_back,
        "total_weeks": total_weeks,
        "processed_weeks": 0,
        "total_rows_inserted": 0,
        "errors": [],
    }
    
    async def download_and_process():
        # Create database session for downloading
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        async with async_session() as db:
            async with WBFinanceReportService(
                db=db,
                shop_id=shop_id,
                api_key=api_key,
            ) as download_service:
                
                # Connect to ClickHouse for loading
                loader = ClickHouseLoader(
                    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                    port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                    username=os.getenv("CLICKHOUSE_USER", "default"),
                    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                    database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
                )
                
                with loader:
                    parser = WBReportParser(shop_id)
                    
                    for i, (date_from, date_to) in enumerate(week_ranges):
                        date_from_str = date_from.strftime("%Y-%m-%d")
                        date_to_str = date_to.strftime("%Y-%m-%d")
                        
                        logger.info(
                            "Finance sync shop %s: week %d/%d [%s → %s]",
                            shop_id, i + 1, total_weeks, date_from_str, date_to_str,
                        )
                        # Sub-progress for frontend (shown during load_historical_data)
                        _r.setex(_sub_key, 3600, f"Неделя {i + 1} из {total_weeks}")
                        
                        # Optimization: Skip if data exists to save API budget
                        if loader.get_row_count(shop_id, date_from, date_to) > 0:
                            stats["processed_weeks"] += 1
                            logger.info("Finance week %d/%d skipped (already loaded)", i + 1, total_weeks)
                            self.update_state(
                                state='PROGRESS',
                                meta={
                                    'current_week': i + 1,
                                    'total_weeks': total_weeks,
                                    'date_range': f"{date_from_str} - {date_to_str}",
                                    'rows_inserted': stats["total_rows_inserted"],
                                    'status': 'Skipped (already loaded)'
                                }
                            )
                            await asyncio.sleep(0.1)
                            continue

                        # Update progress
                        self.update_state(
                            state='PROGRESS',
                            meta={
                                'current_week': i + 1,
                                'total_weeks': total_weeks,
                                'date_range': f"{date_from_str} - {date_to_str}",
                                'rows_inserted': stats["total_rows_inserted"],
                            }
                        )
                        
                        try:
                            # Step 1: Get report data with retry for 429
                            # WB statistics-api limits to ~1 req/min
                            logger.info("Finance: requesting data %s → %s ...", date_from_str, date_to_str)
                            rows_data = None
                            max_retries = 3
                            for attempt in range(max_retries):
                                try:
                                    rows_data = await asyncio.wait_for(
                                        download_service.get_report_data(
                                            date_from_str, date_to_str
                                        ),
                                        timeout=120.0,
                                    )
                                    break  # success
                                except Exception as req_err:
                                    if "429" in str(req_err) and attempt < max_retries - 1:
                                        wait = 60 * (attempt + 1)
                                        logger.warning(
                                            "Finance week %d/%d: 429 rate limited, retry %d/%d in %ds",
                                            i + 1, total_weeks, attempt + 1, max_retries, wait,
                                        )
                                        await asyncio.sleep(wait)
                                    else:
                                        raise
                            
                            if not rows_data:
                                stats["processed_weeks"] += 1
                                logger.info("Finance week %d/%d: empty response", i + 1, total_weeks)
                                await asyncio.sleep(10)
                                continue
                            
                            # Step 2: Parse JSON rows
                            rows = list(parser.parse_json_rows(rows_data))
                            
                            if rows:
                                inserted = loader.insert_batch(rows)
                                stats["total_rows_inserted"] += inserted
                                logger.info(
                                    "Finance week %d/%d: %d rows parsed, %d inserted",
                                    i + 1, total_weeks, len(rows), inserted,
                                )
                            
                            stats["processed_weeks"] += 1
                            
                            # Pause between weeks: WB stats API ~1 req/min
                            await asyncio.sleep(30)
                            
                        except asyncio.TimeoutError:
                            logger.error(
                                "Finance week %d/%d TIMEOUT (120s): %s → %s",
                                i + 1, total_weeks, date_from_str, date_to_str,
                            )
                            stats["errors"].append({
                                "week": f"{date_from_str} - {date_to_str}",
                                "error": "Request timeout (120s)",
                            })
                            stats["processed_weeks"] += 1
                        except Exception as e:
                            logger.error(
                                "Finance week %d/%d error: %s (%s → %s)",
                                i + 1, total_weeks, e, date_from_str, date_to_str,
                            )
                            await db.rollback()
                            stats["errors"].append({
                                "week": f"{date_from_str} - {date_to_str}",
                                "error": str(e),
                            })
        
        await engine.dispose()
    
    try:
        asyncio.run(download_and_process())
        
        stats["status"] = "completed"
        return stats
        
    except Exception as exc:
        stats["status"] = "failed"
        stats["fatal_error"] = str(exc)
        self.retry(exc=exc, countdown=300, max_retries=2)


# =============================================
# CAMPAIGN SNAPSHOT (lightweight, every 30 min)
# =============================================

@celery_app.task(bind=True, time_limit=300, soft_time_limit=270)
def sync_wb_campaign_snapshot(self, shop_id: int, api_key: str):
    """
    Lightweight task: fetch campaign list + full details (bids, names, placements).
    
    Uses only 2 WB API calls:
      1. GET /adv/v1/promotion/count → list of campaign IDs with types/statuses
      2. GET /api/advert/v2/adverts → full info per campaign (bids_kopecks, name, etc.)
    
    Saves to:
      - dim_advert_campaigns (ReplacingMergeTree — upsert with name, payment_type, bid_type)
      - log_wb_bids (MergeTree — append bid snapshots for history)
    
    Runs every 30 minutes via scheduler.
    Queue: HEAVY (uses WB API).
    """
    import asyncio
    import os
    import logging
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_advertising_report_service import WBAdvertisingReportService
    from app.services.wb_advertising_loader import WBAdvertisingLoader
    from app.services.event_detector import EventDetector

    logger = logging.getLogger(__name__)
    settings = get_settings()

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    async def run_snapshot():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        loader = WBAdvertisingLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
        )
        event_detector = EventDetector(redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"))

        try:
            with loader:
                # Step 1: Get campaign list with IDs + types + statuses
                self.update_state(state='PROGRESS', meta={'status': 'Fetching campaign list...'})
                async with async_session() as db:
                    service = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                    campaigns = await service.get_campaigns()

                # Build type map: advert_id -> type (from promotion/count)
                campaign_type_map = {}
                campaign_ids = []
                for c in campaigns:
                    advert_id = c.get("advertId")
                    if advert_id:
                        campaign_ids.append(advert_id)
                        campaign_type_map[advert_id] = int(c.get("type", 9))

                total = len(campaign_ids)
                logger.info(f"[snapshot] shop={shop_id}: found {total} campaigns")

                if not campaign_ids:
                    await engine.dispose()
                    return {"status": "completed", "campaigns": 0, "bids_saved": 0}

                # Step 2: Get full V2 info (bids, names, placements) — batches of 50
                self.update_state(state='PROGRESS', meta={'status': f'Fetching details for {total} campaigns...'})
                all_v2_adverts = []
                for batch in chunk_list(campaign_ids, 50):
                    try:
                        async with async_session() as db:
                            service = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                            v2_data = await service.get_adverts_v2(campaign_ids=batch)
                        all_v2_adverts.extend(v2_data)
                        if len(campaign_ids) > 50:
                            await asyncio.sleep(1)  # rate limit between batches
                    except Exception as e:
                        logger.warning(f"[snapshot] Error fetching V2 adverts batch: {e}")

                logger.info(f"[snapshot] shop={shop_id}: got {len(all_v2_adverts)} V2 adverts")

                # Step 3: Update dim_advert_campaigns with full data
                if all_v2_adverts:
                    dim_count = loader.load_campaigns_v2(
                        all_v2_adverts, shop_id, campaign_type_map
                    )
                    logger.info(f"[snapshot] Updated {dim_count} campaigns in dim_advert_campaigns")

                # Step 4: Save bid snapshot to log_wb_bids
                bids_count = 0
                if all_v2_adverts:
                    bid_rows = event_detector.extract_bid_snapshot_v2(shop_id, all_v2_adverts)
                    if bid_rows:
                        bids_count = loader.insert_bid_snapshot(bid_rows)
                        logger.info(f"[snapshot] Saved {bids_count} bid rows to log_wb_bids")

            await engine.dispose()
            return {
                "status": "completed",
                "campaigns": total,
                "v2_adverts": len(all_v2_adverts),
                "dim_updated": dim_count if all_v2_adverts else 0,
                "bids_saved": bids_count,
            }
        except Exception as e:
            await engine.dispose()
            logger.error(f"[snapshot] sync_wb_campaign_snapshot failed for shop={shop_id}: {e}")
            raise e

    return asyncio.run(run_snapshot())


@celery_app.task(bind=True, time_limit=120, soft_time_limit=110)
def sync_all_campaign_snapshots(self):
    """
    Dispatcher: fetch campaign snapshots for ALL active WB shops.
    Called by scheduler every 30 minutes.
    """
    import asyncio
    import logging
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import select
    from app.config import get_settings
    from app.core.encryption import decrypt_api_key
    from app.models.shop import Shop

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def _dispatch():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with sf() as db:
            result = await db.execute(
                select(Shop).where(Shop.status == "active")
            )
            shops = result.scalars().all()

        await engine.dispose()
        return shops

    shops = asyncio.run(_dispatch())

    if not shops:
        logger.info("sync_all_campaign_snapshots: no active shops")
        return {"dispatched": 0}

    dispatched = 0
    for shop in shops:
        if shop.marketplace != "wildberries":
            continue
        try:
            api_key = decrypt_api_key(shop.api_key_encrypted)
        except Exception as e:
            logger.error(f"sync_all_campaign_snapshots: shop {shop.id} decrypt failed: {e}")
            continue

        sync_wb_campaign_snapshot.delay(shop_id=shop.id, api_key=api_key)
        dispatched += 1
        logger.info(f"sync_all_campaign_snapshots: dispatched for shop {shop.id}")

    logger.info(f"sync_all_campaign_snapshots: dispatched {dispatched} tasks")
    return {"dispatched": dispatched}


@celery_app.task(bind=True, time_limit=14400, soft_time_limit=14100)
def sync_wb_advert_history(
    self,
    shop_id: int,
    api_key: str,
    days_back: int = 180,
    accumulate_history: bool = True,
):
    """
    Sync Advertising Data (History) using V3 API.
    
    NEW FEATURES:
    - Accumulates data in ads_raw_history (MergeTree, not replacing)
    - Detects bid/status/item changes and logs to event_log
    - Enriches data with vendor_code
    - Sets is_associated flag for Halo items
    
    V3 API constraints:
    - Max period: 31 days per request
    - Max campaigns: 50 per request
    - Rate limit: ~1 request per minute
    
    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import date, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_advertising_report_service import WBAdvertisingReportService
    from app.services.wb_advertising_loader import WBAdvertisingLoader
    from app.services.event_detector import EventDetector
    import logging
    
    logger = logging.getLogger(__name__)
    settings = get_settings()
    
    # Helper to split list into chunks
    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
            
    # Helper to generate 30-day intervals for V3 API
    def generate_intervals_30days(days_back: int):
        intervals = []
        end_date = date.today()
        start_date = end_date - timedelta(days=days_back)
        
        current = start_date
        while current < end_date:
            next_end = current + timedelta(days=29)  # 30 days inclusive
            if next_end > end_date:
                next_end = end_date
            intervals.append((current, next_end))
            current = next_end + timedelta(days=1)
        return intervals
    
    # Helper to save events to PostgreSQL
    def save_events_to_db(events: list):
        """Persist detected events to PostgreSQL event_log table."""
        import psycopg2
        import json
        
        if not events:
            return
        
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", 5432),
                user=os.getenv("POSTGRES_USER", "mms"),
                password=os.getenv("POSTGRES_PASSWORD", "mms"),
                database=os.getenv("POSTGRES_DB", "mms")
            )
            cursor = conn.cursor()
            
            for event in events:
                cursor.execute("""
                    INSERT INTO event_log (shop_id, advert_id, nm_id, event_type, old_value, new_value, event_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    event.get("shop_id"),
                    event.get("advert_id"),
                    event.get("nm_id"),
                    event.get("event_type"),
                    event.get("old_value"),
                    event.get("new_value"),
                    json.dumps(event.get("event_metadata")) if event.get("event_metadata") else None
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Saved {len(events)} events to event_log")
        except Exception as e:
            logger.error(f"Error saving events to DB: {e}")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        loader = WBAdvertisingLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
        )
        event_detector = EventDetector(redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"))
        
        try:
            with loader:
                # 1. Fetch Campaigns (via MarketplaceClient + proxy)
                self.update_state(state='PROGRESS', meta={'status': 'Fetching campaigns list via proxy...'})
                async with async_session() as db:
                    service = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                    campaigns = await service.get_campaigns()
                
                # Filter valid campaigns (status 7, 9, 11 only)
                campaigns = [c for c in campaigns if c.get("advertId") and c.get("status") in [7, 9, 11]]
                
                # Save DIM
                loader.load_campaigns(campaigns, shop_id)
                campaign_ids = [c["advertId"] for c in campaigns]
                total_campaigns = len(campaign_ids)
                
                logger.info(f"Found {total_campaigns} campaigns for V3 stats sync")
                
                # 2. Fetch V2 Adverts (for bids, event detection, campaign items)
                # V2 replaces deprecated get_campaign_settings (V1)
                campaign_items = {}
                cpm_values = {}
                campaign_types = {}  # for CPC vs CPM differentiation
                events_detected = 0
                all_v2_adverts = []
                
                # Build type map from count response
                campaign_type_map = {c["advertId"]: c.get("type", 0) for c in campaigns}
                
                self.update_state(state='PROGRESS', meta={'status': 'Fetching V2 adverts for bids & events...'})
                
                for i in range(0, len(campaign_ids), 50):
                    batch_ids = campaign_ids[i:i+50]
                    try:
                        async with async_session() as db:
                            svc = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                            v2_data = await svc.get_adverts_v2(campaign_ids=batch_ids)
                        all_v2_adverts.extend(v2_data)
                        if i + 50 < len(campaign_ids):
                            await asyncio.sleep(1)
                    except Exception as e:
                        logger.warning(f"Error fetching V2 adverts batch: {e}")
                
                # Detect events using V2 format (per-nm_id bid tracking)
                if accumulate_history and all_v2_adverts:
                    events = event_detector.detect_changes_v2(
                        shop_id, all_v2_adverts, campaign_type_map
                    )
                    events_detected = len(events)
                    
                    if events:
                        save_events_to_db(events)
                    
                    # Extract campaign items, bids, types from V2
                    campaign_items, cpm_values, campaign_types = \
                        event_detector.extract_all_campaign_data_v2(
                            all_v2_adverts, campaign_type_map
                        )
                
                # Save bid snapshot to log_wb_bids
                if all_v2_adverts:
                    try:
                        bid_rows = event_detector.extract_bid_snapshot_v2(shop_id, all_v2_adverts)
                        if bid_rows:
                            bids_inserted = loader.insert_bid_snapshot(bid_rows)
                            logger.info(f"V2 bid snapshot: {bids_inserted} rows saved to log_wb_bids")
                    except Exception as e:
                        logger.warning(f"Error saving V2 bid snapshot: {e}")
                
                # Load V2 campaigns into dim (with richer fields)
                if all_v2_adverts:
                    try:
                        loader.load_campaigns_v2(all_v2_adverts, shop_id, campaign_type_map)
                    except Exception as e:
                        logger.warning(f"Error loading V2 campaigns: {e}")
                
                # 3. Prepare vendor_code cache (for enrichment)
                vendor_code_cache = {}
                if accumulate_history:
                    all_nm_ids = set()
                    for items in campaign_items.values():
                        all_nm_ids.update(items)
                    
                    if all_nm_ids:
                        self.update_state(state='PROGRESS', meta={'status': 'Loading vendor_code cache...'})
                        vendor_code_cache = loader.get_vendor_code_cache(list(all_nm_ids))
                
                # 4. Prepare Chunks (max 50 per request)
                batches = list(chunk_list(campaign_ids, 50))
                intervals = generate_intervals_30days(days_back)
                total_steps = len(batches) * len(intervals)
                current_step = 0
                
                stats_inserted = 0
                history_inserted = 0
                empty_interval_streak = 0
                MAX_EMPTY_INTERVALS = 2  # 2 × 30 days with no data → stop
                
                logger.info(f"Processing {len(intervals)} intervals x {len(batches)} batches = {total_steps} requests")
                
                # 5. Loop through intervals and batches
                for interval in intervals:
                    d_from = interval[0].strftime("%Y-%m-%d")
                    d_to = interval[1].strftime("%Y-%m-%d")
                    interval_rows = 0
                    
                    for batch in batches:
                        current_step += 1
                        
                        self.update_state(state='PROGRESS', meta={
                            'current': current_step,
                            'total': total_steps,
                            'status': f'V3: Fetching {d_from} - {d_to} ({len(batch)} campaigns) via proxy'
                        })
                        
                        try:
                            # Fetch V3 stats (via MarketplaceClient + proxy)
                            async with async_session() as db:
                                service = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                                full_stats = await service.get_full_stats_v3(batch, d_from, d_to)
                            
                            # Parse & Insert into V3 table (legacy, for compatibility)
                            rows = loader.parse_full_stats_v3(full_stats, shop_id)
                            count = loader.insert_stats_v3(rows)
                            stats_inserted += count
                            interval_rows += count
                            
                            # NEW: Insert into history table (accumulation)
                            history_count = 0
                            if accumulate_history and full_stats:
                                history_rows = loader.parse_stats_for_history(
                                    full_stats, shop_id,
                                    campaign_items, vendor_code_cache, cpm_values,
                                    campaign_types
                                )
                                history_count = loader.insert_history(history_rows)
                                history_inserted += history_count
                                interval_rows += history_count
                            
                            logger.info(f"Step {current_step}/{total_steps}: Inserted {count} rows (history: {history_count if accumulate_history else 'N/A'})")
                            
                            # Rate Limit Sleep (60-70 sec)
                            await asyncio.sleep(65)
                            
                        except Exception as e:
                            logger.warning(f"Error fetching batch: {e}")
                            # Wait longer on error
                            await asyncio.sleep(70) 
                    
                    # Track empty intervals for early exit
                    if interval_rows == 0:
                        empty_interval_streak += 1
                        logger.info(
                            f"Interval {d_from}→{d_to}: 0 rows "
                            f"(empty streak: {empty_interval_streak}/{MAX_EMPTY_INTERVALS})"
                        )
                        if empty_interval_streak >= MAX_EMPTY_INTERVALS:
                            remaining = len(intervals) - intervals.index(interval) - 1
                            logger.info(
                                f"Early exit: {MAX_EMPTY_INTERVALS} consecutive "
                                f"empty intervals — skipping remaining {remaining} intervals"
                            )
                            break
                    else:
                        empty_interval_streak = 0
                            
            await engine.dispose()
            return {
                "status": "completed",
                "campaigns_loaded": total_campaigns,
                "stats_rows_inserted": stats_inserted,
                "history_rows_inserted": history_inserted,
                "events_detected": events_detected,
                "days_back": days_back,
                "api_version": "V3",
                "accumulate_history": accumulate_history
            }
        except Exception as e:
            await engine.dispose()
            logger.error(f"sync_wb_advert_history failed: {e}")
            raise e
            
    return asyncio.run(run_sync())


# ===================
# COMMERCIAL MONITORING TASKS
# Prices, stocks, warehouses, content
# ===================

@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def sync_commercial_data(
    self,
    shop_id: int,
    api_key: str,
):
    """
    Commercial Monitoring: Sync prices and stocks (every 30 min).
    
    Flow:
        Step 1: Fetch prices -> Redis + dim_products
        Step 2: Fetch stocks -> Redis
        Step 3: Detect events (PRICE_CHANGE, STOCK_OUT, STOCK_REPLENISH)
        Step 4: Batch insert into ClickHouse fact_inventory_snapshot
        Step 5: Check ITEM_INACTIVE (zero stock + active ads)
    
    Queue: HEAVY.
    """
    import asyncio
    import os
    import json
    from datetime import datetime
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_prices_service import WBPricesService
    from app.services.wb_stocks_service import WBStocksService
    from app.services.event_detector import CommercialEventDetector
    from app.core.clickhouse import get_clickhouse_client
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    stats = {
        "shop_id": shop_id,
        "prices_fetched": 0,
        "stocks_fetched": 0,
        "events_detected": 0,
        "snapshot_rows": 0,
        "errors": [],
    }

    # Helper to save events to PostgreSQL (reuse pattern from advert task)
    def save_events_to_db(events: list):
        import psycopg2
        if not events:
            return
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", 5432),
                user=os.getenv("POSTGRES_USER", "mms"),
                password=os.getenv("POSTGRES_PASSWORD", "mms"),
                database=os.getenv("POSTGRES_DB", "mms"),
            )
            cursor = conn.cursor()
            for event in events:
                cursor.execute("""
                    INSERT INTO event_log (shop_id, advert_id, nm_id, event_type, old_value, new_value, event_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    event.get("shop_id"),
                    event.get("advert_id"),
                    event.get("nm_id"),
                    event.get("event_type"),
                    event.get("old_value"),
                    event.get("new_value"),
                    json.dumps(event.get("event_metadata")) if event.get("event_metadata") else None,
                ))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Saved {len(events)} commercial events to event_log")
        except Exception as e:
            logger.error(f"Error saving commercial events to DB: {e}")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        fetched_at = datetime.utcnow()

        async with async_session() as db:
            # ===== Step 1: Fetch Prices =====
            self.update_state(state="PROGRESS", meta={"status": "Fetching prices..."})

            prices_service = WBPricesService(
                db=db, shop_id=shop_id, api_key=api_key,
                redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"),
            )
            prices_data = await prices_service.fetch_all_prices()
            stats["prices_fetched"] = len(prices_data)

            if prices_data:
                await prices_service.update_products_db(prices_data)

            # ===== Step 2: Detect PRICE_CHANGE (before updating Redis!) =====
            self.update_state(state="PROGRESS", meta={"status": "Detecting price events..."})

            event_detector = CommercialEventDetector(
                redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0")
            )
            all_events = []

            if prices_data:
                price_events = event_detector.detect_price_changes(shop_id, prices_data)
                all_events.extend(price_events)
                # Now update Redis state (after detection)
                prices_service.update_redis_state(prices_data)

            # ===== Step 3: Fetch Stocks =====
            self.update_state(state="PROGRESS", meta={"status": "Fetching stocks..."})

            stocks_service = WBStocksService(
                db=db, shop_id=shop_id, api_key=api_key,
                redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"),
            )
            nm_ids = await stocks_service.get_product_nm_ids()

            stocks_data = []
            if nm_ids:
                stocks_data = await stocks_service.fetch_stocks(nm_ids)
                stats["stocks_fetched"] = len(stocks_data)

            # ===== Step 4: Detect STOCK_OUT / STOCK_REPLENISH =====
            self.update_state(state="PROGRESS", meta={"status": "Detecting stock events..."})

            if stocks_data:
                stock_events = event_detector.detect_stock_events(shop_id, stocks_data)
                all_events.extend(stock_events)
                # Ensure warehouse dictionary
                warehouse_map = await stocks_service.ensure_warehouses(stocks_data)
                # Now update Redis state (after detection)
                stocks_service.update_redis_state(stocks_data)
            else:
                warehouse_map = {}

            # ===== Step 5: Batch insert into ClickHouse =====
            self.update_state(state="PROGRESS", meta={"status": "Inserting into ClickHouse..."})

            # Build prices map for snapshot rows
            prices_map = {
                item["nm_id"]: {
                    "converted_price": item["converted_price"],
                    "discount": item["discount"],
                }
                for item in prices_data
            }

            snapshot_rows = stocks_service.prepare_snapshot_rows(
                stocks_data, warehouse_map, prices_map, fetched_at
            )

            if snapshot_rows:
                try:
                    ch_client = get_clickhouse_client()
                    column_names = [
                        "fetched_at", "shop_id", "nm_id", "warehouse_name",
                        "warehouse_id", "quantity", "price", "discount",
                    ]
                    rows = [
                        [r[col] for col in column_names]
                        for r in snapshot_rows
                    ]
                    ch_client.insert(
                        "mms_analytics.fact_inventory_snapshot",
                        rows,
                        column_names=column_names,
                    )
                    stats["snapshot_rows"] = len(rows)
                    ch_client.close()
                    logger.info(f"Inserted {len(rows)} rows into fact_inventory_snapshot")
                except Exception as e:
                    logger.error(f"ClickHouse insert error: {e}")
                    stats["errors"].append(str(e))

            # ===== Step 6: Save events to PostgreSQL =====
            stats["events_detected"] = len(all_events)
            save_events_to_db(all_events)

        await engine.dispose()

    try:
        asyncio.run(run_sync())
        stats["status"] = "completed"
        return stats
    except Exception as exc:
        stats["status"] = "failed"
        stats["fatal_error"] = str(exc)
        self.retry(exc=exc, countdown=120, max_retries=2)


@celery_app.task(bind=True, time_limit=600, soft_time_limit=550)
def sync_warehouses(
    self,
    shop_id: int,
    api_key: str,
):
    """
    Sync WB warehouse dictionary (daily).
    
    Fetches all WB offices and upserts into dim_warehouses.
    Queue: HEAVY.
    """
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_warehouses_service import WBWarehousesService
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            service = WBWarehousesService(db=db, shop_id=shop_id, api_key=api_key)
            synced = await service.sync_warehouses()
            return {"shop_id": shop_id, "warehouses_synced": synced, "status": "completed"}

        await engine.dispose()

    try:
        return asyncio.run(run_sync())
    except Exception as exc:
        self.retry(exc=exc, countdown=300, max_retries=2)


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def sync_product_content(
    self,
    shop_id: int,
    api_key: str,
):
    """
    Sync product content data + SEO audit (daily).
    
    1. Fetch product cards (titles, descriptions, photos, dimensions)
    2. Load existing content hashes from dim_product_content
    3. Detect content events (title/desc/photo changes)
    4. Upsert new hashes as reference for next comparison
    5. Update dim_products and Redis state
    6. Save events to event_log
    
    Queue: HEAVY.
    """
    import asyncio
    import os
    import json
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text as sa_text
    from app.config import get_settings
    from app.services.wb_content_service import WBContentService
    from app.services.event_detector import ContentEventDetector
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    def save_events_to_db(events: list):
        import psycopg2
        if not events:
            return
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=os.getenv("POSTGRES_PORT", 5432),
                user=os.getenv("POSTGRES_USER", "mms"),
                password=os.getenv("POSTGRES_PASSWORD", "mms"),
                database=os.getenv("POSTGRES_DB", "mms"),
            )
            cursor = conn.cursor()
            for event in events:
                cursor.execute("""
                    INSERT INTO event_log (shop_id, advert_id, nm_id, event_type, old_value, new_value, event_metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    event.get("shop_id"),
                    event.get("advert_id"),
                    event.get("nm_id"),
                    event.get("event_type"),
                    event.get("old_value"),
                    event.get("new_value"),
                    json.dumps(event.get("event_metadata")) if event.get("event_metadata") else None,
                ))
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Saved {len(events)} content events to event_log")
        except Exception as e:
            logger.error(f"Error saving content events to DB: {e}")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            # Step 1: Fetch fresh cards from WB API
            self.update_state(state="PROGRESS", meta={
                "status": "Fetching product cards...",
                "step": "1/5",
            })

            service = WBContentService(
                db=db, shop_id=shop_id, api_key=api_key,
                redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"),
            )
            cards_data = await service.fetch_all_cards()

            if not cards_data:
                return {"shop_id": shop_id, "products_updated": 0, "status": "no_data"}

            # Step 2: Load existing content hashes from dim_product_content
            self.update_state(state="PROGRESS", meta={
                "status": "Loading reference hashes from DB...",
                "step": "2/5",
                "products_fetched": len(cards_data),
            })

            rows = await db.execute(
                sa_text("""
                    SELECT nm_id, title_hash, description_hash, 
                           main_photo_id, photos_hash, photos_count
                    FROM dim_product_content
                    WHERE shop_id = :shop_id
                """),
                {"shop_id": shop_id},
            )
            existing_hashes = {}
            for row in rows.fetchall():
                existing_hashes[row[0]] = {
                    "title_hash": row[1],
                    "description_hash": row[2],
                    "main_photo_id": row[3],
                    "photos_hash": row[4],
                    "photos_count": row[5] or 0,
                }

            # Step 3: Detect content events
            self.update_state(state="PROGRESS", meta={
                "status": "Detecting content changes...",
                "step": "3/5",
                "existing_hashes": len(existing_hashes),
            })

            content_detector = ContentEventDetector()
            events = content_detector.detect_content_events(
                shop_id, cards_data, existing_hashes
            )
            save_events_to_db(events)

            # Step 4: Upsert content hashes (new reference)
            self.update_state(state="PROGRESS", meta={
                "status": "Updating content hashes...",
                "step": "4/5",
                "events_detected": len(events),
            })

            hashes_upserted = await service.upsert_content_hashes(cards_data)

            # Step 5: Update dim_products and Redis
            self.update_state(state="PROGRESS", meta={
                "status": "Updating product data and Redis...",
                "step": "5/5",
            })

            updated = await service.update_products_db(cards_data)
            service.update_redis_image_state(cards_data)

            return {
                "shop_id": shop_id,
                "products_updated": updated,
                "hashes_upserted": hashes_upserted,
                "events_detected": len(events),
                "event_types": {
                    etype: len([e for e in events if e["event_type"] == etype])
                    for etype in set(e["event_type"] for e in events)
                } if events else {},
                "existing_hashes_count": len(existing_hashes),
                "status": "completed",
            }

        await engine.dispose()

    try:
        return asyncio.run(run_sync())
    except Exception as exc:
        self.retry(exc=exc, countdown=300, max_retries=2)


# ====================
# SALES FUNNEL TASKS
# Fetch WB funnel analytics: views, cart, orders, buyouts, conversions
# ====================

@celery_app.task(bind=True, time_limit=600, soft_time_limit=550)
def sync_sales_funnel(self, shop_id: int, api_key: str):
    """
    Every-30-min Sync: fetch last 2 days of sales funnel data.

    Every sync INSERTs new rows (append-only) — this preserves history
    of how WB metrics change throughout the day.
    Use fact_sales_funnel_latest view for latest values.

    Pipeline:
    1. Get nm_ids from dim_products
    2. Fetch daily history for yesterday + today
    3. INSERT into ClickHouse fact_sales_funnel (append, not replace)

    Routed to HEAVY queue.
    """
    import asyncio
    import os
    from datetime import date, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_sales_funnel_service import (
        WBSalesFunnelService,
        SalesFunnelLoader,
    )

    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            async with WBSalesFunnelService(db, shop_id, api_key) as svc:
                # Step 1: Get nm_ids
                self.update_state(state="PROGRESS", meta={
                    "status": "Getting product list...",
                    "step": "1/3",
                })
                nm_ids = await svc.get_product_nm_ids()
                if not nm_ids:
                    return {
                        "shop_id": shop_id,
                        "status": "no_products",
                        "message": "No products found in dim_products",
                    }

                # Step 2: Fetch history for last 2 days
                end = date.today()
                start = end - timedelta(days=1)

                self.update_state(state="PROGRESS", meta={
                    "status": f"Fetching funnel data for {len(nm_ids)} products...",
                    "step": "2/3",
                    "nm_ids_count": len(nm_ids),
                    "period": f"{start} — {end}",
                })

                def on_progress(done, total):
                    self.update_state(state="PROGRESS", meta={
                        "status": f"API requests: {done}/{total}",
                        "step": "2/3",
                    })

                rows = await svc.fetch_history_by_days(
                    nm_ids, start, end,
                    progress_callback=on_progress,
                )

                # Step 3: INSERT into ClickHouse (append-only)
                self.update_state(state="PROGRESS", meta={
                    "status": f"Inserting {len(rows)} rows into ClickHouse...",
                    "step": "3/3",
                })

                loader = SalesFunnelLoader(
                    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                    port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                    username=os.getenv("CLICKHOUSE_USER", "default"),
                    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                )
                with loader:
                    inserted = loader.insert_rows(rows)

                return {
                    "shop_id": shop_id,
                    "status": "completed",
                    "nm_ids": len(nm_ids),
                    "period": f"{start} — {end}",
                    "rows_fetched": len(rows),
                    "rows_inserted": inserted,
                }

        await engine.dispose()

    try:
        return asyncio.run(run_sync())
    except Exception as exc:
        self.retry(exc=exc, countdown=120, max_retries=2)


@celery_app.task(bind=True, time_limit=7200, soft_time_limit=7000)
def backfill_sales_funnel(
    self,
    shop_id: int,
    api_key: str,
    months: int = 6,
):
    """
    One-time Backfill: load historical funnel data.

    Strategy:
    1. Try CSV report (async: create → poll → download → parse)
    2. Fallback: History API week-by-week

    Routed to HEAVY queue. Can run up to 2 hours.
    """
    import asyncio
    import os
    from datetime import date, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_sales_funnel_service import (
        WBSalesFunnelService,
        SalesFunnelLoader,
    )

    settings = get_settings()

    async def run_backfill():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        end = date.today()
        start = end - timedelta(days=months * 30)

        async with async_session() as db:
            async with WBSalesFunnelService(db, shop_id, api_key) as svc:
                # Step 1: Get nm_ids
                self.update_state(state="PROGRESS", meta={
                    "status": "Getting product list...",
                    "step": "1/4",
                })
                nm_ids = await svc.get_product_nm_ids()
                if not nm_ids:
                    return {
                        "shop_id": shop_id,
                        "status": "no_products",
                    }

                rows = []
                method_used = "unknown"

                # Step 2: Try CSV report first
                self.update_state(state="PROGRESS", meta={
                    "status": "Creating CSV report...",
                    "step": "2/4",
                    "period": f"{start} — {end}",
                })

                try:
                    report_id = await svc.create_csv_report(start, end, "day")

                    # Poll until ready
                    self.update_state(state="PROGRESS", meta={
                        "status": f"Waiting for CSV report {report_id[:8]}...",
                        "step": "2/4",
                    })

                    status = await svc.poll_csv_report(report_id)

                    if status == "SUCCESS":
                        # Download and parse
                        self.update_state(state="PROGRESS", meta={
                            "status": "Downloading CSV report...",
                            "step": "3/4",
                        })
                        zip_data = await svc.download_csv_report(report_id)
                        rows = svc.parse_csv_report(zip_data)
                        method_used = "csv_report"
                    else:
                        raise RuntimeError(f"CSV report status: {status}")

                except Exception as csv_err:
                    # Fallback: use History API
                    self.update_state(state="PROGRESS", meta={
                        "status": f"CSV failed ({csv_err}), using History API...",
                        "step": "2/4",
                    })

                    def on_progress(done, total):
                        self.update_state(state="PROGRESS", meta={
                            "status": f"History API: {done}/{total} requests",
                            "step": "3/4",
                        })

                    # History API only supports last 7 days
                    # (WB returns 400 "excess limit on days" for older dates)
                    history_start = max(start, end - timedelta(days=6))

                    rows = await svc.fetch_history_by_days(
                        nm_ids, history_start, end,
                        progress_callback=on_progress,
                    )
                    method_used = "history_api"

                # Step 4: Insert into ClickHouse
                self.update_state(state="PROGRESS", meta={
                    "status": f"Inserting {len(rows)} rows into ClickHouse...",
                    "step": "4/4",
                })

                loader = SalesFunnelLoader(
                    host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                    port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                    username=os.getenv("CLICKHOUSE_USER", "default"),
                    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                )
                with loader:
                    inserted = loader.insert_rows(rows)

                return {
                    "shop_id": shop_id,
                    "status": "completed",
                    "method": method_used,
                    "period": f"{start} — {end}",
                    "nm_ids": len(nm_ids),
                    "rows_parsed": len(rows),
                    "rows_inserted": inserted,
                }

        await engine.dispose()

    try:
        return asyncio.run(run_backfill())
    except Exception as exc:
        self.retry(exc=exc, countdown=300, max_retries=2)


# ════════════════════════════════════════════════════════════
# ORDERS MODULE — Operative orders & logistics
# ════════════════════════════════════════════════════════════

@celery_app.task(bind=True, time_limit=600, soft_time_limit=550)
def sync_orders(self, shop_id: int, api_key: str):
    """
    Every-10-min Sync: fetch recent orders from WB Statistics API.

    Uses MarketplaceClient (proxy rotation, rate limiting, circuit breaker).
    flag=0: returns orders where lastChangeDate >= dateFrom.
    dateFrom = last max_date in ClickHouse (fallback 1h ago).

    Routed to HEAVY queue.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_orders_service import (
        WBOrdersService,
        OrdersLoader,
        _parse_order_row,
    )

    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        # Step 1: Determine dateFrom from ClickHouse
        loader = OrdersLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        with loader:
            stats = loader.get_stats(shop_id)
            if stats and stats.get("max_date") and stats["max_date"] != "1970-01-02 00:00:00":
                date_from = datetime.fromisoformat(str(stats["max_date"])) - timedelta(minutes=5)
            else:
                date_from = datetime.utcnow() - timedelta(hours=1)

        self.update_state(state="PROGRESS", meta={
            "status": f"Fetching orders since {date_from.isoformat()} via proxy...",
            "step": "1/3",
        })

        # Step 2: Fetch via MarketplaceClient (with proxy)
        async with async_session() as db:
            svc = WBOrdersService(db, shop_id, api_key)
            raw_orders = await svc.fetch_all_orders(date_from, flag=0)

        await engine.dispose()

        if not raw_orders:
            return {
                "shop_id": shop_id,
                "status": "no_new_orders",
                "date_from": date_from.isoformat(),
            }

        # Step 3: Parse
        self.update_state(state="PROGRESS", meta={
            "status": f"Parsing {len(raw_orders)} orders...",
            "step": "2/3",
        })
        rows = [_parse_order_row(order, shop_id) for order in raw_orders]

        # Step 4: INSERT
        self.update_state(state="PROGRESS", meta={
            "status": f"Inserting {len(rows)} rows into ClickHouse...",
            "step": "3/3",
        })
        with loader:
            inserted = loader.insert_rows(rows)
            stats = loader.get_stats(shop_id)

        return {
            "shop_id": shop_id,
            "status": "completed",
            "date_from": date_from.isoformat(),
            "orders_fetched": len(raw_orders),
            "rows_inserted": inserted,
            "stats": stats,
        }

    try:
        return asyncio.run(run_sync())
    except Exception as exc:
        logger.exception("sync_orders failed for shop_id=%s", shop_id)
        self.retry(exc=exc, countdown=60, max_retries=3)


@celery_app.task(bind=True, time_limit=7200, soft_time_limit=7000)
def backfill_orders(self, shop_id: int, api_key: str, days: int = 90):
    """
    One-time Backfill: load ALL orders for the past N days (default: 90).

    Uses MarketplaceClient (proxy rotation, rate limiting, circuit breaker).
    flag=0 with pagination: fetches up to 80K rows per page,
    uses lastChangeDate from last row for next page.
    Rate limit: 1 request per minute.

    Routed to HEAVY queue. Can run up to 2 hours.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_orders_service import (
        WBOrdersService,
        OrdersLoader,
        _parse_order_row,
    )

    settings = get_settings()

    async def run_backfill():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        date_from = datetime.utcnow() - timedelta(days=days)

        self.update_state(state="PROGRESS", meta={
            "status": f"Fetching orders for last {days} days via proxy (paginated)...",
            "step": "1/3",
            "date_from": date_from.isoformat(),
        })

        def on_progress(page, total):
            self.update_state(state="PROGRESS", meta={
                "status": f"Page {page}: {total} orders fetched so far...",
                "step": "1/3",
            })

        async with async_session() as db:
            svc = WBOrdersService(db, shop_id, api_key)
            raw_orders = await svc.fetch_all_orders(
                date_from, flag=0, on_progress=on_progress,
            )

        await engine.dispose()

        if not raw_orders:
            return {
                "shop_id": shop_id,
                "status": "no_orders",
                "days": days,
                "date_from": date_from.isoformat(),
            }

        # Step 2: Parse
        self.update_state(state="PROGRESS", meta={
            "status": f"Parsing {len(raw_orders)} orders...",
            "step": "2/3",
        })
        rows = [_parse_order_row(order, shop_id) for order in raw_orders]

        # Step 3: INSERT
        self.update_state(state="PROGRESS", meta={
            "status": f"Inserting {len(rows)} rows into ClickHouse...",
            "step": "3/3",
        })

        loader = OrdersLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
        with loader:
            inserted = loader.insert_rows(rows)
            stats = loader.get_stats(shop_id)

        return {
            "shop_id": shop_id,
            "status": "completed",
            "days": days,
            "date_from": date_from.isoformat(),
            "orders_fetched": len(raw_orders),
            "rows_inserted": inserted,
            "stats": stats,
        }

    try:
        return asyncio.run(run_backfill())
    except Exception as exc:
        logger.exception("backfill_orders failed for shop_id=%s", shop_id)
        self.retry(exc=exc, countdown=300, max_retries=2)


# ===================
# OZON CORE TASKS
# Products, Content, Inventory
# ===================

@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_products(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Sync Ozon product catalog to dim_ozon_products (PostgreSQL).

    Pipeline:
        1. POST /v3/product/list — get all product_ids
        2. POST /v3/product/info/list — detailed info (batches of 100)
        3. Upsert into dim_ozon_products (all fields incl. images, statuses, etc.)
        4. Detect image hash changes → events

    Queue: HEAVY (moderate runtime ~1-2 min for 40 products).
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService, upsert_ozon_products,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list (paginated)
            self.update_state(state='PROGRESS', meta={'status': 'Fetching Ozon product list via proxy...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]
            logger.info(f"Ozon: found {len(product_ids)} products for shop {shop_id}")

            # 2. Fetch detailed product info (batches of 100)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching details for {len(product_ids)} products via proxy...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            # 3. Upsert into PostgreSQL (returns count + image change events)
            self.update_state(state='PROGRESS', meta={'status': 'Upserting into dim_ozon_products...'})
            conn_params = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "user": os.getenv("POSTGRES_USER", "mms"),
                "password": os.getenv("POSTGRES_PASSWORD", "mms"),
                "database": os.getenv("POSTGRES_DB", "mms"),
            }
            count, events = upsert_ozon_products(conn_params, shop_id, products_info)

            if events:
                logger.info(f"Detected {len(events)} image change events")

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_found": len(product_list),
                "products_upserted": count,
                "image_events": len(events),
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_product_snapshots(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Daily snapshot of Ozon product data to ClickHouse.

    One API call → 4 ClickHouse inserts:
        1. Promotions → fact_ozon_promotions
        2. Availability → fact_ozon_availability
        3. Commissions → fact_ozon_commissions
        4. Inventory (prices+stocks) → fact_ozon_inventory

    Queue: HEAVY. Designed to run once daily.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService,
        OzonPromotionsLoader, OzonAvailabilityLoader,
        OzonCommissionsLoader, OzonInventoryLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_db = os.getenv("CLICKHOUSE_DB", "mms_analytics")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list
            self.update_state(state='PROGRESS', meta={'status': 'Fetching product list...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]

            # 2. Fetch product info (one call for all data)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching info for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            ch_kwargs = dict(host=ch_host, port=ch_port, username=ch_user, password=os.getenv("CLICKHOUSE_PASSWORD", ""), database=ch_db)
            results = {}

            # 3. Promotions
            self.update_state(state='PROGRESS', meta={'status': 'Inserting promotions...'})
            with OzonPromotionsLoader(**ch_kwargs) as loader:
                results["promotions"] = loader.insert_promotions(shop_id, products_info)
                results["promo_stats"] = loader.get_stats(shop_id)

            # 4. Availability
            self.update_state(state='PROGRESS', meta={'status': 'Inserting availability...'})
            with OzonAvailabilityLoader(**ch_kwargs) as loader:
                results["availability"] = loader.insert_availability(shop_id, products_info)
                results["avail_stats"] = loader.get_stats(shop_id)

            # 5. Commissions
            self.update_state(state='PROGRESS', meta={'status': 'Inserting commissions...'})
            with OzonCommissionsLoader(**ch_kwargs) as loader:
                results["commissions"] = loader.insert_commissions(shop_id, products_info)

            # 6. Inventory
            self.update_state(state='PROGRESS', meta={'status': 'Inserting inventory...'})
            with OzonInventoryLoader(**ch_kwargs) as loader:
                results["inventory"] = loader.insert_inventory(shop_id, products_info)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_found": len(product_list),
                **results,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_orders(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    days_back: int = 14,
):
    """
    Sync Ozon orders (FBO + FBS) to ClickHouse fact_ozon_orders.

    Default: last 14 days (overlap window to catch status changes).
    ReplacingMergeTree deduplicates by posting_number.

    Pipeline:
        1. Fetch FBO postings (paginated)
        2. Fetch FBS postings (paginated)
        3. Normalize → 1 row per product per posting
        4. Insert into ClickHouse

    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_orders_service import OzonOrdersService, OzonOrdersLoader
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    since = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")
    to = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching orders (last {days_back} days)...',
            })

            async with sf() as db:
                service = OzonOrdersService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                orders = await service.fetch_all_orders(since, to)

            logger.info(f"Ozon orders: {len(orders)} rows for shop {shop_id}")

            self.update_state(state='PROGRESS', meta={
                'status': f'Inserting {len(orders)} orders into ClickHouse...',
            })

            with OzonOrdersLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_orders(shop_id, orders)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "days_back": days_back,
                "rows_inserted": inserted,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def backfill_ozon_orders(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    days_back: int = 365,
):
    """
    Backfill historical Ozon orders (FBO + FBS) into ClickHouse.

    Downloads up to 1 year of order history.
    Longer time limit (1 hour) since this may fetch thousands of orders.

    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_orders_service import OzonOrdersService, OzonOrdersLoader
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    since = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00.000Z")
    to = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            self.update_state(state='PROGRESS', meta={
                'status': f'Backfilling {days_back} days of orders...',
            })

            async with sf() as db:
                service = OzonOrdersService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                orders = await service.fetch_all_orders(since, to)

            logger.info(
                "Backfill: %d order rows for shop %d (%d days)",
                len(orders), shop_id, days_back,
            )

            self.update_state(state='PROGRESS', meta={
                'status': f'Inserting {len(orders)} historical orders...',
            })

            with OzonOrdersLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_orders(shop_id, orders)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "days_back": days_back,
                "rows_inserted": inserted,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_finance(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Daily sync of Ozon financial transactions to ClickHouse.

    Fetches yesterday + today (2-day window) to catch late-arriving transactions.
    ReplacingMergeTree deduplicates by operation_id.

    Pipeline:
        1. POST /v3/finance/transaction/list (2 days, paginated)
        2. Normalize → category mapping
        3. Insert into ClickHouse fact_ozon_transactions

    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_finance_service import (
        OzonFinanceService, OzonTransactionsLoader, normalize_transactions,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    since = (now - timedelta(days=2)).strftime("%Y-%m-%dT00:00:00.000Z")
    to = now.strftime("%Y-%m-%dT23:59:59.000Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            self.update_state(state='PROGRESS', meta={
                'status': 'Fetching financial transactions (last 2 days)...',
            })

            async with sf() as db:
                service = OzonFinanceService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw_ops = await service.fetch_transactions(since, to)

            normalized = normalize_transactions(raw_ops)
            logger.info(f"Finance sync: {len(normalized)} transactions for shop {shop_id}")

            self.update_state(state='PROGRESS', meta={
                'status': f'Inserting {len(normalized)} transactions into ClickHouse...',
            })

            with OzonTransactionsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_transactions(shop_id, normalized)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "rows_inserted": inserted,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def backfill_ozon_finance(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    months_back: int = 12,
):
    """
    Backfill historical Ozon financial transactions into ClickHouse.

    Iterates by calendar months (API limit: max 1 month per request).
    Rate limit: 1.5s between pages.

    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_finance_service import (
        OzonFinanceService, OzonTransactionsLoader, normalize_transactions,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    since = (now - timedelta(days=months_back * 30)).strftime("%Y-%m-%dT00:00:00.000Z")
    to = now.strftime("%Y-%m-%dT23:59:59.000Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            self.update_state(state='PROGRESS', meta={
                'status': f'Backfilling {months_back} months of finance data...',
            })

            async with sf() as db:
                service = OzonFinanceService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw_ops = await service.fetch_all_transactions(since, to)

            normalized = normalize_transactions(raw_ops)
            logger.info(
                "Finance backfill: %d transactions for shop %d (%d months)",
                len(normalized), shop_id, months_back,
            )

            self.update_state(state='PROGRESS', meta={
                'status': f'Inserting {len(normalized)} historical transactions...',
            })

            with OzonTransactionsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_transactions(shop_id, normalized)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "months_back": months_back,
                "rows_inserted": inserted,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_funnel(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Daily sync of Ozon sales funnel analytics (views→cart→orders).

    Fetches yesterday's data via POST /v1/analytics/data.
    14 metrics per SKU per day.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_funnel_service import OzonFunnelService, OzonFunnelLoader
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    date_from = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonFunnelService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_all_funnel(date_from, date_to)

            with OzonFunnelLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def backfill_ozon_funnel(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    days_back: int = 365,
):
    """
    Backfill historical Ozon funnel analytics.

    Chunks by 90-day quarters automatically.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_funnel_service import OzonFunnelService, OzonFunnelLoader
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
    date_to = now.strftime("%Y-%m-%d")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            self.update_state(state='PROGRESS', meta={
                'status': f'Backfilling {days_back} days of funnel data...',
            })
            async with sf() as db:
                service = OzonFunnelService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_all_funnel(date_from, date_to)

            with OzonFunnelLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "days_back": days_back,
                    "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_returns(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Sync recent Ozon returns/cancellations (last 30 days).
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_returns_service import (
        OzonReturnsService, OzonReturnsLoader, normalize_returns,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    time_from = (now - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00Z")
    time_to = now.strftime("%Y-%m-%dT23:59:59Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def backfill_ozon_returns(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    days_back: int = 180,
):
    """
    Backfill historical Ozon returns (up to 6 months).
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_returns_service import (
        OzonReturnsService, OzonReturnsLoader, normalize_returns,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    time_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    time_to = now.strftime("%Y-%m-%dT23:59:59Z")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "days_back": days_back,
                    "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_ozon_warehouse_stocks(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Snapshot Ozon warehouse stock levels (FBO + FBS).
    Run twice daily for accurate stock tracking.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_warehouse_stocks_service import (
        OzonWarehouseStocksService, OzonWarehouseStocksLoader,
    )

    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonWarehouseStocksService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_warehouse_stocks()

            with OzonWarehouseStocksLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_ozon_prices(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Snapshot Ozon product prices and commissions.
    Run daily or twice daily for price tracking.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_price_service import OzonPriceService, OzonPriceLoader

    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonPriceService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_prices()

            with OzonPriceLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=120, soft_time_limit=100)
def sync_ozon_seller_rating(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Daily snapshot of Ozon seller rating metrics.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_seller_rating_service import (
        OzonSellerRatingService, OzonSellerRatingLoader,
    )

    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonSellerRatingService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_rating()

            with OzonSellerRatingLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_content(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Sync Ozon content hashes (MD5) and detect changes.

    Pipeline:
        1. Fetch product list + info (images, names)
        2. Fetch descriptions (sequential, rate limited)
        3. Compute MD5 hashes of title, description, images
        4. Compare with dim_ozon_product_content → detect events
        5. Save events to event_log

    Events detected:
        - OZON_PHOTO_CHANGE (main_image or gallery)
        - OZON_SEO_CHANGE (title or description)

    Queue: HEAVY (descriptions fetched sequentially).
    """
    import asyncio
    import os
    import json
    import psycopg2
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService, upsert_ozon_content,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    def save_ozon_events(events: list, conn_params: dict):
        """Save Ozon content events to event_log."""
        if not events:
            return
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        for event in events:
            cursor.execute("""
                INSERT INTO event_log (shop_id, advert_id, nm_id, event_type, old_value, new_value, event_metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                event.get("shop_id"),
                None,
                event.get("product_id"),
                event.get("event_type"),
                event.get("old_value"),
                event.get("new_value"),
                json.dumps({"field": event.get("field"), "platform": "ozon"}),
            ))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Saved {len(events)} Ozon content events")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list
            self.update_state(state='PROGRESS', meta={'status': 'Fetching product list...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]

            # 2. Fetch product info (images, names)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching info for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            # 3. Fetch all descriptions (sequential)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching descriptions for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                descriptions = await service.fetch_all_descriptions(product_ids)

            # 4. Upsert content hashes and detect events
            self.update_state(state='PROGRESS', meta={'status': 'Computing hashes and detecting events...'})
            conn_params = {
                "host": os.getenv("POSTGRES_HOST", "postgres"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "user": os.getenv("POSTGRES_USER", "mms"),
                "password": os.getenv("POSTGRES_PASSWORD", "mms_secret"),
                "database": os.getenv("POSTGRES_DB", "mms"),
            }
            count, events = upsert_ozon_content(conn_params, shop_id, products_info, descriptions)

            # 5. Save events
            if events:
                save_ozon_events(events, conn_params)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_processed": count,
                "events_detected": len(events),
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=300, soft_time_limit=270)
def sync_ozon_inventory(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Snapshot Ozon inventory (prices + stocks) to ClickHouse.

    Pipeline:
        1. Fetch product list
        2. Fetch product info (prices, stocks)
        3. Insert snapshot into fact_ozon_inventory (ClickHouse)

    Designed to run every 30 minutes for continuous monitoring.
    Queue: HEAVY.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService, OzonInventoryLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list
            self.update_state(state='PROGRESS', meta={'status': 'Fetching Ozon products...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]

            # 2. Fetch product info
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching prices & stocks for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            # 3. Insert into ClickHouse
            self.update_state(state='PROGRESS', meta={'status': 'Inserting into ClickHouse...'})
            with OzonInventoryLoader(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
            ) as loader:
                inserted = loader.insert_inventory(shop_id, products_info)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_found": len(product_list),
                "rows_inserted": inserted,
                "stats": stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=300, soft_time_limit=270)
def sync_ozon_commissions(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Snapshot Ozon product commissions to ClickHouse (daily).

    Pipeline:
        1. Fetch product list
        2. Fetch product info (includes commissions)
        3. Extract commissions and insert into fact_ozon_commissions

    Queue: HEAVY. Designed to run once daily.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService, OzonCommissionsLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list
            self.update_state(state='PROGRESS', meta={'status': 'Fetching Ozon products...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]

            # 2. Fetch product info (commissions included)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching info + commissions for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            # 3. Insert commissions into ClickHouse
            self.update_state(state='PROGRESS', meta={'status': 'Inserting commissions into ClickHouse...'})
            with OzonCommissionsLoader(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
            ) as loader:
                inserted = loader.insert_commissions(shop_id, products_info)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_found": len(product_list),
                "commissions_inserted": inserted,
                "stats": stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=300, soft_time_limit=270)
def sync_ozon_content_rating(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Snapshot Ozon content ratings to ClickHouse (daily).

    Pipeline:
        1. Fetch product list
        2. Fetch product info to get SKUs
        3. Fetch content ratings via /v1/product/rating-by-sku
        4. Insert into fact_ozon_content_rating

    Queue: HEAVY. Designed to run once daily.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_products_service import (
        OzonProductsService, OzonContentRatingLoader, _extract_sku,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Fetch product list
            self.update_state(state='PROGRESS', meta={'status': 'Fetching Ozon products...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                product_list = await service.fetch_product_list()

            product_ids = [p["product_id"] for p in product_list]

            # 2. Fetch product info (to get SKUs)
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching info for {len(product_ids)} products...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                products_info = await service.fetch_product_info(product_ids)

            # Build SKU list and SKU → product_id map
            skus = []
            sku_to_pid = {}
            for item in products_info:
                sku = _extract_sku(item)
                pid = item.get("id")
                if sku and pid:
                    skus.append(sku)
                    sku_to_pid[sku] = pid

            logger.info("Found %d SKUs for content rating check", len(skus))

            # 3. Fetch content ratings
            self.update_state(state='PROGRESS', meta={
                'status': f'Fetching content ratings for {len(skus)} SKUs...',
            })
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                ratings = await service.fetch_content_ratings(skus)

            logger.info("Got %d content ratings from API", len(ratings))

            # 4. Insert into ClickHouse
            self.update_state(state='PROGRESS', meta={'status': 'Inserting ratings into ClickHouse...'})
            with OzonContentRatingLoader(
                host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
                port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
                username=os.getenv("CLICKHOUSE_USER", "default"),
                database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
            ) as loader:
                inserted = loader.insert_ratings(shop_id, ratings, sku_to_pid)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "skus_checked": len(skus),
                "ratings_inserted": inserted,
                "stats": stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


# ===================
# OZON ADS & BIDS TRACKING
# monitor_ozon_bids (15 min), sync_ozon_ad_stats (60 min), backfill_ozon_ads (one-time)
# Uses MarketplaceClient (proxy, rate limiting, circuit breaker) — same as WB.
# ===================

@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def monitor_ozon_bids(
    self,
    shop_id: int,
    perf_client_id: str,
    perf_client_secret: str,
):
    """
    Monitor Ozon ad bids every 15 minutes + detect events.

    Pipeline:
        1. OAuth2 token (cached in Redis)
        2. GET /api/client/campaign → list campaigns (via proxy)
        3. GET /api/client/campaign/{id}/v2/products → current bids (via proxy)
        4. OzonAdsEventDetector: compare with Redis → detect events
        5. Insert events into PostgreSQL event_log
        6. Insert changed bids into ClickHouse log_ozon_bids

    Events detected (same as WB):
        OZON_BID_CHANGE, OZON_STATUS_CHANGE, OZON_BUDGET_CHANGE,
        OZON_ITEM_ADD, OZON_ITEM_REMOVE

    Queue: FAST (real-time bid tracking).
    """
    import asyncio
    import json
    import os
    import logging
    from datetime import datetime
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text
    from app.config import get_settings
    from app.services.ozon_ads_service import OzonAdsService, OzonBidsLoader
    from app.services.ozon_ads_event_detector import OzonAdsEventDetector

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_monitor():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        self.update_state(state='PROGRESS', meta={'status': 'Fetching Ozon ad bids via proxy...'})

        # Redis for token caching + bid delta-check
        import redis.asyncio as aioredis
        redis_url = getattr(settings, 'redis_url', None) or os.environ.get(
            'REDIS_URL', 'redis://redis:6379/0'
        )
        redis_client = aioredis.from_url(redis_url, decode_responses=True)

        try:
            async with async_session() as db:
                service = OzonAdsService(
                    db=db,
                    shop_id=shop_id,
                    perf_client_id=perf_client_id,
                    perf_client_secret=perf_client_secret,
                    redis_client=redis_client,
                )

                # 1. Get all campaigns (for status/budget tracking)
                campaigns = await service.get_campaigns()
                running_campaigns = [
                    c for c in campaigns
                    if c.get("state") == "CAMPAIGN_STATE_RUNNING"
                ]

                # 2. Get products per campaign (for bid/item tracking)
                products_by_campaign = {}
                all_bids = []

                for camp in running_campaigns:
                    campaign_id = camp.get("id")
                    if not campaign_id:
                        continue

                    products = await service.get_campaign_products(campaign_id)
                    products_by_campaign[int(campaign_id)] = products

                    for p in products:
                        all_bids.append({
                            "campaign_id": int(campaign_id),
                            "sku": int(p.get("sku", 0)),
                            "bid_micro": int(p.get("bid", 0)),
                            "bid_rub": int(p.get("bid", 0)) / 1_000_000,
                            "title": p.get("title", ""),
                        })

                    await asyncio.sleep(0.3)

                logger.info(
                    "Ozon: fetched %d bids across %d campaigns for shop %d",
                    len(all_bids), len(running_campaigns), shop_id,
                )

                # 3. Event Detection (BID_CHANGE, STATUS_CHANGE, BUDGET_CHANGE, ITEM_ADD/REMOVE)
                detector = OzonAdsEventDetector(redis_url=str(redis_url))
                events = detector.detect_all(
                    shop_id=shop_id,
                    campaigns=campaigns,
                    products_by_campaign=products_by_campaign,
                )

                # 4. Save events to PostgreSQL event_log
                events_saved = 0
                if events:
                    for event in events:
                        metadata_json = json.dumps(event.get("event_metadata")) \
                            if event.get("event_metadata") else None
                        await db.execute(text("""
                            INSERT INTO event_log
                                (created_at, shop_id, advert_id, nm_id,
                                 event_type, old_value, new_value, event_metadata)
                            VALUES
                                (:created_at, :shop_id, :advert_id, :nm_id,
                                 :event_type, :old_value, :new_value, CAST(:event_metadata AS jsonb))
                        """), {
                            "created_at": datetime.utcnow(),
                            "shop_id": event["shop_id"],
                            "advert_id": event["advert_id"],
                            "nm_id": event.get("nm_id"),
                            "event_type": event["event_type"],
                            "old_value": event.get("old_value"),
                            "new_value": event.get("new_value"),
                            "event_metadata": metadata_json,
                        })
                    await db.commit()
                    events_saved = len(events)
                    logger.info("Ozon: saved %d events to event_log", events_saved)

            if not all_bids:
                return {
                    "shop_id": shop_id,
                    "bids_fetched": 0, "bids_changed": 0,
                    "events_detected": events_saved,
                }

            # 5. Delta-check for ClickHouse insertion (same as before)
            cache_key = f"ozon_bids_cache:{shop_id}"
            cached_raw = await redis_client.get(cache_key)
            cached_bids = json.loads(cached_raw) if cached_raw else {}

            changed_bids = []
            new_cache = {}

            for bid in all_bids:
                key = f"{bid['campaign_id']}:{bid['sku']}"
                current_bid = bid['bid_rub']
                new_cache[key] = current_bid

                old_bid = cached_bids.get(key)
                if old_bid is None or abs(float(old_bid) - current_bid) > 0.01:
                    changed_bids.append(bid)

            force_key = f"ozon_bids_last_full:{shop_id}"
            last_full = await redis_client.get(force_key)
            force_write = not last_full

            if force_write and not changed_bids:
                changed_bids = all_bids
                logger.info("Ozon: force-writing full bid snapshot")

            # 6. Insert changed bids into ClickHouse
            inserted = 0
            if changed_bids:
                ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

                with OzonBidsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                    inserted = loader.insert_bids(shop_id, changed_bids)

            # 7. Update Redis cache
            await redis_client.setex(cache_key, 7200, json.dumps(new_cache))
            if force_write or changed_bids:
                await redis_client.setex(force_key, 3600, "1")

            self.update_state(state='PROGRESS', meta={
                'status': f'Done: {inserted} bids, {events_saved} events',
            })

            return {
                "shop_id": shop_id,
                "bids_fetched": len(all_bids),
                "bids_changed": len(changed_bids),
                "bids_inserted": inserted,
                "events_detected": events_saved,
            }

        finally:
            await redis_client.close()
            await engine.dispose()

    return asyncio.run(run_monitor())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
def sync_ozon_ad_stats(
    self,
    shop_id: int,
    perf_client_id: str,
    perf_client_secret: str,
    lookback_days: int = 3,
):
    """
    Sync Ozon ad statistics with sliding window (default: last 3 days).

    Pipeline:
        1. OAuth2 token
        2. GET campaigns → get all campaign IDs (via proxy)
        3. POST /api/client/statistics → UUID (async report, via proxy)
        4. Poll UUID until ready
        5. Download CSV → parse → insert ClickHouse fact_ozon_ad_daily

    Why 3-day window? Ozon attribution: buyer adds to cart today,
    pays tomorrow → order attributed to yesterday retroactively.
    ReplacingMergeTree auto-replaces old rows on FINAL query.

    Queue: HEAVY (60 min schedule).
    """
    import asyncio
    import os
    import logging
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_ads_service import OzonAdsService, OzonBidsLoader

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        self.update_state(state='PROGRESS', meta={'status': 'Preparing Ozon ad stats sync via proxy...'})

        import redis.asyncio as aioredis
        redis_url = getattr(settings, 'redis_url', None) or os.environ.get(
            'REDIS_URL', 'redis://redis:6379/0'
        )
        redis_client = aioredis.from_url(redis_url, decode_responses=True)

        try:
            # Check if backfill is running for ANY shop with the same
            # perf_client_id — skip to avoid competing for Ozon Performance
            # API rate limit (429 errors). Multiple shops can share one
            # Performance API account.
            backfill_active = await redis_client.get(f'ozon_ads_backfill:{perf_client_id}')
            if backfill_active:
                logger.info(
                    'shop %s: backfill_ozon_ads is running (perf_client=%s), '
                    'skipping periodic sync_ozon_ad_stats',
                    shop_id, perf_client_id[:20],
                )
                await redis_client.close()
                await engine.dispose()
                return {'status': 'skipped', 'reason': 'backfill in progress', 'shop_id': shop_id}

            async with async_session() as db:
                service = OzonAdsService(
                    db=db,
                    shop_id=shop_id,
                    perf_client_id=perf_client_id,
                    perf_client_secret=perf_client_secret,
                    redis_client=redis_client,
                )

                # 1. Get all campaign IDs
                campaigns = await service.get_campaigns()
                campaign_ids = [c["id"] for c in campaigns if c.get("id")]
                logger.info(f"Ozon: {len(campaign_ids)} campaigns for stats")

                if not campaign_ids:
                    return {"shop_id": shop_id, "campaigns": 0, "rows": 0}

                # 2. Date range: [today - lookback_days, today]
                today = datetime.utcnow().date()
                date_from = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                date_to = today.strftime("%Y-%m-%d")

                self.update_state(state='PROGRESS', meta={
                    'status': f'Ordering report {date_from} → {date_to} for {len(campaign_ids)} campaigns via proxy...',
                })

                # 3. Full pipeline: order → wait → download → parse
                all_rows = await service.fetch_statistics(
                    shop_id=shop_id,
                    campaign_ids=campaign_ids,
                    date_from=date_from,
                    date_to=date_to,
                )

            logger.info(f"Ozon: parsed {len(all_rows)} stats rows")

            # 4. Insert into ClickHouse
            inserted = 0
            if all_rows:
                ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

                with OzonBidsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                    inserted = loader.insert_stats(all_rows)

            self.update_state(state='PROGRESS', meta={
                'status': f'Done: {inserted} stats rows inserted',
            })

            return {
                "shop_id": shop_id,
                "campaigns": len(campaign_ids),
                "date_from": date_from,
                "date_to": date_to,
                "rows_parsed": len(all_rows),
                "rows_inserted": inserted,
            }

        finally:
            await redis_client.close()
            await engine.dispose()

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=7200, soft_time_limit=7000)
def backfill_ozon_ads(
    self,
    shop_id: int,
    perf_client_id: str,
    perf_client_secret: str,
    days_back: int = 180,
    chunk_days: int = 30,
):
    """
    Backfill Ozon ad statistics history (same as WB: 6 months, then sync).

    Loads data week-by-week to avoid overwhelming API.
    Same table fact_ozon_ad_daily — ReplacingMergeTree handles duplicates.

    Args:
        days_back: How many days of history to load (default: 180 = 6 months).
        chunk_days: How many days per API request (default: 7).

    Queue: HEAVY (one-time or manual).
    """
    import asyncio
    import os
    import logging
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_ads_service import OzonAdsService, OzonBidsLoader

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_backfill():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        import redis.asyncio as aioredis
        redis_url = getattr(settings, 'redis_url', None) or os.environ.get(
            'REDIS_URL', 'redis://redis:6379/0'
        )
        redis_client = aioredis.from_url(redis_url, decode_responses=True)

        try:
            # Set Redis lock keyed by perf_client_id to prevent periodic
            # sync_ozon_ad_stats from competing for the SAME Ozon Performance
            # API rate limit. Multiple shops may share one perf_client_id.
            # TTL = 2h (matches task time_limit), auto-expires if task crashes.
            await redis_client.set(
                f'ozon_ads_backfill:{perf_client_id}', '1', ex=7200,
            )
            logger.info('shop %s: backfill lock SET for perf_client=%s (TTL 2h)', shop_id, perf_client_id[:20])

            # Reset rate limiter backoff/429 counters for this shop's
            # ozon_performance marketplace. Previous 429 errors may have pushed
            # the backoff to maximum, creating a vicious cycle where retries
            # keep failing because the rate limiter itself blocks requests.
            backoff_key = f"mms:ratelimit:{shop_id}:ozon_performance:backoff"
            count_key = f"mms:ratelimit:{shop_id}:ozon_performance:429_count"
            deleted = await redis_client.delete(backoff_key, count_key)
            if deleted:
                logger.info('shop %s: reset %d rate-limiter keys for ozon_performance', shop_id, deleted)

            async with async_session() as db:
                service = OzonAdsService(
                    db=db,
                    shop_id=shop_id,
                    perf_client_id=perf_client_id,
                    perf_client_secret=perf_client_secret,
                    redis_client=redis_client,
                )

                # 1. Get all campaign IDs
                campaigns = await service.get_campaigns()
                campaign_ids = [c["id"] for c in campaigns if c.get("id")]

                if not campaign_ids:
                    return {"shop_id": shop_id, "error": "No campaigns found"}

                # 2. Build date chunks (newest first — so we get recent data
                #    before hitting old empty periods that trigger early exit)
                today = datetime.utcnow().date()
                start_date = today - timedelta(days=days_back)
                chunks = []
                chunk_start = start_date

                while chunk_start < today:
                    chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), today)
                    chunks.append((chunk_start, chunk_end))
                    chunk_start = chunk_end + timedelta(days=1)

                # Reverse: newest first, so we load recent data before
                # hitting old empty periods that trigger early exit
                chunks.reverse()

                logger.info(
                    f"Ozon backfill: {len(chunks)} chunks (newest first), "
                    f"{start_date} → {today}, {len(campaign_ids)} campaigns"
                )

                # 3. Process each chunk
                # Early exit: if N consecutive chunks return 0 rows,
                # stop — campaigns likely didn't exist that far back.
                MAX_EMPTY_STREAK = 5
                ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
                total_rows = 0
                empty_streak = 0

                with OzonBidsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                    # Sub-progress for frontend
                    _sub_key = f"sync_sub_progress:{shop_id}"

                    for i, (cf, ct) in enumerate(chunks):
                        self.update_state(state='PROGRESS', meta={
                            'status': f'Chunk {i+1}/{len(chunks)}: {cf} → {ct} via proxy',
                            'progress': f'{(i+1)*100//len(chunks)}%',
                        })
                        # Write sub-progress to Redis for parent task progress bar
                        await redis_client.set(_sub_key, f"Период {i + 1} из {len(chunks)}", ex=3600)

                        try:
                            rows = await service.fetch_statistics(
                                shop_id=shop_id,
                                campaign_ids=campaign_ids,
                                date_from=cf.strftime("%Y-%m-%d"),
                                date_to=ct.strftime("%Y-%m-%d"),
                            )

                            if rows:
                                inserted = loader.insert_stats(rows)
                                total_rows += inserted
                                empty_streak = 0  # reset on data found
                                logger.info(
                                    f"Backfill chunk {cf}→{ct}: {inserted} rows"
                                )
                            else:
                                empty_streak += 1
                                logger.info(
                                    f"Backfill chunk {cf}→{ct}: 0 rows "
                                    f"(empty streak: {empty_streak}/{MAX_EMPTY_STREAK})"
                                )
                                if empty_streak >= MAX_EMPTY_STREAK:
                                    logger.info(
                                        f"Early exit: {MAX_EMPTY_STREAK} consecutive "
                                        f"empty chunks — skipping remaining "
                                        f"{len(chunks) - i - 1} chunks"
                                    )
                                    break

                            # Rate limit: sleep between chunks
                            await asyncio.sleep(2)

                        except Exception as e:
                            logger.warning(f"Backfill chunk {cf}→{ct} failed: {e}")
                            empty_streak += 1  # treat errors as empty too
                            await asyncio.sleep(5)
                            continue

            return {
                "shop_id": shop_id,
                "campaigns": len(campaign_ids),
                "chunks": len(chunks),
                "total_rows": total_rows,
                "period": f"{start_date} → {today}",
            }

        finally:
            # Release backfill lock so periodic sync_ozon_ad_stats can resume
            try:
                await redis_client.delete(f'ozon_ads_backfill:{perf_client_id}')
                logger.info('shop %s: backfill lock RELEASED for perf_client=%s', shop_id, perf_client_id[:20])
            except Exception:
                pass
            await redis_client.close()
            await engine.dispose()

    return asyncio.run(run_backfill())

