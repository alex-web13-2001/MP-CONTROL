"""
Coordinator tasks — Dispatchers that read shops and dispatch per-shop tasks.

These lightweight tasks run on the sync queue via Celery Beat.
They read all active shops from PostgreSQL, decrypt credentials,
and use _dedup_dispatch to dispatch per-shop tasks with deduplication.
"""

from celery_app.celery import celery_app
from celery_app.tasks.helpers import _dedup_dispatch


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
            from celery_app.tasks.ozon_sync import (
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
            from celery_app.tasks.wb_sync import (
                sync_warehouses,
                sync_product_content,
                sync_wb_finance_history,
                sync_wb_tariffs,
                sync_wb_paid_storage,
            )

            for task_ref in [sync_warehouses, sync_product_content, sync_wb_tariffs, sync_wb_paid_storage]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=82800, api_key=api_key):
                    dispatched += 1
                else:
                    skipped += 1

            # Финансовые отчёты WB — ежедневно, за последние 30 дней
            # (skipping already loaded weeks internally)
            if _dedup_dispatch(sync_wb_finance_history, r, shop.id, ttl=82800, api_key=api_key, days_back=30):
                dispatched += 1
            else:
                skipped += 1

            logger.info("sync_all_daily: WB shop %s (%s) — dispatched/skipped", shop.id, shop.name)

    logger.info("sync_all_daily: dispatched=%d skipped=%d shops=%d", dispatched, skipped, len(shops))
    return {"dispatched": dispatched, "skipped": skipped, "shops": len(shops)}

@celery_app.task(bind=True, time_limit=1800, soft_time_limit=1740)
def sync_all_placement_cost(self):
    """
    Dedicated coordinator for Ozon placement (storage) cost sync.

    Runs SEPARATELY from sync_all_daily to avoid competing for Ozon's
    per-second rate limit with other sync tasks (finance, funnel, returns, etc.).

    Dispatches sync_ozon_placement_cost for each Ozon shop SEQUENTIALLY
    with a 60-second gap between shops to stay under API rate limits.

    Scheduled via Celery Beat at 04:00 UTC (1 hour after sync_all_daily).
    """
    import asyncio
    import os
    import time
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

    async def _get_ozon_shops():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with sf() as db:
            result = await db.execute(
                select(Shop).where(
                    Shop.status == "active",
                    Shop.marketplace == "ozon",
                )
            )
            shops = result.scalars().all()
        await engine.dispose()
        return shops

    shops = asyncio.run(_get_ozon_shops())
    if not shops:
        logger.info("sync_all_placement_cost: no Ozon shops found")
        return {"dispatched": 0}

    from celery_app.tasks.ozon_sync import sync_ozon_placement_cost

    dispatched = 0
    for i, shop in enumerate(shops):
        try:
            api_key = decrypt_api_key(shop.api_key_encrypted)
            client_id = shop.client_id or ""
        except Exception as e:
            logger.error("sync_all_placement_cost: shop %s decrypt failed: %s", shop.id, e)
            continue

        if _dedup_dispatch(
            sync_ozon_placement_cost, r, shop.id, ttl=82800,
            api_key=api_key, client_id=client_id,
        ):
            dispatched += 1
            logger.info(
                "sync_all_placement_cost: dispatched shop %s (%s) [%d/%d]",
                shop.id, shop.name, i + 1, len(shops),
            )
        # Wait 60s between shops to avoid overwhelming Ozon's rate limit
        if i < len(shops) - 1:
            time.sleep(60)

    logger.info("sync_all_placement_cost: dispatched=%d shops=%d", dispatched, len(shops))
    return {"dispatched": dispatched, "shops": len(shops)}

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
            from celery_app.tasks.ozon_sync import (
                sync_ozon_orders,
                sync_ozon_warehouse_stocks,
                sync_ozon_prices,
                sync_ozon_turnover,
            )
            from celery_app.tasks.ozon_advertising import sync_ozon_ad_stats

            kwargs = dict(api_key=api_key, client_id=client_id)

            for task_ref in [sync_ozon_orders, sync_ozon_warehouse_stocks, sync_ozon_prices]:
                if _dedup_dispatch(task_ref, r, shop.id, ttl=1800, **kwargs):  # 30min TTL
                    dispatched += 1
                else:
                    skipped += 1

            # Turnover: once per day (86400 TTL)
            if _dedup_dispatch(sync_ozon_turnover, r, shop.id, ttl=86400, **kwargs):
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
            from celery_app.tasks.wb_sync import (
                sync_orders,
                sync_commercial_data,
                sync_sales_funnel,
            )
            from celery_app.tasks.wb_advertising import (
                sync_wb_advert_history,
                sync_normquery_data,
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

            # Normquery (search cluster) stats: every 6 hours
            if _dedup_dispatch(
                sync_normquery_data, r, shop.id, ttl=21600,  # 6h TTL
                api_key=api_key,
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

            from celery_app.tasks.ozon_advertising import (
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

            from celery_app.tasks.wb_advertising import sync_wb_advert_history

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

@celery_app.task(bind=True, time_limit=60, soft_time_limit=50)
def sync_all_budgets(self):
    """
    Dispatcher: sync budgets for ALL active WB shops.
    Called by scheduler every 15 minutes.
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
                select(Shop).where(Shop.status == "active", Shop.marketplace == "wildberries")
            )
            shops = result.scalars().all()
        await engine.dispose()
        return shops

    shops = asyncio.run(_dispatch())
    dispatched = 0

    for shop in shops:
        try:
            api_key = decrypt_api_key(shop.api_key_encrypted)
            from celery_app.tasks.wb_advertising import sync_wb_budgets
            sync_wb_budgets.delay(shop_id=shop.id, api_key=api_key)
            dispatched += 1
        except Exception as e:
            logger.error(f"sync_all_budgets: shop {shop.id} decrypt failed: {e}")

    logger.info(f"sync_all_budgets: dispatched {dispatched} tasks for {len(shops)} WB shops")
    return {"dispatched": dispatched}


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
        if shop.marketplace == "wildberries":
            try:
                api_key = decrypt_api_key(shop.api_key_encrypted)
            except Exception as e:
                logger.error(f"sync_all_campaign_snapshots: shop {shop.id} decrypt failed: {e}")
                continue

            from celery_app.tasks.wb_advertising import sync_wb_campaign_snapshot
            sync_wb_campaign_snapshot.delay(shop_id=shop.id, api_key=api_key)
            dispatched += 1
            logger.info(f"sync_all_campaign_snapshots: dispatched WB for shop {shop.id}")
            
        elif shop.marketplace in ("ozon", "ozon_performance"):
            if not shop.perf_client_id or not shop.perf_client_secret_encrypted:
                continue
            try:
                perf_secret = decrypt_api_key(shop.perf_client_secret_encrypted)
            except Exception as e:
                logger.error(f"sync_all_campaign_snapshots: Ozon shop {shop.id} decrypt failed: {e}")
                continue
                
            from celery_app.tasks.ozon_advertising import sync_ozon_campaigns_task
            sync_ozon_campaigns_task.delay(
                shop_id=shop.id,
                perf_client_id=shop.perf_client_id,
                perf_client_secret=perf_secret,
            )
            dispatched += 1
            logger.info(f"sync_all_campaign_snapshots: dispatched Ozon for shop {shop.id}")

    logger.info(f"sync_all_campaign_snapshots: dispatched {dispatched} tasks")
    return {"dispatched": dispatched}

