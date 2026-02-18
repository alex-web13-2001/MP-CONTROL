"""Celery application configuration with separate queues."""

from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "mms_worker",
    broker=settings.get_celery_broker_url(),
    backend=settings.get_celery_result_backend(),
    include=["celery_app.tasks.tasks"],
)

# ===================
# Queue Configuration
# ===================
# Four queues for different workload types:
# - fast:     Time-critical tasks (autobidder, every minute)
# - sync:     Regular periodic sync (daily/frequent/ads) — high concurrency
# - backfill: Initial historical data loading — isolated, low concurrency
# - default:  General purpose tasks
#
# KEY INSIGHT: Marketplace API limits are per-API-key (not per-IP).
# Tasks for different shops can run fully in parallel without conflicting.
# The sync queue has high concurrency (8) to exploit this parallelism.

default_exchange = Exchange("mms", type="direct")

celery_app.conf.task_queues = (
    Queue("fast", default_exchange, routing_key="fast", queue_arguments={"x-max-priority": 10}),
    Queue("sync", default_exchange, routing_key="sync"),
    Queue("backfill", default_exchange, routing_key="backfill"),
    Queue("heavy", default_exchange, routing_key="heavy"),  # legacy compat
    Queue("default", default_exchange, routing_key="default"),
)

celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "mms"
celery_app.conf.task_default_routing_key = "default"

# Task routing based on task name
celery_app.conf.task_routes = {
    # Fast queue - autobidder tasks (critical, every minute)
    "celery_app.tasks.autobidder.*": {"queue": "fast", "routing_key": "fast"},
    "celery_app.tasks.tasks.update_bids": {"queue": "fast", "routing_key": "fast"},
    "celery_app.tasks.tasks.check_positions": {"queue": "fast", "routing_key": "fast"},
    "celery_app.tasks.tasks.monitor_ozon_bids": {"queue": "fast", "routing_key": "fast"},
    
    # Sync queue - regular periodic sync (high concurrency, multi-shop parallel)
    "celery_app.tasks.tasks.sync_marketplace_data": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_commercial_data": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_warehouses": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_product_content": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_ad_stats": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_wb_campaign_snapshot": {"queue": "sync", "routing_key": "sync"},
    # Ozon sync tasks
    "celery_app.tasks.tasks.sync_ozon_products": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_product_snapshots": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_finance": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_funnel": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_returns": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_seller_rating": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_content_rating": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_content": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_orders": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_warehouse_stocks": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_ozon_prices": {"queue": "sync", "routing_key": "sync"},
    # WB sync tasks
    "celery_app.tasks.tasks.sync_orders": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_sales_funnel": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_wb_advert_history": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_wb_finance_history": {"queue": "sync", "routing_key": "sync"},

    # Backfill queue - initial historical data loading (isolated from regular sync)
    "celery_app.tasks.tasks.load_historical_data": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.sync_full_history": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_ozon_ads": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_orders": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_ozon_orders": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_ozon_finance": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_ozon_funnel": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_ozon_returns": {"queue": "backfill", "routing_key": "backfill"},
    "celery_app.tasks.tasks.backfill_sales_funnel": {"queue": "backfill", "routing_key": "backfill"},

    # Sync coordinators (lightweight dispatchers, run on sync queue)
    "celery_app.tasks.tasks.sync_all_daily": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_all_frequent": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_all_ads": {"queue": "sync", "routing_key": "sync"},
    "celery_app.tasks.tasks.sync_all_campaign_snapshots": {"queue": "sync", "routing_key": "sync"},
}

# ===================
# Celery Configuration
# ===================
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    
    # Different timeouts for different queue priorities
    task_time_limit=14400,  # 4 hours max (for heavy tasks)
    task_soft_time_limit=14100,  # 3h 55min soft limit
    
    # Fast queue workers should be more responsive
    worker_prefetch_multiplier=1,
    
    # Result expiration
    result_expires=86400,  # 24 hours
    
    # Task priority (higher = more priority)
    task_default_priority=5,
    task_queue_max_priority=10,
    
    # Retry configuration
    task_acks_late=True,  # Acknowledge after task completes (safer)
    task_reject_on_worker_lost=True,
)

# ===================
# Beat Schedule (Periodic Tasks)
# ===================
celery_app.conf.beat_schedule = {
    # Autobidder - runs every minute on FAST queue
    "autobidder-update-bids": {
        "task": "celery_app.tasks.tasks.update_all_bids",
        "schedule": 60.0,  # Every 60 seconds
        "options": {"queue": "fast", "priority": 9},
    },
    
    # Position check - runs every 5 minutes on FAST queue
    "check-all-positions": {
        "task": "celery_app.tasks.tasks.check_all_positions",
        "schedule": 300.0,  # Every 5 minutes
        "options": {"queue": "fast", "priority": 7},
    },
    
    # === SYNC COORDINATORS ===
    # These tasks read ALL active shops from PostgreSQL,
    # decrypt credentials, and dispatch per-shop tasks to SYNC queue.
    # Redis deduplication prevents duplicate tasks in the queue.

    # Daily sync: products, finance, funnel, returns, ratings, content
    "sync-all-daily": {
        "task": "celery_app.tasks.tasks.sync_all_daily",
        "schedule": crontab(hour=3, minute=0),
        "options": {"queue": "sync", "priority": 4},
    },

    # Frequent sync (30 min): orders, stocks, prices
    "sync-all-frequent": {
        "task": "celery_app.tasks.tasks.sync_all_frequent",
        "schedule": 1800.0,  # Every 30 minutes
        "options": {"queue": "sync", "priority": 6},
    },

    # Ads sync (60 min): ad stats, bid monitoring
    "sync-all-ads": {
        "task": "celery_app.tasks.tasks.sync_all_ads",
        "schedule": 3600.0,  # Every 60 minutes
        "options": {"queue": "sync", "priority": 5},
    },

    # Campaign snapshots (30 min): bids, names, statuses — lightweight
    "sync-campaign-snapshots": {
        "task": "celery_app.tasks.tasks.sync_all_campaign_snapshots",
        "schedule": 1800.0,  # Every 30 minutes
        "options": {"queue": "sync", "priority": 6},
    },
}


# ===================
# Worker startup hints
# ===================
# Run separate workers for each queue:
#
# Fast worker (autobidder, positions) - high concurrency, low timeout:
#   celery -A celery_app.celery worker -Q fast -c 4 --loglevel=info -n fast@%h
#
# Sync worker (regular sync) - HIGH concurrency for multi-shop parallelism:
#   celery -A celery_app.celery worker -Q sync,heavy,default -c 8 --loglevel=info -n sync@%h
#
# Backfill worker (initial historical data) - low concurrency, isolated:
#   celery -A celery_app.celery worker -Q backfill -c 2 --loglevel=info -n backfill@%h
#
# Or all queues in one worker (development):
#   celery -A celery_app.celery worker -Q fast,sync,backfill,heavy,default --loglevel=info
