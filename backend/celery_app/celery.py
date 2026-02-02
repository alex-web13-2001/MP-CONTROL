"""Celery application configuration with separate queues."""

from celery import Celery
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
# Two separate queues to prevent heavy tasks from blocking fast ones:
# - fast: For time-critical tasks (autobidder, every minute)
# - heavy: For long-running tasks (6-month history sync)

default_exchange = Exchange("mms", type="direct")

celery_app.conf.task_queues = (
    Queue("fast", default_exchange, routing_key="fast", queue_arguments={"x-max-priority": 10}),
    Queue("heavy", default_exchange, routing_key="heavy"),
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
    
    # Heavy queue - data sync tasks (can take hours)
    "celery_app.tasks.tasks.load_historical_data": {"queue": "heavy", "routing_key": "heavy"},
    "celery_app.tasks.tasks.sync_full_history": {"queue": "heavy", "routing_key": "heavy"},
    "celery_app.tasks.tasks.sync_marketplace_data": {"queue": "heavy", "routing_key": "heavy"},
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
    
    # Daily sync - runs at 3 AM on HEAVY queue
    # "daily-sync": {
    #     "task": "celery_app.tasks.tasks.sync_marketplace_data",
    #     "schedule": crontab(hour=3, minute=0),
    #     "options": {"queue": "heavy"},
    # },
}


# ===================
# Worker startup hints
# ===================
# Run separate workers for each queue:
#
# Fast worker (autobidder, positions) - high concurrency, low timeout:
#   celery -A celery_app.celery worker -Q fast -c 4 --loglevel=info -n fast@%h
#
# Heavy worker (history sync) - low concurrency, high timeout:
#   celery -A celery_app.celery worker -Q heavy -c 2 --loglevel=info -n heavy@%h
#
# Default worker:
#   celery -A celery_app.celery worker -Q default -c 4 --loglevel=info -n default@%h
#
# Or all queues in one worker (development):
#   celery -A celery_app.celery worker -Q fast,heavy,default --loglevel=info
