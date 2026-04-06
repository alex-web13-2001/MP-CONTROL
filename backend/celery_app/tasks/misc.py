"""
Miscellaneous Celery tasks.

Contains general purpose and placeholder tasks that don't fit
into specific marketplace or sync categories.
"""

from celery_app.celery import celery_app


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
