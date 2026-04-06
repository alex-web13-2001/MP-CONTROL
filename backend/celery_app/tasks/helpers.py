"""
Shared helpers for Celery task deduplication.

Provides Redis-based dedup dispatch to prevent duplicate tasks
from accumulating in the queue. Used by all coordinator tasks.
"""

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
