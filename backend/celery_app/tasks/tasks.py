"""Celery tasks module with queue separation."""

from celery_app.celery import celery_app


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
    Load historical data for a new shop.
    
    This is a long-running task (can take hours for 6 months of data).
    Routed to HEAVY queue to not block autobidder.
    
    Args:
        shop_id: Shop ID to load data for
        months: Number of months to load (default: 6)
    """
    # TODO: Implement historical data loading
    # 1. Get API key from PostgreSQL (decrypt!)
    # 2. Loop through date ranges
    # 3. Fetch orders from WB/Ozon API
    # 4. Store in ClickHouse orders table
    # 5. Update progress (store in Redis for UI)
    return {"shop_id": shop_id, "months": months, "status": "loaded"}


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


# ===================
# WB FINANCE REPORTS
# Download weekly realization reports
# ===================

@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def download_wb_finance_reports(
    self,
    shop_id: int,
    date_from: str,
    date_to: str,
    api_key: str,
):
    """
    Download WB weekly finance reports for a period.
    
    This task:
    1. Gets all unique report IDs for the period
    2. For each report: request generation, poll status, download CSV
    3. Returns list of downloaded file paths
    
    Routed to HEAVY queue due to potential long duration.
    
    Args:
        shop_id: Shop ID in our system
        date_from: Start date (YYYY-MM-DD)
        date_to: End date (YYYY-MM-DD)
        api_key: WB API key (decrypted)
    
    Returns:
        Dict with status and list of results
    """
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import settings
    from app.services.wb_finance_report_service import WBFinanceReportService
    
    async def run_sync():
        # Create async database session
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        async with async_session() as db:
            async with WBFinanceReportService(
                db=db,
                shop_id=shop_id,
                api_key=api_key,
            ) as service:
                def progress_callback(current, total, report_id):
                    # Update Celery task state for monitoring
                    self.update_state(
                        state='PROGRESS',
                        meta={
                            'current': current,
                            'total': total,
                            'report_id': report_id,
                        }
                    )
                
                results = await service.sync_reports_for_period(
                    date_from=date_from,
                    date_to=date_to,
                    progress_callback=progress_callback,
                )
                
                return results
        
        await engine.dispose()
    
    try:
        results = asyncio.run(run_sync())
        
        success_count = sum(1 for r in results if r.get('status') == 'success')
        error_count = sum(1 for r in results if r.get('status') == 'error')
        
        return {
            "status": "completed",
            "shop_id": shop_id,
            "date_from": date_from,
            "date_to": date_to,
            "total_reports": len(results),
            "success_count": success_count,
            "error_count": error_count,
            "results": results,
        }
    except Exception as exc:
        self.retry(exc=exc, countdown=60, max_retries=3)


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
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_finance_report_service import WBFinanceReportService
    from app.services.wb_finance_loader import (
        WBReportParser,
        ClickHouseLoader,
        generate_week_ranges,
    )
    
    settings = get_settings()
    
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
        engine = create_async_engine(settings.postgres_url)
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
                        
                        # Optimization: Skip if data exists to save API budget
                        if loader.get_row_count(shop_id, date_from, date_to) > 0:
                            stats["processed_weeks"] += 1
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
                            # Step 1: Get report data directly (JSON)
                            # This replaces the old flow of ID -> Generate -> Poll -> Download
                            rows_data = await download_service.get_report_data(
                                date_from_str, date_to_str
                            )
                            
                            if not rows_data:
                                stats["processed_weeks"] += 1
                                # Still wait to respect rate limits even if empty
                                await asyncio.sleep(5)
                                continue
                            
                            # Step 2: Parse JSON rows
                            rows = list(parser.parse_json_rows(rows_data))
                            
                            if rows:
                                inserted = loader.insert_batch(rows)
                                stats["total_rows_inserted"] += inserted
                            
                            stats["processed_weeks"] += 1
                            
                            # Small pause between weeks
                            await asyncio.sleep(5)
                            
                        except Exception as e:
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
    from app.services.wb_advertising_report_service import WBAdvertisingReportService
    from app.services.wb_advertising_loader import WBAdvertisingLoader
    from app.services.event_detector import EventDetector
    import logging
    
    logger = logging.getLogger(__name__)
    
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
        service = WBAdvertisingReportService(api_key=api_key)
        loader = WBAdvertisingLoader(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            database=os.getenv("CLICKHOUSE_DB", "mms_analytics"),
        )
        event_detector = EventDetector(redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"))
        
        try:
            with loader:
                # 1. Fetch Campaigns
                self.update_state(state='PROGRESS', meta={'status': 'Fetching campaigns list...'})
                campaigns = await service.get_campaigns()
                
                # Filter valid campaigns (status 7, 9, 11 only)
                campaigns = [c for c in campaigns if c.get("advertId") and c.get("status") in [7, 9, 11]]
                
                # Save DIM
                loader.load_campaigns(campaigns, shop_id)
                campaign_ids = [c["advertId"] for c in campaigns]
                total_campaigns = len(campaign_ids)
                
                logger.info(f"Found {total_campaigns} campaigns for V3 stats sync")
                
                # 2. Fetch Campaign Settings (for bid detection & item lists)
                campaign_items = {}
                cpm_values = {}
                campaign_types = {}  # NEW: for CPC vs CPM differentiation
                events_detected = 0
                
                if accumulate_history:
                    self.update_state(state='PROGRESS', meta={'status': 'Fetching campaign settings...'})
                    
                    for batch in chunk_list(campaign_ids, 50):
                        try:
                            settings = await service.get_campaign_settings(batch)
                            
                            # Detect bid/status/item changes (with debouncing)
                            events = event_detector.detect_changes(shop_id, settings)
                            events_detected += len(events)
                            
                            # PERSIST EVENTS to PostgreSQL
                            if events:
                                save_events_to_db(events)
                            
                            # Extract items, CPM, and types for history parsing
                            batch_items, batch_cpm, batch_types = event_detector.extract_all_campaign_data(settings)
                            campaign_items.update(batch_items)
                            cpm_values.update(batch_cpm)
                            campaign_types.update(batch_types)
                            
                            await asyncio.sleep(1)  # Small delay between settings batches
                        except Exception as e:
                            logger.warning(f"Error fetching campaign settings: {e}")
                
                # 3. Prepare vendor_code cache (for enrichment)
                vendor_code_cache = {}
                if accumulate_history:
                    # Collect all nm_ids from campaign items
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
                
                logger.info(f"Processing {len(intervals)} intervals x {len(batches)} batches = {total_steps} requests")
                
                # 5. Loop through intervals and batches
                for interval in intervals:
                    d_from = interval[0].strftime("%Y-%m-%d")
                    d_to = interval[1].strftime("%Y-%m-%d")
                    
                    for batch in batches:
                        current_step += 1
                        
                        self.update_state(state='PROGRESS', meta={
                            'current': current_step,
                            'total': total_steps,
                            'status': f'V3: Fetching {d_from} - {d_to} ({len(batch)} campaigns)'
                        })
                        
                        try:
                            # Fetch V3 stats
                            full_stats = await service.get_full_stats_v3(batch, d_from, d_to)
                            
                            # Parse & Insert into V3 table (legacy, for compatibility)
                            rows = loader.parse_full_stats_v3(full_stats, shop_id)
                            count = loader.insert_stats_v3(rows)
                            stats_inserted += count
                            
                            # NEW: Insert into history table (accumulation)
                            if accumulate_history and full_stats:
                                history_rows = loader.parse_stats_for_history(
                                    full_stats, shop_id,
                                    campaign_items, vendor_code_cache, cpm_values,
                                    campaign_types
                                )
                                history_count = loader.insert_history(history_rows)
                                history_inserted += history_count
                            
                            logger.info(f"Step {current_step}/{total_steps}: Inserted {count} rows (history: {history_count if accumulate_history else 'N/A'})")
                            
                            # Rate Limit Sleep (60-70 sec)
                            await asyncio.sleep(65)
                            
                        except Exception as e:
                            logger.warning(f"Error fetching batch: {e}")
                            # Wait longer on error
                            await asyncio.sleep(70) 
                            
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
            logger.error(f"sync_wb_advert_history failed: {e}")
            raise e
            
    return asyncio.get_event_loop().run_until_complete(run_sync())
