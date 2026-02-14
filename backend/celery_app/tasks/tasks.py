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
        engine = create_async_engine(settings.postgres_url)
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
                
                # 2. Fetch Campaign Settings (for bid detection & item lists)
                campaign_items = {}
                cpm_values = {}
                campaign_types = {}  # NEW: for CPC vs CPM differentiation
                events_detected = 0
                
                if accumulate_history:
                    self.update_state(state='PROGRESS', meta={'status': 'Fetching campaign settings via proxy...'})
                    
                    for batch in chunk_list(campaign_ids, 50):
                        try:
                            async with async_session() as db:
                                service = WBAdvertisingReportService(db=db, shop_id=shop_id, api_key=api_key)
                                settings_data = await service.get_campaign_settings(batch)
                            
                            # Detect bid/status/item changes (with debouncing)
                            events = event_detector.detect_changes(shop_id, settings_data)
                            events_detected += len(events)
                            
                            # PERSIST EVENTS to PostgreSQL
                            if events:
                                save_events_to_db(events)
                            
                            # Extract items, CPM, and types for history parsing
                            batch_items, batch_cpm, batch_types = event_detector.extract_all_campaign_data(settings_data)
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
                            
                            logger.info(f"Step {current_step}/{total_steps}: Inserted {count} rows (history: {history_count if accumulate_history else 'N/A'})")
                            
                            # Rate Limit Sleep (60-70 sec)
                            await asyncio.sleep(65)
                            
                        except Exception as e:
                            logger.warning(f"Error fetching batch: {e}")
                            # Wait longer on error
                            await asyncio.sleep(70) 
                            
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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

                    rows = await svc.fetch_history_by_days(
                        nm_ids, start, end,
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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

            ch_kwargs = dict(host=ch_host, port=ch_port, username=ch_user, database=ch_db)
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
        engine = create_async_engine(settings.postgres_url)
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

            with OzonOrdersLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
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

            with OzonOrdersLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
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

            with OzonTransactionsLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
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

            with OzonTransactionsLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonFunnelService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_all_funnel(date_from, date_to)

            with OzonFunnelLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
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

            with OzonFunnelLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonWarehouseStocksService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_warehouse_stocks()

            with OzonWarehouseStocksLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonPriceService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_prices()

            with OzonPriceLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonSellerRatingService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_rating()

            with OzonSellerRatingLoader(host=ch_host, port=ch_port) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


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
        engine = create_async_engine(settings.postgres_url)
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
                "password": os.getenv("POSTGRES_PASSWORD", "mms"),
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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
        engine = create_async_engine(settings.postgres_url)
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

                with OzonBidsLoader(host=ch_host, port=ch_port) as loader:
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
        engine = create_async_engine(settings.postgres_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        self.update_state(state='PROGRESS', meta={'status': 'Preparing Ozon ad stats sync via proxy...'})

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

                with OzonBidsLoader(host=ch_host, port=ch_port) as loader:
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


@celery_app.task(bind=True, time_limit=3600, soft_time_limit=3500)
def backfill_ozon_ads(
    self,
    shop_id: int,
    perf_client_id: str,
    perf_client_secret: str,
    days_back: int = 180,
    chunk_days: int = 7,
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
        engine = create_async_engine(settings.postgres_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

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

                # 1. Get all campaign IDs
                campaigns = await service.get_campaigns()
                campaign_ids = [c["id"] for c in campaigns if c.get("id")]

                if not campaign_ids:
                    return {"shop_id": shop_id, "error": "No campaigns found"}

                # 2. Build date chunks (week by week, newest first)
                today = datetime.utcnow().date()
                start_date = today - timedelta(days=days_back)
                chunks = []
                chunk_start = start_date

                while chunk_start < today:
                    chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), today)
                    chunks.append((chunk_start, chunk_end))
                    chunk_start = chunk_end + timedelta(days=1)

                logger.info(
                    f"Ozon backfill: {len(chunks)} chunks, "
                    f"{start_date} → {today}, {len(campaign_ids)} campaigns"
                )

                # 3. Process each chunk
                ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
                total_rows = 0

                with OzonBidsLoader(host=ch_host, port=ch_port) as loader:
                    for i, (cf, ct) in enumerate(chunks):
                        self.update_state(state='PROGRESS', meta={
                            'status': f'Chunk {i+1}/{len(chunks)}: {cf} → {ct} via proxy',
                            'progress': f'{(i+1)*100//len(chunks)}%',
                        })

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
                                logger.info(
                                    f"Backfill chunk {cf}→{ct}: {inserted} rows"
                                )

                            # Rate limit: sleep between chunks
                            await asyncio.sleep(2)

                        except Exception as e:
                            logger.warning(f"Backfill chunk {cf}→{ct} failed: {e}")
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
            await redis_client.close()
            await engine.dispose()

    return asyncio.run(run_backfill())

