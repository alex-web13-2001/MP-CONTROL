"""
WB Advertising tasks — Campaign management, bids, and ad statistics for Wildberries.

Covers: autobidder (update_bids), position checks, campaign snapshots,
budget sync, advertising history, and normquery (search cluster) analytics.
"""

from celery_app.celery import celery_app


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



# ===================
# HEAVY QUEUE TASKS
# Historical data loading (long-running)
# ===================


@celery_app.task(bind=True, time_limit=300, soft_time_limit=280)
def sync_wb_budgets(self, shop_id: int, api_key: str):
    """
    Sync campaign budgets + account balance to Redis cache.

    Fetches:
      1. Active campaign IDs from dim_advert_campaigns (ClickHouse)
      2. Budget for each active campaign from WB API (semaphore=5)
      3. Account balance from WB API

    Stores in Redis:
      - budget:{shop_id}:{advert_id} → {total, daily} (TTL 20 min)
      - balance:{shop_id} → {balance, net} (TTL 20 min)

    Runs every 15 minutes via scheduler.
    Queue: SYNC.
    """
    import asyncio
    import os
    import json
    import logging
    import redis as redis_lib
    from datetime import datetime, date
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.wb_ad_management_service import WBAdManagementService
    from app.core.clickhouse import get_clickhouse_client
    from app.models.ad_auto_budget import AdAutoBudget
    from app.models.ad_audit_log import AdAuditLog

    logger = logging.getLogger(__name__)
    settings = get_settings()
    BUDGET_TTL = 3600  # 1 hour (sync runs every 15 min, but tasks can queue up)

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        # Get active campaign IDs from ClickHouse
        ch = get_clickhouse_client()
        rows = ch.query("""
            SELECT advert_id
            FROM mms_analytics.dim_advert_campaigns FINAL
            WHERE shop_id = {shop_id:UInt32}
              AND status IN (4, 9, 11)
        """, parameters={"shop_id": shop_id}).result_rows

        active_ids = [int(r[0]) for r in rows]
        logger.info(f"[budget-sync] shop={shop_id}: {len(active_ids)} active campaigns")

        if not active_ids:
            await engine.dispose()
            return {"status": "ok", "campaigns": 0}

        # Fetch budgets + balance from WB API
        async with async_session() as db:
            service = WBAdManagementService(db=db, shop_id=shop_id, api_key=api_key)
            budgets = await service.get_campaigns_budgets(active_ids)
            balance_raw = await service.get_balance()

        # Store in Redis
        r = redis_lib.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
        pipeline = r.pipeline()

        budget_count = 0
        for aid, data in budgets.items():
            cache_key = f"budget:{shop_id}:{aid}"
            pipeline.setex(cache_key, BUDGET_TTL, json.dumps(data))
            budget_count += 1

        if balance_raw.get("success"):
            balance_key = f"balance:{shop_id}"
            pipeline.setex(balance_key, BUDGET_TTL, json.dumps(balance_raw["data"]))

        pipeline.execute()
        r.close()

        logger.info(
            f"[budget-sync] shop={shop_id}: cached {budget_count} budgets, "
            f"balance={'ok' if balance_raw.get('success') else 'fail'}"
        )

        # ── Auto-replenishment check ─────────────────────────────
        auto_deposits = 0
        try:
            async with async_session() as db:
                result = await db.execute(
                    select(AdAutoBudget).where(
                        AdAutoBudget.shop_id == shop_id,
                        AdAutoBudget.enabled == True,  # noqa: E712
                    )
                )
                auto_rules = result.scalars().all()

            if auto_rules:
                logger.info(
                    f"[auto-budget] shop={shop_id}: checking {len(auto_rules)} enabled rules"
                )

            for rule in auto_rules:
                try:
                    # Reset daily counter if date changed
                    today = date.today()
                    if rule.last_reset_date != today:
                        rule.deposits_today = 0
                        rule.last_reset_date = today

                    # Skip if daily limit reached
                    if rule.deposits_today >= rule.max_per_day:
                        logger.debug(
                            f"[auto-budget] advert={rule.advert_id}: "
                            f"daily limit reached ({rule.deposits_today}/{rule.max_per_day})"
                        )
                        continue

                    # Check budget from freshly fetched data
                    budget_data = budgets.get(rule.advert_id, {})
                    current_total = budget_data.get("total", 0)

                    if current_total >= rule.threshold:
                        continue  # Budget OK — no action needed

                    # Budget below threshold → deposit!
                    logger.info(
                        f"[auto-budget] advert={rule.advert_id}: "
                        f"budget={current_total}₽ < threshold={rule.threshold}₽ → "
                        f"depositing {rule.amount}₽"
                    )

                    async with async_session() as db:
                        svc = WBAdManagementService(
                            db=db, shop_id=shop_id, api_key=api_key
                        )
                        deposit_result = await svc.deposit_budget(
                            rule.advert_id, rule.amount, rule.budget_type
                        )

                    if deposit_result.get("success"):
                        # Update rule state
                        async with async_session() as db:
                            result = await db.execute(
                                select(AdAutoBudget).where(
                                    AdAutoBudget.id == rule.id
                                )
                            )
                            fresh_rule = result.scalar_one()
                            fresh_rule.deposits_today += 1
                            fresh_rule.last_deposit_at = datetime.utcnow()
                            fresh_rule.last_reset_date = today
                            await db.commit()

                        # Update Redis cache with new budget
                        new_total = current_total + rule.amount
                        r2 = redis_lib.from_url(
                            os.getenv("REDIS_URL", "redis://redis:6379/0")
                        )
                        cache_key = f"budget:{shop_id}:{rule.advert_id}"
                        r2.setex(
                            cache_key, BUDGET_TTL,
                            json.dumps({"total": new_total, "daily": 0, "currency": "RUB"})
                        )
                        r2.close()

                        # Audit log
                        async with async_session() as db:
                            audit = AdAuditLog(
                                shop_id=shop_id,
                                action="auto_budget_deposit",
                                advert_id=rule.advert_id,
                                details={
                                    "amount": rule.amount,
                                    "budget_type": rule.budget_type,
                                    "budget_before": current_total,
                                    "budget_after": new_total,
                                    "threshold": rule.threshold,
                                    "deposits_today": rule.deposits_today + 1,
                                    "source": "celery:sync_wb_budgets",
                                },
                                success="true",
                            )
                            db.add(audit)
                            await db.commit()

                        auto_deposits += 1
                        logger.info(
                            f"[auto-budget] ✅ advert={rule.advert_id}: "
                            f"deposited {rule.amount}₽ (budget: {current_total}→{new_total}₽)"
                        )

                        # Small delay between deposits to respect WB rate limits
                        await asyncio.sleep(1.0)
                    else:
                        logger.warning(
                            f"[auto-budget] ❌ advert={rule.advert_id}: "
                            f"deposit failed: {deposit_result.get('message')}"
                        )
                        # Log failed deposit
                        async with async_session() as db:
                            audit = AdAuditLog(
                                shop_id=shop_id,
                                action="auto_budget_deposit",
                                advert_id=rule.advert_id,
                                details={
                                    "amount": rule.amount,
                                    "budget_before": current_total,
                                    "threshold": rule.threshold,
                                    "error": deposit_result.get("message", "Unknown"),
                                },
                                success="false",
                                error_message=deposit_result.get("message", "")[:500],
                            )
                            db.add(audit)
                            await db.commit()

                except Exception as e:
                    logger.error(
                        f"[auto-budget] Error processing rule for advert={rule.advert_id}: {e}"
                    )

        except Exception as e:
            logger.error(f"[auto-budget] Failed to check auto-budget rules for shop={shop_id}: {e}")

        await engine.dispose()

        return {
            "status": "completed",
            "shop_id": shop_id,
            "budgets_cached": budget_count,
            "balance_cached": bool(balance_raw.get("success")),
            "auto_deposits": auto_deposits,
        }

    return asyncio.run(run_sync())




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
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
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
            from app.config import get_settings
            conn = psycopg2.connect(**get_settings().psycopg2_conn_params)
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
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
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


@celery_app.task(bind=True, time_limit=1800, soft_time_limit=1750)
def sync_normquery_data(self, shop_id: int, api_key: str):
    """
    Sync normquery (search cluster) stats and bid snapshots for UWB campaigns.

    Source: WB API normquery endpoints (via WBNormqueryService)
    Target: ClickHouse fact_normquery_stats_daily + log_normquery_bids

    Only processes campaigns with bid_type=manual AND payment_type=cpm.

    Steps:
    1. Get active UWB campaign_ids from dim_advert_campaigns (ClickHouse)
    2. For each campaign: get nm_ids from fact_advert_stats_v3
    3. Fetch normquery daily stats (v1 API) for last 3 days
    4. Fetch current bids per cluster
    5. Fetch bid recommendations
    6. Insert stats into fact_normquery_stats_daily
    7. Insert bid snapshots into log_normquery_bids

    Queue: SYNC.
    """
    import asyncio
    import os
    import logging
    from datetime import date, timedelta

    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    from app.config import get_settings
    from app.core.clickhouse import get_clickhouse_client
    from app.services.wb_normquery_service import WBNormqueryService

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        ch = get_clickhouse_client()

        # 1. Get active CPM campaigns (manual + unified)
        try:
            campaign_rows = ch.query(
                """
                SELECT advert_id,
                       argMax(status, updated_at) as status
                FROM mms_analytics.dim_advert_campaigns
                WHERE shop_id = {sid:UInt32}
                GROUP BY advert_id
                HAVING argMax(bid_type, updated_at) IN ('manual', 'unified')
                   AND argMax(payment_type, updated_at) = 'cpm'
                   AND status IN (9, 11)
                """,
                parameters={"sid": shop_id}
            ).result_rows
        except Exception as e:
            logger.error(f"[normquery-sync] shop={shop_id} failed to get campaigns: {e}")
            await engine.dispose()
            return {"shop_id": shop_id, "status": "error", "error": str(e)}

        if not campaign_rows:
            logger.info(f"[normquery-sync] shop={shop_id} no UWB campaigns found")
            await engine.dispose()
            return {"shop_id": shop_id, "status": "no_campaigns"}

        campaign_ids = [int(r[0]) for r in campaign_rows]
        logger.info(f"[normquery-sync] shop={shop_id} found {len(campaign_ids)} UWB campaigns")

        # Date range: last 7 days (for daily breakdown, supports 7d period filter)
        end_dt = date.today()
        start_dt = end_dt - timedelta(days=7)
        date_from = start_dt.isoformat()
        date_to = end_dt.isoformat()

        total_stats_rows = 0
        total_bid_rows = 0
        errors = []

        async with async_session() as db:
            svc = WBNormqueryService(db=db, shop_id=shop_id, api_key=api_key)

            for cid in campaign_ids:
                try:
                    # 2. Get nm_ids for this campaign
                    nm_rows = ch.query(
                        "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL "
                        "WHERE advert_id = {cid:UInt64} AND shop_id = {sid:UInt32} "
                        "AND (views > 0 OR clicks > 0 OR spend > 0)",
                        parameters={"cid": cid, "sid": shop_id}
                    ).result_rows

                    if not nm_rows:
                        continue

                    nm_ids = [int(r[0]) for r in nm_rows]
                    items = [{"advert_id": cid, "nm_id": nm} for nm in nm_ids]

                    # 3. Fetch stats per-day using v0 API (v1 API format is unreliable)
                    # v0 returns aggregated stats for a period — call it per-day for daily granularity
                    stats_rows = []
                    for day_offset in range(7):
                        day_dt = end_dt - timedelta(days=day_offset)
                        day_str = day_dt.isoformat()
                        try:
                            day_stats = await svc.get_normquery_stats(
                                items=items,
                                date_from=day_str,
                                date_to=day_str,
                            )
                            if isinstance(day_stats, dict):
                                raw_stats = day_stats.get("stats", [])
                                # Format 1: [{nm_id, stats: [{norm_query, ...}]}]
                                # Format 2: [{norm_query, views, clicks, ...}] (flat)
                                flat_items = []
                                for stat_item in raw_stats:
                                    if not isinstance(stat_item, dict):
                                        continue
                                    if "norm_query" in stat_item:
                                        # Flat format — stat_item IS the cluster stats
                                        flat_items.append((nm_ids[0] if nm_ids else 0, stat_item))
                                    elif "stats" in stat_item:
                                        # Nested format — stat_item wraps nm_id + stats
                                        nm_val = stat_item.get("nm_id", nm_ids[0] if nm_ids else 0)
                                        for s in stat_item.get("stats", []):
                                            flat_items.append((nm_val, s))

                                for nm_id_val, s in flat_items:
                                    nq = s.get("norm_query", "")
                                    if not nq:
                                        continue
                                    stats_rows.append({
                                        "dt": day_dt,
                                        "shop_id": shop_id,
                                        "advert_id": cid,
                                        "nm_id": nm_id_val,
                                        "norm_query": nq,
                                        "views": int(s.get("views", 0)),
                                        "clicks": int(s.get("clicks", 0)),
                                        "atbs": int(s.get("atbs", 0)),
                                        "orders": int(s.get("orders", 0)),
                                        "avg_pos": float(s.get("avg_pos", 0)),
                                        "cpc": int(float(s.get("cpc", 0))),
                                        "cpm": int(float(s.get("cpm", 0))),
                                        "ctr": float(s.get("ctr", 0)),
                                        "bid_kopecks": 0,
                                        "spend": float(s.get("spend", 0)),
                                        "shks": int(s.get("shks", 0)),
                                        "cpc_rub": float(s.get("cpc", 0)),
                                        "cpm_rub": float(s.get("cpm", 0)),
                                    })
                            elif isinstance(day_stats, list):
                                # Direct list of stats
                                for s in day_stats:
                                    if isinstance(s, dict) and s.get("norm_query"):
                                        stats_rows.append({
                                            "dt": day_dt,
                                            "shop_id": shop_id,
                                            "advert_id": cid,
                                            "nm_id": nm_ids[0] if nm_ids else 0,
                                            "norm_query": s["norm_query"],
                                            "views": int(s.get("views", 0)),
                                            "clicks": int(s.get("clicks", 0)),
                                            "atbs": int(s.get("atbs", 0)),
                                            "orders": int(s.get("orders", 0)),
                                            "avg_pos": float(s.get("avg_pos", 0)),
                                            "cpc": int(float(s.get("cpc", 0))),
                                            "cpm": int(float(s.get("cpm", 0))),
                                            "ctr": float(s.get("ctr", 0)),
                                            "bid_kopecks": 0,
                                            "spend": float(s.get("spend", 0)),
                                            "shks": int(s.get("shks", 0)),
                                            "cpc_rub": float(s.get("cpc", 0)),
                                            "cpm_rub": float(s.get("cpm", 0)),
                                        })
                            await asyncio.sleep(0.5)  # rate limit between daily calls
                        except Exception as e:
                            logger.warning(f"[normquery-sync] day stats failed for {cid} {day_str}: {e}")

                    # 4. Fetch current bids
                    bids_data = await svc.get_normquery_bids(items=items)

                    # Build bid map and enrich stats rows
                    # NOTE: WB API /normquery/get-bids returns "bid" in RUBLES (not kopecks!)
                    # We convert to kopecks (*100) for consistent storage in bid_kopecks column
                    bid_map = {}
                    if isinstance(bids_data, dict):
                        for bid in bids_data.get("bids", []):
                            nq = bid.get("norm_query", "")
                            bid_rub = int(bid.get("bid", 0))
                            bid_map[nq] = bid_rub * 100  # rubles → kopecks

                    # Enrich stats rows with current bids (kopecks)
                    for row in stats_rows:
                        row["bid_kopecks"] = bid_map.get(row["norm_query"], 0)

                    # 5. Fetch bid recommendations (for first nm_id)
                    rec_map = {}
                    base_competitive = 0
                    base_leaders = 0
                    try:
                        rec_data = await svc.get_bid_recommendations(cid, nm_ids[0])
                        if isinstance(rec_data, dict):
                            base = rec_data.get("base", {})
                            base_competitive = base.get("competitiveBid", {}).get("bidKopecks", 0)
                            base_leaders = base.get("leadersBid", {}).get("bidKopecks", 0)
                            for nq_rec in rec_data.get("normQueries", []):
                                nq_name = nq_rec.get("normQuery", "")
                                rec_map[nq_name] = {
                                    "reach_max": nq_rec.get("reachMax", {}).get("bidKopecks", 0),
                                    "reach_med": nq_rec.get("reachMedium", {}).get("bidKopecks", 0),
                                    "reach_min": nq_rec.get("reachMin", {}).get("bidKopecks", 0),
                                }
                    except Exception:
                        pass  # recommendations are optional

                    # 6. Insert stats into ClickHouse
                    if stats_rows:
                        ch.insert(
                            "mms_analytics.fact_normquery_stats_daily",
                            [list(r.values()) for r in stats_rows],
                            column_names=list(stats_rows[0].keys()),
                        )
                        total_stats_rows += len(stats_rows)

                    # 7. Insert bid snapshots into log_normquery_bids
                    bid_rows = []
                    if bid_map:
                        # Manual campaigns: build from per-cluster bids
                        for nq, bid_k in bid_map.items():
                            rec = rec_map.get(nq, {})
                            bid_rows.append({
                                "shop_id": shop_id,
                                "advert_id": cid,
                                "nm_id": nm_ids[0],
                                "norm_query": nq,
                                "bid_kopecks": bid_k,  # already in kopecks (converted above)
                                "reach_max_bid": rec.get("reach_max", 0),
                                "reach_med_bid": rec.get("reach_med", 0),
                                "reach_min_bid": rec.get("reach_min", 0),
                                "competitive_bid": base_competitive,
                                "leaders_bid": base_leaders,
                            })
                    elif stats_rows and (base_competitive or base_leaders or rec_map):
                        # Unified campaigns: no per-cluster bids, but we have
                        # recommendations — build from stats clusters
                        seen_nq = set()
                        for sr in stats_rows:
                            nq = sr["norm_query"]
                            if nq in seen_nq:
                                continue
                            seen_nq.add(nq)
                            rec = rec_map.get(nq, {})
                            bid_rows.append({
                                "shop_id": shop_id,
                                "advert_id": cid,
                                "nm_id": nm_ids[0],
                                "norm_query": nq,
                                "bid_kopecks": 0,  # unified — no per-cluster bid
                                "reach_max_bid": rec.get("reach_max", 0),
                                "reach_med_bid": rec.get("reach_med", 0),
                                "reach_min_bid": rec.get("reach_min", 0),
                                "competitive_bid": base_competitive,
                                "leaders_bid": base_leaders,
                            })

                    if bid_rows:
                        ch.insert(
                            "mms_analytics.log_normquery_bids",
                            [list(r.values()) for r in bid_rows],
                            column_names=list(bid_rows[0].keys()),
                        )
                        total_bid_rows += len(bid_rows)

                    logger.info(
                        f"[normquery-sync] shop={shop_id} campaign={cid}: "
                        f"stats={len(stats_rows)} bids={len(bid_rows)}"
                    )

                    # Pause between campaigns to avoid rate limiting
                    await asyncio.sleep(2)

                except Exception as e:
                    errors.append(f"campaign {cid}: {e}")
                    logger.error(f"[normquery-sync] shop={shop_id} campaign={cid} error: {e}")
                    continue

        await engine.dispose()

        result = {
            "shop_id": shop_id,
            "status": "completed" if not errors else "completed_with_errors",
            "campaigns_processed": len(campaign_ids),
            "stats_rows_inserted": total_stats_rows,
            "bid_rows_inserted": total_bid_rows,
            "errors": errors,
        }
        logger.info(f"[normquery-sync] shop={shop_id} done: {result}")
        return result

    try:
        return asyncio.run(run_sync())
    except Exception as exc:
        self.retry(exc=exc, countdown=60, max_retries=2)


# ===================
# NORMQUERY BACKFILL (one-time, for WB shop onboarding)
# Collects 30 days of normquery stats + current bid snapshot
# ===================

@celery_app.task(bind=True, time_limit=1800, soft_time_limit=1750)
def backfill_normquery_data(self, shop_id: int, api_key: str, days_back: int = 30):
    """
    One-time backfill of normquery (search cluster) stats for WB shop onboarding.

    Identical to sync_normquery_data but collects data for `days_back` days
    instead of 7. Runs once during load_historical_data WB pipeline.

    Steps:
    1. Get active UWB campaign_ids from dim_advert_campaigns (ClickHouse)
    2. For each campaign: get nm_ids from fact_advert_stats_v3
    3. Fetch normquery daily stats for last `days_back` days
    4. Fetch current bids per cluster (single snapshot)
    5. Fetch bid recommendations
    6. Insert stats into fact_normquery_stats_daily
    7. Insert bid snapshots into log_normquery_bids

    Queue: SYNC (via backfill pipeline).
    """
    import asyncio
    import os
    import logging
    from datetime import date, timedelta

    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker

    from app.config import get_settings
    from app.core.clickhouse import get_clickhouse_client
    from app.services.wb_normquery_service import WBNormqueryService

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_backfill():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        ch = get_clickhouse_client()

        # 1. Get active CPM campaigns (manual + unified)
        try:
            campaign_rows = ch.query(
                """
                SELECT advert_id,
                       argMax(status, updated_at) as status
                FROM mms_analytics.dim_advert_campaigns
                WHERE shop_id = {sid:UInt32}
                GROUP BY advert_id
                HAVING argMax(bid_type, updated_at) IN ('manual', 'unified')
                   AND argMax(payment_type, updated_at) = 'cpm'
                   AND status IN (9, 11)
                """,
                parameters={"sid": shop_id}
            ).result_rows
        except Exception as e:
            logger.error(f"[normquery-backfill] shop={shop_id} failed to get campaigns: {e}")
            await engine.dispose()
            return {"shop_id": shop_id, "status": "error", "error": str(e)}

        if not campaign_rows:
            logger.info(f"[normquery-backfill] shop={shop_id} no UWB campaigns found")
            await engine.dispose()
            return {"shop_id": shop_id, "status": "no_campaigns"}

        campaign_ids = [int(r[0]) for r in campaign_rows]
        logger.info(
            f"[normquery-backfill] shop={shop_id} found {len(campaign_ids)} UWB campaigns, "
            f"backfilling {days_back} days"
        )

        end_dt = date.today()
        total_stats_rows = 0
        total_bid_rows = 0
        errors = []

        async with async_session() as db:
            svc = WBNormqueryService(db=db, shop_id=shop_id, api_key=api_key)

            for ci, cid in enumerate(campaign_ids):
                try:
                    self.update_state(state='PROGRESS', meta={
                        'status': f'Кампания {ci+1}/{len(campaign_ids)} (ID {cid})',
                        'progress': f'{(ci+1)*100//len(campaign_ids)}%',
                    })

                    # 2. Get nm_ids for this campaign
                    nm_rows = ch.query(
                        "SELECT DISTINCT nm_id FROM mms_analytics.fact_advert_stats_v3 FINAL "
                        "WHERE advert_id = {cid:UInt64} AND shop_id = {sid:UInt32} "
                        "AND (views > 0 OR clicks > 0 OR spend > 0)",
                        parameters={"cid": cid, "sid": shop_id}
                    ).result_rows

                    if not nm_rows:
                        continue

                    nm_ids = [int(r[0]) for r in nm_rows]
                    items = [{"advert_id": cid, "nm_id": nm} for nm in nm_ids]

                    # 3. Fetch stats per-day for days_back days
                    stats_rows = []
                    for day_offset in range(days_back):
                        day_dt = end_dt - timedelta(days=day_offset)
                        day_str = day_dt.isoformat()
                        try:
                            day_stats = await svc.get_normquery_stats(
                                items=items,
                                date_from=day_str,
                                date_to=day_str,
                            )
                            if isinstance(day_stats, dict):
                                raw_stats = day_stats.get("stats", [])
                                flat_items = []
                                for stat_item in raw_stats:
                                    if not isinstance(stat_item, dict):
                                        continue
                                    if "norm_query" in stat_item:
                                        flat_items.append((nm_ids[0] if nm_ids else 0, stat_item))
                                    elif "stats" in stat_item:
                                        nm_val = stat_item.get("nm_id", nm_ids[0] if nm_ids else 0)
                                        for s in stat_item.get("stats", []):
                                            flat_items.append((nm_val, s))

                                for nm_id_val, s in flat_items:
                                    nq = s.get("norm_query", "")
                                    if not nq:
                                        continue
                                    stats_rows.append({
                                        "dt": day_dt,
                                        "shop_id": shop_id,
                                        "advert_id": cid,
                                        "nm_id": nm_id_val,
                                        "norm_query": nq,
                                        "views": int(s.get("views", 0)),
                                        "clicks": int(s.get("clicks", 0)),
                                        "atbs": int(s.get("atbs", 0)),
                                        "orders": int(s.get("orders", 0)),
                                        "avg_pos": float(s.get("avg_pos", 0)),
                                        "cpc": int(float(s.get("cpc", 0))),
                                        "cpm": int(float(s.get("cpm", 0))),
                                        "ctr": float(s.get("ctr", 0)),
                                        "bid_kopecks": 0,
                                        "spend": float(s.get("spend", 0)),
                                        "shks": int(s.get("shks", 0)),
                                        "cpc_rub": float(s.get("cpc", 0)),
                                        "cpm_rub": float(s.get("cpm", 0)),
                                    })
                            elif isinstance(day_stats, list):
                                for s in day_stats:
                                    if isinstance(s, dict) and s.get("norm_query"):
                                        stats_rows.append({
                                            "dt": day_dt,
                                            "shop_id": shop_id,
                                            "advert_id": cid,
                                            "nm_id": nm_ids[0] if nm_ids else 0,
                                            "norm_query": s["norm_query"],
                                            "views": int(s.get("views", 0)),
                                            "clicks": int(s.get("clicks", 0)),
                                            "atbs": int(s.get("atbs", 0)),
                                            "orders": int(s.get("orders", 0)),
                                            "avg_pos": float(s.get("avg_pos", 0)),
                                            "cpc": int(float(s.get("cpc", 0))),
                                            "cpm": int(float(s.get("cpm", 0))),
                                            "ctr": float(s.get("ctr", 0)),
                                            "bid_kopecks": 0,
                                            "spend": float(s.get("spend", 0)),
                                            "shks": int(s.get("shks", 0)),
                                            "cpc_rub": float(s.get("cpc", 0)),
                                            "cpm_rub": float(s.get("cpm", 0)),
                                        })
                            await asyncio.sleep(0.5)  # rate limit between daily calls
                        except Exception as e:
                            logger.warning(f"[normquery-backfill] day stats failed for {cid} {day_str}: {e}")

                    # 4. Fetch current bids
                    bids_data = await svc.get_normquery_bids(items=items)
                    # NOTE: WB API /normquery/get-bids returns "bid" in RUBLES (not kopecks!)
                    # Convert to kopecks (*100) for consistent storage
                    bid_map = {}
                    if isinstance(bids_data, dict):
                        for bid in bids_data.get("bids", []):
                            nq = bid.get("norm_query", "")
                            bid_rub = int(bid.get("bid", 0))
                            bid_map[nq] = bid_rub * 100  # rubles → kopecks

                    # Enrich stats rows with current bids (kopecks)
                    for row in stats_rows:
                        row["bid_kopecks"] = bid_map.get(row["norm_query"], 0)

                    # 5. Fetch bid recommendations
                    rec_map = {}
                    base_competitive = 0
                    base_leaders = 0
                    try:
                        rec_data = await svc.get_bid_recommendations(cid, nm_ids[0])
                        if isinstance(rec_data, dict):
                            base = rec_data.get("base", {})
                            base_competitive = base.get("competitiveBid", {}).get("bidKopecks", 0)
                            base_leaders = base.get("leadersBid", {}).get("bidKopecks", 0)
                            for nq_rec in rec_data.get("normQueries", []):
                                nq_name = nq_rec.get("normQuery", "")
                                rec_map[nq_name] = {
                                    "reach_max": nq_rec.get("reachMax", {}).get("bidKopecks", 0),
                                    "reach_med": nq_rec.get("reachMedium", {}).get("bidKopecks", 0),
                                    "reach_min": nq_rec.get("reachMin", {}).get("bidKopecks", 0),
                                }
                    except Exception:
                        pass

                    # 6. Insert stats into ClickHouse
                    if stats_rows:
                        ch.insert(
                            "mms_analytics.fact_normquery_stats_daily",
                            [list(r.values()) for r in stats_rows],
                            column_names=list(stats_rows[0].keys()),
                        )
                        total_stats_rows += len(stats_rows)

                    # 7. Insert bid snapshots
                    bid_rows = []
                    if bid_map:
                        for nq, bid_k in bid_map.items():
                            rec = rec_map.get(nq, {})
                            bid_rows.append({
                                "shop_id": shop_id,
                                "advert_id": cid,
                                "nm_id": nm_ids[0],
                                "norm_query": nq,
                                "bid_kopecks": bid_k,  # already in kopecks (converted above)
                                "reach_max_bid": rec.get("reach_max", 0),
                                "reach_med_bid": rec.get("reach_med", 0),
                                "reach_min_bid": rec.get("reach_min", 0),
                                "competitive_bid": base_competitive,
                                "leaders_bid": base_leaders,
                            })
                    elif stats_rows and (base_competitive or base_leaders or rec_map):
                        seen_nq = set()
                        for sr in stats_rows:
                            nq = sr["norm_query"]
                            if nq in seen_nq:
                                continue
                            seen_nq.add(nq)
                            rec = rec_map.get(nq, {})
                            bid_rows.append({
                                "shop_id": shop_id,
                                "advert_id": cid,
                                "nm_id": nm_ids[0],
                                "norm_query": nq,
                                "bid_kopecks": 0,
                                "reach_max_bid": rec.get("reach_max", 0),
                                "reach_med_bid": rec.get("reach_med", 0),
                                "reach_min_bid": rec.get("reach_min", 0),
                                "competitive_bid": base_competitive,
                                "leaders_bid": base_leaders,
                            })

                    if bid_rows:
                        ch.insert(
                            "mms_analytics.log_normquery_bids",
                            [list(r.values()) for r in bid_rows],
                            column_names=list(bid_rows[0].keys()),
                        )
                        total_bid_rows += len(bid_rows)

                    logger.info(
                        f"[normquery-backfill] shop={shop_id} campaign={cid}: "
                        f"stats={len(stats_rows)} bids={len(bid_rows)}"
                    )

                    # Pause between campaigns to avoid rate limiting
                    await asyncio.sleep(2)

                except Exception as e:
                    errors.append(f"campaign {cid}: {e}")
                    logger.error(f"[normquery-backfill] shop={shop_id} campaign={cid} error: {e}")
                    continue

        await engine.dispose()

        result = {
            "shop_id": shop_id,
            "status": "completed" if not errors else "completed_with_errors",
            "campaigns_processed": len(campaign_ids),
            "days_back": days_back,
            "stats_rows_inserted": total_stats_rows,
            "bid_rows_inserted": total_bid_rows,
            "errors": errors,
        }
        logger.info(f"[normquery-backfill] shop={shop_id} done: {result}")
        return result

    try:
        return asyncio.run(run_backfill())
    except Exception as exc:
        self.retry(exc=exc, countdown=60, max_retries=2)
