"""
Ozon Advertising tasks — Campaign management, bids, and ad statistics for Ozon.

Covers: bid monitoring, campaign sync, ad statistics (periodic + backfill).
Uses Ozon Performance API (separate OAuth2 auth from Seller API).
"""

from celery_app.celery import celery_app


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
                if not campaigns:
                    campaigns = []
                running_campaigns = [
                    c for c in campaigns
                    if c.get("state") == "CAMPAIGN_STATE_RUNNING"
                ]

                # 2. Get products per campaign (for bid/item tracking)
                products_by_campaign = {}
                all_bids = []
                api_errors = 0

                for camp in running_campaigns:
                    campaign_id = camp.get("id")
                    if not campaign_id:
                        continue

                    products = await service.get_campaign_products(campaign_id)

                    # None = API error → skip this campaign entirely
                    # to prevent false ITEM_REMOVE events
                    if products is None:
                        api_errors += 1
                        logger.warning(
                            "Skipping campaign %s for event detection (API error)",
                            campaign_id,
                        )
                        continue

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
                    "Ozon: fetched %d bids across %d campaigns "
                    "(%d API errors) for shop %d",
                    len(all_bids), len(running_campaigns),
                    api_errors, shop_id,
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
                    "api_errors": api_errors,
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
                "api_errors": api_errors,
            }

        finally:
            await redis_client.close()
            await engine.dispose()

    return asyncio.run(run_monitor())



@celery_app.task(bind=True, time_limit=600, soft_time_limit=540)
def sync_ozon_campaigns_task(
    self,
    shop_id: int,
    perf_client_id: str,
    perf_client_secret: str,
):
    """
    Sync Ozon campaigns (titles, states, types) and their products (SKUs, bids).
    Saves to dim_ozon_campaigns and dim_ozon_campaign_products.
    """
    import asyncio
    import logging
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_ads_service import OzonAdsService
    from app.services.ozon_campaigns_loader import OzonCampaignsLoader

    logger = logging.getLogger(__name__)
    settings = get_settings()

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with async_session() as db:
            ads_service = OzonAdsService(
                db=db,
                shop_id=shop_id,
                perf_client_id=perf_client_id,
                perf_client_secret=perf_client_secret,
            )
            loader = OzonCampaignsLoader(db=db, shop_id=shop_id, ozon_ads_service=ads_service)
            
            try:
                await loader.sync_all_campaigns()
            except Exception as e:
                logger.error(f"Error in sync_ozon_campaigns_task for shop {shop_id}: {e}")
                
        await engine.dispose()

    return asyncio.run(run_sync())



@celery_app.task(bind=True, time_limit=1800, soft_time_limit=1740)
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

                # 3.5. Fetch Phrases pipeline (SKU + SEARCH_PROMO campaigns)
                search_campaign_ids = [
                    c["id"] for c in campaigns
                    if c.get("id") and str(c.get("advObjectType", "")).upper() in ("SEARCH_PROMO", "SKU")
                ]
                phrases_rows = []
                if search_campaign_ids:
                    self.update_state(state='PROGRESS', meta={
                        'status': f'Ordering PHRASES report {date_from} → {date_to} for {len(search_campaign_ids)} campaigns...',
                    })
                    phrases_rows = await service.fetch_phrases_statistics(
                        shop_id=shop_id,
                        campaign_ids=search_campaign_ids,
                        date_from=date_from,
                        date_to=date_to,
                    )
                    logger.info(f"Ozon: parsed {len(phrases_rows)} phrase rows")

            # 4. Insert into ClickHouse
            inserted_stats = 0
            inserted_phrases = 0
            
            if True:
                ch_host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
                ch_port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))

                with OzonBidsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                    if all_rows:
                        inserted_stats = loader.insert_stats(all_rows)
                    if phrases_rows:
                        inserted_phrases = loader.insert_phrases(phrases_rows)

            self.update_state(state='PROGRESS', meta={
                'status': f'Done: {inserted_stats} stats, {inserted_phrases} phrases inserted',
            })

            return {
                "shop_id": shop_id,
                "campaigns": len(campaign_ids),
                "phrases_campaigns": len(search_campaign_ids),
                "date_from": date_from,
                "date_to": date_to,
                "rows_inserted": inserted_stats,
                "phrases_inserted": inserted_phrases,
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
    chunk_days: int = 60,  # API max: 62 days per report
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

                # Phrases: only SEARCH_PROMO / SKU campaigns have phrase data
                search_campaign_ids = [
                    c["id"] for c in campaigns
                    if c.get("id") and str(c.get("advObjectType", "")).upper() in ("SEARCH_PROMO", "SKU")
                ]
                logger.info(
                    f"Ozon backfill: {len(search_campaign_ids)}/{len(campaign_ids)} "
                    f"campaigns eligible for phrases backfill"
                )

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
                total_phrases = 0
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

                            # 3.5 Phrases backfill (same chunk dates)
                            if search_campaign_ids:
                                try:
                                    phrases_rows = await service.fetch_phrases_statistics(
                                        shop_id=shop_id,
                                        campaign_ids=search_campaign_ids,
                                        date_from=cf.strftime("%Y-%m-%d"),
                                        date_to=ct.strftime("%Y-%m-%d"),
                                    )
                                    if phrases_rows:
                                        inserted_p = loader.insert_phrases(phrases_rows)
                                        total_phrases += inserted_p
                                        logger.info(
                                            f"Phrases chunk {cf}→{ct}: {inserted_p} rows"
                                        )
                                except Exception as pe:
                                    logger.warning(f"Phrases backfill chunk {cf}→{ct} failed: {pe}")

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
                "phrases_campaigns": len(search_campaign_ids),
                "chunks": len(chunks),
                "total_rows": total_rows,
                "total_phrases": total_phrases,
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


