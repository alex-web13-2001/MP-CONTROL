"""
WB Sync tasks — Data synchronization for Wildberries marketplace.

Covers: orders, finance, commercial data (prices/stocks),
warehouses, product content, tariffs, paid storage, sales funnel.
"""

from celery_app.celery import celery_app


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
# BUDGET SYNC (lightweight, every 15 min)
# =============================================


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

            # ===== Step 3b: Fetch FBS stocks (seller warehouses) =====
            self.update_state(state="PROGRESS", meta={"status": "Fetching FBS stocks..."})
            try:
                fbs_stocks = await stocks_service.fetch_fbs_stocks(nm_ids)
                if fbs_stocks:
                    stocks_data.extend(fbs_stocks)
                    stats["fbs_stocks_fetched"] = len(fbs_stocks)
                    logger.info(f"FBS stocks: {len(fbs_stocks)} items added")
            except Exception as e:
                logger.error(f"FBS stocks fetch error (non-fatal): {e}")
                stats["errors"].append(f"FBS: {e}")

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
# WB TARIFFS TASKS
# Fetch acceptance coefficients & storage/delivery tariffs
# ====================

@celery_app.task(bind=True, queue="sync", time_limit=120, soft_time_limit=110,
                 autoretry_for=(Exception,), retry_kwargs={"max_retries": 2, "countdown": 60})
def sync_wb_tariffs(
    self,
    shop_id: int,
    api_key: str,
):
    """
    Sync WB warehouse tariffs: acceptance coefficients + storage/delivery tariffs.

    Source: GET /api/tariffs/v1/acceptance/coefficients
    Target: ClickHouse fact_wb_acceptance_tariffs

    Runs daily via sync_all_daily coordinator.
    Queue: SYNC.
    """
    import asyncio
    import logging
    from datetime import datetime
    from app.core.clickhouse import get_clickhouse_client
    from app.services.wb_tariffs_service import WBTariffsService

    logger = logging.getLogger(__name__)
    logger.info("sync_wb_tariffs: shop=%s starting", shop_id)

    async def run_sync():
        service = WBTariffsService(api_key=api_key)

        # 1. Fetch from WB API
        items = await service.fetch_acceptance_coefficients()
        if not items:
            logger.warning("sync_wb_tariffs: shop=%s no data from API", shop_id)
            return {"rows_inserted": 0}

        # 2. Prepare rows for ClickHouse
        now = datetime.utcnow()
        rows = service.prepare_ch_rows(items, fetched_at=now)

        if not rows:
            logger.warning("sync_wb_tariffs: shop=%s no valid rows", shop_id)
            return {"rows_inserted": 0}

        # 3. Insert into ClickHouse
        ch = get_clickhouse_client()
        ch.insert(
            "mms_analytics.fact_wb_acceptance_tariffs",
            rows,
            column_names=[
                "dt", "warehouse_id", "warehouse_name", "box_type_id",
                "coefficient", "allow_unload", "is_sorting_center",
                "storage_coef", "storage_base_liter", "storage_additional_liter",
                "delivery_coef", "delivery_base_liter", "delivery_additional_liter",
                "updated_at",
            ],
        )

        logger.info("sync_wb_tariffs: shop=%s inserted %d rows", shop_id, len(rows))
        return {"rows_inserted": len(rows)}

    return asyncio.run(run_sync())


# ====================
# WB PAID STORAGE TASKS
# Fetch actual paid storage costs per SKU from WB report API
# ====================

@celery_app.task(bind=True, queue="sync", time_limit=300, soft_time_limit=280,
                 autoretry_for=(Exception,), retry_kwargs={"max_retries": 2, "countdown": 120})
def sync_wb_paid_storage(
    self,
    shop_id: int,
    api_key: str,
    days_back: int = 7,
):
    """
    Sync WB paid storage report: actual per-SKU storage costs with discounts.

    Source: GET /api/v1/paid_storage (async report: create → poll → download)
    Target: ClickHouse fact_wb_paid_storage

    Runs daily via sync_all_daily coordinator.
    Fetches last 7 days by default (with 7-day chunk splitting for API limit).
    Queue: SYNC.
    """
    import asyncio
    import logging
    from datetime import date, timedelta
    from app.core.clickhouse import get_clickhouse_client
    from app.services.wb_paid_storage_service import WBPaidStorageService

    logger = logging.getLogger(__name__)
    logger.info("sync_wb_paid_storage: shop=%s days_back=%d starting", shop_id, days_back)

    async def run_sync():
        service = WBPaidStorageService(api_key=api_key)

        date_to = date.today() - timedelta(days=1)  # yesterday (today may not be ready)
        date_from = date_to - timedelta(days=days_back - 1)

        self.update_state(state="PROGRESS", meta={
            "status": f"Fetching paid storage {date_from} — {date_to}...",
            "shop_id": shop_id,
        })

        def on_progress(chunk, total, items):
            self.update_state(state="PROGRESS", meta={
                "status": f"Chunk {chunk}/{total}: {items} items...",
                "shop_id": shop_id,
            })

        items = await service.fetch_date_range(date_from, date_to, on_progress=on_progress)
        if not items:
            logger.warning("sync_wb_paid_storage: shop=%s no data from API", shop_id)
            return {"shop_id": shop_id, "rows_inserted": 0, "status": "no_data"}

        # Prepare and insert ClickHouse rows
        rows = service.prepare_ch_rows(items, shop_id)
        if not rows:
            return {"shop_id": shop_id, "rows_inserted": 0, "status": "no_valid_rows"}

        ch = get_clickhouse_client()
        column_names = [
            "dt", "shop_id", "vendor_code", "nm_id",
            "warehouse", "office_id", "warehouse_coef", "log_warehouse_coef",
            "volume_liters", "calc_type", "warehouse_price",
            "barcodes_count", "pallet_place_code", "pallet_count",
            "original_date", "loyalty_discount",
            "tariff_fix_date", "tariff_lower_date",
            "gi_id", "barcode", "brand", "subject", "updated_at",
        ]
        ch.insert("mms_analytics.fact_wb_paid_storage", rows, column_names=column_names)
        ch.close()

        logger.info("sync_wb_paid_storage: shop=%s inserted %d rows (%s — %s)",
                     shop_id, len(rows), date_from, date_to)
        return {
            "shop_id": shop_id,
            "rows_inserted": len(rows),
            "api_items": len(items),
            "period": f"{date_from} — {date_to}",
            "status": "completed",
        }

    return asyncio.run(run_sync())


@celery_app.task(bind=True, queue="sync", time_limit=1800, soft_time_limit=1700)
def backfill_wb_paid_storage(
    self,
    shop_id: int,
    api_key: str,
    months: int = 3,
):
    """
    One-time backfill: load paid storage history for last N months.

    Splits into 7-day chunks, sequentially creates → polls → downloads.
    Can run up to 30 minutes due to polling delays.
    Queue: SYNC.
    """
    import asyncio
    import logging
    from datetime import date, timedelta
    from app.core.clickhouse import get_clickhouse_client
    from app.services.wb_paid_storage_service import WBPaidStorageService

    logger = logging.getLogger(__name__)
    logger.info("backfill_wb_paid_storage: shop=%s months=%d starting", shop_id, months)

    async def run_backfill():
        service = WBPaidStorageService(api_key=api_key)

        date_to = date.today() - timedelta(days=1)
        date_from = date_to - timedelta(days=months * 30)

        self.update_state(state="PROGRESS", meta={
            "status": f"Backfilling paid storage {date_from} — {date_to}...",
            "shop_id": shop_id,
            "months": months,
        })

        def on_progress(chunk, total, items):
            self.update_state(state="PROGRESS", meta={
                "status": f"Chunk {chunk}/{total}: {items} items so far...",
                "shop_id": shop_id,
            })

        items = await service.fetch_date_range(date_from, date_to, on_progress=on_progress)
        if not items:
            logger.warning("backfill_wb_paid_storage: shop=%s no data", shop_id)
            return {"shop_id": shop_id, "rows_inserted": 0, "status": "no_data"}

        rows = service.prepare_ch_rows(items, shop_id)
        if not rows:
            return {"shop_id": shop_id, "rows_inserted": 0, "status": "no_valid_rows"}

        ch = get_clickhouse_client()
        column_names = [
            "dt", "shop_id", "vendor_code", "nm_id",
            "warehouse", "office_id", "warehouse_coef", "log_warehouse_coef",
            "volume_liters", "calc_type", "warehouse_price",
            "barcodes_count", "pallet_place_code", "pallet_count",
            "original_date", "loyalty_discount",
            "tariff_fix_date", "tariff_lower_date",
            "gi_id", "barcode", "brand", "subject", "updated_at",
        ]
        ch.insert("mms_analytics.fact_wb_paid_storage", rows, column_names=column_names)
        ch.close()

        logger.info("backfill_wb_paid_storage: shop=%s inserted %d rows (%s — %s)",
                     shop_id, len(rows), date_from, date_to)
        return {
            "shop_id": shop_id,
            "rows_inserted": len(rows),
            "api_items": len(items),
            "period": f"{date_from} — {date_to}",
            "months": months,
            "status": "completed",
        }

    try:
        return asyncio.run(run_backfill())
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


