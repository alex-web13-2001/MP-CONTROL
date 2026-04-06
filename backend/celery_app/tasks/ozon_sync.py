"""
Ozon Sync tasks — Data synchronization for Ozon marketplace.

Covers: products, orders, finance, funnel, returns, warehouse stocks,
prices/commissions, seller ratings, content, inventory, turnover,
and placement (storage) costs.
"""

from celery_app.celery import celery_app


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
        engine = create_async_engine(settings.database_url)
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

            # 3. Upsert into PostgreSQL (returns count + change events)
            self.update_state(state='PROGRESS', meta={'status': 'Upserting into dim_ozon_products...'})
            from app.config import get_settings
            conn_params = get_settings().psycopg2_conn_params
            count, events = upsert_ozon_products(conn_params, shop_id, products_info)

            if events:
                logger.info(f"Detected {len(events)} product change events (photo/stock/content)")
                # Save events to event_log
                import psycopg2 as _pg2
                import json as _json
                _conn = _pg2.connect(**conn_params)
                _cur = _conn.cursor()
                for ev in events:
                    _cur.execute("""
                        INSERT INTO event_log
                            (shop_id, advert_id, nm_id, event_type, old_value, new_value, event_metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        ev.get("shop_id"),
                        None,
                        ev.get("product_id"),
                        ev.get("event_type"),
                        ev.get("old_value"),
                        ev.get("new_value"),
                        _json.dumps({
                            "field": ev.get("field"),
                            "offer_id": ev.get("offer_id", ""),
                            "platform": "ozon",
                        }),
                    ))
                _conn.commit()
                _cur.close()
                _conn.close()
                logger.info(f"Saved {len(events)} events to event_log")

            # 4. Fetch real seller prices from /v5/product/info/prices
            #    (marketing_seller_price = actual price after Ozon discounts)
            self.update_state(state='PROGRESS', meta={'status': 'Fetching real prices (v5)...'})
            async with async_session_factory() as db:
                service = OzonProductsService(db=db, shop_id=shop_id, api_key=api_key, client_id=client_id)
                v5_prices = await service.fetch_prices_v5()

            if v5_prices:
                import psycopg2
                pg_conn = psycopg2.connect(**conn_params)
                pg_cur = pg_conn.cursor()
                updated_prices = 0
                for offer_id, mktg_price in v5_prices.items():
                    if mktg_price > 0:
                        pg_cur.execute(
                            "UPDATE dim_ozon_products SET marketing_price = %s WHERE shop_id = %s AND offer_id = %s",
                            (mktg_price, shop_id, offer_id),
                        )
                        updated_prices += pg_cur.rowcount
                pg_conn.commit()
                pg_cur.close()
                pg_conn.close()
                logger.info(f"Updated marketing_price for {updated_prices} products from v5 API")

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "products_found": len(product_list),
                "products_upserted": count,
                "image_events": len(events),
                "prices_updated": len(v5_prices) if v5_prices else 0,
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
        engine = create_async_engine(settings.database_url)
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

            ch_kwargs = dict(host=ch_host, port=ch_port, username=ch_user, password=os.getenv("CLICKHOUSE_PASSWORD", ""), database=ch_db)
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
        engine = create_async_engine(settings.database_url)
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

            with OzonOrdersLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
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

            with OzonOrdersLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
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

            with OzonTransactionsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
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

            with OzonTransactionsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonFunnelService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_all_funnel(date_from, date_to)

            with OzonFunnelLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
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

            with OzonFunnelLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonReturnsService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                raw = await service.fetch_returns(time_from, time_to)

            rows = normalize_returns(raw)

            with OzonReturnsLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonWarehouseStocksService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                # Fetch FBS stocks via /v4/product/info/stocks
                rows_fbs = await service.fetch_product_stocks()
                # Fetch FBO per-warehouse stocks via /v2/analytics/stock_on_warehouses
                rows_fbo = await service.fetch_warehouse_stocks()
                rows = rows_fbs + rows_fbo

            with OzonWarehouseStocksLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
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
    Snapshot Ozon product prices and commissions + detect OZON_PRICE_CHANGE events.

    Pipeline:
        1. Fetch prices from Ozon API (/v5/product/info/prices)
        2. Detect OZON_PRICE_CHANGE (compare marketing_price with Redis state)
        3. Save events to PostgreSQL event_log
        4. Update Redis price state
        5. Insert price snapshots into ClickHouse fact_ozon_prices

    Run daily or twice daily via sync_all_frequent.
    """
    import asyncio
    import json
    import os
    import logging
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_price_service import OzonPriceService, OzonPriceLoader
    from app.core.redis_state import RedisStateManager

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonPriceService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_prices()

            if not rows:
                await engine.dispose()
                return {"status": "completed", "rows_inserted": 0, "price_events": 0}

            # ── Step 2: Detect OZON_PRICE_CHANGE (before updating Redis!) ──
            self.update_state(state="PROGRESS", meta={"status": "Detecting price events..."})

            state_mgr = RedisStateManager(redis_url)
            price_events = []

            for row in rows:
                sku = row["sku"]
                # marketing_price is the real buyer-facing price
                current_price = float(row["marketing_price"])

                if current_price <= 0:
                    continue

                old_price = state_mgr.get_price(shop_id, sku)

                if old_price is not None and abs(old_price - current_price) > 0.01:
                    price_events.append({
                        "shop_id": shop_id,
                        "advert_id": 0,  # Not ad-related
                        "nm_id": sku,
                        "event_type": "OZON_PRICE_CHANGE",
                        "old_value": str(old_price),
                        "new_value": str(current_price),
                        "event_metadata": {
                            "offer_id": row.get("offer_id", ""),
                            "product_id": row.get("product_id", 0),
                            "base_price": str(row.get("price", 0)),
                        },
                    })

            logger.info("Detected %d OZON_PRICE_CHANGE events", len(price_events))

            # ── Step 3: Save events to PostgreSQL ──
            events_saved = 0
            if price_events:
                import psycopg2
                try:
                    conn = psycopg2.connect(**settings.psycopg2_conn_params)
                    cursor = conn.cursor()
                    for event in price_events:
                        cursor.execute("""
                            INSERT INTO event_log
                                (shop_id, advert_id, nm_id, event_type,
                                 old_value, new_value, event_metadata)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (
                            event["shop_id"],
                            event["advert_id"],
                            event["nm_id"],
                            event["event_type"],
                            event["old_value"],
                            event["new_value"],
                            json.dumps(event["event_metadata"]),
                        ))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    events_saved = len(price_events)
                    logger.info("Saved %d Ozon price events to event_log", events_saved)
                except Exception as e:
                    logger.error("Error saving Ozon price events: %s", e)

            # ── Step 4: Update Redis price state (after detection!) ──
            for row in rows:
                sku = row["sku"]
                current_price = float(row["marketing_price"])
                if current_price > 0:
                    state_mgr.set_price(shop_id, sku, current_price)

            # ── Step 5: Insert into ClickHouse ──
            with OzonPriceLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "rows_inserted": inserted,
                "price_events": events_saved,
                **stats,
            }
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
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonSellerRatingService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                rows = await service.fetch_rating()

            with OzonSellerRatingLoader(host=ch_host, port=ch_port, username=os.getenv("CLICKHOUSE_USER", "default"), password=os.getenv("CLICKHOUSE_PASSWORD", "")) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())


@celery_app.task(bind=True, time_limit=600, soft_time_limit=560)
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
        engine = create_async_engine(settings.database_url)
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
            from app.config import get_settings
            conn_params = get_settings().psycopg2_conn_params
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
        engine = create_async_engine(settings.database_url)
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
        engine = create_async_engine(settings.database_url)
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
        engine = create_async_engine(settings.database_url)
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
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
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
def sync_ozon_turnover(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
):
    """
    Daily sync of Ozon FBO item turnover (оборачиваемость).

    Fetches via POST /v1/analytics/item_turnover:
    - days_of_supply (на сколько дней хватит стока)
    - avg_daily_sales (средние дневные продажи)
    - turnover_category (категория оборачиваемости)

    Also tries beta /v1/analytics/turnover/stocks for enrichment.
    Queue: DEFAULT.
    """
    import asyncio
    import os
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from app.config import get_settings
    from app.services.ozon_turnover_service import (
        OzonTurnoverService, OzonTurnoverLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        try:
            async with sf() as db:
                service = OzonTurnoverService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                # Primary: /v1/analytics/item_turnover
                rows = await service.fetch_turnover()
                # Enrichment: beta /v1/analytics/turnover/stocks
                beta_rows = await service.fetch_turnover_stocks()

            # Merge beta data where primary data is missing
            if beta_rows:
                existing_skus = {r["sku"] for r in rows}
                for br in beta_rows:
                    if br["sku"] not in existing_skus:
                        rows.append(br)

            logger.info("Turnover sync: %d rows for shop %d", len(rows), shop_id)

            with OzonTurnoverLoader(
                host=ch_host, port=ch_port,
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            ) as loader:
                inserted = loader.insert_rows(shop_id, rows)
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {"status": "completed", "rows_inserted": inserted, **stats}
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())



@celery_app.task(bind=True, time_limit=600, soft_time_limit=580)
def sync_ozon_placement_cost(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    days_back: int = 31,
):
    """
    Sync Ozon placement (storage) costs per SKU from Excel report.

    API: POST /v1/report/placement/by-products/create
    Generates Excel report with per-SKU storage costs, then parses and
    inserts into ClickHouse fact_ozon_placement_cost.

    Limits: max 31-day period, 5 reports per day per seller.

    Pipeline:
        1. Build offer_id → sku mapping from PostgreSQL
        2. Create report via Ozon API
        3. Poll until ready (~10-60 sec)
        4. Download Excel, parse with openpyxl
        5. Insert into ClickHouse

    Queue: HEAVY.
    """
    import asyncio
    import os
    from datetime import datetime, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text
    from app.config import get_settings
    from app.services.ozon_placement_service import (
        OzonPlacementService, OzonPlacementLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    now = datetime.utcnow()
    date_to = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    date_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")

    async def run_sync():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Build offer_id → sku mapping from PostgreSQL
            self.update_state(state='PROGRESS', meta={
                'status': 'Building offer_id → SKU mapping...',
            })
            offer_to_sku = {}
            async with sf() as db:
                result = await db.execute(
                    text("""
                        SELECT offer_id, sku
                        FROM dim_ozon_products
                        WHERE shop_id = :sid AND sku IS NOT NULL AND offer_id IS NOT NULL
                    """),
                    {"sid": shop_id},
                )
                for row in result.fetchall():
                    if row[0] and row[1]:
                        offer_to_sku[str(row[0])] = int(row[1])

            logger.info(
                "Offer→SKU mapping: %d products for shop %d",
                len(offer_to_sku), shop_id,
            )

            # 2. Fetch placement costs from Ozon report
            self.update_state(state='PROGRESS', meta={
                'status': f'Creating placement report ({date_from} → {date_to})...',
            })

            async with sf() as db:
                service = OzonPlacementService(
                    db=db, shop_id=shop_id,
                    api_key=api_key, client_id=client_id,
                )
                # Random delay 10-30s to avoid competing with parallel sync tasks
                # (finance, funnel, returns) for Ozon's per-second rate limit.
                import random
                delay = random.uniform(10, 30)
                logger.info(
                    "Placement cost: waiting %.1fs before API call (shop %d)",
                    delay, shop_id,
                )
                await asyncio.sleep(delay)
                rows = await service.fetch_placement_costs(
                    date_from, date_to,
                    offer_to_sku=offer_to_sku,
                )

            if not rows:
                await engine.dispose()
                return {
                    "status": "completed",
                    "shop_id": shop_id,
                    "rows_inserted": 0,
                    "message": "No data in report or report generation failed",
                }

            logger.info(
                "Placement costs: %d rows for shop %d (%s → %s)",
                len(rows), shop_id, date_from, date_to,
            )

            # 3. Insert into ClickHouse
            self.update_state(state='PROGRESS', meta={
                'status': f'Inserting {len(rows)} placement cost rows...',
            })

            with OzonPlacementLoader(
                host=ch_host, port=ch_port,
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            ) as loader:
                inserted = loader.insert_costs(
                    shop_id, rows, date_from, date_to,
                )
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "period": f"{date_from} → {date_to}",
                "rows_inserted": inserted,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    return asyncio.run(run_sync())



@celery_app.task(bind=True, time_limit=1800, soft_time_limit=1700)
def backfill_ozon_placement_cost(
    self,
    shop_id: int,
    api_key: str,
    client_id: str,
    months: int = 3,
):
    """
    One-time backfill: load Ozon placement cost history for last N months.

    Splits into 30-day chunks (Ozon API limit: 31 days per report).
    3 months = 3 chunks = 3 reports (within 5 reports/day limit).
    Each chunk: create report → poll → download Excel → parse → insert.

    Queue: HEAVY. Can run up to 30 minutes due to polling delays.
    """
    import asyncio
    import os
    from datetime import date, timedelta
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import text
    from app.config import get_settings
    from app.services.ozon_placement_service import (
        OzonPlacementService, OzonPlacementLoader,
    )
    import logging

    logger = logging.getLogger(__name__)
    settings = get_settings()
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = int(os.getenv("CLICKHOUSE_PORT", 8123))

    async def run_backfill():
        engine = create_async_engine(settings.database_url)
        sf = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        try:
            # 1. Build offer_id → sku mapping
            self.update_state(state='PROGRESS', meta={
                'status': 'Building offer_id → SKU mapping...',
                'shop_id': shop_id,
            })
            offer_to_sku = {}
            async with sf() as db:
                result = await db.execute(
                    text("""
                        SELECT offer_id, sku
                        FROM dim_ozon_products
                        WHERE shop_id = :sid AND sku IS NOT NULL AND offer_id IS NOT NULL
                    """),
                    {"sid": shop_id},
                )
                for row in result.fetchall():
                    if row[0] and row[1]:
                        offer_to_sku[str(row[0])] = int(row[1])

            logger.info(
                "backfill_ozon_placement: shop=%s mapping %d products, months=%d",
                shop_id, len(offer_to_sku), months,
            )

            # 2. Split into 30-day chunks (Ozon limit: 31 days per report)
            today = date.today()
            end_date = today - timedelta(days=1)  # yesterday
            start_date = today - timedelta(days=months * 30)

            chunks = []
            chunk_end = end_date
            while chunk_end > start_date:
                chunk_start = max(chunk_end - timedelta(days=29), start_date)
                chunks.append((chunk_start, chunk_end))
                chunk_end = chunk_start - timedelta(days=1)

            total_chunks = len(chunks)
            total_inserted = 0
            chunk_results = []

            logger.info(
                "backfill_ozon_placement: shop=%s %d chunks (%s → %s)",
                shop_id, total_chunks, start_date, end_date,
            )

            # 3. Process each chunk
            for i, (c_start, c_end) in enumerate(chunks, 1):
                date_from_str = c_start.strftime("%Y-%m-%d")
                date_to_str = c_end.strftime("%Y-%m-%d")

                self.update_state(state='PROGRESS', meta={
                    'status': f'Chunk {i}/{total_chunks}: {date_from_str} → {date_to_str}...',
                    'shop_id': shop_id,
                    'chunk': i,
                    'total_chunks': total_chunks,
                })

                async with sf() as db:
                    service = OzonPlacementService(
                        db=db, shop_id=shop_id,
                        api_key=api_key, client_id=client_id,
                    )
                    rows = await service.fetch_placement_costs(
                        date_from_str, date_to_str,
                        offer_to_sku=offer_to_sku,
                    )

                if not rows:
                    logger.warning(
                        "backfill_ozon_placement: shop=%s chunk %d/%d empty (%s → %s)",
                        shop_id, i, total_chunks, date_from_str, date_to_str,
                    )
                    chunk_results.append({
                        "chunk": i, "period": f"{date_from_str} → {date_to_str}",
                        "rows": 0,
                    })
                    continue

                # Insert into ClickHouse
                with OzonPlacementLoader(
                    host=ch_host, port=ch_port,
                    username=os.getenv("CLICKHOUSE_USER", "default"),
                    password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                ) as loader:
                    inserted = loader.insert_costs(
                        shop_id, rows, date_from_str, date_to_str,
                    )

                total_inserted += inserted
                chunk_results.append({
                    "chunk": i, "period": f"{date_from_str} → {date_to_str}",
                    "rows": inserted,
                })

                logger.info(
                    "backfill_ozon_placement: shop=%s chunk %d/%d done: %d rows (%s → %s)",
                    shop_id, i, total_chunks, inserted, date_from_str, date_to_str,
                )

                # Pause between chunks to avoid Ozon API rate limits (429)
                if i < total_chunks:
                    await asyncio.sleep(10)

            # 4. Final stats
            with OzonPlacementLoader(
                host=ch_host, port=ch_port,
                username=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            ) as loader:
                stats = loader.get_stats(shop_id)

            await engine.dispose()
            return {
                "status": "completed",
                "shop_id": shop_id,
                "months": months,
                "total_chunks": total_chunks,
                "total_rows_inserted": total_inserted,
                "chunks": chunk_results,
                **stats,
            }
        except Exception as e:
            await engine.dispose()
            raise e

    try:
        return asyncio.run(run_backfill())
    except Exception as exc:
        self.retry(exc=exc, countdown=300, max_retries=2)


# ===================
# NORMQUERY (Search Cluster) SYNC
# Collects daily normquery stats + bid snapshots for UWB campaigns
# ===================

