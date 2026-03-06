"""
Backfill OZON_PRICE_CHANGE events from ClickHouse historical data.

Compares consecutive daily snapshots in fact_ozon_prices
and generates events for any marketing_price changes.

Run inside backend container:
    docker exec mms-backend python /app/scripts/backfill_ozon_price_events.py
"""

import os
import sys
import json
import logging
from datetime import datetime

import clickhouse_connect
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

# ClickHouse connection
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASS = os.getenv("CLICKHOUSE_PASSWORD", "")

# PostgreSQL connection
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "mms")
PG_USER = os.getenv("POSTGRES_USER", "mms")
PG_PASS = os.getenv("POSTGRES_PASSWORD", os.getenv("DB_PASSWORD", ""))


def main():
    logger.info("Connecting to ClickHouse %s:%s ...", CH_HOST, CH_PORT)
    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASS,
        database="mms_analytics",
    )

    # 1. Find available dates
    dates_result = ch.query(
        "SELECT DISTINCT dt FROM fact_ozon_prices ORDER BY dt"
    )
    dates = [row[0] for row in dates_result.result_rows]
    logger.info("Found %d dates in fact_ozon_prices: %s .. %s",
                len(dates), dates[0] if dates else "?", dates[-1] if dates else "?")

    if len(dates) < 2:
        logger.info("Need at least 2 dates to compare. Exiting.")
        return

    # 2. Find all shop_ids
    shops_result = ch.query(
        "SELECT DISTINCT shop_id FROM fact_ozon_prices"
    )
    shop_ids = [row[0] for row in shops_result.result_rows]
    logger.info("Shop IDs: %s", shop_ids)

    # 3. Compare consecutive days
    all_events = []

    for i in range(1, len(dates)):
        prev_dt = dates[i - 1]
        curr_dt = dates[i]

        for shop_id in shop_ids:
            # Get prices for both days
            query = """
                SELECT
                    t1.sku,
                    t1.marketing_price AS old_price,
                    t2.marketing_price AS new_price,
                    t1.offer_id,
                    t1.product_id
                FROM (
                    SELECT sku, marketing_price, offer_id, product_id
                    FROM fact_ozon_prices
                    WHERE dt = {prev_dt:Date} AND shop_id = {shop_id:UInt32}
                ) t1
                INNER JOIN (
                    SELECT sku, marketing_price
                    FROM fact_ozon_prices
                    WHERE dt = {curr_dt:Date} AND shop_id = {shop_id:UInt32}
                ) t2 ON t1.sku = t2.sku
                WHERE abs(t1.marketing_price - t2.marketing_price) > 0.01
            """

            result = ch.query(query, parameters={
                "prev_dt": prev_dt,
                "curr_dt": curr_dt,
                "shop_id": shop_id,
            })

            for row in result.result_rows:
                sku, old_price, new_price, offer_id, product_id = row
                all_events.append({
                    "created_at": datetime.combine(curr_dt, datetime.min.time()),
                    "shop_id": shop_id,
                    "advert_id": 0,
                    "nm_id": int(sku),
                    "event_type": "OZON_PRICE_CHANGE",
                    "old_value": str(float(old_price)),
                    "new_value": str(float(new_price)),
                    "event_metadata": json.dumps({
                        "offer_id": offer_id or "",
                        "product_id": int(product_id) if product_id else 0,
                        "backfilled": True,
                    }),
                })

            if result.result_rows:
                logger.info("  %s → %s (shop %d): %d price changes",
                            prev_dt, curr_dt, shop_id, len(result.result_rows))

    logger.info("Total events to backfill: %d", len(all_events))

    if not all_events:
        logger.info("No price changes found. Done.")
        ch.close()
        return

    # 4. Insert into PostgreSQL event_log
    logger.info("Connecting to PostgreSQL %s:%s/%s ...", PG_HOST, PG_PORT, PG_DB)
    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS,
    )
    cursor = pg.cursor()

    # Check for existing backfilled events to avoid duplicates
    cursor.execute(
        "SELECT COUNT(*) FROM event_log WHERE event_type = 'OZON_PRICE_CHANGE'"
    )
    existing = cursor.fetchone()[0]
    if existing > 0:
        logger.warning(
            "Already have %d OZON_PRICE_CHANGE events. "
            "Delete them first if you want to re-backfill.", existing
        )
        cursor.close()
        pg.close()
        ch.close()
        return

    inserted = 0
    for ev in all_events:
        cursor.execute("""
            INSERT INTO event_log
                (created_at, shop_id, advert_id, nm_id,
                 event_type, old_value, new_value, event_metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            ev["created_at"], ev["shop_id"], ev["advert_id"], ev["nm_id"],
            ev["event_type"], ev["old_value"], ev["new_value"],
            ev["event_metadata"],
        ))
        inserted += 1

    pg.commit()
    cursor.close()
    pg.close()
    ch.close()

    logger.info("Done! Inserted %d OZON_PRICE_CHANGE events.", inserted)


if __name__ == "__main__":
    main()
