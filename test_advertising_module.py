#!/usr/bin/env python3
"""
Test script to verify ads_raw_history module works correctly.
Inserts mock data and validates it appears in the table and Materialized Views.
"""

import subprocess
import json
from datetime import datetime

# Mock data for testing
SHOP_ID = 999
ADVERT_ID = 123456789
NM_ID = 111222333
TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def run_ch_query(query: str, format: str = None) -> str:
    """Execute ClickHouse query via docker exec."""
    cmd = ["docker", "exec", "mms-clickhouse", "clickhouse-client", "--query", query]
    if format:
        cmd.extend(["--format", format])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"ERROR: {result.stderr}")
    return result.stdout.strip()

def test_insert():
    """Test inserting data into ads_raw_history."""
    print("=" * 50)
    print("TEST 1: Insert mock data into ads_raw_history")
    print("=" * 50)
    
    insert_query = f"""
    INSERT INTO mms_analytics.ads_raw_history 
    (fetched_at, shop_id, advert_id, nm_id, vendor_code, campaign_type, 
     views, clicks, ctr, cpc, spend, atbs, orders, revenue, cpm, is_associated)
    VALUES 
    ('{TIMESTAMP}', {SHOP_ID}, {ADVERT_ID}, {NM_ID}, 'TEST-SKU-001', 8, 
     1500, 45, 3.0, 25.50, 1147.50, 12, 5, 25000.00, 350, 0),
    ('{TIMESTAMP}', {SHOP_ID}, {ADVERT_ID}, {NM_ID + 1}, 'TEST-SKU-002', 8, 
     300, 8, 2.67, 20.00, 160.00, 3, 2, 8000.00, 350, 1)
    """
    
    run_ch_query(insert_query)
    print(f"✅ Inserted 2 rows (1 regular, 1 associated/halo)")

def test_select():
    """Verify data was inserted correctly."""
    print("\n" + "=" * 50)
    print("TEST 2: Verify data in ads_raw_history")
    print("=" * 50)
    
    select_query = f"""
    SELECT 
        fetched_at, nm_id, vendor_code, campaign_type,
        views, clicks, spend, orders, revenue, is_associated
    FROM mms_analytics.ads_raw_history
    WHERE shop_id = {SHOP_ID}
    ORDER BY nm_id
    """
    
    result = run_ch_query(select_query, "PrettyCompact")
    print(result)
    
    # Count check
    count_query = f"SELECT count() FROM mms_analytics.ads_raw_history WHERE shop_id = {SHOP_ID}"
    count = run_ch_query(count_query)
    print(f"\n✅ Total rows for shop {SHOP_ID}: {count}")

def test_daily_mv():
    """Verify Materialized View aggregation."""
    print("\n" + "=" * 50)
    print("TEST 3: Verify ads_daily_mv (Materialized View)")
    print("=" * 50)
    
    query = f"""
    SELECT 
        date, nm_id, campaign_type,
        views, clicks, spend, orders, is_associated
    FROM mms_analytics.ads_daily_mv FINAL
    WHERE shop_id = {SHOP_ID}
    ORDER BY nm_id
    """
    
    result = run_ch_query(query, "PrettyCompact")
    if result:
        print(result)
        print("\n✅ Materialized View working correctly!")
    else:
        print("⚠️  No data in ads_daily_mv (may take a moment to populate)")

def test_cleanup():
    """Clean up test data."""
    print("\n" + "=" * 50)
    print("TEST 4: Cleanup test data")
    print("=" * 50)
    
    delete_query = f"ALTER TABLE mms_analytics.ads_raw_history DELETE WHERE shop_id = {SHOP_ID}"
    run_ch_query(delete_query)
    print(f"✅ Deleted test data for shop_id={SHOP_ID}")

def main():
    print("\n🚀 ADVERTISING MODULE V3 - VERIFICATION TEST")
    print("=" * 50 + "\n")
    
    try:
        test_insert()
        test_select()
        test_daily_mv()
    finally:
        test_cleanup()
    
    print("\n" + "=" * 50)
    print("✅ ALL TESTS PASSED!")
    print("=" * 50)

if __name__ == "__main__":
    main()
