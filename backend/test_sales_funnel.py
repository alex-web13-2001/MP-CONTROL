"""
Download CSV report, parse, and load into ClickHouse.
Report ID: 814a7207-b998-47cb-bf55-a72569acd8c5 (already SUCCESS)
"""
import requests
import clickhouse_connect
import csv
import io
import zipfile
import json
from datetime import date, timedelta, datetime

API_KEY = "eyJhbGciOiJFUzI1NiIsImtpZCI6IjIwMjUwOTA0djEiLCJ0eXAiOiJKV1QifQ.eyJhY2MiOjEsImVudCI6MSwiZXhwIjoxNzc4OTEwNTQzLCJpZCI6IjAxOWE4MzdjLTI3MGUtNzg2My1iZGM0LWU0YTljMTFmM2EzYyIsImlpZCI6MTMxMjExMTYyLCJvaWQiOjEzNTg2MzAsInMiOjE2MTI2LCJzaWQiOiJiMWMwNWE3YS01NWQwLTQ4YmItOTgzZi05NjM0MTAwZmI2MDIiLCJ0IjpmYWxzZSwidWlkIjoxMzEyMTExNjJ9.D07MKx2fB9t2OefPFWc6B30M9Iut-GWXL695OrHxvrZiXBs0dUPSbBMGKNRIKrCRb_9qb_8DJtvJi7b2ZZ09Yg"
BASE = "https://seller-analytics-api.wildberries.ru"
H = {"Authorization": API_KEY, "Content-Type": "application/json"}
SHOP_ID = 1
report_id = "814a7207-b998-47cb-bf55-a72569acd8c5"

ch = clickhouse_connect.get_client(host="localhost", port=8123, database="mms_analytics")
ch.command("TRUNCATE TABLE mms_analytics.fact_sales_funnel")

COLUMNS = [
    "fetched_at", "event_date", "shop_id", "nm_id",
    "open_count", "cart_count", "order_count", "order_sum",
    "buyout_count", "buyout_sum", "cancel_count", "cancel_sum",
    "add_to_cart_pct", "cart_to_order_pct", "buyout_pct",
    "avg_price", "add_to_wishlist",
]

# Step 1: Check report info
print("=" * 60)
print("STEP 1: Get report info")
print("=" * 60)

resp = requests.get(f"{BASE}/api/v2/nm-report/downloads", headers=H)
if resp.status_code == 200:
    data = resp.json()
    reports = data.get("data", data) if isinstance(data, dict) else data
    if isinstance(reports, list):
        for r in reports:
            if r.get("id") == report_id:
                print(f"  Report: {r.get('id')}")
                print(f"  Status: {r.get('status')}")
                print(f"  Name: {r.get('userReportName', 'N/A')}")
                # Show all keys for understanding structure
                print(f"  Keys: {list(r.keys())}")
                print(f"  Full: {json.dumps(r, ensure_ascii=False, indent=2)[:500]}")

# Step 2: Download report
print()
print("=" * 60)
print("STEP 2: Download CSV report")
print("=" * 60)

resp = requests.get(f"{BASE}/api/v2/nm-report/downloads/file/{report_id}", headers=H)
print(f"  Download status: {resp.status_code}")
print(f"  Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
print(f"  Content-Length: {len(resp.content)} bytes")

if resp.status_code != 200:
    print(f"  Error: {resp.text[:500]}")
    exit(1)

# Save raw for inspection
with open("/tmp/funnel_report.bin", "wb") as f:
    f.write(resp.content)
print(f"  Saved to /tmp/funnel_report.bin")

# Step 3: Parse content
print()
print("=" * 60)
print("STEP 3: Parse report content")
print("=" * 60)

content = resp.content
rows = []
now = datetime.now()

# Try ZIP first
try:
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        print(f"  ZIP detected! Files: {zf.namelist()}")
        for name in zf.namelist():
            if name.endswith(".csv"):
                with zf.open(name) as f:
                    text = f.read().decode("utf-8")
                    print(f"  CSV file: {name}, {len(text)} bytes")
                    print(f"  Preview (first 500 chars):")
                    print(f"  {text[:500]}")
                    reader = csv.DictReader(io.StringIO(text))
                    print(f"  CSV headers: {reader.fieldnames}")
                    for row in reader:
                        # Try all possible date columns
                        date_str = row.get("date", "") or row.get("Дата", "") or row.get("dt", "")
                        if not date_str:
                            continue
                        try:
                            ev_date = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
                        except:
                            continue
                        nm_id = int(row.get("nmId", 0) or row.get("nm_id", 0) or row.get("Артикул WB", 0) or 0)
                        if not nm_id:
                            continue
                        rows.append([
                            now, ev_date, SHOP_ID, nm_id,
                            int(row.get("openCount", 0) or row.get("openCardCount", 0) or row.get("Переходы в карточку", 0) or 0),
                            int(row.get("cartCount", 0) or row.get("addToCartCount", 0) or row.get("Добавления в корзину", 0) or 0),
                            int(row.get("orderCount", 0) or row.get("ordersCount", 0) or row.get("Заказы, шт", 0) or 0),
                            float(row.get("orderSum", 0) or row.get("ordersSumRub", 0) or row.get("Заказы, руб", 0) or 0),
                            int(row.get("buyoutCount", 0) or row.get("buyoutsCount", 0) or row.get("Выкупы, шт", 0) or 0),
                            float(row.get("buyoutSum", 0) or row.get("buyoutsSumRub", 0) or row.get("Выкупы, руб", 0) or 0),
                            int(row.get("cancelCount", 0) or row.get("cancelCount", 0) or row.get("Отмены, шт", 0) or 0),
                            float(row.get("cancelSum", 0) or row.get("cancelSumRub", 0) or row.get("Отмены, руб", 0) or 0),
                            float(row.get("addToCartConversion", 0) or row.get("addToCartPercent", 0) or row.get("CR в корзину", 0) or 0),
                            float(row.get("cartToOrderConversion", 0) or row.get("cartToOrderPercent", 0) or row.get("CR в заказ", 0) or 0),
                            float(row.get("buyoutPercent", 0) or row.get("buyoutsPercent", 0) or row.get("Процент выкупа", 0) or 0),
                            float(row.get("avgPrice", 0) or row.get("avgPriceRub", 0) or row.get("Средняя цена", 0) or 0),
                            int(row.get("addToWishlistCount", 0) or row.get("Добавления в избранное", 0) or 0),
                        ])
except zipfile.BadZipFile:
    # Not a ZIP — try as raw CSV
    print("  Not a ZIP, trying as raw CSV...")
    text = content.decode("utf-8")
    print(f"  Text preview: {text[:500]}")
    reader = csv.DictReader(io.StringIO(text))
    print(f"  CSV headers: {reader.fieldnames}")
    for row in reader:
        date_str = row.get("date", "") or row.get("Дата", "") or row.get("dt", "")
        if not date_str:
            continue
        try:
            ev_date = datetime.strptime(date_str.split("T")[0], "%Y-%m-%d").date()
        except:
            continue
        nm_id = int(row.get("nmId", 0) or row.get("nm_id", 0) or row.get("Артикул WB", 0) or 0)
        if not nm_id:
            continue
        rows.append([
            now, ev_date, SHOP_ID, nm_id,
            int(row.get("openCount", 0) or row.get("openCardCount", 0) or 0),
            int(row.get("cartCount", 0) or row.get("addToCartCount", 0) or 0),
            int(row.get("orderCount", 0) or row.get("ordersCount", 0) or 0),
            float(row.get("orderSum", 0) or row.get("ordersSumRub", 0) or 0),
            int(row.get("buyoutCount", 0) or row.get("buyoutsCount", 0) or 0),
            float(row.get("buyoutSum", 0) or row.get("buyoutsSumRub", 0) or 0),
            int(row.get("cancelCount", 0) or 0),
            float(row.get("cancelSum", 0) or 0),
            float(row.get("addToCartConversion", 0) or row.get("addToCartPercent", 0) or 0),
            float(row.get("cartToOrderConversion", 0) or row.get("cartToOrderPercent", 0) or 0),
            float(row.get("buyoutPercent", 0) or 0),
            float(row.get("avgPrice", 0) or row.get("avgPriceRub", 0) or 0),
            int(row.get("addToWishlistCount", 0) or 0),
        ])

print(f"\n  Total rows parsed: {len(rows)}")
if rows:
    dates = [r[1] for r in rows]
    nm_ids = set(r[3] for r in rows)
    print(f"  Date range: {min(dates)} — {max(dates)}")
    print(f"  Unique products: {len(nm_ids)}")
    print(f"  Sample row: {rows[0]}")

# Step 4: Insert into ClickHouse
if rows:
    print()
    print("=" * 60)
    print(f"STEP 4: Insert {len(rows)} rows into ClickHouse")
    print("=" * 60)

    BATCH = 500
    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i+BATCH]
        ch.insert("fact_sales_funnel", batch, column_names=COLUMNS)
        total += len(batch)
    print(f"  Inserted: {total} rows")

    # Step 5: Verify
    print()
    print("=" * 60)
    print("STEP 5: Verify in ClickHouse")
    print("=" * 60)

    result = ch.query("""
        SELECT 
            count() as total,
            uniq(nm_id) as products,
            min(event_date) as min_date,
            max(event_date) as max_date,
            sum(open_count) as opens,
            sum(order_count) as orders,
            sum(buyout_count) as buyouts
        FROM fact_sales_funnel WHERE shop_id = 1
    """)
    r = result.first_row
    print(f"  Total rows:    {r[0]}")
    print(f"  Products:      {r[1]}")
    print(f"  Date range:    {r[2]} — {r[3]}")
    print(f"  Total opens:   {r[4]}")
    print(f"  Total orders:  {r[5]}")
    print(f"  Total buyouts: {r[6]}")

    # Monthly breakdown
    result = ch.query("""
        SELECT toStartOfMonth(event_date) as month, count() as rows, 
               uniq(nm_id) as products, sum(open_count) as opens, sum(order_count) as orders
        FROM fact_sales_funnel WHERE shop_id = 1
        GROUP BY month ORDER BY month
    """)
    print(f"\n  Monthly breakdown:")
    for r in result.result_rows:
        print(f"    {r[0]}: {r[1]} rows, {r[2]} products, opens={r[3]}, orders={r[4]}")

ch.close()
print(f"\n✅ BACKFILL COMPLETE")
