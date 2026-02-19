-- ClickHouse initialization script
-- This file will be executed on first container startup

-- Create database
CREATE DATABASE IF NOT EXISTS mms_analytics;

-- ===================
-- Orders table (for sales/orders history)
-- Using ReplacingMergeTree for idempotency - duplicates will be collapsed
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.orders (
    id UUID,
    shop_id UInt32,
    order_id String,
    order_date DateTime,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),  -- Version field for ReplacingMergeTree
    
    -- Product info
    sku String,
    product_name String,
    brand String,
    category String,
    
    -- Quantities and prices
    quantity UInt32,
    price Decimal(10, 2),
    discount Decimal(10, 2),
    final_price Decimal(10, 2),
    
    -- Logistics
    warehouse_name String,
    region String,
    
    -- Status
    status String,
    is_cancelled UInt8 DEFAULT 0,
    
    -- Metadata
    raw_data String -- JSON with original API response
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (shop_id, order_id, sku)  -- Unique key for deduplication
TTL order_date + INTERVAL 2 YEAR;

-- ===================
-- Sales aggregates table (for faster analytics)
-- Using ReplacingMergeTree to handle re-aggregation
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.sales_daily (
    date Date,
    shop_id UInt32,
    sku String,
    updated_at DateTime DEFAULT now(),
    
    -- Aggregated metrics
    orders_count UInt32,
    total_quantity UInt32,
    revenue Decimal(12, 2),
    returns_count UInt32,
    returns_amount Decimal(12, 2),
    
    -- Average metrics
    avg_price Decimal(10, 2),
    avg_discount Decimal(10, 2)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, shop_id, sku);

-- ===================
-- Advertising statistics table
-- Using ReplacingMergeTree for daily stats updates
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.ad_stats (
    date Date,
    shop_id UInt32,
    campaign_id String,
    campaign_name String,
    updated_at DateTime DEFAULT now(),
    
    -- Impressions and clicks
    impressions UInt64,
    clicks UInt64,
    ctr Decimal(5, 4),
    
    -- Costs
    spent Decimal(12, 2),
    cpc Decimal(10, 2),
    
    -- Conversions
    orders UInt32,
    revenue Decimal(12, 2),
    cpo Decimal(10, 2), -- Cost per order
    drr Decimal(5, 2), -- DRR (доля рекламных расходов)
    
    -- Position tracking
    avg_position Decimal(5, 2),
    
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (date, shop_id, campaign_id)
TTL date + INTERVAL 1 YEAR;

-- ===================
-- Product positions tracking
-- Using ReplacingMergeTree for position updates
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.positions (
    checked_at DateTime,
    shop_id UInt32,
    sku String,
    keyword String,
    updated_at DateTime DEFAULT now(),
    
    -- Position info
    position UInt32,
    page UInt32,
    cpm Decimal(10, 2),
    
    -- Competitors
    competitors_count UInt32
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(checked_at)
ORDER BY (shop_id, sku, keyword, toStartOfMinute(checked_at))
TTL checked_at + INTERVAL 3 MONTH;

-- ===================
-- Materialized view for hourly aggregates
-- ===================
CREATE MATERIALIZED VIEW IF NOT EXISTS mms_analytics.sales_hourly_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, shop_id)
AS SELECT
    toStartOfHour(order_date) as hour,
    shop_id,
    count() as orders_count,
    sum(quantity) as total_quantity,
    sum(final_price) as revenue
FROM mms_analytics.orders
WHERE is_cancelled = 0
GROUP BY hour, shop_id;

-- ===================
-- OPTIMIZED VIEWS (use these instead of FINAL!)
-- argMax() is MUCH faster than FINAL at scale (10M+ rows)
-- ===================

-- Latest orders view (deduplicated without FINAL)
CREATE VIEW IF NOT EXISTS mms_analytics.orders_latest AS
SELECT
    shop_id,
    order_id,
    sku,
    argMax(id, updated_at) as id,
    argMax(order_date, updated_at) as order_date,
    argMax(product_name, updated_at) as product_name,
    argMax(brand, updated_at) as brand,
    argMax(category, updated_at) as category,
    argMax(quantity, updated_at) as quantity,
    argMax(price, updated_at) as price,
    argMax(discount, updated_at) as discount,
    argMax(final_price, updated_at) as final_price,
    argMax(warehouse_name, updated_at) as warehouse_name,
    argMax(region, updated_at) as region,
    argMax(status, updated_at) as status,
    argMax(is_cancelled, updated_at) as is_cancelled,
    max(updated_at) as updated_at
FROM mms_analytics.orders
GROUP BY shop_id, order_id, sku;

-- Latest ad stats view (deduplicated)
CREATE VIEW IF NOT EXISTS mms_analytics.ad_stats_latest AS
SELECT
    date,
    shop_id,
    campaign_id,
    argMax(campaign_name, updated_at) as campaign_name,
    argMax(impressions, updated_at) as impressions,
    argMax(clicks, updated_at) as clicks,
    argMax(ctr, updated_at) as ctr,
    argMax(spent, updated_at) as spent,
    argMax(cpc, updated_at) as cpc,
    argMax(orders, updated_at) as orders,
    argMax(revenue, updated_at) as revenue,
    argMax(cpo, updated_at) as cpo,
    argMax(drr, updated_at) as drr,
    argMax(avg_position, updated_at) as avg_position,
    max(updated_at) as updated_at
FROM mms_analytics.ad_stats
GROUP BY date, shop_id, campaign_id;

-- Latest positions view (deduplicated)
CREATE VIEW IF NOT EXISTS mms_analytics.positions_latest AS
SELECT
    shop_id,
    sku,
    keyword,
    toStartOfMinute(checked_at) as minute,
    argMax(position, updated_at) as position,
    argMax(page, updated_at) as page,
    argMax(cpm, updated_at) as cpm,
    argMax(competitors_count, updated_at) as competitors_count,
    max(updated_at) as updated_at
FROM mms_analytics.positions
GROUP BY shop_id, sku, keyword, minute;

-- ===================
-- Unified finances table (Heart of the system)
-- Accepts data from: WB API, Ozon API, Excel/CSV files
-- Using ReplacingMergeTree for idempotency
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_finances (
    -- 1. CORE GROUP (Common for all marketplaces)
    event_date Date,
    shop_id UInt32,
    marketplace Enum8('wb' = 1, 'ozon' = 2),
    order_id String,              -- Order/Shipment number
    external_id String,           -- nmId (WB) or SKU (Ozon)
    vendor_code String,           -- Seller's article code (for linking)
    rrd_id UInt64 DEFAULT 0,      -- Unique report line ID (IMPORTANT for deduplication)
    operation_type String,        -- Продажа, Возврат, Логистика, Корректировка
    
    -- MAIN MONEY
    quantity Int32,
    retail_amount Decimal(18, 2), -- Customer price
    payout_amount Decimal(18, 2), -- Net amount to seller
    
    -- 2. GEOGRAPHY & WAREHOUSES
    warehouse_name String,        -- Склад отгрузки (office_name)
    delivery_address String,      -- ПВЗ / Регион (ppvz_office_name)
    region_name String,           -- Область/Округ (gi_box_type_name или вычисляемое)
    
    -- 3. DETAILED EXPENSES (Unit Economics)
    commission_amount Decimal(18, 2),
    logistics_total Decimal(18, 2),
    ads_total Decimal(18, 2),     -- If deducted from report
    penalty_total Decimal(18, 2), -- Penalties
    storage_fee Decimal(18, 2) DEFAULT 0,   -- NEW
    acceptance_fee Decimal(18, 2) DEFAULT 0, -- NEW
    bonus_amount Decimal(18, 2) DEFAULT 0,  -- NEW
    
    -- 4. IDENTIFIERS (Tracing)
    shk_id String,                -- Barcode of unit
    rid String,                   -- Unique Order ID (WB rid)
    srid String,                  -- Global Order ID (WB srid)
    
    -- 5. WB SPECIFIC GROUP (Wildberries only)
    wb_gi_id UInt64 DEFAULT 0,              -- Supply number (Номер поставки)
    wb_ppvz_for_pay Decimal(18, 2) DEFAULT 0,
    wb_delivery_rub Decimal(18, 2) DEFAULT 0,
    wb_storage_amount Decimal(18, 2) DEFAULT 0, -- Legacy/Specific field
    
    -- 6. OZON SPECIFIC GROUP (Ozon only)
    ozon_acquiring Decimal(18, 2) DEFAULT 0,
    ozon_last_mile Decimal(18, 2) DEFAULT 0,
    ozon_milestone Decimal(18, 2) DEFAULT 0,
    ozon_marketing_services Decimal(18, 2) DEFAULT 0,

    -- 7. SERVICE FIELDS
    source_file_name String,      -- Filename for audit
    raw_payload String,           -- Full JSON row (backup)
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (shop_id, marketplace, event_date, order_id, external_id, rrd_id);

-- Latest fact_finances view (deduplicated without FINAL)
CREATE VIEW IF NOT EXISTS mms_analytics.fact_finances_latest AS
SELECT
    shop_id,
    marketplace,
    event_date,
    order_id,
    external_id,
    external_id,
    argMax(vendor_code, updated_at) as vendor_code,
    argMax(rrd_id, updated_at) as rrd_id,
    argMax(operation_type, updated_at) as operation_type,
    argMax(quantity, updated_at) as quantity,
    argMax(retail_amount, updated_at) as retail_amount,
    argMax(payout_amount, updated_at) as payout_amount,
    
    -- Geography
    argMax(warehouse_name, updated_at) as warehouse_name,
    argMax(delivery_address, updated_at) as delivery_address,
    argMax(region_name, updated_at) as region_name,
    
    -- Expenses
    argMax(commission_amount, updated_at) as commission_amount,
    argMax(logistics_total, updated_at) as logistics_total,
    argMax(ads_total, updated_at) as ads_total,
    argMax(penalty_total, updated_at) as penalty_total,
    argMax(storage_fee, updated_at) as storage_fee,
    argMax(acceptance_fee, updated_at) as acceptance_fee,
    argMax(bonus_amount, updated_at) as bonus_amount,
    
    -- Identifiers
    argMax(shk_id, updated_at) as shk_id,
    argMax(rid, updated_at) as rid,
    argMax(srid, updated_at) as srid,

    -- WB specific
    argMax(wb_gi_id, updated_at) as wb_gi_id,
    argMax(wb_ppvz_for_pay, updated_at) as wb_ppvz_for_pay,
    argMax(wb_delivery_rub, updated_at) as wb_delivery_rub,
    argMax(wb_storage_amount, updated_at) as wb_storage_amount,
    -- Ozon specific
    argMax(ozon_acquiring, updated_at) as ozon_acquiring,
    argMax(ozon_last_mile, updated_at) as ozon_last_mile,
    argMax(ozon_milestone, updated_at) as ozon_milestone,
    argMax(ozon_marketing_services, updated_at) as ozon_marketing_services,
    -- Service
    argMax(source_file_name, updated_at) as source_file_name,
    argMax(raw_payload, updated_at) as raw_payload,
    max(updated_at) as updated_at
FROM mms_analytics.fact_finances
GROUP BY shop_id, marketplace, event_date, order_id, external_id;

-- ===================
-- USAGE NOTES:
-- ===================
-- For frontend queries, ALWAYS use *_latest views:
--   SELECT * FROM orders_latest WHERE shop_id = 1
--   SELECT * FROM fact_finances_latest WHERE shop_id = 1
-- 
-- FINAL is still available but slow at scale:
--   SELECT * FROM orders FINAL WHERE shop_id = 1
--
-- For background maintenance:
--   OPTIMIZE TABLE orders FINAL
--   OPTIMIZE TABLE fact_finances FINAL
-- ===================


-- ===================
-- Advertising campaigns dictionary
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.dim_advert_campaigns (
    shop_id UInt32,
    advert_id UInt64,
    name String,
    type UInt8 DEFAULT 0,  -- 1=search, 2=carousel, 4=card, 5=recommend, 7=auto, 8=search_plus_catalog, 9=unified
    status Int8,
    updated_at DateTime,
    -- V2 API fields (added Feb 2026)
    payment_type String DEFAULT '',   -- 'cpm' or 'cpc'
    bid_type String DEFAULT '',       -- 'manual' or 'auto'
    search_enabled UInt8 DEFAULT 0,
    recommendations_enabled UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (shop_id, advert_id);

-- ===================
-- Advertising facts (detailed stats)
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_advert_stats (
    date Date,
    shop_id UInt32,
    advert_id UInt64,
    nm_id UInt64,
    views UInt32,
    clicks UInt32,
    spend Decimal(18, 2),
    ctr Float32,
    cpc Decimal(18, 2),
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (shop_id, nm_id, date, advert_id);

-- ===================
-- Advertising V3 Stats (full funnel: views→clicks→carts→orders)
-- ReplacingMergeTree: deduplicates by (shop, nm, date, advert),
-- keeping the row with the latest updated_at
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_advert_stats_v3 (
    date Date,
    shop_id UInt32,
    advert_id UInt64,
    nm_id UInt64,
    views UInt32 DEFAULT 0,
    clicks UInt32 DEFAULT 0,
    atbs UInt32 DEFAULT 0,       -- add to basket (carts)
    orders UInt32 DEFAULT 0,
    revenue Decimal(18, 2) DEFAULT 0,  -- sum_price from API
    spend Decimal(18, 2) DEFAULT 0,    -- sum from API (ad cost)
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(date)
ORDER BY (shop_id, nm_id, date, advert_id)
TTL date + INTERVAL 2 YEAR;

-- ===================
-- Advertising RAW History (for accumulation, NOT replacement!)
-- Uses MergeTree to APPEND data, enabling intraday analytics
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.ads_raw_history (
    -- Fetch timestamp (for intraday graphs!)
    fetched_at DateTime,
    
    -- Keys
    shop_id UInt32,
    advert_id UInt64,
    nm_id UInt64,
    
    -- Enrichment
    vendor_code String DEFAULT '',
    
    -- Campaign classification (CRITICAL for CPC vs CPM logic)
    campaign_type UInt8 DEFAULT 0,  -- 1=search, 7=auto, 8=search_plus_catalog, etc.
    
    -- Metrics (cumulative for the day from WB)
    views UInt32,
    clicks UInt32,
    ctr Float32,
    cpc Decimal(18, 2),
    spend Decimal(18, 2),
    atbs UInt32,
    orders UInt32,
    revenue Decimal(18, 2),
    
    -- Bid tracking (for change detection)
    cpm Decimal(18, 2) DEFAULT 0,
    
    -- Flags
    is_associated UInt8 DEFAULT 0  -- 1 = item not in campaign's official list (Halo)
    
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (shop_id, advert_id, nm_id, fetched_at)
TTL fetched_at + INTERVAL 6 MONTH;

-- ===================
-- Materialized View: Daily aggregates for Sales Map
-- Automatically aggregates 15-minute snapshots to daily MAX values
-- Use this for queries instead of raw data!
-- ===================
CREATE MATERIALIZED VIEW IF NOT EXISTS mms_analytics.ads_daily_mv
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (shop_id, advert_id, nm_id, date)
AS SELECT
    toDate(fetched_at) as date,
    shop_id,
    advert_id,
    nm_id,
    argMax(vendor_code, fetched_at) as vendor_code,
    argMax(campaign_type, fetched_at) as campaign_type,
    -- Take maximum values for the day (WB stats are cumulative)
    max(views) as views,
    max(clicks) as clicks,
    max(spend) as spend,
    max(atbs) as atbs,
    max(orders) as orders,
    max(revenue) as revenue,
    max(cpm) as cpm,
    argMax(is_associated, fetched_at) as is_associated,
    max(fetched_at) as updated_at
FROM mms_analytics.ads_raw_history
GROUP BY date, shop_id, advert_id, nm_id;

-- ===================
-- Hourly aggregates view for detailed timeline
-- ===================
CREATE MATERIALIZED VIEW IF NOT EXISTS mms_analytics.ads_hourly_mv
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (shop_id, advert_id, nm_id, hour)
AS SELECT
    toStartOfHour(fetched_at) as hour,
    shop_id,
    advert_id,
    nm_id,
    max(views) as views,
    max(clicks) as clicks,
    max(spend) as spend,
    max(orders) as orders,
    max(revenue) as revenue,
    max(fetched_at) as updated_at
FROM mms_analytics.ads_raw_history
GROUP BY hour, shop_id, advert_id, nm_id;

-- ===================
-- Commercial Monitoring: Inventory snapshots (prices + stocks)
-- Each row = one product on one warehouse at a point in time
-- Uses MergeTree (APPEND, not replace) for time-series data
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_inventory_snapshot (
    fetched_at DateTime,
    shop_id UInt32,
    nm_id UInt64,
    warehouse_name String,
    warehouse_id UInt32 DEFAULT 0,
    quantity UInt32,
    price Decimal(18, 2),
    discount UInt8
) ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (shop_id, nm_id, warehouse_name)
TTL fetched_at + INTERVAL 1 YEAR;

-- ===================
-- Sales Funnel Analytics (WB Seller Analytics API)
-- Daily funnel metrics per product: views, cart, orders, buyouts, conversions
-- ReplacingMergeTree: deduplicates by (shop_id, nm_id, event_date),
-- keeping the row with the latest fetched_at
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_sales_funnel (
    fetched_at      DateTime DEFAULT now(),  -- when this snapshot was taken
    event_date      Date,
    shop_id         UInt32,
    nm_id           UInt64,

    -- Funnel metrics
    open_count       UInt32 DEFAULT 0,      -- Card views
    cart_count       UInt32 DEFAULT 0,      -- Add to cart
    order_count      UInt32 DEFAULT 0,      -- Orders (qty)
    order_sum        Decimal(18, 2) DEFAULT 0,  -- Orders (sum)
    buyout_count     UInt32 DEFAULT 0,      -- Buyouts (qty)
    buyout_sum       Decimal(18, 2) DEFAULT 0,  -- Buyouts (sum)
    cancel_count     UInt32 DEFAULT 0,      -- Cancels (qty)
    cancel_sum       Decimal(18, 2) DEFAULT 0,  -- Cancels (sum)

    -- Conversions
    add_to_cart_pct  Float32 DEFAULT 0,     -- CR: card → cart (%)
    cart_to_order_pct Float32 DEFAULT 0,    -- CR: cart → order (%)
    buyout_pct       Float32 DEFAULT 0,     -- Buyout rate (%)

    -- Additional
    avg_price        Decimal(18, 2) DEFAULT 0,
    add_to_wishlist  UInt32 DEFAULT 0
) ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(event_date)
ORDER BY (shop_id, nm_id, event_date)
TTL event_date + INTERVAL 2 YEAR;

-- Latest sales funnel view (shows most recent snapshot per product+date)
CREATE VIEW IF NOT EXISTS mms_analytics.fact_sales_funnel_latest AS
SELECT
    shop_id,
    nm_id,
    event_date,
    argMax(open_count, fetched_at) as open_count,
    argMax(cart_count, fetched_at) as cart_count,
    argMax(order_count, fetched_at) as order_count,
    argMax(order_sum, fetched_at) as order_sum,
    argMax(buyout_count, fetched_at) as buyout_count,
    argMax(buyout_sum, fetched_at) as buyout_sum,
    argMax(cancel_count, fetched_at) as cancel_count,
    argMax(cancel_sum, fetched_at) as cancel_sum,
    argMax(add_to_cart_pct, fetched_at) as add_to_cart_pct,
    argMax(cart_to_order_pct, fetched_at) as cart_to_order_pct,
    argMax(buyout_pct, fetched_at) as buyout_pct,
    argMax(avg_price, fetched_at) as avg_price,
    argMax(add_to_wishlist, fetched_at) as add_to_wishlist,
    max(fetched_at) as last_fetched_at
FROM mms_analytics.fact_sales_funnel
GROUP BY shop_id, nm_id, event_date;

-- ═══════════════════════════════════════════════════
-- Operative Orders (raw orders from Statistics API)
-- ═══════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS mms_analytics.fact_orders_raw (
    date               DateTime,
    last_change_date   DateTime,
    shop_id            UInt32,
    nm_id              UInt64,
    g_number           String,
    srid               String DEFAULT '',

    supplier_article   String DEFAULT '',
    barcode            String DEFAULT '',
    category           String DEFAULT '',
    subject            String DEFAULT '',
    brand              String DEFAULT '',
    tech_size          String DEFAULT '0',

    warehouse_name     String DEFAULT '',
    warehouse_type     String DEFAULT '',
    country_name       String DEFAULT '',
    oblast_okrug_name  String DEFAULT '',
    region_name        String DEFAULT '',

    total_price        Decimal(18, 2) DEFAULT 0,
    discount_percent   UInt8 DEFAULT 0,
    spp                Float32 DEFAULT 0,
    finished_price     Decimal(18, 2) DEFAULT 0,
    price_with_disc    Decimal(18, 2) DEFAULT 0,

    is_cancel          UInt8 DEFAULT 0,
    cancel_date        DateTime DEFAULT '1970-01-01 00:00:00',
    sticker            String DEFAULT '',
    income_id          UInt32 DEFAULT 0,
    is_supply          UInt8 DEFAULT 0,
    is_realization     UInt8 DEFAULT 0,

    synced_at          DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(synced_at)
PARTITION BY toYYYYMM(date)
ORDER BY (shop_id, g_number)
TTL date + INTERVAL 2 YEAR;

CREATE VIEW IF NOT EXISTS mms_analytics.fact_orders_raw_latest AS
SELECT
    shop_id,
    g_number,
    argMax(date, synced_at) as date,
    argMax(last_change_date, synced_at) as last_change_date,
    argMax(nm_id, synced_at) as nm_id,
    argMax(srid, synced_at) as srid,
    argMax(supplier_article, synced_at) as supplier_article,
    argMax(warehouse_name, synced_at) as warehouse_name,
    argMax(warehouse_type, synced_at) as warehouse_type,
    argMax(country_name, synced_at) as country_name,
    argMax(region_name, synced_at) as region_name,
    argMax(price_with_disc, synced_at) as price_with_disc,
    argMax(finished_price, synced_at) as finished_price,
    argMax(total_price, synced_at) as total_price,
    argMax(discount_percent, synced_at) as discount_percent,
    argMax(spp, synced_at) as spp,
    argMax(is_cancel, synced_at) as is_cancel,
    argMax(cancel_date, synced_at) as cancel_date,
    max(synced_at) as last_synced_at
FROM mms_analytics.fact_orders_raw
GROUP BY shop_id, g_number;


-- ===================
-- Ozon: Inventory Snapshots (prices + stocks history)
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_inventory (
    fetched_at DateTime DEFAULT now(),
    shop_id UInt32,
    product_id UInt64,
    offer_id String,
    price Decimal(12, 2),
    old_price Decimal(12, 2),
    min_price Decimal(12, 2),
    marketing_price Decimal(12, 2),
    stocks_fbo UInt32,
    stocks_fbs UInt32
) ENGINE = ReplacingMergeTree(fetched_at)
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (shop_id, product_id)
TTL fetched_at + INTERVAL 1 YEAR;

-- ═══════════════════════════════════════════════════════════
-- WB Advertising: Bid History Log (V2 API)
-- Stores per-nm_id bid snapshots every sync cycle (~30min).
-- Bids in kopecks from /api/advert/v2/adverts
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.log_wb_bids (
    timestamp            DateTime DEFAULT now(),
    shop_id              UInt32,
    advert_id            UInt64,
    nm_id                UInt64,
    bid_type             String,
    payment_type         String,
    bid_search           UInt32,
    bid_recommendations  UInt32,
    search_enabled       UInt8,
    recommendations_enabled UInt8,
    status               UInt8
) ENGINE = MergeTree()
ORDER BY (shop_id, advert_id, nm_id, timestamp)
TTL timestamp + INTERVAL 1 YEAR;

-- ═══════════════════════════════════════════════════════════
-- Ozon Advertising: Bid Log
-- Stores bid snapshots every 15 min for running campaigns.
-- bid values in RUB (converted from microroubles).
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.log_ozon_bids (
    timestamp DateTime,
    shop_id UInt32,
    campaign_id UInt64,
    sku UInt64,
    avg_cpc Decimal(18, 2),
    price Decimal(18, 2)
) ENGINE = MergeTree()
ORDER BY (shop_id, campaign_id, sku, timestamp);

-- ═══════════════════════════════════════════════════════════
-- Ozon Advertising: Daily Statistics
-- ReplacingMergeTree auto-replaces old rows on FINAL query
-- when Ozon attribution window updates orders retroactively.
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_ad_daily (
    dt Date,
    updated_at DateTime DEFAULT now(),
    shop_id UInt32,
    campaign_id UInt64,
    sku UInt64,
    views UInt32,
    clicks UInt32,
    ctr Float32,
    add_to_cart UInt32,
    avg_cpc Decimal(18,2),
    money_spent Decimal(18,2),
    orders UInt32,
    revenue Decimal(18,2),
    model_orders UInt32,
    model_revenue Decimal(18,2),
    drr Float32
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (shop_id, campaign_id, sku, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Orders (FBO + FBS postings, 1 row per product per posting)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_orders (
    posting_number String,
    order_id UInt64,
    order_number String,
    order_date DateTime,
    in_process_at DateTime,
    status String,
    substatus String,
    sku UInt64,
    product_id UInt64,
    offer_id String,
    product_name String,
    quantity UInt32,
    warehouse_mode String,
    price Decimal(18, 2),
    old_price Decimal(18, 2),
    commission_amount Decimal(18, 2),
    commission_percent Decimal(5, 2),
    payout Decimal(18, 2),
    total_discount_percent Decimal(5, 2),
    total_discount_value Decimal(18, 2),
    city String,
    region String,
    cluster_from String,
    cluster_to String,
    delivery_type String,
    warehouse_name String,
    cancel_reason String,
    shipment_date DateTime,
    shop_id UInt32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (shop_id, sku, order_date, posting_number);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Financial Transactions
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_transactions (
    operation_id UInt64,
    operation_date DateTime,
    operation_type String,
    operation_type_name String,
    category String,
    posting_number String,
    delivery_schema String,
    sku UInt64,
    item_name String,
    amount Decimal(18, 2),
    accruals_for_sale Decimal(18, 2),
    sale_commission Decimal(18, 2),
    services_total Decimal(18, 2),
    type String,
    shop_id UInt32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(operation_date)
ORDER BY (shop_id, operation_date, operation_id);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Sales Funnel (ordered_units, revenue per SKU per day)
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_funnel (
    dt Date,
    shop_id UInt32,
    sku UInt64,
    sku_name String,
    ordered_units UInt32,
    revenue Decimal(18, 2),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Returns
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_returns (
    dt Date,
    shop_id UInt32,
    return_id UInt64,
    order_id UInt64,
    order_number String,
    posting_number String,
    return_type String,
    return_schema String,
    return_reason String,
    sku UInt64,
    offer_id String,
    product_name String,
    quantity UInt32,
    price Decimal(18, 2),
    place_name String,
    target_place String,
    compensation_status String,
    accepted_at DateTime,
    returned_at DateTime,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, return_id);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Price History
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_prices (
    dt Date,
    shop_id UInt32,
    sku UInt64,
    product_id UInt64,
    offer_id String,
    product_name String,
    price Decimal(18, 2),
    old_price Decimal(18, 2),
    min_price Decimal(18, 2),
    marketing_price Decimal(18, 2),
    sales_percent Float32,
    fbo_commission_percent Float32,
    fbs_commission_percent Float32,
    fbo_commission_value Decimal(18, 2),
    fbs_commission_value Decimal(18, 2),
    acquiring_percent Float32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Commissions per product
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_commissions (
    dt Date,
    updated_at DateTime DEFAULT now(),
    shop_id UInt32,
    product_id UInt64,
    offer_id String,
    sku UInt64,
    sales_percent Float32,
    fbo_fulfillment_amount Decimal(18, 2),
    fbo_direct_flow_trans_min Decimal(18, 2),
    fbo_direct_flow_trans_max Decimal(18, 2),
    fbo_deliv_to_customer Decimal(18, 2),
    fbo_return_flow Decimal(18, 2),
    fbs_direct_flow_trans_min Decimal(18, 2),
    fbs_direct_flow_trans_max Decimal(18, 2),
    fbs_deliv_to_customer Decimal(18, 2),
    fbs_first_mile_min Decimal(18, 2),
    fbs_first_mile_max Decimal(18, 2),
    fbs_return_flow Decimal(18, 2)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, product_id, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Seller Rating
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_seller_rating (
    dt Date,
    shop_id UInt32,
    group_name String,
    rating_name String,
    rating_value Float64,
    rating_status String,
    penalty_score Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, rating_name, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Warehouse Stocks
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_warehouse_stocks (
    dt Date,
    shop_id UInt32,
    sku UInt64,
    product_name String,
    offer_id String,
    warehouse_name String,
    warehouse_type String,
    free_to_sell UInt32,
    promised UInt32,
    reserved UInt32,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, warehouse_name, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Content Rating per SKU
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_content_rating (
    dt Date,
    updated_at DateTime DEFAULT now(),
    shop_id UInt32,
    sku UInt64,
    product_id UInt64,
    rating Float32,
    media_rating Float32,
    description_rating Float32,
    attributes_rating Float32,
    rich_content_rating Float32
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Promotions
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_promotions (
    dt Date,
    updated_at DateTime DEFAULT now(),
    shop_id UInt32,
    product_id UInt64,
    offer_id String,
    promo_type String,
    is_enabled UInt8
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, product_id, promo_type, dt);

-- ═══════════════════════════════════════════════════════════
-- Ozon: Product Availability
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_availability (
    dt Date,
    updated_at DateTime DEFAULT now(),
    shop_id UInt32,
    product_id UInt64,
    offer_id String,
    sku UInt64,
    source String,
    availability String
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, product_id, source, dt);
