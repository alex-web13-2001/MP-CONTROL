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
    type Enum8('search' = 1, 'carousel' = 2, 'card' = 4, 'recommend' = 5, 'auto' = 7, 'search_plus_catalog' = 8, 'recommend_plus_carousel' = 9),
    status Int8,
    updated_at DateTime
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
