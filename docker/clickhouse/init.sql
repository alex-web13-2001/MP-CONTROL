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
-- USAGE NOTES:
-- ===================
-- For frontend queries, ALWAYS use *_latest views:
--   SELECT * FROM orders_latest WHERE shop_id = 1
-- 
-- FINAL is still available but slow at scale:
--   SELECT * FROM orders FINAL WHERE shop_id = 1
--
-- For background maintenance:
--   OPTIMIZE TABLE orders FINAL
-- ===================
