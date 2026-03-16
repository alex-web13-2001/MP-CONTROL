-- Ozon: Placement Cost per SKU (from /v1/report/placement/by-products/create)
-- Stores ACTUAL storage placement costs per SKU from Ozon Excel reports.
-- ReplacingMergeTree: deduplicates by (shop_id, sku, dt) keeping latest updated_at.

CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_placement_cost (
    dt Date,                              -- report period start
    period_end Date,                      -- report period end
    shop_id UInt32,
    sku UInt64,
    product_id UInt64 DEFAULT 0,
    offer_id String,
    product_name String,
    volume_liters Float32 DEFAULT 0,      -- product volume (liters)
    avg_daily_stock Float32 DEFAULT 0,    -- average daily stock (units)
    placement_cost Decimal(18, 2),        -- ACTUAL placement cost (₽)
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, dt);
