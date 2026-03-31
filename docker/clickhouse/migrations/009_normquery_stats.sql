-- ===================
-- Normquery (Search Cluster) Statistics — Daily breakdown
-- Stores per-cluster performance metrics for UWB (manual bid) campaigns.
-- Source: WB API /adv/v1/normquery/stats (daily) + /adv/v0/normquery/stats (aggregated)
-- Engine: ReplacingMergeTree (deduplicates by shop_id, advert_id, nm_id, dt, norm_query)
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_normquery_stats_daily (
    dt              Date,
    updated_at      DateTime DEFAULT now(),
    shop_id         UInt32,
    advert_id       UInt64,
    nm_id           UInt64,
    norm_query      String,

    -- Performance metrics
    views           UInt32 DEFAULT 0,
    clicks          UInt32 DEFAULT 0,
    atbs            UInt32 DEFAULT 0,       -- add to basket (carts)
    orders          UInt32 DEFAULT 0,

    -- Position & Cost (values from API, in kopecks for cpc/cpm)
    avg_pos         Float32 DEFAULT 0,
    cpc             UInt32 DEFAULT 0,       -- kopecks
    cpm             UInt32 DEFAULT 0,       -- kopecks
    ctr             Float32 DEFAULT 0,      -- WB returns ctr*100

    -- Bid snapshot at the time of data collection (kopecks)
    bid_kopecks     UInt32 DEFAULT 0
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, advert_id, nm_id, dt, norm_query)
TTL dt + INTERVAL 1 YEAR;


-- ===================
-- Normquery Bid History Log
-- Stores per-cluster bid snapshots + recommended bid levels.
-- Source: WB API /adv/v0/normquery/get-bids + /api/advert/v0/bids/recommendations
-- Engine: MergeTree (APPEND for time-series tracking)
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.log_normquery_bids (
    timestamp       DateTime DEFAULT now(),
    shop_id         UInt32,
    advert_id       UInt64,
    nm_id           UInt64,
    norm_query      String,
    bid_kopecks     UInt32,

    -- Recommended bids (from /api/advert/v0/bids/recommendations, kopecks)
    reach_max_bid   UInt32 DEFAULT 0,
    reach_med_bid   UInt32 DEFAULT 0,
    reach_min_bid   UInt32 DEFAULT 0,

    -- Campaign-level competitive bids (kopecks)
    competitive_bid UInt32 DEFAULT 0,
    leaders_bid     UInt32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (shop_id, advert_id, nm_id, norm_query, timestamp)
TTL timestamp + INTERVAL 6 MONTH;
