-- ===================
-- Advertising phrases statistics
-- Универсальная таблица для Ozon и WB v3 поисковых запросов
-- Engine: ReplacingMergeTree (deduplicates by shop_id, marketplace, campaign_id, dt, phrase)
-- ===================
CREATE TABLE IF NOT EXISTS mms_analytics.fact_advert_phrases_daily (
    dt              Date,
    updated_at      DateTime DEFAULT now(),
    shop_id         UInt32,
    marketplace     Enum8('wildberries' = 1, 'ozon' = 2),
    campaign_id     UInt64,
    
    -- Keyword metrics
    phrase          String,
    views           UInt32 DEFAULT 0,
    clicks          UInt32 DEFAULT 0,
    ctr             Decimal(18, 2) DEFAULT 0,
    
    -- Conversions & Spend
    orders          UInt32 DEFAULT 0,
    revenue         Decimal(18, 2) DEFAULT 0,
    spend           Decimal(18, 2) DEFAULT 0
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, marketplace, campaign_id, dt, phrase)
TTL dt + INTERVAL 1 YEAR;
