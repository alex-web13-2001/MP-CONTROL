-- PostgreSQL initialization script
-- This file will be executed on first container startup

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    is_superuser BOOLEAN DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Shops table (stores/marketplaces)
CREATE TABLE IF NOT EXISTS shops (
    id SERIAL PRIMARY KEY,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    marketplace VARCHAR(50) NOT NULL, -- 'wildberries', 'ozon', etc.
    api_key VARCHAR(500),
    api_key_encrypted BYTEA,
    client_id VARCHAR(100),  -- Ozon Client-Id
    perf_client_id VARCHAR(100),  -- Ozon Performance Client-Id
    perf_client_secret VARCHAR(500),  -- Ozon Performance Client Secret (plain, deprecated)
    perf_client_secret_encrypted BYTEA,  -- Ozon Performance Client Secret (encrypted)
    is_active BOOLEAN DEFAULT true,
    
    -- Circuit Breaker status
    status VARCHAR(50) DEFAULT 'active',  -- 'active', 'auth_error', 'syncing', 'paused'
    status_message TEXT,  -- User-facing error message
    
    last_sync_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Autobidder settings table
CREATE TABLE IF NOT EXISTS autobidder_settings (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER REFERENCES shops(id) ON DELETE CASCADE,
    campaign_id VARCHAR(100) NOT NULL,
    min_bid DECIMAL(10, 2),
    max_bid DECIMAL(10, 2),
    target_position INTEGER,
    is_enabled BOOLEAN DEFAULT false,
    strategy VARCHAR(50) DEFAULT 'target_position',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(shop_id, campaign_id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_shops_user_id ON shops(user_id);
CREATE INDEX IF NOT EXISTS idx_shops_marketplace ON shops(marketplace);
CREATE INDEX IF NOT EXISTS idx_autobidder_shop_id ON autobidder_settings(shop_id);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_shops_updated_at BEFORE UPDATE ON shops
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_autobidder_updated_at BEFORE UPDATE ON autobidder_settings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Proxy Management (for anti-ban system)
-- ===================
-- Proxies table for rotating IP addresses when scaling to many shops
CREATE TABLE IF NOT EXISTS proxies (
    id SERIAL PRIMARY KEY,
    
    -- Connection info
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    username VARCHAR(255),
    password_encrypted BYTEA,  -- Encrypted like API keys
    
    -- Proxy type
    protocol VARCHAR(20) DEFAULT 'http',  -- 'http', 'https', 'socks5'
    proxy_type VARCHAR(50) DEFAULT 'datacenter',  -- 'datacenter', 'residential', 'mobile'
    
    -- Location (for geo-targeting if needed)
    country VARCHAR(10) DEFAULT 'RU',
    region VARCHAR(100),
    
    -- Health tracking
    status VARCHAR(20) DEFAULT 'active',  -- 'active', 'inactive', 'banned', 'testing'
    success_count INTEGER DEFAULT 0,
    failure_count INTEGER DEFAULT 0,
    last_success_at TIMESTAMP WITH TIME ZONE,
    last_failure_at TIMESTAMP WITH TIME ZONE,
    last_checked_at TIMESTAMP WITH TIME ZONE,
    
    -- Computed success rate (updated by trigger or application)
    success_rate DECIMAL(5, 4) DEFAULT 1.0,  -- 0.0 to 1.0
    
    -- Rate limiting per proxy
    requests_per_minute INTEGER DEFAULT 60,
    current_minute_requests INTEGER DEFAULT 0,
    current_minute_start TIMESTAMP WITH TIME ZONE,
    
    -- Metadata
    provider VARCHAR(100),  -- 'brightdata', 'smartproxy', etc.
    notes TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(host, port)
);

-- Rate limits tracking per shop/marketplace
CREATE TABLE IF NOT EXISTS rate_limits (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER REFERENCES shops(id) ON DELETE CASCADE,
    
    -- Rate limit configuration (per marketplace defaults)
    requests_per_second DECIMAL(5, 2) DEFAULT 3.0,  -- WB default: 3 req/sec
    requests_per_minute INTEGER DEFAULT 100,
    requests_per_hour INTEGER DEFAULT 3000,
    
    -- Current usage tracking
    second_requests INTEGER DEFAULT 0,
    minute_requests INTEGER DEFAULT 0,
    hour_requests INTEGER DEFAULT 0,
    
    -- Timestamps for window tracking
    current_second TIMESTAMP WITH TIME ZONE,
    current_minute TIMESTAMP WITH TIME ZONE,
    current_hour TIMESTAMP WITH TIME ZONE,
    
    -- Backoff tracking
    is_rate_limited BOOLEAN DEFAULT false,
    rate_limit_until TIMESTAMP WITH TIME ZONE,
    consecutive_429_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(shop_id)
);

-- Proxy usage log (for analytics and rotation optimization)
CREATE TABLE IF NOT EXISTS proxy_usage_log (
    id BIGSERIAL PRIMARY KEY,
    proxy_id INTEGER REFERENCES proxies(id) ON DELETE SET NULL,
    shop_id INTEGER REFERENCES shops(id) ON DELETE SET NULL,
    
    -- Request info
    endpoint VARCHAR(255),
    method VARCHAR(10),
    
    -- Response info
    status_code INTEGER,
    response_time_ms INTEGER,
    is_success BOOLEAN,
    error_message TEXT,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status);
CREATE INDEX IF NOT EXISTS idx_proxies_success_rate ON proxies(success_rate DESC);
CREATE INDEX IF NOT EXISTS idx_proxies_type ON proxies(proxy_type);
CREATE INDEX IF NOT EXISTS idx_rate_limits_shop_id ON rate_limits(shop_id);
CREATE INDEX IF NOT EXISTS idx_proxy_usage_log_proxy_id ON proxy_usage_log(proxy_id);
CREATE INDEX IF NOT EXISTS idx_proxy_usage_log_created_at ON proxy_usage_log(created_at);

-- Trigger for proxies updated_at
CREATE TRIGGER update_proxies_updated_at BEFORE UPDATE ON proxies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rate_limits_updated_at BEFORE UPDATE ON rate_limits
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Advertising Event Log (for timeline visualization)
-- ===================
CREATE TABLE IF NOT EXISTS event_log (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    
    shop_id INTEGER NOT NULL,
    advert_id BIGINT NOT NULL,
    nm_id BIGINT,  -- NULL for campaign-level events
    
    event_type VARCHAR(50) NOT NULL,  -- BID_CHANGE, STATUS_CHANGE, ITEM_ADD, ITEM_REMOVE, ITEM_INACTIVE
    old_value TEXT,
    new_value TEXT,
    
    event_metadata JSONB  -- Additional context (campaign_type, reason, etc.)
);

CREATE INDEX IF NOT EXISTS idx_event_log_shop ON event_log(shop_id);
CREATE INDEX IF NOT EXISTS idx_event_log_advert ON event_log(advert_id);
CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_created ON event_log(created_at);

-- ===================
-- Commercial Monitoring: Products Dictionary
-- ===================
CREATE TABLE IF NOT EXISTS dim_products (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    nm_id BIGINT NOT NULL,
    vendor_code VARCHAR(100),
    name VARCHAR(500),
    main_image_url TEXT,
    
    -- Dimensions (for logistics cost calculation)
    length DECIMAL(8, 2),   -- cm
    width DECIMAL(8, 2),    -- cm
    height DECIMAL(8, 2),   -- cm
    
    -- Current pricing (updated every 30 min)
    current_price DECIMAL(12, 2),
    current_discount INTEGER DEFAULT 0,
    
    -- Classification
    category VARCHAR(255),
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE(shop_id, nm_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_products_shop ON dim_products(shop_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_nm ON dim_products(nm_id);
CREATE INDEX IF NOT EXISTS idx_dim_products_vendor ON dim_products(vendor_code);

CREATE TRIGGER update_dim_products_updated_at BEFORE UPDATE ON dim_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Commercial Monitoring: Warehouses Dictionary
-- ===================
CREATE TABLE IF NOT EXISTS dim_warehouses (
    warehouse_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    city VARCHAR(150),
    is_verified BOOLEAN DEFAULT false,  -- false = auto-created from stock response
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_warehouses_name ON dim_warehouses(name);

CREATE TRIGGER update_dim_warehouses_updated_at BEFORE UPDATE ON dim_warehouses
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Content Monitoring: Product Content Hashes (SEO/CTR audit)
-- ===================
-- Stores hash "snapshots" of product content for change detection.
-- Compared nightly against fresh API data to detect:
--   CONTENT_TITLE_CHANGED, CONTENT_DESC_CHANGED,
--   CONTENT_MAIN_PHOTO_CHANGED, CONTENT_PHOTO_ORDER_CHANGED
CREATE TABLE IF NOT EXISTS dim_product_content (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    nm_id BIGINT NOT NULL,

    -- MD5 hashes for fast comparison
    title_hash VARCHAR(32),           -- MD5(title)
    description_hash VARCHAR(32),     -- MD5(description)

    -- Photo tracking
    main_photo_id TEXT,               -- File-ID extracted from WB CDN URL (not full URL)
    photos_hash VARCHAR(32),          -- MD5(sorted photo file-IDs JSON)
    photos_count INTEGER DEFAULT 0,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(shop_id, nm_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_product_content_shop ON dim_product_content(shop_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_content_nm ON dim_product_content(nm_id);

CREATE TRIGGER update_dim_product_content_updated_at BEFORE UPDATE ON dim_product_content
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Ozon: Products Dictionary
-- ===================
CREATE TABLE IF NOT EXISTS dim_ozon_products (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    product_id BIGINT NOT NULL,
    offer_id VARCHAR(100) NOT NULL,
    sku BIGINT,
    name VARCHAR(500),
    main_image_url TEXT,
    barcode VARCHAR(100),
    category_id BIGINT,

    -- Pricing
    price DECIMAL(12, 2),
    old_price DECIMAL(12, 2),
    min_price DECIMAL(12, 2),
    marketing_price DECIMAL(12, 2),

    -- Dimensions
    volume_weight DECIMAL(8, 2),

    -- Stocks
    stocks_fbo INTEGER DEFAULT 0,
    stocks_fbs INTEGER DEFAULT 0,

    -- Flags
    is_archived BOOLEAN DEFAULT FALSE,
    has_fbo_stocks BOOLEAN DEFAULT FALSE,
    has_fbs_stocks BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(shop_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_ozon_products_shop ON dim_ozon_products(shop_id);
CREATE INDEX IF NOT EXISTS idx_dim_ozon_products_product ON dim_ozon_products(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_ozon_products_offer ON dim_ozon_products(offer_id);

CREATE TRIGGER update_dim_ozon_products_updated_at BEFORE UPDATE ON dim_ozon_products
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ===================
-- Ozon: Content Monitoring (MD5 hashes)
-- ===================
CREATE TABLE IF NOT EXISTS dim_ozon_product_content (
    id SERIAL PRIMARY KEY,
    shop_id INTEGER NOT NULL,
    product_id BIGINT NOT NULL,
    title_hash VARCHAR(32),
    description_hash VARCHAR(32),
    main_image_url TEXT,
    images_hash VARCHAR(32),
    images_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(shop_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_ozon_product_content_shop ON dim_ozon_product_content(shop_id);
CREATE INDEX IF NOT EXISTS idx_dim_ozon_product_content_product ON dim_ozon_product_content(product_id);

CREATE TRIGGER update_dim_ozon_product_content_updated_at BEFORE UPDATE ON dim_ozon_product_content
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
