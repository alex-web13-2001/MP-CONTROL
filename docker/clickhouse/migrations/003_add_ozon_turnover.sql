-- Ozon: Item Turnover (оборачиваемость FBO от Ozon API)
-- Source: POST /v1/analytics/item_turnover
-- Снимок раз в день: SKU → days_on_site, оборачиваемость, прогноз дней

CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_turnover (
    dt Date,
    shop_id UInt32,
    sku UInt64,
    product_id UInt64,
    offer_id String,
    product_name String,
    -- Turnover metrics from Ozon
    days_on_site UInt32 DEFAULT 0,          -- Дней товар на площадке
    stock_fbo UInt32 DEFAULT 0,              -- Текущий остаток FBO
    avg_daily_sales Float32 DEFAULT 0,       -- Средние дневные продажи (Ozon-расчёт)
    days_of_supply Float32 DEFAULT 0,        -- На сколько дней хватит (Ozon-расчёт)
    turnover_category String DEFAULT '',     -- Категория оборачиваемости (от Ozon)
    -- Additional data
    revenue_30d Decimal(18, 2) DEFAULT 0,    -- Выручка за 30 дней
    sold_30d UInt32 DEFAULT 0,               -- Продано за 30 дней
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, sku, dt)
TTL dt + INTERVAL 1 YEAR;
