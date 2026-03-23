-- Fix: Ozon placement cost report — ensure correct schema.
-- SAFE migration: uses IF NOT EXISTS only, never drops data.
-- Original migration had DROP TABLE which destroyed data on every deploy/restart.

CREATE TABLE IF NOT EXISTS mms_analytics.fact_ozon_placement_cost (
    dt Date,                              -- row date from Excel ("Дата")
    period_from Date,                     -- report period start (requested from)
    period_to Date,                       -- report period end (requested to)
    shop_id UInt32,
    sku UInt64 DEFAULT 0,                 -- Ozon SKU (numeric)
    offer_id String,                      -- Артикул (seller's article)
    product_name String DEFAULT '',       -- Категория товара
    warehouse_name String DEFAULT '',     -- Склад
    product_type String DEFAULT '',       -- Описательный тип
    item_tag String DEFAULT '',           -- Признак товара
    volume_ml Float32 DEFAULT 0,          -- Суммарный объем в миллилитрах
    item_count UInt32 DEFAULT 0,          -- Кол-во экземпляров
    paid_volume_ml Float32 DEFAULT 0,     -- Платный объем в миллилитрах
    paid_item_count UInt32 DEFAULT 0,     -- Кол-во платных экземпляров
    placement_cost Decimal(18, 2),        -- Начисленная стоимость размещения (₽)
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (shop_id, offer_id, warehouse_name, dt);
