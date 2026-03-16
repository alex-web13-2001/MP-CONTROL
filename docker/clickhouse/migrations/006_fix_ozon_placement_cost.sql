-- Fix: Ozon placement cost report contains daily rows per SKU per warehouse.
-- Need to add warehouse_name, category, item_count, paid_volume columns.
-- Also need to change ORDER BY to include warehouse_name and dt properly.
-- Since we can't change ORDER BY on existing ReplacingMergeTree,
-- we drop and recreate the table with correct schema.

DROP TABLE IF EXISTS mms_analytics.fact_ozon_placement_cost;

CREATE TABLE mms_analytics.fact_ozon_placement_cost (
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
