-- ═══════════════════════════════════════════════════════════
-- WB: Warehouse Acceptance Coefficients & Tariffs
-- Source: GET /api/tariffs/v1/acceptance/coefficients
-- Each row = one warehouse on one date with acceptance/storage/delivery tariffs
-- ═══════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS mms_analytics.fact_wb_acceptance_tariffs (
    dt Date,
    warehouse_id UInt32,
    warehouse_name String,
    box_type_id UInt8,              -- 2=Короб, 5=Монопаллета, 6=Суперсейф
    coefficient Float32,            -- acceptance coefficient (-1 = free)
    allow_unload UInt8,             -- 1 = можно отгрузить
    is_sorting_center UInt8,        -- 1 = сортировочный центр

    -- Storage tariffs (per liter per day)
    storage_coef String DEFAULT '',
    storage_base_liter String DEFAULT '',
    storage_additional_liter String DEFAULT '',

    -- Delivery tariffs (per liter)
    delivery_coef String DEFAULT '',
    delivery_base_liter String DEFAULT '',
    delivery_additional_liter String DEFAULT '',

    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(dt)
ORDER BY (warehouse_id, box_type_id, dt)
TTL dt + INTERVAL 1 YEAR;
