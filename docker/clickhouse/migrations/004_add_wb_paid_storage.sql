-- Migration 004: Add fact_wb_paid_storage table
-- Source: WB API /api/v1/paid_storage (Paid Storage Report)
-- Contains daily per-SKU per-warehouse storage costs with all discounts

CREATE TABLE IF NOT EXISTS mms_analytics.fact_wb_paid_storage (
    dt                  Date        COMMENT 'Дата начисления',
    shop_id             UInt32      COMMENT 'ID магазина (FK shops)',
    vendor_code         String      COMMENT 'Артикул продавца',
    nm_id               UInt64      COMMENT 'Артикул WB',
    warehouse           String      COMMENT 'Название склада WB',
    office_id           UInt32      COMMENT 'ID склада WB',
    warehouse_coef      Float32     COMMENT 'Коэффициент склада',
    log_warehouse_coef  Float32     COMMENT 'Коэффициент логистики и хранения',
    volume_liters       Float32     COMMENT 'Объём товара, литры',
    calc_type           String      COMMENT 'Способ расчёта (паллеты/короба/скидка)',
    warehouse_price     Decimal(18,6) COMMENT 'Сумма хранения ₽ (отрицательная для скидок!)',
    barcodes_count      UInt32      COMMENT 'Количество штрих-кодов',
    pallet_place_code   UInt64      COMMENT 'Код паллето-места',
    pallet_count        Float32     COMMENT 'Количество паллет',
    original_date       Date        COMMENT 'Дата исходная',
    loyalty_discount    Float32     COMMENT 'Скидка программы лояльности %',
    tariff_fix_date     String      COMMENT 'Дата фиксации тарифа',
    tariff_lower_date   String      COMMENT 'Дата понижения тарифа',
    gi_id               UInt64      COMMENT 'ID поставки',
    barcode             String      COMMENT 'Баркод',
    brand               String      COMMENT 'Бренд',
    subject             String      COMMENT 'Предмет/категория',
    updated_at          DateTime    DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (shop_id, dt, nm_id, office_id, calc_type)
PARTITION BY toYYYYMM(dt)
SETTINGS index_granularity = 8192;
