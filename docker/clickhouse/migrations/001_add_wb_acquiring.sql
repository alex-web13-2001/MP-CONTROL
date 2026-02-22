-- Migration 001: Add wb_acquiring column to fact_finances
-- Date: 2026-02-22
-- Description: Банковский эквайринг WB (acquiring_fee из API reportDetailByPeriod)

ALTER TABLE mms_analytics.fact_finances
    ADD COLUMN IF NOT EXISTS wb_acquiring Decimal(18,2) DEFAULT 0;

-- Backfill from raw_payload for existing rows
ALTER TABLE mms_analytics.fact_finances
    UPDATE wb_acquiring = JSONExtractFloat(raw_payload, 'acquiring_fee')
    WHERE wb_acquiring = 0
      AND raw_payload != ''
      AND JSONExtractFloat(raw_payload, 'acquiring_fee') > 0;
