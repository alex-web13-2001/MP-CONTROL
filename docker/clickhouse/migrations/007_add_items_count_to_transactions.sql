-- Add items_count column to fact_ozon_transactions
-- Protection against multi-item transactions (currently Ozon sends 1 item per transaction,
-- but this defends against future API changes)
ALTER TABLE mms_analytics.fact_ozon_transactions
    ADD COLUMN IF NOT EXISTS items_count UInt32 DEFAULT 1;
