-- ===================
-- Migration 010: Add spend, shks, cpc_rub, cpm_rub to fact_normquery_stats_daily
-- 
-- WB API /adv/v0/normquery/stats returns:
--   spend, cpc, cpm — all in RUBLES (currency: RUB)
--   shks — ordered items count
-- Original table has cpc/cpm as UInt32 (kopecks) which is wrong.
-- Adding new Decimal columns for correct values.
-- ===================

ALTER TABLE mms_analytics.fact_normquery_stats_daily
    ADD COLUMN IF NOT EXISTS spend Decimal(18, 2) DEFAULT 0;

ALTER TABLE mms_analytics.fact_normquery_stats_daily
    ADD COLUMN IF NOT EXISTS shks UInt32 DEFAULT 0;

ALTER TABLE mms_analytics.fact_normquery_stats_daily
    ADD COLUMN IF NOT EXISTS cpc_rub Decimal(18, 2) DEFAULT 0;

ALTER TABLE mms_analytics.fact_normquery_stats_daily
    ADD COLUMN IF NOT EXISTS cpm_rub Decimal(18, 2) DEFAULT 0;
