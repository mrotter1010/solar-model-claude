-- M14a: BESS Dispatch Engine schema migration
-- Run against existing solar-model-claude PostgreSQL database
-- All new columns nullable for backward compatibility with existing rows
--
-- Idempotent: safe to run multiple times (IF NOT EXISTS on each column)
-- The bess_dispatch_required type change is NOT idempotent — it will
-- fail harmlessly if already converted to BOOLEAN.

-- Fix bess_dispatch_required type change (float nullable → boolean not null)
-- Existing float values: NULL or 0.0 → FALSE, >0 → TRUE
ALTER TABLE run_inputs ALTER COLUMN bess_dispatch_required
    TYPE BOOLEAN
    USING CASE
        WHEN bess_dispatch_required IS NOT NULL AND bess_dispatch_required > 0
        THEN TRUE
        ELSE FALSE
    END;
ALTER TABLE run_inputs ALTER COLUMN bess_dispatch_required SET DEFAULT FALSE;
ALTER TABLE run_inputs ALTER COLUMN bess_dispatch_required SET NOT NULL;

-- Add BESS input columns
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_power_mw DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_duration_hr DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_rte_percent DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_min_soc_percent DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_max_soc_percent DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_strategy VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_installed_cost_per_kwh DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_cycles_warranty INTEGER;

-- Add BESS results column (JSONB for query support, matching bill_savings pattern)
ALTER TABLE run_results ADD COLUMN IF NOT EXISTS bess_dispatch JSONB;
