-- M14c: BESS Sizing Optimization schema migration
-- Adds sizing optimization input parameters and results storage
-- Run against existing solar-model-claude PostgreSQL database
--
-- Idempotent: safe to run multiple times (IF NOT EXISTS on each column)

-- run_inputs: fix bess_optimization_required type
-- Previously defined as Float/nullable in M14a; now Boolean with default false.
-- The USING clause converts any existing numeric values to boolean:
-- non-zero/non-null → true, zero/null → false.
ALTER TABLE run_inputs
    ALTER COLUMN bess_optimization_required TYPE boolean
    USING bess_optimization_required IS NOT NULL AND bess_optimization_required != 0;
ALTER TABLE run_inputs
    ALTER COLUMN bess_optimization_required SET DEFAULT false;

-- run_inputs: BESS sizing sweep range parameters
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_power_min_mw FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_power_max_mw FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_duration_min_hr FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_duration_max_hr FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_opex_per_kw_year FLOAT;

-- run_inputs: project economics parameters
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS discount_rate_pct FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS project_lifetime_years INTEGER;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS rate_escalation_pct FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS solar_cost_per_kw_dc FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS solar_cost_per_kw_ac FLOAT;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS solar_opex_per_kw_dc_year FLOAT;

-- run_results: BESS sizing optimization results (JSON)
-- Stores the entire summary["bess_sizing"] dict including:
--   - optimal_power_mw, optimal_duration_hr, optimal_capacity_kwh
--   - bess_npv, total_project_npv, system_lcoe_per_kwh
--   - total_installed_cost, solar_cost, bess_cost
--   - total_annual_savings, bess_incremental_savings
--   - annual_production_mwh, lifetime_generation_mwh
--   - combos_evaluated, project_lifetime_years, discount_rate_pct, rate_escalation_pct
--   - sweep_results (list of per-combo dicts)
ALTER TABLE run_results ADD COLUMN IF NOT EXISTS bess_sizing JSON;
