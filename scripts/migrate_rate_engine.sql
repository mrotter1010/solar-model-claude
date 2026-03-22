-- Rate Engine & Load Profile milestone migration
-- Run against existing solar-model-claude PostgreSQL database
-- All columns nullable for backward compatibility with existing rows
--
-- Idempotent: safe to run multiple times (IF NOT EXISTS on each column)

-- run_inputs: bill calculation input parameters
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bill_calculation BOOLEAN;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS rate_file_path VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS utility_name VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS tariff_name VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS load_profile_path VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS load_type VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS annual_consumption_kwh DOUBLE PRECISION;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS peak_demand_kw DOUBLE PRECISION;

-- run_results: bill savings output
ALTER TABLE run_results ADD COLUMN IF NOT EXISTS bill_savings JSONB;
