-- M14b: BESS Enhancements schema migration
-- Adds NEM export, solar-only charging, and grid-only charging support
-- Run against existing solar-model-claude PostgreSQL database
--
-- Idempotent: safe to run multiple times (IF NOT EXISTS on each column)

-- run_inputs: BESS charging mode flags
-- bess_solar_only_charging: when True, battery charges only from excess solar
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_solar_only_charging BOOLEAN NOT NULL DEFAULT FALSE;
-- bess_grid_only_charging: when True, battery operates without solar (standalone BESS)
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS bess_grid_only_charging BOOLEAN NOT NULL DEFAULT FALSE;

-- run_results: no new columns needed
-- The bess_dispatch JSONB column (added in M14a) now carries additional fields:
--   - total_export_kwh: total grid export energy (kWh)
--   - total_export_hours: hours with grid export > 0
--   - charging_source: "any", "solar_only", or "grid_only"
--   - solar_only_export_kwh: annual export kWh in solar-only scenario
--   - solar_only_export_credits: annual export credits ($) in solar-only scenario
--   - solar_plus_bess_export_kwh: annual export kWh in solar+BESS scenario
--   - solar_plus_bess_export_credits: annual export credits ($) in solar+BESS scenario
--
-- The bill_savings JSONB column now carries additional fields:
--   - annual_export_kwh: total NEM export energy (kWh)
--   - annual_export_credits: total NEM export credits ($)
--   - nem_true_up_credit: year-end NEM bank cashout ($)
--
-- No ALTER TABLE needed for JSONB columns; new keys are added dynamically.
