-- M14e: FTM/Wholesale Dispatch
-- RunInputs
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS dispatch_mode VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS iso VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS lmp_zone VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS lmp_market VARCHAR;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS lmp_year INTEGER;
ALTER TABLE run_inputs ADD COLUMN IF NOT EXISTS ancillary_revenue_per_kw_year FLOAT;

-- RunResults
ALTER TABLE run_results ADD COLUMN IF NOT EXISTS lmp_data TEXT;
ALTER TABLE run_results ADD COLUMN IF NOT EXISTS ftm_economics TEXT;
