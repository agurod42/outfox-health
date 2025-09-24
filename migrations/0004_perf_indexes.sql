BEGIN;

-- Composite indexes to accelerate common filters and ordering
-- 1) Queries often filter by DRG code and order by avg_covered_charges
CREATE INDEX IF NOT EXISTS idx_drg_prices_code_charges
  ON drg_prices (ms_drg_code, avg_covered_charges);

-- 2) When DRG is absent, we still join on provider and order by charges
CREATE INDEX IF NOT EXISTS idx_drg_prices_provider_charges
  ON drg_prices (provider_id, avg_covered_charges);

COMMIT;



