BEGIN;

-- Enable trigram extension for fuzzy/text search
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Trigram indexes for ILIKE/fuzzy matches
CREATE INDEX IF NOT EXISTS idx_drg_prices_drg_description_trgm
  ON drg_prices USING gin (ms_drg_description gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_providers_name_trgm
  ON providers USING gin (provider_name gin_trgm_ops);

COMMIT;


