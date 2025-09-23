BEGIN;

-- Core providers table (CCN as primary key)
CREATE TABLE IF NOT EXISTS providers (
    provider_id TEXT PRIMARY KEY,            -- CMS CCN / Rndrng_Prvdr_CCN
    provider_name TEXT NOT NULL,
    city TEXT,
    state TEXT,
    zip TEXT NOT NULL
);

-- Prices per provider per MS-DRG
CREATE TABLE IF NOT EXISTS drg_prices (
    provider_id TEXT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    ms_drg_code TEXT NOT NULL,               -- keep as TEXT to preserve leading zeros
    ms_drg_description TEXT NOT NULL,
    total_discharges INTEGER,
    avg_covered_charges NUMERIC(12,2),
    avg_total_payments NUMERIC(12,2),
    avg_medicare_payments NUMERIC(12,2),
    PRIMARY KEY (provider_id, ms_drg_code)
);

-- Mock ratings per provider (1–10)
CREATE TABLE IF NOT EXISTS ratings (
    provider_id TEXT PRIMARY KEY REFERENCES providers(provider_id) ON DELETE CASCADE,
    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 10)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_providers_zip ON providers(zip);
CREATE INDEX IF NOT EXISTS idx_drg_prices_provider ON drg_prices(provider_id);
CREATE INDEX IF NOT EXISTS idx_drg_prices_drg_code ON drg_prices(ms_drg_code);
CREATE INDEX IF NOT EXISTS idx_drg_prices_avg_cov_charges ON drg_prices(avg_covered_charges);

COMMIT;


