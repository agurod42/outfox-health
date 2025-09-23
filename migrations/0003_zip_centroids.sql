BEGIN;

-- Centroids per ZIP5; lat/lng in decimal degrees
CREATE TABLE IF NOT EXISTS zip_centroids (
    zip TEXT PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_zip_centroids_lat_lng ON zip_centroids(lat, lng);

-- Haversine distance (km); pure SQL and IMMUTABLE for planner optimizations
CREATE OR REPLACE FUNCTION haversine_km(
    lat1 DOUBLE PRECISION,
    lon1 DOUBLE PRECISION,
    lat2 DOUBLE PRECISION,
    lon2 DOUBLE PRECISION
) RETURNS DOUBLE PRECISION
LANGUAGE sql
IMMUTABLE
AS $$
SELECT 2 * 6371 * ASIN(
  SQRT(
    POWER(SIN(RADIANS((lat2 - lat1) / 2)), 2) +
    COS(RADIANS(lat1)) * COS(RADIANS(lat2)) * POWER(SIN(RADIANS((lon2 - lon1) / 2)), 2)
  )
);
$$;

COMMIT;


