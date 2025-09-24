import csv
import os
import random
import re
import sys
import logging
from decimal import Decimal, InvalidOperation
import asyncio

import psycopg
from dotenv import load_dotenv
from geo import geocode_zip, geocode_zip_batch


def normalize_zip(zip_code: str) -> str:
    if not zip_code:
        return "00000"
    digits = "".join(ch for ch in zip_code if ch.isdigit())
    return digits.zfill(5)[:5]


def normalize_ccn(val: str | None) -> str | None:
    """Normalize a Hospital CCN/Provider ID/Facility ID to 6 digits."""
    if not val:
        return None
    digits = "".join(ch for ch in str(val) if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(6)[:6]


DRG_CODE_RE = re.compile(r"\s*(\d{3})\b")
TWOPLACES = Decimal("0.01")


def to_decimal(value: str) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        cleaned = str(value).replace(",", "").replace("$", "").strip()
        return Decimal(cleaned).quantize(TWOPLACES)
    except (InvalidOperation, ValueError):
        return None


def execute_values(cur, sql_stmt: str, rows: list[tuple], page_size: int | None = None) -> None:
    """Lightweight VALUES batcher compatible with psycopg3."""
    def _exec(batch: list[tuple]) -> None:
        if not batch:
            return
        num_columns = len(batch[0])
        placeholders = "(" + ", ".join(["%s"] * num_columns) + ")"
        values_sql = ", ".join([placeholders] * len(batch))
        query = sql_stmt.replace("VALUES %s", f"VALUES {values_sql}")
        params: list = []
        for r in batch:
            params.extend(r)
        cur.execute(query, params)

    if not rows:
        return
    if page_size and page_size > 0 and len(rows) > page_size:
        for i in range(0, len(rows), page_size):
            _exec(rows[i : i + page_size])
    else:
        _exec(rows)


def load_star_ratings_local(logger) -> dict[str, int]:
    """
    Load Medicare Hospital Overall Star Ratings from local data_rating.csv.

    Expected columns:
      - "Facility ID"  (CCN, 6 digits)
      - "Hospital overall rating" (1–5, may be blank or 'Not Available')
    """
    path = os.getenv("STAR_RATINGS_CSV", "data_rating.csv")
    if not os.path.exists(path):
        logger.warning("Local star ratings file not found at %s — no ratings will be loaded.", path)
        return {}

    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames:
            logger.warning("Star ratings CSV appears to have no header: %s", path)
            return {}

        # Map columns using your provided structure; allow a couple variants just in case
        def pick(colnames: list[str], *candidates: str) -> str | None:
            lc = {c.lower(): c for c in colnames}
            for cand in candidates:
                if cand.lower() in lc:
                    return lc[cand.lower()]
            return None

        k_provider = pick(reader.fieldnames, "Facility ID", "Provider ID", "Facility Id", "CMS Certification Number (CCN)")
        k_rating   = pick(reader.fieldnames, "Hospital overall rating", "Overall Rating")

        if not (k_provider and k_rating):
            logger.warning("Star ratings CSV missing required columns: found=%s", reader.fieldnames)
            return {}

        ratings: dict[str, int] = {}
        rows = 0
        good = 0
        for row in reader:
            rows += 1
            ccn = normalize_ccn(row.get(k_provider))
            raw = (row.get(k_rating) or "").strip()
            if not ccn or not raw or raw.lower().startswith("not"):
                continue
            try:
                stars = int(float(raw))
            except ValueError:
                continue
            if 1 <= stars <= 5:
                ratings[ccn] = stars
                good += 1

        logger.info("Loaded %d Medicare star ratings from %s (parsed %d rows)", good, path, rows)
        return ratings


def main() -> int:
    # Load environment variables from a .env file if present
    load_dotenv()
    default_csv = "data.csv" if os.path.exists("data.csv") else "sample_prices_ny.csv"
    csv_path = os.getenv("CSV_PATH", sys.argv[1] if len(sys.argv) > 1 else default_csv)
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "health")
    user = os.getenv("POSTGRES_USER", "health")
    password = os.getenv("POSTGRES_PASSWORD", "health")
    log_level = os.getenv("ETL_LOG_LEVEL", os.getenv("APP_LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(level=getattr(logging, log_level, logging.INFO), format="%(asctime)s %(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    # No external geocoding required; use local data_zip.txt via geo.py

    conn_str = f"dbname={db} user={user} password={password} host={host} port=5432"
    logger.info("Connecting to Postgres at %s", host)

    with psycopg.connect(conn_str) as conn:
        conn.execute("SET client_min_messages TO WARNING;")

        # Load Medicare star ratings from local CSV
        star_ratings = load_star_ratings_local(logger)

        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            # Heuristic column mapping
            col = {k.lower(): k for k in reader.fieldnames or []}

            def pick(*options: str) -> str | None:
                for o in options:
                    if o.lower() in col:
                        return col[o.lower()]
                return None

            k_ccn = pick("Rndrng_Prvdr_CCN", "provider_id", "Provider.Id", "provider_id")
            k_name = pick("Rndrng_Prvdr_Org_Name", "provider_name", "Provider.Name", "name")
            k_city = pick("Rndrng_Prvdr_City", "city")
            k_state = pick("Rndrng_Prvdr_State_Abrvtn", "state")
            k_zip = pick("Rndrng_Prvdr_Zip5", "zip")
            k_drg_code = pick("DRG_Cd", "ms_drg_code", "DRG.Code", "DRG_Code", "drg")
            k_drg_desc = pick(
                "ms_drg_description",
                "ms_drg_definition",
                "DRG.Definition",
                "DRG Definition",
                "DRG_Desc",
            )
            k_disch = pick("Tot_Dschrgs", "total_discharges", "Total Discharges")
            k_cov = pick(
                "Avg_Cvrg_Chrg",
                "average_covered_charges",
                "Average.Covered.Charges",
                "Average Covered Charges",
                "Avg Covered Charges",
            )
            k_tot = pick(
                "Avg_Tot_Pymt_Amt",
                "average_total_payments",
                "Average.Total.Payments",
                "Average Total Payments",
                "Avg Total Payments",
            )
            k_med = pick(
                "Avg_Mdcr_Pymt_Amt",
                "average_medicare_payments",
                "Average.Medicare.Payments",
                "Average Medicare Payments",
                "Avg Medicare Payments",
            )

            if not (k_ccn and k_name and k_zip and k_drg_desc):
                raise SystemExit("CSV missing required columns: provider, zip, or DRG description")

            logger.info("Collecting unique ZIP codes…")
            zip_pending: set[str] = set()
            f.seek(0)  # Reset CSV reader to start
            next(reader)  # Skip header
            for row in reader:
                zip5 = normalize_zip(row.get(k_zip) or "")
                if zip5:
                    zip_pending.add(zip5)

            logger.info("Found %d unique ZIP codes", len(zip_pending))

            # Resolve all ZIP centroids in batches
            total_zips_geocoded = 0
            with conn.cursor() as cur:
                if zip_pending:
                    logger.info("Checking existing ZIP centroids…")
                    cur.execute(
                        "SELECT zip FROM zip_centroids WHERE zip = ANY(%s)",
                        (list(zip_pending),),
                    )
                    present = {row[0] for row in cur.fetchall()}
                    missing = sorted(zip_pending - present)
                    if missing:
                        logger.info("Geocoding %d missing ZIPs…", len(missing))
                        batch_size = 100  # local batch geocoder, not external
                        new_centroids: list[tuple] = []
                        for i in range(0, len(missing), batch_size):
                            batch = missing[i:i + batch_size]
                            geocoded = geocode_zip_batch(batch)
                            missing_in_batch: list[str] = []
                            for z in batch:
                                coords = geocoded.get(z)
                                if coords is None:
                                    missing_in_batch.append(z)
                                    continue
                                lat, lng = coords
                                new_centroids.append((z, lat, lng))
                            if missing_in_batch:
                                sample_n = int(os.getenv("ETL_MISSING_CENTROID_SAMPLE", "15") or 15)
                                sample = ", ".join(missing_in_batch[:sample_n])
                                more = max(0, len(missing_in_batch) - sample_n)
                                trailer = f" (+{more} more)" if more > 0 else ""
                                logger.error(
                                    "Missing centroids for %d ZIPs; examples: %s%s",
                                    len(missing_in_batch), sample, trailer,
                                )
                                raise SystemExit(
                                    f"ERROR: Missing centroids for {len(missing_in_batch)} ZIPs. Examples: {sample}{trailer}. "
                                    "Please ensure the local ZIP dataset contains these codes."
                                )
                        if new_centroids:
                            execute_values(
                                cur,
                                "INSERT INTO zip_centroids (zip, lat, lng) VALUES %s ON CONFLICT (zip) DO NOTHING",
                                new_centroids,
                                page_size=1000,
                            )
                            logger.info("Inserted %d new ZIP centroids", len(new_centroids))
                            total_zips_geocoded = len(new_centroids)

            # Reset CSV reader for main processing
            f.seek(0)
            reader = csv.DictReader(f)

            logger.info("Upserting providers, prices, and ratings…")
            page_size = int(os.getenv("BATCH_SIZE", "2000"))
            logger.info("Batch size set to %d", page_size)
            with conn.cursor() as cur:
                providers_map: dict[str, tuple] = {}
                drg_values: list[tuple] = []
                rating_assigned: set[str] = set()
                total_rows = 0
                total_chunks = 0
                total_providers = 0
                total_drg_rows = 0
                total_ratings = 0
                progress_every = int(os.getenv("PROGRESS_EVERY", "5000"))

                def flush_chunk() -> None:
                    if not (providers_map or drg_values):
                        return
                    logger.debug(
                        "Flushing chunk: providers=%d, drg_rows=%d",
                        len(providers_map),
                        len(drg_values),
                    )
                    nonlocal total_rows, total_chunks, total_providers, total_drg_rows, total_ratings
                    chunk_drg_count = len(drg_values)

                    # Providers upsert
                    if providers_map:
                        execute_values(
                            cur,
                            """
                            INSERT INTO providers (provider_id, provider_name, city, state, zip)
                            VALUES %s
                            ON CONFLICT (provider_id) DO UPDATE
                            SET provider_name = EXCLUDED.provider_name,
                                city = EXCLUDED.city,
                                state = EXCLUDED.state,
                                zip = EXCLUDED.zip
                            """,
                            list(providers_map.values()),
                            page_size=page_size,
                        )
                        logger.debug("Upserted %d providers", len(providers_map))
                        total_providers += len(providers_map)

                    # DRG prices upsert
                    if drg_values:
                        execute_values(
                            cur,
                            """
                            INSERT INTO drg_prices (
                                provider_id, ms_drg_code, ms_drg_description,
                                total_discharges, avg_covered_charges,
                                avg_total_payments, avg_medicare_payments
                            ) VALUES %s
                            ON CONFLICT (provider_id, ms_drg_code) DO UPDATE
                            SET ms_drg_description = EXCLUDED.ms_drg_description,
                                total_discharges = EXCLUDED.total_discharges,
                                avg_covered_charges = EXCLUDED.avg_covered_charges,
                                avg_total_payments = EXCLUDED.avg_total_payments,
                                avg_medicare_payments = EXCLUDED.avg_medicare_payments
                            """,
                            drg_values,
                            page_size=page_size,
                        )
                        logger.debug("Upserted %d drg price rows", len(drg_values))
                        total_drg_rows += len(drg_values)

                    # Insert real Medicare star ratings (1–5) from local CSV, keyed by CCN
                    rating_rows: list[tuple] = []
                    for pid in providers_map.keys():
                        ccn = normalize_ccn(pid)
                        stars = star_ratings.get(ccn) if ccn else None
                        if stars is not None and pid not in rating_assigned:
                            rating_assigned.add(pid)
                            rating_rows.append((pid, stars))

                    if rating_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO ratings (provider_id, rating)
                            VALUES %s
                            ON CONFLICT (provider_id) DO UPDATE SET rating = EXCLUDED.rating
                            """,
                            rating_rows,
                            page_size=page_size,
                        )
                        logger.debug("Inserted/updated %d ratings", len(rating_rows))
                        total_ratings += len(rating_rows)

                    providers_map.clear()
                    drg_values.clear()
                    total_rows += chunk_drg_count
                    total_chunks += 1
                    if total_rows and (total_rows % progress_every == 0 or chunk_drg_count >= page_size):
                        logger.info(
                            "progress rows=%d chunks=%d providers_upserted=%d drg_rows_upserted=%d ratings=%d zips_geocoded=%d",
                            total_rows,
                            total_chunks,
                            total_providers,
                            total_drg_rows,
                            total_ratings,
                            total_zips_geocoded,
                        )

                for row in reader:
                    provider_id = (row[k_ccn] or "").strip()
                    provider_name = (row.get(k_name) or "").strip()
                    city = (row.get(k_city) or "").strip() or None
                    state = (row.get(k_state) or "").strip() or None
                    zip5 = normalize_zip(row.get(k_zip) or "")

                    ms_drg_description = (row[k_drg_desc] or "").strip()
                    if k_drg_code and row.get(k_drg_code):
                        code_raw = (row[k_drg_code] or "").strip()
                        ms_drg_code = code_raw.split(" ")[0]
                    else:
                        m = DRG_CODE_RE.match(ms_drg_description)
                        ms_drg_code = m.group(1) if m else "000"
                    ms_drg_code = ms_drg_code.zfill(3)

                    total_discharges = int(row.get(k_disch) or 0) if row.get(k_disch) else None
                    avg_covered_charges = to_decimal(row.get(k_cov) or "")
                    avg_total_payments = to_decimal(row.get(k_tot) or "")
                    avg_medicare_payments = to_decimal(row.get(k_med) or "")

                    providers_map[provider_id] = (provider_id, provider_name, city, state, zip5)
                    drg_values.append(
                        (
                            provider_id,
                            ms_drg_code,
                            ms_drg_description,
                            total_discharges,
                            avg_covered_charges,
                            avg_total_payments,
                            avg_medicare_payments,
                        )
                    )

                    if len(drg_values) >= page_size:
                        flush_chunk()

                flush_chunk()

            conn.commit()
            logger.info(
                "ETL complete: rows=%d chunks=%d providers_upserted=%d drg_rows_upserted=%d ratings=%d zips_geocoded=%d",
                total_rows,
                total_chunks,
                total_providers,
                total_drg_rows,
                total_ratings,
                total_zips_geocoded,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
