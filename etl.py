import csv
import os
import random
import re
import sys
from decimal import Decimal, InvalidOperation

import psycopg


def normalize_zip(zip_code: str) -> str:
    if not zip_code:
        return "00000"
    digits = "".join(ch for ch in zip_code if ch.isdigit())
    return digits.zfill(5)[:5]


def to_decimal(value: str) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        cleaned = str(value).replace(",", "").replace("$", "").strip()
        return Decimal(cleaned).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def main() -> int:
    default_csv = "data.csv" if os.path.exists("data.csv") else "sample_prices_ny.csv"
    csv_path = os.getenv("CSV_PATH", sys.argv[1] if len(sys.argv) > 1 else default_csv)
    host = os.getenv("POSTGRES_HOST", "localhost")
    db = os.getenv("POSTGRES_DB", "health")
    user = os.getenv("POSTGRES_USER", "health")
    password = os.getenv("POSTGRES_PASSWORD", "health")

    conn_str = f"dbname={db} user={user} password={password} host={host} port=5432"
    print(f"Connecting to Postgres at {host}…")

    with psycopg.connect(conn_str) as conn:
        conn.execute("SET client_min_messages TO WARNING;")
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            # Heuristic column mapping for common CMS inpatient charges files
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

            print("Upserting providers, prices, and ratings…")
            with conn.cursor() as cur:
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
                        # Extract leading 3-digit code from definition like "470 - ..."
                        m = re.match(r"\s*(\d{3})\b", ms_drg_description)
                        ms_drg_code = m.group(1) if m else "000"
                    ms_drg_code = ms_drg_code.zfill(3)

                    total_discharges = int(row.get(k_disch) or 0) if row.get(k_disch) else None
                    avg_covered_charges = to_decimal(row.get(k_cov) or "")
                    avg_total_payments = to_decimal(row.get(k_tot) or "")
                    avg_medicare_payments = to_decimal(row.get(k_med) or "")

                    # providers
                    cur.execute(
                        """
                        INSERT INTO providers (provider_id, provider_name, city, state, zip)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (provider_id) DO UPDATE
                        SET provider_name = EXCLUDED.provider_name,
                            city = EXCLUDED.city,
                            state = EXCLUDED.state,
                            zip = EXCLUDED.zip
                        """,
                        (provider_id, provider_name, city, state, zip5),
                    )

                    # drg_prices
                    cur.execute(
                        """
                        INSERT INTO drg_prices (
                            provider_id, ms_drg_code, ms_drg_description,
                            total_discharges, avg_covered_charges,
                            avg_total_payments, avg_medicare_payments
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (provider_id, ms_drg_code) DO UPDATE
                        SET ms_drg_description = EXCLUDED.ms_drg_description,
                            total_discharges = EXCLUDED.total_discharges,
                            avg_covered_charges = EXCLUDED.avg_covered_charges,
                            avg_total_payments = EXCLUDED.avg_total_payments,
                            avg_medicare_payments = EXCLUDED.avg_medicare_payments
                        """,
                        (
                            provider_id,
                            ms_drg_code,
                            ms_drg_description,
                            total_discharges,
                            avg_covered_charges,
                            avg_total_payments,
                            avg_medicare_payments,
                        ),
                    )

                    # ratings (mock 1–10)
                    rating = random.randint(1, 10)
                    cur.execute(
                        """
                        INSERT INTO ratings (provider_id, rating)
                        VALUES (%s, %s)
                        ON CONFLICT (provider_id) DO UPDATE SET rating = EXCLUDED.rating
                        """,
                        (provider_id, rating),
                    )

            conn.commit()
            print("Done.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


