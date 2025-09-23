# Healthcare Cost Navigator

![CI](https://github.com/agurod42/outfox-health/actions/workflows/ci.yml/badge.svg)

A minimal web service to explore hospital costs and ratings for MS-DRG procedures. Built with **Python 3.11**, **FastAPI**, **async SQLAlchemy**, **PostgreSQL**, and the **OpenAI API** (for NL→SQL) for Outfox Health interview.

## Features

- **AI assistant**
  - Answer cost-related queries (e.g., cheapest provider near a ZIP)
  - Answer quality-related queries (e.g., best-rated hospitals for a DRG)
  - Handle out-of-scope questions appropriately
- **Database search**
  - Support searching for hospitals offering a given MS-DRG within a radius of a ZIP code and viewing estimated prices and quality signals
- **ETL script (etl.py)**
  - Reads the provided CSV file
  - Cleans the data as needed
  - Loads data into PostgreSQL tables
- **REST API endpoints**
  - `GET /providers` — Search hospitals by DRG, ZIP code, and `radius_km`
    - Returns hospitals sorted by average covered charges
    - Implements DRG description matching using ILIKE or fuzzy search
  - `POST /ask` — Natural language interface
    - Accepts questions like "Who is cheapest for DRG 470 within 25 miles of 10001?"
    - Uses OpenAI to convert natural language to SQL queries
    - Returns grounded answers based on database results

---

## Architecture & decisions

```
[Client]
   |
   v
[FastAPI App]
   |-- GET /providers ----> [PostgreSQL]
   |                         ├─ providers
   |                         └─ ratings
   |
   |-- POST /ask ----------> [OpenAI API]
                             ├─ NL → SQL (guarded)
                             └─ Execute on Postgres (grounded answers)

[ETL Script]
   ├─ Read CSV
   ├─ Clean/normalize → cast numerics, trim strings, keep DRG codes as text
   ├─ Upsert into providers
   └─ Seed ratings (1–10 mock)
```

**Design choices:**
- Keep schema normalized for clear joins and fast filters on ZIP + DRG
- Use text search on `ms_drg_description` for fuzzy matching
- Use ZIP as coarse geospatial proxy (ZIP centroid + radius can be added later)
- NL→SQL only for convenience; **all answers must be backed by DB results**

**Trade-offs & future work:**
- **Caching**: add Redis for hot queries
- **Geospatial**: current approach filters by ZIP; add ZIP-centroid + Haversine or PostGIS for true radius
- **Observability**: add basic metrics/tracing; pydantic-settings for config
- **Safety**: strict NL→SQL allow-list; parameterized queries only

Production note: In a real deployment, NL→SQL should be served by an internal
service backed by an offline/open-source model (e.g., a fine-tuned Llama
variant) to avoid sending sensitive data to third-party providers and to keep
data private (also improves latency/cost control). The external API use here is
for demo purposes only.

---

## Quickstart

### 0) Python virtual environment (macOS/Linux)

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 1) Environment

```bash
cp env.sample .env
# Edit .env with database vars and OPENAI_API_KEY (optional)
```

### 2) Docker

```bash
docker-compose up --build
```

Swagger UI: `http://localhost:8000/docs`

### 3) Seed the DB

Download and load the [dataset](https://catalog.data.gov/dataset/medicare-inpatient-hospitals-by-provider-and-service-9af02/resource/e51cf14c-615a-4efe-ba6b-3a3ef15dcfb0).

Option A — local Python (uses `.env` DB settings):
```bash
curl -L "https://data.cms.gov/sites/default/files/2024-05/7d1f4bcd-7dd9-4fd1-aa7f-91cd69e452d3/MUP_INP_RY24_P03_V10_DY22_PrvSvc.CSV" -o data.csv
CSV_PATH=data.csv python etl.py
```

What the ETL does:
- Casts price fields to `NUMERIC(12,2)`
- Inserts/updates a mock `ratings` row per `provider_id`
- Normalizes `DRG_Cd` as **text** (keeps leading zeros)
- Parses the CSV headers listed above
- Seeds `zip_centroids` for all ZIPs in `data.csv` (best-effort)
- Upserts rows into `providers`

### 4) Run tests

```bash
pytest -q
```
Tests stub the database dependency; a running DB is not required.

---

## API

### Swagger/OpenAPI

- Swagger UI: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`
- Root `/` redirects to the Swagger UI

### `GET /providers`

**Query params**
- `drg` (string, e.g., `023` or part of description)
- `zip` (string ZIP5, e.g., `10001`)
- `radius_km` (number, optional; ZIP-centroid radius if enabled later, for now filters by exact ZIP when radius not configured)

**Example**
```bash
curl "http://localhost:8000/providers?drg=023&zip=36301&radius_km=40"
```

**Response (shape)**
```json
[
  {
    "provider_id": "010001",
    "provider_name": "Southeast Health Medical Center",
    "city": "Dothan",
    "state": "AL",
    "zip": "36301",
    "ms_drg_code": "023",
    "ms_drg_description": "CRANIOTOMY WITH ...",
    "total_discharges": 25,
    "avg_covered_charges": 158541.64,
    "avg_total_payments": 37331.00,
    "avg_medicare_payments": 35332.96,
    "rating": 8
  }
]
```

### `POST /ask`

**Body**
```json
{ "question": "Who has the best ratings for DRG 023 near 36301?" }
```

**Example**
```bash
curl -X POST "http://localhost:8000/ask" \
  -H "Content-Type: application/json" \
  -d '{"question":"Who is cheapest for DRG 470 within 25 miles of 10001?"}'
```

Healthcheck:
```bash
curl http://localhost:8000/healthz
```

**Behavior**
- Uses LLM to draft a SQL query template (safe-listed fields only)
- Executes against DB
- Returns grounded results or a helpful out-of-scope message
