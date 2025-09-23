# Healthcare Cost Navigator

A minimal web service to explore hospital costs and ratings for MS-DRG procedures. Built with **Python 3.11**, **FastAPI**, **async SQLAlchemy**, **PostgreSQL**, and the **OpenAI API** (for NL→SQL) for Outfox Health interview.

---

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

---

## Quickstart

### 1) Environment

```bash
cp .env.example .env
# Set:
# POSTGRES_HOST=postgres
# POSTGRES_DB=health
# POSTGRES_USER=health
# POSTGRES_PASSWORD=health
# OPENAI_API_KEY=sk-...
```

### 2) Docker

```bash
docker-compose up --build
```

### 3) Seed the DB

Place your CSV (e.g., `sample_prices_ny.csv`) at repo root, then:

```bash
python etl.py
```

This:
- Parses the CSV headers listed above
- Casts price fields to `NUMERIC(12,2)`
- Normalizes `DRG_Cd` as **text** (keeps leading zeros)
- Upserts rows into `providers`
- Inserts/updates a mock `ratings` row per `provider_id`

---

## API

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

**Behavior**
- Uses LLM to draft a SQL query template (safe-listed fields only)
- Executes against DB
- Returns grounded results or a helpful out-of-scope message

---

## Local Development

- **Run app**: `uvicorn main:app --reload`
- **Format/Lint**: `ruff check . && ruff format .`
- **Tests** (optional): `pytest -q`

---
