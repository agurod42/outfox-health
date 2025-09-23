📌 Requirements (from task description)
Data ingestion
Read data from sample_prices_ny.csv.
Clean and normalize the data:
Handle missing values.
Standardize ZIP codes.
Normalize ms_drg_definition / DRG descriptions.
Load into a database (PostgreSQL).
Database schema
Store provider data: ID, name, city, state, zip code, MS-DRG definition, discharges, cost fields.
Include indexes:
On ZIP codes (for radius queries).
On ms_drg_definition (for text search).
Include ratings table (mock 1–10 values, generated during ETL).
API Endpoints
GET /providers
Parameters: drg, zip, radius_km.
Query by procedure (DRG) + providers within radius.
Sort results by cost (average covered charges).
Return JSON.
POST /ask
Accepts a natural-language question.
Uses OpenAI API to convert NL → SQL.
Executes SQL against the database.
Must ground responses in actual DB results.
Should gracefully handle out-of-scope questions.
AI Assistant
Answer cost queries (e.g., “What’s the cheapest hospital for knee replacement near me?”).
Answer quality queries (e.g., “Which hospitals have the best ratings for heart surgery?”).
Detect out-of-scope (non-medical) queries and reject them politely.
Tech Stack
Python 3.11
FastAPI (async)
async SQLAlchemy
PostgreSQL
OpenAI API (for NL→SQL)
Deployment
Docker Compose setup for PostgreSQL + FastAPI.
ETL script for seeding DB with CSV + ratings.
Bonus / nice-to-have
Use real Medicare ratings instead of mocks (if time).
Add unit tests.
Add error handling.
⚖️ Hard Decisions / Trade-offs (explicitly mentioned)
Simplicity vs. User Experience
Minimal interface (raw HTML/JSON).
Faster to build, but limits user experience.
Scalability
A single providers table is fine for demo but may not scale to national datasets.
Might need sharding/partitioning in real-world usage.
Accuracy
Mock ratings (1–10) are used due to time constraint.
Real CMS/Medicare ratings would be more accurate.
AI Dependency
Using OpenAI API speeds up NL→SQL, but:
Introduces external dependency.
Requires safe-guarding against invalid/unexpected queries.
Timebox
The architecture and implementation are intentionally minimal to fit a 4-hour limit.
Error handling, unit tests, and UI are postponed for later.