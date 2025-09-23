from collections.abc import AsyncGenerator
from typing import Any
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Ensure project root is on sys.path when running tests from anywhere
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from main import app
from db import get_session

# Note: ZIP centroid seeding is covered indirectly via ETL; here we assert SQL shape for radius


class _FakeResult:
    def __init__(self, rows: list[Any]):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        class _Scalars:
            def __init__(self, rows: list[Any]):
                self._rows = rows

            def all(self):
                return self._rows

        return _Scalars([r[0] if isinstance(r, (list, tuple)) else r for r in self._rows])


class _FakeSession:
    def __init__(self, rows: list[dict[str, Any]] | None = None, *, raise_on_execute: bool = False, existing_centroids: set[str] | None = None):
        self._rows = rows or []
        self.raise_on_execute = raise_on_execute
        self.last_sql: str | None = None
        self.last_params: dict[str, Any] | None = None
        self.executed_sql: list[str] = []
        self.existing_centroids = existing_centroids or set()

    async def execute(self, *args, **kwargs):  # pragma: no cover - simple stub
        if self.raise_on_execute:
            raise RuntimeError("boom")
        sql = str(args[0]) if args else ""
        self.last_sql = sql
        self.executed_sql.append(sql)
        # SQLAlchemy accepts params as positional dict or kwargs
        if kwargs:
            self.last_params = kwargs
        elif len(args) > 1 and isinstance(args[1], dict):
            self.last_params = args[1]
        else:
            self.last_params = {}

        # Emulate centroid existence check
        if "FROM zip_centroids" in sql and "SELECT 1" in sql:
            z = self.last_params.get("z")
            return _FakeResult([("1",)] if z in self.existing_centroids else [])
        # Emulate distinct zip preload query
        if "SELECT DISTINCT p.zip" in sql:
            return _FakeResult([])
        # Default: return provider rows
        return _FakeResult(self._rows)

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def close(self):
        return None


def override_session_with(rows: list[dict[str, Any]], *, raise_on_execute: bool = False):
    async def _override() -> AsyncGenerator[_FakeSession, None]:
        session = _FakeSession(rows, raise_on_execute=raise_on_execute)
        try:
            yield session
        finally:
            await session.close()

    return _override


@pytest.fixture(autouse=True)
def reset_overrides():
    app.dependency_overrides = {}
    yield
    app.dependency_overrides = {}


def test_healthz_ok():
    app.dependency_overrides[get_session] = override_session_with([])
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 200
    assert res.json() == {"status": "ok"}


def test_root_redirects_to_docs():
    client = TestClient(app)
    res = client.get("/", follow_redirects=False)
    assert res.status_code in (302, 307)
    assert res.headers["location"].endswith("/docs")


def test_providers_requires_params():
    app.dependency_overrides[get_session] = override_session_with([])
    client = TestClient(app)
    res = client.get("/providers")
    assert res.status_code == 400


def test_providers_by_drg_and_zip():
    fake_rows = [
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
            "rating": 8,
        }
    ]
    app.dependency_overrides[get_session] = override_session_with(fake_rows)
    client = TestClient(app)
    res = client.get("/providers", params={"drg": "023", "zip": "36301"})
    assert res.status_code == 200
    data = res.json()
    assert isinstance(data, list) and len(data) == 1
    assert data[0]["provider_id"] == "010001"


def test_providers_by_drg_only_and_zip_only_and_sql_shape():
    fake_rows = []
    # Capture SQL and params used
    override = override_session_with(fake_rows)
    app.dependency_overrides[get_session] = override
    client = TestClient(app)

    # drg-only
    res = client.get("/providers", params={"drg": "CRANIOTOMY"})
    assert res.status_code == 200

    # zip-only
    res = client.get("/providers", params={"zip": "10001"})
    assert res.status_code == 200

    # SQL capture and parameterization check using a new session
    special = "023' OR 1=1 --"
    async def _yield_sess() -> AsyncGenerator[_FakeSession, None]:
        sess = _FakeSession([])
        try:
            yield sess
        finally:
            await sess.close()
    app.dependency_overrides[get_session] = _yield_sess
    res = client.get("/providers", params={"drg": special, "zip": "10001"})
    assert res.status_code == 200
    # Fetch the last yielded session from override by triggering again and capturing
    sess2 = _FakeSession([])
    app.dependency_overrides[get_session] = (lambda: (yield sess2))  # type: ignore
    _ = client.get("/providers", params={"drg": special, "zip": "10001"})
    assert sess2.last_sql is not None
    assert "ORDER BY dp.avg_covered_charges ASC NULLS LAST" in sess2.last_sql
    assert "LIMIT 100" in sess2.last_sql
    # Placeholders should be present; raw special string should not be embedded in SQL
    assert ":drg" in sess2.last_sql and ":drg_like" in sess2.last_sql
    assert special not in sess2.last_sql
    assert sess2.last_params is not None
    assert sess2.last_params.get("drg") == special
    assert sess2.last_params.get("drg_like") == f"%{special}%"


def test_providers_radius_filter_joins_and_params():
    # Provide a fake session to capture SQL with radius; mark src zip as present so geocode is skipped
    sess = _FakeSession([], existing_centroids={"10001"})
    app.dependency_overrides[get_session] = (lambda: (yield sess))  # type: ignore
    client = TestClient(app)
    res = client.get("/providers", params={"drg": "023", "zip": "10001", "radius_km": 25})
    assert res.status_code == 200
    assert sess.last_sql is not None
    # Ensure the join on zip_centroids and haversine filter are present
    assert "JOIN zip_centroids zc_src" in sess.last_sql
    assert "JOIN zip_centroids zc_dest" in sess.last_sql
    assert "haversine_km(" in sess.last_sql
    assert "<= :radius_km" in sess.last_sql
    assert sess.last_params is not None
    assert sess.last_params.get("src_zip") == "10001"
    assert sess.last_params.get("radius_km") == 25


def test_radius_missing_src_centroid_returns_503(caplog):
    # With no centroid present for source ZIP, the API should 503 and log a warning
    from main import app as _app
    sess = _FakeSession([], existing_centroids=set())
    _app.dependency_overrides[get_session] = (lambda: (yield sess))  # type: ignore
    client = TestClient(_app)
    with caplog.at_level("WARNING"):
        res = client.get("/providers", params={"drg": "023", "zip": "99999", "radius_km": 10})
    assert res.status_code == 503
    assert any("missing_centroid" in rec.message or "missing_centroid" in rec.getMessage() for rec in caplog.records)


def test_radius_with_existing_src_centroid_no_inserts():
    from main import app as _app
    # Mark src centroid present; API should not attempt to insert centroids and should succeed
    sess = _FakeSession([], existing_centroids={"10001"})
    _app.dependency_overrides[get_session] = (lambda: (yield sess))  # type: ignore
    client = TestClient(_app)
    res = client.get("/providers", params={"drg": "023", "zip": "10001", "radius_km": 10})
    assert res.status_code == 200
    assert not any("INSERT INTO zip_centroids" in s for s in sess.executed_sql)


def test_ask_parses_and_answers():
    # Monkeypatch LLM to a fixed SQL (new unified path)
    import main as main_module

    def _fake_nl2sql(_q: str, _hints: str):
        return {
            "outcome": "sql",
            "sql": (
                "SELECT p.provider_id, p.provider_name, p.city, p.state, p.zip, "
                "dp.ms_drg_code, dp.ms_drg_description, dp.total_discharges, "
                "dp.avg_covered_charges, dp.avg_total_payments, dp.avg_medicare_payments, r.rating "
                "FROM drg_prices dp JOIN providers p ON p.provider_id = dp.provider_id "
                "LEFT JOIN ratings r ON r.provider_id = p.provider_id "
                "WHERE dp.ms_drg_code = '470' LIMIT 100"
            ),
            "guidance": "",
            "follow_up": "Consider adding a ZIP to narrow further.",
        }

    fake_rows = [{
        "provider_id": "010001",
        "provider_name": "Southeast Health Medical Center",
        "city": "Dothan",
        "state": "AL",
        "zip": "10001",
        "ms_drg_code": "470",
        "ms_drg_description": "MAJOR JOINT REPLACEMENT OR REATTACHMENT ...",
        "total_discharges": 10,
        "avg_covered_charges": 120000.00,
        "avg_total_payments": 20000.00,
        "avg_medicare_payments": 18000.00,
        "rating": 7,
    }]

    main_module.generate_nl2sql = _fake_nl2sql  # type: ignore
    app.dependency_overrides[get_session] = override_session_with(fake_rows)
    client = TestClient(app)
    res = client.post("/ask", json={"question": "Who is cheapest for DRG 470 within 25 miles of 10001?", "include_sql": true})
    assert res.status_code == 200
    payload = res.json()
    assert "Results for:" in payload["answer"]
    assert len(payload["results"]) == 1
    assert "sql" in payload and payload["sql"].startswith("SELECT ")


def test_ask_unparseable_message_and_no_results():
    # LLM returns unsafe SQL
    import main as main_module

    def _unsafe(_q: str, _hints: str):
        return {"outcome": "sql", "sql": "DROP TABLE providers", "guidance": "", "follow_up": ""}

    main_module.generate_nl2sql = _unsafe  # type: ignore
    app.dependency_overrides[get_session] = override_session_with([])
    client = TestClient(app)
    res = client.post("/ask", json={"question": "hello?"})
    assert res.status_code == 200
    assert "Unsafe SQL" in res.json()["answer"]


def test_ask_llm_failure_returns_400():
    import main as main_module

    def _boom(_q: str, _hints: str):
        raise RuntimeError("model timeout")

    main_module.generate_nl2sql = _boom  # type: ignore
    client = TestClient(app)
    res = client.post("/ask", json={"question": "any"})
    assert res.status_code == 200
    assert "NL2SQL failed:" in res.json()["answer"]


def test_ask_disallowed_table_returns_400():
    import main as main_module

    def _bad_table(_q: str, _hints: str):
        return {"outcome": "sql", "sql": "SELECT * FROM users LIMIT 10", "guidance": "", "follow_up": ""}

    main_module.generate_nl2sql = _bad_table  # type: ignore
    client = TestClient(app)
    res = client.post("/ask", json={"question": "any"})
    assert res.status_code == 200
    assert "Unsafe SQL" in res.json()["answer"]


def test_ask_guidance_path():
    import main as main_module

    def _guidance(_q: str, _hints: str):
        return {"outcome": "guidance", "sql": "", "guidance": "Please add a ZIP.", "follow_up": "ZIP?"}

    main_module.generate_nl2sql = _guidance  # type: ignore
    client = TestClient(app)
    res = client.post("/ask", json={"question": "vague"})
    assert res.status_code == 200
    body = res.json()
    assert body["answer"].startswith("Please add a ZIP")
    assert body.get("follow_up") == "ZIP?"


def test_healthz_failure_returns_503():
    app.dependency_overrides[get_session] = override_session_with([], raise_on_execute=True)
    client = TestClient(app)
    res = client.get("/healthz")
    assert res.status_code == 503


def test_docs_and_openapi_available():
    client = TestClient(app)
    res = client.get("/docs")
    assert res.status_code == 200
    assert "Swagger UI" in res.text or "swagger-ui" in res.text.lower()
    res = client.get("/openapi.json")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")


