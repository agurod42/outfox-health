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


class _FakeResult:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows: list[dict[str, Any]] | None = None, *, raise_on_execute: bool = False):
        self._rows = rows or []
        self.raise_on_execute = raise_on_execute
        self.last_sql: str | None = None
        self.last_params: dict[str, Any] | None = None

    async def execute(self, *args, **kwargs):  # pragma: no cover - simple stub
        if self.raise_on_execute:
            raise RuntimeError("boom")
        self.last_sql = str(args[0]) if args else None
        # SQLAlchemy accepts params as positional dict or kwargs
        if kwargs:
            self.last_params = kwargs
        elif len(args) > 1 and isinstance(args[1], dict):
            self.last_params = args[1]
        else:
            self.last_params = {}
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
    sess = _FakeSession([])
    app.dependency_overrides[get_session] = (lambda: (yield sess))  # type: ignore
    res = client.get("/providers", params={"drg": special, "zip": "10001"})
    assert res.status_code == 200
    assert sess.last_sql is not None
    assert "ORDER BY dp.avg_covered_charges ASC NULLS LAST" in sess.last_sql
    assert "LIMIT 100" in sess.last_sql
    # Placeholders should be present; raw special string should not be embedded in SQL
    assert ":drg" in sess.last_sql and ":drg_like" in sess.last_sql
    assert special not in sess.last_sql
    assert sess.last_params is not None
    assert sess.last_params.get("drg") == special
    assert sess.last_params.get("drg_like") == f"%{special}%"


def test_ask_parses_and_answers():
    fake_rows = [
        {
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
        }
    ]
    app.dependency_overrides[get_session] = override_session_with(fake_rows)
    client = TestClient(app)
    res = client.post("/ask", json={"question": "Who is cheapest for DRG 470 within 25 miles of 10001?"})
    assert res.status_code == 200
    payload = res.json()
    assert "Cheapest for DRG 470" in payload["answer"]
    assert len(payload["results"]) == 1


def test_ask_unparseable_message_and_no_results():
    app.dependency_overrides[get_session] = override_session_with([])
    client = TestClient(app)
    # Unparseable
    res = client.post("/ask", json={"question": "hello?"})
    assert res.status_code == 200
    payload = res.json()
    assert "couldn't extract" in payload["answer"].lower()
    assert payload["results"] == []

    # Parsed but no rows
    res = client.post("/ask", json={"question": "Who is cheapest for DRG 470 within 25 miles of 10001?"})
    payload = res.json()
    assert payload["answer"].lower().startswith("no providers")
    assert payload["results"] == []


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


