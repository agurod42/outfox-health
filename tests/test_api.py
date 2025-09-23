from collections.abc import AsyncGenerator
from typing import Any

import pytest
from fastapi.testclient import TestClient

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
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self._rows = rows or []

    async def execute(self, *args, **kwargs):  # pragma: no cover - simple stub
        return _FakeResult(self._rows)

    async def commit(self):
        return None

    async def rollback(self):
        return None

    async def close(self):
        return None


def override_session_with(rows: list[dict[str, Any]]):
    async def _override() -> AsyncGenerator[_FakeSession, None]:
        session = _FakeSession(rows)
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
    res = client.get("/", allow_redirects=False)
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


