import os
import re
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas import AskRequest, AskResponse, ProviderOut


app = FastAPI(title="Healthcare Cost Navigator", description="Simple API to search providers by DRG and ask NL questions. Swagger available at /docs.")


def _parse_simple_question(question: str) -> dict[str, Any]:
    # Very small heuristic: look for DRG code, radius miles/km, and ZIP5
    drg_match = re.search(r"DRG\s*(\d{3})", question, re.IGNORECASE)
    zip_match = re.search(r"\b(\d{5})\b", question)
    radius_match = re.search(r"(\d{1,3})\s*(?:mi|miles|km)", question, re.IGNORECASE)
    return {
        "drg": drg_match.group(1) if drg_match else None,
        "zip": zip_match.group(1) if zip_match else None,
        "radius_km": float(radius_match.group(1)) if radius_match else None,
    }


async def _query_providers(
    session: AsyncSession,
    drg: str | None,
    zip_code: str | None,
    radius_km: float | None,
) -> list[ProviderOut]:
    # For MVP: radius_km is ignored; we filter by exact ZIP. This is documented in README.
    if not drg and not zip_code:
        raise HTTPException(status_code=400, detail="Please provide at least drg or zip")

    conditions = []
    params: dict[str, Any] = {}

    if drg:
        # Match either code or description via ILIKE
        conditions.append("(dp.ms_drg_code = :drg OR dp.ms_drg_description ILIKE :drg_like)")
        params["drg"] = drg
        params["drg_like"] = f"%{drg}%"

    if zip_code:
        conditions.append("p.zip = :zip")
        params["zip"] = zip_code

    where_sql = " AND ".join(conditions) if conditions else "TRUE"

    sql = f"""
        SELECT
            p.provider_id,
            p.provider_name,
            p.city,
            p.state,
            p.zip,
            dp.ms_drg_code,
            dp.ms_drg_description,
            dp.total_discharges,
            dp.avg_covered_charges,
            dp.avg_total_payments,
            dp.avg_medicare_payments,
            r.rating
        FROM drg_prices dp
        JOIN providers p ON p.provider_id = dp.provider_id
        LEFT JOIN ratings r ON r.provider_id = p.provider_id
        WHERE {where_sql}
        ORDER BY dp.avg_covered_charges ASC NULLS LAST
        LIMIT 100
    """

    rows = (await session.execute(text(sql), params)).mappings().all()
    return [ProviderOut(**dict(row)) for row in rows]


@app.get("/providers", response_model=list[ProviderOut], tags=["providers"])
async def get_providers(
    drg: Annotated[str | None, Query(description="DRG code or description substring")] = None,
    zip: Annotated[str | None, Query(min_length=5, max_length=5, description="ZIP5")] = None,
    radius_km: Annotated[float | None, Query(description="Radius in km (placeholder)")] = None,
    session: AsyncSession = Depends(get_session),
):
    return await _query_providers(session, drg, zip, radius_km)


@app.post("/ask", response_model=AskResponse, tags=["assistant"])
async def ask(body: AskRequest, session: AsyncSession = Depends(get_session)):
    parsed = _parse_simple_question(body.question)
    if not parsed["drg"] or not parsed["zip"]:
        return AskResponse(
            answer=(
                "I couldn't extract a DRG code and ZIP from your question. "
                "Try, for example: 'Who is cheapest for DRG 470 within 25 miles of 10001?'"
            ),
            results=[],
        )

    results = await _query_providers(session, parsed["drg"], parsed["zip"], parsed["radius_km"]) 
    if not results:
        return AskResponse(answer="No providers matched your query.", results=[])

    top = results[0]
    answer = (
        f"Cheapest for DRG {top.ms_drg_code} near {top.zip}: {top.provider_name} "
        f"with avg covered charges ${top.avg_covered_charges:.2f}"
    )
    return AskResponse(answer=answer, results=results)


@app.get("/healthz", tags=["meta"])
async def healthcheck(session: AsyncSession = Depends(get_session)):
    try:
        await session.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"db_unavailable: {exc}")


@app.get("/", include_in_schema=False)
async def root_to_docs():
    return RedirectResponse(url="/docs")

