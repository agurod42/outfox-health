import os
import re
import logging
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db import get_session
from schemas import AskRequest, AskResponse, ProviderOut
from openai import OpenAI
import httpx
import json


app = FastAPI(title="Healthcare Cost Navigator", description="Simple API to search providers by DRG and ask NL questions. Swagger available at /docs.")

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)


@app.on_event("startup")
async def _configure_logging():
    # Ensure our custom loggers emit to stdout in Uvicorn
    level_name = os.getenv("APP_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    for name in ("app", "geo"):
        lg = logging.getLogger(name)
        lg.setLevel(level)
        if not lg.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(level)
            handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
            lg.addHandler(handler)
        lg.propagate = True
    # Optionally raise uvicorn access log level as well
    logging.getLogger("uvicorn").setLevel(level)
    logging.getLogger("uvicorn.error").setLevel(level)
    logging.getLogger("uvicorn.access").setLevel(level)


ALLOWED_TABLES = {"providers", "drg_prices", "ratings", "zip_centroids"}
FORBIDDEN_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "create", "grant", "revoke", "truncate",
    # Block ad-hoc spherical formulas that often cause domain errors; require haversine_km instead
    "acos(", "radians(", "sin(", "cos("
}


def _sql_is_safe(sql: str) -> tuple[bool, str]:
    s = sql.strip().strip(";").lower()
    if not s.startswith("select"):
        return False, "Only SELECT statements are allowed"
    if any(kw in s for kw in FORBIDDEN_KEYWORDS):
        return False, "Statement contains forbidden keywords"
    # Require canonical aliases when those aliases are referenced
    if "dp." in s and not re.search(r"\b(from|join)\s+drg_prices\s+dp\b", s):
        return False, "Missing FROM/JOIN for alias 'dp' (drg_prices dp)"
    if "p." in s and not re.search(r"\b(from|join)\s+providers\s+p\b", s):
        return False, "Missing FROM/JOIN for alias 'p' (providers p)"
    if "r." in s and not re.search(r"\bjoin\s+ratings\s+r\b", s):
        return False, "Missing JOIN for alias 'r' (ratings r)"
    # Check referenced tables are allow-listed
    tables = set(re.findall(r"\bfrom\s+([a-z_][a-z0-9_\.]+)|\bjoin\s+([a-z_][a-z0-9_\.]+)", s))
    flat = set([t for pair in tables for t in pair if t])
    for t in flat:
        # strip optional schema
        tname = t.split(".")[-1]
        if tname not in ALLOWED_TABLES:
            return False, f"Table '{tname}' is not allowed"
    return True, ""


def _schema_prompt() -> str:
    return (
        "Tables and columns:\n"
        "providers p (provider_id TEXT PK, provider_name TEXT, city TEXT, state TEXT, zip TEXT)\n"
        "drg_prices dp (provider_id TEXT FK->providers, ms_drg_code TEXT, ms_drg_description TEXT, total_discharges INT, avg_covered_charges NUMERIC, avg_total_payments NUMERIC, avg_medicare_payments NUMERIC)\n"
        "ratings r (provider_id TEXT FK->providers, rating INT[1-10])\n"
        "zip_centroids(zip TEXT PK, lat DOUBLE, lng DOUBLE)\n"
        "haversine_km(lat1, lon1, lat2, lon2) RETURNS DOUBLE PRECISION (available)\n"
        "Rules: Use only SELECT. Always alias tables as providers p, drg_prices dp, ratings r. "
        "You may JOIN providers/drg_prices/ratings. For distance filters, ALWAYS use haversine_km with two joins to zip_centroids: "
        "JOIN zip_centroids zc_src ON zc_src.zip = '<SOURCE_ZIP>' and JOIN zip_centroids zc_dest ON zc_dest.zip = p.zip, then haversine_km(zc_src.lat, zc_src.lng, zc_dest.lat, zc_dest.lng) <= radius_km. Do not use acos/sin/cos/radians. Limit results to 100."
    )


def _extract_json_obj(text: str) -> dict[str, Any]:
    # Remove fences and try to find a JSON object
    t = text.strip().strip("`")
    start = t.find('{')
    end = t.rfind('}')
    if start != -1 and end != -1 and end > start:
        t = t[start:end+1]
    return json.loads(t)


def _build_hints(question: str | None, drg: str | None, zip_code: str | None, radius_km: float | None) -> str:
    parts: list[str] = []
    if drg:
        parts.append(f"drg={drg}")
    if zip_code:
        parts.append(f"zip={zip_code}")
    if radius_km:
        parts.append(f"radius_km={radius_km}")
    # Light extraction from question if provided (non-binding hints)
    if question:
        m_zip = re.search(r"\b(\d{5})\b", question)
        if not zip_code and m_zip:
            parts.append(f"zip_hint={m_zip.group(1)}")
        m_drg = re.search(r"\bdrg\s*(\d{3})\b", question, re.IGNORECASE)
        if not drg and m_drg:
            parts.append(f"drg_hint={m_drg.group(1)}")
        m_rad = re.search(r"(\d{1,3})\s*(km|kilometers|mi|miles)", question, re.IGNORECASE)
        if not radius_km and m_rad:
            val = float(m_rad.group(1))
            unit = m_rad.group(2).lower()
            km = val * 1.60934 if unit in ("mi", "miles") else val
            parts.append(f"radius_km_hint={km:.0f}")
        # Try to capture a free-text procedure phrase after 'for'
        m_proc = re.search(r"\bfor\s+([a-z][a-z\s/-]{3,40}?)(?:\s+(near|within|in|around|close|by)\b|$)", question, re.IGNORECASE)
        if m_proc and not m_drg:
            phrase = m_proc.group(1).strip()
            parts.append(f"drg_text_hint={phrase}")
        # Intent hint: cost vs quality
        lower_q = question.lower()
        cost_terms = ("cheap", "cheapest", "price", "prices", "cost", "costs", "affordable")
        quality_terms = ("best", "rating", "ratings", "quality", "top")
        if any(t in lower_q for t in cost_terms):
            parts.append("intent_hint=cost")
        elif any(t in lower_q for t in quality_terms):
            parts.append("intent_hint=quality")
    return ", ".join(parts)


def generate_nl2sql(question: str, hints: str) -> dict[str, str]:
    """Single LLM call that can return either SQL or user guidance.

    Returns dict with keys: outcome ('sql'|'guidance'), sql, guidance.
    """
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY not configured")

    client = OpenAI()
    system = (
        "You are Healthcare Cost Navigator’s assistant. You help users find hospitals and prices for MS-DRG procedures "
        "by DRG code/description (optional), ZIP (optional), and distance (optional). DRG is optional: if missing, "
        "return a general list of providers (e.g., within the area) ordered by lowest avg_covered_charges using drg_prices. "
        "If the user asks about COST/cheapest/price, prefer ORDER BY dp.avg_covered_charges ASC. If the user asks about QUALITY/best-rated, prefer ORDER BY r.rating DESC NULLS LAST, then dp.avg_covered_charges ASC as a tie-breaker. "
        "Decide: if you can produce a safe SQL SELECT for the schema below, return it; otherwise return a short guidance "
        "message (no technical terms). Output strict JSON only with keys: {\"outcome\": \"sql|guidance\", \"sql\": string, \"guidance\": string, \"follow_up\": string}. "
        "When outcome=sql, sql must start with SELECT and be limited to 100 rows. If DRG is absent, you may aggregate to one row per provider (e.g., MIN(avg_covered_charges)). "
        "If DRG is free text (not a 3-digit code), use ms_drg_description ILIKE '%text%' to filter. "
        "For distance, ALWAYS use haversine_km with JOIN zip_centroids zc_src ON zc_src.zip = '<SOURCE_ZIP>' (from Hints or user text) AND JOIN zip_centroids zc_dest ON zc_dest.zip = p.zip; then filter haversine_km(zc_src.lat, zc_src.lng, zc_dest.lat, zc_dest.lng) <= <RADIUS_KM>. Do not use acos/sin/cos/radians. "
        "Also include a follow_up string suggesting an extra filter (like DRG) if missing."
    )
    user = (
        f"Schema:\n{_schema_prompt()}\n\n"
        f"User request: {question}\n"
        f"Hints: {hints}\n\n"
        "Produce the JSON described above only."
    )
    logger.info("nl2sql_llm_call model=%s", os.getenv("OPENAI_MODEL", "gpt-4o"))
    resp = client.chat.completions.create(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.0,
    )
    content = resp.choices[0].message.content or "{}"
    logger.debug("nl2sql_llm_raw content_preview=%r", (content[:240] + '…') if len(content or '') > 240 else content)
    try:
        obj = _extract_json_obj(content)
        outcome = str(obj.get("outcome", "")).lower()
        sql = str(obj.get("sql", ""))
        guidance = str(obj.get("guidance", ""))
        follow_up = str(obj.get("follow_up", ""))
        if outcome not in {"sql", "guidance"}:
            raise ValueError("invalid outcome")
        if outcome == "sql":
            logger.info("nl2sql_parsed outcome=sql sql_preview=%s", sql)
        else:
            logger.info("nl2sql_parsed outcome=guidance follow_up=%s", follow_up)
        return {"outcome": outcome, "sql": sql, "guidance": guidance, "follow_up": follow_up}
    except Exception as exc:
        # If parsing fails, treat as guidance path with generic message
        logger.warning("nl2sql_parse_error error=%s content_preview=%s", exc, content)
        return {
            "outcome": "guidance",
            "sql": "",
            "guidance": (
                "I couldn’t understand the request. Try including a DRG code (or a procedure keyword), "
                "your ZIP code, and an optional distance. For example: ‘Cheapest hospitals for DRG 470 within 25 km of 10001’."
            ),
            "follow_up": "Could you share a DRG code and your ZIP code?",
        }


def generate_guidance_from_question(question: str, reason: str) -> str:
    """Ask the LLM to return a short, user-facing guidance message.

    The guidance should avoid technical terms (no mentions of SQL or queries) and
    should be grounded in what this system does: help users find hospitals and
    costs for MS-DRG procedures by location and optional radius. Include
    1–2 concrete example questions the user can copy.

    If the LLM call fails, return a static fallback string.
    """
    try:
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY not configured")
        client = OpenAI()
        system = (
            "You are Healthcare Cost Navigator’s assistant. Your job is to help the "
            "user find hospitals and prices for MS-DRG procedures based on a DRG code "
            "or description, a ZIP code, and optionally a distance. Reply in plain "
            "language; do not mention databases, SQL, or technical details. Keep it "
            "brief and actionable."
        )
        user = (
            f"We couldn't answer because: {reason}.\n"
            f"User asked: {question}\n\n"
            "Write 1–3 short sentences guiding the user on what to include (e.g., DRG code or keyword, "
            "ZIP, and optional distance). Then provide two concrete example questions that fit this app."
        )
        resp = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
            temperature=0.2,
        )
        msg = (resp.choices[0].message.content or "").strip()
        return msg
    except Exception:
        return (
            "I couldn’t understand the request in the context of finding hospitals and costs. "
            "Try including a DRG code (or a procedure keyword), your ZIP code, and an optional distance. "
            "For example: ‘Cheapest hospitals for DRG 470 within 25 km of 10001’ or ‘Best-rated hospitals for knee replacement near 94103’."
        )


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

    if zip_code and not (radius_km and radius_km > 0):
        # Only constrain by exact ZIP when a radius is NOT provided
        conditions.append("p.zip = :zip")
        params["zip"] = zip_code

    where_sql = " AND ".join(conditions) if conditions else "TRUE"

    distance_join = ""
    distance_select = ""
    distance_condition = ""
    if zip_code and radius_km and radius_km > 0:
        # Require that a centroid exists for the source ZIP
        src_exists = (
            await session.execute(text("SELECT 1 FROM zip_centroids WHERE zip = :z"), {"z": zip_code})
        ).first()
        if not src_exists:
            logger.warning("missing_centroid src_zip=%s", zip_code)
            raise HTTPException(status_code=503, detail="zip centroid not available")

        # Join only against existing destination centroids
        # Join centroids and filter by Haversine distance
        distance_join = (
            "JOIN zip_centroids zc_src ON zc_src.zip = :src_zip "
            "JOIN zip_centroids zc_dest ON zc_dest.zip = p.zip "
        )
        distance_select = ", haversine_km(zc_src.lat, zc_src.lng, zc_dest.lat, zc_dest.lng) AS distance_km"
        distance_condition = " AND haversine_km(zc_src.lat, zc_src.lng, zc_dest.lat, zc_dest.lng) <= :radius_km"
        params["src_zip"] = zip_code
        params["radius_km"] = radius_km

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
            r.rating{distance_select}
        FROM drg_prices dp
        JOIN providers p ON p.provider_id = dp.provider_id
        {distance_join}
        LEFT JOIN ratings r ON r.provider_id = p.provider_id
        WHERE {where_sql}{distance_condition}
        ORDER BY dp.avg_covered_charges ASC NULLS LAST
        LIMIT 100
    """

    rows = (await session.execute(text(sql), params)).mappings().all()
    return [ProviderOut(**dict(row)) for row in rows]


@app.get("/providers", response_model=list[ProviderOut], tags=["providers"])
async def get_providers(
    drg: Annotated[str | None, Query(description="DRG code or description substring (optional)")] = None,
    zip: Annotated[str | None, Query(min_length=5, max_length=5, description="ZIP5 (optional)")] = None,
    radius_km: Annotated[float | None, Query(description="Radius in km (optional)")] = None,
    session: AsyncSession = Depends(get_session),
):
    return await _query_providers(session, drg, zip, radius_km)


@app.post("/ask", response_model=AskResponse, tags=["assistant"])
async def ask(body: AskRequest, session: AsyncSession = Depends(get_session)):
    # 1) Ask LLM once for either SQL or guidance
    try:
        hints = _build_hints(body.question, None, None, None)
        result = generate_nl2sql(body.question or "", hints)
    except Exception as exc:
        # Model failed hard; return explicit error
        return AskResponse(answer=f"NL2SQL failed: {exc}", results=[])

    if result.get("outcome") != "sql":
        return AskResponse(
            answer=result.get("guidance", "Please provide DRG, ZIP, and optional distance."),
            results=[],
            follow_up=result.get("follow_up") or None,
            sql=result.get("sql") if (body.include_sql or False) else None,
        )

    sql = result.get("sql", "").strip()
    # 2) Validate SQL against allow-list
    ok, reason = _sql_is_safe(sql)
    if not ok:
        return AskResponse(
            answer=f"Unsafe SQL: {reason}",
            results=[],
            follow_up=None,
            sql=sql if (body.include_sql or False) else None,
        )

    # 3) Execute and return grounded result
    try:
        rows = (await session.execute(text(sql))).mappings().all()
    except Exception as exc:
        return AskResponse(answer=f"SQL execution error: {exc}", results=[], follow_up=None)

    return AskResponse(
        answer=f"Results for: {body.question or ''}",
        results=[ProviderOut(**dict(r)) for r in rows if r],
        follow_up=result.get("follow_up") or None,
        sql=sql if (body.include_sql or False) else None,
    )


@app.get("/healthz", tags=["meta"])
async def healthcheck(session: AsyncSession = Depends(get_session)):
    try:
        await session.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"db_unavailable: {exc}")


@app.get("/", include_in_schema=False)
async def root_to_docs():
    return RedirectResponse(url="/app")


# Serve a minimal static frontend for demo usability
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    # In environments without a working directory, skip mounting
    logger.warning("static_mount_failed")


@app.get("/app", include_in_schema=False)
async def frontend_app():
    # Serve the minimal UI
    index_path = os.path.join(os.getcwd(), "static", "index.html")
    if not os.path.exists(index_path):
        raise HTTPException(status_code=404, detail="frontend not found")
    return FileResponse(index_path)

