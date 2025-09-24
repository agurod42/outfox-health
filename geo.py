"""ZIP geocoding utilities backed by a local TSV dataset.

This module provides simple helpers to resolve ZIP5 centroids from a local file
(`data_zip.txt` by default). Public APIs are synchronous wrappers that work in
regular apps and test contexts without requiring network access.
"""

import os
import time
import logging
import asyncio


logger = logging.getLogger("geo")

_LOCAL_ZIP_DB: dict[str, tuple[float, float]] = {}
_LOCAL_ZIP_DB_LOADED: bool = False


def load_local_zip_db(file_path: str) -> dict[str, tuple[float, float]]:
    """Load ZIP centroids from a tab-separated file.

    The file is expected to follow the GeoNames-like format, where columns are:
    country, zip, city, state_name, state_abbr, county, county_code, ..., lat, lon, accuracy.
    We parse zip at index 1, lat at -3, and lon at -2.
    """
    global _LOCAL_ZIP_DB_LOADED, _LOCAL_ZIP_DB
    if _LOCAL_ZIP_DB_LOADED:
        return _LOCAL_ZIP_DB
    path = os.getenv("ZIP_LOCAL_FILE", file_path)
    try:
        if not os.path.exists(path):
            logger.warning("Local ZIP data file not found: %s", path)
            _LOCAL_ZIP_DB_LOADED = True
            return _LOCAL_ZIP_DB
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                parts = s.split("\t")
                if len(parts) < 6:
                    continue
                zip_code = (parts[1] or "").strip()[:5]
                if not zip_code or not zip_code.isdigit():
                    continue
                try:
                    lat = float(parts[-3])
                    lon = float(parts[-2])
                except Exception:
                    continue
                _LOCAL_ZIP_DB[zip_code] = (lat, lon)
        logger.info("Loaded %d ZIP centroids from %s", len(_LOCAL_ZIP_DB), path)
    except Exception as exc:
        logger.warning("Failed to load local ZIP db from %s: %s", path, exc)
    finally:
        _LOCAL_ZIP_DB_LOADED = True
    return _LOCAL_ZIP_DB

async def geocode_zip_batch_zipcodebase(
    zip_codes: list[str],
    country: str = "US",
) -> dict[str, tuple[float, float] | None]:
    """Resolve centroids for multiple ZIP5 codes from the local dataset.

    Note: The parameters related to remote APIs are intentionally removed; we
    only rely on the local data file for deterministic behavior and tests.
    """
    # Normalize input ZIPs (US 5-digit)
    normalized = []
    for z in zip_codes:
        s = str(z).strip()
        if not s:
            continue
        s = s[:5]
        if s.isdigit():
            normalized.append(s)
    if not normalized:
        return {}

    # Initialize result with None for all
    result_dict: dict[str, tuple[float, float] | None] = {z: None for z in normalized}
    missing_zips = normalized[:]

    # Use local dataset instead of remote API
    start = time.perf_counter()
    local_db = load_local_zip_db(os.getenv("ZIP_LOCAL_FILE", "data_zip.txt"))
    for z in missing_zips:
        coords = local_db.get(z)
        if coords:
            result_dict[z] = coords

    # No cache persistence; rely on local dataset only

    dur_ms = int((time.perf_counter() - start) * 1000)
    logger.info(
        "Geocoded %d/%d ZIPs in %d ms",
        sum(1 for v in result_dict.values() if v),
        len(normalized),
        dur_ms,
    )
    return result_dict


async def geocode_zip_async(
    zip_code: str,
    country: str = "US",
) -> tuple[str, float, float] | None:
    """Resolve a single ZIP5 centroid from the local dataset."""
    zip_str = str(zip_code).strip()[:5]

    result = await geocode_zip_batch_zipcodebase([zip_str], country=country)
    coords = result.get(zip_str)
    return (zip_str, *coords) if coords else None


def geocode_zip(zip_code: str, country: str = "US") -> tuple[float, float] | None:
    """Synchronous helper to geocode a single ZIP5."""
    coro = geocode_zip_async(zip_code, country=country)
    try:
        res = asyncio.run(coro)
    except RuntimeError:
        # If we're inside a running loop (e.g., notebooks), use a temporary loop
        current = None
        try:
            current = asyncio.get_event_loop()
        except Exception:
            current = None
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            res = new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
            asyncio.set_event_loop(current)
    return res[1:] if res else None


def geocode_zip_batch(zip_codes: list[str], country: str = "US") -> dict[str, tuple[float, float] | None]:
    """Synchronous helper to geocode multiple ZIP5 values."""
    coro = geocode_zip_batch_zipcodebase(zip_codes, country=country)
    try:
        return asyncio.run(coro)
    except RuntimeError:
        current = None
        try:
            current = asyncio.get_event_loop()
        except Exception:
            current = None
        new_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(new_loop)
            return new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
            asyncio.set_event_loop(current)
