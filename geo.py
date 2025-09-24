import os
import time
import logging
from typing import Optional, Tuple, Dict, List
import asyncio


logger = logging.getLogger("geo")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

_LOCAL_ZIP_DB: Dict[str, Tuple[float, float]] = {}
_LOCAL_ZIP_DB_LOADED: bool = False


def load_local_zip_db(file_path: str) -> Dict[str, Tuple[float, float]]:
    """Load local ZIP centroids from a tab-separated file like data_zip.txt.

    Expected columns (TSV): country, zip, city, state_name, state_abbr, county, county_code, ..., lat, lon, accuracy
    We parse zip at index 1, lat at -3, lon at -2.
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


def _chunk(lst: List[str], n: int) -> List[List[str]]:
    """Yield successive n-sized chunks from list."""
    return [lst[i:i+n] for i in range(0, len(lst), n)]


async def geocode_zip_batch_zipcodebase(
    zip_codes: List[str],
    api_key: str,
    cache_file: str,
    country: str = "US",
    max_codes_per_req: int = 100,  # keep URLs well under typical limits
) -> Dict[str, Optional[Tuple[float, float]]]:
    """
    Geocode multiple US ZIP5 codes using Zipcodebase's /search GET API, checking local cache first.
    - Endpoint: https://app.zipcodebase.com/api/v1/search
    - Auth: apikey query parameter (NOT Authorization header)
    - Params: codes=comma,separated,list, country=US (optional but recommended)
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
    result_dict: Dict[str, Optional[Tuple[float, float]]] = {z: None for z in normalized}
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
    logger.info("Geocoded %d/%d ZIPs in %d ms",
                sum(1 for v in result_dict.values() if v), len(normalized), dur_ms)
    return result_dict


async def geocode_zip_async(
    zip_code: str,
    api_key: str,
    cache_file: str,
    country: str = "US",
) -> Optional[Tuple[str, float, float]]:
    """Geocode a single ZIP code using local dataset, checking cache first."""
    zip_str = str(zip_code).strip()[:5]

    result = await geocode_zip_batch_zipcodebase([zip_str], api_key, cache_file, country=country)
    coords = result.get(zip_str)
    return (zip_str, *coords) if coords else None


def geocode_zip(zip_code: str, country: str = "US") -> Optional[Tuple[float, float]]:
    """Synchronous wrapper for single ZIP geocoding."""
    try:
        res = asyncio.run(geocode_zip_async(zip_code, "", "", country=country))
        return res[1:] if res else None
    except RuntimeError:
        # If already in an event loop (e.g., Jupyter), create a new task
        return asyncio.get_event_loop().run_until_complete(
            geocode_zip_async(zip_code, "", "", country=country)
        )[1:]


def geocode_zip_batch(zip_codes: List[str], country: str = "US") -> Dict[str, Optional[Tuple[float, float]]]:
    """Synchronous wrapper for batch ZIP geocoding."""
    try:
        return asyncio.run(geocode_zip_batch_zipcodebase(zip_codes, "", "", country=country))
    except RuntimeError:
        return asyncio.get_event_loop().run_until_complete(
            geocode_zip_batch_zipcodebase(zip_codes, "", "", country=country)
        )
