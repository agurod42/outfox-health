import os
import time
import logging
from typing import Optional, Tuple

import httpx


logger = logging.getLogger("geo")


def geocode_zip_nominatim(zip_code: str) -> Optional[Tuple[float, float]]:
    """Geocode a US ZIP5 using OpenStreetMap Nominatim.

    Returns (lat, lng) or None if not found. Respects basic etiquette by setting
    a User-Agent that includes contact info when available.
    """
    zip_str = str(zip_code).strip()[:5]
    if not zip_str or not zip_str.isdigit() or len(zip_str) != 5:
        return None

    base_url = os.getenv(
        "GEOCODER_BASE_URL",
        "https://nominatim.openstreetmap.org/search",
    )
    email = os.getenv("GEOCODER_EMAIL", "")
    user_agent = os.getenv(
        "GEOCODER_USER_AGENT",
        f"outfox-health-geo/1.0 ({email})" if email else "outfox-health-geo/1.0",
    )

    params = {
        "postalcode": zip_str,
        "country": "USA",
        "format": "json",
        "limit": 1,
    }
    headers = {"User-Agent": user_agent}

    start = time.perf_counter()
    with httpx.Client(timeout=10.0, headers=headers) as client:
        logger.info("geocode_request zip=%s base_url=%s", zip_str, base_url)
        resp = client.get(base_url, params=params)
        if resp.status_code != 200:
            logger.warning("geocode_http_error zip=%s status=%s", zip_str, resp.status_code)
            return None
        data = resp.json()
        if not data:
            logger.warning("geocode_empty zip=%s", zip_str)
            return None
        try:
            lat = float(data[0]["lat"])  # type: ignore[index]
            lon = float(data[0]["lon"])  # type: ignore[index]
            logger.info(
                "geocode_success zip=%s lat=%s lon=%s dur_ms=%d",
                zip_str,
                f"{lat:.6f}",
                f"{lon:.6f}",
                int((time.perf_counter() - start) * 1000),
            )
            return (lat, lon)
        except Exception as exc:
            logger.warning("geocode_parse_error zip=%s error=%s", zip_str, exc)
            return None


