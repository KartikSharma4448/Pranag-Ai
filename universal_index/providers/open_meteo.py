from __future__ import annotations

from typing import Any

import requests

from universal_index.config import OPEN_METEO_BASE_URL, LIVE_CONTEXT_TIMEOUT_SECONDS


def fetch_open_meteo_climate_context(lat: float, lon: float) -> dict[str, Any] | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m",
        "daily": "temperature_2m_max,precipitation_sum",
        "forecast_days": 1,
        "timezone": "auto",
    }

    session = requests.Session()
    session.headers.update({"User-Agent": "universal-index-free-context/0.1"})

    try:
        response = session.get(OPEN_METEO_BASE_URL, params=params, timeout=LIVE_CONTEXT_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    current = payload.get("current", {}) or {}
    daily = payload.get("daily", {}) or {}

    temp_current = _safe_float(current.get("temperature_2m"))
    humidity = _safe_float(current.get("relative_humidity_2m"))

    temp_max = None
    daily_temp_max = daily.get("temperature_2m_max")
    if isinstance(daily_temp_max, list) and daily_temp_max:
        temp_max = _safe_float(daily_temp_max[0])

    rainfall = None
    precipitation = daily.get("precipitation_sum")
    if isinstance(precipitation, list) and precipitation:
        rainfall = _safe_float(precipitation[0])

    if all(value is None for value in [temp_current, temp_max, rainfall, humidity]):
        return None

    if temp_max is None:
        temp_max = temp_current
    if temp_current is None:
        temp_current = temp_max

    return {
        "provider": "open_meteo",
        "provider_mode": "free_live",
        "station_name": f"Open-Meteo ({round(lat, 3)}, {round(lon, 3)})",
        "raw_record": {
            "current": current,
            "daily": daily,
        },
        "climate": {
            "temp_current": float(temp_current or 0.0),
            "temp_max": float(temp_max or 0.0),
            "rainfall": float(rainfall or 0.0),
            "humidity": float(humidity or 0.0),
        },
    }


def _safe_float(value: object) -> float | None:
    try:
        if value in (None, "", "NA", "null"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
