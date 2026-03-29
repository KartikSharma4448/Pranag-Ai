# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

from typing import Any

import requests

from universal_index.config import (
    DATA_GOV_API_BASE_URL,
    IMD_API_BASE_URL,
    IMD_API_KEY,
    IMD_API_URL_TEMPLATE,
    IMD_RESOURCE_ID,
    LIVE_CONTEXT_TIMEOUT_SECONDS,
)


def fetch_imd_climate_context(lat: float, lon: float) -> dict[str, Any] | None:
    if not any([IMD_API_URL_TEMPLATE, IMD_API_BASE_URL, IMD_RESOURCE_ID]):
        return None

    session = requests.Session()
    session.headers.update({"User-Agent": "universal-index-live-context/0.1"})

    try:
        if IMD_API_URL_TEMPLATE:
            url = IMD_API_URL_TEMPLATE.format(lat=lat, lon=lon)
            response = session.get(url, timeout=LIVE_CONTEXT_TIMEOUT_SECONDS)
        elif IMD_API_BASE_URL:
            response = session.get(
                IMD_API_BASE_URL,
                params=_build_generic_params(lat=lat, lon=lon),
                timeout=LIVE_CONTEXT_TIMEOUT_SECONDS,
            )
        else:
            response = session.get(
                f"{DATA_GOV_API_BASE_URL.rstrip('/')}/resource/{IMD_RESOURCE_ID}",
                params=_build_generic_params(lat=lat, lon=lon),
                timeout=LIVE_CONTEXT_TIMEOUT_SECONDS,
            )
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    record = _extract_first_record(payload)
    if not record:
        return None

    climate = {
        "temp_current": _extract_number(
            record,
            ["temp_current", "temperature", "temp", "temperature_c", "air_temperature"],
        ),
        "temp_max": _extract_number(
            record,
            ["temp_max", "maximum_temperature", "maxtemp", "max_temperature"],
        ),
        "rainfall": _extract_number(
            record,
            ["rainfall", "rain", "precipitation", "rain_mm"],
        ),
        "humidity": _extract_number(
            record,
            ["humidity", "relative_humidity", "rh"],
        ),
    }

    if all(value is None for value in climate.values()):
        return None

    if climate["temp_max"] is None:
        climate["temp_max"] = climate["temp_current"]
    if climate["temp_current"] is None:
        climate["temp_current"] = climate["temp_max"]

    return {
        "provider": "imd",
        "provider_mode": "live",
        "station_name": _extract_text(
            record,
            ["station_name", "station", "district", "location", "city", "name"],
        ),
        "raw_record": record,
        "climate": {
            "temp_current": float(climate["temp_current"] or 0.0),
            "temp_max": float(climate["temp_max"] or 0.0),
            "rainfall": float(climate["rainfall"] or 0.0),
            "humidity": float(climate["humidity"] or 0.0),
        },
    }


def _build_generic_params(lat: float, lon: float) -> dict[str, object]:
    params: dict[str, object] = {
        "format": "json",
        "limit": 1,
        "offset": 0,
        "lat": lat,
        "lon": lon,
        "latitude": lat,
        "longitude": lon,
    }
    if IMD_API_KEY:
        params["api-key"] = IMD_API_KEY
    return params


def _extract_first_record(payload: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload.get("records"), list) and payload["records"]:
        return payload["records"][0]
    if isinstance(payload.get("result"), list) and payload["result"]:
        return payload["result"][0]
    if isinstance(payload.get("data"), list) and payload["data"]:
        return payload["data"][0]
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload, dict):
        return payload
    return None


def _extract_number(record: dict[str, Any], candidate_keys: list[str]) -> float | None:
    for key in candidate_keys:
        if key not in record:
            continue
        value = record.get(key)
        if value in (None, "", "NA", "null"):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _extract_text(record: dict[str, Any], candidate_keys: list[str]) -> str | None:
    for key in candidate_keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return None
