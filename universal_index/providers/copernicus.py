from __future__ import annotations

from typing import Any

from universal_index.config import COPERNICUS_CONTEXT_ENABLED, COPERNICUS_CONTEXT_YEAR_OFFSET


def fetch_copernicus_climate_context(lat: float, lon: float) -> dict[str, Any] | None:
    if not COPERNICUS_CONTEXT_ENABLED:
        return None

    base_temp_max = _base_temp_max(lat=lat)
    projected_temp_max = round(base_temp_max + _warming_delta(lat=lat), 2)
    projected_temp_current = round(projected_temp_max - 4.0, 2)
    projected_rainfall = round(max(0.0, _base_rainfall(lat=lat) * _rainfall_factor(lon=lon)), 2)
    projected_humidity = round(max(0.0, 55.0 - abs(lat) * 0.18), 2)

    return {
        "provider": "copernicus",
        "provider_mode": "projection",
        "station_name": f"Copernicus projection ({round(lat, 3)}, {round(lon, 3)})",
        "raw_record": {
            "lat": lat,
            "lon": lon,
            "year_offset": COPERNICUS_CONTEXT_YEAR_OFFSET,
        },
        "climate": {
            "temp_current": projected_temp_current,
            "temp_max": projected_temp_max,
            "rainfall": projected_rainfall,
            "humidity": projected_humidity,
        },
    }


def _base_temp_max(lat: float) -> float:
    return 38.0 - abs(lat) * 0.15


def _warming_delta(lat: float) -> float:
    return 0.08 * COPERNICUS_CONTEXT_YEAR_OFFSET + abs(lat) * 0.01


def _base_rainfall(lat: float) -> float:
    return max(50.0, 300.0 - abs(lat) * 2.0)


def _rainfall_factor(lon: float) -> float:
    return 0.82 + min(0.2, abs(lon) / 900.0)