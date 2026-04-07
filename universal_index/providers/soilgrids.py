from __future__ import annotations

from typing import Any

import requests

from universal_index.config import LIVE_CONTEXT_TIMEOUT_SECONDS, SOILGRIDS_BASE_URL


def fetch_soilgrids_soil_context(lat: float, lon: float) -> dict[str, Any] | None:
    session = requests.Session()
    session.headers.update({"User-Agent": "universal-index-free-context/0.1"})

    properties_url = f"{SOILGRIDS_BASE_URL.rstrip('/')}/properties/query"
    params = {
        "lat": lat,
        "lon": lon,
        "property": ["phh2o", "cfvo", "clay", "sand"],
        "depth": ["0-5cm"],
        "value": ["mean"],
    }

    try:
        response = session.get(properties_url, params=params, timeout=LIVE_CONTEXT_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None

    props = payload.get("properties", {}) if isinstance(payload, dict) else {}

    ph = _extract_soilgrids_value(props, "phh2o")
    salinity = _extract_soilgrids_value(props, "cfvo")
    clay = _extract_soilgrids_value(props, "clay")
    sand = _extract_soilgrids_value(props, "sand")

    soil_type = _infer_soil_type(clay=clay, sand=sand)

    if all(value is None for value in [ph, salinity, clay, sand]):
        return None

    # SoilGrids pH generally uses pH * 10 scale in many layers, normalize if needed.
    normalized_ph = ph
    if normalized_ph is not None and normalized_ph > 14:
        normalized_ph = normalized_ph / 10.0

    return {
        "provider": "soilgrids",
        "provider_mode": "free_live",
        "dataset_name": "SoilGrids",
        "raw_record": props,
        "soil": {
            "type": soil_type,
            "salinity": float(salinity or 0.0),
            "ph": float(normalized_ph or 0.0),
        },
    }


def _extract_soilgrids_value(properties: dict[str, Any], key: str) -> float | None:
    for layer in _iter_layers(properties):
        if layer.get("name") != key:
            continue
        value = _extract_mean_value(layer)
        return _safe_float(value)
    return None


def _infer_soil_type(clay: float | None, sand: float | None) -> str:
    if clay is None and sand is None:
        return "soilgrids_estimated"
    if sand is not None and sand >= 60:
        return "sandy"
    if clay is not None and clay >= 40:
        return "clayey"
    return "loamy"


def _iter_layers(properties: dict[str, Any]) -> list[dict[str, Any]]:
    layers = properties.get("layers")
    if not isinstance(layers, list):
        return []
    return [layer for layer in layers if isinstance(layer, dict)]


def _extract_mean_value(layer: dict[str, Any]) -> object:
    depths = layer.get("depths")
    if not isinstance(depths, list) or not depths:
        return None
    first_depth = depths[0]
    if not isinstance(first_depth, dict):
        return None
    values = first_depth.get("values")
    if not isinstance(values, dict):
        return None
    return values.get("mean")


def _safe_float(value: object) -> float | None:
    try:
        if value in (None, "", "NA", "null"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
