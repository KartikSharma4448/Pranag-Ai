# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any

import requests

from universal_index.config import (
    BHUVAN_INFO_FORMAT,
    BHUVAN_PLACE_NAME,
    BHUVAN_WMS_LAYER,
    BHUVAN_WMS_URL,
    LIVE_CONTEXT_TIMEOUT_SECONDS,
)


def fetch_bhuvan_soil_context(lat: float, lon: float) -> dict[str, Any] | None:
    if not BHUVAN_WMS_LAYER:
        return None

    params = _build_wms_params(lat=lat, lon=lon, layer=BHUVAN_WMS_LAYER)
    session = requests.Session()
    session.headers.update({"User-Agent": "universal-index-live-context/0.1"})

    try:
        response = session.get(BHUVAN_WMS_URL, params=params, timeout=LIVE_CONTEXT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except Exception:
        return None

    properties = _extract_properties(response.text)
    if not properties:
        return None

    soil_type = _extract_text(
        properties,
        ["soil_type", "texture", "landform", "class", "category", "name"],
    )
    salinity = _extract_number(properties, ["salinity", "ec", "soil_salinity"])
    ph = _extract_number(properties, ["ph", "soil_ph", "p_h"])

    if soil_type is None and salinity is None and ph is None:
        return None

    return {
        "provider": "bhuvan",
        "provider_mode": "live",
        "dataset_name": BHUVAN_PLACE_NAME,
        "raw_record": properties,
        "soil": {
            "type": soil_type or BHUVAN_PLACE_NAME,
            "salinity": float(salinity or 0.0),
            "ph": float(ph or 0.0),
        },
    }


def _build_wms_params(lat: float, lon: float, layer: str) -> dict[str, object]:
    delta = 0.02
    return {
        "SERVICE": "WMS",
        "VERSION": "1.1.1",
        "REQUEST": "GetFeatureInfo",
        "LAYERS": layer,
        "QUERY_LAYERS": layer,
        "INFO_FORMAT": BHUVAN_INFO_FORMAT,
        "FEATURE_COUNT": 1,
        "SRS": "EPSG:4326",
        "WIDTH": 101,
        "HEIGHT": 101,
        "X": 50,
        "Y": 50,
        "BBOX": f"{lon - delta},{lat - delta},{lon + delta},{lat + delta}",
    }


def _extract_properties(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None

    if stripped.startswith("{"):
        payload = json.loads(stripped)
        if isinstance(payload.get("features"), list) and payload["features"]:
            feature = payload["features"][0]
            if isinstance(feature.get("properties"), dict):
                return feature["properties"]
        if isinstance(payload, dict):
            return payload

    if stripped.startswith("<"):
        root = ET.fromstring(stripped)
        values: dict[str, Any] = {}
        for node in root.iter():
            tag = node.tag.split("}")[-1]
            content = (node.text or "").strip()
            if tag and content and tag.lower() not in {"html", "body", "featureinforesponse"}:
                values[tag] = content
        return values or None

    values: dict[str, Any] = {}
    for line in stripped.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", maxsplit=1)
        key = key.strip().lower().replace(" ", "_")
        value = value.strip()
        if key and value:
            values[key] = value
    return values or None


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
