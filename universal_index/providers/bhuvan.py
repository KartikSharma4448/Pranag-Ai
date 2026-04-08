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

    session = requests.Session()
    session.headers.update({"User-Agent": "universal-index-live-context/0.1"})

    response_text: str | None = None
    for params in _iter_wms_params(lat=lat, lon=lon, layer=BHUVAN_WMS_LAYER):
        try:
            response = session.get(BHUVAN_WMS_URL, params=params, timeout=LIVE_CONTEXT_TIMEOUT_SECONDS)
            response.raise_for_status()
        except Exception:
            continue

        if _extract_properties(response.text):
            response_text = response.text
            break

    if not response_text:
        return None

    properties = _extract_properties(response_text)
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

    if soil_type is None:
        soil_type = _infer_soil_type(properties)

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


def _iter_wms_params(lat: float, lon: float, layer: str) -> list[dict[str, object]]:
    delta = 0.02
    return [
        {
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
        },
        {
            "SERVICE": "WMS",
            "VERSION": "1.3.0",
            "REQUEST": "GetFeatureInfo",
            "LAYERS": layer,
            "QUERY_LAYERS": layer,
            "INFO_FORMAT": BHUVAN_INFO_FORMAT,
            "FEATURE_COUNT": 1,
            "CRS": "EPSG:4326",
            "WIDTH": 101,
            "HEIGHT": 101,
            "I": 50,
            "J": 50,
            "BBOX": f"{lat - delta},{lon - delta},{lat + delta},{lon + delta}",
        },
    ]


def _extract_properties(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None

    if stripped.startswith("{"):
        payload = json.loads(stripped)
        extracted = _extract_properties_from_json(payload)
        if extracted:
            return extracted

    if stripped.startswith("<"):
        root = ET.fromstring(stripped)
        values = _extract_properties_from_xml(root)
        if values:
            return values

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


def _extract_properties_from_json(payload: Any) -> dict[str, Any] | None:
    if isinstance(payload, list):
        return _first_json_mapping(payload)

    if not isinstance(payload, dict):
        return None

    direct_candidate = _mapping_candidate(payload, ["properties", "feature", "result", "data", "attributes"])
    if direct_candidate is not None:
        return direct_candidate

    features = payload.get("features")
    if isinstance(features, list):
        first_feature = _first_json_mapping(features)
        if first_feature is not None:
            feature_candidate = first_feature.get("properties")
            if isinstance(feature_candidate, dict):
                return feature_candidate
            nested_candidate = _extract_properties_from_json(first_feature)
            if nested_candidate is not None:
                return nested_candidate

    return _first_nested_json_mapping(payload)


def _mapping_candidate(container: dict[str, Any], keys: list[str]) -> dict[str, Any] | None:
    for key in keys:
        candidate = container.get(key)
        if isinstance(candidate, dict):
            return candidate
        if isinstance(candidate, list):
            nested = _first_json_mapping(candidate)
            if nested is not None:
                return nested
    return None


def _first_json_mapping(items: list[Any]) -> dict[str, Any] | None:
    for item in items:
        if isinstance(item, dict):
            return item
        if isinstance(item, list):
            nested = _first_json_mapping(item)
            if nested is not None:
                return nested
    return None


def _first_nested_json_mapping(payload: dict[str, Any]) -> dict[str, Any] | None:
    for value in payload.values():
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            nested = _first_json_mapping(value)
            if nested is not None:
                return nested

    return payload or None


def _extract_properties_from_xml(root: ET.Element) -> dict[str, Any] | None:
    values: dict[str, Any] = {}
    for node in root.iter():
        tag = node.tag.split("}")[-1]
        content = (node.text or "").strip()
        if tag and content and tag.lower() not in {"html", "body", "featureinforesponse"}:
            values[tag] = content
    return values or None


def _infer_soil_type(properties: dict[str, Any]) -> str:
    flattened = " ".join(f"{key}={value}" for key, value in properties.items()).lower()
    if any(keyword in flattened for keyword in ["saline", "salt", "alkali"]):
        return "saline"
    if any(keyword in flattened for keyword in ["sandy", "sand"]):
        return "sandy"
    if any(keyword in flattened for keyword in ["clay", "clayey"]):
        return "clayey"
    if any(keyword in flattened for keyword in ["loam", "loamy"]):
        return "loamy"
    return BHUVAN_PLACE_NAME


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
