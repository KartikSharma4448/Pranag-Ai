from __future__ import annotations

from typing import Any


def build_agristack_proxy_context(
    soil: dict[str, Any],
    climate: dict[str, Any],
    fallback_agriculture: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fallback = fallback_agriculture or {}
    fallback_crops = fallback.get("main_crops")
    crops: list[str] = []
    if isinstance(fallback_crops, list):
        crops.extend(str(item) for item in fallback_crops if str(item).strip())

    soil_type = str(soil.get("type") or "").lower()
    salinity = _safe_float(soil.get("salinity")) or 0.0
    ph = _safe_float(soil.get("ph")) or 7.0

    temp_max = _safe_float(climate.get("temp_max")) or 30.0
    rainfall = _safe_float(climate.get("rainfall")) or 0.0

    crops.extend(_crops_from_climate(temp_max=temp_max, rainfall=rainfall))
    irrigation = _irrigation_from_rainfall(rainfall=rainfall)
    crops.extend(_crops_from_soil(soil_type=soil_type, salinity=salinity, ph=ph))

    deduped_crops = _dedupe(crops)

    return {
        "provider": "agristack_proxy",
        "provider_mode": "derived",
        "agriculture": {
            "main_crops": deduped_crops[:8],
            "irrigation": irrigation,
        },
    }


def _safe_float(value: object) -> float | None:
    try:
        if value in (None, "", "NA", "null"):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        normalized = item.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(item.strip())
    return output


def _crops_from_climate(temp_max: float, rainfall: float) -> list[str]:
    if temp_max >= 38:
        return ["millet", "sorghum", "cluster_bean"]
    if temp_max <= 28 and rainfall >= 80:
        return ["rice", "maize"]
    return ["wheat", "mustard"]


def _irrigation_from_rainfall(rainfall: float) -> str:
    if rainfall >= 120:
        return "rainfed"
    if rainfall >= 50:
        return "sprinkler"
    return "drip"


def _crops_from_soil(soil_type: str, salinity: float, ph: float) -> list[str]:
    crops: list[str] = []
    if salinity >= 4:
        crops.append("barley")
    if ph >= 8.0:
        crops.append("cotton")
    elif ph <= 6.0:
        crops.append("potato")

    if "sandy" in soil_type:
        crops.append("groundnut")
    elif "clayey" in soil_type:
        crops.append("paddy")
    return crops
