# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from universal_index.config import (
    AGRISTACK_PROXY_ENABLED,
    CONTEXT_DATASET_PATH,
    FREE_CONTEXT_CLIMATE_ENABLED,
    FREE_CONTEXT_SOIL_ENABLED,
    LIVE_CONTEXT_MODE,
)
from universal_index.providers import (
    build_agristack_proxy_context,
    fetch_bhuvan_soil_context,
    fetch_copernicus_climate_context,
    fetch_imd_climate_context,
    fetch_open_meteo_climate_context,
    fetch_soilgrids_soil_context,
)

REQUIRED_CONTEXT_COLUMNS = [
    "location_name",
    "lat",
    "lon",
    "soil_type",
    "soil_salinity",
    "soil_ph",
    "climate_temp_current",
    "climate_temp_max",
    "climate_rainfall",
    "climate_humidity",
    "agriculture_main_crops",
    "agriculture_irrigation",
    "notes",
]


def load_context_dataset(path: str | Path = CONTEXT_DATASET_PATH) -> pd.DataFrame:
    dataset_path = Path(path)
    if not dataset_path.exists():
        raise FileNotFoundError(
            f"Context dataset not found at {dataset_path}. Expected Day 3 context CSV."
        )

    frame = pd.read_csv(dataset_path)
    missing_columns = [column for column in REQUIRED_CONTEXT_COLUMNS if column not in frame.columns]
    if missing_columns:
        raise ValueError(
            "Context dataset is missing required columns: " + ", ".join(missing_columns)
        )

    for column in [
        "lat",
        "lon",
        "soil_salinity",
        "soil_ph",
        "climate_temp_current",
        "climate_temp_max",
        "climate_rainfall",
        "climate_humidity",
    ]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    if frame[["lat", "lon"]].isna().any().any():
        raise ValueError("Context dataset contains invalid latitude or longitude values.")

    return frame


def lookup_context(
    lat: float,
    lon: float,
    path: str | Path = CONTEXT_DATASET_PATH,
    mode: str = LIVE_CONTEXT_MODE,
) -> dict[str, object]:
    local_context = lookup_local_context(lat=lat, lon=lon, path=path)
    normalized_mode = (mode or "auto").strip().lower()

    if normalized_mode == "local":
        return _annotate_providers(local_context, mode="local", soil="local_csv", climate="local_csv")

    live_soil, live_climate, copernicus_climate = _resolve_context_sources(lat=lat, lon=lon)

    if normalized_mode == "live" and live_soil is None and live_climate is None:
        return _annotate_providers(
            local_context,
            mode="live_failed_fallback",
            soil="local_csv",
            climate="local_csv",
        )

    merged = merge_context_payload(
        local_context=local_context,
        live_soil=live_soil,
        live_climate=live_climate,
        live_agriculture=None,
    )

    merged = _apply_copernicus_projection(merged, copernicus_climate)
    merged, live_agriculture = _apply_agriculture_proxy(merged)
    return _annotate_providers(
        merged,
        mode="auto" if normalized_mode == "auto" else normalized_mode,
        soil=live_soil.get("provider", "local_csv") if live_soil else "local_csv",
        climate=_select_climate_provider(live_climate=live_climate, copernicus_climate=copernicus_climate),
        agriculture=live_agriculture.get("provider", "local_csv") if live_agriculture else "local_csv",
    )


def lookup_local_context(
    lat: float,
    lon: float,
    path: str | Path = CONTEXT_DATASET_PATH,
) -> dict[str, object]:
    frame = load_context_dataset(path).copy()
    frame["distance_km"] = frame.apply(
        lambda row: haversine_km(lat, lon, float(row["lat"]), float(row["lon"])),
        axis=1,
    )
    best = frame.sort_values(
        by=["distance_km", "climate_temp_max"], ascending=[True, False]
    ).iloc[0]

    return {
        "query_lat": float(lat),
        "query_lon": float(lon),
        "location_name": str(best["location_name"]),
        "matched_lat": float(best["lat"]),
        "matched_lon": float(best["lon"]),
        "distance_km": round(float(best["distance_km"]), 2),
        "soil": {
            "type": str(best["soil_type"]),
            "salinity": float(best["soil_salinity"]),
            "ph": float(best["soil_ph"]),
        },
        "climate": {
            "temp_current": float(best["climate_temp_current"]),
            "temp_max": float(best["climate_temp_max"]),
            "rainfall": float(best["climate_rainfall"]),
            "humidity": float(best["climate_humidity"]),
        },
        "agriculture": {
            "main_crops": split_pipe_list(best["agriculture_main_crops"]),
            "irrigation": str(best["agriculture_irrigation"]),
        },
        "notes": str(best["notes"]),
    }


def merge_context_payload(
    local_context: dict[str, object],
    live_soil: dict[str, object] | None,
    live_climate: dict[str, object] | None,
    live_agriculture: dict[str, object] | None,
) -> dict[str, object]:
    merged = dict(local_context)
    merged["soil"] = dict(local_context["soil"])
    merged["climate"] = dict(local_context["climate"])
    merged["agriculture"] = dict(local_context["agriculture"])

    notes = [str(local_context.get("notes") or "").strip()]

    if live_soil:
        live_soil_payload = live_soil.get("soil", {})
        merged["soil"] = {
            "type": str(live_soil_payload.get("type") or merged["soil"]["type"]),
            "salinity": float(live_soil_payload.get("salinity") or merged["soil"]["salinity"]),
            "ph": float(live_soil_payload.get("ph") or merged["soil"]["ph"]),
        }
        notes.append(f"Soil refreshed from {live_soil.get('provider', 'bhuvan')}.")

    if live_climate:
        live_climate_payload = live_climate.get("climate", {})
        merged["climate"] = {
            "temp_current": float(
                live_climate_payload.get("temp_current") or merged["climate"]["temp_current"]
            ),
            "temp_max": float(
                live_climate_payload.get("temp_max") or merged["climate"]["temp_max"]
            ),
            "rainfall": float(
                live_climate_payload.get("rainfall") or merged["climate"]["rainfall"]
            ),
            "humidity": float(
                live_climate_payload.get("humidity") or merged["climate"]["humidity"]
            ),
        }
        station_name = live_climate.get("station_name")
        if station_name:
            merged["location_name"] = str(station_name)
        notes.append(f"Climate refreshed from {live_climate.get('provider', 'imd')}.")

    if live_agriculture:
        merged["agriculture"] = _merge_agriculture_payload(
            base_agriculture=merged["agriculture"],
            live_agriculture=live_agriculture,
        )
        notes.append(
            f"Agriculture guidance generated by {live_agriculture.get('provider', 'agristack_proxy')}."
        )

    merged["notes"] = " ".join(note for note in notes if note).strip()
    return merged


def get_location_context(
    lat: float,
    lon: float,
    path: str | Path = CONTEXT_DATASET_PATH,
    mode: str = LIVE_CONTEXT_MODE,
) -> dict[str, object]:
    return lookup_context(lat=lat, lon=lon, path=path, mode=mode)


def split_pipe_list(value: object) -> list[str]:
    if value is None:
        return []
    return [item.strip() for item in str(value).split("|") if item.strip()]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return radius_km * c


def _merge_agriculture_payload(
    base_agriculture: dict[str, object],
    live_agriculture: dict[str, object],
) -> dict[str, object]:
    agri_payload = live_agriculture.get("agriculture", {})
    main_crops = agri_payload.get("main_crops") if isinstance(agri_payload, dict) else None
    irrigation = agri_payload.get("irrigation") if isinstance(agri_payload, dict) else None

    normalized_crops = (
        [str(item) for item in main_crops if str(item).strip()]
        if isinstance(main_crops, list)
        else base_agriculture["main_crops"]
    )

    return {
        "main_crops": normalized_crops,
        "irrigation": str(irrigation or base_agriculture["irrigation"]),
    }


def _resolve_context_sources(lat: float, lon: float) -> tuple[dict[str, object] | None, dict[str, object] | None, dict[str, object] | None]:
    live_soil = fetch_bhuvan_soil_context(lat=lat, lon=lon)
    if live_soil is None and FREE_CONTEXT_SOIL_ENABLED:
        live_soil = fetch_soilgrids_soil_context(lat=lat, lon=lon)

    live_climate = fetch_imd_climate_context(lat=lat, lon=lon)
    if live_climate is None and FREE_CONTEXT_CLIMATE_ENABLED:
        live_climate = fetch_open_meteo_climate_context(lat=lat, lon=lon)

    return live_soil, live_climate, fetch_copernicus_climate_context(lat=lat, lon=lon)


def _apply_copernicus_projection(
    merged: dict[str, object],
    copernicus_climate: dict[str, object] | None,
) -> dict[str, object]:
    if copernicus_climate is None:
        return merged
    return merge_context_payload(
        local_context=merged,
        live_soil=None,
        live_climate=copernicus_climate,
        live_agriculture=None,
    )


def _apply_agriculture_proxy(merged: dict[str, object]) -> tuple[dict[str, object], dict[str, object] | None]:
    if not AGRISTACK_PROXY_ENABLED:
        return merged, None
    live_agriculture = build_agristack_proxy_context(
        soil=merged["soil"],
        climate=merged["climate"],
        fallback_agriculture=merged["agriculture"],
    )
    merged = merge_context_payload(
        local_context=merged,
        live_soil=None,
        live_climate=None,
        live_agriculture=live_agriculture,
    )
    return merged, live_agriculture


def _select_climate_provider(
    live_climate: dict[str, object] | None,
    copernicus_climate: dict[str, object] | None,
) -> str:
    if copernicus_climate is not None:
        return str(copernicus_climate.get("provider", "local_csv"))
    if live_climate is not None:
        return str(live_climate.get("provider", "local_csv"))
    return "local_csv"


def _annotate_providers(
    context: dict[str, object],
    mode: str,
    soil: str,
    climate: str,
    agriculture: str = "local_csv",
) -> dict[str, object]:
    payload = dict(context)
    payload["providers"] = {
        "mode": mode,
        "soil": soil,
        "climate": climate,
        "agriculture": agriculture,
    }
    return payload
