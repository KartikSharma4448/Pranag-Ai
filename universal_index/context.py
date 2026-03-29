# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from universal_index.config import CONTEXT_DATASET_PATH, LIVE_CONTEXT_MODE
from universal_index.providers import fetch_bhuvan_soil_context, fetch_imd_climate_context

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
        local_context["providers"] = {
            "mode": "local",
            "soil": "local_csv",
            "climate": "local_csv",
            "agriculture": "local_csv",
        }
        return local_context

    live_soil = fetch_bhuvan_soil_context(lat=lat, lon=lon)
    live_climate = fetch_imd_climate_context(lat=lat, lon=lon)

    if normalized_mode == "live" and live_soil is None and live_climate is None:
        local_context["providers"] = {
            "mode": "live_failed_fallback",
            "soil": "local_csv",
            "climate": "local_csv",
            "agriculture": "local_csv",
        }
        return local_context

    merged = merge_context_payload(
        local_context=local_context,
        live_soil=live_soil,
        live_climate=live_climate,
    )
    merged["providers"] = {
        "mode": "auto" if normalized_mode == "auto" else normalized_mode,
        "soil": live_soil.get("provider", "local_csv") if live_soil else "local_csv",
        "climate": live_climate.get("provider", "local_csv") if live_climate else "local_csv",
        "agriculture": "local_csv",
    }
    return merged


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
