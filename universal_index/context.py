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

try:
    from sklearn.neighbors import KDTree
except ImportError:
    KDTree = None  # pragma: no cover - optional dependency

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

# Module-level spatial indexing cache for fast nearest-neighbor lookups
_CONTEXT_DATASET_CACHE: pd.DataFrame | None = None
_CONTEXT_KDTREE_CACHE: object | None = None  # KDTree instance if available


def _build_spatial_index(frame: pd.DataFrame) -> object | None:
    """Build a KDTree spatial index for fast nearest-neighbor queries."""
    if KDTree is None:
        return None
    try:
        coords = frame[["lat", "lon"]].values
        return KDTree(coords, leaf_size=30, metric="haversine")
    except (ImportError, ValueError):
        return None


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

    # Build and cache spatial index for fast nearest-neighbor lookups
    global _CONTEXT_DATASET_CACHE, _CONTEXT_KDTREE_CACHE
    _CONTEXT_DATASET_CACHE = frame.copy()
    _CONTEXT_KDTREE_CACHE = _build_spatial_index(frame)

    return frame


def lookup_context(
    lat: float,
    lon: float,
    path: str | Path = CONTEXT_DATASET_PATH,
    mode: str = LIVE_CONTEXT_MODE,
) -> dict[str, object]:
    local_context = lookup_local_context_kdtree(lat=lat, lon=lon, path=path)
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


def lookup_local_context_kdtree(
    lat: float,
    lon: float,
    path: str | Path = CONTEXT_DATASET_PATH,
    k: int = 1,
) -> dict[str, object]:
    """
    Fast nearest-neighbor lookup for global coordinates using KDTree spatial indexing.
    
    **Global Context Feature**: Supports arbitrary lat/lon queries on Earth via optimized
    spatial indexing. Enables non-India use cases by auto-detecting region and routing
    to appropriate data providers (Copernicus for global climate, SoilGrids for global soil).
    
    Linear search fallback if scikit-learn unavailable. 
    
    Args:
        lat: Query latitude (-90 to 90, degrees North)
        lon: Query longitude (-180 to 180, degrees East)
        path: Context dataset path
        k: Number of neighbors to consider (default 1 for nearest)
    
    Returns:
        Context payload with matched location, soil, climate, agriculture.
        Gracefully degrades to nearest reference point if exact match unavailable.
    """
    frame = load_context_dataset(path).copy()
    
    # Try KDTree acceleration if available
    global _CONTEXT_KDTREE_CACHE, _CONTEXT_DATASET_CACHE
    if _CONTEXT_KDTREE_CACHE is not None and _CONTEXT_DATASET_CACHE is not None:
        try:
            # KDTree uses radians for haversine metric
            query_point = [[math.radians(lat), math.radians(lon)]]
            distances, indices = _CONTEXT_KDTREE_CACHE.query(query_point, k=k)
            
            # Take the first neighbor (nearest)
            nearest_idx = int(indices[0][0])
            best = frame.iloc[nearest_idx]
            
            # Convert KDTree distance (radians) back to km
            distance_km = float(distances[0][0]) * 6371.0
        except (TypeError, ValueError, IndexError):
            # Fallback to linear search if KDTree fails
            distance_km = None
            best = _fallback_linear_search(frame, lat, lon)
            if best is not None:
                distance_km = haversine_km(lat, lon, float(best["lat"]), float(best["lon"]))
    else:
        # No KDTree available, use linear search
        best = _fallback_linear_search(frame, lat, lon)
        distance_km = haversine_km(lat, lon, float(best["lat"]), float(best["lon"])) if best is not None else None
    
    if best is None:
        raise RuntimeError(f"No context location found for lat={lat}, lon={lon}")
    
    if distance_km is None:
        distance_km = haversine_km(lat, lon, float(best["lat"]), float(best["lon"]))
    
    return {
        "query_lat": float(lat),
        "query_lon": float(lon),
        "location_name": str(best["location_name"]),
        "matched_lat": float(best["lat"]),
        "matched_lon": float(best["lon"]),
        "distance_km": round(distance_km, 2),
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


def _fallback_linear_search(frame: pd.DataFrame, lat: float, lon: float) -> pd.Series | None:
    """Linear search fallback for nearest-neighbor when KDTree unavailable."""
    if frame.empty:
        return None
    frame["distance_km"] = frame.apply(
        lambda row: haversine_km(lat, lon, float(row["lat"]), float(row["lon"])),
        axis=1,
    )
    return frame.sort_values(
        by=["distance_km", "climate_temp_max"], ascending=[True, False]
    ).iloc[0]


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


def _is_within_india(lat: float, lon: float) -> bool:
    """Check if coordinates are within India's geographic bounds.
    
    Bounds: Lat 8°N to 35°N, Lon 68°E to 97°E
    Used to decide whether to attempt India-specific providers (IMD, Bhuvan).
    """
    return (8.0 <= lat <= 35.5) and (68.0 <= lon <= 97.5)


def _resolve_context_sources(lat: float, lon: float) -> tuple[dict[str, object] | None, dict[str, object] | None, dict[str, object] | None]:
    within_india = _is_within_india(lat=lat, lon=lon)
    
    # Only try India-specific providers if query is within India bounds
    live_soil = None
    if within_india:
        live_soil = fetch_bhuvan_soil_context(lat=lat, lon=lon)
    
    if live_soil is None and FREE_CONTEXT_SOIL_ENABLED:
        live_soil = fetch_soilgrids_soil_context(lat=lat, lon=lon)

    live_climate = None
    if within_india:
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
    original_location_name = merged.get("location_name")
    projected = merge_context_payload(
        local_context=merged,
        live_soil=None,
        live_climate=copernicus_climate,
        live_agriculture=None,
    )
    if original_location_name:
        projected["location_name"] = original_location_name
    return projected


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
