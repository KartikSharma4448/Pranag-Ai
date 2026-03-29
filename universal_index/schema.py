# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import pandas as pd

UNIVERSAL_COLUMNS = [
    "entity_id",
    "entity_type",
    "name",
    "description",
    "temperature_max",
    "strength",
    "conductivity",
    "ph",
    "salinity",
    "source",
]

NUMERIC_COLUMNS = ["temperature_max", "strength", "conductivity", "ph", "salinity"]
TEXT_COLUMNS = ["entity_id", "entity_type", "name", "description", "source"]


def empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=UNIVERSAL_COLUMNS)


def normalize_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        normalized = empty_frame()
    else:
        normalized = frame.copy()

    for column in UNIVERSAL_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    normalized = normalized[UNIVERSAL_COLUMNS]

    for column in NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    for column in TEXT_COLUMNS:
        normalized[column] = normalized[column].astype("string")

    return normalized.reset_index(drop=True)


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    prepared = [normalize_frame(frame) for frame in frames if frame is not None]
    if not prepared:
        return empty_frame()
    combined = pd.concat(prepared, ignore_index=True)
    return normalize_frame(combined)
