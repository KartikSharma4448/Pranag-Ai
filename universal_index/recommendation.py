# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import json

import pandas as pd


def apply_context_filter(
    results: pd.DataFrame,
    context: dict[str, object],
    prompt: str,
    final_limit: int,
) -> pd.DataFrame:
    if results.empty:
        return results

    working = results.copy().reset_index(drop=True)
    soil = context.get("soil", {})
    climate = context.get("climate", {})
    context_temp = float(climate.get("temp_max", 0.0))
    context_salinity = float(soil.get("salinity", 0.0))
    context_ph = float(soil.get("ph", 0.0))
    prompt_lower = prompt.lower()

    context_scores: list[float] = []
    context_fit: list[str] = []
    context_reasons: list[str] = []

    for row in working.to_dict(orient="records"):
        score = float(row.get("similarity_estimate") or 0.0)
        reasons: list[str] = []
        semantic_text = str(row.get("semantic_text") or "").lower()
        entity_type = str(row.get("entity_type") or "")

        temperature_max = _optional_float(row.get("temperature_max"))
        salinity = _optional_float(row.get("salinity"))
        ph = _optional_float(row.get("ph"))

        if entity_type == "material":
            if temperature_max is not None:
                if temperature_max >= context_temp:
                    score += 0.18
                    reasons.append("material temperature capacity clears the site max")
                else:
                    score -= 0.22
                    reasons.append("material temperature capacity is below the site max")
            elif any(
                keyword in semantic_text
                for keyword in [
                    "high-temperature",
                    "thermal resistance",
                    "heat-facing",
                    "structural resilience",
                    "coatings",
                    "ceramic",
                ]
            ):
                score += 0.14
                reasons.append("material semantics align with high-temperature deployment")

        if entity_type == "molecule":
            if any(
                keyword in semantic_text
                for keyword in ["self-healing", "cross-linking", "polymer", "coating additive"]
            ):
                score += 0.16
                reasons.append("molecule semantics support self-healing chemistry")
            if context_temp >= 45:
                score += 0.04
                reasons.append("molecule retained for hot-environment formulation work")

        if entity_type == "gene":
            if any(
                keyword in semantic_text
                for keyword in ["stress response", "repair", "adaptation", "thermal resilience"]
            ):
                score += 0.14
                reasons.append("gene points to stress-response or repair behavior")
            if "desert" in prompt_lower or context_temp >= 45:
                score += 0.05
                reasons.append("gene retained as a bio-inspired heat adaptation signal")

        if entity_type == "soil":
            if temperature_max is not None:
                if temperature_max >= context_temp:
                    score += 0.18
                    reasons.append("soil sample survives comparable peak temperature")
                else:
                    score -= 0.18
                    reasons.append("soil sample is cooler than the target site")
            if salinity is not None:
                salinity_gap = abs(salinity - context_salinity)
                if salinity_gap <= 3:
                    score += 0.15
                    reasons.append("soil salinity is closely matched to the site")
                elif salinity_gap <= 6:
                    score += 0.08
                    reasons.append("soil salinity is directionally similar to the site")
                else:
                    score -= 0.08
                    reasons.append("soil salinity is less aligned with the site")
            if ph is not None:
                ph_gap = abs(ph - context_ph)
                if ph_gap <= 0.6:
                    score += 0.08
                    reasons.append("soil pH is close to the site context")

        if entity_type == "simulation":
            if temperature_max is not None:
                if temperature_max >= context_temp:
                    score += 0.16
                    reasons.append("simulation envelope clears the site heat requirement")
                else:
                    score -= 0.12
                    reasons.append("simulation envelope is cooler than the target site")
            if any(
                keyword in semantic_text
                for keyword in [
                    "thermal expansion",
                    "heat transfer",
                    "structural loading",
                    "cfd",
                    "fea",
                    "heat-load",
                ]
            ):
                score += 0.12
                reasons.append("simulation context supports engineering validation")

        if not reasons:
            reasons.append("kept from vector search because it remains semantically relevant")

        context_scores.append(round(score, 4))
        context_fit.append(label_context_fit(score))
        context_reasons.append("; ".join(reasons))

    working["context_score"] = context_scores
    working["context_fit"] = context_fit
    working["context_reasons"] = context_reasons

    ordered = working.sort_values(
        by=["context_score", "similarity_estimate", "distance"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    return select_final_recommendations(ordered, final_limit=final_limit)


def select_final_recommendations(results: pd.DataFrame, final_limit: int) -> pd.DataFrame:
    selected_rows: list[pd.Series] = []
    used_ids: set[str] = set()

    priority_types = ["material", "molecule", "gene", "soil"]
    if final_limit >= 5:
        priority_types.append("simulation")

    for entity_type in priority_types:
        if len(selected_rows) >= final_limit:
            break
        matches = results[results["entity_type"] == entity_type]
        if matches.empty:
            continue
        row = matches.iloc[0]
        entity_id = str(row["entity_id"])
        if entity_id in used_ids:
            continue
        selected_rows.append(row)
        used_ids.add(entity_id)

    for _, row in results.iterrows():
        if len(selected_rows) >= final_limit:
            break
        entity_id = str(row["entity_id"])
        if entity_id in used_ids:
            continue
        selected_rows.append(row)
        used_ids.add(entity_id)

    final = pd.DataFrame(selected_rows).reset_index(drop=True)
    if "final_rank" in final.columns:
        final = final.drop(columns=["final_rank"])
    final.insert(0, "final_rank", range(1, len(final) + 1))
    return final


def label_context_fit(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.58:
        return "medium"
    return "low"


def summarize_recommended_combination(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    summary: dict[str, dict[str, object]] = {}
    for entity_type in ["material", "molecule", "gene", "soil", "simulation"]:
        matches = frame[frame["entity_type"] == entity_type]
        if matches.empty:
            continue
        row = matches.iloc[0]
        summary[entity_type] = {
            "entity_id": row["entity_id"],
            "name": row["name"],
            "context_fit": row.get("context_fit"),
            "reason": row.get("context_reasons"),
        }
    return summary


def dataframe_to_records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return json.loads(frame.to_json(orient="records"))


def _optional_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)
