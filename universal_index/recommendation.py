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
    site = _context_site(context)
    prompt_lower = prompt.lower()

    context_scores: list[float] = []
    context_fit: list[str] = []
    context_reasons: list[str] = []

    for row in working.to_dict(orient="records"):
        score = float(row.get("similarity_estimate") or 0.0)
        reasons: list[str] = []
        score, reasons = _score_context_match(row, site, prompt_lower, score, reasons)

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


def _context_site(context: dict[str, object]) -> dict[str, float]:
    soil = context.get("soil", {})
    climate = context.get("climate", {})
    return {
        "temp_max": float(climate.get("temp_max", 0.0)),
        "salinity": float(soil.get("salinity", 0.0)),
        "ph": float(soil.get("ph", 0.0)),
    }


def _score_context_match(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    entity_type = str(row.get("entity_type") or "")
    semantic_text = str(row.get("semantic_text") or "").lower()

    handlers = {
        "material": _score_material_context,
        "molecule": _score_molecule_context,
        "gene": _score_gene_context,
        "soil": _score_soil_context,
        "simulation": _score_simulation_context,
        "protein": _score_protein_context,
        "structure": _score_structure_context,
    }
    handler = handlers.get(entity_type)
    if handler is not None:
        return handler(row, site, prompt_lower, semantic_text, score, reasons)

    return score, reasons


def _score_material_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    temperature_max = _optional_float(row.get("temperature_max"))
    if temperature_max is not None:
        if temperature_max >= site["temp_max"]:
            score += 0.18
            reasons.append("material temperature capacity clears the site max")
        else:
            score -= 0.22
            reasons.append("material temperature capacity is below the site max")
    elif _contains_any(
        semantic_text,
        ["high-temperature", "thermal resistance", "heat-facing", "structural resilience", "coatings", "ceramic"],
    ):
        score += 0.14
        reasons.append("material semantics align with high-temperature deployment")
    return score, reasons


def _score_molecule_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    if _contains_any(semantic_text, ["self-healing", "cross-linking", "polymer", "coating additive"]):
        score += 0.16
        reasons.append("molecule semantics support self-healing chemistry")
    if site["temp_max"] >= 45:
        score += 0.04
        reasons.append("molecule retained for hot-environment formulation work")
    return score, reasons


def _score_gene_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    if _contains_any(semantic_text, ["stress response", "repair", "adaptation", "thermal resilience"]):
        score += 0.14
        reasons.append("gene points to stress-response or repair behavior")
    if "desert" in prompt_lower or site["temp_max"] >= 45:
        score += 0.05
        reasons.append("gene retained as a bio-inspired heat adaptation signal")
    return score, reasons


def _score_soil_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    temperature_max = _optional_float(row.get("temperature_max"))
    salinity = _optional_float(row.get("salinity"))
    ph = _optional_float(row.get("ph"))

    if temperature_max is not None:
        if temperature_max >= site["temp_max"]:
            score += 0.18
            reasons.append("soil sample survives comparable peak temperature")
        else:
            score -= 0.18
            reasons.append("soil sample is cooler than the target site")

    if salinity is not None:
        salinity_gap = abs(salinity - site["salinity"])
        if salinity_gap <= 3:
            score += 0.15
            reasons.append("soil salinity is closely matched to the site")
        elif salinity_gap <= 6:
            score += 0.08
            reasons.append("soil salinity is directionally similar to the site")
        else:
            score -= 0.08
            reasons.append("soil salinity is less aligned with the site")

    if ph is not None and abs(ph - site["ph"]) <= 0.6:
        score += 0.08
        reasons.append("soil pH is close to the site context")

    return score, reasons


def _score_simulation_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    temperature_max = _optional_float(row.get("temperature_max"))
    if temperature_max is not None:
        if temperature_max >= site["temp_max"]:
            score += 0.16
            reasons.append("simulation envelope clears the site heat requirement")
        else:
            score -= 0.12
            reasons.append("simulation envelope is cooler than the target site")
    if _contains_any(
        semantic_text,
        ["thermal expansion", "heat transfer", "structural loading", "cfd", "fea", "heat-load"],
    ):
        score += 0.12
        reasons.append("simulation context supports engineering validation")
    return score, reasons


def _score_protein_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    if _contains_any(semantic_text, ["enzyme", "cataly", "bind", "repair", "thermal", "stress"]):
        score += 0.12
        reasons.append("protein suggests catalytic or stress-response behavior")
    if site["temp_max"] >= 45:
        score += 0.03
        reasons.append("protein retained for hot-environment biological screening")
    return score, reasons


def _score_structure_context(
    row: dict[str, object],
    site: dict[str, float],
    prompt_lower: str,
    semantic_text: str,
    score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    if _contains_any(semantic_text, ["binding pocket", "stability", "symmetry", "fold", "crystal"]):
        score += 0.12
        reasons.append("structure supports fold-aware or lattice-aware screening")
    return score, reasons


def _contains_any(text: str, keywords: list[str]) -> bool:
    return any(keyword in text for keyword in keywords)


def select_final_recommendations(results: pd.DataFrame, final_limit: int) -> pd.DataFrame:
    selected_rows: list[pd.Series] = []
    used_ids: set[str] = set()

    priority_types = ["material", "molecule", "protein", "gene", "structure", "soil"]
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
    for entity_type in ["material", "molecule", "protein", "gene", "structure", "soil", "simulation"]:
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
