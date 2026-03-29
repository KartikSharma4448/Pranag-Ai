# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import argparse
import json

import pandas as pd
from fastapi.testclient import TestClient

from api.main import app
from universal_index.config import (
    DEMO_RECOMMENDATION_PATH,
    PROCESSED_DIR,
)
from universal_index.recommendation import (
    apply_context_filter,
    dataframe_to_records,
    summarize_recommended_combination,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Day 3 context-aware demo pipeline.")
    parser.add_argument(
        "--prompt",
        default="Design a self healing high temperature material for Rajasthan desert deployment",
    )
    parser.add_argument("--lat", type=float, default=26.3)
    parser.add_argument("--lon", type=float, default=73.0)
    parser.add_argument("--top-k", type=int, default=8)
    parser.add_argument("--candidate-pool", type=int, default=128)
    return parser.parse_args()


def fetch_context_via_api(lat: float, lon: float) -> dict[str, object]:
    with TestClient(app) as client:
        response = client.get("/context", params={"lat": lat, "lon": lon})
        response.raise_for_status()
        return response.json()


def fetch_search_via_api(prompt: str, top_k: int, candidate_pool: int) -> dict[str, object]:
    with TestClient(app) as client:
        response = client.get(
            "/search",
            params={"q": prompt, "top_k": top_k, "candidate_pool": candidate_pool},
        )
        response.raise_for_status()
        return response.json()


def write_demo_output(prompt: str, context: dict[str, object], vector_hits: list[dict[str, object]], recommendations) -> dict[str, object]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "prompt": prompt,
        "context": context,
        "vector_hits": vector_hits,
        "recommended_combination": summarize_recommended_combination(recommendations),
        "final_recommendations": dataframe_to_records(recommendations),
    }
    DEMO_RECOMMENDATION_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    args = parse_args()
    search_payload = fetch_search_via_api(
        prompt=args.prompt,
        top_k=max(args.top_k * 2, args.top_k),
        candidate_pool=args.candidate_pool,
    )
    context = fetch_context_via_api(lat=args.lat, lon=args.lon)
    vector_results = pd.DataFrame(search_payload["items"])
    recommendations = apply_context_filter(
        results=vector_results,
        context=context,
        prompt=args.prompt,
        final_limit=args.top_k,
    )
    payload = write_demo_output(
        prompt=args.prompt,
        context=context,
        vector_hits=search_payload["items"],
        recommendations=recommendations,
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
