from __future__ import annotations

import json
import time
from pathlib import Path

from api.main import context_lookup, recommend, search
from universal_index.cache import build_cache_backend
from universal_index.config import BENCHMARK_SUMMARY_PATH, PROCESSED_DIR


def _measure(label: str, func, *args, **kwargs) -> dict[str, object]:
    start = time.perf_counter()
    result = func(*args, **kwargs)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    rows = None
    if hasattr(result, "rows"):
        rows = getattr(result, "rows")
    return {
        "name": label,
        "duration_ms": duration_ms,
        "rows": rows,
    }


def run_benchmark() -> dict[str, object]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    cache = build_cache_backend()

    search_result = _measure(
        "search",
        search,
        q="self healing high temperature material",
        top_k=8,
        candidate_pool=128,
    )
    context_result = _measure("context", context_lookup, lat=26.3, lon=73.0)
    recommend_result = _measure(
        "recommend",
        recommend,
        q="Design a self healing high temperature material for Rajasthan desert deployment",
        lat=26.3,
        lon=73.0,
        top_k=8,
        candidate_pool=128,
    )

    payload = {
        "timestamp": time.time(),
        "backend": cache.stats(),
        "results": [search_result, context_result, recommend_result],
    }
    BENCHMARK_SUMMARY_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    payload = run_benchmark()
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()