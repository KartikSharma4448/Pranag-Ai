from __future__ import annotations

import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.main import context_lookup, metrics, recommend, search
from universal_index.benchmark import run_benchmark
from universal_index.config import PROCESSED_DIR

CACHE_HIT_RATE_PATH = PROCESSED_DIR / "cache_hit_rate_summary.json"
LATENCY_PROOF_PATH = PROCESSED_DIR / "latency_proof_summary.json"
MONITORING_SNAPSHOT_PATH = PROCESSED_DIR / "monitoring_snapshot.json"


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return round(values[0], 2)
    ordered = sorted(values)
    rank = int(round((percentile / 100.0) * (len(ordered) - 1)))
    return round(ordered[rank], 2)


def _measure_ms(func, *args, **kwargs) -> float:
    start = time.perf_counter()
    func(*args, **kwargs)
    return round((time.perf_counter() - start) * 1000, 2)


def collect_cache_hit_rate() -> dict[str, object]:
    scenarios = [
        (
            "search",
            search,
            {"q": "self healing high temperature material", "top_k": 8, "candidate_pool": 128},
        ),
        ("context", context_lookup, {"lat": 26.3, "lon": 73.0, "mode": "auto"}),
        (
            "recommend",
            recommend,
            {
                "q": "Design a self healing high temperature material for Rajasthan desert deployment",
                "lat": 26.3,
                "lon": 73.0,
                "context_mode": "auto",
                "top_k": 8,
                "candidate_pool": 128,
            },
        ),
    ]

    total_calls = 0
    hit_calls = 0
    by_operation: dict[str, dict[str, float]] = {}

    for name, func, kwargs in scenarios:
        first = func(**kwargs)
        second = func(**kwargs)
        flags = [bool(getattr(first, "cache", {}).get("hit")), bool(getattr(second, "cache", {}).get("hit"))]
        op_total = len(flags)
        op_hits = sum(1 for flag in flags if flag)
        total_calls += op_total
        hit_calls += op_hits
        by_operation[name] = {
            "calls": op_total,
            "hits": op_hits,
            "hit_rate": round((op_hits / op_total) * 100.0, 2),
        }

    payload = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "calls_total": total_calls,
        "hits_total": hit_calls,
        "hit_rate_percent": round((hit_calls / total_calls) * 100.0, 2) if total_calls else 0.0,
        "by_operation": by_operation,
    }
    CACHE_HIT_RATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def collect_latency_proof(iterations: int = 10) -> dict[str, object]:
    operations = {
        "search": (
            search,
            {"q": "self healing high temperature material", "top_k": 8, "candidate_pool": 128},
        ),
        "context": (context_lookup, {"lat": 26.3, "lon": 73.0, "mode": "auto"}),
        "recommend": (
            recommend,
            {
                "q": "Design a self healing high temperature material for Rajasthan desert deployment",
                "lat": 26.3,
                "lon": 73.0,
                "context_mode": "auto",
                "top_k": 8,
                "candidate_pool": 128,
            },
        ),
    }

    per_operation: dict[str, dict[str, object]] = {}
    for name, (func, kwargs) in operations.items():
        samples = [_measure_ms(func, **kwargs) for _ in range(iterations)]
        per_operation[name] = {
            "iterations": iterations,
            "samples_ms": samples,
            "min_ms": round(min(samples), 2),
            "max_ms": round(max(samples), 2),
            "avg_ms": round(statistics.mean(samples), 2),
            "p50_ms": _percentile(samples, 50),
            "p95_ms": _percentile(samples, 95),
            "p99_ms": _percentile(samples, 99),
        }

    payload = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "summary": per_operation,
    }
    LATENCY_PROOF_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def collect_monitoring_snapshot() -> dict[str, object]:
    payload = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "api_metrics": metrics(),
    }
    MONITORING_SNAPSHOT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    benchmark_payload = run_benchmark()
    cache_payload = collect_cache_hit_rate()
    latency_payload = collect_latency_proof(iterations=10)
    monitoring_payload = collect_monitoring_snapshot()

    summary = {
        "benchmark_results": benchmark_payload.get("results", []),
        "cache_hit_rate_percent": cache_payload.get("hit_rate_percent", 0.0),
        "latency_p95_ms": {
            name: details.get("p95_ms")
            for name, details in latency_payload.get("summary", {}).items()
        },
        "monitoring_metrics": monitoring_payload.get("api_metrics", {}),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
