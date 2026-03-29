# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import json
import logging
import threading
import time
from collections import defaultdict
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from universal_index.api_models import ContextResponse, RecommendResponse, SearchResponse
from universal_index.cache import build_cache_backend, make_cache_key
from universal_index.config import (
    API_ACCESS_KEY,
    API_AUTH_ENABLED,
    API_KEY_HEADER_NAME,
    CHROMA_COLLECTION_NAME,
    CHROMA_DIR,
    CONTEXT_CACHE_TTL_SECONDS,
    EMBEDDING_MODEL_NAME,
    INGESTION_SUMMARY_PATH,
    JSON_LOGS,
    LIVE_CONTEXT_MODE,
    LITERATURE_SUMMARY_PATH,
    PARQUET_PATH,
    RATE_LIMIT_ENABLED,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_WINDOW_SECONDS,
    RECOMMEND_CACHE_TTL_SECONDS,
    SEARCH_CACHE_TTL_SECONDS,
)
from universal_index.context import lookup_context
from universal_index.recommendation import (
    apply_context_filter,
    dataframe_to_records,
    summarize_recommended_combination,
)
from universal_index.state import PipelineStateStore
from universal_index.vector_search import semantic_search

app = FastAPI(title="Universal Index API", version="0.3.0")
cache = build_cache_backend()
state_store = PipelineStateStore()
logger = logging.getLogger("universal_index.api")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

AUTH_RUNTIME_ENABLED = API_AUTH_ENABLED and bool(API_ACCESS_KEY)
AUTH_EXEMPT_PATHS = {"/health", "/docs", "/openapi.json", "/redoc", "/metrics"}
_rate_limit_lock = threading.Lock()
_rate_limit_buckets: dict[str, list[float]] = defaultdict(list)
_metrics_lock = threading.Lock()
_metrics = {
    "requests_total": 0,
    "errors_total": 0,
    "auth_rejected_total": 0,
    "rate_limited_total": 0,
    "request_duration_ms_total": 0.0,
}
_path_counters: dict[str, int] = defaultdict(int)


@app.on_event("startup")
def startup_checks() -> None:
    if API_AUTH_ENABLED and not API_ACCESS_KEY:
        logger.warning("API_AUTH_ENABLED=true but API_ACCESS_KEY is empty; auth is disabled at runtime.")


@app.middleware("http")
async def production_guardrails(request: Request, call_next: Callable):
    start = time.perf_counter()
    path = request.url.path
    client_host = request.client.host if request.client else "unknown"

    if AUTH_RUNTIME_ENABLED and path not in AUTH_EXEMPT_PATHS:
        provided_key = request.headers.get(API_KEY_HEADER_NAME)
        if provided_key != API_ACCESS_KEY:
            _increment_metric("auth_rejected_total", path)
            return JSONResponse(status_code=401, content={"detail": "Invalid or missing API key."})

    if RATE_LIMIT_ENABLED and path not in AUTH_EXEMPT_PATHS:
        if not _allow_request(client_host=client_host):
            _increment_metric("rate_limited_total", path)
            return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded."})

    response = await call_next(request)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)
    _record_request(path=path, duration_ms=duration_ms, status_code=response.status_code)
    _log_request(
        method=request.method,
        path=path,
        status_code=response.status_code,
        duration_ms=duration_ms,
        client_host=client_host,
    )
    return response


@app.get("/health")
def health() -> dict[str, object]:
    parquet_path = Path(PARQUET_PATH)
    chroma_path = Path(CHROMA_DIR)
    literature_path = Path(LITERATURE_SUMMARY_PATH)
    ingestion_path = Path(INGESTION_SUMMARY_PATH)
    return {
        "status": "ok",
        "parquet_ready": parquet_path.exists(),
        "vector_ready": chroma_path.exists(),
        "literature_agent_ready": literature_path.exists(),
        "distributed_ingestion_ready": ingestion_path.exists(),
        "live_context_mode": LIVE_CONTEXT_MODE,
        "auth_enabled": AUTH_RUNTIME_ENABLED,
        "rate_limit_enabled": RATE_LIMIT_ENABLED,
        "cache": cache.stats(),
        "pipeline_state": state_store.latest_run(),
    }


@app.get("/metrics")
def metrics() -> dict[str, object]:
    with _metrics_lock:
        requests_total = int(_metrics["requests_total"])
        avg_duration_ms = 0.0
        if requests_total > 0:
            avg_duration_ms = round(_metrics["request_duration_ms_total"] / requests_total, 2)
        return {
            "requests_total": requests_total,
            "errors_total": int(_metrics["errors_total"]),
            "auth_rejected_total": int(_metrics["auth_rejected_total"]),
            "rate_limited_total": int(_metrics["rate_limited_total"]),
            "average_request_duration_ms": avg_duration_ms,
            "path_counters": dict(_path_counters),
        }


@app.get("/ops/state")
def ops_state() -> dict[str, object]:
    return state_store.summary()


@app.get("/literature/status")
def literature_status() -> dict[str, object]:
    summary_path = Path(LITERATURE_SUMMARY_PATH)
    if not summary_path.exists():
        raise HTTPException(
            status_code=404,
            detail="No literature ingest summary found. Run `python -m universal_index.literature_agent` first.",
        )
    return json.loads(summary_path.read_text(encoding="utf-8"))


@app.get("/ingestion/status")
def ingestion_status() -> dict[str, object]:
    summary_path = Path(INGESTION_SUMMARY_PATH)
    if not summary_path.exists():
        raise HTTPException(
            status_code=404,
            detail="No distributed ingestion summary found. Run `python -m universal_index.distributed_ingest` first.",
        )
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    payload["pipeline_state"] = state_store.latest_run()
    payload["source_states"] = state_store.source_states()
    return payload


@app.get("/entities/high-temperature")
def high_temperature_entities(
    min_temperature: float = Query(default=45.0, ge=-273.15),
    limit: int = Query(default=25, ge=1, le=250),
) -> dict[str, object]:
    parquet_path = Path(PARQUET_PATH)
    if not parquet_path.exists():
        raise HTTPException(
            status_code=503,
            detail="Build the universal index first with `python -m universal_index.build`.",
        )

    query = """
        SELECT *
        FROM read_parquet(?)
        WHERE temperature_max > ?
        ORDER BY temperature_max DESC NULLS LAST
        LIMIT ?
    """

    with duckdb.connect() as connection:
        results = connection.execute(query, [str(parquet_path), min_temperature, limit]).fetchdf()

    return {
        "rows": len(results),
        "items": dataframe_to_records(results),
    }


@app.get("/context", response_model=ContextResponse)
def context_lookup(
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    mode: str = Query(default=LIVE_CONTEXT_MODE, pattern="^(local|auto|live)$"),
) -> ContextResponse:
    cache_key = make_cache_key({"lat": round(lat, 4), "lon": round(lon, 4), "mode": mode})
    cached = cache.get("context", cache_key)
    if cached is not None:
        cached["cache"] = {"hit": True, "cache_key": cache_key}
        return ContextResponse.model_validate(cached)

    try:
        payload = lookup_context(lat=lat, lon=lon, mode=mode)
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=500, detail=str(error)) from error

    cache.set("context", cache_key, payload, ttl_seconds=CONTEXT_CACHE_TTL_SECONDS)
    payload["cache"] = {"hit": False, "cache_key": cache_key}
    return ContextResponse.model_validate(payload)


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., min_length=3, description="Scientific search prompt"),
    top_k: int = Query(default=8, ge=1, le=25),
    candidate_pool: int = Query(default=128, ge=8, le=512),
) -> SearchResponse:
    cache_key = make_cache_key({"q": q, "top_k": top_k, "candidate_pool": candidate_pool})
    cached = cache.get("search", cache_key)
    if cached is not None:
        cached["cache"] = {"hit": True, "cache_key": cache_key}
        return SearchResponse.model_validate(cached)

    try:
        results = semantic_search(
            query_text=q,
            top_k=top_k,
            candidate_pool=candidate_pool,
            model_name=EMBEDDING_MODEL_NAME,
            chroma_dir=CHROMA_DIR,
            collection_name=CHROMA_COLLECTION_NAME,
            parquet_path=PARQUET_PATH,
        )
    except FileNotFoundError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    except Exception as error:
        raise HTTPException(status_code=500, detail=f"Semantic search failed: {error}") from error

    payload = {
        "query": q,
        "rows": len(results),
        "items": dataframe_to_records(results),
    }
    cache.set("search", cache_key, payload, ttl_seconds=SEARCH_CACHE_TTL_SECONDS)
    payload["cache"] = {"hit": False, "cache_key": cache_key}
    return SearchResponse.model_validate(payload)


@app.get("/recommend", response_model=RecommendResponse)
def recommend(
    q: str = Query(..., min_length=3, description="Scientific design prompt"),
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
    context_mode: str = Query(default=LIVE_CONTEXT_MODE, pattern="^(local|auto|live)$"),
    top_k: int = Query(default=8, ge=1, le=25),
    candidate_pool: int = Query(default=128, ge=8, le=512),
) -> RecommendResponse:
    cache_key = make_cache_key(
        {
            "q": q,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "context_mode": context_mode,
            "top_k": top_k,
            "candidate_pool": candidate_pool,
        }
    )
    cached = cache.get("recommend", cache_key)
    if cached is not None:
        cached["cache"] = {"hit": True, "cache_key": cache_key}
        return RecommendResponse.model_validate(cached)

    context_payload = context_lookup(lat=lat, lon=lon, mode=context_mode).model_dump()
    search_payload = search(
        q=q,
        top_k=max(top_k * 2, top_k),
        candidate_pool=candidate_pool,
    ).model_dump()

    vector_results = pd.DataFrame(search_payload["items"])
    recommendations = apply_context_filter(
        results=vector_results,
        context=context_payload,
        prompt=q,
        final_limit=top_k,
    )

    payload = {
        "prompt": q,
        "context": context_payload,
        "vector_hits": search_payload["items"],
        "recommended_combination": summarize_recommended_combination(recommendations),
        "final_recommendations": dataframe_to_records(recommendations),
    }
    cache.set("recommend", cache_key, payload, ttl_seconds=RECOMMEND_CACHE_TTL_SECONDS)
    payload["cache"] = {"hit": False, "cache_key": cache_key}
    return RecommendResponse.model_validate(payload)


def _allow_request(client_host: str) -> bool:
    now = time.time()
    with _rate_limit_lock:
        bucket = _rate_limit_buckets[client_host]
        cutoff = now - RATE_LIMIT_WINDOW_SECONDS
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)
        if len(bucket) >= RATE_LIMIT_REQUESTS:
            return False
        bucket.append(now)
    return True


def _increment_metric(metric_name: str, path: str) -> None:
    with _metrics_lock:
        _metrics[metric_name] += 1
        _path_counters[path] += 1


def _record_request(path: str, duration_ms: float, status_code: int) -> None:
    with _metrics_lock:
        _metrics["requests_total"] += 1
        _metrics["request_duration_ms_total"] += duration_ms
        _path_counters[path] += 1
        if status_code >= 400:
            _metrics["errors_total"] += 1


def _log_request(
    method: str,
    path: str,
    status_code: int,
    duration_ms: float,
    client_host: str,
) -> None:
    payload = {
        "event": "http_request",
        "method": method,
        "path": path,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "client_host": client_host,
    }
    if JSON_LOGS:
        logger.info(json.dumps(payload))
    else:
        logger.info("%s %s -> %s in %.2fms from %s", method, path, status_code, duration_ms, client_host)
