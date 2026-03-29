# Copyright (c) Kartik Sharma. GitHub: kartiksharma4448
from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
LAKE_DIR = DATA_DIR / "lake"

PARQUET_PATH = PROCESSED_DIR / "universal_index.parquet"
DUCKDB_PATH = PROCESSED_DIR / "universal_index.duckdb"
VALIDATION_CSV_PATH = PROCESSED_DIR / "temperature_over_45.csv"
BUILD_SUMMARY_PATH = PROCESSED_DIR / "build_summary.json"
INGESTION_SUMMARY_PATH = PROCESSED_DIR / "distributed_ingestion_summary.json"
VECTOR_DOCUMENTS_PATH = PROCESSED_DIR / "vector_documents.parquet"
VECTOR_QUERY_RESULTS_PATH = PROCESSED_DIR / "semantic_query_results.json"
VECTOR_SUMMARY_PATH = PROCESSED_DIR / "vector_index_summary.json"
CONTEXT_DATASET_PATH = RAW_DIR / "location_context.csv"
DEMO_RECOMMENDATION_PATH = PROCESSED_DIR / "demo_recommendation.json"
SURROGATE_CACHE_PATH = PROCESSED_DIR / "surrogate_cache.duckdb"
LITERATURE_PAPERS_RAW_PATH = RAW_DIR / "literature_papers.csv"
LITERATURE_ENTITY_PATH = PROCESSED_DIR / "literature_entities.parquet"
LITERATURE_SUMMARY_PATH = PROCESSED_DIR / "paper_ingest_summary.json"
PIPELINE_STATE_PATH = PROCESSED_DIR / "pipeline_state.duckdb"
CHROMA_DIR = PROCESSED_DIR / "chroma"
CHROMA_COLLECTION_NAME = "universal_index_day2"
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
SEARCH_CACHE_TTL_SECONDS = int(os.getenv("SEARCH_CACHE_TTL_SECONDS", "3600"))
CONTEXT_CACHE_TTL_SECONDS = int(os.getenv("CONTEXT_CACHE_TTL_SECONDS", "1800"))
RECOMMEND_CACHE_TTL_SECONDS = int(os.getenv("RECOMMEND_CACHE_TTL_SECONDS", "900"))
CACHE_BACKEND = os.getenv("CACHE_BACKEND", "duckdb").strip().lower()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_PREFIX = os.getenv("REDIS_KEY_PREFIX", "pranag")
REDIS_STREAM_KEY = os.getenv("REDIS_STREAM_KEY", "pranag:ingestion:events")
LIVE_CONTEXT_MODE = os.getenv("LIVE_CONTEXT_MODE", "auto").strip().lower()
LIVE_CONTEXT_TIMEOUT_SECONDS = int(os.getenv("LIVE_CONTEXT_TIMEOUT_SECONDS", "20"))
IMD_API_BASE_URL = os.getenv("IMD_API_BASE_URL", "").strip()
IMD_API_URL_TEMPLATE = os.getenv("IMD_API_URL_TEMPLATE", "").strip()
IMD_API_KEY = os.getenv("IMD_API_KEY", "").strip()
IMD_RESOURCE_ID = os.getenv("IMD_RESOURCE_ID", "").strip()
DATA_GOV_API_BASE_URL = os.getenv("DATA_GOV_API_BASE_URL", "https://api.data.gov.in")
BHUVAN_WMS_URL = os.getenv("BHUVAN_WMS_URL", "https://bhuvan-vec2.nrsc.gov.in/bhuvan/wms")
BHUVAN_WMS_LAYER = os.getenv("BHUVAN_WMS_LAYER", "").strip()
BHUVAN_INFO_FORMAT = os.getenv("BHUVAN_INFO_FORMAT", "application/json").strip()
BHUVAN_PLACE_NAME = os.getenv("BHUVAN_PLACE_NAME", "Bhuvan thematic layer").strip()
INGESTION_MAX_WORKERS = int(os.getenv("INGESTION_MAX_WORKERS", "4"))
API_AUTH_ENABLED = os.getenv("API_AUTH_ENABLED", "false").strip().lower() == "true"
API_KEY_HEADER_NAME = os.getenv("API_KEY_HEADER_NAME", "x-api-key").strip()
API_ACCESS_KEY = os.getenv("API_ACCESS_KEY", "").strip()
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").strip().lower() == "true"
RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "60"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
JSON_LOGS = os.getenv("JSON_LOGS", "true").strip().lower() == "true"
SCHEDULER_ENABLED = os.getenv("SCHEDULER_ENABLED", "false").strip().lower() == "true"
SCHEDULER_INTERVAL_MINUTES = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "60"))

DEFAULT_COUNTS = {
    "genes": 500,
    "materials": 500,
    "molecules": 500,
    "soil": 200,
    "simulations": 150,
}

ENTREZ_EMAIL = os.getenv("ENTREZ_EMAIL", "codex@example.com")
NCBI_API_KEY = os.getenv("NCBI_API_KEY")
MP_API_KEY = os.getenv("MP_API_KEY") or os.getenv("PMG_MAPI_KEY")
RANDOM_SEED = int(os.getenv("UNIVERSAL_INDEX_SEED", "42"))
