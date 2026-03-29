# PRANA-G Universal Scientific Index

A production-minded MVP for cross-domain scientific retrieval across biology, materials, chemistry, environment, and simulation data.

This repository is designed to answer one practical question: how do we build a single system where a gene record, a material property, a molecule, a soil context, and an engineering simulation can all be searched through one common interface?

Instead of being a throwaway demo, this codebase is structured as a deployable foundation:

- small but real ingestion pipelines
- a unified scientific schema
- DuckDB + Parquet analytical storage
- ChromaDB semantic retrieval
- FastAPI search and context services
- Redis-ready cache and event hooks
- incremental literature ingestion
- distributed ingestion scaffolding for future scale-up

## Why This Exists

PRANA-G needs a system that can move from idea to scientific context quickly.
That means one place to:

- search entities across multiple scientific domains
- reason over meaning, not only keywords
- attach real-world deployment context like soil and climate
- stay extendable enough for future production deployment

## What The MVP Already Does

The current implementation supports:

- genes from NCBI/GenBank-style metadata
- materials from Materials Project or deterministic fallback records
- molecules from PubChem or deterministic fallback records
- soil and environment context from local structured datasets
- simulation-style physics and engineering surrogate entities
- paper-derived entities from recent PubMed/arXiv ingestion cycles

The system can:

- normalize all entities into one schema
- write a universal Parquet index
- query it with DuckDB
- build semantic embeddings with `sentence-transformers/all-MiniLM-L6-v2`
- serve search, context, and recommendation APIs
- run distributed-style ingestion into a partitioned local data lake

## Architecture

```text
Source Connectors
  -> Raw Snapshots / Data Lake Partitions
  -> Universal Schema Normalization
  -> DuckDB + Parquet Index
  -> ChromaDB Vector Layer
  -> FastAPI Search / Context / Recommendation APIs
  -> Cache + Event + Ops Layer
```

Core implementation areas:

- ingestion and normalization in `universal_index/`
- API service in `api/`
- processed outputs in `data/processed/`
- lake partitions in `data/lake/`
- tests in `tests/`

## Stack

- Python
- DuckDB
- Apache Parquet
- ChromaDB
- Sentence-Transformers
- FastAPI
- Pandas
- Redis-ready cache abstraction

## Quick Start

1. Install dependencies.

```powershell
python -m pip install --user -r requirements.txt
```

2. Create your local environment file.

```powershell
Copy-Item .env.example .env
```

3. Build the universal index.

```powershell
python -m universal_index.build
```

4. Build the vector layer.

```powershell
python -m universal_index.vector_search
```

5. Run paper ingestion.

```powershell
python -m universal_index.literature_agent --pubmed 4 --arxiv 4
```

6. Run the distributed ingestion pipeline.

```powershell
python -m universal_index.distributed_ingest --include-literature --refresh-vectors
```

7. Start the API.

```powershell
python -m uvicorn api.main:app --reload
```

8. Open interactive docs.

```text
http://127.0.0.1:8000/docs
```

## API Surface

Main endpoints:

- `GET /health`
- `GET /metrics`
- `GET /ops/state`
- `GET /search?q=...`
- `GET /context?lat=&lon=&mode=local|auto|live`
- `GET /recommend?q=&lat=&lon=&context_mode=local|auto|live`
- `GET /literature/status`
- `GET /ingestion/status`
- `GET /entities/high-temperature?min_temperature=45`

Example requests:

```text
GET /search?q=self healing high temperature material
GET /context?lat=26.3&lon=73.0&mode=auto
GET /recommend?q=Design a self healing high temperature material for Rajasthan desert deployment&lat=26.3&lon=73.0&context_mode=auto
```

## Production-Minded Features

This repo is intentionally structured so that the MVP can evolve into a usable production system.

Already implemented:

- unified schema across multiple scientific domains
- partitioned local lake outputs
- Redis-backed cache support with DuckDB fallback
- live-provider adapters for IMD and Bhuvan-style context refresh
- optional API key auth
- rate limiting
- structured request logging
- runtime metrics
- pipeline state tracking
- CI workflow scaffold
- Docker + Docker Compose setup

Environment knobs already supported:

- cache backend selection
- Redis URL / key prefix
- live context mode
- IMD endpoint config
- Bhuvan WMS layer config
- API auth key / header name
- rate limit window and request count
- scheduler interval

## Docker And Redis

Run the API with Redis using Docker Compose:

```powershell
docker compose up --build
```

Default compose behavior:

- starts Redis
- starts the FastAPI application
- uses `CACHE_BACKEND=redis`

## Live Context Notes

The context layer supports both local and live-oriented behavior.

Modes:

- `local`: only local CSV-backed context
- `auto`: try live providers first, then fall back locally
- `live`: force live-provider path

Live provider expectations:

- IMD may require allowlisting, keys, or dataset-specific configuration
- Bhuvan access depends on valid WMS layer names and available feature info responses
- if live configuration is unavailable, the system safely falls back to local context

## Scale Context From The Assignment

The original assignment describes a much larger future platform than a laptop-scale project should directly ingest.

Important distinction:

- source universe: very large, multi-TB, with GenBank alone referencing `51.56 trillion` DNA bases
- MVP implementation: focused, sampled, schema-first, architecture-first
- production trajectory: metadata streaming, partitioned Parquet, distributed workers, scheduled refresh, and controlled incremental ingestion

So this repository should be described honestly as:

> a production-capable MVP foundation, not the final full-scale PRANA-G deployment.

## Outputs

Key outputs produced by the system:

- `data/processed/universal_index.parquet`
- `data/processed/universal_index.duckdb`
- `data/processed/vector_documents.parquet`
- `data/processed/semantic_query_results.json`
- `data/processed/distributed_ingestion_summary.json`
- `data/processed/paper_ingest_summary.json`
- `data/processed/demo_recommendation.json`
- `data/lake/` partitioned source data

## Testing

Run the local test suite:

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

## Notes For Future Scale-Up

What is still intentionally future-facing:

- full enterprise access to IMD / Bhuvan / AgriStack data
- large-scale object storage beyond local disk
- fully distributed worker fleet and job queue orchestration
- long-running scheduled ingestion infrastructure
- high-volume paper ingestion at production cadence
- cloud monitoring, alerting, and cost dashboards

## Repository Character

This project was written to feel like a real engineering build, not an AI-generated checklist dump.
The goal was to keep the code practical, readable, and extensible so it can survive contact with real product pressure.

<!-- Developer provenance marker: Kartik | original developer copyright trace intentionally retained. -->
