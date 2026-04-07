# PRANA-G Universal Scientific Index

A production-minded MVP for cross-domain scientific retrieval and recommendation across biology, materials, chemistry, simulation, and environment context.

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-API-green)
![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-orange)
![ChromaDB](https://img.shields.io/badge/ChromaDB-Vector%20Search-purple)
![Status](https://img.shields.io/badge/Status-MVP%20Ready-brightgreen)

Author: Kartik Sharma  
Copyright: (c) Kartik Sharma

## Quick Status

- Repo build: complete
- API flow: working
- Search + Recommend: validated
- Tests: passing
- CLI + GUI launchers: available

## Live Demo Snapshot

```text
Recommend Query:
Design a self healing high temperature material for Rajasthan desert deployment

Output Categories:
material, molecule, gene, soil
```

<details>
<summary>Screenshot Section (click to expand)</summary>

You can add screenshots in this repo path and reference them here:

- `docs/screenshots/gui-home.png`
- `docs/screenshots/api-docs.png`
- `docs/screenshots/recommend-output.png`

Example markdown once images are added:

```markdown
![GUI Home](docs/screenshots/gui-home.png)
![API Docs](docs/screenshots/api-docs.png)
![Recommend Output](docs/screenshots/recommend-output.png)
```

</details>

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [System Architecture](#system-architecture)
- [How Data Flows](#how-data-flows)
- [API Endpoints](#api-endpoints)
- [Quick Start](#quick-start)
- [CLI and GUI Launch](#cli-and-gui-launch)
- [Configuration](#configuration)
- [Operations Docs](#operations-docs)
- [Project Structure](#project-structure)
- [Validation](#validation)
- [Production Notes](#production-notes)

---

## What This Project Does

This project creates a single searchable scientific layer where different domains can be queried together.

### Domain coverage

- Biology: gene and related metadata entities
- Materials: material properties and structures
- Chemistry: molecule records and descriptors
- Physics/Simulation: simulation-style entities and properties
- Environment: location context with soil/climate/agriculture payload

### Core output

One query can return relevant entities from multiple domains, plus location-aware recommendation context.

---

## System Architecture

```text
Sources (official + fallback)
  -> Raw snapshots + lake partitions
  -> Universal schema normalization
  -> DuckDB + Parquet analytical index
  -> Vector index (ChromaDB embeddings)
  -> FastAPI endpoints
  -> Cache + state tracking + ops artifacts
```

<details>
<summary>Components (click to expand)</summary>

- Ingestion: universal_index/distributed_ingest.py
- Schema: universal_index/schema.py
- Index build: universal_index/build.py
- Vector pipeline: universal_index/vector_search.py
- Context merge: universal_index/context.py
- API server: api/main.py
- Cache: universal_index/cache.py
- State tracking: universal_index/state.py
- Object storage sync: universal_index/storage.py

</details>

---

## How Data Flows

### Ingestion flow

```text
distributed_ingest
  -> parallel source loads
  -> write lake partitions
  -> update run/source state
  -> materialize universal index
  -> optional vector refresh
  -> optional object storage upload
```

### Context flow

```text
/context(lat, lon)
  -> local baseline CSV
  -> official providers (IMD/Bhuvan) if configured
  -> free fallback providers if enabled
  -> agri proxy generation
  -> unified context response
```

---

## API Endpoints

Base docs URL:

http://127.0.0.1:8000/docs

### Core

- GET /health
- GET /metrics
- GET /ops/state
- GET /search
- GET /context
- GET /recommend
- GET /literature/status
- GET /ingestion/status
- GET /entities/high-temperature

<details>
<summary>Example requests</summary>

- /search?q=self healing high temperature material
- /context?lat=26.3&lon=73.0&mode=auto
- /recommend?q=Design a self healing high temperature material for Rajasthan desert deployment&lat=26.3&lon=73.0&context_mode=auto

</details>

---

## Quick Start

### 1) Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 2) Create env file

```powershell
Copy-Item .env.example .env
```

### 3) Build index and vectors

```powershell
python -m universal_index.build
python -m universal_index.vector_search
```

### 4) Start API

```powershell
python -m uvicorn api.main:app --reload
```

### 5) Run tests

```powershell
python -m unittest discover -s tests -p "test_*.py"
```

---

## CLI and GUI Launch

- CLI launcher: start_cli.py
- GUI launcher: start_gui.py

### CLI

```powershell
python start_cli.py
```

### GUI

```powershell
python start_gui.py
```

---

## Configuration

### Main toggles

- CACHE_BACKEND=duckdb|redis
- LIVE_CONTEXT_MODE=local|auto|live
- API_AUTH_ENABLED=true|false
- RATE_LIMIT_ENABLED=true|false

### Free fallback toggles

- FREE_CONTEXT_CLIMATE_ENABLED=true|false
- FREE_CONTEXT_SOIL_ENABLED=true|false
- AGRISTACK_PROXY_ENABLED=true|false

### Object storage toggles

- OBJECT_STORAGE_ENABLED=true|false
- OBJECT_STORAGE_BUCKET=<bucket>
- OBJECT_STORAGE_ENDPOINT_URL=<endpoint for minio/dev>
- OBJECT_STORAGE_ACCESS_KEY_ID=<key>
- OBJECT_STORAGE_SECRET_ACCESS_KEY=<secret>

---

## Operations Docs

The repo includes practical operational documentation and scripts:

- ops/budget-sheet.csv
- ops/access-register.md
- ops/secrets-inventory.md
- ops/storage-decision.md
- ops/backup-policy.md
- ops/production-readiness-checklist.md
- scripts/backup_local.ps1
- scripts/restore_local.ps1

---

## Project Structure

```text
Kartik-Work/
├─ api/
├─ data/
├─ tests/
├─ universal_index/
│  └─ providers/
├─ ops/
├─ scripts/
├─ start_cli.py
├─ start_gui.py
├─ .env.example
├─ requirements.txt
└─ README.md
```

---

## Validation

Validated locally:

- /context: returns unified payload
- /search: returns multi-domain entities
- /recommend: returns cross-domain recommended combination keys
- test suite: passing

---

## Production Notes

Current repo is ready as deployable MVP foundation.  
External production dependencies still require organization-level completion:

- Official IMD/Bhuvan/AgriStack approvals where mandatory
- Cloud IAM/service-account governance rollout
- Production monitoring and alerting hookup

---

## License

Copyright (c) Kartik Sharma.
All rights reserved unless explicitly stated otherwise.
