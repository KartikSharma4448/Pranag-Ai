# Remaining Gap Backlog

Updated: 2026-04-11

## P0

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Live GenBank metadata validation | open | data platform | 2026-04-15 | Validate official nuccore metadata retention and refresh policy against a production run. |
| Live UniProt, AlphaFold, boltz1, and PDB structure-ID coverage | open | biology ingestion | 2026-04-18 | Confirm official endpoint behavior, ID mapping, and update cadence. |
| IMD and Bhuvan production access | blocked | platform ops | 2026-04-12 | Obtain approvals, credentials, and usage policy sign-off for live Indian context data. |
| AgriStack official connector | blocked | platform ops / client | 2026-04-22 | Confirm whether approved land-record integration is required and implement the approved connector. |
| Production benchmark evidence | done | platform / QA | 2026-04-14 | Evidence captured in data/processed/benchmark_summary.json via scripts/collect_runtime_evidence.py. |

## P1

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Nature and Materials Today scheduled ingestion | open | literature pipeline | 2026-04-16 | Run scheduled literature ingest and confirm daily refresh behavior. |
| Optional LLM literature extraction in configured env | open | literature pipeline | 2026-04-17 | Enable the LLM path in a controlled environment and smoke test structured JSON output. |
| Scheduler as managed service | done | platform ops | 2026-04-19 | Windows cron-equivalent integration added via scripts/register_scheduler_task.ps1 with scheduler module support. |
| Cache hit-rate proof | done | platform / QA | 2026-04-20 | Evidence captured in data/processed/cache_hit_rate_summary.json (100% for the measured warm-cache workload). |
| Latency proof for search/context/recommend | done | platform / QA | 2026-04-20 | Evidence captured in data/processed/latency_proof_summary.json with p95 metrics per endpoint. |

## P2

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Live AFLOW and OQMD feeds | surrogate | materials ingestion | 2026-04-26 | Replace surrogate material records with live official feeds if access is available. |
| Live ZINC20 feed | surrogate | chemistry ingestion | 2026-04-26 | Replace surrogate metadata with official purchasable-compound feed if available. |
| Live NASA, NIST, and OpenFOAM feeds | surrogate | physics ingestion | 2026-04-28 | Replace surrogate records with official datasets or approved source mirrors. |
| Scale validation for 4M entities and 500M rows | open | platform / QA | 2026-04-30 | Execute a reproducible scale run and verify row counts and performance. |
| Production monitoring and alerting | done | platform ops | 2026-04-24 | Baseline dashboard and alert thresholds added in ops/monitoring-dashboard-baseline.md and ops/dashboards/universal-index-ops-dashboard.json. |

## Notes

- The repo now supports the architecture shape in code.
- The remaining items are mostly external integration, production access, and evidence collection.
- Keep this file aligned with [production-readiness-checklist.md](production-readiness-checklist.md).

## Team 1 Data Curation Refinement Plan (2026-04-11)

| Refinement | Current State | Priority | Owner | Next Action |
| --- | --- | --- | --- | --- |
| Real-time ingestion pipelines (APIs, RSS feeds) | done | P0 | literature pipeline + platform | Scheduler + feed loader integrated in universal_index/distributed_ingest.py and universal_index/scheduler.py. |
| Differential ingestion logic | done | P0 | data platform | Feed watermark + per-source paper-id dedupe implemented in universal_index/feeds.py. |
| Redis Streams for ingestion events | done | P0 | platform | Publish hooks + Redis stream consumer implemented (universal_index/stream_consumer.py). |
| Context API global scalability | done | P1 | context platform | Global lat/lon handling with provider fallback path implemented in universal_index/context.py. |
| Multi-lingual query support (Hindi/Telugu/English) | done | P1 | search + API | Query language detection + normalization implemented in universal_index/language_support.py and api/main.py. |
| Confidence scores with uncertainty estimates for surrogate cache | done | P1 | recommendation + ML | Confidence, uncertainty estimate, and interval outputs added in universal_index/vector_search.py and universal_index/api_models.py. |
| Visual dashboards for index growth and paper miner outputs | done | P2 | platform ops + analytics | Dashboard baseline artifacts added in ops/monitoring-dashboard-baseline.md and ops/dashboards/universal-index-ops-dashboard.json. |

### Additional Inputs Required (Team 1)

- Approved multilingual stack decision: managed translation API vs open-source model hosting.
- Redis deployment details: retention policy, max stream length, consumer group naming.
- SLO targets: ingestion freshness, paper-miner daily throughput, API latency p95/p99.
- Uncertainty policy: acceptable confidence thresholds per entity type.
- Dashboard runtime decision: Grafana-only vs lightweight internal dashboard app.

### Primary Risks

- External API quotas and schema drift can break continuous ingestion without robust retries and contracts.
- Translation quality for scientific terminology can reduce retrieval precision without domain lexicon tuning.
- Confidence scoring without ground-truth calibration may create false precision in recommendations.

## Status Update Format

- `open`: work not started or awaiting planning.
- `blocked`: work depends on an external approval, credential, or upstream decision.
- `surrogate`: implemented with generated or fallback data, but not yet live official source coverage.
- `done`: verified in production-like conditions and backed by evidence artifacts.

Recommended weekly update fields:

- Date
- Status
- Evidence
- Blocker
- Next Action
