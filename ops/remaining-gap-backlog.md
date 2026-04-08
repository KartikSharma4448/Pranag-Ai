# Remaining Gap Backlog

Updated: 2026-04-08

## P0

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Live GenBank metadata validation | open | data platform | 2026-04-15 | Validate official nuccore metadata retention and refresh policy against a production run. |
| Live UniProt, AlphaFold, boltz1, and PDB structure-ID coverage | open | biology ingestion | 2026-04-18 | Confirm official endpoint behavior, ID mapping, and update cadence. |
| IMD and Bhuvan production access | blocked | platform ops | 2026-04-12 | Obtain approvals, credentials, and usage policy sign-off for live Indian context data. |
| AgriStack official connector | blocked | platform ops / client | 2026-04-22 | Confirm whether approved land-record integration is required and implement the approved connector. |
| Production benchmark evidence | open | platform / QA | 2026-04-14 | Run benchmark harness against a production-like index and archive results. |

## P1

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Nature and Materials Today scheduled ingestion | open | literature pipeline | 2026-04-16 | Run scheduled literature ingest and confirm daily refresh behavior. |
| Optional LLM literature extraction in configured env | open | literature pipeline | 2026-04-17 | Enable the LLM path in a controlled environment and smoke test structured JSON output. |
| Scheduler as managed service | open | platform ops | 2026-04-19 | Move the scheduler from process startup to a managed job runner or cron-equivalent service. |
| Cache hit-rate proof | open | platform / QA | 2026-04-20 | Measure cache hit rate on a representative workload and record the result. |
| Latency proof for search/context/recommend | open | platform / QA | 2026-04-20 | Capture response-time measurements under load and store the benchmark summary artifact. |

## P2

| Gap | Status | Owner | Target Date | Next Action |
| --- | --- | --- | --- | --- |
| Live AFLOW and OQMD feeds | surrogate | materials ingestion | 2026-04-26 | Replace surrogate material records with live official feeds if access is available. |
| Live ZINC20 feed | surrogate | chemistry ingestion | 2026-04-26 | Replace surrogate metadata with official purchasable-compound feed if available. |
| Live NASA, NIST, and OpenFOAM feeds | surrogate | physics ingestion | 2026-04-28 | Replace surrogate records with official datasets or approved source mirrors. |
| Scale validation for 4M entities and 500M rows | open | platform / QA | 2026-04-30 | Execute a reproducible scale run and verify row counts and performance. |
| Production monitoring and alerting | open | platform ops | 2026-04-24 | Add alert thresholds and dashboards for ingestion, cache, and API latency. |

## Notes

- The repo now supports the architecture shape in code.
- The remaining items are mostly external integration, production access, and evidence collection.
- Keep this file aligned with [production-readiness-checklist.md](production-readiness-checklist.md).

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
