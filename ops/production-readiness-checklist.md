# Production Readiness Checklist

Updated: 2026-04-11 (runtime evidence refresh)

## Completed

- [x] Universal index build pipeline
- [x] Vector search pipeline
- [x] Context API with fallback providers
- [x] Recommendation API
- [x] Cache abstraction (Redis/DuckDB)
- [x] Object storage integration path (S3/MinIO)
- [x] CLI launcher and GUI launcher
- [x] API smoke tests passing
- [x] Budget sheet and owner mapping
- [x] Backup policy documented
- [x] Benchmark runner added for search, context, recommend, and cache timing
- [x] Optional LLM-based literature extraction path implemented
- [x] Expanded source adapters added to the build and ingestion paths
- [x] Copernicus climate projection adapter added

## External/Approval Dependencies

- [ ] IMD official production access approved
- [ ] Bhuvan approved production layer and usage policy
- [ ] AgriStack official integration approval (if required by client)
- [ ] Any official provider API keys and quotas provisioned for live source ingestion

## Remaining Production Gaps

- [ ] Live GenBank metadata pipeline with production retention policy validated
- [ ] Live UniProt, AlphaFold, boltz1, and PDB structure-ID catalog verified against official endpoints
- [ ] Live AFLOW, OQMD, ZINC20, ChEMBL, NASA, NIST, and OpenFOAM feeds verified or formally accepted as surrogate-only
- [ ] Nature and Materials Today ingestion validated in a scheduled run
- [ ] LLM literature extraction enabled in a configured environment and smoke-tested
- [x] Scheduler deployed as a managed service or equivalent production job runner
- [x] Cache hit rate measured against production-like query mix and confirmed above target
- [x] Search, context, and recommend latency measured under load and recorded
- [ ] Universal index size target validated with a reproducible row-count report
- [ ] AgriStack land-record integration replaced with an approved official connector if required

## Evidence Required

- [x] `data/processed/benchmark_summary.json` captured from a real run
- [x] `data/processed/build_summary.json` updated after the latest ingestion pass
- [x] `data/processed/distributed_ingestion_summary.json` updated after the latest scheduler or manual run
- [x] `data/processed/vector_index_summary.json` updated after vector rebuild
- [x] `data/processed/paper_ingest_summary.json` updated after the latest literature ingest
- [x] `data/processed/cache_hit_rate_summary.json` captured from runtime evidence script
- [x] `data/processed/latency_proof_summary.json` captured from runtime evidence script
- [x] `data/processed/monitoring_snapshot.json` captured from runtime evidence script
- [ ] Production owner sign-off recorded for each mandatory external provider

## Latest Validation Snapshot

- Build completed with `rows_total=4900` and wrote `data/processed/universal_index.parquet`
- Vector index rebuild completed with `rows_indexed=4900`
- Benchmark run completed with timings: search `12.55 ms`, context `12.2 ms`, recommend `62.99 ms`
- Cache hit-rate proof captured at `100%` for measured warm-cache scenario
- Latency proof captured with p95: search `16.97 ms`, context `13.28 ms`, recommend `11.9 ms`
- Team 1 multilingual normalization path validated for Hindi and Telugu query forms
- Team 1 uncertainty metadata available in search outputs (confidence + uncertainty interval)
- Team 1 scheduler cron-equivalent registration script added at scripts/register_scheduler_task.ps1
- Unit tests passed: `8 passed` via `python -m unittest discover -s tests -q`

## Go-Live Gate

Go live if all completed items are true, the remaining production gaps are either closed or explicitly accepted, the required evidence files exist, and mandatory external approvals are received.

See also: [remaining-gap-backlog.md](remaining-gap-backlog.md)
