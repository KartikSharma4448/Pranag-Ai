# Production Readiness Checklist

Updated: 2026-04-08 (post vector dedup verification)

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
- [ ] Scheduler deployed as a managed service or equivalent production job runner
- [ ] Cache hit rate measured against production-like query mix and confirmed above target
- [ ] Search, context, and recommend latency measured under load and recorded
- [ ] Universal index size target validated with a reproducible row-count report
- [ ] AgriStack land-record integration replaced with an approved official connector if required

## Evidence Required

- [x] `data/processed/benchmark_summary.json` captured from a real run
- [x] `data/processed/build_summary.json` updated after the latest ingestion pass
- [ ] `data/processed/distributed_ingestion_summary.json` updated after the latest scheduler or manual run
- [x] `data/processed/vector_index_summary.json` updated after vector rebuild
- [ ] `data/processed/paper_ingest_summary.json` updated after the latest literature ingest
- [ ] Production owner sign-off recorded for each mandatory external provider

## Latest Validation Snapshot

- Build completed with `rows_total=4900` and wrote `data/processed/universal_index.parquet`
- Vector index rebuild completed with `rows_indexed=4900`
- Benchmark run completed with timings: search `5709.14 ms`, context `18.34 ms`, recommend `215.82 ms`
- Unit tests passed: `8 passed` via `python -m unittest discover -s tests -q`

## Go-Live Gate

Go live if all completed items are true, the remaining production gaps are either closed or explicitly accepted, the required evidence files exist, and mandatory external approvals are received.

See also: [remaining-gap-backlog.md](remaining-gap-backlog.md)
