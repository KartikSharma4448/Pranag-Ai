# Monitoring Dashboard Baseline

Updated: 2026-04-11

## Scope

This baseline defines the minimum dashboard and alert set required for production monitoring coverage.

## Dashboard Panels

1. API throughput and errors
- Source: `/metrics`
- Metrics: `requests_total`, `errors_total`, `rate_limited_total`, `auth_rejected_total`

2. API latency trend
- Source: `data/processed/latency_proof_summary.json`
- Metrics: `avg_ms`, `p95_ms`, `p99_ms` for search, context, recommend

3. Ingestion freshness
- Source: `data/processed/distributed_ingestion_summary.json`
- Metrics: latest `run_id`, `rows_total`, `sources_ingested`

4. Cache effectiveness
- Source: `data/processed/cache_hit_rate_summary.json`
- Metrics: total hit rate and per-operation hit rate

5. Literature pipeline health
- Source: `data/processed/paper_ingest_summary.json`
- Metrics: papers ingested, entities extracted

## Alert Thresholds

1. API errors
- Trigger: `errors_total` growth > 2% of request growth over 5-minute windows

2. API latency
- Trigger: search/recommend `p95_ms > 1500` OR context `p95_ms > 200` for 10 minutes

3. Ingestion stall
- Trigger: no new ingestion `run_id` for > 2 scheduler intervals

4. Cache regression
- Trigger: overall hit rate < 40% for a stable query mix

## Evidence Artifacts

- `data/processed/benchmark_summary.json`
- `data/processed/cache_hit_rate_summary.json`
- `data/processed/latency_proof_summary.json`
- `data/processed/monitoring_snapshot.json`

## Implementation Notes

- Dashboard can be built in Grafana by reading JSON artifacts and API metrics.
- If external observability is not available, this baseline still supports repository-backed audit evidence.
