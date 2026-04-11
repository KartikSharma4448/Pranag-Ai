# Team 1 Lead Control Tower

Updated: 2026-04-11

## What We Are Building Together

A single production-minded scientific intelligence system where multiple domain pipelines feed one unified index and APIs:

- Ingestion pipelines (biology, materials, chemistry, physics/simulation, literature, context)
- Unified schema and index build
- Vector retrieval + recommendations
- Context-aware API outputs
- Scheduler-driven refresh and ops monitoring

In short: all teams are jointly building one "universal scientific operating layer" for search, context, and recommendations.

## Team System Map (Leader View)

## Team 1: Data Curation + Real-Time Pipeline (Your Team)

Mission:
- Keep ingestion fresh, differential, and reliable.
- Ensure stream/event flow, multilingual query normalization, and confidence metadata are available.

Owns:
- universal_index/distributed_ingest.py
- universal_index/feeds.py
- universal_index/scheduler.py
- universal_index/language_support.py
- universal_index/stream_consumer.py

Daily Questions for Team 1:
- Did ingestion run complete end-to-end today?
- How many new feed papers were ingested vs skipped (dedupe)?
- Any source failing due to schema drift, quota, or URL/proxy issues?
- Are Redis stream events being published and consumed?
- Are multilingual queries normalized correctly in Hindi/Telugu/English?

## Team 2: Search + Recommendation Intelligence

Mission:
- Keep retrieval quality high and recommendation outputs stable.

Owns:
- universal_index/vector_search.py
- universal_index/recommendation.py
- universal_index/api_models.py

Daily Questions for Team 2:
- What is p95 latency for search and recommend?
- Are confidence and uncertainty fields populated and calibrated?
- Any ranking regressions vs previous benchmark?
- Any empty-result edge cases or crashes observed?

## Team 3: API + Context + Integration

Mission:
- Ensure API contracts stay stable and context enrichment works globally.

Owns:
- api/main.py
- universal_index/context.py
- provider adapters under universal_index/providers/

Daily Questions for Team 3:
- Are /search, /context, /recommend APIs healthy and backward compatible?
- Which context providers are live vs fallback in current environment?
- Any auth, rate-limit, or contract compatibility issues with consumers?
- Any high-error endpoints in the last 24h?

## Team 4: Platform Ops + Production Readiness

Mission:
- Turn system into reliable production operation with evidence and approvals.

Owns:
- ops/remaining-gap-backlog.md
- ops/production-readiness-checklist.md
- scripts/collect_runtime_evidence.py
- ops/dashboards/universal-index-ops-dashboard.json

Daily Questions for Team 4:
- Which P0/P1 gaps are still open or blocked?
- Are latest evidence artifacts regenerated after current runs?
- Any external approvals/credentials pending (IMD/Bhuvan/AgriStack/providers)?
- Is scheduler job healthy and alert thresholds within limits?

## Cross-Team Questions You Should Ask Every Day

- What changed in last 24h that can break another team?
- Which blocker is internal vs external dependency?
- What is the single highest-risk item for go-live today?
- Which proof artifact was produced today?
- What needs leadership decision today (policy, SLA, approval, infra)?

## Leader Runbook (15-Min Daily)

1. Health Snapshot (3 min)
- API health, latest ingestion status, scheduler status, critical errors.

2. Data Freshness Snapshot (3 min)
- New records by source, failed sources, dedupe count, feed lag.

3. Quality Snapshot (3 min)
- Search/recommend latency p95, confidence distribution, empty-results trend.

4. Risk + Dependency Snapshot (3 min)
- External approvals, quotas, provider issues, schema drift risks.

5. Decision Log (3 min)
- Capture owner, deadline, and success metric for each decision.

## RAG Format for Team Standup Updates

Use this exact format from each team:

- R (Red): blocker and impact in one line
- A (Amber): at-risk item and mitigation
- G (Green): completed output with evidence file path

Example:
- Red: Crossref API failing due to endpoint mismatch; feed papers count impacted.
- Amber: Scheduler retries reduced failures, but source mapping patch pending.
- Green: latency_proof_summary.json regenerated and p95 under target.

## Meeting-Ready Status Statement (Use Verbatim)

"All teams are building one unified scientific retrieval and recommendation platform. Team 1 is currently ensuring real-time ingestion reliability, differential updates, multilingual normalization, and event streaming. Team 2 is ensuring retrieval quality and confidence outputs. Team 3 is maintaining API/context contract stability. Team 4 is driving production evidence, monitoring, and external approvals. Current technical risk is concentrated in external source reliability and approval dependencies, while core pipeline execution is stable."
