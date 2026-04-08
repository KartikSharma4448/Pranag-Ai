# CEO Presentation Brief: PRANA-G Status

Date: 2026-04-08

## Slide 1: What Is Ready Today

### Title
PRANA-G: Universal Scientific Index Is Operational as an MVP

### Key points
- One unified schema now spans biology, materials, chemistry, physics, and India context.
- Search, recommend, context, literature ingest, and cache layers are implemented and wired into the API.
- Expanded source adapters now cover GenBank metadata, UniProt, PDB, AlphaFold, boltz1, PubChem, ChEMBL, ZINC20, AFLOW, OQMD, NASA, NIST, OpenFOAM, Bhuvan, IMD, and Copernicus projection logic.
- Optional LLM-based literature extraction is available for controlled environments.
- Benchmark runner exists to record search, context, recommend, and cache timings.

### Proof points
- API endpoints are live: `/search`, `/context`, `/recommend`, `/literature/status`, `/ingestion/status`.
- CLI and GUI launchers are available.
- Production checklist and remaining-gap backlog are now tracked in ops docs.

### Speaker note
We have a working MVP foundation that connects multiple scientific domains into one retrieval layer. The system is usable and extensible, but the live-provider and scale story still needs production approvals and evidence.

## Slide 2: What Is Still Pending

### Title
What Remains Before Production Go-Live

### Key points
- Official live access still needs approval for IMD, Bhuvan, AgriStack, and other external feeds where required.
- Some source families are still surrogate-based until official feeds or credentials are available.
- Production evidence is still needed for scale, latency, and cache-hit-rate targets.
- Scheduler should move from startup-based execution to a managed job runner or cron-equivalent service.
- Literature automation still needs a configured LLM environment and scheduled daily validation.

### Risks to call out
- The repo supports the architecture, but not every external source is yet a production-grade live connector.
- 4M entities / 500M rows / <100ms claims are goals, not yet proven production metrics.
- AgriStack remains proxy-based until an approved official integration is confirmed.

### Speaker note
The product is now demonstrably real, but the final production gap is external access and evidence. The next milestone is to close approvals, run benchmarks, and validate live feeds in a controlled environment.

## One-minute talk track
- We have a working cross-domain scientific index and API stack.
- We added broader source coverage, optional LLM extraction, and a benchmark path.
- What remains is production access, validation, and scale proof.
- So the message for the CEO is: MVP is ready, production hardening is the next phase.
