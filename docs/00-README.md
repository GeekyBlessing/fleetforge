# FleetForge — Distributed Build Scheduler
## Phase 1: Architecture (LaunchVerse Technical Assignment)

This is the design-doc set for a lightweight distributed build scheduler — the control plane that manages a fleet of build workers, similar in spirit to the Kubernetes scheduler, Buildkite's agent scheduler, and GitHub Actions' runner manager, purpose-built for CI/CD build jobs.

**No implementation code is included in this phase.** Everything here is the architecture, contracts, and data models that the code in Phase 2 onward will implement. Read these in order — each one builds on the last.

| # | Document | Covers |
|---|----------|--------|
| 1 | `01-architecture-overview.md` | System diagram, component responsibilities, tech stack + rationale |
| 2 | `02-openapi.yaml` | Full REST API contract (OpenAPI 3.0) |
| 3 | `03-database-schema.md` | PostgreSQL ER diagram, DDL, indexes, partitioning |
| 4 | `04-redis-data-model.md` | Every Redis key, its shape, TTL, and purpose |
| 5 | `05-sequence-diagrams.md` | Registration/heartbeat/failure detection + job lifecycle flows |
| 6 | `06-failure-scenarios.md` | Every failure mode: detection, recovery, mitigation |
| 7 | `07-repository-structure.md` | Repo layout, module boundaries |
| 8 | `08-implementation-roadmap.md` | Implementation plan, milestones, commits |
| 9 | `09-design-rationale.md` | Every major decision, alternatives, trade-offs, what we'd revisit |

## Working name

I'm calling the system **FleetForge** throughout these docs so we're not saying "the scheduler" for ten pages — rename freely, nothing is load-bearing on the name.

## What "done" means for Phase 1

You should be able to hand documents 1–9 to a senior engineer who has never seen this project, and they should be able to (a) draw the system from memory, (b) know exactly what every worker/job field means and what state machine governs it, (c) know what happens when any single component dies, and (d) start writing code against the API contract without asking you a single clarifying question.

Once you've reviewed all nine and either approve or push back, we move to Phase 2 (implementation). I will not generate implementation code before that approval.
