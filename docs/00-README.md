# FleetForge: Distributed Build Scheduler

## Architecture & Design Documentation

This is the design-doc set for a lightweight distributed build scheduler: the control plane that manages a fleet of build workers, similar in spirit to the Kubernetes scheduler, Buildkite's agent scheduler, and GitHub Actions' runner manager, purpose-built for CI/CD build jobs.

**These documents predate the implementation.** They capture the architecture, contracts, and data models the code was built against. Read them in order; each one builds on the last.

| # | Document | Covers |
|---|----------|--------|
| 1 | `01-architecture-overview.md` | System diagram, component responsibilities, tech stack + rationale |
| 2 | `openapi.yaml` (repo root) | Full REST API contract (OpenAPI 3.0) |
| 3 | `03-database-schema.md` | PostgreSQL ER diagram, DDL, indexes, partitioning |
| 4 | `04-redis-data-model.md` | Every Redis key, its shape, TTL, and purpose |
| 5 | `05-sequence-diagrams.md` | Registration/heartbeat/failure detection + job lifecycle flows |
| 6 | `06-failure-scenarios.md` | Every failure mode: detection, recovery, mitigation |
| 7 | `07-repository-structure.md` | Repo layout, module boundaries |
| 8 | `08-implementation-roadmap.md` | Implementation plan, milestones, commits |
| 9 | `09-design-rationale.md` | Every major decision, alternatives, trade-offs, what to revisit |

## Working name

The system is called **FleetForge** throughout these docs, mainly to avoid writing "the scheduler" for ten pages straight. The name isn't load-bearing and can change freely.

## What "done" means for this doc set

A senior engineer who has never seen this project should be able to read documents 1-9 and: (a) draw the system from memory, (b) know exactly what every worker/job field means and what state machine governs it, (c) know what happens when any single component dies, and (d) start writing code against the API contract without needing a single clarifying question.
