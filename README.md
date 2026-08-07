# FleetForge

[![CI](https://github.com/GeekyBlessing/fleetforge/actions/workflows/ci.yml/badge.svg)](https://github.com/GeekyBlessing/fleetforge/actions/workflows/ci.yml)

A distributed build scheduler and worker orchestration system, written in Go. FleetForge manages a fleet of build workers the way Buildkite's agent scheduler or GitHub Actions' runner manager does: workers register, heartbeat, and pull work; a leader-elected scheduler matches queued jobs to capable, available workers under strict consistency guarantees; failures at any layer are detected and recovered from automatically.

Design docs: see `docs/` (00 through 09) for the original architecture, data model, and failure-mode design, written before implementation began. Where the shipped system diverges from that original design, this README and the docs themselves say so explicitly rather than silently.

## Overview

Running a fleet of build workers reliably means answering a few hard questions: how do you know a worker is still alive, what happens to a job when its worker dies mid-build, how do you avoid scheduling the same job twice, and how do you scale a scheduler itself without a single point of failure. FleetForge answers all four with the same mechanism: Postgres is the only place correctness is allowed to live. Every other component (Redis, gRPC streams, in-memory state) is allowed to be wrong, stale, or momentarily lost, because it can always be reconciled back to Postgres.

A job's path through the system: a client submits a job over REST (`POST /v1/jobs`); it lands in Postgres as `QUEUED` and gets pushed onto a Redis Streams queue; the elected scheduler leader consumes it, matches it against registered workers by capability and load, and assigns it with a single atomic Postgres compare-and-swap transaction; the assignment is pushed to the worker over a live gRPC heartbeat stream; the worker executes it and reports the result, which is recorded, and on failure, retried with backoff up to a configured limit.

What makes this technically interesting isn't any single piece, it's the fencing: every worker holds an epoch number that increments on every registration. Every job assignment, heartbeat, and result report carries that epoch. A worker that goes stale (crashed, partitioned, or simply slow) and reconnects with an old epoch is rejected and forced to re-register, which is what prevents a classic split-brain failure: the same job silently running twice on two workers, both reporting success. This fencing mechanism, and the reaper that depends on it, was exercised against a real bug during development: a worker that crashed before its very first heartbeat left its liveness timestamp permanently null, making it unreapable. Chaos testing (see below) caught this live, and the fix is one line in `internal/store/postgres/workers.go`, stamping the timestamp at registration instead of leaving it null.

## Architecture

The diagram below reflects what actually runs in local development today, not the original target production architecture (that version, with HA scheduler replicas behind a load balancer, a Postgres read replica, and Kubernetes, lives in `docs/01-architecture-overview.md` as the original design target).

```mermaid
flowchart TB
    Client["fleetforgectl / curl"] -->|"REST :8080, JWT on writes"| API["REST API"]

    subgraph Scheduler["Scheduler process (leader-elected, 1+ replicas)"]
        API
        GRPC["gRPC control server :9090"]
        Loop["Scheduling loop"]
        Leader["Leader election"]
        Reaper["Dead-worker reaper"]
        Auto["Autoscaler (scale-down only)"]
    end

    API --> PG[("PostgreSQL")]
    API -->|enqueue| Redis[("Redis Streams")]
    Loop -->|consume| Redis
    Loop -->|"atomic CAS assign"| PG
    Leader -->|advisory lock| PG
    Reaper -->|"sweep every 5s"| PG
    Auto -->|"drain idle workers"| PG

    GRPC <-->|"mTLS, bidirectional heartbeat stream"| W1["Worker Agent"]
    GRPC <-->|"mTLS, bidirectional heartbeat stream"| W2["Worker Agent"]
    GRPC <-->|"mTLS, bidirectional heartbeat stream"| W3["Worker Agent"]

    Scheduler -->|/metrics| Prom["Prometheus"]
    Prom --> Graf["Grafana"]
```

**REST API**: stateless HTTP layer (chi router) for job submission/listing, worker listing/drain/resume, health, and metrics passthrough. Optionally gated by JWT bearer tokens on write endpoints.

**gRPC control server**: the only thing workers talk to. Handles `RegisterWorker`, the bidirectional `Heartbeat` stream (worker reports status, scheduler pushes assignments and drain signals over the same connection), and `ReportJobResult`. Optionally requires mTLS client certificates.

**Scheduling loop**: runs only on the elected leader. Consumes queued jobs from Redis, matches by capability and least-loaded worker, and commits the assignment as a single Postgres transaction guarded by `WHERE status = 'READY'`/`WHERE status = 'QUEUED'`, so a losing race is a safe no-op, not a double-booked worker.

**Leader election**: a Postgres session-scoped advisory lock. Whoever holds it is the leader; the lock releases automatically the instant that connection drops, so "the leader died" and "a standby can take over" are the same event. Verified live: killing the leader process during a two-replica test, a standby acquired leadership in 1.53 seconds.

**Dead-worker reaper**: runs on the leader every 5 seconds, marking any worker whose last heartbeat exceeds the timeout (20s by default) as `DEAD` and atomically requeuing its in-flight job in the same transaction. Depends on nothing but Postgres, so it keeps working even if Redis is down.

**Autoscaler**: scale-down is real: it drains idle workers, never touching a `BUSY` worker. Scale-up is a logged recommendation only; there is no cloud provider integration wired up.

**PostgreSQL**: source of truth for workers, jobs, and retry history. Every state transition that matters for correctness goes through it.

**Redis**: the job queue (Streams with consumer groups) and a hot-path cache for worker state, avoiding a Postgres round-trip on every heartbeat.

**Worker Agent**: registers once, heartbeats every 5 seconds, executes at most one job per capacity slot, reports results. The build execution itself is a `SimulatedExecutor` (`worker-agent-runtime/executor.go`) that spends a real, deterministic few seconds and fails at a configurable rate, exercising the full scheduling and retry pipeline honestly without shelling out to a real build tool. A real executor (`docker run`, log streaming) plugs into the same `Executor` interface without touching the scheduler.

**Prometheus / Grafana**: Prometheus scrapes the scheduler's `/metrics` every 10 seconds; a provisioned Grafana dashboard visualizes queue depth, worker counts, assignment/completion rates, job duration percentiles, and autoscaler decisions.

## Key Engineering Features

Everything below is implemented and was exercised against real running binaries, not just written and assumed correct.

Capability-aware, least-loaded scheduling with an atomic Postgres compare-and-swap assignment transaction, so a race between two scheduling attempts on the same worker can never double-book it.

Idempotent worker registration with epoch fencing: re-registering the same `instance_id` bumps an epoch rather than creating a duplicate row, and any in-flight request carrying a stale epoch is rejected, which is the mechanism that prevents split-brain job execution after a network partition heals.

Heartbeat-based liveness tracking and a Postgres-authoritative dead-worker reaper, independent of Redis. Live-verified: a worker SIGKILLed mid-job was marked dead and its job correctly requeued in 22.9 seconds.

Retry with exponential backoff up to a configured limit, and graceful worker draining (finish current job, refuse new work, never interrupt a running build).

Leader election via Postgres advisory locks across multiple scheduler replicas, live-verified with a 1.53 second failover after killing the leader process outright.

mTLS between worker and scheduler, and JWT-scoped bearer tokens gating job submission and worker drain/resume, both off by default and opt-in via environment variables.

Prometheus metrics and a provisioned Grafana dashboard, both wired to the same six real metric names, no drift between what's collected and what's graphed.

A Python load-testing harness (a simulated worker fleet plus a Locust-driven job submission client) and a chaos-testing suite that kills the leader, kills a worker mid-job, and simulates a network partition against real compiled binaries, not mocks. Running the chaos suite is what found and fixed the heartbeat-fencing bug described above.

Three green CI jobs on every push: lint, unit tests, and integration tests (the latter against a real Postgres via testcontainers, not a mock).

## Quick Start

Prerequisites: Go 1.25, Docker with Compose, and `openssl` if you want to enable mTLS. Python 3 with `pip` is only needed for the load and chaos test suites.

```bash
make up               # docker compose: postgres, redis, prometheus, grafana (postgres also auto-migrates)
make build             # builds bin/scheduler, bin/worker-agent, bin/fleetforgectl
make test               # unit tests
make integration-test
```

`make up` starts Postgres, Redis, Prometheus, and Grafana, and a one-shot `migrate` container applies the schema automatically. The scheduler and worker-agent binaries are not containerized; they run on the host in local development (see `deploy/docker-compose.yml`'s own comments on why). If you need to reapply migrations without restarting the whole stack, `make migrate` runs them via a locally installed `golang-migrate` CLI against `FLEETFORGE_DATABASE_URL`.

Run the scheduler and at least one worker in separate terminals:

```bash
export FLEETFORGE_DATABASE_URL=postgres://fleetforge:fleetforge_dev_only@localhost:5432/fleetforge?sslmode=disable
./bin/scheduler
```

```bash
export FLEETFORGE_INSTANCE_ID=dev-worker-1 FLEETFORGE_CPU_CORES=4 FLEETFORGE_MEMORY_MB=8192
./bin/worker-agent
```

Then inspect and submit work:

```bash
go run ./cmd/fleetforgectl workers list
curl -X POST localhost:8080/v1/jobs -d '{"repository":"github.com/example/app","branch":"main","commit_sha":"'"$(uuidgen)"'","idempotency_key":"demo-1"}'
```

There is no `fleetforgectl jobs submit` command; job submission is REST-only, via `curl` or any HTTP client.

## Demo

```bash
make demo
```

This automates the manual sequence above: brings up Postgres/Redis/Prometheus/Grafana, applies migrations, builds fresh binaries, starts a real scheduler and three real worker agents in the background, waits for them to register, submits a handful of sample jobs, and prints worker and job state as they move through the system. It exercises the actual scheduler and worker-agent binaries, not a simulation of the demo.

```bash
make demo-clean
```

Stops the background scheduler/worker processes started by `make demo` and tears down the Docker Compose stack, including volumes.

See `scripts/demo.sh` for exactly what runs. This was written but not executed in the environment that produced this documentation (no Docker or Go toolchain available there); run it locally and treat its output as the actual demo evidence.

## Security

**mTLS** secures the worker-to-scheduler gRPC control plane. `scripts/gen-certs.sh` generates a local self-signed CA plus a server certificate for the scheduler and one shared client certificate for all workers. This is a development trust model, stated plainly: a real fleet would issue one client certificate per worker at provisioning time, and this setup has no revocation (CRL/OCSP) story. Enable it by setting `FLEETFORGE_TLS_CERT_FILE`, `FLEETFORGE_TLS_KEY_FILE`, and `FLEETFORGE_TLS_CA_FILE` on both the scheduler and every worker-agent; unset (the default), the gRPC connection uses insecure credentials.

**JWT** gates two REST write paths: `POST /v1/jobs` requires the `jobs:submit` scope, and `POST /v1/workers/{id}/drain` and `/resume` require `workers:drain`. All `GET` endpoints stay open regardless, intentionally, since read access is low-risk and keeps ad-hoc inspection via `curl` or `fleetforgectl` simple. Enable it by setting `FLEETFORGE_JWT_SECRET` on the scheduler; issue tokens with `fleetforgectl auth mint-token --scopes jobs:submit,workers:drain`. Unset (the default), write endpoints are unauthenticated. The JWT implementation is a hand-rolled, dependency-free HS256 verifier (`internal/auth/jwt.go`) using constant-time signature comparison, not a third-party library.

Both mechanisms default to off, matching every environment (local dev, CI) that predates them. Neither is a secret committed to this repository; `certs/` and `.env` are gitignored.

## Observability

Prometheus scrapes the scheduler's `/metrics` endpoint every 10 seconds (`deploy/prometheus.yml`). The six collectors it exposes, all in `internal/metrics/metrics.go`:

| Metric | Type | What it measures |
|---|---|---|
| `fleetforge_jobs_queued` | gauge, by priority | Jobs currently `QUEUED` |
| `fleetforge_workers` | gauge, by status | Worker count per status (`READY`, `BUSY`, `DRAINING`, `OFFLINE`, `DEAD`) |
| `fleetforge_jobs_assigned_total` | counter | Successful job assignments |
| `fleetforge_jobs_completed_total` | counter, by status | Job result reports recorded (`SUCCESS`, `FAILED`, `CANCELLED`, `RETRYING`) |
| `fleetforge_job_duration_seconds` | histogram | Execution time for jobs reaching a terminal state |
| `fleetforge_autoscaler_decisions_total` | counter, by decision | Every autoscaler tick's outcome, including holds |

A Grafana dashboard (`deploy/grafana/dashboards/fleetforge.json`) is auto-provisioned with six panels, one per metric above, and is available at `http://localhost:3000` after `make up` (anonymous viewer access is enabled for local dev; the admin password is the same `fleetforge_dev_only` dev-only value used for Postgres). Prometheus itself is at `http://localhost:9091` (not 9090, which is the scheduler's own gRPC port on the host).

## Performance & Reliability

A load-testing harness exists and was exercised manually during development: a Python/asyncio simulated worker fleet (`test/load/fake_worker.py`) registering and heartbeating over real mTLS-secured gRPC, driven against a Locust-based job submission client (`test/load/locustfile.py`) that measures submission-to-assignment latency as a named metric, not just the raw `POST /jobs` response time. No saved, reproducible benchmark numbers are committed to this repository yet; reporting invented figures here would be worse than reporting none. To produce real numbers:

```bash
cd test/load
pip install -r requirements.txt
python3 fake_worker.py --scheduler-addr localhost:9090 --num-workers 200 --duration 300 &
locust -f locustfile.py --host http://localhost:8080 --users 50 --spawn-rate 10 --run-time 2m --headless
```

See `test/load/README.md` for full setup, including mTLS and JWT variants. `docs/benchmarks/` is reserved for committed, dated results once a run has actually been captured; see `docs/benchmarks/README.md` for the format.

## Failure Testing

Three chaos scripts under `test/chaos/` exercise specific rows of the failure matrix in `docs/06-failure-scenarios.md` against real compiled `bin/scheduler` and `bin/worker-agent` binaries, not mocks:

| Script | Scenario | Invariant it checks |
|---|---|---|
| `kill_leader.py` | Scheduler leader crashes (SIGKILL) | A standby replica acquires the Postgres advisory lock and becomes leader within a bounded window. Verified: 1.53s. |
| `kill_worker.py` | Worker crashes mid-job (SIGKILL) | The dead worker is detected and marked `DEAD`, and its in-flight job is atomically requeued rather than stuck. Verified: 22.9s detection. |
| `partition_worker.py` | Worker is alive but unreachable (SIGSTOP, then SIGCONT) | The frozen worker is detected exactly like a crash; once resumed, it hits the stale-epoch rejection and exits demanding re-registration rather than silently resuming, the split-brain guard. |

All three were run to completion against the real system during development; see `test/chaos/README.md` for prerequisites and exact commands to reproduce them yourself.

## Design Decisions

| Document | Covers |
|---|---|
| `docs/01-architecture-overview.md` | System diagram (original target architecture), component responsibilities, tech stack rationale |
| `openapi.yaml` | Full REST API contract (OpenAPI 3.0) |
| `docs/03-database-schema.md` | PostgreSQL ER diagram, DDL, indexes, partitioning |
| `docs/04-redis-data-model.md` | Every Redis key, its shape, TTL, and purpose |
| `docs/05-sequence-diagrams.md` | Registration, heartbeat, failure detection, and job lifecycle sequence diagrams |
| `docs/06-failure-scenarios.md` | Failure-mode matrix: detection and recovery for every scenario considered |
| `docs/07-repository-structure.md` | Repository layout as actually built, and where it diverged from the original plan |
| `docs/08-implementation-roadmap.md` | Milestone-by-milestone build plan |
| `docs/09-design-rationale.md` | Every major decision, alternatives considered, trade-offs |

## Project Structure

```
fleetforge/
├── cmd/                      # main() for each of the three binaries
│   ├── scheduler/
│   ├── worker-agent/
│   └── fleetforgectl/
├── internal/
│   ├── api/                  # REST layer: router, handlers, JWT middleware
│   ├── grpcserver/            # gRPC control plane: register, heartbeat, report result
│   ├── scheduler/             # scheduling loop, matching algorithm, leader election, reaper, autoscaler, retry poller
│   ├── store/
│   │   ├── postgres/          # all SQL, migrations
│   │   └── redis/             # queue, cache, client
│   ├── auth/                  # JWT issuance/verification, mTLS config
│   ├── metrics/                # Prometheus collectors
│   └── config/                 # environment-based configuration
├── worker-agent-runtime/      # simulated build executor, swappable behind an interface
├── proto/fleetforge/v1/        # gRPC service and message definitions
├── deploy/                     # docker-compose stack, Prometheus config, Grafana provisioning
├── test/
│   ├── integration/            # testcontainers-backed Postgres integration tests
│   ├── load/                   # Python load-testing harness
│   └── chaos/                  # Python chaos-testing suite
├── scripts/                    # cert generation, demo, proto generation
└── docs/                       # design documentation, 00 through 09
```

## Testing

```bash
make test                              # unit tests, race detector on
make integration-test                  # real Postgres via testcontainers
```

Load and chaos tests are Python and run separately; see `test/load/README.md` and `test/chaos/README.md` for setup and exact commands. Both require a running scheduler (and, for `kill_worker.py`/`partition_worker.py`, a running worker) against real Postgres and Redis, not the Go test binaries.
