# 7. Repository Structure

Single Go module (monorepo): scheduler and worker-agent share types, proto definitions, and store clients, which matters because the fencing/epoch logic and status enums must never drift between the two binaries. Python appears only for load and chaos tooling, where its ecosystem (Locust, asyncio, scripting ergonomics around process signals) genuinely beats Go for iteration speed on that kind of glue code.

This document originally described the planned structure before implementation began. It now describes what was actually built. Section 7.2 lists the specific places the two diverge and why.

```
fleetforge/
├── cmd/
│   ├── scheduler/              # main() for the scheduler binary
│   ├── worker-agent/           # main() for the worker agent binary
│   └── fleetforgectl/          # operator CLI: workers list/drain/resume, auth mint-token
│
├── internal/
│   ├── api/                    # REST layer (chi router)
│   │   ├── handlers_workers.go
│   │   ├── handlers_jobs.go
│   │   ├── middleware_auth.go       # JWT scope verification
│   │   └── router.go
│   │
│   ├── grpcserver/              # gRPC control plane (worker-facing)
│   │   ├── register.go
│   │   ├── heartbeat.go
│   │   └── report_result.go
│   │
│   ├── scheduler/               # scheduling core
│   │   ├── loop.go                  # main scheduling loop, leader-gated
│   │   ├── algorithm.go             # capability match + least-loaded selection
│   │   ├── leader.go                # Postgres advisory-lock leader election
│   │   ├── reaper.go                # dead-worker detection (doc 5.3)
│   │   ├── retrypoller.go           # retry backoff scheduling
│   │   └── autoscaler.go            # scale-down (real) / scale-up (advisory only)
│   │
│   ├── store/
│   │   ├── postgres/
│   │   │   ├── workers.go
│   │   │   ├── jobs.go
│   │   │   ├── db.go
│   │   │   └── migrations/          # golang-migrate SQL files
│   │   └── redis/
│   │       ├── queue.go             # Redis Streams job queue
│   │       ├── cache.go             # worker state hash helpers
│   │       └── client.go
│   │
│   ├── queue/
│   │   └── backend.go               # queue.Backend interface (Redis Streams today)
│   │
│   ├── auth/
│   │   ├── jwt.go                   # dependency-free HS256 issuance/verification
│   │   └── mtls.go                  # TLS config loading, CA pool
│   │
│   ├── metrics/
│   │   ├── metrics.go                # all counters/gauges/histograms
│   │   └── collector.go              # periodic gauge refresh from Postgres
│   │
│   └── config/
│       └── config.go                 # env-based config, shared by scheduler and worker-agent
│
├── proto/
│   └── fleetforge/v1/
│       ├── worker.proto
│       ├── scheduler.proto           # RegisterWorker, Heartbeat (stream), ReportJobResult
│       └── *.pb.go                    # generated Go stubs, committed
│
├── worker-agent-runtime/          # build-execution logic invoked by the worker agent
│   └── executor.go                 # SimulatedExecutor behind an Executor interface
│
├── deploy/
│   ├── docker-compose.yml          # postgres + redis + prometheus + grafana + a one-shot migrate job
│   ├── prometheus.yml
│   └── grafana/
│       ├── provisioning/            # datasource + dashboard auto-provisioning config
│       └── dashboards/fleetforge.json
│
├── docs/                          # this design-doc set
│   └── 00-README.md ... 09-design-rationale.md
│
├── test/
│   ├── integration/                 # testcontainers-backed Postgres integration tests
│   ├── load/                        # Python + Locust: simulated worker fleet + job submission driver
│   └── chaos/                       # Python scripts against real scheduler/worker-agent binaries
│       ├── kill_leader.py           # scenario #1: leader crash
│       ├── kill_worker.py           # scenario #3: worker crash mid-job
│       └── partition_worker.py      # scenario #6: worker frozen/partitioned, not crashed
│
├── scripts/
│   ├── gen-certs.sh                 # local dev CA + server/client certs for mTLS
│   ├── gen-proto.sh
│   ├── worker-agent.env             # local dev env var template
│   ├── demo.sh
│   └── demo-clean.sh
│
├── Makefile
├── go.mod / go.sum
├── openapi.yaml                    # canonical REST API contract (doc 2 lives here)
├── .golangci.yml
├── .github/workflows/ci.yml        # lint, unit tests, integration tests
└── README.md
```

## 7.1 Why this shape

**`internal/` for everything except `proto/` and `worker-agent-runtime/`.** Go convention: `internal/` cannot be imported by any module outside this repo, which is correct for the scheduler's implementation details. Nothing in this codebase is currently packaged for external consumption; `pkg/fleetforgeclient`, a generated REST client, was planned but deferred (see 7.2).

**Why `queue/backend.go` is a thin interface separate from `store/redis/queue.go`.** This is the seam for a future Redis Streams to NATS JetStream migration, discussed in doc 1.1/9. The scheduler core only ever imports `queue.Backend`, never `store/redis` directly, so swapping implementations later is additive.

**Why `worker-agent-runtime/` is separate from `cmd/worker-agent/`.** `cmd/worker-agent/main.go` is wiring (config, gRPC client setup); the "how do I execute a build and report a result" logic is substantial enough, and eventually testable enough in isolation, to deserve its own package rather than living in `main.go`.

**Why load and chaos tests are Python while everything else is Go.** Locust's distributed load-generation model is a better fit for simulating hundreds of concurrent worker connections than anything in Go's ecosystem without reinventing Locust. Chaos scripts (send a process a signal, poll REST endpoints, assert on timing) are exactly the kind of glue code where Python's scripting ergonomics win and raw performance is irrelevant.

## 7.2 Where the original plan and the shipped system diverge

The original design (see doc 1) scoped several pieces that were not built, either because they were out of scope for what this project set out to demonstrate, or because they were explicitly deferred pending a concrete need:

- **No pluggable cloud-provider autoscaling.** The original plan called for `internal/autoscaler/` with `provider_aws.go`, `provider_noop.go`, and a `FleetProvider` interface. What shipped is a single file, `internal/scheduler/autoscaler.go`: scale-down (draining idle workers) is real, scale-up is a logged recommendation with no cloud API integration behind it.
- **No separate `internal/retry/` package.** Retry backoff scheduling lives inline in `internal/scheduler/retrypoller.go` rather than as its own package; the logic is small enough that the extra boundary wasn't worth it.
- **No generated REST client (`pkg/fleetforgeclient`).** `fleetforgectl` talks to the REST API with plain `net/http` calls. A generated client from `openapi.yaml` was planned to back it, deferred until the API surface stabilizes further.
- **No rate limiting.** The original design called for Redis-backed rate limiting on worker registration. Nothing in the current implementation limits registration rate; this is a real gap, not a hidden one.
- **No Redis-outage fallback poller for job submission.** `docs/06-failure-scenarios.md` #4 describes a Postgres-polling fallback for the job queue if Redis is down. `internal/api/handlers_jobs.go` writes to Postgres first and only enqueues to Redis after that succeeds, but if the Redis enqueue itself fails, there is currently no periodic poller that re-scans Postgres for orphaned `QUEUED` rows. This is called out directly in that file's own comments as a tracked follow-up.
- **No Kubernetes manifests, no Dockerfile for the scheduler or worker-agent.** Local development runs Postgres, Redis, Prometheus, and Grafana in Docker Compose; the scheduler and worker-agent binaries run on the host via `go run` or the built binaries directly. Containerizing them, and any `deploy/k8s/` manifests, is a deployment-time concern that was never started.
- **No Postgres read replica, no HA load balancer.** Multiple scheduler replicas with Postgres advisory-lock leader election do work and were live-tested (a standby took over in 1.53 seconds after the leader was killed), but there's no load balancer in front of them and no read replica behind Postgres. A local demo runs one scheduler replica by default.

None of this was discovered by a reviewer after the fact; each item above is also flagged in the relevant source file's own comments at the exact point the gap exists.

---

**Next:** doc 8, the implementation roadmap that built this repository incrementally.
