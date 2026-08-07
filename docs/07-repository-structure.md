# 7. Repository Structure

Single Go module (monorepo): scheduler and worker-agent share types, proto definitions, and store clients, which matters because the fencing/epoch logic and status enums must never drift between the two binaries. Python appears only for load/chaos tooling, where its ecosystem (locust, faker, chaos scripting) genuinely beats Go for iteration speed.

```
fleetforge/
├── cmd/
│   ├── scheduler/              # main() for the scheduler binary
│   │   └── main.go
│   ├── worker-agent/           # main() for the worker agent binary
│   │   └── main.go
│   └── fleetforgectl/          # CLI for admin ops (drain/resume/submit/list): thin REST client
│       └── main.go
│
├── internal/
│   ├── api/                    # REST layer (chi/gin router, handlers, middleware)
│   │   ├── handlers_workers.go
│   │   ├── handlers_jobs.go
│   │   ├── middleware_auth.go       # JWT verification
│   │   ├── middleware_ratelimit.go
│   │   └── router.go
│   │
│   ├── grpcserver/              # gRPC control plane (worker-facing)
│   │   ├── register.go
│   │   ├── heartbeat.go
│   │   ├── report_result.go
│   │   └── interceptors_mtls.go
│   │
│   ├── scheduler/               # scheduling core: the actual brain
│   │   ├── loop.go                  # main scheduling loop, leader-gated
│   │   ├── algorithm.go             # capability match + least-loaded + priority (doc 9)
│   │   ├── assign.go                # the Postgres CAS assignment transaction
│   │   ├── reaper.go                # dead-worker detection (doc 5.3)
│   │   └── leader.go                # Postgres advisory-lock leader election
│   │
│   ├── autoscaler/
│   │   ├── controller.go            # decision loop: queue depth, idle workers, utilization
│   │   ├── provider_aws.go          # FleetProvider impl: AWS ASG
│   │   ├── provider_noop.go         # for bare-metal / manual fleets
│   │   └── provider.go              # FleetProvider interface
│   │
│   ├── retry/
│   │   └── policy.go                # exponential backoff calculation, max-retry enforcement
│   │
│   ├── store/
│   │   ├── postgres/
│   │   │   ├── workers.go
│   │   │   ├── jobs.go
│   │   │   ├── migrations/          # sql migration files (golang-migrate or atlas)
│   │   │   └── queries/              # sqlc-generated or hand-written query files
│   │   └── redis/
│   │       ├── queue.go             # queue.Backend interface + Redis Streams impl
│   │       ├── cache.go             # worker state hash helpers
│   │       ├── locks.go
│   │       └── ratelimit.go
│   │
│   ├── queue/
│   │   └── backend.go               # queue.Backend interface (Redis Streams today, NATS-swappable later; doc 1.1)
│   │
│   ├── auth/
│   │   ├── jwt.go                   # bootstrap token + client JWT verification
│   │   └── mtls.go                  # CA management, cert issuance/verification
│   │
│   ├── metrics/
│   │   └── prometheus.go            # all counters/gauges/histograms (doc 9 lists them)
│   │
│   └── config/
│       └── config.go                 # env/flag-based config, shared by both binaries
│
├── pkg/                          # anything intentionally importable by external tooling
│   └── fleetforgeclient/         # generated REST client from openapi.yaml, for fleetforgectl and any external caller
│
├── proto/
│   └── fleetforge/v1/
│       ├── worker.proto
│       ├── scheduler.proto           # RegisterWorker, Heartbeat (stream), ReportJobResult RPCs
│       └── gen/                       # generated Go stubs (gitignored or committed, team preference)
│
├── worker-agent-runtime/          # the actual build-execution logic run by the agent
│   ├── executor.go                 # shells out to the build (docker run / native process)
│   ├── logstreamer.go              # streams build logs to object storage, sets log_ref
│   └── sandbox.go                   # resource limits / isolation for untrusted build steps
│
├── deploy/
│   ├── docker-compose.yml          # scheduler + postgres + redis + prometheus + grafana + N fake workers
│   ├── docker-compose.override.yml.example
│   ├── k8s/
│   │   ├── scheduler-deployment.yaml
│   │   ├── scheduler-service.yaml
│   │   ├── postgres-statefulset.yaml   # or pointer to managed Postgres
│   │   ├── redis-statefulset.yaml
│   │   └── prometheus-servicemonitor.yaml
│   └── grafana/
│       └── dashboards/
│           ├── fleet-overview.json
│           └── scheduler-performance.json
│
├── docs/                          # this design-doc set lives here in the real repo
│   ├── 00-README.md ... 09-design-rationale.md
│
├── test/
│   ├── unit/                       # colocated _test.go files per package (Go convention); this dir is for cross-cutting fixtures
│   ├── integration/                 # spins up real Postgres+Redis via testcontainers, exercises full flows
│   ├── load/                        # Python + Locust: simulate N workers + job submission rate
│   │   ├── locustfile.py
│   │   └── fake_worker.py
│   └── chaos/                       # Python scripts against real scheduler/worker-agent binaries (not mocks)
│       ├── kill_leader.py           # scenario #1: leader crash mid-assignment
│       ├── kill_worker.py           # scenario #3: worker crash mid-job
│       └── partition_worker.py      # scenario #6: worker frozen/partitioned, not crashed
│
├── scripts/
│   ├── gen-proto.sh
│   ├── gen-openapi-client.sh
│   └── migrate.sh
│
├── Makefile
├── go.mod / go.sum
├── openapi.yaml                    # canonical copy (doc 2 lives here in the real repo)
├── .golangci.yml
├── .github/workflows/ci.yml        # lint, unit, integration, build images
└── README.md
```

## 7.1 Why this shape

**`internal/` vs `pkg/`.** Go convention: `internal/` cannot be imported by any module outside this repo, which is exactly right for the scheduler's guts (nobody outside this project should ever import `internal/scheduler` directly). `pkg/fleetforgeclient` is the one thing meant for external consumption: a generated REST client, so other teams' tooling can talk to FleetForge without hand-rolling HTTP calls.

**Why `queue/backend.go` is a thin interface separate from `store/redis/queue.go`.** This is the seam identified in doc 1.1/9 for the Redis Streams → NATS JetStream migration path. The scheduler core only ever imports `queue.Backend`, never `store/redis` directly, so swapping implementations later is additive, not a scheduler-core rewrite.

**Why `worker-agent-runtime/` is separate from `cmd/worker-agent/`.** `cmd/worker-agent/main.go` is wiring (config, gRPC client setup); the actual "how do I run a build safely and stream its logs" logic is substantial enough (and eventually testable enough in isolation) to deserve its own package rather than living in `main.go`.

**Why load/chaos tests are Python while everything else is Go.** Locust's web UI and distributed-load-generation model is genuinely better for "simulate 10,000 concurrent worker connections" than anything in Go's ecosystem without reinventing Locust badly. Chaos scripts (kill a container, inject network partition via toxiproxy, SIGKILL the leader) are glue-code around Docker/toxiproxy APIs where Python's scripting ergonomics win and performance is irrelevant.

---

**Next:** doc 8, the implementation roadmap that builds this repository incrementally.
