# 8. Implementation Roadmap

Each milestone ends with something that actually runs via `docker-compose up`, not just code that compiles. Order is chosen so that every milestone is a strict superset of the previous: progress is demoable at the end of any milestone, and it looks like a real, if incomplete, system.

## Milestone 1: Skeleton, schema, worker registration

**Objectives:** repo scaffolded per doc 7; Postgres schema (doc 3) applied via migrations; worker can register over gRPC and show up as `READY` in Postgres.

**Deliverables:**
- Repo scaffold, `go.mod`, Makefile, `.golangci.yml`, CI skeleton (lint + build only for now).
- `docker-compose.yml` with Postgres + Redis (empty roles for now, just running).
- Migrations for `workers`, `jobs`, `job_retry_history`, `worker_events`, `job_events`, enums.
- `proto/fleetforge/v1/scheduler.proto` with `RegisterWorker` RPC defined and generated.
- `internal/grpcserver/register.go`: handles registration, writes to Postgres, returns `worker_id` + `epoch`.
- `cmd/worker-agent`: minimal agent that reads config, calls `RegisterWorker` once, logs the result, exits (no heartbeat loop yet).
- `fleetforgectl workers list`: thin CLI hitting a bare `GET /workers` REST endpoint, enough to visually confirm registration worked.

**Testing goals:** unit tests for the registration handler (new worker, re-registration/epoch-bump path, rate-limit rejection, invalid-token rejection). Integration test spinning up real Postgres via testcontainers and asserting the full registration → row-in-DB path.

**Commit:** `feat: repo scaffold, postgres schema, worker registration (gRPC)`

## Milestone 2: Heartbeats, liveness, dead-worker reaper

**Objectives:** doc 5.2 and 5.3 fully working: a worker that stops heartbeating gets marked DEAD and its (currently nonexistent) jobs would be requeued.

**Deliverables:**
- `Heartbeat` bidirectional streaming RPC, worker-agent heartbeat loop (every 5s).
- Redis integration: `worker:{id}:state`, `worker:{id}:alive` per doc 4.2.
- Async batched Postgres writer (doc 5.2): coalesced flush + immediate-on-transition logic.
- `internal/scheduler/reaper.go`: the authoritative 5s sweep + epoch-bump rejection (409) on stale-epoch heartbeats.
- Basic `/healthz` and `/readyz`.

**Testing goals:** unit tests for idempotent heartbeat handling (duplicate/reordered), epoch-mismatch rejection. Integration test: start worker, kill it (don't let it deregister cleanly), assert it flips to `DEAD` in Postgres within ~20-25s and a `worker_events` row is written.

**Commit:** `feat: heartbeat stream, redis liveness cache, dead-worker reaper`

## Milestone 3: Job submission and the queue

**Objectives:** doc 5.4's first half: jobs can be submitted, land in Postgres as `QUEUED`, and land in the Redis stream.

**Deliverables:**
- REST `POST /jobs`, `GET /jobs`, `GET /jobs/{id}` per `openapi.yaml`.
- Idempotency-key handling (unique constraint + 409-with-existing-job behavior).
- `internal/queue/backend.go` interface + Redis Streams implementation (`XADD` with per-priority streams, consumer group creation).
- `job_events` writes on submission.

**Testing goals:** unit tests for idempotency dedupe, priority-stream routing. Integration test: submit N jobs across priorities, assert they land in the correct streams in the correct relative order.

**Commit:** `feat: job submission API, redis streams queue, idempotency handling`

## Milestone 4: The scheduler core, matching, assignment, completion

**Objectives:** the actual brain. A submitted job gets matched to a real registered worker, assigned atomically, pushed to the worker, executed (can be a fake/no-op executor today), and the result flows back.

**Deliverables:**
- `internal/scheduler/algorithm.go`: capability match filter → label affinity filter → least-loaded selection among survivors (doc 9 explains why this ordering).
- `internal/scheduler/assign.go`: the Postgres CAS transaction from doc 5.4, `XCLAIM` recovery sweep for the PEL.
- `internal/scheduler/leader.go`: Postgres advisory-lock leader election, so this is correct from the start even before you run multiple replicas.
- `ReportJobResult` RPC + epoch-guarded completion handling (doc 5.4's discard-on-mismatch branch).
- Worker-agent: real executor that at minimum runs `docker run` against the job spec and reports exit code (log capture can be minimal: stdout to a local file; full log-streaming-to-object-storage can slip to a stretch goal if time is tight).

**Testing goals:** integration test covering the full doc 5.4 sequence end to end: submit → assigned → running → success, and a second test for the failure branch (assert retry-eligible job re-enqueues, see Milestone 5 for full retry policy; this milestone can stub "retries=0 always" and let Milestone 5 wire in real backoff). This is the milestone to also add a **scheduler benchmark**: measure assignment latency (submission to `ASSIGNED`) under N concurrent workers/jobs, establishing the baseline number for doc 9's "what would make us revisit Redis Streams" discussion.

**Commit:** `feat: scheduler core - capability matching, atomic assignment, job completion`

## Milestone 5: Retry policy and worker draining

**Objectives:** docs 9 (retry policy) and the drain state machine fully implemented.

**Deliverables:**
- `internal/retry/policy.go`: exponential backoff (base delay × 2^attempt, capped, jittered), `max_retries` enforcement, `job_retry_history` writes on every attempt.
- Delayed re-enqueue mechanism (a job in backoff isn't just re-`XADD`'d immediately, it needs a scheduled requeue; simplest correct approach: a `retry_at` timestamp column checked by a small poller that moves due retries into the stream, avoiding the need for a separate delay-queue technology).
- `POST /workers/{id}/drain` and `/resume` REST endpoints; `DRAINING` state respected by the scheduler's candidate filter (never assign to a draining worker); automatic `DRAINING → OFFLINE` transition once `current_job_id` clears.
- Worker-agent must handle receiving a drain signal gracefully (finish current job, stop accepting new assignments, but keep heartbeating until told to fully exit).

**Testing goals:** unit tests for backoff calculation and jitter bounds. Integration test: fail a job repeatedly, assert it retries the configured number of times with increasing delay and then lands in terminal `FAILED`. Integration test: drain a busy worker, assert it finishes its job, then goes `OFFLINE`, and never receives a new assignment while draining.

**Commit:** `feat: exponential-backoff retry policy, worker draining lifecycle`

## Milestone 6: Autoscaler, metrics, dashboards

**Objectives:** doc 9's autoscaling logic implemented against a pluggable provider; every metric needed for fleet observability exposed and visualized.

**Deliverables:**
- `internal/autoscaler/controller.go` + `provider.go` interface + a `provider_noop.go` (logs "would scale by N") so it's demoable without real cloud credentials, plus a stubbed `provider_aws.go` showing the real integration shape.
- `internal/metrics/prometheus.go`: `registered_workers`, `alive_workers`, `dead_workers`, `queued_jobs`, `running_jobs`, `failed_jobs`, `heartbeat_latency`, `scheduler_latency`, `retry_total`, `autoscale_events`, all labeled appropriately (e.g. by priority, by capability where useful).
- Grafana dashboards (`deploy/grafana/dashboards/*.json`): fleet overview (worker states pie/timeline, queue depth over time) and scheduler performance (assignment latency histogram, autoscale events timeline).
- Prometheus scrape config wired into `docker-compose.yml`.

**Testing goals:** assert every metric name/label matches what's documented (a metrics-contract test: parse `/metrics` output, check expected series exist). Manual verification: watch the Grafana dashboard live while the load test from Milestone 7 runs.

**Commit:** `feat: autoscaler, prometheus metrics, grafana dashboards`

## Milestone 7: Security, load/chaos testing, polish

**Objectives:** the non-functional requirements get proven, not just claimed.

**Deliverables:**
- mTLS for worker↔scheduler (CA setup, cert issuance script, `internal/auth/mtls.go` verification) and JWT for human/CI clients (`internal/auth/jwt.go`), per doc 9's security section.
- `test/load/locustfile.py` + `fake_worker.py`: simulate a target fleet size (start realistic, e.g. 500-1,000 simulated workers, and document what it would take to validate the full 10,000, since that's a capacity-planning exercise as much as a code one) and a sustained job submission rate; capture scheduler-latency and heartbeat-latency percentiles.
- `test/chaos/kill_leader.py`, `kill_worker.py`, `partition_worker.py`: automate scenarios #1, #3, #6 from doc 6 against real binaries and assert the system self-heals within expected bounds.
- README pass, `.github/workflows/ci.yml` finished (lint + unit + integration on every PR), final docs sync (make sure doc 1-9 match what actually got built, calling out any deliberate deviations).

**Testing goals:** full chaos suite green; load test report with p50/p95/p99 for scheduler assignment latency and heartbeat round-trip; security review checklist (no plaintext secrets, mTLS actually enforced not just present, rate limits actually effective).

**Commit:** `feat: mTLS + JWT auth, load/chaos test suite, CI, docs sync - v1.0`

## 8.1 What's deliberately out of scope

Full 10,000-worker validated load test (Milestone 7's load test targets a smaller but representative fleet and documents the extrapolation, since actually provisioning 10k simulated agents is an infra/cost exercise, not a code one, and shouldn't eat the schedule). Multi-region/multi-datacenter scheduling. A real web dashboard beyond Grafana. NATS JetStream migration (the seam is built per doc 7, the migration itself is a follow-on). These are exactly the kind of things to list explicitly as "future improvements" in doc 9: naming what wasn't built is part of documenting the full scope, not a gap to hide.

---

**Next:** doc 9, the design rationale: every decision above, its alternatives, and what to revisit.
