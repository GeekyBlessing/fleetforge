# 9. Design Rationale

Each section below is a decision, the alternatives I considered, why I landed where I did, and what would make me revisit it. Treat this as the ADR log for the project.

## 9.1 Scheduling algorithm — which parts, in what order

You listed six approaches: Round Robin, Least Loaded, Priority Scheduling, Capability Matching, Worker Labels/Affinity, Queue Priorities. These aren't six competing algorithms to pick one of — they're a **pipeline**, and the interesting design decision is the order:

```
candidates = all workers WHERE status = READY AND available_capacity > 0
    -> filter: capability matching (required_capabilities ⊆ worker.capabilities)   [hard filter]
    -> filter: label affinity (job.labels match worker.labels per affinity rules)   [hard filter]
    -> select: least-loaded among remaining candidates                             [ranking]
queue order = priority (0 highest) then FIFO within a priority                     [determines processing order, not candidate filtering]
```

**Why capability matching and label affinity are hard filters, never soft/scored.** A build job with `required_capabilities: [gpu]` sent to a non-GPU worker doesn't degrade gracefully — it fails outright. Treating this as a "preference" that a scoring function might override under load is actively wrong for this domain, unlike, say, a general-purpose k8s scheduler where a soft affinity rule failing just means a slightly worse placement. So capability/label matching is a `WHERE`-clause-style hard filter, applied before any ranking.

**Why least-loaded (not round robin) is the ranking function, and what "loaded" means.** Round robin assumes all workers are fungible and ignores current work — fine for stateless web request routing, wrong here because build workers have wildly different capacity (a 4-core and a 64-core box are not interchangeable) and jobs have wildly different durations (a 30s lint job and a 45-minute integration-test job). "Least loaded" here means ranking surviving candidates by `available_capacity / capacity_slots` (fraction free) descending, so a large mostly-idle worker is preferred over a small nearly-full one, rather than blindly rotating. Round robin is trivial to implement as a fallback/comparison mode (useful in the load-testing phase to isolate "is my least-loaded logic actually better" with real numbers) but isn't the production default.

**Why priority is a queue-ordering concern, not a per-job scoring concern.** Priority (0-9) determines *which job gets looked at first* (per-priority Redis streams, drained highest-first, doc 4.1) — it does not change which worker a job is allowed to land on. Keeping these orthogonal (queue order vs. candidate filtering) keeps the algorithm easy to reason about: you can explain "why did job X get worker Y" without needing to know anything about priority, and separately explain "why did job X get looked at before job Z" without needing to know anything about capability matching.

**Build order (Milestone 4):** capability matching first (it's a correctness requirement — assigning to an incapable worker isn't a suboptimal choice, it's a bug), then priority-ordered queue draining (cheap, just stream read order), then least-loaded ranking (the one part that benefits from real load-test data to tune, so it's easiest to get right last, after the scheduler benchmark exists to validate against).

**What I'd add later, explicitly not in the current scope:** true affinity/anti-affinity rules beyond simple label-equality matching (e.g. "prefer a worker that already has this repo's Docker layer cache warm" — this requires tracking cache-locality state per worker, which is a real optimization for build systems specifically, akin to Bazel remote-cache-aware scheduling, but it's a substantial feature on its own).

## 9.2 Autoscaling logic — signals and oscillation avoidance

**Signals used, and why all four rather than one:**

- **Queue depth** (jobs QUEUED, by priority) — the most direct "are we behind" signal, but noisy on its own: a momentary burst of 50 jobs that will drain in 10 seconds shouldn't trigger a scale-up that takes 3 minutes to provision and then sits idle.
- **Idle workers** (READY with full available_capacity) — direct "do we have slack" signal; prevents scaling up when there's actually capacity sitting unused (e.g. queue depth is high only because of a capability mismatch — more workers of the *wrong* kind won't help, which is exactly why idle-worker count is tracked per capability/label combination, not just as a fleet-wide total).
- **CPU utilization** (average across busy workers, from heartbeat `resource_usage`) — catches the case where workers show `available_capacity > 0` by slot-count but are actually resource-starved (e.g. a worker configured for 4 concurrent slots but genuinely CPU-bound at slot 3).
- **Pending jobs trend** (rate of queue-depth change, not just instantaneous value) — this is what actually prevents oscillation: a decision function that only looks at instantaneous queue depth will scale up, see the queue drain (because it *was* already draining on its own), and then scale back down immediately, repeating forever. Tracking the trend (is depth increasing over the last 2-3 sample windows, not just "is it currently > threshold") means a self-resolving spike doesn't trigger action at all.

**Oscillation avoidance mechanisms, concretely:**

1. **Cooldown windows.** After any scale-up, no further scale-up decision for N minutes (default 5) even if signals still say "scale up" — gives newly-provisioned capacity time to register and start pulling jobs before deciding it "didn't help." Same idea for scale-down with a longer cooldown (default 10) since removing capacity is riskier to reverse quickly (new instances take longer to provision than existing ones take to drain).
2. **Asymmetric thresholds (hysteresis).** Scale up when sustained queue depth / worker > threshold_up; scale down only when it drops below a *meaningfully lower* threshold_down (e.g. up at 5 queued jobs/worker, down at 1) — not the same number in both directions, which is the textbook fix for a bang-bang oscillation.
3. **Scale-down never touches BUSY/ASSIGNED workers.** Scale-down candidates are drained first (doc "worker draining" — `POST /workers/{id}/drain`), then terminated only once they've reached `OFFLINE` on their own. This means scale-down latency is bounded by "longest currently-running job," not instantaneous, which is a feature: it structurally cannot kill in-flight work.
4. **Rate-limited scale-up magnitude.** Never more than +25% fleet size (configurable) in a single decision, even if signals suggest a much larger jump is warranted — protects against a metrics glitch (e.g. a monitoring bug reporting queue depth 100x too high) causing a runaway provisioning event and matching cost blowout.

## 9.3 Retry policy detail

`delay = min(base_delay * 2^attempt, max_delay) + jitter(0, base_delay)`. Jitter matters specifically because failures are often correlated (a flaky shared dependency, a bad commit) — without jitter, N jobs that failed simultaneously would all retry simultaneously, hitting whatever caused the failure at the exact same moment again. `max_retries` is per-job configurable (default from doc 2's OpenAPI spec) rather than global, because some jobs (a nightly full-suite run) warrant more retry patience than others (a fast lint check that's unlikely to be flaky and should fail fast to give quick feedback). Every attempt, success or failure, writes a `job_retry_history` row (doc 3) — this is what lets you later answer "is this repo's test suite flaky" as a data question instead of an anecdote.

## 9.4 Security — mTLS + JWT, not one or the other

**Worker ↔ scheduler: mTLS.** Workers are long-lived, infrastructure-owned identities (a build box, not a human), which is exactly the case mTLS is built for: the scheduler runs its own internal CA, issues a short-lived client certificate to each worker at provisioning time (e.g. baked into the worker image/cloud-init, or fetched once from a separate, more tightly-guarded bootstrap endpoint), and the gRPC server verifies the client cert on every connection — not just at registration. This is what actually prevents rogue worker registration: a valid bootstrap JWT alone (a bearer token) can be exfiltrated and replayed from anywhere; a client certificate tied to mTLS is far harder to steal and replay outside its intended host, and can be revoked (CRL/short expiry + rotation) if a specific worker is compromised.

**Human/CI clients ↔ REST API: short-lived JWT.** Job submission and admin actions (drain/resume/cancel) come from CI systems and humans — bearer tokens are the right fit here because these clients aren't long-lived persistent connections the way a worker is, and JWT gives you scoped claims (e.g. a CI-system token scoped to `jobs:submit` only, an on-call engineer's token scoped to `workers:drain` for incident response) without needing per-client certificates issued through the same heavyweight CA flow that workers use.

**Why not mTLS everywhere or JWT everywhere.** mTLS for every human/CI caller is operationally painful (cert distribution to every laptop/CI runner) for a benefit (strong identity) that JWT + short expiry + scope claims already delivers adequately for a bearer-token threat model where the caller isn't claiming to *be* infrastructure. JWT everywhere (including workers) reintroduces the "stolen bearer token can be replayed from anywhere" problem for exactly the identity class (long-lived infrastructure) where you can afford the extra rigor of a certificate.

## 9.5 Testing strategy summary

Full detail belongs in `test/` and CI config (doc 7), but the shape: **unit tests** cover pure logic with no I/O (scheduling algorithm filtering/ranking, backoff calculation, idempotency key logic) — fast, run on every save. **Integration tests** use real Postgres + Redis via testcontainers (not mocks) for anything touching the CAS assignment transaction or the reaper sweep — this is the layer that actually validates the correctness guarantees in doc 6, so it's not optional scope. **Load tests** (Locust, Milestone 7) establish the actual numbers behind claims like "handles 10,000 workers" — a plan without a number attached is a guess. **Chaos tests** automate exactly the scenarios in doc 6 (kill the leader mid-assignment, partition a worker, kill Redis, kill Postgres) and assert recovery within documented bounds — this is what turns doc 6 from a design claim into a verified property of the actual code.

## 9.6 Summary of major decisions

| Decision | Alternative(s) considered | Why this way | Revisit when |
|---|---|---|---|
| Go for scheduler + agent | Python/FastAPI | Concurrency model fits 10k-connection heartbeat tracking; static binary simplifies agent distribution | Never, for this component — Python remains right for load/chaos tooling |
| gRPC for control plane, REST for external | REST-only (polling) everywhere | Bidi streaming avoids poll overhead at 10k workers; REST stays for human/tooling ergonomics | If workers ever need to run somewhere gRPC/HTTP2 is genuinely blocked (some restrictive corporate proxies) |
| Redis Streams for job queue | NATS JetStream, RabbitMQ, plain Redis Lists | Reuses existing Redis dependency; sufficient throughput for job *submission* rate (much lower than heartbeat rate); consumer-group semantics give the delivery guarantee needed | Sustained enqueue rate becomes a bottleneck (rough guide >5k msgs/sec) or true multi-region queue replication is needed |
| Postgres advisory lock for leader election | Redis Redlock, etcd/Consul | Avoids adding a second consensus mechanism; Postgres is already the consistency-critical dependency | If Postgres itself becomes the HA bottleneck at extreme replica counts (unlikely at the scales described here) |
| Postgres CAS as the correctness guarantee for assignment | Redis-only locking | Redis locks are best-effort by nature (TTL-based); Postgres transactions give real atomicity across the worker+job state change | Not really revisitable — this is the load-bearing correctness decision of the whole system |
| Monthly range partitioning on `jobs` | Unpartitioned table + aggressive vacuum tuning | Bounded hot-index size regardless of total history; O(1) archival/deletion of old data | If job volume is far lower than "millions" in practice, partitioning overhead may not be worth it — but over-provisioning this is cheap and safe |
| mTLS for workers, JWT for humans/CI | One mechanism for everyone | Matches identity lifecycle (infrastructure vs. bearer) to appropriate mechanism | If a future zero-trust mesh (e.g. SPIFFE/SPIRE) subsumes both — worth watching as the project matures past v1 |

## 9.7 Open questions before we proceed to code

1. **Log storage backend.** Doc 3 assumes `log_ref` points to external object storage (S3-compatible). Do you have a preferred bucket/MinIO setup already, or should the initial implementation default to local-disk-with-a-documented-swap-point?
2. **Cloud provider for autoscaling.** Doc 9.2/doc 7 stub an AWS ASG provider as the concrete example — confirm that's the right target (vs. GCP, Nomad, or a purely on-prem/manual fleet where the autoscaler is a no-op that only emits recommendations).
3. **Realistic load-test target.** I proposed 500-1,000 simulated workers as the validated number, with the 10,000 figure treated as a documented extrapolation rather than something actually spun up during implementation — confirm that's an acceptable framing for the assignment's grading criteria.
4. **Any existing LaunchVerse infra to integrate with** (an existing Postgres cluster, an existing secrets manager for the mTLS CA, an existing CI system whose webhook should call `POST /jobs`) that should shape the initial scaffold rather than assuming a from-scratch docker-compose environment.

---

This closes the mandatory Phase 1 deliverables (docs 1-9, plus the 00-README index). I have not written any implementation code. Review, push back on anything, answer 9.7 if you can — once you say go, implementation starts.
