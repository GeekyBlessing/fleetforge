# 4. Redis Data Model

Redis plays four distinct roles here: **job queue**, **hot-path cache**, **rate limiter**, and **event fan-out**. It deliberately does *not* play the role of "source of truth" (that's Postgres) or "leader election" (that's a Postgres advisory lock; see doc 1/9). Every key below states its purpose, shape, and TTL/eviction policy explicitly, because an undocumented Redis key is the fastest way to create a production incident nobody can debug.

## 4.1 Job queues: Redis Streams

```
Key:    queue:jobs:p{0-9}          (one stream per priority level, 0=highest)
Type:   Stream
Fields per entry: { job_id, repository, branch, commit_sha, required_capabilities, enqueued_at }
Consumer group: "schedulers" (all scheduler replicas join; only the leader actively XREADGROUP-consumes in normal operation)
TTL: none (streams are trimmed, not expired; see below)
```

**Why per-priority streams instead of one stream + a priority field.** The scheduler loop needs "give me the highest-priority QUEUED job" cheaply and repeatedly. Reading `queue:jobs:p0` empty before falling through to `p1`, etc., is O(1) per priority level checked: no sorting, no scanning. A single stream would need a client-side priority queue rebuilt from an unbounded read, which doesn't scale as depth grows.

**Delivery guarantee.** Consumer groups give at-least-once delivery: the leader `XREADGROUP`s an entry, it becomes "pending" (in the Pending Entries List, PEL) until explicitly `XACK`'d. If the scheduler crashes mid-assignment (after claiming the entry, before the Postgres CAS commits), the entry sits in the PEL. A recovery sweep (`XPENDING` / `XCLAIM`, run by whichever replica becomes leader next, or by the same leader on restart) reclaims and reprocesses anything pending longer than a short threshold (e.g. 10s, comfortably longer than a single scheduling decision should ever take). This is exactly how "scheduler crash mid-assignment" (doc 6) is recovered without a lost job.

**Trimming.** `XADD ... MAXLEN ~ 100000` per stream caps memory; once a job is durably ASSIGNED in Postgres, the stream entry has done its job and can be trimmed. The stream is a delivery mechanism, not a system of record: Postgres already has the durable copy.

## 4.2 Hot-path worker state cache

```
Key:    worker:{worker_id}:state
Type:   Hash
Fields: { status, current_job_id, available_capacity, epoch, last_heartbeat_unix }
TTL:    none on the hash itself (see 4.2.1 for liveness signal, kept separate)
```

**Why this exists at all, given Postgres already has a `workers` table.** The scheduler's matching loop runs continuously and needs to ask "which workers are READY with capability X and spare capacity" possibly hundreds of times a second at full fleet size. Doing that against Postgres on every scheduling tick, at 10,000 workers, works fine, but going through Redis in-memory hashes (populated on every heartbeat, read on every scheduling decision) removes Postgres from the scheduler's innermost hot loop entirely; Postgres is updated on a slower, batched cadence (see 4.2.2) purely for durability/audit, not as the live decision source.

### 4.2.1 Liveness key (drives dead-worker detection)

```
Key:    worker:{worker_id}:alive
Type:   String (value: epoch, so a stale read can be epoch-checked)
TTL:    20 seconds, refreshed (SETEX) on every heartbeat
```

Two independent mechanisms consume this, deliberately redundant (see doc 6 on why Redis keyspace notifications alone are not trustworthy):

1. **Active reaper sweep** (authoritative): every 5s, the leader queries Postgres for `workers WHERE status NOT IN ('OFFLINE','DEAD') AND last_heartbeat < now() - interval '20 seconds'`. This is the source of truth for "who's dead" and does not depend on Redis at all: it would still work if Redis's keyspace-notification feature were disabled or the pub/sub message were dropped.
2. **Keyspace notification fast-path** (optimization only): if `notify-keyspace-events Ex` is enabled, an `EXPIRED` event on `worker:{id}:alive` triggers an immediate reaper check for that one worker, shaving up to ~5s off detection latency in the common case. If this event is missed (Redis failover, notification not delivered), mechanism (1) catches it within one sweep interval regardless.

### 4.2.2 Batched Postgres sync

Heartbeats update `worker:{id}:state` and `worker:{id}:alive` synchronously on every 5s tick (2,000 ops/sec sustained at 10k workers, trivial for Redis). A background writer flushes `last_heartbeat` and `status` to Postgres on a coalesced schedule: at most once per worker per 15 seconds, or immediately on any *state transition* (READY→BUSY, BUSY→READY, →DRAINING, →DEAD). This means Postgres sees every status change in real time but doesn't take a write for every single unchanged heartbeat: sustained heartbeat-driven Postgres writes drop from ~2,000/sec to a small fraction of that, only on actual transitions plus a low-rate keepalive.

## 4.3 Distributed locks

```
Key:    lock:job:{job_id}:assign
Type:   String (SET ... NX PX 5000)
TTL:    5 seconds
```

Guards the assignment critical section for a single job against a race between concurrent goroutines within the leader (the leader may run several scheduling workers in parallel for throughput). This is a short-lived, low-stakes lock: if it's held incorrectly for the *durability* of the assignment, that's fine, because the actual assignment is still made durable via a Postgres compare-and-swap (`UPDATE ... WHERE status = 'READY'`), which is the real correctness guarantee. The Redis lock only prevents wasted work (two goroutines racing to assign the same job), not correctness, which is exactly the right amount of trust to place in a Redis lock. Leader election itself, where correctness actually matters, deliberately uses Postgres advisory locks instead (doc 1.1/9).

```
Key:    lock:worker:{worker_id}:drain
Type:   String (SET ... NX PX 30000)
TTL:    30 seconds, renewed while drain is in progress
```

Prevents a race between an operator issuing a second drain/resume call and the scheduler's own drain-completion logic concurrently mutating the same worker row.

## 4.4 Rate limiting (registration abuse / rogue workers)

```
Key:    ratelimit:register:{source_ip_or_token_id}
Type:   String, INCR-based fixed window
TTL:    60 seconds
Limit:  configurable, default 10 registrations/minute per source
```

`INCR` the key, `EXPIRE` it to 60s if this was the first increment in the window. If the count exceeds the configured limit, `POST /workers/register` returns `429`. This is the first line of defense against a compromised or misconfigured worker image registering in a crash-loop; it's paired with the mTLS/JWT bootstrap-token requirement (doc 9 / security) as the actual identity control: rate limiting alone doesn't stop a rogue worker with a valid token, it just bounds the blast radius of a noisy/broken one.

## 4.5 Event fan-out (pub/sub)

```
Channels: events:worker, events:job
Payload:  JSON, e.g. {"type":"WORKER_DEAD","worker_id":"...","epoch":4,"ts":"..."}
```

Fire-and-forget notifications consumed by the dashboard/Grafana-adjacent live view and by any internal alerting hook. Deliberately *not* used for anything that must be durable or guaranteed-delivered: a missed pub/sub message means the dashboard is a few seconds stale, not that a job gets lost, because the Postgres `job_events`/`worker_events` audit tables (doc 3) are the durable record of the same facts.

## 4.6 Summary table

| Key pattern | Purpose | Type | TTL | Durability requirement |
|---|---|---|---|---|
| `queue:jobs:p{0-9}` | Job queue by priority | Stream | none (MAXLEN trim) | At-least-once (PEL + XCLAIM recovery) |
| `worker:{id}:state` | Hot-path scheduling cache | Hash | none | Best-effort; rebuilt from Postgres on cache miss |
| `worker:{id}:alive` | Liveness / dead-worker trigger | String | 20s | Best-effort; Postgres sweep is authoritative |
| `lock:job:{id}:assign` | Prevent double-assignment race | String | 5s | Best-effort; Postgres CAS is authoritative |
| `lock:worker:{id}:drain` | Serialize drain/resume ops | String | 30s (renewed) | Best-effort |
| `ratelimit:register:{src}` | Registration abuse control | String (counter) | 60s | Best-effort |
| `events:worker`, `events:job` | Live dashboard fan-out | Pub/Sub | n/a | Best-effort; audit tables are durable record |

**What happens if Redis is unavailable entirely:** covered in detail in doc 6, but in short: the scheduler degrades to reading worker candidates directly from Postgres (slower, but correct) and queues jobs via a Postgres-backed fallback table polled at a lower frequency, rather than refusing to schedule at all. Redis is a performance and convenience layer here, not a hard dependency for correctness.

---

**Next:** doc 5, sequence diagrams for registration/heartbeat/failure detection and the full job lifecycle.
