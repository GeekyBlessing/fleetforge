# Chaos testing

Automates three scenarios from docs/06-failure-scenarios.md against real
`bin/scheduler` / `bin/worker-agent` binaries (not mocks) per
docs/09-design-rationale.md 9.5: "assert recovery within documented
bounds ... turns doc 6 from a design claim into a verified property of the
actual code."

| Script | Scenario | What it proves |
|---|---|---|
| `kill_leader.py` | #1 -- leader crash | A standby replica acquires Postgres advisory-lock leadership within a bounded window after the leader is SIGKILLed. |
| `kill_worker.py` | #3 -- worker crash | A worker that dies mid-job is marked DEAD, and its job is atomically requeued rather than stuck ASSIGNED forever. |
| `partition_worker.py` | #6 -- network partition | A worker that's frozen (not crashed) is *still* detected and marked DEAD the same way; once "healed" it hits the stale-epoch rejection and fatal-exits per the worker-agent's split-brain contract, rather than silently resuming as if nothing happened. |

Each script is self-contained, prints `[OK]`/`[FAIL]` per assertion, and
exits non-zero on failure -- safe to wire into a CI job (`make build`
first, then run each script) once you have a real Postgres/Redis
available there.

## Prerequisites

```bash
make build          # produces bin/scheduler, bin/worker-agent
make up              # or otherwise have Postgres + Redis running and migrated
export FLEETFORGE_DATABASE_URL=postgres://fleetforge:fleetforge@localhost:5432/fleetforge?sslmode=disable
```

`kill_worker.py` and `partition_worker.py` additionally need a scheduler
already running and reachable (they only manage the worker side):

```bash
export FLEETFORGE_DATABASE_URL=... FLEETFORGE_REDIS_ADDR=...
./bin/scheduler &
```

`kill_leader.py` manages its own two scheduler replicas (ports 9190/8180
and 9191/8181) and doesn't need one already running -- don't run it
against a port range that's already in use.

## Running

```bash
python3 test/chaos/kill_leader.py
python3 test/chaos/partition_worker.py     # run kill_worker.py or partition_worker.py, not both against
python3 test/chaos/kill_worker.py          # the same live worker at once -- each spawns its own
```

If mTLS is enabled on the scheduler (`FLEETFORGE_TLS_*` env vars --
see `scripts/gen-certs.sh`), export the same three variables before
running `kill_worker.py`/`partition_worker.py` -- `bin/worker-agent`
picks them up automatically, no script changes needed.

## Known gap

`partition_worker.py` simulates a network partition by SIGSTOP'ing the
worker process rather than actually severing its TCP connection at the
network level. This freezes the process (it can't send heartbeats, same
externally-observable effect as a real partition) but isn't perfectly
faithful -- a real partition can also leave a half-open TCP connection in
ways SIGSTOP doesn't reproduce. A toxiproxy-based version that actually
cuts the connection is a reasonable follow-up, noted rather than solved
here (consistent with docs/08-implementation-roadmap.md's original
`toxiproxy_partition.py` naming).
