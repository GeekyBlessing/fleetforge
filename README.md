# FleetForge

Lightweight distributed build scheduler for a fleet of build workers.

Architecture and design docs: see `docs/` (docs 00-09) for the original design set, written before implementation began.

## Local dev

```
make up          # docker compose: postgres, redis, scheduler, prometheus, grafana
make migrate      # apply schema
make build        # build all three binaries
make test          # unit tests
make integration-test
```

## Status

Implemented: worker registration, heartbeat/liveness tracking and dead-worker reaping, job submission and queueing, capability-aware least-loaded scheduling, retry-with-backoff and worker draining, Prometheus metrics with a Grafana dashboard, and an autoscaler implementing the hysteresis/cooldown logic in `docs/09-design-rationale.md`.

Security: mTLS between workers and the scheduler, plus JWT auth on REST API write endpoints, per `docs/09-design-rationale.md` 9.4 (see `scripts/gen-certs.sh` and `fleetforgectl auth mint-token`).

Testing: a load-testing harness (`test/load/`, a simulated worker fleet plus a Locust job-submission driver measuring assignment latency) and a chaos-testing suite (`test/chaos/`: `kill_leader.py`, `kill_worker.py`, `partition_worker.py`) that exercises failure scenarios #1, #3, and #6 from `docs/06-failure-scenarios.md` against real binaries.

CI runs lint, unit tests, and integration tests on every push.
