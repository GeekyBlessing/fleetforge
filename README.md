# FleetForge

Lightweight distributed build scheduler for a fleet of build workers.

Architecture and design docs: see `docs/` (docs 00-09), copied from the Phase 1 design review.

## Local dev

```
make up          # docker compose: postgres, redis, scheduler, prometheus, grafana
make migrate      # apply schema
make build        # build all three binaries
make test          # unit tests
make integration-test
```

## Status

Implemented: worker registration, heartbeat/liveness tracking and dead-worker reaping, job submission and queueing, capability-aware least-loaded scheduling, retry-with-backoff and worker draining, Prometheus metrics with a Grafana dashboard, an autoscaler implementing the hysteresis/cooldown logic in `docs/09-design-rationale.md`, and mTLS (worker&lt;-&gt;scheduler) + JWT (REST API write endpoints) security per `docs/09-design-rationale.md` 9.4 -- see `scripts/gen-certs.sh` and `fleetforgectl auth mint-token`. Remaining: load and chaos testing, CI polish (see `docs/08-implementation-roadmap.md`).
