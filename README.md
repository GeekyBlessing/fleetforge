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

Day 1 of the 7-day plan (see `docs/08-implementation-roadmap.md`): repo scaffold, Postgres schema, worker registration over gRPC.
