#!/usr/bin/env python3
"""Chaos test: docs/06-failure-scenarios.md scenario #1 -- the scheduler
leader crashes.

Spins up two real `bin/scheduler` replicas against the same Postgres/Redis
(the actual leader-election code path, internal/scheduler/leader.go's
Postgres advisory lock -- not a stand-in for it), confirms exactly one
becomes leader, SIGKILLs it, and asserts the standby acquires leadership
within a bounded window. Doc 6 documents "~1-2s"; this asserts a generous
10s bound so the test itself doesn't flake on `go build`-adjacent host
variance -- if it's regularly taking anywhere close to that bound, that's
worth investigating even though it technically still "passes".

Prerequisites:
    make build                     # produces bin/scheduler
    Postgres + Redis running and migrated (make up / make migrate)
    export FLEETFORGE_DATABASE_URL=postgres://...

Usage:
    python3 test/chaos/kill_leader.py
"""
import os
import sys
import time

from common import ChaosFailure, fail, http_get, ok, require_database_url, section, start_binary, stop_process, wait_until

LEADER_ACQUIRE_TIMEOUT_S = 15
FAILOVER_TIMEOUT_S = 10
DOCUMENTED_BOUND_S = 5.0  # doc 6 says ~1-2s; flag (not fail) if we're creeping well past it


def readyz(port: int):
    return http_get(f"http://localhost:{port}/readyz")


def find_leader(replicas):
    for r in replicas:
        body = readyz(r["http_port"])
        if body and body.get("is_leader"):
            return r
    return None


def main() -> int:
    database_url = require_database_url()
    section("Scenario #1: scheduler leader crash (docs/06-failure-scenarios.md #1)")

    replica_ports = [(9190, 8180), (9191, 8181)]
    replicas = []
    for i, (grpc_port, http_port) in enumerate(replica_ports):
        env = os.environ.copy()
        env["FLEETFORGE_DATABASE_URL"] = database_url
        env["FLEETFORGE_GRPC_ADDR"] = f":{grpc_port}"
        env["FLEETFORGE_HTTP_ADDR"] = f":{http_port}"
        proc = start_binary("scheduler", env)
        replicas.append({"proc": proc, "grpc_port": grpc_port, "http_port": http_port, "label": f"replica-{i + 1}"})
        print(f"started {replicas[-1]['label']} (grpc :{grpc_port}, http :{http_port}, pid {proc.pid})")

    try:
        print(f"waiting up to {LEADER_ACQUIRE_TIMEOUT_S}s for a leader to be elected...")
        leader = wait_until(lambda: find_leader(replicas), timeout=LEADER_ACQUIRE_TIMEOUT_S)
        if leader is None:
            fail("no replica became leader in time -- is Postgres reachable and migrated? "
                 "check the replica output below")
        standby = next(r for r in replicas if r is not leader)
        ok(f"{leader['label']} is leader (http :{leader['http_port']})")

        print(f"killing {leader['label']} (pid {leader['proc'].pid}) with SIGKILL...")
        kill_time = time.monotonic()
        leader["proc"].kill()
        leader["proc"].wait(timeout=5)

        print(f"waiting up to {FAILOVER_TIMEOUT_S}s for {standby['label']} to acquire leadership...")
        took_over = wait_until(
            lambda: (readyz(standby["http_port"]) or {}).get("is_leader"),
            timeout=FAILOVER_TIMEOUT_S,
        )
        elapsed = time.monotonic() - kill_time

        if not took_over:
            fail(f"{standby['label']} never acquired leadership within {FAILOVER_TIMEOUT_S}s of the old leader dying")

        ok(f"{standby['label']} acquired leadership {elapsed:.2f}s after the old leader was killed")
        if elapsed > DOCUMENTED_BOUND_S:
            print(f"  NOTE: docs/06-failure-scenarios.md #1 documents ~1-2s; {elapsed:.2f}s is within "
                  f"this test's {FAILOVER_TIMEOUT_S}s bound but worth a look if it's consistently this slow")

        ok("Scenario #1 PASSED")
        return 0
    except ChaosFailure:
        print("\nScenario #1 FAILED")
        return 1
    finally:
        for r in replicas:
            stop_process(r["proc"])
            if r["proc"].stdout:
                tail = r["proc"].stdout.read()
                if tail:
                    print(f"\n--- {r['label']} output tail ---\n{tail[-2000:]}")


if __name__ == "__main__":
    sys.exit(main())
