#!/usr/bin/env python3
"""Chaos test: docs/06-failure-scenarios.md scenario #6: a worker that's
still alive but can't reach the scheduler (network partition), as opposed
to kill_worker.py's outright crash.

Simulated by SIGSTOP'ing a real `bin/worker-agent` process rather than
killing it: this freezes it entirely (it can't send heartbeats, but its
TCP connection isn't cleanly closed either), which is a closer
approximation of "partitioned, not crashed" than a process exit. A fully
faithful version would sever the connection at the network level (e.g.
via toxiproxy) rather than freezing the process; noted in
docs/09-design-rationale.md's open-scope list as a reasonable follow-up,
not solved here.

Asserts two things doc 6 #6 and doc 5.3's edge case #1 both depend on:

  1. The frozen worker is detected exactly like a crash: marked DEAD
     after the heartbeat timeout (scenario #3's detection path is
     scenario-agnostic by design).
  2. Once resumed (SIGCONT), the worker-agent's OWN contract fires: on
     reconnecting with its now-stale epoch, the scheduler rejects it, and
     the agent fatal-exits demanding re-registration rather than silently
     resuming as if nothing happened. This is the split-brain guard,
     verified here by grepping the resumed process's own log output for
     the "must re-register" rejection the worker-agent contract requires
     it to emit, rather than asserting on internal state directly.

Prerequisites:
    make build                    # produces bin/worker-agent
    A scheduler already running and reachable

Usage:
    python3 test/chaos/partition_worker.py
"""
import os
import signal
import sys
import time
import uuid

from common import ChaosFailure, REST_BASE, fail, http_get, ok, section, start_binary, stop_process, wait_until

WORKER_READY_TIMEOUT_S = 15
DEAD_TIMEOUT_S = 35  # 20s heartbeat timeout + 5s reaper sweep interval + buffer
REJECTION_TIMEOUT_S = 15
REJECTION_MARKERS = ("must re-register", "code = NotFound", "is DEAD")


def get_worker(worker_id: str) -> dict | None:
    body = http_get(f"{REST_BASE}/v1/workers")
    for w in body.get("items", []):
        if w["id"] == worker_id:
            return w
    return None


def find_worker_by_instance(instance_id_marker: str) -> dict | None:
    # There's no GET /workers?instance_id=... filter, so cross-reference by
    # hostname instead (worker-agent defaults hostname to os.Hostname(),
    # which won't contain our marker, so this script sets FLEETFORGE_HOSTNAME
    # explicitly to make matching reliable rather than guessing).
    body = http_get(f"{REST_BASE}/v1/workers")
    for w in body.get("items", []):
        if instance_id_marker in w.get("hostname", ""):
            return w
    return None


def main() -> int:
    section("Scenario #6: network partition, worker alive but unreachable (docs/06-failure-scenarios.md #6)")

    marker = f"chaos-partition-{uuid.uuid4().hex[:8]}"
    env = os.environ.copy()
    env["FLEETFORGE_INSTANCE_ID"] = marker
    env["FLEETFORGE_HOSTNAME"] = marker
    env.setdefault("FLEETFORGE_SCHEDULER_ADDR", "localhost:9090")
    env.setdefault("FLEETFORGE_CPU_CORES", "4")
    env.setdefault("FLEETFORGE_MEMORY_MB", "8192")
    env.setdefault("FLEETFORGE_CAPACITY_SLOTS", "1")

    worker_proc = start_binary("worker-agent", env)
    print(f"started worker-agent (pid {worker_proc.pid}, hostname={marker})")

    try:
        print(f"waiting up to {WORKER_READY_TIMEOUT_S}s for it to register...")
        worker = wait_until(lambda: find_worker_by_instance(marker), timeout=WORKER_READY_TIMEOUT_S)
        if worker is None:
            fail("worker never appeared in GET /workers -- check the process output below")
        worker_id = worker["id"]
        ok(f"worker {worker_id} registered (epoch {worker['epoch']})")

        print(f"freezing worker-agent (pid {worker_proc.pid}) with SIGSTOP -- simulating a partition, not a crash...")
        freeze_time = time.monotonic()
        worker_proc.send_signal(signal.SIGSTOP)

        print(f"waiting up to {DEAD_TIMEOUT_S}s for the reaper to mark it DEAD despite the process still existing...")
        dead = wait_until(lambda: (get_worker(worker_id) or {}).get("status") == "DEAD", timeout=DEAD_TIMEOUT_S)
        dead_elapsed = time.monotonic() - freeze_time
        if not dead:
            fail(f"worker {worker_id} was never marked DEAD within {DEAD_TIMEOUT_S}s of being frozen")
        ok(f"worker marked DEAD {dead_elapsed:.1f}s after the freeze (process itself never died)")

        print("resuming the frozen process with SIGCONT -- the partition 'heals'...")
        worker_proc.send_signal(signal.SIGCONT)

        print(f"waiting up to {REJECTION_TIMEOUT_S}s for the resumed worker to hit the stale-epoch rejection...")

        def saw_rejection():
            if worker_proc.poll() is None:
                return False  # hasn't exited yet
            return True  # exited; caller checks the log content below

        exited = wait_until(saw_rejection, timeout=REJECTION_TIMEOUT_S)
        output = worker_proc.stdout.read() if worker_proc.stdout else ""
        if not exited:
            fail(f"resumed worker-agent did not exit within {REJECTION_TIMEOUT_S}s -- expected it to fatal-exit "
                 f"on rejection per the worker-agent contract (docs/06-failure-scenarios.md #6)")

        if not any(marker_text in output for marker_text in REJECTION_MARKERS):
            fail("worker-agent exited, but its output doesn't show the expected stale-epoch rejection "
                 f"(looked for any of {REJECTION_MARKERS}) -- output:\n{output[-2000:]}")

        ok("resumed worker correctly got rejected and fatal-exited demanding re-registration "
           "-- split-brain guard confirmed on the wire, not just in the docs")

        ok("Scenario #6 PASSED")
        return 0
    except ChaosFailure:
        print("\nScenario #6 FAILED")
        return 1
    finally:
        stop_process(worker_proc)
        if worker_proc.stdout and not worker_proc.stdout.closed:
            tail = worker_proc.stdout.read()
            if tail:
                print(f"\n--- worker-agent output tail ---\n{tail[-2000:]}")


if __name__ == "__main__":
    sys.exit(main())
