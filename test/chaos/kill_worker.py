#!/usr/bin/env python3
"""Chaos test: docs/06-failure-scenarios.md scenario #3: a worker crashes
(process dies, host dies, OOM-killed) while running a job.

Spins up one real `bin/worker-agent` against an already-running scheduler,
submits a job, waits for it to be assigned to that worker, then SIGKILLs
the worker-agent process outright (no graceful shutdown, no final
ReportJobResult: a genuine crash, not a clean disconnect). Asserts:

  1. The worker is marked DEAD within the heartbeat-timeout window
     (default 20s + the 5s reaper sweep interval, per docs/05-sequence-diagrams.md 5.3).
  2. Its in-flight job is atomically requeued rather than left stuck
     ASSIGNED/RUNNING forever (docs/06-failure-scenarios.md #3's "same
     transaction" guarantee).

Prerequisites:
    make build                              # produces bin/worker-agent
    A scheduler already running and reachable (this script only starts
    the worker; see test/chaos/kill_leader.py for spinning up
    schedulers themselves).

Usage:
    python3 test/chaos/kill_worker.py
"""
import os
import sys
import time
import uuid

from common import ChaosFailure, REST_BASE, fail, http_get, http_post, ok, section, start_binary, stop_process, wait_until

WORKER_READY_TIMEOUT_S = 15
JOB_ASSIGNED_TIMEOUT_S = 20
DEAD_TIMEOUT_S = 35  # 20s heartbeat timeout + 5s reaper sweep interval + buffer
REQUEUE_TIMEOUT_S = 35


def submit_job() -> str:
    status, body = http_post(f"{REST_BASE}/v1/jobs", {
        "repository": "github.com/example/chaos-test",
        "branch": "main",
        "commit_sha": uuid.uuid4().hex,
        "max_retries": 3,
        "idempotency_key": f"chaos-kill-worker-{uuid.uuid4().hex}",
    })
    if status not in (200, 202):
        fail(f"job submission failed: {status} {body}")
    return body["id"]


def get_job(job_id: str) -> dict:
    return http_get(f"{REST_BASE}/v1/jobs/{job_id}")


def get_worker(worker_id: str) -> dict | None:
    body = http_get(f"{REST_BASE}/v1/workers")
    for w in body.get("items", []):
        if w["id"] == worker_id:
            return w
    return None


def main() -> int:
    section("Scenario #3: worker crash mid-job (docs/06-failure-scenarios.md #3)")

    env = os.environ.copy()
    env["FLEETFORGE_INSTANCE_ID"] = f"chaos-kill-worker-{uuid.uuid4().hex[:8]}"
    env.setdefault("FLEETFORGE_SCHEDULER_ADDR", "localhost:9090")
    env.setdefault("FLEETFORGE_CPU_CORES", "4")
    env.setdefault("FLEETFORGE_MEMORY_MB", "8192")
    env.setdefault("FLEETFORGE_CAPACITY_SLOTS", "1")

    worker_proc = start_binary("worker-agent", env)
    print(f"started worker-agent (pid {worker_proc.pid}, instance_id={env['FLEETFORGE_INSTANCE_ID']})")

    try:
        print(f"submitting a job and waiting up to {JOB_ASSIGNED_TIMEOUT_S}s for it to be assigned...")
        job_id = submit_job()

        def job_assigned():
            job = get_job(job_id)
            return job if job.get("worker_id") else None

        job = wait_until(job_assigned, timeout=max(WORKER_READY_TIMEOUT_S, JOB_ASSIGNED_TIMEOUT_S))
        if job is None:
            fail(f"job {job_id} was never assigned to a worker -- is the worker-agent registering? "
                 "check the process output below")
        worker_id = job["worker_id"]
        ok(f"job {job_id} assigned to worker {worker_id} (status={job['status']})")

        print(f"SIGKILLing worker-agent (pid {worker_proc.pid}) -- no graceful shutdown, no final report...")
        crash_time = time.monotonic()
        worker_proc.kill()
        worker_proc.wait(timeout=5)

        print(f"waiting up to {DEAD_TIMEOUT_S}s for the reaper to mark it DEAD...")
        dead = wait_until(
            lambda: (get_worker(worker_id) or {}).get("status") == "DEAD",
            timeout=DEAD_TIMEOUT_S,
        )
        dead_elapsed = time.monotonic() - crash_time
        if not dead:
            current = get_worker(worker_id)
            fail(f"worker {worker_id} was never marked DEAD within {DEAD_TIMEOUT_S}s "
                 f"(last seen status={current.get('status') if current else 'not found'})")
        ok(f"worker marked DEAD {dead_elapsed:.1f}s after the crash")

        print(f"waiting up to {REQUEUE_TIMEOUT_S}s for job {job_id} to leave ASSIGNED/RUNNING...")
        requeued = wait_until(
            lambda: get_job(job_id).get("status") not in ("ASSIGNED", "RUNNING"),
            timeout=REQUEUE_TIMEOUT_S,
        )
        if not requeued:
            fail(f"job {job_id} is still stuck at {get_job(job_id).get('status')} -- "
                 f"the dead-worker requeue transaction did not fire")
        final_status = get_job(job_id)["status"]
        ok(f"job {job_id} left ASSIGNED/RUNNING (now {final_status}) -- not stuck")

        ok("Scenario #3 PASSED")
        return 0
    except ChaosFailure:
        print("\nScenario #3 FAILED")
        return 1
    finally:
        stop_process(worker_proc)
        if worker_proc.stdout:
            tail = worker_proc.stdout.read()
            if tail:
                print(f"\n--- worker-agent output tail ---\n{tail[-2000:]}")


if __name__ == "__main__":
    sys.exit(main())
