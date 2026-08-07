"""Load test for the REST submission path (docs/08-implementation-roadmap.md
Milestone 7). Run alongside fake_worker.py (which supplies the simulated
worker fleet; without it, every submitted job just sits QUEUED forever
and every request here will time out) via:

    pip install locust
    locust -f locustfile.py --host http://localhost:8080

Then open http://localhost:8089 to configure user count/spawn rate and
watch p50/p95/p99 build up live, or run headless:

    locust -f locustfile.py --host http://localhost:8080 \\
        --users 50 --spawn-rate 10 --run-time 2m --headless

If the scheduler has FLEETFORGE_JWT_SECRET set (docs/09-design-rationale.md
9.4), export FLEETFORGE_TOKEN with a jobs:submit-scoped token first (see
`fleetforgectl auth mint-token`); otherwise every POST /v1/jobs gets a 401
and this only measures how fast the scheduler can reject requests.
"""
import os
import random
import string
import time

from locust import HttpUser, between, events, task

ASSIGNMENT_POLL_INTERVAL_S = 0.2
ASSIGNMENT_TIMEOUT_S = 30


class FleetForgeUser(HttpUser):
    # Deliberately not "as fast as possible": doc 9.2's autoscaler design
    # assumes a somewhat realistic submission rate (CI systems firing on
    # commits/PRs), not a flood. --users/--spawn-rate is how you dial up
    # the sustained rate for a given test run; this wait_time just keeps
    # any single simulated CI client from hammering faster than a real one
    # plausibly would.
    wait_time = between(0.5, 2.0)

    def on_start(self):
        token = os.environ.get("FLEETFORGE_TOKEN", "")
        if token:
            self.client.headers.update({"Authorization": f"Bearer {token}"})

    @task
    def submit_and_track_assignment(self):
        payload = {
            "priority": random.randint(0, 9),
            "repository": f"github.com/example/repo-{random.randint(1, 20)}",
            "branch": "main",
            "commit_sha": "".join(random.choices(string.hexdigits.lower(), k=40)),
            "max_retries": 3,
            # Unique per request: doc 3's idempotency-key dedupe would
            # otherwise make every retry-of-a-failed-request after the
            # first look like a duplicate submission, silently skewing the
            # measured throughput down.
            "idempotency_key": f"loadtest-{time.time_ns()}",
        }

        start = time.monotonic()
        job_id = None
        with self.client.post("/v1/jobs", json=payload, catch_response=True) as resp:
            if resp.status_code not in (200, 202):
                resp.failure(f"submit failed: {resp.status_code} {resp.text[:200]}")
                return
            resp.success()
            job_id = resp.json().get("id")

        if not job_id:
            return

        # The POST above only measures "did Postgres durably accept this
        # job" (doc 5.4's first half); the number that actually matters
        # for doc 9's scheduler benchmark is submission-to-ASSIGNED, which
        # requires a worker fleet to exist (fake_worker.py) and is recorded
        # here as its own named metric so Locust's percentiles reflect
        # real scheduling latency, not just REST API response time.
        assigned_at = None
        last_status = "QUEUED"
        deadline = start + ASSIGNMENT_TIMEOUT_S
        while time.monotonic() < deadline:
            r = self.client.get(f"/v1/jobs/{job_id}", name="/v1/jobs/[id] (poll)")
            if r.status_code == 200:
                last_status = r.json().get("status", last_status)
                if last_status != "QUEUED":
                    assigned_at = time.monotonic()
                    break
            time.sleep(ASSIGNMENT_POLL_INTERVAL_S)

        elapsed_ms = ((assigned_at or time.monotonic()) - start) * 1000
        events.request.fire(
            request_type="ASSIGN",
            name="job_assignment_latency",
            response_time=elapsed_ms,
            response_length=0,
            exception=None
            if assigned_at
            else TimeoutError(
                f"job {job_id} still QUEUED after {ASSIGNMENT_TIMEOUT_S}s (last_status={last_status}) "
                "-- is fake_worker.py running against the same scheduler?"
            ),
        )
