# Benchmarks

This directory holds committed, reproducible load-test results. It is empty except for this file until someone actually runs the load-testing harness (`test/load/`) and commits the output.

No numbers are invented or estimated in this repository. If this directory contains no dated result files, no benchmark has been run yet, full stop.

## How to produce a result worth committing

1. Note the environment: machine (CPU/RAM), OS, whether Postgres/Redis are local Docker containers or remote, and whether mTLS/JWT are enabled (both add measurable overhead per request).
2. Start the real scheduler and a real Postgres/Redis, not a scaled-down substitute.
3. Run the harness per `test/load/README.md`:

   ```bash
   cd test/load
   python3 fake_worker.py --scheduler-addr localhost:9090 --num-workers 200 --duration 300
   locust -f locustfile.py --host http://localhost:8080 \
       --users 50 --spawn-rate 10 --run-time 2m --headless
   ```

4. Capture both outputs in full: `fake_worker.py`'s heartbeat round-trip latency percentiles, and Locust's summary table (raw `POST /v1/jobs` latency and the custom `job_assignment_latency` metric).
5. Save the raw output as `docs/benchmarks/YYYY-MM-DD-<short-description>.md`, with the environment details from step 1 at the top, unedited numbers below.

## Format for a result file

```
# 2026-08-07: baseline, 200 workers, 50 concurrent submitters

Environment: <machine spec>, macOS/Linux, Docker Compose Postgres 16 + Redis 7 on localhost, mTLS off, JWT off.
Command: <exact command run>

<raw fake_worker.py output>

<raw locust summary table>
```

Keep it unedited. The value of this directory is that every number in it can be reproduced by re-running the exact command listed.
