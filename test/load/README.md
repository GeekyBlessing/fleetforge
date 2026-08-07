# Load testing

Two independent pieces, run together against a live scheduler
(`cmd/scheduler`), per docs/08-implementation-roadmap.md Milestone 7 and
docs/09-design-rationale.md 9.5:

- **`fake_worker.py`**: simulates a fleet of workers over the real gRPC
  control plane (register, heartbeat, execute/report). Without this
  running, submitted jobs have nothing to be assigned to and every
  `locustfile.py` run will just time out waiting for assignment.
- **`locustfile.py`**: simulates CI/human traffic submitting jobs over
  the REST API, and measures submission-to-`ASSIGNED` latency, not just
  the `POST /jobs` response time, as a named Locust metric,
  `job_assignment_latency`.

## Setup

```bash
cd test/load
pip install -r requirements.txt
```

The gRPC stubs in `pb/` are pre-generated from `proto/fleetforge/v1/*.proto`.
Regenerate them if those `.proto` files change:

```bash
pip install grpcio-tools
python3 -m grpc_tools.protoc -I<repo-root> \
    --python_out=pb --grpc_python_out=pb \
    <repo-root>/proto/fleetforge/v1/worker.proto \
    <repo-root>/proto/fleetforge/v1/scheduler.proto
```

## Running

With `cmd/scheduler` already running (and Postgres/Redis up), in one
terminal:

```bash
python3 fake_worker.py --scheduler-addr localhost:9090 --num-workers 200 --duration 300
```

In another, either the interactive web UI:

```bash
locust -f locustfile.py --host http://localhost:8080
# open http://localhost:8089, set user count / spawn rate, watch it live
```

or headless, for a fixed run you can paste into a writeup:

```bash
locust -f locustfile.py --host http://localhost:8080 \
    --users 50 --spawn-rate 10 --run-time 2m --headless
```

Locust's summary table at the end reports p50/p95/p99 for both the raw
`POST /v1/jobs` call and the custom `job_assignment_latency` metric.
The second one is the real "how long from submission to a worker picking
it up" number docs/09-design-rationale.md 9.5 asks for.
`fake_worker.py` separately reports heartbeat round-trip latency
percentiles when it exits (Ctrl+C, or its `--duration` elapsing).

If the scheduler has `FLEETFORGE_TLS_CERT_FILE`/`KEY_FILE`/`CA_FILE` set
(mTLS, see `scripts/gen-certs.sh`), pass the worker client cert to
`fake_worker.py`:

```bash
python3 fake_worker.py --tls-cert ../../certs/worker-client.crt \
    --tls-key ../../certs/worker-client.key --tls-ca ../../certs/ca.crt
```

If it has `FLEETFORGE_JWT_SECRET` set, export a `jobs:submit`-scoped token
before starting Locust:

```bash
export FLEETFORGE_TOKEN=$(fleetforgectl auth mint-token --scopes jobs:submit)
```
