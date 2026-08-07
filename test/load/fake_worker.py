#!/usr/bin/env python3
"""fake_worker.py -- simulates a fleet of FleetForge workers for load
testing (docs/08-implementation-roadmap.md Milestone 7).

Speaks the same gRPC control plane as cmd/worker-agent (RegisterWorker,
the bidirectional Heartbeat stream, ReportJobResult) so it exercises the
real scheduler code path -- registration, heartbeat processing, assignment
push, completion handling -- not a stand-in for it. The only thing that's
faked is job execution itself: instead of running `docker run`, each
simulated job just sleeps for a random duration and reports
success/failure per --failure-rate. This is deliberately Python (like
locustfile.py) rather than another Go binary -- doc 7's stated reason
applies here too: spinning up hundreds of lightweight simulated agents is
an easier fit for Python's async/scripting ergonomics than compiling and
supervising hundreds of real worker-agent processes, and performance of
the SIMULATOR itself isn't what's under test.

Usage:
    pip install grpcio
    python3 fake_worker.py --num-workers 200 --scheduler-addr localhost:9090

The pb/ subdirectory holds pre-generated Python gRPC stubs from
proto/fleetforge/v1/*.proto. Regenerate them if the .proto files change
(run from the repo root):

    pip install grpcio-tools
    python3 -m grpc_tools.protoc -I. --python_out=test/load/pb \\
        --grpc_python_out=test/load/pb \\
        proto/fleetforge/v1/worker.proto proto/fleetforge/v1/scheduler.proto
"""
import argparse
import asyncio
import random
import signal
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "pb"))

import grpc  # noqa: E402
from proto.fleetforge.v1 import scheduler_pb2, scheduler_pb2_grpc, worker_pb2  # noqa: E402

heartbeat_latencies_ms: list[float] = []
assignments_received = 0
jobs_completed = 0
_stats_lock = asyncio.Lock()


async def run_worker(
    idx: int,
    addr: str,
    channel_creds,
    capacity_slots: int,
    failure_rate: float,
) -> None:
    """One simulated worker's whole lifecycle: register once, then hold a
    Heartbeat stream open exactly like cmd/worker-agent's real loop, ending
    only when the surrounding task is cancelled (main()'s --duration
    timeout).
    """
    global assignments_received, jobs_completed

    channel = (
        grpc.aio.secure_channel(addr, channel_creds)
        if channel_creds is not None
        else grpc.aio.insecure_channel(addr)
    )

    async with channel:
        stub = scheduler_pb2_grpc.FleetSchedulerStub(channel)

        # A stable-per-process instance_id, not a fixed one -- each fake
        # worker is its own "machine" for the duration of this run, same as
        # a real worker-agent picks one identity for its whole lifetime
        # (docs/05-sequence-diagrams.md 5.1).
        instance_id = f"loadtest-{idx}-{uuid.uuid4().hex[:8]}"
        reg = await stub.RegisterWorker(
            scheduler_pb2.RegisterWorkerRequest(
                hostname=f"fake-worker-{idx}",
                instance_id=instance_id,
                os="linux/amd64",
                cpu_cores=4,
                memory_mb=8192,
                version="loadtest",
                capacity_slots=capacity_slots,
            )
        )
        worker_id, epoch = reg.worker_id, reg.epoch
        interval = reg.heartbeat_interval_seconds or 5

        running_jobs: dict[str, asyncio.Task] = {}
        send_times: list[float] = []

        call = stub.Heartbeat()

        async def execute_job(job_id: str, assignment_epoch: int) -> None:
            global jobs_completed
            await asyncio.sleep(random.uniform(1.0, 3.0))  # simulated build duration
            success = random.random() > failure_rate
            try:
                await stub.ReportJobResult(
                    scheduler_pb2.ReportJobResultRequest(
                        job_id=job_id,
                        assignment_epoch=assignment_epoch,
                        status=(
                            worker_pb2.JOB_STATUS_SUCCESS
                            if success
                            else worker_pb2.JOB_STATUS_FAILED
                        ),
                        exit_code=0 if success else 1,
                        log_ref="",
                        error_message="" if success else "loadtest: simulated failure",
                    )
                )
            except grpc.RpcError:
                # The scheduler may have already reassigned this job (e.g.
                # a reaper sweep decided this fake worker looked dead) --
                # not a load-test failure, same discard-on-stale-epoch path
                # docs/06-failure-scenarios.md #10 documents for the real
                # worker-agent.
                pass
            running_jobs.pop(job_id, None)
            async with _stats_lock:
                jobs_completed += 1

        async def send_loop() -> None:
            try:
                while True:
                    status = (
                        worker_pb2.WORKER_STATUS_BUSY
                        if running_jobs
                        else worker_pb2.WORKER_STATUS_READY
                    )
                    current_job_id = next(iter(running_jobs), "")
                    send_times.append(time.monotonic())
                    await call.write(
                        scheduler_pb2.HeartbeatRequest(
                            worker_id=worker_id,
                            epoch=epoch,
                            status=status,
                            current_job_id=current_job_id,
                            available_capacity=max(0, capacity_slots - len(running_jobs)),
                        )
                    )
                    await asyncio.sleep(interval)
            except asyncio.CancelledError:
                await call.done_writing()
                raise

        async def recv_loop() -> None:
            global assignments_received
            async for resp in call:
                if send_times:
                    latency_ms = (time.monotonic() - send_times.pop(0)) * 1000
                    heartbeat_latencies_ms.append(latency_ms)
                if resp.HasField("assignment") and len(running_jobs) < capacity_slots:
                    a = resp.assignment
                    running_jobs[a.job_id] = asyncio.ensure_future(
                        execute_job(a.job_id, a.assignment_epoch)
                    )
                    async with _stats_lock:
                        assignments_received += 1

        try:
            await asyncio.gather(send_loop(), recv_loop())
        except (grpc.RpcError, asyncio.CancelledError):
            pass


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    ordered = sorted(data)
    k = (len(ordered) - 1) * (p / 100)
    f, c = int(k), min(int(k) + 1, len(ordered) - 1)
    if f == c:
        return ordered[f]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scheduler-addr", default="localhost:9090")
    parser.add_argument("--num-workers", type=int, default=50)
    parser.add_argument("--capacity-slots", type=int, default=2)
    parser.add_argument("--failure-rate", type=float, default=0.1, help="0.0-1.0")
    parser.add_argument("--duration", type=int, default=120, help="seconds to run before reporting and exiting")
    parser.add_argument("--tls-cert", default=None, help="worker client cert, e.g. certs/worker-client.crt")
    parser.add_argument("--tls-key", default=None, help="worker client key, e.g. certs/worker-client.key")
    parser.add_argument("--tls-ca", default=None, help="CA cert, e.g. certs/ca.crt")
    args = parser.parse_args()

    channel_creds = None
    if args.tls_cert and args.tls_key and args.tls_ca:
        channel_creds = grpc.ssl_channel_credentials(
            root_certificates=Path(args.tls_ca).read_bytes(),
            private_key=Path(args.tls_key).read_bytes(),
            certificate_chain=Path(args.tls_cert).read_bytes(),
        )

    print(
        f"Starting {args.num_workers} fake workers against {args.scheduler_addr} "
        f"({'mTLS' if channel_creds else 'insecure'}), running for {args.duration}s..."
    )

    tasks = [
        asyncio.ensure_future(
            run_worker(i, args.scheduler_addr, channel_creds, args.capacity_slots, args.failure_rate)
        )
        for i in range(args.num_workers)
    ]

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: not stop.done() and stop.set_result(None))

    done, _ = await asyncio.wait(
        [asyncio.ensure_future(stop)],
        timeout=args.duration,
    )
    for t in tasks:
        t.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # A worker whose RegisterWorker call itself failed (bad address, TLS
    # handshake rejected, auth misconfigured) never reaches run_worker's
    # own try/except -- surface those here instead of letting
    # return_exceptions=True swallow them into a silent "0 assignments, no
    # heartbeat samples" result that gives no clue what actually happened.
    failures = [r for r in results if isinstance(r, Exception) and not isinstance(r, asyncio.CancelledError)]
    if failures:
        print(f"\n{len(failures)}/{args.num_workers} workers failed to run:")
        for exc in failures[:5]:
            print(f"  - {type(exc).__name__}: {exc}")
        if len(failures) > 5:
            print(f"  ... and {len(failures) - 5} more")

    print("\n--- Load test summary ---")
    print(f"Workers simulated:     {args.num_workers}")
    print(f"Job assignments seen:  {assignments_received}")
    print(f"Jobs completed:        {jobs_completed}")
    if heartbeat_latencies_ms:
        print(
            f"Heartbeat RTT (ms):    p50={percentile(heartbeat_latencies_ms, 50):.1f}  "
            f"p95={percentile(heartbeat_latencies_ms, 95):.1f}  "
            f"p99={percentile(heartbeat_latencies_ms, 99):.1f}  "
            f"max={max(heartbeat_latencies_ms):.1f}  n={len(heartbeat_latencies_ms)}"
        )
    else:
        print("Heartbeat RTT (ms):    no samples collected")


if __name__ == "__main__":
    asyncio.run(main())
