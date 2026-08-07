"""Shared helpers for the chaos test scripts (docs/08-implementation-roadmap.md
Milestone 7, automating scenarios from docs/06-failure-scenarios.md). Pure
stdlib on purpose: unlike test/load, these scripts talk to real
`bin/scheduler` and `bin/worker-agent` processes over plain HTTP for
observation, so there's nothing here that needs grpcio or requests.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BIN_DIR = os.path.join(REPO_ROOT, "bin")

REST_BASE = os.environ.get("FLEETFORGE_REST_ADDR", "http://localhost:8080")


class ChaosFailure(Exception):
    pass


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def ok(msg: str) -> None:
    print(f"  [OK] {msg}")


def fail(msg: str) -> None:
    """Prints the failure and raises; callers should let this propagate
    to main() so process cleanup (stopping spawned binaries) still runs via
    a try/finally, rather than calling sys.exit directly here.
    """
    print(f"  [FAIL] {msg}")
    raise ChaosFailure(msg)


def require_binary(name: str) -> str:
    path = os.path.join(BIN_DIR, name)
    if not os.path.isfile(path):
        print(f"error: {path} not found -- run `make build` first (see test/chaos/README.md)", file=sys.stderr)
        sys.exit(1)
    return path


def require_database_url() -> str:
    url = os.environ.get("FLEETFORGE_DATABASE_URL")
    if not url:
        print("error: FLEETFORGE_DATABASE_URL is not set (needed to start extra scheduler replicas)", file=sys.stderr)
        sys.exit(1)
    return url


def start_binary(name: str, env: dict) -> subprocess.Popen:
    path = require_binary(name)
    return subprocess.Popen(
        [path],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def stop_process(proc: subprocess.Popen, timeout: float = 5.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def http_get(url: str, timeout: float = 3.0):
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())


def http_post(url: str, body: dict, timeout: float = 3.0, headers: dict | None = None):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def wait_until(predicate, timeout: float, interval: float = 0.5):
    """Polls predicate() until it returns a truthy value or timeout elapses.
    Returns the last truthy result, or None on timeout.

    Exceptions from predicate() are swallowed while polling (e.g. connection
    refused during the first second while a process is still starting up,
    which is normal and not a failure worth aborting over). If every single
    attempt across the whole window raised instead of returning a real
    answer, that's indistinguishable from a silent no-op unless surfaced
    somehow (the same failure class as fake_worker.py's RegisterWorker
    errors being swallowed by return_exceptions=True), so on timeout, if
    predicate() never once completed without raising, print the last
    exception before returning None.
    """
    deadline = time.monotonic() + timeout
    last_exc = None
    ever_succeeded_without_raising = False
    while time.monotonic() < deadline:
        try:
            result = predicate()
            ever_succeeded_without_raising = True
        except Exception as e:  # noqa: BLE001 (intentionally broad, see docstring)
            last_exc = e
            result = None
        if result:
            return result
        time.sleep(interval)
    if not ever_succeeded_without_raising and last_exc is not None:
        print(f"  (wait_until: every attempt raised; last error: {type(last_exc).__name__}: {last_exc})")
    return None
