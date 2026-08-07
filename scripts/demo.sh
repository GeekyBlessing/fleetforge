#!/usr/bin/env bash
# End-to-end local demo: brings up infrastructure, builds and starts a real
# scheduler and three real worker agents, submits sample jobs, and prints
# the resulting state. Exercises the actual binaries, not a simulation of
# them. Run `scripts/demo-clean.sh` (or `make demo-clean`) to tear
# everything down afterward.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RUN_DIR="$REPO_ROOT/.demo"
mkdir -p "$RUN_DIR"

export FLEETFORGE_DATABASE_URL="${FLEETFORGE_DATABASE_URL:-postgres://fleetforge:fleetforge_dev_only@localhost:5432/fleetforge?sslmode=disable}"
SCHEDULER_HTTP="http://localhost:8080"

log() { echo "[demo] $*"; }

wait_for() {
  # wait_for <description> <timeout_seconds> <command...>
  local desc="$1" timeout="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      log "timed out waiting for: $desc"
      return 1
    fi
    sleep 1
  done
  log "ready: $desc"
}

log "starting infrastructure (postgres, redis, prometheus, grafana)..."
docker compose -f deploy/docker-compose.yml up -d postgres redis prometheus grafana

wait_for "postgres accepting connections" 60 \
  docker compose -f deploy/docker-compose.yml exec -T postgres pg_isready -U fleetforge -d fleetforge

log "applying migrations..."
docker compose -f deploy/docker-compose.yml up migrate

log "building binaries..."
make build

log "starting scheduler..."
./bin/scheduler > "$RUN_DIR/scheduler.log" 2>&1 &
echo $! > "$RUN_DIR/scheduler.pid"

wait_for "scheduler /readyz reporting postgres ok" 30 \
  bash -c "curl -sf $SCHEDULER_HTTP/readyz | grep -q '\"postgres\":true'"

log "starting 3 worker agents..."
for i in 1 2 3; do
  FLEETFORGE_INSTANCE_ID="demo-worker-$i" \
  FLEETFORGE_HOSTNAME="demo-worker-$i" \
  FLEETFORGE_CPU_CORES=4 \
  FLEETFORGE_MEMORY_MB=8192 \
  FLEETFORGE_CAPACITY_SLOTS=1 \
  ./bin/worker-agent > "$RUN_DIR/worker-$i.log" 2>&1 &
  echo $! > "$RUN_DIR/worker-$i.pid"
done

wait_for "3 workers registered" 30 \
  bash -c "[ \"\$(curl -sf $SCHEDULER_HTTP/v1/workers | grep -o '\"id\"' | wc -l)\" -ge 3 ]"

log "submitting 5 sample jobs..."
for i in 1 2 3 4 5; do
  commit_sha="$(date +%s)-$i-$RANDOM"
  curl -sf -X POST "$SCHEDULER_HTTP/v1/jobs" \
    -H 'Content-Type: application/json' \
    -d "{\"repository\":\"github.com/example/demo-app\",\"branch\":\"main\",\"commit_sha\":\"$commit_sha\",\"idempotency_key\":\"demo-job-$i-$commit_sha\"}" \
    > /dev/null
done

log "waiting for jobs to be picked up..."
sleep 8

echo ""
echo "=== Jobs ==="
curl -sf "$SCHEDULER_HTTP/v1/jobs" | (python3 -m json.tool 2>/dev/null || cat)

echo ""
echo "=== Workers ==="
./bin/fleetforgectl workers list || true

echo ""
log "scheduler log:     $RUN_DIR/scheduler.log"
log "worker logs:        $RUN_DIR/worker-*.log"
log "Grafana:            http://localhost:3000"
log "Prometheus:         http://localhost:9091"
log "REST API:           $SCHEDULER_HTTP"
echo ""
log "run 'make demo-clean' (or scripts/demo-clean.sh) to stop everything started here."
