#!/usr/bin/env bash
# Tears down everything scripts/demo.sh started: the background scheduler
# and worker-agent processes, then the Docker Compose stack and its
# volumes.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

RUN_DIR="$REPO_ROOT/.demo"

log() { echo "[demo-clean] $*"; }

if [ -d "$RUN_DIR" ]; then
  for pidfile in "$RUN_DIR"/*.pid; do
    [ -e "$pidfile" ] || continue
    pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      log "stopping pid $pid ($(basename "$pidfile" .pid))"
      kill "$pid" 2>/dev/null || true
      sleep 1
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$RUN_DIR"
fi

log "stopping docker compose stack..."
docker compose -f deploy/docker-compose.yml down -v

log "done."
