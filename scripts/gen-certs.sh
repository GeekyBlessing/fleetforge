#!/usr/bin/env bash
# Generates a local self-signed CA plus a server certificate (for
# cmd/scheduler) and a client certificate (for cmd/worker-agent), for the
# mTLS control plane described in docs/09-design-rationale.md 9.4.
#
# This is a dev/demo CA, not a production one: one shared client cert for
# every worker (a real fleet would issue one per worker/host at provisioning
# time, per doc 9.4's "baked into the worker image / cloud-init" note), a
# 365-day expiry, and no revocation (CRL/OCSP) story. It's enough to prove
# the mTLS wiring end to end -- internal/auth/mtls.go doesn't care how a
# cert was issued, only that it validates.
set -euo pipefail

OUT_DIR="${1:-certs}"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

echo "Generating CA..."
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 \
  -subj "/O=FleetForge/CN=FleetForge Dev CA" \
  -out ca.crt

echo "Generating scheduler server certificate..."
openssl genrsa -out server.key 4096 2>/dev/null
openssl req -new -key server.key \
  -subj "/O=FleetForge/CN=scheduler" \
  -out server.csr
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -sha256 \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:scheduler,IP:127.0.0.1") \
  -out server.crt
rm -f server.csr

echo "Generating worker-agent client certificate..."
openssl genrsa -out worker-client.key 4096 2>/dev/null
openssl req -new -key worker-client.key \
  -subj "/O=FleetForge/CN=worker-agent" \
  -out worker-client.csr
openssl x509 -req -in worker-client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -sha256 \
  -out worker-client.crt
rm -f worker-client.csr

rm -f ca.srl

echo ""
echo "Done. Files written to $OUT_DIR/:"
echo "  ca.crt              - trust root; needed by BOTH sides (FLEETFORGE_TLS_CA_FILE)"
echo "  server.crt/server.key         - cmd/scheduler (FLEETFORGE_TLS_CERT_FILE / FLEETFORGE_TLS_KEY_FILE)"
echo "  worker-client.crt/worker-client.key - cmd/worker-agent (same two env vars)"
echo ""
echo "To enable mTLS, set on the scheduler:"
echo "  export FLEETFORGE_TLS_CERT_FILE=$OUT_DIR/server.crt"
echo "  export FLEETFORGE_TLS_KEY_FILE=$OUT_DIR/server.key"
echo "  export FLEETFORGE_TLS_CA_FILE=$OUT_DIR/ca.crt"
echo "and on each worker-agent:"
echo "  export FLEETFORGE_TLS_CERT_FILE=$OUT_DIR/worker-client.crt"
echo "  export FLEETFORGE_TLS_KEY_FILE=$OUT_DIR/worker-client.key"
echo "  export FLEETFORGE_TLS_CA_FILE=$OUT_DIR/ca.crt"
