#!/usr/bin/env bash
# Regenerates proto/fleetforge/v1/gen from the .proto sources.
#
# Requires (one-time local setup):
#   brew install protobuf                                    # or apt install protobuf-compiler
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# The generated Go stubs are NOT committed in this snapshot -- generating
# them requires protoc, which isn't available in the environment these files
# were authored in. Run this script once after cloning, before `go build`
# will succeed.
set -euo pipefail

cd "$(dirname "$0")/.."

protoc \
  -I . \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/fleetforge/v1/worker.proto \
  proto/fleetforge/v1/scheduler.proto

echo "Generated proto/fleetforge/v1/gen/*.pb.go"
