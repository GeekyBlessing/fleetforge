.PHONY: build test integration-test lint proto migrate up down fmt demo demo-clean load-test

build:
	go build -o bin/scheduler ./cmd/scheduler
	go build -o bin/worker-agent ./cmd/worker-agent
	go build -o bin/fleetforgectl ./cmd/fleetforgectl

test:
	go test ./... -short -race -count=1

integration-test:
	go test ./test/integration/... -race -count=1 -tags=integration -timeout=5m

lint:
	golangci-lint run ./...

fmt:
	gofmt -l -w .
	goimports -l -w .

proto:
	protoc \
		--go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/fleetforge/v1/*.proto

migrate:
	migrate -path internal/store/postgres/migrations \
		-database "$$FLEETFORGE_DATABASE_URL" up

migrate-down:
	migrate -path internal/store/postgres/migrations \
		-database "$$FLEETFORGE_DATABASE_URL" down 1

up:
	docker compose -f deploy/docker-compose.yml up --build

down:
	docker compose -f deploy/docker-compose.yml down -v

demo:
	bash scripts/demo.sh

demo-clean:
	bash scripts/demo-clean.sh

# Requires a real scheduler already running (make up + ./bin/scheduler) and
# the load-test Python deps installed: cd test/load && pip install -r requirements.txt
# See test/load/README.md for the full picture, including --num-workers and
# --users tuning. Results worth keeping belong in docs/benchmarks/, not here.
load-test:
	cd test/load && python3 fake_worker.py --scheduler-addr localhost:9090 --num-workers 50 --duration 180 & \
	cd test/load && locust -f locustfile.py --host http://localhost:8080 --users 20 --spawn-rate 5 --run-time 1m --headless
