//go:build integration

// Package integration exercises real Postgres (via testcontainers) rather
// than mocks, specifically because the guarantee doc 6 leans on hardest --
// the ON CONFLICT (instance_id) upsert + epoch bump in
// internal/store/postgres/workers.go -- is exactly the kind of logic a mock
// would happily let through wrong. If this test suite passes, the actual
// SQL that runs in production has been exercised, not a stand-in for it.
package integration

import (
	"context"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers the "postgres://" driver with migrate.New
	_ "github.com/golang-migrate/migrate/v4/source/file"       // registers the "file://" source with migrate.New
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	fleetforgepg "github.com/launchverse/fleetforge/internal/store/postgres"
)

func setupPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:16-alpine"),
		postgres.WithDatabase("fleetforge"),
		postgres.WithUsername("fleetforge"),
		postgres.WithPassword("fleetforge_test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	applyMigrations(t, connStr)
	return connStr
}

func applyMigrations(t *testing.T, connStr string) {
	t.Helper()

	m, err := migrate.New("file://../../internal/store/postgres/migrations", connStr)
	if err != nil {
		t.Fatalf("failed to init migrate: %v", err)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}
}

func TestWorkerRegistration_NewWorkerGetsEpochOne(t *testing.T) {
	ctx := context.Background()
	connStr := setupPostgres(t)

	pool, err := fleetforgepg.NewPool(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect pool: %v", err)
	}
	defer pool.Close()

	store := fleetforgepg.NewWorkerStore(pool)

	result, err := store.Register(ctx, fleetforgepg.RegisterWorkerParams{
		Hostname:      "build-node-1",
		InstanceID:    "i-abc123",
		OS:            "linux/amd64",
		CPUCores:      16,
		MemoryMB:      65536,
		Version:       "1.0.0",
		CapacitySlots: 2,
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	if result.Epoch != 1 {
		t.Errorf("expected epoch 1 for new worker, got %d", result.Epoch)
	}

	workers, err := store.List(ctx, "READY", 10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(workers) != 1 {
		t.Fatalf("expected 1 READY worker after registration, got %d", len(workers))
	}
	if workers[0].ID != result.ID {
		t.Errorf("expected listed worker id %s, got %s", result.ID, workers[0].ID)
	}
}

// This is the test that actually validates docs/05-sequence-diagrams.md
// section 5.1's re-registration branch: a worker that restarts (same
// instance_id) must get the SAME worker_id back with an INCREMENTED epoch,
// not a second row.
func TestWorkerRegistration_ReRegistrationBumpsEpochNotRowCount(t *testing.T) {
	ctx := context.Background()
	connStr := setupPostgres(t)

	pool, err := fleetforgepg.NewPool(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect pool: %v", err)
	}
	defer pool.Close()

	store := fleetforgepg.NewWorkerStore(pool)

	params := fleetforgepg.RegisterWorkerParams{
		Hostname:      "build-node-2",
		InstanceID:    "i-restarts-a-lot",
		OS:            "linux/amd64",
		CPUCores:      8,
		MemoryMB:      32768,
		Version:       "1.0.0",
		CapacitySlots: 1,
	}

	first, err := store.Register(ctx, params)
	if err != nil {
		t.Fatalf("first registration failed: %v", err)
	}

	// Simulate a crash + restart with a newer agent version.
	params.Version = "1.0.1"
	second, err := store.Register(ctx, params)
	if err != nil {
		t.Fatalf("second registration failed: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("expected same worker_id across re-registration (instance_id=%s), got %s then %s",
			params.InstanceID, first.ID, second.ID)
	}
	if second.Epoch != first.Epoch+1 {
		t.Errorf("expected epoch to increment by exactly 1 on re-registration, got %d -> %d", first.Epoch, second.Epoch)
	}

	workers, err := store.List(ctx, "", 100)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	count := 0
	for _, w := range workers {
		if w.ID == first.ID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one row for instance_id=%s after re-registration, found %d", params.InstanceID, count)
	}
}

