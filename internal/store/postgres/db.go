// Package postgres holds all direct SQL access. Nothing outside this
// package should import pgx or write raw SQL -- the scheduler core and API
// layer talk to WorkerStore/JobStore, never to *pgxpool.Pool directly. That
// boundary is what makes the CAS-transaction logic (docs/05, doc 6 #2) easy
// to find and easy to keep correct: there's exactly one place it can live.
package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool opens a connection pool and verifies connectivity before
// returning. Failing fast here (rather than lazily on first query) means a
// misconfigured DATABASE_URL shows up as a startup crash with a clear
// message, not as a mysterious first-request timeout.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	// This is a control-plane service doing short CAS transactions and
	// batched heartbeat flushes, not a general app server fanning out to
	// many slow queries -- a large pool here just moves contention from
	// the application to Postgres's own max_connections. 20 is a
	// deliberately conservative starting point per scheduler replica;
	// revisit with real numbers once the Day 7 load test exists.
	cfg.MaxConns = 20
	cfg.MaxConnLifetime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return pool, nil
}
