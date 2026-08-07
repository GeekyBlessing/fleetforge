package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// leaderLockKey is an arbitrary but fixed advisory-lock key shared by every
// scheduler replica: whoever holds pg_advisory_lock(leaderLockKey) is the
// leader. Picked once, never reused for anything else in this codebase.
const leaderLockKey = 727272001

// LeaderElector implements docs/01-architecture-overview.md 1.1/1.3's
// choice of Postgres advisory locks over Redis Redlock for leader
// election: correctness rides on Postgres, which is already the
// consistency-critical dependency everywhere else in this system.
//
// The mechanism: acquire a SESSION-scoped advisory lock on one dedicated
// connection checked out from the pool and never returned. Postgres
// releases a session-scoped advisory lock automatically the instant that
// connection closes, whether from a crash, network partition, or graceful
// shutdown; it doesn't matter which. So "the leader died" and "the lock
// becomes acquirable by someone else" are the same event, with no separate
// heartbeat/lease-renewal protocol needed (docs/06-failure-scenarios.md #1, #12).
type LeaderElector struct {
	pool *pgxpool.Pool
	log  zerolog.Logger

	mu       sync.Mutex
	conn     *pgxpool.Conn
	isLeader bool
}

func NewLeaderElector(pool *pgxpool.Pool, log zerolog.Logger) *LeaderElector {
	return &LeaderElector{pool: pool, log: log.With().Str("component", "leader-elector").Logger()}
}

func (le *LeaderElector) IsLeader() bool {
	le.mu.Lock()
	defer le.mu.Unlock()
	return le.isLeader
}

// Run blocks until ctx is cancelled, attempting to acquire leadership (or,
// once held, re-verifying the dedicated connection is still alive) every
// checkInterval.
func (le *LeaderElector) Run(ctx context.Context, checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	le.tryAcquireOrVerify(ctx)
	for {
		select {
		case <-ctx.Done():
			le.release()
			return
		case <-ticker.C:
			le.tryAcquireOrVerify(ctx)
		}
	}
}

func (le *LeaderElector) tryAcquireOrVerify(ctx context.Context) {
	le.mu.Lock()
	defer le.mu.Unlock()

	if le.conn != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := le.conn.Ping(pingCtx)
		cancel()
		if err == nil {
			return // still holding leadership, nothing to do
		}
		le.log.Warn().Err(err).Msg("lost connection holding leader lock, releasing leadership")
		le.conn.Release()
		le.conn = nil
		le.isLeader = false
	}

	conn, err := le.pool.Acquire(ctx)
	if err != nil {
		le.log.Error().Err(err).Msg("failed to acquire dedicated connection for leader election")
		return
	}

	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, leaderLockKey).Scan(&acquired); err != nil {
		le.log.Error().Err(err).Msg("failed to run pg_try_advisory_lock")
		conn.Release()
		return
	}

	if !acquired {
		conn.Release()
		return // another replica already holds it
	}

	le.conn = conn
	le.isLeader = true
	le.log.Info().Msg("acquired scheduler leadership")
}

func (le *LeaderElector) release() {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.conn == nil {
		return
	}
	_, _ = le.conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, leaderLockKey)
	le.conn.Release()
	le.conn = nil
	le.isLeader = false
}
