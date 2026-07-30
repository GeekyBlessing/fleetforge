// Package scheduler holds the scheduling brain: the dead-worker reaper
// today, leader election / matching / assignment / retry-due polling as
// Days 4-5 land.
package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
)

// Reaper implements the dead-worker detection sweep from
// docs/05-sequence-diagrams.md 5.3. It is the AUTHORITATIVE mechanism --
// Redis's TTL-based alive key (doc 4.2.1) is only a latency optimization
// layered on top; if Redis is down entirely, or a keyspace notification is
// dropped, this sweep still catches every stale worker within one interval
// because it depends on nothing but Postgres (docs/06-failure-scenarios.md #4).
type Reaper struct {
	workers *postgres.WorkerStore
	cache   *ffredis.WorkerCache
	log     zerolog.Logger

	interval       time.Duration
	timeoutSeconds int
}

func NewReaper(
	workers *postgres.WorkerStore,
	cache *ffredis.WorkerCache,
	log zerolog.Logger,
	interval time.Duration,
	timeoutSeconds int,
) *Reaper {
	return &Reaper{
		workers:        workers,
		cache:          cache,
		log:            log.With().Str("component", "reaper").Logger(),
		interval:       interval,
		timeoutSeconds: timeoutSeconds,
	}
}

// Run blocks until ctx is cancelled, sweeping on a fixed interval as long
// as isLeader() reports true.
//
// Gating on leadership (added Day 4, alongside internal/scheduler/leader.go)
// matters more for correctness-adjacent efficiency than correctness itself
// -- MarkDeadAndRequeue's epoch-guarded UPDATE already makes a second
// concurrent reaper's attempt a harmless no-op (RowsAffected()==0), but
// letting every replica hammer the same query every 5s is still wasted
// work once there's more than one replica.
func (r *Reaper) Run(ctx context.Context, isLeader func() bool) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if isLeader() {
				r.sweep(ctx)
			}
		}
	}
}

func (r *Reaper) sweep(ctx context.Context) {
	stale, err := r.workers.ListStale(ctx, r.timeoutSeconds)
	if err != nil {
		r.log.Error().Err(err).Msg("failed to list stale workers")
		return
	}

	for _, w := range stale {
		if err := r.workers.MarkDeadAndRequeue(ctx, w.ID, w.Epoch); err != nil {
			r.log.Error().Err(err).Str("worker_id", w.ID).Msg("failed to mark worker dead")
			continue
		}
		if err := r.cache.Delete(ctx, w.ID); err != nil {
			// Non-fatal: both cache keys carry their own TTL and would
			// expire on their own (doc 4.2.1) -- log and move on rather
			// than treat this as a sweep failure.
			r.log.Warn().Err(err).Str("worker_id", w.ID).Msg("failed to clear redis cache for dead worker")
		}
		r.log.Info().Str("worker_id", w.ID).Int64("epoch", w.Epoch).Msg("marked worker dead, requeued jobs")
	}
}
