package metrics

import (
	"context"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/store/postgres"
)

// Collector periodically refreshes the gauge-style metrics (queue depth by
// priority, worker counts by status) straight from Postgres, the
// authoritative source, same as every other read path in this codebase
// (docs/06-failure-scenarios.md's Postgres-is-the-fallback-of-last-resort
// pattern). Polled on an interval rather than updated at every individual
// state-transition call site, so adding a new gauge never means threading
// a metrics object through internal/store/postgres's write paths: only
// TRUE counters (JobsAssignedTotal, JobsCompletedTotal) are incremented
// inline at their call sites; see metrics.go's comments on each collector
// for which category it is.
//
// Every scheduler replica runs its own Collector (unlike the Reaper/
// RetryPoller/Autoscaler, this is deliberately NOT leader-gated): gauges
// reflect global fleet state, not "what did I just do," so every replica
// computing and exposing the same numbers on its own /metrics endpoint is
// correct and harmless; Prometheus scrapes each replica independently
// anyway.
type Collector struct {
	jobs     *postgres.JobStore
	workers  *postgres.WorkerStore
	log      zerolog.Logger
	interval time.Duration
}

func NewCollector(jobs *postgres.JobStore, workers *postgres.WorkerStore, log zerolog.Logger, interval time.Duration) *Collector {
	return &Collector{
		jobs:     jobs,
		workers:  workers,
		log:      log.With().Str("component", "metrics-collector").Logger(),
		interval: interval,
	}
}

func (c *Collector) Run(ctx context.Context) {
	// Populate immediately on startup rather than leaving gauges at zero
	// (client_golang's default for an unset label combination) for up to a
	// full interval after the scheduler boots.
	c.collect(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect(ctx)
		}
	}
}

func (c *Collector) collect(ctx context.Context) {
	queuedByPriority, err := c.jobs.CountQueuedByPriority(ctx)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to refresh queued-jobs gauge")
	} else {
		// Reset every priority explicitly (not just the ones with a
		// nonzero count) so a priority that just drained to zero doesn't
		// leave its last nonzero value stuck in Prometheus forever:
		// GaugeVec has no "delete stale label combos automatically"
		// behavior, so this loop IS the correctness mechanism for that.
		for p := int16(0); p <= 9; p++ {
			JobsQueued.WithLabelValues(strconv.Itoa(int(p))).Set(float64(queuedByPriority[p]))
		}
	}

	byStatus, err := c.workers.CountByStatus(ctx)
	if err != nil {
		c.log.Error().Err(err).Msg("failed to refresh workers gauge")
		return
	}
	for _, status := range []string{"REGISTERING", "READY", "BUSY", "DRAINING", "OFFLINE", "DEAD"} {
		WorkersByStatus.WithLabelValues(status).Set(float64(byStatus[status]))
	}
}
