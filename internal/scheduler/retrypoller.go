package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/queue"
	"github.com/launchverse/fleetforge/internal/store/postgres"
)

// RetryPoller implements the other half of docs/09-design-rationale.md 9.3's
// backoff path. internal/grpcserver/report_result.go's RetryOrFail parks a
// job in RETRYING with a future retry_at rather than re-enqueuing it
// immediately: it deliberately does not re-enqueue inline in the same gRPC call,
// since a burst of failing jobs all retrying at once would otherwise
// stampede the exact worker pool that just failed them. This poller is what
// actually moves a RETRYING job back to QUEUED and onto the Redis stream
// once its backoff window has elapsed, on its own schedule, decoupled from
// any single ReportJobResult call.
//
// Leader-gated for the same reason as the Reaper: only one replica should
// be doing this at a time, even though the underlying UPDATE (guarded on
// status='RETRYING') makes a second concurrent poller's attempt a harmless
// no-op.
type RetryPoller struct {
	jobs     *postgres.JobStore
	queue    queue.Backend
	log      zerolog.Logger
	interval time.Duration
}

func NewRetryPoller(jobs *postgres.JobStore, q queue.Backend, log zerolog.Logger, interval time.Duration) *RetryPoller {
	return &RetryPoller{
		jobs:     jobs,
		queue:    q,
		log:      log.With().Str("component", "retry-poller").Logger(),
		interval: interval,
	}
}

func (p *RetryPoller) Run(ctx context.Context, isLeader func() bool) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if isLeader() {
				p.poll(ctx)
			}
		}
	}
}

func (p *RetryPoller) poll(ctx context.Context) {
	due, err := p.jobs.ListDueRetries(ctx, 100)
	if err != nil {
		p.log.Error().Err(err).Msg("failed to list due retries")
		return
	}

	for _, j := range due {
		ok, err := p.jobs.MarkQueuedFromRetry(ctx, j.ID, j.CreatedAt)
		if err != nil {
			p.log.Error().Err(err).Str("job_id", j.ID).Msg("failed to mark job queued from retry")
			continue
		}
		if !ok {
			// Already claimed by a concurrent pass, or moved on some other
			// way (e.g. cancelled) between the read above and this write:
			// not an error, just nothing left to do for this job.
			continue
		}

		if err := p.queue.Enqueue(ctx, queue.JobMessage{
			JobID:                j.ID,
			Priority:             j.Priority,
			Repository:           j.Repository,
			Branch:               j.Branch,
			CommitSHA:            j.CommitSHA,
			RequiredCapabilities: j.RequiredCapabilities,
		}); err != nil {
			// Not fatal: the job is QUEUED in Postgres either way
			// (docs/06-failure-scenarios.md #4's Redis-down tolerance
			// applies here too); logged so a stuck enqueue is visible.
			p.log.Error().Err(err).Str("job_id", j.ID).Msg("failed to enqueue retried job to redis stream")
			continue
		}

		p.log.Info().Str("job_id", j.ID).Msg("retry backoff elapsed, requeued job")
	}
}
