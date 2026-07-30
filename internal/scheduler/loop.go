package scheduler

import (
	"context"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
)

const numPriorities = 10

// Loop is the scheduler's matching/assignment brain --
// docs/05-sequence-diagrams.md 5.4. It reads job messages from the Redis
// Streams queue (highest priority first, by stream key order), filters
// candidate workers by capability, ranks survivors by least-loaded, and
// commits the assignment via WorkerStore.AssignJob's compare-and-swap
// transaction.
type Loop struct {
	workers      *postgres.WorkerStore
	jobs         *postgres.JobStore
	cache        *ffredis.WorkerCache
	redis        *goredis.Client
	log          zerolog.Logger
	consumerName string
	isLeader     func() bool
}

func NewLoop(
	workers *postgres.WorkerStore,
	jobs *postgres.JobStore,
	cache *ffredis.WorkerCache,
	redisClient *goredis.Client,
	log zerolog.Logger,
	consumerName string,
	isLeader func() bool,
) *Loop {
	return &Loop{
		workers:      workers,
		jobs:         jobs,
		cache:        cache,
		redis:        redisClient,
		log:          log.With().Str("component", "scheduler-loop").Logger(),
		consumerName: consumerName,
		isLeader:     isLeader,
	}
}

// Run blocks until ctx is cancelled. When not the leader, it just waits --
// only the elected leader consumes from the shared consumer group (doc
// 1.3: standby replicas still serve REST/gRPC traffic, they just don't
// schedule).
func (l *Loop) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !l.isLeader() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}
			continue
		}
		l.tick(ctx)
	}
}

func streamKeys() []string {
	keys := make([]string, numPriorities)
	for p := 0; p < numPriorities; p++ {
		keys[p] = ffredis.StreamKey(int16(p))
	}
	return keys
}

func (l *Loop) tick(ctx context.Context) {
	keys := streamKeys()

	// Pass 1: brand-new entries. Blocking briefly here is what keeps this
	// loop from busy-spinning when the queue is empty.
	l.readAndProcess(ctx, keys, ">", 1*time.Second)

	// Pass 2: this consumer's own previously-unmatched pending entries --
	// the practical stand-in for doc 4.1's XCLAIM-based PEL recovery. A job
	// that had no capable/available worker on a prior tick is left
	// unacknowledged (still in the PEL); reading with ID "0" returns this
	// consumer's own pending entries so they get retried against a fresh
	// candidate list, without needing a second consumer identity or a
	// separate idle-time-based XCLAIM sweep at this fleet size.
	l.readAndProcess(ctx, keys, "0", 0)
}

func (l *Loop) readAndProcess(ctx context.Context, keys []string, id string, block time.Duration) {
	ids := make([]string, len(keys))
	for i := range ids {
		ids[i] = id
	}

	streamsAndIDs := make([]string, 0, len(keys)*2)
	streamsAndIDs = append(streamsAndIDs, keys...)
	streamsAndIDs = append(streamsAndIDs, ids...)

	streams, err := l.redis.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    ffredis.ConsumerGroup,
		Consumer: l.consumerName,
		Streams:  streamsAndIDs,
		Count:    10,
		Block:    block,
	}).Result()
	if err != nil {
		if err != goredis.Nil {
			l.log.Error().Err(err).Msg("xreadgroup failed")
		}
		return
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			l.processMessage(ctx, stream.Stream, msg)
		}
	}
}

func (l *Loop) processMessage(ctx context.Context, streamKey string, msg goredis.XMessage) {
	jobID, _ := msg.Values["job_id"].(string)
	if jobID == "" {
		l.log.Warn().Str("stream", streamKey).Str("msg_id", msg.ID).Msg("malformed queue entry missing job_id, acking and dropping")
		l.ack(ctx, streamKey, msg.ID)
		return
	}

	requiredCapsRaw, _ := msg.Values["required_capabilities"].(string)
	var requiredCaps []string
	if requiredCapsRaw != "" {
		requiredCaps = strings.Split(requiredCapsRaw, ",")
	}

	job, err := l.jobs.Get(ctx, jobID)
	if err != nil {
		l.log.Warn().Err(err).Str("job_id", jobID).Msg("job not found (likely cancelled), acking and dropping from queue")
		l.ack(ctx, streamKey, msg.ID)
		return
	}
	if job.Status != "QUEUED" {
		// Already assigned/completed/cancelled via some other path since
		// this entry was queued -- nothing left to do with it.
		l.ack(ctx, streamKey, msg.ID)
		return
	}

	candidates, err := l.workers.ListReadyCandidates(ctx, requiredCaps)
	if err != nil {
		l.log.Error().Err(err).Str("job_id", jobID).Msg("failed to list candidate workers")
		return // leave unacked -- retried next tick's pending-entries pass
	}

	chosen := SelectLeastLoaded(candidates)
	if chosen == nil {
		// No capable/available worker right now. Left unacked deliberately
		// -- this is exactly the unmet-demand signal Day 6's autoscaler
		// watches for (docs/09-design-rationale.md 9.2), and the
		// pending-entries pass retries it every tick without needing a
		// separate backlog data structure.
		return
	}

	assigned, err := l.workers.AssignJob(ctx, job.ID, job.CreatedAt, chosen.ID, chosen.Epoch)
	if err != nil {
		l.log.Error().Err(err).Str("job_id", jobID).Str("worker_id", chosen.ID).Msg("assignment transaction failed")
		return
	}
	if !assigned {
		l.log.Debug().Str("job_id", jobID).Str("worker_id", chosen.ID).Msg("lost assignment race, will retry")
		return
	}

	// AssignJob writes straight to Postgres and never touches Redis --
	// without this, the cache's "status" field would keep showing READY
	// until the worker's OWN next heartbeat overwrites it, and
	// internal/grpcserver/heartbeat.go's assignment-push check (which reads
	// the CACHE, for speed, not Postgres) would never see BUSY+job_id in
	// time to hand the assignment over. This is what makes the push
	// actually fire on this worker's very next heartbeat instead of one
	// heartbeat late.
	if err := l.cache.SetState(ctx, chosen.ID, ffredis.WorkerState{
		Epoch:             chosen.Epoch,
		Status:            "BUSY",
		CurrentJobID:      job.ID,
		AvailableCapacity: chosen.AvailableCapacity - 1,
		LastPGFlushUnix:   0, // force an immediate flush on the worker's next heartbeat rather than waiting out the coalescing window
	}); err != nil {
		l.log.Warn().Err(err).Str("worker_id", chosen.ID).Msg("failed to update redis cache after assignment")
	}

	l.log.Info().Str("job_id", jobID).Str("worker_id", chosen.ID).Msg("assigned job to worker")
	l.ack(ctx, streamKey, msg.ID)
}

func (l *Loop) ack(ctx context.Context, streamKey, msgID string) {
	if err := l.redis.XAck(ctx, streamKey, ffredis.ConsumerGroup, msgID).Err(); err != nil {
		l.log.Warn().Err(err).Str("stream", streamKey).Str("msg_id", msgID).Msg("failed to ack stream entry")
	}
}
