// Package queue defines the job-queue contract the scheduler depends on,
// deliberately separate from any specific backend. docs/01-architecture-overview.md
// 1.1 flags Redis Streams as a choice we might outgrow at extreme scale;
// this interface is the seam that lets a future NATS JetStream
// implementation be a new file in internal/store/*, not a scheduler-core
// rewrite. Nothing in internal/api should import internal/store/redis
// directly for queue access -- only this interface.
package queue

import "context"

// JobMessage is the (small, non-authoritative) payload carried on the
// queue -- Postgres remains the source of truth for full job state
// (docs/04-redis-data-model.md 4.1); this is just enough for the scheduler
// to find and evaluate a candidate without a round trip per queue entry.
type JobMessage struct {
	JobID                string
	Priority             int16
	Repository           string
	Branch               string
	CommitSHA            string
	RequiredCapabilities []string
}

// Backend is intentionally minimal -- just enough for job submission to
// enqueue. The scheduler's matching loop (internal/scheduler/loop.go)
// reads/acks directly against the Redis client for its consumer-group
// semantics rather than through this interface, since those operations are
// specific to Streams and don't generalize the way plain enqueue does;
// widening this interface to cover them is a contained change if a second
// backend ever needs it.
type Backend interface {
	Enqueue(ctx context.Context, msg JobMessage) error
}
