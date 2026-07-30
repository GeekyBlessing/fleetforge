// Package queue defines the job-queue contract the scheduler depends on,
// deliberately separate from any specific backend. docs/01-architecture-overview.md
// 1.1 flags Redis Streams as a Day 3 choice we might outgrow at extreme
// scale; this interface is the seam that lets a future NATS JetStream
// implementation be a new file in internal/store/*, not a scheduler-core
// rewrite. Nothing in internal/api or internal/scheduler should import
// internal/store/redis directly for queue access -- only this interface.
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

// Backend is intentionally minimal at Day 3 -- just enough for job
// submission to enqueue. Day 4 extends this interface with the
// consumer-group read/ack/claim methods the scheduler's matching loop
// needs; adding them here now, unused, would just be unverified surface
// area.
type Backend interface {
	Enqueue(ctx context.Context, msg JobMessage) error
}
