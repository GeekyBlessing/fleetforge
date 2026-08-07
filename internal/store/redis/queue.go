package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/launchverse/fleetforge/internal/queue"
)

// ConsumerGroup is the single shared consumer group all scheduler replicas
// join (docs/04-redis-data-model.md 4.1): only the elected leader
// actively reads from it, but the group itself is created up front so the
// consumer loop doesn't need a migration-like bootstrap step of its own.
const ConsumerGroup = "schedulers"

// StreamQueue implements queue.Backend using Redis Streams: one stream
// per priority level so the highest-priority non-empty stream can be
// checked in O(1) without a client-side priority queue (doc 4.1 rationale).
type StreamQueue struct {
	client *goredis.Client
}

func NewStreamQueue(client *goredis.Client) *StreamQueue {
	return &StreamQueue{client: client}
}

// StreamKey returns the Redis Streams key for a given priority (0=highest,
// 9=lowest). Exported so the scheduler's consumer loop can iterate
// priorities 0..9 without duplicating this naming convention.
func StreamKey(priority int16) string {
	return "queue:jobs:p" + strconv.Itoa(int(priority))
}

// EnsureConsumerGroups creates the "schedulers" consumer group on every
// priority stream if it doesn't already exist. Safe to call on every
// scheduler startup; BUSYGROUP (group already exists) is the expected,
// harmless outcome after the first call.
func (q *StreamQueue) EnsureConsumerGroups(ctx context.Context) error {
	for p := int16(0); p <= 9; p++ {
		err := q.client.XGroupCreateMkStream(ctx, StreamKey(p), ConsumerGroup, "0").Err()
		if err != nil && !isBusyGroupErr(err) {
			return fmt.Errorf("create consumer group for priority %d: %w", p, err)
		}
	}
	return nil
}

func isBusyGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

// Enqueue implements queue.Backend. MAXLEN ~100000 (approximate trimming)
// bounds stream memory: once a job is durably ASSIGNED in Postgres the
// stream entry has done its job (doc 4.1: the stream is a delivery
// mechanism, not a system of record).
func (q *StreamQueue) Enqueue(ctx context.Context, msg queue.JobMessage) error {
	_, err := q.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: StreamKey(msg.Priority),
		MaxLen: 100000,
		Approx: true,
		Values: map[string]interface{}{
			"job_id":                msg.JobID,
			"repository":            msg.Repository,
			"branch":                msg.Branch,
			"commit_sha":            msg.CommitSHA,
			"required_capabilities": strings.Join(msg.RequiredCapabilities, ","),
		},
	}).Result()
	if err != nil {
		return fmt.Errorf("enqueue job %s at priority %d: %w", msg.JobID, msg.Priority, err)
	}
	return nil
}
