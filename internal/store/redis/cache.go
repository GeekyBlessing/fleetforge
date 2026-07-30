package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// WorkerCache implements the hot-path cache + liveness key from
// docs/04-redis-data-model.md sections 4.2 and 4.2.1. It is a performance
// layer, never a source of truth: every value here is reconstructable from
// Postgres, and every caller is expected to fall back to Postgres on a
// cache miss (see internal/grpcserver/heartbeat.go) rather than treat an
// empty result as "worker doesn't exist."
type WorkerCache struct {
	client *redis.Client
}

func NewWorkerCache(client *redis.Client) *WorkerCache {
	return &WorkerCache{client: client}
}

// WorkerState mirrors the worker:{id}:state hash fields from doc 4.2, plus
// last_pg_flush_unix, which tracks the batched-write coalescing described
// in docs/05-sequence-diagrams.md 5.2 (avoids reintroducing a 2,000
// writes/sec sustained load on Postgres from bare heartbeats).
type WorkerState struct {
	Epoch             int64
	Status            string // matches the Postgres worker_status enum values (READY, BUSY, ...)
	CurrentJobID       string // "" means no current job
	AvailableCapacity  int32
	LastPGFlushUnix    int64
	// DrainRequested is read-only from SetState's perspective -- it is
	// populated by GetState but deliberately NOT one of the fields SetState
	// writes (see SetState's comment). Only SetDrainRequested ever writes
	// this hash field.
	DrainRequested bool
}

func stateKey(workerID string) string { return "worker:" + workerID + ":state" }
func aliveKey(workerID string) string { return "worker:" + workerID + ":alive" }

// SetState deliberately does NOT include drain_requested in the fields it
// writes. It's called on every heartbeat and every assignment/free
// transition, each of which only knows about the worker's operational
// status -- if it also wrote drain_requested (defaulting a zero-value
// WorkerState{} to false), a plain heartbeat from a worker that an operator
// had just told to drain would silently clear that flag back to false on
// its very next heartbeat, undoing the drain request. Keeping
// drain_requested as a field only SetDrainRequested ever touches avoids
// that whole class of read-modify-write race entirely: two independent
// writers, two disjoint sets of hash fields, no coordination needed.
func (c *WorkerCache) SetState(ctx context.Context, workerID string, s WorkerState) error {
	err := c.client.HSet(ctx, stateKey(workerID), map[string]interface{}{
		"epoch":              s.Epoch,
		"status":             s.Status,
		"current_job_id":     s.CurrentJobID,
		"available_capacity": s.AvailableCapacity,
		"last_pg_flush_unix": s.LastPGFlushUnix,
	}).Err()
	if err != nil {
		return fmt.Errorf("hset worker state %s: %w", workerID, err)
	}
	return nil
}

// SetDrainRequested is the only writer of the drain_requested hash field --
// see SetState's comment for why that separation matters. Called from
// internal/api/handlers_workers.go's drain/resume endpoints right after the
// corresponding Postgres update.
func (c *WorkerCache) SetDrainRequested(ctx context.Context, workerID string, drainRequested bool) error {
	if err := c.client.HSet(ctx, stateKey(workerID), "drain_requested", drainRequested).Err(); err != nil {
		return fmt.Errorf("hset drain_requested for worker %s: %w", workerID, err)
	}
	return nil
}

// GetState returns (state, found, err). found=false means a cache miss --
// callers must fall back to Postgres, not assume the worker is gone.
func (c *WorkerCache) GetState(ctx context.Context, workerID string) (WorkerState, bool, error) {
	res, err := c.client.HGetAll(ctx, stateKey(workerID)).Result()
	if err != nil {
		return WorkerState{}, false, fmt.Errorf("hgetall worker state %s: %w", workerID, err)
	}
	if len(res) == 0 {
		return WorkerState{}, false, nil
	}

	epoch, _ := strconv.ParseInt(res["epoch"], 10, 64)
	capVal, _ := strconv.ParseInt(res["available_capacity"], 10, 32)
	lastFlush, _ := strconv.ParseInt(res["last_pg_flush_unix"], 10, 64)
	drainRequested, _ := strconv.ParseBool(res["drain_requested"]) // "" (unset) parses as false, which is the correct default

	return WorkerState{
		Epoch:             epoch,
		Status:            res["status"],
		CurrentJobID:      res["current_job_id"],
		AvailableCapacity: int32(capVal),
		LastPGFlushUnix:   lastFlush,
		DrainRequested:    drainRequested,
	}, true, nil
}

// SetAlive refreshes the TTL-based liveness key doc 4.2.1 describes. This
// is the fast-path signal only -- the reaper's Postgres sweep
// (internal/scheduler/reaper.go) is authoritative and doesn't depend on
// this key existing or expiring correctly.
func (c *WorkerCache) SetAlive(ctx context.Context, workerID string, ttl time.Duration) error {
	if err := c.client.Set(ctx, aliveKey(workerID), "1", ttl).Err(); err != nil {
		return fmt.Errorf("set alive key %s: %w", workerID, err)
	}
	return nil
}

// Delete clears both keys for a worker once the reaper has marked it DEAD
// in Postgres. Not strictly required (both keys carry their own TTL and
// would eventually expire on their own) but avoids a stale-looking READY
// worker briefly appearing in the scheduler's candidate cache between
// being marked dead and its TTL lapsing.
func (c *WorkerCache) Delete(ctx context.Context, workerID string) error {
	if err := c.client.Del(ctx, stateKey(workerID), aliveKey(workerID)).Err(); err != nil {
		return fmt.Errorf("delete worker cache entries %s: %w", workerID, err)
	}
	return nil
}
