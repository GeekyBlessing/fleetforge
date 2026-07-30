// Package redis wraps the Redis-backed pieces described in
// docs/04-redis-data-model.md: the hot-path worker-state cache, the
// liveness/alive keys, and (Day 3) the job queue. Nothing outside this
// package should call the go-redis client directly, for the same reason
// internal/store/postgres owns all SQL -- one place to keep the key
// naming/TTL contracts honest.
package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewClient connects and verifies reachability up front -- same rationale
// as postgres.NewPool: fail at startup with a clear message, not on the
// first heartbeat.
func NewClient(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis at %s: %w", addr, err)
	}
	return client, nil
}
