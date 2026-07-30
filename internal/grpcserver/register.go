// Package grpcserver implements the worker-facing control plane defined in
// proto/fleetforge/v1/scheduler.proto. This is the only package workers ever
// talk to (docs/01-architecture-overview.md 1.2).
package grpcserver

import (
	"context"
	"fmt"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

// Server implements fleetforgev1.FleetSchedulerServer.
type Server struct {
	fleetforgev1.UnimplementedFleetSchedulerServer

	workers *postgres.WorkerStore
	jobs    *postgres.JobStore
	cache   *ffredis.WorkerCache
	log     zerolog.Logger

	heartbeatIntervalSeconds int32
	heartbeatTimeoutSeconds  int32
}

func NewServer(
	workers *postgres.WorkerStore,
	jobs *postgres.JobStore,
	cache *ffredis.WorkerCache,
	log zerolog.Logger,
	heartbeatIntervalSeconds int32,
	heartbeatTimeoutSeconds int32,
) *Server {
	return &Server{
		workers:                  workers,
		jobs:                     jobs,
		cache:                    cache,
		log:                      log.With().Str("component", "grpcserver").Logger(),
		heartbeatIntervalSeconds: heartbeatIntervalSeconds,
		heartbeatTimeoutSeconds:  heartbeatTimeoutSeconds,
	}
}

// RegisterWorker implements docs/05-sequence-diagrams.md section 5.1.
//
// Known, deliberate gaps at Day 1 (both called out explicitly rather than
// silently skipped, and both tracked against the roadmap):
//   - No mTLS/bootstrap-JWT verification yet -- that's docs/09 section 9.4,
//     scheduled for Day 7. Wiring it in means adding a grpc.UnaryServerInterceptor
//     in cmd/scheduler/main.go; this handler doesn't need to change.
//   - No Redis-backed registration rate limiting yet (docs/04-redis-data-model.md
//     4.4) -- Redis isn't introduced until Day 2's heartbeat work, and Day 1
//     shouldn't take on a dependency it doesn't need yet to prove out
//     registration.
func (s *Server) RegisterWorker(
	ctx context.Context,
	req *fleetforgev1.RegisterWorkerRequest,
) (*fleetforgev1.RegisterWorkerResponse, error) {
	if err := validateRegisterRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid registration request: %v", err)
	}

	registered, err := s.workers.Register(ctx, postgres.RegisterWorkerParams{
		Hostname:      req.GetHostname(),
		InstanceID:    req.GetInstanceId(),
		OS:            req.GetOs(),
		CPUCores:      req.GetCpuCores(),
		MemoryMB:      req.GetMemoryMb(),
		Labels:        req.GetLabels(),
		Capabilities:  req.GetCapabilities(),
		Version:       req.GetVersion(),
		CapacitySlots: req.GetCapacitySlots(),
	})
	if err != nil {
		s.log.Error().
			Err(err).
			Str("instance_id", req.GetInstanceId()).
			Str("hostname", req.GetHostname()).
			Msg("worker registration failed")
		return nil, status.Errorf(codes.Internal, "registration failed")
	}

	s.log.Info().
		Str("worker_id", registered.ID).
		Str("instance_id", req.GetInstanceId()).
		Int64("epoch", registered.Epoch).
		Msg("worker registered")

	// Seed the Redis cache immediately so the worker's first heartbeat
	// hits a warm cache instead of falling back to Postgres for the epoch
	// check -- not required for correctness (the fallback path exists
	// precisely so a cold/missing cache entry is always handled safely),
	// just avoids an unnecessary extra Postgres round trip on every single
	// worker's very first heartbeat.
	seedCapacity := req.GetCapacitySlots()
	if seedCapacity <= 0 {
		seedCapacity = 1 // matches WorkerStore.Register's own default -- see internal/store/postgres/workers.go
	}
	if err := s.cache.SetState(ctx, registered.ID, ffredis.WorkerState{
		Epoch:             registered.Epoch,
		Status:            "READY",
		CurrentJobID:      "",
		AvailableCapacity: seedCapacity,
		LastPGFlushUnix:   0,
	}); err != nil {
		s.log.Warn().Err(err).Str("worker_id", registered.ID).Msg("failed to seed redis cache after registration")
	}

	return &fleetforgev1.RegisterWorkerResponse{
		WorkerId:                 registered.ID,
		Epoch:                    registered.Epoch,
		HeartbeatIntervalSeconds: s.heartbeatIntervalSeconds,
		HeartbeatTimeoutSeconds:  s.heartbeatTimeoutSeconds,
	}, nil
}

func validateRegisterRequest(req *fleetforgev1.RegisterWorkerRequest) error {
	switch {
	case req.GetHostname() == "":
		return fmt.Errorf("hostname is required")
	case req.GetInstanceId() == "":
		return fmt.Errorf("instance_id is required")
	case req.GetOs() == "":
		return fmt.Errorf("os is required")
	case req.GetCpuCores() <= 0:
		return fmt.Errorf("cpu_cores must be a positive integer")
	case req.GetMemoryMb() <= 0:
		return fmt.Errorf("memory_mb must be a positive integer")
	case req.GetVersion() == "":
		return fmt.Errorf("version is required")
	}
	return nil
}
