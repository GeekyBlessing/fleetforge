package grpcserver

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

// pgFlushCoalesceInterval bounds how often a single worker's heartbeat gets
// written to Postgres absent a status change -- docs/05-sequence-diagrams.md
// 5.2. At 10k workers heartbeating every 5s, writing every single one
// straight to Postgres is ~2,000 writes/sec sustained for no benefit;
// Redis absorbs the real-time signal and this coalescing window bounds the
// durability lag to something a dashboard or auditor would find acceptable.
const pgFlushCoalesceInterval = 15 * time.Second

// Heartbeat implements the bidirectional stream from
// docs/05-sequence-diagrams.md 5.2. As of Day 4 it also pushes pending job
// assignments (see handleHeartbeat). As of Day 5, HeartbeatResponse.DrainRequested
// reflects the worker's drain_requested flag (internal/api/handlers_workers.go's
// drain/resume endpoints), so the worker-agent itself can stop reporting
// READY once notified.
func (s *Server) Heartbeat(stream fleetforgev1.FleetScheduler_HeartbeatServer) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := s.handleHeartbeat(ctx, req)
		if err != nil {
			// Ending the stream here is deliberate, not just "an error
			// occurred": a stale-epoch heartbeat means this worker
			// identity has been superseded (doc 6 #6), and the only safe
			// response is to force it to re-register from scratch rather
			// than let it keep streaming under an invalidated epoch.
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *Server) handleHeartbeat(ctx context.Context, req *fleetforgev1.HeartbeatRequest) (*fleetforgev1.HeartbeatResponse, error) {
	workerID := req.GetWorkerId()
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	cached, found, err := s.cache.GetState(ctx, workerID)
	if err != nil {
		s.log.Warn().Err(err).Str("worker_id", workerID).Msg("redis cache read failed, falling back to postgres")
	}

	var knownEpoch int64
	var drainRequested bool
	if found {
		knownEpoch = cached.Epoch
		drainRequested = cached.DrainRequested
	} else {
		// Cache miss -- cold cache after a restart, a briefly-unreachable
		// Redis, OR (the common real case) the reaper just marked this
		// worker DEAD, which deletes its cache entries as part of the same
		// operation (internal/scheduler/reaper.go). Falling back to
		// Postgres has to check BOTH epoch and status here -- checking
		// epoch alone was a real bug: a worker marked DEAD keeps its old
		// epoch, so an epoch-only check let a reaped worker's heartbeats
		// keep being silently accepted and resurrect it back to READY,
		// defeating the entire point of marking it dead (doc 6 #4).
		epoch, dbStatus, dbDrainRequested, err := s.workers.GetEpochAndStatus(ctx, workerID)
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "unknown worker_id %s, must re-register", workerID)
		}
		if dbStatus == "DEAD" || dbStatus == "OFFLINE" {
			return nil, status.Errorf(codes.NotFound, "worker_id %s is %s, must re-register", workerID, dbStatus)
		}
		knownEpoch = epoch
		drainRequested = dbDrainRequested
	}

	if req.GetEpoch() != knownEpoch {
		return nil, status.Errorf(codes.Aborted,
			"stale epoch %d (current epoch is %d), re-register required", req.GetEpoch(), knownEpoch)
	}

	newStatus := workerStatusToDB(req.GetStatus())
	currentJobID := req.GetCurrentJobId()

	// Assignment push (Day 4): the assignment transaction
	// (WorkerStore.AssignJob) already flipped this worker to BUSY with a
	// current_job_id in Postgres/cache -- if the WORKER doesn't know about
	// it yet (it's still reporting READY with no current_job_id), this is
	// the moment to hand it over. Piggybacking on the existing heartbeat
	// ack means no separate push channel/connection is needed: the
	// scheduler never initiates contact with a worker, it only ever
	// responds to one (docs/05-sequence-diagrams.md 5.4).
	var assignment *fleetforgev1.JobAssignment
	if found && cached.Status == "BUSY" && cached.CurrentJobID != "" && currentJobID == "" {
		if job, err := s.jobs.Get(ctx, cached.CurrentJobID); err == nil {
			// Real bug, caught by actually running this: this used to
			// construct and push the assignment unconditionally, trusting
			// the cache's CurrentJobID without checking the job's actual
			// Postgres status. If the cache ever went stale for ANY reason
			// (and report_result.go discarding a report as stale used to be
			// exactly such a reason -- it returned early without ever
			// correcting the worker's cache entry), this would happily
			// re-push an already-SUCCESS/FAILED/RETRYING job forever: the
			// worker accepts it (it looks idle), "executes" it again,
			// reports back, gets discarded again (job.status is no longer
			// ASSIGNED/RUNNING so Complete/RetryOrFail's guard can't match),
			// and the cache is STILL never corrected -- an infinite loop
			// observed live with job 8d66b486 (docs/06-failure-scenarios.md
			// should get a #11 entry for this). The fix is this one status
			// check: only a job still genuinely ASSIGNED is a legitimate
			// pending hand-off. If it isn't, DON'T push it, and DON'T early
			// return either -- falling through to this function's normal
			// unconditional cache write below (which reflects the WORKER'S
			// own honestly-reported READY/no-job state) is what actually
			// self-heals the stale cache entry, since nothing else ever
			// will once report_result.go has already moved on.
			if job.Status == "ASSIGNED" {
				assignment = &fleetforgev1.JobAssignment{
					JobId:           job.ID,
					AssignmentEpoch: knownEpoch,
					Repository:      job.Repository,
					Branch:          job.Branch,
					CommitSha:       job.CommitSHA,
				}
			} else {
				s.log.Warn().
					Str("worker_id", workerID).
					Str("job_id", cached.CurrentJobID).
					Str("job_status", job.Status).
					Msg("cache had a stale pending assignment for a job that's no longer ASSIGNED; not pushing, letting this heartbeat's normal cache write self-heal it")
			}
		} else {
			s.log.Warn().Err(err).Str("job_id", cached.CurrentJobID).Msg("failed to load job details for pending assignment push")
		}
	}

	// Once the worker itself confirms it's BUSY on a specific job, that
	// job moves ASSIGNED -> RUNNING (doc 5.4). knownEpoch doubles as the
	// job's assignment_epoch here because AssignJob sets
	// jobs.assignment_epoch to exactly the worker's epoch at assignment
	// time -- a worker that has since re-registered (and so holds a
	// different epoch) could never match a stale assignment this way.
	if newStatus == "BUSY" && currentJobID != "" {
		if _, err := s.jobs.MarkRunning(ctx, currentJobID, knownEpoch); err != nil {
			s.log.Warn().Err(err).Str("job_id", currentJobID).Msg("failed to mark job running")
		}
	}

	statusChanged := found && newStatus != cached.Status
	dueForFlush := !found || time.Now().Unix()-cached.LastPGFlushUnix >= int64(pgFlushCoalesceInterval.Seconds())

	lastFlush := cached.LastPGFlushUnix
	if statusChanged || dueForFlush {
		var jobIDPtr *string
		if currentJobID != "" {
			jobIDPtr = &currentJobID
		}
		if _, err := s.workers.UpdateHeartbeat(ctx, workerID, knownEpoch, newStatus, jobIDPtr); err != nil {
			s.log.Error().Err(err).Str("worker_id", workerID).Msg("failed to flush heartbeat to postgres")
			// Not fatal to the stream -- Redis still reflects the latest
			// state, and the next coalescing window (or next status
			// change) will retry the flush. Durability lags; liveness
			// tracking does not.
		} else {
			lastFlush = time.Now().Unix()
		}
	}

	if err := s.cache.SetState(ctx, workerID, ffredis.WorkerState{
		Epoch:             knownEpoch,
		Status:            newStatus,
		CurrentJobID:      currentJobID,
		AvailableCapacity: req.GetAvailableCapacity(),
		LastPGFlushUnix:   lastFlush,
	}); err != nil {
		s.log.Warn().Err(err).Str("worker_id", workerID).Msg("failed to update redis cache")
	}

	if err := s.cache.SetAlive(ctx, workerID, time.Duration(s.heartbeatTimeoutSeconds)*time.Second); err != nil {
		s.log.Warn().Err(err).Str("worker_id", workerID).Msg("failed to refresh alive key")
	}

	return &fleetforgev1.HeartbeatResponse{
		Acknowledged:   true,
		Assignment:     assignment,
		DrainRequested: drainRequested,
	}, nil
}

// workerStatusToDB maps the proto enum to the Postgres worker_status enum
// values -- kept as one small explicit function rather than string(enum)
// or reflection, so a proto enum rename can't silently desync from the
// database's set of valid values without a compile error here.
func workerStatusToDB(ws fleetforgev1.WorkerStatus) string {
	switch ws {
	case fleetforgev1.WorkerStatus_WORKER_STATUS_REGISTERING:
		return "REGISTERING"
	case fleetforgev1.WorkerStatus_WORKER_STATUS_READY:
		return "READY"
	case fleetforgev1.WorkerStatus_WORKER_STATUS_BUSY:
		return "BUSY"
	case fleetforgev1.WorkerStatus_WORKER_STATUS_DRAINING:
		return "DRAINING"
	case fleetforgev1.WorkerStatus_WORKER_STATUS_OFFLINE:
		return "OFFLINE"
	case fleetforgev1.WorkerStatus_WORKER_STATUS_DEAD:
		return "DEAD"
	default:
		return "REGISTERING"
	}
}
