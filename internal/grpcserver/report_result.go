package grpcserver

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/launchverse/fleetforge/internal/metrics"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

// ReportJobResult implements docs/05-sequence-diagrams.md 5.4's completion
// path. It is epoch-guarded exactly like everything else in this file
// (docs/06-failure-scenarios.md #10, "lost/late/duplicate acknowledgement"):
// a report carrying an assignment_epoch that no longer matches the job's
// current one is discarded, not applied, because the job has already moved
// on (reassigned to another worker after this one was presumed dead, most
// likely).
func (s *Server) ReportJobResult(
	ctx context.Context,
	req *fleetforgev1.ReportJobResultRequest,
) (*fleetforgev1.ReportJobResultResponse, error) {
	jobID := req.GetJobId()
	if jobID == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	job, err := s.jobs.Get(ctx, jobID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "job %s not found", jobID)
	}

	reportedStatus := jobStatusToDB(req.GetStatus())

	// FAILED reports get a retry-or-fail decision (docs/09-design-rationale.md
	// 9.3): a failure with retry budget left parks the job in RETRYING for
	// internal/scheduler's RetryPoller to pick back up once its backoff
	// elapses, rather than treating every failure as terminal the way
	// SUCCESS/CANCELLED always are.
	var completed bool
	var finalStatus string
	if reportedStatus == "FAILED" {
		newStatus, ok, err := s.jobs.RetryOrFail(ctx, job.ID, job.CreatedAt, req.GetAssignmentEpoch(), job.WorkerID, req.GetErrorMessage(), req.GetLogRef())
		if err != nil {
			s.log.Error().Err(err).Str("job_id", jobID).Msg("failed to record retry/fail decision")
			return nil, status.Errorf(codes.Internal, "failed to record job result")
		}
		completed = ok
		finalStatus = newStatus
	} else {
		ok, err := s.jobs.Complete(ctx, job.ID, job.CreatedAt, req.GetAssignmentEpoch(), reportedStatus, req.GetLogRef())
		if err != nil {
			s.log.Error().Err(err).Str("job_id", jobID).Msg("failed to complete job")
			return nil, status.Errorf(codes.Internal, "failed to record job result")
		}
		completed = ok
		finalStatus = reportedStatus
	}

	if !completed {
		// Stale epoch, or the job was already completed/reassigned by the
		// time this report arrived -- exactly doc 6 #10's discard case.
		s.log.Warn().
			Str("job_id", jobID).
			Int64("reported_assignment_epoch", req.GetAssignmentEpoch()).
			Msg("discarding stale/duplicate job result")
		return &fleetforgev1.ReportJobResultResponse{Acknowledged: true, DiscardedStaleEpoch: true}, nil
	}

	if job.WorkerID != nil {
		ok, availableCapacity, newWorkerStatus, err := s.workers.FreeWorker(ctx, *job.WorkerID, req.GetAssignmentEpoch())
		if err != nil {
			s.log.Error().Err(err).Str("worker_id", *job.WorkerID).Msg("failed to free worker after job completion")
			// Not fatal to the RPC -- the job result is already durably
			// recorded above; a worker stuck showing BUSY with no
			// corresponding running job is a (rare, logged) inconsistency,
			// not a reason to tell the worker its result wasn't received.
		} else if ok {
			// Root-cause fix for the reassignment-loop bug: FreeWorker only
			// updates Postgres. Without also correcting the Redis cache
			// here, the cache keeps showing status=BUSY with THIS job's ID
			// forever, and heartbeat.go's assignment-push logic
			// (cached.Status=="BUSY" && cached.CurrentJobID!="" &&
			// currentJobID=="") would keep re-pushing this same, already-
			// terminal job to the worker on every future heartbeat.
			// newWorkerStatus is either READY or DRAINING -- see
			// WorkerStore.FreeWorker's comment on why a draining worker
			// must not come back as READY just because its job finished.
			if err := s.cache.SetState(ctx, *job.WorkerID, ffredis.WorkerState{
				Epoch:             req.GetAssignmentEpoch(),
				Status:            newWorkerStatus,
				CurrentJobID:      "",
				AvailableCapacity: availableCapacity,
				LastPGFlushUnix:   time.Now().Unix(),
			}); err != nil {
				s.log.Warn().Err(err).Str("worker_id", *job.WorkerID).Msg("failed to update redis cache after freeing worker")
			}
		}
	}

	metrics.JobsCompletedTotal.WithLabelValues(finalStatus).Inc()
	// Duration is only meaningful for a job's FINAL terminal transition --
	// a RETRYING job isn't done yet (it's going to run again), so recording
	// "duration" here would conflate one attempt's runtime with the job's
	// eventual total lifetime. job.StartedAt is nil if the job somehow never
	// reached RUNNING (shouldn't happen given the guards above, but a nil
	// check costs nothing).
	if finalStatus != "RETRYING" && job.StartedAt != nil {
		metrics.JobDurationSeconds.Observe(time.Since(*job.StartedAt).Seconds())
	}

	s.log.Info().
		Str("job_id", jobID).
		Str("status", finalStatus).
		Int32("exit_code", req.GetExitCode()).
		Msg("job result recorded")

	return &fleetforgev1.ReportJobResultResponse{Acknowledged: true}, nil
}

func jobStatusToDB(js fleetforgev1.JobStatus) string {
	switch js {
	case fleetforgev1.JobStatus_JOB_STATUS_SUCCESS:
		return "SUCCESS"
	case fleetforgev1.JobStatus_JOB_STATUS_FAILED:
		return "FAILED"
	case fleetforgev1.JobStatus_JOB_STATUS_CANCELLED:
		return "CANCELLED"
	default:
		// Day 4 scope: only terminal outcomes are reported by
		// ReportJobResult; RETRYING is a scheduler-driven transition
		// (Day 5), not something the worker itself declares.
		return "FAILED"
	}
}
