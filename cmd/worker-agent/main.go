// cmd/worker-agent: registers once, then holds a long-lived bidirectional
// heartbeat stream open for the life of the process
// (docs/05-sequence-diagrams.md 5.2). Receiving a job assignment on that
// stream triggers real execution (worker-agent-runtime) and a
// ReportJobResult call back to the scheduler. A DrainRequested=true on that
// same stream (see agentState.setDraining) makes this agent finish its
// current job (if any) and then report itself
// as DRAINING instead of READY -- it keeps heartbeating (so an operator can
// still see it and its eventual OFFLINE/DEAD transition), it just never
// accepts new work again. There is no self-initiated process exit here;
// deciding when it's safe to actually kill the process is left to whatever
// is supervising it (systemd, a process manager, or a human), once it
// observes this worker idle and DRAINING.
package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	agentruntime "github.com/launchverse/fleetforge/worker-agent-runtime"

	"github.com/launchverse/fleetforge/internal/auth"
	"github.com/launchverse/fleetforge/internal/config"
	fleetforgev1 "github.com/launchverse/fleetforge/proto/fleetforge/v1"
)

// agentState tracks what this worker currently reports in its heartbeats.
// A single mutex-guarded struct rather than loose atomics because status
// and current-job change together as one atomic update ("I just started
// running job X"), never independently.
type agentState struct {
	mu                sync.Mutex
	status            fleetforgev1.WorkerStatus
	currentJobID      string
	availableCapacity int32
	draining          bool
}

func (a *agentState) snapshot() (fleetforgev1.WorkerStatus, string, int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.status, a.currentJobID, a.availableCapacity
}

// startJob is only ever called by the single heartbeat-receive goroutine,
// so no separate "am I already busy" locking races are possible -- but the
// mutex still guards against the heartbeat-send goroutine reading a
// half-updated state concurrently.
func (a *agentState) startJob(jobID string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = fleetforgev1.WorkerStatus_WORKER_STATUS_BUSY
	a.currentJobID = jobID
	a.availableCapacity = 0
}

// finishJob reports DRAINING instead of READY if a drain request arrived
// while this job was running (setDraining couldn't flip status itself in
// that case, since the job was still in flight) -- this is what lets an
// operator drain a busy worker: it finishes its current job normally, then
// never goes back to advertising itself as available for new work.
func (a *agentState) finishJob(capacitySlots int32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.draining {
		a.status = fleetforgev1.WorkerStatus_WORKER_STATUS_DRAINING
	} else {
		a.status = fleetforgev1.WorkerStatus_WORKER_STATUS_READY
	}
	a.currentJobID = ""
	a.availableCapacity = capacitySlots
}

func (a *agentState) isBusy() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.currentJobID != ""
}

// setDraining records a scheduler-issued drain request
// (docs/09-design-rationale.md). If the worker is idle right now, it flips
// straight to DRAINING so its very next heartbeat reflects that; if it's
// mid-job, finishJob (above) is what applies the flip once that job
// completes. Returns true only the first time it's called, so the caller
// can log the transition once instead of on every heartbeat the scheduler
// keeps requesting it.
func (a *agentState) setDraining() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	first := !a.draining
	a.draining = true
	if a.currentJobID == "" {
		a.status = fleetforgev1.WorkerStatus_WORKER_STATUS_DRAINING
	}
	return first
}

func (a *agentState) isDraining() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.draining
}

// clearDraining undoes setDraining once the scheduler stops requesting
// drain (an operator called ResumeDrain). Real gap, caught by actually
// running this: without this method, there was NO code path that ever
// flipped agentState.draining back to false -- the receive loop only ever
// called setDraining() when DrainRequested was true, with no corresponding
// "else" for when it goes back to false. A resumed worker would keep
// silently refusing every future assignment as "received assignment while
// draining, ignoring" forever, even though the SERVER correctly considered
// it READY again. Returns true only the first time, same log-once pattern
// as setDraining.
func (a *agentState) clearDraining() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	was := a.draining
	a.draining = false
	if was && a.currentJobID == "" {
		a.status = fleetforgev1.WorkerStatus_WORKER_STATUS_READY
	}
	return was
}

func main() {
	log := zerolog.New(os.Stdout).With().Timestamp().Str("service", "worker-agent").Logger()

	cfg, err := config.LoadWorkerAgentConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("invalid config")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// insecure.NewCredentials() is the fallback for local dev only. mTLS
	// (docs/09-design-rationale.md 9.4) takes over as soon as all three
	// cert/key/CA paths are configured -- see scripts/gen-certs.sh.
	transportCreds := insecure.NewCredentials()
	if cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" && cfg.TLSCAFile != "" {
		tlsConfig, tlsErr := auth.ClientTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, cfg.TLSCAFile)
		if tlsErr != nil {
			//nolint:gocritic // process is exiting immediately; the pending defer stop() above has nothing left to clean up
			log.Fatal().Err(tlsErr).Msg("failed to load mTLS client config")
		}
		transportCreds = credentials.NewTLS(tlsConfig)
		log.Info().Msg("dialing scheduler with mTLS")
	} else {
		log.Warn().Msg("FLEETFORGE_TLS_CERT_FILE/KEY_FILE/CA_FILE not set, dialing scheduler with insecure credentials")
	}

	conn, err := grpc.NewClient(cfg.SchedulerGRPCAddr, grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		//nolint:gocritic // process is exiting immediately; the pending defer stop() above has nothing left to clean up
		log.Fatal().Err(err).Str("addr", cfg.SchedulerGRPCAddr).Msg("failed to dial scheduler")
	}
	defer func() { _ = conn.Close() }()

	client := fleetforgev1.NewFleetSchedulerClient(conn)

	regCtx, regCancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := client.RegisterWorker(regCtx, &fleetforgev1.RegisterWorkerRequest{
		Hostname:      cfg.Hostname,
		InstanceId:    cfg.InstanceID,
		Os:            cfg.OS,
		CpuCores:      int32(cfg.CPUCores),
		MemoryMb:      int32(cfg.MemoryMB),
		Labels:        cfg.Labels,
		Capabilities:  cfg.Capabilities,
		Version:       cfg.Version,
		CapacitySlots: int32(cfg.CapacitySlots),
	})
	regCancel()
	if err != nil {
		log.Fatal().Err(err).Msg("registration failed")
	}

	workerID := resp.GetWorkerId()
	epoch := resp.GetEpoch()
	heartbeatInterval := time.Duration(resp.GetHeartbeatIntervalSeconds()) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = 5 * time.Second
	}

	log.Info().
		Str("worker_id", workerID).
		Int64("epoch", epoch).
		Int32("heartbeat_interval_seconds", resp.GetHeartbeatIntervalSeconds()).
		Int32("heartbeat_timeout_seconds", resp.GetHeartbeatTimeoutSeconds()).
		Msg("registered with scheduler")

	capacitySlots := int32(cfg.CapacitySlots)
	if capacitySlots <= 0 {
		capacitySlots = 1
	}
	state := &agentState{
		status:            fleetforgev1.WorkerStatus_WORKER_STATUS_READY,
		availableCapacity: capacitySlots,
	}

	executor := agentruntime.SimulatedExecutor{FailureRate: cfg.SimulatedFailureRate}

	deps := heartbeatDeps{
		client:        client,
		log:           log,
		workerID:      workerID,
		epoch:         epoch,
		interval:      heartbeatInterval,
		state:         state,
		executor:      executor,
		capacitySlots: capacitySlots,
	}

	if err := streamHeartbeats(ctx, deps); err != nil && ctx.Err() == nil {
		// On stream failure (including a stale-epoch rejection per
		// docs/06-failure-scenarios.md #6) we exit rather than silently
		// re-registering under a NEW worker_id, which would orphan this
		// worker's history under two different identities. A supervised
		// restart (systemd, a process manager, or just re-running the
		// binary) is the explicit recovery action for now.
		log.Fatal().Err(err).Msg("heartbeat stream ended")
	}

	log.Info().Msg("shutting down")
}

type heartbeatDeps struct {
	client        fleetforgev1.FleetSchedulerClient
	log           zerolog.Logger
	workerID      string
	epoch         int64
	interval      time.Duration
	state         *agentState
	executor      agentruntime.Executor
	capacitySlots int32
}

// streamHeartbeats implements the worker side of
// docs/05-sequence-diagrams.md 5.2: one long-lived bidirectional stream, a
// HeartbeatRequest sent every `interval`, and a background goroutine
// reading whatever the scheduler sends back -- including a pending job
// assignment.
func streamHeartbeats(ctx context.Context, d heartbeatDeps) error {
	stream, err := d.client.Heartbeat(ctx)
	if err != nil {
		return err
	}

	recvErrCh := make(chan error, 1)
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				recvErrCh <- nil
				return
			}
			if err != nil {
				recvErrCh <- err
				return
			}

			if a := resp.GetAssignment(); a != nil {
				switch {
				case d.state.isBusy():
					// Already running something -- this would only happen
					// if the scheduler pushed an assignment to a worker it
					// (mistakenly) believes is idle. Log loudly; don't
					// silently drop the mismatch.
					d.log.Warn().Str("job_id", a.GetJobId()).Msg("received assignment while already busy, ignoring")
				case d.state.isDraining():
					// Shouldn't happen in practice -- WorkerStore.AssignJob's
					// CAS requires status='READY', and a draining worker is
					// never READY (docs/09-design-rationale.md) -- but
					// refusing explicitly here rather than silently
					// executing it is cheap insurance against exactly the
					// kind of cache/Postgres desync bug this project has
					// already hit twice.
					d.log.Warn().Str("job_id", a.GetJobId()).Msg("received assignment while draining, ignoring")
				default:
					d.log.Info().Str("job_id", a.GetJobId()).Str("repository", a.GetRepository()).Msg("received job assignment, starting execution")
					d.state.startJob(a.GetJobId())
					go runAssignment(ctx, d, a)
				}
			}
			if resp.GetDrainRequested() {
				if d.state.setDraining() {
					d.log.Info().Msg("scheduler requested drain -- finishing current job (if any) and refusing new assignments")
				}
			} else if d.state.clearDraining() {
				d.log.Info().Msg("drain resumed by scheduler -- accepting new assignments again")
			}
		}
	}()

	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = stream.CloseSend()
			return nil
		case err := <-recvErrCh:
			return err
		case <-ticker.C:
			workerStatus, currentJobID, capacity := d.state.snapshot()
			req := &fleetforgev1.HeartbeatRequest{
				WorkerId:          d.workerID,
				Epoch:             d.epoch,
				Status:            workerStatus,
				AvailableCapacity: capacity,
			}
			if currentJobID != "" {
				req.CurrentJobId = &currentJobID
			}
			if err := stream.Send(req); err != nil {
				return err
			}
		}
	}
}

// runAssignment executes one job and reports the result. It runs in its
// own goroutine so the heartbeat send/receive loop is never blocked on
// build execution -- a worker must keep heartbeating (and receiving any
// drain signal) while a job is RUNNING, not go silent for the length of
// the build.
func runAssignment(ctx context.Context, d heartbeatDeps, a *fleetforgev1.JobAssignment) {
	result := d.executor.Execute(ctx, agentruntime.Assignment{
		JobID:      a.GetJobId(),
		Repository: a.GetRepository(),
		Branch:     a.GetBranch(),
		CommitSHA:  a.GetCommitSha(),
	})

	finalStatus := fleetforgev1.JobStatus_JOB_STATUS_FAILED
	if result.Success {
		finalStatus = fleetforgev1.JobStatus_JOB_STATUS_SUCCESS
	}

	reportCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reportResp, err := d.client.ReportJobResult(reportCtx, &fleetforgev1.ReportJobResultRequest{
		JobId:           a.GetJobId(),
		AssignmentEpoch: a.GetAssignmentEpoch(),
		Status:          finalStatus,
		ExitCode:        result.ExitCode,
		LogRef:          result.LogRef,
		ErrorMessage:    result.ErrorMessage,
	})
	switch {
	case err != nil:
		d.log.Error().Err(err).Str("job_id", a.GetJobId()).Msg("failed to report job result")
	case reportResp.GetDiscardedStaleEpoch():
		d.log.Warn().Str("job_id", a.GetJobId()).Msg("scheduler discarded our result as stale (job was reassigned)")
	default:
		d.log.Info().Str("job_id", a.GetJobId()).Bool("success", result.Success).Msg("job result reported")
	}

	// Free up locally regardless of whether the report was accepted --
	// even in the discarded-stale-epoch case, THIS worker is done with the
	// job either way and should go back to accepting new work.
	d.state.finishJob(d.capacitySlots)
}
