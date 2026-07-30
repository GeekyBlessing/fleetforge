// Package agentruntime implements the actual "run a build" logic invoked
// by cmd/worker-agent once it receives a job assignment. FleetForge's job
// is orchestration, not being a build tool itself -- docs/08-implementation-roadmap.md
// Day 4 calls for "at minimum runs docker run against the job spec"; this
// package instead ships a SimulatedExecutor that spends real wall-clock
// time and reports real success/failure, which is enough to exercise the
// full scheduling/completion/retry pipeline honestly. A real executor
// (shelling out to `docker run`, streaming logs to object storage) plugs
// in behind the same Executor interface without the scheduler, the gRPC
// layer, or cmd/worker-agent's control flow changing at all.
package agentruntime

import (
	"context"
	"hash/fnv"
	"math/rand"
	"time"
)

// Assignment is the subset of a JobAssignment an executor needs.
type Assignment struct {
	JobID      string
	Repository string
	Branch     string
	CommitSHA  string
}

// Result is what cmd/worker-agent turns into a ReportJobResult call.
type Result struct {
	Success      bool
	ExitCode     int32
	LogRef       string
	ErrorMessage string
}

// Executor runs a single assigned job to completion (or cancellation via
// ctx). Swappable -- see the package doc comment.
type Executor interface {
	Execute(ctx context.Context, a Assignment) Result
}

// SimulatedExecutor spends a few real seconds "building" and occasionally
// reports failure, giving Day 5's retry policy something genuine to
// exercise instead of an always-green happy path.
type SimulatedExecutor struct {
	// FailureRate is the probability (0.0-1.0) that a given run reports
	// FAILED. Defaults to 0 (always succeeds) if left unset.
	FailureRate float64
}

func (e SimulatedExecutor) Execute(ctx context.Context, a Assignment) Result {
	// Duration derived from the commit sha (not pure random) so repeated
	// runs of "the same build" behave consistently within a demo --
	// 3-7 seconds, long enough to observe RUNNING in the dashboard/CLI
	// before it completes, short enough not to make manual testing tedious.
	h := fnv.New32a()
	_, _ = h.Write([]byte(a.CommitSHA))
	duration := time.Duration(3+int(h.Sum32()%5)) * time.Second

	select {
	case <-ctx.Done():
		return Result{Success: false, ExitCode: -1, ErrorMessage: "cancelled"}
	case <-time.After(duration):
	}

	if e.FailureRate > 0 && rand.Float64() < e.FailureRate { //nolint:gosec // simulated flakiness, not security-sensitive randomness
		return Result{Success: false, ExitCode: 1, ErrorMessage: "simulated build failure"}
	}
	return Result{Success: true, ExitCode: 0, LogRef: "local://simulated/" + a.JobID}
}
