// Package metrics holds every Prometheus collector FleetForge exposes on
// GET /metrics (docs/02-openapi.yaml's path).
// Collectors live as package-level vars (the standard client_golang pattern)
// registered once in cmd/scheduler/main.go, so any package can import
// internal/metrics and record a measurement without needing a Metrics
// struct threaded through every call site.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// JobsQueued is a gauge, not a counter: it's refreshed wholesale on
	// every Collector tick (internal/metrics/collector.go) from a fresh
	// Postgres COUNT(*), not incremented/decremented at each individual
	// enqueue/dequeue call site. That sidesteps having to thread a metrics
	// object through every job-state-transition call site in
	// internal/store/postgres/jobs.go just to keep a running total in sync.
	JobsQueued = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetforge_jobs_queued",
		Help: "Current number of jobs in QUEUED status, by priority (0=highest, 9=lowest).",
	}, []string{"priority"})

	// WorkersByStatus mirrors WorkerStore.CountByStatus: one gauge sample
	// per worker_status enum value, refreshed the same way as JobsQueued.
	WorkersByStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetforge_workers",
		Help: "Current number of workers, by status.",
	}, []string{"status"})

	// JobsAssignedTotal is a true counter, incremented inline at the one
	// call site that commits an assignment (internal/scheduler/loop.go),
	// right after WorkerStore.AssignJob succeeds.
	JobsAssignedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fleetforge_jobs_assigned_total",
		Help: "Total number of successful job assignments made by the scheduler.",
	})

	// JobsCompletedTotal is incremented inline in
	// internal/grpcserver/report_result.go once Complete/RetryOrFail
	// successfully commits a job's new (terminal-or-retrying) status.
	JobsCompletedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fleetforge_jobs_completed_total",
		Help: "Total number of job result reports successfully recorded, by resulting status (SUCCESS, FAILED, CANCELLED, RETRYING).",
	}, []string{"status"})

	// JobDurationSeconds is only observed for a job's FINAL terminal
	// transition (SUCCESS/FAILED/CANCELLED, not RETRYING, since a job
	// that's going to run again isn't "done" yet), measured from started_at to
	// finished_at (execution time only, not queue wait time).
	JobDurationSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fleetforge_job_duration_seconds",
		Help:    "Job execution duration (started_at to finished_at) for jobs reaching a terminal state.",
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 12), // 0.5s .. ~1024s
	})

	// AutoscalerDecisionsTotal records every autoscaler tick's outcome
	// (docs/09-design-rationale.md 9.2), including "hold", so a Grafana
	// panel can show the decision RATE, not just count the rare scale
	// events, which is what actually lets you eyeball "is the autoscaler
	// alive and evaluating" versus "is it just never deciding to act."
	AutoscalerDecisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fleetforge_autoscaler_decisions_total",
		Help: "Total autoscaler decisions, by decision (scale_up, scale_down, hold).",
	}, []string{"decision"})
)

// MustRegister wires every collector above into reg. Called exactly once
// from cmd/scheduler/main.go: registering package-level vars more than
// once would panic (client_golang's documented behavior for a duplicate
// registration), a deliberate guardrail against accidentally wiring this
// up twice.
func MustRegister(reg prometheus.Registerer) {
	reg.MustRegister(
		JobsQueued,
		WorkersByStatus,
		JobsAssignedTotal,
		JobsCompletedTotal,
		JobDurationSeconds,
		AutoscalerDecisionsTotal,
	)
}
