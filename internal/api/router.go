package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/queue"
	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
)

// NewRouter wires the REST surface: worker listing/drain/resume, job
// submission/listing/get, health checks, and GET /metrics for Prometheus
// scraping (deploy/prometheus.yml), following
// docs/02-openapi.yaml's paths one at a time rather than stubbing all of
// them up front.
func NewRouter(
	workerStore *postgres.WorkerStore,
	jobStore *postgres.JobStore,
	jobQueue queue.Backend,
	workerCache *ffredis.WorkerCache,
	log zerolog.Logger,
	isReady func() (postgres bool, redis bool, isLeader bool),
) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(requestLogger(log))

	workers := NewWorkersHandler(workerStore, workerCache, log)
	jobs := NewJobsHandler(jobStore, jobQueue, log)

	r.Route("/v1", func(r chi.Router) {
		r.Get("/workers", workers.ListWorkers)
		r.Post("/workers/{workerId}/drain", workers.DrainWorker)
		r.Post("/workers/{workerId}/resume", workers.ResumeWorker)

		r.Post("/jobs", jobs.SubmitJob)
		r.Get("/jobs", jobs.ListJobs)
		r.Get("/jobs/{jobId}", jobs.GetJob)
	})

	// Uses the default Prometheus registry/gatherer -- internal/metrics's
	// collectors are registered against prometheus.DefaultRegisterer once
	// in cmd/scheduler/main.go, so this handler needs no explicit registry
	// wiring of its own.
	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r.Get("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pgOK, redisOK, leader := isReady()
		status := http.StatusOK
		if !pgOK {
			// Redis unreachable degrades gracefully (doc 6 #4); Postgres
			// unreachable does not (doc 6 #5) -- readiness reflects that
			// asymmetry rather than treating both dependencies the same.
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]bool{
			"postgres":  pgOK,
			"redis":     redisOK,
			"is_leader": leader,
		})
	})

	return r
}

func requestLogger(log zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Debug().Str("method", r.Method).Str("path", r.URL.Path).Msg("request")
			next.ServeHTTP(w, r)
		})
	}
}
