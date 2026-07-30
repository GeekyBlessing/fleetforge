package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/queue"
	"github.com/launchverse/fleetforge/internal/store/postgres"
)

type JobsHandler struct {
	store *postgres.JobStore
	queue queue.Backend
	log   zerolog.Logger
}

func NewJobsHandler(store *postgres.JobStore, q queue.Backend, log zerolog.Logger) *JobsHandler {
	return &JobsHandler{store: store, queue: q, log: log.With().Str("component", "api.jobs").Logger()}
}

// submitJobRequest mirrors JobSubmissionRequest in docs/02-openapi.yaml.
type submitJobRequest struct {
	Priority             *int16            `json:"priority"`
	Repository           string            `json:"repository"`
	Branch               string            `json:"branch"`
	CommitSHA            string            `json:"commit_sha"`
	Labels               map[string]string `json:"labels"`
	RequiredCapabilities []string          `json:"required_capabilities"`
	MaxRetries           int32             `json:"max_retries"`
	IdempotencyKey       string            `json:"idempotency_key"`
}

type jobResponse struct {
	ID                   string            `json:"id"`
	Priority             int16             `json:"priority"`
	Repository           string            `json:"repository"`
	Branch               string            `json:"branch"`
	CommitSHA            string            `json:"commit_sha"`
	Labels               map[string]string `json:"labels"`
	RequiredCapabilities []string          `json:"required_capabilities"`
	Retries              int32             `json:"retries"`
	MaxRetries           int32             `json:"max_retries"`
	Status               string            `json:"status"`
	WorkerID             *string           `json:"worker_id"`
	LogRef               *string           `json:"log_ref"`
	CreatedAt            string            `json:"created_at"`
	StartedAt            *string           `json:"started_at"`
	FinishedAt           *string           `json:"finished_at"`
}

type jobListResponse struct {
	Items      []jobResponse `json:"items"`
	NextCursor *string       `json:"next_cursor"`
	TotalCount int           `json:"total_count"`
}

// SubmitJob implements POST /jobs (docs/02-openapi.yaml). Writes the job to
// Postgres first (durable, source of truth) and only enqueues to Redis
// once that succeeds -- if the enqueue fails, the job still exists as
// QUEUED in Postgres and Day 4's scheduler-side Postgres fallback path
// (docs/06-failure-scenarios.md #4) will still find and schedule it, just
// on a slower polling cadence instead of via the stream.
func (h *JobsHandler) SubmitJob(w http.ResponseWriter, r *http.Request) {
	var req submitJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
		return
	}

	if req.Repository == "" || req.Branch == "" || req.CommitSHA == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "repository, branch, and commit_sha are required")
		return
	}

	priority := int16(5)
	if req.Priority != nil {
		if *req.Priority < 0 || *req.Priority > 9 {
			writeError(w, http.StatusBadRequest, "invalid_request", "priority must be between 0 and 9")
			return
		}
		priority = *req.Priority
	}

	job, wasExisting, err := h.store.Create(r.Context(), postgres.CreateJobParams{
		Priority:             priority,
		Repository:           req.Repository,
		Branch:               req.Branch,
		CommitSHA:            req.CommitSHA,
		Labels:               req.Labels,
		RequiredCapabilities: req.RequiredCapabilities,
		MaxRetries:           req.MaxRetries,
		IdempotencyKey:       req.IdempotencyKey,
	})
	if err != nil {
		h.log.Error().Err(err).Msg("job creation failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to create job")
		return
	}

	if wasExisting {
		writeJSON(w, http.StatusConflict, toJobResponse(job))
		return
	}

	if err := h.queue.Enqueue(r.Context(), queue.JobMessage{
		JobID:                job.ID,
		Priority:             job.Priority,
		Repository:           job.Repository,
		Branch:               job.Branch,
		CommitSHA:            job.CommitSHA,
		RequiredCapabilities: job.RequiredCapabilities,
	}); err != nil {
		// Logged, not fatal to the request -- see the doc comment above.
		h.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to enqueue job to redis stream (job is still QUEUED in postgres)")
	}

	writeJSON(w, http.StatusAccepted, toJobResponse(job))
}

func (h *JobsHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobId")
	job, err := h.store.Get(r.Context(), jobID)
	if err != nil {
		writeError(w, http.StatusNotFound, "not_found", "job not found")
		return
	}
	writeJSON(w, http.StatusOK, toJobResponse(job))
}

func (h *JobsHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	filter := postgres.ListJobsFilter{
		Status:     r.URL.Query().Get("status"),
		Repository: r.URL.Query().Get("repository"),
		WorkerID:   r.URL.Query().Get("worker_id"),
		Limit:      100,
	}

	jobs, err := h.store.List(r.Context(), filter)
	if err != nil {
		h.log.Error().Err(err).Msg("list jobs failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list jobs")
		return
	}

	items := make([]jobResponse, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, toJobResponse(j))
	}

	writeJSON(w, http.StatusOK, jobListResponse{Items: items, TotalCount: len(items)})
}

func toJobResponse(j postgres.Job) jobResponse {
	var startedAt, finishedAt *string
	if j.StartedAt != nil {
		s := j.StartedAt.Format(rfc3339)
		startedAt = &s
	}
	if j.FinishedAt != nil {
		s := j.FinishedAt.Format(rfc3339)
		finishedAt = &s
	}
	return jobResponse{
		ID:                   j.ID,
		Priority:             j.Priority,
		Repository:           j.Repository,
		Branch:               j.Branch,
		CommitSHA:            j.CommitSHA,
		Labels:               j.Labels,
		RequiredCapabilities: j.RequiredCapabilities,
		Retries:              j.Retries,
		MaxRetries:           j.MaxRetries,
		Status:               j.Status,
		WorkerID:             j.WorkerID,
		LogRef:               j.LogRef,
		CreatedAt:            j.CreatedAt.Format(rfc3339),
		StartedAt:            startedAt,
		FinishedAt:           finishedAt,
	}
}
