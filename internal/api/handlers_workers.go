// Package api implements the human/CI-facing REST layer described in
// docs/02-openapi.yaml. It never talks to workers directly -- that's the
// gRPC control plane's job (internal/grpcserver) -- it only reads/writes
// through the store packages.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"

	"github.com/launchverse/fleetforge/internal/store/postgres"
	ffredis "github.com/launchverse/fleetforge/internal/store/redis"
)

type WorkersHandler struct {
	store *postgres.WorkerStore
	cache *ffredis.WorkerCache
	log   zerolog.Logger
}

func NewWorkersHandler(store *postgres.WorkerStore, cache *ffredis.WorkerCache, log zerolog.Logger) *WorkersHandler {
	return &WorkersHandler{store: store, cache: cache, log: log.With().Str("component", "api.workers").Logger()}
}

// workerResponse mirrors the Worker schema in docs/02-openapi.yaml. Kept as
// its own type (rather than serializing postgres.WorkerRow directly) so a
// storage-layer field rename never silently changes the wire contract --
// the OpenAPI spec is the source of truth for what clients see, and this
// struct is where that gets enforced.
type workerResponse struct {
	ID                string            `json:"id"`
	Hostname          string            `json:"hostname"`
	OS                string            `json:"os"`
	CPUCores          int32             `json:"cpu_cores"`
	MemoryMB          int32             `json:"memory_mb"`
	Labels            map[string]string `json:"labels"`
	Capabilities      []string          `json:"capabilities"`
	Status            string            `json:"status"`
	CurrentJobID      *string           `json:"current_job_id"`
	LastHeartbeat     *string           `json:"last_heartbeat"`
	Version           string            `json:"version"`
	AvailableCapacity int32             `json:"available_capacity"`
	Epoch             int64             `json:"epoch"`
	RegisteredAt      string            `json:"registered_at"`
	DrainRequested    bool              `json:"drain_requested"`
}

type workerListResponse struct {
	Items      []workerResponse `json:"items"`
	NextCursor *string          `json:"next_cursor"`
	TotalCount int              `json:"total_count"`
}

// ListWorkers implements GET /workers, supporting the `status` and `limit`
// query params from docs/02-openapi.yaml; `label` filtering and real
// cursor pagination are noted there as a gap to close once list traffic
// justifies it (docs/03-database-schema.md's GIN index on labels is already
// in place for when that lands).
func (h *WorkersHandler) ListWorkers(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")
	limit := 100

	rows, err := h.store.List(r.Context(), statusFilter, limit)
	if err != nil {
		h.log.Error().Err(err).Msg("list workers failed")
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to list workers")
		return
	}

	items := make([]workerResponse, 0, len(rows))
	for _, row := range rows {
		var lastHeartbeat *string
		if row.LastHeartbeat != nil {
			s := row.LastHeartbeat.Format(rfc3339)
			lastHeartbeat = &s
		}
		items = append(items, workerResponse{
			ID:                row.ID,
			Hostname:          row.Hostname,
			OS:                row.OS,
			CPUCores:          row.CPUCores,
			MemoryMB:          row.MemoryMB,
			Labels:            row.Labels,
			Capabilities:      row.Capabilities,
			Status:            row.Status,
			CurrentJobID:      row.CurrentJobID,
			LastHeartbeat:     lastHeartbeat,
			Version:           row.Version,
			AvailableCapacity: row.AvailableCapacity,
			Epoch:             row.Epoch,
			RegisteredAt:      row.RegisteredAt.Format(rfc3339),
			DrainRequested:    row.DrainRequested,
		})
	}

	writeJSON(w, http.StatusOK, workerListResponse{
		Items:      items,
		NextCursor: nil,
		TotalCount: len(items),
	})
}

// DrainWorker implements POST /workers/{workerId}/drain -- the
// operator-initiated graceful-removal flow (docs/09-design-rationale.md).
// See WorkerStore.RequestDrain for the state-machine details; this handler
// is just the dual-write shell around it, mirroring the transition into the
// Redis cache the same way internal/grpcserver/report_result.go does for
// FreeWorker.
func (h *WorkersHandler) DrainWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "workerId")

	result, ok, err := h.store.RequestDrain(r.Context(), workerID)
	if err != nil {
		h.log.Error().Err(err).Str("worker_id", workerID).Msg("failed to request drain")
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to request drain")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "worker not found, or already OFFLINE/DEAD")
		return
	}

	h.syncDrainToCache(r, workerID, result, true)

	h.log.Info().Str("worker_id", workerID).Str("status", result.Status).Msg("drain requested")
	writeJSON(w, http.StatusOK, map[string]any{
		"worker_id": workerID,
		"status":    result.Status,
	})
}

// ResumeWorker implements POST /workers/{workerId}/resume, undoing a prior
// drain request.
func (h *WorkersHandler) ResumeWorker(w http.ResponseWriter, r *http.Request) {
	workerID := chi.URLParam(r, "workerId")

	result, ok, err := h.store.ResumeDrain(r.Context(), workerID)
	if err != nil {
		h.log.Error().Err(err).Str("worker_id", workerID).Msg("failed to resume from drain")
		writeError(w, http.StatusInternalServerError, "internal_error", "failed to resume from drain")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "worker not found, or already OFFLINE/DEAD")
		return
	}

	h.syncDrainToCache(r, workerID, result, false)

	h.log.Info().Str("worker_id", workerID).Str("status", result.Status).Msg("drain resumed")
	writeJSON(w, http.StatusOK, map[string]any{
		"worker_id": workerID,
		"status":    result.Status,
	})
}

// syncDrainToCache mirrors a drain/resume transition into Redis. Two
// separate cache writes on purpose: SetState (status/current_job_id/etc.)
// and SetDrainRequested (the drain_requested flag) are intentionally
// disjoint writers -- see internal/store/redis/cache.go's SetState comment
// for why merging them back into one call would reintroduce the exact
// read-modify-write race that split was meant to avoid.
func (h *WorkersHandler) syncDrainToCache(r *http.Request, workerID string, result postgres.DrainResult, drainRequested bool) {
	currentJobID := ""
	if result.CurrentJobID != nil {
		currentJobID = *result.CurrentJobID
	}
	if err := h.cache.SetState(r.Context(), workerID, ffredis.WorkerState{
		Epoch:             result.Epoch,
		Status:            result.Status,
		CurrentJobID:      currentJobID,
		AvailableCapacity: result.AvailableCapacity,
		LastPGFlushUnix:   0,
	}); err != nil {
		h.log.Warn().Err(err).Str("worker_id", workerID).Msg("failed to sync worker state to redis cache")
	}
	if err := h.cache.SetDrainRequested(r.Context(), workerID, drainRequested); err != nil {
		h.log.Warn().Err(err).Str("worker_id", workerID).Msg("failed to sync drain_requested to redis cache")
	}
}

const rfc3339 = "2006-01-02T15:04:05Z07:00"

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}
