package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkerStore is the only thing in the codebase allowed to write SQL
// against the workers table.
type WorkerStore struct {
	pool *pgxpool.Pool
}

func NewWorkerStore(pool *pgxpool.Pool) *WorkerStore {
	return &WorkerStore{pool: pool}
}

type RegisterWorkerParams struct {
	Hostname      string
	InstanceID    string
	OS            string
	CPUCores      int32
	MemoryMB      int32
	Labels        map[string]string
	Capabilities  []string
	Version       string
	CapacitySlots int32
}

type RegisteredWorker struct {
	ID    string
	Epoch int64
}

// Register implements the idempotent registration flow from
// docs/05-sequence-diagrams.md section 5.1:
//
//   - A new instance_id creates a fresh row (epoch=1, status=REGISTERING then
//     READY).
//   - An instance_id we've already seen (the worker crashed and restarted, or
//     was marked DEAD and is coming back) bumps epoch by one and resets
//     status/capacity rather than creating a duplicate worker row. This is
//     what keeps the workers table bounded at "current fleet size" instead of
//     growing one row per restart.
//
// The epoch bump is also the fencing mechanism doc 6 depends on: any
// in-flight heartbeat or job-completion report carrying the *previous*
// epoch will be rejected once this transaction commits (see
// docs/06-failure-scenarios.md #6 and #10).
func (s *WorkerStore) Register(ctx context.Context, p RegisterWorkerParams) (RegisteredWorker, error) {
	if p.CapacitySlots <= 0 {
		p.CapacitySlots = 1
	}
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	if p.Capabilities == nil {
		p.Capabilities = []string{}
	}

	var result RegisteredWorker

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO workers (
				hostname, instance_id, os, cpu_cores, memory_mb, labels,
				capabilities, status, version, capacity_slots, available_capacity, epoch
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, 'REGISTERING', $8, $9, $9, 1)
			ON CONFLICT (instance_id) DO UPDATE SET
				hostname                = EXCLUDED.hostname,
				os                      = EXCLUDED.os,
				cpu_cores               = EXCLUDED.cpu_cores,
				memory_mb               = EXCLUDED.memory_mb,
				labels                  = EXCLUDED.labels,
				capabilities            = EXCLUDED.capabilities,
				status                  = 'REGISTERING',
				version                 = EXCLUDED.version,
				capacity_slots          = EXCLUDED.capacity_slots,
				available_capacity      = EXCLUDED.capacity_slots,
				-- Deliberately cleared: if this instance_id previously held a
				-- job and crashed, the reaper (doc 5.3) already requeued that
				-- job well before a real worker process could restart and
				-- re-register (heartbeat timeout is 20s; process restarts
				-- practically never beat that). A freshly (re-)registered
				-- worker must never believe it still owns a prior job.
				current_job_id          = NULL,
				current_job_created_at  = NULL,
				-- Also reset, and just as important as clearing current_job_id:
				-- without this, a worker re-registering after its PREVIOUS
				-- session went stale keeps that session's ancient
				-- last_heartbeat value, which makes it look immediately
				-- "stale" to the reaper (ListStale) before it has even had a
				-- chance to send its first heartbeat under the new epoch --
				-- a real bug caught by actually running this (see git log /
				-- session notes), not a hypothetical. This NULL is
				-- momentary: the promote-to-READY step below overwrites it
				-- with now() in the same transaction, closing a second,
				-- related bug (see that step's comment) rather than
				-- reopening it.
				last_heartbeat          = NULL,
				epoch                   = workers.epoch + 1,
				updated_at              = now()
			RETURNING id, epoch
		`,
			p.Hostname, p.InstanceID, p.OS, p.CPUCores, p.MemoryMB, p.Labels,
			p.Capabilities, p.Version, p.CapacitySlots,
		)

		if err := row.Scan(&result.ID, &result.Epoch); err != nil {
			return fmt.Errorf("insert/update worker: %w", err)
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO worker_events (worker_id, event_type, payload)
			VALUES ($1, 'REGISTERED', jsonb_build_object('epoch', $2::bigint, 'instance_id', $3::text))
		`, result.ID, result.Epoch, p.InstanceID); err != nil {
			return fmt.Errorf("insert worker_events: %w", err)
		}

		// Two statements rather than folding READY into the upsert above
		// because the audit event should record the transition, and doing it
		// as a distinct step keeps this method's shape identical to how the
		// reaper similarly does "state change + event insert" as a pair --
		// one consistent pattern across the codebase for "on top of a state
		// change, always drop a breadcrumb".
		// last_heartbeat is also stamped here, not left at the NULL the
		// insert/upsert above just set. Real bug, caught by chaos-testing
		// scenario #3: worker-agent's heartbeat loop uses time.NewTicker,
		// which fires only AFTER the first interval elapses (5s by
		// default), so a worker that crashes before its first heartbeat
		// -- entirely possible, since the scheduler can assign a job
		// within milliseconds of registration, well before that first
		// tick -- left last_heartbeat NULL forever. ListStale (below)
		// requires last_heartbeat IS NOT NULL, so that worker, and
		// whatever job it was assigned, could never be reaped: stuck
		// BUSY/ASSIGNED permanently. Stamping now() here closes the
		// window (the worker becomes reapable timeoutSeconds after
		// registration even if it never sends a single heartbeat) without
		// reintroducing the OTHER bug this line originally guarded
		// against (a re-registering worker's stale old timestamp making
		// it look instantly dead) -- now() is always fresh, never stale.
		if _, err := tx.Exec(ctx, `UPDATE workers SET status = 'READY', last_heartbeat = now() WHERE id = $1`, result.ID); err != nil {
			return fmt.Errorf("promote worker to ready: %w", err)
		}

		return nil
	})
	if err != nil {
		return RegisteredWorker{}, err
	}

	return result, nil
}

// WorkerRow is the read-side projection used by REST list/get endpoints.
type WorkerRow struct {
	ID                 string
	Hostname            string
	OS                  string
	CPUCores            int32
	MemoryMB            int32
	Labels              map[string]string
	Capabilities        []string
	Status              string
	CurrentJobID        *string
	LastHeartbeat        *time.Time
	Version             string
	CapacitySlots       int32
	AvailableCapacity   int32
	Epoch               int64
	RegisteredAt        time.Time
	DrainRequested      bool
}

// CountByStatus backs internal/metrics's worker-count gauge -- one GROUP BY
// rather than 6 separate List(status=X) calls.
func (s *WorkerStore) CountByStatus(ctx context.Context) (map[string]int32, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, COUNT(*) FROM workers GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count workers by status: %w", err)
	}
	defer rows.Close()

	out := make(map[string]int32)
	for rows.Next() {
		var status string
		var count int32
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan worker-count-by-status row: %w", err)
		}
		out[status] = count
	}
	return out, rows.Err()
}

// PickDrainCandidate implements the "never touch BUSY workers" rule from
// docs/09-design-rationale.md 9.2 point 3: the autoscaler's scale-down path
// only ever selects a fully-idle worker (READY, available_capacity ==
// capacity_slots -- not just READY with SOME spare capacity, since a
// worker mid-way through other work under a higher capacity_slots value
// shouldn't be drained either) that isn't already draining. Returns "" (not
// an error) if nothing qualifies right now -- scale-down simply does
// nothing that tick rather than force a pick.
func (s *WorkerStore) PickDrainCandidate(ctx context.Context) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT id FROM workers
		WHERE status = 'READY' AND available_capacity = capacity_slots AND drain_requested = false
		ORDER BY registered_at ASC
		LIMIT 1
	`).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("pick drain candidate: %w", err)
	}
	return id, nil
}

// List returns workers matching an optional status filter, most-recently
// registered first, capped at limit. Cursor-based pagination (per
// docs/02-openapi.yaml) is a real gap here -- offset-free "first N"
// semantics are enough while the fleet stays small; doc 2's full keyset
// pagination lands whenever list traffic actually needs it.
func (s *WorkerStore) List(ctx context.Context, statusFilter string, limit int) ([]WorkerRow, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var rows pgx.Rows
	var err error
	if statusFilter == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT id, hostname, os, cpu_cores, memory_mb, labels, capabilities,
			       status, current_job_id, last_heartbeat, version,
			       capacity_slots, available_capacity, epoch, registered_at, drain_requested
			FROM workers
			ORDER BY registered_at DESC
			LIMIT $1
		`, limit)
	} else {
		rows, err = s.pool.Query(ctx, `
			SELECT id, hostname, os, cpu_cores, memory_mb, labels, capabilities,
			       status, current_job_id, last_heartbeat, version,
			       capacity_slots, available_capacity, epoch, registered_at, drain_requested
			FROM workers
			WHERE status = $1
			ORDER BY registered_at DESC
			LIMIT $2
		`, statusFilter, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("query workers: %w", err)
	}
	defer rows.Close()

	var out []WorkerRow
	for rows.Next() {
		var w WorkerRow
		if err := rows.Scan(
			&w.ID, &w.Hostname, &w.OS, &w.CPUCores, &w.MemoryMB, &w.Labels,
			&w.Capabilities, &w.Status, &w.CurrentJobID, &w.LastHeartbeat,
			&w.Version, &w.CapacitySlots, &w.AvailableCapacity, &w.Epoch, &w.RegisteredAt,
			&w.DrainRequested,
		); err != nil {
			return nil, fmt.Errorf("scan worker row: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate worker rows: %w", err)
	}

	return out, nil
}

// GetEpochAndStatus is the fallback path used when the Redis cache misses
// (cold start, brief Redis outage) -- see docs/05-sequence-diagrams.md 5.2.
// Returns a not-found-ish error if the worker doesn't exist so the gRPC
// layer can map it to codes.NotFound (worker was reaped, must re-register).
func (s *WorkerStore) GetEpochAndStatus(ctx context.Context, workerID string) (epoch int64, status string, drainRequested bool, err error) {
	err = s.pool.QueryRow(ctx, `SELECT epoch, status, drain_requested FROM workers WHERE id = $1`, workerID).Scan(&epoch, &status, &drainRequested)
	if err != nil {
		return 0, "", false, fmt.Errorf("worker %s not found: %w", workerID, err)
	}
	return epoch, status, drainRequested, nil
}

// UpdateHeartbeat is the batched/coalesced write path from
// docs/05-sequence-diagrams.md 5.2 -- called on a status transition or
// after the coalescing window elapses, not on every single 5s heartbeat.
// The epoch in the WHERE clause is the fencing guard: if a worker was
// re-registered (crash/restart) since this heartbeat's epoch was read, this
// affects zero rows and the caller treats that as "stale, ignore" rather
// than as an error worth surfacing loudly.
func (s *WorkerStore) UpdateHeartbeat(ctx context.Context, workerID string, epoch int64, dbStatus string, currentJobID *string) (bool, error) {
	var currentJobCreatedAt *time.Time
	if currentJobID != nil {
		// Needed alongside current_job_id for the same reason as
		// registration -- see workers.current_job_created_at comment in
		// migration 000002. Look up the job's created_at (its partition
		// key) so the composite FK stays satisfiable.
		if err := s.pool.QueryRow(ctx, `SELECT created_at FROM jobs WHERE id = $1`, *currentJobID).Scan(&currentJobCreatedAt); err != nil {
			return false, fmt.Errorf("lookup current job created_at: %w", err)
		}
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE workers
		SET status = $1,
		    current_job_id = $2,
		    current_job_created_at = $3,
		    last_heartbeat = now()
		WHERE id = $4 AND epoch = $5
	`, dbStatus, currentJobID, currentJobCreatedAt, workerID, epoch)
	if err != nil {
		return false, fmt.Errorf("update heartbeat for worker %s: %w", workerID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// StaleWorker is the reaper's sweep result -- doc 5.3.
type StaleWorker struct {
	ID    string
	Epoch int64
}

// ListStale finds every worker whose last_heartbeat is older than
// timeoutSeconds and isn't already OFFLINE/DEAD. This is the authoritative
// dead-worker check (docs/06-failure-scenarios.md #4): it depends on
// nothing but Postgres, so it keeps working even if Redis is fully down.
func (s *WorkerStore) ListStale(ctx context.Context, timeoutSeconds int) ([]StaleWorker, error) {
	// Numeric interval arithmetic ($1 * interval '1 second'), not string
	// concatenation ($1 || ' seconds')::interval -- the latter makes
	// Postgres infer $1's type as text, which pgx then can't encode a Go
	// int into (no int->text encode plan). Multiplying against an interval
	// literal makes Postgres infer a numeric type instead, which pgx's
	// int/int8 encoding satisfies directly.
	rows, err := s.pool.Query(ctx, `
		SELECT id, epoch FROM workers
		WHERE status NOT IN ('OFFLINE', 'DEAD')
		  AND last_heartbeat IS NOT NULL
		  AND last_heartbeat < now() - ($1 * interval '1 second')
	`, timeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("query stale workers: %w", err)
	}
	defer rows.Close()

	var out []StaleWorker
	for rows.Next() {
		var w StaleWorker
		if err := rows.Scan(&w.ID, &w.Epoch); err != nil {
			return nil, fmt.Errorf("scan stale worker: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// MarkDeadAndRequeue implements the single atomic transaction from
// docs/05-sequence-diagrams.md 5.3: the worker flips to DEAD and any job it
// was running gets requeued, in the SAME transaction, specifically so it's
// never possible to observe "worker is DEAD but its job still shows
// RUNNING forever" (docs/06-failure-scenarios.md #3) even if the reaper
// itself crashes mid-sweep (the whole transaction rolls back and the next
// sweep picks the same worker back up).
//
// The epoch in the WHERE clause guards against a race where the worker
// re-registered (and so is no longer actually stale under its NEW epoch)
// between ListStale's read and this write.
func (s *WorkerStore) MarkDeadAndRequeue(ctx context.Context, workerID string, epoch int64) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// available_capacity is reset back to the full capacity_slots here,
		// not left at whatever it was mid-assignment: a DEAD worker isn't
		// running anything anymore (its job, if any, is being requeued in
		// this same transaction below), so there's nothing left to hold
		// capacity against. Without this reset, a worker that dies while
		// BUSY would (if it ever came back and re-registered -- which
		// resets capacity correctly anyway) be fine, but would otherwise
		// carry a permanently-wrong capacity number in any code path that
		// doesn't go through re-registration.
		tag, err := tx.Exec(ctx, `
			UPDATE workers
			SET status = 'DEAD', available_capacity = capacity_slots
			WHERE id = $1 AND epoch = $2 AND status NOT IN ('OFFLINE', 'DEAD')
		`, workerID, epoch)
		if err != nil {
			return fmt.Errorf("mark worker dead: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Already handled (re-registered under a new epoch, or another
			// reaper pass already caught it) -- not an error, just nothing
			// to do this time.
			return nil
		}

		// Requeue any job this worker was still holding. Clearing
		// worker_id/assignment_epoch is what lets the scheduler's normal
		// candidate-matching pick it back up on the next pass -- this is
		// the SAME "QUEUED, worker_id=NULL" state a brand-new job starts
		// in, so no special-case scheduling logic is needed for
		// requeued-after-death jobs.
		rows, err := tx.Query(ctx, `
			SELECT id, created_at, retries, max_retries FROM jobs
			WHERE worker_id = $1 AND status IN ('ASSIGNED', 'RUNNING')
		`, workerID)
		if err != nil {
			return fmt.Errorf("find jobs assigned to dead worker: %w", err)
		}
		type requeueJob struct {
			id                     string
			createdAt              time.Time
			retries, maxRetries    int
		}
		var jobs []requeueJob
		for rows.Next() {
			var j requeueJob
			if err := rows.Scan(&j.id, &j.createdAt, &j.retries, &j.maxRetries); err != nil {
				rows.Close()
				return fmt.Errorf("scan job assigned to dead worker: %w", err)
			}
			jobs = append(jobs, j)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate jobs assigned to dead worker: %w", err)
		}

		for _, j := range jobs {
			if _, err := tx.Exec(ctx, `
				UPDATE jobs
				SET status = 'QUEUED', worker_id = NULL, assignment_epoch = NULL, started_at = NULL
				WHERE id = $1 AND created_at = $2
			`, j.id, j.createdAt); err != nil {
				return fmt.Errorf("requeue job %s: %w", j.id, err)
			}
			// attempt_number is derived from the actual row count in
			// job_retry_history, NOT from jobs.retries: a worker-death
			// requeue deliberately does NOT increment retries (this is an
			// infrastructure failure, not counted against the job's
			// retry budget -- doc 5.3), but uq_retry_attempt(job_id,
			// attempt_number) still needs a value that's guaranteed unique
			// even if the SAME job dies-and-gets-reassigned more than once
			// while retries stays flat.
			var nextAttempt int
			if err := tx.QueryRow(ctx, `
				SELECT COUNT(*) + 1 FROM job_retry_history WHERE job_id = $1
			`, j.id).Scan(&nextAttempt); err != nil {
				return fmt.Errorf("count retry history for job %s: %w", j.id, err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO job_retry_history (job_id, job_created_at, attempt_number, status, worker_id, error_message, started_at, finished_at)
				VALUES ($1, $2, $3, 'FAILED', $4, 'worker_dead', now(), now())
			`, j.id, j.createdAt, nextAttempt, workerID); err != nil {
				return fmt.Errorf("insert retry history for job %s: %w", j.id, err)
			}
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO worker_events (worker_id, event_type, payload)
			VALUES ($1, 'MARKED_DEAD', jsonb_build_object('epoch', $2::bigint, 'requeued_jobs', $3::int))
		`, workerID, epoch, len(jobs)); err != nil {
			return fmt.Errorf("insert worker_events: %w", err)
		}

		return nil
	})
}

// Candidate is a scheduling candidate returned by ListReadyCandidates --
// see docs/09-design-rationale.md 9.1 for the capability-filter /
// least-loaded-ranking split this type supports.
type Candidate struct {
	ID                string
	Epoch             int64
	Capabilities      []string
	Labels            map[string]string
	AvailableCapacity int32
	CapacitySlots     int32
}

// ListReadyCandidates implements the capability hard-filter from
// docs/09-design-rationale.md 9.1: only READY workers with spare capacity
// AND whose capabilities are a superset of requiredCapabilities come back.
// Ranking among these (least-loaded) is a separate step
// (internal/scheduler/algorithm.go) so "what's a legal candidate"
// (correctness) and "which legal candidate is best" (optimization) stay
// untangled.
//
// Known scope note: this queries Postgres directly rather than scanning
// the Redis worker:*:state hashes doc 4.2 describes as the scheduler's hot
// path. Querying Postgres (via idx_workers_status_ready) is simpler and
// still fast at this fleet size; switching the read side to Redis is a
// contained change to this one method if profiling ever shows it's needed.
func (s *WorkerStore) ListReadyCandidates(ctx context.Context, requiredCapabilities []string) ([]Candidate, error) {
	if requiredCapabilities == nil {
		requiredCapabilities = []string{}
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, epoch, capabilities, labels, available_capacity, capacity_slots
		FROM workers
		WHERE status = 'READY' AND available_capacity > 0
		  AND capabilities @> $1::text[]
	`, requiredCapabilities)
	if err != nil {
		return nil, fmt.Errorf("query ready candidates: %w", err)
	}
	defer rows.Close()

	var out []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(&c.ID, &c.Epoch, &c.Capabilities, &c.Labels, &c.AvailableCapacity, &c.CapacitySlots); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// AssignJob implements the atomic compare-and-swap assignment transaction
// from docs/05-sequence-diagrams.md 5.4: the worker flips READY->BUSY and
// the job flips QUEUED->ASSIGNED in the SAME transaction, each guarded by a
// WHERE clause that only matches if the row is still in the expected
// state. Returns (false, nil) -- not an error -- if the CAS lost a race
// (the worker was taken by a concurrent assignment, or the job was
// cancelled/reassigned since the caller read it); the caller
// (internal/scheduler/loop.go) treats that as "retry next tick against a
// fresh candidate list," not as a failure worth logging loudly. This
// WHERE-clause guard, not any Redis lock, is the actual correctness
// guarantee here (docs/09-design-rationale.md 9.1 / docs/04-redis-data-model.md 4.3).
func (s *WorkerStore) AssignJob(ctx context.Context, jobID string, jobCreatedAt time.Time, workerID string, workerEpoch int64) (bool, error) {
	var ok bool
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		wTag, err := tx.Exec(ctx, `
			UPDATE workers
			SET status = 'BUSY',
			    current_job_id = $1,
			    current_job_created_at = $2,
			    available_capacity = available_capacity - 1
			WHERE id = $3 AND epoch = $4 AND status = 'READY' AND available_capacity > 0
		`, jobID, jobCreatedAt, workerID, workerEpoch)
		if err != nil {
			return fmt.Errorf("assign: update worker: %w", err)
		}
		if wTag.RowsAffected() != 1 {
			// Lost the race (or worker no longer matches) -- nothing was
			// changed, so there's nothing to undo; committing this no-op
			// transaction and committing an explicit rollback are
			// equivalent here.
			return nil
		}

		jTag, err := tx.Exec(ctx, `
			UPDATE jobs
			SET status = 'ASSIGNED', worker_id = $1, assignment_epoch = $2
			WHERE id = $3 AND created_at = $4 AND status = 'QUEUED'
		`, workerID, workerEpoch, jobID, jobCreatedAt)
		if err != nil {
			return fmt.Errorf("assign: update job: %w", err)
		}
		if jTag.RowsAffected() != 1 {
			// Job moved on since we read it (cancelled, or somehow already
			// assigned) -- same "nothing to undo" situation as above, but
			// note the worker-side UPDATE above WILL be committed as part
			// of this same transaction unless we explicitly undo it. Since
			// Postgres transactions are all-or-nothing, returning an error
			// here instead of nil is what forces BOTH updates to roll back
			// together -- the worker must NOT end up BUSY if the job
			// assignment didn't also succeed.
			return errJobAssignRaceLost
		}

		ok = true
		return nil
	})
	if errors.Is(err, errJobAssignRaceLost) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return ok, nil
}

var errJobAssignRaceLost = errors.New("assign: job no longer queued")

// FreeWorker returns a worker to READY with no current job, incrementing
// available_capacity back up. Called once a job's result has been recorded
// (internal/grpcserver/report_result.go) -- kept idempotent-safe via the
// epoch guard, same fencing rationale as everywhere else in this file.
//
// Returns the post-update available_capacity via RETURNING specifically so
// the caller can push the same update into the Redis cache
// (docs/04-redis-data-model.md 4.2) -- this Postgres-only update was a real
// bug on its own: without also correcting the cache, the cache kept
// showing status=BUSY with the completed job's ID forever, which made
// internal/grpcserver/heartbeat.go's assignment-push logic re-push that
// same already-finished job on every subsequent heartbeat, forever.
// newStatus is either READY or DRAINING -- a worker whose drain_requested
// flag was set while it was BUSY (docs/09-design-rationale.md's operator
// drain flow) must NOT return to the READY candidate pool just because its
// current job finished; it goes to DRAINING instead, same as it would have
// immediately if drain had been requested while it was already idle. The
// caller mirrors this returned status straight into the Redis cache -- same
// dual-write reasoning as the availableCapacity return value already had.
func (s *WorkerStore) FreeWorker(ctx context.Context, workerID string, epoch int64) (ok bool, availableCapacity int32, newStatus string, err error) {
	// Real bug, caught by actually running this (a genuine race, not a
	// typo): this used to also require `status = 'BUSY'` in the WHERE
	// clause. UpdateHeartbeat (internal/grpcserver/heartbeat.go) writes
	// whatever status the WORKER itself last self-reported on every single
	// heartbeat, completely independent of what AssignJob/FreeWorker do to
	// the same row. The very first heartbeat after an assignment always
	// reports "READY, no job" (the worker doesn't know about the job yet --
	// that's what the push in heartbeat.go's RESPONSE is for), and if that
	// heartbeat's Postgres flush lands in the same window, it briefly
	// overwrites status back to READY while available_capacity is still 0
	// from AssignJob's decrement. If a job's completion report happened to
	// land during that exact window, requiring status='BUSY' here made
	// this UPDATE match zero rows -- silently leaking a capacity slot
	// forever (status self-corrects on the worker's next heartbeat, but
	// nothing ever re-runs this increment). A worker stuck at
	// status=READY, available_capacity=0 is invisible to
	// ListReadyCandidates permanently, which is exactly what stalled the
	// job queue live during testing.
	//
	// The fix: don't gate on status at all. The real proof that this is a
	// legitimate completion already happened one level up -- Complete/
	// RetryOrFail's guard (assignment_epoch matches AND job.status IN
	// (ASSIGNED,RUNNING)) is a one-shot transition per job, and
	// report_result.go only calls FreeWorker when that already succeeded.
	// All FreeWorker needs to check is that this worker hasn't since
	// re-registered under a new epoch (which WOULD mean a totally
	// different worker-generation, e.g. after being reaped and restarted).
	row := s.pool.QueryRow(ctx, `
		UPDATE workers
		SET status = CASE WHEN drain_requested THEN 'DRAINING'::worker_status ELSE 'READY'::worker_status END,
		    current_job_id = NULL,
		    current_job_created_at = NULL,
		    available_capacity = LEAST(available_capacity + 1, capacity_slots)
		WHERE id = $1 AND epoch = $2
		RETURNING available_capacity, status
	`, workerID, epoch)
	if scanErr := row.Scan(&availableCapacity, &newStatus); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return false, 0, "", nil // epoch mismatch (worker re-registered since) -- not an error, just nothing to free
		}
		return false, 0, "", fmt.Errorf("free worker: %w", scanErr)
	}
	return true, availableCapacity, newStatus, nil
}

// DrainResult carries back everything the REST layer (internal/api/handlers_workers.go)
// needs to mirror a drain/resume transition straight into the Redis cache,
// the same "return what the caller needs to dual-write" pattern FreeWorker
// uses.
type DrainResult struct {
	Epoch             int64
	Status            string
	CurrentJobID      *string
	AvailableCapacity int32
}

// RequestDrain implements the operator-initiated graceful-removal flow
// (docs/09-design-rationale.md). An idle (READY) worker is flipped straight
// to DRAINING -- it stops being a scheduling candidate immediately, since
// WorkerStore.ListReadyCandidates only ever matches status='READY'. A BUSY
// worker keeps its status (it's still finishing a job) but drain_requested
// is set so FreeWorker routes it to DRAINING, not READY, once that job
// completes. A worker already OFFLINE/DEAD can't be drained -- there's
// nothing to gracefully remove.
func (s *WorkerStore) RequestDrain(ctx context.Context, workerID string) (DrainResult, bool, error) {
	var r DrainResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE workers
			SET drain_requested = true,
			    status = CASE WHEN status = 'READY' THEN 'DRAINING'::worker_status ELSE status END
			WHERE id = $1 AND status NOT IN ('OFFLINE', 'DEAD')
			RETURNING epoch, status, current_job_id, available_capacity
		`, workerID)
		if err := row.Scan(&r.Epoch, &r.Status, &r.CurrentJobID, &r.AvailableCapacity); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return pgx.ErrNoRows
			}
			return fmt.Errorf("request drain: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO worker_events (worker_id, event_type, payload)
			VALUES ($1, 'DRAIN_REQUESTED', jsonb_build_object('epoch', $2::bigint))
		`, workerID, r.Epoch); err != nil {
			return fmt.Errorf("insert worker_events: %w", err)
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DrainResult{}, false, nil
	}
	if err != nil {
		return DrainResult{}, false, err
	}
	return r, true, nil
}

// ResumeDrain undoes RequestDrain: clears drain_requested and, if the
// worker is currently DRAINING (i.e. it wasn't mid-job when resumed, or it
// finished its last job while draining and is now idle), returns it to
// READY so it's a scheduling candidate again. A worker that's still BUSY
// when resumed just has its drain_requested flag cleared -- FreeWorker will
// naturally send it to READY (not DRAINING) once it completes, since the
// flag is gone.
func (s *WorkerStore) ResumeDrain(ctx context.Context, workerID string) (DrainResult, bool, error) {
	var r DrainResult
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE workers
			SET drain_requested = false,
			    status = CASE WHEN status = 'DRAINING' THEN 'READY'::worker_status ELSE status END
			WHERE id = $1 AND status NOT IN ('OFFLINE', 'DEAD')
			RETURNING epoch, status, current_job_id, available_capacity
		`, workerID)
		if err := row.Scan(&r.Epoch, &r.Status, &r.CurrentJobID, &r.AvailableCapacity); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return pgx.ErrNoRows
			}
			return fmt.Errorf("resume drain: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO worker_events (worker_id, event_type, payload)
			VALUES ($1, 'DRAIN_RESUMED', jsonb_build_object('epoch', $2::bigint))
		`, workerID, r.Epoch); err != nil {
			return fmt.Errorf("insert worker_events: %w", err)
		}
		return nil
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DrainResult{}, false, nil
	}
	if err != nil {
		return DrainResult{}, false, err
	}
	return r, true, nil
}
