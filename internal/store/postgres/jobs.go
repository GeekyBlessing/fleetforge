package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobStore is the only thing in the codebase allowed to write SQL against
// the jobs table (and, for retry history, job_retry_history).
type JobStore struct {
	pool *pgxpool.Pool
}

func NewJobStore(pool *pgxpool.Pool) *JobStore {
	return &JobStore{pool: pool}
}

type CreateJobParams struct {
	Priority             int16
	Repository           string
	Branch               string
	CommitSHA            string
	Labels               map[string]string
	RequiredCapabilities []string
	MaxRetries           int32
	IdempotencyKey       string // "" means no idempotency check requested
}

type Job struct {
	ID                   string
	Priority             int16
	Repository           string
	Branch               string
	CommitSHA            string
	Labels               map[string]string
	RequiredCapabilities []string
	Retries              int32
	MaxRetries           int32
	Status               string
	WorkerID             *string
	LogRef               *string
	CreatedAt            time.Time
	StartedAt            *time.Time
	FinishedAt           *time.Time
}

var ErrJobNotFound = errors.New("job not found")

// Create implements the idempotent-submission behavior from
// docs/02-openapi.yaml (POST /jobs, 409 on duplicate idempotency_key).
//
// Known gap, documented rather than silently accepted: the check-then-insert
// below has a race window (two concurrent submissions with the same
// idempotency_key could both pass the SELECT before either INSERTs), and
// even outside that race, uq_jobs_idempotency_key is only unique within a
// matching created_at (docs/03-database-schema.md 3.2 note) because jobs is
// partitioned by created_at. Both gaps have the same fix -- a small
// dedicated, non-partitioned job_idempotency_keys(idempotency_key PK,
// job_id) lookup table checked/inserted in the same transaction as job
// creation -- flagged in docs/09-design-rationale.md as a pre-production
// follow-up rather than solved here, given the current scope.
func (s *JobStore) Create(ctx context.Context, p CreateJobParams) (job Job, wasExisting bool, err error) {
	if p.Labels == nil {
		p.Labels = map[string]string{}
	}
	if p.RequiredCapabilities == nil {
		p.RequiredCapabilities = []string{}
	}
	if p.MaxRetries <= 0 {
		p.MaxRetries = 3
	}

	if p.IdempotencyKey != "" {
		existing, err := s.getByIdempotencyKey(ctx, p.IdempotencyKey)
		if err == nil {
			return existing, true, nil
		}
		if !errors.Is(err, ErrJobNotFound) {
			return Job{}, false, err
		}
	}

	var idempotencyKey *string
	if p.IdempotencyKey != "" {
		idempotencyKey = &p.IdempotencyKey
	}

	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (
			priority, repository, branch, commit_sha, labels,
			required_capabilities, max_retries, idempotency_key, status
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'QUEUED')
		RETURNING id, priority, repository, branch, commit_sha, labels,
		          required_capabilities, retries, max_retries, status,
		          worker_id, log_ref, created_at, started_at, finished_at
	`, p.Priority, p.Repository, p.Branch, p.CommitSHA, p.Labels,
		p.RequiredCapabilities, p.MaxRetries, idempotencyKey)

	j, err := scanJob(row)
	if err != nil {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	return j, false, nil
}

func (s *JobStore) getByIdempotencyKey(ctx context.Context, key string) (Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, priority, repository, branch, commit_sha, labels,
		       required_capabilities, retries, max_retries, status,
		       worker_id, log_ref, created_at, started_at, finished_at
		FROM jobs WHERE idempotency_key = $1
		ORDER BY created_at DESC LIMIT 1
	`, key)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, fmt.Errorf("lookup job by idempotency key: %w", err)
	}
	return j, nil
}

func (s *JobStore) Get(ctx context.Context, jobID string) (Job, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, priority, repository, branch, commit_sha, labels,
		       required_capabilities, retries, max_retries, status,
		       worker_id, log_ref, created_at, started_at, finished_at
		FROM jobs WHERE id = $1
	`, jobID)
	j, err := scanJob(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Job{}, ErrJobNotFound
		}
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// CountQueuedByPriority backs internal/metrics's queue-depth gauge -- a
// single GROUP BY rather than 10 separate List(status=QUEUED, priority=N)
// calls, since this runs on every Collector tick.
func (s *JobStore) CountQueuedByPriority(ctx context.Context) (map[int16]int32, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT priority, COUNT(*) FROM jobs WHERE status = 'QUEUED' GROUP BY priority
	`)
	if err != nil {
		return nil, fmt.Errorf("count queued jobs by priority: %w", err)
	}
	defer rows.Close()

	out := make(map[int16]int32)
	for rows.Next() {
		var priority int16
		var count int32
		if err := rows.Scan(&priority, &count); err != nil {
			return nil, fmt.Errorf("scan queued-by-priority row: %w", err)
		}
		out[priority] = count
	}
	return out, rows.Err()
}

type ListJobsFilter struct {
	Status     string
	Repository string
	WorkerID   string
	Limit      int
}

func (s *JobStore) List(ctx context.Context, f ListJobsFilter) ([]Job, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}

	// Built dynamically rather than one static query with "$1 = '' OR
	// status = $1"-style fallbacks: status is the job_status ENUM column,
	// and a parameter that Postgres infers as text (from the "= ''"
	// branch) has no implicit cast to a custom enum type -- the same class
	// of pgx/Postgres type-inference failure as the interval bug in
	// ListStale (see the comment there). Building the predicate list only
	// when a filter is actually supplied sidesteps the issue rather than
	// fighting enum casts inline.
	conditions := []string{}
	args := []any{}
	argN := 0

	nextArg := func(v any) string {
		argN++
		args = append(args, v)
		return fmt.Sprintf("$%d", argN)
	}

	if f.Status != "" {
		conditions = append(conditions, "status = "+nextArg(f.Status)+"::job_status")
	}
	if f.Repository != "" {
		conditions = append(conditions, "repository = "+nextArg(f.Repository))
	}
	if f.WorkerID != "" {
		conditions = append(conditions, "worker_id = "+nextArg(f.WorkerID)+"::uuid")
	}

	where := ""
	if len(conditions) > 0 {
		where = "WHERE " + conditions[0]
		for _, c := range conditions[1:] {
			where += " AND " + c
		}
	}

	query := fmt.Sprintf(`
		SELECT id, priority, repository, branch, commit_sha, labels,
		       required_capabilities, retries, max_retries, status,
		       worker_id, log_ref, created_at, started_at, finished_at
		FROM jobs
		%s
		ORDER BY created_at DESC
		LIMIT %s
	`, where, nextArg(f.Limit))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query jobs: %w", err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		j, err := scanJobRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// MarkRunning transitions ASSIGNED -> RUNNING, setting started_at, once the
// worker's own heartbeat confirms it has picked the job up
// (docs/05-sequence-diagrams.md 5.4). assignmentEpoch is the worker's
// epoch at assignment time (jobs.assignment_epoch was set to exactly this
// value by WorkerStore.AssignJob) -- passing the worker's CURRENT epoch
// here means a worker that has since re-registered (and so holds a new,
// different epoch) can never accidentally mark a stale assignment as
// running.
func (s *JobStore) MarkRunning(ctx context.Context, jobID string, assignmentEpoch int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'RUNNING', started_at = now()
		WHERE id = $1 AND assignment_epoch = $2 AND status = 'ASSIGNED'
	`, jobID, assignmentEpoch)
	if err != nil {
		return false, fmt.Errorf("mark job running: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// Complete implements the epoch-guarded completion path from
// docs/05-sequence-diagrams.md 5.4 and docs/06-failure-scenarios.md #10: a
// ReportJobResult carrying a stale assignment_epoch (the job was already
// reassigned since this worker was given it) affects zero rows here and
// the caller (internal/grpcserver/report_result.go) treats that as
// "discard, this is a late/duplicate report" rather than an error.
func (s *JobStore) Complete(ctx context.Context, jobID string, jobCreatedAt time.Time, assignmentEpoch int64, finalStatus string, logRef string) (bool, error) {
	var logRefArg *string
	if logRef != "" {
		logRefArg = &logRef
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = $1, finished_at = now(), log_ref = $2
		WHERE id = $3 AND created_at = $4 AND assignment_epoch = $5
		  AND status IN ('ASSIGNED', 'RUNNING')
	`, finalStatus, logRefArg, jobID, jobCreatedAt, assignmentEpoch)
	if err != nil {
		return false, fmt.Errorf("complete job: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// retryBackoff implements docs/09-design-rationale.md 9.3's exponential
// backoff: doubling per attempt, capped at 60s so a flaky-but-recovering
// worker pool doesn't leave a job parked for an unreasonable amount of time.
func retryBackoff(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

// RetryOrFail implements the FAILED-report branch of
// docs/05-sequence-diagrams.md 5.4 that Complete alone doesn't cover: a
// reported failure isn't always terminal. If the job still has retry budget
// left, it parks in RETRYING with a future retry_at (internal/scheduler's
// RetryPoller is what later moves it back to QUEUED once that elapses,
// mirroring docs/03-database-schema.md 3.2's idx_jobs_retrying_due index);
// otherwise it's FAILED, same as Complete's terminal path. Either branch is
// guarded by the same (jobID, jobCreatedAt, assignmentEpoch, status IN
// (ASSIGNED,RUNNING)) predicate Complete uses, for the identical
// stale/duplicate-report discard reason (docs/06-failure-scenarios.md #10).
//
// workerID is recorded on the job_retry_history row for audit purposes only
// -- it is NOT part of the guard (the assignment_epoch already proves which
// worker/attempt this report belongs to).
func (s *JobStore) RetryOrFail(ctx context.Context, jobID string, jobCreatedAt time.Time, assignmentEpoch int64, workerID *string, errorMessage string, logRef string) (newStatus string, ok bool, err error) {
	var logRefArg, errMsgArg *string
	if logRef != "" {
		logRefArg = &logRef
	}
	if errorMessage != "" {
		errMsgArg = &errorMessage
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var retries, maxRetries int32
		row := tx.QueryRow(ctx, `
			SELECT retries, max_retries FROM jobs
			WHERE id = $1 AND created_at = $2 AND assignment_epoch = $3
			  AND status IN ('ASSIGNED', 'RUNNING')
			FOR UPDATE
		`, jobID, jobCreatedAt, assignmentEpoch)
		if scanErr := row.Scan(&retries, &maxRetries); scanErr != nil {
			if errors.Is(scanErr, pgx.ErrNoRows) {
				ok = false
				return nil // stale/duplicate report -- same discard as Complete
			}
			return fmt.Errorf("lookup job for retry decision: %w", scanErr)
		}

		var nextAttempt int
		if err := tx.QueryRow(ctx, `
			SELECT COUNT(*) + 1 FROM job_retry_history WHERE job_id = $1
		`, jobID).Scan(&nextAttempt); err != nil {
			return fmt.Errorf("count retry history: %w", err)
		}

		nextRetries := retries + 1
		if nextRetries <= maxRetries {
			retryAt := time.Now().Add(retryBackoff(nextRetries))
			if _, err := tx.Exec(ctx, `
				UPDATE jobs
				SET status = 'RETRYING', retries = $1, retry_at = $2,
				    worker_id = NULL, assignment_epoch = NULL, started_at = NULL
				WHERE id = $3 AND created_at = $4
			`, nextRetries, retryAt, jobID, jobCreatedAt); err != nil {
				return fmt.Errorf("mark job retrying: %w", err)
			}
			newStatus = "RETRYING"
		} else {
			if _, err := tx.Exec(ctx, `
				UPDATE jobs
				SET status = 'FAILED', finished_at = now(), log_ref = $1
				WHERE id = $2 AND created_at = $3
			`, logRefArg, jobID, jobCreatedAt); err != nil {
				return fmt.Errorf("mark job failed (retries exhausted): %w", err)
			}
			newStatus = "FAILED"
		}

		if _, err := tx.Exec(ctx, `
			INSERT INTO job_retry_history (job_id, job_created_at, attempt_number, status, worker_id, error_message, started_at, finished_at)
			VALUES ($1, $2, $3, 'FAILED', $4, $5, now(), now())
		`, jobID, jobCreatedAt, nextAttempt, workerID, errMsgArg); err != nil {
			return fmt.Errorf("insert retry history: %w", err)
		}

		ok = true
		return nil
	})
	if err != nil {
		return "", false, err
	}
	return newStatus, ok, nil
}

// DueRetry is one row of RetryPoller's backoff-elapsed scan.
type DueRetry struct {
	ID                   string
	CreatedAt            time.Time
	Priority             int16
	Repository           string
	Branch               string
	CommitSHA            string
	RequiredCapabilities []string
}

// ListDueRetries finds RETRYING jobs whose backoff window has elapsed --
// the read side of docs/03-database-schema.md 3.2's idx_jobs_retrying_due.
func (s *JobStore) ListDueRetries(ctx context.Context, limit int) ([]DueRetry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, created_at, priority, repository, branch, commit_sha, required_capabilities
		FROM jobs
		WHERE status = 'RETRYING' AND retry_at <= now()
		ORDER BY retry_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query due retries: %w", err)
	}
	defer rows.Close()

	var out []DueRetry
	for rows.Next() {
		var d DueRetry
		if err := rows.Scan(&d.ID, &d.CreatedAt, &d.Priority, &d.Repository, &d.Branch, &d.CommitSHA, &d.RequiredCapabilities); err != nil {
			return nil, fmt.Errorf("scan due retry: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// MarkQueuedFromRetry transitions RETRYING -> QUEUED once RetryPoller has
// decided a job's backoff has elapsed. Guarded on status='RETRYING' so a
// job that somehow moved on (e.g. was cancelled) between ListDueRetries'
// read and this write is left alone rather than resurrected.
func (s *JobStore) MarkQueuedFromRetry(ctx context.Context, jobID string, jobCreatedAt time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET status = 'QUEUED', retry_at = NULL
		WHERE id = $1 AND created_at = $2 AND status = 'RETRYING'
	`, jobID, jobCreatedAt)
	if err != nil {
		return false, fmt.Errorf("mark job queued from retry: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// rowScanner is satisfied by both pgx.Row (QueryRow) and pgx.Rows (Query),
// letting scanJob/scanJobRow share one field list instead of drifting.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanJob(row pgx.Row) (Job, error) {
	return scanJobRow(row)
}

func scanJobRow(row rowScanner) (Job, error) {
	var j Job
	err := row.Scan(
		&j.ID, &j.Priority, &j.Repository, &j.Branch, &j.CommitSHA, &j.Labels,
		&j.RequiredCapabilities, &j.Retries, &j.MaxRetries, &j.Status,
		&j.WorkerID, &j.LogRef, &j.CreatedAt, &j.StartedAt, &j.FinishedAt,
	)
	return j, err
}
