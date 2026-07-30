CREATE TABLE jobs (
    id                      UUID NOT NULL DEFAULT gen_random_uuid(),
    priority                SMALLINT NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
    repository              TEXT NOT NULL,
    branch                  TEXT NOT NULL,
    commit_sha              TEXT NOT NULL,
    labels                  JSONB NOT NULL DEFAULT '{}',
    required_capabilities   TEXT[] NOT NULL DEFAULT '{}',
    retries                 INTEGER NOT NULL DEFAULT 0,
    max_retries             INTEGER NOT NULL DEFAULT 3,
    status                  job_status NOT NULL DEFAULT 'QUEUED',
    worker_id               UUID REFERENCES workers(id) ON DELETE SET NULL,
    assignment_epoch        BIGINT,
    idempotency_key         TEXT,
    retry_at                TIMESTAMPTZ,     -- when a RETRYING job becomes eligible for re-enqueue (doc 9.3 backoff)
    log_ref                 TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at              TIMESTAMPTZ,
    finished_at             TIMESTAMPTZ,

    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Bootstrap partitions for the current month plus a 3-month runway so a
-- freshly-migrated environment doesn't reject inserts on day one. A
-- scheduled job (cron/pg_partman, see docs/03-database-schema.md 3.3) is
-- responsible for keeping this runway topped up in a running system --
-- this migration only guarantees the initial window exists.
DO $$
DECLARE
    start_month date := date_trunc('month', now())::date;
    i integer;
    part_start date;
    part_end date;
    part_name text;
BEGIN
    FOR i IN 0..3 LOOP
        part_start := (start_month + (i || ' months')::interval)::date;
        part_end   := (start_month + ((i + 1) || ' months')::interval)::date;
        part_name  := 'jobs_' || to_char(part_start, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF jobs FOR VALUES FROM (%L) TO (%L)',
            part_name, part_start, part_end
        );
    END LOOP;
END $$;

CREATE UNIQUE INDEX uq_jobs_idempotency_key ON jobs (idempotency_key, created_at)
    WHERE idempotency_key IS NOT NULL;

-- Scheduler's primary read: next QUEUED jobs, priority then age.
CREATE INDEX idx_jobs_queued_priority ON jobs (priority, created_at)
    WHERE status = 'QUEUED';

-- Backoff poller's read: RETRYING jobs whose retry_at has elapsed.
CREATE INDEX idx_jobs_retry_due ON jobs (retry_at)
    WHERE status = 'RETRYING';

CREATE INDEX idx_jobs_worker_id ON jobs (worker_id)
    WHERE status IN ('ASSIGNED', 'RUNNING');

CREATE INDEX idx_jobs_repo_branch ON jobs (repository, branch, created_at DESC);
CREATE INDEX idx_jobs_commit_sha ON jobs (commit_sha);
CREATE INDEX idx_jobs_created_at_brin ON jobs USING BRIN (created_at);

-- Composite FK (id, created_at) -- see the comment on workers.current_job_created_at
-- for why this can't be a plain FK on jobs(id).
ALTER TABLE workers
    ADD CONSTRAINT fk_workers_current_job
    FOREIGN KEY (current_job_id, current_job_created_at)
    REFERENCES jobs(id, created_at) ON DELETE SET NULL;
