CREATE TABLE job_retry_history (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID NOT NULL,
    job_created_at  TIMESTAMPTZ NOT NULL,   -- see jobs partitioning note in 000003
    attempt_number  INTEGER NOT NULL,
    status          job_status NOT NULL,
    worker_id       UUID REFERENCES workers(id) ON DELETE SET NULL,
    error_message   TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ,

    CONSTRAINT fk_retry_job FOREIGN KEY (job_id, job_created_at)
        REFERENCES jobs (id, created_at) ON DELETE CASCADE,
    CONSTRAINT uq_retry_attempt UNIQUE (job_id, attempt_number)
);

CREATE INDEX idx_retry_history_job_id ON job_retry_history (job_id);
