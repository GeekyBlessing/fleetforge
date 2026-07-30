CREATE TABLE workers (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname            TEXT NOT NULL,
    instance_id         TEXT NOT NULL,
    os                  TEXT NOT NULL,
    cpu_cores           INTEGER NOT NULL CHECK (cpu_cores > 0),
    memory_mb           INTEGER NOT NULL CHECK (memory_mb > 0),
    labels              JSONB NOT NULL DEFAULT '{}',
    capabilities        TEXT[] NOT NULL DEFAULT '{}',
    status              worker_status NOT NULL DEFAULT 'REGISTERING',
    current_job_id      UUID,                      -- FK added in 000003 once jobs exists
    current_job_created_at TIMESTAMPTZ,             -- required alongside current_job_id: jobs is partitioned by
                                                      -- created_at, so Postgres can only enforce a FK against its
                                                      -- (id, created_at) composite key, never (id) alone
    last_heartbeat      TIMESTAMPTZ,
    version             TEXT NOT NULL,
    capacity_slots      INTEGER NOT NULL DEFAULT 1 CHECK (capacity_slots > 0),
    available_capacity  INTEGER NOT NULL DEFAULT 1 CHECK (available_capacity >= 0),
    epoch               BIGINT NOT NULL DEFAULT 1,
    registered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_workers_instance_id UNIQUE (instance_id)
);

-- Scheduler's hottest read: READY/BUSY candidates.
CREATE INDEX idx_workers_status_ready
    ON workers (status)
    WHERE status IN ('READY', 'BUSY');

-- Reaper's sweep: staleness check, only over workers that could plausibly be stale.
CREATE INDEX idx_workers_last_heartbeat
    ON workers (last_heartbeat)
    WHERE status NOT IN ('OFFLINE', 'DEAD');

CREATE INDEX idx_workers_labels_gin ON workers USING GIN (labels);
CREATE INDEX idx_workers_capabilities_gin ON workers USING GIN (capabilities);

CREATE TRIGGER trg_workers_updated_at
    BEFORE UPDATE ON workers
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
