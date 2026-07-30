-- gen_random_uuid() is built into Postgres core since v16 (our target image),
-- no CREATE EXTENSION needed. If you ever run against an older Postgres,
-- `CREATE EXTENSION IF NOT EXISTS pgcrypto;` first.

CREATE TYPE worker_status AS ENUM (
    'REGISTERING', 'READY', 'BUSY', 'DRAINING', 'OFFLINE', 'DEAD'
);

CREATE TYPE job_status AS ENUM (
    'QUEUED', 'ASSIGNED', 'RUNNING', 'SUCCESS', 'FAILED', 'RETRYING', 'CANCELLED'
);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
