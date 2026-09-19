-- +goose NO TRANSACTION
-- +goose Up

-- 1. Extend job_status ENUM to include RETRY_WAIT
-- Note: ALTER TYPE ... ADD VALUE must run outside a multi-statement transaction in PostgreSQL.
-- +goose StatementBegin
ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'RETRY_WAIT';
-- +goose StatementEnd

-- 2. Add retry & attempt tracking fields to jobs table
-- +goose StatementBegin
ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS last_error TEXT,
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
-- +goose StatementEnd

-- 3. Update status constraint for started_at
-- +goose StatementBegin
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS chk_started_valid_status;
ALTER TABLE jobs ADD CONSTRAINT chk_started_valid_status
    CHECK (started_at IS NULL OR status IN ('RUNNING', 'COMPLETED', 'FAILED', 'RETRY_WAIT'));
-- +goose StatementEnd

-- 4. Index for scheduler / dispatcher retry poller (RETRY_WAIT status)
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_jobs_retry_poll ON jobs (next_attempt_at ASC)
    WHERE status = 'RETRY_WAIT';
-- +goose StatementEnd

-- 5. Create outbox_events table for Transactional Outbox pattern
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS outbox_events (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id  UUID        NOT NULL,
    event_type    TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    status        TEXT        NOT NULL DEFAULT 'PENDING',
    attempts      INT         NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending ON outbox_events (created_at ASC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_outbox_events_aggregate ON outbox_events (aggregate_id);
-- +goose StatementEnd

-- 6. Create job_attempts table for immutable execution attempt history
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS job_attempts (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INT         NOT NULL,
    worker_id      TEXT,
    status         TEXT        NOT NULL,
    error          TEXT,
    started_at     TIMESTAMPTZ NOT NULL,
    finished_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts (job_id, attempt_number ASC);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS outbox_events;
DROP INDEX IF EXISTS idx_jobs_retry_poll;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS attempt_count,
    DROP COLUMN IF EXISTS max_attempts,
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS next_attempt_at;
-- +goose StatementEnd
