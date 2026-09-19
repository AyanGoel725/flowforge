-- +goose Up
-- +goose StatementBegin

-- Job status enum.
-- Stage 1 valid transitions:
--   PENDING → QUEUED
--   QUEUED  → RUNNING
--   RUNNING → COMPLETED
--   RUNNING → FAILED
CREATE TYPE job_status AS ENUM (
    'PENDING',
    'QUEUED',
    'RUNNING',
    'COMPLETED',
    'FAILED'
);

CREATE TABLE jobs (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    status        job_status  NOT NULL DEFAULT 'PENDING',
    result        JSONB,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,

    -- Guard: completed_at requires started_at
    CONSTRAINT chk_completed_requires_started
        CHECK (completed_at IS NULL OR started_at IS NOT NULL),

    -- Guard: started_at requires status beyond PENDING/QUEUED
    CONSTRAINT chk_started_valid_status
        CHECK (started_at IS NULL OR status IN ('RUNNING', 'COMPLETED', 'FAILED')),

    -- Guard: error only when FAILED
    CONSTRAINT chk_error_only_when_failed
        CHECK (error IS NULL OR status = 'FAILED'),

    -- Guard: result only when COMPLETED
    CONSTRAINT chk_result_only_when_completed
        CHECK (result IS NULL OR status = 'COMPLETED')
);

-- Index for listing jobs by status (worker queries, API filtering).
CREATE INDEX idx_jobs_status ON jobs (status);

-- Index for pagination by creation time.
CREATE INDEX idx_jobs_created_at ON jobs (created_at DESC);

-- Composite index for listing QUEUED jobs oldest first (worker pickup).
CREATE INDEX idx_jobs_status_created ON jobs (status, created_at ASC)
    WHERE status IN ('PENDING', 'QUEUED');

-- +goose StatementEnd

-- +goose Down
DROP TABLE IF EXISTS jobs;
DROP TYPE IF EXISTS job_status;
