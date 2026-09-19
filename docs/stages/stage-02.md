# Stage 2 — Reliable Delivery & Retry Engine

## Objective

Eliminate distributed dual-write failure modes and provide reliable job execution with transactional outbox publication, at-least-once message processing with worker replay guards, immutable execution attempt tracking, error classification, and exponential backoff retries with full jitter.

## Architecture

```
Client
  │
  ▼ POST /jobs
┌──────────────┐
│  REST API    │
│  (cmd/api)   │
└──────────────┘
  │ (atomic TX: INSERT job + INSERT outbox_event)
  ▼
┌─────────────────────────────────────────────────────────────┐
│ PostgreSQL                                                  │
│                                                             │
│  ┌────────────────────────┐    ┌─────────────────────────┐  │
│  │ jobs (PENDING/RETRY)   │    │ outbox_events (PENDING) │  │
│  └────────────────────────┘    └─────────────────────────┘  │
│               ▲                             ▲               │
└───────────────┼─────────────────────────────┼───────────────┘
                │                             │
                │ (SELECT ... FOR UPDATE SKIP LOCKED)
                ▼                             ▼
       ┌─────────────────────────────────────────────┐
       │   Outbox & Retry Dispatcher                 │
       │   (cmd/dispatcher)                          │
       └─────────────────────────────────────────────┘
                │
                │ XADD (flowforge:jobs)
                ▼
       ┌─────────────────────────────────────────────┐
       │   Redis Stream (flowforge:jobs)             │
       └─────────────────────────────────────────────┘
                │
                │ XREADGROUP (flowforge-workers)
                ▼
       ┌─────────────────────────────────────────────┐
       │   Worker Pool (cmd/worker)                  │
       │   - Replay Guard (status == QUEUED)         │
       │   - Task Execution & Attempt Recording      │
       │   - Retry / Failure Evaluation              │
       └─────────────────────────────────────────────┘
                │
                ▼ (UPDATE status, INSERT job_attempts)
       PostgreSQL (jobs, job_attempts)
```

See `docs/architecture/stage-02.md` for the full Mermaid diagrams and sequence flows.

## Core Capabilities Added in Stage 2

### 1. Transactional Outbox Pattern (`outbox_events`)
- `POST /jobs` inserts the job (`status = 'PENDING'`) and an outbox event (`event_type = 'JOB_CREATED'`, `status = 'PENDING'`) within a single database transaction (`pgx.Tx`).
- The API handler never interacts with Redis, eliminating the Stage 1 dual-write window. If Redis is unavailable, job submissions continue unaffected.

### 2. Outbox & Retry Dispatcher (`cmd/dispatcher`)
- Background daemon running a concurrent polling loop.
- Dispatches pending outbox events (`status = 'PENDING'`) to Redis Streams with batching and `SELECT ... FOR UPDATE SKIP LOCKED`.
- Dispatches retry-eligible jobs (`status = 'RETRY_WAIT' AND next_attempt_at <= NOW()`) to Redis Streams and atomically marks them `QUEUED`.

### 3. Worker Replay Guard & State Machine Enforcers
- Workers check `status == 'QUEUED'` before executing a job from Redis Streams.
- Workers atomically execute `UPDATE jobs SET status = 'RUNNING', started_at = now(), attempt_count = attempt_count + 1 WHERE id = $1 AND status = 'QUEUED'`.
- Duplicate, stale, or already-completed deliveries are immediately acknowledged (`XACK`) and skipped without double execution.

### 4. Immutable Execution History (`job_attempts`)
- Every execution attempt creates a dedicated row in `job_attempts` recording `job_id`, `attempt_number`, `worker_id`, `status` (`RUNNING`, `COMPLETED`, `FAILED`), `error`, `started_at`, and `finished_at`.

### 5. Explicit Error Classification & Exponential Backoff
- `TaskError` interface with `IsRetryable() bool`.
- `NewRetryableError()`: triggers transition to `RETRY_WAIT` if `attempt_count < max_attempts`.
- `NewPermanentError()`: triggers immediate transition to `FAILED`.
- Full Jitter exponential backoff: $\text{delay} = \text{rand}(0, \min(\text{base} \times 2^{\text{attempt}-1}, \text{max}))$.

### 6. Deterministic Test Task Handlers
- `flaky`: Fails with retryable error on attempts 1 and 2, succeeds on attempt 3.
- `always_fail`: Fails with retryable error on every attempt until `max_attempts` is exhausted (`FAILED`).
- `permanent_fail`: Fails immediately with a non-retryable permanent error.

## Database Schema Updates (Migration `00002_stage2_reliable_delivery.sql`)

```sql
-- Status enum expansion
ALTER TYPE job_status ADD VALUE IF NOT EXISTS 'RETRY_WAIT';

-- Jobs table modifications
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_error TEXT;

-- Outbox events table
CREATE TABLE IF NOT EXISTS outbox_events (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type    VARCHAR(64) NOT NULL,
    payload       JSONB NOT NULL,
    status        VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_events_status_created ON outbox_events(status, created_at);

-- Job attempts table
CREATE TABLE IF NOT EXISTS job_attempts (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id         UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    worker_id      VARCHAR(128),
    status         VARCHAR(32) NOT NULL,
    error          TEXT,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    CONSTRAINT uq_job_attempts_job_attempt UNIQUE (job_id, attempt_number)
);
CREATE INDEX IF NOT EXISTS idx_job_attempts_job_id ON job_attempts(job_id, attempt_number);
```

## State Transitions in Stage 2

```
       ┌───────────┐
       │  PENDING  │
       └─────┬─────┘
             │ (Dispatcher publishes outbox event)
             ▼
       ┌───────────┐
 ┌────▶│  QUEUED   │
 │     └─────┬─────┘
 │           │ (Worker acquires via conditional UPDATE)
 │           ▼
 │     ┌───────────┐
 │     │  RUNNING  │
 │     └──┬───┬───┬┘
 │        │   │   └──────────────────────────┐
 │ (Success)  │ (Permanent Error OR          │ (Retryable Error &
 │        │   │  Attempt >= MaxAttempts)     │  Attempt < MaxAttempts)
 │        ▼   ▼                              ▼
 │   ┌───────────┐                     ┌────────────┐
 │   │ COMPLETED │ / FAILED            │ RETRY_WAIT │
 │   └───────────┘                     └─────┬──────┘
 │                                           │ (Dispatcher checks
 └───────────────────────────────────────────┘  next_attempt_at <= now())
```

## Testing & Verification

Stage 2 introduces comprehensive automated integration testing in `tests/integration/job_flow_test.go`:

1. **Transactional Outbox Atomicity**: Proves a job and its outbox event are created atomically in PostgreSQL.
2. **Normal Outbox Publication Flow**: Confirms end-to-end execution of `echo` jobs through the dispatcher.
3. **Outbox Resilience to Redis Outage**: Submits jobs while Redis is stopped; restarts Redis and verifies jobs are published and completed.
4. **Duplicate Delivery Protection**: Simulates duplicate Redis stream deliveries; verifies only one execution occurs.
5. **Retry Mechanism with Transient Failure (`flaky`)**: Verifies 3 attempts recorded in `job_attempts`, status transitions through `RETRY_WAIT`, and final `COMPLETED` state.
6. **Retry Exhaustion (`always_fail`)**: Verifies retries up to `max_attempts` followed by final `FAILED` status.
7. **Permanent Failure Fast-Path (`permanent_fail`)**: Verifies immediate transition to `FAILED` without retries.
8. **Exponential Backoff Timing & Jitter**: Verifies calculated backoff intervals match exponential scaling with full jitter.

## How to Run Locally

```bash
# Start PostgreSQL & Redis
docker compose up -d postgres redis

# Run migrations
go run ./cmd/migrate

# Start API Server (Terminal 1)
make run-api

# Start Outbox Dispatcher (Terminal 2)
make run-dispatcher

# Start Worker (Terminal 3)
make run-worker

# Submit a flaky job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"flaky","payload":{}}'
```
