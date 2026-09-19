# FlowForge — Distributed Job Orchestrator

[![CI](https://github.com/flowforge/flowforge/actions/workflows/ci.yml/badge.svg)](https://github.com/flowforge/flowforge/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/flowforge/flowforge)](https://goreportcard.com/report/github.com/flowforge/flowforge)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

FlowForge is a resilient, distributed background job processing and orchestration platform built in Go.

---

## Stage 2: Reliable Delivery & Retry Engine

Stage 2 eliminates the distributed dual-write window and provides end-to-end execution reliability:

- **Transactional Outbox Pattern**: Job creation (`jobs`) and outbox event publication (`outbox_events`) are committed atomically within a single PostgreSQL transaction (`pgx.Tx`), decoupling HTTP ingestion from Redis availability.
- **Outbox & Retry Dispatcher Daemon (`cmd/dispatcher`)**: A dedicated background service that concurrently polls `outbox_events` and retry-eligible jobs (`status = 'RETRY_WAIT' AND next_attempt_at <= NOW()`) using `SELECT ... FOR UPDATE SKIP LOCKED` to publish tasks to Redis Streams.
- **Worker Replay Guard & Idempotency**: Workers validate `status == 'QUEUED'` and perform atomic conditional updates (`UPDATE jobs SET status = 'RUNNING', attempt_count = attempt_count + 1 WHERE id = $1 AND status = 'QUEUED'`) to guarantee safety against duplicate or out-of-order Redis deliveries.
- **Immutable Execution History (`job_attempts`)**: Every task execution records a detailed history row with `job_id`, `attempt_number`, `worker_id`, `status`, `error`, `started_at`, and `finished_at`.
- **Explicit Error Classification**: Differentiates retryable errors (`tasks.NewRetryableError`) from permanent failures (`tasks.NewPermanentError`).
- **Exponential Backoff with Full Jitter**: Non-blocking backoff calculation spreading retries to prevent thundering herd spikes.

```
                      +-------------------+
                      |    HTTP Client    |
                      +---------+---------+
                                |
               1. POST /jobs    |    6. GET /jobs/{id}
                                v
                      +---------+---------+
                      |   FlowForge API   |
                      +---------+---------+
                                |
                   2. ATOMIC DB TRANSACTION
                   (INSERT INTO jobs + INSERT INTO outbox_events)
                                v
                 +--------------+--------------+
                 |          PostgreSQL         |
                 |  jobs, outbox_events,       |
                 |  job_attempts               |
                 +-------+--------------+------+
                         |              ^
             3. SELECT   |              | 5. UPDATE
             FOR UPDATE  |              |    (RUNNING,
             SKIP LOCKED |              |     COMPLETED,
                         v              |     RETRY_WAIT)
                 +-------+------+       |
                 |  Dispatcher  |       |
                 +-------+------+       |
                         |              |
               4. XADD   |              |
                         v              |
                 +-------+------+       |
                 | Redis Stream |       |
                 +-------+------+       |
                         |              |
           4. XREADGROUP |              |
                         v              |
                 +-------+------+-------+
                 |   FlowForge Worker   |
                 +----------------------+
```

---

## Job State Machine (Stage 2)

```
[ PENDING ] ──(outbox flush)──> [ QUEUED ] ──(worker pickup)──> [ RUNNING ]
                                    ▲                               ├──(success)─────────────────────────> [ COMPLETED ]
                                    │                               ├──(permanent error OR attempts >= max)─> [ FAILED ]
                                    └───(retry wait expired)────────┴──(retryable error & attempts < max)──> [ RETRY_WAIT ]
```

| State | Description | Next Allowed States |
|---|---|---|
| `PENDING` | Created atomically in DB with outbox event | `QUEUED` |
| `QUEUED` | Published to Redis Stream by Dispatcher | `RUNNING` |
| `RUNNING` | Acquired by Worker via conditional `UPDATE` | `COMPLETED`, `FAILED`, `RETRY_WAIT` |
| `RETRY_WAIT` | Failed transiently; awaiting exponential backoff | `QUEUED` |
| `COMPLETED` | Task finished successfully; result saved | *Terminal* |
| `FAILED` | Permanent error or maximum attempts exhausted | *Terminal* |

---

## Quickstart

### Running with Docker Compose

```bash
# Clone repository
git clone https://github.com/flowforge/flowforge.git
cd flowforge

# Start all services (PostgreSQL, Redis, API, Dispatcher, Worker)
docker compose up --build -d

# Verify all containers are healthy
docker compose ps
```

### Running the Live Stage 2 Demo

```bash
# On Linux / macOS
./scripts/demo-stage2.sh

# On Windows PowerShell
./scripts/demo-stage2.ps1
```

---

## API Reference

### Health & Readiness

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Process liveness probe |
| `GET` | `/readyz` | Dependency readiness probe (PostgreSQL & Redis check) |

### Jobs

#### 1. Submit a Job
```bash
POST /jobs
Content-Type: application/json

{
  "type": "flaky",
  "payload": {},
  "max_attempts": 3
}
```

**Response (`201 Created`):**
```json
{
  "id": "e8a939bf-f166-4f40-a3bc-223450949d01",
  "type": "flaky",
  "status": "PENDING"
}
```

#### 2. Get Job Status & History
```bash
GET /jobs/e8a939bf-f166-4f40-a3bc-223450949d01
```

**Response (`200 OK`):**
```json
{
  "id": "e8a939bf-f166-4f40-a3bc-223450949d01",
  "type": "flaky",
  "payload": {},
  "status": "COMPLETED",
  "attempt_count": 3,
  "max_attempts": 3,
  "result": {
    "flaky_status": "succeeded on attempt 3"
  },
  "created_at": "2026-09-20T10:00:00Z",
  "started_at": "2026-09-20T10:00:04Z",
  "completed_at": "2026-09-20T10:00:04Z"
}
```

---

## Task Types (Stage 2)

| Task Type | Behavior | Classification |
|---|---|---|
| `echo` | Returns the input payload | Success |
| `sleep` | Pauses execution for $N$ seconds | Success |
| `flaky` | Fails on attempts 1 & 2 with retryable errors; succeeds on attempt 3 | Transient Retryable |
| `always_fail` | Returns a retryable error on every attempt until `max_attempts` is reached | Transient Retryable (Exhausts) |
| `permanent_fail` | Returns a permanent error; fails immediately without retrying | Permanent (Non-Retryable) |

---

## Development & Testing

```bash
# Run unit tests with race detector
make test-race

# Run linter
make lint

# Run integration tests (includes full Stage 2 reliability suite)
make test-integration

# Build all binaries (api, dispatcher, worker, migrate)
make build
```

---

## Architectural Decision Records (ADRs)

- [ADR-001: PostgreSQL as the Source of Truth](docs/adr/001-postgresql-source-of-truth.md)
- [ADR-002: Redis Streams for Job Queueing](docs/adr/002-redis-streams-queue.md)
- [ADR-003: Transactional Outbox Pattern for Dual-Write Elimination](docs/adr/003-transactional-outbox.md)
- [ADR-004: At-Least-Once Publication and Worker Replay Guard](docs/adr/004-at-least-once-publication.md)
- [ADR-005: Explicit Error Classification and Exponential Backoff](docs/adr/005-retry-policy.md)
