# Stage 1 — Basic Job Execution

## Objective

Build the smallest useful version of FlowForge: a system where a client submits a background job through a REST API, a worker executes it asynchronously, and the client can query the result.

## Architecture

```
Client
  │
  ▼
┌──────────┐     ┌────────────┐     ┌───────────────┐
│  REST API │────▶│ PostgreSQL │◀────│    Worker      │
│  (Chi)    │     │ (pgx)      │     │ (consumer)     │
└──────────┘     └────────────┘     └───────────────┘
  │                                       ▲
  │         ┌──────────────────┐          │
  └────────▶│  Redis Stream    │──────────┘
            │  (flowforge:jobs)│
            └──────────────────┘
```

See `docs/architecture/stage-01.md` for the full Mermaid diagram.

## Components

| Component | Location | Purpose |
|-----------|----------|---------|
| API server | `cmd/api/` | HTTP endpoints for job submission, retrieval, listing, health |
| Worker | `cmd/worker/` | Consumes Redis stream, executes tasks, updates job state |
| Job service | `internal/jobs/` | Business logic: validation, state transitions, create/get/list |
| Repository | `internal/jobs/` | PostgreSQL CRUD for jobs |
| Queue | `internal/queue/` | Redis Streams abstraction (publish + consume) |
| Task system | `internal/tasks/` | Task handler interface, registry, built-in handlers |
| Database | `internal/database/` | Connection management, migration runner |
| Config | `internal/config/` | Environment variable loading |

## Job Lifecycle

```
POST /jobs { type: "echo", payload: { message: "hi" } }
    │
    ├─1─▶ Validate request
    ├─2─▶ INSERT into PostgreSQL (status = PENDING)
    ├─3─▶ XADD to Redis stream
    ├─4─▶ UPDATE status to QUEUED
    └─5─▶ Return { job_id, status: "QUEUED" }

Worker:
    ├─1─▶ XREADGROUP (blocks until message arrives)
    ├─2─▶ Load job from PostgreSQL
    ├─3─▶ UPDATE status to RUNNING, set started_at
    ├─4─▶ Execute task handler
    ├─5─▶ On success: UPDATE status to COMPLETED, store result, set completed_at
    │     On failure: UPDATE status to FAILED, store error, set completed_at
    └─6─▶ XACK the Redis message
```

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/jobs` | Create a new job |
| GET | `/jobs/{id}` | Get a job by ID |
| GET | `/jobs` | List jobs (paginated) |
| GET | `/healthz` | Liveness probe |
| GET | `/readyz` | Readiness probe (checks PostgreSQL + Redis) |

See `docs/api/openapi.yaml` for the full specification.

## Database Schema

```sql
CREATE TYPE job_status AS ENUM ('PENDING','QUEUED','RUNNING','COMPLETED','FAILED');

CREATE TABLE jobs (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}',
    status        job_status  NOT NULL DEFAULT 'PENDING',
    result        JSONB,
    error         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);
```

Indexes: `status`, `created_at DESC`, composite `(status, created_at)` for queued job lookup.

Constraints enforce:
- `completed_at` requires `started_at`
- `started_at` requires status ∈ {RUNNING, COMPLETED, FAILED}
- `error` only when FAILED
- `result` only when COMPLETED

Migration tool: **goose** (lightweight, SQL-based, no ORM dependency).

## Redis Model

- Stream: `flowforge:jobs`
- Consumer group: `flowforge-workers`
- Message fields: `job_id`, `type`

Commands used: `XADD`, `XREADGROUP`, `XACK`.

Commands NOT used in Stage 1: `XAUTOCLAIM`, `XCLAIM`, `XPENDING`.

PostgreSQL remains the source of truth. Redis messages are pointers, not copies of state.

## Worker Model

- Single worker process (`cmd/worker`).
- Single consumer in the `flowforge-workers` group.
- Blocking read with `XREADGROUP` (`BLOCK 5000`).
- On receive: loads job from PostgreSQL → runs task handler → updates state → acks message.
- Uses a task registry to dispatch by job type.

## State Transitions

```
PENDING ──▶ QUEUED ──▶ RUNNING ──┬──▶ COMPLETED
                                  └──▶ FAILED
```

Invalid transitions are rejected by the service layer. The database constraints provide a secondary guard.

## Task Handlers

| Type | Behavior | Payload |
|------|----------|---------|
| `echo` | Returns the payload message | `{ "message": "..." }` |
| `sleep` | Sleeps for N seconds, then returns | `{ "seconds": N }` |

All handlers implement:
```go
type Handler interface {
    Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)
}
```

## Known Limitations

Stage 1 deliberately does **NOT** provide:

1. **Crash recovery** — if a worker dies mid-execution, the job stays RUNNING forever. No `XAUTOCLAIM` or lease mechanism.
2. **Duplicate execution** — no idempotency keys or deduplication.
3. **Dual-write consistency** — if Redis publish fails after PostgreSQL insert, the job remains PENDING with no queue message. No transactional outbox.
4. **Retry/backoff** — failed jobs stay FAILED; no automatic retry.
5. **Dead-letter queue** — no DLQ for poison messages.
6. **Worker pools** — single goroutine, single consumer.
7. **Scheduling/priorities** — FIFO only.
8. **Authentication/authorization** — open endpoints.
9. **Rate limiting** — no throttling.
10. **Observability** — structured logs only; no metrics or tracing.

These are planned for later stages. See the roadmap in the README.

## Testing

### Unit tests
- Job validation, state transitions, task handlers, service logic, registry.

### Integration tests
- Full flow: POST → PostgreSQL → Redis → Worker → COMPLETED.
- Failure flow: bad task → FAILED → error stored.

### API tests
- 201/200 success, 400 bad request, 404 not found.

### Race detection
- `go test -race ./...`

## How to Run Locally

Prerequisites: Go 1.23+, Docker, Docker Compose.

```bash
# Start infrastructure
docker compose up -d postgres redis

# Run migrations
go run ./cmd/api --migrate  # or: goose -dir migrations postgres "$DATABASE_URL" up

# Start API
make run-api

# Start worker (separate terminal)
make run-worker

# Test
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type":"echo","payload":{"message":"hello"}}'
```

## How to Run with Docker

```bash
docker compose up --build
```

This starts: API, worker, PostgreSQL, Redis. The API is available at `http://localhost:8080`.

## Acceptance Criteria

- [ ] `docker compose up --build` starts the system
- [ ] POST /jobs creates a job
- [ ] Job appears in PostgreSQL
- [ ] Job is published to Redis
- [ ] Worker consumes and executes
- [ ] COMPLETED jobs have results
- [ ] FAILED jobs have errors
- [ ] GET /jobs/{id} returns correct state
- [ ] GET /jobs supports pagination
- [ ] /healthz and /readyz work
- [ ] Integration tests pass
- [ ] `go test -race ./...` passes
- [ ] CI passes
- [ ] Documentation complete
- [ ] Demo script works
