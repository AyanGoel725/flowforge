# AGENTS.md — Agent & Developer Guide for FlowForge

This document establishes the architecture, invariants, boundaries, and developer workflows for FlowForge. Any agent working on this codebase must adhere strictly to these principles.

---

## 1. Project Overview & Stage Scope

FlowForge is a resilient distributed background job processing and orchestration engine built in Go.

### Current Scope: Stage 1 (Async Task Execution Engine)
Stage 1 implements the fundamental asynchronous job execution pipeline:
- Ingesting jobs via a REST HTTP API (`/jobs`, `/jobs/{id}`, `/healthz`, `/readyz`).
- Persisting jobs as the single source of truth in PostgreSQL (`jobs` table).
- Asynchronous task queuing using Redis Streams (`XADD`, `XREADGROUP`, `XACK`).
- Single-message worker execution loop dispatching to registered task handlers (`echo`, `sleep`).
- Clean error recovery, panic safety, and optimistic concurrency state transitions.

### ⚠️ Constraints: Out of Scope for Stage 1
Do **NOT** implement features slated for future stages:
- **Stage 2**: Transactional Outbox pattern, automated retries with exponential backoff, worker leases/heartbeats, dead-letter queue (DLQ), priority queues, idempotency keys.
- **Stage 3+**: DAG workflows, multi-node dynamic worker pools, Kafka integration, Kubernetes CRDs.

---

## 2. Architecture & Invariants

### 2.1 State Machine
The job lifecycle follows a strict state progression:
```
[ PENDING ] ──(queue)──> [ QUEUED ] ──(pickup)──> [ RUNNING ]
                                                    ├──(success)──> [ COMPLETED ]
                                                    └──(failure)──> [ FAILED ]
```
Valid state transitions are governed by `jobs.ValidTransition(from, to)`:
- `PENDING -> QUEUED`
- `QUEUED -> RUNNING`
- `RUNNING -> COMPLETED`
- `RUNNING -> FAILED`

All other transitions are invalid and must return `jobs.ErrInvalidState`.

### 2.2 Dual-Write Mitigation (Stage 1)
When creating a job:
1. `INSERT` record in PostgreSQL with status `PENDING`.
2. `XADD` payload pointer to Redis Stream (`flowforge:jobs`).
3. `UPDATE` status to `QUEUED` in PostgreSQL upon successful publish.
4. If Redis publishing fails, return an error and leave the record in `PENDING` (to be reconciled or inspected).

### 2.3 Optimistic Concurrency & Repository Error Semantics
All state modifications in `internal/jobs/repository.go` use optimistic concurrency via:
```sql
UPDATE jobs SET status = $3, ... WHERE id = $1 AND status = $2
```
If 0 rows are updated:
- The repository executes a `SELECT EXISTS(...)` query.
- If the job does not exist, return `jobs.ErrJobNotFound`.
- If the job exists in a different status, return `jobs.ErrInvalidState`.

### 2.4 Redis Stream Retention & Concurrency
- `XADD` includes `MaxLen: 10000, Approx: true` to prevent unbounded memory growth in Redis while keeping recent message history for diagnostics.
- `XREADGROUP` uses `Count: 1` and `Block: 2s` for sequential processing per worker instance.
- Messages are acknowledged (`XACK`) only after the terminal status (`COMPLETED` or `FAILED`) has been persisted in PostgreSQL.

### 2.5 Worker Panic Recovery & Graceful Shutdown
- Every worker message processing loop is wrapped with a `defer recover()` handler.
- If a task panics, the worker logs the full stack trace with `slog.Error`, records `StatusFailed` with a sanitized message (`"task execution panicked: ..."`) in PostgreSQL, acknowledges the message with `XACK`, and continues serving subsequent messages without crashing the worker daemon.
- On graceful shutdown / context cancellation during task execution, the worker uses a bounded 5-second context (`context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)`) strictly to ensure terminal state persistence and `XACK` complete cleanly.

### 2.6 Decoupled Readiness Probes
The HTTP API readiness probe (`/readyz`) depends on the abstract `api.Pinger` interface rather than concrete database/cache clients:
```go
type Pinger interface {
    Ping(ctx context.Context) error
}
```

---

## 3. Package Structure

```
flowforge/
├── cmd/
│   ├── api/main.go               # HTTP API server entry point
│   ├── migrate/main.go           # Database schema migration entry point
│   └── worker/main.go            # Worker daemon entry point
├── internal/
│   ├── api/                      # REST routes, handlers, middleware, Pinger interface
│   ├── config/                   # Strongly typed environment configuration
│   ├── database/                 # PostgreSQL pool and embedded Goose migrations
│   ├── jobs/                     # Domain types, state machine, repository, service
│   ├── queue/                    # Queue abstraction (Publisher, Consumer interfaces & Redis impl)
│   ├── tasks/                    # Task Handler interface, Registry, Echo & Sleep handlers
│   └── worker/                   # Worker loop, execution dispatcher, panic safety
├── migrations/                   # SQL Goose migrations
├── scripts/                      # PowerShell and Bash live demo scripts
├── tests/
│   └── integration/              # E2E integration test suite
```

---

## 4. Developer & Verification Workflows

### Run Unit Tests & Race Detection
```bash
go test -v -race ./...
```

### Run Integration Tests
*Requires active PostgreSQL and Redis instances (or via Docker Compose)*
```bash
go test -v -tags=integration ./tests/integration/...
```

### Static Analysis & Linting
```bash
go vet ./...
gofmt -l .
```

### Local Docker Stack
```bash
# Start all services with automated migration execution
docker compose up --build -d

# Check service status
docker compose ps

# Run demo script
./scripts/demo.sh       # On Linux/macOS
./scripts/demo.ps1      # On Windows PowerShell
```
