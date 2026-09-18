# FlowForge — Stage 1 Implementation Plan

## Overview

This document is the step-by-step blueprint for implementing Stage 1 of FlowForge.
Every file is listed. Every interface is sketched. Every design decision is explained.

---

## 1. Repository Bootstrap

### Files
| File | Purpose |
|------|---------|
| `go.mod` | Module `github.com/flowforge/flowforge`, Go 1.23 |
| `go.sum` | Dependency checksums (generated) |
| `.gitignore` | Ignore binaries, `.env`, IDE files, coverage |
| `.env.example` | Template for environment variables |
| `Makefile` | Build, test, lint, docker commands |
| `Dockerfile` | Multi-stage build for API + worker |
| `docker-compose.yml` | API, worker, PostgreSQL, Redis |
| `.github/workflows/ci.yml` | CI pipeline |

### Dependencies
| Package | Version | Why |
|---------|---------|-----|
| `github.com/go-chi/chi/v5` | v5.1.0 | Lightweight router, stdlib-compatible |
| `github.com/jackc/pgx/v5` | v5.7.2 | Native PostgreSQL driver, pgxpool |
| `github.com/google/uuid` | v1.6.0 | UUID generation |
| `github.com/redis/go-redis/v9` | v9.7.0 | Redis client with Streams support |
| `github.com/pressly/goose/v3` | v3.24.1 | SQL migration runner |

### Design decisions
- **Chi over net/http**: Chi adds method routing and URL params without pulling in a framework. It implements `http.Handler` so it composes with stdlib middleware.
- **pgx over database/sql**: pgx is the best Go PostgreSQL driver — native protocol, connection pooling via pgxpool, JSONB support, COPY support for future stages.
- **goose over golang-migrate**: goose uses plain SQL files (no Go code required), sequential versioning, and a simple CLI. It can also run programmatically via `goose.Up()`.

---

## 2. Configuration (`internal/config`)

### File: `internal/config/config.go`

```go
type Config struct {
    DatabaseURL        string // DATABASE_URL
    RedisURL           string // REDIS_URL
    RedisStream        string // REDIS_STREAM (default: "flowforge:jobs")
    RedisConsumerGroup string // REDIS_CONSUMER_GROUP (default: "flowforge-workers")
    ServerPort         string // SERVER_PORT (default: "8080")
}

func Load() (*Config, error)
```

Reads from `os.Getenv`. No external config library — stdlib is sufficient for Stage 1.
Validates that required values are set (`DATABASE_URL`, `REDIS_URL`).

---

## 3. Database Layer (`internal/database`)

### File: `internal/database/postgres.go`

```go
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error)
func RunMigrations(databaseURL string) error
```

- `Connect` creates a `pgxpool.Pool` with sensible defaults.
- `RunMigrations` runs goose migrations from the `migrations/` directory.

### File: `migrations/001_create_jobs.sql`

Already created — see the migration file.

---

## 4. Job Domain (`internal/jobs`)

### File: `internal/jobs/model.go`

```go
type Status string

const (
    StatusPending   Status = "PENDING"
    StatusQueued    Status = "QUEUED"
    StatusRunning   Status = "RUNNING"
    StatusCompleted Status = "COMPLETED"
    StatusFailed    Status = "FAILED"
)

type Job struct {
    ID          uuid.UUID        `json:"id"`
    Type        string           `json:"type"`
    Payload     json.RawMessage  `json:"payload"`
    Status      Status           `json:"status"`
    Result      json.RawMessage  `json:"result,omitempty"`
    Error       *string          `json:"error,omitempty"`
    CreatedAt   time.Time        `json:"created_at"`
    StartedAt   *time.Time       `json:"started_at,omitempty"`
    CompletedAt *time.Time       `json:"completed_at,omitempty"`
}
```

### File: `internal/jobs/transitions.go`

```go
// ValidTransition returns true if from → to is a legal state change.
func ValidTransition(from, to Status) bool

// Returns the map:
//   PENDING   → {QUEUED}
//   QUEUED    → {RUNNING}
//   RUNNING   → {COMPLETED, FAILED}
```

### File: `internal/jobs/validation.go`

```go
type CreateJobRequest struct {
    Type    string          `json:"type"`
    Payload json.RawMessage `json:"payload"`
}

func ValidateCreateRequest(req *CreateJobRequest) error
```

Validates: `type` is non-empty, `payload` is valid JSON, body is under 1MB.

### File: `internal/jobs/repository.go`

```go
type Repository interface {
    Create(ctx context.Context, job *Job) error
    GetByID(ctx context.Context, id uuid.UUID) (*Job, error)
    List(ctx context.Context, limit, offset int) ([]*Job, int, error)
    UpdateStatus(ctx context.Context, id uuid.UUID, from, to Status) error
    SetRunning(ctx context.Context, id uuid.UUID) error
    SetCompleted(ctx context.Context, id uuid.UUID, result json.RawMessage) error
    SetFailed(ctx context.Context, id uuid.UUID, errMsg string) error
}
```

### File: `internal/jobs/postgres_repository.go`

Implements `Repository` using pgxpool. All queries use parameterized statements.

`UpdateStatus` uses optimistic concurrency:
```sql
UPDATE jobs SET status = $1 WHERE id = $2 AND status = $3
```
Returns an error if 0 rows affected (concurrent modification or invalid transition).

### File: `internal/jobs/service.go`

```go
type Service struct {
    repo      Repository
    publisher queue.Publisher
}

func NewService(repo Repository, publisher queue.Publisher) *Service

func (s *Service) CreateJob(ctx context.Context, req *CreateJobRequest) (*Job, error)
func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (*Job, error)
func (s *Service) ListJobs(ctx context.Context, limit, offset int) ([]*Job, int, error)
```

`CreateJob` flow:
1. Validate request.
2. Create job in PostgreSQL (PENDING).
3. Publish `{job_id, type}` to Redis.
4. Update status to QUEUED.
5. If Redis publish fails, log the error and return the job as PENDING (documented dual-write limitation).

---

## 5. Queue Abstraction (`internal/queue`)

### File: `internal/queue/queue.go`

```go
type Message struct {
    ID    string // Redis message ID
    JobID uuid.UUID
    Type  string
}

type Publisher interface {
    Publish(ctx context.Context, jobID uuid.UUID, jobType string) error
}

type Consumer interface {
    Consume(ctx context.Context) (<-chan Message, error)
    Ack(ctx context.Context, messageID string) error
}
```

### File: `internal/queue/redis_publisher.go`

Implements `Publisher` using `XADD`.

### File: `internal/queue/redis_consumer.go`

Implements `Consumer` using `XREADGROUP` with `BLOCK 5000ms`.
Creates the consumer group on startup (`XGROUP CREATE ... MKSTREAM`).

---

## 6. Task System (`internal/tasks`)

### File: `internal/tasks/handler.go`

```go
type Handler interface {
    Handle(ctx context.Context, payload json.RawMessage) (json.RawMessage, error)
}
```

### File: `internal/tasks/registry.go`

```go
type Registry struct {
    handlers map[string]Handler
}

func NewRegistry() *Registry
func (r *Registry) Register(name string, handler Handler)
func (r *Registry) Get(name string) (Handler, error)
```

### File: `internal/tasks/echo.go`

```go
type EchoHandler struct{}

// Expects: {"message": "..."} — returns the same payload.
```

### File: `internal/tasks/sleep.go`

```go
type SleepHandler struct{}

// Expects: {"seconds": N} — sleeps for N seconds (capped at 300), returns {"slept_seconds": N}.
```

Validates:
- `message` is required and non-empty for echo.
- `seconds` is required, must be > 0 and ≤ 300 for sleep.

---

## 7. Worker (`internal/worker`)

### File: `internal/worker/worker.go`

```go
type Worker struct {
    consumer queue.Consumer
    repo     jobs.Repository
    registry *tasks.Registry
    logger   *slog.Logger
}

func New(consumer queue.Consumer, repo jobs.Repository, registry *tasks.Registry, logger *slog.Logger) *Worker

func (w *Worker) Run(ctx context.Context) error
func (w *Worker) processJob(ctx context.Context, msg queue.Message) error
```

`processJob` flow:
1. Load job from PostgreSQL.
2. Verify job is QUEUED (guard against replays).
3. Set status to RUNNING + started_at.
4. Look up handler in task registry.
5. Execute handler.
6. On success: set COMPLETED + result + completed_at.
7. On failure: set FAILED + error + completed_at.
8. Ack the Redis message.

---

## 8. API Server (`internal/api`)

### File: `internal/api/router.go`

```go
func NewRouter(service *jobs.Service, pool *pgxpool.Pool, redis *redis.Client, logger *slog.Logger) http.Handler
```

Registers:
- `POST /jobs` → `handleCreateJob`
- `GET /jobs/{id}` → `handleGetJob`
- `GET /jobs` → `handleListJobs`
- `GET /healthz` → `handleHealth`
- `GET /readyz` → `handleReady`

Middleware:
- Request ID (UUID per request)
- Structured logging (request method, path, status, duration)
- Recovery (panic → 500)
- Content-Type enforcement (application/json for POST)
- Request body size limit (1MB)

### File: `internal/api/handlers.go`

Each handler:
- Parses input (JSON body, path params, query params).
- Calls the service layer.
- Maps service errors to HTTP status codes.
- Returns JSON responses.

### File: `internal/api/errors.go`

```go
type APIError struct {
    StatusCode int    `json:"-"`
    Error      string `json:"error"`
    Code       string `json:"code,omitempty"`
}
```

Mapping:
- Validation error → 400
- Not found → 404
- Invalid UUID → 400
- Internal error → 500 (generic message, no leak)

### File: `internal/api/middleware.go`

Lightweight middleware: logging, recovery, request ID, content-type check.

---

## 9. Entry Points

### File: `cmd/api/main.go`

1. Load config.
2. Connect to PostgreSQL (with retry on startup).
3. Run migrations.
4. Connect to Redis.
5. Create repository, publisher, service.
6. Create router.
7. Start HTTP server with graceful shutdown (SIGINT/SIGTERM).

### File: `cmd/worker/main.go`

1. Load config.
2. Connect to PostgreSQL.
3. Connect to Redis.
4. Create consumer, repository, task registry.
5. Register echo + sleep handlers.
6. Create and run worker.
7. Graceful shutdown on SIGINT/SIGTERM.

---

## 10. Docker

### File: `Dockerfile`

Multi-stage build:
```
Stage 1: golang:1.23-alpine — build API and worker binaries
Stage 2: alpine:3.20 — copy binaries, run as non-root
```

Build args select the target binary (`api` or `worker`).

### File: `docker-compose.yml`

Services:
- `postgres` (postgres:16-alpine, port 5432, volume for data)
- `redis` (redis:7-alpine, port 6379)
- `api` (build from Dockerfile, port 8080, depends on postgres + redis)
- `worker` (build from Dockerfile, depends on postgres + redis)

Health checks:
- `postgres`: `pg_isready`
- `redis`: `redis-cli ping`

---

## 11. Makefile

```makefile
build:          go build ./cmd/api ./cmd/worker
test:           go test ./...
test-race:      go test -race ./...
test-integration: go test -tags=integration ./tests/integration/...
lint:           go vet ./... && gofmt check
run-api:        go run ./cmd/api
run-worker:     go run ./cmd/worker
docker-up:      docker compose up --build -d
docker-down:    docker compose down -v
migrate:        goose -dir migrations postgres "$DATABASE_URL" up
```

---

## 12. CI (`.github/workflows/ci.yml`)

Triggers: push to `main`, pull requests.

Jobs:
1. **lint** — `go vet`, `gofmt -l`
2. **test** — unit tests + race detection
3. **build** — compile API + worker
4. **integration** — spin up PostgreSQL + Redis services, run integration tests
5. **docker** — `docker build`

---

## 13. Testing Strategy

### Unit tests (no external dependencies)

| File | Tests |
|------|-------|
| `internal/jobs/validation_test.go` | Empty type, nil payload, oversized payload, valid |
| `internal/jobs/transitions_test.go` | All valid transitions, all invalid transitions |
| `internal/tasks/echo_test.go` | Valid echo, missing message, empty message |
| `internal/tasks/sleep_test.go` | Valid sleep, negative seconds, zero, over-cap, missing field |
| `internal/tasks/registry_test.go` | Register, get existing, get unknown |
| `internal/jobs/service_test.go` | Create (happy), create (validation fail), create (publish fail), get (found), get (not found) — uses mock repo and mock publisher |

### Integration tests (require PostgreSQL + Redis)

| File | Tests |
|------|-------|
| `tests/integration/job_flow_test.go` | POST → worker → COMPLETED, POST → worker → FAILED, GET /jobs list |
| `tests/integration/api_test.go` | 400 bad JSON, 400 unknown type, 404 unknown ID, healthz, readyz |

### How integration tests work
- Build tag: `//go:build integration`
- Test helper starts PostgreSQL and Redis from env vars.
- Runs migrations before suite.
- Each test gets its own job to avoid interference.

---

## 14. Demo Scripts

### `scripts/demo.ps1` (PowerShell)
### `scripts/demo.sh` (Bash)

Both scripts:
1. Submit an echo job.
2. Print job ID.
3. Poll until COMPLETED (with timeout).
4. Print the result.
5. Submit a sleep job (3 seconds).
6. Show it going through RUNNING → COMPLETED.
7. Submit a job with a type that causes failure ("fail" handler if registered, or bad payload).
8. Show FAILED state with error.

Target: showable in under 2 minutes.

---

## 15. File Manifest

Complete list of files to create:

```
flowforge/
├── .env.example
├── .gitignore
├── .github/
│   └── workflows/
│       └── ci.yml
├── Dockerfile
├── Makefile
├── README.md
├── docker-compose.yml
├── go.mod
├── go.sum
├── cmd/
│   ├── api/
│   │   └── main.go
│   └── worker/
│       └── main.go
├── docs/
│   ├── adr/
│   │   ├── 001-postgresql-source-of-truth.md
│   │   └── 002-redis-streams-queue.md
│   ├── api/
│   │   └── openapi.yaml
│   ├── architecture/
│   │   └── stage-01.md
│   └── stages/
│       └── stage-01.md
├── internal/
│   ├── api/
│   │   ├── errors.go
│   │   ├── handlers.go
│   │   ├── middleware.go
│   │   └── router.go
│   ├── config/
│   │   └── config.go
│   ├── database/
│   │   └── postgres.go
│   ├── jobs/
│   │   ├── model.go
│   │   ├── postgres_repository.go
│   │   ├── repository.go
│   │   ├── service.go
│   │   ├── transitions.go
│   │   ├── transitions_test.go
│   │   ├── validation.go
│   │   ├── validation_test.go
│   │   └── service_test.go
│   ├── queue/
│   │   ├── queue.go
│   │   ├── redis_consumer.go
│   │   └── redis_publisher.go
│   ├── tasks/
│   │   ├── echo.go
│   │   ├── echo_test.go
│   │   ├── handler.go
│   │   ├── registry.go
│   │   ├── registry_test.go
│   │   ├── sleep.go
│   │   └── sleep_test.go
│   └── worker/
│       └── worker.go
├── migrations/
│   └── 001_create_jobs.sql
├── scripts/
│   ├── demo.ps1
│   └── demo.sh
└── tests/
    └── integration/
        ├── api_test.go
        └── job_flow_test.go
```

Total: ~40 files.

---

## 16. Commit Plan

| # | Message | Scope |
|---|---------|-------|
| 1 | `chore: bootstrap Go project` | go.mod, .gitignore, .env.example, Makefile skeleton |
| 2 | `feat(db): add jobs schema and repository` | migrations/, internal/database/, internal/jobs/model+repo |
| 3 | `feat(queue): add Redis stream publisher and consumer` | internal/queue/ |
| 4 | `feat(tasks): add task registry and handlers` | internal/tasks/ |
| 5 | `feat(api): add job submission endpoint` | internal/api/, cmd/api/ |
| 6 | `feat(worker): add job consumer and execution` | internal/worker/, cmd/worker/ |
| 7 | `feat(api): add job status and listing` | GET endpoints |
| 8 | `test: add unit tests` | all *_test.go |
| 9 | `test: add integration tests` | tests/integration/ |
| 10 | `infra: add Dockerfile and Docker Compose` | Dockerfile, docker-compose.yml |
| 11 | `ci: add GitHub Actions pipeline` | .github/workflows/ci.yml |
| 12 | `docs: document Stage 1 architecture and API` | docs/, README.md |
| 13 | `docs: add demo scripts` | scripts/ |

---

## 17. Risk Register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Dual-write (PG insert + Redis publish) | Job stuck PENDING | Documented limitation; outbox in Stage 2 |
| Worker crash mid-job | Job stuck RUNNING | Documented; XAUTOCLAIM + lease in Stage 3 |
| Redis data loss on restart | Lost queue messages | AOF in Docker config; PG is source of truth |
| pgx connection exhaustion | API 500s | pgxpool with max connections + health check |
| Goose migration conflicts | Schema drift | Sequential numbering, CI check |

---

## Next Step

**This plan is ready for review.** Once approved, implementation proceeds commit-by-commit following section 16.

Go must be installed on this machine before implementation can begin. The plan is designed so that all documentation, schema, and configuration can be written without Go, but compilation and testing require Go 1.23+.
