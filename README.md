# FlowForge — Distributed Job Orchestrator

[![CI](https://github.com/flowforge/flowforge/actions/workflows/ci.yml/badge.svg)](https://github.com/flowforge/flowforge/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/flowforge/flowforge)](https://goreportcard.com/report/github.com/flowforge/flowforge)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

FlowForge is a resilient, distributed background job processing and orchestration platform built in Go.

---

## Stage 1: Async Task Execution Engine

In Stage 1, FlowForge implements the fundamental asynchronous execution pipeline:
- **REST API** for job ingestion and querying (built on `chi/v5` and standard library `net/http`).
- **PostgreSQL** as the single source of truth for all job states, timestamps, payloads, and results (using `pgx/v5` connection pooling).
- **Redis Streams** as the distributed FIFO message queue (`go-redis/v9` with consumer groups via `XADD`/`XREADGROUP`/`XACK`).
- **Worker Daemon** consuming tasks sequentially and executing registered task handlers (`echo` and `sleep`).
- **Automated migrations** embedded into the binary with `pressly/goose/v3`.

```
                      +-------------------+
                      |    HTTP Client    |
                      +---------+---------+
                                |
               1. POST /jobs    |    5. GET /jobs/{id}
                                v
                      +---------+---------+
                      |   FlowForge API   |
                      +---+-----------+---+
                          |           |
            2. INSERT     |           | 3. XADD
            (PENDING)     |           | (QUEUED)
                          v           v
                 +--------+---+   +---+--------+
                 | PostgreSQL |   |Redis Stream|
                 +--------+---+   +---+--------+
                          ^           |
            4. UPDATE     |           | 4. XREADGROUP
            (RUNNING)     |           |
            (COMPLETED)   |           |
                          +-----+-----+
                                |
                      +---------+---------+
                      | FlowForge Worker  |
                      +-------------------+
```

---

## Job State Machine

All state transitions are strictly validated and enforced using optimistic concurrency:

```
[ PENDING ] ──(queue)──> [ QUEUED ] ──(worker pickup)──> [ RUNNING ]
                                                            ├──(success)──> [ COMPLETED ]
                                                            └──(failure)──> [ FAILED ]
```

| From State | To State | Trigger | Constraint Guard |
|---|---|---|---|
| `PENDING` | `QUEUED` | Published to Redis Stream | Transition only |
| `QUEUED` | `RUNNING` | Worker picked up message | Sets `started_at = now()` |
| `RUNNING` | `COMPLETED` | Task handler returned nil error | Sets `result`, `completed_at = now()` |
| `RUNNING` | `FAILED` | Task handler returned error / panic | Sets `error`, `completed_at = now()` |

---

## Quickstart

### Running with Docker Compose

```bash
# Clone repository
git clone https://github.com/flowforge/flowforge.git
cd flowforge

# Start all services (PostgreSQL, Redis, API, Worker)
docker compose up --build -d

# Verify services are healthy
docker compose ps
```

### Running the Live Demo

```bash
# On Linux / macOS
./scripts/demo.sh

# On Windows PowerShell
./scripts/demo.ps1
```

---

## API Reference

### Health & Readiness

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/healthz` | Basic process liveness probe |
| `GET` | `/readyz` | Dependency readiness probe (PostgreSQL & Redis ping) |

### Jobs

#### 1. Submit a Job
```bash
POST /jobs
Content-Type: application/json

{
  "type": "echo",
  "payload": {
    "message": "Hello FlowForge"
  }
}
```
**Response (`201 Created`):**
```json
{
  "id": "e8a939bf-f166-4f40-a3bc-223450949d01",
  "type": "echo",
  "status": "QUEUED",
  "created_at": "2026-09-18T20:30:00Z"
}
```

#### 2. Get Job Status & Result
```bash
GET /jobs/e8a939bf-f166-4f40-a3bc-223450949d01
```
**Response (`200 OK`):**
```json
{
  "id": "e8a939bf-f166-4f40-a3bc-223450949d01",
  "type": "echo",
  "payload": {
    "message": "Hello FlowForge"
  },
  "status": "COMPLETED",
  "result": {
    "message": "Hello FlowForge"
  },
  "created_at": "2026-09-18T20:30:00Z",
  "started_at": "2026-09-18T20:30:01Z",
  "completed_at": "2026-09-18T20:30:01Z"
}
```

#### 3. List Jobs (Paginated)
```bash
GET /jobs?limit=20&offset=0
```
**Response (`200 OK`):**
```json
{
  "jobs": [ ... ],
  "total": 42,
  "limit": 20,
  "offset": 0
}
```

---

## Task Types (Stage 1)

1. **`echo`**:
   - Payload: `{"message": "string"}`
   - Result: returns the input payload verbatim.
2. **`sleep`**:
   - Payload: `{"seconds": integer}` (1 to 300)
   - Result: `{"slept_seconds": integer}`

---

## Development & Testing

```bash
# Run all unit tests with race detector
make test-race

# Run linter
make lint

# Run integration tests (requires local PostgreSQL & Redis)
make test-integration

# Build binaries locally
make build
```

---

## Project Structure

```
flowforge/
├── .github/workflows/ci.yml       # GitHub Actions CI pipeline
├── cmd/
│   ├── api/main.go               # HTTP API server entry point
│   └── worker/main.go            # Worker daemon entry point
├── docs/
│   ├── adr/                      # Architectural Decision Records
│   ├── api/openapi.yaml          # OpenAPI 3.1 Specification
│   ├── architecture/stage-01.md  # Architecture documentation & diagrams
│   └── stages/                   # Implementation plans & stage blueprints
├── internal/
│   ├── api/                      # Routing, handlers, middleware, errors
│   ├── config/                   # Environment configuration loader
│   ├── database/                 # Postgres connection pool & embedded migrations
│   ├── jobs/                     # Job domain models, repository, service, transitions
│   ├── queue/                    # Queue abstraction (Redis Streams publisher/consumer)
│   ├── tasks/                    # Task handler interface, registry, echo & sleep handlers
│   └── worker/                   # Worker loop, message dispatch, execution
├── migrations/                   # Plain SQL Goose schema migrations
├── scripts/
│   ├── demo.sh                   # Bash demo script
│   └── demo.ps1                  # PowerShell demo script
├── tests/
│   └── integration/              # End-to-end integration tests
├── Dockerfile                    # Multi-stage production container build
├── docker-compose.yml            # Local orchestration stack
├── Makefile                      # Build and test shortcuts
└── go.mod
```

---

## Stage 1 Design Decisions & Known Limitations

- **Dual-write without outbox**: If PostgreSQL succeeds and Redis fails, the job remains in `PENDING` status. (Transactional Outbox is scheduled for Stage 2).
- **At-least-once message delivery**: Workers acknowledge messages (`XACK`) after database updates. If a worker crashes mid-task, job remains in `RUNNING`. (Lease renewal & zombie reclamation scheduled for Stage 3).
- **In-memory sequential worker loop**: Concurrency controls, dynamic pools, and rate limits will be introduced in future stages.
