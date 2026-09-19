# Stage 1 Architecture

## System Overview

```mermaid
graph LR
    subgraph Client
        C[curl / HTTP client]
    end

    subgraph API["API Server (cmd/api)"]
        H[HTTP Handler<br/>Chi router]
        S[Job Service]
        R[Job Repository]
        QP[Queue Publisher]
    end

    subgraph Storage
        PG[(PostgreSQL<br/>jobs table)]
        RS[(Redis Stream<br/>flowforge:jobs)]
    end

    subgraph Worker["Worker (cmd/worker)"]
        QC[Queue Consumer]
        D[Task Dispatcher]
        TR[Task Registry]
        TH1[Echo Handler]
        TH2[Sleep Handler]
        WR[Job Repository]
    end

    C -->|POST /jobs<br/>GET /jobs| H
    H --> S
    S --> R
    S --> QP
    R -->|INSERT / SELECT / UPDATE| PG
    QP -->|XADD| RS

    RS -->|XREADGROUP| QC
    QC --> D
    D --> TR
    TR --> TH1
    TR --> TH2
    D --> WR
    WR -->|SELECT / UPDATE| PG
    QC -->|XACK| RS
```

## Request Flow — Job Creation

```mermaid
sequenceDiagram
    participant C as Client
    participant A as API Server
    participant PG as PostgreSQL
    participant RD as Redis Stream
    participant W as Worker

    C->>A: POST /jobs {type, payload}
    A->>A: Validate request
    A->>PG: INSERT job (PENDING)
    PG-->>A: job_id
    A->>RD: XADD {job_id, type}
    RD-->>A: message_id
    A->>PG: UPDATE status → QUEUED
    A-->>C: 201 {job_id, status: QUEUED}

    Note over W: Worker blocking on XREADGROUP
    RD->>W: {job_id, type}
    W->>PG: SELECT job
    W->>PG: UPDATE status → RUNNING
    W->>W: Execute task handler
    alt success
        W->>PG: UPDATE status → COMPLETED, result
    else failure
        W->>PG: UPDATE status → FAILED, error
    end
    W->>RD: XACK
```

## Job State Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING: Job created in DB
    PENDING --> QUEUED: Published to Redis
    QUEUED --> RUNNING: Worker picks up
    RUNNING --> COMPLETED: Task succeeds
    RUNNING --> FAILED: Task fails
    COMPLETED --> [*]
    FAILED --> [*]
```

## Package Dependency Graph

```mermaid
graph TD
    CMD_API[cmd/api] --> API[internal/api]
    CMD_API --> CONFIG[internal/config]
    CMD_API --> DB[internal/database]

    CMD_WORKER[cmd/worker] --> WORKER[internal/worker]
    CMD_WORKER --> CONFIG
    CMD_WORKER --> DB

    API --> JOBS[internal/jobs]
    API --> QUEUE[internal/queue]

    WORKER --> JOBS
    WORKER --> QUEUE
    WORKER --> TASKS[internal/tasks]

    JOBS --> DB

    style CMD_API fill:#4a90d9,color:white
    style CMD_WORKER fill:#4a90d9,color:white
    style API fill:#7fb069,color:white
    style JOBS fill:#7fb069,color:white
    style QUEUE fill:#e8a838,color:white
    style WORKER fill:#7fb069,color:white
    style TASKS fill:#7fb069,color:white
    style DB fill:#d94a4a,color:white
    style CONFIG fill:#888,color:white
```

## Infrastructure

```mermaid
graph TB
    subgraph Docker Compose
        MIGRATE_C[migrate<br/>init container]
        API_C[api<br/>:8080]
        WORKER_C[worker]
        PG_C[postgres<br/>:5432 / host: 5433]
        REDIS_C[redis<br/>:6379]
    end

    MIGRATE_C -->|applies migrations| PG_C
    API_C -.->|depends on completion| MIGRATE_C
    WORKER_C -.->|depends on completion| MIGRATE_C
    API_C --> PG_C
    API_C --> REDIS_C
    WORKER_C --> PG_C
    WORKER_C --> REDIS_C
```

## Key Design Decisions

1. **PostgreSQL is the source of truth** — Redis holds queue pointers only. See [ADR-001](../adr/001-postgresql-source-of-truth.md).
2. **Redis Streams for queueing** — lightweight, consumer-group-ready with bounded retention (`MaxLen: 10000, Approx: true`). See [ADR-002](../adr/002-redis-streams-queue.md).
3. **Queue abstraction** — `internal/queue` defines `Publisher` and `Consumer` interfaces. Redis is the current implementation; Kafka can be swapped in later.
4. **Task registry** — handlers register by name; the worker dispatches by job type. New task types require only a new handler + registration.
5. **Decoupled Readiness Probes** — `/readyz` utilizes the abstract `Pinger` interface for PostgreSQL and Redis health checks without exposing internal drivers.
6. **Worker Resilience & Panic Recovery** — Worker recovers from task panics via `recover()`, marking jobs `FAILED` with sanitized error messages and acknowledging queue messages to prevent blocking. Graceful shutdown uses an isolated 5-second context to finalize job status and queue ACKs.
7. **Goose migrations** — SQL-based, no ORM, version-controlled schema changes applied via a dedicated migration runner.
8. **Chi router** — lightweight, idiomatic, stdlib-compatible middleware chain.
