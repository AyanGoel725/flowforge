# ADR-001: PostgreSQL as the Source of Truth for Job State

**Status:** Accepted  
**Date:** 2024-12-01  
**Deciders:** FlowForge engineering team

## Context

FlowForge needs a durable store that holds the authoritative state of every job: its current status, payload, result, error, and timestamps. The system uses Redis Streams as a message queue, but the queue itself is not designed to be the primary datastore. We need to decide where job state lives.

Requirements:
- Job state must survive process restarts.
- The API must be able to query job status and results.
- The system must support listing jobs with pagination.
- State transitions must be enforced consistently.
- The store must support JSONB payloads and results.

## Decision

**PostgreSQL is the single source of truth for all job state.**

Redis holds only transient queue messages containing `job_id` and `type` — enough for the worker to locate the job. All reads and writes of job status, payload, result, and error go to PostgreSQL.

## Alternatives Considered

### 1. Redis as primary store
- **Pros:** Low latency, same system as the queue.
- **Cons:** Redis is not designed for relational queries, pagination, or complex constraints. Data persistence requires AOF/RDB tuning. No schema enforcement. Harder to inspect and debug.

### 2. Dual-source (keep both in sync transactionally)
- **Pros:** Fast reads from Redis, durable writes in PostgreSQL.
- **Cons:** Introduces distributed transaction complexity far too early. Requires a transactional outbox or 2PC, which is a Stage 2+ concern.

### 3. SQLite
- **Pros:** Zero-ops, embedded.
- **Cons:** Single-writer, no concurrent access from API + worker processes, no network access.

## Trade-offs

| Aspect | PostgreSQL | Redis |
|--------|-----------|-------|
| Durability | ✅ WAL + replication | ⚠️ Requires AOF tuning |
| Queryability | ✅ SQL, indexes, pagination | ❌ Limited |
| Schema enforcement | ✅ Types, constraints, enums | ❌ None |
| Operational maturity | ✅ Well-understood | ✅ Well-understood |
| Latency | ⚠️ Higher than Redis | ✅ Sub-ms |

## Consequences

### Positive
- Single, well-understood source of truth.
- Full SQL query capability for the API layer.
- Schema constraints enforce valid state transitions at the database level.
- Easy to add indexes, columns, and constraints in future stages.
- Standard operational tooling (backups, monitoring, replication).

### Negative
- Dual-write problem: the API inserts into PostgreSQL and then publishes to Redis. If Redis publish fails, the job may remain PENDING forever. **This is a documented Stage 1 limitation**; a transactional outbox pattern will be introduced in a later stage.
- Worker must make a round-trip to PostgreSQL to load the full job after receiving a Redis message (minimal overhead; keeps Redis messages small).

### Risks
- PostgreSQL becoming a bottleneck under high throughput. Mitigated by connection pooling and read replicas in later stages.
- Schema migrations require coordination. Mitigated by using goose for versioned migrations.
