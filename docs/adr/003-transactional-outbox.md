# ADR-003: Transactional Outbox Pattern for Dual-Write Elimination

**Status:** Accepted  
**Date:** 2024-12-15  
**Deciders:** FlowForge engineering team

## Context

In Stage 1, job creation suffered from the classic distributed dual-write problem:
1. The HTTP API handler inserted a new job row in PostgreSQL (`status = 'PENDING'`).
2. The handler then published a message to Redis Streams (`XADD`).
3. If Redis was temporarily down, slow, or network partitioned, or if the API process crashed between steps 1 and 2, the job row was committed in PostgreSQL but never published to Redis.
4. Such orphaned jobs remained in `PENDING` status indefinitely without worker execution.

To guarantee zero job loss and decouple HTTP ingestion from Redis availability, we needed a durable, reliable publishing mechanism.

## Decision

**We adopt the Transactional Outbox Pattern with an Outbox Table (`outbox_events`) in PostgreSQL.**

When a client submits a job (`POST /jobs`):
1. A single PostgreSQL transaction (`pgx.Tx`) writes both:
   - The job record in `jobs` (`status = 'PENDING'`).
   - An outbox record in `outbox_events` (`event_type = 'JOB_CREATED'`, `status = 'PENDING'`).
2. The transaction commits atomically.
3. The API immediately returns HTTP 201 (`status = 'PENDING'`) without contacting Redis.
4. An independent daemon process (`cmd/dispatcher`) polls `outbox_events`, publishes events to Redis Streams, and marks them `PUBLISHED` or deletes them within an atomic transaction.

## Alternatives Considered

### 1. Two-Phase Commit (2PC) / XA Transactions
- **Pros:** Immediate consistency across PostgreSQL and Redis.
- **Cons:** Redis does not support standard XA transactions or 2PC. Heavy coordinator overhead, high latency, and fragile failure modes.

### 2. Change Data Capture (CDC) via PostgreSQL Logical Replication (e.g., Debezium)
- **Pros:** Zero polling overhead on PostgreSQL, tails the WAL directly.
- **Cons:** High operational complexity; requires Kafka or Debezium connect infrastructure and PostgreSQL `wal_level = logical`. Overkill for current throughput requirements. Can be adopted in a later stage if needed.

### 3. Synchronous Retry in HTTP Handler
- **Pros:** Simple to implement without new tables or background services.
- **Cons:** Ties HTTP response latency to Redis availability; does not protect against API server crashes between DB write and Redis write.

## Trade-offs

| Aspect | Synchronous Dual-Write (Stage 1) | Transactional Outbox (Stage 2) | CDC (Debezium) |
|--------|-----------------------------------|--------------------------------|----------------|
| Atomicity | ❌ Dual-write window | ✅ Guaranteed by PostgreSQL ACID | ✅ Guaranteed by WAL |
| API Latency | ⚠️ DB RTT + Redis RTT | ✅ DB RTT only | ✅ DB RTT only |
| Redis Outage Impact | ❌ Failed requests / orphaned jobs | ✅ Jobs saved in DB, queued when Redis recovers | ✅ Jobs saved in DB, queued when Redis recovers |
| Operational Complexity | ✅ Low | ⚠️ Moderate (dispatcher process) | ❌ High (Kafka/Connect) |
| End-to-End Latency | ✅ Immediate queueing | ⚠️ Polling interval delay (e.g. 100ms) | ✅ Sub-second stream |

## Consequences

### Positive
- Guaranteed atomicity: A job cannot exist in the database without a corresponding outbox event.
- Resilience to Redis outages: Clients can continue creating jobs even if Redis is completely down; the dispatcher flushes events once Redis recovers.
- Fast API response: The API server only writes to PostgreSQL and does not wait on Redis network calls.
- Clear separation of concerns: The API handles HTTP validation and persistence; the dispatcher handles message transport.

### Negative
- Asynchronous dispatch introduces slight end-to-end latency bounded by the dispatcher's poll interval.
- Polling generates database load (mitigated by indexing `status = 'PENDING'` and using `SELECT ... FOR UPDATE SKIP LOCKED`).
- Outbox table growth requires cleanup/pruning.

### Risks & Mitigations
- **Outbox table bloat:** The dispatcher transitions published events to `PUBLISHED` with timestamps, enabling scheduled background retention cleanup.
- **Polling lock contention:** Mitigated by `FOR UPDATE SKIP LOCKED`, allowing concurrent dispatcher replicas to pull disjoint batches without lock waits.
