# ADR-004: At-Least-Once Publication and Worker Replay Guard

**Status:** Accepted  
**Date:** 2024-12-15  
**Deciders:** FlowForge engineering team

## Context

Distributed queues provide either at-most-once, at-least-once, or exactly-once delivery guarantees. Because network partitions, dispatcher crashes, and Redis timeouts can happen after a message is sent to Redis but before the database marks the outbox event as `PUBLISHED`, duplicate queue messages are mathematically inevitable in an at-least-once architecture.

Similarly, if a worker crashes or encounters network jitter while acknowledging a message (`XACK`), or if a message is re-dispatched during retry cycles, the worker pool may receive duplicate messages for the same `job_id`.

We must ensure that duplicate messages do not result in duplicate executions, race conditions, or corrupted job states.

## Decision

**We embrace at-least-once delivery with two-phase deduplication and an atomic worker replay guard.**

1. **Dispatcher Publication:**
   - The dispatcher selects pending outbox events using `SELECT ... FOR UPDATE SKIP LOCKED`.
   - It publishes each event to Redis Streams (`XADD`).
   - In the same database transaction, it updates the job status from `PENDING` to `QUEUED` and the outbox event from `PENDING` to `PUBLISHED`.
   - If the dispatcher crashes between `XADD` and database commit, the outbox event remains `PENDING` and will be re-published upon recovery (at-least-once delivery).

2. **Worker Replay Guard:**
   - When a worker receives a message from Redis Streams, it loads the job record from PostgreSQL.
   - It verifies that `status == 'QUEUED'`.
   - If `status != 'QUEUED'` (e.g. the job is already `RUNNING`, `COMPLETED`, `FAILED`, or `RETRY_WAIT`), the worker immediately skips execution, logs a warning, and calls `XACK` on the message.
   - If `status == 'QUEUED'`, the worker attempts an atomic conditional state transition:
     ```sql
     UPDATE jobs
     SET status = 'RUNNING', started_at = now(), attempt_count = attempt_count + 1
     WHERE id = $1 AND status = 'QUEUED'
     ```
   - If zero rows are updated (another concurrent worker won the race), the losing worker discards the message and acknowledges Redis.

3. **Execution Recording:**
   - Every valid execution creates a new row in `job_attempts` linked by `job_id` and `attempt_number`.

## Alternatives Considered

### 1. Exactly-Once Delivery with Redis Stream Deduplication IDs
- **Pros:** Prevents duplicate messages in the queue.
- **Cons:** Redis Streams deduplication requires maintaining a global sliding deduplication cache or external coordinator. Redis crashes or failovers can lose state. Distributed systems cannot achieve end-to-end exactly-once without idempotent consumers anyway.

### 2. Distributed Lock per Job (e.g., Redlock)
- **Pros:** Prevents concurrent worker execution.
- **Cons:** Adds operational dependency on distributed lock managers, clock synchronization issues, and lock renewal/TTL edge cases. PostgreSQL row-level locks and conditional updates provide stronger, ACID-compliant guarantees.

## Trade-offs

| Aspect | Unchecked Consumer (Stage 1) | Replay Guard & State Check (Stage 2) | Distributed Locking (Redlock) |
|--------|------------------------------|--------------------------------------|--------------------------------|
| Duplicate Safety | ❌ High risk of double run | ✅ Guaranteed single active execution | ✅ Guaranteed single active execution |
| State Consistency | ⚠️ Overwrite risks | ✅ Enforced by SQL conditional update | ⚠️ Relies on lock leases & TTLs |
| Redis Ack Failure Safety | ❌ Re-execution on redelivery | ✅ Safe ACK and discard | ✅ Safe ACK and discard |
| Throughput Overhead | Zero | 1 extra SELECT + conditional UPDATE | Network roundtrips for lock acquire/release |

## Consequences

### Positive
- Robust idempotency: Workers can safely receive redelivered messages without duplicate task execution.
- Self-healing queues: Stale, duplicate, or out-of-order queue messages are automatically acknowledged and pruned.
- Accurate attempt history: Attempts are recorded immutably per actual execution.

### Negative
- A database roundtrip (`SELECT` + `UPDATE`) occurs before task execution begins.
- Task handlers themselves should still strive to be idempotent if external side effects occur before failure.
