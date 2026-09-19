# ADR-005: Explicit Error Classification and Exponential Backoff with Jitter

**Status:** Accepted  
**Date:** 2024-12-15  
**Deciders:** FlowForge engineering team

## Context

In Stage 1, any error returned by a task handler immediately transitioned the job to `FAILED`. In production distributed systems, tasks experience two distinct classes of failures:
1. **Transient failures:** Network timeouts, temporary service unavailability, rate limits, or transient deadlocks. These usually succeed upon subsequent retries.
2. **Permanent failures:** Invalid payloads, schema violations, malformed arguments, or fatal business rule breaches. Retrying these wastes compute and clogs queue pipelines.

Additionally, retrying immediately or with fixed intervals causes the "thundering herd" problem, where all failed jobs retry simultaneously and overwhelm struggling downstream services.

## Decision

**We introduce explicit error classification and an exponential backoff engine with Full Jitter.**

### 1. Error Classification
We define a custom `TaskError` interface in `internal/tasks`:
```go
type TaskError interface {
    error
    IsRetryable() bool
}
```
- **Retryable Errors:** Created via `tasks.NewRetryableError(msg)`. If `attempt_count < max_attempts`, the job transitions to `RETRY_WAIT`, sets `next_attempt_at`, and schedules a retry.
- **Permanent Errors:** Created via `tasks.NewPermanentError(msg)` or standard unclassified Go `error` (defaulting to non-retryable or configurable). The job transitions immediately to `FAILED`.

### 2. Backoff Calculation (Full Jitter)
Retry delays are computed using exponential backoff with full jitter to evenly distribute retries:
$$\text{delay} = \min(\text{base\_delay} \times 2^{\text{attempt}-1}, \text{max\_delay})$$
$$\text{actual\_delay} = \text{random}(0, \text{delay})$$

Default policy:
- `BaseDelay`: 1 second
- `MaxDelay`: 60 seconds
- `MaxAttempts`: 3 (per-job configurable via `max_attempts` request attribute)
- `Jitter`: Full Jitter ($U[0, \text{delay}]$)

### 3. Asynchronous Retry Scheduling
Instead of sleeping workers or holding Redis connections open during backoff:
1. Failed jobs transition to `status = 'RETRY_WAIT'` and store `next_attempt_at = now() + actual_delay`.
2. The worker acknowledges (`XACK`) the current Redis message, releasing worker capacity.
3. The Outbox & Retry Dispatcher polls for retry-eligible jobs:
   ```sql
   SELECT id, type, attempt_count, max_attempts
   FROM jobs
   WHERE status = 'RETRY_WAIT' AND next_attempt_at <= now()
   ORDER BY next_attempt_at ASC
   LIMIT $1
   FOR UPDATE SKIP LOCKED;
   ```
4. The dispatcher publishes the job to Redis Streams and atomically transitions its status to `QUEUED`.

## Alternatives Considered

### 1. In-Worker Sleeping (`time.Sleep`)
- **Pros:** No database status transitions or dispatcher scheduler needed.
- **Cons:** Blocks worker goroutines, prevents workers from processing other jobs, and loses progress if the worker restarts during the sleep.

### 2. Redis Delayed Queues (Sorted Sets / `ZSET`)
- **Pros:** Highly responsive timing without PostgreSQL polling.
- **Cons:** Splits job state across Redis and PostgreSQL, reintroducing dual-write and consistency risks during retry state transitions. Keeping retry scheduling in PostgreSQL preserves PostgreSQL as the single source of truth.

### 3. Equal Jitter vs Full Jitter
- **Pros of Equal Jitter:** Keeps a guaranteed minimum backoff interval ($\frac{\text{delay}}{2} + \text{random}(0, \frac{\text{delay}}{2})$).
- **Cons:** Full Jitter provides the lowest overall queue competition and shortest average completion times in high-concurrency benchmarks (AWS Architecture Blog: *Exponential Backoff And Jitter*).

## Trade-offs

| Aspect | In-Worker Sleep | Redis ZSET Scheduler | PostgreSQL Dispatcher (Stage 2) |
|--------|-----------------|----------------------|---------------------------------|
| Worker Resource Utilization | ❌ Blocks goroutine/threads | ✅ Non-blocking | ✅ Non-blocking |
| Durability during Worker Crash | ❌ Lost retry state | ⚠️ Redis persistence dependent | ✅ Full ACID durability |
| Architecture Simplicity | ✅ Simplest | ⚠️ Dual-store coordination | ✅ Reuses outbox dispatcher loop |
| Timer Precision | ✅ Millisecond exact | ✅ Millisecond exact | ⚠️ Bound by dispatcher poll interval (e.g. 100ms) |

## Consequences

### Positive
- Failed retryable jobs do not consume worker slots or memory during backoff delays.
- Downstream services are protected against retry storms via full jitter distribution.
- Complete execution transparency: each attempt duration, status, and error is recorded in `job_attempts`.
- Maximum attempt limits prevent endless retry loops.

### Negative
- Polling for `RETRY_WAIT` jobs adds a lightweight query to the dispatcher loop (optimized with index on `(status, next_attempt_at)`).
- Tasks must return explicit `RetryableError` or `PermanentError` wrappers for deterministic classification.
