# ADR-002: Redis Streams as the Initial Queue Mechanism

**Status:** Accepted  
**Date:** 2024-12-01  
**Deciders:** FlowForge engineering team

## Context

FlowForge requires a message queue to decouple job submission (API) from job execution (worker). The API publishes a job message; one or more workers consume it. The queue must support:

- At-least-once delivery semantics.
- Consumer groups (even if Stage 1 has only one worker).
- Message acknowledgment.
- Reasonable ordering guarantees.
- Low operational overhead for development.

Additionally, the architecture must be structured so the queue implementation can be replaced later (e.g., with Kafka or a managed queue service).

## Decision

**Use Redis Streams with consumer groups as the queue mechanism for Stage 1.**

The queue is accessed through an abstraction layer (`internal/queue`) so that the API and worker do not depend on Redis directly.

Stage 1 uses:
- `XADD` — publish job messages.
- `XREADGROUP` — consume messages in a consumer group.
- `XACK` — acknowledge processed messages.

Stage 1 does **not** use:
- `XAUTOCLAIM` — crash recovery is a later stage.
- Dead-letter queues, retry queues, or delayed queues.
- Leases or heartbeats.

## Alternatives Considered

### 1. Kafka
- **Pros:** Battle-tested at scale, rich consumer group semantics, log compaction, replay.
- **Cons:** Heavy operational burden for a Stage 1 prototype. Introduces ZooKeeper/KRaft. Overkill for single-worker single-stream.
- **Verdict:** Deferred to a later stage when throughput requirements justify it.

### 2. RabbitMQ
- **Pros:** Mature, supports multiple exchange patterns, built-in DLQ.
- **Cons:** Another infrastructure component. Redis is already in the stack for potential caching/rate-limiting in future stages.
- **Verdict:** Viable but adds a dependency with no current advantage over Redis Streams.

### 3. PostgreSQL LISTEN/NOTIFY
- **Pros:** No additional infrastructure.
- **Cons:** No built-in consumer groups. Messages are ephemeral — not delivered if no listener is connected. No acknowledgment semantics. Not a real queue.
- **Verdict:** Insufficient for reliable job delivery.

### 4. PostgreSQL-based polling queue
- **Pros:** Single datastore, transactional safety.
- **Cons:** Polling adds latency. `SELECT ... FOR UPDATE SKIP LOCKED` works but creates lock contention at scale. Harder to evolve toward distributed consumption.
- **Verdict:** Considered as a fallback if Redis is removed; not chosen for Stage 1.

## Trade-offs

| Aspect | Redis Streams | Kafka |
|--------|--------------|-------|
| Operational complexity | ✅ Low (already using Redis) | ❌ High |
| Consumer groups | ✅ Built-in | ✅ Built-in |
| At-least-once delivery | ✅ XACK | ✅ Offset commits |
| Persistence | ⚠️ Best-effort (AOF/RDB) | ✅ Log-based |
| Ordering | ✅ Per-stream | ✅ Per-partition |
| Throughput ceiling | ⚠️ Single-node limited | ✅ Horizontally scalable |
| Ecosystem | ⚠️ Smaller | ✅ Rich (Connect, Schema Registry) |

## Consequences

### Positive
- Redis is lightweight and already in the Docker Compose stack.
- Consumer groups allow adding workers in later stages without architectural changes.
- The `XREADGROUP` block call makes the worker efficient (no polling).
- Redis Streams provide a natural message ID for ordering and acknowledgment.

### Negative
- No crash recovery in Stage 1: if a worker dies after `XREADGROUP` but before `XACK`, the message remains in the Pending Entries List (PEL). `XAUTOCLAIM` will be added in a later stage.
- Redis is not as durable as Kafka; messages can be lost on Redis restart without AOF. Acceptable for Stage 1.
- The queue abstraction adds a thin layer of indirection. This is intentional — it keeps the door open for swapping Redis Streams for Kafka or another backend.

### Risks
- Redis becoming a single point of failure. Mitigated in later stages by Redis Sentinel or Cluster.
- Stream growing unbounded if messages are never trimmed. Mitigated by adding `MAXLEN` or `MINID` trimming in a later stage.
