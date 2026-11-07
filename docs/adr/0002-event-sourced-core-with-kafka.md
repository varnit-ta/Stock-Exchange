# ADR-0002: Event-sourced core: Kafka is the system of record for orders

- **Status:** Accepted (reviewed 2026-11-07)
- **Date:** 2026-11-07
- **Related:** D1, D5, A1, L2

## Context
Once we tell a client "order accepted", that order must never be lost (D1), and the matching engine must rebuild its exact state after a crash (A1, D5). We must choose where an accepted order is first durably recorded, and how downstream state (engine books, order status, market data) is derived.

## Options considered

### Option A: Postgres first, then publish (outbox)
OMS writes the order row + outbox row in one DB transaction; a relay publishes to Kafka.
- Pros: the order is queryable immediately; a familiar CRUD model; the DB enforces constraints.
- Cons: an extra hop on the hottest path (DB commit + relay delay, typically +2–20 ms); two sources of truth for order sequence (the DB and the Kafka order); the DB becomes the throughput ceiling.

### Option B: Kafka first (event sourcing)
OMS produces `NewOrder` to `orders.commands` with `acks=all`; that log **is** the record. Everything else (engine state, order projection in Postgres, market data) is derived by consuming logs.
- Pros: one durable write on the hot path; the log order = matching order, so the engine is a deterministic replay of it; replay is free (recovery, new consumers, debugging, backtesting); natural decoupling.
- Cons: "Read your own write" is eventual: the order may not be visible in the Postgres projection for some ms. Schema evolution of events needs care. Querying the log directly is awkward, so projections are needed.

### Option C: The engine is the system of record (direct RPC, engine journals locally)
Like real exchanges with custom replicated journals.
- Pros: lowest possible latency.
- Cons: we'd build our own replicated log. That's a great Phase 7 project, but too much to start with.

## Decision
**Option B.** `orders.commands` is the system of record for order intent; `exec.events` is the system of record for order outcomes. Postgres holds **projections** and the **ledger** (money has its own source of truth, ADR-0008).

**Why B and not A, for us specifically:** T2 (10k orders/s) with L2 (order-ack p99 ≤ 15 ms) puts the extra DB hop and DB throughput of Option A under pressure, and learning Kafka and event sourcing is an explicit project goal. **At ≲ 100 orders/s, Option A (Postgres + outbox, maybe no Kafka at all) would be the better choice**: simpler to run, and its costs wouldn't matter.

### Detail 1: Read path (read-your-own-writes)
Only the Kafka write must succeed before replying `ACCEPTED`. Redis and Postgres are copies built from Kafka:

```
 POST ──► OMS ──(1) must succeed──► KAFKA ──(3) OMS exec consumer──► Redis status cache
           │                          └────(4) projection consumer──► Postgres orders table
           └──(2) best-effort──► Redis  order:{id} = {status, version}   (TTL 10 min)

 GET /orders/{id}:  Redis hit → return │ miss → Postgres hit → return
                    │ both miss + ID is recent (time-ordered ID) → return PENDING, never 404
```

- Status changes are also pushed to the client over its private WebSocket.
- Every exec event carries a per-order `version`; both copies store it and readers trust the **higher version**.
- Consumers commit the Kafka offset **only after** their write succeeds, and writes are idempotent upserts (`ON CONFLICT … WHERE version < new`). A Postgres outage delays the projection; it loses nothing, as long as the outage is shorter than Kafka retention.
- A failed Redis write at accept time does **not** fail the order (the cache is best-effort).
- A message that keeps failing → halt the projection consumer and alert (orders are too important to skip silently).
- Alert on projection lag.
- Optional strict mode: the response includes the Kafka offset; `GET ?after=p7:9912` waits until the projection has consumed it.

### Detail 2: Retention, snapshots and audit
Kafka is a delivery and replay buffer (7 days), not an archive.

```
 Kafka      → short-term: ordering, delivery, replay window (7 days)
 Snapshots  → crash recovery: a photo of the CURRENT book (every resting order, however old)
              + the last offset applied. Recovery = latest snapshot + replay after it.
 Postgres   → permanent history: archive consumer writes every command to an append-only
              `order_commands` table (partitioned by day) for audit
```

- A GTC order resting for weeks is safe: it's in every snapshot while it rests. Only the messages **after** the latest snapshot are needed.
- **Danger:** the latest snapshot becomes older than retention → messages after it are deleted → recovery impossible. Alert when the newest snapshot is older than **1 h** (warn) and older than **retention ÷ 2** (critical).

### Detail 3: Engine code versions and replay
Determinism = **same inputs + same code**. Replaying old inputs with new code can produce a book that disagrees with trades already published (e.g. a bug fix to IOC handling turns a published fill into a resting order → later double fill).

Rule: **new code only processes inputs for the first time; old inputs are only ever replayed by the code version that originally processed them.**
1. **Graceful shutdown always snapshots** at the last processed offset, so after a deploy the new version replays nothing old.
2. **Snapshots record the engine version.** On startup, if the snapshot's version differs **and** there are inputs to replay (the old version crashed), the engine **refuses to start** and alerts. Runbook: start the old version, let it recover, stop it gracefully, then deploy the new one.
3. **Phase 6:** during silent replay, compare a hash of regenerated outputs with the committed outputs in `exec.events`; a mismatch halts the engine.
4. Event schema changes are **additive only**; a missing new field must default to the old behaviour.
5. Intentional rule changes can be versioned by input offset ("rule v2 applies from offset X"). Not needed yet.

## Consequences
- **Positive:** a single durable write on the order path (helps L2); deterministic replay for recovery and testing (D5); new consumers can be added without touching producers.
- **Negative:** eventual consistency for order queries; the client gets status updates asynchronously. Event schemas are a long-term contract (use protobuf; additive changes only).
- **Risks:** retention deletes the record → mitigated by snapshot-age alerting and the `order_commands` archive (Detail 2). Replay with different code → mitigated by version checks (Detail 3).

## Revisit when
- Requirements shrink to ≲ 100 orders/s → Option A is simpler.
- Order-ack latency is dominated by Kafka and can't meet L2 → consider Option C (custom journal) in Phase 7.
