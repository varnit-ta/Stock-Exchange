# ADR-0006: Exactly-once: Kafka transactions in the matcher, idempotent consumers elsewhere

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** D1, D2, D6, A1

## Context
Networks fail, processes crash, and Kafka redelivers. A duplicate order means a duplicate trade, and a duplicate trade means money moves twice. We need effectively-exactly-once effects along the whole path: client → OMS → Kafka → matcher → Kafka → OMS/ledger.

## Options considered

### Option A: At-least-once + idempotent consumers everywhere
- The matcher dedupes inputs by tracking the last processed offset in its state, and downstream dedupes outputs by deterministic `event_id`.
- Pros: simpler Kafka usage; works with any broker.
- Cons: the matcher may *emit* duplicate outputs after a crash (re-processing between its last snapshot offset and the last committed offset). Every consumer must dedupe, and a buggy one causes a double fill.

### Option B: Kafka transactions for consume-transform-produce in the matcher
- Outputs (`exec.events`, `md.events`) + input offset commit are atomic. Consumers read with `read_committed`.
- Pros: no duplicate outputs from the matcher, ever; a clean recovery story (committed offset = exactly what's been emitted).
- Cons: transaction overhead (commit markers, coordinator round trips); `read_committed` consumers wait for the transaction to commit, which adds latency equal to the commit interval. You must batch (e.g. commit every 1–5 ms or N messages) to get throughput.

### Option C: Transactions everywhere
- Cons: transactions don't extend to Redis or Postgres side effects anyway, so idempotency is still needed there.

## Decision
**Hybrid:**
1. **Client → OMS:** idempotency key (`client_order_id`) in Redis with `SET NX`; retries return the original result (D6).
2. **OMS → Kafka:** idempotent producer (no broker-side duplicates on retry).
3. **Matcher:** Kafka transactions (Option B), committed per small batch.
4. **Downstream (OMS, ledger, market data, candles):** at-least-once consumption + **idempotent effects**:
   - Ledger: `event_id` unique constraint inside the same DB transaction.
   - OMS/Redis: Lua scripts check the per-order `last_applied_seq` before applying a fill.
   - Market data: monotonic `md_seq`; drop anything ≤ the last applied.
5. **Deterministic IDs** (`p{partition}-{offset}-{n}`) so even a replay regenerates identical IDs.

## Consequences
- **Positive:** defence in depth. Two independent mechanisms must both fail for a double fill.
- **Negative:** transaction batching adds latency (tune against L3); more moving parts to test.
- **Risks:** zombie matcher instances (old leader still running) → mitigation: transactional IDs per partition give **producer fencing**; the old instance's transactions are aborted.

## Review outcomes (2026-11-07)

### 1. Client retries: key TTL must cover the retry window
- First request: the gateway runs `SET idem:{account}:{client_order_id} PENDING NX EX <ttl>`. It succeeds, so the request goes to the OMS, which creates the order, publishes it, and overwrites the key with the result (`{order_id, ACCEPTED}` or the rejection).
- Retry: `SET NX` fails (the key exists) → `GET` the key. If it holds a result, return that result. If it still says `PENDING` (the first request is in flight), wait briefly and poll; never create a new order. If it's still `PENDING` after the timeout, the OMS resolves it from the order projection (architecture §10).
- If the key expires before the retry arrives, the retry looks new and creates a **duplicate order**. Rules: TTL > the client's maximum retry window, and the gateway **rejects requests older than the window** (the client sends a request timestamp; too old → `REQUEST_EXPIRED`, and the client checks order status instead of retrying). TTL is short because of memory (architecture §9).
- Known gap: Redis replication is async, so a failover can lose a fresh key (ADR-0005). Accepted for now; the deterministic order IDs and the Postgres unique constraint on `(account_id, client_order_id)` in the order projection let us **detect** it (alert), though not prevent it.

### 2. Commit interval is a latency/throughput knob
- `read_committed` consumers see a fill only after its transaction commits: up to one commit interval plus the commit round trip (≈ 5–10 ms worst case at a 5 ms interval). This eats into L3 (fill ≤ 50 ms) and L4 (tick ≤ 50 ms). L2 (order ack) is unaffected: the ack comes from the OMS after its produce.
- Policy: commit every **5 ms or 500 messages, whichever comes first**, both as config properties. Shorter = lower latency but more commits/s; measure both and log the result in `perf-log/`.

### 3. Deterministic IDs are required, not optional
- Random UUIDs would give a replayed fill a **different** ID from the original, so the ledger's unique constraint could not recognise the duplicate. `p{partition}-{offset}-{n}` makes the same input produce the same ID. This depends on engine determinism (D5, ADR-0003).

### 4. Defence in depth: each layer catches what the other can't
| Failure | Matcher transactions | Downstream dedupe |
|---|---|---|
| Matcher crashes between writing fills and committing its offset | ✅ aborted, never visible | ✅ would also catch it |
| Consumer without dedupe (candles, a new service, an invariant checker) | ✅ it never sees duplicates | ❌ not present there |
| Ledger crashes after its DB commit but before its Kafka offset commit → re-reads the event | ❌ outside Kafka | ✅ `event_id` unique constraint |
| Consumer misconfigured with `read_uncommitted` sees aborted output | ❌ bypassed | ✅ same deterministic ID gets rejected |
| Bug in silent replay re-emits old fills | ❌ they're committed as new | ✅ same deterministic ID gets rejected |

A double fill needs **both** layers to fail at once.

## Revisit when
- Transaction commit latency makes L3 unachievable → fall back to Option A with stricter downstream dedupe.
