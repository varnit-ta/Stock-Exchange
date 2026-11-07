# ADR-0005: Pre-trade risk via atomic Lua reservations in Redis

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** F7–F9, D4, L5, A4

## Context
Every order must atomically check-and-reserve buying power (or shares) before it reaches the market. It must hold under heavy concurrency on a single account (D4) and add < 1 ms p99 (L5). The ledger in Postgres is the source of truth for balances, but it's updated asynchronously after fills.

## Options considered

### Option A: Postgres row lock per account (`UPDATE … WHERE available >= x`)
- Pros: strongly consistent with the ledger; durable.
- Cons: ~1–5 ms per order; hot accounts (market makers) serialize on one row lock; Postgres becomes the order-path bottleneck.

### Option B: In-memory risk inside the OMS, accounts sharded across OMS instances
- Pros: fastest (nanoseconds); no network hop.
- Cons: requires sticky routing by account and its own durability/recovery story. It's effectively building another stateful service. Interesting, but harder.

### Option C: Redis hash per account + Lua check-and-reserve
- Pros: atomic (Lua runs alone); ~100–300 µs; simple to scale with Redis Cluster (hash tag per account); a great way to learn Redis deeply.
- Cons: Redis replication is async, so a failover can **lose recent reservations** → possible overspend unless handled. The data must be kept in sync with the ledger.

### Option D: Engine-side risk (exchange checks funds)
- Pros: single point of truth at match time.
- Cons: mixes broker and exchange concerns; slows the engine; not how the real world splits responsibilities.

## Decision
**Option C**, with Redis treated as **derived, rebuildable state**:
- `available = cash − reserved_cash − pending_cash` (see the architecture fill flow).
- Every reservation records `order_id`, so reservations can be audited and rebuilt.
- **Rebuild procedure:** ledger balances (Postgres) + open orders (order projection / `exec.events`) → recompute `{acct}` hashes.
- **Fail closed:** during Redis failover or rebuild, the OMS rejects new orders for affected accounts.
- A reconciliation job compares Redis vs (ledger + open orders) continuously.

## Consequences
- **Positive:** meets L5 easily; a clear, testable atomicity story; a rich learning surface (Lua, hash tags, pipelining, failover).
- **Negative:** a sync mechanism (`account.events`) and a rebuild path to build and test; a window of unavailability during failover.
- **Risks:** OMS crashes between reserve and Kafka produce → leaked reservation → mitigation: sweeper releasing reservations with no corresponding order in the log after N seconds.

## Review outcomes (2026-11-07)

### 1. Why Lua: check-then-act races across OMS instances
- Two OMS instances (different JVMs) reading `cash`/`reserved` and then writing is a check-then-act race: both see ₹20,000 available, both reserve ₹15,000, and the account is overspent. A Java lock doesn't help because the instances are separate processes.
- Redis runs one command (or one Lua script) at a time, so check + reserve in one script is atomic. Scripts must stay short and O(1): while one runs, **every** other Redis client waits.
- Sell orders reserve shares the same way (`h:INFY` held vs `rh:INFY` reserved).

### 2. Per-order reservation records (hash tags)
- Totals alone can't be audited or swept, so each reservation also gets `res:{acct:42}:{order_id}` (amount, created timestamp). The `{acct:42}` hash tag puts it in the same Redis Cluster slot as the account hash, which a multi-key Lua script requires.
- The OMS therefore assigns `order_id` **before** the risk check (architecture §3.2 updated).
- Release and fill scripts are idempotent: they act only if the reservation record still exists (or still has quantity left) and delete or shrink it in the same script.

### 3. Redis failover: fail closed, then rebuild
- Async replication: a reservation acknowledged by the primary can be lost if the primary dies before the replica copies it. The promoted replica then shows more buying power than is real (possible overspend).
- **Decision:** on failover, the OMS rejects orders (fail closed) and rebuilds **all** account hashes from the source of truth: ledger balances + open orders (with their reservations). 100k accounts takes seconds. The invariant checker flags any negative cash in the ledger (D4) as a backstop.
- Experiment (Phase 2): `WAIT 1 <timeout>` after each reserve (the primary waits for one replica to acknowledge). Measure the latency cost against L5 and record it in `perf-log/`.

### 4. Leaked reservations: a careful sweeper
- OMS crashes after reserving but before the Kafka produce → money stays reserved with no order behind it.
- The sweeper releases a reservation only if **all** hold: age > producer `delivery.timeout.ms` + margin (after that, the produce has definitely succeeded or failed); the order projection is caught up past the reservation's timestamp (low lag); and no order with that `order_id` exists. Releasing too early, while a real order is still in flight, would let it fill **without** a reservation (overspend).
- One active sweeper cluster-wide (Postgres advisory lock); the release script is idempotent.

### 5. Fill and settlement can arrive in either order
- Lifecycle, e.g. buy 10 @ ₹1501 with ₹20,000 cash: reserve → `reserved += 15,010`. Fill at ₹1500.50 → `reserved −= 15,010`, `pending += 15,005` (price improvement frees ₹5). Ledger settles (`account.events`) → `cash −= 15,005`, `pending −= 15,005`. `available = cash − reserved − pending` is correct at every step.
- Shares work the same way for sells, with `h:INFY` (held), `rh:INFY` (reserved) and `ph:INFY` (sold, delivery not yet settled): `available shares = h − rh − ph`. Proceeds of a sale (cash) and shares from a purchase become usable only once the ledger settles them (`account.events`): you can spend what the ledger says you own.
- `pending` exists because Redis `cash` mirrors the ledger. If the OMS also debited `cash` on the fill, the later `account.events` update would debit it a second time.
- The OMS reads `exec.events` and `account.events` from different topics, so the settlement can arrive **before** the fill. Both scripts take the `trade_id` and use the marker `st:{acct:42}:{trade_id}`: whichever arrives second sees the marker and skips its `pending` adjustment. Covered by a test that delivers the two events in both orders.

## Revisit when
- Redis round trips dominate order-ack latency → consider Option B.
- Failover-induced rejects become unacceptable → consider `WAIT`-based sync replication or Option B.
