# ADR-0008: Double-entry ledger in Postgres as the financial source of truth

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** F15–F17, D3, L6, T5

## Context
Cash and share balances must be exactly right, auditable, and recoverable. Every movement must be explainable ("why did my balance change?"). Settlement may lag trading by up to ~1 s (L6) but must keep up with bursts (T5).

## Options considered

### Option A: Mutable balance columns (`UPDATE balances SET cash = cash - x`)
- Pros: simple; fast.
- Cons: no audit trail; a bug silently corrupts balances with no way to find when or why; no invariant to check.

### Option B: Double-entry ledger (append-only entries + derived balances)
- Each business event = one **journal** with ≥ 2 **entries** whose signed amounts sum to zero per asset. Balances = running sum, updated in the same transaction.
- Pros: built-in invariant (Σ = 0) catches bugs immediately; full audit trail; balances can always be recomputed from entries; this is how real financial systems work.
- Cons: more rows (≥ 4 per trade); more design thought.

### Option C: Event-sourced balances purely from Kafka (no DB)
- Pros: consistent with ADR-0002.
- Cons: ad-hoc queries, constraints and multi-row invariants are much harder; teaches less about relational transactions, which is a learning goal.

## Decision
**Option B in Postgres.** Per trade (buyer B, seller S, qty q, price p):

| Account | Asset | Amount |
|---|---|---|
| B | INR | −q·p |
| S | INR | +q·p |
| B | SYM | +q |
| S | SYM | −q |

- `journal.event_id` is UNIQUE, so duplicate trade events are no-ops (idempotent consumer).
- Fills from `exec.events` are **batched** (e.g. up to 500 per transaction or 10 ms) to meet T5.
- `balances` has a `CHECK (amount >= 0)` for user accounts. A violation is a bug alarm, not a business rule (risk should have prevented it).
- Outbox → `account.events` for the Redis cache.

## Consequences
- **Positive:** correctness you can prove; reconciliation is simple SQL; balances are recomputable.
- **Negative:** write volume (~4 entry rows per trade); hot-account row contention on `balances` → mitigation: aggregate per account within a batch before updating.
- **Risks:** both fills of one trade arrive in different batches or consumers (buyer and seller fills are separate events) → decision: settle per **trade**, keyed by `trade_id`, so the matcher emits a single `TradeSettlement` record covering both sides, or the ledger pairs fills by `trade_id`. **Resolved in review: `TradeSettlement` (outcome 2).**

## Review outcomes (2026-11-07)

### 1. Why double-entry: money can't appear or vanish unnoticed
- With `UPDATE balances SET cash = cash - x`, a wrong balance has no history: you can't tell when or why it changed.
- With double-entry, every movement is a journal whose entries sum to zero per asset. A bug that creates or destroys money breaks the sum and is caught immediately (invariant checker + a deferred constraint trigger in tests). Any balance can be explained ("show the entries") and recomputed (`SUM(amount)`).
- Sign convention: `amount` is the signed change to that account's holding (+ gains, − loses). We avoid debit/credit jargon in code.

### 2. One `TradeSettlement` event per trade (resolves the open item)
- Buyer and seller `Fill`s are separate events and could land in different batches; posting one without the other leaves a half-trade that doesn't balance.
- **Decision:** the matcher emits one `TradeSettlement` per trade (both accounts, qty, price, `trade_id`) on `exec.events`, in the same Kafka transaction as the fills. The ledger consumes only these and posts one balanced journal per trade, keyed by `trade_id` (`journal.event_id` UNIQUE). `Fill`s stay for the OMS and clients.

### 3. Hot accounts and deadlocks: aggregate, then update in sorted order
- The broker revenue account appears in every fee posting, and an active trader appears in many trades per batch. Updating the same `balances` row 500 times per batch is wasted work and lock time.
- Within a batch: insert all entries, **sum per (account, asset)** in memory, then update each `balances` row once.
- `exec.events` has 16 partitions, so two ledger consumers can touch the same account at once (Alice trades INFY and TCS). Updating rows in different orders deadlocks (A locks Alice then waits on Bob; B locks Bob then waits on Alice). **Always update in sorted `(account_id, asset)` order.** Postgres detects any remaining deadlock and aborts one transaction; the batch retries (idempotent via `event_id`).
- Isolation: READ COMMITTED with row locks from `UPDATE` is enough (each update is `amount = amount + delta`, not read-modify-write in Java).

### 4. Money enters through system accounts
- Every journal sums to zero, so a deposit is `EXTERNAL_BANK −20,000 / Alice +20,000`. System accounts (`EXTERNAL_BANK`, `BROKER_REVENUE`, `BROKER_CLEARING`) are allowed to go negative; user accounts are not (`CHECK (amount >= 0)` fires as a bug alarm).
- Fees: the trade journal gains entries (`Alice −4.50 INR / BROKER_REVENUE +4.50 INR`) and still sums to zero. Rounding per ADR-0004.
- Global invariant: the sum over **all** accounts per asset is always zero.

### 5. Outbox for `account.events`
- Writing to Postgres and then publishing to Kafka are two separate systems (the dual-write problem): a crash between them loses or duplicates the update.
- The ledger inserts an `outbox` row in the **same** DB transaction as the journal. A relay (one active instance, advisory lock) publishes outbox rows to `account.events` and marks them sent. It may publish twice after a crash; consumers dedupe by `trade_id` (ADR-0005 markers).

## Revisit when
- Ledger write throughput can't keep up after batching → partition the ledger by account range.
