# ADR-0007: Market data fan-out via Redis Pub/Sub with sequence numbers and snapshots

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** F12, F13, L4, T4, R4

## Context
Book changes must reach up to 10k WS clients (each subscribed to ~5 symbols) with p99 ≤ 50 ms, while one slow client must not affect others. We have N WS gateway instances, and each needs updates for the symbols its clients care about.

## Options considered

### Option A: Every WS gateway consumes `md.events` from Kafka directly (one consumer group per gateway)
- Pros: durable, replayable; no extra component.
- Cons: each gateway reads **all** partitions even if its clients want 3 symbols; N gateways = N full copies of the stream from Kafka; adding a gateway means a new consumer group and offset management; the book-building logic is duplicated in each gateway.

### Option B: Market Data Service builds books → Redis Pub/Sub channel per symbol → gateways subscribe only to the symbols they need
- Pros: one Kafka consumer; gateways get only relevant symbols; Pub/Sub is very low latency; book state is built once.
- Cons: Pub/Sub is **fire-and-forget**. A gateway disconnected for 1 s misses messages. Redis becomes a fan-out bottleneck at very high rates.

### Option C: Redis Streams per symbol
- Pros: stored, so gateways can resume from an ID after a blip.
- Cons: per-gateway read loops and trimming policy; slightly higher latency; more memory.

### Option D: A dedicated broker (NATS, or a multicast-style UDP feed like real exchanges)
- Pros: purpose-built for fan-out.
- Cons: another technology; out of our learning focus.

## Decision
**Option B**, made safe with **sequence numbers + snapshots**:
- Every delta carries a per-symbol `md_seq`.
- `md:{SYM}:book` in Redis always holds the latest snapshot with its `md_seq`.
- Gateways (and clients) detect gaps and re-fetch the snapshot. Loss becomes a **latency** event, not a **correctness** event.
- Gateways apply **per-client conflation** and disconnect clients persistently behind.

Option C will be **benchmarked** against B in Phase 4 (an experiment, not a decision).

## Consequences
- **Positive:** low latency, efficient fan-out, a clean recovery protocol, and a good tour of Redis Pub/Sub's limits.
- **Negative:** Redis output bandwidth is shared with risk/rate-limiting traffic → may need a **separate Redis instance** for market data (likely; noisy-neighbour isolation).
- **Risks:** Redis `client-output-buffer-limit` disconnecting a slow *gateway* → gateway resubscribes + resnapshots; alert on it.

## Review outcomes (2026-11-07)

### 1. Why not every gateway reading Kafka
- With N gateways, Kafka sends N full copies of `md.events` (all 50 symbols), each gateway rebuilds every book, and each needs its own consumer group. The Market Data Service builds books **once** and Redis forwards each symbol only to gateways that subscribed to it.
- Pub/Sub is "radio": whoever is listening right now hears the message, and nothing is stored. That is acceptable only because of outcome 2.

### 2. Gaps are detected by `md_seq` and repaired with a snapshot
- Each delta carries the symbol's `md_seq`, which increases by exactly 1. A gateway that last applied 100 and then receives 151 knows it missed 101–150: it fetches `md:{SYM}:book` (snapshot + its `md_seq`) and continues from there. Clients apply the same rule to the gateway's stream.
- Loss costs latency (one snapshot fetch), never correctness.

### 3. Subscribe-then-snapshot ordering (avoids a race)
- Fetching the snapshot first and subscribing second loses any delta published in between, with no gap visible.
- **Order:** subscribe → buffer incoming deltas → fetch the snapshot (seq S) → drop buffered deltas ≤ S → apply the rest. If the first remaining delta is > S + 1, fetch again.
- The MDS updates the snapshot and publishes the delta in **one Lua script**, so a snapshot with seq S always reflects exactly deltas ≤ S.

### 4. Slow clients: conflation, then disconnect
- Queuing every delta for a slow client grows memory without bound and delivers ever-staler prices. Each client instead has a mailbox holding only the **latest state per symbol** (conflation, [walkthrough §12.4](../system-walkthrough.md#124-slow-phones-keep-only-the-latest)). A slow client gets fewer but current updates (as compact snapshots, since merged deltas are complicated); a client that stays behind past a deadline is disconnected with `SLOW_CONSUMER`.
- The fan-out thread never writes to a socket, so a slow client can't delay others (R4).

### 5. Isolation inside Redis
- **Separate Redis instance for market data.** Pub/Sub fan-out bandwidth must not slow the risk Lua scripts (ADR-0005), and a risk-Redis failover must not interrupt market data.
- If a gateway itself is slow, Redis disconnects it at `client-output-buffer-limit pubsub` (tuned, e.g. `32mb 8mb 60`). The gateway resubscribes and resnapshots (outcome 3). Metric + alert on these disconnects.

## Revisit when
- Fan-out bandwidth exceeds what one Redis can push (> ~1 GB/s) → shard channels across Redis instances or move to Option D.
