# ADR-0003: Single-threaded matching engine per partition, sharded by symbol

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** L1, T1, T3, D2, D5

## Context
Matching must be strictly ordered per symbol (price-time priority is defined by arrival order), deterministic for replay, and fast. We must decide how to parallelize.

## Options considered

### Option A: Multi-threaded engine with locks per book
- Pros: any thread can process any order.
- Cons: lock contention on hot books; arrival order becomes "lock acquisition order" (non-deterministic); deadlock risk with multi-book operations; replay can't reproduce results.

### Option B: One thread per symbol
- Pros: no locks; deterministic per symbol.
- Cons: 50+ threads, mostly idle; thousands of symbols wouldn't scale; mapping symbols to Kafka partitions 1:1 means too many partitions.

### Option C: One thread per Kafka partition; each thread owns all books for the symbols hashed to it
- Pros: the Kafka partition is already a totally ordered log, so one consumer thread processing it in order gives determinism for free. No locks, cache-friendly (one core owns its data). Scale out by adding partitions/instances.
- Cons: a single symbol can never use more than one core. Hot symbols share a thread with whatever else hashes to the same partition.

## Decision
**Option C.** The Single Writer Principle: each book has exactly one mutating thread. 16 partitions → up to 16 engine threads across matcher instances. A custom partitioner may pin known-hot symbols to dedicated partitions.

## Consequences
- **Positive:** zero locks on the hot path; deterministic (D5); straightforward reasoning; mirrors how LMAX and most real exchanges work.
- **Negative:** per-symbol throughput ceiling = one core. Fine: a well-written engine does > 1M orders/s per core, far above our 10k/s total, so I/O is the actual limit. Rebalancing a partition to another instance means moving its state (snapshot + replay).
- **Risks:** a slow operation (JVM GC pause, JIT deoptimization, logging, syscall) on the engine thread stalls every symbol in that partition → mitigation: keep I/O off the engine thread (ring buffers), zero allocations (R2).

## Review outcomes (2026-11-07)

### 1. Hot symbols: custom partitioner, explicit symbol → partition table
- A hot symbol slows everything sharing its partition. Matching (~1 µs) is not the cost; the shared per-partition I/O is (Kafka fetch, `exec.events` produce, transaction commits). Neighbours see higher, jumpier latency and consumer lag.
- **Decision:** the producer uses a custom partitioner that checks an explicit table first (`INFY → 0`), then falls back to `hash(symbol) % 15 + 1` for everything else. A pinned partition can run on its own matcher instance.
- Metric: consumer lag **per partition**, plus per-symbol order rate, to spot the next hot symbol.
- If a pinned symbol grows further: its engine has ~100× CPU headroom (1M/s vs ~5k/s), so the next limit is the partition's I/O (produce + commit batching), which we tune with batch size and commit interval. A single symbol needing > 1 core is listed under Revisit.

### 2. Failover: snapshot + replay, and replay is silent
- The book exists only in memory, so a new owner must rebuild it **before** matching anything (matching against an empty book produces wrong trades).
- Steps: detect (heartbeat timeout, ~3–6 s) → reassign → load the latest snapshot (tagged with its offset) → replay from snapshot offset + 1 → go live. Budget about 5–10 s (A-requirement RTO ≤ 10 s).
- **Replay time scales with snapshot age**, so snapshot every 30 s or 100k events, whichever comes first (ADR-0009), not hourly.
- **Silent replay:** commands up to the last *committed* offset already produced their outputs. Replaying them only rebuilds state and emits nothing; re-emitting would send duplicate fills (= money moving twice). Commands after the committed offset are processed normally. The new owner's transactional producer fences the old one, aborting its half-written transaction (ADR-0006).
- Phase 6: hot standby follows the partition continuously; takeover ≈ 1–2 s.

### 3. Stalls: the engine thread only does CPU work
- One frozen thread stalls every symbol in its partition (orders queue safely in Kafka but latency spikes).
- Rules for the engine thread: no I/O (Kafka reader/writer threads connected through ring buffers: `ArrayBlockingQueue` first, Disruptor later); no logging; zero allocations in steady state (R2); ZGC; warm up before taking traffic; spare cores, no swap.
- Measure every engine loop with HdrHistogram; alert on **max**, not average.

### 4. Partition count: fixed, with headroom
- Partitions cost broker files × replicas, leader elections, one engine thread + one transactional producer each, snapshot files, and smaller batches. One per symbol does not scale (5,000 symbols × 3 topics × RF 3 = 45,000 replicas) and leaves most threads idle.
- **Rule:** size partitions for the parallelism you need, not the number of things you have. We keep 16.
- Never change the partition count on a live topic: `hash % 16 ≠ hash % 32`, so symbols would move without their books. Symbols move only deliberately via the explicit table (pause → snapshot → move → resume).

## Revisit when
- A single symbol needs more than one core (it won't at our scale).
- Cross-symbol orders (spreads, baskets) are introduced. These need multi-book atomicity.
