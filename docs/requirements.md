# Requirements

Requirements come in two kinds:
- **Functional**: what the system does.
- **Non-functional (NFRs)**: how well it does it (speed, scale, durability, availability, correctness). Every NFR here has a **number** and a **way to verify it**. An NFR without a test is a wish.

> All targets assume a **single developer machine** running Docker Compose (~8–16 cores, 16–32 GB RAM, NVMe SSD). They're deliberately ambitious for that hardware: hitting them should take real optimization work. Phase 6 records the hardware actually used.

---

## 1. Functional requirements

### Trading
| ID | Requirement |
|---|---|
| F1 | Place a **limit** order (BUY/SELL, symbol, qty, price, TIF = GTC or IOC) |
| F2 | Place a **market** order with price protection (max 3% from last trade price) |
| F3 | **Cancel** a resting order (whole remaining quantity) |
| F4 | Match orders by **price-time priority**; trades execute at the resting order's price |
| F5 | Notify the order owner of every state change: accepted, rejected (with reason), partial fill, fill, cancelled |
| F6 | Duplicate submissions with the same `client_order_id` must **not** create a second order (return the original result) |

### Risk & accounts
| ID | Requirement |
|---|---|
| F7 | Reject buys exceeding available buying power (cash − reserved) |
| F8 | Reject sells exceeding available holdings (shares − reserved for open sells) |
| F9 | Reserve funds/shares at acceptance; release on cancel; settle on fill |
| F10 | Per-user rate limit (default 50 orders/s), plus fat-finger limits (max qty, price band ±10%) |
| F11 | Deposit / withdraw fake cash (admin API) |

### Market data
| ID | Requirement |
|---|---|
| F12 | Stream **trades** and **L2 depth (top 10 levels)** per symbol over WebSocket |
| F13 | Provide a snapshot + sequenced deltas; clients can detect gaps and re-sync |
| F14 | 1-second and 1-minute **candles** (OHLCV), live and historical |

### Ledger & reporting
| ID | Requirement |
|---|---|
| F15 | Every trade settles into a **double-entry ledger** (cash and shares) |
| F16 | Query positions, balances, order history, trade history, realized/unrealized P&L |
| F17 | A **reconciliation** job verifies ledger invariants and cross-checks Redis risk state |

### Simulation
| ID | Requirement |
|---|---|
| F18 | Load generator: N traders + M market makers with realistic order mix and a configurable hot-symbol skew |
| F19 | WS subscriber simulator: N clients, configurable symbols per client, configurable slow-client fraction |

---

## 2. Non-functional requirements

### 2.1 Latency (all p99 unless stated; measured with open-loop load at the target throughput)

| ID | Path | Target | Why this number |
|---|---|---|---|
| L1 | **Engine core**: one order through the in-memory book (benchmark, no I/O) | p99 ≤ **10 µs**, p99.9 ≤ 50 µs | Pure CPU and memory; this is where the data-structure work shows |
| L2 | **Order ack**: client POST → durable in Kafka → response | p99 ≤ **15 ms** | Dominated by Kafka `acks=all` replication (~2–10 ms locally); leaves room for gateway, risk and Redis |
| L3 | **Order → fill notification** to the client | p99 ≤ **50 ms** | Two Kafka hops + engine + OMS |
| L4 | **Tick-to-client**: engine emits book change → WS client receives it | p99 ≤ **50 ms** with 10k clients | Real-time feel; < 100 ms is imperceptible to humans |
| L5 | **Risk check** (Redis Lua reserve) | p99 ≤ **1 ms** | One Redis round trip; anything more means contention or a bad script |
| L6 | **Trade → settled in ledger** | p99 ≤ **1 s** | Settlement isn't latency-critical, but lag must stay bounded |
| L7 | Trade history query (last 100 trades of an account) | p99 ≤ **20 ms** | Interactive UI |

### 2.2 Throughput

| ID | Target | Notes |
|---|---|---|
| T1 | Engine core (single thread, one symbol): **≥ 1,000,000 orders/s** | Library benchmark |
| T2 | End-to-end sustained: **≥ 10,000 orders/s** across 50 symbols, meeting L2–L4 | Whole system on one machine |
| T3 | Hot symbol: one symbol at **≥ 50%** of T2 traffic without violating L2/L3 | Forces you to confront the hot partition |
| T4 | Market data: **10,000 concurrent WS clients**, each subscribed to 5 symbols | Fan-out pressure |
| T5 | Ledger: settle **≥ 20,000 trades/s** (burst), with lag recovering within 60 s after a 5× burst | Batching required |

### 2.3 Durability & correctness (zero-tolerance)

| ID | Requirement | Verified by |
|---|---|---|
| D1 | **No acknowledged order is ever lost**, including when any single broker or service is killed | Chaos test: compare client ack log vs engine input log |
| D2 | **No double fills**: total filled qty ≤ order qty, always; no fill after a final state | Invariant checker consuming `exec.events` |
| D3 | **Ledger always balances**: per asset, Σ debits = Σ credits | Reconciliation job, plus a DB-level check |
| D4 | **No overspend**: available cash and holdings never go negative under any concurrency | Concurrency stress test (1,000 parallel orders on one account) |
| D5 | **Deterministic engine**: replaying the same input log produces byte-identical output | Replay test in CI |
| D6 | Duplicate client submissions never create duplicate orders | Retry-storm test with injected timeouts |

### 2.4 Availability & recovery

| ID | Requirement | Target |
|---|---|---|
| A1 | Matching engine crash → resume on the same shard | RTO ≤ **10 s** (snapshot + replay); RPO = **0** |
| A2 | Engine failover with hot standby (Phase 6) | RTO ≤ **2 s** |
| A3 | Loss of 1 Kafka broker | No data loss, no write unavailability (RF=3, min.insync=2) |
| A4 | Redis primary failure | Recover within 30 s; risk state rebuilt or verified from the source of truth; no overspend during the gap (fail closed) |
| A5 | Any stateless service instance dies | No user-visible errors beyond in-flight retries |
| A6 | Graceful degradation | If market data is down, trading continues. If the ledger lags, trading continues (bounded by reservations) |

### 2.5 Resource & stability

| ID | Requirement |
|---|---|
| R1 | **4-hour soak** at 70% of T2: heap growth < 10%, p99 drift < 20%, zero thread/FD/connection leaks |
| R2 | Engine hot path: **0 allocations per order** in steady state (verified by JMH `-prof gc`: `gc.alloc.rate.norm` ≈ 0 B/op) |
| R3 | JVM GC pauses p99 < 1 ms in matcher and WS gateway (generational ZGC) |
| R4 | One slow WS client cannot raise latency for others (isolation test) |

### 2.6 Operability

| ID | Requirement |
|---|---|
| O1 | Every service exposes Prometheus metrics (RED + domain metrics) |
| O2 | Distributed tracing across Kafka hops for a sampled % of orders |
| O3 | Dashboards for each NFR above; alerts on lag, error rate, invariant violations |
| O4 | `make up` brings up the full stack; `make load` and `make chaos` run the standard scenarios |
| O5 | Benchmarks in CI; a regression > 5% fails the build |

---

## 3. Constraints & assumptions

- 50 symbols (synthetic names), with prices in ticks of ₹0.05
- ~100k simulated accounts
- Always-open continuous market (no auctions or trading hours)
- Single broker, single exchange, single currency (INR)
- No authentication beyond API keys
- Message encoding: protobuf on Kafka; JSON or protobuf over WebSocket

## 4. Out of scope

Real money and market connectivity, KYC and regulation, derivatives, multi-currency, short selling and margin, auctions, the UI beyond a minimal debug page, multi-region deployment.

---

**Next:** the [architecture](architecture.md), then the [system walkthrough](system-walkthrough.md).
