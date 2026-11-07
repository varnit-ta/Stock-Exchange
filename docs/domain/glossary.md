# Glossary

Domain terms first, then engineering terms used throughout the docs.

## Trading terms

| Term | Meaning |
|---|---|
| **Ask / Offer** | A sell order resting in the book; the price someone is willing to sell at |
| **Best bid / Best ask** | Highest resting buy price / lowest resting sell price |
| **Bid** | A buy order resting in the book; the price someone is willing to pay |
| **Book (order book)** | Per-symbol collection of all resting limit orders, bids and asks sorted by price-time |
| **Broker** | Licensed intermediary that holds client accounts and sends their orders to the exchange |
| **Cancel** | Instruction to remove the unfilled remainder of a resting order |
| **Candle (OHLCV)** | Summary of trades in a time window: Open, High, Low, Close price and Volume |
| **Clearing house** | Entity that stands between buyer and seller after a trade and guarantees settlement |
| **Client order ID** | ID chosen by the client for its own order; used for deduplication (idempotency) |
| **Crossed book** | Best bid ≥ best ask. Should never persist; it means matching failed |
| **Depth / L2** | Aggregated quantity at each price level |
| **Double fill** | A bug where the same quantity is traded twice. Catastrophic |
| **Execution report** | Message to the order owner about a state change (accepted, filled, cancelled…) |
| **Fill** | A (partial) execution of an order. One trade produces a fill for each side |
| **IOC** | Immediate-Or-Cancel: match what's possible now, cancel the remainder |
| **Limit order** | Order with a worst acceptable price |
| **Liquidity** | How easily you can trade without moving the price; lots of resting orders = liquid |
| **LTP** | Last traded price |
| **Maker / Taker** | Maker = resting order that provided liquidity; Taker = incoming order that removed it |
| **Market maker** | Participant who continuously quotes both bid and ask |
| **Market order** | Order to trade immediately at the best available prices |
| **Matching engine** | The component that applies price-time priority and produces trades |
| **Partial fill** | Only part of the order's quantity has traded |
| **Position** | Quantity of a symbol held by an account |
| **Pre-trade risk** | Checks before an order reaches the market (funds, holdings, limits) |
| **Price level** | All resting orders on one side at one price, kept as a FIFO queue |
| **Price-time priority** | Matching rule: best price first; at equal price, earliest order first |
| **P&L (realized / unrealized)** | Profit or loss locked in by closing positions / on open positions at market price |
| **Reservation** | Buying power or shares set aside for an open order so they can't be used twice |
| **Settlement** | The actual exchange of cash and shares after a trade |
| **Spread** | Best ask − best bid |
| **Symbol / Instrument** | The thing being traded, e.g. `INFY` |
| **Tick** | (1) A single trade/price update. (2) The minimum price increment, e.g. ₹0.05 |
| **Time in force (TIF)** | How long an order stays alive: GTC, DAY, IOC |
| **Top of book / L1** | Best bid and best ask with their quantities |

## Engineering terms

| Term | Meaning |
|---|---|
| **ADR** | Architecture Decision Record: short doc capturing one decision, its options and trade-offs |
| **At-least-once / At-most-once / Exactly-once** | Delivery guarantees: may duplicate / may lose / neither (effectively) |
| **Backpressure** | Mechanism for a slow consumer to slow down a fast producer instead of being overwhelmed |
| **Conflation** | When a consumer is slow, send only the latest state instead of every intermediate update |
| **Consumer group** | Kafka: set of consumers sharing a topic's partitions; each partition goes to one member |
| **Coordinated omission** | Load-testing mistake where a stalled system also stalls the tester, hiding bad latency |
| **Event sourcing** | Storing state as an ordered log of events; current state = replay of the log |
| **Fan-out** | Delivering one message to many recipients |
| **Hot key / hot partition** | One key or partition receiving a disproportionate share of traffic |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **ISR** | Kafka in-sync replicas: replicas fully caught up with the leader |
| **Offset** | Kafka: a message's position in a partition |
| **Outbox pattern** | Write the DB change and the "event to publish" in one DB transaction; a relay publishes later |
| **p50 / p99 / p99.9** | Latency percentiles: 99% of requests are faster than p99 |
| **Partition** | Kafka: an ordered, append-only shard of a topic |
| **Projection / read model** | A view of state built by consuming events (e.g. order status table built from execution events) |
| **RPO / RTO** | Recovery Point Objective (how much data you can lose) / Recovery Time Objective (how long you can be down) |
| **Sequence number** | Monotonic counter on messages so receivers can detect gaps and duplicates |
| **Snapshot** | Saved copy of state at a known log position, to speed up recovery |
| **SLO** | Service Level Objective: a measurable target such as "p99 < 20 ms" |
| **Soak test** | Running under steady load for hours to find leaks and drift |
| **Write amplification** | Bytes physically written ÷ bytes logically written |
