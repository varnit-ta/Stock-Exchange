# Architecture Decision Records

An **ADR** captures one significant decision: the context, the options considered, what we chose, and **what it costs us**. ADRs are immutable once accepted. If we change our mind, we write a new ADR that supersedes the old one, so the history of reasoning is preserved.

Format: [template](0000-template.md). Status values: `Proposed` → `Accepted` → (`Superseded by ADR-XXXX` | `Deprecated`).

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-go-as-primary-language.md) | Go as the primary language | Superseded by 0010 |
| [0002](0002-event-sourced-core-with-kafka.md) | Event-sourced core: Kafka is the system of record for orders | Accepted |
| [0003](0003-single-threaded-matching-per-shard.md) | Single-threaded matching engine per partition, sharded by symbol | Accepted |
| [0004](0004-integer-fixed-point-money.md) | Integer ticks and paise; no floating point for money | Accepted |
| [0005](0005-pre-trade-risk-in-redis.md) | Pre-trade risk via atomic Lua reservations in Redis | Accepted |
| [0006](0006-exactly-once-processing.md) | Exactly-once: Kafka transactions in the matcher, idempotent consumers elsewhere | Accepted |
| [0007](0007-market-data-fanout.md) | Market data fan-out via Redis Pub/Sub with sequence numbers and snapshots | Accepted |
| [0008](0008-double-entry-ledger-in-postgres.md) | Double-entry ledger in Postgres as the financial source of truth | Accepted |
| [0009](0009-snapshots-and-replay-recovery.md) | Matcher recovery via periodic snapshots and log replay | Accepted |
| [0010](0010-java-and-spring-boot.md) | Java 25 + Spring Boot for all services (with guardrails) | Accepted |
| [0011](0011-supporting-infrastructure.md) | Supporting infrastructure: HAProxy, MinIO, Kafka UI | Accepted |

**How to review an ADR:** for each one, ask: *Are the options real? Is anything missing? Do I agree with the trade-off? What would make us revisit this?* Leave comments inline or write a counter-ADR.
