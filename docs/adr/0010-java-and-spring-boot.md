# ADR-0010: Java 25 + Spring Boot for all services

- **Status:** Accepted
- **Date:** 2026-11-07
- **Supersedes:** [ADR-0001](0001-go-as-primary-language.md)
- **Related:** L1–L5, T1, R2, R3, ADR-0003, ADR-0004

## Context
ADR-0001 chose Go on the assumption that it was the developer's strongest language. It isn't: the developer is stronger in Java and would need significant ramp-up in Go. Learning a new language competes for attention with the real goals (domain, Kafka/Redis/Postgres, distributed systems, performance). We also considered a Java + Go split (Java on the order path, Go for fan-out and tooling) and rejected it in favour of a single language.

Spring Boot is the industry-standard Java service framework and is directly relevant to the developer's career. Its risk for this project: it hides threads, offset commits, transactions and serialization, which are exactly the things we want to learn.

## Options considered

| Option | Pros | Cons |
|---|---|---|
| Go everywhere (ADR-0001) | Light services, easy concurrency | Language learning slows every phase |
| Java + Go split | Each language where it's strongest | Two toolchains; cross-language contracts; still a Go ramp-up |
| Plain Java, no framework | Maximum visibility | Lots of hand-written wiring; less career-relevant |
| **Java + Spring Boot everywhere** | Strongest language; industry standard; productive | Framework hides internals; heavier memory; profiling noise |

## Decision
**Java 25 (LTS) and Spring Boot (latest stable 4.x) for all services**, with guardrails so the framework doesn't hide what we're here to learn:

1. **The matching engine (`engine` module) is a plain Java library with zero Spring dependencies.** It's pure logic with no I/O, so there's nothing for a framework to do. Enforced by an ArchUnit test.
2. **Matcher uses the raw Kafka client APIs** (`KafkaConsumer`, transactional `KafkaProducer`) as Spring-managed beans with explicit lifecycle (`SmartLifecycle`), not `@KafkaListener`. Phase 2 is about understanding transactions, offsets and rebalances, so we write that loop ourselves.
3. **Other consumers (OMS, ledger, market data, candles):** write the first one with raw clients; after that, Spring Kafka (`@KafkaListener`) is allowed, once you can explain what it does underneath (container threads, ack modes, `AfterRollbackProcessor`).
4. **Persistence: Spring JDBC (`JdbcClient`) + Flyway**, explicit SQL. **No JPA/Hibernate on the ledger** (batch inserts, `ON CONFLICT`, row-lock control). JPA is acceptable for simple admin CRUD.
5. **Transactions:** `@Transactional` is allowed, but every use names its isolation level explicitly, and the ledger's batch transaction is written so the commit point is obvious.
6. **Redis: Spring Data Redis with Lettuce**; Lua via `RedisScript`, loaded from the shared `redis/scripts/` folder.
7. **Virtual threads on** (`spring.threads.virtual.enabled=true`) for request-handling services.
8. **Observability: Micrometer → Prometheus**, Micrometer Tracing → OpenTelemetry → Jaeger.
9. **Hot-path libraries:** LMAX Disruptor (ring buffers), Agrona (primitive collections), HdrHistogram.
10. **GC:** generational ZGC for the matcher and WS gateway (R3); G1 default elsewhere. GC choice is an experiment knob.

### Toolchain
| Concern | Tool |
|---|---|
| Build | Maven, multi-module (parent POM with `spring-boot-starter-parent`; Maven Wrapper `./mvnw`) |
| Unit tests | JUnit 5, AssertJ |
| Property-based tests | jqwik |
| Concurrency tests | jcstress (memory model), stress tests with `ExecutorService` |
| Fuzzing | Jazzer |
| Integration | Testcontainers (Kafka, Redis, Postgres) |
| Architecture rules | ArchUnit |
| Microbenchmarks | JMH (`-prof gc` for allocations) |
| Profiling | async-profiler (flamegraphs), JFR + JDK Mission Control |
| Static analysis | Error Prone, Spotless (formatting) |
| Load generator | Plain Java 25 (virtual threads), HdrHistogram |

## Consequences
- **Positive:** one language and toolchain; you can be productive from day one; career-relevant; access to the Java low-latency ecosystem (Disruptor, Agrona, ZGC, JMH).
- **Negative:** each Spring Boot service uses ~200–400 MB of RAM. With ~8 services + Kafka ×3 + Redis + Postgres on one laptop, cap heaps (`-Xmx`) and consider co-locating services in dev. Slower startup (irrelevant for us). Framework frames add noise to flamegraphs.
- **Risks:**
  - *The framework hides the lesson.* Mitigated by guardrails 2–5 and the rule "you must be able to explain what the annotation does underneath."
  - *JIT warm-up distorts benchmarks.* JMH handles warm-up for microbenchmarks; load tests exclude a warm-up period.
  - *Object overhead on the hot path* (headers, boxing, records). See the revised ADR-0004 (primitive `long` on the hot path).

## Revisit when
- A service can't meet its gate and profiling attributes the cost to the framework → strip Spring from that service only, and record it in `perf-log/`.
- Memory on the dev machine becomes the bottleneck → co-locate services or move to Kubernetes with resource limits.
