# ADR-0001: Go as the primary language

- **Status:** Superseded by [ADR-0010](0010-java-and-spring-boot.md). The premise that Go was the developer's strongest language was wrong
- **Date:** 2026-11-07
- **Related:** L1, T1, R2, R3

## Context
We need one language for ~8 services plus a latency-sensitive matching engine. Learning goals: write code that performs under load, understand concurrency deeply, and spend the time on system design rather than fighting tooling. The developer knows Java and Go and is open to a new language.

## Options considered

### Option A: Go everywhere
- Pros: excellent for network services; goroutines make 10k WebSockets easy; first-class profiling (`pprof`, `go tool trace`); fast builds; strong Kafka (`franz-go`, with transactions) and Redis (`go-redis`) clients; already known, so ramp-up is fast.
- Cons: garbage collector. Allocation-heavy code shows up as p99 spikes. Less control over memory layout than Rust/C++.

### Option B: Java everywhere
- Pros: the reference language for low-latency trading (LMAX Disruptor, Chronicle, Aeron); excellent Kafka ecosystem (Kafka Streams is Java-native); JIT reaches very high throughput; JFR/async-profiler.
- Cons: heavier services and slower iteration; GC tuning is a deep topic on its own; more boilerplate.

### Option C: Rust everywhere
- Pros: no GC, predictable latency, full control over memory, and the strictest correctness guarantees. Learning it is a goal in itself.
- Cons: steep learning curve on top of learning Kafka/Redis/the domain all at once; slower iteration; async Rust adds complexity.

### Option D: Go for services, Rust for the matching engine
- Pros: right tool per job; FFI or a process boundary teaches interface design.
- Cons: two toolchains from day one; the engine is the first thing we build, so we'd learn Rust before anything else.

## Decision
**Go everywhere** for Phases 0–6. The GC is treated as a *feature for learning*: requirement R2 (zero allocations on the hot path) and R3 (GC pause p99 < 1 ms) force the same discipline as low-latency Java. In **Phase 7 (stretch)**, the matching engine library may be re-implemented in Rust behind the same interface and benchmarked against the Go version.

## Consequences
- **Positive:** fastest path to a working system; one toolchain; the GC pressure is visible and teachable.
- **Negative:** the engine's absolute floor is higher than Rust/C++. Java's mature low-latency libraries (Disruptor) aren't available, so we build simpler equivalents.
- **Risks:** Go channels may be too slow for the engine's input/output queues → mitigation: benchmark channels vs a lock-free ring buffer in Phase 2.

## Revisit when
- The engine can't meet L1 (p99 ≤ 10 µs) after allocation-free optimization.
- Kafka Streams-style stateful processing becomes central (Java's advantage).
