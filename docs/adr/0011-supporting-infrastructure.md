# ADR-0011: Supporting infrastructure: HAProxy, MinIO, Kafka UI

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** A2, A5, T4, ADR-0007, ADR-0009

## Context
Reviewing the tool list exposed two gaps and one quality-of-life need:
1. The architecture runs several gateway and WS gateway instances "behind a load balancer" but never chose one. WebSockets need connection-aware balancing and graceful draining.
2. Engine snapshots live on local disk. A hot standby matcher (Phase 6, A2) on another host can't read them.
3. While learning Kafka, there's no visual way to inspect topics, messages and consumer lag.

## Decision

| Need | Tool | Phase | Why this one |
|---|---|---|---|
| Load balancer | **HAProxy** | 3 (gateway), 4 (WS) | Mature, very fast, first-class WebSocket support, built-in stats page and Prometheus exporter, simple config. (Envoy is more powerful but much more complex; Nginx is fine but HAProxy's runtime API and draining are better for chaos tests) |
| Snapshot storage | **MinIO** (S3-compatible) | 2 (optional), 6 (required) | Runs locally in Docker; teaches the S3 API used across clouds; can also back Kafka tiered storage experiments |
| Kafka inspection | **Kafka UI** (`kafbat/kafka-ui`) or **Redpanda Console** | 0 | Browse topics, messages, consumer groups and lag in a browser. Dev-only |

### Considered and deferred
| Tool | Decision |
|---|---|
| ClickHouse | Optional analytics experiment after Phase 5 |
| Debezium (CDC) | Phase 5 experiment: compare with the hand-written outbox relay |
| Schema Registry | Phase 6 if schema evolution becomes painful |
| Loki | Add if `docker logs` becomes painful |
| ZooKeeper/etcd, Flink, RabbitMQ/NATS, Aeron, Elasticsearch, Cassandra | Not needed. Covered by KRaft, Kafka group membership/fencing, hand-built stream processing, Redis Pub/Sub, Postgres |

## Consequences
- **Positive:** closes the two design gaps; makes Kafka much easier to learn.
- **Negative:** three more containers (small: HAProxy and MinIO each < 100 MB RAM).
- **Risks:** HAProxy becomes a single point of failure → acceptable in dev; in Phase 6 (Kubernetes) the ingress layer replaces it.

## Review outcomes (2026-11-07)

### 1. HAProxy: least-connections for WebSockets, staged draining
- HTTP requests are short, so round robin spreads them well. WebSocket connections live for hours, so round robin can leave one gateway with far more open connections (e.g. after a restart, the restarted gateway sits empty). WS backends use `balance leastconn`; the HTTP gateway uses `roundrobin`.
- Rolling deploys: put the server into `drain` through the HAProxy runtime API (no new connections), let the gateway close clients gradually with a reconnect message and random delays (jitter), then stop it. Health checks hit `/actuator/health/readiness`, which turns DOWN at the start of shutdown.
- Single point of failure: accepted for local dev. Production would run two HAProxies with a floating IP (keepalived) or a cloud/Kubernetes load balancer (Phase 6).

### 2. MinIO: shared snapshot storage, with a "latest" pointer
- Local disk works only if the same machine recovers. A standby on another host (Phase 6) needs shared storage; MinIO speaks the S3 API used by every cloud.
- Write path: write locally (temp → fsync → rename, ADR-0009) → upload `snapshots/p3/<offset>.bin` → **then** overwrite `snapshots/p3/LATEST` with that object name. A crash mid-upload leaves `LATEST` pointing at the previous complete snapshot. Recovery reads `LATEST`, verifies the checksum, and falls back to listing older objects if needed.
- Upload runs on the snapshot-writer thread, never the engine thread.

### 3. Kafka UI: dev-only, read-only
- It shows message contents (account IDs, orders) and can delete topics or reset consumer offsets. It runs only in the dev compose file, bound to localhost, with read-only mode on (`KAFKA_CLUSTERS_0_READONLY=true`) except when deliberately experimenting.

### 4. Rule for adding tools
- A tool is added when a measured pain or a requirement needs it, not in advance. Each deferred tool in the table above lists the trigger that would bring it in.

## Revisit when
- Moving to Kubernetes (Phase 6): HAProxy → Kubernetes ingress / Gateway API.
