# ADR-0009: Matcher recovery via periodic snapshots and log replay

- **Status:** Accepted (2026-11-07, after review)
- **Date:** 2026-11-07
- **Related:** A1, A2, D5, ADR-0002, ADR-0006

## Context
The matcher's books live in memory. After a crash we must rebuild **exactly** the same state (D5) within 10 s (A1). Replaying the entire `orders.commands` history from offset 0 gets slower every day and eventually becomes impossible once retention deletes old data.

## Options considered

### Option A: Replay from the beginning
- Pros: trivial.
- Cons: unbounded recovery time; breaks after retention.

### Option B: Periodic snapshots + replay from the snapshot's offset
- Snapshot = serialized books + the last input offset applied.
- Pros: bounded recovery time (snapshot load + replay of ≤ N events); snapshots double as a debugging and audit tool.
- Cons: snapshotting costs time on the engine thread, or needs copy-on-write; snapshot format versioning.

### Option C: Hot standby replica consuming the same partition
- Pros: near-instant failover (A2).
- Cons: needs leader election and fencing (split-brain = two engines emitting trades); doubles resources.

## Decision
**Option B now, Option C in Phase 6.**
- Snapshot every **N events or T seconds** (tunable; start at 100k events / 30 s), written atomically (write temp → fsync → rename), keeping the last 3.
- Snapshot includes: format version, partition, last applied offset, all books (orders in priority order), per-symbol `md_seq`, ID counters.
- Recovery: load the newest valid snapshot → seek to `offset+1` → replay silently up to the committed consumer offset (outputs were already committed transactionally, ADR-0006) → resume with output on.
- **Verification:** CI test: replay full log vs snapshot + partial replay → byte-identical books and outputs.
- Phase 6 hot standby: follows the same partition in replay mode; promotion via Kafka group membership; fencing via the transactional producer ID.

## Consequences
- **Positive:** bounded RTO; determinism tested continuously.
- **Negative:** snapshot pause on the engine thread (measure; if > 1 ms, move to a forked/copy-on-write or double-buffered approach). Snapshots must be kept compatible across code versions.
- **Risks:** a corrupted snapshot → mitigation: checksums; fall back to the previous snapshot (requires retention to cover two snapshot intervals).

## Review outcomes (2026-11-07)

### 1. Consistent snapshots: copy at an event boundary
- The snapshot writer thread must never read the live books while the engine mutates them; it could capture a half-applied event (e.g. a fill that has reduced Bob's resting sell but not yet recorded the trade), producing a "torn" snapshot that never existed.
- **Start:** the engine thread itself copies the books **between two events** (an event boundary), tags the copy with the last applied offset, and hands it to the snapshot-writer thread through a queue. The writer serializes and stores it; only the copy runs on the engine thread.
- Measure the copy pause with HdrHistogram. If > 1 ms, move to double-buffering or a persistent (copy-on-write) book structure.

### 2. Crash-safe writes
- Write `snap-p3-<offset>.tmp` → `fsync` → atomic `rename` to `snap-p3-<offset>.bin` → (later) upload to MinIO. A crash mid-write leaves only a `.tmp` file, which recovery ignores and deletes.
- Every snapshot carries a header (magic, format version, partition, offset) and a CRC32C checksum of the body. Keep the last 3.

### 3. Fallback when the newest snapshot is bad
- Checksum fails or the version is unsupported → try the previous snapshot. Recovery is slower (more replay) but correct, **as long as Kafka still holds the older snapshot's offset**. With snapshots every 30 s and 7-day retention this holds easily; the snapshot-age alerts in ADR-0002 guard it.
- If no usable snapshot exists and the log no longer reaches back far enough: the partition stays halted and pages a human. Never start from an empty book.

### 4. Proving snapshot + replay == full replay
- CI test: engine A replays the whole recorded log; engine B replays half, snapshots, restores into a **fresh** engine, and replays the rest. Compare a hash of the books **and** the full output streams; they must be byte-identical. Run with random split points (jqwik).
- Determinism rules this test enforces: no wall-clock time in the engine (use the command's timestamp); no iteration over hash-ordered collections where order affects output; no randomness; **every** piece of state goes into the snapshot (books, per-symbol `md_seq`, counters). Forgetting `md_seq`, for example, makes market-data sequence numbers jump backwards after recovery.

## Revisit when
- Snapshot pauses violate L1/L3 → incremental/background snapshots.
- A2 becomes a priority → implement Option C.
