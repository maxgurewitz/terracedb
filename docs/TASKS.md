# TASKS.md

## Purpose

This file turns the architecture into a dependency-aware implementation plan optimized for one developer working with AI agents. Each task is intended to be small enough to hand to an agent once its dependencies are complete, and stable enough that multiple tasks can proceed in parallel without constant interface churn.

The emphasis here is on:

- splitting work into clean task boundaries,
- making dependencies explicit so work can be parallelized aggressively,
- keeping deterministic simulation and replayability in scope from the start, and
- giving each task concrete implementation and verification steps.

## Scope

Included in this plan:

- the core embedded database engine,
- row and columnar table formats,
- unified commit log and change capture,
- tiered and s3-primary storage modes,
- scheduler integration,
- composition primitives built on top of the engine,
- projection and workflow libraries,
- an embedded virtual filesystem library, and
- deterministic simulation coverage for the full stack.

Explicitly excluded from the main execution plan:

- physical sharding,
- mount/protocol adapters that expose the embedded virtual filesystem as a real host filesystem or network service,
- zero-downtime upgrade handoff,
- deployment choreography/platform rollout details,
- time estimates or staffing concerns.

Those excluded areas are either marked as future extensions in the architecture or are outside the implementation-focused scope of this file.

## Global implementation rules

1. **All filesystem, object-store, clock, and randomness access must flow through injected traits.**
2. **Any task that changes ordering, visibility, durability, or recovery behavior must include crash/recovery simulation cases.**
3. **Any task that changes observable database state must include oracle-backed deterministic tests.**
4. **Treat file formats and object layouts introduced by earlier tasks as contracts. Do not silently redefine them in later tasks.**
5. **Prefer a runnable vertical slice early, then harden it.**
6. **Authoritative async consumers must use durable-prefix semantics unless a task explicitly describes a weaker best-effort path.**

## Phase overview

- **Phase 0** freezes contracts and builds the deterministic substrate.
- **Phase 1** creates a minimal local row-engine vertical slice.
- **Phase 2** hardens the row-LSM and scheduling behavior.
- **Phase 3** adds unified-log change capture semantics.
- **Phase 4** adds remote storage modes and disaster recovery.
- **Phase 5** adds columnar tables.
- **Phase 6** builds composition helpers, projections, workflows, and application-facing simulation harnesses.
- **Phase 7** expands deterministic simulation to the full stack.
- **Phase 8** adds an embedded virtual filesystem library on top of Terracedb.

## Parallel tracks

Once Phase 0 is complete, the work naturally splits into eight mostly independent tracks:

- **Track A — local engine core:** T04 and T06 in parallel; T05 after T04; T07 and T08 after T05 + T06; T09 after T07 + T08
- **Track B — LSM hardening:** T10 → T11; then T12, T13, T14, and T16 can proceed; T15 follows T11 + T13
- **Track C — change capture:** T17 → T18 → T19
- **Track D — remote storage:** T20 → T21 / T22 / T23
- **Track E — columnar:** T24 → T25 → T26 → T27
- **Track F — libraries:** T28, T29, and T30 start once their own engine dependencies are met; T31 depends on T30, T32 depends on T18/T19/T28/T29, and T32a depends on T03a/T31/T32
- **Track G — full-stack hardening:** T33 after T32a
- **Track H — embedded virtual filesystem library:** T34 first; T35 depends on T34; T36 depends on T35; T37 depends on T35 + T36 + T30/T31; T38 depends on T35 + T36 + T37 + T22/T23; T39 depends on T36 + T37 + T38; T40 depends on T33 + T37 + T38 + T39

---

## Phase 0 — Stable contracts and deterministic substrate

**Parallelization:** T01 first. After that, T02 and T03 can proceed in parallel, with T03 consuming the abstractions established by T02. T03a can follow once the substrate exists.

### T01. Freeze engine contracts and module seams

**Depends on:** none

**Description**

Define the public API and the internal seam boundaries that all later work will target. The point of this task is to stop interface churn before implementation branches diverge.

**Implementation steps**

1. Define the Rust equivalents of the architecture's public contracts: `DB`, `Table`, `Snapshot`, `WriteBatch`, `ReadSet`, `Scheduler`, `CommitOptions`, `ScanOptions`, `TableConfig`, and `StorageConfig`.
2. Define opaque/stable internal types for `SequenceNumber`, `LogCursor`, `CommitId`, table IDs, manifest IDs, and segment IDs.
3. Freeze module boundaries for:
   - catalog,
   - memtable,
   - commit log,
   - SSTables,
   - manifest,
   - compaction,
   - scheduler integration,
   - change capture,
   - object-store integration,
   - projection runtime,
   - workflow runtime.
4. Reserve fixed-width binary encodings where the architecture explicitly future-proofs the format, especially `CommitId` and `LogCursor`.
5. Add stub/mock implementations so downstream tasks can compile against these interfaces without the real storage logic existing yet.

**Verification**

- Compile-only API tests that instantiate configs and exercise all public method signatures.
- Unit tests that round-trip `LogCursor` and `CommitId` encodings and confirm sort order is preserved.
- A deterministic smoke test that constructs a DB from fake `FileSystem`, `ObjectStore`, `Clock`, and `Rng` implementations without touching real I/O.

---

### T02. Implement filesystem, object-store, clock, and RNG abstractions

**Depends on:** T01

**Description**

Implement the injected trait layer for all external effects: local storage, object storage, time, and randomness. Every later task depends on this layer to remain deterministic and simulation-friendly.

**Implementation steps**

1. Implement production adapters for `FileSystem`, `ObjectStore`, `Clock`, and `Rng`.
2. Ensure the production filesystem adapter hides blocking local I/O behind controlled Tokio integration (for example `spawn_blocking`).
3. Implement deterministic/simulated versions of all four traits for tests, using turmoil-backed simulated I/O/time where appropriate and explicit seeded `Clock`/`Rng` adapters at the trait boundary.
4. Standardize error mapping so later tasks can distinguish I/O failure, corruption, timeouts, and durability boundaries cleanly.
5. Wire dependency injection through DB open/config so no subsystem constructs ambient implementations internally.

**Verification**

- Unit tests for open/read/write/range-read/rename/delete/list behavior on each adapter.
- Deterministic tests proving seeded RNG output and simulated clock behavior are reproducible under the turmoil-based test harness.
- Simulation tests that inject disk-full, timeout, stale-list, and partial-read failures through the trait boundary and verify structured errors rather than panics.

---

### T03. Build the deterministic simulation harness and shadow-oracle scaffolding

**Depends on:** T01, T02

**Description**

Build the deterministic substrate that every later task will extend. This task establishes seed replay, trace capture, controlled crash/restart, and the first reusable fault-injection and oracle machinery, but it is not the place to defer subsystem-specific correctness models; later tasks are expected to add production-path cut points, workload generators, and oracle extensions as their own semantics land.

**Implementation steps**

1. Build a deterministic simulation runner using a single Tokio runtime under turmoil's control, with explicit injected `Clock`/`Rng` seams instead of ambient wall-clock or entropy interception.
2. Integrate a turmoil-based simulated object-store/network host and simulated filesystem adapters through the `FileSystem` and `ObjectStore` traits established in T02.
3. Implement seeded workload and fault-schedule generation plus trace capture.
4. Implement an initial shadow oracle for point state, sequence ordering, and recovery-prefix validation.
5. Add helpers to crash and restart the DB at controlled cut points.

**Verification**


- Reproducibility test: same seed under turmoil produces the same workload, fault schedule, and trace.
- Variance test: different seeds change at least one part of workload shape or execution order.
- Crash/restart simulation of an empty or stub DB to prove the harness itself is stable before engine logic is added.

---

### T03a. Extract a reusable deterministic simulation framework crate

**Depends on:** T03

**Description**

Package the reusable parts of the deterministic simulation substrate as a dedicated crate (for example `terracedb-simulation`) layered above `turmoil-determinism`. The goal is to let Terracedb consumers test application code against the real DB / projection / workflow runtimes under seeded replay, crash/restart, and fault injection without depending on engine-internal scenario types.

**Implementation steps**

1. Define the crate boundary between:
   - `turmoil-determinism` as the low-level seed/time/random helper layer,
   - the reusable Terracedb application simulation framework, and
   - engine-specific workload generators / oracles that remain layered on top.
2. Move or wrap the stable reusable pieces of the current simulation substrate: seeded runner, simulation context, trace capture, scheduled faults, crash/restart helpers, simulated dependency construction, and turmoil object-store host wiring.
3. Provide application-facing extension points for user-defined workload drivers, invariants, and shadow oracles instead of exposing only engine-specific `Db*` scenario types.
4. Decide the re-export policy from `terracedb` itself. Convenience re-exports are acceptable, but the dedicated crate should be the authoritative home of the framework API.
5. Add end-user-facing examples/docs for opening and restarting a DB under simulation and for injecting deterministic failures.

**Verification**

- Existing engine deterministic tests continue to pass through the extracted crate surface.
- Same seed still reproduces the same workload, fault schedule, and trace after the extraction.
- An integration test can drive a DB under the new crate without importing engine-internal scenario or oracle types.
- `turmoil-determinism` remains small and generic rather than gaining Terracedb-specific runtime APIs.

---

## Phase 1 — Minimal local row-engine vertical slice

**Parallelization:** After Phase 0, T04 and T06 can start together. T05 depends on T04. T07 depends on T05 and T06. T08 depends on T04, T05, and T06. T09 depends on T07 and T08.

### T04. Catalog, DB open, table creation, and metadata persistence

**Depends on:** T01, T02

**Description**

Implement DB open/close scaffolding, in-memory table-handle lookup, durable table creation, and persisted table metadata. This task establishes the metadata plane for the whole engine.

**Implementation steps**

1. Implement DB open/config validation and dependency injection.
2. Implement a durable table catalog mapping names to stable table IDs and persisted `TableConfig`.
3. Implement `table(name)` as a synchronous in-memory handle lookup.
4. Implement async `createTable(config)` as an atomic catalog mutation.
5. Persist and reload user-defined table metadata unchanged so it is available to the scheduler later.

**Verification**

- Tests that `table(name)` is synchronous and fallibility-free for existing tables.
- Restart tests proving table IDs, format, schema metadata, compaction strategy, and user metadata survive reopen.
- Crash/recovery tests around table creation showing the table exists fully or not at all after recovery.

---

### T05. Sequence numbers, MVCC key encoding, memtable, and snapshots

**Depends on:** T01, T04

**Description**

Implement the in-memory state required for writes and snapshot reads before SSTables exist: MVCC key encoding, mutable/immutable memtables, snapshots, and snapshot tracking.

**Implementation steps**

1. Implement MVCC key encoding as user-key prefix + separator + inverted/fixed-width version suffix.
2. Implement the concurrent mutable memtable and immutable-memtable handoff state used during flush.
3. Implement snapshot creation, explicit release, and tracking of the oldest active snapshot.
4. Implement memtable-only `read`, `readAt`, `scan`, `scanAt`, `scanPrefix`, and reverse scan behavior.
5. Implement `WriteBatch` and `ReadSet` as local in-memory accumulators with no I/O.

**Verification**

- Unit tests for MVCC key ordering, especially “newer versions sort first”.
- Snapshot tests proving reads at the same snapshot sequence remain stable while newer writes arrive.
- Simulation tests showing unreleased snapshots pin the GC bookkeeping horizon even before compaction exists.

---

### T06. Unified commit-log record format, segment format, and low-level append/read path

**Depends on:** T01, T02

**Description**

Implement the unified commit-log file format and segment manager. This task is about raw append/read correctness and segment structure, not yet the full commit coordinator.

**Implementation steps**

1. Implement `CommitRecord`, `CommitEntry`, checksums, segment framing, and footer encoding.
2. Implement the active-segment writer, segment sealing, footer writing, and low-level segment reader.
3. Implement sparse block indexing and per-table footer metadata.
4. Implement an in-memory segment catalog rebuilt from footers on open.
5. Expose internal append, seal, enumerate, and seek-by-sequence APIs for later tasks.

**Verification**

- Encode/decode tests for records, footers, checksums, and per-table metadata.
- Append/read tests with multi-entry batches across multiple tables, verifying exact `(sequence, op_index)` preservation.
- Fault-injection tests for torn writes, truncation, and checksum failure; recovery must fail closed and never invent records.

---

### T07. Commit coordinator: conflict checks, group commit, deferred durability, and watermarks

**Depends on:** T05, T06

**Description**

Implement the main commit path: conflict-checked atomic commits, sequence assignment, group commit, deferred-durability mode, and the visibility/durability watermarks.

**Implementation steps**

1. Implement the commit mutex critical section: read-set conflict checking, sequence assignment, and in-memory commit-log append.
2. Implement group-commit batching and a leader path that fsyncs one batch for many waiters.
3. Implement deferred-durability mode where visibility can move ahead of durability until `flush()`.
4. Insert writes into the memtable at the correct point for each durability mode.
5. Implement contiguous-prefix tracking so `currentSequence()` advances only when all prior sequences are visible.
6. Implement `currentDurableSequence()` and enforce `currentDurableSequence() <= currentSequence()`.

**Verification**

- Concurrency tests with many writers proving monotonic sequence assignment without gaps.
- Tests where a later sequence finishes memtable insertion first; `currentSequence()` must not skip an earlier unfinished sequence.
- Conflict tests proving `db.commit(batch, { readSet })` returns `ConflictError` when a read key changed after the recorded read sequence.
- Crash/recovery simulations at all commit cut points:
  - after append, before fsync,
  - after fsync, before memtable insert,
  - after memtable insert, before visibility publish,
  - after visibility, before durable-prefix advance in deferred mode.
- Mode-specific tests proving the architecture's visible-vs-durable semantics for tiered group commit, tiered deferred durability, and s3-primary-style deferred behavior.

---

### T08. Row SSTable format, memtable flush, `flush()` integration, and crash-safe manifest updates

**Depends on:** T04, T05, T06

**Description**

Implement the first persistent LSM layer: memtable flush to row SSTables, manifest generations, crash-safe `CURRENT` updates, and the local-storage side of `flush()`.

**Implementation steps**

1. Implement a row SSTable writer and stable metadata: table ID, level, key range, sequence range, length, and checksums.
2. Implement memtable rotation and immutable-memtable flush to SSTable files.
3. Implement manifest generations plus crash-safe `CURRENT` update with temp-file write, fsync, rename, and parent-directory fsync.
4. Persist `lastFlushedSequence` and live SSTable metadata in the manifest.
5. Define `flush()` behavior for local mode: durability checkpoint plus memtable-to-SSTable progress.

**Verification**

- Tests that flushing creates readable SSTables and a manifest with correct ranges.
- Crash tests at each manifest update step proving fallback to the prior valid generation.
- Recovery tests where new SSTables exist without a referencing manifest, and where manifests reference new SSTables while old files still remain; both states must recover safely.

---

### T09. Recovery and fast open path

**Depends on:** T07, T08

**Description**

Implement DB reopen by loading the latest valid manifest, reconstructing in-memory indexes, and replaying the commit-log tail newer than `lastFlushedSequence`.

**Implementation steps**

1. Implement manifest load with checksum validation and fallback to the previous valid generation.
2. Rebuild the in-memory SSTable catalog and commit-log segment index on open.
3. Replay commit-log records newer than `lastFlushedSequence` into the memtable.
4. Reconstruct visibility and durability watermarks from recovered state.
5. Add a fast path for clean shutdown or empty tail so open does not rescan unnecessary history.

**Verification**

- Restart tests showing state survives close/open with and without a commit-log tail.
- Crash tests with corrupt `CURRENT`, corrupt latest manifest, corrupt footer, and partially written active segment.
- Idempotence tests: repeated open/recover cycles must not duplicate writes or advance sequence state incorrectly.
- Simulation tests proving recovered state is always equal to some prefix of committed history, never a non-prefix mix.

---

## Phase 2 — Full row-LSM semantics

**Parallelization:** T10 starts after Phase 1. T11 depends on T10. T12, T13, and T14 can start after T11. T15 depends on T11 and T13. T16 depends on T08 and T11.

### T10. Full read path over memtables and SSTables

**Depends on:** T08, T09

**Description**

Implement the complete row-table read path across mutable memtable, immutable memtables, and SSTables, including scans, reverse scans, prefix scans, and bloom-filter-assisted point lookup.

**Implementation steps**

1. Implement row SSTable readers and iterators.
2. Add bloom filters and user-key prefix handling for point reads.
3. Merge memtable and SSTable iterators for `scan`, `scanAt`, `scanPrefix`, and reverse scans.
4. Enforce sequence visibility, tombstone resolution, and limit handling consistently across all read APIs.
5. Ensure historical reads (`readAt`, `scanAt`) observe the correct version horizon before GC is added.

**Verification**

- Read-path tests covering point reads, range scans, reverse scans, prefix scans, and limit behavior.
- MVCC tests showing `readAt` and `scanAt` return the correct historical value while newer versions exist.
- Fuzz/oracle tests comparing engine reads with the shadow oracle for random put/delete workloads.
- Simulation tests with flushes and reads interleaved, proving snapshots remain consistent.

---

### T10a. Merged row-range iterator and large-scan efficiency

**Depends on:** T10

**Description**

Replace the current “collect all candidate keys, then resolve each key with point lookups” row-scan strategy with a proper merged iterator over memtables and SSTables. This task owns large-range-scan efficiency for row tables so scans do not repeatedly reacquire lookup state or materialize the full keyset before yielding results.

**Implementation steps**

1. Introduce a merged row iterator that can walk mutable memtable, immutable memtables, and SSTables in key order.
2. Resolve visibility, tombstones, and per-key version collapse during iteration rather than via N follow-up point lookups.
3. Preserve existing semantics for `scan`, `scanAt`, `scanPrefix`, reverse scans, and limits.
4. Ensure the iterator can short-circuit early for limits and narrow ranges without collecting the entire keyset first.
5. Reuse the iterator substrate where possible so future read-path optimizations do not fork scan semantics.

**Verification**

- Oracle tests proving the merged iterator returns exactly the same logical rows as the existing semantics across random put/delete/versioned workloads.
- Tests showing limits and reverse scans stop without enumerating the full matching keyspace.
- Historical-scan tests proving `scanAt` still observes the correct version horizon while newer versions exist.
- Read/flush/compaction interleaving tests proving scan results remain snapshot-consistent while range iteration is in flight.

---

### T11. Compaction framework and leveled compaction

**Depends on:** T10

**Description**

Implement the general compaction planner/executor and add leveled compaction first. This task creates the reusable compaction substrate used by later strategies, filters, and merge-operator collapse.

**Implementation steps**

1. Implement compaction job selection, input enumeration, merge iteration, and output SSTable generation.
2. Implement manifest replacement for new outputs plus cleanup of obsolete inputs.
3. Implement leveled overlap rules and per-level size targets.
4. Publish compaction debt and backlog state so the scheduler can reason about pressure later.
5. Run compaction concurrently with reads and foreground writes without breaking snapshot semantics.

**Verification**

- Tests proving logical database content is unchanged before vs. after compaction.
- Tests that overlapping L0 files compact into lower levels with correct key and sequence ranges.
- Crash tests at “new output written”, “manifest switched”, and “old inputs deleted” cut points.
- Simulation tests with active readers and writers during compaction.

---

### T12. Tiered and FIFO compaction strategies

**Depends on:** T11

**Description**

Add the remaining compaction strategies from the architecture: tiered and FIFO. This task should reuse the framework from T11 rather than branch a separate implementation.

**Implementation steps**

1. Implement per-table strategy selection from `TableConfig`.
2. Implement tiered compaction selection heuristics and output rules.
3. Implement FIFO aging/deletion behavior for configured tables.
4. Ensure manifest/state reporting reflects the selected strategy correctly.
5. Support different strategies on different tables in the same DB.

**Verification**

- Strategy-specific tests for leveled, tiered, and FIFO behavior.
- Cross-table tests showing one table's strategy does not change another table's read or compaction behavior.
- Simulation tests with changing write rates and table sizes under each strategy.

---

### T13. MVCC GC horizons and `SnapshotTooOld` for historical reads

**Depends on:** T10, T11

**Description**

Turn snapshot tracking into enforced retention semantics. Implement history reclamation during compaction and `SnapshotTooOld` for old `readAt` / `scanAt` requests.

**Implementation steps**

1. Compute the GC horizon as the minimum of configured retention and oldest active snapshot.
2. Teach compaction to drop superseded versions only when they are older than the GC horizon.
3. Implement `SnapshotTooOld` for `readAt` and `scanAt`.
4. Add introspection so historical-read failures can be attributed to history GC rather than unrelated I/O failure.
5. Add observability for snapshots pinning retention too long.

**Verification**

- Tests proving active snapshots protect versions that are still visible to them.
- Tests that historical reads past the horizon return `SnapshotTooOld`.
- Simulation tests with long-lived snapshots, aggressive compaction, and explicit snapshot release.
- Oracle tests proving reclaimed history never changes reads within the retained horizon.

---

### T14. Merge operators

**Depends on:** T10, T11

**Description**

Implement merge semantics end to end: merge-operand writes, read-time merge resolution, compaction-time full/partial merge, and forced collapse when operand chains get too long. This task also owns the merge-specific correctness model, including ordered-merge oracle behavior, merge-focused workloads, and production-path recovery cut points for unresolved operands.

**Implementation steps**

1. Extend the write path and commit log to store merge operands.
2. Implement read-time merge resolution across memtables and SSTables in strict commit-sequence order.
3. Implement compaction-time partial/full merge and output rewrite.
4. Add a configurable operand-chain-length limit and forced collapse path.
5. Preserve the architecture's semantics that ordering matters but commutativity is not required.

**Verification**

- Unit tests for associative partial merges and deterministic full-merge results.
- Tests with non-commutative merge operators proving commit-sequence order is preserved.
- Read-time-vs-compaction equivalence tests: compacted output must match read-time resolution.
- Crash/recovery simulations while unresolved operands exist in memtables and SSTables.

---

### T15. Compaction filters and deterministic TTL behavior

**Depends on:** T11, T13

**Description**

Implement compaction filters, including deterministic TTL evaluation driven by the injected clock rather than ambient wall-clock time.

**Implementation steps**

1. Define the compaction-filter call surface including level, key, value, sequence, row kind, and engine-provided `now`.
2. Invoke filters only when entries are older than the active snapshot horizon.
3. Implement TTL-style filtering as the reference example.
4. Ensure filtered removals are reflected in manifest/state reporting.
5. Add accounting for bytes/keys removed by compaction filters.

**Verification**

- Tests proving filters are never allowed to remove data still visible to an active snapshot.
- Deterministic TTL tests where advancing the virtual clock changes outcomes reproducibly.
- Tests proving only the injected clock matters, not ambient system time.
- Crash tests around partially completed filtered compactions.

---

### T16. Scheduler integration, work queues, stats, and engine guardrails

**Depends on:** T08, T11

**Description**

Implement background-work observability, scheduler callbacks, default scheduling policy, and engine-enforced safety guardrails that prevent deadlock or unbounded backlog regardless of scheduler behavior. This task also establishes the scheduler-specific deterministic test surface: hostile/random scheduler implementations, progress invariants, and proof that scheduler freedom cannot violate engine guardrails.

**Implementation steps**

1. Implement `pendingWork()` and `tableStats()` for flush and compaction work, with extensible support for later backup/offload work types.
2. Implement the synchronous scheduler callback interface and the default round-robin policy.
3. Integrate `shouldThrottle` decisions into the write path.
4. Enforce the architecture's hard guardrails:
   - forced flush on memory exhaustion,
   - forced L0 compaction at the hard ceiling,
   - eventual execution of indefinitely deferred work.
5. Pass table metadata through to the scheduler untouched.

**Verification**

- Tests that scheduler choices affect priority and throttling but cannot violate safety guardrails.
- Tests for forced flush and forced L0 compaction when the scheduler refuses to run work.
- Simulation tests with hostile/random schedulers proving the engine still makes progress.
- Deterministic tests that `tableStats()` and `pendingWork()` track actual internal backlog.

---

### T16a. Deterministic performance-invariant test coverage

**Depends on:** T16

**Description**

Add deterministic tests for performance-adjacent invariants that the current simulation/runtime model can actually prove: bounded backlog, starvation-free service, scheduler fairness, modeled throttling delay, and bounded catch-up under injected latency. This task is explicitly about **performance invariants under a deterministic model**, not about claiming production throughput or wall-clock latency on real hardware.

**Implementation steps**

1. Extend the scheduler/simulation test suites with service-curve checks for write throttling, including monotonic delay growth as batch size grows and multi-table commits being gated by the slowest modeled table budget.
2. Add starvation/fairness tests showing backlogged tables are serviced within a bounded number of scheduler passes under round-robin-style policies, even when new foreground writes continue to arrive.
3. Add backlog-bound tests showing hostile or random scheduler choices cannot push flush/compaction pressure past the engine's hard guardrails under the existing model.
4. Add modeled catch-up tests for restart/startup scenarios, asserting that backlog replay drains within bounded simulated time under fixed message-latency settings.
5. Document the boundary of these tests clearly in code/comments: they validate deterministic control laws and liveness/performance invariants, not hardware-calibrated benchmarks.

**Verification**

- Deterministic tests proving larger writes incur larger modeled throttling delays than smaller writes.
- Tests proving multi-table commits wait for the slowest applicable write budget rather than the fastest table touched.
- Tests proving round-robin scheduler service clears three or more backlogged tables without starvation in a bounded number of passes.
- Simulation tests proving modeled backlog drains and replay/catch-up complete within explicit simulated-time bounds.

---

## Phase 3 — Unified-log change capture

**Parallelization:** T17 starts first. T18 can proceed once the commit coordinator exists. T19 depends on T17.

### T17. `scanSince` and `scanDurableSince` over the unified commit log

**Depends on:** T06, T07, T09

**Description**

Implement change capture on top of the unified commit log: gap-free iteration, per-table segment indexing, visible-vs-durable boundaries, and exact cursor semantics. This task owns the exact change-feed correctness model for cursor semantics and `(sequence, op_index)` ordering so later full-stack tests can reuse that machinery rather than invent it from scratch.

**Implementation steps**

1. Implement `LogCursor` as an opaque resume token representing position within a multi-entry batch.
2. Build the per-table segment index from sealed segment footers and recovered metadata.
3. Implement `scanSince(table, cursor)` bounded by the visible prefix.
4. Implement `scanDurableSince(table, cursor)` bounded by the durable prefix.
5. Guarantee strict `(sequence, op_index)` delivery order and safe resumption from any entry in a batch.

**Verification**

- Tests where one commit writes multiple entries to the same table and cursor resumption starts after the correct entry.
- Tests showing visible scans can move ahead of durable scans in deferred modes while durable scans never exceed `currentDurableSequence()`.
- Recovery tests proving post-crash scans expose only records recoverable in the configured mode.
- Oracle tests comparing change-feed order to committed write order exactly.

---

### T17a. Incremental streaming change-feed iteration

**Depends on:** T17

**Description**

Make the change-feed surface truly incremental instead of materializing a full `Vec<ChangeEntry>` before returning. This task owns memory-bounded, low-latency iteration for `scanSince` and `scanDurableSince`, including future remote/cold commit-log reads that should naturally pipeline.

**Implementation steps**

1. Replace full-result materialization with a stream/iterator pipeline that yields `ChangeEntry` values incrementally.
2. Preserve strict `(sequence, op_index)` ordering, cursor semantics, and visible-vs-durable upper bounds while yielding incrementally.
3. Bound in-memory buffering to the current page/window rather than the entire scan result.
4. Ensure limit handling can terminate the stream promptly without decoding unnecessary tail records.
5. Structure the scan path so future remote/cold commit-log segment reads can pipeline decode and yield work naturally.

**Verification**

- Tests proving change-feed results remain identical to the pre-streaming semantics across multi-entry batches and resume positions.
- Tests showing small limits terminate without reading or buffering the entire available result set.
- Fault-injection tests proving mid-stream scan failures surface as typed errors without losing already-yielded ordering guarantees.
- Remote/cold-path tests proving large scans can make progress incrementally rather than waiting for full materialization.

---

### T17b. Low-contention commit-log scan path for change feeds

**Depends on:** T17

**Description**

Remove the global `commit_runtime` async mutex as a long-lived scan bottleneck by separating cheap metadata snapshots from long-running I/O. This task owns the concurrency properties of continuous change-feed consumers so scans do not unnecessarily serialize commits, flushes, and other commit-log operations.

**Implementation steps**

1. Refactor the scan path to snapshot the metadata needed for a scan without holding the global commit-log runtime lock across I/O.
2. Split mutable commit-log control-plane state from read-mostly scan metadata where needed so scans can proceed concurrently with foreground operations.
3. Ensure commits, flushes, sealing, and retention can continue safely while scans are reading an earlier stable view.
4. Preserve correctness for visible/durable upper bounds, `SnapshotTooOld`, and table-segment lookup under concurrent maintenance.
5. Add internal contention hooks/metrics so scan-path serialization regressions are visible in tests and profiling.

**Verification**

- Concurrency tests where a blocked or slow change-feed scan does not prevent independent commits from completing.
- Tests where flush/seal/retention progress concurrently with a scan without violating ordering or visibility bounds.
- Regression tests proving `SnapshotTooOld` and table-floor calculations remain correct under concurrent log maintenance.
- Simulation or harness tests that repeatedly interleave scans with commits and maintenance without deadlock or starvation.

---

### T18. `subscribe` and `subscribeDurable` coalescing notifications

**Depends on:** T07

**Description**

Implement the coalescing notification channels paired with change capture. These are wake-up signals, not full event queues. This task establishes the deterministic wakeup semantics that later projection, workflow, and simulation workloads rely on, rather than leaving notification behavior to be inferred indirectly by integration tests.

**Implementation steps**

1. Implement subscriber registries for visible and durable notifications per table.
2. Ensure the implementation is coalescing rather than queueing every commit.
3. Tie subscription lifetime to receiver lifetime and support multiple subscribers per table.
4. Emit notifications only after the relevant visible or durable prefix actually advances.
5. Provide internal helpers so later runtimes can merge multiple subscriptions deterministically.

**Verification**

- Tests where many commits happen before a receiver wakes; the receiver may observe only the latest watermark.
- Tests showing subscriber drop deregisters cleanly.
- Tests that visible and durable subscriptions diverge correctly in deferred-durability modes.
- Simulation tests confirming “initial drain before blocking” plus “drain until empty on wake” avoids missed work.

---

### T19. Commit-log retention, GC, and `SnapshotTooOld` for change feeds

**Depends on:** T17

**Description**

Implement retention and garbage collection for the unified commit log, including the change-feed version of `SnapshotTooOld`. This task owns CDC-retention invariants, lagging-consumer scenarios, and change-feed `SnapshotTooOld` behavior so those rules are verified at the log layer before projections and workflows exercise them transitively.

**Implementation steps**

1. Track the recovery minimum (`lastFlushedSequence`) and CDC minimum per table.
2. Implement segment deletion according to the architecture's min-of-recovery-and-CDC rule.
3. Distinguish physical segment retention from logical per-table history availability.
4. Implement `SnapshotTooOld` for `scanSince` / `scanDurableSince`.
5. Expose stats that identify lagging tables or consumers holding back log GC.

**Verification**

- Tests proving a segment is retained until it is older than both recovery and all relevant CDC needs.
- Tests where a segment remains physically present for table A while table B already receives logical `SnapshotTooOld`.
- Recovery tests proving recovery-only mode deletes segments as soon as they are unnecessary.
- Simulation tests with lagging consumers, retention windows, and concurrent recovery pressure.

---

### T19a. Failed assigned-sequence durability semantics and recoverable-log staging

**Depends on:** T07, T17, T18, T19

**Description**

Repair the failed-group-commit correctness hole by making pre-fsync assigned sequences part of an internal provisional tail rather than public committed history. This task owns the exact semantics for failed assigned sequences so recovery, visible scans, durable scans, watermark publication, and later sequence assignment all observe only the committed prefix after fsync failure, later `flush()`, reopen, and backup/restore paths.

**Implementation steps**

1. Define the provisional-tail model explicitly: sequences reserved before durability are internal reservations, not public history, and `currentSequence()`, `currentDurableSequence()`, recovery, backup, replication, `scanSince`, and `scanDurableSince` observe only the committed prefix.
2. Add commit-log rollback support so the engine can discard the unresolved provisional tail from the first failed reserved sequence onward, including any local active-tail bytes and sealed segments created by that unresolved range.
3. On group-commit fsync failure, discard all unresolved reserved sequences from the failure point onward, fail those commits, and rewind sequence allocation so discarded reservations never become public sequence holes.
4. Ensure recovery replays only the committed prefix and never resurrects a write that previously returned a durability error.
5. Ensure `scanSince`, `scanDurableSince`, subscription watermarks, and any tiered backup/restore tail sync paths stay consistent with the provisional-tail discard behavior.

**Verification**

- Tests where group-commit fsync fails and the write returns an error, then a later `flush()` or reopen must not surface that write.
- Tests proving visible and durable watermarks stay at the committed prefix after failure and later successful commits reuse the discarded sequence range without public holes.
- Recovery tests proving a crash before successful batch durability loses only the staged non-durable work, while a crash after successful durability preserves it.
- Change-feed tests proving failed assigned sequences do not appear in `scanSince` or `scanDurableSince`, including after reopen.
- Backup/restore tests proving remote tail synchronization and disaster recovery never reintroduce a failed write.

---

### T19b. Change-feed typed error surface and panic-free scan failure handling

**Depends on:** T17, T18, T19

**Description**

Replace the current `SnapshotTooOld`-only change-feed error surface with a dedicated `ChangeFeedError` that can express both retention-horizon failures and ordinary storage/runtime scan failures. This task owns the public API churn needed to make `scanSince` and `scanDurableSince` panic-free and fully typed across the core engine, projections, workflows, and simulation helpers.

**Implementation steps**

1. Define a `ChangeFeedError` enum that includes at least `SnapshotTooOld` and `StorageError`, plus any conversion helpers needed by callers.
2. Change `scanSince` and `scanDurableSince` to return `Result<ChangeStream, ChangeFeedError>` instead of `Result<ChangeStream, SnapshotTooOld>`.
3. Remove panic-based handling from change-feed scans and propagate storage/runtime failures as typed errors instead.
4. Update projection, workflow, and simulation crates to consume the new change-feed error surface without collapsing distinct failure modes.
5. Update architecture docs, task references, and tests so the change-feed contract explicitly distinguishes retention failure from ordinary scan I/O/corruption failure.

**Verification**

- Tests where commit-log scan I/O fails and `scanSince` / `scanDurableSince` return typed errors rather than panicking.
- Tests where `SnapshotTooOld` is still surfaced distinctly and is not collapsed into a generic storage error.
- Projection-runtime tests proving durable source scans propagate change-feed failures through the runtime without process abort.
- Workflow-runtime tests proving source admission and durable outbox/timer consumers handle change-feed scan failures as typed runtime errors.
- Simulation tests covering local corruption, remote-read failure, and retention-horizon failure across change-feed consumers.

---

### T19c. Group-commit fsync failure regression suite

**Depends on:** T19a

**Description**

Add a must-pass regression suite for the failed-group-commit durability path so the engine continuously proves that a write which returned an error can never leak back in through reads, change feeds, watermarks, `flush()`, or recovery.

**Implementation steps**

1. Add explicit fault injection for commit-log `sync` failure during the group-commit durability path.
2. Capture same-process aftermath checks for reads, visible scans, durable scans, and watermark/subscription state.
3. Add follow-up operations after the failure, including later successful commits and explicit `flush()`, to prove the failed write is not resurrected.
4. Add reopen/recovery assertions proving the failed write is not replayed after crash or restart.
5. Keep this suite isolated and easy to understand so future refactors can use it as the canonical regression harness for failed assigned sequences.

**Verification**

- Tests where `commit()` returns an error on fsync failure and the failed write is absent from point reads.
- Tests where the failed write is absent from both `scanSince` and `scanDurableSince`.
- Tests proving subscriptions and visible/durable watermarks still progress for later successful commits.
- Tests proving later `flush()` does not make the failed write visible or durable.
- Reopen/recovery tests proving the failed write is never replayed.

---

### T19d. Change-feed structured error regression suite

**Depends on:** T19b

**Description**

Add focused regression coverage for the new typed change-feed error surface so scan failures are verified as structured returns rather than process aborts.

**Implementation steps**

1. Add tests for local commit-log read failures during `scanSince` and `scanDurableSince`.
2. Add tests for remote/object-store failures such as timeout and missing/corrupt segment reads where applicable.
3. Add corruption tests for malformed segment bytes and invalid commit-log frames encountered during scanning.
4. Verify downstream consumers such as projections and workflows propagate the typed failure rather than panicking.
5. Ensure `SnapshotTooOld` remains distinguishable from ordinary storage/runtime scan failure in all helper APIs.

**Verification**

- Tests where local scan I/O returns `ChangeFeedError::Storage(...)` rather than panicking.
- Tests where remote/object-store timeout or read failure is surfaced distinctly as a typed scan error.
- Tests where corrupted segment bytes produce a structured error return instead of process abort.
- Projection and workflow tests proving durable consumers surface the typed change-feed error through their runtime errors.
- Regression tests proving `SnapshotTooOld` is still surfaced as its own variant and is not collapsed into a generic storage failure.

---

### T19e. Watermark and failed-sequence property tests

**Depends on:** T18, T19, T19a

**Description**

Add property-style and randomized invariant tests around visibility/durability watermark behavior once failed assigned-sequence semantics are repaired. This task owns the “holes and prefixes” correctness bar so later refactors cannot quietly reintroduce skipped-prefix or aborted-sequence bugs.

**Implementation steps**

1. Build randomized schedules that interleave successful commits, failed assigned sequences, watermark publication, and later successful commits.
2. Assert monotonicity and prefix invariants for visible and durable watermarks under all such schedules.
3. Assert that no visible or durable change-feed result ever yields a failed/aborted sequence.
4. Assert that later successful sequences still become publishable after earlier failures according to the chosen failed-sequence semantics.
5. Reuse these invariants across unit tests and simulation/harness workloads so they guard both direct coordinator logic and end-to-end behavior.

**Verification**

- Property tests proving visible and durable watermarks are monotonic.
- Property tests proving the durable watermark never exceeds the visible watermark.
- Property tests proving no visible or durable feed yields a failed/aborted sequence.
- Randomized schedule tests proving later successful sequences still publish correctly after earlier failures.
- Reopen/recovery variants proving the same invariants hold across crash boundaries.

---

## Phase 4 — Remote storage modes

**Parallelization:** T20 starts first. After T20, T21, T22, and T23 can proceed in parallel subject to their listed dependencies.

### T20. Object-store integration substrate, caches, and range-read plumbing

**Depends on:** T02, T08

**Description**

Implement the shared object-store substrate used by tiered cold storage, backup, s3-primary mode, and remote columnar reads. This task also establishes the reusable remote-I/O fault model and range-read test substrate that later remote-storage tasks build on, rather than deferring remote fault semantics to the capstone pass.

**Implementation steps**

1. Define stable object naming/layout conventions for manifests, commit-log segments, backup SSTables, and cold SSTables.
2. Implement local metadata/data caches for fetched remote objects and SSTables.
3. Implement exact range-read plumbing so later columnar tasks can fetch only needed byte windows.
4. Surface object-store errors with enough structure for retries and recovery logic.
5. Provide storage-layer abstractions so row and columnar readers can read from local files or remote objects without duplicating high-level logic.

**Verification**

- Tests for object key layout stability and cache-hit/cache-miss behavior.
- Range-read tests proving exact byte windows are fetched and stitched correctly.
- Simulation tests with partitions, stale LIST results, partial reads, and lost responses.
- Restart tests ensuring cache metadata can be rebuilt entirely from durable object-store state.

---

### T21. Tiered cold offload and remote SSTable reads

**Depends on:** T20, T11, T16

**Description**

Implement tiered mode's cold-storage path: offloading old SSTables to S3, updating manifests, reclaiming local bytes, and reading cold SSTables through the normal read path. This task owns offload-specific crash/fault semantics and invariants around manifest publication, local reclamation, and read equivalence across local and remote placement.

**Implementation steps**

1. Implement per-table local-byte accounting and oldest-first SSTable selection for offload.
2. Upload or copy selected SSTables to the cold prefix, switch manifest references, and reclaim local disk only after the manifest update is durable.
3. Extend the row-table read path so SSTable references can resolve to local or remote locations transparently.
4. Integrate offload work with scheduler decisions and eventual-execution guardrails.
5. Ensure offload does not change MVCC semantics, scan order, or historical-read behavior.

**Verification**

- Tests that tables exceeding `maxLocalBytes` offload oldest SSTables first until back under budget.
- Tests proving reads return identical logical results before and after offload.
- Crash tests at “upload complete”, “manifest updated”, and “local file deleted” cut points.
- Simulation tests with network faults during offload and later retry/recovery.

---

### T22. Backup, disaster recovery, manifest upload order, and S3 GC

**Depends on:** T06, T09, T20

**Description**

Implement continuous backup/replication to S3, disaster recovery from S3, immutable manifest generations plus convenience pointers, and mark-and-sweep garbage collection of unreferenced S3 objects. This task owns backup/DR-specific correctness, including upload-order rules, orphan-object harmlessness, tail-RPO behavior, and S3 GC safety, rather than leaving those semantics to the final full-stack task.

**Implementation steps**

1. Upload newly created SSTables, sealed commit-log segments, and manifest generations to the backup prefix.
2. Implement the immutable manifest-generation layout plus `latest` pointer convenience behavior.
3. Implement disaster recovery: load the latest valid manifest, restore hot SSTables locally, and replay commit-log tail newer than `lastFlushedSequence`.
4. Implement an active-tail backup policy so the commit-log tail's RPO is bounded.
5. Implement mark-and-sweep GC rooted in retained manifest generations plus a grace period.

**Verification**

- Disaster-recovery tests proving recovered logical state matches the pre-failure durable/backup boundary.
- Tests that orphaned uploaded SSTables are harmless if they exist before the referencing manifest is published.
- GC tests proving referenced objects are never deleted while any retained manifest still points at them.
- Simulation tests for stale LIST results, successful PUT with lost response, interrupted recovery, and active-tail upload races.

---

### T23. S3-primary mode

**Depends on:** T20, T07, T08, T17

**Description**

Implement memory + S3 mode, including buffered visible commits, explicit `flush()` durability, same-process hybrid `scanSince`, and the exact visible-vs-durable semantics described in the architecture. This task owns the mode-specific visible-vs-durable rules, flush/recovery invariants, and the contract between in-process visible readers and durable-only readers that later higher-level libraries depend on.

**Implementation steps**

1. Implement in-memory commit-log buffering and memtable insertion without local durable fsync.
2. Implement `flush()` to ship buffered commit-log data and SSTables to S3.
3. Implement `currentDurableSequence()` and durable scanning for s3-primary mode.
4. Implement same-process hybrid `scanSince` that merges durable S3 segments with the in-memory visible buffer.
5. Ensure `scanDurableSince` and any separate-process S3 readers see only flushed durable state, while same-process `scanSince` readers can see the visible superset.

**Verification**

- Tests proving `commit()` makes writes visible but not durable until `flush()`.
- Crash tests showing everything after the last flush disappears consistently across source tables, cursors, projections, timers, outbox, and workflow state.
- Tests showing same-process `scanSince` can see visible buffered entries while `scanDurableSince` and other durable-only readers cannot.
- Simulation tests with failed flushes, partial uploads, and recovery to the last durable prefix.

---

## Phase 5 — Columnar tables

**Parallelization:** T24 can begin once metadata contracts exist. T25 depends on T24 plus flush machinery. T26 depends on T25. T27 depends on T26 plus compaction/merge support.

### T24. Schema model, validation, and columnar table creation rules

**Depends on:** T01, T04

**Description**

Implement columnar schema definitions, validation, and the typed record boundary. This task freezes the schema model before any physical columnar format work starts.

**Implementation steps**

1. Define the schema JSON/meta-schema representation and Rust equivalents.
2. Implement validation for field IDs, names, supported types, nullability, defaults, and unknown-field rejection.
3. Implement input-record validation and conversion from user-facing values into internal typed records.
4. Persist schema metadata in the catalog and anywhere SSTables/manifests need schema-version references.
5. Encode the v1 support boundary for columnar tables so later tasks do not over-claim historical semantics.

**Verification**

- Validation tests for valid/invalid schemas, including duplicate field IDs and unsupported type changes.
- Record-validation tests for nullable/default behavior and unknown-field rejection.
- Restart tests proving schemas survive reopen unchanged.
- Deterministic tests that the same named-record input resolves to the same field-ID ordering every time.

---

### T25. Columnar SSTable writer and flush path

**Depends on:** T24, T05, T08

**Description**

Implement memtable flush to the physical columnar SSTable layout: key index, sequence column, tombstone bitmap, per-column data, footer metadata, and row-kind metadata needed for merge operands.

**Implementation steps**

1. Implement row-to-column decomposition during memtable flush.
2. Write the physical layout: key index, sequence column, tombstone bitmap, row-kind metadata, encoded columns, and footer.
3. Implement deterministic type-specific column encoders and stable fallback compression behavior.
4. Persist schema-version metadata in the footer.
5. Integrate the writer with existing manifest and flush orchestration.

**Verification**

- Tests that flushing a columnar table produces a readable SSTable with correct footer metadata.
- Round-trip tests for each supported field type and encoding strategy.
- Deterministic byte-for-byte tests proving identical input flushes produce identical output.
- Crash/recovery tests during columnar SSTable write and manifest switch.

---

### T26. Columnar reads, scans, column pruning, and remote range fetch

**Depends on:** T20, T25

**Description**

Implement local and remote read paths for columnar SSTables, including point lookup, scans with column pruning, default-filling for newly added columns, and exact range-fetch behavior on S3. This task owns the supported read semantics for columnar tables, including exact remote fetch behavior and explicit fail-closed handling around unsupported overwritten-key-history cases.

**Implementation steps**

1. Implement point lookup via key index and row reconstruction.
2. Implement scan iterators with column pruning and tombstone handling.
3. Implement remote column fetch via object-store range reads using footer offsets.
4. Implement default-filling behavior for fields missing from older SSTables after schema evolution.
5. Enforce or clearly fail closed around the architecture's v1 limitation for columnar overwritten-key history.

**Verification**

- Point-read and scan tests comparing reconstructed records to original input.
- Column-pruning tests proving only requested columns are decoded/fetched.
- Remote-range tests proving only the expected byte ranges are read for selected columns.
- Schema-evolution tests where older SSTables lack a new field and reads correctly fill defaults.
- Supported-semantics tests for `readAt` / `scanAt` on columnar tables, including explicit failure/guardrail cases if full overwritten-key history is not claimed.

---

### T27. Columnar schema evolution, compaction, and merge integration

**Depends on:** T11, T14, T24, T26

**Description**

Complete columnar support by integrating it with compaction, merge operators, and lazy schema evolution. This task also owns the columnar-specific correctness model for schema evolution, compaction, and merge behavior so columnar semantics are already well-defined before the final full-stack matrix composes them with other subsystems.

**Implementation steps**

1. Teach compaction to read columnar inputs and rewrite columnar outputs while preserving schema metadata correctly.
2. Implement lazy schema evolution rules for add, remove, and rename; reject in-place type changes.
3. Implement merge-operand storage and compaction-time resolution for columnar tables.
4. Implement read-time merge over columnar inputs.
5. Support coexistence of mixed schema-version SSTables until compaction rewrites them.

**Verification**

- Compaction tests with mixed schema versions proving reads stay correct before and after rewrite.
- Merge tests showing columnar read-time merge and compacted merge are equivalent.
- Schema-evolution tests for add/remove/rename behavior across old and new SSTables.
- Simulation tests with crashes during columnar compaction and later recovery.

---

## Phase 6 — Composition primitives and higher-level libraries

**Parallelization:** T28, T29, and T30 can begin independently once their own engine dependencies are met. T31 depends on T30. T32 depends on T18, T19, T28, and T29. T32a depends on T03a, T31, and T32.

### T28. OCC transaction wrapper

**Depends on:** T05, T07

**Description**

Implement the user-space optimistic transaction helper described in the architecture: snapshot acquisition, read-set accumulation, read-your-own-writes, and conflict-checked commit.

**Implementation steps**

1. Implement `Transaction::begin`, `read`, `write`, `delete`, `commit`, and `abort`.
2. Track local writes so reads inside the transaction are read-your-own-writes.
3. Support explicit flush-on-commit vs. no-flush commit modes as described in the architecture.
4. Ensure snapshots are always released on both success and failure paths.
5. Document and encode the isolation level: snapshot isolation, not full serializability, and no phantom protection for range scans.

**Verification**

- Tests showing snapshot isolation for point reads inside a transaction.
- Conflict tests proving concurrent modification of a read key causes `ConflictError`.
- Read-your-own-writes tests for both puts and deletes.
- Simulation tests with crashes before and after transaction commit, verifying underlying batch atomicity.

---

### T29. Durable timer and transactional outbox helpers

**Depends on:** T17, T18

**Description**

Implement reusable helper libraries for durable timers and transactional outbox processing. These are building blocks for workflows and other application code, and this task owns their durability and replay semantics so workflows later compose trusted primitives rather than redefining timer/outbox correctness for themselves.

**Implementation steps**

1. Implement the schedule-table + lookup-table timer pattern, including scheduling and cancellation helpers.
2. Implement a durable-fenced timer scanner using `currentDurableSequence()` and `scanAt`.
3. Implement the transactional outbox write helper.
4. Implement the durable outbox consumer using `subscribeDurable` + `scanDurableSince`.
5. Persist timer and outbox consumer cursors durably, and use stable IDs/idempotency keys rather than ambient randomness.

**Verification**

- Timer tests proving due timers are not fired before they are durable.
- Duplicate-delivery tests showing timer or outbox reprocessing is benign with state checks or idempotency keys.
- Crash tests where the process dies after scheduling but before firing, and after outbox delivery but before cursor persistence.
- Simulation tests with delayed wakes, clock jumps, and restart catch-up.

---

### T30. Projection runtime: single-source durable projections

**Depends on:** T17, T18

**Description**

Implement the core projection runtime for single-source durable projections: whole-sequence batching, durable cursors, durable subscriptions, cursor/output atomicity, and watermark tracking. This task owns the single-source projection correctness model, including deterministic replay, cursor/output atomicity, and watermark progression, rather than treating projections as only an integration concern.

**Implementation steps**

1. Implement persistent cursor state in `_projection_cursors`.
2. Implement `scanWholeSequenceRun` so handlers never receive partial same-sequence batches.
3. Implement the single-source runtime loop:
   - initial durable catch-up before blocking,
   - durable subscription wakeups,
   - drain-until-empty on each wake.
4. Atomically commit projection output plus cursor advancement in one batch.
5. Implement visible watermark tracking plus `waitForWatermark` for single-source projections.

**Verification**

- Tests proving a projection processes all entries from one source sequence atomically before advancing its watermark.
- Crash tests showing output and cursor advancement never become visible independently.
- Wakeup tests showing startup backlog is processed even without a new notification.
- Deterministic replay tests showing the same source history produces the same output and watermark progression.

---

### T31. Projection runtime: multi-source frontiers, dependency ordering, and recomputation

**Depends on:** T19, T20, T26, T30

**Description**

Extend the projection runtime to support multiple source tables, frontier-pinned reads, dependency ordering, transitive waits where supported, and recomputation from SSTables or checkpoints after `SnapshotTooOld`. This task owns the multi-source projection correctness model, including frontier semantics, deterministic tie-breaking, recomputation behavior, and fail-closed handling of unsupported provenance cases.

**Implementation steps**

1. Implement per-source cursor state and per-source frontier vectors.
2. Implement `ProjectionContext` reads via `readAt` / `scanAt` pinned to the frontier.
3. Implement deterministic next-batch selection across ready sources: lowest next sequence first, source-declaration-order tie-break.
4. Implement dependency tracking between projections and precise transitive waits for the cases supported by the architecture.
5. Implement recomputation from local and remote SSTables, plus optional projection checkpoints.
6. Make unsupported multi-source provenance cases fail closed or remain explicitly conservative rather than silently pretending exactness.

**Verification**

- Multi-source replay tests proving the same frontier yields the same output regardless of wake timing.
- Tests that declaration-order tie-breaks make equal-sequence source progress deterministic.
- Recomputation tests for append-only/event-sourced inputs: rebuilding from SSTables matches tailing from the beginning.
- Tests that `SnapshotTooOld` triggers rebuild/checkpoint restore instead of silent data loss.
- Limitation tests for mutable-source/history-dependent projections showing the runtime either rejects unsupported cases or documents them explicitly.

---

### T32. Workflow runtime

**Depends on:** T18, T19, T28, T29

**Description**

Implement the workflow runtime: durable trigger admission, per-instance trigger ordering, ready-instance scheduling, inbox execution, callback admission, timer admission, outbox delivery, and crash recovery. This task owns workflow-specific durability and ordering semantics, including trigger admission, inbox execution, timer/outbox correctness, and crash cut points, so the final full-stack task can compose workflows rather than define them.

**Implementation steps**

1. Implement workflow tables: state, inbox, trigger order, source cursors, timer schedule/lookup, and outbox.
2. Implement `stageTriggerAdmission` so trigger-sequence allocation, inbox insertion, and associated progress updates happen in one OCC unit.
3. Implement durable source-event admission using `subscribeDurable` + `scanDurableSince`.
4. Implement callback admission with the stronger durability requirement before external acknowledgement.
5. Implement the timer-admission loop using durable-fenced timer scans.
6. Implement the ready-instance scheduler, default fair round-robin policy, and one-active-executor-per-instance rule.
7. Implement inbox execution, outbox integration, and crash/restart recovery.

**Verification**

- Ordering tests proving one workflow instance processes triggers strictly by `triggerSeq`.
- Concurrency tests showing only one executor runs per instance while different instances can proceed independently.
- Crash tests at all important workflow cut points:
  - after trigger admission but before execution,
  - after execution output is built but before commit,
  - after outbox write but before delivery,
  - after delivery but before outbox cursor persistence.
- Duplicate-trigger tests for timers and callbacks proving state-guarded/idempotent handlers remain correct.
- Recovery tests proving source cursors, inbox state, timer state, outbox state, and workflow state resume from durable data only.


---

### T32a. Adopt the reusable simulation framework across projections, workflows, and application tests

**Depends on:** T03a, T31, T32

**Description**

Build the application-facing Terracedb harnesses on top of the shared simulation crate and use them in the projection and workflow libraries themselves. This task makes the reusable framework real for consumers: a Terracedb-based application should be able to stand up DB + projections + workflows inside one deterministic simulation and assert its own invariants across crashes and injected faults.

**Implementation steps**

1. Add harness adapters/builders for opening, shutting down, and restarting a DB together with projection and workflow runtimes inside the shared simulation context.
2. Add helpers for simulated external services so workflow/outbox/callback tests can exercise retries, partitions, timeouts, and duplicate delivery against real hosts under turmoil.
3. Migrate the projection library's deterministic tests and the workflow library's deterministic tests to the shared framework instead of bespoke per-crate harness code.
4. Add at least one example consumer-style test that runs application code on top of Terracedb under the shared harness, with an application-defined oracle/invariant set.
5. Ensure failure output includes the seed, trace, injected faults, and enough app/runtime checkpoint metadata to reproduce issues.

**Verification**

- `terracedb-projections` and the workflow library run their deterministic suites through the shared framework.
- Example application-level tests reproduce the same crash/restart and side-effect behavior from a seed.
- Same-seed reruns produce identical traces/results across DB + projection/workflow + application code.
- A consumer does not need direct access to engine-internal workload DSLs or oracles to write deterministic Terracedb application tests.

---

### T32b. OpenTelemetry export integration and cross-runtime correlation

**Depends on:** T03a, T16, T31, T32

**Description**

Implement Terracedb’s first observability integration as an **export-only** OpenTelemetry path: traces, logs, and metrics are emitted to external OTLP collectors / providers, but Terracedb does not yet act as an in-process telemetry storage backend. This task covers instrumentation of the core DB, projection runtime, and workflow runtime; OTLP bootstrap and shutdown; process-level resource metadata; and correlation with application spans when the embedding application is also using OpenTelemetry.

The design goal is to make observability a **library edge concern**, not a storage-engine concern. Instrumentation should live in the production code paths of `terracedb`, `terracedb-projections`, and `terracedb-workflows`, while exporter/provider setup lives in a small dedicated integration crate or module. The same codebase must support three modes: disabled, deterministic test exporter, and real OTLP export. Correlation must work in the common case where an application already has an active OpenTelemetry/tracing context and Terracedb operations should appear as child spans or linked work within that trace.

**Implementation steps**

1. Add a dedicated observability integration surface (for example `terracedb-otel`) that owns:

   * OpenTelemetry resource construction (`service.name`, version, environment, process/runtime attributes),
   * OTLP exporter/provider bootstrap,
   * propagation setup,
   * test/in-memory exporters,
   * explicit shutdown / flush handles.
2. Instrument the DB, projection, and workflow production paths with `tracing` spans/events rather than hard-coding OTLP calls throughout engine logic. Cover at least:

   * DB open/close,
   * commit / flush / recovery,
   * change-feed catch-up and subscription wake processing,
   * compaction / object-store operations,
   * projection batch processing and watermark advancement,
   * workflow trigger admission, step execution, timer firing, callback admission, and outbox delivery.
3. Define Terracedb-specific span/log/metric attribute names and keep them stable. Include identifiers such as DB name/instance, table, projection/workflow name, sequence / durable-sequence where appropriate, storage mode, and tenant/application labels when available. Do **not** make unbounded user keys or row payloads part of default telemetry.
4. Implement correlation with an embedding application’s existing tracing/OTel context:

   * if a Terracedb API call is invoked under an existing application span, the Terracedb span should become a child span,
   * if asynchronous background work is causally triggered by an earlier span but executes later (projection catch-up, workflow continuation, outbox delivery), preserve correlation via explicit parent propagation or span links rather than ambient thread-local assumptions,
   * expose helper APIs for callers to attach operation-scoped context explicitly when needed.
5. Add a metric reporter layer that exports stable engine/runtime metrics without embedding exporter state into core types. Favor poll/snapshot-based collection for internal state (for example scheduler backlog, compaction debt, projection lag, workflow inbox depth, durable-vs-visible lag) so the engine remains decoupled from the metric backend.
6. Provide configuration for disabled mode, test-exporter mode, and real OTLP mode. Real OTLP mode should support endpoint, protocol, headers/auth, export interval/batch knobs, and a clean shutdown path that flushes pending telemetry before process exit.
7. Keep deterministic simulation intact by ensuring simulation tests do not depend on real background OTLP worker threads or real sockets. The deterministic harness should be able to install a test exporter and assert on completed spans/logs/metrics as ordinary test data.

**Verification**

Write a focused integration test matrix around **correlation**, **stability of emitted telemetry**, and **deterministic testability**, not just “did the exporter send something.”

Start with direct DB-call correlation tests. Create an application-level root span, invoke Terracedb operations (`commit`, `flush`, `scanSince`, transaction helper calls where available), export to an in-memory/test exporter, and assert that the emitted Terracedb spans share the same trace ID as the application span and have the expected parent/child relationship. Add a negative control where Terracedb is called with no ambient context and verify that it still emits valid standalone spans rather than panicking or silently dropping instrumentation. Add tests that nested operations do not create nonsensical span trees (for example every internal helper becoming a top-level root span), and that the stable Terracedb attributes are present while row values, large payloads, and unbounded key material are absent by default.

Then add async/runtime-correlation tests for projections and workflows. Drive a source event under an application span, let it flow through the projection runtime and workflow runtime, and verify that later asynchronous work is still correlated correctly: either it remains in the same trace with an explicit propagated parent, or it is connected by span links where strict parent/child timing no longer makes sense. Include crash/restart cases in deterministic simulation: emit work, crash after durable admission but before the runtime finishes processing, restart, and assert that the replayed projection/workflow spans are still well-formed and carry enough stable identifiers (projection/workflow name, source table, sequence, trigger ID) to correlate them with the original causal chain. The key property is that replay/recovery may create new execution spans, but it must not lose causal linkage or invent nondeterministic IDs outside the configured tracing/export system.

Add metric and log verification with the same rigor. For metrics, use the test exporter to assert that snapshots include expected gauges/counters/histograms for scheduler backlog, compaction debt, projection lag, workflow queue/inbox depth, and durable-vs-visible watermarks, and that repeated collection updates the same logical instruments rather than generating an ever-growing set of metric names or attribute cardinalities. For logs, emit representative warnings/errors from object-store failure, retry, and recovery paths and assert that they carry the current trace/span context when invoked inside an active span. Finally, add deterministic-simulation tests that run with the test exporter enabled across repeated same-seed runs and verify that emitted span/log/metric shapes are reproducible enough for seed-based debugging: same causal operations should produce the same sequence of Terracedb telemetry records modulo exporter timestamps and other explicitly documented nondeterministic fields.

---

## Phase 7 — Full-stack deterministic hardening

**Parallelization:** This hardening phase should begin only after the main engine, projection, workflow, and reusable simulation surfaces exist.

### T33. Full-stack randomized scenario generation and invariant suites

**Depends on:** T03, T03a, T16, T19, T22, T23, T27, T31, T32, T32a

**Description**

Compose the deterministic testing machinery built in earlier tasks into the full-system correctness matrix for the core DB / projection / workflow stack: mixed workloads, mixed fault workloads, large-seed campaigns, and cross-layer invariants spanning engine, projections, and workflows. This task is the capstone integration bar for that stack, not the first place subsystem-specific oracles, fault hooks, or workload generators should appear.

**Interpretation note**

Implement this task in the strongest sense, following the FoundationDB testing philosophy rather than a weaker “simulation-flavored tests” version:

- The simulation target is the real production engine / projection / workflow code, not a parallel stub-only model.
- Production code paths should contain the fault hooks and cut points that simulation drives; adapter-only fault injection is not sufficient.
- Randomized runs should be composed from domain workloads plus fault workloads, with invariants checked throughout, rather than from generic raw-I/O fuzzing alone.
- A failing seed must reproduce the workload shape, injected faults, scheduling decisions, and trace, not merely the final state.
- The goal of T33 is to make this randomized deterministic simulation matrix the primary correctness bar for the core DB / projection / workflow stack, not an optional supplement to unit tests.

**Implementation steps**

1. Extend the shadow oracle to model:
   - puts/deletes,
   - merge operators,
   - snapshot reads,
   - change-feed ordering,
   - projection output/cursor relationships,
   - workflow inbox/outbox/timer/state behavior.
2. Build randomized workload generators for:
   - row and columnar writes,
   - deletes and merges,
   - snapshots and historical reads,
   - projection catch-up and recomputation,
   - workflow event/timer/callback mixes,
   - offload/backup/flush/recovery cycles.
3. Add randomized fault schedules covering:
   - crash points,
   - torn writes,
   - disk full,
   - corrupted files,
   - object-store partitions/timeouts/stale-list behavior,
   - hostile scheduler decisions.
4. Add trace capture and seed replay tooling for failure reproduction.
5. Add a meta-test that reruns the same seed and compares traces to catch uncontrolled nondeterminism.

**Verification**

- Large-seed simulation runs where every invariant is checked after each step or recovery point.
- Reproducibility tests proving the same seed yields the same trace and the same final state.
- Prefix-consistency tests proving recovered state is always some valid durable/visible prefix according to the configured mode.
- End-to-end tests where projections and workflows continue to behave correctly across crash/restart, `SnapshotTooOld`, recomputation, timer replay, and outbox retries.

---

### T33a. CI seeded campaigns and failure-artifact capture

**Depends on:** T33

**Description**

Operationalize the deterministic simulation bar by running larger seeded campaigns in CI and preserving failing seeds as first-class artifacts. This task owns the project-level workflow that turns the simulation machinery from “available” into “continuously enforced.”

**Implementation steps**

1. Add CI jobs for deterministic seeded campaigns at a scale meaningfully above the basic local smoke suite.
2. Split seed execution into tiers as needed, such as per-PR must-pass coverage and larger scheduled/nightly campaigns.
3. Capture failing seeds, traces, injected-fault schedules, and enough runtime metadata to replay failures locally.
4. Provide an easy local replay entrypoint/documented command so a CI-found seed can be reproduced directly by developers.
5. Make campaign configuration explicit and reviewable so seed counts, workloads, and fault mixes can evolve intentionally over time.

**Verification**

- CI runs deterministic seeded campaigns automatically rather than relying only on ad hoc local execution.
- A failing CI seed produces an artifact bundle with the seed and replay metadata needed for local reproduction.
- Replay tooling or documented commands can rerun a captured failing seed locally and reproduce the same failure.
- Campaign tiers demonstrate that larger seed counts can be added operationally without replacing the fast must-pass smoke coverage.

---

## Phase 8 — Embedded virtual filesystem library

**Parallelization:** T34 first. T35 depends on T34. T36 depends on T35. T37 depends on T35, T36, T30, and T31. T38 depends on T35, T36, T37, T22, and T23. T39 depends on T36, T37, and T38. T40 depends on T33, T37, T38, and T39.

### T34. Freeze the embedded virtual filesystem crate boundary, semantics, and reserved tables

**Depends on:** T01, T28

**Description**

Define the embedded virtual filesystem public surface and the reserved table/key contracts before implementation work branches. This task freezes the semantic target: provide a useful filesystem/KV/tool/overlay model on Terracedb for in-process agent runtimes, not SQL compatibility or mount-oriented adapter behavior.

**Implementation steps**

1. Add a new workspace member (for example `crates/terracedb-vfs`) and define its crate/module boundaries:
   - volume lifecycle,
   - path-based filesystem API,
   - KV API,
   - tool-run API,
   - snapshot / clone / overlay API,
   - activity API, and
   - shared error types.
2. Freeze the reserved table families and key encodings:
   - `vfs_volume`,
   - `vfs_allocator`,
   - `vfs_inode`,
   - `vfs_dentry`,
   - `vfs_chunk`,
   - `vfs_symlink`,
   - `vfs_kv`,
   - `vfs_tool_run`,
   - `vfs_activity`,
   - `vfs_whiteout`,
   - `vfs_origin`.
3. Freeze stable encodings for `volume_id`, inode IDs, activity IDs, and tool-run IDs, including the volume-first key-prefix ordering used for scans.
4. Decide and document the compatibility boundary explicitly:
   - preserve POSIX-like virtual filesystem semantics, snapshots, overlays, KV/tool APIs, and auditability,
   - do not promise SQL compatibility, mount/service adapters, or single-file transport.
5. Add compile-only stubs and a fake in-memory volume implementation so downstream tasks can build against the new crate boundary immediately.

**Verification**

- Compile-only tests that instantiate the public volume/filesystem/KV/tool/overlay APIs.
- Unit tests that round-trip the reserved key encodings and confirm scan order for dentries, chunks, and activity rows.
- A smoke test that opens a fake volume under deterministic injected dependencies without touching real I/O.

---

### T35. Implement volume lifecycle, root metadata, allocators, and shared read helpers

**Depends on:** T34, T04, T05, T10, T28

**Description**

Implement the common substrate that every virtual filesystem operation uses: volume creation/open, immutable volume config, root inode initialization, monotonic ID allocation, and snapshot-consistent helper routines for resolving paths and scanning chunk/dentry ranges.

**Implementation steps**

1. Implement `create/open volume` with immutable config such as `chunk_size`, format version, and optional overlay-base descriptor.
2. Initialize and persist root inode `1`, default metadata, and any reserved per-volume rows needed on first open.
3. Implement block-leased monotonic allocators for:
   - inode IDs,
   - activity IDs, and
   - tool-run IDs.
4. Implement shared helpers for:
   - path normalization,
   - parent lookup,
   - snapshot-bound path resolution,
   - dentry prefix scans,
   - chunk range scans, and
   - common virtual filesystem error mapping.
5. Implement the shared durability-cut helper logic the higher-level API will use later for `snapshot({ durable: true })`, `fsync`, and export/clone boundaries.

**Verification**

- Reopen tests proving volume metadata, root inode, and config survive restart unchanged.
- Allocator tests proving monotonicity across restart, retry, and concurrent lease refresh.
- Snapshot-consistency tests showing path resolution and inode/chunk reads do not mix versions under concurrent writers.
- Simulation tests with conflict retries and injected crashes during allocator lease refresh.

---

### T36. Implement core filesystem state and POSIX-like operations

**Depends on:** T35, T28

**Description**

Implement the current-state filesystem itself on top of Terracedb tables: namespace operations, inode metadata, chunked file I/O, links, symlinks, and `fsync` semantics. This task is the heart of the embedded virtual filesystem crate.

**Implementation steps**

1. Implement inode/dentry/chunk/symlink read and write paths using one OCC unit per logical filesystem operation.
2. Implement the core file and directory API:
   - `mkdir`,
   - `writeFile`,
   - `readFile`,
   - `pread`,
   - `pwrite`,
   - `truncate`,
   - `readdir`,
   - `readdirPlus`.
3. Implement metadata and namespace mutation APIs:
   - `stat`,
   - `lstat`,
   - supported ownership/permission/time updates,
   - `link`,
   - `symlink`,
   - `readlink`,
   - `unlink`,
   - `rmdir`,
   - `rename`.
4. Implement correct synchronous maintenance of:
   - `nlink`,
   - file size,
   - nanosecond timestamps,
   - `rdev` when special files are exposed,
   - chunk creation/rewrite/removal for truncate and partial writes.
5. Define `fsync` and volume flush semantics as durability fences over Terracedb flush behavior.

**Verification**

- POSIX-like unit and integration tests for create/read/write/delete/rename/readdir.
- Hard-link and symlink tests, including correct `nlink` behavior and `readlink` semantics.
- `pread` / `pwrite` / `truncate` tests across chunk boundaries.
- Crash tests proving multi-row namespace/data mutations recover atomically.

---

### T37. Implement KV state, tool-run tracking, append-only activity, and timeline helpers

**Depends on:** T35, T36, T30, T31

**Description**

Implement the non-filesystem virtual filesystem surfaces and the auditability model. Every mutating filesystem, KV, and tool action should append one semantic activity row in the same commit as the current-state change.

**Implementation steps**

1. Implement `vfs_kv` current-state operations with JSON serialization/deserialization and `set` / `delete` semantics.
2. Implement `vfs_tool_run` current-state operations supporting both:
   - a two-step `start` → `success|error` lifecycle, and
   - a one-shot “record completed run” path.
3. Instrument every mutating filesystem, KV, and tool operation so it appends one `vfs_activity` row in the same batch as the current-state change.
4. Implement timeline/query helpers over `vfs_activity`, plus lightweight derived projections for at least:
   - recent activity,
   - per-tool counters and latency stats, and
   - volume usage/accounting.
5. Expose visible and durable activity-tail helpers using `scanSince` / `scanDurableSince` and `subscribe` / `subscribeDurable`.

**Verification**

- Tests showing current-state rows and corresponding activity rows never become visible independently.
- Timeline/projection rebuild tests proving recent activity, stats, and usage recompute correctly from append-only history.
- Tool-run lifecycle tests covering pending, success, error, and one-shot completed paths.
- Crash tests around pending → completed transitions and filesystem mutation + activity atomicity.

---

### T38. Implement snapshots, clone/export flows, and copy-on-write overlays

**Depends on:** T35, T36, T37, T22, T23

**Description**

Implement the Terracedb-native reproduction story for embedded agent volumes: short-lived read-only snapshots, logical clone/export flows, and writable overlay volumes backed by read-only virtual filesystem bases. This replaces the SQLite-era “copy the database file” portability story with a Terracedb-native equivalent.

**Implementation steps**

1. Implement `AgentFsSnapshot` as a read-only volume view bound to a visible or durable cut.
2. Implement logical export/import or clone helpers for moving a single `volume_id` into a fresh DB or restoring it elsewhere.
3. Implement overlay metadata plus the `vfs_whiteout` and `vfs_origin` tables, with bases restricted to read-only virtual filesystem snapshots/clones rather than host filesystem adapters.
4. Implement overlay lookup semantics, merged directory listing, whiteout handling, and copy-up on first mutation of base-resident entries.
5. Ensure long-lived reproduction and overlay bases use exported data or retained base metadata rather than GC-pinning engine snapshots indefinitely.

**Verification**

- Snapshot stability tests under concurrent writes.
- Whiteout and copy-up tests, including recreate-after-delete behavior and merged `readdir` semantics.
- Export/import round-trip tests proving the restored volume matches the source cut exactly.
- Tests showing durable-cut exports never include visible-but-not-yet-durable state.

---

### T39. Expose the embedded Rust API and agent-runtime integration examples

**Depends on:** T36, T37, T38

**Description**

Expose the virtual filesystem crate the way applications actually use it: as an embedded Rust library for in-process agent sandboxes. The goal is a stable SDK and examples for agent runtimes, not a second wave of mount, CLI, or service implementations.

**Implementation steps**

1. Finalize the Rust SDK surface for path-based filesystem operations, KV, tool runs, snapshots, overlays, activity tailing, and flush.
2. Add convenience helpers and examples for common AI-agent runtime patterns:
   - open a base volume,
   - create a writable overlay for one run/session,
   - expose a bounded capability surface to the agent,
   - inspect recent file/tool activity.
3. Add end-to-end examples that run an agent or agent-like harness against the same embedded volume API used by production code.
4. Ensure no example or helper bypasses the crate and writes reserved tables directly.
5. Document the explicit version-1 non-goals:
   - no FUSE/NFS/MCP/HTTP/service boundary,
   - no host filesystem mount surface,
   - no mount-oriented inode/handle API.

**Verification**

- Integration tests through the SDK for filesystem, KV, tool-run, snapshot, and overlay operations.
- Example tests showing an agent-style harness can use a base volume plus writable overlay without touching internal tables.
- Restart tests showing existing volumes and overlays reopen without repair or migration tricks.
- API tests proving the agent-facing surface stays path-based and mount-independent.

---

### T40. Build deterministic compatibility, crash, and randomized fault suites for the virtual filesystem crate

**Depends on:** T33, T37, T38, T39

**Description**

Bring the virtual filesystem crate up to the same correctness bar as the rest of Terracedb by extending the deterministic simulation framework to the real embedded filesystem/KV/tool/overlay implementation.

**Implementation steps**

1. Build a shadow model for:
   - current filesystem state,
   - KV state,
   - tool-run lifecycle state,
   - overlay whiteouts/origin mappings, and
   - append-only activity prefix rules.
2. Add randomized workloads covering:
   - namespace operations,
   - file I/O and truncation,
   - hard links and symlinks,
   - tool-run start/success/error,
   - KV updates,
   - overlay copy-up and whiteouts,
   - snapshot/export/import flows,
   - `fsync` / flush behavior.
3. Add crash cut points around create, rename, unlink, truncate, copy-up, tool-run completion, export, and durability-boundary operations.
4. Port or mirror a representative subset of relevant virtual-filesystem behavior/spec examples against the Terracedb implementation.
5. Ensure every failing seed captures the workload, trace, injected faults, and enough volume metadata to replay the failure exactly.

**Verification**

- Large-seed simulation campaigns where both current state and activity-prefix invariants are checked after each step or recovery point.
- Same-seed replay tests proving identical traces and results.
- Compatibility-corpus tests passing against the virtual filesystem crate.
- Recovered-state prefix tests across standalone volumes and overlay volumes.

---

## Suggested execution milestones

These are not separate tasks; they are useful “stop and validate” points before opening more parallel work.

### Milestone A — Minimal usable local engine
Complete: T01–T10

At this point the system should support:
- local tables,
- snapshots,
- atomic commits,
- commit ordering,
- recovery,
- local flush,
- row SSTables, and
- full row-table reads/scans.

### Milestone B — Hardened row engine with change capture
Complete: T11–T19

At this point the system should additionally support:
- compaction strategies,
- MVCC GC,
- merge operators,
- compaction filters,
- scheduler/backpressure,
- visible and durable change feeds, and
- commit-log retention rules.

### Milestone C — Remote durability modes
Complete: T20–T23

At this point the system should additionally support:
- tiered cold storage,
- backup/disaster recovery,
- s3-primary durability semantics, and
- hybrid local change capture in s3-primary mode.

### Milestone D — Columnar support
Complete: T24–T27

At this point the system should additionally support:
- schema-validated columnar tables,
- columnar SSTables,
- column pruning and remote range reads, and
- schema evolution/compaction for columnar data.

### Milestone E — Libraries
Complete: T28–T32a

At this point the system should additionally support:
- OCC transaction helpers,
- durable timers,
- transactional outbox,
- durable projections,
- durable workflows, and
- a reusable deterministic simulation harness for Terracedb-based applications.

### Milestone F — Full correctness bar
Complete: T33

At this point the system should have the deterministic simulation coverage needed to validate the entire stack under randomized failures.

### Milestone G — Embedded virtual filesystem library
Complete: T34–T40

At this point the system should additionally support:
- an embedded virtual filesystem crate on top of Terracedb,
- point-in-time snapshots and copy-on-write overlays for agent sandboxes,
- KV state and tool-run audit history,
- durable clone/export flows instead of SQLite-file copies, and
- deterministic simulation coverage for the virtual filesystem crate itself.

---

## Deferred items from the architecture

The following architecture sections are intentionally **not** decomposed into implementation tasks here because they are either explicitly future work or outside the requested scope:

- physical per-table sharding,
- mount/protocol adapters for exposing the embedded virtual filesystem outside the process,
- zero-downtime upgrade handoff library,
- platform-specific deployment recipes and rollout automation.

If these are pulled into scope later, they should be added as a new phase after the current plan is stable rather than mixed into the core implementation DAG above.
