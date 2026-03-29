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
- projection and workflow libraries, and
- deterministic simulation coverage for the full stack.

Explicitly excluded from the main execution plan:

- physical sharding,
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
- **Phase 6** builds composition helpers, projections, and workflows.
- **Phase 7** expands deterministic simulation to the full stack.

## Parallel tracks

Once Phase 0 is complete, the work naturally splits into seven mostly independent tracks:

- **Track A — local engine core:** T04 and T06 in parallel; T05 after T04; T07 and T08 after T05 + T06; T09 after T07 + T08
- **Track B — LSM hardening:** T10 → T11; then T12, T13, T14, and T16 can proceed; T15 follows T11 + T13
- **Track C — change capture:** T17 → T18 → T19
- **Track D — remote storage:** T20 → T21 / T22 / T23
- **Track E — columnar:** T24 → T25 → T26 → T27
- **Track F — libraries:** T28, T29, and T30 start once their own engine dependencies are met; T31 depends on T30, and T32 depends on T18/T19/T28/T29
- **Track G — full-stack hardening:** T33

---

## Phase 0 — Stable contracts and deterministic substrate

**Parallelization:** T01 first. After that, T02 and T03 can proceed in parallel, with T03 consuming the abstractions established by T02.

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
3. Implement deterministic/simulated versions of all four traits for tests, using turmoil-backed simulated I/O/time where appropriate and mad-turmoil to catch nondeterminism that escapes the trait boundary.
4. Standardize error mapping so later tasks can distinguish I/O failure, corruption, timeouts, and durability boundaries cleanly.
5. Wire dependency injection through DB open/config so no subsystem constructs ambient implementations internally.

**Verification**

- Unit tests for open/read/write/range-read/rename/delete/list behavior on each adapter.
- Deterministic tests proving seeded RNG output and simulated clock behavior are reproducible under the turmoil/mad-turmoil test harness.
- Simulation tests that inject disk-full, timeout, stale-list, and partial-read failures through the trait boundary and verify structured errors rather than panics.

---

### T03. Build the deterministic simulation harness and shadow-oracle scaffolding

**Depends on:** T01, T02

**Description**

Build the reusable simulation harness that every later task will plug into. This includes seeded workload generation, fault injection controls, controlled crash/restart, and the first version of the oracle model.

**Implementation steps**

1. Build a deterministic simulation runner using a single Tokio runtime under turmoil's control, with mad-turmoil enabled so leaked wall-clock or entropy access is surfaced during tests.
2. Integrate a turmoil-based simulated object-store/network host and simulated filesystem adapters through the `FileSystem` and `ObjectStore` traits established in T02.
3. Implement seeded workload and fault-schedule generation plus trace capture.
4. Implement an initial shadow oracle for point state, sequence ordering, and recovery-prefix validation.
5. Add helpers to crash and restart the DB at controlled cut points.

**Verification**

- Reproducibility test: same seed under turmoil/mad-turmoil produces the same workload, fault schedule, and trace.
- Variance test: different seeds change at least one part of workload shape or execution order.
- Crash/restart simulation of an empty or stub DB to prove the harness itself is stable before engine logic is added.

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

Implement merge semantics end to end: merge-operand writes, read-time merge resolution, compaction-time full/partial merge, and forced collapse when operand chains get too long.

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

Implement background-work observability, scheduler callbacks, default scheduling policy, and engine-enforced safety guardrails that prevent deadlock or unbounded backlog regardless of scheduler behavior.

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

## Phase 3 — Unified-log change capture

**Parallelization:** T17 starts first. T18 can proceed once the commit coordinator exists. T19 depends on T17.

### T17. `scanSince` and `scanDurableSince` over the unified commit log

**Depends on:** T06, T07, T09

**Description**

Implement change capture on top of the unified commit log: gap-free iteration, per-table segment indexing, visible-vs-durable boundaries, and exact cursor semantics.

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

### T18. `subscribe` and `subscribeDurable` coalescing notifications

**Depends on:** T07

**Description**

Implement the coalescing notification channels paired with change capture. These are wake-up signals, not full event queues.

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

Implement retention and garbage collection for the unified commit log, including the change-feed version of `SnapshotTooOld`.

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

## Phase 4 — Remote storage modes

**Parallelization:** T20 starts first. After T20, T21, T22, and T23 can proceed in parallel subject to their listed dependencies.

### T20. Object-store integration substrate, caches, and range-read plumbing

**Depends on:** T02, T08

**Description**

Implement the shared object-store substrate used by tiered cold storage, backup, s3-primary mode, and remote columnar reads.

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

Implement tiered mode's cold-storage path: offloading old SSTables to S3, updating manifests, reclaiming local bytes, and reading cold SSTables through the normal read path.

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

Implement continuous backup/replication to S3, disaster recovery from S3, immutable manifest generations plus convenience pointers, and mark-and-sweep garbage collection of unreferenced S3 objects.

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

Implement memory + S3 mode, including buffered visible commits, explicit `flush()` durability, same-process hybrid `scanSince`, and the exact visible-vs-durable semantics described in the architecture.

**Implementation steps**

1. Implement in-memory commit-log buffering and memtable insertion without local durable fsync.
2. Implement `flush()` to ship buffered commit-log data and SSTables to S3.
3. Implement `currentDurableSequence()` and durable scanning for s3-primary mode.
4. Implement same-process hybrid `scanSince` that merges durable S3 segments with the in-memory visible buffer.
5. Ensure remote readers see only durable state while same-process readers can see the visible superset.

**Verification**

- Tests proving `commit()` makes writes visible but not durable until `flush()`.
- Crash tests showing everything after the last flush disappears consistently across source tables, cursors, projections, timers, outbox, and workflow state.
- Tests showing local `scanSince` can see visible buffered entries while `scanDurableSince` cannot.
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

Implement local and remote read paths for columnar SSTables, including point lookup, scans with column pruning, default-filling for newly added columns, and exact range-fetch behavior on S3.

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

Complete columnar support by integrating it with compaction, merge operators, and lazy schema evolution.

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

**Parallelization:** T28, T29, and T30 can begin independently once their own engine dependencies are met. T31 depends on T30. T32 depends on T18, T19, T28, and T29.

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

Implement reusable helper libraries for durable timers and transactional outbox processing. These are building blocks for workflows and other application code.

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

Implement the core projection runtime for single-source durable projections: whole-sequence batching, durable cursors, durable subscriptions, cursor/output atomicity, and watermark tracking.

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

Extend the projection runtime to support multiple source tables, frontier-pinned reads, dependency ordering, transitive waits where supported, and recomputation from SSTables or checkpoints after `SnapshotTooOld`.

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

Implement the workflow runtime: durable trigger admission, per-instance trigger ordering, ready-instance scheduling, inbox execution, callback admission, timer admission, outbox delivery, and crash recovery.

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

## Phase 7 — Full-stack deterministic hardening

**Parallelization:** This final phase should begin only after the main engine, projection, and workflow surfaces exist.

### T33. Full-stack randomized scenario generation and invariant suites

**Depends on:** T03, T16, T19, T22, T23, T27, T31, T32

**Description**

Build the full deterministic simulation matrix that exercises the engine, projections, and workflows together under randomized workloads and injected failures. This is the main correctness bar for the architecture as a whole.

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
Complete: T28–T32

At this point the system should additionally support:
- OCC transaction helpers,
- durable timers,
- transactional outbox,
- durable projections, and
- durable workflows.

### Milestone F — Full correctness bar
Complete: T33

At this point the system should have the deterministic simulation coverage needed to validate the entire stack under randomized failures.

---

## Deferred items from the architecture

The following architecture sections are intentionally **not** decomposed into implementation tasks here because they are either explicitly future work or outside the requested scope:

- physical per-table sharding,
- zero-downtime upgrade handoff library,
- platform-specific deployment recipes and rollout automation.

If these are pulled into scope later, they should be added as a new phase after the current plan is stable rather than mixed into the core implementation DAG above.
