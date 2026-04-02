# TASKS.md

## Purpose

This file turns the architecture into a dependency-aware implementation plan optimized for one developer working with AI assistance. Each task is intended to be small enough to hand to a helper once its dependencies are complete, and stable enough that multiple tasks can proceed in parallel without constant interface churn.

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
- opt-in physical per-table sharding and resharding via virtual partitions,
- scheduler integration,
- composition primitives built on top of the engine,
- projection and workflow libraries,
- explicit workflow run-history, visibility, deployment, and native-Rust-plus-sandbox execution support,
- an embedded sandbox runtime library,
- sandbox capability, migration, query, and published-procedure libraries layered above that runtime,
- an external MCP adapter layered above the sandbox/procedure stack,
- configurable historical workflow bootstrap and recovery,
- external stream ingress libraries for Kafka and Debezium,
- an embedded virtual filesystem library, and
- a `terracedb-bricks` blob / large-object library for out-of-line bytes plus metadata search, and
- an optional Arrow-ecosystem analytical export crate for derived snapshot / CDC-friendly outputs, and
- deterministic simulation coverage for the full stack.

Explicitly excluded from the main execution plan:

- mount/protocol adapters that expose the embedded virtual filesystem as a general-purpose writable host filesystem or generic network service,
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
- **Phase 8a** adds an embedded sandbox runtime library, host-disk/git/PR interop, and local/remote read-only editor views on top of `terracedb-vfs`.
- **Phase 8b** adds a `boa_engine`-derived `terracedb-js` runtime, a forked `gitoxide`-derived `terracedb-git` library, shared simulation-native host-service seams, explicit host bridges, whole-substrate deterministic hardening, and an updated sandbox-plus-git example.
- **Phase 9** adds the `terracedb-bricks` blob / large-object library on top of Terracedb.
- **Phase 10** adds an optional Arrow-ecosystem analytical export crate on top of Terracedb.
- **Phase 11** hardens the hybrid OLTP/OLAP path with columnar-v2 layout contracts, selective-read execution, segmented remote caching, stronger publish/recovery semantics, and a small analytically shaped example app.
- **Phase 12** adds generalized current-state retention and ranking policies plus a small example app that demonstrates how to configure them.
- **Phase 13** adds execution domains, control-plane isolation, and colocated multi-DB foundations.
- **Phase 14** adds unified-log pressure, flush reclamation, and adaptive write admission.
- **Phase 15** adds opt-in physical sharding, resharding via virtual partitions, and a small sharded example app.
- **Phase 16** adds configurable historical workflow bootstrap, replay, and recovery.
- **Phase 17** adds a Kafka ingress crate and deterministic broker simulation support.
- **Phase 18** adds a Debezium crate on top of Kafka ingress, including EventLog / Mirror / Hybrid materialization.
- **Phase 19** adds end-to-end CDC deterministic hardening and a small example app that demonstrates Kafka + Debezium + projections + workflows together.
- **Phase 20** adds sandbox capability policy, capability presets, shell-facing bridges, reviewed migrations, published procedures, MCP exposure, draft/live session UX, and an end-to-end example app on top of the sandbox/runtime stack.
- **Phase 21** refactors workflows around explicit runs, history, transition-engine contracts, and shared native/sandbox execution semantics, then closes with a toy example repo.
- **Phase 22** adds workflow deployment, visibility, upgrades, preview/prod flows, and a second toy example repo that demonstrates the operator and rollout surface.

## Parallel tracks

Once Phase 0 is complete, the work naturally splits into twenty-four mostly independent tracks:

- **Track A — local engine core:** T04 and T06 in parallel; T04a after T04; T05 after T04; T07 and T08 after T05 + T06; T09 after T07 + T08
- **Track B — LSM hardening:** T10 → T11; then T12, T13, T14, and T16 can proceed; T15 follows T11 + T13
- **Track C — change capture:** T17 → T18 → T19
- **Track D — remote storage:** T20 → T21 / T22 / T23; then T23a → T23b
- **Track E — columnar:** T24 → T25 → T26; then T26a and T27 can proceed in parallel
- **Track F — libraries:** T28, T29, and T30 start once their own engine dependencies are met; T28a follows T28; T31 depends on T30; T31a follows T31 and T31b follows T31a; T32 depends on T18/T19/T28/T29, T32c follows T32, and T32d follows T32c; T32a depends on T03a/T31/T32, and T32e depends on T03a
- **Track G — full-stack hardening:** T33b and T33c can begin once the relevant engine/runtime surfaces exist; T33 follows T33c; T33a and T33d follow T33
- **Track H — embedded virtual filesystem library:** T34 first; T35 depends on T34; T36 depends on T35; T37 depends on T35 + T36 + T30/T31; T38 depends on T35 + T36 + T37 + T22/T23; T39 depends on T36 + T37 + T38; T40 depends on T33 + T37 + T38 + T39
- **Track H2 — embedded sandbox runtime:** T40a first; T40b depends on T40a; T40c, T40d, and T40f can proceed once T40b and their library dependencies are ready; T40e depends on T40b + T40c; T40g depends on T40b + T40c + T40f; T40h depends on T33 + T40c + T40d + T40e + T40f + T40g; T40i depends on T40c + T40d + T40e + T40f + T40g + T40h
- **Track H3 — simulation-native sandbox substrate:** T40j first; T40k and T40n can proceed in parallel once T40j exists; T40l depends on T40k; T40m depends on T40k + T40l; T40o depends on T40l + T40n; T40p depends on T40m + T40o; T40q depends on T33 + T40m + T40o + T40p; T40r depends on T40i + T40p + T40q
- **Track I — `terracedb-bricks` blob / large-object library:** T41 first; T42 and T43 proceed in parallel after T41; T44 depends on T43 + T30/T31; T45 depends on T42 + T43; T46 depends on T33 + T44 + T45
- **Track J — analytical export crate:** T47 depends on T31 + T42; workflow-scheduled export adapters may be layered on once T32 exists but are not required for the base crate
- **Track K — hybrid-read and columnar-v2 hardening:** T48 first; T49 follows T48; then T50, T51, T53, T55, and T56 can proceed in parallel; T52 depends on T50 + T51; T54 depends on T51 + T52 + T55; T57 depends on T50 + T51 + T52 + T53 + T55 + T56; T58 depends on T52 + T53 + T54 + T55 + T56 + T57
- **Track L — generalized current-state retention and ranking:** T59 first; T60 and T61 proceed in parallel once the contracts and shared simulation/oracle seams from T59 exist; T62 follows once both policy families exist and can be coordinated with scheduler/offload behavior; T62a follows T62 once the public configuration surface and operational semantics are stable enough to teach through an example
- **Track M — execution domains and colocated multi-DB:** T63 first; T64, T65, and T66 can proceed in parallel; T67 depends on T64 + T65 + T66; T68 depends on T64 + T66 + T67; T69 depends on T64 + T65 + T66 + T67 + T68; T70 depends on T64 + T65 + T66 + T67
- **Track N — pressure-aware flushing and adaptive admission:** T71 first; T72, T73, and T74 can proceed in parallel; T75 depends on T73 + T74 + T70; T76 depends on T72 + T73 + T74 + T75
- **Track O — physical sharding and resharding:** T77 first; T78, T79, and T80 can proceed in parallel; T81 depends on T78 + T79 + T80; T82 depends on T78 + T79 + T80 + T81; T83 depends on T78 + T79 + T80 + T81
- **Track P — historical workflow processing:** T84 first; T85 and T86 can proceed in parallel after T84; T87 follows T85 + T86
- **Track Q — Kafka ingress:** T88 first; T89 follows T88; T90 follows T88 + T89; T91 follows T89 + T90
- **Track R — Debezium CDC materialization:** T92 follows T31 + T84 + T88 + T90 + T91; T93 follows T92; T94 follows T84 + T92 + T93; T95 follows T93 + T94
- **Track S — end-to-end CDC hardening and example app:** T96 follows T87 + T91 + T95; T97 follows T96; T98 follows T33 + T96 + T97; T99 follows T97 + T98
- **Track T — sandbox capabilities, procedures, and MCP:** T100 first; T101 follows T100; T101a follows T101 + T31a; T101b follows T101; T101c follows T40c + T101; T102 follows T100 + T101 + T101a; T102a follows T101a + T102 + T31a; T103 and T104 can proceed in parallel once T102 exists; T105 follows T101 + T102 + T104 + T40g; T106 depends on T33 + T101b + T101c + T102a + T103 + T104 + T105; T107 depends on T101b + T101c + T102a + T103 + T104 + T105 + T106
- **Track U — workflow run model and execution contract:** T108 first; T109, T110, and T111 can proceed in parallel once T108 exists; T112 depends on T109 + T110 + T111; T113 depends on T33 + T109 + T110 + T111 + T112; T114 depends on T109 + T110 + T111 + T112 + T113
- **Track V — workflow deployment, visibility, and upgrades:** T115 follows T108; T116, T117, and T118 can proceed in parallel once T115 exists; T119 depends on T116 + T117 + T118; T120 depends on T116 + T119 + T40i; T121 depends on T33 + T116 + T117 + T118 + T119 + T120; T122 depends on T116 + T117 + T118 + T119 + T120 + T121

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

**Parallelization:** After Phase 0, T04 and T06 can start together. T04a depends on T04 and can proceed in parallel with T05. T05 depends on T04. T07 depends on T05 and T06. T08 depends on T04, T05, and T06. T09 depends on T07 and T08.

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

### T04a. Builder-based DB open API and settings-vs-components split

**Depends on:** T04

**Description**

Add a higher-level builder API for opening Terracedb before the public surface area grows further. The goal is to separate ordinary user-facing settings from swappable runtime components: storage mode, durability knobs, cache sizes, and scheduler selection should read like configuration, while filesystem/object-store/clock/RNG overrides remain explicit advanced hooks for tests and embedding. The existing low-level `Db::open(config, dependencies)` path should remain available as an escape hatch or compatibility wrapper, but most callers should no longer need to construct every dependency by hand.

**Implementation steps**

1. Define a `DbBuilder` public surface (for example via `Db::builder()`) that can construct Terracedb with production defaults while still supporting explicit overrides.
2. Split the builder API between:
   - settings methods for storage mode and tuning knobs, and
   - component methods for injected runtime implementations such as filesystem, object store, clock, RNG, and scheduler.
3. Add ergonomic constructors/helpers for the common storage modes so simple tiered and s3-primary setups do not require callers to manually assemble the full `DbConfig`/`DbDependencies` graph.
4. Keep the low-level `Db::open(config, dependencies)` path as a lower-level wrapper/escape hatch, and document which API is intended for ordinary users versus advanced embedders/tests.
5. Ensure later configuration growth can extend the builder API additively rather than forcing breaking changes to the main open signature.

**Verification**

- API tests proving a caller can open a DB with production defaults through the builder without manually constructing filesystem/object-store/clock/RNG dependencies.
- Tests proving builder-specified component overrides actually flow through to the runtime, including deterministic fake clock/RNG and object-store/file-system adapters.
- Backward-compatibility tests showing the low-level `Db::open(config, dependencies)` path continues to work and remains behaviorally equivalent to the builder-produced configuration.
- Documentation/examples demonstrating simple tiered setup, simple s3-primary setup, and one advanced embedding/test setup with custom components.

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

**Parallelization:** T20 starts first. After T20, T21, T22, and T23 can proceed in parallel subject to their listed dependencies. T23a follows once the durable remote/control-plane formats and their consumers exist. T23b follows T23a.

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

### T23a. Durable-format compatibility contracts, golden fixtures, and local pre-commit checks

**Depends on:** T04, T06, T08, T20, T22, T23

**Description**

Treat Terracedb's persisted bytes as long-lived contracts before changing any of the metadata/control-plane encodings. This task inventories the durable formats already in use, defines explicit compatibility expectations for each, and adds golden fixtures plus local pre-commit guardrails so accidental format drift fails during development instead of surfacing as reopen/recovery breakage later. Scope includes the catalog, commit-log frames, segment footers, local manifests, remote manifests, remote-cache metadata, and backup-GC metadata; hot SSTable layout evolution remains owned by the SSTable tasks.

**Implementation steps**

1. Write down the durable-format policy for each owned format: what bytes are treated as reviewed artifacts for the current version, how version bumps work, and what must fail closed.
2. Add golden fixtures for representative versions and variants of catalog files, commit-record frames, segment footers, local manifests, remote manifests, remote-cache metadata, and backup-GC metadata.
3. Add tests proving current decoders preserve checksum/version semantics and reject corrupt or unsupported variants predictably.
4. Add encode/regression tests for formats where canonical bytes are intentionally part of the current contract, and semantic round-trip assertions where exact byte identity is not the boundary.
5. Add a documented fixture-regeneration workflow and pre-commit gating so intentional durable-format changes require explicit review.

**Verification**

- Golden compatibility tests for catalog, commit-log frame, segment-footer, manifest, remote-manifest, remote-cache metadata, and backup-GC metadata fixtures.
- Corruption and unsupported-version tests proving each format fails closed rather than silently accepting malformed bytes.
- Local pre-commit coverage that fails when durable-format bytes or schemas change without an explicit fixture/schema update.

---

### T23b. FlatBuffers for catalog, manifests, and remote metadata

**Depends on:** T23a

**Description**

Replace JSON for the structured metadata/control-plane formats where schema discipline and lower parse overhead are worth the complexity, while deliberately keeping custom binary framing for the commit log and custom/raw layouts for SSTable data blocks. This task moves the catalog, local manifest, remote manifest, remote-cache metadata, and backup-GC metadata onto FlatBuffers with explicit local schema/fixture review and fail-closed versioning. Terracedb is still greenfield here, so older JSON payloads do not need compatibility shims.

**Implementation steps**

1. Define FlatBuffers schemas for the catalog, local manifest, remote manifest, remote-cache metadata, and backup-GC metadata, including explicit versioning rules and file identifiers for each durable artifact.
2. Keep the checked schema reference, Rust wrapper/bindings, and reviewed fixtures in sync with local tests and pre-commit checks so incompatible schema drift fails before commit.
3. Switch readers and writers fully to canonical FlatBuffers payloads for these metadata formats; do not carry JSON fallback readers in this greenfield phase.
4. Preserve checksums and fail-closed validation behavior where applicable; do not migrate commit-log frames, row SSTables, or columnar data blocks in this task.
5. Update open, recovery, cache-rebuild, and backup-GC paths plus their fixtures/tests so the new schemas are exercised end to end.

**Verification**

- Golden FlatBuffers fixture tests plus local schema-conformance/source-of-truth checks for the new metadata formats.
- Restart, recovery, offload, backup, and cache-rebuild tests proving behavior is unchanged apart from the payload encoding.
- Corruption and unsupported-schema tests proving malformed FlatBuffers or incompatible schema changes fail closed.
- Tests and comments making it explicit that commit-log frames and SSTable hot-data layouts remain custom formats after this migration.

---

## Phase 5 — Columnar tables

**Parallelization:** T24 can begin once metadata contracts exist. T25 depends on T24 plus flush machinery. T26 depends on T25. T26a depends on T20 + T26. T27 depends on T26 plus compaction/merge support, and can proceed in parallel with T26a.

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

### T26a. Lazy columnar read-path caching and mode-aware cache admission

**Depends on:** T20, T26

**Description**

Harden the lazy columnar read path with cache layers that match Terracedb's two storage modes and current implementation shape. This task is intentionally scoped to columnar SSTables: row SSTables are already loaded as resident decoded structures, so the immediate goal is to stop repeated footer/index/metadata/column-block fetch and decode work on lazy columnar reads. Wire remote columnar range reads through the existing raw-byte cache where appropriate and add an in-memory decoded cache for reusable columnar metadata and hot decoded column blocks in both tiered and s3-primary mode.

**Implementation steps**

1. Thread a shared storage/cache context through the lazy columnar read helpers so remote range fetches actually benefit from the object-store cache substrate introduced in T20 instead of bypassing it.
2. Add an in-memory decoded cache for reusable lazy-columnar read artifacts such as footers, key indexes, sequence columns, tombstone bitmaps, row-kind metadata, and hot decoded column blocks.
3. Make cache admission mode-aware:
   - local tiered reads should use the decoded cache without pretending the local SSD is a remote-byte cache,
   - remote tiered cold reads and s3-primary reads may use both the raw-byte cache and the decoded cache, and
   - point reads and scans should be allowed to use different population rules to avoid scan pollution.
4. Key cache entries by immutable SSTable identity plus block/column identity so cached decode results remain valid across compaction output replacement and reopen.
5. Keep the scope specific to lazy columnar metadata and column blocks for now; if row SSTables later move to block-oriented lazy reads, extend the cache design in a follow-on task rather than broadening this one implicitly.
6. Add lightweight observability for cache hit/miss behavior and decode avoidance so later tuning work can compare point-read vs scan-heavy workloads.

**Verification**

- Tests proving repeated columnar point reads in tiered mode avoid redundant footer/index/metadata/column-block decoding even when the SSTable is local.
- Tests proving remote columnar reads in tiered cold-storage mode and s3-primary mode hit the raw-byte cache when re-reading the same ranges, while local tiered reads do not require any remote-byte cache layer.
- Tests proving point reads and scans can use different cache-population rules without changing query results.
- Cache-on/cache-off equivalence tests showing decoded caching changes latency/CPU behavior but not logical reads, MVCC visibility, or schema-default filling.
- Restart tests proving decoded caches can be dropped and rebuilt safely, while durable raw-byte cache metadata still rebuilds from object-store state.
- Tests and comments making it explicit that row SSTables are out of scope for this task because they are already loaded as resident decoded structures.

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

**Parallelization:** T28, T29, and T30 can begin independently once their own engine dependencies are met. T28a depends only on T28, so the typed-records work can proceed in parallel with the runtime tasks. T31 depends on T30. T31a depends on T31, and T31b depends on T31a. T32 depends on T18, T19, T28, and T29. T32c depends on T32, and T32d depends on T32c. T32a depends on T03a, T31, and T32. T32e depends on T03a and can proceed in parallel with the projection/workflow ergonomics work. T32b depends on T03a, T16, T31, and T32.

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

### T28a. Typed record/table helper crate (`terracedb-records`)

**Depends on:** T28

**Description**

Create a dedicated typed-record helper crate on top of `terracedb` so application and example code stop hand-rolling byte keys, value serialization, and repetitive decode/error plumbing. The goal is a lightweight typed layer, not a separate schema engine.

**Implementation steps**

1. Create a new crate boundary, `terracedb-records`, with stable typed wrappers such as `RecordTable<K, V, KC, VC>` and `RecordTransaction`.
2. Define narrow codec traits for keys and values, plus default codecs for common cases such as UTF-8 strings, fixed-width integers, and serde-backed JSON values.
3. Implement typed `read`, `write`, `delete`, `scan`, and `scan_prefix` helpers over `Table` and `Transaction` without weakening Terracedb’s existing atomicity or visibility semantics.
4. Preserve structured errors so callers can distinguish storage failures, decode failures, and application-level validation failures cleanly.
5. Document migration guidance for example applications so the typed layer is clearly optional but preferred for application-facing code.

**Verification**

- CRUD tests for typed keys and values using both direct table access and transactions.
- Scan-order tests proving key codecs preserve the ordering guarantees required by range scans.
- Crash/recovery integration tests proving the typed wrapper does not change batch atomicity or durability behavior.
- Error-path tests showing invalid payloads surface as typed decode failures rather than panics or opaque storage errors.

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

### T31a. Projection API unification for scan-capable single-source handlers

**Depends on:** T31

**Description**

Remove the API mismatch where single-source projections have a less capable handler/context surface than multi-source projections. A single-source projection should be able to scan, read, and rebuild through the same conceptual interface as a multi-source projection; “single source” should be a convenience mode, not a capability restriction.

**Implementation steps**

1. Choose a single canonical handler/context model for projections and make single-source and multi-source constructors target that shared surface.
2. Ensure single-source handlers can use the same `ProjectionContext` read/scan operations that currently force users onto the multi-source path for simple cases.
3. Preserve or provide a clear migration path for existing projection code so ergonomic improvements do not require an all-at-once rewrite.
4. Keep watermarking, recomputation, and dependency semantics unchanged while simplifying the handler API.
5. Update projection examples and docs to demonstrate the intended single-source path explicitly.

**Verification**

- Tests proving a single-source projection can perform deterministic scans and point reads without going through the multi-source wrapper.
- Compatibility tests showing existing multi-source behavior and tie-breaking remain unchanged.
- Rebuild/recompute tests confirming the unified API does not weaken recovery behavior.
- Example-level tests that no longer need a multi-source wrapper solely to gain `scan` access.

---

### T31b. Generic ranked-materialization helper for projections

**Depends on:** T31a

**Description**

Add a reusable projection helper for “materialize the top N rows according to a caller-defined ranking.” This should be generic over ranking logic and tie-breaking rather than hard-coded to `updated_at`, “recent items,” or any particular application domain.

**Implementation steps**

1. Add a helper in `terracedb-projections` that rescans source state, applies a caller-provided ranking function, truncates to a caller-provided limit, and rewrites the materialized output deterministically.
2. Require the ranking contract to produce a total order, either directly or via an explicit tie-break callback, so reruns remain deterministic.
3. Support caller-provided output-key mapping and encoding hooks so the helper can power both example apps and library consumers without forcing a specific schema.
4. Document the performance/semantic tradeoff clearly: the initial helper may be full-recompute-per-batch, but it must be correct and deterministic.
5. Add at least one example-oriented adapter or recipe showing how the helper replaces a hand-written “recent items” projection.

**Verification**

- Projection tests covering inserts, updates, deletions, ties, and truncation at the `N` boundary.
- Deterministic replay tests proving the same source history yields the same ranked output.
- Recompute tests showing rebuild-from-current-state produces the same result as incremental tailing.
- Example-oriented tests proving the helper can replace a bespoke “recent TODOs” style projection.

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

### T32c. Workflow progress modes and safe autonomous defaults

**Depends on:** T32

**Description**

Replace the current boolean durable-progress tuning with an explicit workflow progress policy that makes the safe choice the default. Timer-driven and callback-driven workflows should not require users to discover an obscure flag before they make forward progress correctly under crash/restart.

**Implementation steps**

1. Replace the boolean `with_durable_progress(bool)` style API with a semantic progress-mode configuration that distinguishes durable, buffered, and default/auto behavior.
2. Define the default policy so autonomous workflow progress paths, especially timers and callbacks, choose durable behavior unless the caller explicitly opts into a weaker mode.
3. Preserve a deliberate opt-in fast path for advanced users, but make the weaker mode explicit in naming and documentation rather than easy to select accidentally.
4. Add runtime/docs guidance for when buffered progress is acceptable and when it is not.
5. Update existing workflow examples and tests to use the new API shape and defaults.

**Verification**

- Timer-chain tests proving the default workflow configuration makes forward progress without unrelated flushes or writes.
- Crash/restart tests showing autonomous workflows recover from durable state only under the default mode.
- Migration tests covering the old boolean API shape if compatibility shims are kept temporarily.
- Negative tests showing an explicitly buffered mode behaves as documented rather than silently acting durable.

---

### T32d. Recurring workflow helper in `terracedb-workflows`

**Depends on:** T32c

**Description**

Add a first-class recurring-workflow helper that owns bootstrap, timer scheduling, next-fire computation, and state persistence so application code only implements “what happens on each tick.” This is the workflow-side counterpart to the relay and projection helpers: it should package a common, easy-to-get-wrong pattern into a safe default.

**Implementation steps**

1. Define a `RecurringWorkflow`-style abstraction that separates bootstrap and tick behavior from the underlying timer/callback plumbing.
2. Encapsulate stable timer IDs, bootstrap callback admission, next-fire scheduling, and recurring state updates inside the helper.
3. Make the helper use the safe workflow progress defaults from T32c automatically.
4. Support both fixed-interval recurrence and caller-defined next-fire calculation so the helper covers weekly planners and similar business schedules.
5. Migrate at least one example workflow to this helper and update docs to show the helper as the default recommendation.

**Verification**

- Unit and simulation tests for bootstrap, repeated tick execution, skipped duplicate bootstrap callbacks, and crash/restart recovery.
- Timer-replay tests proving recurring ticks are idempotent when application handlers use stable IDs or state guards.
- Example migration tests showing a weekly-planner-style workflow becomes smaller without losing deterministic behavior.
- API tests confirming the helper still allows advanced callers to supply custom scheduling logic where needed.


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

### T32e. Deterministic HTTP simulation harness crate (`terracedb-http`)

**Depends on:** T03a

**Description**

Create a dedicated HTTP simulation harness crate for Terracedb-based applications. The crate should own simulated client/server wiring over turmoil and expose framework adapters as optional integration layers, with `axum` support first and lower-level `hyper` primitives available underneath.

**Implementation steps**

1. Create `terracedb-http` as the authoritative crate for deterministic HTTP server/client helpers layered on top of the reusable simulation framework.
2. Provide a low-level simulated HTTP server/client harness that can run inside turmoil hosts and surface useful tracing, seed, and barrier/debug hooks.
3. Add an `axum` integration surface or feature that makes it easy to serve an `axum::Router` inside simulation without every example hand-rolling accept loops.
4. Keep the crate framework-agnostic at its core so future adapters can target raw `hyper`, TLS, or other Rust HTTP stacks without splitting the concept across multiple crates.
5. Document fault-injection and debugging patterns for HTTP simulations, including barriers, partitions, retries, and trace capture.

**Verification**

- Simulation tests proving a client can perform create/read/list style HTTP requests against a simulated server host deterministically.
- Fault-injection tests covering delayed responses, dropped connections, partitions, and restart/retry behavior.
- Adapter tests proving the `axum` integration uses the same deterministic harness rather than a separate ad hoc path.
- Same-seed reproducibility tests showing HTTP traces and request/response outcomes are replayable.

---

## Phase 7 — Full-stack deterministic hardening

**Parallelization:** This hardening phase should begin only after the main engine, projection, workflow, and reusable simulation surfaces exist. T33b and T33c can start once their dependencies are ready; T33e should follow once the property-test conventions and shared failpoint/cut-point layer exist; T33 and T33f then consume those shared generators/oracles across the core DB, projections, workflows, and other libraries; T33a and T33d operationalize the deterministic and real-remote suites.

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

### T33b. Property-based, parameterized, and snapshot-style invariant suites

**Depends on:** T23a, T27, T31, T32

**Description**

Add a complementary invariant-testing layer on top of Terracedb's simulation-heavy strategy by adopting `proptest`, `rstest`, and targeted `insta` snapshots where each tool fits best. The goal is not to replace deterministic simulation or durable-format golden fixtures, but to cover the awkward middle ground: broad low-level state-space exploration, configuration-matrix collapse, and stable structured-output assertions. Durable byte-format compatibility should continue to live under T23a's golden fixtures; this task is about semantic invariants and test ergonomics across the engine, projections, and workflows.

**Implementation steps**

1. Add `proptest`, `rstest`, and `insta` to the dev/test stack together with project conventions for when each should be used.
2. Introduce property-based tests for low-level invariants such as commit-log frame round-trips, MVCC key ordering, watermark/prefix monotonicity, manifest/object-layout invariants, and supported row-vs-columnar read equivalence.
3. Use `rstest` or an equivalent parameterized style to collapse repeated test matrices across storage mode, durability mode, row vs columnar, and local vs remote/offloaded placement.
4. Add targeted `insta` snapshot coverage only for stable structured outputs where human-reviewed diffs are helpful, such as normalized debug renderings, telemetry payload shapes, or simulation summaries; do not use snapshot assertions as a substitute for durable-format golden fixtures.
5. Document shrink/repro workflows so property-test failures produce small repro cases that developers can rerun locally.

**Verification**

- Property tests find and shrink invariant violations in core encode/decode, ordering, and visibility/durability logic rather than relying only on hand-written examples.
- Parameterized tests reduce duplicated mode-matrix coverage while keeping row/columnar and tiered/s3-primary behaviors explicit.
- Snapshot tests prove stable structured outputs stay reviewable and intentional without becoming the authoritative durability contract.
- Tests and docs make the split explicit: durable byte-format fixtures remain under T23a, while T33b owns semantic/property/snapshot coverage.

---

### T33c. Unified failpoint and cut-point framework for deterministic and async tests

**Depends on:** T22, T23, T27, T31, T32

**Description**

Generalize Terracedb's existing ad hoc phase blockers and adapter-level fault injection into a named failpoint/cut-point framework that ordinary async tests and deterministic simulation can share. The point is to make exact failure sites easy to express and reuse: tests should be able to pause, error, corrupt, or drop work at precise storage/runtime boundaries without adding one-off test-only APIs every time a new crash or retry path needs coverage.

**Implementation steps**

1. Design a reusable failpoint registry/helper layer with named hooks, one-shot vs persistent modes, and actions such as pause, injected error, dropped response, timeout, or corruption.
2. Instrument critical production paths with named cut points, including commit phases, manifest installation/publication, backup upload ordering, offload transitions, remote-manifest recovery, projection cursor/output commits, workflow trigger admission/execution, timer handling, and outbox delivery.
3. Preserve and integrate the existing adapter-level filesystem/object-store fault injection so transport-level failures and exact internal cut-point failures can be composed in the same tests.
4. Migrate the current bespoke blocker-style tests toward the shared failpoint layer where it improves clarity, while keeping the public runtime API free of test-only complexity.
5. Add documentation and helpers so seeded simulations and ordinary `tokio::test` integration tests can activate the same failpoints consistently.

**Verification**

- Tests can deterministically force exact cut-point failures in commit, manifest, offload, projection, and workflow paths without bespoke per-subsystem blocker plumbing.
- Existing crash/recovery tests remain reproducible after migration to the shared failpoint layer.
- Combined tests can compose internal failpoints with adapter/object-store failures to exercise multi-layer retry and recovery behavior.
- Production builds remain cheap/no-op when failpoints are disabled.

---

### T33d. Real object-store chaos suite with LocalStack, Toxiproxy, and HTTP fault injection

**Depends on:** T20, T22, T23, T26a, T33

**Description**

Add a real remote-integration hardening layer that complements Terracedb's stubbed object-store faults with actual S3-compatible traffic under network- and HTTP-level chaos. This task should stand up a reproducible local environment using LocalStack plus proxy-based fault injection and exercise the real object-store adapter path for s3-primary flush/recovery, tiered backup/offload behavior, cache rebuilds, and remote columnar range reads.

**Implementation steps**

1. Add a Docker Compose or equivalent local/CI environment for at least:
   - LocalStack (or another S3-compatible service),
   - Toxiproxy for TCP/network faults, and
   - an HTTP-level fault proxy or equivalent for transient 429/503 and related request-path failures.
2. Build a real-remote integration harness that can point Terracedb at the chaos environment without changing production code paths.
3. Cover baseline and faulted scenarios for:
   - s3-primary `flush()` durability and recovery,
   - tiered backup manifest ordering and disaster recovery,
   - cold offload plus remote reads,
   - remote-cache rebuilds,
   - remote columnar exact-range reads and cache-assisted rereads.
4. Add representative chaos scenarios such as latency/jitter, bandwidth caps, intermittent resets, timeouts, stale-list-like behavior where possible, and transient HTTP 429/503 responses.
5. Integrate the suite into CI in tiers: a smaller must-pass smoke slice for pull requests and larger/nightly chaos campaigns with artifact capture for logs, proxy state, and failing scenario inputs.

**Verification**

- Real object-store integration tests pass against a baseline LocalStack-backed environment with no proxy faults.
- Faulted scenarios prove Terracedb fails closed, retries where appropriate, and recovers to the correct durable prefix rather than panicking or inventing state.
- Remote-manifest load/recovery, backup/offload ordering, cache rebuild, and remote columnar range-read behavior remain correct under injected network and HTTP faults.
- CI/nightly jobs preserve enough logs and scenario metadata to reproduce a failing chaos run locally.

---

### T33e. Shared cross-cutting fuzz substrate crate for generated simulation and replay plumbing

**Depends on:** T03a, T27, T31, T32, T32a, T32e, T33b, T33c

**Description**

Create the low-level reusable crate that broad-input testing will build on: seed campaigns, scenario replay helpers, artifact codecs, minimization/shrink plumbing, and generic invariant utilities that are useful across Terracedb libraries and third-party applications. This crate should stay deliberately lower-level and avoid baking in one opinionated application stack. The intent is for `terracedb-fuzz` to be the shared substrate that both crate-local generated suites and higher-level system/application harnesses can rely on.

**Implementation steps**

1. Add the public workspace crate `crates/terracedb-fuzz` with a stable-Rust API for:
   - seed campaigns and replay helpers,
   - scenario artifact save/load/encoding helpers,
   - generic minimization/shrink helpers,
   - generic invariant utilities, and
   - scenario-runner traits that are neutral about the application stack sitting above them.
2. Keep the crate low-level and broadly reusable:
   - good contents include seed handling, replay capture, scenario codecs, minimization, and neutral simulation/repro plumbing,
   - bad contents include opinionated HTTP app-stack builders, CDC-plus-web orchestration, or first-party-only omnistack wiring.
3. Add low-level adapters where that abstraction holds naturally, including at minimum:
   - core DB generated scenarios,
   - VFS/current-state helpers,
   - retention or record-level helpers where the invariants are still substrate-like, and
   - other libraries whose state-machine semantics can be exercised without assuming a full application stack.
4. Document the layering explicitly:
   - crate-local generated/property tests can consume `terracedb-fuzz` directly,
   - higher-level application/system harnesses should build on top of it rather than inside it, and
   - open-ended `cargo-fuzz` executables should be thin drivers over the same reusable types.
5. Document the supported failure workflow:
   - generated runs execute against serializable scenarios,
   - failures shrink or minimize into stable repro artifacts, and
   - minimized cases can be promoted into fixed regression tests in the owning crate or application.

**Verification**

- Multiple workspace crates and at least one application/example consume the shared substrate rather than each inventing ad hoc replay/minimization helpers.
- Generated scenarios can be replayed deterministically from a saved seed or minimized scenario artifact.
- The crate remains reusable and public-facing instead of accreting first-party-only app-stack assumptions.
- Docs make the separation explicit: `terracedb-fuzz` owns reusable low-level fuzz/simulation machinery, while higher-level system/app composition lives above it.

---

### T33f. Public systemtest crate plus workspace-wide generated simulation and fuzz suites

**Depends on:** T23a, T33, T33d, T33e

**Description**

Build the public application/system harness layer on top of T33e and use it to power the broad-input suites. The key design goal is that the same systemtest harness can be driven either by a pre-selected deterministic seed under normal `cargo test` / `nextest` or by time-budgeted mutated scenario inputs under `cargo-fuzz`. This task should explicitly cover not only crate-local state-machine suites, but also cross-library composition and at least one example application using the same public API external users would use. The layered strategy should become:

- generated deterministic simulation for ordering, recovery, retries, and restart behavior,
- public app/system harnesses that can be shared by examples and external applications,
- property-based broad input coverage for semantic invariants, and
- dedicated fuzz targets for byte-level decoders and serialized higher-level scenarios.

**Implementation steps**

1. Add the public `crates/terracedb-systemtest` crate on top of `terracedb-fuzz`, with API support for:
   - defining serializable application/system scenarios,
   - running those scenarios from deterministic seeds,
   - replaying serialized scenarios through the same runner,
   - optionally consuming mutated scenario bytes through the same runner, and
   - re-exporting the deterministic simulation/HTTP helpers that application-level tests commonly need.
2. Use the same public `terracedb-systemtest` API in at least one example application so the design is exercised through real usage rather than only in first-party crate tests. The initial proving ground should include `examples/todo-api`, demonstrating that an application can keep a mostly flat local `tests/` directory while still getting seeded scenario runs and application-level deterministic simulation.
3. Add generated simulation/property suites that run broad bounded scenarios against at least:
   - core DB recovery/state-machine behavior,
   - projections frontier/materialization behavior,
   - workflows inbox/outbox/timer/retry behavior,
   - relays/source-progress behavior, and
   - one or more supporting libraries such as VFS, retention, or records where generated scenarios can expose state-machine bugs.
4. Add cross-library generated suites that compose the real production code paths rather than isolated stubs, for example:
   - DB writes flowing into projections,
   - DB/projection outputs driving workflows,
   - relay/source progress interacting with projection/workflow replay,
   - restart/recovery cases spanning more than one library boundary, and
   - application-facing flows where writes or ingress events drive projections/workflows/HTTP-visible state.
5. Add dedicated `cargo-fuzz`-style targets under a separate fuzz package for parser/codec-heavy surfaces such as:
   - commit-log frame decode,
   - segment/footer or manifest parsing,
   - remote-cache metadata and control-file parsing, and
   - any other durable or transport-facing decoders that benefit from byte-level mutation.
6. Ensure the two execution modes share one vocabulary rather than two parallel harnesses:
   - deterministic tests should run fixed seeds or saved scenarios through `terracedb-systemtest`,
   - fuzz targets should mutate serialized scenarios or byte-level parser inputs and then call into the same reusable runner layer,
   - application examples should not need bespoke internal-only glue to participate.
7. Establish the failure-promotion workflow:
   - save failing seeds/scenarios or minimized byte corpora,
   - check in deterministic repros where appropriate,
   - keep the resulting fixed regression in the owning crate while the broad-input harness stays cross-cutting.
8. Integrate the new suites operationally:
   - lightweight local smoke coverage,
   - pre-commit-friendly generated/property slices where cost is acceptable,
   - larger CI/nightly campaigns for broad seeded exploration and fuzz corpus growth.

**Verification**

- Broad-input suites cover core DB, projections, workflows, and additional workspace libraries rather than stopping at the storage engine alone.
- The public `terracedb-systemtest` crate is usable by at least one example application for both seeded deterministic runs and serialized-scenario replay.
- Cross-library generated runs find and replay failures that depend on restart/recovery, scheduling, ordering, or app-surface interaction across crate boundaries.
- Parser/codec fuzz targets and any higher-level scenario mutators fail safely on malformed inputs and can reproduce minimized crashing inputs locally.
- New failures can be promoted into stable deterministic regressions without duplicating runner logic across crates or applications.

---

## Phase 8 — Embedded virtual filesystem library

**Parallelization:** T34 first. T35 depends on T34. T36 depends on T35. T37 depends on T35, T36, T30, and T31. T38 depends on T35, T36, T37, T22, and T23. T39 depends on T36, T37, and T38. T40 depends on T33, T37, T38, and T39.

### T34. Freeze the embedded virtual filesystem crate boundary, semantics, and reserved tables

**Depends on:** T01, T28

**Description**

Define the embedded virtual filesystem public surface and the reserved table/key contracts before implementation work branches. This task freezes the semantic target: provide a useful filesystem/KV/tool/overlay model on Terracedb for in-process embedded runtimes, not SQL compatibility or mount-oriented adapter behavior.

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

Implement the Terracedb-native reproduction story for embedded volumes: short-lived read-only snapshots, logical clone/export flows, and writable overlay volumes backed by read-only virtual filesystem bases. This replaces the SQLite-era “copy the database file” portability story with a Terracedb-native equivalent.

**Implementation steps**

1. Implement `VolumeSnapshot` as a read-only volume view bound to a visible or durable cut.
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

### T39. Expose the embedded Rust API and embedded-runtime integration examples

**Depends on:** T36, T37, T38

**Description**

Expose the virtual filesystem crate the way applications actually use it: as an embedded Rust library for in-process embedded sandboxes. The goal is a stable SDK and examples for embedded runtimes, not a second wave of mount, CLI, or service implementations.

**Implementation steps**

1. Finalize the Rust SDK surface for path-based filesystem operations, KV, tool runs, snapshots, overlays, activity tailing, and flush.
2. Add convenience helpers and examples for common embedded runtime patterns:
   - open a base volume,
   - create a writable overlay for one run/session,
   - expose a bounded capability surface to the caller or runtime,
   - inspect recent file/tool activity.
3. Add end-to-end examples that run an embedded or session-scoped harness against the same embedded volume API used by production code.
4. Ensure no example or helper bypasses the crate and writes reserved tables directly.
5. Document the explicit version-1 non-goals:
   - no FUSE/NFS/MCP/HTTP/service boundary,
   - no host filesystem mount surface,
   - no mount-oriented inode/handle API.

**Verification**

- Integration tests through the SDK for filesystem, KV, tool-run, snapshot, and overlay operations.
- Example tests showing an embedded harness can use a base volume plus writable overlay without touching internal tables.
- Restart tests showing existing volumes and overlays reopen without repair or migration tricks.
- API tests proving the caller-facing surface stays path-based and mount-independent.

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

## Phase 8a — Embedded sandbox runtime library

**Phase rule:** T40a freezes the sandbox crate boundary, editor-view protocol, and deterministic seams first. Every implementation task in this phase must add deterministic simulation coverage for the parts that can run under the seeded harness immediately. Any part of the real stack that cannot run under the simulation harness with acceptable fidelity must be hidden behind a stable interface with a deterministic stub/fake backend and tested through that seam in the same task. Do not defer the simulation-boundary decision to a late hardening-only step.

**Parallelization:** T40a first. T40b depends on T40a. T40c, T40d, and T40f can proceed in parallel once T40b and their own library dependencies exist. T40e depends on T40b and T40c. T40g depends on T40b, T40c, and T40f. T40h depends on T33, T40c, T40d, T40e, T40f, and T40g. T40i depends on T40c, T40d, T40e, T40f, T40g, and T40h.

### T40a. Freeze the sandbox crate boundary, capability contracts, editor-view protocol, and deterministic seams

**Depends on:** T03a, T34, T39

**Description**

Define the `terracedb-sandbox` public surface and the simulation boundary before implementation work branches. This task answers a critical question up front: which parts of the sandbox stack can run under the deterministic simulation harness unchanged, and which parts must sit behind stable interfaces with deterministic stub implementations.

**Implementation steps**

1. Add a new workspace member (for example `crates/terracedb-sandbox`) and define its top-level module boundaries:
   - session lifecycle,
   - runtime backend / actor,
   - filesystem and module-loader shims,
   - capability registry and injected app APIs,
   - TypeScript service,
   - bash service,
   - package-install service,
   - disk/git/PR interop,
   - read-only editor/view service,
   - shared error and metadata types.
2. Freeze the core public contracts:
   - `SandboxStore`,
   - `SandboxSession`,
   - `SandboxRuntimeBackend`,
   - `PackageInstaller`,
   - `GitWorkspaceManager`,
   - `PullRequestProviderClient`,
   - `ReadonlyViewProvider`,
   - `SandboxCapability`,
   - session provenance and conflict-policy types.
3. Freeze the guest-visible module and URI namespaces:
   - sandbox module specifiers such as `terrace:/...` and `terrace:host/...`,
   - any package/builtin compatibility modes,
   - the read-only editor/view URI or protocol surface used by VS Code and Cursor.
4. Evaluate which concrete integrations can run under the current deterministic harness with acceptable fidelity:
   - the real pure-Rust JS runtime backend (currently `boa_engine`),
   - `just-bash`,
   - package fetching and integrity/cache transitions,
   - git export / worktree preparation,
   - PR provider integration,
   - the local/remote read-only editor view service.
5. For anything that is not a good direct simulation target, define the stable interface and deterministic stub/fake implementation strategy now rather than after implementation begins.

**Verification**

- Compile-only tests that instantiate all public sandbox/session/capability/view interfaces.
- Unit tests that round-trip session provenance, conflict-policy, and view-handle metadata encodings.
- A deterministic smoke test that opens a sandbox session over a fake volume and a deterministic stub backend without touching real I/O, the real JS engine, git, or network services.

---

### T40b. Implement session lifecycle, provenance metadata, backend abstraction, and deterministic stub backends

**Depends on:** T40a, T38, T39

**Description**

Implement the shared sandbox substrate that every later task uses: session open/close, overlay binding, persistent session metadata, provenance capture, and the stable backend interfaces that separate deterministic semantics from non-simulatable production integrations.

**Implementation steps**

1. Implement session open/reopen/close on top of `terracedb-vfs` overlays, including `/.terrace/session.json` and any supporting KV metadata.
2. Implement stable provenance metadata for:
   - base volume/snapshot identity,
   - hoisted source path and mode,
   - git repo root / commit / branch / remote when applicable,
   - package-compat and capability configuration,
   - active read-only view handles if the architecture needs to track them.
3. Implement the abstract backend/service traits frozen in T40a plus deterministic stub/fake implementations for simulation:
   - runtime backend,
   - package installer,
   - git workspace manager,
   - PR provider client,
   - read-only view provider.
4. Wire all nontrivial sandbox operations into VFS tool-run/activity semantics so later tasks inherit the same auditability model.
5. Add recovery/reopen logic so existing sessions preserve metadata and provenance without repair steps.

**Verification**

- Reopen tests proving session metadata and provenance survive restart unchanged.
- Tests showing sandbox lifecycle operations append the expected tool/activity records and do not drift from overlay state.
- Deterministic simulation tests using only the stub backends to prove seeded replay of open/reopen/close and metadata update flows.
- Crash/recovery tests around session metadata publish and provenance updates.

---

### T40c. Implement the core sandbox actor, VFS-backed runtime shims, module loading, and injected capability plumbing

**Depends on:** T40a, T40b, T36, T37, T38, T39

**Description**

Implement the execution core of the sandbox: the actor model, the runtime backend integration, the VFS-backed filesystem op layer, guest module loading, and the host-capability injection path. This task establishes the main execution vertical slice for guest code over a sandbox session.

**Implementation steps**

1. Implement the sandbox actor/executor model and the real runtime-backend wrapper for guest execution, preserving the thread-affinity or placement constraints required by the chosen runtime.
2. Implement the VFS-backed op layer for file operations and the guest-side filesystem shim library used by runtime-facing compatibility code.
3. Implement the custom module-loader surface:
   - specifier resolution for sandbox files and host capability modules,
   - runtime cache hooks,
   - source-map plumbing,
   - explicit gating for unsupported schemes or compatibility modes.
4. Implement the capability registry and guest-visible injected app modules with matching type/declaration plumbing where appropriate.
5. Ensure capability calls, entrypoint/module execution, and related runtime actions produce tool-run/activity records and preserve session isolation.

**Verification**

- Integration tests that execute guest code against a real sandbox session and verify VFS reads/writes, capability calls, and activity publication.
- Tests proving capability modules are explicit imports rather than ambient globals and that denied capabilities fail predictably.
- Deterministic simulation tests that exercise the same session/module/capability flows through the stub runtime backend and validate state changes, tool runs, and activity ordering.
- Recovery tests for runtime-cache or metadata publication boundaries exposed by the implementation.

---

### T40d. Implement TypeScript services and just-bash integration on the sandbox contracts

**Depends on:** T40a, T40b, T30, T31

**Description**

Implement the TypeScript and shell-tooling layers against the sandbox contracts without reopening the runtime/session interfaces. This task covers TypeScript transpilation/checking, `just-bash` integration over the VFS-backed sandbox filesystem, and the shell-facing bridge for host-owned capabilities and procedures.

**Implementation steps**

1. Implement execution-time TypeScript transpilation support and cache-key plumbing against the sandbox's module-resolution contracts.
2. Implement the TypeScript mirror / language-service layer using `@typescript/vfs` or an equivalent compiler-host approach tied to the sandbox tree rather than the host disk.
3. Implement guest-visible TypeScript service entrypoints for diagnostics, emits, and related operations, and route them through tool-run/activity surfaces.
4. Implement the `just-bash` filesystem adapter and persistent shell-session wrapper on top of the sandbox filesystem shim.
5. Add the built-in custom command bridges needed by the architecture, such as delegating `npm`, `tsc`, or allowlisted host capability calls through stable sandbox services rather than host subprocesses.
6. Implement a shell-facing capability/procedure bridge for `just-bash` that is derived from the same capability or procedure metadata as typed imports rather than hand-maintained separately.
7. Ensure the shell bridge provides stable command naming, built-in help/argument descriptions, structured JSON input/output, and first-class tool-run/activity recording without introducing a second authority model.

**Verification**

- Integration tests for TypeScript transpile/check flows over sandboxed files.
- Integration tests for bash sessions reading and writing through the sandbox tree and producing expected tool-run records.
- Integration tests proving the same host capability can be exercised through both a typed import and the shell-facing bridge with the same manifest enforcement and audit metadata.
- Deterministic simulation tests for TypeScript-mirror updates, diagnostic replay, and bash/VFS flows using the stub runtime backend where the real runtime is not required.
- Restart tests for any persisted TypeScript or shell-session metadata introduced by the implementation.

---

### T40e. Implement package installation, cache/materialization policy, and runtime compatibility modes

**Depends on:** T40a, T40b, T40c

**Description**

Implement version-1 package installation and compatibility support as a host-managed service rather than a literal embedded npm CLI. The goal is a useful pure-JS/TS package subset with stable cache and manifest semantics that can still be simulated deterministically.

**Implementation steps**

1. Implement the package-installer service contract with support for:
   - version resolution,
   - integrity verification,
   - shared package cache,
   - session-local install metadata,
   - optional compatibility-view materialization.
2. Implement the session-local package/install manifest and whichever compatibility view the runtime module loader expects in version 1.
3. Restrict the supported package surface explicitly to the intended version-1 subset and make unsupported package classes fail predictably.
4. Wire package install/use flows into tool-run/activity metadata and any runtime-loader hooks established in T40c.
5. Add a deterministic fake registry/fetch/cache surface for simulation so package graph changes can be replayed without real network access.

**Verification**

- Integration tests that install a small supported package set and import it successfully from guest code.
- Cache and reinstall tests proving repeated installs reuse compatible cached state correctly.
- Deterministic simulation tests using the fake registry/fetch layer to replay package resolution, cache hits/misses, and manifest updates.
- Crash/recovery tests around install-manifest publication and compatibility-view materialization.

---

### T40f. Implement host-disk hoist/eject, git provenance, branch/export, and pull-request flows

**Depends on:** T40a, T40b, T38

**Description**

Implement the real-world interop path between sandboxes and host projects: import a directory or repo into a session, export a snapshot or delta back to disk, and create branches / pull requests from sandbox changes. The default PR path should prepare an isolated export workspace rather than mutating the user's active checkout directly.

**Implementation steps**

1. Implement host-directory and repo hoist modes:
   - directory snapshot,
   - git head,
   - git working tree with explicit untracked/ignored-file policy.
2. Implement eject modes:
   - materialize full snapshot,
   - apply delta relative to recorded provenance,
   - structured conflict reporting and optional patch-bundle fallback.
3. Implement git provenance capture and validation during hoist/eject/export.
4. Implement git-aware export helpers that prepare an isolated export/worktree, apply sandbox changes, create a branch, commit, and push.
5. Implement the PR provider-client integration on top of the stable provider interface instead of baking one provider directly into sandbox core logic.

**Verification**

- Round-trip tests for hoist → sandbox mutate → eject on ordinary directories and git repos.
- Tests covering rename/delete/conflict behavior and provenance validation.
- Deterministic simulation tests for diff/provenance/conflict logic and PR-export planning using stub git and provider backends.
- Integration tests against real git worktrees for the non-simulated production path.

---

### T40g. Implement the read-only local/remote editor-view service, foreground session surfaces, and ship a real VS Code/Cursor extension package

**Depends on:** T40a, T40b, T40c, T40f

**Description**

Implement the read-only editor-facing view layer so developers can browse sandbox state from VS Code and Cursor whether the application is running locally or remotely, and add a live foreground-session surface so the same tooling can show what an active sandbox is doing right now. This task must produce a real extension package in the repo, not just an abstract provider surface or protocol sketch. The goal is one user-facing editor workflow backed by the same read-only view and foreground-session protocols across both deployment shapes.

**Implementation steps**

1. Implement the read-only view provider/service defined in T40a for:
   - browse/list/open/stat,
   - visible or durable cut selection,
   - refresh/reconnect,
   - session/volume selection.
2. Implement the local transport/bridge path for an app running on the developer's machine.
3. Implement the authenticated remote transport/endpoint path so the same extension/protocol works across the network against a remote app.
4. Implement and check in a real VS Code extension package in the repo, preferably using the editor's read-only virtual filesystem/provider APIs, and make Cursor compatibility an explicit requirement of that package or its build target rather than an assumed follow-up.
5. Expose a foreground session-state surface over the same local/remote bridge for:
   - running or recently failed tool actions,
   - active readonly-view handles and reconnect status,
   - host-enforced denials or budget/resource failures that actually reached the enforcement path,
   - clear machine-readable indication when a capability or shell bridge is simply unavailable because it was never injected.
6. Ensure the editor/view path remains read-only and that the foreground session surface is derived from or mirrored into the same durable activity/tool-run model rather than a disconnected telemetry plane.

**Verification**

- Unit and integration tests for browse/list/open/stat semantics over sandbox snapshots or session views.
- Deterministic simulation tests for the core view protocol and snapshot/view semantics using the stub view provider or local protocol stub.
- Local integration tests for the checked-in extension package against a local app bridge, including foreground session-state queries.
- Remote integration tests for the same checked-in extension package against an authenticated remote endpoint and reconnect/status behavior.

---

### T40h. Build cross-cutting deterministic compatibility, crash, and randomized fault suites for the sandbox stack

**Depends on:** T33, T40c, T40d, T40e, T40f, T40g

**Description**

Bring the sandbox stack up to the same correctness bar as the rest of Terracedb by extending the deterministic simulation framework to the maximum semantically important subset of sandbox behavior and by verifying the non-simulated seams through the deterministic stub contracts.

**Implementation steps**

1. Extend the sandbox shadow model to cover:
   - session filesystem state,
   - capability-visible state,
   - TypeScript mirror state,
   - package/install manifest state,
   - hoist/eject provenance and delta planning,
   - read-only view snapshot semantics,
   - tool-run/activity ordering.
2. Add randomized workloads that combine:
   - guest execution,
   - filesystem mutations,
   - capability calls,
   - TypeScript checks,
   - bash commands,
   - package installs,
   - hoist/eject/export operations,
   - editor-view reads.
3. Add crash cut points around:
   - session creation,
   - cache publication,
   - install metadata updates,
   - hoist/eject commits,
   - PR-export bookkeeping,
   - read-only view handle state if the implementation persists it.
4. Add conformance tests comparing the deterministic stub contracts with the observable behavior of the real integrations wherever that comparison is practical.
5. Ensure every failing seed captures enough session/provenance/cache/view metadata to replay the issue exactly.

**Verification**

- Large-seed simulation campaigns checking both current-state and cross-cutting prefix/order invariants after each step or recovery point.
- Same-seed replay tests proving identical traces and sandbox outcomes.
- Conformance tests for stub-versus-real backend behavior on the seams that must remain outside the harness.
- Recovered-state and export/view consistency tests across standalone and repo-backed sessions.

---

### T40i. Add a simple example project that demonstrates sandbox execution, injected app capabilities, editor visibility, and repo workflows

**Depends on:** T40c, T40d, T40e, T40f, T40g, T40h

**Description**

Add a small, teachable example project that demonstrates what the sandbox is for in practice: a real sandbox session over a project tree, injected app capabilities, guest code execution, shell/tool usage, read-only editor visibility, foreground session state, and an export or PR-style workflow.

**Implementation steps**

1. Extend `examples/sandbox-notes` as the canonical sandbox example unless the scope stops being teachable; prefer evolving that existing example over creating a near-duplicate app.
2. Implement at least one injected app capability that is visible to guest code and easy to understand in the example, such as:
   - a note store,
   - a ticket/comment API,
   - a task queue or inbox surface.
3. Demonstrate the core sandbox flows in the example:
   - open a session on a base tree or hoisted repo,
   - execute guest code through an explicit typed capability import,
   - call the same capability through the shell-facing bridge,
   - use TypeScript and/or bash tooling,
   - inspect live foreground session state for tool runs, view handles, and other active work,
   - view files read-only from the VS Code/Cursor integration,
   - eject or prepare a branch/PR-style export.
4. Add example docs that explain the boundaries clearly:
   - what is simulated versus what is a real integration,
   - why the editor view is read-only,
   - how app capabilities are injected,
   - how typed imports and shell bridges share one authority model,
   - the difference between a capability that is absent from the session and an operation that reaches the host and is denied,
   - how repo export and PR creation work.
5. Keep the example small enough to teach, but real enough that it validates the crate boundaries against normal application embedding.

**Verification**

- End-to-end example tests exercising the documented happy path, including both typed-import and shell-bridge capability use.
- Tests proving the injected app capability behaves the same through the example as it does through the sandbox API directly.
- Tests covering the foreground session-state surface the example presents for active tool runs and readonly-view state.
- Local editor-view smoke tests against the example project and, where practical, a remote-view smoke path using the same protocol.
- Deterministic simulation coverage for the example's capability and session semantics wherever the example uses simulated-capable paths.

---

## Phase 8b — Simulation-native JavaScript runtime and VFS-native git convergence

**Phase rule:** T40j freezes the shared `terracedb-js` / `terracedb-git` substrate boundary, host-service contracts, fork policy, and deterministic seams first so the phase can parallelize aggressively without interface churn. `terracedb-js` is the architectural home of guest-observable JavaScript semantics, `terracedb-git` is the architectural home of repository semantics, and both must consume the same VFS, scheduling, cancellation, time, entropy, capability, and host-bridge model. `boa_engine`, `boa_runtime`, and `boa_wintertc` may remain internal implementation details underneath `terracedb-js`, but crates outside `terracedb-js` should consume the migrated runtime surfaces through `terracedb-js` and its host-service boundary rather than depending on or importing Boa directly.

**Simulation rule:** Every implementation task in this phase must extend the deterministic oracle/cut-point/simulation harness in the same change as the production-path change. When a behavior becomes owned by `terracedb-js` or `terracedb-git`, its primary coverage must move from real-runtime, real-filesystem, or threaded-I/O integration tests into simulation-backed tests immediately. Narrow real runtime / host-service / host-filesystem tests may remain only for explicit `JsHostServices` or `GitHostBridge` conformance and end-to-end bridge smoke behavior. T40q is additive cross-cutting hardening, not a substitute for task-local simulation in T40k-T40p.

**Parallelization:** T40j first. T40k and T40n can proceed in parallel once T40j exists. T40l depends on T40k. T40m depends on T40k and T40l. T40o depends on T40l and T40n. T40p depends on T40m and T40o. T40q depends on T33, T40m, T40o, and T40p. T40r depends on T40i, T40p, and T40q.

### T40j. Freeze the shared `terracedb-js` / `terracedb-git` substrate boundary, fork policy, host-service contracts, and deterministic seams

**Depends on:** T03a, T40a, T40f

**Description**

Define the simulation-native sandbox substrate before the invasive runtime and git work branches. This task exists primarily to maximize parallelism: the Boa-derived runtime work, the `gitoxide`-derived git work, the sandbox integration, and the host bridges should all target fixed contracts rather than discovering them mid-implementation.

**Implementation steps**

1. Add new workspace members (for example `crates/terracedb-js` and `crates/terracedb-git`) and define their top-level module boundaries:
   - runtime host / scheduler / time / entropy / module loading,
   - runtime compatibility and host-service adapters,
   - repository store / open,
   - object / ref / index storage,
   - worktree / checkout / status / diff,
   - host import / export / push bridge,
   - shared sandbox-facing adapters,
   - shared error, provenance, and trace types.
2. Freeze the core public contracts from the architecture:
   - `JsRuntimeHost`,
   - `JsRuntime`,
   - `JsEntropySource` or equivalent entropy interface,
   - `GitRepositoryStore`,
   - `GitRepository`,
   - `GitHostBridge`,
   - open / options / policy / request / report types,
   - explicit execution / cancellation / host-service hook traits.
3. Freeze the internal seams needed to parallelize the phase:
   - VFS adapter and repository-image open / discover seam,
   - runtime module-loader seam,
   - runtime scheduler / clock / entropy seam,
   - compatibility-host-service seam,
   - git object / ref / index seam,
   - worktree materialization seam,
   - host-bridge seam,
   - conformance-harness seams for stub-versus-real runtime and bridge behavior.
4. Decide the fork ownership boundary explicitly:
   - which `boa_engine`, `boa_runtime`, or `boa_wintertc` surfaces are directly consumed, wrapped, forked, or forbidden across the `terracedb-js` boundary,
   - which `gix*` crates or slices are vendored for `terracedb-git`,
   - which ambient defaults are forbidden across either boundary, including host cwd, host time, OS entropy, raw stdout/stderr, host tempfiles/lockfiles, correctness-relevant thread spawning, and process-global interrupt/cleanup state.
5. Freeze the incremental simulation plan up front: once a behavior becomes owned by `terracedb-js` or `terracedb-git`, new primary coverage must land in deterministic simulation, while existing real-runtime or real-filesystem coverage is narrowed to explicit bridge conformance or removed if redundant.
6. Add stub/fake implementations so downstream tasks and the simulation harness can open a fake runtime host and fake repository over a fake VFS without host network, host git, host disk, or real threads.

**Verification**

- Compile-only tests that instantiate the new runtime-host, runtime, repository-store, repository, bridge, and execution-hook contracts together.
- Unit tests that round-trip shared request/report, provenance, policy, and trace metadata encodings.
- A deterministic smoke test that opens a fake runtime and fake repo over a fake VFS and exercises basic module / capability / repo calls without touching host I/O or real threads.

---

### T40k. Establish the `terracedb-js` substrate over `boa_engine`, including the runtime-host model and fake backend

**Depends on:** T40j, T40c

**Description**

Bring the Boa-based runtime core into the workspace and replace the first layer of non-negotiable ambient assumptions so guest execution is hosted through TerraceDB-owned seams rather than stock builder defaults. This task owns the runtime substrate that later VFS-native module loading, deterministic time/entropy, and compatibility-layer work build on.

**Implementation steps**

1. Vendor or pin the chosen Boa slice into the workspace according to T40j's boundary and re-export only through `terracedb-js`.
2. Establish the `terracedb-js` runtime-host model around the relevant Boa embedding seams:
   - context / realm ownership,
   - host hooks,
   - scheduler / job executor,
   - clock,
   - module loader,
   - entropy source,
   - structured-clone or cross-context transfer boundary.
3. Replace or forbid the non-negotiable ambient defaults at the runtime-open layer:
   - stock cwd-based module discovery,
   - `StdClock`,
   - stock job executors in the simulation path,
   - direct process RNG for guest-visible randomness,
   - raw stdout/stderr and ambient process/env exposure as default behavior.
4. Implement the real runtime-host wrapper plus a fake backend that can execute simple guest code, module imports, and capability calls without host filesystems, host network, or real threads.
5. Preserve the sandbox actor boundary explicitly so guest execution remains host-owned and same-thread or placement-constrained where required by the runtime implementation.
6. Add deterministic simulation tests for open/evaluate/import flows as they land, and migrate matching stub-runtime coverage from ad hoc sandbox-only seams onto `terracedb-js`.

**Verification**

- Integration tests that construct a `terracedb-js` runtime from the frozen host interfaces and execute simple guest modules and capability imports.
- Deterministic simulation tests for runtime open/evaluate/import flows using the fake backend and fake VFS.
- Tests proving ambient defaults such as stock cwd loading or process RNG are not reachable through the `terracedb-js` public surface.

**Current backend split**

- `DeterministicJsRuntimeHost` remains the default simulation/oracle backend when loaders or host seams may need real async progress.
- `BoaJsRuntimeHost` is the real `boa_engine` execution backend and takes explicit Boa-owned scheduler, host-service, and module-loader seams.
- `ImmediateBoaModuleLoader` is the bridging adapter from `JsModuleLoader` to the Boa loader boundary; it is only valid when the wrapped loader future completes without yielding.

---

### T40l. Implement simulation-native scheduling, time, entropy, and VFS-native module loading for `terracedb-js`

**Depends on:** T40j, T40k, T36, T38

**Description**

Implement the deterministic core of the JavaScript runtime: VFS-native module loading, host-owned scheduling, injected time, and injected entropy. This task is where guest execution stops depending on ambient host cwd, wall-clock timers, and process randomness for its semantically important behavior. For the module-loading and execution flows covered here, `terracedb-js` becomes the semantic owner even if it continues to use Boa internally, and downstream crates such as `terracedb-sandbox` should stop importing or depending on Boa directly for those paths.

**Implementation steps**

1. Extend the runtime shadow model and deterministic harness first to represent module resolution/loading, timer queues, microtask scheduling, host time, host entropy, and guest-visible randomness before the production path changes.
2. Implement the VFS-native module loader for the sandbox specifier spaces, including explicit roots for `terrace:/workspace`, `terrace:host`, and package-backed modules.
3. Implement the host-owned scheduler / job-executor path so promise jobs, timers, microtasks, and module-loading continuations can run to quiescence under deterministic simulation without wall-clock sleeps.
4. Route guest-visible time and entropy through injected services:
   - `Date`,
   - `Temporal.Now`,
   - timer deadlines,
   - console timing surfaces,
   - `Math.random`,
   - future `crypto.getRandomValues` / `crypto.randomUUID` or equivalent compatibility surfaces.
5. Remove, gate, or explicitly isolate blocking/runtime-owned behaviors such as hidden host sleeps, blocking waits, or other APIs that cannot participate honestly in the simulated path.
6. Migrate the sandbox's core module-loading and execution flows onto `terracedb-js` rather than the earlier ad hoc runtime wrapper. This migration explicitly includes removing direct `boa_engine`, `boa_runtime`, and `boa_wintertc` coupling from `terracedb-sandbox` for those flows, while still allowing `terracedb-js` to keep Boa as its internal execution backend, and replacing real-runtime or threaded-I/O tests whose semantics now live inside `terracedb-js`.

**Verification**

- Tests covering module resolution and loading over VFS-backed snapshots, overlays, capability modules, and package-backed modules.
- Deterministic simulation tests for timer ordering, microtask scheduling, time advancement, and seeded entropy behavior across restart and replay.
- Tests proving `Math.random` or equivalent guest-visible randomness is deterministic under seeded replay and does not call process RNG directly.
- Compile/package-level checks showing the migrated sandbox runtime path no longer depends on Boa directly, and that direct `boa_engine`, `boa_runtime`, or `boa_wintertc` imports/dependencies outside `terracedb-js` are removed for the module-loading and execution surfaces covered by this task.
- Coverage or conformance notes showing `terracedb-js` remains free to use Boa internally as its backend while still owning the guest-observable semantics for the migrated path.

---

### T40m. Implement runtime compatibility layers, host-service adapters, and sandbox migration onto `terracedb-js`

**Depends on:** T40j, T40k, T40l, T40d, T40e

**Description**

Build the host-API layer above the simulation-native runtime core. This task covers console/process/fetch/timer/crypto-style compatibility, the production and fake host-service adapters behind those APIs, and the migration of sandbox TypeScript, package-install, shell, and runtime compatibility flows so they run through `terracedb-js` and its host-service boundary rather than a parallel Boa-based integration path in `terracedb-sandbox`.

**Implementation steps**

1. Define the `terracedb-js-host-services` boundary for the compatibility surfaces that remain host-mediated, such as:
   - console sinks,
   - process/env projections,
   - fetch / registry access,
   - runtime-exposed timers or messaging helpers,
   - future crypto compatibility surfaces.
2. Wrap, fork, or replace the useful pieces of `boa_runtime` and `boa_wintertc` so those surfaces consume the frozen `terracedb-js` host-service traits instead of ambient host behavior.
3. Ensure manifest and deployment policy, rather than ambient process state, determine which compatibility services are visible to one sandbox session.
4. Add deterministic fake host-service implementations for registry fetches, network-like calls, console sinks, and other side effects so sandbox package/runtime behavior can be replayed without real host access.
5. Migrate sandbox TypeScript, package-install, shell-bridge, capability-invocation, and other runtime compatibility / host-service flows onto the new `terracedb-js` runtime plus `terracedb-js-host-services` boundary, removing parallel Boa-based paths from `terracedb-sandbox` for the surfaces this task covers.
6. Replace or demote existing tests that exercise these semantics only through real runtime helpers, real network calls, or threaded I/O; the new primary coverage should be deterministic simulation against `terracedb-js` plus fake host services.

**Verification**

- Integration tests for console/process/fetch/package/shell flows through the `terracedb-js` host-service boundary.
- Deterministic simulation tests for fake-registry, fake-network, and fake-console behavior as observed through guest code.
- Tests proving capability and host-service visibility follow manifest/policy decisions rather than ambient process state.
- Compile/package-level checks showing `terracedb-sandbox` no longer depends on Boa directly for the migrated compatibility and host-service runtime path, and that direct `boa_engine`, `boa_runtime`, or `boa_wintertc` imports/dependencies outside `terracedb-js` are removed for the TypeScript/package/shell/runtime surfaces covered by this task.
- Coverage or conformance notes showing `terracedb-js` may still use Boa internally as its backend while the public ownership boundary for those guest-visible compatibility semantics stays at `terracedb-js` and `terracedb-js-host-services`.

---

### T40n. Establish the forked git substrate, shared host-service alignment, and repository open/discovery path

**Depends on:** T40j, T36, T38

**Description**

Bring the forked `gitoxide` slice into the workspace and replace the first layer of non-negotiable host assumptions so repositories can be opened over `terracedb-vfs` snapshots, overlays, and imported repository images. This task owns the git substrate that later status/diff/checkout and sandbox convergence work build on, and aligns its execution/cancellation model with the shared substrate frozen in T40j.

**Implementation steps**

1. Vendor the chosen `gitoxide` slice into the workspace according to T40j's boundary and re-export only through `terracedb-git`.
2. Replace or wrap the non-negotiable host assumptions at the repository-open layer:
   - ambient cwd discovery,
   - host-path-only repository open,
   - process-global interrupt / cleanup,
   - uncontrolled tempfiles / lockfiles,
   - correctness-relevant thread spawning.
3. Implement the `terracedb-vfs` adapter and explicit repository-image / open flow for snapshots, overlays, and imported repo images.
4. Align repository mutation and cancellation with the shared substrate's execution hooks so deterministic simulation and the sandbox actor model can own scheduling explicitly.
5. Implement read-only repository open/discover, `HEAD` / ref enumeration, and object access over VFS-backed repos.
6. Add deterministic simulation tests for open/discover/ref/object behavior as they land, and migrate matching host-filesystem or threaded-I/O coverage to the new simulation path whenever the semantics now live inside `terracedb-git`.

**Verification**

- Open/discover tests covering explicit-root repository lookup, `HEAD` resolution, and ref enumeration over VFS-backed repos.
- Deterministic simulation tests for repository open/discover across snapshots, overlays, and imported repo images.
- Narrow conformance tests proving the remaining real-host open/import path still agrees with the frozen bridge contract where it crosses the host boundary.

---

### T40o. Implement VFS-native repository semantics and repo-backed sandbox convergence

**Depends on:** T40l, T40n, T40b, T40f

**Description**

Implement the read-mostly and mutable repository semantics used by sandbox authoring and export planning: index refresh, status, diff, pathspec handling, dirty detection, provenance planning, checkout, branch/ref movement, index/worktree updates, and snapshot-plus-overlay worktree flows. This task is where repo-backed sandbox behavior stops depending on host git subprocesses or sibling host worktrees for core semantics.

**Implementation steps**

1. Extend the git shadow model and deterministic harness first to represent index refresh, tracked/untracked/ignored state, dirty detection, pathspec filtering, provenance planning, checkout/materialization, branch movement, index/worktree consistency, copy-up, and conflict reporting before the production path changes.
2. Implement VFS-native index load / refresh / write helpers using VFS inode/stat information instead of `std::fs` metadata.
3. Implement status, diff, pathspec filtering, dirty-state reporting, checkout, branch/ref updates, and tree materialization over VFS snapshots and overlays.
4. Implement snapshot-plus-overlay worktree flows so repo-backed sandbox sessions can branch, reset to base, and materialize alternate trees without host linked-worktree directories.
5. Move sandbox provenance validation, delta planning, repo-backed inspection, and repo-backed mutation flows onto `terracedb-git`, exposing them through the `terracedb-js` capability/runtime boundary rather than host git subprocesses.
6. Replace or demote existing tests that exercise these semantics only through real filesystem writes or threaded I/O; the new primary coverage should be deterministic simulation against the VFS-native code path, with only narrow bridge-conformance tests remaining on real git.

**Verification**

- Tests covering tracked, untracked, ignored, renamed, deleted, pathspec-filtered status/diff behavior, checkout into overlays, branch/ref updates, and conflict reporting over VFS-backed repos.
- Deterministic simulation tests for dirty detection, provenance validation, checkout/reset flows, branch movement, and restart/recovery after partial repo mutation.
- Tests proving repo-backed sandbox sessions can inspect and materialize trees through the `terracedb-js`-exposed capability path without creating sibling host worktrees.

---

### T40p. Implement explicit host-service and host-bridge boundaries for runtime and git

**Depends on:** T40m, T40o, T40f

**Description**

Narrow the remaining host-backed behavior to the explicit boundary where it belongs. This task owns the production adapters for runtime host services and the git host bridge: registry/network/process/console side effects on the runtime side, and host-repo import/export/push/PR flows on the git side. The semantic owners remain `terracedb-js` and `terracedb-git`; the host adapters own only boundary crossings.

**Implementation steps**

1. Implement the production `terracedb-js-host-services` adapters for the host-mediated runtime surfaces the phase keeps, plus matching deterministic stub adapters for simulation.
2. Implement host-repo import into a VFS-native repository image while preserving the provenance captured by the sandbox/session layer.
3. Implement export/materialize flows from a VFS-native repo or branch plan to a real checkout or ephemeral worktree, plus push and PR-provider helpers through the narrow git bridge.
4. Refactor sandbox hoist/eject/branch/PR/runtime host-API flows so `terracedb-js` owns guest-visible runtime semantics, `terracedb-git` owns repository semantics, and the host adapters own only host materialization, transport, or side-effecting provider calls.
5. Add deterministic stub-bridge tests and simulation coverage for import/export/push planning, diverged-base conflicts, runtime host-service failures, and bridge error reporting as the migration lands; keep only the smallest set of real runtime / real git integration tests needed to prove bridge conformance.
6. Preserve explicit fail-closed reporting for diverged provenance, export conflicts, remote push failures, provider-side PR errors, denied runtime side effects, and unavailable-versus-denied host services.

**Verification**

- Round-trip tests for host import → sandbox/VFS-native repo mutate → host export on ordinary repositories.
- Integration tests for the production runtime host-service adapters where real host behavior remains intentionally exposed.
- Deterministic simulation tests for runtime host-service failures, bridge planning, diverged-base handling, and push/PR error propagation using stub host adapters.
- Narrow real runtime / real git integration tests proving the host adapters can still materialize exports, create branches, and hand off to provider adapters correctly.

---

### T40q. Build cross-cutting deterministic simulation and conformance suites for the simulation-native sandbox substrate

**Depends on:** T33, T40m, T40o, T40p

**Description**

Bring the simulation-native sandbox substrate up to the same bar as the rest of Terracedb by adding whole-system deterministic simulation, crash/fault campaigns, and host-adapter conformance tests across the combined guest-runtime and repo-backed sandbox flow. This task is explicitly the cross-cutting hardening pass for the phase.

**Implementation steps**

1. Compose the per-task runtime and git oracle helpers into a whole-system harness that can run sandbox sessions through module loading, guest execution, capability calls, package activity, repo inspection/mutation, export planning, and recovery.
2. Add long-running deterministic campaigns combining:
   - guest execution,
   - filesystem mutations,
   - timer and microtask behavior,
   - seeded entropy and guest-visible randomness,
   - package/cache changes,
   - repo import/mutate/status/diff/checkout/export flows,
   - diverged-base conflicts,
   - restart/recovery.
3. Add crash cut points around runtime open, module/cache publication, install metadata updates, repo open, index write, checkout publication, bridge bookkeeping, and PR planning/publication boundaries.
4. Add conformance tests comparing stub host-service and host-bridge behavior with the real adapters and, where practical, targeted behavioral comparisons between the simulation-native core and the remaining non-simulated seams.
5. Ensure failing seeds capture session metadata, repository image/provenance, simulated fault schedule, and enough trace data to replay the failure exactly.

**Verification**

- Large-seed deterministic simulation campaigns proving combined runtime-plus-git sandbox flows remain reproducible across restart, conflict, and export scenarios.
- Same-seed replay tests proving identical traces and sandbox outcomes for runtime scheduling, entropy, repository state, and host-bridge planning.
- Stub-versus-real conformance tests for both runtime host services and the git host bridge.

---

### T40r. Update the canonical sandbox example to demonstrate simulation-native JS and VFS-native git-backed authoring flows

**Depends on:** T40i, T40p, T40q

**Description**

Update the canonical sandbox example so it demonstrates the simulation-native end state rather than only the earlier host-interop flows. This task should evolve `examples/sandbox-notes` where practical so the teaching artifact remains one coherent sandbox-plus-git example rather than splitting into near-duplicates.

**Implementation steps**

1. Extend `examples/sandbox-notes` as the canonical sandbox example unless the scope stops being teachable; prefer evolving that example over creating a near-duplicate.
2. Add a repo-backed sandbox path that demonstrates:
   - hoist/import of a repo into a sandbox session,
   - editing files in the sandbox,
   - executing guest code through the `terracedb-js` runtime,
   - inspecting git status/diff/branch information inside the sandbox,
   - exporting/applying the delta or preparing a branch/PR through the host bridge.
3. Keep at least one typed-capability flow and one shell-bridge flow from T40i so the example still teaches sandbox authoring rather than becoming only a git demo.
4. Document clearly which runtime and git behaviors are simulation-native and deterministic, and which steps still cross the explicit host-service or host-bridge boundaries for materialization, push, PR-provider calls, registry access, or similar side effects.
5. Add example-owned deterministic simulation tests for the repo-backed authoring flows as the example changes, and keep only narrow real-host smoke tests for the remaining host-materialization paths.

**Verification**

- End-to-end deterministic simulation tests for the example's JS-plus-git authoring workflow, including guest execution, status/diff inspection, and export/PR planning.
- Example integration tests proving the documented sandbox-plus-git flow works through the public example surface without hidden setup.
- Narrow real-host smoke tests for host materialization, PR-style export behavior, and any example-hosted runtime side effects the documentation intentionally exposes.

---

## Phase 9 — `terracedb-bricks` blob and large-object library

**Parallelization:** T41 first. After that, T42 and T43 can begin in parallel against the frozen crate boundary from T41. T44 depends on T43, T30, and T31. T45 depends on T42 and T43. T46 depends on T33, T44, and T45.

### T41. Freeze the `terracedb-bricks` crate boundary, publish semantics, and reserved tables

**Depends on:** T01

**Description**

Define the `terracedb-bricks` public surface and the storage/indexing contracts before implementation work branches. This task freezes the semantic target: large bytes stay out-of-line in a blob store, while Terracedb stores metadata, references, lifecycle activity, and derived search indexes. The point is to keep blob support a library concern rather than turning it into an engine-level `Value::Blob` feature.

**Implementation steps**

1. Define the Rust equivalents of the architecture's blob-library contracts: `BlobCollection`, `BlobId`, `BlobWrite`, `BlobHandle`, `BlobMetadata`, query/search types, and activity types.
2. Freeze the reserved table/key contracts for current metadata, aliases, lifecycle activity, and projection-owned derived index tables.
3. Define the blob-store abstraction as a separate library-edge trait with streaming upload and range-read support, plus a compatibility adapter path for the engine's smaller whole-buffer object-store implementations.
4. Define the upload-before-publish, delete-before-GC, durable-indexing, and fail-closed missing-object semantics explicitly.
5. Add stub/fake implementations so downstream tasks can compile against the crate boundary before the real blob logic exists.

**Verification**

- Compile-only API tests that instantiate configs, fake blob stores, and the public blob-library surface.
- Unit tests that round-trip blob IDs, aliases, and table-key encodings.
- Contract tests proving the frozen object-prefix and reserved-table names are stable and disjoint from Terracedb's backup/cold-storage prefixes.

---

### T42. Implement blob-store trait, adapters, and out-of-line I/O substrate

**Depends on:** T22, T23, T41

**Description**

Implement the library edge for large-object bytes themselves: the `BlobStore` trait, test fakes, compatibility adapters over existing object-store implementations, and the low-level upload/read/delete helpers that the higher-level catalog task will call. This task should own the byte-transport side of `terracedb-bricks` so the metadata/catalog task can proceed in parallel against the frozen trait boundary.

**Implementation steps**

1. Create the blob-store crate/module surface and implement the production `BlobStore` trait, test fakes, and compatibility adapters over the engine's existing object-store implementations where reasonable.
2. Implement low-level streaming upload helpers, range-read helpers, stat/delete helpers, and typed error mapping for object-store failures.
3. Implement upload-time digest and size accounting, plus stable object-key policies such as content-addressed naming or another frozen naming scheme.
4. Ensure `terracedb-bricks` object prefixes and failure semantics stay separate from the engine's own tiered/backup/offload object layout even when they reuse the same physical backend.
5. Add cut points and retry/recovery hooks for stale list results, upload success with lost response, timeout/partition failures, and interrupted reads.

**Verification**

- Trait contract tests for fake and real adapters covering put/get/stat/delete and range-read behavior.
- Tests proving large objects can be fetched incrementally without forcing the DB to inline them.
- Fault-injection tests for timeout, lost-response, and interrupted-read behavior at the blob-store edge.
- Prefix/layout tests proving bricks objects never collide with the engine's backup/cold-storage namespaces.

---

### T43. Implement metadata catalog, publication, aliases, reads, deletes, and lifecycle activity

**Depends on:** T41

**Description**

Implement the Terracedb-resident current-state side of `terracedb-bricks`: metadata rows, alias/upsert semantics, durable lifecycle activity, publish ordering, and metadata-first reads/deletes. This task should proceed against the frozen `BlobStore` trait plus test fakes from T41 rather than waiting for the production adapters in T42.

**Implementation steps**

1. Create the reserved tables and key encodings for `blob_catalog`, `blob_alias`, `blob_activity`, and any current-state GC helper rows.
2. Implement the metadata side of `put`, `stat`, `get`, `delete`, and alias-resolution helpers on top of ordinary Terracedb tables and the frozen `BlobStore` trait.
3. Implement upload-before-publish ordering so current metadata and lifecycle activity become visible only after the backing object exists.
4. Implement metadata-first reads and fail-closed behavior for missing or corrupt referenced objects.
5. Ensure delete and alias-replacement flows update current metadata and append lifecycle activity in one Terracedb batch, while deferring physical object reclamation to the GC task.

**Verification**

- Read/write/delete tests for both ID-based and alias-based access using fake blob-store implementations.
- Tests showing current metadata rows and corresponding activity rows never become visible independently.
- Crash tests at “upload complete but metadata not yet published”, “metadata published but client not yet acknowledged”, and “delete metadata visible before object GC”.
- Restart tests proving bricks metadata reopens cleanly and orphan uploads remain harmless.

---

### T44. Implement durable indexing, extraction, and search helpers for bricks

**Depends on:** T30, T31, T43

**Description**

Build the queryability story above the bricks catalog. Search should come from metadata rows and derived index tables maintained with the projection runtime, not from pretending raw object bytes are a first-class scan surface inside the engine.

**Implementation steps**

1. Implement durable metadata indexes for at least alias/name, content type, tags, size ranges, and timestamps using ordinary Terracedb tables.
2. Implement an optional extraction pipeline that can fetch blob bytes or ranges and write derived rows such as text chunks, previews/snippets, normalized term indexes, or other application-facing searchable summaries.
3. Expose query helpers for metadata search and opt-in extracted-text search, keeping all index state in ordinary Terracedb tables rather than in a special engine subsystem.
4. Use the durable projection machinery so index output and cursor advancement remain atomic, and rebuild indexes from the current catalog plus blob-store reads after `SnapshotTooOld`, extractor changes, or catalog replay.
5. Make unsupported extractor/search cases fail closed or remain explicitly opt-in rather than silently returning partial results.

**Verification**

- Metadata-search tests proving filters and ordering behave deterministically across restart and replay.
- Cursor/output atomicity tests for blob indexers using the projection runtime.
- Rebuild tests showing the same blob catalog plus blob-store contents produce the same derived index state.
- Crash tests during extraction/index writes, including replay after partial indexing progress.

---

### T45. Implement object GC and retention safety for bricks

**Depends on:** T42, T43

**Description**

Implement safe external-object cleanup for `terracedb-bricks`. This task owns the exact semantics for orphan uploads, alias replacement, dedup/shared-object cases, and retained-history safety, but it should remain independent of the richer indexing/search task so GC work can proceed in parallel with indexing.

**Implementation steps**

1. Implement garbage collection that determines object reachability from blob metadata plus retained-history rules and deletes only objects that are provably unreferenced and past the grace horizon.
2. Handle alias replacement, delete/recreate flows, and optional deduplicated/shared-object cases without deleting still-referenced objects.
3. Implement orphan-object handling and sweep logic for uploads that completed before metadata publication.
4. Add fault handling for stale LIST results, upload success with lost response, interrupted GC, and delete retries.
5. Ensure GC bookkeeping stays consistent with the frozen reserved-table contracts and object-key prefixes from earlier tasks.

**Verification**

- Tests proving orphan uploaded objects are harmless before metadata publication and are eventually reclaimed safely.
- Retention tests proving GC does not delete objects that may still be referenced by live metadata or retained historical versions.
- Alias-replacement and dedup/shared-object tests proving still-live objects are never deleted prematurely.
- Fault-injection tests for stale LIST, lost-response, and interrupted-sweep behavior.

---

### T46. Build deterministic crash, fault, and randomized suites for `terracedb-bricks`

**Depends on:** T33, T44, T45

**Description**

Bring the bricks library up to the same correctness bar as the rest of Terracedb by extending the deterministic simulation framework to the real publish/read/delete/index/GC implementation.

**Implementation steps**

1. Extend the deterministic simulation/shadow-model machinery to cover blob metadata rows, alias resolution, object bytes, derived index state, and safe cleanup behavior.
2. Add crash cut points around upload-before-publish, metadata publication, delete-before-GC, extraction/index writes, and object reclamation.
3. Add randomized workloads covering publishes, alias replacements, range reads, metadata searches, extraction/index runs, deletes, orphan cleanup, and GC.
4. Add randomized fault schedules covering object-store timeouts/partitions, stale LIST behavior, successful uploads with lost responses, interrupted reads, and interrupted GC.
5. Ensure every failing seed captures the workload, object-store fault schedule, trace, and enough blob metadata to replay the failure exactly.

**Verification**

- Same-seed replay tests proving blob publish/index/GC traces and outcomes are reproducible.
- Large-seed simulation campaigns where metadata state, object state, and derived search state are checked after each step or recovery point.
- Recovered-state prefix tests proving visible/durable metadata rules remain intact across crash and restart.
- End-to-end tests where publish, search, delete, and GC continue to behave correctly across crash/restart and object-store faults.

---

## Phase 10 — Arrow-ecosystem analytical export crate

**Parallelization:** T47 can begin once the durable projection runtime and blob-store substrate exist. Workflow adapters for scheduling/retention are optional and should be added inside the task only as thin integrations rather than as a core dependency of the crate.

### T47. Build an Arrow-ecosystem analytical export crate for snapshots and incremental feeds

**Depends on:** T31, T42

**Description**

Build a separate add-on crate that turns Terracedb state into analytics-friendly derived artifacts rather than changing the authoritative backup format. The crate should expose Arrow-native in-process batches and persisted Parquet-or-Arrow export files under a dedicated export prefix, using durable projections for incremental materialization and the blob-store substrate for large output files. The core design rule is that these exports are disposable, tooling-friendly views over Terracedb data, not the recovery source of truth; native Terracedb backup artifacts remain authoritative for disaster recovery.

**Implementation steps**

1. Freeze the crate boundary (for example `terracedb-analytics-export`): export job/config types, snapshot-export APIs, incremental-export APIs, sink abstractions built on the blob-store substrate, and stable reserved tables for export cursors/manifests/retention metadata.
2. Implement point-in-time snapshot export from row and columnar tables into Arrow `RecordBatch`es and persisted Parquet-or-Arrow files, including schema mapping, partitioning/layout policy, and explicit handling of MVCC cuts.
3. Implement durable incremental export using the projection runtime so exported files/manifests and cursor advancement are published with explicit upload-before-publish ordering and replay-safe idempotency.
4. Keep export object layout separate from Terracedb backup/cold-storage prefixes and from general-purpose blob-library object prefixes unless explicitly configured to reuse the same physical backend with disjoint namespaces.
5. Handle schema evolution and unsupported source semantics explicitly: support straightforward additive/renaming-safe export cases where possible, and fail closed rather than silently producing misleading external analytics files when a source table's semantics cannot be represented faithfully.
6. Add optional workflow helpers for scheduled snapshot exports, retention windows, or compaction/cleanup of derived export files, but keep those as adapters/examples so the base crate remains usable with `terracedb` + projections alone.

**Verification**

- Snapshot-export tests proving exported Arrow/Parquet data matches snapshot-consistent Terracedb reads for both row and columnar tables.
- Interoperability tests proving standard Arrow-ecosystem readers can consume the emitted files and recover the expected schema and values.
- Incremental-export tests proving projection cursor advancement and published export metadata/files remain replay-safe across crash/restart and do not require exports to be the authoritative recovery format.
- Schema-evolution tests covering additive column changes, renamed-field mappings where supported, and explicit fail-closed cases for unsupported history/merge semantics.
- Prefix/layout tests proving analytical export objects are disjoint from Terracedb backup/cold-storage paths and from other blob-library namespaces by default.

---

## Phase 11 — Columnar v2, hybrid read path, and performance hardening

**Parallelization:** T48 first. T49 follows T48. After that, T50, T51, T53, T55, and T56 can proceed in parallel. T50a depends on T50. T52 depends on T50 + T51. T54 depends on T51 + T52 + T55. T57 depends on T50 + T50a + T51 + T52 + T53 + T55 + T56. T58 depends on T52 + T53 + T54 + T55 + T56 + T57.

**Phase rule:** Every task in this phase must add the related deterministic oracle extensions, cut points, and simulation coverage at the same time as the production-path change. Do not defer crash/recovery, pruning-correctness, or cache-state coverage to a later hardening-only step.

**Adoption rule:** Treat T48-T53 and T55 as the universal foundation for the core engine, with T56 shipping only conservative bounded defaults. Treat T54 and T57, plus any aggressive settings introduced by T56, as incremental opt-in accelerants that must remain explicitly configurable and default-off until workload evidence justifies broader adoption.

### T48. Freeze columnar-v2, scan-engine, cache, and repair contracts

**Depends on:** T23b, T27

**Description**

Freeze the interfaces that will let the hybrid-read work fan out safely. The goal of this task is to formalize the storage, execution, cache, and recovery seams before implementation branches diverge.

**Implementation steps**

1. Define a versioned columnar-v2 physical model for:
   - typed substreams,
   - per-granule/page marks,
   - granule-level synopsis sidecars,
   - per-part checksums/digests, and
   - optional sidecar artifacts such as skip indexes and projection-sidecar metadata.
2. Freeze internal interfaces for:
   - columnar footer/page-directory loading,
   - row-ref batch iteration,
   - selection-mask / survivor-set propagation,
   - raw-byte remote segment cache lookup/fill,
   - verify/quarantine/repair entry points, and
   - compact-to-wide promotion policy.
3. Decide crate ownership up front:
   - core `terracedb` owns physical layout, scan execution, caching, checksums, and repair,
   - `terracedb-projections` may consume planner-visible sidecar/projection hooks but does not own the physical SSTable format, and
   - `terracedb-simulation` owns reusable workload/oracle harness helpers for this phase.
4. Reserve explicit format/version tags and compatibility rules for:
   - columnar-v2 base parts,
   - synopsis sidecars,
   - projection sidecars, and
   - compact checksum digests embedded in manifests or other publish records.
5. Define configuration/defaulting rules up front:
   - bounded caches are required,
   - zone maps are part of the base format,
   - richer skip indexes and projection sidecars are optional,
   - any aggressive background behavior must be tunable and default-off.
6. Add compile-time stubs and internal placeholder types so downstream tasks can build against stable seams before the real logic exists.

**Verification**

- Compile-only tests that exercise all new internal interfaces and prove the core/projections/simulation crate boundaries compile without circular dependencies.
- Round-trip tests for the version-tagged footer/header/checksum metadata shapes introduced by this phase.
- Deterministic smoke tests proving the new scan/cache/repair stubs can be instantiated through injected dependencies without touching real I/O.

---

### T49. Extend deterministic simulation and oracle scaffolding for hybrid-read work

**Depends on:** T33, T33c, T48

**Description**

Build the reusable simulation substrate for this phase before the storage and execution work lands. The point is to make later tasks add production behavior against already-existing oracle and cut-point hooks rather than bolting simulation on at the end.

**Implementation steps**

1. Extend the simulation framework with a reusable reference model for:
   - visible rows by key and by scan order,
   - projected-column reads versus full-row reads,
   - survivor-bitmask filtering semantics, and
   - sidecar fallback semantics when skip indexes or projection sidecars are absent or corrupt.
2. Add crash/restart cut points for:
   - v2 SSTable write before footer publish,
   - per-part checksum/digest publish,
   - remote-cache segment admission and background completion,
   - sidecar publish before and after base-part visibility,
   - verify/quarantine/repair transitions, and
   - compact-to-wide promotion / replacement.
3. Add a simulation-friendly cache reference model that can reason about:
   - exact misses,
   - overlapping range requests,
   - downloader election,
   - partial population, and
   - cache restart/index rebuild behavior.
4. Add reusable workload generators for:
   - mixed point-read, short-range, projection-heavy, and remote-scan traffic,
   - low-memory / low-cache-budget configurations,
   - feature-disabled runs where optional accelerants remain off, and
   - feature-enabled runs where optional accelerants are selectively turned on.
5. Expose these helpers from the simulation crate in a way that later core/projection tasks can extend without reaching into engine-private test modules.

**Verification**

- Reproducibility tests proving the new hybrid-read workload and fault schedules are seed-stable.
- Oracle smoke tests proving the reference model agrees with the current v1 row/columnar behavior before any new production logic is introduced.
- Crash/restart harness tests proving each new cut point can be triggered and replayed without leaving the harness itself in an inconsistent state.
- Feature-toggle tests proving optional accelerants can remain disabled without changing baseline semantics or requiring extra runtime state.

---

### T50. Implement columnar-v2 typed substreams and codec pipeline

**Depends on:** T48, T49

**Description**

Replace JSON-backed column blocks with typed binary substreams while preserving the existing logical schema/default semantics. This task should establish the base physical format that later mark/granule and selective-read work will build on.

**Implementation steps**

1. Extend the T49 oracle/harness first to understand binary substream encoding and versioned v1/v2 read expectations.
2. Introduce typed physical substreams for:
   - fixed-width numeric/bool values,
   - null/present bitmaps,
   - offset-plus-bytes streams for variable-width values, and
   - codec descriptors per stream.
3. Implement the initial codec set in core:
   - `None`,
   - `LZ4`,
   - `ZSTD`,
   while leaving room for later delta/dictionary composition.
4. Preserve existing logical default/nullability semantics at read time so schema evolution continues to fill missing fields correctly.
5. Keep v1 read support explicit and fail-closed on unsupported v2 variants rather than silently accepting malformed bytes.

**Verification**

- Round-trip tests for every field type and codec combination introduced by the task.
- Restart tests proving mixed v1/v2 SSTable sets can be reopened and read correctly where compatibility is intended.
- Deterministic simulation tests, introduced in the same change, covering write/crash/restart at codec/footer publish cut points, low-cache-budget runs, and verifying logical reads match the oracle.

---

### T50a. Implement compact decode metadata and lazy schema materialization

**Depends on:** T48, T49, T50

**Description**

Add a compact per-part/per-version decode-metadata layer that lets reopen and hot read paths interpret persisted columnar bytes without eagerly materializing the full schema object. The goal is to keep the hot path cheap while preserving explicit, fail-closed schema compatibility rules.

**Implementation steps**

1. Extend the T49 oracle/harness first to model:
   - compact decode-metadata lookup,
   - lazy full-schema materialization only when required, and
   - fail-closed behavior when compact decode metadata and full schema metadata disagree.
2. Define a compact decode-metadata representation that is distinct from the full schema object but sufficient to interpret persisted columnar-v2 substreams, including field identity/order, nullability/default-fill requirements, and any compatibility/version identifiers needed by the reader.
3. Persist compact decode metadata, or stable identifiers that resolve to it, in the per-part/footer/publish metadata needed for reopen and read-path use without forcing eager full-schema loads.
4. Teach reopen and hot read paths to use compact decode metadata first, materializing the full schema object only when required for higher-level validation, unsupported edge cases, or explicit schema inspection.
5. Fail closed on incompatible or corrupt decode-metadata/schema combinations rather than silently accepting mismatched physical bytes.

**Verification**

- Restart tests proving mixed schema-version SSTable sets can be reopened and decoded through compact decode metadata without requiring eager full-schema materialization on the hot path.
- Read-path tests proving default-filling and nullability semantics remain correct when compact decode metadata is used instead of the full schema object.
- Corruption and incompatibility tests proving decode-metadata/schema mismatches fail closed and surface actionable recovery or repair states.

---

### T51. Implement marks, granules, page directories, and zone-map synopses

**Depends on:** T48, T49

**Description**

Add the sparse page/granule layer that lets the engine prune work without loading full metadata arrays or whole column blocks. This task should establish the first planner-visible synopsis mechanism and the core structure that later skip indexes and late materialization depend on.

**Implementation steps**

1. Extend the T49 oracle first with expected mark/granule pruning behavior and over-read bounds.
2. Implement per-granule/page metadata covering:
   - sampled/first key,
   - row start/count,
   - sequence min/max,
   - tombstone presence, and
   - per-column stream offsets.
3. Replace the current full metadata-array loading path with page-directory reads and narrow row-ref collection for point and range scans.
4. Implement the first synopsis type as base-format zone maps / min-max summaries on configured fields.
5. Add explicit planner interfaces for future optional skip-index and sidecar types without implementing them all here.

**Verification**

- Unit tests proving point reads and bounded scans only inspect the expected granules/pages.
- Pruning tests proving zone maps exclude impossible granules without dropping valid rows.
- Deterministic simulation tests for pruning correctness, crash/restart around granule metadata publish, fail-closed behavior on corrupt page-directory bytes, and low-memory runs that confirm the base mark directory remains usable under tight cache budgets.

---

### T52. Implement row-ref batch scanning, PREWHERE-lite, and late materialization

**Depends on:** T49, T50, T51

**Description**

Add a lightweight internal scan engine that works on row-ref batches and selection masks before materializing projected values. The goal is to make selective scans cheap without importing a heavyweight general-purpose query planner.

**Implementation steps**

1. Extend the T49 oracle/harness first so it can compare:
   - full materialization,
   - projected materialization, and
   - staged predicate-first materialization
   for the same logical scan.
2. Introduce internal batch/header types for row refs, projected fields, and survivor masks.
3. Build a row-ref scan path that streams row refs/granules rather than collecting every matching key up front.
4. Implement PREWHERE-lite:
   - read predicate columns first,
   - derive a survivor bitmap,
   - fetch remaining projected columns only for survivors.
5. Keep merge-operator-heavy keys on a safe fallback path until a batch-friendly merge resolution plan is intentionally introduced.

**Verification**

- Deterministic equivalence tests proving row-table and columnar-table scan results remain unchanged for the same logical workload.
- Focused tests for all-pass, all-drop, and mixed survivor masks, including reverse scans and limits.
- Simulation tests in the same task covering staged filtering under crash/restart, failpoints, mixed point-read/scan workloads, and constrained-cache runs that prove the optimization degrades gracefully rather than requiring a large memory floor.

---

### T53. Replace exact-range remote caching with segmented cache and coalesced async reads

**Depends on:** T23b, T48, T49

**Description**

Upgrade the remote cache and read path so overlapping scans and nearby point reads share work. This task should introduce the segment/page cache state machine, downloader election, and coalesced range reading needed for S3-primary and tiered cold-read efficiency.

**Implementation steps**

1. Extend the T49 simulation cache model first to cover segment ownership, partial population, downloader election, and restart-time index rebuild.
2. Replace exact-range cache records with aligned segments/pages and explicit per-segment ownership/progress states.
3. Add a coalescing read cursor for remote reads that can:
   - merge nearby ranges,
   - avoid reissuing short forward seeks,
   - prefetch ahead within a bounded budget, and
   - background-complete partially downloaded segments.
4. Keep raw-byte cache policy separate from decoded columnar caches and make segment admission/eviction explicit. No cache introduced by this task may be unbounded.
5. Add startup reconciliation for cache metadata/index files so crash recovery can rebuild or discard incomplete state deterministically.

**Verification**

- Unit tests for overlapping range reuse, aligned-segment lookup, downloader election, and background completion semantics.
- Restart tests proving incomplete or corrupt cache metadata is rebuilt or ignored safely.
- Deterministic simulation and real object-store chaos tests, added with the task, covering concurrent readers, partial downloads, stale listings, timeout/retry paths, and explicit cache-budget ceilings that prove the engine stays correct when the cache is small or disabled.

---

### T54. Implement skip indexes and per-SSTable projection sidecars

**Depends on:** T49, T51, T52, T55

**Description**

Add optional planner-visible sidecars that improve selective reads beyond base zone maps. This includes richer skip indexes and optional per-SSTable projection sidecars, with strict fallback semantics when sidecars are missing or corrupt. Nothing in this task should become required for the default engine profile.

**Implementation steps**

1. Extend the T49 oracle first with sidecar-aware pruning expectations and explicit fallback-to-base behavior.
2. Implement additional skip-index families on top of the T51 synopsis interface, starting with:
   - bloom-style membership summaries, and
   - bounded set summaries where appropriate.
3. Implement per-SSTable projection sidecar metadata and build/read paths in core, keeping them physically tied to the base SSTable lifecycle.
4. Gate all richer skip indexes and sidecars behind explicit per-table or per-feature configuration, with default-off behavior.
5. Expose optional planner/runtime hooks that let higher-level libraries consume these sidecars without moving physical ownership out of the core crate.
6. Ensure sidecar corruption or absence degrades to base SSTable reads rather than causing incorrect answers or whole-table failure.

**Verification**

- Pruning tests proving each skip-index family can exclude work without changing results.
- Crash/restart tests proving base parts remain readable when sidecar publish fails before or after base visibility.
- Deterministic simulation tests, landed with the task, covering sidecar loss/corruption, rebuild/fallback, explicit disabled-by-default runs, and mixed workloads that use or ignore the sidecars.

---

### T55. Harden publish/recovery semantics for immutable parts and sidecars

**Depends on:** T23b, T33c, T48, T49

**Description**

Strengthen the correctness boundary around SSTable, sidecar, and manifest publication. The goal is to make every immutable artifact publish last, verify cleanly, quarantine safely, and GC only after metadata says it is dead.

**Implementation steps**

1. Extend the T49 recovery harness first with cut points for temp-write, checksum publish, manifest switch, quarantine, repair, and delayed delete.
2. Move immutable-part and sidecar publication to an explicit `temp -> finalize checksums/digests -> rename/publish -> visible` flow.
3. Add:
   - full per-file checksums,
   - a compact digest embedded in manifest/publish metadata,
   - per-part applied generation/schema-version records, and
   - delete-as-metadata with delayed physical GC.
4. Implement verify/quarantine/repair entry points that can:
   - recompute checksums from storage,
   - quarantine corrupted artifacts,
   - repair reconstructible metadata where safe, and
   - fail closed otherwise.
5. Ensure sidecars follow the same visibility and quarantine model as base parts, with explicit fallback-to-base semantics.

**Verification**

- Crash/restart simulation matrix proving artifacts are either fully published, fully invisible, or quarantined after faults at every cut point.
- Recovery tests proving delayed deletes do not resurrect dead state and quarantined artifacts are not served.
- Verification-path tests proving checksum mismatch, digest mismatch, and schema/generation mismatch all fail closed and surface actionable repair states, including when optional sidecars are disabled or absent.

---

### T56. Add resource-efficiency primitives and adaptive backpressure for hybrid workloads

**Depends on:** T16, T48, T49

**Description**

Add the low-level efficiency pieces needed to keep the new hybrid-read path cheap: scratch arenas/slabs, bounded scheduler budgets, and adaptive batching/backpressure for flush, compaction, offload, and prefetch work. This task should improve efficiency without raising the default memory floor or enabling aggressive background work by default.

**Implementation steps**

1. Extend the T49 harness first with scheduler/budget observability so fairness and forced-progress behavior can be checked deterministically.
2. Introduce short-lived scratch allocation helpers for encode/merge/materialization hot paths without changing public semantics.
3. Extend the scheduler API with explicit budget surfaces such as:
   - bytes-per-second ceilings,
   - in-flight byte/request limits, and
   - work-class-specific concurrency caps.
4. Add adaptive batching/timeouts for flush, offload, and prefetch work so the engine can force progress under pressure without immediately stalling all writers, but keep aggressive concurrency/prefetch settings explicit and default-off.
5. Make any new caches or buffers introduced by this task byte-bounded and observable.
6. Surface telemetry and table stats needed to reason about these controls without making them authoritative correctness signals.

**Verification**

- Unit tests for budget accounting, work-class concurrency caps, and adaptive timeout decisions.
- Deterministic scheduler tests proving forced progress still occurs under sustained deferral.
- Simulation tests, added in the same task, covering mixed write/scan workloads under constrained local bytes, remote prefetch pressure, compaction backlog, and default-off aggressive settings so the base profile remains low-footprint.

---

### T57. Implement compact-to-wide promotion and the hot-row-to-cold-columnar path

**Depends on:** T50, T51, T52, T53, T55, T56

**Description**

Introduce the staged hybrid layout that lets TerraceDB handle small writes and point reads without giving up efficient colder columnar storage. The goal is not a single magic format, but a deliberate promotion path from write-friendly compact/hot layout to colder wide-columnar layout. This task is workload-driven and should remain opt-in until workload evidence shows it improves real mixed OLTP/OLAP cases without unacceptable complexity or resource overhead.

**Implementation steps**

1. Extend the T49 oracle first with promotion/replacement semantics so hot and cold representations are checked against the same logical state model.
2. Define the compact/hot segment layout and the promotion policy boundary:
   - when a write-friendly segment stays compact,
   - when it promotes during flush/compaction,
   - how replacement is published, and
   - how point reads/scans choose between hot and cold representations.
3. Keep the feature configurable per table so hybrid behavior can be tuned rather than forced globally, with default-off behavior until explicitly enabled.
4. Implement compaction/promotion flows that preserve sequence ordering, visibility, and recovery semantics established in T55.
5. Integrate the T52 scan engine so point reads and short scans can exploit the hot path while longer analytical scans benefit from wide-columnar storage.

**Verification**

- Mixed-workload tests proving hot and cold representations return identical logical results through promotion and replacement.
- Restart tests proving promotion artifacts do not become visible early and old compact state is not resurrected after crashes.
- Deterministic simulation suites, landed with the task, covering point-read-heavy, scan-heavy, and mixed hybrid workloads across flush, compaction, promotion, offload, and recovery, plus explicit disabled-by-default runs that prove the base engine does not depend on this feature.

---

### T58. Build a tiny telemetry example app for the hybrid OLTP/OLAP path

**Depends on:** T49, T52, T53, T54, T55, T56, T57

**Description**

Add a sibling example to `examples/todo-api` that is as small and teachable as the TODO app but analytically shaped. The example should show a basic hybrid application: point-write and point-read current device state plus filtered historical scans over a columnar telemetry table. It should exercise the universal Phase 11 foundation by default, and optionally enable the T54/T57 accelerants via explicit configuration rather than making them mandatory.

**Implementation steps**

1. Extend the T49 simulation/oracle helpers first with an example-oriented telemetry workload model and app-level invariants before the example implementation branches from the engine work.
2. Create `examples/telemetry-api` with a minimal HTTP surface and README, using the TODO example's structure as a template but keeping the data model focused on hybrid reads:
   - a row-oriented or hot `device_state` table keyed by device,
   - a historical `sensor_readings` table configured for columnar-v2 scans, and
   - typed request/response shapes and table helpers so the example stays approachable.
3. Add the smallest API that still exercises the new read path:
   - ingest one or a small batch of readings,
   - fetch the latest state for one device,
   - run a filtered time-window scan with caller-selected projected columns, and
   - run one simple analytical endpoint such as alert counts or min/max/avg over a window, implemented on top of the scan path rather than a separate query engine.
4. Configure the default example profile to demonstrate the universal Phase 11 path:
   - binary columnar-v2 encoding,
   - marks/granules with base zone maps,
   - PREWHERE-lite and late materialization on filterable fields,
   - segmented remote caching in a tiered or S3-primary simulation profile, and
   - publish/recovery and bounded-budget defaults.
5. Add an explicit accelerator profile for the same example workload that can opt into:
   - richer skip indexes or projection sidecars from T54,
   - compact-to-wide promotion from T57, and
   - any non-default T56 knobs needed for side-by-side evaluation,
   while keeping the base profile authoritative and fully supported when the accelerants are off.
6. Document clearly which endpoints and workload patterns exercise which engine features, and call out which behaviors are default versus opt-in.

**Verification**

- End-to-end deterministic simulation tests, added in the same task, covering base-profile ingest, point reads, filtered scans, and the analytical endpoint with seed-stable results.
- Low-memory and low-cache-budget simulation runs proving the default example profile still behaves correctly without requiring a large memory floor.
- Remote cold-read and restart simulations proving segmented cache reuse, publish/recovery correctness, and crash-safe reopen behavior for the telemetry workload.
- Equivalence tests proving the accelerator profile returns the same logical answers as the base profile while richer skip indexes, sidecars, and hot-to-cold promotion are enabled.
- Fallback simulations proving disabled, missing, or corrupt accelerants degrade to the base path rather than breaking the example or changing results.

## Phase 12 — Generalized current-state retention and ranking

**Parallelization:** T59 first. After that, T60 and T61 can proceed in parallel against the frozen contracts and shared simulation/oracle scaffolding. T62 follows once both policy families exist and can be integrated with scheduler/backpressure and physical reclamation behavior. T62a follows T62 once the public configuration surface and operational semantics are stable enough to teach through a small example.

### T59. Freeze generalized current-state retention contracts, planner seams, and shared simulation/oracle scaffolding

**Depends on:** T15, T19, T21, T31b

**Description**

Separate generalized **current-state retention** from the existing sequence-based MVCC/CDC retention surfaces, then freeze the caller-extensible ordering and planning contracts before implementation branches diverge. The goal is to make threshold-style and rank-style retention work parallelizable without weakening `SnapshotTooOld` or commit-log retention semantics.

**Implementation steps**

1. Define a `CurrentStateRetentionPolicy`-style contract that is explicitly distinct from `history_retention_sequences` and commit-log retention, covering at least:
   - threshold retention over a caller-defined sortable key plus cutoff,
   - global-rank retention over a caller-defined total order plus limit, and
   - any required metadata/stat surfaces for policy observability and rebuild.
2. Freeze the deterministic ordering contract up front: sort-key encoding/comparison rules, required tie-break semantics, ascending/descending direction, missing-value behavior, and fail-closed handling when the caller cannot provide a total order.
3. Freeze planner/executor seams for:
   - compaction-time row removal,
   - projection-owned or derived ranked materializations,
   - physical offload/delete integration, and
   - rebuild/recompute behavior after restart or policy revision.
4. Add shared retained-set oracle helpers and deterministic simulation scaffolding immediately, including sortable-threshold scenarios, top-N boundary churn, tie storms, snapshot-pinning cases, and crash/restart skeletons that later tasks will extend rather than re-invent.
5. Define introspection for the effective logical floor/cutoff, current retained-set summary, rows/bytes reclaimed, policy revision, and explicit reasons a policy was skipped, blocked by snapshots, or degraded to derived-only behavior.

**Verification**

- Compile-only/API tests that instantiate every policy family and planner seam without requiring full storage implementations.
- Deterministic unit tests proving sort-key and tie-break contracts yield a stable total order across reruns.
- Oracle tests for retained-set model helpers covering threshold and rank policies before real reclaim logic lands.
- Simulation smoke tests that exercise stub threshold/rank policies under updates, deletes, policy revision, and restart, proving the contract layer itself is deterministic.

---

### T60. Threshold-based sortable current-state retention

**Depends on:** T13, T15, T21, T59

**Description**

Generalize TTL-style row removal and oldest-first local retention into threshold-based current-state retention over caller-defined sortable values rather than hard-coded internal timestamps or sequence heuristics. This task covers policies such as `created_at >= cutoff`, `score >= watermark`, or similar single-row threshold rules while preserving snapshot safety and leaving MVCC/CDC history retention sequence-based.

**Implementation steps**

1. Implement threshold evaluation over caller-provided sortable keys in the row-retention path, reusing the ordering contract from T59 and supporting engine-derived cutoffs (for example injected-clock windows) plus explicit application-provided cutoffs.
2. Extend the shared oracle/simulation scaffolding in lockstep with the production path: add moving-threshold workloads, rows with missing keys, updates that cross the threshold, and restart cases before wiring full reclaim/executor behavior.
3. Preserve snapshot safety and history semantics by ensuring threshold removals obey the active-snapshot horizon, surface clear introspection when snapshots pin reclaim, and never redefine `SnapshotTooOld` or change-feed retention rules.
4. Integrate threshold policies with local space-reclamation planning where exact semantics are available, and make unsupported or approximation-prone layouts fail closed rather than silently reclaiming the wrong files.
5. Add stats/admin surfaces reporting the effective threshold, rows/bytes reclaimed, backlog caused by snapshot pins, and any rows/files deferred because exact reclaim was not yet possible.

**Verification**

- Tests covering threshold retention across inserts, updates, deletes, and rows that move above/below the cutoff.
- Deterministic restart tests proving the same retained current state is reconstructed after reopen.
- Oracle tests proving retained membership matches the model for all rows at a chosen cutoff.
- Simulation tests with moving cutoffs, long-lived snapshots, compaction/offload interleavings, and crash/recovery during partially completed reclaim work.

---

### T61. Ranked and computed-measure current-state retention/materialization

**Depends on:** T31b, T59

**Description**

Implement top-N / leaderboard-style current-state retention over caller-defined computed rankings, including multi-field measures and deterministic tie-breaking. This task owns the planner/materializer semantics for rank-based retention so the engine does not pretend a global rank is a local row filter.

**Implementation steps**

1. Implement a global-rank planner over current state or projection-owned source ranges, requiring rank key + tie-break + source key to form a deterministic total order.
2. Extend the shared oracle/simulation scaffolding immediately with N-boundary churn, tie storms, score recomputation, membership oscillation, and rebuild-vs-incremental equivalence cases before optimizing or broadening the planner.
3. Support computed rankings derived from multiple caller-defined values and add recipe-style helpers for common patterns such as leaderboards, recent items, and hybrid orderings like `(points, created_at, id)`.
4. Publish ranked retention as projection-owned/materialized outputs by default, and allow destructive source-table retention only when the source is explicitly declared rebuildable; otherwise fail closed rather than silently turning a derived ranking into irreversible source deletion.
5. Add introspection for the effective cutoff rank, retained membership changes, evaluation cost, and whether a policy is running in derived-only mode or destructive mode.

**Verification**

- Tests covering inserts, updates, deletes, ties, truncation at the N boundary, and deterministic tie-break behavior.
- Replay and rebuild-equivalence tests proving full recompute and incremental maintenance converge on the same ranked retained set.
- Example-oriented tests showing leaderboard-style and “recent items” policies can be expressed through the new ranking hooks.
- Simulation tests with crash/restart during rank-plan publication, output rewrite, rebuild fallback, and rapid score churn near the cutoff.

---

### T62. Policy coordination, physical reclamation, and deterministic hardening for generalized retention

**Depends on:** T16, T21, T60, T61

**Description**

Coordinate generalized logical retention with compaction, offload/delete, scheduler/backpressure, and recovery so policies reclaim space safely without producing hidden approximation boundaries or semantic drift across restarts.

**Implementation steps**

1. Teach the maintenance/scheduler pipeline to consume generalized retention planner outputs, explicitly separating logical row eviction from physical SSTable movement/deletion and documenting where exact reclaim requires rewrite compaction instead of file-level selection.
2. Add crash points and deterministic simulation cases at each new coordination boundary as the code lands: plan computation, manifest publication, local cleanup, rebuild fallback, and policy-revision changes.
3. Ensure concurrent writes, policy changes, and restarts do not cause oscillation, duplicate reclamation, or retained-set drift; persist enough metadata to resume or recompute safely after reopen.
4. Integrate observability/backpressure so operators can see when a policy is CPU-bound, rank-churn-heavy, blocked on rewrite compaction, or pinned by snapshots before physical space can actually be reclaimed.
5. Document exact-vs-derived semantics clearly for row tables, columnar tables, and projection-owned outputs so callers know when a policy is logical-only, derived-only, or truly reclaiming physical bytes.

**Verification**

- Tests proving policy revisions and restart/recovery do not corrupt retained membership, manifests, or derived outputs.
- Tests proving scheduler/offload/backpressure decisions never violate logical retention guarantees or degrade into silent approximation.
- Cross-mode tests covering tiered/offload and other supported storage layouts, with explicit fail-closed assertions for unsupported combinations.
- Simulation tests with oscillating budgets, rapid rank churn, concurrent compaction/offload, and crash/restart around every coordination boundary introduced by the new policies.

---

### T62a. Build a small example app that demonstrates generalized retention policy configuration

**Depends on:** T62

**Description**

Add a sibling example to `examples/todo-api` that demonstrates how applications configure and observe the generalized current-state retention policies introduced in Phase 12. The example should stay small and teachable while showing both threshold-style retention and rank-based retention/materialization, along with the operational signals that explain when a policy is logical-only, derived-only, or waiting on physical reclamation.

**Implementation steps**

1. Extend the Phase 12 simulation/example harness first with an example-oriented workload model and app-level invariants covering threshold cutoffs, rank churn near the boundary, policy updates, and restart behavior before the example implementation branches from the engine work.
2. Create a small example app (for example `examples/retention-api`) with:
   - one threshold-retained dataset such as expiring sessions, events, or records keyed by a sortable timestamp/score,
   - one rank-retained or derived leaderboard/recent-items dataset with explicit tie-break configuration, and
   - typed request/response helpers so the example focuses on retention concepts rather than serialization boilerplate.
3. Add the smallest API or CLI surface that still demonstrates how to set the new policies in practice:
   - configure or update a threshold policy,
   - configure or update a rank/limit policy,
   - write and mutate rows so membership crosses the configured boundaries, and
   - inspect the retained current state or derived output after policy application.
4. Surface the operational semantics the phase introduces:
   - introspection showing the effective cutoff/rank boundary,
   - whether the example policy is destructive or derived-only,
   - when snapshots or compaction are delaying physical reclaim, and
   - the explicit separation between generalized current-state retention and MVCC/CDC history retention.
5. Document clearly which example operations map to threshold retention, rank retention, policy updates, and physical-reclaim behavior so users can copy the configuration patterns into their own applications without reading the full architecture document first.

**Verification**

- End-to-end deterministic simulation tests for the example workload covering threshold retention, rank retention, policy updates, and seed-stable retained membership/results.
- Example integration tests proving reopen/restart preserves policy configuration, retained outputs, and documented introspection behavior.
- Tests proving the example's documented policy toggles fail closed for unsupported destructive modes and otherwise keep derived-only versus destructive behavior explicit.
- Simulation tests showing snapshot pinning, compaction delay, or rapid rank churn do not make the example report misleading retained-state or reclamation status.

---

## Phase 13 — Execution domains, control-plane isolation, and colocated multi-DB foundations

**Parallelization:** T63 first. After that, T64, T65, and T66 can proceed in parallel. T67 depends on T64 + T65 + T66. T68 depends on T64 + T66 + T67. T69 depends on T64 + T65 + T66 + T67 + T68. T70 depends on T64 + T65 + T66 + T67.

**Phase rule:** T63 freezes the interfaces first, and the rest of the phase should maximize parallel work against those fixed seams rather than re-opening the core contracts. Execution domains are a placement/scheduling/resource abstraction, not a correctness abstraction. Moving work between domains or changing domain budgets may change latency, throughput, and backlog behavior, but must not change commit ordering, visibility, durability semantics, recovery outcomes, or the final correctness of rebuild/replication-derived state.

**Simulation rule:** Every task in this phase must extend the relevant deterministic oracle/cut-point/simulation harness in the same change as the production-path change. Do not defer task-local verification to the capstone; T69 is additive whole-system hardening, not a substitute for self-contained simulation in T64-T68 and T70.

**Adoption rule:** Treat the control-plane domain and conservative bounded per-domain budgeting as the universal foundation. Treat aggressive isolation, shard-local placement rules, and any optional dedicated-resource reservations as incremental opt-in controls that remain configurable and default-off until workload evidence justifies broader adoption.

### T63. Freeze execution-domain, durability-class, and resource-manager contracts

**Depends on:** T16, T33, T56

**Description**

Define the long-term abstraction for colocating multiple Terracedb workloads in one process without conflating correctness and resource isolation. The goal is to freeze the contracts before implementation branches diverge: a unit of work may be assigned an execution domain and a durability class, but the two are related without being the same thing.

**Implementation steps**

1. Define an `ExecutionDomain`-style contract covering:
   - hierarchical naming and ownership,
   - domain-local CPU/memory/I/O/background-work budgets,
   - optional dedicated versus shared-weighted placement modes, and
   - domain introspection and lifecycle hooks.
2. Define a distinct `DurabilityClass`-style contract covering:
   - default user-data durability,
   - internal/control-plane durability,
   - any future specialized classes such as deferred or remote-primary write lanes,
   while making it explicit that execution-domain assignment alone does not change correctness semantics.
3. Freeze the process-wide `ResourceManager` / placement-policy seam that owns total process budgets and maps databases, shards, and subsystems into domains.
4. Define the required invariants up front:
   - correctness invariants under domain movement,
   - isolation invariants under overload,
   - liveness rules for emergency flush/compaction/recovery work, and
   - explicit rules for control-plane progress under user-data pressure.
5. Freeze the reusable simulation/oracle seams up front for:
   - domain-tagged work items,
   - domain-local budget accounting,
   - control-plane versus user-data contention, and
   - colocated multi-DB workload generators.
6. Add compile-time stubs and placeholder types so later tasks can build against the domain/resource contracts without immediately committing to one scheduling backend.

**Verification**

- Compile-only tests proving the new domain/resource/durability contracts compose cleanly with the existing scheduler, DB builder, and maintenance APIs.
- Deterministic smoke tests proving work can be tagged with domains and durability classes through injected fake runtimes without touching real I/O.
- Invariant tests making it explicit that changing execution-domain placement does not change logical DB outcomes.

---

### T64. Implement hierarchical execution domains and process-wide resource budgeting

**Depends on:** T63

**Description**

Implement the runtime substrate for hierarchical execution domains so colocated databases, future shards, and attached subsystems can share one process under explicit budgets. This task owns resource accounting and placement, not separate storage semantics.

**Implementation steps**

1. Extend the simulation/oracle harness first so it can model hierarchical domains, shared versus reserved budgets, and deterministic contention outcomes before the production resource manager lands.
2. Implement the process-wide resource manager for:
   - total CPU worker/scheduling budget,
   - cache and mutable-memory budgeting,
   - local I/O concurrency/bandwidth ceilings,
   - remote/object-store concurrency ceilings, and
   - background task slot accounting.
3. Implement hierarchical domains that can represent at least:
   - process/control,
   - database foreground/background,
   - future shard-local foreground/background, and
   - attached subsystem domains such as projections, workflows, or analytics helpers.
4. Support both shared-weighted and optionally dedicated reservations, while keeping the conservative default profile shared and bounded rather than over-partitioned.
5. Extend the DB open/builder/configuration path so colocated databases can be assigned to domains without requiring storage-level changes.
6. Add observability for configured budgets, effective usage, contention, and domain-local backlog without turning those signals into correctness primitives.

**Verification**

- Tests proving per-domain budgets are enforced for representative CPU/memory/I/O/background limits.
- Deterministic scheduler tests proving busy domains cannot consume more than their configured allowances while idle capacity can still be reused when policy allows.
- Multi-DB simulation tests proving colocated databases can be opened with different domain assignments while preserving identical logical results.

---

### T65. Implement the control-plane domain and dedicated internal durability lane

**Depends on:** T23b, T63

**Description**

Introduce a protected control-plane path for catalog, manifest, schema, cursor, and other recovery-critical metadata. This task owns the explicit mapping between the control-plane execution domain and a dedicated internal durability class, while preserving the rule that domain placement and durability class remain distinct concepts.

**Implementation steps**

1. Extend the simulation/recovery harness first with control-plane-domain contention, dedicated durability-lane behavior, and restart ordering checks before the production control-plane path lands.
2. Define the reserved control-plane domain and route recovery-critical metadata work through it:
   - catalog/schema updates,
   - manifest and publish metadata,
   - durable cursor/control metadata, and
   - any other internal state required for reopen/recovery progress.
3. Implement a dedicated control-plane durability lane or WAL class for that metadata rather than forcing it to share the default user-data path under all conditions.
4. Reserve a bounded internal write/memory budget so control-plane progress cannot deadlock behind user-data pressure.
5. Integrate startup/recovery ordering so control-plane metadata can be replayed/validated before dependent user-data recovery steps when required.
6. Add explicit fail-closed rules for control-plane corruption or class mismatch, including actionable repair/recovery reporting.

**Verification**

- Tests proving control-plane writes remain durable and progress-capable under sustained user-data write/load pressure.
- Crash/restart tests proving control-plane recovery can complete before dependent user-data reopen steps where required.
- Simulation tests proving protected internal budgets do not let user-data workloads starve schema/manifest/cursor progress.

---

### T66. Make scheduler, admission control, caches, and background work domain-aware

**Depends on:** T63, T56

**Description**

Teach the existing runtime controls to respect execution-domain boundaries. This task owns the integration between domains and the scheduler/backpressure/caching machinery so domains become real operational boundaries instead of passive labels.

**Implementation steps**

1. Extend the deterministic scheduler/admission harness first so it can reason about domain-aware deferral, throttling, control-plane overrides, and cache-budget partitioning before the production integration lands.
2. Extend the scheduler and maintenance pipeline so work items carry execution-domain identity and domain-local budget context.
3. Make read/write admission, remote-cache admission, prefetch, compaction, offload, and projection/workflow background work respect domain-local ceilings and priorities.
4. Add support for control-plane priority overrides so emergency/internal work can still make progress without bypassing the domain model entirely.
5. Ensure caches and mutable-memory budgets can be partitioned or weighted per domain, while keeping conservative defaults simple and bounded.
6. Surface domain-aware backlog, throttling, and starvation diagnostics so operators can understand why work was deferred or shed.

**Verification**

- Deterministic tests proving domain-aware scheduling changes resource distribution but not logical DB outcomes.
- Mixed-workload simulation tests where one domain runs scan-heavy or compaction-heavy traffic and cannot starve protected foreground or control-plane domains.
- Cache/admission tests proving per-domain ceilings stay bounded and observable under both local and remote-read pressure.

---

### T67. Add multi-DB colocated deployment support and placement policy wiring

**Depends on:** T64, T65, T66

**Description**

Make execution domains usable by real embeddings that host multiple databases in one process. This task owns the policy/configuration and ergonomic layer needed to place colocated DBs and subsystems into domains without exposing too much runtime plumbing to ordinary callers.

**Implementation steps**

1. Extend the simulation/integration harness first with colocated multi-DB placement scenarios and default-policy checks before the production API wiring lands.
2. Extend the builder/config API so callers can:
   - declare multiple colocated DB instances,
   - assign them to domain hierarchies,
   - select conservative shared versus reserved placements, and
   - wire attached subsystems into the same domain tree.
3. Define default placement policies for common shapes:
   - single DB,
   - two colocated DBs,
   - primary DB plus analytics/helper DB, and
   - future shard-ready layouts with distinct foreground/background lanes.
4. Add introspection/reporting APIs for domain topology, effective budgets, and placement decisions.
5. Keep the single-DB default ergonomics simple so ordinary users are not forced to understand domains before they need them.
6. Document the operational model clearly, including the distinction between execution-domain placement and durability class selection.

**Verification**

- Multi-DB integration tests proving colocated databases can be opened, operated, and recovered independently while sharing one process/runtime.
- Configuration tests proving default single-DB profiles do not require explicit domain setup.
- Introspection tests proving domain trees, budget assignments, and placement decisions are reported consistently.

---

### T68. Add shard-ready placement rules and deterministic hardening for execution domains

**Depends on:** T64, T66, T67

**Description**

Harden the execution-domain system so it can serve as the future foundation for physical sharding without claiming that full sharding is complete in this phase. This task owns shard-ready placement semantics, migration-safe invariants, and the deterministic test matrix for domain-aware overload and recovery behavior.

**Implementation steps**

1. Extend the deterministic domain harness first with shard-ready placement shapes and reconfiguration events before the production hardening work lands.
2. Define shard-ready domain naming and ownership rules so future physical shards can slot into the existing hierarchy without redesigning the abstraction.
3. Add deterministic workload generators for:
   - multiple colocated DBs,
   - control-plane pressure,
   - scan-heavy versus write-heavy competing domains, and
   - future shard-local foreground/background placement shapes.
4. Add cut points and recovery tests for:
   - control-plane replay under user-data pressure,
   - domain-budget reconfiguration,
   - emergency flush/compaction progress, and
   - reopen after overloaded or partially drained background domains.
5. Verify liveness rules such as:
   - control-plane progress under load,
   - emergency maintenance progress,
   - no deadlock when protected and shared domains contend, and
   - deterministic behavior when budgets are tightened or relaxed.
6. Document the exact boundary of the phase: the engine becomes shard-ready from a placement/resource perspective, but physical per-table data sharding still remains a later phase.

**Verification**

- Deterministic simulation suites proving domain-aware overload, reconfiguration, and recovery remain reproducible and do not alter correctness outcomes.
- Liveness tests proving protected domains continue to make progress while shared domains are throttled or shed.
- Tests and docs making it explicit that execution domains provide shard-ready placement foundations without claiming completed physical data sharding.

---

### T69. Build whole-system simulation and chaos suites for execution domains

**Depends on:** T64, T65, T66, T67, T68

**Description**

Add the capstone deterministic hardening pass for the domains subsystem. This task owns the holistic simulation and chaos matrix across multiple features together: colocated DBs, control-plane isolation, domain-aware scheduling, budget reconfiguration, and shard-ready placement behavior. It does not replace task-local simulation; it verifies that the composed system still behaves correctly when all of those features interact.

**Implementation steps**

1. Compose the per-task domain simulation/oracle helpers into a whole-system harness that can run:
   - multiple colocated DB instances,
   - competing foreground/background/control-plane domains,
   - dynamic budget changes,
   - restart/recovery under pressure, and
   - shard-ready placement shapes.
2. Add long-running deterministic campaigns that combine:
   - user-data pressure plus control-plane metadata churn,
   - scan-heavy versus write-heavy competing tenants,
   - remote-I/O contention plus cache partitioning,
   - emergency maintenance work under protected/shared contention, and
   - colocated DB open/close/reopen sequences in one process.
3. Add a real-runtime fault/chaos layer for this phase's features where appropriate, including controlled task stalls, injected timing skew, and budget-tightening events that complement the deterministic harness without weakening reproducibility requirements.
4. Verify the full invariants matrix:
   - correctness does not change under domain reassignment,
   - protected domains retain progress under load,
   - shared domains respect configured ceilings, and
   - recovery remains deterministic and fail-closed.

**Verification**

- Large-seed deterministic simulation campaigns proving domain-aware behavior remains reproducible across colocated multi-DB, control-plane, and shard-ready placement scenarios.
- Cross-feature chaos tests proving budget changes, recovery, and protected-domain progress remain correct under injected contention and task stalls.
- End-to-end invariant tests proving the full execution-domain system changes performance behavior only, not logical DB outcomes.

---

### T70. Build a small example app that demonstrates execution domains

**Depends on:** T64, T65, T66, T67

**Description**

Add a sibling example to `examples/todo-api` that demonstrates why execution domains exist in practice. The example should stay small and teachable while showing two colocated workloads in one process plus a protected control-plane path, so users can see domain assignment, conservative defaults, and basic observability without needing to read the full architecture doc first.

**Implementation steps**

1. Extend the phase-local simulation/example harness first with an example-oriented workload model covering:
   - one latency-sensitive primary DB,
   - one lower-priority analytics/helper DB, and
   - control-plane metadata activity under competing load.
2. Create a small example app (for example `examples/domains-api`) with:
   - two colocated DB instances opened in one process,
   - explicit execution-domain configuration,
   - a protected control-plane domain,
   - a small HTTP or CLI surface that triggers foreground reads/writes, background activity, and a helper/analytics workload, and
   - observability output that shows the chosen domain topology and effective budgets.
3. Keep the default profile conservative and approachable:
   - shared weighted domains by default,
   - optional reserved/protected settings called out explicitly,
   - no requirement to understand future physical sharding.
4. Document clearly:
   - which operations run in which domains,
   - how the control-plane domain differs from ordinary foreground/background domains, and
   - that domain placement changes performance/isolation behavior, not correctness semantics.

**Verification**

- End-to-end deterministic simulation tests for the example workload proving the primary workload remains correct and protected while the helper workload is stressed.
- Example integration tests proving colocated DB open/reopen, domain introspection, and control-plane progress all work under the documented default profile.
- Example-level equivalence tests proving changing domain placement alters latency/backlog behavior but not logical answers.

---

## Phase 14 — Unified-log pressure, flush reclamation, and adaptive write admission

**Parallelization:** T71 first. After that, T72, T73, and T74 can proceed in parallel against the frozen interfaces. T75 depends on T73 + T74 + T70. T76 depends on T72 + T73 + T74 + T75.

**Phase rule:** T71 freezes the accounting and admission interfaces first, and the rest of the phase should maximize parallel implementation against those fixed seams rather than re-opening the core contracts. This phase changes when the engine chooses to flush, throttle, or stall; it must not change commit ordering, visibility rules, durability semantics, crash-recovery results, or the logical answers returned by reads/scans.

**Simulation rule:** Every task in this phase must extend the relevant deterministic oracle/cut-point/simulation harness in the same change as the production-path change. Do not defer task-local simulation to the capstone; T76 is additive whole-system hardening, not a substitute for self-contained coverage in T72-T75.

**Adoption rule:** Treat the existing L0-driven controls as the conservative fallback, not something to delete immediately. The new unified-log pressure and fine-grained memory accounting signals should layer in alongside L0/compaction signals, with bounded default thresholds and explicit observability before more aggressive policies become the default. Domain-local soft policy may differ by workload shape — for example, stricter OLTP-oriented domains and looser OLAP-ingest-oriented domains — but process-wide hard guardrails for memory exhaustion, unified-log exhaustion, and liveness still override domain-local preferences.

### T71. Freeze unified-log pressure, flush-reclaim, and admission-control contracts

**Depends on:** T33, T63, T66

**Description**

Define the long-term contracts for pressure-aware flushing and write admission before implementation branches diverge. The goal is to make the engine reason explicitly about dirty bytes, queued-for-flush bytes, bytes already being flushed, and unified-log pressure without conflating those signals or changing correctness semantics.

**Implementation steps**

1. Extend the deterministic simulation/oracle harness first with a fake unified-log pressure model that can represent:
   - mutable dirty bytes,
   - immutable bytes queued for flush,
   - immutable bytes already in-flight to flush,
   - unified-log bytes pinned by unflushed state,
   - oldest-unflushed age, and
   - estimated relief from candidate flushes.
2. Freeze a `PressureStats` / `FlushPressureCandidate` / `AdmissionSignals`-style interface that distinguishes:
   - current dirty bytes versus already-flushing bytes,
   - memory pressure versus unified-log pressure,
   - local per-table/per-domain signals versus process-wide totals, and
   - pressure signals versus correctness metadata such as durable watermarks.
3. Define the crash/restart and failed-flush reconstruction rules up front so pressure accounting can be rebuilt deterministically from manifests, memtables, and unified-log state after reopen.
4. Define the relationship to execution domains and the resource manager: domains may budget mutable memory and unified-log pressure, but the single-DB/single-domain default must remain simple and conservative. Make it explicit that domains may choose different soft policies by workload shape, while process-wide hard safety guardrails remain global.
5. Freeze the scheduler/admission seam for pressure-aware flush scoring and write throttling/stalling so later tasks can implement policy without reworking the public contracts.
6. Add compile-time stubs and placeholder types so the accounting, scheduler, and observability work can proceed in parallel against the same fixed interface.

**Verification**

- Compile-only tests proving the new pressure/admission contracts compose with the existing scheduler, maintenance, and DB builder APIs.
- Deterministic smoke tests proving fake runtimes can tag work with pressure signals and reconstruct those signals after simulated restart.
- Invariant tests making it explicit that changing pressure thresholds changes latency/backlog behavior only, not logical DB outcomes.

---

### T72. Implement fine-grained dirty-byte, flushing-byte, and unified-log pinning accounting

**Depends on:** T71

**Description**

Implement the accounting substrate needed for pressure-aware flushing. This task owns the production tracking of dirty bytes, queued-for-flush bytes, bytes already being flushed, and unified-log pressure pinned by unflushed state across steady-state operation, failures, and restart.

**Implementation steps**

1. Extend the simulation/oracle harness first so it models accounting transitions across:
   - commit into the mutable memtable,
   - memtable rotation,
   - flush start,
   - flush completion,
   - flush failure, and
   - crash/restart reconstruction.
2. Extend memtable / immutable-memtable state so the engine tracks at least:
   - mutable dirty bytes,
   - immutable queued bytes,
   - immutable flushing bytes,
   - per-table and per-domain contributions, and
   - any unified-log segment/range or sequence-watermark information needed to estimate pinned log pressure.
3. Implement deterministic reconstruction of those counters on open/recovery instead of trusting stale in-memory state.
4. Expose the accounting through runtime stats/telemetry/introspection without turning it into correctness metadata.
5. Keep the accounting precise enough to distinguish "still dirty" bytes from bytes already being drained by an in-flight flush.

**Verification**

- Accounting tests proving bytes transition cleanly between mutable, queued, flushing, and reclaimed states without double-counting.
- Crash/restart tests proving pressure counters reconstruct deterministically after reopen and after failed flushes.
- Simulation tests proving the same write history yields the same pressure-accounting trace across replay of the same seed.

---

### T73. Implement pressure-aware flush candidate scoring and forced-flush guardrails

**Depends on:** T71

**Description**

Teach the maintenance loop to choose flush work based on actual relief value, not just the existence of immutable memtables. This task owns pressure-aware flush scoring, age-sensitive guardrails, and conservative forced-flush rules that react earlier than coarse L0-only pressure.

**Implementation steps**

1. Extend the deterministic maintenance harness first with a pressure-aware flush oracle that scores candidate flushes by:
   - unified-log bytes likely to be reclaimed,
   - dirty-byte relief,
   - oldest-unflushed age,
   - per-table/per-domain budget pressure, and
   - interaction with existing L0/compaction guardrails.
2. Enrich pending flush candidates with the estimated relief metadata needed by the scheduler/maintenance loop rather than surfacing only a raw byte count.
3. Implement pressure-aware flush prioritization so the engine can prefer the flush that best relieves unified-log and memory pressure, while preserving conservative fallback behavior when the richer signals are unavailable.
4. Add forced-flush rules based on unified-log pressure, dirty-byte pressure, and age ceilings, while retaining L0 hard ceilings as an independent safety guardrail.
5. Surface operator-facing diagnostics that explain why a flush was selected or forced.

**Verification**

- Deterministic tests proving the chosen flush order changes when pressure relief changes, even if raw immutable-byte counts are similar.
- Mixed-workload simulation tests proving pressure-aware flush selection reduces pathological stalls compared with L0-only heuristics while preserving correctness.
- Guardrail tests proving the engine still forces progress when the scheduler defers work indefinitely.

---

### T74. Implement multi-signal write admission and domain-aware pressure budgeting

**Depends on:** T71

**Description**

Teach the write-admission path to react to multiple pressure signals together instead of waiting mostly for L0 count to become unhealthy. This task owns adaptive throttling/stalling based on unified-log pressure, fine-grained memory pressure, flush backlog, and optional domain-local budgets.

**Implementation steps**

1. Extend the deterministic scheduler/admission harness first so it can model multi-signal throttling decisions using:
   - unified-log pinned bytes,
   - mutable dirty bytes,
   - immutable queued/flushing bytes,
   - oldest-unflushed age,
   - L0/compaction debt, and
   - optional per-domain budget pressure.
2. Implement admission heuristics that combine those signals into:
   - no throttle,
   - rate limit, or
   - stall,
   while keeping the default thresholds conservative and debuggable.
3. Integrate the new admission signals with execution domains / resource-manager budgets so one busy DB or domain cannot pin all mutable memory or unified-log headroom for the process.
4. Support workload-shaped soft policy at the domain level, so OLTP-oriented domains can prefer earlier throttling/flush pressure relief while OLAP-ingest-oriented domains can tolerate larger buffers and later intervention.
5. Preserve process-wide hard guardrails above all domain policy so global memory exhaustion, unified-log exhaustion, and liveness threats still force action even when a domain's preferred policy is more relaxed.
6. Preserve a simple single-DB path where the engine can run without explicit domain configuration and still benefit from the richer pressure model.
7. Add observability for which signal triggered throttling/stalling and how much pressure relief is needed to recover.

**Verification**

- Deterministic tests proving write admission reacts before the system reaches pathological L0 pressure when unified-log or memory pressure is already high.
- Multi-DB/domain simulation tests proving one workload cannot monopolize pressure budgets and force unrelated tenants into avoidable stalls.
- Equivalence tests proving richer admission signals change only performance/backlog behavior, not committed results.

---

### T75. Extend the example app to demonstrate pressure-aware flushing and write admission

**Depends on:** T73, T74, T70

**Description**

Extend the domains example so users can see pressure-aware flushing and write admission in practice. The example should stay small and teachable while showing how dirty bytes, in-flight flush bytes, unified-log pressure, and domain budgets interact under a bursty workload.

**Implementation steps**

1. Extend the phase-local simulation/example harness first with an example-oriented workload model covering:
   - a bursty primary writer,
   - a lower-priority helper workload,
   - slower background flush capacity, and
   - a visible recovery path once pressure is relieved.
2. Extend the domains sample app (or its successor example) so it surfaces:
   - current dirty bytes,
   - queued versus already-flushing bytes,
   - unified-log pressure,
   - active throttle/stall state, and
   - the configured domain/resource budget layout.
3. Demonstrate conservative defaults first, then optionally show a more aggressive profile where domain-aware budgets protect the primary workload sooner.
4. Document clearly that these controls affect latency, backlog, and resource isolation rather than logical correctness.

**Verification**

- Example simulation tests proving the primary workload remains correct while pressure-aware throttling/flush selection changes backlog and latency behavior.
- Example integration tests proving the surfaced pressure metrics and throttle states match the documented workload transitions.
- Example-level equivalence tests proving conservative and aggressive profiles return the same logical answers.

---

### T76. Build the capstone whole-system simulation, fuzz, and chaos coverage for pressure-aware flushing

**Depends on:** T33e, T72, T73, T74, T75

**Description**

Add the capstone deterministic hardening pass for the pressure-aware flush/admission subsystem. This task owns the whole-system matrix across group commit, deferred durability, unified-log pressure, flush selection, multi-DB/domain contention, and restart/failure handling. It does not replace task-local simulation; it verifies that the composed system still behaves correctly when all of those features interact. The main matrix should be expressed as seeded deterministic simulation / generated-fuzz coverage built on the shared cross-cutting fuzz substrate from T33e rather than as a pressure-only one-off harness: reuse the shared generator/replay/minimization plumbing where possible, and keep only pressure-specific generators/oracles local to this task. Keep a smaller focused runtime chaos layer for fault-injection-specific behavior such as flush stalls and timing-sensitive recovery, but do not let that layer become the primary correctness story.

**Implementation steps**

1. Compose the per-task pressure simulation/oracle helpers into the shared broad-input support layer so the pressure-aware whole-system harness can run:
   - bursty write spikes,
   - slow or failed flushes,
   - group-commit and deferred-durability modes,
   - multiple colocated DB/domain workloads, and
   - restart/recovery under sustained pressure.
2. Add long-running deterministic generated / fuzz campaigns that combine:
   - unified-log pressure plus L0/compaction debt,
   - flush failures plus retry/reopen,
   - pressure spikes during control-plane and user-data contention, and
   - budget reconfiguration in the presence of in-flight flush work.
3. Keep checked-in deterministic pressure regressions split as one seed per test wherever practical so local/pre-commit runners can exploit parallelism; reserve larger seed matrices for CI/nightly campaigns or generated broad-input runs.
4. Add a focused real-runtime chaos layer where appropriate, including injected flush stalls, delayed durability completion, and abrupt budget tightening events that complement the deterministic fuzz/simulation harness without weakening reproducibility requirements.
5. Verify the full invariants matrix:
   - no correctness change under different pressure thresholds,
   - pressure counters eventually converge after work completes,
   - domain-local soft policy differences change performance behavior only, while global hard guardrails still preserve liveness,
   - protected domains retain progress under load, and
   - recovery remains deterministic and fail-closed.

**Verification**

- Large-seed deterministic simulation / generated-fuzz campaigns proving pressure-aware flushing and admission remain reproducible across restart, failure, and multi-tenant contention scenarios.
- Local and pre-commit pressure simulation coverage stays parallel-friendly by avoiding serial multi-seed tests when independent seed cases can be split.
- Cross-feature chaos tests proving flush stalls, retry paths, and budget tightening still preserve deterministic recovery and liveness.
- End-to-end invariant tests proving the subsystem changes performance behavior only, not logical DB outcomes.

**Refactor slices**

1. Slice the observability surfaces first so the whole-system harness can read published snapshots and ordered events without blocking the simulation runtime.
2. Then migrate the remaining in-flight progress assertions to explicit subscriptions or progress probes.
3. Only after those surfaces exist, add the capstone multi-tenant and failure campaigns that compose them.

---

### T76a. Publish simulation-safe scheduler observability snapshots and admission streams

**Depends on:** T76

**Description**

Refactor scheduler/admission observability away from reader-side locking and into published immutable state. This task owns the simulation-facing introspection layer for pressure/admission behavior: snapshots must be cheap to sample from a single-threaded simulated runtime, ordered admission transitions must be assertable without polling shared mutable maps, and the resulting observation surfaces must be easy for the shared T33e/T33f broad-input harnesses to consume without bespoke pressure-only adapters.

**Implementation steps**

1. Replace lock-on-read scheduler observability assembly with published immutable snapshots that writers update whenever the underlying counters or per-domain diagnostics change.
2. Add ordered admission observation streams so tests can assert transitions like `RateLimit -> Open` or "one aggregated event per write" directly, instead of inferring them from eventual shared-state samples.
3. Make current-versus-historical admission state explicit, with the published snapshot carrying the live view and the last-non-open state preserved separately for debugging.
4. Delete polling/try-lock style scheduler snapshot helpers that only existed to avoid deadlocking the simulated runtime, and move simulation tests plus any generated/fuzzed pressure suites onto the published snapshot / stream APIs.
5. Keep the synchronous operator-facing snapshot API as a thin clone of the published state so CLI/debugging callers still have a one-shot read surface.

**Verification**

- Simulation regressions proving in-flight rate-limited writes are observable through the published snapshot subscription without blocking the runtime.
- Simulation regressions proving admission streams emit ordered write-level transitions and do not duplicate per-table diagnostics for one logical write.
- Generated broad-input pressure suites can consume the same published snapshot/stream surfaces without extra lock-backed test hooks.
- Unit tests proving the synchronous snapshot API returns the same data as the published stream state after representative observability updates.
- Unit tests proving the historical last-non-open diagnostic survives recovery writes while the live state returns `Open`.

---

### T76b. Audit and convert remaining simulation-hostile inspection APIs

**Depends on:** T76a

**Description**

Finish the simulation-safety pass across introspection surfaces that may still block the runtime thread or hide ordering behind shared mutable state. This task is broader than admission: it covers any synchronous snapshot or inspection helper that a deterministic simulation test or generated broad-input suite would reasonably want to call while work is in flight.

**Implementation steps**

1. Inventory remaining snapshot/introspection APIs that take `parking_lot` or `std::sync` locks from async contexts or simulation helpers.
2. Convert the high-value surfaces to published immutable snapshots, subscriptions, or explicit poll/step interfaces, depending on whether tests need sampled state, ordered events, or deterministic progress control, and shape those replacements so they plug cleanly into the shared T33e support crate.
3. Remove test-only retry loops that compensate for blocking inspection, and replace them with direct subscription- or progress-driven assertions.
4. Extend the debugging guide with the preferred simulation-safe observation patterns so future work does not regress into blocking shared-state reads.
5. Prioritize the concluding downstream-adoption slices in dependency order:
   - workflow-native wait surfaces first, so async and simulation tests stop polling `load_state()` / `load_source_progress()` with sleeps;
   - projection terminal/frontier waits next, so failure tests stop sleeping before `shutdown()`;
   - relay and downstream crate migrations after that, using native wait surfaces or backing-table watermark publications instead of row-count polling;
   - example-app readiness/progress publication next, so example simulations stop depending on server-startup sleeps or projection polling loops; and
   - simulation-runner startup cleanup last, once the remaining hosts use explicit readiness signals and the driver-side `yield_now()` shim can be deleted.

**Candidate refactor slices**

1. Publish immutable progress snapshots for state that is read frequently but mutated by background work, such as DB progress and resource-manager pressure.
2. Convert ordered transitions to event streams where the test needs the sequence of state changes, not just the latest state.
3. Convert liveness and drain assertions to poll/step helpers where the test needs to prove the background system moved forward.
4. Delete the old blocking reader-side helpers once each replacement slice has landed.

**Verification**

- Targeted simulation tests proving the converted APIs can be used while related work remains in flight.
- A focused audit diff showing the removed blocking inspection paths and the new subscription/poll surfaces that replaced them.
- Debugging-guide updates demonstrating the canonical way to observe runtime state inside deterministic simulations.

---

### T76c. Add deterministic progress probes for background scheduler work

**Depends on:** T76b

**Description**

Expose explicit progress probes for scheduler-driven background work so simulations can assert "one step happened" or "the system is idle" without depending on arbitrary sleeps or repeated `yield_now()`. This task turns background maintenance from a mostly implicit runtime effect into a testable deterministic interface that both hand-written simulation regressions and shared generated/fuzzed suites can drive directly.

**Implementation steps**

1. Identify the highest-value background loops whose progress is currently only inferable through side effects or eventual snapshots.
2. Introduce poll/step helpers or bounded "wait until idle" probes where that can be done without weakening production invariants, and make those helpers reusable from the shared broad-input harness rather than pressure-only tests alone.
3. Rewrite representative simulation tests to use the new probes instead of time-based heuristics.
4. Keep the new probes clearly test/debug oriented so they do not become accidental correctness boundaries in production code.
5. Treat `direct_backlog()` and similar lock-backed helper reads as candidates for this bucket when they are only needed to drive simulation progress or backlog assertions.
6. Prefer a small explicit progress API over incidental `yield_now()` loops whenever the system can expose "made one step" or "fully idle" semantics safely.

**Verification**

- Simulation tests showing background progress can be asserted without arbitrary sleep-based retries.
- Regression coverage for at least one maintenance or scheduler scenario that previously required yield-heavy timing assumptions.
- Generated pressure/fault suites can advance and observe scheduler-driven work through the same explicit probes without bespoke `yield_now()` loops.
- Documentation updates that spell out when to use progress probes vs snapshots vs event streams.

---

## Phase 15 — Opt-in physical sharding, virtual-partition resharding, and shard-local execution

**Parallelization:** T77 first. After that, T78, T79, and T80 can proceed in parallel against the frozen interfaces and shared simulation/oracle seams. T81 depends on T78 + T79 + T80. T82 depends on T78 + T79 + T80 + T81. T83 depends on T78 + T79 + T80 + T81 and can proceed in parallel with T82 once the core sharding surfaces are stable.

**Phase rule:** T77 freezes shard-routing, virtual-partition, and shard-local storage interfaces first, and the rest of the phase should maximize parallel implementation against those fixed seams rather than re-opening the contracts. Physical sharding is a storage-layout and correctness feature; execution domains remain placement and budgeting only. Shard ownership must be defined by catalog/control-plane metadata, not by scheduler placement, domain names, or whichever worker happened to process a request.

**Simulation rule:** Every task in this phase must extend the relevant deterministic oracle/cut-point/simulation harness in the same change as the production-path change. Do not defer task-local verification to the capstone; T82 is additive whole-system hardening, not a substitute for self-contained simulation in T78-T81 and T83.

**Adoption rule:** Treat sharding as opt-in per table, with unsharded tables continuing to use shard `0000` exactly as they do today. The first implementation should prefer conservative, explicit behavior: fixed virtual-partition count at table creation, deterministic hash/routing, fail-closed cross-shard batch rejection, and a conservative reshard cutover for affected partitions rather than an optimistic always-live migration protocol.

**Layout rule:** Anything persisted for a sharded table must record enough virtual-partition coverage to make “move data to another physical shard without rehashing keys or rewriting row/column payloads” real. Do not rely on approximate user-key ranges or best-effort heuristics where exact partition coverage is required for safe resharding.

### T77. Freeze sharding, virtual-partition, and shard-local service contracts

**Depends on:** T23b, T69, T76

**Description**

Define the long-term abstraction for physical per-table sharding before implementation branches diverge. The goal is to freeze the contracts up front: sharded tables opt into a fixed hash-to-virtual-partition function, catalog metadata maps virtual partitions to physical shards, shard-local storage lanes own the actual data, and execution domains remain a placement layer rather than a correctness boundary.

**Implementation steps**

1. Define a `ShardingConfig` / `VirtualPartitionId` / `PhysicalShardId` / `ShardMapRevision`-style contract covering:
   - opt-in sharding in `TableConfig`,
   - fixed virtual-partition count and hash identity at table creation,
   - mutable virtual-partition-to-physical-shard mapping metadata, and
   - unsharded compatibility via shard `0000`.
2. Freeze the routing contract for reads, writes, and batches:
   - how a key resolves to a virtual partition,
   - how that partition resolves to a physical shard,
   - how `WriteBatch` grouping validates shard locality, and
   - how cross-shard batches on the same sharded table fail closed.
3. Freeze the shard-local storage/control-plane seams for:
   - commit-log lane identity,
   - shard-local memtables and SSTable ownership,
   - manifest/catalog metadata for sharded tables,
   - per-shard recovery/open hooks, and
   - change-feed / cursor / `CommitId` behavior for sharded tables.
4. Freeze the partition-locality/layout contract needed to support no-rewrite resharding, for example by requiring partition-bounded flush/compaction outputs or equally precise virtual-partition coverage metadata on persisted artifacts.
5. Freeze the execution-domain integration seam so shard-local foreground/background/control-plane work can be placed in domains such as `db.orders.shard_3.foreground` without letting domain assignment define logical shard ownership.
6. Add shared routing/oracle/simulation scaffolding immediately for:
   - stable key-to-partition mapping,
   - mapping revision changes,
   - shard-local failure/recovery cut points, and
   - reshard-plan skeletons that later tasks will extend rather than re-invent.
7. Add compile-time stubs and placeholder types so routing, storage, scheduler, and resharding work can proceed in parallel against the same fixed interfaces.

**Verification**

- Compile-only tests proving the new sharding contracts compose with `TableConfig`, `WriteBatch`, `CommitId`, manifest metadata, scheduler/domain APIs, and remote-storage metadata.
- Deterministic unit tests proving the hash-to-virtual-partition mapping is stable across reruns and independent of execution-domain placement.
- Oracle tests for stub routing and reshard-plan helpers before production storage logic lands.
- Simulation smoke tests that create sharded and unsharded tables together, route synthetic traffic, and verify identical same-seed traces across replay.

---

### T78. Implement sharded table metadata, routing, and batch-locality validation

**Depends on:** T77

**Description**

Implement the table-level control plane for sharding: durable metadata, key routing, and commit-time validation. This task owns the logical routing surface and catalog persistence, not the shard-local storage engine internals.

**Implementation steps**

1. Extend the shared simulation/oracle harness first with a concrete reference model for:
   - fixed key-to-virtual-partition routing,
   - virtual-partition-to-physical-shard lookup by revision,
   - unsharded `0000` compatibility, and
   - fail-closed cross-shard batch detection.
2. Extend `TableConfig` and catalog persistence to support opt-in sharding, including:
   - virtual-partition count,
   - hash identity/configuration,
   - current shard-map revision, and
   - initial physical-shard assignment metadata.
3. Implement durable control-plane APIs for loading, validating, and atomically publishing shard maps for a table without yet moving data.
4. Implement the routing layer that resolves each read/write key to a virtual partition and then to a physical shard using the persisted shard map.
5. Implement `WriteBatch` grouping and validation so a sharded table rejects batches that would span multiple physical shards of that table, while preserving existing behavior for unsharded tables and for batches that touch multiple different tables.
6. Add introspection for table sharding state, effective shard-map revision, partition counts per physical shard, and explicit reasons a batch was rejected for violating shard locality.

**Verification**

- Catalog/restart tests proving sharding metadata survives reopen exactly and unsharded tables continue to default to shard `0000`.
- Routing tests proving the same key always resolves to the same virtual partition and physical shard for a fixed mapping revision.
- Validation tests proving cross-shard batches on a sharded table fail closed while legal single-shard batches continue to commit.
- Deterministic simulation tests, landed in the same task, covering mixed sharded/unsharded workloads, mapping reload on restart, and repeated same-seed routing behavior.

---

### T79. Implement shard-local commit lanes, partition-aware storage layout, and recovery

**Depends on:** T77

**Description**

Implement the shard-local data path for sharded tables: independent commit-log lanes, memtables, flush outputs, and recovery state per physical shard. This task owns making shard `0000` the compatibility path and additional shard directories the real execution path for sharded tables.

**Implementation steps**

1. Extend the deterministic simulation/oracle harness first so it can model shard-local:
   - commit-log lanes,
   - mutable/immutable memtables,
   - flush/recovery cut points,
   - `CommitId` / cursor behavior, and
   - partition-coverage metadata on persisted artifacts.
2. Implement per-shard local-storage and remote-storage layout for sharded tables using the architecture's shard directories, while preserving shard `0000` as the exact compatibility path for unsharded data.
3. Implement shard-local commit-lane append/read machinery, sequence/cursor handling, and memtable ownership according to the T77 contracts.
4. Implement partition-aware flush/compaction outputs so persisted artifacts are either partition-bounded or carry exact virtual-partition coverage metadata sufficient for later reshard movement without rewriting user payload bytes.
5. Implement per-shard manifest/recovery/open behavior, including deterministic reconstruction of shard-local state after crash or restart.
6. Preserve the single-shard fast path so a table that remains on `0000` does not pay unnecessary complexity costs in the steady state.

**Verification**

- Storage-layout tests proving shard-local directories, manifests, and remote object keys are written and reopened consistently for both sharded tables and shard-`0000` compatibility tables.
- Concurrency tests proving traffic to independent physical shards can make progress without corrupting each other's shard-local state.
- Crash/recovery tests at shard-local append, flush, publish, and reopen cut points proving recovery fails closed and reconstructs the same shard-local state deterministically.
- Deterministic simulation tests, introduced in the same task, covering repeated same-seed shard-local write/flush/restart behavior and verifying persisted partition coverage matches the oracle.

---

### T80. Make scheduler, maintenance, pressure control, and observability shard-aware

**Depends on:** T77

**Description**

Teach the runtime controls to treat physical shards as first-class work units without conflating shard ownership with domain placement. This task owns shard-local scheduling, maintenance, pressure accounting, and diagnostics.

**Implementation steps**

1. Extend the deterministic scheduler/admission harness first with shard-local foreground/background/control-plane work items, hot-shard contention, and domain-aware shard placement scenarios before the production integration lands.
2. Extend pending-work, scheduler, and maintenance pipelines so shard-local flush, compaction, backup, offload, and recovery work carry physical-shard identity plus optional execution-domain placement metadata.
3. Integrate sharding with the resource-manager and pressure-aware admission work so one hot shard can be throttled or drained without implicitly redefining logical shard ownership or starving protected control-plane work.
4. Route shard-map publication, reshard-plan metadata, and other recovery-critical sharding control work through the protected control-plane domain and internal durability lane when enabled.
5. Add per-shard observability for backlog, pressure, mutable memory, unified-log pinning, flush/compaction debt, and current placement decisions.
6. Keep the default profile conservative: simple single-DB and single-shard embeddings should continue to work without explicit shard-aware domain configuration.

**Verification**

- Deterministic tests proving shard-aware scheduling changes resource distribution and backlog behavior but not logical DB outcomes.
- Mixed-workload simulation tests where one hot shard cannot starve unrelated shards or the protected control-plane path.
- Pressure/accounting tests proving per-shard stats remain bounded and reconstruct deterministically after restart.
- Domain-equivalence tests proving moving a shard's work between execution domains changes placement/isolation behavior only, not routing or commit results.

---

### T81. Implement virtual-partition resharding and conservative cutover without data rewriting

**Depends on:** T78, T79, T80

**Description**

Implement resharding as a control-plane change plus physical artifact movement, not a key rewrite. This task owns the conservative first cut of reassignment: move virtual partitions between physical shards by updating mapping metadata and moving the affected persisted state, while keeping the protocol deterministic, restartable, and fail closed.

**Implementation steps**

1. Extend the shared oracle/simulation harness first with:
   - virtual-partition reassignment plans,
   - source/target physical shards,
   - mapping revision cutover,
   - crash/restart during movement, and
   - validation that user keys and row/column payload bytes are not rehashed or rewritten.
2. Implement durable reshard-plan metadata and control-plane APIs covering plan creation, in-flight status, revision publication, completion, and explicit abort/failure reporting.
3. Implement a conservative cutover protocol for affected virtual partitions that prioritizes correctness first, for example by briefly quiescing writes to the affected partitions while their shard-local state is moved and the new mapping revision is published.
4. Move persisted shard-local artifacts by virtual-partition coverage from the source shard directory to the target shard directory without rehashing keys or re-encoding row/column payload bytes.
5. Ensure restart/recovery can resume, roll back, or fail closed on interrupted reshard plans without producing split ownership, duplicate visibility, or silent data loss.
6. Add operator-facing introspection for current shard-map revision, partitions in motion, bytes/artifacts moved, cutover progress, and any partitions temporarily paused for correctness.

**Verification**

- Resharding tests proving the same logical data is visible before and after reassignment and that keys retain the same virtual-partition identity throughout.
- Crash/restart tests proving interrupted reshard plans resume or fail closed without split-brain ownership between source and target shards.
- Structural tests proving affected artifacts move between shard directories without user-key rehashing or payload rewrite.
- Deterministic simulation tests, added with the task, covering hot-partition reassignment, restart during cutover, mapping revision replay, and mixed sharded/unsharded workloads.

---

### T82. Build the capstone whole-system simulation and chaos suite for physical sharding

**Depends on:** T78, T79, T80, T81

**Description**

Add the post-implementation cross-cutting hardening pass for the sharding subsystem. This task owns the whole-system deterministic matrix across routing, shard-local storage, scheduler/resource domains, resharding, crash/recovery, and mixed sharded/unsharded operation. It does not replace task-local simulation; it verifies that the composed system still behaves correctly when all of those features interact.

**Implementation steps**

1. Compose the per-task sharding oracle helpers into a whole-system harness that can run:
   - multiple sharded tables,
   - mixed sharded and unsharded tables,
   - hot-shard skew,
   - resharding under load, and
   - restart/recovery during or after mapping changes.
2. Add long-running deterministic campaigns that combine:
   - write-heavy and scan-heavy shard skews,
   - shard-local flush/compaction/offload pressure,
   - control-plane shard-map churn,
   - execution-domain reassignment of shard-local work, and
   - reshard cutovers while unrelated shards continue serving traffic.
3. Add a real-runtime chaos layer where appropriate, including injected shard-local stalls, delayed control-plane publication, and movement interruptions that complement the deterministic harness without weakening reproducibility requirements.
4. Verify the full invariants matrix:
   - routing remains stable for a fixed mapping revision,
   - execution-domain movement changes placement only, not ownership,
   - resharding preserves logical contents without key rehashing or payload rewrite,
   - shard-local failures remain isolated and recover deterministically, and
   - unsharded tables continue to behave exactly as before on shard `0000`.

**Verification**

- Large-seed deterministic simulation campaigns proving physical sharding and resharding remain reproducible across restart, failure, and hot-shard scenarios.
- Cross-feature chaos tests proving mapping publication, shard-local stalls, and movement interruptions still preserve deterministic recovery and fail-closed behavior.
- End-to-end invariant tests proving mixed sharded/unsharded deployments change parallelism and placement behavior only where expected, not logical DB results.

---

### T83. Build a small example app that demonstrates a sharded database

**Depends on:** T78, T79, T80, T81

**Description**

Add a sibling example to `examples/todo-api` that demonstrates the intended sharded-table model with a simplified workload. A good fit here is a small `chat-rooms-api`: each room is an independent entity, every write is naturally scoped to one room, messages and room state can share the same shard key, and resharding can move busy rooms without teaching users a complicated multi-entity transaction story.

**Implementation steps**

1. Extend the phase-local simulation/example harness first with an example-oriented workload model covering:
   - many rooms spread across virtual partitions,
   - a few hot rooms that create shard skew,
   - room-local writes that must stay single-shard, and
   - a simple reshard operation that moves a hot room's virtual partition to another physical shard.
2. Create `examples/chat-rooms-api` with a minimal HTTP surface and README, using the TODO example's structure as a template while keeping the data model sharding-friendly:
   - a sharded `rooms` or `room_state` table keyed by `room_id`,
   - a sharded `messages` table keyed so all messages for a room live with that room, and
   - typed helpers that make the shard key explicit in application code.
3. Add the smallest API that still teaches the sharding model:
   - create a room,
   - post a message to one room,
   - read recent messages for one room,
   - inspect which physical shard a room currently maps to, and
   - trigger or simulate a reshard of one room's virtual partition for demo/testing purposes.
4. Surface observability that shows:
   - the room-to-virtual-partition-to-physical-shard mapping,
   - shard-local backlog/pressure,
   - any temporary pause during conservative cutover, and
   - optional execution-domain placement for shard-local work.
5. Document clearly:
   - why room-scoped batches are valid,
   - why cross-room batches are intentionally not the teaching path,
   - how unsharded tables would differ, and
   - that execution domains affect placement/isolation while shard maps define logical ownership.

**Verification**

- End-to-end deterministic simulation tests for the example workload proving room-local writes/readbacks remain correct across hot-shard skew and resharding.
- Example integration tests proving shard introspection, conservative cutover, restart/reopen, and shard-`0000` compatibility all behave as documented.
- Example-level equivalence tests proving changing execution-domain placement alters latency/backlog behavior but not room contents, while resharding changes ownership/location without changing logical answers.

---

## Phase 16 — Historical workflow bootstrap, replay, and recovery

**Parallelization:** T84 must land first to freeze the source/recovery interfaces. After that, T85 and T86 can proceed in parallel: one owns runtime/bootstrap plumbing, the other owns checkpoint/export/restore. T87 follows once both surfaces exist and composes them into deterministic historical-recovery suites.

### T84. Freeze historical workflow source contracts, recovery policies, and shared simulation/oracle seams

**Depends on:** T32, T32a

**Description**

Freeze the public and internal interfaces for workflow sources that may attach to history, skip backlog, restore from checkpoints, or replay append-only sources. This task exists to maximize later parallelism: runtime plumbing, checkpoint storage, and replay logic should all target stable contracts rather than redefining source progress independently.

**Implementation steps**

1. Define workflow source configuration contracts for:
   - bootstrap policy,
   - recovery policy,
   - source-progress encoding,
   - replayable-source capabilities, and
   - optional trigger-journal/checkpoint support.
2. Decide the persisted representation of workflow source progress so it can express more than the current bare durable cursor when needed.
3. Freeze the `WorkflowCheckpointStore` / equivalent trait boundary and manifest/artifact naming expectations for checkpoint-capable workflows.
4. Define the simulation/oracle seams needed for later tasks:
   - historical attach vs live-only attach,
   - `SnapshotTooOld` on workflow sources,
   - checkpoint restore,
   - append-only source replay, and
   - lossy fast-forward policies.
5. Document the default safety stance explicitly: fail closed unless a weaker recovery mode is deliberately selected.

**Verification**

- API tests proving bootstrap and recovery policies are constructible and survive round-trip config handling.
- Progress-encoding tests proving persisted source progress round-trips and preserves ordering semantics.
- Deterministic simulation smoke tests covering empty state, beginning bootstrap, and current-durable bootstrap without yet implementing the full runtime behavior.
- Oracle scaffolding tests proving historical replay scenarios can be expressed reproducibly in the shared simulation harness.

---

### T85. Implement workflow source bootstrap and recovery-policy plumbing

**Depends on:** T32, T84

**Description**

Implement the runtime-level behavior for workflow source bootstrap and recovery policies. This task owns the behavior difference between “replay history from the beginning,” “attach live from the current durable frontier,” and “fail closed or fast-forward when old cursors are no longer resumable.”

**Implementation steps**

1. Extend workflow source admission to load source configuration rather than assuming all sources always begin at `LogCursor::beginning()`.
2. Implement first-attach bootstrap modes:
   - beginning,
   - current durable,
   - checkpoint-or-beginning, and
   - checkpoint-or-current-durable.
3. Implement recovery-policy dispatch when a persisted source cursor becomes too old to resume.
4. Preserve the rule that existing durable local workflow state (inbox, timers, outbox, state rows) is always resumed before any source bootstrap decision is applied.
5. Surface recovery decisions clearly in runtime state/telemetry so operators can distinguish historical replay, live-only attach, checkpoint restore, and fail-closed behavior.

**Verification**

- Deterministic tests proving `current-durable` bootstrap skips historical backlog but still processes new events.
- Deterministic tests proving `beginning` bootstrap replays retained history in durable order.
- Recovery tests proving `fail-closed` surfaces `SnapshotTooOld` without silently fast-forwarding.
- Simulation tests proving restart with existing inbox/timer/outbox state resumes local durable work before source bootstrap logic runs.

---

### T86. Implement workflow checkpoints, restore, and optional trigger journaling

**Depends on:** T22, T23, T84

**Description**

Implement the durable checkpoint/export/restore path for workflow-owned state and the optional trigger journal needed for workflows that cannot be reconstructed from source history alone. This task is intentionally independent of the bootstrap-policy plumbing from T85 so both can proceed in parallel after contracts are frozen.

**Implementation steps**

1. Implement checkpoint capture for workflow-owned tables:
   - state,
   - inbox,
   - trigger order,
   - source progress,
   - timer schedule/lookup, and
   - outbox.
2. Implement checkpoint manifest publication and artifact storage with the same fail-closed durability discipline used elsewhere in Terracedb.
3. Implement checkpoint restore on runtime open, including clearing and repopulating workflow-owned tables atomically enough for crash-safe reopen semantics.
4. Add optional admitted-trigger journaling for workflows that need to preserve timer/callback/source trigger history beyond what source replay alone can reconstruct.
5. Expose enough metadata for later replay logic to know what source frontier and trigger-journal point a checkpoint represents.

**Verification**

- Crash/recovery simulation tests around checkpoint capture, manifest publication, and restore.
- Deterministic restore tests proving restored workflow-owned state matches the captured checkpoint exactly.
- Trigger-journal tests proving admitted triggers can be replayed deterministically after restart.
- Simulation tests proving partial/incomplete checkpoints fail closed and never produce mixed restored state.

---

### T87. Add deterministic historical-workflow replay and recovery suites

**Depends on:** T33, T85, T86

**Description**

Compose the new workflow-historical surfaces into deterministic replay and recovery suites. This is the phase-local capstone: it does not replace task-local tests, but it proves the historical bootstrap, checkpoint, trigger-journal, and lossy/live-only policies behave coherently when combined.

**Implementation steps**

1. Add scenario generators that vary:
   - beginning vs current-durable bootstrap,
   - fail-closed vs restore-checkpoint vs replay-from-history vs fast-forward recovery,
   - append-only vs non-replayable source tables, and
   - presence/absence of timers, callbacks, and pending outbox work.
2. Extend the workflow oracle to reason about:
   - skipped historical work under live-only attach,
   - restored checkpoint state,
   - replayed trigger journals, and
   - intentionally lossy fast-forward behavior.
3. Add restart campaigns that inject `SnapshotTooOld`, crashes during restore, and duplicate timer/callback delivery around historical replay.
4. Verify that replay-capable append-only sources can rebuild correctly while non-replayable sources fail closed or follow the explicitly selected lossy policy.

**Verification**

- Large-seed deterministic simulation suites covering historical replay, checkpoint restore, and live-only attachment.
- Cross-cutting replay tests proving append-only replayable sources rebuild workflow progress correctly across crash/restart.
- Negative tests proving current-state-only sources cannot silently opt into replay-from-history semantics.
- Reproducibility tests proving the same seed reproduces bootstrap and recovery choices exactly.

---

## Phase 17 — Kafka ingress crate and deterministic broker simulation

**Parallelization:** T88 lands first to freeze the ingress interfaces. After that, T89 and T90 can proceed mostly independently: T89 owns durable source progress and partition-worker control flow, while T90 owns table layouts/materialization helpers. T91 composes both into a deterministic broker simulation and ingress hardening suite.

### T88. Freeze the `terracedb-kafka` crate boundary, broker abstraction, and offset/progress contracts

**Depends on:** T28, T32a

**Description**

Freeze the contract for a Kafka ingress library that materializes ordinary Terracedb tables and persists source progress atomically with those writes. The goal is to prevent the later runtime and materialization work from disagreeing about offset storage, partition ownership, bootstrap semantics, or record-filter behavior.

**Implementation steps**

1. Define the public `terracedb-kafka` crate boundary, including:
   - source/partition definitions,
   - bootstrap policy (`earliest` / `latest` or equivalent),
   - deterministic record-filter contracts,
   - record/batch handler traits,
   - offset/progress storage contracts, and
   - worker lifecycle/shutdown semantics.
2. Freeze the broker abstraction used by tests and simulation so later tasks can support both a production Kafka adapter and a deterministic simulated broker without changing the higher-level runtime.
3. Define supported Terracedb materialization layouts up front:
   - one table per partition,
   - shared `(partition, offset)` table, and
   - any helper types needed to make replay-sensitive ordering explicit.
4. Decide the telemetry/debug surface for partition lag, applied offsets, filtered/skipped record counts, and restart/bootstrap decisions.
5. Add the shared simulation/oracle seams needed for later tasks: restart from offset, duplicate delivery, filtered-but-acknowledged records, partition-local ordering, rebalance/claim changes if supported, and crash between write and offset persist.

**Verification**

- API tests proving the new crate surface compiles without a concrete broker implementation.
- Offset/progress round-trip tests proving persisted ordering semantics are stable.
- Deterministic simulation smoke tests using a stub broker abstraction to prove the runtime can be driven without real Kafka.
- Deterministic smoke tests proving filtered records are modeled as intentional skips rather than as delivery failures.
- Layout tests proving the partition/materialization policies can be selected without changing core interfaces.

---

### T89. Implement durable Kafka source-progress storage, bootstrap, and partition workers

**Depends on:** T88

**Description**

Implement the runtime that consumes partitioned Kafka batches, applies them through a transaction, and persists offsets in the same OCC unit. This task owns source-progress correctness, deterministic filtering, and restart/bootstrap behavior for Kafka ingress.

**Implementation steps**

1. Implement the persisted offset/progress store with one durable entry per `(consumer-group, topic, partition)` or equivalent identity.
2. Implement bootstrap behavior for earliest/latest attachment using the persisted source-progress contract from T88.
3. Implement partition workers that:
   - load progress,
   - fetch a batch,
   - evaluate deterministic filters,
   - open a Terracedb transaction,
   - apply retained records through the handler,
   - persist the next offset in the same transaction, and
   - commit atomically.
4. Ensure skipped/filtered records still advance source progress intentionally, while restart and worker handoff behavior never advances offsets without the corresponding retained Terracedb writes becoming visible.
5. Surface typed runtime errors for broker failure, decode failure, storage failure, and aborted transaction/retry.

**Verification**

- Restart tests proving the runtime resumes from the persisted offset rather than re-reading from the wrong bootstrap point.
- Crash simulation tests proving offsets do not advance unless the materialized writes commit.
- Duplicate-delivery tests proving retried batches can be handled deterministically when the handler is written appropriately.
- Deterministic tests proving filtered records are skipped reproducibly and do not reappear after restart.
- Deterministic simulation tests proving partition-local ordering is preserved across restart and worker retry.

---

### T90. Implement Kafka materialization helpers and ordering-preserving table layouts

**Depends on:** T88, T89

**Description**

Implement reusable helpers for mapping Kafka records into Terracedb tables while preserving the ordering guarantees that downstream projections and workflows need. This task is separate from T89 so table-layout/materialization choices, filtered materialization surfaces, and worker/offset runtime concerns do not become entangled.

**Implementation steps**

1. Implement helper builders for:
   - one table per partition, and
   - shared `(partition, offset)` key layouts.
2. Provide utilities for encoding Kafka partition/offset into deterministic Terracedb keys suitable for replay-sensitive append-only sources.
3. Add convenience helpers for current-state mirrors built from Kafka records where the handler wants both append-only and current-state outputs.
4. Add helper patterns for pairing filtered append-only outputs with narrower current-state mirrors, while keeping the filtering semantics explicit in the API.
5. Ensure materializers can write multiple tables in one transaction while still persisting the source offset atomically.
6. Document when each materialization layout is appropriate for projections, workflows, live-only current-state consumers, and aggressively filtered large-stream use cases.

**Verification**

- Key-ordering tests proving encoded partition/offset keys preserve the intended replay order.
- Transaction tests proving multiple materializations plus offset persistence commit atomically.
- Deterministic simulation tests proving downstream table scans replay in the same order as the source partition stream.
- Tests proving filtered append-only and current-state outputs stay aligned at the same applied offset frontier.
- Current-state mirror tests proving append-only and mirror writes stay aligned at the same applied offset frontier.

---

### T91. Add deterministic Kafka ingress simulation and runtime hardening suites

**Depends on:** T33, T89, T90

**Description**

Add the Kafka-ingress-specific deterministic test matrix. This task is the phase-local hardening pass for ingress semantics: source progress, ordering, duplicate delivery, restart, and worker failure under a deterministic broker model.

**Implementation steps**

1. Implement a deterministic simulated broker host or adapter capable of:
   - partitioned ordered delivery,
   - repeated delivery,
   - delayed delivery,
   - fetch failure, and
   - restartable source progress.
2. Add ingress-specific oracles for partition-local ordering, applied-offset frontiers, filtered/skipped record accounting, and atomicity between materialized writes and offset persistence.
3. Run multi-seed campaigns covering batch retries, worker restarts, offset corruption/failure, filtered batches, and mixed append-only/current-state materialization.
4. Add regression suites for the exact failure window where records are materialized but the next offset has not yet committed.

**Verification**

- Large-seed deterministic simulations proving applied offsets and Terracedb writes stay in lockstep.
- Ordering tests proving partition-local replay remains deterministic across retries and restart.
- Filtering tests proving skipped records still advance source progress and never materialize accidentally.
- Failure-window tests proving no offset is durably advanced ahead of its writes.
- Reproducibility tests proving the same seed yields the same broker delivery/order/failure trace.

---

## Phase 18 — Debezium crate, materialization modes, and runtime integration

**Parallelization:** T92 lands first to freeze Debezium-specific contracts and materialization modes. T93 then implements connector-envelope decoding. Once the decoder contracts are stable, T94 can implement EventLog / Mirror / Hybrid materialization and the helper surfaces that projections and workflows consume. T95 closes the phase with deterministic Debezium integration suites.

### T92. Freeze the `terracedb-debezium` crate boundary, envelope normalization contracts, and materialization modes

**Depends on:** T31, T84, T88, T90, T91

**Description**

Freeze the Debezium-on-Kafka interfaces before implementation splits across decoder, materializer, and runtime-integration tasks. This task should explicitly codify the EventLog / Mirror / Hybrid modes described in the architecture, along with the schema/table/row-filter contracts and the bootstrap/replay expectations that workflows and projections may rely on.

**Implementation steps**

1. Define the `terracedb-debezium` crate boundary and the normalized Debezium event model consumed by materializers.
2. Freeze the semantics of the three materialization modes:
   - EventLog,
   - Mirror, and
   - Hybrid.
3. Define first-class filtering contracts for:
   - schema/table selection,
   - deterministic row predicates over normalized events, and
   - optional column projection/redaction before writing Terracedb values.
4. Define the supported connector-specific metadata surface for the first connector target (PostgreSQL), including:
   - snapshot markers,
   - tombstones,
   - operation kinds,
   - source identifiers, and
   - transaction metadata where supported.
5. Define helper contracts for projections/workflows, such as snapshot-event filtering, source-table layout builders, and policies for routing workflows from raw CDC vs derived transition tables.
6. Add deterministic fixture/simulation seams for normalized envelopes, table/row filters, snapshot batches, deletes/tombstones, and transaction ordering.

**Verification**

- Contract tests for the normalized Debezium event types and materialization-mode selection.
- Fixture tests proving representative normalized envelopes round-trip cleanly through the public surface.
- Contract tests proving schema/table and row-filter contracts evaluate over stable normalized fields.
- Simulation smoke tests proving snapshot and tombstone semantics can be expressed before the full runtime exists.
- Tests proving the workflow/projection helper contracts do not require direct Kafka access.

---

### T93. Implement PostgreSQL Debezium envelope decoding and normalization

**Depends on:** T92

**Description**

Implement decoding and normalization for PostgreSQL Debezium messages first. This task owns the envelope semantics that every later Debezium materialization relies on, so it should be explicit and fail closed rather than loosely mapping JSON into ad hoc application data.

**Implementation steps**

1. Implement decoding of PostgreSQL Debezium change events into the normalized event model.
2. Normalize operation kinds, before/after payloads, primary-key extraction, snapshot markers, tombstones, and source metadata.
3. Ensure the normalized event surface exposes the stable fields needed for deterministic schema/table filtering, row filtering, and optional column projection/redaction.
4. Add support for transaction metadata when present, but keep row-event materialization sound even when the upstream deployment does not emit a separate transaction topic.
5. Fail closed on malformed/unsupported payloads rather than silently dropping fields or guessing semantics.
6. Add deterministic fixtures for inserts, updates, deletes, snapshot rows, tombstones, transaction metadata, and rows that transition into or out of a filter predicate.

**Verification**

- Fixture-driven decode tests for inserts, updates, deletes, snapshot rows, and tombstones.
- Negative tests proving malformed payloads fail closed with structured errors.
- Filter-surface tests proving normalized events expose stable schema/table/value fields for predicate evaluation.
- Deterministic simulation tests proving decode output is stable and reproducible from the same input fixture stream.
- Transaction-metadata tests proving normalized ordering metadata is preserved where available.

---

### T94. Implement Debezium EventLog / Mirror / Hybrid materializers and runtime integration helpers

**Depends on:** T84, T92, T93

**Description**

Implement the actual Debezium materializers and the helper surfaces needed by projections and workflows. This task should make the trade-offs between replayable append-only history, filtering for large databases, and space-efficient current-state mirrors explicit rather than implicit.

**Implementation steps**

1. Implement **EventLog** materialization into append-only ordered `*_cdc` tables suitable for replay-sensitive projections and historical workflow sources.
2. Implement **Mirror** materialization into PK-keyed `*_current` tables suitable for current-state reads and live-only workflow attachment.
3. Implement **Hybrid** materialization that writes both surfaces atomically with Kafka source-progress persistence.
4. Implement schema/table filtering, row-filter application, and mirror membership-transition handling (`false -> true`, `true -> false`, etc.) so filtered current-state tables never retain stale rows.
5. Add helper builders/utilities for:
   - one-table-per-partition Debezium source layouts,
   - snapshot-event filtering or routing policies for workflows,
   - projection source declarations over partitioned Debezium tables, and
   - transition-friendly patterns that encourage workflows to consume semantic derived tables rather than raw row-level CDC where appropriate.
6. Document the limitations clearly: mirror-only sources are not sufficient for general historical replay after history loss, and filtering before EventLog materialization removes the filtered-out subset from Terracedb replay history by design.

**Verification**

- Transaction tests proving EventLog/Mirror/Hybrid materializations commit atomically with source progress.
- Ordering tests proving EventLog keys preserve deterministic replay order.
- Filtering tests proving schema/table filters and row predicates retain only the intended subset, while mirror membership transitions remove rows that stop matching.
- Snapshot/tombstone tests proving current-state mirrors and append-only logs behave as documented.
- Deterministic simulation tests proving projections and workflows can consume the resulting tables without direct broker access.

---

### T95. Add deterministic Debezium integration suites for projections and workflows

**Depends on:** T33, T93, T94

**Description**

Add the Debezium-specific deterministic hardening pass. This phase-local capstone should verify that Debezium EventLog / Mirror / Hybrid materializations and filtering behavior interact correctly with projection rebuild rules and workflow bootstrap/recovery policies.

**Implementation steps**

1. Add end-to-end deterministic suites that drive normalized Debezium fixtures through Kafka ingress into Terracedb tables, then into projections and workflows.
2. Cover snapshot-heavy startup, tombstones, reorder-resistant partition-local delivery, mixed EventLog/Mirror/Hybrid materialization modes, and schema/table/row filtering.
3. Verify projection behavior for:
   - history-sensitive EventLog consumers, and
   - current-state-only mirror consumers.
4. Verify workflow behavior for:
   - beginning bootstrap on replayable EventLog sources,
   - current-durable live-only attach on mirror sources, and
   - fail-closed behavior when replay is requested from non-replayable sources.

**Verification**

- Deterministic simulation suites proving EventLog projections rebuild correctly from Debezium-derived append-only tables.
- Deterministic simulation suites proving mirror-backed live-only workflows skip backlog but process new changes correctly.
- Filtering suites proving ignored tables and rows never materialize while retained rows still replay deterministically.
- Negative tests proving non-replayable mirror sources cannot silently satisfy replay-from-history recovery policies.
- Reproducibility tests proving the same Debezium fixture stream and seed yield the same projection/workflow outcomes.

---

## Phase 19 — End-to-end CDC hardening and example app

**Parallelization:** T96 lands first to freeze the example boundary, cross-cutting harness, and oracle seams. T97 then implements the example app itself. T98 follows once the example and the three underlying feature phases exist, T99 hardens the example with restart/fault suites and polished documentation, and T99a follows once that CDC stack and the broader HTTP/VFS/sandbox-facing surfaces needed by the omnistack harness are available.

### T96. Freeze the end-to-end CDC harness, oracle seams, and example-app boundary

**Depends on:** T87, T91, T95

**Description**

Freeze the final integration target before building the capstone app and tests. This task defines the end-to-end harness, the example scenario, and the cross-cutting invariants so the final work can proceed in parallel-free but low-churn fashion.

**Implementation steps**

1. Define the end-to-end harness boundary that composes:
   - deterministic broker delivery,
   - Kafka ingress,
   - Debezium normalization/materialization,
   - projections,
   - workflows, and
   - application-facing reads/outbox effects.
2. Freeze the example scenario and table layout for a small CDC-driven app, including explicit table-level and row-level filtering choices.
3. Extend the oracle seams to reason across:
   - source offsets,
   - Debezium materializations,
   - projection frontiers,
   - workflow bootstrap/recovery choices, and
   - example-visible outputs.
4. Document the exact historical-vs-live-only behaviors the example must demonstrate.

**Verification**

- Harness smoke tests proving the full stack can be wired together deterministically.
- Oracle tests proving end-to-end invariants can be checked from a single seed-driven run.
- Example-boundary tests proving the chosen scenario exercises filtering, Debezium, projections, and workflows together.

---

### T97. Build a small CDC example app (`order-watch`)

**Depends on:** T96

**Description**

Build a small example app that demonstrates the new feature set with a simplified but realistic use case. The recommended scenario is `order-watch`: Debezium captures a larger commerce database on Kafka, Terracedb filters that stream down to the `public.orders` table and then further to only the watched subset of rows (for example `region == "west"`), materializes both append-only and current-state views, uses a projection to derive “orders needing attention,” and runs a workflow that emits one alert per order transition. The example should make both the filtering behavior and the difference between historical bootstrap and live-only attach visible.

**Implementation steps**

1. Create a new example app crate/directory that wires together:
   - Kafka ingress,
   - Debezium EventLog / Mirror / Hybrid materialization,
   - explicit table filtering that ignores non-`orders` tables from the larger source database,
   - explicit row filtering that keeps only the watched `orders` subset,
   - one projection that derives an `attention_orders` view or transition stream, and
   - one workflow that emits durable outbox alerts.
2. Provide a minimal application-facing read surface showing:
   - the current mirrored order row,
   - the derived attention/read-model output, and
   - the workflow/outbox-visible alert state, and
   - that filtered-out tables/rows never appear in Terracedb-visible outputs.
3. Add configuration toggles or profiles that demonstrate:
   - historical workflow bootstrap from EventLog history, and
   - live-only workflow attachment from the current durable frontier.
4. Include fixtures or scripted inputs that show:
   - an ignored non-`orders` table event,
   - an `orders` row that is filtered out,
   - an `orders` row that is retained, and
   - a retained row that later leaves the watched subset.
5. Keep the scenario intentionally small and typed, reusing `terracedb-records` where it improves clarity.

**Verification**

- Example integration tests proving the projection and workflow outputs match the documented `order-watch` scenario.
- Example integration tests proving table-level and row-level filtering produce the documented retained vs ignored outcomes.
- Deterministic simulation tests proving historical bootstrap and live-only attach produce intentionally different but reproducible outcomes.
- Restart tests proving the example resumes correctly from persisted ingress/projection/workflow state.
- Example-level tests proving EventLog and Hybrid modes expose the same logical attention/alert outputs while Mirror-only mode is limited as documented.

---

### T98. Add cross-cutting deterministic simulation suites for historical workflows + Kafka + Debezium

**Depends on:** T33, T96, T97

**Description**

Add the post-implementation cross-cutting deterministic test matrix that composes the three new feature areas. This is the place to verify their interaction as one subsystem: historical workflow bootstrap/recovery, Kafka source progress, Debezium materialization modes, projections, and example-visible outputs under crash/restart and history loss.

**Implementation steps**

1. Build long-running deterministic campaigns that vary:
   - workflow bootstrap/recovery policy,
   - Kafka bootstrap policy and batch boundaries,
   - table/row filtering policy,
   - Debezium EventLog / Mirror / Hybrid mode,
   - projection rebuild vs normal tailing, and
   - crash/restart at ingress, projection, and workflow cut points.
2. Extend the oracle to assert cross-cutting invariants such as:
   - offsets never outrun materialized tables,
   - filtered-out tables and rows never appear in Terracedb-visible state,
   - EventLog replay reproduces historical projection/workflow results,
   - Mirror-only live attach intentionally skips backlog,
   - checkpoint restore and append-only replay converge to the same state where both are supported, and
   - example-visible outputs remain deterministic under restart and duplicate delivery.
3. Add failure campaigns for `SnapshotTooOld`, checkpoint corruption, broker retry, duplicate Debezium delivery, and mixed replay/live-only workflow sources.

**Verification**

- Large-seed deterministic suites covering the full Kafka + Debezium + projection + workflow stack.
- Cross-cutting restart/recovery tests proving the combined system remains fail-closed or explicitly lossy according to configuration, never ambiguously in between.
- Filtering-aware regression tests proving filter configuration changes only the intentionally retained subset and does not break offset/progress correctness.
- Reproducibility tests proving failing seeds preserve the full end-to-end trace needed for local replay.
- Oracle-backed tests proving example-visible behavior matches the intended historical/live-only semantics.

---

### T99. Harden the CDC example with restart/fault suites and polished docs

**Depends on:** T97, T98

**Description**

Finish the capstone work by hardening the new example app under failure/restart and documenting it as the reference implementation pattern for Kafka + Debezium + projections + workflows on Terracedb.

**Implementation steps**

1. Add restart/fault suites for the example covering:
   - broker restart,
   - ingress retry,
   - projection rebuild,
   - workflow checkpoint restore or replay,
   - duplicate Debezium delivery, and
   - outbox retry.
2. Add user-facing example docs that explain:
   - EventLog vs Mirror vs Hybrid trade-offs,
   - table/schema and row-filter trade-offs for large upstream databases,
   - historical bootstrap vs live-only workflow modes, and
   - why projections/workflows consume Terracedb tables rather than Kafka directly.
3. Make the example easy to run in deterministic simulation and easy to inspect when a seed fails.
4. Ensure the example is small enough to serve as a reference rather than a framework of its own.

**Verification**

- Example-level deterministic fault suites proving the documented operational modes survive restart and duplicate delivery as intended.
- Smoke tests or docs checks proving the example can be run and understood without hidden setup.
- Regression tests ensuring the example remains aligned with the supported crate APIs rather than drifting into bespoke internals.

---

### T99a. Build omnistack generated simulation/fuzz suites for the full first-party stack, with optional CDC ingress modes

**Depends on:** T29, T32b, T32e, T33e, T33f, T40, T40h, T40q, T98, T99

**Description**

Add the capstone broad-input suite that intentionally spans the full first-party Terracedb stack rather than only one vertical slice at a time. This harness should compose the real production code paths for the core DB, projections, workflows, relays/outbox flows, VFS, HTTP-facing application surfaces, telemetry capture, and sandbox/VFS-native authoring surfaces where practical, and then drive them from one shared generated-scenario vocabulary. CDC should be included, but explicitly as an optional ingress mode layered onto the same downstream stack rather than as the default path for every generated run: most users will not use Kafka/Debezium, but we still want a few high-value omnistack suites that prove the optional CDC path does not break the broader system. This capstone should build on the public `terracedb-systemtest` harness model rather than inventing a one-off internal testing world.

**Implementation steps**

1. Extend the public `terracedb-systemtest` harness layer with an omnistack scenario mode for the full first-party stack, rather than creating a separate internal-only harness family that examples or external apps cannot learn from.
2. Keep the local test organization lightweight and flat:
   - deterministic omnistack suites can live in normal top-level `tests/` files with small adjacent helper modules where useful,
   - if checked-in outputs are needed, prefer one local committed-output directory such as `tests/artifacts/` or `tests/snapshots/` per test root rather than a deep mandatory taxonomy, and
   - reusable logic should live in crates, not in a growing maze of test helper directories.
3. Add a top-level omnistack harness that consumes `terracedb-systemtest` plus the lower-level T33e substrate, and defines one serializable scenario language covering:
   - direct application writes,
   - projection/workflow/relay progress and drains,
   - timer/clock advancement,
   - flush/restart/fault schedules,
   - VFS or sandbox-visible file/activity operations,
   - HTTP requests against an application-facing surface, and
   - optional CDC delivery steps.
4. Build a default non-CDC omnistack mode that spans, in one run:
   - core DB state changes,
   - projections,
   - workflows,
   - relay/outbox delivery,
   - VFS-backed or sandbox-visible artifacts,
   - HTTP-visible read/write behavior, and
   - emitted telemetry/observability shapes where deterministic capture is supported.
5. Extend the oracle/shadow model so one scenario can assert cross-layer invariants such as:
   - projection outputs match the modeled retained state,
   - workflow state, timers, and outbox effects remain idempotent across restart,
   - relay-delivered effects match durable committed outbox entries,
   - VFS/sandbox-visible artifacts match the same logical actions seen by the DB-facing model, and
   - HTTP-visible responses and telemetry snapshots remain consistent with the underlying logical state.
6. Add a small number of CDC-mode omnistack suites that reuse the same downstream harness but swap ingress to Kafka + Debezium, varying duplicate delivery, partition/batch boundaries, Debezium materialization mode, and restart timing.
7. Add at least one direct-ingest versus CDC-ingest equivalence suite for scenarios where the logical semantics should match, comparing final projection/workflow/relay/application-visible outputs rather than low-level traces.
8. Keep the checked-in local/pre-commit slice bounded and parallel-friendly, reserve wider omnistack seed matrices for CI/nightly runs where larger minimized scenario artifacts and longer traces are acceptable, and keep naming clear so deterministic suites use names like `generated` or `simulation` rather than overloading `fuzz` for both finite tests and open-ended mutation runners.

**Verification**

- Saved omnistack scenarios replay deterministically across the full first-party stack and can be minimized into stable local repros.
- At least one generated suite spans core DB, projections, workflows, relays, VFS, HTTP-facing surfaces, and sandbox/VFS-native authoring behavior in one harness rather than as separate crate-local tests.
- CDC-mode omnistack suites prove the optional Kafka/Debezium path preserves downstream logical invariants and remains diagnosable under duplicate delivery, restart, and replay/live-mode variation.
- Direct-ingest and CDC-ingest equivalence suites prove that, where semantics are intended to match, optional CDC ingress changes only the ingest path and not the final user-visible logical state.

---

### T99b. Add time-budgeted mutational fuzz targets over serialized omnistack scenarios

**Depends on:** T33e, T33f, T99a

**Description**

Once the deterministic omnistack scenario harness exists, add a second layer that feeds it from long-running mutation fuzzers rather than only bounded checked-in seed campaigns. The executable mutation targets should live under the standalone `fuzz/` package because they are corpus-driven, open-ended, and should not run in normal workspace `nextest`. However, the scenario schema, serialization format, replay entrypoints, artifact readers/writers, and invariant-checking harnesses they need must live in reusable normal crates, primarily the public `terracedb-systemtest` crate layered on top of the T33e substrate, so both deterministic test suites and fuzz executables consume the same contracts.

**Implementation steps**

1. Refactor the T99a omnistack scenario language and replay entrypoints so they are reusable through `terracedb-systemtest` rather than hidden inside one test tree, including:
   - serializable scenario/operation types,
   - deterministic replay APIs,
   - stable artifact encoding/decoding,
   - failure minimization hooks, and
   - invariant/result summarization helpers suitable for both tests and fuzz targets.
2. Add one or more `cargo-fuzz` targets under `fuzz/fuzz_targets/` that mutate serialized omnistack scenarios rather than arbitrary API bytes, for example:
   - a default direct-ingest omnistack scenario target, and
   - an optional CDC-ingest omnistack scenario target or mode flag.
3. Seed those targets with corpora derived from:
   - checked-in T99a scenario fixtures,
   - minimized regressions found by deterministic generated suites, and
   - hand-authored edge cases for restart, relay drain, timer, VFS, HTTP, and CDC schedule boundaries.
4. Ensure every interesting failure found by the mutational targets can be materialized back into a stable scenario artifact that is replayable by the deterministic omnistack harness and suitable for check-in as a regression.
5. Keep the fuzz target surface property-driven: mutated scenarios should be judged by the same omnistack logical invariants as T99a rather than by ad hoc target-specific assertions.
6. Document the layering explicitly so contributors know:
   - deterministic finite omnistack suites live in normal flat `tests/` roots,
   - reusable scenario/harness code lives in `terracedb-systemtest` and `terracedb-fuzz`, and
   - open-ended mutation executables and corpora live in `fuzz/`.

**Verification**

- `cargo fuzz` targets can mutate omnistack scenarios for extended time budgets without depending on ad hoc `tests/` internals.
- Crashes or invariant failures discovered in `fuzz/` can be exported as stable scenario artifacts and replayed deterministically through the T99a test harness.
- Seed corpora cover both non-CDC and CDC omnistack modes, while keeping the optional CDC path additive rather than mandatory for all fuzz campaigns.

---

## Phase 20 — Sandbox capability policy, authoring UX, reviewed procedures, MCP exposure, and example app

**Phase rule:** T100 freezes the shared capability, procedure, and MCP-facing contracts first, and T101a freezes the row-scope and visibility-index contracts before the row-permission-specific implementation branches diverge. Every implementation task in this phase must add deterministic simulation coverage for the semantics it introduces rather than deferring simulation to the end. T106 is the capstone cross-cutting deterministic hardening task for the combined stack, and T107 adds a small example repo/app with its own simulation tests that demonstrates the intended end-to-end usage.

**Parallelization:** T100 first. T101 follows T100. T101a follows T101 + T31a. T101b follows T101. T101c follows T40c + T101. T102 follows T100 + T101 + T101a. T102a follows T101a + T102 + T31a. T103 and T104 can proceed in parallel once T102 exists. T105 follows T101 + T102 + T104 + T40g. T106 depends on T33 + T101b + T101c + T102a + T103 + T104 + T105. T107 depends on T101b + T101c + T102a + T103 + T104 + T105 + T106.

### T100. Freeze sandbox capability, migration, procedure, and MCP contracts plus deterministic seams

**Depends on:** T40a, T63

**Description**

Freeze the shared contracts that the rest of the phase will implement: capability templates and grants, manifest resolution, reviewed procedure metadata, migration-library boundaries, and the external MCP adapter surface. This task should also decide the deterministic seams up front so later tasks can land simulation coverage incrementally instead of retrofitting it.

**Implementation steps**

1. Add or reserve new workspace members for the shared policy and adapter layers, for example:
   - `terracedb-capabilities`,
   - `terracedb-migrate`,
   - `terracedb-procedures`,
   - `terracedb-mcp`.
2. Freeze the shared public contracts for:
   - `CapabilityTemplate`,
   - `CapabilityGrant`,
   - `CapabilityManifest`,
   - `ResourcePolicy`,
   - `BudgetPolicy`,
   - host-authored preset/profile descriptors that expand into manifests,
   - interactive draft-authorization request/outcome records,
   - foreground session-status snapshot/projection records,
   - sandbox execution-policy wiring that maps operations into execution domains,
   - reviewed procedure publication metadata and invocation contracts,
   - migration-plan and migration-history metadata,
   - MCP tool/resource descriptors and session/auth context.
3. Freeze the guest-visible module conventions for bound capabilities, including import shapes such as `terrace:host/<binding>`, and the matching shell-facing command naming/descriptor conventions where a binding is exposed to `just-bash`.
4. Decide which boundaries must sit behind deterministic stubs or fakes from the start, including:
   - auth and subject-resolution,
   - rate limiting,
   - execution-domain placement and budget accounting hooks,
   - interactive authorization request/approval plumbing,
   - reviewed-procedure publication storage,
   - MCP connection/session state.
5. Document the rule that capability policy and execution-domain policy are separate and composable: capabilities constrain authority, domains constrain resource consumption.

**Verification**

- Compile-only tests that instantiate the frozen capability, manifest, migration, procedure, and MCP contracts together.
- Unit tests that round-trip grant, manifest, publication-metadata, and execution-policy encodings.
- A deterministic smoke test that resolves a fake subject into a manifest and execution policy using only stubbed seams.

---

### T101. Implement capability resolution, audit metadata, and execution-domain assignment substrate

**Depends on:** T100, T40b, T64, T66

**Description**

Implement the shared policy substrate used by every later task: subject-to-grant resolution, manifest construction, preset/profile expansion, audit labels, optional draft authorization plumbing, live session-status projection, budget enforcement hooks, and host-controlled execution-domain assignment for sandbox and procedure work.

**Implementation steps**

1. Implement the shared capability registry and manifest-resolution path:
   - capability templates,
   - subject grants,
   - environment/deployment policy intersection,
   - host-authored preset/profile expansion,
   - binding-name generation,
   - immutable reviewed-manifest loading for published procedures.
2. Implement host-owned execution-policy resolution that maps:
   - draft sandbox execution,
   - package install / type-check / bash helpers,
   - migration publication/application,
   - procedure invocation,
   - MCP requests
   into configured execution domains.
3. Implement reusable audit metadata and event shapes for:
   - subject identity,
   - binding or capability used,
   - target resource set,
   - rate-limit/budget outcome,
   - chosen execution domain,
   - preset/profile name when one seeded the session,
   - authorization request/approval/rejection outcomes for trusted draft sessions.
4. Implement the host-mediated draft interactive-authorization substrate, including:
   - optional requests for additional bindings in trusted draft sessions,
   - approval scopes such as one call, one session, or host-defined policy update,
   - explicit distinction between a capability that was never injected and an operation that reached host enforcement and was denied.
5. Implement a live session-status projection surface derived from tool runs, activity entries, view state, and policy outcomes so active sessions can surface foreground state without inventing a disconnected telemetry plane.
6. Add stub/fake implementations for auth, rate limiting, interactive authorization, and domain-resolution seams so deterministic simulation can exercise policy outcomes without real external identity or quota systems.
7. Ensure every policy decision can be attached to tool runs, activity entries, or equivalent procedure/audit records without inventing an unrelated telemetry plane.

**Verification**

- Deterministic simulation tests proving the same subject/grant/environment inputs resolve to the same manifest and execution-domain assignments on replay.
- Tests covering preset expansion, allow, deny, rate-limited, and budget-exhausted outcomes with stable audit metadata.
- Tests proving absent bindings are surfaced differently from host-enforced denials and that interactive draft authorization can only widen scope through host approval.
- Tests proving the live session-status projection remains consistent with the underlying durable tool/activity history.
- Crash/recovery tests if any manifest or reviewed-policy metadata becomes persistent state.

---

### T101a. Freeze row-scope policy, visibility-index, and denial contracts

**Depends on:** T101, T31a

**Description**

Freeze the row-level-permission model before the implementation branches diverge. The goal is to make row-scope bindings, visibility-index-backed access, denial semantics, and write-time preimage checks first-class contracts rather than letting each later task invent its own interpretation.

**Implementation steps**

1. Freeze the shared public contracts for row-scoped database access, for example:
   - `PolicyContext`,
   - `RowScopePolicy`,
   - `RowScopeBinding`,
   - `VisibilityIndexSpec`,
   - denial/audit metadata for "not visible" vs "explicitly denied" outcomes,
   - scan continuation or resume tokens for filtered access paths.
2. Define the supported version-1 row-scope families and their intended use:
   - deterministic key-prefix scopes,
   - typed row predicates over decoded rows plus caller context,
   - projection-backed visibility indexes for ACL-like sharing,
   - explicit fail-closed or `procedure_only` escapes for queries that cannot be safely bounded.
3. Freeze which query shapes each row-scope family may authorize, including:
   - point read,
   - bounded prefix scan,
   - write/update/delete,
   - caller-selected multi-table query,
   - aggregate or count-like surfaces that may leak existence.
4. Freeze write-time semantics for row-scoped updates:
   - preimage/postimage policy evaluation,
   - scope-escaping-write rejection,
   - required OCC/read-set integration,
   - budget accounting for scanned vs returned rows.
5. Freeze the projection-helper contracts for visibility indexes and membership transitions, along with deterministic stubs/fakes that let simulation model projected visibility state before the full helper implementation lands.

**Verification**

- Compile-only tests that instantiate the row-scope, visibility-index, and denial contracts together with the base capability/grant/manifest types.
- Unit tests that round-trip row-scope policy encodings, visibility-index specs, and denial metadata.
- A deterministic smoke test that resolves a fake caller context into an effective row-scope binding and simulated visibility-index lookup using only stubbed seams.

---

### T101b. Productize named capability presets and profile-driven draft-session UX

**Depends on:** T101

**Description**

Build the user-facing preset/profile layer on top of the shared policy substrate so draft-session UX can start from named host-authored bundles rather than making every caller construct raw manifests by hand. This task is about making presets a first-class product surface rather than just a hidden expansion mechanism in the resolver.

**Implementation steps**

1. Add host-facing APIs and metadata surfaces for listing, describing, and selecting named presets or profiles backed by the expansion machinery from T101.
2. Support inspection of the expanded effective manifest, narrowing or overriding parts of it before session open, and surfacing both the preset name and expanded manifest in audit metadata.
3. Attach default budget, execution-domain, and session-mode hints to presets where useful without letting presets bypass the normal grant/intersection rules.
4. Reuse the same preset/profile surface in examples, draft-session tooling, and host APIs rather than inventing app-local shortcuts.
5. Keep preset resolution deterministic and replayable under the same stubbed policy seams used elsewhere in the phase.

**Verification**

- Unit tests covering preset listing, expansion, narrowing, override, and audit metadata.
- Deterministic simulation tests proving the same subject, preset, and environment inputs produce the same effective manifest and draft-session defaults on replay.
- Tests proving presets cannot widen authority beyond standing grants and environment/deployment policy.

---

### T101c. Productize optional host-mediated interactive authorization for trusted draft sessions

**Depends on:** T40c, T101

**Description**

Build the end-to-end interactive authorization flow on top of the substrate from T101. This is specifically for trusted draft/internal sessions: the host may choose to surface a request for more authority or a retry after a host-enforced denial, but guest code must never self-escalate from inside the sandbox.

**Implementation steps**

1. Add host-facing request/decision APIs for trusted draft sessions that can represent:
   - asking for a currently absent binding to be injected,
   - retry after an operation was denied at host enforcement,
   - approve-once, approve-for-session, reject, or host-policy-update outcomes.
2. Implement manifest refresh or retry flows so approved requests take effect through the same host-authoritative manifest machinery as ordinary grants.
3. Ensure the UX distinguishes cleanly between "binding was never injected" and "operation reached the host and was denied."
4. Record request, approval, rejection, and retry outcomes in the same audit/tool-run history as the attempted operation.
5. Explicitly disable the flow for published procedures, reviewed workflow bundles, and lower-trust production execution paths.

**Verification**

- Integration tests for trusted draft sessions covering request, approval, rejection, and retry behavior.
- Tests proving guest code cannot self-grant authority and that lower-trust paths cannot enter the interactive authorization flow.
- Tests proving absent bindings remain absent unless the host explicitly refreshes the manifest.

---

### T102. Implement database capability families and host-enforced policy checks

**Depends on:** T100, T101, T101a, T40c, T27

**Description**

Implement the first real host capability families for sandboxed code, especially the database-facing ones. The goal is to keep the host in the enforcement path for every database action so table allowlists, tenant scoping, query-shape restrictions, row-level filters, budgets, and audit logging stay authoritative.

**Implementation steps**

1. Implement initial capability families such as:
   - `db.table.v1`,
   - `db.query.v1`,
   - `catalog.migrate.v1`,
   - `procedure.invoke.v1`.
2. Implement host-side enforcement for:
   - table and database allowlists,
   - fixed-tenant or caller-derived tenant scoping,
   - query-shape restrictions such as point read vs prefix scan vs write vs schema mutation,
   - deterministic key-prefix validation or rewriting where the access path can be expressed in the key,
   - typed row-predicate evaluation for point reads and bounded scans,
   - scope-escaping-write rejection using preimage/postimage checks plus OCC read sets,
   - per-binding row/byte/time budgets, including accounting for scanned candidates vs returned rows where applicable.
3. Generate matching TypeScript declarations and guest-visible bound modules from the resolved manifest instead of hand-maintaining separate runtime and type definitions.
4. Ensure capabilities that are not granted are omitted from the injected module or shell-bridge surface entirely, while granted operations that do reach host enforcement fail predictably, carry enough machine-readable metadata for callers, audit logs, and simulations, and can normalize "not visible" to "not found" where low-leakage behavior is required.
5. Record capability usage with consistent tool-run or capability-event metadata so later procedure and MCP layers inherit the same audit surface automatically.

**Verification**

- Deterministic simulation tests for allowed and denied reads/writes/scans across table allowlists, tenant scopes, query-shape restrictions, deterministic key-prefix filters, typed row predicates, and scope-escaping writes.
- Integration tests that import generated bound modules from guest code and verify the host remains in the enforcement path.
- Integration tests proving ungranted bindings are absent from both guest imports and any generated shell-bridge surface, distinct from host-enforced denials for granted bindings.
- Restart or reopen tests for any persisted capability metadata introduced by the implementation.

---

### T102a. Implement projection helpers for row-level visibility indexes and membership transitions

**Depends on:** T101a, T102, T31a

**Description**

Implement reusable projection helpers that materialize visibility indexes for ACL-like row permissions. This task should make relationship-driven visibility cheap and deterministic without turning `db.query.v1` into a general-purpose ad hoc join planner.

**Implementation steps**

1. Add helper APIs in `terracedb-projections` for materializing visibility tables such as:
   - `visible_by_subject/{subject_id}/{row_id}`,
   - `visible_by_tenant/{tenant_id}/{row_id}`,
   - `visible_by_group/{group_id}/{row_id}`.
2. Support deterministic membership transitions explicitly, including:
   - `false -> true`,
   - `true -> false`,
   - share/unshare,
   - subject- or tenant-reassignment,
   - optional emission of narrowed or redacted read mirrors alongside the membership index.
3. Allow a helper to expand one logical visibility rule into multiple output rows when the authorized subject set comes from deterministic row fields or a companion membership table, while preserving deterministic output ordering and rebuild semantics.
4. Define rebuild/recompute behavior clearly:
   - helpers may rebuild from append-only/event-sourced inputs when available,
   - current-state visibility helpers may rebuild from authoritative current-state plus membership sources when provenance supports it,
   - unsupported provenance combinations must fail closed rather than invent visibility.
5. Add integration recipes or helper glue so `db.query.v1` and reviewed procedures can route scans through projected visibility indexes instead of relying on unbounded post-filtered scans of primary tables.

**Verification**

- Projection tests covering grant, revoke, share, unshare, reassignment, deletion, and redaction transitions.
- Recompute tests proving visibility indexes rebuilt from their authoritative sources match incremental tailing.
- Deterministic simulation tests that interleave visibility projection updates with row-scoped reads and writes, proving the answers match the intended oracle.
- Integration tests proving visibility-index-backed access respects the same row/byte/time budget accounting surface used by direct bindings.

---

### T103. Implement reviewed migration library for catalog setup and app-schema evolution

**Depends on:** T102, T40d

**Description**

Implement a `terracedb-migrate` library that executes reviewed TypeScript modules in the sandbox for table/catalog setup and application-schema changes. Version 1 is intentionally narrow: this task owns catalog and schema evolution, not large data backfills or arbitrary row-rewrite jobs.

**Implementation steps**

1. Implement migration authoring and application helpers that run inside the sandbox but call host-enforced catalog capability modules rather than raw engine handles.
2. Implement the reviewed migration model, including:
   - migration identity and ordering,
   - requested capability manifest,
   - migration history metadata,
   - audit records for review and apply outcomes.
3. Support the version-1 migration surface:
   - create or ensure tables,
   - validated columnar schema successors,
   - table-metadata updates,
   - idempotent precondition checks and no-op reapply semantics where possible.
4. Keep migration execution in host-selected execution domains and control-plane lanes so catalog work stays isolated from general sandbox traffic.
5. Add deterministic fakes or stubs for any review/publication seam that is not purely local.

**Verification**

- Deterministic simulation tests proving migration ordering, idempotence, schema-successor validation, and audit/history behavior under replay and crash.
- Tests showing migrations cannot widen their own capability set or escape catalog-scoped authority.
- Restart tests proving partially published or partially applied migration metadata fails closed and replays deterministically.

---

### T104. Implement reviewed procedure publication, immutable versions, and invocation runtime

**Depends on:** T102, T40d, T40f

**Description**

Implement a `terracedb-procedures` library that turns reviewed sandbox code into immutable published artifacts. Lower-trust or external callers should invoke reviewed named versions rather than mutating the code that runs on their behalf.

**Implementation steps**

1. Implement procedure publication metadata and storage:
   - name/version identity,
   - code hash,
   - entrypoint,
   - input/output schemas,
   - reviewed capability manifest,
   - reviewer and publication metadata,
   - execution-budget metadata.
2. Implement the publish flow:
   - start from draft sandbox code,
   - review code plus requested manifest,
   - freeze an immutable snapshot or module bundle,
   - persist publication metadata,
   - make later invocation resolve from the immutable published version.
3. Implement invocation:
   - fresh session from immutable base,
   - caller identity and tenant context injection,
   - caller-context-aware row-scope binding resolution, including visibility-index-backed helper bindings where requested by the reviewed manifest,
   - manifest intersection with deployment policy,
   - JSON-schema or equivalent input/output validation,
   - host-selected execution-domain assignment.
4. Ensure callers cannot mutate published code, widen capability scope, or reuse mutable draft sessions as invocation targets.
5. Record invocation history, policy outcomes, and audit metadata using the shared capability substrate from earlier tasks.

**Verification**

- Deterministic simulation tests for publish, invoke, deny, and replay semantics across reviewed manifests and caller context changes.
- Crash/recovery tests around publication metadata, immutable snapshot publication, and invocation bookkeeping.
- Integration tests proving published procedures are immutable and version-addressable from the host surface.

---

### T105. Implement external MCP adapter over published procedures and approved sandbox surfaces

**Depends on:** T101, T102, T104, T40g

**Description**

Implement `terracedb-mcp` as an external adapter for agents such as Claude Desktop. The adapter should expose selected sandbox and procedure surfaces while reusing the same subject/grant/manifest model as in-process sandboxes instead of inventing a parallel permission system.

**Implementation steps**

1. Implement MCP connection/session resolution into:
   - subject identity,
   - effective capability manifest,
   - execution-domain policy,
   - audit context.
2. Expose a minimal approved tool/resource surface such as:
   - list procedures,
   - inspect procedure metadata,
   - invoke reviewed procedures,
   - inspect read-only sandbox views,
   - read or diff sandbox files,
   - inspect capability/tool-run history,
   - optionally run draft queries only for explicitly trusted subjects.
3. Reuse the read-only view/service protocol from the sandbox/editor work rather than creating a separate file-inspection path for MCP.
4. Ensure every MCP-visible action maps onto the same host capabilities, procedure runtime, or read-only view surface already used in-process.
5. Add deterministic stub transport/session backends so protocol-facing logic can be simulated without depending on a real desktop client or live IPC transport.

**Verification**

- Deterministic simulation tests for subject resolution, allow/deny behavior, approved tool exposure, and audit-history publication over the MCP adapter.
- Integration tests proving MCP invocations reuse published-procedure enforcement and read-only sandbox views rather than bypassing them.
- Tests showing lower-trust MCP subjects cannot access draft-query or broader sandbox capabilities unless explicitly granted.

---

### T106. Build cross-cutting deterministic simulation, crash, and fault suites for the capability/procedure/MCP stack

**Depends on:** T33, T101b, T101c, T102a, T103, T104, T105

**Description**

Bring the new stack up to the same correctness bar as the rest of Terracedb by adding whole-system deterministic workloads, restart/fault campaigns, and oracle-backed validation for policy, review/publication, invocation, and external-agent access.

**Implementation steps**

1. Extend the deterministic shadow/oracle model to cover:
   - capability-grant resolution,
   - preset/profile expansion,
   - manifest intersections,
   - row-scope policy resolution,
   - visibility-index projection state and membership transitions,
   - execution-domain assignment outcomes,
   - migration history state,
   - published-procedure metadata and immutable-version selection,
   - shell-bridge command mapping,
   - MCP request-to-capability mapping,
   - draft-authorization request and outcome state,
   - foreground session-status projection state,
   - audit/tool-run history.
2. Add randomized workloads that combine:
   - draft sandbox execution,
   - approved and denied DB capability calls,
   - direct row-scoped reads and writes,
   - visibility-index-backed shared-row queries,
   - absent binding and shell-bridge lookups,
   - shell-bridge invocations,
   - optional draft authorization requests,
   - reviewed migration application,
   - procedure publication,
   - procedure invocation under varying caller contexts,
   - read-only sandbox inspection over MCP,
   - execution-domain throttling or budget exhaustion.
3. Add crash cut points around:
   - manifest publication,
   - migration-history updates,
   - reviewed-procedure publication,
   - immutable snapshot registration,
   - invocation bookkeeping,
   - MCP session state if any is persisted.
4. Add failure campaigns for:
   - denied capability escalation attempts,
   - scope-escaping writes,
   - stale or divergent visibility-index state,
   - preset/profile expansion mismatches,
   - stale review metadata,
   - rate-limit exhaustion,
   - domain-budget exhaustion,
   - conflicting procedure publication,
   - absent-binding versus host-denial confusion in foreground session state,
   - read-only view reconnect behavior through MCP.
5. Ensure failing seeds preserve enough manifest, subject, procedure-version, and execution-domain context for exact local replay.

**Verification**

- Large-seed deterministic suites covering the full capability/policy/procedure/MCP stack.
- Cross-cutting restart/recovery tests proving reviewed artifacts remain immutable and fail closed under partial publication or invocation crashes.
- Oracle-backed tests proving allow/deny, tenant-scope, row-scope, query-shape, visibility-index, and execution-domain outcomes match the intended contracts.
- Reproducibility tests proving failing seeds preserve the necessary trace and policy context for replay.

---

### T107. Build a small example repo/app that demonstrates reviewed migrations, draft queries, published procedures, and MCP exposure

**Depends on:** T101b, T101c, T102a, T103, T104, T105, T106

**Description**

Add a small, teachable example repo/app that demonstrates the intended way to use the new stack in practice: reviewed migrations for catalog setup, trusted draft query sandboxes for internal users, row-scoped visibility rules, immutable published procedures for lower-trust callers, and an MCP surface that exposes the approved capabilities to an external agent. The example should include its own simulation tests, demonstrate the policy/authorization/session-UX affordances added in this phase, and serve as the reference integration pattern for these libraries. Build on the sandbox example from T40r where practical rather than creating a near-duplicate teaching app.

**Implementation steps**

1. Add a compact example app and companion sandboxed project that demonstrates:
   - reviewed migration authoring and apply flow,
   - an employee draft-query sandbox with broader internal grants,
   - row-scoped access to a shared-record domain enforced through bound DB capabilities,
   - a projection-maintained visibility index for shared rows,
   - a named preset/profile that seeds the draft sandbox manifest,
   - optional host-mediated draft authorization for requesting an additional binding,
   - at least one capability or reviewed procedure exposed through the shell-facing bridge as well as the typed import path,
   - one or more reviewed published procedures for external callers,
   - MCP exposure of approved procedure and read-only sandbox surfaces,
   - a foreground session-state surface for active draft work.
2. Give the example a simple but realistic authorization model with at least:
   - an internal employee subject,
   - a publication/reviewer subject,
   - an external caller subject,
   - at least one share/unshare or tenant-scoped visibility rule that changes over time.
3. Demonstrate execution-domain isolation in the example by assigning:
   - foreground draft-query work,
   - procedure invocation,
   - publication/catalog work, and
   - MCP inspection traffic
   to distinct domains or domain classes.
4. Add example docs that explain:
   - capability templates vs grants vs manifests,
   - row-scope policies vs projection-maintained visibility indexes,
   - why ACL-like shared visibility should usually route through projected indexes or reviewed procedures instead of ad hoc full scans,
   - how presets/profiles expand into concrete manifests,
   - how typed imports and shell-facing bridges share the same authority model,
   - the difference between a capability that is absent from a draft session and a host-enforced denial on a granted binding,
   - why published procedures are immutable,
   - why migrations are intentionally catalog-scoped in version 1,
   - how MCP reuses the same permission model instead of introducing a second one,
   - how execution domains limit resource blast radius,
   - how the foreground session surface relates to the durable audit/tool-run log.
5. Add deterministic simulation tests owned by the example itself for its documented happy paths and representative deny/fault cases, including shell-bridge parity with the typed-import path.

**Verification**

- Example-level deterministic simulation tests proving the documented migration, row-scoped query, procedure, and MCP flows behave as described.
- Tests proving the example's internal and external subjects receive meaningfully different capabilities and execution-domain assignments.
- Tests proving visibility-index transitions such as share/unshare or tenant mismatch produce the documented visible vs hidden outcomes.
- Tests proving the example's preset/profile selection, optional draft authorization flow, and foreground session-state surface behave as documented.
- Tests proving the example's shell-facing bridge and typed-import path stay aligned under the same effective manifest and audit model.
- Smoke tests or docs checks proving the example can be run and inspected without hidden setup.
- Regression tests ensuring the example remains aligned with the supported public crate APIs instead of drifting into bespoke internals.

---

## Phase 21 — Workflow runs, history, transition-engine contracts, and shared native/sandbox execution

**Phase rule:** T108 freezes the workflow run/recovery/state/savepoint model and the public handler contracts first so the rest of the phase can parallelize cleanly. Every implementation task in this phase must add deterministic simulation coverage for the semantics it introduces instead of waiting until the end. T113 is the capstone cross-cutting deterministic hardening task for the combined runtime, and T114 adds a small example repo/app with its own simulation tests that demonstrates both native Rust and sandbox-authored workflows on the same engine.

**Parallelization:** T108 first. T109, T110, and T111 can proceed in parallel once T108 exists. T112 depends on T109 + T110 + T111. T113 depends on T33 + T109 + T110 + T111 + T112. T114 depends on T109 + T110 + T111 + T112 + T113.

### T108. Freeze workflow run, execution, recovery-journal, savepoint, visibility, and handler contracts

**Depends on:** T32, T40a, T84, T100

**Description**

Freeze the stronger workflow model up front: explicit runs pinned to immutable execution targets, hidden recovery journals, lightweight internal savepoints, current mutable state, lifecycle metadata, and separate operator-facing visibility contracts. This task should also freeze the dual handler boundary: native Rust handlers remain first-class, while sandbox handlers plug in through a narrower versioned task ABI.

**Implementation note**

This task may reserve the refined crate boundaries before every runtime file move lands. Once T108 exists, `terracedb-workflows-core` should be treated as the source of truth for the frozen public workflow contracts and `terracedb-workflows-sandbox` as the source of truth for the sandbox task ABI, even if the executable runtime still lives temporarily inside `terracedb-workflows`.

**Implementation steps**

1. Reserve or add the refined workflow crate boundaries, for example:
   - `terracedb-workflows-core`,
   - `terracedb-workflows-runtime`,
   - `terracedb-workflows-sandbox` as an adapter layer rather than the whole workflow library.
2. Freeze the public contracts for:
   - `WorkflowExecutionEpoch`,
   - `WorkflowRunRecord`,
   - `WorkflowRunId`,
   - `WorkflowTaskId`,
   - `WorkflowRecoveryJournalRecord`,
   - `WorkflowSavepointRecord`,
   - `WorkflowVisibleHistoryEntry`,
   - `WorkflowLifecycleState`,
   - `WorkflowStateRecord`,
   - `WorkflowVisibilityRecord`,
   - `WorkflowTransitionInput`,
   - `WorkflowTransitionOutput`.
3. Freeze the public handler contracts for:
   - native Rust handlers,
   - sandboxed `workflow-task/v1` handlers,
   - the deterministic context helpers and observability hooks visible to both.
4. Freeze the rule that public handler contracts and private executor/effects contracts are distinct; guest code should never receive the richer internal reducer/effects surface directly.
5. Freeze deterministic seams for:
   - execution-target resolution,
   - recovery-journal append ordering,
   - lightweight savepoint coverage and segment sealing,
   - retry/timer wakeups,
   - visibility projection updates,
   - native-Rust-vs-sandbox parity testing.

**Verification**

- Compile-only tests that instantiate the new run/recovery/state/visibility contracts together with both native and sandbox handler adapters.
- Round-trip tests for execution metadata, lifecycle records, recovery-journal records, and visible-history encodings.
- A deterministic smoke test that runs one fake Rust handler and one fake sandbox handler through the same frozen task contract.

---

### T109. Implement explicit workflow runs, recovery-journal storage, savepoints, state reduction, and lifecycle records

**Depends on:** T108, T32, T84

**Description**

Implement the refined durable data model for workflows: explicit run records, append-only hidden recovery journals, lightweight internal savepoints, mutable state summaries, and durable lifecycle state. This task owns the storage model and deterministic reducer boundaries, not the higher-level operator surfaces.

**Implementation note**

T109 should store and reduce the T108 contract types rather than introducing parallel run/recovery/state/lifecycle record structs in the runtime layer. If runtime code has not yet moved into a dedicated `terracedb-workflows-runtime` crate, that crate split can still land incrementally during T109/T110/T112 without blocking the semantics in this task.

**Implementation steps**

1. Implement durable storage for:
   - workflow runs and pinned execution identity,
   - append-only per-run recovery journal segments,
   - lightweight internal savepoints,
   - current mutable state summary,
   - lifecycle metadata such as scheduled, running, suspended, paused, retry-waiting, completed, or failed.
2. Implement the state-reduction path that derives current state from admitted transition output and appends matching recovery-journal records in the same durable unit.
3. Record run creation, run completion, continue-as-new edges, and lifecycle changes as explicit durable records instead of burying them in inbox/timer tables only.
4. Make the runtime robust to simple single-run-per-instance cases while still keeping run IDs explicit for later upgrades and restart-as-new behavior.
5. Add deterministic helper builders and fake stores so later tasks can exercise the new model without the full runtime being present.

**Verification**

- Deterministic simulation tests proving run creation, replay, lifecycle transitions, state reduction, and savepoint coverage remain stable across crash/restart.
- Tests proving recovery-journal append order and reduced state stay identical for the same admitted trigger sequence.
- Restart tests showing partially written run metadata, journal segments, or savepoints fail closed and replay deterministically.

---

### T110. Implement the unified workflow transition engine, durability fences, waits, retries, and timers

**Depends on:** T108, T29, T32, T84

**Description**

Implement the Rust-owned transition engine that all workflow inputs must pass through. This task owns the durable-apply-before-side-effects rule, explicit waiter sets, retry state, timer ownership, and the transition from admitted input into recovery-journal, state, visibility, and effect proposals.

**Implementation note**

T110 owns the private executor and effects model that sits behind the frozen T108 handler boundary. It should reuse `WorkflowTransitionInput` and `WorkflowTransitionOutput` at the public edge, while keeping richer reducer/effect internals private to the runtime implementation so later tasks do not accidentally widen the guest-visible contract.

**Implementation steps**

1. Implement one transition engine that accepts:
   - admitted source events,
   - admitted callbacks,
   - admitted timer firings,
   - explicit wakeups/retries,
   - and future control/update messages.
2. Implement explicit durable waiter-set storage so suspended work records exactly what completion, signal, or wakeup it is waiting on.
3. Implement durable retry state with keyed timers so stale retry firings can be detected and ignored safely.
4. Ensure the transition engine appends recovery-journal records, updates state/lifecycle, advances visible-history projections, and schedules effect intents atomically before any external effect delivery path runs.
5. Keep promise/awakeable-style coordination and timer ownership in the engine-owned data model rather than relying on transient guest-runtime memory.

**Verification**

- Deterministic simulation tests for suspend/resume, retries, timer firing, stale wakeup suppression, and duplicate admission.
- Crash/recovery tests proving side effects never escape before the durable transition that justifies them.
- Oracle-backed tests for waiter-set reduction and retry-state replay through restart.

---

### T111. Implement native Rust handler execution and sandbox `workflow-task/v1` adapter parity

**Depends on:** T108, T40i, T100

**Description**

Implement the shared execution boundary so native Rust workflows and sandbox-authored workflows both run on the same durable engine. Rust-native workflows must remain first-class; sandbox execution is an adapter path, not the primary workflow model.

**Implementation note**

T108 may already provide the first pass of the shared handler traits, deterministic context helpers, and `workflow-task/v1` request/response types. T111 still owns the runtime-loadable execution path, bundle/module loading integration, restart semantics, capability narrowing, and parity wiring into the real workflow engine; it should extend the frozen ABI rather than redefining it.

**Implementation steps**

1. Implement a type-erased or otherwise runtime-loadable handler boundary that supports:
   - native Rust handlers registered directly in-process,
   - sandbox handlers loaded through `terracedb-workflows-sandbox`.
2. Implement the `workflow-task/v1` sandbox ABI:
   - deterministic context helpers,
   - replay/state/trigger input,
   - structured command output,
   - stable error and rejection metadata.
3. Ensure native Rust handlers and sandbox handlers receive semantically equivalent task input and produce semantically equivalent reducer commands.
4. Keep capability injection for sandbox handlers narrow and workflow-specific; do not expose raw DB handles or authoring-only affordances on the execution path.
5. Add shared parity helpers so deterministic tests can run the same logical workflow through both the Rust and sandbox paths.

**Verification**

- Deterministic parity tests showing the same logical workflow produces identical history/state/effect output through native Rust and sandbox adapters.
- ABI round-trip tests for sandbox task input/output and rejection cases.
- Restart tests proving sandbox bundle reload or handler re-registration does not alter durable replay semantics.

---

### T112. Implement owner-local ready scheduling, wakeup routing, and post-commit effect handoff

**Depends on:** T109, T110, T111, T63, T66

**Description**

Implement the runtime loop around the new transition engine: owner-local ready-instance scheduling, event-driven wakeups, post-commit effect handoff, and execution-domain-aware placement. This task should preserve the shorter event-driven control loop that the Restate exploration suggested, without abandoning the stronger run/recovery/savepoint model.

**Implementation steps**

1. Implement ready-instance / ready-run scheduling around explicit run ownership and lifecycle state rather than raw inbox scans alone.
2. Keep timer wakeups, retry wakeups, and newly admitted inbox work as event-driven signals into the scheduler rather than a polling-heavy worker acquisition model.
3. Route effect delivery only after durable transition apply, using explicit post-commit effect proposals instead of letting handlers mutate external state directly.
4. Make workflow execution, wakeups, and effect handoff execution-domain aware so hot workflow traffic does not collapse into one uncontrolled runtime lane.
5. Preserve the semantic rule that each run is processed single-threadedly in durable order even if the runtime later parallelizes across runs.

**Verification**

- Deterministic simulation tests for ready-run scheduling, wakeup ordering, retry wakeups, and effect handoff under domain pressure.
- Crash/recovery tests proving post-commit effect handoff does not duplicate or lose durable transition state.
- Tests showing later visible-but-not-durable admissions never leapfrog durable work.

---

### T113. Build cross-cutting deterministic simulation suites for run/recovery replay, waits, retries, and Rust-vs-sandbox parity

**Depends on:** T33, T109, T110, T111, T112

**Description**

Bring the redesigned workflow core up to the same correctness bar as the rest of Terracedb by adding whole-system deterministic simulation and crash/fault campaigns for the new run/recovery/savepoint model, the transition engine, and the dual native/sandbox execution surface.

**Implementation steps**

1. Extend the workflow shadow/oracle model to cover:
   - explicit runs and execution epochs,
   - append-only recovery-journal segments,
   - lightweight internal savepoints,
   - lifecycle transitions,
   - waiter sets,
   - retry state,
   - timer ownership,
   - visible-history projections,
   - effect proposals,
   - native-Rust-vs-sandbox handler equivalence.
2. Add randomized workloads that mix:
   - source events,
   - callbacks,
   - timers,
   - retries,
   - manual pause/resume,
   - continue-as-new,
   - Rust and sandbox handlers for the same logical workflow.
3. Add crash cut points around:
   - recovery-journal append,
   - savepoint creation and segment sealing,
   - lifecycle updates,
   - retry-timer registration,
   - effect-proposal persistence,
   - sandbox task completion and adapter restart.
4. Add failure campaigns for:
   - duplicate timer or callback admission,
   - stale retry wakeups,
   - nondeterministic handler output rejection,
   - sandbox adapter reloads,
   - execution-domain throttling.
5. Preserve enough run, bundle, seed, and handler-path metadata to replay failures locally with exact parity.

**Verification**

- Large-seed deterministic suites covering the combined workflow runtime.
- Cross-cutting crash/recovery tests for the new run/recovery/savepoint model and dual handler surface.
- Oracle-backed parity tests proving Rust and sandbox handlers stay behaviorally aligned.
- Reproducibility tests proving failure artifacts preserve the necessary run and bundle context.

---

### T114. Build a toy example repo/app that demonstrates native Rust and sandbox-authored workflows on the same engine

**Depends on:** T109, T110, T111, T112, T113

**Description**

Add a small, teachable example repo/app that demonstrates the intended workflow-core model in practice: explicit runs and execution identity, hidden recovery journals, automatic savepoints and segment rollover, the same durable engine driving both native Rust and sandbox-authored workflows, explicit waits/retries/timers, and simulation-backed recovery behavior.

**Implementation steps**

1. Add a compact toy app with at least:
   - one native Rust workflow,
   - one sandbox-authored workflow,
   - explicit timer and retry behavior,
   - a simple callback or wakeup path.
2. Show both workflows running against the same run/recovery/runtime model rather than two different executors.
3. Include enough example docs to explain:
   - runs vs execution targets,
   - hidden recovery journal vs visible history,
   - savepoints and automatic compaction,
   - why waits and retries are durable data,
   - why sandbox execution is optional rather than required.
4. Make the example own its own deterministic simulation tests and failure/restart scenarios.
5. Keep the example small enough to remain a reference integration pattern rather than a product-sized sample.

**Verification**

- Example-level deterministic simulation tests covering Rust and sandbox workflow happy paths, restart, retry, and timer behavior.
- Regression tests proving the example stays on public workflow APIs rather than bespoke internals.
- Smoke checks ensuring the example can be run and inspected without hidden setup.

---

## Phase 22 — Workflow deployment, visibility, upgrades, preview/prod flows, and operator surfaces

**Phase rule:** T115 freezes deployment, visibility, query/update, and upgrade contracts first so the rest of the phase can parallelize without interface churn. Every implementation task in this phase must add deterministic simulation coverage when it changes workflow behavior. T121 is the capstone cross-cutting deterministic hardening task for the deployment/operator stack, and T122 adds a small example repo/app with its own simulation tests that demonstrates the intended rollout and inspection flow.

**Parallelization:** T115 first. T116, T117, and T118 can proceed in parallel once T115 exists. T119 depends on T116 + T117 + T118. T120 depends on T116 + T119 + T40i. T121 depends on T33 + T116 + T117 + T118 + T119 + T120. T122 depends on T116 + T117 + T118 + T119 + T120 + T121.

### T115. Freeze workflow deployment, visibility, query/update, visible-history, and upgrade contracts

**Depends on:** T108, T109, T111, T100

**Description**

Freeze the operator- and deployment-facing contracts on top of the redesigned workflow core: immutable bundle and deployment metadata, native Rust registration identity, list/describe/visible-history visibility APIs, query/update message lanes, compatibility manifests, and explicit continue-as-new / restart-as-new upgrade semantics.

**Implementation note**

T115 should build on the bundle/run/visibility contracts frozen in T108 instead of creating a second incompatible bundle-metadata or visibility-record family. The work here is to extend those core contracts with deployment-manager, query/update, and upgrade metadata and APIs.

**Implementation steps**

1. Freeze public contracts for:
   - published workflow bundle metadata,
   - native Rust workflow registration metadata,
   - deployment records and rollout policy,
   - query/update request and response types,
   - describe/list/visible-history visibility APIs,
   - compatibility and upgrade policy metadata.
2. Freeze the rule that runs pin one bundle or native registration identity for an execution epoch unless an explicit upgrade transition occurs.
3. Decide the public deployment-manager seams for:
   - preview,
   - activate/deactivate,
   - rollout,
   - continue-as-new,
   - restart-as-new,
   - rollback.
4. Freeze deterministic seams for:
   - bundle resolution,
   - deployment rollout,
   - compatibility checks,
   - visibility projection rebuild,
   - query/update admission.
5. Keep the contracts neutral across native Rust and sandbox-authored workflows wherever possible.

**Verification**

- Compile-only tests that instantiate deployment, visibility, query/update, and upgrade contracts together.
- Round-trip tests for bundle/deployment/compatibility metadata.
- Deterministic smoke tests that resolve fake preview and production deployments through the new seams.

---

### T116. Implement immutable workflow bundle registry and deployment manager

**Depends on:** T115, T109, T111, T104

**Description**

Implement the workflow deployment substrate: immutable bundle records, native Rust workflow registrations, deployment activation, rollout, rollback, and run-to-bundle assignment. This task should make deployment an explicit part of the workflow system rather than an implicit library bootstrap detail.

**Implementation steps**

1. Implement storage and resolution for:
   - immutable sandbox-authored workflow bundles,
   - native Rust workflow registrations,
   - deployment records and activation state,
   - rollout policy and assignment metadata.
2. Implement run creation so each run records the exact bundle or registration identity it is pinned to.
3. Implement deployment-manager operations such as:
   - register,
   - publish,
   - activate,
   - deactivate,
   - rollback,
   - resolve for new run creation.
4. Ensure deployment records can drive both preview environments and production execution without creating separate correctness models.
5. Keep publication and deployment metadata auditable and compatible with the capability/review substrate from earlier sandbox work.

**Verification**

- Deterministic simulation tests for publish/register, activate, rollback, and new-run bundle assignment.
- Crash/recovery tests around partial publication or partial deployment activation.
- Tests proving runs remain pinned to the originally assigned bundle/registration unless an explicit upgrade transition occurs.

---

### T117. Implement workflow visibility projection, describe/visible-history APIs, and operator forensics hooks

**Depends on:** T115, T109, T112, T32b

**Description**

Implement the operator-facing visibility layer on top of explicit runs/recovery/state: list/search projections, describe APIs, paginated visible-history APIs, trace/correlation fields, and lower-level hooks for operator forensics. This task should preserve the clear split between authoritative recovery data, mutable state, and operator projection.

**Implementation steps**

1. Implement `workflow_visibility` projection maintenance for:
   - active runs,
   - terminal runs,
   - lifecycle summaries,
   - retry/backoff state,
   - current bundle or deployment identity,
   - stable trace or correlation IDs.
2. Implement dedicated APIs or helper surfaces for:
   - list/search,
   - describe one run,
   - paginated visible-history retrieval.
3. Keep raw-table or SQL-style introspection as an optional deeper layer rather than the required path for common operator flows.
4. Add operator-forensics hooks that expose enough metadata to correlate visibility rows with recovery-journal state, scheduler state, and deployment identity.
5. Keep projection rebuild and backfill paths deterministic so visibility can be reconstructed from authoritative recovery data when needed.

**Verification**

- Deterministic simulation tests for visibility projection maintenance, rebuild, and list/describe/visible-history behavior across restart.
- Tests proving visibility rows do not drift from authoritative recovery/state under randomized workloads.
- Trace/correlation tests showing runs can be inspected without coupling core correctness to tracing success.

---

### T118. Implement workflow queries, update/control lanes, and proposal-then-commit semantics

**Depends on:** T115, T109, T110, T112

**Description**

Implement the non-history-everything interaction model: read-only queries, validation-style updates, and control messages that only become durable when accepted. This task should let the workflow system support cheap inspection and controlled mutation without forcing every interaction through the same visible-history or recovery path.

**Implementation steps**

1. Implement read-only workflow queries against current state and/or visibility data without mutating the run recovery journal or visible history.
2. Implement update/control request handling that may:
   - inspect live state,
   - validate against workflow-local policy,
   - reject without appending history,
   - or accept and become a normal durable transition through the engine.
3. Keep accepted updates on the same transition-engine path as ordinary workflow triggers so recovery state, visible history, and mutable state stay coherent.
4. Ensure query/update handling works for both native Rust and sandbox-authored workflows without widening the sandbox execution surface.
5. Add deterministic request IDs or equivalent idempotency hooks for externally initiated update/control flows.

**Verification**

- Deterministic simulation tests for query consistency, accepted vs rejected updates, and idempotent control handling.
- Tests proving rejected updates leave no durable recovery or visible-history entry while accepted ones reduce through the same durable path as ordinary triggers.
- Restart/replay tests around pending or duplicated control requests.

---

### T119. Implement upgrade compatibility, continue-as-new, restart-as-new, and rollout controls

**Depends on:** T116, T117, T118

**Description**

Implement the explicit upgrade model for long-running workflows: compatibility gates, pinned runs, continue-as-new boundaries, restart-as-new controls, and rollout policy for new runs. This task should codify the “runs pin one execution target by default” rule while keeping ordinary replay hygiene on the automatic savepoint and segment-rotation path rather than on manual continue-as-new.

**Implementation steps**

1. Implement compatibility checks and policy evaluation for:
   - new-run assignment,
   - continue-as-new transitions,
   - restart-as-new transitions,
   - manual rollout or rollback.
2. Add explicit run transitions for:
   - continue-as-new,
   - restart-as-new,
   - pause for incompatibility,
   - manual override or operator escape hatch where appropriate.
3. Record upgrade and compatibility decisions in history and visibility so operators can explain why a run stayed pinned, rolled forward, or required restart.
4. Keep the default safe rule that new bundles affect new runs first, while live runs remain pinned unless an explicit upgrade transition occurs.
5. Keep automatic savepoint creation and segment rotation separate from explicit continue-as-new or restart-as-new upgrade boundaries.
6. Ensure the same upgrade model works for both native Rust registrations and sandbox-authored published bundles.

**Verification**

- Deterministic simulation tests for rollout, pinning, continue-as-new, restart-as-new, rollback, and incompatibility handling.
- Crash/recovery tests around mid-upgrade or mid-rollout transitions.
- Tests proving incompatible live-run upgrades fail closed rather than silently switching code under replay.

---

### T120. Implement draft preview, strict preview, and production deployment flows over the same executor

**Depends on:** T116, T119, T40i

**Description**

Implement the development and deployment flow for sandbox-authored workflows without creating a second correctness model. Preview should run through the same durable executor semantics as production; the difference is mutable draft source vs sealed published artifact, not a different workflow engine.

**Implementation steps**

1. Implement draft preview flows that resolve workflow code from mutable sandbox sessions but still run through the same Rust executor and run/recovery/savepoint model.
2. Implement strict-preview flows that package the exact published artifact and run it locally or in preview under production-equivalent execution semantics.
3. Ensure production deployment consumes only immutable bundle artifacts or native Rust registrations, never mutable draft sandboxes.
4. Keep preview/prod table naming, namespace, or run isolation explicit so draft work cannot collide with production state.
5. Reuse the same deployment-manager, compatibility, and visibility surfaces across preview and production rather than inventing a second dev-only lifecycle.

**Verification**

- Deterministic simulation tests for draft preview, strict preview, and production deployment parity.
- Tests proving mutable draft sandboxes can author workflows but cannot be treated as production execution targets directly.
- Restart tests showing preview runs and production runs remain isolated yet semantically equivalent.

---

### T121. Build cross-cutting deterministic simulation suites for deployments, visibility, upgrades, and preview/prod parity

**Depends on:** T33, T116, T117, T118, T119, T120

**Description**

Bring the workflow deployment/operator stack up to the same bar as the core engine by adding cross-cutting deterministic simulation, crash/fault campaigns, and oracle-backed validation for deployment, visibility, upgrades, query/update lanes, and preview/prod parity.

**Implementation steps**

1. Extend the workflow oracle to cover:
   - deployment records,
   - bundle assignment,
   - visibility projection,
   - query/update outcomes,
   - upgrade transitions,
   - preview-vs-production path equivalence.
2. Add randomized workloads that combine:
   - new runs under rolling deployments,
   - continue-as-new and restart-as-new,
   - accepted and rejected updates,
   - list/describe/history queries,
   - preview and strict-preview execution,
   - rollback and redeploy.
3. Add crash cut points around:
   - deployment activation,
   - compatibility decisions,
   - visibility projection updates,
   - upgrade transitions,
   - preview/publication handoff.
4. Add failure campaigns for:
   - incompatible rollout attempts,
   - stale preview bundles,
   - partial visibility rebuilds,
   - duplicate control messages,
   - preview/prod namespace collisions.
5. Preserve enough seed, run, deployment, and bundle metadata to replay failures locally.

**Verification**

- Large-seed deterministic suites covering the workflow deployment/operator stack.
- Cross-cutting crash/recovery tests for deployment activation, visibility projection, and upgrade transitions.
- Oracle-backed parity tests proving preview and production agree on workflow semantics when running the same sealed artifact.
- Reproducibility tests that preserve run, bundle, and deployment identity in failure artifacts.

---

### T122. Build a toy example repo/app that demonstrates workflow deployment, visibility, upgrades, and preview/prod parity

**Depends on:** T116, T117, T118, T119, T120, T121

**Description**

Add a second workflow-focused example repo/app that teaches the operator and rollout surface: immutable workflow bundles, deployment activation, visibility inspection, accepted/rejected updates, continue-as-new, and preview/prod parity. This example should be small, reviewable, and backed by its own deterministic simulation tests.

**Implementation steps**

1. Add a compact toy app that demonstrates:
   - deployment of at least one native Rust or sandbox-authored workflow,
   - list/describe/history visibility,
   - an accepted update and a rejected update,
   - a continue-as-new or restart-as-new transition,
   - preview vs strict-preview vs production execution of the same logical workflow.
2. Document how the example uses:
   - bundle identities,
   - run histories,
   - visibility APIs,
   - upgrade boundaries,
   - preview/prod isolation.
3. Keep the example’s operator surface realistic enough to serve as a reference for real integrations without becoming product-sized.
4. Add deterministic simulation tests owned by the example itself for its documented rollout, visibility, and upgrade scenarios.
5. Ensure the example remains aligned with public crate APIs and does not require hidden internal wiring.

**Verification**

- Example-level deterministic simulation tests covering rollout, visibility, preview/prod parity, and upgrade flows.
- Regression tests proving the example continues to compile and run against public workflow APIs.
- Smoke checks ensuring the example can be followed as a teaching artifact without hidden setup.

---

## Suggested execution milestones

These are not separate tasks; they are useful “stop and validate” points before opening more parallel work.

### Milestone A — Minimal usable local engine
Complete: T01–T10, T04a

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
Complete: T20–T23b

At this point the system should additionally support:
- tiered cold storage,
- backup/disaster recovery,
- s3-primary durability semantics, and
- hybrid local change capture in s3-primary mode, and
- compatibility-checked durable metadata formats with FlatBuffers-backed control-plane metadata.

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
Complete: T33–T33d

At this point the system should have:
- deterministic seeded simulation coverage for the full DB / projection / workflow stack,
- property-based and parameterized invariant suites for low-level and cross-mode semantics,
- reusable failpoints/cut points for exact crash and retry-path testing, and
- a real object-store chaos suite for remote-storage behavior under network and HTTP faults.

### Milestone G — Embedded virtual filesystem library
Complete: T34–T40

At this point the system should additionally support:
- an embedded virtual filesystem crate on top of Terracedb,
- point-in-time snapshots and copy-on-write overlays for embedded sandboxes,
- KV state and tool-run audit history,
- durable clone/export flows instead of SQLite-file copies, and
- deterministic simulation coverage for the virtual filesystem crate itself.

### Milestone H — `terracedb-bricks` blob and large-object library
Complete: T41–T46

At this point the system should additionally support:
- an out-of-line `terracedb-bricks` blob / large-object library on top of Terracedb,
- current-state metadata and lifecycle activity rows for large objects,
- durable metadata and extracted-text search indexes maintained with projections,
- safe orphan-object handling and external-object GC, and
- deterministic simulation coverage for blob publish/read/delete/index/GC behavior.

### Milestone I — Analytical export crate
Complete: T47

At this point the system should additionally support:
- a separate Arrow-ecosystem export crate on top of Terracedb,
- snapshot and incremental derived exports for external analytics tooling,
- analytics-friendly Parquet-or-Arrow objects stored under dedicated export prefixes, and
- a clean separation between authoritative Terracedb backups and disposable analytical exports.

### Milestone J — Hybrid columnar-v2 and selective-read hardening
Complete: T48–T50a, T51–T57

At this point the system should additionally support:
- a formalized columnar-v2 contract with stable internal seams for format, scan, cache, and repair work,
- typed binary columnar substreams with an initial codec pipeline,
- compact per-part/per-version decode metadata with lazy full-schema materialization on reopen and hot read paths,
- sparse marks/granules and zone-map pruning,
- batch-based selective-read execution with PREWHERE-lite and late materialization,
- segmented remote caching with coalesced async reads and downloader election,
- immutable publish-last parts and sidecars with checksums, digests, quarantine, and repair paths,
- adaptive resource budgets and backpressure for mixed OLTP/OLAP pressure with conservative bounded defaults, and
- optional richer skip indexes, sidecars, and hot-row/compact-to-cold-columnar promotion paths that remain explicitly configurable rather than mandatory for the base engine profile.

### Milestone K — Hybrid telemetry example app
Complete: T58

At this point the system should additionally support:
- a small `telemetry-api` example that pairs point-read device state with historical columnar scans,
- a default example profile that demonstrates the universal Phase 11 features without turning on optional accelerants,
- an explicit accelerator profile that can enable richer skip indexes, sidecars, and hot-to-cold promotion without changing logical answers, and
- deterministic end-to-end simulation coverage for ingest, filtered scans, cold remote reads, restart/fault handling, and low-footprint operation for the example workload.

### Milestone L — Generalized current-state retention and ranking
Complete: T59–T62a

At this point the system should additionally support:
- threshold-based current-state retention over caller-defined sortable keys,
- rank-based current-state retention/materialization over computed measures with deterministic tie-breaking,
- explicit separation between sequence-based MVCC/CDC retention and generalized current-state retention,
- coordinated logical and physical reclamation behavior with deterministic simulation coverage for policy churn, crashes, and restart, and
- a small example app that demonstrates how threshold and rank-based retention policies are configured and observed in practice.

### Milestone M — Execution domains and colocated multi-DB operation
Complete: T63–T70

At this point the system should additionally support:
- hierarchical execution domains with fixed resource-manager and durability-class interfaces,
- a protected control-plane domain and dedicated internal durability lane for recovery-critical metadata,
- domain-aware scheduling, admission control, caches, and background work with bounded default policies,
- colocated multi-DB deployment and placement-policy support in one process,
- shard-ready placement/resource foundations without claiming completed physical data sharding,
- whole-system deterministic simulation and chaos coverage across domain composition, and
- a small example app that demonstrates two colocated workloads plus protected control-plane progress.

### Milestone N — Pressure-aware flushing and adaptive admission
Complete: T71–T76

At this point the system should additionally support:
- fixed interfaces for unified-log pressure, fine-grained memory accounting, and adaptive write-admission signals,
- explicit distinction between dirty bytes, queued-for-flush bytes, and bytes already being flushed,
- pressure-aware flush selection that optimizes for actual relief value rather than only immutable presence or coarse L0 heuristics,
- adaptive throttling/stalling that considers unified-log pressure, memory pressure, flush backlog, and L0/compaction pressure together,
- optional domain-aware pressure budgeting so one colocated workload cannot pin all mutable-memory or unified-log headroom,
- whole-system deterministic simulation, seeded fuzz, and focused chaos coverage for pressure spikes, failed flushes, and recovery, and
- an example app that demonstrates pressure-aware flushing and admission without changing logical answers.

### Milestone O — Opt-in physical sharding and virtual-partition resharding
Complete: T77–T83

At this point the system should additionally support:
- opt-in physical per-table sharding with fixed virtual-partition routing and shard `0000` compatibility for unsharded tables,
- durable shard-map metadata that defines logical ownership separately from execution-domain placement,
- shard-local commit-log lanes, memtables, flush outputs, maintenance work, and recovery behavior,
- conservative resharding that moves virtual partitions between physical shards without rehashing keys or rewriting row/column payload bytes,
- whole-system deterministic simulation and chaos coverage for routing, hot-shard skew, reshard cutover, and mixed sharded/unsharded operation, and
- a small `chat-rooms-api` example that demonstrates how to build a sharded application around a clear single-entity shard key.

### Milestone P — Historical workflow bootstrap and recovery
Complete: T84–T87

At this point the system should additionally support:
- configurable workflow source bootstrap from the beginning, the current durable frontier, or restored checkpoint state,
- explicit workflow recovery policies for fail-closed, checkpoint restore, append-only replay, and opt-in lossy fast-forward behavior,
- workflow checkpoints and optional trigger journaling for mixed source/timer/callback workflows, and
- deterministic simulation coverage for historical replay, checkpoint restore, and live-only attachment.

### Milestone Q — Kafka ingress
Complete: T88–T91

At this point the system should additionally support:
- a dedicated `terracedb-kafka` ingress crate,
- durable Kafka source-progress persistence coupled atomically to Terracedb writes,
- deterministic record filtering with skipped-record progress semantics,
- ordering-preserving partition materialization helpers for append-only and current-state surfaces, and
- deterministic broker simulation coverage for restart, duplicate delivery, and offset/write atomicity.

### Milestone R — Debezium CDC materialization
Complete: T92–T95

At this point the system should additionally support:
- a dedicated `terracedb-debezium` crate on top of Kafka ingress,
- PostgreSQL Debezium envelope decoding with explicit snapshot, tombstone, and transaction-metadata handling,
- first-class schema/table and row filtering over normalized Debezium events,
- EventLog, Mirror, and Hybrid materialization modes into ordinary Terracedb tables, and
- deterministic projection/workflow integration coverage over Debezium-derived tables.

### Milestone S — End-to-end CDC workflows and example app
Complete: T96–T99

At this point the system should additionally support:
- a small `order-watch` example app demonstrating Kafka + Debezium + projections + workflows together,
- explicit table-level and row-level filtering in that example's ingress path,
- explicit historical-bootstrap vs live-only workflow modes in a concrete application,
- cross-cutting deterministic simulation campaigns for the full CDC ingestion/materialization/runtime stack, and
- polished reference docs that show how to build on the new crates without coupling projections or workflows directly to Kafka.

### Milestone T — Sandbox capability policy, authoring UX, reviewed procedures, MCP exposure, and example app
Complete: T100–T107

At this point the system should additionally support:
- a shared capability/grant/manifest model for sandbox, migration, procedure, and MCP-facing access,
- host-authored presets/profiles that expand into explainable effective manifests,
- host-enforced database capability families with table allowlists, tenant scoping, query-shape restrictions, key-prefix and row-scope filters, visibility-index projection helpers, rate limits, and audit hooks,
- shell-facing bridges for selected capabilities or reviewed procedures that reuse the same authority model as typed imports,
- reviewed migration flows for catalog setup and app-schema evolution without claiming large backfill support,
- immutable published procedures with reviewed manifests, typed inputs/outputs, and caller-context-aware invocation,
- an external `terracedb-mcp` adapter that reuses the same permission and audit model as in-process sandboxes,
- optional host-mediated draft authorization plus foreground session-state surfaces that distinguish absent bindings from host-enforced denials,
- execution-domain isolation for sandbox, procedure, publication, and MCP work so resource exhaustion cannot trivially starve the enclosing app, and
- a small reference example app with its own deterministic simulation suites covering the intended end-to-end usage.

### Milestone U — Workflow runs, recovery journals, transition engine, and shared native/sandbox execution
Complete: T108–T114

At this point the system should additionally support:
- explicit workflow runs pinned to immutable execution targets,
- append-only hidden recovery journals plus lightweight savepoints, explicit lifecycle state, and reduced mutable workflow state,
- a single Rust-owned transition engine that applies admitted workflow input before any side effects escape,
- durable wait-set, retry, timer, and coordination semantics rather than transient worker-local state,
- both native Rust workflows and sandbox-authored workflows running on the same durable engine semantics, and
- a small example repo/app that demonstrates the shared engine across Rust and sandbox handlers with deterministic simulation coverage.

### Milestone V — Workflow deployment, visibility, upgrades, preview/prod parity, and operator surfaces
Complete: T115–T122

At this point the system should additionally support:
- immutable workflow bundle publication and native registration through a shared deployment manager,
- dedicated workflow list/describe/history visibility surfaces backed by a separate operator projection,
- query and update/control lanes distinct from ordinary durable trigger history,
- explicit continue-as-new / restart-as-new upgrade boundaries and rollout controls for long-running workflows,
- preview, strict-preview, and production execution flows that share one durable executor model, and
- a small example repo/app that demonstrates deployment, visibility inspection, accepted/rejected updates, and upgrade flows with its own deterministic simulation suites.

---

## Deferred items from the architecture

The following architecture sections are intentionally **not** decomposed into implementation tasks here because they are either explicitly future work or outside the requested scope:

- mount/protocol adapters for exposing the embedded virtual filesystem outside the process,
- zero-downtime upgrade handoff library,
- platform-specific deployment recipes and rollout automation.

If these are pulled into scope later, they should be added as a new phase after the current plan is stable rather than mixed into the core implementation DAG above.
