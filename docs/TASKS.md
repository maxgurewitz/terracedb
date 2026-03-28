The implementation-critical constraints are: a unified commit log; distinct visible and durable prefixes (`currentSequence` / `currentDurableSequence`, `scanSince` / `scanDurableSince`, `subscribe` / `subscribeDurable`); batch-oriented, frontier-pinned projections with deterministic multi-source ordering; and workflows driven by a durable inbox ordered per instance, with durable-fenced timers and durable outbox consumption. That means the fastest path is not “build everything in order,” but “freeze contracts early, deliver the authoritative durable path first, and let columnar/S3 extensions and higher-level libraries progress in parallel once the core surfaces stabilize.”     

## Delivery shape

Use three major delivery gates:

**Gate A — Core engine MVP:** row-table engine, commit path, MVCC reads, flush/recovery, compaction, visible + durable change-capture surfaces, scheduler, deterministic simulation harness.

**Gate B — Authoritative async stack:** projection runtime, durable timers/outbox helpers, workflow durable inbox/executor/recovery.

**Gate C — Storage/polish extensions:** tiered backup/offload, s3-primary, columnar tables, disaster recovery hardening, zero-downtime upgrade hooks, performance work.

This sequencing maximizes parallelization because libraries only need the authoritative DB contracts, not the full storage matrix. Columnar is explicitly a v1-specialized capability and should not sit on the critical path for the first correct system.  

## Parallel team lanes

Use six lanes from the beginning.

**Lane A — Contracts/runtime:** API, traits, config, telemetry, CI, docs-conformance.

**Lane B — Commit/visibility core:** commit mutex, sequence assignment, commit log append, group commit, deferred durability, visible/durable watermarks.

**Lane C — Reads/LSM:** memtable, MVCC reads, snapshots, SSTables, flush, compaction, merge/filter, GC.

**Lane D — Cloud/durability:** object store, backup/offload, s3-primary, DR, cache.

**Lane E — Libraries:** OCC helper, timers/outbox helpers, projections, workflows.

**Lane F — Verification/perf:** DST harness, fuzzing, fault injection, benchmarks, chaos tests.

Anything without an explicit dependency below should be treated as runnable in parallel.

---

# Phase 0 — Contract freeze and repo scaffolding

### T0.1 Architecture conformance matrix

Work: turn the design into a machine-checkable matrix: public API, durability semantics, ordering rules, replay guarantees, on-disk objects, and library invariants.

Depends on: none.

Tests: doc example compilation, API signature snapshot tests, conformance checklist reviewed before merge for every feature.

Parallelization: unlocks all other tasks by preventing semantic drift.

### T0.2 Monorepo/workspace, CI, release channels

Work: create crates/modules for core engine, storage backends, projection lib, workflow lib, test harnesses, examples, and benchmarks.

Depends on: none.

Tests: build matrix (Linux/macOS), lint, formatting, unit test smoke, sanitizer jobs, release artifact smoke.

Parallelization: can start immediately and stay ahead of all lanes.

### T0.3 Shared error model, tracing, metrics, config plumbing

Work: define stable error enums, tracing spans, metrics naming, config loading, feature flags.

Depends on: T0.1.

Tests: error round-trip tests, config validation tests, telemetry smoke tests.

Parallelization: supports all lanes without blocking feature work.

### T0.4 Deterministic simulation skeleton

Work: wire `FileSystem`, `ObjectStore`, `Clock`, and `Rng` abstractions into a minimal DST harness and seed-replay runner.

Depends on: T0.2.

Tests: same-seed identical-log meta-test, virtual clock smoke, crash injection smoke, deterministic replay in CI.

Parallelization: lets Lane F validate features as soon as they appear. 

---

# Phase 1 — Core runtime abstractions

### T1.1 `FileSystem` / `ObjectStore` / `Clock` / `Rng` interfaces

Work: implement traits, production adapters, and in-memory fakes.

Depends on: T0.1, T0.2.

Tests: contract tests per trait, cancellation/timeout tests, injected failure tests, fake-vs-prod parity tests.

Parallelization: unblocks core engine, cloud, and DST lanes simultaneously.

### T1.2 DB config, table handles, catalog skeleton

Work: implement `DBConfig`, `table(name)`, `createTable`, table metadata handling, scheduler injection points.

Depends on: T0.1, T0.2.

Tests: create/open/lookup tests, metadata persistence tests, duplicate table rejection tests.

Parallelization: can run alongside T1.1.

### T1.3 `WriteBatch` / `ReadSet` / typed API shells

Work: add in-memory batch accumulation, read-set accumulation, and API shells that later plug into commit/read paths.

Depends on: T0.1.

Tests: batch mutation tests, cross-table batch tests, read-set accumulation tests.

Parallelization: needed by commit, OCC, projections, workflows.

### T1.4 Common key encoding helpers

Work: define user-key/version-key encoding, table IDs, separator rules, cursor encode/decode helpers.

Depends on: T0.1.

Tests: golden encoding tests, ordering tests, fuzz round-trips, reserved-width forward-compat tests.

Parallelization: unblocks memtable, SSTables, commit log, scans.

---

# Phase 2 — Commit path, visibility, and durability

### T2.1 Sequence allocator and commit mutex

Work: implement monotonic sequence assignment and the single serialization point for conflict-checked writes.

Depends on: T1.3, T1.4.

Tests: concurrent commit ordering tests, monotonicity tests, no gaps/duplication tests.

Parallelization: must complete before authoritative commit semantics, but other storage tasks can proceed in parallel on mocked sequence sources.

### T2.2 Commit record encoder/decoder and buffered append path

Work: implement `CommitRecord` / `CommitEntry` format and in-memory append buffers.

Depends on: T1.1, T1.4, T2.1.

Tests: golden commit-log record tests, checksum tests, recovery decode fuzzing.

Parallelization: unblocks recovery and change capture.

### T2.3 Group commit coordinator

Work: implement leader election, fsync batching, waiter release, and contiguous batch semantics.

Depends on: T2.1, T2.2.

Tests: 100–1000 concurrent commit tests, shared-fsync tests, fsync failure propagation tests, “later commit never visible first” tests.

Parallelization: core critical path.

### T2.4 Deferred durability + `currentDurableSequence`

Work: implement durable-prefix tracking, explicit `flush()`, and decoupled visible vs durable progress.

Depends on: T2.2, T2.3.

Tests: visible-prefix ahead-of-durable-prefix tests, flush catch-up tests, crash-before-flush drops > durable-prefix tests, `currentDurableSequence() <= currentSequence()` invariant tests.

Parallelization: unblocks durable scanning and authoritative libraries. 

### T2.5 Conflict detection at commit

Work: implement read-set validation against committed modifications since read sequence.

Depends on: T1.3, T2.1.

Tests: point-conflict tests, no-conflict success tests, concurrent writers/readers race tests, retry-loop tests.

Parallelization: unlocks OCC helper and uniqueness examples.

### T2.6 Visibility publication and subscriber internals

Work: publish visibility in contiguous sequence order and maintain coalescing visible/durable subscriber state.

Depends on: T2.3, T2.4.

Tests: contiguous visibility tests, coalescing-notification tests, durable subscription lag tests, slow-subscriber bounded-memory tests.

Parallelization: needed by change capture and async libraries. 

---

# Phase 3 — In-memory MVCC read path

### T3.1 Memtable with versioned keys

Work: implement concurrent skip-list memtable storing versioned keys and tombstones.

Depends on: T1.4.

Tests: point read tests, range scan ordering tests, tombstone visibility tests, concurrent insert/read tests.

Parallelization: can progress before full durability path is done.

### T3.2 Latest reads fenced by visible prefix

Work: make ordinary reads/scans observe only versions at or below the current visible frontier.

Depends on: T2.6, T3.1.

Tests: no-read-of-unpublished-version tests, concurrent commit/read fence tests.

Parallelization: core critical path for correctness.

### T3.3 Snapshots and pin registry

Work: implement cheap snapshots, snapshot release, and GC horizon pinning.

Depends on: T3.1, T2.1.

Tests: stable-snapshot tests under concurrent writes, unreleased-snapshot GC blocking tests.

Parallelization: unlocks OCC helper and historical reads.

### T3.4 `readAt` / `scanAt` / `scanPrefix`

Work: historical point/range reads using MVCC encoding and prefix scans.

Depends on: T3.1, T3.3.

Tests: historical read correctness tests, range-scan-at-sequence tests, prefix-scan correctness tests, `SnapshotTooOld` tests with forced GC.

Parallelization: needed by frontier-pinned projections and durable timers. 

---

# Phase 4 — Local persistence, SSTables, recovery

### T4.1 Manifest, `CURRENT`, and local layout

Work: implement manifest format, generationing, checksums, atomic `CURRENT` swap, and local file layout.

Depends on: T1.1, T1.2.

Tests: crash-mid-manifest-update tests, corrupt-latest-fallback tests, directory fsync tests.

Parallelization: can run in parallel with row SSTable work.

### T4.2 Row SSTable writer/reader + bloom filters

Work: row SSTable blocks, indexes, bloom filters, iterators.

Depends on: T1.4.

Tests: writer/reader round-trip tests, bloom false-positive envelope tests, iterator seek tests, corruption detection tests.

Parallelization: independent from commit log once key encoding is stable.

### T4.3 Memtable flush pipeline

Work: rotate memtable, write SSTable, update manifest, advance `lastFlushedSequence`.

Depends on: T3.1, T4.1, T4.2, T2.2.

Tests: flush ordering tests, crash-between-flush-steps tests, recovery from partially flushed state.

Parallelization: core critical path for persistence and recovery.

### T4.4 Recovery replay from manifest + commit log

Work: open DB from manifest, rebuild indexes, replay commit log tail newer than flushed sequence.

Depends on: T2.2, T4.1, T4.3.

Tests: crash-at-every-write-path-step tests, prefix-consistency tests, recovery idempotence tests.

Parallelization: must land before cloud DR and before authoritative library recovery tests.

### T4.5 MVCC version GC horizon

Work: implement old-version reclamation based on retention + oldest active snapshot.

Depends on: T3.3, T4.3.

Tests: safe-drop tests, snapshot-pinned version tests, retention-expiry tests.

Parallelization: can run with compaction/filter work.

---

# Phase 5 — Compaction, merge, filter, scheduler

### T5.1 Compaction planner/executor

Work: leveled/tiered/FIFO job selection, file rewrite, manifest swap, obsolete file cleanup.

Depends on: T4.2, T4.3, T4.1.

Tests: logical-equivalence-after-compaction tests, overlapping-level merge tests, crash-during-compaction tests.

Parallelization: unlocks realistic long-run testing.

### T5.2 Merge operator engine

Work: partial/full merge execution during read, flush, and compaction.

Depends on: T3.4, T5.1.

Tests: associative regrouping tests, non-commutative order tests, long-chain collapse tests, recovery parity tests.

Parallelization: can be implemented while filters and scheduler proceed. 

### T5.3 Compaction filter / TTL

Work: deterministic TTL filtering using engine-supplied time and snapshot safety.

Depends on: T5.1, T3.3, T1.1.

Tests: TTL expiry tests with virtual clock, active-snapshot no-drop tests, replay determinism tests.

Parallelization: independent from scheduler and libraries.

### T5.4 `tableStats()` / `pendingWork()` / scheduler interface

Work: expose compaction debt, memtable pressure, local vs S3 bytes, pending jobs, and throttle hooks.

Depends on: T4.3, T5.1.

Tests: stats accuracy tests, pending-work lifecycle tests, scheduler callback tests.

Parallelization: unblocks background policy tuning and simulation scheduler.

### T5.5 Guardrails and write backpressure

Work: forced flush on memory exhaustion, forced L0 compaction/stall, eventual backup/offload execution.

Depends on: T5.1, T5.4.

Tests: memory-pressure tests, L0 hard-ceiling tests, deferred-work eventual-execution tests.

Parallelization: can proceed while cloud work is landing.

---

# Phase 6 — Change capture and retention

### T6.1 Per-table segment metadata and in-memory index

Work: commit-log segment footers, per-table ranges, sparse block index, recovery rebuild of segment index.

Depends on: T2.2, T4.4.

Tests: segment seek tests, rebuild-from-footer tests, per-table filtering tests.

Parallelization: unblocks both visible and durable scanners.

### T6.2 `scanSince` visible surface

Work: iterate visible prefix, honor `LogCursor`, support cold fetch, gap-free within retained visible history.

Depends on: T6.1, T2.6.

Tests: resume-within-same-sequence tests, multi-op batch resumption tests, page-split tests, empty-until-quiescent tests.

Parallelization: needed by best-effort helpers and some internal maintenance.

### T6.3 `scanDurableSince` authoritative surface

Work: bind scanners to durable prefix and prevent reads beyond `currentDurableSequence()`.

Depends on: T2.4, T6.1.

Tests: durable-prefix fence tests, crash-before-flush replay tests, visible-vs-durable divergence tests.

Parallelization: hard prerequisite for authoritative projections/workflows/outbox. 

### T6.4 `subscribe` / `subscribeDurable`

Work: coalescing wakeups for visible and durable prefixes.

Depends on: T2.6, T6.2, T6.3.

Tests: initial-drain-required tests, coalesced wake tests, no-loss-when-drain-until-empty tests.

Parallelization: libraries can start using these as soon as they stabilize. 

### T6.5 Commit-log retention and `SnapshotTooOld`

Work: compute per-table oldest-retained history, GC segments, report `SnapshotTooOld` correctly even with shared segments.

Depends on: T6.1, T4.4.

Tests: per-table logical-retention tests, lagging-consumer storage-pressure tests, rebuild-after-history-loss tests.

Parallelization: needed before recomputation logic is complete.

---

# Phase 7 — Cloud/storage-mode implementation

### T7.1 Object store production adapter + S3 emulator

Work: real SDK implementation plus deterministic network-host emulator for DST.

Depends on: T1.1, T0.4.

Tests: PUT/GET/COPY/LIST contract tests, lost-response tests, stale-LIST tests, partition tests.

Parallelization: can run well before core engine fully finishes. 

### T7.2 Tiered backup uploader + active-tail upload policy

Work: upload sealed commit-log segments, optionally force-seal/upload active tail on timer, upload manifests/SSTables.

Depends on: T4.1, T6.1, T7.1.

Tests: RPO-bound tests with forced seal interval, crash-between-upload-and-manifest tests, orphan-object tests.

Parallelization: can run alongside offload and DR restore. 

### T7.3 Offload to cold S3 + local eviction

Work: move old SSTables to cold prefix, update manifest, reclaim local space, maintain hot/cold read path.

Depends on: T4.2, T4.1, T7.1.

Tests: offload-read transparency tests, manifest-switch tests, cache-hit/miss tests.

Parallelization: not on core critical path.

### T7.4 S3-primary mode

Work: visible-in-memory / durable-on-S3 semantics, flush-to-S3, hybrid visible scanning, durable scanning from S3.

Depends on: T2.4, T6.2, T6.3, T7.1.

Tests: same-process visible > durable tests, crash-after-visible-before-flush tests, post-flush recovery tests.

Parallelization: can proceed after durable-prefix plumbing, independent of libraries. 

### T7.5 Disaster recovery restore path

Work: load latest valid manifest, fetch hot SSTables, leave cold SSTables remote, replay commit-log tail.

Depends on: T7.2, T7.3, T4.4.

Tests: full-disk-loss restore tests, corrupt-latest-manifest fallback tests, DR parity tests.

Parallelization: should finish before GA, but not before first local-engine milestone.

---

# Phase 8 — Columnar tables

### T8.1 Schema meta-schema + validation

Work: schema model, field IDs, validation, compatibility rules, bindings.

Depends on: T1.2.

Tests: schema validation tests, incompatible-change rejection tests, golden-schema tests.

Parallelization: can start early and proceed independently.

### T8.2 Columnar flush writer

Work: convert row memtable contents to columnar SSTables with key index, sequence column, tombstone bitmap, per-column encoding.

Depends on: T4.3, T8.1.

Tests: row-to-column round-trip tests, null/default handling tests, mixed tombstone tests.

Parallelization: does not block projections/workflows.

### T8.3 Columnar read path + pruning

Work: point reads, range scans, requested-column fetch/decode.

Depends on: T8.2.

Tests: projection/read correctness tests, column-pruning tests, append-only workload benchmarks.

Parallelization: can run with S3 range work.

### T8.4 Columnar S3 range reads

Work: footer index fetch, per-column byte-range fetches, remote cache.

Depends on: T8.2, T7.1.

Tests: partial-range correctness tests, cold-read latency tests, footer corruption tests.

Parallelization: separate from local columnar reader once file layout is frozen.

### T8.5 Columnar merge + evolution

Work: row-kind tags, merge reconstruction, compaction rewriting, add/remove/rename fields.

Depends on: T5.2, T8.2, T8.3.

Tests: merge parity vs row format, schema evolution compatibility tests, read-after-compaction tests.

Parallelization: should land after basic columnar reader/writer, but still off the main critical path. 

---

# Phase 9 — Library foundations

### T9.1 OCC transaction helper

Work: snapshot + local write overlay + read-set checked commit helper.

Depends on: T3.3, T3.4, T2.5.

Tests: snapshot-isolation tests, read-your-own-writes tests, conflict retry tests.

Parallelization: unlocks workflow admission helper and app-level coordination. 

### T9.2 Watermark tracker utility

Work: reusable per-source waiter tracker.

Depends on: none beyond core types.

Tests: waiter ordering tests, resolved-immediately tests, concurrent waiter tests.

Parallelization: can land very early in library lane.

### T9.3 Authoritative durable timer helper

Work: package the durable-fenced timer loop pattern around `scanAt(..., currentDurableSequence())`.

Depends on: T3.4, T2.4, T4.4.

Tests: durable-only-fire tests, duplicate-fire idempotence tests, recovery tests.

Parallelization: can be built in parallel with outbox helper. 

### T9.4 Transactional outbox helper

Work: stable-key write helper plus durable consumer loop template.

Depends on: T6.3, T6.4.

Tests: replay-after-crash tests, duplicate-delivery/idempotency tests, cursor persistence tests.

Parallelization: hard prerequisite for workflow side effects. 

---

# Phase 10 — Projection library

### T10.1 Whole-sequence-run scanner

Work: implement `scanWholeSequenceRun(...)` so handlers never see partial same-sequence batches.

Depends on: T6.3.

Tests: page-limit-cuts-through-run tests, multi-entry same-sequence batch tests, cursor resume tests.

Parallelization: needed by all projection runtimes.

### T10.2 Single-source projection runtime

Work: durable source subscription, initial catch-up, batch handling, output+cursor atomic commit.

Depends on: T10.1, T9.2.

Tests: replay-after-crash tests, handler reprocessing tests, cursor/output atomicity tests.

Parallelization: can deliver early while multi-source work continues.

### T10.3 Frontier-pinned projection context

Work: `readAt`/`scanAt`-backed context for batch handlers.

Depends on: T3.4, T10.1.

Tests: frontier correctness tests, no-read-beyond-frontier tests, deterministic replay tests.

Parallelization: can progress alongside single-source runtime after MVCC reads exist. 

### T10.4 Deterministic multi-source runtime

Work: probe sources for next runs, choose lowest sequence then `def.sources` tie-break, commit vector frontier updates.

Depends on: T10.1, T10.3.

Tests: adversarial wake-order tests, same-sequence different-source tie-break tests, restart/resume determinism tests.

Parallelization: independent from multi-stage dependency propagation.  

### T10.5 Multi-stage dependencies + `waitForWatermark`

Work: dependency DAG, per-source watermarks, single-source exact transitive waits.

Depends on: T10.2.

Tests: downstream wait tests, timeout tests, dependency-cycle rejection tests.

Parallelization: can land before full multi-source provenance enhancement.

### T10.6 Recompute-from-SSTables + checkpoints

Work: checkpoint format, checkpoint restore, full rebuild from hot/cold SSTables after `SnapshotTooOld`.

Depends on: T6.5, T7.3, T8.4 (for columnar optimization), T10.2.

Tests: forced-history-loss rebuild tests, checkpoint resume tests, rebuild parity tests.

Parallelization: can start once single-source runtime exists.

### T10.7 Optional exact multi-source provenance propagation

Work: propagate source-frontier provenance through outputs so transitive waits across multi-source DAG nodes become exact instead of conservative.

Depends on: T10.4, T10.5.

Tests: exact-downstream-wait tests across multi-source DAGs, provenance merge tests.

Parallelization: not required for first correct projection runtime; good stretch goal because the latest doc treats this as extra metadata for exact waits, not a prerequisite for deterministic replay. 

---

# Phase 11 — Workflow library

### T11.1 Durable trigger admission staging helper

Work: stage trigger-sequence allocation, inbox row write, and associated cursor/progress write in one OCC unit.

Depends on: T9.1, T6.3.

Tests: concurrent admissions to same instance produce strict `triggerSeq` order, no gaps/dups, cursor+admission atomicity tests.

Parallelization: blocks the rest of workflow runtime.  

### T11.2 Per-instance executor + ready-instance scheduler

Work: lowest-pending-`triggerSeq` executor, at-most-one-active-instance guard, pluggable fair scheduler.

Depends on: T11.1.

Tests: single-instance serial-order tests, inter-instance fairness tests, no-two-executors-same-instance tests.

Parallelization: can run alongside event/callback/timer admission implementations once inbox format is stable. 

### T11.3 Durable event ingress

Work: tail source tables with `subscribeDurable`/`scanDurableSince`, admit event triggers to inbox, advance `sourceCursorTable` in same commit.

Depends on: T11.1, T6.3, T6.4.

Tests: crash-after-admission-before-execution tests, resume-from-durable-cursor tests, duplicate-admission conflict tests.

Parallelization: can land before timers/callbacks.

### T11.4 Durable callback admission

Work: admit callback trigger durably and only then return success to caller.

Depends on: T11.1.

Tests: no-ack-before-durable-admission tests, crash-after-admission replay tests, duplicate-callback dedupe tests.

Parallelization: independent from timer admission. 

### T11.5 Durable-fenced timer admission

Work: scan durable prefix of `timerScheduleTable`, admit self-contained timer triggers to inbox, then let executor apply/dequeue them.

Depends on: T11.1, T3.4, T2.4, T9.3.

Tests: do-not-admit-visible-only timers tests, overlapping-scan duplicate-admission tests, canceled/already-fired no-op tests.

Parallelization: can run once durable timer helper and inbox admission exist.  

### T11.6 Workflow state transition applier

Work: atomic state update + inbox ack + fired-timer delete + timer schedule/cancel + outbox writes.

Depends on: T11.2, T9.4.

Tests: crash-before-commit leaves inbox pending tests, crash-after-commit has state+outbox+ack tests, timer-delete no-op duplicate tests.

Parallelization: central workflow correctness task.

### T11.7 Workflow outbox delivery

Work: durable outbox consumer specialized for workflow side effects.

Depends on: T9.4, T11.6.

Tests: replay-after-crash tests, idempotency-key reuse tests, outbox cursor durability tests.

Parallelization: can run once workflow produces outbox rows.

### T11.8 Workflow recovery

Work: on restart, reload durable source cursors, replay inbox, replay outbox, admit overdue timers.

Depends on: T11.3, T11.5, T11.7.

Tests: full crash-restart matrix, mixed trigger ordering after recovery, overdue timer admission tests.

Parallelization: final authoritative workflow gate. 

---

# Phase 12 — Hardening, performance, and operations

### T12.1 Core DST matrix

Work: large seeded workloads for commit/read/flush/recovery/compaction/storage modes.

Depends on: Gates A features.

Tests: 10k-seed nightly runs, deterministic-log meta-test, crash-at-every-step tests.

Parallelization: Lane F can begin subsets earlier and expand continuously. 

### T12.2 Projection/workflow DST matrix

Work: seeded tests for durable ingress, frontier replay, multi-source ordering, workflow inbox/recovery, timer duplicates.

Depends on: T10.4, T11.8.

Tests: same-seed identical outcomes, replay determinism tests, recovery parity tests.

Parallelization: can start once each library feature lands.

### T12.3 Fuzzing/corruption/chaos

Work: fuzz commit-log decode, manifest parsing, SSTable decode, cursor decode, corrupted S3 object handling.

Depends on: corresponding parsers.

Tests: libFuzzer/AFL suites, corpus minimization, sanitizer runs.

Parallelization: independent and continuous.

### T12.4 Benchmarks and profiling

Work: micro + macro benchmarks for commit latency, group commit batching, scans, compaction, cold reads, projection lag, workflow throughput.

Depends on: relevant feature maturity.

Tests: benchmark regression thresholds in CI, flamegraphs, allocation tracking.

Parallelization: separate from correctness, should not block until regression budgets are set.

### T12.5 `prepareForShutdown()` / fast open / upgrade hooks

Work: implement graceful drain/flush/close semantics and validate fast reopen path.

Depends on: T4.4, T7.2, T7.4.

Tests: shutdown-reopen latency tests, crash-during-shutdown tests, upgrade handoff smoke tests.

Parallelization: can land late; not needed for core correctness. 

### T12.6 Docs, examples, reference app, operator tooling

Work: example apps for row-table MVP, projection demo, workflow order-processing demo, DR runbooks, integrity checker, repair CLI.

Depends on: most major features.

Tests: examples compile/run in CI, runbook validation, CLI smoke tests.

Parallelization: documentation lane can start early and deepen continuously.

---

# Recommended critical path

If the goal is the **fastest correct integrated system**, the serial path is:

T0.1 → T1.3/T1.4 → T2.1 → T2.2 → T2.3 → T2.4 → T3.1 → T3.3/T3.4 → T4.1/T4.2/T4.3/T4.4 → T5.1/T5.2 → T6.1/T6.3/T6.4/T6.5 → T9.1/T9.4 → T10.1/T10.2/T10.3/T10.4 → T11.1/T11.2/T11.3/T11.6/T11.8 → T12.1/T12.2.

Everything else is deliberately off that path:

* columnar: Phase 8,
* offload/DR/S3-primary: most of Phase 7,
* exact multi-source provenance: T10.7,
* zero-downtime upgrades: T12.5.

That gives you a usable **row-table, tiered-mode, authoritative durable-stream system** first, then layers projections and workflows, then broadens storage modes and performance.

# Suggested staffing to maximize parallelization

With 8–10 engineers:

* 2 on commit/durability/change capture
* 2 on reads/SSTables/compaction
* 1 on cloud/backup/DR
* 1 on columnar
* 2 on libraries (1 projections, 1 workflows)
* 1 on simulation/fuzz/perf full-time
* 1 tech lead floating across contract hotspots

With 4–5 engineers, still keep the same order, but postpone:

* columnar until after Gate B,
* exact multi-source provenance until after stable projections,
* zero-downtime upgrades until after DR/perf.

The single biggest mistake to avoid is letting projections/workflows start before the **durable prefix** and **replay surfaces** are correct. The document makes those semantics load-bearing.   

A useful next step would be turning this into a dependency DAG with estimated engineer-weeks and a staffing plan for your actual team size.
