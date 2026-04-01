# LSM-Based Embedded Database — Architecture

## Overview

An embedded database library (not a server) built on LSM tree fundamentals. The engine exposes a small set of composable, low-level primitives — tables, snapshots, sequence numbers, version-aware reads, atomic batches, conflict-checked commits, and durability controls. It supports two storage modes: **tiered** (SSD + S3) and **s3-primary** (memory + S3).

All I/O, time, and randomness are abstracted behind traits, enabling deterministic simulation testing in the style of FoundationDB. The engine is verified via large-scale randomized scenario generation with fault injection and invariant checking, reproducible from a seed.

Higher-level features — OCC transactions, projections, windowed aggregations, change feeds, durable timers, workflows, external stream ingress from systems such as Kafka and Debezium, embedded virtual filesystems, embedded sandbox runtimes, and out-of-line blob storage — are not built into the engine. They are implemented as **separate libraries** on top of the core primitives, sharing the same process, DB instance, and async runtime. This document describes the core engine first (Part 1), then the composition patterns the libraries use (Part 2), then the projection library (Part 3), workflow library (Part 4), the embedded virtual filesystem and sandbox-runtime libraries (Part 5), and the `terracedb-bricks` blob/large-object library (Part 6).

When multiple DB instances, future physical shards, or attached subsystems share one process, resource isolation is expressed through **execution domains**. An execution domain is a named placement and budgeting boundary for CPU, memory, local I/O, remote I/O, and background work. Domains affect where work runs and what it may consume, but they do not change correctness semantics such as commit ordering, visibility, durability, or recovery. A unit of work may carry both an execution domain and a **durability class**; domains control resource isolation, while durability classes control persistence-path semantics.

### Load-Bearing Decisions

These are the architectural choices that most constrain the rest of the design:

- **Unified commit log** replaces a separate WAL and CDC log. Append is serialized within the commit mutex (sequence order). `scanSince` reads from it; crash recovery replays from it.
- **Group commit** amortizes fsync cost. Multiple concurrent commits share a single fsync. Deferred durability mode is available for workloads that prefer visibility-before-fsync.
- **Visibility follows sequence order.** `currentSequence()` returning S guarantees all sequences ≤ S are visible. Later sequences never overtake earlier ones.
- **Public API is async.** Operations may involve commit log durability or cold storage access. `WriteBatch` and `ReadSet` construction are the only sync operations.
- **Colocated derived state is synchronous** (one `WriteBatch`). Cross-entity derived state is asynchronous via `scanSince` with notification-driven consistency (`subscribe` + `waitForWatermark`).
- **Projections are deterministic state updaters with read access.** They declare output writes; they do not cause side effects or commit their own batches.
- **Workflows are stateful orchestrators with side effects.** They use the outbox pattern for at-least-once external delivery and durable timers for scheduling.
- **Historical workflow processing is configurable.** A workflow source may bootstrap from the beginning, from the current durable frontier, or from restored checkpoint/replay state; running a workflow "from the start" is not mandatory.
- **External stream ingress is a library boundary, not an engine feature.** Kafka consumers persist source progress atomically with writes into ordinary Terracedb tables; Debezium support composes on top of Kafka ingress rather than extending the engine.
- **Embedded virtual filesystems are libraries, not engine modes.** Their current-state filesystem/KV/tool rows live in ordinary tables; timelines and watchers are layered on top of append-only activity rows and change capture.
- **Embedded sandbox runtimes are libraries on top of `terracedb-vfs`, not engine modes.** Sandboxes get their own session/overlay, capability model, host-disk/git interop, and editor-facing read-only view layer without turning the engine into a shell, CLI, or host-filesystem runtime.
- **Migration, query-sandbox, and stored-procedure surfaces are capability-driven libraries layered above the sandbox runtime.** Guest code should receive explicit versioned modules with host-enforced policy, not raw engine handles.
- **External agent protocols such as MCP are adapter layers above the library stack, not engine or VFS modes.** They should reuse the same capability and audit model as in-process sandboxes instead of introducing a parallel permission system.
- **Blob and large-object storage are libraries, not engine value variants.** Large bytes live out-of-line in a blob store; Terracedb stores metadata, references, and derived indexes.
- **Virtual filesystem compatibility is semantic, not SQLite-format compatibility.** The goal is to provide an in-process virtual filesystem/KV/tool model on Terracedb, not SQL, a single-file transport format, or SQLite WAL behavior.
- **Event sourcing is recommended** for data that drives history-dependent projections. Mutable-record projections cannot be safely recomputed from SSTables after `SnapshotTooOld`.
- **Append-only ordered source tables are the replay surface for external CDC.** For Kafka-partitioned or Debezium-derived histories, deterministic replay order must be encoded in the Terracedb source keys and table layout; current-state mirrors are convenience read surfaces, not substitutes for replayable history.
- **Execution domains are resource-isolation boundaries, not correctness boundaries.** A unit of work may be assigned both an execution domain and a durability class. Domains control placement and budgets; durability classes control persistence-path semantics. Changing domains may affect latency, throughput, and backlog behavior, but not logical outcomes.
- **Control-plane work may use a protected domain and internal durability lane.** Catalog, manifest, schema, cursor, and other recovery-critical metadata must be able to make progress even under sustained user-data load.
- **Tokio is the sole runtime.** io_uring is an optional backend optimization behind the `FileSystem` trait, not a semantic requirement.
- **Deterministic simulation testing** covers the full stack — DB, projections, workflows, and higher-level libraries such as the embedded virtual filesystem layer — via injected I/O traits, virtual clock, and seeded PRNG.

Note on examples: Code snippets in this document are written in TypeScript-style pseudocode for readability and to make control flow and APIs easier to follow. They are not intended as literal implementation code. The actual implementation is expected to be written in Rust, and the examples should be read as illustrating semantics, invariants, and library boundaries rather than exact syntax or concrete type signatures.

---

# Part 1: Core Engine

---

## API

The engine's public API is asynchronous because operations may involve commit log durability boundaries or cold storage access. Internal code may execute synchronously, but the public contract is uniformly async. The exceptions are pure local object construction (`writeBatch()`, `ReadSet.add()`) which never block.

```typescript
interface DBConfig {
  storage: StorageConfig
  scheduler?: Scheduler          // injected at open time; defaults to built-in scheduler
  execution?: ExecutionConfig    // optional placement/budget policy for colocated DBs/subsystems
}

interface ExecutionConfig {
  resourceManager?: ResourceManager
  foregroundDomain?: string      // default domain for foreground table work
  backgroundDomain?: string      // default domain for flush/compaction/offload work
  controlPlaneDomain?: string    // protected domain for catalog/manifest/schema/cursor work
}

interface DB {
  // Table access — sync, returns a handle (no I/O)
  table(name: string): Table
  createTable(config: TableConfig): Promise<Result<Table, CreateTableError>>

  // Snapshots — read-consistent point-in-time views
  snapshot(): Promise<Snapshot>

  // Sequence numbers — visible vs durable progress.
  currentSequence(): SequenceNumber          // highest visible sequence in this process
  currentDurableSequence(): SequenceNumber   // highest sequence known durable in the configured storage mode

  // Batch construction — sync, no I/O
  writeBatch(): WriteBatch
  readSet(): ReadSet

  // Commit — single gateway for all batch applies.
  // Async because it appends to the commit log and may fsync.
  commit(batch: WriteBatch, opts?: CommitOptions): Promise<Result<SequenceNumber, CommitError>>

  // Durability
  flush(): Promise<Result<void, FlushError>>

  // Visible change capture — may include committed-but-not-yet-durable entries in deferred modes.
  scanSince(table: Table, cursor: LogCursor, opts?: ScanOptions): Promise<Result<AsyncIterator<ChangeEntry>, ChangeFeedError>>
  // Coalescing visible-prefix watermark notifications for driving event loops.
  subscribe(table: Table): Receiver<SequenceNumber>

  // Durable change capture — authoritative consumers must use these variants.
  scanDurableSince(table: Table, cursor: LogCursor, opts?: ScanOptions): Promise<Result<AsyncIterator<ChangeEntry>, ChangeFeedError>>
  // Coalescing durable-prefix watermark notifications for driving event loops.
  subscribeDurable(table: Table): Receiver<SequenceNumber>

  // Scheduling observability
  tableStats(table: Table): Promise<TableStats>
  pendingWork(): Promise<PendingWork[]>
}

interface CommitOptions {
  readSet?: ReadSet              // if provided, conflict detection is performed
}

// Opaque resume position within the commit log.
// Encodes (sequence, op_index) internally. Obtained from ChangeEntry.cursor
// or from LogCursor.beginning(). Do not construct manually.
interface LogCursor {
  static beginning(): LogCursor  // cursor that starts from the earliest retained entry
}

interface ChangeEntry {
  key: Key
  value: Value | null            // null for deletes
  cursor: LogCursor              // opaque resume position — persist this, pass to scanSince
  sequence: SequenceNumber       // exposed for watermark/visibility comparisons, not for resumption
  kind: "put" | "delete" | "merge"
  table: Table                   // source table — used by multi-source projections
}

interface Table {
  name: string
  read(key: Key): Promise<Result<Value | null, ReadError>>
  write(key: Key, value: Value): Promise<Result<SequenceNumber, WriteError>>
  delete(key: Key): Promise<Result<SequenceNumber, WriteError>>
  merge(key: Key, delta: Value): Promise<Result<SequenceNumber, WriteError>>
  scan(start: Key, end: Key, opts?: ScanOptions): Promise<Result<AsyncIterator<[Key, Value]>, ReadError>>
  scanPrefix(prefix: KeyPrefix, opts?: ScanOptions): Promise<Result<AsyncIterator<[Key, Value]>, ReadError>>

  // Version-aware reads
  readAt(key: Key, seq: SequenceNumber): Promise<Result<Value | null, ReadError | SnapshotTooOld>>
  scanAt(start: Key, end: Key, seq: SequenceNumber, opts?: ScanOptions): Promise<Result<AsyncIterator<[Key, Value]>, ReadError | SnapshotTooOld>>
}

interface ScanOptions {
  reverse?: boolean
  limit?: number
  columns?: string[]  // column pruning for columnar tables
}

interface Snapshot {
  sequence: SequenceNumber
  read(table: Table, key: Key): Promise<Result<Value | null, ReadError>>
  scan(table: Table, start: Key, end: Key, opts?: ScanOptions): Promise<Result<AsyncIterator<[Key, Value]>, ReadError>>
  scanPrefix(table: Table, prefix: KeyPrefix, opts?: ScanOptions): Promise<Result<AsyncIterator<[Key, Value]>, ReadError>>
  release(): void
}

interface WriteBatch {
  // Sync — accumulates operations in memory, no I/O
  put(table: Table, key: Key, value: Value): void
  merge(table: Table, key: Key, delta: Value): void
  delete(table: Table, key: Key): void
}

// ReadSet is a local accumulator, no I/O. Created via db.readSet().
interface ReadSet {
  add(table: Table, key: Key, atSequence: SequenceNumber): void
}
```

`execution` is optional. Simple single-DB embeddings can omit it and use built-in defaults. When multiple DBs or future shards share one process, the same `ResourceManager` may be shared across openings so they draw from a common process-wide budget while still retaining per-domain isolation.

`table(name)` is a synchronous handle lookup — it returns an in-memory reference to an already-created table. No I/O, and no `Result` in the guaranteed-existing path. It panics if the table is missing; use `tryTable(name)`/`try_table(name)` when existence is not guaranteed. `createTable(config)` is async and fallible because it mutates catalog metadata, which may touch durable storage.

### Design Notes

Every committed write — whether via `table.write()` or `db.commit(batch)` — is assigned a monotonically increasing **sequence number**. This is the single source of ordering truth for the engine. Sequence numbers drive MVCC visibility, merge operand ordering, change feed cursors, projection watermarks, and conflict detection.

`table.write()`, `table.delete()`, and `table.merge()` are standalone single-key operations. They are equivalent to a single-entry `WriteBatch` committed without a read set.

`table.delete()` is a convenience for writing a tombstone. Under the hood it follows the same write path (commit log append, memtable insert), and the tombstone is resolved during compaction.

`WriteBatch` is a mutable container for accumulating writes. It has no `commit()` method — all commits go through `db.commit(batch, opts?)`. This is the single gateway for applying writes atomically. If `opts.readSet` is provided, the engine acquires the commit mutex, checks whether any key in the read set has been modified since the read's sequence number, and aborts with `ConflictError` if so. If no read set is provided, the commit applies atomically without conflict checking.

`commit()` always means: atomically accepted, assigned a sequence number, and visible to subsequent reads. **Durability on return depends on the configured durability mode** — in group commit mode (the default for tiered storage), writes are durable when `commit()` returns; in deferred durability mode and S3-primary mode, writes are visible but not durable until `flush()`.

#### Commit Path and Group Commit

The engine uses **group commit** to amortize fsync cost across concurrent committers. The commit path is:

```
1. Acquire commit mutex
2. Conflict-check read set (if present)
3. Assign sequence number S
4. Append CommitRecord to in-memory commit log buffer
5. Release commit mutex
6. Wait for group commit leader to fsync the batch
7. Insert entries into memtable
8. Publish visibility (advance currentSequence to S, in sequence order)
```

Steps 1–5 are serialized by the mutex. The commit log buffer is appended within the mutex, so **append order is exactly sequence order**. The mutex critical section is brief — conflict check (proportional to read set size) + sequence assignment + in-memory buffer append.

After the mutex is released, the committer waits for the current batch to be fsynced. A **group commit leader** (the first committer in each batch, or a background task on a short timer) fsyncs all buffered commit records as a single I/O operation. All waiters in that batch are released together.

After fsync, each committer inserts its entries into the memtable and publishes visibility. **Visibility is published in sequence order**: committer S cannot advance `currentSequence()` until all sequences < S are also visible. This is enforced via an internal per-sequence completion tracker — each committer marks its sequence as ready after memtable insertion, and `currentSequence()` advances to the highest contiguous completed sequence. This ensures that if `currentSequence()` returns S, all sequences ≤ S are visible — not just S itself.

**Ordering guarantees:**
- Commit log records are physically ordered by sequence number (appended within the mutex).
- Fsync batches preserve this order — a batch contains a contiguous range of sequences.
- Visibility is published in sequence order. Later sequences never become visible before earlier ones.
- `scanSince` reads the commit log in physical order, which is sequence order.

**Throughput:** group commit amortizes fsync cost. Instead of one fsync per commit (~5-10k commits/sec on NVMe), you get one fsync per batch. Under concurrent load with batch sizes of 100-1000, throughput can reach hundreds of thousands of commits/sec from the same hardware.

**`table.write()` and single-key operations** are equivalent to single-entry batches and participate in group commit automatically. High-throughput callers should still prefer `db.commit(writeBatch)` with multiple entries when possible, but single-key writes are not penalized by a dedicated fsync.

#### Deferred Durability Mode

As an alternative to group commit, the engine supports a **deferred durability** configuration where commits are visible immediately but durable only after `flush()`:

```
1. Acquire commit mutex
2. Conflict-check + assign sequence S + append to buffer
3. Release commit mutex
4. Insert entries into memtable
5. Publish visibility
   — visible but NOT durable until flush —
6. Background: fsync commit log periodically or on explicit flush()
```

This matches the S3-primary durability model: `db.commit()` returns after memtable insertion with no fsync. Durability is controlled explicitly via `flush()` or a configurable background interval. The data loss window is bounded by the flush interval.

Deferred durability gives the lowest write latency and highest throughput, at the cost of a bounded data loss window on crash. It is appropriate for workloads that can tolerate losing the last N milliseconds of writes, or that use `flush()` at application-defined checkpoints.

**Configuration:**
- **Tiered mode, default:** group commit. `db.commit()` is durable on return.
- **Tiered mode, deferred:** `db.commit()` returns before fsync. Durable after `flush()` or background interval.
- **S3-primary mode:** always deferred. No local fsync. Durable after `flush()` ships to S3.

#### Durability Mode Summary

| Property | Tiered (group commit) | Tiered (deferred) | S3-primary |
|---|---|---|---|
| Visible on `commit()` return? | Yes | Yes | Yes |
| Durable on `commit()` return? | Yes | No | No |
| Durable after `flush()`? | Yes | Yes | Yes |
| `scanSince` sees committed-but-not-durable entries? | No | Yes | Yes |
| `scanDurableSince` sees committed-but-not-durable entries? | No | No | No |
| Data loss window on crash | None | Flush interval or explicit checkpoint interval | Flush interval or explicit checkpoint interval |

#### currentSequence

`currentSequence()` is a cheap read of a monotonically increasing atomic counter. It reflects the highest sequence whose writes are visible in the memtable. **If it returns S, all sequences ≤ S are guaranteed visible.** It is:
- Monotonic — never decreases.
- Process-local — not a coordination primitive across processes.
- A visibility watermark — it may advance before background consumers (projections, workflows) have processed the corresponding entries.
- **Not a durability watermark** — in deferred durability mode or S3-primary mode, visible sequences may not yet be durable. See **Storage Modes** for durability semantics.

To observe visibility advances without polling `currentSequence()`, use `db.subscribe(table)` — it pushes a notification each time a commit touching that table becomes visible.

#### currentDurableSequence

`currentDurableSequence()` is the durable counterpart: a cheap, monotonically nondecreasing read of the highest sequence that is guaranteed recoverable in the configured storage mode. It is:
- Monotonic — never decreases.
- Process-local — it describes this DB instance's durable prefix, not cross-process coordination.
- A durability watermark — if it returns `D`, all sequences ≤ `D` are guaranteed durable/recoverable after crash.
- **Never ahead of visibility** — `currentDurableSequence() <= currentSequence()` always holds.
- Potentially behind visibility — in deferred durability mode and s3-primary mode, visible commits may run ahead until `flush()` or the next durability checkpoint.

In tiered group-commit mode, visibility and durability often move together in practice because group fsync completes before the commit becomes visible. But the public contract should still treat them as distinct concepts: `currentSequence()` is the visible prefix, `currentDurableSequence()` is the durable prefix.

To observe durable-prefix advances without polling `currentDurableSequence()`, use `db.subscribeDurable(table)` — it pushes a notification each time the table's durable prefix advances.

`snapshot()` captures a read-consistent view at the current sequence number. Snapshot creation is cheap (captures the current sequence and registers with the GC) and does not fail — the sequence "now" is always within the GC horizon. It is async for API uniformity and possible future routing/runtime reasons, though typical implementations complete immediately. Snapshots must be explicitly released; unreleased snapshots pin the MVCC GC horizon and prevent old versions from being reclaimed during compaction.

`scanPrefix(prefix)` scans all keys sharing a given prefix, following the same lexicographic ordering as the table's key space. This is used heavily in composition patterns (timers by fire-time prefix, bucketed windows, secondary indexes).

#### scanSince Semantics

The engine exposes **two related change-capture surfaces**:

- `scanSince(table, cursor)` iterates the **visible** prefix after `cursor`. In deferred-durability modes, this may include entries that are committed and visible in the current process but not yet durable.
- `scanDurableSince(table, cursor)` iterates the **durable** prefix after `cursor`. It never yields entries beyond `currentDurableSequence()`.

Both return one `ChangeEntry` per committed write operation — not just the latest value per key. Each entry includes the key, value, an opaque `cursor` for resumption, the `sequence` number for watermark comparisons, and the operation kind (`put`, `delete`, or `merge`).

The `cursor` is an opaque `LogCursor` that encodes `(sequence, op_index)` internally. This is necessary because a single `WriteBatch` may contain multiple writes to the same table, all sharing the same sequence number. A bare sequence number is not a valid resume token — persisting sequence `42` after processing the first of two entries at sequence 42 would lose the second entry on restart. The opaque cursor ensures gap-free resumption at any position within a batch.

This is the engine-level primitive for change capture. Unlike `scanAt` (which reads a point-in-time snapshot organized by user key), `scanSince` / `scanDurableSince` return entries ordered by when they were committed. The visible surface is appropriate for in-process best-effort maintenance; the durable surface is the correct primitive for authoritative consumers such as workflow ingress, outbox delivery, and any async state whose progress must survive crashes.

**Gap-free guarantee:** both APIs provide strict gap-free change delivery within the retained history window of the surface they expose. If the cursor precedes the oldest retained commit log history for the table, the call fails with `ChangeFeedError::SnapshotTooOld` rather than silently skipping entries. Ordinary scan I/O, corruption, or remote-read failures surface as `ChangeFeedError::Storage`. This is governed by **commit log retention**, not by SSTable compaction — these APIs read from the commit log, not the LSM. On receiving `SnapshotTooOld`, the caller should fall back to a full recomputation or rebuild from a checkpoint.

**Multiple writes to the same key:** if key K is written 3 times after cursor C, `scanSince(table, C)` yields all 3 entries in commit order. If a single `WriteBatch` contains multiple writes to different keys, each appears as a separate entry, all sharing the same sequence number but with distinct cursors.

**Implementation:** both APIs read from the unified **commit log**, not from the key-sorted LSM. `scanSince` is bounded by the visible prefix; `scanDurableSince` is bounded by the durable prefix. The commit log is a segmented append-only structure that serves both crash recovery and change capture. See the **Commit Log** section for the full design, including segment structure, per-table indexing, retention policy, and the interaction with compaction.

**Async iteration:** both scanning APIs return an `AsyncIterator` because they may need to fetch data from S3 for cold entries. Callers consume them with async-for or paged cursors.

#### subscribe Semantics

`subscribe(table)` and `subscribeDurable(table)` return coalescing `Receiver<SequenceNumber>` channels used to drive event loops without polling:

- `subscribe(table)` notifies when the **visible** prefix for that table advances.
- `subscribeDurable(table)` notifies when the **durable** prefix for that table advances.

The implementation is intentionally **coalescing**, not an unbounded per-commit queue. Each subscriber stores only the latest sequence watermark not yet observed by the receiver. If commits 101, 102, and 103 arrive before the subscriber wakes up, the receiver may observe only `103`. This is sufficient because the consumer must call `scanSince` / `scanDurableSince` to discover the actual entries.

**Subscriber lifecycle:** subscriptions are valid for the lifetime of the DB instance. Dropping the receiver deregisters the subscription. Multiple subscribers per table are supported — each receives an independent coalesced watermark stream.

**Interaction with scanning:** subscriptions tell you *when* new data might exist; scanning tells you *what* changed. The safe consumer pattern is always:

1. **initial drain before blocking** (to pick up backlog present before subscription), and
2. **drain until empty on each wake** (because a single page or wakeup is not a complete proof of quiescence).

Authoritative consumers must use the durable pair:

```typescript
const notifications = db.subscribeDurable(sourceTable)
let cursor: LogCursor = loadPersistedCursor()

async function drainAllAvailable() {
  while (true) {
    let madeProgress = false
    const entries = await db.scanDurableSince(sourceTable, cursor, { limit: BATCH_SIZE })
    for await (const entry of entries) {
      // process entry
      cursor = entry.cursor
      madeProgress = true
    }
    if (!madeProgress) break
  }
}

await drainAllAvailable()              // initial catch-up after restart
for await (const _seq of notifications) {
  await drainAllAvailable()            // drain-until-empty on every wake
}
```

Best-effort in-process maintenance may use the visible pair instead.

**Simulation testing:** in deterministic simulation, both subscription surfaces behave identically in shape — notifications are delivered through the tokio runtime that turmoil controls, preserving determinism.

#### flush Semantics

`flush()` is an async durability operation whose meaning differs by storage mode and durability configuration:
- **Tiered mode (group commit):** individual writes are already durable after group commit fsync, so `flush()` is primarily about forcing memtable rotation to SSTables and ensuring backup progress.
- **Tiered mode (deferred durability):** forces fsync of all buffered commit log data, then rotates memtable. This is the explicit durability checkpoint — writes committed since the last fsync become durable.
- **S3-primary mode:** ships all buffered writes (commit log segment + SSTables) to S3. This is the durability checkpoint — data is not durable until `flush()` resolves.

`flush()` is async because it may involve S3 uploads or disk I/O that should not block the calling thread.

#### Error Types

All read and write operations are fallible. Errors include:
- `WriteError` — commit log failure, disk full, throttled/stalled by scheduler, serialization failure.
- `ReadError` — I/O failure, corruption.
- `ChangeFeedError` — change-feed scan failure. `ChangeFeedError::SnapshotTooOld` means the requested cursor is older than retained commit-log history; `ChangeFeedError::Storage` means the scan hit an ordinary storage/runtime failure such as I/O, corruption, or remote-read failure.
- `SnapshotTooOld` — requested point-in-time read position is older than retained MVCC history for `readAt` / `scanAt`.
- `ConflictError` — read set violated during commit.
- `FlushError` — S3 upload failure, disk full.

---

## Tables

Tables are the unit of logical separation. Each table is an independent column family with its own configuration:

```typescript
interface TableConfig {
  name: string
  format: "row" | "columnar"
  mergeOperator?: MergeOperator
  maxMergeOperandChainLength?: number
  compactionFilter?: CompactionFilter
  bloomFilterBitsPerKey?: number
  historyRetentionSequences?: number
  compactionStrategy: "leveled" | "tiered" | "fifo"
  schema?: SchemaDefinition  // required for columnar tables
  metadata?: Record<string, any>  // user-defined, opaque to engine, read by scheduler
}
```

Each table has independent SSTables, compaction scheduling, and bloom filters. The engine does not interpret table contents, relationships, or metadata. `metadata` is passed through to the scheduler without interpretation; the user uses it to inform scheduling policy (priority, backpressure behavior, etc.). See **Scheduling** for details.

`mergeOperator` and `compactionFilter` are **runtime callbacks**, not durable catalog data. The durable catalog stores the rest of the table definition plus the table's stable ID; applications that rely on those callbacks re-register them after open against the existing table definition.

**A table's format applies everywhere** — on SSD and on S3. Row stays row, columnar stays columnar. There is no format conversion at the offload boundary. SSTables move from SSD to S3 as-is. This simplifies the backup and offload paths (exact same bytes, no transformation) and means the user commits to a format at table creation time.

### SSTable Formats

**Row-oriented:** values are opaque bytes. The engine does not interpret them. Efficient for point lookups and single-record retrieval. No schema required.

**Columnar:** values are structured records decomposed into versioned per-column storage. The architecture uses a `columnar-v2` base format with typed binary substreams, page/granule metadata, and selective-read execution, plus optional sidecars and optional compact-to-wide promotion for mixed OLTP/OLAP workloads. Requires a registered schema.

### Row-Oriented Tables

Row tables accept and return opaque bytes. The engine stores and retrieves them without interpretation:

```typescript
const events = db.createTable({ name: "events", format: "row" })
events.write(key, anyOpaqueBytes)
const value: bytes = events.read(key)  // same bytes back
```

### Columnar Tables

#### Schema Definition

Columnar tables require a schema. The schema is a **JSON document validated against an engine-defined meta-schema**. It is fully portable and language-neutral — it can be constructed programmatically, loaded from a file, sent over the wire, or stored in the DB itself. Any tool or language can produce and validate it.

```json
{
  "version": 1,
  "fields": [
    { "id": 1, "name": "userId",    "type": "string",  "nullable": false },
    { "id": 2, "name": "amount",    "type": "int64",   "nullable": false },
    { "id": 3, "name": "timestamp", "type": "int64",   "nullable": false },
    { "id": 4, "name": "category",  "type": "string",  "nullable": true, "default": null }
  ]
}
```

Supported field types: `int64`, `float64`, `string`, `bytes`, `bool`.

Field IDs (not names) are the stable identifier. Names are for human convenience and can change without affecting storage.

Language-specific bindings can provide convenience for constructing schemas and records. For example, a Rust derive macro could generate a `SchemaDefinition` from a struct at compile time — but this is optional ergonomics, not a requirement:

```rust
// Optional convenience — generates SchemaDefinition automatically
#[derive(ColumnarRecord)]
struct Metric {
    #[field(id = 1)]
    user_id: String,
    #[field(id = 2)]
    amount: i64,
    #[field(id = 3)]
    timestamp: i64,
}

// Equivalent to manually constructing the SchemaDefinition
let schema = SchemaDefinition {
    version: 1,
    fields: vec![
        FieldDef { id: 1, name: "user_id",   type: FieldType::String, nullable: false, default: None },
        FieldDef { id: 2, name: "amount",     type: FieldType::Int64,  nullable: false, default: None },
        FieldDef { id: 3, name: "timestamp",  type: FieldType::Int64,  nullable: false, default: None },
    ],
};
```

#### Write Interface

Writes to columnar tables accept structured records — maps of field IDs to typed values. The engine validates against the schema (correct types, non-null fields present, unknown fields rejected):

```typescript
const metrics = db.createTable({
  name: "metrics",
  format: "columnar",
  schema: metricsSchema,
})

// Write by field ID
metrics.write(key, Record.from({ 1: "alice", 2: 50, 3: 1711123200 }))

// Or by field name (resolved against the schema)
metrics.write(key, Record.fromNamed(metricsSchema, {
  userId: "alice",
  amount: 50,
  timestamp: 1711123200,
}))
```

#### Flush Path (Memtable → Columnar SSTable)

The memtable is always row-oriented regardless of table format (it's a sorted skip list of key-value entries). For columnar tables, the pivot from rows to columns happens at SSTable flush time as a single linear pass:

```typescript
function flushColumnar(memtable: Memtable, schema: SchemaDefinition): ColumnarSSTable {
  const keys: bytes[] = []
  const sequences: SequenceNumber[] = []
  const tombstones: boolean[] = []
  const columns: Map<number, { type: FieldType, values: any[] }> = new Map()

  for (const field of schema.fields) {
    columns.set(field.id, { type: field.type, values: [] })
  }

  for (const [encodedKey, value] of memtable.sortedIterator()) {
    const { userKey, seq } = decodeKey(encodedKey)
    keys.push(userKey)
    sequences.push(seq)

    if (isTombstone(value)) {
      tombstones.push(true)
      for (const col of columns.values()) col.values.push(null)
      continue
    }

    tombstones.push(false)
    const record = decompose(value, schema)
    for (const field of schema.fields) {
      columns.get(field.id).values.push(record[field.id] ?? field.default)
    }
  }

  const encodedColumns = new Map<number, bytes>()
  for (const [fieldId, col] of columns) {
    encodedColumns.set(fieldId, encodeColumn(col.type, col.values))
  }

  return buildColumnarSSTable(keys, sequences, tombstones, encodedColumns, schema)
}
```

#### Versioned Columnar Physical Model

The columnar format is versioned. The architecture for the next frozen format is a `columnar-v2` **base part** plus optional sibling sidecars:

- typed binary substreams for fixed-width numeric/bool values, null/present bitmaps, and offset-plus-bytes variable-width values,
- per-stream codec descriptors with an initial `None` / `LZ4` / `ZSTD` set and room for future composition,
- per-granule/page marks and page directories so readers do not load whole metadata arrays up front,
- base-format zone maps / min-max synopses on configured fields,
- per-part checksums plus compact digests embedded in publish metadata,
- optional, explicitly versioned sibling artifacts such as richer skip indexes and per-SSTable projection sidecars.

Zone maps and bounded caches are part of the base profile. Richer skip indexes, projection sidecars, compact-to-wide promotion, and any aggressive background behavior remain explicit opt-ins rather than mandatory baseline behavior.

#### Selective Read and Hybrid Layout

The `columnar-v2` execution path is intentionally selective rather than full-row-by-default:

- scans collect row refs in batches instead of materializing every candidate row up front,
- predicate columns can be fetched first (`PREWHERE-lite`) to produce a survivor bitmap,
- remaining projected columns are fetched only for surviving row refs,
- remote reads use bounded segmented raw-byte caching with coalesced async range reads and downloader election,
- decoded footer/page/metadata state can be cached separately from raw bytes,
- tables may optionally use compact-to-wide promotion so hot write-friendly segments serve point reads and short scans while colder data promotes into wide-columnar parts for analytical scans.

The base engine profile does not require compact-to-wide promotion or sidecars. Those remain explicit workload-driven accelerants.

#### Read Path

Point lookups still use key-oriented metadata, but wide columnar parts are optimized first for analytical scans and selective projection reads. Mixed OLTP/OLAP workloads can stay row-oriented or opt into the hybrid compact-to-wide path so hot segments remain write-friendly while colder data becomes wide-columnar.

```typescript
function readColumnar(sst: ColumnarSSTable, targetKey: bytes, schema: SchemaDefinition): Value | null {
  const rowIndex = sst.keyIndex.binarySearch(targetKey)
  if (rowIndex < 0) return null
  if (sst.tombstones[rowIndex]) return null

  const record = {}
  for (const field of schema.fields) {
    const colData = sst.getColumn(field.id)
    if (colData) {
      record[field.name] = colData.valueAt(rowIndex)
    } else {
      // Column missing in this SSTable — added after it was written
      record[field.name] = field.default
    }
  }
  return record
}
```

Scans with column pruning operate on row refs, survivor masks, and late materialization rather than eagerly decoding full rows:

```typescript
function scanColumnar(
  sst: ColumnarSSTable, startKey: bytes, endKey: bytes,
  predicate: Predicate,
  projectedColumns: number[]
): Iterator<[Key, Record]> {
  const candidateGranules = pruneGranules(
    sst.pageDirectory,
    sst.zoneMaps,
    startKey,
    endKey,
    predicate
  )

  for (const granule of candidateGranules) {
    const rowRefs = collectRowRefs(granule, startKey, endKey)
    const survivors = evaluatePredicate(
      readColumns(granule, predicate.columns, rowRefs),
      predicate
    )
    const projected = readColumns(granule, projectedColumns, survivors)
    yield* materializeRows(granule, rowRefs, survivors, projected)
  }
}
```

#### S3 Column Pruning

Columnar SSTables on S3 store footer/page-directory metadata plus typed substreams in stable byte ranges. Readers fetch the footer first, prune granules, then issue only the range reads needed for predicate and projected substreams:

```typescript
async function readColumnsFromS3(
  sstKey: string,
  predicate: Predicate,
  projectedColumns: number[]
): ColumnData {
  const footer = await readFooter(sstKey)
  const pageDirectory = await readPageDirectory(sstKey, footer)
  const candidateGranules = pruneGranules(pageDirectory, footer.zoneMaps, predicate)
  const ranges = planCoalescedRanges(candidateGranules, predicate.columns, projectedColumns)
  const bytes = await readCoalescedRanges(sstKey, ranges)
  return decodeRequestedSubstreams(bytes)
}
```

For a 200MB SSTable where a scan needs 2 of 20 columns and most granules are pruned, this fetches only the footer/page metadata plus the predicate and projected substreams for the surviving granules, not the entire object.

#### Columnar SSTable Physical Layout

```
[Versioned header]
[Key index / key marks]
[Sequence + row-kind metadata streams]
[Column 1 typed substreams]
[Column 2 typed substreams]
...
[Column N typed substreams]
[Per-granule/page marks]
[Zone-map synopses]
[Footer / page directory / checksums]
```

Optional skip indexes and projection sidecars are sibling artifacts tied to the base part lifecycle rather than separate logical tables. The footer remains discoverable from the tail so a reader can fetch metadata first, prune granules, and then fetch only the required substreams.

Immutable parts and sidecars publish **temp → finalize checksums/digests → publish/rename → visible**. Verification can quarantine a corrupt artifact; optional sidecars must fall back to the base part rather than changing answers or making the base part unreadable.

#### Schema Evolution

Schema changes are handled lazily — no rewriting of existing SSTables required:

**Add a field:** assign a new field ID (never reuse IDs). New SSTables include the column. Old SSTables don't have it — reads fill in the default value. Compaction eventually rewrites old SSTables with the new column.

**Remove a field:** drop it from the schema. Old SSTables still physically contain the column data — it's skipped during reads and excluded from new SSTables. Compaction eventually rewrites old SSTables without the removed column.

**Rename a field:** change the name in the schema, keep the field ID. No data change needed — field IDs are the stable identifier.

**Type changes:** not supported in-place. Requires creating a new field with a new ID, backfilling, then removing the old field.

#### Merge Operators with Columnar Tables

Merge operators operate on complete records, not individual columns. The columnar encoding is transparent to the merge operator:

1. `table.merge(key, delta)` writes the delta to the memtable as a row-oriented merge operand (same as row tables).
2. During flush, unresolved merge operands are written to the columnar SSTable as rows carrying a **row-kind tag** (`put`, `delete`, or `merge_operand`) in the metadata column alongside the tombstone bitmap. Merge operand rows follow the table schema for any fields present in the delta; absent fields are null.
3. During compaction, the engine reconstructs full records from columns, applies the merge operator, and writes the resolved result back as columns with a `put` row-kind tag.
4. Read-time merge (when operands span levels) also reconstructs records, merges, and returns the result.

The merge operator's interface does not change — it always receives and produces complete records regardless of storage format:

```typescript
class CounterMerge implements MergeOperator {
  fullMerge(key: Key, existing: Record | null, operands: Record[]): Record {
    let total = existing?.count ?? 0
    for (const op of operands) total += op.count
    return { ...existing, count: total }
  }
}
```

---

## Merge Operators

Merge operators enable blind-write deltas that are resolved during compaction, avoiding read-modify-write overhead on the write path.

```typescript
interface MergeOperator {
  fullMerge(key: Key, existingValue: Value | null, operands: Value[]): Value
  partialMerge(key: Key, left: Value, right: Value): Value | null
}
```

### Contract

Merge operators must be **deterministic**: the same inputs must always produce the same output, across compactions, crash/recovery paths, and platforms.

`partialMerge` must be **associative**: regrouping operands during flush or compaction must not change the final result. That is, for any operands a, b, c:

```
fullMerge(key, base, [a, b, c])
== fullMerge(key, base, [partialMerge(a, b), c])
== fullMerge(key, base, [a, partialMerge(b, c)])
```

**Commutativity is not required.** Non-commutative merge operators are supported — the engine evaluates operands in commit sequence order, and this ordering guarantee is part of the engine contract. The engine preserves this order across memtable flush, compaction, and recovery.

**`partialMerge` cannot express key deletion.** TTL cleanup and key expiry must use a separate `CompactionFilter`, not the merge operator. Returning `null` from `partialMerge` means "I cannot merge these operands — defer to `fullMerge`," not "delete this key."

### Operand Accumulation

Unresolved merge operands can accumulate between compactions, increasing read latency as more operands must be folded at read time. The engine supports a configurable max-operand-chain-length that triggers a forced collapse during reads when the chain exceeds the limit.

```typescript
interface CompactionFilter {
  filter(entry: {
    level: number
    key: Key
    value: Value
    sequence: SequenceNumber
    kind: "put" | "delete" | "merge"
    now: Timestamp              // engine-provided current time for TTL evaluation
  }): "keep" | "remove"
}
```

For wall-clock TTL, the user encodes the expiry timestamp in the value and the filter inspects it using the engine-provided `now`. The engine provides `now` rather than letting the filter call the system clock, ensuring deterministic behavior in simulation testing.

**Interaction with MVCC and snapshots:** a compaction filter must not remove data that is still visible to an active snapshot. The engine only passes entries to the filter if they are older than the oldest active snapshot's sequence number.

---

## Storage Modes

```typescript
type StorageConfig =
  | {
      mode: "tiered"
      ssd: { path: string }
      s3: { bucket: string, prefix: string }
      maxLocalBytes: number  // per-table size limit on SSD
      durability?: "group-commit" | "deferred"
      localRetention: "offload" | "delete"  // oldest-first reclaim policy once over budget
    }
  | {
      mode: "s3-primary"
      s3: { bucket: string, prefix: string }
      memCacheSizeBytes: number
      autoFlushIntervalMs?: number
    }
```

### Tiered Mode (SSD + S3)

Data starts on SSD and is reclaimed when a table exceeds its configured `maxLocalBytes` limit. In the default `localRetention: "offload"` mode, Terracedb uploads the oldest SSTables to S3 cold storage while preserving them for reads. In `localRetention: "delete"` mode, Terracedb expires the oldest SSTables instead of publishing them to the cold prefix. Separately, a continuous backup process replicates commit log segments and SSTables to S3 for disaster recovery. See **Backup and Replication** for details.

- **Write path:** commit log append (within mutex) → group commit fsync → memtable insert → visibility. Commit is durable after group fsync completes. In deferred durability mode, fsync is background/explicit. See **Commit Log** for the unified log design and **Commit Path and Group Commit** in the API Design Notes.
- **Flush to SSTable:** memtable → SSTable on local SSD.
- **Offload (cold storage):** in `localRetention: "offload"`, a background process monitors per-table size on SSD. When a table exceeds `maxLocalBytes`, it uploads the oldest SSTables to S3, updates the manifest, and reclaims local space — repeating until the table is back under the limit.
- **Delete retention:** in `localRetention: "delete"`, the same oldest-first selection logic removes SSTables from the live manifest instead of moving them to the cold prefix. Reads no longer see that expired data, and backup GC later removes the unreferenced remote copies.
- **Backup (replication):** background process continuously copies commit log segments and SSTables to S3. If the SSD is lost, the database can be fully recovered.
- **Read path:** memtable → local SSTables (bloom filter assisted) → remote SSTables. Columnar reads use footer/page-directory metadata, zone-map pruning, row-ref batches, PREWHERE-lite, late materialization, and bounded segmented raw-byte caching with coalesced remote reads.
- **Disaster recovery:** pull latest manifest from S3, download live SSTables, replay commit log tail.

### S3-Primary Mode

No persistent local disk. Memory is the hot tier, S3 is the source of truth.

- **Write path:** commit log append to in-memory buffer → memtable insert → ack. Data is in memory only until flushed. The in-memory commit log buffer becomes durable only on flush. See **Commit Log** for details on S3-primary mode behavior.
- **Durability** is controlled per-operation:
  - `table.write()` — buffered in memory, not yet durable. Fast.
  - `await db.flush()` — ships buffered commit log segment and SSTables to S3. Everything committed up to that point becomes durable.
  - `db.commit()` in s3-primary mode does **not** automatically flush. Use `flush()` explicitly for durability checkpoints.
- **Flush to SSTable:** memtable → SSTable uploaded directly to S3.
- **Read path:** memtable → row and columnar SSTables → S3 on demand. Columnar remote reads use the same selective-read path as tiered cold reads: footer/page-directory fetch, pruning, bounded segmented raw-byte caching, and coalesced remote reads. Point-read latency is still bounded by S3/object-store latency on cache miss, so this mode is best suited for write-heavy and scan-heavy workloads, not low-latency random-read OLTP.
- **Auto-flush:** configurable background interval bounds worst-case data loss window.

The calling pattern for high-throughput ingest:

```typescript
await table.write(key1, value1)  // buffered, visible, not yet durable
await table.write(key2, value2)  // buffered
await table.write(key3, value3)  // buffered
// ... thousands more ...
await db.flush()                  // one S3 PUT for the entire batch
```

---

## LSM Internals

### Write Path

All writes go to:

1. **Commit log buffer** — appended within the commit mutex to ensure sequence order. In tiered mode (group commit), the buffer is fsynced in batches by the group commit leader. In deferred durability and S3-primary modes, the buffer is fsynced on `flush()`. See **Commit Log** and **Commit Path and Group Commit** for details.
2. **Memtable** — concurrent skip list (e.g., `crossbeam-skiplist`), sorted in-memory structure supporting highly concurrent inserts from multiple threads.

Every write is assigned a sequence number at commit time. Non-transactional writes (`table.write()`) are equivalent to single-entry batches, participate in group commit, and receive their own sequence number.

When the memtable reaches a size threshold, it is flushed as an immutable **SSTable** (Sorted String Table).

### Compaction

Background process that merge-sorts overlapping SSTables from one level into the next. Each level is ~10x larger than the previous.

Compaction serves three purposes:

1. **Space reclamation** — removes obsolete versions and tombstones.
2. **Read optimization** — reduces the number of SSTables a read must check (reduces read amplification).
3. **Derived computation** — merge operators and compaction filters run during compaction for free since the I/O cost is already being paid.

Compaction backpressure: if writes arrive faster than compaction can keep up, L0 grows unbounded and read amplification spikes. The engine exposes compaction debt and L0 state via `tableStats()` and delegates throttling/stalling decisions to the user-provided **Scheduler**. See **Scheduling** for details.

### Amplification Trade-offs

- **Read amplification:** a point lookup may check memtable + multiple SSTable levels. Mitigated by bloom filters.
- **Write amplification:** a given byte gets rewritten during compaction multiple times over its lifetime. Amortized over large batches.
- **Space amplification:** stale/duplicate entries consume disk until compaction reclaims them.

### MVCC Key Encoding

Versions are encoded directly into the key bytes. The `CommitId` is bitwise-inverted and appended to the user key so that newer versions sort first in the LSM's lexicographic ordering:

```typescript
function encodeKey(userKey: bytes, id: CommitId): bytes {
  return concat(userKey, separator, invertBits(id.sequence))
}
// Read at sequence S: seek to encodeKey(key, CommitId { sequence: S }), take first result
```

The separator byte and fixed-width encoding of the `CommitId` prevent ambiguity between user keys and version suffixes. The encoding reserves a fixed width that accommodates the future addition of a shard identifier to `CommitId` without changing the byte layout. Bloom filters and prefix extractors operate on the user key prefix (before the separator).

### MVCC Garbage Collection

Old versions are dropped during compaction when they fall outside the GC horizon. Historical retention is **sequence-count based**, not wall-clock based. If `historyRetentionSequences` is unset for a table, historical reads remain available until ordinary compaction can prove older versions are obsolete. When it is set, the GC horizon is the older of:

- The per-table retention floor derived from `historyRetentionSequences`.
- The oldest active snapshot's sequence number.

```typescript
retentionFloor = currentSequence - (historyRetentionSequences - 1)
gcHorizon = min(retentionFloor, oldestActiveSnapshot ?? retentionFloor)
```

A compaction filter must not remove data still visible to an active snapshot. Long-running snapshots pin the GC horizon and prevent space reclamation — unreleased snapshots are a resource leak.

**History availability:** the engine uses `SnapshotTooOld`-style retention failures for any request that references history older than what is retained, but the surfaced error type and retention mechanism differ by operation:

- `readAt` / `scanAt`: fail when MVCC versions have been reclaimed from the LSM by compaction or version GC. Governed by the **MVCC GC horizon**.
- `scanSince` / `scanDurableSince`: return `ChangeFeedError::SnapshotTooOld` when commit log segments have been GC'd past the cursor's sequence. Governed by **commit log retention** (see **Commit Log → Retention and GC**).

These horizons may differ — commit log retention can be configured independently of MVCC version GC. Snapshot creation (`db.snapshot()`) always succeeds because it captures the current sequence, which is by definition within both horizons.

### Manifest

Metadata tracking which SSTables are live at each level, their key ranges, sequence-number ranges, checksums, and storage location (local path or remote object key). Table names and durable table configuration live in a separate catalog keyed by stable table IDs. See **Local Storage** for structure and atomicity guarantees.

### Local Storage

The local storage layout is a **directory of files**:

```
db/
  catalog/
    CATALOG.json          ← table IDs + durable table config
  manifest/
    MANIFEST-000012        ← current manifest
    MANIFEST-000011        ← previous (retained for recovery)
  commitlog/
    SEG-000045
    SEG-000046             ← active (append target)
  sst/
    table-000001/          ← per-table-ID SSTable directories
      0000/                ← shard index (always 0000 until physical sharding)
        SST-000001.sst
        SST-000002.sst
        ...
    table-000002/
      0000/
        SST-000003.sst
        ...
  CURRENT                  ← points to latest manifest file
```

The commit log is unified across all tables (see **Commit Log**). SSTables are grouped by stable table ID, each within a shard directory that defaults to `0000`. When physical sharding is added, additional shard directories (`0001/`, `0002/`, ...) appear alongside `0000/`, and the commit log splits into per-shard lanes. See **Future Extension: Physical Sharding**.

Each SSTable, commit log segment, and manifest version is an independent file. Compaction creates new SSTable files and writes a new manifest file; old files are deleted after the new manifest is confirmed. Commit log segments are retained according to the dual retention policy described in **Commit Log**.

A future version may support an optional single-file packaging mode for simplified distribution and deployment, but directory-based storage is the default and the implementation starting point.

#### Manifest Structure

```typescript
interface Manifest {
  generation: number          // monotonically increasing
  checksum: number            // integrity check over manifest body
  lastFlushedSequence: SequenceNumber
  sstables: Array<{
    tableId: number
    shard: number              // always 0 until physical sharding
    level: number
    localId: string
    filePath?: string         // local file path (if on disk)
    remoteKey?: string        // remote object key (if uploaded/offloaded)
    length: number
    checksum: number
    dataChecksum: number
    minKey: bytes
    maxKey: bytes
    minSequence: SequenceNumber
    maxSequence: SequenceNumber
    schemaVersion?: number    // for columnar SSTables
  }>
}
```

The S3-backed remote manifest extends this with the durable commit-log segment descriptors needed for recovery and `scanDurableSince` without reopening every segment object eagerly.

#### Crash Safety

The engine uses immutable manifest generations plus a `CURRENT` pointer for crash-safe manifest updates:

1. Each manifest version is written as a new immutable file with a generation number and checksum.
2. The `CURRENT` pointer file is updated to reference the new manifest.
3. On recovery, the engine reads `CURRENT`, validates the manifest checksum, and falls back to the previous generation if the latest is corrupt.
4. Old manifest files are retained up to a configurable limit for recovery.

Manifest writes are fsynced before the `CURRENT` pointer is updated. `CURRENT` itself is updated atomically (write to temp file → fsync temp file → rename → fsync parent directory). The parent directory fsync ensures the rename is durable on filesystems that may otherwise lose it across a crash.

---

## Commit Log

The engine maintains a single append-only **commit log** that serves two purposes: crash recovery and commit-ordered change capture (`scanSince`). There is no separate WAL and CDC log — the commit log is both.

### Motivation

`scanSince` needs a commit-ordered stream of mutations, retained beyond what crash recovery requires. A traditional WAL is truncated after memtable flush. A naive approach adds a second append-only log (a "CDC log") alongside the WAL, but both structures are structurally identical: append-only, commit-ordered, durable-before-ack, containing full mutation payloads. Writing every mutation to two such logs doubles append I/O and doubles fsync cost for no semantic benefit.

The unified commit log eliminates that redundancy. Mutations are appended once, fsynced once, and retained until they are no longer needed for either recovery or change capture.

### Record Format

Each committed batch produces one commit record:

```rust
struct CommitId {
    sequence: u64,
    // Reserved: shard identifier will be added here for physical sharding.
    // Encoding uses fixed width so existing data is forward-compatible.
}

struct CommitRecord {
    id: CommitId,
    entry_count: u16,
    entries: Vec<CommitEntry>,
    checksum: u32,
}

struct CommitEntry {
    op_index: u16,
    table_id: u32,
    kind: OpKind,       // put | delete | merge_operand
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}
```

Total order within the log is `(id.sequence, op_index)`. A batch with N entries across multiple tables produces N `CommitEntry` records sharing the same `CommitId`.

Full key/value payloads are stored inline. This decouples the commit log from the SSTable lifecycle — compaction, offload, and GC of the LSM do not affect commit log contents. `scanSince` never needs to chase pointers into SSTables that may have been compacted away.

### Segment Structure

The commit log is divided into fixed-size segments (e.g., 64–256 MB). Each segment is an immutable file once sealed.

```
commitlog/
  SEG-000001
  SEG-000002
  SEG-000003   ← active (append target)
```

Each sealed segment has a footer containing:

```rust
struct SegmentFooter {
    segment_id: u64,
    min_sequence: u64,
    max_sequence: u64,
    entry_count: u64,

    // Per-table presence and range — enables scanSince to skip irrelevant segments
    tables: Vec<TableSegmentMeta>,

    // Sparse index: sequence → byte offset within segment, one entry per block
    block_index: Vec<(u64, u64)>,

    checksum: u32,
}

struct TableSegmentMeta {
    table_id: u32,
    min_sequence: u64,
    max_sequence: u64,
    entry_count: u32,
}
```

The block index enables binary search within a segment for a target sequence. Per-table metadata enables `scanSince` to skip segments that contain no entries for the requested table.

### In-Memory Segment Index

The engine maintains a lightweight in-memory index mapping `(table_id, sequence_range) → segment_id` for all retained segments. This index is rebuilt from segment footers on recovery.

`scanSince(table, cursor)` consults this index to identify candidate segments without touching disk, then opens only the relevant segments. For workloads with many tables, this avoids scanning segment footers at read time.

### Commit Path

The commit log append happens within the commit mutex, ensuring records are physically ordered by sequence number. See **Commit Path and Group Commit** in the API Design Notes for the full pipeline.

**Tiered mode (group commit, default):**

```
1. Acquire commit mutex
2. Conflict-check + assign sequence S + append CommitRecord to buffer
3. Release mutex
4. Wait for group commit leader to fsync the batch
5. Insert into memtable
6. Publish visibility (in sequence order)
```

The group commit leader fsyncs one contiguous range of buffered records per batch. Sealed segments (full buffers) have their footer written and become immutable files.

**Tiered mode (deferred durability):**

```
1–3. Same as above (mutex + append)
4. Insert into memtable
5. Publish visibility
   — visible but not durable until background fsync or explicit flush —
```

**S3-primary mode** (always deferred):

```
1–3. Same as above (mutex + append to in-memory buffer)
4. Insert into memtable
5. Publish visibility
   — visible but not durable until flush ships to S3 —
6. On flush(): serialize buffered commit log segment + SSTables → S3 PUT
```

### S3-Primary scanSince Behavior

`scanSince` in S3-primary mode operates in **hybrid mode** for same-process consumers: it reads both durable segments on S3 and the in-memory commit log buffer. This allows projections and workflows running in the same process to consume recent changes immediately, without waiting for `flush()`.

The tradeoff: entries from the in-memory buffer are not durable. If the process crashes before `flush()`, those entries are lost, and projections/workflows will reprocess from their last persisted cursor (which points to durable history). This is consistent with S3-primary's overall durability model — nothing survives crash until flushed — but it means same-process `scanSince` consumers see a superset of what would survive a crash.

Remote consumers (separate processes reading from S3) can only read durable segments. They see entries only after `flush()`.

**Crash recovery in S3-primary mode:** after a crash, everything after the last `flush()` disappears consistently — source writes, projection cursor advances, projection output writes, workflow state transitions, timer schedules, and outbox entries. This is acceptable because all of these are written through the same DB instance, so recovery returns to the last durable prefix atomically. Projections and workflows resume from their last persisted (durable) cursors and reprocess any lost entries.

### scanSince Implementation

`scanSince(table, cursor)`:

1. Check `cursor` against the oldest retained sequence for this table (derived from the in-memory segment index, not from physical segment boundaries — see **Retention and GC** for the distinction). If the cursor's sequence is older than the oldest retained, return `SnapshotTooOld`.
2. Consult the in-memory segment index to find the first segment containing entries for `table` at or after the cursor's position.
3. Open that segment, use the block index to seek to the approximate offset.
4. Stream `CommitEntry` records where `table_id` matches and position is after the cursor, in `(sequence, op_index)` order.
5. Continue through subsequent segments.
6. For cold segments (uploaded to S3), fetch asynchronously — this is why `scanSince` returns `AsyncIterator`.

Within a global commit log, entries for many tables are interleaved. The per-table footer metadata and in-memory segment index minimize wasted I/O, but `scanSince` on a low-volume table in a high-volume log will still skip past interleaved entries from other tables within each block. For most workloads this is acceptable; if it becomes a bottleneck, per-table commit log segments are a future optimization (at the cost of more complex commit atomicity).

### Retention and GC

A commit log segment is retained until it is older than **both**:

- The **recovery minimum needed sequence**: the sequence of the last completed memtable flush. Recovery replays from this point forward.
- The **CDC minimum needed sequence**: the oldest sequence that any `scanSince` consumer might request across all tables. Derived from per-table retention configuration.

```
recovery_min = last_flushed_sequence
cdc_min = min(oldest_needed_sequence(table) for table in tables_with_retention)

segment_deletable = segment.max_sequence < min(recovery_min, cdc_min)
```

**Physical vs logical retention:** the retention unit is a shared commit log segment, but `SnapshotTooOld` is evaluated per-table. A segment may be retained because table A still needs it, even if table B's retention has long since moved past that sequence. The in-memory segment index tracks per-table sequence ranges within each segment, so `scanSince` for table B can report `SnapshotTooOld` even while the underlying segment is still physically present (retained for table A). Conversely, when a segment is GC'd, all tables lose access to the entries in that segment.

This means storage costs are driven by the table with the longest retention window or the slowest consumer — a single lagging projection can prevent GC of old segments. Monitor via `tableStats()` / `pendingWork()`.

When `scanSince` is not in use (no configured retention), the commit log degrades to a recovery-only log: segments are deleted as soon as their data has been flushed to SSTables.

When `scanSince` is in use, the commit log grows proportionally to retention window × write throughput. This is an explicit storage cost, manageable by tuning retention or triggering projection rebuilds from checkpoints.

### Interaction with Compaction

The commit log and the LSM are independent structures with independent lifecycles:

- **Compaction** operates on the LSM (SSTables). It collapses old versions, resolves merge operands, removes tombstones, reclaims space. It does not touch the commit log.
- **Commit log GC** deletes old segments based on retention policy. It does not affect SSTables.

This separation means:

- Compaction cannot break `scanSince`. Even if a key's old versions are compacted away in the LSM, the original mutations remain in the commit log until segment GC.
- `scanSince` reflects committed mutations, not current LSM state. A consumer may see a `put` for a key that no longer exists in the LSM because a `CompactionFilter` TTL'd it. This is correct — `scanSince` is a mutation log, not a state snapshot.
- The `CompactionFilter` receiving `now: Timestamp` for deterministic TTL evaluation has no bearing on commit log retention. TTL is an LSM concern; commit log retention is a CDC concern.

### Disaster Recovery Interaction

In tiered mode, the backup process uploads sealed commit log segments to S3 (replacing the earlier "WAL segment upload" framing). Recovery pulls the latest manifest, downloads live SSTables, and replays commit log segments newer than the last flushed sequence. The data loss window is bounded by how long committed bytes can remain only in the active local segment before that tail is sealed and uploaded. With a policy that forces active-segment seal/upload at least every `T` seconds, the bound is approximately `T`; without such a policy, the tail-loss window can be larger than the sealed-segment upload interval. In practice, the engine should either force-seal the active segment on a timer or upload the active tail incrementally if a tight RPO is required.

---

## Backup and Replication (Tiered Mode)

In tiered mode, S3 serves two independent purposes:

- **Cold storage:** SSTables *moved* from SSD to S3 by the offload process when a table exceeds `maxLocalBytes`. The data is no longer on the SSD.
- **Backup:** commit log segments and SSTables *copied* to S3 continuously while still living on the SSD. If the SSD is lost, the database can be fully recovered from S3.

These are separate background processes. Cold storage is about reclaiming SSD space. Backup is about surviving disk failure.

### Local vs S3 Representation

The local representation is a directory of files. The S3 representation is individual objects. The backup process uploads local files as standalone S3 objects:

```typescript
function onCatalogUpdated(catalog: Catalog) {
  await s3.put("backup/catalog/CATALOG.json", serialize(catalog))
}

function onSSTableCreated(sst: SSTableRef) {
  await s3.put(`backup/sst/table-${sst.tableId}/${sst.shard}/${sst.localId}.sst`, readFile(sst.filePath))
}

function onCommitLogSegmentSealed(segment: CommitLogSegmentRef) {
  await s3.put(`backup/commitlog/${segment.segmentId}`, readFile(segment.filePath))
}

function onManifestUpdated(remoteManifest: RemoteManifest) {
  const manifestKey = `backup/manifest/MANIFEST-${remoteManifest.generation}`
  await s3.put(manifestKey, serialize(remoteManifest))
  await s3.put(`backup/manifest/latest`, utf8(`${manifestKey}\n`))
}
```

Manifest uploads are **immutable and generation-numbered** on S3, not just a single mutable `latest` key. The `latest` object is a convenience pointer containing the newest manifest key, and recovery can also scan manifest generations and pick the highest valid one. The catalog remains a separate replicated object because table names and durable table configuration are not derivable from the manifest alone.

### Interaction Between Backup and Offload

The backup and offload processes share S3 upload work. The backup process uploads every SSTable eagerly (for durability). When the offload process later decides to evict an SSTable to cold storage, the S3 copy already exists. Offload copies to the cold prefix, updates the manifest, and reclaims local space. It does **not** eagerly delete the backup copy — GC owns all S3 object deletion to avoid having two deletion mechanisms:

```typescript
function offloadSSTable(sst: SSTableRef) {
  const coldKey = `cold/table-${sst.tableId}/${sst.shard}/${sst.minSequence}-${sst.maxSequence}/${sst.localId}.sst`
  await s3.copy(`backup/sst/table-${sst.tableId}/${sst.shard}/${sst.localId}.sst`, coldKey)

  updateManifest(sst.localId, { remoteKey: coldKey, filePath: null })
  deleteLocalFile(sst.filePath)
  // backup/sst/table-{id}/{shard}/{localId}.sst is now unreferenced — GC will clean it up
}
```

The backup process skips SSTables that have already been offloaded to cold storage.

### Ordering During Compaction

When compaction produces new SSTables and obsoletes old ones:

1. Write new SSTable files locally.
2. Upload new SSTables to S3.
3. Write and upload the new manifest (references new SSTables, drops old ones).
4. Only then delete obsolete local SSTable files.

If the process crashes between steps 2 and 3, new SSTables exist in S3 but nothing references them — harmless orphans. If it crashes between steps 3 and 4, old SSTables still exist locally — also harmless.

### S3 Object Lifecycle and GC

S3 objects are garbage collected using a **mark-and-sweep** approach:

1. The GC root set includes the retained manifest generations, the `backup/manifest/latest` pointer object, and `backup/catalog/CATALOG.json`.
2. Walk all retained manifests in the root set and collect the set of referenced SSTable and commit log segment keys.
3. List all objects under `backup/` and `cold/` prefixes.
4. Delete any object not referenced by the retained manifests/catalog and older than a configurable grace period.

The grace period prevents races where a new SSTable has been uploaded but the manifest referencing it hasn't been published yet. Implementations can make this fail-closed by storing a small birth record alongside each uploaded backup object (for example under `backup/gc/objects/...`) rather than relying on object-store listing timestamps.

### Disaster Recovery

If the SSD is unrecoverable:

```typescript
async function recover() {
  const catalog = await s3.get("backup/catalog/CATALOG.json")

  // 1. Find the latest valid manifest
  //    Try direct GET on 'latest' pointer first (avoids S3 LIST eventual consistency)
  //    Fall back to listing generations if 'latest' is missing or corrupt
  let manifestKey: string
  let manifest: Manifest
  try {
    manifestKey = decodeUtf8(await s3.get("backup/manifest/latest")).trim()
    manifest = await s3.get(manifestKey)
    validate(manifest)
  } catch {
    const generations = await s3.list("backup/manifest/")
    manifestKey = findLatestValidManifestKey(generations)  // highest generation with valid checksum
    manifest = await s3.get(manifestKey)
  }

  // 2. Download SSTables that were still on SSD
  for (const sst of manifest.sstables) {
    if (sst.remoteKey) continue  // already remote — leave on object storage
    const bytes = await s3.get(`backup/sst/table-${sst.tableId}/${sst.shard}/${sst.localId}.sst`)
    const localPath = `db/sst/table-${sst.tableId}/${sst.shard}/${sst.localId}.sst`
    writeFile(localPath, bytes)
    sst.filePath = localPath
  }

  // 3. Replay commit log segments newer than the last flushed sequence
  const segments = await s3.list("backup/commitlog/",
    { after: manifest.lastFlushedSequence })
  for (const seg of segments) {
    const bytes = await s3.get(seg.key)
    replayCommitLog(bytes, memtable)
  }

  // 4. Restore catalog + manifest locally
  writeCatalog(catalog)
  writeManifest(manifest)
}
```

The recovered database has hot SSTables in the local directory and cold SSTables on S3 — the same tiered structure as before the failure. The data loss window is bounded by the active-tail upload policy, not just the cadence for already sealed segments. For example, if the engine forces active-segment seal/upload at least once per second, the tail-loss window is ~1 second; otherwise it can be larger.

### S3 Layout (Tiered Mode)

```
backup/
  catalog/
    CATALOG.json
  commitlog/
    SEG-000045
    SEG-000046
    ...
  sst/
    table-000001/0000/SST-000001.sst   ← per-table-ID, per-shard (mirrors local layout)
    table-000001/0000/SST-000002.sst
    table-000002/0000/SST-000003.sst
    ...
  manifest/
    MANIFEST-000011                ← immutable, generation-numbered
    MANIFEST-000012
    latest                         ← convenience pointer
  gc/
    objects/
      <hex(object-key)>.json       ← optional birth metadata for grace-period GC

cold/                              ← offloaded SSTables (moved, not on SSD)
  table-000001/0000/00000100-00000500/SST-000045.sst
  table-000001/0000/00000501-00001200/SST-000078.sst
  ...
```

---

## Execution Model

The engine separates work into three classes with different performance characteristics and coordination requirements:

### Foreground Mutation

The fast path. Single-key or multi-key writes via `WriteBatch`:

- Write / delete / merge to any keys
- Derived state updates in the same `WriteBatch` (colocated projections)
- Lowest-latency path — commit log append + memtable insert

### Coordinated Operations

The conflict-checked path. Any operation that needs atomicity with conflict detection:

- `db.commit(batch, { readSet })` — the read set triggers conflict checking at commit time
- OCC transactions use this (e.g., unique constraint enforcement)
- Requires the commit mutex — serialization point
- Higher-latency path (proportional to read set size)

Writes _without_ a read set are also valid — they provide atomic write grouping without conflict detection. The read set controls whether conflict checking happens.

### Background Workers

Heavy, irregular, latency-tolerant work:

- Compaction (merge-sorting SSTables across levels)
- SSTable flush (memtable → disk)
- S3 upload / download (backup, offload, recovery)
- S3 GC (mark-and-sweep over unreferenced objects)
- View recomputation / rebuild from checkpoint

Background work is managed by the **Scheduler** and does not block the foreground mutation path. The engine enforces safety guardrails (forced flush on memory exhaustion, forced L0 compaction at hard ceiling) regardless of scheduler decisions.

### Internal Concurrency Details

- **Memtable:** a concurrent skip list (`crossbeam-skiplist`). Multiple writers can insert concurrently.
- **Commit log:** append is serialized within the commit mutex (part of the critical section), ensuring records are physically ordered by sequence number. Fsync is batched via group commit — one fsync per batch, not per commit.
- **Commit mutex:** held during conflict checking, sequence assignment, and commit log buffer append. The critical section is brief (proportional to read set size + buffer append). This is the serialization point.
- **Post-mutex commit path:** after the mutex is released, the committer waits for the group fsync (in group commit mode), inserts into the memtable, and publishes visibility in sequence order. Multiple committers may be in this phase concurrently, but visibility is strictly ordered — later sequences never become visible before earlier ones.
- **Reads:** concurrent at all times. Read path is memtable → SSTables. MVCC snapshots ensure readers never block writers.
- **Background work:** runs on separate tokio tasks / worker threads, concurrent with foreground mutation. Prioritized by the Scheduler.

---

## Runtime Stance

The engine is built around a **single async runtime model**:

- **Tokio** is the sole runtime for async execution, task scheduling, and concurrency.
- There is no split-runtime architecture (no Tokio + Glommio hybrid).

**io_uring** may be used as an optional backend optimization for local storage I/O (commit log append, SSTable reads/writes, compaction file I/O), hidden behind the `FileSystem` trait. It is:

- Linux-specific and optional
- A performance optimization, not a semantic requirement
- Not part of the engine's correctness model
- Invisible to deterministic simulation testing (the `FileSystem` trait boundary ensures this)

**Implementation priorities:**

- Tokio runtime, `std::fs` + `spawn_blocking` behind the `FileSystem` trait, correctness and semantics first.
- Optional optimizations later: batched sequence allocation, `io_uring` local backend, deeper local I/O tuning.

---

## Scheduling

The **background workers** execution class (see Execution Model above) includes several processes competing for resources: memtable flushes, compaction across multiple tables and levels, S3 backup, S3 offload, and (in user space) projection updaters. The engine delegates scheduling policy to a **user-provided scheduler** (injected at DB open time via `DBConfig.scheduler`), but enforces hard safety invariants that the scheduler cannot override.

### Execution Domains

When multiple DB instances, future shards, or attached subsystems share one process, the engine may assign work into **execution domains** such as:

- `process.control`
- `db.primary.foreground`
- `db.primary.background`
- `db.analytics.foreground`
- `db.analytics.background`

Each work item may carry both:

- an **execution domain**, which controls placement and resource budgeting, and
- a **durability class**, which controls which persistence path or WAL class it uses.

These two concepts are intentionally related but separate. Moving work between domains or changing domain budgets may affect latency, throughput, and backlog behavior, but must not change correctness semantics such as commit ordering, visibility, durability guarantees, or recovery results. Simple single-DB embeddings can ignore this entirely and rely on the built-in defaults; colocated multi-DB or future shard-aware embeddings opt into explicit domain/resource configuration.

For the colocated case, the current API surface is intentionally split into:

- **placement/deployment shape**, via `ColocatedDeployment` presets such as `single_database`, `two_databases`, `primary_with_analytics`, and `shard_ready`, plus explicit `ColocatedDatabasePlacement` / `ColocatedSubsystemPlacement` overrides when a host needs reserved lanes or custom topology; and
- **durability semantics**, via the `DurabilityClass` attached to each lane binding.

This means a host can move a DB or subsystem between shared and reserved execution domains, or change its domain hierarchy entirely, without silently changing which durability path it uses. The common colocated flow is:

1. Build one shared `ColocatedDeployment` for the process.
2. Open each DB with `Db::builder().colocated_database(&deployment, "...")`.
3. Inspect runtime decisions through `deployment.report()` or `db.execution_placement_report()`.

The `shard_ready` preset is deliberately conservative. Today it places the DB in database-wide lanes such as `process/shards/orders/foreground`, `process/shards/orders/background`, and `process/shards/orders/control`, while reserving the nested namespace `process/shards/orders/shards/<physical-shard>/...` for future shard-local lanes. This makes the placement tree shard-ready without claiming that per-table physical sharding, shard maps, or shard-local storage ownership already exist.

### Engine Responsibilities

The engine:

- Executes flushes, compactions, backups, and offloads when told to by the scheduler.
- Resolves work items into execution domains and accounts CPU / memory / I/O usage against domain-local budgets when domains are configured.
- Exposes accurate per-table stats and a list of pending work.
- Calls the scheduler's `shouldThrottle` before accepting writes.
- Calls the scheduler's `onWorkAvailable` when new work appears.
- Routes catalog, manifest, schema, cursor, and other recovery-critical metadata through the protected control-plane domain and its internal durability lane when that path is enabled.
- Passes table `metadata` through without interpreting it.

### Engine Safety Guardrails

The scheduler is trusted policy code, but the engine enforces hard limits to prevent pathological or deadlocked behavior regardless of scheduler decisions:

- **Forced memtable flush** when available memory for memtables is exhausted. The engine will flush even if the scheduler defers all flush work.
- **Forced L0 compaction** when L0 SSTable count exceeds a hard ceiling. The engine will stall writes and compact even if the scheduler defers.
- **Guaranteed eventual execution** of backup and offload work. If the scheduler defers a work item indefinitely, the engine will force it after a configurable maximum deferral time.
- **Protected control-plane progress** for catalog, manifest, schema, cursor, and other recovery-critical metadata. User-data pressure must not be able to starve internal recovery-critical work forever.

The scheduler controls priority, throttling, and fairness *within* these guardrails. It cannot prevent the engine from maintaining core liveness.

### Scheduler Interface

```typescript
interface Scheduler {
  onWorkAvailable(work: PendingWork[]): ScheduleDecision[]
  shouldThrottle(table: Table, stats: TableStats): ThrottleDecision
}

interface PendingWork {
  id: string
  type: "flush" | "compaction" | "backup" | "offload"
  table: string
  domain?: string           // execution-domain label when domain-aware placement is enabled
  level?: number            // for compaction: source level
  estimatedBytes: number
}

interface ScheduleDecision {
  workId: string
  action: "execute" | "defer"
}

interface ThrottleDecision {
  throttle: boolean
  maxWriteBytesPerSecond?: number   // rate limit, or null for no limit
  stall?: boolean                   // hard block until work completes
}

interface TableStats {
  l0SstableCount: number
  totalBytes: number
  localBytes: number
  s3Bytes: number
  compactionDebt: number
  pendingFlushBytes: number
  immutableMemtableCount: number
  metadata: Record<string, any>   // user-defined, from TableConfig
}
```

The scheduler interface is intentionally **synchronous**. The engine gathers stats asynchronously (via `tableStats()` / `pendingWork()`), then invokes the scheduler with in-memory snapshots of that state. The scheduler makes decisions purely from the data it is given — no I/O, no async, no DB access. This keeps the scheduler simple to implement and test, and avoids the scheduler becoming a source of latency or deadlock in the engine's background work loop.

When execution domains are not configured, `PendingWork.domain` may be omitted or collapse to a built-in default. Domain topology, budget usage, and colocated-DB placement introspection are exposed through runtime APIs and diagnostics; the synchronous scheduler callback only needs enough information to prioritize and throttle pending work without becoming a second control plane.

### Table Metadata for Scheduling

The engine does not interpret table `metadata`. The scheduler reads it to make policy decisions. This lets users express priorities, backpressure policies, and fairness weights without the engine needing to understand them:

```typescript
const events = db.createTable({
  name: "events",
  format: "columnar",
  schema: eventsSchema,
  metadata: {
    priority: "high",
    backpressurePolicy: "stall",
    maxL0Count: 4,
  },
})

const rollups = db.createTable({
  name: "rollups",
  format: "columnar",
  schema: rollupsSchema,
  metadata: {
    priority: "low",
    backpressurePolicy: "throttle",
    maxL0Count: 20,
  },
})
```

### Default Scheduler

The engine ships with a sensible default scheduler that does not read custom metadata. It uses reasonable built-in thresholds:

- Memtable flushes have highest priority (blocked flushes block new writes).
- L0→L1 compaction is prioritized over deeper-level compaction.
- Write throttling begins when L0 SSTable count exceeds a default threshold.
- Write stalling begins at a higher threshold.
- Cross-table work is processed round-robin.

Users who need workload-aware prioritization, custom backpressure policies, or cross-table fairness replace the default with their own implementation.

---

## Serialization

The serialization model differs between table formats:

**Row-oriented tables:** values are opaque bytes. The engine does not interpret them. Serialization is entirely the application's concern — FlatBuffers, Cap'n Proto, Protocol Buffers, bincode, or any other format. During compaction, row values pass through untouched as raw bytes; merge operators and compaction filters receive and produce opaque values.

**Columnar tables:** the engine owns serialization. Values are structured records decomposed into typed columns according to the table's schema. The engine handles field extraction, type-specific compression, default filling for schema evolution, and column pruning. The application provides records as field ID → value maps; the engine encodes and decodes internally.

Since the hot paths (compaction, projection recomputation, scan) are all in Rust, the internal encoding details are a Rust implementation concern. The application-facing API accepts and returns whatever types the calling application prefers, with conversion at the boundary.

---

## Primitive Summary

| Primitive | Purpose |
|---|---|
| **Table** | Column family with configurable format (row/columnar), merge operator, compaction filter, schema, and user-defined metadata |
| **Write / Delete / Merge** | Single-key put, tombstone, or blind delta append. Each receives a sequence number. All fallible. |
| **WriteBatch** | Mutable container for accumulating multi-key writes across tables. |
| **commit** | Single gateway for applying a WriteBatch. Optional read set enables conflict detection. The serialization point for conflict-checked writes. |
| **Snapshot** | Read-consistent point-in-time view at a specific sequence number. |
| **currentSequence / currentDurableSequence** | Visible-prefix and durable-prefix watermarks for this DB instance. |
| **Sequence Number** | Monotonically increasing commit order. Drives MVCC, merge ordering, change feeds, conflict detection. |
| **Read / Scan / ReadAt / ScanAt** | Point lookup (bloom-filter assisted) or range scan, optionally at a specific sequence number. Column pruning for columnar tables. All fallible. |
| **scanPrefix** | Range scan by key prefix. Used by timers, indexes, bucketed windows, change feed partitions. |
| **scanSince / scanDurableSince** | Iterate visible or durable committed entries after an opaque `LogCursor` position. Gap-free within retained history. Cursor encodes `(sequence, op_index)` for safe resumption within multi-entry batches. |
| **subscribe / subscribeDurable** | Coalescing visible-prefix or durable-prefix watermark notifications that drive event loops without polling. Paired with the corresponding scan API for data retrieval. |
| **Flush** | Durability checkpoint. Tiered mode: forces memtable rotation + fsync. S3-primary mode: ships buffered data to S3. |
| **Scheduler** | User-replaceable scheduling policy (injected at open time). Engine exposes stats and pending work; scheduler decides prioritization, throttling, and backpressure within engine safety guardrails. |

---

## Non-Goals and Boundaries

The engine provides primitives, not business semantics. The following are explicitly out of scope or deferred:

- **Built-in derived state / projection maintenance.** The engine does not maintain projections, indexes, or aggregates. These are handled by the projection library (Part 3) built from `WriteBatch` (colocated sync) and `scanSince` (async).
- **Built-in transactions.** OCC transactions are a composition pattern, not an engine primitive. The engine provides `commit` with an optional read set; the transaction wrapper lives in a separate library.
- **Query language.** The engine is a key-value store with scans. Query planning, joins, and SQL are out of scope.
- **Universal synchronous consistency.** Not every projection is synchronously maintained. The architecture explicitly distinguishes fast colocated paths from slower coordinated paths.
- **Linux-specific dependencies.** io_uring is an optional backend optimization, not a requirement. The engine runs on any platform with Tokio support.

---

## Future Extension: Physical Sharding

Physical per-table sharding — where a table gets independent memtables, commit log lanes, SSTables, and compaction schedules per shard — is a future optimization for CPU-bound write throughput on many cores. It is not implemented, but the current design is future-proofed in four ways:

1. **Structured sequence identifier.** Commit log records and MVCC key suffixes use a `CommitId` struct rather than a bare `u64`. Currently this contains only a sequence number, but the encoding reserves space for a shard identifier. This avoids a storage format migration when sharding is added.

2. **Shard-aware storage layout.** The on-disk and S3 directory structure includes a shard index (defaulting to `0000`) in the path:
   ```
   db/tables/{table}/shards/0000/commitlog/
   db/tables/{table}/shards/0000/sst/
   ```
   Adding physical shards creates `0001/`, `0002/`, etc. alongside the existing `0000/` directory. No migration of existing data.

3. **WriteBatch shard grouping.** The commit path resolves each entry's target shard (always 0 today) and groups entries by `(table, shard)` before acquiring the mutex. When physical sharding is added, the commit path can validate that no batch spans multiple shards of the same sharded table — a one-line check on an already-computed grouping.

4. **Execution-domain hierarchy already separates placement from correctness.** A future physical shard can slot into the existing shard-ready tree as another workload unit (for example `process/shards/orders/shards/0003/foreground`) without redefining commit semantics or resource-isolation concepts. Domains provide the placement/budgeting foundation for shard-local foreground, background, and control-plane work, but they are not themselves physical sharding and do not define logical shard ownership.

When physical sharding is implemented, it will be opt-in per table via `TableConfig`. Unsharded tables continue to work exactly as today. Cross-shard `WriteBatch` operations on a sharded table will be rejected at commit time, enforcing the single-writer-per-entity discipline that is currently an application convention.

**Resharding without data rewriting:** to avoid the Kafka/Temporal problem where changing the shard count requires rehashing every key and redistributing all data, the sharding model uses a level of indirection. Keys hash to a large fixed number of **virtual partitions** (e.g., 2^16), and a metadata mapping assigns virtual partitions to physical shards. Resharding changes the mapping — reassigning virtual partitions from one physical shard to another — not the hash function. The SSTables whose key ranges fall within the reassigned virtual partitions are moved to the new physical shard's directory. No keys are rehashed, no data is rewritten. The hash function (`key → virtual partition`) is fixed at table creation and never changes; only the `virtual partition → physical shard` mapping is mutable.

---

## Future Extension: Zero-Downtime Upgrades

### Motivation

The DB is an embedded library inside an application binary. Upgrading the application means restarting the process, which means closing and reopening the DB. During the transition, incoming requests must not fail. For a single-node system without replicas, there is no second node to absorb traffic during the restart — the upgrade must be coordinated between the old and new process on the same host.

The goal is a library on top of the DB that provides zero-downtime upgrades with no dropped requests. Requests arriving during the transition experience higher latency but never receive errors.

### Approach

The library wraps all DB access through a **gateway** that can buffer requests during the handoff. The old process launches the new process as a child, they coordinate via a Unix domain socket (`handoff.sock`) in the DB directory, and the listening socket file descriptor is transferred between processes using `SCM_RIGHTS` so the port is never unbound.

The DB engine's contribution is `prepareForShutdown()` (flush + clean state) and fast `open()` after a clean shutdown. The handoff protocol, buffering, and socket transfer are library concerns, not engine concerns. The library is network-agnostic — it transfers opaque file descriptors and serialized request payloads without understanding the application's protocol.

### Handoff Sequence

```
Old Process                                    New Process
    |
    owns listening socket (port 8080)
    accepting and processing requests normally
    |
    receives upgrade signal
    |
    launches new binary as child process
    |--------------------------------------->  starts
    |                                          |
    |                                          initializes runtime, config,
    |                                          connections (everything except DB)
    |                                          |
    |                                          connects to handoff.sock
    |<-- "ready for handoff" ----------------  |
    |                                          |
    passes listening socket FD (SCM_RIGHTS)
    |-- fd --------------------------------->  receives listening socket FD
    |                                          starts calling accept()
    stops calling accept()                     new connections go to new process,
    (in-flight requests on already-accepted    queued in its runtime until DB is open
    connections continue processing)
    |                                          |
    gateway enters buffering mode:             |
    new requests on existing connections       |
    queue in memory                            |
    |                                          |
    waits for in-flight DB work to drain       |
    |                                          |
    db.prepareForShutdown()                    |
    db.close()                                 |
    |                                          |
    |-- "DB released" ---------------------->  |
    |                                          |
    |                                          db.open() (fast, pre-flushed)
    |                                          |
    |                                          starts processing its own
    |                                          queued connections normally
    |                                          |
    |                                          |-- "send me the buffer" -->
    |                                          |
    |<-- pulls buffered requests ------------- |
    |                                          |
    sends buffered requests + connection       processes them, responds
    FDs (SCM_RIGHTS) to new process            directly to original callers
    |                                          on the transferred connections
    |                                          |
    |<-- "buffer drained" -------------------  |
    |                                          |
    exits                                      fully live
```

### Key Properties

**No port gap.** The listening socket is transferred via `SCM_RIGHTS` — the kernel socket is never closed. During the brief gap between the old process's last `accept()` and the new process's first `accept()`, incoming connections queue in the kernel's TCP backlog. No client sees a connection refused error.

**No response relay.** Buffered clients' connection file descriptors are transferred to the new process alongside the serialized request payloads. The new process responds directly on the original connections. The old process does not need to stay alive as a relay.

**Network-agnostic.** The library transfers opaque file descriptors and serialized byte payloads. It does not interpret HTTP, gRPC, or any other protocol. The application provides serialization/deserialization for its request type.

**DB-level requirements.** Only two things from the engine: `prepareForShutdown()` (flush all committed data to durable storage, minimize WAL tail) and fast `open()` (after a clean shutdown, opening the DB is just reading the manifest and setting up the memtable — tens of milliseconds).

**Latency budget.** The total added latency for buffered requests is: drain in-flight work + `prepareForShutdown()` + `db.close()` + `db.open()` + processing the buffered request. With a pre-flushed DB, `close()` + `open()` should be tens of milliseconds. The dominant cost is draining in-flight work, which depends on the application's request latency profile.

---

## Deployment

The DB is an embedded library, so deployment means deploying the application that embeds it. The engine's responsibilities are limited to clean shutdown and fast startup. Everything else — traffic routing, process lifecycle, upgrade orchestration — is the application's or the platform's concern.

The engine provides two primitives relevant to deployment:

- **`prepareForShutdown()`** — flushes all committed data to durable storage (memtable → SSTables, commit log fsync or S3 upload depending on storage mode), finishes any near-complete background work, and returns. After this call, `close()` is fast and `open()` on the next startup requires minimal or no WAL replay.
- **Fast `open()` after clean shutdown** — reads the manifest, sets up the memtable, replays any WAL tail. After `prepareForShutdown()`, this is tens of milliseconds.

Applications should handle `SIGTERM` by stopping new work intake, draining in-flight DB operations, calling `prepareForShutdown()`, and exiting. The drain + flush should complete within the platform's graceful shutdown window (typically 10–30 seconds).

### Render (PaaS with Persistent Disk)

Render is a commodity PaaS that supports persistent disks attached to a single service instance. Persistent disks prevent zero-downtime deploys — Render stops the old instance before starting the new one to prevent two processes from writing to the same disk simultaneously. Deploys incur a few seconds of unavailability.

**Deploy sequence:**

```
1. New code is pushed (or deploy triggered via Render API / deploy hook)
2. Render builds the new image
3. Render sends SIGTERM to old instance
4. Old instance: stop accepting → drain in-flight → prepareForShutdown() → exit
5. Render tears down old container
6. Render starts new container with same persistent disk
7. New instance: open DB (fast, pre-flushed) → start accepting
8. Render health check passes → routes traffic
```

The DB-specific downtime (steps 4–7) is tens of milliseconds. The total downtime is dominated by Render's container lifecycle — typically a few seconds.

**Self-triggered deploys:** the running instance can trigger its own redeployment programmatically by sending an HTTP request to the service's deploy hook URL or calling the Render REST API's trigger-deploy endpoint. This enables patterns like: instance polls for a new version, prepares for shutdown, then triggers the deploy. The upgrade is fully automated with no human intervention.

**Configuration:**

```yaml
# render.yaml
services:
  - type: web
    name: my-app
    runtime: docker
    disk:
      name: data
      mountPath: /data
      sizeGB: 10
    healthCheckPath: /health
    maxShutdownDelaySeconds: 30  # time allowed for graceful shutdown
```

The `maxShutdownDelaySeconds` setting (default 30, max 300) controls how long Render waits after SIGTERM before sending SIGKILL. The application's drain + `prepareForShutdown()` must complete within this window.

### Other Platforms

**Fly.io Machines:** similar model — single instance with attached volume. Use `fly deploy` for rolling restarts. The Machine is stopped and restarted; the volume persists. Fly provides a configurable `kill_timeout` for graceful shutdown.

**Bare metal / VM:** the application manages its own process lifecycle. Use the zero-downtime upgrade library (see Future Extension above) for seamless handoff, or accept brief downtime with a simple stop → start.

**Kubernetes:** for single-instance stateful workloads, use a StatefulSet with `replicas: 1`. K8s sends SIGTERM, waits for `terminationGracePeriodSeconds`, then starts the new pod. The persistent volume is remounted. Same stop → start model as Render. For zero-downtime, use the handoff library with a custom controller or sidecar that coordinates the two-pod overlap.

---

## I/O Abstraction Layer

The engine is parameterized over all sources of I/O, time, and randomness via traits. Production uses real implementations; simulation testing swaps in deterministic fakes. This boundary also controls how blocking I/O interacts with the async runtime.

### Traits

```rust
// Filesystem — async trait. Production wraps std::fs in spawn_blocking internally.
// Simulation returns immediately (no real I/O).
#[async_trait]
trait FileSystem: Send + Sync {
    async fn open(&self, path: &str, opts: OpenOptions) -> Result<FileHandle>;
    async fn write_at(&self, handle: &FileHandle, offset: u64, data: &[u8]) -> Result<()>;
    async fn read_at(&self, handle: &FileHandle, offset: u64, len: usize) -> Result<Vec<u8>>;
    async fn sync(&self, handle: &FileHandle) -> Result<()>;
    async fn rename(&self, from: &str, to: &str) -> Result<()>;
    async fn delete(&self, path: &str) -> Result<()>;
    async fn list(&self, dir: &str) -> Result<Vec<String>>;
}

// Object store — async trait. Real network I/O (or turmoil's simulated network).
#[async_trait]
trait ObjectStore: Send + Sync {
    async fn put(&self, key: &str, data: &[u8]) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>>; // [start, end) — end is exclusive
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn copy(&self, from: &str, to: &str) -> Result<()>;
}

// Clock — virtual time for simulation, real time for production.
trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    async fn sleep(&self, duration: Duration);
}

// Random number generation — seeded in simulation, system entropy in production.
trait Rng: Send + Sync {
    fn next_u64(&self) -> u64;
    fn uuid(&self) -> String;
}
```

### Production Implementations

The `FileSystem` production implementation wraps `std::fs` operations in `spawn_blocking` to avoid blocking the tokio runtime. `tokio::fs` is not used directly — it does the same thing internally but doesn't allow swapping implementations for simulation.

`spawn_blocking` submits work to tokio's reusable blocking thread pool (not thread-per-call). The group commit leader's fsync is a `spawn_blocking` call. For compaction, the entire job should be submitted as a single `spawn_blocking` call (not per individual read/write) to minimize overhead.

The `ObjectStore` production implementation uses the S3 SDK with real network I/O. The `Clock` uses `std::time::SystemTime`. The `Rng` uses `thread_rng()`.

### Simulation Implementations

`FileSystem` uses turmoil's simulated filesystem shim. Supports crash simulation (unsynced writes are lost), torn writes, disk full injection, and configurable sync probability.

`ObjectStore` runs as a turmoil network host — an in-memory S3 emulator with a network address. Turmoil's network simulation provides partition, latency, partial response, and timeout injection naturally.

`Clock` uses turmoil's virtual clock. Time only advances when the simulation steps it. `Rng` uses a seeded PRNG for full reproducibility.

### All Threading Through Tokio

All concurrency must go through the tokio runtime that turmoil controls. `std::thread::spawn` and uncontrolled `spawn_blocking` escape simulation control and break determinism. The concurrent memtable (`crossbeam-skiplist`) works correctly under single-threaded simulation — no correctness issue, just unnecessary synchronization overhead in that mode.

---

## Deterministic Simulation Testing

The engine uses deterministic simulation testing (DST) in the style of FoundationDB, using turmoil plus explicit injected effect seams. The goal is to exercise a large number of randomized scenarios — workloads, fault combinations, scheduling orders — with full reproducibility from a seed.

### Sources of Non-Determinism Controlled

| Source | Mechanism |
|---|---|
| **Time** | Virtual clock via `Clock` trait. Tests control exactly when time advances. |
| **Randomness** | Seeded PRNG via `Rng` trait and simulation helpers. No ambient libc entropy interception. |
| **Filesystem** | Simulated via `FileSystem` trait + turmoil's fs shim. Crash/torn-write/disk-full injection. |
| **S3 / network** | S3 emulator as turmoil host. Network partitions, latency, partial responses via turmoil. |
| **Task scheduling** | Single-threaded tokio runtime. Turmoil controls task interleaving. |
| **Background work ordering** | `Scheduler` trait. Simulation injects a scheduler driven by the seeded PRNG. |
| **System entropy** | Avoided by design on deterministic paths: engine code must use injected `Clock`/`Rng` traits rather than ambient system APIs. |

The harness intentionally does not override libc clocks or entropy. Determinism depends on explicit trait injection, simulation-aware adapters, and avoiding third-party code that escapes those seams on deterministic paths.

### S3 Emulator as Turmoil Host

The S3 emulator runs as a simulated network host, not an in-memory fake with direct function calls. This means turmoil's network simulation naturally provides failure modes:

```rust
let mut sim = turmoil::Builder::new().build();

sim.host("s3", || async {
    S3Emulator::new().listen(9000).await
});

sim.host("db", || async {
    let s3 = S3Client::connect("s3:9000").await;
    let db = DB::open(config, s3).await;
    // ...
});

// Fault injection via turmoil's network controls
sim.partition("db", "s3");                             // full partition
sim.delay("db", "s3", Duration::from_millis(500));     // high latency
sim.repair("db", "s3");                                // restore
```

This covers: connection timeouts, partial responses, PUT-succeeded-but-response-lost (the most dangerous case), slow responses triggering client-side timeouts, and LIST not reflecting recent PUTs.

### Simulation Scheduler

The user-replaceable `Scheduler` doubles as a simulation tool. A simulation scheduler makes random decisions from the seeded PRNG, exercising different orderings of flush vs compaction vs backup vs offload:

```rust
struct SimulationScheduler { rng: SeededRng }

impl Scheduler for SimulationScheduler {
    fn on_work_available(&mut self, work: Vec<PendingWork>) -> Vec<ScheduleDecision> {
        // Randomly execute or defer each work item
        work.into_iter().map(|w| ScheduleDecision {
            work_id: w.id,
            action: if self.rng.next_bool(0.7) { Execute } else { Defer },
        }).collect()
    }

    fn should_throttle(&mut self, _table: &Table, _stats: &TableStats) -> ThrottleDecision {
        // Randomly throttle to exercise backpressure paths
        ThrottleDecision {
            throttle: self.rng.next_bool(0.1),
            stall: self.rng.next_bool(0.01),
            max_write_bytes_per_second: None,
        }
    }
}
```

Each seed produces a different scheduling order — some runs compact aggressively, others let L0 build up, others stall writes at inconvenient moments. All deterministically.

### Fault Injection Categories

**Crash faults:**
- Process crash at arbitrary points during write path (after commit log write but before memtable insert; after memtable insert but before visibility publication)
- Crash during compaction (after writing new SSTable but before updating manifest; after updating manifest but before deleting old SSTables)
- Crash during S3 backup (after upload but before manifest update)
- Crash during flush in s3-primary mode

**Filesystem faults:**
- Torn writes (partial data written before crash)
- fsync not actually durable (simulates disk write cache)
- Disk full during SSTable write or commit log append
- File corruption (bit flips in SSTable or manifest)

**S3 / network faults:**
- PUT timeout / failure
- GET returning stale data (eventual consistency)
- Partial upload completion
- PUT succeeded but response lost (client sees failure, server has the data)
- LIST not reflecting recently uploaded objects
- Rate limiting / throttling

**Scheduling faults:**
- Compaction running simultaneously with writes and reads
- Memtable flush happening during a scan
- Snapshot taken mid-compaction
- Arbitrary reordering of background tasks

### Test Generation

Tests are property-based: define invariants, generate random workloads + fault schedules from a seed, check invariants after every step.

```rust
#[test]
fn simulation_test() {
    for seed in 0..10_000 {
        let rng = SeededRng::new(seed);
        let mut sim = Simulation::new(rng);

        let workload = generate_workload(&mut sim.rng, WorkloadConfig {
            num_tables: 1..4,
            num_writers: 1..8,
            num_readers: 1..4,
            operations: 100..10_000,
            operation_mix: OperationMix {
                write: 50, delete: 10, merge: 20, scan: 10, commit_with_readset: 10,
            },
            fault_schedule: FaultSchedule {
                crash_probability_per_step: 0.001,
                s3_failure_probability: 0.01,
                disk_full_probability: 0.001,
            },
        });

        let result = sim.run(workload);

        if let Err(e) = result {
            panic!("Invariant violation at seed {seed}: {e}");
            // Rerunning with this seed reproduces exactly
        }
    }
}
```

### Invariants Checked

**Durability:**
- After a successful `flush()` return, all prior writes survive any subsequent crash and recovery.
- After recovery, the DB state is consistent with some prefix of the committed write sequence.
- In tiered mode (group commit), after the group fsync completes, the write survives crash. The write becomes visible only after memtable insertion and visibility publication, but durability is established at group fsync. In deferred durability mode, the write survives crash only after the next background fsync or explicit `flush()`.

**Atomicity:**
- A `WriteBatch` is either fully visible or not visible at all — never partially applied.
- After crash and recovery, no partial batches are visible.

**MVCC consistency:**
- A snapshot reads a consistent state: all writes with sequence ≤ snapshot.sequence, no writes with sequence > snapshot.sequence.
- `readAt(key, seq)` returns the same value regardless of concurrent writes with higher sequence numbers.
- A scan within a snapshot never sees a mix of pre-commit and post-commit states from any batch.

**Ordering:**
- Merge operands for a given key are always evaluated in commit sequence order.
- Two reads of the same key at the same sequence number always return the same value.

**Conflict detection:**
- If `db.commit(batch, { readSet })` succeeds, no key in the read set was modified between the read sequence and the commit sequence.
- If a key was concurrently modified, `db.commit()` returns a `ConflictError`.

**Compaction correctness:**
- Logical content of the database is unchanged after compaction — same keys, same values at every sequence number within the GC horizon.
- Compaction never drops data visible to an active snapshot.
- Merge operator results after compaction match what a full read-time merge would produce.

**Recovery:**
- After crash and recovery, the manifest is valid and references only existing files.
- S3 disaster recovery produces a DB with identical logical content to the pre-crash state (minus the data loss window).

### Shadow State Checker

A shadow oracle — a simple in-memory model of what the DB should contain — validates every read and every post-recovery state:

```rust
struct ShadowOracle {
    // Tracks every committed write/merge/delete with its sequence number
    state: BTreeMap<(TableName, Key), Vec<(SequenceNumber, WriteOp)>>,
    // Registered merge operators per table, for resolving merge chains
    merge_operators: HashMap<TableName, Box<dyn MergeOperator>>,
}

enum WriteOp {
    Put(Value),
    Delete,
    Merge(Value),  // unresolved merge operand
}

impl ShadowOracle {
    fn expected_value_at(&self, table: &str, key: &Key, seq: SequenceNumber) -> Option<Value> {
        // Filter to entries with sequence <= seq
        // Resolve merge chains in sequence order using the registered merge operator
        // This mirrors exactly what the engine should produce
    }

    fn check_read(&self, table: &str, key: &Key, seq: SequenceNumber, actual: Option<&Value>) {
        let expected = self.expected_value_at(table, key, seq);
        assert_eq!(actual, expected.as_ref(), "Read mismatch at seq {seq}");
    }

    fn check_after_recovery(&self, db: &DB, last_durable_seq: SequenceNumber) {
        // Every write with seq <= last_durable_seq must be present
        // Writes between last_durable_seq and crash may be present or absent,
        // but must be prefix-consistent: if batch N is present, batch N-1 must be too
    }
}
```

The oracle must model merge operator semantics — it tracks unresolved merge operands, applies them in sequence order using the same merge operator logic as the engine, and validates that reads against merge chains produce the correct resolved value. Without this, merge-heavy workloads would not be properly verified.

### Reproducibility

Given the same seed, the simulation produces exactly the same execution trace — same workload, same faults, same task scheduling, same I/O completion order, same time progression. When a test fails, the seed reproduces it exactly.

A **meta-test** in CI reruns the same seed and compares TRACE-level logs between runs. If logs differ, there is an uncontrolled source of non-determinism.


### Reusable Simulation Framework for Terracedb Applications

The deterministic simulation substrate should be available as a supported crate for Terracedb-based applications, not only as internal engine test code. The goal is that a user can stand up `DB`, the projection library, and the workflow library inside the same seeded, crashable, fault-injectable harness and test application logic with the same replay guarantees the engine uses.

`turmoil-determinism` remains the **lowest layer only**: seeded turmoil builder configuration plus small time/random helpers. It should stay generic and minimal. The broader Terracedb testing framework belongs in a separate crate layered above it (for example `terracedb-simulation`) rather than expanding `turmoil-determinism` until it accumulates Terracedb-specific DB/process/oracle concepts.

That higher-level crate should expose stable **application-facing** building blocks:

- seeded simulation runner + context,
- simulated Terracedb dependencies (`FileSystem`, `ObjectStore`, `Clock`, `Rng`),
- trace capture, seed replay, scheduled fault injection, and crash/restart helpers,
- extension points for application-defined workloads, invariants, and shadow oracles, and
- convenience adapters for opening/restarting a DB and optional projection/workflow runtimes inside one simulated process.

Engine-internal workload generators and oracles (for example DB-specific mutation DSLs or the core engine shadow oracle) may live on top of that crate, but they should not define its public surface. Application tests should not need to import engine-internal scenario types just to drive a workflow or projection under simulation.

```typescript
await TerracedbSimulation.run({ seed }, async (sim) => {
  const db = await sim.openDb(dbConfig)
  const projections = await ProjectionRuntime.open(db)
  const workflows = await WorkflowRuntime.open(db, defs)

  await app.drive(sim, { db, projections, workflows })
  await sim.crashAndRestart()
  await app.assertInvariants(sim, { db, projections, workflows })
})
```

The same framework should also support simulated external hosts (for callback endpoints, message sinks, or other side-effect targets) so workflow/outbox tests exercise real retry, timeout, partition, and duplicate-delivery behavior under turmoil rather than relying on ad hoc mocks. `terracedb-projections` and the workflow library should use this shared crate for their own deterministic tests so Terracedb exposes one consistent testing story from core engine through end-user applications.

---

# Part 2: Composition Patterns

Building blocks used by the projection library, the workflow library, the embedded virtual filesystem library, and application code directly. These patterns are implemented in user space on top of core DB primitives. The engine does not know about transactions, views, windows, timers, events, or filesystems — these are conventions built using tables, writes, scans, batches, snapshots, and sequence numbers.

These patterns fall into two categories based on data locality: **colocated** patterns update source and derived data in the same `WriteBatch` (fast, immediate consistency), while **cross-entity** patterns propagate changes asynchronously via `subscribe` + `scanSince` (flexible, eventually consistent with notification-driven read-after-write). Where relevant, each pattern notes which category it falls into.

**Note on pseudocode:** examples in Parts 2–6 use `await` on DB operations to reflect the async API, but omit `Result` unwrapping for brevity. All engine operations return `Promise<Result<...>>` types; the `Result` unwrapping is implicit. Sequence numbers used as watermarks are **visibility** markers unless the library explicitly consumes the durable stream (`scanDurableSince` / `subscribeDurable`). Authoritative consumers in Parts 3, 4, and the durable surfaces described in Parts 5 and 6 use the durable stream; best-effort maintenance may use the visible stream.

---

## Derived State Patterns

The engine provides primitives for maintaining derived state (indexes, projections, aggregates), but the engine itself does not maintain any derived state. Two composition patterns emerge naturally, each with different consistency properties.

### Colocated Synchronous Derived State

When source data and derived data can be updated together, both go in the same `WriteBatch`. The batch is atomically visible — no coordination required.

```typescript
// Source write + derived index update in one batch
async function onEvent(db: DB, event: Event) {
  const batch = db.writeBatch()
  batch.put(eventsTable, eventKey(event.id), serialize(event))
  batch.put(eventsByTypeIndex, indexKey(event.type, event.id), event.id)
  batch.merge(countersTable, counterKey(event.type), encodeInt64(1))
  await db.commit(batch)
}
```

**Read-after-write:** immediate. A read after `commit` returns sees both the source write and all derived updates. No watermarks, no waiting.

**When to use:** the derived data can be computed from the source data at write time without reading other entities' state. Examples: per-entity counters, per-entity indexes, per-entity windowed aggregates, any local rollup.

### Cross-Entity Asynchronous Derived State

When derived data aggregates across multiple entities or cannot be computed at write time, it may be maintained asynchronously from a change stream. **This simple pattern is only for best-effort / idempotent / disposable maintenance.** It is not the general authoritative pattern for read-after-write guarantees, crash-stable progress, or side-effecting consumers. For those cases, use the stricter projection/workflow runtimes in Parts 3 and 4.

```typescript
class AsyncDerivedUpdater {
  private cursor: LogCursor

  async run(db: DB, sourceTable: Table, derivedTable: Table, deriveFn: DeriveFn) {
    const notifications = db.subscribe(sourceTable)

    async function drainAllAvailable(): Promise<void> {
      while (true) {
        let madeProgress = false
        const entries = await db.scanSince(sourceTable, this.cursor, { limit: BATCH_SIZE })
        for await (const entry of entries) {
          const viewKey = deriveFn.deriveKey(entry.key, entry.value)
          await derivedTable.merge(viewKey, deriveFn.deriveDelta(entry.key, entry.value))
          this.cursor = entry.cursor
          madeProgress = true
        }
        if (!madeProgress) break
        await db.table("_cursors").write(this.cursorKey, encodeCursor(this.cursor))
      }
    }

    await drainAllAvailable.call(this)           // restart-safe initial catch-up
    for await (const _seq of notifications) {
      await drainAllAvailable.call(this)         // drain until empty on each wake
    }
  }
}
```

**Crash semantics:** this updater is appropriate only when replaying an entry is harmless (idempotent merge, disposable cache rebuild, or overwrite-by-deterministic-key). It must not be presented as a general authoritative async-maintenance recipe.

`AsyncDerivedUpdater` does **not** provide authoritative watermark semantics. The `WatermarkTracker` below is used by the stricter projection/workflow runtimes later in this document, not by this best-effort helper.

`WatermarkTracker` is a lightweight coordination primitive used throughout the projection and workflow libraries:

```typescript
class WatermarkTracker {
  private current: SequenceNumber = 0
  private waiters: Array<{ target: SequenceNumber, resolve: () => void }> = []

  advance(seq: SequenceNumber) {
    this.current = seq
    // Drain all waiters whose target has been reached
    const [ready, remaining] = partition(this.waiters, w => w.target <= seq)
    this.waiters = remaining
    for (const w of ready) w.resolve()
  }

  waitFor(seq: SequenceNumber): Promise<void> {
    if (seq <= this.current) return Promise.resolve()
    return new Promise(resolve => {
      this.waiters.push({ target: seq, resolve })
    })
  }
}
```

No polling or sleep intervals are required. The engine pushes notifications via `subscribe`, and the stricter projection/workflow runtimes later in the document use a `WatermarkTracker` to resolve waiting readers once authoritative progress commits.

**When to use:** idempotent caches, search indexes, leaderboards, helper tables, or other non-authoritative cross-entity maintenance where replay is harmless.

### Choosing a Pattern

| Criterion | Colocated Sync | Cross-Entity Async |
|---|---|---|
| Derived computable at write time? | Yes | No, or aggregates across entities |
| Read-after-write model | Immediate | Best-effort only by itself. Use the stricter projection/workflow runtimes in Parts 3/4 for authoritative `waitFor`-style semantics. |
| Coordination cost | None | `subscribe` + `scanSince` + cursor persistence (watermark tracking belongs to the stricter runtimes, not this helper) |
| Failure mode | Batch failure = nothing visible | Derived state may lag; rebuild from checkpoint on SnapshotTooOld |
| Good for | Per-entity derived state | Global/cross-entity aggregations, caches, idempotent maintenance |

---

## OCC Transactions

Optimistic concurrency control transactions provide snapshot isolation: reads see a consistent view, writes are buffered, and conflicts are detected at commit time. This is a library built on the core primitives — the engine itself has no concept of "transaction."

**Isolation level:** snapshot isolation, not full serializability. Range scans are not protected from phantom inserts — a concurrent write may insert a key within a previously scanned range without causing a conflict. Only point reads in the read set are checked. Users who need serializable range transactions would need to implement predicate/range conflict tracking at the application level.

```typescript
// TOMBSTONE is a wrapper-private sentinel value indicating a pending delete.
// It is not part of the engine API.
const TOMBSTONE = Symbol("TOMBSTONE")

class Transaction {
  private db: DB
  private snap: Snapshot
  private batch: WriteBatch
  private readSet: ReadSet
  // Internal map for read-your-own-writes (WriteBatch is write-only)
  private localWrites: Map<string, Value | typeof TOMBSTONE> = new Map()

  static async begin(db: DB): Promise<Transaction> {
    const txn = new Transaction()
    txn.db = db
    txn.snap = await db.snapshot()
    txn.batch = db.writeBatch()
    txn.readSet = db.readSet()  // local accumulator, no I/O
    return txn
  }

  async read(table: Table, key: Key): Promise<Value | null> {
    const localKey = `${table.name}:${key}`
    if (this.localWrites.has(localKey)) {
      const local = this.localWrites.get(localKey)
      return local === TOMBSTONE ? null : local
    }
    const value = await this.snap.read(table, key)
    this.readSet.add(table, key, this.snap.sequence)
    return value
  }

  write(table: Table, key: Key, value: Value) {
    this.batch.put(table, key, value)
    this.localWrites.set(`${table.name}:${key}`, value)
  }

  delete(table: Table, key: Key) {
    this.batch.delete(table, key)
    this.localWrites.set(`${table.name}:${key}`, TOMBSTONE)
  }

  async commit(opts: { flush?: boolean } = {}): Promise<SequenceNumber> {
    try {
      const seq = await this.db.commit(this.batch, { readSet: this.readSet })
      // Visibility and durability are orthogonal in the engine.
      // By default this helper flushes after commit as a conservative choice,
      // but high-throughput runtimes (for example workflow trigger admission)
      // may pass { flush: false } and rely on durable-fenced consumers instead.
      if (opts.flush !== false) {
        await this.db.flush()
      }
      return seq
    } finally {
      this.snap.release()
    }
  }

  abort() {
    this.snap.release()
  }
}
```

### Write-Write Conflicts

Two concurrent transactions that both write to the same key without reading it will not conflict through the read set. If write-write conflict detection is needed, the caller should include a read of the key before writing it — the read adds the key to the read set and ensures a conflict is detected if another transaction modifies it concurrently.

For high-throughput runtimes that already separate visibility from durability — especially workflow/event admission — callers typically use `tx.commit({ flush: false })`. Atomicity still comes from one OCC commit; authoritative execution remains correct because it consumes only from the durable prefix.

### Long-Running Transactions

A transaction holds a snapshot, which pins the MVCC GC horizon. Long-running transactions prevent old versions from being reclaimed. If the GC horizon advances past a transaction's snapshot sequence (because the snapshot was released or the retention limit was exceeded), subsequent reads through that snapshot fail with `SnapshotTooOld`.

---

## Change Feed (Application-Level Pattern)

> **Note:** The engine's change-capture primitives are the primary mechanism used by higher-level libraries: best-effort in-process maintenance typically uses `db.subscribe()` + `db.scanSince()`, while authoritative consumers use `db.subscribeDurable()` + `db.scanDurableSince()`. The pattern below is a **special-purpose application-level alternative** for cases where the application encodes its own sequence numbers into user keys — for example, event tables with application-managed ordering, or transition/crossing tables where the key structure must support both `scanPrefix`-based consumers and cursor-based iteration.

Any append-only, sequence-keyed table can serve as a change feed. In this pattern, **the sequence number is part of the user key** — it is embedded by the application when writing, not obtained from internal MVCC metadata.

```typescript
class ChangeConsumer {
  private cursor: SequenceNumber

  static async create(db: DB, cursorTable: Table, consumerId: string): Promise<ChangeConsumer> {
    const consumer = new ChangeConsumer()
    consumer.db = db
    consumer.cursorTable = cursorTable
    consumer.consumerId = consumerId
    const saved = await cursorTable.read(consumerId)
    consumer.cursor = saved ? decodeCursor(saved) : 0
    return consumer
  }

  async poll(feedTable: Table): Promise<Entry[]> {
    const results: Entry[] = []
    for await (const [key, value] of feedTable.scan(encodeSeq(this.cursor), maxKey)) {
      const seq = extractSequenceFromUserKey(key)
      if (seq <= this.cursor) continue
      results.push(deserialize(value))
      this.cursor = seq
    }

    if (results.length > 0) {
      await this.cursorTable.write(this.consumerId, encodeCursor(this.cursor))
    }
    return results
  }
}
```

---

## Durable Timers

A timer requires two user-created tables: a **schedule table** sorted by fire time for efficient firing, and a **lookup table** keyed by timer ID for efficient cancellation.

This section describes the **authoritative** timer pattern. That means timer discovery is fenced to the **durable prefix**, not just the visible prefix. In deferred-durability modes, a visible timer row is not enough to justify firing; the timer loop must only observe rows that are already within `currentDurableSequence()`.

```typescript
// Schedule table key: (fireAt, timerId) — enables range scan for due timers
// Lookup table key: timerId → fireAt — enables cancellation without knowing fire time

function timerLoop(db: DB, scheduleTable: Table, lookupTable: Table,
                   onFire: (timerId: string, payload: bytes) => WriteBatch | null) {
  setInterval(async () => {
    const durableSeq = db.currentDurableSequence()

    // Bounded durable-fenced range scan — only reads due rows that are already durable
    for await (const [key, value] of scheduleTable.scanAt(
      scheduleKey(0, ""), scheduleKey(now(), "\xff"), durableSeq
    )) {
      const { timerId, payload } = deserialize(value)
      const batch = onFire(timerId, payload) ?? db.writeBatch()
      batch.delete(scheduleTable, key)
      batch.delete(lookupTable, timerId)
      await db.commit(batch)
    }
  }, TICK_INTERVAL)
}
```

The `WriteBatch` guarantees that timer deletion and any database-level state updates in `onFire` are atomic **for a single execution attempt**. But timer delivery itself is **at-least-once**: duplicate firings are possible from crash/retry, overlapping scans, or multiple workers observing the same due timer. Therefore `onFire` must be written as a state-guarded / idempotent transition where duplicate firings are benign no-ops.

**External side effects** (HTTP calls, emails, queue publishes) triggered by `onFire` are also **at-least-once** unless the user implements idempotency, e.g., via the transactional outbox pattern below.

Scheduling and cancelling:

```typescript
function scheduleTimer(batch: WriteBatch, scheduleTable: Table, lookupTable: Table,
                       timerId: string, fireAt: Timestamp, payload: bytes) {
  batch.put(scheduleTable, scheduleKey(fireAt, timerId), serialize({ timerId, payload }))
  batch.put(lookupTable, timerId, serialize({ fireAt }))
}

async function cancelTimer(db: DB, batch: WriteBatch, scheduleTable: Table, lookupTable: Table,
                           timerId: string) {
  const entry = await lookupTable.read(timerId)
  if (entry) {
    const { fireAt } = deserialize(entry)
    batch.delete(scheduleTable, scheduleKey(fireAt, timerId))
    batch.delete(lookupTable, timerId)
  }
}
```

Cancellation reads the lookup table to find the fire time, then deletes both entries atomically in the same `WriteBatch`.

If a user intentionally wants a speculative / best-effort timer loop that can act on visible-but-not-yet-durable rows, that is a separate weaker pattern and should be documented/labeled as such. The default "durable timer" story throughout this document is the authoritative durable-fenced one above.

---

## Transactional Outbox

For reliable delivery of external side effects, write an outbox row in the same `WriteBatch` as the business change, then consume that table through the engine's **durable** change-capture stream:

```typescript
// Writer: record the intent in the same batch as the business logic.
// IDs must be derived from stable inputs, not ambient now()/uuid().
const batch = db.writeBatch()
batch.put(ordersTable, orderId, orderData)
batch.put(outboxTable, outboxKey(`order:${orderId}:send_confirmation_email`), {
  type: "send_confirmation_email",
  orderId,
  idempotencyKey: `order:${orderId}:send_confirmation_email`,
})
await db.commit(batch)

// Consumer: tail the durable outbox stream and execute side effects idempotently.
let cursor = loadOutboxCursor()
const notifications = db.subscribeDurable(outboxTable)

while (true) {
  while (true) {
    let page: ChangeEntry[] = []
    const entries = await db.scanDurableSince(outboxTable, cursor, { limit: BATCH_SIZE })
    for await (const change of entries) page.push(change)
    if (page.length === 0) break

    for (const change of page) {
      const entry = deserialize(change.value)
      await sendEmail(entry.orderId, { idempotencyKey: entry.idempotencyKey })
      cursor = change.cursor
      persistOutboxCursor(cursor)
    }
  }

  await notifications.recv()
}
```

The business write and the outbox entry are atomic (same `WriteBatch`). The external side effect is **at-least-once**, with the idempotency key allowing the external system to deduplicate. This example intentionally uses `subscribeDurable()` + `scanDurableSince()` rather than the Part 2 `ChangeConsumer` helper, because the outbox row is keyed by a stable semantic identifier, not by an application-managed sequence embedded in the user key.

---

## External Stream Ingress (Kafka)

Kafka support should be provided by a **separate library** layered above the DB, not by extending the engine. The library consumes Kafka partitions, decodes records, writes them into ordinary Terracedb tables, and persists Kafka progress in the **same OCC unit** as those writes.

```typescript
interface KafkaIngressDefinition {
  consumerGroup: string
  sources: KafkaPartitionSource[]
  filter?: KafkaRecordFilter
  handler: KafkaIngressHandler
}

interface KafkaPartitionSource {
  topic: string
  partition: number
  bootstrap: "earliest" | "latest"
}

interface KafkaIngressHandler {
  apply(records: KafkaRecord[], tx: Transaction): Promise<void>
}

interface KafkaRecordFilter {
  keep(record: KafkaRecord): boolean
}
```

The critical correctness rule mirrors workflow source admission: **materialized writes and Kafka progress must commit together**. If the process crashes before commit, neither the Terracedb tables nor the Kafka offset advance is visible. If the commit succeeds, both are durably recoverable according to the selected storage mode.

```typescript
async function admitKafkaBatch(db: DB, source: KafkaPartitionSource, records: KafkaRecord[]) {
  const tx = await Transaction.begin(db)

  let nextOffset = loadPersistedOffset(source)
  for (const record of records) {
    await handler.apply([record], tx)
    nextOffset = record.offset + 1
  }

  tx.write(kafkaOffsetTable, kafkaOffsetKey(source.topic, source.partition), encodeOffset(nextOffset))
  await tx.commit({ flush: false })
}
```

The persisted source-progress table is conceptually the Kafka counterpart of a durable `LogCursor` store:

- one row per `(topic, partition)`,
- loaded on restart before resuming consumption,
- advanced only in the same commit as the materialized DB writes.

Kafka ingress may also apply a **deterministic filter** before materialization. This is important when the upstream stream is much larger than the subset Terracedb should retain. The filter must be pure with respect to the consumed record and configuration; replay cannot depend on ambient time or external lookups.

If a record is filtered out, Kafka progress still advances past it. In other words, filtered records are treated as intentionally ignored input, not as retriable failures. This makes filtering operationally practical for large streams, but it also means filtered records do not exist in Terracedb history unless a broader upstream or parallel materialization retains them elsewhere.

**Ordering model:** Kafka partitions are the unit of authoritative order. If downstream replay depends on record order, the Terracedb materialization must preserve that partition-local order in user keys and table layout. Kafka does not provide a global total order across partitions, and Terracedb should not invent one.

Two table layouts are natural:

- **One Terracedb source table per Kafka partition.** This maps cleanly onto the existing projection/workflow runtimes, which already track one cursor per source table.
- **One shared Terracedb table keyed by `(partition, offset)`.** This can work for current-state materializations or bespoke consumers, but replay-sensitive projections and workflows still need to reason in terms of per-partition ordering.

External consumers above Kafka ingress use the resulting Terracedb tables just like any other source table. The projection and workflow libraries do **not** read Kafka directly.

---

## Debezium CDC on Kafka

Debezium support should be a second library layered on top of Kafka ingress. The Kafka layer provides durable source-progress management and batched transactional apply; the Debezium layer is responsible for decoding connector envelopes, transaction metadata, tombstones, and snapshot markers, then materializing one or more Terracedb tables.

Because Debezium is commonly attached to very large databases, filtering should be a **first-class concern** of the library. The intended model is:

- **table/schema selection** at the connector or topic-routing layer, so unwanted tables are never materialized into Terracedb,
- **deterministic row filtering** over the normalized Debezium event, so Terracedb can keep only the relevant subset of rows from the selected tables, and
- optional **column projection/redaction** before writing Terracedb values, when the application needs only part of each row.

Row filtering should be expressed over deterministic event fields such as:

- connector/schema/table identity,
- primary key,
- operation kind,
- snapshot marker,
- `before`/`after` row values, and
- connector-provided source metadata.

For example, an application might ingest only:

- the `public.orders` and `public.refunds` tables from a much larger commerce database,
- only rows whose `tenant_id` is in an allowlist, or
- only rows whose `region == "west"` and `priority == "expedited"`.

For a relational source such as PostgreSQL, Debezium materialization should support three modes:

| Mode | Terracedb tables | Best for | Trade-off |
|---|---|---|---|
| **EventLog** | append-only `*_cdc` tables keyed by deterministic replay order | history-sensitive projections, historical workflow bootstrap/replay, audit | highest storage and write amplification |
| **Mirror** | PK-keyed `*_current` tables containing latest state | current-state queries and indexes | not sufficient as the only source for history-dependent replay/rebuild |
| **Hybrid** | both `*_cdc` and `*_current`, written atomically | most production deployments | more space than mirror-only, but preserves replayable history |

In **EventLog** mode, the Debezium row becomes a Terracedb append-only event. The key must encode the replay order the downstream consumer depends on. In practice, that usually means one table per Kafka partition with keys ordered by Kafka offset, or an equivalent partition-local ordering scheme explicitly encoded in the key. This is the correct source for:

- history-sensitive projections,
- workflow sources configured for historical bootstrap or replay,
- transition detection,
- audit/debug timelines, and
- any future rebuild path that scans source SSTables rather than the commit log.

In **Mirror** mode, Terracedb stores only the latest source row keyed by the relational primary key. Deletes remove the row or store a tombstone according to the selected materialization policy. This is space-efficient and query-friendly, but it intentionally gives up replayable mutation history. Mirror mode is therefore appropriate for:

- read models that depend only on latest state,
- point lookups and secondary indexes over current rows, and
- applications that explicitly choose live-only workflow attachment rather than historical replay.

In **Hybrid** mode, the ingress library writes both materializations and the Kafka/Debezium source-progress update in the same transaction. The append-only table is the **semantic source of truth**; the mirror is a convenience query surface.

Filtering interacts with these modes in an important way:

- in **filtered ingestion** mode, Terracedb stores only the selected subset, which is efficient for very large upstream databases,
- in **full-fidelity** mode, Terracedb stores the whole selected upstream table history and derives narrower views later, and
- in either case, if a row is filtered out before EventLog materialization, it is absent from Terracedb replay history as well as from current-state mirrors.

For mirror/current-state materializations, row filtering must account for **membership transitions**. If a row previously matched the filter and now no longer does, the mirror should remove it rather than leave stale state behind.

**Interaction with projections:** projections consume Debezium-derived Terracedb tables exactly like any other source table. History-dependent projections should read from append-only `*_cdc` tables. Current-state read models may read from `*_current` mirrors. When the source is Kafka-partitioned, a projection should usually declare one source table per partition and rely on the normal multi-source frontier rules rather than assuming a hidden global order.

**Interaction with workflows:** workflows should generally not execute side effects directly from raw Debezium bootstrap snapshots unless that behavior is explicitly configured. A common pattern is:

1. ingest Debezium into append-only `*_cdc` tables,
2. use projections to derive higher-level transition tables or semantic events,
3. route workflows from those transition tables rather than directly from raw row-level CDC.

When a workflow does consume Debezium-derived sources directly, historical bootstrap/replay is only sound from append-only ordered `*_cdc` tables. Mirror tables are compatible with live-only attachment, but not with general replay after history loss.

---

## Data Modeling: Event Sourcing

This section describes a data modeling approach — not an engine feature — that significantly affects how projections (Part 3) and workflows (Part 4) behave, particularly during recomputation and recovery.

### The Core Idea

Model business data as **immutable, append-only event records** rather than mutable entity records. An event captures something that happened: "user clicked button," "order placed," "name changed to Bob." Events are never overwritten or deleted under normal operation.

Current state is not stored directly. It is derived — computed from events by projections. If you need "the user's current name," a projection maintains that by processing `NameChanged` events, not by reading a mutable `users` table.

### Why This Matters for Projections

Projections tail the commit log via `scanSince` on the happy path. When the commit log's retention horizon is exceeded (`SnapshotTooOld`), projections fall back to **recomputing from SSTables** — the compacted state on disk.

Compaction's effect on SSTables depends on the data model:

**Append-only events:** compaction has minimal impact. Events are never overwritten, so every event survives compaction in its original form. A projection recomputing from SSTables sees the complete history **provided the source data also encodes the deterministic replay order the projection relies on** (for example, per-entity version order, commit-log order, or an equivalent stable ordering embedded in keys/metadata).

**Mutable records:** compaction collapses history. Old values of overwritten keys are discarded. Tombstones for deleted keys are eventually removed. SSTables contain only current state, not mutation history.

This creates a split in recomputation safety:

| Projection type | Depends on... | Recompute from SSTables? |
|---|---|---|
| Current-value read model (e.g., secondary index) | Current state | Safe regardless of data model |
| Running aggregate (e.g., windowed count, event funnel) | Mutation history | Safe only with append-only events |
| State transition detection (e.g., "name changed from A to B") | Ordered history per entity | Safe only with append-only events |
| Change replay (e.g., replication consumer) | Full mutation stream | Safe only with append-only events |

If your source data is mutable and your projection depends on history, recomputation from SSTables will produce incorrect results. The original mutations are gone.

### Running-State Projections

Even with event sourcing, some projections require more than filtering individual events. A projection like "users whose name changed from A to B" cannot be answered from a single event — the event says `NameChanged { name: "Bob" }` but doesn't include the old name. The transition is implicit in the *sequence* of events per entity.

To recompute such a projection, you must replay the full event history per entity in order, maintaining running state (e.g., "last known name for this user") and detecting transitions as you go. This is correct — the events are all in SSTables — but it is an ordered sequential replay, not a simple filter.

Projection complexity scales with how much context is needed beyond the individual event:

- **Self-contained event projections:** each event has everything the projection needs. Recomputation is a scan with a filter. Cheap.
- **Running-state projections:** the projection needs ordered per-entity history to reconstruct state. Recomputation is a full ordered replay. Correct but more expensive.
- **Cross-entity projections:** the projection correlates events across multiple entities. Recomputation is a multi-stream replay with coordinated state. Most expensive.

### Compliance Deletes

GDPR and similar regulations require genuine deletion of personal data. Under event sourcing, this means deleting original event records — not just derived state.

When events are deleted for compliance, existing projection entries for that entity are cleaned up at the same time (same `WriteBatch` or a follow-up projection pass). Future projections recomputing from SSTables will naturally exclude the deleted entity because their events are gone. Compliance deletion intentionally changes future recomputation results by removing the source events themselves. Projections recomputed after deletion reproduce the legally retained world, not the pre-deletion historical world. This is the desired behavior — but it means projection outputs are not historically stable across compliance deletions.

### Recommendation

Event sourcing is the recommended default for data that drives history-dependent projections. For behavioral data (clicks, actions, API calls), this is the natural modeling choice. For entity state (user profile, settings, roles), it requires modeling state changes as explicit events (e.g., `NameChanged`, `RoleAssigned`) — less intuitive but necessary for safe recomputation. Append-only storage alone is not enough: the source events must also carry or imply a deterministic replay order for the projections that consume them.

If you model entity state as mutable records, understand that history-dependent projections over that data can only be maintained via `scanSince` (the happy path) and cannot be safely recomputed from SSTables after `SnapshotTooOld`. Current-state projections (indexes, read models) are unaffected.

---

# Part 3: Projection Library

A projection is any maintained derived state built from the DB's change stream. It is a **deterministic state updater with read access** — given the same input events and the same DB state, it always produces the same output. Projections do not cause side effects. They read from source tables, compute derived state, and declare output writes.

The projection library is a separate library built on core DB primitives. It manages the lifecycle of projections: cursor persistence, watermark tracking, dependency ordering, and recomputation on `SnapshotTooOld`. The core DB engine has no concept of "projection."

A classic materialized view (source table → derived read model) is the simplest kind of projection. But projections can also maintain intermediate state, join across multiple source tables, depend on other projections' outputs, or use running per-entity state to detect transitions. The library provides the same cursor/watermark/lifecycle infrastructure for all of them.

---

## Projection Handler Model

A projection is defined by an **async handler** that processes a whole **source-sequence batch** against a frontier-pinned snapshot:

```typescript
interface SourceBatch {
  table: Table
  sequence: SequenceNumber           // one committed source sequence
  entries: ChangeEntry[]             // all entries from this source table at that sequence
  lastCursor: LogCursor              // resume token after the final entry in the batch
}

interface ProjectionHandler {
  // Called once per source-table batch (never per individual entry).
  // The handler may read other tables, but only through the frontier-pinned context.
  handleBatch(batch: SourceBatch, ctx: ProjectionContext): Promise<WriteBatch>
}

interface ProjectionContext {
  frontier(): Record<Table, SequenceNumber>
  read(table: Table, key: Key): Promise<Value | null>
  scan(table: Table, start: Key, end: Key): Promise<AsyncIterator<[Key, Value]>>
  // No write access. Output is exclusively through the returned WriteBatch.
}
```

The key semantic rule is that **reads are pinned to the projection frontier**. If the current frontier is `{A:100, B:10}` and the handler is processing the source-table batch `A@100`, then:

- `ctx.read(A, ...)` means read A as of sequence 100,
- `ctx.read(B, ...)` means read B as of sequence 10,
- likewise for scans.

Internally, the projection library implements this with `readAt` / `scanAt` against the per-table frontier vector. The handler never sees "latest other-table state" that lies beyond the recorded frontier.

This has three important consequences:

1. **Whole-batch progress:** a projection only advances a source cursor/watermark after the *entire* source-sequence batch has been reflected. This preserves meaningful `waitForWatermark(... sequence)` semantics.
2. **Deterministic replay:** re-running from the same per-source frontier sees the same input batch and the same cross-table snapshot, so it produces the same output.
3. **No projection side effects:** handlers remain declarative state updaters. They return a `WriteBatch`; the library applies it atomically with cursor advancement.

Because `readAt` / `scanAt` may require cold I/O, the handler model is async. The async boundary is explicit and honest: the projection runtime, not ambient cache assumptions, defines the semantics.

---

## Projection Lifecycle

```typescript
interface ProjectionDefinition {
  name: string
  sources: Table[]                     // one or more source tables to tail
  outputTables: Table[]                // tables the handler writes to
  handler: ProjectionHandler
  dependencies?: string[]              // names of upstream projections (for multi-stage)
  retentionHorizon?: Duration          // how long source commit log must be retained
}

interface ProjectionLibrary {
  register(def: ProjectionDefinition): void
  start(): void                        // begins tailing all registered projections
  watermark(name: string, source: Table): SequenceNumber
  waitForWatermark(
    name: string,
    targets: Record<Table, SequenceNumber>,  // per-source-table sequences to wait for
    opts?: { timeout?: number }
  ): Promise<void>
}
```

`waitForWatermark` is notification-driven, not polling-based. It resolves when the projection has committed and made **visible** output whose recorded cursor state for *each* specified source table has advanced past the corresponding sequence number. Only the source tables the caller specifies are checked — quiet sources not in `targets` do not block the wait. If the timeout is reached before all targets are met, it rejects with `TimeoutError`.

This is a **visibility fence for reflected output**, not a reflected-durability fence. In deferred-durability modes the projection may have made output visible before that output itself has crossed the storage durability boundary.

Internally, each projection maintains per-source-table cursor state `{ cursor, sequence }`. Watermarks advance only when the runtime commits the handler output **and** the cursor state for the full source-sequence batch in one atomic `WriteBatch`.

On `start()`, the library:

1. Loads persisted cursor state from a `_projection_cursors` table.
2. Performs an **initial catch-up pass across all sources** before blocking on notifications. Startup has no wake hint, so the runtime must probe/drain every source until no source can prove more ready work.
3. Subscribes to the source tables via `db.subscribeDurable(table)`.
4. On each notification, treats the notified table as a wake hint: it drains that source first, then continues probing/draining until the projection is quiescent again.
5. Advances the projection's `WatermarkTracker` as source-sequence batches are committed, resolving any pending `waitForWatermark` futures.

---

## External CDC Sources

The projection library consumes **Terracedb tables**, not Kafka topics directly. External stream systems such as Kafka and Debezium must first be materialized into ordinary source tables by ingress libraries from Part 2.

For Debezium-derived sources, the intended mapping is:

- use append-only ordered `*_cdc` tables for history-sensitive projections, replayable aggregations, transition detection, and any rebuild path that must survive `SnapshotTooOld`,
- use current-state `*_current` mirrors for read models that depend only on latest state, and
- in hybrid mode, treat the append-only table as the semantic source of truth and the mirror as a convenience/query surface.

When ingress applies schema/table or row filtering before materialization, projections and workflows see only that retained subset. This is often the right trade-off for large upstream databases, but it should be an explicit choice: filtering before `*_cdc` materialization reduces Terracedb storage and fanout at the cost of losing the filtered-out portion of replay history inside Terracedb.

When Kafka partitions are materialized as separate Terracedb tables, each partition naturally becomes one projection source. Deterministic replay then follows the ordinary multi-source frontier semantics of this Part: Terracedb preserves per-source order and records a vector frontier, but it does not impose a hidden total order across independent partitions.

---

## Cursor Management

Each projection maintains one cursor state per source table, persisted in a `_projection_cursors` table:

```typescript
interface ProjectionCursorState {
  cursor: LogCursor
  sequence: SequenceNumber
}

async function runProjection(db: DB, def: ProjectionDefinition) {
  const state: Map<Table, ProjectionCursorState> = new Map()
  for (const source of def.sources) {
    const saved = await db.table("_projection_cursors").read(cursorKey(def.name, source.name))
    state.set(source, saved ? decodeProjectionCursorState(saved) : {
      cursor: LogCursor.beginning(),
      sequence: 0,
    })
  }

  async function drainNextRun(table: Table): Promise<boolean> {
    const current = state.get(table)!
    const run = await scanWholeSequenceRun(db, table, current.cursor, { limit: BATCH_SIZE, durable: true })
    if (!run) return false

    const frontier = new Map<Table, SequenceNumber>()
    for (const source of def.sources) {
      frontier.set(source, state.get(source)!.sequence)
    }
    frontier.set(table, run.sequence)

    const ctx = new FrontierProjectionContext(db, frontier)
    const output = await def.handler.handleBatch(run, ctx)

    output.put(
      db.table("_projection_cursors"),
      cursorKey(def.name, table.name),
      encodeProjectionCursorState({ cursor: run.lastCursor, sequence: run.sequence })
    )

    await db.commit(output)

    state.set(table, { cursor: run.lastCursor, sequence: run.sequence })
    def.watermarkTracker.advance(table, run.sequence)
    return true
  }

  async function probeNextRun(table: Table): Promise<SourceBatch | null> {
    const current = state.get(table)!
    return await scanWholeSequenceRun(db, table, current.cursor, { limit: BATCH_SIZE, durable: true })
  }

  async function drainUntilQuiescent(_hintedSources: Table[]) {
    while (true) {
      const ready: Array<{ table: Table, run: SourceBatch }> = []
      for (const table of def.sources) {
        const run = await probeNextRun(table)
        if (run) ready.push({ table, run })
      }
      if (ready.length === 0) break

      ready.sort((a, b) =>
        a.run.sequence !== b.run.sequence
          ? a.run.sequence - b.run.sequence
          : def.sources.indexOf(a.table) - def.sources.indexOf(b.table)
      )

      const next = ready[0]
      await drainNextRun(next.table)
    }
  }

  await drainUntilQuiescent(def.sources)   // initial catch-up before blocking

  const notifications = mergeSubscriptions(
    def.sources.map(source => db.subscribeDurable(source))
  )

  for await (const { table } of notifications) {
    await drainUntilQuiescent([table])
  }
}
```

`scanWholeSequenceRun(...)` is a projection-runtime helper that guarantees the handler never sees a partial run for a source table at a single committed sequence. If the page limit cuts through a same-sequence run, the runtime extends the read through the end of that run before invoking the handler.

`drainUntilQuiescent(...)` must not simply drain the wake-hinted source first. It probes sources for their next available whole-sequence run, then chooses the next batch in deterministic order: **lowest `sequence`, tie-broken by `def.sources` declaration order**. That is what makes multi-source replay stable across wake timing.

The cursor-state update and the output writes are in the same `WriteBatch` + `db.commit()`. If the process crashes before commit, neither the output nor the cursor advance becomes visible.

---

## Single-Source Projections

The simplest case: one source table, one output table, stateless handler.

### Secondary Index

```typescript
const emailIndex: ProjectionDefinition = {
  name: "email-index",
  sources: [usersTable],
  outputTables: [emailIndexTable],
  handler: {
    async handleBatch(batch, ctx) {
      const out = db.writeBatch()
      for (const entry of batch.entries) {
        if (entry.kind === "put") {
          const user = deserialize(entry.value)
          out.put(emailIndexTable, user.email, entry.key)  // email → userId
        } else if (entry.kind === "delete") {
          // Need to read old value to know which email to remove.
          // For append-only events this is unnecessary; for mutable records
          // the handler must track prior state or scan the index.
        }
      }
      return out
    }
  }
}
```

When the source data is append-only events, the delete branch is rarely needed (only for compliance deletes). When the source is mutable records, the handler needs to track previous values — which is why event sourcing simplifies projection logic.

### Counter / Aggregate

```typescript
const eventCountByType: ProjectionDefinition = {
  name: "event-count-by-type",
  sources: [eventsTable],
  outputTables: [countsTable],
  handler: {
    async handleBatch(batch, ctx) {
      const out = db.writeBatch()
      for (const entry of batch.entries) {
        const event = deserialize(entry.value)
        out.merge(countsTable, event.type, encodeInt64(1))
      }
      return out
    }
  }
}
```

Uses the merge operator for blind aggregation — no read needed.

### Uniqueness Constraints

Uniqueness enforcement (e.g., unique email) is a special case that requires OCC coordination. The projection maintains the index, but the write path uses `db.commit(batch, { readSet })` to detect concurrent duplicates:

```typescript
async function insertUserUnique(db: DB, usersTable: Table, emailIndex: Table, user: User) {
  const snap = await db.snapshot()
  const readSet = db.readSet()

  const existing = await snap.read(emailIndex, user.email)
  readSet.add(emailIndex, user.email, snap.sequence)
  snap.release()

  if (existing) throw new DuplicateEmailError()

  const batch = db.writeBatch()
  batch.put(usersTable, user.id, serialize(user))
  batch.put(emailIndex, user.email, user.id)
  await db.commit(batch, { readSet })
}
```

On `ConflictError`, the caller retries by re-reading the index key.

Note: this example uses colocated synchronous maintenance (index updated in the same batch as the source write), not the async projection pattern. Uniqueness requires synchronous enforcement — you can't defer it to an async projection and have it reject writes retroactively.

---

## Multi-Source Projections

A projection that reads from multiple source tables — for example, enriching user events with team membership data — uses one cursor state per source table and a **frontier-pinned** read context.

```typescript
const userActivityWithTeam: ProjectionDefinition = {
  name: "user-activity-with-team",
  sources: [userEventsTable, teamMembersTable],
  outputTables: [enrichedActivityTable],
  handler: {
    async handleBatch(batch, ctx) {
      const out = db.writeBatch()

      if (batch.table === userEventsTable) {
        for (const entry of batch.entries) {
          const event = deserialize(entry.value)
          const membership = await ctx.read(teamMembersTable, memberKey(event.userId))
          out.put(enrichedActivityTable, activityKey(event.id), serialize({
            ...event,
            teamId: membership ? deserialize(membership).teamId : null,
          }))
        }
      }

      if (batch.table === teamMembersTable) {
        for (const entry of batch.entries) {
          const membership = deserialize(entry.value)
          // Rebuild or update any derived rows affected by this membership change.
          // Reads during this batch are still pinned to the frontier vector.
        }
      }

      return out
    }
  }
}
```

Multi-source projections are still **deterministic**, but the determinism is defined against a **vector frontier**, not against a single globally merged event stream. If the current frontier is `{ userEvents: 100, teamMembers: 10 }`, then any read of `teamMembers` while processing `userEvents@100` is evaluated **as of sequence 10**, not against the latest table state.

This is the precise rule that makes replay sound:

- progress is recorded as a per-source vector,
- reads are evaluated against that vector,
- output + vector advancement are committed atomically.

The tradeoff is that cross-table reads may be intentionally stale by frontier. That is fine: later batches from the other source advance its frontier and update the derived output. What the runtime does **not** do is read "latest other-table state" beyond the frontier.

Because of that rule, transition detection inside a multi-source projection is reliable **only when the transition condition is defined over the frontier-pinned state**. The projection runtime provides deterministic replay; it does not promise a single total order across independent source streams.

Exact transitive `waitForWatermark` propagation through a DAG of multi-source nodes still requires source-frontier provenance on emitted outputs. This document keeps precise replay semantics *within* a multi-source projection, but downstream multi-source watermark propagation remains conservative unless that provenance is explicitly tracked.

---

## Multi-Stage Projections

Projections can depend on other projections' output tables, forming a DAG. The library uses **declared dependencies** to ensure ordering.

```typescript
const rawCounts: ProjectionDefinition = {
  name: "raw-counts",
  sources: [eventsTable],
  outputTables: [countsTable],
  handler: { /* count events by type */ }
}

const topCategories: ProjectionDefinition = {
  name: "top-categories",
  sources: [countsTable],  // reads from raw-counts output
  outputTables: [topCategoriesTable],
  dependencies: ["raw-counts"],  // declared dependency
  handler: {
    async handleBatch(batch, ctx) {
      // Processes count updates, maintains a sorted top-N
      // from the whole source-sequence batch.
      // ...
    }
  }
}
```

The library tracks declared dependencies and does not report a downstream projection as caught up past sequence S until its upstream dependencies have processed through at least that sequence. `top-categories` will not advance its watermark past a point that `raw-counts` has not yet reached.

### Transitive Watermark Propagation

When a caller requests `waitForWatermark("top-categories", { [eventsTable]: seq })`, the library automatically computes the transitive closure: it waits for `raw-counts` to reach at least `seq` on the events source, then waits for `top-categories` to process the resulting output. The caller does not need to know the dependency graph.

```typescript
const seq = await db.commit(batch)
await projections.waitForWatermark("top-categories", { [eventsTable]: seq }, { timeout: 5000 })
const top = await topCategoriesTable.read("top-10")
```

Transitive propagation is precise for chains of single-source projections. For DAG nodes that are themselves multi-source, exact transitive waits require source-frontier provenance to be propagated alongside emitted outputs. Without that extra metadata, downstream waits are conservative/best-effort even though each individual multi-source node still replays deterministically from its own frontier.

---

## State Transitions as Projections

A general pattern for detecting when per-entity state crosses a predicate boundary. Each owner maintains local state and emits **transition records** when predicates change. Downstream projections build cross-entity views from those transitions.

### Pattern

1. **Maintain local state** per routing key (counters, flags, cached aggregates).
2. **Evaluate predicates** on the local state after each mutation.
3. **Emit transition records** when a predicate's truth value changes (entry → active, exit → inactive).
4. **Build broader views** from the transition stream using cross-entity async projections.

```typescript
// Projection handler that maintains state and emits transitions
const stateTransitionEmitter: ProjectionHandler = {
  async handleBatch(batch, ctx) {
    const out = db.writeBatch()
    const stagedState = new Map<Key, EntityState>()

    for (const entry of batch.entries) {
      const event = deserialize(entry.value)
      const key = entityKey(event.entityId)

      let oldState = stagedState.get(key)
      if (!oldState) {
        const committedRaw = await ctx.read(stateTable, key)
        oldState = committedRaw ? deserialize(committedRaw) : emptyEntityState()
      }

      const newState = computeNewState(oldState, event)
      stagedState.set(key, newState)
      out.put(stateTable, key, serialize(newState))

      for (const pred of predicates) {
        const wasActive = pred.evaluate(oldState)
        const isActive = pred.evaluate(newState)

        if (isActive !== wasActive) {
          out.put(transitionsTable, transitionKey(entry.sequence, entry.cursor, pred.id), serialize({
            entity: event.entityId,
            predicate: pred.id,
            direction: isActive ? "entered" : "exited",
            sourceSequence: entry.sequence,
            eventTime: event.occurredAt,
          }))
        }
      }
    }

    return out
  }
}
```

The transition table key is derived from stable source metadata (`sequence`, `cursor`, predicate ID), so replay is deterministic; exact commit ordering still comes from `scanSince`. Downstream projections tail the transitions table to maintain cross-entity views (e.g., "currently active entities" set). The local `stagedState` map is required because `ctx` exposes only pre-batch committed state, not writes staged earlier in the same batch.

### Why Transitions, Not Global Recomputation

- **Write path is entity-local.** The emitter only touches local state and appends transitions. No global lock.
- **Downstream projections scale independently.** They run at their own pace, reading from a change feed.
- **Many predicates, one pattern.** Threshold crossings, membership changes, feature-flag transitions, SLA breach detection, billing tier changes — all follow this shape.
- **Exactly-once for DB state.** Transition emission + state update are in one `WriteBatch`.

---

## Windowed Aggregations as Projections

Moving window computations (e.g., "entities with ≥ N events in the last T minutes") built from core primitives. This is a concrete instance of the **State Transitions** pattern: per-entity state (bucketed counters) is maintained locally, threshold crossings are emitted as transition records, and downstream projections build broader views from the crossing stream.

The following example uses a user-activity monitoring workload (per-user event counts, threshold-based alerting). The `userId` / `eventType` names are illustrative — the pattern generalizes to any entity type with time-bucketed counters and threshold crossings.

### Data Model

Five user-created tables:

| Table | Purpose | Key | Configuration |
|---|---|---|---|
| **events** | Raw event store | `eventId` | Bloom filter for dedup |
| **buckets** | Per-minute counts | `(userId, eventType, bucketTimestamp)` | Merge operator: counter. Compaction filter: TTL. |
| **window_state** | Active/inactive flag + cached total | `(userId, eventType, ruleId)` | Standard key-value |
| **crossings** | Entry/exit change feed | `(eventId, ruleId)` | FIFO compaction, no bloom filter |
| **timers** | Durable scheduled callbacks | `(fireAt, timerId)` | Standard key-value |

This example uses the single-table timer pattern (schedule table only). Cancellation is not needed here — timers fire and are deleted atomically. For workflows requiring frequent cancellation, use the two-table pattern from **Durable Timers** in Part 2.

### Bucket Granularity

Buckets are fixed-size time slots (e.g., 1 minute) independent of any rule's window size. Multiple rules with different window durations query the same buckets with different range bounds. For large windows, maintain multiple granularities (1m, 1h, 1d) to keep scan size bounded.

### Write Path

On each incoming event, all updates go in a single `WriteBatch`:

1. Deduplicate via `eventId` point lookup (bloom filter assisted).
2. Increment the bucket via `merge()` — blind write, no read required.
3. Evaluate window rules using **incremental maintenance**: read cached total from `window_state`, subtract expired trailing-edge buckets, add 1 for the current event.
4. If threshold crossed, update `window_state` and append to `crossings` table.

```typescript
async function onEvent(db: DB, event: RawEvent, rules: WindowRule[]) {
  const existing = await db.table("events").read(eventKey(event.eventId))
  if (existing) return  // deduplicate

  const batch = db.writeBatch()
  batch.put(db.table("events"), eventKey(event.eventId), serialize(event))

  const bucket = floorToMinute(event.eventTime)
  batch.merge(db.table("buckets"), bucketKey(event.userId, event.type, bucket), encodeInt64(1))

  for (const rule of rules) {
    const stateTable = db.table("window_state")
    const state = await stateTable.read(windowStateKey(event.userId, event.type, rule.id))
    const newTotal = incrementalEvaluate(state, event, rule)
    const nowActive = rule.operator(newTotal, rule.threshold)

    batch.put(stateTable, windowStateKey(event.userId, event.type, rule.id),
      serialize({ active: nowActive, cachedTotal: newTotal, windowStart: floorToMinute(event.eventTime - rule.windowDuration) }))

    if (nowActive !== (state?.active ?? false)) {
      batch.put(db.table("crossings"), crossingKey(event.eventId, rule.id), serialize({
        userId: event.userId, eventType: event.type,
        direction: nowActive ? "entered" : "exited",
        count: newTotal, timestamp: event.eventTime,
      }))
    }
  }

  await db.commit(batch)
}
```

**Concurrency note:** the dedup check (read then write) is not atomic as written. If two concurrent workers process the same event ID, both may see it as missing and both insert. If dedup correctness matters under concurrency, use `db.commit(batch, { readSet })` with the event key in the read set to detect the race. For many workloads (single-threaded ingest, or idempotent downstream effects), the plain read + batch is sufficient.

The same concurrency caveat applies to `window_state` reads — the `read` of cached total followed by `put` of new total is not atomic relative to concurrent writers for the same user/rule. **This pattern assumes single-writer-per-entity discipline** — each entity/rule combination should be processed by a single task. This is an application-level convention, not an engine-enforced guarantee. Without single-writer discipline, use `db.commit(batch, { readSet })` with the `window_state` key in the read set, or redesign around commutative merge-based state updates.

### Exit Detection

Entries are detected immediately on the write path. Exits due to inactivity are handled by durable timers. When a user enters the window, a timer is scheduled for when their oldest contributing bucket will age out. The timer fires, re-evaluates the window, and emits an exit crossing if the count dropped below threshold.

### Crossings as Change Feed

The `crossings` table is typically consumed through the core change-capture surface rather than the special application-keyed change-feed pattern above. Best-effort in-process maintenance can consume it via the visible stream, but authoritative downstream consumers — especially workflows or side-effecting handlers such as a reactivation email sender — should use `db.subscribeDurable()` and `scanDurableSince()` so progress is tied to the durable prefix.

---

## Recomputation from SSTables

When a projection receives `SnapshotTooOld` from `scanSince`, it must recompute from SSTables rather than from the commit log.

The projection library handles this automatically:

1. Detect `SnapshotTooOld` on `scanSince` call.
2. If a **projection checkpoint** exists on S3, restore it and resume from the checkpoint sequence.
3. Otherwise, stream through all source SSTables (hot + cold):
   - Cold SSTables are fetched directly from S3 without loading onto the SSD.
   - For columnar SSTables, only the columns relevant to the projection are fetched (S3 range reads).
   - The projection's merge operator folds rows as they stream through, keeping memory bounded.
4. Once the SSTable scan completes, switch back to `scanSince` tailing from the new cursor position.

**Projection checkpoints** on S3 enable partial recomputation: the library periodically snapshots the projection's output table and cursor position. Full recomputation (streaming all SSTables) is only needed when the projection definition changes or no checkpoint exists.

Recomputation correctness depends on the data modeling guidance from the **Event Sourcing** section: history-dependent projections produce correct results on recomputation only when the source data is append-only events.

---

## Read Consistency

Readers choose their consistency mode at read time:

```typescript
// Fast (stale ok) — returns immediately, projection might not reflect latest writes
const result = await derivedTable.read(key)

// Consistent (visible) — blocks until the named projection has made visible output
// reflecting the caller's source write
const seq = await db.commit(batch)
await projections.waitForWatermark("my-projection", { [sourceTable]: seq }, { timeout: 5000 })
const result = await derivedTable.read(key)
```

The caller specifies which source table(s) they wrote to and what sequence(s) to wait for. Only those sources are checked — quiet sources not mentioned in the targets do not block the wait.

Current `waitForWatermark(...)` proves **visible reflected progress**, not durable reflected progress. If a caller needs the derived row itself to be durably reflected in deferred-durability modes, the runtime would need an additional reflected-durability tracker (or the caller must establish durability by some stronger application-level checkpointing rule).

For multi-stage projections with single-source nodes, `waitForWatermark` can propagate through the dependency chain: waiting for a downstream projection to process a source write implies waiting for upstream projections to process it first. For multi-source nodes in the DAG, exact transitive waits require source-frontier provenance on emitted outputs; without that extra metadata, downstream waits are conservative/best-effort even though each node still replays deterministically from its own frontier (see **Multi-Source Projections**).

### Cost Model

- **Write:** identical regardless of how many projections exist. Writers only write to source tables.
- **Fast read:** zero overhead — reads whatever the projection currently contains.
- **Consistent read:** blocks for however long the projection (and its upstream dependencies) take to catch up.

---

# Part 4: Workflow Library

A workflow is a **stateful orchestrator that may cause side effects**, using the DB for persistence and the outbox/timer patterns for reliability. Unlike projections (which are deterministic state updaters), workflows interact with the external world: they send emails, call APIs, enqueue jobs, and make decisions based on external responses.

The workflow library is a separate library built on core DB primitives, sibling to the projection library. Both depend on the DB; neither depends on the other. A workflow may read tables that happen to be maintained by projections, but it imports the DB, not the projection library.

```
Projection library ──→ DB
Workflow library   ──→ DB
```

The stronger long-term model for workflows should be explicitly **run-based and history-first**.

- a **workflow definition** names long-lived logic and routing,
- a **workflow bundle** is an immutable implementation artifact for that logic,
- a **workflow run** is one execution epoch pinned to one bundle,
- **workflow history** is the append-only durable replay record,
- **workflow state** is the current mutable summary used for fast execution, and
- **workflow visibility** is a separate operator-facing projection optimized for list/search/describe/history APIs.

This split matters. History is the durable source of truth. State is a speed-oriented summary derived from history and current execution tables. Visibility is a separate product for operators and tooling. These should not collapse into one table or one API.

One design constraint should stay explicit throughout the workflow library:

- workflows must remain implementable directly in Rust without the sandbox, and
- sandbox-authored workflows should be an additional handler/deployment path rather than the only workflow authoring model.

---

## Workflow Instances

A workflow instance is a persistent state machine. Its state is stored in a DB table and advanced by processing durably admitted events, timer firings, or external responses.

```typescript
interface WorkflowDefinition {
  name: string
  runTable: Table                 // durable workflow runs and lifecycle metadata, keyed by runId
  historyTable: Table             // append-only replay record, keyed by (runId, historySeq)
  stateTable: Table               // current mutable workflow state summary, keyed by active run or instance
  visibilityTable: Table          // operator-facing projection for list/search/describe
  inboxTable: Table               // durably admitted event/timer/callback triggers awaiting execution, keyed by (instanceId, triggerSeq)
  triggerOrderTable: Table        // per-instance next trigger sequence for durable ordering
  sourceCursorTable: Table        // progress of durable event-ingress per source table
  timerScheduleTable: Table       // timers keyed by (fireAt, timerId)
  timerLookupTable: Table         // timers keyed by timerId → fireAt
  outboxTable: Table              // side-effect intents
  scheduler?: WorkflowScheduler   // optional inter-instance scheduler; default is fair round-robin over ready instances
  handler: WorkflowHandler        // native Rust handler or adapted sandbox handler
}

interface WorkflowHandler {
  // Determine which workflow instance should process a given event
  routeToInstance(entry: ChangeEntry): string

  // Given current state + incoming trigger, produce new state + side effects + timers
  handle(
    state: WorkflowState | null,
    trigger: WorkflowTrigger,
    ctx: WorkflowContext,
  ): Promise<WorkflowOutput>
}

interface WorkflowContext {
  // Deterministic helpers only. Implementations derive values from trigger/state/history,
  // not from ambient wall-clock time or randomness.
  stableId(scope: string): string
  stableTime(scope: string): Timestamp
}

interface WorkflowOutput {
  newState: WorkflowState         // persisted to state table
  outboxEntries: OutboxEntry[]    // side effects to deliver; IDs/idempotency keys must already be stable
  timers: TimerSchedule[]         // timers to schedule or cancel; timer keys must already be stable
}

type WorkflowTrigger =
  | { kind: "event", entry: ChangeEntry }
  | { kind: "timer", timerId: string, fireAt: Timestamp, payload: bytes }
  | { kind: "callback", callbackId: string, response: ExternalResponse }

interface AdmittedWorkflowTrigger {
  workflowInstance: string
  triggerSeq: number              // durable per-instance ordering across all trigger kinds
  trigger: WorkflowTrigger
}
```

In simple single-run-per-instance configurations, the active `runId` may line up naturally with the logical instance identity. The architecture should still model runs explicitly so upgrades, restarts-as-new, and bundle pinning remain first-class rather than implicit.

The handler is a pure function of `(currentState, trigger) → output`. Every trigger type is first turned into a **self-contained durable inbox row** before execution. The inbox row is the replay unit; the executor then applies the handler output atomically with inbox acknowledgement and timer deletion where relevant.

**Per-instance ordering rule:** every admitted trigger (event, callback, or timer) is assigned a durable, monotonically increasing `triggerSeq` for its workflow instance at admission time. `inboxTable` is keyed by `(instanceId, triggerSeq)`, and the executor processes the **lowest pending** row for each instance. That defines one durable order across competing trigger kinds such as "payment confirmed" versus "payment timeout".

**Inter-instance scheduling is pluggable.** The framework fixes only the correctness rules inside an instance:
- at most one executor is active per workflow instance at a time,
- triggers for a given instance are processed strictly in `triggerSeq` order,
- the scheduler may choose only among **ready instances** (instances that currently have at least one admitted inbox row).

The default scheduler is a fair round-robin / FIFO queue of ready instances. Advanced users may replace it with their own policy and attach metadata/tags that guide scheduling across instances (priority, tenant, shard key, cost class, deadline, etc.), but they cannot violate per-instance ordering.

```typescript
interface WorkflowScheduler {
  markReady(instanceId: string, metadata?: Record<string, any>): void
  nextReadyInstance(): Promise<string | null>
  onInstanceYield(instanceId: string, hasMorePending: boolean): void
}
```

The runtime normalizes `def.scheduler` before any executor/admission loop runs: if the user did not supply one, the library installs the built-in `RoundRobinWorkflowScheduler`.

Think of this split as:
- `inboxTable(instanceId, triggerSeq)` = durable source of truth
- `WorkflowScheduler` = policy for **which ready instance gets the next turn**

The single-process runtime enforces **at most one active executor per instance** with an in-memory `inFlightInstances` set. That is sufficient for the design in this document. A future multi-runtime/distributed executor could replace that with durable claims or leases without changing workflow semantics.

```typescript
async function processWorkflowTrigger(
  db: DB,
  def: WorkflowDefinition,
  instanceId: string,
  trigger: WorkflowTrigger,
  opts: { inboxKey: Key },
) {
  const currentState = await def.stateTable.read(instanceId)
  const ctx = new DeterministicWorkflowContext(instanceId, trigger, currentState)
  const output = await def.handler.handle(currentState, trigger, ctx)

  const batch = db.writeBatch()

  // Acknowledge the admitted trigger in the same batch as the state transition.
  batch.delete(def.inboxTable, opts.inboxKey)

  // Fired timers are deleted using the durable identity carried by the inbox row,
  // not by rereading mutable timer state.
  if (trigger.kind === "timer") {
    batch.delete(def.timerScheduleTable, scheduleKey(trigger.fireAt, trigger.timerId))
    batch.delete(def.timerLookupTable, trigger.timerId)
  }

  batch.put(def.stateTable, instanceId, serialize(output.newState))

  for (const entry of output.outboxEntries) {
    batch.put(def.outboxTable, outboxKey(entry.outboxId), serialize({
      ...entry,
      workflowInstance: instanceId,
    }))
  }

  for (const timer of output.timers) {
    if (timer.action === "schedule") {
      batch.put(def.timerScheduleTable, scheduleKey(timer.fireAt, timer.key), serialize({
        workflowInstance: instanceId,
        timerId: timer.key,
        payload: timer.payload,
      }))
      batch.put(def.timerLookupTable, timer.key, serialize({ fireAt: timer.fireAt }))
    } else if (timer.action === "cancel") {
      const existing = await def.timerLookupTable.read(timer.key)
      if (existing) {
        const { fireAt } = deserialize(existing)
        batch.delete(def.timerScheduleTable, scheduleKey(fireAt, timer.key))
        batch.delete(def.timerLookupTable, timer.key)
      }
    }
  }

  await db.commit(batch)
}
```

State update, outbox entries, timer changes, inbox acknowledgement, and fired-timer deletion are all in one `WriteBatch`. If the process crashes before commit, nothing changes. After commit, the workflow state transition and the acknowledgement of the durable inbox row are atomically visible.

### Workflow Transition Engine and Durability Fences

All workflow inputs should flow through one Rust-owned transition engine:

- admitted source events,
- admitted timer firings,
- admitted callbacks,
- workflow-local retries or wakeups, and
- later query/update-style control messages that are accepted into workflow execution.

That transition engine should be the only place allowed to:

- append workflow history,
- update current workflow state,
- change lifecycle status,
- modify timer ownership or retry state,
- schedule effect intents,
- and acknowledge the admitted inbox row.

The architectural rule is:

- guest code interprets admitted input and proposes commands,
- the Rust executor validates and durably applies those commands,
- and only after that durable apply completes does the runtime hand work to effect delivery, timer wakeup machinery, or other external execution paths.

This is a stricter form of the outbox principle and should be applied uniformly. Workflow correctness must not depend on side effects leaving the process before the durable transition that justifies them.

The workflow library should therefore keep two distinct internal contracts:

- a **public handler contract** used by native Rust handlers and sandbox adapters, and
- a **private internal transition/effects contract** used inside the executor to reduce commands into history/state/timer/outbox mutations.

Those are related, but they should not be conflated.

Admission allocates the per-instance ordering key in the **same OCC unit** that writes the inbox row and any associated cursor/progress update. The helper is therefore a **staging helper**, not a separately committing transaction:

```typescript
async function stageTriggerAdmission(
  tx: Transaction,
  def: WorkflowDefinition,
  instanceId: string,
  trigger: WorkflowTrigger,
): Promise<number> {
  const current = await tx.read(def.triggerOrderTable, instanceId)
  const triggerSeq = (current ? deserializeInt(current) : 0) + 1

  tx.write(def.triggerOrderTable, instanceId, encodeInt64(triggerSeq))
  tx.write(def.inboxTable, inboxKey(instanceId, triggerSeq), serialize({
    workflowInstance: instanceId,
    triggerSeq,
    trigger,
  }))

  return triggerSeq
}
```

The important rule is the atomicity boundary: **trigger-sequence allocation, inbox admission, and any associated source/callback/timer progress advance must commit together in one OCC unit**. There is no separately-committing `allocateTriggerSeq(...)` step. Whether that OCC unit is already durable at commit return depends on the storage mode / flush policy; authoritative executors still consume only from the durable prefix.

**Note on replay and duplicates:** duplicate timer firings or callbacks are possible. That is acceptable because the inbox row is self-contained, acknowledgement is atomic with the state transition, and the handler is written as a state-guarded/idempotent transition. Missing timer schedule/lookup rows on duplicate timer execution are benign no-ops.

### Sandboxed TypeScript Workflow Handlers

The workflow executor should remain Rust-owned even when workflow logic is authored in TypeScript. A sandbox-authored workflow is therefore a different implementation strategy for `WorkflowHandler`, not a second workflow runtime with different correctness rules.

Native Rust workflows remain first-class. A Rust application should be able to implement `WorkflowHandler` directly without depending on the sandbox stack at all. The sandbox path is a companion handler adapter and deployment mechanism, not a replacement for native workflow authoring.

Recommended layering:

- `terracedb-workflows` remains authoritative for durable inbox ordering, timer admission and firing, outbox persistence and delivery, state commits, and crash recovery,
- a companion crate such as `terracedb-workflows-sandbox` adapts a draft or published sandbox module into a `WorkflowHandler`, and
- a workflow registry or deployment layer resolves draft sessions or published bundles, then starts, stops, or upgrades workflows dynamically at runtime without changing executor semantics.

The sandbox-facing boundary should be treated as a real versioned contract, for example `workflow-task/v1`, rather than as direct guest access to engine internals. The executor may keep richer private transition/effects types internally, but guest code should see only the narrower task/command surface.

If the current executor surface is too compile-time-oriented for dynamic workflow loading, it is reasonable to extend it with a type-erased handler adapter or factory. That is an implementation detail; the important architectural rule is that TypeScript plugs into the existing executor contract rather than replacing it.

Recommended TypeScript authoring surface:

```typescript
export default defineWorkflow({
  name: "payments",

  routeToInstance(entry) {
    return entry.key.accountId
  },

  async handle({ instanceId, state, trigger, ctx }) {
    return {
      newState: nextState(state, trigger),
      outboxEntries: buildOutbox(instanceId, state, trigger, ctx),
      timers: buildTimers(instanceId, state, trigger, ctx),
    }
  },
})
```

Execution-time workflow APIs should remain much narrower than general sandbox APIs. A sandboxed workflow turn should receive:

- the admitted trigger,
- the current state,
- deterministic helpers equivalent to `WorkflowContext`, and
- optional workflow-scoped observability hooks or narrowly scoped reviewed read capabilities.

It should not receive arbitrary database handles, shell access, live package installation, host-disk writes, or unrestricted network access. Side effects should still be expressed declaratively through `WorkflowOutput`, with the Rust executor applying them atomically. This keeps replay and recovery semantics identical between Rust-authored and TypeScript-authored workflows.

For correctness, workflow execution must not depend on mutable guest-global state. The host may cache transpiled modules or sealed bundles, but each `routeToInstance` and `handle` call must behave as a pure function of admitted input, current state, and deterministic context.

---

## Event-Driven Workflows

Workflows do **not** execute directly off `scanSince`. Source events are first admitted into `inboxTable`, and the source cursor is advanced **in the same commit as that admission**. The inbox executor consumes only the durable inbox prefix, so execution begins only after that admission commit itself has reached durability. That is what makes crash/replay safe.

```typescript
async function admitWorkflowEvents(db: DB, def: WorkflowDefinition, sourceTable: Table) {
  const scheduler = def.scheduler!
  let cursor: LogCursor = loadCursor(db, def.name, sourceTable)
  const notifications = db.subscribeDurable(sourceTable)

  async function drainOnePage(): Promise<boolean> {
    let page: ChangeEntry[] = []
    const entries = await db.scanDurableSince(sourceTable, cursor, { limit: BATCH_SIZE })
    for await (const entry of entries) page.push(entry)
    if (page.length === 0) return false

    while (true) {
      const tx = await Transaction.begin(db)
      try {
        let newCursor = cursor
        const newlyReady = new Set<string>()

        for (const entry of page) {
          const instanceId = def.handler.routeToInstance(entry)
          await stageTriggerAdmission(tx, def, instanceId, { kind: "event", entry })
          newlyReady.add(instanceId)
          newCursor = entry.cursor
        }

        tx.write(def.sourceCursorTable, cursorKey(def.name, sourceTable.name), encodeCursor(newCursor))
        await tx.commit({ flush: false })

        cursor = newCursor
        for (const instanceId of newlyReady) {
          scheduler.markReady(instanceId)
        }
        return true
      } catch (e) {
        tx.abort()
        if (!(e instanceof ConflictError)) throw e
      }
    }
  }

  while (await drainOnePage()) {}      // initial restart catch-up
  for await (const _ of notifications) {
    while (await drainOnePage()) {}
  }
}
```

A separate inbox executor then processes admitted rows through the ready-instance scheduler rather than by raw inbox key order:

```typescript
async function runWorkflowInboxExecutor(db: DB, def: WorkflowDefinition) {
  const scheduler = def.scheduler!
  const notifications = db.subscribeDurable(def.inboxTable)
  const inFlightInstances = new Set<string>()

  async function seedReadyInstancesFromDurableInbox(): Promise<boolean> {
    const durableSeq = db.currentDurableSequence()
    let found = false

    for await (const [inboxKey, value] of def.inboxTable.scanAt(
      inboxKey("", 0), inboxKey("￿", MAX_I64), durableSeq
    )) {
      const admitted = deserialize(value) as AdmittedWorkflowTrigger
      scheduler.markReady(admitted.workflowInstance)
      found = true
    }

    return found
  }

  async function processOneReadyInstance(): Promise<boolean> {
    const instanceId = await scheduler.nextReadyInstance()
    if (!instanceId) return false
    if (inFlightInstances.has(instanceId)) return true

    inFlightInstances.add(instanceId)
    try {
      const durableSeq = db.currentDurableSequence()
      const pending = await def.inboxTable.scanAt(
        inboxKey(instanceId, 0), inboxKey(instanceId, MAX_I64), durableSeq, { limit: 2 }
      )

      const rows: Array<{ inboxKey: Key, admitted: AdmittedWorkflowTrigger }> = []
      for await (const [key, value] of pending) {
        rows.push({ inboxKey: key, admitted: deserialize(value) })
      }

      if (rows.length === 0) {
        scheduler.onInstanceYield(instanceId, false)
        return false
      }

      await processWorkflowTrigger(
        db,
        def,
        rows[0].admitted.workflowInstance,
        rows[0].admitted.trigger,
        { inboxKey: rows[0].inboxKey },
      )

      // Do not immediately rescan the durable view after acknowledgement.
      // In deferred-durability modes the delete may be visible but not yet durable,
      // so rescanning could rediscover the just-processed row. Instead, only reuse
      // information proven by the pre-execution durable snapshot (rows.length > 1)
      // and let future durable admissions / restart catch-up rediscover later work.
      scheduler.onInstanceYield(instanceId, rows.length > 1)
      return true
    } finally {
      inFlightInstances.delete(instanceId)
    }
  }

  await seedReadyInstancesFromDurableInbox()      // restart-safe initial seeding

  while (await processOneReadyInstance()) {}
  for await (const _ of notifications) {
    await seedReadyInstancesFromDurableInbox()
    while (await processOneReadyInstance()) {}
  }
}
```

The executor uses **durable-fenced** scans (`scanAt(..., currentDurableSequence())`) so a durable wakeup never causes it to process newer visible-but-not-durable inbox rows. It also preserves the semantic rule that each workflow instance is processed **single-threadedly in durable trigger order**, even if the runtime is later parallelized across different instances.

The routing function (`routeToInstance`) determines which workflow instance handles each event. The library provides the durable ingress and replay infrastructure; the application defines the routing and the state machine. Admission assigns a per-instance `triggerSeq` in the same OCC transaction as inbox insertion, so events, callbacks, and timers all join one ordered trigger stream for that instance once that admission transaction reaches the durable prefix.

Event and timer admission use `tx.commit({ flush: false })` intentionally. Atomicity still comes from one OCC commit; the runtime does **not** need to flush every admitted trigger because authoritative execution consumes only from the durable inbox/timer/event surfaces. **Callback admission is different:** because the caller is about to report success to an external system, it must not acknowledge receipt until the callback's inbox admission has reached durability.

---

## Historical Source Processing

Running a workflow from source history is optional. A workflow source should be configured with:

- a **bootstrap policy** used when no persisted source progress exists yet, and
- a **recovery policy** used when persisted source progress can no longer be resumed, for example after `SnapshotTooOld`.

```typescript
type WorkflowSourceBootstrap =
  | { kind: "beginning" }
  | { kind: "current-durable" }
  | { kind: "checkpoint-or-beginning" }
  | { kind: "checkpoint-or-current-durable" }

type WorkflowSourceRecovery =
  | { kind: "fail-closed" }
  | { kind: "restore-checkpoint" }
  | { kind: "restore-checkpoint-or-fast-forward" }
  | { kind: "replay-from-history" }
  | { kind: "fast-forward-to-current-durable" }

interface WorkflowSourceConfig {
  table: Table
  bootstrap: WorkflowSourceBootstrap
  recovery: WorkflowSourceRecovery
}
```

The key distinction is:

- **bootstrap** answers "what should this workflow do the first time it is attached to a source?"
- **recovery** answers "what should this workflow do if its previously persisted source progress is no longer resumable?"

Bootstrap policies:

- **`beginning`**: start from the earliest retained source history. This is the authoritative historical catch-up mode.
- **`current-durable`**: seed source progress at the table's current durable frontier and admit only future changes. This is a live-only attach mode for workflows that should not process historical backlog.
- **`checkpoint-or-*`**: if a previously exported checkpoint exists, restore it; otherwise fall back to the named bootstrap behavior.

Recovery policies:

- **`fail-closed`**: stop the runtime and surface the history-loss error. This is the safest default.
- **`restore-checkpoint`**: recover workflow-owned state and source progress from a checkpoint, then resume.
- **`restore-checkpoint-or-fast-forward`**: prefer checkpoint restore, but if no checkpoint exists, explicitly skip missing history by advancing source progress to the current durable frontier.
- **`replay-from-history`**: rebuild by replaying source history from an append-only ordered source table.
- **`fast-forward-to-current-durable`**: explicitly skip missing source history and continue from "now." This is intentionally lossy and must be opt-in.

Historical source replay is only sound for **append-only ordered** source tables. Kafka-partitioned Debezium `*_cdc` tables are a good example. Current-state mirror tables are not. More generally, replaying the source table alone is **not** a universal substitute for workflow checkpointing, because workflow correctness may also depend on:

- already admitted inbox rows,
- callback admissions,
- durable timers, and
- outbox entries that have not yet been delivered.

Therefore:

- live-only workflows may use `current-durable` bootstrap and mirror/current-state sources,
- replay-capable workflows should consume append-only ordered sources such as Debezium event-log tables, and
- mixed source/timer/callback workflows should pair historical source replay with workflow checkpoints or trigger journaling rather than assuming the source table alone can reconstruct all workflow state.

Regardless of bootstrap policy, restart always resumes **local durable workflow state first**. Existing inbox rows, timer state, and outbox work are drained before the runtime waits for new source notifications. Bootstrap configuration affects source ingestion only; it does not discard already admitted workflow-local durable work.

---

## Callback-Driven Workflows

Callbacks follow the same durable-admission rule as events, but with one extra requirement: the application must not acknowledge external success until the callback's inbox row is itself durable. The executor still replays from `inboxTable` exactly like other trigger kinds.

```typescript
async function admitWorkflowCallback(db: DB, def: WorkflowDefinition, callback: ExternalResponse) {
  const scheduler = def.scheduler!

  while (true) {
    const tx = await Transaction.begin(db)
    try {
      await stageTriggerAdmission(tx, def, callback.workflowInstance, {
        kind: "callback",
        callbackId: callback.callbackId,
        response: callback,
      })
      await tx.commit()
      scheduler.markReady(callback.workflowInstance)
      return
    } catch (e) {
      tx.abort()
      if (!(e instanceof ConflictError)) throw e
    }
  }
}
```

If the process crashes after callback admission but before execution, the callback is replayed from the durable inbox on restart. Because callback admission uses the flush-on-commit path here, returning success to the external caller implies the inbox row is already within the durable prefix. Duplicate admissions should be deduplicated by a stable callback ID in application state or a small callback-dedupe table; the inbox key itself now carries durable **ordering**, not dedup identity.

---

## Timer-Driven Transitions

Workflows schedule durable timers for timeouts, retries, and scheduled transitions. The timer scanner does **not** execute workflow logic directly. Instead, it admits a self-contained timer trigger into `inboxTable`, and the same inbox executor processes admitted rows:

```typescript
async function workflowTimerLoop(db: DB, def: WorkflowDefinition) {
  const scheduler = def.scheduler!

  setInterval(async () => {
    const durableSeq = db.currentDurableSequence()
    for await (const [key, value] of def.timerScheduleTable.scanAt(
      scheduleKey(0, ""), scheduleKey(now(), "\xff"), durableSeq
    )) {
      const timer = deserialize(value)
      const fireAt = extractFireAtFromScheduleKey(key)

      while (true) {
        const tx = await Transaction.begin(db)
        try {
          await stageTriggerAdmission(tx, def, timer.workflowInstance, {
            kind: "timer",
            timerId: timer.timerId,
            fireAt,
            payload: timer.payload,
          })
          await tx.commit({ flush: false })
          scheduler.markReady(timer.workflowInstance)
          break
        } catch (e) {
          tx.abort()
          if (!(e instanceof ConflictError)) throw e
        }
      }
    }
  }, TICK_INTERVAL)
}
```

The admitted timer inbox row is the durable replay record. `processWorkflowTrigger()` uses the inbox row directly for `trigger.kind === "timer"` and never rereads `timerLookupTable` to rediscover `fireAt`. The timer scanner itself is **durable-fenced** via `scanAt(..., currentDurableSequence())`, so it never admits a merely visible timer row that has not yet reached the durable prefix. If a duplicate inbox row runs after the timer was already fired or canceled, deleting the schedule/lookup rows is a harmless no-op; the handler should treat the trigger as duplicate/canceled and leave state unchanged when appropriate.

**API note:** the admission path above uses the Part 2 OCC helper / transaction wrapper explicitly. The core DB API does not expose a built-in `db.transaction()` primitive.

Example: an order fulfillment workflow schedules a 30-minute timer on order creation. If payment isn't confirmed within 30 minutes, the timer firing is admitted to `inboxTable`; the executor later replays that durable trigger and transitions the workflow to `"cancelled"`.

**Concurrency:** the timer scanner may admit duplicate firings due to overlapping scans, crash/retry, or already-fired/already-canceled rows becoming visible again. That is acceptable because the admitted timer row carries full durable identity (`timerId`, `fireAt`, `payload`) and execution treats duplicate/canceled firings as idempotent no-ops. The uniform guarantee is **at-least-once timer delivery with state-guarded handlers**.

---

## Side-Effect Delivery

Workflows cause external side effects (sending emails, calling APIs, publishing messages) through the **transactional outbox** pattern from Part 2. The workflow handler declares intent; a separate outbox processor delivers:

```typescript
class WorkflowOutboxProcessor {
  async run(db: DB, outboxTable: Table, cursorTable: Table) {
    let cursor: LogCursor = await loadOutboxCursor(db, cursorTable, "workflow-outbox")
    const notifications = db.subscribeDurable(outboxTable)

    async function drainAllAvailable(): Promise<void> {
      while (true) {
        let madeProgress = false
        const entries = await db.scanDurableSince(outboxTable, cursor, { limit: BATCH_SIZE })
        for await (const entry of entries) {
          const outbox = deserialize(entry.value) as OutboxEntry
          await this.deliver(outbox)
          cursor = entry.cursor
          madeProgress = true
        }
        if (!madeProgress) break
        await cursorTable.write("workflow-outbox", encodeCursor(cursor))
      }
    }

    await drainAllAvailable.call(this)
    for await (const _ of notifications) {
      await drainAllAvailable.call(this)
    }
  }

  async deliver(entry: OutboxEntry) {
    await externalService.call(entry.payload, { idempotencyKey: entry.idempotencyKey })
  }
}
```

**Exactly-once for workflow state:** the state transition and outbox write are atomic (same `WriteBatch`). If the process crashes after commit, the state is advanced and the outbox entry exists.

**At-least-once for external effects:** the outbox processor may retry on crash. The idempotency key allows the external system to deduplicate.

---

## Queries, Updates, Visibility, and Upgrades

Not every interaction with a running workflow should become a durable workflow-history event.

The library should support three distinct lanes:

1. **durable admitted triggers** that become part of run history and participate in replay,
2. **read-only queries** that inspect current state or derived visibility without mutating run history, and
3. **update/control requests** that may validate against live state first and become durable only if accepted.

This keeps cheap inspection and validation-style interaction from paying the full history-append cost when no state transition is accepted.

Visibility should also be a first-class workflow product rather than a side effect of raw table inspection. The workflow library should expose dedicated surfaces for:

- list and search over runs,
- describe-style live inspection of one run,
- paginated history retrieval for one run, and
- optional lower-level SQL or raw-table introspection for deeper operator forensics.

The key split is:

- `historyTable` is the authoritative replay record,
- `stateTable` is the fast mutable summary,
- `visibilityTable` is the operator-facing projection.

Raw table access may still exist for debugging or advanced tooling, but common operator paths should not require distributed scans or bespoke joins over raw workflow storage.

Workflow upgrades should also remain explicit. A run should be pinned to an immutable bundle or native registration identity for its execution epoch. New runs may pick newer bundles under rollout policy, but changing code under an already-running execution should require an explicit compatibility decision. The default upgrade boundary should be:

- keep the current run pinned,
- let new runs start on the new bundle,
- and use continue-as-new / restart-as-new style transitions when a running execution must migrate to new code with a clean compatibility boundary.

This applies equally to:

- native Rust workflows registered directly with the runtime, and
- sandbox-authored workflows published as immutable reviewed bundles.

Both should participate in the same deployment and visibility model.

---

## Recovery After Crash

On restart, the workflow library:

1. **Loads persisted durable-ingress cursors** from `sourceCursorTable`. Resumes tailing source tables from where durable admission left off.
2. **Replays unprocessed outbox entries.** Any outbox entries not yet delivered are picked up by the outbox processor. External side effects are retried with their idempotency keys.
3. **Replays admitted inbox rows.** Any entries still present in `inboxTable` are durable pending work left by a prior crash; the inbox executor drains them before waiting for new notifications, processing the lowest pending `triggerSeq` per instance first.
4. **Admits overdue timers.** The timer scanner then scans the durable prefix of `timerScheduleTable` for rows with fire time ≤ now and admits timer triggers for anything that should have fired during downtime.
5. **Workflow state is already consistent.** Because state transitions, outbox writes, timer updates, and inbox acknowledgement are all in the same `WriteBatch`, there is no partial state to reconcile.

The recovery model is simple because all workflow state lives in DB tables and all state transitions are atomic. There is no in-memory state to reconstruct beyond loading durable cursors and replaying durable inbox/outbox work.

If a persisted source cursor is older than the retained source history, recovery follows the source's configured **recovery policy** rather than always implying a single behavior. Some workflows should fail closed; some should restore from checkpoint; some live-only workflows may explicitly fast-forward to the current durable frontier. Historical replay without checkpoints is only appropriate for replayable append-only ordered sources.

---

## Worked Example: Order Fulfillment

A minimal order processing workflow to illustrate the pattern:

```typescript
const orderWorkflow: WorkflowHandler = {
  routeToInstance(entry: ChangeEntry): string {
    return deserialize(entry.value).orderId
  },

  async handle(state: OrderState | null, trigger: WorkflowTrigger, ctx: WorkflowContext): Promise<WorkflowOutput> {
    // New order — schedule a payment timeout timer
    if (!state && trigger.kind === "event") {
      const order = deserialize(trigger.entry.value)
      const timerKey = `payment-timeout:${order.orderId}`
      const createdAt = order.createdAt
      return {
        newState: { status: "awaiting_payment", orderId: order.orderId, created: createdAt, paymentTimerKey: timerKey },
        outboxEntries: [{
          outboxId: `order:${order.orderId}:send_order_confirmation`,
          type: "send_order_confirmation",
          orderId: order.orderId,
          idempotencyKey: `order:${order.orderId}:send_order_confirmation`,
        }],
        timers: [{ action: "schedule", key: timerKey, fireAt: createdAt + 30 * MINUTE, payload: { reason: "payment_timeout" } }],
      }
    }

    // Payment received — cancel the timeout timer explicitly
    if (state?.status === "awaiting_payment" && trigger.kind === "event") {
      const event = deserialize(trigger.entry.value)
      if (event.type === "payment_confirmed") {
        return {
          newState: { ...state, status: "fulfilling" },
          outboxEntries: [{
            outboxId: `order:${state.orderId}:initiate_shipment`,
            type: "initiate_shipment",
            orderId: state.orderId,
            idempotencyKey: `order:${state.orderId}:initiate_shipment`,
          }],
          timers: [{ action: "cancel", key: state.paymentTimerKey }],
        }
      }
    }

    // Payment timeout fired
    if (state?.status === "awaiting_payment" && trigger.kind === "timer") {
      return {
        newState: { ...state, status: "cancelled", reason: "payment_timeout" },
        outboxEntries: [{
          outboxId: `order:${state.orderId}:send_cancellation_email`,
          type: "send_cancellation_email",
          orderId: state.orderId,
          idempotencyKey: `order:${state.orderId}:send_cancellation_email`,
        }],
        timers: [],
      }
    }

    // Default: no change
    return { newState: state!, outboxEntries: [], timers: [] }
  }
}
```

Timers are identified by explicit keys (e.g., `payment-timeout:{orderId}`), stored in the workflow state, and cancelled by key. An empty `timers` array means "no timer changes," not "cancel all timers."

---

# Part 5: Embedded Virtual Filesystem Library

This part describes a separate library, tentatively `terracedb-vfs`, that provides an embedded virtual filesystem on top of Terracedb rather than SQLite. The intended use is narrow: embed a virtual filesystem inside the Rust process, expose it to an embedded program or runtime, and persist its state with the same durability, queryability, and deterministic-testing model as the rest of Terracedb.

The compatibility target is semantic, not storage-format-level. The goal is to preserve the behavior that matters to an in-process embedded sandbox: a POSIX-like virtual filesystem, a small JSON key-value store, tool-run history, point-in-time views, and cheap copy-on-write sandboxes. SQL compatibility, a single-file transport unit, and byte-for-byte reuse of the SQLite schema are explicitly out of scope.

## Goals and Non-Goals

`terracedb-vfs` should preserve the following externally visible properties:

- a POSIX-like virtual filesystem with inode/dentry semantics, root inode `1`, hard links, symlinks, per-inode metadata, and chunked file content,
- immediate read-after-write for current filesystem state inside the same process,
- append-only semantic activity records for filesystem mutations, KV mutations, and tool-run lifecycle events,
- point-in-time snapshots for reproducible reads and durable clone/export flows for long-lived reproduction,
- copy-on-write overlays for per-session sandboxes, and
- portability across Terracedb storage modes instead of a SQLite-specific sync story.

Version 1 is intentionally narrower than the full space of mountable/networked virtual filesystem systems:

- no FUSE, NFS, MCP, HTTP, CLI, or daemon/service boundary,
- no host-filesystem mount surface or kernel-facing inode/handle contract,
- no promise to emulate every open-file-handle nuance needed by OS mount adapters,
- no SQL compatibility or SQLite WAL/file-format compatibility,
- no assumption that portability means `cp volume.db`.

The public surface is therefore path-oriented and embedded-library-first. Internally the implementation still uses inode/dentry tables, but callers do not need a separate mount-oriented API to use the library inside a Rust process.

## Load-Bearing Decisions

These decisions most constrain the design:

- **One Terracedb DB may host many virtual filesystem volumes.** Every reserved table is keyed by `volume_id` first. Deployments that want one volume per DB can still do that by using exactly one volume.
- **Current-state tables are authoritative for reads.** `stat`, `readdir`, `readFile`, KV lookups, and current tool-run status read directly from current-state rows, not from asynchronously maintained projections.
- **Every logical mutation is one OCC unit.** Namespace changes, inode updates, chunk rewrites, whiteout/origin updates, KV changes, tool-run status changes, and activity-row insertion commit together.
- **Activity is append-only and volume-ordered.** Each volume owns a monotonic `activity_id` allocator so scans over `vfs_activity` reproduce a stable semantic order.
- **Snapshots are short-lived; clones are explicit.** Request-scoped snapshots use MVCC. Long-lived reproduction uses clone/export data, not unbounded snapshot pinning.
- **Overlay bases are explicit read-only virtual filesystem cuts.** Version 1 overlays sit on top of another read-only virtual filesystem snapshot or clone, not an arbitrary host filesystem adapter.
- **`fsync` remains meaningful.** Visible state may lead durable state in deferred modes; `fsync` and volume `flush()` are explicit durability fences over Terracedb `flush()`.

## Public Surface

The library exposes one embedded API surface:

```typescript
interface VolumeConfig {
  volumeId: string
  chunkSize?: number          // immutable after volume creation
  createIfMissing?: boolean
}

interface VolumeStore {
  openVolume(config: VolumeConfig): Promise<Volume>
  cloneVolume(source: { volumeId: string, durable?: boolean }, target: VolumeConfig): Promise<Volume>
  createOverlay(base: VolumeSnapshot, target: VolumeConfig): Promise<OverlayVolume>
}

interface Volume {
  fs: VfsFileSystem
  kv: VfsKvStore
  tools: ToolRunStore

  snapshot(opts?: { durable?: boolean }): Promise<VolumeSnapshot>
  activitySince(cursor: LogCursor, opts?: { durable?: boolean }): Promise<AsyncIterator<ActivityEntry>>
  subscribeActivity(opts?: { durable?: boolean }): Receiver<SequenceNumber>

  flush(): Promise<void>
}

interface OverlayVolume extends Volume {
  base: VolumeSnapshot
}

interface VolumeSnapshot {
  sequence: SequenceNumber
  fs: ReadOnlyVfsFileSystem
  kv: ReadOnlyVfsKvStore
  tools: ReadOnlyToolRunStore
}

interface VfsFileSystem {
  stat(path: string): Promise<Stats | null>
  lstat(path: string): Promise<Stats | null>
  readFile(path: string): Promise<bytes | null>
  pread(path: string, offset: u64, len: u64): Promise<bytes | null>
  writeFile(path: string, data: bytes, opts?: CreateOptions): Promise<void>
  pwrite(path: string, offset: u64, data: bytes): Promise<void>
  truncate(path: string, size: u64): Promise<void>
  mkdir(path: string, opts?: MkdirOptions): Promise<void>
  readdir(path: string): Promise<DirEntry[]>
  readdirPlus(path: string): Promise<DirEntryPlus[]>
  rename(from: string, to: string): Promise<void>
  link(from: string, to: string): Promise<void>
  symlink(target: string, linkpath: string): Promise<void>
  readlink(path: string): Promise<string>
  unlink(path: string): Promise<void>
  rmdir(path: string): Promise<void>
  fsync(path?: string): Promise<void>
}

interface VfsKvStore {
  get<T>(key: string): Promise<T | null>
  set<T>(key: string, value: T): Promise<void>
  delete(key: string): Promise<void>
  listKeys(): Promise<string[]>
}

interface ToolRunStore {
  start(name: string, params?: Json): Promise<ToolRunId>
  success(id: ToolRunId, result?: Json): Promise<void>
  error(id: ToolRunId, error: string): Promise<void>
  recordCompleted(input: CompletedToolRun): Promise<ToolRunId>
  get(id: ToolRunId): Promise<ToolRun | null>
  recent(limit?: number): Promise<ToolRun[]>
}
```

This surface is intentionally enough to embed a virtual filesystem in-process and hand a constrained capability to an embedded runtime. It does not try to be a kernel-facing mount API.

## Volume Model and Reserved Tables

Version 1 should use row tables only. Filesystem operations are dominated by point lookups and prefix scans over small keys, while file content is naturally represented as fixed-size blob chunks.

All reserved keys begin with `volume_id`, which lets one DB host many isolated volumes without engine changes.

### Current-State Tables

| Table | Key shape | Purpose |
|---|---|---|
| `vfs_volume` | `(volume_id)` | Volume metadata: immutable `chunk_size`, format version, creation time, root inode, optional overlay base descriptor |
| `vfs_allocator` | `(volume_id, kind)` | Persisted high-water marks for `ino`, `activity_id`, and `tool_run_id` block leasing |
| `vfs_inode` | `(volume_id, ino)` | Current inode metadata: `mode`, `nlink`, `uid`, `gid`, `size`, timestamps, nanoseconds, `rdev` |
| `vfs_dentry` | `(volume_id, parent_ino, name)` | Directory entry mapping from parent/name to child inode |
| `vfs_chunk` | `(volume_id, ino, chunk_index)` | Current file bytes in fixed-size chunks |
| `vfs_symlink` | `(volume_id, ino)` | Symlink target text |
| `vfs_kv` | `(volume_id, key)` | Current JSON-serialized KV entries |
| `vfs_tool_run` | `(volume_id, tool_run_id)` | Current tool-run row (`pending`, `success`, `error`, timestamps, params/result/error) |
| `vfs_whiteout` | `(volume_id, path)` | Overlay whiteouts keyed by normalized path |
| `vfs_origin` | `(volume_id, delta_ino)` | Copy-up provenance: which base volume/snapshot/inode a delta inode originated from |

Notes:

- `scanPrefix((volume_id, parent_ino))` over `vfs_dentry` is the primitive behind `readdir`.
- `scan((volume_id, ino, first_chunk), (volume_id, ino, last_chunk + 1))` over `vfs_chunk` is the primitive behind `pread`.
- root inode is always `1` inside a volume,
- empty files have an inode row and no chunk rows.

### Append-Only and Derived Tables

| Table | Key shape | Purpose |
|---|---|---|
| `vfs_activity` | `(volume_id, activity_id)` | Append-only semantic audit stream for filesystem, KV, and tool mutations |
| `vfs_tool_stats` | projection-owned | Optional derived per-tool counters and durations |
| `vfs_volume_usage` | projection-owned | Optional derived file/dir/chunk counts and logical byte usage |

`vfs_activity` is the authoritative history source for timelines and analytics. Projection tables are disposable read models rebuilt from it. Current-state tables such as `vfs_inode` and `vfs_chunk` must not be treated as historical truth after MVCC GC.

## Filesystem Semantics

### Snapshot-Consistent Reads

Every multi-step read operation runs against one Terracedb snapshot:

- `stat(path)` resolves the path and loads the inode in one cut,
- `readdirPlus(path)` resolves the directory and reads child dentries/inodes in one cut,
- `readFile(path)` resolves the inode and scans chunk rows in one cut.

This avoids mixed-version results when concurrent writers modify the same directory or file.

### Namespace and Metadata Mutations

Every logical filesystem operation is one OCC transaction:

1. take a snapshot,
2. resolve the affected path(s) and parent directories in that snapshot,
3. add the relevant read dependencies,
4. stage all current-state row changes plus one semantic `vfs_activity` row,
5. commit once.

This same pattern applies to `mkdir`, `writeFile`, `rename`, `unlink`, `rmdir`, `link`, `symlink`, `truncate`, and `pwrite`. Current state and audit history therefore become visible together.

### Chunked File I/O and Durability

File content lives in `vfs_chunk` as fixed-size chunks. `chunk_size` is immutable per volume and chosen at volume creation time.

Design notes:

- small random writes rewrite only the touched chunk rows plus the inode row,
- `truncate` that shrinks a file deletes chunks above the new EOF and trims the final partial chunk in the same commit,
- writes past EOF create logical holes that read back as zeroes without requiring eager gap materialization,
- chunk size is a volume policy, not an engine global.

Ordinary mutations follow the DB's configured visibility rules. `fsync(path?)` and `volume.flush()` are explicit durability fences over Terracedb `flush()` and durable-sequence tracking.

## Key-Value and Tool Services

The embedded library keeps the same three high-level surfaces that make a virtual filesystem useful to embedded runtimes: filesystem state, KV state, and tool-run history.

### Key-Value State

`vfs_kv` is a simple current-state table:

- `set(key, value)` writes or overwrites the current JSON value and appends `kv_set` activity,
- `delete(key)` removes the current row and appends `kv_delete` activity,
- `listKeys()` is a prefix scan over `(volume_id, *)`.

### Tool-Run Tracking

The tool surface supports both common patterns:

1. a two-step lifecycle where `start()` creates a pending run and `success()` or `error()` finalizes it,
2. a one-shot `recordCompleted()` path for callers that already have the terminal record.

`vfs_tool_run` stores current state. `vfs_activity` stores the immutable audit trail (`tool_started`, `tool_succeeded`, `tool_failed`) that later projections and debugging tools consume.

## Snapshots, Clones, and Overlays

### Point-in-Time Views

`VolumeSnapshot` is the lightweight "show me the exact state at a cut" mechanism. It is appropriate for request-scoped inspection, debugger-style reads, consistent multi-step operations, and one-shot exports.

Because engine snapshots pin MVCC GC, callers should treat `VolumeSnapshot` like any other short-lived database snapshot.

### Durable Cloning and Export

Long-lived reproduction is done by copying data, not by pinning snapshots indefinitely.

Two deployment styles are expected:

1. **one volume per DB**: use DB-level backup, restore, clone, or checkpoint flows,
2. **many volumes per DB**: export or clone the reserved `vfs_*` keyspace for one `volume_id`.

This replaces the SQLite-era "copy the database file" story with a Terracedb-native equivalent.

### Embedded Copy-on-Write Overlays

Overlay mode is modeled as a writable delta volume in front of a read-only virtual filesystem snapshot.

Lookup order is:

1. if the path exists in the delta volume, return the delta entry,
2. else if the path is whiteouted in `vfs_whiteout`, return not found,
3. else if the path exists in the base snapshot, return the base entry,
4. else return not found.

Directory listing merges delta dentries with base dentries and excludes whiteouted names.

On first mutation of a base-resident file or directory, the overlay performs copy-up into the delta volume. `vfs_origin` records which base entry was copied so later mutations and debugging can reason about provenance. Because version 1 is embedded-only and path-oriented, this provenance exists for correctness and observability, not to promise stable kernel-facing inode numbers.

## Queryability and Watchers

Terracedb-backed virtual filesystem support preserves queryability, but the surface is explicit:

- current state is inspected through direct library reads and reserved-table scans,
- history and recent activity come from `vfs_activity`,
- analytics such as per-tool stats or volume usage are projections over `vfs_activity`.

The activity tail should expose both visible and durable variants:

- visible for low-latency local follow modes,
- durable for authoritative export, audit, or workflow triggers.

Because `vfs_activity` is an ordinary Terracedb table, the projection and workflow machinery from Parts 3 and 4 can be reused above the filesystem without coupling filesystem correctness to those async consumers.

## Storage Modes and Deterministic Testing

The embedded virtual filesystem layer does not need its own replication or sync protocol. It inherits the enclosing DB's storage mode:

- in tiered mode, current-state rows and activity rows behave like any other Terracedb tables, with cold SSTables offloaded automatically,
- in s3-primary or deferred-durability configurations, visible state may lead durable state until flush boundaries.

This library should inherit the same deterministic testing bar as the rest of the stack. The simulation target is the real filesystem/KV/tool/overlay code. The shadow model needs to cover directories, inode link counts, file bytes, whiteouts, origin mappings, KV state, tool-run lifecycle state, and activity-prefix correctness across crash and restart.

---

## Embedded Sandbox Runtime Library

This section describes a separate library, tentatively `terracedb-sandbox`, that embeds an AI-oriented JavaScript/TypeScript runtime on top of `terracedb-vfs`. The goal is to let guest code operate inside the application against a TerraceDB-backed virtual tree, optional injected app capabilities, sandboxed shell tooling, TypeScript services, package installation, host-disk hoist/eject flows, and git/PR workflows without turning the engine itself into a general host runtime.

The layering matters:

- the engine remains unaware of sandboxes,
- `terracedb-vfs` remains the authoritative filesystem/KV/tool substrate,
- `terracedb-sandbox` adds runtime, compatibility, disk/git/editor interop, and capability injection on top of that substrate.

### Goals and Non-Goals

`terracedb-sandbox` should preserve the following externally visible properties:

- an embedded guest runtime whose root filesystem is a `terracedb-vfs` overlay or volume rather than the host disk,
- optional injected host capabilities exposed as explicit importable modules with matching TypeScript declarations,
- shell-style utility execution via a guest-visible library surface rather than ambient host subprocess access,
- TypeScript execution and `tsc`-style diagnostics against the same virtual tree,
- package-install workflows for a useful subset of the npm ecosystem,
- hoist of a real directory or git repo into a sandbox session,
- eject of a sandbox snapshot or delta back onto disk,
- easy pull-request creation from sandboxed code changes, and
- an easy read-only way to inspect sandbox state from VS Code and Cursor.

Version 1 is intentionally narrower than the full space of JavaScript runtimes and workstation integrations:

- no engine-level sandbox mode,
- no promise to replicate the full Deno CLI, Node.js host process model, or unrestricted npm ecosystem,
- no ambient host filesystem, subprocess, or FFI access,
- no requirement that sandboxes become writable kernel mounts,
- no requirement that editor visibility imply a writable host filesystem bridge, and
- no assumption that every guest-runtime component can run inside the deterministic simulation harness unchanged.

### Load-Bearing Decisions

These decisions most constrain the sandbox design:

- **A sandbox session is a `terracedb-vfs` overlay volume.** Sandboxes are not ad hoc temp directories; they inherit the virtual filesystem's snapshot, overlay, activity, KV, and tool-run semantics.
- **Execution, TypeScript tooling, package installation, disk/git interop, and editor visibility are separate subsystems sharing one virtual tree.** They should not collapse into one giant runtime abstraction.
- **Guest-runtime compatibility is layered above a small VFS-backed op surface.** The Rust layer should expose a narrow host op set over `terracedb-vfs`; Node or Deno compatibility shims should mostly live in guest-side libraries.
- **Host integration is capability-first.** Application APIs are explicit versioned modules, not ambient globals.
- **Sandbox workloads should run in explicit execution domains.** Capability policy controls what guest code may do; execution domains control what CPU, memory, local I/O, remote I/O, and background capacity it may consume.
- **Tool-like actions are first-class audited events.** `bash`, package install, type-check, hoist/eject, PR export, and host-capability calls should flow through the tool-run and activity model rather than bypassing it.
- **Package installation is a host service, not a literal embedded npm CLI.** A useful pure-JS/TS subset comes first; native addons, arbitrary postinstall scripts, and full host-process expectations are deferred.
- **Host-disk, git, PR, and editor interop are explicit library services.** They are not side effects of exposing the sandbox as a writable real filesystem.
- **If part of the sandbox stack cannot run deterministically under simulation, it must be hidden behind a stable interface with a deterministic stub/fake implementation.** The real integration may remain outside turmoil, but the core semantics should still be exercised under seeded replay.

### Public Surface

The library should expose an embedded API surface oriented around sessions, execution, interop, and read-only views:

```typescript
interface SandboxConfig {
  baseVolumeId: string
  sessionVolumeId: string
  durableBase?: boolean
  npmMode?: "off" | "pure_js" | "compat_view"
  nodeCompat?: "off" | "subset"
  bash?: "off" | "enabled"
  typescript?: "off" | "transpile_only" | "full"
  capabilities?: CapabilityManifest
  execution?: SandboxExecutionPolicy
}

interface SandboxExecutionPolicy {
  sessionDomain?: string
  toolDomain?: string
  typescriptDomain?: string
  packageDomain?: string
  exportDomain?: string
  controlPlaneDomain?: string
}

interface SandboxStore {
  openSession(config: SandboxConfig): Promise<SandboxSession>
}

interface SandboxSession {
  volume: OverlayVolume

  eval(code: string): Promise<ExecOutcome>
  execModule(specifier: string): Promise<ExecOutcome>
  installPackages(reqs: PackageReq[]): Promise<InstallReport>
  checkTypes(roots: string[]): Promise<TypeCheckReport>

  hoistFromDisk(req: HoistRequest): Promise<HoistReport>
  ejectToDisk(req: EjectRequest): Promise<EjectReport>
  createPullRequest(req: PullRequestRequest): Promise<PullRequestReport>

  openReadonlyView(req?: ReadonlyViewRequest): Promise<ReadonlyViewHandle>
  flush(): Promise<void>
}

interface ReadonlyViewHandle {
  viewId: string
  rootUri: string               // for example terrace-vfs:/session/<id>/
  close(): Promise<void>
}
```

The public surface is intentionally library-first. It is enough to embed the sandbox inside an application, hand bounded capabilities to guest code, import/export work from real repos, and expose a read-only editor/browser view without promising a general-purpose mount or daemon boundary.

### Session Model

Each sandbox session should be backed by:

- a base `Volume` or `VolumeSnapshot` containing workspace files, templates, and shared cache state,
- a writable overlay `OverlayVolume` for the live guest session, and
- optional shared read-only cache volumes for unpacked packages, tarballs, transpile caches, or TypeScript standard-library files.

Recommended session layout:

```text
/workspace/                 guest-visible project root
/.terrace/
  session.json             session metadata and origin/provenance
  cache/
    v8/                    code cache blobs
    transpile/             transpile cache metadata
  npm/
    package.json
    install-manifest.json
    node_modules/          optional compatibility view
  typescript/
    libs/
  tools/
    bash/
```

This layout keeps the session self-describing and makes it possible to reopen, export, or inspect the sandbox without inventing hidden side channels.

### Host Filesystem and Git Interop

The sandbox should support three explicit hoist modes:

1. **directory snapshot** — import a host directory tree into the sandbox,
2. **git head** — import the tracked contents of a repo at `HEAD`,
3. **git working tree** — import the current working tree, optionally including untracked or ignored files.

Each hoist should record provenance in session metadata and/or VFS KV:

- source path,
- repo root if applicable,
- `HEAD` commit,
- branch,
- remote URL,
- hoist mode,
- included pathspecs,
- whether the imported source was dirty.

The sandbox should support two explicit eject modes:

1. **materialize snapshot** — write the current visible sandbox tree to a target directory,
2. **apply delta** — apply only changes relative to the recorded hoist base.

`apply delta` should not proceed blindly if the target checkout has diverged from the stored provenance. It should either fail with a conflict report, emit a patch bundle, or apply only non-conflicting changes if the caller opts into that weaker behavior.

For repos, `git head` should be the safest default import mode. `git working tree` is useful, but it should be explicit because it complicates later conflict handling and PR export.

These host-facing hoist and eject flows are part of the intended architecture, but they are boundary adapters rather than the final location of git semantics. The planned git architecture is described later in this part under **VFS-Native Git Library**.

### Runtime Model

The low-level execution surface should be a pure-Rust JavaScript engine, currently `boa_engine`, with custom module loading and host integration. Full Deno CLI behavior and the unrestricted Node host process model are not the design center for version 1.

Choosing a pure-Rust engine avoids the non-Rust V8 embedding stack and keeps the sandbox easier to build, test, and simulate inside the rest of the Rust codebase. Each sandbox instance should still behave like a logically serialized actor with message-passing from the surrounding app so module state, caches, and capability calls stay predictable.

The detailed target architecture for this layer is described later in this part under **Simulation-Native JavaScript Runtime**.

The runtime phases are:

1. open or create the overlay session volume,
2. build the capability manifest,
3. construct the sandbox actor and runtime backend,
4. initialize guest-side libraries for filesystem shims, capabilities, and optional bash/npm/TypeScript helpers,
5. evaluate the requested code or module graph, and
6. publish tool/activity metadata and optionally flush durable state.

### Module Resolution and Loading

The sandbox should implement a custom module loader with three jobs:

1. resolve specifiers,
2. prepare dependencies and caches,
3. load final source plus source maps and code-cache metadata.

Recommended specifier space:

- `terrace:/workspace/...`
  Guest-visible project files stored in the session volume.
- `terrace:host/<capability>`
  Host-provided capability modules generated from the capability manifest.
- `npm:<pkg>` and optionally bare package imports
  Package dependencies resolved by the sandbox package installer.
- `node:<builtin>`
  Optional and gated by node-compat mode.

The loader's `prepareLoad` equivalent is where the sandbox should resolve packages, transpile TypeScript, prefetch dependency graphs, and populate code-cache keys before the runtime requests the final source.

### Filesystem API Strategy

The Rust layer should expose a narrow host-op surface over `terracedb-vfs`:

- `read_file`,
- `write_file`,
- `pread`,
- `pwrite`,
- `stat`,
- `lstat`,
- `readdir`,
- `readlink`,
- `mkdir`,
- `rename`,
- `link`,
- `symlink`,
- `unlink`,
- `rmdir`,
- `truncate`,
- `fsync`.

Guest-facing compatibility should then be layered in libraries:

- a runtime-neutral `@terracedb/sandbox/fs`,
- a small sandbox runtime/global shim for the APIs guest code actually needs,
- `node:fs/promises` shims built on the same op surface,
- selected sync compatibility layers only where a concrete package requires them.

This keeps the Rust surface small and lets compatibility grow without rewriting the storage substrate.

### Shell Tooling via just-bash

`just-bash` fits the sandbox well because it is AI-oriented and already exposes an async filesystem interface. The recommended integration is:

1. provide a guest-side adapter that implements `just-bash`'s filesystem interface on top of `@terracedb/sandbox/fs`,
2. expose a library such as `@terracedb/sandbox/bash`,
3. wrap `just-bash` in a persistent session helper that remembers cwd and exported environment across calls.

Each bash invocation should still be recorded as a tool run and use the same VFS tree as the guest runtime. Good candidates for built-in custom commands are:

- `npm`
  Delegates to the host package installer service.
- `tsc`
  Delegates to the TypeScript service.
- `terrace-call`
  Invokes allowlisted host capabilities.

### TypeScript Strategy

TypeScript needs two distinct stories:

1. execution-time transpilation for guest modules,
2. language-service and `tsc`-style analysis.

These should share a module graph and virtual tree, but they should not be the same implementation.

Execution-time transpilation should happen before the guest runtime evaluates a `.ts`, `.tsx`, `.mts`, or `.cts` module. The resulting JS and source maps should be cached with keys that include:

- file content hash,
- compiler target,
- JSX mode,
- module kind, and
- relevant package-resolution settings.

For language-service behavior, `@typescript/vfs` is a good fit because it supplies a `Map`-backed `ts.System`/compiler-host model. The sandbox should maintain a TypeScript mirror derived from the current volume view or a snapshot and update it incrementally from VFS activity instead of rebuilding it from scratch after every edit.

### npm Dependency Strategy

Package installation is the highest-risk compatibility area. The key constraint is that the JS engine and module-loader surface give the embedder language execution plus resolution hooks, but broader npm and Node compatibility lives above that layer.

Version 1 should support:

- pure JS/TS packages,
- ESM-first packages,
- packages that only require the explicitly supported host shims, and
- installation through a host-managed package service.

Version 1 should explicitly defer:

- native Node-API addons,
- arbitrary postinstall scripts,
- packages that require unrestricted host-process behavior, and
- the full long tail of CommonJS or `node_modules`-layout assumptions.

The package installer should be a host service that:

1. resolves versions,
2. fetches and verifies tarballs,
3. unpacks into a shared cache or session-local cache,
4. materializes the compatibility view the sandbox loader expects, and
5. records install metadata in the session plus tool/activity streams.

### Host Capability Injection

Application APIs should be explicit capability modules with both runtime and type surfaces. Each capability should define:

- a stable module specifier,
- TypeScript declaration text,
- one or more host ops or backend hooks, and
- an allowlist entry in the session capability manifest.

Guest code should import capabilities explicitly:

```typescript
import { tickets } from "terrace:host/tickets"

const open = await tickets.listOpen()
await tickets.addComment({ id: open[0].id, body: "Investigating" })
```

The sandbox should prefer imports over globals, small domain-specific modules over one giant `Terrace` object, async JSON-serializable calls over opaque handles, and idempotent write APIs where possible.

### Shell-Facing Capability Bridge

Typed imports should remain the primary programmatic surface for guest code, but the same host-owned capabilities should also be exposable through a shell-friendly bridge for `just-bash` and related authoring tools.

The important rule is that this bridge must not become a second API stack. It should be generated or derived from the same capability templates and procedure metadata already used for typed imports. The shell form is a convenience surface over the same authority model, not a separate integration plane.

Useful properties for this bridge:

- self-describing commands with built-in `--help`, argument descriptions, and examples,
- structured JSON input/output so shell steps can compose with TypeScript and tooling code,
- stable naming derived from capability bindings or published procedure names,
- the same manifest enforcement, budgets, and audit labels as direct imported calls, and
- tool-run/activity recording that makes shell-driven capability use first-class in session history.

For example, a sandbox might expose:

- a typed import such as `import { orders } from "terrace:host/orders_ro"`, and
- a shell-facing command such as `terrace-call orders_ro get --json '{"id":"..."}'`

both backed by the same capability binding.

### Capability and Permission Model

The sandbox should treat injected APIs as a first-class domain model rather than as an ad hoc bag of permissions. The important distinction is:

- a **capability template** defines what API shape exists,
- a **capability grant** defines which subject may use that API against which resources and budgets, and
- a **capability manifest** is the concrete set of bound modules injected into one sandbox session or one published procedure.

Recommended model:

```typescript
interface CapabilityTemplate {
  id: string
  version: string
  methods: string[]
  dtsText: string
}

interface ResourcePolicy {
  dbs?: string[]
  tables?: string[]
  keyPrefixes?: string[]
  tenantScope?: "none" | "caller" | { fixed: string }
  queryShapes?: Array<"point_read" | "prefix_scan" | "range_scan" | "index_query" | "write" | "schema">
}

interface BudgetPolicy {
  timeoutMs: number
  maxRows?: number
  maxBytes?: number
  rateLimitBucket?: string
}

interface CapabilityGrant {
  binding: string
  templateId: string
  authMode: "caller" | "service"
  resourcePolicy: ResourcePolicy
  budgets: BudgetPolicy
  auditLabel: string
}

interface CapabilityManifest {
  subjectId: string
  bindings: CapabilityGrant[]
}
```

This model should remain host-authoritative. Guest code may request a capability by import specifier, but it must not be able to mint new grants or widen its own scope from inside the sandbox.

The recommended import shape is one injected module per binding:

```typescript
import { orders } from "terrace:host/orders_ro"
import { catalog } from "terrace:host/catalog_admin"
```

The effective manifest for a draft session should be derived from the intersection of:

- the subject's standing grants,
- any environment or deployment policy, and
- the capabilities explicitly requested for that session.

The effective manifest for a published procedure should be the reviewed immutable manifest attached to that procedure version, optionally intersected with deployment policy at invocation time. This keeps review and runtime enforcement aligned while preventing code from self-escalating after publication.

### Capability Presets and Policy Profiles

The underlying capability/grant model should remain explicit and expressive, but normal users should not have to assemble every draft-session manifest from low-level bindings by hand. The architecture should therefore support named capability presets or policy profiles layered on top of the raw grant model.

Examples of useful presets:

- `query_ro`
  Read-only draft query access for trusted internal users.
- `repo_author`
  Authoring flows that need sandbox editing, package install, bash, view, and export support.
- `workflow_preview`
  Draft workflow authoring and preview with workflow-specific helper capabilities.
- `catalog_migrate`
  Narrow reviewed migration authority over catalog/schema operations.
- `procedure_publish`
  Review and publication authority without broad draft-query access.

These presets should be treated as host-authored convenience bundles rather than hidden magic. The host should still be able to:

- inspect the expanded effective manifest,
- override or narrow parts of a preset,
- attach execution-domain mappings and budget defaults,
- persist both the chosen preset name and the expanded manifest in audit metadata.

The goal is not to replace the grant model; it is to give applications a humane UX layer over it.

### Interactive Authorization for Draft Sessions

Trusted draft sessions may benefit from an optional interactive authorization flow, but it must fit the fact that unavailable capabilities are normally omitted from the sandbox entirely rather than injected and then denied at call time. This can improve the foreground authoring experience without weakening the host-authoritative permission model.

Recommended rules:

- only trusted draft/internal sessions may use interactive authorization,
- the host, not the guest, decides whether to surface a permission request,
- interactive authorization should primarily apply either to operations that reached host enforcement and were denied there, or to an explicit host-mediated request surface for adding a new binding to the draft session,
- approval may grant authority for one call, one draft session, or a host-defined policy update,
- every approval or rejection should be recorded in the same audit/tool-run history as the attempted operation,
- published procedures, reviewed workflow bundles, and lower-trust production paths must not request new authority at runtime.

In other words, the sandbox may ask, but it may not self-escalate. The host remains authoritative over grants, manifests, and policy changes.

### Database Access Capability Families

Database access should be exposed through a small set of versioned capability families rather than by handing guest code raw `Db` or `Table` objects. The host must stay in the enforcement path for every database operation so it can apply policy consistently.

Useful starter families are:

- `db.table.v1`
  Fixed-table access with methods such as `get`, `put`, `delete`, and `scanPrefix`.
- `db.query.v1`
  Broader query access for trusted internal subjects that need to choose among multiple tables or indexes dynamically.
- `catalog.migrate.v1`
  Catalog and schema-change helpers such as `ensureTable`, `installSchemaSuccessor`, `updateTableMetadata`, and precondition checks.
- `procedure.invoke.v1`
  Invocation of already-published reviewed procedures without exposing underlying table access directly.

This is the layer that should own:

- table allowlists,
- tenant scoping,
- query-shape restrictions,
- row or key-prefix filters,
- rate limits, and
- audit logging.

For example, a subject may receive a `db.table.v1` binding scoped to:

- one database,
- one or more specific tables,
- a fixed tenant or caller-derived tenant,
- a limited set of query shapes such as point reads and prefix scans only, and
- per-binding timeout and row-count budgets.

Even when a subject is trusted enough to run arbitrary draft query code, that code should still call versioned host capabilities such as `db.query.v1` rather than touching engine objects directly. This preserves one policy enforcement surface for employee sandboxes, reviewed procedures, and external adapters.

### Row-Level Permissions Above the Query Capability Layer

Row-level permissions should remain a capability-layer concern rather than an engine feature. The engine continues to offer tables, snapshots, scans, and OCC commits; the host-side capability layer decides which rows a caller may observe or mutate. This keeps the enforcement point aligned with the existing capability model instead of introducing a second permission system below the sandbox/runtime stack.

The recommended shape is an explicit caller context plus a versioned row-scope policy attached to a binding:

```typescript
interface PolicyContext {
  subjectId: string
  tenantId?: string
  groupIds?: string[]
  attributes?: Record<string, JsonValue>
}

type RowScopePolicy =
  | { kind: "key_prefix", prefixTemplate: KeyTemplate }
  | { kind: "typed_row_predicate", predicate: RowPredicateRef }
  | { kind: "visibility_index", indexTable: string, subjectKey: "subject" | "tenant" | "group" }
  | { kind: "procedure_only" }
```

The important rule is that the host resolves `PolicyContext` and `RowScopePolicy`; guest code may invoke the bound capability, but it must not be able to choose a broader row scope from inside the sandbox.

Recommended version-1 policy kinds:

- **key-prefix scope**
  Best fit for Terracedb's native primitives. A binding may rewrite or validate keys and prefixes so the guest can only touch rows under a deterministic prefix such as `tenant/{tenant_id}/...` or `user/{subject_id}/...`.
- **typed row predicate**
  A host-owned, deterministic predicate over decoded row values plus `PolicyContext`, for example `row.tenant_id == ctx.tenantId` or `row.owner_id == ctx.subjectId`. This is useful when the row key alone is not sufficient, but it should remain limited to query shapes the host can bound and evaluate predictably.
- **visibility index**
  For ACL-like sharing, collaborative documents, or relationship-driven visibility, request-time filtering should generally not degenerate into unbounded scans plus ad hoc row inspection. Instead, the application should read through projected visibility tables such as `visible_by_subject/{subject_id}/{row_id}` or `visible_by_group/{group_id}/{row_id}` and then fetch the referenced rows.
- **procedure only**
  Some access patterns are not safely expressible as direct draft-query bindings, for example complex multi-table joins, aggregates that would leak existence, or queries whose safe plan depends on application-specific reasoning. Those should fail closed at the raw capability layer and be exposed only through reviewed procedures.

Read-path rules should remain explicit:

- Point reads may use key-prefix validation, key rewriting, or read-then-predicate-check depending on the policy kind.
- Prefix scans are acceptable when the host can derive a bounded candidate set from the policy, for example a scoped primary-key prefix or a projected visibility index prefix.
- Queries that would require an unbounded scan of unrelated rows should be denied by `db.query.v1` rather than silently running an expensive post-filtered plan.
- Budgets should charge for candidate rows scanned as well as rows returned, so a heavily filtered query cannot hide a pathological access pattern behind a small result set.
- Where low-leakage behavior matters, "not visible" should normally surface as "not found" or "not present in scan output" rather than as a distinct permission error per row.

Write-path rules should also be host-enforced and OCC-compatible:

1. Read the current row, if any, at a snapshot.
2. Evaluate the caller's permission against both the current row and the candidate next row.
3. Reject scope-escaping writes such as changing `tenant_id`, `owner_id`, or key prefixes in ways the binding would not have allowed as a fresh insert.
4. Add the preimage to the read set and commit through the normal OCC path.

This preserves one correctness model: row-level permissions change which reads and writes are admitted, not how commit ordering, durability, or conflict detection work.

#### Projection Helpers for Visibility Indexes

ACL-like row visibility should be supported by reusable projection helpers rather than forcing every application to hand-roll the same pattern. A helper in `terracedb-projections` should be able to materialize visibility indexes such as:

- `visible_by_subject/{subject_id}/{row_id} -> ()`
- `visible_by_tenant/{tenant_id}/{row_id} -> ()`
- `visible_by_group/{group_id}/{row_id} -> ()`

The helper should own:

- deterministic expansion from source rows and companion membership state into index rows,
- membership transitions such as `false -> true` and `true -> false`,
- optional emission of narrowed or redacted read mirrors alongside the membership index, and
- explicit rebuild/recompute rules from declared authoritative sources.

This keeps request-time authorization simple:

- direct `db.table.v1` bindings for straightforward tenant- or owner-scoped tables,
- `db.query.v1` over projected visibility indexes for relationship-driven sharing, and
- reviewed procedures for queries that still cannot be bounded safely.

The design goal is the same as the Debezium filtering model elsewhere in this architecture: row filtering should be deterministic over declared context and structured row data, not ambient application callbacks or hidden side channels. That makes row-scope behavior auditable, replayable in simulation, and reusable across draft sandboxes, published procedures, and MCP-facing access.

### Execution Domains for Sandbox Workloads

Capability policy and execution-domain policy should be treated as complementary but separate:

- capabilities answer "what may this code touch?",
- execution domains answer "how much of the process may this workload consume while doing it?"

Sandbox execution should reuse the engine's existing execution-domain model rather than introducing a second resource-isolation mechanism. This is how the host prevents sandbox, procedure, or MCP activity from exhausting process resources and degrading the enclosing application.

Recommended sandbox execution policy:

- draft employee query sessions run in a bounded interactive sandbox domain,
- draft workflow preview sessions run in a bounded workflow-preview domain so preview traffic cannot exhaust either foreground query lanes or the core workflow executor,
- published procedure invocations run in a request-oriented domain with tighter CPU, memory, and row/byte budgets,
- deployed published workflow handlers run in workflow execution domains chosen by deployment policy, with per-trigger budgets tighter than authoring sandboxes and isolated from the application's primary request paths,
- package install, type-check, bash, export, and other heavier helper flows may run in sibling sandbox background domains,
- publication, catalog updates, procedure registry writes, and auth bookkeeping stay in protected control-plane domains,
- read-only editor and MCP inspection traffic may use their own low-priority domains so observability traffic cannot starve foreground app work.

Important rules:

- guest code must never choose or mutate its own execution domain,
- the host chooses domains based on subject, operation kind, and deployment policy,
- per-capability budgets such as timeout and row limits apply in addition to domain budgets rather than replacing them, and
- moving sandbox work between domains may change latency or throughput, but must not change correctness semantics.

Illustrative mappings:

- a trusted employee draft session might execute guest code in `sandbox.employee.foreground`,
- a draft workflow preview might execute handler turns in `sandbox.workflow.preview`,
- the same session's `npm install` may run in `sandbox.employee.packages`,
- a published customer-facing procedure may run in `sandbox.procedure.invoke`,
- a deployed reviewed workflow version may execute turns in `workflow.ts.execute`,
- procedure publication and migration-catalog writes may run in `control.sandbox.publish`, and
- MCP connections may resolve tools in `sandbox.mcp.foreground` while publication metadata reads stay in control-plane lanes.

This separation is especially important for colocated embeddings. A runaway sandbox or external-agent session should be throttled, shed, or rate-limited by its assigned domains before it can starve core application request handling or database control-plane recovery work.

### Migrations, Draft Queries, Published Procedures, and Workflows

The same sandbox runtime should support five distinct products with different trust and lifecycle models:

1. **reviewed migrations** for table/catalog setup and app-schema changes,
2. **draft query sandboxes** for trusted internal users who may run arbitrary code within their granted capabilities, and
3. **published procedures** for lower-trust or external callers who may invoke only reviewed immutable code artifacts,
4. **draft workflow sandboxes** for development-time authoring, testing, replay, and preview, and
5. **published workflow bundles** that plug into the Rust workflow executor in preview or production.

These should be modeled as separate libraries above `terracedb-sandbox` and `terracedb-workflows`, not as one undifferentiated execution mode.

These sandbox-facing workflow products are optional complements to the core workflow library. Native Rust workflows should remain first-class and should participate in the same runtime, history, visibility, and deployment model without needing `terracedb-sandbox`.

#### Migrations

A migration library such as `terracedb-migrate` should execute reviewed TypeScript modules inside the sandbox, but it should intentionally remain narrow in scope for version 1:

- create or ensure tables,
- evolve columnar schemas through validated successors,
- update table metadata and related catalog settings,
- record migration metadata and audit history.

Version 1 should explicitly not treat large data backfills or arbitrary row-rewrite jobs as part of this migration surface. Those are operational workloads with different scheduling, retry, and observability needs.

The preferred migration model is reviewed code that drives catalog-scoped host capabilities, not guest code with unrestricted write access to arbitrary tables.

#### Draft Query Sandboxes

Trusted internal users such as company employees may be granted the right to run arbitrary TypeScript code for exploratory or operational queries. Those sessions should still execute under an explicit manifest and should still be constrained by per-binding resource policy and budgets.

This keeps "arbitrary code" compatible with:

- tenant scoping,
- table allowlists,
- query-shape restrictions,
- rate limiting,
- audit logging, and
- future deployment-specific controls.

#### Published Procedures

A procedure library such as `terracedb-procedures` should turn reviewed sandbox code into immutable published artifacts. External or lower-trust callers should invoke a named reviewed version, not mutate the code that runs on their behalf.

Recommended published-procedure metadata:

```typescript
interface PublishedProcedureManifest {
  name: string
  version: string
  codeHash: string
  entrypoint: string
  inputSchema: JsonSchema
  outputSchema: JsonSchema
  capabilities: CapabilityGrant[]
  budgets: BudgetPolicy
  reviewedBy: string
}
```

Recommended publish flow:

1. author or update code in a draft sandbox,
2. review code plus requested capability manifest,
3. freeze an immutable snapshot or module bundle,
4. persist publication metadata such as code hash, schemas, budgets, and reviewer, and
5. invoke published versions by opening a fresh session from that immutable base.

Invocation should accept caller identity and tenant context, then evaluate the published procedure under the reviewed manifest and host-enforced budgets. The caller should not be able to widen capabilities, modify the code, or reuse a mutable draft session as the invocation target.

#### Draft Workflow Sandboxes

Workflow authoring in development should happen inside mutable sandbox sessions rather than inside the production workflow runtime directly. These draft workflow sandboxes are where teams should:

- edit workflow source and fixtures in a VFS-backed workspace,
- install supported npm dependencies through the host package service,
- run `tsc`, local tests, and workflow-specific validation,
- use `just-bash` helpers for preview, replay, inspection, or publish flows, and
- iterate on workflow code without first minting a reviewed immutable bundle.

Draft workflow sandboxes are for authoring and preview, not for direct production execution.

Preview should still route through the Rust executor. A preview runner may resolve workflow code from a mutable sandbox session, then create a normal workflow runtime with isolated table names or prefixes for that session, branch, or preview environment. This keeps development aligned with real inbox ordering, timers, outbox delivery, and crash-recovery semantics instead of inventing a second "dev-only" execution path.

#### Published Workflow Bundles

Production should execute immutable reviewed workflow bundles, not mutable draft sandbox sessions. A companion library or registry layer such as `terracedb-workflows-sandbox` plus a workflow deployment manager should:

- bundle the workflow entrypoint together with supported JS or TS dependencies,
- persist code hash, entrypoint, capability manifest, budgets, reviewer metadata, and execution-domain policy,
- activate or deactivate specific workflow versions at runtime, and
- start or stop the corresponding Rust executor bindings dynamically.

Recommended published-workflow metadata:

```typescript
interface PublishedWorkflowManifest {
  name: string
  version: string
  codeHash: string
  entrypoint: string
  capabilities: CapabilityGrant[]
  budgets: BudgetPolicy
  executionDomain: string
  reviewedBy: string
}
```

At execution time, published workflows should not receive authoring-only affordances such as `npm install`, `just-bash`, mutable VFS writes, or unrestricted network access. They should run as reviewed sealed artifacts under workflow-specific capabilities and host-chosen execution domains.

#### Development Versus Production Workflow Sandboxes

Development and production should share one TypeScript workflow source format, but they should not share one trust model.

- development sandboxes are mutable, tool-rich, and optimized for authoring velocity,
- published workflow sandboxes are sealed, reviewable, and optimized for deterministic execution under the Rust workflow runtime, and
- the publication step converts a draft sandbox into an immutable artifact that can be previewed strictly, deployed, and audited.

This means npm resolution, `just-bash`, and other authoring conveniences belong to draft workflow sessions and publish-time packaging, not to live production handler turns. Applications may still offer a "strict preview" mode in development by running the exact published bundle through the same Rust executor path that production will use.

### External Agent Adapters and MCP

The repo should support an external agent adapter crate, tentatively `terracedb-mcp`, that exposes selected sandbox and procedure surfaces to outside agents such as Claude Desktop. This adapter should sit above `terracedb-sandbox` and any procedure library; it should not be treated as a mode of `terracedb-vfs` itself.

That layering matters because `terracedb-vfs` remains an embedded library surface rather than a general service boundary, while MCP is explicitly a protocol adapter for external tools.

The MCP adapter should reuse the same capability model as in-process sandboxes:

- each MCP connection is resolved to a subject identity,
- the server derives an effective capability manifest for that subject and environment,
- every exposed tool or resource maps onto host capabilities or published procedures, and
- all invocations produce the same audit and tool-run history that local sandbox execution would.

A good starter surface is:

- list and inspect published procedures,
- invoke reviewed procedures,
- inspect read-only sandbox views,
- read or diff files from a sandbox snapshot,
- inspect tool-run and activity history,
- optionally run draft query code only for explicitly trusted subjects.

The important rule is that MCP should not introduce a second permission system. It should be a transport and presentation layer over the same capability templates, grants, manifests, quotas, and audit hooks already used by the sandbox runtime.

### Pull Requests and Editor Visibility

Pull-request creation should be a first-class sandbox workflow, not an external afterthought.

The recommended default PR path is:

1. record git provenance when hoisting the repo,
2. create an ephemeral worktree or temp export checkout at the recorded base,
3. eject the sandbox delta into that worktree,
4. create a branch, commit, push, and
5. open the PR through a provider adapter.

This is safer than mutating the user's active checkout directly and makes failures easier to replay and debug.

Editor visibility should be easy, but read-only by design. The preferred approach is a real VS Code extension package, kept in-repo, backed by a read-only sandbox-view protocol that works the same way for:

- a local app running on the developer machine, and
- a remote app exposing sandbox views across the network.

Cursor inherits VS Code's extension model, so the same extension package or a closely related build target should work there too. The extension should talk to either:

- a local in-process or loopback bridge when the app is local, or
- an authenticated remote endpoint when the app is remote,

without changing the user-facing editor workflow.

The editor-view surface should support at least:

- browse/list/open/stat on visible or durable sandbox cuts,
- refresh/reconnect behavior,
- read-only diffs against hoist base or ejected output when useful,
- session/volume selection if the application hosts multiple sandboxes, and
- the same view protocol and UX for local and remote sandboxes.

If an extension is not available, a fallback export-to-temp or local read-only bridge is acceptable for debugging, but the architecture should still treat "easy read-only viewing from VS Code and Cursor for both local and remote apps" as a first-class requirement rather than a manual debug trick.

### Foreground Session UX

The durable tool-run and activity history is the source of truth, but the sandbox should also support a first-class foreground session UX for active authoring sessions. Users should not need to wait for a post-hoc audit view to understand what the sandbox is doing right now.

Useful live session surfaces include:

- currently running guest execution or tool actions,
- pending interactive authorization requests for draft sessions,
- recent host-enforced denials or budget/resource failures for operations that actually reached the enforcement path,
- clear indication when a capability or shell bridge is simply unavailable in the current session because it was never injected,
- package-install, type-check, export, or PR-progress state,
- active read-only view handles and reconnect status.

This foreground UX should, as much as practical, be derived from or mirrored into the same durable activity model rather than inventing a disconnected telemetry plane. The system should feel live without sacrificing replayability or auditability.

### Onboarding and Teaching Surface

The sandbox architecture is powerful enough that teams will need help learning where different kinds of code belong. The repo should therefore treat onboarding as a first-class architectural concern, not just as a documentation afterthought.

The teaching surface should make these boundaries obvious:

- what belongs in sandbox-authored code,
- how draft authoring differs from reviewed production execution,
- how editor visibility, export, and audit history fit into normal workflows.

The ideal first-run experience is a small, concrete authoring path that demonstrates:

1. open a draft sandbox,
2. import or edit a project tree,
3. call one injected host capability from TypeScript and from shell,
4. inspect the result through the read-only editor view,
5. export or publish through the normal audited path.

This keeps the architecture teachable and makes the host/sandbox boundary feel intentional rather than accidental.

### Deterministic Testing and Simulation Boundary

The deterministic testing bar for `terracedb-sandbox` should be as high as practical, but the architecture should not pretend that every dependency is equally simulatable.

The first step in implementation should be to decide which parts can run inside the seeded simulation harness unchanged. The likely high-value deterministic targets are:

- session lifecycle,
- overlay/VFS mutations and activity ordering,
- guest runtime scheduling, module loading, and time/entropy behavior,
- capability dispatch semantics,
- TypeScript mirror updates,
- package-manifest and cache-state transitions,
- hoist/eject diff and provenance logic,
- PR-export planning,
- read-only editor-view snapshot semantics.

If a component such as real package fetching, remote registry integration, real git provider integration, or a real editor extension host cannot run inside the deterministic harness with acceptable fidelity, it should sit behind a stable interface with a deterministic stub/fake implementation. The simulation target then becomes the real session semantics plus the stubbed boundary behavior, while non-simulated production integrations still receive ordinary integration tests outside the turmoil harness.

This library should therefore define simulation seams explicitly and test them aggressively:

- same-seed replay should reproduce the same sandbox trace,
- cross-cutting workloads should cover guest execution, shell/tool actions, package changes, disk import/export, git export, and read-only editor views,
- crash/restart points should exist around session creation, cache publish, install metadata updates, hoist/eject commits, and PR export bookkeeping.

The architectural goal is not "simulate absolutely everything"; it is "simulate the maximum semantically important subset, and hide the rest behind deterministic contracts."

## Simulation-Native JavaScript Runtime

This section describes the planned JavaScript runtime architecture for TerraceDB. A JavaScript engine is not merely a guest-language implementation choice; it is an intended embedded subsystem that runs inside the same filesystem, sandbox, and simulation model as the rest of the stack.

The architecture target is a TerraceDB-owned runtime layer, tentatively `terracedb-js`, centered on `boa_engine` with maintained forks or replacements for runtime pieces that still assume ambient host behavior. The goal is not "embed Boa as-is" and not "run a stock Node or Deno host inside the sandbox." The goal is a JavaScript runtime whose sources of time, randomness, scheduling, module loading, IO, and compatibility behavior are owned by TerraceDB.

Implementation may still proceed in stages:

- the first shipping milestone may rely on a narrower compatibility surface and omit some host APIs entirely,
- selected upstream Boa runtime helpers may be wrapped first and forked later as deterministic seams are tightened, and
- some helper libraries may begin with deterministic stubs before production adapters exist.

That staged rollout does not change the intended architecture. The target design is a TerraceDB-owned JavaScript runtime core with explicit host-service boundaries and simulation-native execution semantics.

### Why This Is a Separate Subsystem

The reason to give this its own subsystem is structural rather than cosmetic:

- existing JavaScript runtime stacks usually bundle together parser/VM, module loading, timers, randomness, fetch, process/env, console, and compatibility shims,
- `boa_engine` already exposes useful embedding seams, but stock builders and runtime helpers still assume ambient cwd, `std` time, OS entropy, ordinary stdout/stderr, and real network/process integrations,
- TerraceDB wants sandbox execution to run against `terracedb-vfs`, execution domains, capability manifests, and deterministic simulation rather than ambient machine state.

That means the system is not "Boa plus a custom filesystem shim." It is a library with a different ownership model for module resolution, host APIs, entropy, time, scheduling, and compatibility.

### Proposed Layering

Under this design, the stack is:

- `terracedb-vfs`
  The authoritative filesystem, KV, snapshot, overlay, and activity substrate.
- `terracedb-js`
  A new embedded JavaScript runtime layer, most plausibly derived from `boa_engine` plus selectively owned forks or replacements for `boa_runtime` and `boa_wintertc`, that implements guest execution, module loading, deterministic host services, and compatibility shims against the TerraceDB substrate.
- `terracedb-sandbox`
  The guest-runtime layer that uses `terracedb-js` for code evaluation, capability injection, package/type tooling, shell orchestration, and session lifecycle.
- `terracedb-js-host-services`
  A narrow boundary for optional host-backed services such as registry fetches, network requests, console sinks, and process/env projections when a deployment chooses to expose them.

A useful rule of thumb is:

- `terracedb-js` owns **guest-observable JavaScript semantics inside the virtual world**,
- `terracedb-js-host-services` owns **explicit boundary crossings between the virtual world and the host world**.

### Current Backend Split

The current substrate is expected to expose two runtime-host shapes:

- `DeterministicJsRuntimeHost` is the default simulation/oracle backend when a test, loader, or host seam may need real async progress under seeded replay.
- `BoaJsRuntimeHost` is the real `boa_engine` execution backend for guest-visible JavaScript semantics and uses TerraceDB-owned scheduler, host-service, and module-loader seams.
- `ImmediateBoaModuleLoader` is only a bridge for loaders whose `JsModuleLoader` futures complete without yielding; loaders that need broader async progress should stay on the deterministic backend or implement the Boa-specific loader boundary directly.

### Runtime Actor Model

Each sandbox JavaScript instance should behave like a logically serialized actor:

- one host-owned execution context plus its active realms,
- no ambient `std::thread::spawn` or host scheduler ownership in the simulation path,
- host-directed message passing between sandboxes, workers, or helper actors,
- cross-context value transfer through structured-clone style serialization rather than shared mutable JavaScript objects.

This aligns well with Boa's existing same-thread execution model, but the actor boundary must become an explicit architectural rule rather than an informal runtime convention.

### Time, Entropy, and Scheduling

The runtime must treat the main sources of non-determinism as first-class host services:

- wall-clock and monotonic time flow through injected runtime services rather than ambient `SystemTime::now()` or `Instant::now()`,
- `Date`, `Temporal.Now`, timer deadlines, and console timing all derive from those injected services,
- randomness and entropy flow through an injected host-owned entropy source rather than OS randomness.

That entropy rule applies to all guest-visible randomness:

- `Math.random`,
- `crypto.getRandomValues`,
- `crypto.randomUUID`,
- future UUID/token/nonce surfaces exposed by compatibility libraries, and
- any runtime-generated identifiers whose values can leak into guest-observable behavior.

The entropy service should support realm- or context-scoped substreams so different realms may observe distinct sequences while remaining deterministic under seeded replay.

Scheduling must be owned in the same way:

- promise jobs, microtasks, timers, module-loading continuations, and cancellation all run through a host-owned scheduler,
- the runtime must be able to advance work to quiescence under deterministic simulation without wall-clock sleeps,
- blocking APIs such as `await_blocking`, `Atomics.wait`, or hidden host sleeps must be absent from the simulated path, strictly gated, or reworked behind explicit host-controlled adapters.

### Module Resolution and Filesystem Ownership

Module resolution must be VFS-native in a first-class way:

- the runtime never relies on ambient current working directory or Boa's stock filesystem-backed module loader,
- module roots are explicit and point at `terracedb-vfs` snapshots or overlays,
- resolution policy understands the sandbox specifier spaces described earlier in this part such as `terrace:/workspace`, `terrace:host`, and `npm:`,
- loader preparation performs package resolution, TypeScript transpilation, source-map generation, and code-cache planning before evaluation,
- source metadata and diagnostics should preserve stable virtual paths or URLs rather than accidental host paths.

This keeps module semantics aligned with the sandbox volume model instead of leaking host filesystem layout into guest execution.

### Compatibility and Host API Layers

Stock `boa_runtime` and `boa_wintertc` are useful reference points, but they are not the architectural endpoint. TerraceDB should treat them as sources of reusable pieces, not as the final ownership boundary.

The compatibility rule is:

- web/node-style APIs such as `fetch`, `console`, `process`, timers, messaging, and future `crypto` surfaces are layered above explicit TerraceDB-owned host-service traits,
- process/env views come from manifest and deployment policy rather than ambient process state,
- console output routes to tool/activity/audit sinks rather than raw stdout/stderr,
- network access, registry access, and any other side effects go through policy-controlled adapters instead of direct library defaults.

If a compatibility API cannot be simulated faithfully enough in the first phase, it should sit behind a deterministic stub/fake in simulation rather than pulling the whole runtime out of the deterministic harness.

### Internal Interfaces

Recommended internal interfaces would look more like:

```typescript
interface JsRuntimeHost {
  clock(): JsClock
  entropy(): JsEntropySource
  scheduler(): JsScheduler
  moduleLoader(): JsModuleLoader
  capabilities(): JsCapabilityResolver
}

interface JsEntropySource {
  stream(scope: JsEntropyScope): JsEntropyStream
}

interface JsEntropyStream {
  nextF64(): number
  fill(bytes: Uint8Array): void
  nextUuidV4(): string
}

interface JsRuntime {
  evaluate(req: JsEvaluateRequest): Promise<JsEvaluateResult>
  importModule(req: JsImportRequest): Promise<JsModuleHandle>
  runUntilQuiescent(): Promise<void>
}
```

These interfaces are intentionally split so the runtime core can remain simulation-native while deployment-specific host services stay small, explicit, and replaceable.

### Benefits, Costs, and Rollout

This architecture has two major benefits:

1. JavaScript execution becomes part of the same correctness model as the rest of the stack.
   Module resolution, timers, promise scheduling, entropy, capability dispatch, and guest-visible runtime behavior run against the same seeded simulation harness as VFS overlays, sandbox recovery, and git-aware flows.
2. Sandbox behavior becomes more self-contained and policy-driven.
   A sandbox no longer depends on ambient process cwd, env, time, randomness, or stdout just to evaluate code; those semantics are supplied explicitly by the host.

It also has major costs:

- this is a real owned subsystem, not just a thin `boa_engine` embedding,
- the project will likely maintain targeted forks or replacements for parts of the upstream runtime layer,
- web/node compatibility growth must be disciplined so convenience does not quietly reintroduce ambient host behavior.

Because of that cost, this subsystem should be treated as an explicit product commitment rather than as a low-level implementation detail. The project is choosing to make JavaScript runtime semantics participate directly in the TerraceDB simulation and sandbox model instead of treating the guest engine as an opaque library hidden behind host-only integration tests.

### Relationship to VFS-Native Git

`terracedb-js` and `terracedb-git` are separate libraries, but they are intended to sit on one shared simulation-native sandbox substrate rather than evolve as unrelated integrations.

That shared substrate should include:

- `terracedb-vfs` as the only authoritative filesystem and snapshot/overlay model,
- one host-owned scheduler and cancellation model for guest execution and repository work,
- one deterministic source of time and entropy,
- one capability and host-service boundary for explicit crossings to registries, remotes, consoles, or provider APIs,
- one replay/debug story in which a failing seed captures guest actions, repository state, boundary calls, and recovery points together.

Guest-facing git-aware flows should therefore be ordinary sandbox capability calls from JavaScript into `terracedb-git`, not ad hoc subprocess invocations or a second guest-side reimplementation of repository semantics. The important architectural property is that guest execution, VFS mutation, repository inspection, branch/export planning, and host-bridge calls all participate in one seeded replay model.

## VFS-Native Git Library

This section describes the planned git architecture for TerraceDB. Git is not merely a host integration detail; it is an intended embedded subsystem that runs inside the same filesystem, sandbox, and simulation model as the rest of the stack.

This subsystem is designed alongside the **Simulation-Native JavaScript Runtime** above. Together, `terracedb-js` and `terracedb-git` form the simulation-native sandbox substrate that guest code, repo-backed authoring flows, and host-bridge adapters build on.

The architecture target is **true TerraceDB-style VFS/sandbox/runtime compatibility for git itself**. Reaching that target likely requires an invasive fork of `gitoxide`, not a thin wrapper around the `git` CLI and not a small adaptation of unmodified `gix`.

Implementation may still proceed in phases:

- the first shipping milestone may rely more heavily on the simpler host-interop flows described earlier in this part,
- the host bridge may initially carry more of the import/export burden while the VFS-native core is brought up, and
- transport and provider integration remain outside the simulated core even after the VFS-native git subsystem exists.

That staged rollout does not change the intended architecture. The target design is a TerraceDB-owned git subsystem with host interop pushed to explicit boundary adapters.

### Why This Is a Separate Subsystem

The reason to give this its own subsystem is structural rather than cosmetic:

- existing git implementations are strongly shaped around real host paths, `std::fs` metadata, host tempfiles and lockfiles, and ordinary process/runtime assumptions,
- TerraceDB wants repository semantics to run against a `terracedb-vfs` snapshot or overlay,
- deterministic simulation wants semantically important git behavior to run without host filesystems, process-global signal handlers, or ambient machine state.

That means the system is not "git plus a different path type." It is a library with a different ownership model for storage, cancellation, scheduling, and worktree materialization.

### Proposed Layering

Under this design, the stack is:

- `terracedb-vfs`
  The authoritative filesystem, KV, snapshot, overlay, and activity substrate.
- `terracedb-git`
  A new embedded git library, most plausibly derived from a forked `gitoxide`, that implements object, ref, index, checkout, diff, and status semantics against the VFS substrate.
- `terracedb-sandbox`
  The guest-runtime layer that uses `terracedb-git` for repo inspection, status, diffing, branch/export planning, and VFS-native worktree mutation.
- `terracedb-git-host-bridge`
  A narrow boundary for importing from real host repos, exporting changes back to a host checkout, and interacting with remotes or PR providers.

A useful rule of thumb is:

- `terracedb-git` owns **git semantics inside the virtual world**,
- `terracedb-git-host-bridge` owns **boundary crossings between the virtual world and the host world**.

### VFS-Native Repository Model

The repository model must become VFS-native in a first-class way:

- repository discovery and opening take explicit roots and policy context rather than relying on ambient cwd or process environment,
- worktree reads and writes target `terracedb-vfs` volumes or snapshots directly,
- index refresh and checkout metadata read VFS inode/stat information rather than `std::fs` metadata,
- lockfiles, tempfiles, and atomic-replace flows are modeled in VFS-native terms rather than as ordinary host `.lock` files in host directories,
- repository-local scratch state lives in sandbox/session storage instead of host temp directories.

Worktree semantics shift as well:

- "checkout" means materializing a tree into a VFS volume or overlay, not into a host directory,
- linked-worktree style flows map more naturally to snapshot-plus-overlay clones than to sibling directories on the host filesystem,
- diff, status, and index/worktree comparisons operate over VFS state directly,
- sandbox git operations become first-class simulated operations rather than host-only integration tests.

### Runtime and Determinism Requirements

The fork must also adopt a runtime model that matches the rest of TerraceDB:

- repository mutation should support a logically serialized or explicitly host-driven execution model,
- process-global interrupt and cleanup state should be replaced with session-scoped cancellation and resource ownership,
- parallel fan-out should either be host-controlled through explicit execution hooks or be cleanly disabled for deterministic simulation,
- no correctness-critical behavior should depend on host signal handlers, global tempfile registries, or best-effort process-exit cleanup.

This is where the cost becomes real. A viable fork likely needs to own or heavily rewrite the equivalent of:

- `gix-fs`,
- `gix-tempfile`,
- `gix-lock`,
- `gix-worktree`,
- `gix-worktree-state`, and
- higher-level orchestration in `gix`.

Some porcelain workflows are incomplete even before adaptation, so the project must finish missing orchestration while also rehosting the IO and runtime boundary.

### Host Boundary and Import/Export

Even with a VFS-native git core, the application still needs an explicit boundary to the host world. The difference is that those flows are adapters around the core rather than the place where git semantics live.

The host bridge is responsible for:

- importing a real host repo into a VFS-native repository image,
- materializing a VFS-native branch or patch into a real checkout for review,
- pushing to a remote using real credentials and transport,
- creating pull requests through provider APIs.

This split keeps the core repository logic simulation-friendly while preserving practical interoperability with ordinary developer machines and hosted git providers.

### Internal Interfaces

Recommended internal interfaces would look more like:

```typescript
interface GitRepositoryStore {
  open(repoRoot: string, opts?: GitOpenOptions): Promise<GitRepository>
}

interface GitRepository {
  status(opts?: GitStatusOptions): Promise<GitStatusReport>
  diff(req: GitDiffRequest): Promise<GitDiffReport>
  checkout(req: GitCheckoutRequest): Promise<GitCheckoutReport>
  writeIndex(): Promise<void>
  updateRef(req: GitRefUpdate): Promise<void>
}

interface GitHostBridge {
  importHostRepo(req: GitImportRequest): Promise<GitImportReport>
  exportToHost(req: GitExportRequest): Promise<GitExportReport>
  push(req: GitPushRequest): Promise<GitPushReport>
}
```

These interfaces are intentionally split so the core repository semantics can remain VFS-native and simulation-friendly while the host bridge stays small, explicit, and replaceable.

### Benefits, Costs, and Rollout

This architecture has two major benefits:

1. Git becomes part of the same correctness model as the rest of the stack.
   Diff, status, checkout, branch staging, and export planning run against the same seeded simulation harness as VFS overlays, capability actions, and session recovery logic.
2. Sandbox behavior becomes more self-contained.
   A sandbox repo no longer needs a prepared host worktree just to evaluate git-aware workflows; the host bridge is needed only at explicit import/export or transport boundaries.

It also has major costs:

- this is a real fork, not a small integration layer,
- the fork becomes a long-term owned subsystem rather than an incidental vendored dependency,
- the blast radius is large enough that maintenance cost should be treated as product-level scope, not implementation detail.

Because of that cost, this subsystem should be treated as an explicit product commitment rather than as a low-level implementation swap. The project is choosing to make git participate directly in the TerraceDB simulation and sandbox model instead of treating it only as a host import/export integration.

The rollout implication is therefore:

- host-git interop remains part of the design, but as a bridge layer and phased implementation path rather than the architectural endpoint,
- a forked `gitoxide`-derived `terracedb-git` is the intended core git subsystem, and
- implementation should proceed in stages so import/export, PR, and provider workflows can ship before the full VFS-native core is complete.

---

# Part 6: Bricks Library

This part describes a separate library, tentatively `terracedb-bricks`, that stores large binary objects out-of-line in an application-provided blob store while keeping only metadata, references, and derived search indexes inside Terracedb. The intended use is to make large files queryable and composable without turning them into engine-level inline values or forcing every large write through the commit log, memtable, and SSTable paths.

## Goals and Non-Goals

`terracedb-bricks` should preserve the following externally visible properties:

- direct-to-blob-store ingestion with no required local-disk spill and no requirement that the DB inline the full object bytes,
- Terracedb-resident metadata rows for queryability: object IDs, digests, byte length, content type, tags, timestamps, and application metadata,
- range reads and whole-object reads from the backing blob store,
- durable activity rows and durable indexing hooks so metadata search and downstream workflows can tail blob lifecycle events,
- optional derived text/chunk/index tables for search, preview, and enrichment, and
- deterministic testing of publish, delete, indexing, and garbage-collection behavior through injected DB and blob-store traits.

Version 1 is intentionally narrower than the full space of content-addressable stores and search systems:

- no engine-level `Value::Blob` or special blob-aware MVCC type,
- no promise that arbitrary application rows and external blob bytes are one physical atomic unit; correctness comes from explicit publish ordering,
- no requirement that the DB's own tiered-storage, backup, or cold-storage manifests double as the blob catalog,
- no core full-text or vector-search engine; search is metadata plus library-managed derived indexes, and
- no hidden dependence on one specific object-store SDK or one provider's multipart protocol.

## Load-Bearing Decisions

These decisions most constrain the design:

- **Blob bytes are always out-of-line.** Terracedb stores metadata, references, and search indexes, not the primary blob payload.
- **Publish order is upload first, metadata second.** If the process crashes after upload but before metadata publish, the result is an orphan object that GC may later reclaim. The reverse ordering is not allowed because it would expose unreadable blobs.
- **Search is derived-data-first.** Queryability comes from ordinary Terracedb tables containing metadata, extracted text, previews, embeddings, or application-defined indexes rather than from scanning opaque object bytes during reads.
- **The blob store is a library boundary.** The preferred backend supports streaming upload and range reads. A compatibility adapter may wrap the engine's current whole-buffer `ObjectStore` trait for tests or small objects, but the library contract itself should not force whole-object buffering.
- **Blob GC is separate from engine remote-storage GC.** The library may reuse the same physical bucket/account as Terracedb's backup or cold-storage paths, but only with a disjoint prefix and an independent reachability/retention policy.

## Public Surface

The library exposes an embedded API surface oriented around metadata, object I/O, and indexing hooks:

```typescript
interface BlobLibraryConfig {
  namespace: string
  createIfMissing?: boolean
}

interface BlobStore {
  put(key: string, data: AsyncByteStream, opts?: BlobPutOptions): Promise<BlobObjectInfo>
  get(key: string, opts?: { range?: ByteRange }): Promise<AsyncByteStream>
  stat(key: string): Promise<BlobObjectInfo | null>
  delete(key: string): Promise<void>
}

interface BlobCollection {
  put(input: BlobWrite): Promise<BlobHandle>
  stat(id: BlobId): Promise<BlobMetadata | null>
  get(id: BlobId, opts?: { range?: ByteRange }): Promise<BlobReadResult>
  delete(id: BlobId): Promise<void>
  search(query: BlobQuery): Promise<AsyncIterator<BlobSearchRow>>
  activitySince(cursor: LogCursor, opts?: { durable?: boolean }): Promise<AsyncIterator<BlobActivity>>
  subscribeActivity(opts?: { durable?: boolean }): Receiver<SequenceNumber>
}

interface BlobWrite {
  alias?: string
  data: AsyncByteStream | bytes
  contentType?: string
  tags?: Record<string, string>
  metadata?: Record<string, Json>
}

interface BlobHandle {
  id: BlobId
  objectKey: string
  digest: string
  sizeBytes: u64
}
```

`BlobStore` is intentionally a separate library-edge abstraction rather than a hidden alias for the engine's object-store trait. The important difference is that blob ingestion should be able to stream large inputs directly to the backing store. A small-object/test adapter can still be built on top of the engine's injected `ObjectStore`, but the architecture should not hard-code whole-buffer semantics into the blob library itself.

## Reserved Tables and Object Layout

Version 1 should keep object bytes out of Terracedb tables entirely. Terracedb rows hold only current metadata, aliases, lifecycle events, and derived indexes.

### Current-State Tables

| Table | Key shape | Purpose |
|---|---|---|
| `blob_catalog` | `(namespace, blob_id)` | Current metadata row: `object_key`, digest, byte length, content type, tags, application metadata, timestamps, optional indexing status |
| `blob_alias` | `(namespace, alias)` | Optional stable lookup/upsert alias that resolves to the current `blob_id` |
| `blob_object_gc` | `(namespace, object_key)` | Optional GC helper row containing first-seen time, digest, size, and other reachability/cleanup metadata if the implementation wants DB-assisted GC bookkeeping |

### Append-Only and Derived Tables

| Table | Key shape | Purpose |
|---|---|---|
| `blob_activity` | `(namespace, activity_id)` | Append-only semantic audit stream for `blob_published`, `blob_deleted`, alias updates, and indexing milestones |
| `blob_text_chunk` | projection-owned | Optional extracted-text or preview chunks keyed for snippet retrieval and rebuild |
| `blob_term_index` | projection-owned | Optional normalized term/tag/prefix index for metadata or extracted-text search |
| `blob_embedding_index` | application-owned | Optional vector/semantic index metadata if the application wants semantic retrieval layered on top |

Object keys should live under a blob-library-owned prefix such as `blobs/<namespace>/...`, not under Terracedb's `backup/` or `cold/` prefixes. Content-addressed naming is recommended because it simplifies deduplication and orphan cleanup, but random IDs are still a valid implementation if deduplication is deferred.

## Ingestion, Publish, and Read Semantics

Blob publish is a two-system protocol: the object store receives bytes, and Terracedb receives metadata. Because those two systems cannot be made one physical commit without turning the engine into a blob transport, the library must define explicit ordering.

The publish path is:

1. stream the bytes to the blob store while computing digest and byte length,
2. once upload succeeds, commit `blob_catalog`, `blob_alias`, and `blob_activity` rows in one Terracedb batch,
3. let durable indexers react to `blob_activity` or `blob_catalog` changes to build search tables.

This ordering gives the right failure mode:

- if upload succeeds but metadata publish fails or the process crashes, the object is merely orphaned and can be reclaimed later,
- if metadata publish succeeds, the referenced object is already present,
- the library never makes a blob visible before the object exists.

Reads are metadata-first:

1. resolve `blob_id` or alias through Terracedb,
2. capture the referenced `object_key` and metadata at a snapshot-consistent cut,
3. fetch the bytes or requested range from the blob store.

The library should fail closed if the metadata row exists but the object is missing or corrupt. That is a library/storage inconsistency, not a case for silently returning empty content.

## Search and Indexing

The reason to use a blob library on top of Terracedb is not just to store large bytes elsewhere; it is to make those objects queryable without forcing the DB to inline them.

Search therefore operates on ordinary Terracedb rows:

- metadata search over content type, tags, aliases, digest, size, timestamps, and application-defined fields,
- extracted-text search over `blob_text_chunk` and `blob_term_index`,
- preview/search-result rendering from derived snippet rows, and
- optional semantic retrieval from application-managed embedding metadata.

The blob library should not claim that arbitrary raw-byte search is a core read-path feature. Instead, the indexing story should reuse the projection/workflow machinery from earlier parts:

- authoritative indexers consume the **durable** blob activity stream,
- index output and cursor advancement commit atomically using the projection runtime,
- rebuild after `SnapshotTooOld` or extractor changes comes from the current catalog plus blob-store reads, not from treating old SSTables as the blob payload store.

This means large-file search is mostly an indexing problem, not a database-value-type problem.

## Delete, Retention, and Garbage Collection

Delete is also two-phase:

1. remove or supersede the visible metadata/alias rows in Terracedb and append a delete activity row,
2. reclaim the external object later, once it is safe.

Immediate object deletion is unsafe for two reasons:

- another live metadata row may still reference the same object, especially if deduplication or alias replacement is enabled,
- retained MVCC history or long-lived snapshot cuts may still be able to observe an older metadata version that references the object until the normal history horizon moves past it.

Blob GC should therefore respect both **current reachability** and **history retention**. An implementation may use `blob_object_gc`, object-store prefix scans, or provider-side metadata to assist the sweep, but the rule is the same: do not remove an object until no visible metadata row, retained historical metadata version, or documented grace window can still legally reference it.

As with backup/offload GC, stale object-store listings and successful uploads with lost responses must be treated as normal failure modes. The design should prefer harmless orphan objects over premature deletion.

## Storage Modes and Deterministic Testing

The enclosing DB's storage mode still matters, but only for metadata and derived indexes:

- in tiered mode, blob metadata tables behave like ordinary Terracedb tables and may themselves move through hot/cold storage over time,
- in deferred-durability or s3-primary mode, metadata visibility may lead metadata durability until flush boundaries,
- authoritative indexers and workflows above blob events should therefore use the durable stream.

Blob bytes themselves inherit the durability semantics of the chosen `BlobStore`, not of Terracedb's commit log. That separation is intentional.

This library should inherit the same deterministic testing bar as the rest of the stack. The simulation target is the real publish/read/delete/index/GC code. The shadow model needs to cover current metadata rows, alias resolution, object bytes, durable activity order, derived index state, orphan-object harmlessness, and safe cleanup across crash and restart.
