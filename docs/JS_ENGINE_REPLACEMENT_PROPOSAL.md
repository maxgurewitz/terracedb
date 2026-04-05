# Scheduler-Compatible JavaScript Runtime

## Purpose

TerraceDB needs a JavaScript runtime that fits the scheduler and simulation model described in [docs/ARCHITECTURE.md](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/ARCHITECTURE.md).

This document describes that runtime.

The runtime is intended to support:

- production sandbox execution on a Tokio-based scheduler
- deterministic simulation testing
- thread-level parallelism across sandboxes
- explicit host control over time, I/O, randomness, and scheduling
- upstream Node JavaScript running on top of Terrace-owned primitives
- portable durable artifacts that can package filesystem state together with paused JavaScript runtime state

## Runtime Vision

The JavaScript runtime will be built around **turn-based cooperative execution**.

Each runtime instance:

- executes JavaScript on one thread at a time
- reaches explicit safepoints between turns
- exposes scheduler-facing futures that are `Send`
- can be resumed on a different worker thread after a safepoint

This gives the system the right shape for both:

- production execution under Tokio
- deterministic simulation under Terrace-controlled scheduling

## Execution Model

### Turns

JavaScript runs in bounded turns.

Examples of turns:

- bootstrap turn
- entrypoint/module evaluation turn
- timer delivery turn
- host-completion delivery turn
- microtask-drain turn

At the end of a turn, the runtime yields back to the scheduler.

### Safepoints

Safepoints are the boundaries where the runtime is paused and the scheduler regains control.

Typical safepoints:

- before starting a turn
- after completing a turn
- after draining microtasks
- after delivering a timer or task-queue callback
- after issuing a host async operation
- before consuming a completed host async operation

The scheduler may resume the runtime on any worker thread after a safepoint.

### Runtime State

The runtime is divided into two layers.

#### Scheduler-owned suspended state

This state is `Send` and survives between turns.

Examples:

- timer queues
- task queues
- pending host operations
- callback ids
- promise reaction metadata
- module graph metadata
- runtime policy and configuration
- VFS and host-resource handles represented as stable ids

#### Engine-attached state

This state exists only while a turn is actively executing.

Examples:

- interpreter context
- active call frames
- operand stack
- live object handles
- temporary engine-local wrapper state

Engine-attached state is attached for a turn and detached at a safepoint.

## Scheduler Contract

The runtime must integrate directly with TerraceDB's scheduler model.

That requires:

- Tokio-compatible `Send` outer futures
- explicit scheduler-visible yield points
- no ambient host event loop hidden inside the runtime
- no live engine state crossing scheduler-visible async boundaries

The scheduler owns:

- when a turn runs
- which worker thread runs it
- when time advances
- when host operations complete
- when timers and queued callbacks are delivered

## Phase 1 Runtime Contract

Phase 1 defines the concrete execution contract that all later implementation work must follow.

This section is intended to be normative.

### Turn Semantics

A turn is a bounded unit of JavaScript execution selected by the host scheduler.

The runtime must support at least these turn kinds:

- `Bootstrap`
- `EvaluateEntrypoint`
- `EvaluateModule(ModuleId)`
- `DeliverTimer(TimerId)`
- `DeliverTask(TaskId)`
- `DeliverHostCompletion(OpId)`
- `DrainMicrotasks`

Turn rules:

- exactly one turn executes for a runtime at a time
- a turn runs on exactly one worker thread
- a turn may allocate engine-attached state
- a turn may not hold thread-affine engine state across a scheduler-visible suspension point
- a turn ends only by producing an explicit outcome

Each turn must end in one of these outcomes:

- `Completed`
- `Yielded`
- `PendingHostOp(OpId)`
- `PendingTimer(Deadline)`
- `PendingMicrotasks`
- `Threw(JsErrorReport)`
- `Terminated`

`Completed` means the requested unit of work finished.

`Yielded` means execution reached an explicit safepoint without finishing the broader task and the scheduler may resume later.

`PendingHostOp(OpId)` means JavaScript requested a host operation and execution cannot continue until the host reports completion for that operation.

`PendingTimer(Deadline)` means the runtime is waiting for the next timer deadline selected by the scheduler.

`PendingMicrotasks` means the runtime has queued microtask work and must be scheduled for a `DrainMicrotasks` turn before further macrotask progress.

`Threw(JsErrorReport)` means the turn produced an uncaught exception at the current execution boundary.

`Terminated` means the runtime was explicitly aborted by host policy.

### Safepoint Semantics

A safepoint is the only boundary where a runtime may pause on one worker thread and resume on another.

The runtime is at a safepoint only when all of the following are true:

- no engine frame is actively executing
- no borrow or guard tied to engine-attached state remains live
- no host callback is currently running inside the engine
- the engine heap and runtime state are internally consistent
- the next action to perform is represented entirely in scheduler-owned suspended state

The runtime must reach a safepoint:

- before a new turn starts
- after a turn outcome is produced
- after a host operation is recorded but before control returns to the scheduler
- after microtask draining completes
- after timer or queued-task callback delivery completes

The runtime may not migrate between threads:

- during bytecode execution
- during native callback execution
- during GC tracing or sweeping
- while any engine-attached borrow, handle, or guard remains live

### Attachment and Detachment Semantics

Runtime execution alternates between:

- detached suspended state
- attached engine execution

Attachment rules:

- attachment selects one worker thread for the duration of one turn
- attachment creates or reuses engine-attached state for that turn
- attachment may resolve stable ids into live engine values for the duration of the turn
- attachment may not mutate scheduler-owned state except through explicit runtime transition points

Detachment rules:

- detachment happens only at a safepoint
- detachment must leave no live engine borrow, guard, or handle reachable from suspended state
- detachment must materialize any newly created continuation or callback identity into stable ids
- detachment must leave the next unit of work fully described by suspended state plus immutable compiled artifacts

The runtime must not expose an attached engine handle outside the boundary of the current turn.

### Suspended State Inventory

Phase 1 requires a concrete suspended-state model.

At minimum, suspended state must be able to represent:

- runtime configuration and policy
- timer queue entries
- task queue entries
- pending host operations and completion payloads
- stable callback and continuation ids
- promise reaction queues and promise bookkeeping needed across turns
- module graph metadata
- dynamic-import continuation metadata
- runtime termination state
- immutable references to compiled artifacts

Suspended state rules:

- suspended state must be `Send`
- suspended state must not contain `Rc`, `RefCell`, live GC handles, or thread-local runtime references
- suspended state may contain stable ids, copyable metadata, host-owned immutable blobs, and scheduler-owned queues
- suspended state must be serially inspectable for simulation and debugging

### Runtime Object Identity Rules

There are two different kinds of identity in the system:

- engine-local identity while a turn is attached
- stable runtime identity across turns

Engine-local identity covers:

- temporary handles
- frame-local values
- ephemeral wrapper objects used only during one turn

Stable runtime identity covers:

- pending callbacks
- host operations
- promises that survive across turns
- modules and module graph nodes
- host resources exposed to JavaScript across turns

Rules:

- only stable runtime identities may appear in suspended state
- any value that survives a safepoint must be representable either as immutable data or as a stable id
- reconstructing live engine objects from stable ids must be deterministic

### Callback Identity

Any callback or continuation that survives beyond the current turn must have a stable host-owned id.

This includes:

- timer callbacks
- task-queue callbacks
- host-operation completion continuations
- promise reaction continuations
- dynamic-import continuations
- module-evaluation continuation points

Rules:

- stable ids are scheduler-owned data, not live engine handles
- ids may outlive the worker thread that created them
- ids are resolved to live engine callables only while a turn is attached
- ids may be invalidated only by explicit completion, cancellation, or runtime teardown

The runtime may not store a live callable, object handle, or closure directly in suspended state.

### Promise and Microtask Semantics

Promise jobs and microtasks are host-visible runtime state.

Rules:

- promise reactions are enqueued in scheduler-owned suspended state
- microtasks run only during an explicit `DrainMicrotasks` turn
- a macrotask-producing turn that enqueues microtasks must return `PendingMicrotasks` unless the runtime drains them before producing its final outcome
- microtask draining continues until the queue is empty or execution yields a non-microtask outcome such as `PendingHostOp`, `Threw`, or `Terminated`
- no timer, task-queue callback, or host completion may interleave inside the middle of one `DrainMicrotasks` turn

This keeps microtask progression deterministic and fully controlled by the host scheduler.

Promise bookkeeping that crosses turns must therefore be represented in suspended state, not only inside attached engine structures.

### Host Operation Suspension and Completion

All host-visible effects must follow the same suspension model.

This includes:

- filesystem operations
- networking operations
- child-process operations
- module source loading
- dynamic import resolution
- host timers
- entropy or environment queries that are policy-controlled

The protocol is:

1. JavaScript requests a host operation through a binding.
2. The engine records a scheduler-owned `OpId` plus operation metadata.
3. The current turn yields with `PendingHostOp(OpId)`.
4. The host performs the operation outside the engine.
5. The host records the result or error in suspended state.
6. A later `DeliverHostCompletion(OpId)` turn resumes the runtime with that completion.

Rules:

- the engine may not wait on host async work from inside a turn
- live engine state may not be captured across the host async boundary
- completion delivery must be idempotent with respect to scheduler retries
- host completion ordering is determined by the scheduler, not by ambient runtime behavior

### Cancellation and Teardown Semantics

The runtime must support explicit host-driven cancellation and teardown.

Cancellation rules:

- the host may request termination while the runtime is detached or attached
- if attached, termination takes effect at the next legal safepoint unless host policy defines a stronger abort mechanism for the current turn kind
- pending host operations may be cancelled, allowed to complete and then discarded, or converted into synthetic completion errors according to host policy

Teardown rules:

- teardown invalidates all stable callback, continuation, promise, module, and host-resource ids owned by the runtime
- teardown releases scheduler-owned queues and host-operation records
- teardown may not leak attached engine state across the final detachment boundary
- teardown must produce a deterministic terminal report for simulation and debugging

### Module Evaluation Boundaries

Modules are evaluated as an explicit sequence of host-visible steps.

The minimum boundary model is:

1. parse/compile module source into immutable compiled artifacts
2. request dependency resolution and loading through host operations
3. instantiate the module graph
4. evaluate the module graph in one or more turns
5. surface completion, async dependency waits, or errors through explicit turn outcomes

Rules:

- module graph metadata is suspended state
- loaded source bytes and compiled artifacts are host-owned or runtime-owned immutable inputs, not thread-local transient state
- dynamic import continuation state uses stable ids, not live engine-local closures
- `import.meta` and host-defined options are reconstructed at attachment time from suspended state and host policy

### Environment and Policy Configuration

Environment-shaped values are runtime configuration, not ambient host reads.

This includes:

- locale
- timezone
- cwd
- env
- argv
- execArgv
- execPath
- process ids
- umask
- temp/home directory
- platform and architecture policy values

Rules:

- these values are stored in runtime configuration before execution begins
- tests may override them explicitly
- child sandboxes inherit them explicitly through host policy, not implicitly from the host process
- guest code may observe only the configured values

### GC and Memory Contract

Phase 1 does not require a final collector implementation, but it does require the runtime contract that collector work must satisfy.

Rules:

- GC work may run only while the runtime is attached to one worker thread
- GC may not cross a safepoint in a partially-complete state that prevents detachment
- the runtime may not migrate between threads during GC work
- after detachment, suspended state must not contain collector-owned transient handles
- collector ownership must be compatible with attach, detach, and later reattachment on a different worker thread

This is the contract that the later heap and GC implementation must satisfy.

### Non-Goals For Phase 1

Phase 1 does not require:

- a finished Node compatibility layer
- exact production memory statistics
- real worker-thread execution inside guest JavaScript

Phase 1 does require:

- a complete runtime contract that future engine work can implement without changing the scheduler model

### Phase 1 Acceptance Criteria

Phase 1 is complete when this document defines all of the following precisely enough to implement against:

- legal turn kinds and turn outcomes
- legal safepoint boundaries
- attachment and detachment rules
- the minimum suspended-state inventory
- stable identity rules across turns
- promise and microtask progression rules
- host-operation suspension and completion protocol
- module evaluation boundaries
- environment and policy configuration rules
- cancellation and teardown semantics
- the GC and memory contract imposed by scheduler migration

Phase 1 does not require code to exist yet. It requires the implementation contract to be complete enough that Phase 2 can translate it into Rust boundary types without reopening the scheduler model.

## Deterministic Host Control

The runtime will continue to use Terrace-owned primitives for all nondeterministic behavior.

This includes:

- filesystem access
- networking
- child processes
- randomness
- timers
- microtask and task-queue progression
- clock and locale-sensitive environment data

The runtime therefore remains fully compatible with simulation testing and replayable execution.

## Relationship To Node Compatibility

This runtime change does not alter the Node compatibility architecture described in [docs/NODE_COMPAT_ARCHITECTURE.md](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/NODE_COMPAT_ARCHITECTURE.md).

That plan remains:

- run upstream Node JavaScript
- provide Terrace-owned deterministic primitive bindings
- avoid large handwritten JavaScript compatibility layers

This document only changes the JavaScript engine substrate used underneath that architecture.

## Node Integration Modes

The rewritten runtime should support three Node-related operating modes.

### 1. No Node injected

This is the plain Terrace JavaScript runtime.

In this mode:

- no Node compatibility layer is installed
- no Node builtin JavaScript is loaded
- sandboxes run only Terrace-provided JavaScript functionality

This is the simplest mode and should remain available for sandboxes that do not need Node.

### 2. Node compatibility from VFS

This is the transparent compatibility and debugging mode.

In this mode:

- Rust injects the Node primitive binding surface
- the runtime loads upstream Node JavaScript from files on the sandbox VFS
- startup follows the upstream Node bootstrap path as directly as possible

This mode is intended for:

- upstream Node compatibility tests
- compatibility bring-up work
- debugging and inspection when we want the Node JavaScript visible as normal files

It is expected to be slower than production because each fresh runtime still has to load and
prepare the Node JavaScript from the filesystem-backed source tree.

### 3. Node compatibility from a host-prepared runtime package

This is the production and high-throughput mode.

In this mode:

- Rust injects the same Node primitive binding surface as mode 2
- the parent Rust process owns a prepared Node runtime package
- sandbox startup reads Node from that host-owned package instead of walking `/node/...` files on
  the sandbox VFS

The preparation step for this mode is more than "keep Node source in memory". It should front-load
the expensive work that does not need to be repeated for every sandbox.

That preparation step may include:

- collecting the Node JavaScript needed for bootstrap and core modules from the pinned Node source
  tree
- packaging that JavaScript into one host-managed runtime asset rather than many guest-visible
  files
- preprocessing that JavaScript into the runtime's compiled or otherwise ready-to-load internal
  representation
- recording builtin ids, module metadata, and version/digest information needed to validate that
  the package matches the injected binding contract
- loading that prepared package into host memory before sandbox startup when the embedding
  application wants the lowest startup latency

At sandbox startup, the host should then:

- inject the Rust binding surface
- attach the prepared Node runtime package
- boot the sandbox's Node layer from that prepared package

This means:

- the application's own files still live on sandbox VFS
- the Node runtime installation does not need to exist as a normal executable or source tree on the
  sandbox VFS
- production startup can avoid repeated filesystem reads and repeated preparation of the Node core
  JavaScript

Modes 2 and 3 must preserve the same guest-visible Node behavior. The difference is loading and
startup strategy, not compatibility semantics.

## Relationship To Boa

Boa remains useful as a source of implementation ideas and as a source of reusable non-runtime crates.

The primary local reference tree for this work is:

- `~/dev/boa`

Good candidates for continued reuse as dependencies:

- `boa_ast`
- `boa_parser`
- `boa_interner`
- `boa_string`

The runtime described here will draw on Boa's source code where that is helpful for:

- parser behavior
- AST structures
- source mapping
- implementation techniques

The core runtime itself will be shaped by TerraceDB's scheduler contract.

## Boa Source Classification

The local Boa tree already falls into three clear categories for this rewrite:

- reuse as crates
- use as implementation reference
- rewrite for Terrace

This classification should drive the order of work and the amount of code we carry forward.

### Reuse as crates

These parts of Boa are good candidates to keep consuming directly as dependencies:

- `core/ast`
- `core/parser`
- `core/interner`
- `core/string`
- selected macros from `core/macros`
- `core/icu_provider` where ICU data packaging is useful

Why these are reusable:

- they are compile-time or data-model oriented
- they do not define the scheduler contract
- they are much less entangled with thread-affine runtime ownership

What we expect to keep using them for:

- syntax trees
- parsing
- scope analysis
- source mapping
- string interning and string primitives
- helper macros and ICU data packaging

### Use as implementation reference

These parts of Boa are useful as source material but should not become Terrace runtime dependencies in their current form:

- `docs/vm.md`
- `docs/bytecompiler.md`
- `core/engine/src/bytecompiler/*`
- `core/engine/src/vm/code_block.rs`
- `core/engine/src/script.rs`
- `core/engine/src/spanned_source_text.rs`
- `core/engine/src/host_defined.rs`
- `core/engine/src/class.rs`
- `core/runtime/src/url.rs`
- examples under `examples/src/bin/*`

Why these are reference material instead of direct dependencies:

- they describe useful boundaries between parsing, lowering, and compiled representation
- they encode useful implementation techniques
- they are not themselves the source of the scheduler incompatibility
- but they still assume the surrounding Boa runtime model

What we should take from them:

- compiler pipeline structure
- code block layout ideas
- source location handling
- host-defined metadata patterns
- API surface examples for embedder-facing helpers

### Rewrite for Terrace

These parts of Boa are the actual runtime substrate and must be rewritten around Terrace's scheduler model:

- `core/gc/src/*`
- `core/engine/src/context/*`
- `core/engine/src/realm.rs`
- `core/engine/src/job.rs`
- `core/engine/src/module/*`
- `core/engine/src/vm/*`
- `core/engine/src/object/*`
- `core/engine/src/value/*`
- `core/engine/src/environments/*`
- `core/engine/src/native_function/*`
- `core/engine/src/builtins/promise/*`
- `core/engine/src/builtins/atomics/futex.rs`
- `core/runtime/src/lib.rs`
- `core/runtime/src/extensions.rs`
- `core/runtime/src/process/*`
- `core/runtime/src/interval.rs`
- `core/runtime/src/microtask/*`
- `core/runtime/src/message/*`

Why these need rewriting:

- they own runtime state, scheduling, or heap ownership
- they rely heavily on same-thread `Context` ownership
- they use thread-affine or same-thread-biased structures such as `Rc`, `RefCell`, `Gc`, and `GcRefCell`
- they assume a runtime that is driven from inside the engine rather than by the Terrace scheduler

### Broad file-by-file results

The following sections summarize the main rewrite surfaces and the broad code changes each one needs.

#### 1. Context and host integration

Primary files:

- `core/engine/src/context/mod.rs`
- `core/engine/src/context/hooks.rs`
- `core/engine/src/context/time.rs`
- `core/engine/src/host_defined.rs`
- `core/runtime/src/process/mod.rs`

Current role:

- own the top-level execution context
- store host hooks, clock, module loader, and job executor
- expose process/runtime data and host-defined state

Required conversion:

- replace same-thread `Rc` host integration with scheduler-facing runtime ownership
- model locale, timezone, env, cwd, and other environment values as explicit runtime configuration
- make runtime attachment and detachment explicit
- move host-defined runtime state out of ambient context-local storage and into scheduler-owned suspended state

#### 2. VM and execution state

Primary files:

- `core/engine/src/vm/mod.rs`
- `core/engine/src/vm/code_block.rs`
- `core/engine/src/vm/shadow_stack.rs`
- `core/engine/src/script.rs`
- `core/engine/src/native_function/continuation.rs`

Current role:

- own active call frames, stack, pending exception, and execution loop
- run bytecode to completion
- support async/generator/coroutine continuation behavior

Required conversion:

- expose bounded turn execution instead of one monolithic run loop
- define explicit safepoints
- split active execution state from suspended runtime state
- make suspended continuation state representable outside a live thread-local interpreter context
- keep compiled code blocks reusable while detaching them from a single attached runtime instance

#### 3. Jobs, promises, and async delivery

Primary files:

- `core/engine/src/job.rs`
- `core/engine/src/builtins/promise/mod.rs`
- `core/engine/src/object/builtins/jspromise.rs`
- `core/engine/src/native_function/mod.rs`
- `core/engine/src/builtins/atomics/futex.rs`
- `core/runtime/src/interval.rs`
- `core/runtime/src/microtask/mod.rs`
- `core/runtime/src/message/*`

Current role:

- manage promise jobs and timeout jobs
- bridge Rust async work into JS promises
- model message delivery and interval scheduling
- expose microtask and async behavior through the current context/job executor

Required conversion:

- move all job progression under host scheduler control
- represent timers, promise reactions, native async completions, and queued callbacks as scheduler-owned events
- replace context-captured async futures with host operation ids and completion delivery turns
- make interval, microtask, and message delivery use the same Terrace runtime event model as production sandboxes

#### 4. Modules and loading

Primary files:

- `core/engine/src/module/mod.rs`
- `core/engine/src/module/source.rs`
- `core/engine/src/module/synthetic.rs`
- `core/engine/src/bytecompiler/module.rs`
- `core/engine/src/script.rs`

Current role:

- parse modules
- load dependency graphs
- link and evaluate modules
- integrate module loading with promises and current context state

Required conversion:

- represent module loading as host-issued operations and completion events
- keep module graph and resolution state in scheduler-owned suspended state
- make module evaluation a sequence of explicit turns
- preserve compatibility with Terrace's Node loader path and deterministic source loading

#### 5. Object model, heap, and GC

Primary files:

- `core/gc/src/*`
- `core/engine/src/object/jsobject.rs`
- `core/engine/src/object/datatypes.rs`
- `core/engine/src/realm.rs`
- `core/engine/src/environments/*`
- `core/engine/src/value/*`

Current role:

- define object identity
- manage heap ownership and interior mutability
- store realm/module/environment state in GC-backed structures
- represent JS values and object wrappers

Required conversion:

- make the heap and object model compatible with turn-based attachment and detachment
- ensure suspended runtime state can move between worker threads at safepoints
- replace runtime-visible reliance on `GcRefCell`, `RefCell`, and thread-local GC ownership where it crosses the scheduler boundary
- keep object/value semantics intact while making ownership compatible with the new runtime model

This is the deepest conversion surface in the whole rewrite.

### Additional classification notes

#### `core/runtime`

`core/runtime` should be treated mostly as rewrite territory.

The crate is useful for:

- naming and surface-area inspiration
- understanding Boa's current embedding patterns
- understanding helper APIs layered above the engine

It should not be treated as a substrate because it assumes:

- a same-thread engine/context model
- runtime helpers that enqueue directly into the current context
- host/process/message helpers shaped around Boa's existing runtime ownership

The examples are still useful reference material:

- `examples/src/bin/tokio_event_loop.rs`
- `examples/src/bin/smol_event_loop.rs`
- `examples/src/bin/module_fetch_async.rs`

#### `core/gc`

`core/gc` is rewrite territory, not just adaptation territory.

The important issue is not just collector behavior. It is the ownership model:

- collector state is thread-local
- object access uses same-thread interior mutability assumptions
- the current design does not provide a scheduler-visible handoff protocol for moving a paused runtime between worker threads

That makes `core/gc` a foundational rewrite surface rather than an optimization pass.

It is important to distinguish two different designs:

- serialized access with a lock
- explicit pause, move, and resume at safepoints

A lock only provides mutual exclusion. It can ensure that at most one thread touches the collector at a time.

That is not sufficient for the Terrace runtime model, because the scheduler needs to be able to:

- pause a runtime at a safepoint
- detach its suspended state from one worker thread
- resume it on a different worker thread later

That requires an ownership-transfer protocol, not just contention control.

In practice, the GC layer must support:

- explicit quiescent safepoints
- no outstanding borrows at migration time
- collector ownership that can be re-homed across threads
- object access rules that remain valid after handoff

So the likely reuse boundary for `core/gc` is:

- reuse tracing and collector algorithm ideas where they fit
- rewrite the ownership and thread-handoff model around Terrace's scheduler contract

## Broad Code Changes Required

Across those file groups, the rewrite needs a few consistent architectural changes.

### A. Compile artifacts become scheduler-owned

Parser output, ASTs, and compiled code blocks should become immutable or effectively immutable runtime inputs.

They should not be tightly coupled to one live `Context` instance.

### B. Execution becomes turn-driven

The VM and module system need to expose:

- start turn
- run until safepoint
- emit outcome
- detach
- resume later

### C. Host interaction becomes event-driven

Filesystem, networking, timers, modules, and child-process interactions should be represented as:

- host request
- yield
- host completion
- resume turn

### D. Runtime ownership becomes thread-movable between turns

Long-lived runtime state must live in scheduler-owned, `Send` structures.

### E. Optional convenience APIs move above the runtime core

Runtime helpers such as:

- intervals
- microtasks
- message delivery
- process/environment helpers

should be rebuilt on top of the Terrace runtime contract rather than baked into the engine in a same-thread form.

## Terrace Code Changes

The new runtime requires corresponding changes in Terrace's JavaScript and sandbox layers.

### `terracedb-js`

[crates/terracedb-js/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/runtime.rs) will become the scheduler-facing runtime boundary.

Key changes:

- define a first-class turn API
- expose `Send` scheduler-facing futures
- separate suspended runtime state from engine-attached execution state
- model host completions, timer delivery, and microtask drains as explicit runtime events

Example shape:

```rust
enum JsRuntimeTurn {
    Bootstrap,
    EvaluateEntrypoint,
    DeliverTimer(TimerId),
    DeliverHostCompletion(OpId),
    DrainMicrotasks,
}

enum JsTurnOutcome {
    Completed(JsExecutionReport),
    PendingHostOp(OpId),
    PendingTimer(Deadline),
    PendingMicrotasks,
    Yielded,
}
```

### `terracedb-sandbox`

[crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs) will move long-lived runtime ownership onto scheduler-owned data.

Key changes:

- replace engine-local values in long-lived runtime structs with stable ids and host-owned metadata
- model host bindings as suspended operations plus completion events
- move callback identity and async resource tracking into scheduler-owned state
- make runtime attachment explicit instead of thread-local

[crates/terracedb-sandbox/src/session.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/session.rs) and related traits will align with the same scheduler-facing `Send` contract.

## Binding Model

Host bindings will follow a uniform pattern:

1. JavaScript issues an operation.
2. The host records the request in suspended runtime state.
3. The turn yields at a safepoint.
4. The host performs async work outside the engine.
5. A later turn resumes the runtime with the completed result.

This model applies to:

- filesystem bindings
- networking bindings
- child-process bindings
- dynamic import and module loading
- timers and task queues
- async hooks and promise-related callbacks

## Design Rules

The runtime should follow these rules consistently:

1. Scheduler-facing futures are `Send`.
2. JavaScript executes on one thread at a time.
3. Migration happens only between turns at safepoints.
4. Suspended state is host-owned and thread-movable.
5. Engine-attached state exists only during active execution.
6. Host-controlled effects remain deterministic and simulation-compatible.
7. Test execution uses the same runtime model as production.

## Migration Plan

### Phase 1: define the runtime contract

Status: complete in this document.

Write down the precise semantics for:

- turns
- safepoints
- callback identity
- promise and microtask behavior
- host-operation suspension and completion
- module evaluation boundaries

### Phase 2: refactor Terrace runtime boundaries

Status: complete in code.

Reshape `terracedb-js` and `terracedb-sandbox` around:

- turn/outcome APIs
- stable ids for suspended runtime resources
- explicit runtime attachment
- scheduler-owned suspended state

Implemented in:

- [crates/terracedb-js/src/types.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/types.rs)
- [crates/terracedb-js/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/runtime.rs)
- [crates/terracedb-js/src/boa.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/boa.rs)
- [crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs)

Phase 2 now provides:

- explicit runtime environment/configuration objects
- stable id types for timers, tasks, host ops, callbacks, promises, and modules
- explicit turn and turn-outcome types
- attachment and suspended-state snapshots on the JS runtime boundary
- sandbox-side storage of the last observed JS boundary state
- sandbox execution wired through `Bootstrap` and `EvaluateEntrypoint` turns rather than only a monolithic `execute(...)` call

Phase 2 does not yet replace the old engine internals. That remains Phase 3.

### Phase 3: build the new core runtime

Phase 3 is the engine rewrite proper. It should be executed in the following order.

#### Phase 3.a: heap and GC ownership model

Status: complete in code.

Define the new heap and collector substrate.

Deliverables:

- heap ownership model compatible with attach, detach, and later reattachment
- collector ownership model that is not tied to ambient thread-local state
- explicit safepoint rules for when GC work may run
- object identity rules across turns

Primary input material:

- `~/dev/boa/core/gc/src/*`
- `~/dev/boa/core/engine/src/object/*`
- `~/dev/boa/core/engine/src/value/*`

Implemented in:

- [crates/terracedb-js/src/engine/heap.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/engine/heap.rs)

Phase 3.a now provides:

- a heap substrate with explicit attach and detach
- collector state owned by the heap instead of thread-local globals
- stable object, root, weak, and ephemeron ids
- safepoint-gated collection and detach
- a collector snapshot and cycle report surface
- tests that reproduce the equivalent allocation, liveness, deep-chain, weak, and ephemeron behaviors for the new interface

#### Phase 3.b: context, realm, and environment ownership

Status: complete in code.

Replace Boa's same-thread context ownership with the Terrace attach/detach model.

Deliverables:

- attached engine context model
- detached suspended runtime state model
- realm ownership model
- host-defined state model
- explicit environment and policy injection path

Primary input material:

- `~/dev/boa/core/engine/src/context/*`
- `~/dev/boa/core/engine/src/realm.rs`
- `~/dev/boa/core/engine/src/host_defined.rs`

Implemented in:

- [crates/terracedb-js/src/engine/context.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-js/src/engine/context.rs)

Phase 3.b now provides:

- a `JsRuntimeContextBuilder` analogous to Boa's context builder
- explicit runtime configuration and service injection into detached context state
- stable realm ids and detached realm snapshots
- per-realm host-defined state, loaded-module tracking, and registered-class tracking
- explicit context attach and detach on top of the Phase 3.a heap substrate
- tests covering environment injection, realm creation and switching, host-defined persistence across detach/thread handoff, realm-scoped state, and realm root release

#### Phase 3.c: compile pipeline and immutable code artifacts

Build the compilation boundary that turns parser output into immutable scheduler-owned artifacts.

Deliverables:

- AST-to-bytecode lowering boundary
- immutable code block representation
- source map and source location metadata model
- artifact ownership rules independent of one attached runtime instance

Primary input material:

- `~/dev/boa/core/engine/src/bytecompiler/*`
- `~/dev/boa/core/engine/src/vm/code_block.rs`
- `~/dev/boa/docs/bytecompiler.md`
- `~/dev/boa/docs/vm.md`

Phase 3.c now provides:

- `crates/terracedb-js/src/engine/artifact.rs`
- an immutable compiled-artifact model for scripts, modules, and synthetic host modules
- stable code-block ids and instruction streams owned independently of any attached runtime
- lowering from `boa_parser` / `boa_ast` into Terrace code blocks
- compile-time tests for script artifacts, module request/export tracking, and synthetic module artifacts

#### Phase 3.d: turn-based VM execution

Implement the VM loop that runs bounded turns and yields only at legal safepoints.

Deliverables:

- frame and operand stack model
- turn entry and exit
- safepoint detection and detach boundary
- exception propagation across turns
- continuation representation for suspended execution

Primary input material:

- `~/dev/boa/core/engine/src/vm/*`
- `~/dev/boa/core/engine/src/native_function/continuation.rs`
- `~/dev/boa/core/engine/src/script.rs`

Phase 3.d now provides:

- `crates/terracedb-js/src/engine/runtime_engine.rs`
- a turn-based execution loop with explicit frame, stack, and safepoint handling
- attached-only heap allocation for runtime objects and intrinsics
- detachable execution state that survives between turns in `JsRuntimeSuspendedState`
- public `DeterministicJsRuntimeHost` cut over to the new engine path instead of the old fake deterministic executor

#### Phase 3.e: jobs, promises, and host-operation delivery

Rebuild async progression on top of scheduler-owned state instead of an engine-owned event loop.

Deliverables:

- promise reaction queues in suspended state
- explicit `DrainMicrotasks` behavior
- host-operation suspension and completion delivery
- timer/task queue integration with the Terrace scheduler

Primary input material:

- `~/dev/boa/core/engine/src/job.rs`
- `~/dev/boa/core/engine/src/builtins/promise/*`
- `~/dev/boa/core/runtime/src/microtask/*`
- `~/dev/boa/core/runtime/src/interval.rs`

Phase 3.e now provides:

- explicit promise bookkeeping in suspended state
- `PendingMicrotasks` turn outcomes and `DrainMicrotasks` follow-up turns
- `deliver_host_completion(...)` on the runtime boundary so host completions can be injected explicitly
- promise resolution and rejection across host completions instead of only through the `execute()` convenience loop
- engine tests covering cross-turn host completion delivery and promise-microtask progression

#### Phase 3.f: modules and loader integration

Rebuild module loading and module evaluation around explicit host-issued operations and turn boundaries.

Deliverables:

- module graph metadata in suspended state
- dependency loading through host ops
- dynamic import continuation model
- module evaluation turns
- `import.meta` and host-defined options reconstruction rules

Primary input material:

- `~/dev/boa/core/engine/src/module/*`
- `~/dev/boa/core/engine/src/script.rs`

Phase 3.f now provides:

- module graph state in suspended state
- synthetic host-module compilation for capability imports
- dependency loading through explicit pending operations instead of eager preloading before the first turn
- module-load completion decoding and registration on follow-up turns
- module execution ordering derived from the loaded dependency graph
- engine tests covering module execution with helper modules and host capability imports

#### Phase 3.g: Terrace integration cutover

Swap the old Boa-backed execution internals out from underneath the Phase 2 boundary.

Deliverables:

- `terracedb-js` backed by the new engine instead of the current Boa runtime
- `terracedb-sandbox` running against the new engine through the Phase 2 turn API
- removal of remaining thread-affine execution assumptions from the production path

Success criterion:

- the runtime boundary introduced in Phase 2 remains stable while the underlying engine implementation changes

Phase 3.g now provides:

- `terracedb-js::DeterministicJsRuntimeHost` cut over to `EngineJsRuntimeHost`
- removal of the old fake deterministic executor from `crates/terracedb-js/src/runtime.rs`
- `terracedb-sandbox` selecting the new engine for `JsCompatibilityProfile::TerraceOnly`
- `terracedb-sandbox` continuing to use the Boa path for `SandboxCompat` / Node-oriented sessions until the rewritten engine is ready for that compatibility layer

### Phase 4: reconnect Node compatibility

Run the existing Node compatibility plan on top of the new runtime substrate:

- upstream Node JavaScript
- Terrace-owned deterministic primitive bindings
- upstream tests and ecosystem canaries

### Phase 4.a: domain-backed scheduling and resource accounting

Integrate the JavaScript runtime and the simulation harness with TerraceDB's domain model so tests
and production use the same resource-control surface.

This phase exists because:

- simulation runs are intended to execute inside real applications, not only inside repo-local test
  binaries
- those applications already have domains and scheduler policy
- JavaScript, Node, and other simulated components need globally coordinated resource limits rather
  than independent local thread-pool limits

The important design rule is:

- the simulation harness is a client of domains
- it is not a second scheduler with its own separate resource model

The current harness-level `max_workers` style controls are acceptable as temporary safety valves,
but they are not the long-term resource interface.

Deliverables:

- a domain-facing runtime resource model in `terracedb-js`
- runtime allocation counters that are Terrace-owned rather than inferred from host RSS
- a simulation-harness admission path that acquires domain capacity before starting case work
- per-runtime memory budgets for JavaScript and Node sandboxes
- aggregate suite-level memory budgets across all in-flight runtimes in one simulation run
- explicit resource events so the domain can see allocation growth, release, and peak usage
- a shared control path that can be used from:
  - repo-local Rust tests
  - production Rust code
  - injected host APIs inside sandboxes

The JavaScript runtime must report a tracked-memory snapshot that is detailed enough for domain
enforcement and deterministic test assertions.

At minimum, that tracked-memory model must include:

- JavaScript heap / GC bytes
  - Boa GC managed objects while the Boa-backed path still exists
- context and runtime state
  - runtime struct state
  - realm and context records
  - environment records
  - module tables
  - callback, promise, and task id maps
- task-queue state
  - pending task storage
  - pending microtask queue storage, not only a count
  - pending host-completion storage
  - timer table storage
- compiled-code state
  - bytecode and code-block storage
  - constants pools
  - module artifacts
  - retained source-map or compile metadata when present
- parser and compiler retained state
  - retained compiler-owned artifacts that survive across turns
  - not every short-lived stack-local allocation unless it is cheap to track precisely
- module and cache state
  - module cache entries
  - resolved-specifier caches
  - retained source text when the runtime keeps it
  - package-resolution caches
- host-visible buffers owned by the runtime
  - strings
  - blobs
  - buffer backing stores
  - process stdio buffers when they are runtime-owned
- any other runtime-owned structures that materially affect admission and memory pressure

The Node compatibility layer in `terracedb-sandbox` must then map its own resource usage onto the
same accounting model, including:

- bootstrap and builtin module state
- CommonJS and ESM module caches
- `Buffer` / `ArrayBuffer`-backed external storage
- child Node runtimes created through Node-compatible process spawning
- process model data
- `async_wrap`, `task_queue`, and timer state
- stream and socket handle state
- inspector, performance, `module_wrap`, and `contextify` state tables
- Rust-side maps keyed by JavaScript object identity or runtime-owned ids

The simulation harness must evolve from:

- fixed worker-count admission

to:

- domain-backed admission
- domain-backed concurrency limits
- domain-backed aggregate memory limits
- per-case resource assertions

Per-case assertions should be expressed in Terrace-owned terms, for example:

- maximum heap bytes
- maximum context/runtime-state bytes
- maximum queue bytes
- maximum compiled-code bytes
- maximum module/cache bytes
- maximum external-buffer bytes
- maximum total runtime bytes
- maximum aggregate bytes across the suite

This is preferable to asserting host-process RSS because:

- RSS is not a stable simulation metric
- it mixes unrelated allocations
- it does not map cleanly to per-runtime or per-suite ownership

Implementation order:

1. Add explicit runtime resource counters and peak tracking to `terracedb-js`.
2. Thread those counters through `terracedb-sandbox` so Node runtimes report against the same
   model.
3. Extend the simulation harness so suite and case policies can declare memory budgets and resource
   assertions.
4. Replace harness-local concurrency admission with domain-backed admission.
5. Make Node compatibility runs and production-hosted sandbox execution use the same domain-backed
   path.

Success criterion:

- a simulation suite can cap in-flight Node and JavaScript runtime work through a domain policy
- per-case and aggregate memory assertions can fail deterministically using Terrace-owned counters
- the same resource-control path is used in repo tests and in production-hosted simulation runs

### Phase 5: portable runtime checkpoints

Extend the runtime and snapshot layers so Terrace can persist a paused JavaScript program together
with its filesystem substrate as one outer durable artifact.

Deliverables:

- a runtime checkpoint format for heap, realms, stacks, queues, timers, pending host operations,
  and scheduler-visible runtime metadata
- a resume-contract manifest that describes the minimum runtime and injected host bridge required
  to restore and resume the checkpoint safely
- a packaging model that can combine:
  - a VFS/base-layer snapshot
  - immutable compiled code artifacts
  - one or more paused runtime checkpoints
- explicit versioning rules for checkpoint compatibility
- checkpoint restore rules that only permit resume from legal safepoints
- virtualization requirements for external resources so resumed programs remain portable and
  deterministic

Design constraints:

- the filesystem snapshot, compiled artifacts, and runtime checkpoint remain separate internal
  sections even when shipped as one outer file
- the runtime checkpoint must carry an explicit resume contract rather than assuming ambient host
  defaults
- only Terrace-owned or virtualized resources may appear in a portable checkpoint
- checkpoints are more version-sensitive than plain VFS snapshots and must be validated explicitly
  on restore

The resume contract should include at least:

- engine and checkpoint schema versions
- the compatible `terracedb-js` version or version range required to satisfy the internal binding
  contract expected by the checkpoint
- the injected external API interface exposed by the parent Rust application, including the
  permissioned host capabilities made available through that bridge
- runtime environment expectations such as locale, timezone, cwd, argv, and process metadata shape
- identities or digests for the filesystem and compiled-artifact sections the checkpoint expects

Restore must validate this contract before resuming execution and fail closed on mismatch.

In particular, the resume contract is intended to validate the external seams that are not already
contained inside the filesystem or compiled-artifact sections. Examples include:

- the `terracedb-js` runtime version range that provides the internal binding behavior expected by
  the checkpoint
- the injected external API bridge exposed by the parent Rust application
- the permissioned host capability set available through that bridge, such as VFS, networking,
  subprocess, and time services
- the host-created process/bootstrap surface that is injected rather than loaded from the bundled
  filesystem layer

It is not intended to redundantly enumerate Node's internal JavaScript globals or builtins when
those are already supplied by the bundled filesystem and compiled-artifact layers.

Success criterion:

- Terrace can produce and restore a single portable artifact that contains both a virtual
  filesystem and a paused JavaScript runtime, together with the host contract needed to resume it,
  without relying on ambient host process state

## Summary

TerraceDB's JavaScript runtime will be:

- scheduler-compatible
- Tokio-friendly
- deterministic
- single-threaded while active
- migratable between turns
- compatible with the existing Node architecture

The runtime will reuse parser- and AST-level components where helpful and implement the core execution model around TerraceDB's scheduler and simulation requirements.
