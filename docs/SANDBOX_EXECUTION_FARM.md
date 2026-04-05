# Sandbox Execution Farm

## Goal

TerraceDB needs one production-grade sandbox runner abstraction that development tests also use.

The first immediate consumer is the Node/npm compatibility suite. The longer-term consumer is any
application that embeds TerraceDB and wants to expose development environments, package installs,
or repo-backed sandboxes in production.

This runner must:

- support thread-level parallelism when the runtime boundary is `Send`,
- share prepared Node/npm base layers efficiently across many sandbox forks,
- use the same execution-domain and scheduler interfaces that production code uses,
- remain simulation-testable under deterministic clocks, entropy, and scheduling,
- support efficient forking from a prepared base snapshot today and from a paused checkpoint later.

## Requirements

### 1. Production-first API

The runner is not a test-only helper.

Tests should call the same public abstraction that production code will use. The difference between
test and production should be configuration:

- deterministic scheduler vs production placement policy,
- in-memory VFS vs persistent VFS,
- vendored Node/npm base layer vs application-specific base layers,
- outer runner timeout policy.

### 2. Shared Base Preparation

Node and npm should be imported into a shared volume store exactly once per process/store.

After that, each sandbox case should open from an overlay fork:

- shared prepared base volume,
- fresh session volume,
- no re-import of the `.tdva` artifact,
- no repeated materialization of the Node/npm tree.

This same mechanism should later support:

- application repos preloaded into the base image,
- preinstalled dependency layers,
- paused runtime checkpoints as a base source.

### 3. Same Scheduler Contract as Production

The outer runner should be governed by the same resource-management interfaces already present in
TerraceDB:

- `ResourceManager`
- execution domains
- placement policy
- per-operation usage accounting

The runner should not invent a separate test-only concurrency model.

The inner guest runtime remains governed by `terracedb-js` scheduling:

- module loads,
- promise jobs,
- timers,
- host completions.

So there are two scheduling layers:

- process-level sandbox admission and placement through `ResourceManager`,
- per-runtime turn scheduling through `terracedb-js`.

### 4. One Process, Many Subcases

`nextest` uses process isolation. That means in-process shared setup only helps if many logical
cases run inside one Rust test process.

For heavy compatibility suites, the runner should therefore look like:

- one outer Rust integration test or runner binary,
- one shared harness and shared prepared base registry,
- many subcases executed inside that process,
- per-subcase timeout enforced with Tokio,
- a generous outer `nextest` timeout on the process itself.

## Target Architecture

### `SandboxExecutionFarm`

This is the top-level production abstraction.

It owns:

- a `SandboxHarness`,
- a shared `ResourceManager`,
- a prepared-base registry,
- fork/open helpers for new sandbox sessions.

It exposes:

- prepare a base layer once,
- inspect prepared base layers,
- open a fresh fork from a prepared base,
- later, submit many cases for scheduled execution.

### `SandboxPreparedBaseLayer`

This is the durable identity for a prepared base inside a specific volume store.

It records:

- a stable farm-local key,
- the source base-layer name,
- the shared base `VolumeId`.

It is intentionally small. It identifies prepared state already present in the store.

### `Sandbox Fork`

A fork is:

- one prepared base volume,
- one fresh session volume,
- one `SandboxConfig`.

Forking is the production primitive that both test cases and interactive development environments
should use.

That keeps the model consistent:

- compatibility test case = fork + execute,
- dev environment tab = fork + keep session open,
- install/build snapshot pipeline = fork + run commands + freeze.

### `SandboxExecutionFarmRunner`

This is a higher-level layer that will sit on top of `SandboxExecutionFarm`.

It will:

- accept batches of sandbox cases,
- acquire execution-domain leases through `ResourceManager`,
- map cases onto Tokio worker tasks when the runtime boundary is `Send`,
- enforce per-subcase timeouts,
- aggregate results deterministically in simulation mode.

This runner is not implemented yet. The foundational work starts with shared base preparation and
forking, because that substrate is required in both tests and production.

## Execution Model

### Deterministic Simulation Mode

Simulation mode uses:

- deterministic clocks,
- deterministic entropy,
- deterministic runtime backend,
- deterministic placement and resource accounting,
- the same farm API.

This mode exists so the farm itself can be simulation tested:

- same-seed replay,
- timeout behavior,
- cancellation,
- concurrency and admission policy,
- base-layer reuse,
- checkpoint fork semantics later.

### Parallel Production Mode

Production mode uses:

- the host Tokio runtime,
- the same `ResourceManager` and placement contracts,
- the same prepared-base registry,
- the same fork primitive.

The only intended structural difference is that work can actually run on many worker threads.

That requires the sandbox execution boundary to be `Send`. The farm should be designed for that
target now instead of baking in the current `?Send` limitation.

## Node/npm Compatibility Suite Plan

The initial runner use case is the Node/npm compatibility suite.

The shape should be:

1. Start one test-process-local `SandboxExecutionFarm`.
2. Prepare the combined Node/npm base layer once.
3. Generate or enumerate upstream test files as subcases.
4. For each subcase:
   - open a fresh fork from the prepared base,
   - write any per-case fixtures,
   - run the Node command,
   - enforce a per-subcase timeout with Tokio,
   - collect stdout/stderr/exit code and module trace.
5. Fail the outer test process only after aggregating all subcase failures.

That avoids the current problem where each upstream test becomes its own `nextest` process and
pays startup overhead independently.

## Migration Plan

### Stage 1

Land the production primitives:

- `SandboxExecutionFarm`
- `SandboxPreparedBaseLayer`
- shared base preparation
- fork/open helpers

This is foundational and useful immediately.

### Stage 2

Move heavy compatibility suites off one-test-per-process execution and onto one-process custom
runner harnesses built on top of the farm.

That includes:

- generated upstream Node tests,
- npm CLI compatibility batches,
- later, selected package contract suites.

### Stage 3

Add scheduled batch execution on top of the farm once sandbox/runtime execution is `Send`.

That runner should:

- use `ResourceManager` for admission,
- expose per-subcase timeout policy,
- support deterministic simulation mode and production mode through the same API.

### Stage 4

Extend the same farm to support snapshot/checkpoint forks:

- frozen filesystem-only base layers,
- paused runtime checkpoints,
- application-specific development environment images.

## Relationship to Existing Code

The current code already has most of the substrate:

- `SandboxBaseLayer`
- `SandboxSnapshotRecipe`
- `SandboxHarness`
- `SandboxExecutionRouter`
- `ResourceManager`

What is missing is the production-facing composition layer that treats these as one coherent
sandbox execution service rather than separate helper APIs.

That is what this farm abstraction is for.
