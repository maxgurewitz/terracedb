# Guidelines

This document is the catch-all coding-conventions reference for Terracedb.
Use it as the default policy layer for new code, tests, examples, and refactors.

Specialized documents still matter:

- [ARCHITECTURE.md](./ARCHITECTURE.md) explains the intended system shape.
- [DEBUGGING_GUIDE.md](./DEBUGGING_GUIDE.md) explains how to debug deterministic
  simulation failures and choose the right observation surface.
- [DURABLE_FORMATS.md](./DURABLE_FORMATS.md) captures on-disk and durable-format
  contracts.
- [TASKS.md](./TASKS.md) tracks the dependency-aware implementation plan.

When this document conflicts with an older local pattern in the repo, prefer
this document unless a nearby comment or test clearly documents an intentional
exception.

## Core Principles

1. Prefer one clear code path over compatibility shims.
2. Keep behavior deterministic when the product does not require nondeterminism.
3. Make the product surface observable through explicit publications, not by
   forcing tests to sample mutable internals.
4. Treat persistence, recovery, and visibility boundaries as first-class
   contracts.
5. Hold example apps to the same engineering standards as library code.

Terracedb is a greenfield project. Backward compatibility is not a reason to
keep ambiguous, duplicate, or legacy code paths alive. When a better design is
ready, prefer replacing the old path and updating callsites rather than adding
`legacy`, `v2`, `compat`, or similar suffixes.

## API And Design Conventions

- Favor explicit, intention-revealing APIs over convenience wrappers that hide
  ordering, durability, or visibility semantics.
- If a helper may replay caller code, make that replay contract explicit in the
  API shape. Retry loops that may rerun callbacks on OCC conflicts must require
  an explicit replay-safe opt-in and the callback body must stay transaction-local.
- Keep "current state" and "historical state" separate in names and types.
  If both matter, expose both explicitly.
- Use immutable published snapshots or ordered event streams for observability.
  Avoid reader-side reconstruction of live state from multiple mutable locks.
- If an API may block, make that obvious in how it is used and keep it out of
  async runtime-critical paths.
- If a runtime already owns the concept being observed, expose a runtime-level
  wait surface instead of forcing callers to poll tables manually.

## Dependency Injection And Side Effects

All filesystem, object-store, clock, and randomness access must flow through
injected traits.

Rules:

- Do not construct ambient production dependencies inside lower-level logic.
- Keep clock and RNG usage explicit so tests and simulations can replay exactly.
- Avoid hidden global state.
- If a feature depends on external I/O, model that dependency at the boundary so
  it can be stubbed, simulated, and fault-injected.

## Async, Concurrency, And Locking

- Do not block the async runtime thread on mutexes or other synchronous waiting
  primitives when a published or async-friendly path can be used instead.
- Avoid reaching into shared mutable state from tests when a subscription,
  event stream, or explicit progress probe can carry the same signal.
- Hold locks for the shortest possible time and never across unrelated waits.
- Prefer publish-on-change state for observability over repeated reader-side
  locking.

## Durable State And Recovery

Any change that affects ordering, visibility, durability, recovery, or replay
must be treated as a contract change.

Required practice:

- Add or update crash/recovery coverage in the same change.
- Keep durable-format fixtures and round-trip tests current.
- Fail closed on corruption, unsupported versions, or missing required durable
  metadata.
- Preserve the distinction between visible state, durable state, and historical
  state in both code and tests.

## Testing Conventions

- Prefer deterministic simulation coverage whenever the behavior can be modeled
  there.
- If a non-simulation bug can be captured faithfully in simulation, add the
  simulation regression and iterate there first.
- As a rule, keep simulation coverage to one seed per test so the test runner
  can parallelize seeds across independent tests. If multiple seeds matter,
  split them into separate tests instead of serializing them inside one test
  body unless the assertion itself fundamentally depends on cross-seed
  comparison.
- Use focused unit tests for local semantics and deterministic simulation tests
  for ordering, concurrency, recovery, and fault scenarios.
- Keep tests explicit about what semantic boundary they are asserting:
  visibility, durability, historical replay, scheduler behavior, cache state,
  workflow progress, and so on.
- Avoid assertions that depend on incidental task timing.

For changes in persistence or runtime behavior, the default expectation is:

1. unit coverage for the local rule,
2. recovery coverage if durability is involved, and
3. simulation coverage if concurrency, timing, or distributed behavior matters.

## Simulation Testability

This section captures the repo conventions that keep code friendly to
single-threaded deterministic simulation runtimes.

### Observation Rules

- Subscribe before triggering the work you want to observe.
- Wait on published snapshots, event streams, watermark receivers, or explicit
  runtime wait surfaces.
- Use synchronous snapshot helpers only for quiescent post-condition reads.
- If the test cares about ordered transitions, use an event stream.
- If the test cares about the latest state, use a published snapshot
  subscription.
- If the test cares about background liveness, use a bounded progress probe.

### What Not To Do

- Do not write tests that loop on `tokio::task::yield_now()` waiting for
  something to happen.
- Do not use `sleep(...)` as a readiness guess, catch-up guess, or failure
  propagation guess.
- Do not sample mutable shared state when a real publication edge already
  exists.
- Do not add simulation-only backdoors when the product surface can expose the
  needed observation cleanly.

### Allowed Exceptions

`sleep(...)` and `yield_now()` are still acceptable when they are the subject of
the test or an intentional part of the scenario, for example:

- simulated time travel for recurring schedules,
- explicitly modeled slow responses or partitions,
- bounded progress-probe internals, or
- scheduler/fairness tests that deliberately create a timing window.

If a `sleep(...)` or `yield_now()` is not itself part of the behavior under
test, treat it as a smell and look for a published observation surface instead.

### Runtime Wait Surface Conventions

When a runtime owns the semantic concept, expose wait helpers on that runtime or
handle rather than making every caller rediscover the backing-table protocol.

Examples:

- workflow state and source progress waits belong on workflow runtimes,
- projection frontier and terminal waits belong on projection handles,
- server readiness belongs on simulated servers or app-level ready signals,
- relay/projection output visibility should use watermark publications.

### Example App Requirement

Example apps must follow the same simulation-testability rules as libraries:

- publish readiness explicitly,
- publish or expose progress explicitly,
- avoid startup sleeps and polling loops, and
- keep simulation assertions on real product-facing observation paths.

## Observability Conventions

- Publish immutable snapshots for "what is true now?"
- Publish ordered events for "what happened in what order?"
- Keep current state and last non-open or historical breadcrumbs separate.
- Timestamp or otherwise contextualize historical breadcrumbs when staleness
  matters.
- Prefer observation APIs that the product itself can reasonably support, not
  test-only reconstructions of internal state.

## Naming And Code Shape

- Prefer names that encode semantics, not implementation accidents.
- Avoid vague suffixes like `new`, `legacy`, `compat`, or `v2`.
- Delete superseded helpers and call paths once the replacement is in place.
- Keep helper APIs small and composable; avoid adding one-off wrappers when the
  right fix is to expose a reusable primitive.

## Example And Library Parity

- Do not accept lower standards in examples because they are "just examples."
- If an example demonstrates a pattern, that pattern should be one we want users
  to copy.
- If a library gains a better readiness, progress, or observability surface,
  migrate the examples to use it.
- Treat table bootstrap APIs as part of that pattern surface:
  `Db::ensure_table` means the persisted definition must still match exactly,
  while `get_or_create_table_by_name` is the explicit escape hatch for tables
  whose persisted definition is expected to evolve, such as resharded tables.

## Documentation Maintenance

When a refactor changes repo-wide practice:

- update this file,
- update the specialized doc that explains the detailed workflow,
- update task or roadmap docs if the remaining work changed shape, and
- remove stale claims that say a sweep is complete when downstream consumers
  still lag behind.

The goal is not maximum documentation volume. The goal is that the docs tell
the truth about the current preferred way to build and test the system.
