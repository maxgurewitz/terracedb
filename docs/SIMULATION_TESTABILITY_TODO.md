# Simulation Testability Todo

This tracks the current refactor from blocking, reader-assembled inspection
paths toward published immutable state, event streams, and deterministic
simulation probes.

## T76a: Published Observability

- [x] Publish scheduler observability as immutable snapshots.
- [x] Expose a scheduler observability subscription API for simulation tests.
- [x] Keep the synchronous snapshot API as a thin clone of published state.
- [x] Add an admission observation stream for ordered write-level transitions.
- [x] Add simulation regressions for `RateLimit -> Open` admission ordering.
- [x] Add a simulation regression proving one admission event per logical
  multi-table write.
- [x] Add a unit regression proving the synchronous snapshot matches the
  published subscription state after representative updates.
- [ ] Sweep remaining scheduler observability tests onto subscription/event
  helpers where they still rely on incidental yields.

## T76b: Remaining Blocking Inspection Surfaces

- [ ] Audit synchronous snapshot/introspection APIs still reachable from
  simulation and classify them as operator-only or runtime-safe.
- [ ] Convert the highest-value remaining surfaces to published snapshots,
  subscriptions, or explicit poll/step helpers.
- [ ] Remove test-only retry loops that only exist to tiptoe around blocking
  inspection.
- [ ] Extend the debugging guide with the preferred simulation-safe
  observation patterns.
- [ ] Prioritize DB progress subscriptions so async and simulation tests stop
  sampling `current_sequence()` / `current_durable_sequence()` directly when
  they need in-flight visibility.
- [ ] Migrate the remaining resource-manager snapshot callsites to the
  published subscription path and reserve synchronous snapshots for static
  topology assertions.
- [ ] Publish columnar cache usage observability so the public usage snapshot
  no longer reconstructs live state from locks.
- [ ] Replace the failpoint helpers that depend on repeated `yield_now()`
  polling with event-driven or bounded progress helpers.
- [ ] Add remote cache / prefetch progress events so range-cache and dedupe
  tests can wait on explicit completion instead of ad hoc sleeps.

## T76c: Deterministic Progress Probes

- [ ] Identify the background maintenance/scheduler loops whose progress is
  still only inferable through sleeps, yields, or eventual side effects.
- [ ] Add explicit poll/step or bounded "wait until idle" helpers where that
  can be done without weakening production invariants.
- [ ] Rewrite representative simulations to use those probes instead of
  timing-based heuristics.
- [ ] Include `direct_backlog()`-style lock-backed helper reads here only when
  they are truly progress probes, not when a published subscription can carry
  the same signal.

## Highest-Value Follow-Ups

- [ ] [src/api/db_api.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/db_api.rs):
  finish migrating async tests away from direct DB progress sampling and onto
  the new progress subscription.
- [ ] [tests/execution_domain_contracts.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/tests/execution_domain_contracts.rs):
  replace remaining resource-manager polling patterns with subscription-based
  waits where assertions are about in-flight state.
- [ ] [src/api/sstable_io.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/sstable_io.rs)
  and [src/api/internals.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/internals.rs):
  publish columnar cache usage state instead of assembling it from live locks.
- [ ] [tests/simulation_harness.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/tests/simulation_harness.rs):
  extend whole-system scenarios so they assert observability transitions
  through subscriptions/events, not only end-of-test snapshots.
- [ ] [src/api/watermark.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/watermark.rs):
  review watermark/progress inspection for the same published-state vs
  blocking split where it still matters for simulation tests.
