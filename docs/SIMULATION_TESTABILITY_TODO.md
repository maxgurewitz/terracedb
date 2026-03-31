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

## T76c: Deterministic Progress Probes

- [ ] Identify the background maintenance/scheduler loops whose progress is
  still only inferable through sleeps, yields, or eventual side effects.
- [ ] Add explicit poll/step or bounded "wait until idle" helpers where that
  can be done without weakening production invariants.
- [ ] Rewrite representative simulations to use those probes instead of
  timing-based heuristics.

## Highest-Value Follow-Ups

- [ ] [src/api/watermark.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/watermark.rs):
  review table watermark inspection for the same published-state vs blocking
  split.
- [ ] [src/api/db_api.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/db_api.rs):
  audit other synchronous snapshot-style surfaces such as cache or maintenance
  stats that simulations may want to sample mid-flight.
- [ ] [tests/simulation_harness.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/tests/simulation_harness.rs):
  extend whole-system scenarios so they assert observability transitions
  through subscriptions/events, not only end-of-test snapshots.
