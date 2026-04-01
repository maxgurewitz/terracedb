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
- [x] Make the current vs historical admission shape explicit so tests can
  distinguish live pressure from the last non-open reason.
- [x] Sweep remaining scheduler observability tests onto subscription/event
  helpers where they still rely on incidental yields.
- [x] Delete any old reader-side helpers that reconstruct scheduler state from
  locks once the published snapshot path is fully in place.

## T76b: Remaining Blocking Inspection Surfaces

- [x] Classify every remaining synchronous snapshot/introspection helper as
  one of:
  - operator-only blocking read,
  - runtime-safe published snapshot,
  - ordered event stream, or
  - bounded progress probe.
- [ ] Convert the highest-value runtime-safe surfaces first, in this order:
  - [x] DB progress snapshots/subscriptions for in-flight sequence visibility.
  - [x] Resource-manager snapshots into subscription-backed reads for active
    domain/budget assertions.
  - [x] Columnar cache usage into published immutable state instead of lock-backed
    reconstruction.
  - [x] Watermark/progress reads where they are really transition notifications
    rather than static summaries.
- [ ] Replace retry loops and incidental `yield_now()` polling with direct
  observation helpers:
  - event streams when the tests care about ordered transitions,
  - published snapshots when they care about the latest state, and
  - poll/step helpers only when the test is asserting background progress.
- [ ] Remove old blocking helpers once the new surface is available; do not
  keep compatibility shims in the test path.
- [x] Extend the debugging guide with the preferred simulation-safe
  observation patterns and examples of when to choose snapshots versus
  subscriptions versus progress probes.
- [x] Add remote cache / prefetch progress events so range-cache and dedupe
  tests can wait on explicit in-flight claims and completion instead of ad hoc
  sleeps.
- [x] Replace failpoint helpers that depend on repeated `yield_now()` polling
  with event-driven or bounded progress helpers.

## T76c: Deterministic Progress Probes

- [ ] Identify the background maintenance/scheduler loops whose progress is
  still only inferable through sleeps, yields, or eventual side effects.
- [ ] Add explicit progress probes in the form of:
  - `poll_*` helpers for one-step state advancement,
  - `run_until_idle` / bounded drain helpers for background work, and
  - `wait_for_*` helpers only when they can be implemented without blocking
    the runtime thread.
- [ ] Rewrite representative simulations to use those probes instead of
  timing-based heuristics.
- [ ] Keep progress probes separate from published snapshots so tests do not
  accidentally turn a background-liveness check into a state-sampling check.
- [ ] Include `direct_backlog()`-style lock-backed helper reads here only when
  they are truly progress probes, not when a published subscription can carry
  the same signal.

## Highest-Value Follow-Ups

- [ ] [src/test_support.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/test_support.rs):
  replace the remaining `yield_now()`-driven clock-stepping helpers with a more
  explicit bounded progress-probe surface so liveness tests do not hide their
  scheduling assumptions inside ad hoc executor yields.
