# Simulation Testability Todo

This tracks the refactor from blocking, reader-assembled observability toward
published immutable state and event-driven simulation assertions.

## Completed

- [x] Fix per-write admission observability semantics.
- [x] Make current vs historical admission explicit.
- [x] Add simulation regressions for strongest-per-write diagnostics and
  single-count stalled writes.
- [x] Keep whole-system tests in the default nextest profile while excluding
  them from the `pre-commit` profile.

## In Progress

- [x] Publish scheduler observability as immutable snapshots.
- [x] Expose a subscription API for simulation tests instead of retrying a
  blocking snapshot path.
- [ ] Migrate remaining scheduler observability simulation tests to subscribe
  and wait on predicates rather than relying on incidental yields.

## Next

- [ ] Add a dedicated admission event stream so simulations can assert ordered
  transitions, not just sampled snapshots.
- [ ] Convert other test-only polling helpers to subscription or event-driven
  helpers where the product surface already has a natural change boundary.
- [ ] Add a liveness regression that proves observability reads continue to make
  progress while writes and maintenance mutate state concurrently.
- [ ] Audit synchronous snapshot/introspection APIs that are still reachable
  from simulation and split them into operator-only vs runtime-safe paths.

## Blocking Hotspots To Refactor

- [ ] [src/api/watermark.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/watermark.rs):
  move table watermark reads toward published snapshots or subscription-first
  helpers instead of mutex-backed sampling.
- [ ] [src/api/db_api.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/db_api.rs):
  review other synchronous snapshot-style APIs such as compaction/filter stats
  and cache-usage inspection for simulation-safe read paths.
- [ ] [src/api/internals.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/src/api/internals.rs):
  continue replacing mutex-assembled diagnostics state with published immutable
  snapshots where tests need concurrent inspection.
- [ ] [tests/simulation_harness.rs](/Users/maxwellgurewitz/.codex/worktrees/7dfb/terracedb/tests/simulation_harness.rs):
  extend the whole-system simulations to assert observability transitions using
  subscriptions instead of point-in-time sampling after manual yields.

## Stretch

- [ ] Add step/poll helpers for background maintenance so simulations can drive
  scheduler progress explicitly without sleeps.
- [ ] Revisit remaining async-reachable `parking_lot` and `std::sync` locks and
  document which ones are runtime-internal versus user/test-facing.
