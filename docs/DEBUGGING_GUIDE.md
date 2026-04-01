# Debugging Guide

Terracedb's simulation stack is deterministic on purpose. When a bug only shows
up under replay, the usual fastest path is:

1. Re-run the exact failing seed.
2. Turn on tracing and make sure spans survive every async boundary.
3. If the trace is still too wide, add a Turmoil barrier at the exact decision
   point you want to inspect.

For the broader simulation model, see [ARCHITECTURE.md](./ARCHITECTURE.md).

## Reproduce First

Most of the simulation-facing APIs in this repo are already seed-driven:

- `SeededSimulationRunner::new(seed)` in `crates/terracedb-simulation`
- the `turmoil-determinism` helpers in `crates/turmoil-determinism`
- explicit per-test seed loops in `tests/application_simulation.rs`,
  `tests/simulation_harness.rs`, and `tests/turmoil_determinism.rs`

Before adding new instrumentation, reduce the failure to a single seed and keep
that seed in the failing assertion message or failure report.

`SimulationFailureReport` in `crates/terracedb-simulation/src/lib.rs` already
captures the high-value context you usually want first:

- `seed`
- injected faults
- simulation checkpoints
- the recorded trace

That is the cheapest debugging loop we have: reproduce exactly, inspect the
trace, then add narrower instrumentation only if the trace still leaves multiple
plausible explanations.

## Prefer A Simulation Repro For Non-Simulation Failures

If a failing test is not already a simulation test, the usual next step should
be to add one.

The goal is not "write a nearby simulation test that happens to pass." The goal
is "capture the same failure mode in the cheapest deterministic harness we
have."

Use this loop:

1. Start from the original failing test and identify the smallest behavior that
   still looks broken.
2. Add a simulation test that exercises that behavior through the simulation
   path closest to the production code you expect is wrong.
3. Confirm that the new simulation test fails in an equivalent way. If it
   passes immediately, assume the repro is incomplete and tighten it until it
   really captures the bug.
4. Iterate on the implementation using the simulation test first. That is
   usually faster, cheaper, and more deterministic than repeatedly re-running
   the original non-simulation test.
5. Return to the original test and verify that it now passes too.
6. If the original test still fails, treat that as evidence that the simulation
   repro is still missing part of the real behavior. Amend the simulation test,
   then repeat the loop.

In practice this usually produces a better fix, because the simulation test
forces us to state what behavior is actually broken instead of only chasing a
slow or noisy harness symptom.

## Prefer Published Updates Over Sampling Shared State

When a simulation needs to observe live runtime state, prefer the published
subscription or event-stream APIs over point-in-time shared-state sampling.

For scheduler/admission observability, the default pattern should now be:

1. Subscribe first.
2. Trigger the work you want to observe.
3. Wait on the subscription or event stream until the predicate you care about
   becomes true.
4. Only fall back to a one-shot synchronous snapshot when you are inspecting
   quiescent state after the interesting work is already complete.

In practice that means:

- use `Db::subscribe_scheduler_observability()` when the test wants an
  eventually consistent view of the published observability snapshot;
- use `Db::subscribe_admission_observations()` when the test cares about the
  order of admission transitions such as `RateLimit -> Open` or "one event per
  logical write"; and
- use `Db::scheduler_observability_snapshot()` for post-condition assertions
  once the system is idle, not as the primary in-flight observation mechanism.

This keeps the simulation aligned with the product surface and avoids
re-introducing hidden dependencies on mutex timing or reader-side state
assembly.

## Choose The Right Observation Surface

When a simulation or async test needs to "see what the system is doing," choose
the narrowest published surface that matches the question you are asking.

### Use Latest-State Snapshots For "What Is True Right Now?"

Use a published snapshot subscription when the test cares about the latest
visible state, not the precise order of transitions:

- `Db::subscribe_progress()`
  Use this for database sequence frontiers.
  `DbProgressSnapshot` now carries `current_sequence`, `durable_sequence`, and
  `reserved_sequence`, so blocked group-commit tests can observe reserved
  in-flight work without polling `db.inner.next_sequence`.
- `Db::subscribe_scheduler_observability()`
  Use this for domain-level throttling/forced-flush counters and current vs
  last-non-open admission diagnostics.
- `Db::subscribe_resource_manager()`
  Use this for execution-domain usage, backlog, and budget assertions.
- `Db::subscribe_columnar_cache_usage()`
  Use this for cache partition occupancy and published cache-usage deltas.
- `RemoteCache::subscribe_progress()`
  Use this for warmed cache entries plus in-flight remote reads/prefetches.
  The published snapshot now includes both stored entries and in-flight claims
  with fetch kind and waiter count.

The preferred test shape is:

1. Subscribe before triggering the work.
2. Trigger the work.
3. `wait_for(...)` the published predicate.
4. Assert on the returned snapshot.

### Use Event Streams For Ordered Transitions

If the test cares about "what happened first?" rather than "what is true now?",
use an event stream instead of a latest-state snapshot.

- `Db::subscribe_admission_observations()`
  Use this for ordered write-admission transitions such as `RateLimit -> Open`,
  "one observation per logical write," or "recovery cleared the current
  diagnosis after the throttled write."
- Watermark receivers (`subscribe`, `subscribe_durable`, and their `changed()`
  APIs)
  Use these when the semantic question is monotonic publication order for a
  table rather than a whole-DB status summary.

### Use Runtime Wait Surfaces For Workflow And Projection Progress

When the runtime already owns the semantic concept you care about, prefer its
wait surface over ad hoc table polling:

- `WorkflowRuntime::wait_for_state(...)`
  Use this when a workflow test cares about a particular instance state value.
- `WorkflowRuntime::wait_for_source_progress(...)`
  Use this when a workflow test cares about bootstrap or recovery progress
  rather than the final state row.
- `WorkflowRuntime::wait_for_telemetry(...)`
  Use this when the assertion is phrased in terms of attach mode, lag, or other
  published workflow telemetry.
- `RecurringWorkflowRuntime::wait_for_state(...)`
  Use this for recurring tick-count and next-fire assertions.
- `ProjectionHandle::wait_for_watermark(...)` / `wait_for_sources(...)`
  Use these for projection catch-up assertions.
- `ProjectionHandle::wait_until_terminal()` and `WorkflowHandle::wait_until_terminal()`
  Use these for fail-closed and failpoint cases instead of sleeping and then
  calling `shutdown()`.

If a library does not yet expose a native wait surface, wait on the backing DB
publication channel that owns the truth:

- use table watermark subscriptions for relay and projection outputs; and
- use `TerracedbSimulationHarness::wait_for_change(...)` /
  `wait_for_visible(...)` when a simulation already has a harness and the
  product surface does not yet expose a narrower publisher.

The rule is the same either way: react to a real publication edge, not to
elapsed simulated time.

### Use Bounded Progress Helpers For Liveness

Some tests are not about state at all; they are about whether background work
or delayed work eventually makes progress. In those cases, prefer a bounded
progress helper over sleeps or incidental yields:

- `Db::run_next_scheduled_work()` / `Db::wait_for_scheduler_idle(...)`
  Use these for scheduler-driven flush/compaction/offload progress when the
  test needs to assert "one background step happened" or "the scheduler is now
  idle" without open-coding trigger writes, repeated polling, or yield loops.
- `ClockProgressProbe`
  Use this when a test needs bounded virtual-time advancement to drive a task,
  wait for a failpoint, or make a single explicit clock/progress step without
  open-coding `yield_now()` loops.

If a test has to call `tokio::task::yield_now()` in a loop to see whether
something happened, treat that as a smell. Usually the right fix is to expose a
published snapshot, an event stream, or a bounded progress helper instead.

### Make Example Simulations Explicit About Readiness

Example-app simulations should follow the same rules as library tests:

1. Publish a ready signal once the server or runtime can actually answer work.
2. Have the client host wait on that signal before issuing requests.
3. Use app/runtime progress signals for projections, planners, or workflows.
4. Reserve `sleep(...)` only for deliberate scenario time travel, such as
   advancing a recurring schedule or modeling a slow response.

### Keep Synchronous Snapshots For Quiescent Reads

The synchronous snapshot methods are still useful, but mostly after the
interesting work has already completed:

- `Db::scheduler_observability_snapshot()`
- `Db::progress_snapshot()`
- `RemoteCache::progress_snapshot()`

Use these for post-condition assertions or operator/debug surfaces when the
system is idle. Avoid making them the primary observation mechanism for in-flight
simulation behavior when a published subscription already exists.

### Current Snapshot Classification

The main snapshot/introspection helpers in Terracedb currently fall into these
categories:

- Runtime-safe published snapshots
  `Db::progress_snapshot()`, `Db::resource_manager_snapshot()`,
  `Db::columnar_cache_usage_snapshot()`, `Db::scheduler_observability_snapshot()`,
  and `RemoteCache::progress_snapshot()`.
  These are synchronous reads, but they clone already-published immutable state
  rather than reconstructing live state under reader-side locking.
- Ordered event streams
  `Db::subscribe_admission_observations()` plus table watermark receivers.
  Use these when ordering matters more than the latest state.
- Bounded progress helpers
  `Db::run_next_scheduled_work()`, `Db::wait_for_scheduler_idle(...)`, and
  `ClockProgressProbe`.
  These are for liveness and explicit stepping, not for in-flight state
  sampling.
- Operator/debug-only reads
  `Db::scheduler_progress_snapshot()` plus anything else that still reaches
  directly into quiescent state for one-off inspection rather than
  participating in the published subscription surfaces.
  Prefer not to introduce new async tests that depend on these when a published
  surface already exists.

## Use Tracing As The First Debugger

Turmoil already emits useful network/runtime events through `tracing`. Upstream
documents these under the `turmoil` target, including packet-level `Send`,
`Delivered`, `Recv`, `Drop`, `Hold`, and simulation step events.

For one-off debugging sessions, install a subscriber in the test crate and turn
up logging with `RUST_LOG`. A good first pass is:

```bash
RUST_LOG=info,turmoil=trace cargo test <test-name> -- --nocapture
```

That gives you Terracedb's own spans plus Turmoil's packet-level view of the
simulation.

### Prefer Simulated Time In Log Output

Real wall-clock timestamps are much less helpful than logical simulation time.
The upstream Turmoil examples show this pattern, and this repo already exposes a
helper in `crates/turmoil-determinism/src/time.rs`.

```rust
use tracing_subscriber::fmt::time::FormatTime;

#[derive(Clone)]
struct SimElapsedTime;

impl FormatTime for SimElapsedTime {
    fn format_time(
        &self,
        w: &mut tracing_subscriber::fmt::format::Writer<'_>,
    ) -> std::fmt::Result {
        tracing_subscriber::fmt::time()
            .format_time(w)
            .and_then(|()| {
                write!(
                    w,
                    " [{:?}]",
                    turmoil_determinism::time::sim_elapsed_or_zero()
                )
            })
    }
}
```

Attach that timer to `tracing_subscriber::fmt()` and the log becomes much easier
to line up with sleeps, retries, scheduler decisions, and crash/restart points.

## Keep Context Across Async Boundaries With Tracing Traits

In this codebase, tracing usually becomes confusing at spawn boundaries rather
than at the original callsite. Two extension traits from `tracing` are the
important tools here:

- `Instrument` attaches a span to a future.
- `WithSubscriber` keeps the current tracing dispatch attached when a future is
  spawned elsewhere.

We already use this pattern in a few key places:

- `src/api/db_open.rs`
- `src/composition.rs`
- `crates/terracedb-projections/src/lib.rs`
- `crates/terracedb-workflows/src/lib.rs`

The core idea is:

```rust
use tracing::{Instrument, instrument::WithSubscriber};

let span = tracing::info_span!("terracedb.workflow.runtime", workflow = %name);
let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());

let task = tokio::spawn(
    async move {
        run_workflow_runtime(runtime, shutdown_rx).await
    }
    .instrument(span.clone())
    .with_subscriber(dispatch),
);
```

This matters because a missing span or subscriber often looks like "the task
stopped logging" when the real problem is "the task crossed a spawn boundary and
lost its context".

### When To Reach For Each Trait

- Use `Instrument` when you want every event inside a future to stay nested under
  a span you control.
- Use `WithSubscriber` when the future may run in a spawned task, `JoinSet`, or
  another execution context where the default subscriber is no longer guaranteed.
- Use both together for long-lived background loops. That is the pattern the
  workflow and projection runtimes already follow.

### A Useful Rule Of Thumb

If a trace shows the parent operation clearly but child tasks are flat, missing,
or detached from the request/workflow/projection they belong to, the first thing
to check is whether the spawned future needs `.instrument(...)`,
`.with_subscriber(...)`, or both.

## Use Turmoil Barriers When Traces Are Still Too Wide

Turmoil's barrier support is the next step after tracing. It is especially
useful when the bug depends on a very specific interleaving and logs only tell
you "something happened between A and B".

The upstream barrier API is currently marked unstable and requires the
`unstable-barriers` feature on `turmoil`. Terracedb does not enable that
feature by default today, so treat barriers as an opt-in debugging tool for hard
simulation bugs rather than the default approach.

At a high level, barriers work like this:

1. Add a typed trigger in the source code at the exact point you care about.
2. Register a `Barrier` in the test with a predicate matching the trigger.
3. Choose whether the barrier only observes, suspends, or panics.
4. Drive the simulation until that trigger fires.
5. Inspect state, then release execution.

### Choose The Right Trigger Function

- `turmoil::barriers::trigger(...)` is async and can suspend source execution.
- `turmoil::barriers::trigger_noop(...)` is sync and only supports observation.

Use `trigger_noop` when you only need "did this path execute?" Use `trigger`
when you need to freeze the simulation exactly at that point.

### Keep Barrier Payloads Small And Typed

The trigger value should usually be a dedicated enum or struct carrying the
minimal state needed to identify the event:

```rust
#[derive(Debug)]
enum DebugBarrier {
    BeforePublish {
        sequence: u64,
        table: String,
    },
}
```

Then gate the trigger so it is clearly simulation/debug-only:

```rust
#[cfg(feature = "turmoil-barriers")]
turmoil::barriers::trigger(DebugBarrier::BeforePublish {
    sequence,
    table: table.name().to_string(),
})
.await;
```

On the test side, match the specific event you care about:

```rust
use turmoil::barriers::{Barrier, Reaction};

let mut barrier = Barrier::build(Reaction::Suspend, |event: &DebugBarrier| {
    matches!(
        event,
        DebugBarrier::BeforePublish { sequence, .. } if *sequence == expected_sequence
    )
});
```

From there, drive the simulation using the current vendored Turmoil pattern
until the barrier fires, inspect the intermediate state, and then drop the
triggered handle to resume execution.

### Barrier Reactions

- `Reaction::Noop` observes the event and lets execution continue immediately.
- `Reaction::Suspend` pauses the source future until the test releases it.
- `Reaction::Panic` turns the trigger into an injected panic, which is useful
  when you want to prove the path is reachable or test panic handling.

For debugging, start with `Noop`, move to `Suspend` when you need exact
inspection, and reserve `Panic` for very targeted reachability checks.

### Barrier Caveats

- The API is unstable upstream, so re-check the vendored `~/dev/turmoil`
  reference before copy-pasting helpers into tests.
- Each trigger wakes at most one barrier and matching is registration-ordered.
  Avoid stacking multiple barriers on the same trigger type unless you are very
  sure about the ordering you want.
- Keep barriers behind a feature or test-only `cfg`. They are for deterministic
  debugging, not production control flow.

### Built-In Turmoil Trigger Types

The vendored Turmoil crate also exposes some barrier-friendly types for its own
subsystems, such as `turmoil::fs::FsCorruption` in the filesystem shim. That can
be useful when you are debugging code that directly relies on Turmoil's
simulated filesystem.

Terracedb's main simulation path usually goes through its own
`SimulatedFileSystem`, so in this repo custom Terracedb-specific trigger types
will often be more useful than the built-in Turmoil ones.

## Property, Matrix, And Snapshot Tests

Terracedb now uses three complementary test tools alongside the larger
deterministic simulation suites:

- Use `proptest` for low-level semantic invariants where broad input coverage
  and shrinking matter more than a single hand-written example. Current uses
  include MVCC ordering, commit-log frame round-trips, watermark monotonicity,
  and object-layout normalization.
- Use `rstest` for explicit mode matrices where the behavior should stay the
  same across a small set of storage, durability, format, or placement modes.
  Keep the case list readable instead of open-ended.
- Use `insta` only for stable, structured outputs that humans review well in a
  diff, such as normalized telemetry/span or metric shapes. Do not use snapshot
  assertions as a substitute for durable byte-format fixtures.

That split matters: durable-format compatibility and byte-for-byte storage
contracts still belong to the T23a-style golden fixtures and direct format
assertions. `insta` in this repo is for semantic and observability shapes, not
for on-disk or object-store compatibility promises.

### Reproducing Shrunk Property Failures

When a property test fails, the usual loop is:

1. Re-run the exact test with `cargo test <test-name> -- --nocapture`.
2. Let `proptest` shrink the case; the minimized failing input is the thing to
   debug, not the original large sample.
3. If `proptest` writes a regression case under `proptest-regressions/`, keep
   that file around while you fix the bug and rerun the same test to replay it.

The important habit is to preserve the smallest failing case you have. A good
shrunk repro is usually easier to reason about than a simulation trace with a
large randomized workload, and it often tells you exactly which invariant you
actually broke.

## Choosing The Tool

Use the lightest-weight tool that can actually answer the question:

- If you need to know whether the failure is reproducible, re-run the same seed.
- If you need the execution timeline, turn on tracing.
- If the trace becomes fragmented after `tokio::spawn`, fix the future with
  `Instrument` and `WithSubscriber`.
- If you need to stop at one exact interleaving or race point, add a barrier.

## Cleanup After The Fix

Once the root cause is understood:

- keep the failing seed as a regression test when it is still interesting
- keep any generally useful spans or attributes
- remove one-off barrier hooks unless they are likely to be reused
- if a barrier hook is worth keeping, leave it behind a clearly named test/debug
  feature rather than letting it drift into normal runtime code
