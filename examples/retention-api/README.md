# Retention API Example

This example is a small persisted app that demonstrates the generalized current-state retention
surface introduced in Phase 12 without making you read the full architecture document first.

It shows two policies side by side:

- `sessions`: threshold retention over `last_seen_ms`
- `leaderboard`: rank retention/materialization over `(points, tie_break)`

The example keeps the operational semantics explicit:

- threshold retention can be configured for rewrite-compaction-backed physical reclaim or a
  logical-only layout that fails closed for physical cleanup
- leaderboard retention defaults to derived-only materialization
- leaderboard retention can also be switched into an explicitly destructive mode when the source
  is marked rebuildable
- MVCC and CDC history retention are unchanged and remain sequence-based

## Run It

Use a dedicated data directory so you can reopen the example and inspect restart behavior:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo inspect
```

## Threshold Retention: Sessions

Create a few sessions with different `last_seen_ms` values:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-put alpha user-a 90 12
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-put bravo user-b 120 14
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-put charlie user-c 180 16
```

Install a threshold policy that keeps sessions whose `last_seen_ms >= 100` and uses rewrite
compaction plus delete cleanup:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-policy 2 100 rewrite_compaction_delete
```

The inspection output shows:

- the effective cutoff
- retained and non-retained current-state rows
- whether reclaimable rows are waiting on manifest publication or local cleanup
- backpressure and reason signals when snapshots or rewrite-compaction delay reclaim

Pin a snapshot on a stale row to see deferred reclaim stay explicit:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-pin alpha
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-inspect
```

Move the cleanup plan forward:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-manifest
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-cleanup
```

If you want to see logical-only behavior instead of physical reclaim, install the same threshold
policy with a logical-only layout:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo session-policy 3 100 logical_only
```

That keeps retained membership accurate while making the unsupported physical layout explicit in
the inspection reasons.

## Rank Retention: Leaderboard

Create a few players:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-put alpha 10 1 10
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-put bravo 10 2 10
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-put charlie 9 3 10
```

Install a derived-only top-2 leaderboard with an explicit tie-break recipe:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-policy 2 2 derived_only stable_id_ascending
```

`stable_id_ascending` means the ranking is ordered by:

- `points` descending
- `player_id` ascending as the deterministic tie-break

You can also use a hybrid recipe that folds `created_at_ms` into the ordering:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-policy 3 2 derived_only created_at_then_stable_id
```

Publish the retained set to establish the current materialized boundary, then mutate rows near the
cutoff:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-publish
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-put charlie 10 4 10
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-inspect
```

The inspection output shows:

- the effective rank boundary
- the derived top-N output
- source rows that remain outside the limit
- publication churn via `entered_ids` and `exited_ids`
- whether the policy is derived-only or waiting on destructive cleanup

## Explicit Destructive Mode

The example keeps destructive rank retention opt-in and fail-closed.

This destructive mode is allowed because the source is explicitly rebuildable:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-policy 4 2 destructive_rebuildable stable_id_ascending
```

This destructive mode fails closed because the source is not rebuildable:

```bash
cargo run -p terracedb-example-retention-api -- --data-dir .retention-demo leaderboard-policy 5 2 destructive_unrebuildable stable_id_ascending
```

## Restart Behavior

All commands persist the example state to `retention-example-state.json` inside the chosen data
directory. Re-running `inspect`, `session-inspect`, or `leaderboard-inspect` against the same
directory demonstrates that:

- policy configuration survives reopen
- retained outputs survive reopen
- manifest and cleanup state remain explicit across restart

## Mapping Back To The Phase 12 Concepts

- `session-policy`: threshold retention configuration
- `session-pin`: snapshot pinning that defers physical reclaim
- `session-manifest` and `session-cleanup`: physical reclaim coordination
- `leaderboard-policy`: rank retention and materialization configuration
- `leaderboard-publish`: publication boundary for churn tracking
- `leaderboard-policy destructive_*`: derived-only versus destructive source retention
