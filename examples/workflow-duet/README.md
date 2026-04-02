# Workflow Duet Example

This example shows the workflow model we have been moving toward:

- one handwritten native Rust workflow,
- one sandbox-authored JavaScript workflow,
- the same durable runtime underneath both,
- automatic savepoints inside the runtime,
- and separate visible workflow history for inspection.

The native workflow lives in [src/app.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/src/app.rs). The shared transition rules live in [src/model.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/src/model.rs). The sandbox workflow lives in [sandbox/review_workflow.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/sandbox/review_workflow.js) and imports the built-in sandbox workflow SDK from `@terrace/workflow`.

## What It Demonstrates

- **Native Rust stays native**: the Rust workflow is registered as a native registration target rather than pretending to be a sandbox bundle.
- **Sandbox authoring is smaller**: the JavaScript workflow uses `wf.define(...)`, schema-backed state, and helper commands instead of exporting raw `handleTaskV1` boilerplate directly.
- **Visible history is not recovery truth**: the inspection helpers read workflow state, visibility, savepoint, and visible-history surfaces separately.
- **Timers are durable**: retry and timeout behavior is expressed as timer commands, survives restart, and replays under simulation.

## Run It

```bash
cargo run -p terracedb-example-workflow-duet
```

The demo starts both runtimes, kicks off one native and one sandbox review, waits for the retry timer to advance them into the approval stage, approves both, and prints a combined inspection report.

## Verification

This example is covered by:

- [tests/happy_path.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/tests/happy_path.rs), which checks the public inspection APIs
- [tests/simulation.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/tests/simulation.rs), which proves deterministic replay across restart in seeded simulation
