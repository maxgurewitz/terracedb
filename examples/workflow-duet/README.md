# Workflow Duet Example

This example is a compact reference app for the workflow runtime introduced in T108-T114. It keeps the business logic small on purpose so the interesting part stays visible:

- one native Rust workflow
- one sandbox-authored workflow
- the same `WorkflowRuntime` engine and run/history model for both
- explicit retry and timeout timers
- an explicit callback wakeup path
- deterministic restart coverage in the example's own tests

The native handler lives in [`src/app.rs`](/Users/maxwellgurewitz/.codex/worktrees/7ee2/terracedb/examples/workflow-duet/src/app.rs). The sandbox-authored workflow lives in [`sandbox/review_workflow.js`](/Users/maxwellgurewitz/.codex/worktrees/7ee2/terracedb/examples/workflow-duet/sandbox/review_workflow.js). The shared state model and transition rules live in [`src/model.rs`](/Users/maxwellgurewitz/.codex/worktrees/7ee2/terracedb/examples/workflow-duet/src/model.rs).

## Run It

```bash
cargo run -p terracedb-example-workflow-duet
```

The demo opens a local Terracedb instance under `.workflow-duet-data`, starts both runtimes, kicks off a native and sandbox review, waits for the retry timer to fire, approves both reviews, and prints an inspection report as JSON.

## What To Look At

Runs vs bundles:

- Each execution gets its own `run_id`, which is the durable identity for that one review run.
- Each workflow definition pins a `bundle_id`, which is the durable identity for the code version that handled the run.
- The native workflow and the sandbox workflow use different bundle metadata, but both are driven by the same runtime surface.

History vs state:

- `state` is the latest JSON blob for one instance, such as `waiting-approval` or `approved`.
- `history` is append-only and records every admitted trigger and applied transition.
- The example inspection helpers intentionally use the public `describe`, `list`, and `history` APIs rather than reading bespoke internal tables.

Why waits and retries are durable data:

- The workflow stores its current stage, attempt count, pending callback, and timeout deadline in state.
- Retry and timeout behavior are expressed as durable timer commands in history, not as in-memory sleeps.
- Restarting the runtime after the retry timer is scheduled still resumes from the same persisted run state.

Why sandbox execution is optional:

- The native path wraps a Rust handler in `NativeWorkflowHandlerAdapter`.
- The sandbox path wraps a JS module in `SandboxWorkflowHandlerAdapter`.
- Both then run through `ContractWorkflowHandler` plus `WorkflowRuntime`, so the engine, run records, state records, visibility, and history model stay the same.

## Verification

This example is covered by:

- a public-API inspection test for native and sandbox happy paths
- a seeded simulation test that exercises retry, timer, approval, timeout, and restart behavior on the same durable engine
