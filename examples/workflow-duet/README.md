# Workflow Duet Example

This example shows the workflow model we have been moving toward:

- one handwritten native Rust workflow,
- one sandbox-authored JavaScript workflow,
- a small durable relay that lets them interact through workflow outbox messages,
- the same durable runtime underneath both,
- automatic savepoints inside the runtime,
- and separate visible workflow history for inspection.

The native workflow lives in [src/app.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/src/app.rs). The shared transition rules live in [src/model.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/src/model.rs). The sandbox workflow is authored in TypeScript at [sandbox/review_workflow.ts](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/sandbox/review_workflow.ts), declares `lodash` in [sandbox/package.json](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/sandbox/package.json), and is emitted to the runtime entrypoint before the workflow runtime opens.

## What It Demonstrates

- **Native Rust stays native**: the Rust workflow is registered as a native registration target rather than pretending to be a sandbox bundle.
- **Sandbox authoring looks like the intended SDK path**: the sandbox workflow is authored in TypeScript, installs a basic npm dependency (`lodash`), and uses `wf.define(...)`, schema-backed state, and helper commands instead of exporting raw `handleTaskV1` boilerplate directly.
- **Rust and sandbox workflows can talk through durable surfaces**: paired instances emit durable `requested-approval` outbox messages, and the example relay turns those into approval callbacks for the counterpart runtime.
- **Visible history is not recovery truth**: the inspection helpers read workflow state, visibility, savepoint, and visible-history surfaces separately.
- **Timers are durable**: retry and timeout behavior is expressed as timer commands, survives restart, and replays under simulation.

## Verify It

```bash
cargo test -p terracedb-example-workflow-duet
```

The verified path today is the test suite: it starts both runtimes, kicks off one paired review under both runtimes, lets the relay cross-approve the pair when each side reaches `requested-approval`, and proves the interaction both directly and under seeded simulation replay.

## Verification

This example is covered by:

- [tests/happy_path.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/tests/happy_path.rs), which checks the public inspection APIs
- [tests/simulation.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/examples/workflow-duet/tests/simulation.rs), which proves the cross-runtime interaction and replay across restart in seeded simulation
