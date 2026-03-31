# Workflows Proposal

Status: Draft

Purpose: capture the workflow-library direction suggested by our recent sandbox discussion plus the Temporal codebase exploration, then turn those ideas into concrete TerraceDB follow-up tasks before we roll anything into `docs/ARCHITECTURE.md`.

This is intentionally a proposal document, not a final architecture commitment.

## Why This Exists

TerraceDB already has a promising workflow baseline:

- `crates/terracedb-workflows` defines a Rust-owned executor with durable inbox admission, per-instance ordering, durable timers, and an outbox.
- `docs/ARCHITECTURE.md` now states that TypeScript-authored workflows should plug into that Rust executor rather than replace it.
- The sandbox design already distinguishes draft authoring sandboxes from sealed published workflow bundles.

That is a strong starting point. The main thing still missing is a sharper model for:

- durable workflow history,
- the Rust <-> TypeScript execution contract,
- queries and update-like interactions,
- external effect execution,
- upgrades for running workflows,
- operator visibility,
- and the difference between correctness boundaries and performance accelerators.

One important design constraint should stay explicit:

- workflows must remain implementable directly in Rust without the sandbox,
- and sandboxed TypeScript workflows should be an additional adapter path, not the only workflow authoring model.

Temporal is useful here not because we should copy its whole system, but because it exposes a set of battle-tested boundaries.

## Current TerraceDB Baseline

Today our workflow model is centered on:

- `WorkflowHandler::route_event` and `WorkflowHandler::handle` in `crates/terracedb-workflows/src/lib.rs`
- per-workflow tables for `state`, `inbox`, `trigger_order`, `source_cursors`, `timer_schedule`, `timer_lookup`, and `outbox`
- a Rust-owned runtime that enforces strict per-instance trigger ordering
- a draft plan for `terracedb-workflows-sandbox` or similar so reviewed TypeScript bundles can act as handlers alongside native Rust handlers

The existing design is already good at:

- deterministic per-instance ordering,
- atomic state + outbox + timer updates,
- pluggable scheduling,
- durable trigger admission,
- and keeping workflow correctness inside Rust.

The main gap is that the current design does not yet define an explicit append-only workflow run history or an explicit visibility projection. The current tables are sufficient for progress, but not yet ideal for:

- replay auditing,
- bundle compatibility checks,
- live-run upgrades,
- operator forensics,
- or compact, queryable records of past behavior and performance.

Because TerraceDB is greenfield, we should be willing to make radical changes here if a history-first model produces a cleaner library.

## The Main Lessons To Carry Forward

These are the Temporal-derived ideas that seem most relevant to TerraceDB:

1. The engine should own durable truth; guest code should only interpret it.
2. Immutable execution history, current mutable summary, and operator visibility should be separate products.
3. All workflow inputs should flow through one state-transition engine.
4. Not every interaction needs to be a durable history event.
5. Sticky execution and speculative execution should be treated as disposable accelerators.
6. Scheduling should separate durable queue ownership from in-memory dispatch policy.
7. Timers should be derived from durable state rather than managed ad hoc.
8. Duplicate delivery and retries should be assumed, not treated as edge cases.
9. Bundle selection should happen at run boundaries by default, with explicit compatibility gates for live upgrades.
10. Visibility should be an async projection optimized for operators, not a query over raw history.

## What We Should Not Copy

Temporal is also a warning about what not to import into a greenfield design.

- We should not copy its service topology. TerraceDB does not need separate frontend, history, and matching services in v1.
- We should not copy legacy version-set concepts or compatibility modes that only exist for backward compatibility.
- We should not copy network protocol complexity unless we actually need cross-process workers.
- We should not force every workflow interaction into one mechanism if a cheaper ephemeral path is cleaner.
- We should not wait to define visibility until after the executor is complete.

## Restate Exploration Notes

This section captures the current synthesis from our Restate codebase exploration before we decide what to roll into `docs/ARCHITECTURE.md`.

It is still provisional, but it now reflects the main themes that surfaced across the focused reads.

### Initial Repo Signals

The Restate repository is organized around a relatively tight execution path:

- `crates/bifrost`
- `crates/core`
- `crates/invoker-api`
- `crates/invoker-impl`
- `crates/partition-store`
- `crates/service-protocol`
- `crates/timer`
- `crates/timer-queue`
- `crates/worker`
- `server`

Its own repo guidance calls out the latency-critical path as primarily:

- Bifrost,
- the networking layer,
- `restate-core`,
- the partition-processor state machine,
- and the invoker.

That already suggests a design that is more direct and event-driven than Temporal’s more decomposed frontend/history/matching/worker model.

### Initial Architectural Signals

From the top-level docs and the service protocol, Restate appears to lean on a few strong ideas:

- durable journal-first execution,
- explicit commands-and-notifications protocol boundaries,
- first-class suspension and resume semantics,
- retry metadata built directly into the engine/runtime contract,
- and relatively direct integration between the durable engine and non-Rust worker runtimes.

The strongest protocol-level signals so far are:

- immutable journal language in protocol v5,
- `StartMessage.retry_count_since_last_stored_entry`,
- `StartMessage.duration_since_last_stored_entry`,
- deterministic random seed delivery,
- `SuspensionMessage` for waiting on completions or signals,
- `ErrorMessage.next_retry_delay`,
- and protocol errors for journal mismatch / protocol violation cases.

The protocol is also notable for how concrete it is about execution semantics. It does not just hand arbitrary code a database handle and ask it to “do workflow things.” Instead it models:

- start/replay context,
- explicit state snapshots,
- commands,
- notifications,
- promise access,
- sleep/timer registration,
- invocation attachment,
- output attachment,
- and proposed run completions

as typed protocol messages.

That is a very strong signal for TerraceDB. Even if our Rust <-> TypeScript ABI is not literally protobuf-based, it should probably be this explicit.

The surrounding invoker code also suggests that Restate keeps the engine/runtime seam very operationally explicit. The invoker API exposes operations like:

- invoke,
- notify completion,
- notify notification,
- notify stored command ack,
- retry invocation now,
- pause invocation,
- abort invocation,
- and register partition.

In other words, the engine is not merely dispatching opaque work to a foreign runtime. It is continuously driving a state machine through typed callbacks and acknowledgements.

That feels directly relevant to TerraceDB’s proposed `workflow-task/v1` boundary. If we want the Rust executor to remain authoritative, the sandbox runtime should probably behave like a bounded task interpreter that returns structured progress, not like a free-running process we merely supervise.

The deeper read also clarified an important internal split: Restate appears to have both:

- a public streaming service protocol used at the SDK/runtime boundary,
- and a separate internal effect/state-machine interface used inside the engine.

That is a very useful design cue for TerraceDB. We may want:

- one versioned public Rust <-> sandbox ABI,
- and a separate internal transition/effects interface that remains private to the engine.

Those should not be conflated.

The manifest/schema layer also appears to treat retries and retention as first-class configuration:

- `journalRetention`
- `workflowCompletionRetention`
- `retryPolicyInitialInterval`
- `retryPolicyMaxInterval`
- `retryPolicyMaxAttempts`
- `retryPolicyExponentiationFactor`
- `retryPolicyOnMaxAttempts`

That is encouraging for TerraceDB because it supports a workflow model where retries, retention, and replay are not bolted on later.

It is also notable that these settings appear alongside ingress visibility and service configuration rather than being buried deep inside implementation-specific runtime knobs. That suggests a product shape where retry and retention policy are part of the public contract for a service or workflow deployment.

The deeper retry read suggests that Restate’s policy model is also hierarchical and deployment-aware:

- server defaults,
- service-level overrides,
- handler-level overrides,
- and dynamic one-off retry delay overrides from the runtime itself.

That is a strong design cue for TerraceDB. Retry policy should likely compose across deployment, workflow, and effect levels rather than existing as a single flat knob.

The initial read of Restate’s invocation admin APIs also surfaces a useful operational distinction:

- graceful cancellation that preserves progress and consistency expectations,
- forceful kill semantics that may break consistency across state or side effects,
- purge-style cleanup for completed executions,
- and restart / restart-as-new style controls over prior journal state.

That is a good reminder that TerraceDB should distinguish:

- safe workflow termination,
- unsafe operator escape hatches,
- and history-preserving versus history-resetting restart paths.

Restate also appears to support selective restart from a journal prefix and deployment replacement during resume/restart flows. Even if TerraceDB chooses a simpler v1, this is a useful reference point for how upgrade-aware replay control might look once we have immutable workflow bundles and explicit run history.

The scheduler code also looks intentionally explicit about load and fairness. Even from an early read, it models states such as:

- ready,
- scheduled for future visibility,
- throttled,
- blocked on global capacity,
- and waiting for concurrency tokens.

That is a strong fit for our own design instincts. TerraceDB should expose scheduler and queue states as first-class operator-visible data rather than reducing everything to generic “pending” work.

### Confirmed Findings From Deeper Reads

The deeper reads reinforced that Restate’s core execution model is more specific than a generic “workflow runtime.”

The most important correctness pattern is that Restate appears to separate durable apply from side effects very cleanly:

- the partition processor reads durable log records,
- applies them transactionally to partition state,
- commits,
- and only then emits runtime actions to timers, outbox, or invoker paths.

That is one of the strongest patterns we should copy. TerraceDB should not let sandbox execution, timer dispatch, or effect emission race ahead of the durable transition that justifies them.

The replay path also looks explicitly bounded:

- open a consistent storage snapshot,
- load state and journal,
- send a start/replay context to the runtime,
- replay stored entries,
- then drop the snapshot and continue with live execution.

That is better than an implicit “some state was loaded somehow” model. TerraceDB should make the `snapshot -> replay -> live` boundary explicit in both runtime semantics and simulation tests.

The journal itself is not just an audit log. It behaves more like an execution log plus reducer input:

- commands and notifications are persisted,
- the partition state machine interprets them,
- and durable consequences like timers, waits, state writes, and outgoing calls are derived from that interpretation.

This is a very useful contrast with a simpler event-history-only model. It suggests that our eventual `workflow_history` may need to be rich enough to drive deterministic reduction, not just post-hoc inspection.

Invocation lifecycle state is also much more first-class than in many systems. Restate appears to persist durable lifecycle states like:

- scheduled,
- inboxed,
- invoked,
- suspended,
- paused,
- completed,
- and free

with shared metadata like timestamps, journal metadata, retention, and pinned deployment. That strongly supports giving TerraceDB explicit durable run status records rather than treating waits/pauses/completions as transient executor implementation details.

Suspend/resume is likewise represented as durable dependency data rather than hidden inside sleeping worker tasks. The runtime reports the exact completion or signal set it is waiting for, and the engine persists that waiter set. That feels like the right model for TerraceDB too.

Safe retry is also fenced by durability. Restate appears to track what was merely proposed versus what was durably acknowledged, and it retries only across the safe boundary. That is a stronger model than “worker failed, rerun the task” and it fits well with our existing interest in effect idempotency and retry policy.

Finally, workflows seem to be a specialization of the same durable invocation core rather than a completely separate engine. That is probably the right direction for TerraceDB as well: one durable execution substrate, then workflow-specific policy and ergonomics on top.

The deeper retry/timer read also surfaced a few additional patterns worth carrying forward:

- retry backoff state is durable and keyed, so stale timer firings can be ignored safely,
- timers are partition-local durable records that turn back into journal completions or notifications,
- promise/awakeable coordination is durable and first-write-wins,
- and completion retention is an explicit metadata concern rather than an afterthought.

Those all reinforce the same theme: long-running coordination primitives should be stored as engine-owned data, not left to transient worker memory.

### Early Contrast With Temporal

Temporal’s most visible boundary is durable event history plus task dispatch to workers. Restate, at least from the initial code read, appears to push harder on:

- journal-oriented execution,
- direct suspension/resume semantics,
- tighter engine/runtime protocol contracts,
- and a lower-hop execution path.

My working hypothesis is that Restate’s lower-latency reputation likely comes from some combination of:

- less reliance on polling-style task acquisition,
- fewer service boundaries in the hot path,
- a tighter partition-processor + invoker architecture,
- and a protocol that is designed around suspend/resume rather than repeated broad history-task handoff.

The initial invoker/partition path also suggests that work is closely coupled to partition ownership and append-to-log style admission. That may matter just as much as the lack of polling. Temporal often pays for generality and service separation; Restate appears to chase a shorter control loop between durable admission, partition state machine, and runtime execution.

Another useful detail from the deeper read is that acknowledgements appear to be tied to durability rather than simple receipt. That is exactly the kind of boundary we should preserve in TerraceDB if we want retries, replay, and external effects to stay understandable under failure.

The scheduler findings sharpen the latency story too. Restate appears to use an event-driven vqueue scheduler with explicit wakeups and delay-queue scheduling rather than a polling-heavy worker task-queue model. That feels very aligned with TerraceDB’s event-driven instincts.

Some of Restate’s latency strategy also lives below the workflow layer:

- batching that is sized by bytes and storage structure rather than only item count,
- adaptive timeout policy derived from recent latency,
- willingness to accept some recovery complexity to shrink steady-state append latency,
- and care to keep blocking storage work away from the async runtime hot path.

Those are not all workflow-library decisions, but they are useful reminders that a “fast workflow engine” often depends on storage and scheduler discipline as much as API design.

That said, Restate also appears less feature-rich in areas where Temporal is especially mature:

- upgrade/versioning policy,
- operator-facing visibility APIs,
- and long-tail workflow compatibility controls.

It also seems to lean more heavily on SQL-over-storage for visibility and introspection than Temporal does. That is powerful, but the codebase’s own query-engine notes suggest this can become bottlenecked for common list/status paths without precomputed projections or indexes.

The replay model also seems more explicit and engine-driven than a naive “rehydrate and hope” design. The invoker constructs a start/replay context with known entry count, state snapshot, retry metadata, and deterministic seed material, then streams the journal through the runtime. That suggests a concrete TerraceDB design question we should answer early:

- do sandboxed workflow handlers consume replay slices directly,
- or do they receive a more derived task/state view synthesized by Rust?

Restate is a strong argument that explicit replay input is worth taking seriously.

Restate’s top-level positioning also reinforces that it treats introspection as a built-in product, not an optional add-on. Its README puts observability and introspection alongside reliable execution, durable timers, and consistent state. That is a useful contrast to many systems where visibility arrives later.

The codebase also exposes explicit introspection and query surfaces in the admin API, and it carries tracing/telemetry through the runtime and storage layers. Even without fully understanding the user-facing UX yet, the repo structure suggests that inspection and debugging are considered part of the engine, not just external monitoring glue.

The deeper read suggests an important nuance here: Restate’s user-facing visibility seems more SQL-first than API-first. It has raw journal and status tables exposed through a query layer, but not the same kind of first-class paginated history and describe surfaces that Temporal offers. TerraceDB should probably preserve Temporal’s split:

- dedicated describe/detail APIs,
- dedicated paginated history APIs,
- and optional SQL or ad hoc query access for deeper inspection.

That gives us the power of raw inspection without forcing every common operator path through a distributed scan engine.

Another especially interesting detail is that Restate’s protocol feeds deterministic random seed material directly into the worker runtime and lets failures override the next retry delay for a single attempt. That suggests two ideas we may want to carry forward:

- deterministic helper inputs should be part of the engine-owned task contract,
- and retry control should support both policy defaults and per-failure override hints.

### TerraceDB Takeaways

Taken together, the Restate exploration reinforces a few directions that fit TerraceDB well:

- keep the Rust executor on an event-driven hot path rather than introducing a polling-heavy worker model,
- separate durable apply from side effects,
- make suspension and resumption explicit runtime semantics,
- persist wait-sets and lifecycle state as durable data,
- treat retries as a first-class engine feature rather than userland glue,
- make retry policy hierarchical and allow controlled per-failure override hints,
- define a crisp engine <-> guest protocol instead of exposing ambient host APIs,
- keep the engine’s internal transition/effects ABI separate from the public sandbox ABI,
- let the host and guest exchange typed acknowledgements and completions rather than only coarse task success/failure,
- treat “durably accepted” as an explicit protocol concept,
- keep ingress, timer ownership, and scheduling close to the partition or run owner when possible,
- model durable promise/awakeable coordination in the engine instead of burying it inside guest runtime behavior,
- expose scheduler pressure and termination state explicitly to operators,
- make retention, retry, and ingress policy explicit deployment metadata,
- define explicit completion-retention policy for long-running workflows and procedures,
- treat SQL or ad hoc query access as complementary to, not a replacement for, dedicated visibility APIs,
- provide deterministic helper inputs like seeded randomness from the host rather than from ambient guest runtime APIs,
- and avoid extra service decomposition until we truly need it.

The stronger lesson from Restate is not just “streaming protocol good.” It is that explicit replay input, explicit suspension waiter sets, proposal-then-commit durability, and durability-tied acknowledgement all seem to work together as one coherent contract. TerraceDB should aim for that same coherence even if the exact wire format is different.

The likely greenfield consensus is not “copy Temporal” or “copy Restate.”

It is closer to:

- keep Temporal’s rigor around durable truth, upgrade boundaries, and visibility,
- adopt Restate’s instinct for a tighter protocol boundary, durable wait-set modeling, and lower-latency event-driven execution,
- and use our sandbox architecture to get language-runtime flexibility without weakening Rust ownership of correctness.

### Working Consensus After Temporal And Restate

The current comparison points toward a fairly clear synthesis.

What Temporal still seems better at:

- explicit run history as a first-class product,
- upgrade/versioning semantics for long-running executions,
- operator-facing describe/history/visibility APIs,
- and a generally clearer separation between durable truth and convenience projections.

What Restate seems better at:

- a tighter event-driven hot path,
- more explicit runtime protocol semantics,
- cleaner separation of durable apply from side effects,
- owner-local scheduling and timer handling,
- and durable modeling of waits, retries, and coordination primitives.

The TerraceDB consensus should probably be:

- keep a Rust-owned durable transition engine,
- use immutable reviewed workflow bundles,
- maintain explicit run history plus mutable state plus visibility projection,
- use an explicit Rust <-> sandbox task protocol with deterministic helper inputs,
- persist wait-sets, retry state, timer ownership, and lifecycle status as data,
- keep scheduling and wakeups event-driven and as close to the run owner as practical,
- and expose dedicated visibility APIs even if we later add SQL-style introspection over raw workflow storage.

That feels like a stronger greenfield design than copying either system wholesale.

### Open Questions For The Restate Read

The exploration still needs to answer a few specific questions before we treat the Restate comparison as stable:

- how much of Restate’s latency advantage comes from architecture versus workload assumptions,
- how it represents and compacts journal replay over time,
- what its exact durability and ordering boundaries are between partition processing and invoker execution,
- how we want to balance SQL-first introspection with dedicated TerraceDB visibility APIs,
- how much of Restate’s operator-forensics split we should copy directly,
- and how it handles upgrades, protocol evolution, and compatibility for long-running executions.

## Proposed Direction

The most important greenfield adjustment is this:

TerraceDB workflows should become explicitly run-based and history-first.

The core model should be:

- `WorkflowBundle`
  Immutable reviewed workflow artifact.
- `WorkflowRun`
  One execution of one bundle, with its own run ID, status, state summary, and history.
- `WorkflowHistory`
  Append-only durable record of admitted triggers, task attempts, timer changes, effect scheduling, completions, failures, and run transitions.
- `WorkflowState`
  Current summary derived from history and persisted for speed.
- `WorkflowVisibility`
  Async operator-facing projection for search, list, count, and performance inspection.

That implies a sharper split between four layers:

- `terracedb-workflows-core`
  Run/history/state model and deterministic transition engine, independent of guest language.
- `terracedb-workflows-runtime`
  Scheduling, timers, queue ownership, effect dispatch, visibility updates.
- `terracedb-workflows-sandbox`
  TypeScript bundle adapter for the Rust executor.
- `terracedb-workflows-dev`
  Draft sandbox preview, replay, authoring, and packaging tools.

Native Rust workflows should continue to implement the core/runtime contracts directly, without depending on `terracedb-workflows-sandbox`.

## Proposed ABI

The TypeScript workflow handler ABI should stay narrow and versioned.

This ABI is specifically for sandboxed workflow handlers. It should complement, not replace, the native Rust handler path.

Guest code should not receive raw `Db`, raw `Table`, or unrestricted host APIs.

Instead, Rust should invoke something conceptually like:

```ts
type WorkflowTask = {
  runId: string
  bundleId: string
  instanceId: string
  taskId: string
  replay: HistorySlice
  state: JsonValue | null
  trigger: WorkflowTrigger
  context: {
    stableId(scope: string): string
    stableTime(scope: string): string
    logger: WorkflowLogger
    queries?: WorkflowQueryHelpers
  }
}

type WorkflowTaskResult = {
  commands: WorkflowCommand[]
  messages?: WorkflowMessage[]
  patches?: WorkflowPatchMarker[]
}
```

The Rust executor should then:

- validate the task token / identity,
- validate deterministic command shape,
- append history,
- update current state,
- derive timers,
- schedule effects,
- and update visibility.

## Task Backlog

Each task below corresponds to one of the major insights above and maps it onto TerraceDB.

### WFP-01: Make Workflow History First-Class

Insight:
Temporal’s strongest boundary is immutable history as source of truth, with mutable state as a summary.

In-repo relevance:
`crates/terracedb-workflows` currently persists `state`, `inbox`, timers, and `outbox`, but not an explicit append-only workflow run history.

Application to TerraceDB:
Introduce a per-run append-only `workflow_history` table or table family. The current inbox and timer structures may remain, but they should either become derived data or be explicitly tied to a run-history record.

Deliverables:

- Define `WorkflowRunId`, `WorkflowBundleId`, and `WorkflowTaskId`.
- Define `WorkflowHistoryEvent`.
- Decide whether admitted inbox rows are themselves history events or derived queue rows.
- Define replay guarantees against run history.

### WFP-02: Split Run History, Mutable State, and Visibility

Insight:
Execution truth, fast current state, and operator visibility should be separate products.

In-repo relevance:
`docs/ARCHITECTURE.md` currently defines workflow tables for correctness, but not a dedicated visibility layer.

Application to TerraceDB:
Adopt three explicit data products:

- `workflow_history`
- `workflow_state`
- `workflow_visibility`

This is a good greenfield change even if it reshapes the current table layout.

Deliverables:

- Define minimal state summary needed for fast execution.
- Define visibility fields for list/count/search/describe.
- Decide which fields belong only in history, only in state, or only in visibility.

### WFP-03: Introduce a Single Transition Engine

Insight:
User calls, timer firings, worker completions, and callbacks should all flow through one state-transition path.

In-repo relevance:
Our current model already trends in this direction with `WorkflowHandler::handle`, but we should make it explicit in the Rust library design.

Application to TerraceDB:
Define one internal transition engine that accepts:

- admitted trigger,
- current run state,
- bundle identity,
- and optional control messages

and produces history writes, state changes, timer changes, and effect tasks.

Deliverables:

- Define `WorkflowTransitionInput` and `WorkflowTransitionOutput`.
- Remove special-case execution paths where possible.
- Make the transition engine the only place allowed to mutate workflow run state.

### WFP-04: Define the Rust <-> TypeScript Task ABI

Insight:
The guest runtime should not own execution state. It should receive replay input and return commands.

In-repo relevance:
`docs/ARCHITECTURE.md` already says TS workflows should plug into the Rust executor, but the exact ABI is still open.

Application to TerraceDB:
Define a versioned `workflow-task/v1` ABI for sandboxed handlers. Treat it like a real protocol boundary even if both sides initially live in one process.

Deliverables:

- Define `WorkflowTask`, `WorkflowTaskResult`, `WorkflowCommand`, and `WorkflowMessage`.
- Decide what command classes exist in v1.
- Decide what deterministic helpers are injected.
- Decide how ABI version negotiation or rejection works.

### WFP-05: Separate Durable Events from Ephemeral Messages

Insight:
Not every workflow interaction should become a durable event. Queries and update-like interactions may need cheaper lanes.

In-repo relevance:
Our current workflow design talks about events, timers, and callbacks, but does not yet define ephemeral query/update semantics for running workflows.

Application to TerraceDB:
Add a separate message plane for:

- read-only queries,
- validation-style update requests,
- and possibly speculative preview interactions

where rejection or no-op paths do not necessarily append run history.

Deliverables:

- Define `WorkflowQueryRequest` and `WorkflowUpdateRequest`.
- Decide when updates become durable history versus when they disappear.
- Decide whether a rejected update is logged in audit only, visibility only, or nowhere durable.

### WFP-06: Add External Effect Tasks as a First-Class Model

Insight:
Deterministic workflow code should not perform side effects directly.

In-repo relevance:
`terracedb-workflows` already has `outbox`, which is the right seed, but we should expand it into a more explicit effect model.

Application to TerraceDB:
Treat external effects as first-class Rust-owned tasks, whether they are:

- procedure invocations,
- HTTP calls,
- MCP tool calls,
- or app-specific capability invocations

with stable IDs and idempotency contracts.

Deliverables:

- Define `WorkflowEffectTask`.
- Define idempotency requirements for each effect class.
- Decide which effects can feed callback triggers back into the run.

### WFP-07: Treat Sticky Sandboxes as a Cache Hint

Insight:
Worker affinity is a performance optimization, not a correctness boundary.

In-repo relevance:
We are likely to want sandbox isolate reuse for performance, especially in development and maybe in production.

Application to TerraceDB:
Permit sticky reuse of warm sandbox isolates or compiled bundles, but require every execution path to remain correct when replaying from Rust-owned run history into a fresh isolate.

Deliverables:

- Define sticky cache semantics.
- Define invalidation rules for bundle change, capability change, or runtime upgrade.
- Ensure query/update fallback works when a warm isolate is unavailable.

### WFP-08: Redesign Scheduling Around Durable Ownership + Bounded Dispatch

Insight:
Durable queue ownership and in-memory matching policy should be separate concerns.

In-repo relevance:
Our current scheduler abstraction is intentionally small, which is good, but it does not yet define fairness, backpressure, or multi-runtime ownership clearly.

Application to TerraceDB:
Keep durable ready-work ownership in Rust, with an in-memory scheduler choosing among ready runs. Add bounded ingress and explicit resource exhaustion behavior rather than relying on unbounded buffering.

Deliverables:

- Define durable ready-run queue semantics.
- Define backpressure and shedding behavior.
- Decide whether scheduler metadata includes tenant, priority, shard, or domain.
- Decide whether we need weighted fairness in v1 or only after the base executor stabilizes.

### WFP-09: Derive Timers from Durable State

Insight:
Timers should be a pure consequence of durable run state, not an independent logic path.

In-repo relevance:
Current timer schedule and lookup tables are already close to this model.

Application to TerraceDB:
Make timer derivation explicit in the transition engine and treat timer queues as execution support for already-derived timer obligations.

Deliverables:

- Define timer derivation rules from run state/history.
- Decide whether all timers are represented as history events plus current derived tables.
- Batch expired timers into one transition where possible.

### WFP-10: Define Failure Taxonomy and Idempotency Rules

Insight:
Workflow-task failure, run failure, effect failure, timeout, cancellation, nondeterminism, and infrastructure failure should be distinct.

In-repo relevance:
Our current docs describe duplicate timer firings as benign, but we have not yet fully defined the larger failure model for workflow turns and effect retries.

Application to TerraceDB:
Write a first-class failure taxonomy and make every effect path explicitly idempotent or explicitly non-retryable.

Deliverables:

- Define `WorkflowTaskFailure`, `WorkflowRunFailure`, `WorkflowEffectFailure`, and infrastructure errors.
- Define duplicate-delivery expectations.
- Require stable request IDs on control-plane mutations.
- Define how nondeterminism is recorded and surfaced.

### WFP-11: Add First-Class Effect Retry Policies

Insight:
Temporal treats activity retries as a first-class engine concern with configurable policy, attempt tracking, timeout interaction, and terminal failure semantics. TerraceDB should do the same for workflow-owned external effects.

In-repo relevance:
The proposal already introduces `WorkflowEffectTask` in WFP-06 and failure taxonomy in WFP-10, but it does not yet define a dedicated retry-policy model for those effects.

Application to TerraceDB:
Introduce an explicit Rust-owned retry policy for workflow effects. TypeScript workflow code may request or select a retry policy, but the Rust runtime should own:

- attempt counting,
- backoff scheduling,
- timeout interaction,
- retryability checks,
- terminal failure recording,
- and callback delivery on success or terminal failure.

Retryable effects should require either:

- an explicit idempotency key, or
- an explicit declaration that the effect is non-retryable.

Recommended policy shape:

```ts
type WorkflowEffectRetryPolicy = {
  initialBackoffMs: number
  maxBackoffMs?: number
  backoffCoefficient?: number
  maxAttempts?: number
  expirationMs?: number
  timeoutMs?: number
  nonRetryableErrorCodes?: string[]
  jitter?: "none" | "full" | "bounded"
}
```

Deliverables:

- Define `WorkflowEffectRetryPolicy`.
- Define how retry policy is attached to `WorkflowEffectTask`.
- Define effect-attempt history and terminal failure recording.
- Define the interaction between retries, timeouts, and continue-as-new.
- Define which effect classes have default retry policies and which are non-retryable by default.

### WFP-12: Introduce Immutable Bundles and Run-Level Version Assignment

Insight:
Bundle or worker version should normally be assigned at run start, not changed mid-run by default.

In-repo relevance:
We already plan for published workflow bundles, but not yet for formal run-level bundle assignment and upgrade semantics.

Application to TerraceDB:
Record assigned bundle ID on each run. New runs choose a bundle via deployment policy. Existing runs stay pinned unless there is an explicit compatibility-backed redirect or override.

Deliverables:

- Define bundle assignment at start.
- Define deployment policies for current, ramping, and override bundles.
- Decide whether child runs inherit parent bundle assignment by default.

### WFP-13: Make Continue-As-New the Default Upgrade Boundary

Insight:
Long-running workflows need a standard epoch boundary for upgrades and history compaction.

In-repo relevance:
Our current docs mention published bundles and draft preview, but not yet a formal continue-as-new strategy for upgrades.

Application to TerraceDB:
Add first-class continue-as-new support and make it the normal way to move long-lived runs onto a newer bundle, compact history, or checkpoint state into a fresh run.

Deliverables:

- Define `ContinueAsNew` command.
- Define carry-forward state and visibility semantics.
- Define engine-generated rotate hints based on history size, event count, or patch accumulation.

### WFP-14: Add Per-Run Pinning and Explicit Compatibility Gates

Insight:
Live-run redirect should exist, but only with an explicit compatibility story.

In-repo relevance:
We have not yet defined what it means to upgrade a running TerraceDB workflow safely.

Application to TerraceDB:
Support per-run override or pinning, but require bundle compatibility manifests and replay tests before allowing redirect or auto-upgrade of live runs.

Deliverables:

- Define `WorkflowCompatibilityManifest`.
- Define replay-test requirements for bundle upgrades.
- Define whether overrides are mutable by operators, deployment policy, or workflow code.

### WFP-15: Build Visibility as an Async Operator Projection

Insight:
Operators need a denormalized search surface, not raw history replay.

In-repo relevance:
This is currently missing from the workflow library design.

Application to TerraceDB:
Add `workflow_visibility` as an async projection updated from workflow history or effect completion records. Make it the backing store for:

- list runs,
- count runs,
- search runs,
- bundle reachability,
- and run performance summaries.

Deliverables:

- Define visibility schema.
- Define query surface.
- Define async update guarantees and lag reporting.
- Decide what `describe_run` reads from visibility versus live mutable state.

### WFP-16: Add Run Forensics and Performance Inspection

Insight:
A workflow system needs both projection APIs and live forensic APIs.

In-repo relevance:
We should not make operators inspect raw history rows by hand.

Application to TerraceDB:
Provide:

- `describe_run`
- `get_run_history`
- `get_run_trace`
- `get_run_effects`
- `get_run_failures`

where `describe_run` is optimized for current live state and `get_run_history` is the canonical replay/audit view.

Deliverables:

- Define run-inspection API surface.
- Define which metrics belong in visibility versus telemetry versus history.
- Decide how workflow queries fit into inspection.

### WFP-17: Add Replay, Compatibility, and Determinism Test Harnesses

Insight:
Versioning and determinism are not trustworthy unless replay is a first-class test mode.

In-repo relevance:
This repo already values simulation and deterministic tests heavily. Workflows should follow the same standard.

Application to TerraceDB:
Add workflow replay tests, compatibility campaigns, and simulation fixtures so bundle upgrades and executor changes are validated by replaying stored run histories.

Deliverables:

- Define a workflow replay harness.
- Add golden run histories.
- Add bundle-to-bundle compatibility test helpers.
- Add simulation scenarios for duplicates, retries, timer races, and continue-as-new.

## Recommended Order

If we execute this work, the best order is:

1. WFP-01 through WFP-04
   Freeze the core data model and the Rust <-> TS ABI.
2. WFP-05 through WFP-11
   Flesh out runtime semantics: queries, effects, sticky reuse, scheduling, timers, failures, and retries.
3. WFP-12 through WFP-14
   Lock down bundle assignment, continue-as-new, and upgrade safety.
4. WFP-15 through WFP-17
   Add visibility, forensics, and replay/compatibility validation.

That order maximizes parallelism while keeping later work from building on unstable contracts.

## Initial Recommendation

If we only adopt three changes immediately, they should be:

1. Introduce explicit `WorkflowRun` and `WorkflowHistory` as first-class concepts.
2. Freeze a narrow Rust <-> TS `workflow-task/v1` ABI.
3. Define bundle assignment and continue-as-new before we build any live upgrade story.

Those three decisions will shape everything else.

## Open Questions

- Should `terracedb-workflows` remain table-oriented internally, or should it become explicitly run-history-oriented in its public surface?
- Should workflow queries be completely ephemeral, or should some classes of query/update leave audit records?
- Should procedure invocation and workflow effects share one effect-task abstraction?
- Should visibility be maintained by a dedicated projector, or updated inline on selected transitions and repaired asynchronously?
- Do we want deployment-ramp semantics in v1, or only current-versus-pinned bundle selection?

## Bottom Line

The strongest change this proposal suggests is not “copy Temporal.” It is:

make TerraceDB workflows history-first, run-based, Rust-owned, and explicitly versioned.

The current repo is already moving in the right direction. Because the project is still greenfield, we should be comfortable reshaping the workflow library around these boundaries now rather than layering them in later once the public surface hardens.
