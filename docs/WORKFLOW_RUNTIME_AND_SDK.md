# Workflow Runtime and SDK Notes

This document explains the current design direction for Terracedb workflows.

It is written as a simple design note. It is not a frozen compatibility contract.

The main goal is to explain:

- how Terracedb workflows should feel to authors,
- how that differs from Temporal,
- how deterministic git changes workflow versioning,
- and what the JavaScript/TypeScript SDK should look like if we keep it small and opinionated.

---

## Two Workflow Paths

Terracedb should support two workflow authoring paths:

- sandbox-authored JavaScript/TypeScript workflows,
- and native Rust workflows.

These two paths should share the same durable workflow model, but they should not be treated as the same runtime environment.

### JavaScript/TypeScript workflows

This is the simpler and safer path.

It is designed for:

- AI-authored code,
- permissioned execution inside a sandbox,
- deterministic runtime-owned JavaScript semantics,
- deterministic git-aware authoring flows,
- and a small, opinionated SDK.

This path should be the default when we want workflows that are easy to write, easy to review, and safe to run with constrained capabilities.

### Rust workflows

This is the power-user path.

It is designed for:

- human-supervised development,
- the highest performance,
- direct access to first-party Rust interfaces,
- and cases where authors may need broad access to internals.

Rust workflows do **not** run in the sandboxed JavaScript runtime.

They do **not** rely on JavaScript runtime overrides for determinism.

Instead, they should stay deterministic by using first-party workflow/runtime interfaces correctly.

They also do **not** participate in the same sandbox permission model, and git is not part of their runtime in the same way.

### Shared workflow goals

Authors should not need to think about:

- manual checkpoint management,
- history-size management,
- `continue-as-new` as routine hygiene,
- or splitting helper code into a separate user-defined activity layer.

Instead, across both paths:

- plain helper functions are just plain code,
- low-level effect boundaries are the real durable boundaries,
- child workflows are the real separate durable execution primitive,
- and checkpointing/history compaction happen automatically in the runtime.

In short:

- plain code,
- injected effects,
- child workflows,
- automatic checkpointing.

---

## How This Differs From Temporal

Temporal TypeScript already gives workflow code a deterministic environment. It does this with a workflow-specific build/runtime path.

So Terracedb's advantage is **not**:

- "we can make `Date.now()` deterministic and Temporal cannot."

Temporal TS already handles that.

The more important differences are these:

### 1. Automatic checkpointing instead of routine `continue-as-new`

In Temporal, authors often have to think about:

- workflow history growth,
- replay cost,
- and when to use `continue-as-new`.

In Terracedb, the runtime should manage this automatically.

The runtime should:

1. watch hidden recovery budgets,
2. checkpoint at safe suspension points,
3. seal old recovery segments,
4. start new hidden segments under the same logical run,
5. apply backpressure before the workflow hits a hard history-size failure.

So `continue-as-new` becomes mostly an internal runtime mechanism, not a routine authoring tool.

### 2. No user-defined activity layer for ordinary code

Temporal has a workflow/activity split.

Terracedb should avoid that as a public authoring model.

Terracedb should instead treat these as the real durable boundaries:

- injected app services,
- injected host capabilities,
- network effects if allowed,
- timers,
- child-workflow spawning.

That means:

- plain helper functions stay plain helper functions,
- and only primitive runtime-managed effects are durable boundaries.

### 3. Visible history is not the same thing as recovery state

Terracedb should separate:

- the hidden recovery journal,
- checkpoints,
- and the visible history shown to humans.

This is important.

It means the runtime can keep the fine-grained data it needs for correctness while showing operators a much simpler history.

### 4. Deterministic git is part of the sandbox runtime model

For sandbox-authored JavaScript/TypeScript workflows, git is not just an external tool. The goal is that git behavior runs on the same deterministic substrate as:

- the VFS,
- the JS runtime,
- timers,
- and sandbox capabilities.

That makes workflow source code itself part of the same reproducible world as workflow execution.

This does **not** apply in the same way to native Rust workflows.

---

## Shared Runtime Model

This section describes the **shared internal runtime shape** that both workflow paths should lower onto.

The key idea is simple:

- keep a fast mutable current state,
- keep a hidden append-only recovery log,
- keep lightweight internal savepoints so replay stays bounded,
- and keep a separate visible history for humans.

This follows the same broad TerraceDB pattern used elsewhere in the architecture:

- one current mutable summary for fast reads,
- one append-only replay surface for correctness,
- and optional derived views for operators and tools.

For workflows, that should mean:

- **current state** is the fast mutable summary of the active run,
- **recovery journal** is the hidden machine log used for replay and crash recovery,
- **savepoints/checkpoints** bound replay and allow old journal segments to be compacted,
- and **visible history** is a separate operator-facing projection.

The important rule is:

- the machine should recover from the recovery journal plus savepoints,
- and humans should read visible history.

Those are related, but they should not be the same table or the same API.

### Plain code

A workflow should mostly look like ordinary async code.

High-level helper functions are just helper functions.

They do not need:

- a special registration API,
- a separate file category,
- or a separate serialization boundary.

### Primitive effects

The real effect boundaries are low-level services and runtime-managed operations.

Examples:

- `payments.authorize(...)`
- `orders.markAuthorized(...)`
- `fetch(...)`
- `setTimeout(...)`
- `wf.spawn(...)`

These are the things the runtime must make durable and replayable.

### Automatic checkpoints

Checkpointing should be automatic.

There should not be an `"auto"` checkpoint mode because automatic checkpointing should just be the normal behavior.

The runtime should checkpoint based on hidden budgets such as:

- effect count,
- recovery-journal bytes,
- estimated replay cost,
- and maybe age.

### Internal savepoints and export checkpoints

There are really two checkpoint needs:

- a cheap internal runtime savepoint used frequently for replay bounding and compaction,
- and a heavier operator-facing checkpoint used for backup, restore, export, or debugging.

These are closely related, but they do not need to be exactly the same thing.

The internal one wants to be:

- cheap,
- frequent,
- optimized for fast local recovery,
- and allowed to be implementation-shaped.

The operator-facing one wants to be:

- more complete,
- more portable,
- more inspectable,
- and suitable for restore or export flows.

So the best model is probably:

- one overall checkpoint concept,
- but with a lightweight internal form for runtime compaction,
- and a heavier external form for admin and tooling workflows.

The runtime should use the lightweight form automatically.

Users should not need to think about it during normal workflow authoring.

### Safe suspension points

The runtime should checkpoint only at safe suspension points.

Examples:

- after awaited effect calls,
- after awaited timers,
- after signal or update delivery,
- after child-workflow interactions.

The runtime does not need arbitrary instruction-level snapshots to get most of the benefit.

### Internal lowering for waits and wakeups

The runtime will still need an internal control-flow model even if the public SDK stays very small.

For example:

- `setTimeout(...)` in sandbox-authored JavaScript/TypeScript code should lower to a runtime timer effect plus a suspend-until-timer-fired state,
- waiting for an external signal should lower to a suspend-until-signal-arrives state,
- retry helpers should lower to durable timers and wakeups,
- and waiting for a child workflow should lower to a durable child-completion wait.

This internal lowering model is important, but it should stay **inside** the runtime.

Users should not need to write "executor directives" directly.

### No `wf.sleep`

For sandbox-authored JavaScript/TypeScript workflows, the runtime should own JavaScript semantics, so ordinary timer APIs should already be durable and replayable.

So this should just work:

```ts
await new Promise((resolve) => setTimeout(resolve, 5_000));
```

There should not need to be a separate `wf.sleep(...)` API.

### Child workflows

The explicit separate durable execution primitive should be a child workflow.

That is what `wf.spawn(...)` is for.

Child workflows are useful for:

- isolation,
- fanout,
- independent lifecycle,
- independent replay budgets,
- and long-lived subflows.

They are not mainly a history-hygiene tool.

---

## Versioning

Workflow versioning should also be described separately for the two workflow paths.

### Sandbox-authored JavaScript/TypeScript workflows

This is where deterministic git gives Terracedb a real advantage.

Because git runs on the same deterministic substrate as the rest of the sandbox/runtime, a workflow version can be treated as a **reproducible source snapshot**, not just an opaque bundle ID.

#### What git buys us

Git can make workflow versioning much more automatic and much more source-native.

The main wins are:

- runs can be pinned to a reproducible source snapshot,
- new versions can be derived from source state instead of hand-written version strings,
- upgrade checks can use actual source diffs,
- and upgrade safety can be tested inside the same deterministic simulation model.

#### A good simplified model

Instead of asking users to manage explicit workflow version strings, Terracedb can derive execution identity from things like:

- source snapshot,
- workflow entrypoint,
- runtime surface,
- and deployment assignment or epoch.

That means a user may never need to name a workflow version manually.

Package resolution should ideally already be determined by:

- the source snapshot,
- the lockfile,
- and the runtime/package-manager mode.

So package identity should not need to be modeled as a separate user-facing fingerprint.

Capability interface versions should also ideally be explicit in generated capability imports, so they are already visible in the source snapshot rather than hidden in a separate fingerprint.

#### What a run should pin to

A workflow run should still be pinned to a specific execution identity.

That identity should likely include:

- the workflow name,
- the source snapshot,
- the entrypoint,
- the runtime surface,
- and the deployment assignment or epoch.

So deterministic git does **not** remove run pinning.

It makes pinning more natural and more reproducible.

#### What can become automatic

If Terracedb uses deterministic git this way, then the system can likely automate:

- version naming,
- source provenance,
- default compatibility checks,
- and preview/prod alignment.

#### What still does not go away

Git does **not** solve everything.

It does not by itself tell us:

- whether persisted workflow state is semantically compatible with new code,
- whether runtime semantics changed,
- or whether an upgrade needs a new run boundary.

So Terracedb still needs:

- run pinning,
- runtime-surface versioning,
- and compatibility decisions.

The difference is that those can be more derived and less manual.

#### Capability interface versioning

For sandbox-authored JavaScript/TypeScript workflows, capability interface versions should be:

- explicit,
- platform-owned,
- and hard for authors or agents to misuse.

The safest model is:

- no handwritten capability strings in workflow code,
- no unversioned "latest" capability imports,
- and generated capability exports as the only normal authoring surface.

That means an agent should import reviewed generated symbols such as:

```ts
import { Payments, Orders } from "@terrace/capabilities";
```

not write free-form identifiers such as:

```ts
wf.service("app.payments.v1")
```

In other words:

- capability versions should be carried by generated symbols and metadata,
- the platform should own those versions,
- and the workflow source should never need to spell capability IDs manually.

#### Capability compatibility is partly unavoidable

Some compatibility questions can be made mechanical.

Examples:

- request/response shape changes,
- missing bindings,
- unsupported capability versions,
- or missing permissions.

Those should fail clearly and early.

But some semantic compatibility questions are unavoidable.

For example:

- a capability keeps the same interface,
- but its behavior changes in a way that may matter to workflow logic.

That kind of ambiguity cannot be eliminated completely.

So the design goal should be:

- make ambiguous cases rare,
- make them visible,
- and fail closed rather than silently blessing them.

In practice that means:

- generated capability exports,
- explicit interface versions,
- preflight validation,
- compatibility tests where practical,
- and human review for ambiguous semantic changes.

#### The likely user-facing result

The public API can probably be much simpler if git is part of the runtime:

- no manual workflow version strings,
- no routine user-facing `continue-as-new`,
- no need for authors to think in bundles most of the time,
- and explicit migration only for the rare cases where the system cannot safely infer compatibility.

### Native Rust workflows

Native Rust workflows should use a simpler and more conventional versioning story.

They are not sandbox-authored, they do not run inside the JavaScript runtime, and they do not have runtime git as part of execution identity.

For Rust workflows, the execution identity should be based on things like:

- workflow name,
- native registration or artifact identity,
- compatibility manifest,
- and deployment/rollout state.

Git may still matter for build provenance and developer workflow, but it should not be described as part of the Rust workflow runtime the way it is for sandbox-authored JavaScript/TypeScript workflows.

---

## Internal Runtime Direction

This section is about the likely direction of the workflow runtime implementation, not the public SDK.

### Recovery journal over visible history

The long-term runtime should not treat visible per-run history as the primary correctness surface.

Instead:

- a hidden recovery journal should be the thing the machine uses for replay,
- visible history should be a projection for humans,
- and old recovery-journal segments should become compactable once covered by a savepoint.

This matters because otherwise one log is forced to do two jobs at once:

- correctness,
- and human-facing audit/history.

When that happens, compaction becomes much harder.

### Automatic compaction should not create a new run

Routine replay bounding should happen by:

- capturing an internal savepoint,
- sealing old recovery-journal segments,
- and continuing under the same logical run.

That is different from `continue-as-new`.

`continue-as-new` should remain available for rare cases like:

- explicit semantic restart boundaries,
- or upgrades that require a new execution target boundary.

It should not be the normal answer to "this workflow has been running for a long time."

### One shared runtime contract, two authoring paths

Both JavaScript/TypeScript workflows and Rust workflows should lower onto one shared internal runtime contract.

That contract should be about:

- admitted triggers,
- deterministic execution turns,
- durable commands and waits,
- recovery journal updates,
- current state updates,
- and visibility projection updates.

The two authoring paths then differ mainly in how determinism is achieved:

- sandbox-authored JavaScript/TypeScript workflows use a host-owned deterministic runtime,
- native Rust workflows use first-party Rust interfaces and stricter native author discipline.

### Likely implementation backtracks

Given the current in-progress implementation, a few likely refactors seem worth calling out explicitly.

1. **History should stop being the main correctness surface.**
   A hidden recovery journal should carry replay correctness, while visible history becomes more freely compactable.

2. **Automatic runtime savepoints should be lighter than export checkpoints.**
   The runtime should not need to write a heavy operator-facing checkpoint artifact every time it wants to bound replay.

3. **The transition model should be unified.**
   There should be one canonical planner for:
   - next state,
   - effect intents,
   - wait/suspend behavior,
   - recovery-journal delta,
   - and visible-history delta.

4. **Native Rust identity should not be forced through sandbox-shaped bundle assumptions.**
   Native workflows should remain first-class execution targets in their own right.

---

## JavaScript/TypeScript SDK

The rest of this section is specifically about the sandbox-authored JavaScript/TypeScript SDK.

The SDK should stay intentionally small.

The core public pieces should be:

- `wf.define(...)`
- `wf.signal(...)`
- `wf.query(...)`
- `wf.spawn(...)`
- generated capability imports

That is enough for the first version.

### `wf.define(...)`

The main authoring surface should look like this:

```ts
import { Payments, Orders } from "@terrace/capabilities";

export const OrderWorkflow = wf.define({
  name: "order",

  input: OrderInputSchema,
  result: OrderResultSchema,

  signals: {
    paymentConfirmed: wf.signal(PaymentConfirmedSchema),
    cancel: wf.signal(CancelSchema),
  },

  queries: {
    status: wf.query(StatusSchema),
  },

  async run(input, ctx) {
    const payments = ctx.use(Payments);
    const orders = ctx.use(Orders);

    const auth = await payments.authorize({
      orderId: input.orderId,
      cents: input.totalCents,
    });

    await orders.markAuthorized({
      orderId: input.orderId,
      authId: auth.id,
    });

    return { status: "authorized" };
  },
});
```

The main fields are:

- `name`
  Stable workflow type name.

- `input`
  Optional schema for validating workflow start input.

- `result`
  Optional schema for validating workflow result.

- `signals`
  Named inbound message types.

- `queries`
  Named read-only query types.

- `run(input, ctx)`
  The main durable workflow body.

### Generated capability imports

Sandbox-authored JavaScript/TypeScript workflows should not normally call `wf.service(...)` directly.

Instead, the host/platform should generate capability exports into something like:

```ts
import { Payments, Orders, GitHub } from "@terrace/capabilities";
```

Those generated exports should be:

- typed,
- stable,
- versioned in platform-owned metadata,
- and always importable in sandbox code.

This is safer for AI-authored code because:

- there are no handwritten capability strings,
- there are no handwritten version strings,
- and agents only choose from reviewed generated symbols.

It also creates much better failure modes.

If a workflow depends on a capability that is unsupported or not granted, preview/publish/deploy should fail with a direct, clear error rather than surfacing as a confusing module-resolution or TypeScript import failure.

`wf.service(...)` can still exist internally as the low-level primitive used by code generation, but it should be treated as codegen-only rather than as normal workflow authoring API.

### What is intentionally missing from `wf.define(...)`

The SDK should not expose all the runtime machinery directly.

That means:

- no checkpoint API,
- no history-size knobs,
- no explicit `continue-as-new`,
- no activity registry,
- no workflow-specific timer primitive,
- no large operational config surface in workflow code.

Those are runtime or deployment concerns.

### `wf.spawn(...)`

The child-workflow API should be very small:

```ts
const child = await wf.spawn(ReceiptWorkflow, {
  orderId: input.orderId,
});

await child.result();
```

The basic shape can be:

```ts
wf.spawn(workflow, input, opts?)
```

With only a very small options object in v1:

```ts
{
  id?: string;
}
```

And a handle like:

```ts
interface WorkflowHandle<R> {
  id: string;
  result(): Promise<R>;
  signal(name: string, payload: unknown): Promise<void>;
  query(name: string, payload?: unknown): Promise<unknown>;
  cancel(reason?: unknown): Promise<void>;
}
```

That is enough to start.

---

## Dependency Injection For Sandbox Workflows

Dependency injection is required for sandbox-authored JavaScript/TypeScript workflows because those workflows live in sandboxes that may contain code referencing many possible injected APIs, while any given sandbox may only have some of them.

We do **not** want missing permissions to become TypeScript import failures.

So Terracedb needs a model where:

- capability contracts are always type-available,
- concrete implementations are injected later,
- and permission failures happen at bind time or runtime.

### Generated service tokens

The simplest model is still an Effect-like service token, but that token should normally be generated rather than handwritten.

Conceptually, generated code would produce something like:

```ts
// generated
export const Payments = /* typed service token */
```

Those generated tokens are:

- always importable,
- typed,
- versioned in platform-owned metadata,
- and independent from whether the current sandbox is actually allowed to use them.

In many cases the source of truth for these sandbox-exposed capabilities will be reviewed host-side code, often authored in Rust and then exposed into sandbox TypeScript through generation.

### `ctx.use(...)` and `ctx.optional(...)`

Workflow code gets injected services from the workflow context.

Example:

```ts
async run(input, ctx) {
  const payments = ctx.use(Payments);
  const github = ctx.optional(GitHub);

  const auth = await payments.authorize({
    orderId: input.orderId,
    cents: input.totalCents,
  });

  if (github) {
    await github.createPR({ repo: input.repo, branch: input.branch });
  }
}
```

Meaning:

- `ctx.use(Service)`
  Required binding. Fail if missing.

- `ctx.optional(Service)`
  Optional binding. Return nothing if missing.

### Binding

At sandbox or deployment time, the host supplies actual implementations.

Example:

```ts
wf.bind(OrderWorkflow, {
  provide: {
    [Payments.key]: paymentsImpl,
    [Orders.key]: ordersImpl,
    [GitHub.key]: wf.denied("app.github is not granted in this sandbox"),
  },
});
```

This should stay simple.

It should just be:

- token -> implementation,
- token -> denied binding,
- or token omitted.

### Why this is better than import-based concrete modules

If capabilities were imported as concrete environment modules, then code written in a lower-permission sandbox could fail to typecheck simply because the module was absent.

That is the wrong failure mode for agent-authored code in an open repo.

Service-token DI solves that problem cleanly.

---

## Schemas Instead Of Codecs

This section is also about the sandbox-authored JavaScript/TypeScript SDK.

### No explicit codec API in v1

Users should not need to define custom codecs for normal workflow code.

Terracedb should ship one built-in durable value format for common workflow boundaries.

That format should support things like:

- primitives,
- plain objects,
- arrays,
- `Date`,
- `bigint`,
- `Uint8Array`.

If a user has a custom class, they should convert it to a plain durable value before crossing a public workflow boundary.

### What users should see

Users should mostly think in terms of **schemas**, not codecs.

Examples:

- input schema,
- result schema,
- signal schema,
- query schema.

Those schemas can come from:

- Zod,
- Valibot,
- Effect Schema,
- or a future Terracedb-native schema system.

The important thing is:

- validation should be easy,
- wire-format design should not be a routine author concern.

### Cross-language contract ownership

The important rule is that we should not hand-maintain the same contract in two languages for the same boundary.

Instead:

- for a sandbox-authored TypeScript workflow boundary, the TypeScript schema is the source of truth,
- for a native Rust workflow boundary, the Rust type is the source of truth,
- and for a cross-language capability boundary, one side should own the contract and the other side should be generated from it.

That means ordinary sandbox-authored TypeScript workflows should **not** require matching Rust structs for every workflow input, output, signal, or query payload.

The Rust runtime can store and route those values as generic durable structured values, and can validate them against emitted schema metadata when needed.

Likewise, native Rust workflows should not require handwritten mirror Zod schemas unless they are intentionally exposing a reviewed capability to sandbox TypeScript.

The normal cross-language generation direction should be:

- reviewed Rust or host-side capability definition,
- generate safe sandbox TypeScript bindings,
- and let AI-authored sandbox workflows consume those generated bindings.

If a mismatch happens anyway, it should fail as early as possible:

- during generation,
- during registration or publish-time validation,
- or during preflight binding checks,

not as a confusing late runtime surprise.

---

## JavaScript/TypeScript Example

```ts
import { z } from "zod";
import { wf } from "@terrace/workflow";
import { Payments, Orders } from "@terrace/capabilities";

const OrderInput = z.object({
  orderId: z.string(),
  totalCents: z.number(),
});

const OrderResult = z.object({
  status: z.string(),
});

export const OrderWorkflow = wf.define({
  name: "order",
  input: OrderInput,
  result: OrderResult,

  async run(input, ctx) {
    const payments = ctx.use(Payments);
    const orders = ctx.use(Orders);

    const auth = await payments.authorize({
      orderId: input.orderId,
      cents: input.totalCents,
    });

    await orders.markAuthorized({
      orderId: input.orderId,
      authId: auth.id,
    });

    await new Promise((resolve) => setTimeout(resolve, 5_000));

    return { status: "authorized" };
  },
});
```

Host-side binding:

```ts
wf.bind(OrderWorkflow, {
  provide: {
    [Payments.key]: paymentsImpl,
    [Orders.key]: ordersImpl,
  },
});
```

What this example shows:

- the workflow body is ordinary async code,
- capabilities come from generated imports,
- services are typed and injected,
- timers use normal JavaScript APIs,
- there is no user-authored activity API,
- there is no explicit checkpoint API,
- and the runtime handles durability underneath.

---

## Rust Workflows

Rust workflows should keep the same high-level durable workflow semantics:

- automatic checkpointing,
- hidden recovery journal,
- visible history separate from recovery state,
- child workflows as the separate durable execution primitive,
- and no need to invent user-authored activities just for history hygiene.

But the authoring model is different:

- they are not sandbox-authored,
- they are not using the JavaScript runtime,
- they do not use the sandbox capability-permission model,
- they do not need the JavaScript SDK described above,
- and they should rely on first-party Rust workflow interfaces to preserve determinism.

Rust workflows are handwritten directly in Rust.

They should not be described as using `wf.service(...)` or sandbox-style generated service tokens as part of normal workflow authoring.

The code-generation story belongs primarily to sandbox-authored JavaScript/TypeScript workflows:

- reviewed host capabilities, often authored in Rust, may be exposed into sandbox TypeScript as generated capability imports,
- but native Rust workflows themselves should usually just call first-party Rust APIs directly.

Rust workflows should be the path for cases where we want the most performance and are willing to trade away some of the safety and constraint advantages of sandbox-authored JavaScript/TypeScript workflows.

### Native Rust workflow example

This example shows the intended *shape* of a native Rust workflow.

It is handwritten directly in Rust.

It uses ordinary Rust dependencies, not sandbox service tokens.

```rust
use async_trait::async_trait;
use std::sync::Arc;

#[derive(Clone)]
pub struct OrderInput {
    pub order_id: String,
    pub total_cents: u64,
}

#[derive(Clone)]
pub enum OrderState {
    Pending,
    Authorized { auth_id: String },
}

pub struct OrderResult {
    pub status: String,
}

#[derive(Clone)]
pub struct OrderWorkflow {
    payments: Arc<dyn PaymentsPort>,
    orders: Arc<dyn OrdersPort>,
}

impl OrderWorkflow {
    pub fn new(
        payments: Arc<dyn PaymentsPort>,
        orders: Arc<dyn OrdersPort>,
    ) -> Self {
        Self { payments, orders }
    }
}

#[async_trait]
impl NativeWorkflow for OrderWorkflow {
    type Input = OrderInput;
    type State = OrderState;
    type Output = OrderResult;

    async fn handle(
        &self,
        input: &Self::Input,
        state: Option<Self::State>,
        ctx: &mut WorkflowCtx,
    ) -> Result<WorkflowTurn<Self::State, Self::Output>, WorkflowError> {
        match state.unwrap_or(OrderState::Pending) {
            OrderState::Pending => {
                let auth = self
                    .payments
                    .authorize(AuthorizeRequest {
                        order_id: input.order_id.clone(),
                        cents: input.total_cents,
                    })
                    .await?;

                self.orders
                    .mark_authorized(MarkAuthorizedRequest {
                        order_id: input.order_id.clone(),
                        auth_id: auth.id.clone(),
                    })
                    .await?;

                ctx.schedule_timer(
                    "follow-up",
                    ctx.now() + std::time::Duration::from_secs(5),
                )?;

                Ok(WorkflowTurn::update(
                    OrderState::Authorized {
                        auth_id: auth.id,
                    },
                    None,
                ))
            }
            OrderState::Authorized { .. } => Ok(WorkflowTurn::finish(OrderResult {
                status: "authorized".to_string(),
            })),
        }
    }
}
```

What this shows:

- the workflow is handwritten Rust,
- dependencies are passed in through ordinary Rust construction,
- the workflow uses first-party workflow/runtime interfaces,
- and timers/effects still lower onto the same shared runtime model underneath.

### Sample Rust dependency implementations

Those Rust dependencies can be ordinary reviewed application code.

```rust
use async_trait::async_trait;
use std::sync::Arc;

pub struct AuthorizeRequest {
    pub order_id: String,
    pub cents: u64,
}

pub struct AuthorizeResponse {
    pub id: String,
}

pub struct MarkAuthorizedRequest {
    pub order_id: String,
    pub auth_id: String,
}

#[async_trait]
pub trait PaymentsPort: Send + Sync {
    async fn authorize(
        &self,
        req: AuthorizeRequest,
    ) -> Result<AuthorizeResponse, WorkflowError>;
}

#[async_trait]
pub trait OrdersPort: Send + Sync {
    async fn mark_authorized(
        &self,
        req: MarkAuthorizedRequest,
    ) -> Result<(), WorkflowError>;
}

pub struct StripePayments {
    stripe: StripeClient,
}

impl StripePayments {
    pub fn new(stripe: StripeClient) -> Self {
        Self { stripe }
    }
}

#[async_trait]
impl PaymentsPort for StripePayments {
    async fn authorize(
        &self,
        req: AuthorizeRequest,
    ) -> Result<AuthorizeResponse, WorkflowError> {
        let charge = self
            .stripe
            .authorize(req.order_id, req.cents)
            .await
            .map_err(WorkflowError::external)?;

        Ok(AuthorizeResponse { id: charge.id })
    }
}

pub struct OrdersStore {
    orders: Table,
}

impl OrdersStore {
    pub fn new(orders: Table) -> Self {
        Self { orders }
    }
}

#[async_trait]
impl OrdersPort for OrdersStore {
    async fn mark_authorized(
        &self,
        req: MarkAuthorizedRequest,
    ) -> Result<(), WorkflowError> {
        let mut batch = self.orders.db().write_batch();
        batch.put(
            self.orders.clone(),
            format!("order/{}", req.order_id).into_bytes(),
            OrderRow {
                auth_id: Some(req.auth_id),
                status: "authorized".to_string(),
            },
        );
        self.orders.db().commit(batch, None).await?;
        Ok(())
    }
}
```

Runtime wiring can then stay simple:

```rust
let workflow = OrderWorkflow::new(
    Arc::new(StripePayments::new(stripe_client)),
    Arc::new(OrdersStore::new(db.table("orders"))),
);

runtime.register_native("order", workflow)?;
```

This is the kind of "injection" Rust workflows should use:

- normal Rust constructors,
- normal Rust traits or concrete types,
- no sandbox permission model,
- and no generated workflow service tokens.

### Rust capability implementation exposed to sandbox TypeScript

Separately, reviewed host capabilities may still be authored in Rust and then exposed to sandbox-authored TypeScript through generation.

One possible shape would look like:

```rust
pub struct PaymentsCapability {
    stripe: StripeClient,
}

impl PaymentsCapability {
    pub fn new(stripe: StripeClient) -> Self {
        Self { stripe }
    }
}

#[terrace_capability_export(name = "app.payments", version = 1)]
impl PaymentsCapability {
    async fn authorize(
        &self,
        req: AuthorizeRequest,
    ) -> Result<AuthorizeResponse, CapabilityError> {
        let charge = self
            .stripe
            .authorize(req.order_id, req.cents)
            .await
            .map_err(CapabilityError::external)?;

        Ok(AuthorizeResponse { id: charge.id })
    }
}
```

That reviewed Rust definition could then generate sandbox-side imports such as:

```ts
import { Payments } from "@terrace/capabilities";
```

This is the main code-generation direction:

- reviewed Rust or host-side capability definitions
- generate safe sandbox TypeScript imports
- let AI-authored sandbox workflows consume only those generated symbols

It is **not** the normal authoring model for native Rust workflows themselves.

---

## Implementation Backlog

These tasks are meant to capture the backtracking and cleanup work implied by the design in this document.

They are intentionally written in the style of [TASKS.md](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/TASKS.md), but they live here because they are specifically about the workflow runtime and SDK direction described in this note.

The current in-progress implementation and the exploratory example branch `origin/codex/t114-workflow-duet` should be treated as useful prototypes, not as the final direction.

**Phase rule:** freeze the corrected shared runtime model first, then rework the implementation around it, then rebuild the example app and SDK surfaces on top of that corrected foundation. Do not keep piling more public workflow UX onto the current history-centric runtime shape.

**Parallelization:** W01 first. W02 and W03 can proceed in parallel once W01 exists. W04 and W05 depend on W01 plus enough of W02/W03 to stabilize their runtime assumptions. W06 depends on W04 and W05.

### W01. Freeze the revised workflow runtime model around hidden recovery journals, lightweight savepoints, and separate visible history

**Depends on:** none

**Description**

Freeze the corrected shared workflow model before more implementation work piles onto the current shape. The key change is that visible per-run history should stop being the primary recovery truth. The authoritative recovery model should instead be:

- current mutable workflow state,
- hidden recovery journal segments,
- lightweight internal savepoints,
- and a separate visible history projection for humans.

This task should also freeze the rule that sandbox-authored JavaScript/TypeScript workflows and native Rust workflows share the same durable workflow model without sharing the same authoring API or determinism mechanism.

**Implementation steps**

1. Freeze the shared runtime concepts and contracts for:
   - recovery journal segments,
   - lightweight internal savepoints,
   - visible history projection,
   - child workflow boundaries,
   - and the shared run lifecycle model.
2. Freeze the rule that automatic compaction and checkpointing should not create a new logical workflow run as routine hygiene.
3. Freeze the distinction between:
   - lightweight internal runtime savepoints used for replay and compaction,
   - and heavier export, restore, or operator checkpoints.
4. Freeze the rule that sandbox-authored JavaScript/TypeScript workflows and native Rust workflows lower onto the same shared runtime contract, but keep different public authoring surfaces.
5. Mark the older assumptions behind append-only visible run history and routine `continue-as-new` hygiene as superseded by this new model.

**Verification**

- Compile-only tests that instantiate the revised shared workflow contracts without requiring a visible-history-as-truth model.
- Design-note updates in this file and the architecture docs that describe one consistent recovery-journal-plus-savepoint model.
- A deterministic smoke test plan showing the same logical workflow can target the frozen shared runtime model from both sandbox and native authoring paths.

### W02. Rework workflow recovery and compaction so correctness depends on hidden recovery state rather than visible history

**Depends on:** W01

**Description**

Rework the in-progress runtime so crash recovery and replay depend on hidden recovery state, not on a contiguous visible per-run history log. This is the main backtrack required to support automatic compaction, smaller replay windows, and a cleaner operator-facing history model.

**Implementation steps**

1. Make the hidden recovery journal, or a close successor to the current trigger journal path, the authoritative replay surface.
2. Add lightweight internal savepoints that the runtime can create frequently enough for automatic compaction and replay bounding.
3. Keep the existing heavier checkpoint/export path only for operator backup, restore, or forensics flows rather than as the hot-path compaction mechanism.
4. Demote visible per-run history into a projection or audit surface that can be compacted or summarized independently of recovery correctness.
5. Remove runtime assumptions that visible history must remain the single contiguous source of truth for current workflow state and lifecycle validation.

**Verification**

- Crash and replay tests proving workflows recover from internal savepoints plus hidden recovery journal segments without depending on full visible history replay.
- Compaction tests proving old recovery segments can be sealed or discarded once covered by a savepoint.
- Visibility rebuild tests proving human-facing history can be regenerated or summarized without changing replay behavior.

### W03. Unify transition execution and finish the internal wait, wakeup, timer, retry, and signal model

**Depends on:** W01

**Description**

Finish the internal workflow control-flow model so timers, retries, external signals, and child-workflow waits are real runtime behavior rather than half-finished scaffolding. This task should also remove the current split where transition planning and reduction are modeled in two overlapping places.

**Implementation steps**

1. Choose one canonical transition engine and merge or delete the overlapping reducer or planner path so one implementation owns state change, wait state, and effect planning.
2. Make internal directives real runtime behavior for:
   - suspend until timer,
   - suspend until signal,
   - retry later,
   - child-workflow completion waits,
   - complete,
   - and fail.
3. Lower sandbox-authored JavaScript/TypeScript timer behavior such as `setTimeout(...)` onto that internal directive model rather than exposing special timer APIs in the public SDK.
4. Ensure external signals, wakeups, timer firings, retries, and child completions all admit through the same durable transition machinery.
5. Remove or backtrack code paths that currently define wait or directive concepts but still reject them in the live runtime loop.

**Verification**

- Deterministic tests for timer suspension, signal wakeup, retry scheduling, stale wakeup suppression, and child-workflow completion.
- Parity tests proving the same logical wait behavior works from both native Rust and sandbox-authored workflow paths.
- Restart tests proving pending waits, retries, and timers survive crash and replay correctly.

### W04. Simplify the sandbox JavaScript/TypeScript SDK around generated capability imports, schemas, and automatic runtime behavior

**Depends on:** W01, W02, W03

**Description**

Build the smaller sandbox SDK that this document describes, and backtrack away from the lower-level workflow-task-shaped authoring style that has started to show up in exploratory examples. The sandbox SDK should be optimized for AI-authored code being hard to misuse and easy to validate.

**Implementation steps**

1. Keep the public sandbox workflow surface small and opinionated:
   - `wf.define(...)`,
   - `wf.signal(...)`,
   - `wf.query(...)`,
   - and `wf.spawn(...)`.
2. Do not expose user-authored activities, explicit checkpoint APIs, or special timer helper APIs such as `wf.sleep(...)` in the normal sandbox authoring surface.
3. Treat `wf.service(...)` as internal or codegen-only and move ordinary authored code to generated imports such as `@terrace/capabilities`.
4. Use schemas rather than explicit user-authored codecs for sandbox workflow inputs, outputs, signals, and queries, and emit any normalized schema metadata needed by the Rust runtime from the build or publish path.
5. Add fail-fast validation so missing capability bindings, unsupported runtime surfaces, schema mismatches, and permission denials become immediate clear signals instead of confusing late runtime failures.

**Verification**

- Typecheck tests proving sandbox workflows can only consume generated capability exports rather than handwritten service identifiers.
- Build or publish tests proving schema metadata is emitted and validated without requiring Rust codegen for ordinary sandbox workflow boundaries.
- Preview and deploy tests proving missing capabilities, unsupported versions, or invalid bindings fail clearly before execution begins.

### W05. Keep native Rust workflows first-class while removing sandbox-specific assumptions from the core runtime

**Depends on:** W01, W02, W03

**Description**

Keep native Rust workflows as a real first-class path while cleaning sandbox-specific assumptions out of the shared workflow runtime. Rust workflows should share the durable execution model, including automatic compaction, but they should not be described as using sandbox permissions, generated service tokens, deterministic git, or JavaScript runtime shims.

**Implementation steps**

1. Keep native Rust workflow authoring handwritten and based on first-party Rust workflow interfaces and ordinary Rust dependency wiring.
2. Ensure automatic compaction, hidden recovery journals, and internal savepoints apply to native Rust workflows too, even though Rust does not use the sandbox runtime.
3. Remove or backtrack internal assumptions that native workflows must masquerade as sandbox-shaped bundle identities when a native registration or similar concept is the real execution target.
4. Keep native workflow versioning tied to native registrations, rollout, compatibility policy, and deployment identity rather than to sandbox-specific git runtime provenance.
5. Preserve a clear boundary where reviewed Rust host capabilities may be exposed into sandbox TypeScript through generation, while native Rust workflows themselves remain ordinary handwritten Rust.

**Verification**

- Tests proving native Rust workflows recover, compact, and replay correctly under the same shared runtime model as sandbox workflows.
- Registration and rollout tests proving native workflow identity does not depend on sandbox-shaped bundle assumptions.
- Documentation and API examples that show native Rust workflows as a first-class path rather than as a special case of sandbox execution.

### W06. Rework the example workflow app and salvage the useful parts of `origin/codex/t114-workflow-duet`

**Depends on:** W04, W05

**Description**

Rework the premature example-app implementation after the runtime and SDK direction above is real enough to build on. The current branch `origin/codex/t114-workflow-duet` is still useful, but it reflects the earlier history-centric and low-level authoring model. We should salvage the domain logic, tests, and teaching intent while discarding the parts that bake in the wrong workflow surface.

**Implementation steps**

1. Review `origin/codex/t114-workflow-duet` and separate:
   - reusable app or domain logic,
   - useful simulation coverage,
   - and low-level workflow authoring patterns that should be discarded.
2. Rebuild the example so the sandbox workflow uses the smaller JavaScript/TypeScript SDK from W04 rather than returning raw low-level workflow-task command structures.
3. Rebuild the native Rust example so it uses the cleaned-up shared runtime model from W05 rather than any sandbox-shaped identity assumptions.
4. Demonstrate the corrected workflow story in the example:
   - automatic compaction,
   - hidden recovery journal vs visible history,
   - child workflows,
   - durable waits and timers,
   - and sandbox-versus-native authoring differences.
5. Keep the example small enough to teach the intended public workflow model rather than exposing internal reducer or recovery details directly.

**Verification**

- Example-level tests proving the rebuilt app uses only the intended public workflow APIs for sandbox and native paths.
- Deterministic simulation tests covering restart, waits, timers, and child-workflow behavior in the rebuilt example.
- Review notes showing which pieces from `origin/codex/t114-workflow-duet` were intentionally kept, rewritten, or dropped.

---

## Additional Follow-On Tasks

These tasks came out of actually implementing the runtime and duet example above. They are narrower than W01-W06 and mostly reflect places where the current design is right but the current product surface is still more awkward than it should be.

### W07. Let sandbox workflows target TypeScript entrypoints directly instead of manually emitting JavaScript first

**Depends on:** W04

**Description**

The current duet example proved that a sandbox workflow can be authored in TypeScript and use npm packages, but the host still had to install packages and call `emit_typescript(...)` before the workflow runtime could open the emitted JavaScript entrypoint. That is the wrong long-term authoring model. Workflow publication or runtime open should accept TypeScript entrypoints directly and make package installation plus TypeScript emission an internal step.

**Implementation steps**

1. Allow sandbox workflow bundle metadata or publication requests to name a TypeScript source entrypoint directly.
2. Move package install, typecheck, and emit into the workflow preparation path instead of forcing example or application code to call them manually.
3. Record the emitted runtime entrypoint and any related preparation metadata as runtime-owned internal state rather than as ad hoc application setup.
4. Keep the built-in workflow SDK and generated capability imports available to the TypeScript authoring surface without requiring manual mirror setup in each example.
5. Remove or backtrack example code paths that have to seed a pre-emitted JavaScript file just to run a TypeScript-authored workflow.

**Verification**

- Example-level tests where the sandbox workflow source exists only as `.ts`, with no preseeded `.js` entrypoint.
- Publish or runtime-open tests proving npm install and TypeScript emit happen automatically and deterministically.
- Restart tests proving a reopened runtime can reuse the prepared TypeScript artifact without redoing unnecessary work.

### W08. Either support a fuller TypeScript surface or define and enforce a much clearer supported subset

**Depends on:** W04

**Description**

Implementing the TypeScript duet workflow exposed that the current deterministic TypeScript path is still a constrained transpiler rather than a full TypeScript compiler. That is acceptable only if the supported subset is clear, intentionally small, and enforced early. Otherwise AI-authored workflows will keep accidentally writing syntax that looks reasonable but fails later in confusing ways.

**Implementation steps**

1. Decide whether the deterministic TypeScript path should stay a constrained subset for now or move closer to a fuller compiler surface.
2. If it stays constrained, document the supported subset explicitly and reject unsupported syntax early with direct diagnostics.
3. If it expands, prioritize the syntax patterns most likely to appear in AI-authored workflow code:
   - common type aliases,
   - interfaces,
   - multiline declarations,
   - ordinary return type annotations,
   - and straightforward object typing.
4. Add conformance fixtures for intentionally supported and intentionally unsupported syntax so regressions are obvious.
5. Keep the workflow example suite on the supported surface so the public examples do not silently rely on quirks.

**Verification**

- TypeScript conformance tests for supported workflow-source syntax.
- Clear failing diagnostics for unsupported syntax rather than malformed emitted JavaScript.
- Example tests proving the public workflow examples stay inside the intended TypeScript surface.

### W09. Make package compatibility mode a first-class workflow publication concept rather than buried host setup

**Depends on:** W04, W07

**Description**

The difference between “Terrace-only imports” and “real npm-backed workflow project” is currently too easy to hide inside host-side sandbox config. That makes workflow validity depend on operational setup instead of on the authored workflow package or publication request. Package compatibility should be a clear authored or published property of the sandbox workflow itself.

**Implementation steps**

1. Add explicit package-compat or package-surface metadata to sandbox workflow publication or bundle configuration.
2. Validate that metadata before runtime open so a workflow that expects npm packages cannot accidentally run in `TerraceOnly` mode.
3. Keep import-surface diagnostics aligned with that declared compatibility mode.
4. Surface the chosen compatibility mode in workflow inspection or bundle metadata so operators can see it easily.
5. Remove or reduce host-side example code that implicitly decides package mode without the workflow package declaring it.

**Verification**

- Publication tests proving workflows that depend on npm packages fail early when published against a Terrace-only compatibility mode.
- Loader and typecheck tests proving imports are accepted or rejected consistently with the declared mode.
- Inspection or metadata tests proving the chosen mode is visible after publication.

### W10. Introduce a higher-level first-class workflow-to-workflow interaction primitive so simple relays do not require custom outbox plumbing

**Depends on:** W03, W04, W05

**Description**

The duet example showed that cross-runtime interaction works well over durable outbox plus callback surfaces, but it also showed that the raw relay loop is more infrastructure-shaped than most users should have to write. The runtime should grow a more direct primitive for common workflow-to-workflow interaction patterns.

**Implementation steps**

1. Decide which interaction should become first-class first:
   - child workflow spawn,
   - cross-workflow signal,
   - message send,
   - or a small combination of those.
2. Lower that primitive onto the same durable outbox, inbox, callback, or signal machinery already used by the relay implementation.
3. Keep the relay-level mechanics internal so public workflow code does not have to scan raw outbox tables for common interactions.
4. Make the primitive work for both sandbox-authored workflows and native Rust workflows.
5. Update the duet example to prefer the higher-level primitive once it exists.

**Verification**

- Example tests proving a native Rust workflow and a sandbox TypeScript workflow can interact without custom outbox relay code.
- Restart and replay tests proving the higher-level primitive preserves deterministic behavior.
- Internal parity tests proving the new primitive still lowers to the durable messaging machinery correctly.

### W11. Add better runtime and harness readiness helpers for deterministic tests and local examples

**Depends on:** W03

**Description**

The duet example exposed avoidable races around “workflow has been started” versus “workflow has reached its first stable state” before clocks or interactions advance. The current example solved that with `kick_off_pair(...)`, but the underlying need is broader. The runtime and harness should expose better readiness helpers so deterministic tests and demos do not have to reinvent them.

**Implementation steps**

1. Add runtime or harness helpers for common readiness states such as:
   - workflow runtime loop started,
   - instance exists,
   - instance reached first persisted state,
   - or instance reached a named visible status.
2. Prefer those helpers in examples and deterministic tests rather than hard-coded sleeps or guesswork.
3. Ensure readiness helpers work the same way for sandbox and native workflows.
4. Keep readiness waiting durable-state-based rather than relying on wall-clock timing.
5. Remove ad hoc example-specific readiness scaffolding where a shared helper is sufficient.

**Verification**

- Example tests rewritten to use readiness helpers rather than incidental timing assumptions.
- Deterministic simulation tests proving readiness helpers behave consistently across repeated seeds.
- Crash and restart tests proving readiness checks still work after reopening runtimes.

### W12. Treat package installation and TypeScript emission as reusable cached workflow build artifacts

**Depends on:** W07, W09

**Description**

Preparing a sandbox TypeScript workflow currently wants to reinstall packages and re-emit TypeScript in places where the result should be reusable. That work should become a cached workflow build artifact keyed by the source snapshot, lockfile, declared package compatibility mode, and runtime surface.

**Implementation steps**

1. Define the cache key for prepared sandbox workflow artifacts from:
   - source snapshot,
   - package manifests or lockfiles,
   - package compatibility mode,
   - and runtime surface.
2. Cache package-install manifests, compatibility views, and emitted workflow entrypoints under that key.
3. Reuse prepared artifacts across runtime open, replay, preview, and deterministic tests when the key matches.
4. Invalidate cached artifacts cleanly when source, lockfile, package mode, or runtime surface changes.
5. Keep the cache an internal optimization rather than a new author-facing concept.

**Verification**

- Repeated-open tests proving package install and emit work become cache hits when the input key is unchanged.
- Invalidation tests proving source or lockfile changes produce fresh prepared artifacts.
- Replay and preview tests proving cached prepared artifacts do not change workflow semantics.

### W13. Keep visible workflow summaries simple and easy to assert against

**Depends on:** W02, W04, W05

**Description**

The duet implementation used workflow visibility as the clearest place to prove that the sandbox workflow had actually executed `lodash.camelCase(...)`. That is a strong signal that the visible-summary surface is valuable both for operators and for tests. We should preserve that property deliberately.

**Implementation steps**

1. Keep workflow visibility summaries small, string-keyed, and easy to inspect from both sandbox and native paths.
2. Make the public inspection surface return the latest visible summary directly, not just a history list.
3. Encourage examples and tests to assert important user-visible behavior through visibility summaries when appropriate.
4. Keep visibility projection separate from recovery truth so it remains easy to read without constraining compaction.
5. Add guidance for when workflow authors should put something in visibility versus in hidden recovery state.

**Verification**

- Inspection tests proving the latest visibility summary is easy to fetch and compare.
- Example tests that assert on real user-visible summary data, not just internal state transitions.
- Rebuild tests proving visibility can change presentation without affecting replay correctness.

---

## Glossary

- **Workflow**: Durable code that can pause, resume, and survive crashes.

- **Workflow definition**: The reusable code template for a workflow type. In the SDK this is created by `wf.define(...)`.

- **Workflow run**: One concrete execution of a workflow definition.

- **Child workflow**: A workflow started by another workflow. Child workflows are created with `wf.spawn(...)`.

- **Primitive effect**: A low-level effect boundary the runtime must record and replay correctly. Examples are injected services, network calls, timers, and child-workflow spawning.

- **Injected service**: A typed capability provided to sandbox-authored workflow code by the host or sandbox at runtime.

- **Capability**: A thing the host allows sandbox or workflow code to do. Examples are database access, payment APIs, git actions, or network access.

- **Dependency injection (DI)**: A way to make capability contracts always importable while binding concrete implementations later.

- **Service token**: A typed handle used to ask for an injected service. In this document, sandbox-authored TypeScript normally receives these through generated capability exports rather than by calling `wf.service(...)` by hand.

- **Binding**: A concrete mapping from a service token to an implementation, a denial, or nothing.

- **Deterministic runtime**: A runtime where guest-visible behavior such as time, randomness, timers, and scheduling is controlled by the host so replay behaves the same way every time. In this document, that mainly refers to the sandbox JavaScript/TypeScript runtime.

- **Replay**: Re-running workflow logic from durable state so the system can recover after crash or restart.

- **Recovery journal**: The hidden durable log the runtime uses for correctness during replay.

- **Checkpoint**: A durable snapshot the runtime stores so replay does not need to start from the very beginning every time.

- **Segment**: One bounded chunk of the hidden recovery journal. The runtime can seal old segments after checkpointing.

- **Visible history**: The operator-facing history shown to humans. It should be simpler than the hidden recovery journal.

- **Safe suspension point**: A place where the runtime can safely checkpoint, such as after an awaited effect or timer.

- **`continue-as-new`**: A workflow-system pattern where one run ends and a fresh run starts with carried-over state. In this design, that should mostly become an internal runtime mechanism rather than routine authoring work.

- **Runtime surface**: The guest-visible runtime semantics that matter for compatibility. Examples include timer behavior, deterministic globals, and capability behavior.

- **Source snapshot**: The exact source tree state used to define a workflow version. With deterministic git, this can be treated as part of workflow execution identity.

- **Bundle**: A packaged workflow artifact. Terracedb may still use bundles internally even if the public API does not make users think about them often.

- **Native registration**: A way to register a workflow implemented directly in Rust rather than as a sandbox-authored bundle.

- **Deployment**: The operational record that says which workflow artifact is active in a given environment.

- **Rollout**: The policy that decides which new runs get a new deployment.

- **Signal**: A named inbound message delivered to a running workflow.

- **Query**: A read-only request against a running workflow.

- **Schema**: A definition of the shape of a value. Schemas are mainly for validation.

- **Codec**: A way to encode and decode values across durable boundaries. This document recommends hiding explicit codec design in v1 for common cases.

- **Capability interface version**: The reviewed version of a host capability contract that sandbox code is allowed to depend on. Authors should normally consume these through generated imports rather than handwritten identifiers.

- **Provider revision**: An internal implementation revision of a capability provider. Small implementation changes may happen here without changing the capability interface version.

- **Sandbox**: The isolated Terracedb execution environment where agent-authored JavaScript/TypeScript code runs with specific capabilities, package compatibility mode, and execution policy.

- **Git provenance**: Metadata about the repo state a sandbox or workflow source came from, such as repo root, branch, commit, and dirty state.

- **Deterministic git**: Git behavior running on the same deterministic substrate as the rest of the sandbox runtime, so repo operations are reproducible and simulation-friendly.

---

## Implementation Lessons

Implementing the first real version of this model surfaced a few useful simplifications.

### 1. `@terrace/workflow` should be a built-in module

Seeding a helper file like `./sdk/workflow.js` into each sandbox worked as a prototype, but it is the wrong long-term shape.

The better model is:

- `@terrace/workflow` is always available in sandbox-authored workflow code,
- it is versioned as part of the sandbox runtime surface,
- and examples/tests should not need to copy helper files into the workspace.

This removes one source of version drift and makes agent-authored code more obvious.

### 2. Generated capabilities need to exist for tooling too, not just runtime loading

It is not enough for `@terrace/capabilities` to resolve only when the code executes.

Tooling should also know about it.

That means:

- generated capability exports should be part of the typecheck/preflight world,
- generated declarations should be materialized for the sandbox,
- and unknown non-relative imports should fail clearly instead of slipping through.

This is especially important for agent-authored code because we want mistakes to fail early and loudly.

### 3. The live runtime should prefer one transition path

The implementation originally kept two different runtime paths alive:

- one reducer-shaped path,
- and one transition-engine path.

That turned out to be more confusing than helpful.

The better direction is:

- route ordinary transitions through one transition engine,
- keep the hidden execution snapshot model there,
- and only keep special handling where the model truly differs, such as active-run rollover for `continue-as-new`.

### 4. Internal savepoints should be cheaper and more selective

Writing a savepoint after every transition was a good correctness-first move, but it is not the right long-term default.

The runtime should only need an internal savepoint when the execution snapshot contains information that cannot be reconstructed cheaply from current state alone, such as:

- pending waiters,
- retry state,
- owned timers,
- or richer applied-task state.

This keeps the automatic-compaction model while making the hot path cheaper.

### 5. Recovery progress should not be tied too closely to visible-history accounting

Using visible-history-style counters for savepoint coverage was convenient while the implementation was in flux, but it risks coupling recovery internals back to the projection layer.

The long-term direction should be:

- visible history remains a projection for humans,
- recovery keeps its own progress watermark,
- and savepoint coverage is tracked in recovery terms rather than history-UI terms.

### 6. Keep sandbox conveniences out of the native Rust path

The implementation work reinforced the original architectural split:

- sandbox TypeScript wants built-in modules, generated capability imports, permission-aware diagnostics, and deterministic git,
- native Rust wants direct handwritten code over first-party interfaces.

Trying to make native Rust look sandbox-shaped adds confusion without improving the core durable workflow model.

---

## Short Version

The intended model is:

- both JavaScript/TypeScript workflows and Rust workflows should share the same durable workflow model,
- JavaScript/TypeScript workflows are the safer sandboxed path for AI-authored code,
- Rust workflows are the higher-power path for human-supervised code and top-end performance,
- child workflows should be the main separate durable execution primitive,
- the runtime should handle checkpointing and history compaction automatically,
- sandbox-authored JavaScript/TypeScript workflows should use a small SDK plus DI,
- and deterministic git should simplify versioning for sandbox-authored workflows, not for native Rust workflows.
