# Node Compatibility Architecture

## Goal

Make the sandbox a **deterministic Node host**.

That means:

- run real upstream Node JavaScript code
- run real `npm`
- run real `tsc`
- keep control over:
  - file I/O
  - time and scheduling
  - randomness
  - networking
  - child processes

The goal is **not** to hand-reimplement the public Node API in Rust or in a large handwritten JS compatibility file.

## Core Model

The architecture should have three layers:

1. **Terrace primitives**
   Deterministic Rust-owned primitives for filesystem, timers, entropy, networking, child processes, and source loading.

2. **Node internal bridge**
   A small shim that exposes those primitives in the shapes Node expects through `internalBinding(...)`, `primordials`, and a few `internal/*` seams.

3. **Upstream Node JS**
   As much of Node's own JavaScript code as possible, loaded with its original layout intact.

The main rule is:

- **replace low-level nondeterministic primitives**
- **reuse high-level Node JavaScript**

## Why This Works

Node already has the seams we need.

Examples from the Node source:

- [lib/internal/process/task_queues.js](/Users/maxwellgurewitz/dev/node/lib/internal/process/task_queues.js)
  uses `internalBinding('task_queue')`
- [lib/timers.js](/Users/maxwellgurewitz/dev/node/lib/timers.js)
  uses `internalBinding('timers')`
- [lib/fs.js](/Users/maxwellgurewitz/dev/node/lib/fs.js)
  and [lib/internal/fs/promises.js](/Users/maxwellgurewitz/dev/node/lib/internal/fs/promises.js)
  use `internalBinding('fs')`
- [lib/net.js](/Users/maxwellgurewitz/dev/node/lib/net.js)
  uses `internalBinding('tcp_wrap')`, `stream_wrap`, `pipe_wrap`, and `cares_wrap`
- [lib/child_process.js](/Users/maxwellgurewitz/dev/node/lib/child_process.js)
  and [lib/internal/child_process.js](/Users/maxwellgurewitz/dev/node/lib/internal/child_process.js)
  use `pipe_wrap`, `stream_wrap`, `tcp_wrap`, and `spawn_sync`
- [lib/internal/crypto/random.js](/Users/maxwellgurewitz/dev/node/lib/internal/crypto/random.js)
  uses `internalBinding('crypto')`

Those JS files already separate:

- public JavaScript behavior
- internal host/native primitives

That is exactly the separation we want.

## Important Distinction: `require(...)` vs `internalBinding(...)`

These are different mechanisms.

### `require(...)`

Loads JavaScript modules.

Examples:

- `require('fs')`
- `require('path')`
- `require('internal/fs/utils')`

This should load upstream Node JS.

### `internalBinding(...)`

Loads host/internal primitive bindings.

Examples:

- `internalBinding('fs')`
- `internalBinding('timers')`
- `internalBinding('crypto')`

This is where we inject Terrace behavior.

So:

- `require('fs')` should load upstream [lib/fs.js](/Users/maxwellgurewitz/dev/node/lib/fs.js)
- inside that file, `internalBinding('fs')` should return our deterministic fs binding

That is the key architectural seam.

## What We Should Reuse

### Reuse Almost Wholesale

These are strong candidates for direct upstream reuse:

- `path`
- `querystring`
- `punycode`
- much of `events`
- CommonJS and ESM loader logic

### Reuse As JS Wrappers Over Terrace Primitives

These should increasingly come from upstream Node JS, but backed by Terrace primitives:

- `fs`
- `fs/promises`
- `url`
- `os`
- `stream`
- `http`
- `https`
- `dns`
- `tls`
- `child_process`

### Keep Terrace-Owned Primitive Backends

These are the things we must truly own:

- VFS-backed file operations
- deterministic timers and microtasks
- deterministic entropy
- deterministic network and DNS mediation
- deterministic child process execution
- crypto backends
- source loading and compilation hooks

## Node Source Management

The upstream Node source should be a pinned git submodule.

Why:

- the exact upstream version is recorded in git
- upgrades become explicit
- local patches can live on a clear branch if we ever need them
- we preserve Node's original layout instead of copying large pieces around

The submodule should stay a clean upstream mirror.

## Bootstrapping Strategy

There is a bootstrap loop if we insist that sandboxed `npm` must create the very first `node_modules` trees we test with.

We should not insist on that.

Use three distinct inputs:

1. **Pinned Node source**
   The Node submodule, mounted or loaded read-only into the sandbox.

2. **Pinned package projects**
   Separate fixture projects such as:
   - `npm/cli`
   - minimal canary apps

3. **Pre-materialized dependency trees**
   Created on the host with real `npm ci`, then hoisted or mounted read-only into the sandbox.

This breaks the bootstrap loop cleanly:

- first prove the sandbox can execute real Node and real npm code
- only later require sandboxed npm to produce new installs itself

## Git/VFS Integration

TerraceDB's git and VFS support makes this easier.

Recommended dev/test layout:

1. Terrace overlay
2. pinned upstream Node tree
3. pinned npm or canary project tree
4. pre-materialized `node_modules` tree
5. project-specific writable overlay

For normal execution, derive a frozen snapshot from those inputs and key it by:

- Node commit
- package lockfile
- source tree hash
- compatibility target version

## Testing Strategy

Use three kinds of tests.

### 1. Imported Node Tests

Port Node's own tests wherever practical.

Good news:

- most of Node's tests are plain JS files
- many can run as ordinary Node processes with light adaptation
- they do not generally require mocha or jest

Some use Node's own harness helpers, so they still need selective porting, but the format is favorable.

### 2. Differential Parity Tests

Run the same script in:

- real host Node
- the Terrace sandbox

Compare:

- stdout/stderr
- thrown errors
- event ordering
- stream behavior
- promise/microtask behavior

### 3. Ecosystem Canaries

Use real packages as canaries:

- `npm`
- `typescript`
- `make-fetch-happen`
- `minipass`
- `minipass-fetch`
- `@npmcli/agent`

These are not the spec. They are reality checks.

## Debugging Strategy

Keep the sandbox flight recorder.

It should remain a debug-only layer that captures:

- structured runtime traces
- module resolution traces
- builtin access traces
- exception provenance
- JS-to-host trace events
- optional autoinstrumentation in dev/test

It is valuable and reusable, but it should not become part of the semantic implementation.

## Near-Term Milestone

The first meaningful milestone is:

1. create a sandboxed Node project
2. run real `npm install lodash`
3. run a script that uses `lodash`
4. verify the result matches host Node

That milestone should be reached in this order:

1. run upstream Node JS on top of Terrace primitives
2. load pre-materialized third-party dependency trees
3. run real npm bootstrap and fetch paths
4. only then require sandboxed npm to create new installs

## Concrete Tasks

### [N01] Pin And Mount Upstream Node

- add the pinned Node submodule
- preserve its layout
- load Node JS from that tree instead of copying it into handwritten compatibility files

### [N02] Keep The Boundary Low

- treat `internalBinding(...)` as the host injection seam
- treat `require(...)` as the JS loader path
- stop growing handwritten public builtin modules unless unavoidable

### [N03] Move Core JS To Upstream Sources

Start with:

- `path`
- `querystring`
- `events`
- CommonJS loader helpers

Then move into:

- `fs`
- `fs/promises`
- `url`
- `stream`

### [N04] Stabilize Primitive Bindings

Make deterministic primitives explicit for:

- fs
- timers and task queues
- crypto randomness
- networking and DNS
- TLS
- child processes
- compilation/source loading

### [N05] Keep Imported Tests And Parity Tests Green

- keep broad CommonJS coverage
- keep broad ESM coverage
- keep builtin-specific contract suites
- add more direct parity tests against real Node as we swap in upstream JS

### [N06] Support Pre-Materialized npm Trees

- mount pinned host-built `node_modules` trees read-only into the sandbox
- use them to validate runtime correctness before self-hosting npm installs

### [N07] Reach The `lodash` Canary

- run real npm bootstrap
- run real registry fetch/install
- install `lodash`
- execute a script using it
- compare with host Node

### [N08] After `lodash`, Bring Up Real `tsc`

- let users pin `typescript` in `package.json`
- run real `tsc` inside the same runtime
- keep it on the same architecture, not a separate Rust-owned compiler path

## Design Rules

- Prefer upstream Node JS over handwritten compatibility code.
- Prefer primitive hooks over public host-object reimplementation.
- Preserve upstream layout.
- Keep debug tooling separate from runtime semantics.
- Treat imported Node tests as the main compatibility contract.
- Treat npm and tsc as canaries, not the definition of correctness.

## Current Code To Deprecate Or Move

We already have a meaningful amount of Node-compat work in tree.

The right posture is not "throw it all away." It is:

- keep the primitive/runtime substrate
- keep the tracing and test corpus
- shrink or delete the handwritten public builtin layer

### Deprecate Or Replace

These are the main areas that do not fit the long-term architecture and should be retired as we move onto upstream Node JS.

- [crates/terracedb-sandbox/src/node_compat_bootstrap.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/node_compat_bootstrap.js)
  The large `__terraceBuiltin(...)` switch is currently acting like a handwritten public Node stdlib. That was useful for bring-up, but it is the wrong ownership boundary long-term.
- [crates/terracedb-sandbox/src/node_compat_bootstrap.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/node_compat_bootstrap.js)
  `__terraceWrapBuiltinValue(...)` and `__terraceCreateBuiltinStubModule(...)` are useful debugging tools, but they should become debug-only helpers, not the way production builtins behave.
- [crates/terracedb-sandbox/src/loader.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/loader.rs)
  `load_node_builtin_module(...)` currently special-cases a small preview-style builtin path. That should be replaced by loading upstream Node JS with a thinner builtin/internal bridge.
- [crates/terracedb-sandbox/src/packages.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/packages.rs)
  `DeterministicPackageInstaller` and the hard-coded fake registry are explicitly contrary to the real npm plan and should be removed once bootstrap moves to pinned host-built dependency trees.
- [crates/terracedb-sandbox/src/typescript.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/typescript.rs)
  `ensure_typescript_compiler(...)` still assumes the fake package installer path. That should be replaced by the same real npm / pinned dependency-tree model as the rest of the Node runtime.
- [crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs)
  The current handwritten Rust package-resolution policy is valuable as reference and test scaffolding, but it should not keep growing into our permanent replacement for Node's own loader logic.

### Keep, But Move Down To The Primitive Layer

These pieces still fit the new architecture, but they should become substrate for upstream Node JS rather than the place where public Node behavior is authored.

- [crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs)
  `NodeRuntimeHost`, VFS-backed reads, command execution, module graph caches, and snapshot-backed reads are all still useful. They should increasingly back `internalBinding(...)` and related internal seams rather than public builtin wrappers.
- [crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs)
  The current fs, crypto, dns, tls, zlib, and child-process host behaviors are still useful where they represent real deterministic primitives. The shift is about boundary placement, not throwing away the backend work.

### Already Aligned With The New Direction

These pieces are worth preserving and extending.

- [crates/terracedb-sandbox/src/node_path_module.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/node_path_module.js)
  This is already much closer to the desired model: upstream-shaped JS with a smaller Terrace-owned seam.
- [crates/terracedb-sandbox/src/node_os_module.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/node_os_module.js)
  Same story here: this is closer to "reuse upstream JS" than to "author a bespoke public stdlib."
- [crates/terracedb-sandbox/src/runtime.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/runtime.rs)
  The flight recorder, debug traces, and autoinstrumentation are still valuable. They should remain debug infrastructure, not semantic implementation.
- [crates/terracedb-sandbox/tests](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests)
  The imported Node contract suites, parity tests, and npm canaries are some of the highest-value artifacts from this whole effort and should remain central to the migration.

## Migration Rule Of Thumb

When deciding whether to keep or delete a compatibility layer, use this question:

- does this code implement a deterministic primitive or debugging aid we still need
- or does it implement public Node behavior that upstream Node JS should own instead

If it is the second kind, it is probably transitional and should be replaced.

## Feedback

- When we have multiple sandboxes with shared dependencies (node, npm, particular node modules), we should find a way to use our vf's overlay / copy on write semantics to enable these dependencies to be deduplicated.
- We should have a base layer with node / npm.
- That base layer should be vendored into our crate so that others can re-use it, in the file format that our db expects.
- We should export public utilities that allow people to define their own base layer to e.g. install their own required dependencies, pre-install their own application repos for efficient sandbox cloning.
