# npm In Sandbox

## Goal

Run the real `npm` CLI inside the TerraceDB sandbox runtime.

This is a different goal from the current implementation.

Today we have:

- a fake package installer in Rust
- a fake package registry in Rust
- a fake TypeScript compiler/typechecker in Rust

That is not the intended architecture.

The target architecture is:

- real `npm` CLI JS running inside the sandbox
- real `tsc` JS running inside the sandbox
- a deterministic TerraceDB host environment underneath them

The host environment should provide enough Node-compatible behavior for real JS tooling to run, while still fitting into TerraceDB's deterministic runtime and simulation model.

## Problem

`npm` does not just need a few helper shims.

Even its bootstrap path assumes:

- CommonJS
- `require(...)`
- `process`
- `node:path`
- `node:module`
- `node:fs`
- event handlers on `process`
- Node-style module resolution

The current sandbox runtime is still mostly an ESM guest runtime with a very small builtin surface. That is enough for our current workflow examples, but not enough for real `npm`.

## Current Mismatch

These are the main mismatches in the current codebase:

- `crates/terracedb-sandbox/src/packages.rs` implements a fake deterministic package installer and default registry.
- `crates/terracedb-sandbox/src/typescript.rs` and `crates/terracedb-sandbox/src/typescript_runner.js` are in the middle of being changed to use real `typescript`, but the surrounding package/runtime model is still fake.
- `crates/terracedb-sandbox/src/loader.rs` is still fundamentally ESM-first.
- `crates/terracedb-sandbox/src/loader.rs` only exposes `node:fs` and `node:fs/promises` today, and only in `PackageCompatibilityMode::NpmWithNodeBuiltins`.
- There is currently no real CommonJS `require`, `module`, or `exports` execution path in the sandbox runtime.

## Repos Explored

These repos are the main references for this work:

- `/Users/maxwellgurewitz/dev/node`
- `/Users/maxwellgurewitz/dev/cli`

Why they matter:

- `node` tells us which stdlib modules are mostly JS and which are deeply host-backed.
- `npm/cli` tells us which Node APIs are actually needed by the real package manager and its direct dependency graph.

## npm Bootstrap Shape

The npm entrypoint is extremely simple and immediately shows the required runtime shape:

`/Users/maxwellgurewitz/dev/cli/bin/npm-cli.js`

```js
#!/usr/bin/env node
require('../lib/cli.js')(process)
```

`/Users/maxwellgurewitz/dev/cli/lib/cli.js` immediately assumes:

- CommonJS
- `node:module`
- `node:path`
- `process`

`/Users/maxwellgurewitz/dev/cli/lib/cli/entry.js` then assumes:

- `process.title`
- `process.on(...)`
- `process.off(...)`
- `require('graceful-fs').gracefulify(require('node:fs'))`
- normal CommonJS package loading

So the first real milestone is not package download. It is:

- can the sandbox bootstrap real npm CLI JS at all?

## Tentative Primitive Surface

This list is tentative. It should be refined as the sandbox tests begin failing on real npm flows.

### Measured For Basic npm Bootstrap

On a prepared local `npm/cli` checkout, these commands all hit the same small builtin set:

- `npm --version`
- `npm prefix`
- `npm config get cache`

Measured builtins for those commands:

- `path`
- `fs`
- `fs/promises`
- `os`
- `stream`
- `util`
- `events`
- `url`
- `module`
- `constants`
- `querystring`
- `string_decoder`

That means the first sandbox milestone is narrower than full install support.

### Likely Primitive / Host-Backed

These likely need real deterministic host implementations:

- `process`
- `fs`
- `fs/promises`
- timers
- module loading and resolution
- CommonJS execution
- Buffer semantics
- `crypto`
- `os`
- `zlib`

These may also be needed soon after bootstrap:

- `stream`
- `child_process`
- `net`
- `tls`
- `http`
- `https`

### Likely Mostly JS / Layerable

These are better treated as library-level compatibility rather than core host primitives:

- `path`
- `events`
- `assert`
- much of `util`

`url` looks mixed. We should expect to provide a working surface, but it may not need to be implemented at the same low level as `fs` or `crypto`.

## First Likely Core Change

The leading architectural change is:

- add a first-class CommonJS / `require` execution path to the sandbox runtime alongside the existing ESM path

Without that, real npm cannot even reach its own command dispatch.

This should be treated as a sandbox/runtime change, not as a workflow-specific feature.

## Staged Test Plan

The first tests should be split into multiple files so `cargo nextest` can parallelize them.

Suggested first suites:

### `npm_bootstrap.rs`

Purpose:

- prove what happens when we try to run real npm entrypoints inside the sandbox

First operations:

- `bin/npm-cli.js --version`
- `bin/npm-cli.js prefix`
- `bin/npm-cli.js config get cache`

Expected early failures after CommonJS exists:

- missing `process`
- missing `node:module`
- missing `node:path`
- incomplete `node:fs`
- incomplete `node:fs/promises`
- missing `stream` / `events` / `util` compatibility used by `graceful-fs`, `minipass`, and config loading

Representative real files loaded by `npm --version` during bootstrap:

- `bin/npm-cli.js`
- `lib/cli.js`
- `lib/cli/validate-engines.js`
- `lib/cli/entry.js`
- `lib/npm.js`
- `workspaces/config/lib/index.js`
- `node_modules/graceful-fs/graceful-fs.js`
- `node_modules/semver/functions/satisfies.js`
- `node_modules/proc-log/lib/index.js`
- `node_modules/which/lib/index.js`
- `node_modules/fs-minipass/lib/index.js`
- `node_modules/minipass/dist/commonjs/index.js`

### `node_commonjs.rs`

Purpose:

- keep iteration fast while building the runtime pieces npm depends on

First operations:

- execute a tiny CommonJS entrypoint
- `require("./dep")`
- `require("./data.json")`
- `require("node:path")`
- verify `process.argv`, `process.cwd()`, stdout, stderr, and exit code capture

This suite should stay smaller and faster than the real npm suites, and should be used to drive the basic CJS/runtime model before trying to debug npm-specific behavior.

### `npm_local_state.rs`

Purpose:

- exercise local package.json, cwd, and filesystem-driven CLI behavior without requiring registry fetch

First operations:

- `npm prefix`
- `npm root`
- `npm pkg get name`

Expected early failures:

- missing `process.cwd`
- incomplete `fs` / `fs.promises`
- missing package.json lookup and module resolution behavior

### `npm_local_install.rs`

Purpose:

- exercise install/reify behavior without requiring public registry fetch first

First operations:

- `npm install` with a local `file:` dependency
- `npm install` with scripts disabled

Expected early failures:

- missing `crypto`
- missing `zlib`
- missing tar/caching support
- incomplete Arborist filesystem expectations

### `npm_registry_install.rs`

Purpose:

- exercise end-to-end registry-backed install once the local install path works

First operations:

- install a single small package
- install from lockfile

Expected early failures:

- missing HTTP/TLS
- missing integrity and cache behavior
- missing stream/network surfaces

## Dev / Test Flow

The intended flow for this work is:

1. Run only the npm bootstrap suites with `cargo nextest`.
2. Record the first missing runtime surface.
3. Implement the smallest deterministic compatibility layer that resolves that failure.
4. Re-run the same small suites.
5. Move to the next stage only once the prior one is stable.

This is intentionally different from trying to implement all of Node at once.

We should let real npm tell us which surfaces are actually required in practice.

## Likely Backtracks

The following areas are likely to be removed or heavily rewritten:

- the fake default npm registry in `crates/terracedb-sandbox/src/packages.rs`
- the fake installer path in `DeterministicPackageInstaller`
- tests that currently treat fake package install behavior as the desired long-term contract
- ESM-only assumptions in the current module loader for package execution

## Open Questions

- How much of Node's CommonJS loader should we reproduce directly versus adapt into Boa?
- Should bootstrap commands run in a stricter mode than full install commands?
- How far can we get before needing `child_process`?
- For install and fetch, which operations should be mediated by deterministic host capabilities instead of directly copied from Node?

## Immediate Execution Plan

1. Finish the repo exploration and tighten the primitive surface list.
2. Create new nextest-friendly npm sandbox suites.
3. Try to run real npm bootstrap commands inside the sandbox.
4. Fix the first missing runtime surfaces one by one.
5. Keep this document updated as the true required surface becomes clearer.
