# NPM And Node Compatibility In The Sandbox

## Goal

Run the real `npm` CLI and the real `tsc` compiler inside the TerraceDB sandbox.

This means:

- no fake Rust reimplementation of TypeScript
- no fake Rust npm client
- no hard-coded fake package registry as the main path
- real JavaScript tooling running inside the sandbox's deterministic runtime

## Problem

The current sandbox can load:

- ESM-style guest modules
- a very small set of synthetic builtins
- a fake package install view under `/.terrace/npm/node_modules`

That is not enough for real `npm`.

Real `npm` is not just "some packages plus `fs`".
Its bootstrap path is CommonJS from the first line:

- `bin/npm-cli.js` does `require('../lib/cli.js')`
- `lib/cli.js` does more `require(...)`
- `lib/cli/entry.js` continues through CommonJS and uses real Node process/module behavior

So the first missing piece is not even networking or the registry.
It is a real Node-style module/runtime layer:

- CommonJS
- `require`
- `module.exports`
- package main resolution
- `process`
- `Buffer`
- a larger builtin surface

## Relevant Repos

These are the main repos used for this investigation:

- `/Users/maxwellgurewitz/dev/node`
- `/Users/maxwellgurewitz/dev/cli`

The Node repo is the reference for which stdlib modules are mostly JavaScript wrappers versus deeply host-backed modules.

The npm CLI repo is the reference for what the real package manager actually imports and executes.

For the runnable harness, a locally installed npm tree is also useful.
The source repo is excellent for analysis, but the installed npm package tree is easier to seed into the sandbox because it already has its dependency layout materialized.

## What The Current Sandbox Does

Current sandbox/package support is still centered on fake npm compatibility:

- `crates/terracedb-sandbox/src/packages.rs`
  - hard-coded package registry and install materialization
- `crates/terracedb-sandbox/src/loader.rs`
  - package loading from the compatibility view
  - only tiny builtin support today
- `crates/terracedb-sandbox/src/typescript.rs`
  - currently being reworked toward real `tsc` in the sandbox
- `crates/terracedb-sandbox/src/runtime.rs`
  - still fundamentally oriented around ESM execution

Important current limitation:

- `node:fs` and `node:fs/promises` are the only real builtin family exposed today in `NpmWithNodeBuiltins`
- there is no first-class CommonJS/`require` path yet

## Evidence From `npm/cli`

Static scan of `npm/cli` plus its vendored dependencies shows the most common builtin imports are:

- `path`
- `fs`
- `fs/promises`
- `util`
- `url`
- `os`
- `events`
- `crypto`
- `stream`
- `assert`
- `net`
- `string_decoder`
- `buffer`
- `child_process`
- `zlib`
- `http`
- `module`
- `tls`
- `dns`
- `https`
- `process`
- `tty`
- `readline`
- `timers/promises`
- `querystring`
- `v8`

The highest-frequency modules from the static count were:

- `path`: 201
- `fs`: 102
- `fs/promises`: 78
- `util`: 36
- `url`: 36
- `os`: 31
- `events`: 26
- `crypto`: 22
- `stream`: 14
- `assert`: 8
- `net`: 7
- `string_decoder`: 7
- `buffer`: 7
- `child_process`: 6
- `zlib`: 6

This does not mean all of these are required for the very first bootstrap test.
It does mean real npm support will eventually need a fairly broad Node-compatible host surface.

## Runtime Trace For The First Commands

Static import counts are useful for the long-term surface.
For the first milestone, actual runtime traces are more important.

Running real npm under host Node for these commands:

- `npm --version`
- `npm prefix`

produced this much smaller builtin set:

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

This is the best current target for the first sandbox milestone.

It tells us two things:

1. We do not need to start with the full repo-wide builtin set.
2. We still do need CommonJS immediately, because those commands still begin at the normal npm CLI entrypoint.

## What Looks Easy Versus Hard

### Mostly JavaScript-Layer Modules

These are good early targets because they are mostly logic and string/object handling:

- `path`
- much of `url`
- `events`
- large parts of `util`
- `assert`
- `querystring`

For example, `node/lib/path.js` is mostly string/path logic in JS.

### JavaScript APIs Sitting On Top Of Host Primitives

These may be partly reusable from Node's JS implementation, but they depend on lower-level support:

- `buffer`
- `stream`
- `string_decoder`
- `module`

These are not "just a shim".
They depend on runtime behavior, allocation semantics, and module loading.

### Clearly Host-Primitive Modules

These need real host/runtime support:

- `fs`
- `fs/promises`
- `process`
- timers
- `crypto`
- `child_process`
- `zlib`
- `net`
- `tls`
- `http`
- `https`
- `dns`
- `tty`
- `os`
- `v8`

Some of these may be stubbed for the earliest tests.
For example:

- `module.enableCompileCache()` can likely be a no-op
- `child_process` may be blocked or partially stubbed at first
- `dns` may be unnecessary until real registry/network flows

## Likely Core Change

The sandbox probably needs a first-class **Node/CommonJS execution mode** alongside the current ESM path.

Reason:

- real npm starts in CommonJS
- Node package resolution is not the same as our current ESM guest-module resolution
- many npm dependencies assume `require`, `module`, `exports`, `__filename`, and `__dirname`

Adding more builtin modules without adding CommonJS will not get us to real npm.

## Tentative Primitive Surface For The First Milestone

The first milestone is not "full npm install from the public registry".
The first milestone is "real npm boots and handles no-network/local operations in the sandbox".

Tentative minimum surface for that:

- CommonJS loader and cache
- `require`
- `module.exports`
- package `main` resolution
- `process`
- `Buffer`
- `path`
- `fs`
- `fs/promises`
- `util`
- `url`
- `os`
- `events`
- `assert`
- `stream`
- `string_decoder`
- `timers/promises`
- `module` with at least `enableCompileCache()` as a harmless stub

Observed from the host runtime traces for the first commands:

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

Likely needed soon after:

- `crypto`
- `zlib`
- `http` / `https`
- `net` / `tls`

Likely defer/stub for now:

- `child_process`
- `tty`
- `readline`
- `dns`
- `v8`

## Recommended Test Plan

The test suites should be split so `cargo nextest` can parallelize them and so failures isolate the missing host surface.

### Suite 1: `npm_bootstrap`

Purpose:

- prove the real npm entrypoint can start in the sandbox

Suggested operations:

- `npm --version`
- `npm --help`
- `npm` with no args

Likely first failures:

- no CommonJS support
- no `process`
- no `node:module`
- missing `path`
- missing `util`

Current exploratory expectation:

- this suite should fail at the CommonJS/runtime boundary first
- once CommonJS exists, it becomes the first success suite to flip

### Suite 2: `npm_config_local`

Purpose:

- exercise package.json/config/prefix logic without registry fetches

Suggested operations:

- `npm prefix`
- `npm root`
- `npm config get registry`
- `npm pkg get name`

Likely first failures:

- missing `fs/promises`
- missing `os`
- missing config-file save/load primitives
- missing `Buffer` / `stream` edge behavior

Current exploratory expectation:

- this suite shares the same first CommonJS failure today
- after CommonJS lands, it should become the first suite that drives config and filesystem correctness

### Suite 3: `npm_lockfile_local_install`

Purpose:

- exercise local tree mutation without public-registry networking

Suggested operations:

- `npm install --package-lock-only`
- `npm install ./local-package`
- `npm install file:../pkg`

Likely first failures:

- missing CommonJS resolution deeper in npm deps
- missing `crypto`
- missing tar / `zlib`
- missing richer filesystem semantics

Current exploratory expectation:

- local install should stay strictly local at first
- use `file:` or `./dep` installs before registry work

### Suite 4: `npm_registry_mock`

Purpose:

- exercise actual registry fetch behavior after local flows work

Suggested operations:

- install from a mock registry
- read lockfile and package tree results

Likely first failures:

- missing `http` / `https`
- missing `net` / `tls`
- proxy/agent behavior
- integrity/fetch/caching edge cases

## Development Loop

The intended loop is:

1. Seed real npm into the sandbox.
2. Run a very small command in a dedicated nextest suite.
3. Read the first missing builtin/runtime failure.
4. Implement the smallest correct compatibility slice.
5. Rerun only the affected suite.
6. Repeat until the basic milestone passes.

Current first-pass harness:

- `crates/terracedb-sandbox/tests/npm_bootstrap.rs`
- `crates/terracedb-sandbox/tests/npm_config_local.rs`
- `crates/terracedb-sandbox/tests/npm_local_install.rs`

These currently lock in the first missing boundary using a real npm tree copied into the sandbox and a wrapper module that sets up a minimal `process` object before importing npm.

That means:

- the harness is already using real npm assets
- the current passing tests do **not** mean npm works
- they only mean the first missing runtime boundary is stable and easy to rerun under `cargo nextest`

Command shape:

```bash
cargo nextest run -p terracedb-sandbox --test npm_bootstrap
cargo nextest run -p terracedb-sandbox --test npm_config_local
cargo nextest run -p terracedb-sandbox --test npm_lockfile_local_install
cargo nextest run -p terracedb-sandbox --test npm_registry_mock
```

## Important Backtracks

These paths are likely to be reduced or removed as real npm support lands:

- fake package registry as the primary npm path
- hard-coded package install materialization as the main install model
- fake TypeScript transpilation/checking
- npm compatibility that assumes ESM-only guest loading is enough

Some pieces may still remain as helpers:

- deterministic caching
- install manifests
- generated Terrace builtin declarations
- package-compat policy enforcement

But the main execution path should move toward:

- real npm CLI running in the sandbox
- real `tsc` running in the sandbox
- deterministic host/runtime semantics beneath them
