## Problem

We need the sandbox to run real JavaScript tooling, starting with:

- real `tsc`
- real `npm`

The current implementation does not do that.

Today:

- TypeScript support is implemented in Rust in [`crates/terracedb-sandbox/src/typescript.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/typescript.rs).
- Package installation is implemented by a fake deterministic installer in [`crates/terracedb-sandbox/src/packages.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/packages.rs).
- The built-in module loader only supports ESM plus a very small Node surface. In practice, the only `node:` builtins currently wired through are `node:fs` and `node:fs/promises` in [`crates/terracedb-sandbox/src/loader.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/loader.rs).

That is not enough for real `npm`.

## Goal

Run the real `npm/cli` codebase inside the sandbox runtime, deterministically, with:

- our VFS
- our clock/timers
- our network mediation
- our permission model
- our recovery/replay model

The sandbox should provide a Node-compatible host surface. It should not pretend to be npm by hardcoding a tiny registry or a tiny subset of Node.

## Repos Explored

- Node reference runtime: [`/Users/maxwellgurewitz/dev/node`](/Users/maxwellgurewitz/dev/node)
- npm CLI: [`/Users/maxwellgurewitz/dev/cli`](/Users/maxwellgurewitz/dev/cli)
- Current sandbox implementation: [`crates/terracedb-sandbox`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox)
- Current workflow sandbox adapter: [`crates/terracedb-workflows-sandbox`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-workflows-sandbox)

Useful npm references:

- bootstrap entrypoint: [`/Users/maxwellgurewitz/dev/cli/bin/npm-cli.js`](/Users/maxwellgurewitz/dev/cli/bin/npm-cli.js)
- CLI bootstrap: [`/Users/maxwellgurewitz/dev/cli/lib/cli.js`](/Users/maxwellgurewitz/dev/cli/lib/cli.js)
- CLI runtime entry: [`/Users/maxwellgurewitz/dev/cli/lib/cli/entry.js`](/Users/maxwellgurewitz/dev/cli/lib/cli/entry.js)
- main npm object: [`/Users/maxwellgurewitz/dev/cli/lib/npm.js`](/Users/maxwellgurewitz/dev/cli/lib/npm.js)
- install command: [`/Users/maxwellgurewitz/dev/cli/lib/commands/install.js`](/Users/maxwellgurewitz/dev/cli/lib/commands/install.js)
- smoke tests: [`/Users/maxwellgurewitz/dev/cli/smoke-tests/test/index.js`](/Users/maxwellgurewitz/dev/cli/smoke-tests/test/index.js)
- local/workspace lockfile install test: [`/Users/maxwellgurewitz/dev/cli/smoke-tests/test/install-links-package-lock-only.js`](/Users/maxwellgurewitz/dev/cli/smoke-tests/test/install-links-package-lock-only.js)

Useful npm dependency references:

- pacote fetcher: [`/Users/maxwellgurewitz/dev/cli/node_modules/pacote/lib/fetcher.js`](/Users/maxwellgurewitz/dev/cli/node_modules/pacote/lib/fetcher.js)
- npm-registry-fetch: [`/Users/maxwellgurewitz/dev/cli/node_modules/npm-registry-fetch/lib/index.js`](/Users/maxwellgurewitz/dev/cli/node_modules/npm-registry-fetch/lib/index.js)
- cacache index: [`/Users/maxwellgurewitz/dev/cli/node_modules/cacache/lib/index.js`](/Users/maxwellgurewitz/dev/cli/node_modules/cacache/lib/index.js)
- tar: [`/Users/maxwellgurewitz/dev/cli/node_modules/tar/lib/index.js`](/Users/maxwellgurewitz/dev/cli/node_modules/tar/lib/index.js)

Useful Node references:

- `path`: [`/Users/maxwellgurewitz/dev/node/lib/path.js`](/Users/maxwellgurewitz/dev/node/lib/path.js)
- `fs`: [`/Users/maxwellgurewitz/dev/node/lib/fs.js`](/Users/maxwellgurewitz/dev/node/lib/fs.js)

## What The Exploration Found

### 1. npm is not just “more builtins”

Real `npm` needs three layers:

1. A CommonJS runtime and Node-style package resolution
2. A set of host primitives and builtins
3. Registry/cache/install behavior

The current sandbox is missing layer 1 almost entirely.

### 2. The current sandbox is ESM-first and almost no-Node

Current facts:

- Workspace and npm modules are parsed as ESM in [`to_boa_module(...)`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/loader.rs).
- There is no general CommonJS `require` pipeline.
- There is no general `module.exports` / `exports` / `__filename` / `__dirname` path.
- There is no Node package resolution algorithm for:
  - `package.json`
  - `main`
  - `exports`
  - directory index fallback
  - JSON modules
  - `node_modules` traversal
- `node:` builtins currently stop at `node:fs` and `node:fs/promises` in [`load_node_builtin_module(...)`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/loader.rs#L363).

This means real `npm` cannot even bootstrap, before we get to network or install logic.

### 3. The current package-management path is a fake npm

The current bash command:

- `npm install ...`

does not run npm CLI. It calls `session.install_packages(...)` from [`crates/terracedb-sandbox/src/bash.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/bash.rs).

That goes through:

- [`DeterministicPackageInstaller`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/packages.rs)

which resolves packages from a hardcoded registry and materializes them into:

- [`/.terrace/npm/node_modules`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/types.rs#L24)

This path must be treated as legacy scaffolding and replaced for real npm support.

### 4. The current TypeScript path is still special-cased

The current TypeScript service is moving toward real `tsc`, but it is still treated as a special Rust-owned tool path in:

- [`crates/terracedb-sandbox/src/typescript.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/typescript.rs)

Long term, `tsc` should be “real JS tooling running inside the sandbox host” rather than “a special Rust service that knows a lot about TypeScript.”

## Tentative Minimum Surface

This is the tentative Node-compatible surface needed to get a basic proof of concept working.

### Bootstrap Surface

Needed to execute `npm/cli` at all:

- CommonJS evaluation
- synchronous `require(...)`
- `module.exports` / `exports`
- `__filename`
- `__dirname`
- package resolution for relative files
- package resolution for bare specifiers in `node_modules`
- JSON `require(...)`
- shebang-safe script loading for entrypoints like [`bin/npm-cli.js`](/Users/maxwellgurewitz/dev/cli/bin/npm-cli.js)
- `process`
- timers
- `console`
- `Buffer`

Without this, real npm cannot start.

### Basic CLI Surface

Needed for `npm --version`, `npm --help`, and `npm init -y`:

- `fs`
- `fs/promises`
- `path`
- `url`
- `util`
- `os`
- `events`
- `assert`
- `stream`
- `buffer`
- `module`

Static counts from the vendored npm tree show these are the hottest builtins after `fs` and `path`.

### Local Install Surface

Needed for filesystem-only install flows like:

- `npm install ./local-package --ignore-scripts`
- `npm install file:../pkg --ignore-scripts`

Likely additional requirements:

- `crypto`
- `zlib`
- `string_decoder`
- more complete `stream`

This comes from the actual install path:

- [`pacote`](/Users/maxwellgurewitz/dev/cli/node_modules/pacote/lib/fetcher.js)
- [`cacache`](/Users/maxwellgurewitz/dev/cli/node_modules/cacache/lib/index.js)
- [`tar`](/Users/maxwellgurewitz/dev/cli/node_modules/tar/lib/index.js)

### Registry Install Surface

Needed for real remote installs:

- `http`
- `https`
- `dns`
- `net`
- `tls`
- `querystring`

This comes from:

- [`npm-registry-fetch`](/Users/maxwellgurewitz/dev/cli/node_modules/npm-registry-fetch/lib/index.js)
- [`make-fetch-happen`](/Users/maxwellgurewitz/dev/cli/node_modules/make-fetch-happen/lib/index.js)

### Deferred / Optional Surface

These can likely be deferred for the first proof of concept:

- `child_process`
  - avoid initially by forcing `--ignore-scripts`
- `worker_threads`
- `vm`
- `readline`
  - avoid interactive flows initially

## What Should Be Removed Or Backtracked

These are the main code paths that do not fit a real-npm direction.

### Fake Registry / Fake Installer

Backtrack or replace:

- [`crates/terracedb-sandbox/src/packages.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/packages.rs)
- fake `npm install` command in [`crates/terracedb-sandbox/src/bash.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/bash.rs)
- tests that encode the fake installer behavior in:
  - [`crates/terracedb-sandbox/tests/packages.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/packages.rs)
  - package-install portions of [`crates/terracedb-sandbox/tests/execution.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/execution.rs)

### ESM-Only npm Loading

Backtrack or extend:

- [`crates/terracedb-sandbox/src/loader.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/loader.rs)

The loader needs a Node/CommonJS path, not just more `node:` specifiers.

### TypeScript As A Rust-Owned Compiler

Backtrack or thin out:

- [`crates/terracedb-sandbox/src/typescript.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/typescript.rs)

The right end state is:

- Rust manages sandbox state and tool orchestration
- real JS tooling runs inside the sandbox runtime

## Development And Test Flow

This work should be driven by short feedback loops.

### Phase 1: Static Analysis

1. Inspect `npm/cli` bootstrap and smoke tests.
2. Inspect npm dependency paths used by local install and registry install.
3. Inspect Node builtins that npm actually touches.
4. Group required builtins into:
   - bootstrap
   - basic CLI
   - local install
   - registry install

### Phase 2: Real Sandbox Test Harness

Create sandbox tests that use the vendored npm repo at:

- [`/Users/maxwellgurewitz/dev/cli`](/Users/maxwellgurewitz/dev/cli)

using a hoisted source snapshot into the sandbox workspace.

Tests should live in multiple files so `cargo nextest` can parallelize them.

Proposed first suites:

- `npm_cli_bootstrap.rs`
  - hoist npm/cli
  - run `npm --version`
  - run `npm --help` or a minimal non-interactive command
- `npm_cli_init.rs`
  - hoist npm/cli
  - run `npm init -y`
  - assert `package.json` was created
- `npm_cli_local_install.rs`
  - hoist npm/cli
  - create a local dependency in the sandbox workspace
  - run `npm install ./dep --ignore-scripts`
  - assert lockfile / `node_modules` / package.json changes
- `npm_cli_registry_install.rs`
  - later
  - use a deterministic local registry or a heavily controlled mock path

### Phase 3: Tight Iteration

Run only the targeted suites with `nextest`, for example:

```bash
cargo nextest run -p terracedb-sandbox --test npm_cli_bootstrap --test npm_cli_init --test npm_cli_local_install
```

Then iterate:

1. run one or more npm sandbox tests
2. observe the first missing primitive or loader mismatch
3. implement only enough compatibility to pass that step
4. rerun the subset
5. move to the next operation

### Phase 4: Expand Carefully

Only after the local flows are green:

- add registry-backed install
- add more builtins
- add script execution
- add stricter package-manager behaviors

## Immediate Implementation Priorities

1. Get the crate back to green so the npm test loop can run.
2. Add a sandbox test harness that hoists the vendored npm repo.
3. Add a Node/CommonJS execution path.
4. Implement the first builtin set:
   - `process`
   - `path`
   - `fs`
   - `fs/promises`
   - `events`
   - `util`
   - `os`
   - `buffer`
   - `assert`
5. Make `npm --version` pass.
6. Make `npm init -y` pass.
7. Make a local-path install with `--ignore-scripts` pass.

That is the smallest path that proves we can run real npm in the sandbox without skipping straight to the full registry/network problem.
