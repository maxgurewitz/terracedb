# Node Compatibility For Real `npm` In The Sandbox

This note describes the work needed to run the real `npm` CLI inside the sandbox instead of a fake package installer.

## Problem

The current sandbox package path is not real `npm`.

It is a deterministic package system with a hard-coded registry and a host-side installer. That is useful for early bring-up, but it does not exercise the real npm client, real package resolution, or the Node APIs that npm and its dependencies expect.

That means we can get false confidence:

- a package may "install" in the fake installer but still fail in real npm
- npm CLI entrypoints may need CommonJS and `require`, which the fake path does not model
- built-in Node modules such as `path`, `fs`, `process`, `module`, and `events` are not being validated against real npm behavior
- package metadata like `package.json` `main`, `exports`, and `imports` may be ignored or only partially modeled

The end goal is to run the real npm client in a deterministic sandbox, with Node-compatible host APIs supplied by the sandbox runtime.

## What We Learned From `node` And `npm/cli`

Exploration of the vendored Node and npm CLI repos showed that npm is mostly a CommonJS application with a fairly broad Node surface area.

The first important gap is not "just more builtins." It is the execution model:

- npm bootstraps from CommonJS entrypoints
- it uses `require`, `module.exports`, and `process`
- it dynamically loads some packages and commands
- it depends on package resolution rules that follow Node, not a custom sandbox manifest format

The second important gap is built-in coverage. The npm stack touches a lot of Node APIs. The most common ones we saw were:

- `path`
- `fs` and `fs/promises`
- `process`
- `module`
- `events`
- `util`
- `url`
- `os`
- `crypto`
- `stream`
- `assert`
- `buffer`
- `readline`
- `tty`
- `net`
- `http` and `https`
- `zlib`
- `child_process`

Some of those are mostly library logic and can be implemented on top of a smaller substrate. Others are true host primitives and need sandbox support.

The exploration also showed that npm uses package features that the current sandbox loader does not fully model yet:

- CommonJS loading
- package subpath resolution
- `package.json` `main`
- `package.json` `exports`
- `package.json` `imports`
- built-in modules imported as `node:*`

## Why The Fake Installer Is Insufficient

The fake installer is useful as a stopgap, but it cannot be the long-term answer because it bypasses the real client.

It does not prove:

- that npm CLI entrypoints can boot
- that npm's CommonJS graph can load correctly
- that package resolution matches Node rules
- that the sandbox has enough Node compatibility for npm's dependencies
- that a real `package.json` workflow behaves the same way a user expects

It also creates a maintenance burden: every package or builtin we hardcode becomes another divergence from the real ecosystem.

## Minimum Direction

The minimal useful direction is:

1. Add a first-class CommonJS / Node-style execution path in the sandbox.
2. Run the real npm CLI code inside that path.
3. Provide only the smallest host APIs needed by npm and its direct dependencies.
4. Prefer real Node semantics over sandbox-specific fake implementations whenever possible.

That is the right shape for long-term compatibility.

## Staged Test Plan

Use `nextest` so the npm characterizations can run in parallel and fail independently.

Start with focused suites:

- `npm_bootstrap`
- `npm_cli_basic`
- `npm_local_install`

Then expand as each missing API is filled in.

Recommended order:

1. Bootstrap checks
   - `npm --version`
   - `npm root`
   - `npm prefix`
   - `npm config get prefix`
   - `npm run` / `npm exec` entrypoints

2. Local manifest checks
   - install from `package.json`
   - `dependencies`
   - `devDependencies`
   - empty-manifest failure

3. Dependency graph checks
   - CommonJS `require`
   - package subpaths
   - `exports`
   - `imports`
   - built-in `node:*` imports

4. Broader CLI checks
   - local `npm install`
   - cache behavior
   - `npm ls`
   - config behavior

## Dev/Test Loop

The iteration loop should be short and mechanical:

1. Run the smallest `nextest` target that reproduces the current failure.
2. Identify which missing Node builtin or runtime semantic caused the failure.
3. Add the smallest host primitive or shim that fixes that failure.
4. Rerun the same test, then the next tier of tests.
5. Keep going until the basic npm operations pass reliably.

This loop should be driven by tests, not by guessing.

When a failure appears, classify it as one of these:

- missing builtin module
- wrong CommonJS semantics
- wrong package resolution
- wrong `process` behavior
- wrong filesystem behavior
- wrong dynamic import behavior
- missing package metadata support

That classification keeps fixes small and reduces the chance that we accidentally overbuild a fake Node runtime.

## What This Document Is Not

This is not a proposal to reimplement npm in Rust.

It is a proposal to run the real npm client inside a deterministic sandbox, with enough Node compatibility to make npm believe it is in a normal Node-like environment.

