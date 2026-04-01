# terracedb

**terracedb** is a project to build a composable toolkit of primitives for embedded, single-node databases.

Instead of shipping a monolithic database server with every higher-level feature baked in, terracedb focuses on a small set of durable, reusable building blocks: tables, snapshots, sequence numbers, atomic write batches, conflict-checked commits, version-aware reads, and change-capture streams.

The goal is to make it easy to assemble specialized embedded databases in-process — from simple key-value state stores to richer systems with derived views, async pipelines, durable orchestration, and embedded agent sandboxes — while keeping the core understandable, testable, and deterministic.

## Design goals

- **Embedded, not a server** — runs in the same process as the application.
- **Single-node first** — optimized for local correctness, durability, and composability.
- **Primitive-oriented** — higher-level behavior is built from a small core instead of hidden inside the engine.
- **Deterministic by design** — I/O, time, and randomness are abstracted so the full stack can be simulation-tested.

## Core engine

The core engine is LSM-based and provides the low-level primitives needed to build storage systems and runtime libraries on top:

- tables and snapshots
- atomic write batches and optimistic conflict checking
- sequence-ordered visibility and durability
- version-aware reads
- commit-log-backed change capture
- local-first storage with tiered and S3-primary modes

## Projection library

The projection library builds **deterministic derived state** on top of the core engine.

It tails change streams, reads from frontier-pinned snapshots, and writes materialized outputs such as indexes, aggregates, and read models. Projections are state updaters only: they do not perform side effects. Their job is to keep derived data synchronized with source data in a replay-safe way.

## Workflow library

The workflow library builds **durable stateful orchestration** on top of the same primitives.

It supports long-lived workflow instances, durable trigger admission, timers, retries, and external side effects via an outbox pattern. Where projections maintain derived state, workflows coordinate business processes that must survive crashes and resume from durable state.

## Embedded virtual filesystem library

The repository also includes an **embedded virtual filesystem library** on top of Terracedb (`terracedb-vfs`).

Its scope is intentionally narrow: provide an in-process virtual filesystem, small KV state, tool-run history, point-in-time snapshots, and copy-on-write overlays for AI-agent runtimes. Exposing that filesystem as a real host mount or network service is explicitly out of scope.

## Philosophy

The name **terracedb** is inspired by FoundationDB’s emphasis on small, composable primitives. The aim is similar in spirit for the single-node embedded setting: keep the core narrow, make higher-level behavior explicit, and let applications assemble the pieces they actually need.

## Development

This repository includes a shared pre-commit script at `scripts/pre-commit.sh`.

The Git hook at `.githooks/pre-commit` calls that script, which runs:

- `scripts/run-codex-review.sh`
- `scripts/check-durable-format-fixtures.sh`
- `cargo nextest run --workspace`
- `cargo test --workspace --doc`
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`

By default, the Codex review step evaluates the staged diff against
`docs/GUIDELINES.md`, writes a full Markdown review under `.tmp/codex-review/`,
prints the absolute review path, and blocks the commit only when Codex returns
blocking findings. Set `CODEX_REVIEW_ENABLED=0` to disable it, set
`CODEX_REVIEW_STRICT=1` to also fail the commit on Codex tooling/setup errors,
use `SKIP_CODEX_REVIEW=1` to bypass the review step for a single commit, and
set `CODEX_REVIEW_REASONING_EFFORT` to override the default low reasoning mode.
The hook also expects `jq` to be installed locally.

Durable-format policy, fixture inventory, and the checked FlatBuffers schema reference live in `docs/DURABLE_FORMATS.md` and `schemas/durable_metadata.fbs`.

When an intentional durable-format change updates canonical bytes, regenerate the reviewed fixtures with:

```bash
scripts/regenerate-durable-format-fixtures.sh
```

Enable it locally with:

```bash
git config core.hooksPath .githooks
```
