# terracedb

**terracedb** is a project to build a composable toolkit of primitives for embedded, single-node databases.

Instead of shipping a monolithic database server with every higher-level feature baked in, terracedb focuses on a small set of durable, reusable building blocks: tables, snapshots, sequence numbers, atomic write batches, conflict-checked commits, version-aware reads, and change-capture streams.

The goal is to make it easy to assemble specialized embedded databases in-process — from simple key-value state stores to richer systems with derived views, async pipelines, and durable orchestration — while keeping the core understandable, testable, and deterministic.

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

## Philosophy

The name **terracedb** is inspired by FoundationDB’s emphasis on small, composable primitives. The aim is similar in spirit for the single-node embedded setting: keep the core narrow, make higher-level behavior explicit, and let applications assemble the pieces they actually need.
