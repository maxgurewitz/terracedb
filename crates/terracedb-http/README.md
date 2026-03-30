# terracedb-http

`terracedb-http` packages deterministic HTTP simulation helpers for Terracedb applications built on top of `terracedb-simulation`.

The crate provides a reusable way to run HTTP clients and servers inside turmoil without every test or example hand-rolling socket accept loops, request plumbing, and retry scaffolding.

At its core, the crate offers:

- a low-level simulated HTTP server that runs inside a simulation host
- a simulated HTTP client with JSON request helpers and deterministic retry behavior
- request and response tracing that can be compared across same-seed simulation runs
- network control hooks for delay, hold, release, partition, restart, and retry scenarios

The crate stays framework-agnostic at the transport layer and uses `hyper` request and response primitives under the hood.

An optional `axum` feature adds adapters for serving an `axum::Router` through the same deterministic harness. That lets application examples keep their simulation tests focused on behavior rather than HTTP boilerplate.

Typical uses include:

- happy-path create/read/list API simulations
- retry and restart tests after dropped connections or delayed responses
- partition and recovery scenarios for client/server communication
- same-seed reproducibility checks over HTTP traces and outcomes

The TODO example uses this crate to run its API router inside simulation and to drive end-to-end client requests through the same harness.
