# `terracedb-fuzz`

Reusable broad-input testing support for Terracedb and applications built on top of it.

This crate is intentionally public-facing rather than test-only glue. It exports:

- generic seeded campaign helpers for replay and variance checks,
- JSON artifact helpers for saving and replaying failing scenarios,
- a small scenario minimizer that third-party projects can implement for their own workload types,
- Terracedb simulation re-exports for deterministic application-level harnesses, and
- ready-made adapters for core DB and VFS generated scenarios.

The intended workflow is:

1. define a serializable scenario type for your application,
2. implement [`ScenarioOperations`] so failures can be minimized,
3. implement [`GeneratedScenarioHarness`] so the same generator/runner can be used in tests and fuzz campaigns,
4. persist any failing scenario with [`save_json_artifact`], and
5. promote the minimized repro into an owning crate regression test.

`examples/todo-api/tests/fuzz.rs` demonstrates how an application that sits on top of Terracedb can use these utilities without reaching into Terracedb internals.

Byte-level `cargo-fuzz` entrypoints live separately under [`/fuzz`](/Users/maxwellgurewitz/.codex/worktrees/8690/terracedb/fuzz) as the standalone `terracedb-fuzz-targets` package so the reusable API crate can keep a clean public-facing name.
