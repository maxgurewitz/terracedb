# `terracedb-systemtest`

Public app-stack system testing support for Terracedb applications.

This crate sits one layer above `terracedb-fuzz`:

- `terracedb-fuzz` owns low-level reusable seed, artifact, replay, and minimization primitives.
- `terracedb-systemtest` owns the more opinionated application/system harness layer that Terracedb-backed apps and examples can build on.

The intended workflow is:

1. define a serializable scenario for your application,
2. implement [`SystemScenarioHarness`] for the app stack you want to exercise,
3. run fixed seeds under `cargo test` / `nextest`,
4. replay serialized scenarios through the same harness, and
5. optionally feed mutated scenario bytes from `cargo-fuzz` into the same runner.

This crate also re-exports the deterministic HTTP and simulation helpers that application-level tests commonly need, so examples and third-party apps can keep their local `tests/` directories flat and light.

It also owns the in-process simulation suite harness used for:

- suite-scoped shared fixture preparation,
- thread-level parallel case execution inside one Rust test process,
- per-case timeout enforcement, and
- future production-facing simulation scheduling APIs.
