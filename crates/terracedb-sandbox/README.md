# terracedb-sandbox

## Generated Upstream Node Tests

The upstream Node smoke tests live under `tests/generated_node_upstream/` and are checked in.

- Generator binary: [`src/bin/generate_node_upstream_tests.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/src/bin/generate_node_upstream_tests.rs)
- Cargo-discovered wrapper: [`tests/generated_node_upstream/main.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/generated_node_upstream/main.rs)
- Generated body: [`tests/generated_node_upstream/body.rs`](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/generated_node_upstream/body.rs)

Regenerate them with:

```bash
cargo run -p terracedb-sandbox --bin generate_node_upstream_tests
```

The intent is:

- keep handwritten tests separate from generated upstream mirrors
- make the selected upstream Node test set explicit and reviewable
- select relevant upstream `test-module-*` and `test-require-*` files by default, then exclude known unsupported cases with a denylist
- let `cargo test` and `cargo nextest run` parallelize the generated tests normally
