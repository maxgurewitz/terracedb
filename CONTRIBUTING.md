# Contributing

## Local setup

Enable the shared Git hooks locally with:

```bash
git config core.hooksPath .githooks
```

This repository includes a shared pre-commit script at `scripts/pre-commit.sh`.

The Git hook at `.githooks/pre-commit` calls that script, which runs:

- `scripts/check-flatbuffer-bindings.sh`
- `scripts/run-codex-review.sh`
- `scripts/check-durable-format-fixtures.sh`
- `cargo nextest run --workspace`
- `cargo test --workspace --doc`
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features -- -D warnings`

The Codex review hook expects `jq` to be installed locally.
The Codex review hook also expects `iconv` to be available locally.
The FlatBuffers binding check expects `flatc` to be installed locally.

## Codex review hook

By default, the Codex review step evaluates the staged diff against
`docs/GUIDELINES.md`, writes branch-local review history under
`.tmp/codex-review/<branch>/`, prints the absolute decision-log, context-file,
and review paths, and blocks the commit only when Codex returns blocking
findings.

Available controls:

- `CODEX_REVIEW_ENABLED=0` disables the review step
- `CODEX_REVIEW_STRICT=1` also fails the commit on Codex tooling or setup
  errors
- `SKIP_CODEX_REVIEW=1` bypasses the review step for a single commit
- `CODEX_REVIEW_REASONING_EFFORT` overrides the default low reasoning mode

For the web-search-backed review flow, values below `low` and unsupported
override values fall back to `low` automatically.

Each branch keeps review memory in:

- `.tmp/codex-review/<branch>/reviews/` for immutable review rounds
- `.tmp/codex-review/<branch>/decisions.json` for structured user or agent
  adjudications such as `dismissed`, `deferred`, and `accepted`
- `.tmp/codex-review/<branch>/context.md` for optional freeform branch review
  guidance

To record a structured response to a finding so future review rounds can honor
it, run:

```bash
scripts/codex-review-record-decision.sh \
  --finding-id example-finding-id \
  --resolution dismissed \
  --reason "Intentional for this branch because ..."
```

## Durable format workflow

Durable-format policy, fixture inventory, the checked FlatBuffers schemas,
and generated Rust bindings live in `docs/DURABLE_FORMATS.md`,
`schemas/durable/`, and `src/durable_formats/*_generated.rs`.

When an intentional durable-format change updates canonical bytes, regenerate
the reviewed fixtures with:

```bash
scripts/regenerate-durable-format-fixtures.sh
```

When an intentional schema change updates the FlatBuffers bindings, regenerate
them with:

```bash
scripts/regenerate-flatbuffer-bindings.sh
```
