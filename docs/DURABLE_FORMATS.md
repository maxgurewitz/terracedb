# Durable Format Contracts

This document defines the T23a durable-format policy for Terracedb's current control-plane formats.

## Scope

T23a currently covers:

- the catalog file at `catalog/CATALOG.json`
- commit-record frames in the unified commit log
- commit-log segment footers
- local manifest files
- remote manifest files
- remote-cache metadata files
- backup-GC birth metadata files
- workflow runtime records and checkpoint artifacts in `crates/terracedb-workflows`

Hot row/columnar SSTable layout evolution stays with the SSTable tasks and is intentionally out of scope here.

## Policy

All T23a-owned formats are treated as explicit local contracts.

Today that means:

- the exact emitted bytes for each checked fixture are reviewed artifacts,
- any intentional byte drift must update the fixture in the same change,
- Terracedb only promises the current greenfield format set, not backward compatibility with every earlier revision,
- unsupported versions must fail closed, and
- malformed bytes must never be treated as valid durable state.

The checked FlatBuffers schema reference lives at `schemas/durable_metadata.fbs`. Commit-log frames and segment footers remain custom binary formats.

The current per-format policy is:

| Format | Representative location | Compatibility boundary | Version rule | Fail-closed rule |
| --- | --- | --- | --- | --- |
| Catalog FlatBuffer | `catalog/CATALOG.json` | Exact emitted bytes are stable within the current `format_version` 1 fixture set. | Bump `format_version` when new code should reject earlier bytes rather than risk misreading them. | `Db::open` / catalog decode must reject invalid FlatBuffers or unsupported versions. |
| Commit-record frame | commit-log record payload/frame bytes | Exact frame bytes are stable within a record format version. The checked v1/v2 fixtures are reviewed artifacts during greenfield development, but not a long-term compatibility promise. | Bump the record payload version for layout changes that should no longer decode as the same record format. | Frame decode rejects bad magic, length mismatch, checksum mismatch, trailing bytes, and unknown versions. |
| Segment footer | sealed segment footer bytes | Exact footer bytes are stable within the footer format version. | Bump the footer version for incompatible footer layout changes. | Footer decode rejects checksum mismatch, trailing bytes, and unknown versions. |
| Local manifest FlatBuffer | `manifest/MANIFEST-*` | Exact emitted bytes are stable within `format_version` 1. | Bump `format_version` when local recovery should reject earlier bodies. | Local manifest load rejects invalid FlatBuffers, bad checksums, and unsupported versions. |
| Remote manifest FlatBuffer | `backup/manifest/MANIFEST-*` | Exact emitted bytes are stable within `format_version` 1. | Bump `format_version` when remote recovery should reject earlier bodies. | Remote manifest load rejects invalid FlatBuffers, bad checksums, and unsupported versions. |
| Remote-cache metadata FlatBuffer | cache `meta/*` records | Exact emitted bytes are stable within `format_version` 1. | Bump `format_version` when rebuilt cache state should no longer trust earlier metadata bytes. | Cache rebuild must ignore invalid/unsupported metadata and fetch from remote again instead of trusting it. |
| Backup-GC birth metadata FlatBuffer | `backup/gc/objects/*` | Exact emitted bytes are stable within `format_version` 1. | Bump `format_version` when GC should reject earlier birth records. | GC metadata decode must treat invalid/unsupported metadata as unusable so GC leaves the object alone. |
| Workflow runtime versioned-JSON records | workflow-owned row payloads and workflow checkpoint table artifacts | Exact emitted bytes are stable within the current version byte `1` for the frozen T108/T109 workflow contracts. | Bump the leading version byte when workflow recovery/checkpoint restore should reject earlier record or artifact layouts. | Workflow runtime load/restore must reject malformed JSON, bad table-artifact kinds, and unsupported versions instead of treating them as valid workflow state. |

## Workflow Runtime Records

The workflow runtime currently persists versioned JSON payloads with a one-byte format prefix.

The T109 durable workflow record set includes:

- `runs`
- `state`
- `history`
- `lifecycle`
- `visibility`

Checkpoint capture/restore for workflow runtimes now requires the corresponding table artifacts:

- `runs`
- `state`
- `history`
- `lifecycle`
- `visibility`
- `inbox`
- `trigger-order`
- `source-progress`
- `timer-schedule`
- `timer-lookup`
- `outbox`

`trigger-journal` remains conditionally present when the captured checkpoint includes journaled trigger rows. All of these artifacts must fail closed on unsupported versions or malformed bytes.

## Local Workflow

Use these commands for intentional durable-format changes:

1. Run `scripts/check-durable-format-fixtures.sh` to verify the current fixtures.
2. If the byte change is intentional, run `scripts/regenerate-durable-format-fixtures.sh`.
3. Review the fixture diffs together with the code change before committing.

The shared pre-commit hook runs the check script before the broader test/lint pass, so accidental format drift fails locally.

## Fixture Inventory

The checked fixtures live in `tests/fixtures/durable-formats/`:

- `catalog-v1.bin`
- `commit-record-v1.hex`
- `commit-record-v2.hex`
- `segment-footer-v2.hex`
- `local-manifest-v1.bin`
- `remote-manifest-v1.bin`
- `remote-cache-entry-v1.bin`
- `backup-gc-birth-v1.bin`
- `workflow-run-record-v1.bin`
- `workflow-state-record-v1.bin`
- `workflow-history-event-v1.bin`
- `workflow-lifecycle-record-v1.bin`
- `workflow-visibility-record-v1.bin`
- `workflow-runs-artifact-v1.bin`
- `workflow-state-artifact-v1.bin`
- `workflow-history-artifact-v1.bin`
- `workflow-lifecycle-artifact-v1.bin`
- `workflow-visibility-artifact-v1.bin`
