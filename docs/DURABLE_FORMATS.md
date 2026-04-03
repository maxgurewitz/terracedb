# Durable Format Contracts

This document defines the T23a durable-format policy for Terracedb's current control-plane formats.

## Scope

T23a currently covers:

- the catalog file at `catalog/CATALOG.json`
- commit-record frames in the commit log
- commit-log segment footers
- sandbox session control files under `/.terrace/`
- local manifest files
- remote manifest files
- remote-cache metadata files
- backup-GC birth metadata files
- workflow public-contract JSON fixtures in `crates/terracedb-workflows-core`
- workflow runtime records and checkpoint artifacts in `crates/terracedb-workflows`

Hot row/columnar SSTable block layout evolution stays with the SSTable tasks and is intentionally out of scope here, but the manifest metadata and durable storage layout that make SSTables reopenable are part of this contract.

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
| Commit-record frame | commit-log record payload/frame bytes | Exact frame bytes are stable for the current record format version only. Terracedb is still greenfield here, so older commit-record versions are not supported once the current version changes. | Bump the record payload version for layout changes that should replace the previous durable path. | Frame decode rejects bad magic, length mismatch, checksum mismatch, trailing bytes, and any version other than the current one. |
| Segment footer | sealed segment footer bytes | Exact footer bytes are stable within the footer format version. | Bump the footer version for incompatible footer layout changes. | Footer decode rejects checksum mismatch, trailing bytes, and unknown versions. |
| Sandbox session control JSON | `/.terrace/session.json`, `/.terrace/execution-policy-state.json` | Exact JSON bytes are not fixture-locked, but the required fields and current `format_version` values are reviewed durable contracts for reopen/recovery. | Bump `format_version` when reopen should reject earlier session-control layouts instead of guessing. | Reopen must reject malformed JSON, missing required version markers, and unsupported `format_version` values. |
| Local manifest FlatBuffer | `manifest/MANIFEST-*` | Exact emitted bytes are stable within `format_version` 1, including persisted SSTable shard ownership when present. | Bump `format_version` when local recovery should reject earlier bodies. | Local manifest load rejects invalid FlatBuffers, bad checksums, unsupported versions, and shard-ownership mismatches against reopened SSTables. |
| Remote manifest FlatBuffer | `backup/manifest/MANIFEST-*` | Exact emitted bytes are stable within `format_version` 1, including persisted SSTable shard ownership when present. | Bump `format_version` when remote recovery should reject earlier bodies. | Remote manifest load rejects invalid FlatBuffers, bad checksums, unsupported versions, and shard-ownership mismatches against reopened SSTables. |
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
- admitted `inbox` trigger payloads
- admitted `trigger-journal` entries

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

Accepted workflow updates/control requests now reuse the same persisted admitted-trigger encoding as events, timers, and callbacks. Any change to the serialized `StoredWorkflowTrigger` shape, including the `update` variant carried by inbox rows and trigger-journal entries, requires a matching fixture update in the same change.

## Shard-Aware Durable State

The current shard-aware contract adds three reviewed pieces of durable state on top of the original T23a formats:

- Commit-record payload `v3` appends durable shard-routing metadata to each entry after the optional operation-context block:
  `physical_shard: u32`, `virtual_partition: u32`, and `shard_map_revision: u64`.
  `v3` is the only supported commit-record format and the only format that recovery may trust for shard-local replay.
- Manifest SSTable entries may now persist `shard_ownership`, which records the SSTable's `physical_shard`, `shard_map_revision`, and `virtual_partitions` coverage.
  For sharded tables, reopen, recovery, flush, and compaction must preserve this durable ownership instead of recomputing it from the current shard map.
  If sharded manifest or SSTable state is missing ownership metadata, or if the durable ownership disagrees with the reopened file, Terracedb must fail closed.
- Shard-local durable layout is part of the contract:
  local commit-log segments live at `commitlog/SEG-<id>` for the unsharded lane and `commitlog/<shard>/SEG-<id>` for shard-local lanes.
  remote commit-log segments live at `backup/commitlog/SEG-<id>` for the unsharded lane and `backup/commitlog/<shard>/SEG-<id>` for shard-local lanes.
  shard-local SSTables live under `sstables/table-<table>/<shard>/...` locally and `backup/sst/table-<table>/<shard>/...` remotely, with `0000` representing the unsharded lane.

Changing any of those bytes, fields, or path/key conventions requires the same fixture-and-doc update workflow as the earlier T23a formats.

Sandbox session reopen/recovery also now relies on reviewed git-hoist provenance shape inside
`/.terrace/session.json` and `/.terrace/hoist-manifest.json`:

- session git provenance records `origin` as `native`, `host_import`, or `remote_import` rather
  than a legacy boolean host-import marker,
- session git provenance may persist a `remote_url` when the reopened repository depends on a
  provider-backed remote,
- hoist manifests persist structured `source` data (`host_path` or `remote_repository`) instead of
  only a raw `source_path`, and
- reopen must continue to fail closed if required git provenance is malformed, missing required
  origin/source discriminators, or advertises an unsupported session-control `format_version`.

## Local Workflow

Use these commands for intentional durable-format changes:

1. Run `scripts/check-durable-format-fixtures.sh` to verify the current fixtures.
2. If the byte change is intentional, run `scripts/regenerate-durable-format-fixtures.sh`.
3. Review the fixture diffs together with the code change before committing.

The shared pre-commit hook runs the check script before the broader test/lint pass, so accidental format drift fails locally.

## Fixture Inventory

The checked fixtures live in `tests/fixtures/durable-formats/`:

- `catalog-v1.bin`
- `commit-record-v3.hex`
- `segment-footer-v2.hex`
- `local-manifest-v1.bin`
- `remote-manifest-v1.bin`
- `remote-cache-entry-v1.bin`
- `backup-gc-birth-v1.bin`
- `workflow-native-registration-metadata-v1.bin`
- `workflow-execution-descriptor-v1.bin`
- `workflow-execution-descriptor-bundle-v1.bin`
- `workflow-deployment-record-v1.bin`
- `workflow-rollout-policy-immediate-v1.bin`
- `workflow-rollout-policy-manual-v1.bin`
- `workflow-deployment-preview-request-v1.bin`
- `workflow-deployment-resolution-request-v1.bin`
- `workflow-deployment-activation-request-v1.bin`
- `workflow-deployment-deactivation-request-v1.bin`
- `workflow-deployment-rollback-request-v1.bin`
- `workflow-deployment-activation-v1.bin`
- `workflow-deployment-activation-cleared-v1.bin`
- `workflow-visibility-entry-v1.bin`
- `workflow-visibility-list-request-v1.bin`
- `workflow-visibility-list-response-v1.bin`
- `workflow-describe-request-v1.bin`
- `workflow-describe-response-v1.bin`
- `workflow-history-page-request-v1.bin`
- `workflow-history-page-response-v1.bin`
- `workflow-query-request-v1.bin`
- `workflow-query-target-visibility-v1.bin`
- `workflow-query-target-execution-v1.bin`
- `workflow-query-response-v1.bin`
- `workflow-query-response-rejected-v1.bin`
- `workflow-update-request-v1.bin`
- `workflow-update-lane-update-v1.bin`
- `workflow-update-admission-v1.bin`
- `workflow-update-admission-rejected-v1.bin`
- `workflow-update-admission-duplicate-v1.bin`
- `workflow-compatibility-manifest-v1.bin`
- `workflow-compatibility-check-v1.bin`
- `workflow-compatibility-decision-v1.bin`
- `workflow-compatibility-decision-compatible-v1.bin`
- `workflow-compatibility-decision-requires-restart-as-new-v1.bin`
- `workflow-compatibility-decision-rejected-v1.bin`
- `workflow-upgrade-request-v1.bin`
- `workflow-upgrade-plan-v1.bin`
- `workflow-upgrade-plan-restart-as-new-v1.bin`
- `workflow-run-record-v1.bin`
- `workflow-state-record-v1.bin`
- `workflow-history-event-v1.bin`
- `workflow-lifecycle-record-v1.bin`
- `workflow-visibility-record-v1.bin`
- `workflow-inbox-trigger-update-v1.bin`
- `workflow-trigger-journal-entry-update-v1.bin`
- `workflow-runs-artifact-v1.bin`
- `workflow-state-artifact-v1.bin`
- `workflow-history-artifact-v1.bin`
- `workflow-lifecycle-artifact-v1.bin`
- `workflow-visibility-artifact-v1.bin`
