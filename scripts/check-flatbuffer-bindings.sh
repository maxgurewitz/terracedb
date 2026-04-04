#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if ! command -v flatc >/dev/null 2>&1; then
    echo "flatc is required; install the FlatBuffers compiler and try again" >&2
    exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

schemas=(
    "catalog"
    "local_manifest"
    "remote_manifest"
    "remote_cache"
    "backup_gc"
)

for schema in "${schemas[@]}"; do
    flatc --rust -o "$tmpdir" "$repo_root/schemas/durable/${schema}.fbs"

    if ! cmp -s \
        "$tmpdir/${schema}_generated.rs" \
        "$repo_root/src/durable_formats/${schema}_generated.rs"; then
        echo "generated FlatBuffers bindings are out of date; run scripts/regenerate-flatbuffer-bindings.sh" >&2
        exit 1
    fi
done
