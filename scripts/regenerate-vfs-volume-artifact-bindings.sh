#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

flatc --rust -o crates/terracedb-vfs/src/generated schemas/vfs_volume_artifact.fbs
