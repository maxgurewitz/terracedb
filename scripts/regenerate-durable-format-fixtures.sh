#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

TERRACEDB_REGENERATE_DURABLE_FIXTURES=1 cargo test durable_format_fixtures_match_golden_files --lib
