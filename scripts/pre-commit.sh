#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "Running durable-format fixture checks..."
"$repo_root/scripts/check-durable-format-fixtures.sh"

echo "Running cargo test --workspace..."
cargo test --workspace

echo "Running cargo fmt --check..."
cargo fmt --all -- --check

echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings
