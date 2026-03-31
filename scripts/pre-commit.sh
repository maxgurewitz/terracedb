#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if ! command -v cargo-nextest >/dev/null 2>&1; then
    echo "cargo-nextest is required; install it with 'cargo install cargo-nextest --locked'" >&2
    exit 1
fi

export NEXTEST_PROFILE="${NEXTEST_PROFILE:-pre-commit}"

echo "Running durable-format fixture checks..."
"$repo_root/scripts/check-durable-format-fixtures.sh"

echo "Running cargo nextest run --workspace..."
cargo nextest run --workspace

echo "Running cargo test --workspace --doc..."
cargo test --workspace --doc

echo "Running cargo fmt --check..."
cargo fmt --all -- --check

echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings
