#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

echo "Running cargo test..."
cargo test

echo "Running cargo fmt --check..."
cargo fmt --all -- --check

echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings
