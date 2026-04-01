#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
while IFS= read -r git_env_var; do
    unset "$git_env_var"
done < <(git rev-parse --local-env-vars)
cd "$repo_root"

if [[ "${CODEX_REVIEW_ENABLED:-1}" == "1" && "${SKIP_CODEX_REVIEW:-0}" != "1" ]]; then
    echo "Running Codex review..."
    "$repo_root/scripts/run-codex-review.sh"
fi

if ! command -v cargo-nextest >/dev/null 2>&1; then
    echo "cargo-nextest is required; install it with 'cargo install cargo-nextest --locked'" >&2
    exit 1
fi

export NEXTEST_PROFILE="${NEXTEST_PROFILE:-pre-commit}"
nextest_test_threads="${NEXTEST_TEST_THREADS:-}"
if [[ -z "${nextest_test_threads}" ]]; then
    if command -v getconf >/dev/null 2>&1; then
        nextest_test_threads="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
    fi
    if [[ -z "${nextest_test_threads}" ]] && command -v sysctl >/dev/null 2>&1; then
        nextest_test_threads="$(sysctl -n hw.logicalcpu 2>/dev/null || true)"
    fi
    nextest_test_threads="${nextest_test_threads:-1}"
fi

echo "Running durable-format fixture checks..."
"$repo_root/scripts/check-durable-format-fixtures.sh"

echo "Running cargo nextest run --workspace --test-threads=${nextest_test_threads}..."
cargo nextest run --workspace --test-threads "${nextest_test_threads}"

echo "Running cargo test --workspace --doc..."
cargo test --workspace --doc

echo "Running cargo fmt --check..."
cargo fmt --all -- --check

echo "Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings
