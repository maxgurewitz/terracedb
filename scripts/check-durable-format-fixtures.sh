#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
while IFS= read -r git_env_var; do
    unset "$git_env_var"
done < <(git rev-parse --local-env-vars)
cd "$repo_root"

NEXTEST_PROFILE="${NEXTEST_PROFILE:-pre-commit}" \
    cargo nextest run --lib durable_format_fixtures_match_golden_files
