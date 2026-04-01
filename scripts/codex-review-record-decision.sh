#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/codex-review-record-decision.sh \
    --finding-id <finding-id> \
    --resolution <dismissed|deferred|accepted> \
    --reason <text> \
    [--review-file <absolute-or-relative-path>] \
    [--actor <name>]

Records a branch-local review decision under .tmp/codex-review/<branch>/decisions.json.
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
while IFS= read -r git_env_var; do
    unset "$git_env_var"
done < <(git rev-parse --local-env-vars)
cd "$repo_root"

if ! command -v jq >/dev/null 2>&1; then
    echo "jq is required to record Codex review decisions." >&2
    exit 1
fi

finding_id=""
resolution=""
reason=""
review_file=""
actor="${CODEX_REVIEW_DECISION_ACTOR:-${USER:-unknown}}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --finding-id)
            finding_id="${2:-}"
            shift 2
            ;;
        --resolution)
            resolution="${2:-}"
            shift 2
            ;;
        --reason)
            reason="${2:-}"
            shift 2
            ;;
        --review-file)
            review_file="${2:-}"
            shift 2
            ;;
        --actor)
            actor="${2:-}"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "$finding_id" || -z "$resolution" || -z "$reason" ]]; then
    usage >&2
    exit 1
fi

case "$resolution" in
    dismissed|deferred|accepted)
        ;;
    *)
        echo "Resolution must be one of: dismissed, deferred, accepted." >&2
        exit 1
        ;;
esac

branch_name="$(git branch --show-current 2>/dev/null || true)"
branch_name="${branch_name:-detached-head}"
branch_slug="$(printf '%s' "$branch_name" | tr '/[:space:]' '--' | tr -cd 'A-Za-z0-9._-')"
branch_slug="${branch_slug:-detached-head}"

review_root_dir="$repo_root/.tmp/codex-review/$branch_slug"
decision_log_path="$review_root_dir/decisions.json"
context_path="$review_root_dir/context.md"

mkdir -p "$review_root_dir"

if [[ ! -f "$decision_log_path" ]]; then
    jq -n --arg branch "$branch_name" '{schema_version: 1, branch: $branch, decisions: []}' > "$decision_log_path"
fi

if [[ ! -f "$context_path" ]]; then
    : > "$context_path"
fi

recorded_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
tmp_path="$(mktemp "$review_root_dir/decisions.XXXXXX.json")"

if ! jq \
    --arg branch "$branch_name" \
    --arg finding_id "$finding_id" \
    --arg resolution "$resolution" \
    --arg reason "$reason" \
    --arg actor "$actor" \
    --arg recorded_at "$recorded_at" \
    --arg review_file "$review_file" \
    '
    .schema_version = 1
    | .branch = ($branch)
    | .decisions = (
        (.decisions // [])
        + [
            ({
                finding_id: $finding_id,
                resolution: $resolution,
                reason: $reason,
                actor: $actor,
                recorded_at: $recorded_at
            } + (if $review_file == "" then {} else {review_file: $review_file} end))
        ]
    )
    ' "$decision_log_path" > "$tmp_path"; then
    rm -f "$tmp_path"
    echo "Failed to update $decision_log_path." >&2
    exit 1
fi

mv "$tmp_path" "$decision_log_path"

echo "Recorded ${resolution} decision for [${finding_id}]."
echo "Decision log: $decision_log_path"
echo "Context file: $context_path"
