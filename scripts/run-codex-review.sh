#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
while IFS= read -r git_env_var; do
    unset "$git_env_var"
done < <(git rev-parse --local-env-vars)
cd "$repo_root"

strict_review="${CODEX_REVIEW_STRICT:-0}"
review_timeout_seconds="${CODEX_REVIEW_TIMEOUT_SECONDS:-180}"
review_reasoning_effort="${CODEX_REVIEW_REASONING_EFFORT:-low}"
prior_round_limit="${CODEX_REVIEW_PRIOR_ROUNDS_MAX:-3}"
max_context_file_bytes="${CODEX_REVIEW_MAX_CONTEXT_FILE_BYTES:-50000}"
max_context_markdown_bytes="${CODEX_REVIEW_MAX_CONTEXT_MARKDOWN_BYTES:-20000}"
max_logged_decisions="${CODEX_REVIEW_MAX_DECISIONS:-50}"

handle_review_infra_failure() {
    local message="$1"

    if [[ "$strict_review" == "1" ]]; then
        echo "$message" >&2
        echo "REVIEW_FAILED"
        exit 1
    fi

    echo "$message" >&2
    echo "Skipping Codex review failure because CODEX_REVIEW_STRICT is not enabled." >&2
    exit 0
}

run_with_timeout() {
    local timeout_seconds="$1"
    local timed_out_flag="$2"
    local stdin_path="$3"
    shift 3

    (
        exec < "$stdin_path"
        "$@"
    ) &
    local command_pid="$!"

    (
        sleep "$timeout_seconds"
        if kill -0 "$command_pid" 2>/dev/null; then
            : > "$timed_out_flag"
            kill "$command_pid" 2>/dev/null || true
            sleep 2
            kill -9 "$command_pid" 2>/dev/null || true
        fi
    ) &
    local watchdog_pid="$!"

    local command_status=0
    if wait "$command_pid"; then
        command_status=0
    else
        command_status="$?"
    fi

    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true

    return "$command_status"
}

if [[ "${SKIP_CODEX_REVIEW:-0}" == "1" ]]; then
    echo "Skipping Codex review because SKIP_CODEX_REVIEW=1."
    exit 0
fi

if git diff --cached --quiet --exit-code; then
    echo "Skipping Codex review because there are no staged changes."
    exit 0
fi

if ! command -v codex >/dev/null 2>&1; then
    handle_review_infra_failure "codex is required for the optional review hook."
fi

if ! command -v jq >/dev/null 2>&1; then
    handle_review_infra_failure "jq is required for the optional review hook."
fi

guidelines_path="$repo_root/docs/GUIDELINES.md"
if [[ ! -f "$guidelines_path" ]]; then
    handle_review_infra_failure "Missing review guidelines at $guidelines_path."
fi

schema_path="$repo_root/scripts/codex-review-output.schema.json"
if [[ ! -f "$schema_path" ]]; then
    handle_review_infra_failure "Missing Codex review schema at $schema_path."
fi

base_ref="${CODEX_REVIEW_BASE:-main}"
if ! git rev-parse --verify --quiet "${base_ref}^{commit}" >/dev/null; then
    if git rev-parse --verify --quiet "origin/${base_ref}^{commit}" >/dev/null; then
        base_ref="origin/${base_ref}"
    else
        handle_review_infra_failure "Unable to resolve the Codex review base branch '${base_ref}'."
    fi
fi

merge_base="$(git merge-base HEAD "$base_ref" 2>/dev/null || true)"
if [[ -z "$merge_base" ]]; then
    handle_review_infra_failure "Unable to compute a merge-base for HEAD and ${base_ref}."
fi

branch_name="$(git branch --show-current 2>/dev/null || true)"
branch_name="${branch_name:-detached-head}"
branch_slug="$(printf '%s' "$branch_name" | tr '/[:space:]' '--' | tr -cd 'A-Za-z0-9._-')"
branch_slug="${branch_slug:-detached-head}"

timestamp="$(date +%Y%m%d-%H%M%S)"
review_root_dir="$repo_root/.tmp/codex-review/$branch_slug"
reviews_dir="$review_root_dir/reviews"
decision_log_path="$review_root_dir/decisions.json"
context_path="$review_root_dir/context.md"
report_prefix="review-${timestamp}-${branch_slug}"
report_path="$reviews_dir/${report_prefix}.md"
response_path="$reviews_dir/${report_prefix}.json"
stdout_path="$reviews_dir/${report_prefix}.stdout.log"
stderr_path="$reviews_dir/${report_prefix}.stderr.log"
timed_out_flag="$reviews_dir/${report_prefix}.timed_out"
prompt_path=""

if ! mkdir -p "$reviews_dir"; then
    handle_review_infra_failure "Unable to create the Codex review report directory at ${reviews_dir}."
fi

if [[ ! -f "$decision_log_path" ]]; then
    if ! jq -n --arg branch "$branch_name" '{schema_version: 1, branch: $branch, decisions: []}' > "$decision_log_path"; then
        handle_review_infra_failure "Unable to initialize the Codex review decision log at ${decision_log_path}."
    fi
fi

if [[ ! -f "$context_path" ]]; then
    if ! : > "$context_path"; then
        handle_review_infra_failure "Unable to initialize the Codex review context file at ${context_path}."
    fi
fi

if ! prompt_path="$(mktemp "$reviews_dir/${report_prefix}.prompt.XXXXXX.txt")"; then
    handle_review_infra_failure "Unable to create a temporary Codex review prompt file in ${reviews_dir}."
fi

trap '[[ -n "$prompt_path" ]] && rm -f "$prompt_path"; rm -f "$timed_out_flag"' EXIT

staged_files="$(git diff --cached --name-only --relative --no-color)"
staged_stat="$(git diff --cached --stat --find-renames --no-color)"
staged_diff="$(git diff --cached --find-renames --no-color)"
branch_stat="$(git diff --stat --find-renames --no-color "${merge_base}...HEAD")"
guidelines_content="$(cat "$guidelines_path")"
staged_file_contents=""
while IFS= read -r staged_file; do
    [[ -z "$staged_file" ]] && continue

    if ! git cat-file -e ":${staged_file}" 2>/dev/null; then
        staged_file_contents+=$'\n'"--- FILE: ${staged_file} (not available in the staged index snapshot) ---"$'\n'
        continue
    fi

    file_bytes="$(git cat-file -s ":${staged_file}")"
    if (( file_bytes > max_context_file_bytes )); then
        staged_file_contents+=$'\n'"--- FILE: ${staged_file} omitted because it is ${file_bytes} bytes, above the ${max_context_file_bytes}-byte context limit ---"$'\n'
        continue
    fi

    staged_file_contents+=$'\n'"--- FILE: ${staged_file} ---"$'\n'
    staged_file_contents+="$(git show ":${staged_file}")"
    staged_file_contents+=$'\n'
done <<< "$staged_files"

context_markdown="No additional branch review context is recorded."
if [[ -s "$context_path" ]]; then
    context_file_bytes="$(wc -c < "$context_path")"
    if (( context_file_bytes > max_context_markdown_bytes )); then
        context_markdown="Branch review context at ${context_path} is ${context_file_bytes} bytes, which is above the ${max_context_markdown_bytes}-byte prompt limit."
    else
        context_markdown="$(cat "$context_path")"
    fi
fi

if ! decisions_excerpt="$(jq -c --arg branch "$branch_name" --argjson max_decisions "$max_logged_decisions" '
    {
      schema_version: (.schema_version // 1),
      branch: (.branch // $branch),
      decisions: (
        (.decisions // [])
        | if length > $max_decisions then .[-$max_decisions:] else . end
      )
    }
' "$decision_log_path" 2>/dev/null)"; then
    handle_review_infra_failure "Unable to read the Codex review decision log at ${decision_log_path}."
fi

prior_review_rounds="No prior review rounds are recorded for this branch."
prior_review_paths=()
while IFS= read -r prior_review_path; do
    [[ -z "$prior_review_path" ]] && continue
    prior_review_paths+=("$prior_review_path")
done < <(find "$reviews_dir" -maxdepth 1 -type f -name 'review-*.json' | sort | tail -n "$prior_round_limit")

if (( ${#prior_review_paths[@]} > 0 )); then
    prior_review_rounds=""
    for prior_review_path in "${prior_review_paths[@]}"; do
        if ! prior_review_summary="$(jq -c --arg review_file "$prior_review_path" '
            {
              review_file: $review_file,
              status,
              summary,
              blocking_findings,
              warnings
            }
        ' "$prior_review_path" 2>/dev/null)"; then
            prior_review_summary="$(cat "$prior_review_path")"
        fi

        prior_review_rounds+=$'\n'"--- PRIOR REVIEW ROUND ---"$'\n'
        prior_review_rounds+="${prior_review_summary}"
        prior_review_rounds+=$'\n'
    done
fi

session_log_path="${CODEX_PARENT_SESSION_LOG:-}"
session_log_note="No parent Codex session log path was provided."
if [[ -n "$session_log_path" && -f "$session_log_path" ]]; then
    session_log_note="Optional parent Codex session log: ${session_log_path}"
fi

if ! cat > "$prompt_path" <<EOF
Review the current staged changes in the repository at ${repo_root}.

Do not modify files, create files, change git state, or run shell commands.
Inspect only the provided context and return JSON that matches the provided
schema.

Intentional design note:
- This hook is intentionally allowed to write review artifacts under
  ${review_root_dir}, which is inside the repo's ignored .tmp area.
- Do not treat writing those ignored .tmp review artifacts as a bug or blocking
  finding by itself.

Review target:
- the staged changes from \`git diff --cached\`
- the repository guidelines in ${guidelines_path}
- broader branch context versus ${base_ref} only when it helps assess the
  staged changes

Focus on:
- violations of docs/GUIDELINES.md
- likely bugs or behavioral regressions
- missing tests required by the guidelines, especially around persistence,
  recovery, concurrency, or simulation semantics
- naming or compatibility-shim choices that conflict with the repository
  guidance
- documentation updates that are needed because repo-wide practice changed

Only report issues that are specific, actionable, and worth blocking the
commit on. Put non-blocking concerns in warnings. Avoid vague style commentary.

Set \`status\` to \`fail\` only when you found one or more blocking findings.
Set \`status\` to \`pass\` when there are no blocking findings.

Review memory policy:
- Prior review rounds and the branch decision log are authoritative context.
- If a prior finding has been marked \`dismissed\`, do not re-raise it unless
  the staged changes materially change the issue or the dismissal rationale
  clearly conflicts with the repository guidelines.
- If a prior finding has been marked \`deferred\`, prefer a warning instead of a
  blocking finding unless the staged changes materially worsen or reintroduce it.
- Reuse the same finding \`id\` when re-raising the same underlying issue from a
  prior review round.
- For new findings, assign a stable lowercase hyphenated \`id\`.
- In \`full_review_markdown\`, include the finding IDs in the section headings so
  future decision logs can refer back to them.

Current branch: ${branch_name}
Base ref: ${base_ref}
Merge base: ${merge_base}

Staged files list:
${staged_files}

Staged diff stat:
${staged_stat}

Branch diff stat versus merge base:
${branch_stat}

${session_log_note}
Treat that session-log path as optional context only if it was provided.

Branch review decision log path:
${decision_log_path}

Branch review context file path:
${context_path}

Branch review context markdown:
${context_markdown}

Branch review decisions JSON:
${decisions_excerpt}

Recent prior branch review rounds:
${prior_review_rounds}

Repository guidelines:
${guidelines_content}

Current staged file contents:
${staged_file_contents}

Staged diff:
${staged_diff}
EOF
then
    handle_review_infra_failure "Unable to write the Codex review prompt file at ${prompt_path}."
fi

codex_args=(
    exec
    --cd "$repo_root"
    --ephemeral
    -c "model_reasoning_effort=\"${review_reasoning_effort}\""
    --sandbox read-only
    --output-schema "$schema_path"
    --output-last-message "$response_path"
    --color never
)

if [[ -n "${CODEX_REVIEW_MODEL:-}" ]]; then
    codex_args+=(--model "$CODEX_REVIEW_MODEL")
fi

rm -f "$timed_out_flag"
if ! run_with_timeout "$review_timeout_seconds" "$timed_out_flag" "$prompt_path" codex "${codex_args[@]}" - >"$stdout_path" 2>"$stderr_path"; then
    if [[ -f "$timed_out_flag" ]]; then
        handle_review_infra_failure "Codex review timed out after ${review_timeout_seconds} seconds. See ${stdout_path} and ${stderr_path} for captured output."
    fi
    handle_review_infra_failure "Codex review command failed. See ${stdout_path} and ${stderr_path} for captured output."
fi

if [[ ! -s "$response_path" ]]; then
    handle_review_infra_failure "Codex review did not write a response payload to ${response_path}."
fi

status="$(jq -er '.status' "$response_path" 2>/dev/null)" || handle_review_infra_failure "Codex review returned malformed JSON status in ${response_path}."
summary="$(jq -er '.summary' "$response_path" 2>/dev/null)" || handle_review_infra_failure "Codex review returned malformed JSON summary in ${response_path}."

if ! jq -r '.full_review_markdown' "$response_path" > "$report_path"; then
    handle_review_infra_failure "Codex review returned malformed Markdown content in ${response_path}."
fi

echo "$summary"

blocking_count="$(jq -er '.blocking_findings | length' "$response_path" 2>/dev/null)" || handle_review_infra_failure "Codex review returned malformed blocking findings in ${response_path}."
if (( blocking_count > 0 )); then
    echo "Blocking findings:"
    jq -r '.blocking_findings[] | "- [" + .id + "] " + .title + (if (.file // "") == "" then "" else " (" + .file + ")" end)' "$response_path"
fi

warning_count="$(jq -er '.warnings | length' "$response_path" 2>/dev/null)" || handle_review_infra_failure "Codex review returned malformed warnings in ${response_path}."
if (( warning_count > 0 )); then
    echo "Warnings:"
    jq -r '.warnings[] | "- [" + .id + "] " + .title + (if (.file // "") == "" then "" else " (" + .file + ")" end)' "$response_path"
fi

echo "Decision log: $decision_log_path"
echo "Context file: $context_path"
echo "Full review: $report_path"

if [[ "$status" == "fail" ]]; then
    rm -f "$stdout_path" "$stderr_path"
    echo "REVIEW_FAILED"
    exit 1
fi

if [[ "$status" != "pass" ]]; then
    handle_review_infra_failure "Codex review returned an unexpected status '${status}' in ${response_path}."
fi

rm -f "$stdout_path" "$stderr_path"

echo "REVIEW_SUCCEEDED"
