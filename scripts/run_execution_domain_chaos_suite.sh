#!/usr/bin/env bash

set -uo pipefail

campaign="${1:-}"

if [[ -z "${campaign}" ]]; then
  echo "usage: $0 <smoke|nightly>" >&2
  exit 64
fi

artifact_root=".tmp/execution-domain-chaos-artifacts"
artifact_dir="${artifact_root}/${campaign}"
results_file="${artifact_dir}/results.tsv"
summary_file="${artifact_dir}/summary.txt"

mkdir -p "${artifact_dir}"
: > "${results_file}"
: > "${summary_file}"

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
export RUST_TEST_THREADS="${RUST_TEST_THREADS:-1}"

smoke_cases=(
  "execution_domain_contracts|whole-system-campaign|whole_system_execution_domain_campaigns_remain_deterministic_across_seeds|false"
  "execution_domain_contracts|chaos-suite|execution_domain_chaos_suite_preserves_protected_progress_under_stalls_and_timing_skew|false"
)

nightly_cases=(
  "${smoke_cases[@]}"
  "execution_domain_contracts|nightly-seed-matrix|execution_domain_whole_system_nightly_seed_matrix|true"
)

declare -a selected_cases
case "${campaign}" in
  smoke)
    selected_cases=("${smoke_cases[@]}")
    ;;
  nightly)
    selected_cases=("${nightly_cases[@]}")
    ;;
  *)
    echo "unknown campaign '${campaign}'; expected smoke or nightly" >&2
    exit 64
    ;;
esac

failures=0

run_case() {
  local target="$1"
  local label="$2"
  local filter="$3"
  local ignored="$4"
  local logfile="${artifact_dir}/${label}.log"
  local -a cmd=(cargo test --locked --package terracedb --color always --test "${target}" "${filter}" -- --nocapture)

  if [[ "${ignored}" == "true" ]]; then
    cmd+=(--ignored)
  fi

  {
    echo "campaign=${campaign}"
    echo "label=${label}"
    echo "ignored=${ignored}"
    printf 'command='
    printf '%q ' "${cmd[@]}"
    echo
    echo
  } > "${logfile}"

  echo "==> [${label}] ${filter}"
  if "${cmd[@]}" 2>&1 | tee -a "${logfile}"; then
    printf '%s\tpass\t%s\n' "${label}" "${filter}" >> "${results_file}"
  else
    local status="${PIPESTATUS[0]}"
    printf '%s\tfail\t%s\n' "${label}" "${filter}" >> "${results_file}"
    {
      echo
      echo "exit_status=${status}"
    } >> "${logfile}"
    failures=$((failures + 1))
  fi

  echo
}

for case_entry in "${selected_cases[@]}"; do
  IFS='|' read -r target label filter ignored <<< "${case_entry}"
  run_case "${target}" "${label}" "${filter}" "${ignored}"
done

{
  echo "campaign=${campaign}"
  echo "total_cases=${#selected_cases[@]}"
  echo "failures=${failures}"
} > "${summary_file}"

if (( failures > 0 )); then
  echo "execution-domain chaos campaign '${campaign}' failed with ${failures} failing case(s)" >&2
  cat "${summary_file}" >&2
  exit 1
fi

echo "execution-domain chaos campaign '${campaign}' passed"
