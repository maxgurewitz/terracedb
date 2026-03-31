#!/usr/bin/env bash

set -uo pipefail

campaign="${1:-}"

if [[ -z "${campaign}" ]]; then
  echo "usage: $0 <smoke|nightly>" >&2
  exit 64
fi

artifact_root=".tmp/remote-chaos-artifacts"
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
  "lib|s3-primary-flush|s3_primary_flush_persists_state_and_durable_change_feed_across_reopen"
  "lib|offload-read-equivalence|reads_are_identical_before_and_after_remote_offload"
  "lib|remote-columnar-range-fetch|remote_columnar_scan_fetches_only_requested_ranges"
  "simulation_harness|s3-primary-sim-flush-failure|s3_primary_simulation_failed_flush_recovers_last_durable_prefix"
  "simulation_harness|cold-offload-network-retry|cold_offload_simulation_retries_after_network_fault_and_recovers_remote_state"
  "simulation_harness|remote-cache-network-restart|remote_cache_survives_simulated_restart_and_masks_warmed_network_faults"
)

nightly_cases=(
  "${smoke_cases[@]}"
  "lib|s3-primary-manifest-retry|s3_primary_failed_manifest_upload_preserves_last_durable_prefix_until_retry"
  "lib|s3-primary-crash-recovery|s3_primary_crash_recovery_drops_unflushed_visible_tail"
  "lib|offload-upload-recovery|offload_upload_without_manifest_switch_recovers_prior_local_generation"
  "lib|offload-manifest-switch-recovery|offload_manifest_switch_survives_reopen_even_if_local_cleanup_did_not_run"
  "lib|offload-remote-only-recovery|offload_after_local_file_deletion_recovers_remote_only_state"
  "lib|remote-columnar-raw-cache-s3|remote_columnar_point_reads_reuse_raw_byte_cache_in_s3_primary_mode"
  "lib|remote-columnar-raw-cache-tiered|remote_columnar_point_reads_reuse_raw_byte_cache_in_tiered_cold_mode"
  "lib|decoded-cache-restart|columnar_decoded_cache_drops_on_restart_while_raw_byte_cache_rebuilds"
  "simulation_harness|s3-primary-remote-scan-failure|s3_primary_change_feed_simulation_surfaces_structured_remote_scan_failures"
  "simulation_harness|s3-primary-durable-prefix-recovery|s3_primary_simulation_recovers_to_last_durable_prefix"
  "simulation_harness|remote-columnar-range-restart|remote_columnar_cache_survives_simulated_restart_and_masks_warmed_range_faults"
  "simulation_harness|remote-list-failure-structure|remote_list_failures_are_structured_in_simulation"
  "simulation_harness|tiered-backup-disk-loss-recovery|tiered_backup_recovery_restores_remote_state_after_simulated_disk_loss"
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
  local logfile="${artifact_dir}/${label}.log"
  local -a cmd=(cargo test --locked --package terracedb --color always)

  if [[ "${target}" == "lib" ]]; then
    cmd+=(--lib "${filter}" -- --nocapture)
  else
    cmd+=(--test "${target}" "${filter}" -- --nocapture)
  fi

  {
    echo "campaign=${campaign}"
    echo "label=${label}"
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
  IFS='|' read -r target label filter <<< "${case_entry}"
  run_case "${target}" "${label}" "${filter}"
done

{
  echo "campaign=${campaign}"
  echo "total_cases=${#selected_cases[@]}"
  echo "failures=${failures}"
} > "${summary_file}"

if (( failures > 0 )); then
  echo "remote chaos campaign '${campaign}' failed with ${failures} failing case(s)" >&2
  cat "${summary_file}" >&2
  exit 1
fi

echo "remote chaos campaign '${campaign}' passed"
