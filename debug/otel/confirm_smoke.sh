#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
TEMPO_READY_URL="${TEMPO_READY_URL:-http://127.0.0.1:3320/ready}"
TEMPO_API_BASE_URL="${TEMPO_API_BASE_URL:-http://127.0.0.1:3320}"
OTLP_ENDPOINT="${TERRACEDB_OTLP_ENDPOINT:-http://127.0.0.1:14317}"

trace_response=""

cleanup() {
  rm -f "${trace_response}"
}

trap cleanup EXIT

echo "Starting local Tempo/OTel stack..."
docker compose -f "${COMPOSE_FILE}" up -d >/dev/null

echo "Waiting for Tempo readiness at ${TEMPO_READY_URL}..."
for _ in $(seq 1 60); do
  if curl -sf "${TEMPO_READY_URL}" >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf "${TEMPO_READY_URL}" >/dev/null

echo "Running Terracedb OTLP smoke example..."
example_output=$(
  cd "${REPO_ROOT}"
  TERRACEDB_OTLP_ENDPOINT="${OTLP_ENDPOINT}" cargo run -q -p terracedb-otel --example local_otlp_smoke
)
printf '%s\n' "${example_output}"

trace_id=$(printf '%s\n' "${example_output}" | sed -n 's/^trace_id=//p')
run_id=$(printf '%s\n' "${example_output}" | sed -n 's/^run_id=//p')
service_name=$(printf '%s\n' "${example_output}" | sed -n 's/^service_name=//p')

if [[ -z "${trace_id}" || -z "${run_id}" || -z "${service_name}" ]]; then
  echo "Failed to parse smoke example output." >&2
  exit 1
fi

trace_response=$(mktemp)

trace_url="${TEMPO_API_BASE_URL}/api/traces/${trace_id}"

echo "Querying Tempo directly for trace ${trace_id}..."
for _ in $(seq 1 20); do
  curl -sS -o "${trace_response}" "${trace_url}"

  if grep -q 'app.terracedb.otel_smoke' "${trace_response}" \
    && grep -q 'terracedb.db.commit' "${trace_response}" \
    && grep -q 'terracedb.db.flush' "${trace_response}" \
    && grep -q "${run_id}" "${trace_response}"; then
    break
  fi

  sleep 1
done

if ! grep -q 'app.terracedb.otel_smoke' "${trace_response}" \
  || ! grep -q 'terracedb.db.commit' "${trace_response}" \
  || ! grep -q 'terracedb.db.flush' "${trace_response}" \
  || ! grep -q "${run_id}" "${trace_response}"; then
  echo "Tempo API did not return the expected Terracedb trace." >&2
  cat "${trace_response}" >&2
  exit 1
fi

echo "confirmed_service_name=${service_name}"
echo "confirmed_run_id=${run_id}"
echo "confirmed_trace_id=${trace_id}"
echo "confirmed_spans=app.terracedb.otel_smoke,terracedb.db.commit,terracedb.db.flush"
echo "confirmed_via_tempo_api=${trace_url}"
echo "status=ok"
