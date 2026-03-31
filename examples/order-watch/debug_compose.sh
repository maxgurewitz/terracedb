#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/../.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.yml"
CONNECTOR_CONFIG="${SCRIPT_DIR}/connect/order-watch-postgres.json"
DATA_DIR="${REPO_ROOT}/.tmp/order-watch-compose-data"
LOG_FILE="${DATA_DIR}/order-watch.log"
BOOTSTRAP_SERVERS="${ORDER_WATCH_KAFKA_BOOTSTRAP_SERVERS:-127.0.0.1:19092}"
TIMEOUT_SECS="${ORDER_WATCH_TIMEOUT_SECS:-90}"
COMPOSE=(docker compose -f "${COMPOSE_FILE}")

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_for() {
  local label="$1"
  shift

  for _ in $(seq 1 90); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for ${label}" >&2
  return 1
}

topic_high_watermark() {
  "${COMPOSE[@]}" exec -T redpanda rpk topic describe "$1" -p 2>/dev/null \
    | awk '$1 == "0" { print $6; exit }'
}

wait_for_high_watermark() {
  local topic="$1"
  local expected="$2"

  for _ in $(seq 1 90); do
    local observed
    observed="$(topic_high_watermark "${topic}")"
    if [[ "${observed}" == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for ${topic} high watermark ${expected}" >&2
  return 1
}

create_topic() {
  "${COMPOSE[@]}" exec -T redpanda \
    rpk topic create "$1" --partitions 1 --replicas 1 >/dev/null
}

seed_snapshot_rows() {
  "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d commerce <<'SQL'
CREATE TABLE IF NOT EXISTS public.customers (
    id TEXT PRIMARY KEY,
    tier TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS public.orders (
    id TEXT PRIMARY KEY,
    region TEXT NOT NULL,
    status TEXT NOT NULL
);
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
TRUNCATE TABLE public.orders, public.customers;
INSERT INTO public.orders (id, region, status) VALUES
    ('010', 'east', 'open'),
    ('100', 'west', 'open');
INSERT INTO public.customers (id, tier) VALUES
    ('900', 'vip');
SQL
}

apply_live_changes() {
  "${COMPOSE[@]}" exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d commerce <<'SQL'
INSERT INTO public.orders (id, region, status) VALUES
    ('101', 'west', 'open'),
    ('102', 'west', 'open');
UPDATE public.orders
SET region = 'east',
    status = 'closed'
WHERE id = '102';
SQL
}

require_command cargo
require_command curl
require_command docker

rm -rf "${DATA_DIR}"
mkdir -p "${DATA_DIR}"

"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d >/dev/null

wait_for "postgres" "${COMPOSE[@]}" exec -T postgres pg_isready -U postgres -d commerce
wait_for "redpanda" "${COMPOSE[@]}" exec -T redpanda rpk cluster info
wait_for "kafka connect" curl -fsS http://127.0.0.1:18083/connectors

create_topic "commerce.public.orders"
create_topic "commerce.public.customers"
create_topic "commerce.schema_history"

seed_snapshot_rows

curl -fsS \
  -X POST \
  -H 'Content-Type: application/json' \
  --data "@${CONNECTOR_CONFIG}" \
  http://127.0.0.1:18083/connectors >/dev/null

wait_for "order-watch connector" curl -fsS http://127.0.0.1:18083/connectors/order-watch/status
wait_for_high_watermark "commerce.public.orders" "2"
wait_for_high_watermark "commerce.public.customers" "1"

apply_live_changes
wait_for_high_watermark "commerce.public.orders" "5"

cargo run -p terracedb-example-order-watch -- \
  --bootstrap-servers "${BOOTSTRAP_SERVERS}" \
  --data-dir "${DATA_DIR}" \
  --timeout-secs "${TIMEOUT_SECS}" | tee "${LOG_FILE}"

grep -q "historical-replay verified" "${LOG_FILE}"
grep -q "live-only-attach verified" "${LOG_FILE}"

echo
echo "order-watch docker demo verified successfully"
echo "services are still running for inspection"
echo "teardown: docker compose -f ${COMPOSE_FILE} down -v"
