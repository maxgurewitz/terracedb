# Telemetry API Example

This example is a small hybrid OLTP/OLAP service built on Terracedb.

It keeps two tables side by side:

- `device_state`: a row-oriented current-state table keyed by device
- `sensor_readings`: a historical columnar table keyed by `(device_id, reading_at_ms)`

The API is intentionally small, but it demonstrates the Phase 11 path:

- point writes update the current device state and append historical readings in the same durable transaction
- point reads fetch the latest row-oriented state for one device
- projected historical scans read selected columns from the columnar table over a time window
- the summary endpoint computes alert counts plus min/max/avg temperature by streaming the scan path instead of using a separate query engine

## Run It

Base profile:

```bash
cargo run -p terracedb-example-telemetry-api
```

Accelerator profile:

```bash
TELEMETRY_API_PROFILE=accelerated cargo run -p terracedb-example-telemetry-api
```

Useful environment variables:

- `TELEMETRY_API_BIND_ADDR`
- `TELEMETRY_API_DATA_DIR`
- `TELEMETRY_API_PROFILE`

The default bind address is `127.0.0.1:9602`.

## Endpoints

Ingest one or more readings:

```bash
curl -X POST http://127.0.0.1:9602/readings \
  -H 'content-type: application/json' \
  -d '{
    "readings": [
      {
        "device_id": "device-01",
        "reading_at_ms": 100,
        "temperature_c": 20,
        "humidity_pct": 40,
        "battery_mv": 3600,
        "alert_active": false
      },
      {
        "device_id": "device-01",
        "reading_at_ms": 200,
        "temperature_c": 22,
        "humidity_pct": 42,
        "battery_mv": 3590,
        "alert_active": true
      }
    ]
  }'
```

Fetch the latest state for one device:

```bash
curl http://127.0.0.1:9602/devices/device-01/state
```

Run a projected historical scan over a time window:

```bash
curl "http://127.0.0.1:9602/devices/device-01/readings?start_ms=100&end_ms=301&columns=temperature_c,alert_active&only_alerts=true"
```

Run the scan-backed summary:

```bash
curl "http://127.0.0.1:9602/devices/device-01/summary?start_ms=100&end_ms=301"
```

## Base Vs Accelerated

The base profile is the authoritative path and the default recommendation.

It uses:

- tiered storage with bounded local bytes
- bounded raw-byte and decoded column caches
- columnar history reads through Terracedb’s normal projected scan path

The accelerator profile is explicit opt-in. It turns on:

- skip indexes for `alert_active`
- a projection sidecar for `temperature_c` + `alert_active`
- compact-to-wide promotion
- aggressive background repair

Those accelerants are optional. The example’s tests verify that:

- base and accelerated profiles return the same logical answers
- disabled accelerants do not change results
- missing or corrupt projection sidecars fall back to the base path instead of breaking reads
- low-budget restart scenarios still reopen cleanly and preserve answers

## Feature Mapping

- `POST /readings`:
  updates row-oriented current state and appends historical columnar data in one commit
- `GET /devices/:id/state`:
  exercises the hot point-read path
- `GET /devices/:id/readings`:
  exercises projected columnar scans over a bounded time window
- `GET /devices/:id/summary`:
  uses the scan path to compute alert counts plus min/max/avg temperature

The default `temperature_c,alert_active` projection is the workload the accelerator profile is tuned for, while the base profile remains fully supported and correct when those accelerants are off.
