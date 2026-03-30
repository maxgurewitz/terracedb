# Local OTEL Debugging

This directory is for one-off local validation only. The main test path stays on the in-memory `TelemetryMode::Test` exporter and does not connect to a live OTEL collector.

Run the end-to-end smoke check with:

```bash
./debug/otel/confirm_smoke.sh
```

That script:

- starts the local `grafana/otel-lgtm` stack from `debug/otel/docker-compose.yml`,
- runs `crates/terracedb-otel/examples/local_otlp_smoke.rs`,
- submits Terracedb telemetry to OTLP,
- queries Tempo back through its direct HTTP trace API,
- and fails if the expected Terracedb spans are not present.
