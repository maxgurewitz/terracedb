# Order Watch

`order-watch` freezes the end-to-end CDC example boundary used by T96 and now doubles as a
runnable reference for the intended Debezium -> projections -> workflows happy path.

The scenario is intentionally small:

- Debezium reads a larger `commerce` database on Kafka.
- Terracedb keeps only `public.orders`.
- A row predicate keeps only west-region orders.
- Hybrid materialization preserves both replayable `*_cdc` history and a `*_current` mirror.
- A projection derives:
  - `attention_orders`: open west-region orders visible to the application, and
  - `attention_transitions`: append-only enter/exit events for workflow routing.
- A workflow emits one durable outbox alert per transition.

The frozen fixture sequence is:

1. ignore a `public.customers` snapshot row,
2. drop an east-region `public.orders` snapshot row,
3. retain a west-region `public.orders` snapshot row without creating an alert,
4. derive one backlog alert before the workflow attaches,
5. derive one live enter alert after attach, and
6. derive one live exit alert after that row leaves the watched subset.

The documented workflow modes are:

- `historical-replay`: attach from the beginning and replay the backlog transition stream.
- `live-only-attach`: attach from the current durable frontier and skip pre-existing transition backlog.

The tests for this crate are the contract for the boundary: they prove the Kafka ingress offsets,
Debezium materializations, projection frontier, workflow attach mode, and user-visible outputs can
all be checked from one deterministic seeded run.

Both workflow modes now run through the real workflow runtime from the same append-only
`attention_transitions` stream. The example intentionally uses the newer ergonomics added while
freezing this boundary:

- `create_layout_tables(...)` and `DebeziumMaterializer::from_layouts(...)` remove repetitive
  layout bootstrapping.
- `PostgresDebeziumDecoder::from_layouts(...)` and `DebeziumIngressHandler::postgres(...)` let the
  ingress path come from layouts instead of hand-built table collections.
- `DebeziumDerivedTransitionProjection` derives append-only workflow routing rows directly from the
  replayable Debezium event log, which avoids the old mirror-delete provenance pitfall.
- `WorkflowSourceConfig::{historical_replayable_source, live_only_current_state_source,
  live_only_replayable_append_only_source}` captures the intended workflow attach presets.
- `ProjectionSequenceRun::source_scoped_entry_key(...)` and
  `RecordTable::decode_change_entry(...)` remove a lot of example-specific key and decode plumbing.

## Running The Deterministic Contract

```bash
cargo test -p terracedb-example-order-watch --tests
```

## Running Against Real Services

The example also includes a compose stack with Postgres, Redpanda, and Debezium Connect plus a
standalone verification script:

```bash
examples/order-watch/debug_compose.sh
```

That script:

- starts [`docker-compose.yml`](./docker-compose.yml),
- seeds the snapshot rows for the frozen scenario,
- registers the checked-in Debezium connector from [`connect/order-watch-postgres.json`](./connect/order-watch-postgres.json),
- waits for the expected Kafka offsets for snapshot and live phases, and
- runs the example binary against those services until it prints verified historical and live-only
  outcomes.

The compose services stay up after a successful run so the resulting topics, connector state, and
local Terracedb files can be inspected manually. Run `docker compose -f examples/order-watch/docker-compose.yml down -v`
to reset the demo environment.
