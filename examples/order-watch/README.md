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
Debezium materializations, projection frontier, broker reconnect behavior, workflow attach mode,
checkpoint restore, and user-visible outputs can all be checked from one deterministic seeded run.

## Read Surface

The example intentionally exposes a small application-facing read surface instead of a service API:

- `OrderWatchBoundary::attention_orders_table(&db)` is the typed read model the application would
  query for open west-region orders needing attention.
- `OrderWatchBoundary::attention_transitions_table(&db)` is the typed append-only stream the
  workflow consumes to emit alerts.
- `boundary.orders_layout().current_table_name()` is the underlying Debezium mirror table for the
  filtered `orders` current-state when Hybrid or Mirror materialization is enabled.
- `OrderWatchOrder::from_current_value(...)` decodes that mirror row into the example's typed
  `OrderWatchOrder` surface when callers want to inspect the raw current-state directly.

Materialization expectations are part of the example contract:

- `EventLog` and `Hybrid` produce the same logical `attention_orders`, `attention_transitions`, and
  outbox alerts because both feed projections and workflows from the replayable `*_cdc` history.
- `Hybrid` additionally preserves the filtered `*_current` mirror for direct application reads.
- `Mirror` keeps only that filtered current-state. It can show the retained west-region rows, but it
  cannot reconstruct the historical enter/exit transition stream the example uses for backlog replay.
  That is why the example documents `Mirror` as a current-state-only surface, not a full substitute
  for replayable CDC history.

Filtering expectations are part of the example contract too:

- `OrderWatchBoundary::table_filter()` and `row_predicate()` intentionally sit at the Terracedb
  boundary even though Debezium can also narrow the upstream feed. For very large upstream
  databases, keep connector include-lists tight so Kafka is not carrying irrelevant schemas or
  tables, then keep the Terracedb-side filters so retained history, mirrors, projections, and
  workflows only pay storage and replay costs for the subset this example actually owns.
- If the watched slice is still very large, prefer splitting it into multiple narrower Terracedb
  boundaries instead of treating one huge CDC firehose as a single application surface. The example
  uses one table and one row predicate on purpose: it shows the ownership boundary, not a claim
  that every downstream use case should share one connector-sized dataset.

The example also intentionally stops direct Kafka consumption at Debezium ingress:

- Projections and workflows consume Terracedb tables, not Kafka topics, because the Terracedb
  tables already encode the durable contract the rest of the example cares about: decoded Debezium
  rows, table and row filtering, retained replayable history, projection frontiers, and typed read
  surfaces.
- Reading Kafka directly from every downstream component would duplicate bootstrap, decode, filter,
  replay, and recovery logic in multiple places. Using Terracedb tables keeps rebuilds, restart
  tests, and checkpoint restore centered on one durable boundary instead of reimplementing Kafka
  consumer state in each projection or workflow.

Both workflow modes now run through the real workflow runtime from the same append-only
`attention_transitions` stream. The example intentionally uses the newer ergonomics added while
freezing this boundary:

- `ensure_layout_tables(...)` and `Db::ensure_table(...)` make the example bootstrap idempotent
  instead of forcing one-shot setup code.
- `DebeziumMaterializer::from_layouts(...)` removes repetitive materializer wiring.
- `PostgresDebeziumDecoder::from_layouts(...)` and `DebeziumIngressHandler::postgres(...)` let the
  ingress path come from layouts instead of hand-built table collections.
- `terracedb_kafka::RskafkaBroker` is now the real Kafka adapter used by the example binary instead
  of example-local broker glue.
- `DebeziumDerivedTransitionProjection` derives append-only workflow routing rows directly from the
  replayable Debezium event log, and `into_multi_source(...)` turns that into a ready-to-run
  projection with the intended recompute behavior.
- `DebeziumSnapshotMarker::{phase,boundary}` and `DebeziumEvent::{snapshot_phase,
  snapshot_boundary}` expose semantic snapshot state instead of pushing callers toward raw marker
  strings.
- `DebeziumMirrorChange::decode(...)` gives mirror consumers one typed surface for upserts and
  deletes.
- `WorkflowSourceConfig::{historical_replayable_source, live_only_current_state_source,
  live_only_replayable_append_only_source}` captures the intended workflow attach presets, and
  `prepare_source_table_config(...)` makes sure replayable sources get the retained history those
  presets need at the table layer.
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
