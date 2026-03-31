# Order Watch

`order-watch` freezes the end-to-end CDC example boundary used by T96.

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

The tests for this crate are the contract for the boundary: they prove the Kafka ingress offsets, Debezium materializations, projection frontier, workflow attach mode, and user-visible outputs can all be checked from one deterministic seeded run.

For T96 specifically, the live-only profile is exercised through the workflow runtime directly, while the historical profile is frozen at the oracle layer from the append-only `attention_transitions` stream so later work can wire the final runtime choice without changing the scenario contract.
