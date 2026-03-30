# terracedb-relays

`terracedb-relays` packages reusable relay patterns built on top of Terracedb's transactional outbox.

This crate is for the application-side step that sits between a durable outbox and local state changes:

- read a batch of outbox rows
- decode them into typed messages
- apply the whole batch inside one transaction
- delete the outbox rows in that same transaction

That pattern is useful when a workflow, API handler, or another local writer emits durable commands into an outbox and a second in-process component needs to materialize local state from them.

The crate intentionally sits above `terracedb::composition` and beside `terracedb-workflows`:

- `terracedb::composition` provides the low-level outbox and timer primitives
- `terracedb-workflows` provides durable workflow execution
- `terracedb-relays` provides reusable local relay loops that consume outbox batches safely and efficiently

The TODO example uses this crate for its weekly placeholder planner relay.
