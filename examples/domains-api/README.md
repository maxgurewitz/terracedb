# Domains API Example

This example shows why execution domains exist without requiring a large application first.

It opens two Terracedb databases in one process:

- `primary`: a latency-sensitive application database
- `analytics`: a lower-priority helper workload

The example also keeps each database's control-plane work on a reserved control domain so metadata operations can keep moving while user and helper lanes are busy.

## What It Demonstrates

- explicit colocated deployment with one shared resource manager
- conservative default placement using shared weighted user domains
- protected control-plane domains for metadata work
- observability output for live topology, effective budgets, backlog, and pressure
- the key rule for users: domain placement changes isolation and contention behavior, not logical correctness

## Default Profile

The default `conservative` profile uses `ColocatedDeployment::primary_with_analytics(...)`.

That means:

- primary foreground work gets a heavier shared weight than primary background work
- analytics stays shared and lower priority
- control-plane domains are reserved and dedicated

There is also a `balanced` profile that keeps the same logical application behavior but uses simpler `1:1` user-lane weights. The tests compare the two profiles to show that the logical answers stay the same while admission and backlog behavior change.

## API Surface

- `POST /primary/items`
  Creates a foreground write in the primary database.
- `GET /primary/items`
  Reads the primary database through the foreground lane.
- `POST /primary/maintenance`
  Triggers background-oriented maintenance pressure for the primary DB and can optionally flush first.
- `POST /helper/load`
  Writes helper rows into the analytics DB and can hold foreground/background pressure there.
- `GET /helper/reports`
  Reads back helper-side durable state.
- `POST /control/ensure-table`
  Runs a metadata operation through the control-plane path.
- `POST /domains/admission`
  Probes admission for a lane so you can see how the current topology affects effective budgets.
- `GET /domains/report`
  Dumps the placement report plus the live resource-manager snapshot.

## Domain Mapping

- primary reads and writes run in `primary.foreground`
- primary maintenance pressure runs in `primary.background`
- helper batch writes and helper pressure run in `analytics.foreground` and `analytics.background`
- metadata paths such as `create_table` run in the per-database control-plane domain

The control-plane domain is different from foreground/background lanes because it is reserved for metadata and recovery-oriented work. It is there to protect progress, not to create a new correctness model.

## Running It

```bash
cargo run -p terracedb-example-domains-api
```

Optional environment variables:

- `DOMAINS_API_BIND_ADDR`
- `DOMAINS_API_DATA_DIR`
- `DOMAINS_API_PROFILE`

## What To Look At

Start with `GET /domains/report`.

That output shows:

- which domain each lane is bound to
- whether a domain is shared or dedicated
- each lane's effective budget under the current load
- the reserved control-plane metadata that marks the protected path
