# Domains API Example

This example now focuses on T75-style pressure-aware flushing and write admission, not just domain placement.

It opens two Terracedb databases in one process:

- `primary`: a latency-sensitive application database with a bursty writer
- `analytics`: a lower-priority helper workload

Both databases still get isolated control-plane domains for metadata and recovery work, but the interesting part is the live pressure report: it shows real engine pressure and admission state, not only synthetic example counters.

## What It Demonstrates

- colocated foreground, background, and control-plane execution domains
- a bursty primary writer that can build dirty-byte and unified-log pressure
- a lower-priority helper workload that can consume foreground/background resources
- slower background flush capacity through tighter background budgets and held helper pressure
- recovery after flush progress and pressure release
- the important contract: these controls change latency, backlog, and resource isolation, not logical correctness

## Profiles

- `conservative`
  Gives the primary workload more headroom before admission tightens.
- `primary_protected`
  Gives the primary foreground more weight and smaller pressure budgets so rate limiting and stalls happen sooner under pressure.

The tests compare the two profiles and show that they return the same logical rows while pressure, throttling, and recovery behavior differ.

## API Surface

- `POST /primary/items`
  Writes one primary record through the foreground path.
- `POST /primary/burst`
  Writes a burst of primary records with padded payloads so pressure becomes visible quickly.
- `GET /primary/items`
  Reads the primary database through the foreground lane.
- `POST /primary/maintenance`
  Adds primary background pressure and can trigger an explicit flush.
- `POST /primary/maintenance/release`
  Releases example-held primary background pressure.
- `POST /helper/load`
  Runs a helper-side batch write and can hold helper foreground/background pressure.
- `GET /helper/reports`
  Reads helper-side durable state.
- `POST /helper/release`
  Releases example-held helper pressure.
- `POST /control/ensure-table`
  Runs a metadata operation through the reserved control-plane path.
- `POST /domains/admission`
  Probes resource-manager admission for a specific lane.
- `GET /domains/report`
  Returns topology plus live pressure-aware write-admission state.

## What `/domains/report` Shows

The report keeps the full `deployment` placement output and adds two workload-focused sections:

- `primary_writer`
- `helper_writer`

Each section includes:

- `current_pressure`
  Real Terracedb `PressureStats` for that write path, including:
  - `local.mutable_dirty_bytes`
  - `local.immutable_queued_bytes`
  - `local.immutable_flushing_bytes`
  - `local.unified_log_pinned_bytes`
- `next_write_admission`
  Real admission diagnostics for the next sample write on that path
- `throttle_active` / `stall_active`
  Whether the current pressure state would rate-limit or stall the next write
- `throttled_write_events`
  How many throttled writes the scheduler has already recorded
- `last_non_open_write_admission`
  The most recent non-open scheduler-side diagnostics for that foreground domain,
  including when it was recorded

The top-level `deployment` report still shows the configured domain/resource budget layout through each lane's `snapshot.spec.budget`, `snapshot.spec.placement`, and domain metadata.

## Running It

```bash
cargo run -p terracedb-example-domains-api
```

Optional environment variables:

- `DOMAINS_API_BIND_ADDR`
- `DOMAINS_API_DATA_DIR`
- `DOMAINS_API_PROFILE`

Example:

```bash
DOMAINS_API_PROFILE=primary_protected cargo run -p terracedb-example-domains-api
```

## Suggested Walkthrough

Start from the default report:

```bash
curl http://127.0.0.1:9603/domains/report
```

Create a bursty primary write:

```bash
curl -X POST http://127.0.0.1:9603/primary/burst \
  -H 'content-type: application/json' \
  -d '{"batch_id":"burst-a","item_count":4,"title_bytes":1024}'
```

Then check the report again. You should see the primary writer's real pressure fields move:

- `mutable_dirty_bytes` rises after the burst
- `unified_log_pinned_bytes` rises with it
- `next_write_admission.level` can move from `open` to `rate_limit` or `stall`

Add lower-priority helper pressure:

```bash
curl -X POST http://127.0.0.1:9603/helper/load \
  -H 'content-type: application/json' \
  -d '{
    "batch_id":"helper-a",
    "report_count":3,
    "hold_foreground_cpu_workers":2,
    "hold_background_tasks":1,
    "background_in_flight_bytes":512,
    "queued_work_items":3,
    "queued_bytes":1024,
    "flush_after_write":false
  }'
```

Now the report should show helper-side dirty bytes plus the held helper resource pressure, while the primary/control-plane paths still stay logically correct.

Trigger explicit primary maintenance and recovery:

```bash
curl -X POST http://127.0.0.1:9603/primary/maintenance \
  -H 'content-type: application/json' \
  -d '{
    "flush_now":true,
    "hold_background_tasks":1,
    "background_in_flight_bytes":512,
    "queued_work_items":2,
    "queued_bytes":1024
  }'
```

Release the example-held background/helper pressure:

```bash
curl -X POST http://127.0.0.1:9603/helper/release
curl -X POST http://127.0.0.1:9603/primary/maintenance/release
curl http://127.0.0.1:9603/domains/report
```

After recovery, the writer report should show dirty, queued, flushing, and unified-log pressure falling back down, and `throttle_active` / `stall_active` returning to `false`.

## Why This Is Safe

Changing execution domains, weights, and pressure budgets does not change Terracedb's logical answers.

What it changes is operational behavior:

- when writes start to slow down
- when background work can build backlog
- how aggressively the primary path protects its latency
- how much helper work can interfere before admission pushes back

The example tests check that `conservative` and `primary_protected` return the same logical primary/helper rows even though their pressure and admission behavior diverge.
