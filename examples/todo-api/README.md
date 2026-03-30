# TODO API Example

This example app is a small durable TODO service exposed through a basic HTTP API.

It is intentionally simple, but it exercises the full Terracedb stack:

- `terracedb` stores the durable application state
- `terracedb-projections` maintains a read model of recently changed TODOs
- `terracedb-workflows` drives a weekly recurring background task that creates placeholder TODOs
- `terracedb-simulation` tests the system end to end from client to server to embedded database

The goal is to provide a first example that is easy to understand without giving up the parts that make Terracedb interesting.

## What the app does

The app manages TODO items through a small API.

Users can:

- create a TODO
- update a TODO
- complete a TODO
- list TODOs
- view the 10 most recently created or modified TODOs

In the background, a weekly workflow creates placeholder TODOs for each day of the upcoming week.

Those placeholder TODOs give the user a lightweight weekly planning scaffold, such as one task slot per day that can later be renamed, completed, or replaced.

## Core data model

The app keeps the data model small and explicit.

The main durable application records are:

- `todos`
- `recent_todos`

The `todos` table stores one durable record per TODO item.

Each TODO record includes:

- `todo_id`
- `title`
- `notes`
- `status`
- `scheduled_for_date`
- `placeholder`
- `created_at`
- `updated_at`

The `recent_todos` table stores the projection-backed recent view that powers the recent endpoint.

Workflow runtime state, recurring schedule state, timers, inbox entries, and outbox entries use the workflow library's internal tables.

Every durable state transition in the app flows through Terracedb.

## Projection: recent TODOs

The app maintains a projection of the 10 most recently created or modified TODOs.

This projection:

- subscribe to changes from the source TODO table
- order entries by last mutation time or sequence
- materialize the newest 10 items into fixed slots in the projection table
- serve as the fast read model for a `recent TODOs` API endpoint

This projection intentionally recomputes the recent-10 view from the source table at the projection frontier and rewrites the 10 projection slots deterministically. That keeps the example simple and makes replay behavior easy to reason about.

It demonstrates the core projection pattern:

- write durable source state first
- derive a query-friendly read model asynchronously
- keep the read model replay-safe and deterministic

Important invariant:

- after replay or restart, the projection must converge to the same 10 TODOs in the same deterministic order

## Workflow: weekly placeholder creation

The app includes a weekly planner workflow that fires at the configured start of each week and creates placeholder TODOs for the upcoming week.

It uses the recurring helper from `terracedb-workflows`, so the application code only defines what happens on each weekly tick. The helper owns bootstrap admission, stable timer identity, next-fire calculation, and durable recurring state.

The workflow:

- wake up on a weekly schedule
- determine the seven upcoming calendar days for the next week window
- emit one durable planner command per day for the upcoming week
- avoid duplicate placeholder creation if the workflow is retried or replayed

Those commands are consumed by an internal durable relay in the application process. The relay writes placeholder TODO records into the `todos` table and deletes delivered outbox rows in the same transaction.

The placeholder TODO identifiers are deterministic, so retries and replay remain idempotent.

Example behavior:

- if the upcoming week is missing entries for Tuesday and Thursday, only those placeholder TODOs are created
- if the process crashes after creating some of them, recovery should resume safely without double-creating the others

This makes the workflow example useful without making it too complex:

- it uses timers or scheduled triggers
- it performs durable background work
- it needs idempotency
- it has obvious crash/retry edge cases

## Basic API shape

The API is intentionally small.

The app exposes these endpoints:

- `POST /todos`
- `GET /todos`
- `GET /todos/:id`
- `PATCH /todos/:id`
- `POST /todos/:id/complete`
- `GET /todos/recent`

The important split is:

- source-of-truth reads can come from the main TODO table
- `GET /todos/recent` should come from the projection

That keeps the example honest about the difference between durable write state and derived read state.

## End-to-end simulation testing

This example is specifically meant to demonstrate deterministic end-to-end simulation.

The simulated system should include:

- a client host that sends API requests
- a server host that runs the TODO API
- an embedded Terracedb instance inside the server process
- the projection runtime
- the workflow runtime

The simulation suite should not focus only on faults. It should first prove the intended application behavior on the happy path.

Core happy-path scenarios to simulate end to end:

- create a TODO through the API
- read back an individual TODO through the API
- update an individual TODO through the API
- list TODOs through the API
- read the recent-TODO projection through the API

The simulation suite should cover scenarios like:

- a client request creates a TODO and receives a success response
- a client can read back the TODO it just created
- a client can update a TODO and then read the updated value
- a client can list TODOs and see the expected durable state
- the recent-TODO projection reflects newly created and updated TODOs
- a client request times out and retries while the first request may already have committed
- the server crashes after durable admission but before replying to the client
- the recent-TODO projection catches up correctly after restart
- simulated time advances to the beginning of the week and the workflow creates placeholder TODOs for the upcoming week
- simulated time advances further and the workflow does not create duplicates for the same week window
- the weekly workflow partially runs, crashes, restarts, and still creates exactly one placeholder per day
- network delay or partition occurs between client and server without violating durable correctness

Important invariants to check:

- no TODO is lost after a committed write
- individual TODO reads reflect the last durable write
- TODO list results reflect the durable table contents
- retries do not create unintended duplicates
- the recent-TODO projection always contains at most 10 items
- the recent-TODO projection is consistent with the durable TODO history
- weekly placeholder creation is idempotent across retries and recovery
- advancing simulated time is sufficient to trigger weekly placeholder creation without relying on wall-clock time

## Why this is a good first example

This app is intentionally modest, but it shows nearly every important Terracedb idea in a form that is easy to explain:

- embedded durable application state
- derived read models
- durable scheduled background work
- deterministic simulation of real application behavior

It should make a good foundation for later examples that are more ambitious, such as:

- a research orchestrator
- an inference job system
- a support ticket workflow app
- an order fulfillment system

## Initial implementation direction

The implementation of this example consists of:

- a small API surface
- one source table for TODOs
- one projection for recent changes
- one weekly workflow plus one internal planner dispatcher
- end-to-end simulation tests for the normal API behavior
- end-to-end simulation tests for retry and crash behavior
- an end-to-end time-travel simulation test for weekly placeholder creation

The point of the example is not feature breadth.

The point is to show the smallest believable application that uses Terracedb, projections, workflows, and deterministic client-to-server simulation together.
