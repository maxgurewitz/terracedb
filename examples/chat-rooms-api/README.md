# Chat Rooms API Example

This example is a small sharded Terracedb application.

It keeps the workload intentionally narrow:

- each room is an independent entity,
- every durable write is scoped to one room,
- room state and the room-local message log share the same shard key, and
- resharding moves one room's virtual partition without changing the room ID or rewriting keys.

The data model uses two sharded tables:

- `room_state`
- `room_messages`

Both tables are keyed by the same explicit room shard key. In this demo, `room_messages` stores one room-local message log row per room so the application can update state and recent messages in a single-shard transaction with the same key.

## Run It

Default profile:

```bash
cargo run -p terracedb-example-chat-rooms-api
```

Shard-local execution profile:

```bash
CHAT_ROOMS_API_PROFILE=shard_local cargo run -p terracedb-example-chat-rooms-api
```

Useful environment variables:

- `CHAT_ROOMS_API_BIND_ADDR`
- `CHAT_ROOMS_API_DATA_DIR`
- `CHAT_ROOMS_API_PROFILE`

The default bind address is `127.0.0.1:9604`.

## Endpoints

Create a room:

```bash
curl -X POST http://127.0.0.1:9604/rooms \
  -H 'content-type: application/json' \
  -d '{
    "room_id": "lobby",
    "title": "Lobby"
  }'
```

Post one room-local message:

```bash
curl -X POST http://127.0.0.1:9604/rooms/lobby/messages \
  -H 'content-type: application/json' \
  -d '{
    "author": "max",
    "body": "hello from one shard"
  }'
```

Read recent messages for one room:

```bash
curl "http://127.0.0.1:9604/rooms/lobby/messages?limit=10"
```

Inspect where one room currently lives:

```bash
curl http://127.0.0.1:9604/rooms/lobby/shard
```

Move one room's virtual partition to another physical shard:

```bash
curl -X POST http://127.0.0.1:9604/rooms/lobby/reshard \
  -H 'content-type: application/json' \
  -d '{
    "target_physical_shard": 2
  }'
```

Inspect shard mapping and backlog observability:

```bash
curl http://127.0.0.1:9604/observability
```

## What This Example Teaches

Room-scoped batches are valid because both durable rows in the write path use the same explicit room shard key. A `POST /rooms/:room_id/messages` request updates `room_state` and `room_messages` together, but both rows route to the same virtual partition and physical shard.

Cross-room batches are intentionally not the teaching path. The example does not expose a multi-room write API because the whole point is to make the single-entity shard boundary obvious.

Unsharded tables would behave differently. They would continue to live on shard `0000`, would not need room-local routing helpers, and would not need reshard plans for individual entities.

Execution domains and shard maps are related but separate:

- shard maps define which physical shard logically owns a room,
- execution domains define where shard-local work and backlog are placed or isolated,
- changing the execution profile must not change room contents, and
- resharding changes ownership and placement without changing logical answers.

## Profiles

`database_shared` is the default.

It keeps shard ownership visible in the shard map, but publishes backlog on the database-wide background lane. That shows the conservative compatibility path, including shard `0000`.

`shard_local` registers shard-local execution domains for physical shards `0001+`.

That changes where backlog is published and isolated, but it does not change room state, recent messages, or reshard correctness.
