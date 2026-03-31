# Terrace Sandbox View

`terracedb-sandbox-view` is a real VS Code/Cursor extension package for browsing TerraceDB sandbox sessions as a read-only filesystem.

## What it does

- lists sandbox sessions exposed by a host application,
- shows both `visible` and `durable` cuts,
- opens files through a read-only `terrace-view:` filesystem provider,
- supports both a local loopback bridge and an authenticated remote endpoint,
- reconnects by replaying cached view handles.

## Bridge protocol

The extension speaks the JSON protocol exported by `terracedb-sandbox`:

- `list_sessions`
- `refresh_sessions`
- `list_handles`
- `open_view`
- `reconnect_view`
- `close_view`
- `stat`
- `read_file`
- `read_dir`

The expected transport is a JSON POST endpoint that accepts one request object and returns one response object. A local app can expose the endpoint on loopback. A remote deployment can expose the same route behind authentication.

## Commands

- `Terrace Sandbox: Connect Local Bridge`
- `Terrace Sandbox: Connect Remote Endpoint`
- `Terrace Sandbox: Refresh`
- `Terrace Sandbox: Reconnect`
- `Terrace Sandbox: Disconnect`

## Cursor compatibility

Cursor inherits VS Code's extension host and virtual filesystem APIs, so this package is intentionally written against the standard VS Code extension surface without Cursor-specific forks.
