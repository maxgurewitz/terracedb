"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const test = require("node:test");

const { createHttpBridge, createLocalBridge } = require("../src/bridge");
const { ReadonlyViewModel } = require("../src/model");
const { parseReadonlyUri, toReadonlyUri } = require("../src/protocol");

function sampleSession() {
  return {
    session_volume_id: "00000000000000000000000000009301",
    workspace_root: "/workspace",
    state: "open",
    label: "9301 /workspace",
    provider: "deterministic-view",
    active_view_handles: 0,
    available_cuts: ["visible", "durable"]
  };
}

function sampleHandle() {
  return {
    handle_id: "handle-1",
    provider: "deterministic-view",
    label: "workspace visible",
    location: {
      session_volume_id: sampleSession().session_volume_id,
      cut: "visible",
      path: "/workspace"
    },
    uri: toReadonlyUri({
      session_volume_id: sampleSession().session_volume_id,
      cut: "visible",
      path: "/workspace"
    }),
    opened_at: 42,
    metadata: {}
  };
}

test("local bridge speaks the readonly view protocol and model reconnects cached handles", async () => {
  const requests = [];
  const handler = {
    async send(request) {
      requests.push(request);
      switch (request.kind) {
        case "list_sessions":
        case "refresh_sessions":
          return { kind: "sessions", sessions: [sampleSession()] };
        case "open_view":
        case "reconnect_view":
          return { kind: "view", handle: sampleHandle() };
        case "stat":
          return {
            kind: "stat",
            stat: {
              location: request.location,
              uri: toReadonlyUri(request.location),
              kind: "file",
              size: 5,
              modified_at: 100
            }
          };
        case "read_file":
          return { kind: "file", bytes: [104, 101, 108, 108, 111] };
        case "read_dir":
          return {
            kind: "directory",
            entries: [
              {
                name: "file.txt",
                kind: "file",
                location: {
                  session_volume_id: sampleSession().session_volume_id,
                  cut: "visible",
                  path: "/workspace/file.txt"
                },
                uri: toReadonlyUri({
                  session_volume_id: sampleSession().session_volume_id,
                  cut: "visible",
                  path: "/workspace/file.txt"
                })
              }
            ]
          };
        case "list_handles":
          return { kind: "handles", handles: [sampleHandle()] };
        case "close_view":
          return { kind: "ack" };
        default:
          throw new Error(`Unhandled request ${request.kind}`);
      }
    },
    async reconnect() {}
  };

  const bridge = createLocalBridge(handler);
  assert.equal((await bridge.listSessions()).length, 1);
  const handle = await bridge.openView({
    sessionVolumeId: sampleSession().session_volume_id,
    cut: "visible",
    path: "/workspace",
    label: "workspace"
  });
  assert.equal(handle.location.path, "/workspace");
  assert.deepEqual(await bridge.readFile(parseReadonlyUri(handle.uri)), [104, 101, 108, 108, 111]);
  assert.equal((await bridge.readDirectory(handle.location))[0].name, "file.txt");

  const model = new ReadonlyViewModel();
  model.setBridge(bridge);
  await model.refreshSessions();
  await model.ensureHandle({
    sessionVolumeId: sampleSession().session_volume_id,
    cut: "visible",
    path: "/workspace",
    label: "workspace"
  });
  await model.reconnect();

  assert(requests.some((request) => request.kind === "reconnect_view"));
});

test("http bridge uses bearer auth for remote endpoints", async () => {
  const server = http.createServer(async (req, res) => {
    if (req.headers.authorization !== "Bearer secret-token") {
      res.writeHead(401, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }
    const body = await new Promise((resolve, reject) => {
      let raw = "";
      req.setEncoding("utf8");
      req.on("data", (chunk) => {
        raw += chunk;
      });
      req.on("end", () => resolve(raw));
      req.on("error", reject);
    });
    const request = JSON.parse(body);
    if (request.kind === "list_sessions" || request.kind === "refresh_sessions") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ kind: "sessions", sessions: [sampleSession()] }));
      return;
    }
    if (request.kind === "read_file") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ kind: "file", bytes: [79, 75] }));
      return;
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ kind: "ack" }));
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  const endpoint = `http://127.0.0.1:${address.port}/readonly-view`;

  const bridge = createHttpBridge({
    endpoint,
    token: "secret-token",
    fetchImpl: globalThis.fetch
  });

  assert.equal((await bridge.listSessions())[0].workspace_root, "/workspace");
  assert.deepEqual(
    await bridge.readFile({
      session_volume_id: sampleSession().session_volume_id,
      cut: "visible",
      path: "/workspace/ok.txt"
    }),
    [79, 75]
  );

  await new Promise((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
});
