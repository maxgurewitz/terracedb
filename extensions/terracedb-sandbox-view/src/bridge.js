"use strict";

const { normalizePath } = require("./protocol");

function expectKind(response, kind) {
  if (!response || response.kind !== kind) {
    throw new Error(`Expected response kind "${kind}" but received "${response && response.kind}"`);
  }
  return response;
}

function createProtocolBridge(transport) {
  return {
    async listSessions() {
      return expectKind(await transport.send({ kind: "list_sessions" }), "sessions").sessions;
    },

    async refreshSessions() {
      if (typeof transport.refresh === "function") {
        await transport.refresh();
      }
      return expectKind(await transport.send({ kind: "refresh_sessions" }), "sessions").sessions;
    },

    async listHandles(sessionVolumeId) {
      return expectKind(
        await transport.send({
          kind: "list_handles",
          session_volume_id: sessionVolumeId ?? null
        }),
        "handles"
      ).handles;
    },

    async openView({ sessionVolumeId, cut, path, label }) {
      return expectKind(
        await transport.send({
          kind: "open_view",
          session_volume_id: sessionVolumeId,
          request: {
            cut,
            path: normalizePath(path),
            label: label ?? null
          }
        }),
        "view"
      ).handle;
    },

    async reconnectView({ sessionVolumeId, cut, path, label }) {
      return expectKind(
        await transport.send({
          kind: "reconnect_view",
          request: {
            session_volume_id: sessionVolumeId,
            cut,
            path: normalizePath(path),
            label: label ?? null
          }
        }),
        "view"
      ).handle;
    },

    async closeView({ sessionVolumeId, handleId }) {
      await expectKind(
        await transport.send({
          kind: "close_view",
          session_volume_id: sessionVolumeId,
          handle_id: handleId
        }),
        "ack"
      );
    },

    async stat(location) {
      return expectKind(
        await transport.send({
          kind: "stat",
          location
        }),
        "stat"
      ).stat;
    },

    async readFile(location) {
      return expectKind(
        await transport.send({
          kind: "read_file",
          location
        }),
        "file"
      ).bytes;
    },

    async readDirectory(location) {
      return expectKind(
        await transport.send({
          kind: "read_dir",
          location
        }),
        "directory"
      ).entries;
    },

    async reconnect() {
      if (typeof transport.reconnect === "function") {
        await transport.reconnect();
      }
      return this.listSessions();
    }
  };
}

function createLocalBridge(handler) {
  if (!handler || typeof handler.send !== "function") {
    throw new Error("Local bridge requires a send(request) handler");
  }
  return createProtocolBridge({
    send: (request) => handler.send(request),
    refresh: handler.refresh ? () => handler.refresh() : undefined,
    reconnect: handler.reconnect ? () => handler.reconnect() : undefined
  });
}

function createHttpBridge({ endpoint, token, fetchImpl }) {
  const fetchFn = fetchImpl || globalThis.fetch;
  if (typeof fetchFn !== "function") {
    throw new Error("HTTP bridge requires a fetch implementation");
  }
  if (!endpoint) {
    throw new Error("HTTP bridge requires an endpoint");
  }
  return createProtocolBridge({
    async send(request) {
      const headers = {
        "content-type": "application/json"
      };
      if (token) {
        headers.authorization = `Bearer ${token}`;
      }
      const response = await fetchFn(endpoint, {
        method: "POST",
        headers,
        body: JSON.stringify(request)
      });
      if (!response.ok) {
        throw new Error(`Readonly view bridge request failed: ${response.status}`);
      }
      return response.json();
    }
  });
}

module.exports = {
  createHttpBridge,
  createLocalBridge
};
