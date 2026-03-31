"use strict";

class ReadonlyViewModel {
  constructor() {
    this.bridge = null;
    this.sessions = [];
    this.handles = new Map();
  }

  setBridge(bridge) {
    this.bridge = bridge;
    this.sessions = [];
    this.handles.clear();
  }

  clearBridge() {
    this.setBridge(null);
  }

  async refreshSessions() {
    this.sessions = this.bridge ? await this.bridge.refreshSessions() : [];
    return this.sessions;
  }

  async reconnect() {
    if (!this.bridge) {
      return [];
    }
    await this.bridge.reconnect();
    const cached = [...this.handles.values()];
    this.handles.clear();
    for (const handle of cached) {
      const reopened = await this.bridge.reconnectView({
        sessionVolumeId: handle.location.session_volume_id,
        cut: handle.location.cut,
        path: handle.location.path,
        label: handle.label
      });
      const key = `${reopened.location.session_volume_id}:${reopened.location.cut}:${reopened.location.path}`;
      this.handles.set(key, reopened);
    }
    return this.refreshSessions();
  }

  async ensureHandle({ sessionVolumeId, cut, path, label }) {
    const key = `${sessionVolumeId}:${cut}:${path}`;
    if (this.handles.has(key)) {
      return this.handles.get(key);
    }
    if (!this.bridge) {
      throw new Error("Readonly view bridge is not connected");
    }
    const handle = await this.bridge.openView({
      sessionVolumeId,
      cut,
      path,
      label
    });
    this.handles.set(key, handle);
    return handle;
  }
}

module.exports = {
  ReadonlyViewModel
};
