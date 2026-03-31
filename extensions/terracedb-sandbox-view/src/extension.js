"use strict";

const vscode = require("vscode");

const { createHttpBridge } = require("./bridge");
const { ReadonlyViewModel } = require("./model");
const { parseReadonlyUri } = require("./protocol");

const CONNECTION_STATE_KEY = "terracedbSandbox.connection";

class SessionNode {
  constructor(session) {
    this.kind = "session";
    this.session = session;
  }
}

class CutNode {
  constructor(session, cut) {
    this.kind = "cut";
    this.session = session;
    this.cut = cut;
  }
}

class EntryNode {
  constructor(entry) {
    this.kind = "entry";
    this.entry = entry;
  }
}

class PlaceholderNode {
  constructor(label, command) {
    this.kind = "placeholder";
    this.label = label;
    this.command = command;
  }
}

class TerraceReadonlyFileSystemProvider {
  constructor(model) {
    this.model = model;
    this._emitter = new vscode.EventEmitter();
    this.onDidChangeFile = this._emitter.event;
  }

  fireChanged() {
    this._emitter.fire([]);
  }

  watch() {
    return new vscode.Disposable(() => {});
  }

  async stat(uri) {
    const location = parseReadonlyUri(uri.toString());
    const stat = await this.model.bridge.stat(location);
    if (!stat) {
      throw vscode.FileSystemError.FileNotFound(uri);
    }
    return {
      type: toFileType(stat.kind),
      ctime: stat.modified_at,
      mtime: stat.modified_at,
      size: stat.size
    };
  }

  async readDirectory(uri) {
    const location = parseReadonlyUri(uri.toString());
    const entries = await this.model.bridge.readDirectory(location);
    return entries.map((entry) => [entry.name, toFileType(entry.kind)]);
  }

  async readFile(uri) {
    const location = parseReadonlyUri(uri.toString());
    const bytes = await this.model.bridge.readFile(location);
    if (!bytes) {
      throw vscode.FileSystemError.FileNotFound(uri);
    }
    return Uint8Array.from(bytes);
  }

  createDirectory() {
    throw vscode.FileSystemError.NoPermissions("Terrace sandbox views are read-only");
  }

  writeFile() {
    throw vscode.FileSystemError.NoPermissions("Terrace sandbox views are read-only");
  }

  delete() {
    throw vscode.FileSystemError.NoPermissions("Terrace sandbox views are read-only");
  }

  rename() {
    throw vscode.FileSystemError.NoPermissions("Terrace sandbox views are read-only");
  }
}

class TerraceSandboxTreeProvider {
  constructor(model) {
    this.model = model;
    this._emitter = new vscode.EventEmitter();
    this.onDidChangeTreeData = this._emitter.event;
  }

  refresh() {
    this._emitter.fire(undefined);
  }

  async getChildren(element) {
    if (!this.model.bridge) {
      return [
        new PlaceholderNode(
          "Connect to a local or remote Terrace sandbox bridge",
          "terracedbSandbox.connectLocal"
        )
      ];
    }

    if (!element) {
      const sessions = await this.model.refreshSessions();
      return sessions.map((session) => new SessionNode(session));
    }

    if (element.kind === "session") {
      return [
        new CutNode(element.session, "visible"),
        new CutNode(element.session, "durable")
      ];
    }

    if (element.kind === "cut") {
      const handle = await this.model.ensureHandle({
        sessionVolumeId: element.session.session_volume_id,
        cut: element.cut,
        path: element.session.workspace_root,
        label: `${element.session.label} ${element.cut}`
      });
      const entries = await this.model.bridge.readDirectory(handle.location);
      return entries.map((entry) => new EntryNode(entry));
    }

    if (element.kind === "entry" && element.entry.kind === "directory") {
      const entries = await this.model.bridge.readDirectory(element.entry.location);
      return entries.map((entry) => new EntryNode(entry));
    }

    return [];
  }

  getTreeItem(element) {
    if (element.kind === "placeholder") {
      const item = new vscode.TreeItem(element.label, vscode.TreeItemCollapsibleState.None);
      item.command = {
        command: element.command,
        title: element.label
      };
      item.contextValue = "placeholder";
      return item;
    }

    if (element.kind === "session") {
      const item = new vscode.TreeItem(
        element.session.label,
        vscode.TreeItemCollapsibleState.Collapsed
      );
      item.description = element.session.state;
      item.tooltip = `${element.session.workspace_root} via ${element.session.provider}`;
      item.contextValue = "session";
      return item;
    }

    if (element.kind === "cut") {
      const item = new vscode.TreeItem(
        element.cut,
        vscode.TreeItemCollapsibleState.Collapsed
      );
      item.description = element.session.workspace_root;
      item.contextValue = "cut";
      return item;
    }

    const resourceUri = vscode.Uri.parse(element.entry.uri);
    const collapsibleState =
      element.entry.kind === "directory"
        ? vscode.TreeItemCollapsibleState.Collapsed
        : vscode.TreeItemCollapsibleState.None;
    const item = new vscode.TreeItem(resourceUri, collapsibleState);
    item.contextValue = element.entry.kind;
    item.resourceUri = resourceUri;
    if (element.entry.kind !== "directory") {
      item.command = {
        command: "vscode.open",
        title: "Open Terrace Sandbox File",
        arguments: [resourceUri]
      };
    }
    return item;
  }
}

async function promptConnection(kind, previous) {
  const endpoint = await vscode.window.showInputBox({
    prompt:
      kind === "local"
        ? "Loopback readonly-view endpoint URL"
        : "Remote readonly-view endpoint URL",
    value: previous && previous.endpoint ? previous.endpoint : "http://127.0.0.1:6040/readonly-view",
    ignoreFocusOut: true
  });
  if (!endpoint) {
    return null;
  }

  if (kind === "remote") {
    const token = await vscode.window.showInputBox({
      prompt: "Remote bearer token",
      password: true,
      value: previous && previous.token ? previous.token : "",
      ignoreFocusOut: true
    });
    if (!token) {
      return null;
    }
    return { kind, endpoint, token };
  }

  return { kind, endpoint, token: null };
}

async function installConnection(context, model, treeProvider, fileSystemProvider, connection) {
  model.setBridge(
    createHttpBridge({
      endpoint: connection.endpoint,
      token: connection.kind === "remote" ? connection.token : null
    })
  );
  await context.globalState.update(CONNECTION_STATE_KEY, connection);
  await model.refreshSessions();
  treeProvider.refresh();
  fileSystemProvider.fireChanged();
}

function toFileType(kind) {
  switch (kind) {
    case "directory":
      return vscode.FileType.Directory;
    case "symlink":
      return vscode.FileType.SymbolicLink;
    case "file":
    default:
      return vscode.FileType.File;
  }
}

function activate(context) {
  const model = new ReadonlyViewModel();
  const treeProvider = new TerraceSandboxTreeProvider(model);
  const fileSystemProvider = new TerraceReadonlyFileSystemProvider(model);

  context.subscriptions.push(
    vscode.workspace.registerFileSystemProvider("terrace-view", fileSystemProvider, {
      isReadonly: true,
      isCaseSensitive: true
    })
  );
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("terracedbSandboxExplorer", treeProvider)
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("terracedbSandbox.connectLocal", async () => {
      const previous = context.globalState.get(CONNECTION_STATE_KEY);
      const connection = await promptConnection("local", previous);
      if (!connection) {
        return;
      }
      await installConnection(context, model, treeProvider, fileSystemProvider, connection);
      vscode.window.showInformationMessage("Connected to local Terrace sandbox bridge.");
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("terracedbSandbox.connectRemote", async () => {
      const previous = context.globalState.get(CONNECTION_STATE_KEY);
      const connection = await promptConnection("remote", previous);
      if (!connection) {
        return;
      }
      await installConnection(context, model, treeProvider, fileSystemProvider, connection);
      vscode.window.showInformationMessage("Connected to remote Terrace sandbox endpoint.");
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("terracedbSandbox.refresh", async () => {
      await model.refreshSessions();
      treeProvider.refresh();
      fileSystemProvider.fireChanged();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("terracedbSandbox.reconnect", async () => {
      await model.reconnect();
      treeProvider.refresh();
      fileSystemProvider.fireChanged();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("terracedbSandbox.disconnect", async () => {
      model.clearBridge();
      await context.globalState.update(CONNECTION_STATE_KEY, null);
      treeProvider.refresh();
      fileSystemProvider.fireChanged();
      vscode.window.showInformationMessage("Disconnected from Terrace sandbox bridge.");
    })
  );

  const previous = context.globalState.get(CONNECTION_STATE_KEY);
  if (previous && previous.endpoint) {
    installConnection(context, model, treeProvider, fileSystemProvider, previous).catch((error) => {
      vscode.window.showWarningMessage(`Terrace Sandbox reconnect failed: ${error.message}`);
    });
  }
}

function deactivate() {}

module.exports = {
  activate,
  deactivate
};
