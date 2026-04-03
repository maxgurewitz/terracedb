const __terraceModuleCache = new Map();
const __terraceBuiltinCache = new Map();
const __terraceWrappedBuiltinValues = new WeakMap();
const __terraceBuiltinAccesses = [];
const __terraceNodeResolutionBudget = 512;
const __terraceNodeLoadBudget = 512;
let __terraceNodeResolutionCount = 0;
let __terraceNodeLoadCount = 0;
const __terraceRequireStack = [];
const __terraceKnownNodeBuiltins = [
  "assert",
  "assert/strict",
  "async_hooks",
  "buffer",
  "child_process",
  "cluster",
  "console",
  "constants",
  "crypto",
  "dgram",
  "diagnostics_channel",
  "dns",
  "dns/promises",
  "domain",
  "events",
  "fs",
  "fs/promises",
  "http",
  "http2",
  "https",
  "inspector",
  "inspector/promises",
  "module",
  "net",
  "os",
  "path",
  "perf_hooks",
  "process",
  "punycode",
  "querystring",
  "readline",
  "readline/promises",
  "repl",
  "stream",
  "stream/consumers",
  "stream/promises",
  "stream/web",
  "string_decoder",
  "sys",
  "test",
  "timers",
  "timers/promises",
  "tls",
  "trace_events",
  "tty",
  "url",
  "util",
  "util/types",
  "v8",
  "vm",
  "wasi",
  "worker_threads",
  "zlib",
];

function __terraceRecordBuiltinAccess(entry) {
  __terraceBuiltinAccesses.push(entry);
}

function __terraceUnsupportedNodeBuiltinError({ builtin, member = null, operation, referrer = null }) {
  const path = member ? `${builtin}.${member}` : builtin;
  const location = referrer ? ` (required from ${referrer})` : "";
  const error = new Error(
    `ERR_TERRACE_NODE_UNIMPLEMENTED: node builtin API "${path}" is not implemented for operation "${operation}"${location}`,
  );
  error.code = "ERR_TERRACE_NODE_UNIMPLEMENTED";
  error.builtin = builtin;
  error.member = member;
  error.operation = operation;
  error.referrer = referrer;
  return error;
}

function __terraceThrowUnsupportedNodeBuiltin(meta) {
  __terraceRecordBuiltinAccess({
    builtin: meta.builtin,
    member: meta.member ?? null,
    operation: meta.operation,
    referrer: meta.referrer ?? null,
    status: "unimplemented",
  });
  throw __terraceUnsupportedNodeBuiltinError(meta);
}

function __terraceAssertNodeBudget(kind, count, budget, referrer, specifier) {
  if (count <= budget) return;
  const location = referrer ? ` from ${referrer}` : "";
  const stack = __terraceRequireStack.length ? ` [stack: ${__terraceRequireStack.join(" -> ")}]` : "";
  const error = new Error(
    `ERR_TERRACE_NODE_${kind.toUpperCase()}_BUDGET: exceeded deterministic ${kind} budget (${budget}) while processing ${specifier}${location}${stack}`,
  );
  error.code = `ERR_TERRACE_NODE_${kind.toUpperCase()}_BUDGET`;
  throw error;
}

function __terraceWrapBuiltinValue(value, meta) {
  if (value === null || value === undefined) return value;
  const kind = typeof value;
  if (kind !== "object" && kind !== "function") return value;
  if (__terraceWrappedBuiltinValues.has(value)) {
    return __terraceWrappedBuiltinValues.get(value);
  }
  const wrapped = new Proxy(value, {
    get(target, property, receiver) {
      if (property === "__terraceBuiltinMeta") return meta;
      if (property === Symbol.toStringTag) return Reflect.get(target, property, receiver);
      if (property === "then") return Reflect.get(target, property, receiver);
      if (Reflect.has(target, property)) {
        const next = Reflect.get(target, property, receiver);
        if (typeof property === "string" && !property.startsWith("__")) {
          __terraceRecordBuiltinAccess({
            builtin: meta.builtin,
            member: meta.member ?? property,
            operation: "get",
            referrer: meta.referrer ?? null,
            status: "implemented",
          });
        }
        return __terraceWrapBuiltinValue(next, {
          builtin: meta.builtin,
          member: typeof property === "string" ? property : meta.member ?? null,
          referrer: meta.referrer ?? null,
        });
      }
      if (typeof property === "symbol") return undefined;
      __terraceThrowUnsupportedNodeBuiltin({
        builtin: meta.builtin,
        member: property,
        operation: "get",
        referrer: meta.referrer ?? null,
      });
    },
    apply(target, thisArg, args) {
      __terraceRecordBuiltinAccess({
        builtin: meta.builtin,
        member: meta.member ?? null,
        operation: "call",
        referrer: meta.referrer ?? null,
        status: "implemented",
      });
      return __terraceWrapBuiltinValue(
        Reflect.apply(target, thisArg, args),
        meta,
      );
    },
    construct(target, args, newTarget) {
      __terraceRecordBuiltinAccess({
        builtin: meta.builtin,
        member: meta.member ?? null,
        operation: "construct",
        referrer: meta.referrer ?? null,
        status: "implemented",
      });
      return __terraceWrapBuiltinValue(
        Reflect.construct(target, args, newTarget),
        meta,
      );
    },
  });
  __terraceWrappedBuiltinValues.set(value, wrapped);
  return wrapped;
}

function __terraceCreateBuiltinStubModule(builtin, referrer) {
  const target = Object.create(null);
  return new Proxy(target, {
    get(_target, property) {
      if (property === "__terraceBuiltinMeta") {
        return { builtin, member: null, referrer };
      }
      if (property === "default") return undefined;
      if (property === "then") return undefined;
      if (typeof property === "symbol") return undefined;
      __terraceThrowUnsupportedNodeBuiltin({
        builtin,
        member: String(property),
        operation: "get",
        referrer,
      });
    },
    ownKeys() {
      return [];
    },
    getOwnPropertyDescriptor() {
      return undefined;
    },
  });
}

function __terraceNormalize(path) {
  if (!path) return "/";
  const absolute = path.startsWith("/");
  const parts = [];
  for (const part of String(path).split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") {
      if (parts.length) parts.pop();
      continue;
    }
    parts.push(part);
  }
  return `${absolute ? "/" : ""}${parts.join("/")}` || (absolute ? "/" : ".");
}

function __terraceDirname(path) {
  const normalized = __terraceNormalize(path);
  if (normalized === "/") return "/";
  const idx = normalized.lastIndexOf("/");
  return idx <= 0 ? "/" : normalized.slice(0, idx);
}

function __terraceBasename(path, ext = "") {
  const normalized = __terraceNormalize(path);
  const idx = normalized.lastIndexOf("/");
  let base = idx === -1 ? normalized : normalized.slice(idx + 1);
  if (ext && base.endsWith(ext)) base = base.slice(0, -ext.length);
  return base;
}

function __terraceExtname(path) {
  const base = __terraceBasename(path);
  const idx = base.lastIndexOf(".");
  return idx <= 0 ? "" : base.slice(idx);
}

function __terraceRelative(from, to) {
  const fromParts = __terraceNormalize(from).split("/").filter(Boolean);
  const toParts = __terraceNormalize(to).split("/").filter(Boolean);
  while (fromParts.length && toParts.length && fromParts[0] === toParts[0]) {
    fromParts.shift();
    toParts.shift();
  }
  const up = new Array(fromParts.length).fill("..");
  const out = up.concat(toParts).join("/");
  return out || ".";
}

function __terracePathModule() {
  const api = {
    sep: "/",
    delimiter: ":",
    join: (...parts) => __terraceNormalize(parts.filter(Boolean).join("/")),
    resolve: (...parts) => {
      let current = __terraceProcessInfo().cwd;
      for (const part of parts) {
        const value = String(part || "");
        if (!value) continue;
        current = value.startsWith("/") ? value : `${current}/${value}`;
      }
      return __terraceNormalize(current);
    },
    normalize: __terraceNormalize,
    dirname: __terraceDirname,
    basename: __terraceBasename,
    extname: __terraceExtname,
    relative: __terraceRelative,
    isAbsolute: (value) => String(value).startsWith("/"),
  };
  const resolve = (...parts) => {
    let current = __terraceProcessInfo().cwd;
    for (const part of parts) {
      const value = String(part || "");
      if (!value) continue;
      current = value.startsWith("/") ? value : `${current}/${value}`;
    }
    return __terraceNormalize(current);
  };
  api.resolve = resolve;
  api.posix = api;
  return api;
}

class TerraceEventEmitter {
  constructor() {
    this._listeners = new Map();
  }

  on(event, listener) {
    const listeners = this._listeners.get(event) || [];
    listeners.push(listener);
    this._listeners.set(event, listeners);
    return this;
  }

  addListener(event, listener) {
    return this.on(event, listener);
  }

  once(event, listener) {
    const wrapped = (...args) => {
      this.off(event, wrapped);
      listener(...args);
    };
    return this.on(event, wrapped);
  }

  off(event, listener) {
    const listeners = this._listeners.get(event) || [];
    this._listeners.set(
      event,
      listeners.filter((candidate) => candidate !== listener),
    );
    return this;
  }

  removeListener(event, listener) {
    return this.off(event, listener);
  }

  emit(event, ...args) {
    const listeners = this._listeners.get(event) || [];
    for (const listener of [...listeners]) listener(...args);
    return listeners.length > 0;
  }
}

class TerraceBuffer extends Uint8Array {
  static from(value) {
    if (value instanceof TerraceBuffer) return value;
    if (typeof value === "string") {
      return new TerraceBuffer(new TextEncoder().encode(value));
    }
    if (value instanceof ArrayBuffer) {
      return new TerraceBuffer(new Uint8Array(value));
    }
    if (ArrayBuffer.isView(value) || Array.isArray(value)) {
      return new TerraceBuffer(value);
    }
    throw new TypeError("unsupported Buffer.from input");
  }

  static isBuffer(value) {
    return value instanceof TerraceBuffer;
  }

  toString(encoding = "utf8") {
    if (encoding !== "utf8" && encoding !== "utf-8") {
      throw new Error(`unsupported buffer encoding: ${encoding}`);
    }
    return new TextDecoder().decode(this);
  }
}

function __terraceInspect(value) {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function __terraceFormat(...args) {
  if (args.length === 0) return "";
  const [head, ...rest] = args;
  if (typeof head !== "string") return args.map(__terraceInspect).join(" ");
  let index = 0;
  return head.replace(/%[sdjifoO%]/g, (token) => {
    if (token === "%%") return "%";
    const value = rest[index++];
    switch (token) {
      case "%d":
      case "%i":
        return Number(value).toString();
      case "%f":
        return Number(value).toString();
      case "%j":
      case "%O":
      case "%o":
        return __terraceInspect(value);
      case "%s":
      default:
        return String(value);
    }
  }) + (index < rest.length ? ` ${rest.slice(index).map(__terraceInspect).join(" ")}` : "");
}

function __terracePromisify(fn) {
  return (...args) => new Promise((resolve, reject) => {
    fn(...args, (error, value) => error ? reject(error) : resolve(value));
  });
}

function __terraceInherits(ctor, superCtor) {
  Object.setPrototypeOf(ctor.prototype, superCtor.prototype);
  Object.setPrototypeOf(ctor, superCtor);
}

function __terraceAssertModule() {
  const assert = (value, message = "assertion failed") => {
    if (!value) throw new Error(message);
  };
  assert.ok = assert;
  assert.equal = (left, right, message = `${left} != ${right}`) => {
    if (left != right) throw new Error(message);
  };
  assert.strictEqual = (left, right, message = `${left} !== ${right}`) => {
    if (left !== right) throw new Error(message);
  };
  assert.fail = (message = "assert.fail") => {
    throw new Error(message);
  };
  return assert;
}

function __terraceMakeStats(raw) {
  if (!raw) return null;
  return {
    ...raw,
    isFile() { return raw.kind === "file"; },
    isDirectory() { return raw.kind === "directory"; },
    isSymbolicLink() { return false; },
  };
}

function __terraceMaybeCallback(args, impl) {
  const callback = typeof args[args.length - 1] === "function" ? args[args.length - 1] : null;
  try {
    const value = impl();
    if (callback) {
      queueMicrotask(() => callback(null, value));
      return;
    }
    return value;
  } catch (error) {
    if (callback) {
      queueMicrotask(() => callback(error));
      return;
    }
    throw error;
  }
}

function __terraceFsModule() {
  const readFileSync = (path, encoding) => {
    const text = __terraceFsReadTextFile(String(path));
    return encoding ? text : TerraceBuffer.from(text);
  };
  const readFile = (...args) => __terraceMaybeCallback(args, () => {
    const [path, encoding] = args;
    return readFileSync(path, typeof encoding === "string" ? encoding : undefined);
  });
  const writeFileSync = (path, data) => {
    __terraceFsWriteTextFile(
      String(path),
      TerraceBuffer.isBuffer(data) ? data.toString() : String(data),
    );
  };
  const writeFile = (...args) => __terraceMaybeCallback(args, () => {
    const [path, data] = args;
    return writeFileSync(path, data);
  });
  const mkdirSync = (path) => { __terraceFsMkdir(String(path)); };
  const mkdir = (...args) => __terraceMaybeCallback(args, () => mkdirSync(args[0]));
  const readdirSync = (path, options) => {
    const entries = __terraceFsReaddir(String(path));
    if (options && options.withFileTypes) {
      return entries.map((entry) => ({
        name: entry.name,
        isFile: () => entry.kind === "file",
        isDirectory: () => entry.kind === "directory",
        isSymbolicLink: () => false,
      }));
    }
    return entries.map((entry) => entry.name);
  };
  const readdir = (...args) => __terraceMaybeCallback(args, () => {
    const [path, options] = args;
    return readdirSync(path, typeof options === "function" ? undefined : options);
  });
  const statSync = (path) => {
    const stat = __terraceFsStat(String(path));
    if (!stat) throw new Error(`ENOENT: no such file or directory, stat '${path}'`);
    return __terraceMakeStats(stat);
  };
  const stat = (...args) => __terraceMaybeCallback(args, () => statSync(args[0]));
  const lstatSync = statSync;
  const lstat = stat;
  const existsSync = (path) => !!__terraceFsStat(String(path));
  const realpathSync = (path) => __terraceFsRealpath(String(path));
  const realpath = (...args) => __terraceMaybeCallback(args, () => realpathSync(args[0]));
  const rmSync = (path) => { __terraceFsUnlink(String(path)); };
  const rm = (...args) => __terraceMaybeCallback(args, () => rmSync(args[0]));
  const unlinkSync = rmSync;
  const unlink = rm;
  const renameSync = (from, to) => { __terraceFsRename(String(from), String(to)); };
  const rename = (...args) => __terraceMaybeCallback(args, () => renameSync(args[0], args[1]));
  const constants = {
    O_RDONLY: 0,
    O_WRONLY: 1,
    O_RDWR: 2,
  };
  const api = {
    readFileSync,
    readFile,
    writeFileSync,
    writeFile,
    mkdirSync,
    mkdir,
    readdirSync,
    readdir,
    statSync,
    stat,
    lstatSync,
    lstat,
    existsSync,
    realpathSync,
    realpath,
    rmSync,
    rm,
    unlinkSync,
    unlink,
    renameSync,
    rename,
    constants,
  };
  api.realpathSync.native = realpathSync;
  api.realpath.native = realpath;
  return api;
}

function __terraceFsPromisesModule() {
  const fs = __terraceFsModule();
  return {
    readFile: fs.readFile,
    writeFile: fs.writeFile,
    mkdir: fs.mkdir,
    readdir: fs.readdir,
    stat: fs.stat,
    lstat: fs.lstat,
    realpath: fs.realpath,
    rm: fs.rm,
    unlink: fs.unlink,
    rename: fs.rename,
  };
}

class __TerraceProcessExit extends Error {
  constructor(code) {
    super(`process.exit(${code})`);
    this.__terraceExit = true;
    this.code = code;
  }
}

function __terraceProcessInfo() {
  return __terraceGetProcessInfo();
}

function __terraceCreateProcess() {
  const info = __terraceProcessInfo();
  const emitter = new TerraceEventEmitter();
  const process = {
    argv: [...info.argv],
    env: { ...info.env },
    execPath: info.execPath,
    version: info.version,
    versions: { node: info.version.replace(/^v/, "") },
    platform: info.platform,
    arch: info.arch,
    title: info.title,
    exitCode: 0,
    stdout: {
      isTTY: false,
      write(chunk) {
        __terraceWriteStdout(String(chunk));
        return true;
      },
    },
    stderr: {
      isTTY: false,
      write(chunk) {
        __terraceWriteStderr(String(chunk));
        return true;
      },
    },
    stdin: {
      isTTY: false,
      on(...args) { emitter.on(...args); return this; },
      off(...args) { emitter.off(...args); return this; },
      resume() {},
      pause() {},
    },
    cwd() {
      return __terraceGetCwd();
    },
    chdir(path) {
      __terraceChdir(String(path));
    },
    exit(code = process.exitCode || 0) {
      process.exitCode = code;
      __terraceSetExitCode(code);
      throw new __TerraceProcessExit(code);
    },
    nextTick(fn, ...args) {
      queueMicrotask(() => fn(...args));
    },
    emitWarning() {},
    on(event, listener) {
      emitter.on(event, listener);
      return process;
    },
    off(event, listener) {
      emitter.off(event, listener);
      return process;
    },
    once(event, listener) {
      emitter.once(event, listener);
      return process;
    },
    emit(event, ...args) {
      return emitter.emit(event, ...args);
    },
    umask() {
      return 0o022;
    },
  };
  return process;
}

function __terraceUrlModule() {
  const URLImpl = globalThis.URL || class TerraceUrl {
    constructor(href) { this.href = String(href); }
    toString() { return this.href; }
  };
  return {
    URL: URLImpl,
    format(value) { return String(value); },
    pathToFileURL(path) { return new URLImpl(`file://${path}`); },
    fileURLToPath(value) {
      const href = String(value);
      return href.startsWith("file://") ? href.slice("file://".length) : href;
    },
  };
}

function __terraceOsModule() {
  const info = __terraceProcessInfo();
  return {
    EOL: "\n",
    platform: () => info.platform,
    arch: () => info.arch,
    homedir: () => "/workspace/home",
    tmpdir: () => "/tmp",
    cpus: () => [],
    release: () => "terrace",
  };
}

function __terraceTtyModule() {
  return {
    isatty() { return false; },
  };
}

function __terraceUtilModule() {
  const debuglog = (_section) => {
    const logger = (...args) => {
      const message = __terraceFormat(...args);
      if (message) {
        __terraceProcessStderr(`${message}\n`);
      }
    };
    logger.enabled = false;
    return logger;
  };
  return {
    format: __terraceFormat,
    inspect: __terraceInspect,
    promisify: __terracePromisify,
    inherits: __terraceInherits,
    deprecate: (fn) => fn,
    debuglog,
  };
}

function __terraceConstantsModule() {
  return {
    signals: {},
  };
}

function __terraceQuerystringModule() {
  return {
    stringify(value = {}) {
      return Object.entries(value)
        .map(([key, entry]) => `${encodeURIComponent(key)}=${encodeURIComponent(String(entry))}`)
        .join("&");
    },
    parse(input = "") {
      const result = {};
      for (const part of String(input).split("&")) {
        if (!part) continue;
        const [key, value = ""] = part.split("=");
        result[decodeURIComponent(key)] = decodeURIComponent(value);
      }
      return result;
    },
  };
}

function __terraceStringDecoderModule() {
  return {
    StringDecoder: class StringDecoder {
      write(buffer) {
        return TerraceBuffer.from(buffer).toString();
      }
      end(buffer) {
        return buffer ? this.write(buffer) : "";
      }
    },
  };
}

function __terraceStreamModule() {
  class Stream extends TerraceEventEmitter {}
  class Readable extends Stream {}
  class Writable extends Stream {
    write(_chunk, callback) {
      if (typeof callback === "function") callback();
      return true;
    }
    end(_chunk, callback) {
      if (typeof callback === "function") callback();
      this.emit("finish");
    }
  }
  class Duplex extends Readable {}
  class Transform extends Duplex {}
  class PassThrough extends Transform {}
  return { Stream, Readable, Writable, Duplex, Transform, PassThrough };
}

function __terraceBuiltin(specifier, referrer = null) {
  const normalized = String(specifier).replace(/^node:/, "");
  const cacheKey = `${normalized}@@${referrer ?? ""}`;
  if (__terraceBuiltinCache.has(cacheKey)) {
    return __terraceBuiltinCache.get(cacheKey);
  }
  let value;
  switch (normalized) {
    case "module":
      value = {
        enableCompileCache() {
          return { status: "disabled" };
        },
        builtinModules: [...__terraceKnownNodeBuiltins],
      };
      break;
    case "process":
      value = globalThis.process;
      break;
    case "path":
      value = __terracePathModule();
      break;
    case "fs":
      value = __terraceFsModule();
      break;
    case "fs/promises":
      value = __terraceFsPromisesModule();
      break;
    case "events":
      value = { EventEmitter: TerraceEventEmitter };
      break;
    case "buffer":
      value = { Buffer: TerraceBuffer };
      break;
    case "assert":
      value = __terraceAssertModule();
      break;
    case "os":
      value = __terraceOsModule();
      break;
    case "tty":
      value = __terraceTtyModule();
      break;
    case "url":
      value = __terraceUrlModule();
      break;
    case "util":
      value = __terraceUtilModule();
      break;
    case "constants":
      value = __terraceConstantsModule();
      break;
    case "querystring":
      value = __terraceQuerystringModule();
      break;
    case "string_decoder":
      value = __terraceStringDecoderModule();
      break;
    case "stream":
      value = __terraceStreamModule();
      break;
    default:
      value = __terraceCreateBuiltinStubModule(normalized, referrer);
      __terraceBuiltinCache.set(cacheKey, value);
      return value;
  }
  const wrapped = __terraceWrapBuiltinValue(value, {
    builtin: normalized,
    member: null,
    referrer,
  });
  __terraceBuiltinCache.set(cacheKey, wrapped);
  return wrapped;
}

function __terraceUnsupportedGlobal(name) {
  return (..._args) => {
    __terraceThrowUnsupportedNodeBuiltin({
      builtin: "global",
      member: name,
      operation: "call",
      referrer: null,
    });
  };
}

function __terraceModuleForResolved(resolved) {
  if (__terraceModuleCache.has(resolved.id)) {
    return __terraceModuleCache.get(resolved.id);
  }
  __terraceNodeLoadCount += 1;
  __terraceAssertNodeBudget("load", __terraceNodeLoadCount, __terraceNodeLoadBudget, null, resolved.id);
  const module = {
    id: resolved.id,
    filename: resolved.id,
    loaded: false,
    exports: {},
    children: [],
  };
  __terraceModuleCache.set(resolved.id, module);
  if (resolved.kind === "json") {
    module.exports = JSON.parse(__terraceReadModuleSource(resolved.id));
    module.loaded = true;
    return module;
  }
  __terraceRequireStack.push(resolved.id);
  try {
    const source = __terraceReadModuleSource(resolved.id)
      .replace(/\bimport\s*\(/g, "__terraceNodeDynamicImport(__filename, ");
    const dirname = __terraceDirname(resolved.id);
    const localRequire = (specifier) => __terraceRequire(specifier, resolved.id);
    localRequire.resolve = (specifier) => __terraceResolveModule(specifier, resolved.id).id;
    localRequire.main = module;
    const wrapped = eval(`(function(exports, require, module, __filename, __dirname){\n${source}\n})`);
    wrapped(module.exports, localRequire, module, resolved.id, dirname);
    module.loaded = true;
    return module;
  } finally {
    __terraceRequireStack.pop();
  }
}

function __terraceRequire(specifier, referrer) {
  __terraceNodeResolutionCount += 1;
  __terraceAssertNodeBudget(
    "resolution",
    __terraceNodeResolutionCount,
    __terraceNodeResolutionBudget,
    referrer ? String(referrer) : null,
    String(specifier),
  );
  const resolved = __terraceResolveModule(String(specifier), referrer ? String(referrer) : null);
  if (resolved.kind === "builtin") {
    return __terraceBuiltin(resolved.id, referrer ? String(referrer) : null);
  }
  return __terraceModuleForResolved(resolved).exports;
}

function __terraceNodeDynamicImport(referrer, specifier) {
  __terraceNodeResolutionCount += 1;
  __terraceAssertNodeBudget(
    "resolution",
    __terraceNodeResolutionCount,
    __terraceNodeResolutionBudget,
    String(referrer),
    String(specifier),
  );
  const resolved = __terraceResolveModule(String(specifier), String(referrer));
  if (resolved.kind === "builtin") {
    const builtin = __terraceBuiltin(resolved.id, String(referrer));
    return Promise.resolve({
      default: builtin,
      ...builtin,
    });
  }
  return import(resolved.specifier);
}

function __terraceRunNodeCommand(request) {
  globalThis.global = globalThis;
  globalThis.process = __terraceCreateProcess();
  globalThis.console = {
    log(...args) { __terraceWriteStdout(`${args.map(__terraceInspect).join(" ")}\n`); },
    error(...args) { __terraceWriteStderr(`${args.map(__terraceInspect).join(" ")}\n`); },
    warn(...args) { __terraceWriteStderr(`${args.map(__terraceInspect).join(" ")}\n`); },
    info(...args) { __terraceWriteStdout(`${args.map(__terraceInspect).join(" ")}\n`); },
  };
  globalThis.Buffer = __terraceBuiltin("buffer").Buffer;
  globalThis.setTimeout = __terraceUnsupportedGlobal("setTimeout");
  globalThis.clearTimeout = __terraceUnsupportedGlobal("clearTimeout");
  globalThis.setImmediate = __terraceUnsupportedGlobal("setImmediate");
  globalThis.clearImmediate = __terraceUnsupportedGlobal("clearImmediate");
  try {
    __terraceRequire(request.entrypoint, null);
  } catch (error) {
    if (!(error && error.__terraceExit)) {
      throw error;
    }
  }
  const firstUnsupportedBuiltin = __terraceBuiltinAccesses.find(
    (entry) => entry.status === "unimplemented",
  );
  if (firstUnsupportedBuiltin) {
    throw __terraceUnsupportedNodeBuiltinError({
      builtin: firstUnsupportedBuiltin.builtin,
      member: firstUnsupportedBuiltin.member,
      operation: firstUnsupportedBuiltin.operation,
      referrer: firstUnsupportedBuiltin.referrer,
    });
  }
  return {
    cwd: process.cwd(),
    stdout: __terraceProcessInfo().stdout || "",
    stderr: __terraceProcessInfo().stderr || "",
    exitCode: process.exitCode || 0,
    argv: [...process.argv],
    builtinAccesses: [...__terraceBuiltinAccesses],
  };
}
