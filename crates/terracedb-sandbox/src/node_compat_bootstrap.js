const __terraceModuleCache = new Map();
const __terraceRequireCacheBacking = Object.create(null);
const __terraceBuiltinCache = new Map();
const __terraceWrappedBuiltinValues = new WeakMap();
const __terraceBuiltinAccesses = [];
let __terraceFsSingleton = null;
let __terracePathSingleton = null;
let __terraceOsSingleton = null;
let __terraceUrlSingleton = null;
let __terracePunycodeSingleton = null;
let __terraceCryptoSingleton = null;
let __terraceWebCryptoSingleton = null;
let __terraceZlibSingleton = null;
let __terraceV8Singleton = null;
let __terraceVmSingleton = null;
let __terraceWorkerThreadsSingleton = null;
let __terraceEventsSingleton = null;
let __terraceDnsSingleton = null;
let __terraceTlsSingleton = null;
let __terraceModuleSingleton = null;
let __terraceReplSingleton = null;
const __terraceWorkerEnvironmentData = new Map();
const __terraceMarkedUntransferable = new WeakSet();
const __terraceMarkedUncloneable = new WeakSet();
const __terraceBroadcastChannels = new Map();
let __terraceNextWorkerId = 1;
const __terraceNodeResolutionBudget = 16384;
const __terraceNodeLoadBudget = 8192;
let __terraceNodeResolutionCount = 0;
let __terraceNodeLoadCount = 0;
const __terraceRequireStack = [];
const __terraceNextTickQueue = [];
const __terraceTimeouts = new Map();
const __terraceImmediateQueue = [];
const __terraceImmediateEntries = new Map();
let __terraceNextTimerId = 1;
let __terraceCurrentTimerTime = 0;
let __terraceNodeCommandStarted = false;
let __terraceNodeCommandDebug = {
  topLevelResultKind: "unset",
  topLevelThenable: false,
  topLevelAwaited: false,
  caughtExit: false,
  caughtExitCode: null,
};

const __terraceRequireCache = new Proxy(__terraceRequireCacheBacking, {
  deleteProperty(target, property) {
    if (typeof property === "string") {
      __terraceModuleCache.delete(property);
    }
    return Reflect.deleteProperty(target, property);
  },
  defineProperty(target, property, descriptor) {
    if (typeof property === "string" && "value" in descriptor) {
      Reflect.deleteProperty(target, property);
      Reflect.set(target, property, descriptor.value);
      return true;
    }
    return Reflect.defineProperty(target, property, descriptor);
  },
  get(target, property, receiver) {
    return Reflect.get(target, property, receiver);
  },
  getOwnPropertyDescriptor(target, property) {
    return Reflect.getOwnPropertyDescriptor(target, property);
  },
  has(target, property) {
    return Reflect.has(target, property);
  },
  ownKeys(target) {
    return Reflect.ownKeys(target);
  },
  set(target, property, value, receiver) {
    return Reflect.set(target, property, value, receiver);
  },
});
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

if (typeof String.prototype.substr !== "function") {
  Object.defineProperty(String.prototype, "substr", {
    value(start, length) {
      const value = String(this);
      const size = value.length;
      let from = Number(start) || 0;
      if (from < 0) {
        from = Math.max(size + from, 0);
      } else {
        from = Math.min(from, size);
      }
      let span = length === undefined ? size - from : Number(length);
      if (!Number.isFinite(span) || span < 0) {
        span = 0;
      }
      return value.slice(from, from + span);
    },
    configurable: true,
    writable: true,
  });
}

if (typeof Error.captureStackTrace !== "function") {
  Object.defineProperty(Error, "captureStackTrace", {
    value(target, constructorOpt = undefined) {
      if (target == null || (typeof target !== "object" && typeof target !== "function")) {
        return;
      }
      const captured = new Error(target.message || "");
      let stack = typeof captured.stack === "string" ? captured.stack : `${target.name || "Error"}: ${target.message || ""}`;
      if (constructorOpt && typeof constructorOpt.name === "string") {
        const marker = constructorOpt.name;
        const lines = stack.split("\n");
        const trimmed = [lines[0], ...lines.slice(1).filter((line) => !line.includes(marker))];
        stack = trimmed.join("\n");
      }
      Object.defineProperty(target, "stack", {
        value: stack,
        configurable: true,
        writable: true,
        enumerable: false,
      });
    },
    configurable: true,
    writable: true,
  });
}

function __terraceUtf8Encode(input = "") {
  const bytes = [];
  for (const char of String(input)) {
    const codePoint = char.codePointAt(0);
    if (codePoint <= 0x7F) {
      bytes.push(codePoint);
    } else if (codePoint <= 0x7FF) {
      bytes.push(
        0xC0 | (codePoint >> 6),
        0x80 | (codePoint & 0x3F),
      );
    } else if (codePoint <= 0xFFFF) {
      bytes.push(
        0xE0 | (codePoint >> 12),
        0x80 | ((codePoint >> 6) & 0x3F),
        0x80 | (codePoint & 0x3F),
      );
    } else {
      bytes.push(
        0xF0 | (codePoint >> 18),
        0x80 | ((codePoint >> 12) & 0x3F),
        0x80 | ((codePoint >> 6) & 0x3F),
        0x80 | (codePoint & 0x3F),
      );
    }
  }
  return new Uint8Array(bytes);
}

function __terraceUtf8Decode(input = new Uint8Array()) {
  const view =
    input instanceof Uint8Array
      ? input
      : ArrayBuffer.isView(input)
        ? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
        : input instanceof ArrayBuffer
          ? new Uint8Array(input)
          : Uint8Array.from(input);
  let output = "";
  for (let index = 0; index < view.length; ) {
    const first = view[index];
    if ((first & 0x80) === 0) {
      output += String.fromCodePoint(first);
      index += 1;
      continue;
    }
    if ((first & 0xE0) === 0xC0 && index + 1 < view.length) {
      const codePoint = ((first & 0x1F) << 6) | (view[index + 1] & 0x3F);
      output += String.fromCodePoint(codePoint);
      index += 2;
      continue;
    }
    if ((first & 0xF0) === 0xE0 && index + 2 < view.length) {
      const codePoint =
        ((first & 0x0F) << 12) |
        ((view[index + 1] & 0x3F) << 6) |
        (view[index + 2] & 0x3F);
      output += String.fromCodePoint(codePoint);
      index += 3;
      continue;
    }
    if ((first & 0xF8) === 0xF0 && index + 3 < view.length) {
      const codePoint =
        ((first & 0x07) << 18) |
        ((view[index + 1] & 0x3F) << 12) |
        ((view[index + 2] & 0x3F) << 6) |
        (view[index + 3] & 0x3F);
      output += String.fromCodePoint(codePoint);
      index += 4;
      continue;
    }
    output += "\uFFFD";
    index += 1;
  }
  return output;
}

const __terraceBase64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

function __terraceBase64Encode(input = new Uint8Array()) {
  const bytes =
    input instanceof Uint8Array
      ? input
      : ArrayBuffer.isView(input)
        ? new Uint8Array(input.buffer, input.byteOffset, input.byteLength)
        : input instanceof ArrayBuffer
          ? new Uint8Array(input)
          : TerraceBuffer.from(input);
  let output = "";
  for (let index = 0; index < bytes.length; index += 3) {
    const first = bytes[index] ?? 0;
    const second = bytes[index + 1] ?? 0;
    const third = bytes[index + 2] ?? 0;
    const chunk = (first << 16) | (second << 8) | third;
    output += __terraceBase64Alphabet[(chunk >> 18) & 0x3F];
    output += __terraceBase64Alphabet[(chunk >> 12) & 0x3F];
    output += index + 1 < bytes.length ? __terraceBase64Alphabet[(chunk >> 6) & 0x3F] : "=";
    output += index + 2 < bytes.length ? __terraceBase64Alphabet[chunk & 0x3F] : "=";
  }
  return output;
}

function __terraceBase64Decode(input = "") {
  const sanitized = String(input).replace(/[\r\n\s]+/g, "");
  const bytes = [];
  let buffer = 0;
  let bits = 0;
  for (const char of sanitized) {
    if (char === "=") break;
    const index = __terraceBase64Alphabet.indexOf(char);
    if (index === -1) continue;
    buffer = (buffer << 6) | index;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes.push((buffer >> bits) & 0xFF);
    }
  }
  return new Uint8Array(bytes);
}

if (typeof globalThis.TextEncoder !== "function") {
  globalThis.TextEncoder = class TerraceTextEncoder {
    encode(input = "") {
      return __terraceUtf8Encode(input);
    }
  };
}

if (typeof globalThis.TextDecoder !== "function") {
  globalThis.TextDecoder = class TerraceTextDecoder {
    decode(input = new Uint8Array()) {
      return __terraceUtf8Decode(input);
    }
  };
}

function __terraceRecordBuiltinAccess(entry) {
  __terraceBuiltinAccesses.push(entry);
}

function __terraceFormatDebugTraceDetail(detail) {
  if (detail === undefined) return "";
  if (typeof detail === "string") return detail;
  try {
    return JSON.stringify(detail);
  } catch (_error) {
    return String(detail);
  }
}

function __terraceDebugTrace(label, detail) {
  const suffix = __terraceFormatDebugTraceDetail(detail);
  const payload = suffix.length === 0 ? String(label) : `${String(label)} ${suffix}`;
  try {
    __terraceNodeDebugEvent("js", payload);
  } catch (_error) {
    // Debug tracing must never change guest behavior.
  }
}

function __terraceTraceNode(detail) {
  __terraceDebugTrace("trace", detail);
}

function __terraceDescribeReceivedValue(value) {
  if (value === null) return "null";
  const kind = typeof value;
  switch (kind) {
    case "string":
      return `type string ('${String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}')`;
    case "number":
      return `type number (${String(value)})`;
    case "boolean":
      return `type boolean (${String(value)})`;
    case "symbol":
      return `type symbol (${String(value)})`;
    case "bigint":
      return `type bigint (${String(value)}n)`;
    case "undefined":
      return "undefined";
    case "function":
      return `type function (${value.name ? `[Function: ${value.name}]` : "[Function (anonymous)]"})`;
    default:
      return `type ${kind}`;
  }
}

function __terraceInvalidThisError(typeName) {
  return __terraceNodeTypeError(
    "ERR_INVALID_THIS",
    `Value of "this" must be of type ${typeName}`,
  );
}

globalThis.__terraceDebugTrace = __terraceDebugTrace;
if (typeof globalThis.queueMicrotask !== "function") {
  globalThis.queueMicrotask = (fn) => Promise.resolve().then(fn);
}

function __terraceGetNodeDebugConfig() {
  const config = globalThis.__terraceNodeDebugConfig;
  return config && typeof config === "object" ? config : {};
}

function __terraceNodeDebugFlag(name) {
  return !!__terraceGetNodeDebugConfig()[name];
}

function __terraceModulePatternMatches(pattern, id) {
  if (typeof pattern !== "string" || pattern.length === 0) return false;
  if (pattern === id) return true;
  if (pattern.includes("*")) {
    const escaped = pattern
      .replace(/[|\\{}()[\]^$+?.]/g, "\\$&")
      .replace(/\*/g, ".*");
    return new RegExp(`^${escaped}$`).test(id);
  }
  return id.includes(pattern);
}

function __terraceShouldAutoinstrumentModule(id) {
  const patterns = __terraceGetNodeDebugConfig().autoinstrument_modules;
  if (!Array.isArray(patterns) || patterns.length === 0) {
    return false;
  }
  return patterns.some((pattern) => __terraceModulePatternMatches(pattern, id));
}

function __terraceCaptureException(source, detail) {
  if (!__terraceNodeDebugFlag("capture_exceptions")) {
    return;
  }
  const error = detail && detail.error;
  const fallback = new Error(`Sandbox exception at ${source}`);
  __terraceDebugTrace("exception", {
    source,
    module: detail && detail.module ? detail.module : null,
    entrypoint: detail && detail.entrypoint ? detail.entrypoint : null,
    owner: detail && detail.owner ? detail.owner : null,
    member: detail && detail.member ? detail.member : null,
    name: error && error.name ? error.name : null,
    message: error && error.message ? error.message : String(error),
    stack:
      error && typeof error.stack === "string"
        ? error.stack
        : typeof fallback.stack === "string"
          ? fallback.stack
          : null,
    requireStack: __terraceRequireStack.slice(),
  });
}

function __terraceAutoinstrumentModuleSource(id, kind, source) {
  if (kind !== "commonjs" || !__terraceShouldAutoinstrumentModule(id)) {
    return source;
  }
  const renderEntryTrace = (indent, name, type) =>
    `${indent}  __terraceDebugTrace("${type}", { module: ${JSON.stringify(id)}, method: ${JSON.stringify(name)} });`;
  const prologue = source.match(/^(\s*(?:(?:"use strict"|'use strict');?\s*)+)/);
  const directivePrefix = prologue ? prologue[0] : "";
  const body = directivePrefix.length > 0 ? source.slice(directivePrefix.length) : source;
  const instrumentedBody = body
    .split("\n")
    .flatMap((line) => {
      const lines = [line];
      const match = line.match(
        /^(\s*)(?:async\s+)?(?:static\s+)?(?:get\s+|set\s+)?(\*?\s*(?:#?[A-Za-z_$][\w$]*|\[[^\]]+\]))\s*\([^)]*\)\s*\{$/,
      );
      if (match) {
        const indent = match[1] ?? "";
        const method = (match[2] ?? "").trim();
        lines.push(renderEntryTrace(indent, method, "method-enter-source"));
      }
      const functionMatch = line.match(
        /^(\s*)function\s+([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{$/,
      );
      if (functionMatch) {
        const indent = functionMatch[1] ?? "";
        const name = functionMatch[2] ?? "<anonymous>";
        lines.push(renderEntryTrace(indent, name, "function-enter-source"));
      }
      const asyncFunctionMatch = line.match(
        /^(\s*)async\s+function\s+([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{$/,
      );
      if (asyncFunctionMatch) {
        const indent = asyncFunctionMatch[1] ?? "";
        const name = asyncFunctionMatch[2] ?? "<anonymous>";
        lines.push(renderEntryTrace(indent, name, "function-enter-source"));
      }
      const arrowMatch = line.match(
        /^(\s*)(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s+)?(?:\([^)]*\)|[A-Za-z_$][\w$]*)\s*=>\s*\{$/,
      );
      if (arrowMatch) {
        const indent = arrowMatch[1] ?? "";
        const name = arrowMatch[2] ?? "<anonymous>";
        lines.push(renderEntryTrace(indent, name, "function-enter-source"));
      }
      return lines;
    })
    .join("\n");
  return `${directivePrefix}
__terraceDebugTrace("autoinstrument-enter", { module: ${JSON.stringify(id)}, kind: ${JSON.stringify(kind)} });
${instrumentedBody}
__terraceDebugTrace("autoinstrument-exit", { module: ${JSON.stringify(id)}, kind: ${JSON.stringify(kind)} });
`;
}

const __terraceDebugWrappedFunctions = new WeakMap();
const __terraceDebugInstrumentedObjects = new WeakSet();

function __terraceDescribeDebugValue(value) {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (Array.isArray(value)) return "array";
  if (value instanceof Promise) return "promise";
  const kind = typeof value;
  if (kind === "object" || kind === "function") {
    if (kind === "function" && typeof value.name === "string" && value.name.length > 0) {
      return value.name;
    }
    try {
      const tag = Object.prototype.toString.call(value);
      const match = /^\[object ([^\]]+)\]$/.exec(tag);
      if (match && match[1]) {
        return match[1];
      }
    } catch (_error) {
      // Ignore debug labeling failures.
    }
    return kind;
  }
  return kind;
}

function __terraceLooksLikeClass(fn) {
  if (typeof fn !== "function") {
    return false;
  }
  const prototype = fn.prototype;
  if (prototype && typeof prototype === "object") {
    try {
      const prototypeKeys = Object.getOwnPropertyNames(prototype);
      if (prototypeKeys.some((key) => key !== "constructor")) {
        return true;
      }
    } catch (_error) {
      // Ignore descriptor failures in debug heuristics.
    }
  }
  try {
    return /^\s*class\b/.test(Function.prototype.toString.call(fn));
  } catch (_error) {
    return false;
  }
}

function __terraceWrapDebugFunction(moduleId, ownerLabel, key, fn) {
  if (typeof fn !== "function") {
    return fn;
  }
  if (__terraceDebugWrappedFunctions.has(fn)) {
    return __terraceDebugWrappedFunctions.get(fn);
  }
  const invoke = (thisArg, args) => {
    __terraceDebugTrace("call-enter", {
      module: moduleId,
      owner: ownerLabel,
      member: key,
      args: args.map(__terraceDescribeDebugValue),
      requireStack: __terraceRequireStack.slice(),
    });
    try {
      const result = Reflect.apply(fn, thisArg, args);
      if (result && typeof result.then === "function") {
        return result.then(
          (value) => {
            const instrumentedValue = __terraceAutoinstrumentModuleExports(moduleId, value);
            __terraceDebugTrace("call-exit", {
              module: moduleId,
              owner: ownerLabel,
              member: key,
              result: __terraceDescribeDebugValue(instrumentedValue),
              async: true,
            });
            return instrumentedValue;
          },
          (error) => {
            __terraceDebugTrace("call-throw", {
              module: moduleId,
              owner: ownerLabel,
              member: key,
              async: true,
              name: error && error.name ? error.name : null,
              message: error && error.message ? error.message : String(error),
              stack: error && typeof error.stack === "string" ? error.stack : null,
              requireStack: __terraceRequireStack.slice(),
            });
            __terraceCaptureException("call", {
              module: moduleId,
              owner: ownerLabel,
              member: key,
              error,
            });
            throw error;
          },
        );
      }
      const instrumentedResult = __terraceAutoinstrumentModuleExports(moduleId, result);
      __terraceDebugTrace("call-exit", {
        module: moduleId,
        owner: ownerLabel,
        member: key,
        result: __terraceDescribeDebugValue(instrumentedResult),
      });
      return instrumentedResult;
    } catch (error) {
      __terraceDebugTrace("call-throw", {
        module: moduleId,
        owner: ownerLabel,
        member: key,
        async: false,
        name: error && error.name ? error.name : null,
        message: error && error.message ? error.message : String(error),
        stack: error && typeof error.stack === "string" ? error.stack : null,
        requireStack: __terraceRequireStack.slice(),
      });
      __terraceCaptureException("call", {
        module: moduleId,
        owner: ownerLabel,
        member: key,
        error,
      });
      throw error;
    }
  };
  const wrapped = new Proxy(fn, {
    apply(target, thisArg, args) {
      return invoke(thisArg, args);
    },
    construct(target, args, newTarget) {
      return Reflect.construct(target, args, newTarget);
    },
    defineProperty(target, property, descriptor) {
      return Reflect.defineProperty(target, property, descriptor);
    },
    deleteProperty(target, property) {
      return Reflect.deleteProperty(target, property);
    },
    get(target, property, receiver) {
      return Reflect.get(target, property, receiver);
    },
    getOwnPropertyDescriptor(target, property) {
      return Reflect.getOwnPropertyDescriptor(target, property);
    },
    getPrototypeOf(target) {
      return Reflect.getPrototypeOf(target);
    },
    has(target, property) {
      return Reflect.has(target, property);
    },
    isExtensible(target) {
      return Reflect.isExtensible(target);
    },
    ownKeys(target) {
      return Reflect.ownKeys(target);
    },
    preventExtensions(target) {
      return Reflect.preventExtensions(target);
    },
    set(target, property, value, receiver) {
      return Reflect.set(target, property, value, receiver);
    },
    setPrototypeOf(target, prototype) {
      return Reflect.setPrototypeOf(target, prototype);
    },
  });
  __terraceDebugWrappedFunctions.set(fn, wrapped);
  return wrapped;
}

function __terraceAutoinstrumentObjectMethods(moduleId, ownerLabel, target, skipConstructor = false) {
  if (!target || (typeof target !== "object" && typeof target !== "function")) {
    return;
  }
  if (__terraceDebugInstrumentedObjects.has(target)) {
    return;
  }
  __terraceDebugInstrumentedObjects.add(target);
  for (const key of Reflect.ownKeys(target)) {
    if (skipConstructor && key === "constructor") continue;
    const descriptor = Reflect.getOwnPropertyDescriptor(target, key);
    if (!descriptor || !("value" in descriptor) || typeof descriptor.value !== "function") {
      continue;
    }
    try {
      Object.defineProperty(target, key, {
        ...descriptor,
        value: __terraceWrapDebugFunction(moduleId, ownerLabel, String(key), descriptor.value),
      });
    } catch (_error) {
      // Debug instrumentation must never block module evaluation.
    }
  }
}

function __terraceAutoinstrumentModuleExports(moduleId, exportsValue) {
  if (!__terraceShouldAutoinstrumentModule(moduleId)) {
    return exportsValue;
  }
  if (typeof exportsValue === "function") {
    __terraceAutoinstrumentObjectMethods(moduleId, "exports.static", exportsValue);
    if (exportsValue.prototype && typeof exportsValue.prototype === "object") {
      __terraceAutoinstrumentObjectMethods(
        moduleId,
        "exports.prototype",
        exportsValue.prototype,
        true,
      );
    }
    if (__terraceLooksLikeClass(exportsValue)) {
      return exportsValue;
    }
    return __terraceWrapDebugFunction(moduleId, "exports", "<callable>", exportsValue);
  }
  if (exportsValue && typeof exportsValue === "object") {
    __terraceAutoinstrumentObjectMethods(moduleId, "exports", exportsValue);
  }
  return exportsValue;
}

function __terraceTraceNullishToObject(method, argumentIndex) {
  if (!__terraceNodeDebugFlag("trace_intrinsics")) {
    return;
  }
  const stackError = new Error(`ToObject nullish in ${method}`);
  __terraceDebugTrace("nullish-to-object", {
    method,
    argumentIndex,
    stack: typeof stackError.stack === "string" ? stackError.stack : null,
    requireStack: __terraceRequireStack.slice(),
  });
}

function __terraceWrapToObjectBuiltin(owner, method, options = {}) {
  const original = owner[method];
  if (typeof original !== "function") return;
  const wrapped = function (...args) {
    const positions = options.positions ?? [0];
    for (const index of positions) {
      if (index >= args.length) continue;
      if (args[index] === null || args[index] === undefined) {
        __terraceTraceNullishToObject(`${options.ownerName ?? "Object"}.${method}`, index);
        break;
      }
    }
    return Reflect.apply(original, this, args);
  };
  Object.defineProperty(owner, method, {
    value: wrapped,
    configurable: true,
    writable: true,
  });
}

__terraceWrapToObjectBuiltin(Object, "keys");
__terraceWrapToObjectBuiltin(Object, "values");
__terraceWrapToObjectBuiltin(Object, "entries");
__terraceWrapToObjectBuiltin(Object, "getOwnPropertyNames");
__terraceWrapToObjectBuiltin(Object, "getOwnPropertySymbols");
__terraceWrapToObjectBuiltin(Object, "getOwnPropertyDescriptors");
__terraceWrapToObjectBuiltin(Object, "getPrototypeOf");
__terraceWrapToObjectBuiltin(Object, "setPrototypeOf");
__terraceWrapToObjectBuiltin(Object, "seal");
__terraceWrapToObjectBuiltin(Object, "freeze");
__terraceWrapToObjectBuiltin(Object, "preventExtensions");
__terraceWrapToObjectBuiltin(Object, "isSealed");
__terraceWrapToObjectBuiltin(Object, "isFrozen");
__terraceWrapToObjectBuiltin(Object, "isExtensible");
__terraceWrapToObjectBuiltin(Object, "defineProperties", { positions: [0, 1] });
__terraceWrapToObjectBuiltin(Object, "assign", { positions: [0] });
__terraceWrapToObjectBuiltin(Reflect, "ownKeys", { ownerName: "Reflect" });

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
  __terraceDebugTrace("missing-builtin", {
    builtin: meta.builtin,
    member: meta.member ?? null,
    operation: meta.operation,
    referrer: meta.referrer ?? null,
  });
  throw __terraceUnsupportedNodeBuiltinError(meta);
}

function __terraceRecordMissingBuiltinProbe(meta) {
  __terraceRecordBuiltinAccess({
    builtin: meta.builtin,
    member: meta.member ?? null,
    operation: meta.operation,
    referrer: meta.referrer ?? null,
    status: "missing",
  });
  __terraceDebugTrace("missing-builtin", {
    builtin: meta.builtin,
    member: meta.member ?? null,
    operation: meta.operation,
    referrer: meta.referrer ?? null,
    outcome: "undefined",
  });
  return undefined;
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
  if (kind !== "object") return value;
  if (__terraceWrappedBuiltinValues.has(value)) {
    return __terraceWrappedBuiltinValues.get(value);
  }
  const wrapped = new Proxy(value, {
    get(target, property, receiver) {
      if (property === "__terraceBuiltinMeta") return meta;
      if (property === Symbol.toStringTag) return Reflect.get(target, property, receiver);
      if (property === "then") return Reflect.get(target, property, receiver);
      if (property === "__esModule" || property === "default") {
        return Reflect.get(target, property, receiver);
      }
      if (Reflect.has(target, property)) {
        const next = Reflect.get(target, property, receiver);
        const descriptor = Reflect.getOwnPropertyDescriptor(target, property);
        if (typeof property === "string" && !property.startsWith("__")) {
          __terraceRecordBuiltinAccess({
            builtin: meta.builtin,
            member: meta.member ?? property,
            operation: "get",
            referrer: meta.referrer ?? null,
            status: "implemented",
          });
        }
        if (
          descriptor &&
          descriptor.configurable === false &&
          "value" in descriptor &&
          descriptor.writable === false
        ) {
          return next;
        }
        return __terraceWrapBuiltinValue(next, {
          builtin: meta.builtin,
          member: typeof property === "string" ? property : meta.member ?? null,
          referrer: meta.referrer ?? null,
        });
      }
      if (typeof property === "symbol") return undefined;
      if (meta.builtin === "process") {
        return __terraceRecordMissingBuiltinProbe({
          builtin: meta.builtin,
          member: property,
          operation: "get",
          referrer: meta.referrer ?? null,
        });
      }
      __terraceThrowUnsupportedNodeBuiltin({
        builtin: meta.builtin,
        member: property,
        operation: "get",
        referrer: meta.referrer ?? null,
      });
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
      if (property === "__esModule" || property === "default") {
        return __terraceRecordMissingBuiltinProbe({
          builtin,
          member: String(property),
          operation: "get",
          referrer,
        });
      }
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

function __terracePathMatchesGlobPattern(path, pattern, windows) {
  const normalize = (value) => {
    const text = String(value);
    return windows ? text.replace(/\\/g, "/") : text;
  };
  const escaped = normalize(pattern)
    .replace(/\*\*/g, "\u0000")
    .replace(/\*/g, "\u0001")
    .replace(/\?/g, "\u0002")
    .replace(/[.+^${}()|[\]\\]/g, "\\$&")
    .replace(/\u0000/g, ".*")
    .replace(/\u0001/g, "[^/]*")
    .replace(/\u0002/g, ".");
  return new RegExp(`^${escaped}$`).test(normalize(path));
}

function __terracePathPrimordials() {
  return {
    ArrayPrototypeIncludes: (target, value) => Array.prototype.includes.call(target, value),
    ArrayPrototypeJoin: (target, separator) => Array.prototype.join.call(target, separator),
    ArrayPrototypePush: (target, ...values) => Array.prototype.push.apply(target, values),
    ArrayPrototypeSlice: (target, start, end) => Array.prototype.slice.call(target, start, end),
    FunctionPrototypeBind: (fn, thisArg, ...args) => Function.prototype.bind.call(fn, thisArg, ...args),
    StringPrototypeCharCodeAt: (target, index) => String.prototype.charCodeAt.call(target, index),
    StringPrototypeIncludes: (target, search, position) => String.prototype.includes.call(target, search, position),
    StringPrototypeIndexOf: (target, search, position) => String.prototype.indexOf.call(target, search, position),
    StringPrototypeLastIndexOf: (target, search, position) => String.prototype.lastIndexOf.call(target, search, position),
    StringPrototypeRepeat: (target, count) => String.prototype.repeat.call(target, count),
    StringPrototypeReplace: (target, pattern, replacement) => String.prototype.replace.call(target, pattern, replacement),
    StringPrototypeSlice: (target, start, end) => String.prototype.slice.call(target, start, end),
    StringPrototypeSplit: (target, separator, limit) => String.prototype.split.call(target, separator, limit),
    StringPrototypeToLowerCase: (target) => String.prototype.toLowerCase.call(target),
    StringPrototypeToUpperCase: (target) => String.prototype.toUpperCase.call(target),
  };
}

function __terracePathInternalRequire(specifier) {
  switch (specifier) {
    case "internal/constants":
      return {
        CHAR_UPPERCASE_A: 65,
        CHAR_LOWERCASE_A: 97,
        CHAR_UPPERCASE_Z: 90,
        CHAR_LOWERCASE_Z: 122,
        CHAR_DOT: 46,
        CHAR_FORWARD_SLASH: 47,
        CHAR_BACKWARD_SLASH: 92,
        CHAR_COLON: 58,
        CHAR_QUESTION_MARK: 63,
      };
    case "internal/validators":
      return {
        validateObject(value, name) {
          if (value == null || typeof value !== "object") {
            throw __terraceNodeTypeError(
              "ERR_INVALID_ARG_TYPE",
              `The "${name}" argument must be of type object. Received ${value === null ? "null" : typeof value}`,
            );
          }
        },
        validateString(value, name) {
          if (typeof value !== "string") {
            throw __terraceNodeTypeError(
              "ERR_INVALID_ARG_TYPE",
              `The "${name}" argument must be of type string. Received ${value === null ? "null" : typeof value}`,
            );
          }
        },
      };
    case "internal/util":
      return {
        isWindows: globalThis.process?.platform === "win32",
        getLazy(loader) {
          let loaded = false;
          let value;
          return () => {
            if (!loaded) {
              value = loader();
              loaded = true;
            }
            return value;
          };
        },
      };
    case "internal/fs/glob":
      return {
        matchGlobPattern(path, pattern, windows) {
          return __terracePathMatchesGlobPattern(path, pattern, windows);
        },
      };
    default:
      throw __terraceUnsupportedNodeBuiltinError({
        builtin: specifier,
        operation: "require",
      });
  }
}

function __terracePathModule() {
  if (__terracePathSingleton) return __terracePathSingleton;
  const source = __terraceReadBuiltinSource("path");
  const module = { exports: {} };
  const factory = Function(
    "primordials",
    "require",
    "process",
    "module",
    "exports",
    `${source}\n;return module.exports;`,
  );
  __terracePathSingleton = factory(
    __terracePathPrimordials(),
    __terracePathInternalRequire,
    globalThis.process,
    module,
    module.exports,
  );
  return __terracePathSingleton;
}

function __terraceOsPrimordials() {
  return {
    ArrayPrototypePush: (target, ...values) => Array.prototype.push.apply(target, values),
    Float64Array,
    ObjectDefineProperties: Object.defineProperties,
    ObjectFreeze: Object.freeze,
    StringPrototypeSlice: (target, start, end) => String.prototype.slice.call(target, start, end),
    SymbolToPrimitive: Symbol.toPrimitive,
  };
}

function __terraceIpv4MaskToPrefix(netmask) {
  const parts = String(netmask).split(".");
  if (parts.length !== 4) return null;
  let prefix = 0;
  let pastZero = false;
  for (const part of parts) {
    const value = Number(part);
    if (!Number.isInteger(value) || value < 0 || value > 255) return null;
    for (let bit = 7; bit >= 0; bit -= 1) {
      const enabled = (value & (1 << bit)) !== 0;
      if (enabled) {
        if (pastZero) return null;
        prefix += 1;
      } else {
        pastZero = true;
      }
    }
  }
  return prefix;
}

function __terraceIpv6MaskToPrefix(netmask) {
  const groups = String(netmask).split(":");
  if (groups.length !== 8) return null;
  let prefix = 0;
  let pastZero = false;
  for (const group of groups) {
    const value = Number.parseInt(group || "0", 16);
    if (!Number.isInteger(value) || value < 0 || value > 0xFFFF) return null;
    for (let bit = 15; bit >= 0; bit -= 1) {
      const enabled = (value & (1 << bit)) !== 0;
      if (enabled) {
        if (pastZero) return null;
        prefix += 1;
      } else {
        pastZero = true;
      }
    }
  }
  return prefix;
}

function __terraceOsGetCIDR(address, netmask, family) {
  if (!address || !netmask) return null;
  const normalizedFamily = family === 4 ? "IPv4" : family === 6 ? "IPv6" : String(family);
  const prefix =
    normalizedFamily === "IPv6"
      ? __terraceIpv6MaskToPrefix(netmask)
      : __terraceIpv4MaskToPrefix(netmask);
  return prefix == null ? null : `${address}/${prefix}`;
}

function __terraceCreateSystemErrorClass() {
  class TerraceSystemError extends Error {
    constructor(ctx = {}) {
      const syscall = ctx.syscall || "unknown";
      const code = ctx.code || "UNKNOWN";
      const message = ctx.message || "unknown error";
      super(`A system error occurred: ${syscall} returned ${code} (${message})`);
      this.name = "SystemError";
      this.code = "ERR_SYSTEM_ERROR";
      this.info = { ...ctx };
      if (ctx.errno != null) this.errno = ctx.errno;
      if (ctx.path != null) this.path = ctx.path;
      if (ctx.dest != null) this.dest = ctx.dest;
      if (ctx.syscall != null) this.syscall = ctx.syscall;
    }
  }
  TerraceSystemError.HideStackFramesError = class HideStackFramesError extends TerraceSystemError {};
  return TerraceSystemError;
}

function __terraceOsInternalRequire(specifier) {
  switch (specifier) {
    case "internal/errors": {
      const ERR_SYSTEM_ERROR = __terraceCreateSystemErrorClass();
      return {
        codes: { ERR_SYSTEM_ERROR },
        hideStackFrames(fn) {
          return fn;
        },
      };
    }
    case "internal/util":
      return {
        isWindows: globalThis.process?.platform === "win32",
        getLazy(loader) {
          let loaded = false;
          let value;
          return () => {
            if (!loaded) {
              value = loader();
              loaded = true;
            }
            return value;
          };
        },
        getCIDR(address, netmask, family) {
          return __terraceOsGetCIDR(address, netmask, family);
        },
      };
    case "internal/validators":
      return {
        validateObject(value, name) {
          if (value == null || typeof value !== "object") {
            throw __terraceNodeTypeError(
              "ERR_INVALID_ARG_TYPE",
              `The "${name}" argument must be of type object. Received ${value === null ? "null" : typeof value}`,
            );
          }
        },
        validateString(value, name) {
          if (typeof value !== "string") {
            throw __terraceNodeTypeError(
              "ERR_INVALID_ARG_TYPE",
              `The "${name}" argument must be of type string. Received ${value === null ? "null" : typeof value}`,
            );
          }
        },
        validateInt32(value, name, min = -2147483648, max = 2147483647) {
          if (!Number.isInteger(value)) {
            throw __terraceNodeTypeError(
              "ERR_INVALID_ARG_TYPE",
              `The "${name}" argument must be of type number. Received ${value === null ? "null" : typeof value}`,
            );
          }
          if (value < min || value > max) {
            throw __terraceNodeRangeError(
              "ERR_OUT_OF_RANGE",
              `The value of "${name}" is out of range. It must be >= ${min} and <= ${max}. Received ${String(value)}`,
            );
          }
        },
      };
    default:
      return __terracePathInternalRequire(specifier);
  }
}

function __terraceOsInternalBinding(specifier) {
  switch (specifier) {
    case "constants":
      return { os: __terraceOsConstants };
    case "credentials":
      return {
        getTempDir() {
          return __terraceOsTmpdir(globalThis.process?.platform || "linux");
        },
      };
    case "os":
      return {
        getAvailableParallelism() {
          return 1;
        },
        getCPUs() {
          const rows = [];
          for (const cpu of __terraceOsCpuRows) {
            rows.push(
              cpu.model,
              cpu.speed,
              cpu.times.user,
              cpu.times.nice,
              cpu.times.sys,
              cpu.times.idle,
              cpu.times.irq,
            );
          }
          return rows;
        },
        getFreeMem() {
          return 1024 * 1024 * 1024;
        },
        getHomeDirectory(ctx = {}) {
          const value = __terraceOsHomedir(globalThis.process?.platform || "linux");
          if (!value) {
            ctx.code = "ENOENT";
            ctx.syscall = "uv_os_homedir";
            ctx.message = "home directory unavailable";
            return undefined;
          }
          return value;
        },
        getHostname(ctx = {}) {
          const value = "terrace-sandbox";
          if (!value) {
            ctx.code = "ENOENT";
            ctx.syscall = "uv_os_gethostname";
            ctx.message = "hostname unavailable";
            return undefined;
          }
          return value;
        },
        getInterfaceAddresses(_ctx = {}) {
          const rows = [];
          for (const [name, entries] of Object.entries(__terraceOsLoopbackInterfaces)) {
            for (const entry of entries) {
              rows.push(
                name,
                entry.address,
                entry.netmask,
                entry.family,
                entry.mac,
                entry.internal,
                entry.scopeid ?? -1,
              );
            }
          }
          return rows;
        },
        getLoadAvg(target) {
          if (target && typeof target.length === "number") {
            target[0] = 0;
            target[1] = 0;
            target[2] = 0;
          }
        },
        getPriority(pid, ctx = {}) {
          try {
            return __terraceOsGetPriority(pid);
          } catch (error) {
            ctx.code = error.info?.code || "ESRCH";
            ctx.syscall = error.info?.syscall || "uv_os_getpriority";
            ctx.message = error.info?.message || error.message;
            return undefined;
          }
        },
        getOSInformation(_ctx = {}) {
          const info = __terraceProcessInfo();
          const platform = info.platform || "linux";
          return [
            platform === "win32" ? "Windows_NT" : "Linux",
            "#1 Terrace",
            "6.0.0-terrace",
            __terraceOsMachine(info.arch),
          ];
        },
        getTotalMem() {
          return 1024 * 1024 * 1024;
        },
        getUserInfo(options = null, _ctx = {}) {
          const platform = globalThis.process?.platform || "linux";
          const encoding = options && typeof options === "object" ? options.encoding : undefined;
          const asBuffer = encoding === "buffer";
          const homedir = __terraceOsHomedir(platform);
          const username = "sandbox";
          const shell = platform === "win32" ? "cmd.exe" : "/bin/sh";
          return {
            uid: 0,
            gid: 0,
            username: asBuffer ? TerraceBuffer.from(username) : username,
            homedir: asBuffer ? TerraceBuffer.from(homedir) : homedir,
            shell: asBuffer ? TerraceBuffer.from(shell) : shell,
          };
        },
        getUptime(_ctx = {}) {
          return 1;
        },
        isBigEndian: false,
        setPriority(pid, priority, ctx = {}) {
          try {
            __terraceOsSetPriority(pid, priority);
            return 0;
          } catch (error) {
            ctx.code = error.info?.code || "ESRCH";
            ctx.syscall = error.info?.syscall || "uv_os_setpriority";
            ctx.message = error.info?.message || error.message;
            return -1;
          }
        },
      };
    default:
      throw __terraceUnsupportedNodeBuiltinError({
        builtin: `internalBinding(${specifier})`,
        operation: "call",
      });
  }
}

function __terraceOsModule() {
  if (__terraceOsSingleton) return __terraceOsSingleton;
  const source = __terraceReadBuiltinSource("os");
  const module = { exports: {} };
  const factory = Function(
    "primordials",
    "require",
    "process",
    "module",
    "exports",
    "internalBinding",
    `${source}\n;return module.exports;`,
  );
  __terraceOsSingleton = factory(
    __terraceOsPrimordials(),
    __terraceOsInternalRequire,
    globalThis.process,
    module,
    module.exports,
    __terraceOsInternalBinding,
  );
  return __terraceOsSingleton;
}

let __terraceDefaultMaxListeners = 10;
const __terraceEventTargetMaxListeners = new WeakMap();

function __terraceEnsureEventEmitter(value, name = "emitter") {
  if (
    !value ||
    (typeof value.on !== "function" && typeof value.addEventListener !== "function")
  ) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be an instance of EventEmitter or EventTarget. Received ${value === null ? "null" : typeof value}`,
    );
  }
  return value;
}

function __terraceEnsureEventName(value, name = "eventName") {
  if (typeof value !== "string" && typeof value !== "symbol") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type string or symbol. Received ${value === null ? "null" : typeof value}`,
    );
  }
  return value;
}

function __terraceEnsureAbortSignal(signal) {
  if (
    !signal ||
    typeof signal !== "object" ||
    typeof signal.addEventListener !== "function" ||
    typeof signal.removeEventListener !== "function"
  ) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      'The "signal" argument must be an instance of AbortSignal.',
    );
  }
  return signal;
}

function __terraceUnhandledError(value) {
  if (value instanceof Error) return value;
  const error = new Error(`Unhandled error.${value !== undefined ? ` (${value})` : ""}`);
  error.code = "ERR_UNHANDLED_ERROR";
  error.context = value;
  return error;
}

function __terraceGetEventTargetListeners(target, event) {
  if (!target || typeof target !== "object") return [];
  const map = target.__terraceEventTargetListeners;
  if (!(map instanceof Map)) return [];
  return [...(map.get(event) || [])];
}

function __terraceRecordEventTargetListener(target, event, listener, once = false) {
  if (!target || typeof target !== "object") return listener;
  if (!(target.__terraceEventTargetListeners instanceof Map)) {
    Object.defineProperty(target, "__terraceEventTargetListeners", {
      value: new Map(),
      configurable: true,
      enumerable: false,
      writable: true,
    });
  }
  const map = target.__terraceEventTargetListeners;
  const listeners = map.get(event) || [];
  const existing = listeners.find(
    (candidate) =>
      candidate === listener || candidate.__terraceOriginalListener === listener,
  );
  if (existing) return existing;
  const wrapped = once
    ? (...args) => {
        __terraceRemoveEventTargetListenerRecord(target, event, listener);
        return listener(...args);
      }
    : listener;
  wrapped.__terraceOriginalListener = listener;
  listeners.push(wrapped);
  map.set(event, listeners);
  return wrapped;
}

function __terraceRemoveEventTargetListenerRecord(target, event, listener) {
  if (!target || typeof target !== "object") return;
  const map = target.__terraceEventTargetListeners;
  if (!(map instanceof Map)) return;
  const listeners = map.get(event) || [];
  map.set(
    event,
    listeners.filter(
      (candidate) =>
        candidate !== listener &&
        candidate.__terraceOriginalListener !== listener,
    ),
  );
}

let __terraceEventTargetPatched = false;
function __terracePatchEventTargetPrototype() {
  if (__terraceEventTargetPatched) return;
  if (typeof EventTarget !== "function") return;
  __terraceEventTargetPatched = true;
  const originalAdd = EventTarget.prototype.addEventListener;
  const originalRemove = EventTarget.prototype.removeEventListener;
  EventTarget.prototype.addEventListener = function (event, listener, options) {
    if (typeof listener === "function") {
      const once = !!(options && typeof options === "object" && options.once);
      const wrapped = __terraceRecordEventTargetListener(this, event, listener, once);
      return originalAdd.call(this, event, wrapped, options);
    }
    return originalAdd.call(this, event, listener, options);
  };
  EventTarget.prototype.removeEventListener = function (event, listener, options) {
    const registered = __terraceGetEventTargetListeners(this, event);
    const wrapped =
      registered.find(
        (candidate) =>
          candidate === listener || candidate.__terraceOriginalListener === listener,
      ) || listener;
    __terraceRemoveEventTargetListenerRecord(this, event, listener);
    return originalRemove.call(this, event, wrapped, options);
  };
}

function __terraceGetMaxListeners(target) {
  if (target instanceof TerraceEventEmitter) {
    return target.getMaxListeners();
  }
  if (target instanceof AbortSignal) {
    return __terraceEventTargetMaxListeners.has(target)
      ? __terraceEventTargetMaxListeners.get(target)
      : 0;
  }
  __terraceEnsureEventEmitter(target);
  return __terraceEventTargetMaxListeners.has(target)
    ? __terraceEventTargetMaxListeners.get(target)
    : __terraceDefaultMaxListeners;
}

function __terraceSetMaxListeners(value, ...targets) {
  const limit = Number(value);
  if (!Number.isFinite(limit) || limit < 0) {
    throw __terraceNodeTypeError(
      "ERR_OUT_OF_RANGE",
      'The value of "n" is out of range. It must be a non-negative number.',
    );
  }
  if (targets.length === 0) {
    __terraceDefaultMaxListeners = limit;
    return;
  }
  for (const target of targets) {
    if (target instanceof TerraceEventEmitter) {
      target.setMaxListeners(limit);
    } else {
      __terraceEnsureEventEmitter(target);
      __terraceEventTargetMaxListeners.set(target, limit);
    }
  }
}

function __terraceGetEventListeners(target, event) {
  __terraceEnsureEventName(event);
  if (target instanceof TerraceEventEmitter) {
    return target.listeners(event);
  }
  __terraceEnsureEventEmitter(target);
  return __terraceGetEventTargetListeners(target, event).map(
    (listener) => listener.__terraceOriginalListener || listener,
  );
}

function __terraceAbortError() {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  error.code = "ABORT_ERR";
  return error;
}

function __terraceAddAbortListener(signal, listener) {
  __terraceEnsureAbortSignal(signal);
  if (typeof listener !== "function") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      'The "listener" argument must be of type function.',
    );
  }
  const wrapped = () => listener();
  signal.addEventListener("abort", wrapped, { once: true });
  return {
    [Symbol.dispose]() {
      signal.removeEventListener("abort", wrapped);
    },
    dispose() {
      signal.removeEventListener("abort", wrapped);
    },
  };
}

function __terraceEventsOnce(target, event, options = undefined) {
  __terraceEnsureEventEmitter(target);
  __terraceEnsureEventName(event);
  if (options !== undefined && (options === null || typeof options !== "object")) {
    return Promise.reject(
      __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "options" argument must be of type object. Received ' +
          `${options === null ? "null" : typeof options}`,
      ),
    );
  }
  const signal = options && "signal" in options ? options.signal : undefined;
  if (signal !== undefined) __terraceEnsureAbortSignal(signal);
  if (signal && signal.aborted) {
    return Promise.reject(__terraceAbortError());
  }
  return new Promise((resolve, reject) => {
    let done = false;
    let abortDisposable = null;
    const cleanup = () => {
      if (done) return;
      done = true;
      if (target instanceof TerraceEventEmitter) {
        target.off(event, onEvent);
        if (event !== "error") target.off("error", onError);
      } else {
        target.removeEventListener(event, onEvent);
      }
      abortDisposable?.dispose?.();
    };
    const onEvent = (...args) => {
      cleanup();
      resolve(args);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onAbort = () => {
      cleanup();
      reject(__terraceAbortError());
    };
    if (target instanceof TerraceEventEmitter) {
      target.once(event, onEvent);
      if (event !== "error") target.once("error", onError);
    } else {
      target.addEventListener(event, onEvent, { once: true });
    }
    if (signal) {
      abortDisposable = __terraceAddAbortListener(signal, onAbort);
    }
  });
}

function __terraceEventsOn(target, event, options = undefined) {
  __terraceEnsureEventEmitter(target);
  __terraceEnsureEventName(event);
  if (options !== undefined && (options === null || typeof options !== "object")) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      'The "options" argument must be of type object. Received ' +
        `${options === null ? "null" : typeof options}`,
    );
  }
  const signal = options && "signal" in options ? options.signal : undefined;
  if (signal !== undefined) __terraceEnsureAbortSignal(signal);
  const queue = [];
  const waiting = [];
  let finished = false;
  const push = (value) => {
    if (finished) return;
    if (waiting.length) {
      waiting.shift().resolve({ value, done: false });
    } else {
      queue.push(value);
    }
  };
  const close = (error = null) => {
    if (finished) return;
    finished = true;
    if (target instanceof TerraceEventEmitter) {
      target.off(event, onEvent);
      target.off("error", onError);
    } else {
      target.removeEventListener(event, onEvent);
    }
    abortDisposable?.dispose?.();
    while (waiting.length) {
      const next = waiting.shift();
      if (error) next.reject(error);
      else next.resolve({ value: undefined, done: true });
    }
  };
  const onEvent = (...args) => push(args);
  const onError = (error) => close(error);
  if (target instanceof TerraceEventEmitter) {
    target.on(event, onEvent);
    if (event !== "error") target.on("error", onError);
  } else {
    target.addEventListener(event, onEvent);
  }
  let abortDisposable = null;
  if (signal) {
    if (signal.aborted) close(__terraceAbortError());
    else abortDisposable = __terraceAddAbortListener(signal, () => close(__terraceAbortError()));
  }
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (queue.length) {
        return Promise.resolve({ value: queue.shift(), done: false });
      }
      if (finished) {
        return Promise.resolve({ value: undefined, done: true });
      }
      return new Promise((resolve, reject) => waiting.push({ resolve, reject }));
    },
    return() {
      close();
      return Promise.resolve({ value: undefined, done: true });
    },
    throw(error) {
      close(error);
      return Promise.reject(error);
    },
  };
}

function __terraceEnsureEventTargetGlobals() {
  if (typeof globalThis.Event !== "function") {
    globalThis.Event = class Event {
      constructor(type, options = {}) {
        this.type = String(type);
        this.bubbles = !!options.bubbles;
        this.cancelable = !!options.cancelable;
        this.composed = !!options.composed;
        this.defaultPrevented = false;
        this.target = null;
        this.currentTarget = null;
      }
      preventDefault() {
        if (this.cancelable) this.defaultPrevented = true;
      }
    };
  }
  if (typeof globalThis.EventTarget !== "function") {
    globalThis.EventTarget = class EventTarget {
      constructor() {
        Object.defineProperty(this, "__terraceListeners", {
          value: new Map(),
          enumerable: false,
          configurable: true,
          writable: true,
        });
      }
      addEventListener(type, listener, options = undefined) {
        if (typeof listener !== "function") return;
        const listeners = this.__terraceListeners.get(type) || [];
        if (listeners.some((entry) => entry.listener === listener)) return;
        const once = !!(options && typeof options === "object" && options.once);
        listeners.push({ listener, once });
        this.__terraceListeners.set(type, listeners);
      }
      removeEventListener(type, listener) {
        const listeners = this.__terraceListeners.get(type) || [];
        this.__terraceListeners.set(
          type,
          listeners.filter((entry) => entry.listener !== listener),
        );
      }
      dispatchEvent(event) {
        if (!event || typeof event.type !== "string") {
          throw __terraceNodeTypeError(
            "ERR_INVALID_ARG_TYPE",
            'The "event" argument must be an Event.',
          );
        }
        event.target = this;
        event.currentTarget = this;
        const listeners = [...(this.__terraceListeners.get(event.type) || [])];
        for (const entry of listeners) {
          entry.listener.call(this, event);
          if (entry.once) this.removeEventListener(event.type, entry.listener);
        }
        return !event.defaultPrevented;
      }
    };
  }
  if (typeof globalThis.AbortSignal !== "function") {
    globalThis.AbortSignal = class AbortSignal extends globalThis.EventTarget {
      constructor() {
        super();
        this.aborted = false;
        this.reason = undefined;
      }
      throwIfAborted() {
        if (this.aborted) throw this.reason ?? __terraceAbortError();
      }
      static abort(reason = undefined) {
        const controller = new globalThis.AbortController();
        controller.abort(reason);
        return controller.signal;
      }
    };
  }
  if (typeof globalThis.AbortController !== "function") {
    globalThis.AbortController = class AbortController {
      constructor() {
        this.signal = new globalThis.AbortSignal();
      }
      abort(reason = undefined) {
        if (this.signal.aborted) return;
        this.signal.aborted = true;
        this.signal.reason = reason ?? __terraceAbortError();
        this.signal.dispatchEvent(new globalThis.Event("abort"));
      }
    };
  }
}

__terraceEnsureEventTargetGlobals();

class TerraceEventEmitter {
  constructor() {
    this._listeners = new Map();
    this._maxListeners = undefined;
    this._eventsCount = 0;
  }

  _validateListener(listener) {
    if (typeof listener !== "function") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "listener" argument must be of type function.',
      );
    }
  }

  on(event, listener) {
    this._validateListener(listener);
    this.emit("newListener", event, listener);
    const listeners = this._listeners.get(event) || [];
    listeners.push(listener);
    this._listeners.set(event, listeners);
    this._eventsCount = this._listeners.size;
    return this;
  }

  addListener(event, listener) {
    return this.on(event, listener);
  }

  prependListener(event, listener) {
    this._validateListener(listener);
    const listeners = this._listeners.get(event) || [];
    listeners.unshift(listener);
    this._listeners.set(event, listeners);
    return this;
  }

  once(event, listener) {
    this._validateListener(listener);
    const wrapped = (...args) => {
      this.off(event, wrapped);
      listener(...args);
    };
    wrapped.__terraceOriginalListener = listener;
    return this.on(event, wrapped);
  }

  prependOnceListener(event, listener) {
    this._validateListener(listener);
    const wrapped = (...args) => {
      this.off(event, wrapped);
      listener(...args);
    };
    wrapped.__terraceOriginalListener = listener;
    return this.prependListener(event, wrapped);
  }

  off(event, listener) {
    const listeners = this._listeners.get(event) || [];
    const next = listeners.filter(
      (candidate) =>
        candidate !== listener &&
        candidate.__terraceOriginalListener !== listener,
    );
    if (next.length > 0) this._listeners.set(event, next);
    else this._listeners.delete(event);
    this._eventsCount = this._listeners.size;
    this.emit("removeListener", event, listener);
    return this;
  }

  removeListener(event, listener) {
    return this.off(event, listener);
  }

  emit(event, ...args) {
    const listeners = this._listeners.get(event) || [];
    if (event === "error" && listeners.length === 0) {
      throw __terraceUnhandledError(args[0]);
    }
    for (const listener of [...listeners]) {
      if (typeof listener !== "function") {
        __terraceDebugTrace("invalid-listener", {
          event: String(event),
          emitter: this && this.constructor && this.constructor.name,
          listenerType: typeof listener,
        });
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          `Listener for event "${String(event)}" must be callable.`,
        );
      }
      listener(...args);
    }
    return listeners.length > 0;
  }

  listenerCount(event) {
    const listeners = this._listeners.get(event);
    return Array.isArray(listeners) ? listeners.length : 0;
  }

  listeners(event) {
    const listeners = this._listeners.get(event) || [];
    return listeners.map((listener) => listener.__terraceOriginalListener || listener);
  }

  rawListeners(event) {
    return [...(this._listeners.get(event) || [])];
  }

  removeAllListeners(event) {
    if (event === undefined) {
      this._listeners.clear();
      this._eventsCount = 0;
      return this;
    }
    this._listeners.delete(event);
    this._eventsCount = this._listeners.size;
    return this;
  }

  eventNames() {
    return [...this._listeners.keys()];
  }

  getMaxListeners() {
    return this._maxListeners ?? __terraceDefaultMaxListeners;
  }

  setMaxListeners(value) {
    if (!Number.isFinite(value) || value < 0) {
      throw __terraceNodeTypeError(
        "ERR_OUT_OF_RANGE",
        'The value of "n" is out of range. It must be a non-negative number.',
      );
    }
    this._maxListeners = Number(value);
    return this;
  }
}

function __terraceEventsModule() {
  if (__terraceEventsSingleton) return __terraceEventsSingleton;
  __terracePatchEventTargetPrototype();
  const EventEmitter = TerraceEventEmitter;
  EventEmitter.EventEmitter = EventEmitter;
  EventEmitter.captureRejectionSymbol = Symbol.for("nodejs.rejection");
  EventEmitter.errorMonitor = Symbol("events.errorMonitor");
  Object.defineProperty(EventEmitter, "defaultMaxListeners", {
    get() {
      return __terraceDefaultMaxListeners;
    },
    set(value) {
      __terraceSetMaxListeners(value);
    },
    enumerable: true,
  });
  Object.defineProperty(EventEmitter, "captureRejections", {
    get() {
      return !!EventEmitter.prototype.__terraceCaptureRejections;
    },
    set(value) {
      EventEmitter.prototype.__terraceCaptureRejections = !!value;
    },
    enumerable: true,
  });
  EventEmitter.listenerCount = (emitter, event) => {
    __terraceEnsureEventName(event);
    if (emitter && typeof emitter.listenerCount === "function") {
      return emitter.listenerCount(event);
    }
    return __terraceGetEventListeners(emitter, event).length;
  };
  EventEmitter.once = __terraceEventsOnce;
  EventEmitter.on = __terraceEventsOn;
  EventEmitter.getEventListeners = __terraceGetEventListeners;
  EventEmitter.getMaxListeners = __terraceGetMaxListeners;
  EventEmitter.setMaxListeners = __terraceSetMaxListeners;
  EventEmitter.addAbortListener = __terraceAddAbortListener;
  EventEmitter.usingDomains = false;
  const exported = EventEmitter;
  exported.once = __terraceEventsOnce;
  exported.on = __terraceEventsOn;
  exported.getEventListeners = __terraceGetEventListeners;
  exported.getMaxListeners = __terraceGetMaxListeners;
  exported.setMaxListeners = __terraceSetMaxListeners;
  exported.addAbortListener = __terraceAddAbortListener;
  __terraceEventsSingleton = EventEmitter;
  return __terraceEventsSingleton;
}

class TerraceBuffer extends Uint8Array {
  static from(value, encoding = "utf8") {
    if (value instanceof TerraceBuffer) return value;
    if (typeof value === "string") {
      return new TerraceBuffer(__terraceEncodeString(value, encoding));
    }
    if (value instanceof ArrayBuffer) {
      return new TerraceBuffer(new Uint8Array(value));
    }
    if (typeof SharedArrayBuffer === "function" && value instanceof SharedArrayBuffer) {
      return new TerraceBuffer(new Uint8Array(value));
    }
    if (ArrayBuffer.isView(value) || Array.isArray(value)) {
      return new TerraceBuffer(value);
    }
    throw new TypeError("unsupported Buffer.from input");
  }

  static alloc(size, fill = 0, encoding = "utf8") {
    const buffer = new TerraceBuffer(Number(size));
    if (typeof fill === "string") {
      const bytes = TerraceBuffer.from(fill, encoding);
      for (let index = 0; index < buffer.length; index += 1) {
        buffer[index] = bytes[index % bytes.length] ?? 0;
      }
      return buffer;
    }
    buffer.fill(Number(fill) || 0);
    return buffer;
  }

  static allocUnsafe(size) {
    return new TerraceBuffer(Number(size));
  }

  static byteLength(value, encoding = "utf8") {
    return TerraceBuffer.from(value, encoding).byteLength;
  }

  static concat(list = [], totalLength = undefined) {
    const buffers = Array.from(list, (entry) => TerraceBuffer.from(entry));
    const size =
      totalLength == null
        ? buffers.reduce((sum, entry) => sum + entry.byteLength, 0)
        : Number(totalLength);
    const out = new TerraceBuffer(size);
    let offset = 0;
    for (const buffer of buffers) {
      out.set(buffer.subarray(0, Math.max(0, size - offset)), offset);
      offset += buffer.byteLength;
      if (offset >= size) break;
    }
    return out;
  }

  static isBuffer(value) {
    return value instanceof TerraceBuffer;
  }

  write(string, offset = 0, length = undefined, encoding = "utf8") {
    if (typeof offset === "string") {
      encoding = offset;
      offset = 0;
      length = undefined;
    } else if (typeof length === "string") {
      encoding = length;
      length = undefined;
    }
    const start = Math.max(0, Number(offset) || 0);
    const available = Math.max(0, this.byteLength - start);
    const span = length == null ? available : Math.max(0, Math.min(available, Number(length) || 0));
    const bytes = TerraceBuffer.from(String(string), encoding);
    const written = Math.min(span, bytes.byteLength);
    if (written > 0) {
      this.set(bytes.subarray(0, written), start);
    }
    return written;
  }

  toString(encoding = "utf8") {
    return __terraceDecodeBytes(this, encoding);
  }
}

function __terraceEncodeString(input = "", encoding = "utf8") {
  const normalized = String(encoding || "utf8").toLowerCase();
  if (normalized === "utf8" || normalized === "utf-8") {
    return __terraceUtf8Encode(String(input));
  }
  if (normalized === "latin1" || normalized === "binary" || normalized === "ascii") {
    return new Uint8Array(Array.from(String(input), (char) => char.charCodeAt(0) & 0xFF));
  }
  if (normalized === "hex") {
    const text = String(input).trim();
    const size = Math.floor(text.length / 2);
    const output = new Uint8Array(size);
    for (let index = 0; index < size; index += 1) {
      output[index] = Number.parseInt(text.slice(index * 2, index * 2 + 2), 16) || 0;
    }
    return output;
  }
  if (normalized === "base64") {
    return __terraceBase64Decode(String(input));
  }
  if (normalized === "ucs2" || normalized === "ucs-2" || normalized === "utf16le" || normalized === "utf-16le") {
    const value = String(input);
    const output = new Uint8Array(value.length * 2);
    for (let index = 0; index < value.length; index += 1) {
      const code = value.charCodeAt(index);
      output[index * 2] = code & 0xFF;
      output[index * 2 + 1] = (code >> 8) & 0xFF;
    }
    return output;
  }
  throw new Error(`unsupported buffer encoding: ${encoding}`);
}

function __terraceDecodeBytes(input, encoding = "utf8") {
  const normalized = String(encoding || "utf8").toLowerCase();
  const bytes = TerraceBuffer.isBuffer(input) ? input : TerraceBuffer.from(input);
  if (normalized === "utf8" || normalized === "utf-8") {
    return new TextDecoder().decode(bytes);
  }
  if (normalized === "latin1" || normalized === "binary" || normalized === "ascii") {
    return Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
  }
  if (normalized === "hex") {
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  }
  if (normalized === "base64") {
    return __terraceBase64Encode(bytes);
  }
  if (normalized === "ucs2" || normalized === "ucs-2" || normalized === "utf16le" || normalized === "utf-16le") {
    let output = "";
    for (let index = 0; index < bytes.length; index += 2) {
      output += String.fromCharCode((bytes[index] || 0) | ((bytes[index + 1] || 0) << 8));
    }
    return output;
  }
  throw new Error(`unsupported buffer encoding: ${encoding}`);
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

let __terraceMkdtempCounter = 0;

class TerraceStats {
  constructor(raw, kind = null) {
    Object.assign(this, raw || {});
    this.kind = kind ?? raw?.kind ?? "file";
    this.dev ??= 0;
    this.ino ??= Number(this.inode ?? 0);
    this.mode ??= 0o100666;
    this.nlink ??= 1;
    this.uid ??= 0;
    this.gid ??= 0;
    this.rdev ??= 0;
    this.blksize ??= 4096;
    this.blocks ??= Math.ceil((this.size ?? 0) / 512);
    const created = Number(this.created_at ?? 0);
    const modified = Number(this.modified_at ?? created);
    const changed = Number(this.changed_at ?? modified);
    const accessed = Number(this.accessed_at ?? modified);
    this.atimeMs ??= accessed;
    this.mtimeMs ??= modified;
    this.ctimeMs ??= changed;
    this.birthtimeMs ??= created;
    this.atime ??= new Date(this.atimeMs);
    this.mtime ??= new Date(this.mtimeMs);
    this.ctime ??= new Date(this.ctimeMs);
    this.birthtime ??= new Date(this.birthtimeMs);
  }
  isFile() { return this.kind === "file"; }
  isDirectory() { return this.kind === "directory"; }
  isSymbolicLink() { return this.kind === "symlink"; }
  isBlockDevice() { return false; }
  isCharacterDevice() { return false; }
  isFIFO() { return false; }
  isSocket() { return false; }
}

class TerraceDirent {
  constructor(name, kind = "file") {
    this.name = name;
    this.kind = kind;
  }
  isFile() { return this.kind === "file"; }
  isDirectory() { return this.kind === "directory"; }
  isSymbolicLink() { return this.kind === "symlink"; }
  isBlockDevice() { return false; }
  isCharacterDevice() { return false; }
  isFIFO() { return false; }
  isSocket() { return false; }
}

function __terraceMakeStats(raw, kind = null) {
  if (!raw) return null;
  return new TerraceStats(raw, kind);
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

function __terraceNormalizeFsError(error, path, syscall) {
  const message =
    error && typeof error.message === "string" ? error.message : String(error);
  const backendPrefix = "node_runtime backend error: ";
  if (message.startsWith(backendPrefix)) {
    const detail = message.slice(backendPrefix.length);
    const match = /^([A-Z0-9_]+): .*?, ([A-Za-z_][A-Za-z0-9_]*)(?: '([^']+)')?$/.exec(detail);
    const code = match?.[1] ?? null;
    const normalized = new Error(detail);
    if (code) {
      normalized.code = code;
      const errno = __terraceOsErrnoConstants?.[code];
      if (typeof errno === "number") {
        normalized.errno = -errno;
      }
    }
    normalized.path = match?.[3] ?? String(path);
    normalized.syscall = match?.[2] ?? syscall;
    return normalized;
  }
  if (message.includes("sandbox module not found:")) {
    const normalized = new Error(`ENOENT: no such file or directory, ${syscall} '${path}'`);
    normalized.code = "ENOENT";
    normalized.errno = -2;
    normalized.path = String(path);
    normalized.syscall = syscall;
    return normalized;
  }
  return error;
}

function __terraceFsPromise(fn) {
  return (...args) => Promise.resolve().then(() => fn(...args));
}

function __terraceFsModule() {
  if (__terraceFsSingleton) return __terraceFsSingleton;
  const pathApi = __terracePathModule();
  const streamApi = __terraceBuiltin("stream");
  const watcherState = new Map();
  const descriptorState = new Map();

  const constants = {
    O_RDONLY: 0,
    O_WRONLY: 1,
    O_RDWR: 2,
    O_CREAT: 64,
    O_EXCL: 128,
    O_TRUNC: 512,
    O_APPEND: 1024,
    O_SYMLINK: 0,
    F_OK: 0,
    R_OK: 4,
    W_OK: 2,
    X_OK: 1,
    UV_DIRENT_UNKNOWN: 0,
    UV_DIRENT_FILE: 1,
    UV_DIRENT_DIR: 2,
    UV_DIRENT_LINK: 3,
    UV_DIRENT_FIFO: 4,
    UV_DIRENT_SOCKET: 5,
    UV_DIRENT_CHAR: 6,
    UV_DIRENT_BLOCK: 7,
  };

  const normalizePath = (value) => {
    if (typeof value === "string") return pathApi.resolve(value);
    if (TerraceBuffer.isBuffer(value) || value instanceof Uint8Array) {
      return pathApi.resolve(__terraceDecodeBytes(value));
    }
    if (value instanceof URL) {
      if (value.protocol !== "file:") {
        throw new TypeError(`unsupported URL protocol for fs path: ${value.protocol}`);
      }
      return pathApi.resolve(decodeURIComponent(value.pathname));
    }
    if (value && typeof value === "object" && typeof value.href === "string") {
      return normalizePath(new URL(value.href));
    }
    if (value == null) {
      throw new TypeError("fs path must be a string, Buffer, URL, or Uint8Array");
    }
    return pathApi.resolve(String(value));
  };
  const normalizeEncoding = (options, fallback = undefined) => {
    if (typeof options === "string") return options;
    if (options && typeof options === "object" && typeof options.encoding === "string") {
      return options.encoding;
    }
    return fallback;
  };
  const normalizeOpenFlags = (flags, fallback = "r") => {
    if (flags === undefined) return fallback;
    if (typeof flags === "number") return flags;
    const normalized = String(flags);
    const valid = new Set([
      "r", "rs", "sr",
      "r+", "rs+", "sr+",
      "w", "w+",
      "wx", "xw",
      "wx+", "xw+",
      "a", "a+",
      "ax", "xa",
      "ax+", "xa+",
      "as", "sa",
      "as+", "sa+",
    ]);
    if (!valid.has(normalized)) {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_VALUE",
        `The argument 'flags' is invalid. Received ${JSON.stringify(normalized)}`,
      );
    }
    return normalized;
  };
  const normalizeFlushOption = (options) => {
    if (!(options && typeof options === "object")) return false;
    if (!Object.prototype.hasOwnProperty.call(options, "flush")) return false;
    const value = options.flush;
    if (value == null) return false;
    if (typeof value !== "boolean") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "flush" option must be of type boolean.',
      );
    }
    return value;
  };
  const normalizeWriteFlag = (options, fallback) => {
    if (options && typeof options === "object" && typeof options.flag === "string") {
      return normalizeOpenFlags(options.flag, fallback);
    }
    return fallback;
  };
  const toBuffer = (value, encoding = "utf8") => {
    if (TerraceBuffer.isBuffer(value)) return value;
    if (value instanceof Uint8Array || value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
      return TerraceBuffer.from(value);
    }
    if (typeof value === "string") {
      return TerraceBuffer.from(value, encoding);
    }
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      "The \"data\" argument must be of type string or an instance of Buffer, TypedArray, or DataView.",
    );
  };
  const toReturnValue = (bytes, options) => {
    const encoding = normalizeEncoding(options);
    return encoding ? __terraceDecodeBytes(bytes, encoding) : TerraceBuffer.from(bytes);
  };
  const makeFsError = (code, syscall, path, message = undefined) => {
    const error = new Error(
      message ?? `${code}: ${code === "ENOENT" ? "no such file or directory" : "fs error"}, ${syscall} '${path}'`,
    );
    error.code = code;
    error.path = String(path);
    error.syscall = syscall;
    return error;
  };
  const makeBadFdError = (syscall) => {
    const error = new Error(`EBADF: bad file descriptor, ${syscall}`);
    error.code = "EBADF";
    error.syscall = syscall;
    return error;
  };
  const validateAccessMode = (mode) => {
    if (mode == null) return constants.F_OK;
    if (typeof mode !== "number" || !Number.isFinite(mode)) {
      const error = new TypeError("ERR_INVALID_ARG_TYPE: access mode must be a finite number");
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    if (mode < 0 || mode > 7) {
      const error = new RangeError("ERR_OUT_OF_RANGE: access mode must be between 0 and 7");
      error.code = "ERR_OUT_OF_RANGE";
      throw error;
    }
    return mode;
  };
  const lookupDescriptor = (fd, syscall = "operation") => {
    const descriptor = descriptorState.get(Number(fd));
    if (!descriptor || descriptor.closed) {
      throw makeBadFdError(syscall);
    }
    return descriptor;
  };
  const unwrapFileHandle = (value, syscall = "operation") => {
    if (value && value.__terraceKind === "FileHandle") {
      const state = value.__terraceState;
      if (!state || state.closed || state.fd < 0) {
        throw makeBadFdError(syscall);
      }
      return state;
    }
    return null;
  };
  const fdFromValue = (value, syscall = "operation") => {
    if (typeof value === "number") return Number(value);
    const state = unwrapFileHandle(value, syscall);
    return state ? state.fd : null;
  };
  const registerDescriptor = (fd, descriptor) => {
    descriptorState.set(Number(fd), {
      fd: Number(fd),
      path: descriptor.path,
      append: !!descriptor.append,
      closed: false,
    });
  };
  const closeDescriptor = (fd) => {
    const descriptor = descriptorState.get(Number(fd));
    if (descriptor) descriptor.closed = true;
    descriptorState.delete(Number(fd));
  };
  const emitCallback = (callback, ...values) => {
    queueMicrotask(() => callback(...values));
  };
  const ensureStats = (path, syscall = "stat") => {
    const normalized = normalizePath(path);
    const stat = __terraceFsStat(normalized);
    if (!stat) {
      const error = new Error(`ENOENT: no such file or directory, ${syscall} '${path}'`);
      error.code = "ENOENT";
      error.syscall = syscall;
      error.path = String(path);
      throw error;
    }
    return __terraceMakeStats(stat);
  };
  const ensureParentSync = (path) => {
    const normalized = normalizePath(path);
    const parent = pathApi.dirname(normalized);
    if (!parent || parent === normalized || parent === ".") return;
    if (!__terraceFsStat(parent)) {
      ensureParentSync(parent);
      try {
        __terraceFsMkdir(parent);
      } catch (error) {
        if (!__terraceFsStat(parent)) {
          throw __terraceNormalizeFsError(error, parent, "mkdir");
        }
      }
    }
  };
  const readAllFromFd = (fd) => TerraceBuffer.from(__terraceFsReadFd(Number(fd), 0x7fffffff, 0));
  const openSync = (path, flags = "r", mode = 0o666) => {
    const normalized = normalizePath(path);
    try {
      const resolvedFlags = normalizeOpenFlags(flags, "r");
      const fd = __terraceFsOpen(normalized, resolvedFlags, mode);
      registerDescriptor(fd, {
        path: normalized,
        append:
          typeof resolvedFlags === "string"
            ? resolvedFlags.includes("a")
            : (Number(resolvedFlags) & constants.O_APPEND) !== 0,
      });
      return fd;
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "open");
    }
  };
  const open = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [path, flags = "r", mode = 0o666] = args;
    const invoke = () => openSync(path, flags, mode);
    if (callback) {
      try {
        emitCallback(callback, null, invoke());
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return invoke();
  };
  const closeSync = (fdOrHandle) => {
    const handleState = unwrapFileHandle(fdOrHandle, "close");
    const fd = handleState ? handleState.fd : Number(fdOrHandle);
    __terraceFsClose(Number(fd));
    closeDescriptor(fd);
    if (handleState) {
      handleState.closed = true;
      handleState.fd = -1;
    }
  };
  const close = (...args) => __terraceMaybeCallback(args, () => closeSync(args[0]));
  const readSync = (fd, buffer, offset = 0, length = buffer.byteLength - offset, position = null) => {
    const resolvedFd = fdFromValue(fd, "read");
    const chunk = __terraceFsReadFd(Number(resolvedFd), Number(length), position == null ? null : Number(position));
    buffer.set(chunk, Number(offset));
    return chunk.byteLength;
  };
  const read = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [fd, buffer, offset = 0, length = buffer.byteLength - offset, position = null] = args;
    const invoke = () => {
      const bytesRead = readSync(fd, buffer, offset, length, position);
      return { bytesRead, buffer };
    };
    if (callback) {
      try {
        const { bytesRead, buffer: outBuffer } = invoke();
        emitCallback(callback, null, bytesRead, outBuffer);
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return invoke();
  };
  const normalizeWriteInput = (bufferOrString, offsetOrEncoding, length, position) => {
    if (typeof bufferOrString === "string") {
      const resolvedPosition =
        typeof offsetOrEncoding === "number"
          ? offsetOrEncoding
          : typeof length === "number"
            ? length
            : position;
      const encoding =
        typeof offsetOrEncoding === "string"
          ? offsetOrEncoding
          : typeof position === "string"
            ? position
            : typeof length === "string"
              ? length
              : "utf8";
      const data = TerraceBuffer.from(bufferOrString, encoding);
      return { data, position: resolvedPosition };
    }
    const data = TerraceBuffer.from(bufferOrString);
    const offset = Number(offsetOrEncoding || 0);
    const span = length == null ? Math.max(0, data.byteLength - offset) : Number(length);
    return { data: data.slice(offset, offset + span), position };
  };
  const writeSync = (fd, bufferOrString, offsetOrEncoding = 0, length = undefined, position = null) => {
    const resolvedFd = fdFromValue(fd, "write");
    const { data, position: resolvedPosition } =
      normalizeWriteInput(bufferOrString, offsetOrEncoding, length, position);
    return __terraceFsWriteFd(Number(resolvedFd), data, resolvedPosition == null ? null : Number(resolvedPosition));
  };
  const write = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [fd, bufferOrString, offsetOrEncoding = 0, length = undefined, position = null] = args;
    const invoke = () => {
      const bytesWritten = writeSync(fd, bufferOrString, offsetOrEncoding, length, position);
      return { bytesWritten, buffer: bufferOrString };
    };
    if (callback) {
      try {
        const { bytesWritten, buffer } = invoke();
        emitCallback(callback, null, bytesWritten, buffer);
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return invoke();
  };
  const writevSync = (fd, buffers, position = null) => {
    let total = 0;
    for (const buffer of buffers || []) {
      total += writeSync(fd, buffer, 0, buffer.byteLength, position == null ? null : total + Number(position));
    }
    return total;
  };
  const writev = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [fd, buffers, position = null] = args;
    const invoke = () => {
      const bytesWritten = writevSync(fd, buffers, position);
      return { bytesWritten, buffers };
    };
    if (callback) {
      try {
        const { bytesWritten, buffers: outBuffers } = invoke();
        emitCallback(callback, null, bytesWritten, outBuffers);
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return invoke();
  };
  const readvSync = (fd, buffers, position = null) => {
    let total = 0;
    for (const buffer of buffers || []) {
      total += readSync(fd, buffer, 0, buffer.byteLength, position == null ? null : total + Number(position));
    }
    return total;
  };
  const readv = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [fd, buffers, position = null] = args;
    const invoke = () => {
      const bytesRead = readvSync(fd, buffers, position);
      return { bytesRead, buffers };
    };
    if (callback) {
      try {
        const { bytesRead, buffers: outBuffers } = invoke();
        emitCallback(callback, null, bytesRead, outBuffers);
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return invoke();
  };
  const readFileSync = (path, options = undefined) => {
    const fd = fdFromValue(path, "read");
    try {
      const bytes = fd == null
        ? (() => {
            const opened = openSync(path, "r");
            try {
              return readAllFromFd(opened);
            } finally {
              closeSync(opened);
            }
          })()
        : readAllFromFd(fd);
      return toReturnValue(bytes, options);
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, fd == null ? "open" : "read");
    }
  };
  const readFile = (...args) => __terraceMaybeCallback(args, () => {
    const [path, options] = args;
    return readFileSync(path, options);
  });
  const writeFileSync = (path, data, options = undefined) => {
    const bytes = toBuffer(data, normalizeEncoding(options, "utf8"));
    const flush = normalizeFlushOption(options);
    const fd = fdFromValue(path, "write");
    try {
      if (fd != null) {
        __terraceFsTruncateFd(fd, 0);
        writeSync(fd, bytes, 0, bytes.byteLength, null);
        if (flush) __terraceFsSingleton.fsyncSync(fd);
        return;
      }
      ensureParentSync(path);
      const flag = normalizeWriteFlag(options, "w");
      const opened = openSync(path, flag);
      try {
        writeSync(opened, bytes, 0, bytes.byteLength, null);
        if (flush) __terraceFsSingleton.fsyncSync(opened);
      } finally {
        closeSync(opened);
      }
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "open");
    }
  };
  const writeFile = (...args) => __terraceMaybeCallback(args, () => {
    const [path, data, options] = args;
    return writeFileSync(path, data, options);
  });
  const appendFileSync = (path, data, options = undefined) => {
    const bytes = toBuffer(data, normalizeEncoding(options, "utf8"));
    const flush = normalizeFlushOption(options);
    const fd = fdFromValue(path, "write");
    if (fd != null) {
      const descriptor = lookupDescriptor(fd, "write");
      const position = Number(statSync(descriptor.path).size ?? 0);
      writeSync(fd, bytes, 0, bytes.byteLength, position);
      if (flush) __terraceFsSingleton.fsyncSync(fd);
      return;
    }
    ensureParentSync(path);
    const opened = openSync(path, normalizeWriteFlag(options, "a"));
    try {
      writeSync(opened, bytes, 0, bytes.byteLength, null);
      if (flush) __terraceFsSingleton.fsyncSync(opened);
    } finally {
      closeSync(opened);
    }
  };
  const appendFile = (...args) => __terraceMaybeCallback(args, () => appendFileSync(args[0], args[1], args[2]));
  const mkdirSync = (path, options = {}) => {
    const normalized = normalizePath(path);
    if (options && options.recursive) {
      if (normalized === "/" || normalized === ".") return normalized;
      ensureParentSync(normalized);
      if (!__terraceFsStat(normalized)) {
        try {
          __terraceFsMkdir(normalized);
        } catch (error) {
          if (!__terraceFsStat(normalized)) {
            throw __terraceNormalizeFsError(error, path, "mkdir");
          }
        }
      }
      return normalized;
    }
    try {
      __terraceFsMkdir(normalized);
      return undefined;
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "mkdir");
    }
  };
  const mkdir = (...args) => __terraceMaybeCallback(args, () => mkdirSync(args[0], args[1]));
  const mkdtempSync = (prefix, options = undefined) => {
    const suffix = `${__terraceMkdtempCounter++}`.padStart(6, "0");
    const path = `${normalizePath(prefix)}${suffix}`;
    mkdirSync(path, { recursive: true });
    return options && options.encoding ? String(path) : path;
  };
  const mkdtemp = (...args) => __terraceMaybeCallback(args, () => mkdtempSync(args[0], args[1]));
  const mkdtempDisposableSync = (prefix, options = undefined) => {
    const path = mkdtempSync(prefix, options);
    const remove = () => rmSync(path, { recursive: true, force: true });
    return {
      path,
      remove,
      [Symbol.dispose]: remove,
    };
  };
  const readdirSync = (path, options = undefined) => {
    const normalized = normalizePath(path);
    const currentStats = __terraceFsStat(normalized);
    if (currentStats && currentStats.kind !== "directory") {
      throw makeFsError("ENOTDIR", "scandir", normalized, `ENOTDIR: not a directory, scandir '${normalized}'`);
    }
    const entries = __terraceFsReaddir(normalized).map((entry) => {
      return new TerraceDirent(entry.name, entry.kind || "file");
    });
    if (options && options.withFileTypes) {
      return entries;
    }
    return entries.map((entry) => entry.name);
  };
  const readdir = (...args) => __terraceMaybeCallback(args, () => readdirSync(args[0], typeof args[1] === "function" ? undefined : args[1]));
  const statSync = (path) => ensureStats(path, "stat");
  const stat = (...args) => __terraceMaybeCallback(args, () => statSync(args[0]));
  const lstatSync = (path) => {
    const normalized = normalizePath(path);
    const stat = __terraceFsLstat(normalized);
    if (!stat) {
      const error = new Error(`ENOENT: no such file or directory, lstat '${path}'`);
      error.code = "ENOENT";
      error.syscall = "lstat";
      error.path = String(path);
      throw error;
    }
    return __terraceMakeStats(stat);
  };
  const lstat = (...args) => __terraceMaybeCallback(args, () => lstatSync(args[0]));
  const existsSync = (path) => {
    try {
      return !!statSync(path);
    } catch {
      return false;
    }
  };
  const exists = (path, callback) => {
    if (typeof callback !== "function") {
      const error = new TypeError("ERR_INVALID_ARG_TYPE: fs.exists requires a callback");
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    emitCallback(callback, existsSync(path));
  };
  const accessSync = (path, _mode = constants.F_OK) => {
    validateAccessMode(_mode);
    if (!existsSync(path)) {
      const error = new Error(`ENOENT: no such file or directory, access '${path}'`);
      error.code = "ENOENT";
      error.path = String(path);
      error.syscall = "access";
      throw error;
    }
  };
  const access = (...args) => __terraceMaybeCallback(args, () => accessSync(args[0], args[1]));
  const realpathSync = (path) => {
    try {
      return __terraceFsRealpath(normalizePath(path));
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "realpath");
    }
  };
  const realpath = (...args) => __terraceMaybeCallback(args, () => realpathSync(args[0]));
  const rmSync = (path, options = {}) => {
    const normalized = normalizePath(path);
    const stat = __terraceFsStat(normalized);
    const lstat = __terraceFsLstat(normalized);
    if (!lstat) {
      if (options && options.force) return;
      const error = new Error(`ENOENT: no such file or directory, unlink '${path}'`);
      error.code = "ENOENT";
      throw error;
    }
    if ((stat && stat.kind === "directory") && options && options.recursive) {
      for (const child of readdirSync(normalized, { withFileTypes: true })) {
        rmSync(pathApi.join(normalized, child.name), { recursive: true, force: options.force });
      }
    }
    try {
      __terraceFsUnlink(normalized);
    } catch (error) {
      const normalizedError = __terraceNormalizeFsError(error, path, "unlink");
      if (options && options.force && normalizedError && normalizedError.code === "ENOENT") {
        return;
      }
      throw normalizedError;
    }
  };
  const rm = (...args) => __terraceMaybeCallback(args, () => rmSync(args[0], args[1]));
  const unlinkSync = (path) => rmSync(path, {});
  const unlink = (...args) => __terraceMaybeCallback(args, () => unlinkSync(args[0]));
  const rmdirSync = (path, options = {}) => rmSync(path, { recursive: !!options.recursive, force: !!options.force });
  const rmdir = (...args) => __terraceMaybeCallback(args, () => rmdirSync(args[0], args[1] ?? {}));
  const renameSync = (from, to) => {
    try {
      ensureParentSync(to);
      __terraceFsRename(normalizePath(from), normalizePath(to));
    } catch (error) {
      throw __terraceNormalizeFsError(error, from, "rename");
    }
  };
  const rename = (...args) => __terraceMaybeCallback(args, () => renameSync(args[0], args[1]));
  const copyFileSync = (from, to) => {
    const data = readFileSync(from);
    writeFileSync(to, data);
  };
  const copyFile = (...args) => __terraceMaybeCallback(args, () => copyFileSync(args[0], args[1]));
  const cpSync = (from, to, options = {}) => {
    const sourceStat = lstatSync(from);
    if (sourceStat.isDirectory()) {
      mkdirSync(to, { recursive: true });
      for (const entry of readdirSync(from, { withFileTypes: true })) {
        cpSync(pathApi.join(normalizePath(from), entry.name), pathApi.join(normalizePath(to), entry.name), options);
      }
      return;
    }
    if (sourceStat.isSymbolicLink()) {
      symlinkSync(readlinkSync(from), to);
      return;
    }
    copyFileSync(from, to);
  };
  const cp = (...args) => __terraceMaybeCallback(args, () => cpSync(args[0], args[1], args[2] ?? {}));
  const truncateSync = (pathOrFd, len = 0) => {
    const fd = fdFromValue(pathOrFd, "truncate");
    if (fd != null) {
      __terraceFsTruncateFd(fd, len);
      return;
    }
    const data = TerraceBuffer.from(readFileSync(pathOrFd));
    if (len < data.byteLength) {
      writeFileSync(pathOrFd, data.slice(0, len));
    } else if (len > data.byteLength) {
      writeFileSync(pathOrFd, TerraceBuffer.from([...data, ...new Array(len - data.byteLength).fill(0)]));
    }
  };
  const truncate = (...args) => __terraceMaybeCallback(args, () => truncateSync(args[0], args[1] ?? 0));
  const ftruncateSync = (fd, len = 0) => truncateSync(fd, len);
  const ftruncate = (...args) => __terraceMaybeCallback(args, () => ftruncateSync(args[0], args[1] ?? 0));
  const chmodSync = () => undefined;
  const chmod = (...args) => __terraceMaybeCallback(args, () => chmodSync(...args));
  const fchmodSync = () => undefined;
  const fchmod = (...args) => __terraceMaybeCallback(args, () => fchmodSync(...args));
  const lchmodSync = () => undefined;
  const lchmod = (...args) => __terraceMaybeCallback(args, () => lchmodSync(...args));
  const chownSync = () => undefined;
  const chown = (...args) => __terraceMaybeCallback(args, () => chownSync(...args));
  const fchownSync = () => undefined;
  const fchown = (...args) => __terraceMaybeCallback(args, () => fchownSync(...args));
  const lchownSync = () => undefined;
  const lchown = (...args) => __terraceMaybeCallback(args, () => lchownSync(...args));
  const utimesSync = () => undefined;
  const utimes = (...args) => __terraceMaybeCallback(args, () => utimesSync(...args));
  const futimesSync = () => undefined;
  const futimes = (...args) => __terraceMaybeCallback(args, () => futimesSync(...args));
  const lutimesSync = () => undefined;
  const lutimes = (...args) => __terraceMaybeCallback(args, () => lutimesSync(...args));
  const fsyncSync = () => undefined;
  const fsync = (...args) => __terraceMaybeCallback(args, () => fsyncSync(...args));
  const fdatasyncSync = () => undefined;
  const fdatasync = (...args) => __terraceMaybeCallback(args, () => fdatasyncSync(...args));
  const fstatSync = (fd) => {
    const descriptor = lookupDescriptor(fd, "fstat");
    return statSync(descriptor.path);
  };
  const fstat = (...args) => __terraceMaybeCallback(args, () => fstatSync(args[0]));
  const statfsSync = (_path) => ({
    type: 0,
    bsize: 4096,
    blocks: 0,
    bfree: 0,
    bavail: 0,
    files: 0,
    ffree: 0,
  });
  const statfs = (...args) => __terraceMaybeCallback(args, () => statfsSync(args[0]));
  const readlinkSync = (path) => {
    try {
      return __terraceFsReadlink(normalizePath(path));
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "readlink");
    }
  };
  const readlink = (...args) => __terraceMaybeCallback(args, () => readlinkSync(args[0]));
  const symlinkSync = (target, path) => {
    const normalized = normalizePath(path);
    ensureParentSync(normalized);
    try {
      __terraceFsSymlink(String(target), normalized);
    } catch (error) {
      throw __terraceNormalizeFsError(error, path, "symlink");
    }
  };
  const symlink = (...args) => __terraceMaybeCallback(args, () => symlinkSync(args[0], args[1]));
  const linkSync = (existingPath, newPath) => {
    const existing = normalizePath(existingPath);
    const target = normalizePath(newPath);
    ensureParentSync(target);
    try {
      __terraceFsLink(existing, target);
    } catch (error) {
      throw __terraceNormalizeFsError(error, newPath, "link");
    }
  };
  const link = (...args) => __terraceMaybeCallback(args, () => linkSync(args[0], args[1]));

  class TerraceDir {
    constructor(path, options = {}) {
      this.path = normalizePath(path);
      this._entries = readdirSync(this.path, { withFileTypes: true, ...options });
      this._index = 0;
      this._closed = false;
    }
    readSync() {
      if (this._closed) throw makeBadFdError("scandir");
      return this._entries[this._index++] ?? null;
    }
    read(callback) {
      if (typeof callback === "function") {
        try {
          emitCallback(callback, null, this.readSync());
        } catch (error) {
          emitCallback(callback, error);
        }
        return;
      }
      return Promise.resolve(this.readSync());
    }
    closeSync() {
      this._closed = true;
    }
    close(callback) {
      if (typeof callback === "function") {
        emitCallback(callback, null);
        return;
      }
      this.closeSync();
      return Promise.resolve();
    }
    async *entries() {
      let entry;
      while ((entry = this.readSync()) != null) {
        yield entry;
      }
    }
    [Symbol.asyncIterator]() {
      return this.entries();
    }
  }
  const opendirSync = (path, options = undefined) => new TerraceDir(path, options);
  const opendir = (...args) => __terraceMaybeCallback(args, () => opendirSync(args[0], args[1]));

  class TerraceFsReadStream extends streamApi.Readable {
    constructor(path, options = {}) {
      super();
      this.path = normalizePath(path);
      this.fd = options.fd ?? openSync(this.path, options.flags || "r");
      queueMicrotask(() => {
        this.emit("open", this.fd);
        let data = TerraceBuffer.from(readFileSync(this.fd));
        const start = Number(options.start ?? 0);
        const end = options.end == null ? data.byteLength : Number(options.end) + 1;
        data = data.subarray(start, end);
        this.emit("data", data);
        this.emit("end");
        if (options.autoClose !== false) closeSync(this.fd);
        this.emit("close");
      });
    }
  }
  class TerraceFsWriteStream extends streamApi.Writable {
    constructor(path, options = {}) {
      super();
      this.path = normalizePath(path);
      this.fd = options.fd ?? openSync(this.path, options.flags || "w");
      this._options = options;
      this._opened = false;
      queueMicrotask(() => {
        if (this._opened) return;
        this._opened = true;
        this.emit("open", this.fd);
      });
    }
    write(chunk, encoding, callback) {
      if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      writeSync(this.fd, typeof chunk === "string" ? TerraceBuffer.from(chunk, encoding) : chunk);
      if (typeof callback === "function") callback();
      return true;
    }
    end(chunk, encoding, callback) {
      if (chunk != null) {
        this.write(chunk, encoding);
      } else if (typeof encoding === "function") {
        callback = encoding;
      }
      if (!this._opened) {
        this._opened = true;
        this.emit("open", this.fd);
      }
      if (this._options.autoClose !== false) closeSync(this.fd);
      if (typeof callback === "function") callback();
      this.emit("finish");
      this.emit("close");
    }
  }
  const createReadStream = (path, options = {}) => new TerraceFsReadStream(path, options);
  const createWriteStream = (path, options = {}) => new TerraceFsWriteStream(path, options);
  const watch = (path, options, listener) => {
    const emitter = new (__terraceEventsModule())();
    if (typeof options === "function") {
      listener = options;
      options = {};
    }
    if (typeof listener === "function") emitter.on("change", listener);
    const normalized = normalizePath(path);
    watcherState.set(normalized, watcherState.get(normalized) || new Set());
    watcherState.get(normalized).add(emitter);
    emitter.close = () => {
      watcherState.get(normalized)?.delete(emitter);
    };
    return emitter;
  };
  const watchFile = (path, options, listener) => {
    if (typeof options === "function") {
      listener = options;
    }
    if (typeof listener === "function") {
      const normalized = normalizePath(path);
      watcherState.set(normalized, watcherState.get(normalized) || new Set());
      watcherState.get(normalized).add(listener);
    }
  };
  const unwatchFile = (path, listener) => {
    const normalized = normalizePath(path);
    if (!watcherState.has(normalized)) return;
    if (!listener) {
      watcherState.delete(normalized);
      return;
    }
    watcherState.get(normalized)?.delete(listener);
  };
  const collectRecursive = (root) => {
    const out = [];
    const visit = (current) => {
      if (!existsSync(current)) return;
      out.push(current);
      const stat = lstatSync(current);
      if (!stat.isDirectory()) return;
      for (const entry of readdirSync(current, { withFileTypes: true })) {
        visit(pathApi.join(current, entry.name));
      }
    };
    visit(root);
    return out;
  };
  const globPatternToRegex = (pattern) => {
    const escaped = normalizePath(pattern)
      .replace(/[.+^${}()|[\]\\]/g, "\\$&")
      .replace(/\\\*\\\*/g, ".*")
      .replace(/\\\*/g, "[^/]*")
      .replace(/\\\?/g, ".");
    return new RegExp(`^${escaped}$`);
  };
  const globSync = (pattern, options = {}) => {
    const normalized = normalizePath(pattern);
    if (!/[*?]/.test(normalized)) {
      return existsSync(normalized) ? [normalized] : [];
    }
    const wildcardIndex = normalized.search(/[*?]/);
    const prefix = wildcardIndex === -1 ? normalized : normalized.slice(0, normalized.lastIndexOf("/", wildcardIndex));
    const root = prefix && prefix.length > 0 ? prefix : "/";
    const regex = globPatternToRegex(normalized);
    const matches = collectRecursive(root).filter((entry) => regex.test(entry));
    return options.withFileTypes
      ? matches.map((entry) => new TerraceDirent(pathApi.basename(entry), lstatSync(entry).kind))
      : matches;
  };
  const glob = (...args) => {
    const callback = typeof args[args.length - 1] === "function" ? args.pop() : null;
    const [pattern, options = {}] = args;
    if (callback) {
      try {
        emitCallback(callback, null, globSync(pattern, options));
      } catch (error) {
        emitCallback(callback, error);
      }
      return;
    }
    return {
      async *[Symbol.asyncIterator]() {
        for (const entry of globSync(pattern, options)) {
          yield entry;
        }
      },
    };
  };
  const openAsBlob = (path) => {
    const bytes = readFileSync(path);
    if (typeof Blob === "function") {
      return new Blob([bytes]);
    }
    return {
      size: bytes.byteLength,
      async arrayBuffer() {
        return TerraceBuffer.from(bytes).buffer.slice(0);
      },
      async text() {
        return TerraceBuffer.from(bytes).toString("utf8");
      },
      stream() {
        return createReadStream(path);
      },
    };
  };
  const createFileHandle = (path, flags = "r", mode = 0o666) => {
    const fd = openSync(path, flags, mode);
    const state = {
      fd,
      path: normalizePath(path),
      closed: false,
    };
    const ensureOpen = (syscall) => {
      if (state.closed || state.fd < 0) throw makeBadFdError(syscall);
      return state.fd;
    };
    const handle = {
      __terraceKind: "FileHandle",
      __terraceState: state,
      get fd() {
        return state.fd;
      },
      appendFile(data, options) {
        ensureOpen("write");
        return Promise.resolve(appendFileSync(handle, data, options));
      },
      chmod(_mode) {
        ensureOpen("fchmod");
        return Promise.resolve();
      },
      chown(_uid, _gid) {
        ensureOpen("fchown");
        return Promise.resolve();
      },
      createReadStream(options = {}) {
        ensureOpen("read");
        return createReadStream(state.path, { ...options, fd: state.fd, autoClose: false });
      },
      createWriteStream(options = {}) {
        ensureOpen("write");
        return createWriteStream(state.path, { ...options, fd: state.fd, autoClose: false });
      },
      datasync() {
        ensureOpen("fdatasync");
        return Promise.resolve();
      },
      getAsyncId() {
        return 0;
      },
      read(buffer, offset = 0, length = buffer.byteLength - offset, position = null) {
        ensureOpen("read");
        return Promise.resolve(read(handle, buffer, offset, length, position));
      },
      async *readLines() {
        const text = await handle.readFile("utf8");
        for (const line of String(text).split(/\r?\n/)) {
          yield line;
        }
      },
      readFile(options) {
        ensureOpen("read");
        return Promise.resolve(readFileSync(handle, options));
      },
      readableWebStream() {
        ensureOpen("read");
        if (typeof ReadableStream === "function") {
          const bytes = readFileSync(handle);
          return new ReadableStream({
            start(controller) {
              controller.enqueue(bytes);
              controller.close();
            },
          });
        }
        return createReadStream(state.path, { fd: state.fd, autoClose: false });
      },
      readv(buffers, position = null) {
        ensureOpen("read");
        return Promise.resolve({ bytesRead: readvSync(handle, buffers, position), buffers });
      },
      stat() {
        ensureOpen("fstat");
        return Promise.resolve(fstatSync(handle));
      },
      sync() {
        ensureOpen("fsync");
        return Promise.resolve();
      },
      truncate(len = 0) {
        ensureOpen("ftruncate");
        return Promise.resolve(ftruncateSync(handle, len));
      },
      utimes(_atime, _mtime) {
        ensureOpen("futimes");
        return Promise.resolve();
      },
      write(bufferOrString, offsetOrEncoding = 0, length = undefined, position = null) {
        ensureOpen("write");
        const bytesWritten = writeSync(handle, bufferOrString, offsetOrEncoding, length, position);
        return Promise.resolve({ bytesWritten, buffer: bufferOrString });
      },
      async writeFile(data, options = undefined) {
        ensureOpen("write");
        const encoding = normalizeEncoding(options, "utf8");
        if (data && typeof data[Symbol.asyncIterator] === "function") {
          for await (const chunk of data) {
            appendFileSync(handle, toBuffer(chunk, encoding));
          }
          return;
        }
        if (data && typeof data[Symbol.iterator] === "function" && typeof data !== "string" && !TerraceBuffer.isBuffer(data) && !(data instanceof Uint8Array)) {
          for (const chunk of data) {
            appendFileSync(handle, toBuffer(chunk, encoding));
          }
          return;
        }
        return Promise.resolve(writeFileSync(handle, data, options));
      },
      writev(buffers, position = null) {
        ensureOpen("write");
        return Promise.resolve({ bytesWritten: writevSync(handle, buffers, position), buffers });
      },
      close() {
        if (state.closed || state.fd < 0) return Promise.resolve();
        closeSync(handle);
        return Promise.resolve();
      },
    };
    return handle;
  };

  const promises = {
    access: __terraceFsPromise((path, mode) => accessSync(path, mode)),
    appendFile: __terraceFsPromise((path, data, options) => appendFileSync(path, data, options)),
    chmod: __terraceFsPromise((...args) => chmodSync(...args)),
    chown: __terraceFsPromise((...args) => chownSync(...args)),
    copyFile: __terraceFsPromise((from, to) => copyFileSync(from, to)),
    cp: __terraceFsPromise((from, to, options = {}) => cpSync(from, to, options)),
    glob(pattern, options = {}) {
      const entries = globSync(pattern, options);
      return {
        async *[Symbol.asyncIterator]() {
          for (const entry of entries) {
            yield entry;
          }
        },
      };
    },
    lchmod: __terraceFsPromise((...args) => lchmodSync(...args)),
    lchown: __terraceFsPromise((...args) => lchownSync(...args)),
    link: __terraceFsPromise((from, to) => linkSync(from, to)),
    lstat: __terraceFsPromise((path) => lstatSync(path)),
    lutimes: __terraceFsPromise((...args) => lutimesSync(...args)),
    mkdir: __terraceFsPromise((path, options) => mkdirSync(path, options)),
    mkdtemp: __terraceFsPromise((prefix, options) => mkdtempSync(prefix, options)),
    mkdtempDisposable: __terraceFsPromise((prefix, options) => mkdtempDisposableSync(prefix, options)),
    open: __terraceFsPromise((path, flags = "r", mode = 0o666) => createFileHandle(path, flags, mode)),
    opendir: __terraceFsPromise((path, options) => opendirSync(path, options)),
    readFile: __terraceFsPromise((path, options) => readFileSync(path, options)),
    readdir: __terraceFsPromise((path, options) => readdirSync(path, options)),
    readlink: __terraceFsPromise((path) => readlinkSync(path)),
    realpath: __terraceFsPromise((path) => realpathSync(path)),
    rename: __terraceFsPromise((from, to) => renameSync(from, to)),
    rm: __terraceFsPromise((path, options) => rmSync(path, options)),
    rmdir: __terraceFsPromise((path, options = {}) => rmdirSync(path, options)),
    stat: __terraceFsPromise((path) => statSync(path)),
    statfs: __terraceFsPromise((path) => statfsSync(path)),
    symlink: __terraceFsPromise((target, path) => symlinkSync(target, path)),
    truncate: __terraceFsPromise((path, len = 0) => truncateSync(path, len)),
    unlink: __terraceFsPromise((path) => unlinkSync(path)),
    watch(path, options = {}) {
      return {
        async *[Symbol.asyncIterator]() {
          watch(path, options);
        },
      };
    },
    utimes: __terraceFsPromise((...args) => utimesSync(...args)),
    writeFile: __terraceFsPromise((path, data, options) => writeFileSync(path, data, options)),
    constants,
  };

  const api = {
    Dir: TerraceDir,
    Dirent: TerraceDirent,
    Stats: TerraceStats,
    FileReadStream: TerraceFsReadStream,
    FileWriteStream: TerraceFsWriteStream,
    ReadStream: TerraceFsReadStream,
    WriteStream: TerraceFsWriteStream,
    Utf8Stream: streamApi.Transform,
    _toUnixTimestamp(value) {
      return value instanceof Date ? Math.floor(value.getTime() / 1000) : Number(value);
    },
    accessSync,
    access,
    appendFileSync,
    appendFile,
    chmodSync,
    chmod,
    chownSync,
    chown,
    cpSync,
    cp,
    closeSync,
    close,
    copyFileSync,
    copyFile,
    createReadStream,
    createWriteStream,
    exists,
    existsSync,
    fchmodSync,
    fchmod,
    fchownSync,
    fchown,
    fdatasyncSync,
    fdatasync,
    fstatSync,
    fstat,
    ftruncateSync,
    ftruncate,
    futimesSync,
    futimes,
    fsyncSync,
    fsync,
    globSync,
    glob,
    lchmodSync,
    lchmod,
    lchownSync,
    lchown,
    linkSync,
    link,
    lstatSync,
    lstat,
    lutimesSync,
    lutimes,
    mkdirSync,
    mkdir,
    mkdtempDisposableSync,
    mkdtempSync,
    mkdtemp,
    openAsBlob,
    openSync,
    open,
    opendirSync,
    opendir,
    promises,
    readSync,
    read,
    readvSync,
    readv,
    readFileSync,
    readFile,
    readdirSync,
    readdir,
    readlinkSync,
    readlink,
    realpathSync,
    realpath,
    renameSync,
    rename,
    rmSync,
    rm,
    rmdirSync,
    rmdir,
    statfsSync,
    statfs,
    statSync,
    stat,
    symlinkSync,
    symlink,
    truncate,
    truncateSync,
    unlinkSync,
    unlink,
    unwatchFile,
    utimesSync,
    utimes,
    watch,
    watchFile,
    writeSync,
    write,
    writevSync,
    writev,
    writeFileSync,
    writeFile,
    constants,
  };
  api.realpathSync.native = realpathSync;
  api.realpath.native = realpath;
  Object.defineProperty(api, "default", { value: api, enumerable: false });
  __terraceFsSingleton = api;
  return api;
}

function __terraceFsPromisesModule() {
  return __terraceFsModule().promises;
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

function __terraceValidateBoolean(value, name) {
  if (typeof value !== "boolean") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type boolean.`,
    );
  }
}

function __terraceValidateString(value, name) {
  if (typeof value !== "string") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type string.`,
    );
  }
}

function __terraceValidateObject(value, name) {
  if (value == null || typeof value !== "object") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type object.`,
    );
  }
}

const __terraceKnownSignals = new Set([
  "SIGABRT",
  "SIGALRM",
  "SIGBUS",
  "SIGCHLD",
  "SIGCONT",
  "SIGFPE",
  "SIGHUP",
  "SIGILL",
  "SIGINT",
  "SIGIO",
  "SIGIOT",
  "SIGKILL",
  "SIGPIPE",
  "SIGPOLL",
  "SIGPROF",
  "SIGPWR",
  "SIGQUIT",
  "SIGSEGV",
  "SIGSTKFLT",
  "SIGSTOP",
  "SIGSYS",
  "SIGTERM",
  "SIGTRAP",
  "SIGTSTP",
  "SIGTTIN",
  "SIGTTOU",
  "SIGURG",
  "SIGUSR1",
  "SIGUSR2",
  "SIGVTALRM",
  "SIGWINCH",
  "SIGXCPU",
  "SIGXFSZ",
]);

function __terraceValidateSignalName(value, name) {
  __terraceValidateString(value, name);
  if (value !== value.toUpperCase()) {
    const error = new TypeError(`Unknown signal: ${value} (signals must use all capital letters)`);
    error.code = "ERR_UNKNOWN_SIGNAL";
    throw error;
  }
  if (!__terraceKnownSignals.has(value)) {
    const error = new TypeError(`Unknown signal: ${value}`);
    error.code = "ERR_UNKNOWN_SIGNAL";
    throw error;
  }
}

function __terraceReportErrorProperties(err) {
  if (!err || typeof err !== "object") {
    return {};
  }
  const properties = {};
  for (const key of Object.keys(err)) {
    properties[key] = err[key];
  }
  return properties;
}

function __terraceBuildProcessReport(process, state, err) {
  const reportError = err === undefined ? new Error("synthetic report error") : err;
  const header = {
    event: "JavaScript API",
    trigger: "API",
    filename: state.filename,
    dumpEventTime: new Date(0).toISOString(),
    dumpEventTimeStamp: "0",
    processId: process.pid,
    cwd: process.cwd(),
    commandLine: [...process.argv],
    nodejsVersion: process.version,
    glibcVersionRuntime: process.platform === "linux" ? "2.39" : undefined,
    arch: process.arch,
    platform: process.platform,
    wordSize: 64,
  };
  const javascriptStack = {
    message: reportError?.message ?? String(reportError),
    stack: typeof reportError?.stack === "string"
      ? reportError.stack.split("\n")
      : [],
    errorProperties: __terraceReportErrorProperties(reportError),
  };
  const report = {
    header,
    javascriptStack,
    sharedObjects: process.platform === "linux"
      ? ["/lib/x86_64-linux-gnu/libc.so.6"]
      : [],
    libuv: [],
    resourceUsage: {},
    userLimits: {},
  };
  if (!state.excludeEnv) {
    report.environmentVariables = { ...process.env };
  }
  if (!state.excludeNetwork) {
    report.networkInterfaces = __terraceBuiltin("os").networkInterfaces();
  }
  return report;
}

function __terraceCreateProcessReport(process) {
  const state = {
    directory: "",
    filename: "",
    compact: true,
    excludeNetwork: false,
    excludeEnv: false,
    reportOnFatalError: true,
    reportOnSignal: true,
    reportOnUncaughtException: true,
    signal: "SIGUSR2",
  };
  const signalHandler = () => {};
  const syncSignalHandler = () => {
    process.off(state.signal, signalHandler);
    if (state.reportOnSignal) {
      process.on(state.signal, signalHandler);
    }
  };
  syncSignalHandler();
  return {
    writeReport(file, err) {
      if (typeof file === "object" && file !== null) {
        err = file;
        file = undefined;
      } else if (file !== undefined) {
        __terraceValidateString(file, "file");
      }
      if (err !== undefined) {
        __terraceValidateObject(err, "err");
      }
      const report = __terraceBuildProcessReport(process, state, err);
      const reportText = JSON.stringify(report, null, state.compact ? 0 : 2);
      const resolvedFile = file ?? state.filename;
      if (resolvedFile === "stdout") {
        process.stdout.write(`${reportText}\n`);
        return "stdout";
      }
      if (resolvedFile === "stderr") {
        process.stderr.write(`${reportText}\n`);
        return "stderr";
      }
      if (resolvedFile) {
        const target = state.directory && !resolvedFile.startsWith("/")
          ? __terraceBuiltin("path").join(state.directory, resolvedFile)
          : resolvedFile;
        __terraceFsModule().writeFileSync(target, `${reportText}\n`);
        return resolvedFile;
      }
      return reportText;
    },
    getReport(err) {
      if (err !== undefined) {
        __terraceValidateObject(err, "err");
      }
      return __terraceBuildProcessReport(process, state, err);
    },
    get directory() {
      return state.directory;
    },
    set directory(value) {
      __terraceValidateString(value, "directory");
      state.directory = value;
    },
    get filename() {
      return state.filename;
    },
    set filename(value) {
      __terraceValidateString(value, "filename");
      state.filename = value;
    },
    get compact() {
      return state.compact;
    },
    set compact(value) {
      __terraceValidateBoolean(value, "compact");
      state.compact = value;
    },
    get excludeNetwork() {
      return state.excludeNetwork;
    },
    set excludeNetwork(value) {
      __terraceValidateBoolean(value, "excludeNetwork");
      state.excludeNetwork = value;
    },
    get excludeEnv() {
      return state.excludeEnv;
    },
    set excludeEnv(value) {
      __terraceValidateBoolean(value, "excludeEnv");
      state.excludeEnv = value;
    },
    get reportOnFatalError() {
      return state.reportOnFatalError;
    },
    set reportOnFatalError(value) {
      __terraceValidateBoolean(value, "trigger");
      state.reportOnFatalError = value;
    },
    get reportOnSignal() {
      return state.reportOnSignal;
    },
    set reportOnSignal(value) {
      __terraceValidateBoolean(value, "trigger");
      state.reportOnSignal = value;
      syncSignalHandler();
    },
    get reportOnUncaughtException() {
      return state.reportOnUncaughtException;
    },
    set reportOnUncaughtException(value) {
      __terraceValidateBoolean(value, "trigger");
      state.reportOnUncaughtException = value;
    },
    get signal() {
      return state.signal;
    },
    set signal(value) {
      __terraceValidateSignalName(value, "signal");
      process.off(state.signal, signalHandler);
      state.signal = value;
      syncSignalHandler();
    },
  };
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
    pid: typeof info.pid === "number" ? info.pid : 1,
    ppid: typeof info.ppid === "number" ? info.ppid : 0,
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
      __terraceNextTickQueue.push(() => fn(...args));
    },
    emitWarning() {},
    on(event, listener) {
      emitter.on(event, listener);
      return process;
    },
    addListener(event, listener) {
      emitter.on(event, listener);
      return process;
    },
    prependListener(event, listener) {
      emitter.prependListener(event, listener);
      return process;
    },
    off(event, listener) {
      emitter.off(event, listener);
      return process;
    },
    removeListener(event, listener) {
      emitter.off(event, listener);
      return process;
    },
    removeAllListeners(event) {
      emitter.removeAllListeners(event);
      return process;
    },
    once(event, listener) {
      emitter.once(event, listener);
      return process;
    },
    prependOnceListener(event, listener) {
      emitter.prependOnceListener(event, listener);
      return process;
    },
    listenerCount(event) {
      return emitter.listenerCount(event);
    },
    listeners(event) {
      return emitter.listeners(event);
    },
    rawListeners(event) {
      return emitter.rawListeners(event);
    },
    getMaxListeners() {
      return emitter.getMaxListeners();
    },
    setMaxListeners(value) {
      emitter.setMaxListeners(value);
      return process;
    },
    eventNames() {
      return emitter.eventNames();
    },
    emit(event, ...args) {
      return emitter.emit(event, ...args);
    },
    kill(pid, signal = "SIGTERM") {
      const normalizedPid = Number(pid);
      __terraceValidateSignalName(signal, "signal");
      if (normalizedPid !== process.pid) {
        return true;
      }
      process.nextTick(() => {
        process.emit(signal, signal);
      });
      return true;
    },
    umask() {
      return 0o022;
    },
  };
  process[Symbol.toStringTag] = "process";
  process.report = __terraceCreateProcessReport(process);
  return process;
}

function __terraceDeepFreeze(value) {
  if (value == null || typeof value !== "object" || Object.isFrozen(value)) {
    return value;
  }
  Object.freeze(value);
  for (const key of Object.getOwnPropertyNames(value)) {
    __terraceDeepFreeze(value[key]);
  }
  return value;
}

function __terraceCloneSimple(value) {
  if (Array.isArray(value)) return value.map((entry) => __terraceCloneSimple(entry));
  if (value && typeof value === "object") {
    const copy = {};
    for (const [key, entry] of Object.entries(value)) {
      copy[key] = __terraceCloneSimple(entry);
    }
    return copy;
  }
  return value;
}

function __terraceDrainNextTicks() {
  let drained = 0;
  while (__terraceNextTickQueue.length > 0) {
    const batch = __terraceNextTickQueue.splice(0, __terraceNextTickQueue.length);
    for (const task of batch) {
      task();
      drained += 1;
    }
  }
  return drained;
}

function __terraceTimerIdFromHandle(handle) {
  if (typeof handle === "number") return handle;
  if (handle && typeof handle === "object" && typeof handle.__terraceTimerId === "number") {
    return handle.__terraceTimerId;
  }
  return null;
}

function __terraceCreateTimerHandle(id, kind) {
  return {
    __terraceTimerId: id,
    __terraceTimerKind: kind,
    __terraceRefed: true,
    ref() {
      this.__terraceRefed = true;
      return this;
    },
    unref() {
      this.__terraceRefed = false;
      return this;
    },
    hasRef() {
      return this.__terraceRefed;
    },
    refresh() {
      if (kind === "timeout") {
        const timer = __terraceTimeouts.get(id);
        if (timer) {
          timer.due = __terraceCurrentTimerTime + timer.delay;
        }
      }
      return this;
    },
    [Symbol.toPrimitive]() {
      return id;
    },
  };
}

function __terraceSetTimeout(fn, delay = 0, ...args) {
  if (typeof fn !== "function") {
    throw new TypeError("setTimeout callback must be a function");
  }
  const id = __terraceNextTimerId++;
  const normalizedDelay = Number.isFinite(Number(delay)) ? Math.max(Number(delay), 0) : 0;
  const handle = __terraceCreateTimerHandle(id, "timeout");
  __terraceTimeouts.set(id, {
    id,
    delay: normalizedDelay,
    due: __terraceCurrentTimerTime + normalizedDelay,
    callback: fn,
    args,
    handle,
  });
  return handle;
}

function __terraceClearTimeout(handle) {
  const id = __terraceTimerIdFromHandle(handle);
  if (id !== null) {
    __terraceTimeouts.delete(id);
  }
}

function __terraceSetImmediate(fn, ...args) {
  if (typeof fn !== "function") {
    throw new TypeError("setImmediate callback must be a function");
  }
  const id = __terraceNextTimerId++;
  const handle = __terraceCreateTimerHandle(id, "immediate");
  __terraceImmediateEntries.set(id, { id, callback: fn, args, handle });
  __terraceImmediateQueue.push(id);
  return handle;
}

function __terraceClearImmediate(handle) {
  const id = __terraceTimerIdFromHandle(handle);
  if (id !== null) {
    __terraceImmediateEntries.delete(id);
  }
}

function __terraceDrainTimers() {
  let drained = 0;
  if (__terraceImmediateQueue.length > 0) {
    const batch = __terraceImmediateQueue.splice(0, __terraceImmediateQueue.length);
    for (const id of batch) {
      const entry = __terraceImmediateEntries.get(id);
      if (!entry) continue;
      __terraceImmediateEntries.delete(id);
      entry.callback(...entry.args);
      drained += 1;
    }
    return drained;
  }
  if (__terraceTimeouts.size === 0) return 0;

  let nextDue = Infinity;
  for (const timer of __terraceTimeouts.values()) {
    if (timer.due < nextDue) nextDue = timer.due;
  }
  if (!Number.isFinite(nextDue)) return 0;
  __terraceCurrentTimerTime = nextDue;

  const due = [];
  for (const [id, timer] of __terraceTimeouts.entries()) {
    if (timer.due <= __terraceCurrentTimerTime) {
      due.push(id);
    }
  }
  for (const id of due) {
    const timer = __terraceTimeouts.get(id);
    if (!timer) continue;
    __terraceTimeouts.delete(id);
    timer.callback(...timer.args);
    drained += 1;
  }
  return drained;
}

function __terracePunycodeModule() {
  if (__terracePunycodeSingleton) return __terracePunycodeSingleton;

  const maxInt = 2147483647;
  const base = 36;
  const tMin = 1;
  const tMax = 26;
  const skew = 38;
  const damp = 700;
  const initialBias = 72;
  const initialN = 128;
  const delimiter = "-";
  const regexPunycode = /^xn--/;
  const regexNonASCII = /[^\0-\x7F]/;
  const regexSeparators = /[\x2E\u3002\uFF0E\uFF61]/g;
  const baseMinusTMin = base - tMin;
  const floor = Math.floor;
  const stringFromCharCode = String.fromCharCode;

  function punycodeError(type) {
    const messages = {
      overflow: "Overflow: input needs wider integers to process",
      "not-basic": "Illegal input >= 0x80 (not a basic code point)",
      "invalid-input": "Invalid input",
    };
    throw new RangeError(messages[type]);
  }

  function map(array, callback) {
    const result = [];
    let length = array.length;
    while (length--) result[length] = callback(array[length]);
    return result;
  }

  function mapDomain(domain, callback) {
    const parts = String(domain).split("@");
    let result = "";
    if (parts.length > 1) {
      result = `${parts[0]}@`;
      domain = parts[1];
    }
    domain = String(domain).replace(regexSeparators, "\x2E");
    return result + map(String(domain).split("."), callback).join(".");
  }

  function ucs2decode(string) {
    const output = [];
    let counter = 0;
    const length = string.length;
    while (counter < length) {
      const value = string.charCodeAt(counter++);
      if (value >= 0xD800 && value <= 0xDBFF && counter < length) {
        const extra = string.charCodeAt(counter++);
        if ((extra & 0xFC00) === 0xDC00) {
          output.push(((value & 0x3FF) << 10) + (extra & 0x3FF) + 0x10000);
        } else {
          output.push(value);
          counter--;
        }
      } else {
        output.push(value);
      }
    }
    return output;
  }

  const ucs2encode = (codePoints) => String.fromCodePoint(...codePoints);

  function basicToDigit(codePoint) {
    if (codePoint >= 0x30 && codePoint < 0x3A) return 26 + (codePoint - 0x30);
    if (codePoint >= 0x41 && codePoint < 0x5B) return codePoint - 0x41;
    if (codePoint >= 0x61 && codePoint < 0x7B) return codePoint - 0x61;
    return base;
  }

  function digitToBasic(digit, flag) {
    return digit + 22 + 75 * (digit < 26) - ((flag !== 0) << 5);
  }

  function adapt(delta, numPoints, firstTime) {
    let k = 0;
    delta = firstTime ? floor(delta / damp) : delta >> 1;
    delta += floor(delta / numPoints);
    for (; delta > (baseMinusTMin * tMax >> 1); k += base) {
      delta = floor(delta / baseMinusTMin);
    }
    return floor(k + (baseMinusTMin + 1) * delta / (delta + skew));
  }

  function decode(input) {
    const output = [];
    const inputLength = input.length;
    let i = 0;
    let n = initialN;
    let bias = initialBias;
    let basic = input.lastIndexOf(delimiter);
    if (basic < 0) basic = 0;
    for (let j = 0; j < basic; ++j) {
      if (input.charCodeAt(j) >= 0x80) punycodeError("not-basic");
      output.push(input.charCodeAt(j));
    }
    for (let index = basic > 0 ? basic + 1 : 0; index < inputLength;) {
      const oldi = i;
      for (let w = 1, k = base;; k += base) {
        if (index >= inputLength) punycodeError("invalid-input");
        const digit = basicToDigit(input.charCodeAt(index++));
        if (digit >= base) punycodeError("invalid-input");
        if (digit > floor((maxInt - i) / w)) punycodeError("overflow");
        i += digit * w;
        const t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);
        if (digit < t) break;
        const baseMinusT = base - t;
        if (w > floor(maxInt / baseMinusT)) punycodeError("overflow");
        w *= baseMinusT;
      }
      const out = output.length + 1;
      bias = adapt(i - oldi, out, oldi === 0);
      if (floor(i / out) > maxInt - n) punycodeError("overflow");
      n += floor(i / out);
      i %= out;
      output.splice(i++, 0, n);
    }
    return String.fromCodePoint(...output);
  }

  function encode(input) {
    const output = [];
    input = ucs2decode(String(input));
    const inputLength = input.length;
    let n = initialN;
    let delta = 0;
    let bias = initialBias;
    for (const currentValue of input) {
      if (currentValue < 0x80) output.push(stringFromCharCode(currentValue));
    }
    const basicLength = output.length;
    let handledCPCount = basicLength;
    if (basicLength) output.push(delimiter);
    while (handledCPCount < inputLength) {
      let m = maxInt;
      for (const currentValue of input) {
        if (currentValue >= n && currentValue < m) m = currentValue;
      }
      const handledCPCountPlusOne = handledCPCount + 1;
      if (m - n > floor((maxInt - delta) / handledCPCountPlusOne)) punycodeError("overflow");
      delta += (m - n) * handledCPCountPlusOne;
      n = m;
      for (const currentValue of input) {
        if (currentValue < n && ++delta > maxInt) punycodeError("overflow");
        if (currentValue === n) {
          let q = delta;
          for (let k = base;; k += base) {
            const t = k <= bias ? tMin : (k >= bias + tMax ? tMax : k - bias);
            if (q < t) break;
            const qMinusT = q - t;
            const baseMinusT = base - t;
            output.push(stringFromCharCode(digitToBasic(t + (qMinusT % baseMinusT), 0)));
            q = floor(qMinusT / baseMinusT);
          }
          output.push(stringFromCharCode(digitToBasic(q, 0)));
          bias = adapt(delta, handledCPCountPlusOne, handledCPCount === basicLength);
          delta = 0;
          ++handledCPCount;
        }
      }
      ++delta;
      ++n;
    }
    return output.join("");
  }

  function toUnicode(input) {
    return mapDomain(input, (label) => regexPunycode.test(label) ? decode(label.slice(4).toLowerCase()) : label);
  }

  function toASCII(input) {
    return mapDomain(input, (label) => regexNonASCII.test(label) ? `xn--${encode(label)}` : label);
  }

  __terracePunycodeSingleton = {
    version: "2.1.0",
    ucs2: {
      decode: ucs2decode,
      encode: ucs2encode,
    },
    decode,
    encode,
    toASCII,
    toUnicode,
  };
  return __terracePunycodeSingleton;
}

function __terraceUrlModule() {
  if (__terraceUrlSingleton) return __terraceUrlSingleton;

  function invalidUrlError(input) {
    const error = new TypeError("Invalid URL");
    error.code = "ERR_INVALID_URL";
    error.input = String(input);
    return error;
  }

  function parseAuthority(authority) {
    let auth = "";
    let hostPort = authority || "";
    const at = hostPort.lastIndexOf("@");
    if (at !== -1) {
      auth = hostPort.slice(0, at);
      hostPort = hostPort.slice(at + 1);
    }
    let hostname = hostPort;
    let port = "";
    if (hostPort.startsWith("[")) {
      const end = hostPort.indexOf("]");
      hostname = end === -1 ? hostPort : hostPort.slice(0, end + 1);
      const remainder = end === -1 ? "" : hostPort.slice(end + 1);
      if (remainder.startsWith(":")) port = remainder.slice(1);
    } else {
      const colon = hostPort.lastIndexOf(":");
      if (colon !== -1 && /^[0-9]*$/.test(hostPort.slice(colon + 1))) {
        hostname = hostPort.slice(0, colon);
        port = hostPort.slice(colon + 1);
      }
    }
    let username = "";
    let password = "";
    if (auth) {
      const colon = auth.indexOf(":");
      if (colon === -1) {
        username = decodeURIComponent(auth);
      } else {
        username = decodeURIComponent(auth.slice(0, colon));
        password = decodeURIComponent(auth.slice(colon + 1));
      }
    }
    return {
      auth,
      host: hostPort,
      hostname,
      port,
      username,
      password,
    };
  }

  function parseBasicUrlRecord(raw) {
    const input = String(raw);
    const match = input.match(/^([a-zA-Z][a-zA-Z0-9+.-]*:)(\/\/([^/?#]*))?([^?#]*)(\?[^#]*)?(#.*)?$/);
    if (!match) throw invalidUrlError(input);
    const protocol = match[1];
    const authority = match[3] || "";
    const pathname = match[4] || (authority ? "/" : "");
    const search = match[5] || "";
    const hash = match[6] || "";
    const parsedAuthority = parseAuthority(authority);
    return {
      protocol,
      host: parsedAuthority.host,
      hostname: parsedAuthority.hostname,
      port: parsedAuthority.port,
      username: parsedAuthority.username,
      password: parsedAuthority.password,
      pathname,
      search,
      hash,
      href:
        protocol === "file:" && !authority && pathname.startsWith("/")
          ? `${protocol}//${pathname}${search}${hash}`
          : `${protocol}${authority ? `//${authority}` : ""}${pathname}${search}${hash}`,
    };
  }

  const NativeURLImpl = globalThis.URL;
  const URLImpl = NativeURLImpl
    ? class TerraceUrl extends NativeURLImpl {
        constructor(href, base = undefined) {
          const input = String(href);
          try {
            if (base === undefined) {
              super(input);
            } else {
              super(input, String(base));
            }
          } catch {
            throw invalidUrlError(input);
          }
        }
      }
    : class TerraceUrl {
        constructor(href, base = undefined) {
          const input = String(href);
          try {
            const raw = base === undefined ? input : urlResolve(String(base), input);
            Object.assign(this, parseBasicUrlRecord(raw));
          } catch {
            throw invalidUrlError(input);
          }
        }
        toString() { return this.href; }
      };
  const pathModule = __terraceRequire("path");
  const punycode = __terracePunycodeModule();
  const querystring = __terraceRequire("querystring");

  const kUrlSearchParamsBrand = Symbol("TerraceURLSearchParams");

  function assertUrlSearchParams(value) {
    if (!value || value[kUrlSearchParamsBrand] !== true) {
      throw __terraceInvalidThisError("URLSearchParams");
    }
  }

  const URLSearchParamsImpl = class TerraceUrlSearchParams {
    constructor(init = "") {
      Object.defineProperty(this, kUrlSearchParamsBrand, {
        value: true,
        configurable: false,
        enumerable: false,
        writable: false,
      });
      this._pairs = [];
      if (typeof init === "string") {
        const source = init.startsWith("?") ? init.slice(1) : init;
        for (const part of source.split("&")) {
          if (!part) continue;
          const [key, value = ""] = part.split("=");
          this._pairs.push([decodeURIComponent(key), decodeURIComponent(value)]);
        }
      } else if (init && typeof init[Symbol.iterator] === "function") {
        for (const entry of init) {
          if (!entry || entry.length < 2) continue;
          this._pairs.push([String(entry[0]), String(entry[1])]);
        }
      } else if (init && typeof init === "object") {
        for (const [key, value] of Object.entries(init)) {
          this._pairs.push([String(key), String(value)]);
        }
      }
    }

    append(name, value) {
      assertUrlSearchParams(this);
      this._pairs.push([String(name), String(value)]);
    }

    delete(name) {
      assertUrlSearchParams(this);
      const key = String(name);
      this._pairs = this._pairs.filter(([entry]) => entry !== key);
    }

    entries() {
      assertUrlSearchParams(this);
      return this._pairs[Symbol.iterator]();
    }

    forEach(callback, thisArg = undefined) {
      assertUrlSearchParams(this);
      for (const [key, value] of this._pairs) {
        callback.call(thisArg, value, key, this);
      }
    }

    get(name) {
      assertUrlSearchParams(this);
      const key = String(name);
      const found = this._pairs.find(([entry]) => entry === key);
      return found ? found[1] : null;
    }

    getAll(name) {
      assertUrlSearchParams(this);
      const key = String(name);
      return this._pairs.filter(([entry]) => entry === key).map(([, value]) => value);
    }

    has(name) {
      assertUrlSearchParams(this);
      const key = String(name);
      return this._pairs.some(([entry]) => entry === key);
    }

    keys() {
      assertUrlSearchParams(this);
      return this._pairs.map(([key]) => key)[Symbol.iterator]();
    }

    set(name, value) {
      assertUrlSearchParams(this);
      const key = String(name);
      const normalized = String(value);
      const next = [];
      let replaced = false;
      for (const [entry, current] of this._pairs) {
        if (entry !== key) {
          next.push([entry, current]);
          continue;
        }
        if (!replaced) {
          next.push([entry, normalized]);
          replaced = true;
        }
      }
      if (!replaced) next.push([key, normalized]);
      this._pairs = next;
    }

    sort() {
      assertUrlSearchParams(this);
      this._pairs.sort(([left], [right]) => left.localeCompare(right));
    }

    toString() {
      assertUrlSearchParams(this);
      return this._pairs
        .map(([key, value]) => `${encodeURIComponent(key)}=${encodeURIComponent(value)}`)
        .join("&");
    }

    values() {
      assertUrlSearchParams(this);
      return this._pairs.map(([, value]) => value)[Symbol.iterator]();
    }

    get size() {
      assertUrlSearchParams(this);
      return this._pairs.length;
    }

    [Symbol.iterator]() {
      return this.entries();
    }
  };
  Object.defineProperty(URLSearchParamsImpl.prototype, Symbol.toStringTag, {
    value: "URLSearchParams",
    configurable: true,
  });

  class Url {
    constructor() {
      this.protocol = null;
      this.slashes = null;
      this.auth = null;
      this.host = null;
      this.port = null;
      this.hostname = null;
      this.hash = null;
      this.search = null;
      this.query = null;
      this.pathname = null;
      this.path = null;
      this.href = null;
    }

    parse(value, parseQueryString = false, slashesDenoteHost = false) {
      Object.assign(this, urlParse(value, parseQueryString, slashesDenoteHost));
      return this;
    }

    format() {
      return urlFormat(this);
    }

    resolve(relative) {
      return urlResolve(this, relative);
    }

    resolveObject(relative) {
      return urlResolveObject(this, relative);
    }
  }

  function invalidUrlSchemeError() {
    const error = new TypeError("The URL must be of scheme file");
    error.code = "ERR_INVALID_URL_SCHEME";
    return error;
  }

  function invalidFileUrlHostError(host) {
    const error = new TypeError(
      `File URL host must be "localhost" or empty on non-win32 platforms. Received ${JSON.stringify(host)}`,
    );
    error.code = "ERR_INVALID_FILE_URL_HOST";
    return error;
  }

  function normalizeUrlInput(value, name = "url") {
    if (value instanceof URLImpl) return value;
    if (typeof value === "string") return new URLImpl(value);
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "${name}" argument must be of type string or an instance of URL. Received ${value === null ? "null" : typeof value}`,
    );
  }

  function pathnameFromHref(href) {
    const hashIndex = href.indexOf("#");
    const withoutHash = hashIndex === -1 ? href : href.slice(0, hashIndex);
    const searchIndex = withoutHash.indexOf("?");
    return searchIndex === -1 ? withoutHash : withoutHash.slice(0, searchIndex);
  }

  function normalizePosixPathname(pathname) {
    const decoded = decodeURIComponent(pathname || "/");
    return decoded || "/";
  }

  function normalizeWindowsPathname(pathname, host) {
    const decoded = decodeURIComponent(pathname || "/");
    if (host && host !== "localhost") {
      const unc = decoded.replace(/\//g, "\\");
      return `\\\\${host}${unc}`;
    }
    const withoutLeadingSlash = decoded.startsWith("/") ? decoded.slice(1) : decoded;
    if (/^[A-Za-z]:/.test(withoutLeadingSlash)) {
      return withoutLeadingSlash.replace(/\//g, "\\");
    }
    return decoded.replace(/\//g, "\\");
  }

  function fileURLToPath(value, options = undefined) {
    if (options != null && typeof options !== "object") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options" argument must be of type object. Received ${options === null ? "null" : typeof options}`,
      );
    }
    const url = normalizeUrlInput(value);
    if (url.protocol !== "file:") throw invalidUrlSchemeError();
    const useWindows = options && Object.prototype.hasOwnProperty.call(options, "windows")
      ? Boolean(options.windows)
      : process.platform === "win32";
    const host = url.host || "";
    if (!useWindows && host && host !== "localhost") {
      throw invalidFileUrlHostError(host);
    }
    const pathname = url.pathname || "/";
    return useWindows
      ? normalizeWindowsPathname(pathname, host)
      : normalizePosixPathname(pathname);
  }

  function encodeFilePath(path, windows) {
    const normalized = windows ? String(path).replace(/\\/g, "/") : String(path);
    return encodeURI(normalized)
      .replace(/\?/g, "%3F")
      .replace(/#/g, "%23");
  }

  function pathToFileURL(pathValue, options = undefined) {
    if (typeof pathValue !== "string") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "path" argument must be of type string. Received ${pathValue === null ? "null" : typeof pathValue}`,
      );
    }
    if (options != null && typeof options !== "object") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options" argument must be of type object. Received ${options === null ? "null" : typeof options}`,
      );
    }
    const useWindows = options && Object.prototype.hasOwnProperty.call(options, "windows")
      ? Boolean(options.windows)
      : process.platform === "win32";
    if (useWindows) {
      const normalized = String(pathValue).replace(/\\/g, "/");
      if (normalized.startsWith("//")) {
        const withoutSlashes = normalized.slice(2);
        const slashIndex = withoutSlashes.indexOf("/");
        const host = slashIndex === -1 ? withoutSlashes : withoutSlashes.slice(0, slashIndex);
        const tail = slashIndex === -1 ? "/" : withoutSlashes.slice(slashIndex);
        return new URLImpl(`file://${host}${encodeFilePath(tail, true)}`);
      }
      const absolute = /^[A-Za-z]:/.test(normalized)
        ? normalized
        : pathModule.win32.resolve(normalized);
      return new URLImpl(`file:///${encodeFilePath(absolute, true)}`);
    }
    const absolute = pathModule.resolve(String(pathValue));
    return new URLImpl(`file://${encodeFilePath(absolute, false)}`);
  }

  function domainToASCII(value) {
    try {
      return punycode.toASCII(String(value ?? ""));
    } catch {
      return "";
    }
  }

  function domainToUnicode(value) {
    try {
      return punycode.toUnicode(String(value ?? ""));
    } catch {
      return String(value ?? "");
    }
  }

  function isAbsoluteUrl(value) {
    return /^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(value);
  }

  function parseRelativeUrl(raw, parseQueryString) {
    const result = new Url();
    const input = String(raw);
    let rest = input;
    let hash = null;
    const hashIndex = rest.indexOf("#");
    if (hashIndex !== -1) {
      hash = rest.slice(hashIndex);
      rest = rest.slice(0, hashIndex);
    }
    let search = null;
    let query = null;
    const searchIndex = rest.indexOf("?");
    if (searchIndex !== -1) {
      search = rest.slice(searchIndex);
      const rawQuery = rest.slice(searchIndex + 1);
      query = parseQueryString ? querystring.parse(rawQuery) : rawQuery;
      rest = rest.slice(0, searchIndex);
    } else if (parseQueryString) {
      query = Object.create(null);
    }
    result.hash = hash;
    result.search = search;
    result.query = query;
    result.pathname = rest || "";
    result.path = `${result.pathname}${search || ""}`;
    result.href = input;
    return result;
  }

  function parseAbsoluteUrl(raw, parseQueryString) {
    const url = new URLImpl(String(raw));
    const result = new Url();
    result.protocol = url.protocol || null;
    result.slashes = true;
    const username = url.username || "";
    const password = url.password || "";
    result.auth = username ? (password ? `${username}:${password}` : username) : null;
    result.host = url.host || null;
    result.port = url.port || null;
    result.hostname = url.hostname || null;
    result.hash = url.hash || null;
    result.search = url.search || null;
    result.query = parseQueryString
      ? querystring.parse((url.search || "").replace(/^\?/, ""))
      : (url.search ? url.search.slice(1) : null);
    result.pathname = url.pathname || "/";
    result.path = `${result.pathname}${result.search || ""}`;
    result.href = url.toString();
    return result;
  }

  function urlParse(value, parseQueryString = false, slashesDenoteHost = false) {
    const input = String(value).trim();
    if (!slashesDenoteHost && !isAbsoluteUrl(input) && !input.startsWith("//")) {
      return parseRelativeUrl(input, parseQueryString);
    }
    if (input.startsWith("//")) {
      return parseAbsoluteUrl(`http:${input}`, parseQueryString);
    }
    try {
      return parseAbsoluteUrl(input, parseQueryString);
    } catch {
      return parseRelativeUrl(input, parseQueryString);
    }
  }

  function urlFormat(value) {
    if (typeof value === "string") return value;
    if (value instanceof URLImpl) return value.toString();
    if (value && typeof value.href === "string") return value.href;
    const protocol = value?.protocol || "";
    const slashes = (value?.slashes || value?.host != null || value?.hostname != null) ? "//" : "";
    const auth = value?.auth ? `${value.auth}@` : "";
    const host = value?.host || (value?.hostname ? `${value.hostname}${value.port ? `:${value.port}` : ""}` : "");
    const pathname = value?.pathname || "";
    const search = value?.search || (
      value?.query && typeof value.query === "object"
        ? `?${querystring.stringify(value.query)}`
        : value?.query ? `?${String(value.query)}` : ""
    );
    const hash = value?.hash || "";
    return `${protocol}${slashes}${auth}${host}${pathname}${search}${hash}`;
  }

  function resolveRelativePath(base, relative) {
    if (relative.startsWith("/")) return relative;
    const hashIndex = relative.indexOf("#");
    const hash = hashIndex === -1 ? "" : relative.slice(hashIndex);
    const withoutHash = hashIndex === -1 ? relative : relative.slice(0, hashIndex);
    const searchIndex = withoutHash.indexOf("?");
    const search = searchIndex === -1 ? "" : withoutHash.slice(searchIndex);
    const pathOnly = searchIndex === -1 ? withoutHash : withoutHash.slice(0, searchIndex);
    const baseHashIndex = base.indexOf("#");
    const baseWithoutHash = baseHashIndex === -1 ? base : base.slice(0, baseHashIndex);
    const baseSearchIndex = baseWithoutHash.indexOf("?");
    const basePathOnly = baseSearchIndex === -1 ? baseWithoutHash : baseWithoutHash.slice(0, baseSearchIndex);
    if (pathOnly === ".") {
      return `${basePathOnly.endsWith("/") ? basePathOnly : `${pathModule.posix.dirname(basePathOnly)}/`}${search}${hash}`;
    }
    const resolvedBase = basePathOnly.endsWith("/") ? basePathOnly : pathModule.posix.dirname(basePathOnly);
    const joined = pathModule.posix.normalize(pathModule.posix.join(resolvedBase || "/", pathOnly || ""));
    return `${joined}${search}${hash}`;
  }

  function urlResolve(from, to) {
    const target = String(to);
    const base = typeof from === "string" ? from : urlFormat(from);
    if (!base) return target;
    if (isAbsoluteUrl(target)) {
      const schemeMatch = target.match(/^([a-zA-Z][a-zA-Z0-9+.-]*:)(.*)$/);
      const scheme = schemeMatch ? schemeMatch[1] : "";
      const remainder = schemeMatch ? schemeMatch[2] : "";
      const baseUrl = isAbsoluteUrl(base) ? new URLImpl(base) : null;
      if (remainder.startsWith("//")) return target;
      if (remainder.startsWith("#") || remainder === "") {
        if (baseUrl && baseUrl.protocol === scheme) {
          return `${baseUrl.protocol}//${baseUrl.host}${baseUrl.pathname}${baseUrl.search}${remainder}`;
        }
        return `${scheme}///${remainder.replace(/^#?/, "#")}`.replace("/##", "/#");
      }
      if (remainder.startsWith("/")) {
        if (baseUrl && baseUrl.protocol === scheme) {
          return `${baseUrl.protocol}//${baseUrl.host}${remainder}`;
        }
        return `${scheme}//${remainder.slice(1)}`;
      }
      return target;
    }
    if (target.startsWith("//")) {
      const baseUrl = isAbsoluteUrl(base)
        ? parseBasicUrlRecord(base)
        : parseBasicUrlRecord(`http://placeholder${base.startsWith("/") ? "" : "/"}${base}`);
      return `${baseUrl.protocol}${target}`;
    }
    if (isAbsoluteUrl(base)) {
      const baseUrl = parseBasicUrlRecord(base);
      if (target.startsWith("?")) {
        return `${baseUrl.protocol}//${baseUrl.host}${baseUrl.pathname}${target}`;
      }
      if (target.startsWith("#")) {
        return `${baseUrl.protocol}//${baseUrl.host}${baseUrl.pathname}${baseUrl.search}${target}`;
      }
      if (target.startsWith("/")) {
        return `${baseUrl.protocol}//${baseUrl.host}${target}`;
      }
      const baseDir = baseUrl.pathname.endsWith("/")
        ? baseUrl.pathname
        : `${pathModule.posix.dirname(baseUrl.pathname)}/`;
      const relative = resolveRelativePath(baseDir, target);
      return `${baseUrl.protocol}//${baseUrl.host}${relative}`;
    }
    return resolveRelativePath(base, target);
  }

  function urlResolveObject(from, to) {
    const base = typeof from === "string" ? from : urlFormat(from);
    if (!base) return typeof to === "string" ? to : urlFormat(to);
    return urlParse(urlResolve(base, typeof to === "string" ? to : urlFormat(to)));
  }

  function urlToHttpOptions(value) {
    if (value === null || (typeof value !== "object" && typeof value !== "function")) {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "url" argument must be of type object. Received ${value === null ? "null" : typeof value}`,
      );
    }
    const knownKeys = new Set([
      "protocol",
      "username",
      "password",
      "host",
      "hostname",
      "port",
      "pathname",
      "search",
      "hash",
      "href",
      "origin",
    ]);
    const extraProps = {};
    for (const key of Object.keys(value)) {
      if (knownKeys.has(key)) continue;
      extraProps[key] = value[key];
    }
    const {
      hostname,
      pathname,
      port,
      username,
      password,
      search,
    } = value;
    const normalizedHostname = hostname && hostname.startsWith("[")
      ? hostname.slice(1, -1)
      : hostname
        ? domainToASCII(hostname)
        : hostname;
    const auth = username || password
      ? `${decodeURIComponent(username)}:${decodeURIComponent(password)}`
      : undefined;
    const encodedHost = port !== ""
      ? `${normalizedHostname}:${String(port)}`
      : normalizedHostname;
    const options = {
      __proto__: null,
      ...extraProps,
      protocol: value.protocol,
      hostname: normalizedHostname,
      hash: value.hash,
      search,
      pathname,
      path: `${pathname || ""}${search || ""}`,
      href: `${value.protocol}//${auth ? `${auth}@` : ""}${encodedHost || ""}${pathname || ""}${search || ""}${value.hash || ""}`,
    };
    if (port !== "") options.port = Number(port);
    if (auth) options.auth = auth;
    return options;
  }

  __terraceUrlSingleton = {
    Url,
    URL: URLImpl,
    URLSearchParams: URLSearchParamsImpl,
    URLPattern: globalThis.URLPattern,
    parse: urlParse,
    format: urlFormat,
    resolve: urlResolve,
    resolveObject: urlResolveObject,
    pathToFileURL,
    fileURLToPath,
    fileURLToPathBuffer(value, options = undefined) {
      return TerraceBuffer.from(fileURLToPath(value, options));
    },
    domainToASCII,
    domainToUnicode,
    urlToHttpOptions,
  };
  return __terraceUrlSingleton;
}

const __terraceOsErrnoConstants = __terraceDeepFreeze({
  E2BIG: 7,
  EACCES: 13,
  EADDRINUSE: 98,
  EADDRNOTAVAIL: 99,
  EAFNOSUPPORT: 97,
  EAGAIN: 11,
  EALREADY: 114,
  EBADF: 9,
  EBADMSG: 74,
  EBUSY: 16,
  ECANCELED: 125,
  ECHILD: 10,
  ECONNABORTED: 103,
  ECONNREFUSED: 111,
  ECONNRESET: 104,
  EDEADLK: 35,
  EDESTADDRREQ: 89,
  EDOM: 33,
  EDQUOT: 122,
  EEXIST: 17,
  EFAULT: 14,
  EFBIG: 27,
  EHOSTUNREACH: 113,
  EIDRM: 43,
  EILSEQ: 84,
  EINPROGRESS: 115,
  EINTR: 4,
  EINVAL: 22,
  EIO: 5,
  EISCONN: 106,
  EISDIR: 21,
  ELOOP: 40,
  EMFILE: 24,
  EMLINK: 31,
  EMSGSIZE: 90,
  EMULTIHOP: 72,
  ENAMETOOLONG: 36,
  ENETDOWN: 100,
  ENETRESET: 102,
  ENETUNREACH: 101,
  ENFILE: 23,
  ENOBUFS: 105,
  ENODATA: 61,
  ENODEV: 19,
  ENOENT: 2,
  ENOEXEC: 8,
  ENOLCK: 37,
  ENOLINK: 67,
  ENOMEM: 12,
  ENOMSG: 42,
  ENOPROTOOPT: 92,
  ENOSPC: 28,
  ENOSR: 63,
  ENOSTR: 60,
  ENOSYS: 38,
  ENOTCONN: 107,
  ENOTDIR: 20,
  ENOTEMPTY: 39,
  ENOTSOCK: 88,
  ENOTSUP: 95,
  ENOTTY: 25,
  ENXIO: 6,
  EOPNOTSUPP: 95,
  EOVERFLOW: 75,
  EPERM: 1,
  EPIPE: 32,
  EPROTO: 71,
  EPROTONOSUPPORT: 93,
  EPROTOTYPE: 91,
  ERANGE: 34,
  EROFS: 30,
  ESPIPE: 29,
  ESRCH: 3,
  ESTALE: 116,
  ETIME: 62,
  ETIMEDOUT: 110,
  ETXTBSY: 26,
  EWOULDBLOCK: 11,
  EXDEV: 18,
});

const __terraceOsSignalConstants = __terraceDeepFreeze({
  SIGHUP: 1,
  SIGINT: 2,
  SIGQUIT: 3,
  SIGILL: 4,
  SIGTRAP: 5,
  SIGABRT: 6,
  SIGIOT: 6,
  SIGBUS: 7,
  SIGFPE: 8,
  SIGKILL: 9,
  SIGUSR1: 10,
  SIGSEGV: 11,
  SIGUSR2: 12,
  SIGPIPE: 13,
  SIGALRM: 14,
  SIGTERM: 15,
  SIGCHLD: 17,
  SIGCONT: 18,
  SIGSTOP: 19,
  SIGTSTP: 20,
  SIGTTIN: 21,
  SIGTTOU: 22,
  SIGURG: 23,
  SIGXCPU: 24,
  SIGXFSZ: 25,
  SIGVTALRM: 26,
  SIGPROF: 27,
  SIGWINCH: 28,
  SIGIO: 29,
  SIGINFO: 29,
  SIGSYS: 31,
});

const __terraceOsPriorityConstants = __terraceDeepFreeze({
  PRIORITY_LOW: 19,
  PRIORITY_BELOW_NORMAL: 10,
  PRIORITY_NORMAL: 0,
  PRIORITY_ABOVE_NORMAL: -7,
  PRIORITY_HIGH: -14,
  PRIORITY_HIGHEST: -20,
});

const __terraceOsDlopenConstants = __terraceDeepFreeze({
  RTLD_LAZY: 1,
  RTLD_NOW: 2,
  RTLD_GLOBAL: 256,
  RTLD_LOCAL: 0,
});

const __terraceOsConstants = __terraceDeepFreeze({
  UV_UDP_REUSEADDR: 4,
  dlopen: __terraceOsDlopenConstants,
  errno: __terraceOsErrnoConstants,
  priority: __terraceOsPriorityConstants,
  signals: __terraceOsSignalConstants,
});

const __terraceOsCpuRows = __terraceDeepFreeze([
  {
    model: "Terrace Deterministic CPU",
    speed: 2400,
    times: {
      user: 1,
      nice: 0,
      sys: 1,
      idle: 1,
      irq: 0,
    },
  },
]);

const __terraceOsLoopbackInterfaces = __terraceDeepFreeze({
  lo: [
    {
      address: "127.0.0.1",
      netmask: "255.0.0.0",
      family: "IPv4",
      mac: "00:00:00:00:00:00",
      internal: true,
      cidr: "127.0.0.1/8",
    },
    {
      address: "::1",
      netmask: "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
      family: "IPv6",
      mac: "00:00:00:00:00:00",
      internal: true,
      cidr: "::1/128",
      scopeid: 0,
    },
  ],
});

let __terraceOsCurrentPriority = __terraceOsPriorityConstants.PRIORITY_NORMAL;

function __terraceCurrentEnv() {
  if (globalThis.process && globalThis.process.env) return globalThis.process.env;
  return __terraceProcessInfo().env || {};
}

function __terraceOsTmpdir(platform) {
  const env = __terraceCurrentEnv();
  if (platform === "win32") {
    const candidate = env.TEMP || env.TMP || `${env.SystemRoot || env.windir || "C:\\Windows"}\\temp`;
    return candidate;
  }
  let candidate = env.TMPDIR || env.TMP || env.TEMP || "/tmp";
  if (candidate.length > 1 && /\/+$/.test(candidate)) {
    candidate = candidate.replace(/\/+$/, "") || "/";
  }
  return candidate;
}

function __terraceOsHomedir(platform) {
  const env = __terraceCurrentEnv();
  if (platform === "win32") return env.USERPROFILE || "C:\\Users\\sandbox";
  return env.HOME || "/workspace/home";
}

function __terraceOsMachine(arch) {
  switch (arch) {
    case "x64":
      return "x86_64";
    case "arm64":
      return "aarch64";
    default:
      return arch;
  }
}

function __terraceValidateOsInteger(name, value) {
  if (typeof value !== "number") {
    throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", `The "${name}" argument must be of type number.`);
  }
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < -1 || value > 2 ** 32 - 1) {
    throw __terraceNodeRangeError(
      "ERR_OUT_OF_RANGE",
      `The value of "${name}" is out of range. It must be an integer >= -1 and <= 4294967295. Received ${String(value)}`,
    );
  }
  return value;
}

function __terraceOsSystemError(syscall, code, message) {
  const error = new Error(`A system error occurred: ${syscall} returned ${code} (${message})`);
  error.name = "SystemError";
  error.code = "ERR_SYSTEM_ERROR";
  error.info = { code, syscall, message };
  return error;
}

function __terraceOsValidatePriority(priority) {
  const value = __terraceValidateOsInteger("priority", priority);
  if (value < __terraceOsPriorityConstants.PRIORITY_HIGHEST || value > __terraceOsPriorityConstants.PRIORITY_LOW) {
    throw __terraceNodeRangeError(
      "ERR_OUT_OF_RANGE",
      `The value of "priority" is out of range. It must be >= ${__terraceOsPriorityConstants.PRIORITY_HIGHEST} and <= ${__terraceOsPriorityConstants.PRIORITY_LOW}. Received ${String(priority)}`,
    );
  }
  return value;
}

function __terraceOsGetPriority(pid) {
  if (pid === undefined) return __terraceOsCurrentPriority;
  const value = __terraceValidateOsInteger("pid", pid);
  const currentPid = globalThis.process && typeof globalThis.process.pid === "number" ? globalThis.process.pid : 1;
  if (value === 0 || value === currentPid) return __terraceOsCurrentPriority;
  throw __terraceOsSystemError("uv_os_getpriority", "ESRCH", "no such process");
}

function __terraceOsSetPriority(pidOrPriority, maybePriority) {
  let pid = 0;
  let priority = pidOrPriority;
  if (maybePriority !== undefined) {
    pid = __terraceValidateOsInteger("pid", pidOrPriority);
    priority = maybePriority;
  }
  const normalizedPriority = __terraceOsValidatePriority(priority);
  const currentPid = globalThis.process && typeof globalThis.process.pid === "number" ? globalThis.process.pid : 1;
  if (pid !== 0 && pid !== currentPid) {
    throw __terraceOsSystemError("uv_os_setpriority", "ESRCH", "no such process");
  }
  __terraceOsCurrentPriority = normalizedPriority;
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
        __terraceWriteStderr(`${message}\n`);
      }
    };
    logger.enabled = false;
    return logger;
  };
  return {
    format: __terraceFormat,
    formatWithOptions: (_options, ...args) => __terraceFormat(...args),
    inspect: __terraceInspect,
    promisify: __terracePromisify,
    inherits: __terraceInherits,
    TextEncoder: globalThis.TextEncoder,
    TextDecoder: globalThis.TextDecoder,
    deprecate: (fn) => fn,
    debuglog,
  };
}

function __terraceConstantsModule() {
  return {
    O_RDONLY: 0,
    O_WRONLY: 1,
    O_RDWR: 2,
    O_CREAT: 64,
    O_EXCL: 128,
    O_TRUNC: 512,
    O_APPEND: 1024,
    O_SYMLINK: 0,
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
      const result = Object.create(null);
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

const __terraceHttpStatusCodes = Object.freeze({
  100: "Continue",
  101: "Switching Protocols",
  102: "Processing",
  103: "Early Hints",
  200: "OK",
  201: "Created",
  202: "Accepted",
  203: "Non-Authoritative Information",
  204: "No Content",
  205: "Reset Content",
  206: "Partial Content",
  207: "Multi-Status",
  208: "Already Reported",
  226: "IM Used",
  300: "Multiple Choices",
  301: "Moved Permanently",
  302: "Found",
  303: "See Other",
  304: "Not Modified",
  305: "Use Proxy",
  307: "Temporary Redirect",
  308: "Permanent Redirect",
  400: "Bad Request",
  401: "Unauthorized",
  402: "Payment Required",
  403: "Forbidden",
  404: "Not Found",
  405: "Method Not Allowed",
  406: "Not Acceptable",
  407: "Proxy Authentication Required",
  408: "Request Timeout",
  409: "Conflict",
  410: "Gone",
  411: "Length Required",
  412: "Precondition Failed",
  413: "Payload Too Large",
  414: "URI Too Long",
  415: "Unsupported Media Type",
  416: "Range Not Satisfiable",
  417: "Expectation Failed",
  418: "I'm a Teapot",
  421: "Misdirected Request",
  422: "Unprocessable Entity",
  423: "Locked",
  424: "Failed Dependency",
  425: "Too Early",
  426: "Upgrade Required",
  428: "Precondition Required",
  429: "Too Many Requests",
  431: "Request Header Fields Too Large",
  451: "Unavailable For Legal Reasons",
  500: "Internal Server Error",
  501: "Not Implemented",
  502: "Bad Gateway",
  503: "Service Unavailable",
  504: "Gateway Timeout",
  505: "HTTP Version Not Supported",
  506: "Variant Also Negotiates",
  507: "Insufficient Storage",
  508: "Loop Detected",
  509: "Bandwidth Limit Exceeded",
  510: "Not Extended",
  511: "Network Authentication Required",
});

const __terraceHttpMethods = Object.freeze([
  "ACL",
  "BIND",
  "CHECKOUT",
  "CONNECT",
  "COPY",
  "DELETE",
  "GET",
  "HEAD",
  "LINK",
  "LOCK",
  "M-SEARCH",
  "MERGE",
  "MKACTIVITY",
  "MKCALENDAR",
  "MKCOL",
  "MOVE",
  "NOTIFY",
  "OPTIONS",
  "PATCH",
  "POST",
  "PROPFIND",
  "PROPPATCH",
  "PURGE",
  "PUT",
  "QUERY",
  "REBIND",
  "REPORT",
  "SEARCH",
  "SOURCE",
  "SUBSCRIBE",
  "TRACE",
  "UNBIND",
  "UNLINK",
  "UNLOCK",
  "UNSUBSCRIBE",
]);

function __terraceStreamModule() {
  function normalizeChunk(chunk, encoding, objectMode) {
    if (objectMode) return chunk;
    if (chunk == null) return chunk;
    if (TerraceBuffer.isBuffer(chunk)) return chunk;
    if (chunk instanceof Uint8Array) return TerraceBuffer.from(chunk);
    if (typeof chunk === "string") return TerraceBuffer.from(chunk, encoding || "utf8");
    if (chunk instanceof ArrayBuffer) return TerraceBuffer.from(new Uint8Array(chunk));
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      'The "chunk" argument must be of type string or an instance of Buffer, TypedArray, or DataView.',
    );
  }

  function initReadable(target, options = {}) {
    target._readableState = {
      objectMode: !!options.objectMode,
      highWaterMark: options.highWaterMark ?? (options.objectMode ? 16 : 16384),
      encoding: options.encoding || null,
      buffer: [],
      flowing: false,
      ended: false,
      endEmitted: false,
      reading: false,
      pipes: [],
    };
    target.readable = true;
    if (typeof options.read === "function") {
      target._read = options.read.bind(target);
    }
  }

  function initWritable(target, options = {}) {
    target._writableState = {
      objectMode: !!options.objectMode,
      highWaterMark: options.highWaterMark ?? (options.objectMode ? 16 : 16384),
      ending: false,
      ended: false,
      finished: false,
      writing: false,
      corked: 0,
      needDrain: false,
      buffer: [],
      finalCalled: false,
    };
    target.writable = true;
    target._write = typeof options.write === "function" ? options.write.bind(target) : target._write;
    target._final = typeof options.final === "function" ? options.final.bind(target) : target._final;
  }

  function maybeRead(stream) {
    const state = stream._readableState;
    if (state.reading || state.ended || state.buffer.length > 0) return;
    state.reading = true;
    try {
      stream._read(state.highWaterMark);
    } finally {
      state.reading = false;
    }
  }

  function maybeEmitReadable(stream) {
    const state = stream._readableState;
    if (!state.flowing && state.buffer.length > 0) {
      TerraceEventEmitter.prototype.emit.call(stream, "readable");
    }
  }

  function endReadable(stream) {
    const state = stream._readableState;
    if (state.endEmitted || !state.ended || state.buffer.length > 0) return;
    state.endEmitted = true;
    stream.readable = false;
    const pipes = [...state.pipes];
    state.pipes = [];
    for (const pipe of pipes) {
      if (pipe.end !== false && typeof pipe.dest.end === "function") {
        pipe.dest.end();
      }
      if (typeof pipe.dest.emit === "function") {
        pipe.dest.emit("unpipe", stream);
      }
    }
    TerraceEventEmitter.prototype.emit.call(stream, "end");
  }

  function flowReadable(stream) {
    const state = stream._readableState;
    if (!state.flowing) return;
    while (state.flowing) {
      if (state.buffer.length === 0) {
        maybeRead(stream);
        if (state.buffer.length === 0) break;
      }
      const chunk = state.buffer.shift();
      for (const pipe of [...state.pipes]) {
        const wrote = typeof pipe.dest.write === "function" ? pipe.dest.write(chunk) : true;
        if (wrote === false) {
          state.flowing = false;
          if (typeof pipe.dest.once === "function") {
            pipe.dest.once("drain", () => {
              state.flowing = true;
              flowReadable(stream);
            });
          }
        }
      }
      if (stream.listenerCount("data") > 0) {
        TerraceEventEmitter.prototype.emit.call(stream, "data", chunk);
      }
    }
    endReadable(stream);
  }

  function performWrite(stream, chunk, encoding, callback) {
    const state = stream._writableState;
    state.writing = true;
    let settled = false;
    const done = (error) => {
      if (settled) return;
      settled = true;
      state.writing = false;
      if (typeof callback === "function") callback(error);
      if (error) {
        TerraceEventEmitter.prototype.emit.call(stream, "error", error);
        return;
      }
      if (state.buffer.length > 0 && state.corked === 0) {
        const next = state.buffer.shift();
        performWrite(stream, next.chunk, next.encoding, next.callback);
        return;
      }
      if (state.needDrain && state.buffer.length === 0) {
        state.needDrain = false;
        TerraceEventEmitter.prototype.emit.call(stream, "drain");
      }
      if (state.ending) finishWritable(stream);
    };
    try {
      (stream._write || ((_c, _e, cb) => cb()))(chunk, encoding || "utf8", done);
    } catch (error) {
      done(error);
    }
    const ok = state.buffer.length < state.highWaterMark;
    if (!ok) state.needDrain = true;
    return ok;
  }

  function finishWritable(stream) {
    const state = stream._writableState;
    if (state.finished || state.writing || state.buffer.length > 0 || state.corked > 0) return;
    const finish = (error) => {
      if (error) {
        TerraceEventEmitter.prototype.emit.call(stream, "error", error);
        return;
      }
      if (state.finished) return;
      state.finished = true;
      state.ended = true;
      stream.writable = false;
      TerraceEventEmitter.prototype.emit.call(stream, "finish");
    };
    if (typeof stream._final === "function" && !state.finalCalled) {
      state.finalCalled = true;
      try {
        stream._final(finish);
      } catch (error) {
        finish(error);
      }
    } else {
      finish();
    }
  }

  class Stream extends TerraceEventEmitter {
    destroy(error = null) {
      this.destroyed = true;
      if (error) TerraceEventEmitter.prototype.emit.call(this, "error", error);
      TerraceEventEmitter.prototype.emit.call(this, "close");
      return this;
    }
  }

  class Readable extends Stream {
    constructor(options = {}) {
      super();
      initReadable(this, options);
    }

    _read() {
      return;
    }

    push(chunk, encoding = undefined) {
      const state = this._readableState;
      if (chunk === null) {
        state.ended = true;
        endReadable(this);
        return false;
      }
      state.buffer.push(normalizeChunk(chunk, encoding, state.objectMode));
      maybeEmitReadable(this);
      if (state.flowing) flowReadable(this);
      return state.buffer.length < state.highWaterMark;
    }

    read() {
      const state = this._readableState;
      if (state.buffer.length === 0) {
        maybeRead(this);
        if (state.buffer.length === 0) {
          endReadable(this);
          return null;
        }
      }
      const chunk = state.buffer.shift();
      if (!state.objectMode && state.encoding && chunk != null) {
        const decoded = TerraceBuffer.from(chunk).toString(state.encoding);
        endReadable(this);
        return decoded;
      }
      endReadable(this);
      return chunk;
    }

    pipe(dest, options = {}) {
      const state = this._readableState;
      state.pipes.push({ dest, end: options.end !== false });
      if (typeof dest.emit === "function") {
        dest.emit("pipe", this);
      }
      process.nextTick(() => this.resume());
      return dest;
    }

    unpipe(dest = undefined) {
      const state = this._readableState;
      const removed = [];
      if (dest === undefined) {
        removed.push(...state.pipes);
        state.pipes = [];
      } else {
        state.pipes = state.pipes.filter((pipe) => {
          if (pipe.dest === dest) {
            removed.push(pipe);
            return false;
          }
          return true;
        });
      }
      for (const pipe of removed) {
        if (typeof pipe.dest.emit === "function") {
          pipe.dest.emit("unpipe", this);
        }
      }
      if (state.pipes.length === 0 && this.listenerCount("data") === 0) {
        state.flowing = false;
      }
      return this;
    }

    pause() {
      this._readableState.flowing = false;
      return this;
    }

    resume() {
      if (!this._readableState.flowing) {
        this._readableState.flowing = true;
        TerraceEventEmitter.prototype.emit.call(this, "resume");
      }
      flowReadable(this);
      return this;
    }

    setEncoding(encoding) {
      this._readableState.encoding = encoding;
      return this;
    }

    on(event, listener) {
      const ret = super.on(event, listener);
      if (event === "data") this.resume();
      if (event === "readable") maybeEmitReadable(this);
      if (event === "end") endReadable(this);
      return ret;
    }
  }

  class Writable extends Stream {
    constructor(options = {}) {
      super();
      initWritable(this, options);
    }

    _write(_chunk, _encoding, callback) {
      if (typeof callback === "function") callback();
    }

    get writableCorked() {
      return this._writableState.corked;
    }

    write(chunk, encoding = undefined, callback = undefined) {
      if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      const state = this._writableState;
      if (state.ending) {
        const error = new Error("write after end");
        error.code = "ERR_STREAM_WRITE_AFTER_END";
        if (typeof callback === "function") callback(error);
        else TerraceEventEmitter.prototype.emit.call(this, "error", error);
        return false;
      }
      const normalized = normalizeChunk(chunk, encoding, state.objectMode);
      if (state.corked > 0 || state.writing) {
        state.buffer.push({ chunk: normalized, encoding, callback });
        const ok = state.buffer.length < state.highWaterMark;
        if (!ok) state.needDrain = true;
        return ok;
      }
      return performWrite(this, normalized, encoding, callback);
    }

    cork() {
      this._writableState.corked += 1;
    }

    uncork() {
      const state = this._writableState;
      if (state.corked > 0) state.corked -= 1;
      if (state.corked === 0 && !state.writing && state.buffer.length > 0) {
        const next = state.buffer.shift();
        performWrite(this, next.chunk, next.encoding, next.callback);
      } else if (state.corked === 0 && state.ending) {
        finishWritable(this);
      }
      return this;
    }

    end(chunk = undefined, encoding = undefined, callback = undefined) {
      if (typeof chunk === "function") {
        callback = chunk;
        chunk = undefined;
        encoding = undefined;
      } else if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      if (chunk !== undefined) this.write(chunk, encoding);
      if (typeof callback === "function") this.once("finish", callback);
      this._writableState.ending = true;
      finishWritable(this);
      return this;
    }
  }

  class Duplex extends Readable {
    constructor(options = {}) {
      super(options);
      initWritable(this, options);
    }
  }
  Object.getOwnPropertyNames(Writable.prototype).forEach((name) => {
    if (name === "constructor") return;
    if (!(name in Duplex.prototype)) {
      Object.defineProperty(Duplex.prototype, name, Object.getOwnPropertyDescriptor(Writable.prototype, name));
    }
  });

  class Transform extends Duplex {
    constructor(options = {}) {
      super(options);
      this._transform = typeof options.transform === "function"
        ? options.transform.bind(this)
        : this._transform;
      this._flush = typeof options.flush === "function" ? options.flush.bind(this) : this._flush;
    }

    _transform(chunk, _encoding, callback) {
      callback(null, chunk);
    }

    write(chunk, encoding = undefined, callback = undefined) {
      if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      const normalized = normalizeChunk(chunk, encoding, this._writableState.objectMode);
      try {
        this._transform(normalized, encoding || "utf8", (error, output) => {
          if (error) {
            if (typeof callback === "function") callback(error);
            TerraceEventEmitter.prototype.emit.call(this, "error", error);
            return;
          }
          if (output !== undefined) this.push(output);
          if (typeof callback === "function") callback();
        });
      } catch (error) {
        if (typeof callback === "function") callback(error);
        TerraceEventEmitter.prototype.emit.call(this, "error", error);
      }
      return true;
    }

    end(chunk = undefined, encoding = undefined, callback = undefined) {
      if (typeof chunk === "function") {
        callback = chunk;
        chunk = undefined;
        encoding = undefined;
      } else if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      if (chunk !== undefined) this.write(chunk, encoding);
      if (typeof this._flush === "function") {
        this._flush((error, output) => {
          if (error) {
            TerraceEventEmitter.prototype.emit.call(this, "error", error);
            return;
          }
          if (output !== undefined) this.push(output);
          this.push(null);
        });
      } else {
        this.push(null);
      }
      this._writableState.ending = true;
      finishWritable(this);
      if (typeof callback === "function") this.once("finish", callback);
      return this;
    }
  }

  class PassThrough extends Transform {
    _transform(chunk, _encoding, callback) {
      callback(null, chunk);
    }
  }

  function finished(stream, callback) {
    let done = false;
    const settle = (error = null) => {
      if (done) return;
      done = true;
      if (typeof callback === "function") callback(error);
    };
    stream.once("error", (error) => settle(error));
    stream.once("end", () => settle(null));
    stream.once("finish", () => settle(null));
    return stream;
  }

  function pipeline(...args) {
    let callback = null;
    if (typeof args[args.length - 1] === "function") callback = args.pop();
    const streams = args;
    for (let i = 0; i < streams.length - 1; i += 1) {
      streams[i].pipe(streams[i + 1]);
    }
    const tail = streams[streams.length - 1];
    finished(tail, (error) => callback && callback(error));
    return tail;
  }

  Stream.Stream = Stream;
  Stream.Readable = Readable;
  Stream.Writable = Writable;
  Stream.Duplex = Duplex;
  Stream.Transform = Transform;
  Stream.PassThrough = PassThrough;
  Stream.pipeline = pipeline;
  Stream.finished = finished;
  Stream.promises = {
    pipeline(...streams) {
      return new Promise((resolve, reject) => {
        pipeline(...streams, (error) => (error ? reject(error) : resolve()));
      });
    },
    finished(stream) {
      return new Promise((resolve, reject) => {
        finished(stream, (error) => (error ? reject(error) : resolve()));
      });
    },
  };
  return Stream;
}

function __terraceUnsupportedNetworkError(moduleName = "http") {
  const error = new Error(`Terrace sandbox does not yet implement outbound ${moduleName.toUpperCase()} networking`);
  error.code = "ERR_TERRACE_NETWORK_UNSUPPORTED";
  return error;
}

function __terraceNetModule() {
  const stream = __terraceStreamModule();

  class Socket extends stream.Duplex {
    constructor(options = {}) {
      super();
      this.connecting = false;
      this.destroyed = false;
      this.readable = true;
      this.writable = true;
      this.remoteAddress = options.host || null;
      this.remotePort = options.port || null;
    }

    connect(...args) {
      let callback = null;
      for (const value of args) {
        if (typeof value === "function") callback = value;
      }
      this.connecting = true;
      process.nextTick(() => {
        this.connecting = false;
        this.emit("connect");
        if (callback) callback();
      });
      return this;
    }

    destroy(error = null) {
      this.destroyed = true;
      if (error) this.emit("error", error);
      this.emit("close");
      return this;
    }

    end(chunk, encoding, callback) {
      super.end(chunk, encoding, callback);
      return this;
    }

    setKeepAlive() { return this; }
    setNoDelay() { return this; }
    setTimeout(_ms, callback) {
      if (typeof callback === "function") this.once("timeout", callback);
      return this;
    }
    address() {
      return this.remoteAddress == null || this.remotePort == null
        ? null
        : { address: this.remoteAddress, port: this.remotePort, family: isIP(this.remoteAddress) === 6 ? "IPv6" : "IPv4" };
    }
  }

  class Server extends TerraceEventEmitter {
    constructor(connectionListener = null) {
      super();
      this.listening = false;
      this._connectionListener = connectionListener;
      if (typeof connectionListener === "function") this.on("connection", connectionListener);
    }

    listen(...args) {
      const callback = args.find((value) => typeof value === "function") || null;
      this.listening = true;
      process.nextTick(() => {
        this.emit("listening");
        if (callback) callback();
      });
      return this;
    }

    close(callback) {
      this.listening = false;
      process.nextTick(() => {
        this.emit("close");
        if (typeof callback === "function") callback();
      });
      return this;
    }

    address() {
      return null;
    }
  }

  function isIPv4(value) {
    return /^(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})(\.(25[0-5]|2[0-4][0-9]|1?[0-9]{1,2})){3}$/.test(String(value));
  }

  function isIPv6(value) {
    return String(value).includes(":");
  }

  function isIP(value) {
    if (isIPv4(value)) return 4;
    if (isIPv6(value)) return 6;
    return 0;
  }

  function connect(...args) {
    const socket = new Socket();
    return socket.connect(...args);
  }

  return {
    BlockList: class BlockList {},
    Server,
    Socket,
    SocketAddress: class SocketAddress {},
    Stream: Socket,
    _createServerHandle() { return null; },
    _normalizeArgs(args) { return args; },
    connect,
    createConnection: connect,
    createServer(connectionListener = null) {
      return new Server(connectionListener);
    },
    getDefaultAutoSelectFamily() { return false; },
    getDefaultAutoSelectFamilyAttemptTimeout() { return 250; },
    isIP,
    isIPv4,
    isIPv6,
    setDefaultAutoSelectFamily() {},
    setDefaultAutoSelectFamilyAttemptTimeout() {},
  };
}

function __terraceHttpModule(protocol = "http:") {
  const events = __terraceEventsModule();
  const stream = __terraceStreamModule();
  const net = __terraceNetModule();
  const headerNamePattern = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
  const headerValuePattern = /[^\t\x20-\x7E\x80-\xFF]/;

  function validateHostProperty(options, name) {
    const value = options?.[name];
    if (value === undefined || value === null || typeof value === "string") return;
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "options.${name}" property must be of type string or one of undefined or null. Received ${__terraceDescribeReceivedValue(value)}`,
    );
  }

  class OutgoingMessage extends stream.Writable {
    constructor() {
      super();
      this._headers = Object.create(null);
      this.headersSent = false;
    }

    setHeader(name, value) {
      this._headers[String(name).toLowerCase()] = value;
      return this;
    }

    getHeader(name) {
      return this._headers[String(name).toLowerCase()];
    }

    getHeaders() {
      return { ...this._headers };
    }

    hasHeader(name) {
      return Object.prototype.hasOwnProperty.call(this._headers, String(name).toLowerCase());
    }

    removeHeader(name) {
      delete this._headers[String(name).toLowerCase()];
    }

    flushHeaders() {
      this.headersSent = true;
    }
  }

  class IncomingMessage extends stream.Readable {
    constructor(socket = null) {
      super();
      this.socket = socket;
      this.headers = Object.create(null);
      this.rawHeaders = [];
      this.trailers = Object.create(null);
      this.rawTrailers = [];
      this.statusCode = null;
      this.statusMessage = null;
      this.url = "";
      this.method = null;
      this.httpVersion = "1.1";
      this.complete = false;
    }
  }

  class ClientRequest extends OutgoingMessage {
    constructor(options = {}, callback = null) {
      super();
      this.agent = options.agent === false
        ? new (options._agentClass || Agent)({ keepAlive: false })
        : (options.agent ?? options._defaultAgent ?? null);
      this.protocol = options.protocol || protocol;
      this.aborted = false;
      this.destroyed = false;
      this.res = null;
      this.path = options.path || "/";
      this.method = options.method || "GET";
      this.host = options.host || options.hostname || null;
      this.port = options.port || (this.protocol === "https:" ? 443 : 80);
      if (typeof callback === "function") this.once("response", callback);
    }

    _dispatch() {
      if (this._didDispatch) return;
      this._didDispatch = true;
      process.nextTick(() => {
        this.emit("socket", new net.Socket({ host: this.host, port: this.port }));
        this.emit("error", __terraceUnsupportedNetworkError(this.protocol === "https:" ? "https" : "http"));
      });
    }

    abort() {
      this.aborted = true;
      this._dispatch();
      return this;
    }

    destroy(error = null) {
      this.destroyed = true;
      if (error) this.emit("error", error);
      this.emit("close");
      return this;
    }

    end(chunk, encoding, callback) {
      super.end(chunk, encoding, callback);
      this._dispatch();
      return this;
    }

    setNoDelay() { return this; }
    setSocketKeepAlive() { return this; }
    setTimeout(_ms, callback) {
      if (typeof callback === "function") this.once("timeout", callback);
      return this;
    }
  }

  class Agent extends events.EventEmitter {
    constructor(options = {}) {
      super();
      this.options = { ...options };
      this.protocol = protocol;
      this.defaultPort = protocol === "https:" ? 443 : 80;
      this.keepAlive = !!options.keepAlive;
      this.maxSockets = options.maxSockets ?? Infinity;
      this.maxTotalSockets = options.maxTotalSockets ?? Infinity;
      this.sockets = Object.create(null);
    }

    addRequest(req, options = {}) {
      process.nextTick(() => {
        req.emit("socket", new net.Socket({ host: options.host || options.hostname, port: options.port }));
      });
    }

    createConnection(options = {}) {
      return new net.Socket({ host: options.host || options.hostname, port: options.port });
    }

    getName(options = {}) {
      let name = options.host || "localhost";
      name += ":";
      if (options.port) name += options.port;
      name += ":";
      if (options.localAddress) name += options.localAddress;
      if (options.family === 4 || options.family === 6) name += `:${options.family}`;
      if (options.socketPath) name += `:${options.socketPath}`;
      return name;
    }
  }

  class ServerResponse extends OutgoingMessage {}
  class Server extends events.EventEmitter {
    listen(...args) {
      const callback = args.find((value) => typeof value === "function") || null;
      process.nextTick(() => {
        this.emit("listening");
        if (callback) callback();
      });
      return this;
    }
    close(callback) {
      process.nextTick(() => {
        this.emit("close");
        if (typeof callback === "function") callback();
      });
      return this;
    }
  }

  function request(...args) {
    let options = {};
    let callback = null;
    if (args.length > 0) {
      if (args[0] instanceof (__terraceUrlModule().URL)) {
        options = __terraceUrlModule().urlToHttpOptions(args[0]);
      } else if (typeof args[0] === "string") {
        try {
          options = __terraceUrlModule().urlToHttpOptions(new (__terraceUrlModule().URL)(args[0]));
        } catch {
          options = { path: String(args[0]) };
        }
      } else if (args[0] && typeof args[0] === "object") {
        options = { ...args[0] };
      }
    }
    if (args[1] && typeof args[1] === "object") options = { ...options, ...args[1] };
    callback = args.find((value) => typeof value === "function") || null;
    if (!options.protocol) options.protocol = protocol;
    validateHostProperty(options, "hostname");
    validateHostProperty(options, "host");
    options._defaultAgent = options._defaultAgent ?? globalAgent;
    options._agentClass = options._agentClass ?? Agent;
    return new ClientRequest(options, callback);
  }

  function get(...args) {
    const req = request(...args);
    req.end();
    return req;
  }

  const globalAgent = new Agent();

  return {
    Agent,
    ClientRequest,
    CloseEvent: globalThis.CloseEvent,
    IncomingMessage,
    METHODS: [...__terraceHttpMethods],
    MessageEvent: globalThis.MessageEvent,
    OutgoingMessage,
    STATUS_CODES: { ...__terraceHttpStatusCodes },
    Server,
    ServerResponse,
    WebSocket: globalThis.WebSocket,
    _connectionListener() {},
    createServer(requestListener = null) {
      const server = new Server();
      if (typeof requestListener === "function") server.on("request", requestListener);
      return server;
    },
    get,
    globalAgent,
    maxHeaderSize: 16384,
    request,
    setMaxIdleHTTPParsers() {},
    validateHeaderName(name) {
      const normalized = String(name);
      if (!headerNamePattern.test(normalized)) {
        const error = new TypeError(`Header name must be a valid HTTP token ["${normalized}"]`);
        error.code = "ERR_INVALID_HTTP_TOKEN";
        throw error;
      }
      return true;
    },
    validateHeaderValue(name, value) {
      const normalizedName = String(name);
      const normalizedValue = String(value);
      if (headerValuePattern.test(normalizedValue)) {
        const error = new TypeError(`Invalid character in header content ["${normalizedName}"]`);
        error.code = "ERR_INVALID_CHAR";
        throw error;
      }
      return true;
    },
  };
}

function __terraceHttpsModule() {
  const http = __terraceHttpModule("https:");
  class Agent extends http.Agent {
    constructor(options = {}) {
      super({ ...options });
      this.protocol = "https:";
      this.defaultPort = 443;
    }

    getName(options = {}) {
      let name = http.Agent.prototype.getName.call(this, options);
      const append = (value) => {
        name += ":";
        if (value !== undefined && value !== null && value !== false) name += value;
      };
      append(options.ca);
      append(options.cert);
      append(options.clientCertEngine);
      append(options.ciphers);
      append(options.key);
      append(options.pfx);
      name += ":";
      if (options.rejectUnauthorized !== undefined) name += options.rejectUnauthorized;
      name += ":";
      if (options.servername && options.servername !== options.host) name += options.servername;
      append(options.minVersion);
      append(options.maxVersion);
      append(options.secureProtocol);
      append(options.crl);
      name += ":";
      if (options.honorCipherOrder !== undefined) name += options.honorCipherOrder;
      append(options.ecdhCurve);
      append(options.dhparam);
      name += ":";
      if (options.secureOptions !== undefined) name += options.secureOptions;
      append(options.sessionIdContext);
      name += ":";
      if (options.sigalgs) name += JSON.stringify(options.sigalgs);
      append(options.privateKeyIdentifier);
      append(options.privateKeyEngine);
      return name;
    }
  }
  const globalAgent = new Agent();
  return {
    Agent,
    Server: http.Server,
    createServer: http.createServer,
    get(...args) {
      const req = http.request(...args);
      req.protocol = "https:";
      req.end();
      return req;
    },
    globalAgent,
    request(...args) {
      let options = {};
      if (typeof args[0] === "string") {
        try {
          options = __terraceUrlModule().urlToHttpOptions(new (__terraceUrlModule().URL)(args[0]));
          args = args.slice(1);
        } catch {
          options = { path: String(args[0]) };
          args = args.slice(1);
        }
      } else if (args[0] instanceof (__terraceUrlModule().URL)) {
        options = __terraceUrlModule().urlToHttpOptions(args[0]);
        args = args.slice(1);
      }
      if (args[0] && typeof args[0] === "object" && typeof args[0] !== "function") {
        options = { ...options, ...args[0] };
        args = args.slice(1);
      }
      const callback = args.find((value) => typeof value === "function") || null;
      options.protocol = "https:";
      options._defaultAgent = globalAgent;
      options._agentClass = Agent;
      return http.request(options, callback);
    },
  };
}

function __terraceHttp2Module() {
  class Http2ServerRequest extends TerraceEventEmitter {}
  class Http2ServerResponse extends TerraceEventEmitter {}
  class Http2Server extends TerraceEventEmitter {
    listen(...args) {
      const callback = args.find((value) => typeof value === "function") || null;
      process.nextTick(() => {
        this.emit("listening");
        if (callback) callback();
      });
      return this;
    }

    close(callback) {
      process.nextTick(() => {
        this.emit("close");
        if (typeof callback === "function") callback();
      });
      return this;
    }
  }

  const constants = {
    HTTP2_HEADER_STATUS: ":status",
    HTTP2_HEADER_METHOD: ":method",
    HTTP2_HEADER_AUTHORITY: ":authority",
    HTTP2_HEADER_SCHEME: ":scheme",
    HTTP2_HEADER_PATH: ":path",
    HTTP2_HEADER_PROTOCOL: ":protocol",
    HTTP2_HEADER_ACCEPT: "accept",
    HTTP2_HEADER_ACCEPT_ENCODING: "accept-encoding",
    HTTP2_HEADER_ACCEPT_LANGUAGE: "accept-language",
    HTTP2_HEADER_ACCEPT_RANGES: "accept-ranges",
    HTTP2_HEADER_AUTHORIZATION: "authorization",
    HTTP2_HEADER_CACHE_CONTROL: "cache-control",
    HTTP2_HEADER_CONNECTION: "connection",
    HTTP2_HEADER_CONTENT_DISPOSITION: "content-disposition",
    HTTP2_HEADER_CONTENT_ENCODING: "content-encoding",
    HTTP2_HEADER_CONTENT_LENGTH: "content-length",
    HTTP2_HEADER_CONTENT_TYPE: "content-type",
    HTTP2_HEADER_COOKIE: "cookie",
    HTTP2_HEADER_DATE: "date",
    HTTP2_HEADER_ETAG: "etag",
    HTTP2_HEADER_LOCATION: "location",
    HTTP2_HEADER_USER_AGENT: "user-agent",
    HTTP_STATUS_REQUEST_TIMEOUT: 408,
    HTTP_STATUS_TOO_MANY_REQUESTS: 429,
    HTTP_STATUS_INTERNAL_SERVER_ERROR: 500,
  };

  return {
    Http2ServerRequest,
    Http2ServerResponse,
    connect() {
      throw __terraceUnsupportedNetworkError("http2");
    },
    constants,
    createSecureServer() {
      return new Http2Server();
    },
    createServer() {
      return new Http2Server();
    },
    getDefaultSettings() {
      return {};
    },
    getPackedSettings() {
      return TerraceBuffer.alloc(0);
    },
    getUnpackedSettings() {
      return {};
    },
    performServerHandshake() {
      return {};
    },
    sensitiveHeaders: [],
  };
}

let __terraceDnsServers = ["127.0.0.1"];
let __terraceDnsDefaultResultOrder = "verbatim";

function __terraceDnsError(code, message = code) {
  const error = new Error(message);
  error.code = code;
  error.errno = code;
  error.syscall = "getaddrinfo";
  return error;
}

function __terraceDnsUnsupportedCallback(name, callback) {
  const error = __terraceDnsError("ENOTSUP", `dns.${name} is not implemented in this sandbox`);
  if (typeof callback === "function") {
    process.nextTick(() => callback(error));
    return;
  }
  throw error;
}

function __terraceDnsFamilyForAddress(address) {
  const family = __terraceNetModule().isIP(address);
  return family === 0 ? null : family;
}

function __terraceCreateResolverClass() {
  return class Resolver {
    constructor(options = {}) {
      this.timeout = options.timeout ?? -1;
      this.tries = options.tries ?? 4;
      this.maxTimeout = options.maxTimeout ?? 0;
      this._servers = [...__terraceDnsServers];
      this._activeQuery = false;
      this._handle = {
        getServers: () => [...this._servers],
      };
    }

    cancel() {
      this._activeQuery = false;
    }

    getServers() {
      return this._handle.getServers();
    }

    setServers(servers) {
      if (!Array.isArray(servers)) {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          'The "servers" argument must be of type Array. Received ' +
            `${servers === null ? "null" : typeof servers}`,
        );
      }
      if (this._activeQuery) {
        throw __terraceDnsError("ERR_DNS_SET_SERVERS_FAILED", "c-ares failed to set servers: query in progress");
      }
      this._servers = servers.map((server) => String(server));
      return undefined;
    }

    lookup(hostname, options, callback) {
      return __terraceDnsLookupImpl(hostname, options, callback);
    }

    lookupService(address, port, callback) {
      return __terraceDnsLookupServiceImpl(address, port, callback);
    }

    resolve(hostname, rrtype, callback) {
      return __terraceDnsResolveImpl("resolve", hostname, rrtype, callback);
    }

    resolve4(hostname, options, callback) { return __terraceDnsResolveImpl("resolve4", hostname, options, callback); }
    resolve6(hostname, options, callback) { return __terraceDnsResolveImpl("resolve6", hostname, options, callback); }
    resolveAny(hostname, callback) { return __terraceDnsResolveImpl("resolveAny", hostname, callback); }
    resolveCaa(hostname, callback) { return __terraceDnsResolveImpl("resolveCaa", hostname, callback); }
    resolveCname(hostname, callback) { return __terraceDnsResolveImpl("resolveCname", hostname, callback); }
    resolveMx(hostname, callback) { return __terraceDnsResolveImpl("resolveMx", hostname, callback); }
    resolveNaptr(hostname, callback) { return __terraceDnsResolveImpl("resolveNaptr", hostname, callback); }
    resolveNs(hostname, callback) { return __terraceDnsResolveImpl("resolveNs", hostname, callback); }
    resolvePtr(hostname, callback) { return __terraceDnsResolveImpl("resolvePtr", hostname, callback); }
    resolveSoa(hostname, callback) { return __terraceDnsResolveImpl("resolveSoa", hostname, callback); }
    resolveSrv(hostname, callback) { return __terraceDnsResolveImpl("resolveSrv", hostname, callback); }
    resolveTlsa(hostname, callback) { return __terraceDnsResolveImpl("resolveTlsa", hostname, callback); }
    resolveTxt(hostname, callback) { return __terraceDnsResolveImpl("resolveTxt", hostname, callback); }
    reverse(ip, callback) { return __terraceDnsResolveImpl("reverse", ip, callback); }
  };
}

function __terraceDnsNormalizeLookupOptions(options) {
  if (options == null || typeof options === "function") return {};
  if (typeof options === "number") return { family: options };
  if (typeof options === "object") return { ...options };
  throw __terraceNodeTypeError(
    "ERR_INVALID_ARG_TYPE",
    'The "options" argument must be of type object or number. Received ' +
      `${typeof options}`,
  );
}

function __terraceDnsLookupImpl(hostname, options, callback) {
  if (typeof options === "function") {
    callback = options;
    options = {};
  } else if (callback === undefined && options !== undefined && typeof options !== "object" && typeof options !== "number") {
    callback = options;
    options = {};
  }
  if (typeof hostname !== "string" && hostname !== false && hostname !== undefined && hostname !== null) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "hostname" argument must be of type string. Received ${__terraceDescribeReceivedValue(hostname)}`,
    );
  }
  if (typeof callback !== "function") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "callback" argument must be of type function. Received ${__terraceDescribeReceivedValue(callback)}`,
    );
  }
  const normalized = __terraceDnsNormalizeLookupOptions(options);
  if (normalized.family !== undefined && normalized.family !== 0 && normalized.family !== 4 && normalized.family !== 6) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_VALUE",
      `The property 'options.family' must be one of: 0, 4, 6. Received ${String(normalized.family)}`,
    );
  }
  if (normalized.hints !== undefined && typeof normalized.hints !== "number") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "options.hints" property must be of type number. Received ${__terraceDescribeReceivedValue(normalized.hints)}`,
    );
  }
  if (normalized.all !== undefined && typeof normalized.all !== "boolean") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "options.all" property must be of type boolean. Received ${__terraceDescribeReceivedValue(normalized.all)}`,
    );
  }
  if (normalized.verbatim !== undefined && typeof normalized.verbatim !== "boolean") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "options.verbatim" property must be of type boolean. Received ${__terraceDescribeReceivedValue(normalized.verbatim)}`,
    );
  }
  const family = __terraceDnsFamilyForAddress(hostname);
  process.nextTick(() => {
    if (family != null) {
      if (normalized.all) {
        callback(null, [{ address: String(hostname), family }]);
      } else {
        callback(null, String(hostname), family);
      }
      return;
    }
    callback(__terraceDnsError("ENOTFOUND", `getaddrinfo ENOTFOUND ${String(hostname)}`));
  });
}

function __terraceDnsLookupServiceImpl(address, port, callback) {
  if (typeof callback !== "function") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      'The "callback" argument must be of type function. Received ' +
        `${callback === null ? "null" : typeof callback}`,
    );
  }
  process.nextTick(() => {
    callback(__terraceDnsError("ENOTFOUND", "lookupService ENOTFOUND"), undefined, undefined);
  });
}

function __terraceDnsResolveImpl(name, ...args) {
  const callback = args.find((value) => typeof value === "function");
  return __terraceDnsUnsupportedCallback(name, callback);
}

function __terraceDnsPromisesModule(resolverClass) {
  const promises = {};
  promises.lookup = (hostname, options) =>
    new Promise((resolve, reject) => {
      __terraceDnsLookupImpl(hostname, options, (error, address, familyOrRecords) => {
        if (error) {
          reject(error);
        } else if (options && typeof options === "object" && options.all) {
          resolve(address);
        } else {
          resolve({ address, family: familyOrRecords });
        }
      });
    });
  promises.lookupService = (address, port) =>
    new Promise((resolve, reject) => {
      __terraceDnsLookupServiceImpl(address, port, (error, hostname, service) => {
        if (error) reject(error);
        else resolve({ hostname, service });
      });
    });
  for (const name of [
    "resolve",
    "resolve4",
    "resolve6",
    "resolveAny",
    "resolveCaa",
    "resolveCname",
    "resolveMx",
    "resolveNaptr",
    "resolveNs",
    "resolvePtr",
    "resolveSoa",
    "resolveSrv",
    "resolveTlsa",
    "resolveTxt",
    "reverse",
  ]) {
    promises[name] = (...args) => new Promise((_resolve, reject) => reject(__terraceDnsError("ENOTSUP", `dns.promises.${name} is not implemented in this sandbox`)));
  }
  promises.Resolver = class Resolver extends resolverClass {
    lookup(hostname, options) {
      return promises.lookup(hostname, options);
    }
    lookupService(address, port) {
      return promises.lookupService(address, port);
    }
  };
  return promises;
}

function __terraceDnsModule() {
  if (__terraceDnsSingleton) return __terraceDnsSingleton;
  const Resolver = __terraceCreateResolverClass();
  const constants = {
    ADDRCONFIG: 1024,
    ADDRGETNETWORKPARAMS: 7,
    ALL: 16,
    BADFAMILY: "EBADFAMILY",
    BADFLAGS: "EBADFLAGS",
    BADHINTS: "EBADHINTS",
    BADNAME: "EBADNAME",
    BADQUERY: "EBADQUERY",
    BADRESP: "EBADRESP",
    BADSTR: "EBADSTR",
    CANCELLED: "ECANCELLED",
    CONNREFUSED: "ECONNREFUSED",
    DESTRUCTION: "EDESTRUCTION",
    EOF: "EOF",
    FILE: "EFILE",
    FORMERR: "EFORMERR",
    LOADIPHLPAPI: "ELOADIPHLPAPI",
    NODATA: "ENODATA",
    NOMEM: "ENOMEM",
    NONAME: "ENONAME",
    NOTFOUND: "ENOTFOUND",
    NOTIMP: "ENOTIMP",
    NOTINITIALIZED: "ENOTINITIALIZED",
    REFUSED: "EREFUSED",
    SERVFAIL: "ESERVFAIL",
    TIMEOUT: "ETIMEOUT",
    V4MAPPED: 8,
  };
  const module = {
    ...constants,
    Resolver,
    getDefaultResultOrder() {
      return __terraceDnsDefaultResultOrder;
    },
    getServers() {
      return [...__terraceDnsServers];
    },
    lookup: __terraceDnsLookupImpl,
    lookupService: __terraceDnsLookupServiceImpl,
    promises: __terraceDnsPromisesModule(Resolver),
    resolve(hostname, rrtype, callback) { return __terraceDnsResolveImpl("resolve", hostname, rrtype, callback); },
    resolve4(hostname, options, callback) { return __terraceDnsResolveImpl("resolve4", hostname, options, callback); },
    resolve6(hostname, options, callback) { return __terraceDnsResolveImpl("resolve6", hostname, options, callback); },
    resolveAny(hostname, callback) { return __terraceDnsResolveImpl("resolveAny", hostname, callback); },
    resolveCaa(hostname, callback) { return __terraceDnsResolveImpl("resolveCaa", hostname, callback); },
    resolveCname(hostname, callback) { return __terraceDnsResolveImpl("resolveCname", hostname, callback); },
    resolveMx(hostname, callback) { return __terraceDnsResolveImpl("resolveMx", hostname, callback); },
    resolveNaptr(hostname, callback) { return __terraceDnsResolveImpl("resolveNaptr", hostname, callback); },
    resolveNs(hostname, callback) { return __terraceDnsResolveImpl("resolveNs", hostname, callback); },
    resolvePtr(hostname, callback) { return __terraceDnsResolveImpl("resolvePtr", hostname, callback); },
    resolveSoa(hostname, callback) { return __terraceDnsResolveImpl("resolveSoa", hostname, callback); },
    resolveSrv(hostname, callback) { return __terraceDnsResolveImpl("resolveSrv", hostname, callback); },
    resolveTlsa(hostname, callback) { return __terraceDnsResolveImpl("resolveTlsa", hostname, callback); },
    resolveTxt(hostname, callback) { return __terraceDnsResolveImpl("resolveTxt", hostname, callback); },
    reverse(ip, callback) { return __terraceDnsResolveImpl("reverse", ip, callback); },
    setDefaultResultOrder(order) {
      if (order !== "ipv4first" && order !== "ipv6first" && order !== "verbatim") {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_VALUE",
          `The argument 'order' must be one of: 'verbatim', 'ipv4first', 'ipv6first'. Received ${String(order)}`,
        );
      }
      __terraceDnsDefaultResultOrder = order;
    },
    setServers(servers) {
      if (!Array.isArray(servers)) {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          'The "servers" argument must be of type Array. Received ' +
            `${servers === null ? "null" : typeof servers}`,
        );
      }
      __terraceDnsServers = servers.map((server) => String(server));
    },
  };
  module.promises = __terraceDnsPromisesModule(Resolver);
  for (const key of [
    "ADDRGETNETWORKPARAMS",
    "BADFAMILY",
    "BADFLAGS",
    "BADHINTS",
    "BADNAME",
    "BADQUERY",
    "BADRESP",
    "BADSTR",
    "CANCELLED",
    "CONNREFUSED",
    "DESTRUCTION",
    "EOF",
    "FILE",
    "FORMERR",
    "LOADIPHLPAPI",
    "NODATA",
    "NOMEM",
    "NONAME",
    "NOTFOUND",
    "NOTIMP",
    "NOTINITIALIZED",
    "REFUSED",
    "SERVFAIL",
    "TIMEOUT",
  ]) {
    module.promises[key] = module[key];
  }
  module.promises.getServers = () => module.getServers();
  module.promises.setServers = (servers) => module.setServers(servers);
  module.promises.getDefaultResultOrder = () => module.getDefaultResultOrder();
  module.promises.setDefaultResultOrder = (order) => module.setDefaultResultOrder(order);
  __terraceDnsSingleton = module;
  return __terraceDnsSingleton;
}

function __terraceTlsModule() {
  if (__terraceTlsSingleton) return __terraceTlsSingleton;
  const events = __terraceEventsModule();
  const net = __terraceNetModule();
  const validTlsVersions = new Set(["TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3"]);

  function validateTlsCommonOptions(options = {}) {
    if (options.ciphers !== undefined && typeof options.ciphers !== "string") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.ciphers" property must be of type string. Received ${__terraceDescribeReceivedValue(options.ciphers)}`,
      );
    }
    if (options.passphrase !== undefined && typeof options.passphrase !== "string") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.passphrase" property must be of type string. Received ${__terraceDescribeReceivedValue(options.passphrase)}`,
      );
    }
    if (options.ecdhCurve !== undefined && typeof options.ecdhCurve !== "string") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.ecdhCurve" property must be of type string. Received ${__terraceDescribeReceivedValue(options.ecdhCurve)}`,
      );
    }
    if (options.handshakeTimeout !== undefined && typeof options.handshakeTimeout !== "number") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.handshakeTimeout" property must be of type number. Received ${__terraceDescribeReceivedValue(options.handshakeTimeout)}`,
      );
    }
    if (options.sessionTimeout !== undefined && typeof options.sessionTimeout !== "number") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.sessionTimeout" property must be of type number. Received ${__terraceDescribeReceivedValue(options.sessionTimeout)}`,
      );
    }
    if (
      options.ticketKeys !== undefined &&
      !TerraceBuffer.isBuffer(options.ticketKeys) &&
      !(options.ticketKeys instanceof Uint8Array) &&
      !ArrayBuffer.isView(options.ticketKeys)
    ) {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "options.ticketKeys" property must be an instance of Buffer, TypedArray, or DataView. Received ${__terraceDescribeReceivedValue(options.ticketKeys)}`,
      );
    }
    if (options.minVersion !== undefined && !validTlsVersions.has(options.minVersion)) {
      const error = new TypeError(`"${String(options.minVersion)}" is not a valid minimum TLS protocol version`);
      error.code = "ERR_TLS_INVALID_PROTOCOL_VERSION";
      throw error;
    }
    if (options.maxVersion !== undefined && !validTlsVersions.has(options.maxVersion)) {
      const error = new TypeError(`"${String(options.maxVersion)}" is not a valid maximum TLS protocol version`);
      error.code = "ERR_TLS_INVALID_PROTOCOL_VERSION";
      throw error;
    }
  }

  function parseAltNames(value) {
    return String(value || "")
      .split(/,\s*/)
      .filter(Boolean);
  }

  function canonicalizeIp(value) {
    return String(value || "").replace(/^\[|\]$/g, "");
  }
  class SecureContext {
    constructor(options = {}) {
      validateTlsCommonOptions(options);
      this.context = Object.freeze({ ...options });
    }
  }
  class TLSSocket extends net.Socket {
    constructor(socket = undefined, options = {}) {
      super(options);
      this.encrypted = true;
      this.authorized = false;
      this.authorizationError = null;
      this.secureConnecting = false;
      this.alpnProtocol = false;
      this.servername = options.servername || undefined;
      this._secureContext = options.secureContext || new SecureContext(options);
      if (socket && typeof socket === "object") {
        this._wrappedSocket = socket;
      }
    }
    disableRenegotiation() {}
    enableTrace() {}
    exportKeyingMaterial() { return TerraceBuffer.alloc(0); }
    getCipher() { return null; }
    getEphemeralKeyInfo() { return null; }
    getFinished() { return null; }
    getPeerCertificate() { return {}; }
    getProtocol() { return null; }
    getSession() { return undefined; }
    getSharedSigalgs() { return []; }
    getTLSTicket() { return undefined; }
    getX509Certificate() { return undefined; }
    isSessionReused() { return false; }
    renegotiate(options, callback) {
      if (typeof callback === "function") process.nextTick(callback);
      return false;
    }
    setKeyCert() { return this; }
    setMaxSendFragment() { return true; }
    setServername(servername) {
      if (this._isServer) {
        throw __terraceNodeTypeError("ERR_TLS_SNI_FROM_SERVER", "Cannot issue SNI from a TLS server-side socket");
      }
      this.servername = servername;
    }
  }
  class Server extends events.EventEmitter {
    constructor(options = {}, secureConnectionListener = null) {
      super();
      validateTlsCommonOptions(options);
      this._tlsOptions = { ...options };
      if (typeof secureConnectionListener === "function") {
        this.on("secureConnection", secureConnectionListener);
      }
    }
    addContext() { return this; }
    close(callback) {
      process.nextTick(() => {
        this.emit("close");
        if (typeof callback === "function") callback();
      });
      return this;
    }
    listen(...args) {
      const callback = args.find((value) => typeof value === "function") || null;
      process.nextTick(() => {
        this.emit("listening");
        if (callback) callback();
      });
      return this;
    }
    setSecureContext(options = {}) {
      this._tlsOptions = { ...this._tlsOptions, ...options };
      return this;
    }
  }
  const rootCertificates = Object.freeze([]);
  const defaultCACertificates = Object.freeze([]);
  __terraceTlsSingleton = {
    CLIENT_RENEG_LIMIT: 3,
    CLIENT_RENEG_WINDOW: 600,
    DEFAULT_CIPHERS: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:DHE-RSA-AES128-SHA256:ECDHE-RSA-AES256-SHA384:DHE-RSA-AES256-SHA384:ECDHE-RSA-AES256-SHA256:DHE-RSA-AES256-SHA256:HIGH:!aNULL:!eNULL:!EXPORT:!DES:!RC4:!MD5:!PSK:!SRP:!CAMELLIA",
    DEFAULT_ECDH_CURVE: "auto",
    DEFAULT_MAX_VERSION: "TLSv1.3",
    DEFAULT_MIN_VERSION: "TLSv1.2",
    SecureContext,
    Server,
    TLSSocket,
    checkServerIdentity(hostname, cert) {
      hostname = String(hostname);
      const altNames = parseAltNames(cert?.subjectaltname);
      const ipNames = altNames
        .filter((entry) => entry.startsWith("IP Address:"))
        .map((entry) => canonicalizeIp(entry.slice("IP Address:".length)));
      if (__terraceNetModule().isIP(hostname)) {
        if (ipNames.includes(canonicalizeIp(hostname))) return undefined;
        const error = new Error(
          `Hostname/IP does not match certificate's altnames: IP: ${hostname} is not in the cert's list: ${ipNames.join(", ")}`,
        );
        error.code = "ERR_TLS_CERT_ALTNAME_INVALID";
        return error;
      }
      const commonName = cert?.subject?.CN;
      if (commonName && String(commonName).replace(/[.]$/, "") === hostname.replace(/[.]$/, "")) {
        return undefined;
      }
      if (commonName) {
        const error = new Error(
          `Hostname/IP does not match certificate's altnames: Host: ${hostname}. is not cert's CN: ${commonName}`,
        );
        error.code = "ERR_TLS_CERT_ALTNAME_INVALID";
        return error;
      }
      return undefined;
    },
    connect(...args) {
      const callback = args.find((value) => typeof value === "function") || null;
      let options = args.find((value) => value && typeof value === "object") || {};
      if (typeof args[0] === "number") options = { ...options, port: args[0] };
      validateTlsCommonOptions(options);
      if (Object.prototype.hasOwnProperty.call(options, "checkServerIdentity") &&
          options.checkServerIdentity !== undefined &&
          typeof options.checkServerIdentity !== "function") {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          `The "options.checkServerIdentity" property must be of type function. Received ${__terraceDescribeReceivedValue(options.checkServerIdentity)}`,
        );
      }
      const socket = new TLSSocket(undefined, options);
      process.nextTick(() => {
        if (callback) callback.call(socket);
        socket.emit("error", __terraceUnsupportedNetworkError("tls"));
      });
      return socket;
    },
    convertALPNProtocols(value, out = {}) {
      if (Array.isArray(value)) {
        let total = 0;
        const lengths = value.map((entry) => {
          const length = TerraceBuffer.byteLength(String(entry));
          total += 1 + length;
          return length;
        });
        const buffer = TerraceBuffer.alloc(total);
        let offset = 0;
        value.forEach((entry, index) => {
          buffer[offset++] = lengths[index];
          buffer.write(String(entry), offset);
          offset += lengths[index];
        });
        out.ALPNProtocols = buffer;
      } else if (value instanceof Uint8Array) {
        out.ALPNProtocols = TerraceBuffer.from(value);
      } else if (ArrayBuffer.isView(value)) {
        out.ALPNProtocols = TerraceBuffer.from(
          new Uint8Array(value.buffer, value.byteOffset, value.byteLength),
        );
      }
      return undefined;
    },
    createSecureContext(options = {}) {
      return new SecureContext(options);
    },
    createServer(options = {}, secureConnectionListener = null) {
      return new Server(options, secureConnectionListener);
    },
    getCACertificates(kind = "default") {
      return kind === "default" ? defaultCACertificates : rootCertificates;
    },
    getCiphers() {
      return Object.freeze([
        "tls_aes_256_gcm_sha384",
        "tls_chacha20_poly1305_sha256",
        "tls_aes_128_gcm_sha256",
      ]);
    },
    rootCertificates,
    setDefaultCACertificates(certs) {
      if (!Array.isArray(certs)) {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          `The "certs" argument must be of type Array. Received ${__terraceDescribeReceivedValue(certs)}`,
        );
      }
    },
  };
  return __terraceTlsSingleton;
}

const __terraceZlibConstants = Object.freeze(
  Object.assign(Object.create(null), {
    Z_NO_FLUSH: 0,
    Z_PARTIAL_FLUSH: 1,
    Z_SYNC_FLUSH: 2,
    Z_FULL_FLUSH: 3,
    Z_FINISH: 4,
    Z_BLOCK: 5,
    Z_OK: 0,
    Z_STREAM_END: 1,
    Z_NEED_DICT: 2,
    Z_ERRNO: -1,
    Z_STREAM_ERROR: -2,
    Z_DATA_ERROR: -3,
    Z_MEM_ERROR: -4,
    Z_BUF_ERROR: -5,
    Z_VERSION_ERROR: -6,
    Z_NO_COMPRESSION: 0,
    Z_BEST_SPEED: 1,
    Z_BEST_COMPRESSION: 9,
    Z_DEFAULT_COMPRESSION: -1,
    Z_FILTERED: 1,
    Z_HUFFMAN_ONLY: 2,
    Z_RLE: 3,
    Z_FIXED: 4,
    Z_DEFAULT_STRATEGY: 0,
    DEFLATE: 1,
    INFLATE: 2,
    GZIP: 3,
    GUNZIP: 4,
    DEFLATERAW: 5,
    INFLATERAW: 6,
    UNZIP: 7,
    BROTLI_DECODE: 8,
    BROTLI_ENCODE: 9,
    Z_MIN_WINDOWBITS: 8,
    Z_MAX_WINDOWBITS: 15,
    Z_DEFAULT_WINDOWBITS: 15,
    Z_MIN_CHUNK: 64,
    Z_MAX_CHUNK: Infinity,
    Z_DEFAULT_CHUNK: 16384,
    Z_MIN_MEMLEVEL: 1,
    Z_MAX_MEMLEVEL: 9,
    Z_DEFAULT_MEMLEVEL: 8,
    Z_MIN_LEVEL: -1,
    Z_MAX_LEVEL: 9,
    Z_DEFAULT_LEVEL: -1,
    BROTLI_OPERATION_PROCESS: 0,
    BROTLI_OPERATION_FLUSH: 1,
    BROTLI_OPERATION_FINISH: 2,
    BROTLI_DEFAULT_QUALITY: 11,
    BROTLI_PARAM_QUALITY: 1,
  }),
);

function __terraceZlibNormalizeInput(input, encoding = "utf8") {
  if (TerraceBuffer.isBuffer(input)) return input;
  if (input instanceof Uint8Array || ArrayBuffer.isView(input) || input instanceof ArrayBuffer) {
    return TerraceBuffer.from(input);
  }
  if (typeof input === "string") {
    return TerraceBuffer.from(input, encoding || "utf8");
  }
  throw __terraceNodeTypeError(
    "ERR_INVALID_ARG_TYPE",
    'The "buffer" argument must be of type string or an instance of Buffer, TypedArray, DataView, or ArrayBuffer.',
  );
}

function __terraceZlibNormalizeOptions(options) {
  if (options == null) return {};
  if (typeof options !== "object") {
    throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "options" argument must be of type object.');
  }
  return { ...options };
}

function __terraceZlibNormalizeCallbackArguments(options, callback) {
  if (typeof options === "function") {
    return [{}, options];
  }
  return [__terraceZlibNormalizeOptions(options), callback];
}

function __terraceZlibIsCompressionMode(mode) {
  return (
    mode === "gzip" ||
    mode === "deflate" ||
    mode === "deflateRaw" ||
    mode === "brotliCompress" ||
    mode === "zstdCompress"
  );
}

function __terraceZlibProcessSync(mode, input, options = {}, finalize = true) {
  return TerraceBuffer.from(
    __terraceZlibProcess(mode, __terraceZlibNormalizeInput(input), !!finalize, options || {}),
  );
}

function __terraceZlibWrapInfo(buffer, engine, options) {
  if (options && options.info) {
    return { buffer, engine };
  }
  return buffer;
}

function __terraceZlibCrc32Value(input, seed = 0) {
  return Number(__terraceZlibCrc32(__terraceZlibNormalizeInput(input), Number(seed) >>> 0)) >>> 0;
}

class TerraceZlibBinding extends TerraceEventEmitter {
  constructor(mode, options = {}) {
    super();
    this._mode = mode;
    this._options = __terraceZlibNormalizeOptions(options);
    this._level = typeof this._options.level === "number"
      ? this._options.level
      : __terraceZlibConstants.Z_DEFAULT_COMPRESSION;
    this._strategy = typeof this._options.strategy === "number"
      ? this._options.strategy
      : __terraceZlibConstants.Z_DEFAULT_STRATEGY;
    this._flushFlag = typeof this._options.flush === "number" ? this._options.flush : __terraceZlibConstants.Z_NO_FLUSH;
    this._finishFlushFlag = typeof this._options.finishFlush === "number"
      ? this._options.finishFlush
      : __terraceZlibConstants.Z_FINISH;
    this._fullFlushFlag = typeof this._options.fullFlush === "number"
      ? this._options.fullFlush
      : __terraceZlibConstants.Z_FULL_FLUSH;
    this._chunks = [];
    this._readQueue = [];
    this._emittedLength = 0;
    this._closed = false;
    this._ended = false;
    this._paused = false;
    this._streamId = __terraceZlibStreamCreate(mode, this._options);
    this._handle = {
      close() {},
    };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    if (this._streamId !== null && this._streamId !== undefined) {
      __terraceZlibStreamClose(this._streamId);
    }
    this.emit("close");
  }

  reset() {
    if (this._streamId !== null && this._streamId !== undefined) {
      __terraceZlibStreamReset(this._streamId);
    } else {
      this._chunks = [];
      this._readQueue = [];
      this._emittedLength = 0;
    }
  }

  pause() {
    this._paused = true;
    return this;
  }

  resume() {
    if (!this._paused) return this;
    this._paused = false;
    this.emit("resume");
    return this;
  }

  params(level, strategy, callback) {
    level = Number(level);
    strategy = Number(strategy);
    if (this._level === level && this._strategy === strategy) {
      if (typeof callback === "function") {
        queueMicrotask(() => callback(null));
      }
      return;
    }
    this.flush(__terraceZlibConstants.Z_SYNC_FLUSH, () => {
      this._options.level = level;
      this._options.strategy = strategy;
      if (this._streamId !== null && this._streamId !== undefined) {
        __terraceZlibStreamSetParams(this._streamId, level, strategy);
      }
      this._level = level;
      this._strategy = strategy;
      if (typeof callback === "function") {
        callback(null);
      }
    });
  }

  _queueOutput(buffer) {
    if (!buffer || buffer.byteLength === 0) return;
    const chunk = TerraceBuffer.from(buffer);
    this._readQueue.push(chunk);
    this.emit("readable");
    this.emit("data", chunk);
  }

  _processAccumulated(flushFlag) {
    if (this._closed) {
      const error = new Error("zlib binding closed");
      error.code = "ERR_ZLIB_BINDING_CLOSED";
      throw error;
    }
    const finalize = flushFlag === __terraceZlibConstants.Z_FINISH;
    const input = TerraceBuffer.concat(this._chunks);
    const result = __terraceZlibProcessSync(this._mode, input, this._options, finalize);
    const delta = result.subarray(this._emittedLength);
    this._emittedLength = result.byteLength;
    return TerraceBuffer.from(delta);
  }

  _processChunk(chunk, flushFlag = __terraceZlibConstants.Z_FINISH) {
    const normalizedChunk = chunk == null ? null : __terraceZlibNormalizeInput(chunk);
    if (this._streamId !== null && this._streamId !== undefined) {
      try {
        return TerraceBuffer.from(
          __terraceZlibStreamProcess(
            this._streamId,
            normalizedChunk == null ? TerraceBuffer.alloc(0) : normalizedChunk,
            flushFlag,
          ),
        );
      } catch (error) {
        this.emit("error", error);
        throw error;
      }
    }
    if (normalizedChunk != null && normalizedChunk.byteLength > 0) {
      this._chunks.push(normalizedChunk);
    }
    try {
      return this._processAccumulated(flushFlag);
    } catch (error) {
      this.emit("error", error);
      throw error;
    }
  }

  write(chunk, encoding, callback) {
    if (typeof encoding === "function") {
      callback = encoding;
      encoding = undefined;
    }
    const data = __terraceZlibNormalizeInput(chunk, encoding || "utf8");
    const output = this._processChunk(data, this._flushFlag);
    this._queueOutput(output);
    if (typeof callback === "function") {
      process.nextTick(() => callback());
    }
    return true;
  }

  flush(kind, callback) {
    if (typeof kind === "function") {
      callback = kind;
      kind = this._fullFlushFlag;
    }
    const output = this._processChunk(null, kind ?? this._fullFlushFlag);
    this._queueOutput(output);
    if (typeof callback === "function") {
      process.nextTick(() => callback());
    }
  }

  end(chunk, encoding, callback) {
    if (typeof chunk === "function") {
      callback = chunk;
      chunk = undefined;
      encoding = undefined;
    } else if (typeof encoding === "function") {
      callback = encoding;
      encoding = undefined;
    }
    const output = this._processChunk(
      chunk != null ? __terraceZlibNormalizeInput(chunk, encoding || "utf8") : null,
      this._finishFlushFlag,
    );
    this._queueOutput(output);
    this._ended = true;
    if (typeof callback === "function") {
      this.once("finish", callback);
    }
    process.nextTick(() => {
      this.emit("finish");
      this.emit("end");
      this.close();
    });
    return this;
  }

  read() {
    return this._readQueue.length > 0 ? this._readQueue.shift() : null;
  }

  get readableLength() {
    return this._readQueue.reduce((total, chunk) => total + (chunk?.length ?? 0), 0);
  }
}

class Deflate extends TerraceZlibBinding {
  constructor(options = {}) {
    super("deflate", options);
  }
}
class Inflate extends TerraceZlibBinding {
  constructor(options = {}) {
    super("inflate", options);
  }
}
class Gzip extends TerraceZlibBinding {
  constructor(options = {}) {
    super("gzip", options);
  }
}
class Gunzip extends TerraceZlibBinding {
  constructor(options = {}) {
    super("gunzip", options);
  }
}
class DeflateRaw extends TerraceZlibBinding {
  constructor(options = {}) {
    super("deflateRaw", options);
  }
}
class InflateRaw extends TerraceZlibBinding {
  constructor(options = {}) {
    super("inflateRaw", options);
  }
}
class Unzip extends TerraceZlibBinding {
  constructor(options = {}) {
    super("unzip", options);
  }
}
class BrotliCompress extends TerraceZlibBinding {
  constructor(options = {}) {
    super("brotliCompress", options);
  }
}
class BrotliDecompress extends TerraceZlibBinding {
  constructor(options = {}) {
    super("brotliDecompress", options);
  }
}
class ZstdCompress extends TerraceZlibBinding {
  constructor(options = {}) {
    super("zstdCompress", options);
  }
}
class ZstdDecompress extends TerraceZlibBinding {
  constructor(options = {}) {
    super("zstdDecompress", options);
  }
}

function __terraceZlibAsync(mode, Engine, input, options, callback) {
  const [normalizedOptions, normalizedCallback] = __terraceZlibNormalizeCallbackArguments(options, callback);
  if (typeof normalizedCallback !== "function") {
    throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "callback" argument must be of type function.');
  }
  queueMicrotask(() => {
    try {
      const engine = new Engine(normalizedOptions);
      const buffer = __terraceZlibProcessSync(mode, input, normalizedOptions, true);
      normalizedCallback(null, __terraceZlibWrapInfo(buffer, engine, normalizedOptions));
    } catch (error) {
      normalizedCallback(error);
    }
  });
}

function __terraceZlibSync(mode, Engine, input, options) {
  const normalizedOptions = __terraceZlibNormalizeOptions(options);
  const engine = new Engine(normalizedOptions);
  const buffer = __terraceZlibProcessSync(mode, input, normalizedOptions, true);
  return __terraceZlibWrapInfo(buffer, engine, normalizedOptions);
}

function __terraceZlibModule() {
  if (__terraceZlibSingleton) return __terraceZlibSingleton;
  const module = {
    constants: __terraceZlibConstants,
    crc32: __terraceZlibCrc32Value,
    Zlib: TerraceZlibBinding,
    Deflate,
    Inflate,
    Gzip,
    Gunzip,
    DeflateRaw,
    InflateRaw,
    Unzip,
    BrotliCompress,
    BrotliDecompress,
    ZstdCompress,
    ZstdDecompress,
    createDeflate(options) { return new Deflate(options); },
    createInflate(options) { return new Inflate(options); },
    createGzip(options) { return new Gzip(options); },
    createGunzip(options) { return new Gunzip(options); },
    createDeflateRaw(options) { return new DeflateRaw(options); },
    createInflateRaw(options) { return new InflateRaw(options); },
    createUnzip(options) { return new Unzip(options); },
    createBrotliCompress(options) { return new BrotliCompress(options); },
    createBrotliDecompress(options) { return new BrotliDecompress(options); },
    createZstdCompress(options) { return new ZstdCompress(options); },
    createZstdDecompress(options) { return new ZstdDecompress(options); },
    deflate(input, options, callback) { return __terraceZlibAsync("deflate", Deflate, input, options, callback); },
    inflate(input, options, callback) { return __terraceZlibAsync("inflate", Inflate, input, options, callback); },
    gzip(input, options, callback) { return __terraceZlibAsync("gzip", Gzip, input, options, callback); },
    gunzip(input, options, callback) { return __terraceZlibAsync("gunzip", Gunzip, input, options, callback); },
    deflateRaw(input, options, callback) { return __terraceZlibAsync("deflateRaw", DeflateRaw, input, options, callback); },
    inflateRaw(input, options, callback) { return __terraceZlibAsync("inflateRaw", InflateRaw, input, options, callback); },
    unzip(input, options, callback) { return __terraceZlibAsync("unzip", Unzip, input, options, callback); },
    brotliCompress(input, options, callback) { return __terraceZlibAsync("brotliCompress", BrotliCompress, input, options, callback); },
    brotliDecompress(input, options, callback) { return __terraceZlibAsync("brotliDecompress", BrotliDecompress, input, options, callback); },
    zstdCompress(input, options, callback) { return __terraceZlibAsync("zstdCompress", ZstdCompress, input, options, callback); },
    zstdDecompress(input, options, callback) { return __terraceZlibAsync("zstdDecompress", ZstdDecompress, input, options, callback); },
    deflateSync(input, options) { return __terraceZlibSync("deflate", Deflate, input, options); },
    inflateSync(input, options) { return __terraceZlibSync("inflate", Inflate, input, options); },
    gzipSync(input, options) { return __terraceZlibSync("gzip", Gzip, input, options); },
    gunzipSync(input, options) { return __terraceZlibSync("gunzip", Gunzip, input, options); },
    deflateRawSync(input, options) { return __terraceZlibSync("deflateRaw", DeflateRaw, input, options); },
    inflateRawSync(input, options) { return __terraceZlibSync("inflateRaw", InflateRaw, input, options); },
    unzipSync(input, options) { return __terraceZlibSync("unzip", Unzip, input, options); },
    brotliCompressSync(input, options) { return __terraceZlibSync("brotliCompress", BrotliCompress, input, options); },
    brotliDecompressSync(input, options) { return __terraceZlibSync("brotliDecompress", BrotliDecompress, input, options); },
    zstdCompressSync(input, options) { return __terraceZlibSync("zstdCompress", ZstdCompress, input, options); },
    zstdDecompressSync(input, options) { return __terraceZlibSync("zstdDecompress", ZstdDecompress, input, options); },
  };
  __terraceZlibSingleton = module;
  return module;
}

function __terraceChildProcessModule() {
  const eventsApi = __terraceEventsModule();
  const streamApi = __terraceBuiltin("stream");
  const pathApi = __terracePathModule();

  class ChildReadable extends streamApi.Readable {
    constructor() {
      super();
      this.readable = true;
      this.destroyed = false;
      this._buffer = TerraceBuffer.alloc(0);
      this._encoding = null;
      this._delivered = false;
      this._paused = false;
    }
    setEncoding(encoding) {
      this._encoding = encoding || "utf8";
      return this;
    }
    pause() {
      this._paused = true;
      return this;
    }
    resume() {
      this._paused = false;
      this._flush();
      return this;
    }
    pipe(destination) {
      this.on("data", (chunk) => destination.write?.(chunk));
      this.on("end", () => destination.end?.());
      this.resume();
      return destination;
    }
    _setText(value) {
      this._buffer = TerraceBuffer.from(value || "", "utf8");
    }
    _flush() {
      if (this._delivered || this._paused) return;
      this._delivered = true;
      if (this._buffer.byteLength > 0) {
        const chunk = this._encoding ? this._buffer.toString(this._encoding) : this._buffer;
        this.emit("data", chunk);
      }
      this.emit("end");
      this.emit("close");
    }
  }

  class ChildWritable extends streamApi.Writable {
    constructor() {
      super();
      this.writable = true;
      this.destroyed = false;
      this._chunks = [];
      this._encoding = "utf8";
      this._ended = false;
    }
    setDefaultEncoding(encoding) {
      this._encoding = encoding || "utf8";
      return this;
    }
    write(chunk, encoding, callback) {
      if (typeof encoding === "function") {
        callback = encoding;
        encoding = undefined;
      }
      if (this._ended) {
        const error = new Error("write after end");
        error.code = "ERR_STREAM_WRITE_AFTER_END";
        throw error;
      }
      this._chunks.push(
        TerraceBuffer.isBuffer(chunk) || chunk instanceof Uint8Array
          ? TerraceBuffer.from(chunk)
          : TerraceBuffer.from(String(chunk), encoding || this._encoding),
      );
      if (typeof callback === "function") callback();
      return true;
    }
    end(chunk, encoding, callback) {
      if (chunk != null) {
        this.write(chunk, encoding);
      } else if (typeof encoding === "function") {
        callback = encoding;
      }
      this._ended = true;
      if (typeof callback === "function") callback();
      this.emit("finish");
      this.emit("close");
      return this;
    }
    _asString() {
      return TerraceBuffer.concat(this._chunks).toString(this._encoding);
    }
  }

  const makeChildError = (raw, fallbackCmd) => {
    const error = new Error(raw?.message || "child process failed");
    if (raw && typeof raw === "object") {
      for (const [key, value] of Object.entries(raw)) {
        error[key] = value;
      }
    }
    if (!("cmd" in error) && fallbackCmd) {
      error.cmd = fallbackCmd;
    }
    return error;
  };

  const makeAbortError = () => {
    const error = new Error("The operation was aborted");
    error.name = "AbortError";
    error.code = "ABORT_ERR";
    return error;
  };

  const collectEnv = (env) => {
    if (env == null) {
      return { ...process.env };
    }
    if (typeof env !== "object") {
      const error = new TypeError('The "options.env" property must be of type object.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    const result = {};
    for (const key in env) {
      const value = env[key];
      if (value === undefined) continue;
      result[key] = value === null ? "null" : String(value);
    }
    return result;
  };

  const knownSignals = new Set([
    "SIGTERM",
    "SIGKILL",
    "SIGINT",
    "SIGHUP",
    "SIGQUIT",
    "SIGABRT",
    "SIGALRM",
    "SIGUSR1",
    "SIGUSR2",
    "SIGPIPE",
    "SIGCHLD",
    "SIGCONT",
    "SIGSTOP",
  ]);

  const normalizeSpawnArguments = (command, args, options) => {
    if (Array.isArray(args)) {
      return {
        command,
        args: [...args],
        options: options && typeof options === "object" ? { ...options } : {},
      };
    }
    if (args && typeof args === "object") {
      return {
        command,
        args: [],
        options: { ...args },
      };
    }
    if (args == null) {
      return {
        command,
        args: [],
        options: options && typeof options === "object" ? { ...options } : {},
      };
    }
    const error = new TypeError('The "args" argument must be an instance of Array.');
    error.code = "ERR_INVALID_ARG_TYPE";
    throw error;
  };

  const validateSpawnState = (command, args, options) => {
    if (typeof command !== "string") {
      const error = new TypeError('The "file" argument must be of type string.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    if (options != null && typeof options !== "object") {
      const error = new TypeError('The "options" argument must be of type object.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    if (options && options.args != null && !Array.isArray(options.args)) {
      const error = new TypeError('The "options.args" property must be an instance of Array.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    if (options && options.envPairs != null && !Array.isArray(options.envPairs)) {
      const error = new TypeError('The "options.envPairs" property must be an instance of Array.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
    if (!Array.isArray(args)) {
      const error = new TypeError('The "args" argument must be an instance of Array.');
      error.code = "ERR_INVALID_ARG_TYPE";
      throw error;
    }
  };

  const stdioMode = (options, fd) => {
    const stdio = options?.stdio;
    if (stdio == null || stdio === "pipe") return "pipe";
    if (stdio === "ignore") return "ignore";
    if (Array.isArray(stdio)) return stdio[fd] ?? "pipe";
    return "pipe";
  };

  const toRequest = (command, args, options, stdinText = undefined) => ({
    command: String(command),
    args: [...args],
    cwd: options?.cwd == null ? undefined : String(options.cwd),
    env: collectEnv(options?.env),
    shell: !!options?.shell,
    input: stdinText,
  });

  const spawnSyncResultObject = (result, options = {}) => {
    const encoding =
      typeof options.encoding === "string" && options.encoding !== "buffer"
        ? options.encoding
        : null;
    const stdoutBuffer = TerraceBuffer.from(result.stdout || "", "utf8");
    const stderrBuffer = TerraceBuffer.from(result.stderr || "", "utf8");
    const stdout = encoding ? stdoutBuffer.toString(encoding) : stdoutBuffer;
    const stderr = encoding ? stderrBuffer.toString(encoding) : stderrBuffer;
    return {
      pid: result.pid,
      output: [null, stdout, stderr],
      stdout,
      stderr,
      status: result.status ?? null,
      signal: result.signal ?? null,
      error: result.error ? makeChildError(result.error, result.error.cmd || result.file) : undefined,
      file: result.file,
    };
  };

  const execErrorFromResult = (label, result, stdout, stderr) => {
    if (result.error) {
      const error = makeChildError(result.error, label);
      error.stdout = stdout;
      error.stderr = stderr;
      return error;
    }
    if ((result.status ?? 0) === 0 && !result.signal) {
      return null;
    }
    const error = new Error(`Command failed: ${label}`);
    error.code = result.status ?? result.signal ?? 1;
    error.killed = false;
    error.signal = result.signal ?? null;
    error.cmd = label;
    error.stdout = stdout;
    error.stderr = stderr;
    return error;
  };

  class ChildProcess extends eventsApi {
    constructor() {
      super();
      this.pid = undefined;
      this.exitCode = null;
      this.signalCode = null;
      this.killed = false;
      this.spawnfile = undefined;
      this.spawnargs = [];
      this.stdin = new ChildWritable();
      this.stdout = new ChildReadable();
      this.stderr = new ChildReadable();
      this.connected = false;
      this._completed = false;
      this._refed = true;
    }
    spawn(options) {
      if (options == null || typeof options !== "object") {
        const error = new TypeError('The "options" argument must be of type object.');
        error.code = "ERR_INVALID_ARG_TYPE";
        throw error;
      }
      const file = options.file;
      const args = options.args ?? [];
      validateSpawnState(file, args, options);
      const env = options.envPairs
        ? Object.fromEntries(options.envPairs.map((entry) => String(entry).split(/=(.*)/s)).map(([k, v]) => [k, v ?? ""]))
        : collectEnv(options.env);
      const result = __terraceChildProcessRun({
        command: String(file),
        args: [...args],
        cwd: options.cwd == null ? undefined : String(options.cwd),
        env,
        shell: !!options.shell,
        input: this.stdin._asString(),
      });
      return finalizeChildProcess(this, result, options);
    }
    kill(signal = "SIGTERM") {
      if (signal != null && typeof signal !== "string") {
        const error = new TypeError("ERR_UNKNOWN_SIGNAL");
        error.code = "ERR_UNKNOWN_SIGNAL";
        throw error;
      }
      if (typeof signal === "string" && !knownSignals.has(signal)) {
        const error = new TypeError("ERR_UNKNOWN_SIGNAL");
        error.code = "ERR_UNKNOWN_SIGNAL";
        throw error;
      }
      this.killed = true;
      this.signalCode = signal || "SIGTERM";
      return true;
    }
    ref() {
      this._refed = true;
      return this;
    }
    unref() {
      this._refed = false;
      return this;
    }
  }

  const finalizeChildProcess = (child, result, options = {}) => {
    child.pid = result.pid;
    child.spawnfile = result.spawnfile || result.file;
    child.spawnargs = Array.isArray(result.spawnargs) ? [...result.spawnargs] : [];
    child.stdout = stdioMode(options, 1) === "ignore" ? null : child.stdout;
    child.stderr = stdioMode(options, 2) === "ignore" ? null : child.stderr;
    child.stdin = stdioMode(options, 0) === "ignore" ? null : child.stdin;
    child.stdout?._setText(result.stdout || "");
    child.stderr?._setText(result.stderr || "");
    queueMicrotask(() => {
      if (result.error) {
        const error = makeChildError(result.error, result.error.cmd || result.file);
        child.emit("error", error);
        child.stdout?._flush();
        child.stderr?._flush();
        child.emit("close", result.status ?? null, result.signal ?? null);
        child._completed = true;
        return;
      }
      child.emit("spawn");
      child.stdout?._flush();
      child.stderr?._flush();
      child.exitCode = result.status ?? null;
      child.signalCode = result.signal ?? null;
      child.emit("exit", child.exitCode, child.signalCode);
      child.emit("close", child.exitCode, child.signalCode);
      child._completed = true;
    });
    return child;
  };

  const spawn = (command, args = [], options = {}) => {
    const normalized = normalizeSpawnArguments(command, args, options);
    validateSpawnState(normalized.command, normalized.args, normalized.options);
    if (normalized.options.signal) {
      if (typeof normalized.options.signal !== "object") {
        const error = new TypeError('The "options.signal" property must be an instance of AbortSignal.');
        error.code = "ERR_INVALID_ARG_TYPE";
        throw error;
      }
      if (normalized.options.signal.aborted) {
        const child = new ChildProcess();
        queueMicrotask(() => {
          child.emit("error", makeAbortError());
          child.emit("close", null, null);
        });
        return child;
      }
    }
    const child = new ChildProcess();
    const result = __terraceChildProcessRun(toRequest(
      normalized.command,
      normalized.args,
      normalized.options,
      child.stdin?._asString(),
    ));
    return finalizeChildProcess(child, result, normalized.options);
  };

  const spawnSync = (command, args = [], options = {}) => {
    const normalized = normalizeSpawnArguments(command, args, options);
    validateSpawnState(normalized.command, normalized.args, normalized.options);
    const result = __terraceChildProcessRun(toRequest(
      normalized.command,
      normalized.args,
      normalized.options,
      typeof normalized.options.input === "string"
        ? normalized.options.input
        : normalized.options.input != null
          ? TerraceBuffer.from(normalized.options.input).toString("utf8")
          : undefined,
    ));
    return spawnSyncResultObject(result, normalized.options);
  };

  const execFile = (file, args = [], options, callback) => {
    if (!Array.isArray(args)) {
      callback = options;
      options = args || {};
      args = [];
    }
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    options = options && typeof options === "object" ? { ...options } : {};
    const child = spawn(file, args, options);
    const stdoutChunks = [];
    const stderrChunks = [];
    child.stdout?.on("data", (chunk) => stdoutChunks.push(chunk));
    child.stderr?.on("data", (chunk) => stderrChunks.push(chunk));
    if (typeof callback === "function") {
      let settled = false;
      const settle = (errorLike) => {
        if (settled) return;
        settled = true;
        const stdout = stdoutChunks
          .map((chunk) => (typeof chunk === "string" ? chunk : TerraceBuffer.from(chunk).toString("utf8")))
          .join("");
        const stderr = stderrChunks
          .map((chunk) => (typeof chunk === "string" ? chunk : TerraceBuffer.from(chunk).toString("utf8")))
          .join("");
        callback(errorLike, stdout, stderr);
      };
      child.once("error", (error) => settle(error));
      child.once("close", (code, signal) => {
        if (settled) return;
        const stdout = stdoutChunks
          .map((chunk) => (typeof chunk === "string" ? chunk : TerraceBuffer.from(chunk).toString("utf8")))
          .join("");
        const stderr = stderrChunks
          .map((chunk) => (typeof chunk === "string" ? chunk : TerraceBuffer.from(chunk).toString("utf8")))
          .join("");
        const label =
          options.shell
            ? `${file}${args.length ? ` ${args.join(" ")}` : ""}`
            : [file, ...args].join(" ");
        const error = (code || signal) ? execErrorFromResult(label, { status: code, signal }, stdout, stderr) : null;
        callback(error, stdout, stderr);
      });
    }
    return child;
  };

  const exec = (command, options, callback) => {
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    const execOptions = options && typeof options === "object" ? { ...options, shell: options.shell ?? true } : { shell: true };
    return execFile(command, [], execOptions, callback);
  };

  const execFileSync = (file, args = [], options = {}) => {
    if (!Array.isArray(args)) {
      options = args || {};
      args = [];
    }
    const result = spawnSync(file, args, options);
    const label = [file, ...args].join(" ");
    const error = result.error || execErrorFromResult(label, result, result.stdout, result.stderr);
    if (error) throw error;
    return result.stdout;
  };

  const execSync = (command, options = {}) => {
    const result = spawnSync(command, [], { ...options, shell: options?.shell ?? true });
    const error = result.error || execErrorFromResult(String(command), result, result.stdout, result.stderr);
    if (error) throw error;
    return result.stdout;
  };

  const fork = (modulePath, args = [], options = {}) => {
    const execPath = options.execPath || process.execPath;
    const execArgv = Array.isArray(options.execArgv) ? [...options.execArgv] : [];
    return spawn(execPath, [...execArgv, pathApi.resolve(modulePath), ...args], options);
  };

  return {
    ChildProcess,
    _forkChild() {},
    exec,
    execFile,
    execFileSync,
    execSync,
    fork,
    spawn,
    spawnSync,
  };
}

function __terraceNodeTypeError(code, message) {
  const error = new TypeError(message);
  error.code = code;
  return error;
}

function __terraceNodeRangeError(code, message) {
  const error = new RangeError(message);
  error.code = code;
  return error;
}

function __terraceInvalidDigestError(name) {
  return __terraceNodeTypeError("ERR_CRYPTO_INVALID_DIGEST", `Invalid digest: ${name}`);
}

function __terraceHashFinalizedError() {
  const error = new Error("Digest already called");
  error.code = "ERR_CRYPTO_HASH_FINALIZED";
  return error;
}

function __terraceCryptoOutput(bytes, encoding) {
  if (encoding == null || encoding === "buffer") return TerraceBuffer.from(bytes);
  return TerraceBuffer.from(bytes).toString(encoding);
}

function __terraceCryptoDataBytes(value, encoding = "utf8") {
  if (value == null) {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "data" argument must be of type string or an instance of Buffer, TypedArray, or DataView. Received ${value}`,
    );
  }
  if (typeof value === "string") return TerraceBuffer.from(value, encoding);
  if (TerraceBuffer.isBuffer(value) || value instanceof Uint8Array) return TerraceBuffer.from(value);
  if (ArrayBuffer.isView(value)) return TerraceBuffer.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  if (value instanceof ArrayBuffer || (typeof SharedArrayBuffer === "function" && value instanceof SharedArrayBuffer)) {
    return TerraceBuffer.from(new Uint8Array(value));
  }
  throw __terraceNodeTypeError(
    "ERR_INVALID_ARG_TYPE",
    `The "data" argument must be of type string or an instance of Buffer, TypedArray, or DataView. Received ${typeof value}`,
  );
}

function __terraceCryptoBufferLikeBytes(value, name) {
  if (typeof value === "string") return TerraceBuffer.from(value, "utf8");
  if (TerraceBuffer.isBuffer(value) || value instanceof Uint8Array) return TerraceBuffer.from(value);
  if (ArrayBuffer.isView(value)) return TerraceBuffer.from(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
  if (value instanceof ArrayBuffer || (typeof SharedArrayBuffer === "function" && value instanceof SharedArrayBuffer)) {
    return TerraceBuffer.from(new Uint8Array(value));
  }
  throw __terraceNodeTypeError(
    "ERR_INVALID_ARG_TYPE",
    `The "${name}" argument must be of type string or an instance of Buffer, TypedArray, DataView, ArrayBuffer, or SharedArrayBuffer.`,
  );
}

function __terraceNormalizeDigestName(value) {
  const normalized = String(value || "").trim().toLowerCase();
  switch (normalized) {
    case "md5":
      return "md5";
    case "ripemd":
    case "ripemd160":
    case "rmd160":
      return "ripemd160";
    case "sha1":
    case "sha-1":
      return "sha1";
    case "sha224":
    case "sha-224":
      return "sha224";
    case "sha256":
    case "sha-256":
      return "sha256";
    case "sha384":
    case "sha-384":
      return "sha384";
    case "sha512":
    case "sha-512":
      return "sha512";
    case "sha512-224":
    case "sha-512/224":
      return "sha512-224";
    case "sha512-256":
    case "sha-512/256":
      return "sha512-256";
    case "sha3-224":
      return "sha3-224";
    case "sha3-256":
      return "sha3-256";
    case "sha3-384":
      return "sha3-384";
    case "sha3-512":
      return "sha3-512";
    case "shake128":
      return "shake128";
    case "shake256":
      return "shake256";
    default:
      return null;
  }
}

function __terraceAssertDigestSupported(algorithm) {
  if (typeof algorithm !== "string") {
    throw __terraceNodeTypeError(
      "ERR_INVALID_ARG_TYPE",
      `The "algorithm" argument must be of type string. Received ${algorithm}`,
    );
  }
  const normalized = __terraceNormalizeDigestName(algorithm);
  const supported = Array.from(__terraceCryptoGetHashes()).map((entry) => __terraceNormalizeDigestName(entry));
  if (!normalized || !supported.includes(normalized)) {
    throw __terraceInvalidDigestError(algorithm);
  }
  return normalized;
}

function __terraceRandomFillView(value) {
  if (TerraceBuffer.isBuffer(value) || value instanceof Uint8Array) return value;
  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof ArrayBuffer || (typeof SharedArrayBuffer === "function" && value instanceof SharedArrayBuffer)) {
    return new Uint8Array(value);
  }
  throw __terraceNodeTypeError(
    "ERR_INVALID_ARG_TYPE",
    'The "buf" argument must be an instance of ArrayBuffer, Buffer, TypedArray, DataView, or SharedArrayBuffer.',
  );
}

function __terraceIntegerTypedArray(value) {
  return value instanceof Int8Array ||
    value instanceof Uint8Array ||
    value instanceof Uint8ClampedArray ||
    value instanceof Int16Array ||
    value instanceof Uint16Array ||
    value instanceof Int32Array ||
    value instanceof Uint32Array ||
    (typeof BigInt64Array === "function" && value instanceof BigInt64Array) ||
    (typeof BigUint64Array === "function" && value instanceof BigUint64Array);
}

class TerraceKeyObject {
  constructor(type, data) {
    this.type = type;
    this._data = TerraceBuffer.from(data);
  }

  equals(other) {
    return other instanceof TerraceKeyObject &&
      other.type === this.type &&
      TerraceBuffer.from(this._data).toString("hex") === TerraceBuffer.from(other._data).toString("hex");
  }
}

class TerraceHash {
  constructor(algorithm, options = {}) {
    __terraceAssertDigestSupported(algorithm);
    this.algorithm = algorithm;
    this.options = options && typeof options === "object" ? { ...options } : {};
    this._chunks = [];
    this._finalized = false;
    this._result = null;
    this._encoding = null;
  }

  update(data, encoding = "utf8") {
    if (this._finalized) throw __terraceHashFinalizedError();
    if (arguments.length === 0) {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "data" argument must be of type string or an instance of Buffer, TypedArray, or DataView. Received undefined',
      );
    }
    this._chunks.push(__terraceCryptoDataBytes(data, encoding));
    return this;
  }

  digest(encoding = undefined) {
    if (this._finalized) throw __terraceHashFinalizedError();
    const data = TerraceBuffer.concat(this._chunks);
    let bytes;
    try {
      bytes = __terraceCryptoDigest({
        algorithm: this.algorithm,
        data: Array.from(data),
        output_length: this.options.outputLength == null ? undefined : Math.trunc(Number(this.options.outputLength)),
      });
    } catch (error) {
      if (error?.message?.includes("Invalid digest")) {
        throw __terraceInvalidDigestError(this.algorithm);
      }
      throw error;
    }
    this._finalized = true;
    this._result = TerraceBuffer.from(bytes);
    return __terraceCryptoOutput(this._result, encoding);
  }

  copy(options = undefined) {
    if (this._finalized) throw __terraceHashFinalizedError();
    const nextOptions = { ...this.options, ...(options || {}) };
    if ((String(this.algorithm).toLowerCase() === "shake128" || String(this.algorithm).toLowerCase() === "shake256") &&
        (!options || !Object.prototype.hasOwnProperty.call(options, "outputLength"))) {
      delete nextOptions.outputLength;
    }
    const next = new TerraceHash(this.algorithm, nextOptions);
    next._chunks = this._chunks.map((chunk) => TerraceBuffer.from(chunk));
    next._encoding = this._encoding;
    return next;
  }

  write(chunk, encoding = "utf8") {
    this.update(chunk, encoding);
    return true;
  }

  end(chunk = undefined, encoding = "utf8") {
    if (chunk != null) this.update(chunk, encoding);
    if (!this._finalized) {
      this._result = TerraceBuffer.from(this.digest());
    }
    return this;
  }

  read() {
    if (!this._finalized) {
      this._result = TerraceBuffer.from(this.digest());
    }
    return this._encoding ? this._result.toString(this._encoding) : this._result;
  }

  setEncoding(encoding) {
    this._encoding = encoding;
    return this;
  }

  pause() { return this; }
  resume() { return this; }
  unpipe() { return this; }
}

class TerraceHmac {
  constructor(algorithm, key) {
    if (typeof algorithm !== "string") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "hmac" argument must be of type string. Received ${algorithm}`,
      );
    }
    __terraceAssertDigestSupported(algorithm);
    if (key == null) {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "key" argument must be of type string or an instance of ArrayBuffer, Buffer, TypedArray, or DataView. Received ${key}`,
      );
    }
    this.algorithm = algorithm;
    this.key = key instanceof TerraceKeyObject ? TerraceBuffer.from(key._data) : __terraceCryptoBufferLikeBytes(key, "key");
    this._chunks = [];
    this._finalized = false;
    this._result = null;
    this._encoding = null;
  }

  update(data, encoding = "utf8") {
    if (this._finalized) throw __terraceHashFinalizedError();
    this._chunks.push(__terraceCryptoDataBytes(data, encoding));
    return this;
  }

  digest(encoding = undefined) {
    if (this._finalized) throw __terraceHashFinalizedError();
    const data = TerraceBuffer.concat(this._chunks);
    let bytes;
    try {
      bytes = __terraceCryptoHmac({
        algorithm: this.algorithm,
        key: Array.from(this.key),
        data: Array.from(data),
      });
    } catch (error) {
      if (error?.message?.includes("Invalid digest")) {
        throw __terraceInvalidDigestError(this.algorithm);
      }
      throw error;
    }
    this._finalized = true;
    this._result = TerraceBuffer.from(bytes);
    return __terraceCryptoOutput(this._result, encoding);
  }

  write(chunk, encoding = "utf8") {
    this.update(chunk, encoding);
    return true;
  }

  end(chunk = undefined, encoding = "utf8") {
    if (chunk != null) this.update(chunk, encoding);
    if (!this._finalized) {
      this._result = TerraceBuffer.from(this.digest());
    }
    return this;
  }

  read() {
    if (!this._finalized) {
      this._result = TerraceBuffer.from(this.digest());
    }
    return this._encoding ? this._result.toString(this._encoding) : this._result;
  }

  setEncoding(encoding) {
    this._encoding = encoding;
    return this;
  }

  pause() { return this; }
  resume() { return this; }
  unpipe() { return this; }
}

function __terraceCryptoModule() {
  if (__terraceCryptoSingleton) return __terraceCryptoSingleton;

  const Hash = function Hash(algorithm, options) {
    return new TerraceHash(algorithm, options);
  };
  Hash.prototype = TerraceHash.prototype;

  const Hmac = function Hmac(algorithm, key, options) {
    return new TerraceHmac(algorithm, key, options);
  };
  Hmac.prototype = TerraceHmac.prototype;

  const createHash = (algorithm, options) => new TerraceHash(algorithm, options);
  const createHmac = (algorithm, key, options) => new TerraceHmac(algorithm, key, options);

  const createSecretKey = (key) => new TerraceKeyObject("secret", __terraceCryptoBufferLikeBytes(key, "key"));

  const hash = (algorithm, data, outputEncoding = undefined) =>
    createHash(__terraceAssertDigestSupported(algorithm)).update(data).digest(
      typeof outputEncoding === "string" ? outputEncoding : outputEncoding?.outputEncoding,
    );

  const randomBytes = (size, callback) => {
    if (typeof size !== "number") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        `The "size" argument must be of type number. Received ${size}`,
      );
    }
    const buffer = TerraceBuffer.from(__terraceCryptoRandomBytes(Math.floor(size)));
    if (typeof callback === "function") {
      queueMicrotask(() => callback(null, buffer));
      return;
    }
    return buffer;
  };

  const pseudoRandomBytes = (size, callback) => randomBytes(size, callback);

  const randomFillSync = (buf, offset = 0, size = undefined) => {
    const view = __terraceRandomFillView(buf);
    const start = Number(offset) || 0;
    const span = size == null ? Math.max(0, view.byteLength - start) : Number(size);
    const bytes = TerraceBuffer.from(__terraceCryptoRandomBytes(span));
    view.set(bytes, start);
    return buf;
  };

  const randomFill = (buf, offset, size, callback) => {
    if (typeof offset === "function") {
      callback = offset;
      offset = 0;
      size = undefined;
    } else if (typeof size === "function") {
      callback = size;
      size = undefined;
    }
    const result = randomFillSync(buf, offset ?? 0, size);
    if (typeof callback === "function") {
      queueMicrotask(() => callback(null, result));
      return;
    }
    return result;
  };

  const randomUUID = () => {
    const bytes = TerraceBuffer.from(__terraceCryptoRandomBytes(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    const hex = bytes.toString("hex");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  };

  const randomInt = (minOrMax, maxOrCallback, maybeCallback) => {
    let min = 0;
    let max = minOrMax;
    let callback = maybeCallback;
    if (typeof maxOrCallback === "number") {
      min = minOrMax;
      max = maxOrCallback;
    } else {
      callback = maxOrCallback;
    }
    const value = __terraceCryptoRandomInt(Number(min), Number(max));
    if (typeof callback === "function") {
      queueMicrotask(() => callback(null, value));
      return;
    }
    return value;
  };

  const timingSafeEqual = (left, right) => __terraceCryptoTimingSafeEqual(left, right);

  const pbkdf2Sync = (password, salt, iterations, keylen, digest) =>
    TerraceBuffer.from(__terraceCryptoPbkdf2({
      password: Array.from(__terraceCryptoBufferLikeBytes(password, "password")),
      salt: Array.from(__terraceCryptoBufferLikeBytes(salt, "salt")),
      iterations: Math.trunc(Number(iterations)),
      keylen: Math.trunc(Number(keylen)),
      digest: __terraceAssertDigestSupported(digest),
    }));

  const pbkdf2 = (password, salt, iterations, keylen, digest, callback) => {
    if (typeof callback !== "function") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "callback" argument must be of type function.',
      );
    }
    const result = pbkdf2Sync(password, salt, iterations, keylen, digest);
    queueMicrotask(() => callback(null, result));
  };

  const hkdfSync = (digest, ikm, salt, info, keylen) =>
    TerraceBuffer.from(__terraceCryptoHkdf({
      digest: __terraceAssertDigestSupported(digest),
      ikm: Array.from(__terraceCryptoBufferLikeBytes(ikm, "ikm")),
      salt: Array.from(__terraceCryptoBufferLikeBytes(salt, "salt")),
      info: Array.from(__terraceCryptoBufferLikeBytes(info, "info")),
      keylen: Math.trunc(Number(keylen)),
    }));

  const hkdf = (digest, ikm, salt, info, keylen, callback) => {
    if (typeof callback !== "function") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "callback" argument must be of type function.',
      );
    }
    const result = hkdfSync(digest, ikm, salt, info, keylen);
    queueMicrotask(() => callback(null, result));
  };

  const scryptSync = (password, salt, keylen, options = {}) => {
    const cost = options.N || options.cost;
    const logN =
      typeof cost === "number" && cost > 0 && Number.isInteger(Math.log2(cost))
        ? Math.log2(cost)
        : undefined;
    return TerraceBuffer.from(__terraceCryptoScrypt({
      password: Array.from(__terraceCryptoBufferLikeBytes(password, "password")),
      salt: Array.from(__terraceCryptoBufferLikeBytes(salt, "salt")),
      keylen: Math.trunc(Number(keylen)),
      cost: logN,
      block_size: options.r == null ? undefined : Math.trunc(Number(options.r)),
      parallelization: options.p == null ? undefined : Math.trunc(Number(options.p)),
    }));
  };

  const scrypt = (password, salt, keylen, options, callback) => {
    if (typeof options === "function") {
      callback = options;
      options = {};
    }
    if (typeof callback !== "function") {
      throw __terraceNodeTypeError(
        "ERR_INVALID_ARG_TYPE",
        'The "callback" argument must be of type function.',
      );
    }
    const result = scryptSync(password, salt, keylen, options || {});
    queueMicrotask(() => callback(null, result));
  };

  const subtle = {
    digest(algorithm, data) {
      const name = typeof algorithm === "string" ? algorithm : algorithm?.name;
      __terraceAssertDigestSupported(name);
      const outputLength = typeof algorithm === "object" ? algorithm?.outputLength : undefined;
      const bytes = __terraceCryptoDigest({
        algorithm: name,
        data: Array.from(__terraceCryptoBufferLikeBytes(data, "data")),
        output_length: outputLength == null ? undefined : Math.trunc(Number(outputLength)),
      });
      const buffer = TerraceBuffer.from(bytes).buffer.slice(0);
      return Promise.resolve(buffer);
    },
    importKey: __terraceUnsupportedGlobal("crypto.subtle.importKey"),
    exportKey: __terraceUnsupportedGlobal("crypto.subtle.exportKey"),
    generateKey: __terraceUnsupportedGlobal("crypto.subtle.generateKey"),
    sign: __terraceUnsupportedGlobal("crypto.subtle.sign"),
    verify: __terraceUnsupportedGlobal("crypto.subtle.verify"),
    encrypt: __terraceUnsupportedGlobal("crypto.subtle.encrypt"),
    decrypt: __terraceUnsupportedGlobal("crypto.subtle.decrypt"),
    deriveBits: __terraceUnsupportedGlobal("crypto.subtle.deriveBits"),
    deriveKey: __terraceUnsupportedGlobal("crypto.subtle.deriveKey"),
    wrapKey: __terraceUnsupportedGlobal("crypto.subtle.wrapKey"),
    unwrapKey: __terraceUnsupportedGlobal("crypto.subtle.unwrapKey"),
    encapsulateKey: __terraceUnsupportedGlobal("crypto.subtle.encapsulateKey"),
    encapsulateBits: __terraceUnsupportedGlobal("crypto.subtle.encapsulateBits"),
    decapsulateKey: __terraceUnsupportedGlobal("crypto.subtle.decapsulateKey"),
    decapsulateBits: __terraceUnsupportedGlobal("crypto.subtle.decapsulateBits"),
  };

  const webcrypto = {
    get subtle() {
      return subtle;
    },
    getRandomValues(typedArray) {
      if (!__terraceIntegerTypedArray(typedArray)) {
        throw __terraceNodeTypeError(
          "ERR_INVALID_ARG_TYPE",
          'The "typedArray" argument must be an integer TypedArray.',
        );
      }
      randomFillSync(typedArray);
      return typedArray;
    },
    randomUUID,
  };

  __terraceWebCryptoSingleton = webcrypto;

  const unsupported = (name) => () => __terraceThrowUnsupportedNodeBuiltin({
    builtin: "crypto",
    member: name,
    operation: "call",
    referrer: null,
  });

  const api = {
    Certificate: unsupported("Certificate"),
    Cipheriv: unsupported("Cipheriv"),
    Decipheriv: unsupported("Decipheriv"),
    DiffieHellman: unsupported("DiffieHellman"),
    DiffieHellmanGroup: unsupported("DiffieHellmanGroup"),
    ECDH: unsupported("ECDH"),
    Hash,
    Hmac,
    KeyObject: TerraceKeyObject,
    Sign: unsupported("Sign"),
    Verify: unsupported("Verify"),
    X509Certificate: unsupported("X509Certificate"),
    argon2: unsupported("argon2"),
    argon2Sync: unsupported("argon2Sync"),
    checkPrime: unsupported("checkPrime"),
    checkPrimeSync: unsupported("checkPrimeSync"),
    constants: {},
    createCipheriv: unsupported("createCipheriv"),
    createDecipheriv: unsupported("createDecipheriv"),
    createDiffieHellman: unsupported("createDiffieHellman"),
    createDiffieHellmanGroup: unsupported("createDiffieHellmanGroup"),
    createECDH: unsupported("createECDH"),
    createHash,
    createHmac,
    createPrivateKey: unsupported("createPrivateKey"),
    createPublicKey: unsupported("createPublicKey"),
    createSecretKey,
    createSign: unsupported("createSign"),
    createVerify: unsupported("createVerify"),
    decapsulate: unsupported("decapsulate"),
    diffieHellman: unsupported("diffieHellman"),
    encapsulate: unsupported("encapsulate"),
    generateKey: unsupported("generateKey"),
    generateKeyPair: unsupported("generateKeyPair"),
    generateKeyPairSync: unsupported("generateKeyPairSync"),
    generateKeySync: unsupported("generateKeySync"),
    generatePrime: unsupported("generatePrime"),
    generatePrimeSync: unsupported("generatePrimeSync"),
    getCipherInfo: () => null,
    getCiphers: () => [],
    getCurves: () => [],
    getDiffieHellman: unsupported("getDiffieHellman"),
    getFips: () => 0,
    getHashes: () => Array.from(__terraceCryptoGetHashes()),
    getRandomValues: (...args) => webcrypto.getRandomValues(...args),
    hash,
    hkdf,
    hkdfSync,
    pbkdf2,
    pbkdf2Sync,
    privateDecrypt: unsupported("privateDecrypt"),
    privateEncrypt: unsupported("privateEncrypt"),
    publicDecrypt: unsupported("publicDecrypt"),
    publicEncrypt: unsupported("publicEncrypt"),
    randomBytes,
    randomFill,
    randomFillSync,
    randomInt,
    randomUUID,
    scrypt,
    scryptSync,
    secureHeapUsed: () => ({ total: 0, used: 0, utilization: 0, min: 0 }),
    setEngine: unsupported("setEngine"),
    setFips: () => {},
    sign: unsupported("sign"),
    subtle,
    timingSafeEqual,
    verify: unsupported("verify"),
    webcrypto,
  };

  Object.defineProperty(api, "pseudoRandomBytes", {
    value: pseudoRandomBytes,
    enumerable: false,
    configurable: true,
    writable: true,
  });

  __terraceCryptoSingleton = api;
  return api;
}

class TerraceMessagePort extends TerraceEventEmitter {
  constructor() {
    super();
    this._queue = [];
    this._counterpart = null;
    this._closed = false;
    this._refed = true;
    this._onmessage = null;
    this._onmessageerror = null;
  }

  postMessage(message) {
    if (this._closed || !this._counterpart || this._counterpart._closed) return;
    this._counterpart._queue.push(message);
    process.nextTick(() => __terraceFlushMessagePortQueue(this._counterpart));
  }

  close() {
    this._closed = true;
    this.emit("close");
    return this;
  }

  start() { return this; }
  ref() { this._refed = true; return this; }
  unref() { this._refed = false; return this; }
  hasRef() { return this._refed; }

  get onmessage() { return this._onmessage; }
  set onmessage(listener) { this._onmessage = typeof listener === "function" ? listener : null; }
  get onmessageerror() { return this._onmessageerror; }
  set onmessageerror(listener) { this._onmessageerror = typeof listener === "function" ? listener : null; }
}

function __terraceFlushMessagePortQueue(port) {
  while (!port._closed && port._queue.length) {
    const message = port._queue.shift();
    const event = { data: message, target: port };
    port.emit("message", message);
    if (typeof port._onmessage === "function") {
      port._onmessage(event);
    }
  }
}

class TerraceMessageChannel {
  constructor() {
    this.port1 = new TerraceMessagePort();
    this.port2 = new TerraceMessagePort();
    this.port1._counterpart = this.port2;
    this.port2._counterpart = this.port1;
  }
}

class TerraceBroadcastChannel extends TerraceEventEmitter {
  constructor(name) {
    super();
    this._name = String(name);
    this._closed = false;
    this._refed = true;
    this._onmessage = null;
    this._onmessageerror = null;
    const peers = __terraceBroadcastChannels.get(this._name) || [];
    peers.push(this);
    __terraceBroadcastChannels.set(this._name, peers);
  }

  postMessage(message) {
    const peers = __terraceBroadcastChannels.get(this._name) || [];
    for (const peer of peers) {
      if (peer === this || peer._closed) continue;
      process.nextTick(() => {
        const event = { data: message, target: peer };
        peer.emit("message", message);
        if (typeof peer._onmessage === "function") {
          peer._onmessage(event);
        }
      });
    }
  }

  close() {
    this._closed = true;
    const peers = (__terraceBroadcastChannels.get(this._name) || []).filter((peer) => peer !== this);
    if (peers.length) {
      __terraceBroadcastChannels.set(this._name, peers);
    } else {
      __terraceBroadcastChannels.delete(this._name);
    }
  }

  ref() { this._refed = true; return this; }
  unref() { this._refed = false; return this; }

  get name() { return this._name; }
  get onmessage() { return this._onmessage; }
  set onmessage(listener) { this._onmessage = typeof listener === "function" ? listener : null; }
  get onmessageerror() { return this._onmessageerror; }
  set onmessageerror(listener) { this._onmessageerror = typeof listener === "function" ? listener : null; }
}

class TerraceWorker extends TerraceEventEmitter {
  constructor(_filename, options = {}) {
    super();
    this._threadId = __terraceNextWorkerId++;
    this._threadName = options.name || "WorkerThread";
    this._resourceLimits = { ...(options.resourceLimits || {}) };
    this._stdout = null;
    this._stderr = null;
    this._stdin = null;
    process.nextTick(() => this.emit("online"));
  }

  postMessage(message) {
    process.nextTick(() => this.emit("message", message));
  }

  terminate() {
    process.nextTick(() => {
      this.emit("exit", 0);
      this.emit("close");
    });
    return Promise.resolve(0);
  }

  ref() { return this; }
  unref() { return this; }
  cpuUsage() { return { user: 0, system: 0 }; }
  getHeapSnapshot() { return __terraceV8Module().getHeapSnapshot(); }
  getHeapStatistics() { return __terraceV8Module().getHeapStatistics(); }
  startCpuProfile(name = "") { return __terraceV8Module().startCpuProfile(name); }
  startHeapProfile() {
    return {
      stop() {
        return { head: null, samples: [] };
      },
    };
  }

  get threadId() { return this._threadId; }
  get threadName() { return this._threadName; }
  get resourceLimits() { return this._resourceLimits; }
  get stdout() { return this._stdout; }
  get stderr() { return this._stderr; }
  get stdin() { return this._stdin; }
}

class TerraceLocks {
  query() { return []; }
  request(_name, _options, callback) {
    if (typeof _options === "function") callback = _options;
    if (typeof callback === "function") {
      return Promise.resolve(callback());
    }
    return Promise.resolve(undefined);
  }
}

function __terraceWorkerThreadsModule() {
  if (__terraceWorkerThreadsSingleton) return __terraceWorkerThreadsSingleton;

  const api = {
    BroadcastChannel: TerraceBroadcastChannel,
    MessageChannel: TerraceMessageChannel,
    MessagePort: TerraceMessagePort,
    SHARE_ENV: Symbol("nodejs.worker_threads.SHARE_ENV"),
    Worker: TerraceWorker,
    getEnvironmentData(key) {
      return __terraceWorkerEnvironmentData.get(key);
    },
    isInternalThread: false,
    isMainThread: true,
    isMarkedAsUntransferable(value) {
      return !!(value && typeof value === "object" && __terraceMarkedUntransferable.has(value));
    },
    locks: new TerraceLocks(),
    markAsUncloneable(value) {
      if (value && typeof value === "object") __terraceMarkedUncloneable.add(value);
      return value;
    },
    markAsUntransferable(value) {
      if (value && typeof value === "object") __terraceMarkedUntransferable.add(value);
      return value;
    },
    moveMessagePortToContext(port, _contextifiedSandbox) {
      return port;
    },
    parentPort: null,
    postMessageToThread(_threadId, _value, _transferList, _timeout) {
      return Promise.resolve();
    },
    receiveMessageOnPort(port) {
      if (!port || !Array.isArray(port._queue) || port._queue.length === 0) return undefined;
      return { message: port._queue.shift() };
    },
    resourceLimits: {},
    setEnvironmentData(key, value) {
      __terraceWorkerEnvironmentData.set(key, value);
    },
    threadId: 0,
    threadName: "",
    workerData: null,
  };

  __terraceWorkerThreadsSingleton = api;
  return api;
}

function __terraceV8HeapStatistics() {
  return {
    total_heap_size: 8 * 1024 * 1024,
    total_heap_size_executable: 256 * 1024,
    total_physical_size: 8 * 1024 * 1024,
    total_available_size: 4 * 1024 * 1024 * 1024,
    used_heap_size: 2 * 1024 * 1024,
    heap_size_limit: 4 * 1024 * 1024 * 1024,
    malloced_memory: 128 * 1024,
    peak_malloced_memory: 128 * 1024,
    does_zap_garbage: 0,
    number_of_native_contexts: 1,
    number_of_detached_contexts: 0,
    total_global_handles_size: 16 * 1024,
    used_global_handles_size: 4 * 1024,
    external_memory: 512 * 1024,
  };
}

function __terraceV8HeapSpaceStatistics() {
  const mib = 1024 * 1024;
  return [
    "read_only_space",
    "new_space",
    "old_space",
    "code_space",
    "shared_space",
    "trusted_space",
    "new_large_object_space",
    "large_object_space",
    "code_large_object_space",
    "shared_large_object_space",
    "trusted_large_object_space",
    "shared_trusted_large_object_space",
    "shared_trusted_space",
  ].map((space_name, index) => ({
    space_name,
    space_size: mib,
    space_used_size: (index + 1) * 4096,
    space_available_size: Math.max(0, mib - ((index + 1) * 4096)),
    physical_space_size: mib,
  }));
}

function __terraceV8CodeStatistics() {
  return {
    code_and_metadata_size: 128 * 1024,
    bytecode_and_metadata_size: 64 * 1024,
    external_script_source_size: 0,
    cpu_profiler_metadata_size: 0,
  };
}

function __terraceV8CppHeapStatistics(detailLevel = "brief") {
  return {
    committed_size_bytes: 128 * 1024,
    resident_size_bytes: 128 * 1024,
    used_size_bytes: 96 * 1024,
    space_statistics: detailLevel === "detailed" ? __terraceV8HeapSpaceStatistics() : [],
    type_names: [],
    detail_level: detailLevel === "detailed" ? "detailed" : "brief",
  };
}

function __terraceV8SerializePayload(value) {
  return TerraceBuffer.from(__terraceUtf8Encode(JSON.stringify(value)));
}

function __terraceV8DeserializePayload(data) {
  const bytes = TerraceBuffer.from(data);
  const text = __terraceUtf8Decode(bytes);
  return text.length ? JSON.parse(text) : undefined;
}

class TerraceV8Serializer {
  constructor() {
    this._headerWritten = false;
    this._value = undefined;
  }

  writeHeader() {
    this._headerWritten = true;
  }

  writeValue(value) {
    this._value = value;
  }

  releaseBuffer() {
    return __terraceV8SerializePayload({
      headerWritten: this._headerWritten,
      value: this._value,
    });
  }

  transferArrayBuffer(_id, _arrayBuffer) {}
  writeDouble(value) { this._value = Number(value); }
  writeUint32(value) { this._value = Number(value) >>> 0; }
  writeUint64(value) { this._value = Number(value); }
  writeRawBytes(value) { this._value = TerraceBuffer.from(value); }
}

class TerraceV8DefaultSerializer extends TerraceV8Serializer {}

class TerraceV8Deserializer {
  constructor(data) {
    this._payload = __terraceV8DeserializePayload(data);
  }

  readHeader() {
    return true;
  }

  readValue() {
    return this._payload?.value;
  }

  transferArrayBuffer(_id, _arrayBuffer) {}
  readUint32() { return Number(this.readValue()) >>> 0; }
  readUint64() { return Number(this.readValue()); }
  readDouble() { return Number(this.readValue()); }
  readRawBytes(length) {
    const bytes = TerraceBuffer.from(this.readValue() ?? []);
    return bytes.subarray(0, length);
  }
}

class TerraceV8DefaultDeserializer extends TerraceV8Deserializer {}

class HeapSnapshotStream extends __terraceStreamModule().Readable {
  constructor(payload) {
    super();
    this._payload = TerraceBuffer.from(payload);
    this._offset = 0;
    this.destroyed = false;
  }

  read(size = undefined) {
    if (this.destroyed) return null;
    if (this._offset >= this._payload.length) return null;
    const end =
      typeof size === "number" && size > 0
        ? Math.min(this._payload.length, this._offset + size)
        : this._payload.length;
    const chunk = this._payload.subarray(this._offset, end);
    this._offset = end;
    if (this._offset >= this._payload.length) {
      process.nextTick(() => this.emit("end"));
    }
    return chunk;
  }

  pause() { return this; }
  resume() { return this; }

  destroy(error = null) {
    this.destroyed = true;
    if (error) this.emit("error", error);
    this.emit("close");
    return this;
  }

  pipe(destination) {
    let chunk;
    while ((chunk = this.read()) != null) {
      destination.write?.(chunk);
    }
    destination.end?.();
    return destination;
  }
}

class TerraceV8CpuProfileHandle {
  constructor(name = "") {
    this.name = name;
    this.started = true;
  }

  stop() {
    this.started = false;
    return {
      nodes: [],
      startTime: 0,
      endTime: 0,
      samples: [],
      timeDeltas: [],
      title: this.name,
    };
  }
}

class TerraceV8PromiseHook {
  enable() { return this; }
  disable() { return this; }
}

class TerraceGCProfiler {
  start() { return this; }
  stop() { return []; }
}

function __terraceV8Module() {
  if (__terraceV8Singleton) return __terraceV8Singleton;

  const promiseHooks = {
    createHook() { return new TerraceV8PromiseHook(); },
    onInit() {},
    onBefore() {},
    onAfter() {},
    onSettled() {},
  };

  const startupSnapshot = {
    addDeserializeCallback() {},
    addSerializeCallback() {},
    setDeserializeMainFunction() {},
    isBuildingSnapshot() { return false; },
  };

  const api = {
    DefaultDeserializer: TerraceV8DefaultDeserializer,
    DefaultSerializer: TerraceV8DefaultSerializer,
    Deserializer: TerraceV8Deserializer,
    GCProfiler: TerraceGCProfiler,
    Serializer: TerraceV8Serializer,
    cachedDataVersionTag() { return 1; },
    deserialize(data) {
      const payload = __terraceV8DeserializePayload(data);
      return payload?.value ?? payload;
    },
    getCppHeapStatistics(detailLevel = "brief") {
      return __terraceV8CppHeapStatistics(detailLevel);
    },
    getHeapCodeStatistics() {
      return __terraceV8CodeStatistics();
    },
    getHeapSnapshot(_options = undefined) {
      const snapshot = JSON.stringify({
        meta: {
          node: process.version,
          heapStatistics: __terraceV8HeapStatistics(),
        },
        nodes: [],
        edges: [],
      });
      return new HeapSnapshotStream(__terraceUtf8Encode(snapshot));
    },
    getHeapSpaceStatistics() {
      return __terraceV8HeapSpaceStatistics();
    },
    getHeapStatistics() {
      return __terraceV8HeapStatistics();
    },
    isStringOneByteRepresentation(value) {
      if (typeof value !== "string") return false;
      for (const char of value) {
        if (char.codePointAt(0) > 0xFF) return false;
      }
      return true;
    },
    promiseHooks,
    queryObjects() { return []; },
    serialize(value) {
      return __terraceV8SerializePayload({ value });
    },
    setFlagsFromString(_flags) {},
    setHeapSnapshotNearHeapLimit(_limit) {},
    startCpuProfile(name = "") {
      return new TerraceV8CpuProfileHandle(name);
    },
    startupSnapshot,
    stopCoverage() {},
    takeCoverage() { return []; },
    writeHeapSnapshot(filename = `/workspace/Heap.${Date.now()}.heapsnapshot`) {
      const stream = api.getHeapSnapshot();
      let body = "";
      let chunk;
      while ((chunk = stream.read()) != null) {
        body += TerraceBuffer.from(chunk).toString("utf8");
      }
      __terraceFsModule().writeFileSync(filename, TerraceBuffer.from(body));
      return filename;
    },
  };

  __terraceV8Singleton = api;
  return api;
}

function __terraceVmNormalizeOptions(options = undefined) {
  if (typeof options === "string") {
    return { filename: options };
  }
  if (options == null) {
    return {};
  }
  if (typeof options !== "object") {
    throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "options" argument must be of type object.');
  }
  return options;
}

function __terraceVmEvaluateInThisContext(code, options = undefined) {
  const normalized = __terraceVmNormalizeOptions(options);
  const filename = typeof normalized.filename === "string"
    ? normalized.filename
    : "evalmachine.<anonymous>";
  const source = `${String(code)}\n//# sourceURL=${filename}`;
  try {
    return (0, eval)(source);
  } catch (error) {
    if (error && (error.stack == null || error.stack === "" || error.stack === "undefined")) {
      const name = error?.name ?? "Error";
      const message = error?.message ?? String(error);
      error.stack = `${filename}:1\n${name}: ${message}`;
    }
    throw error;
  }
}

function __terraceVmModule() {
  if (__terraceVmSingleton) return __terraceVmSingleton;

  class TerraceVmScript {
    constructor(code, options = undefined) {
      this.code = String(code);
      this.options = __terraceVmNormalizeOptions(options);
    }

    runInThisContext(options = undefined) {
      return __terraceVmEvaluateInThisContext(this.code, options ?? this.options);
    }

    runInContext(_contextifiedObject, options = undefined) {
      throw __terraceCreateBuiltinStubError("vm", "Script.runInContext", "call", null);
    }

    runInNewContext(_contextObject, options = undefined) {
      throw __terraceCreateBuiltinStubError("vm", "Script.runInNewContext", "call", null);
    }
  }

  const api = {
    Script: TerraceVmScript,
    runInThisContext(code, options = undefined) {
      return __terraceVmEvaluateInThisContext(code, options);
    },
    createContext(_contextObject = {}, _options = undefined) {
      throw __terraceCreateBuiltinStubError("vm", "createContext", "call", null);
    },
    isContext(_value) {
      return false;
    },
    runInContext(_code, _contextifiedObject, _options = undefined) {
      throw __terraceCreateBuiltinStubError("vm", "runInContext", "call", null);
    },
    runInNewContext(_code, _contextObject = {}, _options = undefined) {
      throw __terraceCreateBuiltinStubError("vm", "runInNewContext", "call", null);
    },
    compileFunction(code, params = [], _options = undefined) {
      if (code === undefined) {
        throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "code" argument must be of type string. Received undefined');
      }
      if (params != null && !Array.isArray(params)) {
        throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "params" argument must be an instance of Array. Received null');
      }
      return Function(...(params ?? []), String(code));
    },
  };

  __terraceVmSingleton = api;
  return api;
}

function __terraceModuleNodeModulePaths(from) {
  let normalized = __terraceNormalize(from || ".");
  if (!normalized.startsWith("/")) {
    normalized = __terraceNormalize(`/workspace/${normalized}`);
  }
  if (normalized === "/") {
    return ["/node_modules"];
  }
  const paths = [];
  let cursor = normalized;
  while (cursor !== "/") {
    if (__terraceBasename(cursor) !== "node_modules") {
      paths.push(`${cursor}/node_modules`);
    }
    cursor = __terraceDirname(cursor);
  }
  paths.push("/node_modules");
  return paths;
}

function __terraceModuleParentOf(referrer) {
  if (!referrer) return null;
  return __terraceModuleCache.get(referrer) ?? null;
}

function __terraceCurrentRequireExtensions() {
  const Module = __terraceCreateModuleBuiltin();
  return Object.keys(Module._extensions);
}

function __terraceResolveModuleWithNodeOptions(specifier, referrer, mode = "require") {
  const options = mode === "require"
    ? { extensions: __terraceCurrentRequireExtensions() }
    : null;
  return globalThis.__terraceResolveModule(
    String(specifier),
    referrer == null ? null : String(referrer),
    mode,
    options,
  );
}

function __terraceFindLongestRegisteredExtension(filename) {
  const Module = __terraceCreateModuleBuiltin();
  const name = __terraceBasename(filename);
  let currentExtension;
  let index;
  let startIndex = 0;
  while ((index = name.indexOf(".", startIndex)) !== -1) {
    startIndex = index + 1;
    if (index === 0) continue;
    currentExtension = name.slice(index);
    if (Object.prototype.hasOwnProperty.call(Module._extensions, currentExtension)) {
      return currentExtension;
    }
  }
  return ".js";
}

function __terraceCompileCommonJsModule(module, filename, source) {
  const normalizedFilename = String(filename);
  const instrumentedSource = __terraceAutoinstrumentModuleSource(
    normalizedFilename,
    "commonjs",
    String(source).replace(/\bimport\s*\(/g, "__terraceNodeDynamicImport(__filename, "),
  );
  const dirname = __terraceDirname(normalizedFilename);
  const localRequire = __terraceCreateRequireFromReferrer(normalizedFilename);
  localRequire.main = __terraceModuleCache.get(__terraceRequireStack[0]) ?? module;
  const sourceURL = `\n//# sourceURL=${normalizedFilename.replace(/\\/g, "\\\\")}`;
  const wrapped = (0, eval)(
    `(function(exports, require, module, __filename, __dirname) {\n${instrumentedSource}${sourceURL}\n})`,
  );
  module.__terraceEvaluationResult = wrapped(
    module.exports,
    localRequire,
    module,
    normalizedFilename,
    dirname,
  );
  module.exports = __terraceAutoinstrumentModuleExports(normalizedFilename, module.exports);
  module.loaded = true;
  return module.exports;
}

function __terraceCreateNodeModule(id = "", parent = null) {
  if (!(this instanceof __terraceCreateNodeModule)) {
    return new __terraceCreateNodeModule(id, parent);
  }
  const normalizedId = id == null ? "" : String(id);
  this.id = normalizedId;
  this.path = normalizedId ? __terraceDirname(normalizedId) : ".";
  this.exports = {};
  this.filename = normalizedId || null;
  this.loaded = false;
  this.children = [];
  this.parent = parent ?? null;
  this.paths = this.filename ? __terraceModuleNodeModulePaths(this.path) : [];
  if (this.parent && Array.isArray(this.parent.children) && !this.parent.children.includes(this)) {
    this.parent.children.push(this);
  }
}

__terraceCreateNodeModule.prototype.require = function require(specifier) {
  return __terraceRequire(specifier, this.filename ?? this.id ?? null);
};

__terraceCreateNodeModule.prototype._compile = function _compile(content, filename) {
  const normalizedFilename = filename == null ? this.filename ?? this.id ?? "" : String(filename);
  this.id = normalizedFilename;
  this.filename = normalizedFilename;
  this.path = normalizedFilename ? __terraceDirname(normalizedFilename) : ".";
  this.paths = this.filename ? __terraceModuleNodeModulePaths(this.path) : [];
  return __terraceCompileCommonJsModule(this, normalizedFilename, content);
};

function __terraceCreateRequireFromReferrer(referrer) {
  const normalizedReferrer = String(referrer);
  const localRequire = (specifier) => __terraceRequire(specifier, normalizedReferrer);
  localRequire.resolve = (specifier, options) =>
    __terraceRequireResolve(specifier, normalizedReferrer, options);
  localRequire.resolve.paths = (specifier) =>
    __terraceRequireResolvePaths(specifier, normalizedReferrer);
  localRequire.cache = __terraceRequireCache;
  localRequire.extensions = __terraceCreateModuleBuiltin()._extensions;
  localRequire.main = __terraceModuleCache.get(__terraceRequireStack[0]) ?? null;
  return localRequire;
}

function __terraceCreateModuleBuiltin() {
  if (__terraceModuleSingleton) {
    return __terraceModuleSingleton;
  }

  function Module(id = "", parent = null) {
    return new __terraceCreateNodeModule(id, parent);
  }

  Module.prototype = __terraceCreateNodeModule.prototype;
  Module.prototype.constructor = Module;
  Module.Module = Module;
  Module._cache = __terraceRequireCache;
  Module._extensions = Object.assign(Object.create(null), {
    ".js": (module, filename) => module._compile(__terraceReadModuleSource(filename), filename),
    ".json": (module, filename) => {
      module.exports = JSON.parse(__terraceReadModuleSource(filename));
      module.loaded = true;
      return module.exports;
    },
    ".node": (_module, filename) => {
      const error = new Error(`ERR_DLOPEN_DISABLED: native addon loading is not supported for ${filename}`);
      error.code = "ERR_DLOPEN_DISABLED";
      throw error;
    },
  });
  Module._pathCache = Object.create(null);
  Module.builtinModules = [
    ...__terraceKnownNodeBuiltins,
    ...__terraceKnownNodeBuiltins
      .filter((entry) => !entry.startsWith("node:"))
      .map((entry) => `node:${entry}`),
  ];
  Module.isBuiltin = (specifier) => {
    const raw = String(specifier);
    const normalized = raw.replace(/^node:/, "");
    if (normalized === "test" && !raw.startsWith("node:")) {
      return false;
    }
    return __terraceKnownNodeBuiltins.includes(normalized);
  };
  Module.enableCompileCache = () => ({ status: "disabled" });
  Module.syncBuiltinESMExports = () => {};
  Module._nodeModulePaths = (from) => __terraceModuleNodeModulePaths(from);
  Module._resolveFilename = (request, parent, _isMain = false, options = undefined) => {
    const referrer =
      parent && typeof parent === "object"
        ? parent.filename ?? parent.id ?? null
        : null;
    return __terraceRequireResolve(String(request), referrer, options);
  };
  Module.createRequire = (filenameOrUrl) => {
    const invalidCreateRequireArg = (value) => {
      const inspected =
        typeof value === "string"
          ? `'${value}'`
          : __terraceInspect(value);
      const error = new TypeError(
        `The argument 'filename' must be a file URL object, file URL string, or absolute path string. Received ${inspected}`,
      );
      error.code = "ERR_INVALID_ARG_VALUE";
      return error;
    };
    let referrer = null;
    if (filenameOrUrl && typeof filenameOrUrl === "object" && typeof filenameOrUrl.href === "string") {
      referrer = __terraceUrlModule().fileURLToPath(filenameOrUrl);
    } else if (typeof filenameOrUrl === "string" && filenameOrUrl.startsWith("file:")) {
      referrer = __terraceUrlModule().fileURLToPath(filenameOrUrl);
    } else if (typeof filenameOrUrl === "string" && filenameOrUrl.startsWith("/")) {
      referrer = filenameOrUrl;
    } else {
      throw invalidCreateRequireArg(filenameOrUrl);
    }
    if (typeof referrer !== "string" || !referrer.startsWith("/")) {
      throw invalidCreateRequireArg(filenameOrUrl);
    }
    return __terraceCreateRequireFromReferrer(referrer);
  };
  __terraceModuleSingleton = Module;
  return __terraceModuleSingleton;
}

function __terraceReplModule() {
  if (__terraceReplSingleton) {
    return __terraceReplSingleton;
  }

  class TerraceReplServer extends __terraceEventsModule().EventEmitter {
    close() {
      this.emit("close");
      return this;
    }
  }

  __terraceReplSingleton = {
    REPLServer: TerraceReplServer,
    start(_options = undefined) {
      return new TerraceReplServer();
    },
  };
  return __terraceReplSingleton;
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
      value = __terraceCreateModuleBuiltin();
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
      value = __terraceEventsModule();
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
    case "punycode":
      value = __terracePunycodeModule();
      break;
    case "util":
      value = __terraceUtilModule();
      break;
    case "constants":
      value = __terraceConstantsModule();
      break;
    case "crypto":
      value = __terraceCryptoModule();
      break;
    case "v8":
      value = __terraceV8Module();
      break;
    case "vm":
      value = __terraceVmModule();
      break;
    case "zlib":
      value = __terraceZlibModule();
      break;
    case "querystring":
      value = __terraceQuerystringModule();
      break;
    case "dns":
      value = __terraceDnsModule();
      break;
    case "string_decoder":
      value = __terraceStringDecoderModule();
      break;
    case "stream":
      value = __terraceStreamModule();
      break;
    case "tls":
      value = __terraceTlsModule();
      break;
    case "net":
      value = __terraceNetModule();
      break;
    case "http":
      value = __terraceHttpModule("http:");
      break;
    case "https":
      value = __terraceHttpsModule();
      break;
    case "http2":
      value = __terraceHttp2Module();
      break;
    case "worker_threads":
      value = __terraceWorkerThreadsModule();
      break;
    case "child_process":
      value = __terraceChildProcessModule();
      break;
    case "repl":
      value = __terraceReplModule();
      break;
    default:
      value = __terraceCreateBuiltinStubModule(normalized, referrer);
      __terraceBuiltinCache.set(cacheKey, value);
      return value;
  }
  if (normalized === "fs" || normalized === "fs/promises") {
    __terraceBuiltinCache.set(cacheKey, value);
    return value;
  }
  const wrapped = __terraceWrapBuiltinValue(value, {
    builtin: normalized,
    member: null,
    referrer,
  });
  if (normalized === "crypto") {
    try {
      globalThis.crypto = wrapped.webcrypto;
    } catch (_error) {
      // Keep builtin initialization side-effect free if the global is sealed.
    }
  }
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
  if (Object.prototype.hasOwnProperty.call(__terraceRequireCacheBacking, resolved.id)) {
    __terraceTraceNode(`module-cache-hit ${resolved.id}`);
    return __terraceRequireCacheBacking[resolved.id];
  }
  if (__terraceModuleCache.has(resolved.id)) {
    __terraceTraceNode(`module-cache-hit ${resolved.id}`);
    return __terraceModuleCache.get(resolved.id);
  }
  __terraceNodeLoadCount += 1;
  __terraceAssertNodeBudget("load", __terraceNodeLoadCount, __terraceNodeLoadBudget, null, resolved.id);
  __terraceTraceNode(`module-eval-start ${resolved.id} kind=${resolved.kind}`);
  const parent = __terraceModuleParentOf(__terraceRequireStack[__terraceRequireStack.length - 1] ?? null);
  const module = new __terraceCreateNodeModule(resolved.id, parent);
  __terraceRequireStack.push(resolved.id);
  try {
    if (resolved.kind === "esm") {
      module.exports = globalThis.__terraceRequireEsmNamespace(resolved);
      module.loaded = true;
      module.__terraceEvaluationResult = module.exports;
      __terraceTraceNode(
        `module-eval-ok ${resolved.id} exports=${typeof module.exports} result=${typeof module.__terraceEvaluationResult}`,
      );
      return module;
    }
    __terraceModuleCache.set(resolved.id, module);
    __terraceRequireCacheBacking[resolved.id] = module;
    const Module = __terraceCreateModuleBuiltin();
    const extension = resolved.kind === "json"
      ? ".json"
      : __terraceFindLongestRegisteredExtension(resolved.id);
    const handler =
      Module._extensions[extension] ??
      Module._extensions[".js"];
    if (typeof handler !== "function") {
      throw new Error(`ERR_TERRACE_NODE_INVALID_EXTENSION_HANDLER: no handler for ${extension}`);
    }
    handler(module, resolved.id);
    __terraceTraceNode(
      `module-eval-ok ${resolved.id} exports=${typeof module.exports} result=${typeof module.__terraceEvaluationResult}`,
    );
    return module;
  } catch (error) {
    const stack =
      error && typeof error.stack === "string"
        ? error.stack
        : error && error.message
          ? error.message
          : String(error);
    if (__terraceShouldAutoinstrumentModule(resolved.id)) {
      __terraceDebugTrace("autoinstrument-throw", {
        module: resolved.id,
        kind: resolved.kind,
        name: error && error.name ? error.name : null,
        message: error && error.message ? error.message : String(error),
        stack,
        requireStack: __terraceRequireStack.slice(),
      });
    }
    __terraceCaptureException("module-eval", { module: resolved.id, error });
    __terraceTraceNode(
      `module-eval-throw ${resolved.id} error=${stack}`,
    );
    throw error;
  } finally {
    __terraceRequireStack.pop();
  }
}

function __terraceNamespaceForResolved(resolved) {
  const value =
    resolved.kind === "builtin"
      ? __terraceBuiltin(resolved.id, null)
      : __terraceModuleForResolved(resolved).exports;
  const namespace = Object.create(null);
  namespace.default = value;
  if (value && (typeof value === "object" || typeof value === "function")) {
    for (const key of Object.keys(value)) {
      if (key === "default" || key === "__esModule") continue;
      namespace[key] = value[key];
    }
  }
  return namespace;
}

function __terraceRequire(specifier, referrer) {
  const rawSpecifier = String(specifier);
  const isNodeBuiltinBypass = rawSpecifier.startsWith("node:");
  if (
    !isNodeBuiltinBypass &&
    Object.prototype.hasOwnProperty.call(__terraceRequireCacheBacking, rawSpecifier)
  ) {
    const cached = __terraceRequireCacheBacking[rawSpecifier];
    return cached && typeof cached === "object" && "exports" in cached ? cached.exports : cached;
  }
  __terraceNodeResolutionCount += 1;
  __terraceAssertNodeBudget(
    "resolution",
    __terraceNodeResolutionCount,
    __terraceNodeResolutionBudget,
    referrer ? String(referrer) : null,
    rawSpecifier,
  );
  let resolved;
  try {
    resolved = __terraceResolveModuleWithNodeOptions(
      rawSpecifier,
      referrer ? String(referrer) : null,
    );
  } catch (error) {
    if (error && (error.code === "MODULE_NOT_FOUND" || String(error.message || "").includes("sandbox module not found:"))) {
      const requireStack = __terraceRequireStack.length
        ? `\nRequire stack:\n- ${__terraceRequireStack.slice().reverse().join("\n- ")}`
        : "";
      throw __terraceCreateModuleNotFoundError(
        `Cannot find module '${rawSpecifier}'${requireStack}`,
      );
    }
    throw error;
  }
  if (
    !isNodeBuiltinBypass &&
    Object.prototype.hasOwnProperty.call(__terraceRequireCacheBacking, resolved.id)
  ) {
    const cached = __terraceRequireCacheBacking[resolved.id];
    __terraceTraceNode(`require-cache-hit ${rawSpecifier} -> ${resolved.id}`);
    return cached && typeof cached === "object" && "exports" in cached ? cached.exports : cached;
  }
  if (resolved.kind === "builtin") {
    __terraceTraceNode(`require-builtin ${rawSpecifier} -> ${resolved.id}`);
    return __terraceBuiltin(resolved.id, referrer ? String(referrer) : null);
  }
  const exports = __terraceModuleForResolved(resolved).exports;
  __terraceTraceNode(`require-module ${rawSpecifier} -> ${resolved.id} exports=${typeof exports}`);
  return exports;
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
  const resolved = __terraceResolveModuleWithNodeOptions(
    String(specifier),
    String(referrer),
    "import",
  );
  return import(resolved.specifier);
}

function __terraceCreateModuleNotFoundError(message) {
  const error = new Error(message);
  error.code = "MODULE_NOT_FOUND";
  return error;
}

function __terraceRequireResolve(specifier, referrer, options) {
  const request = String(specifier);
  if (options != null && (typeof options !== "object" || Array.isArray(options))) {
    throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "options" argument must be of type object.');
  }

  let normalizedOptions = { extensions: __terraceCurrentRequireExtensions() };
  if (options && Object.prototype.hasOwnProperty.call(options, "paths")) {
    if (!Array.isArray(options.paths)) {
      const error = new TypeError('The "paths" argument must be an array of strings.');
      error.code = "ERR_INVALID_ARG_VALUE";
      throw error;
    }
    normalizedOptions.paths = [];
    for (const entry of options.paths) {
      if (typeof entry !== "string") {
        throw __terraceNodeTypeError("ERR_INVALID_ARG_TYPE", 'The "paths" argument must be an array of strings.');
      }
      normalizedOptions.paths.push(entry);
    }
  }

  const report = globalThis.__terraceRequireResolveImpl(
    request,
    referrer == null ? null : String(referrer),
    normalizedOptions,
  );
  if (report && report.ok) {
    return report.resolved;
  }
  if (report && report.code === "MODULE_NOT_FOUND") {
    throw __terraceCreateModuleNotFoundError(
      report.message || `Cannot find module '${request}'`,
    );
  }
  const error = new Error(
    report && report.message ? report.message : `Failed to resolve module '${request}'`,
  );
  if (report && report.code) {
    error.code = report.code;
  }
  throw error;
}

function __terraceRequireResolvePaths(specifier, referrer) {
  const report = __terraceRequireResolvePathsImpl(
    String(specifier),
    referrer == null ? null : String(referrer),
  );
  return report ? report.paths : null;
}

function __terraceRunNodeCommand(request) {
  globalThis.global = globalThis;
  globalThis.process = __terraceCreateProcess();
  globalThis.URL = __terraceUrlModule().URL;
  globalThis.URLSearchParams = __terraceUrlModule().URLSearchParams;
  if (__terraceUrlModule().URLPattern !== undefined) {
    globalThis.URLPattern = __terraceUrlModule().URLPattern;
  }
  globalThis.console = {
    log(...args) { __terraceWriteStdout(`${args.map(__terraceInspect).join(" ")}\n`); },
    error(...args) { __terraceWriteStderr(`${args.map(__terraceInspect).join(" ")}\n`); },
    warn(...args) { __terraceWriteStderr(`${args.map(__terraceInspect).join(" ")}\n`); },
    info(...args) { __terraceWriteStdout(`${args.map(__terraceInspect).join(" ")}\n`); },
  };
  globalThis.Buffer = __terraceBuiltin("buffer").Buffer;
  globalThis.setTimeout = __terraceSetTimeout;
  globalThis.clearTimeout = __terraceClearTimeout;
  globalThis.setImmediate = __terraceSetImmediate;
  globalThis.clearImmediate = __terraceClearImmediate;
  __terraceNodeCommandStarted = true;
  __terraceNodeCommandDebug = {
    topLevelResultKind: "undefined",
    topLevelThenable: false,
    topLevelAwaited: false,
    caughtExit: false,
    caughtExitCode: null,
  };
  try {
    __terraceTraceNode(`command-start ${request.entrypoint}`);
    const resolved = __terraceResolveModuleWithNodeOptions(String(request.entrypoint), null);
    if (resolved.kind === "esm") {
      const topLevelResult = import(resolved.specifier).then((namespace) => {
        __terraceTraceNode(`command-finished ${resolved.id}`);
        return namespace;
      });
      __terraceNodeCommandDebug.topLevelResultKind = typeof topLevelResult;
      __terraceNodeCommandDebug.topLevelThenable = true;
      return topLevelResult;
    }
    const entryExports = resolved.kind === "builtin"
      ? __terraceBuiltin(resolved.id, null)
      : __terraceModuleForResolved(resolved).exports;
    const topLevelResult =
      resolved.kind === "builtin" ? entryExports : __terraceModuleCache.get(resolved.id).__terraceEvaluationResult;
    __terraceNodeCommandDebug.topLevelResultKind =
      topLevelResult === null ? "null" : typeof topLevelResult;
    __terraceNodeCommandDebug.topLevelThenable =
      !!(topLevelResult && typeof topLevelResult.then === "function");
    __terraceTraceNode(`command-finished ${resolved.id}`);
  } catch (error) {
    if (!(error && error.__terraceExit)) {
      __terraceCaptureException("command", { entrypoint: request.entrypoint, error });
      __terraceTraceNode(
        `command-throw ${request.entrypoint} error=${error && error.message ? error.message : String(error)}`,
      );
      throw error;
    }
    __terraceNodeCommandDebug.caughtExit = true;
    __terraceNodeCommandDebug.caughtExitCode =
      error && Object.prototype.hasOwnProperty.call(error, "code") ? error.code : null;
    __terraceTraceNode(`command-exit ${request.entrypoint} code=${__terraceNodeCommandDebug.caughtExitCode}`);
  }
}

function __terraceFinishNodeCommand() {
  if (!__terraceNodeCommandStarted) {
    throw new Error("ERR_TERRACE_NODE_RUNTIME: node command was not started");
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
    nodeCommandDebug: { ...__terraceNodeCommandDebug },
  };
}
