// Vendored from Node.js v24.12.0 lib/internal/bootstrap/realm.js.
// Keep this file as close to upstream as possible; Terrace-specific glue lives
// in node_compat_bootstrap.js and runtime.rs.

'use strict';

// This file is compiled as if it's wrapped in a function with arguments
// passed by node::RunBootstrapping()
/* global process, getLinkedBinding, getInternalBinding, primordials */

const {
  ArrayFrom,
  ArrayPrototypeFilter,
  ArrayPrototypeIncludes,
  ArrayPrototypeMap,
  ArrayPrototypePush,
  ArrayPrototypePushApply,
  ArrayPrototypeSlice,
  Error,
  ObjectDefineProperty,
  ObjectKeys,
  ObjectPrototypeHasOwnProperty,
  ObjectSetPrototypeOf,
  ReflectGet,
  SafeMap,
  SafeSet,
  String,
  StringPrototypeSlice,
  StringPrototypeStartsWith,
  TypeError,
} = primordials;

const moduleLoadList = [];
ObjectDefineProperty(process, 'moduleLoadList', {
  __proto__: null,
  value: moduleLoadList,
  configurable: true,
  enumerable: true,
  writable: false,
});

const processBindingAllowList = new SafeSet([
  'buffer',
  'cares_wrap',
  'config',
  'constants',
  'contextify',
  'fs',
  'fs_event_wrap',
  'icu',
  'inspector',
  'js_stream',
  'os',
  'pipe_wrap',
  'process_wrap',
  'spawn_sync',
  'stream_wrap',
  'tcp_wrap',
  'tls_wrap',
  'tty_wrap',
  'udp_wrap',
  'uv',
  'zlib',
]);

const runtimeDeprecatedList = new SafeSet([]);
const legacyWrapperList = new SafeSet([
  'natives',
  'util',
]);

const schemelessBlockList = new SafeSet([
  'sea',
  'sqlite',
  'quic',
  'test',
  'test/reporters',
]);
const experimentalModuleList = new SafeSet(['sqlite', 'quic', 'stream/iter', 'zlib/iter']);

{
  const bindingObj = { __proto__: null };

  process.binding = function binding(module) {
    module = String(module);
    const mod = bindingObj[module];
    if (typeof mod === 'object') {
      return mod;
    }
    if (runtimeDeprecatedList.has(module)) {
      process.emitWarning(
        `Access to process.binding('${module}') is deprecated.`,
        'DeprecationWarning',
        'DEP0111');
      return internalBinding(module);
    }
    if (legacyWrapperList.has(module)) {
      throw new Error(`No such module: ${module}`);
    }
    if (processBindingAllowList.has(module)) {
      return internalBinding(module);
    }
    throw new Error(`No such module: ${module}`);
  };

  process._linkedBinding = function _linkedBinding(module) {
    module = String(module);
    let mod = bindingObj[module];
    if (typeof mod !== 'object')
      mod = bindingObj[module] = getLinkedBinding(module);
    return mod;
  };
}

let internalBinding;
{
  const bindingObj = { __proto__: null };
  internalBinding = function internalBinding(module) {
    let mod = bindingObj[module];
    if (typeof mod !== 'object') {
      mod = bindingObj[module] = getInternalBinding(module);
      ArrayPrototypePush(moduleLoadList, `Internal Binding ${module}`);
    }
    return mod;
  };
}

const selfId = 'internal/bootstrap/realm';
const {
  builtinIds,
  compileFunction,
  setInternalLoaders,
} = internalBinding('builtins');

const { ModuleWrap } = internalBinding('module_wrap');
ObjectSetPrototypeOf(ModuleWrap.prototype, null);

const getOwn = (target, property, receiver) => {
  return ObjectPrototypeHasOwnProperty(target, property) ?
    ReflectGet(target, property, receiver) :
    undefined;
};

const publicBuiltinIds = builtinIds
  .filter((id) =>
    !StringPrototypeStartsWith(id, 'internal/') &&
      !experimentalModuleList.has(id),
  );
const internalBuiltinIds = builtinIds
  .filter((id) => StringPrototypeStartsWith(id, 'internal/') && id !== selfId);

let canBeRequiredByUsersList = new SafeSet(publicBuiltinIds);
let canBeRequiredByUsersWithoutSchemeList =
  new SafeSet(publicBuiltinIds.filter((id) => !schemelessBlockList.has(id)));

class BuiltinModule {
  static map = new SafeMap(
    ArrayPrototypeMap(builtinIds, (id) => [id, new BuiltinModule(id)]),
  );

  constructor(id) {
    this.filename = `${id}.js`;
    this.id = id;
    this.exports = {};
    this.loaded = false;
    this.loading = false;
    this.module = undefined;
    this.exportKeys = undefined;
  }

  static allowRequireByUsers(id) {
    if (id === selfId) {
      throw new Error(`Should not allow ${id}`);
    }
    canBeRequiredByUsersList.add(id);
    if (!schemelessBlockList.has(id)) {
      canBeRequiredByUsersWithoutSchemeList.add(id);
    }
  }

  static setRealmAllowRequireByUsers(ids) {
    canBeRequiredByUsersList =
      new SafeSet(ArrayPrototypeFilter(ids, (id) => ArrayPrototypeIncludes(publicBuiltinIds, id)));
    canBeRequiredByUsersWithoutSchemeList =
      new SafeSet(ArrayPrototypeFilter(ids, (id) => !schemelessBlockList.has(id)));
  }

  static exposeInternals() {
    for (let i = 0; i < internalBuiltinIds.length; ++i) {
      BuiltinModule.allowRequireByUsers(internalBuiltinIds[i]);
    }
  }

  static exists(id) {
    return BuiltinModule.map.has(id);
  }

  static canBeRequiredByUsers(id) {
    return canBeRequiredByUsersList.has(id);
  }

  static canBeRequiredWithoutScheme(id) {
    return canBeRequiredByUsersWithoutSchemeList.has(id);
  }

  static normalizeRequirableId(id) {
    if (StringPrototypeStartsWith(id, 'node:')) {
      const normalizedId = StringPrototypeSlice(id, 5);
      if (BuiltinModule.canBeRequiredByUsers(normalizedId)) {
        return normalizedId;
      }
    } else if (BuiltinModule.canBeRequiredWithoutScheme(id)) {
      return id;
    }

    return undefined;
  }

  static isBuiltin(id) {
    return BuiltinModule.canBeRequiredWithoutScheme(id) || (
      typeof id === 'string' &&
        StringPrototypeStartsWith(id, 'node:') &&
        BuiltinModule.canBeRequiredByUsers(StringPrototypeSlice(id, 5))
    );
  }

  static getSchemeOnlyModuleNames() {
    return ArrayFrom(schemelessBlockList);
  }

  static getAllBuiltinModuleIds() {
    const allBuiltins = ArrayFrom(canBeRequiredByUsersWithoutSchemeList);
    ArrayPrototypePushApply(allBuiltins, ArrayFrom(schemelessBlockList, (x) => `node:${x}`));
    return allBuiltins;
  }

  compileForPublicLoader() {
    if (!BuiltinModule.canBeRequiredByUsers(this.id)) {
      throw new Error(`Should not compile ${this.id} for public use`);
    }
    this.compileForInternalLoader();
    if (!this.exportKeys) {
      const internal = StringPrototypeStartsWith(this.id, 'internal/');
      this.exportKeys = internal ? [] : ObjectKeys(this.exports);
    }
    return this.exports;
  }

  getESMFacade() {
    if (this.module) return this.module;
    const url = `node:${this.id}`;
    const builtin = this;
    const exportsKeys = ArrayPrototypeSlice(this.exportKeys);
    if (!ArrayPrototypeIncludes(exportsKeys, 'default')) {
      ArrayPrototypePush(exportsKeys, 'default');
    }
    this.module = new ModuleWrap(
      url, undefined, exportsKeys,
      function() {
        builtin.syncExports();
        this.setExport('default', builtin.exports);
      });
    this.module.instantiate();
    this.module.evaluate(-1, false);
    return this.module;
  }

  syncExports() {
    const names = this.exportKeys;
    if (this.module) {
      for (let i = 0; i < names.length; i++) {
        const exportName = names[i];
        if (exportName === 'default') continue;
        this.module.setExport(exportName,
                              getOwn(this.exports, exportName, this.exports));
      }
    }
  }

  compileForInternalLoader() {
    if (this.loaded || this.loading) {
      return this.exports;
    }

    const id = this.id;
    this.loading = true;

    try {
      const fn = compileFunction(id);
      fn(this.exports, requireBuiltin, this, process, internalBinding, primordials);
      this.loaded = true;
    } finally {
      this.loading = false;
    }

    ArrayPrototypePush(moduleLoadList, `NativeModule ${id}`);
    return this.exports;
  }
}

const loaderExports = {
  internalBinding,
  BuiltinModule,
  require: requireBuiltin,
};

function requireBuiltin(id) {
  if (id === selfId) {
    return loaderExports;
  }

  const mod = BuiltinModule.map.get(id);
  if (!mod) throw new TypeError(`Missing internal module '${id}'`);
  return mod.compileForInternalLoader();
}

function setupPrepareStackTrace() {
  const {
    setEnhanceStackForFatalException,
    setPrepareStackTraceCallback,
  } = internalBinding('errors');
  const {
    prepareStackTraceCallback,
    ErrorPrepareStackTrace,
    fatalExceptionStackEnhancers: {
      beforeInspector,
      afterInspector,
    },
  } = requireBuiltin('internal/errors');
  setPrepareStackTraceCallback(prepareStackTraceCallback);
  setEnhanceStackForFatalException(beforeInspector, afterInspector);
  ObjectDefineProperty(Error, 'prepareStackTrace', {
    __proto__: null,
    writable: true,
    enumerable: false,
    configurable: true,
    value: ErrorPrepareStackTrace,
  });
}

setInternalLoaders(internalBinding, requireBuiltin);
setupPrepareStackTrace();
