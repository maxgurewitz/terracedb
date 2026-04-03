 * Attempt to resolve a module request using the parent module package metadata.
 * @param {string} parentPath The path of the parent module
 * @param {string} request The module request to resolve
 * @param {unknown} conditions
 * @returns {false|string}
 */
function trySelf(parentPath, request, conditions) {
  if (!parentPath) { return false; }

  const pkg = packageJsonReader.getNearestParentPackageJSON(parentPath);
  if (pkg?.data.exports === undefined || pkg.data.name === undefined) {
    return false;
  }

  let expansion;
  if (request === pkg.data.name) {
    expansion = '.';
  } else if (StringPrototypeStartsWith(request, `${pkg.data.name}/`)) {
    expansion = '.' + StringPrototypeSlice(request, pkg.data.name.length);
  } else {
    return false;
  }

  try {
    const { packageExportsResolve } = require('internal/modules/esm/resolve');
    return finalizeEsmResolution(packageExportsResolve(
      pathToFileURL(pkg.path), expansion, pkg.data,
      pathToFileURL(parentPath), conditions), parentPath, pkg.path);
  } catch (e) {
    if (e.code === 'ERR_MODULE_NOT_FOUND') {
      throw createEsmNotFoundErr(request, pkg.path);
    }
    throw e;
  }
}

/**
 * This only applies to requests of a specific form:
 * 1. `name/.*`
 * 2. `@scope/name/.*`
 */
const EXPORTS_PATTERN = /^((?:@[^/\\%]+\/)?[^./\\%][^/\\%]*)(\/.*)?$/;

/**
 * Resolves the exports for a given module path and request.
 * @param {string} nmPath The path to the module.
 * @param {string} request The request for the module.
 * @param {Set<string>} conditions The conditions to use for resolution.
 * @returns {undefined|string}
 */
function resolveExports(nmPath, request, conditions) {
  // The implementation's behavior is meant to mirror resolution in ESM.
  const { 1: name, 2: expansion = '' } =
    RegExpPrototypeExec(EXPORTS_PATTERN, request) || kEmptyObject;
  if (!name) { return; }
  const pkgPath = path.resolve(nmPath, name);
  const pkg = _readPackage(pkgPath);
  if (pkg.exists && pkg.exports != null) {
    try {
      const { packageExportsResolve } = require('internal/modules/esm/resolve');
      return finalizeEsmResolution(packageExportsResolve(
        pathToFileURL(pkgPath + '/package.json'), '.' + expansion, pkg, null,
        conditions), null, pkgPath);
    } catch (e) {
      if (e.code === 'ERR_MODULE_NOT_FOUND') {
        throw createEsmNotFoundErr(request, pkgPath + '/package.json');
      }
      throw e;
    }
  }
}

/**
 * Get the absolute path to a module.
 * @param {string} request Relative or absolute file path
 * @param {Array<string>} paths Folders to search as file paths
 * @param {boolean} isMain Whether the request is the main app entry point
 * @returns {string | false}
 */
Module._findPath = function(request, paths, isMain, conditions = getCjsConditions()) {
  const absoluteRequest = path.isAbsolute(request);
  if (absoluteRequest) {
    paths = [''];
  } else if (!paths || paths.length === 0) {
    return false;
  }

  const cacheKey = request + '\x00' + ArrayPrototypeJoin(paths, '\x00');
  const entry = Module._pathCache[cacheKey];
  if (entry) {
    return entry;
  }

  let exts;
  const trailingSlash = request.length > 0 &&
    (StringPrototypeCharCodeAt(request, request.length - 1) === CHAR_FORWARD_SLASH || (
      StringPrototypeCharCodeAt(request, request.length - 1) === CHAR_DOT &&
      (
        request.length === 1 ||
        StringPrototypeCharCodeAt(request, request.length - 2) === CHAR_FORWARD_SLASH ||
        (StringPrototypeCharCodeAt(request, request.length - 2) === CHAR_DOT && (
          request.length === 2 ||
          StringPrototypeCharCodeAt(request, request.length - 3) === CHAR_FORWARD_SLASH
        ))
      )
    ));

  let insidePath = true;
  if (isRelative(request)) {
    const normalizedRequest = path.normalize(request);
    if (StringPrototypeStartsWith(normalizedRequest, '..')) {
      insidePath = false;
    }
  }

  // For each path
  for (let i = 0; i < paths.length; i++) {
    // Don't search further if path doesn't exist
    const curPath = paths[i];
    if (typeof curPath !== 'string') {
      throw new ERR_INVALID_ARG_TYPE('paths', 'array of strings', paths);
    }
    if (insidePath && curPath && _stat(curPath) < 1) {
      continue;
    }

    if (!absoluteRequest) {
      const exportsResolved = resolveExports(curPath, request, conditions);
      if (exportsResolved) {
        return exportsResolved;
      }
    }

    const basePath = path.resolve(curPath, request);
    let filename;

    const rc = _stat(basePath);
    if (!trailingSlash) {
      if (rc === 0) {  // File.
        if (!isMain) {
          if (getOptionValue('--preserve-symlinks')) {
            filename = path.resolve(basePath);
          } else {
            filename = toRealPath(basePath);
          }
        } else if (getOptionValue('--preserve-symlinks-main')) {
          // For the main module, we use the --preserve-symlinks-main flag instead
          // mainly for backward compatibility, as the preserveSymlinks flag
          // historically has not applied to the main module.  Most likely this
          // was intended to keep .bin/ binaries working, as following those
          // symlinks is usually required for the imports in the corresponding
          // files to resolve; that said, in some use cases following symlinks
          // causes bigger problems which is why the --preserve-symlinks-main option
          // is needed.
          filename = path.resolve(basePath);
        } else {
          filename = toRealPath(basePath);
        }
      }

      if (!filename) {
        // Try it with each of the extensions
        if (exts === undefined) {
          exts = ObjectKeys(Module._extensions);
        }
        filename = tryExtensions(basePath, exts, isMain);
      }
    }

    if (!filename && rc === 1) {  // Directory.
      // try it with each of the extensions at "index"
      if (exts === undefined) {
        exts = ObjectKeys(Module._extensions);
      }
      filename = tryPackage(basePath, exts, isMain, request);
    }

    if (filename) {
      Module._pathCache[cacheKey] = filename;
      return filename;
    }

    const extensions = [''];
    if (exts !== undefined) {
      ArrayPrototypePushApply(extensions, exts);
    }
    reportModuleNotFoundToWatchMode(basePath, extensions);
  }

  return false;
};

/** `node_modules` character codes reversed */
const nmChars = [ 115, 101, 108, 117, 100, 111, 109, 95, 101, 100, 111, 110 ];
const nmLen = nmChars.length;
if (isWindows) {
  /**
   * Get the paths to the `node_modules` folder for a given path.
   * @param {string} from `__dirname` of the module
   * @returns {string[]}
   */
  Module._nodeModulePaths = function(from) {
    // Guarantee that 'from' is absolute.
    from = path.resolve(from);

    // note: this approach *only* works when the path is guaranteed
    // to be absolute.  Doing a fully-edge-case-correct path.split
    // that works on both Windows and Posix is non-trivial.

    // return root node_modules when path is 'D:\\'.
    // path.resolve will make sure from.length >=3 in Windows.
    if (StringPrototypeCharCodeAt(from, from.length - 1) ===
          CHAR_BACKWARD_SLASH &&
        StringPrototypeCharCodeAt(from, from.length - 2) === CHAR_COLON) {
      return [from + 'node_modules'];
    }

    /** @type {string[]} */
    const paths = [];
    for (let i = from.length - 1, p = 0, last = from.length; i >= 0; --i) {
      const code = StringPrototypeCharCodeAt(from, i);
      // The path segment separator check ('\' and '/') was used to get
      // node_modules path for every path segment.
      // Use colon as an extra condition since we can get node_modules
      // path for drive root like 'C:\node_modules' and don't need to
      // parse drive name.
      if (code === CHAR_BACKWARD_SLASH ||
          code === CHAR_FORWARD_SLASH ||
          code === CHAR_COLON) {
        if (p !== nmLen) {
          ArrayPrototypePush(
            paths,
            StringPrototypeSlice(from, 0, last) + '\\node_modules',
          );
        }
        last = i;
        p = 0;
      } else if (p !== -1) {
        if (nmChars[p] === code) {
          ++p;
        } else {
          p = -1;
        }
      }
    }

    return paths;
  };
} else { // posix
  /**
   * Get the paths to the `node_modules` folder for a given path.
   * @param {string} from `__dirname` of the module
   * @returns {string[]}
   */
  Module._nodeModulePaths = function(from) {
    // Guarantee that 'from' is absolute.
    from = path.resolve(from);
    // Return early not only to avoid unnecessary work, but to *avoid* returning
    // an array of two items for a root: [ '//node_modules', '/node_modules' ]
    if (from === '/') {
      return ['/node_modules'];
    }

    // note: this approach *only* works when the path is guaranteed
    // to be absolute.  Doing a fully-edge-case-correct path.split
    // that works on both Windows and Posix is non-trivial.
    /** @type {string[]} */
    const paths = [];
    for (let i = from.length - 1, p = 0, last = from.length; i >= 0; --i) {
      const code = StringPrototypeCharCodeAt(from, i);
      if (code === CHAR_FORWARD_SLASH) {
        if (p !== nmLen) {
          ArrayPrototypePush(
            paths,
            StringPrototypeSlice(from, 0, last) + '/node_modules',
          );
        }
        last = i;
        p = 0;
      } else if (p !== -1) {
        if (nmChars[p] === code) {
          ++p;
        } else {
          p = -1;
        }
      }
    }

    // Append /node_modules to handle root paths.
    ArrayPrototypePush(paths, '/node_modules');

    return paths;
  };
}

/**
 * Get the paths for module resolution.
 * @param {string} request
 * @param {Module} parent
 * @returns {null|string[]}
 */
Module._resolveLookupPaths = function(request, parent) {
  if (BuiltinModule.normalizeRequirableId(request)) {
    debug('looking for %j in []', request);
    return null;
  }

  // Check for node modules paths.
  if (StringPrototypeCharAt(request, 0) !== '.' ||
      (request.length > 1 &&
      StringPrototypeCharAt(request, 1) !== '.' &&
      StringPrototypeCharAt(request, 1) !== '/' &&
      (!isWindows || StringPrototypeCharAt(request, 1) !== '\\'))) {

    /** @type {string[]} */
    let paths;
    if (parent?.paths?.length) {
      paths = ArrayPrototypeSlice(modulePaths);
      ArrayPrototypeUnshiftApply(paths, parent.paths);
    } else {
      paths = modulePaths;
    }

    debug('looking for %j in %j', request, paths);
    return paths.length > 0 ? paths : null;
  }

  // In REPL, parent.filename is null.
  if (!parent || !parent.id || !parent.filename) {
    // Make require('./path/to/foo') work - normally the path is taken
    // from realpath(__filename) but in REPL there is no filename
    const mainPaths = ['.'];

    debug('looking for %j in %j', request, mainPaths);
    return mainPaths;
  }

  debug('RELATIVE: requested: %s from parent.id %s', request, parent.id);

  const parentDir = [path.dirname(parent.filename)];
  debug('looking for %j', parentDir);
  return parentDir;
};

/**
 * Emits a warning when a non-existent property of module exports is accessed inside a circular dependency.
 * @param {string} prop The name of the non-existent property.
 * @returns {void}
 */
function emitCircularRequireWarning(prop) {
  process.emitWarning(
    `Accessing non-existent property '${String(prop)}' of module exports ` +
    'inside circular dependency',
  );
}

// A Proxy that can be used as the prototype of a module.exports object and
// warns when non-existent properties are accessed.
const CircularRequirePrototypeWarningProxy = new Proxy({}, {
  __proto__: null,

  get(target, prop) {
    // Allow __esModule access in any case because it is used in the output
    // of transpiled code to determine whether something comes from an
    // ES module, and is not used as a regular key of `module.exports`.
    if (prop in target || prop === '__esModule') { return target[prop]; }
    emitCircularRequireWarning(prop);
    return undefined;
  },

  getOwnPropertyDescriptor(target, prop) {
    if (ObjectPrototypeHasOwnProperty(target, prop) || prop === '__esModule') {
      return ObjectGetOwnPropertyDescriptor(target, prop);
    }
    emitCircularRequireWarning(prop);
    return undefined;
  },
});

/**
 * Returns the exports object for a module that has a circular `require`.
 * If the exports object is a plain object, it is wrapped in a proxy that warns
 * about circular dependencies.
 * @param {Module} module The module instance
 * @returns {object}
 */
function getExportsForCircularRequire(module) {
  const requiredESM = module[kRequiredModuleSymbol];
  if (requiredESM && requiredESM.getStatus() !== kEvaluated) {
    let message = `Cannot require() ES Module ${module.id} in a cycle.`;
    const parent = module[kLastModuleParent];
    if (parent) {
      message += ` (from ${parent.filename})`;
    }
    throw new ERR_REQUIRE_CYCLE_MODULE(message);
  }

  if (module.exports &&
      !isProxy(module.exports) &&
      ObjectGetPrototypeOf(module.exports) === ObjectPrototype &&
      // Exclude transpiled ES6 modules / TypeScript code because those may
      // employ unusual patterns for accessing 'module.exports'. That should
      // be okay because ES6 modules have a different approach to circular
      // dependencies anyway.
      !module.exports.__esModule) {
    // This is later unset once the module is done loading.
    ObjectSetPrototypeOf(
      module.exports, CircularRequirePrototypeWarningProxy);
  }

  return module.exports;
}


/**
 * Wraps result of Module._resolveFilename to include additional fields for hooks.
 * See resolveForCJSWithHooks.
 * @param {string} specifier
 * @param {Module|undefined} parent
 * @param {boolean} isMain
 * @param {ResolveFilenameOptions} options
 * @returns {{url?: string, format?: string, parentURL?: string, filename: string}}
 */
function wrapResolveFilename(specifier, parent, isMain, options) {
  const filename = Module._resolveFilename(specifier, parent, isMain, options).toString();
  return { __proto__: null, url: undefined, format: undefined, filename };
}

/**
 * See resolveForCJSWithHooks.
 * @param {string} specifier
 * @param {Module|undefined} parent
 * @param {boolean} isMain
 * @param {ResolveFilenameOptions} options
 * @returns {{url?: string, format?: string, parentURL?: string, filename: string}}
 */
function defaultResolveImplForCJSLoading(specifier, parent, isMain, options) {
  // For backwards compatibility, when encountering requests starting with node:,
  // throw ERR_UNKNOWN_BUILTIN_MODULE on failure or return the normalized ID on success
  // without going into Module._resolveFilename.
  let normalized;
  if (StringPrototypeStartsWith(specifier, 'node:')) {
    normalized = BuiltinModule.normalizeRequirableId(specifier);
    if (!normalized) {
      throw new ERR_UNKNOWN_BUILTIN_MODULE(specifier);
    }
    return { __proto__: null, url: specifier, format: 'builtin', filename: normalized };
  }
  return wrapResolveFilename(specifier, parent, isMain, options);
}

/**
 * Resolve a module request for CommonJS, invoking hooks from module.registerHooks()
 * if necessary.
 * @param {string} specifier
 * @param {Module|undefined} parent
 * @param {boolean} isMain
 * @param {object} internalResolveOptions
 * @param {boolean} internalResolveOptions.shouldSkipModuleHooks Whether to skip module hooks.
 * @param {ResolveFilenameOptions} internalResolveOptions.requireResolveOptions Options from require.resolve().
 *   Only used when it comes from require.resolve().
 * @returns {{url?: string, format?: string, parentURL?: string, filename: string}}
 */
function resolveForCJSWithHooks(specifier, parent, isMain, internalResolveOptions) {
  const { requireResolveOptions, shouldSkipModuleHooks } = internalResolveOptions;
  const defaultResolveImpl = requireResolveOptions ?
    wrapResolveFilename : defaultResolveImplForCJSLoading;
  // Fast path: no hooks, just return simple results.
  if (!resolveHooks.length || shouldSkipModuleHooks) {
    return defaultResolveImpl(specifier, parent, isMain, requireResolveOptions);
  }

  // Slow path: has hooks, do the URL conversions and invoke hooks with contexts.
  let parentURL;
  if (parent) {
    if (!parent[kURL] && parent.filename) {
      parent[kURL] = convertCJSFilenameToURL(parent.filename);
    }
    parentURL = parent[kURL];
  }

  // This is used as the last nextResolve for the resolve hooks.
  function defaultResolve(specifier, context) {
    // TODO(joyeecheung): parent and isMain should be part of context, then we
    // no longer need to use a different defaultResolve for every resolution.
    // In the hooks, context.conditions is passed around as an array, but internally
    // the resolution helpers expect a SafeSet. Do the conversion here.
    let conditionSet;
    const conditions = context.conditions;
    if (conditions !== undefined && conditions !== getCjsConditionsArray()) {
      if (!ArrayIsArray(conditions)) {
        throw new ERR_INVALID_ARG_VALUE('context.conditions', conditions,
                                        'expected an array');
      }
      conditionSet = new SafeSet(conditions);
    } else {
      conditionSet = getCjsConditions();
    }

    const result = defaultResolveImpl(specifier, parent, isMain, {
      __proto__: null,
      paths: requireResolveOptions?.paths,
      conditions: conditionSet,
    });
    // If the default resolver does not return a URL, convert it for the public API.
    result.url ??= convertCJSFilenameToURL(result.filename);

    // Remove filename because it's not part of the public API.
    // TODO(joyeecheung): maybe expose it in the public API to avoid re-conversion for users too.
    return { __proto__: null, url: result.url, format: result.format };
  }

  const resolveResult = resolveWithHooks(specifier, parentURL, /* importAttributes */ undefined,
                                         getCjsConditionsArray(), defaultResolve);
  const { url, format } = resolveResult;
  // Convert the URL from the hook chain back to a filename for internal use.
  const filename = convertURLToCJSFilename(url);
  const result = { __proto__: null, url, format, filename, parentURL };
  debug('resolveForCJSWithHooks', specifier, parent?.id, isMain, shouldSkipModuleHooks, '->', result);
  return result;
}

/**
 * @typedef {import('internal/modules/customization_hooks').ModuleLoadContext} ModuleLoadContext
 * @typedef {import('internal/modules/customization_hooks').ModuleLoadResult} ModuleLoadResult
 */

/**
 * Load the source code of a module based on format.
 * @param {string} filename Filename of the module.
 * @param {string|undefined|null} format Format of the module.
 * @returns {string|null}
 */
function defaultLoadImpl(filename, format) {
  switch (format) {
    case undefined:
    case null:
    case 'module':
    case 'commonjs':
    case 'json':
    case 'module-typescript':
    case 'commonjs-typescript':
    case 'typescript': {
      return fs.readFileSync(filename, 'utf8');
    }
    case 'builtin':
      return null;
    default:
      // URL is not necessarily necessary/available - convert it on the spot for errors.
      throw new ERR_UNKNOWN_MODULE_FORMAT(format, convertCJSFilenameToURL(filename));
  }
}

/**
 * Construct a last nextLoad() for load hooks invoked for the CJS loader.
 * @param {string} url URL passed from the hook.
 * @param {string} filename Filename inferred from the URL.
 * @returns {(url: string, context: ModuleLoadContext) => ModuleLoadResult}
 */
function getDefaultLoad(url, filename) {
  return function defaultLoad(urlFromHook, context) {
    // If the url is the same as the original one, save the conversion.
    const isLoadingOriginalModule = (urlFromHook === url);
    const filenameFromHook = isLoadingOriginalModule ? filename : convertURLToCJSFilename(urlFromHook);
    const source = defaultLoadImpl(filenameFromHook, context.format);
    // Format from context is directly returned, because format detection should only be
    // done after the entire load chain is completed.
    return { source, format: context.format };
  };
}

/**
 * Load a specified builtin module, invoking load hooks if necessary.
 * @param {string} id The module ID (without the node: prefix)
 * @param {string} url The module URL (with the node: prefix)
 * @param {string} format Format from resolution.
 * @returns {{builtinExports: any, resultFromHook: undefined|ModuleLoadResult}} If there are no load
 *   hooks or the load hooks do not override the format of the builtin, load and return the exports
 *   of the builtin module. Otherwise, return the loadResult for the caller to continue loading.
 */
function loadBuiltinWithHooks(id, url, format) {
  let resultFromHook;
  if (loadHooks.length) {
    url ??= `node:${id}`;
    debug('loadBuiltinWithHooks ', loadHooks.length, id, url, format);
    // TODO(joyeecheung): do we really want to invoke the load hook for the builtins?
    resultFromHook = loadWithHooks(url, format || 'builtin', /* importAttributes */ undefined,
                                   getCjsConditionsArray(), getDefaultLoad(url, id), validateLoadStrict);
    if (resultFromHook.format && resultFromHook.format !== 'builtin') {
      debug('loadBuiltinWithHooks overriding module', id, url, resultFromHook);
      // Format has been overridden, return result for the caller to continue loading.
      return { builtinExports: undefined, resultFromHook };
    }
  }

  // No hooks or the hooks have not overridden the format. Load it as a builtin module and return the
  // exports.
  const mod = loadBuiltinModule(id);
  return { builtinExports: mod.exports, resultFromHook: undefined };
}

/**
 * Load a module from cache if it exists, otherwise create a new module instance.
 * 1. If a module already exists in the cache: return its exports object.
 * 2. If the module is native: call
 *    `BuiltinModule.prototype.compileForPublicLoader()` and return the exports.
 * 3. Otherwise, create a new module for the file and save it to the cache.
 *    Then have it load the file contents before returning its exports object.
 * @param {string} request Specifier of module to load via `require`
 * @param {Module} parent Absolute path of the module importing the child
 * @param {boolean} isMain Whether the module is the main entry point
 * @param {object|undefined} internalResolveOptions Additional options for loading the module
 * @returns {object}
 */
Module._load = function(request, parent, isMain, internalResolveOptions = kEmptyObject) {
  let relResolveCacheIdentifier;
  if (parent) {
    debug('Module._load REQUEST %s parent: %s', request, parent.id);
    // Fast path for (lazy loaded) modules in the same directory. The indirect
    // caching is required to allow cache invalidation without changing the old
    // cache key names.
    relResolveCacheIdentifier = `${parent.path}\x00${request}`;
    const filename = relativeResolveCache[relResolveCacheIdentifier];
    reportModuleToWatchMode(filename);
    if (filename !== undefined) {
      const cachedModule = Module._cache[filename];
      if (cachedModule !== undefined) {
        updateChildren(parent, cachedModule, true);
        if (!cachedModule.loaded) {
          return getExportsForCircularRequire(cachedModule);
        }
        return cachedModule.exports;
      }
      delete relativeResolveCache[relResolveCacheIdentifier];
    }
  }

  const resolveResult = resolveForCJSWithHooks(request, parent, isMain, internalResolveOptions);
  let { format } = resolveResult;
  const { url, filename } = resolveResult;

  let resultFromLoadHook;
  // For backwards compatibility, if the request itself starts with node:, load it before checking
  // Module._cache. Otherwise, load it after the check.
  // TODO(joyeecheung): a more sensible handling is probably, if there are hooks, always go through the hooks
  // first before checking the cache. Otherwise, check the cache first, then proceed to default loading.
  if (request === url && StringPrototypeStartsWith(request, 'node:')) {
    const normalized = BuiltinModule.normalizeRequirableId(request);
    if (normalized) {  // It's a builtin module.
      const { resultFromHook, builtinExports } = loadBuiltinWithHooks(normalized, url, format);
      if (builtinExports) {
        return builtinExports;
      }
      // The format of the builtin has been overridden by user hooks. Continue loading.
      resultFromLoadHook = resultFromHook;
      format = resultFromLoadHook.format;
    }
  }

  // If load hooks overrides the format for a built-in, bypass the cache.
  let cachedModule;
  if (resultFromLoadHook === undefined) {
    cachedModule = Module._cache[filename];
    debug('Module._load checking cache for', filename, !!cachedModule);
    if (cachedModule !== undefined) {
      updateChildren(parent, cachedModule, true);
      if (cachedModule.loaded) {
        return cachedModule.exports;
      }
      // If it's not cached by the ESM loader, the loading request
      // comes from required CJS, and we can consider it a circular
      // dependency when it's cached.
      if (!cachedModule[kIsCachedByESMLoader]) {
        return getExportsForCircularRequire(cachedModule);
      }
      // If it's cached by the ESM loader as a way to indirectly pass
      // the module in to avoid creating it twice, the loading request
      // came from imported CJS. In that case use the kModuleCircularVisited
      // to determine if it's loading or not.
      if (cachedModule[kModuleCircularVisited]) {
        return getExportsForCircularRequire(cachedModule);
      }
      // This is an ESM loader created cache entry, mark it as visited and fallthrough to loading the module.
      cachedModule[kModuleCircularVisited] = true;
    }
  }

  if (resultFromLoadHook === undefined && BuiltinModule.canBeRequiredWithoutScheme(filename)) {
    const { resultFromHook, builtinExports } = loadBuiltinWithHooks(filename, url, format);
    if (builtinExports) {
      return builtinExports;
    }
    // The format of the builtin has been overridden by user hooks. Continue loading.
    resultFromLoadHook = resultFromHook;
    format = resultFromLoadHook.format;
  }

  // Don't call updateChildren(), Module constructor already does.
  const module = cachedModule || new Module(filename, parent);

  if (!cachedModule) {
    if (isMain) {
      setOwnProperty(process, 'mainModule', module);
      setOwnProperty(module.require, 'main', process.mainModule);
      module.id = '.';
      module[kIsMainSymbol] = true;
    } else {
      module[kIsMainSymbol] = false;
    }
    if (resultFromLoadHook !== undefined) {
      module[kModuleSource] = resultFromLoadHook.source;
    }

    reportModuleToWatchMode(filename);
    Module._cache[filename] = module;
    module[kIsCachedByESMLoader] = false;
    // If there are resolve hooks, carry the context information into the
    // load hooks for the module keyed by the (potentially customized) filename.
    module[kURL] = url;
    module[kFormat] = format;
  } else {
    module[kLastModuleParent] = parent;
  }

  if (parent !== undefined) {
    relativeResolveCache[relResolveCacheIdentifier] = filename;
  }

  let threw = true;
  try {
    module.load(filename);
    threw = false;
  } finally {
    if (threw) {
      delete Module._cache[filename];
      if (parent !== undefined) {
        delete relativeResolveCache[relResolveCacheIdentifier];
        const children = parent?.children;
        if (ArrayIsArray(children)) {
          const index = ArrayPrototypeIndexOf(children, module);
          if (index !== -1) {
            ArrayPrototypeSplice(children, index, 1);
          }
        }
      }
    } else if (module.exports &&
               !isProxy(module.exports) &&
               ObjectGetPrototypeOf(module.exports) ===
                 CircularRequirePrototypeWarningProxy) {
      ObjectSetPrototypeOf(module.exports, ObjectPrototype);
    }
  }

  return module.exports;
};

/**
 * Given a `require` string and its context, get its absolute file path.
 * @param {string} request The specifier to resolve
 * @param {Module} parent The module containing the `require` call
 * @param {boolean} isMain Whether the module is the main entry point
 * @param {ResolveFilenameOptions} options Options object
 * @typedef {object} ResolveFilenameOptions
 * @property {string[]} paths Paths to search for modules in
 * @property {Set<string>?} conditions The conditions to use for resolution.
 * @returns {void|string}
 */
Module._resolveFilename = function(request, parent, isMain, options) {
  if (BuiltinModule.normalizeRequirableId(request)) {
    return request;
  }
  const conditions = (options?.conditions) || getCjsConditions();

  let paths;

  if (typeof options === 'object' && options !== null) {
    if (ArrayIsArray(options.paths)) {
      if (isRelative(request)) {
        paths = options.paths;
      } else {
        const fakeParent = new Module('', null);

        paths = [];

        for (let i = 0; i < options.paths.length; i++) {
          const path = options.paths[i];
          fakeParent.paths = Module._nodeModulePaths(path);
          const lookupPaths = Module._resolveLookupPaths(request, fakeParent);

          for (let j = 0; j < lookupPaths.length; j++) {
            if (!ArrayPrototypeIncludes(paths, lookupPaths[j])) {
              ArrayPrototypePush(paths, lookupPaths[j]);
            }
          }
        }
      }
    } else if (options.paths === undefined) {
      paths = Module._resolveLookupPaths(request, parent);
    } else {
      throw new ERR_INVALID_ARG_VALUE('options.paths', options.paths);
    }
  } else {
    paths = Module._resolveLookupPaths(request, parent);
  }

  if (request[0] === '#' && (parent?.filename || parent?.id === '<repl>')) {
    const parentPath = parent?.filename ?? process.cwd() + path.sep;
    const pkg = packageJsonReader.getNearestParentPackageJSON(parentPath);
    if (pkg?.data.imports != null) {
      try {
        const { packageImportsResolve } = require('internal/modules/esm/resolve');
        return finalizeEsmResolution(
          packageImportsResolve(request, pathToFileURL(parentPath), conditions),
          parentPath,
          pkg.path,
        );
      } catch (e) {
        if (e.code === 'ERR_MODULE_NOT_FOUND') {
          throw createEsmNotFoundErr(request);
        }
        throw e;
      }
    }
  }

  // Try module self resolution first
  const parentPath = trySelfParentPath(parent);
  const selfResolved = trySelf(parentPath, request, conditions);
  if (selfResolved) {
    const cacheKey = request + '\x00' +
         (paths.length === 1 ? paths[0] : ArrayPrototypeJoin(paths, '\x00'));
    Module._pathCache[cacheKey] = selfResolved;
    return selfResolved;
  }

  // Look up the filename first, since that's the cache key.
  const filename = Module._findPath(request, paths, isMain, conditions);
  if (filename) { return filename; }
  const requireStack = [];
  for (let cursor = parent;
    cursor;
    // TODO(joyeecheung): it makes more sense to use kLastModuleParent here.
    cursor = cursor[kFirstModuleParent]) {
    ArrayPrototypePush(requireStack, cursor.filename || cursor.id);
  }
  let message = `Cannot find module '${request}'`;
  if (requireStack.length > 0) {
    message = message + '\nRequire stack:\n- ' +
              ArrayPrototypeJoin(requireStack, '\n- ');
  }
  // eslint-disable-next-line no-restricted-syntax
  const err = new Error(message);
  err.code = 'MODULE_NOT_FOUND';
  err.requireStack = requireStack;
  throw err;
};

/**
 * Finishes resolving an ES module specifier into an absolute file path.
 * @param {string} resolved The resolved module specifier
 * @param {string} parentPath The path of the parent module
 * @param {string} pkgPath The path of the package.json file
 * @throws {ERR_INVALID_MODULE_SPECIFIER} If the resolved module specifier contains encoded `/` or `\\` characters
 * @throws {Error} If the module cannot be found
 * @returns {void|string|undefined}
 */
function finalizeEsmResolution(resolved, parentPath, pkgPath) {
  const { encodedSepRegEx } = require('internal/modules/esm/resolve');
  if (RegExpPrototypeExec(encodedSepRegEx, resolved) !== null) {
    throw new ERR_INVALID_MODULE_SPECIFIER(
      resolved, 'must not include encoded "/" or "\\" characters', parentPath);
  }
  const filename = fileURLToPath(resolved);
  const actual = tryFile(filename);
  if (actual) {
    return actual;
  }
  throw createEsmNotFoundErr(filename, pkgPath);
}

/**
 * Creates an error object for when a requested ES module cannot be found.
 * @param {string} request The name of the requested module
 * @param {string} [path] The path to the requested module
 * @returns {Error}
 */
function createEsmNotFoundErr(request, path) {
  // eslint-disable-next-line no-restricted-syntax
  const err = new Error(`Cannot find module '${request}'`);
  err.code = 'MODULE_NOT_FOUND';
  if (path) {
    err.path = path;
  }
  // TODO(BridgeAR): Add the requireStack as well.
  return err;
}

function getExtensionForFormat(format) {
  switch (format) {
    case 'addon':
      return '.node';
