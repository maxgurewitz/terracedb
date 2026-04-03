'use strict';

const {
  ArrayIsArray,
  ArrayPrototypeJoin,
  ArrayPrototypeMap,
  JSONStringify,
  ObjectGetOwnPropertyNames,
  ObjectPrototypeHasOwnProperty,
  RegExpPrototypeExec,
  RegExpPrototypeSymbolReplace,
  SafeMap,
  SafeSet,
  String,
  StringPrototypeEndsWith,
  StringPrototypeIncludes,
  StringPrototypeIndexOf,
  StringPrototypeLastIndexOf,
  StringPrototypeReplace,
  StringPrototypeSlice,
  StringPrototypeSplit,
  StringPrototypeStartsWith,
  encodeURIComponent,
} = primordials;
const assert = require('internal/assert');
const internalFS = require('internal/fs/utils');
const { BuiltinModule } = require('internal/bootstrap/realm');
const { realpathSync } = require('fs');
const { getOptionValue } = require('internal/options');
// Do not eagerly grab .manifest, it may be in TDZ
const { sep, posix: { relative: relativePosixPath }, resolve } = require('path');
const { URL, pathToFileURL, fileURLToPath, isURL, URLParse } = require('internal/url');
const { getCWDURL, setOwnProperty } = require('internal/util');
const { canParse: URLCanParse } = internalBinding('url');
const { legacyMainResolve: FSLegacyMainResolve } = internalBinding('fs');
const {
  ERR_INPUT_TYPE_NOT_ALLOWED,
  ERR_INVALID_ARG_TYPE,
  ERR_INVALID_MODULE_SPECIFIER,
  ERR_INVALID_PACKAGE_CONFIG,
  ERR_INVALID_PACKAGE_TARGET,
  ERR_INVALID_URL,
  ERR_MODULE_NOT_FOUND,
  ERR_PACKAGE_IMPORT_NOT_DEFINED,
  ERR_PACKAGE_PATH_NOT_EXPORTED,
  ERR_UNSUPPORTED_DIR_IMPORT,
  ERR_UNSUPPORTED_RESOLVE_REQUEST,
} = require('internal/errors').codes;
const { defaultGetFormatWithoutErrors } = require('internal/modules/esm/get_format');
const { getConditionsSet } = require('internal/modules/esm/utils');
const packageJsonReader = require('internal/modules/package_json_reader');
const internalFsBinding = internalBinding('fs');

/**
 * @typedef {import('internal/modules/esm/package_config.js').PackageConfig} PackageConfig
 */


const emittedPackageWarnings = new SafeSet();

/**
 * Emits a deprecation warning for the use of a deprecated trailing slash pattern mapping in the "exports" field
 * module resolution of a package.
 * @param {string} match - The deprecated trailing slash pattern mapping.
 * @param {string} pjsonUrl - The URL of the package.json file.
 * @param {string} base - The URL of the module that imported the package.
 */
function emitTrailingSlashPatternDeprecation(match, pjsonUrl, base) {
  if (process.noDeprecation) {
    return;
  }
  const pjsonPath = fileURLToPath(pjsonUrl);
  if (emittedPackageWarnings.has(pjsonPath + '|' + match)) { return; }
  emittedPackageWarnings.add(pjsonPath + '|' + match);
  process.emitWarning(
    `Use of deprecated trailing slash pattern mapping "${match}" in the ` +
    `"exports" field module resolution of the package at ${pjsonPath}${
      base ? ` imported from ${fileURLToPath(base)}` :
        ''}. Mapping specifiers ending in "/" is no longer supported.`,
    'DeprecationWarning',
    'DEP0155',
  );
}

const doubleSlashRegEx = /[/\\][/\\]/;

/**
 * Emits a deprecation warning for invalid segment in module resolution.
 * @param {string} target - The target module.
 * @param {string} request - The requested module.
 * @param {string} match - The matched module.
 * @param {string} pjsonUrl - The package.json URL.
 * @param {boolean} internal - Whether the module is in the "imports" or "exports" field.
 * @param {string} base - The base URL.
 * @param {boolean} isTarget - Whether the target is a module.
 */
function emitInvalidSegmentDeprecation(target, request, match, pjsonUrl, internal, base, isTarget) {
  if (process.noDeprecation) {
    return;
  }
  const pjsonPath = fileURLToPath(pjsonUrl);
  const double = RegExpPrototypeExec(doubleSlashRegEx, isTarget ? target : request) !== null;
  process.emitWarning(
    `Use of deprecated ${double ? 'double slash' :
      'leading or trailing slash matching'} resolving "${target}" for module ` +
      `request "${request}" ${request !== match ? `matched to "${match}" ` : ''
      }in the "${internal ? 'imports' : 'exports'}" field module resolution of the package at ${
        pjsonPath}${base ? ` imported from ${fileURLToPath(base)}` : ''}.`,
    'DeprecationWarning',
    'DEP0166',
  );
}

/**
 * Emits a deprecation warning if the given URL is a module and
 * the package.json file does not define a "main" or "exports" field.
 * @param {URL} url - The URL of the module being resolved.
 * @param {string} path - The path of the module being resolved.
 * @param {string} pkgPath - The path of the parent dir of the package.json file for the module.
 * @param {string | URL} [base] - The base URL for the module being resolved.
 * @param {string} [main] - The "main" field from the package.json file.
 */
function emitLegacyIndexDeprecation(url, path, pkgPath, base, main) {
  if (process.noDeprecation) {
    return;
  }
  const format = defaultGetFormatWithoutErrors(url);
  if (format !== 'module') { return; }
  const basePath = fileURLToPath(base);
  if (!main) {
    process.emitWarning(
      `No "main" or "exports" field defined in the package.json for ${pkgPath
      } resolving the main entry point "${
        StringPrototypeSlice(path, pkgPath.length)}", imported from ${basePath
      }.\nDefault "index" lookups for the main are deprecated for ES modules.`,
      'DeprecationWarning',
      'DEP0151',
    );
  } else if (resolve(pkgPath, main) !== path) {
    process.emitWarning(
      `Package ${pkgPath} has a "main" field set to "${main}", ` +
      `excluding the full filename and extension to the resolved file at "${
        StringPrototypeSlice(path, pkgPath.length)}", imported from ${
        basePath}.\n Automatic extension resolution of the "main" field is ` +
      'deprecated for ES modules.',
      'DeprecationWarning',
      'DEP0151',
    );
  }
}

const realpathCache = new SafeMap();

const legacyMainResolveExtensions = [
  '',
  '.js',
  '.json',
  '.node',
  '/index.js',
  '/index.json',
  '/index.node',
  './index.js',
  './index.json',
  './index.node',
];

const legacyMainResolveExtensionsIndexes = {
  // 0-6: when packageConfig.main is defined
  kResolvedByMain: 0,
  kResolvedByMainJs: 1,
  kResolvedByMainJson: 2,
  kResolvedByMainNode: 3,
  kResolvedByMainIndexJs: 4,
  kResolvedByMainIndexJson: 5,
  kResolvedByMainIndexNode: 6,
  // 7-9: when packageConfig.main is NOT defined,
  //      or when the previous case didn't found the file
  kResolvedByPackageAndJs: 7,
  kResolvedByPackageAndJson: 8,
  kResolvedByPackageAndNode: 9,
};

/**
 * Legacy CommonJS main resolution:
 * 1. let M = pkg_url + (json main field)
 * 2. TRY(M, M.js, M.json, M.node)
 * 3. TRY(M/index.js, M/index.json, M/index.node)
 * 4. TRY(pkg_url/index.js, pkg_url/index.json, pkg_url/index.node)
 * 5. NOT_FOUND
 * @param {URL} packageJSONUrl
 * @param {import('typings/internalBinding/modules').PackageConfig} packageConfig
 * @param {string | URL | undefined} base
 * @returns {URL}
 */
function legacyMainResolve(packageJSONUrl, packageConfig, base) {
  assert(isURL(packageJSONUrl));
  const pkgPath = fileURLToPath(new URL('.', packageJSONUrl));

  const baseStringified = isURL(base) ? base.href : base;

  const resolvedOption = FSLegacyMainResolve(pkgPath, packageConfig.main, baseStringified);

  const maybeMain = resolvedOption <= legacyMainResolveExtensionsIndexes.kResolvedByMainIndexNode ?
    packageConfig.main || './' : '';
  const resolvedPath = resolve(pkgPath, maybeMain + legacyMainResolveExtensions[resolvedOption]);
  const resolvedUrl = pathToFileURL(resolvedPath);

  emitLegacyIndexDeprecation(resolvedUrl, resolvedPath, pkgPath, base, packageConfig.main);

  return resolvedUrl;
}

const encodedSepRegEx = /%2F|%5C/i;
/**
 * Finalizes the resolution of a module specifier by checking if the resolved pathname contains encoded "/" or "\\"
 * characters, checking if the resolved pathname is a directory or file, and resolving any symlinks if necessary.
 * @param {URL} resolved - The resolved URL object.
 * @param {string | URL | undefined} base - The base URL object.
 * @param {boolean} preserveSymlinks - Whether to preserve symlinks or not.
 * @returns {URL} - The finalized URL object.
 * @throws {ERR_INVALID_MODULE_SPECIFIER} - If the resolved pathname contains encoded "/" or "\\" characters.
 * @throws {ERR_UNSUPPORTED_DIR_IMPORT} - If the resolved pathname is a directory.
 * @throws {ERR_MODULE_NOT_FOUND} - If the resolved pathname is not a file.
 */
function finalizeResolution(resolved, base, preserveSymlinks) {
  if (RegExpPrototypeExec(encodedSepRegEx, resolved.pathname) !== null) {
    let basePath;
    try {
      basePath = fileURLToPath(base);
    } catch {
      basePath = base;
    }
    throw new ERR_INVALID_MODULE_SPECIFIER(
      resolved.pathname, 'must not include encoded "/" or "\\" characters',
      basePath);
  }

  let path;
  try {
    path = fileURLToPath(resolved);
  } catch (err) {
    setOwnProperty(err, 'input', `${resolved}`);
    setOwnProperty(err, 'module', `${base}`);
    throw err;
  }

  const stats = internalFsBinding.internalModuleStat(
    StringPrototypeEndsWith(path, '/') ? StringPrototypeSlice(path, -1) : path,
  );

  // Check for stats.isDirectory()
  if (stats === 1) {
    let basePath;
    try {
      basePath = fileURLToPath(base);
    } catch {
      basePath = base;
    }
    throw new ERR_UNSUPPORTED_DIR_IMPORT(path, basePath, String(resolved));
  } else if (stats !== 0) {
    // Check for !stats.isFile()
    if (process.env.WATCH_REPORT_DEPENDENCIES && process.send) {
      process.send({ 'watch:require': [path || resolved.pathname] });
    }
    let basePath;
    try {
      basePath = fileURLToPath(base);
    } catch {
      basePath = base;
    }
    throw new ERR_MODULE_NOT_FOUND(
      path || resolved.pathname, basePath, resolved);
  }

  if (!preserveSymlinks) {
    const real = realpathSync(path, {
      [internalFS.realpathCacheKey]: realpathCache,
    });
    const { search, hash } = resolved;
    resolved =
        pathToFileURL(real + (StringPrototypeEndsWith(path, sep) ? '/' : ''));
    resolved.search = search;
    resolved.hash = hash;
  }

  return resolved;
}

/**
 * Returns an error object indicating that the specified import is not defined.
 * @param {string} specifier - The import specifier that is not defined.
 * @param {URL} packageJSONUrl - The URL of the package.json file, or null if not available.
 * @param {string | URL | undefined} base - The base URL to use for resolving relative URLs.
 * @returns {ERR_PACKAGE_IMPORT_NOT_DEFINED} - The error object.
 */
function importNotDefined(specifier, packageJSONUrl, base) {
  return new ERR_PACKAGE_IMPORT_NOT_DEFINED(
    specifier, packageJSONUrl && fileURLToPath(new URL('.', packageJSONUrl)),
    fileURLToPath(base));
}

/**
 * Returns an error object indicating that the specified subpath was not exported by the package.
 * @param {string} subpath - The subpath that was not exported.
 * @param {URL} packageJSONUrl - The URL of the package.json file.
 * @param {string | URL | undefined} [base] - The base URL to use for resolving the subpath.
 * @returns {ERR_PACKAGE_PATH_NOT_EXPORTED} - The error object.
 */
function exportsNotFound(subpath, packageJSONUrl, base) {
  return new ERR_PACKAGE_PATH_NOT_EXPORTED(
    fileURLToPath(new URL('.', packageJSONUrl)), subpath,
    base && fileURLToPath(base));
}

/**
 * Throws an error indicating that the given request is not a valid subpath match for the specified pattern.
 * @param {string} request - The request that failed to match the pattern.
 * @param {string} match - The pattern that the request was compared against.
 * @param {URL} packageJSONUrl - The URL of the package.json file being resolved.
 * @param {boolean} internal - Whether the resolution is for an "imports" or "exports" field in package.json.
 * @param {string | URL | undefined} base - The base URL for the resolution.
 * @throws {ERR_INVALID_MODULE_SPECIFIER} When the request is not a valid match for the pattern.
 */
function throwInvalidSubpath(request, match, packageJSONUrl, internal, base) {
  const reason = `request is not a valid match in pattern "${match}" for the "${
    internal ? 'imports' : 'exports'}" resolution of ${
    fileURLToPath(packageJSONUrl)}`;
  throw new ERR_INVALID_MODULE_SPECIFIER(request, reason,
                                         base && fileURLToPath(base));
}

/**
 * Creates an error object for an invalid package target.
 * @param {string} subpath - The subpath.
 * @param {import('internal/modules/esm/package_config.js').PackageTarget} target - The target.
 * @param {URL} packageJSONUrl - The URL of the package.json file.
 * @param {boolean} internal - Whether the package is internal.
 * @param {string | URL | undefined} base - The base URL.
 * @returns {ERR_INVALID_PACKAGE_TARGET} - The error object.
 */
