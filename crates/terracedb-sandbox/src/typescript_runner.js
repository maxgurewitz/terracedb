import ts from "typescript";
import {
  readJsonFile,
  readTextFile,
  readdir,
  stat,
  writeTextFile,
} from "@terracedb/sandbox/fs";

const NPM_INSTALL_MANIFEST_PATH = "/.terrace/npm/install-manifest.json";

function normalizePath(path) {
  const parts = [];
  for (const part of String(path).split("/")) {
    if (!part || part === ".") continue;
    if (part === "..") {
      parts.pop();
      continue;
    }
    parts.push(part);
  }
  return `/${parts.join("/")}`;
}

function dirname(path) {
  const normalized = normalizePath(path);
  const index = normalized.lastIndexOf("/");
  return index <= 0 ? "/" : normalized.slice(0, index);
}

function joinPath(base, next) {
  if (String(next).startsWith("/")) {
    return normalizePath(next);
  }
  return normalizePath(`${base}/${next}`);
}

function fileExtension(path) {
  const normalized = normalizePath(path);
  const index = normalized.lastIndexOf(".");
  return index === -1 ? "" : normalized.slice(index);
}

function statSafe(path) {
  try {
    return stat(normalizePath(path));
  } catch {
    return null;
  }
}

function fileExists(path) {
  return statSafe(path)?.kind === "file";
}

function directoryExists(path) {
  return statSafe(path)?.kind === "directory";
}

function readFile(path) {
  try {
    return readTextFile(normalizePath(path));
  } catch {
    return undefined;
  }
}

function getDirectories(path) {
  try {
    return readdir(normalizePath(path))
      .filter((entry) => entry.kind === "directory")
      .map((entry) => normalizePath(`${path}/${entry.name}`));
  } catch {
    return [];
  }
}

function getFileSystemEntries(path) {
  try {
    const directories = [];
    const files = [];
    for (const entry of readdir(normalizePath(path))) {
      if (entry.kind === "directory") {
        directories.push(entry.name);
      } else if (entry.kind === "file") {
        files.push(entry.name);
      }
    }
    return { files, directories };
  } catch {
    return { files: [], directories: [] };
  }
}

function readDirectory(path, extensions, excludes, includes, depth) {
  return ts.matchFiles(
    normalizePath(path),
    extensions,
    excludes,
    includes,
    true,
    "/",
    depth,
    getFileSystemEntries,
    normalizePath,
  );
}

function jsxKind(value) {
  switch (String(value || "").toLowerCase()) {
    case "preserve":
      return ts.JsxEmit.Preserve;
    case "react":
      return ts.JsxEmit.React;
    case "react-jsx":
      return ts.JsxEmit.ReactJSX;
    case "react-jsxdev":
      return ts.JsxEmit.ReactJSXDev;
    case "react-native":
      return ts.JsxEmit.ReactNative;
    default:
      return undefined;
  }
}

function scriptTarget(value) {
  switch (String(value || "").toLowerCase()) {
    case "es2015":
    case "es6":
      return ts.ScriptTarget.ES2015;
    case "es2016":
      return ts.ScriptTarget.ES2016;
    case "es2017":
      return ts.ScriptTarget.ES2017;
    case "es2018":
      return ts.ScriptTarget.ES2018;
    case "es2019":
      return ts.ScriptTarget.ES2019;
    case "es2020":
      return ts.ScriptTarget.ES2020;
    case "es2021":
      return ts.ScriptTarget.ES2021;
    case "es2022":
      return ts.ScriptTarget.ES2022;
    case "es2023":
      return ts.ScriptTarget.ES2023;
    case "esnext":
      return ts.ScriptTarget.ESNext;
    default:
      return undefined;
  }
}

function moduleKind(value) {
  switch (String(value || "").toLowerCase()) {
    case "cjs":
    case "commonjs":
      return ts.ModuleKind.CommonJS;
    case "esm":
    case "esnext":
      return ts.ModuleKind.ESNext;
    case "es2022":
      return ts.ModuleKind.ES2022;
    case "nodenext":
      return ts.ModuleKind.NodeNext;
    case "node16":
      return ts.ModuleKind.Node16;
    case "preserve":
      return ts.ModuleKind.Preserve;
    default:
      return undefined;
  }
}

function moduleResolutionKind(options) {
  if (options.moduleResolution != null) {
    return options.moduleResolution;
  }
  if (options.module === ts.ModuleKind.NodeNext) {
    return ts.ModuleResolutionKind.NodeNext;
  }
  if (options.module === ts.ModuleKind.Node16) {
    return ts.ModuleResolutionKind.Node16;
  }
  return ts.ModuleResolutionKind.Bundler;
}

function normalizeRoots(roots) {
  return Array.from(new Set((roots || []).map((root) => normalizePath(root))));
}

function packageCatalog() {
  try {
    const manifest = readJsonFile(NPM_INSTALL_MANIFEST_PATH);
    const byPackage = new Map();
    for (const entry of manifest?.packages || []) {
      byPackage.set(entry.package, entry);
    }
    return byPackage;
  } catch {
    return new Map();
  }
}

function packageResolutionFor(specifier, packages) {
  const entry = packages.get(specifier);
  if (!entry) {
    return undefined;
  }
  const packageRoot = normalizePath(entry.compatibility_root || entry.session_root);
  let resolvedPath = normalizePath(
    entry.compatibility_entrypoint_path || entry.entrypoint_path,
  );
  const packageJson = readFile(`${packageRoot}/package.json`);
  if (packageJson) {
    try {
      const parsed = JSON.parse(packageJson);
      if (typeof parsed.types === "string" && parsed.types.length > 0) {
        resolvedPath = joinPath(packageRoot, parsed.types);
      }
    } catch {
      // Ignore invalid package metadata and fall back to the runtime entrypoint.
    }
  }
  return {
    resolvedFileName: resolvedPath,
    extension: extensionFor(resolvedPath),
    isExternalLibraryImport: true,
  };
}

function extensionFor(path) {
  switch (fileExtension(path)) {
    case ".ts":
      return ts.Extension.Ts;
    case ".tsx":
      return ts.Extension.Tsx;
    case ".mts":
      return ts.Extension.Mts;
    case ".cts":
      return ts.Extension.Cts;
    case ".d.ts":
      return ts.Extension.Dts;
    case ".mjs":
      return ts.Extension.Mjs;
    case ".cjs":
      return ts.Extension.Cjs;
    case ".jsx":
      return ts.Extension.Jsx;
    case ".json":
      return ts.Extension.Json;
    default:
      return ts.Extension.Js;
  }
}

function createTerraceSys(cwd) {
  return {
    args: [],
    newLine: "\n",
    useCaseSensitiveFileNames: true,
    write(s) {
      return s;
    },
    readFile,
    writeFile(fileName, contents) {
      writeTextFile(normalizePath(fileName), contents);
    },
    fileExists,
    directoryExists,
    createDirectory(_path) {},
    getCurrentDirectory() {
      return cwd;
    },
    getDirectories,
    readDirectory,
    realpath: normalizePath,
    getExecutingFilePath() {
      return joinPath(cwd, ".terrace/typescript-runner.mjs");
    },
  };
}

function readProjectConfig(request, sys) {
  const tsconfigPath = request.tsconfig_path
    ? normalizePath(request.tsconfig_path)
    : undefined;
  if (!tsconfigPath) {
    return {
      options: {},
      rootNames: normalizeRoots(request.roots),
    };
  }

  const configFile = ts.readConfigFile(tsconfigPath, sys.readFile);
  if (configFile.error) {
    throw new Error(ts.flattenDiagnosticMessageText(configFile.error.messageText, "\n"));
  }
  const parsed = ts.parseJsonConfigFileContent(
    configFile.config,
    sys,
    dirname(tsconfigPath),
    undefined,
    tsconfigPath,
  );
  return {
    options: parsed.options,
    rootNames: request.roots?.length
      ? normalizeRoots(request.roots)
      : normalizeRoots(parsed.fileNames),
      errors: parsed.errors || [],
  };
}

function mergedCompilerOptions(request, projectOptions, typescriptLibRoot) {
  const options = {
    noEmit: request.mode === "check",
    allowJs: true,
    checkJs: false,
    resolveJsonModule: true,
    skipLibCheck: true,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    target: ts.ScriptTarget.ES2022,
    sourceMap: Boolean(request.force_source_map),
    ...projectOptions,
  };

  const overrideTarget = scriptTarget(request.target);
  if (overrideTarget != null) {
    options.target = overrideTarget;
  }

  const overrideModule = moduleKind(request.module_kind);
  if (overrideModule != null) {
    options.module = overrideModule;
  }

  const overrideJsx = jsxKind(request.jsx);
  if (overrideJsx != null) {
    options.jsx = overrideJsx;
  }

  options.moduleResolution = moduleResolutionKind(options);
  options.noEmit = request.mode === "check";
  if (request.force_source_map) {
    options.sourceMap = true;
  }
  options.configFilePath = request.tsconfig_path || options.configFilePath;
  options.types = [];
  options.typeRoots = [];
  options.lib = options.lib || [ts.getDefaultLibFileName(options)];
  options.terraceTypescriptLibRoot = typescriptLibRoot;
  return options;
}

function compilerHostFor(programRequest, sys, options, packages) {
  const emittedFiles = [];
  const rootOutputs = {};
  const rootSet = new Set(normalizeRoots(programRequest.roots));
  const host = ts.createCompilerHost(options);
  const moduleResolutionHost = {
    fileExists,
    readFile,
    directoryExists,
    getCurrentDirectory: sys.getCurrentDirectory,
    getDirectories,
    realpath: normalizePath,
  };

  host.fileExists = fileExists;
  host.readFile = readFile;
  host.directoryExists = directoryExists;
  host.getDirectories = getDirectories;
  host.realpath = normalizePath;
  host.getCurrentDirectory = sys.getCurrentDirectory;
  host.getCanonicalFileName = (fileName) => fileName;
  host.getNewLine = () => "\n";
  host.getDefaultLibFileName = (compilerOptions) =>
    joinPath(
      programRequest.typescript_lib_root,
      ts.getDefaultLibFileName(compilerOptions),
    );
  host.getDefaultLibLocation = () => programRequest.typescript_lib_root;
  host.writeFile = (fileName, contents, _bom, _onError, sourceFiles) => {
    const normalized = normalizePath(fileName);
    writeTextFile(normalized, contents);
    emittedFiles.push(normalized);
    for (const sourceFile of sourceFiles || []) {
      const candidate = normalizePath(sourceFile.fileName);
      if (!(candidate.endsWith(".ts") || candidate.endsWith(".tsx") || candidate.endsWith(".mts") || candidate.endsWith(".cts"))) {
        continue;
      }
      if (rootSet.has(candidate) && !normalized.endsWith(".map")) {
        rootOutputs[candidate] = normalized;
      }
    }
  };
  host.resolveModuleNames = (moduleNames, containingFile) =>
    moduleNames.map((moduleName) => {
      const resolved = ts.resolveModuleName(
        moduleName,
        normalizePath(containingFile),
        options,
        moduleResolutionHost,
      ).resolvedModule;
      if (resolved) {
        return {
          ...resolved,
          resolvedFileName: normalizePath(resolved.resolvedFileName),
        };
      }
      return packageResolutionFor(moduleName, packages);
    });

  return { host, emittedFiles, rootOutputs };
}

function formatDiagnostics(diagnostics) {
  const seen = new Set();
  const formatted = [];
  for (const diagnostic of diagnostics) {
    const path = diagnostic.file
      ? normalizePath(diagnostic.file.fileName)
      : "<typescript>";
    const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, "\n");
    const code = `TS${diagnostic.code}`;
    const key = `${path}\u001f${code}\u001f${message}`;
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    formatted.push({ path, message, code });
  }
  formatted.sort((left, right) =>
    left.path.localeCompare(right.path) ||
    left.code.localeCompare(right.code) ||
    left.message.localeCompare(right.message),
  );
  return formatted;
}

function sourceFiles(program) {
  return program
    .getSourceFiles()
    .map((sourceFile) => normalizePath(sourceFile.fileName))
    .sort();
}

export function runTypeScript(request) {
  const sys = createTerraceSys(request.cwd || "/workspace");
  const config = readProjectConfig(request, sys);
  const roots = config.rootNames.length
    ? config.rootNames
    : normalizeRoots(request.roots);
  if (!roots.length) {
    throw new Error("TypeScript roots cannot be empty");
  }

  const configErrors = formatDiagnostics(config.errors || []);
  if (configErrors.length) {
    return {
      diagnostics: configErrors,
      checked_files: roots,
    };
  }

  const options = mergedCompilerOptions(
    request,
    config.options,
    request.typescript_lib_root,
  );
  const packages = packageCatalog();
  const { host, emittedFiles, rootOutputs } = compilerHostFor(
    { ...request, roots, typescript_lib_root: request.typescript_lib_root },
    sys,
    options,
    packages,
  );
  const program = ts.createProgram({
    rootNames: roots,
    options,
    host,
  });
  const preEmitDiagnostics = ts.getPreEmitDiagnostics(program);

  if (request.mode === "check") {
    return {
      diagnostics: formatDiagnostics(preEmitDiagnostics),
      checked_files: sourceFiles(program),
    };
  }

  const emitResult = program.emit();
  return {
    diagnostics: formatDiagnostics([
      ...preEmitDiagnostics,
      ...emitResult.diagnostics,
    ]),
    checked_files: sourceFiles(program),
    emitted_files: Array.from(new Set(emittedFiles)).sort(),
    root_outputs: rootOutputs,
  };
}
