# Node Module Resolution Reference

This note pins the upstream Node sources and tests that we are using as the
reference point for the sandbox's CommonJS and ESM resolution behavior.

## Target

- Node compatibility target: `v24.12.0`

## Vendored Reference Excerpts

These are copied from the vendored Node checkout in `~/dev/node` and are kept in
the repo only as implementation references:

- [cjs-loader-resolution.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/reference/node-v24.12.0/cjs-loader-resolution.js)
  Source excerpt from:
  `/Users/maxwellgurewitz/dev/node/lib/internal/modules/cjs/loader.js`
  Focus:
  - `Module._findPath`
  - `Module._nodeModulePaths`
  - `Module._resolveLookupPaths`
  - `Module._resolveFilename`
  - circular-require export behavior

- [esm-resolve.js](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/reference/node-v24.12.0/esm-resolve.js)
  Source excerpt from:
  `/Users/maxwellgurewitz/dev/node/lib/internal/modules/esm/resolve.js`
  Focus:
  - `finalizeResolution`
  - legacy package main fallback
  - package `imports` / `exports`
  - ESM file and package target resolution

## Upstream Tests We Are Using As References

### CommonJS

- `/Users/maxwellgurewitz/dev/node/test/parallel/test-require-resolve.js`
- `/Users/maxwellgurewitz/dev/node/test/parallel/test-require-resolve-opts-paths-relative.js`
- `/Users/maxwellgurewitz/dev/node/test/parallel/test-require-resolve-invalid-paths.js`
- `/Users/maxwellgurewitz/dev/node/test/fixtures/require-resolve.js`

Mapped local coverage:

- [node_require_resolve.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/node_require_resolve.rs)

### ESM / `import`-style resolution

- `/Users/maxwellgurewitz/dev/node/test/es-module/test-esm-imports.mjs`
- `/Users/maxwellgurewitz/dev/node/test/es-module/test-require-module-conditional-exports.js`
- `/Users/maxwellgurewitz/dev/node/test/es-module/test-esm-exports.mjs`
- `/Users/maxwellgurewitz/dev/node/test/es-module/test-esm-import-meta-resolve.mjs`

Mapped local coverage:

- [node_package_resolution.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/node_package_resolution.rs)
- [node_esm_loader.rs](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/crates/terracedb-sandbox/tests/node_esm_loader.rs)

## Current Scope

The sandbox should converge on Node's resolution behavior in this order:

1. CommonJS `require(...)`
2. `require.resolve(...)` and `require.resolve.paths(...)`
3. ESM package `imports` / `exports` and conditional resolution
4. `import.meta.resolve(...)`

The current tests cover the first three areas directly. `import.meta.resolve(...)`
is identified here as an upstream reference surface and should be brought under
parity once the underlying `import.meta` host hook is exposed cleanly through the
Boa bridge.
