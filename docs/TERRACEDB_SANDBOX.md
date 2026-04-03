# TerraceDB Sandbox Architecture

`terracedb-sandbox` is an embedded JavaScript/TypeScript sandbox for AI agents. It should let guest code run inside the application while seeing a TerraceDB-backed virtual filesystem instead of the host filesystem, optionally call a constrained subset of the surrounding app, and use shell-like tooling without relying on native host binaries.

This document proposes an architecture that fits the current repo:

- `terracedb-vfs` already provides the right storage kernel: filesystem, KV, tool runs, snapshots, overlays, and visible vs durable cuts.
- `boa_engine` is the right low-level embedding surface for a guest JS runtime because it keeps the sandbox pure Rust.
- `just-bash` is a strong fit for shell utilities because it is designed for AI agents and accepts a custom async filesystem interface.
- `@typescript/vfs` is a good fit for TypeScript language service and `tsc`-style analysis, but it should not be treated as the runtime filesystem itself.

## Load-Bearing Recommendations

1. Model a sandbox session as a `terracedb-vfs` overlay volume, not as an ad hoc temp directory.
2. Treat execution, TypeScript tooling, and package installation as separate subsystems that share one virtual tree.
3. Start with `boa_engine` plus a custom `ModuleLoader`; do not try to rebuild full Deno/npm/Node compatibility in the first iteration.
4. Expose host integration as versioned capability modules, not ambient globals.
5. Make `bash`, `npm install`, `tsc`, and host API calls flow through `ToolRunStore` so they become part of the sandbox timeline.
6. Support pure JS/TS npm packages first; explicitly defer native Node-API addons and postinstall-heavy packages.
7. Treat host-disk hoist/eject as first-class operations with provenance metadata, not as an afterthought.
8. Make PR creation flow through git-aware export logic, preferably using ephemeral worktrees instead of mutating the user's checkout directly.

## Policy And Domain Separation

Capability policy and execution-domain policy are separate and composable:

- capability policy answers what a sandbox session, reviewed procedure, migration, or MCP adapter is allowed to touch,
- execution-domain policy answers where the work runs and which resource budgets apply once the capability check has succeeded.

Neither policy layer substitutes for the other. Authority must stay explicit in capability manifests and grants, while placement, concurrency, and budget decisions stay explicit in execution-domain policy.

## Why This Fits TerraceDB

The existing VFS architecture already describes the intended shape of an embedded sandbox:

- the library is path-oriented and designed to hand constrained capabilities to embedded runtimes,
- overlays provide cheap per-session copy-on-write isolation,
- tool runs and activity streams already exist for auditability,
- `flush()` and durable cuts already give you meaningful persistence fences.

That means `terracedb-sandbox` should be another library on top of the same primitives, not a new storage mode.

## Proposed Crate Layout

Add a new crate:

```text
crates/terracedb-sandbox/
  src/lib.rs
  src/session.rs
  src/runtime.rs
  src/loader.rs
  src/extensions.rs
  src/fs_ops.rs
  src/disk.rs
  src/git.rs
  src/pr.rs
  src/npm.rs
  src/bash.rs
  src/typescript.rs
  src/host_api.rs
  src/permissions.rs
  src/cache.rs
```

Reserve shared control-plane crates alongside the sandbox:

```text
crates/terracedb-capabilities/  capability templates, grants, manifests, presets, execution policy
crates/terracedb-migrate/       migration plan and migration history contracts
crates/terracedb-procedures/    reviewed procedure publication and invocation contracts
crates/terracedb-mcp/           external SSE-based MCP tool/resource and session contracts
```

Suggested top-level API:

```rust
pub struct SandboxConfig {
    pub base_volume_id: VolumeId,
    pub session_volume_id: VolumeId,
    pub durable_base: bool,
    pub npm_mode: NpmMode,
    pub node_compat: NodeCompatMode,
    pub typescript: TypeScriptMode,
    pub bash: BashMode,
    pub capabilities: CapabilityManifest,
}

pub trait SandboxStore {
    async fn open_session(&self, config: SandboxConfig) -> Result<SandboxSession, SandboxError>;
}

pub struct SandboxSession {
    pub volume: Arc<dyn OverlayVolume>,
    pub metadata: SandboxSessionInfo,
}

impl SandboxSession {
    pub async fn exec_module(&self, specifier: &str) -> Result<ExecOutcome, SandboxError>;
    pub async fn eval(&self, code: &str) -> Result<ExecOutcome, SandboxError>;
    pub async fn install_packages(&self, reqs: &[PackageReq]) -> Result<InstallReport, SandboxError>;
    pub async fn check_types(&self, roots: &[String]) -> Result<TypeCheckReport, SandboxError>;
    pub async fn hoist_from_disk(&self, req: HoistRequest) -> Result<HoistReport, SandboxError>;
    pub async fn eject_to_disk(&self, req: EjectRequest) -> Result<EjectReport, SandboxError>;
    pub async fn create_pull_request(&self, req: PullRequestRequest) -> Result<PullRequestReport, SandboxError>;
    pub async fn flush(&self) -> Result<(), SandboxError>;
}
```

## Session Model

Each agent sandbox should be backed by:

- a base `Volume` or `VolumeSnapshot` containing workspace files, templates, and cached dependencies,
- a writable overlay `OverlayVolume` for the live session,
- optional shared read-only cache volumes for npm tarballs, unpacked packages, or TypeScript standard library files.

Recommended layout inside the session volume:

```text
/workspace/                 guest-visible project root
/.terrace/
  session.json             session metadata and config
  execution-policy-state.json
                           versioned reopen-time execution budget counters
  cache/
    v8/                    code cache blobs
    transpile/             transpiled TS/JS metadata
  npm/
    package.json
    package-lock.json      or Terrace-native install manifest
    node_modules/          optional compatibility view
  typescript/
    libs/                  cached lib*.d.ts mirrors if needed
  tools/
    bash/
```

This uses the existing VFS overlay semantics instead of inventing another session boundary.

## Host Filesystem Interop

The new goals imply that `terracedb-sandbox` should not stay purely in-memory. It should have an explicit disk-interop layer that can:

- hoist a real directory or repo into a sandbox,
- eject a sandbox snapshot or delta back onto disk,
- do so with provenance and conflict detection.

This should live in `terracedb-sandbox`, not in `terracedb-vfs` itself. That keeps `terracedb-vfs` true to its current embedded-library focus while still letting the sandbox interoperate with real projects.

### Hoist Modes

Support three explicit import modes:

1. `DirectorySnapshot`
   Copy a host directory tree into a volume or overlay.
2. `GitHead`
   Import the tracked contents of a repo at `HEAD` without local uncommitted edits.
3. `GitWorkingTree`
   Import the current working tree, optionally including untracked files.

Suggested API:

```rust
pub enum HoistMode {
    DirectorySnapshot,
    GitHead,
    GitWorkingTree {
        include_untracked: bool,
        include_ignored: bool,
    },
}

pub struct HoistRequest {
    pub source_path: PathBuf,
    pub target_root: String,
    pub mode: HoistMode,
    pub filter: PathFilter,
    pub delete_missing: bool,
}
```

Recommended behavior:

- walk the source tree,
- normalize and filter paths,
- write into the target VFS root,
- store provenance metadata in `/.terrace/session.json` and/or VFS KV,
- for repo imports, record:
  - repo root,
  - HEAD commit,
  - branch,
  - remote URL,
  - hoist mode,
  - included pathspecs,
  - whether the source was dirty.

For repos, I would make `GitHead` the safest default. `GitWorkingTree` is useful, but it should be explicit because it makes later conflict handling more complex.

### Eject Modes

Support two main export modes:

1. `MaterializeSnapshot`
   Write the visible sandbox tree to a target directory.
2. `ApplyDelta`
   Apply only the sandbox changes relative to the recorded hoist base.

Suggested API:

```rust
pub enum EjectMode {
    MaterializeSnapshot,
    ApplyDelta,
}

pub struct EjectRequest {
    pub target_path: PathBuf,
    pub mode: EjectMode,
    pub conflict_policy: ConflictPolicy,
}
```

Recommended behavior:

- `MaterializeSnapshot`
  - best for exporting to a temp directory, archive root, or CI workspace,
  - writes the full visible tree.
- `ApplyDelta`
  - best for updating an existing checkout,
  - computes changes against the hoisted base and only applies the delta.

`ApplyDelta` should refuse to proceed blindly if the target checkout has diverged from the stored provenance. It should either:

- fail with a conflict report,
- write a patch bundle instead,
- or apply only non-conflicting changes if explicitly requested.

### Provenance Metadata

To make hoist/eject safe, each session should track origin metadata like:

```json
{
  "origin_kind": "git_working_tree",
  "source_path": "/repo",
  "head_commit": "abc123",
  "branch": "main",
  "remote_url": "git@github.com:org/repo.git",
  "pathspec": ["."],
  "included_untracked": true
}
```

Without this, eject and PR creation become guesswork.

## Runtime Model

### Core Runtime

Use `boa_engine` as the base embedding surface.

Important runtime implication: even though the chosen engine is pure Rust, each sandbox instance should still be treated as a logically serialized actor. Keep one runtime owner per session and do not let guest execution, module-cache mutation, or capability dispatch race across arbitrary tasks.

Recommended shape:

- one `SandboxActor` per session,
- one logical runtime owner per actor,
- message-passing API from the host app to the actor,
- explicit teardown that flushes the overlay if requested.

### Runtime Phases

1. Open or create overlay session volume.
2. Build capability manifest for this session.
3. Start the JS runtime backend with:
   - custom `ModuleLoader`,
   - bootstrapped JS libraries for FS shims, host APIs, and optional bash/npm helpers.
4. Load entrypoint module or evaluate code.
5. Persist tool activity and optionally flush durable state.

## Module Resolution and Loading

Implement a custom module loader with three distinct jobs:

1. Resolve specifiers
2. Prepare dependencies
3. Load module source plus source maps and code cache

Recommended specifier space:

- `terrace:/workspace/...`
  Purpose: first-class modules stored in the TerraceDB VFS.
- `terrace:host/<capability>`
  Purpose: host-provided libraries generated from capability manifests.
- `npm:<pkg>` or bare package imports
  Purpose: npm dependencies resolved by the sandbox package resolver.
- `node:<builtin>`
  Purpose: only in node compatibility mode.
- `data:` and `file:`:
  Purpose: optional, but if supported, `file:` should be virtualized to the sandbox root rather than mapped to host disk.

Recommended `ModuleLoader` behavior:

- `resolve()`
  - normalize `terrace:/` and relative imports against the VFS tree,
  - gate `node:` and `npm:` based on session config,
  - reject unsupported schemes early.
- `prepare_load()`
  - prefetch dependency graphs,
  - resolve npm packages,
  - transpile TS/TSX/MTS/CTS to JS,
  - collect source maps,
  - populate code cache lookup keys.
- `load()`
  - return final JS source for the guest runtime,
  - attach source-map metadata,
  - use VFS or shared cache volumes as backing storage.
- module-cache publication
  - persist module cache metadata or transpilation artifacts into `/.terrace/cache/runtime`.

## File System API Strategy

Do not try to perfectly emulate all of Node FS and Deno FS on day one.

Instead, define a minimal host op layer over `terracedb_vfs::VfsFileSystem`:

- `op_fs_read_file`
- `op_fs_write_file`
- `op_fs_pread`
- `op_fs_pwrite`
- `op_fs_stat`
- `op_fs_lstat`
- `op_fs_readdir`
- `op_fs_readlink`
- `op_fs_mkdir`
- `op_fs_rename`
- `op_fs_link`
- `op_fs_symlink`
- `op_fs_unlink`
- `op_fs_rmdir`
- `op_fs_truncate`
- `op_fs_fsync`

Then layer JS shims on top:

- `@terracedb/sandbox/fs`
  A small runtime-neutral library used by everything else.
- sandbox runtime/global shim
  Only the APIs you actually need.
- `node:fs/promises` shim
  Implemented in JS using the same op layer.
- `node:fs` sync API
  Defer or emulate only where strictly necessary; many packages will work with the promise API only.

This keeps the Rust layer small and lets compatibility evolve in JS.

## just-bash Integration

`just-bash` is a strong fit here because:

- it is explicitly designed for AI-agent use,
- it already supports a custom filesystem abstraction,
- its filesystem interface is async-only, which aligns well with `terracedb-vfs`,
- it supports custom commands.

Recommended integration:

1. Provide a guest-side adapter implementing `just-bash`'s `IFileSystem` on top of your `@terracedb/sandbox/fs` library.
2. Ship a built-in library such as `@terracedb/sandbox/bash`.
3. The library exposes a persistent `BashSession` wrapper that stores `cwd` and exported env between calls.

That wrapper matters because `just-bash` resets shell state between `exec()` calls while sharing the filesystem. Without a wrapper, the agent will not experience shell-like continuity.

Suggested guest API:

```ts
import { createBashSession } from "@terracedb/sandbox/bash";

const bash = await createBashSession();
await bash.exec("mkdir -p src");
await bash.exec("cd src && printf 'hello\\n' > note.txt");
```

Internally:

- each bash invocation records a `ToolRun` with params, stdout/stderr summary, and exit code,
- the adapter routes file operations to the same VFS as the JS runtime,
- custom commands can bridge to host capabilities like `npm install`, `tsc`, or app APIs.

### Recommended Bash Commands

Expose bash functionality as libraries and tool runs, not as host subprocesses.

Good candidates for built-in custom commands:

- `npm`
  Delegates to the host package installer service rather than spawning the real npm CLI.
- `tsc`
  Delegates to the TypeScript service.
- `terrace-call`
  Invokes allowlisted host capabilities for debugging and scripting.

## TypeScript Strategy

TypeScript needs two separate stories:

1. execution-time transpilation,
2. language-service and `tsc`-style tooling.

These should share a module graph and virtual tree, but they should not be the same implementation.

### Execution-Time TS

`boa_engine` executes JavaScript, not TypeScript, so TS execution is unambiguously the embedder's responsibility.

Recommended design:

- the module loader transpiles `.ts`, `.tsx`, `.mts`, and `.cts` to JS before guest execution,
- source maps are stored alongside the transpiled output,
- transpile cache keys include:
  - file content hash,
  - compiler target,
  - JSX mode,
  - module kind,
  - relevant package-resolution settings.

I would implement this transpilation in the host, not in guest JS. A Rust-side transpiler keeps runtime startup smaller and makes module loading deterministic.

### Type Checking and Language Service

Use `@typescript/vfs` for a virtual TypeScript environment, not as the canonical runtime filesystem.

Reason:

- TypeScript expects a `ts.System` and compiler host semantics,
- your VFS is async and versioned,
- `@typescript/vfs` already provides the right `Map`-backed abstractions for language-service operations.

Recommended design:

1. Build a `TypeScriptMirror` from a `VolumeSnapshot` or current overlay view.
2. Materialize a `Map<String, String>` of text files and `.d.ts` files.
3. Create:
   - `createSystem(fs_map)` for pure virtual mode, or
   - a custom FS-backed system if you later need hybrid resolution.
4. Run:
   - diagnostics,
   - completions,
   - quick info,
   - emits,
   - refactors,
   - import rewrites.
5. Write emitted files back to VFS only when explicitly requested.

Because the mirror is map-based, it can also provide the sync view needed by any compatibility shims that cannot tolerate async FS.

### Keeping TS in Sync

Do not rebuild the TypeScript mirror from scratch after every edit.

Instead:

- initialize from a snapshot,
- subscribe to VFS activity,
- apply incremental updates for:
  - file writes,
  - patches,
  - truncates,
  - renames,
  - deletes.

That gives you a fast long-lived TS service attached to the session.

## npm Dependency Strategy

This is the highest-risk part of the feature.

The key architectural point is:

- the chosen JS engine gives you the embedder runtime and module-loader hooks,
- broader npm/Node compatibility lives above that,
- full compatibility from scratch will be a large project.

### Recommended Scope Split

#### v1

Support:

- pure JS/TS packages,
- ESM-first packages,
- packages that only need the Node APIs you explicitly shim,
- installation through a host-managed package service.

Do not support yet:

- Node-API native addons,
- arbitrary postinstall scripts,
- packages that require a real host process model,
- packages that expect unrestricted host disk layout.

This is still a useful feature set for agent workflows.

#### v2

Add:

- broader `node:` built-ins,
- CommonJS,
- optional `node_modules` compatibility view,
- more of Deno/Node resolution semantics.

#### v3

Consider:

- allowlisted native modules,
- build-script execution in a more isolated sub-sandbox,
- registry mirrors and offline package sets.

### Installer Design

Do not make the real `npm install` CLI the source of truth.

Instead, implement a host service:

```rust
pub trait PackageInstaller {
    async fn install(
        &self,
        volume: Arc<dyn Volume>,
        reqs: &[PackageReq],
        opts: InstallOptions,
    ) -> Result<InstallReport, SandboxError>;
}
```

The installer should:

1. resolve versions,
2. fetch tarballs,
3. verify integrity,
4. unpack into a shared cache or session cache,
5. materialize whatever compatibility view your module loader expects,
6. update a Terrace-native install manifest.

For agent ergonomics, expose this as:

- a JS library: `sandbox.install(["zod", "lodash"])`,
- an optional bash `npm install` custom command that delegates to the same service.

### Package Storage Model

Use two levels of storage:

- shared immutable package cache
  - tarballs,
  - unpacked package contents by integrity hash,
  - shared TypeScript lib cache,
  - optionally another VFS volume or host-managed cache outside the session.
- session-local compatibility view
  - `package.json`,
  - install manifest,
  - symlinked or copied view at `/.terrace/npm/node_modules`,
  - lockfile-like metadata.

This avoids duplicating full packages into every overlay while still giving each session a writable dependency graph.

### Important Constraint

Deno's own docs note that some npm workflows still need a local `node_modules` directory, especially for CommonJS tools and Node-API addons. In an embedded TerraceDB sandbox, that should be treated as a compatibility mode, not the base model.

## Host API Injection

Host APIs should be explicit capability modules with both runtime code and types.

Recommended model:

```rust
pub trait SandboxCapability: Send + Sync {
    fn name(&self) -> &str;
    fn esm_specifier(&self) -> String;
    fn dts_text(&self) -> String;
    fn module_source(&self) -> String;
}
```

Each capability should ship:

- a JS/TS import surface,
- a `.d.ts` declaration surface,
- one or more ops,
- an allowlist entry in the session capability manifest.

Example guest usage:

```ts
import { tickets } from "terrace:host/tickets";

const items = await tickets.listOpen();
await tickets.addComment({ id: items[0].id, body: "Investigating" });
```

### Design Rules for Host APIs

1. Prefer imports over globals.
2. Prefer small, domain-specific modules over a giant `Terrace` object.
3. Prefer async methods returning JSON-serializable data.
4. Version modules explicitly.
5. Record calls as tool runs or capability events.
6. Make write APIs idempotent where practical.

### Type Injection

There are two good options:

- generate `.d.ts` files into the TypeScript mirror and expose the matching runtime module through the loader,
- or provide virtual modules whose source contains both the runtime wrapper and an adjacent declaration mapping.

Either is fine. The key is that types and runtime exports come from the same capability registration.

## Permissions and Safety

Recommended default policy:

- no host filesystem access,
- no host process spawning,
- no FFI,
- no network except explicit allowlist,
- no direct DB access except the VFS/capability surface,
- no ambient app globals.

Capability categories:

- `fs`
  Backed by `terracedb-vfs`.
- `npm`
  Backed by the installer.
- `bash`
  Backed by `just-bash`.
- `typescript`
  Backed by the TS service.
- `app:<name>`
  Backed by host capability modules.
- `net`
  Optional and strongly allowlisted.

Also enforce:

- execution timeout,
- memory limit,
- maximum pending async ops,
- maximum package graph size,
- maximum emitted file size,
- maximum tool-run payload size.

## Observability

Use existing VFS primitives rather than inventing a second telemetry plane.

### Record as Tool Runs

- `bash.exec`
- `npm.install`
- `tsc.check`
- `tsc.emit`
- `host_api.<capability>.<method>`

### Record as VFS Activity

The existing activity log already captures:

- file mutations,
- KV mutations,
- tool-run lifecycle,
- overlay creation.

That gives you a replayable sandbox timeline almost for free.

### Optional Session Metadata

Store lightweight metadata in VFS KV:

- active capability manifest,
- current package manifest hash,
- runtime options,
- last successful type-check,
- last entrypoint,
- hoist provenance,
- user-visible summary state.

## Git and Pull Requests

If you want easy PR creation, treat git as another first-class subsystem of the sandbox.

### Recommendation

Do not make PR creation operate by mutating the original checkout in place.

Preferred flow:

1. Hoist repo into sandbox and record provenance.
2. Import that repo as a VFS-native repository image and run agent work in the overlay.
3. When creating a PR:
   - open the VFS-native repo through `terracedb-git`,
   - create/update the target branch and commit changes in-process,
   - push through the configured git bridge,
   - open a PR through a provider adapter.

This keeps git semantics inside the sandbox/VFS boundary while still avoiding writes into the user's active checkout and preserving a clean audit trail.

### Git-Aware API

Suggested API:

```rust
pub struct PullRequestRequest {
    pub title: String,
    pub body: String,
    pub branch_name: String,
    pub base_branch: Option<String>,
    pub provider: PullRequestProvider,
    pub push_remote: Option<String>,
}
```

Back it with traits like:

```rust
pub trait GitRepositoryStore {
    async fn open(
        &self,
        image: Arc<dyn GitRepositoryImage>,
        request: GitOpenRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<Arc<dyn GitRepository>, GitSubstrateError>;
}

pub trait GitHostBridge {
    async fn push(
        &self,
        repository: Arc<dyn GitRepository>,
        request: GitPushRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPushReport, GitSubstrateError>;

    async fn create_pull_request(
        &self,
        request: GitPullRequestRequest,
        cancellation: Arc<dyn GitCancellationToken>,
    ) -> Result<GitPullRequestReport, GitSubstrateError>;
}

pub trait PullRequestProviderClient {
    async fn create_pull_request(
        &self,
        input: ProviderPullRequestInput,
    ) -> Result<ProviderPullRequestOutput, SandboxError>;
}
```

### PR Export Flow

Recommended sequence:

1. Validate the session has git provenance.
2. Open the imported VFS-native repo image through `terracedb-git`.
3. Run optional validation commands over the sandbox/VFS worktree.
4. Update or create the target branch ref inside the VFS-native repo.
5. Commit the current worktree through `terracedb-git`.
6. Push through the configured git bridge.
9. Create PR via provider client.

Return:

- branch name,
- commit SHA,
- compare URL,
- PR URL,
- conflict warnings if any files were skipped or rewritten.

### Why Worktrees Are the Right Default

Using a fresh worktree gives you:

- no accidental edits to the user's active checkout,
- deterministic PR generation from the recorded base,
- easier cleanup,
- better concurrency when multiple sandbox sessions are active.

### Manual Export Is Still Useful

Not every flow should create a PR directly. Also support:

- `export_patch()`
  Produce a patch against the hoisted base.
- `eject_to_disk()`
  Write to a target path for manual review.
- `commit_to_existing_checkout()`
  Only as an explicit opt-in mode.

## Testing Strategy

This repo already values deterministic simulation and in-process libraries. `terracedb-sandbox` should follow the same pattern.

### Core Test Layers

1. Unit tests
   - path normalization,
   - specifier resolution,
   - permission checks,
   - transpile cache keys,
   - package-manifest updates.
2. Integration tests
   - execute guest code against overlay volume,
   - verify writes land in session overlay,
   - verify snapshots behave correctly,
   - verify tool runs are recorded.
3. Compatibility tests
   - import simple npm packages,
   - use `just-bash` over the VFS adapter,
   - run TS diagnostics and emits.
4. Failure tests
   - package fetch interruption,
   - denied capability,
   - bad module graph,
   - corrupt cache entry,
   - flush/reopen/recovery behavior,
   - hoist/eject conflicts,
   - PR export failure and retry behavior.

### High-Value End-to-End Tests

- guest writes files, runs bash, installs package, imports it, then flushes and reopens;
- TS diagnostics update after incremental file edits;
- overlay isolates guest mutations from base volume;
- denied host capability produces a stable guest-visible error;
- cached transpiled/code-cache artifacts are reused across session reopen;
- repo hoisted from disk can be ejected back with correct rename/delete handling;
- sandbox delta can be turned into a clean PR from an ephemeral worktree.

## Recommended Implementation Order

### Phase 1: Session + Runtime + VFS

Build:

- `SandboxSession`,
- `SandboxActor`,
- minimal `ModuleLoader`,
- minimal FS ops and JS shim,
- `terrace:/workspace` entrypoint loading.

Skip:

- npm,
- `just-bash`,
- TypeScript checking,
- node compatibility.

Goal:

Guest JS can run against TerraceDB VFS and read/write files safely.

### Phase 2: Hoist/Eject + Git Provenance

Build:

- host directory hoist,
- repo hoist modes,
- provenance metadata,
- snapshot and delta eject,
- conflict detection.

Goal:

A real repo can move into and out of the sandbox safely.

### Phase 3: TypeScript Execution + TS Service

Build:

- TS transpilation during module load,
- source maps,
- `@typescript/vfs` mirror,
- `check_types()` and `emit()` service.

Goal:

Guest TS runs, and host/agent can request diagnostics and emits.

### Phase 4: just-bash

Build:

- guest `IFileSystem` adapter,
- `@terracedb/sandbox/bash`,
- persistent shell-session wrapper,
- tool-run recording for bash invocations.

Goal:

Agent can use rich shell utilities without host-native binaries.

### Phase 5: npm v1

Build:

- package installer service,
- shared package cache,
- session compatibility view,
- `npm:` or bare-import resolution for pure JS packages.

Goal:

Agent can install and import useful JS packages.

### Phase 6: PR Creation

Build:

- ephemeral worktree export,
- commit/push pipeline,
- provider adapters for PR creation,
- patch export fallback.

Goal:

Sandboxed changes can become a branch or PR with one operation.

### Phase 7: Node Compatibility Expansion

Build:

- selected `node:` built-ins,
- CommonJS,
- optional `node_modules` mode,
- more package edge cases.

Goal:

Broader npm ecosystem support without abandoning the embedded model.

## Hard Calls I Would Make Up Front

1. Keep the authoritative filesystem in `terracedb-vfs`; every other subsystem mirrors or adapts it.
2. Make package installation a host service, not a literal embedded npm CLI.
3. Treat native addons as unsupported in v1.
4. Expose host APIs as explicit importable capabilities.
5. Record all nontrivial side-effecting operations through `ToolRunStore`.
6. Pin each sandbox runtime to its own thread/current-thread executor.
7. Use git provenance plus ephemeral worktrees as the default PR path.

## Short Version

If we compress this down to one sentence:

`terracedb-sandbox` should be a `boa_engine`-embedded, capability-oriented JS runtime whose root filesystem is a `terracedb-vfs` overlay, whose disk and git interop are explicit hoist/eject services with provenance, whose shell tooling comes from `just-bash` via a VFS adapter, whose TypeScript tooling comes from a separate `@typescript/vfs` mirror, whose npm support is delivered by a host-managed installer plus a custom module loader, and whose default PR flow exports into an ephemeral git worktree rather than mutating the user's checkout directly.
