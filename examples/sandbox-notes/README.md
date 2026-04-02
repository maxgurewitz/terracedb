# Sandbox Notes Example

This example is a small host application plus a companion sandboxed project tree that shows what `terracedb-sandbox` is for in practice:

- start a draft session from the named `notes-review` preset plus the `foreground` profile
- hoist a real project directory into a sandbox session
- inject an explicit app capability at `terrace:host/notes`
- execute guest code that reads and updates host-side state
- run TypeScript and bash tooling over the sandbox tree
- expose generated files through the read-only editor-view protocol
- export the sandboxed result back to disk or a git worktree

The host-side Rust code lives in [`src/lib.rs`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/src/lib.rs) and [`src/app.rs`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/src/app.rs). The hoisted project tree lives under [`project-template/`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/project-template).

## Run It

Run the deterministic local demo:

```bash
cargo run -p terracedb-example-sandbox-notes
```

The demo will:

1. create an in-memory sandbox store
2. hoist the companion project from disk into `/workspace`
3. install `lodash` and `zod`
4. check and emit the TypeScript helper in `src/render.ts`
5. execute `src/review.js`, which imports the injected async `terrace:host/notes` capability
6. create `docs/review-notes.md` through the bash adapter
7. open a read-only view for `/workspace/generated`
8. export the resulting snapshot to a temp directory

## Companion Project

The companion tree is intentionally small:

- [`project-template/src/review.js`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/project-template/src/review.js) is the guest entrypoint
- [`project-template/src/render.ts`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/project-template/src/render.ts) is the TypeScript file checked and emitted through the sandbox tooling APIs
- [`project-template/inbox.json`](/Users/maxwellgurewitz/dev/terracedb/examples/sandbox-notes/project-template/inbox.json) is read through the sandbox filesystem shim

`review.js` awaits the sandbox FS helpers and host notes capability, validates the payload with `zod`, derives slugs with `lodash`, writes `generated/triage-summary.json`, and appends a host-side comment back to the note store.

## Boundaries

What is simulated versus real:

- The default runnable demo uses deterministic in-memory runtime, package, readonly-view, and PR providers so the flow is teachable and repeatable.
- The example tests also cover real host-disk hoist/eject and real git worktree export using `HostGitWorkspaceManager`.
- The simulation suite seeds the same project directly into the VFS store so capability and session semantics replay under the seeded harness even when disk-backed hoist is not involved.

Why the editor view is read-only:

- The editor bridge is intentionally browse-only so file inspection does not bypass tool-run history, provenance checks, or copy-on-write session boundaries.
- Writable edits belong inside the sandbox APIs, not through a side channel in VS Code or Cursor.

How capabilities are injected:

- The app resolves a named preset/profile into an expanded capability manifest, then materializes the matching sandbox capability modules for `terrace:host/notes`.
- Guest code must import that module explicitly; there are no ambient globals.
- The same note-store implementation is exercised both through direct `invoke_capability` calls and through guest JS imports in the tests.

How export and PR creation work:

- Directory export uses `eject_to_disk` with `MaterializeSnapshot`.
- Git-backed export uses `create_pull_request`, which prepares an isolated worktree, applies the sandbox delta, commits it, and pushes the branch when the hoisted repo has a configured `origin`.

## Verification

The example is covered by:

- a happy-path test for hoist, package install, TypeScript emit, guest execution, bash, readonly view, and snapshot export
- a capability-equivalence test comparing direct API calls with guest-module calls
- local and remote readonly-view smoke tests using the same protocol bridge
- a git-backed PR/export test over a real temporary repo and bare remote
- a deterministic simulation replay test for the example capability and session semantics
