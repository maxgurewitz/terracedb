# terracedb-js

`terracedb-js` is TerraceDB's JavaScript substrate crate.

It provides:

- the scheduler-facing JavaScript runtime boundary used by TerraceDB
- Terrace-owned module loading, host service, clock, entropy, and scheduler seams
- the in-progress runtime rewrite described in [docs/JS_ENGINE_REPLACEMENT_PROPOSAL.md](/Users/maxwellgurewitz/.codex/worktrees/fc5a/terracedb/docs/JS_ENGINE_REPLACEMENT_PROPOSAL.md)

## Attribution

This crate depends on and is developed with reference to the [Boa](https://github.com/boa-dev/boa) project.

TerraceDB currently uses Boa crates including:

- `boa_ast`
- `boa_engine`
- `boa_gc`
- `boa_interner`
- `boa_parser`

The ongoing runtime rewrite in `terracedb-js` also uses Boa source code and documentation as implementation reference material, especially for parser, compiler, VM, and module-system structure.

Boa is licensed under the MIT License. See:

- `~/dev/boa/README.md`
- `~/dev/boa/LICENSE-MIT`

When copying or adapting any Boa source into TerraceDB, the original Boa copyright and license notices must be preserved.
