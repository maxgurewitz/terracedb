#[path = "support/npm_cli.rs"]
mod npm_cli;

#[path = "support/node_compat.rs"]
mod node_compat_support;

use serde_json::Value;
use std::path::Path;

fn isolated_reifier_source() -> Option<String> {
    let path = Path::new(
        "/Users/maxwellgurewitz/dev/cli/workspaces/arborist/lib/arborist/isolated-reifier.js",
    );
    std::fs::read_to_string(path).ok()
}

fn replace_method_body(source: &str, signature: &str, body: &str) -> String {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing method signature: {signature}"));
    let body_start = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .expect("method body start");
    let mut depth = 0usize;
    let mut end = None;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(body_start + offset);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end.expect("method body end");
    let mut rewritten = String::with_capacity(source.len() + body.len());
    rewritten.push_str(&source[..=body_start]);
    rewritten.push('\n');
    rewritten.push_str(body);
    if !body.ends_with('\n') {
        rewritten.push('\n');
    }
    rewritten.push_str(&source[end..]);
    rewritten
}

fn replace_exact(source: &str, old: &str, new: &str) -> String {
    assert!(
        source.contains(old),
        "expected source to contain exact block:\n{old}"
    );
    source.replacen(old, new, 1)
}

async fn run_isolated_reifier_variant(source: &str) -> Option<(Value, Value)> {
    if !node_compat_support::real_node_available() {
        return None;
    }

    let source = serde_json::to_string(source).expect("serialize source");
    let script = format!(
        r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-isolated-reifier-runtime-"));
      const write = (file, body) => {{
        fs.mkdirSync(path.dirname(file), {{ recursive: true }});
        fs.writeFileSync(file, body);
      }};

      write(path.join(root, "pkg/isolated-reifier.cjs"), {source});
      write(path.join(root, "isolated-classes.js"), `
        class IsolatedNode {{
          constructor(init = {{}}) {{
            Object.assign(this, init);
            this.children = new Map();
            this.inventory = new Map();
            this.edgesOut = new Map();
            this.edgesIn = new Set();
            this.linksIn = new Set();
            this.fsChildren = new Set();
            this.workspaces = new Map();
            this.binPaths = [];
          }}
        }}
        class IsolatedLink extends IsolatedNode {{}}
        module.exports = {{ IsolatedNode, IsolatedLink }};
      `);
      write(path.join(root, "pkg/node_modules/pacote/index.js"), `
        module.exports = {{
          extract: async () => undefined,
        }};
      `);
      write(path.join(root, "pkg/node_modules/treeverse/index.js"), `
        module.exports = {{
          depth: () => undefined,
        }};
      `);

      const mixin = require(path.join(root, "pkg/isolated-reifier.cjs"));
      try {{
        const Mixed = mixin(require("node:events"));
        console.log(JSON.stringify({{ ok: true, name: Mixed.name }}));
      }} catch (error) {{
        console.log(JSON.stringify({{
          ok: false,
          error: String(error && (error.stack || error.message || error)),
        }}));
      }}
    "#
    );

    let sandbox = {
        let result = node_compat_support::exec_node_fixture(&script)
            .await
            .expect("sandbox isolated reifier variant");
        let report = result.result.expect("sandbox report");
        serde_json::from_str(report["stdout"].as_str().expect("sandbox stdout").trim())
            .expect("sandbox stdout json")
    };
    let real = serde_json::from_str(
        &node_compat_support::exec_real_node_eval(&script)
            .expect("real node isolated reifier variant")
            .stdout,
    )
    .expect("real stdout json");
    Some((sandbox, real))
}

async fn assert_create_isolated_tree_variant_matches_real_node(
    base_source: &str,
    body: &str,
) -> Option<(Value, Value)> {
    let rewritten = replace_method_body(base_source, "  async createIsolatedTree () {", body);
    run_isolated_reifier_variant(&rewritten).await
}

#[tokio::test]
async fn npm_arborist_mixin_chain_builds_over_node_events() {
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(430, 97).await else {
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/arborist-mixins.cjs",
        r#"
        const mixinPaths = [
          "/workspace/npm/node_modules/@npmcli/arborist/lib/tracker.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/build-ideal-tree.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/load-actual.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/load-virtual.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/rebuild.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/reify.js",
          "/workspace/npm/node_modules/@npmcli/arborist/lib/arborist/isolated-reifier.js",
        ];
        const results = [];
        let base = require("node:events");
        for (const mixinPath of mixinPaths) {
          const mixin = require(mixinPath);
          try {
            base = mixin(base);
            results.push({
              mixinPath,
              ok: true,
              name: base.name,
              protoSample: Object.getOwnPropertyNames(base.prototype).slice(0, 8),
            });
          } catch (error) {
            results.push({
              mixinPath,
              ok: false,
              error: String(error && (error.stack || error.message || error)),
            });
            break;
          }
        }
        console.log(JSON.stringify(results));
        "#,
        &[],
    )
    .await
    .expect("arborist mixin chain should execute");

    let report = result.result.clone().expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let results: serde_json::Value = serde_json::from_str(stdout.trim()).expect("stdout json");
    let failures = results
        .as_array()
        .expect("results array")
        .iter()
        .filter(|entry| entry["ok"] != serde_json::Value::Bool(true))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "expected all arborist mixins to compose over node:events, got failures: {failures:#?}\ntrace: {:#?}",
        npm_cli::node_runtime_trace(&result),
    );
}

#[tokio::test]
async fn npm_isolated_reifier_stubbed_create_isolated_tree_matches_real_node() {
    let Some(source) = isolated_reifier_source() else {
        return;
    };
    let Some((sandbox, real)) =
        assert_create_isolated_tree_variant_matches_real_node(&source, "    return null").await
    else {
        return;
    };

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn npm_isolated_reifier_create_isolated_tree_statement_reductions_match_real_node() {
    let Some(source) = isolated_reifier_source() else {
        return;
    };

    let variants = [
        (
            "await makeIdealGraph",
            r#"
    await this.makeIdealGraph()
    return null
"#,
        ),
        (
            "await bundled tree",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    return bundledTree
"#,
        ),
        (
            "root construction",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    const root = new IsolatedNode(this.idealGraph)
    root.root = root
    root.inventory.set('', root)
    return { bundledTree, name: root.name ?? null }
"#,
        ),
        (
            "workspace loop skeleton",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    const root = new IsolatedNode(this.idealGraph)
    root.root = root
    root.inventory.set('', root)
    for (const c of this.idealGraph.workspaces) {
      const wsName = c.packageName
      const workspace = new IsolatedNode({
        location: c.localLocation,
        name: wsName,
        package: c.package,
        path: c.localPath,
        resolved: c.resolved,
      })
      root.fsChildren.add(workspace)
      root.inventory.set(workspace.location, workspace)
      root.workspaces.set(wsName, workspace.path)
      const isDeclared = this.#rootDeclaredDeps.has(wsName)
      const wsLink = new IsolatedLink({
        location: isDeclared ? join('node_modules', wsName) : join(c.localLocation, 'node_modules', wsName),
        name: wsName,
        package: workspace.package,
        parent: root,
        path: isDeclared ? join(root.path, 'node_modules', wsName) : join(root.path, c.localLocation, 'node_modules', wsName),
        realpath: workspace.path,
        root,
        target: workspace,
      })
      if (!isDeclared) {
        workspace.children.set(wsName, wsLink)
      }
      root.children.set(wsName, wsLink)
      root.inventory.set(wsLink.location, wsLink)
      workspace.linksIn.add(wsLink)
    }
    return { bundledTree, workspaces: root.fsChildren.size }
"#,
        ),
        (
            "external proxy loop",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    const root = new IsolatedNode(this.idealGraph)
    root.root = root
    root.inventory.set('', root)
    const processed = new Set()
    this.idealGraph.external.forEach(c => {
      const key = getKey(c)
      if (processed.has(key)) {
        return
      }
      processed.add(key)
      const location = join('node_modules', '.store', key, 'node_modules', c.packageName)
      this.#generateChild(c, location, c.package, true, root)
    })
    return { bundledTree, processed: processed.size }
"#,
        ),
        (
            "bundled tree projection",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    const root = new IsolatedNode(this.idealGraph)
    root.root = root
    root.inventory.set('', root)
    bundledTree.nodes.forEach(node => {
      this.#generateChild(node, node.location, node.pkg, false, root)
    })
    bundledTree.edges.forEach(edge => {
      const from = edge.from === 'root' ? root : root.inventory.get(edge.from)
      const to = root.inventory.get(edge.to)
      const newEdge = { optional: false, from, to }
      from.edgesOut.set(to.name, newEdge)
      to.edgesIn.add(newEdge)
    })
    return root
"#,
        ),
        (
            "process edges tail",
            r#"
    await this.makeIdealGraph()
    const bundledTree = await this.#createBundledTree()
    const root = new IsolatedNode(this.idealGraph)
    root.root = root
    root.inventory.set('', root)
    this.#processEdges(this.idealGraph, false, root)
    for (const node of this.idealGraph.workspaces) {
      this.#processEdges(node, false, root)
    }
    return { bundledTree, root }
"#,
        ),
    ];

    for (label, body) in variants {
        let Some((sandbox, real)) =
            assert_create_isolated_tree_variant_matches_real_node(&source, body).await
        else {
            return;
        };
        assert_eq!(
            sandbox, real,
            "createIsolatedTree reduction diverged for {label}"
        );
    }
}

#[tokio::test]
async fn npm_isolated_reifier_full_body_minus_one_block_reductions_match_real_node() {
    let Some(source) = isolated_reifier_source() else {
        return;
    };

    let variants = [
        (
            "without workspace loop",
            replace_exact(
                &source,
                r#"    for (const c of this.idealGraph.workspaces) {
      const wsName = c.packageName
      // XXX parent? root?
      const workspace = new IsolatedNode({
        location: c.localLocation,
        name: wsName,
        package: c.package,
        path: c.localPath,
        resolved: c.resolved,
      })
      root.fsChildren.add(workspace)
      root.inventory.set(workspace.location, workspace)
      root.workspaces.set(wsName, workspace.path)

      // Create workspace Link. For root declared deps, link at root node_modules/. For undeclared deps, link at the workspace's own node_modules/ (self-link).
      const isDeclared = this.#rootDeclaredDeps.has(wsName)
      const wsLink = new IsolatedLink({
        location: isDeclared ? join('node_modules', wsName) : join(c.localLocation, 'node_modules', wsName),
        name: wsName,
        package: workspace.package,
        parent: root,
        path: isDeclared ? join(root.path, 'node_modules', wsName) : join(root.path, c.localLocation, 'node_modules', wsName),
        realpath: workspace.path,
        root,
        target: workspace,
      })
      if (!isDeclared) {
        workspace.children.set(wsName, wsLink)
      }
      root.children.set(wsName, wsLink)
      root.inventory.set(wsLink.location, wsLink)
      workspace.linksIn.add(wsLink)
    }
"#,
                "",
            ),
        ),
        (
            "without external loop",
            replace_exact(
                &source,
                r#"    this.idealGraph.external.forEach(c => {
      const key = getKey(c)
      if (processed.has(key)) {
        return
      }
      processed.add(key)
      const location = join('node_modules', '.store', key, 'node_modules', c.packageName)
      this.#generateChild(c, location, c.package, true, root)
    })
"#,
                "",
            ),
        ),
        (
            "without bundled projection",
            replace_exact(
                &source,
                r#"    bundledTree.nodes.forEach(node => {
      this.#generateChild(node, node.location, node.pkg, false, root)
    })

    bundledTree.edges.forEach(edge => {
      const from = edge.from === 'root' ? root : root.inventory.get(edge.from)
      const to = root.inventory.get(edge.to)
      // Maybe optional should be propagated from the original edge
      const newEdge = { optional: false, from, to }
      from.edgesOut.set(to.name, newEdge)
      to.edgesIn.add(newEdge)
    })
"#,
                "",
            ),
        ),
        (
            "without process edges tail",
            replace_exact(
                &source,
                r#"    this.#processEdges(this.idealGraph, false, root)
    for (const node of this.idealGraph.workspaces) {
      this.#processEdges(node, false, root)
    }
"#,
                "",
            ),
        ),
    ];

    for (label, variant_source) in variants {
        let Some((sandbox, real)) = run_isolated_reifier_variant(&variant_source).await else {
            return;
        };
        assert_eq!(sandbox, real, "full-body reduction diverged for {label}");
    }
}

#[tokio::test]
async fn npm_isolated_reifier_other_method_stub_reductions_match_real_node() {
    let Some(source) = isolated_reifier_source() else {
        return;
    };

    let variants = [
        (
            "stub assignCommonProperties",
            replace_method_body(
                &source,
                "  async #assignCommonProperties (node, result, populateDeps = true) {",
                "    return result",
            ),
        ),
        (
            "stub createBundledTree",
            replace_method_body(
                &source,
                "  async #createBundledTree () {",
                "    return { edges: [], nodes: new Map() }",
            ),
        ),
        (
            "stub processEdges",
            replace_method_body(
                &source,
                "  #processEdges (node, externalEdge, root) {",
                "    return root",
            ),
        ),
        (
            "stub processDeps",
            replace_method_body(
                &source,
                "  #processDeps (dep, optional, external, root, from, nmFolder) {",
                "    return dep",
            ),
        ),
        (
            "stub externalProxy",
            replace_method_body(
                &source,
                "  async #externalProxy (node) {",
                "    return node",
            ),
        ),
    ];

    for (label, variant_source) in variants {
        let Some((sandbox, real)) = run_isolated_reifier_variant(&variant_source).await else {
            return;
        };
        assert_eq!(sandbox, real, "method-stub reduction diverged for {label}");
    }
}
