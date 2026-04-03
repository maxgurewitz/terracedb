use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

#[tokio::test]
async fn node_commonjs_private_field_mixins_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-mixins-"));
      const write = (file, body) => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, body);
      };

      write(path.join(root, "pkg/lib/index.cjs"), `
        module.exports = require('./arborist/index.cjs');
        module.exports.Arborist = module.exports;
      `);

      write(path.join(root, "pkg/lib/tracker.cjs"), `
        module.exports = cls => class Tracker extends cls {
          #progress = new Map();
          addTracker(key) {
            this.#progress.set(key, true);
            return this.#progress.size;
          }
        };
      `);

      write(path.join(root, "pkg/lib/arborist/load-virtual.cjs"), `
        module.exports = cls => class VirtualLoader extends cls {
          #rootOptionProvided = false;
          loadVirtual() { return this.#rootOptionProvided; }
        };
      `);

      write(path.join(root, "pkg/lib/arborist/rebuild.cjs"), `
        module.exports = cls => class Builder extends cls {
          #queues = { install: [] };
          queueLength() { return this.#queues.install.length; }
        };
      `);

      write(path.join(root, "pkg/lib/arborist/index.cjs"), `
        const mixins = [
          require('../tracker.cjs'),
          require('./load-virtual.cjs'),
          require('./rebuild.cjs'),
        ];
        const Base = mixins.reduce((a, b) => b(a), require('node:events'));
        class Arborist extends Base {
          constructor() {
            super();
            this.kind = 'ok';
          }
        }
        module.exports = Arborist;
      `);

      const Arborist = require(path.join(root, "pkg/lib/index.cjs"));
      const arb = new Arborist();
      console.log(JSON.stringify({
        ctor: arb.constructor.name,
        kind: arb.kind,
        trackerSize: arb.addTracker("x"),
        virtual: arb.loadVirtual(),
        queueLength: arb.queueLength(),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox commonjs mixin repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node commonjs mixin repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_commonjs_private_method_forward_refs_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-private-forward-"));
      const write = (file, body) => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, body);
      };

      write(path.join(root, "pkg/index.cjs"), `
        module.exports = cls => class ForwardPrivateRefs extends cls {
          #values = new Map();
          #seen = new Set();

          async run(value) {
            this.#values.set(value, true);
            return this.#compute(value);
          }

          async #compute(value) {
            this.#seen.add(value);
            return this.#finalize(value);
          }

          #finalize(value) {
            return {
              hasValue: this.#values.has(value),
              seen: this.#seen.has(value),
            };
          }
        };
      `);

      const ForwardPrivateRefs = require(path.join(root, "pkg/index.cjs"))(require("node:events"));
      const instance = new ForwardPrivateRefs();
      instance.run("x").then((value) => console.log(JSON.stringify(value)));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox forward private refs repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node forward private refs repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_commonjs_isolated_reifier_skeleton_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-isolated-reifier-"));
      const write = (file, body) => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, body);
      };

      write(path.join(root, "pkg/index.cjs"), `
        module.exports = cls => class IsolatedReifier extends cls {
          #externalProxies = new Map();
          #omit = new Set();
          #rootDeclaredDeps = new Set();
          #processedEdges = new Set();
          #workspaceProxies = new Map();

          async makeIdealGraph() {
            this.#omit = new Set(["dev"]);
            return this.#workspaceProxy("root");
          }

          async #workspaceProxy(node) {
            if (this.#workspaceProxies.has(node)) {
              return this.#workspaceProxies.get(node);
            }
            const result = { node };
            this.#workspaceProxies.set(node, result);
            await this.#assignCommonProperties(node, result);
            return result;
          }

          async #externalProxy(node) {
            if (this.#externalProxies.has(node)) {
              return this.#externalProxies.get(node);
            }
            const result = { node };
            this.#externalProxies.set(node, result);
            await this.#assignCommonProperties(node, result);
            return result;
          }

          async #assignCommonProperties(node, result) {
            result.rootDeclared = this.#rootDeclaredDeps.size;
            result.processed = this.#processedEdges.size;
            result.omit = Array.from(this.#omit);
            result.node = node;
            return result;
          }
        };
      `);

      const IsolatedReifier = require(path.join(root, "pkg/index.cjs"))(require("node:events"));
      const instance = new IsolatedReifier();
      instance.makeIdealGraph().then((value) => console.log(JSON.stringify(value)));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox isolated reifier skeleton repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node isolated reifier skeleton repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_commonjs_private_methods_inside_arrow_callbacks_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-private-arrow-"));
      const write = (file, body) => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, body);
      };

      write(path.join(root, "pkg/index.cjs"), `
        module.exports = cls => class PrivateArrowRefs extends cls {
          #values = new Map();

          async run() {
            return Promise.all(Array.from(["a", "b"], value => this.#proxy(value)));
          }

          async #proxy(value) {
            this.#values.set(value, true);
            return this.#values.has(value);
          }
        };
      `);

      const PrivateArrowRefs = require(path.join(root, "pkg/index.cjs"))(require("node:events"));
      const instance = new PrivateArrowRefs();
      instance.run().then((value) => console.log(JSON.stringify(value)));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox private arrow refs repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node private arrow refs repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_commonjs_nested_private_method_chain_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const fs = require("node:fs");
      const os = require("node:os");
      const path = require("node:path");

      const root = fs.mkdtempSync(path.join(os.tmpdir(), "terrace-private-nested-"));
      const write = (file, body) => {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.writeFileSync(file, body);
      };

      write(path.join(root, "pkg/index.cjs"), `
        module.exports = cls => class NestedPrivateRefs extends cls {
          #seen = new Set();

          createTree(values) {
            this.#processEdges(values);
            return Array.from(this.#seen);
          }

          #processEdges(values) {
            for (const value of values) {
              this.#processDep(value);
            }
          }

          #processDep(value) {
            this.#seen.add(value);
          }
        };
      `);

      const NestedPrivateRefs = require(path.join(root, "pkg/index.cjs"))(require("node:events"));
      const instance = new NestedPrivateRefs();
      console.log(JSON.stringify(instance.createTree(["a", "b"])));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox nested private refs repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node nested private refs repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}

#[tokio::test]
async fn node_function_constructor_isolated_reifier_shape_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r#"
      const getKey = value => value;
      class IsolatedNode {
        constructor(init = {}) {
          Object.assign(this, init);
          this.children = new Map();
          this.inventory = new Map();
          this.edgesOut = new Map();
          this.edgesIn = new Set();
          this.linksIn = new Set();
          this.fsChildren = new Set();
          this.workspaces = new Map();
          this.binPaths = [];
        }
      }
      class IsolatedLink extends IsolatedNode {}
      const mixinExpression = String.raw`(cls) => class IsolatedReifier extends cls {
        #externalProxies = new Map()
        #omit = new Set()
        #rootDeclaredDeps = new Set()
        #processedEdges = new Set()
        #workspaceProxies = new Map()

        #generateChild (node, location, pkg, isInStore, root) {
          const newChild = new IsolatedNode({
            isInStore,
            location,
            name: node.packageName || node.name,
            optional: node.optional,
            package: pkg,
            parent: root,
            path: location,
            resolved: node.resolved,
            root,
          })
          newChild.top = { path: '/' }
          root.children.set(newChild.location, newChild)
          root.inventory.set(newChild.location, newChild)
        }

        async makeIdealGraph () {
          this.idealGraph = {
            external: [],
            workspaces: [],
          }
        }

        async #workspaceProxy (node) {
          return node
        }

        async #externalProxy (node) {
          return node
        }

        async #assignCommonProperties (_node, result, _populateDeps = true) {
          return result
        }

        async #createBundledTree () {
          return { nodes: new Map(), edges: [] }
        }

        async createIsolatedTree () {
          await this.makeIdealGraph()
          const bundledTree = await this.#createBundledTree()

          const root = new IsolatedNode(this.idealGraph)
          root.root = root
          root.inventory.set('', root)
          const processed = new Set()
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
              location: isDeclared ? wsName : c.localLocation,
              name: wsName,
              package: workspace.package,
              parent: root,
              path: isDeclared ? wsName : c.localLocation,
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

          this.idealGraph.external.forEach(c => {
            const key = getKey(c)
            if (processed.has(key)) {
              return
            }
            processed.add(key)
            const location = key
            this.#generateChild(c, location, c.package, true, root)
          })

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

          this.#processEdges(this.idealGraph, false, root)
          for (const node of this.idealGraph.workspaces) {
            this.#processEdges(node, false, root)
          }
          return root
        }

        #processEdges (node, externalEdge, root) {
          const key = getKey(node)
          if (this.#processedEdges.has(key)) {
            return
          }
          this.#processedEdges.add(key)

          let from, nmFolder
          if (externalEdge) {
            const fromLocation = key
            from = root.children.get(fromLocation)
            nmFolder = key
          } else {
            from = node.isProjectRoot ? root : root.inventory.get(node.localLocation)
            nmFolder = node.localLocation
          }
          if (!from) {
            return
          }

          for (const dep of node.localDependencies || []) {
            this.#processEdges(dep, false, root)
            this.#processDeps(dep, false, false, root, from, nmFolder)
          }
          for (const dep of node.externalDependencies || []) {
            this.#processEdges(dep, true, root)
            this.#processDeps(dep, false, true, root, from, nmFolder)
          }
          for (const dep of node.externalOptionalDependencies || []) {
            this.#processEdges(dep, true, root)
            this.#processDeps(dep, true, true, root, from, nmFolder)
          }
        }

        #processDeps (dep, _optional, _external, _root, _from, _nmFolder) {
          return dep
        }
      }`;

      const evaluate = (mode) => {
        try {
          const mixin = mode === "eval"
            ? eval(mixinExpression)
            : Function(`return ${mixinExpression};`)();
          const Mixed = mixin(require("node:events"));
          return { ok: true, name: Mixed.name };
        } catch (error) {
          return {
            ok: false,
            error: String(error && (error.stack || error.message || error)),
          };
        }
      };

      console.log(JSON.stringify({
        eval: evaluate("eval"),
        functionCtor: evaluate("function"),
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox function constructor repro"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node function constructor repro")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
