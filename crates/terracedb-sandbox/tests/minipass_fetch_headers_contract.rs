use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;
#[path = "support/npm_cli.rs"]
mod npm_cli;

fn parse_stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout.trim()).expect("stdout json")
}

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    parse_stdout_json(report["stdout"].as_str().expect("stdout string"))
}

fn render_headers_source(headers_require: &str) -> String {
    format!(
        r#"
        const Headers = require({headers_require});
        const captureError = (fn) => {{
          try {{
            return {{ ok: true, value: fn() }};
          }} catch (error) {{
            return {{ ok: false, name: error.name, message: error.message }};
          }}
        }};

        const results = {{}};
        const headers = new Headers();
        results.enumerableOwn = Object.getOwnPropertyNames(headers).sort();
        results.enumerableForIn = (() => {{
          const props = [];
          for (const prop in headers) props.push(prop);
          return props.sort();
        }})();
        results.tag = String(headers);

        const missing = new Headers([["foo", "bar"]]);
        results.missing = {{
          hasBaz: missing.has("baz"),
          getBaz: missing.get("baz"),
        }};

        const setTwice = new Headers();
        setTwice.set("foo", "bar");
        setTwice.set("foo", "baz");
        setTwice.append("foo", "qux");
        results.setTwice = setTwice.get("foo");

        const nodeCompatible = new Headers();
        nodeCompatible.set("foo", "bar");
        nodeCompatible.set("host", "example.com");
        results.nodeCompatible = Headers.exportNodeCompatibleHeaders(nodeCompatible);

        const lenient = Headers.createHeadersLenient({{
          "💩": ["ignore", "these"],
          badList: ["ok", "💩", "bar"],
          badStr: "💩",
          goodstr: "good",
        }});
        results.lenient = Headers.exportNodeCompatibleHeaders(lenient);

        const deleting = new Headers([["foo", "bar"]]);
        deleting.delete("foo");
        deleting.delete("foo");
        results.delete = deleting.has("foo");

        const ordered = new Headers([
          ["b", "2"],
          ["c", "4"],
          ["b", "3"],
          ["a", "1"],
        ]);
        results.forEach = [];
        ordered.forEach((value, key) => results.forEach.push([key, value]));
        results.iteration = {{
          entries: Array.from(ordered.entries()),
          keys: Array.from(ordered.keys()),
          values: Array.from(ordered.values()),
          iteratorTag: String(ordered.keys()),
        }};

        const illegal = new Headers();
        results.illegal = [
          captureError(() => new Headers({{ "He y": "ok" }})),
          captureError(() => new Headers({{ "Hé-y": "ok" }})),
          captureError(() => new Headers({{ "He-y": "ăk" }})),
          captureError(() => illegal.append("Hé-y", "ok")),
          captureError(() => illegal.delete("Hé-y")),
          captureError(() => illegal.get("Hé-y")),
          captureError(() => illegal.has("Hé-y")),
          captureError(() => illegal.set("Hé-y", "ok")),
          captureError(() => illegal.append("", "ok")),
        ];

        class FakeHeader {{}}
        FakeHeader.prototype.z = "fake";
        const fake = new FakeHeader();
        fake.a = "string";
        fake.b = ["1", "2"];
        fake.c = "";
        fake.d = [];
        fake.e = 1;
        fake.f = [1, 2];
        fake.g = {{ a: 1 }};
        fake.h = undefined;
        fake.i = null;
        fake.j = NaN;
        fake.k = true;
        fake.l = false;
        fake.m = Buffer.from("test");
        const weird = new Headers(fake);
        weird.set("n", [1, 2]);
        weird.append("n", ["3", 4]);
        results.weird = weird.raw();

        const wrapped1 = new Headers({{ a: "1" }});
        const wrapped2 = new Headers(wrapped1);
        wrapped2.set("b", "1");
        const wrapped3 = new Headers(wrapped2);
        wrapped3.append("a", "2");
        results.wrapped = {{
          h1: wrapped1.raw(),
          h2: wrapped2.raw(),
          h3: wrapped3.raw(),
        }};

        const iterable = new Headers([
          ["a", "1"],
          ["b", "2"],
          ["a", "3"],
        ]);
        results.iterable = {{
          a: iterable.get("a"),
          b: iterable.get("b"),
        }};

        console.log(JSON.stringify(results));
        "#,
        headers_require = serde_json::to_string(headers_require).expect("quoted require path"),
    )
}

#[tokio::test]
async fn minipass_fetch_headers_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping minipass-fetch headers contract because npm/cli root is unavailable");
        return;
    };
    let sandbox_require = "/workspace/npm/node_modules/minipass-fetch/lib/headers.js";
    let real_require = format!("{npm_root}/node_modules/minipass-fetch/lib/headers.js");

    let sandbox_source = render_headers_source(sandbox_require);
    let real_source = render_headers_source(&real_require);

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(460, 140).await else {
        eprintln!("skipping minipass-fetch headers contract because npm/cli session is unavailable");
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/minipass-fetch-headers-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox minipass-fetch headers contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node minipass-fetch headers contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
