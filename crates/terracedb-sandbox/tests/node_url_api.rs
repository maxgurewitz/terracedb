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
async fn node_url_exports_and_core_ops_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r##"
      const url = require("url");
      const parsed = url.parse("/foo/bar?baz=quux#frag", true);
      const queryProtoIsNull = Object.getPrototypeOf(parsed.query) === null;
      const httpOptions = url.urlToHttpOptions(new URL("https://user:pass@example.com:8080/p?q=1#h"));
      console.log(JSON.stringify({
        keys: Object.keys(url).sort(),
        parsed: {
          href: parsed.href,
          hash: parsed.hash,
          search: parsed.search,
          query: parsed.query,
          queryProtoIsNull,
          pathname: parsed.pathname,
          path: parsed.path,
        },
        format: url.format({
          protocol: "https:",
          slashes: true,
          hostname: "example.com",
          pathname: "/pkg",
          query: { name: "demo", value: "1" },
          hash: "#anchor",
        }),
        resolve: [
          url.resolve("https://registry.npmjs.org", "@foo/bar"),
          url.resolve("/foo/bar/baz", "../quux/baz"),
          url.resolve("http://example.com/b//c//d;p?q#blarg", "http:/p/a/t/h?s#hash2"),
        ],
        httpOptions,
        searchParams: Array.from(new url.URLSearchParams("a=b&c=d").entries()),
      }));
    "##;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox url api"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node url api")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
