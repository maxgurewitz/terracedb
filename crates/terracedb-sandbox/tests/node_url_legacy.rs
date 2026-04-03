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
async fn node_url_legacy_parse_and_resolve_cases_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    let source = r##"
      const url = require("url");
      const parseCases = [
        url.parse("http://example.com"),
        url.parse("/example?query=value", true),
        url.parse("git+ssh://git@github.com/npm/cli.git"),
        url.parse("git@github.com:npm/cli.git"),
      ].map((entry) => ({
        protocol: entry.protocol,
        slashes: entry.slashes,
        auth: entry.auth,
        host: entry.host,
        port: entry.port,
        hostname: entry.hostname,
        hash: entry.hash,
        search: entry.search,
        query: entry.query,
        pathname: entry.pathname,
        path: entry.path,
        href: entry.href,
      }));
      const resolveCases = [
        ["/foo/bar/baz", "quux", url.resolve("/foo/bar/baz", "quux")],
        ["/foo/bar/baz", "../quux/baz", url.resolve("/foo/bar/baz", "../quux/baz")],
        ["http://example.com/b//c//d;p?q#blarg", "https:#hash2", url.resolve("http://example.com/b//c//d;p?q#blarg", "https:#hash2")],
        ["http://example.com/b//c//d;p?q#blarg", "http:/p/a/t/h?s#hash2", url.resolve("http://example.com/b//c//d;p?q#blarg", "http:/p/a/t/h?s#hash2")],
        ["https://registry.npmjs.org", "@foo/bar", url.resolve("https://registry.npmjs.org", "@foo/bar")],
      ];
      const resolvedObject = url.resolveObject(url.parse("https://registry.npmjs.org/left-pad"), "../@foo/bar");
      console.log(JSON.stringify({
        parseCases,
        resolveCases,
        resolveObject: {
          href: resolvedObject.href,
          protocol: resolvedObject.protocol,
          host: resolvedObject.host,
          pathname: resolvedObject.pathname,
        },
      }));
    "##;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox url legacy"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node url legacy")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
