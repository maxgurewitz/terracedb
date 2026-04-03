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
async fn node_url_searchparams_and_urltohttpoptions_contract_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }

    // Based on test-url-urltooptions.js and the custom searchparams suites.
    let source = r#"
      const { URL, URLSearchParams, urlToHttpOptions } = require("url");
      const results = { invalidThis: [] };
      const extended = new URL("http://a:b@測試?abc#foo");
      extended.demo = 1;
      const base = new URL("https://user:pass@example.com:8080/p?q=1#h");
      const params = new URLSearchParams([
        ["b", "2"],
        ["a", "1"],
        ["b", "3"],
      ]);
      params.append("c", "4");
      params.set("a", "5");
      params.delete("missing");
      const sortedBefore = params.toString();
      params.sort();
      results.params = {
        beforeSort: sortedBefore,
        afterSort: params.toString(),
        getA: params.get("a"),
        getAllB: params.getAll("b"),
        hasC: params.has("c"),
        entries: Array.from(params.entries()),
        keys: Array.from(params.keys()),
        values: Array.from(params.values()),
        size: params.size,
        tag: Object.prototype.toString.call(params),
      };
      results.httpOptions = {
        base: urlToHttpOptions(base),
      };
      results.httpOptions.extended = urlToHttpOptions(extended);
      for (const thunk of [
        () => URLSearchParams.prototype.append.call(undefined, "a", "1"),
        () => URLSearchParams.prototype.delete.call(undefined, "a"),
        () => URLSearchParams.prototype.entries.call(undefined),
        () => URLSearchParams.prototype.forEach.call(undefined, () => {}),
        () => URLSearchParams.prototype.get.call(undefined, "a"),
        () => URLSearchParams.prototype.getAll.call(undefined, "a"),
        () => URLSearchParams.prototype.has.call(undefined, "a"),
        () => URLSearchParams.prototype.keys.call(undefined),
        () => URLSearchParams.prototype.set.call(undefined, "a", "1"),
        () => URLSearchParams.prototype.sort.call(undefined),
        () => URLSearchParams.prototype.toString.call(undefined),
        () => URLSearchParams.prototype.values.call(undefined),
      ]) {
        try {
          const value = thunk();
          results.invalidThis.push({ ok: true, value: typeof value });
        } catch (error) {
          results.invalidThis.push({ ok: false, name: error.name, code: error.code ?? null, message: error.message });
        }
      }
      console.log(JSON.stringify(results));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox url contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(source)
            .expect("real node url contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
