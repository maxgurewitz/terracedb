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

fn render_make_fetch_happen_source(index_require: &str, options_require: &str) -> String {
    format!(
        r#"
        const makeFetchHappen = require({index_require});
        const configureOptions = require({options_require});

        const sanitizeOptions = (value) => {{
          const result = {{}};
          for (const [key, entry] of Object.entries(value)) {{
            if (key === "dns") {{
              result.dns = {{
                ttl: entry.ttl,
                lookupType: typeof entry.lookup,
              }};
              continue;
            }}
            if (key === "retry") {{
              result.retry = {{ ...entry }};
              continue;
            }}
            if (key === "headers") {{
              result.headers = entry ? {{ ...entry }} : entry;
              continue;
            }}
            if (Array.isArray(entry)) {{
              result[key] = [...entry];
              continue;
            }}
            result[key] = entry;
          }}
          return result;
        }};

        const results = {{}};
        const tlsBefore = process.env.NODE_TLS_REJECT_UNAUTHORIZED;

        results.defaults = {{
          noValue: sanitizeOptions(configureOptions()),
          empty: sanitizeOptions(configureOptions({{}})),
          lowercaseMethod: sanitizeOptions(configureOptions({{ method: "post" }})),
          strictTrue: sanitizeOptions(configureOptions({{ strictSSL: true }})),
          strictFalse: sanitizeOptions(configureOptions({{ strictSSL: false }})),
          strictNull: sanitizeOptions(configureOptions({{ strictSSL: null }})),
          dnsTtl: sanitizeOptions(configureOptions({{ dns: {{ ttl: 100 }} }})),
          dnsLookup: sanitizeOptions(configureOptions({{ dns: {{ lookup() {{}} }} }})),
          retryString: sanitizeOptions(configureOptions({{ retry: "10" }})),
          retryNumber: sanitizeOptions(configureOptions({{ retry: 10 }})),
          retryObject: sanitizeOptions(configureOptions({{ retry: {{ factor: 2 }} }})),
          conditionalCache: sanitizeOptions(configureOptions({{
            headers: {{ "If-None-Match": "abc" }},
          }})),
          cacheManager: sanitizeOptions(configureOptions({{
            cacheManager: "/tmp/cache-a",
          }})),
        }};

        process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
        results.rejectUnauthorizedFromEnv = sanitizeOptions(configureOptions({{}})).rejectUnauthorized;
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = tlsBefore;

        const wrappedCalls = [];
        const wrappedFetch = (url, options) => {{
          wrappedCalls.push({{
            url,
            options: sanitizeOptions(options),
          }});
          return wrappedCalls[wrappedCalls.length - 1];
        }};
        const defaulted = makeFetchHappen.defaults("http://localhost/test", {{ headers: {{ "x-foo": "bar" }} }}, wrappedFetch);
        const layered = defaulted.defaults({{ headers: {{ "x-bar": "baz" }} }});
        const nilDefaulted = makeFetchHappen.defaults(undefined, undefined, wrappedFetch);

        results.defaultsApi = {{
          noArgs: nilDefaulted("http://localhost/empty"),
          baseUrl: defaulted(undefined, {{ headers: {{ "x-baz": "qux" }} }}),
          layered: layered("http://localhost/override", {{ headers: {{ "x-quux": "zot" }} }}),
        }};
        results.wrappedCalls = wrappedCalls;

        console.log(JSON.stringify(results));
        "#,
        index_require = serde_json::to_string(index_require).expect("quoted index require"),
        options_require = serde_json::to_string(options_require).expect("quoted options require"),
    )
}

#[tokio::test]
async fn make_fetch_happen_options_and_defaults_contract_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping make-fetch-happen contract because npm/cli root is unavailable");
        return;
    };
    let sandbox_source = render_make_fetch_happen_source(
        "/workspace/npm/node_modules/make-fetch-happen/lib/index.js",
        "/workspace/npm/node_modules/make-fetch-happen/lib/options.js",
    );
    let real_source = render_make_fetch_happen_source(
        &format!("{npm_root}/node_modules/make-fetch-happen/lib/index.js"),
        &format!("{npm_root}/node_modules/make-fetch-happen/lib/options.js"),
    );

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(462, 142).await else {
        eprintln!("skipping make-fetch-happen contract because npm/cli session is unavailable");
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/make-fetch-happen-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox make-fetch-happen contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node make-fetch-happen contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
