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

fn render_index_source(index_require: &str, proxy_require: &str, dns_require: &str) -> String {
    format!(
        r#"
        const indexPath = {index_require};
        const proxyPath = {proxy_require};
        const dnsPath = {dns_require};

        const summarizeAgent = (agent) => {{
          if (agent === false) {{
            return {{ kind: "false" }};
          }}
          const proxyUrl = agent && agent.proxy && agent.proxy.url
            ? {{
                protocol: agent.proxy.url.protocol,
                hostname: agent.proxy.url.hostname,
                port: agent.proxy.url.port,
              }}
            : null;
          return {{
            ctor: agent && agent.constructor ? agent.constructor.name : typeof agent,
            maxSockets: agent && agent.options ? agent.options.maxSockets ?? null : null,
            keepAlive: agent && agent.options ? agent.options.keepAlive ?? null : null,
            proxyUrl,
          }};
        }};

        const purge = (...paths) => {{
          for (const path of paths) {{
            try {{
              delete require.cache[require.resolve(path)];
            }} catch (_error) {{}}
          }}
        }};

        const freshIndex = () => {{
          purge(indexPath, proxyPath, dnsPath);
          return require(indexPath);
        }};

        const withEnv = (updates, fn) => {{
          const previous = {{}};
          for (const [key, value] of Object.entries(updates)) {{
            previous[key] = Object.prototype.hasOwnProperty.call(process.env, key)
              ? process.env[key]
              : undefined;
            if (value === undefined) {{
              delete process.env[key];
            }} else {{
              process.env[key] = value;
            }}
          }}
          try {{
            return fn();
          }} finally {{
            for (const [key, value] of Object.entries(previous)) {{
              if (value === undefined) {{
                delete process.env[key];
              }} else {{
                process.env[key] = value;
              }}
            }}
          }}
        }};

        const {{ getAgent, Agent, HttpAgent, HttpsAgent }} = require(indexPath);
        const results = {{
          http: summarizeAgent(getAgent("http://localhost")),
          https: summarizeAgent(getAgent("https://localhost")),
          falseAgent: summarizeAgent(getAgent("http://localhost", {{ agent: false }})),
          defaultSockets: summarizeAgent(getAgent("http://localhost", {{}})),
          explicitProxy: summarizeAgent(getAgent("http://localhost", {{ proxy: "http://localhost:8080" }})),
          noProxyMiss: summarizeAgent(getAgent("http://localhost", {{
            proxy: "http://localhost:8080",
            noProxy: "google.com",
          }})),
          noProxyHit: summarizeAgent(getAgent("http://google.com", {{
            proxy: "http://localhost:8080",
            noProxy: "google.com",
          }})),
          sameIdentity: null,
          differentIdentity: null,
          envHttpsProxyHttpsTarget: null,
          envHttpsProxyHttpTarget: null,
          envHttpProxyIgnoredForHttps: null,
          envProxyHttpTarget: null,
          envNoProxyHttpsTarget: null,
          invalidHttpProxy: null,
          invalidHttpsProxy: null,
          classNames: {{
            Agent: Agent.name,
            HttpAgent: HttpAgent.name,
            HttpsAgent: HttpsAgent.name,
          }},
        }};

        {{
          const a = getAgent("http://localhost", {{ proxy: "http://user1:pass1@localhost" }});
          const b = getAgent("http://localhost", {{ proxy: "http://user1:pass1@localhost" }});
          const c = getAgent("http://localhost", {{ proxy: "http://user2:pass2@localhost" }});
          results.sameIdentity = a === b;
          results.differentIdentity = a === c;
        }}

        results.envHttpsProxyHttpsTarget = withEnv({{ https_proxy: "https://localhost" }}, () => {{
          const fresh = freshIndex();
          return summarizeAgent(fresh.getAgent("https://localhost"));
        }});
        results.envHttpsProxyHttpTarget = withEnv({{ https_proxy: "https://localhost" }}, () => {{
          const fresh = freshIndex();
          return summarizeAgent(fresh.getAgent("http://localhost"));
        }});
        results.envHttpProxyIgnoredForHttps = withEnv({{ http_proxy: "http://localhost" }}, () => {{
          const fresh = freshIndex();
          return summarizeAgent(fresh.getAgent("https://localhost"));
        }});
        results.envProxyHttpTarget = withEnv({{ proxy: "http://localhost" }}, () => {{
          const fresh = freshIndex();
          return summarizeAgent(fresh.getAgent("http://localhost"));
        }});
        results.envNoProxyHttpsTarget = withEnv({{
          https_proxy: "https://localhost",
          no_proxy: "google.com",
        }}, () => {{
          const fresh = freshIndex();
          return {{
            google: summarizeAgent(fresh.getAgent("https://google.com")),
            localhost: summarizeAgent(fresh.getAgent("https://localhost")),
          }};
        }});

        try {{
          new HttpAgent({{ proxy: "foo://not-supported" }});
          results.invalidHttpProxy = null;
        }} catch (error) {{
          results.invalidHttpProxy = {{
            name: error.name,
            code: error.code ?? null,
            message: error.message,
          }};
        }}

        try {{
          new HttpsAgent({{ proxy: "foo://not-supported" }});
          results.invalidHttpsProxy = null;
        }} catch (error) {{
          results.invalidHttpsProxy = {{
            name: error.name,
            code: error.code ?? null,
            message: error.message,
          }};
        }}

        console.log(JSON.stringify(results));
        "#,
        index_require = serde_json::to_string(index_require).expect("quoted index require"),
        proxy_require = serde_json::to_string(proxy_require).expect("quoted proxy require"),
        dns_require = serde_json::to_string(dns_require).expect("quoted dns require"),
    )
}

#[tokio::test]
async fn npmcli_agent_index_contract_matches_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!("skipping @npmcli/agent index contract because npm/cli root is unavailable");
        return;
    };

    let sandbox_source = render_index_source(
        "/workspace/npm/node_modules/@npmcli/agent/lib/index.js",
        "/workspace/npm/node_modules/@npmcli/agent/lib/proxy.js",
        "/workspace/npm/node_modules/@npmcli/agent/lib/dns.js",
    );
    let real_source = render_index_source(
        &format!("{npm_root}/node_modules/@npmcli/agent/lib/index.js"),
        &format!("{npm_root}/node_modules/@npmcli/agent/lib/proxy.js"),
        &format!("{npm_root}/node_modules/@npmcli/agent/lib/dns.js"),
    );

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(471, 151).await else {
        eprintln!("skipping @npmcli/agent index contract because npm/cli session is unavailable");
        return;
    };

    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/npmcli-agent-index-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox @npmcli/agent index contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node @npmcli/agent index contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
