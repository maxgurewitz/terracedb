use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_os_constants_match_real_node_shape() {
    let source = r#"
      "use strict";
      const os = require("os");
      let signalMutation;
      try {
        os.constants.signals.FOOBAR = 1337;
        signalMutation = "mutated";
      } catch (error) {
        signalMutation = { name: error.name, code: error.code ?? null };
      }
      console.log(JSON.stringify({
        keys: Object.keys(os.constants).sort(),
        errnoKeys: Object.keys(os.constants.errno).sort(),
        signalKeys: Object.keys(os.constants.signals).sort(),
        priorityKeys: Object.keys(os.constants.priority).sort(),
        dlopenKeys: Object.keys(os.constants.dlopen).sort(),
        uv: os.constants.UV_UDP_REUSEADDR,
        errnoSubset: {
          EEXIST: os.constants.errno.EEXIST,
          EISDIR: os.constants.errno.EISDIR,
          EINVAL: os.constants.errno.EINVAL,
          ENOTDIR: os.constants.errno.ENOTDIR,
        },
        signalMutation,
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox os constants"),
    );
    assert_eq!(
        sandbox["keys"],
        serde_json::json!(["UV_UDP_REUSEADDR", "dlopen", "errno", "priority", "signals"])
    );
    for key in ["EEXIST", "EISDIR", "EINVAL", "ENOTDIR"] {
        assert!(
            sandbox["errnoKeys"]
                .as_array()
                .expect("errno keys")
                .iter()
                .any(|entry| entry.as_str() == Some(key)),
            "missing errno key {key}: {:#?}",
            sandbox["errnoKeys"]
        );
    }
    for key in ["SIGINT", "SIGTERM", "SIGKILL"] {
        assert!(
            sandbox["signalKeys"]
                .as_array()
                .expect("signal keys")
                .iter()
                .any(|entry| entry.as_str() == Some(key)),
            "missing signal key {key}: {:#?}",
            sandbox["signalKeys"]
        );
    }
    assert_eq!(
        sandbox["priorityKeys"],
        serde_json::json!([
            "PRIORITY_ABOVE_NORMAL",
            "PRIORITY_BELOW_NORMAL",
            "PRIORITY_HIGH",
            "PRIORITY_HIGHEST",
            "PRIORITY_LOW",
            "PRIORITY_NORMAL",
        ])
    );
    assert_eq!(
        sandbox["dlopenKeys"],
        serde_json::json!(["RTLD_GLOBAL", "RTLD_LAZY", "RTLD_LOCAL", "RTLD_NOW"])
    );
    assert_eq!(
        sandbox["signalMutation"]["name"],
        Value::String("TypeError".into())
    );
}
