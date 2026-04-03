use serde_json::Value;

#[path = "support/node_compat.rs"]
mod node_compat_support;

fn sandbox_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_os_priority_and_user_info_behavior_match_expected_contract() {
    let source = r#"
      "use strict";
      const os = require("os");
      function capture(fn) {
        try {
          return { ok: true, value: fn() };
        } catch (error) {
          return { ok: false, name: error.name, code: error.code ?? null, message: String(error.message) };
        }
      }

      const user = os.userInfo();
      const userBuffer = os.userInfo({ encoding: "buffer" });
      console.log(JSON.stringify({
        constants: os.constants.priority,
        invalidPidType: capture(() => os.getPriority("foo")),
        invalidPidRange: capture(() => os.getPriority(2 ** 32)),
        invalidPriorityType: capture(() => os.setPriority(0, "foo")),
        invalidPriorityRange: capture(() => os.setPriority(0, os.constants.priority.PRIORITY_HIGHEST - 1)),
        setCurrent: capture(() => {
          os.setPriority(os.constants.priority.PRIORITY_BELOW_NORMAL);
          return os.getPriority();
        }),
        setExplicit: capture(() => {
          os.setPriority(0, os.constants.priority.PRIORITY_NORMAL);
          return os.getPriority(0);
        }),
        missingPid: capture(() => os.getPriority(-1)),
        getterError: capture(() => os.userInfo({
          get encoding() {
            throw new Error("xyz");
          }
        })),
        user,
        userBuffer: {
          usernameIsBuffer: Buffer.isBuffer(userBuffer.username),
          homedirIsBuffer: Buffer.isBuffer(userBuffer.homedir),
          shellIsBuffer: Buffer.isBuffer(userBuffer.shell),
        },
      }));
    "#;

    let sandbox = sandbox_stdout_json(
        node_compat_support::exec_node_fixture(source)
            .await
            .expect("sandbox os priority"),
    );

    assert_eq!(sandbox["invalidPidType"]["ok"], Value::Bool(false));
    assert_eq!(
        sandbox["invalidPidType"]["code"],
        Value::String("ERR_INVALID_ARG_TYPE".into())
    );
    assert_eq!(sandbox["invalidPidRange"]["ok"], Value::Bool(false));
    assert_eq!(
        sandbox["invalidPidRange"]["code"],
        Value::String("ERR_OUT_OF_RANGE".into())
    );
    assert_eq!(sandbox["invalidPriorityType"]["ok"], Value::Bool(false));
    assert_eq!(
        sandbox["invalidPriorityType"]["code"],
        Value::String("ERR_INVALID_ARG_TYPE".into())
    );
    assert_eq!(sandbox["invalidPriorityRange"]["ok"], Value::Bool(false));
    assert_eq!(
        sandbox["invalidPriorityRange"]["code"],
        Value::String("ERR_OUT_OF_RANGE".into())
    );
    assert_eq!(sandbox["setCurrent"]["ok"], Value::Bool(true));
    assert_eq!(sandbox["setExplicit"]["ok"], Value::Bool(true));
    assert_eq!(sandbox["missingPid"]["ok"], Value::Bool(false));
    assert_eq!(
        sandbox["missingPid"]["name"],
        Value::String("SystemError".into())
    );
    assert_eq!(sandbox["getterError"]["ok"], Value::Bool(false));
    assert!(
        sandbox["getterError"]["message"]
            .as_str()
            .expect("getter error string")
            .contains("xyz")
    );
    assert_eq!(sandbox["user"]["username"], Value::String("sandbox".into()));
    assert_eq!(sandbox["userBuffer"]["usernameIsBuffer"], Value::Bool(true));
    assert_eq!(sandbox["userBuffer"]["homedirIsBuffer"], Value::Bool(true));
    assert_eq!(sandbox["userBuffer"]["shellIsBuffer"], Value::Bool(true));
}
