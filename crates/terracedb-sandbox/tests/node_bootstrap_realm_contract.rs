#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/tracing.rs"]
mod tracing_support;

#[tokio::test]
async fn bootstrap_realm_loads_path_and_caches_internal_bindings() {
    tracing_support::init_tracing();

    let result = node_compat::exec_node_fixture(
        r#"
        const realm = __terraceLoadBootstrapRealm();
        const pathA = realm.require("path");
        const pathB = realm.require("path");
        const builtinsA = realm.internalBinding("builtins");
        const builtinsB = realm.internalBinding("builtins");

        console.log(JSON.stringify({
          sameBuiltins: builtinsA === builtinsB,
          samePath: pathA === pathB,
          normalizeNodePath: realm.BuiltinModule.normalizeRequirableId("node:path"),
          canRequirePath: realm.BuiltinModule.canBeRequiredByUsers("path"),
          isBuiltinNodePath: realm.BuiltinModule.isBuiltin("node:path"),
          joined: pathA.join("/workspace", "app", "index.js"),
          basename: pathA.basename("/workspace/app/index.js"),
          moduleLoadList: process.moduleLoadList.filter((entry) =>
            entry.includes("Internal Binding builtins") ||
            entry.includes("NativeModule path") ||
            entry.includes("NativeModule internal/constants")
          ),
        }));
        "#,
    )
    .await
    .expect("bootstrap realm path contract");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    assert_eq!(payload["sameBuiltins"].as_bool(), Some(true));
    assert_eq!(payload["samePath"].as_bool(), Some(true));
    assert_eq!(payload["normalizeNodePath"].as_str(), Some("path"));
    assert_eq!(payload["canRequirePath"].as_bool(), Some(true));
    assert_eq!(payload["isBuiltinNodePath"].as_bool(), Some(true));
    assert_eq!(payload["joined"].as_str(), Some("/workspace/app/index.js"));
    assert_eq!(payload["basename"].as_str(), Some("index.js"));

    let load_list = payload["moduleLoadList"]
        .as_array()
        .expect("module load list");
    assert!(
        load_list
            .iter()
            .any(|entry| entry.as_str() == Some("Internal Binding builtins")),
        "expected builtins binding in moduleLoadList: {load_list:?}"
    );
    assert!(
        load_list
            .iter()
            .any(|entry| entry.as_str() == Some("NativeModule path")),
        "expected path native module in moduleLoadList: {load_list:?}"
    );
}

#[tokio::test]
async fn bootstrap_realm_loads_upstream_internal_support_modules() {
    tracing_support::init_tracing();

    let result = node_compat::exec_node_fixture(
        r#"
        const realm = __terraceLoadBootstrapRealm();
        const constants = realm.require("internal/constants");
        const internalAssert = realm.require("internal/assert");
        const validators = realm.require("internal/validators");
        const options = realm.require("internal/options");
        const util = realm.require("internal/util");
        const utilTypes = realm.require("internal/util/types");
        const errors = realm.require("internal/errors");

        let validateStringCode = null;
        try {
          validators.validateString(42, "name");
        } catch (error) {
          validateStringCode = error.code;
        }

        let internalAssertCode = null;
        try {
          internalAssert(false, "boom");
        } catch (error) {
          internalAssertCode = error.code;
        }

        const lazy = util.getLazy(() => ({ ok: true }));
        const first = lazy();
        const second = lazy();

        console.log(JSON.stringify({
          slash: constants.CHAR_FORWARD_SLASH,
          eol: constants.EOL,
          validateStringCode,
          internalAssertCode,
          sameLazyValue: first === second,
          cidr: util.getCIDR("127.0.0.1", "255.0.0.0", "IPv4"),
          preserveSymlinks: options.getOptionValue("--preserve-symlinks"),
          noGlobalSearchPaths: options.getEmbedderOptions().noGlobalSearchPaths,
          isArrayBufferView: utilTypes.isArrayBufferView(new Uint8Array([1, 2, 3])),
          isRegExp: utilTypes.isRegExp(/x/),
          unknownBuiltinCode: new errors.codes.ERR_UNKNOWN_BUILTIN_MODULE("node:nope").code,
        }));
        "#,
    )
    .await
    .expect("bootstrap realm upstream internal support contract");

    let report = result.result.expect("node command report");
    let stdout = report["stdout"].as_str().expect("stdout");
    let payload = stdout
        .lines()
        .find(|line| line.starts_with('{'))
        .expect("structured payload");
    let payload: serde_json::Value = serde_json::from_str(payload).expect("decode payload");

    assert_eq!(payload["slash"].as_i64(), Some(47));
    assert_eq!(payload["eol"].as_str(), Some("\n"));
    assert_eq!(payload["validateStringCode"].as_str(), Some("ERR_INVALID_ARG_TYPE"));
    assert_eq!(
        payload["internalAssertCode"].as_str(),
        Some("ERR_INTERNAL_ASSERTION")
    );
    assert_eq!(payload["sameLazyValue"].as_bool(), Some(true));
    assert_eq!(payload["cidr"].as_str(), Some("127.0.0.1/8"));
    assert_eq!(payload["preserveSymlinks"].as_bool(), Some(false));
    assert_eq!(payload["noGlobalSearchPaths"].as_bool(), Some(true));
    assert_eq!(payload["isArrayBufferView"].as_bool(), Some(true));
    assert_eq!(payload["isRegExp"].as_bool(), Some(true));
    assert_eq!(
        payload["unknownBuiltinCode"].as_str(),
        Some("ERR_UNKNOWN_BUILTIN_MODULE")
    );
}
