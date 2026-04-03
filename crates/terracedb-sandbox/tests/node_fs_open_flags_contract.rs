#[path = "support/node_compat.rs"]
mod node_compat_support;

fn stdout(result: terracedb_sandbox::SandboxExecutionResult) -> String {
    result.result.expect("node command report")["stdout"]
        .as_str()
        .unwrap_or_default()
        .trim()
        .to_string()
}

#[tokio::test]
async fn node_fs_open_flag_contract_matches_upstream_subset() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("node:fs");
        const path = require("node:path");

        const base = "/workspace/app/tmp-open-contract";
        fs.mkdirSync(base, { recursive: true });

        const existing = path.join(base, "existing.txt");
        fs.writeFileSync(existing, "seed");

        const exclusiveExisting = {};
        for (const flag of ["wx", "xw", "wx+", "xw+", "ax", "xa", "ax+", "xa+"]) {
          try {
            fs.openSync(existing, flag);
            exclusiveExisting[flag] = "opened";
          } catch (error) {
            exclusiveExisting[flag] = error.code ?? null;
          }
        }

        const appendSyncFlags = {};
        for (const flag of ["as", "sa", "as+", "sa+"]) {
          const target = path.join(base, `${flag.replace(/[^a-z+]/g, "_")}.txt`);
          fs.writeFileSync(target, "seed");
          try {
            const fd = fs.openSync(target, flag);
            fs.writeSync(fd, Buffer.from(flag), 0, flag.length, null);
            fs.closeSync(fd);
            appendSyncFlags[flag] = fs.readFileSync(target, "utf8");
          } catch (error) {
            appendSyncFlags[flag] = { code: error.code ?? null, message: String(error.message || error) };
          }
        }

        const syncReadFlags = {};
        for (const flag of ["rs", "sr", "rs+", "sr+"]) {
          try {
            const fd = fs.openSync(existing, flag);
            fs.closeSync(fd);
            syncReadFlags[flag] = "ok";
          } catch (error) {
            syncReadFlags[flag] = error.code ?? null;
          }
        }

        const invalid = {};
        for (const flag of ["+", "+a", "+r", "+w", "rw", "wa", "war", "raw", "r++", "a++", "w++", "x", "+x", "x+", "rx", "rx+", "wxx", "wax", "xwx", "xxx"]) {
          try {
            fs.openSync(existing, flag);
            invalid[flag] = "opened";
          } catch (error) {
            invalid[flag] = error.code ?? null;
          }
        }

        console.log(JSON.stringify({
          exclusiveExisting,
          appendSyncFlags,
          syncReadFlags,
          invalid,
        }));
        "#,
    )
    .await
    .expect("fs open flags contract should execute");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");

    for flag in ["wx", "xw", "wx+", "xw+", "ax", "xa", "ax+", "xa+"] {
        assert_eq!(
            parsed["exclusiveExisting"][flag], "EEXIST",
            "flag {flag} mismatch: {parsed:#?}"
        );
    }

    for flag in ["rs", "sr", "rs+", "sr+"] {
        assert_eq!(
            parsed["syncReadFlags"][flag], "ok",
            "flag {flag} mismatch: {parsed:#?}"
        );
    }

    for flag in ["as", "sa", "as+", "sa+"] {
        let value = parsed["appendSyncFlags"][flag]
            .as_str()
            .unwrap_or_else(|| panic!("flag {flag} missing appended contents: {parsed:#?}"));
        assert!(
            value.ends_with(flag),
            "flag {flag} should append at end, got {value:?} in {parsed:#?}"
        );
    }

    for flag in [
        "+", "+a", "+r", "+w", "rw", "wa", "war", "raw", "r++", "a++", "w++", "x", "+x", "x+",
        "rx", "rx+", "wxx", "wax", "xwx", "xxx",
    ] {
        assert_eq!(
            parsed["invalid"][flag], "ERR_INVALID_ARG_VALUE",
            "invalid flag {flag} mismatch: {parsed:#?}"
        );
    }
}
