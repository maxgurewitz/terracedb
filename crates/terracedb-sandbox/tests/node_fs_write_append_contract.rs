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
async fn node_fs_write_and_append_contract_matches_upstream_subset() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("node:fs");
        const path = require("node:path");

        const base = "/workspace/app/tmp-write-append-contract";
        fs.mkdirSync(base, { recursive: true });

        const writeString = path.join(base, "write-string.txt");
        fs.writeFileSync(writeString, "hello");

        const writeBuffer = path.join(base, "write-buffer.txt");
        fs.writeFileSync(writeBuffer, Buffer.from("world"));

        const writeFd = path.join(base, "write-fd.txt");
        const fd = fs.openSync(writeFd, "w+");
        fs.writeFileSync(fd, "123");
        fs.closeSync(fd);

        const appendFlag = path.join(base, "append-flag.txt");
        fs.writeFileSync(appendFlag, "hello ", { flag: "a" });
        fs.writeFileSync(appendFlag, "world!", { flag: "a" });

        const readOnly = path.join(base, "readonly.txt");
        fs.writeFileSync(readOnly, "");
        let readOnlyCode = null;
        try {
          fs.writeFileSync(readOnly, "boom", { flag: "r" });
        } catch (error) {
          readOnlyCode = error.code ?? null;
        }

        const invalidWriteData = {};
        for (const [name, value] of [
          ["false", false],
          ["number", 5],
          ["object", {}],
          ["array", []],
          ["null", null],
          ["undefined", undefined],
          ["true", true],
          ["bigint", 5n],
          ["fn", () => {}],
          ["symbol", Symbol("s")],
          ["map", new Map()],
        ]) {
          try {
            fs.writeFileSync(path.join(base, `invalid-write-${name}.txt`), value, { flag: "a" });
            invalidWriteData[name] = "ok";
          } catch (error) {
            invalidWriteData[name] = error.code ?? null;
          }
        }

        const invalidWriteFlush = {};
        for (const [name, value] of [
          ["string", "true"],
          ["empty", ""],
          ["zero", 0],
          ["one", 1],
          ["array", []],
          ["object", {}],
          ["symbol", Symbol("flush")],
        ]) {
          try {
            fs.writeFileSync(path.join(base, `flush-${name}.txt`), "x", { flush: value });
            invalidWriteFlush[name] = "ok";
          } catch (error) {
            invalidWriteFlush[name] = error.code ?? null;
          }
        }

        let writeFsyncCalls = 0;
        const originalFsyncSync = fs.fsyncSync;
        fs.fsyncSync = (...args) => {
          writeFsyncCalls += 1;
          return originalFsyncSync(...args);
        };
        fs.writeFileSync(path.join(base, "flush-write.txt"), "sync", { flush: true });
        fs.fsyncSync = originalFsyncSync;

        const appendCreate = path.join(base, "append-create.txt");
        fs.appendFileSync(appendCreate, "AB");
        fs.appendFileSync(appendCreate, Buffer.from("CD"));

        const appendExisting = path.join(base, "append-existing.txt");
        fs.writeFileSync(appendExisting, "ABCD");
        fs.appendFileSync(appendExisting, "EF");

        const appendFdTarget = path.join(base, "append-fd.txt");
        fs.writeFileSync(appendFdTarget, "ABCD");
        const appendFd = fs.openSync(appendFdTarget, "a+");
        fs.appendFileSync(appendFd, "GH");
        fs.closeSync(appendFd);

        const invalidAppendData = {};
        for (const [name, value] of [
          ["true", true],
          ["false", false],
          ["zero", 0],
          ["one", 1],
          ["infinity", Infinity],
          ["fn", () => {}],
          ["object", {}],
          ["array", []],
          ["undefined", undefined],
          ["null", null],
        ]) {
          try {
            const target = path.join(base, `invalid-append-${name}.txt`);
            fs.appendFileSync(target, value, { mode: 0o600 });
            invalidAppendData[name] = "ok";
          } catch (error) {
            invalidAppendData[name] = error.code ?? null;
          }
        }

        const invalidAppendFlush = {};
        for (const [name, value] of [
          ["string", "true"],
          ["empty", ""],
          ["zero", 0],
          ["one", 1],
          ["array", []],
          ["object", {}],
          ["symbol", Symbol("flush")],
        ]) {
          try {
            fs.appendFileSync(path.join(base, `append-flush-${name}.txt`), "x", { flush: value });
            invalidAppendFlush[name] = "ok";
          } catch (error) {
            invalidAppendFlush[name] = error.code ?? null;
          }
        }

        let appendFsyncCalls = 0;
        fs.fsyncSync = (...args) => {
          appendFsyncCalls += 1;
          return originalFsyncSync(...args);
        };
        fs.appendFileSync(path.join(base, "append-flush.txt"), "sync", { flush: true });
        fs.fsyncSync = originalFsyncSync;

        console.log(JSON.stringify({
          writeString: fs.readFileSync(writeString, "utf8"),
          writeBuffer: fs.readFileSync(writeBuffer, "utf8"),
          writeFd: fs.readFileSync(writeFd, "utf8"),
          appendFlag: fs.readFileSync(appendFlag, "utf8"),
          readOnlyCode,
          invalidWriteData,
          invalidWriteFlush,
          writeFsyncCalls,
          appendCreate: fs.readFileSync(appendCreate, "utf8"),
          appendExisting: fs.readFileSync(appendExisting, "utf8"),
          appendFd: fs.readFileSync(appendFdTarget, "utf8"),
          invalidAppendData,
          invalidAppendFlush,
          appendFsyncCalls,
        }));
        "#,
    )
    .await
    .expect("fs write/append contract should execute");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["writeString"], "hello");
    assert_eq!(parsed["writeBuffer"], "world");
    assert_eq!(parsed["writeFd"], "123");
    assert_eq!(parsed["appendFlag"], "hello world!");
    assert_eq!(parsed["readOnlyCode"], "EBADF");
    assert_eq!(parsed["appendCreate"], "ABCD");
    assert_eq!(parsed["appendExisting"], "ABCDEF");
    assert_eq!(parsed["appendFd"], "ABCDGH");
    assert_eq!(parsed["writeFsyncCalls"], 1);
    assert_eq!(parsed["appendFsyncCalls"], 1);

    for key in [
        "false",
        "number",
        "object",
        "array",
        "null",
        "undefined",
        "true",
        "bigint",
        "fn",
        "symbol",
        "map",
    ] {
        assert_eq!(
            parsed["invalidWriteData"][key], "ERR_INVALID_ARG_TYPE",
            "invalid write data {key} mismatch: {parsed:#?}"
        );
    }

    for key in [
        "string", "empty", "zero", "one", "array", "object", "symbol",
    ] {
        assert_eq!(
            parsed["invalidWriteFlush"][key], "ERR_INVALID_ARG_TYPE",
            "invalid write flush {key} mismatch: {parsed:#?}"
        );
        assert_eq!(
            parsed["invalidAppendFlush"][key], "ERR_INVALID_ARG_TYPE",
            "invalid append flush {key} mismatch: {parsed:#?}"
        );
    }

    for key in [
        "true",
        "false",
        "zero",
        "one",
        "infinity",
        "fn",
        "object",
        "array",
        "undefined",
        "null",
    ] {
        assert_eq!(
            parsed["invalidAppendData"][key], "ERR_INVALID_ARG_TYPE",
            "invalid append data {key} mismatch: {parsed:#?}"
        );
    }
}
