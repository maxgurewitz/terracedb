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
async fn node_fs_promises_write_append_and_mkdtemp_contract_matches_upstream_subset() {
    let result = node_compat_support::exec_node_fixture(
        r#"
        const fs = require("node:fs");
        const path = require("node:path");

        (async () => {
          const base = "/workspace/app/tmp-promises-contract";
          fs.mkdirSync(base, { recursive: true });

          const writeString = path.join(base, "write-string.txt");
          await fs.promises.writeFile(writeString, "hello");

          const writeBuffer = path.join(base, "write-buffer.txt");
          await fs.promises.writeFile(writeBuffer, Buffer.from("world"));

          const writeFd = path.join(base, "write-fd.txt");
          const handle = await fs.promises.open(writeFd, "w+");
          await handle.writeFile("123");
          await handle.close();

          const appendCreate = path.join(base, "append-create.txt");
          await fs.promises.appendFile(appendCreate, "AB");
          await fs.promises.appendFile(appendCreate, Buffer.from("CD"));

          const appendExisting = path.join(base, "append-existing.txt");
          fs.writeFileSync(appendExisting, "ABCD");
          await fs.promises.appendFile(appendExisting, "EF");

          const appendFd = path.join(base, "append-fd.txt");
          fs.writeFileSync(appendFd, "ABCD");
          const appendHandle = await fs.promises.open(appendFd, "a+");
          await fs.promises.appendFile(appendHandle, "GH");
          await appendHandle.close();

          const readOnly = path.join(base, "readonly.txt");
          fs.writeFileSync(readOnly, "");
          let readOnlyCode = null;
          try {
            await fs.promises.writeFile(readOnly, "boom", { flag: "r" });
          } catch (error) {
            readOnlyCode = error.code ?? null;
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
              await fs.promises.writeFile(path.join(base, `invalid-write-flush-${name}.txt`), "x", { flush: value });
              invalidWriteFlush[name] = "ok";
            } catch (error) {
              invalidWriteFlush[name] = error.code ?? null;
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
              await fs.promises.appendFile(path.join(base, `invalid-append-flush-${name}.txt`), "x", { flush: value });
              invalidAppendFlush[name] = "ok";
            } catch (error) {
              invalidAppendFlush[name] = error.code ?? null;
            }
          }

          const mkdtempString = await fs.promises.mkdtemp(path.join(base, "foo."));

          console.log(JSON.stringify({
            writeString: fs.readFileSync(writeString, "utf8"),
            writeBuffer: fs.readFileSync(writeBuffer, "utf8"),
            writeFd: fs.readFileSync(writeFd, "utf8"),
            appendCreate: fs.readFileSync(appendCreate, "utf8"),
            appendExisting: fs.readFileSync(appendExisting, "utf8"),
            appendFd: fs.readFileSync(appendFd, "utf8"),
            readOnlyCode,
            invalidWriteFlush,
            invalidAppendFlush,
            mkdtempStringLength: path.basename(mkdtempString).length,
            mkdtempExists: fs.existsSync(mkdtempString),
          }));
        })().catch((error) => {
          console.error(error && error.stack || String(error));
          process.exitCode = 1;
        });
        "#,
    )
    .await
    .expect("fs.promises contract should execute");

    let parsed: serde_json::Value = serde_json::from_str(&stdout(result)).expect("stdout json");
    assert_eq!(parsed["writeString"], "hello");
    assert_eq!(parsed["writeBuffer"], "world");
    assert_eq!(parsed["writeFd"], "123");
    assert_eq!(parsed["appendCreate"], "ABCD");
    assert_eq!(parsed["appendExisting"], "ABCDEF");
    assert_eq!(parsed["appendFd"], "ABCDGH");
    assert_eq!(parsed["readOnlyCode"], "EBADF");
    assert_eq!(parsed["mkdtempStringLength"], "foo.XXXXXX".len());
    assert_eq!(parsed["mkdtempExists"], true);

    for key in [
        "string", "empty", "zero", "one", "array", "object", "symbol",
    ] {
        assert_eq!(
            parsed["invalidWriteFlush"][key], "ERR_INVALID_ARG_TYPE",
            "invalid promises.writeFile flush {key} mismatch: {parsed:#?}"
        );
        assert_eq!(
            parsed["invalidAppendFlush"][key], "ERR_INVALID_ARG_TYPE",
            "invalid promises.appendFile flush {key} mismatch: {parsed:#?}"
        );
    }
}
