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

fn render_body_response_source(
    body_require: &str,
    response_require: &str,
    headers_require: &str,
    blob_require: &str,
    minipass_require: &str,
) -> String {
    format!(
        r#"
        const Body = require({body_require});
        const Response = require({response_require});
        const Headers = require({headers_require});
        const Blob = require({blob_require});
        const {{ Minipass }} = require({minipass_require});
        const {{ PassThrough }} = require("stream");
        const {{ URLSearchParams }} = require("url");

        const captureError = (fn) => {{
          try {{
            return {{ ok: true, value: fn() }};
          }} catch (error) {{
            return {{ ok: false, name: error.name, message: error.message, code: error.code ?? null, type: error.type ?? null }};
          }}
        }};
        const captureErrorAsync = async (fn) => {{
          try {{
            return {{ ok: true, value: await fn() }};
          }} catch (error) {{
            return {{ ok: false, name: error.name, message: error.message, code: error.code ?? null, type: error.type ?? null }};
          }}
        }};
        const stripMessage = (result) => result && result.ok === false
          ? {{ ok: false, name: result.name, code: result.code ?? null, type: result.type ?? null }}
          : result;

        (async () => {{
          const results = {{}};

          const nullBody = new Body();
          results.bodyNull = {{
            bodyIsNull: nullBody.body === null,
            contentType: Body.extractContentType(nullBody.body),
            totalBytes: Body.getTotalBytes(nullBody),
            buffer: (await nullBody.buffer()).toString("utf8"),
          }};

          const paramsBody = new Body(new URLSearchParams("a=1"));
          results.bodyUrlSearchParams = {{
            text: paramsBody.body.toString(),
            totalBytes: Body.getTotalBytes(paramsBody),
          }};

          const blobBody = new Body(new Blob("a=1", {{ type: "foo", size: 3 }}));
          blobBody.url = "double";
          results.bodyBlob = {{
            totalBytes: Body.getTotalBytes(blobBody),
            contentType: Body.extractContentType(blobBody.body),
            bodyUsedBefore: blobBody.bodyUsed,
            text: await blobBody.text(),
            bodyUsedAfter: blobBody.bodyUsed,
            secondRead: await captureErrorAsync(() => blobBody.buffer()),
          }};

          const bufferBody = new Body(Buffer.from("a=1"));
          const arrayBufferBody = new Body(new Uint8Array([97, 61, 49]).buffer);
          const uint8Body = new Body(new Uint8Array([97, 61, 49]));
          const dataViewBody = new Body(new DataView(new Uint8Array([97, 61, 49]).buffer));
          results.bodyBinary = {{
            bufferText: bufferBody.body.toString(),
            bufferArrayBufferText: Buffer.from(await bufferBody.arrayBuffer()).toString(),
            arrayBufferText: arrayBufferBody.body.toString(),
            uint8Text: uint8Body.body.toString(),
            dataViewText: dataViewBody.body.toString(),
          }};

          const streamBody = new Body(new Minipass({{ encoding: "utf8" }}).end("a=1"));
          const oversizedBody = new Body(new PassThrough({{ encoding: "utf8" }}).end("a=1"), {{ size: 1 }});
          results.bodyStreams = {{
            text: await streamBody.text(),
            tooLong: await captureErrorAsync(() => oversizedBody.text()),
            staticContentTypes: {{
              string: Body.extractContentType("a=1"),
              searchParams: Body.extractContentType(new URLSearchParams("a=1")),
              arrayBuffer: Body.extractContentType(new Uint8Array([97, 61, 49]).buffer),
              uint8: Body.extractContentType(new Uint8Array([97, 61, 49])),
              blob: Body.extractContentType(new Blob()),
              object: Body.extractContentType({{}}),
              totalBytesObject: Body.getTotalBytes({{ body: {{}} }}),
            }},
          }};

          const invalidJson = Object.assign(new Body("a=1"), {{ url: "asdf" }});
          results.bodyJsonError = stripMessage(await captureErrorAsync(() => invalidJson.json()));

          const writable = new Minipass({{ encoding: "utf8" }});
          Body.writeToStream(writable, {{ body: "a=1" }});
          results.bodyWriteToStream = await writable.concat();

          const response = new Response("a=1", {{
            headers: {{ a: "1", "Content-Type": "text/plain" }},
            url: "http://localhost/demo",
            status: 346,
            statusText: "production",
          }});
          const cloneSource = new Minipass().end("a=1");
          cloneSource.pause();
          setTimeout(() => cloneSource.resume());
          const clonedResponse = new Response(cloneSource.pipe(new Minipass()), {{
            headers: {{ a: "1" }},
            url: "http://localhost/demo",
            status: 346,
            statusText: "production",
          }});
          const cloneCopy = clonedResponse.clone();
          const responseEnumerable = [];
          for (const key in new Response()) responseEnumerable.push(key);
          results.response = {{
            tag: String(new Response()),
            enumerableForIn: responseEnumerable.sort(),
            headers: response.headers.get("a"),
            text: await response.text(),
            jsonValue: await new Response('{{"a":1}}').json(),
            bufferText: (await new Response("a=1").buffer()).toString(),
            blob: await (async () => {{
              const blob = await new Response("a=1", {{ headers: {{ "Content-Type": "text/plain" }} }}).blob();
              return {{ size: blob.size, type: blob.type }};
            }})(),
            clone: {{
              values: await Promise.all([cloneCopy.text(), clonedResponse.text()]),
              headers: cloneCopy.headers.get("a"),
              url: cloneCopy.url,
              status: cloneCopy.status,
              statusText: cloneCopy.statusText,
              ok: cloneCopy.ok,
              sameBody: cloneCopy.body === clonedResponse.body,
            }},
            defaults: {{
              emptyBody: (await new Response().text()),
              defaultStatus: new Response(null).status,
              defaultUrl: new Response().url,
            }},
            trailerKeys: Array.from((await new Response(null, {{
              trailer: Headers.createHeadersLenient({{ "X-Node-Fetch": "hello world!" }}),
            }}).trailer).keys()),
          }};

          console.log(JSON.stringify(results));
        }})().catch((error) => {{
          console.log(JSON.stringify({{
            ok: false,
            name: error && error.name ? error.name : null,
            message: error && error.message ? error.message : String(error),
            code: error && error.code ? error.code : null,
            type: error && error.type ? error.type : null,
          }}));
        }});
        "#,
        body_require = serde_json::to_string(body_require).expect("quoted body require"),
        response_require =
            serde_json::to_string(response_require).expect("quoted response require"),
        headers_require = serde_json::to_string(headers_require).expect("quoted headers require"),
        blob_require = serde_json::to_string(blob_require).expect("quoted blob require"),
        minipass_require =
            serde_json::to_string(minipass_require).expect("quoted minipass require"),
    )
}

#[tokio::test]
async fn minipass_fetch_body_and_response_contract_match_real_node() {
    if !node_compat_support::real_node_available() {
        return;
    }
    let Some(npm_root) = npm_cli::npm_cli_root() else {
        eprintln!(
            "skipping minipass-fetch body/response contract because npm/cli root is unavailable"
        );
        return;
    };

    let sandbox_source = render_body_response_source(
        "/workspace/npm/node_modules/minipass-fetch/lib/body.js",
        "/workspace/npm/node_modules/minipass-fetch/lib/response.js",
        "/workspace/npm/node_modules/minipass-fetch/lib/headers.js",
        "/workspace/npm/node_modules/minipass-fetch/lib/blob.js",
        "/workspace/npm/node_modules/minipass/dist/commonjs/index.js",
    );
    let real_source = render_body_response_source(
        &format!("{npm_root}/node_modules/minipass-fetch/lib/body.js"),
        &format!("{npm_root}/node_modules/minipass-fetch/lib/response.js"),
        &format!("{npm_root}/node_modules/minipass-fetch/lib/headers.js"),
        &format!("{npm_root}/node_modules/minipass-fetch/lib/blob.js"),
        &format!("{npm_root}/node_modules/minipass/dist/commonjs/index.js"),
    );

    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(463, 143).await else {
        eprintln!(
            "skipping minipass-fetch body/response contract because npm/cli session is unavailable"
        );
        return;
    };
    let sandbox = sandbox_stdout_json(
        npm_cli::run_inline_node_script(
            &session,
            npm_cli::SANDBOX_PROJECT_ROOT,
            "/workspace/project/.terrace/minipass-fetch-body-response-contract.cjs",
            &sandbox_source,
            &[],
        )
        .await
        .expect("sandbox minipass-fetch body/response contract"),
    );
    let real = parse_stdout_json(
        &node_compat_support::exec_real_node_eval(&real_source)
            .expect("real node minipass-fetch body/response contract")
            .stdout,
    );

    assert_eq!(sandbox, real);
}
