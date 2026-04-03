#[path = "support/node_compat.rs"]
mod node_compat;
#[path = "support/npm_cli.rs"]
mod npm_cli;
#[path = "support/tracing.rs"]
mod tracing_support;

use serde_json::Value;

fn parse_stdout_json(result: terracedb_sandbox::SandboxExecutionResult) -> Value {
    let report = result.result.expect("node command report");
    serde_json::from_str(report["stdout"].as_str().expect("stdout string").trim())
        .expect("stdout json")
}

#[tokio::test]
async fn node_process_report_matches_basic_node_contract() {
    tracing_support::init_tracing();
    let payload = parse_stdout_json(
        node_compat::exec_node_fixture(
            r#"
            const summary = {
              directory0: process.report.directory,
              filename0: process.report.filename,
              fatal0: process.report.reportOnFatalError,
              uncaught0: process.report.reportOnUncaughtException,
              signal0: process.report.reportOnSignal,
              compact0: process.report.compact,
              excludeNetwork0: process.report.excludeNetwork,
              signalName0: process.report.signal,
            };

            process.report.directory = "/workspace/reports";
            process.report.filename = "report.json";
            process.report.reportOnFatalError = false;
            process.report.reportOnUncaughtException = false;
            process.report.reportOnSignal = false;
            process.report.compact = false;
            process.report.excludeNetwork = true;
            process.report.signal = "SIGUSR1";

            summary.directory1 = process.report.directory;
            summary.filename1 = process.report.filename;
            summary.fatal1 = process.report.reportOnFatalError;
            summary.uncaught1 = process.report.reportOnUncaughtException;
            summary.signal1 = process.report.reportOnSignal;
            summary.compact1 = process.report.compact;
            summary.excludeNetwork1 = process.report.excludeNetwork;
            summary.signalName1 = process.report.signal;
            summary.maxListeners0 = process.getMaxListeners();
            process.setMaxListeners(16);
            summary.maxListeners1 = process.getMaxListeners();
            process.on("SIGUSR1", () => {});
            summary.listenersSigusr1 = process.listeners("SIGUSR1").length;
            summary.rawListenersSigusr1 = process.rawListeners("SIGUSR1").length;
            summary.sigusr2Listeners = process.listenerCount("SIGUSR2");
            summary.sigusr1Listeners = process.listenerCount("SIGUSR1");

            const invalid = {};
            for (const [label, op] of Object.entries({
              directory: () => { process.report.directory = {}; },
              filename: () => { process.report.filename = {}; },
              excludeNetwork: () => { process.report.excludeNetwork = {}; },
              signal: () => { process.report.signal = "sigusr1"; },
            })) {
              try {
                op();
              } catch (error) {
                invalid[label] = { code: error.code, message: error.message };
              }
            }
            summary.invalid = invalid;

            const report1 = process.report.getReport();
            summary.headerPlatform = report1.header.platform;
            summary.headerArch = report1.header.arch;
            summary.glibc = report1.header.glibcVersionRuntime;
            summary.hasNetworkInterfacesWhenExcluded =
              Object.prototype.hasOwnProperty.call(report1, "networkInterfaces");

            process.report.excludeNetwork = false;
            const report2 = process.report.getReport(new Error("boom"));
            summary.hasNetworkInterfacesWhenIncluded =
              Object.prototype.hasOwnProperty.call(report2, "networkInterfaces");
            summary.errorMessage = report2.javascriptStack.message;
            summary.errorStackKind = Array.isArray(report2.javascriptStack.stack);

            console.log(JSON.stringify(summary));
            "#,
        )
        .await
        .expect("sandbox execution"),
    );

    assert_eq!(payload["directory0"].as_str(), Some(""));
    assert_eq!(payload["filename0"].as_str(), Some(""));
    assert_eq!(payload["fatal0"].as_bool(), Some(true));
    assert_eq!(payload["uncaught0"].as_bool(), Some(true));
    assert_eq!(payload["signal0"].as_bool(), Some(true));
    assert_eq!(payload["compact0"].as_bool(), Some(true));
    assert_eq!(payload["excludeNetwork0"].as_bool(), Some(false));
    assert_eq!(payload["signalName0"].as_str(), Some("SIGUSR2"));

    assert_eq!(payload["directory1"].as_str(), Some("/workspace/reports"));
    assert_eq!(payload["filename1"].as_str(), Some("report.json"));
    assert_eq!(payload["fatal1"].as_bool(), Some(false));
    assert_eq!(payload["uncaught1"].as_bool(), Some(false));
    assert_eq!(payload["signal1"].as_bool(), Some(false));
    assert_eq!(payload["compact1"].as_bool(), Some(false));
    assert_eq!(payload["excludeNetwork1"].as_bool(), Some(true));
    assert_eq!(payload["signalName1"].as_str(), Some("SIGUSR1"));
    assert_eq!(payload["maxListeners0"].as_u64(), Some(10));
    assert_eq!(payload["maxListeners1"].as_u64(), Some(16));
    assert_eq!(payload["listenersSigusr1"].as_u64(), Some(1));
    assert_eq!(payload["rawListenersSigusr1"].as_u64(), Some(1));
    assert_eq!(payload["sigusr2Listeners"].as_u64(), Some(0));
    assert_eq!(payload["sigusr1Listeners"].as_u64(), Some(1));

    assert_eq!(
        payload["invalid"]["directory"]["code"].as_str(),
        Some("ERR_INVALID_ARG_TYPE")
    );
    assert_eq!(
        payload["invalid"]["filename"]["code"].as_str(),
        Some("ERR_INVALID_ARG_TYPE")
    );
    assert_eq!(
        payload["invalid"]["excludeNetwork"]["code"].as_str(),
        Some("ERR_INVALID_ARG_TYPE")
    );
    assert_eq!(
        payload["invalid"]["signal"]["code"].as_str(),
        Some("ERR_UNKNOWN_SIGNAL")
    );

    assert_eq!(payload["headerPlatform"].as_str(), Some("linux"));
    assert_eq!(payload["headerArch"].as_str(), Some("x64"));
    assert_eq!(payload["glibc"].as_str(), Some("2.39"));
    assert_eq!(
        payload["hasNetworkInterfacesWhenExcluded"].as_bool(),
        Some(false)
    );
    assert_eq!(
        payload["hasNetworkInterfacesWhenIncluded"].as_bool(),
        Some(true)
    );
    assert_eq!(payload["errorMessage"].as_str(), Some("boom"));
    assert_eq!(payload["errorStackKind"].as_bool(), Some(true));
}

#[tokio::test]
async fn node_process_report_unblocks_npm_install_checks_current_env() {
    tracing_support::init_tracing();
    let Some((session, _vfs)) = npm_cli::open_npm_cli_session(421, 94).await else {
        eprintln!("skipping npm install checks test because npm/cli repo is unavailable");
        return;
    };

    let result = npm_cli::run_inline_node_script(
        &session,
        npm_cli::SANDBOX_PROJECT_ROOT,
        "/workspace/project/.terrace/check-current-env.mjs",
        r#"
        const currentEnvModule = await import("/workspace/npm/node_modules/npm-install-checks/lib/current-env.js");
        const currentEnv = currentEnvModule.default ?? currentEnvModule;
        console.log(JSON.stringify({
          os: currentEnv.os(),
          cpu: currentEnv.cpu(),
          libc: currentEnv.libc(currentEnv.os()),
          devEngines: currentEnv.devEngines({ npmVersion: "11.6.2" }),
        }));
        "#,
        &[],
    )
    .await
    .expect("run current-env check");

    let payload = parse_stdout_json(result);
    assert_eq!(payload["os"].as_str(), Some("linux"));
    assert_eq!(payload["cpu"].as_str(), Some("x64"));
    assert_eq!(payload["libc"].as_str(), Some("glibc"));
    assert_eq!(
        payload["devEngines"]["runtime"]["name"].as_str(),
        Some("node")
    );
    assert_eq!(
        payload["devEngines"]["runtime"]["version"].as_str(),
        Some("v24.12.0")
    );
    assert_eq!(
        payload["devEngines"]["libc"]["name"].as_str(),
        Some("glibc")
    );
}
