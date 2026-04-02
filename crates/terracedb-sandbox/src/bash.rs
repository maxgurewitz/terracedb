use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use terracedb_vfs::{
    CompletedToolRunOutcome, CreateOptions, DirEntry, FileKind, MkdirOptions, Stats,
};

use crate::{
    CapabilityCallRequest, DeterministicTypeScriptService, PackageInstallRequest, SandboxError,
    SandboxFilesystemShim, SandboxSession, SandboxShellCommand, SandboxShellCommandTarget,
    TypeCheckRequest, TypeScriptService, session::record_completed_tool_run,
};

pub const TERRACE_BASH_SESSION_STATE_PATH: &str = "/.terrace/tools/bash/session.json";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashRequest {
    pub command: String,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashReport {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BashSessionState {
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub last_exit_code: i32,
    #[serde(default)]
    pub history: Vec<String>,
}

#[derive(Clone)]
pub struct JustBashFilesystemAdapter {
    filesystem: Arc<dyn SandboxFilesystemShim>,
    cwd: String,
}

impl JustBashFilesystemAdapter {
    pub fn new(filesystem: Arc<dyn SandboxFilesystemShim>, cwd: impl Into<String>) -> Self {
        Self {
            filesystem,
            cwd: cwd.into(),
        }
    }

    pub fn cwd(&self) -> &str {
        &self.cwd
    }

    pub fn resolve_path(&self, path: &str) -> Result<String, SandboxError> {
        normalize_shell_path(&self.cwd, path)
    }

    pub async fn stat(&self, path: &str) -> Result<Option<Stats>, SandboxError> {
        let path = self.resolve_path(path)?;
        self.filesystem.stat(&path).await
    }

    pub async fn read_text(&self, path: &str) -> Result<Option<String>, SandboxError> {
        let path = self.resolve_path(path)?;
        match self.filesystem.read_file(&path).await? {
            Some(bytes) => {
                String::from_utf8(bytes)
                    .map(Some)
                    .map_err(|error| SandboxError::Service {
                        service: "bash",
                        message: format!("{path} is not valid utf-8: {error}"),
                    })
            }
            None => Ok(None),
        }
    }

    pub async fn write_text(
        &self,
        path: &str,
        contents: String,
        append: bool,
    ) -> Result<(), SandboxError> {
        let path = self.resolve_path(path)?;
        let payload = if append {
            let mut existing = self.filesystem.read_file(&path).await?.unwrap_or_default();
            existing.extend_from_slice(contents.as_bytes());
            existing
        } else {
            contents.into_bytes()
        };
        self.filesystem
            .write_file(
                &path,
                payload,
                CreateOptions {
                    create_parents: true,
                    overwrite: true,
                    ..Default::default()
                },
            )
            .await
    }

    pub async fn mkdir_all(&self, path: &str) -> Result<(), SandboxError> {
        let path = self.resolve_path(path)?;
        self.filesystem
            .mkdir(
                &path,
                MkdirOptions {
                    recursive: true,
                    ..Default::default()
                },
            )
            .await
    }

    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, SandboxError> {
        let path = self.resolve_path(path)?;
        self.filesystem.readdir(&path).await
    }
}

#[async_trait]
pub trait BashService: Send + Sync {
    fn name(&self) -> &str;
    async fn run(
        &self,
        session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError>;
}

#[derive(Clone)]
pub struct DeterministicBashService {
    name: String,
    typescript: Arc<dyn TypeScriptService>,
}

impl fmt::Debug for DeterministicBashService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeterministicBashService")
            .field("name", &self.name)
            .field("typescript", &self.typescript.name())
            .finish()
    }
}

#[derive(Default)]
struct CommandOutcome {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

impl DeterministicBashService {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            typescript: Arc::new(DeterministicTypeScriptService::default()),
        }
    }

    pub fn with_typescript_service(mut self, typescript: Arc<dyn TypeScriptService>) -> Self {
        self.typescript = typescript;
        self
    }

    async fn run_inner(
        &self,
        session: &SandboxSession,
        request: &BashRequest,
    ) -> Result<BashReport, SandboxError> {
        let mut state = load_bash_state(session, request).await?;
        let mut stdout = String::new();
        let mut stderr = String::new();
        let mut exit_code = 0;

        for fragment in split_command_sequence(&request.command)? {
            let outcome = self
                .execute_fragment(session, &mut state, &fragment)
                .await?;
            stdout.push_str(&outcome.stdout);
            stderr.push_str(&outcome.stderr);
            exit_code = outcome.exit_code;
            if exit_code != 0 {
                break;
            }
        }

        state.last_exit_code = exit_code;
        state.history.push(request.command.clone());
        if state.history.len() > 64 {
            state.history.remove(0);
        }
        persist_bash_state(session, &state).await?;

        Ok(BashReport {
            exit_code,
            stdout,
            stderr,
            cwd: state.cwd,
            env: state.env,
        })
    }

    async fn execute_fragment(
        &self,
        session: &SandboxSession,
        state: &mut BashSessionState,
        fragment: &str,
    ) -> Result<CommandOutcome, SandboxError> {
        let tokens = tokenize(fragment)?;
        if tokens.is_empty() {
            return Ok(CommandOutcome::default());
        }
        let adapter = JustBashFilesystemAdapter::new(session.filesystem(), state.cwd.clone());

        Ok(match tokens[0].as_str() {
            "pwd" => CommandOutcome {
                exit_code: 0,
                stdout: format!("{}\n", state.cwd),
                stderr: String::new(),
            },
            "cd" => self.cd_command(session, state, &adapter, &tokens).await,
            "mkdir" => mkdir_command(&adapter, &tokens).await,
            "cat" => cat_command(&adapter, &tokens).await,
            "printf" => printf_command(state, &adapter, &tokens).await,
            "echo" => echo_command(state, &tokens),
            "ls" => ls_command(&adapter, &tokens).await,
            "export" => export_command(state, &tokens),
            "npm" => self.npm_command(session, &tokens).await,
            "tsc" => self.tsc_command(session, state, &tokens).await,
            command => match shell_bridge_command(session, &adapter, &tokens).await? {
                Some(outcome) => outcome,
                None => shell_failure(
                    127,
                    String::new(),
                    format!("unsupported sandbox command: {command}\n"),
                ),
            },
        })
    }

    async fn cd_command(
        &self,
        session: &SandboxSession,
        state: &mut BashSessionState,
        adapter: &JustBashFilesystemAdapter,
        tokens: &[String],
    ) -> CommandOutcome {
        let target = if let Some(target) = tokens.get(1) {
            target.clone()
        } else {
            session.info().await.workspace_root
        };
        let resolved = match adapter.resolve_path(&target) {
            Ok(path) => path,
            Err(error) => return shell_failure(1, String::new(), format!("{error}\n")),
        };
        match adapter.stat(&target).await {
            Ok(Some(stats)) if stats.kind == FileKind::Directory => {
                state.cwd = resolved;
                CommandOutcome::default()
            }
            Ok(Some(_)) => {
                shell_failure(1, String::new(), format!("cd: {target}: not a directory\n"))
            }
            Ok(None) => shell_failure(1, String::new(), format!("cd: {target}: not found\n")),
            Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
        }
    }

    async fn npm_command(&self, session: &SandboxSession, tokens: &[String]) -> CommandOutcome {
        if tokens.get(1).map(String::as_str) != Some("install") || tokens.len() < 3 {
            return shell_failure(
                1,
                String::new(),
                "supported syntax is `npm install <package...>`\n".to_string(),
            );
        }
        match session
            .install_packages(PackageInstallRequest {
                packages: tokens[2..].to_vec(),
                materialize_compatibility_view: true,
            })
            .await
        {
            Ok(report) => CommandOutcome {
                exit_code: 0,
                stdout: format!("installed {}\n", report.packages.join(", ")),
                stderr: String::new(),
            },
            Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
        }
    }

    async fn tsc_command(
        &self,
        session: &SandboxSession,
        state: &BashSessionState,
        tokens: &[String],
    ) -> CommandOutcome {
        let mut emit = false;
        let mut paths = Vec::new();
        for token in &tokens[1..] {
            match token.as_str() {
                "emit" | "--emit" => emit = true,
                "check" | "--noEmit" => emit = false,
                value => match normalize_shell_path(&state.cwd, value) {
                    Ok(path) => paths.push(path),
                    Err(error) => return shell_failure(1, String::new(), format!("{error}\n")),
                },
            }
        }

        if paths.is_empty() {
            paths.push(format!("{}/index.ts", state.cwd.trim_end_matches('/')));
        }

        let request = TypeCheckRequest {
            roots: paths,
            ..Default::default()
        };
        if emit {
            match self.typescript.emit(session, request).await {
                Ok(report) => CommandOutcome {
                    exit_code: 0,
                    stdout: render_lines(&report.emitted_files),
                    stderr: String::new(),
                },
                Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
            }
        } else {
            match self.typescript.check(session, request).await {
                Ok(report) if report.diagnostics.is_empty() => CommandOutcome {
                    exit_code: 0,
                    stdout: "0 errors\n".to_string(),
                    stderr: String::new(),
                },
                Ok(report) => CommandOutcome {
                    exit_code: 2,
                    stdout: String::new(),
                    stderr: render_lines(
                        &report
                            .diagnostics
                            .into_iter()
                            .map(|diagnostic| {
                                format!(
                                    "{} {} {}",
                                    diagnostic.path,
                                    diagnostic.code.unwrap_or_else(|| "TS0000".to_string()),
                                    diagnostic.message
                                )
                            })
                            .collect::<Vec<_>>(),
                    ),
                },
                Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
            }
        }
    }
}

async fn shell_bridge_command(
    session: &SandboxSession,
    adapter: &JustBashFilesystemAdapter,
    tokens: &[String],
) -> Result<Option<CommandOutcome>, SandboxError> {
    let mut commands = available_shell_commands(session)
        .await
        .into_iter()
        .filter(|command| command.descriptor.command_name == tokens[0])
        .collect::<Vec<_>>();
    if commands.is_empty() {
        return Ok(None);
    }
    commands.sort_by(|left, right| {
        render_shell_command_prefix(left).cmp(&render_shell_command_prefix(right))
    });

    if tokens.len() == 1 || is_help_flag(tokens[1].as_str()) {
        return Ok(Some(shell_bridge_root_help(&commands)));
    }

    let command = match commands
        .iter()
        .filter(|command| shell_command_matches(tokens, command))
        .max_by_key(|command| command.descriptor.argv.len())
    {
        Some(command) => command,
        None => {
            let available = commands
                .iter()
                .map(render_shell_command_prefix)
                .collect::<Vec<_>>();
            return Ok(Some(shell_bridge_error(
                &tokens[0],
                None,
                None,
                (
                    "binding_not_available",
                    format!("{} is not available in this session", tokens[1]),
                    BTreeMap::from([("available_bindings".to_string(), json!(available))]),
                    69,
                    false,
                ),
            )));
        }
    };

    let method_index = 1 + command.descriptor.argv.len();
    if tokens.len() <= method_index || is_help_flag(tokens[method_index].as_str()) {
        return Ok(Some(shell_bridge_binding_help(command)));
    }

    let method_name = &tokens[method_index];
    let Some(method) = command
        .methods
        .iter()
        .find(|method| method.name == *method_name)
    else {
        return Ok(Some(shell_bridge_error(
            &tokens[0],
            Some(command),
            Some(method_name),
            (
                "method_not_available",
                format!(
                    "method {method_name} is not available for {}",
                    render_shell_command_prefix(command)
                ),
                BTreeMap::new(),
                69,
                false,
            ),
        )));
    };

    let args = &tokens[method_index + 1..];
    if args.len() == 1 && is_help_flag(args[0].as_str()) {
        return Ok(Some(shell_bridge_method_help(command, method)));
    }

    let invocation = match parse_shell_bridge_invocation(command, method, adapter, args).await {
        Ok(invocation) => invocation,
        Err(outcome) => return Ok(Some(outcome)),
    };
    let request = CapabilityCallRequest {
        specifier: capability_specifier_for_command(command).ok_or_else(|| {
            SandboxError::Service {
                service: "bash",
                message: "shell command target is not yet supported".to_string(),
            }
        })?,
        method: method.name.clone(),
        args: invocation.input.into_iter().collect(),
    };

    Ok(Some(match session.invoke_capability(request).await {
        Ok(result) => shell_bridge_success(
            command,
            &method.name,
            result.value,
            result.metadata,
            invocation.compact,
        ),
        Err(error) => {
            shell_bridge_error_from_invocation(command, &method.name, error, invocation.compact)
        }
    }))
}

impl Default for DeterministicBashService {
    fn default() -> Self {
        Self::new("deterministic-bash")
    }
}

#[async_trait]
impl BashService for DeterministicBashService {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(
        &self,
        session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError> {
        if session.execution_policy().await.is_some() {
            return self.run_inner(session, &request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.run_inner(session, &request).await {
            Ok(report) => {
                record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.bash.exec",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await?;
                Ok(report)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    session.volume().as_ref(),
                    "sandbox.bash.exec",
                    params,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }
}

struct ShellBridgeInvocation {
    compact: bool,
    input: Option<JsonValue>,
}

type ShellBridgeErrorDetails<'a> = (&'a str, String, BTreeMap<String, JsonValue>, i32, bool);

async fn available_shell_commands(session: &SandboxSession) -> Vec<SandboxShellCommand> {
    let allowed = session.info().await.provenance.capabilities;
    session
        .capability_registry()
        .shell_commands()
        .into_iter()
        .filter(|command| {
            capability_specifier_for_command(command)
                .is_some_and(|specifier| allowed.contains(&specifier))
        })
        .collect()
}

async fn parse_shell_bridge_invocation(
    command: &SandboxShellCommand,
    method: &crate::SandboxCapabilityMethod,
    adapter: &JustBashFilesystemAdapter,
    tokens: &[String],
) -> Result<ShellBridgeInvocation, CommandOutcome> {
    let mut compact = false;
    let mut input = None;
    let mut index = 0usize;

    while index < tokens.len() {
        match tokens[index].as_str() {
            "--compact" => {
                compact = true;
                index += 1;
            }
            "--input" => {
                let Some(raw) = tokens.get(index + 1) else {
                    return Err(shell_bridge_error(
                        &command.descriptor.command_name,
                        Some(command),
                        Some(&method.name),
                        (
                            "usage_error",
                            "--input requires a JSON argument".to_string(),
                            BTreeMap::new(),
                            64,
                            compact,
                        ),
                    ));
                };
                if input
                    .replace(parse_shell_json_input(raw, compact, command, &method.name)?)
                    .is_some()
                {
                    return Err(shell_bridge_error(
                        &command.descriptor.command_name,
                        Some(command),
                        Some(&method.name),
                        (
                            "usage_error",
                            "provide only one JSON input value".to_string(),
                            BTreeMap::new(),
                            64,
                            compact,
                        ),
                    ));
                }
                index += 2;
            }
            "--input-file" => {
                let Some(path) = tokens.get(index + 1) else {
                    return Err(shell_bridge_error(
                        &command.descriptor.command_name,
                        Some(command),
                        Some(&method.name),
                        (
                            "usage_error",
                            "--input-file requires a path".to_string(),
                            BTreeMap::new(),
                            64,
                            compact,
                        ),
                    ));
                };
                if input
                    .replace(
                        read_shell_json_input_file(adapter, path, compact, command, &method.name)
                            .await?,
                    )
                    .is_some()
                {
                    return Err(shell_bridge_error(
                        &command.descriptor.command_name,
                        Some(command),
                        Some(&method.name),
                        (
                            "usage_error",
                            "provide only one JSON input value".to_string(),
                            BTreeMap::new(),
                            64,
                            compact,
                        ),
                    ));
                }
                index += 2;
            }
            flag if flag.starts_with("--") => {
                return Err(shell_bridge_error(
                    &command.descriptor.command_name,
                    Some(command),
                    Some(&method.name),
                    (
                        "usage_error",
                        format!("unknown option: {flag}"),
                        BTreeMap::new(),
                        64,
                        compact,
                    ),
                ));
            }
            raw => {
                if input
                    .replace(parse_shell_json_input(raw, compact, command, &method.name)?)
                    .is_some()
                {
                    return Err(shell_bridge_error(
                        &command.descriptor.command_name,
                        Some(command),
                        Some(&method.name),
                        (
                            "usage_error",
                            "provide only one JSON input value".to_string(),
                            BTreeMap::new(),
                            64,
                            compact,
                        ),
                    ));
                }
                index += 1;
            }
        }
    }

    if method.min_args > 1 || method.max_args > 1 {
        return Err(shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(&method.name),
            (
                "unsupported_signature",
                format!(
                    "{} currently supports only methods with zero or one JSON argument",
                    render_shell_command_prefix(command)
                ),
                BTreeMap::from([
                    ("min_args".to_string(), json!(method.min_args)),
                    ("max_args".to_string(), json!(method.max_args)),
                ]),
                64,
                compact,
            ),
        ));
    }
    if method.min_args == 0 && method.max_args == 0 && input.is_some() {
        return Err(shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(&method.name),
            (
                "usage_error",
                format!("{} does not accept a JSON input value", method.name),
                BTreeMap::new(),
                64,
                compact,
            ),
        ));
    }
    if method.requires_args() && input.is_none() {
        return Err(shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(&method.name),
            (
                "usage_error",
                format!("{} requires one JSON input value", method.name),
                BTreeMap::new(),
                64,
                compact,
            ),
        ));
    }

    Ok(ShellBridgeInvocation { compact, input })
}

fn parse_shell_json_input(
    raw: &str,
    compact: bool,
    command: &SandboxShellCommand,
    method_name: &str,
) -> Result<JsonValue, CommandOutcome> {
    serde_json::from_str(raw).map_err(|error| {
        shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(method_name),
            (
                "invalid_json_input",
                format!("invalid JSON input: {error}"),
                BTreeMap::new(),
                65,
                compact,
            ),
        )
    })
}

async fn read_shell_json_input_file(
    adapter: &JustBashFilesystemAdapter,
    path: &str,
    compact: bool,
    command: &SandboxShellCommand,
    method_name: &str,
) -> Result<JsonValue, CommandOutcome> {
    let Some(contents) = adapter.read_text(path).await.map_err(|error| {
        shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(method_name),
            (
                "input_file_error",
                error.to_string(),
                BTreeMap::from([("path".to_string(), json!(path))]),
                66,
                compact,
            ),
        )
    })?
    else {
        return Err(shell_bridge_error(
            &command.descriptor.command_name,
            Some(command),
            Some(method_name),
            (
                "input_file_missing",
                format!("JSON input file not found: {path}"),
                BTreeMap::from([("path".to_string(), json!(path))]),
                66,
                compact,
            ),
        ));
    };
    parse_shell_json_input(&contents, compact, command, method_name)
}

fn shell_bridge_root_help(commands: &[SandboxShellCommand]) -> CommandOutcome {
    let command_name = &commands[0].descriptor.command_name;
    let mut lines = vec![
        format!("{command_name} exposes manifest-granted host bindings as JSON commands."),
        String::new(),
        "Usage:".to_string(),
        format!(
            "  {command_name} <binding> <method> [--input <json> | --input-file <path>] [--compact]"
        ),
        format!("  {command_name} <binding> --help"),
        String::new(),
        "Available bindings:".to_string(),
    ];
    for command in commands {
        lines.push(format!(
            "  {:<24} {}",
            render_shell_command_prefix(command),
            command
                .description
                .as_deref()
                .unwrap_or("No description available.")
        ));
    }
    CommandOutcome {
        exit_code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    }
}

fn shell_bridge_binding_help(command: &SandboxShellCommand) -> CommandOutcome {
    let prefix = render_shell_command_prefix(command);
    let mut lines = vec![
        format!("{prefix} invokes {}", render_shell_command_target(command)),
        command
            .description
            .clone()
            .unwrap_or_else(|| "No description available.".to_string()),
        String::new(),
        "Usage:".to_string(),
        format!("  {prefix} <method> [--input <json> | --input-file <path>] [--compact]"),
        format!("  {prefix} <method> --help"),
        String::new(),
        "Methods:".to_string(),
    ];
    for method in &command.methods {
        lines.push(format!(
            "  {:<20} {}",
            method.name,
            method
                .description
                .as_deref()
                .unwrap_or("No description available.")
        ));
        if let Some(input_description) = method.input_description.as_deref() {
            lines.push(format!("    input: {input_description}"));
        }
        if let Some(input_example) = method.input_example.as_deref() {
            lines.push(format!("    example: {input_example}"));
        }
    }
    CommandOutcome {
        exit_code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    }
}

fn shell_bridge_method_help(
    command: &SandboxShellCommand,
    method: &crate::SandboxCapabilityMethod,
) -> CommandOutcome {
    let prefix = render_shell_command_prefix(command);
    let mut lines = vec![
        format!("Usage for {prefix} {}:", method.name),
        format!("  {prefix} {} --input <json> [--compact]", method.name),
        format!("  {prefix} {} --input-file <path> [--compact]", method.name),
        format!("  {prefix} {} '<json>' [--compact]", method.name),
        String::new(),
    ];
    if let Some(description) = method.description.as_deref() {
        lines.push(format!("Description: {description}"));
    }
    if method.min_args == 0 && method.max_args == 0 {
        lines.push("Input: no JSON input is accepted.".to_string());
    } else if let Some(input_description) = method.input_description.as_deref() {
        lines.push(format!("Input: {input_description}"));
    }
    if let Some(input_example) = method.input_example.as_deref() {
        lines.push(format!("Example: {input_example}"));
    }
    if let Some(output_description) = method.output_description.as_deref() {
        lines.push(format!("Output: {output_description}"));
    }
    CommandOutcome {
        exit_code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    }
}

fn shell_bridge_success(
    command: &SandboxShellCommand,
    method_name: &str,
    value: JsonValue,
    result_metadata: BTreeMap<String, JsonValue>,
    compact: bool,
) -> CommandOutcome {
    let mut metadata = command.metadata.clone();
    metadata.extend(result_metadata);
    let payload = json!({
        "ok": true,
        "command": format!("{} {}", render_shell_command_prefix(command), method_name),
        "binding": shell_binding_name(command),
        "specifier": capability_specifier_for_command(command),
        "method": method_name,
        "value": value,
        "metadata": metadata,
    });
    CommandOutcome {
        exit_code: 0,
        stdout: format!("{}\n", render_json(&payload, compact)),
        stderr: String::new(),
    }
}

fn shell_bridge_error_from_invocation(
    command: &SandboxShellCommand,
    method_name: &str,
    error: SandboxError,
    compact: bool,
) -> CommandOutcome {
    let mut metadata = command.metadata.clone();
    let (kind, message, exit_code) = match &error {
        SandboxError::CapabilityDenied { .. } => ("capability_denied", error.to_string(), 69),
        SandboxError::CapabilityUnavailable { .. } => {
            ("capability_unavailable", error.to_string(), 69)
        }
        SandboxError::CapabilityMethodNotFound { .. } => {
            ("method_not_available", error.to_string(), 69)
        }
        SandboxError::Service { service, message } if *service == "capabilities" => {
            if let Some(policy_outcome) = parse_policy_outcome(message) {
                metadata.insert("policy_outcome".to_string(), json!(policy_outcome));
                ("host_enforcement_denied", message.clone(), 70)
            } else {
                ("host_invoke_error", message.clone(), 70)
            }
        }
        SandboxError::Service { service, message } => {
            metadata.insert("service".to_string(), json!(service));
            ("service_error", message.clone(), 70)
        }
        _ => ("shell_bridge_error", error.to_string(), 70),
    };

    shell_bridge_error(
        &command.descriptor.command_name,
        Some(command),
        Some(method_name),
        (kind, message, metadata, exit_code, compact),
    )
}

fn shell_bridge_error(
    command_name: &str,
    command: Option<&SandboxShellCommand>,
    method_name: Option<&str>,
    details: ShellBridgeErrorDetails<'_>,
) -> CommandOutcome {
    let (kind, message, metadata, exit_code, compact) = details;
    let payload = json!({
        "ok": false,
        "command": command
            .map(|command| {
                method_name.map_or_else(
                    || render_shell_command_prefix(command),
                    |method_name| format!("{} {}", render_shell_command_prefix(command), method_name),
                )
            })
            .unwrap_or_else(|| command_name.to_string()),
        "binding": command.map(shell_binding_name),
        "specifier": command.and_then(capability_specifier_for_command),
        "method": method_name,
        "error": {
            "kind": kind,
            "message": message,
        },
        "metadata": metadata,
    });
    CommandOutcome {
        exit_code,
        stdout: String::new(),
        stderr: format!("{}\n", render_json(&payload, compact)),
    }
}

fn shell_command_matches(tokens: &[String], command: &SandboxShellCommand) -> bool {
    let prefix_len = command.descriptor.argv.len();
    tokens.len() > prefix_len
        && tokens[0] == command.descriptor.command_name
        && tokens[1..=prefix_len]
            .iter()
            .zip(command.descriptor.argv.iter())
            .all(|(token, expected)| token == expected)
}

fn capability_specifier_for_command(command: &SandboxShellCommand) -> Option<String> {
    match &command.target {
        SandboxShellCommandTarget::Capability { specifier } => Some(specifier.clone()),
    }
}

fn render_shell_command_prefix(command: &SandboxShellCommand) -> String {
    if command.descriptor.argv.is_empty() {
        command.descriptor.command_name.clone()
    } else {
        format!(
            "{} {}",
            command.descriptor.command_name,
            command.descriptor.argv.join(" ")
        )
    }
}

fn render_shell_command_target(command: &SandboxShellCommand) -> String {
    match &command.target {
        SandboxShellCommandTarget::Capability { specifier } => specifier.clone(),
    }
}

fn shell_binding_name(command: &SandboxShellCommand) -> String {
    command
        .metadata
        .get("binding_name")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .or_else(|| command.descriptor.argv.last().cloned())
        .unwrap_or_else(|| command.descriptor.command_name.clone())
}

fn parse_policy_outcome(message: &str) -> Option<String> {
    let outcome = message.strip_prefix("policy ")?;
    Some(outcome.split(':').next()?.trim().to_ascii_lowercase())
}

fn render_json(value: &JsonValue, compact: bool) -> String {
    if compact {
        serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
    } else {
        serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".to_string())
    }
}

fn is_help_flag(value: &str) -> bool {
    matches!(value, "--help" | "-h" | "help")
}

async fn load_bash_state(
    session: &SandboxSession,
    request: &BashRequest,
) -> Result<BashSessionState, SandboxError> {
    let info = session.info().await;
    let mut state = match session
        .filesystem()
        .read_file(TERRACE_BASH_SESSION_STATE_PATH)
        .await?
    {
        Some(bytes) => serde_json::from_slice::<BashSessionState>(&bytes)?,
        None => BashSessionState {
            cwd: info.workspace_root.clone(),
            ..Default::default()
        },
    };
    if state.cwd.is_empty() {
        state.cwd = info.workspace_root;
    }
    if !request.cwd.is_empty() {
        state.cwd = normalize_shell_path(&state.cwd, &request.cwd)?;
    }
    for (key, value) in &request.env {
        state.env.insert(key.clone(), value.clone());
    }
    Ok(state)
}

async fn persist_bash_state(
    session: &SandboxSession,
    state: &BashSessionState,
) -> Result<(), SandboxError> {
    session
        .filesystem()
        .write_file(
            TERRACE_BASH_SESSION_STATE_PATH,
            serde_json::to_vec_pretty(state)?,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
}

async fn mkdir_command(adapter: &JustBashFilesystemAdapter, tokens: &[String]) -> CommandOutcome {
    let mut recursive = false;
    let mut paths = Vec::new();
    for token in &tokens[1..] {
        if token == "-p" {
            recursive = true;
        } else {
            paths.push(token);
        }
    }
    if paths.is_empty() {
        return shell_failure(1, String::new(), "mkdir: missing path\n".to_string());
    }
    for path in paths {
        let result = if recursive {
            adapter.mkdir_all(path).await
        } else {
            match adapter.resolve_path(path) {
                Ok(path) => {
                    adapter
                        .filesystem
                        .mkdir(&path, MkdirOptions::default())
                        .await
                }
                Err(error) => Err(error),
            }
        };
        if let Err(error) = result {
            return shell_failure(1, String::new(), format!("{error}\n"));
        }
    }
    CommandOutcome::default()
}

async fn cat_command(adapter: &JustBashFilesystemAdapter, tokens: &[String]) -> CommandOutcome {
    if tokens.len() < 2 {
        return shell_failure(1, String::new(), "cat: missing path\n".to_string());
    }
    let mut stdout = String::new();
    for path in &tokens[1..] {
        match adapter.read_text(path).await {
            Ok(Some(contents)) => stdout.push_str(&contents),
            Ok(None) => {
                return shell_failure(1, String::new(), format!("cat: {path}: not found\n"));
            }
            Err(error) => return shell_failure(1, String::new(), format!("{error}\n")),
        }
    }
    CommandOutcome {
        exit_code: 0,
        stdout,
        stderr: String::new(),
    }
}

async fn printf_command(
    state: &BashSessionState,
    adapter: &JustBashFilesystemAdapter,
    tokens: &[String],
) -> CommandOutcome {
    if tokens.len() < 2 {
        return shell_failure(1, String::new(), "printf: missing payload\n".to_string());
    }

    let mut redirect = None;
    let mut payload_tokens = Vec::new();
    let mut iter = tokens[1..].iter().peekable();
    while let Some(token) = iter.next() {
        if token == ">" || token == ">>" {
            redirect = iter.next().map(|path| (token.as_str(), path.clone()));
            break;
        }
        payload_tokens.push(token.clone());
    }

    let payload = decode_printf_escapes(&expand_env(&payload_tokens.join(" "), state));
    if let Some((mode, path)) = redirect {
        let append = mode == ">>";
        match adapter.write_text(&path, payload, append).await {
            Ok(()) => CommandOutcome::default(),
            Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
        }
    } else {
        CommandOutcome {
            exit_code: 0,
            stdout: payload,
            stderr: String::new(),
        }
    }
}

fn echo_command(state: &BashSessionState, tokens: &[String]) -> CommandOutcome {
    CommandOutcome {
        exit_code: 0,
        stdout: format!("{}\n", expand_env(&tokens[1..].join(" "), state)),
        stderr: String::new(),
    }
}

async fn ls_command(adapter: &JustBashFilesystemAdapter, tokens: &[String]) -> CommandOutcome {
    let path = tokens.get(1).map(String::as_str).unwrap_or(".");
    match adapter.readdir(path).await {
        Ok(mut entries) => {
            entries.sort_by(|left, right| left.name.cmp(&right.name));
            let lines = entries
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>();
            CommandOutcome {
                exit_code: 0,
                stdout: render_lines(&lines),
                stderr: String::new(),
            }
        }
        Err(error) => shell_failure(1, String::new(), format!("{error}\n")),
    }
}

fn export_command(state: &mut BashSessionState, tokens: &[String]) -> CommandOutcome {
    if tokens.len() < 2 {
        return shell_failure(1, String::new(), "export: missing assignment\n".to_string());
    }
    for assignment in &tokens[1..] {
        let Some((key, value)) = assignment.split_once('=') else {
            return shell_failure(
                1,
                String::new(),
                format!("export: invalid assignment `{assignment}`\n"),
            );
        };
        state
            .env
            .insert(key.to_string(), expand_env(value, state).to_string());
    }
    CommandOutcome::default()
}

fn shell_failure(exit_code: i32, stdout: String, stderr: String) -> CommandOutcome {
    CommandOutcome {
        exit_code,
        stdout,
        stderr,
    }
}

fn render_lines(lines: &[String]) -> String {
    if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    }
}

fn expand_env(input: &str, state: &BashSessionState) -> String {
    let mut output = String::new();
    let chars = input.chars().collect::<Vec<_>>();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '$' {
            output.push(chars[index]);
            index += 1;
            continue;
        }

        let start = index + 1;
        let mut end = start;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        if end == start {
            output.push('$');
            index += 1;
            continue;
        }
        let name = chars[start..end].iter().collect::<String>();
        if name == "PWD" {
            output.push_str(&state.cwd);
        } else if let Some(value) = state.env.get(&name) {
            output.push_str(value);
        }
        index = end;
    }
    output
}

fn decode_printf_escapes(input: &str) -> String {
    let mut output = String::new();
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('n') => output.push('\n'),
            Some('t') => output.push('\t'),
            Some('r') => output.push('\r'),
            Some('\\') => output.push('\\'),
            Some('\'') => output.push('\''),
            Some('"') => output.push('"'),
            Some(other) => {
                output.push('\\');
                output.push(other);
            }
            None => output.push('\\'),
        }
    }
    output
}

fn split_command_sequence(command: &str) -> Result<Vec<String>, SandboxError> {
    let mut commands = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        match quote {
            Some(active) => {
                if ch == '\\' {
                    current.push(ch);
                    if let Some(next) = chars.next() {
                        current.push(next);
                    }
                    continue;
                }
                if ch == active {
                    quote = None;
                }
                current.push(ch);
            }
            None => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    current.push(ch);
                }
                '&' if chars.peek() == Some(&'&') => {
                    chars.next();
                    let trimmed = current.trim();
                    if !trimmed.is_empty() {
                        commands.push(trimmed.to_string());
                    }
                    current.clear();
                }
                ';' => {
                    let trimmed = current.trim();
                    if !trimmed.is_empty() {
                        commands.push(trimmed.to_string());
                    }
                    current.clear();
                }
                _ => current.push(ch),
            },
        }
    }

    if quote.is_some() {
        return Err(SandboxError::Service {
            service: "bash",
            message: "unterminated quoted string".to_string(),
        });
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        commands.push(trimmed.to_string());
    }
    Ok(commands)
}

fn tokenize(command: &str) -> Result<Vec<String>, SandboxError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    let mut chars = command.chars().peekable();

    while let Some(ch) = chars.next() {
        match quote {
            Some(active) => {
                if ch == active {
                    quote = None;
                } else {
                    current.push(ch);
                }
            }
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                ' ' | '\t' if !current.is_empty() => {
                    tokens.push(std::mem::take(&mut current));
                }
                ' ' | '\t' => {}
                '>' => {
                    if !current.is_empty() {
                        tokens.push(std::mem::take(&mut current));
                    }
                    if chars.peek() == Some(&'>') {
                        chars.next();
                        tokens.push(">>".to_string());
                    } else {
                        tokens.push(">".to_string());
                    }
                }
                _ => current.push(ch),
            },
        }
    }

    if quote.is_some() {
        return Err(SandboxError::Service {
            service: "bash",
            message: "unterminated quoted string".to_string(),
        });
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    Ok(tokens)
}

fn normalize_shell_path(cwd: &str, path: &str) -> Result<String, SandboxError> {
    if path.is_empty() {
        return normalize_internal_path(cwd);
    }
    if path.starts_with('/') {
        return normalize_internal_path(path);
    }
    let combined = if cwd == "/" {
        format!("/{path}")
    } else {
        format!("{cwd}/{path}")
    };
    normalize_internal_path(&combined)
}

fn normalize_internal_path(path: &str) -> Result<String, SandboxError> {
    if !path.starts_with('/') {
        return Err(SandboxError::Service {
            service: "bash",
            message: format!("path must be absolute: {path}"),
        });
    }
    let mut parts = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            value => parts.push(value),
        }
    }
    if parts.is_empty() {
        Ok("/".to_string())
    } else {
        Ok(format!("/{}", parts.join("/")))
    }
}
