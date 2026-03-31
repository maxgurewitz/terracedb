use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use terracedb_vfs::{
    CompletedToolRunOutcome, CreateOptions, DirEntry, FileKind, MkdirOptions, Stats,
};

use crate::{
    DeterministicTypeScriptService, PackageInstallRequest, SandboxError, SandboxFilesystemShim,
    SandboxSession, TypeCheckRequest, TypeScriptService, session::record_completed_tool_run,
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
            command => shell_failure(
                127,
                String::new(),
                format!("unsupported sandbox command: {command}\n"),
            ),
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
            match session.info().await.workspace_root {
                workspace_root => workspace_root,
            }
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
