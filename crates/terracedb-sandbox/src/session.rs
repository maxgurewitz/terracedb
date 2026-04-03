use std::{
    collections::BTreeMap,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use terracedb::Clock;
use terracedb_capabilities::{ExecutionOperation, ExecutionPolicy};
use terracedb_git::{
    DeterministicGitHostBridge, DeterministicGitRepositoryStore, GitCheckoutReport,
    GitCheckoutRequest, GitCommitRequest, GitDiffReport, GitDiffRequest, GitHeadState,
    GitHostBridge, GitImportEntryKind, GitImportLayout, GitImportMode, GitImportRequest, GitImportSource,
    GitObjectFormat, GitOpenRequest, GitPullRequestRequest as GitBridgePullRequestRequest,
    GitPushRequest, GitRefUpdate, GitRefUpdateReport, GitReference, GitRepository,
    GitRepositoryImage, GitRepositoryPolicy, GitRepositoryProvenance, GitRepositoryStore,
    GitStatusOptions, GitStatusReport, HostGitBridge, NeverCancel, VfsGitRepositoryImage,
};
use terracedb_vfs::{
    CompletedToolRun, CompletedToolRunOutcome, CreateOptions, DirEntry, MkdirOptions,
    ReadOnlyVfsFileSystem, SnapshotOptions, Stats, ToolRunId, ToolRunStatus, VfsError, VfsKvStore,
    VfsStoreExt, Volume, VolumeConfig, VolumeSnapshot, VolumeStore,
};
use tokio::sync::Mutex;

use crate::disk::{
    HoistManifest, ManifestEntry, PreparedHoist, TreeEntry, TreeEntryData, apply_delta_to_host,
    load_hoist_manifest, materialize_snapshot_to_host, prepare_hoist, replace_vfs_tree,
    write_hoist_manifest,
};
use crate::git::git_substrate_error_to_sandbox;
use crate::routing::{
    RoutedBashService, RoutedPackageInstaller, RoutedRuntimeBackend, RoutedTypeScriptService,
    SandboxExecutionRouter,
};
use crate::types::{
    durable_remote_bridge_metadata, sanitize_git_import_source, sanitize_remote_url,
};
use crate::{
    BaseSnapshotIdentity, BashReport, BashRequest, BashService, CapabilityCallRequest,
    CapabilityCallResult, CapabilityRegistry, ConflictPolicy, DeterministicBashService,
    DeterministicPackageInstaller, DeterministicPullRequestProviderClient,
    DeterministicReadonlyViewProvider, DeterministicRuntimeBackend, DeterministicTypeScriptService,
    EjectMode, EjectReport, EjectRequest, GitRemoteImportReport, GitRemoteImportRequest,
    HoistReport, HoistRequest, HoistedSource, PackageInstallReport, PackageInstallRequest,
    PackageInstaller, PullRequestProviderClient, PullRequestReport, PullRequestRequest,
    ReadonlyViewCut, ReadonlyViewHandle, ReadonlyViewLocation, ReadonlyViewProvider,
    ReadonlyViewRequest, SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION, SandboxConfig,
    SandboxError, SandboxExecutionKind, SandboxExecutionRequest, SandboxExecutionResult,
    NodeDebugExecutionOptions,
    SandboxFilesystemShim, SandboxModuleLoader, SandboxRuntimeActor, SandboxRuntimeBackend,
    SandboxRuntimeHandle, SandboxRuntimeStateHandle, SandboxServiceBindings, SandboxSessionInfo,
    SandboxSessionProvenance, SandboxSessionState, StaticCapabilityRegistry,
    TERRACE_EXECUTION_POLICY_STATE_PATH, TERRACE_METADATA_DIR, TERRACE_NPM_COMPATIBILITY_ROOT,
    TERRACE_NPM_DIR, TERRACE_NPM_SESSION_CACHE_DIR, TERRACE_RUNTIME_CACHE_DIR,
    TERRACE_SESSION_INFO_KV_KEY, TERRACE_SESSION_METADATA_PATH, TypeCheckReport, TypeCheckRequest,
    TypeScriptEmitReport, TypeScriptService, TypeScriptTranspileReport, TypeScriptTranspileRequest,
    VfsSandboxFilesystemShim,
};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReopenSessionOptions {
    pub session_volume_id: terracedb_vfs::VolumeId,
    pub session_chunk_size: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseSessionOptions {
    pub flush: bool,
}

#[derive(Clone)]
pub struct SandboxServices {
    pub runtime: Arc<dyn SandboxRuntimeBackend>,
    pub packages: Arc<dyn PackageInstaller>,
    pub git: Arc<dyn GitRepositoryStore>,
    pub git_bridge: Arc<dyn GitHostBridge>,
    pub pull_requests: Arc<dyn PullRequestProviderClient>,
    pub readonly_views: Arc<dyn ReadonlyViewProvider>,
    pub typescript: Arc<dyn TypeScriptService>,
    pub bash: Arc<dyn BashService>,
    pub capabilities: Arc<dyn CapabilityRegistry>,
    execution_router: Option<Arc<SandboxExecutionRouter>>,
}

impl SandboxServices {
    pub fn new(
        runtime: Arc<dyn SandboxRuntimeBackend>,
        packages: Arc<dyn PackageInstaller>,
        git: Arc<dyn GitRepositoryStore>,
        pull_requests: Arc<dyn PullRequestProviderClient>,
        readonly_views: Arc<dyn ReadonlyViewProvider>,
    ) -> Self {
        let typescript: Arc<dyn TypeScriptService> =
            Arc::new(DeterministicTypeScriptService::default());
        let bash: Arc<dyn BashService> = Arc::new(
            DeterministicBashService::default().with_typescript_service(typescript.clone()),
        );
        Self {
            runtime,
            packages,
            git,
            git_bridge: Arc::new(DeterministicGitHostBridge::default()),
            pull_requests,
            readonly_views,
            typescript,
            bash,
            capabilities: Arc::new(StaticCapabilityRegistry::default()),
            execution_router: None,
        }
    }

    pub fn with_capabilities(mut self, capabilities: Arc<dyn CapabilityRegistry>) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn with_git_repository_store(mut self, git: Arc<dyn GitRepositoryStore>) -> Self {
        self.git = git;
        self
    }

    pub fn with_git_host_bridge(mut self, git_bridge: Arc<dyn GitHostBridge>) -> Self {
        self.git_bridge = git_bridge;
        self
    }

    pub fn with_typescript_service(mut self, typescript: Arc<dyn TypeScriptService>) -> Self {
        self.typescript = typescript;
        self
    }

    pub fn with_bash_service(mut self, bash: Arc<dyn BashService>) -> Self {
        self.bash = bash;
        self
    }

    pub fn deterministic() -> Self {
        Self::new(
            Arc::new(DeterministicRuntimeBackend::default()),
            Arc::new(DeterministicPackageInstaller::default()),
            Arc::new(DeterministicGitRepositoryStore::default()),
            Arc::new(DeterministicPullRequestProviderClient::default()),
            Arc::new(DeterministicReadonlyViewProvider::default()),
        )
    }

    pub fn deterministic_with_capabilities(capabilities: Arc<dyn CapabilityRegistry>) -> Self {
        Self::deterministic().with_capabilities(capabilities)
    }

    pub fn deterministic_with_host_git() -> Self {
        Self::deterministic().with_git_host_bridge(Arc::new(HostGitBridge::default()))
    }

    pub fn deterministic_with_host_git_and_capabilities(
        capabilities: Arc<dyn CapabilityRegistry>,
    ) -> Self {
        Self::deterministic_with_host_git().with_capabilities(capabilities)
    }

    pub fn with_execution_router(mut self, router: SandboxExecutionRouter) -> Self {
        let router = Arc::new(router);
        self.runtime = Arc::new(RoutedRuntimeBackend::new(
            self.runtime.clone(),
            router.clone(),
        ));
        self.packages = Arc::new(RoutedPackageInstaller::new(
            self.packages.clone(),
            router.clone(),
        ));
        self.typescript = Arc::new(RoutedTypeScriptService::new(
            self.typescript.clone(),
            router.clone(),
        ));
        self.bash = Arc::new(RoutedBashService::new(self.bash.clone(), router.clone()));
        self.execution_router = Some(router);
        self
    }

    pub fn bindings(&self) -> SandboxServiceBindings {
        SandboxServiceBindings {
            runtime_backend: self.runtime.name().to_string(),
            package_installer: self.packages.name().to_string(),
            git_repository_store: self.git.name().to_string(),
            pull_request_provider: self.pull_requests.name().to_string(),
            readonly_view_provider: self.readonly_views.name().to_string(),
            typescript_service: self.typescript.name().to_string(),
            bash_service: self.bash.name().to_string(),
            execution_bindings: BTreeMap::new(),
        }
    }

    pub fn bindings_for_policy(
        &self,
        execution_policy: Option<&ExecutionPolicy>,
    ) -> Result<SandboxServiceBindings, SandboxError> {
        let legacy = self.bindings();
        match execution_policy {
            Some(policy) => self
                .execution_router
                .as_ref()
                .ok_or(SandboxError::Service {
                    service: "execution-policy",
                    message: "sandbox execution policy requires a sandbox execution router"
                        .to_string(),
                })?
                .bindings_for_policy(policy, &legacy),
            None => Ok(legacy),
        }
    }
}

pub struct DefaultSandboxStore<S> {
    volumes: Arc<S>,
    clock: Arc<dyn Clock>,
    services: SandboxServices,
}

impl<S> DefaultSandboxStore<S> {
    pub fn new(volumes: Arc<S>, clock: Arc<dyn Clock>, services: SandboxServices) -> Self {
        Self {
            volumes,
            clock,
            services,
        }
    }

    async fn open_existing_session(
        &self,
        options: ReopenSessionOptions,
        expected_config: Option<&SandboxConfig>,
    ) -> Result<SandboxSession, SandboxError>
    where
        S: VolumeStore + 'static,
    {
        let volume = self
            .volumes
            .open_volume(session_volume_config(
                options.session_volume_id,
                options.session_chunk_size,
                false,
            ))
            .await?;
        let mut info = load_session_info(volume.as_ref()).await?;
        if let Some(config) = expected_config {
            validate_existing_session(&info, config)?;
        }
        let resolved_bindings = self
            .services
            .bindings_for_policy(info.provenance.execution_policy.as_ref())?;
        let mut changed = false;
        if info.services != resolved_bindings {
            info.services = resolved_bindings;
            changed = true;
        }
        if info.state == SandboxSessionState::Closed {
            info.state = SandboxSessionState::Open;
            info.closed_at = None;
            changed = true;
        }
        if changed {
            info.updated_at = self.clock.now();
            info.revision = info.revision.saturating_add(1);
            write_session_info_exact(volume.as_ref(), &info).await?;
        }
        let execution_counts = load_execution_counts(volume.as_ref()).await?;
        let runtime = self.services.runtime.resume_session(&info).await?;
        let params = json!({
            "mode": "reopen",
            "session_volume_id": info.session_volume_id.to_string(),
        });
        let result = json!({
            "runtime_actor_id": runtime.actor_id,
            "revision": info.revision,
        });
        record_completed_tool_run(
            volume.as_ref(),
            "sandbox.session.open",
            Some(params),
            CompletedToolRunOutcome::Success {
                result: Some(result),
            },
        )
        .await?;
        Ok(SandboxSession::new(
            volume,
            self.clock.clone(),
            self.services.clone(),
            info,
            execution_counts,
            runtime,
        ))
    }

    async fn create_session(&self, config: SandboxConfig) -> Result<SandboxSession, SandboxError>
    where
        S: VolumeStore + 'static,
    {
        config.capabilities.validate()?;
        let base = self
            .volumes
            .open_volume(VolumeConfig::new(config.base_volume_id))
            .await?;
        let overlay = self
            .volumes
            .create_overlay_from_volume(
                base,
                SnapshotOptions {
                    durable: config.durable_base,
                },
                session_volume_config(config.session_volume_id, config.session_chunk_size, true),
            )
            .await?;
        let base_snapshot = overlay.base();
        let volume: Arc<dyn Volume> = overlay;
        seed_session_layout(volume.as_ref(), &config.workspace_root).await?;
        let now = self.clock.now();
        let services = self
            .services
            .bindings_for_policy(config.execution_policy.as_ref())?;
        let info = SandboxSessionInfo {
            format_version: crate::SANDBOX_SESSION_FORMAT_VERSION,
            revision: 1,
            session_volume_id: config.session_volume_id,
            workspace_root: config.workspace_root.clone(),
            state: SandboxSessionState::Open,
            created_at: now,
            updated_at: now,
            closed_at: None,
            conflict_policy: config.conflict_policy,
            services,
            provenance: SandboxSessionProvenance {
                base_snapshot: BaseSnapshotIdentity {
                    volume_id: base_snapshot.volume_id(),
                    sequence: base_snapshot.sequence(),
                    durable: base_snapshot.durable(),
                },
                hoisted_source: config.hoisted_source.clone(),
                git: config.git_provenance.clone(),
                package_compat: config.package_compat,
                capabilities: config.capabilities.clone(),
                execution_policy: config.execution_policy.clone(),
                active_view_handles: Vec::new(),
            },
        };
        write_session_info_exact(volume.as_ref(), &info).await?;
        let execution_counts = BTreeMap::new();
        let runtime = self.services.runtime.start_session(&info).await?;
        let params = json!({
            "mode": "create",
            "base_volume_id": config.base_volume_id.to_string(),
            "durable_base": config.durable_base,
            "session_volume_id": config.session_volume_id.to_string(),
        });
        let result = json!({
            "runtime_actor_id": runtime.actor_id,
            "revision": info.revision,
            "base_sequence": info.provenance.base_snapshot.sequence.get(),
        });
        record_completed_tool_run(
            volume.as_ref(),
            "sandbox.session.open",
            Some(params),
            CompletedToolRunOutcome::Success {
                result: Some(result),
            },
        )
        .await?;
        Ok(SandboxSession::new(
            volume,
            self.clock.clone(),
            self.services.clone(),
            info,
            execution_counts,
            runtime,
        ))
    }
}

#[async_trait(?Send)]
pub trait SandboxStore: Send + Sync {
    async fn open_session(&self, config: SandboxConfig) -> Result<SandboxSession, SandboxError>;
    async fn reopen_session(
        &self,
        options: ReopenSessionOptions,
    ) -> Result<SandboxSession, SandboxError>;
}

#[async_trait(?Send)]
impl<S> SandboxStore for DefaultSandboxStore<S>
where
    S: VolumeStore + Send + Sync + 'static,
{
    async fn open_session(&self, config: SandboxConfig) -> Result<SandboxSession, SandboxError> {
        match self
            .open_existing_session(
                ReopenSessionOptions {
                    session_volume_id: config.session_volume_id,
                    session_chunk_size: config.session_chunk_size,
                },
                Some(&config),
            )
            .await
        {
            Ok(session) => Ok(session),
            Err(SandboxError::Vfs(VfsError::VolumeNotFound { .. })) => {
                self.create_session(config).await
            }
            Err(error) => Err(error),
        }
    }

    async fn reopen_session(
        &self,
        options: ReopenSessionOptions,
    ) -> Result<SandboxSession, SandboxError> {
        self.open_existing_session(options, None).await
    }
}

#[derive(Clone)]
pub struct SandboxSession {
    volume: Arc<dyn Volume>,
    clock: Arc<dyn Clock>,
    services: SandboxServices,
    info: Arc<Mutex<SandboxSessionInfo>>,
    remote_bridge_session_metadata: Arc<Mutex<BTreeMap<String, BTreeMap<String, JsonValue>>>>,
    operation_lock: Arc<Mutex<()>>,
    execution_counts: Arc<Mutex<ExecutionCountState>>,
    execution_count_persist_lock: Arc<Mutex<()>>,
    runtime: SandboxRuntimeHandle,
    runtime_actor: SandboxRuntimeActor,
}

impl SandboxSession {
    fn new(
        volume: Arc<dyn Volume>,
        clock: Arc<dyn Clock>,
        services: SandboxServices,
        info: SandboxSessionInfo,
        execution_counts: BTreeMap<ExecutionOperation, u64>,
        runtime: SandboxRuntimeHandle,
    ) -> Self {
        Self {
            volume,
            clock,
            runtime_actor: SandboxRuntimeActor::new(services.runtime.clone(), runtime.clone()),
            services,
            info: Arc::new(Mutex::new(info)),
            remote_bridge_session_metadata: Arc::new(Mutex::new(BTreeMap::new())),
            operation_lock: Arc::new(Mutex::new(())),
            execution_counts: Arc::new(Mutex::new(ExecutionCountState::new(execution_counts))),
            execution_count_persist_lock: Arc::new(Mutex::new(())),
            runtime,
        }
    }

    pub async fn info(&self) -> SandboxSessionInfo {
        self.info.lock().await.clone()
    }

    pub fn volume(&self) -> Arc<dyn Volume> {
        self.volume.clone()
    }

    pub fn runtime_handle(&self) -> SandboxRuntimeHandle {
        self.runtime.clone()
    }

    pub(crate) fn operation_lock(&self) -> Arc<Mutex<()>> {
        self.operation_lock.clone()
    }

    pub fn filesystem(&self) -> Arc<dyn SandboxFilesystemShim> {
        Arc::new(VfsSandboxFilesystemShim::new(self.volume.fs()))
    }

    pub fn capability_registry(&self) -> Arc<dyn CapabilityRegistry> {
        self.services.capabilities.clone()
    }

    pub fn git_host_bridge(&self) -> Arc<dyn GitHostBridge> {
        self.services.git_bridge.clone()
    }

    /// Attaches app-owned remote bridge metadata to this live session without persisting it.
    ///
    /// This is the intended place to register live credentials such as short-lived access tokens
    /// or provider secrets that should participate in bridge calls but must not become
    /// guest-visible sandbox provenance.
    pub async fn set_remote_bridge_session_metadata(
        &self,
        remote_url: impl Into<String>,
        metadata: BTreeMap<String, JsonValue>,
    ) {
        let remote_url = remote_url.into();
        self.remember_remote_bridge_session_metadata(&remote_url, &metadata)
            .await;
    }

    pub(crate) async fn execution_policy(&self) -> Option<ExecutionPolicy> {
        self.info().await.provenance.execution_policy
    }

    pub(crate) async fn reserve_execution_call_count(&self, operation: ExecutionOperation) -> u64 {
        let mut counts = self.execution_counts.lock().await;
        let next = counts
            .committed
            .get(&operation)
            .copied()
            .unwrap_or_default()
            .saturating_add(counts.inflight.get(&operation).copied().unwrap_or_default())
            .saturating_add(1);
        counts
            .inflight
            .entry(operation)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        next
    }

    pub(crate) async fn release_execution_call_count(&self, operation: ExecutionOperation) {
        let mut counts = self.execution_counts.lock().await;
        decrement_execution_count(&mut counts.inflight, operation);
    }

    pub(crate) async fn commit_execution_call_count(&self, operation: ExecutionOperation) -> u64 {
        let (call_count, snapshot) = {
            let mut counts = self.execution_counts.lock().await;
            decrement_execution_count(&mut counts.inflight, operation);
            let next = counts
                .committed
                .entry(operation)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            (*next, counts.committed.clone())
        };
        let _ = self.persist_execution_counts_snapshot(snapshot).await;
        call_count
    }

    async fn persist_execution_counts_snapshot(
        &self,
        snapshot: BTreeMap<ExecutionOperation, u64>,
    ) -> Result<(), SandboxError> {
        let _guard = self.execution_count_persist_lock.lock().await;
        let merged = merge_execution_counts(
            load_persisted_execution_counts(self.volume.as_ref()).await?,
            snapshot,
        );
        persist_execution_counts(self.volume.as_ref(), &merged).await
    }

    pub fn kv(&self) -> Arc<dyn VfsKvStore> {
        self.volume.kv()
    }

    pub async fn module_loader(&self) -> SandboxModuleLoader {
        let info = self.info().await;
        self.module_loader_with_state(info, self.runtime_actor.state())
            .await
    }

    pub async fn module_loader_with_state(
        &self,
        info: SandboxSessionInfo,
        state: SandboxRuntimeStateHandle,
    ) -> SandboxModuleLoader {
        SandboxModuleLoader::new(info, self.filesystem(), self.capability_registry(), state)
    }

    pub async fn snapshot_for_cut(
        &self,
        cut: ReadonlyViewCut,
    ) -> Result<Arc<dyn VolumeSnapshot>, SandboxError> {
        self.volume
            .snapshot(SnapshotOptions {
                durable: matches!(cut, ReadonlyViewCut::Durable),
            })
            .await
            .map_err(Into::into)
    }

    pub async fn readonly_fs(
        &self,
        cut: ReadonlyViewCut,
    ) -> Result<Arc<dyn ReadOnlyVfsFileSystem>, SandboxError> {
        Ok(self.snapshot_for_cut(cut).await?.fs())
    }

    pub async fn flush(&self) -> Result<(), SandboxError> {
        self.volume.flush().await.map_err(Into::into)
    }

    pub async fn exec_module(
        &self,
        specifier: impl Into<String>,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime_logged(
            "sandbox.runtime.exec_module",
            SandboxExecutionRequest::module(specifier.into()),
        )
        .await
    }

    pub async fn eval(
        &self,
        source: impl Into<String>,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime_logged(
            "sandbox.runtime.eval",
            SandboxExecutionRequest::eval(source.into()),
        )
        .await
    }

    pub async fn exec_node_command(
        &self,
        entrypoint: impl Into<String>,
        argv: Vec<String>,
        cwd: impl Into<String>,
        env: BTreeMap<String, String>,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime_logged(
            "sandbox.runtime.exec_node_command",
            SandboxExecutionRequest::node_command(entrypoint, argv, cwd, env),
        )
        .await
    }

    pub async fn exec_node_command_with_debug(
        &self,
        entrypoint: impl Into<String>,
        argv: Vec<String>,
        cwd: impl Into<String>,
        env: BTreeMap<String, String>,
        debug: NodeDebugExecutionOptions,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime_logged(
            "sandbox.runtime.exec_node_command",
            SandboxExecutionRequest::node_command(entrypoint, argv, cwd, env)
                .with_node_debug(debug),
        )
        .await
    }

    pub(crate) async fn eval_untracked(
        &self,
        source: impl Into<String>,
        virtual_specifier: Option<String>,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime_untracked(SandboxExecutionRequest {
            kind: SandboxExecutionKind::Eval {
                source: source.into(),
                virtual_specifier,
            },
            metadata: BTreeMap::new(),
        })
        .await
    }

    pub async fn transpile_typescript(
        &self,
        request: TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        if !counted {
            return self.services.typescript.transpile(self, request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.services.typescript.transpile(self, request).await {
            Ok(report) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.transpile",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        self.commit_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Ok(report)
                    }
                    Err(error) => {
                        self.release_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.transpile",
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

    pub async fn typecheck(
        &self,
        request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        if !counted {
            return self.services.typescript.check(self, request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.services.typescript.check(self, request).await {
            Ok(report) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.check",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        self.commit_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Ok(report)
                    }
                    Err(error) => {
                        self.release_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.check",
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

    pub async fn emit_typescript(
        &self,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        if !counted {
            return self.services.typescript.emit(self, request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.services.typescript.emit(self, request).await {
            Ok(report) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.emit",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        self.commit_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Ok(report)
                    }
                    Err(error) => {
                        self.release_execution_call_count(ExecutionOperation::TypeCheck)
                            .await;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.typescript.emit",
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

    pub async fn run_bash(&self, request: BashRequest) -> Result<BashReport, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        if !counted {
            return self.services.bash.run(self, request).await;
        }
        let params = Some(serde_json::to_value(&request)?);
        match self.services.bash.run(self, request).await {
            Ok(report) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.bash.exec",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        self.commit_execution_call_count(ExecutionOperation::BashHelper)
                            .await;
                        Ok(report)
                    }
                    Err(error) => {
                        self.release_execution_call_count(ExecutionOperation::BashHelper)
                            .await;
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
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

    pub async fn invoke_capability(
        &self,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        if !self
            .info()
            .await
            .provenance
            .capabilities
            .contains(&request.specifier)
        {
            return Err(SandboxError::CapabilityDenied {
                specifier: request.specifier,
            });
        }

        let tool_name = format!(
            "host_api.{}.{}",
            request
                .specifier
                .strip_prefix(crate::HOST_CAPABILITY_PREFIX)
                .unwrap_or(request.specifier.as_str()),
            request.method
        );
        let params = Some(serde_json::to_value(&request)?);
        match self.services.capabilities.invoke(self, request).await {
            Ok(result) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    &tool_name,
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    &tool_name,
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

    pub async fn set_conflict_policy(
        &self,
        policy: ConflictPolicy,
    ) -> Result<SandboxSessionInfo, SandboxError> {
        self.mutate_info(
            "sandbox.session.set_conflict_policy",
            &policy,
            move |info| {
                info.conflict_policy = policy;
            },
        )
        .await
    }

    pub async fn update_provenance<F>(&self, mutator: F) -> Result<SandboxSessionInfo, SandboxError>
    where
        F: FnOnce(&mut SandboxSessionProvenance),
    {
        self.mutate_info(
            "sandbox.session.update_provenance",
            &json!({"kind": "provenance"}),
            move |info| {
                mutator(&mut info.provenance);
                info.provenance
                    .active_view_handles
                    .sort_by(|left, right| left.handle_id.cmp(&right.handle_id));
            },
        )
        .await
    }

    pub async fn install_packages(
        &self,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        let params = Some(serde_json::to_value(&request)?);
        match self.services.packages.install(self, request).await {
            Ok(report) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.package.install",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&report)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        if counted {
                            self.commit_execution_call_count(ExecutionOperation::PackageInstall)
                                .await;
                        }
                        Ok(report)
                    }
                    Err(error) => {
                        if counted {
                            self.release_execution_call_count(ExecutionOperation::PackageInstall)
                                .await;
                        }
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.package.install",
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

    pub async fn hoist_from_disk(
        &self,
        request: HoistRequest,
    ) -> Result<HoistReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.hoist_from_disk_inner(request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.disk.hoist",
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
                    self.volume.as_ref(),
                    "sandbox.disk.hoist",
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

    pub(crate) async fn import_git_tree_into_root(
        &self,
        request: HoistRequest,
        pathspec: Vec<String>,
    ) -> Result<HoistReport, SandboxError> {
        let report = match &request.mode {
            crate::HoistMode::GitHead
                if self.services.git_bridge.supports_host_filesystem_bridge() =>
            {
                self.services
                    .git_bridge
                    .import_repository(
                        GitImportRequest {
                            source: GitImportSource::HostPath {
                                path: request.source_path.clone(),
                            },
                            target_root: request.target_root.clone(),
                            mode: GitImportMode::Head,
                            layout: GitImportLayout::TreeOnly,
                            pathspec,
                            metadata: BTreeMap::new(),
                        },
                        Arc::new(NeverCancel),
                    )
                    .await
                    .map_err(git_substrate_error_to_sandbox)?
            }
            crate::HoistMode::GitWorkingTree {
                include_untracked,
                include_ignored,
            } if self.services.git_bridge.supports_host_filesystem_bridge() => {
                self.services
                    .git_bridge
                    .import_repository(
                        GitImportRequest {
                            source: GitImportSource::HostPath {
                                path: request.source_path.clone(),
                            },
                            target_root: request.target_root.clone(),
                            mode: GitImportMode::WorkingTree {
                                include_untracked: *include_untracked,
                                include_ignored: *include_ignored,
                            },
                            layout: GitImportLayout::TreeOnly,
                            pathspec,
                            metadata: BTreeMap::new(),
                        },
                        Arc::new(NeverCancel),
                    )
                    .await
                    .map_err(git_substrate_error_to_sandbox)?
            }
            crate::HoistMode::DirectorySnapshot => {
                return Err(SandboxError::Service {
                    service: "git",
                    message: "git bridge cannot import directory snapshots".to_string(),
                });
            }
            crate::HoistMode::GitHead | crate::HoistMode::GitWorkingTree { .. } => {
                return Err(SandboxError::Service {
                    service: "git",
                    message:
                        "git support tree import requires a git host bridge that supports host repository import"
                            .to_string(),
                });
            }
        };

        let entries = report
            .entries
            .iter()
            .map(git_import_entry_to_tree_entry)
            .collect::<Result<Vec<_>, _>>()?;

        let deleted_paths = replace_vfs_tree(
            self.volume.fs().as_ref(),
            &report.target_root,
            &entries,
            request.delete_missing,
        )
        .await?;

        let source_path = match &report.source {
            GitImportSource::HostPath { path } => path.clone(),
            GitImportSource::RemoteRepository { remote_url, .. } => remote_url.clone(),
        };

        Ok(HoistReport {
            source_path,
            target_root: report.target_root,
            hoisted_paths: entries.len(),
            deleted_paths,
            git_provenance: Some(crate::GitProvenance {
                repo_root: report.repository_root,
                origin: report.origin,
                head_commit: report.head_commit,
                branch: report.branch,
                remote_url: report.remote_url.as_deref().map(sanitize_remote_url),
                remote_bridge_metadata: durable_remote_bridge_metadata(&report.metadata),
                object_format: Some(report.object_format),
                pathspec: report.pathspec,
                dirty: report.dirty,
            }),
        })
    }

    pub async fn import_remote_repository(
        &self,
        request: GitRemoteImportRequest,
    ) -> Result<GitRemoteImportReport, SandboxError> {
        let params = Some(serde_json::to_value(sanitized_remote_import_request(
            &request,
        ))?);
        match self.import_remote_repository_inner(request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.import_repository",
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
                    self.volume.as_ref(),
                    "sandbox.git.import_repository",
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

    pub async fn eject_to_disk(&self, request: EjectRequest) -> Result<EjectReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.eject_to_disk_inner(request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.disk.eject",
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
                    self.volume.as_ref(),
                    "sandbox.disk.eject",
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

    pub async fn git_head(&self) -> Result<GitHeadState, SandboxError> {
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .head()
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.head",
                    None,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.head",
                    None,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }

    pub async fn git_list_refs(&self) -> Result<Vec<GitReference>, SandboxError> {
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .list_refs()
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.list_refs",
                    None,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.list_refs",
                    None,
                    CompletedToolRunOutcome::Error {
                        message: error.to_string(),
                    },
                )
                .await;
                Err(error)
            }
        }
    }

    pub async fn git_status(
        &self,
        options: GitStatusOptions,
    ) -> Result<GitStatusReport, SandboxError> {
        let params = Some(serde_json::to_value(&options)?);
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .status_with_options(options)
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                self.sync_git_provenance(&repository, None, Some(result.dirty))
                    .await?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.status",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.status",
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

    pub async fn git_diff(&self, request: GitDiffRequest) -> Result<GitDiffReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .diff(request)
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                self.sync_git_provenance(&repository, None, Some(result.dirty))
                    .await?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.diff",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.diff",
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

    pub async fn git_checkout(
        &self,
        request: GitCheckoutRequest,
    ) -> Result<GitCheckoutReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .checkout(request, Arc::new(NeverCancel))
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                self.sync_git_provenance(&repository, result.head_oid.clone(), Some(false))
                    .await?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.checkout",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.checkout",
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

    pub async fn git_update_ref(
        &self,
        request: GitRefUpdate,
    ) -> Result<GitRefUpdateReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.open_git_repository().await {
            Ok(repository) => {
                let result = repository
                    .update_ref(request, Arc::new(NeverCancel))
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                self.sync_git_provenance(&repository, None, None).await?;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.update_ref",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.update_ref",
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

    pub async fn create_pull_request(
        &self,
        request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.create_pull_request_inner(request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.pr.create",
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
                    self.volume.as_ref(),
                    "sandbox.pr.create",
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

    pub async fn open_readonly_view(
        &self,
        request: ReadonlyViewRequest,
    ) -> Result<ReadonlyViewHandle, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.services.readonly_views.open_view(self, request).await {
            Ok(handle) => {
                let _guard = self.operation_lock.lock().await;
                let mut updated = self.info.lock().await.clone();
                if let Some(existing) = updated
                    .provenance
                    .active_view_handles
                    .iter_mut()
                    .find(|candidate| candidate.handle_id == handle.handle_id)
                {
                    *existing = handle.clone();
                } else {
                    updated.provenance.active_view_handles.push(handle.clone());
                }
                updated
                    .provenance
                    .active_view_handles
                    .sort_by(|left, right| left.handle_id.cmp(&right.handle_id));
                updated.revision = updated.revision.saturating_add(1);
                updated.updated_at = self.clock.now();
                write_session_info_exact(self.volume.as_ref(), &updated).await?;
                *self.info.lock().await = updated.clone();
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.view.open",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&handle)?),
                    },
                )
                .await?;
                Ok(handle)
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.view.open",
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

    pub async fn close_readonly_view(&self, handle_id: &str) -> Result<(), SandboxError> {
        let _guard = self.operation_lock.lock().await;
        let current = self.info.lock().await.clone();
        let handle = current
            .provenance
            .active_view_handles
            .iter()
            .find(|candidate| candidate.handle_id == handle_id)
            .cloned()
            .ok_or_else(|| SandboxError::ViewHandleNotFound {
                handle_id: handle_id.to_string(),
            })?;
        let params = Some(json!({ "handle_id": handle_id }));
        match self.services.readonly_views.close_view(self, &handle).await {
            Ok(()) => {
                let mut updated = current;
                updated
                    .provenance
                    .active_view_handles
                    .retain(|candidate| candidate.handle_id != handle_id);
                updated.revision = updated.revision.saturating_add(1);
                updated.updated_at = self.clock.now();
                write_session_info_exact(self.volume.as_ref(), &updated).await?;
                *self.info.lock().await = updated;
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.view.close",
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(JsonValue::Null),
                    },
                )
                .await?;
                Ok(())
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.view.close",
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

    pub async fn active_readonly_views(&self) -> Vec<ReadonlyViewHandle> {
        self.info().await.provenance.active_view_handles
    }

    pub async fn stat_readonly_view(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Stats>, SandboxError> {
        self.services.readonly_views.stat(self, location).await
    }

    pub async fn read_file_readonly_view(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Option<Vec<u8>>, SandboxError> {
        self.services.readonly_views.read_file(self, location).await
    }

    pub async fn readdir_readonly_view(
        &self,
        location: &ReadonlyViewLocation,
    ) -> Result<Vec<DirEntry>, SandboxError> {
        self.services.readonly_views.readdir(self, location).await
    }

    async fn open_git_repository(&self) -> Result<Arc<dyn GitRepository>, SandboxError> {
        let info = self.info().await;
        let manifest = load_hoist_manifest(self.volume.as_ref())
            .await?
            .ok_or(SandboxError::MissingGitProvenance)?;
        let provenance = info
            .provenance
            .git
            .clone()
            .ok_or(SandboxError::MissingGitProvenance)?;
        let image = Arc::new(
            VfsGitRepositoryImage::from_volume(
                self.volume.clone(),
                manifest.target_root.clone(),
                SnapshotOptions::default(),
            )
            .await
            .map_err(git_substrate_error_to_sandbox)?,
        );
        let descriptor = GitRepositoryImage::descriptor(image.as_ref());
        let object_format = resolve_git_object_format(&provenance)?;
        self.services
            .git
            .open(
                image,
                GitOpenRequest {
                    repository_id: format!(
                        "sandbox-session-{}:{}",
                        info.session_volume_id, manifest.target_root
                    ),
                    repository_image: descriptor.clone(),
                    policy: GitRepositoryPolicy {
                        allow_host_bridge: provenance.origin.uses_external_bridge(),
                        ..Default::default()
                    },
                    provenance: GitRepositoryProvenance {
                        backend: self.services.git.name().to_string(),
                        repo_root: provenance.repo_root,
                        origin: provenance.origin,
                        remote_url: provenance.remote_url.clone(),
                        object_format,
                        volume_id: descriptor.volume_id,
                        snapshot_sequence: descriptor.snapshot_sequence,
                        durable_snapshot: descriptor.durable_snapshot,
                        fork_policy: self.services.git.fork_policy(),
                    },
                    metadata: BTreeMap::from([
                        (
                            "session_volume_id".to_string(),
                            JsonValue::from(info.session_volume_id.to_string()),
                        ),
                        (
                            "workspace_root".to_string(),
                            JsonValue::from(info.workspace_root),
                        ),
                        (
                            "repository_root".to_string(),
                            JsonValue::from(manifest.target_root),
                        ),
                    ]),
                },
                Arc::new(NeverCancel),
            )
            .await
            .map_err(git_substrate_error_to_sandbox)
    }

    async fn sync_git_provenance(
        &self,
        repository: &Arc<dyn GitRepository>,
        head_oid: Option<String>,
        dirty: Option<bool>,
    ) -> Result<(), SandboxError> {
        let head = repository
            .head()
            .await
            .map_err(git_substrate_error_to_sandbox)?;
        let dirty = match dirty {
            Some(value) => value,
            None => {
                repository
                    .status()
                    .await
                    .map_err(git_substrate_error_to_sandbox)?
                    .dirty
            }
        };
        let next_head_oid = head_oid.or(head.oid.clone());
        let next_branch = git_branch_name(&head);
        let _guard = self.operation_lock.lock().await;
        let mut current = self.info.lock().await;
        let mut updated = current.clone();
        let Some(git) = updated.provenance.git.as_mut() else {
            return Ok(());
        };
        if git.head_commit == next_head_oid && git.branch == next_branch && git.dirty == dirty {
            return Ok(());
        }
        git.head_commit = next_head_oid;
        git.branch = next_branch;
        git.dirty = dirty;
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = self.clock.now();
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *current = updated;
        Ok(())
    }

    async fn remember_remote_bridge_session_metadata(
        &self,
        remote_url: &str,
        metadata: &BTreeMap<String, JsonValue>,
    ) {
        if metadata.is_empty() {
            return;
        }
        self.remote_bridge_session_metadata
            .lock()
            .await
            .entry(sanitize_remote_url(remote_url))
            .and_modify(|existing| existing.extend(metadata.clone()))
            .or_insert_with(|| metadata.clone());
    }

    async fn remote_bridge_session_metadata(
        &self,
        remote_url: &str,
    ) -> BTreeMap<String, JsonValue> {
        self.remote_bridge_session_metadata
            .lock()
            .await
            .get(&sanitize_remote_url(remote_url))
            .cloned()
            .unwrap_or_default()
    }

    async fn hoist_from_disk_inner(
        &self,
        request: HoistRequest,
    ) -> Result<HoistReport, SandboxError> {
        let prepared = match &request.mode {
            crate::HoistMode::DirectorySnapshot => prepare_hoist(&request)?,
            crate::HoistMode::GitHead
                if self.services.git_bridge.supports_host_filesystem_bridge() =>
            {
                prepare_git_hoist_via_bridge(self.services.git_bridge.clone(), &request).await?
            }
            crate::HoistMode::GitWorkingTree { .. }
                if self.services.git_bridge.supports_host_filesystem_bridge() =>
            {
                prepare_git_hoist_via_bridge(self.services.git_bridge.clone(), &request).await?
            }
            crate::HoistMode::GitHead | crate::HoistMode::GitWorkingTree { .. } => {
                return Err(SandboxError::Service {
                    service: "git",
                    message:
                        "git hoist requires a git host bridge that supports host repository import"
                            .to_string(),
                });
            }
        };
        let _guard = self.operation_lock.lock().await;
        let deleted_paths = replace_vfs_tree(
            self.volume.fs().as_ref(),
            &prepared.manifest.target_root,
            &prepared.entries,
            request.delete_missing,
        )
        .await?;
        write_hoist_manifest(self.volume.as_ref(), &prepared.manifest).await?;
        let source_path = match &prepared.manifest.source {
            GitImportSource::HostPath { path } => path.clone(),
            GitImportSource::RemoteRepository { remote_url, .. } => remote_url.clone(),
        };
        let mut updated = self.info.lock().await.clone();
        updated.provenance.hoisted_source = Some(HoistedSource {
            source_path: source_path.clone(),
            mode: prepared.manifest.mode.clone(),
        });
        updated.provenance.git = prepared.manifest.git_provenance.clone();
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = self.clock.now();
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *self.info.lock().await = updated;
        Ok(HoistReport {
            source_path,
            target_root: prepared.manifest.target_root,
            hoisted_paths: prepared.entries.len(),
            deleted_paths,
            git_provenance: prepared.manifest.git_provenance,
        })
    }

    async fn import_remote_repository_inner(
        &self,
        request: GitRemoteImportRequest,
    ) -> Result<GitRemoteImportReport, SandboxError> {
        if !self.services.git_bridge.supports_remote_repository_bridge() {
            return Err(SandboxError::Service {
                service: "git",
                message:
                    "git remote import requires a git host bridge that supports remote repositories"
                        .to_string(),
            });
        }
        self.remember_remote_bridge_session_metadata(&request.remote_url, &request.metadata)
            .await;
        let mut bridge_request = request.clone();
        bridge_request.metadata.extend(
            self.remote_bridge_session_metadata(&request.remote_url)
                .await,
        );
        let prepared =
            prepare_remote_git_import_via_bridge(self.services.git_bridge.clone(), &bridge_request)
                .await?;
        let _guard = self.operation_lock.lock().await;
        let deleted_paths = replace_vfs_tree(
            self.volume.fs().as_ref(),
            &prepared.manifest.target_root,
            &prepared.entries,
            request.delete_missing,
        )
        .await?;
        write_hoist_manifest(self.volume.as_ref(), &prepared.manifest).await?;
        let git_provenance = prepared
            .manifest
            .git_provenance
            .clone()
            .ok_or(SandboxError::MissingGitProvenance)?;
        let mut updated = self.info.lock().await.clone();
        updated.provenance.hoisted_source = None;
        updated.provenance.git = Some(git_provenance.clone());
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = self.clock.now();
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *self.info.lock().await = updated;
        Ok(GitRemoteImportReport {
            remote_url: sanitize_remote_url(&request.remote_url),
            target_root: prepared.manifest.target_root,
            imported_paths: prepared.entries.len(),
            deleted_paths,
            git_provenance,
        })
    }

    async fn eject_to_disk_inner(
        &self,
        request: EjectRequest,
    ) -> Result<EjectReport, SandboxError> {
        let export_root = self.export_root().await?;
        let target_path = PathBuf::from(&request.target_path);
        let manifest = load_hoist_manifest(self.volume.as_ref()).await?;
        let git_provenance = self.info().await.provenance.git;
        match request.mode {
            EjectMode::MaterializeSnapshot => {
                materialize_snapshot_to_host(self.volume.fs().as_ref(), &export_root, &target_path)
                    .await
            }
            EjectMode::ApplyDelta => {
                let manifest = manifest.ok_or(SandboxError::MissingHoistProvenance)?;
                apply_delta_to_host(
                    self.volume.fs().as_ref(),
                    &export_root,
                    &target_path,
                    &manifest,
                    request.conflict_policy,
                    git_provenance.as_ref(),
                )
                .await
            }
        }
    }

    async fn create_pull_request_inner(
        &self,
        request: PullRequestRequest,
    ) -> Result<PullRequestReport, SandboxError> {
        let info = self.info().await;
        if let Some(git_provenance) = info.provenance.git.clone() {
            if let Some(remote_url) = git_provenance
                .remote_url
                .clone()
                .filter(|url| !url.is_empty())
            {
                let repository = self.open_git_repository().await?;
                let commit = repository
                    .commit(
                        GitCommitRequest {
                            target_ref: format!("refs/heads/{}", request.head_branch),
                            base_ref: Some(format!("refs/heads/{}", request.base_branch)),
                            title: request.title.clone(),
                            body: request.body.clone(),
                            require_changes: true,
                            update_head: true,
                        },
                        Arc::new(NeverCancel),
                    )
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                self.sync_git_provenance(&repository, Some(commit.commit_oid.clone()), Some(false))
                    .await?;
                let ephemeral_remote_metadata =
                    self.remote_bridge_session_metadata(&remote_url).await;
                let mut git_metadata = git_provenance.remote_bridge_metadata;
                git_metadata.extend(ephemeral_remote_metadata.clone());
                git_metadata.extend(BTreeMap::from([
                    (
                        "session_volume_id".to_string(),
                        JsonValue::from(info.session_volume_id.to_string()),
                    ),
                    (
                        "base_branch".to_string(),
                        JsonValue::from(request.base_branch.clone()),
                    ),
                    (
                        "head_commit".to_string(),
                        JsonValue::from(commit.commit_oid.clone()),
                    ),
                    (
                        "tree_oid".to_string(),
                        JsonValue::from(commit.tree_oid.clone()),
                    ),
                    ("committed".to_string(), JsonValue::from(commit.committed)),
                    ("remote_url".to_string(), JsonValue::from(remote_url)),
                ]));
                let push = self
                    .services
                    .git_bridge
                    .push(
                        repository.clone(),
                        GitPushRequest {
                            remote: "origin".to_string(),
                            branch_name: request.head_branch.clone(),
                            head_oid: Some(commit.commit_oid.clone()),
                            metadata: git_metadata.clone(),
                        },
                        Arc::new(NeverCancel),
                    )
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                let mut durable_git_metadata = durable_remote_bridge_metadata(&push.metadata);
                durable_git_metadata.insert("pushed".to_string(), JsonValue::from(true));
                durable_git_metadata.insert(
                    "push_remote_name".to_string(),
                    JsonValue::from(push.remote.clone()),
                );
                if let Some(oid) = push.pushed_oid {
                    durable_git_metadata.insert("pushed_oid".to_string(), JsonValue::from(oid));
                }
                let mut bridge_pr_metadata = durable_git_metadata.clone();
                bridge_pr_metadata.extend(ephemeral_remote_metadata);

                let report = self
                    .services
                    .git_bridge
                    .create_pull_request(
                        GitBridgePullRequestRequest {
                            title: request.title,
                            body: request.body,
                            head_branch: request.head_branch,
                            base_branch: request.base_branch,
                            metadata: bridge_pr_metadata,
                        },
                        Arc::new(NeverCancel),
                    )
                    .await
                    .map_err(git_substrate_error_to_sandbox)?;
                return Ok(git_bridge_pull_request_to_sandbox(
                    self.services.git_bridge.name(),
                    report,
                ));
            }
            return Err(SandboxError::MissingGitRemote);
        }
        self.services
            .pull_requests
            .create_pull_request(self, request)
            .await
    }

    pub async fn close(&self, options: CloseSessionOptions) -> Result<(), SandboxError> {
        let _guard = self.operation_lock.lock().await;
        let mut updated = self.info.lock().await.clone();
        self.services
            .runtime
            .close_session(&updated, &self.runtime)
            .await?;
        updated.state = SandboxSessionState::Closed;
        updated.closed_at = Some(self.clock.now());
        updated.updated_at = updated.closed_at.expect("closed_at set");
        updated.revision = updated.revision.saturating_add(1);
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *self.info.lock().await = updated.clone();
        record_completed_tool_run(
            self.volume.as_ref(),
            "sandbox.session.close",
            Some(serde_json::to_value(options)?),
            CompletedToolRunOutcome::Success {
                result: Some(json!({ "revision": updated.revision })),
            },
        )
        .await?;
        if options.flush {
            self.volume.flush().await?;
        }
        Ok(())
    }

    async fn execute_runtime_logged(
        &self,
        operation_name: &str,
        request: SandboxExecutionRequest,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        let params = Some(serde_json::to_value(&request)?);
        match self.execute_runtime_untracked(request).await {
            Ok(result) => {
                let outcome = record_completed_tool_run(
                    self.volume.as_ref(),
                    operation_name,
                    params,
                    CompletedToolRunOutcome::Success {
                        result: Some(serde_json::to_value(&result)?),
                    },
                )
                .await;
                match outcome {
                    Ok(_) => {
                        if counted {
                            self.commit_execution_call_count(ExecutionOperation::DraftSession)
                                .await;
                        }
                        Ok(result)
                    }
                    Err(error) => {
                        if counted {
                            self.release_execution_call_count(ExecutionOperation::DraftSession)
                                .await;
                        }
                        Err(error)
                    }
                }
            }
            Err(error) => {
                let _ = record_completed_tool_run(
                    self.volume.as_ref(),
                    operation_name,
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

    pub(crate) async fn execute_runtime_untracked(
        &self,
        request: SandboxExecutionRequest,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.runtime_actor.execute(self, request).await
    }

    async fn export_root(&self) -> Result<String, SandboxError> {
        if let Some(manifest) = load_hoist_manifest(self.volume.as_ref()).await? {
            Ok(manifest.target_root)
        } else {
            Ok(self.info().await.workspace_root)
        }
    }

    async fn mutate_info<P, F>(
        &self,
        operation_name: &str,
        params: &P,
        mutator: F,
    ) -> Result<SandboxSessionInfo, SandboxError>
    where
        P: Serialize + ?Sized,
        F: FnOnce(&mut SandboxSessionInfo),
    {
        let _guard = self.operation_lock.lock().await;
        let mut updated = self.info.lock().await.clone();
        mutator(&mut updated);
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = self.clock.now();
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *self.info.lock().await = updated.clone();
        record_completed_tool_run(
            self.volume.as_ref(),
            operation_name,
            Some(serde_json::to_value(params)?),
            CompletedToolRunOutcome::Success {
                result: Some(serde_json::to_value(&updated)?),
            },
        )
        .await?;
        Ok(updated)
    }
}

fn session_volume_config(
    volume_id: terracedb_vfs::VolumeId,
    chunk_size: Option<u32>,
    create_if_missing: bool,
) -> VolumeConfig {
    let config = VolumeConfig::new(volume_id).with_create_if_missing(create_if_missing);
    match chunk_size {
        Some(value) => config.with_chunk_size(value),
        None => config,
    }
}

async fn seed_session_layout(
    volume: &dyn Volume,
    workspace_root: &str,
) -> Result<(), SandboxError> {
    let fs = volume.fs();
    fs.mkdir(
        TERRACE_METADATA_DIR,
        MkdirOptions {
            recursive: true,
            ..Default::default()
        },
    )
    .await?;
    for path in [
        workspace_root,
        "/.terrace/cache",
        TERRACE_RUNTIME_CACHE_DIR,
        "/.terrace/typescript",
        "/.terrace/typescript/transpile",
        "/.terrace/tools",
        TERRACE_NPM_DIR,
        "/.terrace/npm/cache",
        TERRACE_NPM_SESSION_CACHE_DIR,
        TERRACE_NPM_COMPATIBILITY_ROOT,
        "/.terrace/typescript/libs",
        "/.terrace/typescript/transpile",
        "/.terrace/typescript/emits",
        "/.terrace/tools/bash",
    ] {
        fs.mkdir(
            path,
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

async fn load_session_info(volume: &dyn Volume) -> Result<SandboxSessionInfo, SandboxError> {
    let file_info = match volume.fs().read_file(TERRACE_SESSION_METADATA_PATH).await? {
        Some(bytes) => Some(serde_json::from_slice::<SandboxSessionInfo>(&bytes)?),
        None => None,
    };
    let kv_info = match volume.kv().get_json(TERRACE_SESSION_INFO_KV_KEY).await? {
        Some(value) => Some(serde_json::from_value::<SandboxSessionInfo>(value)?),
        None => None,
    };
    let selected = match (file_info, kv_info) {
        (Some(file), Some(kv)) => {
            if file.revision >= kv.revision {
                file
            } else {
                kv
            }
        }
        (Some(file), None) => file,
        (None, Some(kv)) => kv,
        (None, None) => {
            return Err(SandboxError::MissingSessionMetadata {
                path: TERRACE_SESSION_METADATA_PATH.to_string(),
            });
        }
    };
    write_session_info_exact(volume, &selected).await?;
    Ok(selected)
}

async fn load_execution_counts(
    volume: &dyn Volume,
) -> Result<BTreeMap<ExecutionOperation, u64>, SandboxError> {
    let mut tool_counts = BTreeMap::new();
    for run in volume.tools().recent(None).await? {
        if run.status == ToolRunStatus::Success
            && let Some(operation) = execution_operation_for_tool_run(&run.name)
        {
            tool_counts
                .entry(operation)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }
    }

    if let Some(persisted) = load_persisted_execution_counts(volume).await? {
        let mut reconciled = tool_counts.clone();
        for (operation, persisted_count) in persisted.counts {
            let count = tool_counts
                .get(&operation)
                .copied()
                .map_or(0, |tool_count| persisted_count.min(tool_count));
            if count > 0 {
                reconciled.insert(operation, count);
            } else {
                reconciled.remove(&operation);
            }
        }
        return Ok(reconciled);
    }

    Ok(tool_counts)
}

async fn load_persisted_execution_counts(
    volume: &dyn Volume,
) -> Result<Option<PersistedExecutionPolicyState>, SandboxError> {
    let Some(bytes) = volume
        .fs()
        .read_file(TERRACE_EXECUTION_POLICY_STATE_PATH)
        .await?
    else {
        return Ok(None);
    };
    let persisted = serde_json::from_slice::<PersistedExecutionPolicyState>(&bytes)?;
    if persisted.format_version != SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION {
        return Err(SandboxError::InvalidSessionMetadata {
            path: TERRACE_EXECUTION_POLICY_STATE_PATH.to_string(),
            reason: format!(
                "unsupported execution policy state format version {}",
                persisted.format_version
            ),
        });
    }
    Ok(Some(persisted))
}

async fn persist_execution_counts(
    volume: &dyn Volume,
    counts: &BTreeMap<ExecutionOperation, u64>,
) -> Result<(), SandboxError> {
    let bytes = serde_json::to_vec_pretty(&PersistedExecutionPolicyState {
        format_version: SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION,
        counts: counts.clone(),
    })?;
    volume
        .fs()
        .write_file(
            TERRACE_EXECUTION_POLICY_STATE_PATH,
            bytes,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

fn merge_execution_counts(
    persisted: Option<PersistedExecutionPolicyState>,
    snapshot: BTreeMap<ExecutionOperation, u64>,
) -> BTreeMap<ExecutionOperation, u64> {
    let mut merged = snapshot;
    for (operation, persisted_count) in persisted.unwrap_or_default().counts {
        merged
            .entry(operation)
            .and_modify(|count| *count = (*count).max(persisted_count))
            .or_insert(persisted_count);
    }
    merged
}

#[derive(Clone, Debug, Default)]
struct ExecutionCountState {
    committed: BTreeMap<ExecutionOperation, u64>,
    inflight: BTreeMap<ExecutionOperation, u64>,
}

impl ExecutionCountState {
    fn new(committed: BTreeMap<ExecutionOperation, u64>) -> Self {
        Self {
            committed,
            inflight: BTreeMap::new(),
        }
    }
}

fn decrement_execution_count(
    counts: &mut BTreeMap<ExecutionOperation, u64>,
    operation: ExecutionOperation,
) {
    let mut remove = false;
    if let Some(count) = counts.get_mut(&operation) {
        *count = count.saturating_sub(1);
        remove = *count == 0;
    }
    if remove {
        counts.remove(&operation);
    }
}

fn execution_operation_for_tool_run(name: &str) -> Option<ExecutionOperation> {
    match name {
        "sandbox.runtime.exec_module" | "sandbox.runtime.eval" => {
            Some(ExecutionOperation::DraftSession)
        }
        "sandbox.package.install" => Some(ExecutionOperation::PackageInstall),
        "sandbox.typescript.transpile" | "sandbox.typescript.check" | "sandbox.typescript.emit" => {
            Some(ExecutionOperation::TypeCheck)
        }
        "sandbox.bash.exec" => Some(ExecutionOperation::BashHelper),
        _ => None,
    }
}

async fn prepare_git_hoist_via_bridge(
    bridge: Arc<dyn GitHostBridge>,
    request: &HoistRequest,
) -> Result<PreparedHoist, SandboxError> {
    let report = bridge
        .import_repository(
            GitImportRequest {
                source: GitImportSource::HostPath {
                    path: request.source_path.clone(),
                },
                target_root: request.target_root.clone(),
                mode: match &request.mode {
                    crate::HoistMode::GitHead => GitImportMode::Head,
                    crate::HoistMode::GitWorkingTree {
                        include_untracked,
                        include_ignored,
                    } => GitImportMode::WorkingTree {
                        include_untracked: *include_untracked,
                        include_ignored: *include_ignored,
                    },
                    crate::HoistMode::DirectorySnapshot => {
                        return Err(SandboxError::Service {
                            service: "git",
                            message: "git bridge cannot import directory snapshots".to_string(),
                        });
                    }
                },
                layout: GitImportLayout::RepositoryImage,
                pathspec: Vec::new(),
                metadata: BTreeMap::new(),
            },
            Arc::new(NeverCancel),
        )
        .await
        .map_err(git_substrate_error_to_sandbox)?;
    let entries = report
        .entries
        .iter()
        .map(git_import_entry_to_tree_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = HoistManifest {
        format_version: 1,
        source: sanitize_git_import_source(&report.source),
        target_root: report.target_root,
        mode: request.mode.clone(),
        git_provenance: Some(crate::GitProvenance {
            repo_root: report.repository_root,
            origin: report.origin,
            head_commit: report.head_commit,
            branch: report.branch,
            remote_url: report.remote_url.as_deref().map(sanitize_remote_url),
            remote_bridge_metadata: durable_remote_bridge_metadata(&report.metadata),
            object_format: Some(report.object_format),
            pathspec: report.pathspec,
            dirty: report.dirty,
        }),
        entries: entries
            .iter()
            .map(ManifestEntry::from_tree_entry)
            .collect::<Result<Vec<_>, _>>()?,
    };
    Ok(PreparedHoist { manifest, entries })
}

async fn prepare_remote_git_import_via_bridge(
    bridge: Arc<dyn GitHostBridge>,
    request: &GitRemoteImportRequest,
) -> Result<PreparedHoist, SandboxError> {
    let report = bridge
        .import_repository(
            GitImportRequest {
                source: GitImportSource::RemoteRepository {
                    remote_url: request.remote_url.clone(),
                    reference: request.reference.clone(),
                },
                target_root: request.target_root.clone(),
                mode: GitImportMode::Head,
                layout: GitImportLayout::RepositoryImage,
                pathspec: Vec::new(),
                metadata: request.metadata.clone(),
            },
            Arc::new(NeverCancel),
        )
        .await
        .map_err(git_substrate_error_to_sandbox)?;
    let entries = report
        .entries
        .iter()
        .map(git_import_entry_to_tree_entry)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = HoistManifest {
        format_version: 1,
        source: sanitize_git_import_source(&report.source),
        target_root: report.target_root,
        mode: crate::HoistMode::GitHead,
        git_provenance: Some(crate::GitProvenance {
            repo_root: report.repository_root,
            origin: report.origin,
            head_commit: report.head_commit,
            branch: report.branch,
            remote_url: report.remote_url.as_deref().map(sanitize_remote_url),
            remote_bridge_metadata: durable_remote_bridge_metadata(&report.metadata),
            object_format: Some(report.object_format),
            pathspec: report.pathspec,
            dirty: report.dirty,
        }),
        entries: entries
            .iter()
            .map(ManifestEntry::from_tree_entry)
            .collect::<Result<Vec<_>, _>>()?,
    };
    Ok(PreparedHoist { manifest, entries })
}

fn git_import_entry_to_tree_entry(
    entry: &terracedb_git::GitImportEntry,
) -> Result<TreeEntry, SandboxError> {
    let path = normalize_bridge_import_path(&entry.path)?;
    let data = match entry.kind {
        GitImportEntryKind::File => {
            TreeEntryData::File(entry.data.clone().ok_or(SandboxError::Service {
                service: "git",
                message: format!("missing file payload for imported path {}", entry.path),
            })?)
        }
        GitImportEntryKind::Directory => TreeEntryData::Directory,
        GitImportEntryKind::Symlink => {
            TreeEntryData::Symlink(entry.symlink_target.clone().ok_or(SandboxError::Service {
                service: "git",
                message: format!("missing symlink target for imported path {}", entry.path),
            })?)
        }
    };
    Ok(TreeEntry {
        path,
        data,
        mode: entry.mode,
    })
}

fn git_bridge_pull_request_to_sandbox(
    provider: &str,
    report: terracedb_git::GitPullRequestReport,
) -> PullRequestReport {
    let id = report
        .url
        .rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or("pull-request")
        .to_string();
    PullRequestReport {
        provider: provider.to_string(),
        id,
        url: report.url,
        metadata: durable_remote_bridge_metadata(&report.metadata),
    }
}

fn resolve_git_object_format(
    provenance: &crate::GitProvenance,
) -> Result<GitObjectFormat, SandboxError> {
    provenance
        .object_format
        .or_else(|| {
            provenance
                .head_commit
                .as_deref()
                .and_then(GitObjectFormat::from_oid)
        })
        .ok_or(SandboxError::MissingGitObjectFormat)
}

fn normalize_bridge_import_path(path: &str) -> Result<String, SandboxError> {
    let mut normalized = Vec::new();
    for component in Path::new(path).components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment.to_string_lossy().into_owned()),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(SandboxError::Service {
                    service: "git",
                    message: format!("bridge import path escapes sandbox root: {path}"),
                });
            }
        }
    }
    let normalized = normalized.join("/");
    if normalized.is_empty() {
        return Err(SandboxError::Service {
            service: "git",
            message: format!("bridge import path must not be empty: {path}"),
        });
    }
    Ok(normalized)
}

fn git_branch_name(head: &GitHeadState) -> Option<String> {
    head.symbolic_ref.as_ref().map(|reference| {
        reference
            .strip_prefix("refs/heads/")
            .unwrap_or(reference)
            .to_string()
    })
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedExecutionPolicyState {
    format_version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    counts: BTreeMap<ExecutionOperation, u64>,
}

impl Default for PersistedExecutionPolicyState {
    fn default() -> Self {
        Self {
            format_version: SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION,
            counts: BTreeMap::new(),
        }
    }
}

async fn write_session_info_exact(
    volume: &dyn Volume,
    info: &SandboxSessionInfo,
) -> Result<(), SandboxError> {
    let mut durable_info = info.clone();
    durable_info.provenance.git = durable_info
        .provenance
        .git
        .as_ref()
        .map(crate::GitProvenance::sanitized_for_durability);
    let bytes = serde_json::to_vec_pretty(&durable_info)?;
    volume
        .fs()
        .write_file(
            TERRACE_SESSION_METADATA_PATH,
            bytes,
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    volume
        .kv()
        .set_json(
            TERRACE_SESSION_INFO_KV_KEY,
            serde_json::to_value(&durable_info)?,
        )
        .await?;
    Ok(())
}

fn sanitized_remote_import_request(request: &GitRemoteImportRequest) -> GitRemoteImportRequest {
    let mut sanitized = request.clone();
    sanitized.metadata = durable_remote_bridge_metadata(&sanitized.metadata);
    sanitized
}

pub(crate) async fn record_completed_tool_run(
    volume: &dyn Volume,
    name: &str,
    params: Option<JsonValue>,
    outcome: CompletedToolRunOutcome,
) -> Result<ToolRunId, SandboxError> {
    volume
        .tools()
        .record_completed(CompletedToolRun {
            name: name.to_string(),
            params,
            outcome,
        })
        .await
        .map_err(Into::into)
}

fn validate_existing_session(
    info: &SandboxSessionInfo,
    config: &SandboxConfig,
) -> Result<(), SandboxError> {
    if info.provenance.base_snapshot.volume_id != config.base_volume_id {
        return Err(SandboxError::SessionConfigMismatch {
            field: "base_volume_id",
            expected: config.base_volume_id.to_string(),
            found: info.provenance.base_snapshot.volume_id.to_string(),
        });
    }
    if info.workspace_root != config.workspace_root {
        return Err(SandboxError::SessionConfigMismatch {
            field: "workspace_root",
            expected: config.workspace_root.clone(),
            found: info.workspace_root.clone(),
        });
    }
    if info.provenance.package_compat != config.package_compat {
        return Err(SandboxError::SessionConfigMismatch {
            field: "package_compat",
            expected: format!("{:?}", config.package_compat),
            found: format!("{:?}", info.provenance.package_compat),
        });
    }
    if info.conflict_policy != config.conflict_policy {
        return Err(SandboxError::SessionConfigMismatch {
            field: "conflict_policy",
            expected: format!("{:?}", config.conflict_policy),
            found: format!("{:?}", info.conflict_policy),
        });
    }
    if info.provenance.capabilities != config.capabilities {
        return Err(SandboxError::SessionConfigMismatch {
            field: "capabilities",
            expected: serde_json::to_string(&config.capabilities)?,
            found: serde_json::to_string(&info.provenance.capabilities)?,
        });
    }
    if info.provenance.execution_policy != config.execution_policy {
        return Err(SandboxError::SessionConfigMismatch {
            field: "execution_policy",
            expected: serde_json::to_string(&config.execution_policy)?,
            found: serde_json::to_string(&info.provenance.execution_policy)?,
        });
    }
    Ok(())
}
