use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use terracedb::Clock;
use terracedb_capabilities::{ExecutionOperation, ExecutionPolicy};
use terracedb_vfs::{
    CompletedToolRun, CompletedToolRunOutcome, CreateOptions, DirEntry, MkdirOptions,
    ReadOnlyVfsFileSystem, SnapshotOptions, Stats, ToolRunId, ToolRunStatus, VfsError, VfsKvStore,
    VfsStoreExt, Volume, VolumeConfig, VolumeSnapshot, VolumeStore,
};
use tokio::sync::Mutex;

use crate::disk::{
    apply_delta_to_host, load_hoist_manifest, materialize_snapshot_to_host, prepare_hoist,
    replace_vfs_tree, write_hoist_manifest,
};
use crate::git::{default_export_workspace_path, finalize_git_export};
use crate::routing::{
    RoutedBashService, RoutedPackageInstaller, RoutedRuntimeBackend, RoutedTypeScriptService,
    SandboxExecutionRouter,
};
use crate::{
    BaseSnapshotIdentity, BashReport, BashRequest, BashService, CapabilityCallRequest,
    CapabilityCallResult, CapabilityRegistry, ConflictPolicy, DeterministicBashService,
    DeterministicGitWorkspaceManager, DeterministicPackageInstaller,
    DeterministicPullRequestProviderClient, DeterministicReadonlyViewProvider,
    DeterministicRuntimeBackend, DeterministicTypeScriptService, EjectMode, EjectReport,
    EjectRequest, GitWorkspaceManager, GitWorkspaceReport, GitWorkspaceRequest, HoistReport,
    HoistRequest, HoistedSource, HostGitWorkspaceManager, PackageInstallReport,
    PackageInstallRequest, PackageInstaller, PullRequestProviderClient, PullRequestReport,
    PullRequestRequest, ReadonlyViewCut, ReadonlyViewHandle, ReadonlyViewLocation,
    ReadonlyViewProvider, ReadonlyViewRequest, SANDBOX_EXECUTION_POLICY_STATE_FORMAT_VERSION,
    SandboxConfig, SandboxError, SandboxExecutionRequest, SandboxExecutionResult,
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
    pub git: Arc<dyn GitWorkspaceManager>,
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
        git: Arc<dyn GitWorkspaceManager>,
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

    pub fn with_git_workspace_manager(mut self, git: Arc<dyn GitWorkspaceManager>) -> Self {
        self.git = git;
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
            Arc::new(DeterministicGitWorkspaceManager::default()),
            Arc::new(DeterministicPullRequestProviderClient::default()),
            Arc::new(DeterministicReadonlyViewProvider::default()),
        )
    }

    pub fn deterministic_with_capabilities(capabilities: Arc<dyn CapabilityRegistry>) -> Self {
        Self::deterministic().with_capabilities(capabilities)
    }

    pub fn deterministic_with_host_git() -> Self {
        Self::deterministic()
            .with_git_workspace_manager(Arc::new(HostGitWorkspaceManager::default()))
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
            git_workspace_manager: self.git.name().to_string(),
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
        self.execute_runtime(
            "sandbox.runtime.exec_module",
            SandboxExecutionRequest::module(specifier.into()),
        )
        .await
    }

    pub async fn eval(
        &self,
        source: impl Into<String>,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        self.execute_runtime(
            "sandbox.runtime.eval",
            SandboxExecutionRequest::eval(source.into()),
        )
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

    pub async fn prepare_git_workspace(
        &self,
        request: GitWorkspaceRequest,
    ) -> Result<GitWorkspaceReport, SandboxError> {
        let params = Some(serde_json::to_value(&request)?);
        match self.services.git.prepare_workspace(self, request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.git.prepare_workspace",
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
                    "sandbox.git.prepare_workspace",
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

    async fn hoist_from_disk_inner(
        &self,
        request: HoistRequest,
    ) -> Result<HoistReport, SandboxError> {
        let prepared = prepare_hoist(&request)?;
        let _guard = self.operation_lock.lock().await;
        let deleted_paths = replace_vfs_tree(
            self.volume.fs().as_ref(),
            &prepared.manifest.target_root,
            &prepared.entries,
            request.delete_missing,
        )
        .await?;
        write_hoist_manifest(self.volume.as_ref(), &prepared.manifest).await?;
        let mut updated = self.info.lock().await.clone();
        updated.provenance.hoisted_source = Some(HoistedSource {
            source_path: prepared.manifest.source_path.clone(),
            mode: prepared.manifest.mode.clone(),
        });
        updated.provenance.git = prepared.manifest.git_provenance.clone();
        updated.revision = updated.revision.saturating_add(1);
        updated.updated_at = self.clock.now();
        write_session_info_exact(self.volume.as_ref(), &updated).await?;
        *self.info.lock().await = updated;
        Ok(HoistReport {
            source_path: prepared.manifest.source_path,
            target_root: prepared.manifest.target_root,
            hoisted_paths: prepared.entries.len(),
            deleted_paths,
            git_provenance: prepared.manifest.git_provenance,
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
        let has_git_provenance = self.info().await.provenance.git.is_some();
        let mut git_metadata = BTreeMap::new();
        if has_git_provenance {
            let workspace_path = default_export_workspace_path(
                &self.info().await.session_volume_id.to_string(),
                &request.head_branch,
            );
            let workspace = self
                .prepare_git_workspace(GitWorkspaceRequest {
                    branch_name: request.head_branch.clone(),
                    base_branch: Some(request.base_branch.clone()),
                    target_path: workspace_path.to_string_lossy().into_owned(),
                })
                .await?;
            let export_mode = if load_hoist_manifest(self.volume.as_ref()).await?.is_some() {
                EjectMode::ApplyDelta
            } else {
                EjectMode::MaterializeSnapshot
            };
            let export_mode_label = match export_mode {
                EjectMode::MaterializeSnapshot => "materialize_snapshot",
                EjectMode::ApplyDelta => "apply_delta",
            };
            let eject_report = self
                .eject_to_disk(EjectRequest {
                    target_path: workspace.workspace_path.clone(),
                    mode: export_mode.clone(),
                    conflict_policy: ConflictPolicy::Fail,
                })
                .await?;
            git_metadata.extend(workspace.metadata.clone());
            git_metadata.insert(
                "workspace_path".to_string(),
                JsonValue::from(workspace.workspace_path.clone()),
            );
            git_metadata.insert("eject_mode".to_string(), JsonValue::from(export_mode_label));
            git_metadata.insert(
                "provenance_validated".to_string(),
                JsonValue::from(eject_report.provenance_validated),
            );
            if workspace.metadata.contains_key("repo_root") {
                let workspace_path = PathBuf::from(&workspace.workspace_path);
                git_metadata.extend(finalize_git_export(
                    workspace_path.as_path(),
                    &request.title,
                    &request.body,
                    &request.head_branch,
                )?);
            }
        }
        let mut report = self
            .services
            .pull_requests
            .create_pull_request(self, request)
            .await?;
        report.metadata.extend(git_metadata);
        Ok(report)
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

    async fn execute_runtime(
        &self,
        operation_name: &str,
        request: SandboxExecutionRequest,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let counted = self.execution_policy().await.is_some();
        let params = Some(serde_json::to_value(&request)?);
        match self.runtime_actor.execute(self, request).await {
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
    let bytes = serde_json::to_vec_pretty(info)?;
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
        .set_json(TERRACE_SESSION_INFO_KV_KEY, serde_json::to_value(info)?)
        .await?;
    Ok(())
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
