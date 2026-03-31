use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use terracedb::Clock;
use terracedb_vfs::{
    CompletedToolRun, CompletedToolRunOutcome, CreateOptions, DirEntry, MkdirOptions,
    ReadOnlyVfsFileSystem, SnapshotOptions, Stats, ToolRunId, VfsError, VfsKvStore, VfsStoreExt,
    Volume, VolumeConfig, VolumeSnapshot, VolumeStore,
};
use tokio::sync::Mutex;

use crate::disk::{
    apply_delta_to_host, load_hoist_manifest, materialize_snapshot_to_host, prepare_hoist,
    replace_vfs_tree, write_hoist_manifest,
};
use crate::git::{default_export_workspace_path, finalize_git_export};
use crate::{
    BaseSnapshotIdentity, CapabilityCallRequest, CapabilityCallResult, CapabilityRegistry,
    ConflictPolicy, DeterministicGitWorkspaceManager, DeterministicPackageInstaller,
    DeterministicPullRequestProviderClient, DeterministicReadonlyViewProvider,
    DeterministicRuntimeBackend, EjectMode, EjectReport, EjectRequest, GitWorkspaceManager,
    GitWorkspaceReport, GitWorkspaceRequest, HoistReport, HoistRequest, HoistedSource,
    PackageInstallReport, PackageInstallRequest, PackageInstaller, PullRequestProviderClient,
    PullRequestReport, PullRequestRequest, ReadonlyViewCut, ReadonlyViewHandle,
    ReadonlyViewLocation, ReadonlyViewProvider, ReadonlyViewRequest, SandboxConfig, SandboxError,
    SandboxExecutionRequest, SandboxExecutionResult, SandboxFilesystemShim, SandboxModuleLoader,
    SandboxRuntimeActor, SandboxRuntimeBackend, SandboxRuntimeHandle, SandboxRuntimeStateHandle,
    SandboxServiceBindings, SandboxSessionInfo, SandboxSessionProvenance, SandboxSessionState,
    StaticCapabilityRegistry, TERRACE_METADATA_DIR, TERRACE_NPM_COMPATIBILITY_ROOT,
    TERRACE_NPM_DIR, TERRACE_NPM_SESSION_CACHE_DIR, TERRACE_RUNTIME_CACHE_DIR,
    TERRACE_SESSION_INFO_KV_KEY, TERRACE_SESSION_METADATA_PATH, VfsSandboxFilesystemShim,
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
    pub capabilities: Arc<dyn CapabilityRegistry>,
}

impl SandboxServices {
    pub fn new(
        runtime: Arc<dyn SandboxRuntimeBackend>,
        packages: Arc<dyn PackageInstaller>,
        git: Arc<dyn GitWorkspaceManager>,
        pull_requests: Arc<dyn PullRequestProviderClient>,
        readonly_views: Arc<dyn ReadonlyViewProvider>,
    ) -> Self {
        Self {
            runtime,
            packages,
            git,
            pull_requests,
            readonly_views,
            capabilities: Arc::new(StaticCapabilityRegistry::default()),
        }
    }

    pub fn with_capabilities(mut self, capabilities: Arc<dyn CapabilityRegistry>) -> Self {
        self.capabilities = capabilities;
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

    pub fn bindings(&self) -> SandboxServiceBindings {
        SandboxServiceBindings {
            runtime_backend: self.runtime.name().to_string(),
            package_installer: self.packages.name().to_string(),
            git_workspace_manager: self.git.name().to_string(),
            pull_request_provider: self.pull_requests.name().to_string(),
            readonly_view_provider: self.readonly_views.name().to_string(),
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
        if info.state == SandboxSessionState::Closed {
            info.state = SandboxSessionState::Open;
            info.closed_at = None;
            info.updated_at = self.clock.now();
            info.revision = info.revision.saturating_add(1);
            write_session_info_exact(volume.as_ref(), &info).await?;
        }
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
            services: self.services.bindings(),
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
                active_view_handles: Vec::new(),
            },
        };
        write_session_info_exact(volume.as_ref(), &info).await?;
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
    runtime: SandboxRuntimeHandle,
    runtime_actor: SandboxRuntimeActor,
}

impl SandboxSession {
    fn new(
        volume: Arc<dyn Volume>,
        clock: Arc<dyn Clock>,
        services: SandboxServices,
        info: SandboxSessionInfo,
        runtime: SandboxRuntimeHandle,
    ) -> Self {
        Self {
            volume,
            clock,
            runtime_actor: SandboxRuntimeActor::new(services.runtime.clone(), runtime.clone()),
            services,
            info: Arc::new(Mutex::new(info)),
            operation_lock: Arc::new(Mutex::new(())),
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
        let params = Some(serde_json::to_value(&request)?);
        match self.services.packages.install(self, request).await {
            Ok(report) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    "sandbox.package.install",
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
        let params = Some(serde_json::to_value(&request)?);
        match self.runtime_actor.execute(self, request).await {
            Ok(result) => {
                record_completed_tool_run(
                    self.volume.as_ref(),
                    operation_name,
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
    Ok(())
}
