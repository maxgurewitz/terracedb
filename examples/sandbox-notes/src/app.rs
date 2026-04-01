use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use terracedb_capabilities::{
    BudgetPolicy, CapabilityGrant, CapabilityPresetDescriptor, CapabilityProfileDescriptor,
    CapabilityTemplate, CapabilityUseMetrics, CapabilityUseRequest, DeterministicPolicyEngine,
    DeterministicRateLimiter, DeterministicSubjectResolver, ExecutionDomain,
    ExecutionDomainAssignment, ExecutionOperation, ExecutionPolicy, PolicyDecisionRecord,
    PolicyOutcomeKind, PolicySubject, PreparedSessionPreset, PresetBinding, ResourceKind,
    ResourcePolicy, ResourceSelector, SessionMode, SessionPresetRequest,
    StaticExecutionPolicyResolver, SubjectSelector,
};
use terracedb_sandbox::{
    BashReport, BashRequest, BashService, CapabilityManifest as SandboxCapabilityManifest,
    CapabilityMethod0, CapabilityMethod1, CapabilityRegistry, ConflictPolicy,
    DeterministicBashService, DeterministicPackageInstaller, DeterministicRuntimeBackend,
    DeterministicTypeScriptService, EjectMode, EjectRequest, GitWorkspaceRequest, HoistMode,
    HoistRequest, PackageCompatibilityMode, PackageInstallReport, PackageInstallRequest,
    PackageInstaller, PullRequestRequest, ReadonlyViewHandle, ReadonlyViewRequest,
    SandboxCapability, SandboxCapabilityModule, SandboxError, SandboxExecutionRequest,
    SandboxExecutionResult, SandboxHarness, SandboxRuntimeBackend, SandboxRuntimeHandle,
    SandboxRuntimeStateHandle, SandboxServices, SandboxSession, SandboxSessionInfo,
    TypeCheckReport, TypeCheckRequest, TypeScriptEmitReport, TypeScriptService,
    TypeScriptTranspileReport, TypeScriptTranspileRequest, TypedCapabilityModuleBuilder,
    TypedCapabilityRegistry,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeId, VolumeStore};
use tokio::sync::Mutex;

use crate::model::{AddCommentInput, DemoReport, ExampleComment, ExampleNote, ReviewSummary};

pub const NOTES_CAPABILITY_SPECIFIER: &str = "terrace:host/notes";
pub const NOTES_PRESET_NAME: &str = "notes-review";
pub const NOTES_PROFILE_NAME: &str = "foreground";
pub const REVIEW_ENTRYPOINT: &str = "/workspace/src/review.js";
pub const TYPESCRIPT_ENTRYPOINT: &str = "/workspace/src/render.ts";
pub const GENERATED_SUMMARY_PATH: &str = "/workspace/generated/triage-summary.json";
pub const GENERATED_REVIEW_NOTES_PATH: &str = "/workspace/docs/review-notes.md";

const NOTES_TEMPLATE_ID: &str = "host.notes.v1";
const NOTES_SUBJECT_ID: &str = "user:sandbox-notes";
const NOTES_TENANT_ID: &str = "example-notes";

const PROJECT_FILES: &[(&str, &str)] = &[
    ("README.md", include_str!("../project-template/README.md")),
    (
        "package.json",
        include_str!("../project-template/package.json"),
    ),
    (
        "tsconfig.json",
        include_str!("../project-template/tsconfig.json"),
    ),
    ("inbox.json", include_str!("../project-template/inbox.json")),
    (
        "src/review.js",
        include_str!("../project-template/src/review.js"),
    ),
    (
        "src/render.ts",
        include_str!("../project-template/src/render.ts"),
    ),
];

static UNIQUE_PATH_COUNTER: AtomicU64 = AtomicU64::new(0);

type NotesState = Arc<Mutex<Vec<ExampleNote>>>;
type NotesRegistry = TypedCapabilityRegistry<NotesState>;

#[derive(Clone)]
pub struct ExampleHostApp {
    notes: NotesState,
    registry: Arc<NotesRegistry>,
}

impl ExampleHostApp {
    pub fn new(notes: Vec<ExampleNote>) -> Self {
        let notes = Arc::new(Mutex::new(notes));
        let registry = Arc::new(build_notes_registry(notes.clone()).expect("build notes registry"));
        Self { notes, registry }
    }

    pub fn sample() -> Self {
        Self::new(sample_notes())
    }

    pub fn notes_registry(&self) -> Arc<NotesRegistry> {
        self.registry.clone()
    }

    pub async fn notes_snapshot(&self) -> Vec<ExampleNote> {
        self.notes.lock().await.clone()
    }

    pub fn deterministic_services(&self) -> SandboxServices {
        SandboxServices::deterministic_with_capabilities(self.notes_registry())
    }

    pub fn host_git_services(&self) -> SandboxServices {
        SandboxServices::deterministic_with_host_git_and_capabilities(self.notes_registry())
    }

    pub fn deterministic_services_for_prepared_session(
        &self,
        session_id: impl Into<String>,
        prepared: &PreparedSessionPreset,
    ) -> Result<SandboxServices, SandboxError> {
        let mut services = SandboxServices::deterministic_with_capabilities(
            self.prepared_notes_registry(session_id, prepared)?,
        );
        apply_prepared_execution_policy(&mut services, prepared);
        Ok(services)
    }

    pub fn host_git_services_for_prepared_session(
        &self,
        session_id: impl Into<String>,
        prepared: &PreparedSessionPreset,
    ) -> Result<SandboxServices, SandboxError> {
        let mut services = SandboxServices::deterministic_with_host_git_and_capabilities(
            self.prepared_notes_registry(session_id, prepared)?,
        );
        apply_prepared_execution_policy(&mut services, prepared);
        Ok(services)
    }

    pub fn notes_policy_engine(&self, session_id: impl Into<String>) -> DeterministicPolicyEngine {
        let session_id = session_id.into();
        let execution_policy = notes_execution_policy();
        DeterministicPolicyEngine::new(
            vec![notes_template()],
            vec![notes_grant()],
            DeterministicSubjectResolver::default().with_session_subject(
                session_id,
                PolicySubject {
                    subject_id: NOTES_SUBJECT_ID.to_string(),
                    tenant_id: Some(NOTES_TENANT_ID.to_string()),
                    groups: vec!["example-host".to_string()],
                    attributes: BTreeMap::from([("app".to_string(), "sandbox-notes".to_string())]),
                },
            ),
            StaticExecutionPolicyResolver::new(execution_policy.clone())
                .with_policy(SessionMode::Draft, execution_policy.clone())
                .with_preset_policy(NOTES_PRESET_NAME, execution_policy.clone())
                .with_profile_policy(NOTES_PROFILE_NAME, execution_policy.clone()),
        )
        .with_preset(notes_preset())
        .with_profile(notes_profile())
        .with_rate_limiter(DeterministicRateLimiter::default())
    }

    pub fn prepare_notes_draft_session(
        &self,
        session_id: impl Into<String>,
    ) -> Result<PreparedSessionPreset, SandboxError> {
        let session_id = session_id.into();
        self.notes_policy_engine(session_id.clone())
            .prepare_session_from_preset(&SessionPresetRequest {
                subject: terracedb_capabilities::SubjectResolutionRequest {
                    session_id,
                    auth_subject_hint: Some(NOTES_SUBJECT_ID.to_string()),
                    tenant_hint: Some(NOTES_TENANT_ID.to_string()),
                    groups: vec!["example-host".to_string()],
                    attributes: BTreeMap::new(),
                },
                preset_name: NOTES_PRESET_NAME.to_string(),
                profile_name: Some(NOTES_PROFILE_NAME.to_string()),
                session_mode_override: Some(SessionMode::Draft),
                binding_overrides: Vec::new(),
                metadata: BTreeMap::from([("surface".to_string(), JsonValue::from("example"))]),
            })
            .map_err(|error| SandboxError::Service {
                service: "sandbox-notes",
                message: format!("prepare notes preset session: {error}"),
            })
    }

    pub fn sandbox_manifest_for_prepared_session(
        &self,
        session_id: impl Into<String>,
        prepared: &PreparedSessionPreset,
    ) -> Result<SandboxCapabilityManifest, SandboxError> {
        Ok(self
            .prepared_notes_registry(session_id, prepared)?
            .manifest())
    }

    fn prepared_notes_registry(
        &self,
        session_id: impl Into<String>,
        prepared: &PreparedSessionPreset,
    ) -> Result<Arc<dyn CapabilityRegistry>, SandboxError> {
        Ok(Arc::new(PreparedNotesPolicyRegistry::new(
            self.notes_registry(),
            session_id.into(),
            prepared,
        )?))
    }
}

#[derive(Clone)]
struct PreparedNotesPolicyRegistry {
    inner: Arc<NotesRegistry>,
    session_id: String,
    manifest: SandboxCapabilityManifest,
    modules: BTreeMap<String, SandboxCapabilityModule>,
    prepared: PreparedSessionPreset,
    call_counts: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl PreparedNotesPolicyRegistry {
    fn new(
        inner: Arc<NotesRegistry>,
        session_id: String,
        prepared: &PreparedSessionPreset,
    ) -> Result<Self, SandboxError> {
        let available = inner.manifest();
        let mut capabilities = Vec::new();
        let mut modules = BTreeMap::new();
        for binding in &prepared.resolved.manifest.bindings {
            let mut capability = available
                .get(&binding.module_specifier)
                .cloned()
                .ok_or_else(|| SandboxError::CapabilityUnavailable {
                    specifier: binding.module_specifier.clone(),
                })?;
            capability.metadata.extend(capability_policy_metadata(
                prepared,
                &binding.binding_name,
                None,
            )?);
            capabilities.push(capability.clone());
            if let Some(mut module) = inner.module(&binding.module_specifier) {
                module.capability = capability;
                module.metadata.extend(capability_policy_metadata(
                    prepared,
                    &binding.binding_name,
                    None,
                )?);
                modules.insert(binding.module_specifier.clone(), module);
            }
        }

        Ok(Self {
            inner,
            session_id,
            manifest: SandboxCapabilityManifest { capabilities },
            modules,
            prepared: prepared.clone(),
            call_counts: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn binding_for_specifier(
        &self,
        specifier: &str,
    ) -> Option<&terracedb_capabilities::ManifestBinding> {
        self.prepared
            .resolved
            .manifest
            .bindings
            .iter()
            .find(|binding| binding.module_specifier == specifier)
    }

    async fn policy_decision_for(
        &self,
        session: &SandboxSession,
        request: &terracedb_sandbox::CapabilityCallRequest,
    ) -> Result<PolicyDecisionRecord, SandboxError> {
        let binding = self
            .binding_for_specifier(&request.specifier)
            .ok_or_else(|| SandboxError::CapabilityDenied {
                specifier: request.specifier.clone(),
            })?;
        let call_count = {
            let mut counts = self.call_counts.lock().await;
            let next = counts
                .entry(binding.binding_name.clone())
                .and_modify(|count| *count += 1)
                .or_insert(1);
            *next
        };
        let requested_at = session.info().await.updated_at;
        Ok(self.prepared.resolved.evaluate_use(&CapabilityUseRequest {
            session_id: self.session_id.clone(),
            operation: ExecutionOperation::DraftSession,
            binding_name: binding.binding_name.clone(),
            capability_family: Some(binding.capability_family.clone()),
            targets: Vec::new(),
            metrics: CapabilityUseMetrics {
                call_count,
                ..CapabilityUseMetrics::default()
            },
            requested_at,
            metadata: BTreeMap::from([
                (
                    "capability_specifier".to_string(),
                    JsonValue::String(request.specifier.clone()),
                ),
                (
                    "capability_method".to_string(),
                    JsonValue::String(request.method.clone()),
                ),
            ]),
        }))
    }
}

#[async_trait]
impl CapabilityRegistry for PreparedNotesPolicyRegistry {
    fn manifest(&self) -> SandboxCapabilityManifest {
        self.manifest.clone()
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        self.manifest.get(specifier).cloned()
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        self.modules.get(specifier).cloned()
    }

    async fn invoke(
        &self,
        session: &SandboxSession,
        request: terracedb_sandbox::CapabilityCallRequest,
    ) -> Result<terracedb_sandbox::CapabilityCallResult, SandboxError> {
        let decision = self.policy_decision_for(session, &request).await?;
        match decision.outcome.outcome {
            PolicyOutcomeKind::Allowed => {
                let binding_name = decision.audit.binding_name.clone();
                let mut result = self.inner.invoke(session, request).await?;
                result.metadata.extend(capability_policy_metadata(
                    &self.prepared,
                    &binding_name,
                    Some(&decision),
                )?);
                Ok(result)
            }
            PolicyOutcomeKind::MissingBinding | PolicyOutcomeKind::Denied => {
                Err(SandboxError::CapabilityDenied {
                    specifier: request.specifier,
                })
            }
            PolicyOutcomeKind::RateLimited | PolicyOutcomeKind::BudgetExhausted => {
                Err(SandboxError::Service {
                    service: "sandbox-notes",
                    message: decision
                        .outcome
                        .message
                        .clone()
                        .unwrap_or_else(|| "prepared preset policy rejected the call".to_string()),
                })
            }
        }
    }
}

#[derive(Clone)]
struct PreparedExecutionPolicyController {
    prepared: PreparedSessionPreset,
    operation_counts: Arc<Mutex<BTreeMap<ExecutionOperation, u64>>>,
}

impl PreparedExecutionPolicyController {
    fn new(prepared: &PreparedSessionPreset) -> Self {
        Self {
            prepared: prepared.clone(),
            operation_counts: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    async fn begin_operation(
        &self,
        operation: ExecutionOperation,
    ) -> Result<BTreeMap<String, JsonValue>, SandboxError> {
        let call_count = {
            let mut counts = self.operation_counts.lock().await;
            let next = counts
                .entry(operation)
                .and_modify(|count| *count += 1)
                .or_insert(1);
            *next
        };
        let assignment = self
            .prepared
            .resolved
            .execution_policy
            .assignment_for(operation)
            .clone();
        let budget = assignment.budget.clone().or_else(|| {
            self.prepared
                .resolved
                .execution_policy
                .default_assignment
                .budget
                .clone()
        });
        if let Some(max_calls) = budget.as_ref().and_then(|budget| budget.max_calls)
            && call_count > max_calls
        {
            return Err(SandboxError::Service {
                service: "sandbox-notes",
                message: format!(
                    "execution policy for {operation:?} exceeded max_calls {max_calls}"
                ),
            });
        }

        let mut metadata = BTreeMap::from([
            ("operation".to_string(), serde_json::to_value(operation)?),
            (
                "execution_domain".to_string(),
                serde_json::to_value(&assignment.domain)?,
            ),
            (
                "placement_tags".to_string(),
                serde_json::to_value(&assignment.placement_tags)?,
            ),
            (
                "execution_call_count".to_string(),
                JsonValue::from(call_count),
            ),
            (
                "execution_policy".to_string(),
                serde_json::to_value(&self.prepared.resolved.execution_policy)?,
            ),
        ]);
        if let Some(budget) = budget {
            metadata.insert(
                "execution_budget".to_string(),
                serde_json::to_value(&budget)?,
            );
        }
        Ok(metadata)
    }
}

#[derive(Clone)]
struct PreparedPolicyPackageInstaller {
    inner: Arc<dyn PackageInstaller>,
    controller: PreparedExecutionPolicyController,
}

#[async_trait]
impl PackageInstaller for PreparedPolicyPackageInstaller {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn install(
        &self,
        session: &SandboxSession,
        request: PackageInstallRequest,
    ) -> Result<PackageInstallReport, SandboxError> {
        let metadata = self
            .controller
            .begin_operation(ExecutionOperation::PackageInstall)
            .await?;
        let mut report = self.inner.install(session, request).await?;
        report.metadata.extend(metadata);
        Ok(report)
    }
}

#[derive(Clone)]
struct PreparedPolicyTypeScriptService {
    inner: Arc<dyn TypeScriptService>,
    controller: PreparedExecutionPolicyController,
}

#[async_trait]
impl TypeScriptService for PreparedPolicyTypeScriptService {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn transpile(
        &self,
        session: &SandboxSession,
        request: TypeScriptTranspileRequest,
    ) -> Result<TypeScriptTranspileReport, SandboxError> {
        self.controller
            .begin_operation(ExecutionOperation::TypeCheck)
            .await?;
        self.inner.transpile(session, request).await
    }

    async fn check(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeCheckReport, SandboxError> {
        self.controller
            .begin_operation(ExecutionOperation::TypeCheck)
            .await?;
        self.inner.check(session, request).await
    }

    async fn emit(
        &self,
        session: &SandboxSession,
        request: TypeCheckRequest,
    ) -> Result<TypeScriptEmitReport, SandboxError> {
        self.controller
            .begin_operation(ExecutionOperation::TypeCheck)
            .await?;
        self.inner.emit(session, request).await
    }
}

#[derive(Clone)]
struct PreparedPolicyBashService {
    inner: Arc<dyn BashService>,
    controller: PreparedExecutionPolicyController,
}

#[async_trait]
impl BashService for PreparedPolicyBashService {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn run(
        &self,
        session: &SandboxSession,
        request: BashRequest,
    ) -> Result<BashReport, SandboxError> {
        self.controller
            .begin_operation(ExecutionOperation::BashHelper)
            .await?;
        self.inner.run(session, request).await
    }
}

#[derive(Clone)]
struct PreparedPolicyRuntimeBackend {
    inner: Arc<dyn SandboxRuntimeBackend>,
    controller: PreparedExecutionPolicyController,
}

#[async_trait(?Send)]
impl SandboxRuntimeBackend for PreparedPolicyRuntimeBackend {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn start_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        let mut handle = self.inner.start_session(session).await?;
        handle.metadata.extend(
            self.controller
                .begin_operation(ExecutionOperation::DraftSession)
                .await?,
        );
        Ok(handle)
    }

    async fn resume_session(
        &self,
        session: &SandboxSessionInfo,
    ) -> Result<SandboxRuntimeHandle, SandboxError> {
        let mut handle = self.inner.resume_session(session).await?;
        handle.metadata.extend(
            self.controller
                .begin_operation(ExecutionOperation::DraftSession)
                .await?,
        );
        Ok(handle)
    }

    async fn execute(
        &self,
        session: &SandboxSession,
        handle: &SandboxRuntimeHandle,
        request: SandboxExecutionRequest,
        state: SandboxRuntimeStateHandle,
    ) -> Result<SandboxExecutionResult, SandboxError> {
        let metadata = self
            .controller
            .begin_operation(ExecutionOperation::DraftSession)
            .await?;
        let mut result = self.inner.execute(session, handle, request, state).await?;
        result.metadata.extend(metadata);
        Ok(result)
    }

    async fn close_session(
        &self,
        session: &SandboxSessionInfo,
        handle: &SandboxRuntimeHandle,
    ) -> Result<(), SandboxError> {
        self.inner.close_session(session, handle).await
    }
}

fn apply_prepared_execution_policy(
    services: &mut SandboxServices,
    prepared: &PreparedSessionPreset,
) {
    let controller = PreparedExecutionPolicyController::new(prepared);
    let package_domain = prepared
        .resolved
        .execution_policy
        .assignment_for(ExecutionOperation::PackageInstall)
        .domain;
    let typecheck_domain = prepared
        .resolved
        .execution_policy
        .assignment_for(ExecutionOperation::TypeCheck)
        .domain;
    let bash_domain = prepared
        .resolved
        .execution_policy
        .assignment_for(ExecutionOperation::BashHelper)
        .domain;
    let runtime_domain = prepared
        .resolved
        .execution_policy
        .assignment_for(ExecutionOperation::DraftSession)
        .domain;

    let typescript_backend: Arc<dyn TypeScriptService> =
        Arc::new(DeterministicTypeScriptService::new(format!(
            "notes-typescript-{}",
            execution_domain_label(typecheck_domain)
        )));
    let bash_backend: Arc<dyn BashService> = Arc::new(
        DeterministicBashService::new(format!(
            "notes-bash-{}",
            execution_domain_label(bash_domain)
        ))
        .with_typescript_service(typescript_backend.clone()),
    );

    services.packages = Arc::new(PreparedPolicyPackageInstaller {
        inner: Arc::new(DeterministicPackageInstaller::new(format!(
            "notes-packages-{}",
            execution_domain_label(package_domain)
        ))),
        controller: controller.clone(),
    });
    services.typescript = Arc::new(PreparedPolicyTypeScriptService {
        inner: typescript_backend,
        controller: controller.clone(),
    });
    services.bash = Arc::new(PreparedPolicyBashService {
        inner: bash_backend,
        controller: controller.clone(),
    });
    services.runtime = Arc::new(PreparedPolicyRuntimeBackend {
        inner: Arc::new(DeterministicRuntimeBackend::new(format!(
            "notes-runtime-{}",
            execution_domain_label(runtime_domain)
        ))),
        controller,
    });
}

fn execution_domain_label(domain: ExecutionDomain) -> &'static str {
    match domain {
        ExecutionDomain::OwnerForeground => "owner-foreground",
        ExecutionDomain::SharedBackground => "shared-background",
        ExecutionDomain::DedicatedSandbox => "dedicated-sandbox",
        ExecutionDomain::ProductionIsolate => "production-isolate",
        ExecutionDomain::RemoteWorker => "remote-worker",
    }
}

pub fn sample_notes() -> Vec<ExampleNote> {
    vec![
        ExampleNote {
            id: "note-1".to_string(),
            title: "Add Review Summary Panel".to_string(),
            status: "open".to_string(),
            comments: vec![ExampleComment {
                author: "product".to_string(),
                body: "Keep the panel readable in the readonly editor view.".to_string(),
            }],
        },
        ExampleNote {
            id: "note-2".to_string(),
            title: "Document Repo Export Flow".to_string(),
            status: "open".to_string(),
            comments: vec![],
        },
        ExampleNote {
            id: "note-3".to_string(),
            title: "Archive Deprecated Mockups".to_string(),
            status: "closed".to_string(),
            comments: vec![ExampleComment {
                author: "design".to_string(),
                body: "Already handled in February.".to_string(),
            }],
        },
    ]
}

pub fn companion_project_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("project-template")
}

pub fn unique_temp_path(label: &str) -> PathBuf {
    let id = UNIQUE_PATH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "terracedb-sandbox-notes-{label}-{}-{id}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

pub fn cleanup(path: &Path) {
    let _ = fs::remove_dir_all(path);
}

pub async fn seed_example_project_into_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
) -> Result<(), SandboxError> {
    let base = store
        .open_volume(
            terracedb_vfs::VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    let fs = base.fs();
    for (relative_path, contents) in PROJECT_FILES {
        let target = format!("/workspace/{relative_path}");
        fs.write_file(
            &target,
            contents.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await?;
    }
    Ok(())
}

pub async fn write_companion_project(target: &Path) -> Result<(), SandboxError> {
    for (relative_path, contents) in PROJECT_FILES {
        let path = target.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| SandboxError::Io {
                path: parent.to_string_lossy().into_owned(),
                message: error.to_string(),
            })?;
        }
        fs::write(&path, contents).map_err(|error| SandboxError::Io {
            path: path.to_string_lossy().into_owned(),
            message: error.to_string(),
        })?;
    }
    Ok(())
}

pub async fn hoist_companion_project(session: &SandboxSession) -> Result<(), SandboxError> {
    session
        .hoist_from_disk(HoistRequest {
            source_path: companion_project_path().to_string_lossy().into_owned(),
            target_root: "/workspace".to_string(),
            mode: HoistMode::DirectorySnapshot,
            delete_missing: true,
        })
        .await?;
    Ok(())
}

pub async fn install_example_packages(
    session: &SandboxSession,
) -> Result<Vec<String>, SandboxError> {
    Ok(session
        .install_packages(PackageInstallRequest {
            packages: vec!["lodash".to_string(), "zod".to_string()],
            materialize_compatibility_view: true,
        })
        .await?
        .packages)
}

pub async fn typecheck_example(session: &SandboxSession) -> Result<TypeCheckReport, SandboxError> {
    let report = session
        .typecheck(TypeCheckRequest {
            roots: vec![TYPESCRIPT_ENTRYPOINT.to_string()],
            ..Default::default()
        })
        .await?;
    if !report.diagnostics.is_empty() {
        return Err(SandboxError::Service {
            service: "sandbox-notes",
            message: format!(
                "expected clean TypeScript example, found {} diagnostics",
                report.diagnostics.len()
            ),
        });
    }
    Ok(report)
}

pub async fn emit_example_typescript(
    session: &SandboxSession,
) -> Result<TypeScriptEmitReport, SandboxError> {
    session
        .emit_typescript(TypeCheckRequest {
            roots: vec![TYPESCRIPT_ENTRYPOINT.to_string()],
            ..Default::default()
        })
        .await
}

pub async fn run_example_bash(session: &SandboxSession) -> Result<BashReport, SandboxError> {
    session
        .run_bash(BashRequest {
            command: "mkdir -p docs && printf '# Review Notes\\nGenerated from the sandbox example.\\n' > docs/review-notes.md && pwd".to_string(),
            cwd: "/workspace".to_string(),
            ..Default::default()
        })
        .await
}

pub async fn run_example_review(
    session: &SandboxSession,
) -> Result<(ReviewSummary, JsonValue), SandboxError> {
    let result = session.exec_module(REVIEW_ENTRYPOINT).await?;
    let value = result.result.ok_or_else(|| SandboxError::Service {
        service: "sandbox-notes",
        message: "runtime result should contain a JSON payload".to_string(),
    })?;
    let summary =
        serde_json::from_value::<ReviewSummary>(value.get("summary").cloned().ok_or_else(
            || SandboxError::Service {
                service: "sandbox-notes",
                message: "runtime result should include a summary".to_string(),
            },
        )?)?;
    Ok((summary, value))
}

pub async fn open_generated_view(
    session: &SandboxSession,
) -> Result<ReadonlyViewHandle, SandboxError> {
    session
        .open_readonly_view(ReadonlyViewRequest {
            cut: terracedb_sandbox::ReadonlyViewCut::Visible,
            path: "/workspace/generated".to_string(),
            label: Some("generated".to_string()),
        })
        .await
}

pub async fn prepare_example_git_workspace(
    session: &SandboxSession,
    target_path: &Path,
) -> Result<String, SandboxError> {
    Ok(session
        .prepare_git_workspace(GitWorkspaceRequest {
            branch_name: "sandbox/example-notes".to_string(),
            base_branch: Some("main".to_string()),
            target_path: target_path.to_string_lossy().into_owned(),
        })
        .await?
        .workspace_path)
}

pub async fn create_example_pull_request(session: &SandboxSession) -> Result<String, SandboxError> {
    Ok(session
        .create_pull_request(PullRequestRequest {
            title: "Sandbox notes update".to_string(),
            body: "Generated from the sandbox-notes example.".to_string(),
            head_branch: "sandbox/example-notes".to_string(),
            base_branch: "main".to_string(),
        })
        .await?
        .url)
}

pub async fn read_summary_from_session(
    session: &SandboxSession,
) -> Result<ReviewSummary, SandboxError> {
    let bytes = session
        .filesystem()
        .read_file(GENERATED_SUMMARY_PATH)
        .await?
        .ok_or_else(|| SandboxError::Service {
            service: "sandbox-notes",
            message: "summary file should exist".to_string(),
        })?;
    serde_json::from_slice(&bytes).map_err(Into::into)
}

pub async fn run_demo() -> Result<DemoReport, SandboxError> {
    let app = ExampleHostApp::sample();
    let base_volume_id = VolumeId::new(0x5a00);
    let session_volume_id = VolumeId::new(0x5a01);
    let policy_session_id = format!("sandbox-notes-demo-{session_volume_id}");
    let prepared = app.prepare_notes_draft_session(policy_session_id.clone())?;
    let services = app.deterministic_services_for_prepared_session(policy_session_id, &prepared)?;
    let sandbox_manifest = services.capabilities.manifest();
    let harness = SandboxHarness::deterministic(1_000, 41, services);
    let session = harness
        .open_session_with(base_volume_id, session_volume_id, |config| {
            config
                .with_chunk_size(4096)
                .with_package_compat(PackageCompatibilityMode::NpmPureJs)
                .with_capabilities(sandbox_manifest)
        })
        .await?;

    hoist_companion_project(&session).await?;
    install_example_packages(&session).await?;
    typecheck_example(&session).await?;
    let emitted = emit_example_typescript(&session).await?;
    let (summary, runtime_result) = run_example_review(&session).await?;
    run_example_bash(&session).await?;
    let handle = open_generated_view(&session).await?;

    let export_path = unique_temp_path("demo-export");
    fs::create_dir_all(&export_path).map_err(|error| SandboxError::Io {
        path: export_path.to_string_lossy().into_owned(),
        message: error.to_string(),
    })?;
    session
        .eject_to_disk(EjectRequest {
            target_path: export_path.to_string_lossy().into_owned(),
            mode: EjectMode::MaterializeSnapshot,
            conflict_policy: ConflictPolicy::Fail,
        })
        .await?;

    Ok(DemoReport {
        summary,
        runtime_result,
        emitted_files: emitted.emitted_files,
        view_uri: handle.uri,
        export_path: export_path.to_string_lossy().into_owned(),
        notes: app.notes_snapshot().await,
    })
}

pub async fn direct_add_comment(
    session: &SandboxSession,
    note_id: &str,
    author: &str,
    body: &str,
) -> Result<JsonValue, SandboxError> {
    let note = add_comment_method()
        .invoke(
            session,
            &AddCommentInput {
                note_id: note_id.to_string(),
                author: author.to_string(),
                body: body.to_string(),
            },
        )
        .await?;
    serde_json::to_value(note).map_err(Into::into)
}

pub async fn guest_add_comment(
    session: &SandboxSession,
    note_id: &str,
    author: &str,
    body: &str,
) -> Result<JsonValue, SandboxError> {
    Ok(session
        .eval(format!(
            r#"
            import {{ addComment }} from "{specifier}";
            export default addComment({{
              noteId: "{note_id}",
              author: "{author}",
              body: "{body}",
            }});
            "#,
            specifier = NOTES_CAPABILITY_SPECIFIER
        ))
        .await?
        .result
        .ok_or_else(|| SandboxError::Service {
            service: "sandbox-notes",
            message: "guest capability call should return a result".to_string(),
        })?)
}

fn notes_template() -> CapabilityTemplate {
    CapabilityTemplate {
        template_id: NOTES_TEMPLATE_ID.to_string(),
        capability_family: "sandbox.notes.v1".to_string(),
        default_binding: "notes".to_string(),
        description: Some("Read and update the example host note store.".to_string()),
        default_resource_policy: ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Custom,
                pattern: "notes".to_string(),
            }],
            deny: vec![],
            tenant_scopes: vec![NOTES_TENANT_ID.to_string()],
            row_scope_binding: None,
            visibility_index: None,
            metadata: BTreeMap::from([("example".to_string(), JsonValue::from(true))]),
        },
        default_budget_policy: BudgetPolicy {
            max_calls: Some(16),
            max_scanned_rows: None,
            max_returned_rows: None,
            max_bytes: Some(8192),
            max_millis: Some(500),
            rate_limit_bucket: Some("notes-draft".to_string()),
            labels: BTreeMap::from([("surface".to_string(), "example".to_string())]),
        },
        expose_in_just_bash: false,
        metadata: BTreeMap::from([("example".to_string(), JsonValue::from("sandbox-notes"))]),
    }
}

fn notes_grant() -> CapabilityGrant {
    CapabilityGrant {
        grant_id: "grant-notes-example".to_string(),
        subject: SubjectSelector::Exact {
            subject_id: NOTES_SUBJECT_ID.to_string(),
        },
        template_id: NOTES_TEMPLATE_ID.to_string(),
        binding_name: Some("notes".to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: false,
        metadata: BTreeMap::new(),
    }
}

fn notes_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: Some(BudgetPolicy {
                max_calls: Some(32),
                max_scanned_rows: None,
                max_returned_rows: None,
                max_bytes: Some(16_384),
                max_millis: Some(1_000),
                rate_limit_bucket: Some("notes-draft".to_string()),
                labels: BTreeMap::from([("lane".to_string(), "draft".to_string())]),
            }),
            placement_tags: vec!["example".to_string(), "draft".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([
            (
                ExecutionOperation::DraftSession,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: None,
                    placement_tags: vec!["foreground".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::BashHelper,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::OwnerForeground,
                    budget: None,
                    placement_tags: vec!["tooling".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::TypeCheck,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::OwnerForeground,
                    budget: None,
                    placement_tags: vec!["tooling".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
        ]),
        metadata: BTreeMap::from([("example".to_string(), JsonValue::from("sandbox-notes"))]),
    }
}

fn notes_preset() -> CapabilityPresetDescriptor {
    CapabilityPresetDescriptor {
        name: NOTES_PRESET_NAME.to_string(),
        description: Some("Named preset for the sandbox-notes authoring flow.".to_string()),
        bindings: vec![PresetBinding {
            template_id: NOTES_TEMPLATE_ID.to_string(),
            binding_name: Some("notes".to_string()),
            resource_policy: Some(ResourcePolicy {
                allow: vec![ResourceSelector {
                    kind: ResourceKind::Custom,
                    pattern: "notes".to_string(),
                }],
                deny: vec![],
                tenant_scopes: vec![NOTES_TENANT_ID.to_string()],
                row_scope_binding: None,
                visibility_index: None,
                metadata: BTreeMap::new(),
            }),
            budget_policy: Some(BudgetPolicy {
                max_calls: Some(8),
                max_scanned_rows: None,
                max_returned_rows: None,
                max_bytes: Some(4096),
                max_millis: Some(250),
                rate_limit_bucket: Some("notes-draft".to_string()),
                labels: BTreeMap::new(),
            }),
            expose_in_just_bash: Some(false),
        }],
        default_session_mode: SessionMode::Draft,
        default_execution_policy: Some(notes_execution_policy()),
        metadata: BTreeMap::from([("example".to_string(), JsonValue::from("sandbox-notes"))]),
    }
}

fn notes_profile() -> CapabilityProfileDescriptor {
    CapabilityProfileDescriptor {
        name: NOTES_PROFILE_NAME.to_string(),
        preset_name: NOTES_PRESET_NAME.to_string(),
        add_bindings: vec![],
        drop_bindings: vec![],
        execution_policy_override: Some(notes_execution_policy()),
        metadata: BTreeMap::from([("profile".to_string(), JsonValue::from("foreground"))]),
    }
}

fn capability_policy_metadata(
    prepared: &PreparedSessionPreset,
    binding_name: &str,
    decision: Option<&PolicyDecisionRecord>,
) -> Result<BTreeMap<String, JsonValue>, SandboxError> {
    let mut metadata = BTreeMap::from([
        (
            "binding_name".to_string(),
            JsonValue::String(binding_name.to_string()),
        ),
        (
            "preset_name".to_string(),
            JsonValue::String(prepared.preset.name.clone()),
        ),
        (
            "expanded_manifest".to_string(),
            serde_json::to_value(&prepared.resolved.manifest)?,
        ),
        (
            "execution_policy".to_string(),
            serde_json::to_value(&prepared.resolved.execution_policy)?,
        ),
    ]);
    if let Some(profile) = prepared.profile.as_ref() {
        metadata.insert(
            "profile_name".to_string(),
            JsonValue::String(profile.name.clone()),
        );
    }
    if let Some(decision) = decision {
        metadata.insert(
            "policy_outcome".to_string(),
            serde_json::to_value(&decision.outcome)?,
        );
        metadata.insert(
            "policy_audit".to_string(),
            serde_json::to_value(&decision.audit)?,
        );
    }
    Ok(metadata)
}

fn notes_capability() -> SandboxCapability {
    SandboxCapability::host_module("notes")
        .with_description("Host application note store used by the sandbox-notes example.")
        .with_typescript_declarations(
            [
                "export type ExampleComment = { author: string; body: string };",
                "export type ExampleNote = { id: string; title: string; status: string; comments: ExampleComment[] };",
                "export function listNotes(): ExampleNote[];",
                "export function addComment(input: { noteId: string; author: string; body: string }): ExampleNote;",
            ]
            .join("\n"),
        )
        .with_metadata("example", JsonValue::from("sandbox-notes"))
}

fn list_notes_method() -> CapabilityMethod0<Vec<ExampleNote>> {
    notes_capability().method0("listNotes")
}

fn add_comment_method() -> CapabilityMethod1<AddCommentInput, ExampleNote> {
    notes_capability().method1("addComment")
}

fn build_notes_registry(notes: NotesState) -> Result<NotesRegistry, SandboxError> {
    let capability = notes_capability();
    let list_notes = list_notes_method();
    let add_comment = add_comment_method();
    let module = TypedCapabilityModuleBuilder::new(capability)?
        .with_module_metadata("kind", JsonValue::from("example-note-store"))
        .with_method0(
            &list_notes,
            Some("Return the host application's current notes."),
            move |notes: NotesState, _session| async move { Ok(notes.lock().await.clone()) },
        )?
        .with_method1(
            &add_comment,
            Some("Append a host-side comment to a note."),
            move |notes: NotesState, _session, input: AddCommentInput| async move {
                let mut notes = notes.lock().await;
                let note = notes
                    .iter_mut()
                    .find(|note| note.id == input.note_id)
                    .ok_or_else(|| SandboxError::Service {
                        service: "sandbox-notes",
                        message: format!("unknown note id: {}", input.note_id),
                    })?;
                note.comments.push(ExampleComment {
                    author: input.author,
                    body: input.body,
                });
                Ok(note.clone())
            },
        )?
        .build();
    TypedCapabilityRegistry::new(notes, vec![module])
}
