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
use serde_json::{Value as JsonValue, json};
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    BashReport, BashRequest, BashService, CapabilityCallRequest, CapabilityCallResult,
    CapabilityManifest, CapabilityRegistry, ConflictPolicy, DefaultSandboxStore,
    DeterministicBashService, DeterministicGitWorkspaceManager, DeterministicPackageInstaller,
    DeterministicPullRequestProviderClient, DeterministicReadonlyViewProvider,
    DeterministicRuntimeBackend, DeterministicTypeScriptService, EjectMode, EjectRequest,
    GitWorkspaceRequest, HoistMode, HoistRequest, HostGitWorkspaceManager,
    PackageCompatibilityMode, PackageInstallRequest, PullRequestRequest, ReadonlyViewHandle,
    ReadonlyViewRequest, SandboxCapability, SandboxCapabilityMethod, SandboxCapabilityModule,
    SandboxConfig, SandboxError, SandboxServices, SandboxSession, SandboxStore, TypeCheckRequest,
    TypeScriptEmitReport, TypeScriptService,
};
use terracedb_vfs::{
    CreateOptions, InMemoryVfsStore, MkdirOptions, VolumeConfig, VolumeId, VolumeStore,
};
use tokio::sync::Mutex;

use crate::model::{AddCommentInput, DemoReport, ExampleComment, ExampleNote, ReviewSummary};

pub const NOTES_CAPABILITY_SPECIFIER: &str = "terrace:host/notes";
pub const REVIEW_ENTRYPOINT: &str = "/workspace/src/review.js";
pub const TYPESCRIPT_ENTRYPOINT: &str = "/workspace/src/render.ts";
pub const GENERATED_SUMMARY_PATH: &str = "/workspace/generated/triage-summary.json";
pub const GENERATED_REVIEW_NOTES_PATH: &str = "/workspace/docs/review-notes.md";

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

#[derive(Clone, Default)]
pub struct NotesCapabilityRegistry {
    notes: Arc<Mutex<Vec<ExampleNote>>>,
}

impl NotesCapabilityRegistry {
    pub fn new(notes: Vec<ExampleNote>) -> Self {
        Self {
            notes: Arc::new(Mutex::new(notes)),
        }
    }

    pub async fn snapshot(&self) -> Vec<ExampleNote> {
        self.notes.lock().await.clone()
    }

    pub fn capability() -> SandboxCapability {
        let mut capability = SandboxCapability::host_module("notes");
        capability.description =
            Some("Host application note store used by the sandbox-notes example.".to_string());
        capability.typescript_declarations = Some(
            [
                "export type ExampleComment = { author: string; body: string };",
                "export type ExampleNote = { id: string; title: string; status: string; comments: ExampleComment[] };",
                "export function listNotes(): ExampleNote[];",
                "export function addComment(input: { noteId: string; author: string; body: string }): ExampleNote;",
            ]
            .join("\n"),
        );
        capability
            .metadata
            .insert("example".to_string(), JsonValue::from("sandbox-notes"));
        capability
    }

    fn capability_module() -> SandboxCapabilityModule {
        SandboxCapabilityModule {
            capability: Self::capability(),
            methods: vec![
                SandboxCapabilityMethod {
                    name: "listNotes".to_string(),
                    description: Some("Return the host application's current notes.".to_string()),
                },
                SandboxCapabilityMethod {
                    name: "addComment".to_string(),
                    description: Some("Append a host-side comment to a note.".to_string()),
                },
            ],
            metadata: BTreeMap::from([("kind".to_string(), JsonValue::from("example-note-store"))]),
        }
    }
}

#[async_trait]
impl CapabilityRegistry for NotesCapabilityRegistry {
    fn manifest(&self) -> CapabilityManifest {
        CapabilityManifest {
            capabilities: vec![Self::capability()],
        }
    }

    fn resolve(&self, specifier: &str) -> Option<SandboxCapability> {
        (specifier == NOTES_CAPABILITY_SPECIFIER).then(Self::capability)
    }

    fn module(&self, specifier: &str) -> Option<SandboxCapabilityModule> {
        (specifier == NOTES_CAPABILITY_SPECIFIER).then(Self::capability_module)
    }

    async fn invoke(
        &self,
        _session: &SandboxSession,
        request: CapabilityCallRequest,
    ) -> Result<CapabilityCallResult, SandboxError> {
        if request.specifier != NOTES_CAPABILITY_SPECIFIER {
            return Err(SandboxError::CapabilityUnavailable {
                specifier: request.specifier,
            });
        }

        let value = match request.method.as_str() {
            "listNotes" => {
                let notes = self.notes.lock().await.clone();
                serde_json::to_value(notes)?
            }
            "addComment" => {
                let input: AddCommentInput =
                    serde_json::from_value(request.args.first().cloned().ok_or_else(|| {
                        SandboxError::Service {
                            service: "sandbox-notes",
                            message: "addComment expects one object argument".to_string(),
                        }
                    })?)?;
                let mut notes = self.notes.lock().await;
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
                serde_json::to_value(note.clone())?
            }
            _ => {
                return Err(SandboxError::CapabilityMethodNotFound {
                    specifier: request.specifier,
                    method: request.method,
                });
            }
        };

        Ok(CapabilityCallResult {
            specifier: request.specifier,
            method: request.method,
            value,
            metadata: BTreeMap::from([("example".to_string(), JsonValue::from("sandbox-notes"))]),
        })
    }
}

#[derive(Clone)]
pub struct ExampleHostApp {
    notes: Arc<NotesCapabilityRegistry>,
}

impl ExampleHostApp {
    pub fn new(notes: Vec<ExampleNote>) -> Self {
        Self {
            notes: Arc::new(NotesCapabilityRegistry::new(notes)),
        }
    }

    pub fn sample() -> Self {
        Self::new(sample_notes())
    }

    pub fn notes_registry(&self) -> Arc<NotesCapabilityRegistry> {
        self.notes.clone()
    }

    pub async fn notes_snapshot(&self) -> Vec<ExampleNote> {
        self.notes.snapshot().await
    }

    pub fn deterministic_services(&self) -> SandboxServices {
        SandboxServices::new(
            Arc::new(DeterministicRuntimeBackend::default()),
            Arc::new(DeterministicPackageInstaller::default()),
            Arc::new(DeterministicGitWorkspaceManager::default()),
            Arc::new(DeterministicPullRequestProviderClient::default()),
            Arc::new(DeterministicReadonlyViewProvider::default()),
        )
        .with_capabilities(self.notes.clone())
    }

    pub fn host_git_services(&self) -> SandboxServices {
        SandboxServices::new(
            Arc::new(DeterministicRuntimeBackend::default()),
            Arc::new(DeterministicPackageInstaller::default()),
            Arc::new(HostGitWorkspaceManager::default()),
            Arc::new(DeterministicPullRequestProviderClient::default()),
            Arc::new(DeterministicReadonlyViewProvider::default()),
        )
        .with_capabilities(self.notes.clone())
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

pub async fn in_memory_store(
    now: u64,
    seed: u64,
    services: SandboxServices,
) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(Arc::new(vfs.clone()), dependencies.clock, services);
    (vfs, sandbox)
}

pub async fn create_empty_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
) -> Result<(), SandboxError> {
    store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await?;
    Ok(())
}

pub async fn seed_example_project_into_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
) -> Result<(), SandboxError> {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
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

pub async fn typecheck_example(
    session: &SandboxSession,
) -> Result<DeterministicTypeScriptService, SandboxError> {
    let typescript = DeterministicTypeScriptService::default();
    let diagnostics = typescript
        .check(
            session,
            TypeCheckRequest {
                roots: vec![TYPESCRIPT_ENTRYPOINT.to_string()],
                ..Default::default()
            },
        )
        .await?;
    if !diagnostics.diagnostics.is_empty() {
        return Err(SandboxError::Service {
            service: "sandbox-notes",
            message: format!(
                "expected clean TypeScript example, found {} diagnostics",
                diagnostics.diagnostics.len()
            ),
        });
    }
    Ok(typescript)
}

pub async fn emit_example_typescript(
    typescript: &DeterministicTypeScriptService,
    session: &SandboxSession,
) -> Result<TypeScriptEmitReport, SandboxError> {
    typescript
        .emit(
            session,
            TypeCheckRequest {
                roots: vec![TYPESCRIPT_ENTRYPOINT.to_string()],
                ..Default::default()
            },
        )
        .await
}

pub async fn run_example_bash(session: &SandboxSession) -> Result<BashReport, SandboxError> {
    let bash = DeterministicBashService::default();
    bash.run(
        session,
        BashRequest {
            command: "mkdir -p docs && printf '# Review Notes\\nGenerated from the sandbox example.\\n' > docs/review-notes.md && pwd".to_string(),
            cwd: "/workspace".to_string(),
            ..Default::default()
        },
    )
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
    let (_vfs, sandbox) = in_memory_store(1_000, 41, app.deterministic_services()).await;
    let base_volume_id = VolumeId::new(0x5a00);
    let session_volume_id = VolumeId::new(0x5a01);
    create_empty_base(&_vfs, base_volume_id).await?;

    let session = sandbox
        .open_session(
            SandboxConfig::new(base_volume_id, session_volume_id)
                .with_chunk_size(4096)
                .with_package_compat(PackageCompatibilityMode::NpmPureJs),
        )
        .await?;
    hoist_companion_project(&session).await?;
    install_example_packages(&session).await?;
    let typescript = typecheck_example(&session).await?;
    let emitted = emit_example_typescript(&typescript, &session).await?;
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
    Ok(session
        .invoke_capability(CapabilityCallRequest {
            specifier: NOTES_CAPABILITY_SPECIFIER.to_string(),
            method: "addComment".to_string(),
            args: vec![json!({
                "noteId": note_id,
                "author": author,
                "body": body,
            })],
        })
        .await?
        .value)
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

pub async fn ensure_workspace_dir(session: &SandboxSession) -> Result<(), SandboxError> {
    session
        .filesystem()
        .mkdir(
            "/workspace",
            MkdirOptions {
                recursive: true,
                ..Default::default()
            },
        )
        .await
}
