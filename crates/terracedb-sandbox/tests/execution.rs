use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{
    DbDependencies, DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionResourceUsage, InMemoryResourceManager, StubClock,
    StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_capabilities::{
    BudgetPolicy, CapabilityManifest as PolicyCapabilityManifest, CapabilityUseMetrics,
    DatabaseAccessRequest, DatabaseActionKind, DatabaseTarget, DeterministicTypedRowPredicate,
    DeterministicTypedRowPredicateEvaluator, DeterministicVisibilityIndexStore, ExecutionDomain,
    ExecutionDomainAssignment, ExecutionOperation, ExecutionPolicy, ManifestBinding,
    PolicyOutcomeKind, PolicySubject, ResolvedSessionPolicy, ResourceKind, ResourcePolicy,
    ResourceSelector, RowScopeBinding, RowScopePolicy, SessionMode, ShellCommandDescriptor,
    capability_module_specifier,
};
use terracedb_sandbox::{
    BashRequest, CapabilityRegistry, ConflictPolicy, DefaultSandboxStore, DeterministicBashService,
    DeterministicCapabilityModule, DeterministicCapabilityRegistry, DeterministicPackageInstaller,
    DeterministicRuntimeBackend, DeterministicTypeScriptService, ManifestBoundCapabilityDispatcher,
    ManifestBoundCapabilityInvocation, ManifestBoundCapabilityRegistry,
    ManifestBoundCapabilityResult, PackageCompatibilityMode, ReopenSessionOptions,
    SandboxCapability, SandboxConfig, SandboxError, SandboxExecutionDomainRoute,
    SandboxExecutionPlacement, SandboxExecutionRouter, SandboxServices, SandboxStore,
    TERRACE_RUNTIME_MODULE_CACHE_PATH, TypeCheckRequest,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};
use tokio::sync::Mutex;

fn sandbox_store(
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

async fn seed_base(
    store: &InMemoryVfsStore,
    volume_id: VolumeId,
    main_source: &str,
    extra_files: &[(&str, &str)],
) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base volume");
    let fs = base.fs();
    fs.write_file(
        "/workspace/main.js",
        main_source.as_bytes().to_vec(),
        CreateOptions {
            create_parents: true,
            ..Default::default()
        },
    )
    .await
    .expect("write main module");
    for (path, contents) in extra_files {
        fs.write_file(
            path,
            contents.as_bytes().to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("write extra file");
    }
}

fn deterministic_capabilities() -> DeterministicCapabilityRegistry {
    DeterministicCapabilityRegistry::new(vec![
        DeterministicCapabilityModule::new(SandboxCapability::host_module("tickets"))
            .expect("valid capability")
            .with_echo_method("echo"),
    ])
    .expect("build registry")
}

#[derive(Clone)]
struct DeterministicPolicyDbDispatcher {
    policy: ResolvedSessionPolicy,
    predicates: DeterministicTypedRowPredicateEvaluator,
    visibility: DeterministicVisibilityIndexStore,
    tables: Arc<Mutex<BTreeMap<String, BTreeMap<String, serde_json::Value>>>>,
}

impl DeterministicPolicyDbDispatcher {
    fn new(policy: ResolvedSessionPolicy) -> Self {
        Self {
            policy,
            predicates: DeterministicTypedRowPredicateEvaluator::default(),
            visibility: DeterministicVisibilityIndexStore::default(),
            tables: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn with_predicates(mut self, predicates: DeterministicTypedRowPredicateEvaluator) -> Self {
        self.predicates = predicates;
        self
    }

    async fn seed_row(&self, table: &str, key: &str, row: serde_json::Value) {
        let mut tables = self.tables.lock().await;
        tables
            .entry(table.to_string())
            .or_default()
            .insert(key.to_string(), row);
    }
}

#[async_trait]
impl ManifestBoundCapabilityDispatcher for DeterministicPolicyDbDispatcher {
    async fn invoke_binding(
        &self,
        _session: &terracedb_sandbox::SandboxSession,
        binding: &ManifestBinding,
        request: ManifestBoundCapabilityInvocation,
    ) -> Result<ManifestBoundCapabilityResult, SandboxError> {
        match (binding.capability_family.as_str(), request.method.as_str()) {
            ("db.table.v1", "get") => self.handle_get(binding, request).await,
            ("db.table.v1", "put") => self.handle_put(binding, request).await,
            _ => Err(SandboxError::Service {
                service: "capabilities",
                message: format!(
                    "deterministic test dispatcher does not implement {}::{}",
                    binding.capability_family, request.method
                ),
            }),
        }
    }
}

impl DeterministicPolicyDbDispatcher {
    async fn handle_get(
        &self,
        binding: &ManifestBinding,
        request: ManifestBoundCapabilityInvocation,
    ) -> Result<ManifestBoundCapabilityResult, SandboxError> {
        let input = parse_single_argument_object(&request)?;
        let requested_key = input
            .get("key")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SandboxError::Service {
                service: "capabilities",
                message: "db.table.v1 get requires a string key".to_string(),
            })?
            .to_string();
        let table = fixed_table_name(binding, &input);

        let preflight = self
            .policy
            .evaluate_database_access(
                &DatabaseAccessRequest {
                    session_id: "session-1".to_string(),
                    operation: terracedb_capabilities::ExecutionOperation::DraftSession,
                    binding_name: binding.binding_name.clone(),
                    capability_family: Some(binding.capability_family.clone()),
                    action: DatabaseActionKind::PointRead,
                    target: DatabaseTarget {
                        database: None,
                        table: Some(table.clone()),
                        procedure_id: None,
                    },
                    key: Some(requested_key.clone()),
                    prefix: None,
                    row_id: None,
                    candidate_row: None,
                    preimage: None,
                    postimage: None,
                    occ_read_set: vec![],
                    metrics: CapabilityUseMetrics::default(),
                    requested_at: Timestamp::new(200),
                    metadata: BTreeMap::new(),
                },
                &self.predicates,
                &self.visibility,
            )
            .map_err(policy_error)?;
        if preflight.policy.outcome.outcome != PolicyOutcomeKind::Allowed {
            return policy_denied(preflight);
        }
        let effective_key = preflight
            .effective_key
            .clone()
            .unwrap_or_else(|| requested_key.clone());
        let row = self
            .tables
            .lock()
            .await
            .get(&table)
            .and_then(|rows| rows.get(&effective_key))
            .cloned();

        let evaluated = self
            .policy
            .evaluate_database_access(
                &DatabaseAccessRequest {
                    session_id: "session-1".to_string(),
                    operation: terracedb_capabilities::ExecutionOperation::DraftSession,
                    binding_name: binding.binding_name.clone(),
                    capability_family: Some(binding.capability_family.clone()),
                    action: DatabaseActionKind::PointRead,
                    target: DatabaseTarget {
                        database: None,
                        table: Some(table),
                        procedure_id: None,
                    },
                    key: Some(requested_key),
                    prefix: None,
                    row_id: Some(effective_key.clone()),
                    candidate_row: row.clone(),
                    preimage: None,
                    postimage: None,
                    occ_read_set: vec![],
                    metrics: CapabilityUseMetrics::default(),
                    requested_at: Timestamp::new(201),
                    metadata: BTreeMap::new(),
                },
                &self.predicates,
                &self.visibility,
            )
            .map_err(policy_error)?;

        match evaluated.policy.outcome.outcome {
            PolicyOutcomeKind::Allowed => Ok(ManifestBoundCapabilityResult {
                value: json!({
                    "key": effective_key,
                    "row": row,
                }),
                metadata: decision_metadata(&evaluated),
            }),
            PolicyOutcomeKind::NotVisible => Ok(ManifestBoundCapabilityResult {
                value: json!({
                    "key": effective_key,
                    "row": serde_json::Value::Null,
                }),
                metadata: decision_metadata(&evaluated),
            }),
            _ => policy_denied(evaluated),
        }
    }

    async fn handle_put(
        &self,
        binding: &ManifestBinding,
        request: ManifestBoundCapabilityInvocation,
    ) -> Result<ManifestBoundCapabilityResult, SandboxError> {
        let input = parse_single_argument_object(&request)?;
        let requested_key = input
            .get("key")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| SandboxError::Service {
                service: "capabilities",
                message: "db.table.v1 put requires a string key".to_string(),
            })?
            .to_string();
        let row = input
            .get("row")
            .cloned()
            .ok_or_else(|| SandboxError::Service {
                service: "capabilities",
                message: "db.table.v1 put requires a row payload".to_string(),
            })?;
        let occ_read_set = input
            .get("occReadSet")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_string))
            .collect::<Vec<_>>();
        let table = fixed_table_name(binding, &input);

        let preflight = self
            .policy
            .evaluate_database_access(
                &DatabaseAccessRequest {
                    session_id: "session-1".to_string(),
                    operation: terracedb_capabilities::ExecutionOperation::DraftSession,
                    binding_name: binding.binding_name.clone(),
                    capability_family: Some(binding.capability_family.clone()),
                    action: DatabaseActionKind::WriteMutation,
                    target: DatabaseTarget {
                        database: None,
                        table: Some(table.clone()),
                        procedure_id: None,
                    },
                    key: Some(requested_key.clone()),
                    prefix: None,
                    row_id: None,
                    candidate_row: None,
                    preimage: None,
                    postimage: Some(row.clone()),
                    occ_read_set: occ_read_set.clone(),
                    metrics: CapabilityUseMetrics::default(),
                    requested_at: Timestamp::new(202),
                    metadata: BTreeMap::new(),
                },
                &self.predicates,
                &self.visibility,
            )
            .map_err(policy_error)?;
        if preflight.policy.outcome.outcome != PolicyOutcomeKind::Allowed {
            return policy_denied(preflight);
        }
        let effective_key = preflight
            .effective_key
            .clone()
            .unwrap_or_else(|| requested_key.clone());
        let preimage = self
            .tables
            .lock()
            .await
            .get(&table)
            .and_then(|rows| rows.get(&effective_key))
            .cloned();

        let evaluated = self
            .policy
            .evaluate_database_access(
                &DatabaseAccessRequest {
                    session_id: "session-1".to_string(),
                    operation: terracedb_capabilities::ExecutionOperation::DraftSession,
                    binding_name: binding.binding_name.clone(),
                    capability_family: Some(binding.capability_family.clone()),
                    action: DatabaseActionKind::WriteMutation,
                    target: DatabaseTarget {
                        database: None,
                        table: Some(table.clone()),
                        procedure_id: None,
                    },
                    key: Some(requested_key),
                    prefix: None,
                    row_id: Some(effective_key.clone()),
                    candidate_row: None,
                    preimage,
                    postimage: Some(row.clone()),
                    occ_read_set,
                    metrics: CapabilityUseMetrics::default(),
                    requested_at: Timestamp::new(203),
                    metadata: BTreeMap::new(),
                },
                &self.predicates,
                &self.visibility,
            )
            .map_err(policy_error)?;
        if evaluated.policy.outcome.outcome != PolicyOutcomeKind::Allowed {
            return policy_denied(evaluated);
        }

        let mut tables = self.tables.lock().await;
        tables
            .entry(table)
            .or_default()
            .insert(effective_key.clone(), row.clone());

        Ok(ManifestBoundCapabilityResult {
            value: json!({
                "key": effective_key,
                "row": row,
                "written": true,
            }),
            metadata: decision_metadata(&evaluated),
        })
    }
}

fn parse_single_argument_object(
    request: &ManifestBoundCapabilityInvocation,
) -> Result<serde_json::Map<String, serde_json::Value>, SandboxError> {
    if request.args.len() != 1 {
        return Err(SandboxError::Service {
            service: "capabilities",
            message: format!("{} expects exactly one argument", request.method),
        });
    }
    request.args[0]
        .as_object()
        .cloned()
        .ok_or_else(|| SandboxError::Service {
            service: "capabilities",
            message: format!("{} expects an object argument", request.method),
        })
}

fn fixed_table_name(
    binding: &ManifestBinding,
    input: &serde_json::Map<String, serde_json::Value>,
) -> String {
    input
        .get("table")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            binding
                .resource_policy
                .allow
                .iter()
                .find(|selector| selector.kind == ResourceKind::Table)
                .map(|selector| selector.pattern.clone())
        })
        .unwrap_or_else(|| binding.binding_name.clone())
}

fn decision_metadata(
    decision: &terracedb_capabilities::DatabaseAccessDecision,
) -> BTreeMap<String, serde_json::Value> {
    let mut metadata = BTreeMap::from([(
        "policy_outcome".to_string(),
        serde_json::to_value(&decision.policy.outcome).expect("encode policy outcome"),
    )]);
    if let Some(key) = decision.effective_key.as_ref() {
        metadata.insert("effective_key".to_string(), json!(key));
    }
    if let Some(prefix) = decision.effective_prefix.as_ref() {
        metadata.insert("effective_prefix".to_string(), json!(prefix));
    }
    metadata
}

fn policy_denied(
    decision: terracedb_capabilities::DatabaseAccessDecision,
) -> Result<ManifestBoundCapabilityResult, SandboxError> {
    Err(SandboxError::Service {
        service: "capabilities",
        message: format!(
            "policy {:?}: {}",
            decision.policy.outcome.outcome,
            decision
                .policy
                .outcome
                .message
                .unwrap_or_else(|| "host policy denied the request".to_string())
        ),
    })
}

fn policy_error(error: terracedb_capabilities::DatabasePolicyError) -> SandboxError {
    SandboxError::Service {
        service: "capabilities",
        message: error.to_string(),
    }
}

#[tokio::test]
async fn guest_modules_read_write_vfs_and_call_explicit_capabilities() {
    let registry = deterministic_capabilities();
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(100, 51, services);
    let base_volume_id = VolumeId::new(0x9000);
    let session_volume_id = VolumeId::new(0x9001);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { readTextFile, writeTextFile } from "@terracedb/sandbox/fs";
        import { echo } from "terrace:host/tickets";

        const input = readTextFile("/workspace/input.txt");
        writeTextFile("/workspace/output.txt", `${input} + runtime`);

        export default echo({
          text: readTextFile("/workspace/output.txt"),
        });
        "#,
        &[("/workspace/input.txt", "hello sandbox")],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute module");

    assert_eq!(
        session
            .filesystem()
            .read_file("/workspace/output.txt")
            .await
            .expect("read output file"),
        Some(b"hello sandbox + runtime".to_vec())
    );
    assert_eq!(
        result.result,
        Some(json!({
            "specifier": "terrace:host/tickets",
            "method": "echo",
            "args": [{
                "text": "hello sandbox + runtime"
            }]
        }))
    );
    assert!(
        result
            .module_graph
            .contains(&"terrace:/workspace/main.js".to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"@terracedb/sandbox/fs".to_string())
    );
    assert!(
        result
            .module_graph
            .contains(&"terrace:host/tickets".to_string())
    );
    assert_eq!(result.capability_calls.len(), 1);

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tool runs")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.runtime.exec_module".to_string()));
    assert!(tool_names.contains(&"host_api.tickets.echo".to_string()));

    let runtime_cache = session
        .filesystem()
        .read_file(TERRACE_RUNTIME_MODULE_CACHE_PATH)
        .await
        .expect("read runtime cache file")
        .expect("runtime cache should exist after execution");
    let cached: serde_json::Value =
        serde_json::from_slice(&runtime_cache).expect("decode runtime cache manifest");
    assert!(
        cached
            .as_array()
            .expect("cache entries array")
            .iter()
            .any(|entry| entry["specifier"] == "terrace:/workspace/main.js")
    );
}

#[tokio::test]
async fn denied_capabilities_fail_predictably_and_no_runtime_helpers_are_ambient() {
    let registry = deterministic_capabilities();
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(110, 52, services);
    let base_volume_id = VolumeId::new(0x9010);
    let session_volume_id = VolumeId::new(0x9011);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { echo } from "terrace:host/admin";
        export default echo({ denied: true });
        "#,
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let loader = session.module_loader().await;
    let denied = loader
        .load("terrace:host/admin", None)
        .await
        .expect_err("admin capability should be denied");
    assert!(matches!(
        denied,
        SandboxError::CapabilityDenied { ref specifier } if specifier == "terrace:host/admin"
    ));

    let runtime_error = session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("denied capability module should fail");
    assert!(matches!(
        runtime_error,
        SandboxError::Execution { ref message, .. }
            if message.contains("sandbox capability is not allowed in this session")
    ));

    let globals = session
        .eval(
            r#"
            export default {
              fs: typeof readTextFile,
              host: typeof __terrace_capability_call,
            };
            "#,
        )
        .await
        .expect("evaluate globals check");
    assert_eq!(
        globals.result,
        Some(json!({
            "fs": "undefined",
            "host": "undefined"
        }))
    );
}

fn resolved_policy_for_manifest(manifest: PolicyCapabilityManifest) -> ResolvedSessionPolicy {
    ResolvedSessionPolicy {
        subject: PolicySubject {
            subject_id: "user:alice".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            groups: vec!["support".to_string()],
            attributes: BTreeMap::new(),
        },
        session_mode: SessionMode::Draft,
        manifest,
        execution_policy: ExecutionPolicy {
            default_assignment: ExecutionDomainAssignment {
                domain: ExecutionDomain::OwnerForeground,
                budget: None,
                placement_tags: vec!["local".to_string()],
                metadata: BTreeMap::new(),
            },
            operations: BTreeMap::new(),
            metadata: BTreeMap::new(),
        },
        rate_limits: vec![],
        budget_hooks: BTreeMap::new(),
    }
}

fn db_table_policy_manifest(resource_policy: ResourcePolicy) -> PolicyCapabilityManifest {
    PolicyCapabilityManifest {
        subject: None,
        preset_name: Some("draft-support".to_string()),
        profile_name: None,
        bindings: vec![ManifestBinding {
            binding_name: "tickets".to_string(),
            capability_family: "db.table.v1".to_string(),
            module_specifier: capability_module_specifier("tickets"),
            shell_command: Some(ShellCommandDescriptor::for_binding("tickets")),
            resource_policy,
            budget_policy: Default::default(),
            source_template_id: "db.table.v1".to_string(),
            source_grant_id: Some("grant-1".to_string()),
            allow_interactive_widening: true,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn manifest_bound_registry_generates_modules_and_records_policy_metadata() {
    let policy_manifest = db_table_policy_manifest(ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Table,
            pattern: "tickets".to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(RowScopeBinding {
            binding_id: "tickets.owner".to_string(),
            policy: RowScopePolicy::TypedRowPredicate {
                predicate_id: "ticket_owner".to_string(),
                row_type: Some("ticket".to_string()),
                referenced_fields: vec!["owner_id".to_string()],
            },
            allowed_query_shapes: vec![terracedb_capabilities::RowQueryShape::PointRead],
            write_semantics: Default::default(),
            denial_contract: Default::default(),
            metadata: BTreeMap::new(),
        }),
        visibility_index: None,
        metadata: BTreeMap::new(),
    });
    let dispatcher =
        DeterministicPolicyDbDispatcher::new(resolved_policy_for_manifest(policy_manifest.clone()))
            .with_predicates(
                DeterministicTypedRowPredicateEvaluator::default().with_predicate(
                    "ticket_owner",
                    DeterministicTypedRowPredicate::MatchContext {
                        row_field: "owner_id".to_string(),
                        context_field: "subject_id".to_string(),
                    },
                ),
            );
    dispatcher
        .seed_row("tickets", "ticket:t-1", json!({ "owner_id": "user:alice" }))
        .await;
    let registry =
        ManifestBoundCapabilityRegistry::new(policy_manifest, Arc::new(dispatcher.clone()))
            .expect("build manifest-bound registry");
    assert!(
        registry
            .manifest()
            .capabilities
            .first()
            .and_then(|capability| capability.typescript_declarations.as_ref())
            .is_some_and(|declarations| declarations.contains("declare module"))
    );

    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(120, 53, services);
    let base_volume_id = VolumeId::new(0x9020);
    let session_volume_id = VolumeId::new(0x9021);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { get } from "terrace:host/tickets";
        export default get({ key: "ticket:t-1" });
        "#,
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute generated binding");
    assert_eq!(
        result.result,
        Some(json!({
            "key": "ticket:t-1",
            "row": {
                "owner_id": "user:alice"
            }
        }))
    );
    assert_eq!(result.capability_calls.len(), 1);
    assert_eq!(
        result.capability_calls[0].metadata.get("capability_family"),
        Some(&json!("db.table.v1"))
    );
    assert_eq!(
        result.capability_calls[0]
            .metadata
            .get("policy_outcome")
            .and_then(|value| value.get("outcome")),
        Some(&json!("allowed"))
    );
}

#[tokio::test]
async fn manifest_bound_registry_omits_ungranted_bindings_and_surfaces_granted_denials() {
    let policy_manifest = db_table_policy_manifest(ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Table,
            pattern: "tickets".to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(RowScopeBinding {
            binding_id: "tickets.owner".to_string(),
            policy: RowScopePolicy::TypedRowPredicate {
                predicate_id: "ticket_owner".to_string(),
                row_type: Some("ticket".to_string()),
                referenced_fields: vec!["owner_id".to_string()],
            },
            allowed_query_shapes: vec![terracedb_capabilities::RowQueryShape::WriteMutation],
            write_semantics: Default::default(),
            denial_contract: Default::default(),
            metadata: BTreeMap::new(),
        }),
        visibility_index: None,
        metadata: BTreeMap::new(),
    });
    let dispatcher =
        DeterministicPolicyDbDispatcher::new(resolved_policy_for_manifest(policy_manifest.clone()))
            .with_predicates(
                DeterministicTypedRowPredicateEvaluator::default().with_predicate(
                    "ticket_owner",
                    DeterministicTypedRowPredicate::MatchContext {
                        row_field: "owner_id".to_string(),
                        context_field: "subject_id".to_string(),
                    },
                ),
            );
    dispatcher
        .seed_row("tickets", "ticket:t-1", json!({ "owner_id": "user:alice" }))
        .await;
    let registry =
        ManifestBoundCapabilityRegistry::new(policy_manifest, Arc::new(dispatcher.clone()))
            .expect("build manifest-bound registry");
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(130, 54, services);
    let base_volume_id = VolumeId::new(0x9030);
    let absent_base_volume_id = VolumeId::new(0x9033);
    let denied_session_volume_id = VolumeId::new(0x9031);
    let absent_session_volume_id = VolumeId::new(0x9032);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { put } from "terrace:host/tickets";
        export default put({
          key: "ticket:t-1",
          row: { owner_id: "user:bob" },
          occReadSet: ["ticket:t-1"],
        });
        "#,
        &[],
    )
    .await;
    let denied_session = sandbox
        .open_session(SandboxConfig {
            session_volume_id: denied_session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open denied session");
    let denied = denied_session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("scope-escaping write should be denied");
    assert!(matches!(
        denied,
        SandboxError::Execution { ref message, .. }
            if message.contains("policy denied")
                || message.contains("policy Denied")
                || message.contains("escape")
    ));

    seed_base(
        &vfs,
        absent_base_volume_id,
        r#"
        import { get } from "terrace:host/admin";
        export default get({ key: "t-1" });
        "#,
        &[],
    )
    .await;
    let absent_session = sandbox
        .open_session(SandboxConfig {
            session_volume_id: absent_session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id: absent_base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open absent-binding session");
    let loader = absent_session.module_loader().await;
    let absent = loader
        .load("terrace:host/admin", None)
        .await
        .expect_err("ungranted binding should stay absent");
    assert!(matches!(
        absent,
        SandboxError::CapabilityDenied { ref specifier } if specifier == "terrace:host/admin"
    ));
}

#[tokio::test]
async fn manifest_bound_shell_bridge_matches_typed_import_metadata_and_audit_names() {
    let policy_manifest = db_table_policy_manifest(ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Table,
            pattern: "tickets".to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(RowScopeBinding {
            binding_id: "tickets.owner".to_string(),
            policy: RowScopePolicy::TypedRowPredicate {
                predicate_id: "ticket_owner".to_string(),
                row_type: Some("ticket".to_string()),
                referenced_fields: vec!["owner_id".to_string()],
            },
            allowed_query_shapes: vec![terracedb_capabilities::RowQueryShape::PointRead],
            write_semantics: Default::default(),
            denial_contract: Default::default(),
            metadata: BTreeMap::new(),
        }),
        visibility_index: None,
        metadata: BTreeMap::new(),
    });
    let dispatcher =
        DeterministicPolicyDbDispatcher::new(resolved_policy_for_manifest(policy_manifest.clone()))
            .with_predicates(
                DeterministicTypedRowPredicateEvaluator::default().with_predicate(
                    "ticket_owner",
                    DeterministicTypedRowPredicate::MatchContext {
                        row_field: "owner_id".to_string(),
                        context_field: "subject_id".to_string(),
                    },
                ),
            );
    dispatcher
        .seed_row("tickets", "ticket:t-1", json!({ "owner_id": "user:alice" }))
        .await;
    let registry =
        ManifestBoundCapabilityRegistry::new(policy_manifest, Arc::new(dispatcher.clone()))
            .expect("build manifest-bound registry");
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(131, 541, services);
    let base_volume_id = VolumeId::new(0x9034);
    let session_volume_id = VolumeId::new(0x9035);

    seed_base(
        &vfs,
        base_volume_id,
        r#"
        import { get } from "terrace:host/tickets";
        export default get({ key: "ticket:t-1" });
        "#,
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open shell-bridge session");

    let typed = session
        .exec_module("/workspace/main.js")
        .await
        .expect("execute typed import");
    let shell = session
        .run_bash(BashRequest {
            command: r#"terrace-call tickets get '{"key":"ticket:t-1"}'"#.to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("execute shell bridge");
    let shell_json: serde_json::Value =
        serde_json::from_str(shell.stdout.trim()).expect("decode shell bridge JSON");

    assert_eq!(shell.exit_code, 0);
    assert_eq!(shell_json["ok"], json!(true));
    assert_eq!(shell_json["binding"], json!("tickets"));
    assert_eq!(shell_json["specifier"], json!("terrace:host/tickets"));
    assert_eq!(shell_json["method"], json!("get"));
    assert_eq!(shell_json["value"], typed.result.expect("typed result"));
    assert_eq!(
        shell_json["metadata"],
        serde_json::to_value(&typed.capability_calls[0].metadata).expect("encode typed metadata")
    );

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tool runs")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert_eq!(
        tool_names
            .iter()
            .filter(|name| name.as_str() == "host_api.tickets.get")
            .count(),
        2
    );
}

#[tokio::test]
async fn manifest_bound_shell_bridge_exposes_help_and_json_io() {
    let policy_manifest = db_table_policy_manifest(ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Table,
            pattern: "tickets".to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(RowScopeBinding {
            binding_id: "tickets.owner".to_string(),
            policy: RowScopePolicy::TypedRowPredicate {
                predicate_id: "ticket_owner".to_string(),
                row_type: Some("ticket".to_string()),
                referenced_fields: vec!["owner_id".to_string()],
            },
            allowed_query_shapes: vec![terracedb_capabilities::RowQueryShape::PointRead],
            write_semantics: Default::default(),
            denial_contract: Default::default(),
            metadata: BTreeMap::new(),
        }),
        visibility_index: None,
        metadata: BTreeMap::new(),
    });
    let dispatcher =
        DeterministicPolicyDbDispatcher::new(resolved_policy_for_manifest(policy_manifest.clone()))
            .with_predicates(
                DeterministicTypedRowPredicateEvaluator::default().with_predicate(
                    "ticket_owner",
                    DeterministicTypedRowPredicate::MatchContext {
                        row_field: "owner_id".to_string(),
                        context_field: "subject_id".to_string(),
                    },
                ),
            );
    dispatcher
        .seed_row("tickets", "ticket:t-1", json!({ "owner_id": "user:alice" }))
        .await;
    let registry =
        ManifestBoundCapabilityRegistry::new(policy_manifest, Arc::new(dispatcher.clone()))
            .expect("build manifest-bound registry");
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(132, 542, services);
    let base_volume_id = VolumeId::new(0x9036);
    let session_volume_id = VolumeId::new(0x9037);

    seed_base(&vfs, base_volume_id, "export default null;", &[]).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open shell help session");
    session
        .filesystem()
        .write_file(
            "/workspace/request.json",
            br#"{"key":"ticket:t-1"}"#.to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed JSON input file");

    let help = session
        .run_bash(BashRequest {
            command: "terrace-call --help".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("root help");
    assert_eq!(help.exit_code, 0);
    assert!(help.stdout.contains("Available bindings"));
    assert!(help.stdout.contains("terrace-call tickets"));

    let method_help = session
        .run_bash(BashRequest {
            command: "terrace-call tickets get --help".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("method help");
    assert_eq!(method_help.exit_code, 0);
    assert!(
        method_help
            .stdout
            .contains("Pass a JSON object with the row key to read.")
    );
    assert!(method_help.stdout.contains(r#"{"key":"ticket:t-1"}"#));

    let shell = session
        .run_bash(BashRequest {
            command: "terrace-call tickets get --input-file request.json --compact".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("input file invocation");
    assert_eq!(shell.exit_code, 0);
    assert!(!shell.stdout.trim().contains('\n'));
    let shell_json: serde_json::Value =
        serde_json::from_str(shell.stdout.trim()).expect("decode compact JSON");
    assert_eq!(shell_json["value"]["key"], json!("ticket:t-1"));

    let invalid = session
        .run_bash(BashRequest {
            command: r#"terrace-call tickets get '{bad-json}'"#.to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("invalid JSON should still return a bash report");
    let invalid_json: serde_json::Value =
        serde_json::from_str(invalid.stderr.trim()).expect("decode invalid JSON error");
    assert_eq!(invalid.exit_code, 65);
    assert_eq!(invalid_json["error"]["kind"], json!("invalid_json_input"));
}

#[tokio::test]
async fn manifest_bound_shell_bridge_omits_ungranted_bindings_and_reports_host_denials_as_json() {
    let policy_manifest = db_table_policy_manifest(ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Table,
            pattern: "tickets".to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(RowScopeBinding {
            binding_id: "tickets.owner".to_string(),
            policy: RowScopePolicy::TypedRowPredicate {
                predicate_id: "ticket_owner".to_string(),
                row_type: Some("ticket".to_string()),
                referenced_fields: vec!["owner_id".to_string()],
            },
            allowed_query_shapes: vec![terracedb_capabilities::RowQueryShape::WriteMutation],
            write_semantics: Default::default(),
            denial_contract: Default::default(),
            metadata: BTreeMap::new(),
        }),
        visibility_index: None,
        metadata: BTreeMap::new(),
    });
    let dispatcher =
        DeterministicPolicyDbDispatcher::new(resolved_policy_for_manifest(policy_manifest.clone()))
            .with_predicates(
                DeterministicTypedRowPredicateEvaluator::default().with_predicate(
                    "ticket_owner",
                    DeterministicTypedRowPredicate::MatchContext {
                        row_field: "owner_id".to_string(),
                        context_field: "subject_id".to_string(),
                    },
                ),
            );
    dispatcher
        .seed_row("tickets", "ticket:t-1", json!({ "owner_id": "user:alice" }))
        .await;
    let registry =
        ManifestBoundCapabilityRegistry::new(policy_manifest, Arc::new(dispatcher.clone()))
            .expect("build manifest-bound registry");
    let services = SandboxServices::deterministic().with_capabilities(Arc::new(registry.clone()));
    let (vfs, sandbox) = sandbox_store(133, 543, services);
    let base_volume_id = VolumeId::new(0x9038);
    let session_volume_id = VolumeId::new(0x9039);

    seed_base(&vfs, base_volume_id, "export default null;", &[]).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: registry.manifest(),
            execution_policy: None,
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open denial session");

    let help = session
        .run_bash(BashRequest {
            command: "terrace-call --help".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("shell bridge help");
    assert!(help.stdout.contains("terrace-call tickets"));
    assert!(!help.stdout.contains("admin"));

    let absent = session
        .run_bash(BashRequest {
            command: r#"terrace-call admin get '{"key":"ticket:t-1"}'"#.to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("absent binding should still return a bash report");
    let absent_json: serde_json::Value =
        serde_json::from_str(absent.stderr.trim()).expect("decode absent binding error");
    assert_eq!(absent.exit_code, 69);
    assert_eq!(absent_json["error"]["kind"], json!("binding_not_available"));
    assert_eq!(
        absent_json["metadata"]["available_bindings"],
        json!(["terrace-call tickets"])
    );

    let denied = session
        .run_bash(BashRequest {
            command: r#"terrace-call tickets put '{"key":"ticket:t-1","row":{"owner_id":"user:bob"},"occReadSet":["ticket:t-1"]}'"#.to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("host denial should still return a bash report");
    let denied_json: serde_json::Value =
        serde_json::from_str(denied.stderr.trim()).expect("decode host denial JSON");
    assert_eq!(denied.exit_code, 70);
    assert_eq!(
        denied_json["error"]["kind"],
        json!("host_enforcement_denied")
    );
    assert_eq!(denied_json["metadata"]["binding_name"], json!("tickets"));
    assert_eq!(
        denied_json["metadata"]["capability_family"],
        json!("db.table.v1")
    );
    assert_eq!(denied_json["metadata"]["policy_outcome"], json!("denied"));
}

#[tokio::test]
async fn execution_policy_routes_runtime_and_rejects_overloaded_typecheck_domains() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(120, 53, services);
    let base_volume_id = VolumeId::new(0x9020);
    let session_volume_id = VolumeId::new(0x9021);

    seed_base(&vfs, base_volume_id, "export default { ok: true };", &[]).await;

    let policy = routed_execution_policy();
    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(policy.clone()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open policy-routed session");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("runtime should be admitted in dedicated domain");
    let info = session.info().await;

    assert_eq!(session.runtime_handle().backend, "routed-runtime-dedicated");
    assert_eq!(info.services.runtime_backend, "routed-runtime-dedicated");
    assert_eq!(
        info.services.execution_bindings[&ExecutionOperation::DraftSession]
            .domain_path
            .as_string(),
        "process/sandbox-tests/dedicated"
    );
    assert_eq!(
        info.services.execution_bindings[&ExecutionOperation::TypeCheck].backend,
        "routed-typescript-owner-foreground"
    );
    assert_eq!(
        result.metadata["execution_domain_path"],
        json!("process/sandbox-tests/dedicated")
    );

    let error = session
        .typecheck(TypeCheckRequest {
            roots: vec!["/workspace/main.js".to_string()],
            ..Default::default()
        })
        .await
        .expect_err("typecheck domain should be overloaded");
    assert!(matches!(
        error,
        SandboxError::ExecutionDomainOverloaded { ref operation, ref path, .. }
            if *operation == ExecutionOperation::TypeCheck
                && path.as_string() == "process/sandbox-tests/owner-foreground"
    ));

    let bash = session
        .run_bash(BashRequest {
            command: "pwd".to_string(),
            cwd: "/workspace".to_string(),
            env: BTreeMap::new(),
        })
        .await
        .expect("bash should be admitted in dedicated domain");
    assert_eq!(bash.cwd, "/workspace");

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tool runs")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.typescript.check".to_string()));
    assert!(tool_names.contains(&"sandbox.bash.exec".to_string()));
    assert_eq!(
        tool_names
            .iter()
            .filter(|name| name.as_str() == "sandbox.typescript.check")
            .count(),
        1
    );
    assert_eq!(
        tool_names
            .iter()
            .filter(|name| name.as_str() == "sandbox.bash.exec")
            .count(),
        1
    );
}

#[tokio::test]
async fn execution_policy_inherits_default_budget_for_operation_overrides() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(125, 530, services);
    let base_volume_id = VolumeId::new(0x9028);
    let session_volume_id = VolumeId::new(0x9029);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { inherited: true };",
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(default_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open policy-routed session with inherited budget");

    let result = session
        .exec_module("/workspace/main.js")
        .await
        .expect("first runtime call should inherit default budget");
    let info = session.info().await;

    assert_eq!(result.metadata["execution_budget"]["max_calls"], json!(1));
    assert_eq!(
        info.services.execution_bindings[&ExecutionOperation::DraftSession]
            .budget
            .as_ref()
            .and_then(|budget| budget.max_calls),
        Some(1)
    );

    let error = session
        .exec_module("/workspace/main.js")
        .await
        .expect_err("second runtime call should exceed inherited max_calls budget");
    assert!(matches!(
        error,
        SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 1")
    ));
}

#[tokio::test]
async fn execution_policy_max_calls_survive_session_reopen() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(130, 54, services);
    let base_volume_id = VolumeId::new(0x9030);
    let session_volume_id = VolumeId::new(0x9031);

    seed_base(&vfs, base_volume_id, "export default { once: true };", &[]).await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(reopen_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open budgeted session");
    session
        .exec_module("/workspace/main.js")
        .await
        .expect("first runtime call within budget");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen budgeted session");
    let error = reopened
        .exec_module("/workspace/main.js")
        .await
        .expect_err("reopen should not reset execution max_calls");
    assert!(matches!(
        error,
        SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 1")
    ));
}

#[tokio::test]
async fn execution_policy_reopen_reconciles_stale_sidecar_counts() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(135, 55, services);
    let base_volume_id = VolumeId::new(0x9038);
    let session_volume_id = VolumeId::new(0x9039);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { crash_safe: true };",
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(reopen_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open budgeted session");

    session
        .filesystem()
        .write_file(
            "/.terrace/execution-policy-state.json",
            serde_json::to_vec_pretty(&json!({
                "format_version": 1,
                "counts": {
                    "draft_session": 1
                }
            }))
            .expect("encode stale execution count state"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write stale execution count state");
    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close session with stale sidecar");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session with stale sidecar");
    reopened
        .exec_module("/workspace/main.js")
        .await
        .expect("stale sidecar should not exhaust max_calls without matching tool run");

    let error = reopened
        .exec_module("/workspace/main.js")
        .await
        .expect_err("second call should still exhaust the real max_calls budget");
    assert!(matches!(
        error,
        SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 1")
    ));
}

#[tokio::test]
async fn execution_policy_reopen_rejects_unsupported_sidecar_versions() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(136, 57, services);
    let base_volume_id = VolumeId::new(0x903a);
    let session_volume_id = VolumeId::new(0x903b);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { invalid: true };",
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(reopen_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session for unsupported execution-policy state version");

    session
        .filesystem()
        .write_file(
            "/.terrace/execution-policy-state.json",
            serde_json::to_vec_pretty(&json!({
                "format_version": 99,
                "counts": {
                    "draft_session": 1
                }
            }))
            .expect("encode unsupported execution count state"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("write unsupported execution count state");
    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close session with unsupported sidecar version");

    let error = match sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
    {
        Ok(_) => panic!("reopen should reject unsupported execution-policy state versions"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        SandboxError::InvalidSessionMetadata { ref path, ref reason }
            if path == "/.terrace/execution-policy-state.json"
                && reason.contains("unsupported execution policy state format version 99")
    ));
}

#[tokio::test]
async fn execution_policy_concurrent_installs_do_not_regress_reopen_counts() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(140, 56, services);
    let base_volume_id = VolumeId::new(0x9040);
    let session_volume_id = VolumeId::new(0x9041);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { packages: true };",
        &[],
    )
    .await;

    let session = Arc::new(
        sandbox
            .open_session(SandboxConfig {
                session_volume_id,
                session_chunk_size: Some(4096),
                base_volume_id,
                durable_base: false,
                workspace_root: "/workspace".to_string(),
                package_compat: PackageCompatibilityMode::NpmPureJs,
                conflict_policy: ConflictPolicy::Fail,
                capabilities: Default::default(),
                execution_policy: Some(concurrent_package_budget_execution_policy()),
                hoisted_source: None,
                git_provenance: None,
            })
            .await
            .expect("open policy-routed package session"),
    );

    let first_session = session.clone();
    let second_session = session.clone();
    let (first, second) = tokio::join!(
        async move {
            first_session
                .install_packages(terracedb_sandbox::PackageInstallRequest {
                    packages: vec!["zod".to_string()],
                    materialize_compatibility_view: false,
                })
                .await
        },
        async move {
            second_session
                .install_packages(terracedb_sandbox::PackageInstallRequest {
                    packages: vec!["lodash".to_string()],
                    materialize_compatibility_view: false,
                })
                .await
        }
    );
    first.expect("first routed package install");
    second.expect("second routed package install");

    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close session after concurrent package installs");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session after concurrent package installs");
    let error = reopened
        .install_packages(terracedb_sandbox::PackageInstallRequest {
            packages: vec!["dayjs".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect_err("reopen should retain both completed package install calls");
    assert!(matches!(
        error,
        SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 2")
    ));
}

#[tokio::test]
async fn execution_policy_reopen_recovers_missing_sidecar_operations_from_tool_history() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(145, 58, services);
    let base_volume_id = VolumeId::new(0x9048);
    let session_volume_id = VolumeId::new(0x9049);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { package_history: true };",
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::NpmPureJs,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(single_package_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open policy-routed package session");

    session
        .install_packages(terracedb_sandbox::PackageInstallRequest {
            packages: vec!["zod".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect("record package install before sidecar rewrite");
    session
        .filesystem()
        .write_file(
            "/.terrace/execution-policy-state.json",
            serde_json::to_vec_pretty(&json!({
                "format_version": 1,
                "counts": {
                    "draft_session": 1
                }
            }))
            .expect("encode sidecar missing package install counts"),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("overwrite sidecar without package install counts");
    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close session after sidecar rewrite");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen session with sidecar missing package install counts");
    let error = reopened
        .install_packages(terracedb_sandbox::PackageInstallRequest {
            packages: vec!["dayjs".to_string()],
            materialize_compatibility_view: false,
        })
        .await
        .expect_err("tool history should restore omitted package install counts");
    assert!(matches!(
        error,
        SandboxError::Service { service, ref message }
            if service == "execution-policy"
                && message.contains("exceeded max_calls 1")
    ));
}

#[tokio::test]
async fn execution_policy_failed_admission_attempts_do_not_consume_budget() {
    let services =
        SandboxServices::deterministic().with_execution_router(routed_execution_router());
    let (vfs, sandbox) = sandbox_store(146, 59, services);
    let base_volume_id = VolumeId::new(0x904a);
    let session_volume_id = VolumeId::new(0x904b);

    seed_base(
        &vfs,
        base_volume_id,
        "export default { overloaded: true };",
        &[],
    )
    .await;

    let session = sandbox
        .open_session(SandboxConfig {
            session_volume_id,
            session_chunk_size: Some(4096),
            base_volume_id,
            durable_base: false,
            workspace_root: "/workspace".to_string(),
            package_compat: PackageCompatibilityMode::TerraceOnly,
            conflict_policy: ConflictPolicy::Fail,
            capabilities: Default::default(),
            execution_policy: Some(overloaded_typecheck_budget_execution_policy()),
            hoisted_source: None,
            git_provenance: None,
        })
        .await
        .expect("open session with overloaded typecheck budget");

    for _ in 0..2 {
        let error = session
            .typecheck(TypeCheckRequest {
                roots: vec!["/workspace/main.js".to_string()],
                ..Default::default()
            })
            .await
            .expect_err("overloaded typecheck should fail without consuming budget");
        assert!(matches!(
            error,
            SandboxError::ExecutionDomainOverloaded { ref operation, .. }
                if *operation == ExecutionOperation::TypeCheck
        ));
    }

    session
        .close(terracedb_sandbox::CloseSessionOptions::default())
        .await
        .expect("close overloaded typecheck session");

    let reopened = sandbox
        .reopen_session(ReopenSessionOptions {
            session_volume_id,
            session_chunk_size: Some(4096),
        })
        .await
        .expect("reopen overloaded typecheck session");
    let error = reopened
        .typecheck(TypeCheckRequest {
            roots: vec!["/workspace/main.js".to_string()],
            ..Default::default()
        })
        .await
        .expect_err("reopen should keep overloaded typecheck failures budget-free");
    assert!(matches!(
        error,
        SandboxError::ExecutionDomainOverloaded { ref operation, .. }
            if *operation == ExecutionOperation::TypeCheck
    ));
}

fn routed_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([
            (
                ExecutionOperation::DraftSession,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: None,
                    placement_tags: vec!["draft".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::PackageInstall,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: None,
                    placement_tags: vec!["packages".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::BashHelper,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: None,
                    placement_tags: vec!["bash".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::TypeCheck,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::OwnerForeground,
                    budget: None,
                    placement_tags: vec!["foreground".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
        ]),
        metadata: BTreeMap::new(),
    }
}

fn default_budget_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: Some(BudgetPolicy {
                max_calls: Some(1),
                max_scanned_rows: None,
                max_returned_rows: None,
                max_bytes: None,
                max_millis: None,
                rate_limit_bucket: None,
                labels: BTreeMap::new(),
            }),
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::DraftSession,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::DedicatedSandbox,
                budget: None,
                placement_tags: vec!["draft".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn reopen_budget_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::DraftSession,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::DedicatedSandbox,
                budget: Some(BudgetPolicy {
                    max_calls: Some(1),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                }),
                placement_tags: vec!["draft".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn concurrent_package_budget_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::PackageInstall,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::DedicatedSandbox,
                budget: Some(BudgetPolicy {
                    max_calls: Some(2),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                }),
                placement_tags: vec!["packages".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn single_package_budget_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::PackageInstall,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::DedicatedSandbox,
                budget: Some(BudgetPolicy {
                    max_calls: Some(1),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                }),
                placement_tags: vec!["packages".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn overloaded_typecheck_budget_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::DedicatedSandbox,
            budget: None,
            placement_tags: vec!["dedicated".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::TypeCheck,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::OwnerForeground,
                budget: Some(BudgetPolicy {
                    max_calls: Some(1),
                    max_scanned_rows: None,
                    max_returned_rows: None,
                    max_bytes: None,
                    max_millis: None,
                    rate_limit_bucket: None,
                    labels: BTreeMap::new(),
                }),
                placement_tags: vec!["foreground".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn routed_execution_router() -> SandboxExecutionRouter {
    let resource_manager = Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    let dedicated_typescript = Arc::new(DeterministicTypeScriptService::new(
        "routed-typescript-dedicated",
    ));
    let dedicated_bash = Arc::new(
        DeterministicBashService::new("routed-bash-dedicated")
            .with_typescript_service(dedicated_typescript.clone()),
    );

    SandboxExecutionRouter::new(resource_manager)
        .with_domain(
            ExecutionDomain::DedicatedSandbox,
            SandboxExecutionDomainRoute::new(
                SandboxExecutionPlacement::new(ExecutionDomainPath::new([
                    "process",
                    "sandbox-tests",
                    "dedicated",
                ]))
                .with_placement(ExecutionDomainPlacement::Dedicated)
                .with_budget(ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    ..Default::default()
                })
                .with_usage(
                    ExecutionOperation::DraftSession,
                    ExecutionResourceUsage {
                        cpu_workers: 1,
                        ..Default::default()
                    },
                ),
            )
            .with_runtime(Arc::new(DeterministicRuntimeBackend::new(
                "routed-runtime-dedicated",
            )))
            .with_packages(Arc::new(DeterministicPackageInstaller::new(
                "routed-packages-dedicated",
            )))
            .with_bash(dedicated_bash)
            .with_typescript(dedicated_typescript),
        )
        .with_domain(
            ExecutionDomain::OwnerForeground,
            SandboxExecutionDomainRoute::new(
                SandboxExecutionPlacement::new(ExecutionDomainPath::new([
                    "process",
                    "sandbox-tests",
                    "owner-foreground",
                ]))
                .with_placement(ExecutionDomainPlacement::SharedWeighted { weight: 3 })
                .with_budget(ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(0),
                        weight: Some(3),
                    },
                    ..Default::default()
                })
                .with_usage(
                    ExecutionOperation::TypeCheck,
                    ExecutionResourceUsage {
                        cpu_workers: 1,
                        ..Default::default()
                    },
                ),
            )
            .with_typescript(Arc::new(DeterministicTypeScriptService::new(
                "routed-typescript-owner-foreground",
            ))),
        )
}
