use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use serde_json::json;
use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_capabilities::{
    CapabilityManifest as PolicyCapabilityManifest, CapabilityUseMetrics, DatabaseAccessRequest,
    DatabaseActionKind, DatabaseTarget, DeterministicTypedRowPredicate,
    DeterministicTypedRowPredicateEvaluator, DeterministicVisibilityIndexStore, ExecutionDomain,
    ExecutionDomainAssignment, ExecutionPolicy, ManifestBinding, PolicyOutcomeKind, PolicySubject,
    ResolvedSessionPolicy, ResourceKind, ResourcePolicy, ResourceSelector, RowScopeBinding,
    RowScopePolicy, SessionMode, capability_module_specifier,
};
use terracedb_sandbox::{
    CapabilityRegistry, ConflictPolicy, DefaultSandboxStore, DeterministicCapabilityModule,
    DeterministicCapabilityRegistry, ManifestBoundCapabilityDispatcher,
    ManifestBoundCapabilityInvocation, ManifestBoundCapabilityRegistry,
    ManifestBoundCapabilityResult, PackageCompatibilityMode, SandboxCapability, SandboxConfig,
    SandboxError, SandboxServices, SandboxStore, TERRACE_RUNTIME_MODULE_CACHE_PATH,
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
            shell_command: None,
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
