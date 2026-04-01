use std::{collections::BTreeMap, time::Duration};

use serde_json::json;
use terracedb_capabilities::{
    BudgetPolicy, CapabilityGrant, CapabilityPresetDescriptor, CapabilityProfileDescriptor,
    CapabilityTemplate, DeterministicPolicyEngine, DeterministicRateLimiter,
    DeterministicSubjectResolver, ExecutionDomain, ExecutionDomainAssignment, ExecutionOperation,
    ExecutionPolicy, ManifestBindingOverride, PolicySubject, PresetBinding, ResourceKind,
    ResourcePolicy, ResourceSelector, RowDenialContract, RowQueryShape, RowScopeBinding,
    RowScopePolicy, SessionMode, SessionPresetRequest, StaticExecutionPolicyResolver,
    SubjectResolutionRequest, SubjectSelector, VisibilityIndexSpec, VisibilityIndexSubjectKey,
};
use terracedb_simulation::SeededSimulationRunner;

#[derive(Clone, Debug, PartialEq, Eq)]
struct SimulationCapture {
    manifest_json: String,
    audit_json: String,
    session_mode: SessionMode,
    profile_name: Option<String>,
    execution_domain: ExecutionDomain,
}

fn simulation_budget() -> BudgetPolicy {
    BudgetPolicy {
        max_calls: Some(8),
        max_scanned_rows: Some(100),
        max_returned_rows: Some(25),
        max_bytes: Some(4096),
        max_millis: Some(250),
        rate_limit_bucket: Some("draft-user".to_string()),
        labels: BTreeMap::from([("tier".to_string(), "draft".to_string())]),
    }
}

fn simulation_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::OwnerForeground,
            budget: Some(simulation_budget()),
            placement_tags: vec!["local".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([(
            ExecutionOperation::DraftSession,
            ExecutionDomainAssignment {
                domain: ExecutionDomain::DedicatedSandbox,
                budget: Some(simulation_budget()),
                placement_tags: vec!["draft".to_string()],
                metadata: BTreeMap::new(),
            },
        )]),
        metadata: BTreeMap::new(),
    }
}

fn simulation_template() -> CapabilityTemplate {
    CapabilityTemplate {
        template_id: "db.query.v1".to_string(),
        capability_family: "db.query.v1".to_string(),
        default_binding: "tickets".to_string(),
        description: Some("Read tickets".to_string()),
        default_resource_policy: ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets".to_string(),
            }],
            deny: vec![],
            tenant_scopes: vec!["tenant-a".to_string()],
            row_scope_binding: Some(simulation_row_scope_binding()),
            visibility_index: Some(simulation_visibility_index()),
            metadata: BTreeMap::new(),
        },
        default_budget_policy: simulation_budget(),
        expose_in_just_bash: true,
        metadata: BTreeMap::from([("surface".to_string(), json!("sandbox"))]),
    }
}

fn simulation_visibility_index() -> VisibilityIndexSpec {
    VisibilityIndexSpec {
        index_name: "visible_by_subject".to_string(),
        index_table: "visible_by_subject".to_string(),
        subject_key: VisibilityIndexSubjectKey::Subject,
        row_id_field: "ticket_id".to_string(),
        membership_source: Some("ticket_shares".to_string()),
        read_mirror_table: None,
        authoritative_sources: vec!["ticket_shares".to_string(), "tickets".to_string()],
        metadata: BTreeMap::new(),
    }
}

fn simulation_row_scope_binding() -> RowScopeBinding {
    RowScopeBinding {
        binding_id: "tickets.visible".to_string(),
        policy: RowScopePolicy::VisibilityIndex {
            index_name: "visible_by_subject".to_string(),
        },
        allowed_query_shapes: vec![RowQueryShape::PointRead, RowQueryShape::BoundedPrefixScan],
        write_semantics: Default::default(),
        denial_contract: RowDenialContract::default(),
        metadata: BTreeMap::new(),
    }
}

fn simulation_grant() -> CapabilityGrant {
    CapabilityGrant {
        grant_id: "grant-1".to_string(),
        subject: SubjectSelector::Exact {
            subject_id: "user:alice".to_string(),
        },
        template_id: "db.query.v1".to_string(),
        binding_name: Some("tickets".to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: false,
        metadata: BTreeMap::new(),
    }
}

fn simulation_subject(session_id: &str) -> DeterministicSubjectResolver {
    DeterministicSubjectResolver::default().with_session_subject(
        session_id,
        PolicySubject {
            subject_id: "user:alice".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            groups: vec!["support".to_string()],
            attributes: BTreeMap::new(),
        },
    )
}

fn simulation_engine(session_id: &str) -> DeterministicPolicyEngine {
    let execution_policy = simulation_execution_policy();
    DeterministicPolicyEngine::new(
        vec![simulation_template()],
        vec![simulation_grant()],
        simulation_subject(session_id),
        StaticExecutionPolicyResolver::new(execution_policy.clone())
            .with_policy(SessionMode::Draft, execution_policy.clone()),
    )
    .with_preset(CapabilityPresetDescriptor {
        name: "draft-support".to_string(),
        description: Some("Draft support".to_string()),
        bindings: vec![PresetBinding {
            template_id: "db.query.v1".to_string(),
            binding_name: Some("tickets".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        default_session_mode: SessionMode::Draft,
        default_execution_policy: Some(execution_policy.clone()),
        metadata: BTreeMap::new(),
    })
    .with_profile(CapabilityProfileDescriptor {
        name: "foreground".to_string(),
        preset_name: "draft-support".to_string(),
        add_bindings: vec![],
        drop_bindings: vec![],
        execution_policy_override: Some(execution_policy),
        metadata: BTreeMap::new(),
    })
    .with_rate_limiter(DeterministicRateLimiter::default())
}

fn run_seeded_preset_simulation(seed: u64) -> turmoil::Result<SimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(20))
        .run_with(move |_context| async move {
            let session_id = format!("session-{seed:x}");
            let prepared = simulation_engine(&session_id)
                .prepare_session_from_preset(&SessionPresetRequest {
                    subject: SubjectResolutionRequest {
                        session_id: session_id.clone(),
                        auth_subject_hint: Some("alice".to_string()),
                        tenant_hint: Some("tenant-a".to_string()),
                        groups: vec![],
                        attributes: BTreeMap::new(),
                    },
                    preset_name: "draft-support".to_string(),
                    profile_name: Some("foreground".to_string()),
                    session_mode_override: None,
                    binding_overrides: vec![ManifestBindingOverride {
                        binding_name: "tickets".to_string(),
                        drop_binding: false,
                        resource_policy: None,
                        budget_policy: Some(BudgetPolicy {
                            max_calls: Some(3),
                            max_scanned_rows: None,
                            max_returned_rows: None,
                            max_bytes: None,
                            max_millis: Some(120),
                            rate_limit_bucket: Some("draft-user".to_string()),
                            labels: BTreeMap::new(),
                        }),
                        expose_in_just_bash: Some(false),
                        metadata: BTreeMap::new(),
                    }],
                    metadata: BTreeMap::from([("surface".to_string(), json!("simulation"))]),
                })
                .expect("prepare session preset");

            Ok(SimulationCapture {
                manifest_json: serde_json::to_string(&prepared.resolved.manifest)
                    .expect("encode manifest"),
                audit_json: serde_json::to_string(&prepared.audit_metadata).expect("encode audit"),
                session_mode: prepared.resolved.session_mode,
                profile_name: prepared.profile.map(|profile| profile.name),
                execution_domain: prepared
                    .resolved
                    .execution_policy
                    .assignment_for(ExecutionOperation::DraftSession)
                    .domain,
            })
        })
}

#[test]
fn preset_selection_replays_identically_under_seeded_simulation() -> turmoil::Result {
    let first = run_seeded_preset_simulation(0x101b)?;
    let second = run_seeded_preset_simulation(0x101b)?;
    let manifest: serde_json::Value =
        serde_json::from_str(&first.manifest_json).expect("decode manifest");
    let audit: serde_json::Value = serde_json::from_str(&first.audit_json).expect("decode audit");

    assert_eq!(first, second);
    assert_eq!(first.session_mode, SessionMode::Draft);
    assert_eq!(first.profile_name.as_deref(), Some("foreground"));
    assert_eq!(first.execution_domain, ExecutionDomain::DedicatedSandbox);
    assert_eq!(manifest["preset_name"], json!("draft-support"));
    assert_eq!(manifest["profile_name"], json!("foreground"));
    assert_eq!(audit["preset_name"], json!("draft-support"));
    assert_eq!(audit["profile_name"], json!("foreground"));
    assert_eq!(audit["surface"], json!("simulation"));
    assert!(audit.get("expanded_manifest").is_some());
    Ok(())
}
