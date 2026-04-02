use std::collections::BTreeMap;

use terracedb::Timestamp;
use terracedb_capabilities::{
    AuthorizationScope, BudgetPolicy, CapabilityGrant, CapabilityManifest,
    CapabilityPresetDescriptor, CapabilityProfileDescriptor, CapabilityTemplate,
    CapabilityUseMetrics, CapabilityUseRequest, DatabaseAccessRequest, DatabaseActionKind,
    DatabaseCapabilityFamily, DatabaseResultRow, DatabaseTarget,
    DeterministicDraftAuthorizationSession, DeterministicPolicyEngine, DeterministicRateLimiter,
    DeterministicSubjectResolver, DeterministicTypedRowPredicate,
    DeterministicTypedRowPredicateEvaluator, DeterministicVisibilityIndexStore,
    DraftAuthorizationDecision, DraftAuthorizationFlowError, DraftAuthorizationHistoryKind,
    DraftAuthorizationOutcomeKind, DraftAuthorizationRequestKind, ExecutionDomain,
    ExecutionDomainAssignment, ExecutionOperation, ExecutionPolicy, FilteredScanResumeToken,
    ForegroundSessionStatusProjector, ManifestBinding, ManifestBindingOverride,
    PolicyAuditMetadata, PolicyContext, PolicyDecisionRecord, PolicyError, PolicyOutcomeKind,
    PolicyOutcomeRecord, PolicyResolutionRequest, PolicySubject, PresetBinding, RateLimitOutcome,
    ResolvedSessionPolicy, ResourceKind, ResourcePolicy, ResourceSelector, ResourceTarget,
    RowDenialContract, RowQueryShape, RowScopeBinding, RowScopeFamily, RowScopePolicy,
    RowVisibilityOutcomeKind, SessionLifecycleState, SessionMode, SessionPresetRequest,
    SessionStatusSource, SessionStatusUpdate, ShellCommandDescriptor,
    StaticExecutionPolicyResolver, SubjectResolutionRequest, SubjectSelector, VisibilityIndexSpec,
    VisibilityIndexSubjectKey, VisibilityMembershipTransition, capability_module_specifier,
};
use terracedb_mcp::{
    DeterministicMcpAuthenticator, McpAuthContext, McpAuthenticationRequest, McpAuthenticator,
    McpResourceDescriptor, McpSessionContext, McpSseConnectionState, McpSseEndpoint,
    McpToolDescriptor,
};
use terracedb_migrate::{
    MigrationHistoryEntry, MigrationPlan, MigrationState, MigrationStep, MigrationStepKind,
};
use terracedb_procedures::{
    DeterministicProcedurePublicationStore, FrozenProcedureArtifact, PendingProcedureInvocation,
    PendingProcedurePublication, PreparedProcedureInvocation, ProcedureAuditEventKind,
    ProcedureAuditRecord, ProcedureBindingResolution, ProcedureDeployment, ProcedureDraft,
    ProcedureDraftSource, ProcedureImmutableArtifact, ProcedureInvocationContext,
    ProcedureInvocationReceipt, ProcedureInvocationRecord, ProcedureInvocationRequest,
    ProcedureInvocationState, ProcedurePublicationReceipt, ProcedurePublicationStore,
    ProcedureReview, ProcedureVersionRef, ReviewedProcedureDraft, ReviewedProcedurePublication,
};

fn sample_budget() -> BudgetPolicy {
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

fn sample_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::OwnerForeground,
            budget: Some(sample_budget()),
            placement_tags: vec!["local".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([
            (
                ExecutionOperation::ProcedureInvocation,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: Some(sample_budget()),
                    placement_tags: vec!["reviewed".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::McpRequest,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::RemoteWorker,
                    budget: Some(sample_budget()),
                    placement_tags: vec!["mcp".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
        ]),
        metadata: BTreeMap::from([("owner".to_string(), serde_json::json!("host"))]),
    }
}

fn sample_template() -> CapabilityTemplate {
    CapabilityTemplate {
        template_id: "db.query.v1".to_string(),
        capability_family: "db.query.v1".to_string(),
        default_binding: "tickets".to_string(),
        description: Some("Read tenant-scoped tickets".to_string()),
        default_resource_policy: ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets".to_string(),
            }],
            deny: vec![],
            tenant_scopes: vec!["tenant-a".to_string()],
            row_scope_binding: Some(sample_row_scope_binding()),
            visibility_index: Some(sample_visibility_index()),
            metadata: BTreeMap::new(),
        },
        default_budget_policy: sample_budget(),
        expose_in_just_bash: true,
        metadata: BTreeMap::from([("surface".to_string(), serde_json::json!("sandbox"))]),
    }
}

fn sample_grant() -> CapabilityGrant {
    CapabilityGrant {
        grant_id: "grant-1".to_string(),
        subject: SubjectSelector::Exact {
            subject_id: "user:alice".to_string(),
        },
        template_id: "db.query.v1".to_string(),
        binding_name: Some("tickets".to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: true,
        metadata: BTreeMap::new(),
    }
}

fn sample_subject() -> PolicySubject {
    PolicySubject {
        subject_id: "user:alice".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        groups: vec!["support".to_string()],
        attributes: BTreeMap::from([("role".to_string(), "operator".to_string())]),
    }
}

fn sample_visibility_index() -> VisibilityIndexSpec {
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

fn sample_row_scope_binding() -> RowScopeBinding {
    RowScopeBinding {
        binding_id: "tickets.visible".to_string(),
        policy: RowScopePolicy::VisibilityIndex {
            index_name: "visible_by_subject".to_string(),
        },
        allowed_query_shapes: vec![RowQueryShape::PointRead, RowQueryShape::BoundedPrefixScan],
        write_semantics: Default::default(),
        denial_contract: RowDenialContract::default(),
        metadata: BTreeMap::from([("scope".to_string(), serde_json::json!("shared"))]),
    }
}

fn sample_subject_resolver() -> DeterministicSubjectResolver {
    DeterministicSubjectResolver::default().with_session_subject("session-1", sample_subject())
}

fn sample_resolution_request(profile_name: Option<&str>) -> PolicyResolutionRequest {
    PolicyResolutionRequest {
        subject: SubjectResolutionRequest {
            session_id: "session-1".to_string(),
            auth_subject_hint: Some("alice".to_string()),
            tenant_hint: Some("tenant-a".to_string()),
            groups: vec![],
            attributes: BTreeMap::new(),
        },
        session_mode: SessionMode::Draft,
        preset_name: Some("draft-support".to_string()),
        profile_name: profile_name.map(str::to_string),
    }
}

fn sample_preset_request(profile_name: Option<&str>) -> SessionPresetRequest {
    SessionPresetRequest {
        subject: SubjectResolutionRequest {
            session_id: "session-1".to_string(),
            auth_subject_hint: Some("alice".to_string()),
            tenant_hint: Some("tenant-a".to_string()),
            groups: vec![],
            attributes: BTreeMap::new(),
        },
        preset_name: "draft-support".to_string(),
        profile_name: profile_name.map(str::to_string),
        session_mode_override: None,
        binding_overrides: Vec::new(),
        metadata: BTreeMap::from([("surface".to_string(), serde_json::json!("host-api"))]),
    }
}

fn resolve_sample_policy(rate_limiter: DeterministicRateLimiter) -> ResolvedSessionPolicy {
    sample_policy_engine(rate_limiter)
        .resolve(&sample_resolution_request(Some("foreground")))
        .expect("resolve sample policy")
}

fn sample_policy_engine(rate_limiter: DeterministicRateLimiter) -> DeterministicPolicyEngine {
    let execution_policy = sample_execution_policy();
    DeterministicPolicyEngine::new(
        vec![sample_template()],
        vec![sample_grant()],
        sample_subject_resolver(),
        StaticExecutionPolicyResolver::new(execution_policy.clone())
            .with_policy(SessionMode::Draft, execution_policy.clone()),
    )
    .with_preset(CapabilityPresetDescriptor {
        name: "draft-support".to_string(),
        description: Some("Draft support preset".to_string()),
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
        execution_policy_override: None,
        metadata: BTreeMap::new(),
    })
    .with_rate_limiter(rate_limiter)
}

fn resource_policy_for_tables(tables: &[&str]) -> ResourcePolicy {
    ResourcePolicy {
        allow: tables
            .iter()
            .map(|table| ResourceSelector {
                kind: ResourceKind::Table,
                pattern: (*table).to_string(),
            })
            .collect(),
        deny: vec![],
        tenant_scopes: vec!["tenant-a".to_string()],
        row_scope_binding: Some(sample_row_scope_binding()),
        visibility_index: Some(sample_visibility_index()),
        metadata: BTreeMap::new(),
    }
}

fn resolve_database_policy(
    capability_family: &str,
    binding_name: &str,
    resource_policy: ResourcePolicy,
) -> ResolvedSessionPolicy {
    let execution_policy = sample_execution_policy();
    let template = CapabilityTemplate {
        template_id: capability_family.to_string(),
        capability_family: capability_family.to_string(),
        default_binding: binding_name.to_string(),
        description: Some(format!("database binding {binding_name}")),
        default_resource_policy: resource_policy,
        default_budget_policy: sample_budget(),
        expose_in_just_bash: true,
        metadata: BTreeMap::new(),
    };
    let grant = CapabilityGrant {
        grant_id: format!("grant-{binding_name}"),
        subject: SubjectSelector::Exact {
            subject_id: "user:alice".to_string(),
        },
        template_id: capability_family.to_string(),
        binding_name: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: true,
        metadata: BTreeMap::new(),
    };
    DeterministicPolicyEngine::new(
        vec![template],
        vec![grant],
        sample_subject_resolver(),
        StaticExecutionPolicyResolver::new(execution_policy.clone())
            .with_policy(SessionMode::Draft, execution_policy),
    )
    .resolve(&PolicyResolutionRequest {
        subject: SubjectResolutionRequest {
            session_id: "session-1".to_string(),
            auth_subject_hint: Some("alice".to_string()),
            tenant_hint: Some("tenant-a".to_string()),
            groups: vec![],
            attributes: BTreeMap::new(),
        },
        session_mode: SessionMode::Draft,
        preset_name: None,
        profile_name: None,
    })
    .expect("resolve database policy")
}

fn named_template(
    template_id: &str,
    default_binding: &str,
    description: &str,
    tables: &[&str],
) -> CapabilityTemplate {
    CapabilityTemplate {
        template_id: template_id.to_string(),
        capability_family: template_id.to_string(),
        default_binding: default_binding.to_string(),
        description: Some(description.to_string()),
        default_resource_policy: resource_policy_for_tables(tables),
        default_budget_policy: sample_budget(),
        expose_in_just_bash: true,
        metadata: BTreeMap::from([("surface".to_string(), serde_json::json!("sandbox"))]),
    }
}

fn named_grant(grant_id: &str, template_id: &str, binding_name: &str) -> CapabilityGrant {
    CapabilityGrant {
        grant_id: grant_id.to_string(),
        subject: SubjectSelector::Exact {
            subject_id: "user:alice".to_string(),
        },
        template_id: template_id.to_string(),
        binding_name: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: true,
        metadata: BTreeMap::new(),
    }
}

fn interactive_policy_engine() -> DeterministicPolicyEngine {
    let execution_policy = sample_execution_policy();
    DeterministicPolicyEngine::new(
        vec![
            named_template(
                "db.query.v1",
                "tickets",
                "Read tenant-scoped tickets",
                &["tickets"],
            ),
            named_template(
                "db.admin.v1",
                "admin",
                "Access admin tables",
                &["admin_console"],
            ),
        ],
        vec![
            named_grant("grant-1", "db.query.v1", "tickets"),
            named_grant("grant-2", "db.admin.v1", "admin"),
        ],
        sample_subject_resolver(),
        StaticExecutionPolicyResolver::new(execution_policy.clone())
            .with_policy(SessionMode::Draft, execution_policy.clone())
            .with_policy(SessionMode::ReviewedProcedure, execution_policy),
    )
    .with_preset(CapabilityPresetDescriptor {
        name: "draft-support".to_string(),
        description: Some("Draft support preset".to_string()),
        bindings: vec![PresetBinding {
            template_id: "db.query.v1".to_string(),
            binding_name: Some("tickets".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        default_session_mode: SessionMode::Draft,
        default_execution_policy: Some(sample_execution_policy()),
        metadata: BTreeMap::new(),
    })
    .with_profile(CapabilityProfileDescriptor {
        name: "admin-elevated".to_string(),
        preset_name: "draft-support".to_string(),
        add_bindings: vec![PresetBinding {
            template_id: "db.admin.v1".to_string(),
            binding_name: Some("admin".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        drop_bindings: vec![],
        execution_policy_override: None,
        metadata: BTreeMap::new(),
    })
    .with_profile(CapabilityProfileDescriptor {
        name: "private-tickets".to_string(),
        preset_name: "draft-support".to_string(),
        add_bindings: vec![PresetBinding {
            template_id: "db.query.v1".to_string(),
            binding_name: Some("tickets".to_string()),
            resource_policy: Some(resource_policy_for_tables(&["tickets", "private_tickets"])),
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        drop_bindings: vec!["tickets".to_string()],
        execution_policy_override: None,
        metadata: BTreeMap::new(),
    })
}

fn interactive_policy_engine_with_private_ticket_access() -> DeterministicPolicyEngine {
    let execution_policy = sample_execution_policy();
    DeterministicPolicyEngine::new(
        vec![
            named_template(
                "db.query.v1",
                "tickets",
                "Read tenant-scoped tickets",
                &["tickets"],
            ),
            named_template(
                "db.admin.v1",
                "admin",
                "Access admin tables",
                &["admin_console"],
            ),
        ],
        vec![
            CapabilityGrant {
                grant_id: "grant-1".to_string(),
                subject: SubjectSelector::Exact {
                    subject_id: "user:alice".to_string(),
                },
                template_id: "db.query.v1".to_string(),
                binding_name: Some("tickets".to_string()),
                resource_policy: Some(resource_policy_for_tables(&["tickets", "private_tickets"])),
                budget_policy: None,
                allow_interactive_widening: true,
                metadata: BTreeMap::new(),
            },
            named_grant("grant-2", "db.admin.v1", "admin"),
        ],
        sample_subject_resolver(),
        StaticExecutionPolicyResolver::new(execution_policy.clone())
            .with_policy(SessionMode::Draft, execution_policy.clone())
            .with_policy(SessionMode::ReviewedProcedure, execution_policy),
    )
    .with_preset(CapabilityPresetDescriptor {
        name: "draft-support".to_string(),
        description: Some("Draft support preset".to_string()),
        bindings: vec![PresetBinding {
            template_id: "db.query.v1".to_string(),
            binding_name: Some("tickets".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        default_session_mode: SessionMode::Draft,
        default_execution_policy: Some(sample_execution_policy()),
        metadata: BTreeMap::new(),
    })
    .with_profile(CapabilityProfileDescriptor {
        name: "admin-elevated".to_string(),
        preset_name: "draft-support".to_string(),
        add_bindings: vec![PresetBinding {
            template_id: "db.admin.v1".to_string(),
            binding_name: Some("admin".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        drop_bindings: vec![],
        execution_policy_override: None,
        metadata: BTreeMap::new(),
    })
}

#[test]
fn frozen_contracts_compile_together() {
    let template = sample_template();
    let grant = sample_grant();
    let execution_policy = sample_execution_policy();
    let policy_context = PolicyContext::from(sample_subject());
    let resume_token = FilteredScanResumeToken {
        version: 1,
        binding_name: "tickets".to_string(),
        query_shape: RowQueryShape::BoundedPrefixScan,
        family: RowScopeFamily::VisibilityIndex,
        last_primary_key: Some("ticket:t-1".to_string()),
        last_visibility_key: Some("user:alice/ticket:t-1".to_string()),
        scanned_rows: 3,
        returned_rows: 2,
        metadata: BTreeMap::new(),
    };
    let membership_transition = VisibilityMembershipTransition {
        index_name: "visible_by_subject".to_string(),
        lookup_key: "user:alice".to_string(),
        row_id: "ticket:t-1".to_string(),
        from_visible: false,
        to_visible: true,
        metadata: BTreeMap::new(),
    };
    let row_visibility = sample_row_scope_binding().not_visible_audit(
        RowQueryShape::PointRead,
        Some("row is outside the subject visibility index".to_string()),
    );
    let manifest = CapabilityManifest {
        subject: Some(PolicySubject {
            subject_id: "user:alice".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            groups: vec!["support".to_string()],
            attributes: BTreeMap::from([("role".to_string(), "operator".to_string())]),
        }),
        preset_name: Some("draft-support".to_string()),
        profile_name: Some("foreground".to_string()),
        bindings: vec![ManifestBinding {
            binding_name: "tickets".to_string(),
            capability_family: template.capability_family.clone(),
            module_specifier: capability_module_specifier("tickets"),
            shell_command: Some(ShellCommandDescriptor::for_binding("tickets")),
            resource_policy: template.default_resource_policy.clone(),
            budget_policy: template.default_budget_policy.clone(),
            source_template_id: template.template_id.clone(),
            source_grant_id: Some(grant.grant_id.clone()),
            allow_interactive_widening: true,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    };

    let migration_plan = MigrationPlan {
        plan_id: "plan-1".to_string(),
        application_id: "notes-app".to_string(),
        created_at: Timestamp::new(10),
        requested_manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        steps: vec![MigrationStep {
            step_id: "01".to_string(),
            label: "create tickets".to_string(),
            module_specifier: "terrace:/workspace/migrations/01-create-tickets.ts".to_string(),
            checksum: "sha256:1234".to_string(),
            kind: MigrationStepKind::SchemaChange,
            requested_bindings: vec!["tickets".to_string()],
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    };
    let migration_history = MigrationHistoryEntry {
        plan_id: migration_plan.plan_id.clone(),
        step_id: "01".to_string(),
        state: MigrationState::Applied,
        applied_sequence: None,
        recorded_at: Timestamp::new(11),
        metadata: BTreeMap::new(),
    };

    let publication = ReviewedProcedurePublication {
        publication: ProcedureVersionRef {
            procedure_id: "sync-ticket".to_string(),
            version: 1,
        },
        artifact: ProcedureImmutableArtifact::SandboxSnapshot {
            snapshot_id: "snapshot-1".to_string(),
        },
        code_hash: "sha256:abc123".to_string(),
        entrypoint: "terrace:/workspace/procedures/sync-ticket.ts".to_string(),
        input_schema: serde_json::json!({ "type": "object" }),
        output_schema: serde_json::json!({ "type": "object" }),
        published_at: Timestamp::new(20),
        manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        review: ProcedureReview {
            reviewed_by: "reviewer".to_string(),
            source_revision: "abc123".to_string(),
            note: Some("looks good".to_string()),
            approved_at: Timestamp::new(19),
        },
        metadata: BTreeMap::new(),
    };
    let invocation = ProcedureInvocationRequest {
        publication: publication.publication.clone(),
        arguments: serde_json::json!({ "ticket_id": "t-1" }),
        context: ProcedureInvocationContext {
            caller: manifest.subject.clone(),
            session_mode: SessionMode::ReviewedProcedure,
            session_id: Some("proc-session".to_string()),
            dry_run: false,
            metadata: BTreeMap::new(),
        },
    };

    let mcp_tool = McpToolDescriptor {
        name: "listTickets".to_string(),
        description: Some("List visible tickets".to_string()),
        capability_binding: Some("tickets".to_string()),
        resource_policy: Some(template.default_resource_policy.clone()),
        budget_policy: Some(template.default_budget_policy.clone()),
        input_schema: serde_json::json!({ "type": "object" }),
        output_schema: Some(serde_json::json!({ "type": "array" })),
        metadata: BTreeMap::new(),
    };
    let mcp_resource = McpResourceDescriptor {
        name: "ticket".to_string(),
        uri_template: "ticket://{id}".to_string(),
        description: Some("A single ticket".to_string()),
        mime_type: Some("application/json".to_string()),
        capability_binding: Some("tickets".to_string()),
        metadata: BTreeMap::new(),
    };
    let mcp_session = McpSessionContext {
        session_id: "mcp-session".to_string(),
        stream_id: "stream-1".to_string(),
        endpoint: McpSseEndpoint {
            events_url: "https://mcp.example.invalid/events".to_string(),
            post_url: Some("https://mcp.example.invalid/messages".to_string()),
            last_event_id: Some("evt-7".to_string()),
            retry_millis: Some(1_000),
        },
        auth: McpAuthContext {
            subject: manifest.subject.clone(),
            authentication_kind: Some("bearer".to_string()),
            trusted_draft: true,
            metadata: BTreeMap::new(),
        },
        execution_policy: execution_policy.clone(),
        requested_tools: vec![mcp_tool.name.clone()],
        requested_resources: vec![mcp_resource.name.clone()],
        metadata: BTreeMap::new(),
    };
    let authenticator = DeterministicMcpAuthenticator::default()
        .with_bearer_token("secret-token", mcp_session.auth.clone());

    let mut store = DeterministicProcedurePublicationStore::default();
    store.publish(publication.clone()).expect("publish");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let authenticated = runtime
        .block_on(authenticator.authenticate(&McpAuthenticationRequest {
            session_id: mcp_session.session_id.clone(),
            stream_id: Some(mcp_session.stream_id.clone()),
            headers: BTreeMap::from([(
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            )]),
            peer_addr: None,
            metadata: BTreeMap::new(),
        }))
        .expect("authenticate");

    let _ = (
        template,
        grant,
        manifest,
        migration_plan,
        migration_history,
        publication,
        invocation,
        mcp_tool,
        mcp_resource,
        authenticated,
        mcp_session,
        McpSseConnectionState::Active,
        store,
        policy_context,
        resume_token,
        membership_transition,
        row_visibility,
    );
}

#[test]
fn row_scope_contracts_round_trip_through_serde() {
    let key_prefix_policy = RowScopePolicy::KeyPrefix {
        prefix_template: "tenant/{tenant_id}/tickets/".to_string(),
        context_fields: vec!["tenant_id".to_string()],
    };
    let predicate_policy = RowScopePolicy::TypedRowPredicate {
        predicate_id: "ticket_owner_or_tenant".to_string(),
        row_type: Some("ticket".to_string()),
        referenced_fields: vec!["tenant_id".to_string(), "owner_id".to_string()],
    };
    let visibility_index = sample_visibility_index();
    let row_scope_binding = sample_row_scope_binding();
    let denial = row_scope_binding.explicit_denial_audit(
        RowQueryShape::Aggregate,
        Some("aggregates leak existence for this binding".to_string()),
    );
    let resume_token = FilteredScanResumeToken {
        version: 1,
        binding_name: "tickets".to_string(),
        query_shape: RowQueryShape::BoundedPrefixScan,
        family: RowScopeFamily::VisibilityIndex,
        last_primary_key: Some("ticket:t-9".to_string()),
        last_visibility_key: Some("user:alice/ticket:t-9".to_string()),
        scanned_rows: 11,
        returned_rows: 4,
        metadata: BTreeMap::from([("surface".to_string(), serde_json::json!("sandbox"))]),
    };

    let decoded_key_prefix: RowScopePolicy =
        serde_json::from_slice(&serde_json::to_vec(&key_prefix_policy).expect("encode key prefix"))
            .expect("decode key prefix");
    let decoded_predicate: RowScopePolicy =
        serde_json::from_slice(&serde_json::to_vec(&predicate_policy).expect("encode predicate"))
            .expect("decode predicate");
    let decoded_visibility_index: VisibilityIndexSpec = serde_json::from_slice(
        &serde_json::to_vec(&visibility_index).expect("encode visibility index"),
    )
    .expect("decode visibility index");
    let decoded_binding: RowScopeBinding =
        serde_json::from_slice(&serde_json::to_vec(&row_scope_binding).expect("encode binding"))
            .expect("decode binding");
    let decoded_denial: terracedb_capabilities::RowVisibilityAuditRecord =
        serde_json::from_slice(&serde_json::to_vec(&denial).expect("encode row denial"))
            .expect("decode row denial");
    let decoded_resume_token: FilteredScanResumeToken =
        serde_json::from_slice(&serde_json::to_vec(&resume_token).expect("encode resume token"))
            .expect("decode resume token");

    assert_eq!(decoded_key_prefix, key_prefix_policy);
    assert_eq!(decoded_predicate, predicate_policy);
    assert_eq!(decoded_visibility_index, visibility_index);
    assert_eq!(decoded_binding, row_scope_binding);
    assert_eq!(decoded_denial, denial);
    assert_eq!(
        decoded_denial.outcome,
        RowVisibilityOutcomeKind::ExplicitlyDenied
    );
    assert_eq!(decoded_resume_token, resume_token);
}

#[test]
fn serde_round_trips_grant_manifest_publication_and_execution_policy() {
    let grant = sample_grant();
    let manifest = CapabilityManifest {
        subject: Some(PolicySubject {
            subject_id: "user:alice".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            groups: vec![],
            attributes: BTreeMap::new(),
        }),
        preset_name: Some("draft-support".to_string()),
        profile_name: None,
        bindings: vec![ManifestBinding {
            binding_name: "tickets".to_string(),
            capability_family: "db.query.v1".to_string(),
            module_specifier: capability_module_specifier("tickets"),
            shell_command: Some(ShellCommandDescriptor::for_binding("tickets")),
            resource_policy: sample_template().default_resource_policy,
            budget_policy: sample_budget(),
            source_template_id: "db.query.v1".to_string(),
            source_grant_id: Some("grant-1".to_string()),
            allow_interactive_widening: true,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    };
    let execution_policy = sample_execution_policy();
    let publication = ReviewedProcedurePublication {
        publication: ProcedureVersionRef {
            procedure_id: "sync-ticket".to_string(),
            version: 2,
        },
        artifact: ProcedureImmutableArtifact::SandboxSnapshot {
            snapshot_id: "snapshot-2".to_string(),
        },
        code_hash: "sha256:def456".to_string(),
        entrypoint: "terrace:/workspace/procedures/sync-ticket.ts".to_string(),
        input_schema: serde_json::json!({ "type": "object" }),
        output_schema: serde_json::json!({ "type": "object" }),
        published_at: Timestamp::new(30),
        manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        review: ProcedureReview {
            reviewed_by: "reviewer".to_string(),
            source_revision: "def456".to_string(),
            note: None,
            approved_at: Timestamp::new(29),
        },
        metadata: BTreeMap::from([("immutable".to_string(), serde_json::json!(true))]),
    };

    let round_trip_grant: CapabilityGrant =
        serde_json::from_slice(&serde_json::to_vec(&grant).expect("encode grant"))
            .expect("decode grant");
    let round_trip_manifest: CapabilityManifest =
        serde_json::from_slice(&serde_json::to_vec(&manifest).expect("encode manifest"))
            .expect("decode manifest");
    let round_trip_execution: ExecutionPolicy =
        serde_json::from_slice(&serde_json::to_vec(&execution_policy).expect("encode exec policy"))
            .expect("decode exec policy");
    let round_trip_publication: ReviewedProcedurePublication =
        serde_json::from_slice(&serde_json::to_vec(&publication).expect("encode publication"))
            .expect("decode publication");

    assert_eq!(round_trip_grant, grant);
    assert_eq!(round_trip_manifest, manifest);
    assert_eq!(round_trip_execution, execution_policy);
    assert_eq!(round_trip_publication, publication);
}

#[test]
fn serde_round_trips_reviewed_procedure_contracts() {
    fn assert_round_trip<T>(value: &T)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let decoded: T =
            serde_json::from_slice(&serde_json::to_vec(value).expect("encode procedure contract"))
                .expect("decode procedure contract");
        assert_eq!(&decoded, value);
    }

    let caller = PolicySubject {
        subject_id: "user:alice".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        groups: vec!["support".to_string()],
        attributes: BTreeMap::from([("region".to_string(), "us-west".to_string())]),
    };
    let manifest = CapabilityManifest {
        subject: Some(caller.clone()),
        preset_name: Some("draft-support".to_string()),
        profile_name: None,
        bindings: vec![ManifestBinding {
            binding_name: "tickets".to_string(),
            capability_family: "db.query.v1".to_string(),
            module_specifier: capability_module_specifier("tickets"),
            shell_command: Some(ShellCommandDescriptor::for_binding("tickets")),
            resource_policy: sample_template().default_resource_policy,
            budget_policy: sample_budget(),
            source_template_id: "db.query.v1".to_string(),
            source_grant_id: Some("grant-1".to_string()),
            allow_interactive_widening: false,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::from([("surface".to_string(), serde_json::json!("reviewed"))]),
    };
    let execution_policy = sample_execution_policy();
    let publication_ref = ProcedureVersionRef {
        procedure_id: "sync-ticket".to_string(),
        version: 7,
    };
    let review = ProcedureReview {
        reviewed_by: "reviewer".to_string(),
        source_revision: "abc123".to_string(),
        note: Some("ship it".to_string()),
        approved_at: Timestamp::new(88),
    };
    let draft = ProcedureDraft {
        publication: publication_ref.clone(),
        source: ProcedureDraftSource::DraftSandboxSession {
            session_id: "draft-session".to_string(),
        },
        entrypoint: "run".to_string(),
        requested_manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        input_schema: serde_json::json!({
            "type": "object",
            "required": ["ticketId"],
            "properties": { "ticketId": { "type": "string" } }
        }),
        output_schema: serde_json::json!({
            "type": "object",
            "required": ["status"],
            "properties": { "status": { "type": "string" } }
        }),
        metadata: BTreeMap::from([("draft".to_string(), serde_json::json!(true))]),
    };
    let reviewed_draft = ReviewedProcedureDraft {
        draft: draft.clone(),
        review: review.clone(),
        published_at: Timestamp::new(90),
        metadata: BTreeMap::from([("ready".to_string(), serde_json::json!(true))]),
    };
    let artifact = FrozenProcedureArtifact {
        artifact: ProcedureImmutableArtifact::SandboxSnapshot {
            snapshot_id: "snapshot-7".to_string(),
        },
        code_hash: "sha256:7777".to_string(),
        metadata: BTreeMap::from([("immutable".to_string(), serde_json::json!(true))]),
    };
    let publication = ReviewedProcedurePublication {
        publication: publication_ref.clone(),
        artifact: artifact.artifact.clone(),
        code_hash: artifact.code_hash.clone(),
        entrypoint: draft.entrypoint.clone(),
        input_schema: draft.input_schema.clone(),
        output_schema: draft.output_schema.clone(),
        published_at: reviewed_draft.published_at,
        manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        review: review.clone(),
        metadata: BTreeMap::from([("published".to_string(), serde_json::json!(true))]),
    };
    let publication_receipt = ProcedurePublicationReceipt {
        publication: publication_ref.clone(),
        published_at: publication.published_at,
        code_hash: publication.code_hash.clone(),
        execution_policy: execution_policy.clone(),
    };
    let deployment = ProcedureDeployment {
        deployment_id: "prod".to_string(),
        invocation_binding: ManifestBinding {
            binding_name: "procedures".to_string(),
            capability_family: "procedure.invoke.v1".to_string(),
            module_specifier: capability_module_specifier("procedures"),
            shell_command: None,
            resource_policy: ResourcePolicy {
                allow: vec![ResourceSelector {
                    kind: ResourceKind::Procedure,
                    pattern: "sync-*".to_string(),
                }],
                deny: vec![],
                tenant_scopes: vec!["tenant-a".to_string()],
                row_scope_binding: None,
                visibility_index: None,
                metadata: BTreeMap::new(),
            },
            budget_policy: sample_budget(),
            source_template_id: "procedure.invoke.v1".to_string(),
            source_grant_id: None,
            allow_interactive_widening: false,
            metadata: BTreeMap::new(),
        },
        execution_policy: execution_policy.clone(),
        binding_overrides: vec![ManifestBindingOverride {
            binding_name: "tickets".to_string(),
            drop_binding: false,
            resource_policy: None,
            budget_policy: Some(BudgetPolicy {
                max_calls: Some(2),
                max_scanned_rows: Some(10),
                max_returned_rows: Some(5),
                max_bytes: Some(1024),
                max_millis: Some(50),
                rate_limit_bucket: Some("reviewed-procedure".to_string()),
                labels: BTreeMap::new(),
            }),
            expose_in_just_bash: None,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::from([("env".to_string(), serde_json::json!("prod"))]),
    };
    let request = ProcedureInvocationRequest {
        publication: publication_ref.clone(),
        arguments: serde_json::json!({ "ticketId": "t-1" }),
        context: ProcedureInvocationContext {
            caller: Some(caller.clone()),
            session_mode: SessionMode::ReviewedProcedure,
            session_id: Some("proc-session".to_string()),
            dry_run: false,
            metadata: BTreeMap::from([("trace".to_string(), serde_json::json!("abc"))]),
        },
    };
    let policy_outcome = PolicyDecisionRecord {
        outcome: PolicyOutcomeRecord {
            binding_name: "procedures".to_string(),
            outcome: PolicyOutcomeKind::Allowed,
            message: None,
            observed_at: Timestamp::new(91),
            row_visibility: None,
            metadata: BTreeMap::new(),
        },
        audit: PolicyAuditMetadata {
            session_id: "proc-session".to_string(),
            session_mode: SessionMode::ReviewedProcedure,
            subject: caller.clone(),
            binding_name: "procedures".to_string(),
            capability_family: Some("procedure.invoke.v1".to_string()),
            target_resources: vec![ResourceTarget {
                kind: ResourceKind::Procedure,
                identifier: "sync-ticket".to_string(),
            }],
            execution_domain: ExecutionDomain::DedicatedSandbox,
            placement_tags: vec!["reviewed".to_string()],
            preset_name: None,
            profile_name: None,
            expanded_manifest: Some(manifest.clone()),
            rate_limit: None,
            budget_hook: None,
            metadata: BTreeMap::new(),
        },
    };
    let binding_resolution = ProcedureBindingResolution {
        binding_name: "tickets".to_string(),
        effective_row_scope: None,
    };
    let prepared = PreparedProcedureInvocation {
        invocation_id: "sync-ticket@7:invoke-1".to_string(),
        publication: publication.clone(),
        request: request.clone(),
        deployment: deployment.clone(),
        effective_manifest: manifest.clone(),
        execution_policy: execution_policy.clone(),
        execution_domain: ExecutionDomain::DedicatedSandbox,
        binding_resolutions: vec![binding_resolution.clone()],
        policy_outcome: policy_outcome.clone(),
        metadata: BTreeMap::from([("prepared".to_string(), serde_json::json!(true))]),
    };
    let receipt = ProcedureInvocationReceipt {
        invocation_id: prepared.invocation_id.clone(),
        publication: publication_ref.clone(),
        accepted_at: Timestamp::new(91),
        completed_at: Timestamp::new(92),
        execution_policy: execution_policy.clone(),
        execution_domain: ExecutionDomain::DedicatedSandbox,
        effective_manifest: manifest.clone(),
        binding_resolutions: vec![binding_resolution.clone()],
        output: serde_json::json!({ "status": "queued" }),
        metadata: BTreeMap::new(),
    };
    let record = ProcedureInvocationRecord {
        invocation_id: prepared.invocation_id.clone(),
        publication: publication_ref.clone(),
        state: ProcedureInvocationState::Succeeded,
        requested_at: Timestamp::new(91),
        completed_at: Timestamp::new(92),
        caller: Some(caller.clone()),
        execution_policy: execution_policy.clone(),
        execution_domain: ExecutionDomain::DedicatedSandbox,
        effective_manifest: manifest.clone(),
        binding_resolutions: vec![binding_resolution.clone()],
        policy_outcome: policy_outcome.clone(),
        arguments: request.arguments.clone(),
        output: Some(receipt.output.clone()),
        failure: None,
        metadata: BTreeMap::new(),
    };
    let audit = ProcedureAuditRecord {
        audit_id: "sync-ticket@7:audit-1".to_string(),
        procedure_id: "sync-ticket".to_string(),
        version: 7,
        invocation_id: Some(prepared.invocation_id.clone()),
        event: ProcedureAuditEventKind::InvocationSucceeded,
        recorded_at: Timestamp::new(92),
        execution_operation: ExecutionOperation::ProcedureInvocation,
        metadata: BTreeMap::new(),
    };
    let pending_publication = PendingProcedurePublication {
        publication: publication.clone(),
        recorded_at: Timestamp::new(90),
    };
    let pending_invocation = PendingProcedureInvocation {
        invocation: prepared.clone(),
        recorded_at: Timestamp::new(91),
    };

    assert_round_trip(&draft);
    assert_round_trip(&reviewed_draft);
    assert_round_trip(&artifact);
    assert_round_trip(&publication);
    assert_round_trip(&publication_receipt);
    assert_round_trip(&deployment);
    assert_round_trip(&request);
    assert_round_trip(&binding_resolution);
    assert_round_trip(&prepared);
    assert_round_trip(&receipt);
    assert_round_trip(&record);
    assert_round_trip(&audit);
    assert_round_trip(&pending_publication);
    assert_round_trip(&pending_invocation);
}

#[test]
fn deterministic_smoke_resolves_fake_subject_into_manifest_and_execution_policy() {
    let engine = resolve_sample_policy(DeterministicRateLimiter::default());

    assert_eq!(engine.subject.subject_id, "user:alice");
    assert_eq!(engine.session_mode, SessionMode::Draft);
    assert_eq!(engine.manifest.bindings.len(), 1);
    let binding = &engine.manifest.bindings[0];
    assert_eq!(binding.binding_name, "tickets");
    assert_eq!(binding.module_specifier, "terrace:host/tickets");
    assert_eq!(
        binding.shell_command,
        Some(ShellCommandDescriptor::for_binding("tickets"))
    );
    assert!(binding.allow_interactive_widening);
    assert_eq!(
        engine.execution_policy.operations[&ExecutionOperation::ProcedureInvocation].domain,
        ExecutionDomain::DedicatedSandbox
    );
    assert!(
        engine
            .rate_limits
            .iter()
            .all(|outcome| outcome.outcome.allowed),
        "smoke test should use deterministic allow-by-default rate limiting"
    );
}

#[test]
fn deterministic_row_scope_lookup_replays_the_same_visibility_answer() {
    let resolved = resolve_sample_policy(DeterministicRateLimiter::default());
    let binding = &resolved.manifest.bindings[0];
    let context = PolicyContext::from(resolved.subject.clone());
    let visibility = DeterministicVisibilityIndexStore::default()
        .with_transition(VisibilityMembershipTransition {
            index_name: "visible_by_subject".to_string(),
            lookup_key: "user:alice".to_string(),
            row_id: "ticket:t-1".to_string(),
            from_visible: false,
            to_visible: true,
            metadata: BTreeMap::new(),
        })
        .with_visible_row("visible_by_subject", "user:alice", "ticket:t-2");

    let first = binding
        .resource_policy
        .resolve_row_scope(context.clone(), &visibility)
        .expect("resolve first row scope")
        .expect("row scope binding should be attached");
    let second = binding
        .resource_policy
        .resolve_row_scope(context, &visibility)
        .expect("resolve second row scope")
        .expect("row scope binding should be attached");

    assert_eq!(first, second);
    assert_eq!(first.binding_id, "tickets.visible");
    assert_eq!(
        first.allowed_query_shapes,
        vec![RowQueryShape::PointRead, RowQueryShape::BoundedPrefixScan]
    );
    assert_eq!(
        first
            .visibility_lookup
            .as_ref()
            .map(|lookup| lookup.lookup_keys.clone()),
        Some(vec!["user:alice".to_string()])
    );
    assert_eq!(
        first
            .visibility_lookup
            .as_ref()
            .map(|lookup| lookup.visible_row_ids.clone()),
        Some(vec!["ticket:t-1".to_string(), "ticket:t-2".to_string()])
    );
}

#[test]
fn preset_host_api_lists_describes_and_prepares_selected_manifest() {
    let engine = sample_policy_engine(DeterministicRateLimiter::default());

    let presets = engine.list_presets();
    let profiles = engine.list_profiles(Some("draft-support"));
    let described_preset = engine
        .describe_preset("draft-support")
        .expect("describe preset");
    let described_profile = engine
        .describe_profile("foreground")
        .expect("describe profile");
    let prepared = engine
        .prepare_session_from_preset(&sample_preset_request(Some("foreground")))
        .expect("prepare session from preset");

    assert_eq!(presets.len(), 1);
    assert_eq!(presets[0].name, "draft-support");
    assert_eq!(presets[0].default_session_mode, SessionMode::Draft);
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].name, "foreground");
    assert_eq!(described_preset.name, "draft-support");
    assert_eq!(described_profile.preset_name, "draft-support");
    assert_eq!(prepared.preset.name, "draft-support");
    assert_eq!(
        prepared
            .profile
            .as_ref()
            .map(|profile| profile.name.as_str()),
        Some("foreground")
    );
    assert_eq!(
        prepared.resolved.manifest.preset_name.as_deref(),
        Some("draft-support")
    );
    assert_eq!(
        prepared.resolved.manifest.profile_name.as_deref(),
        Some("foreground")
    );
    assert_eq!(
        prepared.resolved.manifest.bindings[0]
            .metadata
            .get("preset_name"),
        Some(&serde_json::json!("draft-support"))
    );
    assert_eq!(
        prepared.audit_metadata.get("surface"),
        Some(&serde_json::json!("host-api"))
    );
    assert_eq!(
        prepared.audit_metadata.get("expanded_manifest"),
        Some(&serde_json::to_value(&prepared.resolved.manifest).expect("manifest json"))
    );
}

#[test]
fn prepared_sessions_can_narrow_bindings_before_open() {
    let engine = sample_policy_engine(DeterministicRateLimiter::default());
    let mut request = sample_preset_request(Some("foreground"));
    request.binding_overrides.push(ManifestBindingOverride {
        binding_name: "tickets".to_string(),
        drop_binding: false,
        resource_policy: Some(ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets".to_string(),
            }],
            deny: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets/archive".to_string(),
            }],
            tenant_scopes: vec!["tenant-a".to_string()],
            row_scope_binding: Some(sample_row_scope_binding()),
            visibility_index: Some(sample_visibility_index()),
            metadata: BTreeMap::from([("narrowed".to_string(), serde_json::json!(true))]),
        }),
        budget_policy: Some(BudgetPolicy {
            max_calls: Some(2),
            max_scanned_rows: Some(20),
            max_returned_rows: None,
            max_bytes: None,
            max_millis: Some(100),
            rate_limit_bucket: Some("draft-user".to_string()),
            labels: BTreeMap::from([("lane".to_string(), "foreground".to_string())]),
        }),
        expose_in_just_bash: Some(false),
        metadata: BTreeMap::from([("override".to_string(), serde_json::json!("session"))]),
    });

    let prepared = engine
        .prepare_session_from_preset(&request)
        .expect("prepare narrowed preset session");
    let binding = &prepared.resolved.manifest.bindings[0];

    assert_eq!(binding.budget_policy.max_calls, Some(2));
    assert_eq!(binding.budget_policy.max_scanned_rows, Some(20));
    assert_eq!(binding.budget_policy.max_bytes, Some(4096));
    assert_eq!(binding.shell_command, None);
    assert!(
        binding
            .resource_policy
            .deny
            .iter()
            .any(|selector| selector.pattern == "tickets/archive")
    );
    assert_eq!(
        binding.metadata.get("override"),
        Some(&serde_json::json!("session"))
    );
}

#[test]
fn presets_cannot_widen_authority_beyond_standing_grants() {
    let engine = sample_policy_engine(DeterministicRateLimiter::default()).with_preset(
        CapabilityPresetDescriptor {
            name: "too-wide".to_string(),
            description: Some("Attempts to widen authority".to_string()),
            bindings: vec![PresetBinding {
                template_id: "db.query.v1".to_string(),
                binding_name: Some("tickets".to_string()),
                resource_policy: Some(ResourcePolicy {
                    allow: vec![ResourceSelector {
                        kind: ResourceKind::Table,
                        pattern: "private_tickets".to_string(),
                    }],
                    deny: vec![],
                    tenant_scopes: vec![],
                    row_scope_binding: Some(sample_row_scope_binding()),
                    visibility_index: Some(sample_visibility_index()),
                    metadata: BTreeMap::new(),
                }),
                budget_policy: None,
                expose_in_just_bash: Some(true),
            }],
            default_session_mode: SessionMode::Draft,
            default_execution_policy: None,
            metadata: BTreeMap::new(),
        },
    );

    let error = engine
        .prepare_session_from_preset(&SessionPresetRequest {
            subject: SubjectResolutionRequest {
                session_id: "session-1".to_string(),
                auth_subject_hint: Some("alice".to_string()),
                tenant_hint: Some("tenant-a".to_string()),
                groups: vec![],
                attributes: BTreeMap::new(),
            },
            preset_name: "too-wide".to_string(),
            profile_name: None,
            session_mode_override: None,
            binding_overrides: Vec::new(),
            metadata: BTreeMap::new(),
        })
        .expect_err("widening preset should be rejected");

    assert!(matches!(
        error,
        PolicyError::UnsafeBindingOverride {
            override_source,
            binding_name,
            ..
        } if override_source == "preset/profile selection" && binding_name == "tickets"
    ));
}

#[test]
fn draft_authorization_decision_round_trips() {
    let decision = DraftAuthorizationDecision {
        request_id: "auth-1".to_string(),
        outcome: DraftAuthorizationOutcomeKind::Approved,
        approved_scope: Some(AuthorizationScope::Session),
        decided_at: Timestamp::new(42),
        note: Some("approved for this session".to_string()),
        metadata: BTreeMap::new(),
    };
    let encoded = serde_json::to_vec(&decision).expect("encode");
    let decoded: DraftAuthorizationDecision = serde_json::from_slice(&encoded).expect("decode");
    assert_eq!(decoded, decision);
}

#[test]
fn static_execution_policy_resolver_prefers_profile_and_replays_deterministically() {
    let mode_policy = sample_execution_policy();
    let preset_policy = ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::SharedBackground,
            budget: Some(sample_budget()),
            placement_tags: vec!["preset".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::new(),
        metadata: BTreeMap::from([("owner".to_string(), serde_json::json!("preset"))]),
    };
    let profile_policy = ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::RemoteWorker,
            budget: Some(sample_budget()),
            placement_tags: vec!["profile".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::new(),
        metadata: BTreeMap::from([("owner".to_string(), serde_json::json!("profile"))]),
    };

    let engine = DeterministicPolicyEngine::new(
        vec![sample_template()],
        vec![sample_grant()],
        sample_subject_resolver(),
        StaticExecutionPolicyResolver::new(sample_execution_policy())
            .with_policy(SessionMode::Draft, mode_policy.clone())
            .with_preset_policy("draft-support", preset_policy.clone())
            .with_profile_policy("remote", profile_policy.clone()),
    )
    .with_preset(CapabilityPresetDescriptor {
        name: "draft-support".to_string(),
        description: Some("Draft support preset".to_string()),
        bindings: vec![PresetBinding {
            template_id: "db.query.v1".to_string(),
            binding_name: Some("tickets".to_string()),
            resource_policy: None,
            budget_policy: None,
            expose_in_just_bash: Some(true),
        }],
        default_session_mode: SessionMode::Draft,
        default_execution_policy: Some(preset_policy.clone()),
        metadata: BTreeMap::new(),
    })
    .with_profile(CapabilityProfileDescriptor {
        name: "remote".to_string(),
        preset_name: "draft-support".to_string(),
        add_bindings: vec![],
        drop_bindings: vec![],
        execution_policy_override: Some(profile_policy.clone()),
        metadata: BTreeMap::new(),
    });

    let request = sample_resolution_request(Some("remote"));
    let first = engine.resolve(&request).expect("resolve first policy");
    let second = engine.resolve(&request).expect("resolve second policy");

    assert_eq!(first.execution_policy, second.execution_policy);
    assert_eq!(
        first
            .execution_policy
            .assignment_for(ExecutionOperation::PackageInstall)
            .domain,
        ExecutionDomain::RemoteWorker
    );
    assert_eq!(
        first
            .execution_policy
            .assignment_for(ExecutionOperation::DraftSession)
            .placement_tags,
        vec!["profile".to_string()]
    );
    assert!(
        first
            .budget_hooks
            .contains_key(&ExecutionOperation::PackageInstall)
    );
}

#[test]
fn policy_decisions_emit_stable_audit_metadata_and_auth_request_kinds() {
    let resolved = resolve_sample_policy(DeterministicRateLimiter::default());

    let allowed = resolved.evaluate_use(&CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::ProcedureInvocation,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(100),
        metadata: BTreeMap::from([("caller".to_string(), serde_json::json!("sandbox"))]),
    });
    assert_eq!(allowed.outcome.outcome, PolicyOutcomeKind::Allowed);
    assert_eq!(
        allowed.audit.execution_domain,
        ExecutionDomain::DedicatedSandbox
    );
    assert_eq!(allowed.audit.preset_name.as_deref(), Some("draft-support"));
    assert_eq!(allowed.audit.profile_name.as_deref(), Some("foreground"));
    assert_eq!(
        allowed.audit.expanded_manifest,
        Some(resolved.manifest.clone())
    );
    assert_eq!(
        allowed
            .audit
            .budget_hook
            .as_ref()
            .map(|hook| hook.hook_name.as_str()),
        Some("budget::ProcedureInvocation")
    );

    let missing = resolved.evaluate_use(&CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "admin".to_string(),
        capability_family: Some("db.admin.v1".to_string()),
        targets: vec![],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(101),
        metadata: BTreeMap::new(),
    });
    assert_eq!(missing.outcome.outcome, PolicyOutcomeKind::MissingBinding);
    let missing_auth = missing
        .draft_authorization_request(
            "auth-missing",
            AuthorizationScope::Session,
            Timestamp::new(102),
            None,
        )
        .expect("missing binding should produce an authorization request");
    assert_eq!(
        missing_auth.kind,
        DraftAuthorizationRequestKind::InjectMissingBinding
    );
    assert_eq!(missing_auth.capability_family, "db.admin.v1");

    let denied = resolved.evaluate_use(&CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "private_tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(103),
        metadata: BTreeMap::new(),
    });
    assert_eq!(denied.outcome.outcome, PolicyOutcomeKind::Denied);
    let denied_auth = denied
        .draft_authorization_request(
            "auth-denied",
            AuthorizationScope::OneCall,
            Timestamp::new(104),
            Some("retry with approval".to_string()),
        )
        .expect("denied operation should produce an authorization request");
    assert_eq!(
        denied_auth.kind,
        DraftAuthorizationRequestKind::RetryDeniedOperation
    );
    assert_eq!(denied_auth.binding_name, "tickets");

    let rate_limited =
        resolve_sample_policy(DeterministicRateLimiter::default().with_binding_outcome(
            "tickets",
            RateLimitOutcome {
                allowed: false,
                bucket: Some("draft-user".to_string()),
                reason: Some("burst exceeded".to_string()),
            },
        ))
        .evaluate_use(&CapabilityUseRequest {
            session_id: "session-1".to_string(),
            operation: ExecutionOperation::DraftSession,
            binding_name: "tickets".to_string(),
            capability_family: None,
            targets: vec![ResourceTarget {
                kind: ResourceKind::Table,
                identifier: "tickets".to_string(),
            }],
            metrics: CapabilityUseMetrics::default(),
            requested_at: Timestamp::new(105),
            metadata: BTreeMap::new(),
        });
    assert_eq!(rate_limited.outcome.outcome, PolicyOutcomeKind::RateLimited);
    assert_eq!(
        rate_limited.outcome.message.as_deref(),
        Some("burst exceeded")
    );
    assert!(
        rate_limited
            .draft_authorization_request(
                "auth-rate",
                AuthorizationScope::Session,
                Timestamp::new(106),
                None,
            )
            .is_none()
    );

    let budget_exhausted = resolved.evaluate_use(&CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics {
            bytes: Some(8_192),
            ..CapabilityUseMetrics::default()
        },
        requested_at: Timestamp::new(107),
        metadata: BTreeMap::new(),
    });
    assert_eq!(
        budget_exhausted.outcome.outcome,
        PolicyOutcomeKind::BudgetExhausted
    );
    assert_eq!(
        budget_exhausted.outcome.message.as_deref(),
        Some("requested bytes 8192 exceeds max_bytes 4096")
    );
}

#[test]
fn foreground_session_status_projection_replays_the_same_history() {
    let resolved = resolve_sample_policy(DeterministicRateLimiter::default());
    let denied = resolved.evaluate_use(&CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "private_tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(200),
        metadata: BTreeMap::new(),
    });
    let pending_authorization = denied
        .draft_authorization_request(
            "auth-waiting",
            AuthorizationScope::Session,
            Timestamp::new(201),
            None,
        )
        .expect("draft denial should create a pending request");
    let projector = ForegroundSessionStatusProjector::new(
        "session-1",
        resolved.session_mode,
        Some(resolved.subject.clone()),
        resolved.manifest.clone(),
        resolved.execution_policy.clone(),
    )
    .with_metadata("surface", serde_json::json!("sandbox"));
    let updates = vec![
        SessionStatusUpdate::ToolRun {
            running: true,
            updated_at: Timestamp::new(202),
            metadata: BTreeMap::new(),
        },
        SessionStatusUpdate::PolicyOutcome {
            outcome: denied.outcome.clone(),
            updated_at: Timestamp::new(203),
            metadata: BTreeMap::new(),
        },
        SessionStatusUpdate::ActivityEntry {
            lifecycle: None,
            pending_authorization: Some(pending_authorization.clone()),
            updated_at: Timestamp::new(204),
            metadata: BTreeMap::from([(
                "activity".to_string(),
                serde_json::json!("authorization_requested"),
            )]),
        },
        SessionStatusUpdate::ViewState {
            visible: true,
            updated_at: Timestamp::new(205),
            metadata: BTreeMap::new(),
        },
    ];

    let first = projector.project(7, &updates);
    let second = projector.project(7, &updates);

    assert_eq!(first, second);
    assert_eq!(
        first.snapshot.lifecycle,
        SessionLifecycleState::WaitingForAuthorization
    );
    assert_eq!(
        first.snapshot.pending_authorization,
        Some(pending_authorization)
    );
    assert_eq!(
        first.snapshot.last_policy_outcome,
        Some(denied.outcome.clone())
    );
    assert_eq!(first.snapshot.updated_at, Timestamp::new(205));
    assert_eq!(
        first.sources,
        vec![
            SessionStatusSource::ToolRun,
            SessionStatusSource::PolicyOutcome,
            SessionStatusSource::ActivityEntry,
            SessionStatusSource::ViewState,
        ]
    );
    assert_eq!(
        first.snapshot.metadata.get("surface"),
        Some(&serde_json::json!("sandbox"))
    );
    assert_eq!(
        first.snapshot.metadata.get("view_visible"),
        Some(&serde_json::json!(true))
    );
}

#[test]
fn trusted_draft_binding_requests_refresh_manifest_and_record_retry_history() {
    let mut session = DeterministicDraftAuthorizationSession::open(
        interactive_policy_engine(),
        sample_resolution_request(None),
        true,
    )
    .expect("open trusted draft session");
    assert!(
        session
            .resolved_policy()
            .manifest
            .bindings
            .iter()
            .all(|binding| binding.binding_name != "admin")
    );

    let request = session
        .request_binding_authorization(
            "auth-admin",
            "admin",
            "db.admin.v1",
            AuthorizationScope::Session,
            Timestamp::new(300),
            Some("need admin tools".to_string()),
            BTreeMap::from([("surface".to_string(), serde_json::json!("host"))]),
        )
        .expect("request admin binding");
    assert_eq!(
        request.kind,
        DraftAuthorizationRequestKind::InjectMissingBinding
    );

    let applied = session
        .apply_decision(
            DraftAuthorizationDecision {
                request_id: request.request_id.clone(),
                outcome: DraftAuthorizationOutcomeKind::Approved,
                approved_scope: Some(AuthorizationScope::Session),
                decided_at: Timestamp::new(301),
                note: Some("approved for this draft session".to_string()),
                metadata: BTreeMap::new(),
            },
            None,
            Some(sample_resolution_request(Some("admin-elevated"))),
        )
        .expect("approve admin binding");
    assert!(applied.pending_authorization.is_none());
    assert!(
        applied
            .refreshed_manifest
            .as_ref()
            .expect("session approval should refresh the manifest")
            .bindings
            .iter()
            .any(|binding| binding.binding_name == "admin")
    );

    let allowed = session.evaluate_use(CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "admin".to_string(),
        capability_family: Some("db.admin.v1".to_string()),
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "admin_console".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(302),
        metadata: BTreeMap::new(),
    });
    assert_eq!(allowed.outcome.outcome, PolicyOutcomeKind::Allowed);

    assert_eq!(session.history().len(), 4);
    assert!(matches!(
        session.history()[0].kind,
        DraftAuthorizationHistoryKind::AuthorizationRequested { .. }
    ));
    assert!(matches!(
        session.history()[1].kind,
        DraftAuthorizationHistoryKind::AuthorizationDecided { .. }
    ));
    assert!(matches!(
        session.history()[2].kind,
        DraftAuthorizationHistoryKind::ManifestRefreshed { .. }
    ));
    assert!(matches!(
        session.history()[3].kind,
        DraftAuthorizationHistoryKind::RetryOutcome { .. }
    ));
}

#[test]
fn trusted_draft_retry_denial_can_be_approved_for_one_call_only() {
    let mut session = DeterministicDraftAuthorizationSession::open(
        interactive_policy_engine(),
        sample_resolution_request(None),
        true,
    )
    .expect("open trusted draft session");

    let request = CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "private_tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(400),
        metadata: BTreeMap::new(),
    };

    let denied = session.evaluate_use(request.clone());
    assert_eq!(denied.outcome.outcome, PolicyOutcomeKind::Denied);

    let auth_request = session
        .request_authorization_for_outcome(
            request.clone(),
            denied.clone(),
            "auth-private-tickets",
            AuthorizationScope::OneCall,
            Timestamp::new(401),
            Some("retry this denied operation".to_string()),
        )
        .expect("request should be allowed for trusted draft")
        .expect("denied operation should create an authorization request");
    assert_eq!(
        auth_request.kind,
        DraftAuthorizationRequestKind::RetryDeniedOperation
    );

    let applied = session
        .apply_decision(
            DraftAuthorizationDecision {
                request_id: auth_request.request_id.clone(),
                outcome: DraftAuthorizationOutcomeKind::Approved,
                approved_scope: Some(AuthorizationScope::OneCall),
                decided_at: Timestamp::new(402),
                note: Some("one retry approved".to_string()),
                metadata: BTreeMap::new(),
            },
            Some(interactive_policy_engine_with_private_ticket_access()),
            None,
        )
        .expect("approve one retry");
    assert!(applied.refreshed_manifest.is_none());

    let mutated_retry = session.evaluate_use(CapabilityUseRequest {
        requested_at: Timestamp::new(403),
        metrics: CapabilityUseMetrics {
            call_count: 2,
            ..CapabilityUseMetrics::default()
        },
        ..request.clone()
    });
    assert_eq!(mutated_retry.outcome.outcome, PolicyOutcomeKind::Denied);

    let first_retry = session.evaluate_use(request.clone());
    assert_eq!(first_retry.outcome.outcome, PolicyOutcomeKind::Allowed);

    let second_retry = session.evaluate_use(CapabilityUseRequest { ..request.clone() });
    assert_eq!(second_retry.outcome.outcome, PolicyOutcomeKind::Denied);

    assert_eq!(session.history().len(), 6);
    assert!(matches!(
        session.history()[0].kind,
        DraftAuthorizationHistoryKind::PolicyOutcome { .. }
    ));
    assert!(matches!(
        session.history()[1].kind,
        DraftAuthorizationHistoryKind::AuthorizationRequested { .. }
    ));
    assert!(matches!(
        session.history()[2].kind,
        DraftAuthorizationHistoryKind::AuthorizationDecided { .. }
    ));
    assert!(matches!(
        session.history()[3].kind,
        DraftAuthorizationHistoryKind::PolicyOutcome { .. }
    ));
    assert!(matches!(
        session.history()[4].kind,
        DraftAuthorizationHistoryKind::RetryOutcome { .. }
    ));
    assert!(matches!(
        session.history()[5].kind,
        DraftAuthorizationHistoryKind::PolicyOutcome { .. }
    ));
}

#[test]
fn interactive_authorization_is_blocked_for_untrusted_and_non_draft_sessions() {
    let untrusted_engine = interactive_policy_engine();
    let mut untrusted = DeterministicDraftAuthorizationSession::open(
        untrusted_engine,
        sample_resolution_request(None),
        false,
    )
    .expect("open untrusted draft session");
    let request = CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "admin".to_string(),
        capability_family: Some("db.admin.v1".to_string()),
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "admin_console".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(500),
        metadata: BTreeMap::new(),
    };
    let missing = untrusted.evaluate_use(request.clone());
    assert_eq!(missing.outcome.outcome, PolicyOutcomeKind::MissingBinding);
    let untrusted_error = untrusted
        .request_authorization_for_outcome(
            request,
            missing,
            "auth-untrusted",
            AuthorizationScope::Session,
            Timestamp::new(501),
            None,
        )
        .expect_err("untrusted drafts should not enter interactive authorization");
    assert!(matches!(
        untrusted_error,
        DraftAuthorizationFlowError::InteractiveAuthorizationDisabled
    ));

    let mut reviewed_request = sample_resolution_request(None);
    reviewed_request.session_mode = SessionMode::ReviewedProcedure;
    let mut reviewed = DeterministicDraftAuthorizationSession::open(
        interactive_policy_engine(),
        reviewed_request,
        true,
    )
    .expect("open reviewed procedure session");
    let reviewed_error = reviewed
        .request_binding_authorization(
            "auth-reviewed",
            "admin",
            "db.admin.v1",
            AuthorizationScope::Session,
            Timestamp::new(502),
            Some("should stay blocked".to_string()),
            BTreeMap::new(),
        )
        .expect_err("reviewed procedures should not enter interactive authorization");
    assert!(matches!(
        reviewed_error,
        DraftAuthorizationFlowError::InteractiveAuthorizationDisabled
    ));
}

#[test]
fn draft_authorization_session_state_round_trips_across_restore() {
    let mut pending = DeterministicDraftAuthorizationSession::open(
        interactive_policy_engine(),
        sample_resolution_request(None),
        true,
    )
    .expect("open trusted draft session");
    let pending_request = pending
        .request_binding_authorization(
            "auth-admin-pending",
            "admin",
            "db.admin.v1",
            AuthorizationScope::Session,
            Timestamp::new(600),
            Some("need admin tools".to_string()),
            BTreeMap::new(),
        )
        .expect("request binding");
    let pending_snapshot = pending.snapshot_json().expect("snapshot pending state");
    let restored_pending = DeterministicDraftAuthorizationSession::restore_from_json(
        interactive_policy_engine(),
        pending_snapshot,
    )
    .expect("restore pending state");
    assert_eq!(
        restored_pending.pending_authorization(),
        Some(&pending_request)
    );
    assert_eq!(restored_pending.history(), pending.history());

    let mut one_call = DeterministicDraftAuthorizationSession::open(
        interactive_policy_engine(),
        sample_resolution_request(None),
        true,
    )
    .expect("open trusted draft session");
    let denied_request = CapabilityUseRequest {
        session_id: "session-1".to_string(),
        operation: ExecutionOperation::DraftSession,
        binding_name: "tickets".to_string(),
        capability_family: None,
        targets: vec![ResourceTarget {
            kind: ResourceKind::Table,
            identifier: "private_tickets".to_string(),
        }],
        metrics: CapabilityUseMetrics::default(),
        requested_at: Timestamp::new(601),
        metadata: BTreeMap::new(),
    };
    let denied = one_call.evaluate_use(denied_request.clone());
    let retry_request = one_call
        .request_authorization_for_outcome(
            denied_request.clone(),
            denied,
            "auth-private-restore",
            AuthorizationScope::OneCall,
            Timestamp::new(602),
            None,
        )
        .expect("request should succeed")
        .expect("denied call should create request");
    one_call
        .apply_decision(
            DraftAuthorizationDecision {
                request_id: retry_request.request_id.clone(),
                outcome: DraftAuthorizationOutcomeKind::Approved,
                approved_scope: Some(AuthorizationScope::OneCall),
                decided_at: Timestamp::new(603),
                note: Some("retry once".to_string()),
                metadata: BTreeMap::new(),
            },
            Some(interactive_policy_engine_with_private_ticket_access()),
            None,
        )
        .expect("approve retry");

    let one_call_snapshot = one_call.snapshot_json().expect("snapshot one-call state");
    let mut restored_one_call = DeterministicDraftAuthorizationSession::restore_from_json(
        interactive_policy_engine(),
        one_call_snapshot,
    )
    .expect("restore one-call state");
    let allowed = restored_one_call.evaluate_use(denied_request.clone());
    assert_eq!(allowed.outcome.outcome, PolicyOutcomeKind::Allowed);
    assert_eq!(
        restored_one_call
            .evaluate_use(denied_request)
            .outcome
            .outcome,
        PolicyOutcomeKind::Denied
    );
    let replay_snapshot = restored_one_call
        .snapshot_json()
        .expect("snapshot consumed one-call state");
    let replayed = DeterministicDraftAuthorizationSession::restore_from_json(
        interactive_policy_engine(),
        replay_snapshot,
    )
    .expect("restore consumed state");
    assert_eq!(replayed.history(), restored_one_call.history());
}

#[test]
fn database_capability_families_expose_expected_generated_surfaces() {
    assert_eq!(
        DatabaseCapabilityFamily::DbTableV1.generated_methods(),
        &["get", "scanPrefix", "put", "delete"]
    );
    assert_eq!(
        DatabaseCapabilityFamily::CatalogMigrateV1.generated_methods(),
        &[
            "ensureTable",
            "installSchemaSuccessor",
            "updateTableMetadata",
            "checkPrecondition",
        ]
    );
    assert!(
        DatabaseCapabilityFamily::ProcedureInvokeV1
            .generated_typescript_declarations("terrace:host/proc")
            .contains("procedureId")
    );
}

#[test]
fn database_access_rewrites_key_prefix_for_allowed_reads() {
    let resolved = resolve_database_policy(
        "db.table.v1",
        "tickets",
        ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets".to_string(),
            }],
            deny: vec![],
            tenant_scopes: vec!["tenant-a".to_string()],
            row_scope_binding: Some(RowScopeBinding {
                binding_id: "tickets.prefix".to_string(),
                policy: RowScopePolicy::KeyPrefix {
                    prefix_template: "tenant/{tenant_id}/".to_string(),
                    context_fields: vec!["tenant_id".to_string()],
                },
                allowed_query_shapes: vec![
                    RowQueryShape::PointRead,
                    RowQueryShape::BoundedPrefixScan,
                    RowQueryShape::WriteMutation,
                ],
                write_semantics: Default::default(),
                denial_contract: Default::default(),
                metadata: BTreeMap::new(),
            }),
            visibility_index: None,
            metadata: BTreeMap::new(),
        },
    );

    let decision = resolved
        .evaluate_database_access(
            &DatabaseAccessRequest {
                session_id: "session-1".to_string(),
                operation: ExecutionOperation::DraftSession,
                binding_name: "tickets".to_string(),
                capability_family: None,
                action: DatabaseActionKind::PointRead,
                target: DatabaseTarget {
                    database: None,
                    table: Some("tickets".to_string()),
                    procedure_id: None,
                },
                key: Some("ticket:t-1".to_string()),
                prefix: None,
                row_id: None,
                candidate_row: None,
                preimage: None,
                postimage: None,
                occ_read_set: vec![],
                metrics: CapabilityUseMetrics::default(),
                requested_at: Timestamp::new(700),
                metadata: BTreeMap::new(),
            },
            &DeterministicTypedRowPredicateEvaluator::default(),
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect("evaluate database access");

    assert_eq!(decision.policy.outcome.outcome, PolicyOutcomeKind::Allowed);
    assert_eq!(
        decision.effective_key.as_deref(),
        Some("tenant/tenant-a/ticket:t-1")
    );
}

#[test]
fn database_access_marks_typed_predicate_misses_as_not_visible_and_rejects_aggregate() {
    let resolved = resolve_database_policy(
        "db.query.v1",
        "tickets",
        ResourcePolicy {
            allow: vec![ResourceSelector {
                kind: ResourceKind::Table,
                pattern: "tickets".to_string(),
            }],
            deny: vec![],
            tenant_scopes: vec!["tenant-a".to_string()],
            row_scope_binding: Some(RowScopeBinding {
                binding_id: "tickets.owner".to_string(),
                policy: RowScopePolicy::TypedRowPredicate {
                    predicate_id: "ticket_owner_or_tenant".to_string(),
                    row_type: Some("ticket".to_string()),
                    referenced_fields: vec!["owner_id".to_string(), "tenant_id".to_string()],
                },
                allowed_query_shapes: vec![
                    RowQueryShape::PointRead,
                    RowQueryShape::BoundedPrefixScan,
                    RowQueryShape::WriteMutation,
                ],
                write_semantics: Default::default(),
                denial_contract: Default::default(),
                metadata: BTreeMap::new(),
            }),
            visibility_index: None,
            metadata: BTreeMap::new(),
        },
    );
    let predicates = DeterministicTypedRowPredicateEvaluator::default().with_predicate(
        "ticket_owner_or_tenant",
        DeterministicTypedRowPredicate::AnyOf {
            predicates: vec![
                DeterministicTypedRowPredicate::MatchContext {
                    row_field: "owner_id".to_string(),
                    context_field: "subject_id".to_string(),
                },
                DeterministicTypedRowPredicate::MatchContext {
                    row_field: "tenant_id".to_string(),
                    context_field: "tenant_id".to_string(),
                },
            ],
        },
    );

    let not_visible = resolved
        .evaluate_database_access(
            &DatabaseAccessRequest {
                session_id: "session-1".to_string(),
                operation: ExecutionOperation::DraftSession,
                binding_name: "tickets".to_string(),
                capability_family: None,
                action: DatabaseActionKind::PointRead,
                target: DatabaseTarget {
                    database: None,
                    table: Some("tickets".to_string()),
                    procedure_id: None,
                },
                key: Some("ticket:t-2".to_string()),
                prefix: None,
                row_id: Some("ticket:t-2".to_string()),
                candidate_row: Some(serde_json::json!({
                    "owner_id": "user:bob",
                    "tenant_id": "tenant-b"
                })),
                preimage: None,
                postimage: None,
                occ_read_set: vec![],
                metrics: CapabilityUseMetrics::default(),
                requested_at: Timestamp::new(710),
                metadata: BTreeMap::new(),
            },
            &predicates,
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect("evaluate not-visible read");
    assert_eq!(
        not_visible.policy.outcome.outcome,
        PolicyOutcomeKind::NotVisible
    );
    assert_eq!(
        not_visible
            .policy
            .outcome
            .row_visibility
            .as_ref()
            .map(|audit| audit.outcome),
        Some(RowVisibilityOutcomeKind::NotVisible)
    );

    let aggregate = resolved
        .evaluate_database_access(
            &DatabaseAccessRequest {
                session_id: "session-1".to_string(),
                operation: ExecutionOperation::DraftSession,
                binding_name: "tickets".to_string(),
                capability_family: None,
                action: DatabaseActionKind::Aggregate,
                target: DatabaseTarget {
                    database: None,
                    table: Some("tickets".to_string()),
                    procedure_id: None,
                },
                key: None,
                prefix: None,
                row_id: None,
                candidate_row: None,
                preimage: None,
                postimage: None,
                occ_read_set: vec![],
                metrics: CapabilityUseMetrics::default(),
                requested_at: Timestamp::new(711),
                metadata: BTreeMap::new(),
            },
            &predicates,
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect("evaluate aggregate denial");
    assert_eq!(aggregate.policy.outcome.outcome, PolicyOutcomeKind::Denied);
    assert!(aggregate.policy.outcome.row_visibility.is_none());
}

#[test]
fn database_access_authorizes_scan_results_row_by_row() {
    let resolved = resolve_database_policy(
        "db.query.v1",
        "tickets",
        ResourcePolicy {
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
                allowed_query_shapes: vec![RowQueryShape::BoundedPrefixScan],
                write_semantics: Default::default(),
                denial_contract: Default::default(),
                metadata: BTreeMap::new(),
            }),
            visibility_index: None,
            metadata: BTreeMap::new(),
        },
    );
    let predicates = DeterministicTypedRowPredicateEvaluator::default().with_predicate(
        "ticket_owner",
        DeterministicTypedRowPredicate::MatchContext {
            row_field: "owner_id".to_string(),
            context_field: "subject_id".to_string(),
        },
    );

    let decision = resolved
        .evaluate_database_access(
            &DatabaseAccessRequest {
                session_id: "session-1".to_string(),
                operation: ExecutionOperation::DraftSession,
                binding_name: "tickets".to_string(),
                capability_family: None,
                action: DatabaseActionKind::BoundedPrefixScan,
                target: DatabaseTarget {
                    database: None,
                    table: Some("tickets".to_string()),
                    procedure_id: None,
                },
                key: None,
                prefix: Some("ticket:".to_string()),
                row_id: None,
                candidate_row: None,
                preimage: None,
                postimage: None,
                occ_read_set: vec![],
                metrics: CapabilityUseMetrics::default(),
                requested_at: Timestamp::new(712),
                metadata: BTreeMap::new(),
            },
            &predicates,
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect("evaluate scan preflight");

    let authorized = decision
        .authorize_result_rows(
            vec![
                DatabaseResultRow {
                    row_id: "ticket:t-1".to_string(),
                    row: serde_json::json!({ "owner_id": "user:alice" }),
                },
                DatabaseResultRow {
                    row_id: "ticket:t-2".to_string(),
                    row: serde_json::json!({ "owner_id": "user:bob" }),
                },
            ],
            &predicates,
        )
        .expect("authorize result rows");

    assert_eq!(authorized.scanned_rows, 2);
    assert_eq!(authorized.returned_rows, 1);
    assert_eq!(authorized.filtered_rows, 1);
    assert_eq!(authorized.rows.len(), 1);
    assert_eq!(authorized.rows[0].row_id, "ticket:t-1");
    assert_eq!(authorized.row_audits.len(), 1);
    assert_eq!(
        authorized.row_audits[0].outcome,
        RowVisibilityOutcomeKind::NotVisible
    );
    assert_eq!(
        authorized
            .resume_token
            .as_ref()
            .map(|token| token.binding_name.as_str()),
        Some("tickets")
    );
    assert_eq!(
        authorized
            .resume_token
            .as_ref()
            .and_then(|token| token.last_primary_key.as_deref()),
        Some("ticket:t-2")
    );
}

#[test]
fn database_access_rejects_scope_escaping_writes() {
    let resolved = resolve_database_policy(
        "db.table.v1",
        "tickets",
        ResourcePolicy {
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
                allowed_query_shapes: vec![RowQueryShape::WriteMutation],
                write_semantics: Default::default(),
                denial_contract: Default::default(),
                metadata: BTreeMap::new(),
            }),
            visibility_index: None,
            metadata: BTreeMap::new(),
        },
    );
    let predicates = DeterministicTypedRowPredicateEvaluator::default().with_predicate(
        "ticket_owner",
        DeterministicTypedRowPredicate::MatchContext {
            row_field: "owner_id".to_string(),
            context_field: "subject_id".to_string(),
        },
    );

    let denied = resolved
        .evaluate_database_access(
            &DatabaseAccessRequest {
                session_id: "session-1".to_string(),
                operation: ExecutionOperation::DraftSession,
                binding_name: "tickets".to_string(),
                capability_family: None,
                action: DatabaseActionKind::WriteMutation,
                target: DatabaseTarget {
                    database: None,
                    table: Some("tickets".to_string()),
                    procedure_id: None,
                },
                key: Some("ticket:t-3".to_string()),
                prefix: None,
                row_id: Some("ticket:t-3".to_string()),
                candidate_row: None,
                preimage: Some(serde_json::json!({ "owner_id": "user:alice" })),
                postimage: Some(serde_json::json!({ "owner_id": "user:bob" })),
                occ_read_set: vec!["ticket:t-3".to_string()],
                metrics: CapabilityUseMetrics::default(),
                requested_at: Timestamp::new(720),
                metadata: BTreeMap::new(),
            },
            &predicates,
            &DeterministicVisibilityIndexStore::default(),
        )
        .expect("evaluate write denial");

    assert_eq!(denied.policy.outcome.outcome, PolicyOutcomeKind::Denied);
    assert_eq!(
        denied
            .policy
            .outcome
            .row_visibility
            .as_ref()
            .map(|audit| audit.outcome),
        Some(RowVisibilityOutcomeKind::ExplicitlyDenied)
    );
    assert!(
        denied
            .policy
            .outcome
            .message
            .as_deref()
            .is_some_and(|message| message.contains("escape"))
    );
}
