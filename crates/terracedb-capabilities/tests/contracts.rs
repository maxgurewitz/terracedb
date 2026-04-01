use std::collections::BTreeMap;

use terracedb::Timestamp;
use terracedb_capabilities::{
    AuthorizationScope, BudgetPolicy, CapabilityGrant, CapabilityManifest,
    CapabilityPresetDescriptor, CapabilityProfileDescriptor, CapabilityTemplate,
    CapabilityUseMetrics, CapabilityUseRequest, DeterministicPolicyEngine,
    DeterministicRateLimiter, DeterministicSubjectResolver, DraftAuthorizationDecision,
    DraftAuthorizationOutcomeKind, DraftAuthorizationRequestKind, ExecutionDomain,
    ExecutionDomainAssignment, ExecutionOperation, ExecutionPolicy,
    ForegroundSessionStatusProjector, ManifestBinding, PolicyOutcomeKind, PolicyResolutionRequest,
    PolicySubject, PresetBinding, RateLimitOutcome, ResolvedSessionPolicy, ResourceKind,
    ResourcePolicy, ResourceSelector, ResourceTarget, SessionLifecycleState, SessionMode,
    SessionStatusSource, SessionStatusUpdate, ShellCommandDescriptor,
    StaticExecutionPolicyResolver, SubjectResolutionRequest, SubjectSelector,
    capability_module_specifier,
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
    DeterministicProcedurePublicationStore, ProcedureInvocationContext, ProcedureInvocationRequest,
    ProcedurePublicationStore, ProcedureReview, ProcedureVersionRef, ReviewedProcedurePublication,
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
            row_scope_binding: Some("tenant_id".to_string()),
            visibility_index: Some("visible_by_subject".to_string()),
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

fn resolve_sample_policy(rate_limiter: DeterministicRateLimiter) -> ResolvedSessionPolicy {
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
    .resolve(&sample_resolution_request(Some("foreground")))
    .expect("resolve sample policy")
}

#[test]
fn frozen_contracts_compile_together() {
    let template = sample_template();
    let grant = sample_grant();
    let execution_policy = sample_execution_policy();
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
        entrypoint: "terrace:/workspace/procedures/sync-ticket.ts".to_string(),
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
    );
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
        entrypoint: "terrace:/workspace/procedures/sync-ticket.ts".to_string(),
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
