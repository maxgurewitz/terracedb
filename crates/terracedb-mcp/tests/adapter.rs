use std::{collections::BTreeMap, sync::Arc};

use serde_json::json;
use terracedb::{StubClock, Timestamp};
use terracedb_capabilities::{
    BudgetPolicy, CapabilityGrant, CapabilityManifest, CapabilityTemplate,
    DeterministicPolicyEngine, DeterministicSubjectResolver, DeterministicVisibilityIndexStore,
    ExecutionDomain, ExecutionDomainAssignment, ExecutionOperation, ExecutionPolicy,
    ManifestBinding, PolicySubject, ResourceKind, ResourcePolicy, ResourceSelector, ResourceTarget,
    SessionMode, StaticExecutionPolicyResolver, SubjectSelector, capability_module_specifier,
};
use terracedb_mcp::{
    DeterministicDraftQueryRuntime, DeterministicMcpAuthenticator, MCP_RESOURCE_PROCEDURE_AUDITS,
    MCP_RESOURCE_PROCEDURE_METADATA, MCP_RESOURCE_SANDBOX_TOOL_RUNS, MCP_RESOURCE_SANDBOX_VIEW,
    MCP_TOOL_DRAFT_QUERY_RUN, MCP_TOOL_PROCEDURES_AUDITS, MCP_TOOL_PROCEDURES_INSPECT,
    MCP_TOOL_PROCEDURES_INVOKE, MCP_TOOL_PROCEDURES_LIST, MCP_TOOL_RUNS_URI_SCHEME,
    MCP_TOOL_SANDBOX_FILE_DIFF, MCP_TOOL_SANDBOX_SESSIONS_LIST, MCP_TOOL_SANDBOX_TOOL_RUNS_LIST,
    McpAdapterBindings, McpAdapterConfig, McpAdapterError, McpAdapterService, McpAuthContext,
    McpAuthenticationRequest, McpDraftQueryRequest, McpOpenSessionRequest, McpProtocolTransport,
    McpPublishedProcedureRoute, McpResourceContents, McpToolCall, McpToolResult,
};
use terracedb_procedures::{
    DeterministicProcedurePublicationStore, DeterministicReviewedProcedureRuntime,
    ProcedureDeployment, ProcedureInvocationState, ProcedurePublicationStore, ProcedureReview,
    ProcedureVersionRef, ReviewedProcedurePublication,
};
use terracedb_sandbox::{
    LocalReadonlyViewBridge, ReadonlyViewCut, ReadonlyViewLocation, ReadonlyViewService,
    SandboxHarness, SandboxServices, SandboxSession, StaticReadonlyViewRegistry,
};
use terracedb_vfs::{CreateOptions, VolumeId};

type ViewRegistry = StaticReadonlyViewRegistry;
type ViewTransport = LocalReadonlyViewBridge<ViewRegistry>;
type Adapter = McpAdapterService<ViewTransport>;

fn sample_budget() -> BudgetPolicy {
    BudgetPolicy {
        max_calls: Some(16),
        max_scanned_rows: Some(256),
        max_returned_rows: Some(128),
        max_bytes: Some(8192),
        max_millis: Some(500),
        rate_limit_bucket: Some("mcp".to_string()),
        labels: BTreeMap::new(),
    }
}

fn sample_execution_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::OwnerForeground,
            budget: Some(sample_budget()),
            placement_tags: vec!["default".to_string()],
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
        metadata: BTreeMap::new(),
    }
}

fn subject() -> PolicySubject {
    PolicySubject {
        subject_id: "user:alice".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        groups: vec!["support".to_string()],
        attributes: BTreeMap::from([("role".to_string(), "operator".to_string())]),
    }
}

fn allow_all_policy() -> ResourcePolicy {
    ResourcePolicy {
        allow: vec![
            ResourceSelector {
                kind: ResourceKind::McpTool,
                pattern: "*".to_string(),
            },
            ResourceSelector {
                kind: ResourceKind::McpResource,
                pattern: "*".to_string(),
            },
            ResourceSelector {
                kind: ResourceKind::Procedure,
                pattern: "*".to_string(),
            },
            ResourceSelector {
                kind: ResourceKind::Session,
                pattern: "*".to_string(),
            },
            ResourceSelector {
                kind: ResourceKind::Custom,
                pattern: "*".to_string(),
            },
        ],
        deny: vec![],
        tenant_scopes: vec![],
        row_scope_binding: None,
        visibility_index: None,
        metadata: BTreeMap::new(),
    }
}

fn deployment_policy(pattern: &str) -> ResourcePolicy {
    ResourcePolicy {
        allow: vec![ResourceSelector {
            kind: ResourceKind::Procedure,
            pattern: pattern.to_string(),
        }],
        deny: vec![],
        tenant_scopes: vec![],
        row_scope_binding: None,
        visibility_index: None,
        metadata: BTreeMap::new(),
    }
}

fn template(template_id: &str, family: &str, binding_name: &str) -> CapabilityTemplate {
    CapabilityTemplate {
        template_id: template_id.to_string(),
        capability_family: family.to_string(),
        default_binding: binding_name.to_string(),
        description: Some(template_id.to_string()),
        default_resource_policy: allow_all_policy(),
        default_budget_policy: sample_budget(),
        expose_in_just_bash: false,
        metadata: BTreeMap::new(),
    }
}

fn grant(template_id: &str, binding_name: &str) -> CapabilityGrant {
    CapabilityGrant {
        grant_id: format!("grant::{template_id}"),
        subject: SubjectSelector::Exact {
            subject_id: subject().subject_id,
        },
        template_id: template_id.to_string(),
        binding_name: Some(binding_name.to_string()),
        resource_policy: None,
        budget_policy: None,
        allow_interactive_widening: false,
        metadata: BTreeMap::new(),
    }
}

fn policy_engine(include_sandbox: bool, include_draft: bool) -> DeterministicPolicyEngine {
    let mut templates = vec![template(
        "procedure.invoke.v1",
        "procedure.invoke.v1",
        "procedures",
    )];
    let mut grants = vec![grant("procedure.invoke.v1", "procedures")];
    if include_sandbox {
        templates.push(template("mcp.sandbox.v1", "mcp.sandbox.v1", "sandbox"));
        grants.push(grant("mcp.sandbox.v1", "sandbox"));
    }
    if include_draft {
        templates.push(template("db.query.v1", "db.query.v1", "draft_query"));
        grants.push(grant("db.query.v1", "draft_query"));
    }
    let policy = sample_execution_policy();
    DeterministicPolicyEngine::new(
        templates,
        grants,
        DeterministicSubjectResolver::default(),
        StaticExecutionPolicyResolver::new(policy.clone()).with_policy(SessionMode::Mcp, policy),
    )
}

fn sync_ticket_ref() -> ProcedureVersionRef {
    ProcedureVersionRef {
        procedure_id: "sync-ticket".to_string(),
        version: 1,
    }
}

fn admin_reset_ref() -> ProcedureVersionRef {
    ProcedureVersionRef {
        procedure_id: "admin-reset".to_string(),
        version: 1,
    }
}

fn publication(publication: ProcedureVersionRef) -> ReviewedProcedurePublication {
    ReviewedProcedurePublication {
        publication,
        artifact: terracedb_procedures::ProcedureImmutableArtifact::SandboxSnapshot {
            snapshot_id: "snapshot-1".to_string(),
        },
        code_hash: "sha256:abc123".to_string(),
        entrypoint: "run".to_string(),
        input_schema: json!({
            "type": "object",
            "required": ["ticketId"],
            "properties": {
                "ticketId": { "type": "string" }
            }
        }),
        output_schema: json!({
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": { "type": "string" }
            }
        }),
        published_at: Timestamp::new(42),
        manifest: CapabilityManifest {
            subject: None,
            preset_name: None,
            profile_name: None,
            bindings: vec![],
            metadata: BTreeMap::new(),
        },
        execution_policy: sample_execution_policy(),
        review: ProcedureReview {
            reviewed_by: "reviewer".to_string(),
            source_revision: "abc123".to_string(),
            note: None,
            approved_at: Timestamp::new(41),
        },
        metadata: BTreeMap::new(),
    }
}

fn deployment(pattern: &str) -> ProcedureDeployment {
    ProcedureDeployment {
        deployment_id: format!("deploy::{pattern}"),
        invocation_binding: ManifestBinding {
            binding_name: "procedures".to_string(),
            capability_family: "procedure.invoke.v1".to_string(),
            module_specifier: capability_module_specifier("procedures"),
            shell_command: None,
            resource_policy: deployment_policy(pattern),
            budget_policy: sample_budget(),
            source_template_id: "procedure.invoke.v1".to_string(),
            source_grant_id: None,
            allow_interactive_widening: false,
            metadata: BTreeMap::new(),
        },
        execution_policy: sample_execution_policy(),
        binding_overrides: vec![],
        metadata: BTreeMap::new(),
    }
}

async fn sandbox_fixture() -> (SandboxSession, ViewTransport, Arc<ViewRegistry>) {
    let harness = SandboxHarness::deterministic(200, 601, SandboxServices::deterministic());
    let base_volume_id = VolumeId::new(0x9300);
    let session_volume_id = VolumeId::new(0x9301);
    let session = harness
        .open_session(base_volume_id, session_volume_id)
        .await
        .expect("open session");
    session
        .filesystem()
        .write_file(
            "/workspace/visible.txt",
            b"visible only\n".to_vec(),
            CreateOptions {
                create_parents: true,
                overwrite: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed visible file");
    let registry = Arc::new(StaticReadonlyViewRegistry::new([session.clone()]));
    let view_service = Arc::new(ReadonlyViewService::new(registry.clone()));
    (
        session,
        view_service.local_client().transport().clone(),
        registry,
    )
}

async fn build_service(
    trusted_draft: bool,
    include_sandbox: bool,
    include_draft: bool,
) -> (Arc<Adapter>, SandboxSession) {
    let (session, readonly_transport, registry) = sandbox_fixture().await;
    let mut store = DeterministicProcedurePublicationStore::default();
    store
        .publish(publication(sync_ticket_ref()))
        .expect("publish sync-ticket");
    store
        .publish(publication(admin_reset_ref()))
        .expect("publish admin-reset");

    let procedure_runtime = DeterministicReviewedProcedureRuntime::default()
        .with_result(sync_ticket_ref(), json!({ "status": "queued" }));
    let draft_runtime =
        Arc::new(DeterministicDraftQueryRuntime::default().with_echo("draft_query"));
    let config = McpAdapterConfig {
        bindings: McpAdapterBindings::default()
            .with_procedures("procedures")
            .with_sandbox("sandbox")
            .with_draft_queries("draft_query"),
        procedures: vec![
            McpPublishedProcedureRoute::new(sync_ticket_ref(), deployment("sync-*"))
                .with_description("Sync a ticket"),
            McpPublishedProcedureRoute::new(admin_reset_ref(), deployment("sync-*"))
                .with_description("Admin reset"),
        ],
        metadata: BTreeMap::new(),
    };
    let service = Arc::new(
        McpAdapterService::new(
            Arc::new(StubClock::new(Timestamp::new(500))),
            Arc::new(DeterministicMcpAuthenticator::default().with_bearer_token(
                "secret-token",
                McpAuthContext {
                    subject: Some(subject()),
                    authentication_kind: Some("bearer".to_string()),
                    trusted_draft,
                    metadata: BTreeMap::new(),
                },
            )),
            Arc::new(policy_engine(include_sandbox, include_draft)),
            config,
            store,
            Box::new(procedure_runtime),
            Arc::new(DeterministicVisibilityIndexStore::default()),
            terracedb_sandbox::ReadonlyViewClient::new(readonly_transport),
            registry,
        )
        .with_draft_query_runtime(draft_runtime),
    );
    (service, session)
}

async fn open_session<T>(client: &terracedb_mcp::McpClient<T>) -> terracedb_mcp::McpSessionRecord
where
    T: McpProtocolTransport,
{
    client
        .open_session(McpOpenSessionRequest {
            authentication: McpAuthenticationRequest {
                session_id: "mcp-session-1".to_string(),
                stream_id: Some("stream-1".to_string()),
                headers: BTreeMap::from([(
                    "authorization".to_string(),
                    "Bearer secret-token".to_string(),
                )]),
                peer_addr: Some("127.0.0.1:9000".to_string()),
                metadata: BTreeMap::new(),
            },
            endpoint: terracedb_mcp::McpSseEndpoint {
                events_url: "https://mcp.example.invalid/events".to_string(),
                post_url: Some("https://mcp.example.invalid/messages".to_string()),
                last_event_id: Some("evt-1".to_string()),
                retry_millis: Some(1_000),
            },
            preset_name: None,
            profile_name: None,
            requested_tools: vec![],
            requested_resources: vec![],
            metadata: BTreeMap::new(),
        })
        .await
        .expect("open mcp session")
}

#[tokio::test]
async fn lower_trust_subjects_resolve_sessions_but_do_not_get_sandbox_or_draft_surfaces() {
    let (service, _session) = build_service(false, true, true).await;
    let client = service.local_client();
    let record = open_session(&client).await;

    let audit = record.audit_context.expect("audit context");
    assert_eq!(audit.subject.subject_id, "user:alice");
    assert_eq!(
        audit
            .execution_policy
            .assignment_for(ExecutionOperation::McpRequest)
            .domain,
        ExecutionDomain::RemoteWorker
    );
    let mut bindings: Vec<_> = record
        .policy
        .expect("policy")
        .manifest
        .bindings
        .into_iter()
        .map(|binding| binding.binding_name)
        .collect();
    bindings.sort();
    assert_eq!(bindings, vec!["draft_query", "procedures", "sandbox"]);

    let tool_names: Vec<_> = client
        .list_tools("stream-1")
        .await
        .expect("list tools")
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert!(tool_names.contains(&MCP_TOOL_PROCEDURES_LIST.to_string()));
    assert!(tool_names.contains(&MCP_TOOL_PROCEDURES_INSPECT.to_string()));
    assert!(tool_names.contains(&MCP_TOOL_PROCEDURES_INVOKE.to_string()));
    assert!(tool_names.contains(&MCP_TOOL_PROCEDURES_AUDITS.to_string()));
    assert!(!tool_names.contains(&MCP_TOOL_SANDBOX_SESSIONS_LIST.to_string()));
    assert!(!tool_names.contains(&MCP_TOOL_SANDBOX_FILE_DIFF.to_string()));
    assert!(!tool_names.contains(&MCP_TOOL_SANDBOX_TOOL_RUNS_LIST.to_string()));
    assert!(!tool_names.contains(&MCP_TOOL_DRAFT_QUERY_RUN.to_string()));

    let resource_names: Vec<_> = client
        .list_resources("stream-1")
        .await
        .expect("list resources")
        .into_iter()
        .map(|resource| resource.name)
        .collect();
    assert!(resource_names.contains(&MCP_RESOURCE_PROCEDURE_METADATA.to_string()));
    assert!(resource_names.contains(&MCP_RESOURCE_PROCEDURE_AUDITS.to_string()));
    assert!(!resource_names.contains(&MCP_RESOURCE_SANDBOX_VIEW.to_string()));
    assert!(!resource_names.contains(&MCP_RESOURCE_SANDBOX_TOOL_RUNS.to_string()));

    let sandbox_error = client
        .call_tool("stream-1", McpToolCall::ListSandboxSessions)
        .await
        .expect_err("sandbox surface should require trusted draft");
    assert!(matches!(
        sandbox_error,
        McpAdapterError::DraftTrustRequired { .. }
    ));

    let draft_error = client
        .call_tool(
            "stream-1",
            McpToolCall::RunDraftQuery {
                request: McpDraftQueryRequest {
                    query: json!({ "sql": "select 1" }),
                    target_resources: vec![ResourceTarget {
                        kind: ResourceKind::Custom,
                        identifier: "draft.query".to_string(),
                    }],
                    ..Default::default()
                },
            },
        )
        .await
        .expect_err("draft queries should require trusted draft");
    assert!(matches!(
        draft_error,
        McpAdapterError::DraftTrustRequired { .. }
    ));
}

#[tokio::test]
async fn procedure_invocations_reuse_reviewed_runtime_and_fail_closed_on_deployment_denials() {
    let (service, _session) = build_service(false, false, false).await;
    let client = service.local_client();
    open_session(&client).await;

    let sync_receipt = client
        .call_tool(
            "stream-1",
            McpToolCall::InvokeProcedure {
                publication: sync_ticket_ref(),
                arguments: json!({ "ticketId": "t-1" }),
            },
        )
        .await
        .expect("invoke sync-ticket");
    let McpToolResult::Invocation { receipt } = sync_receipt.result else {
        panic!("expected invocation result");
    };
    assert_eq!(receipt.output, json!({ "status": "queued" }));

    let sync_detail = client
        .call_tool(
            "stream-1",
            McpToolCall::InspectProcedure {
                publication: sync_ticket_ref(),
            },
        )
        .await
        .expect("inspect sync-ticket");
    let McpToolResult::Procedure { detail } = sync_detail.result else {
        panic!("expected procedure detail");
    };
    assert_eq!(detail.invocations.len(), 1);
    assert_eq!(
        detail.invocations[0].state,
        ProcedureInvocationState::Succeeded
    );

    let denied = client
        .call_tool(
            "stream-1",
            McpToolCall::InvokeProcedure {
                publication: admin_reset_ref(),
                arguments: json!({ "ticketId": "t-1" }),
            },
        )
        .await
        .expect_err("admin-reset should be denied by reviewed deployment policy");
    let McpAdapterError::Procedure { message } = denied else {
        panic!("expected procedure error");
    };
    assert!(
        message.contains("was denied"),
        "unexpected message: {message}"
    );

    let denied_detail = client
        .call_tool(
            "stream-1",
            McpToolCall::InspectProcedure {
                publication: admin_reset_ref(),
            },
        )
        .await
        .expect("inspect denied procedure");
    let McpToolResult::Procedure { detail } = denied_detail.result else {
        panic!("expected procedure detail");
    };
    assert_eq!(detail.invocations.len(), 1);
    assert_eq!(
        detail.invocations[0].state,
        ProcedureInvocationState::Denied
    );
}

#[tokio::test]
async fn trusted_sessions_reuse_readonly_view_protocol_and_publish_tool_run_history() {
    let (service, session) = build_service(true, true, false).await;
    let client = service.local_client();
    open_session(&client).await;
    let session_volume_id = session.info().await.session_volume_id;

    let visible_uri = ReadonlyViewLocation {
        session_volume_id,
        cut: ReadonlyViewCut::Visible,
        path: "/workspace/visible.txt".to_string(),
    }
    .to_uri();
    let visible = client
        .read_resource("stream-1", visible_uri)
        .await
        .expect("read visible resource");
    match visible.contents {
        McpResourceContents::Bytes { bytes, .. } => assert_eq!(bytes, b"visible only\n".to_vec()),
        other => panic!("expected bytes, got {other:?}"),
    }

    let diff = client
        .call_tool(
            "stream-1",
            McpToolCall::DiffSandboxFile {
                session_volume_id,
                path: "/workspace/visible.txt".to_string(),
            },
        )
        .await
        .expect("diff sandbox file");
    let McpToolResult::SandboxFileDiff { diff } = diff.result else {
        panic!("expected diff result");
    };
    assert!(diff.changed);
    assert_eq!(diff.visible, Some(b"visible only\n".to_vec()));
    assert_eq!(diff.durable, None);

    let sessions = client
        .call_tool("stream-1", McpToolCall::ListSandboxSessions)
        .await
        .expect("list sandbox sessions");
    let McpToolResult::SandboxSessions { sessions } = sessions.result else {
        panic!("expected sandbox sessions");
    };
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_volume_id, session_volume_id);

    let tool_runs_uri = format!("{MCP_TOOL_RUNS_URI_SCHEME}://session/{session_volume_id}");
    let tool_runs = client
        .read_resource("stream-1", tool_runs_uri)
        .await
        .expect("read tool runs");
    let McpResourceContents::Json { value, .. } = tool_runs.contents else {
        panic!("expected json tool runs");
    };
    let run_names: Vec<_> = value
        .as_array()
        .expect("tool runs array")
        .iter()
        .filter_map(|run| run.get("name").and_then(|name| name.as_str()))
        .collect();
    assert!(run_names.contains(&"sandbox.session.open"));

    session.flush().await.expect("flush session");
    let durable_uri = ReadonlyViewLocation {
        session_volume_id,
        cut: ReadonlyViewCut::Durable,
        path: "/workspace/visible.txt".to_string(),
    }
    .to_uri();
    let durable = client
        .read_resource("stream-1", durable_uri)
        .await
        .expect("read durable resource");
    match durable.contents {
        McpResourceContents::Bytes { bytes, .. } => assert_eq!(bytes, b"visible only\n".to_vec()),
        other => panic!("expected bytes, got {other:?}"),
    }
}

#[tokio::test]
async fn trusted_subjects_need_explicit_draft_binding_and_can_run_draft_queries_when_granted() {
    let (service_without_binding, _session) = build_service(true, false, false).await;
    let client_without_binding = service_without_binding.local_client();
    open_session(&client_without_binding).await;
    let tool_names: Vec<_> = client_without_binding
        .list_tools("stream-1")
        .await
        .expect("list tools")
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert!(!tool_names.contains(&MCP_TOOL_DRAFT_QUERY_RUN.to_string()));
    let ungranted = client_without_binding
        .call_tool(
            "stream-1",
            McpToolCall::RunDraftQuery {
                request: McpDraftQueryRequest {
                    query: json!({ "sql": "select 1" }),
                    target_resources: vec![ResourceTarget {
                        kind: ResourceKind::Custom,
                        identifier: "draft.query".to_string(),
                    }],
                    ..Default::default()
                },
            },
        )
        .await
        .expect_err("draft query should fail without explicit binding");
    assert!(matches!(ungranted, McpAdapterError::ToolForbidden { .. }));

    let (service_with_binding, _session) = build_service(true, false, true).await;
    let client_with_binding = service_with_binding.local_client();
    open_session(&client_with_binding).await;
    let tool_names: Vec<_> = client_with_binding
        .list_tools("stream-1")
        .await
        .expect("list tools")
        .into_iter()
        .map(|tool| tool.name)
        .collect();
    assert!(tool_names.contains(&MCP_TOOL_DRAFT_QUERY_RUN.to_string()));

    let draft_query = client_with_binding
        .call_tool(
            "stream-1",
            McpToolCall::RunDraftQuery {
                request: McpDraftQueryRequest {
                    query: json!({ "sql": "select 1" }),
                    target_resources: vec![ResourceTarget {
                        kind: ResourceKind::Custom,
                        identifier: "draft.query".to_string(),
                    }],
                    ..Default::default()
                },
            },
        )
        .await
        .expect("run draft query");
    let McpToolResult::DraftQuery { output } = draft_query.result else {
        panic!("expected draft query output");
    };
    assert_eq!(output.result, json!({ "sql": "select 1" }));
}
