use std::collections::BTreeMap;

use terracedb::{FieldDefinition, FieldId, FieldType, FieldValue, SchemaDefinition, Timestamp};
use terracedb_capabilities::{
    BudgetPolicy, CapabilityManifest, ExecutionDomain, ExecutionDomainAssignment,
    ExecutionOperation, ExecutionPolicy, ManifestBinding, ResourceKind, ResourcePolicy,
    ResourceSelector, capability_module_specifier,
};
use terracedb_migrate::{
    CheckPreconditionRequest, DeterministicMigrationCatalog, DeterministicMigrationStore,
    DeterministicReviewedMigrationRuntime, EnsureTableRequest, InstallSchemaSuccessorRequest,
    MigrationError, MigrationModuleAction, MigrationPlan, MigrationPlanRef, MigrationPrecondition,
    MigrationReview, MigrationState, MigrationStep, MigrationStepKind, PendingMigrationStep,
    PreconditionFailurePolicy, ReviewedMigrationPlan, UpdateTableMetadataRequest,
    apply_published_plan, begin_reviewed_plan_publication, publish_reviewed_plan,
};

fn control_plane_policy() -> ExecutionPolicy {
    ExecutionPolicy {
        default_assignment: ExecutionDomainAssignment {
            domain: ExecutionDomain::SharedBackground,
            budget: Some(BudgetPolicy::default()),
            placement_tags: vec!["control-plane".to_string()],
            metadata: BTreeMap::new(),
        },
        operations: BTreeMap::from([
            (
                ExecutionOperation::MigrationPublication,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: Some(BudgetPolicy::default()),
                    placement_tags: vec!["control-plane".to_string(), "publish".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
            (
                ExecutionOperation::MigrationApply,
                ExecutionDomainAssignment {
                    domain: ExecutionDomain::DedicatedSandbox,
                    budget: Some(BudgetPolicy::default()),
                    placement_tags: vec!["control-plane".to_string(), "apply".to_string()],
                    metadata: BTreeMap::new(),
                },
            ),
        ]),
        metadata: BTreeMap::new(),
    }
}

fn catalog_manifest(table_pattern: &str) -> CapabilityManifest {
    CapabilityManifest {
        subject: None,
        preset_name: Some("migrations".to_string()),
        profile_name: Some("control-plane".to_string()),
        bindings: vec![ManifestBinding {
            binding_name: "catalog".to_string(),
            capability_family: "catalog.migrate.v1".to_string(),
            module_specifier: capability_module_specifier("catalog"),
            shell_command: None,
            resource_policy: ResourcePolicy {
                allow: vec![ResourceSelector {
                    kind: ResourceKind::Table,
                    pattern: table_pattern.to_string(),
                }],
                ..Default::default()
            },
            budget_policy: BudgetPolicy::default(),
            source_template_id: "catalog.migrate.v1".to_string(),
            source_grant_id: Some("grant:migrations".to_string()),
            allow_interactive_widening: false,
            metadata: BTreeMap::new(),
        }],
        metadata: BTreeMap::new(),
    }
}

fn schema_v1() -> SchemaDefinition {
    SchemaDefinition {
        version: 1,
        fields: vec![
            FieldDefinition {
                id: FieldId::new(1),
                name: "ticket_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: FieldId::new(2),
                name: "status".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
        ],
    }
}

fn schema_v2() -> SchemaDefinition {
    SchemaDefinition {
        version: 2,
        fields: vec![
            FieldDefinition {
                id: FieldId::new(1),
                name: "ticket_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: FieldId::new(2),
                name: "status".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: FieldId::new(3),
                name: "priority".to_string(),
                field_type: FieldType::Int64,
                nullable: false,
                default: Some(FieldValue::Int64(0)),
            },
        ],
    }
}

fn invalid_successor_schema() -> SchemaDefinition {
    SchemaDefinition {
        version: 2,
        fields: vec![
            FieldDefinition {
                id: FieldId::new(1),
                name: "ticket_id".to_string(),
                field_type: FieldType::String,
                nullable: false,
                default: None,
            },
            FieldDefinition {
                id: FieldId::new(2),
                name: "status".to_string(),
                field_type: FieldType::Bool,
                nullable: false,
                default: None,
            },
        ],
    }
}

fn reviewed_plan(steps: Vec<MigrationStep>) -> ReviewedMigrationPlan {
    ReviewedMigrationPlan {
        plan: MigrationPlan {
            plan_id: "plan-1".to_string(),
            application_id: "notes-app".to_string(),
            created_at: Timestamp::new(10),
            requested_manifest: catalog_manifest("tickets"),
            execution_policy: control_plane_policy(),
            steps,
            metadata: BTreeMap::new(),
        },
        review: MigrationReview {
            reviewed_by: "reviewer".to_string(),
            source_revision: "abc123".to_string(),
            note: Some("approved".to_string()),
            approved_at: Timestamp::new(11),
        },
        published_at: Timestamp::new(12),
        metadata: BTreeMap::new(),
    }
}

fn plan_ref(plan: &ReviewedMigrationPlan) -> MigrationPlanRef {
    MigrationPlanRef::from(plan)
}

#[test]
fn publish_and_apply_are_ordered_and_idempotent() {
    let reviewed = reviewed_plan(vec![
        MigrationStep {
            step_id: "01".to_string(),
            label: "create tickets".to_string(),
            module_specifier: "terrace:/workspace/migrations/01-create.ts".to_string(),
            checksum: "sha256:01".to_string(),
            kind: MigrationStepKind::CatalogBootstrap,
            requested_bindings: vec!["catalog".to_string()],
            metadata: BTreeMap::new(),
        },
        MigrationStep {
            step_id: "02".to_string(),
            label: "upgrade tickets".to_string(),
            module_specifier: "terrace:/workspace/migrations/02-upgrade.ts".to_string(),
            checksum: "sha256:02".to_string(),
            kind: MigrationStepKind::SchemaChange,
            requested_bindings: vec!["catalog".to_string()],
            metadata: BTreeMap::new(),
        },
    ]);
    let reference = plan_ref(&reviewed);

    let mut store = DeterministicMigrationStore::default();
    let publication = publish_reviewed_plan(&mut store, reviewed.clone()).expect("publish");
    assert_eq!(publication.plan, reference);

    let mut runtime = DeterministicReviewedMigrationRuntime::default()
        .with_actions(
            "terrace:/workspace/migrations/01-create.ts",
            vec![MigrationModuleAction::ensure_table(
                "catalog",
                EnsureTableRequest {
                    table: "tickets".to_string(),
                    database: None,
                    format: None,
                    schema: Some(schema_v1()),
                },
            )],
        )
        .with_actions(
            "terrace:/workspace/migrations/02-upgrade.ts",
            vec![
                MigrationModuleAction::install_schema_successor(
                    "catalog",
                    InstallSchemaSuccessorRequest {
                        table: "tickets".to_string(),
                        database: None,
                        successor: schema_v2(),
                    },
                ),
                MigrationModuleAction::update_table_metadata(
                    "catalog",
                    UpdateTableMetadataRequest {
                        table: "tickets".to_string(),
                        database: None,
                        metadata: BTreeMap::from([(
                            "owner".to_string(),
                            serde_json::json!("support"),
                        )]),
                    },
                ),
            ],
        );
    let mut catalog = DeterministicMigrationCatalog::default();

    let first = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(20),
    )
    .expect("apply should succeed");
    assert_eq!(
        first.applied_steps,
        vec!["01".to_string(), "02".to_string()]
    );
    assert!(first.skipped_steps.is_empty());
    assert!(first.noop_steps.is_empty());

    let history = store.history_for_plan(&reference);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].step_id, "01");
    assert_eq!(history[0].state, MigrationState::Applied);
    assert_eq!(history[1].step_id, "02");
    assert_eq!(history[1].state, MigrationState::Applied);
    assert_eq!(
        runtime.invocation_count("terrace:/workspace/migrations/01-create.ts"),
        1
    );
    assert_eq!(
        runtime.invocation_count("terrace:/workspace/migrations/02-upgrade.ts"),
        1
    );

    let table = catalog
        .load_table(None, "tickets")
        .expect("tickets table should exist");
    assert_eq!(table.schema.expect("schema").version, 2);
    assert_eq!(table.metadata["owner"], serde_json::json!("support"));

    let second = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(21),
    )
    .expect("reapply should noop");
    assert!(second.applied_steps.is_empty());
    assert!(second.skipped_steps.is_empty());
    assert_eq!(second.noop_steps, vec!["01".to_string(), "02".to_string()]);
    assert_eq!(store.history_for_plan(&reference).len(), 2);
    assert_eq!(
        runtime.invocation_count("terrace:/workspace/migrations/01-create.ts"),
        1
    );
    assert_eq!(
        runtime.invocation_count("terrace:/workspace/migrations/02-upgrade.ts"),
        1
    );

    let audits = store.audits_for_plan(&reference);
    assert_eq!(audits.len(), 7);
}

#[test]
fn precondition_skip_records_skipped_history() {
    let reviewed = reviewed_plan(vec![MigrationStep {
        step_id: "01".to_string(),
        label: "skip if table already exists".to_string(),
        module_specifier: "terrace:/workspace/migrations/01-skip.ts".to_string(),
        checksum: "sha256:01".to_string(),
        kind: MigrationStepKind::SchemaChange,
        requested_bindings: vec!["catalog".to_string()],
        metadata: BTreeMap::new(),
    }]);
    let reference = plan_ref(&reviewed);

    let mut store = DeterministicMigrationStore::default();
    publish_reviewed_plan(&mut store, reviewed).expect("publish");

    let mut catalog = DeterministicMigrationCatalog::default();
    catalog
        .ensure_table(EnsureTableRequest {
            table: "tickets".to_string(),
            database: None,
            format: None,
            schema: Some(schema_v1()),
        })
        .expect("seed table");

    let mut runtime = DeterministicReviewedMigrationRuntime::default().with_actions(
        "terrace:/workspace/migrations/01-skip.ts",
        vec![
            MigrationModuleAction::require_precondition(
                "catalog",
                CheckPreconditionRequest {
                    table: Some("tickets".to_string()),
                    database: None,
                    precondition: MigrationPrecondition::TableMissing,
                },
                PreconditionFailurePolicy::SkipStep,
            ),
            MigrationModuleAction::update_table_metadata(
                "catalog",
                UpdateTableMetadataRequest {
                    table: "tickets".to_string(),
                    database: None,
                    metadata: BTreeMap::from([(
                        "owner".to_string(),
                        serde_json::json!("should-not-apply"),
                    )]),
                },
            ),
        ],
    );

    let receipt = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(30),
    )
    .expect("apply should skip");
    assert!(receipt.applied_steps.is_empty());
    assert_eq!(receipt.skipped_steps, vec!["01".to_string()]);

    let history = store.history_for_plan(&reference);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, MigrationState::Skipped);
    assert!(
        catalog
            .load_table(None, "tickets")
            .expect("table")
            .metadata
            .is_empty()
    );
}

#[test]
fn binding_escape_is_denied_and_records_failure() {
    let reviewed = reviewed_plan(vec![MigrationStep {
        step_id: "01".to_string(),
        label: "attempt to escape".to_string(),
        module_specifier: "terrace:/workspace/migrations/01-escape.ts".to_string(),
        checksum: "sha256:01".to_string(),
        kind: MigrationStepKind::CatalogBootstrap,
        requested_bindings: vec!["catalog".to_string()],
        metadata: BTreeMap::new(),
    }]);
    let reference = plan_ref(&reviewed);

    let mut store = DeterministicMigrationStore::default();
    publish_reviewed_plan(&mut store, reviewed).expect("publish");

    let mut runtime = DeterministicReviewedMigrationRuntime::default().with_actions(
        "terrace:/workspace/migrations/01-escape.ts",
        vec![MigrationModuleAction::ensure_table(
            "catalog",
            EnsureTableRequest {
                table: "admins".to_string(),
                database: None,
                format: None,
                schema: Some(schema_v1()),
            },
        )],
    );
    let mut catalog = DeterministicMigrationCatalog::default();

    let error = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(40),
    )
    .expect_err("authority escape should be denied");
    assert!(matches!(error, MigrationError::BindingTargetDenied { .. }));

    let history = store.history_for_plan(&reference);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, MigrationState::Failed);
    assert!(catalog.load_table(None, "admins").is_none());
}

#[test]
fn invalid_schema_successor_fails_and_replays_closed() {
    let reviewed = reviewed_plan(vec![
        MigrationStep {
            step_id: "01".to_string(),
            label: "create table".to_string(),
            module_specifier: "terrace:/workspace/migrations/01-create.ts".to_string(),
            checksum: "sha256:01".to_string(),
            kind: MigrationStepKind::CatalogBootstrap,
            requested_bindings: vec!["catalog".to_string()],
            metadata: BTreeMap::new(),
        },
        MigrationStep {
            step_id: "02".to_string(),
            label: "invalid successor".to_string(),
            module_specifier: "terrace:/workspace/migrations/02-invalid.ts".to_string(),
            checksum: "sha256:02".to_string(),
            kind: MigrationStepKind::SchemaChange,
            requested_bindings: vec!["catalog".to_string()],
            metadata: BTreeMap::new(),
        },
    ]);
    let reference = plan_ref(&reviewed);

    let mut store = DeterministicMigrationStore::default();
    publish_reviewed_plan(&mut store, reviewed).expect("publish");

    let mut runtime = DeterministicReviewedMigrationRuntime::default()
        .with_actions(
            "terrace:/workspace/migrations/01-create.ts",
            vec![MigrationModuleAction::ensure_table(
                "catalog",
                EnsureTableRequest {
                    table: "tickets".to_string(),
                    database: None,
                    format: None,
                    schema: Some(schema_v1()),
                },
            )],
        )
        .with_actions(
            "terrace:/workspace/migrations/02-invalid.ts",
            vec![MigrationModuleAction::install_schema_successor(
                "catalog",
                InstallSchemaSuccessorRequest {
                    table: "tickets".to_string(),
                    database: None,
                    successor: invalid_successor_schema(),
                },
            )],
        );
    let mut catalog = DeterministicMigrationCatalog::default();

    let error = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(50),
    )
    .expect_err("invalid successor should fail");
    assert!(matches!(error, MigrationError::SchemaValidation { .. }));

    let history = store.history_for_plan(&reference);
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].state, MigrationState::Applied);
    assert_eq!(history[1].state, MigrationState::Failed);
    assert_eq!(
        catalog
            .load_table(None, "tickets")
            .expect("table should exist")
            .schema
            .expect("schema")
            .version,
        1
    );

    let replay_error = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(51),
    )
    .expect_err("failed steps should not replay");
    assert!(matches!(
        replay_error,
        MigrationError::StepPreviouslyFailed { .. }
    ));
}

#[test]
fn failed_multi_action_step_rolls_back_catalog_changes() {
    let reviewed = reviewed_plan(vec![MigrationStep {
        step_id: "01".to_string(),
        label: "create then fail".to_string(),
        module_specifier: "terrace:/workspace/migrations/01-partial.ts".to_string(),
        checksum: "sha256:01".to_string(),
        kind: MigrationStepKind::CatalogBootstrap,
        requested_bindings: vec!["catalog".to_string()],
        metadata: BTreeMap::new(),
    }]);
    let reference = plan_ref(&reviewed);

    let mut store = DeterministicMigrationStore::default();
    publish_reviewed_plan(&mut store, reviewed).expect("publish");

    let mut runtime = DeterministicReviewedMigrationRuntime::default().with_actions(
        "terrace:/workspace/migrations/01-partial.ts",
        vec![
            MigrationModuleAction::ensure_table(
                "catalog",
                EnsureTableRequest {
                    table: "tickets".to_string(),
                    database: None,
                    format: None,
                    schema: Some(schema_v1()),
                },
            ),
            MigrationModuleAction::require_precondition(
                "catalog",
                CheckPreconditionRequest {
                    table: Some("tickets".to_string()),
                    database: None,
                    precondition: MigrationPrecondition::MetadataEquals {
                        key: "owner".to_string(),
                        value: serde_json::json!("support"),
                    },
                },
                PreconditionFailurePolicy::FailStep,
            ),
        ],
    );
    let mut catalog = DeterministicMigrationCatalog::default();

    let error = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(60),
    )
    .expect_err("step should fail");
    assert!(matches!(error, MigrationError::PreconditionFailed { .. }));
    assert!(catalog.load_table(None, "tickets").is_none());

    let history = store.history_for_plan(&reference);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, MigrationState::Failed);
}

#[test]
fn incomplete_publication_and_apply_fail_closed() {
    let reviewed = reviewed_plan(vec![MigrationStep {
        step_id: "01".to_string(),
        label: "create table".to_string(),
        module_specifier: "terrace:/workspace/migrations/01-create.ts".to_string(),
        checksum: "sha256:01".to_string(),
        kind: MigrationStepKind::CatalogBootstrap,
        requested_bindings: vec!["catalog".to_string()],
        metadata: BTreeMap::new(),
    }]);
    let reference = plan_ref(&reviewed);

    let mut pending_store = DeterministicMigrationStore::default();
    let pending_reference =
        begin_reviewed_plan_publication(&mut pending_store, reviewed.clone()).expect("begin");
    let pending_apply_error = apply_published_plan(
        &mut pending_store,
        &mut DeterministicReviewedMigrationRuntime::default(),
        &mut DeterministicMigrationCatalog::default(),
        &pending_reference,
        Timestamp::new(13),
    )
    .expect_err("incomplete publication should fail closed");
    assert!(matches!(
        pending_apply_error,
        MigrationError::PendingPublication { .. }
    ));
    let publication_error =
        publish_reviewed_plan(&mut pending_store, reviewed.clone()).expect_err("fail closed");
    assert!(matches!(
        publication_error,
        MigrationError::PendingPublication { .. }
    ));

    let mut store = DeterministicMigrationStore::default();
    publish_reviewed_plan(&mut store, reviewed.clone()).expect("publish");
    store.record_incomplete_apply(PendingMigrationStep {
        plan: reference.clone(),
        step_id: "01".to_string(),
        module_specifier: "terrace:/workspace/migrations/01-create.ts".to_string(),
        recorded_at: Timestamp::new(13),
    });

    let mut runtime = DeterministicReviewedMigrationRuntime::default().with_actions(
        "terrace:/workspace/migrations/01-create.ts",
        vec![MigrationModuleAction::ensure_table(
            "catalog",
            EnsureTableRequest {
                table: "tickets".to_string(),
                database: None,
                format: None,
                schema: Some(schema_v1()),
            },
        )],
    );
    let mut catalog = DeterministicMigrationCatalog::default();
    let apply_error = apply_published_plan(
        &mut store,
        &mut runtime,
        &mut catalog,
        &reference,
        Timestamp::new(14),
    )
    .expect_err("incomplete apply metadata should fail closed");
    assert!(matches!(apply_error, MigrationError::PendingApply { .. }));
}
