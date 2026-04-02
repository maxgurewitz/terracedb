use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use terracedb::{
    CommitOptions, Db, KvStream, LogCursor, ScanOptions, SequenceNumber, StubFileSystem,
    StubObjectStore, Table, TableConfig, TieredDurabilityMode, Value,
    test_support::{row_table_config, test_dependencies, tiered_test_config},
};
use terracedb_capabilities::{PolicyContext, VisibilityIndexSpec, VisibilityIndexSubjectKey};
use terracedb_projections::{
    ProjectionHandle, ProjectionHandlerError, ProjectionRuntime, VisibilityGrant,
    VisibilityIndexProjection, VisibilityProjectionTransition, VisibilitySourceProvenance,
    decode_visibility_index_entry_key, visibility_index_entry_key, visibility_index_lookup_range,
    visibility_projection_transitions, visibility_scan_plans,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ticket {
    owner_id: String,
    tenant_id: String,
    body: String,
    redacted_body: String,
}

fn retained_row_table_config(name: &str, retained: u64) -> TableConfig {
    let mut config = row_table_config(name);
    config.history_retention_sequences = Some(retained);
    config
}

fn ticket_value(owner_id: &str, tenant_id: &str, body: &str, redacted_body: &str) -> Value {
    Value::bytes(format!("{owner_id}|{tenant_id}|{body}|{redacted_body}"))
}

fn decode_bytes(value: Value, expected: &str) -> Result<String, ProjectionHandlerError> {
    let Value::Bytes(bytes) = value else {
        return Err(ProjectionHandlerError::new(std::io::Error::other(format!(
            "{expected} must use byte values"
        ))));
    };
    String::from_utf8(bytes).map_err(ProjectionHandlerError::new)
}

fn decode_ticket(value: Value) -> Result<Ticket, ProjectionHandlerError> {
    let raw = decode_bytes(value, "ticket rows")?;
    let mut fields = raw.splitn(4, '|');
    let owner_id = fields
        .next()
        .ok_or_else(|| ProjectionHandlerError::new(std::io::Error::other("missing owner_id")))?;
    let tenant_id = fields
        .next()
        .ok_or_else(|| ProjectionHandlerError::new(std::io::Error::other("missing tenant_id")))?;
    let body = fields
        .next()
        .ok_or_else(|| ProjectionHandlerError::new(std::io::Error::other("missing body")))?;
    let redacted_body = fields.next().ok_or_else(|| {
        ProjectionHandlerError::new(std::io::Error::other("missing redacted body"))
    })?;
    Ok(Ticket {
        owner_id: owner_id.to_string(),
        tenant_id: tenant_id.to_string(),
        body: body.to_string(),
        redacted_body: redacted_body.to_string(),
    })
}

fn fold_ticket_source(
    state: &mut BTreeMap<String, Ticket>,
    key: Vec<u8>,
    value: Value,
) -> Result<(), ProjectionHandlerError> {
    let row_id = String::from_utf8(key).map_err(ProjectionHandlerError::new)?;
    state.insert(row_id, decode_ticket(value)?);
    Ok(())
}

fn fold_share_source(
    state: &mut BTreeMap<String, BTreeSet<String>>,
    key: Vec<u8>,
    _value: Value,
) -> Result<(), ProjectionHandlerError> {
    let raw = String::from_utf8(key).map_err(ProjectionHandlerError::new)?;
    let (row_id, subject_id) = raw.split_once('/').ok_or_else(|| {
        ProjectionHandlerError::new(std::io::Error::other(
            "share rows must use the key format <row_id>/<subject_id>",
        ))
    })?;
    state
        .entry(row_id.to_string())
        .or_default()
        .insert(subject_id.to_string());
    Ok(())
}

fn expand_ticket_visibility(
    tickets: &BTreeMap<String, Ticket>,
    shares: &BTreeMap<String, BTreeSet<String>>,
) -> Result<Vec<VisibilityGrant>, ProjectionHandlerError> {
    let mut grants = Vec::new();
    for (row_id, ticket) in tickets {
        grants.push(VisibilityGrant {
            lookup_key: ticket.owner_id.clone(),
            row_id: row_id.clone(),
            read_mirror: Some(Value::bytes(format!("full:{}", ticket.body))),
        });
        if let Some(shared_subjects) = shares.get(row_id) {
            for subject_id in shared_subjects {
                grants.push(VisibilityGrant {
                    lookup_key: subject_id.clone(),
                    row_id: row_id.clone(),
                    read_mirror: Some(Value::bytes(format!("redacted:{}", ticket.redacted_body))),
                });
            }
        }
    }
    Ok(grants)
}

fn subject_visibility_spec(index_table: &str, read_mirror_table: &str) -> VisibilityIndexSpec {
    VisibilityIndexSpec {
        index_name: "visible_by_subject".to_string(),
        index_table: index_table.to_string(),
        subject_key: VisibilityIndexSubjectKey::Subject,
        row_id_field: "ticket_id".to_string(),
        membership_source: Some("shares".to_string()),
        read_mirror_table: Some(read_mirror_table.to_string()),
        authoritative_sources: vec!["shares".to_string(), "tickets".to_string()],
        metadata: BTreeMap::new(),
    }
}

fn build_subject_visibility_projection(
    spec: VisibilityIndexSpec,
    tickets: &Table,
    shares: &Table,
    index: &Table,
    read_mirror: &Table,
) -> VisibilityIndexProjection<BTreeMap<String, Ticket>, BTreeMap<String, BTreeSet<String>>> {
    VisibilityIndexProjection::new(
        spec,
        tickets.clone(),
        VisibilitySourceProvenance::AuthoritativeCurrentState,
        index.clone(),
        fold_ticket_source,
        expand_ticket_visibility,
    )
    .expect("build subject visibility projection")
    .with_membership_source(
        shares.clone(),
        VisibilitySourceProvenance::AuthoritativeCurrentState,
        fold_share_source,
    )
    .expect("attach share source")
    .with_read_mirror_table(read_mirror.clone())
    .expect("attach read mirror table")
}

async fn wait_for_sources<'a, I>(handle: &mut ProjectionHandle, targets: I)
where
    I: IntoIterator<Item = (&'a Table, SequenceNumber)>,
{
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_sources(targets))
        .await
        .expect("projection should settle before timeout")
        .expect("projection should reach the requested source frontiers");
}

async fn collect_rows(stream: KvStream) -> Vec<(Vec<u8>, Value)> {
    stream.collect().await
}

async fn collect_index_rows(table: &Table) -> Vec<(String, String)> {
    let mut rows = collect_rows(
        table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan index rows"),
    )
    .await
    .into_iter()
    .map(|(key, _value)| decode_visibility_index_entry_key(&key).expect("decode visibility key"))
    .collect::<Vec<_>>();
    rows.sort();
    rows
}

async fn collect_read_mirrors(table: &Table) -> Vec<((String, String), String)> {
    let mut rows = collect_rows(
        table
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan read mirrors"),
    )
    .await
    .into_iter()
    .map(|(key, value)| {
        (
            decode_visibility_index_entry_key(&key).expect("decode mirror key"),
            decode_bytes(value, "read mirror rows").expect("decode mirror payload"),
        )
    })
    .collect::<Vec<_>>();
    rows.sort();
    rows
}

#[test]
fn visibility_transition_diff_covers_membership_and_redaction_changes() {
    let mut spec = subject_visibility_spec("visible_by_subject", "visible_ticket_reads");
    spec.metadata
        .insert("helper".to_string(), serde_json::json!("tickets"));
    let transitions = visibility_projection_transitions(
        &spec,
        [
            VisibilityGrant {
                lookup_key: "user:alice".to_string(),
                row_id: "ticket:t-1".to_string(),
                read_mirror: Some(Value::bytes("full:body-v1")),
            },
            VisibilityGrant {
                lookup_key: "user:bob".to_string(),
                row_id: "ticket:t-1".to_string(),
                read_mirror: Some(Value::bytes("redacted:summary-v1")),
            },
        ],
        [
            VisibilityGrant {
                lookup_key: "user:bob".to_string(),
                row_id: "ticket:t-1".to_string(),
                read_mirror: Some(Value::bytes("redacted:summary-v2")),
            },
            VisibilityGrant {
                lookup_key: "user:carol".to_string(),
                row_id: "ticket:t-1".to_string(),
                read_mirror: Some(Value::bytes("full:body-v2")),
            },
        ],
    )
    .expect("compute visibility transitions");

    assert_eq!(
        transitions,
        vec![
            VisibilityProjectionTransition {
                membership: terracedb_capabilities::VisibilityMembershipTransition {
                    index_name: "visible_by_subject".to_string(),
                    lookup_key: "user:alice".to_string(),
                    row_id: "ticket:t-1".to_string(),
                    from_visible: true,
                    to_visible: false,
                    metadata: BTreeMap::from([(
                        "helper".to_string(),
                        serde_json::json!("tickets"),
                    )]),
                },
                previous_read_mirror: Some(Value::bytes("full:body-v1")),
                next_read_mirror: None,
            },
            VisibilityProjectionTransition {
                membership: terracedb_capabilities::VisibilityMembershipTransition {
                    index_name: "visible_by_subject".to_string(),
                    lookup_key: "user:bob".to_string(),
                    row_id: "ticket:t-1".to_string(),
                    from_visible: true,
                    to_visible: true,
                    metadata: BTreeMap::from([(
                        "helper".to_string(),
                        serde_json::json!("tickets"),
                    )]),
                },
                previous_read_mirror: Some(Value::bytes("redacted:summary-v1")),
                next_read_mirror: Some(Value::bytes("redacted:summary-v2")),
            },
            VisibilityProjectionTransition {
                membership: terracedb_capabilities::VisibilityMembershipTransition {
                    index_name: "visible_by_subject".to_string(),
                    lookup_key: "user:carol".to_string(),
                    row_id: "ticket:t-1".to_string(),
                    from_visible: false,
                    to_visible: true,
                    metadata: BTreeMap::from([(
                        "helper".to_string(),
                        serde_json::json!("tickets"),
                    )]),
                },
                previous_read_mirror: None,
                next_read_mirror: Some(Value::bytes("full:body-v2")),
            },
        ]
    );
}

#[test]
fn visibility_scan_plans_cover_subject_tenant_group_and_key_round_trips() {
    let context = PolicyContext {
        subject_id: "user:alice".to_string(),
        tenant_id: Some("tenant-a".to_string()),
        group_ids: vec![
            "support".to_string(),
            "ops".to_string(),
            "support".to_string(),
        ],
        attributes: BTreeMap::new(),
    };

    let subject_plans = visibility_scan_plans(
        &VisibilityIndexSpec {
            index_name: "visible_by_subject".to_string(),
            index_table: "visible_by_subject".to_string(),
            subject_key: VisibilityIndexSubjectKey::Subject,
            row_id_field: "ticket_id".to_string(),
            membership_source: None,
            read_mirror_table: None,
            authoritative_sources: vec!["tickets".to_string()],
            metadata: BTreeMap::from([("role".to_string(), serde_json::json!("owner"))]),
        },
        &context,
    );
    assert_eq!(subject_plans.len(), 1);
    assert_eq!(subject_plans[0].lookup_key, "user:alice");
    assert_eq!(
        subject_plans[0].metadata.get("role"),
        Some(&serde_json::json!("owner"))
    );
    assert_eq!(
        decode_visibility_index_entry_key(&visibility_index_entry_key(
            &subject_plans[0].lookup_key,
            "ticket:t-1"
        ))
        .expect("round-trip visibility key"),
        ("user:alice".to_string(), "ticket:t-1".to_string())
    );

    let tenant_plans = visibility_scan_plans(
        &VisibilityIndexSpec {
            index_name: "visible_by_tenant".to_string(),
            index_table: "visible_by_tenant".to_string(),
            subject_key: VisibilityIndexSubjectKey::Tenant,
            row_id_field: "ticket_id".to_string(),
            membership_source: None,
            read_mirror_table: None,
            authoritative_sources: vec!["tickets".to_string()],
            metadata: BTreeMap::new(),
        },
        &context,
    );
    assert_eq!(tenant_plans[0].lookup_key, "tenant-a");

    let group_plans = visibility_scan_plans(
        &VisibilityIndexSpec {
            index_name: "visible_by_group".to_string(),
            index_table: "visible_by_group".to_string(),
            subject_key: VisibilityIndexSubjectKey::Group,
            row_id_field: "ticket_id".to_string(),
            membership_source: None,
            read_mirror_table: Some("visible_group_reads".to_string()),
            authoritative_sources: vec!["tickets".to_string()],
            metadata: BTreeMap::new(),
        },
        &context,
    );
    assert_eq!(
        group_plans
            .iter()
            .map(|plan| plan.lookup_key.clone())
            .collect::<Vec<_>>(),
        vec!["ops".to_string(), "support".to_string()]
    );
    let (start, end) = visibility_index_lookup_range("support");
    assert_eq!(group_plans[1].start, start);
    assert_eq!(group_plans[1].end, end);
    assert_eq!(
        group_plans[1].read_mirror_table.as_deref(),
        Some("visible_group_reads")
    );
}

#[test]
fn visibility_key_helpers_round_trip_embedded_nuls_without_collisions() {
    let lookup_key = "user:\0alice";
    let row_id = "ticket:\0t-1";
    let encoded = visibility_index_entry_key(lookup_key, row_id);
    assert_eq!(
        decode_visibility_index_entry_key(&encoded).expect("decode embedded-nul visibility key"),
        (lookup_key.to_string(), row_id.to_string())
    );

    let (start, end) = visibility_index_lookup_range(lookup_key);
    assert!(
        start <= encoded,
        "encoded key should sort at or after the lookup-key range start"
    );
    assert!(
        encoded < end,
        "encoded key should remain inside the lookup-key range even with embedded NULs"
    );

    let different_lookup = visibility_index_entry_key("user:\0alice:other", row_id);
    assert!(
        different_lookup >= end,
        "a different logical lookup key should not collide with the original lookup range"
    );
}

#[tokio::test]
async fn visibility_projection_materializes_share_unshare_reassign_delete_and_redaction() {
    let db = Db::open(
        tiered_test_config("/visibility-projection-transitions"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open db");
    let tickets = db
        .create_table(row_table_config("tickets"))
        .await
        .expect("create tickets table");
    let shares = db
        .create_table(row_table_config("shares"))
        .await
        .expect("create shares table");
    let index = db
        .create_table(row_table_config("visible_by_subject"))
        .await
        .expect("create visibility index");
    let read_mirror = db
        .create_table(row_table_config("visible_ticket_reads"))
        .await
        .expect("create read mirror table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            build_subject_visibility_projection(
                subject_visibility_spec("visible_by_subject", "visible_ticket_reads"),
                &tickets,
                &shares,
                &index,
                &read_mirror,
            )
            .into_projection("ticket-visibility")
            .expect("build ticket visibility projection"),
        )
        .await
        .expect("start visibility projection");

    let create = tickets
        .write(
            b"ticket:t-1".to_vec(),
            ticket_value("user:alice", "tenant-a", "secret-body", "summary-v1"),
        )
        .await
        .expect("write ticket");
    wait_for_sources(&mut handle, [(&tickets, create)]).await;
    assert_eq!(
        collect_index_rows(&index).await,
        vec![("user:alice".to_string(), "ticket:t-1".to_string())]
    );
    assert_eq!(
        collect_read_mirrors(&read_mirror).await,
        vec![(
            ("user:alice".to_string(), "ticket:t-1".to_string()),
            "full:secret-body".to_string()
        )]
    );

    let share = shares
        .write(
            b"ticket:t-1/user:bob".to_vec(),
            Value::bytes(Vec::<u8>::new()),
        )
        .await
        .expect("write share");
    wait_for_sources(&mut handle, [(&shares, share)]).await;
    assert_eq!(
        collect_index_rows(&index).await,
        vec![
            ("user:alice".to_string(), "ticket:t-1".to_string()),
            ("user:bob".to_string(), "ticket:t-1".to_string()),
        ]
    );
    assert_eq!(
        collect_read_mirrors(&read_mirror).await,
        vec![
            (
                ("user:alice".to_string(), "ticket:t-1".to_string()),
                "full:secret-body".to_string(),
            ),
            (
                ("user:bob".to_string(), "ticket:t-1".to_string()),
                "redacted:summary-v1".to_string(),
            ),
        ]
    );

    let redact = tickets
        .write(
            b"ticket:t-1".to_vec(),
            ticket_value("user:alice", "tenant-a", "secret-body-v2", "summary-v2"),
        )
        .await
        .expect("update ticket redaction");
    wait_for_sources(&mut handle, [(&tickets, redact)]).await;
    assert_eq!(
        collect_read_mirrors(&read_mirror).await,
        vec![
            (
                ("user:alice".to_string(), "ticket:t-1".to_string()),
                "full:secret-body-v2".to_string(),
            ),
            (
                ("user:bob".to_string(), "ticket:t-1".to_string()),
                "redacted:summary-v2".to_string(),
            ),
        ]
    );

    let unshare = shares
        .delete(b"ticket:t-1/user:bob".to_vec())
        .await
        .expect("delete share");
    wait_for_sources(&mut handle, [(&shares, unshare)]).await;
    assert_eq!(
        collect_index_rows(&index).await,
        vec![("user:alice".to_string(), "ticket:t-1".to_string())]
    );

    let reassign = tickets
        .write(
            b"ticket:t-1".to_vec(),
            ticket_value("user:carol", "tenant-a", "secret-body-v3", "summary-v3"),
        )
        .await
        .expect("reassign owner");
    wait_for_sources(&mut handle, [(&tickets, reassign)]).await;
    assert_eq!(
        collect_index_rows(&index).await,
        vec![("user:carol".to_string(), "ticket:t-1".to_string())]
    );
    assert_eq!(
        collect_read_mirrors(&read_mirror).await,
        vec![(
            ("user:carol".to_string(), "ticket:t-1".to_string()),
            "full:secret-body-v3".to_string()
        )]
    );

    let delete = tickets
        .delete(b"ticket:t-1".to_vec())
        .await
        .expect("delete ticket");
    wait_for_sources(&mut handle, [(&tickets, delete)]).await;
    assert!(collect_index_rows(&index).await.is_empty());
    assert!(collect_read_mirrors(&read_mirror).await.is_empty());

    handle.shutdown().await.expect("stop visibility projection");
}

#[tokio::test]
async fn visibility_projection_rebuild_matches_incremental_state() {
    let db = Db::open(
        tiered_test_config("/visibility-projection-rebuild"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open db");
    let tickets = db
        .create_table(retained_row_table_config("tickets", 1))
        .await
        .expect("create retained tickets table");
    let shares = db
        .create_table(retained_row_table_config("shares", 1))
        .await
        .expect("create retained shares table");
    let incremental_index = db
        .create_table(row_table_config("visible_by_subject_incremental"))
        .await
        .expect("create incremental index");
    let incremental_mirror = db
        .create_table(row_table_config("visible_ticket_reads_incremental"))
        .await
        .expect("create incremental mirror");
    let rebuilt_index = db
        .create_table(row_table_config("visible_by_subject_rebuilt"))
        .await
        .expect("create rebuilt index");
    let rebuilt_mirror = db
        .create_table(row_table_config("visible_ticket_reads_rebuilt"))
        .await
        .expect("create rebuilt mirror");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let incremental_spec = subject_visibility_spec(
        "visible_by_subject_incremental",
        "visible_ticket_reads_incremental",
    );
    let mut incremental = runtime
        .start_multi_source(
            build_subject_visibility_projection(
                incremental_spec,
                &tickets,
                &shares,
                &incremental_index,
                &incremental_mirror,
            )
            .into_projection("ticket-visibility-incremental")
            .expect("build incremental projection"),
        )
        .await
        .expect("start incremental projection");

    let mut first = db.write_batch();
    first.put(
        &tickets,
        b"ticket:t-1".to_vec(),
        ticket_value("user:alice", "tenant-a", "body-v1", "summary-v1"),
    );
    let first_seq = db
        .commit(first, CommitOptions::default())
        .await
        .expect("commit first visibility batch");

    let mut second = db.write_batch();
    second.put(
        &shares,
        b"ticket:t-1/user:bob".to_vec(),
        Value::bytes(Vec::<u8>::new()),
    );
    let second_seq = db
        .commit(second, CommitOptions::default())
        .await
        .expect("commit share batch");

    let mut third = db.write_batch();
    third.put(
        &tickets,
        b"ticket:t-1".to_vec(),
        ticket_value("user:carol", "tenant-a", "body-v2", "summary-v2"),
    );
    let third_seq = db
        .commit(third, CommitOptions::default())
        .await
        .expect("commit reassignment batch");
    db.flush().await.expect("flush retained visibility sources");

    wait_for_sources(
        &mut incremental,
        [(&tickets, third_seq), (&shares, second_seq)],
    )
    .await;
    let incremental_rows = collect_index_rows(&incremental_index).await;
    let incremental_mirrors = collect_read_mirrors(&incremental_mirror).await;
    assert!(
        first_seq < third_seq,
        "expected source sequences to advance"
    );

    let rebuilt_spec =
        subject_visibility_spec("visible_by_subject_rebuilt", "visible_ticket_reads_rebuilt");
    let mut rebuilt = runtime
        .start_multi_source(
            build_subject_visibility_projection(
                rebuilt_spec,
                &tickets,
                &shares,
                &rebuilt_index,
                &rebuilt_mirror,
            )
            .into_projection("ticket-visibility-rebuilt")
            .expect("build rebuilt projection"),
        )
        .await
        .expect("start rebuilt projection");

    wait_for_sources(&mut rebuilt, [(&tickets, third_seq), (&shares, second_seq)]).await;
    assert_eq!(collect_index_rows(&rebuilt_index).await, incremental_rows);
    assert_eq!(
        collect_read_mirrors(&rebuilt_mirror).await,
        incremental_mirrors
    );

    incremental
        .shutdown()
        .await
        .expect("stop incremental projection");
    rebuilt.shutdown().await.expect("stop rebuilt projection");
}

#[tokio::test]
async fn visibility_projection_recovers_index_and_read_mirror_after_crash() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());
    let dependencies = test_dependencies(file_system.clone(), object_store.clone());
    let config = terracedb::test_support::tiered_test_config_with_durability(
        "/visibility-projection-crash-recovery",
        TieredDurabilityMode::Deferred,
    );
    let db = Db::open(config.clone(), dependencies.clone())
        .await
        .expect("open db");
    let tickets = db
        .create_table(row_table_config("tickets"))
        .await
        .expect("create tickets table");
    let shares = db
        .create_table(row_table_config("shares"))
        .await
        .expect("create shares table");
    let index = db
        .create_table(row_table_config("visible_by_subject"))
        .await
        .expect("create visibility index");
    let read_mirror = db
        .create_table(row_table_config("visible_ticket_reads"))
        .await
        .expect("create read mirror table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = runtime
        .start_multi_source(
            build_subject_visibility_projection(
                subject_visibility_spec("visible_by_subject", "visible_ticket_reads"),
                &tickets,
                &shares,
                &index,
                &read_mirror,
            )
            .into_projection("ticket-visibility")
            .expect("build visibility projection"),
        )
        .await
        .expect("start visibility projection");

    let mut batch = db.write_batch();
    batch.put(
        &tickets,
        b"ticket:t-1".to_vec(),
        ticket_value("user:alice", "tenant-a", "body-v1", "summary-v1"),
    );
    batch.put(
        &shares,
        b"ticket:t-1/user:bob".to_vec(),
        Value::bytes(Vec::<u8>::new()),
    );
    let sequence = db
        .commit(batch, CommitOptions::default())
        .await
        .expect("commit visibility sources");
    db.flush().await.expect("flush durable visibility sources");
    wait_for_sources(&mut handle, [(&tickets, sequence), (&shares, sequence)]).await;

    assert_eq!(
        runtime
            .load_projection_frontier("ticket-visibility", [&tickets, &shares])
            .await
            .expect("load visible frontier")
            .into_iter()
            .map(|(name, state)| (name, state.cursor()))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            ("shares".to_string(), LogCursor::new(sequence, 1)),
            ("tickets".to_string(), LogCursor::new(sequence, 0)),
        ])
    );
    assert_eq!(
        collect_index_rows(&index).await,
        vec![
            ("user:alice".to_string(), "ticket:t-1".to_string()),
            ("user:bob".to_string(), "ticket:t-1".to_string()),
        ]
    );
    assert_eq!(
        collect_read_mirrors(&read_mirror).await,
        vec![
            (
                ("user:alice".to_string(), "ticket:t-1".to_string()),
                "full:body-v1".to_string(),
            ),
            (
                ("user:bob".to_string(), "ticket:t-1".to_string()),
                "redacted:summary-v1".to_string(),
            ),
        ]
    );

    file_system.crash();
    drop(handle);

    let reopened = Db::open(config, dependencies)
        .await
        .expect("reopen db after crash");
    let reopened_tickets = reopened.table("tickets");
    let reopened_shares = reopened.table("shares");
    let reopened_index = reopened.table("visible_by_subject");
    let reopened_read_mirror = reopened.table("visible_ticket_reads");
    let reopened_runtime = ProjectionRuntime::open(reopened.clone())
        .await
        .expect("open projection runtime after crash");

    assert_eq!(
        reopened_runtime
            .load_projection_frontier("ticket-visibility", [&reopened_tickets, &reopened_shares])
            .await
            .expect("load frontier after crash")
            .into_iter()
            .map(|(name, state)| (name, state.cursor()))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            ("shares".to_string(), LogCursor::beginning()),
            ("tickets".to_string(), LogCursor::beginning()),
        ])
    );
    assert!(collect_index_rows(&reopened_index).await.is_empty());
    assert!(collect_read_mirrors(&reopened_read_mirror).await.is_empty());

    let mut replay = reopened_runtime
        .start_multi_source(
            build_subject_visibility_projection(
                subject_visibility_spec("visible_by_subject", "visible_ticket_reads"),
                &reopened_tickets,
                &reopened_shares,
                &reopened_index,
                &reopened_read_mirror,
            )
            .into_projection("ticket-visibility")
            .expect("rebuild visibility projection"),
        )
        .await
        .expect("restart visibility projection");
    wait_for_sources(
        &mut replay,
        [(&reopened_tickets, sequence), (&reopened_shares, sequence)],
    )
    .await;

    assert_eq!(
        reopened_runtime
            .load_projection_frontier("ticket-visibility", [&reopened_tickets, &reopened_shares])
            .await
            .expect("load replayed frontier")
            .into_iter()
            .map(|(name, state)| (name, state.cursor()))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            ("shares".to_string(), LogCursor::new(sequence, 1)),
            ("tickets".to_string(), LogCursor::new(sequence, 0)),
        ])
    );
    assert_eq!(
        collect_index_rows(&reopened_index).await,
        vec![
            ("user:alice".to_string(), "ticket:t-1".to_string()),
            ("user:bob".to_string(), "ticket:t-1".to_string()),
        ]
    );
    assert_eq!(
        collect_read_mirrors(&reopened_read_mirror).await,
        vec![
            (
                ("user:alice".to_string(), "ticket:t-1".to_string()),
                "full:body-v1".to_string(),
            ),
            (
                ("user:bob".to_string(), "ticket:t-1".to_string()),
                "redacted:summary-v1".to_string(),
            ),
        ]
    );

    replay
        .shutdown()
        .await
        .expect("stop replayed visibility projection");
}
