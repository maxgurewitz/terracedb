use std::{str::FromStr, sync::Arc, time::Duration};

use futures::StreamExt;
use terracedb::{
    CommitOptions, Db, KvStream, ScanOptions, SequenceNumber, StubFileSystem, StubObjectStore,
    Table, Value,
    test_support::{row_table_config, test_dependencies, tiered_test_config},
};
use terracedb_projections::{
    MultiSourceProjection, ProjectionHandle, ProjectionHandlerError, ProjectionRuntime,
    RankedMaterializedProjection, RecomputeStrategy,
};

const OUTPUT_LIMIT: usize = 3;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RankedTodo {
    todo_id: String,
    updated_at_ms: u64,
}

fn todo_value(updated_at_ms: u64, title: &str) -> Value {
    Value::bytes(format!("{updated_at_ms:020}|{title}"))
}

fn decode_ranked_todo(
    source_key: Vec<u8>,
    value: Value,
) -> Result<Option<RankedTodo>, ProjectionHandlerError> {
    let Value::Bytes(bytes) = value else {
        return Err(ProjectionHandlerError::new(std::io::Error::other(
            "ranked test source rows must be byte encoded",
        )));
    };
    let raw = std::str::from_utf8(&bytes).map_err(ProjectionHandlerError::new)?;
    let (updated_at_ms, _title) = raw.split_once('|').ok_or_else(|| {
        ProjectionHandlerError::new(std::io::Error::other(
            "ranked test source rows must include a numeric rank prefix",
        ))
    })?;
    Ok(Some(RankedTodo {
        todo_id: String::from_utf8(source_key).map_err(ProjectionHandlerError::new)?,
        updated_at_ms: u64::from_str(updated_at_ms).map_err(ProjectionHandlerError::new)?,
    }))
}

fn rank_recent(todo: &RankedTodo) -> u64 {
    todo.updated_at_ms
}

fn tie_break_todo_id(todo: &RankedTodo) -> String {
    todo.todo_id.clone()
}

fn recent_slot_key(slot: usize, _todo: &RankedTodo) -> Vec<u8> {
    format!("recent:{slot:02}").into_bytes()
}

fn encode_output_todo_id(todo: &RankedTodo) -> Result<Value, ProjectionHandlerError> {
    Ok(Value::bytes(todo.todo_id.clone()))
}

async fn collect_output_ids(stream: KvStream) -> Vec<String> {
    let mut ids = Vec::new();
    futures::pin_mut!(stream);
    while let Some((_key, value)) = stream.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("ranked materialization tests only expect byte output rows");
        };
        ids.push(String::from_utf8(bytes).expect("output ids should be utf-8"));
    }
    ids
}

async fn current_output_ids(output: &Table) -> Vec<String> {
    collect_output_ids(
        output
            .scan(Vec::new(), vec![0xff], ScanOptions::default())
            .await
            .expect("scan ranked output"),
    )
    .await
}

async fn wait_for_source(handle: &mut ProjectionHandle, source: &Table, sequence: SequenceNumber) {
    tokio::time::timeout(
        Duration::from_secs(3),
        handle.wait_for_sources([(source, sequence)]),
    )
    .await
    .expect("projection should reach requested source frontier")
    .expect("projection should not fail while waiting");
}

async fn start_ranked_projection(
    runtime: &ProjectionRuntime,
    name: &str,
    source: &Table,
    output: &Table,
    recompute: RecomputeStrategy,
) -> ProjectionHandle {
    runtime
        .start_multi_source(
            MultiSourceProjection::new(
                name,
                [source.clone()],
                RankedMaterializedProjection::new(
                    source.clone(),
                    output.clone(),
                    OUTPUT_LIMIT,
                    decode_ranked_todo,
                    rank_recent,
                    tie_break_todo_id,
                    recent_slot_key,
                    encode_output_todo_id,
                ),
            )
            .with_outputs([output.clone()])
            .with_recompute_strategy(recompute),
        )
        .await
        .expect("start ranked projection")
}

async fn apply_ranked_todo_history(db: &Db, source: &Table) -> SequenceNumber {
    let mut first_batch = db.write_batch();
    first_batch.put(&source, b"alpha".to_vec(), todo_value(10, "alpha"));
    first_batch.put(&source, b"bravo".to_vec(), todo_value(12, "bravo"));
    first_batch.put(&source, b"charlie".to_vec(), todo_value(12, "charlie"));
    first_batch.put(&source, b"delta".to_vec(), todo_value(9, "delta"));
    let _first = db
        .commit(first_batch, CommitOptions::default())
        .await
        .expect("commit initial ranked todos");

    let mut second_batch = db.write_batch();
    second_batch.put(&source, b"alpha".to_vec(), todo_value(15, "alpha updated"));
    second_batch.delete(&source, b"bravo".to_vec());
    second_batch.put(&source, b"echo".to_vec(), todo_value(15, "echo"));
    second_batch.put(&source, b"foxtrot".to_vec(), todo_value(14, "foxtrot"));
    let _second = db
        .commit(second_batch, CommitOptions::default())
        .await
        .expect("commit ranked todo updates");

    let mut third_batch = db.write_batch();
    third_batch.delete(&source, b"alpha".to_vec());
    db.commit(third_batch, CommitOptions::default())
        .await
        .expect("commit ranked todo delete")
}

#[tokio::test]
async fn ranked_materialization_tracks_updates_deletes_ties_and_truncation() {
    let db = Db::open(
        tiered_test_config("/ranked-materialization-basic"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("todos"))
        .await
        .expect("create source table");
    let output = db
        .create_table(row_table_config("recent_todos"))
        .await
        .expect("create output table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut handle = start_ranked_projection(
        &runtime,
        "ranked-basic",
        &source,
        &output,
        RecomputeStrategy::FailClosed,
    )
    .await;

    let mut first_batch = db.write_batch();
    first_batch.put(&source, b"alpha".to_vec(), todo_value(10, "alpha"));
    first_batch.put(&source, b"bravo".to_vec(), todo_value(12, "bravo"));
    first_batch.put(&source, b"charlie".to_vec(), todo_value(12, "charlie"));
    first_batch.put(&source, b"delta".to_vec(), todo_value(9, "delta"));
    let first_sequence = db
        .commit(first_batch, CommitOptions::default())
        .await
        .expect("commit initial ranked todos");
    wait_for_source(&mut handle, &source, first_sequence).await;
    assert_eq!(
        current_output_ids(&output).await,
        vec![
            "bravo".to_string(),
            "charlie".to_string(),
            "alpha".to_string(),
        ]
    );

    let mut second_batch = db.write_batch();
    second_batch.put(&source, b"alpha".to_vec(), todo_value(15, "alpha updated"));
    second_batch.delete(&source, b"bravo".to_vec());
    second_batch.put(&source, b"echo".to_vec(), todo_value(15, "echo"));
    second_batch.put(&source, b"foxtrot".to_vec(), todo_value(14, "foxtrot"));
    let second_sequence = db
        .commit(second_batch, CommitOptions::default())
        .await
        .expect("commit ranked todo updates");
    wait_for_source(&mut handle, &source, second_sequence).await;
    assert_eq!(
        current_output_ids(&output).await,
        vec![
            "alpha".to_string(),
            "echo".to_string(),
            "foxtrot".to_string(),
        ]
    );

    let mut third_batch = db.write_batch();
    third_batch.delete(&source, b"alpha".to_vec());
    let third_sequence = db
        .commit(third_batch, CommitOptions::default())
        .await
        .expect("commit ranked todo delete");
    wait_for_source(&mut handle, &source, third_sequence).await;
    assert_eq!(
        current_output_ids(&output).await,
        vec![
            "echo".to_string(),
            "foxtrot".to_string(),
            "charlie".to_string(),
        ]
    );

    handle.shutdown().await.expect("stop ranked projection");
}

#[tokio::test]
async fn ranked_materialization_backlog_replay_matches_incremental_result() {
    let db = Db::open(
        tiered_test_config("/ranked-materialization-backlog"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open db");
    let source = db
        .create_table(row_table_config("todos"))
        .await
        .expect("create source table");
    let incremental_output = db
        .create_table(row_table_config("recent_incremental"))
        .await
        .expect("create incremental output table");
    let replay_output = db
        .create_table(row_table_config("recent_replay"))
        .await
        .expect("create replay output table");

    let runtime = ProjectionRuntime::open(db.clone())
        .await
        .expect("open projection runtime");
    let mut incremental = start_ranked_projection(
        &runtime,
        "ranked-incremental",
        &source,
        &incremental_output,
        RecomputeStrategy::FailClosed,
    )
    .await;

    let final_sequence = apply_ranked_todo_history(&db, &source).await;
    wait_for_source(&mut incremental, &source, final_sequence).await;
    db.flush().await.expect("flush backlog history");

    let mut replay = start_ranked_projection(
        &runtime,
        "ranked-replay",
        &source,
        &replay_output,
        RecomputeStrategy::FailClosed,
    )
    .await;
    wait_for_source(&mut replay, &source, final_sequence).await;

    assert_eq!(
        current_output_ids(&incremental_output).await,
        current_output_ids(&replay_output).await
    );

    replay.shutdown().await.expect("stop replay projection");
    incremental
        .shutdown()
        .await
        .expect("stop incremental projection");
}

#[tokio::test]
async fn ranked_materialization_rebuild_matches_incremental_result() {
    let incremental_db = Db::open(
        tiered_test_config("/ranked-materialization-rebuild-incremental"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open incremental db");
    let incremental_source = incremental_db
        .create_table(row_table_config("todos"))
        .await
        .expect("create incremental source table");
    let incremental_output = incremental_db
        .create_table(row_table_config("recent_incremental"))
        .await
        .expect("create incremental output table");
    let incremental_runtime = ProjectionRuntime::open(incremental_db.clone())
        .await
        .expect("open incremental projection runtime");
    let mut incremental = start_ranked_projection(
        &incremental_runtime,
        "ranked-rebuild-incremental",
        &incremental_source,
        &incremental_output,
        RecomputeStrategy::FailClosed,
    )
    .await;
    let final_incremental_sequence =
        apply_ranked_todo_history(&incremental_db, &incremental_source).await;
    wait_for_source(
        &mut incremental,
        &incremental_source,
        final_incremental_sequence,
    )
    .await;
    let incremental_ids = current_output_ids(&incremental_output).await;
    incremental
        .shutdown()
        .await
        .expect("stop incremental projection");

    let rebuild_db = Db::open(
        tiered_test_config("/ranked-materialization-rebuild-current-state"),
        test_dependencies(
            Arc::new(StubFileSystem::default()),
            Arc::new(StubObjectStore::default()),
        ),
    )
    .await
    .expect("open rebuild db");
    let mut source_config = row_table_config("todos");
    source_config.history_retention_sequences = Some(1);
    let rebuild_source = rebuild_db
        .create_table(source_config)
        .await
        .expect("create rebuild source table");
    let rebuild_output = rebuild_db
        .create_table(row_table_config("recent_rebuild"))
        .await
        .expect("create rebuild output table");
    let final_sequence = apply_ranked_todo_history(&rebuild_db, &rebuild_source).await;
    rebuild_db.flush().await.expect("flush rebuild history");

    let rebuild_runtime = ProjectionRuntime::open(rebuild_db.clone())
        .await
        .expect("open rebuild projection runtime");
    let mut rebuilt = start_ranked_projection(
        &rebuild_runtime,
        "ranked-rebuild-current-state",
        &rebuild_source,
        &rebuild_output,
        RecomputeStrategy::RebuildFromCurrentState,
    )
    .await;
    wait_for_source(&mut rebuilt, &rebuild_source, final_sequence).await;

    assert_eq!(incremental_ids, current_output_ids(&rebuild_output).await);

    rebuilt.shutdown().await.expect("stop rebuilt projection");
}
