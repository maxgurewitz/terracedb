use std::sync::Arc;

use terracedb::{
    CommitError, CompactionStrategy, Db, DbConfig, DbDependencies, S3Location, SequenceNumber,
    SsdConfig, StorageConfig, StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig,
    TableFormat, TieredDurabilityMode, TieredStorageConfig, Transaction, TransactionCommitError,
    Value,
};

fn tiered_config(path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "occ-transactions".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
            local_retention: terracedb::TieredLocalRetentionMode::Offload,
        }),
        scheduler: None,
    }
}

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(8),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        metadata: Default::default(),
    }
}

fn dependencies(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> DbDependencies {
    DbDependencies::new(
        file_system,
        object_store,
        Arc::new(StubClock::default()),
        Arc::new(StubRng::seeded(7)),
    )
}

async fn open_db(
    path: &str,
    durability: TieredDurabilityMode,
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> Db {
    Db::open(
        tiered_config(path, durability),
        dependencies(file_system, object_store),
    )
    .await
    .expect("open db")
}

#[tokio::test]
async fn transaction_reads_are_snapshot_isolated_and_read_your_own_writes() {
    let db = open_db(
        "/occ-snapshot",
        TieredDurabilityMode::GroupCommit,
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
    )
    .await;
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect("seed value");

    let mut tx = Transaction::begin(&db).await;
    assert_eq!(tx.snapshot_sequence(), SequenceNumber::new(1));
    assert_eq!(
        tx.read(&table, b"user:1".to_vec())
            .await
            .expect("read snapshot value"),
        Some(Value::bytes("v1"))
    );

    table
        .write(b"user:1".to_vec(), Value::bytes("v2"))
        .await
        .expect("concurrent update");

    assert_eq!(
        tx.read(&table, b"user:1".to_vec())
            .await
            .expect("re-read snapshot value"),
        Some(Value::bytes("v1"))
    );

    tx.write(&table, b"user:2".to_vec(), Value::bytes("local"));
    assert_eq!(
        tx.read(&table, b"user:2".to_vec())
            .await
            .expect("read own write"),
        Some(Value::bytes("local"))
    );

    tx.delete(&table, b"user:2".to_vec());
    assert_eq!(
        tx.read(&table, b"user:2".to_vec())
            .await
            .expect("read own delete"),
        None
    );

    tx.abort();
}

#[tokio::test]
async fn transaction_commit_conflicts_when_a_read_key_changes() {
    let db = open_db(
        "/occ-conflict",
        TieredDurabilityMode::GroupCommit,
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
    )
    .await;
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    table
        .write(b"user:1".to_vec(), Value::bytes("v1"))
        .await
        .expect("seed value");

    let mut tx = Transaction::begin(&db).await;
    assert_eq!(
        tx.read(&table, b"user:1".to_vec())
            .await
            .expect("read current value"),
        Some(Value::bytes("v1"))
    );

    table
        .write(b"user:1".to_vec(), Value::bytes("v2"))
        .await
        .expect("concurrent update");
    tx.write(&table, b"user:2".to_vec(), Value::bytes("candidate"));

    let error = tx.commit().await.expect_err("commit should conflict");
    assert_eq!(error, TransactionCommitError::Commit(CommitError::Conflict));
}

#[tokio::test]
async fn transaction_write_write_without_read_does_not_conflict() {
    let db = open_db(
        "/occ-write-write",
        TieredDurabilityMode::GroupCommit,
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
    )
    .await;
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    let mut tx = Transaction::begin(&db).await;
    tx.write(&table, b"user:1".to_vec(), Value::bytes("txn"));

    table
        .write(b"user:1".to_vec(), Value::bytes("outside"))
        .await
        .expect("concurrent outside write");

    assert_eq!(
        tx.commit().await.expect("commit should succeed"),
        SequenceNumber::new(2)
    );
    assert_eq!(
        table
            .read(b"user:1".to_vec())
            .await
            .expect("read final value"),
        Some(Value::bytes("txn"))
    );
}

#[tokio::test]
async fn transaction_flush_modes_match_crash_recovery_atomicity() {
    let file_system = Arc::new(StubFileSystem::default());
    let object_store = Arc::new(StubObjectStore::default());

    let db = open_db(
        "/occ-durability",
        TieredDurabilityMode::Deferred,
        file_system.clone(),
        object_store.clone(),
    )
    .await;
    let table = db
        .create_table(row_table_config("events"))
        .await
        .expect("create table");

    let mut no_flush_tx = Transaction::begin(&db).await;
    no_flush_tx.write(&table, b"user:1".to_vec(), Value::bytes("volatile-a"));
    no_flush_tx.write(&table, b"user:2".to_vec(), Value::bytes("volatile-b"));
    assert_eq!(
        no_flush_tx
            .commit_no_flush()
            .await
            .expect("commit without flush"),
        SequenceNumber::new(1)
    );
    assert_eq!(db.current_sequence(), SequenceNumber::new(1));
    assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));
    assert_eq!(
        table
            .read(b"user:1".to_vec())
            .await
            .expect("visible volatile write"),
        Some(Value::bytes("volatile-a"))
    );
    assert_eq!(
        table
            .read(b"user:2".to_vec())
            .await
            .expect("visible volatile write"),
        Some(Value::bytes("volatile-b"))
    );

    file_system.crash();

    let reopened = open_db(
        "/occ-durability",
        TieredDurabilityMode::Deferred,
        file_system.clone(),
        object_store.clone(),
    )
    .await;
    let reopened_table = reopened.table("events");
    assert_eq!(reopened.current_sequence(), SequenceNumber::new(0));
    assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(0));
    assert_eq!(
        reopened_table
            .read(b"user:1".to_vec())
            .await
            .expect("read after crash"),
        None
    );
    assert_eq!(
        reopened_table
            .read(b"user:2".to_vec())
            .await
            .expect("read after crash"),
        None
    );

    let mut flushed_tx = Transaction::begin(&reopened).await;
    flushed_tx.write(
        &reopened_table,
        b"user:1".to_vec(),
        Value::bytes("durable-a"),
    );
    flushed_tx.write(
        &reopened_table,
        b"user:2".to_vec(),
        Value::bytes("durable-b"),
    );
    assert_eq!(
        flushed_tx.commit().await.expect("commit with flush"),
        SequenceNumber::new(1)
    );
    assert_eq!(reopened.current_sequence(), SequenceNumber::new(1));
    assert_eq!(reopened.current_durable_sequence(), SequenceNumber::new(1));

    file_system.crash();

    let durable_reopened = open_db(
        "/occ-durability",
        TieredDurabilityMode::Deferred,
        file_system,
        object_store,
    )
    .await;
    let durable_table = durable_reopened.table("events");
    assert_eq!(durable_reopened.current_sequence(), SequenceNumber::new(1));
    assert_eq!(
        durable_table
            .read(b"user:1".to_vec())
            .await
            .expect("read durable value"),
        Some(Value::bytes("durable-a"))
    );
    assert_eq!(
        durable_table
            .read(b"user:2".to_vec())
            .await
            .expect("read durable value"),
        Some(Value::bytes("durable-b"))
    );
}
