use std::{collections::BTreeSet, sync::Arc, time::Duration};

use terracedb::{
    Clock, CommitOptions, CompactionStrategy, CutPoint, Db, DbConfig, DbDependencies,
    DurableOutboxConsumer, DurableTimerSet, OutboxEntry, ScheduledTimer, SeededSimulationRunner,
    SequenceNumber, StubClock, StubFileSystem, StubObjectStore, StubRng, Table, TableConfig,
    TableFormat, TieredDurabilityMode, TieredStorageConfig, Timestamp, Transaction,
    TransactionalOutbox, Value,
};
use terracedb::{S3Location, SsdConfig, StorageConfig};

fn row_table_config(name: &str) -> TableConfig {
    TableConfig {
        name: name.to_string(),
        format: TableFormat::Row,
        merge_operator: None,
        max_merge_operand_chain_length: None,
        compaction_filter: None,
        bloom_filter_bits_per_key: Some(10),
        history_retention_sequences: None,
        compaction_strategy: CompactionStrategy::Leveled,
        schema: None,
        metadata: Default::default(),
    }
}

#[derive(Clone)]
struct TestEnv {
    config: DbConfig,
    dependencies: DbDependencies,
    file_system: Arc<StubFileSystem>,
    clock: Arc<StubClock>,
}

impl TestEnv {
    fn new(path: &str, durability: TieredDurabilityMode) -> Self {
        let file_system = Arc::new(StubFileSystem::default());
        let object_store = Arc::new(StubObjectStore::default());
        let clock = Arc::new(StubClock::default());
        let dependencies = DbDependencies::new(
            file_system.clone(),
            object_store,
            clock.clone(),
            Arc::new(StubRng::seeded(0x2929)),
        );

        let config = DbConfig {
            storage: StorageConfig::Tiered(TieredStorageConfig {
                ssd: SsdConfig {
                    path: path.to_string(),
                },
                s3: S3Location {
                    bucket: "terracedb-test".to_string(),
                    prefix: "t29".to_string(),
                },
                max_local_bytes: 1024 * 1024,
                durability,
            }),
            scheduler: None,
        };

        Self {
            config,
            dependencies,
            file_system,
            clock,
        }
    }

    async fn open(&self) -> Db {
        Db::open(self.config.clone(), self.dependencies.clone())
            .await
            .expect("open db")
    }

    async fn restart(&self) -> Db {
        self.file_system.crash();
        self.open().await
    }
}

async fn fire_timer_once(
    db: &Db,
    timers: &DurableTimerSet,
    state_table: &Table,
    timer_id: &[u8],
    due: &terracedb::DueTimer,
) -> bool {
    let mut tx = Transaction::begin(db).await;
    if tx
        .read(state_table, timer_id.to_vec())
        .await
        .expect("read timer state")
        .is_some()
    {
        return false;
    }

    tx.write(state_table, timer_id.to_vec(), Value::bytes("fired"));
    timers.stage_due_deletion_in_transaction(&mut tx, due);
    tx.commit().await.expect("commit fired timer");
    true
}

#[tokio::test]
async fn due_timers_wait_for_the_durable_prefix() {
    let env = TestEnv::new(
        "/terracedb/t29/direct-durable-fence",
        TieredDurabilityMode::Deferred,
    );
    let db = env.open().await;
    let schedule = db
        .create_table(row_table_config("timer_schedule"))
        .await
        .expect("create timer schedule");
    let lookup = db
        .create_table(row_table_config("timer_lookup"))
        .await
        .expect("create timer lookup");
    let timers = DurableTimerSet::new(schedule, lookup);

    let mut batch = db.write_batch();
    timers
        .stage_schedule(
            &mut batch,
            ScheduledTimer {
                timer_id: b"timer:signup".to_vec(),
                fire_at: Timestamp::new(5),
                payload: b"welcome".to_vec(),
            },
        )
        .expect("stage timer");
    let scheduled = db
        .commit(batch, CommitOptions::default())
        .await
        .expect("commit timer");
    assert_eq!(scheduled, SequenceNumber::new(1));
    assert_eq!(db.current_durable_sequence(), SequenceNumber::new(0));

    env.clock.advance(Duration::from_millis(10));
    let before_flush = timers
        .scan_due_durable(&db, env.clock.now(), None)
        .await
        .expect("scan before flush");
    assert!(before_flush.is_empty());
    assert_eq!(before_flush.durable_sequence, SequenceNumber::new(0));

    db.flush().await.expect("flush durable timer");
    let after_flush = timers
        .scan_due_durable(&db, env.clock.now(), None)
        .await
        .expect("scan after flush");
    assert_eq!(after_flush.durable_sequence, scheduled);
    assert_eq!(after_flush.timers.len(), 1);
    assert_eq!(after_flush.timers[0].timer_id, b"timer:signup".to_vec());
    assert_eq!(after_flush.timers[0].payload, b"welcome".to_vec());
}

#[tokio::test]
async fn duplicate_timer_reprocessing_is_benign_with_state_guards() {
    let env = TestEnv::new(
        "/terracedb/t29/direct-timer-duplicates",
        TieredDurabilityMode::GroupCommit,
    );
    let db = env.open().await;
    let schedule = db
        .create_table(row_table_config("timer_schedule"))
        .await
        .expect("create timer schedule");
    let lookup = db
        .create_table(row_table_config("timer_lookup"))
        .await
        .expect("create timer lookup");
    let state = db
        .create_table(row_table_config("workflow_state"))
        .await
        .expect("create state table");
    let timers = DurableTimerSet::new(schedule.clone(), lookup.clone());

    let timer_id = b"timer:payment-timeout".to_vec();
    let mut batch = db.write_batch();
    timers
        .stage_schedule(
            &mut batch,
            ScheduledTimer {
                timer_id: timer_id.clone(),
                fire_at: Timestamp::new(1),
                payload: b"timeout".to_vec(),
            },
        )
        .expect("stage timer");
    db.commit(batch, CommitOptions::default())
        .await
        .expect("commit timer");

    env.clock.advance(Duration::from_millis(5));
    let due = timers
        .scan_due_durable(&db, env.clock.now(), None)
        .await
        .expect("scan due timers")
        .timers
        .pop()
        .expect("one due timer");

    assert!(fire_timer_once(&db, &timers, &state, &timer_id, &due).await);
    assert!(!fire_timer_once(&db, &timers, &state, &timer_id, &due).await);
    assert_eq!(
        state
            .read(timer_id.clone())
            .await
            .expect("read fired state"),
        Some(Value::bytes("fired"))
    );
    assert!(
        schedule
            .read(due.schedule_key.clone())
            .await
            .expect("schedule row gone")
            .is_none()
    );
    assert!(
        lookup
            .read(timer_id)
            .await
            .expect("lookup row gone")
            .is_none()
    );
}

#[tokio::test]
async fn durable_timers_survive_restart_before_firing() {
    let env = TestEnv::new(
        "/terracedb/t29/direct-timer-restart",
        TieredDurabilityMode::GroupCommit,
    );
    let db = env.open().await;
    let schedule = db
        .create_table(row_table_config("timer_schedule"))
        .await
        .expect("create timer schedule");
    let lookup = db
        .create_table(row_table_config("timer_lookup"))
        .await
        .expect("create timer lookup");
    let timers = DurableTimerSet::new(schedule, lookup);

    let mut batch = db.write_batch();
    timers
        .stage_schedule(
            &mut batch,
            ScheduledTimer {
                timer_id: b"timer:retry".to_vec(),
                fire_at: Timestamp::new(3),
                payload: b"retry-payload".to_vec(),
            },
        )
        .expect("stage timer");
    db.commit(batch, CommitOptions::default())
        .await
        .expect("commit timer");

    env.clock.advance(Duration::from_millis(10));
    let reopened = env.restart().await;
    let reopened_timers = DurableTimerSet::new(
        reopened.table("timer_schedule"),
        reopened.table("timer_lookup"),
    );
    let due = reopened_timers
        .scan_due_durable(&reopened, env.clock.now(), None)
        .await
        .expect("scan due timers after restart");
    assert_eq!(due.timers.len(), 1);
    assert_eq!(due.timers[0].timer_id, b"timer:retry".to_vec());
}

#[tokio::test]
async fn outbox_replays_after_delivery_before_cursor_persistence() {
    let env = TestEnv::new(
        "/terracedb/t29/direct-outbox-replay",
        TieredDurabilityMode::GroupCommit,
    );
    let db = env.open().await;
    let outbox_table = db
        .create_table(row_table_config("outbox"))
        .await
        .expect("create outbox table");
    let cursor_table = db
        .create_table(row_table_config("outbox_cursors"))
        .await
        .expect("create outbox cursor table");
    let outbox = TransactionalOutbox::new(outbox_table.clone());

    let mut batch = db.write_batch();
    outbox
        .stage_entry(
            &mut batch,
            OutboxEntry {
                outbox_id: b"order:1:confirmation".to_vec(),
                idempotency_key: "order:1:confirmation".to_string(),
                payload: b"send-confirmation".to_vec(),
            },
        )
        .expect("stage outbox entry");
    db.commit(batch, CommitOptions::default())
        .await
        .expect("commit outbox entry");

    let consumer_id = b"mailer".to_vec();
    let consumer = DurableOutboxConsumer::open(
        &db,
        outbox_table.clone(),
        cursor_table.clone(),
        consumer_id.clone(),
    )
    .await
    .expect("open outbox consumer");
    let first_pass = consumer.poll(Some(10)).await.expect("poll outbox");
    assert_eq!(first_pass.entries.len(), 1);

    let mut delivered = BTreeSet::new();
    for message in &first_pass.entries {
        delivered.insert(message.entry.idempotency_key.clone());
    }

    let reopened = env.restart().await;
    let mut replay_consumer = DurableOutboxConsumer::open(
        &reopened,
        reopened.table("outbox"),
        reopened.table("outbox_cursors"),
        consumer_id,
    )
    .await
    .expect("reopen outbox consumer");
    let replay = replay_consumer
        .poll(Some(10))
        .await
        .expect("poll replayed outbox");
    assert_eq!(replay.entries.len(), 1);
    for message in &replay.entries {
        delivered.insert(message.entry.idempotency_key.clone());
    }
    assert_eq!(delivered.len(), 1);

    replay_consumer
        .persist_through(replay.last_cursor().expect("replayed cursor"))
        .await
        .expect("persist replayed cursor");

    let reopened_again = env.restart().await;
    let settled = DurableOutboxConsumer::open(
        &reopened_again,
        reopened_again.table("outbox"),
        reopened_again.table("outbox_cursors"),
        b"mailer".to_vec(),
    )
    .await
    .expect("open settled consumer");
    assert!(
        settled
            .poll(Some(10))
            .await
            .expect("poll after cursor persistence")
            .is_empty()
    );
}

fn simulation_config(root_path: &str, durability: TieredDurabilityMode) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: root_path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-sim".to_string(),
                prefix: "t29".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
        }),
        scheduler: None,
    }
}

#[test]
fn timer_simulation_catches_up_after_delayed_wake_and_clock_jump() -> turmoil::Result {
    SeededSimulationRunner::new(0x2929).run_with(|context| async move {
        let config = simulation_config(
            "/terracedb/sim/t29-timer-delayed-wake",
            TieredDurabilityMode::Deferred,
        );
        let db = context.open_db(config).await?;
        let schedule = db.create_table(row_table_config("timer_schedule")).await?;
        let lookup = db.create_table(row_table_config("timer_lookup")).await?;
        let timers = DurableTimerSet::new(schedule, lookup);
        let mut wake = timers.subscribe_durable(&db);

        let mut first = db.write_batch();
        timers.stage_schedule(
            &mut first,
            ScheduledTimer {
                timer_id: b"timer:a".to_vec(),
                fire_at: Timestamp::new(2),
                payload: b"a".to_vec(),
            },
        )?;
        db.commit(first, CommitOptions::default()).await?;

        let mut second = db.write_batch();
        timers.stage_schedule(
            &mut second,
            ScheduledTimer {
                timer_id: b"timer:b".to_vec(),
                fire_at: Timestamp::new(4),
                payload: b"b".to_vec(),
            },
        )?;
        let second_sequence = db.commit(second, CommitOptions::default()).await?;

        context.clock().sleep(Duration::from_millis(10)).await;
        assert!(
            timers
                .scan_due_durable(&db, context.clock().now(), None)
                .await?
                .is_empty()
        );

        db.flush().await?;
        assert_eq!(wake.changed().await.expect("durable wake"), second_sequence);

        let due = timers
            .scan_due_durable(&db, context.clock().now(), None)
            .await?;
        assert_eq!(due.timers.len(), 2);
        assert_eq!(
            due.timers
                .iter()
                .map(|timer| timer.timer_id.clone())
                .collect::<Vec<_>>(),
            vec![b"timer:a".to_vec(), b"timer:b".to_vec()]
        );

        Ok(())
    })
}

#[test]
fn outbox_simulation_restarts_and_catches_up_without_cursor_persistence() -> turmoil::Result {
    SeededSimulationRunner::new(0x2930).run_with(|context| async move {
        let config = simulation_config(
            "/terracedb/sim/t29-outbox-restart",
            TieredDurabilityMode::Deferred,
        );
        let db = context.open_db(config.clone()).await?;
        let outbox_table = db.create_table(row_table_config("outbox")).await?;
        let cursor_table = db.create_table(row_table_config("outbox_cursors")).await?;
        let outbox = TransactionalOutbox::new(outbox_table.clone());
        let mut wake = outbox.subscribe_durable(&db);

        let mut first = db.write_batch();
        outbox.stage_entry(
            &mut first,
            OutboxEntry {
                outbox_id: b"order:1:email".to_vec(),
                idempotency_key: "order:1:email".to_string(),
                payload: b"email".to_vec(),
            },
        )?;
        db.commit(first, CommitOptions::default()).await?;

        let mut second = db.write_batch();
        outbox.stage_entry(
            &mut second,
            OutboxEntry {
                outbox_id: b"order:1:ship".to_vec(),
                idempotency_key: "order:1:ship".to_string(),
                payload: b"ship".to_vec(),
            },
        )?;
        let second_sequence = db.commit(second, CommitOptions::default()).await?;

        db.flush().await?;
        assert_eq!(wake.changed().await.expect("durable wake"), second_sequence);

        let consumer_id = b"shipper".to_vec();
        let consumer = DurableOutboxConsumer::open(
            &db,
            outbox_table.clone(),
            cursor_table.clone(),
            consumer_id.clone(),
        )
        .await?;
        let initial = consumer.poll(Some(10)).await?;
        assert_eq!(initial.entries.len(), 2);

        let reopened = context
            .restart_db(config.clone(), CutPoint::AfterStep)
            .await?;
        let mut replay_consumer = DurableOutboxConsumer::open(
            &reopened,
            reopened.table("outbox"),
            reopened.table("outbox_cursors"),
            consumer_id,
        )
        .await?;
        let replay = replay_consumer.poll(Some(10)).await?;
        assert_eq!(replay.entries.len(), 2);
        assert_eq!(
            replay
                .entries
                .iter()
                .map(|entry| entry.entry.idempotency_key.clone())
                .collect::<Vec<_>>(),
            vec!["order:1:email".to_string(), "order:1:ship".to_string()]
        );

        replay_consumer
            .persist_through(replay.last_cursor().expect("replay cursor"))
            .await?;

        let reopened_again = context.restart_db(config, CutPoint::AfterStep).await?;
        let settled = DurableOutboxConsumer::open(
            &reopened_again,
            reopened_again.table("outbox"),
            reopened_again.table("outbox_cursors"),
            b"shipper".to_vec(),
        )
        .await?;
        assert!(settled.poll(Some(10)).await?.is_empty());

        Ok(())
    })
}
