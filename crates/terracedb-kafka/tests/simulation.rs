use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use futures::StreamExt;
use terracedb::{
    Key, ReadError, ScanOptions, Table, Transaction, Value,
    test_support::{row_table_config, tiered_test_config},
};
use terracedb_kafka::{
    KafkaAdmissionBatch, KafkaBatchHandler, KafkaBootstrapPolicy, KafkaBroker, KafkaFetchedBatch,
    KafkaFilterDecision, KafkaMaterializationLayout, KafkaOffset, KafkaPartitionClaim,
    KafkaPartitionSource, KafkaPartitionTelemetrySnapshot, KafkaProgressStore, KafkaRecord,
    KafkaRecordFilter, KafkaRuntimeEvent, KafkaRuntimeObserver, KafkaSimulationSeam,
    KafkaSourceProgress, KafkaWorkerOptions, NoopKafkaTelemetrySink, TableKafkaProgressStore,
    drive_partition_once,
};
use terracedb_simulation::{CutPoint, SeededSimulationRunner};

#[derive(Clone, Debug)]
struct ScriptedBroker {
    earliest: KafkaOffset,
    latest: KafkaOffset,
    batches: Arc<BTreeMap<u64, KafkaFetchedBatch>>,
}

impl ScriptedBroker {
    fn new(
        earliest: KafkaOffset,
        latest: KafkaOffset,
        batches: BTreeMap<u64, KafkaFetchedBatch>,
    ) -> Self {
        Self {
            earliest,
            latest,
            batches: Arc::new(batches),
        }
    }
}

#[async_trait]
impl KafkaBroker for ScriptedBroker {
    type Error = std::convert::Infallible;

    async fn resolve_offset(
        &self,
        _claim: &KafkaPartitionClaim,
        policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error> {
        Ok(match policy {
            KafkaBootstrapPolicy::Earliest => self.earliest,
            KafkaBootstrapPolicy::Latest => self.latest,
        })
    }

    async fn fetch_batch(
        &self,
        _claim: &KafkaPartitionClaim,
        next_offset: KafkaOffset,
        max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error> {
        let batch = self
            .batches
            .get(&next_offset.get())
            .cloned()
            .unwrap_or_else(|| KafkaFetchedBatch::empty(Some(self.latest)));
        let records = batch.records.into_iter().take(max_records).collect();
        Ok(KafkaFetchedBatch::new(records, batch.high_watermark))
    }
}

#[derive(Clone)]
struct LayoutWriter {
    table: Table,
    layout: KafkaMaterializationLayout,
}

#[async_trait]
impl KafkaBatchHandler for LayoutWriter {
    type Error = std::convert::Infallible;

    async fn apply_batch(
        &self,
        tx: &mut Transaction,
        batch: &KafkaAdmissionBatch,
    ) -> Result<(), Self::Error> {
        for record in &batch.retained_records {
            let route = self.layout.route_record(record);
            tx.write(
                &self.table,
                route.key.encode(),
                Value::bytes(record.value.clone().unwrap_or_default()),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct KeepEvenOffsets;

impl KafkaRecordFilter for KeepEvenOffsets {
    fn classify(&self, record: &KafkaRecord) -> KafkaFilterDecision {
        if record.offset.get() % 2 == 0 {
            KafkaFilterDecision::Retain
        } else {
            KafkaFilterDecision::Skip
        }
    }
}

#[derive(Clone, Default)]
struct RecordingObserver {
    events: Arc<Mutex<Vec<KafkaRuntimeEvent>>>,
}

impl RecordingObserver {
    fn events(&self) -> Vec<KafkaRuntimeEvent> {
        self.events.lock().expect("lock events").clone()
    }
}

impl KafkaRuntimeObserver for RecordingObserver {
    fn on_event(&self, event: KafkaRuntimeEvent) {
        self.events.lock().expect("lock events").push(event);
    }
}

#[derive(Clone, Default)]
struct RecordingTelemetrySink {
    snapshots: Arc<Mutex<Vec<KafkaPartitionTelemetrySnapshot>>>,
}

impl RecordingTelemetrySink {
    fn snapshots(&self) -> Vec<KafkaPartitionTelemetrySnapshot> {
        self.snapshots.lock().expect("lock snapshots").clone()
    }
}

impl terracedb_kafka::KafkaTelemetrySink for RecordingTelemetrySink {
    fn record_snapshot(&self, snapshot: KafkaPartitionTelemetrySnapshot) {
        self.snapshots
            .lock()
            .expect("lock snapshots")
            .push(snapshot);
    }
}

async fn collect_rows(table: &Table) -> Result<Vec<(Key, Vec<u8>)>, ReadError> {
    let mut rows = table
        .scan(Vec::new(), vec![0xff], ScanOptions::default())
        .await?;
    let mut out = Vec::new();
    while let Some((key, value)) = rows.next().await {
        let Value::Bytes(bytes) = value else {
            panic!("kafka simulation only expects byte rows");
        };
        out.push((key, bytes));
    }
    Ok(out)
}

fn record(topic: &str, partition: u32, offset: u64, payload: &str) -> KafkaRecord {
    let mut record = KafkaRecord::new(topic, partition, offset);
    record.value = Some(payload.as_bytes().to_vec());
    record
}

#[test]
fn worker_runtime_can_restart_from_persisted_progress_in_simulation() -> turmoil::Result {
    SeededSimulationRunner::new(0x88_01).run_with(|context| async move {
        let config = tiered_test_config("/kafka/t88/restart");
        let db = context.open_db(config.clone()).await?;
        let output = db.create_table(row_table_config("ingest")).await?;
        let progress_table = db.create_table(row_table_config("kafka_progress")).await?;
        let progress_store = TableKafkaProgressStore::new(progress_table.clone());
        let source = KafkaPartitionSource::new("orders", 2, KafkaBootstrapPolicy::Earliest);
        let claim = KafkaPartitionClaim::new(source.source_id("consumer-a"), source.bootstrap, 1);

        let broker = ScriptedBroker::new(
            KafkaOffset::new(0),
            KafkaOffset::new(3),
            BTreeMap::from([(
                0,
                KafkaFetchedBatch::new(
                    vec![
                        record("orders", 2, 0, "a"),
                        record("orders", 2, 1, "b"),
                        record("orders", 2, 2, "c"),
                    ],
                    Some(KafkaOffset::new(3)),
                ),
            )]),
        );
        let observer = RecordingObserver::default();
        let telemetry = RecordingTelemetrySink::default();
        let handler = LayoutWriter {
            table: output.clone(),
            layout: KafkaMaterializationLayout::OneTablePerPartition,
        };

        let first = drive_partition_once(
            &db,
            &broker,
            &progress_store,
            &claim,
            KafkaWorkerOptions::default(),
            &terracedb_kafka::KeepAllKafkaRecords,
            &handler,
            &observer,
            &telemetry,
        )
        .await?;
        assert_eq!(first.telemetry.next_offset, KafkaOffset::new(3));
        assert_eq!(first.retained_offsets.len(), 3);
        assert_eq!(
            progress_store.load(&claim.source).await?,
            Some(KafkaSourceProgress::new(KafkaOffset::new(3)))
        );
        assert_eq!(collect_rows(&output).await?.len(), 3);

        db.flush().await?;
        let reopened = context
            .restart_db(config.clone(), CutPoint::AfterDurabilityBoundary)
            .await?;
        let reopened_output = reopened.table("ingest");
        let reopened_progress = TableKafkaProgressStore::new(reopened.table("kafka_progress"));

        let second = drive_partition_once(
            &reopened,
            &broker,
            &reopened_progress,
            &claim,
            KafkaWorkerOptions::default(),
            &terracedb_kafka::KeepAllKafkaRecords,
            &LayoutWriter {
                table: reopened_output.clone(),
                layout: KafkaMaterializationLayout::OneTablePerPartition,
            },
            &observer,
            &NoopKafkaTelemetrySink,
        )
        .await?;
        assert_eq!(
            second.telemetry.start_position.next_offset(),
            KafkaOffset::new(3)
        );
        assert_eq!(second.telemetry.committed_sequence, None);
        assert_eq!(collect_rows(&reopened_output).await?.len(), 3);

        let events = observer.events();
        assert!(events.iter().any(|event| matches!(
            event,
            KafkaRuntimeEvent::SimulationSeam(KafkaSimulationSeam::RestartFromOffset {
                source,
                next_offset
            }) if *source == claim.source && *next_offset == KafkaOffset::new(3)
        )));
        assert_eq!(telemetry.snapshots().len(), 1);
        Ok(())
    })
}

#[test]
fn filtered_records_advance_progress_as_intentional_skips() -> turmoil::Result {
    SeededSimulationRunner::new(0x88_02).run_with(|context| async move {
        let db = context
            .open_db(tiered_test_config("/kafka/t88/filtered"))
            .await?;
        let output = db.create_table(row_table_config("ingest")).await?;
        let progress_table = db.create_table(row_table_config("kafka_progress")).await?;
        let progress_store = TableKafkaProgressStore::new(progress_table);
        let source = KafkaPartitionSource::new("orders", 4, KafkaBootstrapPolicy::Earliest);
        let claim = KafkaPartitionClaim::new(source.source_id("consumer-b"), source.bootstrap, 9);
        let observer = RecordingObserver::default();
        let telemetry = RecordingTelemetrySink::default();

        let broker = ScriptedBroker::new(
            KafkaOffset::new(0),
            KafkaOffset::new(3),
            BTreeMap::from([(
                0,
                KafkaFetchedBatch::new(
                    vec![
                        record("orders", 4, 0, "keep-0"),
                        record("orders", 4, 1, "skip-1"),
                        record("orders", 4, 2, "keep-2"),
                    ],
                    Some(KafkaOffset::new(3)),
                ),
            )]),
        );
        let handler = LayoutWriter {
            table: output.clone(),
            layout: KafkaMaterializationLayout::SharedPartitionOffsetTable,
        };

        let outcome = drive_partition_once(
            &db,
            &broker,
            &progress_store,
            &claim,
            KafkaWorkerOptions::default(),
            &KeepEvenOffsets,
            &handler,
            &observer,
            &telemetry,
        )
        .await?;

        assert_eq!(
            outcome.retained_offsets,
            vec![KafkaOffset::new(0), KafkaOffset::new(2)]
        );
        assert_eq!(outcome.skipped_offsets, vec![KafkaOffset::new(1)]);
        assert_eq!(outcome.telemetry.next_offset, KafkaOffset::new(3));
        assert_eq!(outcome.telemetry.skipped_records, 1);
        assert_eq!(
            progress_store.load(&claim.source).await?,
            Some(KafkaSourceProgress::new(KafkaOffset::new(3)))
        );

        let rows = collect_rows(&output).await?;
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.into_iter().map(|(_, value)| value).collect::<Vec<_>>(),
            vec![b"keep-0".to_vec(), b"keep-2".to_vec()]
        );

        let events = observer.events();
        assert!(events.iter().any(|event| matches!(
            event,
            KafkaRuntimeEvent::SimulationSeam(KafkaSimulationSeam::FilteredButAcknowledged {
                source,
                offset
            }) if *source == claim.source && *offset == KafkaOffset::new(1)
        )));

        let snapshots = telemetry.snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].retained_records, 2);
        assert_eq!(snapshots[0].lag_records, Some(0));
        Ok(())
    })
}
