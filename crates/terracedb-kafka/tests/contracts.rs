use async_trait::async_trait;
use terracedb::{ReadError, Transaction, test_support::row_table_config};
use terracedb_kafka::{
    KafkaBatchHandler, KafkaBootstrapPolicy, KafkaBroker, KafkaFetchedBatch, KafkaFilterDecision,
    KafkaIngressDefinition, KafkaMaterializationLayout, KafkaOffset, KafkaPartitionOffset,
    KafkaPartitionSource, KafkaProgressStore, KafkaRecord, KafkaRecordFilter, KafkaSourceId,
    KafkaSourceProgress, KafkaWorkerOptions, KeepAllKafkaRecords,
};

#[derive(Clone, Debug)]
struct StubHandler;

#[async_trait]
impl KafkaBatchHandler for StubHandler {
    type Error = std::convert::Infallible;

    async fn apply_batch(
        &self,
        _tx: &mut Transaction,
        _batch: &terracedb_kafka::KafkaAdmissionBatch,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct StubFilter;

impl KafkaRecordFilter for StubFilter {
    fn classify(&self, record: &KafkaRecord) -> KafkaFilterDecision {
        if record.offset.get() % 2 == 0 {
            KafkaFilterDecision::Retain
        } else {
            KafkaFilterDecision::Skip
        }
    }
}

#[derive(Clone, Debug)]
struct StubBroker;

#[async_trait]
impl KafkaBroker for StubBroker {
    type Error = std::convert::Infallible;

    async fn resolve_offset(
        &self,
        _claim: &terracedb_kafka::KafkaPartitionClaim,
        _policy: KafkaBootstrapPolicy,
    ) -> Result<KafkaOffset, Self::Error> {
        Ok(KafkaOffset::new(0))
    }

    async fn fetch_batch(
        &self,
        _claim: &terracedb_kafka::KafkaPartitionClaim,
        _next_offset: KafkaOffset,
        _max_records: usize,
    ) -> Result<KafkaFetchedBatch, Self::Error> {
        Ok(KafkaFetchedBatch::empty(Some(KafkaOffset::new(0))))
    }
}

#[derive(Clone, Debug)]
struct StubProgressStore;

#[async_trait]
impl KafkaProgressStore for StubProgressStore {
    async fn load(
        &self,
        _source: &KafkaSourceId,
    ) -> Result<Option<KafkaSourceProgress>, ReadError> {
        Ok(None)
    }

    fn stage_persist(
        &self,
        _tx: &mut Transaction,
        _source: &KafkaSourceId,
        _progress: KafkaSourceProgress,
    ) {
    }
}

fn assert_contract_surface<B, F, H, S>(
    _definition: &KafkaIngressDefinition<F, H>,
    _broker: &B,
    _store: &S,
    _worker: KafkaWorkerOptions,
) where
    B: KafkaBroker,
    F: KafkaRecordFilter,
    H: KafkaBatchHandler,
    S: KafkaProgressStore,
{
}

#[test]
fn api_surface_compiles_without_concrete_kafka_adapter() {
    let definition = KafkaIngressDefinition::new(
        "consumer-a",
        vec![
            KafkaPartitionSource::new("orders", 0, KafkaBootstrapPolicy::Earliest),
            KafkaPartitionSource::new("orders", 1, KafkaBootstrapPolicy::Latest),
        ],
        StubHandler,
    )
    .with_filter(StubFilter)
    .with_materialization(KafkaMaterializationLayout::SharedPartitionOffsetTable)
    .with_worker_options(KafkaWorkerOptions {
        batch_limit: 32,
        ..KafkaWorkerOptions::default()
    });
    let broker = StubBroker;
    let progress_store = StubProgressStore;

    let claims = definition.claims(7);
    assert_eq!(claims.len(), 2);
    assert_eq!(claims[0].generation, 7);
    assert_eq!(definition.source_ids()[0].consumer_group, "consumer-a");

    assert_contract_surface(
        &definition,
        &broker,
        &progress_store,
        KafkaWorkerOptions::default(),
    );

    let defaulted = KafkaIngressDefinition::new(
        "consumer-b",
        vec![KafkaPartitionSource::new(
            "audit",
            3,
            KafkaBootstrapPolicy::Earliest,
        )],
        StubHandler,
    );
    let _allow_all = KeepAllKafkaRecords;
    assert_eq!(
        defaulted.materialization,
        KafkaMaterializationLayout::OneTablePerPartition
    );
}

#[test]
fn source_and_progress_round_trip_and_preserve_ordering() {
    let source = KafkaSourceId::new("consumer-a", "orders", 9);
    let encoded_source = source.encode();
    let decoded_source = KafkaSourceId::decode(&encoded_source).expect("decode source id");
    assert_eq!(decoded_source, source);

    let progress = KafkaSourceProgress::new(KafkaOffset::new(42));
    let encoded_progress = progress.encode();
    let decoded_progress =
        KafkaSourceProgress::decode(&encoded_progress).expect("decode source progress");
    assert_eq!(decoded_progress, progress);
    assert_eq!(
        decoded_progress.last_applied_offset(),
        Some(KafkaOffset::new(41))
    );

    let offsets = [
        KafkaOffset::new(0).encode().to_vec(),
        KafkaOffset::new(1).encode().to_vec(),
        KafkaOffset::new(10).encode().to_vec(),
    ];
    assert!(offsets.windows(2).all(|window| window[0] < window[1]));

    let shared_keys = [
        KafkaPartitionOffset::new(terracedb_kafka::KafkaPartition::new(0), KafkaOffset::new(7))
            .encode()
            .to_vec(),
        KafkaPartitionOffset::new(terracedb_kafka::KafkaPartition::new(0), KafkaOffset::new(8))
            .encode()
            .to_vec(),
        KafkaPartitionOffset::new(terracedb_kafka::KafkaPartition::new(1), KafkaOffset::new(0))
            .encode()
            .to_vec(),
    ];
    assert!(shared_keys.windows(2).all(|window| window[0] < window[1]));

    let _ = row_table_config("kafka_progress");
}

#[test]
fn layout_selection_uses_the_same_route_interface() {
    let record = KafkaRecord::new("orders", 4, 27);
    let per_partition = KafkaMaterializationLayout::OneTablePerPartition.route_record(&record);
    let shared = KafkaMaterializationLayout::SharedPartitionOffsetTable.route_record(&record);

    assert_eq!(per_partition.topic_partition, record.topic_partition);
    assert_eq!(shared.topic_partition, record.topic_partition);
    match per_partition.key {
        terracedb_kafka::KafkaMaterializedOrderKey::Offset(offset) => {
            assert_eq!(offset, KafkaOffset::new(27));
        }
        terracedb_kafka::KafkaMaterializedOrderKey::PartitionOffset(_) => {
            panic!("per-partition layout should only use raw offsets")
        }
    }
    match shared.key {
        terracedb_kafka::KafkaMaterializedOrderKey::PartitionOffset(partition_offset) => {
            assert_eq!(partition_offset.partition.get(), 4);
            assert_eq!(partition_offset.offset, KafkaOffset::new(27));
        }
        terracedb_kafka::KafkaMaterializedOrderKey::Offset(_) => {
            panic!("shared layout should use (partition, offset) ordering")
        }
    }
}
