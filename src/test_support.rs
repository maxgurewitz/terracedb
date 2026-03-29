use std::{collections::BTreeMap, sync::Arc};

use crate::{
    CompactionStrategy, DbConfig, DbDependencies, S3Location, SsdConfig, StorageConfig, StubClock,
    StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat, TieredDurabilityMode,
    TieredStorageConfig, Value,
};

pub fn tiered_test_config(path: &str) -> DbConfig {
    tiered_test_config_with_durability(path, TieredDurabilityMode::GroupCommit)
}

pub fn tiered_test_config_with_durability(
    path: &str,
    durability: TieredDurabilityMode,
) -> DbConfig {
    DbConfig {
        storage: StorageConfig::Tiered(TieredStorageConfig {
            ssd: SsdConfig {
                path: path.to_string(),
            },
            s3: S3Location {
                bucket: "terracedb-test".to_string(),
                prefix: "tiered".to_string(),
            },
            max_local_bytes: 1024 * 1024,
            durability,
        }),
        scheduler: None,
    }
}

pub fn test_dependencies(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
) -> DbDependencies {
    test_dependencies_with_clock(file_system, object_store, Arc::new(StubClock::default()))
}

pub fn test_dependencies_with_clock(
    file_system: Arc<StubFileSystem>,
    object_store: Arc<StubObjectStore>,
    clock: Arc<StubClock>,
) -> DbDependencies {
    DbDependencies::new(
        file_system,
        object_store,
        clock,
        Arc::new(StubRng::seeded(7)),
    )
}

pub fn row_table_config(name: &str) -> TableConfig {
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
        metadata: BTreeMap::new(),
    }
}

pub fn bytes(value: &str) -> Value {
    Value::bytes(value.as_bytes().to_vec())
}
