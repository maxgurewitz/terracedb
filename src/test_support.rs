use std::{
    collections::BTreeMap, env, fs, io::ErrorKind, path::PathBuf, sync::Arc, time::Duration,
};

use crate::{
    Clock, CompactionStrategy, Db, DbConfig, DbDependencies, S3Location, SsdConfig, StorageConfig,
    StubClock, StubFileSystem, StubObjectStore, StubRng, TableConfig, TableFormat,
    TieredDurabilityMode, TieredLocalRetentionMode, TieredStorageConfig, Value,
};
use tokio::task::JoinHandle;

pub use crate::failpoints::{
    FailpointAction, FailpointHandle, FailpointHit, FailpointMode, FailpointOutcome,
    FailpointRegistry, names as failpoint_names,
};

/// Attaches a shared failpoint registry to a dependency set so DB opens, async
/// tests, and simulation helpers can arm the same named cut points.
pub fn attach_failpoint_registry(dependencies: &DbDependencies, registry: Arc<FailpointRegistry>) {
    dependencies.__attach_failpoint_registry(registry);
}

/// Returns the registry currently attached to a DB's dependency graph.
pub fn db_failpoint_registry(db: &Db) -> Arc<FailpointRegistry> {
    db.__failpoint_registry()
}

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
            local_retention: TieredLocalRetentionMode::Offload,
        }),
        hybrid_read: Default::default(),
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

/// Waits for a failpoint hit while advancing a stub clock in bounded steps.
pub async fn wait_for_failpoint_hit_with_clock(
    handle: &crate::FailpointHandle,
    clock: &StubClock,
    step: Duration,
    max_steps: usize,
) -> crate::FailpointHit {
    let mut wait = Box::pin(handle.next_hit());
    for _ in 0..max_steps {
        tokio::select! {
            hit = &mut wait => return hit,
            _ = tokio::task::yield_now() => {
                clock.advance(step);
            }
        }
    }

    panic!("failpoint was not hit within {max_steps} virtual-clock advances");
}

/// Advances a stub clock until a spawned task finishes or the bound is hit.
pub async fn advance_clock_until_finished<T>(
    clock: &StubClock,
    handle: &JoinHandle<T>,
    step: Duration,
    max_steps: usize,
) -> u64 {
    let start = clock.now().get();
    for _ in 0..max_steps {
        if handle.is_finished() {
            return clock.now().get().saturating_sub(start);
        }
        clock.advance(step);
        tokio::task::yield_now().await;
    }

    panic!("task was not finished within {max_steps} virtual-clock advances");
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

pub fn durable_format_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/durable-formats")
}

pub fn durable_format_fixture_path(name: &str) -> PathBuf {
    durable_format_fixture_dir().join(name)
}

pub fn durable_format_fixture_regeneration_requested() -> bool {
    env::var("TERRACEDB_REGENERATE_DURABLE_FIXTURES")
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}

pub fn assert_durable_format_fixture(name: &str, actual: &[u8]) {
    let path = durable_format_fixture_path(name);
    match fs::read(&path) {
        Ok(expected) if expected == actual => {}
        Ok(_) => {
            if durable_format_fixture_regeneration_requested() {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).unwrap_or_else(|create_error| {
                        panic!(
                            "failed to create durable format fixture directory {}: {create_error}",
                            parent.display()
                        )
                    });
                }
                fs::write(&path, actual).unwrap_or_else(|write_error| {
                    panic!(
                        "failed to write durable format fixture {}: {write_error}",
                        path.display()
                    )
                });
                return;
            }

            panic!(
                "durable format fixture {} is missing or out of date; run `scripts/regenerate-durable-format-fixtures.sh` if this change is intentional",
                path.display()
            );
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            if durable_format_fixture_regeneration_requested() {
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent).unwrap_or_else(|create_error| {
                        panic!(
                            "failed to create durable format fixture directory {}: {create_error}",
                            parent.display()
                        )
                    });
                }
                fs::write(&path, actual).unwrap_or_else(|write_error| {
                    panic!(
                        "failed to write durable format fixture {}: {write_error}",
                        path.display()
                    )
                });
                return;
            }

            panic!(
                "durable format fixture {} is missing or out of date; run `scripts/regenerate-durable-format-fixtures.sh` if this change is intentional",
                path.display()
            );
        }
        Err(error) => {
            panic!(
                "failed to read durable format fixture {}: {error}",
                path.display()
            );
        }
    }
}

pub fn durable_format_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for &byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
