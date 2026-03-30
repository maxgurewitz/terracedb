use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    hash::{Hash, Hasher},
    ops::Bound,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crc32fast::hash as checksum32;
use futures::{Stream, stream};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tracing::Instrument;

use crate::{
    config::{
        CompactionDecision, CompactionDecisionContext, CompactionStrategy, DbConfig, S3Location,
        S3PrimaryStorageConfig, StorageConfig, TableConfig, TableFormat, TableMetadata,
        TieredDurabilityMode, TieredStorageConfig,
    },
    engine::commit_log::{
        CommitEntry, CommitRecord, LocalSegmentScanPlan, SegmentFooter, SegmentManager,
        SegmentOptions, SegmentRecordScanner, encode_segment_bytes, segment_footer_from_bytes,
    },
    error::{
        ChangeFeedError, CommitError, CreateTableError, FlushError, OpenError, ReadError,
        SnapshotTooOld, StorageError, StorageErrorKind, SubscriptionClosed, WriteError,
    },
    ids::{CommitId, FieldId, LogCursor, ManifestId, SegmentId, SequenceNumber, TableId},
    io::{DbDependencies, FileHandle, OpenOptions},
    metadata_flatbuffers as metadata_fb,
    remote::{ObjectKeyLayout, RemoteCache, StorageSource, UnifiedStorage},
    scheduler::{
        DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT, PendingWork, PendingWorkType, RoundRobinScheduler,
        ScheduleAction, Scheduler, TableStats,
    },
    telemetry::{
        OperationContext, db_instance_from_storage, db_name_from_storage, storage_mode_name,
    },
};

pub type Key = Vec<u8>;
pub type KeyPrefix = Vec<u8>;
pub type KvStream = Pin<Box<dyn Stream<Item = (Key, Value)> + Send + 'static>>;
pub type ChangeStream =
    Pin<Box<dyn Stream<Item = Result<ChangeEntry, StorageError>> + Send + 'static>>;

fn log_cursor_attribute(cursor: LogCursor) -> String {
    format!("{}:{}", cursor.sequence().get(), cursor.op_index())
}

fn apply_db_span_attributes(
    span: &tracing::Span,
    db_name: &str,
    db_instance: &str,
    storage_mode: &str,
) {
    crate::set_span_attributes(
        span,
        [
            (
                crate::telemetry_attrs::DB_NAME,
                opentelemetry::Value::String(db_name.to_string().into()),
            ),
            (
                crate::telemetry_attrs::DB_INSTANCE,
                opentelemetry::Value::String(db_instance.to_string().into()),
            ),
            (
                crate::telemetry_attrs::STORAGE_MODE,
                opentelemetry::Value::String(storage_mode.to_string().into()),
            ),
        ],
    );
}

fn apply_table_span_attribute(span: &tracing::Span, table_name: &str) {
    crate::set_span_attribute(
        span,
        crate::telemetry_attrs::TABLE,
        opentelemetry::Value::String(table_name.to_string().into()),
    );
}

const CHANGE_FEED_READ_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub struct Db {
    inner: Arc<DbInner>,
}

include!("schema.rs");
include!("operations.rs");
include!("watermark.rs");
include!("internals.rs");
include!("metadata_codec.rs");
include!("memtable.rs");
include!("builder.rs");
include!("db_open.rs");
include!("sstable_io.rs");
include!("maintenance.rs");
include!("db_api.rs");
include!("table.rs");
include!("snapshot.rs");
include!("util.rs");
#[cfg(test)]
mod property_tests;
#[cfg(test)]
mod tests;
