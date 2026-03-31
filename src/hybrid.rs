use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    DbDependencies, FieldId, FieldType, FieldValue, Key, RemoteCache, SequenceNumber, StorageError,
    StorageSource, TableId,
};

pub const COLUMNAR_V2_BASE_PART_FORMAT_VERSION: u32 = 2;
pub const COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridReadConfig {
    pub raw_segment_cache_bytes: u64,
    pub decoded_metadata_cache_entries: usize,
    pub decoded_column_cache_entries: usize,
    pub zone_maps_in_base_part: bool,
    pub skip_indexes_enabled: bool,
    pub projection_sidecars_enabled: bool,
    pub aggressive_background_repair: bool,
    pub compact_to_wide_promotion_enabled: bool,
}

impl Default for HybridReadConfig {
    fn default() -> Self {
        Self {
            raw_segment_cache_bytes: 16 * 1024 * 1024,
            decoded_metadata_cache_entries: 256,
            decoded_column_cache_entries: 1024,
            zone_maps_in_base_part: true,
            skip_indexes_enabled: false,
            projection_sidecars_enabled: false,
            aggressive_background_repair: false,
            compact_to_wide_promotion_enabled: false,
        }
    }
}

impl HybridReadConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.raw_segment_cache_bytes == 0 {
            return Err("hybrid read config requires raw_segment_cache_bytes > 0".to_string());
        }
        if self.decoded_metadata_cache_entries == 0 {
            return Err(
                "hybrid read config requires decoded_metadata_cache_entries > 0".to_string(),
            );
        }
        if self.decoded_column_cache_entries == 0 {
            return Err("hybrid read config requires decoded_column_cache_entries > 0".to_string());
        }
        if !self.zone_maps_in_base_part {
            return Err("zone maps are part of the columnar-v2 base format".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub const fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    pub fn is_empty(self) -> bool {
        self.start >= self.end
    }

    pub fn contains(self, other: Self) -> bool {
        self.start <= other.start && self.end >= other.end
    }

    pub fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }

    pub fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then_some(Self { start, end })
    }

    pub fn to_std(self) -> std::ops::Range<u64> {
        self.start..self.end
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarV2ArtifactKind {
    BasePart,
    SynopsisSidecar,
    SkipIndexSidecar,
    ProjectionSidecar,
    CompactDigest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2FormatTag {
    pub kind: ColumnarV2ArtifactKind,
    pub format_version: u32,
    pub min_reader_version: u32,
    pub max_writer_version: u32,
}

impl ColumnarV2FormatTag {
    pub const fn base_part() -> Self {
        Self {
            kind: ColumnarV2ArtifactKind::BasePart,
            format_version: COLUMNAR_V2_BASE_PART_FORMAT_VERSION,
            min_reader_version: COLUMNAR_V2_BASE_PART_FORMAT_VERSION,
            max_writer_version: COLUMNAR_V2_BASE_PART_FORMAT_VERSION,
        }
    }

    pub const fn synopsis_sidecar() -> Self {
        Self {
            kind: ColumnarV2ArtifactKind::SynopsisSidecar,
            format_version: COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn skip_index_sidecar() -> Self {
        Self {
            kind: ColumnarV2ArtifactKind::SkipIndexSidecar,
            format_version: COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn projection_sidecar() -> Self {
        Self {
            kind: ColumnarV2ArtifactKind::ProjectionSidecar,
            format_version: COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn compact_digest() -> Self {
        Self {
            kind: ColumnarV2ArtifactKind::CompactDigest,
            format_version: COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION,
            min_reader_version: COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION,
            max_writer_version: COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarV2Encoding {
    Plain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarV2Compression {
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarV2SubstreamKind {
    KeyIndex,
    Sequence,
    TombstoneBitmap,
    RowKind,
    DefinitionLevels,
    PresentBitmap,
    Int64Values,
    Float64Bits,
    StringOffsets,
    StringData,
    BytesOffsets,
    BytesData,
    BoolValues,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2SubstreamRef {
    pub ordinal: u32,
    pub field_id: Option<FieldId>,
    pub field_type: Option<FieldType>,
    pub kind: ColumnarV2SubstreamKind,
    pub encoding: ColumnarV2Encoding,
    pub compression: ColumnarV2Compression,
    pub range: ByteRange,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2MarkOffset {
    pub substream_ordinal: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2Mark {
    pub granule_index: u32,
    pub page_index: u32,
    pub row_ordinal: u64,
    pub offsets: Vec<ColumnarV2MarkOffset>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ZoneMapSynopsis {
    pub field_id: FieldId,
    pub min_value: Option<FieldValue>,
    pub max_value: Option<FieldValue>,
    pub null_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnarGranuleSynopsis {
    pub granule_index: u32,
    pub row_range: ByteRange,
    pub zone_maps: Vec<ZoneMapSynopsis>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnarSynopsisSidecar {
    pub format_tag: ColumnarV2FormatTag,
    pub part_local_id: String,
    pub granules: Vec<ColumnarGranuleSynopsis>,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipIndexSidecarDescriptor {
    pub format_tag: ColumnarV2FormatTag,
    pub part_local_id: String,
    pub index_name: String,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSidecarDescriptor {
    pub format_tag: ColumnarV2FormatTag,
    pub part_local_id: String,
    pub projection_name: String,
    pub projected_fields: Vec<FieldId>,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ColumnarOptionalSidecar {
    SkipIndex(SkipIndexSidecarDescriptor),
    Projection(ProjectionSidecarDescriptor),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartDigestAlgorithm {
    Crc32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactPartDigest {
    pub format_tag: ColumnarV2FormatTag,
    pub algorithm: PartDigestAlgorithm,
    pub logical_bytes: u64,
    pub digest_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2Header {
    pub format_tag: ColumnarV2FormatTag,
    pub table_id: TableId,
    pub local_id: String,
    pub schema_version: u32,
    pub part_digest: CompactPartDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2PageRef {
    pub substream_ordinal: u32,
    pub page_ordinal: u32,
    pub range: ByteRange,
    pub row_range: ByteRange,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarV2PageDirectory {
    pub pages: Vec<ColumnarV2PageRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnarV2Footer {
    pub format_tag: ColumnarV2FormatTag,
    pub table_id: TableId,
    pub local_id: String,
    pub schema_version: u32,
    pub row_count: u64,
    pub data_range: ByteRange,
    pub substreams: Vec<ColumnarV2SubstreamRef>,
    pub marks: Vec<ColumnarV2Mark>,
    pub synopsis: ColumnarSynopsisSidecar,
    pub optional_sidecars: Vec<ColumnarOptionalSidecar>,
    pub digests: Vec<CompactPartDigest>,
}

#[async_trait]
pub trait ColumnarFooterPageDirectoryLoader: Send + Sync {
    async fn load_header(
        &self,
        source: &StorageSource,
    ) -> Result<Arc<ColumnarV2Header>, StorageError>;

    async fn load_footer(
        &self,
        source: &StorageSource,
    ) -> Result<Arc<ColumnarV2Footer>, StorageError>;

    async fn load_page_directory(
        &self,
        source: &StorageSource,
        footer: &ColumnarV2Footer,
    ) -> Result<Arc<ColumnarV2PageDirectory>, StorageError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridRowRef {
    pub table_id: TableId,
    pub local_id: String,
    pub key: Key,
    pub sequence: SequenceNumber,
    pub row_ordinal: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowProjection {
    FullRow,
    Fields(Vec<FieldId>),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelectionMask {
    pub selected: Vec<bool>,
}

impl SelectionMask {
    pub fn all(len: usize) -> Self {
        Self {
            selected: vec![true; len],
        }
    }

    pub fn from_selected_positions(len: usize, positions: &[usize]) -> Self {
        let mut selected = vec![false; len];
        for &position in positions {
            if let Some(slot) = selected.get_mut(position) {
                *slot = true;
            }
        }
        Self { selected }
    }

    pub fn len(&self) -> usize {
        self.selected.len()
    }

    pub fn is_empty(&self) -> bool {
        self.selected.is_empty()
    }

    pub fn selected_count(&self) -> usize {
        self.selected.iter().filter(|selected| **selected).count()
    }

    pub fn is_selected(&self, position: usize) -> bool {
        self.selected.get(position).copied().unwrap_or(false)
    }

    pub fn intersect(&self, other: &Self) -> Result<Self, StorageError> {
        if self.len() != other.len() {
            return Err(StorageError::unsupported(
                "selection masks must have the same length",
            ));
        }
        Ok(Self {
            selected: self
                .selected
                .iter()
                .zip(&other.selected)
                .map(|(left, right)| *left && *right)
                .collect(),
        })
    }

    pub fn survivors(&self) -> SurvivorSet {
        SurvivorSet {
            row_positions: self
                .selected
                .iter()
                .enumerate()
                .filter_map(|(index, selected)| selected.then_some(index))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurvivorSet {
    pub row_positions: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowRefBatch {
    pub projection: RowProjection,
    pub rows: Vec<HybridRowRef>,
    pub selection: SelectionMask,
    pub survivors: SurvivorSet,
}

impl RowRefBatch {
    pub fn new(
        rows: Vec<HybridRowRef>,
        projection: RowProjection,
        selection: Option<SelectionMask>,
    ) -> Result<Self, StorageError> {
        let selection = selection.unwrap_or_else(|| SelectionMask::all(rows.len()));
        if selection.len() != rows.len() {
            return Err(StorageError::unsupported(
                "row-ref batch selection length must match the row count",
            ));
        }
        let survivors = selection.survivors();
        Ok(Self {
            projection,
            rows,
            selection,
            survivors,
        })
    }
}

#[async_trait]
pub trait RowRefBatchIterator: Send {
    async fn next_batch(&mut self, max_rows: usize) -> Result<Option<RowRefBatch>, StorageError>;
}

#[async_trait]
pub trait RawByteSegmentCache: Send + Sync {
    async fn lookup(
        &self,
        object_key: &str,
        range: ByteRange,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    async fn fill(
        &self,
        object_key: &str,
        range: ByteRange,
        bytes: &[u8],
    ) -> Result<(), StorageError>;
}

#[async_trait]
impl RawByteSegmentCache for RemoteCache {
    async fn lookup(
        &self,
        object_key: &str,
        range: ByteRange,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.read_range(object_key, range.to_std()).await
    }

    async fn fill(
        &self,
        object_key: &str,
        range: ByteRange,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.store(
            object_key,
            crate::CacheSpan::Range {
                start: range.start,
                end: range.end,
            },
            bytes,
        )
        .await
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HybridPartDescriptor {
    pub table_id: TableId,
    pub local_id: String,
    pub source: StorageSource,
    pub format_tag: ColumnarV2FormatTag,
    pub digests: Vec<CompactPartDigest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairState {
    Verified,
    Quarantined { reason: String },
    Repaired,
    Deferred,
}

#[async_trait]
pub trait PartRepairController: Send + Sync {
    async fn verify(&self, part: &HybridPartDescriptor) -> Result<RepairState, StorageError>;

    async fn quarantine(
        &self,
        part: &HybridPartDescriptor,
        reason: &str,
    ) -> Result<RepairState, StorageError>;

    async fn repair(&self, part: &HybridPartDescriptor) -> Result<RepairState, StorageError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactToWidePromotionCandidate {
    pub table_id: TableId,
    pub local_id: String,
    pub row_count: u64,
    pub projected_read_count: u64,
    pub full_row_read_count: u64,
    pub projected_bytes_read: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactToWidePromotionDecision {
    KeepCompact,
    PromoteWide,
}

pub trait CompactToWidePromotionPolicy: Send + Sync {
    fn decide(&self, candidate: &CompactToWidePromotionCandidate)
    -> CompactToWidePromotionDecision;
}

#[derive(Clone, Debug)]
pub struct HybridRuntimeContext {
    pub dependencies: DbDependencies,
    pub config: HybridReadConfig,
}

impl HybridRuntimeContext {
    pub fn new(dependencies: DbDependencies, config: HybridReadConfig) -> Result<Self, String> {
        config.validate()?;
        Ok(Self {
            dependencies,
            config,
        })
    }
}

#[derive(Clone, Debug)]
pub struct StubColumnarFooterPageDirectoryLoader {
    header: Arc<ColumnarV2Header>,
    footer: Arc<ColumnarV2Footer>,
    page_directory: Arc<ColumnarV2PageDirectory>,
}

impl StubColumnarFooterPageDirectoryLoader {
    pub fn new(
        header: ColumnarV2Header,
        footer: ColumnarV2Footer,
        page_directory: ColumnarV2PageDirectory,
    ) -> Self {
        Self {
            header: Arc::new(header),
            footer: Arc::new(footer),
            page_directory: Arc::new(page_directory),
        }
    }
}

#[async_trait]
impl ColumnarFooterPageDirectoryLoader for StubColumnarFooterPageDirectoryLoader {
    async fn load_header(
        &self,
        _source: &StorageSource,
    ) -> Result<Arc<ColumnarV2Header>, StorageError> {
        Ok(self.header.clone())
    }

    async fn load_footer(
        &self,
        _source: &StorageSource,
    ) -> Result<Arc<ColumnarV2Footer>, StorageError> {
        Ok(self.footer.clone())
    }

    async fn load_page_directory(
        &self,
        _source: &StorageSource,
        _footer: &ColumnarV2Footer,
    ) -> Result<Arc<ColumnarV2PageDirectory>, StorageError> {
        Ok(self.page_directory.clone())
    }
}

#[derive(Default)]
pub struct VecRowRefBatchIterator {
    batches: VecDeque<RowRefBatch>,
}

impl VecRowRefBatchIterator {
    pub fn new(batches: impl IntoIterator<Item = RowRefBatch>) -> Self {
        Self {
            batches: batches.into_iter().collect(),
        }
    }
}

#[async_trait]
impl RowRefBatchIterator for VecRowRefBatchIterator {
    async fn next_batch(&mut self, _max_rows: usize) -> Result<Option<RowRefBatch>, StorageError> {
        Ok(self.batches.pop_front())
    }
}

#[derive(Debug, Default)]
pub struct InMemoryRawByteSegmentCache {
    entries: Mutex<BTreeMap<(String, ByteRange), Vec<u8>>>,
}

#[async_trait]
impl RawByteSegmentCache for InMemoryRawByteSegmentCache {
    async fn lookup(
        &self,
        object_key: &str,
        range: ByteRange,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .entries
            .lock()
            .get(&(object_key.to_string(), range))
            .cloned())
    }

    async fn fill(
        &self,
        object_key: &str,
        range: ByteRange,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        self.entries
            .lock()
            .insert((object_key.to_string(), range), bytes.to_vec());
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StubPartRepairController {
    verify_state: RepairState,
    repair_state: RepairState,
    quarantined: Arc<Mutex<Vec<(String, String)>>>,
}

impl Default for StubPartRepairController {
    fn default() -> Self {
        Self {
            verify_state: RepairState::Verified,
            repair_state: RepairState::Repaired,
            quarantined: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl StubPartRepairController {
    pub fn with_states(verify_state: RepairState, repair_state: RepairState) -> Self {
        Self {
            verify_state,
            repair_state,
            quarantined: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn quarantine_log(&self) -> Vec<(String, String)> {
        self.quarantined.lock().clone()
    }
}

#[async_trait]
impl PartRepairController for StubPartRepairController {
    async fn verify(&self, _part: &HybridPartDescriptor) -> Result<RepairState, StorageError> {
        Ok(self.verify_state.clone())
    }

    async fn quarantine(
        &self,
        part: &HybridPartDescriptor,
        reason: &str,
    ) -> Result<RepairState, StorageError> {
        self.quarantined
            .lock()
            .push((part.local_id.clone(), reason.to_string()));
        Ok(RepairState::Quarantined {
            reason: reason.to_string(),
        })
    }

    async fn repair(&self, _part: &HybridPartDescriptor) -> Result<RepairState, StorageError> {
        Ok(self.repair_state.clone())
    }
}

#[derive(Clone, Debug, Default)]
pub struct ConservativeCompactToWidePolicy {
    pub enabled: bool,
}

impl CompactToWidePromotionPolicy for ConservativeCompactToWidePolicy {
    fn decide(
        &self,
        candidate: &CompactToWidePromotionCandidate,
    ) -> CompactToWidePromotionDecision {
        if self.enabled && candidate.projected_read_count > candidate.full_row_read_count {
            CompactToWidePromotionDecision::PromoteWide
        } else {
            CompactToWidePromotionDecision::KeepCompact
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        FileSystemOperation, StubClock, StubFileSystem, StubObjectStore, StubRng,
        test_support::test_dependencies,
    };

    fn sample_digest() -> CompactPartDigest {
        CompactPartDigest {
            format_tag: ColumnarV2FormatTag::compact_digest(),
            algorithm: PartDigestAlgorithm::Crc32,
            logical_bytes: 128,
            digest_bytes: vec![0xaa, 0xbb, 0xcc, 0xdd],
        }
    }

    fn sample_header() -> ColumnarV2Header {
        ColumnarV2Header {
            format_tag: ColumnarV2FormatTag::base_part(),
            table_id: TableId::new(7),
            local_id: "SST-000777".to_string(),
            schema_version: 3,
            part_digest: sample_digest(),
        }
    }

    fn sample_footer() -> ColumnarV2Footer {
        ColumnarV2Footer {
            format_tag: ColumnarV2FormatTag::base_part(),
            table_id: TableId::new(7),
            local_id: "SST-000777".to_string(),
            schema_version: 3,
            row_count: 2,
            data_range: ByteRange::new(8, 64),
            substreams: vec![ColumnarV2SubstreamRef {
                ordinal: 0,
                field_id: Some(FieldId::new(1)),
                field_type: Some(FieldType::String),
                kind: ColumnarV2SubstreamKind::StringData,
                encoding: ColumnarV2Encoding::Plain,
                compression: ColumnarV2Compression::None,
                range: ByteRange::new(8, 32),
                checksum: 11,
            }],
            marks: vec![ColumnarV2Mark {
                granule_index: 0,
                page_index: 0,
                row_ordinal: 0,
                offsets: vec![ColumnarV2MarkOffset {
                    substream_ordinal: 0,
                    offset: 8,
                }],
            }],
            synopsis: ColumnarSynopsisSidecar {
                format_tag: ColumnarV2FormatTag::synopsis_sidecar(),
                part_local_id: "SST-000777".to_string(),
                granules: vec![ColumnarGranuleSynopsis {
                    granule_index: 0,
                    row_range: ByteRange::new(0, 2),
                    zone_maps: vec![ZoneMapSynopsis {
                        field_id: FieldId::new(2),
                        min_value: Some(FieldValue::Int64(1)),
                        max_value: Some(FieldValue::Int64(9)),
                        null_count: 0,
                    }],
                }],
                checksum: 22,
            },
            optional_sidecars: vec![
                ColumnarOptionalSidecar::SkipIndex(SkipIndexSidecarDescriptor {
                    format_tag: ColumnarV2FormatTag::skip_index_sidecar(),
                    part_local_id: "SST-000777".to_string(),
                    index_name: "count_gt_zero".to_string(),
                    checksum: 33,
                }),
                ColumnarOptionalSidecar::Projection(ProjectionSidecarDescriptor {
                    format_tag: ColumnarV2FormatTag::projection_sidecar(),
                    part_local_id: "SST-000777".to_string(),
                    projection_name: "metric_only".to_string(),
                    projected_fields: vec![FieldId::new(1)],
                    checksum: 44,
                }),
            ],
            digests: vec![sample_digest()],
        }
    }

    #[test]
    fn hybrid_read_config_requires_bounded_caches_and_base_zone_maps() {
        let mut config = HybridReadConfig::default();
        assert_eq!(config.validate(), Ok(()));

        config.raw_segment_cache_bytes = 0;
        assert!(config.validate().is_err());

        config = HybridReadConfig::default();
        config.zone_maps_in_base_part = false;
        assert!(config.validate().is_err());
    }

    #[test]
    fn columnar_v2_metadata_shapes_round_trip() {
        let header = sample_header();
        let footer = sample_footer();
        let digest = sample_digest();

        let encoded_header = serde_json::to_vec(&header).expect("encode header");
        let encoded_footer = serde_json::to_vec(&footer).expect("encode footer");
        let encoded_digest = serde_json::to_vec(&digest).expect("encode digest");

        assert_eq!(
            serde_json::from_slice::<ColumnarV2Header>(&encoded_header).expect("decode header"),
            header
        );
        assert_eq!(
            serde_json::from_slice::<ColumnarV2Footer>(&encoded_footer).expect("decode footer"),
            footer
        );
        assert_eq!(
            serde_json::from_slice::<CompactPartDigest>(&encoded_digest).expect("decode digest"),
            digest
        );
    }

    #[tokio::test]
    async fn stub_scan_cache_and_repair_components_instantiate_without_io() {
        let file_system = Arc::new(StubFileSystem::default());
        let dependencies =
            test_dependencies(file_system.clone(), Arc::new(StubObjectStore::default()));
        let runtime = HybridRuntimeContext::new(dependencies, HybridReadConfig::default())
            .expect("validate hybrid runtime config");
        let loader = StubColumnarFooterPageDirectoryLoader::new(
            sample_header(),
            sample_footer(),
            ColumnarV2PageDirectory {
                pages: vec![ColumnarV2PageRef {
                    substream_ordinal: 0,
                    page_ordinal: 0,
                    range: ByteRange::new(8, 32),
                    row_range: ByteRange::new(0, 2),
                }],
            },
        );
        let mut iterator = VecRowRefBatchIterator::new([RowRefBatch::new(
            vec![HybridRowRef {
                table_id: TableId::new(7),
                local_id: "SST-000777".to_string(),
                key: b"user:1".to_vec(),
                sequence: SequenceNumber::new(1),
                row_ordinal: 0,
            }],
            RowProjection::Fields(vec![FieldId::new(1)]),
            Some(SelectionMask::from_selected_positions(1, &[0])),
        )
        .expect("row-ref batch")]);
        let cache = InMemoryRawByteSegmentCache::default();
        let repair =
            StubPartRepairController::with_states(RepairState::Verified, RepairState::Repaired);
        let promotion = ConservativeCompactToWidePolicy::default();
        let source = StorageSource::remote_object("cold/table-000007/SST-000777.sst");

        let reads_before = file_system.operation_count(FileSystemOperation::ReadAt);
        let _ = loader.load_header(&source).await.expect("load header");
        let footer = loader.load_footer(&source).await.expect("load footer");
        let _ = loader
            .load_page_directory(&source, footer.as_ref())
            .await
            .expect("load page directory");
        let batch = iterator
            .next_batch(128)
            .await
            .expect("next batch")
            .expect("present row-ref batch");
        assert_eq!(batch.survivors.row_positions, vec![0]);

        assert_eq!(
            cache
                .lookup(source.target(), ByteRange::new(8, 32))
                .await
                .expect("cache lookup"),
            None
        );
        cache
            .fill(source.target(), ByteRange::new(8, 32), b"cached-bytes")
            .await
            .expect("cache fill");
        assert_eq!(
            cache
                .lookup(source.target(), ByteRange::new(8, 32))
                .await
                .expect("cache hit"),
            Some(b"cached-bytes".to_vec())
        );

        let part = HybridPartDescriptor {
            table_id: TableId::new(7),
            local_id: "SST-000777".to_string(),
            source,
            format_tag: ColumnarV2FormatTag::base_part(),
            digests: vec![sample_digest()],
        };
        assert_eq!(
            repair.verify(&part).await.expect("verify"),
            RepairState::Verified
        );
        assert_eq!(
            repair
                .quarantine(&part, "test quarantine")
                .await
                .expect("quarantine"),
            RepairState::Quarantined {
                reason: "test quarantine".to_string(),
            }
        );
        assert_eq!(
            repair.repair(&part).await.expect("repair"),
            RepairState::Repaired
        );
        assert_eq!(
            promotion.decide(&CompactToWidePromotionCandidate {
                table_id: TableId::new(7),
                local_id: "SST-000777".to_string(),
                row_count: 1,
                projected_read_count: 9,
                full_row_read_count: 3,
                projected_bytes_read: 128,
            }),
            CompactToWidePromotionDecision::KeepCompact
        );

        let reads_after = file_system.operation_count(FileSystemOperation::ReadAt);
        assert_eq!(reads_after, reads_before);
        let _ = runtime;
        let _ = Arc::new(StubClock::default());
        let _ = Arc::new(StubRng::seeded(9));
    }
}
