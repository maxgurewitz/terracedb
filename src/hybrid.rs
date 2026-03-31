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

pub const COLUMNAR_BASE_PART_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION: u32 = 1;
pub const COLUMNAR_V2_BASE_PART_FORMAT_VERSION: u32 = COLUMNAR_BASE_PART_FORMAT_VERSION;
pub const COLUMNAR_V2_SYNOPSIS_SIDECAR_FORMAT_VERSION: u32 =
    COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION;
pub const COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION: u32 =
    COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION;
pub const COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION: u32 =
    COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION;
pub const COLUMNAR_V2_COMPACT_DIGEST_FORMAT_VERSION: u32 = COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION;
pub const HYBRID_TABLE_FEATURES_METADATA_KEY: &str = "terracedb.hybrid";
pub const HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY: &str = "terracedb.hybrid.compact_to_wide";

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
            return Err("zone maps are part of the columnar base format".to_string());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridCompactToWidePromotionConfig {
    #[serde(default = "default_compact_to_wide_promote_on_compaction_to_level")]
    pub promote_on_compaction_to_level: u32,
    #[serde(default = "default_compact_to_wide_max_compact_rows")]
    pub max_compact_rows: usize,
}

const fn default_compact_to_wide_promote_on_compaction_to_level() -> u32 {
    1
}

const fn default_compact_to_wide_max_compact_rows() -> usize {
    64
}

impl Default for HybridCompactToWidePromotionConfig {
    fn default() -> Self {
        Self {
            promote_on_compaction_to_level: default_compact_to_wide_promote_on_compaction_to_level(
            ),
            max_compact_rows: default_compact_to_wide_max_compact_rows(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HybridTableFeatures {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skip_indexes: Vec<HybridSkipIndexConfig>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub projection_sidecars: Vec<HybridProjectionSidecarConfig>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridSkipIndexConfig {
    pub name: String,
    pub family: HybridSkipIndexFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default = "default_skip_index_max_values")]
    pub max_values: usize,
}

const fn default_skip_index_max_values() -> usize {
    64
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HybridSkipIndexFamily {
    UserKeyBloom,
    FieldValueBloom,
    BoundedSet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridProjectionSidecarConfig {
    pub name: String,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SkipIndexProbe {
    Key(Key),
    FieldEquals { field: String, value: FieldValue },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipIndexProbeResult {
    pub local_id: String,
    pub used_indexes: Vec<String>,
    pub may_match: bool,
    pub fallback_to_base: bool,
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
pub enum ColumnarArtifactKind {
    BasePart,
    SynopsisSidecar,
    SkipIndexSidecar,
    ProjectionSidecar,
    CompactDigest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarFormatTag {
    pub kind: ColumnarArtifactKind,
    pub format_version: u32,
    pub min_reader_version: u32,
    pub max_writer_version: u32,
}

impl ColumnarFormatTag {
    pub const fn base_part() -> Self {
        Self {
            kind: ColumnarArtifactKind::BasePart,
            format_version: COLUMNAR_BASE_PART_FORMAT_VERSION,
            min_reader_version: COLUMNAR_BASE_PART_FORMAT_VERSION,
            max_writer_version: COLUMNAR_BASE_PART_FORMAT_VERSION,
        }
    }

    pub const fn synopsis_sidecar() -> Self {
        Self {
            kind: ColumnarArtifactKind::SynopsisSidecar,
            format_version: COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_SYNOPSIS_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn skip_index_sidecar() -> Self {
        Self {
            kind: ColumnarArtifactKind::SkipIndexSidecar,
            format_version: COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn projection_sidecar() -> Self {
        Self {
            kind: ColumnarArtifactKind::ProjectionSidecar,
            format_version: COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION,
            min_reader_version: COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION,
            max_writer_version: COLUMNAR_PROJECTION_SIDECAR_FORMAT_VERSION,
        }
    }

    pub const fn compact_digest() -> Self {
        Self {
            kind: ColumnarArtifactKind::CompactDigest,
            format_version: COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION,
            min_reader_version: COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION,
            max_writer_version: COLUMNAR_COMPACT_DIGEST_FORMAT_VERSION,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarEncoding {
    Plain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarCompression {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnarSubstreamKind {
    KeyOffsets,
    KeyData,
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
pub struct ColumnarDecodeField {
    pub field_id: FieldId,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub nullable: bool,
    pub has_default: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarDecodeMetadata {
    pub schema_version: u32,
    pub fields: Vec<ColumnarDecodeField>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarSubstreamRef {
    pub ordinal: u32,
    pub field_id: Option<FieldId>,
    pub field_type: Option<FieldType>,
    pub kind: ColumnarSubstreamKind,
    pub encoding: ColumnarEncoding,
    pub compression: ColumnarCompression,
    pub range: ByteRange,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarMarkOffset {
    pub substream_ordinal: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarMark {
    pub granule_index: u32,
    pub page_index: u32,
    pub row_ordinal: u64,
    pub offsets: Vec<ColumnarMarkOffset>,
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
    pub format_tag: ColumnarFormatTag,
    pub part_local_id: String,
    pub granules: Vec<ColumnarGranuleSynopsis>,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkipIndexSidecarDescriptor {
    pub format_tag: ColumnarFormatTag,
    pub part_local_id: String,
    pub index_name: String,
    pub checksum: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSidecarDescriptor {
    pub format_tag: ColumnarFormatTag,
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
    pub format_tag: ColumnarFormatTag,
    pub algorithm: PartDigestAlgorithm,
    pub logical_bytes: u64,
    pub digest_bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarHeader {
    pub format_tag: ColumnarFormatTag,
    pub table_id: TableId,
    pub local_id: String,
    pub schema_version: u32,
    pub part_digest: CompactPartDigest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarSequenceBounds {
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
}

impl ColumnarSequenceBounds {
    pub fn contains(&self, sequence: SequenceNumber) -> bool {
        self.min_sequence <= sequence && sequence <= self.max_sequence
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarGranuleRef {
    pub granule_index: u32,
    pub first_key: Key,
    pub row_range: ByteRange,
    pub page_range: ByteRange,
    pub sequence_bounds: ColumnarSequenceBounds,
    pub has_tombstones: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarPageRef {
    pub granule_index: u32,
    pub substream_ordinal: u32,
    pub page_ordinal: u32,
    pub first_key: Key,
    pub range: ByteRange,
    pub row_range: ByteRange,
    pub sequence_bounds: ColumnarSequenceBounds,
    pub has_tombstones: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarPageDirectory {
    pub granules: Vec<ColumnarGranuleRef>,
    pub pages: Vec<ColumnarPageRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnarFooter {
    pub format_tag: ColumnarFormatTag,
    pub table_id: TableId,
    pub local_id: String,
    pub schema_version: u32,
    pub row_count: u64,
    pub data_range: ByteRange,
    pub decode_metadata: ColumnarDecodeMetadata,
    pub substreams: Vec<ColumnarSubstreamRef>,
    pub marks: Vec<ColumnarMark>,
    pub synopsis: ColumnarSynopsisSidecar,
    pub optional_sidecars: Vec<ColumnarOptionalSidecar>,
    pub digests: Vec<CompactPartDigest>,
}

pub type ColumnarV2ArtifactKind = ColumnarArtifactKind;
pub type ColumnarV2FormatTag = ColumnarFormatTag;
pub type ColumnarV2Encoding = ColumnarEncoding;
pub type ColumnarV2Compression = ColumnarCompression;
pub type ColumnarV2SubstreamKind = ColumnarSubstreamKind;
pub type ColumnarV2SubstreamRef = ColumnarSubstreamRef;
pub type ColumnarV2MarkOffset = ColumnarMarkOffset;
pub type ColumnarV2Mark = ColumnarMark;
pub type ColumnarV2Header = ColumnarHeader;
pub type ColumnarV2PageRef = ColumnarPageRef;
pub type ColumnarV2PageDirectory = ColumnarPageDirectory;
pub type ColumnarV2Footer = ColumnarFooter;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridKeyRange {
    pub start_inclusive: Option<Key>,
    pub end_exclusive: Option<Key>,
}

impl HybridKeyRange {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        if self
            .start_inclusive
            .as_deref()
            .is_some_and(|start| key < start)
        {
            return false;
        }
        if self.end_exclusive.as_deref().is_some_and(|end| key >= end) {
            return false;
        }
        true
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ZoneMapPredicate {
    AlwaysTrue,
    FieldEquals {
        field_id: FieldId,
        value: FieldValue,
    },
    Int64AtLeast {
        field_id: FieldId,
        value: i64,
    },
    BoolEquals {
        field_id: FieldId,
        value: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarGranuleSelection {
    pub granule: ColumnarGranuleRef,
    pub pages: Vec<ColumnarPageRef>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarPruningStats {
    pub inspected_granules: usize,
    pub inspected_pages: usize,
    pub skipped_by_key_range: usize,
    pub skipped_by_zone_map: usize,
    pub selected_row_upper_bound: u64,
    pub overread_row_upper_bound: u64,
    pub page_pruned_rows: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnarPruningOutcome {
    pub selected: Vec<ColumnarGranuleSelection>,
    pub stats: ColumnarPruningStats,
}

pub trait HybridSynopsisPruner: Send + Sync {
    fn prune(
        &self,
        page_directory: &ColumnarPageDirectory,
        synopsis: &ColumnarSynopsisSidecar,
        key_range: &HybridKeyRange,
        predicate: &ZoneMapPredicate,
    ) -> Result<ColumnarPruningOutcome, StorageError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BaseZoneMapPruner;

impl HybridSynopsisPruner for BaseZoneMapPruner {
    fn prune(
        &self,
        page_directory: &ColumnarPageDirectory,
        synopsis: &ColumnarSynopsisSidecar,
        key_range: &HybridKeyRange,
        predicate: &ZoneMapPredicate,
    ) -> Result<ColumnarPruningOutcome, StorageError> {
        page_directory.prune_with_zone_maps(synopsis, key_range, predicate)
    }
}

impl ZoneMapSynopsis {
    pub fn may_match(&self, predicate: &ZoneMapPredicate) -> bool {
        match predicate {
            ZoneMapPredicate::AlwaysTrue => true,
            ZoneMapPredicate::FieldEquals { field_id, value } => {
                if &self.field_id != field_id {
                    return true;
                }
                let Some(min) = self.min_value.as_ref() else {
                    return false;
                };
                let Some(max) = self.max_value.as_ref() else {
                    return false;
                };
                !field_value_lt(value, min) && !field_value_gt(value, max)
            }
            ZoneMapPredicate::Int64AtLeast { field_id, value } => {
                if &self.field_id != field_id {
                    return true;
                }
                match self.max_value.as_ref() {
                    Some(FieldValue::Int64(max)) => max >= value,
                    Some(_) => true,
                    None => false,
                }
            }
            ZoneMapPredicate::BoolEquals { field_id, value } => {
                if &self.field_id != field_id {
                    return true;
                }
                let Some(min) = self.min_value.as_ref() else {
                    return false;
                };
                let Some(max) = self.max_value.as_ref() else {
                    return false;
                };
                let expected = FieldValue::Bool(*value);
                !field_value_lt(&expected, min) && !field_value_gt(&expected, max)
            }
        }
    }
}

impl ColumnarSynopsisSidecar {
    pub fn granule(&self, granule_index: u32) -> Option<&ColumnarGranuleSynopsis> {
        self.granules
            .iter()
            .find(|granule| granule.granule_index == granule_index)
    }
}

impl ColumnarPageDirectory {
    pub fn pages_for_granule(&self, granule_index: u32) -> Vec<ColumnarPageRef> {
        self.pages
            .iter()
            .filter(|page| page.granule_index == granule_index)
            .cloned()
            .collect()
    }

    pub fn prune_with_zone_maps(
        &self,
        synopsis: &ColumnarSynopsisSidecar,
        key_range: &HybridKeyRange,
        predicate: &ZoneMapPredicate,
    ) -> Result<ColumnarPruningOutcome, StorageError> {
        let mut selected = Vec::new();
        let mut stats = ColumnarPruningStats::default();

        for (granule_position, granule) in self.granules.iter().enumerate() {
            stats.inspected_granules += 1;
            let next_granule_first_key = self
                .granules
                .get(granule_position + 1)
                .map(|next| next.first_key.as_slice());
            if !key_window_overlaps(
                granule.first_key.as_slice(),
                next_granule_first_key,
                key_range,
            ) {
                stats.skipped_by_key_range += 1;
                continue;
            }

            let granule_synopsis = synopsis.granule(granule.granule_index).ok_or_else(|| {
                StorageError::corruption(format!(
                    "synopsis sidecar is missing granule {}",
                    granule.granule_index
                ))
            })?;
            if !granule_synopsis
                .zone_maps
                .iter()
                .all(|zone_map| zone_map.may_match(predicate))
            {
                stats.skipped_by_zone_map += 1;
                continue;
            }

            let pages = self.pages_for_granule(granule.granule_index);
            let mut selected_pages = Vec::new();
            for (page_position, page) in pages.iter().enumerate() {
                stats.inspected_pages += 1;
                let next_page_first_key = pages
                    .get(page_position + 1)
                    .map(|next| next.first_key.as_slice())
                    .or(next_granule_first_key);
                if key_window_overlaps(page.first_key.as_slice(), next_page_first_key, key_range) {
                    stats.selected_row_upper_bound = stats
                        .selected_row_upper_bound
                        .saturating_add(page.row_range.len());
                    selected_pages.push(page.clone());
                }
            }
            if selected_pages.is_empty() {
                stats.skipped_by_key_range += 1;
                continue;
            }

            let selected_rows = selected_pages
                .iter()
                .map(|page| page.row_range.len())
                .sum::<u64>();
            let granule_rows = granule.row_range.len();
            stats.overread_row_upper_bound =
                stats.overread_row_upper_bound.saturating_add(selected_rows);
            stats.page_pruned_rows = stats
                .page_pruned_rows
                .saturating_add(granule_rows.saturating_sub(selected_rows));
            selected.push(ColumnarGranuleSelection {
                granule: granule.clone(),
                pages: selected_pages,
            });
        }

        Ok(ColumnarPruningOutcome { selected, stats })
    }
}

#[async_trait]
pub trait ColumnarFooterPageDirectoryLoader: Send + Sync {
    async fn load_header(
        &self,
        source: &StorageSource,
    ) -> Result<Arc<ColumnarHeader>, StorageError>;

    async fn load_footer(
        &self,
        source: &StorageSource,
    ) -> Result<Arc<ColumnarFooter>, StorageError>;

    async fn load_page_directory(
        &self,
        source: &StorageSource,
        footer: &ColumnarFooter,
    ) -> Result<Arc<ColumnarPageDirectory>, StorageError>;
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
pub struct LateMaterializationPlan {
    pub predicate_projection: RowProjection,
    pub final_projection: RowProjection,
    pub late_projection: RowProjection,
}

impl LateMaterializationPlan {
    pub fn new(predicate_projection: RowProjection, final_projection: RowProjection) -> Self {
        let late_projection = match (&predicate_projection, &final_projection) {
            (RowProjection::FullRow, RowProjection::Fields(_)) => RowProjection::Fields(Vec::new()),
            (_, RowProjection::FullRow) => RowProjection::FullRow,
            (RowProjection::Fields(predicate_fields), RowProjection::Fields(final_fields)) => {
                RowProjection::Fields(
                    final_fields
                        .iter()
                        .copied()
                        .filter(|field_id| !predicate_fields.contains(field_id))
                        .collect(),
                )
            }
        };
        Self {
            predicate_projection,
            final_projection,
            late_projection,
        }
    }

    pub fn needs_late_materialization(&self) -> bool {
        match &self.late_projection {
            RowProjection::FullRow => true,
            RowProjection::Fields(fields) => !fields.is_empty(),
        }
    }
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

    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<Self, StorageError> {
        if range.start > range.end || range.end > self.len() {
            return Err(StorageError::unsupported(
                "selection mask slice must stay within bounds",
            ));
        }
        Ok(Self {
            selected: self.selected[range].to_vec(),
        })
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

    pub fn apply_selection(&self, selection: &SelectionMask) -> Result<Self, StorageError> {
        Self::new(
            self.rows.clone(),
            self.projection.clone(),
            Some(self.selection.intersect(selection)?),
        )
    }

    pub fn survivor_rows(&self) -> Vec<HybridRowRef> {
        self.survivors
            .row_positions
            .iter()
            .filter_map(|index| self.rows.get(*index).cloned())
            .collect()
    }

    pub fn slice(&self, range: std::ops::Range<usize>) -> Result<Self, StorageError> {
        if range.start > range.end || range.end > self.rows.len() {
            return Err(StorageError::unsupported(
                "row-ref batch slice must stay within bounds",
            ));
        }
        Self::new(
            self.rows[range.clone()].to_vec(),
            self.projection.clone(),
            Some(self.selection.slice(range)?),
        )
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
    pub format_tag: ColumnarFormatTag,
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
    header: Arc<ColumnarHeader>,
    footer: Arc<ColumnarFooter>,
    page_directory: Arc<ColumnarPageDirectory>,
}

impl StubColumnarFooterPageDirectoryLoader {
    pub fn new(
        header: ColumnarHeader,
        footer: ColumnarFooter,
        page_directory: ColumnarPageDirectory,
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
    ) -> Result<Arc<ColumnarHeader>, StorageError> {
        Ok(self.header.clone())
    }

    async fn load_footer(
        &self,
        _source: &StorageSource,
    ) -> Result<Arc<ColumnarFooter>, StorageError> {
        Ok(self.footer.clone())
    }

    async fn load_page_directory(
        &self,
        _source: &StorageSource,
        _footer: &ColumnarFooter,
    ) -> Result<Arc<ColumnarPageDirectory>, StorageError> {
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
    async fn next_batch(&mut self, max_rows: usize) -> Result<Option<RowRefBatch>, StorageError> {
        if max_rows == 0 {
            return Err(StorageError::unsupported(
                "row-ref batch iterator max_rows must be greater than zero",
            ));
        }
        let Some(batch) = self.batches.pop_front() else {
            return Ok(None);
        };
        if batch.rows.len() <= max_rows {
            return Ok(Some(batch));
        }

        let head = batch.slice(0..max_rows)?;
        let tail = batch.slice(max_rows..batch.rows.len())?;
        self.batches.push_front(tail);
        Ok(Some(head))
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

fn key_window_overlaps(
    first_key: &[u8],
    next_first_key: Option<&[u8]>,
    key_range: &HybridKeyRange,
) -> bool {
    if key_range
        .end_exclusive
        .as_deref()
        .is_some_and(|end| first_key >= end)
    {
        return false;
    }
    if let Some(start) = key_range.start_inclusive.as_deref()
        && next_first_key.is_some_and(|next| next <= start)
    {
        return false;
    }
    true
}

fn field_value_lt(left: &FieldValue, right: &FieldValue) -> bool {
    match (left, right) {
        (FieldValue::Int64(left), FieldValue::Int64(right)) => left < right,
        (FieldValue::Float64(left), FieldValue::Float64(right)) => left < right,
        (FieldValue::String(left), FieldValue::String(right)) => left < right,
        (FieldValue::Bytes(left), FieldValue::Bytes(right)) => left < right,
        (FieldValue::Bool(left), FieldValue::Bool(right)) => left < right,
        _ => false,
    }
}

fn field_value_gt(left: &FieldValue, right: &FieldValue) -> bool {
    match (left, right) {
        (FieldValue::Int64(left), FieldValue::Int64(right)) => left > right,
        (FieldValue::Float64(left), FieldValue::Float64(right)) => left > right,
        (FieldValue::String(left), FieldValue::String(right)) => left > right,
        (FieldValue::Bytes(left), FieldValue::Bytes(right)) => left > right,
        (FieldValue::Bool(left), FieldValue::Bool(right)) => left > right,
        _ => false,
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
            format_tag: ColumnarFormatTag::compact_digest(),
            algorithm: PartDigestAlgorithm::Crc32,
            logical_bytes: 128,
            digest_bytes: vec![0xaa, 0xbb, 0xcc, 0xdd],
        }
    }

    fn sample_header() -> ColumnarHeader {
        ColumnarHeader {
            format_tag: ColumnarFormatTag::base_part(),
            table_id: TableId::new(7),
            local_id: "SST-000777".to_string(),
            schema_version: 3,
            part_digest: sample_digest(),
        }
    }

    fn sample_footer() -> ColumnarFooter {
        ColumnarFooter {
            format_tag: ColumnarFormatTag::base_part(),
            table_id: TableId::new(7),
            local_id: "SST-000777".to_string(),
            schema_version: 3,
            row_count: 2,
            data_range: ByteRange::new(8, 64),
            decode_metadata: ColumnarDecodeMetadata {
                schema_version: 3,
                fields: vec![
                    ColumnarDecodeField {
                        field_id: FieldId::new(1),
                        field_type: FieldType::String,
                        nullable: false,
                        has_default: false,
                    },
                    ColumnarDecodeField {
                        field_id: FieldId::new(2),
                        field_type: FieldType::Int64,
                        nullable: false,
                        has_default: true,
                    },
                ],
            },
            substreams: vec![ColumnarSubstreamRef {
                ordinal: 0,
                field_id: Some(FieldId::new(1)),
                field_type: Some(FieldType::String),
                kind: ColumnarSubstreamKind::StringData,
                encoding: ColumnarEncoding::Plain,
                compression: ColumnarCompression::None,
                range: ByteRange::new(8, 32),
                checksum: 11,
            }],
            marks: vec![ColumnarMark {
                granule_index: 0,
                page_index: 0,
                row_ordinal: 0,
                offsets: vec![ColumnarMarkOffset {
                    substream_ordinal: 0,
                    offset: 8,
                }],
            }],
            synopsis: ColumnarSynopsisSidecar {
                format_tag: ColumnarFormatTag::synopsis_sidecar(),
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
                    format_tag: ColumnarFormatTag::skip_index_sidecar(),
                    part_local_id: "SST-000777".to_string(),
                    index_name: "count_gt_zero".to_string(),
                    checksum: 33,
                }),
                ColumnarOptionalSidecar::Projection(ProjectionSidecarDescriptor {
                    format_tag: ColumnarFormatTag::projection_sidecar(),
                    part_local_id: "SST-000777".to_string(),
                    projection_name: "metric_only".to_string(),
                    projected_fields: vec![FieldId::new(1)],
                    checksum: 44,
                }),
            ],
            digests: vec![sample_digest()],
        }
    }

    fn sample_page_directory() -> ColumnarPageDirectory {
        ColumnarPageDirectory {
            granules: vec![ColumnarGranuleRef {
                granule_index: 0,
                first_key: b"user:1".to_vec(),
                row_range: ByteRange::new(0, 2),
                page_range: ByteRange::new(0, 1),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence: SequenceNumber::new(1),
                    max_sequence: SequenceNumber::new(2),
                },
                has_tombstones: false,
            }],
            pages: vec![ColumnarPageRef {
                granule_index: 0,
                substream_ordinal: 0,
                page_ordinal: 0,
                first_key: b"user:1".to_vec(),
                range: ByteRange::new(8, 32),
                row_range: ByteRange::new(0, 2),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence: SequenceNumber::new(1),
                    max_sequence: SequenceNumber::new(2),
                },
                has_tombstones: false,
            }],
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
    fn columnar_metadata_shapes_round_trip() {
        let header = sample_header();
        let footer = sample_footer();
        let digest = sample_digest();

        let encoded_header = serde_json::to_vec(&header).expect("encode header");
        let encoded_footer = serde_json::to_vec(&footer).expect("encode footer");
        let encoded_digest = serde_json::to_vec(&digest).expect("encode digest");

        assert_eq!(
            serde_json::from_slice::<ColumnarHeader>(&encoded_header).expect("decode header"),
            header
        );
        assert_eq!(
            serde_json::from_slice::<ColumnarFooter>(&encoded_footer).expect("decode footer"),
            footer
        );
        assert_eq!(
            serde_json::from_slice::<CompactPartDigest>(&encoded_digest).expect("decode digest"),
            digest
        );
    }

    #[test]
    fn columnar_decode_metadata_round_trip() {
        let metadata = ColumnarDecodeMetadata {
            schema_version: 4,
            fields: vec![
                ColumnarDecodeField {
                    field_id: FieldId::new(1),
                    field_type: FieldType::String,
                    nullable: false,
                    has_default: false,
                },
                ColumnarDecodeField {
                    field_id: FieldId::new(2),
                    field_type: FieldType::Bool,
                    nullable: true,
                    has_default: true,
                },
            ],
        };

        let encoded = serde_json::to_vec(&metadata).expect("encode decode metadata");
        assert_eq!(
            serde_json::from_slice::<ColumnarDecodeMetadata>(&encoded)
                .expect("decode decode metadata"),
            metadata
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
            sample_page_directory(),
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
            format_tag: ColumnarFormatTag::base_part(),
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

    #[test]
    fn page_directory_prunes_granules_with_key_ranges_and_zone_maps() {
        let page_directory = ColumnarPageDirectory {
            granules: vec![
                ColumnarGranuleRef {
                    granule_index: 0,
                    first_key: b"user:1".to_vec(),
                    row_range: ByteRange::new(0, 2),
                    page_range: ByteRange::new(0, 1),
                    sequence_bounds: ColumnarSequenceBounds {
                        min_sequence: SequenceNumber::new(1),
                        max_sequence: SequenceNumber::new(2),
                    },
                    has_tombstones: false,
                },
                ColumnarGranuleRef {
                    granule_index: 1,
                    first_key: b"user:3".to_vec(),
                    row_range: ByteRange::new(2, 4),
                    page_range: ByteRange::new(1, 2),
                    sequence_bounds: ColumnarSequenceBounds {
                        min_sequence: SequenceNumber::new(3),
                        max_sequence: SequenceNumber::new(4),
                    },
                    has_tombstones: false,
                },
            ],
            pages: vec![
                ColumnarPageRef {
                    granule_index: 0,
                    substream_ordinal: 0,
                    page_ordinal: 0,
                    first_key: b"user:1".to_vec(),
                    range: ByteRange::new(8, 32),
                    row_range: ByteRange::new(0, 2),
                    sequence_bounds: ColumnarSequenceBounds {
                        min_sequence: SequenceNumber::new(1),
                        max_sequence: SequenceNumber::new(2),
                    },
                    has_tombstones: false,
                },
                ColumnarPageRef {
                    granule_index: 1,
                    substream_ordinal: 0,
                    page_ordinal: 1,
                    first_key: b"user:3".to_vec(),
                    range: ByteRange::new(32, 56),
                    row_range: ByteRange::new(2, 4),
                    sequence_bounds: ColumnarSequenceBounds {
                        min_sequence: SequenceNumber::new(3),
                        max_sequence: SequenceNumber::new(4),
                    },
                    has_tombstones: false,
                },
            ],
        };
        let synopsis = ColumnarSynopsisSidecar {
            format_tag: ColumnarFormatTag::synopsis_sidecar(),
            part_local_id: "SST-000777".to_string(),
            granules: vec![
                ColumnarGranuleSynopsis {
                    granule_index: 0,
                    row_range: ByteRange::new(0, 2),
                    zone_maps: vec![ZoneMapSynopsis {
                        field_id: FieldId::new(2),
                        min_value: Some(FieldValue::Int64(1)),
                        max_value: Some(FieldValue::Int64(5)),
                        null_count: 0,
                    }],
                },
                ColumnarGranuleSynopsis {
                    granule_index: 1,
                    row_range: ByteRange::new(2, 4),
                    zone_maps: vec![ZoneMapSynopsis {
                        field_id: FieldId::new(2),
                        min_value: Some(FieldValue::Int64(8)),
                        max_value: Some(FieldValue::Int64(12)),
                        null_count: 0,
                    }],
                },
            ],
            checksum: 55,
        };
        let outcome = BaseZoneMapPruner
            .prune(
                &page_directory,
                &synopsis,
                &HybridKeyRange {
                    start_inclusive: Some(b"user:2".to_vec()),
                    end_exclusive: Some(b"user:9".to_vec()),
                },
                &ZoneMapPredicate::Int64AtLeast {
                    field_id: FieldId::new(2),
                    value: 7,
                },
            )
            .expect("prune page directory");

        assert_eq!(outcome.selected.len(), 1);
        assert_eq!(outcome.selected[0].granule.granule_index, 1);
        assert_eq!(outcome.selected[0].pages.len(), 1);
        assert_eq!(outcome.stats.inspected_granules, 2);
        assert_eq!(outcome.stats.skipped_by_zone_map, 1);
        assert_eq!(outcome.stats.selected_row_upper_bound, 2);
        assert_eq!(outcome.stats.page_pruned_rows, 0);
    }

    #[tokio::test]
    async fn row_ref_batches_split_apply_predicates_and_plan_late_materialization() {
        let rows = vec![
            HybridRowRef {
                table_id: TableId::new(7),
                local_id: "SST-000777".to_string(),
                key: b"user:1".to_vec(),
                sequence: SequenceNumber::new(1),
                row_ordinal: 0,
            },
            HybridRowRef {
                table_id: TableId::new(7),
                local_id: "SST-000777".to_string(),
                key: b"user:2".to_vec(),
                sequence: SequenceNumber::new(2),
                row_ordinal: 1,
            },
            HybridRowRef {
                table_id: TableId::new(7),
                local_id: "SST-000777".to_string(),
                key: b"user:3".to_vec(),
                sequence: SequenceNumber::new(3),
                row_ordinal: 2,
            },
        ];
        let batch = RowRefBatch::new(
            rows,
            RowProjection::Fields(vec![FieldId::new(1), FieldId::new(2), FieldId::new(3)]),
            None,
        )
        .expect("batch");
        let plan = LateMaterializationPlan::new(
            RowProjection::Fields(vec![FieldId::new(2)]),
            batch.projection.clone(),
        );
        let filtered = batch
            .apply_selection(&SelectionMask::from_selected_positions(3, &[1, 2]))
            .expect("apply predicate selection");
        let mut iterator = VecRowRefBatchIterator::new([filtered.clone()]);

        let head = iterator
            .next_batch(2)
            .await
            .expect("first split")
            .expect("head batch");
        let tail = iterator
            .next_batch(2)
            .await
            .expect("second split")
            .expect("tail batch");

        assert!(plan.needs_late_materialization());
        assert_eq!(
            plan.late_projection,
            RowProjection::Fields(vec![FieldId::new(1), FieldId::new(3)])
        );
        assert_eq!(head.rows.len(), 2);
        assert_eq!(head.survivor_rows().len(), 1);
        assert_eq!(head.survivor_rows()[0].key, b"user:2".to_vec());
        assert_eq!(tail.rows.len(), 1);
        assert_eq!(tail.survivor_rows().len(), 1);
        assert_eq!(tail.survivor_rows()[0].key, b"user:3".to_vec());
    }
}
