use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb::{
    BaseZoneMapPruner, ByteRange, ColumnarFormatTag, ColumnarGranuleRef, ColumnarGranuleSynopsis,
    ColumnarPageDirectory, ColumnarPageRef, ColumnarSequenceBounds, ColumnarSynopsisSidecar,
    DeterministicRng, FieldId, FieldValue, HybridKeyRange, HybridReadConfig, HybridSynopsisPruner,
    LateMaterializationPlan, Rng, RowProjection, SchemaDefinition, SelectionMask, SequenceNumber,
    TableFormat, Value, ZoneMapPredicate, ZoneMapSynopsis,
};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridTableSpec {
    pub name: String,
    pub format: TableFormat,
    pub schema: Option<SchemaDefinition>,
}

pub type HybridReadRows = Vec<(Vec<u8>, Value)>;
pub type HybridSelectedRows = (SelectionMask, HybridReadRows);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridPruningExpectation {
    pub total_granules: usize,
    pub selected_granules: Vec<u32>,
    pub survivor_rows: usize,
    pub overread_rows_upper_bound: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridStagedScanExpectation {
    pub plan: LateMaterializationPlan,
    pub selection: SelectionMask,
    pub survivors: HybridReadRows,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridSubstreamCodec {
    None,
    Lz4,
    Zstd,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridDecodeField {
    pub field_id: FieldId,
    pub field_type: terracedb::FieldType,
    pub nullable: bool,
    pub has_default: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridDecodeMetadata {
    pub schema_version: u32,
    pub fields: Vec<HybridDecodeField>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HybridColumnarReadExpectation {
    pub projection: RowProjection,
    pub rows: HybridReadRows,
    pub decode_metadata: Option<HybridDecodeMetadata>,
    pub codecs: Vec<HybridSubstreamCodec>,
}

impl HybridTableSpec {
    pub fn row(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            format: TableFormat::Row,
            schema: None,
        }
    }

    pub fn columnar(name: impl Into<String>, schema: SchemaDefinition) -> Self {
        Self {
            name: name.into(),
            format: TableFormat::Columnar,
            schema: Some(schema),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum HybridReadMutation {
    Put {
        table: String,
        key: Vec<u8>,
        value: Value,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct HybridVersion {
    sequence: SequenceNumber,
    value: Option<Value>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HybridReadOracle {
    table_specs: BTreeMap<String, HybridTableSpec>,
    versions: BTreeMap<String, BTreeMap<Vec<u8>, Vec<HybridVersion>>>,
}

impl HybridReadOracle {
    pub fn new(table_specs: &[HybridTableSpec]) -> Self {
        Self {
            table_specs: table_specs
                .iter()
                .cloned()
                .map(|spec| (spec.name.clone(), spec))
                .collect(),
            versions: BTreeMap::new(),
        }
    }

    pub fn apply(
        &mut self,
        sequence: SequenceNumber,
        mutation: HybridReadMutation,
    ) -> Result<(), HybridReadOracleError> {
        let (table, key, value) = match mutation {
            HybridReadMutation::Put { table, key, value } => (table, key, Some(value)),
            HybridReadMutation::Delete { table, key } => (table, key, None),
        };
        self.ensure_table_known(&table)?;
        let versions = self
            .versions
            .entry(table)
            .or_default()
            .entry(key)
            .or_default();
        if let Some(previous) = versions.last()
            && previous.sequence >= sequence
        {
            return Err(HybridReadOracleError::SequenceRegression {
                previous: previous.sequence,
                next: sequence,
            });
        }
        versions.push(HybridVersion { sequence, value });
        Ok(())
    }

    pub fn read(
        &self,
        table: &str,
        key: &[u8],
        sequence: SequenceNumber,
        projection: &RowProjection,
    ) -> Result<Option<Value>, HybridReadOracleError> {
        let spec = self.ensure_table_known(table)?;
        let value = self
            .versions
            .get(table)
            .and_then(|rows| rows.get(key))
            .and_then(|versions| {
                versions
                    .iter()
                    .rev()
                    .find(|version| version.sequence <= sequence)
            })
            .and_then(|version| version.value.clone());
        value
            .map(|value| project_value(spec.format, value, projection))
            .transpose()
    }

    pub fn scan(
        &self,
        table: &str,
        sequence: SequenceNumber,
        projection: &RowProjection,
    ) -> Result<Vec<(Vec<u8>, Value)>, HybridReadOracleError> {
        self.ensure_table_known(table)?;
        let mut rows = Vec::new();
        if let Some(table_versions) = self.versions.get(table) {
            for key in table_versions.keys() {
                if let Some(value) = self.read(table, key, sequence, projection)? {
                    rows.push((key.clone(), value));
                }
            }
        }
        Ok(rows)
    }

    pub fn selection_mask(
        &self,
        table: &str,
        sequence: SequenceNumber,
        projection: &RowProjection,
        predicate: &HybridPredicate,
    ) -> Result<SelectionMask, HybridReadOracleError> {
        let rows = self.scan(table, sequence, projection)?;
        Ok(SelectionMask {
            selected: rows
                .iter()
                .map(|(_, value)| predicate.matches(value))
                .collect(),
        })
    }

    pub fn scan_with_selection(
        &self,
        table: &str,
        sequence: SequenceNumber,
        projection: &RowProjection,
        predicate: &HybridPredicate,
    ) -> Result<HybridSelectedRows, HybridReadOracleError> {
        let rows = self.scan(table, sequence, projection)?;
        let mask = SelectionMask {
            selected: rows
                .iter()
                .map(|(_, value)| predicate.matches(value))
                .collect(),
        };
        let survivors = rows
            .into_iter()
            .zip(mask.selected.iter().copied())
            .filter_map(|(row, keep)| keep.then_some(row))
            .collect();
        Ok((mask, survivors))
    }

    pub fn pruning_expectation(
        &self,
        table: &str,
        sequence: SequenceNumber,
        predicate: &HybridPredicate,
        granule_size: usize,
    ) -> Result<HybridPruningExpectation, HybridReadOracleError> {
        let visible = self.visible_versions(table, sequence)?;
        if visible.is_empty() {
            return Ok(HybridPruningExpectation::default());
        }

        let granule_size = granule_size.max(1);
        let mut granules = Vec::new();
        let mut pages = Vec::new();
        let mut synopsis_granules = Vec::new();

        for (granule_index, chunk) in visible.chunks(granule_size).enumerate() {
            let row_start = (granule_index * granule_size) as u64;
            let row_end = row_start + chunk.len() as u64;
            let min_sequence = chunk
                .iter()
                .map(|(_, version)| version.sequence)
                .min()
                .unwrap_or_default();
            let max_sequence = chunk
                .iter()
                .map(|(_, version)| version.sequence)
                .max()
                .unwrap_or_default();
            let has_tombstones = chunk.iter().any(|(_, version)| version.value.is_none());
            let granule_index = granule_index as u32;

            granules.push(ColumnarGranuleRef {
                granule_index,
                first_key: chunk[0].0.clone(),
                row_range: ByteRange::new(row_start, row_end),
                page_range: ByteRange::new(granule_index as u64, granule_index as u64 + 1),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence,
                    max_sequence,
                },
                has_tombstones,
            });
            pages.push(ColumnarPageRef {
                granule_index,
                substream_ordinal: 0,
                page_ordinal: granule_index,
                first_key: chunk[0].0.clone(),
                range: ByteRange::new(granule_index as u64, granule_index as u64 + 1),
                row_range: ByteRange::new(row_start, row_end),
                sequence_bounds: ColumnarSequenceBounds {
                    min_sequence,
                    max_sequence,
                },
                has_tombstones,
            });
            synopsis_granules.push(ColumnarGranuleSynopsis {
                granule_index,
                row_range: ByteRange::new(row_start, row_end),
                zone_maps: build_zone_maps(chunk),
            });
        }

        let outcome = BaseZoneMapPruner
            .prune(
                &ColumnarPageDirectory { granules, pages },
                &ColumnarSynopsisSidecar {
                    format_tag: ColumnarFormatTag::synopsis_sidecar(),
                    part_local_id: format!("{table}-oracle"),
                    granules: synopsis_granules,
                    checksum: 0,
                },
                &HybridKeyRange::all(),
                &zone_map_predicate(predicate),
            )
            .map_err(|_| HybridReadOracleError::UnsupportedOraclePruning)?;

        Ok(HybridPruningExpectation {
            total_granules: outcome.stats.inspected_granules,
            selected_granules: outcome
                .selected
                .iter()
                .map(|selection| selection.granule.granule_index)
                .collect(),
            survivor_rows: visible
                .iter()
                .filter_map(|(_, version)| version.value.as_ref())
                .filter(|value| predicate.matches(value))
                .count(),
            overread_rows_upper_bound: outcome.stats.overread_row_upper_bound,
        })
    }

    pub fn staged_scan_with_selection(
        &self,
        table: &str,
        sequence: SequenceNumber,
        predicate_projection: RowProjection,
        final_projection: RowProjection,
        predicate: &HybridPredicate,
    ) -> Result<HybridStagedScanExpectation, HybridReadOracleError> {
        let plan =
            LateMaterializationPlan::new(predicate_projection.clone(), final_projection.clone());
        let selection = self.selection_mask(table, sequence, &predicate_projection, predicate)?;
        let rows = self.scan(table, sequence, &final_projection)?;
        let survivors = rows
            .into_iter()
            .zip(selection.selected.iter().copied())
            .filter_map(|(row, keep)| keep.then_some(row))
            .collect();
        Ok(HybridStagedScanExpectation {
            plan,
            selection,
            survivors,
        })
    }

    pub fn compact_decode_metadata(
        &self,
        table: &str,
    ) -> Result<Option<HybridDecodeMetadata>, HybridReadOracleError> {
        let spec = self.ensure_table_known(table)?;
        Ok(spec.schema.as_ref().map(|schema| HybridDecodeMetadata {
            schema_version: schema.version,
            fields: schema
                .fields
                .iter()
                .map(|field| HybridDecodeField {
                    field_id: field.id,
                    field_type: field.field_type,
                    nullable: field.nullable,
                    has_default: field.default.is_some(),
                })
                .collect(),
        }))
    }

    pub fn validate_decode_metadata(
        &self,
        table: &str,
        metadata: &HybridDecodeMetadata,
    ) -> Result<(), HybridReadOracleError> {
        let expected = self.compact_decode_metadata(table)?.ok_or_else(|| {
            HybridReadOracleError::DecodeMetadataUnavailable {
                table: table.to_string(),
            }
        })?;
        if expected.schema_version != metadata.schema_version {
            return Err(HybridReadOracleError::DecodeMetadataMismatch {
                table: table.to_string(),
                reason: format!(
                    "schema version {} did not match expected {}",
                    metadata.schema_version, expected.schema_version
                ),
            });
        }
        if expected.fields.len() != metadata.fields.len() {
            return Err(HybridReadOracleError::DecodeMetadataMismatch {
                table: table.to_string(),
                reason: format!(
                    "field count {} did not match expected {}",
                    metadata.fields.len(),
                    expected.fields.len()
                ),
            });
        }
        for (actual, expected) in metadata.fields.iter().zip(expected.fields.iter()) {
            if actual != expected {
                return Err(HybridReadOracleError::DecodeMetadataMismatch {
                    table: table.to_string(),
                    reason: format!(
                        "field {} metadata did not match expected layout",
                        actual.field_id.get()
                    ),
                });
            }
        }
        Ok(())
    }

    pub fn read_expectation(
        &self,
        table: &str,
        sequence: SequenceNumber,
        projection: &RowProjection,
    ) -> Result<HybridColumnarReadExpectation, HybridReadOracleError> {
        Ok(HybridColumnarReadExpectation {
            projection: projection.clone(),
            rows: self.scan(table, sequence, projection)?,
            decode_metadata: self.compact_decode_metadata(table)?,
            codecs: vec![
                HybridSubstreamCodec::None,
                HybridSubstreamCodec::Lz4,
                HybridSubstreamCodec::Zstd,
            ],
        })
    }

    pub fn resolve_sidecar_fallback(
        requested: HybridSidecarKind,
        state: HybridSidecarState,
    ) -> HybridSidecarResolution {
        match (requested, state) {
            (HybridSidecarKind::SkipIndex, HybridSidecarState::Healthy) => {
                HybridSidecarResolution {
                    requested,
                    state,
                    execution_path: HybridExecutionPath::SkipIndexSidecar,
                    fallback_to_base: false,
                }
            }
            (HybridSidecarKind::Projection, HybridSidecarState::Healthy) => {
                HybridSidecarResolution {
                    requested,
                    state,
                    execution_path: HybridExecutionPath::ProjectionSidecar,
                    fallback_to_base: false,
                }
            }
            (_, HybridSidecarState::Absent) => HybridSidecarResolution {
                requested,
                state,
                execution_path: HybridExecutionPath::BasePartFallbackMissing,
                fallback_to_base: true,
            },
            (_, HybridSidecarState::Corrupt) => HybridSidecarResolution {
                requested,
                state,
                execution_path: HybridExecutionPath::BasePartFallbackCorrupt,
                fallback_to_base: true,
            },
        }
    }

    fn ensure_table_known(&self, table: &str) -> Result<&HybridTableSpec, HybridReadOracleError> {
        self.table_specs
            .get(table)
            .ok_or_else(|| HybridReadOracleError::UnknownTable {
                table: table.to_string(),
            })
    }

    fn visible_versions(
        &self,
        table: &str,
        sequence: SequenceNumber,
    ) -> Result<Vec<(Vec<u8>, HybridVersion)>, HybridReadOracleError> {
        self.ensure_table_known(table)?;
        let mut visible = Vec::new();
        if let Some(table_versions) = self.versions.get(table) {
            for (key, versions) in table_versions {
                if let Some(version) = versions
                    .iter()
                    .rev()
                    .find(|version| version.sequence <= sequence)
                {
                    visible.push((key.clone(), version.clone()));
                }
            }
        }
        Ok(visible)
    }
}

fn project_value(
    format: TableFormat,
    value: Value,
    projection: &RowProjection,
) -> Result<Value, HybridReadOracleError> {
    match (format, value, projection) {
        (_, value, RowProjection::FullRow) => Ok(value),
        (TableFormat::Row, Value::Bytes(bytes), RowProjection::Fields(_)) => {
            Ok(Value::Bytes(bytes))
        }
        (TableFormat::Columnar, Value::Record(record), RowProjection::Fields(fields)) => {
            Ok(Value::record(
                record
                    .into_iter()
                    .filter(|(field_id, _)| fields.contains(field_id))
                    .collect(),
            ))
        }
        (TableFormat::Columnar, Value::Bytes(_), _) => Err(HybridReadOracleError::UnexpectedBytes),
        (TableFormat::Row, Value::Record(_), _) => Err(HybridReadOracleError::UnexpectedRecord),
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum HybridPredicate {
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

impl HybridPredicate {
    fn matches(&self, value: &Value) -> bool {
        match self {
            Self::AlwaysTrue => true,
            Self::FieldEquals {
                field_id,
                value: expected,
            } => match value {
                Value::Record(record) => record.get(field_id) == Some(expected),
                Value::Bytes(_) => false,
            },
            Self::Int64AtLeast {
                field_id,
                value: expected,
            } => match value {
                Value::Record(record) => {
                    matches!(record.get(field_id), Some(FieldValue::Int64(actual)) if actual >= expected)
                }
                Value::Bytes(_) => false,
            },
            Self::BoolEquals {
                field_id,
                value: expected,
            } => match value {
                Value::Record(record) => {
                    matches!(record.get(field_id), Some(FieldValue::Bool(actual)) if actual == expected)
                }
                Value::Bytes(_) => false,
            },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridSidecarKind {
    SkipIndex,
    Projection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridSidecarState {
    Absent,
    Healthy,
    Corrupt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridExecutionPath {
    SkipIndexSidecar,
    ProjectionSidecar,
    BasePartFallbackMissing,
    BasePartFallbackCorrupt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridSidecarResolution {
    pub requested: HybridSidecarKind,
    pub state: HybridSidecarState,
    pub execution_path: HybridExecutionPath,
    pub fallback_to_base: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachedRange {
    pub range: ByteRange,
    pub persisted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightDownload {
    pub range: ByteRange,
    pub downloader: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridSegmentCacheModel {
    cached: BTreeMap<String, Vec<CachedRange>>,
    in_flight: BTreeMap<String, Vec<InFlightDownload>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridCacheLookupOutcome {
    Hit {
        cached: Vec<ByteRange>,
    },
    Fetch {
        cached: Vec<ByteRange>,
        missing: Vec<ByteRange>,
        downloader: String,
    },
    Wait {
        cached: Vec<ByteRange>,
        missing: Vec<ByteRange>,
        downloader: String,
    },
}

impl HybridSegmentCacheModel {
    pub fn lookup_or_elect(
        &mut self,
        object_key: &str,
        requested: ByteRange,
        requester: impl Into<String>,
    ) -> HybridCacheLookupOutcome {
        let requester = requester.into();
        let cached = normalized_cached_ranges(
            self.cached
                .get(object_key)
                .into_iter()
                .flat_map(|entries| entries.iter().map(|entry| entry.range))
                .filter_map(|range| range.intersection(requested))
                .collect(),
        );
        let missing = subtract_ranges(requested, &cached);
        if missing.is_empty() {
            return HybridCacheLookupOutcome::Hit { cached };
        }

        if let Some(waiting_on) = self
            .in_flight
            .get(object_key)
            .into_iter()
            .flat_map(|entries| entries.iter())
            .find(|entry| {
                entry.downloader != requester
                    && missing.iter().any(|missing| entry.range.overlaps(*missing))
            })
            .map(|entry| entry.downloader.clone())
        {
            return HybridCacheLookupOutcome::Wait {
                cached,
                missing,
                downloader: waiting_on,
            };
        }

        let in_flight = self.in_flight.entry(object_key.to_string()).or_default();
        for range in &missing {
            if !in_flight
                .iter()
                .any(|entry| entry.range == *range && entry.downloader == requester)
            {
                in_flight.push(InFlightDownload {
                    range: *range,
                    downloader: requester.clone(),
                });
            }
        }
        HybridCacheLookupOutcome::Fetch {
            cached,
            missing,
            downloader: requester,
        }
    }

    pub fn admit(&mut self, object_key: &str, range: ByteRange, persisted: bool) {
        self.cached
            .entry(object_key.to_string())
            .or_default()
            .push(CachedRange { range, persisted });
    }

    pub fn complete_download(
        &mut self,
        object_key: &str,
        range: ByteRange,
        downloader: &str,
        persisted: bool,
    ) {
        if let Some(entries) = self.in_flight.get_mut(object_key) {
            entries.retain(|entry| !(entry.range == range && entry.downloader == downloader));
            if entries.is_empty() {
                self.in_flight.remove(object_key);
            }
        }
        self.admit(object_key, range, persisted);
    }

    pub fn cached_ranges(&self, object_key: &str) -> Vec<CachedRange> {
        self.cached.get(object_key).cloned().unwrap_or_default()
    }

    pub fn rebuild_after_restart(&self) -> Self {
        let cached = self
            .cached
            .iter()
            .map(|(object_key, entries)| {
                (
                    object_key.clone(),
                    entries
                        .iter()
                        .filter(|entry| entry.persisted)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, entries)| !entries.is_empty())
            .collect();
        Self {
            cached,
            in_flight: BTreeMap::new(),
        }
    }
}

fn normalized_cached_ranges(mut ranges: Vec<ByteRange>) -> Vec<ByteRange> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort();
    let mut merged: Vec<ByteRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.end >= range.start => {
                last.end = last.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

fn subtract_ranges(requested: ByteRange, covered: &[ByteRange]) -> Vec<ByteRange> {
    let mut missing = Vec::new();
    let mut cursor = requested.start;
    for cover in covered {
        if cursor < cover.start {
            missing.push(ByteRange::new(cursor, cover.start));
        }
        cursor = cursor.max(cover.end);
    }
    if cursor < requested.end {
        missing.push(ByteRange::new(cursor, requested.end));
    }
    missing
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridFeatureToggles {
    pub skip_indexes: bool,
    pub projection_sidecars: bool,
    pub compact_to_wide_promotion: bool,
    pub aggressive_background_repair: bool,
}

impl From<&HybridReadConfig> for HybridFeatureToggles {
    fn from(config: &HybridReadConfig) -> Self {
        Self {
            skip_indexes: config.skip_indexes_enabled,
            projection_sidecars: config.projection_sidecars_enabled,
            compact_to_wide_promotion: config.compact_to_wide_promotion_enabled,
            aggressive_background_repair: config.aggressive_background_repair,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridWorkloadConfig {
    pub table: String,
    pub steps: usize,
    pub key_count: usize,
    pub projection_fields: Vec<FieldId>,
    pub low_cache_budget: bool,
    pub feature_toggles: HybridFeatureToggles,
}

impl Default for HybridWorkloadConfig {
    fn default() -> Self {
        Self {
            table: "metrics".to_string(),
            steps: 16,
            key_count: 6,
            projection_fields: vec![FieldId::new(1)],
            low_cache_budget: false,
            feature_toggles: HybridFeatureToggles::default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridReadWorkloadOperation {
    PointRead {
        table: String,
        key: Vec<u8>,
    },
    ShortRangeScan {
        table: String,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    ProjectionRead {
        table: String,
        key: Vec<u8>,
        projection: RowProjection,
    },
    RemoteScan {
        table: String,
        start: Vec<u8>,
        end: Vec<u8>,
        projection: RowProjection,
        low_cache_budget: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HybridReadCutPoint {
    ColumnarSstableWriteBeforeFooterPublish,
    ColumnarCodecSubstreamWrite,
    ColumnarFooterPublish,
    PartDigestPublish,
    RemoteCacheSegmentAdmission,
    RemoteCacheBackgroundCompletion,
    SidecarPublishBeforeBaseVisibility,
    SidecarPublishAfterBaseVisibility,
    VerifyTransition,
    QuarantineTransition,
    RepairTransition,
    CompactToWidePromotion,
    CompactToWideReplacement,
}

impl HybridReadCutPoint {
    pub const ALL: [Self; 13] = [
        Self::ColumnarSstableWriteBeforeFooterPublish,
        Self::ColumnarCodecSubstreamWrite,
        Self::ColumnarFooterPublish,
        Self::PartDigestPublish,
        Self::RemoteCacheSegmentAdmission,
        Self::RemoteCacheBackgroundCompletion,
        Self::SidecarPublishBeforeBaseVisibility,
        Self::SidecarPublishAfterBaseVisibility,
        Self::VerifyTransition,
        Self::QuarantineTransition,
        Self::RepairTransition,
        Self::CompactToWidePromotion,
        Self::CompactToWideReplacement,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledHybridReadCutPoint {
    pub step: usize,
    pub cut_point: HybridReadCutPoint,
    pub replay_on_restart: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HybridReadScenario {
    pub seed: u64,
    pub config: HybridWorkloadConfig,
    pub operations: Vec<HybridReadWorkloadOperation>,
    pub cut_points: Vec<ScheduledHybridReadCutPoint>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryReading {
    pub device_id: String,
    pub reading_at_ms: u64,
    pub temperature_c: i64,
    pub humidity_pct: i64,
    pub battery_mv: i64,
    pub alert_active: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryDeviceState {
    pub device_id: String,
    pub latest_reading_at_ms: u64,
    pub temperature_c: i64,
    pub humidity_pct: i64,
    pub battery_mv: i64,
    pub alert_active: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryProjectionColumn {
    TemperatureC,
    HumidityPct,
    BatteryMv,
    AlertActive,
}

impl TelemetryProjectionColumn {
    pub const ALL: [Self; 4] = [
        Self::TemperatureC,
        Self::HumidityPct,
        Self::BatteryMv,
        Self::AlertActive,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryWindow {
    pub device_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryScanRequest {
    pub window: TelemetryWindow,
    pub columns: Vec<TelemetryProjectionColumn>,
    pub only_alerts: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryProjectedReading {
    pub device_id: String,
    pub reading_at_ms: u64,
    pub temperature_c: Option<i64>,
    pub humidity_pct: Option<i64>,
    pub battery_mv: Option<i64>,
    pub alert_active: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetryWindowSummary {
    pub device_id: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub reading_count: usize,
    pub alert_count: usize,
    pub min_temperature_c: Option<i64>,
    pub max_temperature_c: Option<i64>,
    pub average_temperature_c: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryAppOracle {
    readings_by_device: BTreeMap<String, BTreeMap<u64, TelemetryReading>>,
}

impl TelemetryAppOracle {
    pub fn apply_ingest<I>(&mut self, readings: I)
    where
        I: IntoIterator<Item = TelemetryReading>,
    {
        for reading in readings {
            self.readings_by_device
                .entry(reading.device_id.clone())
                .or_default()
                .insert(reading.reading_at_ms, reading);
        }
    }

    pub fn apply_operation(&mut self, operation: &TelemetryWorkloadOperation) {
        if let TelemetryWorkloadOperation::Ingest { readings } = operation {
            self.apply_ingest(readings.clone());
        }
    }

    pub fn latest_state(&self, device_id: &str) -> Option<TelemetryDeviceState> {
        let reading = self
            .readings_by_device
            .get(device_id)
            .and_then(|readings| readings.last_key_value())
            .map(|(_, reading)| reading)?;
        Some(TelemetryDeviceState {
            device_id: reading.device_id.clone(),
            latest_reading_at_ms: reading.reading_at_ms,
            temperature_c: reading.temperature_c,
            humidity_pct: reading.humidity_pct,
            battery_mv: reading.battery_mv,
            alert_active: reading.alert_active,
        })
    }

    pub fn scan(&self, request: &TelemetryScanRequest) -> Vec<TelemetryProjectedReading> {
        let readings = self
            .readings_by_device
            .get(&request.window.device_id)
            .into_iter()
            .flat_map(|device_readings| {
                device_readings.range(request.window.start_ms..request.window.end_ms)
            })
            .map(|(_, reading)| reading)
            .filter(|reading| !request.only_alerts || reading.alert_active);

        readings
            .map(|reading| projected_reading(reading, &request.columns))
            .collect()
    }

    pub fn summarize(&self, window: &TelemetryWindow) -> TelemetryWindowSummary {
        let readings = self
            .readings_by_device
            .get(&window.device_id)
            .into_iter()
            .flat_map(|device_readings| device_readings.range(window.start_ms..window.end_ms))
            .map(|(_, reading)| reading)
            .collect::<Vec<_>>();
        let reading_count = readings.len();
        let alert_count = readings
            .iter()
            .filter(|reading| reading.alert_active)
            .count();
        let min_temperature_c = readings.iter().map(|reading| reading.temperature_c).min();
        let max_temperature_c = readings.iter().map(|reading| reading.temperature_c).max();
        let average_temperature_c = (!readings.is_empty()).then(|| {
            readings
                .iter()
                .map(|reading| reading.temperature_c as f64)
                .sum::<f64>()
                / readings.len() as f64
        });

        TelemetryWindowSummary {
            device_id: window.device_id.clone(),
            start_ms: window.start_ms,
            end_ms: window.end_ms,
            reading_count,
            alert_count,
            min_temperature_c,
            max_temperature_c,
            average_temperature_c,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryWorkloadConfig {
    pub device_count: usize,
    pub readings_per_device: usize,
    pub base_timestamp_ms: u64,
    pub reading_interval_ms: u64,
    pub ingest_batch_size: usize,
    pub scan_columns: Vec<TelemetryProjectionColumn>,
    pub include_only_alert_queries: bool,
}

impl Default for TelemetryWorkloadConfig {
    fn default() -> Self {
        Self {
            device_count: 3,
            readings_per_device: 4,
            base_timestamp_ms: 1_000,
            reading_interval_ms: 25,
            ingest_batch_size: 2,
            scan_columns: vec![
                TelemetryProjectionColumn::TemperatureC,
                TelemetryProjectionColumn::AlertActive,
            ],
            include_only_alert_queries: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TelemetryWorkloadOperation {
    Ingest { readings: Vec<TelemetryReading> },
    ReadState { device_id: String },
    Scan { request: TelemetryScanRequest },
    Summarize { window: TelemetryWindow },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TelemetryWorkloadScenario {
    pub seed: u64,
    pub config: TelemetryWorkloadConfig,
    pub operations: Vec<TelemetryWorkloadOperation>,
}

#[derive(Clone, Debug)]
pub struct TelemetryWorkloadGenerator {
    seed: u64,
}

impl TelemetryWorkloadGenerator {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn generate(&self, config: &TelemetryWorkloadConfig) -> TelemetryWorkloadScenario {
        let rng = DeterministicRng::seeded(self.seed);
        let mut operations = Vec::new();
        let mut staged = Vec::new();

        for device_index in 0..config.device_count.max(1) {
            let device_id = format!("device-{device_index:02}");
            for reading_index in 0..config.readings_per_device.max(1) {
                let jitter = choose_index(&rng, 5) as u64;
                staged.push(TelemetryReading {
                    device_id: device_id.clone(),
                    reading_at_ms: config
                        .base_timestamp_ms
                        .saturating_add(device_index as u64)
                        .saturating_add(reading_index as u64 * config.reading_interval_ms.max(1)),
                    temperature_c: 20 + choose_index(&rng, 10) as i64 + jitter as i64,
                    humidity_pct: 35 + choose_index(&rng, 30) as i64,
                    battery_mv: 3600 - (reading_index as i64 * 10),
                    alert_active: rng.next_u64().is_multiple_of(3),
                });

                if staged.len() >= config.ingest_batch_size.max(1) {
                    operations.push(TelemetryWorkloadOperation::Ingest {
                        readings: std::mem::take(&mut staged),
                    });
                }
            }
        }
        if !staged.is_empty() {
            operations.push(TelemetryWorkloadOperation::Ingest { readings: staged });
        }

        for device_index in 0..config.device_count.max(1) {
            let device_id = format!("device-{device_index:02}");
            let window = TelemetryWindow {
                device_id: device_id.clone(),
                start_ms: config.base_timestamp_ms,
                end_ms: config.base_timestamp_ms.saturating_add(
                    config.readings_per_device.max(1) as u64 * config.reading_interval_ms.max(1),
                ),
            };
            operations.push(TelemetryWorkloadOperation::ReadState {
                device_id: device_id.clone(),
            });
            operations.push(TelemetryWorkloadOperation::Scan {
                request: TelemetryScanRequest {
                    window: window.clone(),
                    columns: config.scan_columns.clone(),
                    only_alerts: config.include_only_alert_queries
                        && rng.next_u64().is_multiple_of(2),
                },
            });
            operations.push(TelemetryWorkloadOperation::Summarize { window });
        }

        TelemetryWorkloadScenario {
            seed: self.seed,
            config: config.clone(),
            operations,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HybridReadWorkloadGenerator {
    seed: u64,
}

impl HybridReadWorkloadGenerator {
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }

    pub fn generate(&self, config: &HybridWorkloadConfig) -> HybridReadScenario {
        let rng = DeterministicRng::seeded(self.seed);
        let mut operations = Vec::with_capacity(config.steps);
        let mut cut_points = Vec::new();

        for step in 0..config.steps {
            let key_index = choose_index(&rng, config.key_count.max(1));
            let key = fixed_key(key_index);
            let start_index = choose_index(&rng, config.key_count.max(1));
            let end_index = (start_index + choose_index(&rng, config.key_count.max(1)).max(1))
                .min(config.key_count.max(1));
            let start = fixed_key(start_index);
            let end = fixed_key(end_index.max(start_index + 1));
            let projection = RowProjection::Fields(config.projection_fields.clone());
            let operation = match rng.next_u64() % 4 {
                0 => HybridReadWorkloadOperation::PointRead {
                    table: config.table.clone(),
                    key,
                },
                1 => HybridReadWorkloadOperation::ShortRangeScan {
                    table: config.table.clone(),
                    start,
                    end,
                },
                2 => HybridReadWorkloadOperation::ProjectionRead {
                    table: config.table.clone(),
                    key,
                    projection,
                },
                _ => HybridReadWorkloadOperation::RemoteScan {
                    table: config.table.clone(),
                    start,
                    end,
                    projection,
                    low_cache_budget: config.low_cache_budget,
                },
            };
            operations.push(operation);

            if rng.next_u64().is_multiple_of(3) {
                let cut_point =
                    HybridReadCutPoint::ALL[choose_index(&rng, HybridReadCutPoint::ALL.len())];
                cut_points.push(ScheduledHybridReadCutPoint {
                    step,
                    cut_point,
                    replay_on_restart: rng.next_u64().is_multiple_of(2),
                });
            }
        }

        HybridReadScenario {
            seed: self.seed,
            config: config.clone(),
            operations,
            cut_points,
        }
    }
}

fn projected_reading(
    reading: &TelemetryReading,
    columns: &[TelemetryProjectionColumn],
) -> TelemetryProjectedReading {
    let include = |column| columns.contains(&column);
    TelemetryProjectedReading {
        device_id: reading.device_id.clone(),
        reading_at_ms: reading.reading_at_ms,
        temperature_c: include(TelemetryProjectionColumn::TemperatureC)
            .then_some(reading.temperature_c),
        humidity_pct: include(TelemetryProjectionColumn::HumidityPct)
            .then_some(reading.humidity_pct),
        battery_mv: include(TelemetryProjectionColumn::BatteryMv).then_some(reading.battery_mv),
        alert_active: include(TelemetryProjectionColumn::AlertActive)
            .then_some(reading.alert_active),
    }
}

fn choose_index(rng: &DeterministicRng, bound: usize) -> usize {
    if bound == 0 {
        0
    } else {
        (rng.next_u64() as usize) % bound
    }
}

fn fixed_key(index: usize) -> Vec<u8> {
    format!("key-{index:02}").into_bytes()
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HybridReadCrashHarness {
    schedule: Vec<ScheduledHybridReadCutPoint>,
    fired: Vec<(u64, usize, HybridReadCutPoint)>,
    epoch: u64,
}

impl HybridReadCrashHarness {
    pub fn new(schedule: Vec<ScheduledHybridReadCutPoint>) -> Self {
        Self {
            schedule,
            fired: Vec::new(),
            epoch: 0,
        }
    }

    pub fn fire(
        &mut self,
        step: usize,
        cut_point: HybridReadCutPoint,
    ) -> Result<bool, HybridReadHarnessError> {
        let matching = self
            .schedule
            .iter()
            .filter(|item| item.step == step && item.cut_point == cut_point)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Ok(false);
        }
        if self
            .fired
            .iter()
            .any(|(epoch, fired_step, fired_cut_point)| {
                *epoch == self.epoch && *fired_step == step && *fired_cut_point == cut_point
            })
        {
            return Err(HybridReadHarnessError::DuplicateFire {
                epoch: self.epoch,
                step,
                cut_point,
            });
        }
        let replayable = matching.iter().any(|item| item.replay_on_restart);
        if self.epoch > 0
            && !replayable
            && self.fired.iter().any(|(_, fired_step, fired_cut_point)| {
                *fired_step == step && *fired_cut_point == cut_point
            })
        {
            return Err(HybridReadHarnessError::NonReplayableAfterRestart { step, cut_point });
        }

        self.fired.push((self.epoch, step, cut_point));
        Ok(true)
    }

    pub fn restart(&mut self) {
        self.epoch += 1;
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn consistency_check(&self) -> Result<(), HybridReadHarnessError> {
        for (index, (epoch, step, cut_point)) in self.fired.iter().enumerate() {
            if self.fired[..index].iter().any(
                |(previous_epoch, previous_step, previous_cut_point)| {
                    previous_epoch == epoch
                        && previous_step == step
                        && previous_cut_point == cut_point
                },
            ) {
                return Err(HybridReadHarnessError::DuplicateFire {
                    epoch: *epoch,
                    step: *step,
                    cut_point: *cut_point,
                });
            }
            let replayable = self
                .schedule
                .iter()
                .filter(|item| item.step == *step && item.cut_point == *cut_point)
                .any(|item| item.replay_on_restart);
            if *epoch > 0
                && !replayable
                && self.fired[..index]
                    .iter()
                    .any(|(_, previous_step, previous_cut_point)| {
                        previous_step == step && previous_cut_point == cut_point
                    })
            {
                return Err(HybridReadHarnessError::NonReplayableAfterRestart {
                    step: *step,
                    cut_point: *cut_point,
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HybridReadHarnessError {
    #[error("cut point {cut_point:?} fired twice in epoch {epoch} at step {step}")]
    DuplicateFire {
        epoch: u64,
        step: usize,
        cut_point: HybridReadCutPoint,
    },
    #[error("cut point {cut_point:?} at step {step} is not replayable after restart")]
    NonReplayableAfterRestart {
        step: usize,
        cut_point: HybridReadCutPoint,
    },
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HybridReadOracleError {
    #[error("unknown hybrid oracle table: {table}")]
    UnknownTable { table: String },
    #[error("hybrid oracle sequence regression: {previous} followed by {next}")]
    SequenceRegression {
        previous: SequenceNumber,
        next: SequenceNumber,
    },
    #[error("hybrid oracle table {table} does not expose compact decode metadata")]
    DecodeMetadataUnavailable { table: String },
    #[error("hybrid oracle decode metadata mismatch for {table}: {reason}")]
    DecodeMetadataMismatch { table: String, reason: String },
    #[error("row-format table unexpectedly produced record values")]
    UnexpectedRecord,
    #[error("columnar-format table unexpectedly produced byte values")]
    UnexpectedBytes,
    #[error("hybrid oracle could not derive a pruning expectation")]
    UnsupportedOraclePruning,
}

fn zone_map_predicate(predicate: &HybridPredicate) -> ZoneMapPredicate {
    match predicate {
        HybridPredicate::AlwaysTrue => ZoneMapPredicate::AlwaysTrue,
        HybridPredicate::FieldEquals { field_id, value } => ZoneMapPredicate::FieldEquals {
            field_id: *field_id,
            value: value.clone(),
        },
        HybridPredicate::Int64AtLeast { field_id, value } => ZoneMapPredicate::Int64AtLeast {
            field_id: *field_id,
            value: *value,
        },
        HybridPredicate::BoolEquals { field_id, value } => ZoneMapPredicate::BoolEquals {
            field_id: *field_id,
            value: *value,
        },
    }
}

fn build_zone_maps(rows: &[(Vec<u8>, HybridVersion)]) -> Vec<ZoneMapSynopsis> {
    let mut builders = BTreeMap::<FieldId, ZoneMapBuilder>::new();
    for (_, version) in rows {
        let Some(Value::Record(record)) = version.value.as_ref() else {
            continue;
        };
        for (&field_id, value) in record {
            builders.entry(field_id).or_default().observe(value);
        }
    }
    builders
        .into_iter()
        .map(|(field_id, builder)| ZoneMapSynopsis {
            field_id,
            min_value: builder.min_value,
            max_value: builder.max_value,
            null_count: builder.null_count,
        })
        .collect()
}

#[derive(Clone, Debug, Default)]
struct ZoneMapBuilder {
    min_value: Option<FieldValue>,
    max_value: Option<FieldValue>,
    null_count: u64,
}

impl ZoneMapBuilder {
    fn observe(&mut self, value: &FieldValue) {
        if matches!(value, FieldValue::Null) {
            self.null_count = self.null_count.saturating_add(1);
            return;
        }
        match self.min_value.as_ref() {
            Some(current) if !field_value_lt(value, current) => {}
            _ => self.min_value = Some(value.clone()),
        }
        match self.max_value.as_ref() {
            Some(current) if !field_value_gt(value, current) => {}
            _ => self.max_value = Some(value.clone()),
        }
    }
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
    use super::*;
    use terracedb::{FieldDefinition, FieldType};

    fn metric_schema() -> SchemaDefinition {
        SchemaDefinition {
            version: 1,
            fields: vec![
                FieldDefinition {
                    id: FieldId::new(1),
                    name: "user_id".to_string(),
                    field_type: FieldType::String,
                    nullable: false,
                    default: None,
                },
                FieldDefinition {
                    id: FieldId::new(2),
                    name: "count".to_string(),
                    field_type: FieldType::Int64,
                    nullable: false,
                    default: Some(FieldValue::Int64(0)),
                },
            ],
        }
    }

    fn metric_record(user_id: &str, count: i64) -> Value {
        Value::record(BTreeMap::from([
            (FieldId::new(1), FieldValue::String(user_id.to_string())),
            (FieldId::new(2), FieldValue::Int64(count)),
        ]))
    }

    #[test]
    fn hybrid_read_workload_generation_is_seed_stable() {
        let config = HybridWorkloadConfig::default();
        let first = HybridReadWorkloadGenerator::new(0x4849).generate(&config);
        let second = HybridReadWorkloadGenerator::new(0x4849).generate(&config);
        let different = HybridReadWorkloadGenerator::new(0x4850).generate(&config);

        assert_eq!(first, second);
        assert_ne!(first, different);
    }

    #[test]
    fn hybrid_read_oracle_projects_rows_and_selection_masks() {
        let mut oracle =
            HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
        oracle
            .apply(
                SequenceNumber::new(1),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:1".to_vec(),
                    value: metric_record("alice", 3),
                },
            )
            .expect("apply first row");
        oracle
            .apply(
                SequenceNumber::new(2),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:2".to_vec(),
                    value: metric_record("bob", 8),
                },
            )
            .expect("apply second row");

        let projected = oracle
            .scan(
                "metrics",
                SequenceNumber::new(2),
                &RowProjection::Fields(vec![FieldId::new(2)]),
            )
            .expect("projected scan");
        assert_eq!(
            projected,
            vec![
                (
                    b"user:1".to_vec(),
                    Value::record(BTreeMap::from([(FieldId::new(2), FieldValue::Int64(3))])),
                ),
                (
                    b"user:2".to_vec(),
                    Value::record(BTreeMap::from([(FieldId::new(2), FieldValue::Int64(8))])),
                ),
            ]
        );

        let (mask, filtered) = oracle
            .scan_with_selection(
                "metrics",
                SequenceNumber::new(2),
                &RowProjection::Fields(vec![FieldId::new(2)]),
                &HybridPredicate::Int64AtLeast {
                    field_id: FieldId::new(2),
                    value: 5,
                },
            )
            .expect("scan with selection");
        assert_eq!(mask.selected, vec![false, true]);
        assert_eq!(mask.selected_count(), 1);
        assert_eq!(
            filtered,
            vec![(
                b"user:2".to_vec(),
                Value::record(BTreeMap::from([(FieldId::new(2), FieldValue::Int64(8))])),
            )]
        );
    }

    #[test]
    fn hybrid_oracle_pruning_and_staged_scan_expectations_are_seed_stable() {
        let mut oracle =
            HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
        oracle
            .apply(
                SequenceNumber::new(1),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:1".to_vec(),
                    value: metric_record("alice", 3),
                },
            )
            .expect("apply first row");
        oracle
            .apply(
                SequenceNumber::new(2),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:2".to_vec(),
                    value: metric_record("bob", 8),
                },
            )
            .expect("apply second row");
        oracle
            .apply(
                SequenceNumber::new(3),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:3".to_vec(),
                    value: metric_record("carol", 12),
                },
            )
            .expect("apply third row");

        let pruning = oracle
            .pruning_expectation(
                "metrics",
                SequenceNumber::new(3),
                &HybridPredicate::Int64AtLeast {
                    field_id: FieldId::new(2),
                    value: 8,
                },
                2,
            )
            .expect("oracle pruning");
        let staged = oracle
            .staged_scan_with_selection(
                "metrics",
                SequenceNumber::new(3),
                RowProjection::Fields(vec![FieldId::new(2)]),
                RowProjection::Fields(vec![FieldId::new(1), FieldId::new(2)]),
                &HybridPredicate::Int64AtLeast {
                    field_id: FieldId::new(2),
                    value: 8,
                },
            )
            .expect("staged scan");

        assert_eq!(pruning.total_granules, 2);
        assert_eq!(pruning.selected_granules, vec![0, 1]);
        assert_eq!(pruning.survivor_rows, 2);
        assert!(pruning.overread_rows_upper_bound >= 2);
        assert!(staged.plan.needs_late_materialization());
        assert_eq!(staged.selection.selected, vec![false, true, true]);
        assert_eq!(staged.survivors.len(), 2);
    }

    #[test]
    fn hybrid_read_oracle_tracks_columnar_expectations() {
        let mut oracle =
            HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
        oracle
            .apply(
                SequenceNumber::new(1),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:1".to_vec(),
                    value: metric_record("alice", 3),
                },
            )
            .expect("apply metric row");

        let projection = RowProjection::Fields(vec![FieldId::new(2)]);
        let expectation = oracle
            .read_expectation("metrics", SequenceNumber::new(1), &projection)
            .expect("build columnar expectation");

        assert_eq!(
            expectation.decode_metadata,
            Some(HybridDecodeMetadata {
                schema_version: 1,
                fields: vec![
                    HybridDecodeField {
                        field_id: FieldId::new(1),
                        field_type: FieldType::String,
                        nullable: false,
                        has_default: false,
                    },
                    HybridDecodeField {
                        field_id: FieldId::new(2),
                        field_type: FieldType::Int64,
                        nullable: false,
                        has_default: true,
                    },
                ],
            })
        );
        assert_eq!(
            expectation.codecs,
            vec![
                HybridSubstreamCodec::None,
                HybridSubstreamCodec::Lz4,
                HybridSubstreamCodec::Zstd,
            ]
        );
    }

    #[test]
    fn hybrid_read_oracle_rejects_decode_metadata_mismatch() {
        let oracle =
            HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
        let mut metadata = oracle
            .compact_decode_metadata("metrics")
            .expect("decode metadata lookup")
            .expect("columnar metadata");
        metadata.fields[1].field_type = FieldType::Bool;

        assert_eq!(
            oracle.validate_decode_metadata("metrics", &metadata),
            Err(HybridReadOracleError::DecodeMetadataMismatch {
                table: "metrics".to_string(),
                reason: "field 2 metadata did not match expected layout".to_string(),
            })
        );
    }

    #[test]
    fn hybrid_segment_cache_model_covers_partial_population_and_restart_rebuild() {
        let mut cache = HybridSegmentCacheModel::default();
        let object_key = "cold/table-000001/SST-000001.sst";
        let requested = ByteRange::new(0, 100);

        assert_eq!(
            cache.lookup_or_elect(object_key, requested, "reader-a"),
            HybridCacheLookupOutcome::Fetch {
                cached: Vec::new(),
                missing: vec![requested],
                downloader: "reader-a".to_string(),
            }
        );

        cache.admit(object_key, ByteRange::new(0, 40), true);
        assert_eq!(
            cache.lookup_or_elect(object_key, requested, "reader-b"),
            HybridCacheLookupOutcome::Wait {
                cached: vec![ByteRange::new(0, 40)],
                missing: vec![ByteRange::new(40, 100)],
                downloader: "reader-a".to_string(),
            }
        );

        cache.complete_download(object_key, requested, "reader-a", true);
        let mut rebuilt = cache.rebuild_after_restart();
        assert_eq!(
            rebuilt.lookup_or_elect(object_key, requested, "reader-c"),
            HybridCacheLookupOutcome::Hit {
                cached: vec![requested],
            }
        );
        assert!(rebuilt.in_flight.is_empty());
    }

    #[test]
    fn hybrid_read_cut_points_replay_cleanly_after_restart() {
        for cut_point in HybridReadCutPoint::ALL {
            let mut harness = HybridReadCrashHarness::new(vec![ScheduledHybridReadCutPoint {
                step: 0,
                cut_point,
                replay_on_restart: true,
            }]);
            assert_eq!(harness.fire(0, cut_point), Ok(true));
            harness.restart();
            assert_eq!(harness.epoch(), 1);
            assert_eq!(harness.fire(0, cut_point), Ok(true));
            assert_eq!(harness.consistency_check(), Ok(()));
        }
    }

    #[test]
    fn optional_accelerants_can_stay_disabled_without_changing_baseline_rows() {
        let mut oracle =
            HybridReadOracle::new(&[HybridTableSpec::columnar("metrics", metric_schema())]);
        oracle
            .apply(
                SequenceNumber::new(1),
                HybridReadMutation::Put {
                    table: "metrics".to_string(),
                    key: b"user:1".to_vec(),
                    value: metric_record("alice", 5),
                },
            )
            .expect("apply row");
        let baseline = oracle
            .scan("metrics", SequenceNumber::new(1), &RowProjection::FullRow)
            .expect("baseline scan");

        let disabled = HybridFeatureToggles::default();
        let enabled = HybridFeatureToggles {
            skip_indexes: true,
            projection_sidecars: true,
            compact_to_wide_promotion: true,
            aggressive_background_repair: true,
        };
        let missing_resolution = HybridReadOracle::resolve_sidecar_fallback(
            HybridSidecarKind::Projection,
            HybridSidecarState::Absent,
        );
        let corrupt_resolution = HybridReadOracle::resolve_sidecar_fallback(
            HybridSidecarKind::SkipIndex,
            HybridSidecarState::Corrupt,
        );

        assert_eq!(baseline.len(), 1);
        assert!(missing_resolution.fallback_to_base);
        assert!(corrupt_resolution.fallback_to_base);
        assert_ne!(disabled, enabled);
        assert_eq!(
            oracle
                .scan("metrics", SequenceNumber::new(1), &RowProjection::FullRow)
                .expect("feature-disabled scan"),
            baseline
        );
    }

    #[test]
    fn telemetry_workload_generation_is_seed_stable() {
        let config = TelemetryWorkloadConfig::default();
        let first = TelemetryWorkloadGenerator::new(0x5858).generate(&config);
        let second = TelemetryWorkloadGenerator::new(0x5858).generate(&config);
        let different = TelemetryWorkloadGenerator::new(0x5859).generate(&config);

        assert_eq!(first, second);
        assert_ne!(first, different);
        assert!(
            first
                .operations
                .iter()
                .any(|operation| matches!(operation, TelemetryWorkloadOperation::Scan { .. }))
        );
    }

    #[test]
    fn telemetry_oracle_tracks_latest_state_scans_and_summaries() {
        let mut oracle = TelemetryAppOracle::default();
        oracle.apply_ingest([
            TelemetryReading {
                device_id: "device-01".to_string(),
                reading_at_ms: 100,
                temperature_c: 20,
                humidity_pct: 40,
                battery_mv: 3600,
                alert_active: false,
            },
            TelemetryReading {
                device_id: "device-01".to_string(),
                reading_at_ms: 200,
                temperature_c: 22,
                humidity_pct: 42,
                battery_mv: 3590,
                alert_active: true,
            },
            TelemetryReading {
                device_id: "device-01".to_string(),
                reading_at_ms: 300,
                temperature_c: 24,
                humidity_pct: 44,
                battery_mv: 3580,
                alert_active: true,
            },
        ]);

        assert_eq!(
            oracle.latest_state("device-01"),
            Some(TelemetryDeviceState {
                device_id: "device-01".to_string(),
                latest_reading_at_ms: 300,
                temperature_c: 24,
                humidity_pct: 44,
                battery_mv: 3580,
                alert_active: true,
            })
        );

        let projected = oracle.scan(&TelemetryScanRequest {
            window: TelemetryWindow {
                device_id: "device-01".to_string(),
                start_ms: 100,
                end_ms: 301,
            },
            columns: vec![
                TelemetryProjectionColumn::TemperatureC,
                TelemetryProjectionColumn::AlertActive,
            ],
            only_alerts: true,
        });
        assert_eq!(
            projected,
            vec![
                TelemetryProjectedReading {
                    device_id: "device-01".to_string(),
                    reading_at_ms: 200,
                    temperature_c: Some(22),
                    humidity_pct: None,
                    battery_mv: None,
                    alert_active: Some(true),
                },
                TelemetryProjectedReading {
                    device_id: "device-01".to_string(),
                    reading_at_ms: 300,
                    temperature_c: Some(24),
                    humidity_pct: None,
                    battery_mv: None,
                    alert_active: Some(true),
                },
            ]
        );

        let summary = oracle.summarize(&TelemetryWindow {
            device_id: "device-01".to_string(),
            start_ms: 100,
            end_ms: 301,
        });
        assert_eq!(summary.reading_count, 3);
        assert_eq!(summary.alert_count, 2);
        assert_eq!(summary.min_temperature_c, Some(20));
        assert_eq!(summary.max_temperature_c, Some(24));
        assert_eq!(summary.average_temperature_c, Some(22.0));
    }
}
