use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb::{
    ByteRange, DeterministicRng, FieldId, FieldValue, HybridReadConfig, Rng, RowProjection,
    SchemaDefinition, SelectionMask, SequenceNumber, TableFormat, Value,
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
    V2SstableWriteBeforeFooterPublish,
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
    pub const ALL: [Self; 11] = [
        Self::V2SstableWriteBeforeFooterPublish,
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
    #[error("row-format table unexpectedly produced record values")]
    UnexpectedRecord,
    #[error("columnar-format table unexpectedly produced byte values")]
    UnexpectedBytes,
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
}
