use super::*;

#[derive(Clone, Debug)]
pub(super) struct MemtableEntry {
    pub(super) user_key: Key,
    pub(super) sequence: SequenceNumber,
    pub(super) kind: ChangeKind,
    pub(super) value: Option<Value>,
    pub(super) size_bytes: u64,
}

impl MemtableEntry {
    pub(super) fn new(
        user_key: Key,
        sequence: SequenceNumber,
        kind: ChangeKind,
        value: Option<Value>,
    ) -> Self {
        let size_bytes = (user_key.len()
            + encode_mvcc_key(&user_key, CommitId::new(sequence)).len()
            + value.as_ref().map(value_size_bytes).unwrap_or_default())
            as u64;

        Self {
            user_key,
            sequence,
            kind,
            value,
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct TableMemtable {
    pub(super) entries: BTreeMap<Vec<u8>, MemtableEntry>,
    pub(super) pending_flush_bytes: u64,
    pub(super) min_sequence: Option<SequenceNumber>,
    pub(super) max_sequence: Option<SequenceNumber>,
}

impl TableMemtable {
    pub(super) fn insert(&mut self, entry: MemtableEntry) {
        let encoded_key = encode_mvcc_key(&entry.user_key, CommitId::new(entry.sequence));
        if let Some(replaced) = self.entries.insert(encoded_key, entry.clone()) {
            self.pending_flush_bytes = self.pending_flush_bytes.saturating_sub(replaced.size_bytes);
        }
        self.pending_flush_bytes = self.pending_flush_bytes.saturating_add(entry.size_bytes);
        self.min_sequence = Some(
            self.min_sequence
                .map(|current| current.min(entry.sequence))
                .unwrap_or(entry.sequence),
        );
        self.max_sequence = Some(
            self.max_sequence
                .map(|current| current.max(entry.sequence))
                .unwrap_or(entry.sequence),
        );
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn read_at(&self, key: &[u8], sequence: SequenceNumber) -> Option<MemtableEntry> {
        let seek = encode_mvcc_key(key, CommitId::new(sequence));
        let (_encoded_key, entry) = self.entries.range(seek..).next()?;
        (entry.user_key.as_slice() == key).then(|| entry.clone())
    }

    pub(super) fn collect_matching_keys(&self, matcher: &KeyMatcher<'_>, keys: &mut BTreeSet<Key>) {
        for entry in self.entries.values() {
            if matcher.matches(&entry.user_key) {
                keys.insert(entry.user_key.clone());
            }
        }
    }

    pub(super) fn collect_visible_rows(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        let seek = encode_mvcc_key(key, CommitId::new(sequence));
        for (_encoded_key, entry) in self.entries.range(seek..) {
            match entry.user_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if entry.sequence <= sequence {
                        rows.push(SstableRow {
                            key: entry.user_key.clone(),
                            sequence: entry.sequence,
                            kind: entry.kind,
                            value: entry.value.clone(),
                        });
                    }
                }
            }
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn sequence_range(&self) -> Option<(SequenceNumber, SequenceNumber)> {
        self.min_sequence.zip(self.max_sequence)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct Memtable {
    pub(super) tables: BTreeMap<TableId, TableMemtable>,
    pub(super) min_sequence: Option<SequenceNumber>,
    pub(super) max_sequence: SequenceNumber,
}

impl Memtable {
    pub(super) fn apply(&mut self, sequence: SequenceNumber, operation: &ResolvedBatchOperation) {
        self.min_sequence = Some(
            self.min_sequence
                .map(|current| current.min(sequence))
                .unwrap_or(sequence),
        );
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(operation.table_id)
            .or_default()
            .insert(MemtableEntry::new(
                operation.key.clone(),
                sequence,
                operation.kind,
                operation.value.clone(),
            ));
    }

    pub(super) fn apply_recovered_entry(&mut self, sequence: SequenceNumber, entry: &CommitEntry) {
        self.min_sequence = Some(
            self.min_sequence
                .map(|current| current.min(sequence))
                .unwrap_or(sequence),
        );
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(entry.table_id)
            .or_default()
            .insert(MemtableEntry::new(
                entry.key.clone(),
                sequence,
                entry.kind,
                entry.value.clone(),
            ));
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn read_at(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<MemtableEntry> {
        self.tables.get(&table_id)?.read_at(key, sequence)
    }

    pub(super) fn collect_matching_keys(
        &self,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        keys: &mut BTreeSet<Key>,
    ) {
        if let Some(table) = self.tables.get(&table_id) {
            table.collect_matching_keys(matcher, keys);
        }
    }

    pub(super) fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        if let Some(table) = self.tables.get(&table_id) {
            table.collect_visible_rows(key, sequence, rows);
        }
    }

    pub(super) fn force_collapse(
        &mut self,
        table_id: TableId,
        key: Key,
        sequence: SequenceNumber,
        value: Value,
    ) {
        self.min_sequence = Some(
            self.min_sequence
                .map(|current| current.min(sequence))
                .unwrap_or(sequence),
        );
        self.max_sequence = self.max_sequence.max(sequence);
        self.tables
            .entry(table_id)
            .or_default()
            .insert(MemtableEntry::new(
                key,
                sequence,
                ChangeKind::Put,
                Some(value),
            ));
    }

    pub(super) fn pending_flush_bytes(&self, table_id: TableId) -> u64 {
        self.tables
            .get(&table_id)
            .map(|table| table.pending_flush_bytes)
            .unwrap_or_default()
    }

    pub(super) fn pending_flush_bytes_by_table(&self) -> BTreeMap<TableId, u64> {
        self.tables
            .iter()
            .filter_map(|(&table_id, table)| {
                (table.pending_flush_bytes > 0).then_some((table_id, table.pending_flush_bytes))
            })
            .collect()
    }

    pub(super) fn total_pending_flush_bytes(&self) -> u64 {
        self.tables
            .values()
            .map(|table| table.pending_flush_bytes)
            .sum()
    }

    pub(super) fn pending_flush_sequence_range(
        &self,
        table_id: TableId,
    ) -> Option<(SequenceNumber, SequenceNumber)> {
        self.tables
            .get(&table_id)
            .and_then(TableMemtable::sequence_range)
    }

    pub(super) fn total_sequence_range(&self) -> Option<(SequenceNumber, SequenceNumber)> {
        self.min_sequence
            .zip((self.max_sequence != SequenceNumber::default()).then_some(self.max_sequence))
    }

    pub(super) fn has_entries_for_table(&self, table_id: TableId) -> bool {
        self.tables
            .get(&table_id)
            .map(|table| !table.is_empty())
            .unwrap_or(false)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tables.values().all(TableMemtable::is_empty)
    }

    pub(super) fn record_table_watermarks(
        &self,
        watermarks: &mut BTreeMap<TableId, SequenceNumber>,
    ) {
        for (&table_id, table) in &self.tables {
            let Some(sequence) = table.entries.values().map(|entry| entry.sequence).max() else {
                continue;
            };
            update_table_watermark(watermarks, table_id, sequence);
        }
    }
}

impl ResidentRowSstable {
    pub(super) fn is_columnar(&self) -> bool {
        self.columnar.is_some()
    }

    pub(super) fn collect_visible_rows(
        &self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        if key < self.meta.min_key.as_slice()
            || key > self.meta.max_key.as_slice()
            || sequence < self.meta.min_sequence
        {
            return;
        }

        if let Some(filter) = &self.user_key_bloom_filter
            && !filter.may_contain(key)
        {
            return;
        }

        let start = self.lower_bound(key);
        for row in &self.rows[start..] {
            match row.key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if row.sequence <= sequence {
                        rows.push(row.clone());
                    }
                }
            }
        }
    }

    pub(super) fn lower_bound(&self, target: &[u8]) -> usize {
        self.rows.partition_point(|row| row.key.as_slice() < target)
    }

    pub(super) async fn load_columnar_metadata(
        &self,
        columnar_read_context: &ColumnarReadContext,
        access: ColumnarReadAccessPattern,
    ) -> Result<LoadedColumnarMetadata, StorageError> {
        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let location = self.meta.storage_descriptor();
        let footer = columnar_read_context
            .footer_from_source(&self.meta, &columnar.source, location, access, true)
            .await?;
        Db::validate_loaded_columnar_footer(
            location,
            &self.meta,
            footer.footer.as_ref(),
            footer.footer.applied_generation.unwrap_or_default(),
        )?;

        let row_count = usize::try_from(footer.footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let key_index = columnar_read_context
            .key_index(&self.meta, &columnar.source, &footer, location, access)
            .await?;
        let sequences = columnar_read_context
            .sequence_column(&self.meta, &columnar.source, &footer, location, access)
            .await?;
        let tombstones = columnar_read_context
            .tombstone_bitmap(&self.meta, &columnar.source, &footer, location, access)
            .await?;
        let row_kinds = columnar_read_context
            .row_kind_column(&self.meta, &columnar.source, &footer, location, access)
            .await?;

        if key_index.len() != row_count
            || sequences.len() != row_count
            || tombstones.len() != row_count
            || row_kinds.len() != row_count
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} contains inconsistent block lengths",
            )));
        }

        Ok(LoadedColumnarMetadata {
            footer: footer.footer,
            footer_start: footer.footer_start,
            key_index,
            sequences,
            tombstones,
            row_kinds,
        })
    }

    async fn load_projection_sidecar(
        &self,
        columnar_read_context: &ColumnarReadContext,
        metadata: &LoadedColumnarMetadata,
        projection: &ColumnProjection,
    ) -> Result<Option<PersistedProjectionSidecarBody>, StorageError> {
        if !columnar_read_context.projection_sidecars_enabled {
            return Ok(None);
        }
        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let Some(descriptor) = metadata
            .footer
            .optional_sidecars
            .iter()
            .find_map(|descriptor| match descriptor {
                PersistedOptionalSidecarDescriptor::Projection(descriptor)
                    if Db::sidecar_projection_matches(&descriptor.projected_fields, projection) =>
                {
                    Some(descriptor)
                }
                PersistedOptionalSidecarDescriptor::Projection(_) => None,
                PersistedOptionalSidecarDescriptor::SkipIndex(_) => None,
            })
        else {
            return Ok(None);
        };

        let source = Db::sibling_source(&columnar.source, &descriptor.file_name);
        if let Some(marker) =
            Db::read_quarantine_marker(&columnar_read_context.dependencies, &source).await?
        {
            return Err(StorageError::corruption(format!(
                "projection sidecar {} is quarantined: {}",
                descriptor.projection_name, marker.reason
            )));
        }
        let Some(bytes) =
            Db::read_optional_source(&columnar_read_context.dependencies, &source).await?
        else {
            return Ok(None);
        };
        let file: PersistedProjectionSidecarFile =
            serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::corruption(format!(
                    "decode projection sidecar {} failed: {error}",
                    descriptor.projection_name
                ))
            })?;
        let body_bytes = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!(
                "encode projection sidecar {} body failed: {error}",
                descriptor.projection_name
            ))
        })?;
        if checksum32(&body_bytes) != file.checksum || file.checksum != descriptor.checksum {
            return Err(StorageError::corruption(format!(
                "projection sidecar {} checksum mismatch",
                descriptor.projection_name
            )));
        }
        let digest_input = Db::projection_sidecar_digest_input(&file.body)?;
        Db::validate_compact_digest(
            &file.body.digest,
            crate::ColumnarV2FormatTag::projection_sidecar(),
            &digest_input,
        )?;
        if file.body.format_version != COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION
            || file.body.table_id != self.meta.table_id
            || file.body.local_id != self.meta.local_id
            || file.body.schema_version != metadata.footer.schema_version
            || file.body.row_count != metadata.footer.row_count
            || file.body.projected_fields != descriptor.projected_fields
            || file.body.applied_generation != metadata.footer.applied_generation
        {
            return Err(StorageError::corruption(format!(
                "projection sidecar {} does not match its base SSTable",
                descriptor.projection_name
            )));
        }
        Ok(Some(file.body))
    }

    async fn load_skip_index_sidecar(
        &self,
        columnar_read_context: &ColumnarReadContext,
        metadata: &LoadedColumnarMetadata,
        descriptor: &PersistedSkipIndexSidecarDescriptor,
    ) -> Result<Option<PersistedSkipIndexSidecarBody>, StorageError> {
        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let source = Db::sibling_source(&columnar.source, &descriptor.file_name);
        if let Some(marker) =
            Db::read_quarantine_marker(&columnar_read_context.dependencies, &source).await?
        {
            return Err(StorageError::corruption(format!(
                "skip-index sidecar {} is quarantined: {}",
                descriptor.index_name, marker.reason
            )));
        }
        let Some(bytes) =
            Db::read_optional_source(&columnar_read_context.dependencies, &source).await?
        else {
            return Ok(None);
        };
        let file: PersistedSkipIndexSidecarFile =
            serde_json::from_slice(&bytes).map_err(|error| {
                StorageError::corruption(format!(
                    "decode skip-index sidecar {} failed: {error}",
                    descriptor.index_name
                ))
            })?;
        let body_bytes = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!(
                "encode skip-index sidecar {} body failed: {error}",
                descriptor.index_name
            ))
        })?;
        if checksum32(&body_bytes) != file.checksum || file.checksum != descriptor.checksum {
            return Err(StorageError::corruption(format!(
                "skip-index sidecar {} checksum mismatch",
                descriptor.index_name
            )));
        }
        let digest_input = Db::skip_index_sidecar_digest_input(&file.body)?;
        Db::validate_compact_digest(
            &file.body.digest,
            crate::ColumnarV2FormatTag::skip_index_sidecar(),
            &digest_input,
        )?;
        if file.body.format_version != COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION
            || file.body.table_id != self.meta.table_id
            || file.body.local_id != self.meta.local_id
            || file.body.schema_version != metadata.footer.schema_version
            || file.body.row_count != metadata.footer.row_count
            || file.body.family != descriptor.family
            || file.body.field_id != descriptor.field_id
            || file.body.applied_generation != metadata.footer.applied_generation
        {
            return Err(StorageError::corruption(format!(
                "skip-index sidecar {} does not match its base SSTable",
                descriptor.index_name
            )));
        }
        Ok(Some(file.body))
    }

    fn skip_index_payload_may_match(
        payload: &PersistedSkipIndexSidecarPayload,
        probe: &SkipIndexProbe,
    ) -> Result<bool, StorageError> {
        match (payload, probe) {
            (
                PersistedSkipIndexSidecarPayload::UserKeyBloom { filter },
                SkipIndexProbe::Key(key),
            ) => Ok(filter.may_contain(key)),
            (
                PersistedSkipIndexSidecarPayload::FieldValueBloom { filter },
                SkipIndexProbe::FieldEquals { value, .. },
            ) => Ok(filter.may_contain(&Db::encoded_field_value_bytes(value)?)),
            (
                PersistedSkipIndexSidecarPayload::BoundedSet { values, saturated },
                SkipIndexProbe::FieldEquals { value, .. },
            ) => Ok(*saturated || values.iter().any(|candidate| candidate == value)),
            _ => Ok(true),
        }
    }

    pub(super) async fn probe_skip_indexes_columnar(
        &self,
        columnar_read_context: &ColumnarReadContext,
        probe: &SkipIndexProbe,
        probe_field_id: Option<FieldId>,
    ) -> Result<SkipIndexProbeResult, StorageError> {
        if !columnar_read_context.skip_indexes_enabled {
            return Ok(SkipIndexProbeResult {
                local_id: self.meta.local_id.clone(),
                used_indexes: Vec::new(),
                may_match: true,
                fallback_to_base: false,
            });
        }
        let metadata = self
            .load_columnar_metadata(columnar_read_context, ColumnarReadAccessPattern::Point)
            .await?;
        let mut used_indexes = Vec::new();
        let mut may_match = true;
        let mut fallback_to_base = false;

        for descriptor in metadata
            .footer
            .optional_sidecars
            .iter()
            .filter_map(|descriptor| match descriptor {
                PersistedOptionalSidecarDescriptor::SkipIndex(descriptor) => Some(descriptor),
                PersistedOptionalSidecarDescriptor::Projection(_) => None,
            })
        {
            let relevant = match (descriptor.family, probe) {
                (HybridSkipIndexFamily::UserKeyBloom, SkipIndexProbe::Key(_)) => true,
                (
                    HybridSkipIndexFamily::FieldValueBloom | HybridSkipIndexFamily::BoundedSet,
                    SkipIndexProbe::FieldEquals { .. },
                ) => descriptor.field_id == probe_field_id,
                _ => false,
            };
            if !relevant {
                continue;
            }

            match self
                .load_skip_index_sidecar(columnar_read_context, &metadata, descriptor)
                .await
            {
                Ok(Some(body)) => {
                    used_indexes.push(descriptor.index_name.clone());
                    if !Self::skip_index_payload_may_match(&body.payload, probe)? {
                        may_match = false;
                    }
                }
                Ok(None) => fallback_to_base = true,
                Err(error) => {
                    fallback_to_base = true;
                    let columnar = self.columnar.as_ref().expect("columnar source present");
                    let source = Db::sibling_source(&columnar.source, &descriptor.file_name);
                    let _ = Db::quarantine_artifact(
                        &columnar_read_context.dependencies,
                        &columnar_read_context.dependencies.rng,
                        &source,
                        &self.meta.local_id,
                        error.message(),
                        columnar_read_context.dependencies.clock.now().get(),
                    )
                    .await;
                }
            }
        }

        Ok(SkipIndexProbeResult {
            local_id: self.meta.local_id.clone(),
            used_indexes,
            may_match,
            fallback_to_base,
        })
    }

    pub(super) async fn collect_visible_row_refs_for_key_columnar(
        &self,
        columnar_read_context: &ColumnarReadContext,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Vec<ColumnarRowRef>, StorageError> {
        if key < self.meta.min_key.as_slice()
            || key > self.meta.max_key.as_slice()
            || sequence < self.meta.min_sequence
        {
            return Ok(Vec::new());
        }
        if let Some(filter) = &self.user_key_bloom_filter
            && !filter.may_contain(key)
        {
            return Ok(Vec::new());
        }

        let metadata = self
            .load_columnar_metadata(columnar_read_context, ColumnarReadAccessPattern::Point)
            .await?;
        let start = metadata
            .key_index
            .partition_point(|candidate| candidate.as_slice() < key);
        let mut rows = Vec::new();

        for row_index in start..metadata.key_index.len() {
            match metadata.key_index[row_index].as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Greater => break,
                std::cmp::Ordering::Equal => {
                    if metadata.sequences[row_index] > sequence {
                        continue;
                    }
                    rows.push(ColumnarRowRef {
                        local_id: self.meta.local_id.clone(),
                        key: metadata.key_index[row_index].clone(),
                        sequence: metadata.sequences[row_index],
                        kind: Db::normalized_columnar_row_kind(
                            self.meta.storage_descriptor(),
                            row_index,
                            metadata.tombstones[row_index],
                            metadata.row_kinds[row_index],
                        )?,
                        row_index,
                    });
                }
            }
        }

        Ok(rows)
    }

    pub(super) async fn collect_scan_row_refs_columnar(
        &self,
        columnar_read_context: &ColumnarReadContext,
        matcher: &KeyMatcher<'_>,
        sequence: SequenceNumber,
    ) -> Result<Vec<ColumnarRowRef>, StorageError> {
        if sequence < self.meta.min_sequence {
            return Ok(Vec::new());
        }

        let metadata = self
            .load_columnar_metadata(columnar_read_context, ColumnarReadAccessPattern::Scan)
            .await?;
        let start = match matcher {
            KeyMatcher::Range { start, .. } | KeyMatcher::Prefix(start) => metadata
                .key_index
                .partition_point(|key| key.as_slice() < *start),
        };
        let mut rows = Vec::new();

        for row_index in start..metadata.key_index.len() {
            let key = &metadata.key_index[row_index];
            if !matcher.matches(key) {
                if matcher.is_past_end(key) {
                    break;
                }
                continue;
            }
            if metadata.sequences[row_index] > sequence {
                continue;
            }
            rows.push(ColumnarRowRef {
                local_id: self.meta.local_id.clone(),
                key: key.clone(),
                sequence: metadata.sequences[row_index],
                kind: Db::normalized_columnar_row_kind(
                    self.meta.storage_descriptor(),
                    row_index,
                    metadata.tombstones[row_index],
                    metadata.row_kinds[row_index],
                )?,
                row_index,
            });
        }

        Ok(rows)
    }

    pub(super) async fn materialize_columnar_rows(
        &self,
        columnar_read_context: &ColumnarReadContext,
        projection: &ColumnProjection,
        row_indexes: &BTreeSet<usize>,
        access: ColumnarReadAccessPattern,
    ) -> Result<ColumnarMaterialization, StorageError> {
        if row_indexes.is_empty() {
            return Ok(ColumnarMaterialization {
                rows: BTreeMap::new(),
                source: ScanMaterializationSource::BasePart,
            });
        }

        let columnar = self.columnar.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} is missing a storage source",
                self.meta.storage_descriptor()
            ))
        })?;
        let metadata = self
            .load_columnar_metadata(columnar_read_context, access)
            .await?;
        let location = self.meta.storage_descriptor();
        let row_count = metadata.key_index.len();
        let has_projection_sidecar =
            metadata
                .footer
                .optional_sidecars
                .iter()
                .any(|descriptor| match descriptor {
                    PersistedOptionalSidecarDescriptor::Projection(descriptor) => {
                        Db::sidecar_projection_matches(&descriptor.projected_fields, projection)
                    }
                    PersistedOptionalSidecarDescriptor::SkipIndex(_) => false,
                });
        match self
            .load_projection_sidecar(columnar_read_context, &metadata, projection)
            .await
        {
            Ok(Some(sidecar)) => {
                let mut materialized = BTreeMap::new();
                for &row_index in row_indexes {
                    let row_kind = Db::normalized_columnar_row_kind(
                        location,
                        row_index,
                        metadata.tombstones[row_index],
                        metadata.row_kinds[row_index],
                    )?;
                    if row_kind == ChangeKind::Delete {
                        return Err(StorageError::corruption(format!(
                            "columnar SSTable {location} delete row {row_index} was requested for materialization",
                        )));
                    }
                    let value = sidecar.rows.get(row_index).cloned().ok_or_else(|| {
                        StorageError::corruption(format!(
                            "projection sidecar {} omitted row {}",
                            sidecar.projection_name, row_index
                        ))
                    })?;
                    let Some(value) = value else {
                        return Err(StorageError::corruption(format!(
                            "projection sidecar {} stored delete row {} for materialization",
                            sidecar.projection_name, row_index
                        )));
                    };
                    materialized.insert(row_index, value);
                }
                return Ok(ColumnarMaterialization {
                    rows: materialized,
                    source: ScanMaterializationSource::ProjectionSidecar,
                });
            }
            Ok(None) => {}
            Err(error) => {
                if let Some(columnar) = &self.columnar {
                    let source = metadata
                        .footer
                        .optional_sidecars
                        .iter()
                        .find_map(|descriptor| match descriptor {
                            PersistedOptionalSidecarDescriptor::Projection(descriptor)
                                if Db::sidecar_projection_matches(
                                    &descriptor.projected_fields,
                                    projection,
                                ) =>
                            {
                                Some(Db::sibling_source(&columnar.source, &descriptor.file_name))
                            }
                            PersistedOptionalSidecarDescriptor::Projection(_) => None,
                            PersistedOptionalSidecarDescriptor::SkipIndex(_) => None,
                        });
                    if let Some(source) = source {
                        let _ = Db::quarantine_artifact(
                            &columnar_read_context.dependencies,
                            &columnar_read_context.dependencies.rng,
                            &source,
                            &self.meta.local_id,
                            error.message(),
                            columnar_read_context.dependencies.clock.now().get(),
                        )
                        .await;
                    }
                }
            }
        }
        let columns_by_field = metadata
            .footer
            .columns
            .iter()
            .map(|column| (column.field_id, column))
            .collect::<BTreeMap<_, _>>();
        let mut values_by_field = BTreeMap::<FieldId, Arc<Vec<FieldValue>>>::new();

        for field in &projection.fields {
            if let Some(column) = columns_by_field.get(&field.id) {
                if column.field_type != field.field_type {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} field {} type metadata does not match schema",
                        field.id.get()
                    )));
                }
                values_by_field.insert(
                    field.id,
                    columnar_read_context
                        .column_block(
                            &self.meta,
                            &columnar.source,
                            &CachedColumnarFooter {
                                footer: metadata.footer.clone(),
                                footer_start: metadata.footer_start,
                            },
                            column,
                            row_count,
                            location,
                            access,
                        )
                        .await?,
                );
            }
        }

        let mut materialized = BTreeMap::new();
        for &row_index in row_indexes {
            let row_kind = Db::normalized_columnar_row_kind(
                location,
                row_index,
                metadata.tombstones[row_index],
                metadata.row_kinds[row_index],
            )?;
            if row_kind == ChangeKind::Delete {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} delete row {row_index} was requested for materialization",
                )));
            }
            let mut record = ColumnarRecord::new();
            for field in &projection.fields {
                let value = match values_by_field.get(&field.id) {
                    Some(values) => values[row_index].clone(),
                    None => Db::missing_columnar_projection_value(field, row_kind)?,
                };
                record.insert(field.id, value);
            }
            materialized.insert(row_index, Value::Record(record));
        }

        Ok(ColumnarMaterialization {
            rows: materialized,
            source: if has_projection_sidecar {
                ScanMaterializationSource::ProjectionFallbackToBase
            } else {
                ScanMaterializationSource::BasePart
            },
        })
    }
}

impl SstableState {
    pub(super) fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        for sstable in &self.live {
            if sstable.meta.table_id != table_id {
                continue;
            }

            sstable.collect_visible_rows(key, sequence, rows);
        }
    }

    pub(super) fn table_stats(&self, table_id: TableId) -> (u32, u64, u64) {
        let matching = self
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id)
            .collect::<Vec<_>>();

        let l0_count = matching
            .iter()
            .filter(|sstable| sstable.meta.level == 0)
            .count() as u32;
        let total_bytes = matching
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        let local_bytes = matching
            .iter()
            .filter(|sstable| !sstable.meta.file_path.is_empty())
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();
        (l0_count, total_bytes, local_bytes)
    }

    pub(super) fn table_watermarks(&self) -> BTreeMap<TableId, SequenceNumber> {
        let mut watermarks = BTreeMap::new();
        for sstable in &self.live {
            update_table_watermark(
                &mut watermarks,
                sstable.meta.table_id,
                sstable.meta.max_sequence,
            );
        }
        watermarks
    }
}

#[derive(Clone, Debug)]
pub(super) enum ImmutableMemtableFlushState {
    Queued,
    Flushing,
}

#[derive(Clone, Debug)]
pub(super) struct ImmutableMemtable {
    pub(super) min_sequence: SequenceNumber,
    pub(super) max_sequence: SequenceNumber,
    pub(super) state: ImmutableMemtableFlushState,
    pub(super) memtable: Memtable,
}

#[derive(Clone, Debug, Default)]
pub(super) struct MemtableState {
    pub(super) mutable: Memtable,
    pub(super) immutables: Vec<ImmutableMemtable>,
}

impl MemtableState {
    pub(super) fn apply(
        &mut self,
        sequence: SequenceNumber,
        operations: &[ResolvedBatchOperation],
    ) {
        for operation in operations {
            self.mutable.apply(sequence, operation);
        }
    }

    pub(super) fn apply_recovered_record(&mut self, record: &CommitRecord) {
        for entry in &record.entries {
            self.mutable.apply_recovered_entry(record.sequence(), entry);
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn read_at(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Option<MemtableEntry> {
        let mut best = self.mutable.read_at(table_id, key, sequence);

        for immutable in self.immutables.iter().rev() {
            if immutable.max_sequence < sequence
                && let Some(current) = best.as_ref()
                && current.sequence >= immutable.max_sequence
            {
                continue;
            }

            let Some(candidate) = immutable.memtable.read_at(table_id, key, sequence) else {
                continue;
            };

            let replace = match best.as_ref() {
                Some(current) => candidate.sequence > current.sequence,
                None => true,
            };
            if replace {
                best = Some(candidate);
            }
        }

        best
    }

    pub(super) fn collect_matching_keys(
        &self,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        keys: &mut BTreeSet<Key>,
    ) {
        self.mutable.collect_matching_keys(table_id, matcher, keys);
        for immutable in &self.immutables {
            immutable
                .memtable
                .collect_matching_keys(table_id, matcher, keys);
        }
    }

    pub(super) fn collect_visible_rows(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        self.mutable
            .collect_visible_rows(table_id, key, sequence, rows);
        for immutable in &self.immutables {
            immutable
                .memtable
                .collect_visible_rows(table_id, key, sequence, rows);
        }
    }

    pub(super) fn force_collapse(
        &mut self,
        table_id: TableId,
        key: Key,
        sequence: SequenceNumber,
        value: Value,
    ) {
        self.mutable.force_collapse(table_id, key, sequence, value);
    }

    pub(super) fn rotate_mutable(&mut self) -> Option<SequenceNumber> {
        if self.mutable.is_empty() {
            return None;
        }

        let rotated = std::mem::take(&mut self.mutable);
        let min_sequence = rotated
            .min_sequence
            .expect("non-empty memtable should track a minimum sequence");
        let max_sequence = rotated.max_sequence;
        self.immutables.push(ImmutableMemtable {
            min_sequence,
            max_sequence,
            state: ImmutableMemtableFlushState::Queued,
            memtable: rotated,
        });
        Some(max_sequence)
    }

    pub(super) fn pending_flush_bytes(&self, table_id: TableId) -> u64 {
        let mutable_bytes = self.mutable.pending_flush_bytes(table_id);
        let immutable_bytes = self
            .immutables
            .iter()
            .map(|immutable| immutable.memtable.pending_flush_bytes(table_id))
            .sum::<u64>();

        mutable_bytes.saturating_add(immutable_bytes)
    }

    pub(super) fn immutable_flush_backlog_by_table(&self) -> BTreeMap<TableId, u64> {
        let mut backlog = BTreeMap::new();
        for immutable in &self.immutables {
            if !matches!(immutable.state, ImmutableMemtableFlushState::Queued) {
                continue;
            }
            for (table_id, bytes) in immutable.memtable.pending_flush_bytes_by_table() {
                backlog
                    .entry(table_id)
                    .and_modify(|current: &mut u64| *current = current.saturating_add(bytes))
                    .or_insert(bytes);
            }
        }
        backlog
    }

    pub(super) fn immutable_flushing_bytes_by_table(&self) -> BTreeMap<TableId, u64> {
        let mut flushing = BTreeMap::new();
        for immutable in &self.immutables {
            if !matches!(immutable.state, ImmutableMemtableFlushState::Flushing) {
                continue;
            }
            for (table_id, bytes) in immutable.memtable.pending_flush_bytes_by_table() {
                flushing
                    .entry(table_id)
                    .and_modify(|current: &mut u64| *current = current.saturating_add(bytes))
                    .or_insert(bytes);
            }
        }
        flushing
    }

    pub(super) fn total_pending_flush_bytes(&self) -> u64 {
        self.mutable.total_pending_flush_bytes().saturating_add(
            self.immutables
                .iter()
                .map(|immutable| immutable.memtable.total_pending_flush_bytes())
                .sum::<u64>(),
        )
    }

    pub(super) fn immutable_memtable_count(&self, table_id: TableId) -> u32 {
        self.immutables
            .iter()
            .filter(|immutable| immutable.memtable.has_entries_for_table(table_id))
            .count() as u32
    }

    pub(super) fn queued_immutable_memtable_count(&self, table_id: Option<TableId>) -> u32 {
        self.immutables
            .iter()
            .filter(|immutable| matches!(immutable.state, ImmutableMemtableFlushState::Queued))
            .filter(|immutable| {
                table_id
                    .map(|table_id| immutable.memtable.has_entries_for_table(table_id))
                    .unwrap_or(true)
            })
            .count() as u32
    }

    pub(super) fn flushing_immutable_memtable_count(&self, table_id: Option<TableId>) -> u32 {
        self.immutables
            .iter()
            .filter(|immutable| matches!(immutable.state, ImmutableMemtableFlushState::Flushing))
            .filter(|immutable| {
                table_id
                    .map(|table_id| immutable.memtable.has_entries_for_table(table_id))
                    .unwrap_or(true)
            })
            .count() as u32
    }

    pub(super) fn pending_flush_sequence_range(
        &self,
        table_id: TableId,
    ) -> Option<(SequenceNumber, SequenceNumber)> {
        let mut min_sequence = self
            .mutable
            .pending_flush_sequence_range(table_id)
            .map(|range| range.0);
        let mut max_sequence = self
            .mutable
            .pending_flush_sequence_range(table_id)
            .map(|range| range.1);
        for immutable in &self.immutables {
            let Some((immutable_min, immutable_max)) =
                immutable.memtable.pending_flush_sequence_range(table_id)
            else {
                continue;
            };
            min_sequence = Some(
                min_sequence
                    .map(|current| current.min(immutable_min))
                    .unwrap_or(immutable_min),
            );
            max_sequence = Some(
                max_sequence
                    .map(|current| current.max(immutable_max))
                    .unwrap_or(immutable_max),
            );
        }
        min_sequence.zip(max_sequence)
    }

    pub(super) fn total_sequence_range(&self) -> Option<(SequenceNumber, SequenceNumber)> {
        let mut min_sequence = self.mutable.total_sequence_range().map(|range| range.0);
        let mut max_sequence = self.mutable.total_sequence_range().map(|range| range.1);
        for immutable in &self.immutables {
            min_sequence = Some(
                min_sequence
                    .map(|current| current.min(immutable.min_sequence))
                    .unwrap_or(immutable.min_sequence),
            );
            max_sequence = Some(
                max_sequence
                    .map(|current| current.max(immutable.max_sequence))
                    .unwrap_or(immutable.max_sequence),
            );
        }
        min_sequence.zip(max_sequence)
    }

    pub(super) fn mark_queued_immutables_flushing(&mut self) -> Vec<ImmutableMemtable> {
        let mut flushing = Vec::new();
        for immutable in &mut self.immutables {
            if matches!(immutable.state, ImmutableMemtableFlushState::Queued) {
                immutable.state = ImmutableMemtableFlushState::Flushing;
                flushing.push(immutable.clone());
            }
        }
        flushing
    }

    pub(super) fn restore_flushing_immutables(&mut self) {
        for immutable in &mut self.immutables {
            if matches!(immutable.state, ImmutableMemtableFlushState::Flushing) {
                immutable.state = ImmutableMemtableFlushState::Queued;
            }
        }
    }

    pub(super) fn remove_flushing_immutables(&mut self, count: usize) {
        if count == 0 {
            return;
        }

        let mut remaining = count;
        self.immutables.retain(|immutable| {
            if remaining > 0 && matches!(immutable.state, ImmutableMemtableFlushState::Flushing) {
                remaining = remaining.saturating_sub(1);
                false
            } else {
                true
            }
        });
    }

    pub(super) fn table_watermarks(&self) -> BTreeMap<TableId, SequenceNumber> {
        let mut watermarks = BTreeMap::new();
        self.mutable.record_table_watermarks(&mut watermarks);
        for immutable in &self.immutables {
            immutable.memtable.record_table_watermarks(&mut watermarks);
        }
        watermarks
    }
}

pub(super) fn update_table_watermark(
    watermarks: &mut BTreeMap<TableId, SequenceNumber>,
    table_id: TableId,
    sequence: SequenceNumber,
) {
    watermarks
        .entry(table_id)
        .and_modify(|current| *current = (*current).max(sequence))
        .or_insert(sequence);
}

#[derive(Clone, Debug)]
pub(super) enum KeyMatcher<'a> {
    Range { start: &'a [u8], end: &'a [u8] },
    Prefix(&'a [u8]),
}

impl KeyMatcher<'_> {
    pub(super) fn start_bound(&self) -> &[u8] {
        match self {
            Self::Range { start, .. } | Self::Prefix(start) => start,
        }
    }

    pub(super) fn exclusive_upper_bound(&self) -> Option<Key> {
        match self {
            Self::Range { end, .. } => Some(end.to_vec()),
            Self::Prefix(prefix) => prefix_exclusive_upper_bound(prefix),
        }
    }

    pub(super) fn matches(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { start, end } => key >= *start && key < *end,
            Self::Prefix(prefix) => key.starts_with(prefix),
        }
    }

    pub(super) fn is_past_end(&self, key: &[u8]) -> bool {
        match self {
            Self::Range { end, .. } => key >= *end,
            Self::Prefix(prefix) => key > *prefix,
        }
    }
}

pub(super) fn prefix_exclusive_upper_bound(prefix: &[u8]) -> Option<Key> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] == u8::MAX {
            continue;
        }

        upper[index] = upper[index].saturating_add(1);
        upper.truncate(index + 1);
        return Some(upper);
    }

    None
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScanDirection {
    Forward,
    Reverse,
}

pub(super) struct TableMemtableCursor<'a> {
    pub(super) range: std::collections::btree_map::Range<'a, Vec<u8>, MemtableEntry>,
    pub(super) current: Option<&'a MemtableEntry>,
    pub(super) direction: ScanDirection,
}

impl<'a> TableMemtableCursor<'a> {
    pub(super) fn new(
        table: &'a TableMemtable,
        matcher: &KeyMatcher<'_>,
        direction: ScanDirection,
    ) -> Self {
        let upper = matcher.exclusive_upper_bound();
        let bounds = (
            Bound::Included(matcher.start_bound()),
            match upper.as_ref() {
                Some(upper) => Bound::Excluded(upper.as_slice()),
                None => Bound::Unbounded,
            },
        );
        let mut range = table.entries.range::<[u8], _>(bounds);
        let current = match direction {
            ScanDirection::Forward => range.next().map(|(_, entry)| entry),
            ScanDirection::Reverse => range.next_back().map(|(_, entry)| entry),
        };

        Self {
            range,
            current,
            direction,
        }
    }

    pub(super) fn current_key(&self) -> Option<&[u8]> {
        self.current.map(|entry| entry.user_key.as_slice())
    }

    pub(super) fn take_key_rows(
        &mut self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        while self.current_key() == Some(key) {
            let entry = self
                .current
                .take()
                .expect("current memtable entry should exist");
            if entry.sequence <= sequence {
                rows.push(SstableRow {
                    key: entry.user_key.clone(),
                    sequence: entry.sequence,
                    kind: entry.kind,
                    value: entry.value.clone(),
                });
            }
            self.advance();
        }
    }

    pub(super) fn advance(&mut self) {
        self.current = match self.direction {
            ScanDirection::Forward => self.range.next().map(|(_, entry)| entry),
            ScanDirection::Reverse => self.range.next_back().map(|(_, entry)| entry),
        };
    }
}

pub(super) struct ResidentRowSstableCursor<'a> {
    pub(super) rows: &'a [SstableRow],
    pub(super) start_index: usize,
    pub(super) end_index: usize,
    pub(super) current_index: Option<usize>,
    pub(super) direction: ScanDirection,
}

impl<'a> ResidentRowSstableCursor<'a> {
    pub(super) fn new(
        sstable: &'a ResidentRowSstable,
        matcher: &KeyMatcher<'_>,
        direction: ScanDirection,
    ) -> Self {
        let start_index = sstable.lower_bound(matcher.start_bound());
        let end_index = matcher
            .exclusive_upper_bound()
            .as_ref()
            .map(|upper| sstable.lower_bound(upper))
            .unwrap_or_else(|| sstable.rows.len());
        let current_index = if start_index < end_index {
            match direction {
                ScanDirection::Forward => Some(start_index),
                ScanDirection::Reverse => Some(end_index - 1),
            }
        } else {
            None
        };

        Self {
            rows: &sstable.rows,
            start_index,
            end_index,
            current_index,
            direction,
        }
    }

    pub(super) fn current(&self) -> Option<&'a SstableRow> {
        self.current_index.map(|index| &self.rows[index])
    }

    pub(super) fn current_key(&self) -> Option<&[u8]> {
        self.current().map(|row| row.key.as_slice())
    }

    pub(super) fn take_key_rows(
        &mut self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        while self.current_key() == Some(key) {
            let row = self.current().expect("current SSTable row should exist");
            if row.sequence <= sequence {
                rows.push(row.clone());
            }
            self.advance();
        }
    }

    pub(super) fn advance(&mut self) {
        let Some(index) = self.current_index else {
            return;
        };

        self.current_index = match self.direction {
            ScanDirection::Forward => {
                let next = index.saturating_add(1);
                (next < self.end_index).then_some(next)
            }
            ScanDirection::Reverse => index
                .checked_sub(1)
                .filter(|next| *next >= self.start_index),
        };
    }
}

pub(super) enum RowScanSourceCursor<'a> {
    Memtable(TableMemtableCursor<'a>),
    Sstable(ResidentRowSstableCursor<'a>),
}

impl<'a> RowScanSourceCursor<'a> {
    pub(super) fn current_key(&self) -> Option<&[u8]> {
        match self {
            Self::Memtable(cursor) => cursor.current_key(),
            Self::Sstable(cursor) => cursor.current_key(),
        }
    }

    pub(super) fn take_key_rows(
        &mut self,
        key: &[u8],
        sequence: SequenceNumber,
        rows: &mut Vec<SstableRow>,
    ) {
        match self {
            Self::Memtable(cursor) => cursor.take_key_rows(key, sequence, rows),
            Self::Sstable(cursor) => cursor.take_key_rows(key, sequence, rows),
        }
    }
}

pub(super) struct MergedRowRangeIterator<'a> {
    pub(super) sources: Vec<RowScanSourceCursor<'a>>,
    pub(super) direction: ScanDirection,
}

impl<'a> MergedRowRangeIterator<'a> {
    pub(super) fn new(
        memtables: &'a MemtableState,
        sstables: &'a SstableState,
        table_id: TableId,
        matcher: &KeyMatcher<'_>,
        direction: ScanDirection,
    ) -> Self {
        let mut sources = Vec::new();

        if let Some(table) = memtables.mutable.tables.get(&table_id) {
            sources.push(RowScanSourceCursor::Memtable(TableMemtableCursor::new(
                table, matcher, direction,
            )));
        }
        for immutable in &memtables.immutables {
            if let Some(table) = immutable.memtable.tables.get(&table_id) {
                sources.push(RowScanSourceCursor::Memtable(TableMemtableCursor::new(
                    table, matcher, direction,
                )));
            }
        }
        for sstable in &sstables.live {
            if sstable.meta.table_id != table_id || sstable.is_columnar() {
                continue;
            }
            sources.push(RowScanSourceCursor::Sstable(ResidentRowSstableCursor::new(
                sstable, matcher, direction,
            )));
        }

        Self { sources, direction }
    }

    pub(super) fn next_key_rows(
        &mut self,
        sequence: SequenceNumber,
    ) -> Option<(Key, Vec<SstableRow>)> {
        let target_key = self
            .sources
            .iter()
            .filter_map(RowScanSourceCursor::current_key)
            .reduce(|left, right| match self.direction {
                ScanDirection::Forward => left.min(right),
                ScanDirection::Reverse => left.max(right),
            })?
            .to_vec();
        let mut rows = Vec::new();

        for source in &mut self.sources {
            if source.current_key() == Some(target_key.as_slice()) {
                source.take_key_rows(&target_key, sequence, &mut rows);
            }
        }

        Some((target_key, rows))
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct SnapshotTracker {
    pub(super) registrations: BTreeMap<u64, SequenceNumber>,
    pub(super) counts_by_sequence: BTreeMap<SequenceNumber, usize>,
}

impl SnapshotTracker {
    pub(super) fn register(&mut self, id: u64, sequence: SequenceNumber) {
        self.registrations.insert(id, sequence);
        *self.counts_by_sequence.entry(sequence).or_default() += 1;
    }

    pub(super) fn release(&mut self, id: u64) {
        let Some(sequence) = self.registrations.remove(&id) else {
            return;
        };

        let mut remove_key = false;
        if let Some(count) = self.counts_by_sequence.get_mut(&sequence) {
            *count = count.saturating_sub(1);
            remove_key = *count == 0;
        }

        if remove_key {
            self.counts_by_sequence.remove(&sequence);
        }
    }

    pub(super) fn oldest_active(&self) -> Option<SequenceNumber> {
        self.counts_by_sequence.keys().next().copied()
    }

    pub(super) fn count(&self) -> u64 {
        self.registrations.len() as u64
    }
}

pub(super) fn encode_mvcc_key(user_key: &[u8], commit_id: CommitId) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(user_key.len() + 1 + CommitId::ENCODED_LEN);
    encoded.extend_from_slice(user_key);
    encoded.push(MVCC_KEY_SEPARATOR);
    encoded.extend(commit_id.encode().into_iter().map(|byte| !byte));
    encoded
}

#[cfg(test)]
pub(super) fn decode_mvcc_key(encoded: &[u8]) -> Result<(Key, CommitId), StorageError> {
    if encoded.len() < CommitId::ENCODED_LEN + 1 {
        return Err(StorageError::corruption("mvcc key is too short"));
    }

    let separator_index = encoded.len() - CommitId::ENCODED_LEN - 1;
    if encoded[separator_index] != MVCC_KEY_SEPARATOR {
        return Err(StorageError::corruption("mvcc key missing separator"));
    }

    let mut commit_bytes = [0_u8; CommitId::ENCODED_LEN];
    for (decoded, source) in commit_bytes.iter_mut().zip(&encoded[separator_index + 1..]) {
        *decoded = !*source;
    }

    let commit_id = CommitId::decode(&commit_bytes)
        .map_err(|error| StorageError::corruption(format!("decode mvcc key failed: {error}")))?;
    Ok((encoded[..separator_index].to_vec(), commit_id))
}

pub(super) fn value_size_bytes(value: &Value) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.len(),
        Value::Record(record) => record.values().map(field_value_size_bytes).sum(),
    }
}

pub(super) fn field_value_size_bytes(value: &FieldValue) -> usize {
    match value {
        FieldValue::Null => 0,
        FieldValue::Int64(_) | FieldValue::Float64(_) => 8,
        FieldValue::String(value) => value.len(),
        FieldValue::Bytes(value) => value.len(),
        FieldValue::Bool(_) => 1,
    }
}

impl PersistedCatalog {
    pub(super) fn from_tables(tables: &BTreeMap<String, StoredTable>) -> Self {
        Self {
            format_version: CATALOG_FORMAT_VERSION,
            tables: tables
                .values()
                .map(PersistedCatalogEntry::from_stored)
                .collect(),
        }
    }
}

impl Default for PersistedCatalog {
    fn default() -> Self {
        Self {
            format_version: CATALOG_FORMAT_VERSION,
            tables: Vec::new(),
        }
    }
}

impl PersistedCatalogEntry {
    pub(super) fn from_stored(table: &StoredTable) -> Self {
        Self {
            id: table.id,
            config: PersistedTableConfig {
                name: table.config.name.clone(),
                format: table.config.format,
                max_merge_operand_chain_length: table.config.max_merge_operand_chain_length,
                bloom_filter_bits_per_key: table.config.bloom_filter_bits_per_key,
                history_retention_sequences: table.config.history_retention_sequences,
                compaction_strategy: table.config.compaction_strategy,
                schema: table.config.schema.clone(),
                metadata: table.config.metadata.clone(),
            },
        }
    }

    pub(super) fn into_stored(self) -> StoredTable {
        StoredTable {
            id: self.id,
            config: TableConfig {
                name: self.config.name,
                format: self.config.format,
                merge_operator: None,
                max_merge_operand_chain_length: self.config.max_merge_operand_chain_length,
                compaction_filter: None,
                bloom_filter_bits_per_key: self.config.bloom_filter_bits_per_key,
                history_retention_sequences: self.config.history_retention_sequences,
                compaction_strategy: self.config.compaction_strategy,
                schema: self.config.schema,
                metadata: self.config.metadata,
            },
        }
    }
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("storage", &self.inner.config.storage)
            .field("current_sequence", &self.current_sequence())
            .field("current_durable_sequence", &self.current_durable_sequence())
            .finish()
    }
}
