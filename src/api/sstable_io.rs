use super::*;

type ColumnarSubstreamDescriptor = (
    Option<FieldId>,
    Option<FieldType>,
    crate::ColumnarSubstreamKind,
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColumnarOutputLayout {
    Compact,
    Wide,
}

impl ColumnarReadContext {
    pub(super) fn cache_usage_snapshot(&self) -> ColumnarCacheUsageSnapshot {
        let mut usage = self.decoded_cache.usage_snapshot(
            self.raw_byte_cache_budget_bytes,
            &self.cache_domain_paths,
            &self.cache_lane_budgets,
        );
        let state = self.raw_byte_cache_budget_state.lock();
        usage.raw_byte_entries = state.lengths.len();
        usage.raw_byte_bytes = state.total_bytes;
        for (domain, domain_usage) in &mut usage.by_domain {
            if let Some((lane, _)) = self
                .cache_domain_paths
                .iter()
                .find(|(_, mapped_domain)| *mapped_domain == domain)
            {
                domain_usage.raw_byte_entries = state
                    .owners
                    .values()
                    .filter(|owner| **owner == *lane)
                    .count();
                domain_usage.raw_byte_bytes = state
                    .owners
                    .iter()
                    .filter(|(_, owner)| **owner == *lane)
                    .map(|(key, _)| state.lengths.get(key).copied().unwrap_or_default())
                    .sum();
            }
        }
        usage
    }

    pub(super) async fn trim_raw_byte_cache_to_budget(&self) -> Result<(), StorageError> {
        let Some(cache) = &self.remote_cache else {
            return Ok(());
        };

        let evictions = {
            let mut state = self.raw_byte_cache_budget_state.lock();
            self.collect_raw_byte_cache_evictions(&mut state, None)
        };
        for evicted in evictions {
            cache.remove_span(&evicted.object_key, evicted.span).await?;
        }
        Ok(())
    }

    pub(super) fn cache_policy(
        &self,
        source: &StorageSource,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> ColumnarCachePolicy {
        let use_decoded_cache = self.decoded_cache_enabled.load(Ordering::Relaxed);
        let use_raw_byte_cache = matches!(source, StorageSource::RemoteObject { .. })
            && self.raw_byte_cache_enabled.load(Ordering::Relaxed)
            && self.remote_cache.is_some()
            && match artifact {
                ColumnarReadArtifact::Footer | ColumnarReadArtifact::Metadata => true,
                ColumnarReadArtifact::ColumnBlock => access == ColumnarReadAccessPattern::Point,
            };
        let populate_hot_data = match artifact {
            ColumnarReadArtifact::Footer | ColumnarReadArtifact::Metadata => {
                access != ColumnarReadAccessPattern::Recovery
            }
            ColumnarReadArtifact::ColumnBlock => access == ColumnarReadAccessPattern::Point,
        };

        ColumnarCachePolicy {
            use_raw_byte_cache,
            populate_raw_byte_cache: use_raw_byte_cache
                && populate_hot_data
                && self
                    .raw_byte_cache_population_enabled
                    .load(Ordering::Relaxed),
            use_decoded_cache: use_decoded_cache && access != ColumnarReadAccessPattern::Recovery,
            populate_decoded_cache: use_decoded_cache && populate_hot_data,
        }
    }

    fn should_use_exact_raw_byte_cache_read(
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> bool {
        matches!(
            access,
            ColumnarReadAccessPattern::Scan
                | ColumnarReadAccessPattern::Background
                | ColumnarReadAccessPattern::Recovery
        ) && matches!(
            artifact,
            ColumnarReadArtifact::Footer | ColumnarReadArtifact::Metadata
        )
    }

    async fn read_exact_remote_range_via_raw_cache(
        &self,
        key: &str,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        populate_raw_byte_cache: bool,
        access: ColumnarReadAccessPattern,
    ) -> Result<Vec<u8>, StorageError> {
        let bytes = UnifiedStorage::new(
            self.dependencies.file_system.clone(),
            self.dependencies.object_store.clone(),
            None,
        )
        .read_range(source, range.clone())
        .await
        .map_err(|error| error.into_storage_error())?;
        if populate_raw_byte_cache && let Some(cache) = &self.remote_cache {
            cache
                .store(
                    key,
                    crate::remote::CacheSpan::Range {
                        start: range.start,
                        end: range.end,
                    },
                    &bytes,
                )
                .await?;
            self.admit_raw_byte_cache_range(key, range, &bytes, access)
                .await?;
        }
        Ok(bytes)
    }

    async fn read_range_with_policy(
        &self,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
        policy: ColumnarCachePolicy,
    ) -> Result<Vec<u8>, StorageError> {
        if let StorageSource::RemoteObject { key } = source
            && policy.use_raw_byte_cache
            && let Some(cache) = &self.remote_cache
        {
            if let Some(bytes) = cache.read_range(key, range.clone()).await? {
                self.decoded_cache
                    .stats
                    .raw_byte_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(bytes);
            }
            self.decoded_cache
                .stats
                .raw_byte_misses
                .fetch_add(1, Ordering::Relaxed);
            if Self::should_use_exact_raw_byte_cache_read(access, artifact) {
                return self
                    .read_exact_remote_range_via_raw_cache(
                        key,
                        source,
                        range,
                        policy.populate_raw_byte_cache,
                        access,
                    )
                    .await;
            }
        }

        let bytes = UnifiedStorage::new(
            self.dependencies.file_system.clone(),
            self.dependencies.object_store.clone(),
            if policy.populate_raw_byte_cache {
                self.remote_cache.clone()
            } else {
                None
            },
        )
        .read_range(source, range.clone())
        .await
        .map_err(|error| error.into_storage_error())?;
        if let StorageSource::RemoteObject { key } = source
            && policy.populate_raw_byte_cache
        {
            self.admit_raw_byte_cache_range(key, range.clone(), &bytes, access)
                .await?;
        }
        Ok(bytes)
    }

    pub(super) async fn read_range(
        &self,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> Result<Vec<u8>, StorageError> {
        let policy = self.cache_policy(source, access, artifact);
        self.read_range_with_policy(source, range, access, artifact, policy)
            .await
    }

    async fn admit_raw_byte_cache_range(
        &self,
        object_key: &str,
        range: std::ops::Range<u64>,
        _bytes: &[u8],
        access: ColumnarReadAccessPattern,
    ) -> Result<(), StorageError> {
        let Some(cache) = &self.remote_cache else {
            return Ok(());
        };

        let entries = cache.entries_snapshot();
        let lengths = entries
            .iter()
            .map(|entry| {
                (
                    RawByteCacheBudgetKey {
                        object_key: entry.object_key.clone(),
                        span: entry.span,
                    },
                    entry.data_len,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let order = entries
            .into_iter()
            .map(|entry| RawByteCacheBudgetKey {
                object_key: entry.object_key,
                span: entry.span,
            })
            .collect::<VecDeque<_>>();

        let evictions = {
            let mut state = self.raw_byte_cache_budget_state.lock();
            state.total_bytes = cache.total_cached_bytes();
            state.order = order;
            state.lengths = lengths;
            let lane = self.cache_lane(access);
            let matching_keys = state
                .lengths
                .keys()
                .filter(|key| {
                    key.object_key == object_key
                        && matches!(
                            key.span,
                            crate::remote::CacheSpan::Range { start, end }
                                if start < range.end && end > range.start
                        )
                })
                .cloned()
                .collect::<Vec<_>>();
            let live_keys = state.lengths.keys().cloned().collect::<BTreeSet<_>>();
            state.owners.retain(|key, _| live_keys.contains(key));
            for key in matching_keys {
                state.owners.insert(key, lane);
            }
            self.collect_raw_byte_cache_evictions(&mut state, None)
        };
        for evicted in evictions {
            cache.remove_span(&evicted.object_key, evicted.span).await?;
        }
        Ok(())
    }

    fn cache_lane(&self, access: ColumnarReadAccessPattern) -> crate::ExecutionLane {
        match access {
            ColumnarReadAccessPattern::Point | ColumnarReadAccessPattern::Scan => {
                crate::ExecutionLane::UserForeground
            }
            ColumnarReadAccessPattern::Background => crate::ExecutionLane::UserBackground,
            ColumnarReadAccessPattern::Recovery => crate::ExecutionLane::ControlPlane,
        }
    }

    fn collect_raw_byte_cache_evictions(
        &self,
        state: &mut RawByteCacheBudgetState,
        preserve: Option<&RawByteCacheBudgetKey>,
    ) -> Vec<RawByteCacheBudgetKey> {
        let mut evictions = Vec::new();
        for (&lane, budget) in &self.cache_lane_budgets {
            loop {
                let lane_bytes = state
                    .owners
                    .iter()
                    .filter(|(_, owner)| **owner == lane)
                    .map(|(key, _)| state.lengths.get(key).copied().unwrap_or_default())
                    .sum::<u64>();
                if lane_bytes <= budget.raw_byte_budget_bytes {
                    break;
                }
                let position = state.order.iter().position(|key| {
                    preserve != Some(key) && state.owners.get(key).copied() == Some(lane)
                });
                let Some(position) = position else {
                    break;
                };
                let Some(next) = state.order.remove(position) else {
                    break;
                };
                if let Some(bytes) = state.lengths.remove(&next) {
                    state.total_bytes = state.total_bytes.saturating_sub(bytes);
                    state.owners.remove(&next);
                    evictions.push(next);
                }
            }
        }
        while state.total_bytes > self.raw_byte_cache_budget_bytes {
            let Some(next) = state.order.pop_front() else {
                break;
            };
            if preserve == Some(&next) {
                state.order.push_back(next);
                if state.order.len() <= 1 {
                    break;
                }
                continue;
            }
            if let Some(bytes) = state.lengths.remove(&next) {
                state.total_bytes = state.total_bytes.saturating_sub(bytes);
                state.owners.remove(&next);
                evictions.push(next);
            }
        }
        evictions
    }

    pub(super) async fn footer_from_source(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        location: &str,
        access: ColumnarReadAccessPattern,
        populate_raw_byte_cache: bool,
    ) -> Result<CachedColumnarFooter, StorageError> {
        let identity = meta.columnar_identity();
        let mut policy = self.cache_policy(source, access, ColumnarReadArtifact::Footer);
        if !populate_raw_byte_cache {
            policy.populate_raw_byte_cache = false;
        }
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.footer(&identity)
        {
            return Ok(cached);
        }

        let min_len = (COLUMNAR_SSTABLE_MAGIC.len() * 2 + std::mem::size_of::<u64>()) as u64;
        if meta.length < min_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is too short",
            )));
        }

        let trailer_start = meta
            .length
            .saturating_sub((COLUMNAR_SSTABLE_MAGIC.len() + std::mem::size_of::<u64>()) as u64);
        let trailer = self
            .read_range_with_policy(
                source,
                trailer_start..meta.length,
                access,
                ColumnarReadArtifact::Footer,
                policy,
            )
            .await?;
        let footer_len = u64::from_le_bytes(
            trailer[..std::mem::size_of::<u64>()]
                .try_into()
                .map_err(|_| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} footer length trailer is truncated",
                    ))
                })?,
        );
        if &trailer[std::mem::size_of::<u64>()..] != COLUMNAR_SSTABLE_MAGIC {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the trailer magic",
            )));
        }

        let footer_start_u64 = trailer_start.checked_sub(footer_len).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer length points before the file start",
            ))
        })?;
        let footer_bytes = self
            .read_range_with_policy(
                source,
                footer_start_u64..trailer_start,
                access,
                ColumnarReadArtifact::Footer,
                policy,
            )
            .await?;
        let footer = Arc::new(serde_json::from_slice(&footer_bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar SSTable footer for {location} failed: {error}"
            ))
        })?);

        let footer_start = usize::try_from(footer_start_u64).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer offset exceeds platform limits",
            ))
        })?;
        let cached = CachedColumnarFooter {
            footer,
            footer_start,
        };
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_footer(identity, cached.clone(), self.cache_lane(access));
        }
        Ok(cached)
    }

    pub(super) async fn key_index(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        footer: &CachedColumnarFooter,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<Arc<Vec<Key>>, StorageError> {
        let identity = meta.columnar_identity();
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::Metadata);
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.key_index(&identity)
        {
            return Ok(cached);
        }

        let range = Db::columnar_block_range(
            location,
            footer.footer_start,
            "key index",
            &footer.footer.key_index,
        )?;
        let bytes = self
            .read_range(source, range, access, ColumnarReadArtifact::Metadata)
            .await?;
        let row_count = usize::try_from(footer.footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let values: Arc<Vec<Key>> = Arc::new(
            Db::decode_columnar_key_index(
                location,
                footer.footer.as_ref(),
                &footer.footer.key_index,
                &bytes,
                row_count,
            )
            .map_err(|error| {
                StorageError::corruption(format!(
                    "decode columnar key index for {location} failed: {error}"
                ))
            })?,
        );
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_key_index(identity, values.clone(), self.cache_lane(access));
        }
        Ok(values)
    }

    pub(super) async fn sequence_column(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        footer: &CachedColumnarFooter,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<Arc<Vec<SequenceNumber>>, StorageError> {
        let identity = meta.columnar_identity();
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::Metadata);
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.sequence_column(&identity)
        {
            return Ok(cached);
        }

        let range = Db::columnar_block_range(
            location,
            footer.footer_start,
            "sequence column",
            &footer.footer.sequence_column,
        )?;
        let bytes = self
            .read_range(source, range, access, ColumnarReadArtifact::Metadata)
            .await?;
        let row_count = usize::try_from(footer.footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let values: Arc<Vec<SequenceNumber>> = Arc::new(
            Db::decode_columnar_sequences(
                location,
                footer.footer.as_ref(),
                &footer.footer.sequence_column,
                &bytes,
                row_count,
            )
            .map_err(|error| {
                StorageError::corruption(format!(
                    "decode columnar sequence column for {location} failed: {error}"
                ))
            })?,
        );
        if policy.populate_decoded_cache {
            self.decoded_cache.insert_sequence_column(
                identity,
                values.clone(),
                self.cache_lane(access),
            );
        }
        Ok(values)
    }

    pub(super) async fn tombstone_bitmap(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        footer: &CachedColumnarFooter,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<Arc<Vec<bool>>, StorageError> {
        let identity = meta.columnar_identity();
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::Metadata);
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.tombstone_bitmap(&identity)
        {
            return Ok(cached);
        }

        let range = Db::columnar_block_range(
            location,
            footer.footer_start,
            "tombstone bitmap",
            &footer.footer.tombstone_bitmap,
        )?;
        let bytes = self
            .read_range(source, range, access, ColumnarReadArtifact::Metadata)
            .await?;
        let row_count = usize::try_from(footer.footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let values: Arc<Vec<bool>> = Arc::new(
            Db::decode_columnar_tombstones(
                location,
                footer.footer.as_ref(),
                &footer.footer.tombstone_bitmap,
                &bytes,
                row_count,
            )
            .map_err(|error| {
                StorageError::corruption(format!(
                    "decode columnar tombstone bitmap for {location} failed: {error}"
                ))
            })?,
        );
        if policy.populate_decoded_cache {
            self.decoded_cache.insert_tombstone_bitmap(
                identity,
                values.clone(),
                self.cache_lane(access),
            );
        }
        Ok(values)
    }

    pub(super) async fn row_kind_column(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        footer: &CachedColumnarFooter,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<Arc<Vec<ChangeKind>>, StorageError> {
        let identity = meta.columnar_identity();
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::Metadata);
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.row_kind_column(&identity)
        {
            return Ok(cached);
        }

        let range = Db::columnar_block_range(
            location,
            footer.footer_start,
            "row-kind column",
            &footer.footer.row_kind_column,
        )?;
        let bytes = self
            .read_range(source, range, access, ColumnarReadArtifact::Metadata)
            .await?;
        let row_count = usize::try_from(footer.footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let values: Arc<Vec<ChangeKind>> = Arc::new(
            Db::decode_columnar_row_kinds(
                location,
                footer.footer.as_ref(),
                &footer.footer.row_kind_column,
                &bytes,
                row_count,
            )
            .map_err(|error| {
                StorageError::corruption(format!(
                    "decode columnar row-kind column for {location} failed: {error}"
                ))
            })?,
        );
        if policy.populate_decoded_cache {
            self.decoded_cache.insert_row_kind_column(
                identity,
                values.clone(),
                self.cache_lane(access),
            );
        }
        Ok(values)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn column_block(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        footer: &CachedColumnarFooter,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<Arc<Vec<FieldValue>>, StorageError> {
        let key = ColumnarColumnCacheKey {
            sstable: meta.columnar_identity(),
            field_id: column.field_id,
        };
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::ColumnBlock);
        if policy.use_decoded_cache
            && let Some(cached) = self.decoded_cache.column_block(&key)
        {
            return Ok(cached);
        }

        let range =
            Db::columnar_block_range(location, footer.footer_start, "column block", &column.block)?;
        let bytes = self
            .read_range(source, range, access, ColumnarReadArtifact::ColumnBlock)
            .await?;
        let values = Arc::new(Db::decode_columnar_field_values(
            location,
            footer.footer.as_ref(),
            column,
            row_count,
            &bytes,
        )?);
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_column_block(key, values.clone(), self.cache_lane(access));
        }
        Ok(values)
    }
}

#[derive(Clone, Debug)]
struct ResolvedSkipIndexSpec {
    name: String,
    family: HybridSkipIndexFamily,
    field: Option<FieldDefinition>,
    max_values: usize,
}

#[derive(Clone, Debug)]
struct ResolvedProjectionSidecarSpec {
    name: String,
    projection: ColumnProjection,
}

#[derive(Clone, Debug)]
enum EncodedOptionalSidecar {
    SkipIndex {
        descriptor: PersistedSkipIndexSidecarDescriptor,
        bytes: Vec<u8>,
    },
    Projection {
        descriptor: PersistedProjectionSidecarDescriptor,
        bytes: Vec<u8>,
    },
}

impl EncodedOptionalSidecar {
    fn file_name(&self) -> &str {
        match self {
            Self::SkipIndex { descriptor, .. } => descriptor.file_name.as_str(),
            Self::Projection { descriptor, .. } => descriptor.file_name.as_str(),
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::SkipIndex { bytes, .. } | Self::Projection { bytes, .. } => bytes,
        }
    }

    fn descriptor(&self) -> PersistedOptionalSidecarDescriptor {
        match self {
            Self::SkipIndex { descriptor, .. } => {
                PersistedOptionalSidecarDescriptor::SkipIndex(descriptor.clone())
            }
            Self::Projection { descriptor, .. } => {
                PersistedOptionalSidecarDescriptor::Projection(descriptor.clone())
            }
        }
    }
}

impl Db {
    pub(super) fn compact_crc32_digest(
        format_tag: crate::ColumnarV2FormatTag,
        logical_bytes: u64,
        bytes: &[u8],
    ) -> CompactPartDigest {
        CompactPartDigest {
            format_tag,
            algorithm: PartDigestAlgorithm::Crc32,
            logical_bytes,
            digest_bytes: checksum32(bytes).to_be_bytes().to_vec(),
        }
    }

    pub(super) fn validate_compact_digest(
        digest: &CompactPartDigest,
        expected_format_tag: crate::ColumnarV2FormatTag,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        if digest.format_tag != expected_format_tag {
            return Err(StorageError::corruption(format!(
                "artifact digest format tag mismatch: expected {:?}, found {:?}",
                expected_format_tag.kind, digest.format_tag.kind
            )));
        }
        if digest.algorithm != PartDigestAlgorithm::Crc32 {
            return Err(StorageError::unsupported(
                "only crc32 compact digests are supported",
            ));
        }
        if digest.logical_bytes != bytes.len() as u64 {
            return Err(StorageError::corruption(format!(
                "artifact digest length mismatch: expected {}, found {}",
                digest.logical_bytes,
                bytes.len()
            )));
        }
        if digest.digest_bytes != checksum32(bytes).to_be_bytes() {
            return Err(StorageError::corruption("artifact compact digest mismatch"));
        }
        Ok(())
    }

    fn sanitize_artifact_name_component(component: &str) -> String {
        let sanitized = component
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                    ch
                } else {
                    '_'
                }
            })
            .collect::<String>();
        if sanitized.is_empty() {
            "artifact".to_string()
        } else {
            sanitized
        }
    }

    fn sibling_path(base_path: &str, file_name: &str) -> String {
        let parent = PathBuf::from(base_path)
            .parent()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_default();
        if parent.is_empty() {
            file_name.to_string()
        } else {
            Self::join_fs_path(&parent, file_name)
        }
    }

    fn sibling_remote_key(base_key: &str, file_name: &str) -> String {
        match base_key.rsplit_once('/') {
            Some((prefix, _)) => format!("{prefix}/{file_name}"),
            None => file_name.to_string(),
        }
    }

    pub(super) fn sibling_source(source: &StorageSource, file_name: &str) -> StorageSource {
        match source {
            StorageSource::LocalFile { path } => {
                StorageSource::local_file(Self::sibling_path(path, file_name))
            }
            StorageSource::RemoteObject { key } => {
                StorageSource::remote_object(Self::sibling_remote_key(key, file_name))
            }
        }
    }

    fn artifact_quarantine_source(source: &StorageSource) -> StorageSource {
        match source {
            StorageSource::LocalFile { path } => {
                StorageSource::local_file(format!("{path}.quarantine.json"))
            }
            StorageSource::RemoteObject { key } => {
                StorageSource::remote_object(format!("{key}.quarantine.json"))
            }
        }
    }

    pub(super) async fn read_optional_source(
        dependencies: &DbDependencies,
        source: &StorageSource,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        match source {
            StorageSource::LocalFile { path } => read_optional_path(dependencies, path).await,
            StorageSource::RemoteObject { key } => {
                read_optional_remote_object(dependencies, key).await
            }
        }
    }

    pub(super) async fn write_source_atomic(
        dependencies: &DbDependencies,
        rng: &Arc<dyn crate::Rng>,
        source: &StorageSource,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        match source {
            StorageSource::LocalFile { path } => {
                write_local_file_atomic(dependencies, path, bytes).await
            }
            StorageSource::RemoteObject { key } => {
                let temp_key = format!("{key}.tmp.{}", rng.uuid());
                dependencies.object_store.put(&temp_key, bytes).await?;
                dependencies.object_store.copy(&temp_key, key).await?;
                let _ = dependencies.object_store.delete(&temp_key).await;
                Ok(())
            }
        }
    }

    pub(super) async fn delete_source_if_exists(
        dependencies: &DbDependencies,
        source: &StorageSource,
    ) -> Result<(), StorageError> {
        match source {
            StorageSource::LocalFile { path } => {
                delete_local_file_if_exists(dependencies, path).await
            }
            StorageSource::RemoteObject { key } => {
                match dependencies.object_store.delete(key).await {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == StorageErrorKind::NotFound => Ok(()),
                    Err(error) => Err(error),
                }
            }
        }
    }

    pub(super) async fn read_quarantine_marker(
        dependencies: &DbDependencies,
        source: &StorageSource,
    ) -> Result<Option<PersistedArtifactQuarantineMarker>, StorageError> {
        let Some(bytes) =
            Self::read_optional_source(dependencies, &Self::artifact_quarantine_source(source))
                .await?
        else {
            return Ok(None);
        };
        let marker = serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!("decode artifact quarantine marker failed: {error}"))
        })?;
        Ok(Some(marker))
    }

    pub(super) async fn quarantine_artifact(
        dependencies: &DbDependencies,
        rng: &Arc<dyn crate::Rng>,
        source: &StorageSource,
        local_id: &str,
        reason: &str,
        now_millis: u64,
    ) -> Result<(), StorageError> {
        let marker = PersistedArtifactQuarantineMarker {
            format_version: 1,
            local_id: local_id.to_string(),
            reason: reason.to_string(),
            quarantined_at_millis: now_millis,
        };
        let payload = serde_json::to_vec(&marker).map_err(|error| {
            StorageError::corruption(format!("encode artifact quarantine marker failed: {error}"))
        })?;
        Self::write_source_atomic(
            dependencies,
            rng,
            &Self::artifact_quarantine_source(source),
            &payload,
        )
        .await
    }

    pub(super) fn sidecar_projection_matches(
        projected_fields: &[FieldId],
        projection: &ColumnProjection,
    ) -> bool {
        let mut left = projected_fields.to_vec();
        left.sort();
        let mut right = projection
            .fields
            .iter()
            .map(|field| field.id)
            .collect::<Vec<_>>();
        right.sort();
        left == right
    }

    fn skip_index_file_name(local_id: &str, name: &str) -> String {
        format!(
            "{local_id}.skip-index.{}.json",
            Self::sanitize_artifact_name_component(name)
        )
    }

    fn projection_sidecar_file_name(local_id: &str, name: &str) -> String {
        format!(
            "{local_id}.projection.{}.json",
            Self::sanitize_artifact_name_component(name)
        )
    }

    #[allow(dead_code)]
    pub(super) async fn load_resident_sstable(
        dependencies: &DbDependencies,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let columnar_read_context = Self::ephemeral_columnar_read_context(dependencies);
        Self::load_resident_sstable_with_context(
            dependencies,
            &columnar_read_context,
            ManifestId::default(),
            meta,
        )
        .await
    }

    pub(super) async fn load_resident_sstable_with_context(
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
        manifest_generation: ManifestId,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        if meta.schema_version.is_some() {
            Self::load_lazy_columnar_sstable(columnar_read_context, manifest_generation, meta).await
        } else {
            let source = meta.storage_source();
            let location = meta.storage_descriptor();
            let bytes = read_source(dependencies, &source).await?;
            if bytes.len() as u64 != meta.length {
                return Err(StorageError::corruption(format!(
                    "sstable {} length mismatch: manifest={}, file={}",
                    location,
                    meta.length,
                    bytes.len()
                )));
            }
            Self::decode_resident_row_sstable(location, meta, &bytes)
        }
    }

    #[allow(dead_code)]
    pub(super) async fn read_exact_source_range(
        dependencies: &DbDependencies,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        location: &str,
        label: &str,
    ) -> Result<Vec<u8>, StorageError> {
        let columnar_read_context = Self::ephemeral_columnar_read_context(dependencies);
        Self::read_exact_source_range_with_context(
            &columnar_read_context,
            source,
            range,
            location,
            label,
            ColumnarReadAccessPattern::Point,
            ColumnarReadArtifact::Metadata,
        )
        .await
    }

    #[allow(dead_code)]
    pub(super) async fn read_exact_source_range_with_context(
        columnar_read_context: &ColumnarReadContext,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        location: &str,
        label: &str,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> Result<Vec<u8>, StorageError> {
        let expected_len =
            usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {label} length exceeds platform limits",
                ))
            })?;
        let bytes = columnar_read_context
            .read_range(source, range, access, artifact)
            .await?;
        if bytes.len() != expected_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} is truncated",
            )));
        }
        Ok(bytes)
    }

    #[allow(dead_code)]
    pub(super) async fn columnar_footer_from_source(
        dependencies: &DbDependencies,
        source: &StorageSource,
        length: u64,
        location: &str,
    ) -> Result<(PersistedColumnarSstableFooter, usize), StorageError> {
        let meta = PersistedManifestSstable {
            table_id: TableId::new(0),
            level: 0,
            local_id: location.to_string(),
            file_path: match source {
                StorageSource::LocalFile { path } => path.clone(),
                StorageSource::RemoteObject { .. } => String::new(),
            },
            remote_key: match source {
                StorageSource::RemoteObject { key } => Some(key.clone()),
                StorageSource::LocalFile { .. } => None,
            },
            length,
            checksum: 0,
            data_checksum: 0,
            min_key: Vec::new(),
            max_key: Vec::new(),
            min_sequence: SequenceNumber::new(0),
            max_sequence: SequenceNumber::new(0),
            schema_version: Some(0),
        };
        let columnar_read_context = Self::ephemeral_columnar_read_context(dependencies);
        let footer = columnar_read_context
            .footer_from_source(
                &meta,
                source,
                location,
                ColumnarReadAccessPattern::Point,
                true,
            )
            .await?;
        Ok(((*footer.footer).clone(), footer.footer_start))
    }

    pub(super) fn validate_loaded_columnar_footer(
        location: &str,
        meta: &PersistedManifestSstable,
        footer: &PersistedColumnarSstableFooter,
        manifest_generation: ManifestId,
    ) -> Result<(), StorageError> {
        if footer.table_id != meta.table_id
            || footer.level != meta.level
            || footer.local_id != meta.local_id
            || footer.min_key != meta.min_key
            || footer.max_key != meta.max_key
            || footer.min_sequence != meta.min_sequence
            || footer.max_sequence != meta.max_sequence
            || Some(footer.schema_version) != meta.schema_version
        {
            return Err(StorageError::corruption(format!(
                "manifest metadata does not match SSTable {location}",
            )));
        }

        if footer.format_version != COLUMNAR_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported columnar SSTable version {}",
                footer.format_version
            )));
        }

        let layout = &footer.layout;
        if layout.format_tag != crate::ColumnarFormatTag::base_part()
            || layout.table_id != footer.table_id
            || layout.local_id != footer.local_id
            || layout.schema_version != footer.schema_version
            || layout.row_count != footer.row_count
            || layout.decode_metadata.schema_version != footer.schema_version
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} footer metadata does not match the persisted footer",
            )));
        }
        let mut decode_field_ids = BTreeSet::new();
        for field in &layout.decode_metadata.fields {
            if !decode_field_ids.insert(field.field_id) {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} decode metadata repeats field {}",
                    field.field_id.get()
                )));
            }
        }
        let mut column_field_ids = BTreeSet::new();
        for column in &footer.columns {
            if !column_field_ids.insert(column.field_id) {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} contains duplicate field id {}",
                    column.field_id.get()
                )));
            }
            let Some(decode_field) =
                Self::columnar_decode_field(location, footer, column.field_id)?
            else {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} decode metadata is missing field {}",
                    column.field_id.get()
                )));
            };
            if decode_field.field_type != column.field_type {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} decode metadata type for field {} does not match the column footer",
                    column.field_id.get()
                )));
            }
            let _ = Self::columnar_substream(
                location,
                footer,
                Some(column.field_id),
                crate::ColumnarSubstreamKind::PresentBitmap,
            )?;
            match column.field_type {
                FieldType::Int64 => {
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::Int64Values,
                    )?;
                }
                FieldType::Float64 => {
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::Float64Bits,
                    )?;
                }
                FieldType::String => {
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::StringOffsets,
                    )?;
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::StringData,
                    )?;
                }
                FieldType::Bytes => {
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::BytesOffsets,
                    )?;
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::BytesData,
                    )?;
                }
                FieldType::Bool => {
                    let _ = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::BoolValues,
                    )?;
                }
            }
        }
        let _ = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::KeyOffsets,
        )?;
        let _ = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::KeyData,
        )?;
        let _ = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::Sequence,
        )?;
        let _ = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::TombstoneBitmap,
        )?;
        let _ = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::RowKind,
        )?;
        if let Some(applied_generation) = footer.applied_generation
            && applied_generation > manifest_generation
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} applied generation {} exceeds manifest generation {}",
                applied_generation.get(),
                manifest_generation.get()
            )));
        }
        Ok(())
    }

    pub(super) async fn load_lazy_columnar_sstable(
        columnar_read_context: &ColumnarReadContext,
        manifest_generation: ManifestId,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let source = meta.storage_source();
        let location = meta.storage_descriptor();
        if let Some(marker) =
            Self::read_quarantine_marker(&columnar_read_context.dependencies, &source).await?
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is quarantined: {}",
                marker.reason
            )));
        }
        let footer = columnar_read_context
            .footer_from_source(
                meta,
                &source,
                location,
                ColumnarReadAccessPattern::Recovery,
                false,
            )
            .await?;
        Self::validate_loaded_columnar_footer(
            location,
            meta,
            footer.footer.as_ref(),
            manifest_generation,
        )?;

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: Vec::new(),
            user_key_bloom_filter: footer.footer.user_key_bloom_filter.clone(),
            columnar: Some(ResidentColumnarSstable { source }),
        })
    }

    pub(super) fn decode_resident_row_sstable(
        location: &str,
        meta: &PersistedManifestSstable,
        bytes: &[u8],
    ) -> Result<ResidentRowSstable, StorageError> {
        let file: PersistedRowSstableFile = serde_json::from_slice(bytes).map_err(|error| {
            StorageError::corruption(format!("decode SSTable {location} failed: {error}"))
        })?;
        if file.body.format_version != ROW_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported SSTable version {}",
                file.body.format_version
            )));
        }

        let encoded_body = serde_json::to_vec(&file.body).map_err(|error| {
            StorageError::corruption(format!("encode SSTable body failed: {error}"))
        })?;
        if checksum32(&encoded_body) != file.checksum || file.checksum != meta.checksum {
            return Err(StorageError::corruption(format!(
                "SSTable checksum mismatch for {location}",
            )));
        }

        let rows_bytes = serde_json::to_vec(&file.body.rows).map_err(|error| {
            StorageError::corruption(format!("encode SSTable rows failed: {error}"))
        })?;
        if checksum32(&rows_bytes) != file.body.data_checksum
            || file.body.data_checksum != meta.data_checksum
        {
            return Err(StorageError::corruption(format!(
                "SSTable data checksum mismatch for {location}",
            )));
        }

        if file.body.table_id != meta.table_id
            || file.body.level != meta.level
            || file.body.local_id != meta.local_id
            || file.body.min_key != meta.min_key
            || file.body.max_key != meta.max_key
            || file.body.min_sequence != meta.min_sequence
            || file.body.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "manifest metadata does not match SSTable {location}",
            )));
        }

        let computed = Self::summarize_sstable_rows(
            meta.table_id,
            meta.level,
            &meta.local_id,
            &file.body.rows,
        )?;
        if computed.min_key != meta.min_key
            || computed.max_key != meta.max_key
            || computed.min_sequence != meta.min_sequence
            || computed.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "SSTable row summary does not match metadata for {location}",
            )));
        }

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: file.body.rows,
            user_key_bloom_filter: file.body.user_key_bloom_filter,
            columnar: None,
        })
    }

    #[allow(dead_code)]
    pub(super) fn decode_resident_columnar_sstable(
        location: &str,
        meta: &PersistedManifestSstable,
        bytes: &[u8],
    ) -> Result<ResidentRowSstable, StorageError> {
        if checksum32(bytes) != meta.checksum {
            return Err(StorageError::corruption(format!(
                "SSTable checksum mismatch for {location}",
            )));
        }

        let (footer, footer_start) = Self::columnar_footer_from_bytes(location, bytes)?;
        Self::validate_loaded_columnar_footer(
            location,
            meta,
            &footer,
            footer.applied_generation.unwrap_or_default(),
        )?;

        let data_region = &bytes[COLUMNAR_SSTABLE_MAGIC.len()..footer_start];
        if checksum32(data_region) != footer.data_checksum
            || footer.data_checksum != meta.data_checksum
        {
            return Err(StorageError::corruption(format!(
                "SSTable data checksum mismatch for {location}",
            )));
        }

        let row_count = usize::try_from(footer.row_count).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} row count exceeds platform limits"
            ))
        })?;
        let key_index_block = Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "key index",
            &footer.key_index,
        )?;
        let sequence_block = Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "sequence column",
            &footer.sequence_column,
        )?;
        let tombstone_block = Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "tombstone bitmap",
            &footer.tombstone_bitmap,
        )?;
        let row_kind_block = Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "row-kind column",
            &footer.row_kind_column,
        )?;
        let key_index: Vec<Key> = Self::decode_columnar_key_index(
            location,
            &footer,
            &footer.key_index,
            key_index_block,
            row_count,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar key index for {location} failed: {error}"
            ))
        })?;
        let sequences: Vec<SequenceNumber> = Self::decode_columnar_sequences(
            location,
            &footer,
            &footer.sequence_column,
            sequence_block,
            row_count,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar sequence column for {location} failed: {error}"
            ))
        })?;
        let tombstones: Vec<bool> = Self::decode_columnar_tombstones(
            location,
            &footer,
            &footer.tombstone_bitmap,
            tombstone_block,
            row_count,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar tombstone bitmap for {location} failed: {error}"
            ))
        })?;
        let row_kinds: Vec<ChangeKind> = Self::decode_columnar_row_kinds(
            location,
            &footer,
            &footer.row_kind_column,
            row_kind_block,
            row_count,
        )
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar row-kind column for {location} failed: {error}"
            ))
        })?;

        if key_index.len() != row_count
            || sequences.len() != row_count
            || tombstones.len() != row_count
            || row_kinds.len() != row_count
        {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} contains inconsistent block lengths",
            )));
        }

        let mut values_by_field = BTreeMap::<FieldId, Vec<FieldValue>>::new();
        let mut seen_field_ids = BTreeSet::new();
        for column in &footer.columns {
            if !seen_field_ids.insert(column.field_id) {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} contains duplicate field id {}",
                    column.field_id.get()
                )));
            }
            let block = Self::columnar_block_slice(
                location,
                bytes,
                footer_start,
                "column block",
                &column.block,
            )?;
            values_by_field.insert(
                column.field_id,
                Self::decode_columnar_field_values(location, &footer, column, row_count, block)?,
            );
        }

        let mut rows = Vec::with_capacity(row_count);
        for row_index in 0..row_count {
            let kind = row_kinds[row_index];
            let tombstone = tombstones[row_index];
            let value = if tombstone {
                if kind != ChangeKind::Delete {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} marks non-delete row {} as a tombstone",
                        row_index
                    )));
                }
                None
            } else {
                if kind == ChangeKind::Delete {
                    return Err(StorageError::corruption(format!(
                        "columnar SSTable {location} stores delete row {} without a tombstone",
                        row_index
                    )));
                }

                let mut record = ColumnarRecord::new();
                for column in &footer.columns {
                    let values = values_by_field.get(&column.field_id).ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {location} omitted field {} during decode",
                            column.field_id.get()
                        ))
                    })?;
                    record.insert(column.field_id, values[row_index].clone());
                }
                Some(Value::Record(record))
            };

            rows.push(SstableRow {
                key: key_index[row_index].clone(),
                sequence: sequences[row_index],
                kind,
                value,
            });
        }

        let computed =
            Self::summarize_sstable_rows(meta.table_id, meta.level, &meta.local_id, &rows)?;
        if computed.min_key != meta.min_key
            || computed.max_key != meta.max_key
            || computed.min_sequence != meta.min_sequence
            || computed.max_sequence != meta.max_sequence
        {
            return Err(StorageError::corruption(format!(
                "SSTable row summary does not match metadata for {location}",
            )));
        }

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows,
            user_key_bloom_filter: footer.user_key_bloom_filter,
            columnar: Some(ResidentColumnarSstable {
                source: meta.storage_source(),
            }),
        })
    }

    #[allow(dead_code)]
    pub(super) fn columnar_footer_from_bytes(
        location: &str,
        bytes: &[u8],
    ) -> Result<(PersistedColumnarSstableFooter, usize), StorageError> {
        let min_len = COLUMNAR_SSTABLE_MAGIC.len() * 2 + std::mem::size_of::<u64>();
        if bytes.len() < min_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is too short",
            )));
        }
        if !bytes.starts_with(COLUMNAR_SSTABLE_MAGIC) {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the header magic",
            )));
        }

        let footer_magic_offset = bytes.len() - COLUMNAR_SSTABLE_MAGIC.len();
        if &bytes[footer_magic_offset..] != COLUMNAR_SSTABLE_MAGIC {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} is missing the trailer magic",
            )));
        }

        let footer_len_offset = footer_magic_offset - std::mem::size_of::<u64>();
        let footer_len = u64::from_le_bytes(
            bytes[footer_len_offset..footer_magic_offset]
                .try_into()
                .map_err(|_| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} footer length trailer is truncated",
                    ))
                })?,
        ) as usize;
        let footer_start = footer_len_offset.checked_sub(footer_len).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} footer length points before the file start",
            ))
        })?;
        let footer_bytes = &bytes[footer_start..footer_len_offset];
        let footer = serde_json::from_slice(footer_bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar SSTable footer for {location} failed: {error}"
            ))
        })?;

        Ok((footer, footer_start))
    }

    #[allow(dead_code)]
    pub(super) fn columnar_block_slice<'a>(
        location: &str,
        bytes: &'a [u8],
        footer_start: usize,
        block_name: &str,
        block: &ColumnarBlockLocation,
    ) -> Result<&'a [u8], StorageError> {
        let range = Self::columnar_block_range(location, footer_start, block_name, block)?;
        let start = usize::try_from(range.start).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} offset exceeds platform limits",
            ))
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} length exceeds platform limits",
            ))
        })?;
        Ok(&bytes[start..end])
    }

    pub(super) fn columnar_block_range(
        location: &str,
        footer_start: usize,
        block_name: &str,
        block: &ColumnarBlockLocation,
    ) -> Result<std::ops::Range<u64>, StorageError> {
        let offset = usize::try_from(block.offset).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} offset exceeds platform limits",
            ))
        })?;
        let length = usize::try_from(block.length).map_err(|_| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} length exceeds platform limits",
            ))
        })?;
        let end = offset.checked_add(length).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} range overflows",
            ))
        })?;
        if offset < COLUMNAR_SSTABLE_MAGIC.len() || end > footer_start {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} range is outside the data region",
            )));
        }

        Ok(block.offset..block.offset.saturating_add(block.length))
    }

    fn columnar_decode_field<'a>(
        location: &str,
        footer: &'a PersistedColumnarSstableFooter,
        field_id: FieldId,
    ) -> Result<Option<&'a crate::hybrid::ColumnarDecodeField>, StorageError> {
        let layout = &footer.layout;
        let mut matches = layout
            .decode_metadata
            .fields
            .iter()
            .filter(|field| field.field_id == field_id);
        let first = matches.next();
        if matches.next().is_some() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} decode metadata repeats field {}",
                field_id.get()
            )));
        }
        Ok(first)
    }

    fn columnar_substream<'a>(
        location: &str,
        footer: &'a PersistedColumnarSstableFooter,
        field_id: Option<FieldId>,
        kind: crate::ColumnarSubstreamKind,
    ) -> Result<&'a crate::ColumnarSubstreamRef, StorageError> {
        let layout = &footer.layout;
        let mut matches = layout
            .substreams
            .iter()
            .filter(|substream| substream.field_id == field_id && substream.kind == kind);
        let first = matches.next().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} is missing {kind:?} substream",
            ))
        })?;
        if matches.next().is_some() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} repeats {kind:?} substreams",
            )));
        }
        Ok(first)
    }

    fn columnar_substream_slice<'a>(
        location: &str,
        block_name: &str,
        block: &ColumnarBlockLocation,
        block_bytes: &'a [u8],
        substream: &crate::ColumnarSubstreamRef,
    ) -> Result<&'a [u8], StorageError> {
        let block_end = block.offset.checked_add(block.length).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {location} {block_name} range overflows",
            ))
        })?;
        if substream.range.start < block.offset || substream.range.end > block_end {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} substream {:?} is outside {block_name}",
                substream.kind
            )));
        }
        let start =
            usize::try_from(substream.range.start.saturating_sub(block.offset)).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {block_name} start exceeds platform limits",
                ))
            })?;
        let end =
            usize::try_from(substream.range.end.saturating_sub(block.offset)).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {block_name} end exceeds platform limits",
                ))
            })?;
        if end > block_bytes.len() || start > end {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} substream {:?} slice is truncated",
                substream.kind
            )));
        }
        Ok(&block_bytes[start..end])
    }

    fn encode_columnar_bitmap(bits: &[bool]) -> Vec<u8> {
        let mut bytes = vec![0; bits.len().div_ceil(8)];
        for (index, bit) in bits.iter().copied().enumerate() {
            if bit {
                bytes[index / 8] |= 1 << (index % 8);
            }
        }
        bytes
    }

    fn decode_columnar_bitmap(
        location: &str,
        label: &str,
        count: usize,
        bytes: &[u8],
    ) -> Result<Vec<bool>, StorageError> {
        let expected_len = count.div_ceil(8);
        if bytes.len() != expected_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} length mismatch: expected {expected_len} bytes, found {}",
                bytes.len()
            )));
        }

        let trailing_bits = count % 8;
        if !count.is_multiple_of(8) {
            let used_mask = (1_u8 << trailing_bits) - 1;
            if bytes.last().copied().unwrap_or_default() & !used_mask != 0 {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} {label} sets unused bitmap bits",
                )));
            }
        }

        let mut decoded = Vec::with_capacity(count);
        for index in 0..count {
            let byte = bytes[index / 8];
            decoded.push(byte & (1 << (index % 8)) != 0);
        }
        Ok(decoded)
    }

    fn encode_columnar_u64_stream(values: impl IntoIterator<Item = u64>) -> Vec<u8> {
        let values = values.into_iter().collect::<Vec<_>>();
        let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<u64>());
        for value in values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    fn decode_columnar_u64_stream(
        location: &str,
        label: &str,
        bytes: &[u8],
    ) -> Result<Vec<u64>, StorageError> {
        if !bytes.len().is_multiple_of(std::mem::size_of::<u64>()) {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} length is not a multiple of 8 bytes",
            )));
        }
        Ok(bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|chunk| {
                let mut value = [0_u8; 8];
                value.copy_from_slice(chunk);
                u64::from_le_bytes(value)
            })
            .collect())
    }

    fn encode_columnar_i64_stream(values: impl IntoIterator<Item = i64>) -> Vec<u8> {
        let values = values.into_iter().collect::<Vec<_>>();
        let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<i64>());
        for value in values {
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        bytes
    }

    fn decode_columnar_i64_stream(
        location: &str,
        label: &str,
        bytes: &[u8],
    ) -> Result<Vec<i64>, StorageError> {
        if !bytes.len().is_multiple_of(std::mem::size_of::<i64>()) {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} length is not a multiple of 8 bytes",
            )));
        }
        Ok(bytes
            .chunks_exact(std::mem::size_of::<i64>())
            .map(|chunk| {
                let mut value = [0_u8; 8];
                value.copy_from_slice(chunk);
                i64::from_le_bytes(value)
            })
            .collect())
    }

    fn decode_columnar_u8_stream(
        location: &str,
        label: &str,
        bytes: &[u8],
        expected_len: usize,
    ) -> Result<Vec<u8>, StorageError> {
        if bytes.len() != expected_len {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} length mismatch: expected {expected_len} bytes, found {}",
                bytes.len()
            )));
        }
        Ok(bytes.to_vec())
    }

    fn encode_columnar_offsets_and_data(values: &[Vec<u8>]) -> (Vec<u8>, Vec<u8>) {
        let mut offsets = Vec::with_capacity(values.len() + 1);
        let mut data = Vec::new();
        offsets.push(0_u64);
        for value in values {
            data.extend_from_slice(value);
            offsets.push(data.len() as u64);
        }
        (Self::encode_columnar_u64_stream(offsets), data)
    }

    fn decode_columnar_offsets(
        location: &str,
        label: &str,
        bytes: &[u8],
    ) -> Result<Vec<u64>, StorageError> {
        let offsets = Self::decode_columnar_u64_stream(location, label, bytes)?;
        if offsets.is_empty() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} is empty",
            )));
        }
        if offsets[0] != 0 {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} does not start at offset 0",
            )));
        }
        if offsets.windows(2).any(|window| window[0] > window[1]) {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} is not monotonically increasing",
            )));
        }
        Ok(offsets)
    }

    fn decode_columnar_variable_values(
        location: &str,
        label: &str,
        offsets: &[u64],
        data: &[u8],
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        if offsets.last().copied().unwrap_or_default() != data.len() as u64 {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} final offset does not match the data length",
            )));
        }

        let mut values = Vec::with_capacity(offsets.len().saturating_sub(1));
        for window in offsets.windows(2) {
            let start = usize::try_from(window[0]).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {label} offset exceeds platform limits",
                ))
            })?;
            let end = usize::try_from(window[1]).map_err(|_| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} {label} offset exceeds platform limits",
                ))
            })?;
            values.push(data[start..end].to_vec());
        }
        Ok(values)
    }

    fn encode_columnar_change_kinds(values: impl IntoIterator<Item = ChangeKind>) -> Vec<u8> {
        values
            .into_iter()
            .map(|kind| match kind {
                ChangeKind::Put => 0,
                ChangeKind::Delete => 1,
                ChangeKind::Merge => 2,
            })
            .collect()
    }

    fn decode_columnar_change_kinds(
        location: &str,
        bytes: &[u8],
        row_count: usize,
    ) -> Result<Vec<ChangeKind>, StorageError> {
        Self::decode_columnar_u8_stream(location, "row-kind column", bytes, row_count)?
            .into_iter()
            .map(|value| match value {
                0 => Ok(ChangeKind::Put),
                1 => Ok(ChangeKind::Delete),
                2 => Ok(ChangeKind::Merge),
                other => Err(StorageError::corruption(format!(
                    "columnar SSTable {location} contains unknown row kind tag {other}",
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn encode_columnar_payload(
        compression: crate::ColumnarCompression,
        raw_bytes: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        match compression {
            crate::ColumnarCompression::None => Ok(raw_bytes.to_vec()),
            crate::ColumnarCompression::Lz4 => Ok(lz4_flex::compress_prepend_size(raw_bytes)),
            crate::ColumnarCompression::Zstd => {
                zstd::stream::encode_all(std::io::Cursor::new(raw_bytes), 0).map_err(|error| {
                    StorageError::unsupported(format!(
                        "encode zstd columnar substream failed: {error}"
                    ))
                })
            }
        }
    }

    fn decode_columnar_payload(
        location: &str,
        label: &str,
        substream: &crate::ColumnarSubstreamRef,
        bytes: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        let decoded = match substream.compression {
            crate::ColumnarCompression::None => Ok(bytes.to_vec()),
            crate::ColumnarCompression::Lz4 => {
                lz4_flex::decompress_size_prepended(bytes).map_err(|error| {
                    StorageError::corruption(format!(
                        "decode columnar SSTable {location} {label} lz4 payload failed: {error}"
                    ))
                })
            }
            crate::ColumnarCompression::Zstd => {
                zstd::stream::decode_all(std::io::Cursor::new(bytes)).map_err(|error| {
                    StorageError::corruption(format!(
                        "decode columnar SSTable {location} {label} zstd payload failed: {error}"
                    ))
                })
            }
        }?;
        if checksum32(bytes) != substream.checksum {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} {label} checksum mismatch",
            )));
        }
        Ok(decoded)
    }

    fn columnar_codec_for_substream(
        kind: crate::ColumnarSubstreamKind,
    ) -> crate::ColumnarCompression {
        match kind {
            crate::ColumnarSubstreamKind::KeyData
            | crate::ColumnarSubstreamKind::StringData
            | crate::ColumnarSubstreamKind::BytesData => crate::ColumnarCompression::Lz4,
            crate::ColumnarSubstreamKind::Sequence
            | crate::ColumnarSubstreamKind::Int64Values
            | crate::ColumnarSubstreamKind::Float64Bits => crate::ColumnarCompression::Zstd,
            crate::ColumnarSubstreamKind::KeyOffsets
            | crate::ColumnarSubstreamKind::TombstoneBitmap
            | crate::ColumnarSubstreamKind::RowKind
            | crate::ColumnarSubstreamKind::DefinitionLevels
            | crate::ColumnarSubstreamKind::PresentBitmap
            | crate::ColumnarSubstreamKind::StringOffsets
            | crate::ColumnarSubstreamKind::BytesOffsets
            | crate::ColumnarSubstreamKind::BoolValues => crate::ColumnarCompression::None,
        }
    }

    fn columnar_decode_metadata(schema: &SchemaDefinition) -> crate::ColumnarDecodeMetadata {
        crate::ColumnarDecodeMetadata {
            schema_version: schema.version,
            fields: schema
                .fields
                .iter()
                .map(|field| crate::ColumnarDecodeField {
                    field_id: field.id,
                    field_type: field.field_type,
                    nullable: field.nullable,
                    has_default: field.default.is_some(),
                })
                .collect(),
        }
    }

    fn append_columnar_substream(
        block_start: u64,
        block_bytes: &mut Vec<u8>,
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
        descriptor: ColumnarSubstreamDescriptor,
        raw_bytes: &[u8],
    ) -> Result<(), StorageError> {
        let (field_id, field_type, kind) = descriptor;
        let compression = Self::columnar_codec_for_substream(kind);
        let encoded = Self::encode_columnar_payload(compression, raw_bytes)?;
        let start = block_start + block_bytes.len() as u64;
        block_bytes.extend_from_slice(&encoded);
        let end = block_start + block_bytes.len() as u64;
        substreams.push(crate::ColumnarSubstreamRef {
            ordinal: *next_ordinal,
            field_id,
            field_type,
            kind,
            encoding: crate::ColumnarEncoding::Plain,
            compression,
            range: crate::ByteRange::new(start, end),
            checksum: checksum32(&encoded),
        });
        *next_ordinal += 1;
        Ok(())
    }

    fn decode_columnar_key_index(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        block: &ColumnarBlockLocation,
        block_bytes: &[u8],
        row_count: usize,
    ) -> Result<Vec<Key>, StorageError> {
        let offsets = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::KeyOffsets,
        )?;
        let data = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::KeyData,
        )?;
        let offsets_label = "key index offsets";
        let data_label = "key index data";
        let offsets = Self::decode_columnar_offsets(
            location,
            offsets_label,
            &Self::decode_columnar_payload(
                location,
                offsets_label,
                offsets,
                Self::columnar_substream_slice(location, "key index", block, block_bytes, offsets)?,
            )?,
        )?;
        if offsets.len() != row_count + 1 {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} key index offset count mismatch",
            )));
        }
        Self::decode_columnar_variable_values(
            location,
            "key index",
            &offsets,
            &Self::decode_columnar_payload(
                location,
                data_label,
                data,
                Self::columnar_substream_slice(location, "key index", block, block_bytes, data)?,
            )?,
        )
    }

    fn decode_columnar_sequences(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        block: &ColumnarBlockLocation,
        block_bytes: &[u8],
        row_count: usize,
    ) -> Result<Vec<SequenceNumber>, StorageError> {
        let substream = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::Sequence,
        )?;
        let values = Self::decode_columnar_u64_stream(
            location,
            "sequence column",
            &Self::decode_columnar_payload(
                location,
                "sequence column",
                substream,
                Self::columnar_substream_slice(
                    location,
                    "sequence column",
                    block,
                    block_bytes,
                    substream,
                )?,
            )?,
        )?;
        if values.len() != row_count {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} sequence column length mismatch",
            )));
        }
        Ok(values.into_iter().map(SequenceNumber::new).collect())
    }

    fn decode_columnar_tombstones(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        block: &ColumnarBlockLocation,
        block_bytes: &[u8],
        row_count: usize,
    ) -> Result<Vec<bool>, StorageError> {
        let substream = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::TombstoneBitmap,
        )?;
        Self::decode_columnar_bitmap(
            location,
            "tombstone bitmap",
            row_count,
            &Self::decode_columnar_payload(
                location,
                "tombstone bitmap",
                substream,
                Self::columnar_substream_slice(
                    location,
                    "tombstone bitmap",
                    block,
                    block_bytes,
                    substream,
                )?,
            )?,
        )
    }

    fn decode_columnar_row_kinds(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        block: &ColumnarBlockLocation,
        block_bytes: &[u8],
        row_count: usize,
    ) -> Result<Vec<ChangeKind>, StorageError> {
        let substream = Self::columnar_substream(
            location,
            footer,
            None,
            crate::ColumnarSubstreamKind::RowKind,
        )?;
        Self::decode_columnar_change_kinds(
            location,
            &Self::decode_columnar_payload(
                location,
                "row-kind column",
                substream,
                Self::columnar_substream_slice(
                    location,
                    "row-kind column",
                    block,
                    block_bytes,
                    substream,
                )?,
            )?,
            row_count,
        )
    }

    fn decode_columnar_substream_field_values(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        block_bytes: &[u8],
    ) -> Result<Vec<FieldValue>, StorageError> {
        let decode_field = Self::columnar_decode_field(location, footer, column.field_id)?
            .ok_or_else(|| {
                StorageError::corruption(format!(
                    "columnar SSTable {location} decode metadata is missing field {}",
                    column.field_id.get()
                ))
            })?;
        if decode_field.field_type != column.field_type {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} decode metadata type for field {} does not match the column footer",
                column.field_id.get()
            )));
        }

        let present = Self::decode_columnar_bitmap(
            location,
            &format!("field {} present bitmap", column.field_id.get()),
            row_count,
            &{
                let substream = Self::columnar_substream(
                    location,
                    footer,
                    Some(column.field_id),
                    crate::ColumnarSubstreamKind::PresentBitmap,
                )?;
                Self::decode_columnar_payload(
                    location,
                    &format!("field {} present bitmap", column.field_id.get()),
                    substream,
                    Self::columnar_substream_slice(
                        location,
                        "column block",
                        &column.block,
                        block_bytes,
                        substream,
                    )?,
                )?
            },
        )?;
        let present_count = present.iter().filter(|present| **present).count();

        let values = match column.field_type {
            FieldType::Int64 => Self::decode_columnar_i64_stream(
                location,
                &format!("field {} int64 values", column.field_id.get()),
                &{
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::Int64Values,
                    )?;
                    Self::decode_columnar_payload(
                        location,
                        &format!("field {} int64 values", column.field_id.get()),
                        substream,
                        Self::columnar_substream_slice(
                            location,
                            "column block",
                            &column.block,
                            block_bytes,
                            substream,
                        )?,
                    )?
                },
            )?
            .into_iter()
            .map(FieldValue::Int64)
            .collect(),
            FieldType::Float64 => Self::decode_columnar_u64_stream(
                location,
                &format!("field {} float64 bits", column.field_id.get()),
                &{
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::Float64Bits,
                    )?;
                    Self::decode_columnar_payload(
                        location,
                        &format!("field {} float64 bits", column.field_id.get()),
                        substream,
                        Self::columnar_substream_slice(
                            location,
                            "column block",
                            &column.block,
                            block_bytes,
                            substream,
                        )?,
                    )?
                },
            )?
            .into_iter()
            .map(|bits| FieldValue::Float64(f64::from_bits(bits)))
            .collect(),
            FieldType::String => {
                let offsets = {
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::StringOffsets,
                    )?;
                    Self::decode_columnar_offsets(
                        location,
                        &format!("field {} string offsets", column.field_id.get()),
                        &Self::decode_columnar_payload(
                            location,
                            &format!("field {} string offsets", column.field_id.get()),
                            substream,
                            Self::columnar_substream_slice(
                                location,
                                "column block",
                                &column.block,
                                block_bytes,
                                substream,
                            )?,
                        )?,
                    )?
                };
                let data = {
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::StringData,
                    )?;
                    Self::decode_columnar_payload(
                        location,
                        &format!("field {} string data", column.field_id.get()),
                        substream,
                        Self::columnar_substream_slice(
                            location,
                            "column block",
                            &column.block,
                            block_bytes,
                            substream,
                        )?,
                    )?
                };
                Self::decode_columnar_variable_values(
                    location,
                    &format!("field {} string values", column.field_id.get()),
                    &offsets,
                    &data,
                )?
                .into_iter()
                .map(|bytes| {
                    String::from_utf8(bytes).map(FieldValue::String).map_err(|error| {
                        StorageError::corruption(format!(
                            "columnar SSTable {location} field {} string payload is not valid UTF-8: {error}",
                            column.field_id.get()
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
            }
            FieldType::Bytes => {
                let offsets = {
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::BytesOffsets,
                    )?;
                    Self::decode_columnar_offsets(
                        location,
                        &format!("field {} bytes offsets", column.field_id.get()),
                        &Self::decode_columnar_payload(
                            location,
                            &format!("field {} bytes offsets", column.field_id.get()),
                            substream,
                            Self::columnar_substream_slice(
                                location,
                                "column block",
                                &column.block,
                                block_bytes,
                                substream,
                            )?,
                        )?,
                    )?
                };
                let data = {
                    let substream = Self::columnar_substream(
                        location,
                        footer,
                        Some(column.field_id),
                        crate::ColumnarSubstreamKind::BytesData,
                    )?;
                    Self::decode_columnar_payload(
                        location,
                        &format!("field {} bytes data", column.field_id.get()),
                        substream,
                        Self::columnar_substream_slice(
                            location,
                            "column block",
                            &column.block,
                            block_bytes,
                            substream,
                        )?,
                    )?
                };
                Self::decode_columnar_variable_values(
                    location,
                    &format!("field {} bytes values", column.field_id.get()),
                    &offsets,
                    &data,
                )?
                .into_iter()
                .map(FieldValue::Bytes)
                .collect()
            }
            FieldType::Bool => {
                let substream = Self::columnar_substream(
                    location,
                    footer,
                    Some(column.field_id),
                    crate::ColumnarSubstreamKind::BoolValues,
                )?;
                Self::decode_columnar_bitmap(
                    location,
                    &format!("field {} bool values", column.field_id.get()),
                    present_count,
                    &Self::decode_columnar_payload(
                        location,
                        &format!("field {} bool values", column.field_id.get()),
                        substream,
                        Self::columnar_substream_slice(
                            location,
                            "column block",
                            &column.block,
                            block_bytes,
                            substream,
                        )?,
                    )?,
                )?
                .into_iter()
                .map(FieldValue::Bool)
                .collect()
            }
        };

        Self::decode_nullable_column_values(location, column, row_count, &present, values)
    }

    pub(super) fn normalized_columnar_row_kind(
        location: &str,
        row_index: usize,
        tombstone: bool,
        kind: ChangeKind,
    ) -> Result<ChangeKind, StorageError> {
        if tombstone {
            if kind != ChangeKind::Delete {
                return Err(StorageError::corruption(format!(
                    "columnar SSTable {location} marks non-delete row {} as a tombstone",
                    row_index
                )));
            }
            return Ok(ChangeKind::Delete);
        }
        if kind == ChangeKind::Delete {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} stores delete row {} without a tombstone",
                row_index
            )));
        }

        Ok(kind)
    }

    pub(super) fn decode_columnar_field_values(
        location: &str,
        footer: &PersistedColumnarSstableFooter,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        block: &[u8],
    ) -> Result<Vec<FieldValue>, StorageError> {
        Self::decode_columnar_substream_field_values(location, footer, column, row_count, block)
            .map_err(|error| {
                StorageError::corruption(format!(
                    "decode columnar field {} for {location} failed: {error}",
                    column.field_id.get()
                ))
            })
    }

    pub(super) fn decode_nullable_column_values(
        location: &str,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        present_bitmap: &[bool],
        values: Vec<FieldValue>,
    ) -> Result<Vec<FieldValue>, StorageError> {
        if present_bitmap.len() != row_count {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} bitmap length mismatch",
                column.field_id.get()
            )));
        }
        if present_bitmap.iter().filter(|present| **present).count() != values.len() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} present bitmap does not match its value count",
                column.field_id.get()
            )));
        }

        let mut decoded = Vec::with_capacity(row_count);
        let mut values = values.into_iter();
        for present in present_bitmap {
            decoded.push(if *present {
                values.next().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar SSTable {location} field {} ran out of values during decode",
                        column.field_id.get()
                    ))
                })?
            } else {
                FieldValue::Null
            });
        }
        if values.next().is_some() {
            return Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} contains extra decoded values",
                column.field_id.get()
            )));
        }

        Ok(decoded)
    }

    pub(super) fn summarize_sstable_rows(
        table_id: TableId,
        level: u32,
        local_id: &str,
        rows: &[SstableRow],
    ) -> Result<PersistedManifestSstable, StorageError> {
        let min_key = rows
            .iter()
            .map(|row| row.key.clone())
            .min()
            .ok_or_else(|| StorageError::unsupported("cannot build an empty SSTable"))?;
        let max_key = rows
            .iter()
            .map(|row| row.key.clone())
            .max()
            .expect("max key");
        let min_sequence = rows
            .iter()
            .map(|row| row.sequence)
            .min()
            .expect("min sequence");
        let max_sequence = rows
            .iter()
            .map(|row| row.sequence)
            .max()
            .expect("max sequence");

        Ok(PersistedManifestSstable {
            table_id,
            level,
            local_id: local_id.to_string(),
            file_path: String::new(),
            remote_key: None,
            length: 0,
            checksum: 0,
            data_checksum: 0,
            min_key,
            max_key,
            min_sequence,
            max_sequence,
            schema_version: None,
        })
    }

    pub(super) async fn flush_immutable(
        &self,
        local_root: &str,
        immutable: &ImmutableMemtable,
        applied_generation: ManifestId,
    ) -> Result<Vec<ResidentRowSstable>, FlushError> {
        let mut outputs = Vec::new();
        let tables = self.tables_read().clone();

        for (&table_id, table_memtable) in &immutable.memtable.tables {
            if table_memtable.is_empty() {
                continue;
            }

            let stored = tables
                .values()
                .find(|table| table.id == table_id)
                .cloned()
                .ok_or_else(|| {
                    FlushError::Storage(StorageError::corruption(format!(
                        "flush references unknown table id {}",
                        table_id.get()
                    )))
                })?;

            let rows = table_memtable
                .entries
                .values()
                .map(|entry| SstableRow {
                    key: entry.user_key.clone(),
                    sequence: entry.sequence,
                    kind: entry.kind,
                    value: entry.value.clone(),
                })
                .collect::<Vec<_>>();
            let local_id = format!(
                "SST-{:06}",
                self.inner.next_sstable_id.fetch_add(1, Ordering::SeqCst)
            );
            let path = Self::local_sstable_path(local_root, table_id, &local_id);
            let output = match stored.config.format {
                TableFormat::Row => {
                    self.write_row_sstable(
                        &path,
                        table_id,
                        0,
                        local_id,
                        rows,
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .await
                }
                TableFormat::Columnar => {
                    self.write_columnar_table_output(
                        &path,
                        0,
                        local_id,
                        &stored,
                        rows,
                        Some(applied_generation),
                        &[],
                    )
                    .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    pub(super) async fn flush_immutable_remote(
        &self,
        config: &S3PrimaryStorageConfig,
        immutable: &ImmutableMemtable,
        applied_generation: ManifestId,
    ) -> Result<Vec<ResidentRowSstable>, FlushError> {
        let mut outputs = Vec::new();
        let tables = self.tables_read().clone();

        for (&table_id, table_memtable) in &immutable.memtable.tables {
            if table_memtable.is_empty() {
                continue;
            }

            let stored = tables
                .values()
                .find(|table| table.id == table_id)
                .cloned()
                .ok_or_else(|| {
                    FlushError::Storage(StorageError::corruption(format!(
                        "flush references unknown table id {}",
                        table_id.get()
                    )))
                })?;

            let rows = table_memtable
                .entries
                .values()
                .map(|entry| SstableRow {
                    key: entry.user_key.clone(),
                    sequence: entry.sequence,
                    kind: entry.kind,
                    value: entry.value.clone(),
                })
                .collect::<Vec<_>>();
            let local_id = format!(
                "SST-{:06}",
                self.inner.next_sstable_id.fetch_add(1, Ordering::SeqCst)
            );
            let object_key = Self::remote_sstable_key(config, table_id, &local_id);
            let output = match stored.config.format {
                TableFormat::Row => {
                    self.write_row_sstable_remote(
                        &object_key,
                        table_id,
                        0,
                        local_id,
                        rows,
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .await
                }
                TableFormat::Columnar => {
                    self.write_columnar_table_output_remote(
                        &object_key,
                        0,
                        local_id,
                        &stored,
                        rows,
                        Some(applied_generation),
                        &[],
                    )
                    .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    pub(super) fn encode_row_sstable(
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<(ResidentRowSstable, Vec<u8>), StorageError> {
        let mut meta = Self::summarize_sstable_rows(table_id, level, &local_id, &rows)?;
        let user_key_bloom_filter = UserKeyBloomFilter::build(&rows, bloom_filter_bits_per_key);
        let rows_bytes = serde_json::to_vec(&rows).map_err(|error| {
            StorageError::corruption(format!("encode SSTable rows failed: {error}"))
        })?;
        let data_checksum = checksum32(&rows_bytes);

        let body = PersistedRowSstableBody {
            format_version: ROW_SSTABLE_FORMAT_VERSION,
            table_id,
            level,
            local_id: local_id.clone(),
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            min_sequence: meta.min_sequence,
            max_sequence: meta.max_sequence,
            rows: rows.clone(),
            data_checksum,
            user_key_bloom_filter: user_key_bloom_filter.clone(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode SSTable body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        let file = PersistedRowSstableFile { body, checksum };
        let bytes = serde_json::to_vec_pretty(&file).map_err(|error| {
            StorageError::corruption(format!("encode SSTable file failed: {error}"))
        })?;

        meta.length = bytes.len() as u64;
        meta.checksum = checksum;
        meta.data_checksum = data_checksum;

        Ok((
            ResidentRowSstable {
                meta,
                rows,
                user_key_bloom_filter,
                columnar: None,
            },
            bytes,
        ))
    }

    pub(super) fn encode_json_block<T: Serialize>(
        label: &str,
        value: &T,
    ) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(value)
            .map_err(|error| StorageError::corruption(format!("encode {label} failed: {error}")))
    }

    pub(super) fn append_columnar_block(
        bytes: &mut Vec<u8>,
        block_bytes: &[u8],
    ) -> ColumnarBlockLocation {
        let location = ColumnarBlockLocation {
            offset: bytes.len() as u64,
            length: block_bytes.len() as u64,
        };
        bytes.extend_from_slice(block_bytes);
        location
    }

    pub(super) fn columnar_row_is_tombstone(row: &SstableRow) -> Result<bool, StorageError> {
        match row.kind {
            ChangeKind::Delete => {
                if row.value.is_some() {
                    return Err(StorageError::corruption(
                        "columnar delete row unexpectedly contains a value",
                    ));
                }
                Ok(true)
            }
            ChangeKind::Put | ChangeKind::Merge => {
                if row.value.is_none() {
                    return Err(StorageError::corruption(
                        "columnar non-delete row is missing a record value",
                    ));
                }
                Ok(false)
            }
        }
    }

    pub(super) fn columnar_field_value_for_row(
        field: &FieldDefinition,
        row: &SstableRow,
    ) -> Result<Option<FieldValue>, StorageError> {
        if Self::columnar_row_is_tombstone(row)? {
            return Ok(None);
        }

        let value = row.value.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar row for field {} is missing a value",
                field.name
            ))
        })?;
        let Value::Record(record) = value else {
            return Err(StorageError::corruption(format!(
                "columnar row for field {} is stored as bytes",
                field.name
            )));
        };
        let field_value = record.get(&field.id).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar row is missing field {} ({})",
                field.id.get(),
                field.name
            ))
        })?;
        match row.kind {
            ChangeKind::Put => {
                validate_field_value_against_definition(field, field_value, "columnar row value")?;
            }
            ChangeKind::Merge => {
                validate_merge_operand_value_against_definition(
                    field,
                    field_value,
                    "columnar merge operand value",
                )?;
            }
            ChangeKind::Delete => {
                return Err(StorageError::corruption(
                    "columnar tombstone row unexpectedly reached field extraction",
                ));
            }
        }

        if matches!(field_value, FieldValue::Null) {
            Ok(None)
        } else {
            Ok(Some(field_value.clone()))
        }
    }

    fn encode_columnar_key_block(
        block_start: u64,
        rows: &[SstableRow],
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
    ) -> Result<Vec<u8>, StorageError> {
        let mut block_bytes = Vec::new();
        let key_bytes = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
        let (offsets, data) = Self::encode_columnar_offsets_and_data(&key_bytes);
        Self::append_columnar_substream(
            block_start,
            &mut block_bytes,
            substreams,
            next_ordinal,
            (None, None, crate::ColumnarSubstreamKind::KeyOffsets),
            &offsets,
        )?;
        Self::append_columnar_substream(
            block_start,
            &mut block_bytes,
            substreams,
            next_ordinal,
            (None, None, crate::ColumnarSubstreamKind::KeyData),
            &data,
        )?;
        Ok(block_bytes)
    }

    fn encode_columnar_sequence_block(
        block_start: u64,
        rows: &[SstableRow],
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
    ) -> Result<Vec<u8>, StorageError> {
        let mut block_bytes = Vec::new();
        let raw = Self::encode_columnar_u64_stream(rows.iter().map(|row| row.sequence.get()));
        Self::append_columnar_substream(
            block_start,
            &mut block_bytes,
            substreams,
            next_ordinal,
            (None, None, crate::ColumnarSubstreamKind::Sequence),
            &raw,
        )?;
        Ok(block_bytes)
    }

    fn encode_columnar_tombstone_block(
        block_start: u64,
        rows: &[SstableRow],
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
    ) -> Result<(Vec<u8>, Vec<bool>), StorageError> {
        let mut tombstones = Vec::with_capacity(rows.len());
        for row in rows {
            tombstones.push(Self::columnar_row_is_tombstone(row)?);
        }
        let mut block_bytes = Vec::new();
        let raw = Self::encode_columnar_bitmap(&tombstones);
        Self::append_columnar_substream(
            block_start,
            &mut block_bytes,
            substreams,
            next_ordinal,
            (None, None, crate::ColumnarSubstreamKind::TombstoneBitmap),
            &raw,
        )?;
        Ok((block_bytes, tombstones))
    }

    fn encode_columnar_row_kind_block(
        block_start: u64,
        rows: &[SstableRow],
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
    ) -> Result<Vec<u8>, StorageError> {
        let mut block_bytes = Vec::new();
        let raw = Self::encode_columnar_change_kinds(rows.iter().map(|row| row.kind));
        Self::append_columnar_substream(
            block_start,
            &mut block_bytes,
            substreams,
            next_ordinal,
            (None, None, crate::ColumnarSubstreamKind::RowKind),
            &raw,
        )?;
        Ok(block_bytes)
    }

    fn encode_columnar_field_block(
        field: &FieldDefinition,
        rows: &[SstableRow],
        block_start: u64,
        substreams: &mut Vec<crate::ColumnarSubstreamRef>,
        next_ordinal: &mut u32,
    ) -> Result<Vec<u8>, StorageError> {
        let mut block_bytes = Vec::new();
        match field.field_type {
            FieldType::Int64 => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Int64(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected int64",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                let present = Self::encode_columnar_bitmap(&present_bitmap);
                let values = Self::encode_columnar_i64_stream(values);
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::PresentBitmap,
                    ),
                    &present,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::Int64Values,
                    ),
                    &values,
                )?;
            }
            FieldType::Float64 => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values_bits = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Float64(value)) => {
                            present_bitmap.push(true);
                            values_bits.push(value.to_bits());
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected float64",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                let present = Self::encode_columnar_bitmap(&present_bitmap);
                let values = Self::encode_columnar_u64_stream(values_bits);
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::PresentBitmap,
                    ),
                    &present,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::Float64Bits,
                    ),
                    &values,
                )?;
            }
            FieldType::String => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::String(value)) => {
                            present_bitmap.push(true);
                            values.push(value.into_bytes());
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected string",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                let present = Self::encode_columnar_bitmap(&present_bitmap);
                let (offsets, data) = Self::encode_columnar_offsets_and_data(&values);
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::PresentBitmap,
                    ),
                    &present,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::StringOffsets,
                    ),
                    &offsets,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::StringData,
                    ),
                    &data,
                )?;
            }
            FieldType::Bytes => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Bytes(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected bytes",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                let present = Self::encode_columnar_bitmap(&present_bitmap);
                let (offsets, data) = Self::encode_columnar_offsets_and_data(&values);
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::PresentBitmap,
                    ),
                    &present,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::BytesOffsets,
                    ),
                    &offsets,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::BytesData,
                    ),
                    &data,
                )?;
            }
            FieldType::Bool => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::Bool(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
                        }
                        Some(other) => {
                            return Err(StorageError::corruption(format!(
                                "columnar row value for field {} had type {}, expected bool",
                                field.name,
                                field_value_type_name(&other)
                            )));
                        }
                        None => present_bitmap.push(false),
                    }
                }
                let present = Self::encode_columnar_bitmap(&present_bitmap);
                let values = Self::encode_columnar_bitmap(&values);
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::PresentBitmap,
                    ),
                    &present,
                )?;
                Self::append_columnar_substream(
                    block_start,
                    &mut block_bytes,
                    substreams,
                    next_ordinal,
                    (
                        Some(field.id),
                        Some(field.field_type),
                        crate::ColumnarSubstreamKind::BoolValues,
                    ),
                    &values,
                )?;
            }
        }
        Ok(block_bytes)
    }

    fn encode_skip_index_body(
        body: &PersistedSkipIndexSidecarBody,
    ) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(body).map_err(|error| {
            StorageError::corruption(format!("encode skip-index sidecar body failed: {error}"))
        })
    }

    fn encode_projection_sidecar_body(
        body: &PersistedProjectionSidecarBody,
    ) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(body).map_err(|error| {
            StorageError::corruption(format!("encode projection sidecar body failed: {error}"))
        })
    }

    fn placeholder_compact_digest(format_tag: crate::ColumnarV2FormatTag) -> CompactPartDigest {
        CompactPartDigest {
            format_tag,
            algorithm: PartDigestAlgorithm::Crc32,
            logical_bytes: 0,
            digest_bytes: vec![0; std::mem::size_of::<u32>()],
        }
    }

    pub(super) fn skip_index_sidecar_digest_input(
        body: &PersistedSkipIndexSidecarBody,
    ) -> Result<Vec<u8>, StorageError> {
        let mut digest_body = body.clone();
        digest_body.digest =
            Self::placeholder_compact_digest(crate::ColumnarV2FormatTag::skip_index_sidecar());
        Self::encode_skip_index_body(&digest_body)
    }

    pub(super) fn projection_sidecar_digest_input(
        body: &PersistedProjectionSidecarBody,
    ) -> Result<Vec<u8>, StorageError> {
        let mut digest_body = body.clone();
        digest_body.digest =
            Self::placeholder_compact_digest(crate::ColumnarV2FormatTag::projection_sidecar());
        Self::encode_projection_sidecar_body(&digest_body)
    }

    pub(super) fn encoded_field_value_bytes(value: &FieldValue) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(value).map_err(|error| {
            StorageError::corruption(format!("encode skip-index field value failed: {error}"))
        })
    }

    fn build_bloom_from_encoded_values(
        encoded_values: &[Vec<u8>],
        bits_per_key: Option<u32>,
    ) -> Option<UserKeyBloomFilter> {
        let bits_per_key = bits_per_key?.max(1);
        let unique_values = encoded_values
            .iter()
            .map(|value| value.as_slice())
            .collect::<BTreeSet<_>>();
        if unique_values.is_empty() {
            return None;
        }

        let bit_count = unique_values
            .len()
            .saturating_mul(bits_per_key as usize)
            .max(8);
        let byte_len = bit_count.div_ceil(8);
        let total_bits = byte_len * 8;
        let hash_count = ((bits_per_key.saturating_mul(69)).saturating_add(50) / 100).max(1);
        let mut filter = UserKeyBloomFilter {
            bits_per_key,
            hash_count,
            bytes: vec![0; byte_len],
        };

        for value in unique_values {
            filter.insert(value, total_bits);
        }

        Some(filter)
    }

    fn build_sidecar_specs(
        &self,
        stored: &StoredTable,
    ) -> Result<
        (
            Vec<ResolvedSkipIndexSpec>,
            Vec<ResolvedProjectionSidecarSpec>,
        ),
        StorageError,
    > {
        let features = Self::hybrid_table_features(&stored.config.metadata)?;
        let Some(schema) = stored.config.schema.as_ref() else {
            return Ok((Vec::new(), Vec::new()));
        };
        let validation = SchemaValidation::new(schema)?;

        let skip_indexes = if self.inner.config.hybrid_read.skip_indexes_enabled {
            features
                .skip_indexes
                .into_iter()
                .map(|config| {
                    let field = match config.family {
                        HybridSkipIndexFamily::UserKeyBloom => None,
                        HybridSkipIndexFamily::FieldValueBloom
                        | HybridSkipIndexFamily::BoundedSet => {
                            let field_name = config.field.ok_or_else(|| {
                                StorageError::unsupported(format!(
                                    "skip-index {} requires a field",
                                    config.name
                                ))
                            })?;
                            let field_id = validation
                                .field_ids_by_name
                                .get(&field_name)
                                .copied()
                                .ok_or_else(|| {
                                StorageError::unsupported(format!(
                                    "skip-index {} references unknown field {}",
                                    config.name, field_name
                                ))
                            })?;
                            Some(
                                (*validation.fields_by_id.get(&field_id).ok_or_else(|| {
                                    StorageError::corruption(format!(
                                        "columnar table {} schema is missing field id {}",
                                        stored.config.name,
                                        field_id.get()
                                    ))
                                })?)
                                .clone(),
                            )
                        }
                    };
                    Ok(ResolvedSkipIndexSpec {
                        name: config.name,
                        family: config.family,
                        field,
                        max_values: config.max_values,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?
        } else {
            Vec::new()
        };

        let projection_sidecars = if self.inner.config.hybrid_read.projection_sidecars_enabled {
            features
                .projection_sidecars
                .into_iter()
                .map(|config| {
                    let mut fields = Vec::with_capacity(config.fields.len());
                    for field_name in config.fields {
                        let field_id = validation
                            .field_ids_by_name
                            .get(&field_name)
                            .copied()
                            .ok_or_else(|| {
                                StorageError::unsupported(format!(
                                    "projection-sidecar {} references unknown field {}",
                                    config.name, field_name
                                ))
                            })?;
                        let field = validation.fields_by_id.get(&field_id).ok_or_else(|| {
                            StorageError::corruption(format!(
                                "columnar table {} schema is missing field id {}",
                                stored.config.name,
                                field_id.get()
                            ))
                        })?;
                        fields.push((*field).clone());
                    }
                    Ok(ResolvedProjectionSidecarSpec {
                        name: config.name,
                        projection: ColumnProjection { fields },
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?
        } else {
            Vec::new()
        };

        Ok((skip_indexes, projection_sidecars))
    }

    fn build_optional_sidecars(
        &self,
        local_id: &str,
        stored: &StoredTable,
        rows: &[SstableRow],
        applied_generation: Option<ManifestId>,
    ) -> Result<Vec<EncodedOptionalSidecar>, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let (skip_specs, projection_specs) = self.build_sidecar_specs(stored)?;
        let mut encoded = Vec::new();

        for spec in skip_specs {
            let file_name = Self::skip_index_file_name(local_id, &spec.name);
            let payload = match spec.family {
                HybridSkipIndexFamily::UserKeyBloom => {
                    let unique_keys = rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
                    let filter = UserKeyBloomFilter::build(
                        &unique_keys
                            .iter()
                            .map(|key| SstableRow {
                                key: key.clone(),
                                sequence: SequenceNumber::new(0),
                                kind: ChangeKind::Put,
                                value: Some(Value::bytes(Vec::new())),
                            })
                            .collect::<Vec<_>>(),
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "skip-index {} cannot be built for an empty part",
                            spec.name
                        ))
                    })?;
                    PersistedSkipIndexSidecarPayload::UserKeyBloom { filter }
                }
                HybridSkipIndexFamily::FieldValueBloom => {
                    let field = spec.field.as_ref().ok_or_else(|| {
                        StorageError::corruption(format!(
                            "skip-index {} is missing its resolved field",
                            spec.name
                        ))
                    })?;
                    let encoded_values = rows
                        .iter()
                        .filter_map(|row| row.value.as_ref())
                        .map(|value| {
                            let Value::Record(record) = value else {
                                return Err(StorageError::corruption(
                                    "columnar row unexpectedly stored bytes in a skip index",
                                ));
                            };
                            let field_value = record.get(&field.id).ok_or_else(|| {
                                StorageError::corruption(format!(
                                    "columnar row is missing field {} for skip-index {}",
                                    field.name, spec.name
                                ))
                            })?;
                            Self::encoded_field_value_bytes(field_value)
                        })
                        .collect::<Result<Vec<_>, StorageError>>()?;
                    let filter = Self::build_bloom_from_encoded_values(
                        &encoded_values,
                        stored.config.bloom_filter_bits_per_key,
                    )
                    .ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "skip-index {} cannot be built for an empty part",
                            spec.name
                        ))
                    })?;
                    PersistedSkipIndexSidecarPayload::FieldValueBloom { filter }
                }
                HybridSkipIndexFamily::BoundedSet => {
                    let field = spec.field.as_ref().ok_or_else(|| {
                        StorageError::corruption(format!(
                            "skip-index {} is missing its resolved field",
                            spec.name
                        ))
                    })?;
                    let mut values = BTreeMap::<Vec<u8>, FieldValue>::new();
                    let mut saturated = false;
                    for row in rows.iter().filter_map(|row| row.value.as_ref()) {
                        let Value::Record(record) = row else {
                            return Err(StorageError::corruption(
                                "columnar row unexpectedly stored bytes in a skip index",
                            ));
                        };
                        let field_value = record.get(&field.id).ok_or_else(|| {
                            StorageError::corruption(format!(
                                "columnar row is missing field {} for skip-index {}",
                                field.name, spec.name
                            ))
                        })?;
                        values
                            .entry(Self::encoded_field_value_bytes(field_value)?)
                            .or_insert_with(|| field_value.clone());
                        if values.len() > spec.max_values {
                            saturated = true;
                            break;
                        }
                    }
                    PersistedSkipIndexSidecarPayload::BoundedSet {
                        values: if saturated {
                            Vec::new()
                        } else {
                            values.into_values().collect()
                        },
                        saturated,
                    }
                }
            };
            let body = PersistedSkipIndexSidecarBody {
                format_version: COLUMNAR_V2_SKIP_INDEX_SIDECAR_FORMAT_VERSION,
                table_id: stored.id,
                local_id: local_id.to_string(),
                index_name: spec.name.clone(),
                family: spec.family,
                field_id: spec.field.as_ref().map(|field| field.id),
                schema_version: schema.version,
                applied_generation,
                row_count: rows.len() as u64,
                digest: Self::placeholder_compact_digest(
                    crate::ColumnarV2FormatTag::skip_index_sidecar(),
                ),
                payload,
            };
            let mut body = body;
            let body_bytes = Self::skip_index_sidecar_digest_input(&body)?;
            body.digest = Self::compact_crc32_digest(
                crate::ColumnarV2FormatTag::skip_index_sidecar(),
                body_bytes.len() as u64,
                &body_bytes,
            );
            let body_bytes = Self::encode_skip_index_body(&body)?;
            let checksum = checksum32(&body_bytes);
            let file = PersistedSkipIndexSidecarFile { body, checksum };
            let bytes = serde_json::to_vec(&file).map_err(|error| {
                StorageError::corruption(format!("encode skip-index sidecar file failed: {error}"))
            })?;
            encoded.push(EncodedOptionalSidecar::SkipIndex {
                descriptor: PersistedSkipIndexSidecarDescriptor {
                    file_name,
                    index_name: spec.name,
                    family: spec.family,
                    field_id: spec.field.map(|field| field.id),
                    checksum,
                },
                bytes,
            });
        }

        for spec in projection_specs {
            let file_name = Self::projection_sidecar_file_name(local_id, &spec.name);
            let rows = rows
                .iter()
                .map(|row| {
                    row.value
                        .as_ref()
                        .map(|value| {
                            Self::project_columnar_value(value, &spec.projection, row.kind)
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>, StorageError>>()?;
            let body = PersistedProjectionSidecarBody {
                format_version: COLUMNAR_V2_PROJECTION_SIDECAR_FORMAT_VERSION,
                table_id: stored.id,
                local_id: local_id.to_string(),
                projection_name: spec.name.clone(),
                schema_version: schema.version,
                applied_generation,
                row_count: rows.len() as u64,
                projected_fields: spec
                    .projection
                    .fields
                    .iter()
                    .map(|field| field.id)
                    .collect(),
                digest: Self::placeholder_compact_digest(
                    crate::ColumnarV2FormatTag::projection_sidecar(),
                ),
                rows,
            };
            let mut body = body;
            let body_bytes = Self::projection_sidecar_digest_input(&body)?;
            body.digest = Self::compact_crc32_digest(
                crate::ColumnarV2FormatTag::projection_sidecar(),
                body_bytes.len() as u64,
                &body_bytes,
            );
            let body_bytes = Self::encode_projection_sidecar_body(&body)?;
            let checksum = checksum32(&body_bytes);
            let file = PersistedProjectionSidecarFile { body, checksum };
            let bytes = serde_json::to_vec(&file).map_err(|error| {
                StorageError::corruption(format!("encode projection sidecar file failed: {error}"))
            })?;
            encoded.push(EncodedOptionalSidecar::Projection {
                descriptor: PersistedProjectionSidecarDescriptor {
                    file_name,
                    projection_name: spec.name,
                    projected_fields: spec
                        .projection
                        .fields
                        .iter()
                        .map(|field| field.id)
                        .collect(),
                    checksum,
                },
                bytes,
            });
        }

        Ok(encoded)
    }

    fn choose_columnar_output_layout(
        &self,
        stored: &StoredTable,
        level: u32,
        local_id: &str,
        rows: &[SstableRow],
        inputs: &[ResidentRowSstable],
    ) -> Result<ColumnarOutputLayout, StorageError> {
        let Some(config) = self.compact_to_wide_promotion_config_for_table(stored)? else {
            return Ok(ColumnarOutputLayout::Wide);
        };
        if inputs.iter().any(ResidentRowSstable::is_columnar) {
            return Ok(ColumnarOutputLayout::Wide);
        }
        if rows.len() > config.max_compact_rows {
            return Ok(ColumnarOutputLayout::Wide);
        }
        if level < config.promote_on_compaction_to_level {
            return Ok(ColumnarOutputLayout::Compact);
        }
        if inputs.is_empty() {
            return Ok(ColumnarOutputLayout::Wide);
        }

        let decision = ConservativeCompactToWidePolicy { enabled: true }
            .decide(&self.compact_to_wide_candidate(stored.id, local_id, rows.len(), inputs));
        Ok(match decision {
            CompactToWidePromotionDecision::KeepCompact => ColumnarOutputLayout::Compact,
            CompactToWidePromotionDecision::PromoteWide => ColumnarOutputLayout::Wide,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_columnar_table_output(
        &self,
        path: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
        applied_generation: Option<ManifestId>,
        inputs: &[ResidentRowSstable],
    ) -> Result<ResidentRowSstable, StorageError> {
        match self.choose_columnar_output_layout(stored, level, &local_id, &rows, inputs)? {
            ColumnarOutputLayout::Compact => {
                self.write_row_sstable(
                    path,
                    stored.id,
                    level,
                    local_id,
                    rows,
                    stored.config.bloom_filter_bits_per_key,
                )
                .await
            }
            ColumnarOutputLayout::Wide => {
                self.write_columnar_sstable(path, level, local_id, stored, rows, applied_generation)
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn write_columnar_table_output_remote(
        &self,
        object_key: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
        applied_generation: Option<ManifestId>,
        inputs: &[ResidentRowSstable],
    ) -> Result<ResidentRowSstable, StorageError> {
        match self.choose_columnar_output_layout(stored, level, &local_id, &rows, inputs)? {
            ColumnarOutputLayout::Compact => {
                self.write_row_sstable_remote(
                    object_key,
                    stored.id,
                    level,
                    local_id,
                    rows,
                    stored.config.bloom_filter_bits_per_key,
                )
                .await
            }
            ColumnarOutputLayout::Wide => {
                self.write_columnar_sstable_remote(
                    object_key,
                    level,
                    local_id,
                    stored,
                    rows,
                    applied_generation,
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn encode_columnar_sstable(
        table_id: TableId,
        level: u32,
        local_id: String,
        schema: &SchemaDefinition,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
        applied_generation: Option<ManifestId>,
        optional_sidecars: Vec<PersistedOptionalSidecarDescriptor>,
    ) -> Result<(ResidentRowSstable, Vec<u8>), StorageError> {
        schema.validate()?;

        let mut meta = Self::summarize_sstable_rows(table_id, level, &local_id, &rows)?;
        meta.schema_version = Some(schema.version);
        let user_key_bloom_filter = UserKeyBloomFilter::build(&rows, bloom_filter_bits_per_key);

        let mut bytes = Vec::from(&COLUMNAR_SSTABLE_MAGIC[..]);
        let mut substreams = Vec::new();
        let mut next_ordinal = 0_u32;

        let key_index_block = Self::encode_columnar_key_block(
            bytes.len() as u64,
            &rows,
            &mut substreams,
            &mut next_ordinal,
        )?;
        let key_index = Self::append_columnar_block(&mut bytes, &key_index_block);

        let sequence_block = Self::encode_columnar_sequence_block(
            bytes.len() as u64,
            &rows,
            &mut substreams,
            &mut next_ordinal,
        )?;
        let sequence_column = Self::append_columnar_block(&mut bytes, &sequence_block);

        let (tombstone_block, _tombstones) = Self::encode_columnar_tombstone_block(
            bytes.len() as u64,
            &rows,
            &mut substreams,
            &mut next_ordinal,
        )?;
        let tombstone_bitmap = Self::append_columnar_block(&mut bytes, &tombstone_block);

        let row_kind_block = Self::encode_columnar_row_kind_block(
            bytes.len() as u64,
            &rows,
            &mut substreams,
            &mut next_ordinal,
        )?;
        let row_kind_column = Self::append_columnar_block(&mut bytes, &row_kind_block);

        let mut columns = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let block_bytes = Self::encode_columnar_field_block(
                field,
                &rows,
                bytes.len() as u64,
                &mut substreams,
                &mut next_ordinal,
            )?;
            let block = Self::append_columnar_block(&mut bytes, &block_bytes);
            columns.push(PersistedColumnarColumnFooter {
                field_id: field.id,
                field_type: field.field_type,
                encoding: ColumnEncoding::Plain,
                compression: ColumnCompression::None,
                block,
            });
        }

        let data_range =
            crate::ByteRange::new(COLUMNAR_SSTABLE_MAGIC.len() as u64, bytes.len() as u64);
        let data_checksum = checksum32(&bytes[COLUMNAR_SSTABLE_MAGIC.len()..]);
        let layout = crate::ColumnarFooter {
            format_tag: crate::ColumnarFormatTag::base_part(),
            table_id,
            local_id: local_id.clone(),
            schema_version: schema.version,
            row_count: rows.len() as u64,
            data_range,
            decode_metadata: Self::columnar_decode_metadata(schema),
            substreams,
            marks: Vec::new(),
            synopsis: crate::ColumnarSynopsisSidecar {
                format_tag: crate::ColumnarFormatTag::synopsis_sidecar(),
                part_local_id: local_id.clone(),
                granules: Vec::new(),
                checksum: 0,
            },
            optional_sidecars: Vec::new(),
            digests: vec![crate::CompactPartDigest {
                format_tag: crate::ColumnarFormatTag::compact_digest(),
                algorithm: crate::PartDigestAlgorithm::Crc32,
                logical_bytes: data_range.len(),
                digest_bytes: data_checksum.to_le_bytes().to_vec(),
            }],
        };
        let footer = PersistedColumnarSstableFooter {
            format_version: COLUMNAR_SSTABLE_FORMAT_VERSION,
            table_id,
            level,
            local_id: local_id.clone(),
            row_count: rows.len() as u64,
            min_key: meta.min_key.clone(),
            max_key: meta.max_key.clone(),
            min_sequence: meta.min_sequence,
            max_sequence: meta.max_sequence,
            schema_version: schema.version,
            data_checksum,
            key_index,
            sequence_column,
            tombstone_bitmap,
            row_kind_column,
            columns,
            layout,
            applied_generation,
            digests: Vec::new(),
            optional_sidecars,
            user_key_bloom_filter: user_key_bloom_filter.clone(),
        };
        let mut footer = footer;
        footer.digests = vec![Self::compact_crc32_digest(
            crate::ColumnarV2FormatTag::base_part(),
            bytes.len().saturating_sub(COLUMNAR_SSTABLE_MAGIC.len()) as u64,
            &bytes[COLUMNAR_SSTABLE_MAGIC.len()..],
        )];
        let footer_bytes = Self::encode_json_block("columnar SSTable footer", &footer)?;
        bytes.extend_from_slice(&footer_bytes);
        bytes.extend_from_slice(&(footer_bytes.len() as u64).to_le_bytes());
        bytes.extend_from_slice(COLUMNAR_SSTABLE_MAGIC);

        let checksum = checksum32(&bytes);
        meta.length = bytes.len() as u64;
        meta.checksum = checksum;
        meta.data_checksum = data_checksum;

        Ok((
            ResidentRowSstable {
                meta,
                rows,
                user_key_bloom_filter,
                columnar: None,
            },
            bytes,
        ))
    }

    pub(super) async fn write_row_sstable(
        &self,
        path: &str,
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let (mut resident, bytes) =
            Self::encode_row_sstable(table_id, level, local_id, rows, bloom_filter_bits_per_key)?;

        let handle = self
            .inner
            .dependencies
            .file_system
            .open(
                path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&handle, 0, &bytes)
            .await?;
        self.inner.dependencies.file_system.sync(&handle).await?;

        resident.meta.file_path = path.to_string();
        Ok(resident)
    }

    async fn publish_optional_sidecars_local(
        &self,
        base_path: &str,
        sidecars: &[EncodedOptionalSidecar],
    ) {
        for sidecar in sidecars {
            let target = Self::sibling_path(base_path, sidecar.file_name());
            let _ =
                write_local_file_atomic(&self.inner.dependencies, &target, sidecar.bytes()).await;
        }
    }

    async fn publish_optional_sidecars_remote(
        &self,
        base_key: &str,
        sidecars: &[EncodedOptionalSidecar],
    ) {
        for sidecar in sidecars {
            let target = StorageSource::remote_object(Self::sibling_remote_key(
                base_key,
                sidecar.file_name(),
            ));
            let _ = Self::write_source_atomic(
                &self.inner.dependencies,
                &self.inner.dependencies.rng,
                &target,
                sidecar.bytes(),
            )
            .await;
        }
    }

    pub(super) async fn write_columnar_sstable(
        &self,
        path: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
        applied_generation: Option<ManifestId>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let sidecars =
            self.build_optional_sidecars(&local_id, stored, &rows, applied_generation)?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
            applied_generation,
            sidecars
                .iter()
                .map(EncodedOptionalSidecar::descriptor)
                .collect(),
        )?;
        self.publish_optional_sidecars_local(path, &sidecars).await;
        write_local_file_atomic(&self.inner.dependencies, path, &bytes).await?;

        resident.meta.file_path = path.to_string();
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::local_file(path.to_string()),
        });
        Ok(resident)
    }

    pub(super) async fn write_row_sstable_remote(
        &self,
        object_key: &str,
        table_id: TableId,
        level: u32,
        local_id: String,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let (mut resident, bytes) =
            Self::encode_row_sstable(table_id, level, local_id, rows, bloom_filter_bits_per_key)?;
        self.inner
            .dependencies
            .object_store
            .put(object_key, &bytes)
            .await?;
        resident.meta.remote_key = Some(object_key.to_string());
        Ok(resident)
    }

    pub(super) async fn write_columnar_sstable_remote(
        &self,
        object_key: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
        applied_generation: Option<ManifestId>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let sidecars =
            self.build_optional_sidecars(&local_id, stored, &rows, applied_generation)?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
            applied_generation,
            sidecars
                .iter()
                .map(EncodedOptionalSidecar::descriptor)
                .collect(),
        )?;
        self.publish_optional_sidecars_remote(object_key, &sidecars)
            .await;
        Self::write_source_atomic(
            &self.inner.dependencies,
            &self.inner.dependencies.rng,
            &StorageSource::remote_object(object_key.to_string()),
            &bytes,
        )
        .await?;
        resident.meta.remote_key = Some(object_key.to_string());
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::remote_object(object_key.to_string()),
        });
        Ok(resident)
    }

    pub(super) async fn install_manifest(
        &self,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
    ) -> Result<(), StorageError> {
        let root = self.local_storage_root().ok_or_else(|| {
            StorageError::unsupported("local manifest is only used in tiered mode")
        })?;
        let manifest_dir = Self::local_manifest_dir(root);
        let manifest_path = Self::local_manifest_path(root, generation);
        let manifest_temp_path = format!("{manifest_path}{LOCAL_MANIFEST_TEMP_SUFFIX}");
        let payload = Self::encode_manifest_payload_from_sstables(
            generation,
            last_flushed_sequence,
            &live
                .iter()
                .map(|sstable| sstable.meta.clone())
                .collect::<Vec<_>>(),
        )?;

        let manifest_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                &manifest_temp_path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&manifest_handle, 0, &payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&manifest_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(&manifest_temp_path, &manifest_path)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync_dir(&manifest_dir)
            .await?;

        let _ = self
            .__run_failpoint(
                crate::failpoints::names::DB_MANIFEST_BEFORE_CURRENT_POINTER,
                BTreeMap::from([("generation".to_string(), generation.get().to_string())]),
            )
            .await?;

        let current_path = Self::local_current_path(root);
        let current_temp_path = format!("{current_path}{LOCAL_CATALOG_TEMP_SUFFIX}");
        let current_payload = format!("{}\n", Self::manifest_filename(generation)).into_bytes();
        let current_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                &current_temp_path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .write_at(&current_handle, 0, &current_payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&current_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(&current_temp_path, &current_path)
            .await?;
        self.inner.dependencies.file_system.sync_dir(root).await?;

        Ok(())
    }

    pub(super) async fn install_remote_manifest(
        &self,
        config: &S3PrimaryStorageConfig,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
        durable_commit_log_segments: &[DurableRemoteCommitLogSegment],
    ) -> Result<(), StorageError> {
        let manifest_key = Self::remote_manifest_path(config, generation);
        let latest_key = Self::remote_manifest_latest_key(config);
        let payload = Self::encode_remote_manifest_payload(
            generation,
            last_flushed_sequence,
            &live
                .iter()
                .map(|sstable| sstable.meta.clone())
                .collect::<Vec<_>>(),
            durable_commit_log_segments,
        )?;

        self.inner
            .dependencies
            .object_store
            .put(&manifest_key, &payload)
            .await?;
        let _ = self
            .__run_failpoint(
                crate::failpoints::names::DB_REMOTE_MANIFEST_BEFORE_LATEST_POINTER,
                BTreeMap::from([("generation".to_string(), generation.get().to_string())]),
            )
            .await?;
        self.inner
            .dependencies
            .object_store
            .put(&latest_key, format!("{manifest_key}\n").as_bytes())
            .await?;
        Ok(())
    }

    pub(super) fn tiered_backup_layout(&self) -> Option<ObjectKeyLayout> {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => Some(Self::tiered_object_layout(config)),
            StorageConfig::S3Primary(_) => None,
        }
    }

    pub(super) async fn write_remote_backup_object(
        &self,
        layout: &ObjectKeyLayout,
        object_key: &str,
        bytes: &[u8],
        track_for_gc: bool,
    ) -> Result<(), StorageError> {
        self.inner
            .dependencies
            .object_store
            .put(object_key, bytes)
            .await?;
        if track_for_gc {
            Self::note_backup_object_birth(&self.inner.dependencies, layout, object_key).await;
        }
        Ok(())
    }

    pub(super) async fn note_backup_object_birth(
        dependencies: &DbDependencies,
        layout: &ObjectKeyLayout,
        object_key: &str,
    ) {
        let metadata_key = layout.backup_gc_metadata(object_key);
        if read_optional_remote_object(dependencies, &metadata_key)
            .await
            .ok()
            .flatten()
            .is_some()
        {
            return;
        }

        let record = BackupObjectBirthRecord {
            format_version: BACKUP_GC_METADATA_FORMAT_VERSION,
            object_key: object_key.to_string(),
            first_uploaded_at_millis: dependencies.clock.now().get(),
        };
        let Ok(payload) = Self::encode_backup_object_birth_payload(&record) else {
            return;
        };
        let _ = dependencies.object_store.put(&metadata_key, &payload).await;
    }

    pub(super) async fn read_backup_object_birth(
        dependencies: &DbDependencies,
        layout: &ObjectKeyLayout,
        object_key: &str,
    ) -> Option<BackupObjectBirthRecord> {
        let metadata_key = layout.backup_gc_metadata(object_key);
        let bytes = read_optional_remote_object(dependencies, &metadata_key)
            .await
            .ok()
            .flatten()?;
        let record = Self::decode_backup_object_birth_payload(&bytes).ok()?;
        (record.format_version == BACKUP_GC_METADATA_FORMAT_VERSION
            && record.object_key == object_key)
            .then_some(record)
    }

    pub(super) async fn collect_tiered_commit_log_snapshots(
        &self,
        layout: &ObjectKeyLayout,
    ) -> Result<Vec<(DurableRemoteCommitLogSegment, Vec<u8>)>, StorageError> {
        let mut runtime = self.inner.commit_runtime.lock().await;
        let CommitLogBackend::Local(manager) = &mut runtime.backend else {
            return Ok(Vec::new());
        };

        let mut snapshots = Vec::new();
        for descriptor in manager.enumerate_segments() {
            if descriptor.record_count == 0 {
                continue;
            }

            let bytes = manager.read_segment_bytes(descriptor.segment_id).await?;
            let footer = segment_footer_from_bytes(descriptor.segment_id, &bytes)?;
            snapshots.push((
                DurableRemoteCommitLogSegment {
                    object_key: layout.backup_commit_log_segment(descriptor.segment_id),
                    footer,
                },
                bytes,
            ));
        }

        Ok(snapshots)
    }

    pub(super) async fn sync_tiered_backup_catalog(&self) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };
        let payload = Self::encode_catalog(&self.tables_read().clone())?;
        let _backup_guard = self.inner.backup_lock.lock().await;
        self.write_remote_backup_object(&layout, &layout.backup_catalog(), &payload, false)
            .await
    }

    pub(super) async fn sync_tiered_commit_log_tail(&self) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };
        let _backup_guard = self.inner.backup_lock.lock().await;
        let snapshots = self.collect_tiered_commit_log_snapshots(&layout).await?;
        for (segment, bytes) in snapshots {
            self.write_remote_backup_object(&layout, &segment.object_key, &bytes, true)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn sync_tiered_backup_manifest(
        &self,
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        live: &[ResidentRowSstable],
    ) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };

        let catalog_payload = Self::encode_catalog(&self.tables_read().clone())?;
        let mut manifest_sstables = Vec::with_capacity(live.len());
        let mut local_sstable_uploads = Vec::<(String, Vec<u8>)>::new();

        for sstable in live {
            let mut meta = sstable.meta.clone();
            if !meta.file_path.is_empty() {
                let backup_key = layout.backup_sstable(meta.table_id, 0, &meta.local_id);
                let bytes = read_path(&self.inner.dependencies, &meta.file_path).await?;
                meta.file_path.clear();
                meta.remote_key = Some(backup_key.clone());
                local_sstable_uploads.push((backup_key, bytes));
            }
            manifest_sstables.push(meta);
        }

        let _backup_guard = self.inner.backup_lock.lock().await;
        self.write_remote_backup_object(&layout, &layout.backup_catalog(), &catalog_payload, false)
            .await?;

        for (object_key, bytes) in local_sstable_uploads {
            self.write_remote_backup_object(&layout, &object_key, &bytes, true)
                .await?;
        }

        let segment_snapshots = self.collect_tiered_commit_log_snapshots(&layout).await?;
        let durable_segments = segment_snapshots
            .iter()
            .map(|(segment, _)| segment.clone())
            .filter(|segment| segment.footer.max_sequence <= last_flushed_sequence)
            .collect::<Vec<_>>();
        for (segment, bytes) in &segment_snapshots {
            self.write_remote_backup_object(&layout, &segment.object_key, bytes, true)
                .await?;
        }

        let payload = Self::encode_remote_manifest_payload(
            generation,
            last_flushed_sequence,
            &manifest_sstables,
            &durable_segments,
        )?;
        let manifest_key = layout.backup_manifest(generation);
        self.write_remote_backup_object(&layout, &manifest_key, &payload, true)
            .await?;
        let _ = self
            .__run_failpoint(
                crate::failpoints::names::DB_BACKUP_MANIFEST_BEFORE_LATEST_POINTER,
                BTreeMap::from([("generation".to_string(), generation.get().to_string())]),
            )
            .await?;
        self.inner
            .dependencies
            .object_store
            .put(
                &layout.backup_manifest_latest(),
                format!("{manifest_key}\n").as_bytes(),
            )
            .await?;
        self.run_tiered_backup_gc_locked(&layout).await
    }

    pub(super) async fn run_tiered_backup_gc_locked(
        &self,
        layout: &ObjectKeyLayout,
    ) -> Result<(), StorageError> {
        let latest_key = layout.backup_manifest_latest();
        let mut referenced = BTreeSet::from([latest_key.clone(), layout.backup_catalog()]);
        let mut manifest_keys = self
            .inner
            .dependencies
            .object_store
            .list(&layout.backup_manifest_prefix())
            .await?;
        manifest_keys
            .retain(|key| key != &latest_key && Self::parse_manifest_generation(key).is_some());
        manifest_keys.sort_by_key(|key| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(key)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });

        let retained_manifest_keys = manifest_keys
            .iter()
            .take(BACKUP_MANIFEST_RETENTION_LIMIT)
            .cloned()
            .collect::<Vec<_>>();
        let mut min_last_flushed_sequence = None;
        for key in &retained_manifest_keys {
            referenced.insert(key.clone());
            let Ok(file) =
                Self::read_remote_manifest_file_at_key(&self.inner.dependencies, key).await
            else {
                continue;
            };
            min_last_flushed_sequence = Some(
                min_last_flushed_sequence
                    .map(|current: SequenceNumber| current.min(file.body.last_flushed_sequence))
                    .unwrap_or(file.body.last_flushed_sequence),
            );
            for sstable in &file.body.sstables {
                if let Some(remote_key) = &sstable.remote_key {
                    referenced.insert(remote_key.clone());
                }
            }
        }

        if let Some(min_last_flushed_sequence) = min_last_flushed_sequence {
            let commit_log_keys = self
                .inner
                .dependencies
                .object_store
                .list(&layout.backup_commit_log_prefix())
                .await?;
            for key in commit_log_keys {
                let Some(segment_id) = Self::parse_segment_id(&key) else {
                    continue;
                };
                let Ok(bytes) = self.inner.dependencies.object_store.get(&key).await else {
                    continue;
                };
                let Ok(footer) = segment_footer_from_bytes(segment_id, &bytes) else {
                    continue;
                };
                if footer.max_sequence > min_last_flushed_sequence {
                    referenced.insert(key);
                }
            }
        }

        let mut candidates = Vec::new();
        for prefix in [
            layout.backup_manifest_prefix(),
            layout.backup_commit_log_prefix(),
            layout.backup_sstable_prefix(),
            layout.cold_prefix(),
        ] {
            candidates.extend(self.inner.dependencies.object_store.list(&prefix).await?);
        }
        candidates.sort();
        candidates.dedup();

        let now_millis = self.inner.dependencies.clock.now().get();
        for key in candidates {
            if key.starts_with(&layout.backup_gc_metadata_prefix()) || referenced.contains(&key) {
                continue;
            }
            let Some(record) =
                Self::read_backup_object_birth(&self.inner.dependencies, layout, &key).await
            else {
                continue;
            };
            if now_millis.saturating_sub(record.first_uploaded_at_millis)
                < BACKUP_GC_GRACE_PERIOD_MILLIS
            {
                continue;
            }
            self.inner.dependencies.object_store.delete(&key).await?;
            let _ = self
                .inner
                .dependencies
                .object_store
                .delete(&layout.backup_gc_metadata(&key))
                .await;
        }

        Ok(())
    }

    async fn verify_part_bytes(&self, part: &HybridPartDescriptor) -> Result<(), StorageError> {
        let bytes = read_source(&self.inner.dependencies, &part.source).await?;
        match part.format_tag.kind {
            crate::ColumnarV2ArtifactKind::BasePart => {
                let (footer, footer_start) =
                    Self::columnar_footer_from_bytes(part.source.target(), &bytes)?;
                let data_region = &bytes[COLUMNAR_SSTABLE_MAGIC.len()..footer_start];
                let digests = if part.digests.is_empty() {
                    footer.digests
                } else {
                    part.digests.clone()
                };
                if digests.is_empty() {
                    return Err(StorageError::unsupported(
                        "base part verification requires at least one digest",
                    ));
                }
                for digest in &digests {
                    Self::validate_compact_digest(
                        digest,
                        crate::ColumnarV2FormatTag::base_part(),
                        data_region,
                    )?;
                }
                if footer.local_id != part.local_id {
                    return Err(StorageError::corruption(format!(
                        "base part {} did not match requested local id {}",
                        footer.local_id, part.local_id
                    )));
                }
                Ok(())
            }
            crate::ColumnarV2ArtifactKind::SkipIndexSidecar => {
                let file: PersistedSkipIndexSidecarFile =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        StorageError::corruption(format!(
                            "decode skip-index sidecar {} failed: {error}",
                            part.local_id
                        ))
                    })?;
                let body_bytes = serde_json::to_vec(&file.body).map_err(|error| {
                    StorageError::corruption(format!(
                        "encode skip-index sidecar {} body failed: {error}",
                        part.local_id
                    ))
                })?;
                if checksum32(&body_bytes) != file.checksum {
                    return Err(StorageError::corruption(format!(
                        "skip-index sidecar {} checksum mismatch",
                        part.local_id
                    )));
                }
                let digest_input = Self::skip_index_sidecar_digest_input(&file.body)?;
                Self::validate_compact_digest(
                    &file.body.digest,
                    crate::ColumnarV2FormatTag::skip_index_sidecar(),
                    &digest_input,
                )?;
                for digest in &part.digests {
                    Self::validate_compact_digest(
                        digest,
                        crate::ColumnarV2FormatTag::skip_index_sidecar(),
                        &digest_input,
                    )?;
                }
                if file.body.local_id != part.local_id {
                    return Err(StorageError::corruption(format!(
                        "skip-index sidecar {} did not match requested local id {}",
                        file.body.local_id, part.local_id
                    )));
                }
                Ok(())
            }
            crate::ColumnarV2ArtifactKind::ProjectionSidecar => {
                let file: PersistedProjectionSidecarFile =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        StorageError::corruption(format!(
                            "decode projection sidecar {} failed: {error}",
                            part.local_id
                        ))
                    })?;
                let body_bytes = serde_json::to_vec(&file.body).map_err(|error| {
                    StorageError::corruption(format!(
                        "encode projection sidecar {} body failed: {error}",
                        part.local_id
                    ))
                })?;
                if checksum32(&body_bytes) != file.checksum {
                    return Err(StorageError::corruption(format!(
                        "projection sidecar {} checksum mismatch",
                        part.local_id
                    )));
                }
                let digest_input = Self::projection_sidecar_digest_input(&file.body)?;
                Self::validate_compact_digest(
                    &file.body.digest,
                    crate::ColumnarV2FormatTag::projection_sidecar(),
                    &digest_input,
                )?;
                for digest in &part.digests {
                    Self::validate_compact_digest(
                        digest,
                        crate::ColumnarV2FormatTag::projection_sidecar(),
                        &digest_input,
                    )?;
                }
                if file.body.local_id != part.local_id {
                    return Err(StorageError::corruption(format!(
                        "projection sidecar {} did not match requested local id {}",
                        file.body.local_id, part.local_id
                    )));
                }
                Ok(())
            }
            crate::ColumnarV2ArtifactKind::SynopsisSidecar
            | crate::ColumnarV2ArtifactKind::CompactDigest => Ok(()),
        }
    }
}

#[async_trait]
impl PartRepairController for Db {
    async fn verify(&self, part: &HybridPartDescriptor) -> Result<RepairState, StorageError> {
        if let Some(marker) =
            Self::read_quarantine_marker(&self.inner.dependencies, &part.source).await?
        {
            return Ok(RepairState::Quarantined {
                reason: marker.reason,
            });
        }

        match self.verify_part_bytes(part).await {
            Ok(()) => Ok(RepairState::Verified),
            Err(error) => {
                Self::quarantine_artifact(
                    &self.inner.dependencies,
                    &self.inner.dependencies.rng,
                    &part.source,
                    &part.local_id,
                    error.message(),
                    self.inner.dependencies.clock.now().get(),
                )
                .await?;
                Ok(RepairState::Quarantined {
                    reason: error.message().to_string(),
                })
            }
        }
    }

    async fn quarantine(
        &self,
        part: &HybridPartDescriptor,
        reason: &str,
    ) -> Result<RepairState, StorageError> {
        Self::quarantine_artifact(
            &self.inner.dependencies,
            &self.inner.dependencies.rng,
            &part.source,
            &part.local_id,
            reason,
            self.inner.dependencies.clock.now().get(),
        )
        .await?;
        Ok(RepairState::Quarantined {
            reason: reason.to_string(),
        })
    }

    async fn repair(&self, part: &HybridPartDescriptor) -> Result<RepairState, StorageError> {
        match self.verify_part_bytes(part).await {
            Ok(()) => {
                Self::delete_source_if_exists(
                    &self.inner.dependencies,
                    &Self::artifact_quarantine_source(&part.source),
                )
                .await?;
                Ok(RepairState::Repaired)
            }
            Err(error) => {
                let reason = error.message().to_string();
                Self::quarantine_artifact(
                    &self.inner.dependencies,
                    &self.inner.dependencies.rng,
                    &part.source,
                    &part.local_id,
                    &reason,
                    self.inner.dependencies.clock.now().get(),
                )
                .await?;
                Ok(RepairState::Quarantined { reason })
            }
        }
    }
}
