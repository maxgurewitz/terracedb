impl ColumnarReadContext {
    fn cache_policy(
        &self,
        source: &StorageSource,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> ColumnarCachePolicy {
        let use_decoded_cache = self.decoded_cache_enabled.load(Ordering::Relaxed);
        let use_raw_byte_cache = matches!(source, StorageSource::RemoteObject { .. })
            && self.raw_byte_cache_enabled.load(Ordering::Relaxed)
            && self.remote_cache.is_some();
        let populate_hot_data = match artifact {
            ColumnarReadArtifact::Footer | ColumnarReadArtifact::Metadata => true,
            ColumnarReadArtifact::ColumnBlock => access == ColumnarReadAccessPattern::Point,
        };

        ColumnarCachePolicy {
            use_raw_byte_cache,
            populate_raw_byte_cache: use_raw_byte_cache && populate_hot_data,
            use_decoded_cache,
            populate_decoded_cache: use_decoded_cache && populate_hot_data,
        }
    }

    async fn read_range(
        &self,
        source: &StorageSource,
        range: std::ops::Range<u64>,
        access: ColumnarReadAccessPattern,
        artifact: ColumnarReadArtifact,
    ) -> Result<Vec<u8>, StorageError> {
        let policy = self.cache_policy(source, access, artifact);
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
        }

        let bytes = UnifiedStorage::new(
            self.dependencies.file_system.clone(),
            self.dependencies.object_store.clone(),
            None,
        )
        .read_range(source, range.clone())
        .await
        .map_err(|error| error.into_storage_error())?;
        if let StorageSource::RemoteObject { key } = source
            && policy.populate_raw_byte_cache
            && let Some(cache) = &self.remote_cache
        {
            cache.store(
                key,
                crate::remote::CacheSpan::Range {
                    start: range.start,
                    end: range.end,
                },
                &bytes,
            )
            .await?;
        }
        Ok(bytes)
    }

    async fn footer_from_source(
        &self,
        meta: &PersistedManifestSstable,
        source: &StorageSource,
        location: &str,
        access: ColumnarReadAccessPattern,
    ) -> Result<CachedColumnarFooter, StorageError> {
        let identity = meta.columnar_identity();
        let policy = self.cache_policy(source, access, ColumnarReadArtifact::Footer);
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
            .read_range(
                source,
                trailer_start..meta.length,
                access,
                ColumnarReadArtifact::Footer,
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
            .read_range(
                source,
                footer_start_u64..trailer_start,
                access,
                ColumnarReadArtifact::Footer,
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
            self.decoded_cache.insert_footer(identity, cached.clone());
        }
        Ok(cached)
    }

    async fn key_index(
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
        let values: Arc<Vec<Key>> = Arc::new(serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar key index for {location} failed: {error}"
            ))
        })?);
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_key_index(identity, values.clone());
        }
        Ok(values)
    }

    async fn sequence_column(
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
        let values: Arc<Vec<SequenceNumber>> =
            Arc::new(serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar sequence column for {location} failed: {error}"
            ))
            })?);
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_sequence_column(identity, values.clone());
        }
        Ok(values)
    }

    async fn tombstone_bitmap(
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
        let values: Arc<Vec<bool>> = Arc::new(serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar tombstone bitmap for {location} failed: {error}"
            ))
        })?);
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_tombstone_bitmap(identity, values.clone());
        }
        Ok(values)
    }

    async fn row_kind_column(
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
        let values: Arc<Vec<ChangeKind>> =
            Arc::new(serde_json::from_slice(&bytes).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar row-kind column for {location} failed: {error}"
            ))
            })?);
        if policy.populate_decoded_cache {
            self.decoded_cache
                .insert_row_kind_column(identity, values.clone());
        }
        Ok(values)
    }

    #[allow(clippy::too_many_arguments)]
    async fn column_block(
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
            location, column, row_count, &bytes,
        )?);
        if policy.populate_decoded_cache {
            self.decoded_cache.insert_column_block(key, values.clone());
        }
        Ok(values)
    }
}

impl Db {
    #[allow(dead_code)]
    async fn load_resident_sstable(
        dependencies: &DbDependencies,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let columnar_read_context = Self::ephemeral_columnar_read_context(dependencies);
        Self::load_resident_sstable_with_context(dependencies, &columnar_read_context, meta).await
    }

    async fn load_resident_sstable_with_context(
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        if meta.schema_version.is_some() {
            Self::load_lazy_columnar_sstable(columnar_read_context, meta).await
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
    async fn read_exact_source_range(
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
    async fn read_exact_source_range_with_context(
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
    async fn columnar_footer_from_source(
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
            .footer_from_source(&meta, source, location, ColumnarReadAccessPattern::Point)
            .await?;
        Ok(((*footer.footer).clone(), footer.footer_start))
    }

    fn validate_loaded_columnar_footer(
        location: &str,
        meta: &PersistedManifestSstable,
        footer: &PersistedColumnarSstableFooter,
    ) -> Result<(), StorageError> {
        if footer.format_version != COLUMNAR_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported columnar SSTable version {}",
                footer.format_version
            )));
        }
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
        Ok(())
    }

    async fn load_lazy_columnar_sstable(
        columnar_read_context: &ColumnarReadContext,
        meta: &PersistedManifestSstable,
    ) -> Result<ResidentRowSstable, StorageError> {
        let source = meta.storage_source();
        let location = meta.storage_descriptor();
        let footer = columnar_read_context
            .footer_from_source(meta, &source, location, ColumnarReadAccessPattern::Point)
            .await?;
        Self::validate_loaded_columnar_footer(location, meta, footer.footer.as_ref())?;

        Ok(ResidentRowSstable {
            meta: meta.clone(),
            rows: Vec::new(),
            user_key_bloom_filter: footer.footer.user_key_bloom_filter.clone(),
            columnar: Some(ResidentColumnarSstable { source }),
        })
    }

    fn decode_resident_row_sstable(
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
    fn decode_resident_columnar_sstable(
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
        if footer.format_version != COLUMNAR_SSTABLE_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported columnar SSTable version {}",
                footer.format_version
            )));
        }

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
        let key_index: Vec<Key> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "key index",
            &footer.key_index,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar key index for {location} failed: {error}"
            ))
        })?;
        let sequences: Vec<SequenceNumber> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "sequence column",
            &footer.sequence_column,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar sequence column for {location} failed: {error}"
            ))
        })?;
        let tombstones: Vec<bool> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "tombstone bitmap",
            &footer.tombstone_bitmap,
        )?)
        .map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar tombstone bitmap for {location} failed: {error}"
            ))
        })?;
        let row_kinds: Vec<ChangeKind> = serde_json::from_slice(Self::columnar_block_slice(
            location,
            bytes,
            footer_start,
            "row-kind column",
            &footer.row_kind_column,
        )?)
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
                Self::decode_columnar_field_values(location, column, row_count, block)?,
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
    fn columnar_footer_from_bytes(
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
    fn columnar_block_slice<'a>(
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

    fn columnar_block_range(
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

    fn normalized_columnar_row_kind(
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

    fn decode_columnar_field_values(
        location: &str,
        column: &PersistedColumnarColumnFooter,
        row_count: usize,
        block: &[u8],
    ) -> Result<Vec<FieldValue>, StorageError> {
        let decoded: PersistedColumnBlock = serde_json::from_slice(block).map_err(|error| {
            StorageError::corruption(format!(
                "decode columnar field {} for {location} failed: {error}",
                column.field_id.get()
            ))
        })?;
        match (column.field_type, decoded) {
            (FieldType::Int64, PersistedColumnBlock::Int64(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Int64).collect(),
                )
            }
            (FieldType::Float64, PersistedColumnBlock::Float64(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values
                        .values_bits
                        .into_iter()
                        .map(|bits| FieldValue::Float64(f64::from_bits(bits)))
                        .collect(),
                )
            }
            (FieldType::String, PersistedColumnBlock::String(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::String).collect(),
                )
            }
            (FieldType::Bytes, PersistedColumnBlock::Bytes(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Bytes).collect(),
                )
            }
            (FieldType::Bool, PersistedColumnBlock::Bool(values)) => {
                Self::decode_nullable_column_values(
                    location,
                    column,
                    row_count,
                    &values.present_bitmap,
                    values.values.into_iter().map(FieldValue::Bool).collect(),
                )
            }
            _ => Err(StorageError::corruption(format!(
                "columnar SSTable {location} field {} type metadata does not match its block",
                column.field_id.get()
            ))),
        }
    }

    fn decode_nullable_column_values(
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

    fn summarize_sstable_rows(
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

    async fn flush_immutable(
        &self,
        local_root: &str,
        immutable: &ImmutableMemtable,
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
                    self.write_columnar_sstable(&path, 0, local_id, &stored, rows)
                        .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    async fn flush_immutable_remote(
        &self,
        config: &S3PrimaryStorageConfig,
        immutable: &ImmutableMemtable,
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
                    self.write_columnar_sstable_remote(&object_key, 0, local_id, &stored, rows)
                        .await
                }
            }
            .map_err(FlushError::Storage)?;
            outputs.push(output);
        }

        Ok(outputs)
    }

    fn encode_row_sstable(
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

    fn encode_json_block<T: Serialize>(label: &str, value: &T) -> Result<Vec<u8>, StorageError> {
        serde_json::to_vec(value)
            .map_err(|error| StorageError::corruption(format!("encode {label} failed: {error}")))
    }

    fn append_columnar_block(bytes: &mut Vec<u8>, block_bytes: &[u8]) -> ColumnarBlockLocation {
        let location = ColumnarBlockLocation {
            offset: bytes.len() as u64,
            length: block_bytes.len() as u64,
        };
        bytes.extend_from_slice(block_bytes);
        location
    }

    fn columnar_row_is_tombstone(row: &SstableRow) -> Result<bool, StorageError> {
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

    fn columnar_field_value_for_row(
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

    fn encode_columnar_field_block(
        field: &FieldDefinition,
        rows: &[SstableRow],
    ) -> Result<Vec<u8>, StorageError> {
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
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Int64(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
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
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Float64(PersistedFloat64Column {
                        present_bitmap,
                        values_bits,
                    }),
                )
            }
            FieldType::String => {
                let mut present_bitmap = Vec::with_capacity(rows.len());
                let mut values = Vec::new();
                for row in rows {
                    match Self::columnar_field_value_for_row(field, row)? {
                        Some(FieldValue::String(value)) => {
                            present_bitmap.push(true);
                            values.push(value);
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
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::String(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
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
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Bytes(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
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
                Self::encode_json_block(
                    &format!("columnar field {} block", field.name),
                    &PersistedColumnBlock::Bool(PersistedNullableColumn {
                        present_bitmap,
                        values,
                    }),
                )
            }
        }
    }

    fn encode_columnar_sstable(
        table_id: TableId,
        level: u32,
        local_id: String,
        schema: &SchemaDefinition,
        rows: Vec<SstableRow>,
        bloom_filter_bits_per_key: Option<u32>,
    ) -> Result<(ResidentRowSstable, Vec<u8>), StorageError> {
        schema.validate()?;

        let mut meta = Self::summarize_sstable_rows(table_id, level, &local_id, &rows)?;
        meta.schema_version = Some(schema.version);
        let user_key_bloom_filter = UserKeyBloomFilter::build(&rows, bloom_filter_bits_per_key);

        let mut bytes = Vec::from(&COLUMNAR_SSTABLE_MAGIC[..]);
        let key_index = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar key index",
                &rows.iter().map(|row| row.key.clone()).collect::<Vec<_>>(),
            )?,
        );
        let sequence_column = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar sequence column",
                &rows.iter().map(|row| row.sequence).collect::<Vec<_>>(),
            )?,
        );

        let mut tombstones = Vec::with_capacity(rows.len());
        for row in &rows {
            tombstones.push(Self::columnar_row_is_tombstone(row)?);
        }
        let tombstone_bitmap = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block("columnar tombstone bitmap", &tombstones)?,
        );
        let row_kind_column = Self::append_columnar_block(
            &mut bytes,
            &Self::encode_json_block(
                "columnar row-kind column",
                &rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
            )?,
        );

        let mut columns = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let block = Self::append_columnar_block(
                &mut bytes,
                &Self::encode_columnar_field_block(field, &rows)?,
            );
            columns.push(PersistedColumnarColumnFooter {
                field_id: field.id,
                field_type: field.field_type,
                encoding: ColumnEncoding::Plain,
                compression: ColumnCompression::None,
                block,
            });
        }

        let data_checksum = checksum32(&bytes[COLUMNAR_SSTABLE_MAGIC.len()..]);
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
            user_key_bloom_filter: user_key_bloom_filter.clone(),
        };
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

    async fn write_row_sstable(
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

    async fn write_columnar_sstable(
        &self,
        path: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
        )?;

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
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::local_file(path.to_string()),
        });
        Ok(resident)
    }

    async fn write_row_sstable_remote(
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

    async fn write_columnar_sstable_remote(
        &self,
        object_key: &str,
        level: u32,
        local_id: String,
        stored: &StoredTable,
        rows: Vec<SstableRow>,
    ) -> Result<ResidentRowSstable, StorageError> {
        let schema = stored.config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                stored.config.name
            ))
        })?;
        let (mut resident, bytes) = Self::encode_columnar_sstable(
            stored.id,
            level,
            local_id,
            schema,
            rows,
            stored.config.bloom_filter_bits_per_key,
        )?;
        self.inner
            .dependencies
            .object_store
            .put(object_key, &bytes)
            .await?;
        resident.meta.remote_key = Some(object_key.to_string());
        resident.columnar = Some(ResidentColumnarSstable {
            source: StorageSource::remote_object(object_key.to_string()),
        });
        Ok(resident)
    }

    fn encode_manifest_payload_from_sstables(
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        sstables: &[PersistedManifestSstable],
    ) -> Result<Vec<u8>, StorageError> {
        let body = PersistedManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            generation,
            last_flushed_sequence,
            sstables: sstables.to_vec(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode manifest body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        serde_json::to_vec_pretty(&PersistedManifestFile { body, checksum }).map_err(|error| {
            StorageError::corruption(format!("encode manifest file failed: {error}"))
        })
    }

    fn encode_remote_manifest_payload(
        generation: ManifestId,
        last_flushed_sequence: SequenceNumber,
        sstables: &[PersistedManifestSstable],
        durable_commit_log_segments: &[DurableRemoteCommitLogSegment],
    ) -> Result<Vec<u8>, StorageError> {
        let body = PersistedRemoteManifestBody {
            format_version: REMOTE_MANIFEST_FORMAT_VERSION,
            generation,
            last_flushed_sequence,
            sstables: sstables.to_vec(),
            commit_log_segments: durable_commit_log_segments.to_vec(),
        };
        let encoded_body = serde_json::to_vec(&body).map_err(|error| {
            StorageError::corruption(format!("encode remote manifest body failed: {error}"))
        })?;
        let checksum = checksum32(&encoded_body);
        serde_json::to_vec_pretty(&PersistedRemoteManifestFile { body, checksum }).map_err(
            |error| {
                StorageError::corruption(format!("encode remote manifest file failed: {error}"))
            },
        )
    }

    async fn install_manifest(
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

    async fn install_remote_manifest(
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

    fn tiered_backup_layout(&self) -> Option<ObjectKeyLayout> {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => Some(Self::tiered_object_layout(config)),
            StorageConfig::S3Primary(_) => None,
        }
    }

    async fn write_remote_backup_object(
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

    async fn note_backup_object_birth(
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
        let Ok(payload) = serde_json::to_vec_pretty(&record) else {
            return;
        };
        let _ = dependencies.object_store.put(&metadata_key, &payload).await;
    }

    async fn read_backup_object_birth(
        dependencies: &DbDependencies,
        layout: &ObjectKeyLayout,
        object_key: &str,
    ) -> Option<BackupObjectBirthRecord> {
        let metadata_key = layout.backup_gc_metadata(object_key);
        let bytes = read_optional_remote_object(dependencies, &metadata_key)
            .await
            .ok()
            .flatten()?;
        let record: BackupObjectBirthRecord = serde_json::from_slice(&bytes).ok()?;
        (record.format_version == BACKUP_GC_METADATA_FORMAT_VERSION
            && record.object_key == object_key)
            .then_some(record)
    }

    async fn collect_tiered_commit_log_snapshots(
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

    async fn sync_tiered_backup_catalog(&self) -> Result<(), StorageError> {
        let Some(layout) = self.tiered_backup_layout() else {
            return Ok(());
        };
        let payload = Self::encode_catalog(&self.tables_read().clone())?;
        let _backup_guard = self.inner.backup_lock.lock().await;
        self.write_remote_backup_object(&layout, &layout.backup_catalog(), &payload, false)
            .await
    }

    async fn sync_tiered_commit_log_tail(&self) -> Result<(), StorageError> {
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

    async fn sync_tiered_backup_manifest(
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

    async fn run_tiered_backup_gc_locked(
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
}
