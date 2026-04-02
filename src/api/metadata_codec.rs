use super::*;

type FbMetadataEntryVector<'fbb> = flatbuffers::WIPOffset<
    flatbuffers::Vector<'fbb, flatbuffers::ForwardsUOffset<metadata_fb::MetadataEntry<'fbb>>>,
>;

const FB_TABLE_FORMAT_ROW: u8 = 1;
const FB_TABLE_FORMAT_COLUMNAR: u8 = 2;

const FB_COMPACTION_STRATEGY_LEVELED: u8 = 1;
const FB_COMPACTION_STRATEGY_TIERED: u8 = 2;
const FB_COMPACTION_STRATEGY_FIFO: u8 = 3;

const FB_FIELD_TYPE_INT64: u8 = 1;
const FB_FIELD_TYPE_FLOAT64: u8 = 2;
const FB_FIELD_TYPE_STRING: u8 = 3;
const FB_FIELD_TYPE_BYTES: u8 = 4;
const FB_FIELD_TYPE_BOOL: u8 = 5;

const FB_FIELD_VALUE_NULL: u8 = 1;
const FB_FIELD_VALUE_INT64: u8 = 2;
const FB_FIELD_VALUE_FLOAT64: u8 = 3;
const FB_FIELD_VALUE_STRING: u8 = 4;
const FB_FIELD_VALUE_BYTES: u8 = 5;
const FB_FIELD_VALUE_BOOL: u8 = 6;

impl Db {
    pub(super) fn encode_table_format_flatbuffer(format: TableFormat) -> u8 {
        match format {
            TableFormat::Row => FB_TABLE_FORMAT_ROW,
            TableFormat::Columnar => FB_TABLE_FORMAT_COLUMNAR,
        }
    }

    pub(super) fn decode_table_format_flatbuffer(tag: u8) -> Result<TableFormat, StorageError> {
        match tag {
            FB_TABLE_FORMAT_ROW => Ok(TableFormat::Row),
            FB_TABLE_FORMAT_COLUMNAR => Ok(TableFormat::Columnar),
            _ => Err(StorageError::corruption(format!(
                "unknown table format tag {tag}"
            ))),
        }
    }

    pub(super) fn encode_compaction_strategy_flatbuffer(strategy: CompactionStrategy) -> u8 {
        match strategy {
            CompactionStrategy::Leveled => FB_COMPACTION_STRATEGY_LEVELED,
            CompactionStrategy::Tiered => FB_COMPACTION_STRATEGY_TIERED,
            CompactionStrategy::Fifo => FB_COMPACTION_STRATEGY_FIFO,
        }
    }

    pub(super) fn decode_compaction_strategy_flatbuffer(
        tag: u8,
    ) -> Result<CompactionStrategy, StorageError> {
        match tag {
            FB_COMPACTION_STRATEGY_LEVELED => Ok(CompactionStrategy::Leveled),
            FB_COMPACTION_STRATEGY_TIERED => Ok(CompactionStrategy::Tiered),
            FB_COMPACTION_STRATEGY_FIFO => Ok(CompactionStrategy::Fifo),
            _ => Err(StorageError::corruption(format!(
                "unknown compaction strategy tag {tag}"
            ))),
        }
    }

    pub(super) fn encode_field_type_flatbuffer(field_type: FieldType) -> u8 {
        match field_type {
            FieldType::Int64 => FB_FIELD_TYPE_INT64,
            FieldType::Float64 => FB_FIELD_TYPE_FLOAT64,
            FieldType::String => FB_FIELD_TYPE_STRING,
            FieldType::Bytes => FB_FIELD_TYPE_BYTES,
            FieldType::Bool => FB_FIELD_TYPE_BOOL,
        }
    }

    pub(super) fn decode_field_type_flatbuffer(tag: u8) -> Result<FieldType, StorageError> {
        match tag {
            FB_FIELD_TYPE_INT64 => Ok(FieldType::Int64),
            FB_FIELD_TYPE_FLOAT64 => Ok(FieldType::Float64),
            FB_FIELD_TYPE_STRING => Ok(FieldType::String),
            FB_FIELD_TYPE_BYTES => Ok(FieldType::Bytes),
            FB_FIELD_TYPE_BOOL => Ok(FieldType::Bool),
            _ => Err(StorageError::corruption(format!(
                "unknown field type tag {tag}"
            ))),
        }
    }

    pub(super) fn encode_field_value_flatbuffer<'fbb>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'fbb>,
        value: &FieldValue,
    ) -> Result<flatbuffers::WIPOffset<metadata_fb::FieldValue<'fbb>>, StorageError> {
        let offset = match value {
            FieldValue::Null => {
                metadata_fb::create_field_value(fbb, FB_FIELD_VALUE_NULL, 0, 0.0, None, None, false)
            }
            FieldValue::Int64(value) => metadata_fb::create_field_value(
                fbb,
                FB_FIELD_VALUE_INT64,
                *value,
                0.0,
                None,
                None,
                false,
            ),
            FieldValue::Float64(value) => metadata_fb::create_field_value(
                fbb,
                FB_FIELD_VALUE_FLOAT64,
                0,
                *value,
                None,
                None,
                false,
            ),
            FieldValue::String(value) => {
                let string_value = fbb.create_string(value);
                metadata_fb::create_field_value(
                    fbb,
                    FB_FIELD_VALUE_STRING,
                    0,
                    0.0,
                    Some(string_value),
                    None,
                    false,
                )
            }
            FieldValue::Bytes(value) => {
                let bytes_value = fbb.create_vector(value);
                metadata_fb::create_field_value(
                    fbb,
                    FB_FIELD_VALUE_BYTES,
                    0,
                    0.0,
                    None,
                    Some(bytes_value),
                    false,
                )
            }
            FieldValue::Bool(value) => metadata_fb::create_field_value(
                fbb,
                FB_FIELD_VALUE_BOOL,
                0,
                0.0,
                None,
                None,
                *value,
            ),
        };
        Ok(offset)
    }

    pub(super) fn decode_field_value_flatbuffer(
        value: metadata_fb::FieldValue<'_>,
    ) -> Result<FieldValue, StorageError> {
        match value.kind() {
            FB_FIELD_VALUE_NULL => Ok(FieldValue::Null),
            FB_FIELD_VALUE_INT64 => Ok(FieldValue::Int64(value.i64_value())),
            FB_FIELD_VALUE_FLOAT64 => Ok(FieldValue::Float64(value.f64_value())),
            FB_FIELD_VALUE_STRING => Ok(FieldValue::String(
                value
                    .string_value()
                    .ok_or_else(|| {
                        StorageError::corruption("flatbuffer field value is missing string payload")
                    })?
                    .to_string(),
            )),
            FB_FIELD_VALUE_BYTES => Ok(FieldValue::Bytes(
                value
                    .bytes_value()
                    .ok_or_else(|| {
                        StorageError::corruption("flatbuffer field value is missing bytes payload")
                    })?
                    .bytes()
                    .to_vec(),
            )),
            FB_FIELD_VALUE_BOOL => Ok(FieldValue::Bool(value.bool_value())),
            other => Err(StorageError::corruption(format!(
                "unknown field value tag {other}"
            ))),
        }
    }

    pub(super) fn encode_schema_flatbuffer<'fbb>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'fbb>,
        schema: &SchemaDefinition,
    ) -> Result<flatbuffers::WIPOffset<metadata_fb::SchemaDefinition<'fbb>>, StorageError> {
        let mut field_offsets = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let name = fbb.create_string(&field.name);
            let default_value = match &field.default {
                Some(value) => Some(Self::encode_field_value_flatbuffer(fbb, value)?),
                None => None,
            };
            field_offsets.push(metadata_fb::create_field_definition(
                fbb,
                field.id.get(),
                name,
                Self::encode_field_type_flatbuffer(field.field_type),
                field.nullable,
                default_value,
            ));
        }
        let fields = fbb.create_vector(&field_offsets);
        Ok(metadata_fb::create_schema_definition(
            fbb,
            schema.version,
            fields,
        ))
    }

    pub(super) fn decode_schema_flatbuffer(
        schema: metadata_fb::SchemaDefinition<'_>,
    ) -> Result<SchemaDefinition, StorageError> {
        let schema_fields = schema.fields();
        let mut fields = Vec::with_capacity(schema_fields.len());
        for field in schema_fields {
            fields.push(FieldDefinition {
                id: FieldId::new(field.id()),
                name: field.name().to_string(),
                field_type: Self::decode_field_type_flatbuffer(field.field_type())?,
                nullable: field.nullable(),
                default: match field.default_value() {
                    Some(default) => Some(Self::decode_field_value_flatbuffer(default)?),
                    None => None,
                },
            });
        }
        Ok(SchemaDefinition {
            version: schema.version(),
            fields,
        })
    }

    pub(super) fn encode_metadata_entries_flatbuffer<'fbb>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'fbb>,
        metadata: &TableMetadata,
    ) -> Result<FbMetadataEntryVector<'fbb>, StorageError> {
        let mut entries = Vec::with_capacity(metadata.len());
        for (key, value) in metadata {
            let key_offset = fbb.create_string(key);
            let value_json = serde_json::to_string(value).map_err(|error| {
                StorageError::corruption(format!(
                    "encode table metadata entry {key} failed: {error}"
                ))
            })?;
            let value_offset = fbb.create_string(&value_json);
            entries.push(metadata_fb::create_metadata_entry(
                fbb,
                key_offset,
                value_offset,
            ));
        }
        Ok(fbb.create_vector(&entries))
    }

    pub(super) fn decode_metadata_entries_flatbuffer(
        entries: flatbuffers::Vector<
            '_,
            flatbuffers::ForwardsUOffset<metadata_fb::MetadataEntry<'_>>,
        >,
    ) -> Result<TableMetadata, StorageError> {
        let mut metadata = BTreeMap::new();
        for entry in entries {
            let value = serde_json::from_str(entry.value_json()).map_err(|error| {
                StorageError::corruption(format!(
                    "decode table metadata entry {} failed: {error}",
                    entry.key()
                ))
            })?;
            metadata.insert(entry.key().to_string(), value);
        }
        Ok(metadata)
    }

    pub(super) fn encode_catalog_entry_flatbuffer<'fbb>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'fbb>,
        entry: &PersistedCatalogEntry,
    ) -> Result<flatbuffers::WIPOffset<metadata_fb::CatalogEntry<'fbb>>, StorageError> {
        let persisted_metadata = crate::sharding::encode_persisted_table_metadata(
            &entry.config.metadata,
            &entry.config.sharding,
        )?;
        let metadata_entries = Self::encode_metadata_entries_flatbuffer(fbb, &persisted_metadata)?;
        let schema = match &entry.config.schema {
            Some(schema) => Some(Self::encode_schema_flatbuffer(fbb, schema)?),
            None => None,
        };
        let name = fbb.create_string(&entry.config.name);
        let config = metadata_fb::create_table_config(
            fbb,
            metadata_fb::TableConfigCreateArgs {
                name,
                format: Self::encode_table_format_flatbuffer(entry.config.format),
                has_max_merge_operand_chain_length: entry
                    .config
                    .max_merge_operand_chain_length
                    .is_some(),
                max_merge_operand_chain_length: entry
                    .config
                    .max_merge_operand_chain_length
                    .unwrap_or_default(),
                has_bloom_filter_bits_per_key: entry.config.bloom_filter_bits_per_key.is_some(),
                bloom_filter_bits_per_key: entry
                    .config
                    .bloom_filter_bits_per_key
                    .unwrap_or_default(),
                has_history_retention_sequences: entry.config.history_retention_sequences.is_some(),
                history_retention_sequences: entry
                    .config
                    .history_retention_sequences
                    .unwrap_or_default(),
                compaction_strategy: Self::encode_compaction_strategy_flatbuffer(
                    entry.config.compaction_strategy,
                ),
                schema,
                metadata_entries,
            },
        );
        Ok(metadata_fb::create_catalog_entry(
            fbb,
            entry.id.get(),
            config,
        ))
    }

    pub(super) fn decode_catalog_entry_flatbuffer(
        entry: metadata_fb::CatalogEntry<'_>,
    ) -> Result<PersistedCatalogEntry, StorageError> {
        let config = entry.config();
        let encoded_metadata =
            Self::decode_metadata_entries_flatbuffer(config.metadata_entries().ok_or_else(
                || StorageError::corruption("catalog entry is missing metadata entries"),
            )?)?;
        let (metadata, sharding) =
            crate::sharding::decode_persisted_table_metadata(encoded_metadata)?;
        Ok(PersistedCatalogEntry {
            id: TableId::new(entry.id()),
            config: PersistedTableConfig {
                name: config.name().to_string(),
                format: Self::decode_table_format_flatbuffer(config.format())?,
                max_merge_operand_chain_length: config
                    .has_max_merge_operand_chain_length()
                    .then_some(config.max_merge_operand_chain_length()),
                bloom_filter_bits_per_key: config
                    .has_bloom_filter_bits_per_key()
                    .then_some(config.bloom_filter_bits_per_key()),
                history_retention_sequences: config
                    .has_history_retention_sequences()
                    .then_some(config.history_retention_sequences()),
                compaction_strategy: Self::decode_compaction_strategy_flatbuffer(
                    config.compaction_strategy(),
                )?,
                schema: match config.schema() {
                    Some(schema) => Some(Self::decode_schema_flatbuffer(schema)?),
                    None => None,
                },
                sharding,
                metadata,
            },
        })
    }

    pub(super) fn encode_manifest_sstable_flatbuffer<'fbb>(
        fbb: &mut flatbuffers::FlatBufferBuilder<'fbb>,
        sstable: &PersistedManifestSstable,
    ) -> Result<flatbuffers::WIPOffset<metadata_fb::ManifestSstable<'fbb>>, StorageError> {
        let local_id = fbb.create_string(&sstable.local_id);
        let file_path =
            (!sstable.file_path.is_empty()).then(|| fbb.create_string(&sstable.file_path));
        let remote_key = sstable
            .remote_key
            .as_ref()
            .map(|key| fbb.create_string(key));
        let min_key = fbb.create_vector(&sstable.min_key);
        let max_key = fbb.create_vector(&sstable.max_key);
        let shard_ownership_json = match &sstable.shard_ownership {
            Some(shard_ownership) => Some(fbb.create_vector(
                &serde_json::to_vec(shard_ownership).map_err(|error| {
                    StorageError::corruption(format!(
                        "encode manifest shard ownership failed: {error}"
                    ))
                })?,
            )),
            None => None,
        };
        Ok(metadata_fb::create_manifest_sstable(
            fbb,
            metadata_fb::ManifestSstableCreateArgs {
                table_id: sstable.table_id.get(),
                level: sstable.level,
                local_id,
                file_path,
                remote_key,
                length: sstable.length,
                checksum: sstable.checksum,
                data_checksum: sstable.data_checksum,
                min_key,
                max_key,
                min_sequence: sstable.min_sequence.get(),
                max_sequence: sstable.max_sequence.get(),
                has_schema_version: sstable.schema_version.is_some(),
                schema_version: sstable.schema_version.unwrap_or_default(),
                shard_ownership_json,
            },
        ))
    }

    pub(super) fn decode_manifest_sstable_flatbuffer(
        sstable: metadata_fb::ManifestSstable<'_>,
    ) -> Result<PersistedManifestSstable, StorageError> {
        Ok(PersistedManifestSstable {
            table_id: TableId::new(sstable.table_id()),
            level: sstable.level(),
            local_id: sstable.local_id().to_string(),
            file_path: sstable.file_path().unwrap_or_default().to_string(),
            remote_key: sstable.remote_key().map(str::to_string),
            length: sstable.length(),
            checksum: sstable.checksum(),
            data_checksum: sstable.data_checksum(),
            min_key: sstable.min_key().bytes().to_vec(),
            max_key: sstable.max_key().bytes().to_vec(),
            min_sequence: SequenceNumber::new(sstable.min_sequence()),
            max_sequence: SequenceNumber::new(sstable.max_sequence()),
            schema_version: sstable
                .has_schema_version()
                .then_some(sstable.schema_version()),
            shard_ownership: match sstable.shard_ownership_json() {
                Some(bytes) => Some(serde_json::from_slice(bytes.bytes()).map_err(|error| {
                    StorageError::corruption(format!(
                        "decode manifest shard ownership failed: {error}"
                    ))
                })?),
                None => None,
            },
        })
    }

    pub(super) fn encode_catalog(
        tables: &BTreeMap<String, StoredTable>,
    ) -> Result<Vec<u8>, StorageError> {
        let persisted = PersistedCatalog::from_tables(tables);
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let mut entries = Vec::with_capacity(persisted.tables.len());
        for entry in &persisted.tables {
            entries.push(Self::encode_catalog_entry_flatbuffer(&mut fbb, entry)?);
        }
        let entries = fbb.create_vector(&entries);
        let root = metadata_fb::create_catalog(&mut fbb, persisted.format_version, entries);
        fbb.finish(root, Some(metadata_fb::CATALOG_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn decode_catalog(bytes: &[u8]) -> Result<PersistedCatalog, StorageError> {
        let catalog = metadata_fb::root_with_identifier::<metadata_fb::Catalog<'_>>(
            bytes,
            metadata_fb::CATALOG_IDENTIFIER,
            "catalog",
        )
        .map_err(|error| StorageError::corruption(format!("decode catalog failed: {error}")))?;

        if catalog.format_version() != CATALOG_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported catalog version {}",
                catalog.format_version()
            )));
        }

        let mut tables = Vec::with_capacity(catalog.tables().len());
        for entry in catalog.tables() {
            tables.push(Self::decode_catalog_entry_flatbuffer(entry)?);
        }
        Ok(PersistedCatalog {
            format_version: catalog.format_version(),
            tables,
        })
    }

    pub(super) fn encode_manifest_payload_from_sstables(
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
        let body_bytes = Self::encode_manifest_body_flatbuffer(&body)?;
        let checksum = checksum32(&body_bytes);
        Self::encode_manifest_file_flatbuffer(
            &PersistedManifestFile { body, checksum },
            &body_bytes,
        )
    }

    pub(super) fn encode_manifest_body_flatbuffer(
        body: &PersistedManifestBody,
    ) -> Result<Vec<u8>, StorageError> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let mut sstable_offsets = Vec::with_capacity(body.sstables.len());
        for sstable in &body.sstables {
            sstable_offsets.push(Self::encode_manifest_sstable_flatbuffer(&mut fbb, sstable)?);
        }
        let sstables = fbb.create_vector(&sstable_offsets);
        let root = metadata_fb::create_manifest_body(
            &mut fbb,
            body.format_version,
            body.generation.get(),
            body.last_flushed_sequence.get(),
            sstables,
        );
        fbb.finish(root, Some(metadata_fb::MANIFEST_BODY_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn encode_manifest_file_flatbuffer(
        file: &PersistedManifestFile,
        body_bytes: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let body_bytes = fbb.create_vector(body_bytes);
        let root = metadata_fb::create_manifest_file(&mut fbb, body_bytes, file.checksum);
        fbb.finish(root, Some(metadata_fb::MANIFEST_FILE_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn decode_manifest_file_flatbuffer(
        bytes: &[u8],
    ) -> Result<PersistedManifestFile, StorageError> {
        let file = metadata_fb::root_with_identifier::<metadata_fb::ManifestFile<'_>>(
            bytes,
            metadata_fb::MANIFEST_FILE_IDENTIFIER,
            "manifest file",
        )
        .map_err(|error| StorageError::corruption(format!("decode manifest failed: {error}")))?;

        let body_bytes = file.body_bytes().bytes();
        let body = metadata_fb::root_with_identifier::<metadata_fb::ManifestBody<'_>>(
            body_bytes,
            metadata_fb::MANIFEST_BODY_IDENTIFIER,
            "manifest body",
        )
        .map_err(|error| {
            StorageError::corruption(format!("decode manifest body failed: {error}"))
        })?;

        let mut sstables = Vec::with_capacity(body.sstables().len());
        for sstable in body.sstables() {
            sstables.push(Self::decode_manifest_sstable_flatbuffer(sstable)?);
        }
        Ok(PersistedManifestFile {
            body: PersistedManifestBody {
                format_version: body.format_version(),
                generation: ManifestId::new(body.generation()),
                last_flushed_sequence: SequenceNumber::new(body.last_flushed_sequence()),
                sstables,
            },
            checksum: file.checksum(),
        })
    }

    pub(super) fn encode_remote_manifest_payload(
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
        let body_bytes = Self::encode_remote_manifest_body_flatbuffer(&body)?;
        let checksum = checksum32(&body_bytes);
        Self::encode_remote_manifest_file_flatbuffer(
            &PersistedRemoteManifestFile { body, checksum },
            &body_bytes,
        )
    }

    pub(super) fn encode_remote_manifest_body_flatbuffer(
        body: &PersistedRemoteManifestBody,
    ) -> Result<Vec<u8>, StorageError> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let mut sstable_offsets = Vec::with_capacity(body.sstables.len());
        for sstable in &body.sstables {
            sstable_offsets.push(Self::encode_manifest_sstable_flatbuffer(&mut fbb, sstable)?);
        }
        let mut segment_offsets = Vec::with_capacity(body.commit_log_segments.len());
        for segment in &body.commit_log_segments {
            let object_key = fbb.create_string(&segment.object_key);
            let footer_bytes = segment.footer.encode()?;
            let footer_bytes = fbb.create_vector(&footer_bytes);
            segment_offsets.push(metadata_fb::create_remote_commit_log_segment(
                &mut fbb,
                object_key,
                footer_bytes,
            ));
        }
        let sstables = fbb.create_vector(&sstable_offsets);
        let commit_log_segments = fbb.create_vector(&segment_offsets);
        let root = metadata_fb::create_remote_manifest_body(
            &mut fbb,
            body.format_version,
            body.generation.get(),
            body.last_flushed_sequence.get(),
            sstables,
            commit_log_segments,
        );
        fbb.finish(root, Some(metadata_fb::REMOTE_MANIFEST_BODY_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn encode_remote_manifest_file_flatbuffer(
        file: &PersistedRemoteManifestFile,
        body_bytes: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let body_bytes = fbb.create_vector(body_bytes);
        let root = metadata_fb::create_remote_manifest_file(&mut fbb, body_bytes, file.checksum);
        fbb.finish(root, Some(metadata_fb::REMOTE_MANIFEST_FILE_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn decode_remote_manifest_file_flatbuffer(
        bytes: &[u8],
    ) -> Result<PersistedRemoteManifestFile, StorageError> {
        let file = metadata_fb::root_with_identifier::<metadata_fb::RemoteManifestFile<'_>>(
            bytes,
            metadata_fb::REMOTE_MANIFEST_FILE_IDENTIFIER,
            "remote manifest file",
        )
        .map_err(|error| {
            StorageError::corruption(format!("decode remote manifest failed: {error}"))
        })?;

        let body_bytes = file.body_bytes().bytes();
        let body = metadata_fb::root_with_identifier::<metadata_fb::RemoteManifestBody<'_>>(
            body_bytes,
            metadata_fb::REMOTE_MANIFEST_BODY_IDENTIFIER,
            "remote manifest body",
        )
        .map_err(|error| {
            StorageError::corruption(format!("decode remote manifest body failed: {error}"))
        })?;

        let mut sstables = Vec::with_capacity(body.sstables().len());
        for sstable in body.sstables() {
            sstables.push(Self::decode_manifest_sstable_flatbuffer(sstable)?);
        }

        let mut commit_log_segments = Vec::with_capacity(body.commit_log_segments().len());
        for segment in body.commit_log_segments() {
            let footer =
                SegmentFooter::decode(segment.footer_bytes().bytes()).map_err(|error| {
                    StorageError::corruption(format!(
                        "decode remote manifest commit-log footer failed: {error}"
                    ))
                })?;
            commit_log_segments.push(DurableRemoteCommitLogSegment {
                object_key: segment.object_key().to_string(),
                footer,
            });
        }

        Ok(PersistedRemoteManifestFile {
            body: PersistedRemoteManifestBody {
                format_version: body.format_version(),
                generation: ManifestId::new(body.generation()),
                last_flushed_sequence: SequenceNumber::new(body.last_flushed_sequence()),
                sstables,
                commit_log_segments,
            },
            checksum: file.checksum(),
        })
    }

    pub(super) fn encode_backup_object_birth_payload(
        record: &BackupObjectBirthRecord,
    ) -> Result<Vec<u8>, StorageError> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let object_key = fbb.create_string(&record.object_key);
        let root = metadata_fb::create_backup_gc_birth_record(
            &mut fbb,
            record.format_version,
            object_key,
            record.first_uploaded_at_millis,
        );
        fbb.finish(root, Some(metadata_fb::BACKUP_GC_IDENTIFIER));
        Ok(fbb.finished_data().to_vec())
    }

    pub(super) fn decode_backup_object_birth_payload(
        bytes: &[u8],
    ) -> Result<BackupObjectBirthRecord, StorageError> {
        let record = metadata_fb::root_with_identifier::<metadata_fb::BackupGcBirthRecord<'_>>(
            bytes,
            metadata_fb::BACKUP_GC_IDENTIFIER,
            "backup GC metadata",
        )
        .map_err(|error| {
            StorageError::corruption(format!("decode backup GC metadata failed: {error}"))
        })?;

        Ok(BackupObjectBirthRecord {
            format_version: record.format_version(),
            object_key: record.object_key().to_string(),
            first_uploaded_at_millis: record.first_uploaded_at_millis(),
        })
    }
}
