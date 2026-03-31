use super::*;

impl Db {
    pub async fn open(
        mut config: DbConfig,
        dependencies: DbDependencies,
    ) -> Result<Self, OpenError> {
        let db_name = db_name_from_storage(&config.storage);
        let db_instance = db_instance_from_storage(&config.storage);
        let storage_mode = storage_mode_name(&config.storage);
        let span = tracing::info_span!("terracedb.db.open");
        apply_db_span_attributes(&span, &db_name, &db_instance, storage_mode);

        async move {
            Self::validate_storage_config(&config.storage)?;
            Self::validate_hybrid_read_config(&config.hybrid_read)?;
            let scheduler = config
                .scheduler
                .clone()
                .unwrap_or_else(|| Arc::new(RoundRobinScheduler::default()));
            config.scheduler = Some(scheduler.clone());
            if let StorageConfig::Tiered(tiered) = &config.storage {
                Self::maybe_restore_tiered_from_backup(tiered, &dependencies).await?;
            }
            let columnar_read_context = Arc::new(
                Self::open_columnar_read_context(&config.storage, &dependencies)
                    .await
                    .map_err(OpenError::Storage)?,
            );
            let catalog_location = Self::catalog_location(&config.storage);
            let (tables, next_table_id) =
                Self::load_tables(&dependencies, &catalog_location).await?;
            let loaded_manifest =
                Self::load_local_manifest(&config.storage, &dependencies, &columnar_read_context)
                    .await?;
            let mut commit_runtime =
                Self::open_commit_runtime(&config.storage, &dependencies).await?;
            if let CommitLogBackend::Memory(log) = &mut commit_runtime.backend {
                log.durable_commit_log_segments =
                    loaded_manifest.durable_commit_log_segments.clone();
                log.next_segment_id = loaded_manifest
                    .durable_commit_log_segments
                    .iter()
                    .map(|segment| segment.footer.segment_id.get())
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1)
                    .max(1);
            }
            let recovered_commit_log = commit_runtime
                .recover_after(loaded_manifest.last_flushed_sequence)
                .await?;
            let recovered_sequence = recovered_commit_log
                .max_sequence
                .max(loaded_manifest.last_flushed_sequence);
            let next_sstable_id = loaded_manifest
                .live_sstables
                .iter()
                .filter_map(|sstable| Self::parse_sstable_local_id(&sstable.meta.local_id))
                .max()
                .unwrap_or(0)
                .saturating_add(1)
                .max(1);
            let memtables = recovered_commit_log.memtables;
            let sstables = SstableState {
                manifest_generation: loaded_manifest.generation,
                last_flushed_sequence: loaded_manifest.last_flushed_sequence,
                live: loaded_manifest.live_sstables,
            };
            let initial_table_watermarks =
                Self::initial_table_watermarks(&tables, &memtables, &sstables);

            let db = Self {
                inner: Arc::new(DbInner {
                    config,
                    scheduler,
                    dependencies,
                    columnar_read_context,
                    catalog_location,
                    catalog_write_lock: AsyncMutex::new(()),
                    commit_lock: AsyncMutex::new(()),
                    maintenance_lock: AsyncMutex::new(()),
                    backup_lock: AsyncMutex::new(()),
                    commit_runtime: AsyncMutex::new(commit_runtime),
                    commit_coordinator: Mutex::new(CommitCoordinator::default()),
                    commit_log_scans: Mutex::new(CommitLogScanRegistry::default()),
                    next_table_id: AtomicU32::new(next_table_id),
                    next_sequence: AtomicU64::new(recovered_sequence.get()),
                    current_sequence: AtomicU64::new(recovered_sequence.get()),
                    current_durable_sequence: AtomicU64::new(recovered_sequence.get()),
                    tables: RwLock::new(tables),
                    memtables: RwLock::new(memtables),
                    sstables: RwLock::new(sstables),
                    next_sstable_id: AtomicU64::new(next_sstable_id),
                    snapshot_tracker: Mutex::new(SnapshotTracker::default()),
                    next_snapshot_id: AtomicU64::new(0),
                    compaction_filter_stats: Mutex::new(BTreeMap::new()),
                    visible_watchers: Arc::new(WatermarkRegistry::new(
                        initial_table_watermarks.clone(),
                    )),
                    durable_watchers: Arc::new(WatermarkRegistry::new(initial_table_watermarks)),
                    work_deferrals: Mutex::new(BTreeMap::new()),
                }),
            };
            db.prune_commit_log(true)
                .await
                .map_err(OpenError::Storage)?;

            Ok(db)
        }
        .instrument(span.clone())
        .await
    }

    pub(super) fn validate_storage_config(storage: &StorageConfig) -> Result<(), OpenError> {
        match storage {
            StorageConfig::Tiered(TieredStorageConfig {
                ssd,
                s3,
                max_local_bytes,
                ..
            }) => {
                if ssd.path.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires an SSD path".to_string(),
                    ));
                }
                if s3.bucket.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires an S3 bucket".to_string(),
                    ));
                }
                if *max_local_bytes == 0 {
                    return Err(OpenError::InvalidConfig(
                        "tiered storage requires max_local_bytes > 0".to_string(),
                    ));
                }
            }
            StorageConfig::S3Primary(S3PrimaryStorageConfig {
                s3,
                mem_cache_size_bytes,
                ..
            }) => {
                if s3.bucket.is_empty() {
                    return Err(OpenError::InvalidConfig(
                        "s3-primary storage requires an S3 bucket".to_string(),
                    ));
                }
                if *mem_cache_size_bytes == 0 {
                    return Err(OpenError::InvalidConfig(
                        "s3-primary storage requires mem_cache_size_bytes > 0".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    pub(super) fn validate_table_config(config: &TableConfig) -> Result<(), CreateTableError> {
        if config.name.is_empty() {
            return Err(CreateTableError::InvalidConfig(
                "table name cannot be empty".to_string(),
            ));
        }
        if config.history_retention_sequences == Some(0) {
            return Err(CreateTableError::InvalidConfig(
                "history_retention_sequences must be greater than zero".to_string(),
            ));
        }
        if matches!(config.max_merge_operand_chain_length, Some(0)) {
            return Err(CreateTableError::InvalidConfig(
                "max_merge_operand_chain_length must be greater than zero".to_string(),
            ));
        }
        match config.format {
            TableFormat::Row => {
                if config.schema.is_some() {
                    return Err(CreateTableError::InvalidConfig(
                        "row tables do not accept a schema".to_string(),
                    ));
                }
            }
            TableFormat::Columnar => {
                let schema = config.schema.as_ref().ok_or_else(|| {
                    CreateTableError::InvalidConfig("columnar tables require a schema".to_string())
                })?;
                schema.validate().map_err(|error| {
                    CreateTableError::InvalidConfig(format!(
                        "invalid columnar schema: {}",
                        error.message()
                    ))
                })?;
            }
        }

        Ok(())
    }

    pub(super) fn same_persisted_table_config(left: &TableConfig, right: &TableConfig) -> bool {
        left.name == right.name
            && left.format == right.format
            && left.max_merge_operand_chain_length == right.max_merge_operand_chain_length
            && left.bloom_filter_bits_per_key == right.bloom_filter_bits_per_key
            && left.history_retention_sequences == right.history_retention_sequences
            && left.compaction_strategy == right.compaction_strategy
            && left.schema == right.schema
            && left.metadata == right.metadata
    }

    pub(super) fn normalize_value_for_table(
        stored: &StoredTable,
        value: &Value,
    ) -> Result<Value, StorageError> {
        match stored.config.format {
            TableFormat::Row => match value {
                Value::Bytes(_) => Ok(value.clone()),
                Value::Record(_) => Err(StorageError::unsupported(format!(
                    "row table {} only accepts byte values",
                    stored.config.name
                ))),
            },
            TableFormat::Columnar => {
                let schema = stored.config.schema.as_ref().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} is missing a schema",
                        stored.config.name
                    ))
                })?;
                match value {
                    Value::Record(record) => Ok(Value::Record(schema.normalize_record(record)?)),
                    Value::Bytes(_) => Err(StorageError::unsupported(format!(
                        "columnar table {} requires structured record values",
                        stored.config.name
                    ))),
                }
            }
        }
    }

    pub(super) fn validate_hybrid_read_config(
        config: &crate::HybridReadConfig,
    ) -> Result<(), OpenError> {
        config.validate().map_err(OpenError::InvalidConfig)
    }

    pub(super) fn normalize_merge_operand_for_table(
        stored: &StoredTable,
        value: &Value,
    ) -> Result<Value, StorageError> {
        match stored.config.format {
            TableFormat::Row => match value {
                Value::Bytes(_) => Ok(value.clone()),
                Value::Record(_) => Err(StorageError::unsupported(format!(
                    "row table {} only accepts byte values",
                    stored.config.name
                ))),
            },
            TableFormat::Columnar => {
                let schema = stored.config.schema.as_ref().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} is missing a schema",
                        stored.config.name
                    ))
                })?;
                match value {
                    Value::Record(record) => {
                        Ok(Value::Record(schema.normalize_merge_operand(record)?))
                    }
                    Value::Bytes(_) => Err(StorageError::unsupported(format!(
                        "columnar table {} requires structured record values",
                        stored.config.name
                    ))),
                }
            }
        }
    }

    pub(super) fn catalog_location(storage: &StorageConfig) -> CatalogLocation {
        match storage {
            StorageConfig::Tiered(config) => {
                let path = Self::join_fs_path(&config.ssd.path, LOCAL_CATALOG_RELATIVE_PATH);
                CatalogLocation::LocalFile {
                    temp_path: format!("{path}{LOCAL_CATALOG_TEMP_SUFFIX}"),
                    path,
                }
            }
            StorageConfig::S3Primary(config) => CatalogLocation::ObjectStore {
                key: Self::join_object_key(&config.s3.prefix, OBJECT_CATALOG_RELATIVE_KEY),
            },
        }
    }

    pub(super) async fn open_commit_runtime(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<CommitRuntime, OpenError> {
        let backend = match storage {
            StorageConfig::Tiered(config) => {
                let dir = Self::local_commit_log_dir(&config.ssd.path);
                CommitLogBackend::Local(Box::new(
                    SegmentManager::open(
                        dependencies.file_system.clone(),
                        dir,
                        SegmentOptions::default(),
                    )
                    .await?,
                ))
            }
            StorageConfig::S3Primary(_) => CommitLogBackend::Memory(MemoryCommitLog::new()),
        };

        Ok(CommitRuntime { backend })
    }

    pub(super) async fn load_tables(
        dependencies: &DbDependencies,
        catalog_location: &CatalogLocation,
    ) -> Result<(BTreeMap<String, StoredTable>, u32), OpenError> {
        let persisted = Self::load_catalog(dependencies, catalog_location).await?;
        let mut tables = BTreeMap::new();
        let mut max_table_id = 0_u32;

        for entry in persisted.tables {
            let stored = entry.into_stored();
            Self::validate_table_config(&stored.config).map_err(|error| match error {
                CreateTableError::InvalidConfig(message) => {
                    OpenError::Storage(StorageError::corruption(message))
                }
                CreateTableError::Storage(error) => OpenError::Storage(error),
                CreateTableError::AlreadyExists(_) | CreateTableError::Unimplemented(_) => {
                    OpenError::Storage(StorageError::corruption(
                        "catalog contains invalid table definition",
                    ))
                }
            })?;

            max_table_id = max_table_id.max(stored.id.get());
            if tables
                .insert(stored.config.name.clone(), stored.clone())
                .is_some()
            {
                return Err(OpenError::Storage(StorageError::corruption(
                    "catalog contains duplicate table names",
                )));
            }
        }

        let next_table_id = max_table_id.checked_add(1).unwrap_or(max_table_id).max(1);

        Ok((tables, next_table_id))
    }

    pub(super) async fn load_catalog(
        dependencies: &DbDependencies,
        catalog_location: &CatalogLocation,
    ) -> Result<PersistedCatalog, OpenError> {
        let bytes = match catalog_location {
            CatalogLocation::LocalFile { path, .. } => {
                match dependencies
                    .file_system
                    .open(
                        path,
                        OpenOptions {
                            create: false,
                            read: true,
                            write: false,
                            truncate: false,
                            append: false,
                        },
                    )
                    .await
                {
                    Ok(handle) => Some(read_all_file(dependencies, &handle).await?),
                    Err(error) if error.kind() == StorageErrorKind::NotFound => None,
                    Err(error) => return Err(OpenError::Storage(error)),
                }
            }
            CatalogLocation::ObjectStore { key } => {
                match dependencies.object_store.get(key).await {
                    Ok(bytes) => Some(bytes),
                    Err(error) if error.kind() == StorageErrorKind::NotFound => None,
                    Err(error) => return Err(OpenError::Storage(error)),
                }
            }
        };

        match bytes {
            Some(bytes) => Self::decode_catalog(&bytes).map_err(OpenError::Storage),
            None => Ok(PersistedCatalog::default()),
        }
    }

    pub(super) async fn persist_tables(
        &self,
        tables: &BTreeMap<String, StoredTable>,
    ) -> Result<(), CreateTableError> {
        let payload = Self::encode_catalog(tables)?;

        match &self.inner.catalog_location {
            CatalogLocation::LocalFile { path, temp_path } => {
                self.persist_catalog_file(path, temp_path, &payload).await?
            }
            CatalogLocation::ObjectStore { key } => {
                self.inner
                    .dependencies
                    .object_store
                    .put(key, &payload)
                    .await?
            }
        }

        Ok(())
    }

    pub(super) async fn persist_catalog_file(
        &self,
        path: &str,
        temp_path: &str,
        payload: &[u8],
    ) -> Result<(), StorageError> {
        let temp_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                temp_path,
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
            .write_at(&temp_handle, 0, payload)
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&temp_handle)
            .await?;
        self.inner
            .dependencies
            .file_system
            .rename(temp_path, path)
            .await?;

        let catalog_handle = self
            .inner
            .dependencies
            .file_system
            .open(
                path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await?;
        self.inner
            .dependencies
            .file_system
            .sync(&catalog_handle)
            .await?;

        Ok(())
    }

    pub(super) fn join_fs_path(root: &str, relative: &str) -> String {
        PathBuf::from(root)
            .join(relative)
            .to_string_lossy()
            .into_owned()
    }

    pub(super) fn join_object_key(prefix: &str, relative: &str) -> String {
        let prefix = prefix.trim_matches('/');
        if prefix.is_empty() {
            relative.to_string()
        } else {
            format!("{prefix}/{relative}")
        }
    }

    pub(super) fn local_storage_root_for(storage: &StorageConfig) -> Option<&str> {
        match storage {
            StorageConfig::Tiered(config) => Some(config.ssd.path.as_str()),
            StorageConfig::S3Primary(_) => None,
        }
    }

    pub(super) fn local_storage_root(&self) -> Option<&str> {
        Self::local_storage_root_for(&self.inner.config.storage)
    }

    pub(super) fn local_current_path(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_CURRENT_RELATIVE_PATH)
    }

    pub(super) fn local_manifest_dir(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_MANIFEST_DIR_RELATIVE_PATH)
    }

    pub(super) fn local_commit_log_dir(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_COMMIT_LOG_RELATIVE_DIR)
    }

    pub(super) fn manifest_filename(generation: ManifestId) -> String {
        format!("MANIFEST-{:06}", generation.get())
    }

    pub(super) fn local_manifest_path(root: &str, generation: ManifestId) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_MANIFEST_DIR_RELATIVE_PATH}/{}",
                Self::manifest_filename(generation)
            ),
        )
    }

    pub(super) fn local_sstable_path(root: &str, table_id: TableId, local_id: &str) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_SSTABLE_RELATIVE_DIR}/table-{:06}/{LOCAL_SSTABLE_SHARD_DIR}/{local_id}.sst",
                table_id.get()
            ),
        )
    }

    pub(super) fn parse_manifest_generation(path: &str) -> Option<ManifestId> {
        let path_buf = PathBuf::from(path);
        let file_name = path_buf.file_name()?.to_str()?;
        let suffix = file_name.strip_prefix("MANIFEST-")?;
        if suffix.is_empty() || suffix.contains('.') {
            return None;
        }
        suffix.parse::<u64>().ok().map(ManifestId::new)
    }

    pub(super) fn parse_sstable_local_id(local_id: &str) -> Option<u64> {
        local_id.strip_prefix("SST-")?.parse::<u64>().ok()
    }

    pub(super) fn parse_segment_id(path: &str) -> Option<SegmentId> {
        let path_buf = PathBuf::from(path);
        let file_name = path_buf.file_name()?.to_str()?;
        let suffix = file_name.strip_prefix("SEG-")?;
        if suffix.is_empty() || suffix.contains('.') {
            return None;
        }
        suffix.parse::<u64>().ok().map(SegmentId::new)
    }

    pub(super) fn local_commit_log_segment_path(root: &str, segment_id: SegmentId) -> String {
        Self::join_fs_path(
            root,
            &format!(
                "{LOCAL_COMMIT_LOG_RELATIVE_DIR}/SEG-{:06}",
                segment_id.get()
            ),
        )
    }

    pub(super) fn backup_restore_marker_path(root: &str) -> String {
        Self::join_fs_path(root, LOCAL_BACKUP_RESTORE_MARKER_RELATIVE_PATH)
    }

    pub(super) fn object_key_layout(location: &S3Location) -> ObjectKeyLayout {
        ObjectKeyLayout::new(location)
    }

    pub(super) fn storage_cache_namespace(location: &S3Location) -> String {
        let mut encoded = String::new();
        for byte in format!("{}/{}", location.bucket, location.prefix).bytes() {
            encoded.push_str(&format!("{byte:02x}"));
        }
        encoded
    }

    pub(super) fn s3_primary_remote_cache_root(config: &S3PrimaryStorageConfig) -> String {
        std::env::temp_dir()
            .join("terracedb-object-cache")
            .join(Self::storage_cache_namespace(&config.s3))
            .to_string_lossy()
            .into_owned()
    }

    pub(super) async fn open_columnar_read_context(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<ColumnarReadContext, StorageError> {
        let remote_cache_root = match storage {
            StorageConfig::Tiered(config) => Some(Self::join_fs_path(
                &config.ssd.path,
                LOCAL_REMOTE_CACHE_RELATIVE_DIR,
            )),
            StorageConfig::S3Primary(config) => Some(Self::s3_primary_remote_cache_root(config)),
        };
        let remote_cache = match remote_cache_root {
            Some(root) => Some(Arc::new(
                RemoteCache::open(dependencies.file_system.clone(), root).await?,
            )),
            None => None,
        };
        Ok(ColumnarReadContext {
            dependencies: dependencies.clone(),
            remote_cache,
            decoded_cache: DecodedColumnarCache::default(),
            raw_byte_cache_enabled: AtomicBool::new(true),
            decoded_cache_enabled: AtomicBool::new(true),
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn ephemeral_columnar_read_context(
        dependencies: &DbDependencies,
    ) -> ColumnarReadContext {
        ColumnarReadContext {
            dependencies: dependencies.clone(),
            remote_cache: None,
            decoded_cache: DecodedColumnarCache::default(),
            raw_byte_cache_enabled: AtomicBool::new(false),
            decoded_cache_enabled: AtomicBool::new(true),
        }
    }

    pub(super) async fn load_local_manifest(
        storage: &StorageConfig,
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
    ) -> Result<LoadedManifest, OpenError> {
        let StorageConfig::Tiered(config) = storage else {
            let StorageConfig::S3Primary(config) = storage else {
                return Ok(LoadedManifest::default());
            };
            return Self::load_remote_manifest(config, dependencies, columnar_read_context).await;
        };

        let current_path = Self::local_current_path(&config.ssd.path);
        let manifest_dir = Self::local_manifest_dir(&config.ssd.path);
        let mut candidates = Vec::new();
        let mut last_error = None;

        if let Some(pointer) = read_optional_path(dependencies, &current_path).await? {
            match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    if !pointer.is_empty() {
                        candidates.push(Self::join_fs_path(
                            &config.ssd.path,
                            &format!("{LOCAL_MANIFEST_DIR_RELATIVE_PATH}/{pointer}"),
                        ));
                    }
                }
                Err(error) => {
                    last_error = Some(StorageError::corruption(format!(
                        "decode CURRENT pointer failed: {error}"
                    )));
                }
            }
        }

        let mut listed = dependencies.file_system.list(&manifest_dir).await?;
        listed.retain(|path| Self::parse_manifest_generation(path).is_some());
        listed.sort_by_key(|path| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(path)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });
        for path in listed {
            if !candidates.iter().any(|candidate| candidate == &path) {
                candidates.push(path);
            }
        }

        let mut saw_manifest = false;
        for path in candidates {
            saw_manifest = true;
            match Self::read_manifest_at_path_with_context(
                dependencies,
                columnar_read_context,
                &path,
            )
            .await
            {
                Ok(manifest) => return Ok(manifest),
                Err(error) => last_error = Some(error),
            }
        }

        if saw_manifest {
            Err(OpenError::Storage(last_error.unwrap_or_else(|| {
                StorageError::corruption("no valid manifest generation found")
            })))
        } else {
            Ok(LoadedManifest::default())
        }
    }

    pub(super) fn remote_object_layout(config: &S3PrimaryStorageConfig) -> ObjectKeyLayout {
        Self::object_key_layout(&config.s3)
    }

    pub(super) fn tiered_object_layout(config: &TieredStorageConfig) -> ObjectKeyLayout {
        Self::object_key_layout(&config.s3)
    }

    pub(super) fn remote_manifest_path(
        config: &S3PrimaryStorageConfig,
        generation: ManifestId,
    ) -> String {
        Self::remote_object_layout(config).backup_manifest(generation)
    }

    pub(super) fn remote_manifest_latest_key(config: &S3PrimaryStorageConfig) -> String {
        Self::remote_object_layout(config).backup_manifest_latest()
    }

    pub(super) fn remote_commit_log_segment_key(
        config: &S3PrimaryStorageConfig,
        segment_id: crate::SegmentId,
    ) -> String {
        Self::remote_object_layout(config).backup_commit_log_segment(segment_id)
    }

    pub(super) fn remote_sstable_key(
        config: &S3PrimaryStorageConfig,
        table_id: TableId,
        local_id: &str,
    ) -> String {
        Self::remote_object_layout(config).backup_sstable(table_id, 0, local_id)
    }

    pub(super) async fn load_remote_manifest(
        config: &S3PrimaryStorageConfig,
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
    ) -> Result<LoadedManifest, OpenError> {
        Self::load_remote_manifest_from_layout(
            &Self::remote_object_layout(config),
            dependencies,
            columnar_read_context,
        )
        .await
    }

    pub(super) async fn load_remote_manifest_from_layout(
        layout: &ObjectKeyLayout,
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
    ) -> Result<LoadedManifest, OpenError> {
        let Some((key, file)) =
            Self::load_remote_manifest_file_from_layout(layout, dependencies).await?
        else {
            return Ok(LoadedManifest::default());
        };
        Self::loaded_manifest_from_remote_file(dependencies, columnar_read_context, &key, file)
            .await
            .map_err(OpenError::Storage)
    }

    pub(super) async fn load_remote_manifest_file_from_layout(
        layout: &ObjectKeyLayout,
        dependencies: &DbDependencies,
    ) -> Result<Option<(String, PersistedRemoteManifestFile)>, OpenError> {
        let latest_key = layout.backup_manifest_latest();
        let manifest_prefix = layout.backup_manifest_prefix();
        let mut candidates = Vec::new();
        let mut last_error = None;

        match dependencies.object_store.get(&latest_key).await {
            Ok(pointer) => match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    if !pointer.is_empty() {
                        candidates.push(pointer.to_string());
                    }
                }
                Err(error) => {
                    last_error = Some(StorageError::corruption(format!(
                        "decode remote latest pointer failed: {error}"
                    )));
                }
            },
            Err(error) if error.kind() == StorageErrorKind::NotFound => {}
            Err(error) => last_error = Some(error),
        }

        let _ = dependencies
            .__failpoint_registry()
            .trigger(
                crate::failpoints::names::DB_REMOTE_MANIFEST_RECOVERY_AFTER_POINTER_READ,
                BTreeMap::from([("candidate_count".to_string(), candidates.len().to_string())]),
            )
            .await
            .map_err(OpenError::Storage)?;

        let mut listed = dependencies.object_store.list(&manifest_prefix).await?;
        listed.retain(|key| key != &latest_key && Self::parse_manifest_generation(key).is_some());
        listed.sort_by_key(|key| {
            std::cmp::Reverse(
                Self::parse_manifest_generation(key)
                    .map(ManifestId::get)
                    .unwrap_or_default(),
            )
        });
        for key in listed {
            if !candidates.iter().any(|candidate| candidate == &key) {
                candidates.push(key);
            }
        }

        let mut saw_manifest = false;
        for key in candidates {
            saw_manifest = true;
            match Self::read_remote_manifest_file_at_key(dependencies, &key).await {
                Ok(file) => return Ok(Some((key, file))),
                Err(error) => last_error = Some(error),
            }
        }

        if saw_manifest {
            Err(OpenError::Storage(last_error.unwrap_or_else(|| {
                StorageError::corruption("no valid remote manifest generation found")
            })))
        } else {
            Ok(None)
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn read_manifest_at_path(
        dependencies: &DbDependencies,
        path: &str,
    ) -> Result<LoadedManifest, StorageError> {
        let columnar_read_context = Self::ephemeral_columnar_read_context(dependencies);
        Self::read_manifest_at_path_with_context(dependencies, &columnar_read_context, path).await
    }

    pub(super) async fn read_manifest_at_path_with_context(
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
        path: &str,
    ) -> Result<LoadedManifest, StorageError> {
        let bytes = read_source(dependencies, &StorageSource::local_file(path)).await?;
        let file = Self::decode_manifest_file_flatbuffer(&bytes)?;

        if file.body.format_version != MANIFEST_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported manifest version {}",
                file.body.format_version
            )));
        }

        let encoded_body = Self::encode_manifest_body_flatbuffer(&file.body)?;
        if checksum32(&encoded_body) != file.checksum {
            return Err(StorageError::corruption(format!(
                "manifest checksum mismatch for {path}"
            )));
        }

        let mut live_sstables = Vec::with_capacity(file.body.sstables.len());
        let mut max_sequence = SequenceNumber::new(0);
        for sstable in &file.body.sstables {
            max_sequence = max_sequence.max(sstable.max_sequence);
            live_sstables.push(
                Self::load_resident_sstable_with_context(
                    dependencies,
                    columnar_read_context,
                    sstable,
                )
                .await?,
            );
        }

        if max_sequence > file.body.last_flushed_sequence {
            return Err(StorageError::corruption(format!(
                "manifest {path} flush watermark is behind referenced SSTables"
            )));
        }

        Ok(LoadedManifest {
            generation: file.body.generation,
            last_flushed_sequence: file.body.last_flushed_sequence,
            live_sstables,
            durable_commit_log_segments: Vec::new(),
        })
    }

    pub(super) async fn read_remote_manifest_file_at_key(
        dependencies: &DbDependencies,
        key: &str,
    ) -> Result<PersistedRemoteManifestFile, StorageError> {
        let bytes = read_source(dependencies, &StorageSource::remote_object(key)).await?;
        let file = Self::decode_remote_manifest_file_flatbuffer(&bytes)?;

        if file.body.format_version != REMOTE_MANIFEST_FORMAT_VERSION {
            return Err(StorageError::unsupported(format!(
                "unsupported remote manifest version {}",
                file.body.format_version
            )));
        }

        let encoded_body = Self::encode_remote_manifest_body_flatbuffer(&file.body)?;
        if checksum32(&encoded_body) != file.checksum {
            return Err(StorageError::corruption(format!(
                "remote manifest checksum mismatch for {key}"
            )));
        }

        Ok(file)
    }

    pub(super) async fn loaded_manifest_from_remote_file(
        dependencies: &DbDependencies,
        columnar_read_context: &ColumnarReadContext,
        key: &str,
        file: PersistedRemoteManifestFile,
    ) -> Result<LoadedManifest, StorageError> {
        let mut live_sstables = Vec::with_capacity(file.body.sstables.len());
        let mut max_sstable_sequence = SequenceNumber::new(0);
        for sstable in &file.body.sstables {
            max_sstable_sequence = max_sstable_sequence.max(sstable.max_sequence);
            live_sstables.push(
                Self::load_resident_sstable_with_context(
                    dependencies,
                    columnar_read_context,
                    sstable,
                )
                .await?,
            );
        }

        let max_log_sequence = file
            .body
            .commit_log_segments
            .iter()
            .map(|segment| segment.footer.max_sequence)
            .max()
            .unwrap_or_default();

        if max_sstable_sequence.max(max_log_sequence) > file.body.last_flushed_sequence {
            return Err(StorageError::corruption(format!(
                "remote manifest {key} flush watermark is behind durable objects"
            )));
        }

        Ok(LoadedManifest {
            generation: file.body.generation,
            last_flushed_sequence: file.body.last_flushed_sequence,
            live_sstables,
            durable_commit_log_segments: file.body.commit_log_segments,
        })
    }

    pub(super) async fn maybe_restore_tiered_from_backup(
        config: &TieredStorageConfig,
        dependencies: &DbDependencies,
    ) -> Result<(), OpenError> {
        let root = &config.ssd.path;
        let marker_path = Self::backup_restore_marker_path(root);
        let restore_incomplete = read_optional_path(dependencies, &marker_path)
            .await?
            .is_some();
        let has_local_catalog = read_optional_path(
            dependencies,
            &Self::join_fs_path(root, LOCAL_CATALOG_RELATIVE_PATH),
        )
        .await?
        .is_some();
        let has_local_current = read_optional_path(dependencies, &Self::local_current_path(root))
            .await?
            .is_some();
        let has_local_manifests = !dependencies
            .file_system
            .list(&Self::local_manifest_dir(root))
            .await?
            .is_empty();
        let has_local_commit_log = !dependencies
            .file_system
            .list(&Self::local_commit_log_dir(root))
            .await?
            .is_empty();

        if !restore_incomplete
            && (has_local_catalog
                || has_local_current
                || has_local_manifests
                || has_local_commit_log)
        {
            return Ok(());
        }

        let layout = Self::tiered_object_layout(config);
        let remote_catalog =
            read_optional_remote_object(dependencies, &layout.backup_catalog()).await?;
        let remote_manifest =
            Self::load_remote_manifest_file_from_layout(&layout, dependencies).await?;
        let mut remote_commit_log_keys = dependencies
            .object_store
            .list(&layout.backup_commit_log_prefix())
            .await?;

        let Some(catalog_bytes) = remote_catalog else {
            if remote_manifest.is_some() || !remote_commit_log_keys.is_empty() {
                return Err(OpenError::Storage(StorageError::corruption(
                    "backup recovery requires a replicated catalog",
                )));
            }
            return Ok(());
        };

        write_local_file_atomic(dependencies, &marker_path, b"incomplete backup restore\n").await?;
        write_local_file_atomic(
            dependencies,
            &Self::join_fs_path(root, LOCAL_CATALOG_RELATIVE_PATH),
            &catalog_bytes,
        )
        .await?;

        if let Some((_key, file)) = remote_manifest {
            for sstable in &file.body.sstables {
                let remote_key = sstable.remote_key.as_ref().ok_or_else(|| {
                    OpenError::Storage(StorageError::corruption(format!(
                        "remote manifest SSTable {} is missing a remote key",
                        sstable.local_id
                    )))
                })?;
                let local_path = if sstable.file_path.is_empty() {
                    Self::local_sstable_path(root, sstable.table_id, &sstable.local_id)
                } else {
                    sstable.file_path.clone()
                };
                let bytes = read_source(
                    dependencies,
                    &StorageSource::remote_object(remote_key.clone()),
                )
                .await?;
                write_local_file_atomic(dependencies, &local_path, &bytes).await?;
            }

            remote_commit_log_keys.extend(
                file.body
                    .commit_log_segments
                    .iter()
                    .map(|segment| segment.object_key.clone()),
            );
            let local_manifest_bytes = Self::encode_manifest_payload_from_sstables(
                file.body.generation,
                file.body.last_flushed_sequence,
                &file.body.sstables,
            )?;
            write_local_file_atomic(
                dependencies,
                &Self::local_manifest_path(root, file.body.generation),
                &local_manifest_bytes,
            )
            .await?;
            write_local_file_atomic(
                dependencies,
                &Self::local_current_path(root),
                format!("{}\n", Self::manifest_filename(file.body.generation)).as_bytes(),
            )
            .await?;
        }

        remote_commit_log_keys.sort();
        remote_commit_log_keys.dedup();
        for object_key in remote_commit_log_keys {
            let Some(segment_id) = Self::parse_segment_id(&object_key) else {
                continue;
            };
            let bytes =
                read_source(dependencies, &StorageSource::remote_object(object_key)).await?;
            write_local_file_atomic(
                dependencies,
                &Self::local_commit_log_segment_path(root, segment_id),
                &bytes,
            )
            .await?;
        }

        delete_local_file_if_exists(dependencies, &marker_path).await?;
        Ok(())
    }
}
