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
            let execution_identity = dependencies
                .execution_identity()
                .unwrap_or(db_name.as_str())
                .to_string();
            let scheduler = config
                .scheduler
                .clone()
                .unwrap_or_else(|| Arc::new(RoundRobinScheduler::default()));
            config.scheduler = Some(scheduler.clone());
            let resource_manager = dependencies.resource_manager();
            let execution_profile = Self::resolve_execution_profile(
                &resource_manager,
                dependencies.execution_profile().clone(),
                &execution_identity,
            );
            Self::register_execution_profile_domains(
                &resource_manager,
                &execution_identity,
                &execution_profile,
            );
            Self::ensure_control_plane_domain_registered(
                &resource_manager,
                &execution_profile,
                &execution_identity,
            );
            if let StorageConfig::Tiered(tiered) = &config.storage {
                Self::maybe_restore_tiered_from_backup(tiered, &dependencies).await?;
            }
            let columnar_read_context = Arc::new(
                Self::open_columnar_read_context(
                    &config.storage,
                    &config.hybrid_read,
                    &execution_profile,
                    &resource_manager,
                    &dependencies,
                )
                .await
                .map_err(OpenError::Storage)?,
            );
            let catalog_location = Self::catalog_location(&config.storage);
            let (tables, next_table_id) =
                Self::load_tables(&dependencies, &catalog_location).await?;
            let prior_decoded_cache_enabled = columnar_read_context
                .decoded_cache_enabled
                .swap(false, std::sync::atomic::Ordering::Relaxed);
            let prior_raw_byte_cache_population_enabled = columnar_read_context
                .raw_byte_cache_population_enabled
                .swap(false, std::sync::atomic::Ordering::Relaxed);
            let loaded_manifest =
                Self::load_local_manifest(&config.storage, &dependencies, &columnar_read_context)
                    .await;
            columnar_read_context.decoded_cache.clear();
            columnar_read_context.decoded_cache.reset_stats();
            columnar_read_context.decoded_cache_enabled.store(
                prior_decoded_cache_enabled,
                std::sync::atomic::Ordering::Relaxed,
            );
            columnar_read_context
                .raw_byte_cache_population_enabled
                .store(
                    prior_raw_byte_cache_population_enabled,
                    std::sync::atomic::Ordering::Relaxed,
                );
            let loaded_manifest = loaded_manifest?;
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
                    execution_identity,
                    scheduler,
                    resource_manager,
                    execution_profile,
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
                    work_deferral_domains: Mutex::new(BTreeMap::new()),
                    pending_work_budget_state: Mutex::new(PendingWorkBudgetState::default()),
                    scheduler_observability: SchedulerObservabilityStats::default(),
                    compact_to_wide_stats: Mutex::new(BTreeMap::new()),
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

    fn resolve_execution_profile(
        resource_manager: &Arc<dyn crate::execution::ResourceManager>,
        execution_profile: crate::execution::DbExecutionProfile,
        db_name: &str,
    ) -> crate::execution::DbExecutionProfile {
        let foreground = Self::resolve_execution_lane(
            resource_manager,
            crate::execution::ExecutionDomainOwner::Database {
                name: db_name.to_string(),
            },
            crate::execution::ExecutionLane::UserForeground,
            &execution_profile.foreground,
        );
        let background = Self::resolve_execution_lane(
            resource_manager,
            crate::execution::ExecutionDomainOwner::Database {
                name: db_name.to_string(),
            },
            crate::execution::ExecutionLane::UserBackground,
            &execution_profile.background,
        );
        let control_plane = Self::resolve_execution_lane(
            resource_manager,
            crate::execution::ExecutionDomainOwner::Subsystem {
                database: Some(db_name.to_string()),
                name: "control-plane".to_string(),
            },
            crate::execution::ExecutionLane::ControlPlane,
            &execution_profile.control_plane,
        );
        execution_profile
            .with_foreground(foreground)
            .with_background(background)
            .with_control_plane(control_plane)
    }

    fn resolve_execution_lane(
        resource_manager: &Arc<dyn crate::execution::ResourceManager>,
        owner: crate::execution::ExecutionDomainOwner,
        lane: crate::execution::ExecutionLane,
        binding: &crate::execution::ExecutionLaneBinding,
    ) -> crate::execution::ExecutionLaneBinding {
        let assignment = resource_manager.assign(crate::execution::PlacementRequest {
            owner,
            lane,
            preferred_domain: binding.domain.clone(),
            requested_budget: crate::execution::ExecutionDomainBudget::default(),
            placement: crate::execution::ExecutionDomainPlacement::SharedWeighted { weight: 1 },
            default_durability_class: binding.durability_class.clone(),
        });
        crate::execution::ExecutionLaneBinding::new(
            assignment.domain,
            assignment.default_durability_class,
        )
    }

    fn register_execution_profile_domains(
        resource_manager: &Arc<dyn crate::execution::ResourceManager>,
        db_name: &str,
        execution_profile: &crate::execution::DbExecutionProfile,
    ) {
        let user_owner = crate::execution::ExecutionDomainOwner::Database {
            name: db_name.to_string(),
        };

        for (binding, owner, lane_key) in [
            (
                &execution_profile.foreground,
                user_owner.clone(),
                "user-foreground",
            ),
            (
                &execution_profile.background,
                user_owner.clone(),
                "user-background",
            ),
        ] {
            resource_manager.register_domain(crate::execution::ExecutionDomainSpec {
                path: binding.domain.clone(),
                owner,
                budget: crate::execution::ExecutionDomainBudget::default(),
                placement: crate::execution::ExecutionDomainPlacement::SharedWeighted { weight: 1 },
                metadata: BTreeMap::from([
                    ("terracedb.execution.lane".to_string(), lane_key.to_string()),
                    (
                        format!("terracedb.execution.lane.{lane_key}"),
                        "true".to_string(),
                    ),
                    (
                        "terracedb.execution.durability".to_string(),
                        crate::execution::durability_class_metadata_value(
                            &binding.durability_class,
                        ),
                    ),
                    (
                        "terracedb.execution.database".to_string(),
                        db_name.to_string(),
                    ),
                ]),
            });
        }
    }

    fn ensure_control_plane_domain_registered(
        resource_manager: &Arc<dyn crate::execution::ResourceManager>,
        execution_profile: &crate::execution::DbExecutionProfile,
        db_name: &str,
    ) {
        let path = execution_profile.control_plane.domain.clone();
        if resource_manager.snapshot().domains.contains_key(&path) {
            return;
        }

        resource_manager.register_domain(crate::execution::ExecutionDomainSpec {
            path,
            owner: crate::execution::ExecutionDomainOwner::ProcessControl,
            budget: crate::execution::reserved_control_plane_budget(),
            placement: crate::execution::ExecutionDomainPlacement::SharedWeighted { weight: 1 },
            metadata: BTreeMap::from([
                ("reserved".to_string(), "true".to_string()),
                (
                    "durability_class".to_string(),
                    crate::execution::durability_class_metadata_value(
                        &execution_profile.control_plane.durability_class,
                    ),
                ),
                (
                    "terracedb.execution.lane".to_string(),
                    crate::execution::execution_lane_name(
                        crate::execution::ExecutionLane::ControlPlane,
                    )
                    .to_string(),
                ),
                ("db_name".to_string(), db_name.to_string()),
                (
                    "terracedb.execution.database".to_string(),
                    db_name.to_string(),
                ),
                (
                    "terracedb.execution.lane.control-plane".to_string(),
                    "true".to_string(),
                ),
                (
                    "terracedb.execution.durability".to_string(),
                    crate::execution::durability_class_metadata_value(
                        &execution_profile.control_plane.durability_class,
                    ),
                ),
            ]),
        });
    }

    fn control_plane_error(label: &str, detail: impl Into<String>) -> StorageError {
        StorageError::corruption(format!(
            "control-plane {label}: {}; repair by restoring or regenerating a single authoritative control-plane copy before reopening",
            detail.into()
        ))
    }

    fn annotate_control_plane_error(label: &str, error: StorageError) -> StorageError {
        StorageError::new(
            error.kind(),
            format!(
                "control-plane {label}: {}; repair by restoring or regenerating a single authoritative control-plane copy before reopening",
                error.message()
            ),
        )
    }

    fn control_plane_mismatch(label: &str, primary: &str, legacy: &str) -> StorageError {
        Self::control_plane_error(
            label,
            format!("durability lane mismatch between {primary} and legacy {legacy}"),
        )
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
        Self::validate_hybrid_table_features(config)
            .map_err(|error| CreateTableError::InvalidConfig(error.message().to_string()))?;

        Ok(())
    }

    pub(super) fn hybrid_table_features(
        metadata: &TableMetadata,
    ) -> Result<HybridTableFeatures, StorageError> {
        let Some(value) = metadata.get(HYBRID_TABLE_FEATURES_METADATA_KEY).cloned() else {
            return Ok(HybridTableFeatures::default());
        };
        serde_json::from_value(value).map_err(|error| {
            StorageError::unsupported(format!(
                "invalid {} table metadata: {error}",
                HYBRID_TABLE_FEATURES_METADATA_KEY
            ))
        })
    }

    pub(super) fn compact_to_wide_promotion_config(
        metadata: &TableMetadata,
    ) -> Result<Option<HybridCompactToWidePromotionConfig>, StorageError> {
        let Some(value) = metadata
            .get(HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY)
            .cloned()
        else {
            return Ok(None);
        };
        serde_json::from_value(value).map(Some).map_err(|error| {
            StorageError::unsupported(format!(
                "invalid {} table metadata: {error}",
                HYBRID_COMPACT_TO_WIDE_PROMOTION_METADATA_KEY
            ))
        })
    }

    pub(super) fn validate_hybrid_table_features(config: &TableConfig) -> Result<(), StorageError> {
        let features = Self::hybrid_table_features(&config.metadata)?;
        let promotion = Self::compact_to_wide_promotion_config(&config.metadata)?;
        if let Some(promotion) = promotion.as_ref() {
            if config.format != TableFormat::Columnar {
                return Err(StorageError::unsupported(
                    "compact-to-wide promotion requires a columnar table",
                ));
            }
            if promotion.max_compact_rows == 0 {
                return Err(StorageError::unsupported(
                    "compact-to-wide promotion requires max_compact_rows > 0",
                ));
            }
        }
        if features.skip_indexes.is_empty() && features.projection_sidecars.is_empty() {
            return Ok(());
        }
        if config.format != TableFormat::Columnar {
            return Err(StorageError::unsupported(
                "hybrid skip indexes and projection sidecars require a columnar table",
            ));
        }

        let schema = config.schema.as_ref().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar table {} is missing a schema",
                config.name
            ))
        })?;
        let validation = SchemaValidation::new(schema)?;
        let mut seen_names = BTreeSet::new();

        for skip in &features.skip_indexes {
            if skip.name.trim().is_empty() {
                return Err(StorageError::unsupported(
                    "skip-index configs require a non-empty name",
                ));
            }
            if !seen_names.insert(format!("skip:{}", skip.name)) {
                return Err(StorageError::unsupported(format!(
                    "duplicate skip-index config {}",
                    skip.name
                )));
            }
            if skip.max_values == 0 {
                return Err(StorageError::unsupported(format!(
                    "skip-index {} requires max_values > 0",
                    skip.name
                )));
            }
            match skip.family {
                HybridSkipIndexFamily::UserKeyBloom => {
                    if skip.field.is_some() {
                        return Err(StorageError::unsupported(format!(
                            "skip-index {} uses user_key_bloom and may not specify a field",
                            skip.name
                        )));
                    }
                }
                HybridSkipIndexFamily::FieldValueBloom | HybridSkipIndexFamily::BoundedSet => {
                    let field_name = skip.field.as_ref().ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "skip-index {} requires a field",
                            skip.name
                        ))
                    })?;
                    let field_id =
                        validation
                            .field_ids_by_name
                            .get(field_name)
                            .ok_or_else(|| {
                                StorageError::unsupported(format!(
                                    "skip-index {} references unknown field {}",
                                    skip.name, field_name
                                ))
                            })?;
                    let field = validation.fields_by_id.get(field_id).ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar table {} schema is missing field id {}",
                            config.name,
                            field_id.get()
                        ))
                    })?;
                    if skip.family == HybridSkipIndexFamily::BoundedSet
                        && field.field_type == FieldType::Float64
                    {
                        return Err(StorageError::unsupported(format!(
                            "bounded-set skip-index {} does not support float64 field {}",
                            skip.name, field_name
                        )));
                    }
                }
            }
        }

        for projection in &features.projection_sidecars {
            if projection.name.trim().is_empty() {
                return Err(StorageError::unsupported(
                    "projection-sidecar configs require a non-empty name",
                ));
            }
            if !seen_names.insert(format!("projection:{}", projection.name)) {
                return Err(StorageError::unsupported(format!(
                    "duplicate projection-sidecar config {}",
                    projection.name
                )));
            }
            if projection.fields.is_empty() {
                return Err(StorageError::unsupported(format!(
                    "projection-sidecar {} requires at least one field",
                    projection.name
                )));
            }
            let mut seen_fields = BTreeSet::new();
            for field_name in &projection.fields {
                let field_id = validation
                    .field_ids_by_name
                    .get(field_name)
                    .copied()
                    .ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "projection-sidecar {} references unknown field {}",
                            projection.name, field_name
                        ))
                    })?;
                if !seen_fields.insert(field_id) {
                    return Err(StorageError::unsupported(format!(
                        "projection-sidecar {} lists field {} more than once",
                        projection.name, field_name
                    )));
                }
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
                legacy_key: Some(Self::remote_object_layout(config).backup_catalog()),
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
            CatalogLocation::ObjectStore { key, legacy_key } => {
                let primary = read_optional_remote_object(dependencies, key).await?;
                let legacy = match legacy_key {
                    Some(legacy_key) => {
                        read_optional_remote_object(dependencies, legacy_key).await?
                    }
                    None => None,
                };
                if let (Some(primary), Some(legacy)) = (&primary, &legacy)
                    && primary != legacy
                {
                    return Err(OpenError::Storage(Self::control_plane_mismatch(
                        "catalog",
                        key,
                        legacy_key
                            .as_deref()
                            .expect("legacy key should exist when comparing copies"),
                    )));
                }
                primary.or(legacy)
            }
        };

        match bytes {
            Some(bytes) => Self::decode_catalog(&bytes).map_err(|error| {
                OpenError::Storage(Self::annotate_control_plane_error("catalog", error))
            }),
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
            CatalogLocation::ObjectStore { key, .. } => {
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

    fn columnar_cache_domain_paths(
        execution_profile: &crate::DbExecutionProfile,
    ) -> BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath> {
        BTreeMap::from([
            (
                crate::ExecutionLane::UserForeground,
                execution_profile.foreground.domain.clone(),
            ),
            (
                crate::ExecutionLane::UserBackground,
                execution_profile.background.domain.clone(),
            ),
            (
                crate::ExecutionLane::ControlPlane,
                execution_profile.control_plane.domain.clone(),
            ),
        ])
    }

    fn default_columnar_cache_lane_budgets(
        hybrid_read: &crate::HybridReadConfig,
    ) -> BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget> {
        let weights = BTreeMap::from([
            (crate::ExecutionLane::UserForeground, 4_u32),
            (crate::ExecutionLane::UserBackground, 2_u32),
            (crate::ExecutionLane::ControlPlane, 1_u32),
        ]);
        let raw = Self::distribute_u64_by_weight(hybrid_read.raw_segment_cache_bytes, &weights);
        let metadata =
            Self::distribute_usize_by_weight(hybrid_read.decoded_metadata_cache_entries, &weights);
        let columns =
            Self::distribute_usize_by_weight(hybrid_read.decoded_column_cache_entries, &weights);
        weights
            .keys()
            .copied()
            .map(|lane| {
                (
                    lane,
                    ColumnarCacheLaneBudget {
                        raw_byte_budget_bytes: raw.get(&lane).copied().unwrap_or_default(),
                        decoded_metadata_entry_limit: metadata
                            .get(&lane)
                            .copied()
                            .unwrap_or_default(),
                        decoded_column_entry_limit: columns.get(&lane).copied().unwrap_or_default(),
                    },
                )
            })
            .collect()
    }

    fn columnar_cache_lane_budgets(
        hybrid_read: &crate::HybridReadConfig,
        cache_domain_paths: &BTreeMap<crate::ExecutionLane, crate::ExecutionDomainPath>,
        resource_manager: &Arc<dyn crate::ResourceManager>,
    ) -> BTreeMap<crate::ExecutionLane, ColumnarCacheLaneBudget> {
        let snapshot = resource_manager.snapshot();
        let default_weights = BTreeMap::from([
            (crate::ExecutionLane::UserForeground, 4_u32),
            (crate::ExecutionLane::UserBackground, 2_u32),
            (crate::ExecutionLane::ControlPlane, 1_u32),
        ]);
        let requested_raw = cache_domain_paths
            .iter()
            .map(|(lane, path)| {
                (
                    *lane,
                    snapshot
                        .domains
                        .get(path)
                        .and_then(|domain| domain.spec.budget.memory.cache_bytes),
                )
            })
            .collect::<BTreeMap<_, _>>();
        if requested_raw.values().all(Option::is_none) {
            return Self::default_columnar_cache_lane_budgets(hybrid_read);
        }
        let weights = cache_domain_paths
            .iter()
            .map(|(lane, path)| {
                let weight = snapshot
                    .domains
                    .get(path)
                    .map(|domain| match domain.spec.placement {
                        crate::ExecutionDomainPlacement::SharedWeighted { weight } => weight,
                        crate::ExecutionDomainPlacement::Dedicated => {
                            default_weights.get(lane).copied().unwrap_or(1).max(2)
                        }
                    })
                    .unwrap_or_else(|| default_weights.get(lane).copied().unwrap_or(1));
                (*lane, weight)
            })
            .collect::<BTreeMap<_, _>>();
        let raw = Self::distribute_u64_with_requests(
            hybrid_read.raw_segment_cache_bytes,
            &requested_raw,
            &weights,
        );
        let metadata = Self::distribute_usize_by_budget(
            hybrid_read.decoded_metadata_cache_entries,
            &raw,
            &weights,
        );
        let columns = Self::distribute_usize_by_budget(
            hybrid_read.decoded_column_cache_entries,
            &raw,
            &weights,
        );
        weights
            .keys()
            .copied()
            .map(|lane| {
                (
                    lane,
                    ColumnarCacheLaneBudget {
                        raw_byte_budget_bytes: raw.get(&lane).copied().unwrap_or_default(),
                        decoded_metadata_entry_limit: metadata
                            .get(&lane)
                            .copied()
                            .unwrap_or_default(),
                        decoded_column_entry_limit: columns.get(&lane).copied().unwrap_or_default(),
                    },
                )
            })
            .collect()
    }

    fn distribute_u64_by_weight(
        total: u64,
        weights: &BTreeMap<crate::ExecutionLane, u32>,
    ) -> BTreeMap<crate::ExecutionLane, u64> {
        let requested = weights
            .keys()
            .copied()
            .map(|lane| (lane, None))
            .collect::<BTreeMap<_, _>>();
        Self::distribute_u64_with_requests(total, &requested, weights)
    }

    fn distribute_u64_with_requests(
        total: u64,
        requested: &BTreeMap<crate::ExecutionLane, Option<u64>>,
        weights: &BTreeMap<crate::ExecutionLane, u32>,
    ) -> BTreeMap<crate::ExecutionLane, u64> {
        let mut allocated = requested
            .keys()
            .copied()
            .map(|lane| (lane, 0_u64))
            .collect::<BTreeMap<_, _>>();
        let fixed_total = requested.values().flatten().copied().sum::<u64>();
        if fixed_total > 0 && fixed_total >= total {
            let fixed_lanes = requested
                .iter()
                .filter_map(|(lane, bytes)| bytes.map(|bytes| (*lane, bytes)))
                .collect::<BTreeMap<_, _>>();
            for (lane, share) in Self::apportion_u64(total, &fixed_lanes, weights) {
                allocated.insert(lane, share);
            }
            return allocated;
        }

        let mut remaining = total;
        for (&lane, bytes) in requested {
            if let Some(bytes) = bytes {
                allocated.insert(lane, *bytes);
                remaining = remaining.saturating_sub(*bytes);
            }
        }
        let flexible_weights = requested
            .iter()
            .filter(|(_, bytes)| bytes.is_none())
            .map(|(lane, _)| (*lane, u64::from(weights.get(lane).copied().unwrap_or(1))))
            .collect::<BTreeMap<_, _>>();
        for (lane, share) in Self::apportion_u64(remaining, &flexible_weights, weights) {
            *allocated.entry(lane).or_default() += share;
        }
        allocated
    }

    fn distribute_usize_by_weight(
        total: usize,
        weights: &BTreeMap<crate::ExecutionLane, u32>,
    ) -> BTreeMap<crate::ExecutionLane, usize> {
        let budget = weights
            .iter()
            .map(|(lane, weight)| (*lane, u64::from(*weight)))
            .collect::<BTreeMap<_, _>>();
        Self::distribute_usize_by_budget(total, &budget, weights)
    }

    fn distribute_usize_by_budget(
        total: usize,
        budget: &BTreeMap<crate::ExecutionLane, u64>,
        weights: &BTreeMap<crate::ExecutionLane, u32>,
    ) -> BTreeMap<crate::ExecutionLane, usize> {
        Self::apportion_u64(total as u64, budget, weights)
            .into_iter()
            .map(|(lane, share)| (lane, share as usize))
            .collect()
    }

    fn apportion_u64(
        total: u64,
        numerators: &BTreeMap<crate::ExecutionLane, u64>,
        weights: &BTreeMap<crate::ExecutionLane, u32>,
    ) -> BTreeMap<crate::ExecutionLane, u64> {
        let mut allocated = numerators
            .keys()
            .copied()
            .map(|lane| (lane, 0_u64))
            .collect::<BTreeMap<_, _>>();
        let total_numerator = numerators.values().copied().sum::<u64>();
        if total == 0 || total_numerator == 0 {
            return allocated;
        }

        let total_u128 = u128::from(total);
        let denominator_u128 = u128::from(total_numerator);
        let mut allocated_total = 0_u64;
        let mut remainders = Vec::with_capacity(numerators.len());
        for (&lane, &numerator) in numerators {
            let quota = total_u128.saturating_mul(u128::from(numerator));
            let share = (quota / denominator_u128) as u64;
            let remainder = quota % denominator_u128;
            allocated.insert(lane, share);
            allocated_total = allocated_total.saturating_add(share);
            remainders.push((lane, remainder));
        }

        remainders.sort_by(
            |(left_lane, left_remainder), (right_lane, right_remainder)| {
                right_remainder
                    .cmp(left_remainder)
                    .then_with(|| {
                        weights
                            .get(right_lane)
                            .copied()
                            .unwrap_or(1)
                            .cmp(&weights.get(left_lane).copied().unwrap_or(1))
                    })
                    .then_with(|| {
                        Self::lane_allocation_priority(*left_lane)
                            .cmp(&Self::lane_allocation_priority(*right_lane))
                    })
            },
        );

        for (lane, _) in remainders
            .into_iter()
            .take(total.saturating_sub(allocated_total) as usize)
        {
            *allocated.entry(lane).or_default() += 1;
        }
        allocated
    }

    fn lane_allocation_priority(lane: crate::ExecutionLane) -> u8 {
        match lane {
            crate::ExecutionLane::UserForeground => 0,
            crate::ExecutionLane::UserBackground => 1,
            crate::ExecutionLane::ControlPlane => 2,
        }
    }

    pub(super) async fn open_columnar_read_context(
        storage: &StorageConfig,
        hybrid_read: &crate::HybridReadConfig,
        execution_profile: &crate::DbExecutionProfile,
        resource_manager: &Arc<dyn crate::ResourceManager>,
        dependencies: &DbDependencies,
    ) -> Result<ColumnarReadContext, StorageError> {
        let cache_domain_paths = Self::columnar_cache_domain_paths(execution_profile);
        let cache_lane_budgets =
            Self::columnar_cache_lane_budgets(hybrid_read, &cache_domain_paths, resource_manager);
        let total_raw_byte_budget = cache_lane_budgets
            .values()
            .map(|budget| budget.raw_byte_budget_bytes)
            .sum::<u64>()
            .max(1);
        let background_prefetch_budget = cache_lane_budgets
            .get(&crate::ExecutionLane::UserBackground)
            .map(|budget| budget.raw_byte_budget_bytes)
            .unwrap_or_default()
            // Prefetch is a per-read lookahead window, not a signal to download
            // the entire background cache partition behind one point read.
            .min(crate::remote::DEFAULT_REMOTE_CACHE_PREFETCH_BYTES);
        let remote_cache_config = match storage {
            StorageConfig::Tiered(config) => Some((
                Self::join_fs_path(&config.ssd.path, LOCAL_REMOTE_CACHE_RELATIVE_DIR),
                total_raw_byte_budget,
            )),
            StorageConfig::S3Primary(config) => Some((
                Self::s3_primary_remote_cache_root(config),
                total_raw_byte_budget,
            )),
        };
        let remote_cache = match remote_cache_config {
            Some((root, max_bytes)) => Some(Arc::new(
                RemoteCache::open_with_config(
                    dependencies.file_system.clone(),
                    root,
                    crate::remote::RemoteCacheConfig {
                        max_bytes,
                        prefetch_bytes: background_prefetch_budget,
                        ..Default::default()
                    },
                )
                .await?,
            )),
            None => None,
        };
        let (raw_byte_total, raw_byte_order, raw_byte_lengths) = remote_cache
            .as_ref()
            .map(|cache| {
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
                (cache.total_cached_bytes(), order, lengths)
            })
            .unwrap_or_default();
        let context = ColumnarReadContext {
            dependencies: dependencies.clone(),
            remote_cache,
            decoded_cache: DecodedColumnarCache::new(
                cache_lane_budgets
                    .iter()
                    .map(|(lane, budget)| (*lane, budget.decoded_metadata_entry_limit))
                    .collect(),
                cache_lane_budgets
                    .iter()
                    .map(|(lane, budget)| (*lane, budget.decoded_column_entry_limit))
                    .collect(),
            ),
            raw_byte_cache_enabled: AtomicBool::new(true),
            raw_byte_cache_population_enabled: AtomicBool::new(true),
            decoded_cache_enabled: AtomicBool::new(true),
            raw_byte_cache_budget_bytes: total_raw_byte_budget,
            raw_byte_cache_budget_state: Mutex::new(RawByteCacheBudgetState {
                total_bytes: raw_byte_total,
                order: raw_byte_order,
                lengths: raw_byte_lengths,
                owners: BTreeMap::new(),
            }),
            cache_domain_paths,
            cache_lane_budgets,
            skip_indexes_enabled: hybrid_read.skip_indexes_enabled,
            projection_sidecars_enabled: hybrid_read.projection_sidecars_enabled,
            aggressive_background_repair: hybrid_read.aggressive_background_repair,
        };
        context.trim_raw_byte_cache_to_budget().await?;
        Ok(context)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn ephemeral_columnar_read_context(
        dependencies: &DbDependencies,
    ) -> ColumnarReadContext {
        let config = crate::HybridReadConfig::default();
        let cache_lane_budgets = Self::default_columnar_cache_lane_budgets(&config);
        ColumnarReadContext {
            dependencies: dependencies.clone(),
            remote_cache: None,
            decoded_cache: DecodedColumnarCache::new(
                cache_lane_budgets
                    .iter()
                    .map(|(lane, budget)| (*lane, budget.decoded_metadata_entry_limit))
                    .collect(),
                cache_lane_budgets
                    .iter()
                    .map(|(lane, budget)| (*lane, budget.decoded_column_entry_limit))
                    .collect(),
            ),
            raw_byte_cache_enabled: AtomicBool::new(false),
            raw_byte_cache_population_enabled: AtomicBool::new(false),
            decoded_cache_enabled: AtomicBool::new(true),
            raw_byte_cache_budget_bytes: 0,
            raw_byte_cache_budget_state: Mutex::new(RawByteCacheBudgetState::default()),
            cache_domain_paths: Self::columnar_cache_domain_paths(
                &crate::DbExecutionProfile::default(),
            ),
            cache_lane_budgets,
            skip_indexes_enabled: false,
            projection_sidecars_enabled: false,
            aggressive_background_repair: false,
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
                    last_error = Some(Self::control_plane_error(
                        "local manifest latest pointer",
                        format!("decode CURRENT pointer failed: {error}"),
                    ));
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
                Err(error) => {
                    last_error = Some(Self::annotate_control_plane_error("local manifest", error))
                }
            }
        }

        if saw_manifest {
            Err(OpenError::Storage(last_error.unwrap_or_else(|| {
                Self::control_plane_error(
                    "local manifest",
                    "recovery found no valid manifest generation",
                )
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
        Self::remote_object_layout(config).control_manifest(generation)
    }

    pub(super) fn remote_manifest_latest_key(config: &S3PrimaryStorageConfig) -> String {
        Self::remote_object_layout(config).control_manifest_latest()
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
            .map_err(|error| {
                OpenError::Storage(Self::annotate_control_plane_error("remote manifest", error))
            })
    }

    pub(super) async fn load_remote_manifest_file_from_layout(
        layout: &ObjectKeyLayout,
        dependencies: &DbDependencies,
    ) -> Result<Option<(String, PersistedRemoteManifestFile)>, OpenError> {
        let latest_key = layout.control_manifest_latest();
        let manifest_prefix = layout.control_manifest_prefix();
        let legacy_latest_key = layout.backup_manifest_latest();
        let legacy_manifest_prefix = layout.backup_manifest_prefix();
        let mut candidates = Vec::new();
        let mut last_error = None;

        let primary_pointer = match read_optional_remote_object(dependencies, &latest_key).await? {
            Some(pointer) => match String::from_utf8(pointer) {
                Ok(pointer) => {
                    let pointer = pointer.trim();
                    (!pointer.is_empty()).then(|| pointer.to_string())
                }
                Err(error) => {
                    return Err(OpenError::Storage(Self::control_plane_error(
                        "remote manifest latest pointer",
                        format!("decode pointer failed: {error}"),
                    )));
                }
            },
            None => None,
        };
        let legacy_pointer =
            match read_optional_remote_object(dependencies, &legacy_latest_key).await? {
                Some(pointer) => match String::from_utf8(pointer) {
                    Ok(pointer) => {
                        let pointer = pointer.trim();
                        (!pointer.is_empty()).then(|| pointer.to_string())
                    }
                    Err(error) => {
                        last_error = Some(Self::control_plane_error(
                            "legacy remote manifest latest pointer",
                            format!("decode pointer failed: {error}"),
                        ));
                        None
                    }
                },
                None => None,
            };
        if let (Some(primary_pointer), Some(legacy_pointer)) = (&primary_pointer, &legacy_pointer)
            && primary_pointer != legacy_pointer
        {
            return Err(OpenError::Storage(Self::control_plane_mismatch(
                "remote manifest latest pointer",
                &latest_key,
                &legacy_latest_key,
            )));
        }

        let _ = dependencies
            .__failpoint_registry()
            .trigger(
                crate::failpoints::names::DB_REMOTE_MANIFEST_RECOVERY_AFTER_POINTER_READ,
                BTreeMap::from([(
                    "candidate_count".to_string(),
                    usize::from(primary_pointer.is_some() || legacy_pointer.is_some()).to_string(),
                )]),
            )
            .await
            .map_err(OpenError::Storage)?;

        let mut listed = dependencies.object_store.list(&manifest_prefix).await?;
        listed.retain(|key| key != &latest_key && Self::parse_manifest_generation(key).is_some());
        let control_plane_lane_present = primary_pointer.is_some() || !listed.is_empty();
        if !control_plane_lane_present {
            listed = dependencies
                .object_store
                .list(&legacy_manifest_prefix)
                .await?;
            listed.retain(|key| {
                key != &legacy_latest_key && Self::parse_manifest_generation(key).is_some()
            });
        }
        if control_plane_lane_present {
            if let Some(pointer) = primary_pointer.as_ref() {
                candidates.push(pointer.clone());
            }
        } else if let Some(pointer) = legacy_pointer.as_ref() {
            candidates.push(pointer.clone());
        }
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
                Self::control_plane_error(
                    "remote manifest",
                    "recovery found no valid manifest generation",
                )
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
                    file.body.generation,
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
                    file.body.generation,
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
