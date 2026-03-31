use super::*;

impl Db {
    /// Returns a synchronous handle lookup for an already-created table.
    ///
    /// Panics if the table does not exist. Use [`Db::try_table`] when
    /// existence is not guaranteed.
    pub fn table(&self, name: impl Into<String>) -> Table {
        let name = name.into();
        let Some(id) = self.tables_read().get(&name).map(|table| table.id) else {
            let error = Self::missing_table_error(&name);
            panic!("{}", error.message());
        };
        Table {
            db: self.clone(),
            name: Arc::from(name),
            id: Some(id),
        }
    }

    /// Returns a synchronous handle lookup when the table already exists.
    pub fn try_table(&self, name: impl Into<String>) -> Option<Table> {
        let name = name.into();
        let id = self.tables_read().get(&name).map(|table| table.id)?;
        Some(Table {
            db: self.clone(),
            name: Arc::from(name),
            id: Some(id),
        })
    }

    pub async fn create_table(&self, config: TableConfig) -> Result<Table, CreateTableError> {
        Self::validate_table_config(&config)?;

        let _catalog_guard = self.inner.catalog_write_lock.lock().await;
        let name = config.name.clone();
        let next_table_id = self.inner.next_table_id.load(Ordering::SeqCst);
        let id = TableId::new(next_table_id);
        let next_table_id = next_table_id.checked_add(1).ok_or_else(|| {
            CreateTableError::Storage(StorageError::unsupported("table id space exhausted"))
        })?;

        let mut updated_tables = self.tables_read().clone();
        if let Some(existing) = updated_tables.get(&name).cloned() {
            if Self::same_persisted_table_config(&existing.config, &config)
                && (config.merge_operator.is_some() || config.compaction_filter.is_some())
            {
                let mut refreshed = existing.clone();
                refreshed.config.merge_operator = config.merge_operator;
                refreshed.config.compaction_filter = config.compaction_filter;
                updated_tables.insert(name.clone(), refreshed);
                *self.tables_write() = updated_tables;

                return Ok(Table {
                    db: self.clone(),
                    name: Arc::from(name),
                    id: Some(existing.id),
                });
            }
            return Err(CreateTableError::AlreadyExists(name));
        }

        updated_tables.insert(name.clone(), StoredTable { id, config });
        self.persist_tables(&updated_tables).await?;

        self.inner
            .next_table_id
            .store(next_table_id, Ordering::SeqCst);
        *self.tables_write() = updated_tables;

        let table = Table {
            db: self.clone(),
            name: Arc::from(name),
            id: Some(id),
        };
        let _ = self.sync_tiered_backup_catalog().await;
        Ok(table)
    }

    pub async fn snapshot(&self) -> Snapshot {
        let sequence = self.current_sequence();
        self.register_snapshot(sequence)
    }

    pub async fn durable_snapshot(&self) -> Snapshot {
        let sequence = self.current_durable_sequence();
        self.register_snapshot(sequence)
    }

    pub(super) fn register_snapshot(&self, sequence: SequenceNumber) -> Snapshot {
        let registration_id = self.inner.next_snapshot_id.fetch_add(1, Ordering::SeqCst) + 1;
        mutex_lock(&self.inner.snapshot_tracker).register(registration_id, sequence);

        Snapshot {
            registration: Arc::new(SnapshotRegistration {
                db: self.clone(),
                id: registration_id,
                sequence,
                released: AtomicBool::new(false),
            }),
        }
    }

    pub fn current_sequence(&self) -> SequenceNumber {
        SequenceNumber::new(self.inner.current_sequence.load(Ordering::SeqCst))
    }

    pub fn current_durable_sequence(&self) -> SequenceNumber {
        SequenceNumber::new(self.inner.current_durable_sequence.load(Ordering::SeqCst))
    }

    pub fn write_batch(&self) -> WriteBatch {
        WriteBatch::default()
    }

    pub fn read_set(&self) -> ReadSet {
        ReadSet::default()
    }

    pub fn telemetry_db_name(&self) -> String {
        db_name_from_storage(&self.inner.config.storage)
    }

    pub fn telemetry_db_instance(&self) -> String {
        db_instance_from_storage(&self.inner.config.storage)
    }

    pub fn telemetry_storage_mode(&self) -> &'static str {
        storage_mode_name(&self.inner.config.storage)
    }

    pub async fn commit(
        &self,
        batch: WriteBatch,
        opts: CommitOptions,
    ) -> Result<SequenceNumber, CommitError> {
        if batch.is_empty() {
            return Err(CommitError::EmptyBatch);
        }

        let span = tracing::info_span!("terracedb.db.commit");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::OPERATION,
            opentelemetry::Value::String("commit".into()),
        );
        crate::set_span_attribute(&span, "terracedb.batch.operation_count", batch.len() as u64);
        crate::set_span_attribute(
            &span,
            "terracedb.commit.read_set_count",
            opts.read_set
                .as_ref()
                .map(|read_set| read_set.entries().len() as u64)
                .unwrap_or(0),
        );
        let span_for_attrs = span.clone();

        async move {
            let resolved_operations = self.resolve_batch_operations(batch.operations())?;
            let resolved_read_set = self.resolve_read_set_entries(opts.read_set.as_ref())?;
            self.apply_write_backpressure(&resolved_operations).await?;

            let participant = {
                let _commit_guard = self.inner.commit_lock.lock().await;
                self.resolve_failed_provisional_tail_locked()
                    .await
                    .map_err(CommitError::Storage)?;
                self.check_read_conflicts(&resolved_read_set)?;

                let sequence =
                    SequenceNumber::new(self.inner.next_sequence.load(Ordering::SeqCst) + 1);
                let operation_context = opts
                    .operation_context
                    .clone()
                    .or_else(|| Some(OperationContext::current()))
                    .filter(|context| !context.is_empty());
                let record =
                    Self::build_commit_record(sequence, &resolved_operations, operation_context)?;
                let participant = if self.durable_on_commit() {
                    let participant = self.register_commit(sequence, resolved_operations.clone());
                    participant
                        .group_batch
                        .as_ref()
                        .expect("group-commit participants should always have a batch")
                        .stage_record(record);
                    participant
                } else {
                    self.inner
                        .commit_runtime
                        .lock()
                        .await
                        .append(record)
                        .await
                        .map_err(CommitError::Storage)?;
                    self.register_commit(sequence, resolved_operations.clone())
                };
                self.inner
                    .next_sequence
                    .store(sequence.get(), Ordering::SeqCst);
                participant
            };

            crate::set_span_attribute(
                &span_for_attrs,
                crate::telemetry_attrs::SEQUENCE,
                participant.sequence.get(),
            );

            if let Some(batch) = participant.group_batch.clone() {
                if let Err(error) = self.await_group_commit(batch, participant.sequence).await {
                    let cleanup = {
                        let _commit_guard = self.inner.commit_lock.lock().await;
                        self.resolve_failed_provisional_tail_locked().await
                    };
                    return Err(cleanup.map(|_| error).unwrap_or_else(CommitError::Storage));
                }

                self.mark_durable_confirmed(participant.sequence);
                let _ = self.sync_tiered_commit_log_tail().await;
                self.maybe_pause_commit_phase(
                    CommitPhase::AfterDurabilitySync,
                    participant.sequence,
                )
                .await
                .map_err(CommitError::Storage)?;
            }

            self.maybe_pause_commit_phase(CommitPhase::BeforeMemtableInsert, participant.sequence)
                .await
                .map_err(CommitError::Storage)?;
            self.memtables_write()
                .apply(participant.sequence, &participant.operations);
            self.mark_memtable_inserted(participant.sequence);
            self.maybe_pause_commit_phase(CommitPhase::AfterMemtableInsert, participant.sequence)
                .await
                .map_err(CommitError::Storage)?;

            self.publish_watermarks();

            if self.current_sequence() >= participant.sequence {
                self.maybe_pause_commit_phase(
                    CommitPhase::AfterVisibilityPublish,
                    participant.sequence,
                )
                .await
                .map_err(CommitError::Storage)?;
            }
            if self.current_durable_sequence() >= participant.sequence {
                self.maybe_pause_commit_phase(
                    CommitPhase::AfterDurablePublish,
                    participant.sequence,
                )
                .await
                .map_err(CommitError::Storage)?;
            }

            crate::set_span_attribute(
                &span_for_attrs,
                crate::telemetry_attrs::DURABLE_SEQUENCE,
                self.current_durable_sequence().get(),
            );

            Ok(participant.sequence)
        }
        .instrument(span.clone())
        .await
    }

    pub async fn flush(&self) -> Result<(), FlushError> {
        self.flush_internal(true).await
    }

    pub(super) async fn flush_internal(
        &self,
        _allow_scheduler_follow_up: bool,
    ) -> Result<(), FlushError> {
        let span = tracing::info_span!("terracedb.db.flush");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::OPERATION,
            opentelemetry::Value::String("flush".into()),
        );
        let span_for_attrs = span.clone();

        async move {
            let _maintenance_guard = self.inner.maintenance_lock.lock().await;
            match &self.inner.config.storage {
                StorageConfig::Tiered(_) => {
                    let local_root = self
                        .local_storage_root()
                        .expect("tiered storage should have local root")
                        .to_string();
                    let immutables = {
                        let _commit_guard = self.inner.commit_lock.lock().await;
                        self.resolve_failed_provisional_tail_locked()
                            .await
                            .map_err(FlushError::Storage)?;
                        self.memtables_write().rotate_mutable();

                        self.inner
                            .commit_runtime
                            .lock()
                            .await
                            .sync()
                            .await
                            .map_err(FlushError::Storage)?;
                        self.mark_all_commits_durable();
                        self.publish_watermarks();
                        self.memtables_read().immutables.clone()
                    };

                    let mut flushed_count = 0_usize;
                    let mut sstable_state = self.sstables_read().clone();
                    let mut new_live = sstable_state.live.clone();
                    let mut manifest_generation = sstable_state.manifest_generation;

                    for immutable in &immutables {
                        let outputs = self.flush_immutable(&local_root, immutable).await?;
                        if outputs.is_empty() {
                            flushed_count += 1;
                            continue;
                        }

                        new_live.extend(outputs);
                        Self::sort_live_sstables(&mut new_live);
                        manifest_generation =
                            ManifestId::new(manifest_generation.get().saturating_add(1));
                        self.install_manifest(
                            manifest_generation,
                            immutable.max_sequence,
                            &new_live,
                        )
                        .await
                        .map_err(FlushError::Storage)?;
                        flushed_count += 1;
                    }

                    if flushed_count > 0 {
                        let mut memtables = self.memtables_write();
                        memtables.immutables.drain(0..flushed_count);

                        sstable_state.live = new_live;
                        sstable_state.manifest_generation = manifest_generation;
                        sstable_state.last_flushed_sequence =
                            sstable_state.last_flushed_sequence.max(
                                immutables
                                    .iter()
                                    .take(flushed_count)
                                    .map(|immutable| immutable.max_sequence)
                                    .max()
                                    .unwrap_or_default(),
                            );
                        *self.sstables_write() = sstable_state;
                    }

                    let backup_result = if flushed_count > 0 {
                        let state = self.sstables_read().clone();
                        self.sync_tiered_backup_manifest(
                            state.manifest_generation,
                            state.last_flushed_sequence,
                            &state.live,
                        )
                        .await
                    } else {
                        self.sync_tiered_commit_log_tail().await
                    };
                    let _ = backup_result;

                    self.prune_commit_log(true)
                        .await
                        .map_err(FlushError::Storage)?;
                }
                StorageConfig::S3Primary(config) => {
                    let (immutables, buffered_records, mut durable_segments, next_segment_id) = {
                        let _commit_guard = self.inner.commit_lock.lock().await;
                        self.resolve_failed_provisional_tail_locked()
                            .await
                            .map_err(FlushError::Storage)?;
                        self.memtables_write().rotate_mutable();

                        let immutables = self.memtables_read().immutables.clone();
                        let mut commit_runtime = self.inner.commit_runtime.lock().await;
                        commit_runtime.sync().await.map_err(FlushError::Storage)?;
                        match &mut commit_runtime.backend {
                            CommitLogBackend::Memory(log) => (
                                immutables,
                                log.records.clone(),
                                log.durable_commit_log_segments.clone(),
                                log.next_segment_id,
                            ),
                            CommitLogBackend::Local(_) => {
                                return Err(FlushError::Storage(StorageError::unsupported(
                                    "s3-primary flush requires the memory commit-log backend",
                                )));
                            }
                        }
                    };

                    if immutables.is_empty() && buffered_records.is_empty() {
                        return Ok(());
                    }

                    if !buffered_records.is_empty() {
                        let (segment_bytes, footer) = encode_segment_bytes(
                            crate::SegmentId::new(next_segment_id),
                            &buffered_records,
                            SegmentOptions::default().records_per_block,
                        )
                        .map_err(FlushError::Storage)?;
                        let object_key =
                            Self::remote_commit_log_segment_key(config, footer.segment_id);
                        self.inner
                            .dependencies
                            .object_store
                            .put(&object_key, &segment_bytes)
                            .await
                            .map_err(FlushError::Storage)?;
                        durable_segments.push(DurableRemoteCommitLogSegment { object_key, footer });
                        durable_segments.sort_by_key(|segment| segment.footer.segment_id.get());
                    }

                    let mut sstable_state = self.sstables_read().clone();
                    let mut new_live = sstable_state.live.clone();
                    for immutable in &immutables {
                        new_live.extend(self.flush_immutable_remote(config, immutable).await?);
                    }
                    Self::sort_live_sstables(&mut new_live);

                    let flushed_through = buffered_records
                        .last()
                        .map(CommitRecord::sequence)
                        .into_iter()
                        .chain(immutables.iter().map(|immutable| immutable.max_sequence))
                        .max()
                        .unwrap_or(sstable_state.last_flushed_sequence)
                        .max(sstable_state.last_flushed_sequence);
                    let next_generation =
                        ManifestId::new(sstable_state.manifest_generation.get().saturating_add(1));
                    self.install_remote_manifest(
                        config,
                        next_generation,
                        flushed_through,
                        &new_live,
                        &durable_segments,
                    )
                    .await
                    .map_err(FlushError::Storage)?;

                    {
                        let _commit_guard = self.inner.commit_lock.lock().await;
                        let mut commit_runtime = self.inner.commit_runtime.lock().await;
                        match &mut commit_runtime.backend {
                            CommitLogBackend::Memory(log) => {
                                Arc::make_mut(&mut log.records)
                                    .retain(|record| record.sequence() > flushed_through);
                                log.durable_commit_log_segments = durable_segments.clone();
                                if !buffered_records.is_empty() {
                                    log.next_segment_id = next_segment_id.saturating_add(1);
                                }
                            }
                            CommitLogBackend::Local(_) => {
                                return Err(FlushError::Storage(StorageError::unsupported(
                                    "s3-primary flush requires the memory commit-log backend",
                                )));
                            }
                        }

                        if !immutables.is_empty() {
                            let mut memtables = self.memtables_write();
                            memtables.immutables.drain(0..immutables.len());
                        }

                        self.mark_commits_durable_through(flushed_through);
                        self.publish_watermarks();
                    }

                    sstable_state.live = new_live;
                    sstable_state.manifest_generation = next_generation;
                    sstable_state.last_flushed_sequence = flushed_through;
                    *self.sstables_write() = sstable_state;
                }
            }

            crate::set_span_attribute(
                &span_for_attrs,
                crate::telemetry_attrs::DURABLE_SEQUENCE,
                self.current_durable_sequence().get(),
            );
            Ok(())
        }
        .instrument(span.clone())
        .await
    }

    pub async fn scan_since(
        &self,
        table: &Table,
        cursor: LogCursor,
        opts: ScanOptions,
    ) -> Result<ChangeStream, ChangeFeedError> {
        self.scan_change_feed(table, cursor, self.current_sequence(), opts)
            .await
    }

    pub fn subscribe(&self, table: &Table) -> WatermarkReceiver {
        self.inner.visible_watchers.subscribe(table.name())
    }

    pub async fn scan_durable_since(
        &self,
        table: &Table,
        cursor: LogCursor,
        opts: ScanOptions,
    ) -> Result<ChangeStream, ChangeFeedError> {
        self.scan_change_feed(table, cursor, self.current_durable_sequence(), opts)
            .await
    }

    pub fn subscribe_durable(&self, table: &Table) -> WatermarkReceiver {
        self.inner.durable_watchers.subscribe(table.name())
    }

    pub fn subscribe_visible_set<'a, I>(&self, tables: I) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        Self::subscribe_set(&self.inner.visible_watchers, tables)
    }

    pub fn subscribe_durable_set<'a, I>(&self, tables: I) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        Self::subscribe_set(&self.inner.durable_watchers, tables)
    }

    pub async fn table_stats(&self, table: &Table) -> TableStats {
        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let current_sequence = self.current_sequence();
        let recovery_floor_sequence = self.sstables_read().last_flushed_sequence;
        let cdc_gc_min_sequence = self.cdc_gc_min_sequence(current_sequence);
        let commit_log_gc_floor_sequence = cdc_gc_min_sequence
            .map(|cdc_min| recovery_floor_sequence.min(cdc_min))
            .unwrap_or(recovery_floor_sequence);
        let oldest_active_snapshot_sequence = self.oldest_active_snapshot_sequence();
        let active_snapshot_count = self.active_snapshot_count();
        let metadata = tables
            .get(table.name())
            .map(|table| table.config.metadata.clone())
            .unwrap_or_default();
        let (
            l0_sstable_count,
            total_bytes,
            local_bytes,
            compaction_debt,
            compaction_filter_removed_bytes,
            compaction_filter_removed_keys,
            pending_flush_bytes,
            immutable_memtable_count,
            history_retention_floor_sequence,
            history_gc_horizon_sequence,
            history_pinned_by_snapshots,
        ) = table
            .resolve_id()
            .map(|table_id| {
                let memtables = self.memtables_read();
                let (l0_sstable_count, total_bytes, local_bytes) = SstableState {
                    manifest_generation: ManifestId::default(),
                    last_flushed_sequence: SequenceNumber::default(),
                    live: live.clone(),
                }
                .table_stats(table_id);
                let compaction_debt = tables
                    .values()
                    .find(|stored| stored.id == table_id)
                    .map(|stored| Self::table_compaction_state(stored, &live).compaction_debt)
                    .unwrap_or_default();
                let compaction_filter_stats = self.compaction_filter_stats(table_id);
                let history_retention_floor_sequence =
                    self.history_retention_floor_sequence(table_id);
                let history_gc_horizon_sequence = self.history_gc_horizon(table_id);
                let history_pinned_by_snapshots = history_retention_floor_sequence
                    .zip(oldest_active_snapshot_sequence)
                    .is_some_and(|(retention_floor, oldest_snapshot)| {
                        oldest_snapshot < retention_floor
                    });

                (
                    l0_sstable_count,
                    total_bytes,
                    local_bytes,
                    compaction_debt,
                    compaction_filter_stats.removed_bytes,
                    compaction_filter_stats.removed_keys,
                    memtables.pending_flush_bytes(table_id),
                    memtables.immutable_memtable_count(table_id),
                    history_retention_floor_sequence,
                    history_gc_horizon_sequence,
                    history_pinned_by_snapshots,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0, 0, 0, 0, None, None, false));
        let (
            change_feed_oldest_available_sequence,
            change_feed_floor_sequence,
            change_feed_pins_commit_log_gc,
        ) = if let Some(table_id) = table.resolve_id() {
            let table_watermark = self.table_change_feed_watermark(table_id);
            let runtime = self.inner.commit_runtime.lock().await;
            let logical_floor = self.cdc_retention_floor_sequence(table_id, current_sequence);
            let floor = self.change_feed_floor_from_state(
                table_id,
                current_sequence,
                runtime.oldest_sequence_for_table(table_id),
                runtime.oldest_segment_id(),
                table_watermark,
            );
            let pins =
                logical_floor
                    .zip(cdc_gc_min_sequence)
                    .is_some_and(|(table_floor, gc_min)| {
                        table_floor == gc_min && gc_min <= recovery_floor_sequence
                    });
            (runtime.oldest_sequence_for_table(table_id), floor, pins)
        } else {
            (None, None, false)
        };

        TableStats {
            l0_sstable_count,
            total_bytes,
            local_bytes,
            s3_bytes: total_bytes.saturating_sub(local_bytes),
            compaction_debt,
            compaction_filter_removed_bytes,
            compaction_filter_removed_keys,
            pending_flush_bytes,
            immutable_memtable_count,
            history_retention_floor_sequence,
            history_gc_horizon_sequence,
            oldest_active_snapshot_sequence,
            active_snapshot_count,
            history_pinned_by_snapshots,
            change_feed_oldest_available_sequence,
            change_feed_floor_sequence,
            commit_log_recovery_floor_sequence: recovery_floor_sequence,
            commit_log_gc_floor_sequence,
            change_feed_pins_commit_log_gc,
            current_state_retention: None,
            metadata,
        }
    }

    pub async fn pending_work(&self) -> Vec<PendingWork> {
        self.pending_work_candidates()
            .into_iter()
            .map(|candidate| candidate.pending)
            .collect()
    }

    pub(super) async fn apply_write_backpressure(
        &self,
        operations: &[ResolvedBatchOperation],
    ) -> Result<(), CommitError> {
        let batch_bytes_by_table = Self::estimated_batch_bytes_by_table(operations);
        let total_batch_bytes = batch_bytes_by_table.values().copied().sum::<u64>();
        if self.memtable_budget_exceeded_by(total_batch_bytes) {
            self.record_forced_execution();
            self.record_forced_flush();
            self.flush_internal(false)
                .await
                .map_err(|error| CommitError::Storage(Self::flush_error_into_storage(error)))?;
        }

        let touched_tables = operations
            .iter()
            .map(|operation| {
                (
                    operation.table_id,
                    Table {
                        db: self.clone(),
                        name: Arc::from(operation.table_name.clone()),
                        id: Some(operation.table_id),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();

        loop {
            let mut max_delay = Duration::ZERO;
            let mut should_run_maintenance = false;
            let mut must_stall = false;

            for table in &touched_tables {
                let stats = self.table_stats(table).await;
                let decision = self.inner.scheduler.should_throttle(table, &stats);
                let table_bytes = table
                    .id()
                    .and_then(|table_id| batch_bytes_by_table.get(&table_id).copied())
                    .unwrap_or(total_batch_bytes);

                if let Some(rate) = decision.max_write_bytes_per_second {
                    max_delay = max_delay.max(Self::throttle_delay(table_bytes, rate));
                }
                if decision.throttle {
                    should_run_maintenance = true;
                }
                if decision.stall || stats.l0_sstable_count >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
                {
                    should_run_maintenance = true;
                    must_stall = true;
                }
            }

            if should_run_maintenance {
                let progressed = self
                    .run_scheduler_pass(true)
                    .await
                    .map_err(CommitError::Storage)?;
                if progressed {
                    continue;
                }
                if must_stall {
                    break;
                }
            }

            if max_delay > Duration::ZERO {
                self.inner.dependencies.clock.sleep(max_delay).await;
            }
            break;
        }

        Ok(())
    }

    pub(super) fn memtable_budget_bytes(&self) -> Option<u64> {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => Some(config.max_local_bytes),
            StorageConfig::S3Primary(_) => None,
        }
    }

    pub(super) fn local_sstable_budget_bytes(&self) -> Option<u64> {
        self.memtable_budget_bytes()
    }

    pub(super) fn local_sstable_retention(&self) -> TieredLocalRetentionMode {
        match &self.inner.config.storage {
            StorageConfig::Tiered(config) => config.local_retention,
            StorageConfig::S3Primary(_) => TieredLocalRetentionMode::Offload,
        }
    }

    pub(super) fn memtable_budget_exceeded_by(&self, additional_bytes: u64) -> bool {
        self.memtable_budget_bytes().is_some_and(|budget| {
            self.memtables_read()
                .total_pending_flush_bytes()
                .saturating_add(additional_bytes)
                > budget
        })
    }

    pub(super) fn estimated_batch_bytes_by_table(
        operations: &[ResolvedBatchOperation],
    ) -> BTreeMap<TableId, u64> {
        let mut bytes_by_table = BTreeMap::new();
        for operation in operations {
            let entry_bytes = Self::estimated_operation_size_bytes(operation);
            bytes_by_table
                .entry(operation.table_id)
                .and_modify(|bytes: &mut u64| *bytes = bytes.saturating_add(entry_bytes))
                .or_insert(entry_bytes);
        }
        bytes_by_table
    }

    pub(super) fn estimated_operation_size_bytes(operation: &ResolvedBatchOperation) -> u64 {
        (operation.key.len()
            + encode_mvcc_key(&operation.key, CommitId::new(SequenceNumber::default())).len()
            + operation
                .value
                .as_ref()
                .map(value_size_bytes)
                .unwrap_or_default()) as u64
    }

    pub(super) fn throttle_delay(bytes: u64, max_write_bytes_per_second: u64) -> Duration {
        if bytes == 0 || max_write_bytes_per_second == 0 {
            return Duration::ZERO;
        }

        let millis = ((bytes as u128).saturating_mul(1000))
            .div_ceil(max_write_bytes_per_second as u128) as u64;
        Duration::from_millis(millis)
    }

    pub(super) fn flush_error_into_storage(error: FlushError) -> StorageError {
        match error {
            FlushError::Storage(error) => error,
            FlushError::Unimplemented(message) => StorageError::unsupported(message),
        }
    }

    pub(super) fn missing_table_error(name: &str) -> StorageError {
        StorageError::not_found(format!("table does not exist: {name}"))
    }

    pub(super) fn resolve_read_set_entries(
        &self,
        read_set: Option<&ReadSet>,
    ) -> Result<Vec<ResolvedReadSetEntry>, CommitError> {
        read_set
            .into_iter()
            .flat_map(ReadSet::entries)
            .map(|entry| {
                let table_id = self.resolve_table_id(&entry.table).ok_or_else(|| {
                    CommitError::Storage(Self::missing_table_error(entry.table.name()))
                })?;
                Ok(ResolvedReadSetEntry {
                    table_id,
                    key: entry.key.clone(),
                    at_sequence: entry.at_sequence,
                })
            })
            .collect()
    }

    pub(super) fn check_read_conflicts(
        &self,
        read_set: &[ResolvedReadSetEntry],
    ) -> Result<(), CommitError> {
        let coordinator = mutex_lock(&self.inner.commit_coordinator);
        for entry in read_set {
            let conflict_key = CommitConflictKey {
                table_id: entry.table_id,
                key: entry.key.clone(),
            };
            let changed = coordinator
                .keys
                .get(&conflict_key)
                .and_then(|sequences| sequences.iter().next_back().copied())
                .is_some_and(|sequence| sequence > entry.at_sequence);
            if changed {
                return Err(CommitError::Conflict);
            }
        }

        Ok(())
    }

    pub(super) fn build_commit_record(
        sequence: SequenceNumber,
        operations: &[ResolvedBatchOperation],
        operation_context: Option<OperationContext>,
    ) -> Result<CommitRecord, CommitError> {
        let entries = operations
            .iter()
            .enumerate()
            .map(|(index, operation)| {
                let op_index = u16::try_from(index).map_err(|_| {
                    CommitError::Storage(StorageError::unsupported(
                        "write batch contains more than 65535 operations",
                    ))
                })?;
                Ok(CommitEntry {
                    op_index,
                    table_id: operation.table_id,
                    kind: operation.kind,
                    key: operation.key.clone(),
                    value: operation.value.clone(),
                    operation_context: operation_context.clone(),
                })
            })
            .collect::<Result<Vec<_>, CommitError>>()?;

        Ok(CommitRecord {
            id: CommitId::new(sequence),
            entries,
        })
    }

    pub(super) fn register_commit(
        &self,
        sequence: SequenceNumber,
        operations: Vec<ResolvedBatchOperation>,
    ) -> CommitParticipant {
        let touched_tables = operations
            .iter()
            .map(|operation| operation.table_name.clone())
            .collect::<BTreeSet<_>>();
        let conflict_keys = operations
            .iter()
            .map(|operation| CommitConflictKey {
                table_id: operation.table_id,
                key: operation.key.clone(),
            })
            .collect::<Vec<_>>();

        let group_batch = {
            let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
            for key in &conflict_keys {
                coordinator
                    .keys
                    .entry(key.clone())
                    .or_default()
                    .insert(sequence);
            }
            let group_batch = if self.durable_on_commit() {
                let batch = match coordinator.current_batch.as_ref() {
                    Some(batch) if batch.is_open() => batch.clone(),
                    _ => {
                        let predecessor = coordinator
                            .current_batch
                            .as_ref()
                            .filter(|batch| batch.result().is_none())
                            .cloned();
                        let batch = Arc::new(GroupCommitBatch::new(sequence, predecessor));
                        coordinator.current_batch = Some(batch.clone());
                        batch
                    }
                };
                Some(batch)
            } else {
                None
            };
            coordinator.sequences.insert(
                sequence,
                SequenceCommitState {
                    touched_tables: touched_tables.clone(),
                    conflict_keys: conflict_keys.clone(),
                    group_batch: group_batch.clone(),
                    ..SequenceCommitState::default()
                },
            );

            group_batch
        };

        CommitParticipant {
            sequence,
            operations,
            group_batch,
        }
    }

    pub(super) fn mark_memtable_inserted(&self, sequence: SequenceNumber) {
        if let Some(state) = mutex_lock(&self.inner.commit_coordinator)
            .sequences
            .get_mut(&sequence)
        {
            state.memtable_inserted = true;
        }
    }

    pub(super) fn mark_durable_confirmed(&self, sequence: SequenceNumber) {
        if let Some(state) = mutex_lock(&self.inner.commit_coordinator)
            .sequences
            .get_mut(&sequence)
        {
            state.durable_confirmed = true;
        }
    }

    pub(super) fn mark_all_commits_durable(&self) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        for state in coordinator.sequences.values_mut() {
            if !state.aborted {
                state.durable_confirmed = true;
            }
        }
    }

    pub(super) fn mark_commits_durable_through(&self, upper_bound: SequenceNumber) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        for (&sequence, state) in coordinator.sequences.iter_mut() {
            if !state.aborted && sequence <= upper_bound {
                state.durable_confirmed = true;
            }
        }
    }

    pub(super) fn note_failed_provisional_tail(
        &self,
        start_sequence: SequenceNumber,
        error: StorageError,
    ) {
        let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
        match coordinator.pending_failed_tail.as_ref() {
            Some(pending) if pending.start_sequence <= start_sequence => {}
            _ => {
                coordinator.pending_failed_tail = Some(PendingFailedTail {
                    start_sequence,
                    error,
                });
            }
        }
    }

    pub(super) fn pending_failed_provisional_tail_error(
        &self,
        sequence: SequenceNumber,
    ) -> Option<StorageError> {
        mutex_lock(&self.inner.commit_coordinator)
            .pending_failed_tail
            .as_ref()
            .filter(|pending| pending.start_sequence <= sequence)
            .map(|pending| pending.error.clone())
    }

    pub(super) async fn resolve_failed_provisional_tail_locked(&self) -> Result<(), StorageError> {
        let (pending, affected_sequences, affected_batches) = {
            let coordinator = mutex_lock(&self.inner.commit_coordinator);
            let Some(pending) = coordinator.pending_failed_tail.clone() else {
                return Ok(());
            };

            let mut seen_batches = BTreeSet::new();
            let mut affected_batches = Vec::new();
            let affected_sequences = coordinator
                .sequences
                .range(pending.start_sequence..)
                .map(|(&sequence, state)| {
                    if let Some(batch) = state.group_batch.as_ref() {
                        let batch_ptr = Arc::as_ptr(batch) as usize;
                        if seen_batches.insert(batch_ptr) {
                            affected_batches.push(batch.clone());
                        }
                    }
                    sequence
                })
                .collect::<Vec<_>>();

            (pending, affected_sequences, affected_batches)
        };

        {
            let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
            if coordinator
                .pending_failed_tail
                .as_ref()
                .is_some_and(|current| current.start_sequence != pending.start_sequence)
            {
                return Ok(());
            }

            for sequence in &affected_sequences {
                let Some(state) = coordinator.sequences.remove(sequence) else {
                    continue;
                };
                for key in state.conflict_keys {
                    let remove_key = if let Some(sequences) = coordinator.keys.get_mut(&key) {
                        sequences.remove(sequence);
                        sequences.is_empty()
                    } else {
                        false
                    };
                    if remove_key {
                        coordinator.keys.remove(&key);
                    }
                }
            }

            coordinator.pending_failed_tail = None;
            coordinator.current_batch = None;
        }

        self.inner.next_sequence.store(
            pending.start_sequence.get().saturating_sub(1),
            Ordering::SeqCst,
        );
        for batch in affected_batches {
            batch.finish(Err(pending.error.clone()));
        }

        Ok(())
    }

    pub(super) async fn await_group_commit(
        &self,
        batch: Arc<GroupCommitBatch>,
        sequence: SequenceNumber,
    ) -> Result<(), CommitError> {
        loop {
            let notified = batch.notify.notified();
            if let Some(result) = batch.result() {
                return result.map_err(CommitError::Storage);
            }
            if let Some(error) = self.pending_failed_provisional_tail_error(sequence) {
                batch.finish(Err(error.clone()));
                return Err(CommitError::Storage(error));
            }

            self.maybe_pause_commit_phase(CommitPhase::BeforeDurabilitySync, sequence)
                .await
                .map_err(CommitError::Storage)?;

            let should_lead = {
                let _commit_guard = self.inner.commit_lock.lock().await;
                let mut state = mutex_lock(&batch.state);
                if state.result.is_some() {
                    false
                } else if !state.sealed {
                    state.sealed = true;
                    true
                } else {
                    false
                }
            };

            if should_lead {
                self.maybe_pause_commit_phase(CommitPhase::AfterBatchSeal, sequence)
                    .await
                    .map_err(CommitError::Storage)?;
                if let Some(predecessor) = batch.predecessor()
                    && let Err(error) = predecessor.await_result().await
                {
                    batch.finish(Err(error.clone()));
                    return Err(CommitError::Storage(error));
                }
                if let Some(error) = self.pending_failed_provisional_tail_error(sequence) {
                    batch.finish(Err(error.clone()));
                    return Err(CommitError::Storage(error));
                }
                let records = batch.staged_records();
                let result = self
                    .inner
                    .commit_runtime
                    .lock()
                    .await
                    .append_group_batch(&records)
                    .await;
                if let Err(error) = result.as_ref() {
                    self.note_failed_provisional_tail(batch.first_sequence(), error.clone());
                }
                batch.finish(result.clone());
                return result.map_err(CommitError::Storage);
            }

            notified.await;
        }
    }

    pub(super) fn publish_watermarks(&self) {
        let advance = {
            let mut coordinator = mutex_lock(&self.inner.commit_coordinator);
            let mut advance = WatermarkAdvance::default();
            let mut visible = self.current_sequence();
            let mut durable = self.current_durable_sequence();

            loop {
                let next = SequenceNumber::new(visible.get() + 1);
                let Some(state) = coordinator.sequences.get_mut(&next) else {
                    break;
                };

                if state.aborted {
                    state.visible_published = true;
                    visible = next;
                    advance.visible_sequence = Some(visible);
                    continue;
                }
                if !state.memtable_inserted {
                    break;
                }

                state.visible_published = true;
                visible = next;
                advance.visible_sequence = Some(visible);
                Self::record_table_notifications(
                    &mut advance.visible_tables,
                    &state.touched_tables,
                    visible,
                );
            }

            loop {
                let next = SequenceNumber::new(durable.get() + 1);
                if next > visible {
                    break;
                }

                let Some(state) = coordinator.sequences.get_mut(&next) else {
                    break;
                };

                if state.aborted {
                    state.durable_published = true;
                    durable = next;
                    advance.durable_sequence = Some(durable);
                    continue;
                }
                if !state.durable_confirmed {
                    break;
                }

                state.durable_published = true;
                durable = next;
                advance.durable_sequence = Some(durable);
                Self::record_table_notifications(
                    &mut advance.durable_tables,
                    &state.touched_tables,
                    durable,
                );
            }

            coordinator
                .sequences
                .retain(|_, state| !state.visible_published || !state.durable_published);

            advance
        };

        if let Some(sequence) = advance.visible_sequence {
            self.inner
                .current_sequence
                .store(sequence.get(), Ordering::SeqCst);
        }
        if let Some(sequence) = advance.durable_sequence {
            self.inner
                .current_durable_sequence
                .store(sequence.get(), Ordering::SeqCst);
        }

        self.notify_table_sequences(&self.inner.visible_watchers, &advance.visible_tables);
        self.notify_table_sequences(&self.inner.durable_watchers, &advance.durable_tables);
    }

    pub(super) fn record_table_notifications(
        notifications: &mut BTreeMap<String, SequenceNumber>,
        tables: &BTreeSet<String>,
        sequence: SequenceNumber,
    ) {
        for table in tables {
            notifications.insert(table.clone(), sequence);
        }
    }

    pub(super) fn notify_table_sequences(
        &self,
        watchers: &Arc<WatermarkRegistry>,
        updates: &BTreeMap<String, SequenceNumber>,
    ) {
        watchers.notify(updates);
    }

    pub(super) fn durable_on_commit(&self) -> bool {
        matches!(
            &self.inner.config.storage,
            StorageConfig::Tiered(TieredStorageConfig {
                durability: TieredDurabilityMode::GroupCommit,
                ..
            })
        )
    }

    pub(super) async fn scan_change_feed(
        &self,
        table: &Table,
        cursor: LogCursor,
        upper_bound: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<ChangeStream, ChangeFeedError> {
        let span = tracing::debug_span!("terracedb.db.change_feed.scan");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        apply_table_span_attribute(&span, table.name());
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::LOG_CURSOR,
            opentelemetry::Value::String(log_cursor_attribute(cursor).into()),
        );
        crate::set_span_attribute(&span, crate::telemetry_attrs::SEQUENCE, upper_bound.get());
        if let Some(limit) = opts.limit {
            crate::set_span_attribute(&span, "terracedb.change_feed.limit", limit as u64);
        }

        async move {
            let Some(table_id) = self.resolve_table_id(table) else {
                return Err(Self::missing_table_error(table.name()).into());
            };
            if cursor.sequence() > upper_bound || matches!(opts.limit, Some(0)) {
                return Ok(Box::pin(stream::empty()) as ChangeStream);
            }
            let table_watermark = self.table_change_feed_watermark(table_id);

            let table_handle = Table {
                db: self.clone(),
                name: Arc::from(table.name()),
                id: Some(table_id),
            };
            let sources = {
                let runtime = self.inner.commit_runtime.lock().await;
                let floor = self.change_feed_floor_from_state(
                    table_id,
                    upper_bound,
                    runtime.oldest_sequence_for_table(table_id),
                    runtime.oldest_segment_id(),
                    table_watermark,
                );
                if let Some(oldest_available) = floor
                    && cursor.sequence() < oldest_available
                {
                    return Err(ChangeFeedError::SnapshotTooOld(SnapshotTooOld {
                        requested: cursor.sequence(),
                        oldest_available,
                    }));
                }

                runtime.change_feed_scan_plan(table_id, cursor.sequence())
            };

            let scan_guard = self.register_commit_log_scan(&pinned_segment_ids(&sources));
            let scanner = ChangeFeedScanner::new(
                table_id,
                table_handle,
                cursor,
                upper_bound,
                opts.limit,
                scan_guard,
                sources,
            );
            Ok(
                Box::pin(stream::try_unfold(scanner, |mut scanner| async move {
                    match scanner.next_change().await? {
                        Some(entry) => Ok(Some((entry, scanner))),
                        None => Ok(None),
                    }
                })) as ChangeStream,
            )
        }
        .instrument(span.clone())
        .await
    }

    #[cfg(test)]
    pub(super) fn block_next_commit_phase(&self, phase: CommitPhase) -> CommitPhaseBlocker {
        CommitPhaseBlocker {
            handle: self.inner.dependencies.__failpoint_registry().arm_pause(
                phase.failpoint_name(),
                crate::failpoints::FailpointMode::Once,
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn block_next_compaction_phase(
        &self,
        phase: CompactionPhase,
    ) -> CompactionPhaseBlocker {
        CompactionPhaseBlocker {
            handle: self.inner.dependencies.__failpoint_registry().arm_pause(
                phase.failpoint_name(),
                crate::failpoints::FailpointMode::Once,
            ),
        }
    }

    #[cfg(test)]
    pub(super) fn block_next_offload_phase(&self, phase: OffloadPhase) -> OffloadPhaseBlocker {
        OffloadPhaseBlocker {
            handle: self.inner.dependencies.__failpoint_registry().arm_pause(
                phase.failpoint_name(),
                crate::failpoints::FailpointMode::Once,
            ),
        }
    }

    pub(super) async fn maybe_pause_commit_phase(
        &self,
        phase: CommitPhase,
        sequence: SequenceNumber,
    ) -> Result<(), StorageError> {
        let metadata = BTreeMap::from([("sequence".to_string(), sequence.get().to_string())]);
        let _ = self
            .__run_failpoint(phase.failpoint_name(), metadata)
            .await?;
        Ok(())
    }

    pub(super) async fn maybe_pause_compaction_phase(
        &self,
        phase: CompactionPhase,
    ) -> Result<(), StorageError> {
        let _ = self
            .__run_failpoint(phase.failpoint_name(), BTreeMap::new())
            .await?;
        Ok(())
    }

    pub(super) async fn maybe_pause_offload_phase(
        &self,
        phase: OffloadPhase,
    ) -> Result<(), StorageError> {
        let _ = self
            .__run_failpoint(phase.failpoint_name(), BTreeMap::new())
            .await?;
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn dependencies(&self) -> &DbDependencies {
        &self.inner.dependencies
    }

    #[doc(hidden)]
    pub fn __failpoint_registry(&self) -> Arc<crate::failpoints::FailpointRegistry> {
        self.inner.dependencies.__failpoint_registry()
    }

    #[doc(hidden)]
    pub async fn __run_failpoint(
        &self,
        name: &str,
        metadata: BTreeMap<String, String>,
    ) -> Result<crate::failpoints::FailpointOutcome, StorageError> {
        self.__failpoint_registry().trigger(name, metadata).await
    }

    pub(super) fn resolve_batch_operations(
        &self,
        operations: &[BatchOperation],
    ) -> Result<Vec<ResolvedBatchOperation>, CommitError> {
        operations
            .iter()
            .map(|operation| match operation {
                BatchOperation::Put { table, key, value } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(Self::missing_table_error(table.name()))
                    })?;
                    let value = Self::normalize_value_for_table(&stored, value)
                        .map_err(CommitError::Storage)?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Put,
                        value: Some(value),
                    })
                }
                BatchOperation::Merge { table, key, value } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(Self::missing_table_error(table.name()))
                    })?;
                    if stored.config.merge_operator.is_none() {
                        return Err(CommitError::Storage(StorageError::unsupported(format!(
                            "merge operator is not configured for table {}",
                            table.name()
                        ))));
                    }
                    let value = Self::normalize_merge_operand_for_table(&stored, value)
                        .map_err(CommitError::Storage)?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Merge,
                        value: Some(value),
                    })
                }
                BatchOperation::Delete { table, key } => {
                    let stored = self.resolve_stored_table(table).ok_or_else(|| {
                        CommitError::Storage(Self::missing_table_error(table.name()))
                    })?;
                    Ok(ResolvedBatchOperation {
                        table_id: stored.id,
                        table_name: table.name().to_string(),
                        key: key.clone(),
                        kind: ChangeKind::Delete,
                        value: None,
                    })
                }
            })
            .collect()
    }

    pub(super) fn resolve_stored_table(&self, table: &Table) -> Option<StoredTable> {
        let table_id = self.resolve_table_id(table)?;
        Self::stored_table_by_id(&self.tables_read(), table_id).cloned()
    }

    pub(super) fn resolve_table_id(&self, table: &Table) -> Option<TableId> {
        let tables = self.tables_read();
        let stored = tables.get(table.name())?;
        if let Some(id) = table.id
            && id != stored.id
        {
            return None;
        }
        Some(stored.id)
    }

    pub(super) fn register_commit_log_scan(&self, segment_ids: &[SegmentId]) -> CommitLogScanGuard {
        mutex_lock(&self.inner.commit_log_scans).register(segment_ids);
        CommitLogScanGuard {
            db: Arc::downgrade(&self.inner),
            segment_ids: segment_ids.to_vec(),
        }
    }

    pub(super) fn stored_table_by_id(
        tables: &BTreeMap<String, StoredTable>,
        table_id: TableId,
    ) -> Option<&StoredTable> {
        tables.values().find(|stored| stored.id == table_id)
    }

    pub(super) fn max_merge_operand_chain_length(config: &TableConfig) -> usize {
        config
            .max_merge_operand_chain_length
            .map(|limit| limit.max(1) as usize)
            .unwrap_or(DEFAULT_MAX_MERGE_OPERAND_CHAIN_LENGTH)
    }

    pub(super) fn visible_row_priority(kind: ChangeKind) -> u8 {
        match kind {
            ChangeKind::Merge => 1,
            ChangeKind::Put | ChangeKind::Delete => 0,
        }
    }

    pub(super) fn collapse_merge_operands(
        operator: &dyn crate::config::MergeOperator,
        key: &[u8],
        operands: &[Value],
    ) -> Result<Vec<Value>, StorageError> {
        let mut collapsed = Vec::with_capacity(operands.len());
        for operand in operands.iter().cloned() {
            collapsed.push(operand);
            while collapsed.len() >= 2 {
                let right = collapsed.pop().expect("right operand should exist");
                let left = collapsed.pop().expect("left operand should exist");
                match operator.partial_merge(key, &left, &right)? {
                    Some(merged) => collapsed.push(merged),
                    None => {
                        collapsed.push(left);
                        collapsed.push(right);
                        break;
                    }
                }
            }
        }

        Ok(collapsed)
    }

    pub(super) fn resolve_visible_value_with_state(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<VisibleValueResolution, StorageError> {
        let table = Self::stored_table_by_id(tables, table_id).ok_or_else(|| {
            StorageError::not_found(format!("table id {} is not registered", table_id.get()))
        })?;
        let mut rows = Vec::new();
        memtables.collect_visible_rows(table_id, key, sequence, &mut rows);
        sstables.collect_visible_rows(table_id, key, sequence, &mut rows);
        Self::resolve_visible_value_from_rows(table, key, &rows)
    }

    pub(super) fn resolve_visible_value_from_rows(
        table: &StoredTable,
        key: &[u8],
        rows: &[SstableRow],
    ) -> Result<VisibleValueResolution, StorageError> {
        let mut rows = rows.iter().collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            right.sequence.cmp(&left.sequence).then_with(|| {
                Self::visible_row_priority(left.kind).cmp(&Self::visible_row_priority(right.kind))
            })
        });

        let Some(head) = rows.first() else {
            return Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            });
        };

        match head.kind {
            ChangeKind::Put => Ok(VisibleValueResolution {
                value: Some(
                    head.value
                        .clone()
                        .ok_or_else(|| StorageError::corruption("put row is missing a value"))?,
                ),
                collapse: None,
            }),
            ChangeKind::Delete => Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            }),
            ChangeKind::Merge => {
                let operator = table.config.merge_operator.as_ref().ok_or_else(|| {
                    StorageError::unsupported(format!(
                        "merge operator is not configured for table {}",
                        table.config.name
                    ))
                })?;
                let mut operands = Vec::new();
                let mut existing = None;
                for row in &rows {
                    match row.kind {
                        ChangeKind::Merge => operands.push(row.value.clone().ok_or_else(|| {
                            StorageError::corruption("merge row is missing an operand value")
                        })?),
                        ChangeKind::Put => {
                            existing = Some(row.value.as_ref().ok_or_else(|| {
                                StorageError::corruption("put row is missing a value")
                            })?);
                            break;
                        }
                        ChangeKind::Delete => break,
                    }
                }
                operands.reverse();
                let collapsed = Self::collapse_merge_operands(operator.as_ref(), key, &operands)?;
                let value = operator.full_merge(key, existing, &collapsed)?;
                let collapse = (operands.len()
                    > Self::max_merge_operand_chain_length(&table.config))
                .then(|| MergeCollapse {
                    sequence: head.sequence,
                    value: value.clone(),
                });

                Ok(VisibleValueResolution {
                    value: Some(value),
                    collapse,
                })
            }
        }
    }

    pub(super) fn force_collapse_merge_chain(
        &self,
        table_id: TableId,
        key: &[u8],
        collapse: MergeCollapse,
    ) {
        self.memtables_write().force_collapse(
            table_id,
            key.to_vec(),
            collapse.sequence,
            collapse.value,
        );
    }

    pub(super) fn scan_visible_row_with_state(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Result<RowScanResult, StorageError> {
        let table = Self::stored_table_by_id(tables, table_id).ok_or_else(|| {
            StorageError::not_found(format!("table id {} is not registered", table_id.get()))
        })?;
        let limit = opts.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(RowScanResult::default());
        }

        let direction = if opts.reverse {
            ScanDirection::Reverse
        } else {
            ScanDirection::Forward
        };
        let mut iterator =
            MergedRowRangeIterator::new(memtables, sstables, table_id, &matcher, direction);
        let mut result = RowScanResult::default();

        while result.rows.len() < limit {
            let Some((key, rows)) = iterator.next_key_rows(sequence) else {
                break;
            };
            result.visited_key_groups = result.visited_key_groups.saturating_add(1);

            let resolution = Self::resolve_visible_value_from_rows(table, &key, &rows)?;
            if let Some(collapse) = resolution.collapse {
                result.collapses.push((key.clone(), collapse));
            }
            if let Some(value) = resolution.value {
                result.rows.push((key, value));
            }
        }

        Ok(result)
    }

    pub(super) fn read_visible_value_row(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, StorageError> {
        let resolution = {
            let tables = self.tables_read();
            let memtables = self.memtables_read();
            let sstables = self.sstables_read();
            Self::resolve_visible_value_with_state(
                &tables, &memtables, &sstables, table_id, key, sequence,
            )?
        };

        if let Some(collapse) = resolution.collapse.clone() {
            self.force_collapse_merge_chain(table_id, key, collapse);
        }

        Ok(resolution.value)
    }

    pub(super) fn scan_visible_row(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let result = {
            let tables = self.tables_read();
            let memtables = self.memtables_read();
            let sstables = self.sstables_read();
            Self::scan_visible_row_with_state(
                &tables, &memtables, &sstables, table_id, sequence, matcher, opts,
            )?
        };

        for (key, collapse) in result.collapses {
            self.force_collapse_merge_chain(table_id, &key, collapse);
        }

        Ok(result.rows)
    }

    pub(super) fn resolve_scan_projection(
        table: &StoredTable,
        requested_columns: Option<&[String]>,
    ) -> Result<Option<ColumnProjection>, StorageError> {
        let Some(schema) = table.config.schema.as_ref() else {
            return Ok(None);
        };

        let fields = if let Some(requested_columns) = requested_columns {
            let validation = SchemaValidation::new(schema)?;
            let mut seen = BTreeSet::new();
            let mut fields = Vec::with_capacity(requested_columns.len());
            for column_name in requested_columns {
                let field_id = validation
                    .field_ids_by_name
                    .get(column_name)
                    .copied()
                    .ok_or_else(|| {
                        StorageError::unsupported(format!(
                            "columnar table {} does not contain column {}",
                            table.config.name, column_name
                        ))
                    })?;
                if !seen.insert(field_id) {
                    continue;
                }
                let field = validation.fields_by_id.get(&field_id).ok_or_else(|| {
                    StorageError::corruption(format!(
                        "columnar table {} schema is missing field id {}",
                        table.config.name,
                        field_id.get()
                    ))
                })?;
                fields.push((*field).clone());
            }
            fields
        } else {
            schema.fields.clone()
        };

        Ok(Some(ColumnProjection { fields }))
    }

    pub(super) fn missing_columnar_projection_value(
        field: &FieldDefinition,
        kind: ChangeKind,
    ) -> Result<FieldValue, StorageError> {
        match kind {
            ChangeKind::Merge => Ok(FieldValue::Null),
            ChangeKind::Put | ChangeKind::Delete => missing_field_value_for_definition(field),
        }
    }

    pub(super) fn project_columnar_value(
        value: &Value,
        projection: &ColumnProjection,
        kind: ChangeKind,
    ) -> Result<Value, StorageError> {
        let Value::Record(record) = value else {
            return Err(StorageError::corruption(
                "columnar value is stored as bytes during read projection",
            ));
        };

        let mut projected = ColumnarRecord::new();
        for field in &projection.fields {
            let value = match record.get(&field.id) {
                Some(value) => value.clone(),
                None => Self::missing_columnar_projection_value(field, kind)?,
            };
            projected.insert(field.id, value);
        }

        Ok(Value::Record(projected))
    }

    pub(super) fn columnar_overwritten_history_error(
        table: &StoredTable,
        key: &[u8],
    ) -> StorageError {
        StorageError::unsupported(format!(
            "columnar table {} does not support historical overwritten-key reads in v1 (key {:?})",
            table.config.name, key
        ))
    }

    pub(super) fn materialized_columnar_value(
        materialized_by_sstable: &BTreeMap<String, BTreeMap<usize, Value>>,
        row: &ColumnarRowRef,
    ) -> Result<Value, StorageError> {
        let values = materialized_by_sstable.get(&row.local_id).ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} materialization is missing",
                row.local_id
            ))
        })?;
        values.get(&row.row_index).cloned().ok_or_else(|| {
            StorageError::corruption(format!(
                "columnar SSTable {} row {} was not materialized",
                row.local_id, row.row_index
            ))
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn resolve_visible_value_columnar_with_state(
        &self,
        table: &StoredTable,
        memtables: &MemtableState,
        sstables: &SstableState,
        key: &[u8],
        sequence: SequenceNumber,
        projection: &ColumnProjection,
        access: ColumnarReadAccessPattern,
    ) -> Result<VisibleValueResolution, StorageError> {
        let mut mem_rows = Vec::new();
        memtables.collect_visible_rows(table.id, key, sequence, &mut mem_rows);

        let mut sstables_by_local_id = BTreeMap::new();
        let mut columnar_rows = Vec::new();
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table.id && sstable.is_columnar())
        {
            sstables_by_local_id.insert(sstable.meta.local_id.clone(), sstable.clone());
            columnar_rows.extend(
                sstable
                    .collect_visible_row_refs_for_key_columnar(
                        &self.inner.columnar_read_context,
                        key,
                        sequence,
                    )
                    .await?,
            );
        }

        enum VisibleCandidate {
            Memtable(SstableRow),
            Columnar(ColumnarRowRef),
        }

        let mut candidates = mem_rows
            .into_iter()
            .map(VisibleCandidate::Memtable)
            .chain(columnar_rows.into_iter().map(VisibleCandidate::Columnar))
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            let (left_sequence, left_kind) = match left {
                VisibleCandidate::Memtable(row) => (row.sequence, row.kind),
                VisibleCandidate::Columnar(row) => (row.sequence, row.kind),
            };
            let (right_sequence, right_kind) = match right {
                VisibleCandidate::Memtable(row) => (row.sequence, row.kind),
                VisibleCandidate::Columnar(row) => (row.sequence, row.kind),
            };
            right_sequence.cmp(&left_sequence).then_with(|| {
                Self::visible_row_priority(left_kind).cmp(&Self::visible_row_priority(right_kind))
            })
        });

        let Some(head) = candidates.first() else {
            return Ok(VisibleValueResolution {
                value: None,
                collapse: None,
            });
        };
        let head_sequence = match head {
            VisibleCandidate::Memtable(row) => row.sequence,
            VisibleCandidate::Columnar(row) => row.sequence,
        };

        match head {
            VisibleCandidate::Memtable(row) => match row.kind {
                ChangeKind::Put => {
                    let value = row
                        .value
                        .as_ref()
                        .ok_or_else(|| StorageError::corruption("put row is missing a value"))?;
                    return Ok(VisibleValueResolution {
                        value: Some(Self::project_columnar_value(
                            value,
                            projection,
                            ChangeKind::Put,
                        )?),
                        collapse: None,
                    });
                }
                ChangeKind::Delete => {
                    return Ok(VisibleValueResolution {
                        value: None,
                        collapse: None,
                    });
                }
                ChangeKind::Merge => {}
            },
            VisibleCandidate::Columnar(row) => match row.kind {
                ChangeKind::Put => {
                    let sstable = sstables_by_local_id.get(&row.local_id).ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {} disappeared during point read",
                            row.local_id
                        ))
                    })?;
                    let values = sstable
                        .materialize_columnar_rows(
                            &self.inner.columnar_read_context,
                            projection,
                            &BTreeSet::from([row.row_index]),
                            access,
                        )
                        .await?;
                    return Ok(VisibleValueResolution {
                        value: values.get(&row.row_index).cloned(),
                        collapse: None,
                    });
                }
                ChangeKind::Delete => {
                    return Ok(VisibleValueResolution {
                        value: None,
                        collapse: None,
                    });
                }
                ChangeKind::Merge => {}
            },
        }

        let operator = table.config.merge_operator.as_ref().ok_or_else(|| {
            StorageError::unsupported(format!(
                "merge operator is not configured for table {}",
                table.config.name
            ))
        })?;
        let full_projection = Self::resolve_scan_projection(table, None)?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let mut needed_by_sstable = BTreeMap::<String, BTreeSet<usize>>::new();
        for candidate in &candidates {
            match candidate {
                VisibleCandidate::Memtable(row) => {
                    if matches!(row.kind, ChangeKind::Put | ChangeKind::Delete) {
                        break;
                    }
                }
                VisibleCandidate::Columnar(row) => {
                    if row.kind != ChangeKind::Delete {
                        needed_by_sstable
                            .entry(row.local_id.clone())
                            .or_default()
                            .insert(row.row_index);
                    }
                    if row.kind != ChangeKind::Merge {
                        break;
                    }
                }
            }
        }

        let mut materialized_by_sstable = BTreeMap::<String, BTreeMap<usize, Value>>::new();
        for (local_id, row_indexes) in needed_by_sstable {
            let sstable = sstables_by_local_id.get(&local_id).ok_or_else(|| {
                StorageError::corruption(format!(
                    "columnar SSTable {} disappeared during merge resolution",
                    local_id
                ))
            })?;
            materialized_by_sstable.insert(
                local_id.clone(),
                sstable
                    .materialize_columnar_rows(
                        &self.inner.columnar_read_context,
                        &full_projection,
                        &row_indexes,
                        access,
                    )
                    .await?,
            );
        }

        let mut operands = Vec::new();
        let mut existing_owned = None;
        for candidate in &candidates {
            match candidate {
                VisibleCandidate::Memtable(row) => match row.kind {
                    ChangeKind::Merge => operands.push(Self::project_columnar_value(
                        row.value.as_ref().ok_or_else(|| {
                            StorageError::corruption("merge row is missing an operand value")
                        })?,
                        &full_projection,
                        ChangeKind::Merge,
                    )?),
                    ChangeKind::Put => {
                        existing_owned = Some(Self::project_columnar_value(
                            row.value.as_ref().ok_or_else(|| {
                                StorageError::corruption("put row is missing a value")
                            })?,
                            &full_projection,
                            ChangeKind::Put,
                        )?);
                        break;
                    }
                    ChangeKind::Delete => break,
                },
                VisibleCandidate::Columnar(row) => match row.kind {
                    ChangeKind::Merge => {
                        operands.push(Self::materialized_columnar_value(
                            &materialized_by_sstable,
                            row,
                        )?);
                    }
                    ChangeKind::Put => {
                        existing_owned = Some(Self::materialized_columnar_value(
                            &materialized_by_sstable,
                            row,
                        )?);
                        break;
                    }
                    ChangeKind::Delete => break,
                },
            }
        }

        operands.reverse();
        let collapsed = Self::collapse_merge_operands(operator.as_ref(), key, &operands)?;
        let full_value = Self::normalize_value_for_table(
            table,
            &operator.full_merge(key, existing_owned.as_ref(), &collapsed)?,
        )?;
        let collapse =
            (operands.len() > Self::max_merge_operand_chain_length(&table.config)).then(|| {
                MergeCollapse {
                    sequence: head_sequence,
                    value: full_value.clone(),
                }
            });

        Ok(VisibleValueResolution {
            value: Some(Self::project_columnar_value(
                &full_value,
                projection,
                ChangeKind::Put,
            )?),
            collapse,
        })
    }

    pub(super) async fn read_visible_value(
        &self,
        table_id: TableId,
        key: &[u8],
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, StorageError> {
        let table = Self::stored_table_by_id(&self.tables_read(), table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("table id {} is not registered", table_id.get()))
            })?;
        if table.config.format == TableFormat::Row {
            return self.read_visible_value_row(table_id, key, sequence);
        }

        let projection = Self::resolve_scan_projection(&table, None)?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let memtables = self.memtables_read().clone();
        let sstables = self.sstables_read().clone();
        let current_sequence = self.current_sequence();

        let mut persisted_version_count = 0_usize;
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id && sstable.is_columnar())
        {
            if sequence < current_sequence {
                persisted_version_count += sstable
                    .collect_visible_row_refs_for_key_columnar(
                        &self.inner.columnar_read_context,
                        key,
                        current_sequence,
                    )
                    .await?
                    .len();
            }
        }

        if sequence < current_sequence && persisted_version_count > 1 {
            return Err(Self::columnar_overwritten_history_error(&table, key));
        }
        let resolution = self
            .resolve_visible_value_columnar_with_state(
                &table,
                &memtables,
                &sstables,
                key,
                sequence,
                &projection,
                ColumnarReadAccessPattern::Point,
            )
            .await?;
        if let Some(collapse) = resolution.collapse.clone() {
            self.force_collapse_merge_chain(table_id, key, collapse);
        }

        Ok(resolution.value)
    }

    pub(super) async fn scan_visible(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
        matcher: KeyMatcher<'_>,
        opts: &ScanOptions,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let table = Self::stored_table_by_id(&self.tables_read(), table_id)
            .cloned()
            .ok_or_else(|| {
                StorageError::not_found(format!("table id {} is not registered", table_id.get()))
            })?;
        if table.config.format == TableFormat::Row {
            return self.scan_visible_row(table_id, sequence, matcher, opts);
        }

        let limit = opts.limit.unwrap_or(usize::MAX);
        if limit == 0 {
            return Ok(Vec::new());
        }

        let projection = Self::resolve_scan_projection(&table, opts.columns.as_deref())?
            .ok_or_else(|| StorageError::corruption("columnar table is missing a schema"))?;
        let memtables = self.memtables_read().clone();
        let sstables = self.sstables_read().clone();
        let current_sequence = self.current_sequence();

        let mut keys = BTreeSet::new();
        memtables.collect_matching_keys(table_id, &matcher, &mut keys);

        let mut persisted_versions_by_key = BTreeMap::<Key, usize>::new();
        for sstable in sstables
            .live
            .iter()
            .filter(|sstable| sstable.meta.table_id == table_id && sstable.is_columnar())
        {
            for row in sstable
                .collect_scan_row_refs_columnar(
                    &self.inner.columnar_read_context,
                    &matcher,
                    if sequence < current_sequence {
                        current_sequence
                    } else {
                        sequence
                    },
                )
                .await?
            {
                if sequence < current_sequence {
                    *persisted_versions_by_key
                        .entry(row.key.clone())
                        .or_default() += 1;
                }
                if row.sequence <= sequence {
                    keys.insert(row.key.clone());
                }
            }
        }

        let mut ordered_keys = keys.into_iter().collect::<Vec<_>>();
        if opts.reverse {
            ordered_keys.reverse();
        }

        let mut rows = Vec::new();
        for key in ordered_keys {
            if sequence < current_sequence
                && persisted_versions_by_key
                    .get(&key)
                    .copied()
                    .unwrap_or_default()
                    > 1
            {
                return Err(Self::columnar_overwritten_history_error(&table, &key));
            }
            let resolution = self
                .resolve_visible_value_columnar_with_state(
                    &table,
                    &memtables,
                    &sstables,
                    &key,
                    sequence,
                    &projection,
                    ColumnarReadAccessPattern::Scan,
                )
                .await?;
            if let Some(collapse) = resolution.collapse.clone() {
                self.force_collapse_merge_chain(table_id, &key, collapse);
            }
            let Some(value) = resolution.value else {
                continue;
            };
            rows.push((key, value));
            if rows.len() >= limit {
                break;
            }
        }

        Ok(rows)
    }

    pub(super) fn release_snapshot_registration(&self, id: u64) {
        mutex_lock(&self.inner.snapshot_tracker).release(id);
    }

    pub(super) fn oldest_active_snapshot_sequence(&self) -> Option<SequenceNumber> {
        mutex_lock(&self.inner.snapshot_tracker).oldest_active()
    }

    pub(super) fn active_snapshot_count(&self) -> u64 {
        mutex_lock(&self.inner.snapshot_tracker).count()
    }

    pub(super) fn record_compaction_filter_stats(
        &self,
        table_id: TableId,
        removed_bytes: u64,
        removed_keys: u64,
    ) {
        if removed_bytes == 0 && removed_keys == 0 {
            return;
        }

        let mut stats = mutex_lock(&self.inner.compaction_filter_stats);
        let entry = stats.entry(table_id).or_default();
        entry.removed_bytes = entry.removed_bytes.saturating_add(removed_bytes);
        entry.removed_keys = entry.removed_keys.saturating_add(removed_keys);
    }

    pub(super) fn compaction_filter_stats(&self, table_id: TableId) -> CompactionFilterStats {
        mutex_lock(&self.inner.compaction_filter_stats)
            .get(&table_id)
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn history_retention_sequences(&self, table_id: TableId) -> Option<u64> {
        self.tables_read()
            .values()
            .find(|table| table.id == table_id)
            .and_then(|table| table.config.history_retention_sequences)
    }

    pub(super) fn history_retention_floor_sequence(
        &self,
        table_id: TableId,
    ) -> Option<SequenceNumber> {
        let retained = self.history_retention_sequences(table_id)?;
        Some(SequenceNumber::new(
            self.current_sequence()
                .get()
                .saturating_sub(retained.saturating_sub(1)),
        ))
    }

    pub(super) fn history_gc_horizon(&self, table_id: TableId) -> Option<SequenceNumber> {
        let retention_floor = self.history_retention_floor_sequence(table_id)?;
        Some(
            self.oldest_active_snapshot_sequence()
                .map(|snapshot| snapshot.min(retention_floor))
                .unwrap_or(retention_floor),
        )
    }

    pub(super) fn cdc_retention_floor_sequence(
        &self,
        table_id: TableId,
        upper_bound: SequenceNumber,
    ) -> Option<SequenceNumber> {
        let retained = self.history_retention_sequences(table_id)?;
        Some(SequenceNumber::new(
            upper_bound.get().saturating_sub(retained.saturating_sub(1)),
        ))
    }

    pub(super) fn cdc_gc_min_sequence(
        &self,
        upper_bound: SequenceNumber,
    ) -> Option<SequenceNumber> {
        self.tables_read()
            .values()
            .filter_map(|table| self.cdc_retention_floor_sequence(table.id, upper_bound))
            .min()
    }

    pub(super) fn table_change_feed_watermark(&self, table_id: TableId) -> Option<SequenceNumber> {
        let mut watermark = None;

        if let Some(sequence) = self
            .memtables_read()
            .table_watermarks()
            .get(&table_id)
            .copied()
        {
            watermark = Some(sequence);
        }
        if let Some(sequence) = self
            .sstables_read()
            .table_watermarks()
            .get(&table_id)
            .copied()
        {
            watermark = Some(
                watermark
                    .map(|current| current.max(sequence))
                    .unwrap_or(sequence),
            );
        }

        watermark
    }

    pub(super) fn change_feed_floor_from_state(
        &self,
        table_id: TableId,
        upper_bound: SequenceNumber,
        physical_oldest: Option<SequenceNumber>,
        oldest_segment_id: Option<SegmentId>,
        table_watermark: Option<SequenceNumber>,
    ) -> Option<SequenceNumber> {
        let mut floor = self
            .cdc_retention_floor_sequence(table_id, upper_bound)
            .filter(|logical| physical_oldest.is_some_and(|oldest| oldest < *logical));

        if oldest_segment_id.is_some_and(|segment_id| segment_id.get() > 1)
            && let Some(physical_floor) = physical_oldest.or(table_watermark)
        {
            floor = Some(
                floor
                    .map(|current| current.max(physical_floor))
                    .unwrap_or(physical_floor),
            );
        }

        floor.filter(|sequence| sequence.get() > 0)
    }

    pub(super) async fn prune_commit_log(&self, seal_active: bool) -> Result<(), StorageError> {
        if self.local_storage_root().is_none() {
            return Ok(());
        }

        let recovery_min = self.sstables_read().last_flushed_sequence;
        let cdc_min = self.cdc_gc_min_sequence(self.current_sequence());
        let gc_floor = cdc_min
            .map(|cdc_min| recovery_min.min(cdc_min))
            .unwrap_or(recovery_min);
        if gc_floor == SequenceNumber::new(0) {
            return Ok(());
        }

        let protected_segments = mutex_lock(&self.inner.commit_log_scans).pinned_segments();
        let mut runtime = self.inner.commit_runtime.lock().await;
        if seal_active {
            runtime.maybe_seal_active().await?;
        }
        runtime
            .prune_segments_before(gc_floor, &protected_segments)
            .await
    }

    pub(super) fn validate_historical_read(
        &self,
        table_id: TableId,
        requested: SequenceNumber,
    ) -> Result<(), ReadError> {
        let Some(oldest_available) = self.history_gc_horizon(table_id) else {
            return Ok(());
        };
        if requested < oldest_available {
            return Err(SnapshotTooOld {
                requested,
                oldest_available,
            }
            .into());
        }

        Ok(())
    }

    #[cfg(test)]
    pub(super) fn snapshot_gc_horizon(&self) -> SequenceNumber {
        self.oldest_active_snapshot_sequence()
            .unwrap_or_else(|| self.current_sequence())
    }

    #[cfg(test)]
    pub(super) fn visible_subscriber_count(&self, table: &Table) -> usize {
        self.inner
            .visible_watchers
            .active_subscriber_count(table.name())
    }

    #[cfg(test)]
    pub(super) fn durable_subscriber_count(&self, table: &Table) -> usize {
        self.inner
            .durable_watchers
            .active_subscriber_count(table.name())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn subscribe_set<'a, I>(
        registry: &Arc<WatermarkRegistry>,
        tables: I,
    ) -> WatermarkSubscriptionSet
    where
        I: IntoIterator<Item = &'a Table>,
    {
        WatermarkSubscriptionSet::new(
            registry,
            tables
                .into_iter()
                .map(|table| (table.name().to_string(), registry.subscribe(table.name())))
                .collect(),
        )
    }

    pub(super) fn initial_table_watermarks(
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
    ) -> BTreeMap<String, SequenceNumber> {
        let mut by_id = sstables.table_watermarks();
        for (table_id, sequence) in memtables.table_watermarks() {
            update_table_watermark(&mut by_id, table_id, sequence);
        }

        tables
            .iter()
            .filter_map(|(name, table)| {
                by_id
                    .get(&table.id)
                    .copied()
                    .map(|sequence| (name.clone(), sequence))
            })
            .collect()
    }

    pub(super) fn tables_read(&self) -> RwLockReadGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.read()
    }

    pub(super) fn tables_write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, StoredTable>> {
        self.inner.tables.write()
    }

    pub(super) fn memtables_read(&self) -> RwLockReadGuard<'_, MemtableState> {
        self.inner.memtables.read()
    }

    pub(super) fn memtables_write(&self) -> RwLockWriteGuard<'_, MemtableState> {
        self.inner.memtables.write()
    }

    pub(super) fn sstables_read(&self) -> RwLockReadGuard<'_, SstableState> {
        self.inner.sstables.read()
    }

    pub(super) fn sstables_write(&self) -> RwLockWriteGuard<'_, SstableState> {
        self.inner.sstables.write()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn columnar_read_context(&self) -> &ColumnarReadContext {
        self.inner.columnar_read_context.as_ref()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn columnar_cache_stats_snapshot(&self) -> ColumnarCacheStatsSnapshot {
        self.inner.columnar_read_context.decoded_cache.snapshot()
    }

    pub fn columnar_cache_usage_snapshot(&self) -> ColumnarCacheUsageSnapshot {
        self.inner.columnar_read_context.cache_usage_snapshot()
    }

    pub fn scheduler_observability_snapshot(&self) -> SchedulerObservabilitySnapshot {
        SchedulerObservabilitySnapshot {
            deferred_work: mutex_lock(&self.inner.work_deferrals).clone(),
            forced_executions: self
                .inner
                .scheduler_observability
                .forced_executions
                .load(Ordering::Relaxed),
            forced_flushes: self
                .inner
                .scheduler_observability
                .forced_flushes
                .load(Ordering::Relaxed),
            forced_l0_compactions: self
                .inner
                .scheduler_observability
                .forced_l0_compactions
                .load(Ordering::Relaxed),
            budget_blocked_executions: self
                .inner
                .scheduler_observability
                .budget_blocked_executions
                .load(Ordering::Relaxed),
            background_delay_events: self
                .inner
                .scheduler_observability
                .background_delay_events
                .load(Ordering::Relaxed),
            background_delay_millis: self
                .inner
                .scheduler_observability
                .background_delay_millis
                .load(Ordering::Relaxed),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn reset_columnar_cache_stats(&self) {
        self.inner.columnar_read_context.decoded_cache.reset_stats();
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn clear_columnar_decoded_cache(&self) {
        self.inner.columnar_read_context.decoded_cache.clear();
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn set_columnar_cache_enabled(&self, raw_byte_cache: bool, decoded_cache: bool) {
        self.inner
            .columnar_read_context
            .raw_byte_cache_enabled
            .store(raw_byte_cache, Ordering::Relaxed);
        self.inner
            .columnar_read_context
            .decoded_cache_enabled
            .store(decoded_cache, Ordering::Relaxed);
    }
}
