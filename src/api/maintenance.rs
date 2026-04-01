use super::*;

impl Db {
    pub(super) fn sort_live_sstables(live: &mut [ResidentRowSstable]) {
        live.sort_by(|left, right| {
            (
                left.meta.table_id.get(),
                left.meta.level,
                left.meta.min_key.as_slice(),
                left.meta.max_key.as_slice(),
                left.meta.local_id.as_str(),
            )
                .cmp(&(
                    right.meta.table_id.get(),
                    right.meta.level,
                    right.meta.min_key.as_slice(),
                    right.meta.max_key.as_slice(),
                    right.meta.local_id.as_str(),
                ))
        });
    }

    pub(super) fn leveled_level_target_bytes(level: u32) -> u64 {
        if level == 0 {
            return 0;
        }

        let mut target = LEVELED_BASE_LEVEL_TARGET_BYTES;
        for _ in 1..level {
            target = target.saturating_mul(LEVELED_LEVEL_SIZE_MULTIPLIER);
        }
        target
    }

    pub(super) fn table_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        match table.config.compaction_strategy {
            CompactionStrategy::Leveled => Self::leveled_compaction_state(table, live),
            CompactionStrategy::Tiered => Self::tiered_compaction_state(table, live),
            CompactionStrategy::Fifo => Self::fifo_compaction_state(table, live),
        }
    }

    pub(super) fn table_live_sstables(
        table_id: TableId,
        live: &[ResidentRowSstable],
    ) -> Vec<ResidentRowSstable> {
        live.iter()
            .filter(|sstable| sstable.meta.table_id == table_id)
            .cloned()
            .collect()
    }

    pub(super) fn sstables_at_level(
        table_live: &[ResidentRowSstable],
        level: u32,
    ) -> Vec<ResidentRowSstable> {
        table_live
            .iter()
            .filter(|sstable| sstable.meta.level == level)
            .cloned()
            .collect()
    }

    pub(super) fn leveled_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let table_live = Self::table_live_sstables(table.id, live);
        if table_live.is_empty() {
            return TableCompactionState::default();
        }

        let mut compaction_debt = 0_u64;
        let mut next_job = None;

        let l0 = Self::sstables_at_level(&table_live, 0);
        if l0.len() >= LEVELED_L0_COMPACTION_TRIGGER {
            compaction_debt = compaction_debt
                .saturating_add(l0.iter().map(|sstable| sstable.meta.length).sum::<u64>());
            next_job = Self::build_leveled_compaction_job(table, &table_live, 0, &l0);
        }

        let max_level = table_live
            .iter()
            .map(|sstable| sstable.meta.level)
            .max()
            .unwrap_or_default();
        for level in 1..=max_level {
            let level_files = Self::sstables_at_level(&table_live, level);
            if level_files.is_empty() {
                continue;
            }

            let level_bytes = level_files
                .iter()
                .map(|sstable| sstable.meta.length)
                .sum::<u64>();
            let target_bytes = Self::leveled_level_target_bytes(level);
            if level_bytes > target_bytes {
                compaction_debt =
                    compaction_debt.saturating_add(level_bytes.saturating_sub(target_bytes));
                if next_job.is_none() {
                    next_job =
                        Self::build_leveled_compaction_job(table, &table_live, level, &level_files);
                }
            }
        }

        TableCompactionState {
            compaction_debt,
            next_job,
        }
    }

    pub(super) fn tiered_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let table_live = Self::table_live_sstables(table.id, live);
        if table_live.is_empty() {
            return TableCompactionState::default();
        }

        let mut compaction_debt = 0_u64;
        let mut next_job = None;
        let max_level = table_live
            .iter()
            .map(|sstable| sstable.meta.level)
            .max()
            .unwrap_or_default();

        for level in 0..=max_level {
            let level_files = Self::sstables_at_level(&table_live, level);
            if level_files.len() < TIERED_LEVEL_RUN_COMPACTION_TRIGGER {
                continue;
            }

            compaction_debt = compaction_debt.saturating_add(
                level_files
                    .iter()
                    .map(|sstable| sstable.meta.length)
                    .sum::<u64>(),
            );
            if next_job.is_none() {
                next_job = Self::build_rewrite_compaction_job(
                    table,
                    level,
                    level.saturating_add(1),
                    &level_files,
                );
            }
        }

        TableCompactionState {
            compaction_debt,
            next_job,
        }
    }

    pub(super) fn fifo_compaction_state(
        table: &StoredTable,
        live: &[ResidentRowSstable],
    ) -> TableCompactionState {
        let mut table_live = Self::table_live_sstables(table.id, live);
        if table_live.len() <= FIFO_MAX_LIVE_SSTABLES {
            return TableCompactionState::default();
        }

        table_live.sort_by(|left, right| {
            (
                left.meta.max_sequence.get(),
                left.meta.min_sequence.get(),
                left.meta.local_id.as_str(),
            )
                .cmp(&(
                    right.meta.max_sequence.get(),
                    right.meta.min_sequence.get(),
                    right.meta.local_id.as_str(),
                ))
        });

        let delete_count = table_live.len().saturating_sub(FIFO_MAX_LIVE_SSTABLES);
        let expired = table_live
            .into_iter()
            .take(delete_count)
            .collect::<Vec<_>>();
        let compaction_debt = expired
            .iter()
            .map(|sstable| sstable.meta.length)
            .sum::<u64>();

        TableCompactionState {
            compaction_debt,
            next_job: Self::build_delete_only_compaction_job(table, &expired),
        }
    }

    pub(super) fn build_leveled_compaction_job(
        table: &StoredTable,
        table_live: &[ResidentRowSstable],
        source_level: u32,
        source_inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        let target_level = source_level.saturating_add(1);
        let (min_key, max_key) = Self::sstable_key_span(source_inputs)?;
        let mut inputs = source_inputs.to_vec();

        for sstable in table_live.iter().filter(|sstable| {
            sstable.meta.level == target_level
                && Self::key_ranges_overlap(
                    sstable.meta.min_key.as_slice(),
                    sstable.meta.max_key.as_slice(),
                    min_key.as_slice(),
                    max_key.as_slice(),
                )
        }) {
            inputs.push(sstable.clone());
        }

        Self::build_rewrite_compaction_job(table, source_level, target_level, &inputs)
    }

    pub(super) fn build_rewrite_compaction_job(
        table: &StoredTable,
        source_level: u32,
        target_level: u32,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        Self::build_compaction_job(
            table,
            source_level,
            target_level,
            CompactionJobKind::Rewrite,
            inputs,
        )
    }

    pub(super) fn build_delete_only_compaction_job(
        table: &StoredTable,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        let source_level = inputs
            .iter()
            .map(|sstable| sstable.meta.level)
            .min()
            .unwrap_or_default();
        Self::build_compaction_job(
            table,
            source_level,
            source_level,
            CompactionJobKind::DeleteOnly,
            inputs,
        )
    }

    pub(super) fn build_compaction_job(
        table: &StoredTable,
        source_level: u32,
        target_level: u32,
        kind: CompactionJobKind,
        inputs: &[ResidentRowSstable],
    ) -> Option<CompactionJob> {
        if inputs.is_empty() {
            return None;
        }

        let mut input_local_ids = inputs
            .iter()
            .map(|sstable| sstable.meta.local_id.clone())
            .collect::<Vec<_>>();
        input_local_ids.sort();
        input_local_ids.dedup();

        Some(CompactionJob {
            id: format!(
                "compaction:{}:L{}:{}",
                table.config.name,
                source_level,
                input_local_ids.join("+")
            ),
            table_id: table.id,
            table_name: table.config.name.clone(),
            source_level,
            target_level,
            kind,
            estimated_bytes: inputs
                .iter()
                .map(|sstable| sstable.meta.length)
                .sum::<u64>(),
            input_local_ids,
        })
    }

    pub(super) fn build_offload_job(
        table: &StoredTable,
        kind: OffloadJobKind,
        inputs: &[ResidentRowSstable],
    ) -> Option<OffloadJob> {
        if inputs.is_empty() {
            return None;
        }

        let input_local_ids = inputs
            .iter()
            .map(|sstable| sstable.meta.local_id.clone())
            .collect::<Vec<_>>();
        let work_prefix = match kind {
            OffloadJobKind::Offload => "offload",
            OffloadJobKind::Delete => "expire",
        };
        Some(OffloadJob {
            id: format!(
                "{work_prefix}:{}:{}",
                table.config.name,
                input_local_ids.join("+")
            ),
            table_id: table.id,
            table_name: table.config.name.clone(),
            kind,
            estimated_bytes: inputs
                .iter()
                .map(|sstable| sstable.meta.length)
                .sum::<u64>(),
            input_local_ids,
        })
    }

    pub(super) fn sstable_key_span(sstables: &[ResidentRowSstable]) -> Option<(Key, Key)> {
        Some((
            sstables
                .iter()
                .map(|sstable| sstable.meta.min_key.clone())
                .min()?,
            sstables
                .iter()
                .map(|sstable| sstable.meta.max_key.clone())
                .max()?,
        ))
    }

    pub(super) fn key_ranges_overlap(
        left_min: &[u8],
        left_max: &[u8],
        right_min: &[u8],
        right_max: &[u8],
    ) -> bool {
        left_min <= right_max && right_min <= left_max
    }

    pub(super) fn pending_compaction_jobs(&self) -> Vec<CompactionJob> {
        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let mut jobs = tables
            .values()
            .filter_map(|table| Self::table_compaction_state(table, &live).next_job)
            .collect::<Vec<_>>();
        jobs.sort_by(|left, right| {
            (
                left.source_level,
                left.table_name.as_str(),
                left.id.as_str(),
            )
                .cmp(&(
                    right.source_level,
                    right.table_name.as_str(),
                    right.id.as_str(),
                ))
        });
        jobs
    }

    pub(super) fn pending_flush_candidates(&self) -> Vec<PendingWorkCandidate> {
        if self.local_storage_root().is_none() {
            return Vec::new();
        }

        let tables = self.tables_read().clone();
        let mut candidates = self
            .memtables_read()
            .pending_flush_bytes_by_table()
            .into_iter()
            .filter_map(|(table_id, estimated_bytes)| {
                let stored = Self::stored_table_by_id(&tables, table_id)?;
                let tagged = self.tag_pending_work(PendingWork {
                    id: format!("flush:{}", stored.config.name),
                    work_type: PendingWorkType::Flush,
                    table: stored.config.name.clone(),
                    level: None,
                    estimated_bytes,
                });
                Some(PendingWorkCandidate {
                    pending: tagged.work,
                    tag: tagged.tag,
                    spec: PendingWorkSpec::Flush,
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.pending
                .table
                .cmp(&right.pending.table)
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    pub(super) fn pending_offload_candidates(&self) -> Vec<PendingWorkCandidate> {
        let Some(max_local_bytes) = self.local_sstable_budget_bytes() else {
            return Vec::new();
        };
        let retention = self.local_sstable_retention();

        let tables = self.tables_read().clone();
        let live = self.sstables_read().live.clone();
        let mut candidates = tables
            .values()
            .filter_map(|table| {
                if table.config.format != TableFormat::Row {
                    return None;
                }

                let mut local_sstables = live
                    .iter()
                    .filter(|sstable| {
                        sstable.meta.table_id == table.id && !sstable.meta.file_path.is_empty()
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                let local_bytes = local_sstables
                    .iter()
                    .map(|sstable| sstable.meta.length)
                    .sum::<u64>();
                if local_bytes <= max_local_bytes {
                    return None;
                }

                local_sstables.sort_by(|left, right| {
                    (
                        left.meta.max_sequence.get(),
                        left.meta.min_sequence.get(),
                        left.meta.level,
                        left.meta.local_id.as_str(),
                    )
                        .cmp(&(
                            right.meta.max_sequence.get(),
                            right.meta.min_sequence.get(),
                            right.meta.level,
                            right.meta.local_id.as_str(),
                        ))
                });

                let mut remaining_local_bytes = local_bytes;
                let mut selected = Vec::new();
                for sstable in local_sstables {
                    if remaining_local_bytes <= max_local_bytes {
                        break;
                    }
                    remaining_local_bytes =
                        remaining_local_bytes.saturating_sub(sstable.meta.length);
                    selected.push(sstable);
                }

                let job = Self::build_offload_job(
                    table,
                    match retention {
                        TieredLocalRetentionMode::Offload => OffloadJobKind::Offload,
                        TieredLocalRetentionMode::Delete => OffloadJobKind::Delete,
                    },
                    &selected,
                )?;
                let tagged = self.tag_pending_work(PendingWork {
                    id: job.id.clone(),
                    work_type: PendingWorkType::Offload,
                    table: job.table_name.clone(),
                    level: None,
                    estimated_bytes: job.estimated_bytes,
                });
                Some(PendingWorkCandidate {
                    pending: tagged.work,
                    tag: tagged.tag,
                    spec: PendingWorkSpec::Offload(job),
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| {
            left.pending
                .table
                .cmp(&right.pending.table)
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    pub(super) fn pending_work_candidates(&self) -> Vec<PendingWorkCandidate> {
        let mut candidates = self.pending_flush_candidates();
        candidates.extend(self.pending_compaction_jobs().into_iter().map(|job| {
            let tagged = self.tag_pending_work(PendingWork {
                id: job.id.clone(),
                work_type: PendingWorkType::Compaction,
                table: job.table_name.clone(),
                level: Some(job.source_level),
                estimated_bytes: job.estimated_bytes,
            });
            PendingWorkCandidate {
                pending: tagged.work,
                tag: tagged.tag,
                spec: PendingWorkSpec::Compaction(job),
            }
        }));
        candidates.extend(self.pending_offload_candidates());
        candidates.sort_by(|left, right| {
            pending_work_sort_key(&left.pending)
                .cmp(&pending_work_sort_key(&right.pending))
                .then_with(|| left.pending.id.cmp(&right.pending.id))
        });
        candidates
    }

    pub(super) fn prune_work_deferrals(&self, candidates: &[PendingWorkCandidate]) {
        let live_work_ids = candidates
            .iter()
            .map(|candidate| candidate.pending.id.as_str())
            .collect::<BTreeSet<_>>();
        self.inner
            .scheduler_observability
            .prune_deferred_work(&live_work_ids);
    }

    pub(super) fn record_deferred_work(
        &self,
        candidates: &[PendingWorkCandidate],
        decisions: &BTreeMap<String, ScheduleAction>,
    ) {
        self.inner
            .scheduler_observability
            .record_deferred_work(candidates, decisions);
    }

    pub(super) fn reset_work_deferral(&self, work_id: &str) {
        self.inner
            .scheduler_observability
            .reset_work_deferral(work_id);
    }

    pub(super) fn work_deferral_cycles(&self, work_id: &str) -> u32 {
        self.inner
            .scheduler_observability
            .work_deferral_cycles(work_id)
    }

    pub(super) fn record_forced_execution(&self) {
        self.inner.scheduler_observability.record_forced_execution();
    }

    pub(super) fn record_forced_flush(&self) {
        self.inner.scheduler_observability.record_forced_flush();
    }

    pub(super) fn record_forced_l0_compaction(&self) {
        self.inner
            .scheduler_observability
            .record_forced_l0_compaction();
    }

    pub(super) fn record_budget_blocked_execution(&self, tag: &crate::WorkRuntimeTag) {
        self.inner
            .scheduler_observability
            .record_budget_blocked_execution(tag);
    }

    pub(super) fn record_background_delay(&self, tag: &crate::WorkRuntimeTag, delay: Duration) {
        self.inner
            .scheduler_observability
            .record_background_delay(tag, delay);
    }

    pub(super) fn pending_work_budget_block_reason(
        &self,
        candidate: &PendingWorkCandidate,
        budget: PendingWorkBudget,
    ) -> Option<PendingWorkBudgetBlockReason> {
        let state = mutex_lock(&self.inner.pending_work_budget_state);
        let work_type = candidate.pending.work_type;
        let work_state = state.by_work_type.get(&work_type);
        let domain_state = state.by_domain.get(&candidate.tag.domain);
        if budget.max_domain_concurrency.is_some_and(|max| {
            domain_state
                .map(|usage| usage.concurrency)
                .unwrap_or_default()
                >= max
        }) {
            return Some(PendingWorkBudgetBlockReason::DomainConcurrency);
        }
        if budget.max_domain_local_requests.is_some_and(|max| {
            domain_state
                .map(|usage| usage.local_requests)
                .unwrap_or_default()
                >= max
        }) {
            return Some(PendingWorkBudgetBlockReason::DomainLocalRequests);
        }
        if budget.max_domain_remote_requests.is_some_and(|max| {
            domain_state
                .map(|usage| usage.remote_requests)
                .unwrap_or_default()
                >= max
        }) {
            return Some(PendingWorkBudgetBlockReason::DomainRemoteRequests);
        }
        if budget.max_domain_in_flight_bytes.is_some_and(|max| {
            domain_state
                .map(|usage| usage.in_flight_bytes)
                .unwrap_or_default()
                .saturating_add(candidate.pending.estimated_bytes)
                > max
        }) {
            return Some(PendingWorkBudgetBlockReason::DomainInFlightBytes);
        }
        if budget.max_concurrency.is_some_and(|max| {
            work_state
                .map(|usage| usage.concurrency)
                .unwrap_or_default()
                >= max
        }) {
            return Some(PendingWorkBudgetBlockReason::Concurrency);
        }
        if budget.max_in_flight_requests.is_some_and(|max| {
            work_state
                .map(|usage| usage.in_flight_requests)
                .unwrap_or_default()
                >= max
        }) {
            return Some(PendingWorkBudgetBlockReason::InFlightRequests);
        }
        if budget.max_in_flight_bytes.is_some_and(|max| {
            work_state
                .map(|usage| usage.in_flight_bytes)
                .unwrap_or_default()
                .saturating_add(candidate.pending.estimated_bytes)
                > max
        }) {
            return Some(PendingWorkBudgetBlockReason::InFlightBytes);
        }
        None
    }

    pub(super) fn reserve_pending_work_budget(&self, candidate: &PendingWorkCandidate) {
        let mut state = mutex_lock(&self.inner.pending_work_budget_state);
        let usage = state
            .by_work_type
            .entry(candidate.pending.work_type)
            .or_default();
        usage.in_flight_bytes = usage
            .in_flight_bytes
            .saturating_add(candidate.pending.estimated_bytes);
        usage.in_flight_requests = usage.in_flight_requests.saturating_add(1);
        usage.concurrency = usage.concurrency.saturating_add(1);

        let domain_usage = state
            .by_domain
            .entry(candidate.tag.domain.clone())
            .or_default();
        domain_usage.in_flight_bytes = domain_usage
            .in_flight_bytes
            .saturating_add(candidate.pending.estimated_bytes);
        domain_usage.concurrency = domain_usage.concurrency.saturating_add(1);
        if self.pending_work_uses_remote_io(candidate.pending.work_type) {
            domain_usage.remote_requests = domain_usage.remote_requests.saturating_add(1);
        } else {
            domain_usage.local_requests = domain_usage.local_requests.saturating_add(1);
        }
    }

    pub(super) fn release_pending_work_budget(&self, candidate: &PendingWorkCandidate) {
        let mut state = mutex_lock(&self.inner.pending_work_budget_state);
        let work_type = candidate.pending.work_type;
        if let Some(usage) = state.by_work_type.get_mut(&work_type) {
            usage.in_flight_bytes = usage
                .in_flight_bytes
                .saturating_sub(candidate.pending.estimated_bytes);
            usage.in_flight_requests = usage.in_flight_requests.saturating_sub(1);
            usage.concurrency = usage.concurrency.saturating_sub(1);
            if usage.in_flight_bytes == 0 && usage.in_flight_requests == 0 && usage.concurrency == 0
            {
                state.by_work_type.remove(&work_type);
            }
        }

        if let Some(domain_usage) = state.by_domain.get_mut(&candidate.tag.domain) {
            domain_usage.in_flight_bytes = domain_usage
                .in_flight_bytes
                .saturating_sub(candidate.pending.estimated_bytes);
            domain_usage.concurrency = domain_usage.concurrency.saturating_sub(1);
            if self.pending_work_uses_remote_io(candidate.pending.work_type) {
                domain_usage.remote_requests = domain_usage.remote_requests.saturating_sub(1);
            } else {
                domain_usage.local_requests = domain_usage.local_requests.saturating_sub(1);
            }
            if domain_usage.in_flight_bytes == 0
                && domain_usage.concurrency == 0
                && domain_usage.local_requests == 0
                && domain_usage.remote_requests == 0
            {
                state.by_domain.remove(&candidate.tag.domain);
            }
        }
    }

    pub(super) fn adaptive_pending_work_delay(
        estimated_bytes: u64,
        max_bytes_per_second: u64,
        deferred_cycles: u32,
    ) -> Duration {
        let base = Self::throttle_delay(estimated_bytes, max_bytes_per_second);
        if base.is_zero() {
            return Duration::ZERO;
        }
        let divisor = u64::from(deferred_cycles.saturating_add(1));
        let base_millis = base.as_millis() as u64;
        Duration::from_millis(base_millis.div_ceil(divisor))
    }

    pub(super) async fn scheduler_budget_for_candidate(
        &self,
        candidate: &PendingWorkCandidate,
    ) -> PendingWorkBudget {
        let Some(table) = self.try_table(candidate.pending.table.clone()) else {
            return PendingWorkBudget::default();
        };
        let stats = self.table_stats(&table).await;
        let tagged = crate::DomainTaggedWork::new(candidate.pending.clone(), candidate.tag.clone());
        let domain_budget = self.execution_domain_budget(&candidate.tag.domain);
        let priority_override = crate::scheduler::pending_work_domain_priority_override(&tagged);
        self.inner.scheduler.domain_work_budget(
            &tagged,
            &stats,
            domain_budget.as_ref(),
            priority_override,
        )
    }

    pub(super) fn deferred_work_candidate(
        &self,
        candidates: &[PendingWorkCandidate],
    ) -> Option<PendingWorkCandidate> {
        candidates
            .iter()
            .find(|candidate| {
                self.work_deferral_cycles(&candidate.pending.id) >= MAX_SCHEDULER_DEFER_CYCLES
            })
            .cloned()
    }

    pub(super) fn forced_l0_compaction_candidate(
        &self,
        candidates: &[PendingWorkCandidate],
    ) -> Option<PendingWorkCandidate> {
        let live = self.sstables_read().live.clone();
        candidates
            .iter()
            .find(|candidate| match &candidate.spec {
                PendingWorkSpec::Compaction(job) if job.source_level == 0 => {
                    SstableState {
                        manifest_generation: ManifestId::default(),
                        last_flushed_sequence: SequenceNumber::default(),
                        live: live.clone(),
                    }
                    .table_stats(job.table_id)
                    .0 >= DEFAULT_WRITE_STALL_L0_SSTABLE_COUNT
                }
                PendingWorkSpec::Flush
                | PendingWorkSpec::Compaction(_)
                | PendingWorkSpec::Offload(_) => false,
            })
            .cloned()
    }

    pub(super) fn forced_flush_candidate(
        &self,
        candidates: &[PendingWorkCandidate],
        flush_candidates: &[crate::DomainTaggedWork<FlushPressureCandidate>],
    ) -> Option<PendingWorkCandidate> {
        let mut forced = flush_candidates
            .iter()
            .filter(|candidate| {
                candidate
                    .work
                    .metadata
                    .get(crate::pressure::FLUSH_FORCE_REASON_METADATA_KEY)
                    .and_then(serde_json::Value::as_str)
                    .is_some()
            })
            .collect::<Vec<_>>();
        forced.sort_by(|left, right| {
            crate::pressure::flush_candidate_priority_key(&right.work)
                .cmp(&crate::pressure::flush_candidate_priority_key(&left.work))
                .then_with(|| left.tag.domain.cmp(&right.tag.domain))
                .then_with(|| left.work.work.table.cmp(&right.work.work.table))
                .then_with(|| left.work.work.id.cmp(&right.work.work.id))
        });

        forced.into_iter().find_map(|candidate| {
            candidates
                .iter()
                .find(|pending| pending.pending.id == candidate.work.work.id)
                .cloned()
        })
    }

    pub(super) async fn execute_pending_work(
        &self,
        local_root: &str,
        candidate: PendingWorkCandidate,
    ) -> Result<(), StorageError> {
        self.reset_work_deferral(&candidate.pending.id);
        match candidate.spec {
            PendingWorkSpec::Flush => self
                .flush_internal(false)
                .await
                .map(|_| ())
                .map_err(Self::flush_error_into_storage),
            PendingWorkSpec::Compaction(job) => self.execute_compaction_job(local_root, job).await,
            PendingWorkSpec::Offload(job) => self.execute_offload_job(job).await,
        }
    }

    pub(super) async fn execute_pending_work_budgeted(
        &self,
        local_root: &str,
        candidate: PendingWorkCandidate,
        budget: PendingWorkBudget,
        forced: bool,
    ) -> Result<(), StorageError> {
        if !forced && let Some(rate) = budget.max_bytes_per_second {
            let delay = Self::adaptive_pending_work_delay(
                candidate.pending.estimated_bytes,
                rate,
                self.work_deferral_cycles(&candidate.pending.id),
            );
            self.record_background_delay(&candidate.tag, delay);
            self.inner.dependencies.clock.sleep(delay).await;
        }

        self.reserve_pending_work_budget(&candidate);
        let result = self
            .execute_pending_work(local_root, candidate.clone())
            .await;
        self.release_pending_work_budget(&candidate);
        result
    }

    pub(super) async fn run_scheduler_pass(
        &self,
        allow_forced_execution: bool,
    ) -> Result<bool, StorageError> {
        let Some(local_root) = self.local_storage_root().map(str::to_string) else {
            return Ok(false);
        };

        let attempts = if allow_forced_execution {
            MAX_SCHEDULER_DEFER_CYCLES
        } else {
            1
        };

        for _ in 0..attempts {
            let candidates = self.pending_work_candidates();
            self.prune_work_deferrals(&candidates);
            if candidates.is_empty() {
                return Ok(false);
            }

            if let Some(candidate) = self.forced_l0_compaction_candidate(&candidates) {
                self.record_forced_execution();
                self.record_forced_l0_compaction();
                self.execute_pending_work_budgeted(
                    &local_root,
                    candidate,
                    PendingWorkBudget::default(),
                    true,
                )
                .await?;
                return Ok(true);
            }

            let flush_candidates = self.pending_flush_pressure_candidates().await;
            if let Some(candidate) = self.forced_flush_candidate(&candidates, &flush_candidates) {
                self.record_forced_execution();
                self.record_forced_flush();
                self.execute_pending_work_budgeted(
                    &local_root,
                    candidate,
                    PendingWorkBudget::default(),
                    true,
                )
                .await?;
                return Ok(true);
            }

            if !flush_candidates.is_empty() {
                let flush_decisions = self
                    .inner
                    .scheduler
                    .on_flush_pressure_available(&flush_candidates)
                    .into_iter()
                    .map(|decision| (decision.work_id, decision.action))
                    .collect::<BTreeMap<_, _>>();
                let mut scheduled_flushes = flush_candidates
                    .iter()
                    .filter(|candidate| {
                        flush_decisions
                            .get(&candidate.work.work.id)
                            .copied()
                            .unwrap_or(ScheduleAction::Defer)
                            == ScheduleAction::Execute
                    })
                    .collect::<Vec<_>>();
                scheduled_flushes.sort_by(|left, right| {
                    crate::pressure::flush_candidate_priority_key(&right.work)
                        .cmp(&crate::pressure::flush_candidate_priority_key(&left.work))
                        .then_with(|| left.tag.domain.cmp(&right.tag.domain))
                        .then_with(|| left.work.work.table.cmp(&right.work.work.table))
                        .then_with(|| left.work.work.id.cmp(&right.work.work.id))
                });

                for selected in scheduled_flushes {
                    let Some(candidate) = candidates
                        .iter()
                        .find(|pending| pending.pending.id == selected.work.work.id)
                        .cloned()
                    else {
                        continue;
                    };
                    let budget = self.scheduler_budget_for_candidate(&candidate).await;
                    if self
                        .pending_work_budget_block_reason(&candidate, budget)
                        .is_some()
                    {
                        self.record_budget_blocked_execution(&candidate.tag);
                        continue;
                    }

                    self.execute_pending_work_budgeted(&local_root, candidate, budget, false)
                        .await?;
                    return Ok(true);
                }
            }

            let pending = candidates
                .iter()
                .map(|candidate| {
                    crate::DomainTaggedWork::new(candidate.pending.clone(), candidate.tag.clone())
                })
                .collect::<Vec<_>>();
            let mut decisions = self
                .inner
                .scheduler
                .on_domain_work_available(&pending)
                .into_iter()
                .map(|decision| (decision.work_id, decision.action))
                .collect::<BTreeMap<_, _>>();

            let scheduled = candidates
                .iter()
                .filter(|candidate| {
                    decisions
                        .get(&candidate.pending.id)
                        .copied()
                        .unwrap_or(ScheduleAction::Defer)
                        == ScheduleAction::Execute
                })
                .cloned()
                .collect::<Vec<_>>();

            for candidate in &scheduled {
                let budget = self.scheduler_budget_for_candidate(candidate).await;
                if self
                    .pending_work_budget_block_reason(candidate, budget)
                    .is_some()
                {
                    decisions.insert(candidate.pending.id.clone(), ScheduleAction::Defer);
                    self.record_budget_blocked_execution(&candidate.tag);
                    continue;
                }

                self.execute_pending_work_budgeted(&local_root, candidate.clone(), budget, false)
                    .await?;
                return Ok(true);
            }

            self.record_deferred_work(&candidates, &decisions);
            if allow_forced_execution
                && let Some(candidate) = self.deferred_work_candidate(&candidates)
            {
                self.record_forced_execution();
                self.execute_pending_work_budgeted(
                    &local_root,
                    candidate,
                    PendingWorkBudget::default(),
                    true,
                )
                .await?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn pending_work_uses_remote_io(&self, work_type: PendingWorkType) -> bool {
        matches!(
            work_type,
            PendingWorkType::Backup | PendingWorkType::Offload | PendingWorkType::Prefetch
        )
    }

    #[cfg(test)]
    pub(super) async fn run_next_scheduled_work(&self) -> Result<bool, StorageError> {
        self.run_scheduler_pass(true).await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn estimated_sstable_row_bytes(row: &SstableRow) -> u64 {
        (row.key.len() + row.value.as_ref().map(value_size_bytes).unwrap_or_default() + 32) as u64
    }

    pub(super) fn retain_rows_within_horizon(
        rows: Vec<CompactionRow>,
        horizon: Option<SequenceNumber>,
    ) -> Vec<CompactionRow> {
        let Some(horizon) = horizon else {
            return rows;
        };

        let mut retained = Vec::with_capacity(rows.len());
        let mut index = 0_usize;
        while index < rows.len() {
            let key = rows[index].row.key.clone();
            let mut anchor_kept = false;

            while index < rows.len() && rows[index].row.key == key {
                let row = rows[index].clone();
                if row.row.sequence > horizon {
                    retained.push(row);
                } else if !anchor_kept {
                    retained.push(row);
                    anchor_kept = true;
                }
                index += 1;
            }
        }

        retained
    }

    pub(super) async fn load_compaction_input_rows(
        &self,
        table: &StoredTable,
        inputs: &[ResidentRowSstable],
    ) -> Result<Vec<CompactionRow>, StorageError> {
        let projection = match table.config.format {
            TableFormat::Row => None,
            TableFormat::Columnar => {
                Some(Self::resolve_scan_projection(table, None)?.ok_or_else(|| {
                    StorageError::corruption("columnar table is missing a schema")
                })?)
            }
        };
        let mut rows = Vec::new();

        for sstable in inputs {
            if !sstable.is_columnar() {
                rows.extend(sstable.rows.iter().cloned().map(|row| CompactionRow {
                    level: sstable.meta.level,
                    row,
                }));
                continue;
            }

            let projection = projection.as_ref().ok_or_else(|| {
                StorageError::corruption(format!(
                    "row table {} unexpectedly references a columnar SSTable",
                    table.config.name
                ))
            })?;
            let metadata = sstable
                .load_columnar_metadata(
                    &self.inner.columnar_read_context,
                    ColumnarReadAccessPattern::Background,
                )
                .await?;
            let location = sstable.meta.storage_descriptor();
            let mut row_kinds = Vec::with_capacity(metadata.key_index.len());
            let mut row_indexes = BTreeSet::new();
            for row_index in 0..metadata.key_index.len() {
                let kind = Self::normalized_columnar_row_kind(
                    location,
                    row_index,
                    metadata.tombstones[row_index],
                    metadata.row_kinds[row_index],
                )?;
                row_kinds.push(kind);
                if kind != ChangeKind::Delete {
                    row_indexes.insert(row_index);
                }
            }
            let values = sstable
                .materialize_columnar_rows(
                    &self.inner.columnar_read_context,
                    projection,
                    &row_indexes,
                    ColumnarReadAccessPattern::Background,
                )
                .await?;

            for (row_index, kind) in row_kinds.iter().copied().enumerate() {
                let value = if kind == ChangeKind::Delete {
                    None
                } else {
                    Some(values.rows.get(&row_index).cloned().ok_or_else(|| {
                        StorageError::corruption(format!(
                            "columnar SSTable {} row {} was not materialized for compaction",
                            location, row_index
                        ))
                    })?)
                };
                rows.push(CompactionRow {
                    level: sstable.meta.level,
                    row: SstableRow {
                        key: metadata.key_index[row_index].clone(),
                        sequence: metadata.sequences[row_index],
                        kind,
                        value,
                    },
                });
            }
        }

        Ok(rows)
    }

    pub(super) async fn rewrite_compaction_rows(
        &self,
        tables: &BTreeMap<String, StoredTable>,
        memtables: &MemtableState,
        sstables: &SstableState,
        table: &StoredTable,
        rows: Vec<CompactionRow>,
    ) -> Result<Vec<CompactionRow>, StorageError> {
        let columnar_projection =
            if table.config.format == TableFormat::Columnar {
                Some(Self::resolve_scan_projection(table, None)?.ok_or_else(|| {
                    StorageError::corruption("columnar table is missing a schema")
                })?)
            } else {
                None
            };
        let mut rewritten = Vec::with_capacity(rows.len());
        for row in rows {
            if row.row.kind != ChangeKind::Merge {
                rewritten.push(row);
                continue;
            }

            let resolved = match table.config.format {
                TableFormat::Row => Self::resolve_visible_value_with_state(
                    tables,
                    memtables,
                    sstables,
                    table.id,
                    &row.row.key,
                    row.row.sequence,
                )?,
                TableFormat::Columnar => {
                    self.resolve_visible_value_columnar_with_state(
                        table,
                        memtables,
                        sstables,
                        &row.row.key,
                        row.row.sequence,
                        columnar_projection
                            .as_ref()
                            .expect("columnar projection should exist"),
                        ColumnarReadAccessPattern::Background,
                    )
                    .await?
                    .0
                }
            };
            let value = resolved.value.ok_or_else(|| {
                StorageError::corruption("merge resolution unexpectedly produced a tombstone")
            })?;
            rewritten.push(CompactionRow {
                level: row.level,
                row: SstableRow {
                    key: row.row.key,
                    sequence: row.row.sequence,
                    kind: ChangeKind::Put,
                    value: Some(value),
                },
            });
        }

        Ok(rewritten)
    }

    pub(super) fn apply_compaction_filter(
        &self,
        table: &StoredTable,
        rows: Vec<CompactionRow>,
    ) -> FilteredCompactionRows {
        let Some(filter) = table.config.compaction_filter.as_ref() else {
            return FilteredCompactionRows {
                rows,
                ..FilteredCompactionRows::default()
            };
        };

        let now = self.inner.dependencies.clock.now();
        let oldest_active_snapshot = self.oldest_active_snapshot_sequence();
        let mut retained = Vec::with_capacity(rows.len());
        let mut filtered_keys = BTreeSet::new();
        let mut removed_bytes = 0_u64;

        for row in rows {
            let snapshot_protected = oldest_active_snapshot
                .is_some_and(|oldest_snapshot| row.row.sequence >= oldest_snapshot);
            if snapshot_protected {
                retained.push(row);
                continue;
            }

            let decision = filter.decide(CompactionDecisionContext {
                level: row.level,
                key: &row.row.key,
                value: row.row.value.as_ref(),
                sequence: row.row.sequence,
                kind: row.row.kind,
                now,
            });
            if decision == CompactionDecision::Remove {
                removed_bytes =
                    removed_bytes.saturating_add(Self::estimated_sstable_row_bytes(&row.row));
                filtered_keys.insert(row.row.key.clone());
            } else {
                retained.push(row);
            }
        }

        let retained_keys = retained
            .iter()
            .map(|row| row.row.key.clone())
            .collect::<BTreeSet<_>>();
        let removed_keys = filtered_keys
            .into_iter()
            .filter(|key| !retained_keys.contains(key))
            .count() as u64;

        FilteredCompactionRows {
            rows: retained,
            removed_bytes,
            removed_keys,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn write_compaction_outputs(
        &self,
        local_root: &str,
        table: &StoredTable,
        level: u32,
        rows: Vec<SstableRow>,
        applied_generation: ManifestId,
        inputs: &[ResidentRowSstable],
    ) -> Result<Vec<ResidentRowSstable>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let target_bytes =
            Self::leveled_level_target_bytes(level).max(LEVELED_BASE_LEVEL_TARGET_BYTES);
        let mut outputs = Vec::new();
        let mut start = 0_usize;

        while start < rows.len() {
            let mut end = start;
            let mut estimated_bytes = 0_u64;
            while end < rows.len() {
                estimated_bytes =
                    estimated_bytes.saturating_add(Self::estimated_sstable_row_bytes(&rows[end]));
                end += 1;

                let next_shares_key = end < rows.len() && rows[end - 1].key == rows[end].key;
                if estimated_bytes >= target_bytes && !next_shares_key {
                    break;
                }
            }

            let local_id = format!(
                "SST-{:06}",
                self.inner.next_sstable_id.fetch_add(1, Ordering::SeqCst)
            );
            let path = Self::local_sstable_path(local_root, table.id, &local_id);
            let output = match table.config.format {
                TableFormat::Row => {
                    self.write_row_sstable(
                        &path,
                        table.id,
                        level,
                        local_id,
                        rows[start..end].to_vec(),
                        table.config.bloom_filter_bits_per_key,
                    )
                    .await?
                }
                TableFormat::Columnar => {
                    self.write_columnar_table_output(
                        &path,
                        level,
                        local_id,
                        table,
                        rows[start..end].to_vec(),
                        Some(applied_generation),
                        inputs,
                    )
                    .await?
                }
            };
            outputs.push(output);
            start = end;
        }

        Ok(outputs)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn run_next_offload(&self) -> Result<bool, StorageError> {
        let Some(_) = self.local_storage_root() else {
            return Ok(false);
        };
        let _maintenance_guard = self.inner.maintenance_lock.lock().await;
        let Some(job) = self
            .pending_offload_candidates()
            .into_iter()
            .find_map(|candidate| match candidate.spec {
                PendingWorkSpec::Offload(job) => Some(job),
                PendingWorkSpec::Flush | PendingWorkSpec::Compaction(_) => None,
            })
        else {
            return Ok(false);
        };

        self.execute_offload_job(job).await?;
        Ok(true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn run_next_compaction(&self) -> Result<bool, StorageError> {
        let span = tracing::info_span!("terracedb.db.compaction.run_next");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::WORK_KIND,
            opentelemetry::Value::String("compaction".into()),
        );
        let span_for_attrs = span.clone();

        async move {
            let Some(local_root) = self.local_storage_root().map(str::to_string) else {
                return Ok(false);
            };
            let _maintenance_guard = self.inner.maintenance_lock.lock().await;
            let Some(job) = self.pending_compaction_jobs().into_iter().next() else {
                return Ok(false);
            };
            apply_table_span_attribute(&span_for_attrs, &job.table_name);
            crate::set_span_attribute(
                &span_for_attrs,
                "terracedb.compaction.target_level",
                job.target_level,
            );
            crate::set_span_attribute(
                &span_for_attrs,
                "terracedb.compaction.input_count",
                job.input_local_ids.len() as u64,
            );

            self.execute_compaction_job(&local_root, job).await?;
            Ok(true)
        }
        .instrument(span.clone())
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn execute_offload_job(&self, job: OffloadJob) -> Result<(), StorageError> {
        let span = tracing::info_span!("terracedb.db.offload.execute");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::WORK_KIND,
            opentelemetry::Value::String("offload".into()),
        );
        apply_table_span_attribute(&span, &job.table_name);
        crate::set_span_attribute(
            &span,
            "terracedb.offload.input_count",
            job.input_local_ids.len() as u64,
        );

        async move {
            let StorageConfig::Tiered(config) = &self.inner.config.storage else {
                return Err(StorageError::unsupported(
                    "cold offload is only supported in tiered mode",
                ));
            };

            let tables = self.tables_read().clone();
            let table = Self::stored_table_by_id(&tables, job.table_id)
                .cloned()
                .ok_or_else(|| {
                    StorageError::corruption(format!(
                        "offload references unknown table id {}",
                        job.table_id.get()
                    ))
                })?;
            if table.config.format != TableFormat::Row {
                return Err(StorageError::unsupported(
                    "cold offload only supports row tables",
                ));
            }

            let live = self.sstables_read().live.clone();
            let by_local_id = live
                .into_iter()
                .map(|sstable| (sstable.meta.local_id.clone(), sstable))
                .collect::<BTreeMap<_, _>>();
            let mut inputs = Vec::with_capacity(job.input_local_ids.len());
            for local_id in &job.input_local_ids {
                let input = by_local_id.get(local_id).cloned().ok_or_else(|| {
                    StorageError::corruption(format!(
                        "offload job {} resolved no SSTable for {}",
                        job.id, local_id
                    ))
                })?;
                if input.meta.table_id != job.table_id {
                    return Err(StorageError::corruption(format!(
                        "offload job {} references SSTable {} from another table",
                        job.id, local_id
                    )));
                }
                if input.meta.file_path.is_empty() {
                    return Err(StorageError::corruption(format!(
                        "offload job {} references non-local SSTable {}",
                        job.id, local_id
                    )));
                }
                inputs.push(input);
            }

            let layout = ObjectKeyLayout::new(&config.s3);
            let current_state = self.sstables_read().clone();
            let current_live = current_state.live.clone();
            let input_local_ids = job.input_local_ids.iter().cloned().collect::<BTreeSet<_>>();
            let new_live = match job.kind {
                OffloadJobKind::Offload => {
                    let storage = UnifiedStorage::from_dependencies(&self.inner.dependencies);
                    let mut updated_inputs = BTreeMap::new();
                    for input in &inputs {
                        let remote_key = layout.cold_sstable(
                            input.meta.table_id,
                            0,
                            input.meta.min_sequence,
                            input.meta.max_sequence,
                            &input.meta.local_id,
                        );
                        let bytes =
                            read_path(&self.inner.dependencies, &input.meta.file_path).await?;
                        storage
                            .put_object(&remote_key, &bytes)
                            .await
                            .map_err(|error| error.into_storage_error())?;
                        Self::note_backup_object_birth(
                            &self.inner.dependencies,
                            &layout,
                            &remote_key,
                        )
                        .await;

                        let mut updated = input.clone();
                        updated.meta.file_path.clear();
                        updated.meta.remote_key = Some(remote_key);
                        updated_inputs.insert(updated.meta.local_id.clone(), updated);
                    }

                    self.maybe_pause_offload_phase(OffloadPhase::UploadComplete)
                        .await?;

                    let mut replaced = 0_usize;
                    let mut new_live = current_live
                        .into_iter()
                        .map(|sstable| {
                            if let Some(updated) = updated_inputs.get(&sstable.meta.local_id) {
                                replaced += 1;
                                updated.clone()
                            } else {
                                sstable
                            }
                        })
                        .collect::<Vec<_>>();
                    if replaced != job.input_local_ids.len() {
                        return Err(StorageError::corruption(format!(
                            "offload job {} replaced {} of {} SSTables",
                            job.id,
                            replaced,
                            job.input_local_ids.len()
                        )));
                    }
                    Self::sort_live_sstables(&mut new_live);
                    new_live
                }
                OffloadJobKind::Delete => {
                    let mut new_live = current_live
                        .into_iter()
                        .filter(|sstable| !input_local_ids.contains(&sstable.meta.local_id))
                        .collect::<Vec<_>>();
                    Self::sort_live_sstables(&mut new_live);
                    new_live
                }
            };

            let next_generation =
                ManifestId::new(current_state.manifest_generation.get().saturating_add(1));
            self.install_manifest(
                next_generation,
                current_state.last_flushed_sequence,
                &new_live,
            )
            .await?;

            {
                let mut sstables = self.sstables_write();
                sstables.manifest_generation = next_generation;
                sstables.live = new_live;
            }
            self.retain_compact_to_wide_stats_for_live(&self.sstables_read().live);
            let state = self.sstables_read().clone();
            let _ = self
                .sync_tiered_backup_manifest(
                    state.manifest_generation,
                    state.last_flushed_sequence,
                    &state.live,
                )
                .await;

            self.maybe_pause_offload_phase(OffloadPhase::ManifestSwitched)
                .await?;

            for input in &inputs {
                self.inner
                    .dependencies
                    .file_system
                    .delete(&input.meta.file_path)
                    .await?;
            }

            self.maybe_pause_offload_phase(OffloadPhase::LocalCleanupFinished)
                .await?;

            Ok(())
        }
        .instrument(span.clone())
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) async fn execute_compaction_job(
        &self,
        local_root: &str,
        job: CompactionJob,
    ) -> Result<(), StorageError> {
        let span = tracing::info_span!("terracedb.db.compaction.execute");
        apply_db_span_attributes(
            &span,
            &self.telemetry_db_name(),
            &self.telemetry_db_instance(),
            self.telemetry_storage_mode(),
        );
        crate::set_span_attribute(
            &span,
            crate::telemetry_attrs::WORK_KIND,
            opentelemetry::Value::String("compaction".into()),
        );
        apply_table_span_attribute(&span, &job.table_name);
        crate::set_span_attribute(&span, "terracedb.compaction.target_level", job.target_level);
        crate::set_span_attribute(
            &span,
            "terracedb.compaction.input_count",
            job.input_local_ids.len() as u64,
        );

        async move {
            let tables = self.tables_read().clone();
            let memtables = self.memtables_read().clone();
            let sstables = self.sstables_read().clone();
            let table = Self::stored_table_by_id(&tables, job.table_id)
                .cloned()
                .ok_or_else(|| {
                    StorageError::corruption(format!(
                        "compaction references unknown table id {}",
                        job.table_id.get()
                    ))
                })?;
            let live = sstables.live.clone();
            let input_local_ids = job.input_local_ids.iter().cloned().collect::<BTreeSet<_>>();
            let inputs = live
                .iter()
                .filter(|sstable| input_local_ids.contains(&sstable.meta.local_id))
                .cloned()
                .collect::<Vec<_>>();
            if inputs.len() != job.input_local_ids.len() {
                return Err(StorageError::corruption(format!(
                    "compaction job {} resolved {} of {} inputs",
                    job.id,
                    inputs.len(),
                    job.input_local_ids.len()
                )));
            }

            let filtered = match job.kind {
                CompactionJobKind::Rewrite => {
                    let mut merged_rows = self.load_compaction_input_rows(&table, &inputs).await?;
                    merged_rows.sort_by_key(|row| {
                        encode_mvcc_key(&row.row.key, CommitId::new(row.row.sequence))
                    });
                    merged_rows = self
                        .rewrite_compaction_rows(
                            &tables,
                            &memtables,
                            &sstables,
                            &table,
                            merged_rows,
                        )
                        .await?;
                    merged_rows = Self::retain_rows_within_horizon(
                        merged_rows,
                        self.history_gc_horizon(job.table_id),
                    );
                    self.apply_compaction_filter(&table, merged_rows)
                }
                CompactionJobKind::DeleteOnly => FilteredCompactionRows::default(),
            };

            let next_generation =
                ManifestId::new(sstables.manifest_generation.get().saturating_add(1));
            let outputs = match job.kind {
                CompactionJobKind::Rewrite => {
                    let outputs = self
                        .write_compaction_outputs(
                            local_root,
                            &table,
                            job.target_level,
                            filtered
                                .rows
                                .iter()
                                .cloned()
                                .map(|row| row.row)
                                .collect::<Vec<_>>(),
                            next_generation,
                            &inputs,
                        )
                        .await?;

                    if !outputs.is_empty() {
                        self.maybe_pause_compaction_phase(CompactionPhase::OutputWritten)
                            .await?;
                    }

                    outputs
                }
                CompactionJobKind::DeleteOnly => Vec::new(),
            };

            let current_state = self.sstables_read().clone();
            let mut new_live = current_state
                .live
                .into_iter()
                .filter(|sstable| !input_local_ids.contains(&sstable.meta.local_id))
                .collect::<Vec<_>>();
            new_live.extend(outputs);
            Self::sort_live_sstables(&mut new_live);

            self.install_manifest(
                next_generation,
                current_state.last_flushed_sequence,
                &new_live,
            )
            .await?;

            {
                let mut sstables = self.sstables_write();
                sstables.manifest_generation = next_generation;
                sstables.live = new_live;
            }
            self.retain_compact_to_wide_stats_for_live(&self.sstables_read().live);
            let state = self.sstables_read().clone();
            let _ = self
                .sync_tiered_backup_manifest(
                    state.manifest_generation,
                    state.last_flushed_sequence,
                    &state.live,
                )
                .await;
            self.record_compaction_filter_stats(
                job.table_id,
                filtered.removed_bytes,
                filtered.removed_keys,
            );

            self.maybe_pause_compaction_phase(CompactionPhase::ManifestSwitched)
                .await?;

            for input in &inputs {
                self.inner
                    .dependencies
                    .file_system
                    .delete(&input.meta.file_path)
                    .await?;
            }

            self.maybe_pause_compaction_phase(CompactionPhase::InputCleanupFinished)
                .await?;

            Ok(())
        }
        .instrument(span.clone())
        .await
    }
}
