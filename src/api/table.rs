use super::*;

#[derive(Clone)]
pub struct Table {
    pub(super) db: Db,
    pub(super) name: Arc<str>,
    pub(super) id: Option<TableId>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Option<TableId> {
        self.id
    }

    pub fn sharding_state(&self) -> Option<crate::TableShardingState> {
        let stored = self.db.resolve_stored_table(self)?;
        Some(
            crate::TableShardingState::new(stored.id, &stored.config)
                .expect("stored table sharding configs should already be validated"),
        )
    }

    pub fn route_key(&self, key: &[u8]) -> Option<crate::KeyShardRoute> {
        Some(self.sharding_state()?.route_key(key))
    }

    pub fn validate_shard_map(
        &self,
        sharding: &crate::ShardingConfig,
    ) -> Result<crate::TableShardingState, crate::PublishShardMapError> {
        self.db.validate_table_shard_map(self, sharding)
    }

    pub async fn publish_shard_map(
        &self,
        sharding: crate::ShardingConfig,
    ) -> Result<crate::TableShardingState, crate::PublishShardMapError> {
        self.db.publish_table_shard_map(self, sharding).await
    }

    pub async fn read(&self, key: Key) -> Result<Option<Value>, ReadError> {
        self.read_at(key, self.db.current_sequence()).await
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<SequenceNumber, WriteError> {
        self.write_with_options(key, value, CommitOptions::default())
            .await
    }

    pub async fn write_with_options(
        &self,
        key: Key,
        value: Value,
        opts: CommitOptions,
    ) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.put(self, key, value);
        self.db.commit(batch, opts).await.map_err(Into::into)
    }

    pub async fn delete(&self, key: Key) -> Result<SequenceNumber, WriteError> {
        self.delete_with_options(key, CommitOptions::default())
            .await
    }

    pub async fn delete_with_options(
        &self,
        key: Key,
        opts: CommitOptions,
    ) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.delete(self, key);
        self.db.commit(batch, opts).await.map_err(Into::into)
    }

    pub async fn merge(&self, key: Key, delta: Value) -> Result<SequenceNumber, WriteError> {
        self.merge_with_options(key, delta, CommitOptions::default())
            .await
    }

    pub async fn merge_with_options(
        &self,
        key: Key,
        delta: Value,
        opts: CommitOptions,
    ) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.merge(self, key, delta);
        self.db.commit(batch, opts).await.map_err(Into::into)
    }

    pub async fn scan(
        &self,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(self
            .scan_at_with_execution(start, end, self.db.current_sequence(), opts)
            .await?
            .0)
    }

    pub async fn scan_prefix(
        &self,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(self
            .scan_prefix_at_with_execution(prefix, self.db.current_sequence(), opts)
            .await?
            .0)
    }

    pub async fn read_at(
        &self,
        key: Key,
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, ReadError> {
        let table_id = self
            .resolve_id()
            .ok_or_else(|| Db::missing_table_error(self.name()))?;
        self.db.validate_historical_read(table_id, sequence)?;

        Ok(self.db.read_visible_value(table_id, &key, sequence).await?)
    }

    pub async fn scan_at(
        &self,
        start: Key,
        end: Key,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(self
            .scan_at_with_execution(start, end, sequence, opts)
            .await?
            .0)
    }

    pub async fn scan_with_execution(
        &self,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        self.scan_at_with_execution(start, end, self.db.current_sequence(), opts)
            .await
    }

    pub async fn scan_prefix_with_execution(
        &self,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        self.scan_prefix_at_with_execution(prefix, self.db.current_sequence(), opts)
            .await
    }

    pub async fn scan_at_with_execution(
        &self,
        start: Key,
        end: Key,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        let table_id = self
            .resolve_id()
            .ok_or_else(|| Db::missing_table_error(self.name()))?;
        self.db.validate_historical_read(table_id, sequence)?;

        let (rows, execution) = self
            .db
            .scan_visible_with_execution(
                table_id,
                sequence,
                KeyMatcher::Range {
                    start: &start,
                    end: &end,
                },
                &opts,
            )
            .await?;
        Ok((Box::pin(stream::iter(rows)), execution))
    }

    pub async fn scan_prefix_at(
        &self,
        prefix: KeyPrefix,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(self
            .scan_prefix_at_with_execution(prefix, sequence, opts)
            .await?
            .0)
    }

    pub async fn scan_prefix_at_with_execution(
        &self,
        prefix: KeyPrefix,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        let table_id = self
            .resolve_id()
            .ok_or_else(|| Db::missing_table_error(self.name()))?;
        self.db.validate_historical_read(table_id, sequence)?;

        let (rows, execution) = self
            .db
            .scan_visible_with_execution(table_id, sequence, KeyMatcher::Prefix(&prefix), &opts)
            .await?;
        Ok((Box::pin(stream::iter(rows)), execution))
    }

    pub async fn probe_skip_indexes(
        &self,
        probe: crate::SkipIndexProbe,
    ) -> Result<Vec<crate::SkipIndexProbeResult>, ReadError> {
        let table_id = self
            .resolve_id()
            .ok_or_else(|| Db::missing_table_error(self.name()))?;
        self.db.probe_skip_indexes(table_id, &probe).await
    }

    pub(super) fn resolve_id(&self) -> Option<TableId> {
        self.db.resolve_table_id(self)
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("id", &self.id)
            .finish()
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.id == other.id
    }
}

impl Eq for Table {}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.id.hash(state);
    }
}
