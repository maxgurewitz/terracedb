#[derive(Clone)]
pub struct Table {
    db: Db,
    name: Arc<str>,
    id: Option<TableId>,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn id(&self) -> Option<TableId> {
        self.id
    }

    pub async fn read(&self, key: Key) -> Result<Option<Value>, ReadError> {
        self.read_at(key, self.db.current_sequence()).await
    }

    pub async fn write(&self, key: Key, value: Value) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.put(self, key, value);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn delete(&self, key: Key) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.delete(self, key);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn merge(&self, key: Key, delta: Value) -> Result<SequenceNumber, WriteError> {
        let mut batch = self.db.write_batch();
        batch.merge(self, key, delta);
        self.db
            .commit(batch, CommitOptions::default())
            .await
            .map_err(Into::into)
    }

    pub async fn scan(
        &self,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        self.scan_at(start, end, self.db.current_sequence(), opts)
            .await
    }

    pub async fn scan_prefix(
        &self,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        self.scan_prefix_at(prefix, self.db.current_sequence(), opts)
            .await
    }

    pub async fn read_at(
        &self,
        key: Key,
        sequence: SequenceNumber,
    ) -> Result<Option<Value>, ReadError> {
        let Some(table_id) = self.resolve_id() else {
            return Ok(None);
        };
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
        let Some(table_id) = self.resolve_id() else {
            return Ok(Box::pin(stream::empty()));
        };
        self.db.validate_historical_read(table_id, sequence)?;

        let rows = self
            .db
            .scan_visible(
                table_id,
                sequence,
                KeyMatcher::Range {
                    start: &start,
                    end: &end,
                },
                &opts,
            )
            .await?;
        Ok(Box::pin(stream::iter(rows)))
    }

    async fn scan_prefix_at(
        &self,
        prefix: KeyPrefix,
        sequence: SequenceNumber,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        let Some(table_id) = self.resolve_id() else {
            return Ok(Box::pin(stream::empty()));
        };
        self.db.validate_historical_read(table_id, sequence)?;

        let rows = self
            .db
            .scan_visible(table_id, sequence, KeyMatcher::Prefix(&prefix), &opts)
            .await?;
        Ok(Box::pin(stream::iter(rows)))
    }

    fn resolve_id(&self) -> Option<TableId> {
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
