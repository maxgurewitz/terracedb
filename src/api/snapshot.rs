use super::*;

#[derive(Clone)]
pub struct Snapshot {
    pub(super) registration: Arc<SnapshotRegistration>,
}

pub(super) struct SnapshotRegistration {
    pub(super) db: Db,
    pub(super) id: u64,
    pub(super) sequence: SequenceNumber,
    pub(super) released: AtomicBool,
}

impl Snapshot {
    pub fn sequence(&self) -> SequenceNumber {
        self.registration.sequence
    }

    pub async fn read(&self, table: &Table, key: Key) -> Result<Option<Value>, ReadError> {
        table.read_at(key, self.sequence()).await
    }

    pub async fn scan(
        &self,
        table: &Table,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(table
            .scan_at_with_execution(start, end, self.sequence(), opts)
            .await?
            .0)
    }

    pub async fn scan_prefix(
        &self,
        table: &Table,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        Ok(table
            .scan_prefix_at_with_execution(prefix, self.sequence(), opts)
            .await?
            .0)
    }

    pub async fn scan_with_execution(
        &self,
        table: &Table,
        start: Key,
        end: Key,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        table
            .scan_at_with_execution(start, end, self.sequence(), opts)
            .await
    }

    pub async fn scan_prefix_with_execution(
        &self,
        table: &Table,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<(KvStream, ScanExecution), ReadError> {
        table
            .scan_prefix_at_with_execution(prefix, self.sequence(), opts)
            .await
    }

    pub fn release(&self) {
        if self.registration.released.swap(true, Ordering::SeqCst) {
            return;
        }

        self.registration
            .db
            .release_snapshot_registration(self.registration.id);
    }

    pub fn is_released(&self) -> bool {
        self.registration.released.load(Ordering::SeqCst)
    }

    pub fn db(&self) -> &Db {
        &self.registration.db
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("sequence", &self.sequence())
            .field("released", &self.is_released())
            .finish()
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if Arc::strong_count(&self.registration) == 1 {
            self.release();
        }
    }
}
