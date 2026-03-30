#[derive(Clone)]
pub struct Snapshot {
    registration: Arc<SnapshotRegistration>,
}

struct SnapshotRegistration {
    db: Db,
    id: u64,
    sequence: SequenceNumber,
    released: AtomicBool,
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
        table.scan_at(start, end, self.sequence(), opts).await
    }

    pub async fn scan_prefix(
        &self,
        table: &Table,
        prefix: KeyPrefix,
        opts: ScanOptions,
    ) -> Result<KvStream, ReadError> {
        table.scan_prefix_at(prefix, self.sequence(), opts).await
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
