use arc_swap::ArcSwap;

use super::*;

#[derive(Debug)]
pub struct WatermarkReceiver {
    inner: watch::Receiver<SequenceNumber>,
    subscription: WatermarkSubscription,
}

impl WatermarkReceiver {
    pub(super) fn new(
        registry: &Arc<WatermarkRegistry>,
        table_name: &str,
        inner: watch::Receiver<SequenceNumber>,
    ) -> Self {
        Self {
            inner,
            subscription: WatermarkSubscription::new(registry, table_name),
        }
    }

    pub fn current(&self) -> SequenceNumber {
        *self.inner.borrow()
    }

    pub async fn changed(&mut self) -> Result<SequenceNumber, SubscriptionClosed> {
        self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        Ok(*self.inner.borrow_and_update())
    }

    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<SequenceNumber, SubscriptionClosed>
    where
        F: FnMut(SequenceNumber) -> bool,
    {
        let current = self.current();
        if predicate(current) {
            return Ok(current);
        }

        loop {
            let current = self.changed().await?;
            if predicate(current) {
                return Ok(current);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn has_changed(&self) -> Result<bool, SubscriptionClosed> {
        self.inner.has_changed().map_err(|_| SubscriptionClosed)
    }
}

impl Clone for WatermarkReceiver {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            subscription: self.subscription.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DbProgressSnapshot {
    pub current_sequence: SequenceNumber,
    pub durable_sequence: SequenceNumber,
    pub reserved_sequence: SequenceNumber,
}

#[derive(Debug)]
pub struct DbProgressSubscription {
    inner: watch::Receiver<Arc<DbProgressSnapshot>>,
}

impl DbProgressSubscription {
    pub(super) fn new(inner: watch::Receiver<Arc<DbProgressSnapshot>>) -> Self {
        Self { inner }
    }

    pub fn current(&self) -> DbProgressSnapshot {
        self.inner.borrow().as_ref().clone()
    }

    pub async fn changed(&mut self) -> Result<DbProgressSnapshot, SubscriptionClosed> {
        self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        Ok(self.current())
    }

    /// Waits until the current or next published progress snapshot satisfies `predicate`.
    pub async fn wait_for<F>(
        &mut self,
        mut predicate: F,
    ) -> Result<DbProgressSnapshot, SubscriptionClosed>
    where
        F: FnMut(&DbProgressSnapshot) -> bool,
    {
        let snapshot = self.current();
        if predicate(&snapshot) {
            return Ok(snapshot);
        }

        loop {
            let snapshot = self.changed().await?;
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
        }
    }
}

impl Clone for DbProgressSubscription {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Default)]
pub(super) struct WatermarkAdvance {
    pub(super) visible_sequence: Option<SequenceNumber>,
    pub(super) visible_tables: BTreeMap<String, SequenceNumber>,
    pub(super) durable_sequence: Option<SequenceNumber>,
    pub(super) durable_tables: BTreeMap<String, SequenceNumber>,
}

#[derive(Debug)]
pub(super) struct DbProgressPublisher {
    latest_snapshot: ArcSwap<DbProgressSnapshot>,
    published_snapshot: watch::Sender<Arc<DbProgressSnapshot>>,
}

impl DbProgressPublisher {
    pub(super) fn new(
        current_sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        reserved_sequence: SequenceNumber,
    ) -> Self {
        let initial_snapshot = Arc::new(DbProgressSnapshot {
            current_sequence,
            durable_sequence,
            reserved_sequence,
        });
        let (published_snapshot, _receiver) = watch::channel(initial_snapshot.clone());
        Self {
            latest_snapshot: ArcSwap::from(initial_snapshot),
            published_snapshot,
        }
    }

    pub(super) fn snapshot(&self) -> DbProgressSnapshot {
        self.latest_snapshot.load_full().as_ref().clone()
    }

    pub(super) fn subscribe(&self) -> DbProgressSubscription {
        DbProgressSubscription::new(self.published_snapshot.subscribe())
    }

    pub(super) fn publish(
        &self,
        current_sequence: SequenceNumber,
        durable_sequence: SequenceNumber,
        reserved_sequence: SequenceNumber,
    ) {
        let snapshot = Arc::new(DbProgressSnapshot {
            current_sequence,
            durable_sequence,
            reserved_sequence,
        });
        self.latest_snapshot.store(snapshot.clone());
        self.published_snapshot.send_replace(snapshot);
    }
}

#[derive(Debug)]
pub(super) struct WatermarkSubscription {
    pub(super) registry: Weak<WatermarkRegistry>,
    pub(super) table_name: String,
}

impl WatermarkSubscription {
    pub(super) fn new(registry: &Arc<WatermarkRegistry>, table_name: &str) -> Self {
        Self {
            registry: Arc::downgrade(registry),
            table_name: table_name.to_string(),
        }
    }
}

impl Clone for WatermarkSubscription {
    fn clone(&self) -> Self {
        if let Some(registry) = self.registry.upgrade() {
            registry.increment(&self.table_name);
        }

        Self {
            registry: self.registry.clone(),
            table_name: self.table_name.clone(),
        }
    }
}

impl Drop for WatermarkSubscription {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.decrement(&self.table_name);
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct WatermarkTableState {
    pub(super) current: SequenceNumber,
    pub(super) sender: Option<watch::Sender<SequenceNumber>>,
    pub(super) subscribers: usize,
}

impl WatermarkTableState {
    pub(super) fn new(current: SequenceNumber) -> Self {
        Self {
            current,
            sender: None,
            subscribers: 0,
        }
    }
}

#[derive(Debug)]
pub(super) struct WatermarkRegistry {
    pub(super) tables: Mutex<BTreeMap<String, WatermarkTableState>>,
    latest_snapshot: ArcSwap<BTreeMap<String, SequenceNumber>>,
    published_snapshot: watch::Sender<Arc<BTreeMap<String, SequenceNumber>>>,
}

impl WatermarkRegistry {
    pub(super) fn new(initial: BTreeMap<String, SequenceNumber>) -> Self {
        let tables = initial
            .clone()
            .into_iter()
            .map(|(table, sequence)| (table, WatermarkTableState::new(sequence)))
            .collect();
        let initial_snapshot = Arc::new(initial);
        let (published_snapshot, _receiver) = watch::channel(initial_snapshot.clone());
        Self {
            tables: Mutex::new(tables),
            latest_snapshot: ArcSwap::from(initial_snapshot),
            published_snapshot,
        }
    }

    pub(super) fn subscribe(self: &Arc<Self>, table_name: &str) -> WatermarkReceiver {
        let receiver = {
            let mut tables = mutex_lock(&self.tables);
            let state = tables
                .entry(table_name.to_string())
                .or_insert_with(|| WatermarkTableState::new(SequenceNumber::default()));
            state.subscribers += 1;
            let current = state.current;
            let sender = state.sender.get_or_insert_with(|| {
                let (sender, _receiver) = watch::channel(current);
                sender
            });
            sender.subscribe()
        };

        WatermarkReceiver::new(self, table_name, receiver)
    }

    pub(super) fn increment(&self, table_name: &str) {
        if let Some(state) = mutex_lock(&self.tables).get_mut(table_name) {
            state.subscribers += 1;
        }
    }

    pub(super) fn decrement(&self, table_name: &str) {
        if let Some(state) = mutex_lock(&self.tables).get_mut(table_name) {
            state.subscribers = state.subscribers.saturating_sub(1);
            if state.subscribers == 0 {
                state.sender = None;
            }
        }
    }

    pub(super) fn notify(&self, updates: &BTreeMap<String, SequenceNumber>) {
        if updates.is_empty() {
            return;
        }

        let mut tables = mutex_lock(&self.tables);
        let mut advanced = false;
        for (table, sequence) in updates {
            let state = tables
                .entry(table.clone())
                .or_insert_with(|| WatermarkTableState::new(SequenceNumber::default()));
            if *sequence <= state.current {
                continue;
            }

            state.current = *sequence;
            if let Some(sender) = &state.sender {
                sender.send_replace(*sequence);
            }
            advanced = true;
        }
        let snapshot = advanced.then(|| {
            Arc::new(
                tables
                    .iter()
                    .map(|(table, state)| (table.clone(), state.current))
                    .collect::<BTreeMap<_, _>>(),
            )
        });
        drop(tables);

        if let Some(snapshot) = snapshot {
            self.latest_snapshot.store(snapshot.clone());
            self.published_snapshot.send_replace(snapshot);
        }
    }

    pub(super) fn snapshot(&self) -> Arc<BTreeMap<String, SequenceNumber>> {
        self.latest_snapshot.load_full()
    }

    pub(super) fn subscribe_snapshot(
        &self,
    ) -> watch::Receiver<Arc<BTreeMap<String, SequenceNumber>>> {
        self.published_snapshot.subscribe()
    }

    #[cfg(test)]
    pub(super) fn active_subscriber_count(&self, table_name: &str) -> usize {
        mutex_lock(&self.tables)
            .get(table_name)
            .map(|state| state.subscribers)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatermarkUpdate {
    pub table: String,
    pub sequence: SequenceNumber,
}

#[derive(Debug)]
pub struct WatermarkSubscriptionSet {
    inner: watch::Receiver<Arc<BTreeMap<String, SequenceNumber>>>,
    tables: Vec<String>,
    observed: Vec<SequenceNumber>,
}

impl WatermarkSubscriptionSet {
    pub(super) fn new(registry: &Arc<WatermarkRegistry>, tables: Vec<String>) -> Self {
        let mut tables = tables;
        tables.sort();

        let snapshot = registry.snapshot();
        let observed = tables
            .iter()
            .map(|table| snapshot.get(table).copied().unwrap_or_default())
            .collect();

        Self {
            inner: registry.subscribe_snapshot(),
            tables,
            observed,
        }
    }

    pub fn pending_updates(&mut self) -> Vec<WatermarkUpdate> {
        let mut pending = Vec::new();
        let snapshot = self.inner.borrow().clone();
        for (table, observed) in self.tables.iter().zip(self.observed.iter_mut()) {
            let current = snapshot.get(table).copied().unwrap_or_default();
            if current <= *observed {
                continue;
            }

            *observed = current;
            pending.push(WatermarkUpdate {
                table: table.clone(),
                sequence: current,
            });
        }

        pending
    }

    pub async fn changed(&mut self) -> Result<Vec<WatermarkUpdate>, SubscriptionClosed> {
        loop {
            let pending = self.pending_updates();
            if !pending.is_empty() {
                return Ok(pending);
            }

            self.inner.changed().await.map_err(|_| SubscriptionClosed)?;
        }
    }
}
