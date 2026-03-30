use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc};

use crate::{DbDependencies, StorageError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailpointMode {
    Once,
    Persistent,
}

#[derive(Clone, Debug)]
pub enum FailpointAction {
    Pause,
    Error(StorageError),
    Drop,
}

impl FailpointAction {
    pub fn timeout(message: impl Into<String>) -> Self {
        Self::Error(StorageError::timeout(message))
    }

    pub fn corruption(message: impl Into<String>) -> Self {
        Self::Error(StorageError::corruption(message))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailpointHit {
    pub name: String,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailpointOutcome {
    Continue,
    Drop,
}

#[derive(Debug, Default)]
pub struct FailpointRegistry {
    rules: Mutex<HashMap<String, VecDeque<Arc<ArmedFailpoint>>>>,
}

#[derive(Debug)]
struct ArmedFailpoint {
    name: String,
    mode: FailpointMode,
    action: FailpointAction,
    hit_tx: mpsc::UnboundedSender<FailpointHit>,
    pause: Arc<PauseControl>,
}

#[derive(Debug, Default)]
struct PauseControl {
    hits: AtomicU64,
    released: AtomicU64,
    notify: Notify,
}

impl PauseControl {
    async fn wait_for_release(&self) {
        let hit = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
        loop {
            let notified = self.notify.notified();
            if self.released.load(Ordering::SeqCst) >= hit {
                return;
            }
            notified.await;
        }
    }

    fn release_one(&self) {
        self.released.fetch_add(1, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

impl ArmedFailpoint {
    async fn fire(
        &self,
        metadata: BTreeMap<String, String>,
    ) -> Result<FailpointOutcome, StorageError> {
        let _ = self.hit_tx.send(FailpointHit {
            name: self.name.clone(),
            metadata,
        });

        match &self.action {
            FailpointAction::Pause => {
                self.pause.wait_for_release().await;
                Ok(FailpointOutcome::Continue)
            }
            FailpointAction::Error(error) => Err(error.clone()),
            FailpointAction::Drop => Ok(FailpointOutcome::Drop),
        }
    }
}

#[derive(Debug)]
pub struct FailpointHandle {
    receiver: AsyncMutex<mpsc::UnboundedReceiver<FailpointHit>>,
    pause: PauseHandle,
}

#[derive(Clone, Debug)]
struct PauseHandle(Arc<PauseControl>);

impl FailpointHandle {
    fn new(receiver: mpsc::UnboundedReceiver<FailpointHit>, pause: Arc<PauseControl>) -> Self {
        Self {
            receiver: AsyncMutex::new(receiver),
            pause: PauseHandle(pause),
        }
    }

    pub async fn next_hit(&self) -> FailpointHit {
        let mut receiver = self.receiver.lock().await;
        receiver
            .recv()
            .await
            .expect("failpoint handle should remain attached while awaiting a hit")
    }

    pub async fn wait_until_hit(&self) {
        let _ = self.next_hit().await;
    }

    pub fn release(&self) {
        self.pause.0.release_one();
    }
}

impl FailpointRegistry {
    pub fn arm(
        &self,
        name: impl Into<String>,
        action: FailpointAction,
        mode: FailpointMode,
    ) -> FailpointHandle {
        let name = name.into();
        let pause = Arc::new(PauseControl::default());
        let (hit_tx, hit_rx) = mpsc::unbounded_channel();
        let armed = Arc::new(ArmedFailpoint {
            name: name.clone(),
            mode,
            action,
            hit_tx,
            pause: pause.clone(),
        });
        let handle = FailpointHandle::new(hit_rx, pause);

        {
            let mut rules = self.rules.lock();
            rules.entry(name).or_default().push_back(armed);
        }

        handle
    }

    pub fn arm_pause(&self, name: impl Into<String>, mode: FailpointMode) -> FailpointHandle {
        self.arm(name, FailpointAction::Pause, mode)
    }

    pub fn arm_error(
        &self,
        name: impl Into<String>,
        error: StorageError,
        mode: FailpointMode,
    ) -> FailpointHandle {
        self.arm(name, FailpointAction::Error(error), mode)
    }

    pub fn arm_timeout(
        &self,
        name: impl Into<String>,
        message: impl Into<String>,
        mode: FailpointMode,
    ) -> FailpointHandle {
        self.arm(name, FailpointAction::timeout(message), mode)
    }

    pub fn arm_corruption(
        &self,
        name: impl Into<String>,
        message: impl Into<String>,
        mode: FailpointMode,
    ) -> FailpointHandle {
        self.arm(name, FailpointAction::corruption(message), mode)
    }

    pub fn arm_drop(&self, name: impl Into<String>, mode: FailpointMode) -> FailpointHandle {
        self.arm(name, FailpointAction::Drop, mode)
    }

    pub async fn trigger(
        &self,
        name: &str,
        metadata: BTreeMap<String, String>,
    ) -> Result<FailpointOutcome, StorageError> {
        let armed = {
            let mut rules = self.rules.lock();
            let Some(queue) = rules.get_mut(name) else {
                return Ok(FailpointOutcome::Continue);
            };

            let armed = queue.front().cloned();
            if let Some(failpoint) = &armed
                && failpoint.mode == FailpointMode::Once
            {
                queue.pop_front();
            }

            if queue.is_empty() {
                rules.remove(name);
            }

            armed
        };

        match armed {
            Some(failpoint) => failpoint.fire(metadata).await,
            None => Ok(FailpointOutcome::Continue),
        }
    }
}

fn attached_registries() -> &'static Mutex<HashMap<u64, Arc<FailpointRegistry>>> {
    static ATTACHED: OnceLock<Mutex<HashMap<u64, Arc<FailpointRegistry>>>> = OnceLock::new();
    ATTACHED.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn attach_registry(dependencies: &DbDependencies, registry: Arc<FailpointRegistry>) {
    attached_registries()
        .lock()
        .insert(dependencies.failpoint_key, registry);
}

pub(crate) fn registry_for_dependencies(dependencies: &DbDependencies) -> Arc<FailpointRegistry> {
    let mut registries = attached_registries().lock();
    registries
        .entry(dependencies.failpoint_key)
        .or_insert_with(|| Arc::new(FailpointRegistry::default()))
        .clone()
}

pub mod names {
    pub const DB_BACKUP_MANIFEST_BEFORE_LATEST_POINTER: &str =
        "db.backup.manifest.before_latest_pointer";
    pub const DB_COMMIT_AFTER_BATCH_SEAL: &str = "db.commit.after_batch_seal";
    pub const DB_COMMIT_AFTER_DURABLE_PUBLISH: &str = "db.commit.after_durable_publish";
    pub const DB_COMMIT_AFTER_DURABILITY_SYNC: &str = "db.commit.after_durability_sync";
    pub const DB_COMMIT_AFTER_MEMTABLE_INSERT: &str = "db.commit.after_memtable_insert";
    pub const DB_COMMIT_AFTER_VISIBILITY_PUBLISH: &str = "db.commit.after_visibility_publish";
    pub const DB_COMMIT_BEFORE_DURABILITY_SYNC: &str = "db.commit.before_durability_sync";
    pub const DB_COMMIT_BEFORE_MEMTABLE_INSERT: &str = "db.commit.before_memtable_insert";
    pub const DB_COMPACTION_INPUT_CLEANUP_FINISHED: &str = "db.compaction.input_cleanup_finished";
    pub const DB_COMPACTION_MANIFEST_SWITCHED: &str = "db.compaction.manifest_switched";
    pub const DB_COMPACTION_OUTPUT_WRITTEN: &str = "db.compaction.output_written";
    pub const DB_MANIFEST_BEFORE_CURRENT_POINTER: &str = "db.manifest.before_current_pointer";
    pub const DB_OFFLOAD_LOCAL_CLEANUP_FINISHED: &str = "db.offload.local_cleanup_finished";
    pub const DB_OFFLOAD_MANIFEST_SWITCHED: &str = "db.offload.manifest_switched";
    pub const DB_OFFLOAD_UPLOAD_COMPLETE: &str = "db.offload.upload_complete";
    pub const DB_REMOTE_MANIFEST_BEFORE_LATEST_POINTER: &str =
        "db.remote_manifest.before_latest_pointer";
    pub const DB_REMOTE_MANIFEST_RECOVERY_AFTER_POINTER_READ: &str =
        "db.remote_manifest.recovery.after_pointer_read";
    pub const OUTBOX_CURSOR_PERSIST_BEFORE_COMMIT: &str = "outbox.cursor_persist.before_commit";
    pub const PROJECTION_APPLY_BEFORE_COMMIT: &str = "projection.apply.before_commit";
    pub const PROJECTION_REBUILD_APPLY_BEFORE_COMMIT: &str =
        "projection.rebuild.apply.before_commit";
    pub const PROJECTION_REBUILD_RESET_BEFORE_COMMIT: &str =
        "projection.rebuild.reset.before_commit";
    pub const WORKFLOW_CALLBACK_ADMISSION_BEFORE_COMMIT: &str =
        "workflow.callback_admission.before_commit";
    pub const WORKFLOW_EXECUTION_BEFORE_COMMIT: &str = "workflow.execution.before_commit";
    pub const WORKFLOW_TIMER_ADMISSION_BEFORE_COMMIT: &str =
        "workflow.timer_admission.before_commit";
}

impl DbDependencies {
    #[doc(hidden)]
    pub fn __attach_failpoint_registry(&self, registry: Arc<FailpointRegistry>) {
        attach_registry(self, registry);
    }

    #[doc(hidden)]
    pub fn __failpoint_registry(&self) -> Arc<FailpointRegistry> {
        registry_for_dependencies(self)
    }
}
