use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    thread::ThreadId,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

macro_rules! heap_id {
    ($name:ident) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
    };
}

heap_id!(JsHeapObjectId);
heap_id!(JsHeapRootId);
heap_id!(JsWeakObjectId);
heap_id!(JsEphemeronId);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsHeapConfig {
    pub name: String,
    pub object_limit: usize,
    pub byte_limit: usize,
}

impl Default for JsHeapConfig {
    fn default() -> Self {
        Self {
            name: "terrace-js-heap".to_string(),
            object_limit: 1_000_000,
            byte_limit: 1 << 30,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsHeapPhase {
    Safepoint,
    Active,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsHeapAttachmentSnapshot {
    pub attached: bool,
    pub attachment_epoch: u64,
    pub phase: JsHeapPhase,
    pub thread_id: Option<String>,
    pub label: Option<String>,
}

impl Default for JsHeapAttachmentSnapshot {
    fn default() -> Self {
        Self {
            attached: false,
            attachment_epoch: 0,
            phase: JsHeapPhase::Safepoint,
            thread_id: None,
            label: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsCollectorSnapshot {
    pub cycles_completed: u64,
    pub allocated_bytes: usize,
    pub reclaimed_bytes: usize,
    pub live_objects: usize,
    pub root_count: usize,
    pub weak_count: usize,
    pub ephemeron_count: usize,
    pub last_cycle: Option<JsGcCycleReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsHeapSnapshot {
    pub config: JsHeapConfig,
    pub attachment: JsHeapAttachmentSnapshot,
    pub collector: JsCollectorSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsHeapObjectDescriptor {
    pub kind: String,
    pub label: Option<String>,
    pub size_bytes: usize,
}

impl JsHeapObjectDescriptor {
    pub fn opaque(kind: impl Into<String>, size_bytes: usize) -> Self {
        Self {
            kind: kind.into(),
            label: None,
            size_bytes,
        }
    }

    pub fn labeled(kind: impl Into<String>, label: impl Into<String>, size_bytes: usize) -> Self {
        Self {
            kind: kind.into(),
            label: Some(label.into()),
            size_bytes,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsGcCycleReport {
    pub cycle: u64,
    pub marked_objects: usize,
    pub swept_objects: usize,
    pub reclaimed_bytes: usize,
    pub live_objects: usize,
    pub live_roots: usize,
    pub live_weaks: usize,
    pub live_ephemerons: usize,
}

#[derive(Debug, Error)]
pub enum JsHeapError {
    #[error("heap object limit exceeded: attempted {attempted}, limit {limit}")]
    ObjectLimitExceeded { attempted: usize, limit: usize },
    #[error("heap byte limit exceeded: attempted {attempted}, limit {limit}")]
    ByteLimitExceeded { attempted: usize, limit: usize },
    #[error("heap object not found: {id:?}")]
    ObjectNotFound { id: JsHeapObjectId },
    #[error("heap root not found: {id:?}")]
    RootNotFound { id: JsHeapRootId },
    #[error("weak object handle not found: {id:?}")]
    WeakNotFound { id: JsWeakObjectId },
    #[error("ephemeron not found: {id:?}")]
    EphemeronNotFound { id: JsEphemeronId },
    #[error("heap operation requires a safepoint")]
    NotAtSafepoint,
    #[error("heap operation requires active execution")]
    NotInActiveTurn,
}

#[derive(Clone)]
pub struct JsRuntimeHeap {
    inner: Arc<Mutex<JsRuntimeHeapInner>>,
}

impl JsRuntimeHeap {
    pub fn new(config: JsHeapConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(JsRuntimeHeapInner::new(config))),
        }
    }

    pub fn attach(&self, label: impl Into<String>) -> JsRuntimeHeapAttachment<'_> {
        let mut guard = self
            .inner
            .lock()
            .expect("js runtime heap mutex poisoned during attach");
        guard.attachment.attached = true;
        guard.attachment.attachment_epoch = guard.attachment.attachment_epoch.saturating_add(1);
        guard.attachment.phase = JsHeapPhase::Safepoint;
        guard.attachment.thread_id = Some(format!("{:?}", std::thread::current().id()));
        guard.attachment.label = Some(label.into());
        let owner_thread = std::thread::current().id();
        JsRuntimeHeapAttachment {
            guard,
            owner_thread,
        }
    }

    pub fn snapshot(&self) -> JsHeapSnapshot {
        self.inner
            .lock()
            .expect("js runtime heap mutex poisoned during snapshot")
            .snapshot()
    }
}

pub struct JsRuntimeHeapAttachment<'a> {
    guard: MutexGuard<'a, JsRuntimeHeapInner>,
    owner_thread: ThreadId,
}

impl<'a> JsRuntimeHeapAttachment<'a> {
    pub fn begin_turn(&mut self, label: impl Into<String>) {
        self.guard.attachment.phase = JsHeapPhase::Active;
        self.guard.attachment.label = Some(label.into());
    }

    pub fn reach_safepoint(&mut self, label: impl Into<String>) {
        self.guard.attachment.phase = JsHeapPhase::Safepoint;
        self.guard.attachment.label = Some(label.into());
    }

    pub fn snapshot(&self) -> JsHeapSnapshot {
        self.guard.snapshot()
    }

    pub fn allocate_object(
        &mut self,
        descriptor: JsHeapObjectDescriptor,
    ) -> Result<JsHeapObjectId, JsHeapError> {
        self.guard.allocate_object(descriptor)
    }

    pub fn object_exists(&self, id: JsHeapObjectId) -> bool {
        self.guard.objects.contains_key(&id)
    }

    pub fn object_descriptor(
        &self,
        id: JsHeapObjectId,
    ) -> Result<JsHeapObjectDescriptor, JsHeapError> {
        self.guard
            .objects
            .get(&id)
            .map(|record| record.descriptor.clone())
            .ok_or(JsHeapError::ObjectNotFound { id })
    }

    pub fn add_edge(
        &mut self,
        from: JsHeapObjectId,
        to: JsHeapObjectId,
    ) -> Result<(), JsHeapError> {
        if !self.guard.objects.contains_key(&to) {
            return Err(JsHeapError::ObjectNotFound { id: to });
        }
        let object = self
            .guard
            .objects
            .get_mut(&from)
            .ok_or(JsHeapError::ObjectNotFound { id: from })?;
        object.outgoing.insert(to);
        Ok(())
    }

    pub fn remove_edge(
        &mut self,
        from: JsHeapObjectId,
        to: JsHeapObjectId,
    ) -> Result<(), JsHeapError> {
        let object = self
            .guard
            .objects
            .get_mut(&from)
            .ok_or(JsHeapError::ObjectNotFound { id: from })?;
        object.outgoing.remove(&to);
        Ok(())
    }

    pub fn create_root(
        &mut self,
        target: JsHeapObjectId,
        label: impl Into<String>,
    ) -> Result<JsHeapRootId, JsHeapError> {
        if !self.guard.objects.contains_key(&target) {
            return Err(JsHeapError::ObjectNotFound { id: target });
        }
        let root_id = self.guard.next_root_id();
        self.guard.roots.insert(
            root_id,
            JsHeapRootRecord {
                target,
                _label: label.into(),
            },
        );
        Ok(root_id)
    }

    pub fn retarget_root(
        &mut self,
        root_id: JsHeapRootId,
        target: JsHeapObjectId,
    ) -> Result<(), JsHeapError> {
        if !self.guard.objects.contains_key(&target) {
            return Err(JsHeapError::ObjectNotFound { id: target });
        }
        let root = self
            .guard
            .roots
            .get_mut(&root_id)
            .ok_or(JsHeapError::RootNotFound { id: root_id })?;
        root.target = target;
        Ok(())
    }

    pub fn drop_root(&mut self, root_id: JsHeapRootId) -> Result<(), JsHeapError> {
        self.guard
            .roots
            .remove(&root_id)
            .map(|_| ())
            .ok_or(JsHeapError::RootNotFound { id: root_id })
    }

    pub fn create_weak(&mut self, target: JsHeapObjectId) -> Result<JsWeakObjectId, JsHeapError> {
        if !self.guard.objects.contains_key(&target) {
            return Err(JsHeapError::ObjectNotFound { id: target });
        }
        let weak_id = self.guard.next_weak_id();
        self.guard.weak_handles.insert(weak_id, target);
        Ok(weak_id)
    }

    pub fn upgrade_weak(
        &self,
        weak_id: JsWeakObjectId,
    ) -> Result<Option<JsHeapObjectId>, JsHeapError> {
        let target = self
            .guard
            .weak_handles
            .get(&weak_id)
            .copied()
            .ok_or(JsHeapError::WeakNotFound { id: weak_id })?;
        Ok(self.guard.objects.contains_key(&target).then_some(target))
    }

    pub fn create_ephemeron(
        &mut self,
        key: JsHeapObjectId,
        value: JsHeapObjectId,
    ) -> Result<JsEphemeronId, JsHeapError> {
        if !self.guard.objects.contains_key(&key) {
            return Err(JsHeapError::ObjectNotFound { id: key });
        }
        if !self.guard.objects.contains_key(&value) {
            return Err(JsHeapError::ObjectNotFound { id: value });
        }
        let eph_id = self.guard.next_ephemeron_id();
        self.guard
            .ephemerons
            .insert(eph_id, JsHeapEphemeronRecord { key, value });
        Ok(eph_id)
    }

    pub fn ephemeron_value(
        &self,
        ephemeron_id: JsEphemeronId,
    ) -> Result<Option<JsHeapObjectId>, JsHeapError> {
        let record = self
            .guard
            .ephemerons
            .get(&ephemeron_id)
            .ok_or(JsHeapError::EphemeronNotFound { id: ephemeron_id })?;
        Ok(self
            .guard
            .objects
            .contains_key(&record.key)
            .then(|| self.guard.objects.contains_key(&record.value))
            .unwrap_or(false)
            .then_some(record.value))
    }

    pub fn collect_garbage(&mut self) -> Result<JsGcCycleReport, JsHeapError> {
        if self.guard.attachment.phase != JsHeapPhase::Safepoint {
            return Err(JsHeapError::NotAtSafepoint);
        }
        Ok(self.guard.collect_garbage())
    }

    pub fn detach(mut self) -> Result<JsHeapSnapshot, JsHeapError> {
        if self.guard.attachment.phase != JsHeapPhase::Safepoint {
            return Err(JsHeapError::NotAtSafepoint);
        }
        self.guard.attachment.attached = false;
        self.guard.attachment.thread_id = None;
        self.guard.attachment.label = None;
        Ok(self.guard.snapshot())
    }

    pub fn owner_thread(&self) -> ThreadId {
        self.owner_thread
    }
}

#[derive(Clone, Debug)]
struct JsRuntimeHeapInner {
    config: JsHeapConfig,
    attachment: JsHeapAttachmentSnapshot,
    collector: JsCollectorState,
    next_object_id: u64,
    next_root_id: u64,
    next_weak_id: u64,
    next_ephemeron_id: u64,
    objects: BTreeMap<JsHeapObjectId, JsHeapObjectRecord>,
    roots: BTreeMap<JsHeapRootId, JsHeapRootRecord>,
    weak_handles: BTreeMap<JsWeakObjectId, JsHeapObjectId>,
    ephemerons: BTreeMap<JsEphemeronId, JsHeapEphemeronRecord>,
}

impl JsRuntimeHeapInner {
    fn new(config: JsHeapConfig) -> Self {
        Self {
            config,
            attachment: JsHeapAttachmentSnapshot::default(),
            collector: JsCollectorState::default(),
            next_object_id: 0,
            next_root_id: 0,
            next_weak_id: 0,
            next_ephemeron_id: 0,
            objects: BTreeMap::new(),
            roots: BTreeMap::new(),
            weak_handles: BTreeMap::new(),
            ephemerons: BTreeMap::new(),
        }
    }

    fn snapshot(&self) -> JsHeapSnapshot {
        JsHeapSnapshot {
            config: self.config.clone(),
            attachment: self.attachment.clone(),
            collector: JsCollectorSnapshot {
                cycles_completed: self.collector.cycles_completed,
                allocated_bytes: self.collector.allocated_bytes,
                reclaimed_bytes: self.collector.reclaimed_bytes,
                live_objects: self.objects.len(),
                root_count: self.roots.len(),
                weak_count: self.weak_handles.len(),
                ephemeron_count: self.ephemerons.len(),
                last_cycle: self.collector.last_cycle.clone(),
            },
        }
    }

    fn next_object_id(&mut self) -> JsHeapObjectId {
        self.next_object_id = self.next_object_id.saturating_add(1);
        JsHeapObjectId::new(self.next_object_id)
    }

    fn next_root_id(&mut self) -> JsHeapRootId {
        self.next_root_id = self.next_root_id.saturating_add(1);
        JsHeapRootId::new(self.next_root_id)
    }

    fn next_weak_id(&mut self) -> JsWeakObjectId {
        self.next_weak_id = self.next_weak_id.saturating_add(1);
        JsWeakObjectId::new(self.next_weak_id)
    }

    fn next_ephemeron_id(&mut self) -> JsEphemeronId {
        self.next_ephemeron_id = self.next_ephemeron_id.saturating_add(1);
        JsEphemeronId::new(self.next_ephemeron_id)
    }

    fn allocate_object(
        &mut self,
        descriptor: JsHeapObjectDescriptor,
    ) -> Result<JsHeapObjectId, JsHeapError> {
        let attempted_objects = self.objects.len().saturating_add(1);
        if attempted_objects > self.config.object_limit {
            return Err(JsHeapError::ObjectLimitExceeded {
                attempted: attempted_objects,
                limit: self.config.object_limit,
            });
        }

        let attempted_bytes = self
            .collector
            .allocated_bytes
            .saturating_add(descriptor.size_bytes);
        if attempted_bytes > self.config.byte_limit {
            return Err(JsHeapError::ByteLimitExceeded {
                attempted: attempted_bytes,
                limit: self.config.byte_limit,
            });
        }

        let object_id = self.next_object_id();
        self.collector.allocated_bytes = attempted_bytes;
        self.objects.insert(
            object_id,
            JsHeapObjectRecord {
                id: object_id,
                descriptor,
                outgoing: BTreeSet::new(),
            },
        );
        Ok(object_id)
    }

    fn collect_garbage(&mut self) -> JsGcCycleReport {
        self.collector.cycles_completed = self.collector.cycles_completed.saturating_add(1);
        let cycle = self.collector.cycles_completed;
        let mut marked = BTreeSet::new();
        let mut stack: VecDeque<JsHeapObjectId> =
            self.roots.values().map(|root| root.target).collect();

        while let Some(object_id) = stack.pop_back() {
            if !marked.insert(object_id) {
                continue;
            }
            if let Some(object) = self.objects.get(&object_id) {
                for child in &object.outgoing {
                    stack.push_back(*child);
                }
            }
        }

        loop {
            let mut changed = false;
            for record in self.ephemerons.values() {
                if marked.contains(&record.key) && marked.insert(record.value) {
                    changed = true;
                    if let Some(object) = self.objects.get(&record.value) {
                        for child in &object.outgoing {
                            stack.push_back(*child);
                        }
                    }
                }
            }
            while let Some(object_id) = stack.pop_back() {
                if !marked.insert(object_id) {
                    continue;
                }
                if let Some(object) = self.objects.get(&object_id) {
                    for child in &object.outgoing {
                        stack.push_back(*child);
                    }
                }
            }
            if !changed {
                break;
            }
        }

        let mut swept_objects = 0usize;
        let mut reclaimed_bytes = 0usize;
        self.objects.retain(|_, record| {
            let live = marked.contains(&record.id);
            if !live {
                swept_objects = swept_objects.saturating_add(1);
                reclaimed_bytes = reclaimed_bytes.saturating_add(record.descriptor.size_bytes);
            }
            live
        });
        self.collector.allocated_bytes = self
            .collector
            .allocated_bytes
            .saturating_sub(reclaimed_bytes);
        self.collector.reclaimed_bytes = self
            .collector
            .reclaimed_bytes
            .saturating_add(reclaimed_bytes);

        let report = JsGcCycleReport {
            cycle,
            marked_objects: marked.len(),
            swept_objects,
            reclaimed_bytes,
            live_objects: self.objects.len(),
            live_roots: self.roots.len(),
            live_weaks: self.weak_handles.len(),
            live_ephemerons: self.ephemerons.len(),
        };
        self.collector.last_cycle = Some(report.clone());
        report
    }
}

#[derive(Clone, Debug, Default)]
struct JsCollectorState {
    cycles_completed: u64,
    allocated_bytes: usize,
    reclaimed_bytes: usize,
    last_cycle: Option<JsGcCycleReport>,
}

#[derive(Clone, Debug)]
struct JsHeapObjectRecord {
    id: JsHeapObjectId,
    descriptor: JsHeapObjectDescriptor,
    outgoing: BTreeSet<JsHeapObjectId>,
}

#[derive(Clone, Debug)]
struct JsHeapRootRecord {
    target: JsHeapObjectId,
    _label: String,
}

#[derive(Clone, Debug)]
struct JsHeapEphemeronRecord {
    key: JsHeapObjectId,
    value: JsHeapObjectId,
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::{
        JsGcCycleReport, JsHeapConfig, JsHeapError, JsHeapObjectDescriptor, JsHeapPhase,
        JsRuntimeHeap,
    };

    fn assert_live_counts(report: &JsGcCycleReport, objects: usize, roots: usize) {
        assert_eq!(report.live_objects, objects);
        assert_eq!(report.live_roots, roots);
    }

    #[test]
    fn heap_basic_allocation_and_root_liveness_match_gc_smoke_expectations() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("allocation-smoke");
        let object = attached
            .allocate_object(JsHeapObjectDescriptor::labeled("cell", "counter", 16))
            .expect("allocate object");
        let root = attached.create_root(object, "root").expect("create root");

        let first = attached.collect_garbage().expect("collect with root");
        assert_live_counts(&first, 1, 1);
        assert!(attached.object_exists(object));

        attached.drop_root(root).expect("drop root");
        let second = attached.collect_garbage().expect("collect after root drop");
        assert_live_counts(&second, 0, 0);
        assert!(!attached.object_exists(object));
        attached.detach().expect("detach at safepoint");
    }

    #[test]
    fn heap_supports_many_children_on_one_root_without_borrow_style_failures() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("many-children");
        let parent = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("vec", 64))
            .expect("allocate parent");
        let root = attached.create_root(parent, "parent-root").expect("root");

        for index in 0..259 {
            let child = attached
                .allocate_object(JsHeapObjectDescriptor::labeled(
                    "cell",
                    index.to_string(),
                    10,
                ))
                .expect("allocate child");
            attached.add_edge(parent, child).expect("add edge");
        }

        let report = attached
            .collect_garbage()
            .expect("collect with rooted parent");
        assert_live_counts(&report, 260, 1);

        attached.drop_root(root).expect("drop root");
        let after_drop = attached.collect_garbage().expect("collect after root drop");
        assert_live_counts(&after_drop, 0, 0);
        attached.detach().expect("detach");
    }

    #[test]
    fn heap_collects_deep_chains_iteratively() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("deep-chain");
        let mut root_object = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("node", 8))
            .expect("allocate root");
        let root = attached
            .create_root(root_object, "chain-root")
            .expect("create root");

        for _ in 0..10_000 {
            let next = attached
                .allocate_object(JsHeapObjectDescriptor::opaque("node", 8))
                .expect("allocate next");
            attached.add_edge(root_object, next).expect("link chain");
            root_object = next;
        }

        let live = attached.collect_garbage().expect("collect rooted chain");
        assert_eq!(live.live_objects, 10_001);

        attached.drop_root(root).expect("drop root");
        let collected = attached.collect_garbage().expect("collect dropped chain");
        assert_eq!(collected.live_objects, 0);
        attached.detach().expect("detach");
    }

    #[test]
    fn heap_requires_safepoint_for_collection_and_detach() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("safepoint");
        attached.begin_turn("executing");

        let object = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("object", 4))
            .expect("allocate");
        attached.create_root(object, "root").expect("root");

        assert!(matches!(
            attached.collect_garbage(),
            Err(JsHeapError::NotAtSafepoint)
        ));

        attached.reach_safepoint("pause");
        attached.collect_garbage().expect("collect at safepoint");
        let snapshot = attached.detach().expect("detach at safepoint");
        assert_eq!(snapshot.attachment.phase, JsHeapPhase::Safepoint);
        assert!(!snapshot.attachment.attached);
    }

    #[test]
    fn heap_attachment_can_move_between_threads_after_detach() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("main-thread");
        let object = attached
            .allocate_object(JsHeapObjectDescriptor::labeled("object", "shared", 12))
            .expect("allocate");
        let root = attached.create_root(object, "root").expect("root");
        let main_thread = attached.owner_thread();
        attached.detach().expect("detach from main thread");

        let heap_for_thread = heap.clone();
        let handle = thread::spawn(move || {
            let mut attached = heap_for_thread.attach("worker-thread");
            assert_ne!(attached.owner_thread(), main_thread);
            assert!(attached.object_exists(object));
            attached.drop_root(root).expect("drop root");
            let report = attached.collect_garbage().expect("collect on worker");
            assert_eq!(report.live_objects, 0);
            attached.detach().expect("detach from worker");
        });
        handle.join().expect("worker thread joined");
    }

    #[test]
    fn weak_handles_upgrade_while_live_and_clear_after_collection() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("weak");
        let object = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("string", 24))
            .expect("allocate object");
        let root = attached.create_root(object, "root").expect("root");
        let weak = attached.create_weak(object).expect("weak");

        assert_eq!(attached.upgrade_weak(weak).expect("upgrade"), Some(object));
        attached.collect_garbage().expect("collect while rooted");
        assert_eq!(attached.upgrade_weak(weak).expect("upgrade"), Some(object));

        attached.drop_root(root).expect("drop root");
        attached.collect_garbage().expect("collect after root drop");
        assert_eq!(attached.upgrade_weak(weak).expect("upgrade"), None);
        attached.detach().expect("detach");
    }

    #[test]
    fn ephemeron_value_survives_while_key_is_live_and_clears_after_key_collection() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("ephemeron");
        let key = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("key", 8))
            .expect("allocate key");
        let value = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("value", 16))
            .expect("allocate value");
        let root = attached.create_root(key, "key-root").expect("root key");
        let ephemeron = attached
            .create_ephemeron(key, value)
            .expect("create ephemeron");

        attached
            .collect_garbage()
            .expect("collect while key rooted");
        assert_eq!(
            attached
                .ephemeron_value(ephemeron)
                .expect("ephemeron value"),
            Some(value)
        );

        attached.drop_root(root).expect("drop root");
        attached.collect_garbage().expect("collect after key drop");
        assert_eq!(
            attached
                .ephemeron_value(ephemeron)
                .expect("ephemeron lookup"),
            None
        );
        attached.detach().expect("detach");
    }

    #[test]
    fn ephemeron_chain_follows_key_liveness_fixed_point() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("ephemeron-chain");
        let watched = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("watched", 8))
            .expect("watched");
        let chain1 = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("chain", 8))
            .expect("chain1");
        let chain2 = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("chain", 8))
            .expect("chain2");
        let root = attached.create_root(watched, "watched-root").expect("root");
        let eph1 = attached
            .create_ephemeron(watched, chain1)
            .expect("ephemeron 1");
        let eph2 = attached
            .create_ephemeron(watched, chain2)
            .expect("ephemeron 2");
        attached.add_edge(chain1, chain2).expect("link chain");

        attached
            .collect_garbage()
            .expect("collect while watched rooted");
        assert_eq!(attached.ephemeron_value(eph1).expect("eph1"), Some(chain1));
        assert_eq!(attached.ephemeron_value(eph2).expect("eph2"), Some(chain2));

        attached.drop_root(root).expect("drop root");
        let report = attached.collect_garbage().expect("collect after drop");
        assert_eq!(report.live_objects, 0);
        attached.detach().expect("detach");
    }

    #[test]
    fn detach_clears_thread_identity_and_preserves_snapshot_state() {
        let heap = JsRuntimeHeap::new(JsHeapConfig::default());
        let mut attached = heap.attach("detach-snapshot");
        let object = attached
            .allocate_object(JsHeapObjectDescriptor::opaque("node", 8))
            .expect("allocate");
        attached.create_root(object, "root").expect("root");
        let snapshot = attached.detach().expect("detach");
        assert!(!snapshot.attachment.attached);
        assert!(snapshot.attachment.thread_id.is_none());
        assert_eq!(snapshot.collector.live_objects, 1);
    }
}
