use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{EnvFrameId, JsRuntimeId, JsRuntimeInstance, ObjectId, StackId};

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SegmentKind {
    HeapObjects,
    BindingCells,
    EnvFrames,
    VmStack,
    Programs,
    Modules,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub owner: JsRuntimeId,
    pub kind: SegmentKind,
    pub used_slots: usize,
    pub capacity_slots: usize,
    pub dirty: bool,
    pub dirty_bytes: usize,
}

#[derive(Debug, Clone, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeSegments {
    pub heap: Vec<SegmentId>,
    pub bindings: Vec<SegmentId>,
    pub env_frames: Vec<SegmentId>,
    pub stacks: Vec<SegmentId>,
    pub programs: Vec<SegmentId>,
    pub modules: Vec<SegmentId>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct SegmentTable {
    next_segment_id: u64,
    segments: HashMap<SegmentId, SegmentMeta>,
    by_runtime: HashMap<JsRuntimeId, RuntimeSegments>,
}

impl SegmentTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create_segment(
        &mut self,
        owner: JsRuntimeId,
        kind: SegmentKind,
        capacity_slots: usize,
    ) -> SegmentId {
        let id = SegmentId(self.next_segment_id);
        self.next_segment_id += 1;

        let meta = SegmentMeta {
            id,
            owner,
            kind,
            used_slots: 0,
            capacity_slots,
            dirty: false,
            dirty_bytes: 0,
        };

        self.segments.insert(id, meta);

        let runtime_segments = self.by_runtime.entry(owner).or_default();
        match kind {
            SegmentKind::HeapObjects => runtime_segments.heap.push(id),
            SegmentKind::BindingCells => runtime_segments.bindings.push(id),
            SegmentKind::EnvFrames => runtime_segments.env_frames.push(id),
            SegmentKind::VmStack => runtime_segments.stacks.push(id),
            SegmentKind::Programs => runtime_segments.programs.push(id),
            SegmentKind::Modules => runtime_segments.modules.push(id),
        }

        id
    }

    pub fn register_existing_segment(
        &mut self,
        id: SegmentId,
        owner: JsRuntimeId,
        kind: SegmentKind,
        capacity_slots: usize,
    ) {
        self.next_segment_id = self.next_segment_id.max(id.0 + 1);

        self.segments.entry(id).or_insert(SegmentMeta {
            id,
            owner,
            kind,
            used_slots: 0,
            capacity_slots,
            dirty: false,
            dirty_bytes: 0,
        });

        let runtime_segments = self.by_runtime.entry(owner).or_default();
        let list = match kind {
            SegmentKind::HeapObjects => &mut runtime_segments.heap,
            SegmentKind::BindingCells => &mut runtime_segments.bindings,
            SegmentKind::EnvFrames => &mut runtime_segments.env_frames,
            SegmentKind::VmStack => &mut runtime_segments.stacks,
            SegmentKind::Programs => &mut runtime_segments.programs,
            SegmentKind::Modules => &mut runtime_segments.modules,
        };

        if !list.contains(&id) {
            list.push(id);
        }
    }

    pub fn meta(&self, segment: SegmentId) -> Result<&SegmentMeta, Error> {
        self.segments
            .get(&segment)
            .ok_or(Error::JsSegmentNotFound { segment: segment.0 })
    }

    pub fn meta_mut(&mut self, segment: SegmentId) -> Result<&mut SegmentMeta, Error> {
        self.segments
            .get_mut(&segment)
            .ok_or(Error::JsSegmentNotFound { segment: segment.0 })
    }

    pub fn owner(&self, segment: SegmentId) -> Result<JsRuntimeId, Error> {
        Ok(self.meta(segment)?.owner)
    }

    pub fn segments_for_runtime(&self, runtime: JsRuntimeId) -> Option<&RuntimeSegments> {
        self.by_runtime.get(&runtime)
    }

    pub fn remove_runtime(&mut self, runtime: JsRuntimeId) {
        let Some(segments) = self.by_runtime.remove(&runtime) else {
            return;
        };

        for segment in segments
            .heap
            .into_iter()
            .chain(segments.bindings)
            .chain(segments.env_frames)
            .chain(segments.stacks)
            .chain(segments.programs)
            .chain(segments.modules)
        {
            self.segments.remove(&segment);
        }
    }

    pub fn mark_dirty(&mut self, segment: SegmentId, dirty_bytes: usize) -> Result<(), Error> {
        let meta = self.meta_mut(segment)?;
        meta.dirty = true;
        meta.dirty_bytes = meta.dirty_bytes.saturating_add(dirty_bytes);
        Ok(())
    }

    pub fn set_used_slots(&mut self, segment: SegmentId, used_slots: usize) -> Result<(), Error> {
        self.meta_mut(segment)?.used_slots = used_slots;
        Ok(())
    }

    pub fn runtime_resident_bytes(&self, runtime: JsRuntimeId) -> Result<usize, Error> {
        let segments = self
            .segments_for_runtime(runtime)
            .ok_or(Error::RuntimeNotFound(runtime))?;

        let mut total = 0usize;
        for segment in segments
            .heap
            .iter()
            .chain(segments.bindings.iter())
            .chain(segments.env_frames.iter())
            .chain(segments.stacks.iter())
            .chain(segments.programs.iter())
            .chain(segments.modules.iter())
        {
            let meta = self.meta(*segment)?;
            total = total.saturating_add(meta.used_slots.saturating_mul(8));
        }

        Ok(total)
    }

    pub fn runtime_dirty_bytes(&self, runtime: JsRuntimeId) -> Result<usize, Error> {
        let segments = self
            .segments_for_runtime(runtime)
            .ok_or(Error::RuntimeNotFound(runtime))?;

        let mut total = 0usize;
        for segment in segments
            .heap
            .iter()
            .chain(segments.bindings.iter())
            .chain(segments.env_frames.iter())
            .chain(segments.stacks.iter())
            .chain(segments.programs.iter())
            .chain(segments.modules.iter())
        {
            total = total.saturating_add(self.meta(*segment)?.dirty_bytes);
        }

        Ok(total)
    }
}

pub struct PoolRuntimeStorage {
    pub segments: SegmentTable,
    runtimes: HashMap<JsRuntimeId, JsRuntimeInstance>,
}

impl PoolRuntimeStorage {
    pub fn new() -> Self {
        Self {
            segments: SegmentTable::new(),
            runtimes: HashMap::new(),
        }
    }

    pub fn create_runtime_segments(&mut self, runtime: JsRuntimeId) -> RuntimeStorageSegments {
        RuntimeStorageSegments {
            heap: self
                .segments
                .create_segment(runtime, SegmentKind::HeapObjects, 1024),
            bindings: self
                .segments
                .create_segment(runtime, SegmentKind::BindingCells, 1024),
            env_frames: self
                .segments
                .create_segment(runtime, SegmentKind::EnvFrames, 1024),
            stack: self
                .segments
                .create_segment(runtime, SegmentKind::VmStack, 1),
        }
    }

    pub fn register_runtime_segments(
        &mut self,
        runtime: JsRuntimeId,
        segments: RuntimeStorageSegments,
    ) {
        self.segments.register_existing_segment(
            segments.heap,
            runtime,
            SegmentKind::HeapObjects,
            1024,
        );
        self.segments.register_existing_segment(
            segments.bindings,
            runtime,
            SegmentKind::BindingCells,
            1024,
        );
        self.segments.register_existing_segment(
            segments.env_frames,
            runtime,
            SegmentKind::EnvFrames,
            1024,
        );
        self.segments
            .register_existing_segment(segments.stack, runtime, SegmentKind::VmStack, 1);
    }

    pub fn runtime_segments(&self, runtime: JsRuntimeId) -> Result<&RuntimeSegments, Error> {
        self.segments
            .segments_for_runtime(runtime)
            .ok_or(Error::RuntimeNotFound(runtime))
    }

    pub fn runtime_resident_bytes(&self, runtime: JsRuntimeId) -> Result<usize, Error> {
        self.segments.runtime_resident_bytes(runtime)
    }

    pub fn runtime_dirty_bytes(&self, runtime: JsRuntimeId) -> Result<usize, Error> {
        self.segments.runtime_dirty_bytes(runtime)
    }

    pub fn validate_segment_owner(
        &self,
        runtime: JsRuntimeId,
        segment: SegmentId,
    ) -> Result<(), Error> {
        let actual = self.segments.owner(segment)?;
        if actual != runtime {
            return Err(Error::JsSegmentOwnerMismatch {
                segment: segment.0,
                expected: runtime,
                actual,
            });
        }

        Ok(())
    }

    pub fn validate_object_owner(
        &self,
        runtime: JsRuntimeId,
        object: ObjectId,
    ) -> Result<(), Error> {
        self.validate_segment_owner(runtime, object.segment)
    }

    pub fn refresh_runtime_usage(&mut self, runtime: JsRuntimeId) -> Result<(), Error> {
        let (storage_segments, usage) = {
            let runtime_state = self.runtime(runtime)?;
            (
                runtime_state.storage_segments(),
                runtime_state.storage_usage(),
            )
        };

        self.segments
            .set_used_slots(storage_segments.heap, usage.heap_objects)?;
        self.segments
            .set_used_slots(storage_segments.bindings, usage.binding_cells)?;
        self.segments
            .set_used_slots(storage_segments.env_frames, usage.env_frames)?;
        self.segments
            .set_used_slots(storage_segments.stack, usage.stacks)?;

        Ok(())
    }

    pub fn insert_runtime(
        &mut self,
        runtime_id: JsRuntimeId,
        runtime: JsRuntimeInstance,
    ) -> Result<(), Error> {
        if self.runtimes.contains_key(&runtime_id) {
            return Err(Error::JsRuntimeAlreadyExists(runtime_id));
        }

        self.runtimes.insert(runtime_id, runtime);
        Ok(())
    }

    pub fn runtime(&self, runtime: JsRuntimeId) -> Result<&JsRuntimeInstance, Error> {
        self.runtimes
            .get(&runtime)
            .ok_or(Error::RuntimeNotFound(runtime))
    }

    pub fn runtime_mut(&mut self, runtime: JsRuntimeId) -> Result<&mut JsRuntimeInstance, Error> {
        self.runtimes
            .get_mut(&runtime)
            .ok_or(Error::RuntimeNotFound(runtime))
    }

    pub fn remove_runtime(&mut self, runtime: JsRuntimeId) -> Result<JsRuntimeInstance, Error> {
        let runtime = self
            .runtimes
            .remove(&runtime)
            .ok_or(Error::RuntimeNotFound(runtime))?;
        self.segments.remove_runtime(runtime.id());
        Ok(runtime)
    }

    pub fn contains_runtime(&self, runtime: JsRuntimeId) -> bool {
        self.runtimes.contains_key(&runtime)
    }
}

impl Default for PoolRuntimeStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeStorageSegments {
    pub heap: SegmentId,
    pub bindings: SegmentId,
    pub env_frames: SegmentId,
    pub stack: SegmentId,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct RuntimeStorageUsage {
    pub heap_objects: usize,
    pub binding_cells: usize,
    pub env_frames: usize,
    pub stacks: usize,
    pub stack_values: usize,
}

#[derive(Debug, Default)]
pub struct RuntimeTable {
    next_runtime_id: u64,
    runtimes: HashMap<JsRuntimeId, RuntimeSlot>,
}

impl RuntimeTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate_id(&mut self) -> JsRuntimeId {
        let id = JsRuntimeId(self.next_runtime_id);
        self.next_runtime_id += 1;
        id
    }

    pub fn bump_next_runtime_id(&mut self, runtime_id: JsRuntimeId) {
        self.next_runtime_id = self.next_runtime_id.max(runtime_id.0 + 1);
    }

    pub fn insert(&mut self, slot: RuntimeSlot) -> Result<(), Error> {
        if self.runtimes.contains_key(&slot.id) {
            return Err(Error::JsRuntimeAlreadyExists(slot.id));
        }

        self.runtimes.insert(slot.id, slot);
        Ok(())
    }

    pub fn remove(&mut self, id: JsRuntimeId) -> Result<RuntimeSlot, Error> {
        self.runtimes.remove(&id).ok_or(Error::RuntimeNotFound(id))
    }

    pub fn get(&self, id: JsRuntimeId) -> Result<&RuntimeSlot, Error> {
        self.runtimes.get(&id).ok_or(Error::RuntimeNotFound(id))
    }

    pub fn get_mut(&mut self, id: JsRuntimeId) -> Result<&mut RuntimeSlot, Error> {
        self.runtimes.get_mut(&id).ok_or(Error::RuntimeNotFound(id))
    }

    pub fn contains(&self, id: JsRuntimeId) -> bool {
        self.runtimes.contains_key(&id)
    }
}

#[derive(Debug)]
pub struct RuntimeSlot {
    pub id: JsRuntimeId,
    pub state: RuntimeState,
    pub root_env: EnvFrameId,
    pub stack: StackId,
    pub current_program: Option<super::ProgramId>,
    pub resident_bytes: usize,
    pub dirty_bytes: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RuntimeState {
    Idle,
    Running,
    SuspendedInMemory,
}
