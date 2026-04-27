use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{
    Binding, BytecodeProgram, Constant, EnvFrame, EnvFrameId, GcPolicy, HeapObject, ImportEntry,
    Instr, JsRuntimeId, JsValue, LocalExportEntry, ModuleKey, ModuleRecord, ModuleRegistry,
    ObjectId, RuntimeDetached, RuntimeRecord, RuntimeRecordView, RuntimeRecordViewRef,
    RuntimeSnapshot, StackId, SymbolTable, Vm,
};

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct SegmentId(pub u64);

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum SegmentKind {
    RuntimeRecords,
    SymbolTables,
    VmStates,
    ModuleRegistries,
    HeapObjects,
    BindingCells,
    EnvFrames,
    VmStack,
    SymbolNames,
    Programs,
    ProgramConstants,
    ProgramInstructions,
    ProgramFunctions,
    Modules,
}

#[derive(Debug, Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentMeta {
    pub id: SegmentId,
    pub owner: JsRuntimeId,
    pub kind: SegmentKind,
    pub start_slot: u32,
    pub used_slots: usize,
    pub capacity_slots: usize,
    pub dirty: bool,
    pub dirty_bytes: usize,
}

#[derive(Debug, Clone, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeSegments {
    pub runtime_records: Vec<SegmentId>,
    pub symbol_tables: Vec<SegmentId>,
    pub vm_states: Vec<SegmentId>,
    pub module_registries: Vec<SegmentId>,
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
    next_start_by_kind: HashMap<SegmentKind, u32>,
    segments: HashMap<SegmentId, SegmentMeta>,
    by_runtime: HashMap<JsRuntimeId, RuntimeSegments>,
    free_by_kind: HashMap<SegmentKind, Vec<SegmentId>>,
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
        if let Some(segment) = self.reuse_segment(owner, kind) {
            return segment;
        }

        let id = SegmentId(self.next_segment_id);
        self.next_segment_id += 1;
        let start_slot = self.next_start_slot(kind, capacity_slots);

        let meta = SegmentMeta {
            id,
            owner,
            kind,
            start_slot,
            used_slots: 0,
            capacity_slots,
            dirty: false,
            dirty_bytes: 0,
        };

        self.segments.insert(id, meta);

        let runtime_segments = self.by_runtime.entry(owner).or_default();
        match kind {
            SegmentKind::RuntimeRecords => runtime_segments.runtime_records.push(id),
            SegmentKind::SymbolTables => runtime_segments.symbol_tables.push(id),
            SegmentKind::VmStates => runtime_segments.vm_states.push(id),
            SegmentKind::ModuleRegistries => runtime_segments.module_registries.push(id),
            SegmentKind::HeapObjects => runtime_segments.heap.push(id),
            SegmentKind::BindingCells => runtime_segments.bindings.push(id),
            SegmentKind::EnvFrames => runtime_segments.env_frames.push(id),
            SegmentKind::VmStack => runtime_segments.stacks.push(id),
            SegmentKind::Programs => runtime_segments.programs.push(id),
            SegmentKind::SymbolNames
            | SegmentKind::ProgramConstants
            | SegmentKind::ProgramInstructions
            | SegmentKind::ProgramFunctions => {}
            SegmentKind::Modules => runtime_segments.modules.push(id),
        }

        id
    }

    fn reuse_segment(&mut self, owner: JsRuntimeId, kind: SegmentKind) -> Option<SegmentId> {
        let segment = self.free_by_kind.get_mut(&kind)?.pop()?;
        let meta = self.segments.get_mut(&segment)?;

        meta.owner = owner;
        meta.used_slots = 0;
        meta.dirty = false;
        meta.dirty_bytes = 0;

        self.add_runtime_segment(owner, kind, segment);

        Some(segment)
    }

    fn next_start_slot(&mut self, kind: SegmentKind, capacity_slots: usize) -> u32 {
        let next = self.next_start_by_kind.entry(kind).or_default();
        let start = *next;
        *next = next.saturating_add(capacity_slots as u32);
        start
    }

    fn add_runtime_segment(&mut self, owner: JsRuntimeId, kind: SegmentKind, segment: SegmentId) {
        let runtime_segments = self.by_runtime.entry(owner).or_default();
        let list = match kind {
            SegmentKind::RuntimeRecords => Some(&mut runtime_segments.runtime_records),
            SegmentKind::SymbolTables => Some(&mut runtime_segments.symbol_tables),
            SegmentKind::VmStates => Some(&mut runtime_segments.vm_states),
            SegmentKind::ModuleRegistries => Some(&mut runtime_segments.module_registries),
            SegmentKind::HeapObjects => Some(&mut runtime_segments.heap),
            SegmentKind::BindingCells => Some(&mut runtime_segments.bindings),
            SegmentKind::EnvFrames => Some(&mut runtime_segments.env_frames),
            SegmentKind::VmStack => Some(&mut runtime_segments.stacks),
            SegmentKind::Programs => Some(&mut runtime_segments.programs),
            SegmentKind::Modules => Some(&mut runtime_segments.modules),
            SegmentKind::SymbolNames
            | SegmentKind::ProgramConstants
            | SegmentKind::ProgramInstructions
            | SegmentKind::ProgramFunctions => None,
        };

        if let Some(list) = list
            && !list.contains(&segment)
        {
            list.push(segment);
        }
    }

    pub fn register_existing_segment(
        &mut self,
        id: SegmentId,
        owner: JsRuntimeId,
        kind: SegmentKind,
        capacity_slots: usize,
    ) {
        self.next_segment_id = self.next_segment_id.max(id.0 + 1);
        if let Some(free) = self.free_by_kind.get_mut(&kind) {
            free.retain(|segment| *segment != id);
        }

        if !self.segments.contains_key(&id) {
            let start_slot = self.next_start_slot(kind, capacity_slots);
            self.segments.insert(
                id,
                SegmentMeta {
                    id,
                    owner,
                    kind,
                    start_slot,
                    used_slots: 0,
                    capacity_slots,
                    dirty: false,
                    dirty_bytes: 0,
                },
            );
        } else if let Some(meta) = self.segments.get_mut(&id) {
            meta.owner = owner;
            meta.kind = kind;
            meta.used_slots = 0;
            meta.dirty = false;
            meta.dirty_bytes = 0;
        }

        self.add_runtime_segment(owner, kind, id);
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
            .runtime_records
            .into_iter()
            .chain(segments.symbol_tables)
            .chain(segments.vm_states)
            .chain(segments.module_registries)
            .chain(segments.heap)
            .chain(segments.bindings)
            .chain(segments.env_frames)
            .chain(segments.stacks)
            .chain(segments.programs)
            .chain(segments.modules)
        {
            if let Some(meta) = self.segments.get_mut(&segment) {
                meta.used_slots = 0;
                meta.dirty = false;
                meta.dirty_bytes = 0;
                self.free_by_kind
                    .entry(meta.kind)
                    .or_default()
                    .push(segment);
            }
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
            .runtime_records
            .iter()
            .chain(segments.symbol_tables.iter())
            .chain(segments.vm_states.iter())
            .chain(segments.module_registries.iter())
            .chain(segments.heap.iter())
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
            .runtime_records
            .iter()
            .chain(segments.symbol_tables.iter())
            .chain(segments.vm_states.iter())
            .chain(segments.module_registries.iter())
            .chain(segments.heap.iter())
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
    pub heap: PoolHeapStorage,
    pub env: PoolEnvStorage,
    pub vm: PoolVmStorage,
    pub symbols: PoolSymbolStorage,
    pub programs: PoolProgramStorage,
    pub modules: PoolModuleStorage,
    runtime_records: FlatSegmentStore<RuntimeRecord>,
    runtime_index: HashMap<JsRuntimeId, RuntimeRecordId>,
}

impl PoolRuntimeStorage {
    pub fn new() -> Self {
        Self {
            segments: SegmentTable::new(),
            heap: PoolHeapStorage::new(),
            env: PoolEnvStorage::new(),
            vm: PoolVmStorage::new(),
            symbols: PoolSymbolStorage::new(),
            programs: PoolProgramStorage::new(),
            modules: PoolModuleStorage::new(),
            runtime_records: FlatSegmentStore::new(),
            runtime_index: HashMap::new(),
        }
    }

    pub fn create_runtime_segments(&mut self, runtime: JsRuntimeId) -> RuntimeStorageSegments {
        RuntimeStorageSegments {
            runtime_record: self
                .segments
                .create_segment(runtime, SegmentKind::RuntimeRecords, 1),
            symbol_table: self
                .segments
                .create_segment(runtime, SegmentKind::SymbolTables, 1),
            vm_state: self
                .segments
                .create_segment(runtime, SegmentKind::VmStates, 1),
            module_registry: self.segments.create_segment(
                runtime,
                SegmentKind::ModuleRegistries,
                1,
            ),
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
            segments.runtime_record,
            runtime,
            SegmentKind::RuntimeRecords,
            1,
        );
        self.segments.register_existing_segment(
            segments.symbol_table,
            runtime,
            SegmentKind::SymbolTables,
            1,
        );
        self.segments.register_existing_segment(
            segments.vm_state,
            runtime,
            SegmentKind::VmStates,
            1,
        );
        self.segments.register_existing_segment(
            segments.module_registry,
            runtime,
            SegmentKind::ModuleRegistries,
            1,
        );
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
            let vm = self.vm.state(&self.segments, runtime_state.vm_state_id())?;
            (runtime_state.storage_segments(), vm.storage_usage())
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

    pub(crate) fn insert_runtime_state(
        &mut self,
        runtime_id: JsRuntimeId,
        gc_policy: GcPolicy,
        storage_segments: RuntimeStorageSegments,
    ) -> Result<(), Error> {
        if self.runtime_index.contains_key(&runtime_id) {
            return Err(Error::JsRuntimeAlreadyExists(runtime_id));
        }

        let symbol_slot = self.symbols.alloc_table(
            &mut self.segments,
            storage_segments.symbol_table,
            SymbolTable::new(),
        )?;
        let vm_slot = self.vm.alloc_state(
            &mut self.segments,
            storage_segments.vm_state,
            Vm::with_storage_segments(storage_segments, gc_policy),
        )?;
        let module_slot = self.modules.alloc_registry(
            &mut self.segments,
            storage_segments.module_registry,
            ModuleRegistry::new(),
        )?;
        let runtime = RuntimeRecord::new(
            runtime_id,
            storage_segments,
            SymbolTableId {
                segment: storage_segments.symbol_table,
                slot: symbol_slot,
            },
            VmStateId {
                segment: storage_segments.vm_state,
                slot: vm_slot,
            },
            ModuleRegistryId {
                segment: storage_segments.module_registry,
                slot: module_slot,
            },
        );
        let segment = storage_segments.runtime_record;
        let slot = self
            .runtime_records
            .alloc(&mut self.segments, segment, runtime)?;
        self.runtime_index
            .insert(runtime_id, RuntimeRecordId { segment, slot });
        Ok(())
    }

    pub(crate) fn insert_runtime_snapshot(
        &mut self,
        snapshot: RuntimeSnapshot,
    ) -> Result<(), Error> {
        let runtime_id = snapshot.runtime_id;
        let storage_segments = snapshot.storage_segments;

        if self.runtime_index.contains_key(&runtime_id) {
            return Err(Error::JsRuntimeAlreadyExists(runtime_id));
        }

        let symbol_slot = self.symbols.alloc_table(
            &mut self.segments,
            storage_segments.symbol_table,
            snapshot.symbols,
        )?;
        let vm_slot =
            self.vm
                .alloc_state(&mut self.segments, storage_segments.vm_state, snapshot.vm)?;
        let module_slot = self.modules.alloc_registry(
            &mut self.segments,
            storage_segments.module_registry,
            snapshot.modules,
        )?;
        let runtime = RuntimeRecord::new(
            runtime_id,
            storage_segments,
            SymbolTableId {
                segment: storage_segments.symbol_table,
                slot: symbol_slot,
            },
            VmStateId {
                segment: storage_segments.vm_state,
                slot: vm_slot,
            },
            ModuleRegistryId {
                segment: storage_segments.module_registry,
                slot: module_slot,
            },
        );
        let slot = self.runtime_records.alloc(
            &mut self.segments,
            storage_segments.runtime_record,
            runtime,
        )?;
        self.runtime_index.insert(
            runtime_id,
            RuntimeRecordId {
                segment: storage_segments.runtime_record,
                slot,
            },
        );

        Ok(())
    }

    pub(crate) fn runtime(&self, runtime: JsRuntimeId) -> Result<&RuntimeRecord, Error> {
        let id = self
            .runtime_index
            .get(&runtime)
            .copied()
            .ok_or(Error::RuntimeNotFound(runtime))?;
        self.runtime_records
            .get(&self.segments, id.segment, id.slot)
    }

    pub(crate) fn runtime_view(
        &self,
        runtime: JsRuntimeId,
    ) -> Result<RuntimeRecordViewRef<'_>, Error> {
        let record = self.runtime(runtime)?;
        let symbols = self
            .symbols
            .table(&self.segments, record.symbol_table_id())?;
        let vm = self.vm.state(&self.segments, record.vm_state_id())?;
        let modules = self
            .modules
            .registry(&self.segments, record.module_registry_id())?;

        Ok(RuntimeRecordViewRef {
            id: record.id(),
            storage_segments: record.storage_segments(),
            symbols,
            vm,
            modules,
        })
    }

    pub(crate) fn runtime_view_mut(
        &mut self,
        runtime: JsRuntimeId,
    ) -> Result<RuntimeRecordView<'_>, Error> {
        let (id, symbol_table, vm_state, module_registry) = {
            let record = self.runtime(runtime)?;
            (
                record.id(),
                record.symbol_table_id(),
                record.vm_state_id(),
                record.module_registry_id(),
            )
        };
        let symbols = self.symbols.table_mut(&self.segments, symbol_table)?;
        let vm = self.vm.state_mut(&self.segments, vm_state)?;
        let modules = self.modules.registry_mut(&self.segments, module_registry)?;

        Ok(RuntimeRecordView {
            id,
            symbols,
            vm,
            modules,
        })
    }

    pub(crate) fn remove_runtime(
        &mut self,
        runtime: JsRuntimeId,
    ) -> Result<RuntimeDetached, Error> {
        let id = self
            .runtime_index
            .remove(&runtime)
            .ok_or(Error::RuntimeNotFound(runtime))?;
        let runtime_record = self
            .runtime_records
            .free(&mut self.segments, id.segment, id.slot)?;
        let symbols = self
            .symbols
            .free_table(&mut self.segments, runtime_record.symbol_table_id())?;
        drop(symbols);
        let modules = self
            .modules
            .free_registry(&mut self.segments, runtime_record.module_registry_id())?;
        drop(modules);
        let vm = self
            .vm
            .free_state(&mut self.segments, runtime_record.vm_state_id())?;
        self.segments.remove_runtime(runtime_record.id());

        Ok(RuntimeDetached::new(vm))
    }

    pub fn contains_runtime(&self, runtime: JsRuntimeId) -> bool {
        self.runtime_index.contains_key(&runtime)
    }
}

impl Default for PoolRuntimeStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeStorageSegments {
    pub runtime_record: SegmentId,
    pub symbol_table: SegmentId,
    pub vm_state: SegmentId,
    pub module_registry: SegmentId,
    pub heap: SegmentId,
    pub bindings: SegmentId,
    pub env_frames: SegmentId,
    pub stack: SegmentId,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeRecordId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct SymbolTableId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct VmStateId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ModuleRegistryId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct RuntimeStorageUsage {
    pub heap_objects: usize,
    pub binding_cells: usize,
    pub env_frames: usize,
    pub stacks: usize,
    pub stack_values: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FlatSegmentStore<T> {
    values: Vec<Option<T>>,
    free_slots: HashMap<SegmentId, Vec<u32>>,
}

impl<T> FlatSegmentStore<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            free_slots: HashMap::new(),
        }
    }

    pub fn alloc(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        value: T,
    ) -> Result<u32, Error> {
        let slot = if let Some(slot) = self.free_slots.get_mut(&segment).and_then(Vec::pop) {
            slot
        } else {
            self.next_empty_slot(segments.meta(segment)?)?
        };
        let index = self.absolute_index(segments.meta(segment)?, slot)?;

        self.ensure_len(index + 1);
        self.values[index] = Some(value);

        let used = self.count_used_in_segment(segments.meta(segment)?)?;
        segments.set_used_slots(segment, used)?;
        segments.mark_dirty(segment, 1)?;

        Ok(slot)
    }

    pub fn get(&self, segments: &SegmentTable, segment: SegmentId, slot: u32) -> Result<&T, Error> {
        let index = self.absolute_index(segments.meta(segment)?, slot)?;
        self.values
            .get(index)
            .and_then(Option::as_ref)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: segment.0,
                slot,
            })
    }

    pub fn get_mut(
        &mut self,
        segments: &SegmentTable,
        segment: SegmentId,
        slot: u32,
    ) -> Result<&mut T, Error> {
        let index = self.absolute_index(segments.meta(segment)?, slot)?;
        self.values
            .get_mut(index)
            .and_then(Option::as_mut)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: segment.0,
                slot,
            })
    }

    pub fn free(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        slot: u32,
    ) -> Result<T, Error> {
        let index = self.absolute_index(segments.meta(segment)?, slot)?;
        let value =
            self.values
                .get_mut(index)
                .and_then(Option::take)
                .ok_or(Error::JsSegmentSlotEmpty {
                    segment: segment.0,
                    slot,
                })?;

        self.free_slots.entry(segment).or_default().push(slot);
        let used = self.count_used_in_segment(segments.meta(segment)?)?;
        segments.set_used_slots(segment, used)?;
        segments.mark_dirty(segment, 1)?;

        Ok(value)
    }

    pub fn clear_segment(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
    ) -> Result<(), Error> {
        let meta = segments.meta(segment)?.clone();
        let start = meta.start_slot as usize;
        let end = start.saturating_add(meta.capacity_slots);
        self.ensure_len(end);

        for slot in start..end {
            self.values[slot] = None;
        }

        self.free_slots.remove(&segment);
        segments.set_used_slots(segment, 0)?;
        segments.mark_dirty(segment, meta.capacity_slots)?;
        Ok(())
    }

    fn next_empty_slot(&self, meta: &SegmentMeta) -> Result<u32, Error> {
        let start = meta.start_slot as usize;
        let end = start.saturating_add(meta.capacity_slots);

        for index in start..end {
            if self.values.get(index).is_none_or(Option::is_none) {
                return Ok((index - start) as u32);
            }
        }

        Err(Error::JsSegmentFull { segment: meta.id.0 })
    }

    fn count_used_in_segment(&self, meta: &SegmentMeta) -> Result<usize, Error> {
        let start = meta.start_slot as usize;
        let end = start.saturating_add(meta.capacity_slots);
        Ok((start..end)
            .filter(|index| self.values.get(*index).is_some_and(Option::is_some))
            .count())
    }

    fn absolute_index(&self, meta: &SegmentMeta, slot: u32) -> Result<usize, Error> {
        if slot as usize >= meta.capacity_slots {
            return Err(Error::JsSegmentSlotOutOfBounds {
                segment: meta.id.0,
                slot,
            });
        }

        Ok(meta.start_slot as usize + slot as usize)
    }

    fn ensure_len(&mut self, len: usize) {
        if self.values.len() < len {
            self.values.resize_with(len, || None);
        }
    }
}

impl<T> Default for FlatSegmentStore<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolHeapStorage {
    pub objects: FlatSegmentStore<HeapObject>,
}

impl PoolHeapStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolEnvStorage {
    pub binding_cells: FlatSegmentStore<Binding>,
    pub env_frames: FlatSegmentStore<EnvFrame>,
}

impl PoolEnvStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VmStack {
    pub values: Vec<JsValue>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct PoolVmStorage {
    pub stacks: FlatSegmentStore<VmStack>,
    states: FlatSegmentStore<Vm>,
}

impl PoolVmStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_state(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        vm: Vm,
    ) -> Result<u32, Error> {
        self.states.alloc(segments, segment, vm)
    }

    pub(crate) fn state(&self, segments: &SegmentTable, id: VmStateId) -> Result<&Vm, Error> {
        self.states.get(segments, id.segment, id.slot)
    }

    pub(crate) fn state_mut(
        &mut self,
        segments: &SegmentTable,
        id: VmStateId,
    ) -> Result<&mut Vm, Error> {
        self.states.get_mut(segments, id.segment, id.slot)
    }

    pub(crate) fn free_state(
        &mut self,
        segments: &mut SegmentTable,
        id: VmStateId,
    ) -> Result<Vm, Error> {
        self.states.free(segments, id.segment, id.slot)
    }
}

#[derive(Default, Deserialize, Serialize)]
pub struct PoolSymbolStorage {
    tables: FlatSegmentStore<SymbolTable>,
    pub names: FlatSegmentStore<String>,
    pub by_runtime_name: HashMap<(JsRuntimeId, String), u32>,
}

impl PoolSymbolStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_table(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        table: SymbolTable,
    ) -> Result<u32, Error> {
        self.tables.alloc(segments, segment, table)
    }

    pub(crate) fn table(
        &self,
        segments: &SegmentTable,
        id: SymbolTableId,
    ) -> Result<&SymbolTable, Error> {
        self.tables.get(segments, id.segment, id.slot)
    }

    pub(crate) fn table_mut(
        &mut self,
        segments: &SegmentTable,
        id: SymbolTableId,
    ) -> Result<&mut SymbolTable, Error> {
        self.tables.get_mut(segments, id.segment, id.slot)
    }

    pub(crate) fn free_table(
        &mut self,
        segments: &mut SegmentTable,
        id: SymbolTableId,
    ) -> Result<SymbolTable, Error> {
        self.tables.free(segments, id.segment, id.slot)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoredProgram {
    pub constants_start: u32,
    pub constants_len: u32,
    pub instructions_start: u32,
    pub instructions_len: u32,
    pub functions_start: u32,
    pub functions_len: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoredFunction {
    pub name: Option<super::Symbol>,
    pub params: Vec<super::Symbol>,
    pub body: BytecodeProgram,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolProgramStorage {
    pub programs: FlatSegmentStore<StoredProgram>,
    pub constants: Vec<Constant>,
    pub instructions: Vec<Instr>,
    pub functions: Vec<StoredFunction>,
}

impl PoolProgramStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolModuleStorage {
    pub modules: FlatSegmentStore<ModuleRecord>,
    registries: FlatSegmentStore<ModuleRegistry>,
    pub by_runtime_key: HashMap<(JsRuntimeId, ModuleKey), u32>,
    pub import_entries: Vec<ImportEntry>,
    pub local_export_entries: Vec<LocalExportEntry>,
}

impl PoolModuleStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_registry(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        registry: ModuleRegistry,
    ) -> Result<u32, Error> {
        self.registries.alloc(segments, segment, registry)
    }

    pub(crate) fn registry(
        &self,
        segments: &SegmentTable,
        id: ModuleRegistryId,
    ) -> Result<&ModuleRegistry, Error> {
        self.registries.get(segments, id.segment, id.slot)
    }

    pub(crate) fn registry_mut(
        &mut self,
        segments: &SegmentTable,
        id: ModuleRegistryId,
    ) -> Result<&mut ModuleRegistry, Error> {
        self.registries.get_mut(segments, id.segment, id.slot)
    }

    pub(crate) fn free_registry(
        &mut self,
        segments: &mut SegmentTable,
        id: ModuleRegistryId,
    ) -> Result<ModuleRegistry, Error> {
        self.registries.free(segments, id.segment, id.slot)
    }
}

#[derive(Debug, Default)]
pub struct RuntimeTable {
    next_runtime_id: u64,
    slots: Vec<Option<RuntimeSlot>>,
    by_id: HashMap<JsRuntimeId, u32>,
    free_slots: Vec<u32>,
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
        if self.by_id.contains_key(&slot.id) {
            return Err(Error::JsRuntimeAlreadyExists(slot.id));
        }

        let id = slot.id;
        let index = self.free_slots.pop().unwrap_or(self.slots.len() as u32);
        if index as usize == self.slots.len() {
            self.slots.push(Some(slot));
        } else {
            self.slots[index as usize] = Some(slot);
        }
        self.by_id.insert(id, index);

        Ok(())
    }

    pub fn remove(&mut self, id: JsRuntimeId) -> Result<RuntimeSlot, Error> {
        let index = self.by_id.remove(&id).ok_or(Error::RuntimeNotFound(id))?;
        let slot = self
            .slots
            .get_mut(index as usize)
            .and_then(Option::take)
            .ok_or(Error::RuntimeNotFound(id))?;
        self.free_slots.push(index);
        Ok(slot)
    }

    pub fn get(&self, id: JsRuntimeId) -> Result<&RuntimeSlot, Error> {
        let index = self
            .by_id
            .get(&id)
            .copied()
            .ok_or(Error::RuntimeNotFound(id))?;
        self.slots
            .get(index as usize)
            .and_then(Option::as_ref)
            .ok_or(Error::RuntimeNotFound(id))
    }

    pub fn get_mut(&mut self, id: JsRuntimeId) -> Result<&mut RuntimeSlot, Error> {
        let index = self
            .by_id
            .get(&id)
            .copied()
            .ok_or(Error::RuntimeNotFound(id))?;
        self.slots
            .get_mut(index as usize)
            .and_then(Option::as_mut)
            .ok_or(Error::RuntimeNotFound(id))
    }

    pub fn contains(&self, id: JsRuntimeId) -> bool {
        self.by_id.contains_key(&id)
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
