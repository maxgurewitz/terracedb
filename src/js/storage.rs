use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{
    GcPolicy, JsRuntimeId, ModuleRegistry, RuntimeDetached, RuntimeRecord, RuntimeRecordView,
    RuntimeRecordViewRef, RuntimeSnapshot, SymbolTable, Vm,
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
        self.add_runtime_segment(owner, kind, id);
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

        for segment in self.runtime_segment_iter(segments) {
            let meta = self.meta(segment)?;
            total = total.saturating_add(meta.used_slots.saturating_mul(8));
        }

        Ok(total)
    }

    pub fn runtime_dirty_bytes(&self, runtime: JsRuntimeId) -> Result<usize, Error> {
        let segments = self
            .segments_for_runtime(runtime)
            .ok_or(Error::RuntimeNotFound(runtime))?;
        let mut total = 0usize;

        for segment in self.runtime_segment_iter(segments) {
            total = total.saturating_add(self.meta(segment)?.dirty_bytes);
        }

        Ok(total)
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

    fn runtime_segment_iter<'a>(
        &'a self,
        segments: &'a RuntimeSegments,
    ) -> impl Iterator<Item = SegmentId> + 'a {
        segments
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
            .copied()
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
    runtime_records: RuntimeRecordCols,
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
            runtime_records: RuntimeRecordCols::new(),
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
        object: super::ObjectId,
    ) -> Result<(), Error> {
        self.validate_segment_owner(runtime, object.segment)
    }

    pub fn refresh_runtime_usage(&mut self, runtime: JsRuntimeId) -> Result<(), Error> {
        let (storage_segments, usage) = {
            let runtime_state = self.runtime(runtime)?;
            let vm = self.vm.state(runtime_state.vm_state_id())?;
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

        let symbol_slot = self
            .symbols
            .alloc_table(storage_segments.symbol_table, SymbolTable::new())?;
        let vm_slot = self.vm.alloc_state(
            storage_segments.vm_state,
            Vm::with_storage_segments(storage_segments, gc_policy),
        )?;
        let module_slot = self
            .modules
            .alloc_registry(storage_segments.module_registry, ModuleRegistry::new())?;
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

    pub(crate) fn insert_runtime_snapshot(
        &mut self,
        snapshot: RuntimeSnapshot,
    ) -> Result<(), Error> {
        let runtime_id = snapshot.runtime_id;
        let storage_segments = snapshot.storage_segments;

        if self.runtime_index.contains_key(&runtime_id) {
            return Err(Error::JsRuntimeAlreadyExists(runtime_id));
        }

        let symbol_slot = self
            .symbols
            .alloc_table(storage_segments.symbol_table, snapshot.symbols)?;
        let vm_slot = self
            .vm
            .alloc_state(storage_segments.vm_state, snapshot.vm)?;
        let module_slot = self
            .modules
            .alloc_registry(storage_segments.module_registry, snapshot.modules)?;
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

    pub(crate) fn runtime(&self, runtime: JsRuntimeId) -> Result<RuntimeRecord, Error> {
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
        let symbols = self.symbols.table(record.symbol_table_id())?;
        let vm = self.vm.state(record.vm_state_id())?;
        let modules = self.modules.registry(record.module_registry_id())?;

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
        let symbols = self.symbols.table_mut(symbol_table)?;
        let vm = self.vm.state_mut(vm_state)?;
        let modules = self.modules.registry_mut(module_registry)?;

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
        let _symbols = self.symbols.free_table(runtime_record.symbol_table_id())?;
        let _modules = self
            .modules
            .free_registry(runtime_record.module_registry_id())?;
        let vm = self.vm.free_state(runtime_record.vm_state_id())?;
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

impl RuntimeStorageSegments {
    fn empty() -> Self {
        Self {
            runtime_record: SegmentId(0),
            symbol_table: SegmentId(0),
            vm_state: SegmentId(0),
            module_registry: SegmentId(0),
            heap: SegmentId(0),
            bindings: SegmentId(0),
            env_frames: SegmentId(0),
            stack: SegmentId(0),
        }
    }
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

#[derive(Default)]
pub struct RuntimeRecordCols {
    live: Vec<bool>,
    id: Vec<JsRuntimeId>,
    storage_segments: Vec<RuntimeStorageSegments>,
    symbol_table: Vec<SymbolTableId>,
    vm_state: Vec<VmStateId>,
    module_registry: Vec<ModuleRegistryId>,
}

impl RuntimeRecordCols {
    fn new() -> Self {
        Self::default()
    }

    fn alloc(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        value: RuntimeRecord,
    ) -> Result<u32, Error> {
        let slot = 0;
        let index = absolute_index(segments.meta(segment)?, slot)?;
        ensure_slot(&mut self.live, index, false);
        ensure_slot(&mut self.id, index, JsRuntimeId(0));
        ensure_slot(
            &mut self.storage_segments,
            index,
            RuntimeStorageSegments::empty(),
        );
        ensure_slot(
            &mut self.symbol_table,
            index,
            SymbolTableId {
                segment: SegmentId(0),
                slot: 0,
            },
        );
        ensure_slot(
            &mut self.vm_state,
            index,
            VmStateId {
                segment: SegmentId(0),
                slot: 0,
            },
        );
        ensure_slot(
            &mut self.module_registry,
            index,
            ModuleRegistryId {
                segment: SegmentId(0),
                slot: 0,
            },
        );
        self.live[index] = true;
        self.id[index] = value.id();
        self.storage_segments[index] = value.storage_segments();
        self.symbol_table[index] = value.symbol_table_id();
        self.vm_state[index] = value.vm_state_id();
        self.module_registry[index] = value.module_registry_id();
        segments.set_used_slots(segment, 1)?;
        segments.mark_dirty(segment, 1)?;
        Ok(slot)
    }

    fn get(
        &self,
        segments: &SegmentTable,
        segment: SegmentId,
        slot: u32,
    ) -> Result<RuntimeRecord, Error> {
        let index = absolute_index(segments.meta(segment)?, slot)?;
        if !self.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: segment.0,
                slot,
            });
        }
        Ok(RuntimeRecord::new(
            self.id[index],
            self.storage_segments[index],
            self.symbol_table[index],
            self.vm_state[index],
            self.module_registry[index],
        ))
    }

    fn free(
        &mut self,
        segments: &mut SegmentTable,
        segment: SegmentId,
        slot: u32,
    ) -> Result<RuntimeRecord, Error> {
        let index = absolute_index(segments.meta(segment)?, slot)?;
        if !self.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: segment.0,
                slot,
            });
        }
        self.live[index] = false;
        segments.set_used_slots(segment, 0)?;
        segments.mark_dirty(segment, 1)?;
        Ok(RuntimeRecord::new(
            self.id[index],
            self.storage_segments[index],
            self.symbol_table[index],
            self.vm_state[index],
            self.module_registry[index],
        ))
    }
}

#[derive(Default)]
pub struct SymbolTableCols {
    live: Vec<bool>,
    segment: Vec<SegmentId>,
    value: Vec<SymbolTable>,
}

#[derive(Default)]
pub struct VmStateCols {
    live: Vec<bool>,
    segment: Vec<SegmentId>,
    value: Vec<Vm>,
}

#[derive(Default)]
pub struct ModuleRegistryCols {
    live: Vec<bool>,
    segment: Vec<SegmentId>,
    value: Vec<ModuleRegistry>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolHeapStorage {
    pub heap_object_live: Vec<bool>,
}

impl PoolHeapStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolEnvStorage {
    pub env_cell_live: Vec<bool>,
    pub env_frame_live: Vec<bool>,
}

impl PoolEnvStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct VmStack {
    pub value_tag: Vec<super::ValueTag>,
    pub value_number: Vec<f64>,
    pub value_bool: Vec<bool>,
    pub value_string: Vec<String>,
    pub value_object: Vec<super::ObjectId>,
}

#[derive(Default)]
pub struct PoolVmStorage {
    pub stack_live: Vec<bool>,
    states: VmStateCols,
}

impl PoolVmStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_state(&mut self, segment: SegmentId, vm: Vm) -> Result<u32, Error> {
        let index = segment.0 as usize;
        ensure_slot(&mut self.states.live, index, false);
        ensure_slot(&mut self.states.segment, index, SegmentId(0));
        ensure_default_slot(&mut self.states.value, index);
        self.states.live[index] = true;
        self.states.segment[index] = segment;
        self.states.value[index] = vm;
        Ok(0)
    }

    pub(crate) fn state(&self, id: VmStateId) -> Result<&Vm, Error> {
        let index = id.segment.0 as usize;
        if !self.states.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.states
            .value
            .get(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn state_mut(&mut self, id: VmStateId) -> Result<&mut Vm, Error> {
        let index = id.segment.0 as usize;
        if !self.states.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.states
            .value
            .get_mut(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn free_state(&mut self, id: VmStateId) -> Result<Vm, Error> {
        let index = id.segment.0 as usize;
        if !self.states.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.states.live[index] = false;
        Ok(std::mem::take(&mut self.states.value[index]))
    }
}

#[derive(Default)]
pub struct PoolSymbolStorage {
    tables: SymbolTableCols,
    pub string_bytes: Vec<u8>,
    pub string_start: Vec<u32>,
    pub string_len: Vec<u32>,
    pub symbol_live: Vec<bool>,
    pub symbol_runtime: Vec<JsRuntimeId>,
    pub symbol_string: Vec<u32>,
    pub by_runtime_name: HashMap<(JsRuntimeId, String), u32>,
}

impl PoolSymbolStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_table(
        &mut self,
        segment: SegmentId,
        table: SymbolTable,
    ) -> Result<u32, Error> {
        let index = segment.0 as usize;
        ensure_slot(&mut self.tables.live, index, false);
        ensure_slot(&mut self.tables.segment, index, SegmentId(0));
        ensure_default_slot(&mut self.tables.value, index);
        self.tables.live[index] = true;
        self.tables.segment[index] = segment;
        self.tables.value[index] = table;
        Ok(0)
    }

    pub(crate) fn table(&self, id: SymbolTableId) -> Result<&SymbolTable, Error> {
        let index = id.segment.0 as usize;
        if !self.tables.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.tables
            .value
            .get(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn table_mut(&mut self, id: SymbolTableId) -> Result<&mut SymbolTable, Error> {
        let index = id.segment.0 as usize;
        if !self.tables.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.tables
            .value
            .get_mut(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn free_table(&mut self, id: SymbolTableId) -> Result<SymbolTable, Error> {
        let index = id.segment.0 as usize;
        if !self.tables.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.tables.live[index] = false;
        Ok(std::mem::take(&mut self.tables.value[index]))
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
    pub param_start: u32,
    pub param_len: u32,
    pub body: super::ProgramId,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct PoolProgramStorage {
    pub program_live: Vec<bool>,
    pub program_runtime: Vec<JsRuntimeId>,
    pub program_constant_range_start: Vec<u32>,
    pub program_constant_range_len: Vec<u32>,
    pub program_instr_range_start: Vec<u32>,
    pub program_instr_range_len: Vec<u32>,
    pub program_function_range_start: Vec<u32>,
    pub program_function_range_len: Vec<u32>,
    pub constant_value: super::ValueCols,
    pub instr_opcode: Vec<super::Instr>,
    pub compiled_function_name: Vec<Option<super::Symbol>>,
    pub compiled_function_param_range_start: Vec<u32>,
    pub compiled_function_param_range_len: Vec<u32>,
    pub compiled_function_body: Vec<super::ProgramId>,
    pub compiled_function_param_symbol: Vec<super::Symbol>,
}

impl PoolProgramStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct PoolModuleStorage {
    pub module_live: Vec<bool>,
    pub module_runtime: Vec<JsRuntimeId>,
    registries: ModuleRegistryCols,
    pub by_runtime_key: HashMap<(JsRuntimeId, super::ModuleKey), u32>,
    pub module_import_request: Vec<super::ModuleKey>,
    pub module_local_export_local_name: Vec<super::Symbol>,
}

impl PoolModuleStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn alloc_registry(
        &mut self,
        segment: SegmentId,
        registry: ModuleRegistry,
    ) -> Result<u32, Error> {
        let index = segment.0 as usize;
        ensure_slot(&mut self.registries.live, index, false);
        ensure_slot(&mut self.registries.segment, index, SegmentId(0));
        ensure_default_slot(&mut self.registries.value, index);
        self.registries.live[index] = true;
        self.registries.segment[index] = segment;
        self.registries.value[index] = registry;
        Ok(0)
    }

    pub(crate) fn registry(&self, id: ModuleRegistryId) -> Result<&ModuleRegistry, Error> {
        let index = id.segment.0 as usize;
        if !self.registries.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.registries
            .value
            .get(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn registry_mut(
        &mut self,
        id: ModuleRegistryId,
    ) -> Result<&mut ModuleRegistry, Error> {
        let index = id.segment.0 as usize;
        if !self.registries.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.registries
            .value
            .get_mut(index)
            .ok_or(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            })
    }

    pub(crate) fn free_registry(&mut self, id: ModuleRegistryId) -> Result<ModuleRegistry, Error> {
        let index = id.segment.0 as usize;
        if !self.registries.live.get(index).copied().unwrap_or(false) {
            return Err(Error::JsSegmentSlotEmpty {
                segment: id.segment.0,
                slot: id.slot,
            });
        }
        self.registries.live[index] = false;
        Ok(std::mem::take(&mut self.registries.value[index]))
    }
}

#[derive(Debug, Default)]
pub struct RuntimeTable {
    next_runtime_id: u64,
    runtime_live: Vec<bool>,
    runtime_id: Vec<JsRuntimeId>,
    runtime_state: Vec<RuntimeState>,
    runtime_root_env: Vec<super::EnvFrameId>,
    runtime_stack: Vec<super::StackId>,
    runtime_current_program: Vec<Option<super::ProgramId>>,
    runtime_resident_bytes: Vec<usize>,
    runtime_dirty_bytes: Vec<usize>,
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

        let index = self
            .free_slots
            .pop()
            .unwrap_or(self.runtime_live.len() as u32) as usize;
        ensure_slot(&mut self.runtime_live, index, false);
        ensure_slot(&mut self.runtime_id, index, JsRuntimeId(0));
        ensure_slot(&mut self.runtime_state, index, RuntimeState::Idle);
        ensure_slot(&mut self.runtime_root_env, index, slot.root_env);
        ensure_slot(&mut self.runtime_stack, index, slot.stack);
        ensure_slot(&mut self.runtime_current_program, index, None);
        ensure_slot(&mut self.runtime_resident_bytes, index, 0);
        ensure_slot(&mut self.runtime_dirty_bytes, index, 0);

        self.runtime_live[index] = true;
        self.runtime_id[index] = slot.id;
        self.runtime_state[index] = slot.state;
        self.runtime_root_env[index] = slot.root_env;
        self.runtime_stack[index] = slot.stack;
        self.runtime_current_program[index] = slot.current_program;
        self.runtime_resident_bytes[index] = slot.resident_bytes;
        self.runtime_dirty_bytes[index] = slot.dirty_bytes;
        self.by_id.insert(slot.id, index as u32);
        Ok(())
    }

    pub fn remove(&mut self, id: JsRuntimeId) -> Result<RuntimeSlot, Error> {
        let index = self.by_id.remove(&id).ok_or(Error::RuntimeNotFound(id))? as usize;
        if !self.runtime_live.get(index).copied().unwrap_or(false) {
            return Err(Error::RuntimeNotFound(id));
        }

        self.runtime_live[index] = false;
        self.free_slots.push(index as u32);
        Ok(self.slot_at(index))
    }

    pub fn get(&self, id: JsRuntimeId) -> Result<RuntimeSlotRef<'_>, Error> {
        let index = self.index_for(id)?;
        Ok(RuntimeSlotRef { table: self, index })
    }

    pub fn get_mut(&mut self, id: JsRuntimeId) -> Result<RuntimeSlotMut<'_>, Error> {
        let index = self.index_for(id)?;
        Ok(RuntimeSlotMut { table: self, index })
    }

    pub fn contains(&self, id: JsRuntimeId) -> bool {
        self.by_id.contains_key(&id)
    }

    fn index_for(&self, id: JsRuntimeId) -> Result<usize, Error> {
        let index = self
            .by_id
            .get(&id)
            .copied()
            .ok_or(Error::RuntimeNotFound(id))? as usize;
        if !self.runtime_live.get(index).copied().unwrap_or(false) {
            return Err(Error::RuntimeNotFound(id));
        }
        Ok(index)
    }

    fn slot_at(&self, index: usize) -> RuntimeSlot {
        RuntimeSlot {
            id: self.runtime_id[index],
            state: self.runtime_state[index],
            root_env: self.runtime_root_env[index],
            stack: self.runtime_stack[index],
            current_program: self.runtime_current_program[index],
            resident_bytes: self.runtime_resident_bytes[index],
            dirty_bytes: self.runtime_dirty_bytes[index],
        }
    }
}

pub struct RuntimeSlotRef<'a> {
    table: &'a RuntimeTable,
    index: usize,
}

impl RuntimeSlotRef<'_> {
    pub fn id(&self) -> JsRuntimeId {
        self.table.runtime_id[self.index]
    }
}

pub struct RuntimeSlotMut<'a> {
    table: &'a mut RuntimeTable,
    index: usize,
}

impl RuntimeSlotMut<'_> {
    pub fn set_accounting(&mut self, resident_bytes: usize, dirty_bytes: usize) {
        self.table.runtime_resident_bytes[self.index] = resident_bytes;
        self.table.runtime_dirty_bytes[self.index] = dirty_bytes;
    }
}

#[derive(Debug)]
pub struct RuntimeSlot {
    pub id: JsRuntimeId,
    pub state: RuntimeState,
    pub root_env: super::EnvFrameId,
    pub stack: super::StackId,
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

fn absolute_index(meta: &SegmentMeta, slot: u32) -> Result<usize, Error> {
    if slot as usize >= meta.capacity_slots {
        return Err(Error::JsSegmentSlotOutOfBounds {
            segment: meta.id.0,
            slot,
        });
    }
    Ok(meta.start_slot as usize + slot as usize)
}

fn ensure_slot<T: Clone>(values: &mut Vec<T>, index: usize, value: T) {
    if values.len() <= index {
        values.resize(index + 1, value);
    }
}

fn ensure_default_slot<T: Default>(values: &mut Vec<T>, index: usize) {
    if values.len() <= index {
        values.resize_with(index + 1, T::default);
    }
}
