use std::{collections::HashMap, mem};

use serde::{Deserialize, Serialize};

use crate::Error;

use super::attachment::JsHostBindings;
use super::{
    BindingCellId, BytecodeProgram, EnvFrameId, JsStreamKind, JsValue, ModuleId, SegmentId,
    StorageColumn, Symbol, SymbolTable, ValueCols,
};

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ObjectId {
    pub segment: SegmentId,
    pub slot: u32,
}

impl ObjectId {
    pub(crate) fn debug_index(self) -> u64 {
        self.slot as u64
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum PropertyKey {
    Symbol(Symbol),
}

pub const DEFAULT_GC_THRESHOLD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum GcPolicy {
    ManualOnly,
    Automatic { threshold_bytes: usize },
}

impl Default for GcPolicy {
    fn default() -> Self {
        Self::Automatic {
            threshold_bytes: DEFAULT_GC_THRESHOLD_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcHeader {
    pub ref_count: u32,
    pub mark: GcMark,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum GcMark {
    None,
    Tmp,
    Live,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
enum GcPhase {
    None,
    RemoveCycles,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub struct HeapStats {
    pub allocated_objects: usize,
    pub allocated_bytes: usize,
    pub freed_objects: usize,
    pub gc_runs: u64,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum ObjectKindTag {
    Ordinary,
    HostFunction,
    JsFunction,
    ModuleNamespace,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsHeap {
    segment: SegmentId,
    free_object_slots: Vec<u32>,

    heap_object_live: Vec<bool>,
    heap_object_ref_count: Vec<u32>,
    heap_object_gc_mark: Vec<GcMark>,
    heap_object_bytes: Vec<usize>,
    heap_object_kind: Vec<ObjectKindTag>,
    heap_object_kind_payload: Vec<u32>,
    heap_object_property_range_start: Vec<u32>,
    heap_object_property_range_len: Vec<u32>,

    heap_property_live: Vec<bool>,
    heap_property_key_symbol: StorageColumn<Symbol>,
    heap_property_value: ValueCols,

    #[serde(skip, default)]
    property_index: HashMap<(u32, Symbol), usize>,

    host_function_live: Vec<bool>,
    host_function_name: Vec<Symbol>,
    host_function_kind: Vec<HostFunctionKind>,

    js_function_live: Vec<bool>,
    js_function_name: Vec<Option<Symbol>>,
    js_function_param_range_start: Vec<u32>,
    js_function_param_range_len: Vec<u32>,
    js_function_body: Vec<BytecodeProgram>,
    js_function_captured_env: Vec<EnvFrameId>,
    js_function_param_symbol: Vec<Symbol>,

    module_namespace_live: Vec<bool>,
    module_namespace_module: Vec<ModuleId>,
    module_namespace_export_range_start: Vec<u32>,
    module_namespace_export_range_len: Vec<u32>,
    module_namespace_export_symbol: Vec<Symbol>,
    module_namespace_export_cell: Vec<BindingCellId>,

    gc_object: Vec<ObjectId>,
    gc_zero_ref: Vec<ObjectId>,
    gc_tmp_cycle: Vec<ObjectId>,
    allocated_objects: usize,
    allocated_bytes: usize,
    freed_objects: usize,
    gc_runs: u64,
    gc_policy: GcPolicy,
    gc_phase: GcPhase,
}

impl JsHeap {
    pub fn new() -> Self {
        Self::with_gc_policy(GcPolicy::default())
    }

    pub fn with_gc_policy(gc_policy: GcPolicy) -> Self {
        Self::with_segment_and_gc_policy(SegmentId(0), gc_policy)
    }

    pub fn with_segment_and_gc_policy(segment: SegmentId, gc_policy: GcPolicy) -> Self {
        Self {
            segment,
            free_object_slots: Vec::new(),
            heap_object_live: Vec::new(),
            heap_object_ref_count: Vec::new(),
            heap_object_gc_mark: Vec::new(),
            heap_object_bytes: Vec::new(),
            heap_object_kind: Vec::new(),
            heap_object_kind_payload: Vec::new(),
            heap_object_property_range_start: Vec::new(),
            heap_object_property_range_len: Vec::new(),
            heap_property_live: Vec::new(),
            heap_property_key_symbol: StorageColumn::new(),
            heap_property_value: ValueCols::new(),
            property_index: HashMap::new(),
            host_function_live: Vec::new(),
            host_function_name: Vec::new(),
            host_function_kind: Vec::new(),
            js_function_live: Vec::new(),
            js_function_name: Vec::new(),
            js_function_param_range_start: Vec::new(),
            js_function_param_range_len: Vec::new(),
            js_function_body: Vec::new(),
            js_function_captured_env: Vec::new(),
            js_function_param_symbol: Vec::new(),
            module_namespace_live: Vec::new(),
            module_namespace_module: Vec::new(),
            module_namespace_export_range_start: Vec::new(),
            module_namespace_export_range_len: Vec::new(),
            module_namespace_export_symbol: Vec::new(),
            module_namespace_export_cell: Vec::new(),
            gc_object: Vec::new(),
            gc_zero_ref: Vec::new(),
            gc_tmp_cycle: Vec::new(),
            allocated_objects: 0,
            allocated_bytes: 0,
            freed_objects: 0,
            gc_runs: 0,
            gc_policy,
            gc_phase: GcPhase::None,
        }
    }

    pub(crate) fn allocated_objects(&self) -> usize {
        self.allocated_objects
    }

    pub fn alloc_object(&mut self, kind: ObjectKind) -> ObjectId {
        let (kind_tag, payload) = self.alloc_kind_payload(kind);
        let slot = self
            .free_object_slots
            .pop()
            .unwrap_or(self.heap_object_live.len() as u32);
        let id = ObjectId {
            segment: self.segment,
            slot,
        };
        let index = slot as usize;
        let bytes = estimate_object_bytes(kind_tag);

        if index == self.heap_object_live.len() {
            self.heap_object_live.push(true);
            self.heap_object_ref_count.push(0);
            self.heap_object_gc_mark.push(GcMark::None);
            self.heap_object_bytes.push(bytes);
            self.heap_object_kind.push(kind_tag);
            self.heap_object_kind_payload.push(payload);
            self.heap_object_property_range_start
                .push(self.heap_property_live.len() as u32);
            self.heap_object_property_range_len.push(0);
        } else {
            self.heap_object_live[index] = true;
            self.heap_object_ref_count[index] = 0;
            self.heap_object_gc_mark[index] = GcMark::None;
            self.heap_object_bytes[index] = bytes;
            self.heap_object_kind[index] = kind_tag;
            self.heap_object_kind_payload[index] = payload;
            self.heap_object_property_range_start[index] = self.heap_property_live.len() as u32;
            self.heap_object_property_range_len[index] = 0;
        }

        self.gc_object.push(id);
        self.allocated_objects += 1;
        self.allocated_bytes += bytes;

        id
    }

    pub fn dup_value(&mut self, value: &JsValue) {
        if let JsValue::Object(id) = value {
            self.dup_object(*id);
        }
    }

    pub fn free_value(&mut self, value: JsValue) -> Result<(), Error> {
        if let JsValue::Object(id) = value {
            self.free_object_ref(id)?;
        }

        Ok(())
    }

    pub fn dup_object(&mut self, id: ObjectId) {
        let index = self
            .live_object_index(id)
            .expect("dup_object for missing object");
        self.heap_object_ref_count[index] += 1;
    }

    pub fn free_object_ref(&mut self, id: ObjectId) -> Result<(), Error> {
        let index = self.live_object_index(id)?;

        if self.heap_object_ref_count[index] == 0 {
            return Err(Error::JsRefCountUnderflow {
                object: id.debug_index(),
            });
        }

        self.heap_object_ref_count[index] -= 1;

        if self.heap_object_ref_count[index] == 0 {
            if self.gc_phase == GcPhase::RemoveCycles {
                return Ok(());
            }

            self.gc_zero_ref.push(id);
            self.drain_zero_ref()?;
        }

        Ok(())
    }

    pub fn get_property(&mut self, object: ObjectId, key: PropertyKey) -> Result<JsValue, Error> {
        self.live_object_index(object)?;
        let value = self
            .find_property_index(object, key)?
            .map(|property| self.heap_property_value.get(property as u32))
            .unwrap_or(JsValue::Undefined);

        self.dup_value(&value);

        Ok(value)
    }

    pub fn set_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        value: JsValue,
    ) -> Result<(), Error> {
        self.live_object_index(object)?;
        self.dup_value(&value);

        if let Some(property) = self.find_property_index(object, key)? {
            let old = self.heap_property_value.get(property as u32);
            self.heap_property_value.set(property as u32, value);
            return self.free_value(old);
        }

        let result = self.push_object_property(object, key, value.clone());
        if let Err(err) = result {
            self.free_value(value)?;
            return Err(err);
        }

        Ok(())
    }

    pub fn object_kind(&self, object: ObjectId) -> Result<ObjectKind, Error> {
        let index = self.live_object_index(object)?;
        self.object_kind_at(index)
    }

    pub fn stats(&self) -> HeapStats {
        HeapStats {
            allocated_objects: self.allocated_objects,
            allocated_bytes: self.allocated_bytes,
            freed_objects: self.freed_objects,
            gc_runs: self.gc_runs,
        }
    }

    #[cfg(test)]
    pub(crate) fn storage_trace(&self) -> super::StorageAccessTrace {
        super::StorageAccessTrace {
            heap_property_key_symbol: self.heap_property_key_symbol.trace(),
            ..super::StorageAccessTrace::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_storage_trace(&self) {
        self.heap_property_key_symbol.reset_trace();
    }

    pub fn maybe_run_gc(&mut self) -> Result<usize, Error> {
        match self.gc_policy {
            GcPolicy::ManualOnly => Ok(0),
            GcPolicy::Automatic { threshold_bytes } => {
                if self.allocated_bytes >= threshold_bytes {
                    self.run_gc()
                } else {
                    Ok(0)
                }
            }
        }
    }

    pub(crate) fn should_run_gc(&self) -> bool {
        match self.gc_policy {
            GcPolicy::ManualOnly => false,
            GcPolicy::Automatic { threshold_bytes } => self.allocated_bytes >= threshold_bytes,
        }
    }

    pub fn force_gc(&mut self) -> Result<usize, Error> {
        self.run_gc()
    }

    pub(crate) fn runtime_edges(&self, object: ObjectId) -> Result<RuntimeEdges, Error> {
        let mut edges = RuntimeEdges {
            objects: Vec::new(),
            frames: Vec::new(),
        };

        self.append_object_runtime_edges(object, &mut edges)?;

        Ok(edges)
    }

    pub fn run_gc(&mut self) -> Result<usize, Error> {
        self.gc_runs += 1;
        self.drain_zero_ref()?;
        self.reset_gc_marks();
        self.gc_decref_all()?;
        self.gc_scan_live()?;
        self.gc_restore_cycles()?;
        let freed = self.gc_free_cycles()?;
        self.drain_zero_ref()?;

        Ok(freed)
    }

    pub(crate) fn drain_zero_ref(&mut self) -> Result<(), Error> {
        while let Some(id) = self.gc_zero_ref.pop() {
            let should_free = self
                .live_object_index(id)
                .ok()
                .is_some_and(|index| self.heap_object_ref_count[index] == 0);

            if should_free {
                self.free_object_now(id)?;
            }
        }

        Ok(())
    }

    fn free_object_now(&mut self, id: ObjectId) -> Result<(), Error> {
        let index = self.live_object_index(id)?;
        let bytes = self.heap_object_bytes[index];
        self.release_object_children(id)?;
        self.account_freed_object(id, bytes);
        Ok(())
    }

    fn gc_decref_all(&mut self) -> Result<(), Error> {
        self.gc_tmp_cycle.clear();

        for id in self.gc_object.clone() {
            if self.live_object_index(id).is_ok() {
                self.gc_decref_children(id)?;
            }
        }

        Ok(())
    }

    fn gc_decref_children(&mut self, id: ObjectId) -> Result<(), Error> {
        let children = self.child_objects(id)?;

        for child in children {
            let child_index = self.live_object_index(child)?;

            if self.heap_object_ref_count[child_index] == 0 {
                return Err(Error::JsRefCountUnderflow {
                    object: child.debug_index(),
                });
            }

            self.heap_object_ref_count[child_index] -= 1;

            if self.heap_object_ref_count[child_index] == 0
                && self.heap_object_gc_mark[child_index] != GcMark::Tmp
            {
                self.heap_object_gc_mark[child_index] = GcMark::Tmp;
                self.gc_tmp_cycle.push(child);
            }
        }

        Ok(())
    }

    fn gc_scan_live(&mut self) -> Result<(), Error> {
        for id in self.gc_object.clone() {
            let is_live_root = self
                .live_object_index(id)
                .ok()
                .is_some_and(|index| self.heap_object_ref_count[index] > 0);

            if is_live_root {
                self.gc_scan_object(id)?;
            }
        }

        Ok(())
    }

    fn gc_scan_object(&mut self, id: ObjectId) -> Result<(), Error> {
        let index = self.live_object_index(id)?;

        if self.heap_object_gc_mark[index] == GcMark::Live {
            return Ok(());
        }

        self.heap_object_gc_mark[index] = GcMark::Live;

        for child in self.child_objects(id)? {
            if let Ok(child_index) = self.live_object_index(child) {
                self.heap_object_ref_count[child_index] += 1;
            }

            self.gc_scan_object(child)?;
        }

        Ok(())
    }

    fn gc_restore_cycles(&mut self) -> Result<(), Error> {
        for id in self.gc_tmp_cycle.clone() {
            let should_restore = self
                .live_object_index(id)
                .ok()
                .is_some_and(|index| self.heap_object_gc_mark[index] != GcMark::Live);

            if should_restore {
                self.gc_incref_children(id)?;
            }
        }

        Ok(())
    }

    fn gc_incref_children(&mut self, id: ObjectId) -> Result<(), Error> {
        for child in self.child_objects(id)? {
            if let Ok(child_index) = self.live_object_index(child) {
                self.heap_object_ref_count[child_index] += 1;
            }
        }

        Ok(())
    }

    fn gc_free_cycles(&mut self) -> Result<usize, Error> {
        let candidates = mem::take(&mut self.gc_tmp_cycle);
        let collected = candidates
            .into_iter()
            .filter(|id| {
                self.live_object_index(*id)
                    .ok()
                    .is_some_and(|index| self.heap_object_gc_mark[index] != GcMark::Live)
            })
            .collect::<Vec<_>>();

        self.gc_phase = GcPhase::RemoveCycles;
        for id in &collected {
            if self.live_object_index(*id).is_ok() {
                self.release_object_children(*id)?;
            }
        }
        self.gc_phase = GcPhase::None;

        let mut freed = 0;

        for id in collected {
            if let Ok(index) = self.live_object_index(id) {
                let bytes = self.heap_object_bytes[index];
                self.account_freed_object(id, bytes);
                freed += 1;
            }
        }

        self.reset_gc_marks();

        Ok(freed)
    }

    fn release_object_children(&mut self, id: ObjectId) -> Result<(), Error> {
        let values = self.child_values(id)?;

        for value in values {
            self.free_value(value)?;
        }

        Ok(())
    }

    fn reset_gc_marks(&mut self) {
        for index in 0..self.heap_object_gc_mark.len() {
            if self.heap_object_live[index] {
                self.heap_object_gc_mark[index] = GcMark::None;
            }
        }
    }

    fn account_freed_object(&mut self, id: ObjectId, bytes: usize) {
        let index = id.slot as usize;
        let properties = self.property_indices(id).unwrap_or_default();
        self.heap_object_live[index] = false;
        self.heap_object_ref_count[index] = 0;
        self.allocated_objects = self.allocated_objects.saturating_sub(1);
        self.freed_objects += 1;
        self.allocated_bytes = self.allocated_bytes.saturating_sub(bytes);
        self.gc_object.retain(|existing| *existing != id);
        self.gc_zero_ref.retain(|existing| *existing != id);
        self.property_index
            .retain(|(object_slot, _), _| *object_slot != id.slot);
        self.free_object_slots.push(id.slot);

        for property in properties {
            self.heap_property_live[property] = false;
        }
    }

    fn alloc_kind_payload(&mut self, kind: ObjectKind) -> (ObjectKindTag, u32) {
        match kind {
            ObjectKind::Ordinary => (ObjectKindTag::Ordinary, 0),
            ObjectKind::HostFunction(function) => {
                let slot = self.host_function_live.len() as u32;
                self.host_function_live.push(true);
                self.host_function_name.push(function.name);
                self.host_function_kind.push(function.kind);
                (ObjectKindTag::HostFunction, slot)
            }
            ObjectKind::JsFunction(function) => {
                let slot = self.js_function_live.len() as u32;
                self.js_function_live.push(true);
                self.js_function_name.push(function.name);
                self.js_function_param_range_start
                    .push(self.js_function_param_symbol.len() as u32);
                self.js_function_param_range_len
                    .push(function.params.len() as u32);
                self.js_function_param_symbol.extend(function.params);
                self.js_function_body.push(function.body);
                self.js_function_captured_env.push(function.captured_env);
                (ObjectKindTag::JsFunction, slot)
            }
            ObjectKind::ModuleNamespace(namespace) => {
                let slot = self.module_namespace_live.len() as u32;
                self.module_namespace_live.push(true);
                self.module_namespace_module.push(namespace.module);
                self.module_namespace_export_range_start
                    .push(self.module_namespace_export_symbol.len() as u32);
                self.module_namespace_export_range_len
                    .push(namespace.exports.len() as u32);
                for (symbol, cell) in namespace.exports {
                    self.module_namespace_export_symbol.push(symbol);
                    self.module_namespace_export_cell.push(cell);
                }
                (ObjectKindTag::ModuleNamespace, slot)
            }
        }
    }

    fn object_kind_at(&self, object_index: usize) -> Result<ObjectKind, Error> {
        let payload = self.heap_object_kind_payload[object_index] as usize;
        match self.heap_object_kind[object_index] {
            ObjectKindTag::Ordinary => Ok(ObjectKind::Ordinary),
            ObjectKindTag::HostFunction => {
                if !self
                    .host_function_live
                    .get(payload)
                    .copied()
                    .unwrap_or(false)
                {
                    return Err(Error::JsObjectNotFound {
                        object: object_index as u64,
                    });
                }

                Ok(ObjectKind::HostFunction(HostFunction {
                    name: self.host_function_name[payload],
                    kind: self.host_function_kind[payload],
                }))
            }
            ObjectKindTag::JsFunction => {
                if !self.js_function_live.get(payload).copied().unwrap_or(false) {
                    return Err(Error::JsObjectNotFound {
                        object: object_index as u64,
                    });
                }

                let start = self.js_function_param_range_start[payload] as usize;
                let len = self.js_function_param_range_len[payload] as usize;

                Ok(ObjectKind::JsFunction(Box::new(JsFunction {
                    name: self.js_function_name[payload],
                    params: self.js_function_param_symbol[start..start + len].to_vec(),
                    body: self.js_function_body[payload].clone(),
                    captured_env: self.js_function_captured_env[payload],
                })))
            }
            ObjectKindTag::ModuleNamespace => {
                if !self
                    .module_namespace_live
                    .get(payload)
                    .copied()
                    .unwrap_or(false)
                {
                    return Err(Error::JsObjectNotFound {
                        object: object_index as u64,
                    });
                }

                let start = self.module_namespace_export_range_start[payload] as usize;
                let len = self.module_namespace_export_range_len[payload] as usize;
                let mut exports = Vec::with_capacity(len);

                for index in start..start + len {
                    exports.push((
                        self.module_namespace_export_symbol[index],
                        self.module_namespace_export_cell[index],
                    ));
                }

                Ok(ObjectKind::ModuleNamespace(ModuleNamespace {
                    module: self.module_namespace_module[payload],
                    exports,
                }))
            }
        }
    }

    fn push_object_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        value: JsValue,
    ) -> Result<(), Error> {
        let index = self.live_object_index(object)?;
        let expected_start = self.heap_property_live.len() as u32;
        let range_end = self.heap_object_property_range_start[index]
            + self.heap_object_property_range_len[index];

        if range_end != expected_start {
            return self.repack_and_push_object_property(object, key, value);
        }

        self.heap_property_live.push(true);
        let PropertyKey::Symbol(symbol) = key;
        let property_index = self.heap_property_live.len() - 1;
        self.heap_property_key_symbol.push(symbol);
        self.heap_property_value.push(value);
        self.property_index
            .insert((object.slot, symbol), property_index);
        self.heap_object_property_range_len[index] += 1;
        self.heap_object_bytes[index] = self.heap_object_bytes[index].saturating_add(16);
        self.allocated_bytes = self.allocated_bytes.saturating_add(16);
        Ok(())
    }

    fn repack_and_push_object_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        value: JsValue,
    ) -> Result<(), Error> {
        let properties = self
            .property_indices(object)?
            .into_iter()
            .filter(|index| self.heap_property_live[*index])
            .map(|index| {
                (
                    PropertyKey::Symbol(self.property_key_symbol(index)),
                    self.heap_property_value.get(index as u32),
                )
            })
            .collect::<Vec<_>>();
        let object_index = object.slot as usize;

        for index in self.property_indices(object)? {
            self.heap_property_live[index] = false;
        }

        self.heap_object_property_range_start[object_index] = self.heap_property_live.len() as u32;
        self.heap_object_property_range_len[object_index] = 0;

        for (key, value) in properties {
            self.push_object_property(object, key, value)?;
        }

        self.push_object_property(object, key, value)
    }

    fn find_property_index(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
    ) -> Result<Option<usize>, Error> {
        let PropertyKey::Symbol(symbol) = key;
        if let Some(index) = self.property_index.get(&(object.slot, symbol)).copied()
            && self.property_index_entry_valid(object, index)
        {
            return Ok(Some(index));
        }

        for index in self.property_indices(object)? {
            if self.heap_property_live[index] && self.property_key_symbol(index) == symbol {
                self.property_index.insert((object.slot, symbol), index);
                return Ok(Some(index));
            }
        }

        Ok(None)
    }

    fn child_objects(&self, object: ObjectId) -> Result<Vec<ObjectId>, Error> {
        let mut objects = Vec::new();

        for value in self.child_values(object)? {
            if let JsValue::Object(object) = value {
                objects.push(object);
            }
        }

        Ok(objects)
    }

    fn child_values(&self, object: ObjectId) -> Result<Vec<JsValue>, Error> {
        let mut values = Vec::new();

        for index in self.property_indices(object)? {
            if self.heap_property_live[index] {
                values.push(self.heap_property_value.get(index as u32));
            }
        }

        Ok(values)
    }

    fn append_object_runtime_edges(
        &self,
        object: ObjectId,
        edges: &mut RuntimeEdges,
    ) -> Result<(), Error> {
        for child in self.child_objects(object)? {
            edges.objects.push(child);
        }

        let object_index = self.live_object_index(object)?;
        if self.heap_object_kind[object_index] == ObjectKindTag::JsFunction {
            let payload = self.heap_object_kind_payload[object_index] as usize;
            edges.frames.push(self.js_function_captured_env[payload]);
        }

        Ok(())
    }

    fn property_indices(&self, object: ObjectId) -> Result<Vec<usize>, Error> {
        let index = self.live_object_index(object)?;
        let start = self.heap_object_property_range_start[index] as usize;
        let len = self.heap_object_property_range_len[index] as usize;
        Ok((start..start + len).collect())
    }

    fn property_key_symbol(&self, index: usize) -> Symbol {
        *self
            .heap_property_key_symbol
            .get(index)
            .expect("heap property key column out of sync")
    }

    fn property_index_entry_valid(&self, object: ObjectId, property: usize) -> bool {
        if !self
            .heap_property_live
            .get(property)
            .copied()
            .unwrap_or(false)
        {
            return false;
        }

        let Ok(object_index) = self.live_object_index(object) else {
            return false;
        };
        let start = self.heap_object_property_range_start[object_index] as usize;
        let len = self.heap_object_property_range_len[object_index] as usize;

        (start..start + len).contains(&property)
    }

    fn live_object_index(&self, object: ObjectId) -> Result<usize, Error> {
        if object.segment != self.segment
            || !self
                .heap_object_live
                .get(object.slot as usize)
                .copied()
                .unwrap_or(false)
        {
            return Err(Error::JsObjectNotFound {
                object: object.debug_index(),
            });
        }

        Ok(object.slot as usize)
    }
}

impl Default for JsHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum ObjectKind {
    Ordinary,
    HostFunction(HostFunction),
    JsFunction(Box<JsFunction>),
    ModuleNamespace(ModuleNamespace),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct HostFunction {
    pub name: Symbol,
    pub kind: HostFunctionKind,
}

impl HostFunction {
    pub(crate) fn call(
        &self,
        args: &[JsValue],
        host: &JsHostBindings,
        _heap: &mut JsHeap,
        _symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        match self.kind {
            HostFunctionKind::ConsoleLog => {
                host.emit_console(JsStreamKind::Stdout, args)?;
                Ok(JsValue::Undefined)
            }
            HostFunctionKind::ConsoleError => {
                host.emit_console(JsStreamKind::Stderr, args)?;
                Ok(JsValue::Undefined)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum HostFunctionKind {
    ConsoleLog,
    ConsoleError,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JsFunction {
    pub name: Option<Symbol>,
    pub params: Vec<Symbol>,
    pub body: BytecodeProgram,
    pub captured_env: EnvFrameId,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ModuleNamespace {
    pub module: ModuleId,
    pub exports: Vec<(Symbol, BindingCellId)>,
}

pub(crate) struct RuntimeEdges {
    pub(crate) objects: Vec<ObjectId>,
    pub(crate) frames: Vec<EnvFrameId>,
}

fn estimate_object_bytes(kind: ObjectKindTag) -> usize {
    mem::size_of::<GcHeader>() + mem::size_of_val(&kind)
}
