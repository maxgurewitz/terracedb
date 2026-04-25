use std::{
    collections::{HashMap, HashSet},
    mem,
};

use bytes::Bytes;

use crate::Error;

use super::{
    JsOutputChunk, JsOutputSender, JsRuntimeId, JsStreamKind, JsValue, Symbol, SymbolTable,
};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct ObjectId(pub u64);

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum PropertyKey {
    Symbol(Symbol),
}

pub const DEFAULT_GC_THRESHOLD_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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

#[derive(Debug, Clone)]
pub struct JsHeap {
    next_object_id: u64,
    objects: HashMap<ObjectId, HeapObject>,
    gc_objects: Vec<ObjectId>,
    zero_ref: Vec<ObjectId>,
    tmp_cycle: Vec<ObjectId>,
    allocated_objects: usize,
    allocated_bytes: usize,
    freed_objects: usize,
    gc_runs: u64,
    gc_policy: GcPolicy,
}

#[derive(Debug, Clone)]
pub struct HeapObject {
    header: GcHeader,
    object: JsObject,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct GcHeader {
    pub ref_count: u32,
    pub mark: GcMark,
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum GcMark {
    None,
    Tmp,
    Live,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct HeapStats {
    pub allocated_objects: usize,
    pub allocated_bytes: usize,
    pub freed_objects: usize,
    pub gc_runs: u64,
}

impl JsHeap {
    pub fn new() -> Self {
        Self::with_gc_policy(GcPolicy::default())
    }

    pub fn with_gc_policy(gc_policy: GcPolicy) -> Self {
        Self {
            next_object_id: 0,
            objects: HashMap::new(),
            gc_objects: Vec::new(),
            zero_ref: Vec::new(),
            tmp_cycle: Vec::new(),
            allocated_objects: 0,
            allocated_bytes: 0,
            freed_objects: 0,
            gc_runs: 0,
            gc_policy,
        }
    }

    pub fn alloc_object(&mut self, kind: ObjectKind) -> ObjectId {
        self.alloc_js_object(JsObject::new(kind))
    }

    pub fn alloc_js_object(&mut self, object: JsObject) -> ObjectId {
        let id = ObjectId(self.next_object_id);
        self.next_object_id += 1;

        let bytes = estimate_object_bytes(&object);

        self.objects.insert(
            id,
            HeapObject {
                header: GcHeader {
                    ref_count: 0,
                    mark: GcMark::None,
                    bytes,
                },
                object,
            },
        );
        self.gc_objects.push(id);
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
        let object = self
            .objects
            .get_mut(&id)
            .expect("dup_object for missing object");
        object.header.ref_count += 1;
    }

    pub fn free_object_ref(&mut self, id: ObjectId) -> Result<(), Error> {
        let object = self
            .objects
            .get_mut(&id)
            .ok_or(Error::JsObjectNotFound { object: id.0 })?;

        if object.header.ref_count == 0 {
            return Err(Error::JsRefCountUnderflow { object: id.0 });
        }

        object.header.ref_count -= 1;

        if object.header.ref_count == 0 {
            self.zero_ref.push(id);
            self.drain_zero_ref()?;
        }

        Ok(())
    }

    pub fn get_property(&mut self, object: ObjectId, key: PropertyKey) -> Result<JsValue, Error> {
        let value = self
            .object(object)?
            .object
            .get_property(key)
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
        if !self.objects.contains_key(&object) {
            return Err(Error::JsObjectNotFound { object: object.0 });
        }

        self.dup_value(&value);

        let old = self.object_mut(object)?.set_property(key, value);

        if let Some(old) = old {
            self.free_value(old.value)?;
        }

        Ok(())
    }

    pub fn object_kind(&self, object: ObjectId) -> Result<ObjectKind, Error> {
        Ok(self.object(object)?.object.kind().clone())
    }

    pub fn stats(&self) -> HeapStats {
        HeapStats {
            allocated_objects: self.allocated_objects,
            allocated_bytes: self.allocated_bytes,
            freed_objects: self.freed_objects,
            gc_runs: self.gc_runs,
        }
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

    pub fn force_gc(&mut self) -> Result<usize, Error> {
        self.run_gc()
    }

    pub fn run_gc(&mut self) -> Result<usize, Error> {
        self.gc_runs += 1;
        self.drain_zero_ref()?;
        self.reset_gc_marks();
        self.gc_decref_all()?;
        self.gc_scan_live()?;
        let freed = self.gc_free_cycles()?;
        self.drain_zero_ref()?;

        Ok(freed)
    }

    pub(crate) fn drain_zero_ref(&mut self) -> Result<(), Error> {
        while let Some(id) = self.zero_ref.pop() {
            let should_free = self
                .objects
                .get(&id)
                .is_some_and(|object| object.header.ref_count == 0);

            if should_free {
                self.free_object_now(id)?;
            }
        }

        Ok(())
    }

    fn free_object_now(&mut self, id: ObjectId) -> Result<(), Error> {
        let heap_object = self
            .objects
            .remove(&id)
            .ok_or(Error::JsObjectNotFound { object: id.0 })?;

        self.account_freed_object(id, heap_object.header.bytes);
        heap_object.object.release_children(self)
    }

    fn gc_decref_all(&mut self) -> Result<(), Error> {
        self.tmp_cycle.clear();

        for id in self.gc_objects.clone() {
            if self.objects.contains_key(&id) {
                self.gc_decref_children(id)?;
            }
        }

        Ok(())
    }

    fn gc_decref_children(&mut self, id: ObjectId) -> Result<(), Error> {
        let mut children = Vec::new();

        self.object(id)?.object.visit_children(&mut |child| {
            children.push(child);
        });

        for child in children {
            let child_object = self
                .objects
                .get_mut(&child)
                .ok_or(Error::JsObjectNotFound { object: child.0 })?;

            if child_object.header.ref_count == 0 {
                return Err(Error::JsRefCountUnderflow { object: child.0 });
            }

            child_object.header.ref_count -= 1;

            if child_object.header.ref_count == 0 && child_object.header.mark != GcMark::Tmp {
                child_object.header.mark = GcMark::Tmp;
                self.tmp_cycle.push(child);
            }
        }

        Ok(())
    }

    fn gc_scan_live(&mut self) -> Result<(), Error> {
        for id in self.gc_objects.clone() {
            let is_live_root = self
                .objects
                .get(&id)
                .is_some_and(|object| object.header.ref_count > 0);

            if is_live_root {
                self.gc_scan_object(id)?;
            }
        }

        Ok(())
    }

    fn gc_scan_object(&mut self, id: ObjectId) -> Result<(), Error> {
        let already_live = {
            let object = self
                .objects
                .get_mut(&id)
                .ok_or(Error::JsObjectNotFound { object: id.0 })?;

            if object.header.mark == GcMark::Live {
                true
            } else {
                object.header.mark = GcMark::Live;
                false
            }
        };

        if already_live {
            return Ok(());
        }

        let mut children = Vec::new();

        self.object(id)?.object.visit_children(&mut |child| {
            children.push(child);
        });

        for child in children {
            if let Some(child_object) = self.objects.get_mut(&child) {
                child_object.header.ref_count += 1;
            }

            self.gc_scan_object(child)?;
        }

        Ok(())
    }

    fn gc_free_cycles(&mut self) -> Result<usize, Error> {
        let candidates = mem::take(&mut self.tmp_cycle);
        let collected = candidates
            .iter()
            .copied()
            .filter(|id| {
                self.objects
                    .get(id)
                    .is_some_and(|object| object.header.mark != GcMark::Live)
            })
            .collect::<HashSet<_>>();
        let mut freed = 0;

        for id in candidates {
            if collected.contains(&id) && self.objects.contains_key(&id) {
                self.free_cycle_object_now(id, &collected)?;
                freed += 1;
            }
        }

        self.reset_gc_marks();

        Ok(freed)
    }

    fn free_cycle_object_now(
        &mut self,
        id: ObjectId,
        collected: &HashSet<ObjectId>,
    ) -> Result<(), Error> {
        let heap_object = self
            .objects
            .remove(&id)
            .ok_or(Error::JsObjectNotFound { object: id.0 })?;

        self.account_freed_object(id, heap_object.header.bytes);
        heap_object
            .object
            .release_children_for_cycle_free(self, collected)
    }

    fn reset_gc_marks(&mut self) {
        for object in self.objects.values_mut() {
            object.header.mark = GcMark::None;
        }
    }

    fn account_freed_object(&mut self, id: ObjectId, bytes: usize) {
        self.allocated_objects = self.allocated_objects.saturating_sub(1);
        self.freed_objects += 1;
        self.allocated_bytes = self.allocated_bytes.saturating_sub(bytes);
        self.gc_objects.retain(|existing| *existing != id);
        self.zero_ref.retain(|existing| *existing != id);
    }

    fn object(&self, object: ObjectId) -> Result<&HeapObject, Error> {
        self.objects
            .get(&object)
            .ok_or(Error::JsObjectNotFound { object: object.0 })
    }

    fn object_mut(&mut self, object: ObjectId) -> Result<&mut JsObject, Error> {
        self.objects
            .get_mut(&object)
            .map(|object| &mut object.object)
            .ok_or(Error::JsObjectNotFound { object: object.0 })
    }
}

impl Default for JsHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct JsObject {
    kind: ObjectKind,
    properties: HashMap<PropertyKey, JsProperty>,
}

impl JsObject {
    pub fn new(kind: ObjectKind) -> Self {
        Self {
            kind,
            properties: HashMap::new(),
        }
    }

    pub fn kind(&self) -> &ObjectKind {
        &self.kind
    }

    pub fn get_property(&self, key: PropertyKey) -> Option<JsValue> {
        self.properties.get(&key).map(JsProperty::value)
    }

    pub fn set_property(&mut self, key: PropertyKey, value: JsValue) -> Option<JsProperty> {
        self.properties.insert(key, JsProperty::new(value))
    }

    pub fn visit_children(&self, visitor: &mut impl FnMut(ObjectId)) {
        for property in self.properties.values() {
            if let JsValue::Object(id) = property.value {
                visitor(id);
            }
        }

        self.kind.visit_children(visitor);
    }

    pub fn release_children(self, heap: &mut JsHeap) -> Result<(), Error> {
        for property in self.properties.into_values() {
            heap.free_value(property.value)?;
        }

        self.kind.release_children(heap)
    }

    fn release_children_for_cycle_free(
        self,
        heap: &mut JsHeap,
        collected: &HashSet<ObjectId>,
    ) -> Result<(), Error> {
        for property in self.properties.into_values() {
            release_cycle_child_value(property.value, heap, collected)?;
        }

        self.kind.release_children_for_cycle_free(heap, collected)
    }
}

#[derive(Debug, Clone)]
pub enum ObjectKind {
    Ordinary,
    HostFunction(HostFunction),
}

impl ObjectKind {
    pub fn visit_children(&self, visitor: &mut impl FnMut(ObjectId)) {
        match self {
            Self::Ordinary => {}
            Self::HostFunction(host_function) => host_function.visit_children(visitor),
        }
    }

    pub fn release_children(self, heap: &mut JsHeap) -> Result<(), Error> {
        match self {
            Self::Ordinary => Ok(()),
            Self::HostFunction(host_function) => host_function.release_children(heap),
        }
    }

    fn release_children_for_cycle_free(
        self,
        heap: &mut JsHeap,
        collected: &HashSet<ObjectId>,
    ) -> Result<(), Error> {
        match self {
            Self::Ordinary => Ok(()),
            Self::HostFunction(host_function) => {
                host_function.release_children_for_cycle_free(heap, collected)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsProperty {
    value: JsValue,
}

impl JsProperty {
    pub fn new(value: JsValue) -> Self {
        Self { value }
    }

    pub fn value(&self) -> JsValue {
        self.value.clone()
    }
}

#[derive(Debug, Clone)]
pub struct HostFunction {
    pub name: Symbol,
    pub kind: HostFunctionKind,
}

impl HostFunction {
    pub fn call(
        &self,
        args: &[JsValue],
        _heap: &mut JsHeap,
        _symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        match &self.kind {
            HostFunctionKind::ConsoleLog {
                runtime_id,
                output_tx,
            } => {
                emit_console(*runtime_id, JsStreamKind::Stdout, output_tx, args)?;
                Ok(JsValue::Undefined)
            }
            HostFunctionKind::ConsoleError {
                runtime_id,
                output_tx,
            } => {
                emit_console(*runtime_id, JsStreamKind::Stderr, output_tx, args)?;
                Ok(JsValue::Undefined)
            }
        }
    }

    pub fn visit_children(&self, _visitor: &mut impl FnMut(ObjectId)) {}

    pub fn release_children(self, _heap: &mut JsHeap) -> Result<(), Error> {
        Ok(())
    }

    fn release_children_for_cycle_free(
        self,
        _heap: &mut JsHeap,
        _collected: &HashSet<ObjectId>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum HostFunctionKind {
    ConsoleLog {
        runtime_id: JsRuntimeId,
        output_tx: JsOutputSender,
    },
    ConsoleError {
        runtime_id: JsRuntimeId,
        output_tx: JsOutputSender,
    },
}

fn emit_console(
    runtime_id: JsRuntimeId,
    stream: JsStreamKind,
    output_tx: &JsOutputSender,
    args: &[JsValue],
) -> Result<(), Error> {
    let mut line = args
        .iter()
        .map(JsValue::stringify)
        .collect::<Vec<_>>()
        .join(" ");
    line.push('\n');

    output_tx
        .send(JsOutputChunk {
            runtime_id,
            stream,
            bytes: Bytes::from(line),
        })
        .map_err(|_| Error::OutputReceiverDropped)
}

fn release_cycle_child_value(
    value: JsValue,
    _heap: &mut JsHeap,
    _collected: &HashSet<ObjectId>,
) -> Result<(), Error> {
    // Cycle collection has already accounted for all child object edges during
    // the temporary decref pass. Releasing them again would underflow internal
    // cycle edges and would double-decrement edges into live objects.
    let _ = value;
    Ok(())
}

fn estimate_object_bytes(object: &JsObject) -> usize {
    mem::size_of::<HeapObject>()
        + mem::size_of_val(object)
        + object.properties.len() * mem::size_of::<(PropertyKey, JsProperty)>()
}
