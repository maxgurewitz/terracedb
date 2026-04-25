use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{JsHeap, JsValue, Symbol, SymbolTable};

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvStack {
    root: EnvFrameId,
    current: EnvFrameId,
    frames: Vec<Option<EnvFrame>>,
    cells: Vec<Option<Binding>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvFrame {
    parent: Option<EnvFrameId>,
    bindings: HashMap<Symbol, BindingCellId>,
    depth: usize,
    active: bool,
    capture_count: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Binding {
    kind: BindingKind,
    value: JsValue,
    initialized: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BindingCellId(pub u32);

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct EnvFrameId(pub u32);

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum BindingKind {
    Let,
    Const,
}

impl EnvFrame {
    fn new(parent: Option<EnvFrameId>, depth: usize) -> Self {
        Self {
            parent,
            bindings: HashMap::new(),
            depth,
            active: true,
            capture_count: 0,
        }
    }
}

impl EnvStack {
    pub(crate) fn new() -> Self {
        let root = EnvFrameId(0);

        Self {
            root,
            current: root,
            frames: vec![Some(EnvFrame::new(None, 1))],
            cells: Vec::new(),
        }
    }

    pub(crate) fn capture_current_frame(&mut self) -> Result<EnvFrameId, Error> {
        let mut current = Some(self.current);

        while let Some(frame_id) = current {
            let frame = self.frame_mut(frame_id)?;
            frame.capture_count += 1;
            current = frame.parent;
        }

        Ok(self.current)
    }

    pub(crate) fn set_current_frame(&mut self, frame: EnvFrameId) -> Result<EnvFrameId, Error> {
        self.frame(frame)?;

        let previous = self.current;
        self.current = frame;

        Ok(previous)
    }

    pub(crate) fn restore_current_frame(&mut self, frame: EnvFrameId) -> Result<(), Error> {
        self.frame(frame)?;
        self.current = frame;

        Ok(())
    }

    pub(crate) fn push_scope(&mut self) {
        let depth = self
            .frame(self.current)
            .expect("current frame must exist")
            .depth
            + 1;
        let id = EnvFrameId(self.frames.len() as u32);

        self.frames
            .push(Some(EnvFrame::new(Some(self.current), depth)));
        self.current = id;
    }

    pub(crate) fn pop_scope(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        if self.current == self.root {
            return Err(Error::JsCannotPopRootScope);
        }

        let frame_id = self.current;
        let parent = self
            .frame(frame_id)?
            .parent
            .expect("non-root frame has parent");

        self.current = parent;
        self.deactivate_frame(frame_id, heap)
    }

    pub(crate) fn depth(&self) -> usize {
        self.frame(self.current)
            .expect("current frame must exist")
            .depth
    }

    pub(crate) fn truncate_to_depth(
        &mut self,
        depth: usize,
        heap: &mut JsHeap,
    ) -> Result<(), Error> {
        while self.depth() > depth && self.current != self.root {
            self.pop_scope(heap)?;
        }

        Ok(())
    }

    pub(crate) fn declare_current(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        if self.frame(self.current)?.bindings.contains_key(&name) {
            return Err(Error::JsDuplicateBinding {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        let cell = self.alloc_cell(Binding {
            kind,
            value: JsValue::Undefined,
            initialized: false,
        });

        self.frame_mut(self.current)?.bindings.insert(name, cell);

        Ok(())
    }

    pub fn declare_current_value(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.declare_current(name, kind, symbols)?;
        self.store(name, value, heap, symbols)
    }

    pub(crate) fn lookup(
        &self,
        name: Symbol,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        let cell = self
            .find_cell(name)
            .ok_or_else(|| Error::JsBindingNotFound {
                name: symbols.resolve_expect(name).to_owned(),
            })?;
        let value = self.cell(cell)?.value.clone();

        heap.dup_value(&value);

        Ok(value)
    }

    pub(crate) fn store(
        &mut self,
        name: Symbol,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        let cell = self
            .find_cell(name)
            .ok_or_else(|| Error::JsBindingNotFound {
                name: symbols.resolve_expect(name).to_owned(),
            })?;

        {
            let binding = self.cell(cell)?;

            if binding.kind == BindingKind::Const && binding.initialized {
                return Err(Error::JsAssignToConst {
                    name: symbols.resolve_expect(name).to_owned(),
                });
            }
        }

        heap.dup_value(&value);
        let old = {
            let binding = self.cell_mut(cell)?;
            let old = std::mem::replace(&mut binding.value, value);
            binding.initialized = true;
            old
        };

        heap.free_value(old)
    }

    pub(crate) fn release_all_scopes(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        for cell in &mut self.cells {
            if let Some(binding) = cell.take() {
                heap.free_value(binding.value)?;
            }
        }

        self.frames.clear();

        Ok(())
    }

    pub(crate) fn active_frame_roots(&self) -> Vec<EnvFrameId> {
        self.frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| {
                frame
                    .as_ref()
                    .filter(|frame| frame.active)
                    .map(|_| EnvFrameId(index as u32))
            })
            .collect()
    }

    pub(crate) fn frame_edges(&self, frame_id: EnvFrameId) -> Result<EnvFrameEdges, Error> {
        let frame = self.frame(frame_id)?;
        let mut edges = EnvFrameEdges {
            frames: Vec::new(),
            objects: Vec::new(),
        };

        if let Some(parent) = frame.parent {
            edges.frames.push(parent);
        }

        for cell in frame.bindings.values() {
            if let JsValue::Object(object) = &self.cell(*cell)?.value {
                edges.objects.push(*object);
            }
        }

        Ok(edges)
    }

    pub(crate) fn release_unmarked_inactive_frames(
        &mut self,
        marked: &HashSet<EnvFrameId>,
        heap: &mut JsHeap,
    ) -> Result<usize, Error> {
        let frames = self
            .frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| {
                frame
                    .as_ref()
                    .filter(|frame| !frame.active)
                    .map(|_| EnvFrameId(index as u32))
            })
            .collect::<Vec<_>>();
        let mut released = 0;

        for frame in frames {
            if marked.contains(&frame) {
                continue;
            }

            self.release_frame(frame, heap)?;
            released += 1;
        }

        Ok(released)
    }

    fn deactivate_frame(&mut self, frame_id: EnvFrameId, heap: &mut JsHeap) -> Result<(), Error> {
        let should_release = {
            let frame = self.frame_mut(frame_id)?;
            frame.active = false;
            frame.capture_count == 0
        };

        if should_release {
            self.release_frame(frame_id, heap)?;
        }

        Ok(())
    }

    fn release_frame(&mut self, frame_id: EnvFrameId, heap: &mut JsHeap) -> Result<(), Error> {
        let Some(frame) = self.frame_slot_mut(frame_id)?.take() else {
            return Ok(());
        };

        for cell in frame.bindings.into_values() {
            if let Some(binding) = self.cell_slot_mut(cell)?.take() {
                heap.free_value(binding.value)?;
            }
        }

        Ok(())
    }

    fn find_cell(&self, name: Symbol) -> Option<BindingCellId> {
        let mut current = Some(self.current);

        while let Some(frame_id) = current {
            let frame = self.frame(frame_id).ok()?;

            if let Some(cell) = frame.bindings.get(&name) {
                return Some(*cell);
            }

            current = frame.parent;
        }

        None
    }

    fn alloc_cell(&mut self, binding: Binding) -> BindingCellId {
        let id = BindingCellId(self.cells.len() as u32);
        self.cells.push(Some(binding));
        id
    }

    fn frame(&self, id: EnvFrameId) -> Result<&EnvFrame, Error> {
        self.frames
            .get(id.0 as usize)
            .and_then(Option::as_ref)
            .ok_or(Error::JsEnvFrameNotFound { frame: id.0 })
    }

    fn frame_mut(&mut self, id: EnvFrameId) -> Result<&mut EnvFrame, Error> {
        self.frames
            .get_mut(id.0 as usize)
            .and_then(Option::as_mut)
            .ok_or(Error::JsEnvFrameNotFound { frame: id.0 })
    }

    fn frame_slot_mut(&mut self, id: EnvFrameId) -> Result<&mut Option<EnvFrame>, Error> {
        self.frames
            .get_mut(id.0 as usize)
            .ok_or(Error::JsEnvFrameNotFound { frame: id.0 })
    }

    fn cell(&self, id: BindingCellId) -> Result<&Binding, Error> {
        self.cells
            .get(id.0 as usize)
            .and_then(Option::as_ref)
            .ok_or(Error::JsBindingCellNotFound { cell: id.0 })
    }

    fn cell_mut(&mut self, id: BindingCellId) -> Result<&mut Binding, Error> {
        self.cells
            .get_mut(id.0 as usize)
            .and_then(Option::as_mut)
            .ok_or(Error::JsBindingCellNotFound { cell: id.0 })
    }

    fn cell_slot_mut(&mut self, id: BindingCellId) -> Result<&mut Option<Binding>, Error> {
        self.cells
            .get_mut(id.0 as usize)
            .ok_or(Error::JsBindingCellNotFound { cell: id.0 })
    }
}

pub(crate) struct EnvFrameEdges {
    pub(crate) frames: Vec<EnvFrameId>,
    pub(crate) objects: Vec<super::ObjectId>,
}
