use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::Error;

use super::{JsHeap, JsValue, SegmentId, StorageColumn, Symbol, SymbolTable, ValueCols};

#[derive(Debug, Deserialize, Serialize)]
pub struct EnvStack {
    root: EnvFrameId,
    current: EnvFrameId,
    binding_segment: SegmentId,
    frame_segment: SegmentId,

    env_frame_live: Vec<bool>,
    env_frame_parent: Vec<Option<EnvFrameId>>,
    env_frame_depth: Vec<usize>,
    env_frame_active: Vec<bool>,
    env_frame_capture_count: Vec<usize>,
    env_frame_binding_range_start: Vec<u32>,
    env_frame_binding_range_len: Vec<u32>,

    env_frame_binding_live: Vec<bool>,
    env_frame_binding_symbol: StorageColumn<Symbol>,
    env_frame_binding_cell: Vec<BindingCellId>,

    #[serde(skip, default)]
    binding_index: HashMap<(u32, Symbol), BindingCellId>,

    env_cell_live: Vec<bool>,
    env_cell_owner_frame: Vec<EnvFrameId>,
    env_cell_kind: Vec<BindingKind>,
    env_cell_initialized: Vec<bool>,
    env_cell_value: ValueCols,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct BindingCellId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct EnvFrameId {
    pub segment: SegmentId,
    pub slot: u32,
}

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum BindingKind {
    Let,
    Const,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BindingSnapshot {
    pub kind: BindingKind,
    pub value: JsValue,
    pub initialized: bool,
}

impl EnvStack {
    pub(crate) fn with_segments(binding_segment: SegmentId, frame_segment: SegmentId) -> Self {
        let root = EnvFrameId {
            segment: frame_segment,
            slot: 0,
        };
        let mut stack = Self {
            root,
            current: root,
            binding_segment,
            frame_segment,
            env_frame_live: Vec::new(),
            env_frame_parent: Vec::new(),
            env_frame_depth: Vec::new(),
            env_frame_active: Vec::new(),
            env_frame_capture_count: Vec::new(),
            env_frame_binding_range_start: Vec::new(),
            env_frame_binding_range_len: Vec::new(),
            env_frame_binding_live: Vec::new(),
            env_frame_binding_symbol: StorageColumn::new(),
            env_frame_binding_cell: Vec::new(),
            binding_index: HashMap::new(),
            env_cell_live: Vec::new(),
            env_cell_owner_frame: Vec::new(),
            env_cell_kind: Vec::new(),
            env_cell_initialized: Vec::new(),
            env_cell_value: ValueCols::new(),
        };

        stack.push_frame_row(None, 1, true);
        stack
    }

    pub(crate) fn capture_current_frame(&mut self) -> Result<EnvFrameId, Error> {
        let mut current = Some(self.current);

        while let Some(frame_id) = current {
            self.validate_frame_id(frame_id)?;
            self.env_frame_capture_count[frame_id.slot as usize] += 1;
            current = self.env_frame_parent[frame_id.slot as usize];
        }

        Ok(self.current)
    }

    pub(crate) fn root(&self) -> EnvFrameId {
        self.root
    }

    pub(crate) fn live_frame_count(&self) -> usize {
        self.env_frame_live.iter().filter(|live| **live).count()
    }

    pub(crate) fn live_cell_count(&self) -> usize {
        self.env_cell_live.iter().filter(|live| **live).count()
    }

    #[cfg(test)]
    pub(crate) fn storage_trace(&self) -> super::StorageAccessTrace {
        super::StorageAccessTrace {
            env_frame_binding_symbol: self.env_frame_binding_symbol.trace(),
            ..super::StorageAccessTrace::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn reset_storage_trace(&self) {
        self.env_frame_binding_symbol.reset_trace();
    }

    pub(crate) fn set_current_frame(&mut self, frame: EnvFrameId) -> Result<EnvFrameId, Error> {
        self.validate_frame_id(frame)?;
        let previous = self.current;
        self.current = frame;
        Ok(previous)
    }

    pub(crate) fn create_module_frame(&mut self) -> EnvFrameId {
        let depth = self.env_frame_depth[self.root.slot as usize] + 1;
        self.push_frame_row(Some(self.root), depth, true)
    }

    pub(crate) fn restore_current_frame(&mut self, frame: EnvFrameId) -> Result<(), Error> {
        self.validate_frame_id(frame)?;
        self.current = frame;
        Ok(())
    }

    pub(crate) fn push_scope(&mut self) {
        let depth = self.env_frame_depth[self.current.slot as usize] + 1;
        self.current = self.push_frame_row(Some(self.current), depth, true);
    }

    pub(crate) fn pop_scope(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        if self.current == self.root {
            return Err(Error::JsCannotPopRootScope);
        }

        let frame_id = self.current;
        let parent =
            self.env_frame_parent[frame_id.slot as usize].expect("non-root frame has parent");

        self.current = parent;
        self.deactivate_frame(frame_id, heap)
    }

    pub(crate) fn depth(&self) -> usize {
        self.env_frame_depth[self.current.slot as usize]
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
        self.declare_in_frame(self.current, name, kind, symbols)
            .map(|_| ())
    }

    pub(crate) fn declare_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        kind: BindingKind,
        symbols: &SymbolTable,
    ) -> Result<BindingCellId, Error> {
        if self.binding_cell_in_frame(frame, name)?.is_some() {
            return Err(Error::JsDuplicateBinding {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        let cell = self.alloc_cell(frame, kind, JsValue::Undefined, false);
        self.push_frame_binding(frame, name, cell)?;

        Ok(cell)
    }

    pub fn declare_current_value(
        &mut self,
        name: Symbol,
        kind: BindingKind,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.declare_in_frame_value(self.current, name, kind, value, heap, symbols)
    }

    pub(crate) fn declare_in_frame_value(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        kind: BindingKind,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        let cell = self.declare_in_frame(frame, name, kind, symbols)?;
        self.store_cell(cell, name, value, heap, symbols)
    }

    pub(crate) fn alias_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        cell: BindingCellId,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.validate_cell_id(cell)?;

        if self.binding_cell_in_frame(frame, name)?.is_some() {
            return Err(Error::JsDuplicateBinding {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        self.push_frame_binding(frame, name, cell)
    }

    pub(crate) fn cell_for_name_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
        symbols: &SymbolTable,
    ) -> Result<BindingCellId, Error> {
        self.binding_cell_in_frame(frame, name)?
            .ok_or_else(|| Error::JsBindingNotFound {
                name: symbols.resolve_expect(name).to_owned(),
            })
    }

    pub(crate) fn lookup(
        &mut self,
        name: Symbol,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        let cell = self
            .find_cell(name)
            .ok_or_else(|| Error::JsBindingNotFound {
                name: symbols.resolve_expect(name).to_owned(),
            })?;

        self.load_cell(cell, name, heap, symbols)
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

        self.store_cell(cell, name, value, heap, symbols)
    }

    pub(crate) fn load_cell(
        &self,
        cell: BindingCellId,
        name: Symbol,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<JsValue, Error> {
        self.validate_cell_id(cell)?;
        let index = cell.slot as usize;

        if !self.env_cell_initialized[index] {
            return Err(Error::JsUninitializedBinding {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        let value = self.env_cell_value.get(cell.slot);
        heap.dup_value(&value);

        Ok(value)
    }

    pub(crate) fn store_cell(
        &mut self,
        cell: BindingCellId,
        name: Symbol,
        value: JsValue,
        heap: &mut JsHeap,
        symbols: &SymbolTable,
    ) -> Result<(), Error> {
        self.validate_cell_id(cell)?;
        let index = cell.slot as usize;

        if self.env_cell_kind[index] == BindingKind::Const && self.env_cell_initialized[index] {
            return Err(Error::JsAssignToConst {
                name: symbols.resolve_expect(name).to_owned(),
            });
        }

        heap.dup_value(&value);
        let old = self.env_cell_value.get(cell.slot);
        self.env_cell_value.set(cell.slot, value);
        self.env_cell_initialized[index] = true;
        heap.free_value(old)
    }

    pub(crate) fn release_all_scopes(&mut self, heap: &mut JsHeap) -> Result<(), Error> {
        for slot in 0..self.env_cell_live.len() {
            if !self.env_cell_live[slot] {
                continue;
            }

            self.env_cell_live[slot] = false;
            let value = self.env_cell_value.get(slot as u32);
            heap.free_value(value)?;
        }

        self.env_frame_live.fill(false);
        self.env_frame_binding_live.fill(false);
        self.binding_index.clear();
        Ok(())
    }

    pub(crate) fn active_frame_roots(&self) -> Vec<EnvFrameId> {
        self.env_frame_live
            .iter()
            .enumerate()
            .filter_map(|(index, live)| {
                if *live && self.env_frame_active[index] {
                    Some(EnvFrameId {
                        segment: self.frame_segment,
                        slot: index as u32,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn frame_edges(&self, frame_id: EnvFrameId) -> Result<EnvFrameEdges, Error> {
        self.validate_frame_id(frame_id)?;
        let frame_index = frame_id.slot as usize;
        let mut edges = EnvFrameEdges {
            frames: Vec::new(),
            objects: Vec::new(),
        };

        if let Some(parent) = self.env_frame_parent[frame_index] {
            edges.frames.push(parent);
        }

        for binding in self.binding_indices(frame_id)? {
            if !self.env_frame_binding_live[binding] {
                continue;
            }

            let cell = self.env_frame_binding_cell[binding];
            self.validate_cell_id(cell)?;
            if let JsValue::Object(object) = self.env_cell_value.get(cell.slot) {
                edges.objects.push(object);
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
            .env_frame_live
            .iter()
            .enumerate()
            .filter_map(|(index, live)| {
                if *live && !self.env_frame_active[index] {
                    Some(EnvFrameId {
                        segment: self.frame_segment,
                        slot: index as u32,
                    })
                } else {
                    None
                }
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

    fn push_frame_row(
        &mut self,
        parent: Option<EnvFrameId>,
        depth: usize,
        active: bool,
    ) -> EnvFrameId {
        let slot = self.env_frame_live.len() as u32;
        self.env_frame_live.push(true);
        self.env_frame_parent.push(parent);
        self.env_frame_depth.push(depth);
        self.env_frame_active.push(active);
        self.env_frame_capture_count.push(0);
        self.env_frame_binding_range_start
            .push(self.env_frame_binding_live.len() as u32);
        self.env_frame_binding_range_len.push(0);

        EnvFrameId {
            segment: self.frame_segment,
            slot,
        }
    }

    fn alloc_cell(
        &mut self,
        owner_frame: EnvFrameId,
        kind: BindingKind,
        value: JsValue,
        initialized: bool,
    ) -> BindingCellId {
        let slot = self.env_cell_live.len() as u32;
        self.env_cell_live.push(true);
        self.env_cell_owner_frame.push(owner_frame);
        self.env_cell_kind.push(kind);
        self.env_cell_initialized.push(initialized);
        self.env_cell_value.push(value);

        BindingCellId {
            segment: self.binding_segment,
            slot,
        }
    }

    fn push_frame_binding(
        &mut self,
        frame: EnvFrameId,
        symbol: Symbol,
        cell: BindingCellId,
    ) -> Result<(), Error> {
        self.validate_frame_id(frame)?;
        self.validate_cell_id(cell)?;
        let frame_index = frame.slot as usize;
        let expected_start = self.env_frame_binding_live.len() as u32;
        let range_end = self.env_frame_binding_range_start[frame_index]
            + self.env_frame_binding_range_len[frame_index];

        if range_end != expected_start {
            return self.repack_and_push_frame_binding(frame, symbol, cell);
        }

        self.env_frame_binding_live.push(true);
        self.env_frame_binding_symbol.push(symbol);
        self.env_frame_binding_cell.push(cell);
        self.binding_index.insert((frame.slot, symbol), cell);
        self.env_frame_binding_range_len[frame_index] += 1;
        Ok(())
    }

    fn repack_and_push_frame_binding(
        &mut self,
        frame: EnvFrameId,
        symbol: Symbol,
        cell: BindingCellId,
    ) -> Result<(), Error> {
        let existing = self
            .binding_indices(frame)?
            .into_iter()
            .filter(|index| self.env_frame_binding_live[*index])
            .map(|index| {
                (
                    self.binding_symbol(index),
                    self.env_frame_binding_cell[index],
                )
            })
            .collect::<Vec<_>>();
        let frame_index = frame.slot as usize;

        for index in self.binding_indices(frame)? {
            self.env_frame_binding_live[index] = false;
        }
        self.binding_index
            .retain(|(frame_slot, _), _| *frame_slot != frame.slot);

        self.env_frame_binding_range_start[frame_index] = self.env_frame_binding_live.len() as u32;
        self.env_frame_binding_range_len[frame_index] = 0;

        for (symbol, cell) in existing {
            self.push_frame_binding(frame, symbol, cell)?;
        }

        self.push_frame_binding(frame, symbol, cell)
    }

    fn deactivate_frame(&mut self, frame_id: EnvFrameId, heap: &mut JsHeap) -> Result<(), Error> {
        self.validate_frame_id(frame_id)?;
        let index = frame_id.slot as usize;
        self.env_frame_active[index] = false;

        if self.env_frame_capture_count[index] == 0 {
            self.release_frame(frame_id, heap)?;
        }

        Ok(())
    }

    fn release_frame(&mut self, frame_id: EnvFrameId, heap: &mut JsHeap) -> Result<(), Error> {
        self.validate_frame_id(frame_id)?;
        let frame_index = frame_id.slot as usize;

        if !self.env_frame_live[frame_index] {
            return Ok(());
        }

        let bindings = self.binding_indices(frame_id)?;
        self.env_frame_live[frame_index] = false;
        self.binding_index
            .retain(|(frame_slot, _), _| *frame_slot != frame_id.slot);
        for binding in bindings {
            if !self.env_frame_binding_live[binding] {
                continue;
            }

            self.env_frame_binding_live[binding] = false;
            let cell = self.env_frame_binding_cell[binding];
            let cell_index = cell.slot as usize;

            if self.env_cell_live[cell_index] && self.env_cell_owner_frame[cell_index] == frame_id {
                self.env_cell_live[cell_index] = false;
                let value = self.env_cell_value.get(cell.slot);
                heap.free_value(value)?;
            }
        }

        Ok(())
    }

    fn find_cell(&mut self, name: Symbol) -> Option<BindingCellId> {
        let mut current = Some(self.current);

        while let Some(frame_id) = current {
            let frame_index = frame_id.slot as usize;
            if self.validate_frame_id(frame_id).is_err() {
                return None;
            }

            if let Ok(Some(cell)) = self.binding_cell_in_frame(frame_id, name) {
                return Some(cell);
            }

            current = self.env_frame_parent[frame_index];
        }

        None
    }

    fn binding_cell_in_frame(
        &mut self,
        frame: EnvFrameId,
        name: Symbol,
    ) -> Result<Option<BindingCellId>, Error> {
        self.validate_frame_id(frame)?;

        if let Some(cell) = self.binding_index.get(&(frame.slot, name)).copied()
            && self.validate_cell_id(cell).is_ok()
        {
            return Ok(Some(cell));
        }

        for index in self.binding_indices(frame)? {
            if self.env_frame_binding_live[index] && self.binding_symbol(index) == name {
                let cell = self.env_frame_binding_cell[index];
                self.binding_index.insert((frame.slot, name), cell);
                return Ok(Some(cell));
            }
        }

        Ok(None)
    }

    fn binding_indices(&self, frame: EnvFrameId) -> Result<Vec<usize>, Error> {
        self.validate_frame_id(frame)?;
        let frame_index = frame.slot as usize;
        let start = self.env_frame_binding_range_start[frame_index] as usize;
        let len = self.env_frame_binding_range_len[frame_index] as usize;
        Ok((start..start + len).collect())
    }

    fn binding_symbol(&self, index: usize) -> Symbol {
        *self
            .env_frame_binding_symbol
            .get(index)
            .expect("env frame binding symbol column out of sync")
    }

    fn validate_frame_id(&self, id: EnvFrameId) -> Result<(), Error> {
        if id.segment != self.frame_segment
            || !self
                .env_frame_live
                .get(id.slot as usize)
                .copied()
                .unwrap_or(false)
        {
            return Err(Error::JsEnvFrameNotFound { frame: id.slot });
        }

        Ok(())
    }

    fn validate_cell_id(&self, id: BindingCellId) -> Result<(), Error> {
        if id.segment != self.binding_segment
            || !self
                .env_cell_live
                .get(id.slot as usize)
                .copied()
                .unwrap_or(false)
        {
            return Err(Error::JsBindingCellNotFound { cell: id.slot });
        }

        Ok(())
    }
}

pub(crate) struct EnvFrameEdges {
    pub(crate) frames: Vec<EnvFrameId>,
    pub(crate) objects: Vec<super::ObjectId>,
}
