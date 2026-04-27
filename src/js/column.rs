use serde::{Deserialize, Serialize};

#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct ColumnAccessTrace {
    pub reads: usize,
    pub writes: usize,
    pub appends: usize,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ColumnRecorder {
    reads: Cell<usize>,
    writes: Cell<usize>,
    appends: Cell<usize>,
}

#[cfg(test)]
impl Clone for ColumnRecorder {
    fn clone(&self) -> Self {
        let recorder = Self::default();
        recorder.reads.set(self.reads.get());
        recorder.writes.set(self.writes.get());
        recorder.appends.set(self.appends.get());
        recorder
    }
}

#[cfg(test)]
impl ColumnRecorder {
    fn trace(&self) -> ColumnAccessTrace {
        ColumnAccessTrace {
            reads: self.reads.get(),
            writes: self.writes.get(),
            appends: self.appends.get(),
        }
    }

    fn reset(&self) {
        self.reads.set(0);
        self.writes.set(0);
        self.appends.set(0);
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageColumn<T> {
    values: Vec<T>,

    #[cfg(test)]
    #[serde(skip, default)]
    recorder: ColumnRecorder,
}

impl<T> StorageColumn<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),

            #[cfg(test)]
            recorder: ColumnRecorder::default(),
        }
    }

    pub fn push(&mut self, value: T) {
        #[cfg(test)]
        self.recorder.appends.set(self.recorder.appends.get() + 1);

        self.values.push(value);
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        #[cfg(test)]
        self.recorder.reads.set(self.recorder.reads.get() + 1);

        self.values.get(index)
    }

    #[cfg(test)]
    pub fn trace(&self) -> ColumnAccessTrace {
        self.recorder.trace()
    }

    #[cfg(test)]
    pub fn reset_trace(&self) {
        self.recorder.reset();
    }
}

impl<T> Default for StorageColumn<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub struct StorageAccessTrace {
    pub heap_property_key_symbol: ColumnAccessTrace,
    pub env_frame_binding_symbol: ColumnAccessTrace,
}
