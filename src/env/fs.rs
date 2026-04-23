use std::path::PathBuf;

use super::{Bytes, CompletionTarget};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenOptions;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileHandle;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FsOpId;

pub trait Fs {
    fn open(&mut self, _path: PathBuf, _opts: OpenOptions, _target: CompletionTarget) -> FsOpId {
        panic!("Fs::open stub")
    }

    fn read_at(
        &mut self,
        _file: FileHandle,
        _offset: u64,
        _len: usize,
        _target: CompletionTarget,
    ) -> FsOpId {
        panic!("Fs::read_at stub")
    }

    fn write_at(
        &mut self,
        _file: FileHandle,
        _offset: u64,
        _bytes: Bytes,
        _target: CompletionTarget,
    ) -> FsOpId {
        panic!("Fs::write_at stub")
    }

    fn sync(&mut self, _file: FileHandle, _target: CompletionTarget) -> FsOpId {
        panic!("Fs::sync stub")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FsCompletion {
    Opened(Result<FileHandle, FsError>),
    Read(Result<Bytes, FsError>),
    Wrote(Result<usize, FsError>),
    Synced(Result<(), FsError>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsError;
