use std::time::Duration;

use super::{ActorId, Addr, ConnId, FsError, FsOpId, ObjectError, ObjectKey, ObjectOpId, WorkerId};

pub trait Observability {
    fn enabled(&self, _kind: ObsEventKind) -> bool {
        panic!("Observability::enabled stub")
    }

    fn emit(&mut self, _event: ObsEvent) {
        panic!("Observability::emit stub")
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObsEventKind {
    NetConnectStarted,
    NetConnectSucceeded,
    NetConnectFailed,
    NetReadFailed,
    NetWriteFailed,
    FsOpStarted,
    FsOpFinished,
    FsOpFailed,
    ObjectStoreRequestStarted,
    ObjectStoreRequestFinished,
    ObjectStoreRequestFailed,
    MessageSent,
    MessageDelivered,
    MessageQueueDepth,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObsEvent {
    NetConnectStarted {
        addr: Addr,
    },
    NetConnectSucceeded {
        conn: ConnId,
        addr: Addr,
    },
    NetConnectFailed {
        addr: Addr,
        error: super::NetError,
    },
    NetReadFailed {
        conn: ConnId,
        error: super::NetError,
    },
    NetWriteFailed {
        conn: ConnId,
        error: super::NetError,
    },
    FsOpStarted {
        op: FsOpId,
        kind: FsOpKind,
    },
    FsOpFinished {
        op: FsOpId,
        kind: FsOpKind,
        result: ResultKind,
        elapsed: Duration,
    },
    FsOpFailed {
        op: FsOpId,
        kind: FsOpKind,
        error: FsError,
        elapsed: Duration,
    },
    ObjectStoreRequestStarted {
        op: ObjectOpId,
        kind: ObjectOpKind,
        key: ObjectKey,
    },
    ObjectStoreRequestFinished {
        op: ObjectOpId,
        kind: ObjectOpKind,
        result: ResultKind,
        elapsed: Duration,
    },
    ObjectStoreRequestFailed {
        op: ObjectOpId,
        kind: ObjectOpKind,
        error: ObjectError,
        elapsed: Duration,
    },
    MessageSent {
        from: ActorId,
        to: ActorId,
    },
    MessageDelivered {
        to: ActorId,
    },
    MessageQueueDepth {
        worker: WorkerId,
        depth: usize,
    },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FsOpKind {
    Open,
    ReadAt,
    WriteAt,
    Sync,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObjectOpKind {
    Put,
    Get,
    GetRanges,
    StartMultipartPut,
    UploadPart,
    CompleteMultipartPut,
    AbortMultipartPut,
    Delete,
    List,
    ListWithDelimiter,
    Copy,
    Rename,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ResultKind {
    Success,
    Cancelled,
    Timeout,
}
