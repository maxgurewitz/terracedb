use std::net::SocketAddr;

use super::{Bytes, CompletionTarget};

pub type Addr = SocketAddr;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ListenerId;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConnId;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetOpId;

pub trait Net {
    fn listen(&mut self, _addr: Addr, _target: CompletionTarget) -> ListenerId {
        panic!("Net::listen stub")
    }

    fn connect(&mut self, _addr: Addr, _target: CompletionTarget) -> NetOpId {
        panic!("Net::connect stub")
    }

    fn accept(&mut self, _listener: ListenerId, _target: CompletionTarget) -> NetOpId {
        panic!("Net::accept stub")
    }

    fn read(&mut self, _conn: ConnId, _max_len: usize, _target: CompletionTarget) -> NetOpId {
        panic!("Net::read stub")
    }

    fn write(&mut self, _conn: ConnId, _bytes: Bytes, _target: CompletionTarget) -> NetOpId {
        panic!("Net::write stub")
    }

    fn close(&mut self, _conn: ConnId) {
        panic!("Net::close stub")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetCompletion {
    Connected(Result<ConnId, NetError>),
    Accepted(Result<ConnId, NetError>),
    Read(Result<Bytes, NetError>),
    Wrote(Result<usize, NetError>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NetError {
    ConnectionReset,
}
