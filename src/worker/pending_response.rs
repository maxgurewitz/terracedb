use super::{ConnectionHandle, HostReply};

#[allow(dead_code)]
pub(crate) enum PendingResponse {
    Host(HostReply),
    Connection(ConnectionHandle),
}
