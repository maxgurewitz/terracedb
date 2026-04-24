use super::HostReply;

pub(crate) enum PendingResponse {
    Host(HostReply),
}
