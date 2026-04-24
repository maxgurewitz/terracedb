use crate::{ErasedResponse, Error};

pub struct HostReply {
    sender: flume::Sender<Result<ErasedResponse, Error>>,
}

impl HostReply {
    pub(crate) fn new(sender: flume::Sender<Result<ErasedResponse, Error>>) -> Self {
        Self { sender }
    }

    pub fn send(self, response: Result<ErasedResponse, Error>) -> Result<(), Error> {
        self.sender
            .send(response)
            .map_err(|_| Error::HostReplyClosed)
    }
}
