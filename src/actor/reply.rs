use crate::{ErasedResponse, Error, HostReply, RequestId, WorkerHandle};

pub enum ReplyTo {
    Host(HostReply),
    Worker {
        worker: WorkerHandle,
        request_id: RequestId,
    },
}

impl ReplyTo {
    pub fn reply(self, response: ErasedResponse) -> Result<(), Error> {
        match self {
            Self::Host(reply) => reply.send(Ok(response)),
            Self::Worker { worker, request_id } => worker.submit(crate::WorkerMsg::RequestDone {
                request_id,
                response,
            }),
        }
    }
}
