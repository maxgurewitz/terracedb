use crate::{Error, WorkerId};

use super::{WorkerIngress, WorkerMsg};

#[derive(Clone)]
pub struct WorkerHandle {
    id: WorkerId,
    ingress: WorkerIngress,
}

impl WorkerHandle {
    pub fn new(id: WorkerId, ingress: WorkerIngress) -> Self {
        Self { id, ingress }
    }

    pub fn id(&self) -> WorkerId {
        self.id
    }

    pub fn submit(&self, msg: WorkerMsg) -> Result<(), Error> {
        self.ingress.submit(msg)
    }
}
