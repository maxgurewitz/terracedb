use bytes::Bytes;

use crate::Error;

use super::JsRuntimeId;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum JsStreamKind {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsOutputChunk {
    pub runtime_id: JsRuntimeId,
    pub stream: JsStreamKind,
    pub bytes: Bytes,
}

pub type JsOutputSender = flume::Sender<JsOutputChunk>;
pub type JsOutputReceiver = flume::Receiver<JsOutputChunk>;

#[derive(Clone)]
pub struct ChannelByteSink {
    pub runtime_id: JsRuntimeId,
    pub stream: JsStreamKind,
    pub tx: JsOutputSender,
}

impl ChannelByteSink {
    pub fn write(&self, bytes: Bytes) -> Result<(), Error> {
        self.tx
            .send(JsOutputChunk {
                runtime_id: self.runtime_id,
                stream: self.stream,
                bytes,
            })
            .map_err(|_| Error::OutputReceiverDropped)
    }
}
