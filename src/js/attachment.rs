use crate::Error;

use super::{ChannelByteSink, JsOutputSender, JsRuntimeId, JsRuntimeInstance, JsStreamKind};

pub trait JsAttachment {
    fn instantiate(&self, runtime_id: JsRuntimeId) -> Result<Box<dyn JsRuntimeAttachment>, Error>;
}

pub trait JsRuntimeAttachment: Send {
    fn install(self: Box<Self>, runtime: &mut JsRuntimeInstance) -> Result<(), Error>;
}

#[derive(Clone)]
pub struct ConsoleAttachment {
    pub output_tx: JsOutputSender,
}

pub struct RuntimeConsole {
    pub stdout: ChannelByteSink,
    pub stderr: ChannelByteSink,
}

impl JsAttachment for ConsoleAttachment {
    fn instantiate(&self, runtime_id: JsRuntimeId) -> Result<Box<dyn JsRuntimeAttachment>, Error> {
        Ok(Box::new(RuntimeConsole {
            stdout: ChannelByteSink {
                runtime_id,
                stream: JsStreamKind::Stdout,
                tx: self.output_tx.clone(),
            },
            stderr: ChannelByteSink {
                runtime_id,
                stream: JsStreamKind::Stderr,
                tx: self.output_tx.clone(),
            },
        }))
    }
}

impl JsRuntimeAttachment for RuntimeConsole {
    fn install(self: Box<Self>, runtime: &mut JsRuntimeInstance) -> Result<(), Error> {
        runtime.insert_attachment(*self);
        Ok(())
    }
}
