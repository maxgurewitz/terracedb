use std::collections::HashMap;

use crate::{Actor, Env, Error, WorkerShardCtx};

use super::{JsAttachment, JsRuntimeId, JsRuntimeInstance, JsValue};

pub struct JsRuntimePoolConfig {
    pub attachments: Vec<Box<dyn JsAttachment + Send>>,
}

pub struct JsRuntimePoolActor {
    next_runtime_id: u64,
    runtimes: HashMap<JsRuntimeId, JsRuntimeInstance>,
    attachments: Vec<Box<dyn JsAttachment + Send>>,
}

#[derive(Debug)]
pub enum JsPoolMsg {
    CreateRuntime,
    Eval {
        runtime_id: JsRuntimeId,
        source: String,
    },
}

#[derive(Debug, PartialEq)]
pub enum JsPoolReply {
    RuntimeCreated(JsRuntimeId),
    EvalCompleted(JsValue),
}

impl JsRuntimePoolActor {
    pub fn new(config: JsRuntimePoolConfig) -> Self {
        Self {
            next_runtime_id: 0,
            runtimes: HashMap::new(),
            attachments: config.attachments,
        }
    }

    fn create_runtime(&mut self) -> Result<JsPoolReply, Error> {
        let runtime_id = JsRuntimeId(self.next_runtime_id);
        self.next_runtime_id += 1;

        let mut runtime = JsRuntimeInstance::new(runtime_id);

        for attachment in &self.attachments {
            let runtime_attachment = attachment.instantiate(runtime_id)?;
            runtime.install(runtime_attachment)?;
        }

        self.runtimes.insert(runtime_id, runtime);

        Ok(JsPoolReply::RuntimeCreated(runtime_id))
    }

    fn eval(&mut self, runtime_id: JsRuntimeId, source: &str) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;
        let value = runtime.eval(source)?;

        Ok(JsPoolReply::EvalCompleted(value))
    }
}

impl Actor<WorkerShardCtx> for JsRuntimePoolActor {
    type Msg = JsPoolMsg;
    type Reply = JsPoolReply;

    fn handle(
        &mut self,
        msg: Self::Msg,
        _ctx: &mut WorkerShardCtx,
        _env: &mut dyn Env,
    ) -> Result<Self::Reply, Error> {
        match msg {
            JsPoolMsg::CreateRuntime => self.create_runtime(),
            JsPoolMsg::Eval { runtime_id, source } => self.eval(runtime_id, &source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{Error, SimRuntime};

    use super::*;
    use crate::{ConsoleAttachment, JsOutputChunk, JsStreamKind};

    #[test]
    fn minimal_js_console_log_adds_numbers() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
            }),
        )?;

        let create = sim.call(&worker, pool.id, Box::new(JsPoolMsg::CreateRuntime))?;
        sim.run_until_idle()?;

        let runtime_id = match *create
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::RuntimeCreated(id) => id,
            other => panic!("unexpected reply: {other:?}"),
        };

        let eval = sim.call(
            &worker,
            pool.id,
            Box::new(JsPoolMsg::Eval {
                runtime_id,
                source: r#"
                    let x = 1;
                    let y = 2;
                    console.log(x + y);
                "#
                .to_owned(),
            }),
        )?;

        sim.run_until_idle()?;

        match *eval
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let mut stdout_by_runtime: HashMap<JsRuntimeId, Vec<u8>> = HashMap::new();
        let mut stderr_by_runtime: HashMap<JsRuntimeId, Vec<u8>> = HashMap::new();

        while let Ok(chunk) = output_rx.try_recv() {
            match chunk.stream {
                JsStreamKind::Stdout => {
                    stdout_by_runtime
                        .entry(chunk.runtime_id)
                        .or_default()
                        .extend_from_slice(&chunk.bytes);
                }
                JsStreamKind::Stderr => {
                    stderr_by_runtime
                        .entry(chunk.runtime_id)
                        .or_default()
                        .extend_from_slice(&chunk.bytes);
                }
            }
        }

        assert_eq!(
            stdout_by_runtime.get(&runtime_id).map(Vec::as_slice),
            Some(&b"3\n"[..])
        );
        assert_eq!(stderr_by_runtime.get(&runtime_id).map(Vec::as_slice), None);

        Ok(())
    }
}
