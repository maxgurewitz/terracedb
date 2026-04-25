use std::collections::HashMap;

use crate::{Actor, Env, Error, WorkerShardCtx};

use super::{
    GcPolicy, HeapStats, JsAttachment, JsRuntimeConfig, JsRuntimeId, JsRuntimeInstance, JsValue,
};

pub struct JsRuntimePoolConfig {
    pub attachments: Vec<Box<dyn JsAttachment + Send>>,
    pub gc_policy: GcPolicy,
}

impl Default for JsRuntimePoolConfig {
    fn default() -> Self {
        Self {
            attachments: Vec::new(),
            gc_policy: GcPolicy::default(),
        }
    }
}

pub struct JsRuntimePoolActor {
    next_runtime_id: u64,
    runtimes: HashMap<JsRuntimeId, JsRuntimeInstance>,
    attachments: Vec<Box<dyn JsAttachment + Send>>,
    gc_policy: GcPolicy,
}

#[derive(Debug)]
pub enum JsPoolMsg {
    CreateRuntime,
    Eval {
        runtime_id: JsRuntimeId,
        source: String,
    },
    ForceGc {
        runtime_id: JsRuntimeId,
    },
    HeapStats {
        runtime_id: JsRuntimeId,
    },
    DestroyRuntime {
        runtime_id: JsRuntimeId,
    },
}

#[derive(Debug, PartialEq)]
pub enum JsPoolReply {
    RuntimeCreated(JsRuntimeId),
    EvalCompleted(JsValue),
    EvalFailed(Error),
    GcCompleted { freed: usize },
    HeapStats(HeapStats),
    RuntimeDestroyed,
}

impl JsRuntimePoolActor {
    pub fn new(config: JsRuntimePoolConfig) -> Self {
        Self {
            next_runtime_id: 0,
            runtimes: HashMap::new(),
            attachments: config.attachments,
            gc_policy: config.gc_policy,
        }
    }

    fn create_runtime(&mut self) -> Result<JsPoolReply, Error> {
        let runtime_id = JsRuntimeId(self.next_runtime_id);
        self.next_runtime_id += 1;

        let mut runtime = JsRuntimeInstance::with_config(
            runtime_id,
            JsRuntimeConfig {
                gc_policy: self.gc_policy(),
            },
        );

        for attachment in &self.attachments {
            runtime.install_attachment(attachment.as_ref())?;
        }

        self.runtimes.insert(runtime_id, runtime);

        Ok(JsPoolReply::RuntimeCreated(runtime_id))
    }

    fn gc_policy(&self) -> GcPolicy {
        self.gc_policy
    }

    fn eval(&mut self, runtime_id: JsRuntimeId, source: &str) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;
        match runtime.eval(source) {
            Ok(value) => Ok(JsPoolReply::EvalCompleted(value)),
            Err(err) => Ok(JsPoolReply::EvalFailed(err)),
        }
    }

    fn force_gc(&mut self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::GcCompleted {
            freed: runtime.force_gc()?,
        })
    }

    fn heap_stats(&self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::HeapStats(runtime.heap_stats()))
    }

    fn destroy_runtime(&mut self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .remove(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        runtime.destroy()?;

        Ok(JsPoolReply::RuntimeDestroyed)
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
            JsPoolMsg::ForceGc { runtime_id } => self.force_gc(runtime_id),
            JsPoolMsg::HeapStats { runtime_id } => self.heap_stats(runtime_id),
            JsPoolMsg::DestroyRuntime { runtime_id } => self.destroy_runtime(runtime_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{ActorRef, Error, SimRuntime, WorkerHandle};

    use super::*;
    use crate::{ConsoleAttachment, JsOutputChunk, JsOutputReceiver, JsStreamKind};

    fn create_pool_runtime(
        sim: &mut SimRuntime,
    ) -> Result<
        (
            WorkerHandle,
            ActorRef<JsRuntimePoolActor>,
            JsRuntimeId,
            JsOutputReceiver,
        ),
        Error,
    > {
        create_pool_runtime_with_policy(sim, GcPolicy::default())
    }

    fn create_pool_runtime_with_policy(
        sim: &mut SimRuntime,
        gc_policy: GcPolicy,
    ) -> Result<
        (
            WorkerHandle,
            ActorRef<JsRuntimePoolActor>,
            JsRuntimeId,
            JsOutputReceiver,
        ),
        Error,
    > {
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy,
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

        Ok((worker, pool, runtime_id, output_rx))
    }

    fn eval_source(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
        source: &str,
    ) -> Result<JsPoolReply, Error> {
        let eval = sim.call(
            worker,
            pool.id,
            Box::new(JsPoolMsg::Eval {
                runtime_id,
                source: source.to_owned(),
            }),
        )?;

        sim.run_until_idle()?;

        eval.wait()?
            .downcast::<JsPoolReply>()
            .map(|reply| *reply)
            .map_err(|_| Error::ActorReplyTypeMismatch)
    }

    fn collect_stdout(output_rx: &JsOutputReceiver, runtime_id: JsRuntimeId) -> Vec<u8> {
        let mut stdout = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id == runtime_id && chunk.stream == JsStreamKind::Stdout {
                stdout.extend_from_slice(&chunk.bytes);
            }
        }

        stdout
    }

    fn heap_stats(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
    ) -> Result<HeapStats, Error> {
        let call = sim.call(
            worker,
            pool.id,
            Box::new(JsPoolMsg::HeapStats { runtime_id }),
        )?;

        sim.run_until_idle()?;

        match *call
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::HeapStats(stats) => Ok(stats),
            other => panic!("unexpected reply: {other:?}"),
        }
    }

    fn force_gc(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
    ) -> Result<usize, Error> {
        let call = sim.call(worker, pool.id, Box::new(JsPoolMsg::ForceGc { runtime_id }))?;

        sim.run_until_idle()?;

        match *call
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::GcCompleted { freed } => Ok(freed),
            other => panic!("unexpected reply: {other:?}"),
        }
    }

    fn destroy_runtime(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
    ) -> Result<(), Error> {
        let call = sim.call(
            worker,
            pool.id,
            Box::new(JsPoolMsg::DestroyRuntime { runtime_id }),
        )?;

        sim.run_until_idle()?;

        match *call
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::RuntimeDestroyed => Ok(()),
            other => panic!("unexpected reply: {other:?}"),
        }
    }

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
                gc_policy: GcPolicy::default(),
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

    #[test]
    fn minimal_js_let_const_and_assignment() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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
                    const y = 2;
                    x = x + y;
                    console.log(x);
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

        let mut stdout = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id == runtime_id && chunk.stream == JsStreamKind::Stdout {
                stdout.extend_from_slice(&chunk.bytes);
            }
        }

        assert_eq!(stdout, b"3\n");

        Ok(())
    }

    #[test]
    fn minimal_js_const_reassignment_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, _output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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
                    const x = 1;
                    x = 2;
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
            JsPoolReply::EvalFailed(Error::JsAssignToConst { name }) => {
                assert_eq!(name, "x");
            }
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn minimal_js_assignment_to_missing_binding_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, _output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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
                source: "x = 1;".to_owned(),
            }),
        )?;

        sim.run_until_idle()?;

        match *eval
            .wait()?
            .downcast::<JsPoolReply>()
            .map_err(|_| Error::ActorReplyTypeMismatch)?
        {
            JsPoolReply::EvalFailed(Error::JsBindingNotFound { name }) => {
                assert_eq!(name, "x");
            }
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn minimal_js_block_scope_shadowing() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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

                    {
                        let x = 2;
                        console.log(x);
                    }

                    console.log(x);
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

        let mut stdout = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id == runtime_id && chunk.stream == JsStreamKind::Stdout {
                stdout.extend_from_slice(&chunk.bytes);
            }
        }

        assert_eq!(stdout, b"2\n1\n");

        Ok(())
    }

    #[test]
    fn minimal_js_block_assignment_updates_outer_binding() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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

                    {
                        x = x + 2;
                    }

                    console.log(x);
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

        let mut stdout = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id == runtime_id && chunk.stream == JsStreamKind::Stdout {
                stdout.extend_from_slice(&chunk.bytes);
            }
        }

        assert_eq!(stdout, b"3\n");

        Ok(())
    }

    #[test]
    fn minimal_js_duplicate_binding_in_same_scope_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, _output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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
                    let x = 2;
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
            JsPoolReply::EvalFailed(_) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn minimal_js_internal_symbols_preserve_scoping_behavior() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let console = ConsoleAttachment { output_tx };
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(console)],
                gc_policy: GcPolicy::default(),
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

                    {
                        let x = 2;
                        console.log(x);
                    }

                    x = x + 3;
                    console.log(x);
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

        let mut stdout = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id == runtime_id && chunk.stream == JsStreamKind::Stdout {
                stdout.extend_from_slice(&chunk.bytes);
            }
        }

        assert_eq!(stdout, b"2\n4\n");

        Ok(())
    }

    #[test]
    fn minimal_js_basic_arithmetic_ops() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                console.log(1 + 2 * 3);
                console.log((1 + 2) * 3);
                console.log(10 - 3);
                console.log(10 / 2);
                console.log(10 % 3);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"7\n9\n7\n5\n1\n");

        Ok(())
    }

    #[test]
    fn minimal_js_comparison_and_equality_ops() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                console.log(3 > 2);
                console.log(3 >= 3);
                console.log(2 < 1);
                console.log(2 <= 2);
                console.log(1 === 1);
                console.log(1 !== 2);
                console.log(true === false);
                console.log(null === null);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(
            collect_stdout(&output_rx, runtime_id),
            b"true\ntrue\nfalse\ntrue\ntrue\ntrue\nfalse\ntrue\n"
        );

        Ok(())
    }

    #[test]
    fn minimal_js_boolean_ops_short_circuit() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 1;
                false && (x = 2);
                console.log(x);

                let y = 1;
                true || (y = 2);
                console.log(y);

                console.log(true && false);
                console.log(true || false);
                console.log(!false);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(
            collect_stdout(&output_rx, runtime_id),
            b"1\n1\nfalse\ntrue\ntrue\n"
        );

        Ok(())
    }

    #[test]
    fn minimal_js_arithmetic_type_error() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(&mut sim, &worker, &pool, runtime_id, "true + 1;")?;

        assert!(matches!(
            reply,
            JsPoolReply::EvalFailed(Error::JsTypeError { .. })
        ));

        Ok(())
    }

    #[test]
    fn minimal_js_logical_type_error() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(&mut sim, &worker, &pool, runtime_id, "1 && true;")?;

        assert!(matches!(
            reply,
            JsPoolReply::EvalFailed(Error::JsTypeError { .. })
        ));

        Ok(())
    }

    #[test]
    fn console_attachment_installs_global_console_object() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 1;
                let y = 2;
                console.log(x + y);
                console.error(y);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        while let Ok(chunk) = output_rx.try_recv() {
            if chunk.runtime_id != runtime_id {
                continue;
            }

            match chunk.stream {
                JsStreamKind::Stdout => stdout.extend_from_slice(&chunk.bytes),
                JsStreamKind::Stderr => stderr.extend_from_slice(&chunk.bytes),
            }
        }

        assert_eq!(stdout, b"3\n");
        assert_eq!(stderr, b"2\n");

        Ok(())
    }

    #[test]
    fn console_log_is_callable_property_value() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let f = console.log;
                f(3);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn missing_console_attachment_leaves_console_unbound() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: Vec::new(),
                gc_policy: GcPolicy::default(),
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

        let reply = eval_source(&mut sim, &worker, &pool, runtime_id, "console.log(1);")?;

        match reply {
            JsPoolReply::EvalFailed(Error::JsBindingNotFound { name }) => {
                assert_eq!(name, "console");
            }
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn minimal_js_object_literal_property_get() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let obj = { x: 1, y: 2 };
                console.log(obj.x + obj.y);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn minimal_js_object_property_assignment() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let obj = {};
                obj.x = 1;
                obj.x = obj.x + 2;
                console.log(obj.x);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn minimal_js_missing_property_is_undefined() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let obj = {};
                console.log(obj.missing);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"undefined\n");

        Ok(())
    }

    #[test]
    fn minimal_js_property_access_on_non_object_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 1;
                console.log(x.y);
            "#,
        )?;

        assert!(matches!(
            reply,
            JsPoolReply::EvalFailed(Error::JsTypeError { .. })
        ));

        Ok(())
    }

    #[test]
    fn js_gc_frees_acyclic_object_after_scope_exit() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let obj = {};
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let after = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(after.allocated_objects, baseline.allocated_objects);
        assert_eq!(after.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_gc_frees_nested_acyclic_graph_after_scope_exit() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let a = {};
                    let b = {};
                    a.child = b;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let after = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(after.allocated_objects, baseline.allocated_objects);
        assert_eq!(after.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_gc_property_overwrite_releases_old_object() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let a = {};
                    let b = {};
                    let holder = {};

                    holder.x = a;
                    holder.x = b;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let after = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(after.allocated_objects, baseline.allocated_objects);
        assert_eq!(after.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_gc_collects_simple_object_cycle() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::ManualOnly)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let a = {};
                    let b = {};

                    a.b = b;
                    b.a = a;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let before_gc = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert!(before_gc.allocated_objects > baseline.allocated_objects);

        let freed = force_gc(&mut sim, &worker, &pool, runtime_id)?;
        assert!(freed >= 2);

        let after_gc = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(after_gc.allocated_objects, baseline.allocated_objects);
        assert_eq!(after_gc.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_gc_collects_self_cycle() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::ManualOnly)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let a = {};
                    a.self = a;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let before_gc = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert!(before_gc.allocated_objects > baseline.allocated_objects);

        let freed = force_gc(&mut sim, &worker, &pool, runtime_id)?;
        assert!(freed >= 1);

        let after_gc = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(after_gc.allocated_objects, baseline.allocated_objects);
        assert_eq!(after_gc.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_gc_preserves_externally_reachable_graph() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::ManualOnly)?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let root = {};

                {
                    let child = {};
                    root.child = child;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        force_gc(&mut sim, &worker, &pool, runtime_id)?;

        let stats = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert!(stats.allocated_objects >= baseline.allocated_objects + 2);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        destroy_runtime(&mut sim, &worker, &pool, runtime_id)?;

        Ok(())
    }

    #[test]
    fn js_gc_automatic_policy_collects_cycles_at_threshold() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::Automatic { threshold_bytes: 1 })?;
        let baseline = heap_stats(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                {
                    let a = {};
                    let b = {};

                    a.b = b;
                    b.a = a;
                }

                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let after = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert!(after.gc_runs > baseline.gc_runs);
        assert_eq!(after.allocated_objects, baseline.allocated_objects);
        assert_eq!(after.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn minimal_js_bytecode_vm_preserves_existing_behavior() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 1;
                const y = 2;

                {
                    let x = 10;
                    console.log(x);
                }

                x = x + y;
                console.log(x);
                console.log(1 + 2 * 3);
                console.log(true || (x = 100));
                console.log(x);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(
            collect_stdout(&output_rx, runtime_id),
            b"10\n3\n7\ntrue\n3\n"
        );

        Ok(())
    }
}
