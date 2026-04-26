use std::collections::HashMap;

use crate::{Actor, Env, Error, WorkerShardCtx};

use super::{
    GcPolicy, HeapStats, InstructionBudget, JsAttachment, JsAttachmentBundle, JsRuntimeConfig,
    JsRuntimeId, JsRuntimeInstance, JsValue, ModuleId, ModuleKey, RunResult,
};

pub struct JsRuntimePoolConfig {
    pub attachments: Vec<Box<dyn JsAttachment + Send>>,
    pub gc_policy: GcPolicy,
}

impl JsRuntimePoolConfig {
    pub fn new() -> Self {
        Self {
            attachments: Vec::new(),
            gc_policy: GcPolicy::default(),
        }
    }

    pub fn with_attachment<A>(mut self, attachment: A) -> Self
    where
        A: JsAttachment + Send + 'static,
    {
        self.attachments.push(Box::new(attachment));
        self
    }

    pub fn with_boxed_attachment(mut self, attachment: Box<dyn JsAttachment + Send>) -> Self {
        self.attachments.push(attachment);
        self
    }

    pub fn with_bundle<B>(mut self, bundle: B) -> Self
    where
        B: JsAttachmentBundle + Send + 'static,
    {
        Box::new(bundle).append_attachments(&mut self.attachments);
        self
    }

    pub fn with_boxed_bundle(mut self, bundle: Box<dyn JsAttachmentBundle + Send>) -> Self {
        bundle.append_attachments(&mut self.attachments);
        self
    }

    pub fn with_gc_policy(mut self, gc_policy: GcPolicy) -> Self {
        self.gc_policy = gc_policy;
        self
    }
}

impl Default for JsRuntimePoolConfig {
    fn default() -> Self {
        Self::new()
    }
}

pub struct JsRuntimePoolActor {
    next_runtime_id: u64,
    runtimes: HashMap<JsRuntimeId, JsRuntimeInstance>,
    attachments: Vec<Box<dyn JsAttachment + Send>>,
    module_sources: HashMap<ModuleKey, String>,
    gc_policy: GcPolicy,
}

#[derive(Debug)]
pub enum JsPoolMsg {
    CreateRuntime,
    Eval {
        runtime_id: JsRuntimeId,
        source: String,
    },
    DefineModule {
        specifier: String,
        source: String,
    },
    EvaluateModule {
        runtime_id: JsRuntimeId,
        specifier: String,
    },
    GetModuleExport {
        runtime_id: JsRuntimeId,
        specifier: String,
        export_name: String,
    },
    EvalStart {
        runtime_id: JsRuntimeId,
        source: String,
    },
    RunForBudget {
        runtime_id: JsRuntimeId,
        budget: InstructionBudget,
    },
    RunUntilComplete {
        runtime_id: JsRuntimeId,
    },
    SerializeRuntime {
        runtime_id: JsRuntimeId,
    },
    DropRuntime {
        runtime_id: JsRuntimeId,
    },
    DeserializeRuntime {
        bytes: Vec<u8>,
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
    ModuleDefined,
    ModuleEvaluated { module: ModuleId },
    ModuleExport { value: JsValue },
    RunResult(RunResult),
    RuntimeSerialized { bytes: Vec<u8> },
    RuntimeDropped,
    RuntimeDeserialized { runtime_id: JsRuntimeId },
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
            module_sources: HashMap::new(),
            gc_policy: config.gc_policy,
        }
    }

    fn create_runtime(&mut self) -> Result<JsPoolReply, Error> {
        let runtime_id = JsRuntimeId(self.next_runtime_id);
        self.next_runtime_id += 1;

        let mut runtime = JsRuntimeInstance::with_config(
            runtime_id,
            JsRuntimeConfig {
                gc_policy: self.gc_policy,
            },
        );

        for attachment in &self.attachments {
            runtime.install_attachment(attachment.as_ref())?;
        }

        self.runtimes.insert(runtime_id, runtime);

        Ok(JsPoolReply::RuntimeCreated(runtime_id))
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

    fn define_module(&mut self, specifier: String, source: String) -> Result<JsPoolReply, Error> {
        self.module_sources.insert(ModuleKey(specifier), source);

        Ok(JsPoolReply::ModuleDefined)
    }

    fn evaluate_module(
        &mut self,
        runtime_id: JsRuntimeId,
        specifier: String,
    ) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        match runtime.evaluate_module(
            ModuleKey(specifier),
            &self.module_sources,
            &self.attachments,
        ) {
            Ok(module) => Ok(JsPoolReply::ModuleEvaluated { module }),
            Err(err) => Ok(JsPoolReply::EvalFailed(err)),
        }
    }

    fn get_module_export(
        &mut self,
        runtime_id: JsRuntimeId,
        specifier: String,
        export_name: String,
    ) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;
        let export_name = runtime.module_export_name(&export_name);

        match runtime.get_module_export(&ModuleKey(specifier), export_name) {
            Ok(value) => Ok(JsPoolReply::ModuleExport { value }),
            Err(err) => Ok(JsPoolReply::EvalFailed(err)),
        }
    }

    fn eval_start(&mut self, runtime_id: JsRuntimeId, source: &str) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::RunResult(runtime.eval_start(source)))
    }

    fn run_for_budget(
        &mut self,
        runtime_id: JsRuntimeId,
        budget: InstructionBudget,
    ) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::RunResult(runtime.run_for_budget(budget)))
    }

    fn run_until_complete(&mut self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get_mut(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::RunResult(runtime.run_until_complete()))
    }

    fn serialize_runtime(&self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .get(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        Ok(JsPoolReply::RuntimeSerialized {
            bytes: runtime.serialize()?,
        })
    }

    fn drop_runtime(&mut self, runtime_id: JsRuntimeId) -> Result<JsPoolReply, Error> {
        let runtime = self
            .runtimes
            .remove(&runtime_id)
            .ok_or(Error::RuntimeNotFound(runtime_id))?;

        runtime.destroy()?;

        Ok(JsPoolReply::RuntimeDropped)
    }

    fn deserialize_runtime(&mut self, bytes: &[u8]) -> Result<JsPoolReply, Error> {
        let runtime = JsRuntimeInstance::deserialize(bytes, &self.attachments)?;
        let runtime_id = runtime.id();

        if self.runtimes.contains_key(&runtime_id) {
            return Err(Error::JsRuntimeAlreadyExists(runtime_id));
        }

        self.next_runtime_id = self.next_runtime_id.max(runtime_id.0 + 1);
        self.runtimes.insert(runtime_id, runtime);

        Ok(JsPoolReply::RuntimeDeserialized { runtime_id })
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
            JsPoolMsg::DefineModule { specifier, source } => self.define_module(specifier, source),
            JsPoolMsg::EvaluateModule {
                runtime_id,
                specifier,
            } => self.evaluate_module(runtime_id, specifier),
            JsPoolMsg::GetModuleExport {
                runtime_id,
                specifier,
                export_name,
            } => self.get_module_export(runtime_id, specifier, export_name),
            JsPoolMsg::EvalStart { runtime_id, source } => self.eval_start(runtime_id, &source),
            JsPoolMsg::RunForBudget { runtime_id, budget } => {
                self.run_for_budget(runtime_id, budget)
            }
            JsPoolMsg::RunUntilComplete { runtime_id } => self.run_until_complete(runtime_id),
            JsPoolMsg::SerializeRuntime { runtime_id } => self.serialize_runtime(runtime_id),
            JsPoolMsg::DropRuntime { runtime_id } => self.drop_runtime(runtime_id),
            JsPoolMsg::DeserializeRuntime { bytes } => self.deserialize_runtime(&bytes),
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
    use crate::{
        AttachmentInstallCtx, ConsoleAttachment, CoreHostAttachments, CoreHostConfig,
        HostModuleInstallCtx, JsAttachment, JsOutputChunk, JsOutputReceiver, JsStreamKind,
        ModuleKey,
    };

    struct HostNumbersAttachment;

    impl JsAttachment for HostNumbersAttachment {
        fn install(&self, _ctx: &mut AttachmentInstallCtx<'_>) -> Result<(), Error> {
            Ok(())
        }

        fn has_module(&self, specifier: &ModuleKey) -> bool {
            specifier.0 == "host:numbers"
        }

        fn install_module(
            &self,
            specifier: &ModuleKey,
            ctx: &mut HostModuleInstallCtx<'_>,
        ) -> Result<bool, Error> {
            if !self.has_module(specifier) {
                return Ok(false);
            }

            ctx.export_const("one", JsValue::Number(1.0))?;
            ctx.export_const("default", JsValue::Number(2.0))?;

            Ok(true)
        }
    }

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

    fn pool_call(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        msg: JsPoolMsg,
    ) -> Result<JsPoolReply, Error> {
        let call = sim.call(worker, pool.id, Box::new(msg))?;

        sim.run_until_idle()?;

        call.wait()?
            .downcast::<JsPoolReply>()
            .map(|reply| *reply)
            .map_err(|_| Error::ActorReplyTypeMismatch)
    }

    #[test]
    fn core_host_bundle_installs_console() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(
                JsRuntimePoolConfig::new()
                    .with_bundle(CoreHostAttachments::new(CoreHostConfig { output_tx })),
            ),
        )?;

        let runtime_id = match pool_call(&mut sim, &worker, &pool, JsPoolMsg::CreateRuntime)? {
            JsPoolReply::RuntimeCreated(id) => id,
            other => panic!("unexpected reply: {other:?}"),
        };

        match pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::Eval {
                runtime_id,
                source: "console.log(1 + 2);".to_owned(),
            },
        )? {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn direct_attachment_builder_installs_console() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(
                JsRuntimePoolConfig::new().with_attachment(ConsoleAttachment { output_tx }),
            ),
        )?;

        let runtime_id = match pool_call(&mut sim, &worker, &pool, JsPoolMsg::CreateRuntime)? {
            JsPoolReply::RuntimeCreated(id) => id,
            other => panic!("unexpected reply: {other:?}"),
        };

        match pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::Eval {
                runtime_id,
                source: "console.log(1 + 2);".to_owned(),
            },
        )? {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    fn define_module(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        specifier: &str,
        source: &str,
    ) -> Result<(), Error> {
        match pool_call(
            sim,
            worker,
            pool,
            JsPoolMsg::DefineModule {
                specifier: specifier.to_owned(),
                source: source.to_owned(),
            },
        )? {
            JsPoolReply::ModuleDefined => Ok(()),
            other => panic!("unexpected reply: {other:?}"),
        }
    }

    fn evaluate_module(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
        specifier: &str,
    ) -> Result<JsPoolReply, Error> {
        pool_call(
            sim,
            worker,
            pool,
            JsPoolMsg::EvaluateModule {
                runtime_id,
                specifier: specifier.to_owned(),
            },
        )
    }

    fn get_module_export(
        sim: &mut SimRuntime,
        worker: &WorkerHandle,
        pool: &ActorRef<JsRuntimePoolActor>,
        runtime_id: JsRuntimeId,
        specifier: &str,
        export_name: &str,
    ) -> Result<JsPoolReply, Error> {
        pool_call(
            sim,
            worker,
            pool,
            JsPoolMsg::GetModuleExport {
                runtime_id,
                specifier: specifier.to_owned(),
                export_name: export_name.to_owned(),
            },
        )
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
    fn console_host_objects_survive_gc_and_release_on_destroy() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::ManualOnly)?;

        force_gc(&mut sim, &worker, &pool, runtime_id)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                console.log(1);
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        destroy_runtime(&mut sim, &worker, &pool, runtime_id)?;

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
    fn js_gc_collects_cycle_with_owned_child_object() -> Result<(), Error> {
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
                    let child = {};

                    a.b = b;
                    b.a = a;
                    a.child = child;
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
        assert!(freed >= 3);

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
    fn js_gc_preserves_externally_reachable_cycle_without_leaking_refs() -> Result<(), Error> {
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
                    let a = {};
                    let b = {};

                    root.a = a;
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

        force_gc(&mut sim, &worker, &pool, runtime_id)?;

        let stats = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(stats.allocated_objects, baseline.allocated_objects + 3);
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

    #[test]
    fn js_function_declaration_can_be_called() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                function add(a, b) {
                    return a + b;
                }

                console.log(add(1, 2));
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
    fn js_function_without_return_returns_undefined() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                function f() {
                }

                console.log(f());
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
    fn js_function_value_can_be_called_through_variable() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                function add(a, b) {
                    return a + b;
                }

                let f = add;
                console.log(f(2, 3));
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"5\n");

        Ok(())
    }

    #[test]
    fn js_duplicate_plain_parameter_uses_last_argument_binding() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                function pickLast(x, x) {
                    return x;
                }

                console.log(pickLast(1, 2));
                console.log(pickLast(1));
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"2\nundefined\n");

        Ok(())
    }

    #[test]
    fn js_function_object_can_have_properties() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                function add(a, b) {
                    return a + b;
                }

                add.x = 10;

                console.log(add.x);
                console.log(add(1, 2));
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"10\n3\n");

        Ok(())
    }

    #[test]
    fn js_function_captures_outer_binding_cell() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 10;

                function readX() {
                    return x;
                }

                x = 20;

                console.log(readX());
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"20\n");

        Ok(())
    }

    #[test]
    fn js_gc_releases_captured_block_after_closure_becomes_unreachable() -> Result<(), Error> {
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
                let f = null;

                {
                    let obj = {};

                    function captureObj() {
                        return obj;
                    }

                    f = captureObj;
                }

                console.log(f());
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        let retained = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert!(retained.allocated_objects > baseline.allocated_objects);

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                f = null;
            "#,
        )?;

        match reply {
            JsPoolReply::EvalCompleted(JsValue::Undefined) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        force_gc(&mut sim, &worker, &pool, runtime_id)?;

        let released = heap_stats(&mut sim, &worker, &pool, runtime_id)?;
        assert_eq!(released.allocated_objects, baseline.allocated_objects);
        assert_eq!(released.allocated_bytes, baseline.allocated_bytes);
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"[object Object]\n");

        Ok(())
    }

    #[test]
    fn js_runtime_serializes_mid_program_and_resumes_through_pool() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) =
            create_pool_runtime_with_policy(&mut sim, GcPolicy::ManualOnly)?;

        let start = pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::EvalStart {
                runtime_id,
                source: r#"
                    function add(a, b) {
                        let c = a + b;
                        return c;
                    }

                    let x = add(1, 2);
                    console.log(x);
                "#
                .to_owned(),
            },
        )?;

        assert_eq!(start, JsPoolReply::RunResult(RunResult::Suspended));

        let run = pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::RunForBudget {
                runtime_id,
                budget: InstructionBudget { instructions: 3 },
            },
        )?;

        assert_eq!(run, JsPoolReply::RunResult(RunResult::Suspended));
        assert!(output_rx.try_recv().is_err());

        let bytes = match pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::SerializeRuntime { runtime_id },
        )? {
            JsPoolReply::RuntimeSerialized { bytes } => bytes,
            other => panic!("unexpected reply: {other:?}"),
        };

        assert!(!bytes.is_empty());

        let dropped = pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::DropRuntime { runtime_id },
        )?;

        assert_eq!(dropped, JsPoolReply::RuntimeDropped);

        let restored_id = match pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::DeserializeRuntime { bytes },
        )? {
            JsPoolReply::RuntimeDeserialized { runtime_id } => runtime_id,
            other => panic!("unexpected reply: {other:?}"),
        };

        assert_eq!(restored_id, runtime_id);

        let done = pool_call(
            &mut sim,
            &worker,
            &pool,
            JsPoolMsg::RunUntilComplete { runtime_id },
        )?;

        assert_eq!(
            done,
            JsPoolReply::RunResult(RunResult::Completed(JsValue::Undefined))
        );
        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_runtime_deserialize_rebinds_console_to_current_pool_attachment() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx_1, output_rx_1) = flume::unbounded::<JsOutputChunk>();
        let pool_1 = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(ConsoleAttachment {
                    output_tx: output_tx_1,
                })],
                gc_policy: GcPolicy::ManualOnly,
            }),
        )?;

        let runtime_id = match pool_call(&mut sim, &worker, &pool_1, JsPoolMsg::CreateRuntime)? {
            JsPoolReply::RuntimeCreated(runtime_id) => runtime_id,
            other => panic!("unexpected reply: {other:?}"),
        };

        assert_eq!(
            pool_call(
                &mut sim,
                &worker,
                &pool_1,
                JsPoolMsg::EvalStart {
                    runtime_id,
                    source: "console.log(3);".to_owned(),
                },
            )?,
            JsPoolReply::RunResult(RunResult::Suspended)
        );
        assert_eq!(
            pool_call(
                &mut sim,
                &worker,
                &pool_1,
                JsPoolMsg::RunForBudget {
                    runtime_id,
                    budget: InstructionBudget { instructions: 1 },
                },
            )?,
            JsPoolReply::RunResult(RunResult::Suspended)
        );

        let bytes = match pool_call(
            &mut sim,
            &worker,
            &pool_1,
            JsPoolMsg::SerializeRuntime { runtime_id },
        )? {
            JsPoolReply::RuntimeSerialized { bytes } => bytes,
            other => panic!("unexpected reply: {other:?}"),
        };

        assert_eq!(
            pool_call(
                &mut sim,
                &worker,
                &pool_1,
                JsPoolMsg::DropRuntime { runtime_id },
            )?,
            JsPoolReply::RuntimeDropped
        );

        let (output_tx_2, output_rx_2) = flume::unbounded::<JsOutputChunk>();
        let pool_2 = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![Box::new(ConsoleAttachment {
                    output_tx: output_tx_2,
                })],
                gc_policy: GcPolicy::ManualOnly,
            }),
        )?;

        let restored_id = match pool_call(
            &mut sim,
            &worker,
            &pool_2,
            JsPoolMsg::DeserializeRuntime { bytes },
        )? {
            JsPoolReply::RuntimeDeserialized { runtime_id } => runtime_id,
            other => panic!("unexpected reply: {other:?}"),
        };

        assert_eq!(restored_id, runtime_id);

        assert_eq!(
            pool_call(
                &mut sim,
                &worker,
                &pool_2,
                JsPoolMsg::RunUntilComplete { runtime_id },
            )?,
            JsPoolReply::RunResult(RunResult::Completed(JsValue::Undefined))
        );

        assert!(output_rx_1.try_recv().is_err());
        assert_eq!(collect_stdout(&output_rx_2, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_calling_non_callable_value_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        let reply = eval_source(
            &mut sim,
            &worker,
            &pool,
            runtime_id,
            r#"
                let x = 1;
                x();
            "#,
        )?;

        match reply {
            JsPoolReply::EvalFailed(Error::JsTypeError { message }) => {
                assert_eq!(message, "property access expected object");
            }
            JsPoolReply::EvalFailed(Error::JsNotCallable) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn js_module_named_exports_and_imports() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./values.js",
            r#"
                export const one = 1;
                export const two = one + 1;
            "#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import { one, two } from "./values.js";
                console.log(one + two);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_default_export_and_import() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(&mut sim, &worker, &pool, "./value.js", "export default 3;")?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import value from "./value.js";
                console.log(value);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_namespace_import() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./values.js",
            r#"
                export const one = 1;
                export const two = 2;
            "#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import * as values from "./values.js";
                console.log(values.one + values.two);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_named_re_export() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(&mut sim, &worker, &pool, "./a.js", "export const one = 1;")?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./b.js",
            r#"export { one } from "./a.js";"#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import { one } from "./b.js";
                console.log(one);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"1\n");

        Ok(())
    }

    #[test]
    fn js_module_star_re_export() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./a.js",
            r#"
                export const one = 1;
                export const two = 2;
            "#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./b.js",
            r#"export * from "./a.js";"#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import { one, two } from "./b.js";
                console.log(one + two);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_default_named_re_export() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(&mut sim, &worker, &pool, "./a.js", "export default 3;")?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./b.js",
            r#"export { default as value } from "./a.js";"#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import { value } from "./b.js";
                console.log(value);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_cyclic_graph_with_live_bindings() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./a.js",
            r#"
                import { b } from "./b.js";
                export const a = 1;
                export const fromB = b;
            "#,
        )?;
        define_module(&mut sim, &worker, &pool, "./b.js", "export const b = 2;")?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import { a, fromB } from "./a.js";
                console.log(a + fromB);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");

        Ok(())
    }

    #[test]
    fn js_module_tdz_uninitialized_binding_in_cycle_errors() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./a.js",
            r#"
                import { b } from "./b.js";
                console.log(b);
                export const a = 1;
            "#,
        )?;
        define_module(
            &mut sim,
            &worker,
            &pool,
            "./b.js",
            r#"
                import { a } from "./a.js";
                console.log(a);
                export const b = 2;
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./a.js")? {
            JsPoolReply::EvalFailed(Error::JsUninitializedBinding { .. }) => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn js_module_host_can_get_exports() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let (worker, pool, runtime_id, _output_rx) = create_pool_runtime(&mut sim)?;

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./values.js",
            r#"
                export const one = 1;
                export default 2;
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./values.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(
            get_module_export(&mut sim, &worker, &pool, runtime_id, "./values.js", "one")?,
            JsPoolReply::ModuleExport {
                value: JsValue::Number(1.0)
            }
        );
        assert_eq!(
            get_module_export(
                &mut sim,
                &worker,
                &pool,
                runtime_id,
                "./values.js",
                "default",
            )?,
            JsPoolReply::ModuleExport {
                value: JsValue::Number(2.0)
            }
        );

        Ok(())
    }

    #[test]
    fn js_attachment_can_provide_host_module_exports() -> Result<(), Error> {
        let mut sim = SimRuntime::builder().seed(123).workers(1).build()?;
        let worker = sim.workers()[0].clone();
        let (output_tx, output_rx) = flume::unbounded::<JsOutputChunk>();
        let pool = sim.register_actor(
            &worker,
            JsRuntimePoolActor::new(JsRuntimePoolConfig {
                attachments: vec![
                    Box::new(ConsoleAttachment { output_tx }),
                    Box::new(HostNumbersAttachment),
                ],
                gc_policy: GcPolicy::default(),
            }),
        )?;

        let runtime_id = match pool_call(&mut sim, &worker, &pool, JsPoolMsg::CreateRuntime)? {
            JsPoolReply::RuntimeCreated(runtime_id) => runtime_id,
            other => panic!("unexpected reply: {other:?}"),
        };

        define_module(
            &mut sim,
            &worker,
            &pool,
            "./main.js",
            r#"
                import value, { one } from "host:numbers";
                console.log(one + value);
            "#,
        )?;

        match evaluate_module(&mut sim, &worker, &pool, runtime_id, "./main.js")? {
            JsPoolReply::ModuleEvaluated { .. } => {}
            other => panic!("unexpected reply: {other:?}"),
        }

        assert_eq!(collect_stdout(&output_rx, runtime_id), b"3\n");
        assert_eq!(
            get_module_export(&mut sim, &worker, &pool, runtime_id, "host:numbers", "one")?,
            JsPoolReply::ModuleExport {
                value: JsValue::Number(1.0)
            }
        );

        Ok(())
    }
}
