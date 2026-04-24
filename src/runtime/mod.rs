use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    Actor, ActorId, ActorRef, Clock, Entropy, Env, ErasedActorMsg, ErasedResponse, Error, Fs,
    HostReply, NoopObservability, ObjectStore, Observability, RequestId, Timers, WorkerHandle,
    WorkerId, WorkerMsg,
};

mod sim;

pub use sim::{SimRuntime, SimRuntimeBuilder};

pub trait RuntimeApi {
    fn workers(&self) -> &[WorkerHandle];

    fn call(
        &self,
        worker: &WorkerHandle,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<CallHandle, Error>;
}

pub trait Shutdown {
    fn shutdown(self) -> Result<(), Error>
    where
        Self: Sized;
}

pub struct Runtime {
    workers: Vec<WorkerHandle>,
    thread_handles: Vec<std::thread::JoinHandle<Result<(), Error>>>,
}

impl Runtime {
    pub fn builder() -> RuntimeBuilder {
        RuntimeBuilder { worker_count: None }
    }

    pub fn workers(&self) -> &[WorkerHandle] {
        &self.workers
    }

    pub fn call(
        &self,
        worker: &WorkerHandle,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<CallHandle, Error> {
        submit_host_call(worker, actor, msg)
    }

    pub fn register_actor<A>(&self, worker: &WorkerHandle, actor: A) -> Result<ActorRef<A>, Error>
    where
        A: Actor<crate::worker::WorkerShardCtx> + Send + 'static,
    {
        worker.register_actor(actor)
    }

    pub fn shutdown(self) -> Result<(), Error> {
        for worker in &self.workers {
            let _ = worker.submit(WorkerMsg::Shutdown);
        }

        for handle in self.thread_handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(_) => return Err(Error::ThreadPanicked),
            }
        }

        Ok(())
    }
}

impl RuntimeApi for Runtime {
    fn workers(&self) -> &[WorkerHandle] {
        self.workers()
    }

    fn call(
        &self,
        worker: &WorkerHandle,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<CallHandle, Error> {
        self.call(worker, actor, msg)
    }
}

impl Shutdown for Runtime {
    fn shutdown(self) -> Result<(), Error> {
        self.shutdown()
    }
}

pub struct RuntimeBuilder {
    worker_count: Option<usize>,
}

impl RuntimeBuilder {
    pub fn workers(mut self, workers: usize) -> Self {
        self.worker_count = Some(workers);
        self
    }

    pub fn build(self) -> Result<Runtime, Error> {
        build_real_runtime(self)
    }
}

fn build_real_runtime(builder: RuntimeBuilder) -> Result<Runtime, Error> {
    let core_ids = core_affinity::get_core_ids().ok_or(Error::NoCores)?;

    if core_ids.is_empty() {
        return Err(Error::NoCores);
    }

    let worker_count = builder.worker_count.unwrap_or(core_ids.len());

    if worker_count == 0 {
        return Err(Error::NoWorkers);
    }

    if worker_count > core_ids.len() {
        return Err(Error::NoCores);
    }

    let mut workers = Vec::with_capacity(worker_count);
    let mut thread_handles = Vec::with_capacity(worker_count);
    let mut init_receivers = Vec::with_capacity(worker_count);

    for (index, core_id) in core_ids.into_iter().take(worker_count).enumerate() {
        let worker_id = WorkerId(index);
        let env = Box::new(RealEnv::new(worker_id)?);
        let (worker, worker_handle) = crate::worker::RealWorker::new(worker_id)?;
        let _ = crate::worker::Worker::id(&worker);

        workers.push(worker_handle.clone());

        let (init_tx, init_rx) = std::sync::mpsc::channel();
        init_receivers.push(init_rx);

        let thread_handle = std::thread::Builder::new()
            .name(format!("terracedb-worker-{index}"))
            .spawn(move || -> Result<(), Error> {
                if !core_affinity::set_for_current(core_id) {
                    let err = Error::PinFailed { worker: worker_id };
                    let _ = init_tx.send(Err(err.clone()));
                    return Err(err);
                }

                let compio_rt = match compio::runtime::Runtime::new() {
                    Ok(runtime) => runtime,
                    Err(err) => {
                        let err = Error::CompioRuntimeInit {
                            worker: worker_id,
                            source: err.to_string(),
                        };
                        let _ = init_tx.send(Err(err.clone()));
                        return Err(err);
                    }
                };

                let _ = init_tx.send(Ok(()));

                compio_rt.block_on(async move { worker.run(env).await })
            })
            .map_err(|err| Error::ThreadSpawn {
                worker: worker_id,
                source: err.to_string(),
            })?;

        thread_handles.push(thread_handle);
    }

    for init_rx in init_receivers {
        match init_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                for worker in &workers {
                    let _ = worker.submit(WorkerMsg::Shutdown);
                }
                return Err(err);
            }
            Err(_) => {
                for worker in &workers {
                    let _ = worker.submit(WorkerMsg::Shutdown);
                }
                return Err(Error::WorkerInitChannelClosed);
            }
        }
    }

    Ok(Runtime {
        workers,
        thread_handles,
    })
}

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn submit_host_call(
    worker: &WorkerHandle,
    actor: ActorId,
    msg: ErasedActorMsg,
) -> Result<CallHandle, Error> {
    let request_id = RequestId(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed));
    let (sender, receiver) = flume::bounded(1);

    worker.submit(WorkerMsg::HostRequest {
        request_id,
        actor,
        msg,
        reply_to: HostReply::new(sender),
    })?;

    Ok(CallHandle { receiver })
}

pub struct CallHandle {
    receiver: flume::Receiver<Result<ErasedResponse, Error>>,
}

impl CallHandle {
    pub fn wait(self) -> Result<ErasedResponse, Error> {
        self.receiver.recv().map_err(|_| Error::HostReplyClosed)?
    }
}

pub(crate) struct RealEnv {
    observability: NoopObservability,
    clock: StubClock,
    entropy: StubEntropy,
    timers: StubTimers,
    fs: StubFs,
    net: StubNet,
    object_store: StubObjectStore,
}

impl RealEnv {
    fn new(_worker: WorkerId) -> Result<Self, Error> {
        Ok(Self::new_stub())
    }

    fn new_stub() -> Self {
        Self {
            observability: NoopObservability,
            clock: StubClock,
            entropy: StubEntropy,
            timers: StubTimers,
            fs: StubFs,
            net: StubNet,
            object_store: StubObjectStore,
        }
    }
}

impl Env for RealEnv {
    fn observability(&mut self) -> &mut dyn Observability {
        &mut self.observability
    }

    fn clock(&mut self) -> &mut dyn Clock {
        &mut self.clock
    }

    fn entropy(&mut self) -> &mut dyn Entropy {
        &mut self.entropy
    }

    fn timers(&mut self) -> &mut dyn Timers {
        &mut self.timers
    }

    fn fs(&mut self) -> &mut dyn Fs {
        &mut self.fs
    }

    fn net(&mut self) -> &mut dyn crate::Net {
        &mut self.net
    }

    fn object_store(&mut self) -> &mut dyn ObjectStore {
        &mut self.object_store
    }
}

pub(crate) struct SimEnv(RealEnv);

impl SimEnv {
    pub(crate) fn new(_worker: WorkerId, _seed: u64) -> Result<Self, Error> {
        Ok(Self(RealEnv::new_stub()))
    }
}

impl Env for SimEnv {
    fn observability(&mut self) -> &mut dyn Observability {
        self.0.observability()
    }

    fn clock(&mut self) -> &mut dyn Clock {
        self.0.clock()
    }

    fn entropy(&mut self) -> &mut dyn Entropy {
        self.0.entropy()
    }

    fn timers(&mut self) -> &mut dyn Timers {
        self.0.timers()
    }

    fn fs(&mut self) -> &mut dyn Fs {
        self.0.fs()
    }

    fn net(&mut self) -> &mut dyn crate::Net {
        self.0.net()
    }

    fn object_store(&mut self) -> &mut dyn ObjectStore {
        self.0.object_store()
    }
}

struct StubClock;
struct StubEntropy;
struct StubTimers;
struct StubFs;
struct StubNet;
struct StubObjectStore;

impl Clock for StubClock {}
impl Entropy for StubEntropy {}
impl Timers for StubTimers {}
impl Fs for StubFs {}
impl crate::Net for StubNet {}
impl ObjectStore for StubObjectStore {}

#[cfg(test)]
mod tests {
    use crate::{Actor, Env, Error};

    use super::{Runtime, SimRuntime};
    use crate::worker::WorkerShardCtx;

    struct Increment;

    struct CounterActor {
        value: u64,
    }

    impl Actor<WorkerShardCtx> for CounterActor {
        type Msg = Increment;
        type Reply = String;

        fn handle(
            &mut self,
            _msg: Self::Msg,
            _ctx: &mut WorkerShardCtx,
            _env: &mut dyn Env,
        ) -> Result<Self::Reply, Error> {
            self.value += 1;

            Ok(format!("hello world {}", self.value))
        }
    }

    #[test]
    fn real_runtime_host_calls_increment_actor_counter() {
        let runtime = Runtime::builder().workers(1).build().unwrap();
        let actor_ref = runtime
            .register_actor(&runtime.workers()[0], CounterActor { value: 0 })
            .unwrap();

        let mut replies = Vec::new();

        for _ in 0..3 {
            let response = runtime
                .call(&runtime.workers()[0], actor_ref.id, Box::new(Increment))
                .unwrap()
                .wait()
                .unwrap();
            let response = *response.downcast::<String>().unwrap();

            replies.push(response);
        }

        runtime.shutdown().unwrap();

        assert_eq!(
            replies,
            vec![
                "hello world 1".to_owned(),
                "hello world 2".to_owned(),
                "hello world 3".to_owned(),
            ]
        );
    }

    #[test]
    fn sim_runtime_host_calls_increment_actor_counters_on_two_workers() {
        let mut runtime = SimRuntime::builder()
            .seed(12345)
            .workers(2)
            .build()
            .unwrap();
        let worker_0 = runtime.workers()[0].clone();
        let worker_1 = runtime.workers()[1].clone();
        let actor_0 = runtime
            .register_actor(&worker_0, CounterActor { value: 0 })
            .unwrap();
        let actor_1 = runtime
            .register_actor(&worker_1, CounterActor { value: 0 })
            .unwrap();

        let call_0 = runtime
            .call(&worker_0, actor_0.id, Box::new(Increment))
            .unwrap();
        let call_1 = runtime
            .call(&worker_1, actor_1.id, Box::new(Increment))
            .unwrap();
        let call_2 = runtime
            .call(&worker_1, actor_1.id, Box::new(Increment))
            .unwrap();

        runtime.run_until_idle().unwrap();

        let replies = vec![
            *call_0.wait().unwrap().downcast::<String>().unwrap(),
            *call_1.wait().unwrap().downcast::<String>().unwrap(),
            *call_2.wait().unwrap().downcast::<String>().unwrap(),
        ];

        assert_eq!(
            replies,
            vec![
                "hello world 1".to_owned(),
                "hello world 1".to_owned(),
                "hello world 2".to_owned(),
            ]
        );
    }
}
