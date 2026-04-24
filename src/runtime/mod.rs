use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    ActorId, Clock, Entropy, Env, ErasedActorMsg, ErasedResponse, Error, Fs, HostReply,
    NoopObservability, ObjectStore, Observability, RequestId, Timers, WorkerHandle, WorkerId,
    WorkerMsg,
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
        RuntimeBuilder
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

pub struct RuntimeBuilder;

impl RuntimeBuilder {
    pub fn build(self) -> Result<Runtime, Error> {
        build_real_runtime()
    }
}

fn build_real_runtime() -> Result<Runtime, Error> {
    let core_ids = core_affinity::get_core_ids().ok_or(Error::NoCores)?;

    if core_ids.is_empty() {
        return Err(Error::NoCores);
    }

    let mut workers = Vec::with_capacity(core_ids.len());
    let mut thread_handles = Vec::with_capacity(core_ids.len());
    let mut init_receivers = Vec::with_capacity(core_ids.len());

    for (index, core_id) in core_ids.into_iter().enumerate() {
        let worker_id = WorkerId(index);
        let env = Box::new(RealEnv::new(worker_id)?);
        let (worker, worker_handle) = crate::worker::Worker::new(worker_id, env)?;

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

                compio_rt.block_on(async move { worker.run().await })
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
