use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::{
    Actor, ActorId, ActorRef, Clock, CompletionTarget, Entropy, Env, ErasedActorMsg,
    ErasedResponse, Error, Fs, HostReply, NoopObservability, ObjectStore, Observability, RequestId,
    TimerId, Timers, WorkerHandle, WorkerId, WorkerMsg,
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

    pub fn try_wait(&self) -> Result<Option<ErasedResponse>, Error> {
        match self.receiver.try_recv() {
            Ok(response) => response.map(Some),
            Err(flume::TryRecvError::Empty) => Ok(None),
            Err(flume::TryRecvError::Disconnected) => Err(Error::HostReplyClosed),
        }
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

#[derive(Clone)]
pub(crate) struct SimTime {
    inner: Arc<Mutex<SimTimeState>>,
}

impl SimTime {
    pub(crate) fn new(start_unix_timestamp: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimTimeState {
                origin: Instant::now(),
                elapsed: Duration::ZERO,
                start_unix_timestamp,
                next_timer_id: 1,
                timers: Vec::new(),
            })),
        }
    }

    pub(crate) fn fast_forward(&self, duration: Duration) -> Vec<DueTimer> {
        let mut state = self.inner.lock().expect("sim time lock poisoned");
        state.elapsed += duration;

        let now = state.now();
        let timers = std::mem::take(&mut state.timers);
        let mut due = Vec::new();

        for timer in timers {
            if timer.at <= now {
                due.push(DueTimer {
                    timer_id: timer.timer_id,
                    target: timer.target,
                });
            } else {
                state.timers.push(timer);
            }
        }

        due
    }
}

struct SimTimeState {
    origin: Instant,
    elapsed: Duration,
    start_unix_timestamp: u64,
    next_timer_id: u64,
    timers: Vec<ScheduledTimer>,
}

impl SimTimeState {
    fn now(&self) -> Instant {
        self.origin + self.elapsed
    }

    fn unix_timestamp(&self) -> u64 {
        self.start_unix_timestamp + self.elapsed.as_secs()
    }
}

#[derive(Clone, Copy)]
struct ScheduledTimer {
    timer_id: TimerId,
    at: Instant,
    target: CompletionTarget,
}

#[derive(Clone, Copy)]
pub(crate) struct DueTimer {
    pub(crate) timer_id: TimerId,
    pub(crate) target: CompletionTarget,
}

struct SimClock {
    time: SimTime,
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        self.time
            .inner
            .lock()
            .expect("sim time lock poisoned")
            .now()
    }

    fn unix_timestamp(&self) -> u64 {
        self.time
            .inner
            .lock()
            .expect("sim time lock poisoned")
            .unix_timestamp()
    }
}

struct SimTimers {
    time: SimTime,
}

impl Timers for SimTimers {
    fn sleep_until(&mut self, at: Instant, target: CompletionTarget) -> TimerId {
        let mut state = self.time.inner.lock().expect("sim time lock poisoned");
        let timer_id = TimerId(state.next_timer_id);
        state.next_timer_id += 1;
        state.timers.push(ScheduledTimer {
            timer_id,
            at,
            target,
        });

        timer_id
    }

    fn cancel_timer(&mut self, timer: TimerId) -> bool {
        let mut state = self.time.inner.lock().expect("sim time lock poisoned");
        let before = state.timers.len();
        state.timers.retain(|scheduled| scheduled.timer_id != timer);

        state.timers.len() != before
    }
}

struct SimEntropy {
    state: u64,
}

impl SimEntropy {
    fn new(seed: u64, worker: WorkerId) -> Self {
        let worker_offset = (worker.0 as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);

        Self {
            state: seed ^ worker_offset,
        }
    }
}

impl Entropy for SimEntropy {
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn fill_bytes(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

pub(crate) struct SimEnv {
    observability: NoopObservability,
    clock: SimClock,
    entropy: SimEntropy,
    timers: SimTimers,
    fs: StubFs,
    net: StubNet,
    object_store: StubObjectStore,
}

impl SimEnv {
    pub(crate) fn new(worker: WorkerId, seed: u64, time: SimTime) -> Result<Self, Error> {
        Ok(Self {
            observability: NoopObservability,
            clock: SimClock { time: time.clone() },
            entropy: SimEntropy::new(seed, worker),
            timers: SimTimers { time },
            fs: StubFs,
            net: StubNet,
            object_store: StubObjectStore,
        })
    }
}

impl Env for SimEnv {
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
    use std::time::Duration;

    use crate::{Actor, Env, Error, OpId, TimerCompletion};

    use super::{Runtime, SimRuntime};
    use crate::worker::{DeferredResponse, WorkerShardCtx};

    struct Increment;

    struct CounterActor {
        value: u64,
    }

    struct RandomNumbers {
        count: usize,
    }

    struct RandomNumbersActor;

    enum SleepForTimestampMsg {
        Start,
        TimerFired(TimerCompletion),
    }

    struct SleepForTimestampActor;

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

    impl Actor<WorkerShardCtx> for RandomNumbersActor {
        type Msg = RandomNumbers;
        type Reply = Vec<u64>;

        fn handle(
            &mut self,
            msg: Self::Msg,
            _ctx: &mut WorkerShardCtx,
            env: &mut dyn Env,
        ) -> Result<Self::Reply, Error> {
            Ok((0..msg.count).map(|_| env.entropy().next_u64()).collect())
        }
    }

    impl Actor<WorkerShardCtx> for SleepForTimestampActor {
        type Msg = SleepForTimestampMsg;
        type Reply = DeferredResponse;

        fn handle(
            &mut self,
            msg: Self::Msg,
            ctx: &mut WorkerShardCtx,
            env: &mut dyn Env,
        ) -> Result<Self::Reply, Error> {
            match msg {
                SleepForTimestampMsg::Start => {
                    let wake_at = env.clock().now() + Duration::from_secs(5 * 60);
                    let timer_id = env
                        .timers()
                        .sleep_until(wake_at, ctx.completion_target(OpId));

                    Ok(DeferredResponse::Timer(timer_id))
                }
                SleepForTimestampMsg::TimerFired(completion) => {
                    let _ = completion;

                    Ok(DeferredResponse::Ready(Box::new(
                        env.clock().unix_timestamp(),
                    )))
                }
            }
        }

        fn timer_fired(completion: TimerCompletion) -> Option<Self::Msg> {
            Some(SleepForTimestampMsg::TimerFired(completion))
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

    #[test]
    fn sim_runtime_entropy_is_deterministic_from_seed() {
        let mut runtime = SimRuntime::builder()
            .seed(12345)
            .workers(1)
            .build()
            .unwrap();
        let worker = runtime.workers()[0].clone();
        let actor_ref = runtime.register_actor(&worker, RandomNumbersActor).unwrap();
        let call = runtime
            .call(&worker, actor_ref.id, Box::new(RandomNumbers { count: 4 }))
            .unwrap();

        runtime.run_until_idle().unwrap();

        let values = *call.wait().unwrap().downcast::<Vec<u64>>().unwrap();

        assert_eq!(
            values,
            vec![
                2_454_886_589_211_414_944,
                3_778_200_017_661_327_597,
                2_205_171_434_679_333_405,
                3_248_800_117_070_709_450,
            ]
        );
    }

    #[test]
    fn sim_runtime_fast_forwards_synthetic_time_for_timer_backed_call() {
        let start_time = 1_700_000_000;
        let partial_wait = Duration::from_secs(2 * 60);
        let remaining_wait = Duration::from_secs(3 * 60);
        let total_wait = partial_wait + remaining_wait;
        let mut runtime = SimRuntime::builder()
            .seed(12345)
            .workers(1)
            .start_time(start_time)
            .build()
            .unwrap();
        let worker = runtime.workers()[0].clone();
        let actor_ref = runtime
            .register_actor(&worker, SleepForTimestampActor)
            .unwrap();
        let call = runtime
            .call(&worker, actor_ref.id, Box::new(SleepForTimestampMsg::Start))
            .unwrap();

        runtime.run_until_idle().unwrap();
        assert!(call.try_wait().unwrap().is_none());

        runtime.fast_forward(partial_wait).unwrap();
        runtime.run_until_idle().unwrap();
        assert!(call.try_wait().unwrap().is_none());

        runtime.fast_forward(remaining_wait).unwrap();
        runtime.run_until_idle().unwrap();

        let response = call.try_wait().unwrap().expect("timer response");
        let timestamp = *response.downcast::<u64>().unwrap();

        assert_eq!(timestamp, start_time + total_wait.as_secs());
    }
}
