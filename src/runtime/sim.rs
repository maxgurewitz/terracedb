use std::time::Duration;

use crate::worker::WorkerShardCtx;
use crate::{Actor, ActorId, ActorRef, ErasedActorMsg, Error, WorkerHandle, WorkerId, WorkerMsg};

use super::{RuntimeApi, SimEnv, SimTime};

pub struct SimRuntime {
    workers: Vec<WorkerHandle>,
    scheduler: SimScheduler,
    time: SimTime,
    seed: u64,
}

impl SimRuntime {
    pub fn builder() -> SimRuntimeBuilder {
        SimRuntimeBuilder {
            seed: None,
            workers: None,
            start_time: None,
        }
    }

    pub fn workers(&self) -> &[WorkerHandle] {
        &self.workers
    }

    pub fn call(
        &self,
        worker: &WorkerHandle,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<crate::CallHandle, Error> {
        super::submit_host_call(worker, actor, msg)
    }

    pub fn register_actor<A>(
        &mut self,
        worker: &WorkerHandle,
        actor: A,
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        self.scheduler.register_actor(worker.id(), actor)
    }

    pub fn run_until_idle(&mut self) -> Result<(), Error> {
        self.scheduler.run_until_idle()
    }

    pub fn run_steps(&mut self, steps: usize) -> Result<(), Error> {
        self.scheduler.run_steps(steps)
    }

    pub fn fast_forward(&mut self, duration: Duration) -> Result<(), Error> {
        for timer in self.time.fast_forward(duration) {
            let worker_index = timer.target.worker.0;
            let worker = self
                .workers
                .get(worker_index)
                .ok_or(Error::InvalidWorkerIndex {
                    worker: worker_index,
                })?;

            worker.submit(WorkerMsg::TimerFired {
                timer_id: timer.timer_id,
                target: timer.target,
            })?;
        }

        Ok(())
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl RuntimeApi for SimRuntime {
    fn workers(&self) -> &[WorkerHandle] {
        self.workers()
    }

    fn call(
        &self,
        worker: &WorkerHandle,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<crate::CallHandle, Error> {
        self.call(worker, actor, msg)
    }
}

pub struct SimRuntimeBuilder {
    seed: Option<u64>,
    workers: Option<usize>,
    start_time: Option<u64>,
}

impl SimRuntimeBuilder {
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = Some(workers);
        self
    }

    pub fn start_time(mut self, timestamp: u64) -> Self {
        self.start_time = Some(timestamp);
        self
    }

    pub fn build(self) -> Result<SimRuntime, Error> {
        let seed = self.seed.ok_or(Error::MissingSimulationSeed)?;
        let worker_count = self.workers.ok_or(Error::MissingSimulationWorkerCount)?;
        let start_time = self.start_time.unwrap_or(0);

        if worker_count == 0 {
            return Err(Error::NoWorkers);
        }

        build_sim_runtime(seed, worker_count, start_time)
    }
}

fn build_sim_runtime(seed: u64, worker_count: usize, start_time: u64) -> Result<SimRuntime, Error> {
    let time = SimTime::new(start_time);
    let mut workers = Vec::with_capacity(worker_count);
    let mut sim_workers = Vec::with_capacity(worker_count);

    for index in 0..worker_count {
        let worker_id = WorkerId(index);
        let env = Box::new(SimEnv::new(worker_id, seed, time.clone())?);
        let (worker, worker_handle) = crate::worker::SimWorker::new(worker_id, env)?;
        let _ = crate::worker::Worker::id(&worker);

        workers.push(worker_handle);
        sim_workers.push(worker);
    }

    let scheduler = SimScheduler::new(seed, sim_workers)?;

    Ok(SimRuntime {
        workers,
        scheduler,
        time,
        seed,
    })
}

struct SimScheduler {
    #[allow(dead_code)]
    seed: u64,
    workers: Vec<crate::worker::SimWorker>,
}

impl SimScheduler {
    fn new(seed: u64, workers: Vec<crate::worker::SimWorker>) -> Result<Self, Error> {
        Ok(Self { seed, workers })
    }

    fn run_until_idle(&mut self) -> Result<(), Error> {
        loop {
            let mut made_progress = false;

            for worker in &mut self.workers {
                made_progress |= worker.process_one_message()?;
            }

            if !made_progress {
                return Ok(());
            }
        }
    }

    fn run_steps(&mut self, steps: usize) -> Result<(), Error> {
        if self.workers.is_empty() {
            return Ok(());
        }

        for step in 0..steps {
            let worker = step % self.workers.len();
            let _ = self.workers[worker].process_one_message()?;
        }

        Ok(())
    }

    fn register_actor<A>(&mut self, worker: WorkerId, actor: A) -> Result<ActorRef<A>, Error>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        let worker_index = worker.0;
        let worker = self
            .workers
            .get_mut(worker_index)
            .ok_or(Error::InvalidWorkerIndex {
                worker: worker_index,
            })?;

        Ok(worker.register_actor(actor))
    }
}
