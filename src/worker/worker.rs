use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    Actor, ActorId, ActorRef, CompletionTarget, Env, Error, FsCompletion, LocalActorId,
    LocalActorRef, NetCompletion, ObjectCompletion, OpId, ReplyTo, RequestId, ShardCtx,
    TimerCompletion, TimerId, WorkerHandle, WorkerId,
};

use super::{
    ErasedActorMsg, ErasedResponse, PendingResponse, WorkerInbox, WorkerMsg,
    actor_registry::ActorRegistry,
    message::{DeferredResponse, WorkerEnvelope},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ConnectionHandle;

#[allow(dead_code)]
pub(crate) trait Worker {
    fn id(&self) -> WorkerId;

    fn enqueue(&mut self, msg: WorkerMsg);

    fn register_erased_actor(
        &mut self,
        actor: ActorId,
        handler: Box<dyn super::ErasedActor<WorkerShardCtx>>,
    );

    fn register_request(&mut self, request_id: RequestId, conn: ConnectionHandle);
}

pub(super) struct WorkerCore {
    id: WorkerId,
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    inbox: WorkerInbox,
    pending_requests: HashMap<RequestId, PendingResponse>,
    pending_timer_requests: HashMap<TimerId, RequestId>,
}

impl WorkerCore {
    fn new(id: WorkerId) -> Result<(Self, WorkerHandle), Error> {
        let inbox = WorkerInbox::new();
        let handle = WorkerHandle::new(id, inbox.ingress());
        let worker = Self {
            id,
            registry: Arc::new(Mutex::new(ActorRegistry::new(id))),
            inbox,
            pending_requests: HashMap::new(),
            pending_timer_requests: HashMap::new(),
        };

        Ok((worker, handle))
    }

    pub(super) fn register_actor<A>(&mut self, actor: A) -> ActorRef<A>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        self.registry
            .lock()
            .expect("actor registry lock poisoned")
            .register(actor)
    }

    #[allow(dead_code)]
    fn enqueue(&mut self, msg: WorkerMsg) {
        let _ = self.inbox.enqueue(msg);
    }

    #[allow(dead_code)]
    fn register_erased_actor(
        &mut self,
        actor: ActorId,
        handler: Box<dyn super::ErasedActor<WorkerShardCtx>>,
    ) {
        self.registry
            .lock()
            .expect("actor registry lock poisoned")
            .insert_erased(actor, handler);
    }

    #[allow(dead_code)]
    fn register_request(&mut self, request_id: RequestId, conn: ConnectionHandle) {
        self.pending_requests
            .insert(request_id, PendingResponse::Connection(conn));
    }

    fn process_one_message(&mut self, env: &mut dyn Env) -> Result<bool, Error> {
        let Some(msg) = self.inbox.try_recv()? else {
            return Ok(false);
        };

        let _ = self.handle_envelope(msg, env)?;
        Ok(true)
    }

    fn handle_envelope(
        &mut self,
        envelope: WorkerEnvelope,
        env: &mut dyn Env,
    ) -> Result<bool, Error> {
        match envelope {
            WorkerEnvelope::RegisterActor { registration } => {
                registration.register(self)?;
                Ok(false)
            }
            WorkerEnvelope::Msg(msg) => self.handle_msg(msg, env),
        }
    }

    fn handle_msg(&mut self, msg: WorkerMsg, env: &mut dyn Env) -> Result<bool, Error> {
        match msg {
            WorkerMsg::ToActor { actor, msg } => {
                let _ = self.dispatch_actor(actor, msg, env)?;
            }
            WorkerMsg::HostRequest {
                request_id,
                actor,
                msg,
                reply_to,
            } => {
                self.pending_requests
                    .insert(request_id, PendingResponse::Host(reply_to));

                let response = self.dispatch_actor(actor, msg, env)?;
                self.handle_host_response(request_id, response)?;
            }
            WorkerMsg::RequestDone {
                request_id,
                response,
            } => {
                self.finish_request(request_id, response)?;
            }
            WorkerMsg::NetCompletion(completion) => {
                self.dispatch_completion(Completion::Net(completion), env)?;
            }
            WorkerMsg::FsCompletion(completion) => {
                self.dispatch_completion(Completion::Fs(completion), env)?;
            }
            WorkerMsg::ObjectStoreCompletion(completion) => {
                self.dispatch_completion(Completion::ObjectStore(completion), env)?;
            }
            WorkerMsg::TimerFired { timer_id, target } => {
                self.dispatch_completion(Completion::Timer { timer_id, target }, env)?;
            }
            WorkerMsg::Shutdown => {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn dispatch_actor(
        &mut self,
        actor: ActorId,
        msg: ErasedActorMsg,
        env: &mut dyn Env,
    ) -> Result<ErasedResponse, Error> {
        dispatch_registry_actor(Arc::clone(&self.registry), actor, msg, None, self.id, env)
    }

    fn handle_host_response(
        &mut self,
        request_id: RequestId,
        response: ErasedResponse,
    ) -> Result<(), Error> {
        if response.is::<DeferredResponse>() {
            let response = *response
                .downcast::<DeferredResponse>()
                .map_err(|_| Error::ActorReplyTypeMismatch)?;

            return match response {
                DeferredResponse::Timer(timer_id) => {
                    self.pending_timer_requests.insert(timer_id, request_id);
                    Ok(())
                }
                DeferredResponse::Ready(response) => self.finish_request(request_id, response),
            };
        }

        self.finish_request(request_id, response)
    }

    fn finish_request(
        &mut self,
        request_id: RequestId,
        response: ErasedResponse,
    ) -> Result<(), Error> {
        match self.pending_requests.remove(&request_id) {
            Some(PendingResponse::Host(reply)) => reply.send(Ok(response)),
            Some(PendingResponse::Connection(_conn)) => Ok(()),
            None => Err(Error::HostReplyClosed),
        }
    }

    fn dispatch_completion(
        &mut self,
        _completion: Completion,
        env: &mut dyn Env,
    ) -> Result<(), Error> {
        match _completion {
            Completion::Timer { timer_id, target } => {
                let response = dispatch_registry_timer(
                    Arc::clone(&self.registry),
                    TimerCompletion { timer_id, target },
                    self.id,
                    env,
                )?;
                let request_id = self
                    .pending_timer_requests
                    .remove(&timer_id)
                    .ok_or(Error::HostReplyClosed)?;

                self.handle_host_response(request_id, response)
            }
            completion => {
                completion.touch();
                panic!("Worker::dispatch_completion stub")
            }
        }
    }
}

pub(crate) struct RealWorker {
    core: WorkerCore,
}

impl RealWorker {
    pub(crate) fn new(id: WorkerId) -> Result<(Self, WorkerHandle), Error> {
        let (core, handle) = WorkerCore::new(id)?;

        Ok((Self { core }, handle))
    }

    pub(crate) async fn run(self, env: Box<dyn Env>) -> Result<(), Error> {
        let mut worker = self;
        worker.run_inner(env).await
    }

    async fn run_inner(&mut self, mut env: Box<dyn Env>) -> Result<(), Error> {
        loop {
            let msg = self.core.inbox.recv().await?;

            if self.core.handle_envelope(msg, env.as_mut())? {
                break;
            }
        }

        Ok(())
    }
}

impl Worker for RealWorker {
    fn id(&self) -> WorkerId {
        self.core.id
    }

    fn enqueue(&mut self, msg: WorkerMsg) {
        self.core.enqueue(msg);
    }

    fn register_erased_actor(
        &mut self,
        actor: ActorId,
        handler: Box<dyn super::ErasedActor<WorkerShardCtx>>,
    ) {
        self.core.register_erased_actor(actor, handler);
    }

    fn register_request(&mut self, request_id: RequestId, conn: ConnectionHandle) {
        self.core.register_request(request_id, conn);
    }
}

pub(crate) struct SimWorker {
    core: WorkerCore,
    env: Box<dyn Env>,
}

impl SimWorker {
    pub(crate) fn new(id: WorkerId, env: Box<dyn Env>) -> Result<(Self, WorkerHandle), Error> {
        let (core, handle) = WorkerCore::new(id)?;

        Ok((Self { core, env }, handle))
    }

    pub(crate) fn register_actor<A>(&mut self, actor: A) -> ActorRef<A>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        self.core.register_actor(actor)
    }

    pub(crate) fn process_one_message(&mut self) -> Result<bool, Error> {
        self.core.process_one_message(self.env.as_mut())
    }
}

impl Worker for SimWorker {
    fn id(&self) -> WorkerId {
        self.core.id
    }

    fn enqueue(&mut self, msg: WorkerMsg) {
        self.core.enqueue(msg);
    }

    fn register_erased_actor(
        &mut self,
        actor: ActorId,
        handler: Box<dyn super::ErasedActor<WorkerShardCtx>>,
    ) {
        self.core.register_erased_actor(actor, handler);
    }

    fn register_request(&mut self, request_id: RequestId, conn: ConnectionHandle) {
        self.core.register_request(request_id, conn);
    }
}

pub struct WorkerShardCtx {
    worker_id: WorkerId,
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    current_actor: LocalActorId,
}

impl WorkerShardCtx {
    pub fn current_actor_id(&self) -> ActorId {
        ActorId {
            worker: self.worker_id,
            local: self.current_actor,
        }
    }

    pub fn completion_target(&self, op: OpId) -> CompletionTarget {
        CompletionTarget {
            worker: self.worker_id,
            actor: self.current_actor_id(),
            op,
        }
    }
}

impl ShardCtx for WorkerShardCtx {
    fn worker_id(&self) -> WorkerId {
        self.worker_id
    }

    fn call_local<A: Actor<Self>>(
        &mut self,
        actor: LocalActorRef<A>,
        msg: A::Msg,
        env: &mut dyn Env,
    ) -> Result<A::Reply, Error>
    where
        Self: Sized,
    {
        let actor_id = ActorId {
            worker: self.worker_id,
            local: actor.id,
        };
        let response = dispatch_registry_actor(
            Arc::clone(&self.registry),
            actor_id,
            Box::new(msg),
            Some(self.current_actor),
            self.worker_id,
            env,
        )?;

        response
            .downcast::<A::Reply>()
            .map(|reply| *reply)
            .map_err(|_| Error::ActorReplyTypeMismatch)
    }

    fn send_remote(&mut self, worker: WorkerHandle, msg: WorkerMsg) -> Result<(), Error> {
        worker.submit(msg)
    }

    fn reply(&mut self, reply_to: ReplyTo, response: ErasedResponse) -> Result<(), Error> {
        reply_to.reply(response)
    }
}

enum Completion {
    Net(NetCompletion),
    Fs(FsCompletion),
    ObjectStore(ObjectCompletion),
    Timer {
        timer_id: TimerId,
        target: CompletionTarget,
    },
}

impl Completion {
    fn touch(&self) {
        match self {
            Self::Net(completion) => {
                let _ = completion;
            }
            Self::Fs(completion) => {
                let _ = completion;
            }
            Self::ObjectStore(completion) => {
                let _ = completion;
            }
            Self::Timer { timer_id, target } => {
                let _ = timer_id;
                let _ = target;
            }
        }
    }
}

fn dispatch_registry_actor(
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    actor: ActorId,
    msg: ErasedActorMsg,
    caller: Option<LocalActorId>,
    worker_id: WorkerId,
    env: &mut dyn Env,
) -> Result<ErasedResponse, Error> {
    if actor.worker != worker_id {
        return Err(Error::ActorNotFound { actor });
    }

    if caller == Some(actor.local) {
        return Err(Error::ReentrantActorCall { actor });
    }

    let mut entry = {
        let mut registry = registry.lock().map_err(|_| Error::ActorRegistryPoisoned)?;
        registry
            .remove_entry(actor.local)
            .ok_or(Error::ActorNotFound { actor })?
    };

    let mut ctx = WorkerShardCtx {
        worker_id,
        registry: Arc::clone(&registry),
        current_actor: actor.local,
    };
    let result = entry.actor.handle_erased(msg, &mut ctx, env);

    registry
        .lock()
        .map_err(|_| Error::ActorRegistryPoisoned)?
        .insert_entry(actor.local, entry);

    result
}

fn dispatch_registry_timer(
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    completion: TimerCompletion,
    worker_id: WorkerId,
    env: &mut dyn Env,
) -> Result<ErasedResponse, Error> {
    let actor = completion.target.actor;

    if actor.worker != worker_id {
        return Err(Error::ActorNotFound { actor });
    }

    let mut entry = {
        let mut registry = registry.lock().map_err(|_| Error::ActorRegistryPoisoned)?;
        registry
            .remove_entry(actor.local)
            .ok_or(Error::ActorNotFound { actor })?
    };

    let mut ctx = WorkerShardCtx {
        worker_id,
        registry: Arc::clone(&registry),
        current_actor: actor.local,
    };
    let result = entry.actor.handle_timer_erased(completion, &mut ctx, env);

    registry
        .lock()
        .map_err(|_| Error::ActorRegistryPoisoned)?
        .insert_entry(actor.local, entry);

    result
}
