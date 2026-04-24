use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    Actor, ActorId, ActorRef, Env, Error, FsCompletion, LocalActorId, LocalActorRef, NetCompletion,
    ObjectCompletion, ReplyTo, RequestId, ShardCtx, TimerId, WorkerHandle, WorkerId,
};

use super::{
    ErasedActorMsg, ErasedResponse, PendingResponse, WorkerInbox, WorkerMsg,
    actor_registry::ActorRegistry,
};

pub(crate) struct Worker {
    id: WorkerId,
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    inbox: WorkerInbox,
    pending_requests: HashMap<RequestId, PendingResponse>,
    env: Box<dyn Env>,
}

impl Worker {
    pub(crate) fn new(id: WorkerId, env: Box<dyn Env>) -> Result<(Self, WorkerHandle), Error> {
        let inbox = WorkerInbox::new();
        let handle = WorkerHandle::new(id, inbox.ingress());
        let worker = Self {
            id,
            registry: Arc::new(Mutex::new(ActorRegistry::new(id))),
            inbox,
            pending_requests: HashMap::new(),
            env,
        };

        Ok((worker, handle))
    }

    pub(crate) fn handle(&self) -> WorkerHandle {
        WorkerHandle::new(self.id, self.inbox.ingress())
    }

    pub(crate) fn register_actor<A>(&mut self, actor: A) -> ActorRef<A>
    where
        A: Actor<WorkerShardCtx> + Send + 'static,
    {
        self.registry
            .lock()
            .expect("actor registry lock poisoned")
            .register(actor)
    }

    pub(crate) async fn run(mut self) -> Result<(), Error> {
        loop {
            let msg = self.inbox.recv().await?;

            match msg {
                WorkerMsg::ToActor { actor, msg } => {
                    let _ = self.dispatch_actor(actor, msg)?;
                }
                WorkerMsg::HostRequest {
                    request_id,
                    actor,
                    msg,
                    reply_to,
                } => {
                    self.pending_requests
                        .insert(request_id, PendingResponse::Host(reply_to));

                    let response = self.dispatch_actor(actor, msg)?;
                    self.finish_request(request_id, response)?;
                }
                WorkerMsg::RequestDone {
                    request_id,
                    response,
                } => {
                    self.finish_request(request_id, response)?;
                }
                WorkerMsg::NetCompletion(completion) => {
                    self.dispatch_completion(Completion::Net(completion))?;
                }
                WorkerMsg::FsCompletion(completion) => {
                    self.dispatch_completion(Completion::Fs(completion))?;
                }
                WorkerMsg::ObjectStoreCompletion(completion) => {
                    self.dispatch_completion(Completion::ObjectStore(completion))?;
                }
                WorkerMsg::TimerFired(timer) => {
                    self.dispatch_completion(Completion::Timer(timer))?;
                }
                WorkerMsg::Shutdown => {
                    break;
                }
            }
        }

        Ok(())
    }

    fn dispatch_actor(
        &mut self,
        actor: ActorId,
        msg: ErasedActorMsg,
    ) -> Result<ErasedResponse, Error> {
        dispatch_registry_actor(
            Arc::clone(&self.registry),
            actor,
            msg,
            None,
            self.id,
            self.env.as_mut(),
        )
    }

    fn finish_request(
        &mut self,
        request_id: RequestId,
        response: ErasedResponse,
    ) -> Result<(), Error> {
        match self.pending_requests.remove(&request_id) {
            Some(PendingResponse::Host(reply)) => reply.send(Ok(response)),
            None => Err(Error::HostReplyClosed),
        }
    }

    fn dispatch_completion(&mut self, _completion: Completion) -> Result<(), Error> {
        panic!("Worker::dispatch_completion stub")
    }
}

pub(crate) struct WorkerShardCtx {
    worker_id: WorkerId,
    registry: Arc<Mutex<ActorRegistry<WorkerShardCtx>>>,
    current_actor: LocalActorId,
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
    Timer(TimerId),
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
