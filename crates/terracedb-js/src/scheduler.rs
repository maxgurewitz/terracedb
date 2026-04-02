use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsTaskQueue {
    ModuleLoader,
    PromiseJobs,
    Timers,
    HostCallbacks,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsScheduledTask {
    pub queue: JsTaskQueue,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_millis: Option<u64>,
}

impl JsScheduledTask {
    pub fn ready(queue: JsTaskQueue, label: impl Into<String>) -> Self {
        Self {
            queue,
            label: label.into(),
            deadline_millis: None,
        }
    }

    pub fn deadline(queue: JsTaskQueue, label: impl Into<String>, deadline_millis: u64) -> Self {
        Self {
            queue,
            label: label.into(),
            deadline_millis: Some(deadline_millis),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsSchedulerSnapshot {
    pub scheduler: String,
    pub scheduled_tasks: Vec<JsScheduledTask>,
}

#[async_trait]
pub trait JsScheduler: Send + Sync {
    fn name(&self) -> &str;
    async fn schedule(&self, task: JsScheduledTask);
    async fn drain(&self) -> Vec<JsScheduledTask>;
    async fn snapshot(&self) -> JsSchedulerSnapshot;
}

#[derive(Clone, Debug)]
pub struct DeterministicJsScheduler {
    pub(crate) name: Arc<str>,
    pub(crate) gate: Arc<Mutex<()>>,
    pub(crate) scheduled_tasks: Arc<StdMutex<Vec<JsScheduledTask>>>,
}

impl DeterministicJsScheduler {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            gate: Arc::new(Mutex::new(())),
            scheduled_tasks: Arc::new(StdMutex::new(Vec::new())),
        }
    }

    pub async fn schedule_deadline(
        &self,
        queue: JsTaskQueue,
        label: impl Into<String>,
        deadline_millis: u64,
    ) {
        self.schedule(JsScheduledTask::deadline(queue, label, deadline_millis))
            .await;
    }

    pub async fn drain_ready(&self, now_millis: u64) -> Vec<JsScheduledTask> {
        let _guard = self.gate.lock().await;
        let mut tasks = self
            .scheduled_tasks
            .lock()
            .expect("deterministic scheduler mutex poisoned");
        let all: Vec<JsScheduledTask> = std::mem::take(tasks.as_mut());
        let (ready, pending): (Vec<_>, Vec<_>) = all.into_iter().partition(|task| {
            task.deadline_millis
                .is_none_or(|deadline| deadline <= now_millis)
        });
        *tasks = pending;
        ready
    }

    pub async fn next_deadline_millis(&self) -> Option<u64> {
        let _guard = self.gate.lock().await;
        self.scheduled_tasks
            .lock()
            .expect("deterministic scheduler mutex poisoned")
            .iter()
            .filter_map(|task| task.deadline_millis)
            .min()
    }
}

impl Default for DeterministicJsScheduler {
    fn default() -> Self {
        Self::new("deterministic-js-scheduler")
    }
}

#[async_trait]
impl JsScheduler for DeterministicJsScheduler {
    fn name(&self) -> &str {
        self.name.as_ref()
    }

    async fn schedule(&self, task: JsScheduledTask) {
        let _guard = self.gate.lock().await;
        self.scheduled_tasks
            .lock()
            .expect("deterministic scheduler mutex poisoned")
            .push(task);
    }

    async fn drain(&self) -> Vec<JsScheduledTask> {
        let _guard = self.gate.lock().await;
        let mut tasks = self
            .scheduled_tasks
            .lock()
            .expect("deterministic scheduler mutex poisoned");
        std::mem::take(tasks.as_mut())
    }

    async fn snapshot(&self) -> JsSchedulerSnapshot {
        let _guard = self.gate.lock().await;
        JsSchedulerSnapshot {
            scheduler: self.name.to_string(),
            scheduled_tasks: self
                .scheduled_tasks
                .lock()
                .expect("deterministic scheduler mutex poisoned")
                .clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeterministicJsScheduler, JsScheduledTask, JsScheduler, JsTaskQueue};

    #[tokio::test]
    async fn deterministic_scheduler_drains_ready_tasks_by_deadline() {
        let scheduler = DeterministicJsScheduler::default();
        JsScheduler::schedule(
            &scheduler,
            JsScheduledTask::deadline(JsTaskQueue::Timers, "late", 200),
        )
        .await;
        JsScheduler::schedule(
            &scheduler,
            JsScheduledTask::ready(JsTaskQueue::PromiseJobs, "promise"),
        )
        .await;
        scheduler
            .schedule_deadline(JsTaskQueue::Timers, "early", 100)
            .await;

        assert_eq!(scheduler.next_deadline_millis().await, Some(100));
        assert_eq!(
            scheduler.drain_ready(0).await,
            vec![JsScheduledTask::ready(JsTaskQueue::PromiseJobs, "promise")]
        );
        assert_eq!(
            scheduler.drain_ready(100).await,
            vec![JsScheduledTask::deadline(JsTaskQueue::Timers, "early", 100)]
        );
        assert_eq!(scheduler.next_deadline_millis().await, Some(200));
        assert_eq!(
            scheduler.drain_ready(200).await,
            vec![JsScheduledTask::deadline(JsTaskQueue::Timers, "late", 200)]
        );
        assert!(JsScheduler::drain(&scheduler).await.is_empty());
    }
}
