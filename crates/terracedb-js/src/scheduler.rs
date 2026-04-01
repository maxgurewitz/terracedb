use std::sync::Arc;

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
    name: Arc<str>,
    scheduled_tasks: Arc<Mutex<Vec<JsScheduledTask>>>,
}

impl DeterministicJsScheduler {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            scheduled_tasks: Arc::new(Mutex::new(Vec::new())),
        }
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
        self.scheduled_tasks.lock().await.push(task);
    }

    async fn drain(&self) -> Vec<JsScheduledTask> {
        let mut tasks = self.scheduled_tasks.lock().await;
        std::mem::take(tasks.as_mut())
    }

    async fn snapshot(&self) -> JsSchedulerSnapshot {
        JsSchedulerSnapshot {
            scheduler: self.name.to_string(),
            scheduled_tasks: self.scheduled_tasks.lock().await.clone(),
        }
    }
}
