use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use crate::{
    JsHostServiceCallRecord, JsHostServiceRequest, JsHostServiceResponse, JsSubstrateError,
};

#[async_trait(?Send)]
pub trait JsHostServices: Send + Sync {
    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError>;
    async fn calls(&self) -> Vec<JsHostServiceCallRecord>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum DeterministicJsServiceOutcome {
    Response {
        result: JsonValue,
        metadata: BTreeMap<String, JsonValue>,
    },
    Denied {
        message: String,
    },
    Unavailable,
}

#[derive(Clone, Debug, Default)]
pub struct DeterministicJsHostServices {
    pub(crate) gate: Arc<Mutex<()>>,
    pub(crate) outcomes: Arc<StdMutex<BTreeMap<(String, String), DeterministicJsServiceOutcome>>>,
    pub(crate) calls: Arc<StdMutex<Vec<JsHostServiceCallRecord>>>,
}

impl DeterministicJsHostServices {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_outcome(
        &self,
        service: impl Into<String>,
        operation: impl Into<String>,
        outcome: DeterministicJsServiceOutcome,
    ) {
        let _guard = self.gate.lock().await;
        self.outcomes
            .lock()
            .expect("deterministic host outcomes mutex poisoned")
            .insert((service.into(), operation.into()), outcome);
    }
}

#[async_trait(?Send)]
impl JsHostServices for DeterministicJsHostServices {
    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        let _guard = self.gate.lock().await;
        let key = (request.service.clone(), request.operation.clone());
        let outcome = self
            .outcomes
            .lock()
            .expect("deterministic host outcomes mutex poisoned")
            .get(&key)
            .cloned()
            .unwrap_or(DeterministicJsServiceOutcome::Unavailable);
        match outcome {
            DeterministicJsServiceOutcome::Response { result, metadata } => {
                let response = JsHostServiceResponse {
                    result: Some(result),
                    metadata,
                };
                self.calls
                    .lock()
                    .expect("deterministic host calls mutex poisoned")
                    .push(JsHostServiceCallRecord {
                        service: request.service,
                        operation: request.operation,
                        arguments: request.arguments,
                        result: response.result.clone(),
                        metadata: response.metadata.clone(),
                    });
                Ok(response)
            }
            DeterministicJsServiceOutcome::Denied { message } => {
                Err(JsSubstrateError::HostServiceDenied {
                    service: request.service,
                    operation: request.operation,
                    message,
                })
            }
            DeterministicJsServiceOutcome::Unavailable => {
                Err(JsSubstrateError::HostServiceUnavailable {
                    service: request.service,
                    operation: request.operation,
                })
            }
        }
    }

    async fn calls(&self) -> Vec<JsHostServiceCallRecord> {
        let _guard = self.gate.lock().await;
        self.calls
            .lock()
            .expect("deterministic host calls mutex poisoned")
            .clone()
    }
}
