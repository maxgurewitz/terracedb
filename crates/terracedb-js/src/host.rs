use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use terracedb_vfs::{CreateOptions, MkdirOptions, Stats, VfsFileSystem};
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

#[async_trait(?Send)]
pub trait JsHostServiceAdapter: Send + Sync {
    fn handles_service(&self, service: &str) -> bool;
    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError>;
}

#[derive(Clone, Default)]
pub struct RoutedJsHostServices {
    adapters: Vec<Arc<dyn JsHostServiceAdapter>>,
    calls: Arc<Mutex<Vec<JsHostServiceCallRecord>>>,
}

impl RoutedJsHostServices {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_adapter(mut self, adapter: Arc<dyn JsHostServiceAdapter>) -> Self {
        self.adapters.push(adapter);
        self
    }
}

#[async_trait(?Send)]
impl JsHostServices for RoutedJsHostServices {
    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        for adapter in &self.adapters {
            if !adapter.handles_service(&request.service) {
                continue;
            }
            let response = adapter.call(request.clone()).await?;
            self.calls.lock().await.push(JsHostServiceCallRecord {
                service: request.service,
                operation: request.operation,
                arguments: request.arguments,
                result: response.result.clone(),
                metadata: response.metadata.clone(),
            });
            return Ok(response);
        }
        Err(JsSubstrateError::HostServiceUnavailable {
            service: request.service,
            operation: request.operation,
        })
    }

    async fn calls(&self) -> Vec<JsHostServiceCallRecord> {
        self.calls.lock().await.clone()
    }
}

#[derive(Clone)]
pub struct VfsJsHostServiceAdapter {
    fs: Arc<dyn VfsFileSystem>,
    service_names: Vec<String>,
}

impl VfsJsHostServiceAdapter {
    pub fn new<I, S>(fs: Arc<dyn VfsFileSystem>, service_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            fs,
            service_names: service_names.into_iter().map(Into::into).collect(),
        }
    }
}

#[async_trait(?Send)]
impl JsHostServiceAdapter for VfsJsHostServiceAdapter {
    fn handles_service(&self, service: &str) -> bool {
        self.service_names
            .iter()
            .any(|candidate| candidate == service)
    }

    async fn call(
        &self,
        request: JsHostServiceRequest,
    ) -> Result<JsHostServiceResponse, JsSubstrateError> {
        let args = host_service_arguments(&request.arguments);
        match request.operation.as_str() {
            "readTextFile" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let bytes =
                    self.fs.read_file(&path).await?.ok_or_else(|| {
                        evaluation_error(&request, format!("path not found: {path}"))
                    })?;
                let contents = String::from_utf8(bytes).map_err(|error| {
                    evaluation_error(&request, format!("path {path} is not valid UTF-8: {error}"))
                })?;
                Ok(json_response(JsonValue::String(contents)))
            }
            "writeTextFile" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let contents = required_string_arg(&request, &args, 1, "contents")?;
                self.fs
                    .write_file(
                        &path,
                        contents.into_bytes(),
                        CreateOptions {
                            create_parents: true,
                            overwrite: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(empty_response())
            }
            "readJsonFile" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let bytes =
                    self.fs.read_file(&path).await?.ok_or_else(|| {
                        evaluation_error(&request, format!("path not found: {path}"))
                    })?;
                let contents = String::from_utf8(bytes).map_err(|error| {
                    evaluation_error(&request, format!("path {path} is not valid UTF-8: {error}"))
                })?;
                let value = serde_json::from_str(&contents).map_err(|error| {
                    evaluation_error(&request, format!("json parse failed: {error}"))
                })?;
                Ok(json_response(value))
            }
            "writeJsonFile" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let value = args.get(1).cloned().unwrap_or(JsonValue::Null);
                let encoded = serde_json::to_vec_pretty(&value)?;
                self.fs
                    .write_file(
                        &path,
                        encoded,
                        CreateOptions {
                            create_parents: true,
                            overwrite: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(empty_response())
            }
            "mkdir" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                self.fs
                    .mkdir(
                        &path,
                        MkdirOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(empty_response())
            }
            "readdir" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let entries = self.fs.readdir(&path).await?;
                let json = entries
                    .into_iter()
                    .map(|entry| {
                        serde_json::json!({
                            "name": entry.name,
                            "inode": entry.inode.to_string(),
                            "kind": format!("{:?}", entry.kind).to_lowercase(),
                        })
                    })
                    .collect();
                Ok(json_response(JsonValue::Array(json)))
            }
            "stat" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                let stats = self.fs.stat(&path).await?;
                Ok(json_response(
                    stats.map(stats_to_json).unwrap_or(JsonValue::Null),
                ))
            }
            "unlink" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                self.fs.unlink(&path).await?;
                Ok(empty_response())
            }
            "rmdir" => {
                let path = required_string_arg(&request, &args, 0, "path")?;
                self.fs.rmdir(&path).await?;
                Ok(empty_response())
            }
            "rename" => {
                let from = required_string_arg(&request, &args, 0, "from")?;
                let to = required_string_arg(&request, &args, 1, "to")?;
                self.fs.rename(&from, &to).await?;
                Ok(empty_response())
            }
            "fsync" => {
                let path = optional_string_arg(&args, 0);
                self.fs.fsync(path.as_deref()).await?;
                Ok(empty_response())
            }
            _ => Err(JsSubstrateError::HostServiceUnavailable {
                service: request.service,
                operation: request.operation,
            }),
        }
    }
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

fn host_service_arguments(arguments: &JsonValue) -> Vec<JsonValue> {
    match arguments {
        JsonValue::Null => Vec::new(),
        JsonValue::Array(values) => values.clone(),
        other => vec![other.clone()],
    }
}

fn required_string_arg(
    request: &JsHostServiceRequest,
    args: &[JsonValue],
    index: usize,
    name: &str,
) -> Result<String, JsSubstrateError> {
    let value = args
        .get(index)
        .ok_or_else(|| evaluation_error(request, format!("missing required argument: {name}")))?;
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| evaluation_error(request, format!("argument {name} must be a string")))
}

fn optional_string_arg(args: &[JsonValue], index: usize) -> Option<String> {
    args.get(index)
        .and_then(JsonValue::as_str)
        .map(ToString::to_string)
}

fn empty_response() -> JsHostServiceResponse {
    JsHostServiceResponse {
        result: None,
        metadata: BTreeMap::new(),
    }
}

fn json_response(result: JsonValue) -> JsHostServiceResponse {
    JsHostServiceResponse {
        result: Some(result),
        metadata: BTreeMap::new(),
    }
}

fn stats_to_json(stats: Stats) -> JsonValue {
    serde_json::json!({
        "inode": stats.inode.to_string(),
        "kind": format!("{:?}", stats.kind).to_lowercase(),
        "mode": stats.mode,
        "nlink": stats.nlink,
        "uid": stats.uid,
        "gid": stats.gid,
        "size": stats.size,
        "created_at": stats.created_at.get(),
        "modified_at": stats.modified_at.get(),
        "changed_at": stats.changed_at.get(),
        "accessed_at": stats.accessed_at.get(),
        "rdev": stats.rdev,
    })
}

fn evaluation_error(request: &JsHostServiceRequest, message: String) -> JsSubstrateError {
    JsSubstrateError::EvaluationFailed {
        entrypoint: format!("{}::{}", request.service, request.operation),
        message,
    }
}
