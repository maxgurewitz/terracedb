use std::{borrow::Cow, path::Path, sync::Arc};

use opentelemetry::{
    Context, KeyValue, global,
    propagation::{Extractor, Injector},
    trace::TraceContextExt,
};
use serde::{Deserialize, Serialize};
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::StorageConfig;
use crate::{SequenceNumber, Timestamp};

pub mod telemetry_attrs {
    pub const DB_NAME: &str = "terracedb.db.name";
    pub const DB_INSTANCE: &str = "terracedb.db.instance";
    pub const STORAGE_MODE: &str = "terracedb.storage.mode";
    pub const TABLE: &str = "terracedb.table";
    pub const PHYSICAL_SHARD: &str = "terracedb.physical_shard";
    pub const SEQUENCE: &str = "terracedb.sequence";
    pub const DURABLE_SEQUENCE: &str = "terracedb.durable_sequence";
    pub const LOG_CURSOR: &str = "terracedb.log_cursor";
    pub const OPERATION: &str = "terracedb.operation";
    pub const WORK_KIND: &str = "terracedb.work.kind";
    pub const SOURCE_TABLE: &str = "terracedb.source.table";
    pub const PROJECTION_NAME: &str = "terracedb.projection.name";
    pub const PROJECTION_MODE: &str = "terracedb.projection.mode";
    pub const PROJECTION_SOURCE_COUNT: &str = "terracedb.projection.source_count";
    pub const PROJECTION_OUTPUT_COUNT: &str = "terracedb.projection.output_count";
    pub const PROJECTION_OPERATION_COUNT: &str = "terracedb.projection.operation_count";
    pub const WORKFLOW_NAME: &str = "terracedb.workflow.name";
    pub const WORKFLOW_INSTANCE_ID: &str = "terracedb.workflow.instance_id";
    pub const WORKFLOW_TRIGGER_KIND: &str = "terracedb.workflow.trigger.kind";
    pub const WORKFLOW_TRIGGER_SEQ: &str = "terracedb.workflow.trigger.seq";
    pub const TIMER_ID: &str = "terracedb.timer.id";
    pub const CALLBACK_ID: &str = "terracedb.callback.id";
    pub const OUTBOX_ID: &str = "terracedb.outbox.id";
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanRelation {
    Parent,
    Link,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationContext {
    pub(crate) traceparent: Option<String>,
    pub(crate) tracestate: Option<String>,
}

impl OperationContext {
    pub fn current() -> Self {
        Self::from_span(&Span::current())
    }

    pub fn from_span(span: &Span) -> Self {
        let context = span.context();
        if !context.span().span_context().is_valid() {
            return Self::default();
        }

        let mut carrier = PropagationCarrier::default();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut carrier);
        });

        Self {
            traceparent: carrier.traceparent,
            tracestate: carrier.tracestate,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
    }

    pub fn traceparent(&self) -> Option<&str> {
        self.traceparent.as_deref()
    }

    pub fn tracestate(&self) -> Option<&str> {
        self.tracestate.as_deref()
    }

    pub fn context(&self) -> Context {
        if self.is_empty() {
            return Context::new();
        }

        let carrier = PropagationCarrier {
            traceparent: self.traceparent.clone(),
            tracestate: self.tracestate.clone(),
        };
        global::get_text_map_propagator(|propagator| propagator.extract(&carrier))
    }

    pub fn attach_to_span(&self, span: &Span, relation: SpanRelation) -> bool {
        let context = self.context();
        let span_ref = context.span();
        let span_context = span_ref.span_context();
        if !span_context.is_valid() {
            return false;
        }

        match relation {
            SpanRelation::Parent => {
                let _ = span.set_parent(context);
            }
            SpanRelation::Link => {
                span.add_link(span_context.clone());
            }
        }

        true
    }
}

pub fn set_span_attribute(
    span: &Span,
    key: impl Into<opentelemetry::Key>,
    value: impl IntoTelemetryValue,
) {
    span.set_attribute(key, value.into_telemetry_value());
}

pub fn set_span_attributes<K, V>(span: &Span, attributes: impl IntoIterator<Item = (K, V)>)
where
    K: Into<opentelemetry::Key>,
    V: IntoTelemetryValue,
{
    for (key, value) in attributes {
        span.set_attribute(key, value.into_telemetry_value());
    }
}

pub fn add_span_event(
    span: &Span,
    name: impl Into<std::borrow::Cow<'static, str>>,
    attributes: Vec<KeyValue>,
) {
    span.add_event(name, attributes);
}

pub trait IntoTelemetryValue {
    fn into_telemetry_value(self) -> opentelemetry::Value;
}

impl IntoTelemetryValue for opentelemetry::Value {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self
    }
}

impl IntoTelemetryValue for bool {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.into()
    }
}

impl IntoTelemetryValue for i64 {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.into()
    }
}

impl IntoTelemetryValue for i32 {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        i64::from(self).into()
    }
}

impl IntoTelemetryValue for u64 {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        match i64::try_from(self) {
            Ok(value) => value.into(),
            Err(_) => self.to_string().into(),
        }
    }
}

impl IntoTelemetryValue for usize {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        (self as u64).into_telemetry_value()
    }
}

impl IntoTelemetryValue for u32 {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        u64::from(self).into_telemetry_value()
    }
}

impl IntoTelemetryValue for String {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.into()
    }
}

impl IntoTelemetryValue for &str {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.to_string().into()
    }
}

impl IntoTelemetryValue for Arc<str> {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.into()
    }
}

impl IntoTelemetryValue for Cow<'_, str> {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.into_owned().into()
    }
}

impl IntoTelemetryValue for SequenceNumber {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.get().into_telemetry_value()
    }
}

impl IntoTelemetryValue for Timestamp {
    fn into_telemetry_value(self) -> opentelemetry::Value {
        self.get().into_telemetry_value()
    }
}

#[derive(Clone, Debug, Default)]
struct PropagationCarrier {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl Injector for PropagationCarrier {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" => self.traceparent = Some(value),
            "tracestate" => self.tracestate = Some(value),
            _ => {}
        }
    }
}

impl Extractor for PropagationCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut keys = Vec::new();
        if self.traceparent.is_some() {
            keys.push("traceparent");
        }
        if self.tracestate.is_some() {
            keys.push("tracestate");
        }
        keys
    }
}

pub fn storage_mode_name(storage: &StorageConfig) -> &'static str {
    match storage {
        StorageConfig::Tiered(_) => "tiered",
        StorageConfig::S3Primary(_) => "s3_primary",
    }
}

pub fn db_name_from_storage(storage: &StorageConfig) -> String {
    match storage {
        StorageConfig::Tiered(config) => Path::new(&config.ssd.path)
            .file_name()
            .and_then(|segment| segment.to_str())
            .filter(|segment| !segment.is_empty())
            .unwrap_or(config.ssd.path.as_str())
            .to_string(),
        StorageConfig::S3Primary(config) => config
            .s3
            .prefix
            .rsplit('/')
            .find(|segment| !segment.is_empty())
            .unwrap_or(config.s3.bucket.as_str())
            .to_string(),
    }
}

pub fn db_instance_from_storage(storage: &StorageConfig) -> String {
    match storage {
        StorageConfig::Tiered(config) => config.ssd.path.clone(),
        StorageConfig::S3Primary(config) => {
            format!(
                "{}/{}",
                config.s3.bucket,
                config.s3.prefix.trim_matches('/')
            )
        }
    }
}
