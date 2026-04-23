use std::time::Instant;

pub trait Observability {
    fn enabled(&self, _meta: ObsMeta) -> bool {
        panic!("Observability::enabled stub")
    }

    fn emit(&mut self, _event: ObsEvent) {
        panic!("Observability::emit stub")
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ObsMeta {
    pub level: ObsLevel,
    pub target: &'static str,
    pub kind: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ObsLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObsEvent {
    pub meta: ObsMeta,
    pub timestamp: Instant,
    pub attributes: ObsFields,
    pub context: Option<ObsContext>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ObsContext {
    pub trace_id: Option<TraceId>,
    pub span_id: Option<SpanId>,
    pub parent_span_id: Option<SpanId>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TraceId;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SpanId;

#[derive(Clone, Debug, PartialEq)]
pub struct ObsFields {
    pub fields: Vec<ObsField>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ObsField {
    pub key: &'static str,
    pub value: ObsValue,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObsValue {
    Str(String),
    StaticStr(&'static str),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

impl From<String> for ObsValue {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<&'static str> for ObsValue {
    fn from(value: &'static str) -> Self {
        Self::StaticStr(value)
    }
}

impl From<i64> for ObsValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u64> for ObsValue {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<f64> for ObsValue {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<bool> for ObsValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

pub struct NoopObservability;

impl Observability for NoopObservability {
    fn enabled(&self, _meta: ObsMeta) -> bool {
        false
    }

    fn emit(&mut self, _event: ObsEvent) {}
}

#[macro_export]
macro_rules! obs_fields {
    () => {
        $crate::ObsFields {
            fields: std::vec::Vec::new(),
        }
    };
    ($($key:ident = $value:expr),+ $(,)?) => {
        $crate::ObsFields {
            fields: vec![
                $(
                    $crate::ObsField {
                        key: stringify!($key),
                        value: $crate::ObsValue::from($value),
                    }
                ),+
            ],
        }
    };
}

#[macro_export]
macro_rules! observe {
    (
        $env:expr,
        level: $level:expr,
        target: $target:expr,
        kind: $kind:expr,
        { $($fields:tt)* }
    ) => {{
        #[cfg(feature = "observability")]
        {
            let meta = $crate::ObsMeta {
                level: $level,
                target: $target,
                kind: $kind,
            };

            if $env.observability().enabled(meta) {
                let timestamp = $env.clock().now();

                $env.observability().emit($crate::ObsEvent {
                    meta,
                    timestamp,
                    attributes: $crate::obs_fields! { $($fields)* },
                    context: None,
                });
            }
        }
    }};
}
