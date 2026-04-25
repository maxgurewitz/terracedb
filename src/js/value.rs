use super::ObjectId;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub enum JsValue {
    Number(f64),
    Bool(bool),
    String(String),
    Null,
    Undefined,
    Object(ObjectId),
}

impl JsValue {
    pub(crate) fn stringify(&self) -> String {
        match self {
            Self::Number(value) if value.is_finite() && value.fract() == 0.0 => {
                format!("{value:.0}")
            }
            Self::Number(value) => value.to_string(),
            Self::Bool(value) => value.to_string(),
            Self::String(value) => value.clone(),
            Self::Null => "null".to_owned(),
            Self::Undefined => "undefined".to_owned(),
            Self::Object(_) => "[object Object]".to_owned(),
        }
    }
}
