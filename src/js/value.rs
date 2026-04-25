#[derive(Debug, Clone, PartialEq)]
pub enum JsValue {
    Number(f64),
    Bool(bool),
    String(String),
    Null,
    Undefined,
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
        }
    }
}
