use super::ObjectId;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
pub enum ValueTag {
    Number,
    Bool,
    String,
    Null,
    Undefined,
    Object,
}

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

#[derive(Debug, Clone, Deserialize, PartialEq, Serialize)]
pub struct ValueCols {
    pub value_tag: Vec<ValueTag>,
    pub value_number: Vec<f64>,
    pub value_bool: Vec<bool>,
    pub value_string: Vec<String>,
    pub value_object: Vec<ObjectId>,
}

impl ValueCols {
    pub fn new() -> Self {
        Self {
            value_tag: Vec::new(),
            value_number: Vec::new(),
            value_bool: Vec::new(),
            value_string: Vec::new(),
            value_object: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.value_tag.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value_tag.is_empty()
    }

    pub fn push(&mut self, value: JsValue) -> u32 {
        let slot = self.len() as u32;
        self.push_raw(value);
        slot
    }

    pub fn pop(&mut self) -> Option<JsValue> {
        if self.is_empty() {
            return None;
        }

        let index = self.len() - 1;
        let value = self.get(index as u32);
        self.value_tag.pop();
        self.value_number.pop();
        self.value_bool.pop();
        self.value_string.pop();
        self.value_object.pop();
        Some(value)
    }

    pub fn truncate(&mut self, len: usize) {
        self.value_tag.truncate(len);
        self.value_number.truncate(len);
        self.value_bool.truncate(len);
        self.value_string.truncate(len);
        self.value_object.truncate(len);
    }

    pub fn get(&self, slot: u32) -> JsValue {
        let index = slot as usize;
        match self.value_tag[index] {
            ValueTag::Number => JsValue::Number(self.value_number[index]),
            ValueTag::Bool => JsValue::Bool(self.value_bool[index]),
            ValueTag::String => JsValue::String(self.value_string[index].clone()),
            ValueTag::Null => JsValue::Null,
            ValueTag::Undefined => JsValue::Undefined,
            ValueTag::Object => JsValue::Object(self.value_object[index]),
        }
    }

    pub fn values(&self) -> impl Iterator<Item = JsValue> + '_ {
        (0..self.len()).map(|index| self.get(index as u32))
    }

    pub fn set(&mut self, slot: u32, value: JsValue) {
        let index = slot as usize;
        self.ensure_len(index + 1);
        self.write(index, value);
    }

    pub fn clear(&mut self) {
        self.value_tag.clear();
        self.value_number.clear();
        self.value_bool.clear();
        self.value_string.clear();
        self.value_object.clear();
    }

    fn push_raw(&mut self, value: JsValue) {
        self.value_tag.push(ValueTag::Undefined);
        self.value_number.push(0.0);
        self.value_bool.push(false);
        self.value_string.push(String::new());
        self.value_object.push(ObjectId {
            segment: super::SegmentId(0),
            slot: 0,
        });

        let index = self.len() - 1;
        self.write(index, value);
    }

    fn ensure_len(&mut self, len: usize) {
        while self.len() < len {
            self.push_raw(JsValue::Undefined);
        }
    }

    fn write(&mut self, index: usize, value: JsValue) {
        self.value_number[index] = 0.0;
        self.value_bool[index] = false;
        self.value_string[index].clear();

        match value {
            JsValue::Number(value) => {
                self.value_tag[index] = ValueTag::Number;
                self.value_number[index] = value;
            }
            JsValue::Bool(value) => {
                self.value_tag[index] = ValueTag::Bool;
                self.value_bool[index] = value;
            }
            JsValue::String(value) => {
                self.value_tag[index] = ValueTag::String;
                self.value_string[index] = value;
            }
            JsValue::Null => {
                self.value_tag[index] = ValueTag::Null;
            }
            JsValue::Undefined => {
                self.value_tag[index] = ValueTag::Undefined;
            }
            JsValue::Object(value) => {
                self.value_tag[index] = ValueTag::Object;
                self.value_object[index] = value;
            }
        }
    }
}

impl Default for ValueCols {
    fn default() -> Self {
        Self::new()
    }
}
