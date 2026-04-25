use std::collections::HashMap;

use crate::Error;

use super::{JsValue, Symbol};

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct ObjectId(pub u64);

#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub enum PropertyKey {
    Symbol(Symbol),
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsHeap {
    next_object_id: u64,
    objects: HashMap<ObjectId, JsObject>,
}

impl JsHeap {
    pub fn new() -> Self {
        Self {
            next_object_id: 0,
            objects: HashMap::new(),
        }
    }

    pub fn alloc_object(&mut self, kind: ObjectKind) -> ObjectId {
        let id = ObjectId(self.next_object_id);
        self.next_object_id += 1;
        self.objects.insert(id, JsObject::new(kind));
        id
    }

    pub fn get_property(&self, object: ObjectId, key: PropertyKey) -> Result<JsValue, Error> {
        let object = self.object(object)?;

        Ok(object.get_property(key).unwrap_or(JsValue::Undefined))
    }

    pub fn set_property(
        &mut self,
        object: ObjectId,
        key: PropertyKey,
        value: JsValue,
    ) -> Result<(), Error> {
        self.object_mut(object)?.set_property(key, value);

        Ok(())
    }

    fn object(&self, object: ObjectId) -> Result<&JsObject, Error> {
        self.objects
            .get(&object)
            .ok_or(Error::JsObjectNotFound { object: object.0 })
    }

    fn object_mut(&mut self, object: ObjectId) -> Result<&mut JsObject, Error> {
        self.objects
            .get_mut(&object)
            .ok_or(Error::JsObjectNotFound { object: object.0 })
    }
}

impl Default for JsHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsObject {
    kind: ObjectKind,
    properties: HashMap<PropertyKey, JsProperty>,
}

impl JsObject {
    pub fn new(kind: ObjectKind) -> Self {
        Self {
            kind,
            properties: HashMap::new(),
        }
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub fn get_property(&self, key: PropertyKey) -> Option<JsValue> {
        self.properties.get(&key).map(JsProperty::value)
    }

    pub fn set_property(&mut self, key: PropertyKey, value: JsValue) {
        self.properties.insert(key, JsProperty::new(value));
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ObjectKind {
    Ordinary,
    Host,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JsProperty {
    value: JsValue,
}

impl JsProperty {
    pub fn new(value: JsValue) -> Self {
        Self { value }
    }

    pub fn value(&self) -> JsValue {
        self.value.clone()
    }
}
