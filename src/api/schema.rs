use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDefinition {
    pub version: u32,
    pub fields: Vec<FieldDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDefinition {
    pub id: FieldId,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    pub nullable: bool,
    pub default: Option<FieldValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    Int64,
    Float64,
    String,
    Bytes,
    Bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldValue {
    Null,
    Int64(i64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Bool(bool),
}

pub type ColumnarRecord = BTreeMap<FieldId, FieldValue>;
pub type NamedColumnarRecord = BTreeMap<String, FieldValue>;

impl SchemaDefinition {
    pub fn validate(&self) -> Result<(), StorageError> {
        SchemaValidation::new(self).map(|_| ())
    }

    pub fn field(&self, name: &str) -> Option<&FieldDefinition> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn field_by_id(&self, field_id: FieldId) -> Option<&FieldDefinition> {
        self.fields.iter().find(|field| field.id == field_id)
    }

    pub fn field_id(&self, name: &str) -> Option<FieldId> {
        self.field(name).map(|field| field.id)
    }

    pub fn validate_successor(&self, successor: &Self) -> Result<(), StorageError> {
        let current = SchemaValidation::new(self)?;
        let next = SchemaValidation::new(successor)?;

        if successor.version <= self.version {
            return Err(StorageError::unsupported(format!(
                "schema version must increase: current={}, successor={}",
                self.version, successor.version
            )));
        }

        for (&field_id, current_field) in &current.fields_by_id {
            let Some(next_field) = next.fields_by_id.get(&field_id) else {
                continue;
            };
            if next_field.field_type != current_field.field_type {
                return Err(StorageError::unsupported(format!(
                    "field {} ({}) changes type from {} to {}",
                    field_id.get(),
                    current_field.name,
                    current_field.field_type.as_str(),
                    next_field.field_type.as_str()
                )));
            }
        }

        for (&field_id, next_field) in &next.fields_by_id {
            if current.fields_by_id.contains_key(&field_id) {
                continue;
            }
            if !next_field.nullable && next_field.default.is_none() {
                return Err(StorageError::unsupported(format!(
                    "new field {} ({}) must be nullable or define a default for lazy schema evolution",
                    field_id.get(),
                    next_field.name
                )));
            }
        }

        for (name, &current_field_id) in &current.field_ids_by_name {
            let Some(&next_field_id) = next.field_ids_by_name.get(name) else {
                continue;
            };
            if next_field_id != current_field_id {
                return Err(StorageError::unsupported(format!(
                    "field name {} changes id from {} to {}; renames must preserve field ids",
                    name,
                    current_field_id.get(),
                    next_field_id.get()
                )));
            }
        }

        Ok(())
    }

    pub fn normalize_record(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        SchemaValidation::new(self)?.normalize_record(record)
    }

    pub fn normalize_merge_operand(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        SchemaValidation::new(self)?.normalize_merge_operand(record)
    }

    pub fn record_from_names<I, S>(&self, fields: I) -> Result<ColumnarRecord, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        SchemaValidation::new(self)?.record_from_names(fields)
    }
}

impl FieldType {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Int64 => "int64",
            Self::Float64 => "float64",
            Self::String => "string",
            Self::Bytes => "bytes",
            Self::Bool => "bool",
        }
    }
}

impl From<i64> for FieldValue {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<f64> for FieldValue {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Vec<u8>> for FieldValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for FieldValue {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<bool> for FieldValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

pub(super) struct SchemaValidation<'a> {
    pub(super) schema: &'a SchemaDefinition,
    pub(super) fields_by_id: BTreeMap<FieldId, &'a FieldDefinition>,
    pub(super) field_ids_by_name: BTreeMap<String, FieldId>,
}

impl<'a> SchemaValidation<'a> {
    pub(super) fn new(schema: &'a SchemaDefinition) -> Result<Self, StorageError> {
        if schema.version == 0 {
            return Err(StorageError::unsupported(
                "schema version must be greater than zero",
            ));
        }
        if schema.fields.is_empty() {
            return Err(StorageError::unsupported(
                "columnar schemas must define at least one field",
            ));
        }

        let mut fields_by_id = BTreeMap::new();
        let mut field_ids_by_name = BTreeMap::new();

        for field in &schema.fields {
            if field.id.get() == 0 {
                return Err(StorageError::unsupported(format!(
                    "field {} ({}) uses reserved field id 0",
                    field.id.get(),
                    field.name
                )));
            }
            if field.name.trim().is_empty() {
                return Err(StorageError::unsupported(format!(
                    "field {} has an empty name",
                    field.id.get()
                )));
            }
            if fields_by_id.insert(field.id, field).is_some() {
                return Err(StorageError::unsupported(format!(
                    "schema contains duplicate field id {}",
                    field.id.get()
                )));
            }
            if field_ids_by_name
                .insert(field.name.clone(), field.id)
                .is_some()
            {
                return Err(StorageError::unsupported(format!(
                    "schema contains duplicate field name {}",
                    field.name
                )));
            }
            if let Some(default) = &field.default {
                validate_field_value_against_definition(field, default, "default value")?;
            }
        }

        Ok(Self {
            schema,
            fields_by_id,
            field_ids_by_name,
        })
    }

    pub(super) fn normalize_record(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        for (&field_id, value) in record {
            let field = self.fields_by_id.get(&field_id).ok_or_else(|| {
                StorageError::unsupported(format!(
                    "record contains unknown field id {}",
                    field_id.get()
                ))
            })?;
            validate_field_value_against_definition(field, value, "record value")?;
        }

        let mut normalized = BTreeMap::new();
        for field in &self.schema.fields {
            let value = match record.get(&field.id) {
                Some(value) => value.clone(),
                None => self.missing_field_value(field)?,
            };
            normalized.insert(field.id, value);
        }

        Ok(normalized)
    }

    pub(super) fn normalize_merge_operand(
        &self,
        record: &ColumnarRecord,
    ) -> Result<ColumnarRecord, StorageError> {
        for (&field_id, value) in record {
            let field = self.fields_by_id.get(&field_id).ok_or_else(|| {
                StorageError::unsupported(format!(
                    "record contains unknown field id {}",
                    field_id.get()
                ))
            })?;
            validate_merge_operand_value_against_definition(field, value, "merge operand value")?;
        }

        let mut normalized = BTreeMap::new();
        for field in &self.schema.fields {
            let value = record.get(&field.id).cloned().unwrap_or(FieldValue::Null);
            validate_merge_operand_value_against_definition(field, &value, "merge operand value")?;
            normalized.insert(field.id, value);
        }

        Ok(normalized)
    }

    pub(super) fn record_from_names<I, S>(&self, fields: I) -> Result<ColumnarRecord, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        let mut resolved = BTreeMap::new();
        for (name, value) in fields {
            let name = name.into();
            let field_id = self.field_ids_by_name.get(&name).copied().ok_or_else(|| {
                StorageError::unsupported(format!("record contains unknown field name {}", name))
            })?;
            if resolved.insert(field_id, value).is_some() {
                return Err(StorageError::unsupported(format!(
                    "record contains duplicate field name {}",
                    name
                )));
            }
        }

        self.normalize_record(&resolved)
    }

    pub(super) fn missing_field_value(
        &self,
        field: &FieldDefinition,
    ) -> Result<FieldValue, StorageError> {
        missing_field_value_for_definition(field)
    }
}

pub(super) fn missing_field_value_for_definition(
    field: &FieldDefinition,
) -> Result<FieldValue, StorageError> {
    if let Some(default) = &field.default {
        return Ok(default.clone());
    }
    if field.nullable {
        return Ok(FieldValue::Null);
    }

    Err(StorageError::unsupported(format!(
        "record is missing required field {}",
        field.name
    )))
}

pub(super) fn validate_field_value_against_definition(
    field: &FieldDefinition,
    value: &FieldValue,
    context: &str,
) -> Result<(), StorageError> {
    if matches!(value, FieldValue::Null) {
        return if field.nullable {
            Ok(())
        } else {
            Err(StorageError::unsupported(format!(
                "{context} for field {} cannot be null",
                field.name
            )))
        };
    }

    if field_value_matches_type(field.field_type, value) {
        Ok(())
    } else {
        Err(StorageError::unsupported(format!(
            "{context} for field {} has type {}, expected {}",
            field.name,
            field_value_type_name(value),
            field.field_type.as_str()
        )))
    }
}

pub(super) fn validate_merge_operand_value_against_definition(
    field: &FieldDefinition,
    value: &FieldValue,
    context: &str,
) -> Result<(), StorageError> {
    if matches!(value, FieldValue::Null) {
        return Ok(());
    }

    validate_field_value_against_definition(field, value, context)
}

pub(super) fn field_value_matches_type(field_type: FieldType, value: &FieldValue) -> bool {
    matches!(
        (field_type, value),
        (FieldType::Int64, FieldValue::Int64(_))
            | (FieldType::Float64, FieldValue::Float64(_))
            | (FieldType::String, FieldValue::String(_))
            | (FieldType::Bytes, FieldValue::Bytes(_))
            | (FieldType::Bool, FieldValue::Bool(_))
    )
}

pub(super) fn field_value_type_name(value: &FieldValue) -> &'static str {
    match value {
        FieldValue::Null => "null",
        FieldValue::Int64(_) => "int64",
        FieldValue::Float64(_) => "float64",
        FieldValue::String(_) => "string",
        FieldValue::Bytes(_) => "bytes",
        FieldValue::Bool(_) => "bool",
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Bytes(Vec<u8>),
    Record(ColumnarRecord),
}

impl Value {
    pub fn bytes(value: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(value.into())
    }

    pub fn record(record: ColumnarRecord) -> Self {
        Self::Record(record)
    }

    pub fn named_record<I, S>(schema: &SchemaDefinition, fields: I) -> Result<Self, StorageError>
    where
        I: IntoIterator<Item = (S, FieldValue)>,
        S: Into<String>,
    {
        Ok(Self::Record(schema.record_from_names(fields)?))
    }
}
