//! Typed record helpers for applications layered on top of `terracedb`.
//!
//! `terracedb-records` is intentionally small:
//!
//! - it keeps Terracedb's table, transaction, durability, and visibility semantics intact,
//! - it adds typed key/value codecs plus structured codec errors, and
//! - it gives application code a lightweight way to stop hand-rolling `Vec<u8>` keys and
//!   `Value::Bytes` JSON plumbing.
//!
//! Migration guidance:
//!
//! - Keep raw [`terracedb::Table`] handles where lower-level libraries still require them, such
//!   as projections or workflows.
//! - Wrap application-facing row tables in [`RecordTable`] and move key/value encoding into codecs.
//! - Use [`RecordTransaction`] for typed OCC flows when the transaction is created in your
//!   application code.
//! - When a helper library gives you a raw [`terracedb::Transaction`], use
//!   [`RecordTable::read_in`], [`RecordTable::write_in`], and [`RecordTable::delete_in`] so the
//!   typed layer stays optional instead of becoming a separate engine.
//!
//! The TODO example app in this repository shows the intended pattern: typed tables own the
//! application model, while projections and workflows still operate on the same underlying Terracedb
//! tables.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error as StdError,
    fmt,
    marker::PhantomData,
    mem::size_of,
    pin::Pin,
};

use futures::{Stream, StreamExt, stream};
use serde::{Serialize, de::DeserializeOwned};
use terracedb::{
    ColumnarRecord, Db, Key, KvStream, ReadError, ScanExecution, ScanOptions, SchemaDefinition,
    SequenceNumber, Table, Transaction, TransactionCommitError, Value, WriteError,
};
use thiserror::Error;

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

pub type RecordStream<K, V> = Pin<Box<dyn Stream<Item = (K, V)> + Send + 'static>>;
pub type ProjectionStream<V> = Pin<Box<dyn Stream<Item = V> + Send + 'static>>;

/// Typed key codec used by [`RecordTable`] and [`RecordTransaction`].
pub trait KeyCodec<K>: Clone + Send + Sync + 'static {
    fn encode_key(&self, key: &K) -> Result<Key, RecordCodecError>;

    fn decode_key(&self, key: &[u8]) -> Result<K, RecordCodecError>;

    fn encode_prefix(&self, prefix: &K) -> Result<Key, RecordCodecError> {
        self.encode_key(prefix)
    }
}

/// Typed value codec used by [`RecordTable`] and [`RecordTransaction`].
pub trait ValueCodec<V>: Clone + Send + Sync + 'static {
    fn encode_value(&self, value: &V) -> Result<Value, RecordCodecError>;

    fn decode_value(&self, value: &Value) -> Result<V, RecordCodecError>;
}

/// Typed codec for structured columnar records.
pub trait ColumnarRecordCodec<V>: Clone + Send + Sync + 'static {
    fn encode_record(&self, value: &V) -> Result<ColumnarRecord, RecordCodecError>;

    fn decode_record(&self, record: &ColumnarRecord) -> Result<V, RecordCodecError>;
}

/// Typed decoder for projected columnar scans.
pub trait ColumnarProjection<K, V>: Clone + Send + Sync + 'static {
    fn columns(&self) -> Vec<String>;

    fn decode_projection(&self, key: &K, record: &ColumnarRecord) -> Result<V, RecordCodecError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecTarget {
    Key,
    Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecPhase {
    Encode,
    Decode,
    Validation,
}

#[derive(Debug, Error)]
pub enum RecordCodecError {
    #[error("failed to encode {target}")]
    Encode {
        target: CodecTarget,
        #[source]
        source: BoxError,
    },
    #[error("failed to decode {target}")]
    Decode {
        target: CodecTarget,
        #[source]
        source: BoxError,
    },
    #[error("invalid {target}")]
    Validation {
        target: CodecTarget,
        #[source]
        source: BoxError,
    },
}

impl RecordCodecError {
    pub fn encode_key<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Encode {
            target: CodecTarget::Key,
            source: Box::new(source),
        }
    }

    pub fn decode_key<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Decode {
            target: CodecTarget::Key,
            source: Box::new(source),
        }
    }

    pub fn validate_key<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Validation {
            target: CodecTarget::Key,
            source: Box::new(source),
        }
    }

    pub fn encode_value<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Encode {
            target: CodecTarget::Value,
            source: Box::new(source),
        }
    }

    pub fn decode_value<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Decode {
            target: CodecTarget::Value,
            source: Box::new(source),
        }
    }

    pub fn validate_value<E>(source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Validation {
            target: CodecTarget::Value,
            source: Box::new(source),
        }
    }

    pub fn target(&self) -> CodecTarget {
        match self {
            Self::Encode { target, .. }
            | Self::Decode { target, .. }
            | Self::Validation { target, .. } => *target,
        }
    }

    pub fn phase(&self) -> CodecPhase {
        match self {
            Self::Encode { .. } => CodecPhase::Encode,
            Self::Decode { .. } => CodecPhase::Decode,
            Self::Validation { .. } => CodecPhase::Validation,
        }
    }

    pub fn is_decode(&self) -> bool {
        self.phase() == CodecPhase::Decode
    }

    pub fn is_validation(&self) -> bool {
        self.phase() == CodecPhase::Validation
    }
}

#[derive(Debug, Error)]
pub enum RecordReadError {
    #[error(transparent)]
    Storage(#[from] ReadError),
    #[error(transparent)]
    Codec(#[from] RecordCodecError),
}

impl RecordReadError {
    pub fn storage(&self) -> Option<&ReadError> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Codec(_) => None,
        }
    }

    pub fn codec(&self) -> Option<&RecordCodecError> {
        match self {
            Self::Storage(_) => None,
            Self::Codec(error) => Some(error),
        }
    }
}

#[derive(Debug, Error)]
pub enum RecordWriteError {
    #[error(transparent)]
    Storage(#[from] WriteError),
    #[error(transparent)]
    Codec(#[from] RecordCodecError),
}

impl RecordWriteError {
    pub fn storage(&self) -> Option<&WriteError> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Codec(_) => None,
        }
    }

    pub fn codec(&self) -> Option<&RecordCodecError> {
        match self {
            Self::Storage(_) => None,
            Self::Codec(error) => Some(error),
        }
    }
}

/// Raw `Vec<u8>` passthrough codec for byte keys or values.
#[derive(Clone, Copy, Debug, Default)]
pub struct BytesCodec;

impl KeyCodec<Vec<u8>> for BytesCodec {
    fn encode_key(&self, key: &Vec<u8>) -> Result<Key, RecordCodecError> {
        Ok(key.clone())
    }

    fn decode_key(&self, key: &[u8]) -> Result<Vec<u8>, RecordCodecError> {
        Ok(key.to_vec())
    }

    fn encode_prefix(&self, prefix: &Vec<u8>) -> Result<Key, RecordCodecError> {
        Ok(prefix.clone())
    }
}

impl ValueCodec<Vec<u8>> for BytesCodec {
    fn encode_value(&self, value: &Vec<u8>) -> Result<Value, RecordCodecError> {
        Ok(Value::Bytes(value.clone()))
    }

    fn decode_value(&self, value: &Value) -> Result<Vec<u8>, RecordCodecError> {
        Ok(expect_bytes(value, CodecTarget::Value)?.to_vec())
    }
}

/// UTF-8 string codec for row keys or byte-backed row values.
#[derive(Clone, Copy, Debug, Default)]
pub struct Utf8StringCodec;

impl Utf8StringCodec {
    pub fn encode_str(&self, value: &str) -> Key {
        value.as_bytes().to_vec()
    }
}

impl KeyCodec<String> for Utf8StringCodec {
    fn encode_key(&self, key: &String) -> Result<Key, RecordCodecError> {
        Ok(self.encode_str(key))
    }

    fn decode_key(&self, key: &[u8]) -> Result<String, RecordCodecError> {
        String::from_utf8(key.to_vec()).map_err(RecordCodecError::decode_key)
    }

    fn encode_prefix(&self, prefix: &String) -> Result<Key, RecordCodecError> {
        Ok(self.encode_str(prefix))
    }
}

impl ValueCodec<String> for Utf8StringCodec {
    fn encode_value(&self, value: &String) -> Result<Value, RecordCodecError> {
        Ok(Value::bytes(value.as_bytes().to_vec()))
    }

    fn decode_value(&self, value: &Value) -> Result<String, RecordCodecError> {
        String::from_utf8(expect_bytes(value, CodecTarget::Value)?.to_vec())
            .map_err(RecordCodecError::decode_value)
    }
}

macro_rules! impl_unsigned_big_endian_codec {
    ($name:ident, $ty:ty) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl KeyCodec<$ty> for $name {
            fn encode_key(&self, key: &$ty) -> Result<Key, RecordCodecError> {
                Ok(key.to_be_bytes().to_vec())
            }

            fn decode_key(&self, key: &[u8]) -> Result<$ty, RecordCodecError> {
                decode_fixed_width::<$ty>(key).map_err(RecordCodecError::decode_key)
            }
        }

        impl ValueCodec<$ty> for $name {
            fn encode_value(&self, value: &$ty) -> Result<Value, RecordCodecError> {
                Ok(Value::bytes(value.to_be_bytes().to_vec()))
            }

            fn decode_value(&self, value: &Value) -> Result<$ty, RecordCodecError> {
                decode_fixed_width::<$ty>(expect_bytes(value, CodecTarget::Value)?)
                    .map_err(RecordCodecError::decode_value)
            }
        }
    };
}

macro_rules! impl_signed_big_endian_codec {
    ($name:ident, $signed:ty, $unsigned:ty, $mask:expr) => {
        #[derive(Clone, Copy, Debug, Default)]
        pub struct $name;

        impl KeyCodec<$signed> for $name {
            fn encode_key(&self, key: &$signed) -> Result<Key, RecordCodecError> {
                let encoded = ((*key as $unsigned) ^ $mask).to_be_bytes().to_vec();
                Ok(encoded)
            }

            fn decode_key(&self, key: &[u8]) -> Result<$signed, RecordCodecError> {
                let raw =
                    decode_fixed_width::<$unsigned>(key).map_err(RecordCodecError::decode_key)?;
                Ok(((raw ^ $mask) as $signed))
            }
        }

        impl ValueCodec<$signed> for $name {
            fn encode_value(&self, value: &$signed) -> Result<Value, RecordCodecError> {
                let encoded = ((*value as $unsigned) ^ $mask).to_be_bytes().to_vec();
                Ok(Value::bytes(encoded))
            }

            fn decode_value(&self, value: &Value) -> Result<$signed, RecordCodecError> {
                let raw = decode_fixed_width::<$unsigned>(expect_bytes(value, CodecTarget::Value)?)
                    .map_err(RecordCodecError::decode_value)?;
                Ok(((raw ^ $mask) as $signed))
            }
        }
    };
}

impl_unsigned_big_endian_codec!(BigEndianU16Codec, u16);
impl_unsigned_big_endian_codec!(BigEndianU32Codec, u32);
impl_unsigned_big_endian_codec!(BigEndianU64Codec, u64);
impl_signed_big_endian_codec!(BigEndianI16Codec, i16, u16, 0x8000_u16);
impl_signed_big_endian_codec!(BigEndianI32Codec, i32, u32, 0x8000_0000_u32);
impl_signed_big_endian_codec!(BigEndianI64Codec, i64, u64, 0x8000_0000_0000_0000_u64);

/// Serde JSON value codec for byte-backed row values.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonValueCodec<V> {
    marker: PhantomData<fn() -> V>,
}

impl<V> JsonValueCodec<V> {
    pub const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<V> ValueCodec<V> for JsonValueCodec<V>
where
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn encode_value(&self, value: &V) -> Result<Value, RecordCodecError> {
        serde_json::to_vec(value)
            .map(Value::bytes)
            .map_err(RecordCodecError::encode_value)
    }

    fn decode_value(&self, value: &Value) -> Result<V, RecordCodecError> {
        serde_json::from_slice(expect_bytes(value, CodecTarget::Value)?)
            .map_err(RecordCodecError::decode_value)
    }
}

/// Typed wrapper over a Terracedb columnar [`Table`].
#[derive(Clone, Debug)]
pub struct ColumnarTable<K, V, KC, VC> {
    table: Table,
    schema: SchemaDefinition,
    key_codec: KC,
    value_codec: VC,
    marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, KC, VC> ColumnarTable<K, V, KC, VC> {
    pub fn with_codecs(
        table: Table,
        schema: SchemaDefinition,
        key_codec: KC,
        value_codec: VC,
    ) -> Self {
        Self {
            table,
            schema,
            key_codec,
            value_codec,
            marker: PhantomData,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn schema(&self) -> &SchemaDefinition {
        &self.schema
    }

    pub fn key_codec(&self) -> &KC {
        &self.key_codec
    }

    pub fn value_codec(&self) -> &VC {
        &self.value_codec
    }

    pub fn into_inner(self) -> Table {
        self.table
    }
}

impl<K, V, KC, VC> ColumnarTable<K, V, KC, VC>
where
    KC: KeyCodec<K>,
    VC: ColumnarRecordCodec<V>,
    K: Send + 'static,
    V: Send + 'static,
{
    fn encode_key(&self, key: &K) -> Result<Key, RecordCodecError> {
        self.key_codec.encode_key(key)
    }

    fn encode_prefix(&self, prefix: &K) -> Result<Key, RecordCodecError> {
        self.key_codec.encode_prefix(prefix)
    }

    fn decode_key(&self, key: &[u8]) -> Result<K, RecordCodecError> {
        self.key_codec.decode_key(key)
    }

    fn encode_value(&self, value: &V) -> Result<Value, RecordCodecError> {
        let record = self.value_codec.encode_record(value)?;
        self.schema
            .normalize_record(&record)
            .map(Value::record)
            .map_err(RecordCodecError::validate_value)
    }

    fn decode_value(&self, value: &Value) -> Result<V, RecordCodecError> {
        self.value_codec
            .decode_record(expect_record(value, CodecTarget::Value)?)
    }

    pub async fn read(&self, key: &K) -> Result<Option<V>, RecordReadError> {
        let raw_key = self.encode_key(key)?;
        let value = self.table.read(raw_key).await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn read_in(
        &self,
        tx: &mut Transaction,
        key: &K,
    ) -> Result<Option<V>, RecordReadError> {
        let raw_key = self.encode_key(key)?;
        let value = tx.read(self.table(), raw_key).await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn write(&self, key: &K, value: &V) -> Result<SequenceNumber, RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        let raw_value = self.encode_value(value)?;
        self.table
            .write(raw_key, raw_value)
            .await
            .map_err(Into::into)
    }

    pub fn write_in(
        &self,
        tx: &mut Transaction,
        key: &K,
        value: &V,
    ) -> Result<(), RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        let raw_value = self.encode_value(value)?;
        tx.write(self.table(), raw_key, raw_value);
        Ok(())
    }

    pub async fn delete(&self, key: &K) -> Result<SequenceNumber, RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        self.table.delete(raw_key).await.map_err(Into::into)
    }

    pub fn delete_in(&self, tx: &mut Transaction, key: &K) -> Result<(), RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        tx.delete(self.table(), raw_key);
        Ok(())
    }

    pub async fn scan(
        &self,
        start: &K,
        end: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError> {
        Ok(self.scan_with_execution(start, end, opts).await?.0)
    }

    pub async fn scan_with_execution(
        &self,
        start: &K,
        end: &K,
        opts: ScanOptions,
    ) -> Result<(RecordStream<K, V>, ScanExecution), RecordReadError> {
        let raw_start = self.encode_key(start)?;
        let raw_end = self.encode_key(end)?;
        let (stream, execution) = self
            .table
            .scan_with_execution(raw_start, raw_end, opts)
            .await?;
        Ok((
            self.decode_stream(stream)
                .await
                .map_err(RecordReadError::from)?,
            execution,
        ))
    }

    pub async fn scan_prefix(
        &self,
        prefix: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError> {
        Ok(self.scan_prefix_with_execution(prefix, opts).await?.0)
    }

    pub async fn scan_prefix_with_execution(
        &self,
        prefix: &K,
        opts: ScanOptions,
    ) -> Result<(RecordStream<K, V>, ScanExecution), RecordReadError> {
        let raw_prefix = self.encode_prefix(prefix)?;
        let (stream, execution) = self
            .table
            .scan_prefix_with_execution(raw_prefix, opts)
            .await?;
        Ok((
            self.decode_stream(stream)
                .await
                .map_err(RecordReadError::from)?,
            execution,
        ))
    }

    pub async fn scan_all(&self, opts: ScanOptions) -> Result<RecordStream<K, V>, RecordReadError> {
        Ok(self.scan_all_with_execution(opts).await?.0)
    }

    pub async fn scan_all_with_execution(
        &self,
        opts: ScanOptions,
    ) -> Result<(RecordStream<K, V>, ScanExecution), RecordReadError> {
        let (stream, execution) = self
            .table
            .scan_with_execution(Vec::new(), vec![0xff], opts)
            .await?;
        Ok((
            self.decode_stream(stream)
                .await
                .map_err(RecordReadError::from)?,
            execution,
        ))
    }

    pub async fn scan_projected<P, PC>(
        &self,
        start: &K,
        end: &K,
        projection: &PC,
        opts: ScanOptions,
    ) -> Result<ProjectionStream<P>, RecordReadError>
    where
        PC: ColumnarProjection<K, P>,
        P: Send + 'static,
    {
        Ok(self
            .scan_projected_with_execution(start, end, projection, opts)
            .await?
            .0)
    }

    pub async fn scan_projected_with_execution<P, PC>(
        &self,
        start: &K,
        end: &K,
        projection: &PC,
        mut opts: ScanOptions,
    ) -> Result<(ProjectionStream<P>, ScanExecution), RecordReadError>
    where
        PC: ColumnarProjection<K, P>,
        P: Send + 'static,
    {
        opts.columns = Some(projection.columns());
        let raw_start = self.encode_key(start)?;
        let raw_end = self.encode_key(end)?;
        let (stream, execution) = self
            .table
            .scan_with_execution(raw_start, raw_end, opts)
            .await?;
        Ok((
            self.decode_projected_stream(stream, projection.clone())
                .await
                .map_err(RecordReadError::from)?,
            execution,
        ))
    }

    pub async fn scan_projected_prefix<P, PC>(
        &self,
        prefix: &K,
        projection: &PC,
        opts: ScanOptions,
    ) -> Result<ProjectionStream<P>, RecordReadError>
    where
        PC: ColumnarProjection<K, P>,
        P: Send + 'static,
    {
        Ok(self
            .scan_projected_prefix_with_execution(prefix, projection, opts)
            .await?
            .0)
    }

    pub async fn scan_projected_prefix_with_execution<P, PC>(
        &self,
        prefix: &K,
        projection: &PC,
        mut opts: ScanOptions,
    ) -> Result<(ProjectionStream<P>, ScanExecution), RecordReadError>
    where
        PC: ColumnarProjection<K, P>,
        P: Send + 'static,
    {
        opts.columns = Some(projection.columns());
        let raw_prefix = self.encode_prefix(prefix)?;
        let (stream, execution) = self
            .table
            .scan_prefix_with_execution(raw_prefix, opts)
            .await?;
        Ok((
            self.decode_projected_stream(stream, projection.clone())
                .await
                .map_err(RecordReadError::from)?,
            execution,
        ))
    }

    async fn decode_stream(
        &self,
        mut stream: KvStream,
    ) -> Result<RecordStream<K, V>, RecordCodecError> {
        let mut rows = Vec::new();
        while let Some((key, value)) = stream.next().await {
            rows.push((self.decode_key(&key)?, self.decode_value(&value)?));
        }
        Ok(Box::pin(stream::iter(rows)))
    }

    async fn decode_projected_stream<P, PC>(
        &self,
        mut stream: KvStream,
        projection: PC,
    ) -> Result<ProjectionStream<P>, RecordCodecError>
    where
        PC: ColumnarProjection<K, P>,
        P: Send + 'static,
    {
        let mut rows = Vec::new();
        while let Some((key, value)) = stream.next().await {
            let decoded_key = self.decode_key(&key)?;
            let projected = projection
                .decode_projection(&decoded_key, expect_record(&value, CodecTarget::Value)?)?;
            rows.push(projected);
        }
        Ok(Box::pin(stream::iter(rows)))
    }
}

/// Typed wrapper over a Terracedb [`Table`].
#[derive(Clone, Debug)]
pub struct RecordTable<K, V, KC, VC> {
    table: Table,
    key_codec: KC,
    value_codec: VC,
    marker: PhantomData<fn() -> (K, V)>,
}

impl<K, V, KC, VC> RecordTable<K, V, KC, VC> {
    pub fn with_codecs(table: Table, key_codec: KC, value_codec: VC) -> Self {
        Self {
            table,
            key_codec,
            value_codec,
            marker: PhantomData,
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn key_codec(&self) -> &KC {
        &self.key_codec
    }

    pub fn value_codec(&self) -> &VC {
        &self.value_codec
    }

    pub fn into_inner(self) -> Table {
        self.table
    }
}

impl<K, V, KC, VC> RecordTable<K, V, KC, VC>
where
    KC: KeyCodec<K>,
    VC: ValueCodec<V>,
    K: Send + 'static,
    V: Send + 'static,
{
    pub fn encode_key(&self, key: &K) -> Result<Key, RecordCodecError> {
        self.key_codec.encode_key(key)
    }

    pub fn encode_prefix(&self, prefix: &K) -> Result<Key, RecordCodecError> {
        self.key_codec.encode_prefix(prefix)
    }

    pub fn decode_key(&self, key: &[u8]) -> Result<K, RecordCodecError> {
        self.key_codec.decode_key(key)
    }

    pub fn encode_value(&self, value: &V) -> Result<Value, RecordCodecError> {
        self.value_codec.encode_value(value)
    }

    pub fn decode_value(&self, value: &Value) -> Result<V, RecordCodecError> {
        self.value_codec.decode_value(value)
    }

    pub async fn read(&self, key: &K) -> Result<Option<V>, RecordReadError> {
        let raw_key = self.encode_key(key)?;
        let value = self.table.read(raw_key).await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn read_in(
        &self,
        tx: &mut Transaction,
        key: &K,
    ) -> Result<Option<V>, RecordReadError> {
        let raw_key = self.encode_key(key)?;
        let value = tx.read(self.table(), raw_key).await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn write(&self, key: &K, value: &V) -> Result<SequenceNumber, RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        let raw_value = self.encode_value(value)?;
        self.table
            .write(raw_key, raw_value)
            .await
            .map_err(Into::into)
    }

    pub fn write_in(
        &self,
        tx: &mut Transaction,
        key: &K,
        value: &V,
    ) -> Result<(), RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        let raw_value = self.encode_value(value)?;
        tx.write(self.table(), raw_key, raw_value);
        Ok(())
    }

    pub async fn delete(&self, key: &K) -> Result<SequenceNumber, RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        self.table.delete(raw_key).await.map_err(Into::into)
    }

    pub fn delete_in(&self, tx: &mut Transaction, key: &K) -> Result<(), RecordWriteError> {
        let raw_key = self.encode_key(key)?;
        tx.delete(self.table(), raw_key);
        Ok(())
    }

    pub async fn scan(
        &self,
        start: &K,
        end: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError> {
        let raw_start = self.encode_key(start)?;
        let raw_end = self.encode_key(end)?;
        let stream = self.table.scan(raw_start, raw_end, opts).await?;
        self.decode_stream(stream).await.map_err(Into::into)
    }

    pub async fn scan_prefix(
        &self,
        prefix: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError> {
        let raw_prefix = self.encode_prefix(prefix)?;
        let stream = self.table.scan_prefix(raw_prefix, opts).await?;
        self.decode_stream(stream).await.map_err(Into::into)
    }

    pub async fn scan_all(&self, opts: ScanOptions) -> Result<RecordStream<K, V>, RecordReadError> {
        let stream = self.table.scan(Vec::new(), vec![0xff], opts).await?;
        self.decode_stream(stream).await.map_err(Into::into)
    }

    pub async fn decode_stream(
        &self,
        mut stream: KvStream,
    ) -> Result<RecordStream<K, V>, RecordCodecError> {
        let mut rows = Vec::new();
        while let Some((key, value)) = stream.next().await {
            rows.push((self.decode_key(&key)?, self.decode_value(&value)?));
        }
        Ok(Box::pin(stream::iter(rows)))
    }

    fn decode_rows(&self, rows: Vec<(Key, Value)>) -> Result<RecordStream<K, V>, RecordCodecError> {
        let decoded = rows
            .into_iter()
            .map(|(key, value)| Ok((self.decode_key(&key)?, self.decode_value(&value)?)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Box::pin(stream::iter(decoded)))
    }
}

impl<V, VC> RecordTable<String, V, Utf8StringCodec, VC>
where
    VC: ValueCodec<V>,
    V: Send + 'static,
{
    pub async fn read_str(&self, key: &str) -> Result<Option<V>, RecordReadError> {
        let raw_key = self.key_codec.encode_str(key);
        let value = self.table.read(raw_key).await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub async fn write_str(
        &self,
        key: &str,
        value: &V,
    ) -> Result<SequenceNumber, RecordWriteError> {
        let raw_value = self.encode_value(value)?;
        self.table
            .write(self.key_codec.encode_str(key), raw_value)
            .await
            .map_err(Into::into)
    }

    pub async fn delete_str(&self, key: &str) -> Result<SequenceNumber, RecordWriteError> {
        self.table
            .delete(self.key_codec.encode_str(key))
            .await
            .map_err(Into::into)
    }

    pub async fn read_in_str(
        &self,
        tx: &mut Transaction,
        key: &str,
    ) -> Result<Option<V>, RecordReadError> {
        let value = tx
            .read(self.table(), self.key_codec.encode_str(key))
            .await?;
        value
            .map(|value| self.decode_value(&value))
            .transpose()
            .map_err(Into::into)
    }

    pub fn write_in_str(
        &self,
        tx: &mut Transaction,
        key: &str,
        value: &V,
    ) -> Result<(), RecordWriteError> {
        let raw_value = self.encode_value(value)?;
        tx.write(self.table(), self.key_codec.encode_str(key), raw_value);
        Ok(())
    }

    pub fn delete_in_str(&self, tx: &mut Transaction, key: &str) -> Result<(), RecordWriteError> {
        tx.delete(self.table(), self.key_codec.encode_str(key));
        Ok(())
    }

    pub async fn scan_str(
        &self,
        start: &str,
        end: &str,
        opts: ScanOptions,
    ) -> Result<RecordStream<String, V>, RecordReadError> {
        let stream = self
            .table
            .scan(
                self.key_codec.encode_str(start),
                self.key_codec.encode_str(end),
                opts,
            )
            .await?;
        self.decode_stream(stream).await.map_err(Into::into)
    }

    pub async fn scan_prefix_str(
        &self,
        prefix: &str,
        opts: ScanOptions,
    ) -> Result<RecordStream<String, V>, RecordReadError> {
        let stream = self
            .table
            .scan_prefix(self.key_codec.encode_str(prefix), opts)
            .await?;
        self.decode_stream(stream).await.map_err(Into::into)
    }
}

/// Transaction-scoped typed helper layered on top of Terracedb's OCC helper.
#[derive(Debug)]
pub struct RecordTransaction {
    inner: Transaction,
    local_writes: HashMap<Table, BTreeMap<Key, Option<Value>>>,
}

impl RecordTransaction {
    pub async fn begin(db: &Db) -> Self {
        Self {
            inner: Transaction::begin(db).await,
            local_writes: HashMap::new(),
        }
    }

    pub fn snapshot_sequence(&self) -> SequenceNumber {
        self.inner.snapshot_sequence()
    }

    pub async fn read<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        key: &K,
    ) -> Result<Option<V>, RecordReadError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        table.read_in(&mut self.inner, key).await
    }

    pub fn write<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        key: &K,
        value: &V,
    ) -> Result<(), RecordWriteError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        let raw_key = table.encode_key(key)?;
        let raw_value = table.encode_value(value)?;
        self.inner
            .write(table.table(), raw_key.clone(), raw_value.clone());
        remember_local_write(
            &mut self.local_writes,
            table.table(),
            raw_key,
            Some(raw_value),
        );
        Ok(())
    }

    pub fn delete<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        key: &K,
    ) -> Result<(), RecordWriteError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        let raw_key = table.encode_key(key)?;
        self.inner.delete(table.table(), raw_key.clone());
        remember_local_write(&mut self.local_writes, table.table(), raw_key, None);
        Ok(())
    }

    pub async fn scan<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        start: &K,
        end: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        let raw_start = table.encode_key(start)?;
        let raw_end = table.encode_key(end)?;
        scan_transaction_range(
            &mut self.inner,
            &self.local_writes,
            table,
            raw_start,
            raw_end,
            opts,
        )
        .await
    }

    pub async fn scan_prefix<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        prefix: &K,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        let raw_prefix = table.encode_prefix(prefix)?;
        scan_transaction_prefix(&mut self.inner, &self.local_writes, table, raw_prefix, opts).await
    }

    pub async fn scan_all<K, V, KC, VC>(
        &mut self,
        table: &RecordTable<K, V, KC, VC>,
        opts: ScanOptions,
    ) -> Result<RecordStream<K, V>, RecordReadError>
    where
        KC: KeyCodec<K>,
        VC: ValueCodec<V>,
        K: Send + 'static,
        V: Send + 'static,
    {
        scan_transaction_range(
            &mut self.inner,
            &self.local_writes,
            table,
            Vec::new(),
            vec![0xff],
            opts,
        )
        .await
    }

    pub async fn commit(self) -> Result<SequenceNumber, TransactionCommitError> {
        self.inner.commit().await
    }

    pub async fn commit_no_flush(self) -> Result<SequenceNumber, TransactionCommitError> {
        self.inner.commit_no_flush().await
    }

    pub fn abort(self) {
        self.inner.abort();
    }
}

impl RecordTransaction {
    pub async fn read_str<V, VC>(
        &mut self,
        table: &RecordTable<String, V, Utf8StringCodec, VC>,
        key: &str,
    ) -> Result<Option<V>, RecordReadError>
    where
        VC: ValueCodec<V>,
        V: Send + 'static,
    {
        table.read_in_str(&mut self.inner, key).await
    }

    pub fn write_str<V, VC>(
        &mut self,
        table: &RecordTable<String, V, Utf8StringCodec, VC>,
        key: &str,
        value: &V,
    ) -> Result<(), RecordWriteError>
    where
        VC: ValueCodec<V>,
        V: Send + 'static,
    {
        let raw_key = table.key_codec.encode_str(key);
        let raw_value = table.encode_value(value)?;
        self.inner
            .write(table.table(), raw_key.clone(), raw_value.clone());
        remember_local_write(
            &mut self.local_writes,
            table.table(),
            raw_key,
            Some(raw_value),
        );
        Ok(())
    }

    pub fn delete_str<V, VC>(
        &mut self,
        table: &RecordTable<String, V, Utf8StringCodec, VC>,
        key: &str,
    ) -> Result<(), RecordWriteError>
    where
        VC: ValueCodec<V>,
        V: Send + 'static,
    {
        let raw_key = table.key_codec.encode_str(key);
        self.inner.delete(table.table(), raw_key.clone());
        remember_local_write(&mut self.local_writes, table.table(), raw_key, None);
        Ok(())
    }

    pub async fn scan_str<V, VC>(
        &mut self,
        table: &RecordTable<String, V, Utf8StringCodec, VC>,
        start: &str,
        end: &str,
        opts: ScanOptions,
    ) -> Result<RecordStream<String, V>, RecordReadError>
    where
        VC: ValueCodec<V>,
        V: Send + 'static,
    {
        scan_transaction_range(
            &mut self.inner,
            &self.local_writes,
            table,
            table.key_codec.encode_str(start),
            table.key_codec.encode_str(end),
            opts,
        )
        .await
    }

    pub async fn scan_prefix_str<V, VC>(
        &mut self,
        table: &RecordTable<String, V, Utf8StringCodec, VC>,
        prefix: &str,
        opts: ScanOptions,
    ) -> Result<RecordStream<String, V>, RecordReadError>
    where
        VC: ValueCodec<V>,
        V: Send + 'static,
    {
        scan_transaction_prefix(
            &mut self.inner,
            &self.local_writes,
            table,
            table.key_codec.encode_str(prefix),
            opts,
        )
        .await
    }
}

fn remember_local_write(
    local_writes: &mut HashMap<Table, BTreeMap<Key, Option<Value>>>,
    table: &Table,
    key: Key,
    value: Option<Value>,
) {
    local_writes
        .entry(table.clone())
        .or_default()
        .insert(key, value);
}

async fn scan_transaction_range<K, V, KC, VC>(
    tx: &mut Transaction,
    local_writes: &HashMap<Table, BTreeMap<Key, Option<Value>>>,
    table: &RecordTable<K, V, KC, VC>,
    raw_start: Key,
    raw_end: Key,
    opts: ScanOptions,
) -> Result<RecordStream<K, V>, RecordReadError>
where
    KC: KeyCodec<K>,
    VC: ValueCodec<V>,
    K: Send + 'static,
    V: Send + 'static,
{
    let stream = table
        .table()
        .scan_at(
            raw_start.clone(),
            raw_end.clone(),
            tx.snapshot_sequence(),
            ScanOptions {
                limit: None,
                ..opts.clone()
            },
        )
        .await?;

    let rows = overlay_range_rows(
        stream,
        local_writes.get(table.table()),
        &raw_start,
        &raw_end,
        opts,
    )
    .await;
    table.decode_rows(rows).map_err(Into::into)
}

async fn scan_transaction_prefix<K, V, KC, VC>(
    tx: &mut Transaction,
    local_writes: &HashMap<Table, BTreeMap<Key, Option<Value>>>,
    table: &RecordTable<K, V, KC, VC>,
    raw_prefix: Key,
    opts: ScanOptions,
) -> Result<RecordStream<K, V>, RecordReadError>
where
    KC: KeyCodec<K>,
    VC: ValueCodec<V>,
    K: Send + 'static,
    V: Send + 'static,
{
    let stream = table
        .table()
        .scan_prefix_at(
            raw_prefix.clone(),
            tx.snapshot_sequence(),
            ScanOptions {
                limit: None,
                ..opts.clone()
            },
        )
        .await?;

    let rows =
        overlay_prefix_rows(stream, local_writes.get(table.table()), &raw_prefix, opts).await;
    table.decode_rows(rows).map_err(Into::into)
}

async fn overlay_range_rows(
    mut stream: KvStream,
    pending: Option<&BTreeMap<Key, Option<Value>>>,
    start: &[u8],
    end: &[u8],
    opts: ScanOptions,
) -> Vec<(Key, Value)> {
    let mut rows = BTreeMap::new();
    while let Some((key, value)) = stream.next().await {
        rows.insert(key, value);
    }

    if let Some(pending) = pending {
        for (key, value) in pending.range(start.to_vec()..end.to_vec()) {
            match value {
                Some(value) => {
                    rows.insert(key.clone(), value.clone());
                }
                None => {
                    rows.remove(key);
                }
            }
        }
    }

    finalize_overlay_rows(rows, opts)
}

async fn overlay_prefix_rows(
    mut stream: KvStream,
    pending: Option<&BTreeMap<Key, Option<Value>>>,
    prefix: &[u8],
    opts: ScanOptions,
) -> Vec<(Key, Value)> {
    let mut rows = BTreeMap::new();
    while let Some((key, value)) = stream.next().await {
        rows.insert(key, value);
    }

    if let Some(pending) = pending {
        for (key, value) in pending {
            if !key.starts_with(prefix) {
                continue;
            }
            match value {
                Some(value) => {
                    rows.insert(key.clone(), value.clone());
                }
                None => {
                    rows.remove(key);
                }
            }
        }
    }

    finalize_overlay_rows(rows, opts)
}

fn finalize_overlay_rows(rows: BTreeMap<Key, Value>, opts: ScanOptions) -> Vec<(Key, Value)> {
    let mut rows: Vec<_> = rows.into_iter().collect();
    if opts.reverse {
        rows.reverse();
    }
    if let Some(limit) = opts.limit {
        rows.truncate(limit);
    }
    rows
}

fn expect_bytes(value: &Value, target: CodecTarget) -> Result<&[u8], RecordCodecError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => Err(match target {
            CodecTarget::Key => RecordCodecError::decode_key(UnexpectedValueKindError),
            CodecTarget::Value => RecordCodecError::decode_value(UnexpectedValueKindError),
        }),
    }
}

fn expect_record(value: &Value, target: CodecTarget) -> Result<&ColumnarRecord, RecordCodecError> {
    match value {
        Value::Record(record) => Ok(record),
        Value::Bytes(_) => Err(match target {
            CodecTarget::Key => RecordCodecError::decode_key(UnexpectedValueKindError),
            CodecTarget::Value => RecordCodecError::decode_value(UnexpectedValueKindError),
        }),
    }
}

fn decode_fixed_width<T>(bytes: &[u8]) -> Result<T, FixedWidthDecodeError>
where
    T: FixedWidthInteger,
{
    T::decode_fixed_width(bytes)
}

trait FixedWidthInteger: Sized {
    fn decode_fixed_width(bytes: &[u8]) -> Result<Self, FixedWidthDecodeError>;
}

macro_rules! impl_fixed_width_integer {
    ($ty:ty) => {
        impl FixedWidthInteger for $ty {
            fn decode_fixed_width(bytes: &[u8]) -> Result<Self, FixedWidthDecodeError> {
                if bytes.len() != size_of::<$ty>() {
                    return Err(FixedWidthDecodeError {
                        expected: size_of::<$ty>(),
                        actual: bytes.len(),
                    });
                }
                let mut raw = [0_u8; size_of::<$ty>()];
                raw.copy_from_slice(bytes);
                Ok(<$ty>::from_be_bytes(raw))
            }
        }
    };
}

impl_fixed_width_integer!(u16);
impl_fixed_width_integer!(u32);
impl_fixed_width_integer!(u64);

#[derive(Debug, Error)]
#[error("expected a byte-backed value")]
struct UnexpectedValueKindError;

#[derive(Debug, Error)]
#[error("expected {expected} bytes, got {actual}")]
pub struct FixedWidthDecodeError {
    expected: usize,
    actual: usize,
}

impl fmt::Display for CodecTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Key => f.write_str("key"),
            Self::Value => f.write_str("value"),
        }
    }
}
