use std::convert::TryFrom;

use futures::StreamExt;

use crate::{
    ChangeKind, Db, Key, LogCursor, ReadError, ScanOptions, SequenceNumber, StorageError, Table,
    Timestamp, Transaction, TransactionCommitError, Value, WatermarkReceiver, WriteBatch,
};

const TIMER_KEY_SEPARATOR: u8 = 0;
const TIMER_SCHEDULE_VALUE_VERSION: u8 = 1;
const TIMER_LOOKUP_VALUE_VERSION: u8 = 1;
const OUTBOX_VALUE_VERSION: u8 = 1;
const CURSOR_VALUE_VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduledTimer {
    pub timer_id: Key,
    pub fire_at: Timestamp,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueTimer {
    pub schedule_key: Key,
    pub timer_id: Key,
    pub fire_at: Timestamp,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueTimerBatch {
    pub durable_sequence: SequenceNumber,
    pub now: Timestamp,
    pub timers: Vec<DueTimer>,
}

impl DueTimerBatch {
    pub fn is_empty(&self) -> bool {
        self.timers.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct DurableTimerSet {
    schedule_table: Table,
    lookup_table: Table,
}

impl DurableTimerSet {
    pub fn new(schedule_table: Table, lookup_table: Table) -> Self {
        Self {
            schedule_table,
            lookup_table,
        }
    }

    pub fn schedule_table(&self) -> &Table {
        &self.schedule_table
    }

    pub fn lookup_table(&self) -> &Table {
        &self.lookup_table
    }

    pub fn subscribe_durable(&self, db: &Db) -> WatermarkReceiver {
        db.subscribe_durable(&self.schedule_table)
    }

    pub fn stage_schedule(
        &self,
        batch: &mut WriteBatch,
        timer: ScheduledTimer,
    ) -> Result<(), StorageError> {
        let schedule_key = encode_timer_schedule_key(timer.fire_at, &timer.timer_id);
        batch.put(
            &self.schedule_table,
            schedule_key,
            Value::bytes(encode_timer_schedule_value(&timer.payload)),
        );
        batch.put(
            &self.lookup_table,
            timer.timer_id,
            Value::bytes(encode_timer_lookup_value(timer.fire_at)),
        );
        Ok(())
    }

    pub fn stage_schedule_in_transaction(
        &self,
        tx: &mut Transaction,
        timer: ScheduledTimer,
    ) -> Result<(), StorageError> {
        let schedule_key = encode_timer_schedule_key(timer.fire_at, &timer.timer_id);
        tx.write(
            &self.schedule_table,
            schedule_key,
            Value::bytes(encode_timer_schedule_value(&timer.payload)),
        );
        tx.write(
            &self.lookup_table,
            timer.timer_id,
            Value::bytes(encode_timer_lookup_value(timer.fire_at)),
        );
        Ok(())
    }

    pub async fn cancel_in_transaction(
        &self,
        tx: &mut Transaction,
        timer_id: Key,
    ) -> Result<bool, ReadError> {
        let Some(value) = tx.read(&self.lookup_table, timer_id.clone()).await? else {
            return Ok(false);
        };
        let fire_at = decode_timer_lookup_value(&value).map_err(ReadError::from)?;
        tx.delete(
            &self.schedule_table,
            encode_timer_schedule_key(fire_at, &timer_id),
        );
        tx.delete(&self.lookup_table, timer_id);
        Ok(true)
    }

    pub fn stage_due_deletion(&self, batch: &mut WriteBatch, timer: &DueTimer) {
        batch.delete(&self.schedule_table, timer.schedule_key.clone());
        batch.delete(&self.lookup_table, timer.timer_id.clone());
    }

    pub fn stage_delete_at(&self, batch: &mut WriteBatch, timer_id: Key, fire_at: Timestamp) {
        batch.delete(
            &self.schedule_table,
            encode_timer_schedule_key(fire_at, &timer_id),
        );
        batch.delete(&self.lookup_table, timer_id);
    }

    pub fn stage_due_deletion_in_transaction(&self, tx: &mut Transaction, timer: &DueTimer) {
        tx.delete(&self.schedule_table, timer.schedule_key.clone());
        tx.delete(&self.lookup_table, timer.timer_id.clone());
    }

    pub fn stage_delete_at_in_transaction(
        &self,
        tx: &mut Transaction,
        timer_id: Key,
        fire_at: Timestamp,
    ) {
        tx.delete(
            &self.schedule_table,
            encode_timer_schedule_key(fire_at, &timer_id),
        );
        tx.delete(&self.lookup_table, timer_id);
    }

    pub async fn scan_due_durable(
        &self,
        db: &Db,
        now: Timestamp,
        limit: Option<usize>,
    ) -> Result<DueTimerBatch, ReadError> {
        let durable_sequence = db.current_durable_sequence();
        let mut rows = self
            .schedule_table
            .scan_at(
                timer_due_scan_start_key(),
                timer_due_scan_end_key(now),
                durable_sequence,
                ScanOptions {
                    limit,
                    ..ScanOptions::default()
                },
            )
            .await?;

        let mut timers = Vec::new();
        while let Some((schedule_key, value)) = rows.next().await {
            timers.push(decode_due_timer(schedule_key, value).map_err(ReadError::from)?);
        }

        Ok(DueTimerBatch {
            durable_sequence,
            now,
            timers,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxEntry {
    pub outbox_id: Key,
    pub idempotency_key: String,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxMessage {
    pub entry: OutboxEntry,
    pub cursor: LogCursor,
    pub sequence: SequenceNumber,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxBatch {
    pub start_cursor: LogCursor,
    pub entries: Vec<OutboxMessage>,
}

impl OutboxBatch {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn last_cursor(&self) -> Option<LogCursor> {
        self.entries.last().map(|entry| entry.cursor)
    }
}

#[derive(Clone, Debug)]
pub struct TransactionalOutbox {
    outbox_table: Table,
}

impl TransactionalOutbox {
    pub fn new(outbox_table: Table) -> Self {
        Self { outbox_table }
    }

    pub fn outbox_table(&self) -> &Table {
        &self.outbox_table
    }

    pub fn subscribe_durable(&self, db: &Db) -> WatermarkReceiver {
        db.subscribe_durable(&self.outbox_table)
    }

    pub fn stage_entry(
        &self,
        batch: &mut WriteBatch,
        entry: OutboxEntry,
    ) -> Result<(), StorageError> {
        batch.put(
            &self.outbox_table,
            entry.outbox_id,
            Value::bytes(encode_outbox_value(&entry.idempotency_key, &entry.payload)?),
        );
        Ok(())
    }

    pub fn stage_entry_in_transaction(
        &self,
        tx: &mut Transaction,
        entry: OutboxEntry,
    ) -> Result<(), StorageError> {
        tx.write(
            &self.outbox_table,
            entry.outbox_id,
            Value::bytes(encode_outbox_value(&entry.idempotency_key, &entry.payload)?),
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DurableCursorStore {
    cursor_table: Table,
}

impl DurableCursorStore {
    pub fn new(cursor_table: Table) -> Self {
        Self { cursor_table }
    }

    pub fn cursor_table(&self) -> &Table {
        &self.cursor_table
    }

    pub async fn load(&self, consumer_id: &[u8]) -> Result<LogCursor, ReadError> {
        let Some(value) = self.cursor_table.read(consumer_id.to_vec()).await? else {
            return Ok(LogCursor::beginning());
        };
        decode_cursor_value(&value).map_err(ReadError::from)
    }

    pub fn stage_persist_in_transaction(
        &self,
        tx: &mut Transaction,
        consumer_id: Key,
        cursor: LogCursor,
    ) {
        tx.write(
            &self.cursor_table,
            consumer_id,
            Value::bytes(encode_cursor_value(cursor)),
        );
    }

    pub async fn persist(
        &self,
        db: &Db,
        consumer_id: Key,
        cursor: LogCursor,
    ) -> Result<(), TransactionCommitError> {
        let mut tx = Transaction::begin(db).await;
        self.stage_persist_in_transaction(&mut tx, consumer_id, cursor);
        tx.commit().await.map(|_| ())
    }
}

#[derive(Clone, Debug)]
pub struct DurableOutboxConsumer {
    db: Db,
    outbox: TransactionalOutbox,
    cursors: DurableCursorStore,
    consumer_id: Key,
    cursor: LogCursor,
}

impl DurableOutboxConsumer {
    pub async fn open(
        db: &Db,
        outbox_table: Table,
        cursor_table: Table,
        consumer_id: Key,
    ) -> Result<Self, ReadError> {
        let cursors = DurableCursorStore::new(cursor_table);
        let cursor = cursors.load(&consumer_id).await?;
        Ok(Self {
            db: db.clone(),
            outbox: TransactionalOutbox::new(outbox_table),
            cursors,
            consumer_id,
            cursor,
        })
    }

    pub fn cursor(&self) -> LogCursor {
        self.cursor
    }

    pub fn subscribe_durable(&self) -> WatermarkReceiver {
        self.outbox.subscribe_durable(&self.db)
    }

    pub async fn poll(&self, limit: Option<usize>) -> Result<OutboxBatch, ReadError> {
        let mut stream = self
            .db
            .scan_durable_since(
                self.outbox.outbox_table(),
                self.cursor,
                ScanOptions {
                    limit,
                    ..ScanOptions::default()
                },
            )
            .await
            .map_err(ReadError::from)?;

        let mut entries = Vec::new();
        while let Some(change) = stream.next().await {
            entries.push(decode_outbox_message(change).map_err(ReadError::from)?);
        }

        Ok(OutboxBatch {
            start_cursor: self.cursor,
            entries,
        })
    }

    pub async fn persist_through(
        &mut self,
        cursor: LogCursor,
    ) -> Result<(), TransactionCommitError> {
        self.cursors
            .persist(&self.db, self.consumer_id.clone(), cursor)
            .await?;
        self.cursor = cursor;
        Ok(())
    }
}

fn timer_due_scan_start_key() -> Key {
    vec![0; 8]
}

fn timer_due_scan_end_key(now: Timestamp) -> Key {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(&now.get().to_be_bytes());
    key.push(u8::MAX);
    key
}

fn encode_timer_schedule_key(fire_at: Timestamp, timer_id: &[u8]) -> Key {
    let mut key = Vec::with_capacity(9 + timer_id.len());
    key.extend_from_slice(&fire_at.get().to_be_bytes());
    key.push(TIMER_KEY_SEPARATOR);
    key.extend_from_slice(timer_id);
    key
}

fn decode_timer_schedule_key(key: &[u8]) -> Result<(Timestamp, Key), StorageError> {
    if key.len() < 9 {
        return Err(StorageError::corruption(
            "timer schedule keys must be at least 9 bytes",
        ));
    }
    if key[8] != TIMER_KEY_SEPARATOR {
        return Err(StorageError::corruption(
            "timer schedule key separator is missing",
        ));
    }

    let mut fire_at = [0_u8; 8];
    fire_at.copy_from_slice(&key[..8]);
    Ok((
        Timestamp::new(u64::from_be_bytes(fire_at)),
        key[9..].to_vec(),
    ))
}

fn encode_timer_schedule_value(payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + payload.len());
    bytes.push(TIMER_SCHEDULE_VALUE_VERSION);
    bytes.extend_from_slice(payload);
    bytes
}

fn decode_timer_schedule_value(value: &Value) -> Result<Vec<u8>, StorageError> {
    let bytes = expect_bytes_value(value, "timer schedule")?;
    if bytes.first().copied() != Some(TIMER_SCHEDULE_VALUE_VERSION) {
        return Err(StorageError::corruption(
            "timer schedule value version is invalid",
        ));
    }
    Ok(bytes[1..].to_vec())
}

fn encode_timer_lookup_value(fire_at: Timestamp) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(9);
    bytes.push(TIMER_LOOKUP_VALUE_VERSION);
    bytes.extend_from_slice(&fire_at.get().to_be_bytes());
    bytes
}

fn decode_timer_lookup_value(value: &Value) -> Result<Timestamp, StorageError> {
    let bytes = expect_bytes_value(value, "timer lookup")?;
    if bytes.len() != 9 || bytes[0] != TIMER_LOOKUP_VALUE_VERSION {
        return Err(StorageError::corruption(
            "timer lookup value encoding is invalid",
        ));
    }

    let mut fire_at = [0_u8; 8];
    fire_at.copy_from_slice(&bytes[1..]);
    Ok(Timestamp::new(u64::from_be_bytes(fire_at)))
}

fn decode_due_timer(schedule_key: Key, value: Value) -> Result<DueTimer, StorageError> {
    let (fire_at, timer_id) = decode_timer_schedule_key(&schedule_key)?;
    let payload = decode_timer_schedule_value(&value)?;
    Ok(DueTimer {
        schedule_key,
        timer_id,
        fire_at,
        payload,
    })
}

fn encode_outbox_value(idempotency_key: &str, payload: &[u8]) -> Result<Vec<u8>, StorageError> {
    let key_len = u32::try_from(idempotency_key.len()).map_err(|_| {
        StorageError::unsupported("outbox idempotency keys must fit in u32::MAX bytes")
    })?;

    let mut bytes = Vec::with_capacity(1 + 4 + idempotency_key.len() + payload.len());
    bytes.push(OUTBOX_VALUE_VERSION);
    bytes.extend_from_slice(&key_len.to_be_bytes());
    bytes.extend_from_slice(idempotency_key.as_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn decode_outbox_value(value: &Value) -> Result<(String, Vec<u8>), StorageError> {
    let bytes = expect_bytes_value(value, "outbox")?;
    if bytes.len() < 5 || bytes[0] != OUTBOX_VALUE_VERSION {
        return Err(StorageError::corruption("outbox value encoding is invalid"));
    }

    let mut key_len = [0_u8; 4];
    key_len.copy_from_slice(&bytes[1..5]);
    let key_len = u32::from_be_bytes(key_len) as usize;
    let header_len = 5 + key_len;
    if bytes.len() < header_len {
        return Err(StorageError::corruption(
            "outbox value ended before the idempotency key finished",
        ));
    }

    let idempotency_key = String::from_utf8(bytes[5..header_len].to_vec())
        .map_err(|_| StorageError::corruption("outbox idempotency key is not valid utf-8"))?;
    Ok((idempotency_key, bytes[header_len..].to_vec()))
}

fn encode_cursor_value(cursor: LogCursor) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(1 + LogCursor::ENCODED_LEN);
    bytes.push(CURSOR_VALUE_VERSION);
    bytes.extend_from_slice(&cursor.encode());
    bytes
}

fn decode_cursor_value(value: &Value) -> Result<LogCursor, StorageError> {
    let bytes = expect_bytes_value(value, "cursor")?;
    if bytes.len() != 1 + LogCursor::ENCODED_LEN || bytes[0] != CURSOR_VALUE_VERSION {
        return Err(StorageError::corruption("cursor value encoding is invalid"));
    }
    LogCursor::decode(&bytes[1..]).map_err(|error| StorageError::corruption(error.to_string()))
}

fn decode_outbox_message(change: crate::ChangeEntry) -> Result<OutboxMessage, StorageError> {
    if change.kind != ChangeKind::Put {
        return Err(StorageError::corruption(
            "transactional outbox consumers only support put entries",
        ));
    }
    let value = change
        .value
        .ok_or_else(|| StorageError::corruption("outbox change is missing its value"))?;
    let (idempotency_key, payload) = decode_outbox_value(&value)?;
    Ok(OutboxMessage {
        entry: OutboxEntry {
            outbox_id: change.key,
            idempotency_key,
            payload,
        },
        cursor: change.cursor,
        sequence: change.sequence,
    })
}

fn expect_bytes_value<'a>(value: &'a Value, context: &str) -> Result<&'a [u8], StorageError> {
    match value {
        Value::Bytes(bytes) => Ok(bytes),
        Value::Record(_) => Err(StorageError::corruption(format!(
            "{context} helper values must use byte encoding"
        ))),
    }
}
