use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use crc32fast::hash as checksum32;
use futures::stream;
use serde::{Deserialize, Serialize};

use crate::{
    api::{ChangeKind, ChangeStream, FieldValue, ScanOptions, Table, Value},
    error::{ChangeFeedError, CommitError, FlushError, StorageError},
    ids::{CommitId, FieldId, LogCursor, SegmentId, SequenceNumber, TableId},
    io::{FileHandle, FileSystem, OpenOptions},
};

const RECORD_MAGIC: [u8; 4] = *b"TDBR";
const FOOTER_MAGIC: [u8; 8] = *b"TDBFTR1\0";
const RECORD_HEADER_LEN: usize = 12;
const FOOTER_TRAILER_LEN: usize = 16;
const FORMAT_VERSION: u8 = 1;
const DEFAULT_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RECORDS_PER_BLOCK: u64 = 64;
const FILE_READ_CHUNK_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub struct CommitRecord {
    pub id: CommitId,
    pub entries: Vec<CommitEntry>,
}

impl CommitRecord {
    pub fn sequence(&self) -> SequenceNumber {
        self.id.sequence()
    }

    pub fn encode_frame(&self) -> Result<Vec<u8>, StorageError> {
        let payload = self.encode_payload()?;
        let payload_len = u32::try_from(payload.len())
            .map_err(|_| StorageError::unsupported("commit record payload exceeds 4 GiB"))?;
        let checksum = checksum32(&payload);

        let mut bytes = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
        bytes.extend_from_slice(&RECORD_MAGIC);
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(&checksum.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    pub fn decode_frame(bytes: &[u8]) -> Result<Self, StorageError> {
        if bytes.len() < RECORD_HEADER_LEN {
            return Err(StorageError::corruption(
                "commit record frame shorter than header",
            ));
        }
        if bytes[..4] != RECORD_MAGIC {
            return Err(StorageError::corruption("commit record magic mismatch"));
        }

        let payload_len = u32::from_be_bytes(bytes[4..8].try_into().expect("payload len")) as usize;
        let checksum = u32::from_be_bytes(bytes[8..12].try_into().expect("checksum"));
        let expected_len = RECORD_HEADER_LEN + payload_len;
        if bytes.len() != expected_len {
            return Err(StorageError::corruption(format!(
                "commit record frame length mismatch: expected {expected_len} bytes, got {}",
                bytes.len()
            )));
        }

        let payload = &bytes[RECORD_HEADER_LEN..];
        if checksum32(payload) != checksum {
            return Err(StorageError::corruption("commit record checksum mismatch"));
        }

        Self::decode_payload(payload)
    }

    fn encode_payload(&self) -> Result<Vec<u8>, StorageError> {
        let entry_count = u16::try_from(self.entries.len())
            .map_err(|_| StorageError::unsupported("commit record has more than 65535 entries"))?;

        let mut bytes = Vec::new();
        push_u8(&mut bytes, FORMAT_VERSION);
        bytes.extend_from_slice(&self.id.encode());
        push_u16(&mut bytes, entry_count);
        for entry in &self.entries {
            entry.encode(&mut bytes)?;
        }
        Ok(bytes)
    }

    fn decode_payload(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut cursor = ByteCursor::new(bytes);
        let version = cursor.read_u8()?;
        if version != FORMAT_VERSION {
            return Err(StorageError::corruption(format!(
                "unsupported commit record format version {version}"
            )));
        }

        let id_bytes = cursor.read_exact(CommitId::ENCODED_LEN)?;
        let id = CommitId::decode(id_bytes)
            .map_err(|error| StorageError::corruption(format!("invalid commit id: {error}")))?;
        let entry_count = cursor.read_u16()? as usize;

        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(CommitEntry::decode(&mut cursor)?);
        }

        if cursor.remaining() != 0 {
            return Err(StorageError::corruption(
                "commit record payload contains trailing bytes",
            ));
        }

        Ok(Self { id, entries })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommitEntry {
    pub op_index: u16,
    pub table_id: TableId,
    pub kind: ChangeKind,
    pub key: Vec<u8>,
    pub value: Option<Value>,
}

impl CommitEntry {
    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), StorageError> {
        push_u16(bytes, self.op_index);
        push_u32(bytes, self.table_id.get());
        push_u8(bytes, encode_change_kind(self.kind));
        push_len(bytes, self.key.len())?;
        bytes.extend_from_slice(&self.key);

        match &self.value {
            Some(value) => {
                let mut encoded_value = Vec::new();
                encode_value(&mut encoded_value, value)?;
                push_u32(
                    bytes,
                    u32::try_from(encoded_value.len()).map_err(|_| {
                        StorageError::unsupported("commit entry value exceeds 4 GiB")
                    })?,
                );
                bytes.extend_from_slice(&encoded_value);
            }
            None => push_u32(bytes, u32::MAX),
        }

        Ok(())
    }

    fn decode(cursor: &mut ByteCursor<'_>) -> Result<Self, StorageError> {
        let op_index = cursor.read_u16()?;
        let table_id = TableId::new(cursor.read_u32()?);
        let kind = decode_change_kind(cursor.read_u8()?)?;
        let key = cursor.read_len_prefixed_bytes()?;
        let value_len = cursor.read_u32()?;
        let value = if value_len == u32::MAX {
            None
        } else {
            let value_bytes = cursor.read_exact(value_len as usize)?;
            Some(decode_value(value_bytes)?)
        };

        Ok(Self {
            op_index,
            table_id,
            kind,
            key: key.to_vec(),
            value,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockIndexEntry {
    pub sequence: SequenceNumber,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSegmentMeta {
    pub table_id: TableId,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub entry_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentFooter {
    pub segment_id: SegmentId,
    pub min_sequence: SequenceNumber,
    pub max_sequence: SequenceNumber,
    pub record_count: u64,
    pub entry_count: u64,
    pub data_end_offset: u64,
    pub tables: Vec<TableSegmentMeta>,
    pub block_index: Vec<BlockIndexEntry>,
}

impl SegmentFooter {
    pub fn encode(&self) -> Result<Vec<u8>, StorageError> {
        let table_count = u32::try_from(self.tables.len())
            .map_err(|_| StorageError::unsupported("segment footer has too many tables"))?;
        let block_count = u32::try_from(self.block_index.len())
            .map_err(|_| StorageError::unsupported("segment footer has too many blocks"))?;

        let mut bytes = Vec::new();
        push_u8(&mut bytes, FORMAT_VERSION);
        push_u64(&mut bytes, self.segment_id.get());
        push_u64(&mut bytes, self.min_sequence.get());
        push_u64(&mut bytes, self.max_sequence.get());
        push_u64(&mut bytes, self.record_count);
        push_u64(&mut bytes, self.entry_count);
        push_u64(&mut bytes, self.data_end_offset);
        push_u32(&mut bytes, table_count);
        for table in &self.tables {
            push_u32(&mut bytes, table.table_id.get());
            push_u64(&mut bytes, table.min_sequence.get());
            push_u64(&mut bytes, table.max_sequence.get());
            push_u32(&mut bytes, table.entry_count);
        }
        push_u32(&mut bytes, block_count);
        for block in &self.block_index {
            push_u64(&mut bytes, block.sequence.get());
            push_u64(&mut bytes, block.offset);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, StorageError> {
        let mut cursor = ByteCursor::new(bytes);
        let version = cursor.read_u8()?;
        if version != FORMAT_VERSION {
            return Err(StorageError::corruption(format!(
                "unsupported segment footer format version {version}"
            )));
        }

        let segment_id = SegmentId::new(cursor.read_u64()?);
        let min_sequence = SequenceNumber::new(cursor.read_u64()?);
        let max_sequence = SequenceNumber::new(cursor.read_u64()?);
        let record_count = cursor.read_u64()?;
        let entry_count = cursor.read_u64()?;
        let data_end_offset = cursor.read_u64()?;

        let table_count = cursor.read_u32()? as usize;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            tables.push(TableSegmentMeta {
                table_id: TableId::new(cursor.read_u32()?),
                min_sequence: SequenceNumber::new(cursor.read_u64()?),
                max_sequence: SequenceNumber::new(cursor.read_u64()?),
                entry_count: cursor.read_u32()?,
            });
        }

        let block_count = cursor.read_u32()? as usize;
        let mut block_index = Vec::with_capacity(block_count);
        for _ in 0..block_count {
            block_index.push(BlockIndexEntry {
                sequence: SequenceNumber::new(cursor.read_u64()?),
                offset: cursor.read_u64()?,
            });
        }

        if cursor.remaining() != 0 {
            return Err(StorageError::corruption(
                "segment footer contains trailing bytes",
            ));
        }

        Ok(Self {
            segment_id,
            min_sequence,
            max_sequence,
            record_count,
            entry_count,
            data_end_offset,
            tables,
            block_index,
        })
    }

    pub fn seek_offset(&self, sequence: SequenceNumber) -> u64 {
        self.block_index
            .iter()
            .rev()
            .find(|entry| entry.sequence <= sequence)
            .map(|entry| entry.offset)
            .unwrap_or(0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentDescriptor {
    pub segment_id: SegmentId,
    pub sealed: bool,
    pub min_sequence: Option<SequenceNumber>,
    pub max_sequence: Option<SequenceNumber>,
    pub record_count: u64,
    pub entry_count: u64,
    pub tables: Vec<TableSegmentMeta>,
}

#[derive(Clone)]
pub(crate) struct LocalSegmentScanPlan {
    pub fs: Arc<dyn FileSystem>,
    pub segment_id: SegmentId,
    pub path: String,
    #[allow(dead_code)]
    pub read_len: Option<u64>,
    pub min_sequence: Option<SequenceNumber>,
    pub max_sequence: Option<SequenceNumber>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendLocation {
    pub segment_id: SegmentId,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentOptions {
    pub max_segment_size_bytes: u64,
    pub records_per_block: u64,
}

impl Default for SegmentOptions {
    fn default() -> Self {
        Self {
            max_segment_size_bytes: DEFAULT_SEGMENT_SIZE_BYTES,
            records_per_block: DEFAULT_RECORDS_PER_BLOCK,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SegmentCatalog {
    footers: Vec<SegmentFooter>,
    table_index: BTreeMap<TableId, Vec<TableSegmentSpan>>,
}

impl SegmentCatalog {
    fn new(mut footers: Vec<SegmentFooter>) -> Self {
        footers.sort_by_key(|footer| footer.segment_id.get());

        let mut table_index: BTreeMap<TableId, Vec<TableSegmentSpan>> = BTreeMap::new();
        for footer in &footers {
            for table in &footer.tables {
                table_index
                    .entry(table.table_id)
                    .or_default()
                    .push(TableSegmentSpan {
                        segment_id: footer.segment_id,
                        min_sequence: table.min_sequence,
                        max_sequence: table.max_sequence,
                    });
            }
        }

        for spans in table_index.values_mut() {
            spans.sort_by_key(|span| (span.min_sequence.get(), span.segment_id.get()));
        }

        Self {
            footers,
            table_index,
        }
    }

    fn insert(&mut self, footer: SegmentFooter) {
        self.footers.push(footer);
        *self = Self::new(std::mem::take(&mut self.footers));
    }

    fn remove_segments(&mut self, deleted: &BTreeSet<SegmentId>) {
        if deleted.is_empty() {
            return;
        }

        self.footers
            .retain(|footer| !deleted.contains(&footer.segment_id));
        *self = Self::new(std::mem::take(&mut self.footers));
    }

    pub fn footers(&self) -> &[SegmentFooter] {
        &self.footers
    }

    pub fn seek_by_sequence(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
    ) -> Option<SegmentId> {
        self.table_index.get(&table_id).and_then(|spans| {
            spans
                .iter()
                .find(|span| span.max_sequence >= sequence)
                .map(|span| span.segment_id)
        })
    }

    pub fn oldest_sequence(&self, table_id: TableId) -> Option<SequenceNumber> {
        self.table_index
            .get(&table_id)
            .and_then(|spans| spans.first().map(|span| span.min_sequence))
    }

    pub fn segment_ids_for_table_since(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
    ) -> Vec<SegmentId> {
        self.table_index
            .get(&table_id)
            .into_iter()
            .flat_map(|spans| spans.iter())
            .filter(|span| span.max_sequence >= sequence)
            .map(|span| span.segment_id)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct TableSegmentSpan {
    segment_id: SegmentId,
    min_sequence: SequenceNumber,
    max_sequence: SequenceNumber,
}

pub struct SegmentManager {
    fs: Arc<dyn FileSystem>,
    dir: String,
    options: SegmentOptions,
    catalog: SegmentCatalog,
    active: ActiveSegment,
}

impl SegmentManager {
    pub async fn open(
        fs: Arc<dyn FileSystem>,
        dir: impl Into<String>,
        mut options: SegmentOptions,
    ) -> Result<Self, StorageError> {
        if options.max_segment_size_bytes == 0 {
            options.max_segment_size_bytes = DEFAULT_SEGMENT_SIZE_BYTES;
        }
        if options.records_per_block == 0 {
            options.records_per_block = DEFAULT_RECORDS_PER_BLOCK;
        }

        let dir = dir.into();
        let mut segments = list_segment_paths(fs.as_ref(), &dir).await?;
        segments.sort_by_key(|(segment_id, _)| segment_id.get());

        let mut footers = Vec::new();
        let mut active = None;
        let highest_segment_id = segments.last().map(|(segment_id, _)| *segment_id);

        for (segment_id, path) in segments {
            let bytes = read_file(fs.as_ref(), &path).await?;
            match parse_segment_footer(&bytes)? {
                Some(footer) => footers.push(footer),
                None => {
                    if Some(segment_id) != highest_segment_id {
                        return Err(StorageError::corruption(format!(
                            "unsealed segment {} is not the newest segment",
                            segment_id.get()
                        )));
                    }

                    let handle = fs
                        .open(
                            &path,
                            OpenOptions {
                                create: false,
                                read: true,
                                write: true,
                                truncate: false,
                                append: false,
                            },
                        )
                        .await?;
                    let recovered = ActiveSegment::recover(
                        segment_id,
                        path.clone(),
                        handle,
                        bytes,
                        options.records_per_block,
                    )?;
                    if let Some(repaired_tail) = recovered.repaired_tail.as_ref() {
                        Self::repair_active_segment(fs.as_ref(), &path, repaired_tail).await?;
                    }
                    active = Some(recovered.segment);
                }
            }
        }

        let catalog = SegmentCatalog::new(footers);
        let next_segment_id = SegmentId::new(
            highest_segment_id
                .map(|segment_id| segment_id.get().saturating_add(1))
                .unwrap_or(1),
        );
        let active = match active {
            Some(active) => active,
            None => ActiveSegment::create(fs.as_ref(), &dir, next_segment_id).await?,
        };

        Ok(Self {
            fs,
            dir,
            options,
            catalog,
            active,
        })
    }

    pub fn enumerate_segments(&self) -> Vec<SegmentDescriptor> {
        let mut descriptors = self
            .catalog
            .footers()
            .iter()
            .map(SegmentDescriptor::from)
            .collect::<Vec<_>>();
        descriptors.push(self.active.descriptor());
        descriptors
    }

    pub fn sealed_catalog(&self) -> &SegmentCatalog {
        &self.catalog
    }

    pub fn active_segment_id(&self) -> SegmentId {
        self.active.id
    }

    pub async fn read_segment_bytes(&self, segment_id: SegmentId) -> Result<Vec<u8>, StorageError> {
        if self.active.id == segment_id {
            return read_file(self.fs.as_ref(), &self.active.path).await;
        }

        read_file(self.fs.as_ref(), &segment_path(&self.dir, segment_id)).await
    }

    pub fn oldest_segment_id(&self) -> Option<SegmentId> {
        self.catalog
            .footers()
            .first()
            .map(|footer| footer.segment_id)
            .or_else(|| (self.active.record_count > 0).then_some(self.active.id))
    }

    pub fn oldest_sequence_for_table(&self, table_id: TableId) -> Option<SequenceNumber> {
        self.catalog
            .oldest_sequence(table_id)
            .into_iter()
            .chain(self.active.oldest_sequence_for_table(table_id))
            .min()
    }

    pub fn seek_by_sequence(
        &self,
        table_id: TableId,
        sequence: SequenceNumber,
    ) -> Option<SegmentId> {
        self.catalog
            .seek_by_sequence(table_id, sequence)
            .or_else(|| self.active.seek_by_sequence(table_id, sequence))
    }

    pub async fn append(&mut self, record: CommitRecord) -> Result<AppendLocation, StorageError> {
        self.append_internal(record, true).await
    }

    pub async fn append_without_seal(
        &mut self,
        record: CommitRecord,
    ) -> Result<AppendLocation, StorageError> {
        self.append_internal(record, false).await
    }

    pub fn active_len(&self) -> u64 {
        self.active.len
    }

    pub async fn rollback_active_to_len(&mut self, len: u64) -> Result<(), StorageError> {
        if len >= self.active.len {
            return Ok(());
        }

        let bytes = read_file(self.fs.as_ref(), &self.active.path).await?;
        let repaired = bytes
            .get(..len as usize)
            .ok_or_else(|| {
                StorageError::corruption(format!(
                    "cannot roll back active segment {} to {len} bytes; current length is {}",
                    self.active.id.get(),
                    bytes.len(),
                ))
            })?
            .to_vec();
        Self::repair_active_segment(self.fs.as_ref(), &self.active.path, &repaired).await?;
        self.active = ActiveSegment::recover(
            self.active.id,
            self.active.path.clone(),
            FileHandle::new(self.active.path.clone()),
            repaired,
            self.options.records_per_block,
        )?
        .segment;
        Ok(())
    }

    async fn append_internal(
        &mut self,
        record: CommitRecord,
        allow_seal: bool,
    ) -> Result<AppendLocation, StorageError> {
        let frame = record.encode_frame()?;
        if allow_seal
            && self.active.record_count > 0
            && self.active.len + frame.len() as u64 > self.options.max_segment_size_bytes
        {
            self.seal_active().await?;
        }

        let offset = self.active.len;
        self.fs
            .write_at(&self.active.handle, offset, &frame)
            .await?;
        self.active.observe(
            offset,
            &record,
            frame.len() as u64,
            self.options.records_per_block,
        );

        Ok(AppendLocation {
            segment_id: self.active.id,
            offset,
        })
    }

    pub async fn append_batch_and_sync(
        &mut self,
        records: &[CommitRecord],
    ) -> Result<(), StorageError> {
        if records.is_empty() {
            return Ok(());
        }

        let mut frames = Vec::with_capacity(records.len());
        let mut batch_bytes = 0_u64;
        for record in records {
            let frame = record.encode_frame()?;
            batch_bytes = batch_bytes.saturating_add(frame.len() as u64);
            frames.push(frame);
        }

        if self.active.record_count > 0
            && self.active.len.saturating_add(batch_bytes) > self.options.max_segment_size_bytes
        {
            self.seal_active().await?;
        }

        let committed_len = self.active.len;
        let mut offset = committed_len;
        for frame in &frames {
            if let Err(error) = self.fs.write_at(&self.active.handle, offset, frame).await {
                self.restore_active_prefix(committed_len).await?;
                return Err(error);
            }
            offset = offset.saturating_add(frame.len() as u64);
        }

        if let Err(error) = self.fs.sync(&self.active.handle).await {
            self.restore_active_prefix(committed_len).await?;
            return Err(error);
        }

        let mut observed_offset = committed_len;
        for (record, frame) in records.iter().zip(frames.iter()) {
            self.active.observe(
                observed_offset,
                record,
                frame.len() as u64,
                self.options.records_per_block,
            );
            observed_offset = observed_offset.saturating_add(frame.len() as u64);
        }

        Ok(())
    }

    pub async fn rollback_to(&mut self, location: AppendLocation) -> Result<(), StorageError> {
        let target_path = segment_path(&self.dir, location.segment_id);
        let target_bytes = if self.active.id == location.segment_id {
            read_file(self.fs.as_ref(), &self.active.path).await?
        } else {
            read_file(self.fs.as_ref(), &target_path).await?
        };
        let rollback_offset = usize::try_from(location.offset).map_err(|_| {
            StorageError::corruption(format!(
                "rollback offset {} does not fit in usize",
                location.offset
            ))
        })?;
        if rollback_offset > target_bytes.len() {
            return Err(StorageError::corruption(format!(
                "rollback offset {} exceeds segment {} length {}",
                location.offset,
                location.segment_id.get(),
                target_bytes.len()
            )));
        }

        let prefix = target_bytes[..rollback_offset].to_vec();
        let removed_catalog_ids = self
            .catalog
            .footers()
            .iter()
            .filter(|footer| footer.segment_id >= location.segment_id)
            .map(|footer| footer.segment_id)
            .collect::<BTreeSet<_>>();
        let deleted_segment_ids = removed_catalog_ids
            .iter()
            .copied()
            .filter(|segment_id| *segment_id > location.segment_id)
            .collect::<Vec<_>>();

        for segment_id in &deleted_segment_ids {
            self.fs
                .delete(&segment_path(&self.dir, *segment_id))
                .await?;
        }
        if self.active.id > location.segment_id {
            self.fs.delete(&self.active.path).await?;
        }
        self.catalog.remove_segments(&removed_catalog_ids);

        Self::repair_active_segment(self.fs.as_ref(), &target_path, &prefix).await?;
        self.fs.sync_dir(&self.dir).await?;

        let handle = self
            .fs
            .open(
                &target_path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await?;
        let recovered = ActiveSegment::recover(
            location.segment_id,
            target_path,
            handle,
            prefix,
            self.options.records_per_block,
        )?;
        self.active = recovered.segment;

        Ok(())
    }

    pub async fn seal_active(&mut self) -> Result<Option<SegmentFooter>, StorageError> {
        if self.active.record_count == 0 {
            return Ok(None);
        }

        let footer = self.active.footer()?;
        let footer_bytes = footer.encode()?;
        let footer_len = u32::try_from(footer_bytes.len())
            .map_err(|_| StorageError::unsupported("segment footer exceeds 4 GiB"))?;
        let footer_checksum = checksum32(&footer_bytes);

        let mut trailer = Vec::with_capacity(FOOTER_TRAILER_LEN);
        push_u32(&mut trailer, footer_len);
        push_u32(&mut trailer, footer_checksum);
        trailer.extend_from_slice(&FOOTER_MAGIC);

        let footer_offset = self.active.len;
        self.fs
            .write_at(&self.active.handle, footer_offset, &footer_bytes)
            .await?;
        self.fs
            .write_at(
                &self.active.handle,
                footer_offset + footer_bytes.len() as u64,
                &trailer,
            )
            .await?;
        self.fs.sync(&self.active.handle).await?;

        self.catalog.insert(footer.clone());

        let next_segment_id = SegmentId::new(self.active.id.get().saturating_add(1));
        self.active = ActiveSegment::create(self.fs.as_ref(), &self.dir, next_segment_id).await?;

        Ok(Some(footer))
    }

    pub async fn prune_sealed_before(
        &mut self,
        sequence_exclusive: SequenceNumber,
        protected_segments: &BTreeSet<SegmentId>,
    ) -> Result<Vec<SegmentFooter>, StorageError> {
        let deleted = self
            .catalog
            .footers()
            .iter()
            .filter(|footer| footer.max_sequence < sequence_exclusive)
            .filter(|footer| !protected_segments.contains(&footer.segment_id))
            .cloned()
            .collect::<Vec<_>>();
        if deleted.is_empty() {
            return Ok(Vec::new());
        }

        let deleted_ids = deleted
            .iter()
            .map(|footer| footer.segment_id)
            .collect::<BTreeSet<_>>();
        for footer in &deleted {
            self.fs
                .delete(&segment_path(&self.dir, footer.segment_id))
                .await?;
        }
        self.catalog.remove_segments(&deleted_ids);

        Ok(deleted)
    }

    pub async fn sync_active(&mut self) -> Result<(), StorageError> {
        if self.active.len == 0 {
            return Ok(());
        }

        self.fs.sync(&self.active.handle).await
    }

    async fn restore_active_prefix(&self, len: u64) -> Result<(), StorageError> {
        let prefix = if len == 0 {
            Vec::new()
        } else {
            let mut bytes = read_file(self.fs.as_ref(), &self.active.path).await?;
            bytes.truncate(len as usize);
            bytes
        };
        let handle = self
            .fs
            .open(
                &self.active.path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        if !prefix.is_empty() {
            self.fs.write_at(&handle, 0, &prefix).await?;
        }
        Ok(())
    }

    pub async fn read_from_sequence(
        &self,
        segment_id: SegmentId,
        sequence: SequenceNumber,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        if self.active.id == segment_id {
            let bytes = read_file(self.fs.as_ref(), &self.active.path).await?;
            return parse_records_range(&bytes, 0, bytes.len(), Some(sequence));
        }

        let footer = self
            .catalog
            .footers()
            .iter()
            .find(|footer| footer.segment_id == segment_id)
            .ok_or_else(|| {
                StorageError::not_found(format!("unknown segment {}", segment_id.get()))
            })?;
        let reader =
            SegmentReader::open(self.fs.clone(), segment_path(&self.dir, segment_id)).await?;
        if reader.footer.segment_id != footer.segment_id {
            return Err(StorageError::corruption(
                "segment footer id mismatch after open",
            ));
        }
        reader.read_from_sequence(sequence)
    }

    pub async fn scan_from_sequence(
        &self,
        sequence_exclusive: SequenceNumber,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let mut records = Vec::new();
        let first_needed = SequenceNumber::new(sequence_exclusive.get().saturating_add(1));

        for descriptor in self.enumerate_segments() {
            let Some(max_sequence) = descriptor.max_sequence else {
                continue;
            };
            if max_sequence <= sequence_exclusive {
                continue;
            }

            records.extend(
                self.read_from_sequence(descriptor.segment_id, first_needed)
                    .await?
                    .into_iter()
                    .filter(|record| record.sequence() > sequence_exclusive),
            );
        }

        Ok(records)
    }

    pub async fn scan_table_from_sequence(
        &self,
        table_id: TableId,
        sequence_inclusive: SequenceNumber,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let mut records = Vec::new();

        for segment_id in self
            .catalog
            .segment_ids_for_table_since(table_id, sequence_inclusive)
        {
            records.extend(
                self.read_from_sequence(segment_id, sequence_inclusive)
                    .await?
                    .into_iter()
                    .filter(|record| record.sequence() >= sequence_inclusive)
                    .filter(|record| record_touches_table(record, table_id)),
            );
        }

        if self
            .active
            .has_entries_for_table_since(table_id, sequence_inclusive)
        {
            records.extend(
                self.read_from_sequence(self.active.id, sequence_inclusive)
                    .await?
                    .into_iter()
                    .filter(|record| record.sequence() >= sequence_inclusive)
                    .filter(|record| record_touches_table(record, table_id)),
            );
        }

        Ok(records)
    }

    pub(crate) fn table_scan_plans_since(
        &self,
        table_id: TableId,
        sequence_inclusive: SequenceNumber,
    ) -> Vec<LocalSegmentScanPlan> {
        self.enumerate_segments()
            .into_iter()
            .filter(|descriptor| descriptor.max_sequence >= Some(sequence_inclusive))
            .filter(|descriptor| {
                descriptor
                    .tables
                    .iter()
                    .any(|table| table.table_id == table_id)
            })
            .map(|descriptor| LocalSegmentScanPlan {
                fs: self.fs.clone(),
                segment_id: descriptor.segment_id,
                path: if descriptor.sealed {
                    segment_path(&self.dir, descriptor.segment_id)
                } else {
                    self.active.path.clone()
                },
                read_len: (!descriptor.sealed).then_some(self.active.len),
                min_sequence: descriptor.min_sequence,
                max_sequence: descriptor.max_sequence,
            })
            .collect()
    }

    async fn repair_active_segment(
        fs: &dyn FileSystem,
        path: &str,
        bytes: &[u8],
    ) -> Result<(), StorageError> {
        let handle = fs
            .open(
                path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await?;
        fs.write_at(&handle, 0, bytes).await?;
        fs.sync(&handle).await
    }
}

pub fn inspect_segment_bytes(
    segment_id: SegmentId,
    bytes: &[u8],
) -> Result<SegmentDescriptor, StorageError> {
    Ok(SegmentDescriptor::from(&segment_footer_from_bytes(
        segment_id, bytes,
    )?))
}

pub(crate) fn segment_footer_from_bytes(
    segment_id: SegmentId,
    bytes: &[u8],
) -> Result<SegmentFooter, StorageError> {
    if let Some(footer) = parse_segment_footer(bytes)? {
        return Ok(footer);
    }

    let recovered = ActiveSegment::recover(
        segment_id,
        format!("segment-{:06}", segment_id.get()),
        FileHandle::new(format!("segment-{:06}", segment_id.get())),
        bytes.to_vec(),
        DEFAULT_RECORDS_PER_BLOCK,
    )?;
    recovered.segment.footer()
}

struct ActiveSegmentRecovery {
    segment: ActiveSegment,
    repaired_tail: Option<Vec<u8>>,
}

struct ActiveSegment {
    id: SegmentId,
    path: String,
    handle: FileHandle,
    len: u64,
    record_count: u64,
    entry_count: u64,
    min_sequence: Option<SequenceNumber>,
    max_sequence: Option<SequenceNumber>,
    tables: BTreeMap<TableId, TableMetaAccumulator>,
    block_index: Vec<BlockIndexEntry>,
}

impl ActiveSegment {
    async fn create(
        fs: &dyn FileSystem,
        dir: &str,
        segment_id: SegmentId,
    ) -> Result<Self, StorageError> {
        let path = segment_path(dir, segment_id);
        let handle = fs
            .open(
                &path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: false,
                    append: false,
                },
            )
            .await?;

        Ok(Self {
            id: segment_id,
            path,
            handle,
            len: 0,
            record_count: 0,
            entry_count: 0,
            min_sequence: None,
            max_sequence: None,
            tables: BTreeMap::new(),
            block_index: Vec::new(),
        })
    }

    fn recover(
        id: SegmentId,
        path: String,
        handle: FileHandle,
        bytes: Vec<u8>,
        records_per_block: u64,
    ) -> Result<ActiveSegmentRecovery, StorageError> {
        let mut segment = Self {
            id,
            path,
            handle,
            len: 0,
            record_count: 0,
            entry_count: 0,
            min_sequence: None,
            max_sequence: None,
            tables: BTreeMap::new(),
            block_index: Vec::new(),
        };

        let mut offset = 0;
        while offset < bytes.len() {
            match parse_record_frame(&bytes, offset) {
                Ok(next) => {
                    let frame_len = next.1 - offset;
                    segment.observe(offset as u64, &next.0, frame_len as u64, records_per_block);
                    offset = next.1;
                }
                Err(_) => break,
            }
        }
        segment.len = offset as u64;

        let repaired_tail = (offset < bytes.len()).then(|| bytes[..offset].to_vec());

        Ok(ActiveSegmentRecovery {
            segment,
            repaired_tail,
        })
    }

    fn observe(
        &mut self,
        offset: u64,
        record: &CommitRecord,
        frame_len: u64,
        records_per_block: u64,
    ) {
        if self.record_count.is_multiple_of(records_per_block) {
            self.block_index.push(BlockIndexEntry {
                sequence: record.sequence(),
                offset,
            });
        }

        self.record_count += 1;
        self.entry_count += record.entries.len() as u64;
        self.len = offset + frame_len;
        self.min_sequence = Some(
            self.min_sequence
                .map(|current| current.min(record.sequence()))
                .unwrap_or(record.sequence()),
        );
        self.max_sequence = Some(
            self.max_sequence
                .map(|current| current.max(record.sequence()))
                .unwrap_or(record.sequence()),
        );

        for entry in &record.entries {
            let table = self.tables.entry(entry.table_id).or_default();
            table.min_sequence = Some(
                table
                    .min_sequence
                    .map(|current| current.min(record.sequence()))
                    .unwrap_or(record.sequence()),
            );
            table.max_sequence = Some(
                table
                    .max_sequence
                    .map(|current| current.max(record.sequence()))
                    .unwrap_or(record.sequence()),
            );
            table.entry_count = table.entry_count.saturating_add(1);
        }
    }

    fn footer(&self) -> Result<SegmentFooter, StorageError> {
        let min_sequence = self
            .min_sequence
            .ok_or_else(|| StorageError::unsupported("cannot seal an empty segment"))?;
        let max_sequence = self
            .max_sequence
            .ok_or_else(|| StorageError::unsupported("cannot seal an empty segment"))?;

        Ok(SegmentFooter {
            segment_id: self.id,
            min_sequence,
            max_sequence,
            record_count: self.record_count,
            entry_count: self.entry_count,
            data_end_offset: self.len,
            tables: self
                .tables
                .iter()
                .map(|(&table_id, meta)| {
                    Ok(TableSegmentMeta {
                        table_id,
                        min_sequence: meta.min_sequence.ok_or_else(|| {
                            StorageError::corruption("active segment table is missing min sequence")
                        })?,
                        max_sequence: meta.max_sequence.ok_or_else(|| {
                            StorageError::corruption("active segment table is missing max sequence")
                        })?,
                        entry_count: meta.entry_count,
                    })
                })
                .collect::<Result<Vec<_>, StorageError>>()?,
            block_index: self.block_index.clone(),
        })
    }

    fn descriptor(&self) -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: self.id,
            sealed: false,
            min_sequence: self.min_sequence,
            max_sequence: self.max_sequence,
            record_count: self.record_count,
            entry_count: self.entry_count,
            tables: self
                .tables
                .iter()
                .filter_map(|(&table_id, meta)| {
                    Some(TableSegmentMeta {
                        table_id,
                        min_sequence: meta.min_sequence?,
                        max_sequence: meta.max_sequence?,
                        entry_count: meta.entry_count,
                    })
                })
                .collect(),
        }
    }

    fn seek_by_sequence(&self, table_id: TableId, sequence: SequenceNumber) -> Option<SegmentId> {
        self.tables.get(&table_id).and_then(|meta| {
            let max_sequence = meta.max_sequence?;
            if max_sequence >= sequence {
                Some(self.id)
            } else {
                None
            }
        })
    }

    fn oldest_sequence_for_table(&self, table_id: TableId) -> Option<SequenceNumber> {
        self.tables
            .get(&table_id)
            .and_then(|meta| meta.min_sequence)
    }

    fn has_entries_for_table_since(&self, table_id: TableId, sequence: SequenceNumber) -> bool {
        self.tables
            .get(&table_id)
            .and_then(|meta| meta.max_sequence)
            .is_some_and(|max_sequence| max_sequence >= sequence)
    }
}

#[derive(Clone, Debug, Default)]
struct TableMetaAccumulator {
    min_sequence: Option<SequenceNumber>,
    max_sequence: Option<SequenceNumber>,
    entry_count: u32,
}

fn record_touches_table(record: &CommitRecord, table_id: TableId) -> bool {
    record
        .entries
        .iter()
        .any(|entry| entry.table_id == table_id)
}

impl From<&SegmentFooter> for SegmentDescriptor {
    fn from(footer: &SegmentFooter) -> Self {
        Self {
            segment_id: footer.segment_id,
            sealed: true,
            min_sequence: Some(footer.min_sequence),
            max_sequence: Some(footer.max_sequence),
            record_count: footer.record_count,
            entry_count: footer.entry_count,
            tables: footer.tables.clone(),
        }
    }
}

pub struct SegmentReader {
    bytes: Vec<u8>,
    pub footer: SegmentFooter,
}

impl SegmentReader {
    pub async fn open(
        fs: Arc<dyn FileSystem>,
        path: impl Into<String>,
    ) -> Result<Self, StorageError> {
        let path = path.into();
        let bytes = read_path(fs.as_ref(), &path).await?;
        let footer = parse_segment_footer(&bytes)?.ok_or_else(|| {
            StorageError::corruption(format!("segment {path} is missing a footer trailer"))
        })?;
        Ok(Self { bytes, footer })
    }

    pub fn read_from_sequence(
        &self,
        sequence: SequenceNumber,
    ) -> Result<Vec<CommitRecord>, StorageError> {
        let offset = self.footer.seek_offset(sequence) as usize;
        parse_records_range(
            &self.bytes,
            offset,
            self.footer.data_end_offset as usize,
            Some(sequence),
        )
    }
}

pub(crate) struct SegmentRecordScanner {
    bytes: Vec<u8>,
    offset: usize,
    end_offset: usize,
    min_sequence: SequenceNumber,
}

impl SegmentRecordScanner {
    pub(crate) fn new(
        segment_id: SegmentId,
        bytes: Vec<u8>,
        sequence_inclusive: SequenceNumber,
    ) -> Result<Self, StorageError> {
        let (offset, end_offset) = match parse_segment_footer(&bytes)? {
            Some(footer) => (
                footer.seek_offset(sequence_inclusive) as usize,
                footer.data_end_offset as usize,
            ),
            None => (0, bytes.len()),
        };
        if offset > end_offset || end_offset > bytes.len() {
            return Err(StorageError::corruption(format!(
                "segment {} scan range is out of bounds",
                segment_id.get()
            )));
        }

        Ok(Self {
            bytes,
            offset,
            end_offset,
            min_sequence: sequence_inclusive,
        })
    }

    pub(crate) fn next_record(&mut self) -> Result<Option<CommitRecord>, StorageError> {
        while self.offset < self.end_offset {
            let (record, next_offset) = parse_record_frame(&self.bytes, self.offset)?;
            if next_offset > self.end_offset {
                return Err(StorageError::corruption(
                    "commit record frame extends beyond declared data range",
                ));
            }
            self.offset = next_offset;
            if record.sequence() >= self.min_sequence {
                return Ok(Some(record));
            }
        }

        Ok(None)
    }
}

pub(crate) fn encode_segment_bytes(
    segment_id: SegmentId,
    records: &[CommitRecord],
    records_per_block: u64,
) -> Result<(Vec<u8>, SegmentFooter), StorageError> {
    if records.is_empty() {
        return Err(StorageError::unsupported(
            "cannot encode an empty commit-log segment",
        ));
    }

    let records_per_block = records_per_block.max(1);
    let mut bytes = Vec::new();
    let mut record_count = 0_u64;
    let mut entry_count = 0_u64;
    let mut min_sequence = None;
    let mut max_sequence = None;
    let mut tables: BTreeMap<TableId, TableMetaAccumulator> = BTreeMap::new();
    let mut block_index = Vec::new();

    for record in records {
        if record_count.is_multiple_of(records_per_block) {
            block_index.push(BlockIndexEntry {
                sequence: record.sequence(),
                offset: bytes.len() as u64,
            });
        }

        let frame = record.encode_frame()?;
        record_count = record_count.saturating_add(1);
        entry_count = entry_count.saturating_add(record.entries.len() as u64);
        min_sequence = Some(
            min_sequence
                .map(|current: SequenceNumber| current.min(record.sequence()))
                .unwrap_or(record.sequence()),
        );
        max_sequence = Some(
            max_sequence
                .map(|current: SequenceNumber| current.max(record.sequence()))
                .unwrap_or(record.sequence()),
        );

        for entry in &record.entries {
            let table = tables.entry(entry.table_id).or_default();
            table.min_sequence = Some(
                table
                    .min_sequence
                    .map(|current| current.min(record.sequence()))
                    .unwrap_or(record.sequence()),
            );
            table.max_sequence = Some(
                table
                    .max_sequence
                    .map(|current| current.max(record.sequence()))
                    .unwrap_or(record.sequence()),
            );
            table.entry_count = table.entry_count.saturating_add(1);
        }

        bytes.extend_from_slice(&frame);
    }

    let footer = SegmentFooter {
        segment_id,
        min_sequence: min_sequence.ok_or_else(|| {
            StorageError::unsupported("cannot encode a segment without a minimum sequence")
        })?,
        max_sequence: max_sequence.ok_or_else(|| {
            StorageError::unsupported("cannot encode a segment without a maximum sequence")
        })?,
        record_count,
        entry_count,
        data_end_offset: bytes.len() as u64,
        tables: tables
            .into_iter()
            .map(|(table_id, meta)| {
                Ok(TableSegmentMeta {
                    table_id,
                    min_sequence: meta.min_sequence.ok_or_else(|| {
                        StorageError::corruption(
                            "encoded segment table metadata is missing min_sequence",
                        )
                    })?,
                    max_sequence: meta.max_sequence.ok_or_else(|| {
                        StorageError::corruption(
                            "encoded segment table metadata is missing max_sequence",
                        )
                    })?,
                    entry_count: meta.entry_count,
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        block_index,
    };

    let footer_bytes = footer.encode()?;
    let footer_len = u32::try_from(footer_bytes.len())
        .map_err(|_| StorageError::unsupported("segment footer exceeds 4 GiB"))?;
    let footer_checksum = checksum32(&footer_bytes);

    let mut trailer = Vec::with_capacity(FOOTER_TRAILER_LEN);
    push_u32(&mut trailer, footer_len);
    push_u32(&mut trailer, footer_checksum);
    trailer.extend_from_slice(&FOOTER_MAGIC);

    bytes.extend_from_slice(&footer_bytes);
    bytes.extend_from_slice(&trailer);

    Ok((bytes, footer))
}

#[async_trait]
pub trait CommitLog: Send + Sync {
    async fn append(&self, record: CommitRecord) -> Result<CommitId, CommitError>;
    async fn flush(&self) -> Result<(), FlushError>;
    async fn scan(
        &self,
        table: &Table,
        cursor: LogCursor,
        opts: ScanOptions,
    ) -> Result<ChangeStream, ChangeFeedError>;
}

#[derive(Debug, Default)]
pub struct StubCommitLog;

#[async_trait]
impl CommitLog for StubCommitLog {
    async fn append(&self, record: CommitRecord) -> Result<CommitId, CommitError> {
        Ok(record.id)
    }

    async fn flush(&self) -> Result<(), FlushError> {
        Ok(())
    }

    async fn scan(
        &self,
        _table: &Table,
        _cursor: LogCursor,
        _opts: ScanOptions,
    ) -> Result<ChangeStream, ChangeFeedError> {
        Ok(Box::pin(stream::empty()))
    }
}

fn encode_value(bytes: &mut Vec<u8>, value: &Value) -> Result<(), StorageError> {
    match value {
        Value::Bytes(inner) => {
            push_u8(bytes, 0);
            push_len(bytes, inner.len())?;
            bytes.extend_from_slice(inner);
        }
        Value::Record(record) => {
            push_u8(bytes, 1);
            push_u32(
                bytes,
                u32::try_from(record.len())
                    .map_err(|_| StorageError::unsupported("record has too many fields"))?,
            );
            for (&field_id, field_value) in record {
                push_u32(bytes, field_id.get());
                encode_field_value(bytes, field_value)?;
            }
        }
    }

    Ok(())
}

fn decode_value(bytes: &[u8]) -> Result<Value, StorageError> {
    let mut cursor = ByteCursor::new(bytes);
    let tag = cursor.read_u8()?;
    let value = match tag {
        0 => Value::Bytes(cursor.read_len_prefixed_bytes()?.to_vec()),
        1 => {
            let field_count = cursor.read_u32()? as usize;
            let mut record = BTreeMap::new();
            for _ in 0..field_count {
                let field_id = FieldId::new(cursor.read_u32()?);
                let field_value = decode_field_value(&mut cursor)?;
                record.insert(field_id, field_value);
            }
            Value::Record(record)
        }
        other => {
            return Err(StorageError::corruption(format!(
                "unknown value tag {other}"
            )));
        }
    };

    if cursor.remaining() != 0 {
        return Err(StorageError::corruption("value contains trailing bytes"));
    }

    Ok(value)
}

fn encode_field_value(bytes: &mut Vec<u8>, value: &FieldValue) -> Result<(), StorageError> {
    match value {
        FieldValue::Null => push_u8(bytes, 0),
        FieldValue::Int64(inner) => {
            push_u8(bytes, 1);
            bytes.extend_from_slice(&inner.to_be_bytes());
        }
        FieldValue::Float64(inner) => {
            push_u8(bytes, 2);
            bytes.extend_from_slice(&inner.to_bits().to_be_bytes());
        }
        FieldValue::String(inner) => {
            push_u8(bytes, 3);
            push_len(bytes, inner.len())?;
            bytes.extend_from_slice(inner.as_bytes());
        }
        FieldValue::Bytes(inner) => {
            push_u8(bytes, 4);
            push_len(bytes, inner.len())?;
            bytes.extend_from_slice(inner);
        }
        FieldValue::Bool(inner) => {
            push_u8(bytes, 5);
            push_u8(bytes, u8::from(*inner));
        }
    }

    Ok(())
}

fn decode_field_value(cursor: &mut ByteCursor<'_>) -> Result<FieldValue, StorageError> {
    match cursor.read_u8()? {
        0 => Ok(FieldValue::Null),
        1 => Ok(FieldValue::Int64(cursor.read_i64()?)),
        2 => Ok(FieldValue::Float64(f64::from_bits(cursor.read_u64()?))),
        3 => {
            let bytes = cursor.read_len_prefixed_bytes()?;
            let value = std::str::from_utf8(bytes).map_err(|error| {
                StorageError::corruption(format!("field string is not valid utf-8: {error}"))
            })?;
            Ok(FieldValue::String(value.to_string()))
        }
        4 => Ok(FieldValue::Bytes(
            cursor.read_len_prefixed_bytes()?.to_vec(),
        )),
        5 => Ok(FieldValue::Bool(match cursor.read_u8()? {
            0 => false,
            1 => true,
            other => {
                return Err(StorageError::corruption(format!(
                    "invalid bool tag {other}"
                )));
            }
        })),
        other => Err(StorageError::corruption(format!(
            "unknown field value tag {other}"
        ))),
    }
}

fn encode_change_kind(kind: ChangeKind) -> u8 {
    match kind {
        ChangeKind::Put => 1,
        ChangeKind::Delete => 2,
        ChangeKind::Merge => 3,
    }
}

fn decode_change_kind(tag: u8) -> Result<ChangeKind, StorageError> {
    match tag {
        1 => Ok(ChangeKind::Put),
        2 => Ok(ChangeKind::Delete),
        3 => Ok(ChangeKind::Merge),
        other => Err(StorageError::corruption(format!(
            "unknown change kind tag {other}"
        ))),
    }
}

fn push_u8<B: BufMut>(bytes: &mut B, value: u8) {
    bytes.put_u8(value);
}

fn push_u16<B: BufMut>(bytes: &mut B, value: u16) {
    bytes.put_u16(value);
}

fn push_u32<B: BufMut>(bytes: &mut B, value: u32) {
    bytes.put_u32(value);
}

fn push_u64<B: BufMut>(bytes: &mut B, value: u64) {
    bytes.put_u64(value);
}

fn push_len<B: BufMut>(bytes: &mut B, value: usize) -> Result<(), StorageError> {
    push_u32(
        bytes,
        u32::try_from(value).map_err(|_| StorageError::unsupported("length exceeds 4 GiB"))?,
    );
    Ok(())
}

fn parse_segment_footer(bytes: &[u8]) -> Result<Option<SegmentFooter>, StorageError> {
    if bytes.len() < FOOTER_TRAILER_LEN {
        return Ok(None);
    }

    let trailer = &bytes[bytes.len() - FOOTER_TRAILER_LEN..];
    if trailer[8..] != FOOTER_MAGIC {
        return Ok(None);
    }

    let footer_len = u32::from_be_bytes(trailer[..4].try_into().expect("footer len")) as usize;
    let footer_checksum = u32::from_be_bytes(trailer[4..8].try_into().expect("footer checksum"));
    if bytes.len() < FOOTER_TRAILER_LEN + footer_len {
        return Err(StorageError::corruption(
            "segment footer length exceeds segment size",
        ));
    }

    let footer_start = bytes.len() - FOOTER_TRAILER_LEN - footer_len;
    let footer_bytes = &bytes[footer_start..footer_start + footer_len];
    if checksum32(footer_bytes) != footer_checksum {
        return Err(StorageError::corruption("segment footer checksum mismatch"));
    }

    let footer = SegmentFooter::decode(footer_bytes)?;
    if footer.data_end_offset != footer_start as u64 {
        return Err(StorageError::corruption(
            "segment footer data_end_offset does not match footer position",
        ));
    }

    Ok(Some(footer))
}

fn parse_record_frame(bytes: &[u8], offset: usize) -> Result<(CommitRecord, usize), StorageError> {
    if bytes.len().saturating_sub(offset) < RECORD_HEADER_LEN {
        return Err(StorageError::corruption(format!(
            "truncated commit record frame at offset {offset}"
        )));
    }

    if bytes[offset..offset + 4] != RECORD_MAGIC {
        return Err(StorageError::corruption(format!(
            "invalid commit record magic at offset {offset}"
        )));
    }

    let payload_len = u32::from_be_bytes(
        bytes[offset + 4..offset + 8]
            .try_into()
            .expect("payload len"),
    ) as usize;
    let checksum = u32::from_be_bytes(bytes[offset + 8..offset + 12].try_into().expect("checksum"));
    let frame_end = offset + RECORD_HEADER_LEN + payload_len;
    if frame_end > bytes.len() {
        return Err(StorageError::corruption(format!(
            "truncated commit record payload at offset {offset}"
        )));
    }

    let payload = &bytes[offset + RECORD_HEADER_LEN..frame_end];
    if checksum32(payload) != checksum {
        return Err(StorageError::corruption(format!(
            "commit record checksum mismatch at offset {offset}"
        )));
    }

    let record = CommitRecord::decode_payload(payload)?;
    Ok((record, frame_end))
}

fn parse_records_range(
    bytes: &[u8],
    start_offset: usize,
    end_offset: usize,
    min_sequence: Option<SequenceNumber>,
) -> Result<Vec<CommitRecord>, StorageError> {
    if start_offset > end_offset || end_offset > bytes.len() {
        return Err(StorageError::corruption(
            "record scan range is out of bounds",
        ));
    }

    let mut records = Vec::new();
    let mut offset = start_offset;
    while offset < end_offset {
        let (record, next_offset) = parse_record_frame(bytes, offset)?;
        if next_offset > end_offset {
            return Err(StorageError::corruption(
                "commit record frame extends beyond declared data range",
            ));
        }
        if min_sequence.is_none_or(|sequence| record.sequence() >= sequence) {
            records.push(record);
        }
        offset = next_offset;
    }

    Ok(records)
}

async fn list_segment_paths(
    fs: &dyn FileSystem,
    dir: &str,
) -> Result<Vec<(SegmentId, String)>, StorageError> {
    let mut paths = fs.list(dir).await?;
    paths.retain(|path| parse_segment_id(path).is_some());
    let mut segments = paths
        .into_iter()
        .filter_map(|path| parse_segment_id(&path).map(|segment_id| (segment_id, path)))
        .collect::<Vec<_>>();
    segments.sort_by_key(|(segment_id, _)| segment_id.get());
    Ok(segments)
}

fn parse_segment_id(path: &str) -> Option<SegmentId> {
    let file_name = path.rsplit('/').next()?;
    let suffix = file_name.strip_prefix("SEG-")?;
    let parsed = suffix.parse::<u64>().ok()?;
    Some(SegmentId::new(parsed))
}

fn segment_path(dir: &str, segment_id: SegmentId) -> String {
    format!("{dir}/SEG-{:06}", segment_id.get())
}

async fn read_path(fs: &dyn FileSystem, path: &str) -> Result<Vec<u8>, StorageError> {
    let handle = fs
        .open(
            path,
            OpenOptions {
                create: false,
                read: true,
                write: false,
                truncate: false,
                append: false,
            },
        )
        .await?;
    read_file(fs, handle.path()).await
}

async fn read_file(fs: &dyn FileSystem, path: &str) -> Result<Vec<u8>, StorageError> {
    let handle = FileHandle::new(path);
    let mut bytes = Vec::new();
    let mut offset = 0_u64;
    loop {
        let chunk = fs.read_at(&handle, offset, FILE_READ_CHUNK_BYTES).await?;
        if chunk.is_empty() {
            break;
        }
        offset = offset.saturating_add(chunk.len() as u64);
        bytes.extend_from_slice(&chunk);
        if chunk.len() < FILE_READ_CHUNK_BYTES {
            break;
        }
    }
    Ok(bytes)
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], StorageError> {
        if self.remaining() < len {
            return Err(StorageError::corruption(format!(
                "unexpected end of buffer: needed {len} bytes, have {}",
                self.remaining()
            )));
        }

        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn read_u8(&mut self) -> Result<u8, StorageError> {
        if !self.bytes.has_remaining() {
            return Err(StorageError::corruption(
                "unexpected end of buffer: needed 1 bytes, have 0",
            ));
        }
        Ok(self.bytes.get_u8())
    }

    fn read_u16(&mut self) -> Result<u16, StorageError> {
        if self.remaining() < 2 {
            return Err(StorageError::corruption(format!(
                "unexpected end of buffer: needed 2 bytes, have {}",
                self.remaining()
            )));
        }
        Ok(self.bytes.get_u16())
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        if self.remaining() < 4 {
            return Err(StorageError::corruption(format!(
                "unexpected end of buffer: needed 4 bytes, have {}",
                self.remaining()
            )));
        }
        Ok(self.bytes.get_u32())
    }

    fn read_u64(&mut self) -> Result<u64, StorageError> {
        if self.remaining() < 8 {
            return Err(StorageError::corruption(format!(
                "unexpected end of buffer: needed 8 bytes, have {}",
                self.remaining()
            )));
        }
        Ok(self.bytes.get_u64())
    }

    fn read_i64(&mut self) -> Result<i64, StorageError> {
        if self.remaining() < 8 {
            return Err(StorageError::corruption(format!(
                "unexpected end of buffer: needed 8 bytes, have {}",
                self.remaining()
            )));
        }
        Ok(self.bytes.get_i64())
    }

    fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], StorageError> {
        let len = self.read_u32()? as usize;
        self.read_exact(len)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use crate::{
        adapters::{FileSystemFailure, FileSystemOperation, SimulatedFileSystem},
        api::FieldValue,
        error::StorageErrorKind,
        ids::SequenceNumber,
    };

    use super::{
        AppendLocation, BlockIndexEntry, CommitEntry, CommitRecord, SegmentFooter, SegmentManager,
        SegmentOptions, TableSegmentMeta, checksum32, parse_segment_footer, segment_path,
    };
    use crate::api::{ChangeKind, Value};
    use crate::ids::{CommitId, FieldId, SegmentId, TableId};
    use crate::io::{FileSystem, OpenOptions};

    fn sample_record(sequence: u64) -> CommitRecord {
        let mut record_value = BTreeMap::new();
        record_value.insert(FieldId::new(1), FieldValue::String("alice".to_string()));
        record_value.insert(FieldId::new(2), FieldValue::Int64(sequence as i64));

        CommitRecord {
            id: CommitId::new(SequenceNumber::new(sequence)),
            entries: vec![
                CommitEntry {
                    op_index: 0,
                    table_id: TableId::new(1),
                    kind: ChangeKind::Put,
                    key: format!("user:{sequence}").into_bytes(),
                    value: Some(Value::Record(record_value)),
                },
                CommitEntry {
                    op_index: 1,
                    table_id: TableId::new(2),
                    kind: ChangeKind::Delete,
                    key: format!("audit:{sequence}").into_bytes(),
                    value: None,
                },
                CommitEntry {
                    op_index: 2,
                    table_id: TableId::new(1),
                    kind: ChangeKind::Merge,
                    key: format!("merge:{sequence}").into_bytes(),
                    value: Some(Value::Bytes(vec![sequence as u8, sequence as u8 + 1])),
                },
            ],
        }
    }

    async fn read_segment_bytes(fs: &dyn FileSystem, path: &str) -> Vec<u8> {
        let handle = fs
            .open(
                path,
                OpenOptions {
                    create: false,
                    read: true,
                    write: false,
                    truncate: false,
                    append: false,
                },
            )
            .await
            .expect("open segment");

        let mut bytes = Vec::new();
        let mut offset = 0_u64;
        loop {
            let chunk = fs
                .read_at(&handle, offset, 64 * 1024)
                .await
                .expect("read chunk");
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            bytes.extend_from_slice(&chunk);
            if chunk.len() < 64 * 1024 {
                break;
            }
        }
        bytes
    }

    #[test]
    fn commit_record_round_trips_with_checksums() {
        let record = sample_record(7);
        let frame = record.encode_frame().expect("encode frame");
        let decoded = CommitRecord::decode_frame(&frame).expect("decode frame");
        assert_eq!(decoded, record);

        let mut corrupted = frame;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x7f;
        let error = CommitRecord::decode_frame(&corrupted).expect_err("checksum failure");
        assert_eq!(error.kind(), StorageErrorKind::Corruption);
    }

    #[test]
    fn segment_footer_round_trips_and_seeks_by_sequence() {
        let footer = SegmentFooter {
            segment_id: SegmentId::new(9),
            min_sequence: SequenceNumber::new(11),
            max_sequence: SequenceNumber::new(42),
            record_count: 4,
            entry_count: 9,
            data_end_offset: 1024,
            tables: vec![
                TableSegmentMeta {
                    table_id: TableId::new(1),
                    min_sequence: SequenceNumber::new(11),
                    max_sequence: SequenceNumber::new(40),
                    entry_count: 5,
                },
                TableSegmentMeta {
                    table_id: TableId::new(2),
                    min_sequence: SequenceNumber::new(20),
                    max_sequence: SequenceNumber::new(42),
                    entry_count: 4,
                },
            ],
            block_index: vec![
                BlockIndexEntry {
                    sequence: SequenceNumber::new(11),
                    offset: 0,
                },
                BlockIndexEntry {
                    sequence: SequenceNumber::new(20),
                    offset: 300,
                },
                BlockIndexEntry {
                    sequence: SequenceNumber::new(35),
                    offset: 700,
                },
            ],
        };

        let encoded = footer.encode().expect("encode footer");
        let decoded = SegmentFooter::decode(&encoded).expect("decode footer");
        assert_eq!(decoded, footer);
        assert_eq!(decoded.seek_offset(SequenceNumber::new(10)), 0);
        assert_eq!(decoded.seek_offset(SequenceNumber::new(22)), 300);
        assert_eq!(decoded.seek_offset(SequenceNumber::new(40)), 700);
    }

    #[tokio::test]
    async fn segment_manager_appends_reads_and_rebuilds_catalog() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/commitlog";

        let mut manager = SegmentManager::open(
            fs.clone(),
            dir,
            SegmentOptions {
                max_segment_size_bytes: 4096,
                records_per_block: 1,
            },
        )
        .await
        .expect("open manager");

        let first = manager
            .append(sample_record(1))
            .await
            .expect("append first");
        let second = manager
            .append(sample_record(2))
            .await
            .expect("append second");
        let sealed = manager.seal_active().await.expect("seal").expect("footer");
        let third = manager
            .append(sample_record(3))
            .await
            .expect("append third");

        assert_eq!(
            first,
            AppendLocation {
                segment_id: sealed.segment_id,
                offset: 0,
            }
        );
        assert_eq!(second.segment_id, sealed.segment_id);
        assert_ne!(third.segment_id, sealed.segment_id);

        let descriptors = manager.enumerate_segments();
        assert_eq!(descriptors.len(), 2);
        assert!(descriptors[0].sealed);
        assert!(!descriptors[1].sealed);
        assert_eq!(
            manager.seek_by_sequence(TableId::new(1), SequenceNumber::new(1)),
            Some(sealed.segment_id)
        );
        assert_eq!(
            manager.seek_by_sequence(TableId::new(1), SequenceNumber::new(3)),
            Some(third.segment_id)
        );

        let sealed_records = manager
            .read_from_sequence(sealed.segment_id, SequenceNumber::new(1))
            .await
            .expect("read sealed segment");
        assert_eq!(
            sealed_records
                .iter()
                .flat_map(|record| {
                    record
                        .entries
                        .iter()
                        .map(move |entry| (record.sequence(), entry.op_index, entry.table_id))
                })
                .collect::<Vec<_>>(),
            vec![
                (SequenceNumber::new(1), 0, TableId::new(1)),
                (SequenceNumber::new(1), 1, TableId::new(2)),
                (SequenceNumber::new(1), 2, TableId::new(1)),
                (SequenceNumber::new(2), 0, TableId::new(1)),
                (SequenceNumber::new(2), 1, TableId::new(2)),
                (SequenceNumber::new(2), 2, TableId::new(1)),
            ]
        );

        let reopened = SegmentManager::open(
            fs.clone(),
            dir,
            SegmentOptions {
                max_segment_size_bytes: 4096,
                records_per_block: 1,
            },
        )
        .await
        .expect("reopen manager");
        assert_eq!(reopened.sealed_catalog().footers().len(), 1);
        assert_eq!(
            reopened
                .sealed_catalog()
                .oldest_sequence(TableId::new(1))
                .expect("oldest sequence"),
            SequenceNumber::new(1)
        );

        let active_records = reopened
            .read_from_sequence(reopened.active_segment_id(), SequenceNumber::new(3))
            .await
            .expect("read active segment");
        assert_eq!(active_records.len(), 1);
        assert_eq!(active_records[0].sequence(), SequenceNumber::new(3));
        assert_eq!(
            active_records[0]
                .entries
                .iter()
                .map(|entry| (entry.op_index, entry.table_id))
                .collect::<Vec<_>>(),
            vec![
                (0, TableId::new(1)),
                (1, TableId::new(2)),
                (2, TableId::new(1)),
            ]
        );
    }

    #[tokio::test]
    async fn segment_manager_scans_table_entries_across_sealed_and_active_segments() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/table-scan";

        let mut manager = SegmentManager::open(
            fs.clone(),
            dir,
            SegmentOptions {
                max_segment_size_bytes: 4096,
                records_per_block: 1,
            },
        )
        .await
        .expect("open manager");

        manager
            .append(sample_record(1))
            .await
            .expect("append first");
        manager
            .append(sample_record(2))
            .await
            .expect("append second");
        manager.seal_active().await.expect("seal").expect("sealed");
        manager
            .append(sample_record(3))
            .await
            .expect("append third");

        let reopened = SegmentManager::open(
            fs.clone(),
            dir,
            SegmentOptions {
                max_segment_size_bytes: 4096,
                records_per_block: 1,
            },
        )
        .await
        .expect("reopen manager");

        let records = reopened
            .scan_table_from_sequence(TableId::new(1), SequenceNumber::new(2))
            .await
            .expect("scan table");

        assert_eq!(
            records
                .iter()
                .flat_map(|record| {
                    record
                        .entries
                        .iter()
                        .filter(|entry| entry.table_id == TableId::new(1))
                        .map(move |entry| (record.sequence(), entry.op_index, entry.table_id))
                })
                .collect::<Vec<_>>(),
            vec![
                (SequenceNumber::new(2), 0, TableId::new(1)),
                (SequenceNumber::new(2), 2, TableId::new(1)),
                (SequenceNumber::new(3), 0, TableId::new(1)),
                (SequenceNumber::new(3), 2, TableId::new(1)),
            ]
        );
    }

    #[tokio::test]
    async fn reopening_a_truncated_active_segment_keeps_the_valid_prefix() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/truncated";
        let path = segment_path(dir, SegmentId::new(1));

        let first = sample_record(8).encode_frame().expect("encode first frame");
        let second = sample_record(9)
            .encode_frame()
            .expect("encode second frame");
        let handle = fs
            .open(
                &path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await
            .expect("create segment");
        fs.write_at(&handle, 0, &first)
            .await
            .expect("write first frame");
        fs.write_at(&handle, first.len() as u64, &second[..second.len() - 3])
            .await
            .expect("write truncated second frame");

        let reopened = SegmentManager::open(fs.clone(), dir, SegmentOptions::default())
            .await
            .expect("reopen manager");
        let records = reopened
            .read_from_sequence(reopened.active_segment_id(), SequenceNumber::new(1))
            .await
            .expect("read recovered prefix");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].sequence(), SequenceNumber::new(8));

        let repaired = read_segment_bytes(fs.as_ref(), &path).await;
        assert_eq!(repaired, first);
    }

    #[tokio::test]
    async fn reopening_a_segment_with_corrupt_footer_fails_closed() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/corrupt-footer";

        let mut manager = SegmentManager::open(fs.clone(), dir, SegmentOptions::default())
            .await
            .expect("open manager");
        let sealed = manager
            .append(sample_record(9))
            .await
            .expect("append")
            .segment_id;
        manager.seal_active().await.expect("seal").expect("sealed");

        let path = segment_path(dir, sealed);
        let mut bytes = read_segment_bytes(fs.as_ref(), &path).await;
        let trailer_offset = bytes.len() - 4;
        bytes[trailer_offset] ^= 0x55;

        let handle = fs
            .open(
                &path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await
            .expect("reopen for rewrite");
        fs.write_at(&handle, 0, &bytes)
            .await
            .expect("rewrite bytes");

        let error = match SegmentManager::open(fs.clone(), dir, SegmentOptions::default()).await {
            Ok(_) => panic!("corrupt footer should fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), StorageErrorKind::Corruption);
    }

    #[tokio::test]
    async fn torn_reads_and_checksum_failures_never_invent_records() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/failures";

        let mut manager = SegmentManager::open(fs.clone(), dir, SegmentOptions::default())
            .await
            .expect("open manager");
        let sealed_segment = manager
            .append(sample_record(10))
            .await
            .expect("append")
            .segment_id;
        manager.seal_active().await.expect("seal").expect("sealed");

        let path = segment_path(dir, sealed_segment);
        fs.inject_failure(FileSystemFailure::partial_read(path.clone()));
        let error = manager
            .read_from_sequence(sealed_segment, SequenceNumber::new(10))
            .await
            .expect_err("partial read should fail");
        assert_eq!(error.kind(), StorageErrorKind::Corruption);

        let mut bytes = read_segment_bytes(fs.as_ref(), &path).await;
        let footer = parse_segment_footer(&bytes)
            .expect("parse footer")
            .expect("sealed footer");
        let payload_offset = footer.block_index[0].offset as usize + 14;
        bytes[payload_offset] ^= 0x22;
        let checksum = checksum32(
            &bytes[footer.block_index[0].offset as usize + 12..footer.data_end_offset as usize],
        );
        assert_ne!(checksum, 0);

        let handle = fs
            .open(
                &path,
                OpenOptions {
                    create: true,
                    read: true,
                    write: true,
                    truncate: true,
                    append: false,
                },
            )
            .await
            .expect("rewrite handle");
        fs.write_at(&handle, 0, &bytes)
            .await
            .expect("rewrite bytes");

        let reopened = SegmentManager::open(fs.clone(), dir, SegmentOptions::default())
            .await
            .expect("reopen manager");
        let error = reopened
            .read_from_sequence(sealed_segment, SequenceNumber::new(10))
            .await
            .expect_err("checksum mismatch should fail");
        assert_eq!(error.kind(), StorageErrorKind::Corruption);
    }

    #[tokio::test]
    async fn checksum_failures_from_write_injection_surface_as_storage_errors() {
        let fs = Arc::new(SimulatedFileSystem::default());
        let dir = "/db/write-failure";
        fs.inject_failure(FileSystemFailure::timeout(
            FileSystemOperation::WriteAt,
            dir.to_string(),
        ));

        let mut manager = SegmentManager::open(fs.clone(), dir, SegmentOptions::default())
            .await
            .expect("open manager");
        let error = manager
            .append(sample_record(11))
            .await
            .expect_err("append failure");
        assert_eq!(error.kind(), StorageErrorKind::Timeout);
    }
}
