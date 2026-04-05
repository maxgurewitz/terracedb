use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{Read, Seek, SeekFrom, Write, copy},
};

use flatbuffers::FlatBufferBuilder;
use flate2::{Compression as FlateCompression, read::ZlibDecoder, write::ZlibEncoder};
use tempfile::tempfile;

use super::{
    FileContent, InodeData, InodeRecord, OriginRecord, SnapshotState,
    VFS_VOLUME_ARTIFACT_FORMAT_VERSION, VFS_VOLUME_ARTIFACT_MAGIC, VfsError, VolumeInfo,
    build_children_by_parent, build_paths_by_inode, file_content_to_bytes,
};
use crate::{
    FileKind, InodeId, JsonValue, Stats, ToolRun, ToolRunId, ToolRunStatus, VolumeId,
    generated::vfs_volume_artifact_generated::terracedb::vfs::{
        FilePayloadEntry, FilePayloadEntryArgs, InodeEntry, InodeEntryArgs, KvEntry, KvEntryArgs,
        OriginEntry, OriginEntryArgs, OverlayBase, OverlayBaseArgs, PathEntry, PathEntryArgs,
        PayloadCompression, ToolRunEntry, ToolRunEntryArgs, VolumeArtifactManifest,
        VolumeArtifactManifestArgs, finish_volume_artifact_manifest_buffer,
        root_as_volume_artifact_manifest, volume_artifact_manifest_buffer_has_identifier,
    },
};
use terracedb::{SequenceNumber, Timestamp};

const ARTIFACT_HEADER_LEN: usize = 16;

#[derive(Clone, Copy, Debug)]
pub(super) struct EncodedFilePayload {
    pub(super) payload_offset: u64,
    pub(super) payload_len: u64,
    pub(super) compression: PayloadCompression,
}

pub(super) fn write_volume_artifact<W: Write>(
    snapshot: &SnapshotState,
    writer: &mut W,
) -> Result<(), VfsError> {
    write_encoded_volume_artifact(snapshot, encode_file_payloads(snapshot)?, writer)
}

pub(super) fn read_volume_artifact<R: Read>(reader: &mut R) -> Result<SnapshotState, VfsError> {
    let mut header = [0_u8; ARTIFACT_HEADER_LEN];
    reader
        .read_exact(&mut header)
        .map_err(io_error_to_artifact)?;

    if &header[..4] != VFS_VOLUME_ARTIFACT_MAGIC {
        return Err(VfsError::VolumeArtifact {
            reason: "volume artifact magic mismatch".to_string(),
        });
    }

    let format_version = u32::from_be_bytes(header[4..8].try_into().expect("header slice"));
    if format_version != VFS_VOLUME_ARTIFACT_FORMAT_VERSION {
        return Err(VfsError::VolumeArtifact {
            reason: format!("unsupported volume artifact format version {format_version}"),
        });
    }

    let manifest_len = u64::from_be_bytes(header[8..16].try_into().expect("header slice"));
    let manifest_len: usize = manifest_len
        .try_into()
        .map_err(|_| VfsError::VolumeArtifact {
            reason: format!("manifest too large: {manifest_len}"),
        })?;
    let mut manifest_bytes = vec![0_u8; manifest_len];
    reader
        .read_exact(&mut manifest_bytes)
        .map_err(io_error_to_artifact)?;

    if !volume_artifact_manifest_buffer_has_identifier(&manifest_bytes) {
        return Err(VfsError::VolumeArtifact {
            reason: "volume artifact manifest identifier mismatch".to_string(),
        });
    }

    let manifest = root_as_volume_artifact_manifest(&manifest_bytes).map_err(|error| {
        VfsError::VolumeArtifact {
            reason: format!("invalid volume artifact manifest: {error}"),
        }
    })?;

    if manifest.format_version() != VFS_VOLUME_ARTIFACT_FORMAT_VERSION {
        return Err(VfsError::VolumeArtifact {
            reason: format!(
                "unsupported manifest format version {}",
                manifest.format_version()
            ),
        });
    }

    let info = VolumeInfo {
        volume_id: decode_volume_id(manifest.volume_id())?,
        chunk_size: manifest.chunk_size(),
        format_version: manifest.volume_format_version(),
        root_inode: InodeId::new(manifest.root_inode()),
        created_at: Timestamp::new(manifest.created_at()),
        overlay_base: manifest
            .overlay_base()
            .map(decode_overlay_base)
            .transpose()?,
    };

    let mut paths = BTreeMap::new();
    for entry in manifest.paths().iter() {
        paths.insert(entry.path().to_string(), InodeId::new(entry.inode()));
    }

    let mut payload_offset = 0_u64;
    let mut inodes = BTreeMap::new();
    for entry in manifest.inodes().iter() {
        let inode_id = InodeId::new(entry.inode());
        let kind = decode_file_kind(entry.kind())?;
        let data = match kind {
            FileKind::Directory => {
                if entry.file_payload().is_some() || entry.symlink_target().is_some() {
                    return Err(VfsError::VolumeArtifact {
                        reason: format!("directory inode {inode_id} has unexpected payload data"),
                    });
                }
                InodeData::Directory
            }
            FileKind::Symlink => {
                if entry.file_payload().is_some() {
                    return Err(VfsError::VolumeArtifact {
                        reason: format!("symlink inode {inode_id} has unexpected file payload"),
                    });
                }
                InodeData::Symlink(
                    entry
                        .symlink_target()
                        .ok_or_else(|| VfsError::VolumeArtifact {
                            reason: format!("symlink inode {inode_id} missing target"),
                        })?
                        .to_string(),
                )
            }
            FileKind::File => {
                if entry.symlink_target().is_some() {
                    return Err(VfsError::VolumeArtifact {
                        reason: format!("file inode {inode_id} has unexpected symlink target"),
                    });
                }
                let content = decode_file_content(
                    entry.file_payload(),
                    entry.size(),
                    info.chunk_size,
                    reader,
                    &mut payload_offset,
                    inode_id,
                )?;
                InodeData::File(content)
            }
        };

        let stats = Stats {
            inode: inode_id,
            kind,
            mode: entry.mode(),
            nlink: entry.nlink(),
            uid: entry.uid(),
            gid: entry.gid(),
            size: entry.size(),
            created_at: Timestamp::new(entry.created_at()),
            modified_at: Timestamp::new(entry.modified_at()),
            changed_at: Timestamp::new(entry.changed_at()),
            accessed_at: Timestamp::new(entry.accessed_at()),
            rdev: entry.rdev(),
        };

        inodes.insert(inode_id, InodeRecord { stats, data });
    }

    let mut kv = BTreeMap::new();
    for entry in manifest.kv().iter() {
        kv.insert(entry.key().to_string(), decode_json(entry.value_json())?);
    }

    let mut tool_runs = BTreeMap::new();
    for entry in manifest.tool_runs().iter() {
        let id = ToolRunId::new(entry.id());
        tool_runs.insert(
            id,
            ToolRun {
                id,
                name: entry.name().to_string(),
                status: decode_tool_run_status(entry.status())?,
                params: entry.params_json().map(decode_json).transpose()?,
                result: entry.result_json().map(decode_json).transpose()?,
                error: entry.error().map(ToOwned::to_owned),
                started_at: Timestamp::new(entry.started_at()),
                completed_at: if entry.has_completed_at() {
                    Some(Timestamp::new(entry.completed_at()))
                } else {
                    None
                },
            },
        );
    }

    let whiteouts = manifest
        .whiteouts()
        .iter()
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>();
    let origins = manifest
        .origins()
        .iter()
        .map(|entry| {
            Ok((
                InodeId::new(entry.inode()),
                OriginRecord {
                    base_volume_id: decode_volume_id(entry.base_volume_id())?,
                    base_sequence: SequenceNumber::new(entry.base_sequence()),
                    base_durable: entry.base_durable(),
                    base_inode: InodeId::new(entry.base_inode()),
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, VfsError>>()?;

    reject_trailing_bytes(reader)?;

    Ok(SnapshotState {
        info,
        sequence: SequenceNumber::new(manifest.sequence()),
        durable: manifest.durable(),
        children_by_parent: build_children_by_parent(&paths),
        paths_by_inode: build_paths_by_inode(&paths),
        paths,
        inodes,
        kv,
        tool_runs,
        whiteouts,
        origins,
    })
}

pub(super) struct EncodedPayloads {
    pub(super) entries: BTreeMap<InodeId, EncodedFilePayload>,
    pub(super) spool: File,
}

pub(super) fn new_encoded_payloads() -> Result<EncodedPayloads, VfsError> {
    Ok(EncodedPayloads {
        entries: BTreeMap::new(),
        spool: tempfile().map_err(io_error_to_artifact)?,
    })
}

fn encode_file_payloads(snapshot: &SnapshotState) -> Result<EncodedPayloads, VfsError> {
    let mut encoded = new_encoded_payloads()?;
    for record in snapshot.inodes.values() {
        let InodeData::File(content) = &record.data else {
            continue;
        };
        if record.stats.size == 0 {
            continue;
        }

        let raw_len = file_content_len(content)?;
        if raw_len != record.stats.size {
            return Err(VfsError::VolumeArtifact {
                reason: format!(
                    "file inode {} size mismatch: stats {}, content {}",
                    record.stats.inode, record.stats.size, raw_len
                ),
            });
        }

        let payload = encode_bytes_payload_to_spool(
            &file_content_to_bytes(content, record.stats.size, snapshot.info.chunk_size),
            &mut encoded.spool,
        )?;
        encoded.entries.insert(record.stats.inode, payload);
    }
    Ok(encoded)
}

pub(super) fn write_encoded_volume_artifact<W: Write>(
    snapshot: &SnapshotState,
    encoded_payloads: EncodedPayloads,
    writer: &mut W,
) -> Result<(), VfsError> {
    let mut builder = FlatBufferBuilder::with_capacity(16 * 1024);
    let manifest = build_manifest(snapshot, &encoded_payloads, &mut builder)?;
    finish_volume_artifact_manifest_buffer(&mut builder, manifest);
    let manifest_bytes = builder.finished_data();
    let manifest_len: u64 =
        manifest_bytes
            .len()
            .try_into()
            .map_err(|_| VfsError::VolumeArtifact {
                reason: "manifest exceeds u64 length".to_string(),
            })?;

    writer
        .write_all(VFS_VOLUME_ARTIFACT_MAGIC)
        .map_err(io_error_to_artifact)?;
    writer
        .write_all(&VFS_VOLUME_ARTIFACT_FORMAT_VERSION.to_be_bytes())
        .map_err(io_error_to_artifact)?;
    writer
        .write_all(&manifest_len.to_be_bytes())
        .map_err(io_error_to_artifact)?;
    writer
        .write_all(manifest_bytes)
        .map_err(io_error_to_artifact)?;

    let mut spool = encoded_payloads.spool;
    spool
        .seek(SeekFrom::Start(0))
        .map_err(io_error_to_artifact)?;
    copy(&mut spool, writer).map_err(io_error_to_artifact)?;
    Ok(())
}

pub(super) fn encode_bytes_payload_to_spool(
    bytes: &[u8],
    spool: &mut File,
) -> Result<EncodedFilePayload, VfsError> {
    let payload_offset = spool.stream_position().map_err(io_error_to_artifact)?;
    let (compression, payload_len) = encode_bytes_to_spool(bytes, spool)?;
    Ok(EncodedFilePayload {
        payload_offset,
        payload_len,
        compression,
    })
}

fn encode_bytes_to_spool(
    bytes: &[u8],
    spool: &mut File,
) -> Result<(PayloadCompression, u64), VfsError> {
    let start = spool.stream_position().map_err(io_error_to_artifact)?;
    {
        let mut encoder = ZlibEncoder::new(&mut *spool, FlateCompression::fast());
        encoder.write_all(bytes).map_err(io_error_to_artifact)?;
        encoder.try_finish().map_err(io_error_to_artifact)?;
    }
    let compressed_end = spool.stream_position().map_err(io_error_to_artifact)?;
    let compressed_len = compressed_end.saturating_sub(start);
    if compressed_len < bytes.len() as u64 {
        return Ok((PayloadCompression::Zlib, compressed_len));
    }

    spool.set_len(start).map_err(io_error_to_artifact)?;
    spool
        .seek(SeekFrom::Start(start))
        .map_err(io_error_to_artifact)?;
    spool.write_all(bytes).map_err(io_error_to_artifact)?;
    let raw_end = spool.stream_position().map_err(io_error_to_artifact)?;
    Ok((PayloadCompression::None, raw_end.saturating_sub(start)))
}

fn build_manifest<'a>(
    snapshot: &SnapshotState,
    encoded_payloads: &EncodedPayloads,
    builder: &mut FlatBufferBuilder<'a>,
) -> Result<flatbuffers::WIPOffset<VolumeArtifactManifest<'a>>, VfsError> {
    let overlay_base = snapshot.info.overlay_base.as_ref().map(|overlay| {
        let volume_id = builder.create_vector(&overlay.volume_id.encode());
        OverlayBase::create(
            builder,
            &OverlayBaseArgs {
                volume_id: Some(volume_id),
                sequence: overlay.sequence.get(),
                durable: overlay.durable,
            },
        )
    });

    let path_offsets = snapshot
        .paths
        .iter()
        .map(|(path, inode)| {
            let path = builder.create_string(path);
            PathEntry::create(
                builder,
                &PathEntryArgs {
                    path: Some(path),
                    inode: inode.get(),
                },
            )
        })
        .collect::<Vec<_>>();
    let paths = builder.create_vector(&path_offsets);

    let inode_offsets = snapshot
        .inodes
        .values()
        .map(|record| {
            let symlink_target = match &record.data {
                InodeData::Directory => None,
                InodeData::Symlink(target) => Some(builder.create_string(target)),
                InodeData::File(_) => None,
            };
            let file_payload = encoded_payloads
                .entries
                .get(&record.stats.inode)
                .map(|payload| {
                    FilePayloadEntry::create(
                        builder,
                        &FilePayloadEntryArgs {
                            payload_offset: payload.payload_offset,
                            payload_len: payload.payload_len,
                            compression: payload.compression,
                        },
                    )
                });
            Ok(InodeEntry::create(
                builder,
                &InodeEntryArgs {
                    inode: record.stats.inode.get(),
                    kind: encode_file_kind(record.stats.kind),
                    mode: record.stats.mode,
                    nlink: record.stats.nlink,
                    uid: record.stats.uid,
                    gid: record.stats.gid,
                    size: record.stats.size,
                    created_at: record.stats.created_at.get(),
                    modified_at: record.stats.modified_at.get(),
                    changed_at: record.stats.changed_at.get(),
                    accessed_at: record.stats.accessed_at.get(),
                    rdev: record.stats.rdev,
                    symlink_target,
                    file_payload,
                },
            ))
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    let inodes = builder.create_vector(&inode_offsets);

    let kv_offsets = snapshot
        .kv
        .iter()
        .map(|(key, value)| {
            let key = builder.create_string(key);
            let value_json = builder.create_string(&encode_json(value)?);
            Ok(KvEntry::create(
                builder,
                &KvEntryArgs {
                    key: Some(key),
                    value_json: Some(value_json),
                },
            ))
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    let kv = builder.create_vector(&kv_offsets);

    let tool_run_offsets = snapshot
        .tool_runs
        .values()
        .map(|run| {
            let name = builder.create_string(&run.name);
            let params_json = run
                .params
                .as_ref()
                .map(|value| encode_json(value))
                .transpose()?
                .map(|value| builder.create_string(&value));
            let result_json = run
                .result
                .as_ref()
                .map(|value| encode_json(value))
                .transpose()?
                .map(|value| builder.create_string(&value));
            let error = run
                .error
                .as_deref()
                .map(|value| builder.create_string(value));
            Ok(ToolRunEntry::create(
                builder,
                &ToolRunEntryArgs {
                    id: run.id.get(),
                    name: Some(name),
                    status: encode_tool_run_status(&run.status),
                    params_json,
                    result_json,
                    error,
                    started_at: run.started_at.get(),
                    has_completed_at: run.completed_at.is_some(),
                    completed_at: run.completed_at.map_or(0, |ts| ts.get()),
                },
            ))
        })
        .collect::<Result<Vec<_>, VfsError>>()?;
    let tool_runs = builder.create_vector(&tool_run_offsets);

    let whiteout_offsets = snapshot
        .whiteouts
        .iter()
        .map(|path| builder.create_string(path))
        .collect::<Vec<_>>();
    let whiteouts = builder.create_vector(&whiteout_offsets);

    let origin_offsets = snapshot
        .origins
        .iter()
        .map(|(inode, origin)| {
            let base_volume_id = builder.create_vector(&origin.base_volume_id.encode());
            OriginEntry::create(
                builder,
                &OriginEntryArgs {
                    inode: inode.get(),
                    base_volume_id: Some(base_volume_id),
                    base_sequence: origin.base_sequence.get(),
                    base_durable: origin.base_durable,
                    base_inode: origin.base_inode.get(),
                },
            )
        })
        .collect::<Vec<_>>();
    let origins = builder.create_vector(&origin_offsets);

    let volume_id = builder.create_vector(&snapshot.info.volume_id.encode());
    Ok(VolumeArtifactManifest::create(
        builder,
        &VolumeArtifactManifestArgs {
            format_version: VFS_VOLUME_ARTIFACT_FORMAT_VERSION,
            volume_id: Some(volume_id),
            sequence: snapshot.sequence.get(),
            durable: snapshot.durable,
            chunk_size: snapshot.info.chunk_size,
            volume_format_version: snapshot.info.format_version,
            root_inode: snapshot.info.root_inode.get(),
            created_at: snapshot.info.created_at.get(),
            overlay_base,
            paths: Some(paths),
            inodes: Some(inodes),
            kv: Some(kv),
            tool_runs: Some(tool_runs),
            whiteouts: Some(whiteouts),
            origins: Some(origins),
        },
    ))
}

fn decode_file_content<R: Read>(
    payload: Option<FilePayloadEntry<'_>>,
    raw_len: u64,
    chunk_size: u32,
    reader: &mut R,
    payload_offset: &mut u64,
    inode_id: InodeId,
) -> Result<FileContent, VfsError> {
    if raw_len == 0 {
        if payload.is_some() {
            return Err(VfsError::VolumeArtifact {
                reason: format!("empty file inode {inode_id} has unexpected payload"),
            });
        }
        return Ok(FileContent::default());
    }

    let payload = payload.ok_or_else(|| VfsError::VolumeArtifact {
        reason: format!("file inode {inode_id} missing payload"),
    })?;
    if payload.payload_offset() != *payload_offset {
        return Err(VfsError::VolumeArtifact {
            reason: format!(
                "payload offset mismatch for inode {inode_id}: expected {}, found {}",
                *payload_offset,
                payload.payload_offset()
            ),
        });
    }

    let encoded_len = payload.payload_len();
    let mut content = FileContent::default();
    let mut source = reader.take(encoded_len);
    let chunk_size: usize = chunk_size
        .try_into()
        .map_err(|_| VfsError::VolumeArtifact {
            reason: format!("chunk size too large for inode {inode_id}: {chunk_size}"),
        })?;
    let mut remaining: usize = raw_len.try_into().map_err(|_| VfsError::VolumeArtifact {
        reason: format!("file payload too large for inode {inode_id}: {raw_len}"),
    })?;

    match payload.compression() {
        PayloadCompression::None => {
            if encoded_len != raw_len {
                return Err(VfsError::VolumeArtifact {
                    reason: format!(
                        "raw payload length mismatch for inode {inode_id}: encoded {encoded_len}, raw {raw_len}",
                    ),
                });
            }
            let mut chunk_index = 0_u64;
            while remaining > 0 {
                let chunk_len = remaining.min(chunk_size);
                let mut bytes = vec![0_u8; chunk_len];
                source
                    .read_exact(&mut bytes)
                    .map_err(io_error_to_artifact)?;
                content.chunks.insert(chunk_index, bytes);
                remaining -= chunk_len;
                chunk_index += 1;
            }
        }
        PayloadCompression::Zlib => {
            let mut decoder = ZlibDecoder::new(source);
            let mut chunk_index = 0_u64;
            while remaining > 0 {
                let chunk_len = remaining.min(chunk_size);
                let mut bytes = vec![0_u8; chunk_len];
                decoder
                    .read_exact(&mut bytes)
                    .map_err(io_error_to_artifact)?;
                content.chunks.insert(chunk_index, bytes);
                remaining -= chunk_len;
                chunk_index += 1;
            }
            let mut extra = Vec::new();
            decoder
                .read_to_end(&mut extra)
                .map_err(io_error_to_artifact)?;
            if !extra.is_empty() {
                return Err(VfsError::VolumeArtifact {
                    reason: format!(
                        "compressed payload for inode {inode_id} decoded beyond expected size"
                    ),
                });
            }
            source = decoder.into_inner();
        }
        other => {
            return Err(VfsError::VolumeArtifact {
                reason: format!("unknown payload compression tag {other:?} for inode {inode_id}"),
            });
        }
    }

    if source.limit() != 0 {
        return Err(VfsError::VolumeArtifact {
            reason: format!(
                "encoded payload length mismatch for inode {inode_id}: {} bytes left unread",
                source.limit()
            ),
        });
    }

    *payload_offset = payload_offset.saturating_add(encoded_len);
    Ok(content)
}

fn file_content_len(content: &FileContent) -> Result<u64, VfsError> {
    content.chunks.values().try_fold(0_u64, |acc, bytes| {
        let len: u64 = bytes
            .len()
            .try_into()
            .map_err(|_| VfsError::VolumeArtifact {
                reason: "file chunk length exceeds u64".to_string(),
            })?;
        Ok(acc.saturating_add(len))
    })
}

fn decode_overlay_base(overlay: OverlayBase<'_>) -> Result<super::OverlayBaseDescriptor, VfsError> {
    Ok(super::OverlayBaseDescriptor {
        volume_id: decode_volume_id(overlay.volume_id())?,
        sequence: SequenceNumber::new(overlay.sequence()),
        durable: overlay.durable(),
    })
}

fn decode_volume_id(bytes: flatbuffers::Vector<'_, u8>) -> Result<VolumeId, VfsError> {
    let bytes = bytes.iter().collect::<Vec<_>>();
    Ok(VolumeId::decode(&bytes)?)
}

fn encode_file_kind(kind: FileKind) -> u8 {
    match kind {
        FileKind::File => 1,
        FileKind::Directory => 2,
        FileKind::Symlink => 3,
    }
}

fn decode_file_kind(kind: u8) -> Result<FileKind, VfsError> {
    match kind {
        1 => Ok(FileKind::File),
        2 => Ok(FileKind::Directory),
        3 => Ok(FileKind::Symlink),
        other => Err(VfsError::VolumeArtifact {
            reason: format!("unknown file kind tag {other}"),
        }),
    }
}

fn encode_tool_run_status(status: &ToolRunStatus) -> u8 {
    match status {
        ToolRunStatus::Pending => 1,
        ToolRunStatus::Success => 2,
        ToolRunStatus::Error => 3,
    }
}

fn decode_tool_run_status(status: u8) -> Result<ToolRunStatus, VfsError> {
    match status {
        1 => Ok(ToolRunStatus::Pending),
        2 => Ok(ToolRunStatus::Success),
        3 => Ok(ToolRunStatus::Error),
        other => Err(VfsError::VolumeArtifact {
            reason: format!("unknown tool run status tag {other}"),
        }),
    }
}

fn encode_json(value: &JsonValue) -> Result<String, VfsError> {
    serde_json::to_string(value).map_err(|error| VfsError::VolumeArtifact {
        reason: format!("failed to encode json value: {error}"),
    })
}

fn decode_json(value: &str) -> Result<JsonValue, VfsError> {
    serde_json::from_str(value).map_err(|error| VfsError::VolumeArtifact {
        reason: format!("invalid json payload in artifact: {error}"),
    })
}

fn reject_trailing_bytes<R: Read>(reader: &mut R) -> Result<(), VfsError> {
    let mut extra = [0_u8; 1];
    match reader.read(&mut extra) {
        Ok(0) => Ok(()),
        Ok(_) => Err(VfsError::VolumeArtifact {
            reason: "artifact has trailing bytes".to_string(),
        }),
        Err(error) => Err(io_error_to_artifact(error)),
    }
}

fn io_error_to_artifact(error: std::io::Error) -> VfsError {
    VfsError::VolumeArtifact {
        reason: format!("artifact io error: {error}"),
    }
}
