use std::collections::BTreeMap;

use serde_json::json;

use terracedb::{CompactionStrategy, TableConfig, TableFormat};

pub const VFS_VOLUME_TABLE_NAME: &str = "vfs_volume";
pub const VFS_ALLOCATOR_TABLE_NAME: &str = "vfs_allocator";
pub const VFS_INODE_TABLE_NAME: &str = "vfs_inode";
pub const VFS_DENTRY_TABLE_NAME: &str = "vfs_dentry";
pub const VFS_CHUNK_TABLE_NAME: &str = "vfs_chunk";
pub const VFS_SYMLINK_TABLE_NAME: &str = "vfs_symlink";
pub const VFS_KV_TABLE_NAME: &str = "vfs_kv";
pub const VFS_TOOL_RUN_TABLE_NAME: &str = "vfs_tool_run";
pub const VFS_ACTIVITY_TABLE_NAME: &str = "vfs_activity";
pub const VFS_WHITEOUT_TABLE_NAME: &str = "vfs_whiteout";
pub const VFS_ORIGIN_TABLE_NAME: &str = "vfs_origin";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservedTableDescriptor {
    pub name: &'static str,
    pub key_shape: &'static str,
    pub purpose: &'static str,
    pub append_only: bool,
}

impl ReservedTableDescriptor {
    pub fn table_config(&self) -> TableConfig {
        let mut metadata = BTreeMap::new();
        metadata.insert("terracedb_vfs.key_shape".to_string(), json!(self.key_shape));
        metadata.insert("terracedb_vfs.purpose".to_string(), json!(self.purpose));
        metadata.insert(
            "terracedb_vfs.append_only".to_string(),
            json!(self.append_only),
        );

        TableConfig {
            name: self.name.to_string(),
            format: TableFormat::Row,
            merge_operator: None,
            max_merge_operand_chain_length: None,
            compaction_filter: None,
            bloom_filter_bits_per_key: Some(10),
            history_retention_sequences: None,
            compaction_strategy: if self.append_only {
                CompactionStrategy::Tiered
            } else {
                CompactionStrategy::Leveled
            },
            schema: None,
            metadata,
        }
    }
}

pub const RESERVED_TABLES: [ReservedTableDescriptor; 11] = [
    ReservedTableDescriptor {
        name: VFS_VOLUME_TABLE_NAME,
        key_shape: "(volume_id)",
        purpose: "Volume metadata: immutable chunk_size, format version, creation time, root inode, optional overlay base descriptor",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_ALLOCATOR_TABLE_NAME,
        key_shape: "(volume_id, kind)",
        purpose: "Persisted high-water marks for ino, activity_id, and tool_run_id block leasing",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_INODE_TABLE_NAME,
        key_shape: "(volume_id, ino)",
        purpose: "Current inode metadata: mode, nlink, uid, gid, size, timestamps, nanoseconds, rdev",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_DENTRY_TABLE_NAME,
        key_shape: "(volume_id, parent_ino, name)",
        purpose: "Directory entry mapping from parent/name to child inode",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_CHUNK_TABLE_NAME,
        key_shape: "(volume_id, ino, chunk_index)",
        purpose: "Current file bytes in fixed-size chunks",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_SYMLINK_TABLE_NAME,
        key_shape: "(volume_id, ino)",
        purpose: "Symlink target text",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_KV_TABLE_NAME,
        key_shape: "(volume_id, key)",
        purpose: "Current JSON-serialized KV entries",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_TOOL_RUN_TABLE_NAME,
        key_shape: "(volume_id, tool_run_id)",
        purpose: "Current tool-run row (pending, success, error, timestamps, params/result/error)",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_ACTIVITY_TABLE_NAME,
        key_shape: "(volume_id, activity_id)",
        purpose: "Append-only semantic audit stream for filesystem, KV, and tool mutations",
        append_only: true,
    },
    ReservedTableDescriptor {
        name: VFS_WHITEOUT_TABLE_NAME,
        key_shape: "(volume_id, path)",
        purpose: "Overlay whiteouts keyed by normalized path",
        append_only: false,
    },
    ReservedTableDescriptor {
        name: VFS_ORIGIN_TABLE_NAME,
        key_shape: "(volume_id, delta_ino)",
        purpose: "Copy-up provenance: which base volume/snapshot/inode a delta inode originated from",
        append_only: false,
    },
];

pub fn reserved_table_descriptors() -> &'static [ReservedTableDescriptor] {
    &RESERVED_TABLES
}

pub fn reserved_table(name: &str) -> Option<&'static ReservedTableDescriptor> {
    RESERVED_TABLES
        .iter()
        .find(|descriptor| descriptor.name == name)
}

pub fn reserved_table_configs() -> Vec<TableConfig> {
    RESERVED_TABLES
        .iter()
        .map(ReservedTableDescriptor::table_config)
        .collect()
}
