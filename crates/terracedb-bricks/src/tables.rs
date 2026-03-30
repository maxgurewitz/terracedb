use std::collections::BTreeMap;

use serde_json::json;

use terracedb::{CompactionStrategy, TableConfig, TableFormat};

pub const BLOB_CATALOG_TABLE_NAME: &str = "blob_catalog";
pub const BLOB_ALIAS_TABLE_NAME: &str = "blob_alias";
pub const BLOB_OBJECT_GC_TABLE_NAME: &str = "blob_object_gc";
pub const BLOB_ACTIVITY_TABLE_NAME: &str = "blob_activity";
pub const BLOB_TEXT_CHUNK_TABLE_NAME: &str = "blob_text_chunk";
pub const BLOB_TERM_INDEX_TABLE_NAME: &str = "blob_term_index";
pub const BLOB_EMBEDDING_INDEX_TABLE_NAME: &str = "blob_embedding_index";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrozenTableOwner {
    Library,
    Projection,
    Application,
}

impl FrozenTableOwner {
    fn metadata_value(self) -> &'static str {
        match self {
            Self::Library => "library",
            Self::Projection => "projection",
            Self::Application => "application",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrozenTableDescriptor {
    pub name: &'static str,
    pub key_shape: &'static str,
    pub purpose: &'static str,
    pub append_only: bool,
    pub owner: FrozenTableOwner,
    pub bootstrap: bool,
}

impl FrozenTableDescriptor {
    pub fn table_config(&self) -> TableConfig {
        let mut metadata = BTreeMap::new();
        metadata.insert(
            "terracedb_bricks.key_shape".to_string(),
            json!(self.key_shape),
        );
        metadata.insert("terracedb_bricks.purpose".to_string(), json!(self.purpose));
        metadata.insert(
            "terracedb_bricks.append_only".to_string(),
            json!(self.append_only),
        );
        metadata.insert(
            "terracedb_bricks.owner".to_string(),
            json!(self.owner.metadata_value()),
        );
        metadata.insert(
            "terracedb_bricks.bootstrap".to_string(),
            json!(self.bootstrap),
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

pub const FROZEN_TABLES: [FrozenTableDescriptor; 7] = [
    FrozenTableDescriptor {
        name: BLOB_CATALOG_TABLE_NAME,
        key_shape: "(namespace, blob_id)",
        purpose: "Current metadata row: object key, digest, byte length, content type, tags, application metadata, timestamps, and indexing status",
        append_only: false,
        owner: FrozenTableOwner::Library,
        bootstrap: true,
    },
    FrozenTableDescriptor {
        name: BLOB_ALIAS_TABLE_NAME,
        key_shape: "(namespace, alias)",
        purpose: "Optional stable alias mapping to the current blob ID for alias-based upserts and lookups",
        append_only: false,
        owner: FrozenTableOwner::Library,
        bootstrap: true,
    },
    FrozenTableDescriptor {
        name: BLOB_OBJECT_GC_TABLE_NAME,
        key_shape: "(namespace, object_key)",
        purpose: "Optional GC helper rows recording first-seen object metadata and cleanup bookkeeping",
        append_only: false,
        owner: FrozenTableOwner::Library,
        bootstrap: true,
    },
    FrozenTableDescriptor {
        name: BLOB_ACTIVITY_TABLE_NAME,
        key_shape: "(namespace, activity_id)",
        purpose: "Append-only semantic audit stream for blob publish, delete, alias, and indexing milestones",
        append_only: true,
        owner: FrozenTableOwner::Library,
        bootstrap: true,
    },
    FrozenTableDescriptor {
        name: BLOB_TEXT_CHUNK_TABLE_NAME,
        key_shape: "(namespace, blob_id, extractor, chunk_index)",
        purpose: "Projection-owned extracted-text or preview chunks keyed for deterministic snippet retrieval and rebuild",
        append_only: false,
        owner: FrozenTableOwner::Projection,
        bootstrap: false,
    },
    FrozenTableDescriptor {
        name: BLOB_TERM_INDEX_TABLE_NAME,
        key_shape: "(namespace, term, blob_id, extractor, chunk_index)",
        purpose: "Projection-owned normalized term, tag, or prefix index rows derived from metadata or extracted text",
        append_only: false,
        owner: FrozenTableOwner::Projection,
        bootstrap: false,
    },
    FrozenTableDescriptor {
        name: BLOB_EMBEDDING_INDEX_TABLE_NAME,
        key_shape: "(namespace, index_name, blob_id)",
        purpose: "Application-owned vector or semantic retrieval metadata layered above the blob catalog",
        append_only: false,
        owner: FrozenTableOwner::Application,
        bootstrap: false,
    },
];

pub fn frozen_table_descriptors() -> &'static [FrozenTableDescriptor] {
    &FROZEN_TABLES
}

pub fn frozen_table(name: &str) -> Option<&'static FrozenTableDescriptor> {
    FROZEN_TABLES
        .iter()
        .find(|descriptor| descriptor.name == name)
}

pub fn frozen_table_configs() -> Vec<TableConfig> {
    FROZEN_TABLES
        .iter()
        .filter(|descriptor| descriptor.bootstrap)
        .map(FrozenTableDescriptor::table_config)
        .collect()
}
