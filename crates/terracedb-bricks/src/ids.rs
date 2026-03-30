use std::fmt;

use serde::{Deserialize, Serialize};

use terracedb::IdEncodingError;

use crate::BlobContractError;

const SEPARATOR: u8 = 0;

fn validate_namespace(namespace: &str) -> Result<(), BlobContractError> {
    validate_key_part(namespace, "namespace", true)
}

fn validate_key_part(
    value: &str,
    field: &'static str,
    require_non_empty: bool,
) -> Result<(), BlobContractError> {
    if require_non_empty && value.is_empty() {
        return Err(BlobContractError::EmptyKeyPart { field });
    }
    if value.as_bytes().contains(&SEPARATOR) {
        return Err(BlobContractError::NulByteInKeyPart { field });
    }
    Ok(())
}

fn encode_namespace_prefix(namespace: &str) -> Result<Vec<u8>, BlobContractError> {
    validate_namespace(namespace)?;
    let mut bytes = Vec::with_capacity(namespace.len() + 1);
    bytes.extend_from_slice(namespace.as_bytes());
    bytes.push(SEPARATOR);
    Ok(bytes)
}

fn decode_namespace(bytes: &[u8]) -> Result<String, BlobContractError> {
    validate_namespace_bytes(bytes)?;
    String::from_utf8(bytes.to_vec()).map_err(|error| BlobContractError::InvalidKey {
        reason: format!("namespace is not valid utf-8: {error}"),
    })
}

fn decode_string(bytes: &[u8], field: &'static str) -> Result<String, BlobContractError> {
    let value =
        String::from_utf8(bytes.to_vec()).map_err(|error| BlobContractError::InvalidKey {
            reason: format!("{field} is not valid utf-8: {error}"),
        })?;
    validate_key_part(&value, field, true)?;
    Ok(value)
}

fn validate_namespace_bytes(bytes: &[u8]) -> Result<(), BlobContractError> {
    if bytes.is_empty() {
        return Err(BlobContractError::EmptyNamespace);
    }
    if bytes.contains(&SEPARATOR) {
        return Err(BlobContractError::NulByteInKeyPart { field: "namespace" });
    }
    Ok(())
}

fn split_once<'a>(
    bytes: &'a [u8],
    field: &'static str,
) -> Result<(&'a [u8], &'a [u8]), BlobContractError> {
    let Some(index) = bytes.iter().position(|byte| *byte == SEPARATOR) else {
        return Err(BlobContractError::InvalidKey {
            reason: format!("{field} key is missing a namespace separator"),
        });
    };

    Ok((&bytes[..index], &bytes[index + 1..]))
}

macro_rules! encoded_id_newtype {
    ($name:ident, $inner:ty, $display_fmt:expr) => {
        #[derive(
            Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name($inner);

        impl $name {
            pub const ENCODED_LEN: usize = std::mem::size_of::<$inner>();

            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            pub const fn get(self) -> $inner {
                self.0
            }

            pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
                self.0.to_be_bytes()
            }

            pub fn decode(bytes: &[u8]) -> Result<Self, IdEncodingError> {
                if bytes.len() != Self::ENCODED_LEN {
                    return Err(IdEncodingError::InvalidLength {
                        expected: Self::ENCODED_LEN,
                        actual: bytes.len(),
                    });
                }

                let mut raw = [0_u8; Self::ENCODED_LEN];
                raw.copy_from_slice(bytes);
                Ok(Self(<$inner>::from_be_bytes(raw)))
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({self})", stringify!($name))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, $display_fmt, self.0)
            }
        }
    };
}

encoded_id_newtype!(BlobId, u128, "{:032x}");
encoded_id_newtype!(BlobActivityId, u64, "{}");

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobAlias(String);

impl BlobAlias {
    pub fn new(alias: impl Into<String>) -> Result<Self, BlobContractError> {
        let alias = alias.into();
        if alias.is_empty() {
            return Err(BlobContractError::EmptyAlias);
        }
        validate_key_part(&alias, "alias", true)?;
        Ok(Self(alias))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn encode(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let alias =
            String::from_utf8(bytes.to_vec()).map_err(|error| BlobContractError::InvalidKey {
                reason: format!("alias is not valid utf-8: {error}"),
            })?;
        Self::new(alias)
    }
}

impl fmt::Debug for BlobAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobAlias({})", self.0)
    }
}

impl fmt::Display for BlobAlias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobCatalogKey {
    pub namespace: String,
    pub blob_id: BlobId,
}

impl BlobCatalogKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(&self.blob_id.encode());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        if bytes.len() <= BlobId::ENCODED_LEN {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob catalog key expects at least {} bytes, got {}",
                    BlobId::ENCODED_LEN + 2,
                    bytes.len()
                ),
            });
        }

        let separator_index = bytes
            .len()
            .checked_sub(BlobId::ENCODED_LEN + 1)
            .ok_or_else(|| BlobContractError::InvalidKey {
                reason: "blob catalog key is truncated".to_string(),
            })?;
        if bytes[separator_index] != SEPARATOR {
            return Err(BlobContractError::InvalidKey {
                reason: "blob catalog key is missing the namespace separator".to_string(),
            });
        }

        Ok(Self {
            namespace: decode_namespace(&bytes[..separator_index])?,
            blob_id: BlobId::decode(&bytes[separator_index + 1..])?,
        })
    }

    pub fn namespace_prefix(namespace: &str) -> Result<Vec<u8>, BlobContractError> {
        encode_namespace_prefix(namespace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobAliasKey {
    pub namespace: String,
    pub alias: BlobAlias,
}

impl BlobAliasKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(self.alias.as_str().as_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let (namespace, alias) = split_once(bytes, "blob alias")?;
        Ok(Self {
            namespace: decode_namespace(namespace)?,
            alias: BlobAlias::decode(alias)?,
        })
    }

    pub fn namespace_prefix(namespace: &str) -> Result<Vec<u8>, BlobContractError> {
        encode_namespace_prefix(namespace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobObjectGcKey {
    pub namespace: String,
    pub object_key: String,
}

impl BlobObjectGcKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        validate_key_part(&self.object_key, "object_key", true)?;
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(self.object_key.as_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let (namespace, object_key) = split_once(bytes, "blob object gc")?;
        Ok(Self {
            namespace: decode_namespace(namespace)?,
            object_key: decode_string(object_key, "object_key")?,
        })
    }

    pub fn namespace_prefix(namespace: &str) -> Result<Vec<u8>, BlobContractError> {
        encode_namespace_prefix(namespace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobActivityKey {
    pub namespace: String,
    pub activity_id: BlobActivityId,
}

impl BlobActivityKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(&self.activity_id.encode());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        if bytes.len() <= BlobActivityId::ENCODED_LEN {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob activity key expects at least {} bytes, got {}",
                    BlobActivityId::ENCODED_LEN + 2,
                    bytes.len()
                ),
            });
        }

        let separator_index = bytes
            .len()
            .checked_sub(BlobActivityId::ENCODED_LEN + 1)
            .ok_or_else(|| BlobContractError::InvalidKey {
                reason: "blob activity key is truncated".to_string(),
            })?;
        if bytes[separator_index] != SEPARATOR {
            return Err(BlobContractError::InvalidKey {
                reason: "blob activity key is missing the namespace separator".to_string(),
            });
        }

        Ok(Self {
            namespace: decode_namespace(&bytes[..separator_index])?,
            activity_id: BlobActivityId::decode(&bytes[separator_index + 1..])?,
        })
    }

    pub fn namespace_prefix(namespace: &str) -> Result<Vec<u8>, BlobContractError> {
        encode_namespace_prefix(namespace)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobTextChunkKey {
    pub namespace: String,
    pub blob_id: BlobId,
    pub extractor: String,
    pub chunk_index: u32,
}

impl BlobTextChunkKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        validate_key_part(&self.extractor, "extractor", true)?;
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(&self.blob_id.encode());
        bytes.extend_from_slice(self.extractor.as_bytes());
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let (namespace, rest) = split_once(bytes, "blob text chunk")?;
        if rest.len() < BlobId::ENCODED_LEN + 1 + std::mem::size_of::<u32>() {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob text chunk key expects at least {} bytes after namespace, got {}",
                    BlobId::ENCODED_LEN + 1 + std::mem::size_of::<u32>(),
                    rest.len()
                ),
            });
        }

        let blob_id = BlobId::decode(&rest[..BlobId::ENCODED_LEN])?;
        let extractor_and_index = &rest[BlobId::ENCODED_LEN..];
        let Some(separator_index) = extractor_and_index
            .iter()
            .position(|byte| *byte == SEPARATOR)
        else {
            return Err(BlobContractError::InvalidKey {
                reason: "blob text chunk key is missing the extractor separator".to_string(),
            });
        };
        let extractor = decode_string(&extractor_and_index[..separator_index], "extractor")?;
        let chunk_bytes = &extractor_and_index[separator_index + 1..];
        if chunk_bytes.len() != std::mem::size_of::<u32>() {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob text chunk key expects {} chunk-index bytes, got {}",
                    std::mem::size_of::<u32>(),
                    chunk_bytes.len()
                ),
            });
        }
        let mut chunk_index = [0_u8; std::mem::size_of::<u32>()];
        chunk_index.copy_from_slice(chunk_bytes);

        Ok(Self {
            namespace: decode_namespace(namespace)?,
            blob_id,
            extractor,
            chunk_index: u32::from_be_bytes(chunk_index),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobTermIndexKey {
    pub namespace: String,
    pub term: String,
    pub blob_id: BlobId,
    pub extractor: String,
    pub chunk_index: u32,
}

impl BlobTermIndexKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        validate_key_part(&self.term, "term", true)?;
        validate_key_part(&self.extractor, "extractor", true)?;
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(self.term.as_bytes());
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.blob_id.encode());
        bytes.extend_from_slice(self.extractor.as_bytes());
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let (namespace, rest) = split_once(bytes, "blob term index")?;
        let (term, after_term) = split_once(rest, "blob term index")?;
        if after_term.len() < BlobId::ENCODED_LEN + 1 + std::mem::size_of::<u32>() {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob term index key expects at least {} bytes after term, got {}",
                    BlobId::ENCODED_LEN + 1 + std::mem::size_of::<u32>(),
                    after_term.len()
                ),
            });
        }

        let blob_id = BlobId::decode(&after_term[..BlobId::ENCODED_LEN])?;
        let extractor_and_index = &after_term[BlobId::ENCODED_LEN..];
        let Some(separator_index) = extractor_and_index
            .iter()
            .position(|byte| *byte == SEPARATOR)
        else {
            return Err(BlobContractError::InvalidKey {
                reason: "blob term index key is missing the extractor separator".to_string(),
            });
        };
        let extractor = decode_string(&extractor_and_index[..separator_index], "extractor")?;
        let chunk_bytes = &extractor_and_index[separator_index + 1..];
        if chunk_bytes.len() != std::mem::size_of::<u32>() {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob term index key expects {} chunk-index bytes, got {}",
                    std::mem::size_of::<u32>(),
                    chunk_bytes.len()
                ),
            });
        }
        let mut chunk_index = [0_u8; std::mem::size_of::<u32>()];
        chunk_index.copy_from_slice(chunk_bytes);

        Ok(Self {
            namespace: decode_namespace(namespace)?,
            term: decode_string(term, "term")?,
            blob_id,
            extractor,
            chunk_index: u32::from_be_bytes(chunk_index),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobEmbeddingIndexKey {
    pub namespace: String,
    pub index_name: String,
    pub blob_id: BlobId,
}

impl BlobEmbeddingIndexKey {
    pub fn encode(&self) -> Result<Vec<u8>, BlobContractError> {
        validate_key_part(&self.index_name, "index_name", true)?;
        let mut bytes = encode_namespace_prefix(&self.namespace)?;
        bytes.extend_from_slice(self.index_name.as_bytes());
        bytes.push(SEPARATOR);
        bytes.extend_from_slice(&self.blob_id.encode());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, BlobContractError> {
        let (namespace, rest) = split_once(bytes, "blob embedding index")?;
        if rest.len() <= BlobId::ENCODED_LEN {
            return Err(BlobContractError::InvalidKey {
                reason: format!(
                    "blob embedding index key expects at least {} bytes after namespace, got {}",
                    BlobId::ENCODED_LEN + 2,
                    rest.len()
                ),
            });
        }

        let separator_index = rest
            .len()
            .checked_sub(BlobId::ENCODED_LEN + 1)
            .ok_or_else(|| BlobContractError::InvalidKey {
                reason: "blob embedding index key is truncated".to_string(),
            })?;
        if rest[separator_index] != SEPARATOR {
            return Err(BlobContractError::InvalidKey {
                reason: "blob embedding index key is missing the index-name separator".to_string(),
            });
        }

        Ok(Self {
            namespace: decode_namespace(namespace)?,
            index_name: decode_string(&rest[..separator_index], "index_name")?,
            blob_id: BlobId::decode(&rest[separator_index + 1..])?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BlobActivityId, BlobActivityKey, BlobAlias, BlobAliasKey, BlobCatalogKey,
        BlobEmbeddingIndexKey, BlobId, BlobObjectGcKey, BlobTermIndexKey, BlobTextChunkKey,
    };

    #[test]
    fn blob_id_round_trips() {
        let original = BlobId::new(0xfeed_beef);
        let encoded = original.encode();
        let decoded = BlobId::decode(&encoded).expect("decode blob id");
        assert_eq!(decoded, original);
    }

    #[test]
    fn alias_round_trips() {
        let alias = BlobAlias::new("docs/readme").expect("valid alias");
        let decoded = BlobAlias::decode(&alias.encode()).expect("decode alias");
        assert_eq!(decoded, alias);
    }

    #[test]
    fn reserved_keys_round_trip() {
        let catalog = BlobCatalogKey {
            namespace: "photos".to_string(),
            blob_id: BlobId::new(1),
        };
        let alias = BlobAliasKey {
            namespace: "photos".to_string(),
            alias: BlobAlias::new("cover").expect("alias"),
        };
        let gc = BlobObjectGcKey {
            namespace: "photos".to_string(),
            object_key: "blobs/photos/objects/1".to_string(),
        };
        let activity = BlobActivityKey {
            namespace: "photos".to_string(),
            activity_id: BlobActivityId::new(7),
        };
        let text_chunk = BlobTextChunkKey {
            namespace: "photos".to_string(),
            blob_id: BlobId::new(1),
            extractor: "plain".to_string(),
            chunk_index: 2,
        };
        let term_index = BlobTermIndexKey {
            namespace: "photos".to_string(),
            term: "terracedb".to_string(),
            blob_id: BlobId::new(1),
            extractor: "plain".to_string(),
            chunk_index: 2,
        };
        let embedding = BlobEmbeddingIndexKey {
            namespace: "photos".to_string(),
            index_name: "default".to_string(),
            blob_id: BlobId::new(1),
        };

        assert_eq!(
            BlobCatalogKey::decode(&catalog.encode().expect("encode catalog"))
                .expect("decode catalog"),
            catalog
        );
        assert_eq!(
            BlobAliasKey::decode(&alias.encode().expect("encode alias")).expect("decode alias"),
            alias
        );
        assert_eq!(
            BlobObjectGcKey::decode(&gc.encode().expect("encode gc")).expect("decode gc"),
            gc
        );
        assert_eq!(
            BlobActivityKey::decode(&activity.encode().expect("encode activity"))
                .expect("decode activity"),
            activity
        );
        assert_eq!(
            BlobTextChunkKey::decode(&text_chunk.encode().expect("encode text chunk"))
                .expect("decode text chunk"),
            text_chunk
        );
        assert_eq!(
            BlobTermIndexKey::decode(&term_index.encode().expect("encode term index"))
                .expect("decode term index"),
            term_index
        );
        assert_eq!(
            BlobEmbeddingIndexKey::decode(&embedding.encode().expect("encode embedding"))
                .expect("decode embedding"),
            embedding
        );
    }
}
