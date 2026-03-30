use std::fmt;

use serde::{Deserialize, Serialize};

use terracedb::IdEncodingError;

use crate::AgentFsError;

macro_rules! encoded_id_newtype {
    ($name:ident, $inner:ty) => {
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
                write!(f, "{}({})", stringify!($name), self.0)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

encoded_id_newtype!(InodeId, u64);
encoded_id_newtype!(ActivityId, u64);
encoded_id_newtype!(ToolRunId, u64);

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VolumeId(u128);

impl VolumeId {
    pub const ENCODED_LEN: usize = 16;

    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u128 {
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
        Ok(Self(u128::from_be_bytes(raw)))
    }
}

impl From<u128> for VolumeId {
    fn from(value: u128) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "VolumeId({self})")
    }
}

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AllocatorKind {
    Inode = 1,
    Activity = 2,
    ToolRun = 3,
}

impl AllocatorKind {
    pub const ENCODED_LEN: usize = 1;

    pub fn encode(self) -> [u8; 1] {
        [self as u8]
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        match bytes {
            [1] => Ok(Self::Inode),
            [2] => Ok(Self::Activity),
            [3] => Ok(Self::ToolRun),
            [other] => Err(AgentFsError::InvalidKey {
                reason: format!("unknown allocator kind tag {other}"),
            }),
            _ => Err(AgentFsError::InvalidKey {
                reason: format!(
                    "allocator kind expects {} byte, got {}",
                    Self::ENCODED_LEN,
                    bytes.len()
                ),
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeKey {
    pub volume_id: VolumeId,
}

impl VolumeKey {
    pub fn encode(&self) -> Vec<u8> {
        self.volume_id.encode().to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        Ok(Self {
            volume_id: VolumeId::decode(bytes)?,
        })
    }

    pub fn prefix(volume_id: VolumeId) -> Vec<u8> {
        volume_id.encode().to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AllocatorKey {
    pub volume_id: VolumeId,
    pub kind: AllocatorKind,
}

impl AllocatorKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + AllocatorKind::ENCODED_LEN);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.kind.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() != VolumeId::ENCODED_LEN + AllocatorKind::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "allocator key expects {} bytes, got {}",
                    VolumeId::ENCODED_LEN + AllocatorKind::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            kind: AllocatorKind::decode(&bytes[VolumeId::ENCODED_LEN..])?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodeKey {
    pub volume_id: VolumeId,
    pub inode: InodeId,
}

impl InodeKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.inode.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() != VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "inode key expects {} bytes, got {}",
                    VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            inode: InodeId::decode(&bytes[VolumeId::ENCODED_LEN..])?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DentryKey {
    pub volume_id: VolumeId,
    pub parent: InodeId,
    pub name: String,
}

impl DentryKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN + self.name.len());
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.parent.encode());
        bytes.extend_from_slice(self.name.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        let fixed = VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN;
        if bytes.len() < fixed {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "dentry key expects at least {fixed} bytes, got {}",
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            parent: InodeId::decode(&bytes[VolumeId::ENCODED_LEN..fixed])?,
            name: String::from_utf8(bytes[fixed..].to_vec())?,
        })
    }

    pub fn prefix(volume_id: VolumeId, parent: InodeId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN);
        bytes.extend_from_slice(&volume_id.encode());
        bytes.extend_from_slice(&parent.encode());
        bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkKey {
    pub volume_id: VolumeId,
    pub inode: InodeId,
    pub chunk_index: u64,
}

impl ChunkKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN + 8);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.inode.encode());
        bytes.extend_from_slice(&self.chunk_index.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        let fixed = VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN + 8;
        if bytes.len() != fixed {
            return Err(AgentFsError::InvalidKey {
                reason: format!("chunk key expects {fixed} bytes, got {}", bytes.len()),
            });
        }

        let mut chunk_index = [0_u8; 8];
        chunk_index.copy_from_slice(&bytes[VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN..]);

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            inode: InodeId::decode(
                &bytes[VolumeId::ENCODED_LEN..VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN],
            )?,
            chunk_index: u64::from_be_bytes(chunk_index),
        })
    }

    pub fn prefix(volume_id: VolumeId, inode: InodeId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN);
        bytes.extend_from_slice(&volume_id.encode());
        bytes.extend_from_slice(&inode.encode());
        bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SymlinkKey {
    pub volume_id: VolumeId,
    pub inode: InodeId,
}

impl SymlinkKey {
    pub fn encode(&self) -> Vec<u8> {
        InodeKey {
            volume_id: self.volume_id,
            inode: self.inode,
        }
        .encode()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        let key = InodeKey::decode(bytes)?;
        Ok(Self {
            volume_id: key.volume_id,
            inode: key.inode,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KvKey {
    pub volume_id: VolumeId,
    pub key: String,
}

impl KvKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + self.key.len());
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(self.key.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() < VolumeId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "kv key expects at least {} bytes, got {}",
                    VolumeId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            key: String::from_utf8(bytes[VolumeId::ENCODED_LEN..].to_vec())?,
        })
    }

    pub fn prefix(volume_id: VolumeId) -> Vec<u8> {
        volume_id.encode().to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolRunKey {
    pub volume_id: VolumeId,
    pub tool_run_id: ToolRunId,
}

impl ToolRunKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + ToolRunId::ENCODED_LEN);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.tool_run_id.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() != VolumeId::ENCODED_LEN + ToolRunId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "tool run key expects {} bytes, got {}",
                    VolumeId::ENCODED_LEN + ToolRunId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            tool_run_id: ToolRunId::decode(&bytes[VolumeId::ENCODED_LEN..])?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivityKey {
    pub volume_id: VolumeId,
    pub activity_id: ActivityId,
}

impl ActivityKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + ActivityId::ENCODED_LEN);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.activity_id.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() != VolumeId::ENCODED_LEN + ActivityId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "activity key expects {} bytes, got {}",
                    VolumeId::ENCODED_LEN + ActivityId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            activity_id: ActivityId::decode(&bytes[VolumeId::ENCODED_LEN..])?,
        })
    }

    pub fn prefix(volume_id: VolumeId) -> Vec<u8> {
        volume_id.encode().to_vec()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhiteoutKey {
    pub volume_id: VolumeId,
    pub path: String,
}

impl WhiteoutKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + self.path.len());
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(self.path.as_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() < VolumeId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "whiteout key expects at least {} bytes, got {}",
                    VolumeId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            path: String::from_utf8(bytes[VolumeId::ENCODED_LEN..].to_vec())?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OriginKey {
    pub volume_id: VolumeId,
    pub delta_inode: InodeId,
}

impl OriginKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN);
        bytes.extend_from_slice(&self.volume_id.encode());
        bytes.extend_from_slice(&self.delta_inode.encode());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AgentFsError> {
        if bytes.len() != VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN {
            return Err(AgentFsError::InvalidKey {
                reason: format!(
                    "origin key expects {} bytes, got {}",
                    VolumeId::ENCODED_LEN + InodeId::ENCODED_LEN,
                    bytes.len()
                ),
            });
        }

        Ok(Self {
            volume_id: VolumeId::decode(&bytes[..VolumeId::ENCODED_LEN])?,
            delta_inode: InodeId::decode(&bytes[VolumeId::ENCODED_LEN..])?,
        })
    }
}
