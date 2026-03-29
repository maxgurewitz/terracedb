use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

macro_rules! id_newtype {
    ($name:ident, $inner:ty) => {
        #[derive(
            Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name($inner);

        impl $name {
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            pub const fn get(self) -> $inner {
                self.0
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

id_newtype!(SequenceNumber, u64);
id_newtype!(TableId, u32);
id_newtype!(ManifestId, u64);
id_newtype!(SegmentId, u64);
id_newtype!(FieldId, u32);
id_newtype!(Timestamp, u64);

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum IdEncodingError {
    #[error("invalid encoded length: expected {expected} bytes, got {actual}")]
    InvalidLength { expected: usize, actual: usize },
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitId {
    sequence: SequenceNumber,
    shard_hint: u32,
    reserved: u32,
}

impl CommitId {
    pub const ENCODED_LEN: usize = 16;

    pub const fn new(sequence: SequenceNumber) -> Self {
        Self {
            sequence,
            shard_hint: 0,
            reserved: 0,
        }
    }

    pub const fn sequence(self) -> SequenceNumber {
        self.sequence
    }

    pub const fn shard_hint(self) -> u32 {
        self.shard_hint
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0_u8; Self::ENCODED_LEN];
        bytes[..8].copy_from_slice(&self.sequence.get().to_be_bytes());
        bytes[8..12].copy_from_slice(&self.shard_hint.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.reserved.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IdEncodingError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(IdEncodingError::InvalidLength {
                expected: Self::ENCODED_LEN,
                actual: bytes.len(),
            });
        }

        let mut sequence = [0_u8; 8];
        sequence.copy_from_slice(&bytes[..8]);

        let mut shard_hint = [0_u8; 4];
        shard_hint.copy_from_slice(&bytes[8..12]);

        let mut reserved = [0_u8; 4];
        reserved.copy_from_slice(&bytes[12..16]);

        Ok(Self {
            sequence: SequenceNumber::new(u64::from_be_bytes(sequence)),
            shard_hint: u32::from_be_bytes(shard_hint),
            reserved: u32::from_be_bytes(reserved),
        })
    }
}

impl fmt::Debug for CommitId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitId")
            .field("sequence", &self.sequence)
            .field("shard_hint", &self.shard_hint)
            .finish()
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogCursor {
    sequence: SequenceNumber,
    op_index: u16,
    reserved: [u8; 6],
}

impl LogCursor {
    pub const ENCODED_LEN: usize = 16;

    pub const fn beginning() -> Self {
        Self {
            sequence: SequenceNumber::new(0),
            op_index: 0,
            reserved: [0; 6],
        }
    }

    pub const fn new(sequence: SequenceNumber, op_index: u16) -> Self {
        Self {
            sequence,
            op_index,
            reserved: [0; 6],
        }
    }

    pub const fn sequence(self) -> SequenceNumber {
        self.sequence
    }

    pub const fn op_index(self) -> u16 {
        self.op_index
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut bytes = [0_u8; Self::ENCODED_LEN];
        bytes[..8].copy_from_slice(&self.sequence.get().to_be_bytes());
        bytes[8..10].copy_from_slice(&self.op_index.to_be_bytes());
        bytes[10..].copy_from_slice(&self.reserved);
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IdEncodingError> {
        if bytes.len() != Self::ENCODED_LEN {
            return Err(IdEncodingError::InvalidLength {
                expected: Self::ENCODED_LEN,
                actual: bytes.len(),
            });
        }

        let mut sequence = [0_u8; 8];
        sequence.copy_from_slice(&bytes[..8]);

        let mut op_index = [0_u8; 2];
        op_index.copy_from_slice(&bytes[8..10]);

        let mut reserved = [0_u8; 6];
        reserved.copy_from_slice(&bytes[10..]);

        Ok(Self {
            sequence: SequenceNumber::new(u64::from_be_bytes(sequence)),
            op_index: u16::from_be_bytes(op_index),
            reserved,
        })
    }
}

impl fmt::Debug for LogCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LogCursor")
            .field("sequence", &self.sequence)
            .field("op_index", &self.op_index)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{CommitId, LogCursor, SequenceNumber};

    #[test]
    fn commit_id_round_trips() {
        let original = CommitId::new(SequenceNumber::new(42));
        let encoded = original.encode();
        let decoded = CommitId::decode(&encoded).expect("decode commit id");

        assert_eq!(decoded, original);
    }

    #[test]
    fn commit_id_encoding_preserves_sort_order() {
        let ids = [
            CommitId::new(SequenceNumber::new(1)),
            CommitId::new(SequenceNumber::new(2)),
            CommitId::new(SequenceNumber::new(3)),
        ];

        let mut encoded = ids.map(CommitId::encode);
        encoded.sort();

        assert_eq!(encoded, ids.map(CommitId::encode));
    }

    #[test]
    fn log_cursor_round_trips() {
        let original = LogCursor::new(SequenceNumber::new(9), 3);
        let encoded = original.encode();
        let decoded = LogCursor::decode(&encoded).expect("decode log cursor");

        assert_eq!(decoded, original);
    }

    #[test]
    fn log_cursor_encoding_preserves_sort_order() {
        let cursors = [
            LogCursor::new(SequenceNumber::new(7), 0),
            LogCursor::new(SequenceNumber::new(7), 1),
            LogCursor::new(SequenceNumber::new(8), 0),
        ];

        let mut encoded = cursors.map(LogCursor::encode);
        encoded.sort();

        assert_eq!(encoded, cursors.map(LogCursor::encode));
    }
}
