use std::sync::Arc;

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

pub trait JsEntropySource: Send + Sync {
    fn fill_bytes(&self, len: usize) -> Vec<u8>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsEntropySnapshot {
    pub state: u64,
}

#[derive(Debug)]
pub struct DeterministicJsEntropySource {
    state: Arc<Mutex<u64>>,
}

impl DeterministicJsEntropySource {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(seed.max(1))),
        }
    }

    pub fn snapshot(&self) -> JsEntropySnapshot {
        JsEntropySnapshot {
            state: *self.state.lock().expect("entropy mutex poisoned"),
        }
    }

    pub fn restore(&self, snapshot: JsEntropySnapshot) {
        *self.state.lock().expect("entropy mutex poisoned") = snapshot.state.max(1);
    }

    pub fn next_u64(&self) -> u64 {
        let mut locked = self.state.lock().expect("entropy mutex poisoned");
        let mut x = *locked;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *locked = x;
        x
    }
}

impl Default for DeterministicJsEntropySource {
    fn default() -> Self {
        Self::new(0x5eed_u64)
    }
}

impl Clone for DeterministicJsEntropySource {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl JsEntropySource for DeterministicJsEntropySource {
    fn fill_bytes(&self, len: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            bytes.extend_from_slice(&self.next_u64().to_le_bytes());
        }
        bytes.truncate(len);
        bytes
    }
}

impl From<DeterministicJsEntropySource> for Arc<dyn JsEntropySource> {
    fn from(value: DeterministicJsEntropySource) -> Self {
        Arc::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{DeterministicJsEntropySource, JsEntropySnapshot, JsEntropySource};

    #[test]
    fn deterministic_entropy_replays_from_snapshots() {
        let entropy = DeterministicJsEntropySource::new(0x1234);
        let snapshot = entropy.snapshot();
        assert_eq!(snapshot, JsEntropySnapshot { state: 0x1234 });

        let first = entropy.fill_bytes(16);
        let second = entropy.fill_bytes(16);
        assert_ne!(first, second);

        entropy.restore(snapshot);
        assert_eq!(entropy.fill_bytes(16), first);
    }
}
