use std::sync::Arc;

use std::sync::Mutex;

pub trait JsEntropySource: Send + Sync {
    fn fill_bytes(&self, len: usize) -> Vec<u8>;
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
        let mut locked = self.state.lock().expect("entropy mutex poisoned");
        let mut bytes = Vec::with_capacity(len);
        while bytes.len() < len {
            let mut x = *locked;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            *locked = x;
            bytes.extend_from_slice(&x.to_le_bytes());
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
