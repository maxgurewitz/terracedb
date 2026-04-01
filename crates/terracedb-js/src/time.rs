use std::sync::Arc;

pub trait JsClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Clone, Debug)]
pub struct FixedJsClock {
    now_millis: u64,
}

impl FixedJsClock {
    pub fn new(now_millis: u64) -> Self {
        Self { now_millis }
    }
}

impl Default for FixedJsClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl JsClock for FixedJsClock {
    fn now_millis(&self) -> u64 {
        self.now_millis
    }
}

impl From<FixedJsClock> for Arc<dyn JsClock> {
    fn from(value: FixedJsClock) -> Self {
        Arc::new(value)
    }
}
