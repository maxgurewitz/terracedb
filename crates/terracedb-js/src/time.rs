use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

pub trait JsClock: Send + Sync {
    fn now_millis(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsClockSnapshot {
    pub now_millis: u64,
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

#[derive(Debug)]
pub struct DeterministicJsClock {
    state: Arc<Mutex<u64>>,
}

impl DeterministicJsClock {
    pub fn new(now_millis: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(now_millis)),
        }
    }

    pub fn advance_millis(&self, millis: u64) -> u64 {
        let mut locked = self.state.lock().expect("clock mutex poisoned");
        *locked = locked.saturating_add(millis);
        *locked
    }

    pub fn set_now_millis(&self, now_millis: u64) {
        *self.state.lock().expect("clock mutex poisoned") = now_millis;
    }

    pub fn snapshot(&self) -> JsClockSnapshot {
        JsClockSnapshot {
            now_millis: self.now_millis(),
        }
    }

    pub fn restore(&self, snapshot: JsClockSnapshot) {
        self.set_now_millis(snapshot.now_millis);
    }
}

impl Default for DeterministicJsClock {
    fn default() -> Self {
        Self::new(0)
    }
}

impl Clone for DeterministicJsClock {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl JsClock for DeterministicJsClock {
    fn now_millis(&self) -> u64 {
        *self.state.lock().expect("clock mutex poisoned")
    }
}

impl From<DeterministicJsClock> for Arc<dyn JsClock> {
    fn from(value: DeterministicJsClock) -> Self {
        Arc::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::{DeterministicJsClock, JsClock, JsClockSnapshot};

    #[test]
    fn deterministic_clock_advances_and_restores() {
        let clock = DeterministicJsClock::new(1_000);
        assert_eq!(clock.now_millis(), 1_000);

        let snapshot = clock.snapshot();
        assert_eq!(snapshot, JsClockSnapshot { now_millis: 1_000 });

        assert_eq!(clock.advance_millis(250), 1_250);
        assert_eq!(clock.now_millis(), 1_250);

        clock.restore(snapshot);
        assert_eq!(clock.now_millis(), 1_000);

        clock.set_now_millis(5_000);
        assert_eq!(clock.now_millis(), 5_000);
    }
}
