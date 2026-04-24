use std::time::Instant;

use super::CompletionTarget;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TimerId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TimerCompletion {
    pub timer_id: TimerId,
    pub target: CompletionTarget,
}

pub trait Timers {
    fn sleep_until(&mut self, _at: Instant, _target: CompletionTarget) -> TimerId {
        panic!("Timers::sleep_until stub")
    }

    fn cancel_timer(&mut self, _timer: TimerId) -> bool {
        panic!("Timers::cancel_timer stub")
    }
}
