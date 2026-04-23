use std::time::Instant;

pub trait Clock {
    fn now(&self) -> Instant {
        panic!("Clock::now stub")
    }
}
