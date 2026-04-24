use std::time::Instant;

pub trait Clock {
    fn now(&self) -> Instant {
        panic!("Clock::now stub")
    }

    fn unix_timestamp(&self) -> u64 {
        panic!("Clock::unix_timestamp stub")
    }
}
