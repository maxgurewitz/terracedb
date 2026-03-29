use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn host_elapsed() -> Option<Duration> {
    if turmoil::in_simulation() {
        Some(turmoil::elapsed())
    } else {
        None
    }
}

pub fn host_elapsed_or_zero() -> Duration {
    host_elapsed().unwrap_or(Duration::ZERO)
}

pub fn sim_elapsed() -> Option<Duration> {
    turmoil::sim_elapsed()
}

pub fn sim_elapsed_or_zero() -> Duration {
    sim_elapsed().unwrap_or(Duration::ZERO)
}

pub fn logical_time() -> Option<SystemTime> {
    turmoil::since_epoch().map(|elapsed| UNIX_EPOCH + elapsed)
}

pub fn logical_time_or_epoch() -> SystemTime {
    logical_time().unwrap_or(UNIX_EPOCH)
}
