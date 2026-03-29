use std::{
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rand::{SeedableRng, rngs::StdRng};
use terracedb::{Clock, DeterministicRng, Rng as TerraceRng, SimulatedClock, Timestamp};

const EXPECTED_MAD_TURMOIL_BYTES: [u8; 16] = [
    68, 7, 230, 209, 134, 195, 227, 157, 91, 18, 108, 194, 221, 180, 171, 1,
];

#[test]
fn turmoil_and_mad_turmoil_produce_reproducible_effect_traces() -> turmoil::Result {
    let seed = 0x5eed_u64;
    let _clock_guard = mad_turmoil::time::SimClocksGuard::init();

    mad_turmoil::rand::set_rng(StdRng::seed_from_u64(seed));

    let trace = Arc::new(Mutex::new(Vec::new()));
    let trace_client = trace.clone();

    let mut sim = turmoil::Builder::new().build();
    sim.client("determinism", async move {
        let wall_clock = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_millis() as u64;
        assert_eq!(wall_clock, 0);

        let mut actual_bytes = [0_u8; 16];
        getrandom::fill(&mut actual_bytes).expect("mad-turmoil random bytes");
        assert_eq!(actual_bytes, EXPECTED_MAD_TURMOIL_BYTES);

        let clock = Arc::new(SimulatedClock::default());
        let waiter = {
            let clock = clock.clone();
            tokio::spawn(async move {
                clock.sleep(Duration::from_millis(7)).await;
                clock.now()
            })
        };

        tokio::task::yield_now().await;
        clock.advance(Duration::from_millis(7));
        let woke_at = waiter.await.expect("waiter join");
        assert_eq!(woke_at, Timestamp::new(7));

        let rng = DeterministicRng::seeded(seed);
        let mut trace = trace_client.lock().expect("trace mutex");
        trace.push(wall_clock);
        trace.push(woke_at.get());
        trace.push(rng.next_u64());
        trace.push(rng.next_u64());
        Ok(())
    });

    sim.run()?;

    let trace = trace.lock().expect("trace mutex");
    let expected_rng = DeterministicRng::seeded(seed);
    let expected_trace = vec![0, 7, expected_rng.next_u64(), expected_rng.next_u64()];
    assert_eq!(trace.as_slice(), expected_trace.as_slice());

    Ok(())
}
