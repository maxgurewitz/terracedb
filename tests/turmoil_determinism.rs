use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use terracedb::{Clock, DeterministicRng, Rng as TerraceRng, Timestamp};
use terracedb_simulation::{SeededSimulationRunner, SimulationScenarioConfig, TurmoilClock};

const EXPECTED_SEEDED_BYTES: [u8; 16] = [
    145, 129, 172, 165, 143, 210, 43, 215, 45, 148, 110, 122, 148, 103, 179, 205,
];

#[test]
fn turmoil_and_seeded_helpers_produce_reproducible_effect_traces() -> turmoil::Result {
    let seed = 0x5eed_u64;
    let mut actual_bytes = [0_u8; 16];
    turmoil_determinism::rand::fill_seeded_bytes(seed, &mut actual_bytes);
    assert_eq!(actual_bytes, EXPECTED_SEEDED_BYTES);

    let trace = Arc::new(Mutex::new(Vec::new()));
    let trace_client = trace.clone();

    let mut sim = turmoil_determinism::Builder::new(seed).build();
    sim.client("determinism", async move {
        let wall_clock = turmoil_determinism::time::sim_elapsed_or_zero().as_millis() as u64;
        assert_eq!(wall_clock, 0);

        let clock = TurmoilClock;
        clock.sleep(Duration::from_millis(7)).await;
        let woke_at = clock.now();
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

#[test]
fn seeded_simulation_runner_is_parallel_safe_across_threads() {
    let seeds = [0x1111_u64, 0x2222, 0x3333, 0x4444];
    let config = SimulationScenarioConfig {
        steps: 8,
        path_count: 3,
        key_count: 3,
        max_payload_len: 8,
        max_clock_advance_millis: 4,
    };

    let expected = seeds
        .into_iter()
        .map(|seed| {
            let outcome = SeededSimulationRunner::new(seed)
                .with_scenario_config(config.clone())
                .run_generated()
                .expect("sequential seeded run");
            (seed, outcome)
        })
        .collect::<Vec<_>>();

    let handles = seeds.into_iter().map(|seed| {
        let config = config.clone();
        thread::spawn(move || {
            SeededSimulationRunner::new(seed)
                .with_scenario_config(config)
                .run_generated()
                .map_err(|error| error.to_string())
        })
    });

    let actual = handles
        .map(|handle| {
            let outcome = handle.join().expect("simulation thread should not panic");
            let outcome = outcome.expect("parallel seeded run");
            (outcome.seed, outcome)
        })
        .collect::<BTreeMap<_, _>>();

    for (seed, expected_outcome) in expected {
        let actual_outcome = actual.get(&seed).cloned().expect("matching seeded outcome");
        assert_eq!(actual_outcome, expected_outcome);
    }
}
