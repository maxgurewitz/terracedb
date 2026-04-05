#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use serde::de::DeserializeOwned;
use terracedb_fuzz::{
    GeneratedScenarioHarness, assert_seed_replays as fuzz_assert_seed_replays,
    assert_seed_variation as fuzz_assert_seed_variation, decode_json_artifact,
    replay_seed as fuzz_replay_seed, run_campaign as fuzz_run_campaign,
};
use thiserror::Error;

pub mod harness;

pub mod http {
    pub use terracedb_http::{
        HttpFailureStage, HttpTraceEvent, SimulatedHttpClient, SimulatedHttpServer,
        SimulatedHttpTrace, axum_router_server, axum_router_server_with_shutdown,
    };
}

pub mod simulation {
    pub use terracedb_simulation::{
        NetworkObjectStore, SeededSimulationRunner, SimulationContext, SimulationHost, TurmoilClock,
    };
}

pub use harness::{
    SimulationCaseContext, SimulationCaseReport, SimulationCaseSpec, SimulationCaseStatus,
    SimulationDomainConfig, SimulationHarness, SimulationHarnessConfig, SimulationHarnessError,
    SimulationSuiteDefinition, SimulationSuiteReport,
};

pub use terracedb_fuzz::{
    ArtifactError, MinimizationReport, ReplayBatch, ReplayCapture, ReplayPair, ScenarioOperations,
    SeedCampaign, assert_non_decreasing, decode_json_artifact as decode_json_scenario,
    encode_json_artifact as encode_json_scenario, load_json_artifact as load_json_scenario,
    minimize_by_removal, save_json_artifact as save_json_scenario,
};

pub trait SystemScenarioHarness {
    type Scenario: Clone + std::fmt::Debug + PartialEq + Eq;
    type Outcome: Clone + std::fmt::Debug + PartialEq + Eq;
    type Error;

    fn generate(&self, seed: u64) -> Self::Scenario;
    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error>;
}

#[derive(Debug, Error)]
pub enum ScenarioReplayError {
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
    #[error("system scenario failed: {0}")]
    Harness(String),
}

struct HarnessAdapter<'a, H>(&'a H);

impl<H> GeneratedScenarioHarness for HarnessAdapter<'_, H>
where
    H: SystemScenarioHarness,
{
    type Scenario = H::Scenario;
    type Outcome = H::Outcome;
    type Error = H::Error;

    fn generate(&self, seed: u64) -> Self::Scenario {
        self.0.generate(seed)
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        self.0.run(scenario)
    }
}

pub fn replay_seed<H>(
    harness: &H,
    seed: u64,
) -> Result<ReplayCapture<H::Scenario, H::Outcome>, H::Error>
where
    H: SystemScenarioHarness,
{
    fuzz_replay_seed(&HarnessAdapter(harness), seed)
}

pub fn assert_seed_replays<H>(
    harness: &H,
    seed: u64,
) -> Result<ReplayCapture<H::Scenario, H::Outcome>, H::Error>
where
    H: SystemScenarioHarness,
{
    fuzz_assert_seed_replays(&HarnessAdapter(harness), seed)
}

pub fn assert_seed_variation<H, F>(
    harness: &H,
    left_seed: u64,
    right_seed: u64,
    differs: F,
) -> Result<ReplayPair<H::Scenario, H::Outcome>, H::Error>
where
    H: SystemScenarioHarness,
    F: FnOnce(
        &ReplayCapture<H::Scenario, H::Outcome>,
        &ReplayCapture<H::Scenario, H::Outcome>,
    ) -> bool,
{
    fuzz_assert_seed_variation(&HarnessAdapter(harness), left_seed, right_seed, differs)
}

pub fn run_campaign<H>(
    harness: &H,
    campaign: &SeedCampaign,
) -> Result<ReplayBatch<H::Scenario, H::Outcome>, H::Error>
where
    H: SystemScenarioHarness,
{
    fuzz_run_campaign(&HarnessAdapter(harness), campaign)
}

pub fn replay_json_scenario<H>(
    harness: &H,
    json: &str,
) -> Result<ReplayCapture<H::Scenario, H::Outcome>, ScenarioReplayError>
where
    H: SystemScenarioHarness,
    H::Scenario: DeserializeOwned,
    H::Error: std::fmt::Display,
{
    let scenario: H::Scenario = decode_json_artifact(json)?;
    let outcome = harness
        .run(scenario.clone())
        .map_err(|error| ScenarioReplayError::Harness(error.to_string()))?;
    Ok(ReplayCapture {
        seed: 0,
        scenario,
        outcome,
    })
}

pub fn replay_json_scenario_bytes<H>(
    harness: &H,
    bytes: &[u8],
) -> Result<Option<ReplayCapture<H::Scenario, H::Outcome>>, H::Error>
where
    H: SystemScenarioHarness,
    H::Scenario: DeserializeOwned,
{
    let Ok(json) = std::str::from_utf8(bytes) else {
        return Ok(None);
    };
    let Ok(scenario) = decode_json_artifact::<H::Scenario>(json) else {
        return Ok(None);
    };
    let outcome = harness.run(scenario.clone())?;
    Ok(Some(ReplayCapture {
        seed: 0,
        scenario,
        outcome,
    }))
}
