use std::fmt::Debug;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SeedCampaign {
    seeds: Vec<u64>,
}

impl SeedCampaign {
    pub fn new(seeds: impl IntoIterator<Item = u64>) -> Self {
        Self {
            seeds: seeds.into_iter().collect(),
        }
    }

    pub fn smoke() -> Self {
        Self::new([0x5101, 0x5102, 0x5103])
    }

    pub fn seeds(&self) -> &[u64] {
        &self.seeds
    }

    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.seeds.iter().copied()
    }
}

pub trait GeneratedScenarioHarness {
    type Scenario: Clone + Debug + PartialEq + Eq;
    type Outcome: Clone + Debug + PartialEq + Eq;
    type Error;

    fn generate(&self, seed: u64) -> Self::Scenario;
    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayCapture<S, O> {
    pub seed: u64,
    pub scenario: S,
    pub outcome: O,
}

pub type ReplayPair<S, O> = (ReplayCapture<S, O>, ReplayCapture<S, O>);
pub type ReplayBatch<S, O> = Vec<ReplayCapture<S, O>>;

pub fn replay_seed<H>(
    harness: &H,
    seed: u64,
) -> Result<ReplayCapture<H::Scenario, H::Outcome>, H::Error>
where
    H: GeneratedScenarioHarness,
{
    let scenario = harness.generate(seed);
    let outcome = harness.run(scenario.clone())?;
    Ok(ReplayCapture {
        seed,
        scenario,
        outcome,
    })
}

pub fn assert_seed_replays<H>(
    harness: &H,
    seed: u64,
) -> Result<ReplayCapture<H::Scenario, H::Outcome>, H::Error>
where
    H: GeneratedScenarioHarness,
{
    let first = replay_seed(harness, seed)?;
    let second = replay_seed(harness, seed)?;
    assert_eq!(first, second, "seed {seed:#x} should replay identically");
    Ok(first)
}

pub fn assert_seed_variation<H, F>(
    harness: &H,
    left_seed: u64,
    right_seed: u64,
    differs: F,
) -> Result<ReplayPair<H::Scenario, H::Outcome>, H::Error>
where
    H: GeneratedScenarioHarness,
    F: FnOnce(
        &ReplayCapture<H::Scenario, H::Outcome>,
        &ReplayCapture<H::Scenario, H::Outcome>,
    ) -> bool,
{
    let left = replay_seed(harness, left_seed)?;
    let right = replay_seed(harness, right_seed)?;
    assert!(
        differs(&left, &right),
        "distinct seeds {left_seed:#x} and {right_seed:#x} should exercise different shapes"
    );
    Ok((left, right))
}

pub fn run_campaign<H>(
    harness: &H,
    campaign: &SeedCampaign,
) -> Result<ReplayBatch<H::Scenario, H::Outcome>, H::Error>
where
    H: GeneratedScenarioHarness,
{
    campaign
        .iter()
        .map(|seed| replay_seed(harness, seed))
        .collect()
}
