use crate::{GeneratedScenarioHarness, ScenarioOperations};

pub use terracedb_simulation::{
    DbGeneratedScenario, DbSimulationOutcome, DbSimulationScenarioConfig, DbWorkloadOperation,
    GeneratedScenario, SeededSimulationRunner, SimulationOutcome, SimulationScenarioConfig,
    WorkloadOperation,
};

#[derive(Clone, Debug)]
pub struct DbScenarioHarness {
    config: DbSimulationScenarioConfig,
}

impl DbScenarioHarness {
    pub fn new(config: DbSimulationScenarioConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &DbSimulationScenarioConfig {
        &self.config
    }
}

impl GeneratedScenarioHarness for DbScenarioHarness {
    type Scenario = DbGeneratedScenario;
    type Outcome = DbSimulationOutcome;
    type Error = Box<dyn std::error::Error>;

    fn generate(&self, seed: u64) -> Self::Scenario {
        SeededSimulationRunner::new(seed).generate_db_scenario(&self.config)
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        SeededSimulationRunner::new(scenario.seed).run_db_scenario(scenario)
    }
}

impl ScenarioOperations for GeneratedScenario {
    type Operation = WorkloadOperation;

    fn operations(&self) -> &[Self::Operation] {
        &self.workload
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        let mut next = self.clone();
        next.workload = operations;
        next
    }
}

impl ScenarioOperations for DbGeneratedScenario {
    type Operation = DbWorkloadOperation;

    fn operations(&self) -> &[Self::Operation] {
        &self.workload
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        let mut next = self.clone();
        next.workload = operations;
        next
    }
}
