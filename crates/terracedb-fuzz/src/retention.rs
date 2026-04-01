use crate::ScenarioOperations;

pub use terracedb_simulation::{
    CurrentStateSimulationError, CurrentStateSimulationOperation, CurrentStateSimulationOutcome,
    CurrentStateSimulationScenario, run_current_state_simulation,
};

impl ScenarioOperations for CurrentStateSimulationScenario {
    type Operation = CurrentStateSimulationOperation;

    fn operations(&self) -> &[Self::Operation] {
        &self.operations
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        let mut next = self.clone();
        next.operations = operations;
        next
    }
}
