#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MinimizationReport<S> {
    pub original_operation_count: usize,
    pub minimized_operation_count: usize,
    pub minimized: S,
}

pub trait ScenarioOperations: Clone {
    type Operation: Clone;

    fn operations(&self) -> &[Self::Operation];
    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self;
}

pub fn minimize_by_removal<S, E, F>(
    scenario: S,
    mut still_fails: F,
) -> Result<MinimizationReport<S>, E>
where
    S: ScenarioOperations,
    F: FnMut(&S) -> Result<bool, E>,
{
    let original_operation_count = scenario.operations().len();
    let mut minimized = scenario;
    let mut index = 0;
    while index < minimized.operations().len() {
        let mut candidate_ops = minimized.operations().to_vec();
        candidate_ops.remove(index);
        let candidate = minimized.with_operations(candidate_ops);
        if still_fails(&candidate)? {
            minimized = candidate;
        } else {
            index += 1;
        }
    }

    Ok(MinimizationReport {
        original_operation_count,
        minimized_operation_count: minimized.operations().len(),
        minimized,
    })
}
