use terracedb_fuzz::{
    ScenarioOperations, decode_json_artifact, encode_json_artifact, minimize_by_removal,
};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ExampleScenario {
    operations: Vec<u8>,
}

impl ScenarioOperations for ExampleScenario {
    type Operation = u8;

    fn operations(&self) -> &[Self::Operation] {
        &self.operations
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        Self { operations }
    }
}

#[test]
fn public_artifact_helpers_round_trip_json() {
    let scenario = ExampleScenario {
        operations: vec![1, 2, 3],
    };
    let encoded = encode_json_artifact(&scenario).expect("encode public artifact");
    let decoded: ExampleScenario = decode_json_artifact(&encoded).expect("decode public artifact");
    assert_eq!(decoded, scenario);
}

#[test]
fn public_minimizer_removes_nonessential_operations() {
    let scenario = ExampleScenario {
        operations: vec![1, 9, 2, 3],
    };
    let report = minimize_by_removal(scenario, |candidate| {
        Ok::<_, std::convert::Infallible>(candidate.operations.contains(&9))
    })
    .expect("minimize scenario");

    assert_eq!(report.original_operation_count, 4);
    assert_eq!(report.minimized.operations, vec![9]);
}
