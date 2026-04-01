use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use terracedb::{
    DeterministicRng, Rng, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp,
};
use terracedb_example_todo_api::{
    CreateTodoRequest, PlannerSchedule, TodoApp, TodoAppOptions, TodoAppState, TodoRecord,
    UpdateTodoRequest, todo_db_builder,
};
use terracedb_systemtest::{
    ScenarioOperations, SystemScenarioHarness, assert_seed_replays, assert_seed_variation,
    decode_json_scenario, encode_json_scenario, replay_json_scenario, replay_json_scenario_bytes,
};

type BoxError = Box<dyn std::error::Error>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum TodoFuzzOperation {
    Create {
        todo_id: String,
        title: String,
        notes: String,
        scheduled_for_day: Option<u64>,
    },
    Update {
        todo_id: String,
        title: Option<String>,
        notes: Option<String>,
        scheduled_for_day: Option<u64>,
    },
    Complete {
        todo_id: String,
    },
    AdvanceClock {
        millis: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TodoFuzzScenario {
    seed: u64,
    operations: Vec<TodoFuzzOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TodoFuzzOutcome {
    todos: Vec<TodoRecord>,
    recent: Vec<TodoRecord>,
}

struct TodoFuzzHarness;

impl SystemScenarioHarness for TodoFuzzHarness {
    type Scenario = TodoFuzzScenario;
    type Outcome = TodoFuzzOutcome;
    type Error = BoxError;

    fn generate(&self, seed: u64) -> Self::Scenario {
        let rng = DeterministicRng::seeded(seed ^ 0x70d0);
        let mut operations = vec![TodoFuzzOperation::Create {
            todo_id: "todo-0".to_string(),
            title: format!("bootstrap-{seed:x}"),
            notes: "bootstrap".to_string(),
            scheduled_for_day: Some(1_000),
        }];
        for step in 1..20 {
            let todo_id = format!("todo-{}", (rng.next_u64() as usize) % 6);
            let op = match rng.next_u64() % 4 {
                0 => TodoFuzzOperation::Create {
                    todo_id,
                    title: format!("title-{seed:x}-{step}"),
                    notes: format!("notes-{step}"),
                    scheduled_for_day: (rng.next_u64().is_multiple_of(2))
                        .then_some(((rng.next_u64() % 5) + 1) * 1_000),
                },
                1 => TodoFuzzOperation::Update {
                    todo_id,
                    title: Some(format!("retitle-{seed:x}-{step}")),
                    notes: Some(format!("renotes-{step}")),
                    scheduled_for_day: Some(((rng.next_u64() % 7) + 1) * 1_000),
                },
                2 => TodoFuzzOperation::Complete { todo_id },
                _ => TodoFuzzOperation::AdvanceClock {
                    millis: (rng.next_u64() % 4) + 1,
                },
            };
            operations.push(op);
        }

        TodoFuzzScenario { seed, operations }
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        Ok(run_todo_fuzz_scenario(scenario)?)
    }
}

impl ScenarioOperations for TodoFuzzScenario {
    type Operation = TodoFuzzOperation;

    fn operations(&self) -> &[Self::Operation] {
        &self.operations
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        let mut next = self.clone();
        next.operations = operations;
        next
    }
}

fn run_todo_fuzz_scenario(scenario: TodoFuzzScenario) -> Result<TodoFuzzOutcome, BoxError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let clock = Arc::new(StubClock::new(Timestamp::new(0)));
        let db = todo_db_builder(
            &format!("/todo-api-fuzz-{}", scenario.seed),
            &format!("todo-api-fuzz-{}", scenario.seed),
        )
        .file_system(Arc::new(StubFileSystem::default()))
        .object_store(Arc::new(StubObjectStore::default()))
        .clock(clock.clone())
        .rng(Arc::new(StubRng::seeded(scenario.seed)))
        .open()
        .await?;
        let app = TodoApp::open_with_options(
            db,
            clock.clone(),
            TodoAppOptions {
                planner_schedule: PlannerSchedule::new(1_000_000, 1),
                planner_initial_fire_at_ms: None,
            },
        )
        .await?;
        let state = app.state().clone();
        let mut model = BTreeMap::<String, TodoRecord>::new();
        let mut now_ms = 0_u64;

        for operation in &scenario.operations {
            match operation {
                TodoFuzzOperation::Create {
                    todo_id,
                    title,
                    notes,
                    scheduled_for_day,
                } => {
                    let result = state
                        .create_todo(CreateTodoRequest {
                            todo_id: todo_id.clone(),
                            title: title.clone(),
                            notes: notes.clone(),
                            scheduled_for_day: *scheduled_for_day,
                        })
                        .await;
                    if model.contains_key(todo_id) {
                        assert!(result.is_err(), "duplicate todo ids should be rejected");
                    } else {
                        let created = result.expect("fresh todo should be created");
                        model.insert(todo_id.clone(), created);
                    }
                }
                TodoFuzzOperation::Update {
                    todo_id,
                    title,
                    notes,
                    scheduled_for_day,
                } => {
                    let updated = state
                        .update_todo(
                            todo_id,
                            UpdateTodoRequest {
                                title: title.clone(),
                                notes: notes.clone(),
                                scheduled_for_day: *scheduled_for_day,
                            },
                        )
                        .await
                        .expect("update should surface typed todo-api errors");
                    match updated {
                        Some(todo) => {
                            model.insert(todo_id.clone(), todo);
                        }
                        None => {
                            assert!(
                                !model.contains_key(todo_id),
                                "missing updates should only happen for absent todos"
                            );
                        }
                    }
                }
                TodoFuzzOperation::Complete { todo_id } => {
                    let completed = state
                        .complete_todo(todo_id)
                        .await
                        .expect("complete should surface typed todo-api errors");
                    match completed {
                        Some(todo) => {
                            model.insert(todo_id.clone(), todo);
                        }
                        None => {
                            assert!(
                                !model.contains_key(todo_id),
                                "missing completions should only happen for absent todos"
                            );
                        }
                    }
                }
                TodoFuzzOperation::AdvanceClock { millis } => {
                    now_ms = now_ms.saturating_add(*millis);
                    clock.set(Timestamp::new(now_ms));
                }
            }

            tokio::task::yield_now().await;
            assert_todo_invariants(&state, &model).await?;
        }

        let todos = state.list_todos().await?;
        let recent = state.list_recent_todos().await?;
        app.shutdown().await?;
        Ok(TodoFuzzOutcome { todos, recent })
    })
}

async fn assert_todo_invariants(
    state: &TodoAppState,
    model: &BTreeMap<String, TodoRecord>,
) -> Result<(), BoxError> {
    let mut expected_todos = model.values().cloned().collect::<Vec<_>>();
    sort_list_todos(&mut expected_todos);
    let actual_todos = state.list_todos().await?;
    assert_eq!(actual_todos, expected_todos);

    for todo in &actual_todos {
        assert_eq!(state.get_todo(&todo.todo_id).await?, Some(todo.clone()));
    }

    let mut expected_recent = model.values().cloned().collect::<Vec<_>>();
    sort_recent_todos(&mut expected_recent);
    expected_recent.truncate(10);

    let actual_recent = state.list_recent_todos().await?;
    assert_eq!(actual_recent, expected_recent);
    assert!(
        actual_recent
            .iter()
            .all(|todo| model.contains_key(&todo.todo_id))
    );
    Ok(())
}

fn sort_recent_todos(todos: &mut [TodoRecord]) {
    todos.sort_by(|left, right| {
        right
            .updated_at_ms
            .cmp(&left.updated_at_ms)
            .then_with(|| right.created_at_ms.cmp(&left.created_at_ms))
            .then_with(|| left.todo_id.cmp(&right.todo_id))
    });
}

fn sort_list_todos(todos: &mut [TodoRecord]) {
    todos.sort_by(|left, right| {
        left.scheduled_for_day
            .unwrap_or(u64::MAX)
            .cmp(&right.scheduled_for_day.unwrap_or(u64::MAX))
            .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
            .then_with(|| left.todo_id.cmp(&right.todo_id))
    });
}

#[test]
fn todo_api_generated_fuzz_replays_same_seed() -> turmoil::Result {
    let replay = assert_seed_replays(&TodoFuzzHarness, 0x70d1)?;
    let encoded = encode_json_scenario(&replay.scenario).expect("encode todo fuzz scenario");
    let decoded: TodoFuzzScenario =
        decode_json_scenario(&encoded).expect("decode todo fuzz scenario");

    assert_eq!(decoded, replay.scenario);
    assert_eq!(
        replay.outcome.recent.len(),
        replay.outcome.todos.len().min(10)
    );
    Ok(())
}

#[test]
fn todo_api_generated_fuzz_varies_across_seeds() -> turmoil::Result {
    let _ = assert_seed_variation(&TodoFuzzHarness, 0x70d1, 0x70d2, |left, right| {
        left.scenario != right.scenario || left.outcome != right.outcome
    })?;
    Ok(())
}

#[test]
fn todo_api_generated_fuzz_replays_serialized_scenarios_through_systemtest() -> turmoil::Result {
    let replay = assert_seed_replays(&TodoFuzzHarness, 0x70d3)?;
    let encoded = encode_json_scenario(&replay.scenario).expect("encode todo fuzz scenario");

    let replayed = replay_json_scenario(&TodoFuzzHarness, &encoded)
        .expect("replay serialized scenario through systemtest");
    let replayed_from_bytes = replay_json_scenario_bytes(&TodoFuzzHarness, encoded.as_bytes())?
        .expect("valid serialized scenario should decode from bytes");

    assert_eq!(replayed.scenario, replay.scenario);
    assert_eq!(replayed.outcome, replay.outcome);
    assert_eq!(replayed_from_bytes.outcome, replay.outcome);
    Ok(())
}
