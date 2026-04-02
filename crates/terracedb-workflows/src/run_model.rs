use async_trait::async_trait;
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use terracedb_workflows_core::{
    AppendOnlyWorkflowHistoryOrderer, PassthroughWorkflowVisibilityProjector,
    WorkflowDescribeRequest, WorkflowDescribeResponse, WorkflowExecutionTarget,
    WorkflowHistoryEvent, WorkflowHistoryOrderer, WorkflowLifecycleRecord,
    WorkflowLifecycleState, WorkflowRegistrationId, WorkflowRunId, WorkflowStateRecord,
    WorkflowTaskError, WorkflowTaskId, WorkflowTransitionInput, WorkflowTransitionOutput,
    WorkflowVisibilityApi, WorkflowVisibilityEntry, WorkflowVisibilityListRequest,
    WorkflowVisibilityListResponse, WorkflowVisibilityProjector, WorkflowVisibilityRecord,
    WorkflowVisibleHistoryEntry, WorkflowVisibleHistoryPageRequest,
    WorkflowVisibleHistoryPageResponse,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRunRecord {
    pub run_id: WorkflowRunId,
    pub target: WorkflowExecutionTarget,
    pub workflow_name: String,
    pub instance_id: String,
    pub created_at_millis: u64,
    pub continued_from_run_id: Option<WorkflowRunId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowHistoryRecord {
    pub run_id: WorkflowRunId,
    pub sequence: u64,
    pub event: WorkflowHistoryEvent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowReductionPlan {
    pub active_state: WorkflowStateRecord,
    pub run_records: Vec<WorkflowRunRecord>,
    pub lifecycle_records: Vec<WorkflowLifecycleRecord>,
    pub visibility_records: Vec<WorkflowVisibilityRecord>,
    pub history: Vec<WorkflowHistoryRecord>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WorkflowTransitionReducer {
    history_orderer: AppendOnlyWorkflowHistoryOrderer,
    visibility_projector: PassthroughWorkflowVisibilityProjector,
}

impl WorkflowTransitionReducer {
    pub fn reduce(
        &self,
        current_state: Option<&WorkflowStateRecord>,
        input: &WorkflowTransitionInput,
        output: &WorkflowTransitionOutput,
        committed_at_millis: u64,
    ) -> Result<WorkflowReductionPlan, WorkflowTaskError> {
        validate_reduction_input(current_state, input, output)?;

        let mut history = Vec::new();
        let mut run_records = Vec::new();
        let mut lifecycle_records = Vec::new();
        let mut visibility_records = Vec::new();

        let mut current_history_len = current_state.map(|state| state.history_len).unwrap_or(0);

        let is_new_run = current_state.is_none();
        if is_new_run {
            let run_record = WorkflowRunRecord {
                run_id: input.run_id.clone(),
                target: input.target.clone(),
                workflow_name: input.workflow_name.clone(),
                instance_id: input.instance_id.clone(),
                created_at_millis: input.admitted_at_millis,
                continued_from_run_id: None,
            };
            run_records.push(run_record);

            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut current_history_len,
                input.run_id.clone(),
                WorkflowHistoryEvent::RunCreated {
                    run_id: input.run_id.clone(),
                    target: input.target.clone(),
                    instance_id: input.instance_id.clone(),
                    scheduled_at_millis: input.admitted_at_millis,
                },
            );

            let scheduled_record = WorkflowLifecycleRecord {
                run_id: input.run_id.clone(),
                lifecycle: WorkflowLifecycleState::Scheduled,
                updated_at_millis: input.admitted_at_millis,
                reason: Some("run-created".to_string()),
                task_id: None,
                attempt: 0,
            };
            lifecycle_records.push(scheduled_record.clone());
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut current_history_len,
                input.run_id.clone(),
                WorkflowHistoryEvent::LifecycleChanged {
                    record: scheduled_record,
                },
            );
        }

        push_history_record(
            &self.history_orderer,
            &mut history,
            &mut current_history_len,
            input.run_id.clone(),
            WorkflowHistoryEvent::TaskAdmitted {
                task_id: input.task_id.clone(),
                trigger: input.trigger.clone(),
                admitted_at_millis: input.admitted_at_millis,
            },
        );
        push_history_record(
            &self.history_orderer,
            &mut history,
            &mut current_history_len,
            input.run_id.clone(),
            WorkflowHistoryEvent::TaskApplied {
                task_id: input.task_id.clone(),
                output: output.clone(),
                committed_at_millis,
            },
        );

        let previous_lifecycle = current_state
            .map(|state| state.lifecycle)
            .unwrap_or(WorkflowLifecycleState::Scheduled);
        let current_lifecycle = output
            .lifecycle
            .unwrap_or_else(|| default_lifecycle_after_apply(current_state, previous_lifecycle));

        if current_lifecycle != previous_lifecycle {
            let lifecycle_record = WorkflowLifecycleRecord {
                run_id: input.run_id.clone(),
                lifecycle: current_lifecycle,
                updated_at_millis: committed_at_millis,
                reason: Some("task-applied".to_string()),
                task_id: Some(input.task_id.clone()),
                attempt: input.attempt,
            };
            lifecycle_records.push(lifecycle_record.clone());
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut current_history_len,
                input.run_id.clone(),
                WorkflowHistoryEvent::LifecycleChanged {
                    record: lifecycle_record,
                },
            );
        }

        let current_payload = match &output.state {
            terracedb_workflows_core::WorkflowStateMutation::Unchanged => {
                current_state.and_then(|state| state.state.clone())
            }
            terracedb_workflows_core::WorkflowStateMutation::Put { state } => Some(state.clone()),
            terracedb_workflows_core::WorkflowStateMutation::Delete => None,
        };

        let current_run_terminal_state =
            if let Some(continue_as_new) = output.continue_as_new.as_ref() {
                if current_lifecycle != WorkflowLifecycleState::Completed {
                    let completed_record = WorkflowLifecycleRecord {
                        run_id: input.run_id.clone(),
                        lifecycle: WorkflowLifecycleState::Completed,
                        updated_at_millis: committed_at_millis,
                        reason: Some("continue-as-new".to_string()),
                        task_id: Some(input.task_id.clone()),
                        attempt: input.attempt,
                    };
                    lifecycle_records.push(completed_record.clone());
                    push_history_record(
                        &self.history_orderer,
                        &mut history,
                        &mut current_history_len,
                        input.run_id.clone(),
                        WorkflowHistoryEvent::LifecycleChanged {
                            record: completed_record,
                        },
                    );
                }

                push_history_record(
                    &self.history_orderer,
                    &mut history,
                    &mut current_history_len,
                    input.run_id.clone(),
                    WorkflowHistoryEvent::ContinuedAsNew {
                        from_run_id: input.run_id.clone(),
                        next_run_id: continue_as_new.next_run_id.clone(),
                        next_target: continue_as_new.next_target.clone(),
                        continued_at_millis: committed_at_millis,
                    },
                );
                push_history_record(
                    &self.history_orderer,
                    &mut history,
                    &mut current_history_len,
                    input.run_id.clone(),
                    WorkflowHistoryEvent::RunCompleted {
                        run_id: input.run_id.clone(),
                        completed_at_millis: committed_at_millis,
                    },
                );
                Some(WorkflowStateRecord {
                    run_id: input.run_id.clone(),
                    target: input.target.clone(),
                    workflow_name: input.workflow_name.clone(),
                    instance_id: input.instance_id.clone(),
                    lifecycle: WorkflowLifecycleState::Completed,
                    current_task_id: Some(input.task_id.clone()),
                    history_len: current_history_len.saturating_add(1),
                    state: current_payload.clone(),
                    updated_at_millis: committed_at_millis,
                })
            } else {
                if current_lifecycle == WorkflowLifecycleState::Completed {
                    push_history_record(
                        &self.history_orderer,
                        &mut history,
                        &mut current_history_len,
                        input.run_id.clone(),
                        WorkflowHistoryEvent::RunCompleted {
                            run_id: input.run_id.clone(),
                            completed_at_millis: committed_at_millis,
                        },
                    );
                } else if current_lifecycle == WorkflowLifecycleState::Failed {
                    push_history_record(
                        &self.history_orderer,
                        &mut history,
                        &mut current_history_len,
                        input.run_id.clone(),
                        WorkflowHistoryEvent::RunFailed {
                            run_id: input.run_id.clone(),
                            failed_at_millis: committed_at_millis,
                            error_code: "workflow-failed".to_string(),
                            error_message: "workflow transitioned to failed".to_string(),
                        },
                    );
                }

                Some(WorkflowStateRecord {
                    run_id: input.run_id.clone(),
                    target: input.target.clone(),
                    workflow_name: input.workflow_name.clone(),
                    instance_id: input.instance_id.clone(),
                    lifecycle: current_lifecycle,
                    current_task_id: Some(input.task_id.clone()),
                    history_len: current_history_len.saturating_add(1),
                    state: current_payload,
                    updated_at_millis: committed_at_millis,
                })
            };

        if let Some(current_terminal_state) = current_run_terminal_state.as_ref() {
            let visibility = self
                .visibility_projector
                .project_visibility(current_terminal_state, output);
            visibility_records.push(visibility.clone());
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut current_history_len,
                input.run_id.clone(),
                WorkflowHistoryEvent::VisibilityUpdated { record: visibility },
            );
        }

        let active_state = if let Some(continue_as_new) = output.continue_as_new.as_ref() {
            let next_run_record = WorkflowRunRecord {
                run_id: continue_as_new.next_run_id.clone(),
                target: continue_as_new.next_target.clone(),
                workflow_name: input.workflow_name.clone(),
                instance_id: input.instance_id.clone(),
                created_at_millis: committed_at_millis,
                continued_from_run_id: Some(input.run_id.clone()),
            };
            run_records.push(next_run_record);

            let mut next_history_len = 0;
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut next_history_len,
                continue_as_new.next_run_id.clone(),
                WorkflowHistoryEvent::RunCreated {
                    run_id: continue_as_new.next_run_id.clone(),
                    target: continue_as_new.next_target.clone(),
                    instance_id: input.instance_id.clone(),
                    scheduled_at_millis: committed_at_millis,
                },
            );

            let scheduled_record = WorkflowLifecycleRecord {
                run_id: continue_as_new.next_run_id.clone(),
                lifecycle: WorkflowLifecycleState::Scheduled,
                updated_at_millis: committed_at_millis,
                reason: Some("continue-as-new".to_string()),
                task_id: None,
                attempt: 0,
            };
            lifecycle_records.push(scheduled_record.clone());
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut next_history_len,
                continue_as_new.next_run_id.clone(),
                WorkflowHistoryEvent::LifecycleChanged {
                    record: scheduled_record,
                },
            );

            let next_state = WorkflowStateRecord {
                run_id: continue_as_new.next_run_id.clone(),
                target: continue_as_new.next_target.clone(),
                workflow_name: input.workflow_name.clone(),
                instance_id: input.instance_id.clone(),
                lifecycle: WorkflowLifecycleState::Scheduled,
                current_task_id: None,
                history_len: next_history_len.saturating_add(1),
                state: continue_as_new.state.clone(),
                updated_at_millis: committed_at_millis,
            };
            let visibility = self
                .visibility_projector
                .project_visibility(&next_state, &WorkflowTransitionOutput::default());
            visibility_records.push(visibility.clone());
            push_history_record(
                &self.history_orderer,
                &mut history,
                &mut next_history_len,
                continue_as_new.next_run_id.clone(),
                WorkflowHistoryEvent::VisibilityUpdated { record: visibility },
            );
            next_state
        } else {
            current_run_terminal_state.expect("current run state should always exist")
        };

        Ok(WorkflowReductionPlan {
            active_state,
            run_records,
            lifecycle_records,
            visibility_records,
            history,
        })
    }
}

fn push_history_record(
    history_orderer: &AppendOnlyWorkflowHistoryOrderer,
    history: &mut Vec<WorkflowHistoryRecord>,
    history_len: &mut u64,
    run_id: WorkflowRunId,
    event: WorkflowHistoryEvent,
) {
    *history_len = history_orderer.next_history_sequence(*history_len);
    history.push(WorkflowHistoryRecord {
        run_id,
        sequence: *history_len,
        event,
    });
}

#[derive(Clone, Debug)]
pub struct WorkflowRunBuilder {
    workflow_name: String,
    instance_id: String,
    run_id: Option<WorkflowRunId>,
    target: Option<WorkflowExecutionTarget>,
    lifecycle: WorkflowLifecycleState,
    state: Option<terracedb_workflows_core::WorkflowPayload>,
    updated_at_millis: u64,
    history_len: u64,
    current_task_id: Option<WorkflowTaskId>,
    continued_from_run_id: Option<WorkflowRunId>,
}

impl WorkflowRunBuilder {
    pub fn new(workflow_name: impl Into<String>, instance_id: impl Into<String>) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            instance_id: instance_id.into(),
            run_id: None,
            target: None,
            lifecycle: WorkflowLifecycleState::Scheduled,
            state: None,
            updated_at_millis: 0,
            history_len: 0,
            current_task_id: None,
            continued_from_run_id: None,
        }
    }

    pub fn with_run_id(mut self, run_id: WorkflowRunId) -> Self {
        self.run_id = Some(run_id);
        self
    }

    pub fn with_target(mut self, target: WorkflowExecutionTarget) -> Self {
        self.target = Some(target);
        self
    }

    pub fn with_lifecycle(mut self, lifecycle: WorkflowLifecycleState) -> Self {
        self.lifecycle = lifecycle;
        self
    }

    pub fn with_state(mut self, state: Option<terracedb_workflows_core::WorkflowPayload>) -> Self {
        self.state = state;
        self
    }

    pub fn with_updated_at_millis(mut self, updated_at_millis: u64) -> Self {
        self.updated_at_millis = updated_at_millis;
        self
    }

    pub fn with_history_len(mut self, history_len: u64) -> Self {
        self.history_len = history_len;
        self
    }

    pub fn with_current_task_id(mut self, current_task_id: Option<WorkflowTaskId>) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn with_continued_from_run_id(
        mut self,
        continued_from_run_id: Option<WorkflowRunId>,
    ) -> Self {
        self.continued_from_run_id = continued_from_run_id;
        self
    }

    pub fn build_run_record(&self) -> WorkflowRunRecord {
        WorkflowRunRecord {
            run_id: self.run_id(),
            target: self.target(),
            workflow_name: self.workflow_name.clone(),
            instance_id: self.instance_id.clone(),
            created_at_millis: self.updated_at_millis,
            continued_from_run_id: self.continued_from_run_id.clone(),
        }
    }

    pub fn build_state_record(&self) -> WorkflowStateRecord {
        WorkflowStateRecord {
            run_id: self.run_id(),
            target: self.target(),
            workflow_name: self.workflow_name.clone(),
            instance_id: self.instance_id.clone(),
            lifecycle: self.lifecycle,
            current_task_id: self.current_task_id.clone(),
            history_len: self.history_len,
            state: self.state.clone(),
            updated_at_millis: self.updated_at_millis,
        }
    }

    pub fn build_store(&self) -> InMemoryWorkflowRunStore {
        let state = self.build_state_record();
        let visibility = PassthroughWorkflowVisibilityProjector
            .project_visibility(&state, &WorkflowTransitionOutput::default());
        InMemoryWorkflowRunStore {
            runs: BTreeMap::from([(state.run_id.clone(), self.build_run_record())]),
            active_state_by_instance: BTreeMap::from([(state.instance_id.clone(), state.clone())]),
            lifecycle_by_run: BTreeMap::from([(
                state.run_id.clone(),
                WorkflowLifecycleRecord {
                    run_id: state.run_id.clone(),
                    lifecycle: state.lifecycle,
                    updated_at_millis: state.updated_at_millis,
                    reason: Some("builder-seed".to_string()),
                    task_id: state.current_task_id.clone(),
                    attempt: 0,
                },
            )]),
            visibility_by_run: BTreeMap::from([(state.run_id.clone(), visibility)]),
            history_by_run: BTreeMap::new(),
            reducer: WorkflowTransitionReducer::default(),
        }
    }

    fn run_id(&self) -> WorkflowRunId {
        self.run_id
            .clone()
            .unwrap_or_else(|| default_run_id(&self.workflow_name, &self.instance_id, 1))
    }

    fn target(&self) -> WorkflowExecutionTarget {
        self.target
            .clone()
            .unwrap_or_else(|| default_native_target(&self.workflow_name))
    }
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryWorkflowRunStore {
    runs: BTreeMap<WorkflowRunId, WorkflowRunRecord>,
    active_state_by_instance: BTreeMap<String, WorkflowStateRecord>,
    lifecycle_by_run: BTreeMap<WorkflowRunId, WorkflowLifecycleRecord>,
    visibility_by_run: BTreeMap<WorkflowRunId, WorkflowVisibilityRecord>,
    history_by_run: BTreeMap<WorkflowRunId, Vec<WorkflowHistoryRecord>>,
    reducer: WorkflowTransitionReducer,
}

impl InMemoryWorkflowRunStore {
    pub fn reducer(&self) -> WorkflowTransitionReducer {
        self.reducer
    }

    pub fn active_state(&self, instance_id: &str) -> Option<&WorkflowStateRecord> {
        self.active_state_by_instance.get(instance_id)
    }

    pub fn run(&self, run_id: &WorkflowRunId) -> Option<&WorkflowRunRecord> {
        self.runs.get(run_id)
    }

    pub fn lifecycle(&self, run_id: &WorkflowRunId) -> Option<&WorkflowLifecycleRecord> {
        self.lifecycle_by_run.get(run_id)
    }

    pub fn visibility(&self, run_id: &WorkflowRunId) -> Option<&WorkflowVisibilityRecord> {
        self.visibility_by_run.get(run_id)
    }

    pub fn history(&self, run_id: &WorkflowRunId) -> &[WorkflowHistoryRecord] {
        self.history_by_run
            .get(run_id)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn apply_plan(&mut self, plan: WorkflowReductionPlan) {
        for run in plan.run_records {
            self.runs.insert(run.run_id.clone(), run);
        }
        for lifecycle in plan.lifecycle_records {
            self.lifecycle_by_run
                .insert(lifecycle.run_id.clone(), lifecycle);
        }
        for visibility in plan.visibility_records {
            self.visibility_by_run
                .insert(visibility.run_id.clone(), visibility);
        }
        for history_record in plan.history {
            self.history_by_run
                .entry(history_record.run_id.clone())
                .or_default()
                .push(history_record);
        }
        self.active_state_by_instance
            .insert(plan.active_state.instance_id.clone(), plan.active_state);
    }

    pub fn apply_transition(
        &mut self,
        input: &WorkflowTransitionInput,
        output: &WorkflowTransitionOutput,
        committed_at_millis: u64,
    ) -> Result<WorkflowReductionPlan, WorkflowTaskError> {
        let plan = self.reducer.reduce(
            self.active_state(input.instance_id.as_str()),
            input,
            output,
            committed_at_millis,
        )?;
        self.apply_plan(plan.clone());
        Ok(plan)
    }

    fn describe_response(
        &self,
        run_id: &WorkflowRunId,
    ) -> Result<WorkflowDescribeResponse, WorkflowTaskError> {
        let run = self.runs.get(run_id).ok_or_else(|| {
            WorkflowTaskError::new("missing-run", format!("unknown workflow run {run_id}"))
        })?;
        let visibility = self.visibility_by_run.get(run_id).cloned().ok_or_else(|| {
            WorkflowTaskError::new(
                "missing-visibility",
                format!("missing visibility for workflow run {run_id}"),
            )
        })?;
        let lifecycle = self.lifecycle_by_run.get(run_id).cloned();
        let state = self
            .active_state_by_instance
            .get(&run.instance_id)
            .filter(|state| state.run_id == *run_id)
            .cloned()
            .unwrap_or_else(|| {
                reconstruct_state_for_run(
                    run,
                    lifecycle.as_ref(),
                    &visibility,
                    self.history(run_id),
                )
            });
        Ok(WorkflowDescribeResponse {
            state,
            lifecycle,
            visibility: WorkflowVisibilityEntry {
                record: visibility,
                execution: None,
            },
        })
    }
}

#[async_trait]
impl WorkflowVisibilityApi for InMemoryWorkflowRunStore {
    async fn list(
        &self,
        request: WorkflowVisibilityListRequest,
    ) -> Result<WorkflowVisibilityListResponse, WorkflowTaskError> {
        let limit = if request.page_size == 0 {
            usize::MAX
        } else {
            request.page_size as usize
        };
        let mut entries = Vec::new();
        let mut next_cursor = None;
        for (run_id, record) in &self.visibility_by_run {
            if request
                .cursor
                .as_ref()
                .is_some_and(|cursor| run_id <= cursor)
            {
                continue;
            }
            if request
                .workflow_name
                .as_ref()
                .is_some_and(|workflow_name| &record.workflow_name != workflow_name)
            {
                continue;
            }
            if request
                .lifecycle
                .is_some_and(|lifecycle| record.lifecycle != lifecycle)
            {
                continue;
            }
            if request.deployment_id.is_some() || request.target.is_some() {
                continue;
            }
            if entries.len() == limit {
                next_cursor = entries
                    .last()
                    .map(|entry: &WorkflowVisibilityEntry| entry.record.run_id.clone());
                break;
            }
            entries.push(WorkflowVisibilityEntry {
                record: record.clone(),
                execution: None,
            });
        }
        Ok(WorkflowVisibilityListResponse {
            entries,
            next_cursor,
        })
    }

    async fn describe(
        &self,
        request: WorkflowDescribeRequest,
    ) -> Result<WorkflowDescribeResponse, WorkflowTaskError> {
        self.describe_response(&request.run_id)
    }

    async fn visible_history(
        &self,
        request: WorkflowVisibleHistoryPageRequest,
    ) -> Result<WorkflowVisibleHistoryPageResponse, WorkflowTaskError> {
        let limit = if request.limit == 0 {
            usize::MAX
        } else {
            request.limit as usize
        };
        let mut entries = Vec::new();
        let mut next_after_sequence = None;
        for record in self.history(&request.run_id) {
            let Some(entry) = workflow_visible_history_entry(record) else {
                continue;
            };
            if request
                .after_sequence
                .is_some_and(|after_sequence| entry.sequence <= after_sequence)
            {
                continue;
            }
            if entries.len() == limit {
                next_after_sequence = entries
                    .last()
                    .map(|entry: &WorkflowVisibleHistoryEntry| entry.sequence);
                break;
            }
            entries.push(entry);
        }
        Ok(WorkflowVisibleHistoryPageResponse {
            entries,
            next_after_sequence,
        })
    }
}

pub fn default_native_target(workflow_name: &str) -> WorkflowExecutionTarget {
    WorkflowExecutionTarget::NativeRegistration {
        registration_id: WorkflowRegistrationId::new(format!("native:{workflow_name}"))
            .expect("default workflow registration ids should always be valid"),
    }
}

pub fn default_run_id(workflow_name: &str, instance_id: &str, trigger_seq: u64) -> WorkflowRunId {
    WorkflowRunId::new(format!("run:{workflow_name}:{instance_id}:{trigger_seq}"))
        .expect("default workflow run ids should always be valid")
}

pub fn default_task_id(instance_id: &str, trigger_seq: u64) -> WorkflowTaskId {
    WorkflowTaskId::new(format!("task:{instance_id}:{trigger_seq}"))
        .expect("default workflow task ids should always be valid")
}

fn validate_reduction_input(
    current_state: Option<&WorkflowStateRecord>,
    input: &WorkflowTransitionInput,
    output: &WorkflowTransitionOutput,
) -> Result<(), WorkflowTaskError> {
    if let Some(current_state) = current_state {
        if current_state.instance_id != input.instance_id {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow state instance does not match transition input",
            ));
        }
        if current_state.workflow_name != input.workflow_name {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow state name does not match transition input",
            ));
        }
        if current_state.run_id != input.run_id {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow state run id does not match transition input",
            ));
        }
        if current_state.target != input.target {
            return Err(WorkflowTaskError::invalid_contract(
                "workflow state target does not match transition input",
            ));
        }
    }

    if let Some(continue_as_new) = output.continue_as_new.as_ref() {
        if continue_as_new.next_run_id == input.run_id {
            return Err(WorkflowTaskError::invalid_contract(
                "continue-as-new must point at a distinct run id",
            ));
        }
        if output.lifecycle == Some(WorkflowLifecycleState::Failed) {
            return Err(WorkflowTaskError::invalid_contract(
                "continue-as-new cannot transition the current run to failed",
            ));
        }
    }

    Ok(())
}

fn default_lifecycle_after_apply(
    current_state: Option<&WorkflowStateRecord>,
    previous_lifecycle: WorkflowLifecycleState,
) -> WorkflowLifecycleState {
    if current_state.is_none() || previous_lifecycle == WorkflowLifecycleState::Scheduled {
        WorkflowLifecycleState::Running
    } else {
        previous_lifecycle
    }
}

fn reconstruct_state_for_run(
    run: &WorkflowRunRecord,
    lifecycle: Option<&WorkflowLifecycleRecord>,
    visibility: &WorkflowVisibilityRecord,
    history: &[WorkflowHistoryRecord],
) -> WorkflowStateRecord {
    let mut state = None;
    for record in history {
        if let WorkflowHistoryEvent::TaskApplied { output, .. } = &record.event {
            match &output.state {
                terracedb_workflows_core::WorkflowStateMutation::Unchanged => {}
                terracedb_workflows_core::WorkflowStateMutation::Put { state: next_state } => {
                    state = Some(next_state.clone())
                }
                terracedb_workflows_core::WorkflowStateMutation::Delete => state = None,
            }
        }
    }
    WorkflowStateRecord {
        run_id: run.run_id.clone(),
        target: run.target.clone(),
        workflow_name: run.workflow_name.clone(),
        instance_id: run.instance_id.clone(),
        lifecycle: lifecycle
            .map(|record| record.lifecycle)
            .unwrap_or(visibility.lifecycle),
        current_task_id: visibility.last_task_id.clone(),
        history_len: visibility.visible_history_len,
        state,
        updated_at_millis: visibility.updated_at_millis,
    }
}

fn workflow_visible_history_entry(
    record: &WorkflowHistoryRecord,
) -> Option<WorkflowVisibleHistoryEntry> {
    let WorkflowHistoryEvent::VisibilityUpdated {
        record: visibility_record,
    } = &record.event
    else {
        return None;
    };

    Some(WorkflowVisibleHistoryEntry {
        sequence: record.sequence,
        lifecycle: visibility_record.lifecycle,
        task_id: visibility_record.last_task_id.clone(),
        summary: visibility_record.summary.clone(),
        note: None,
        recorded_at_millis: visibility_record.updated_at_millis,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use terracedb_workflows_core::{
        WorkflowChangeKind, WorkflowContinueAsNew, WorkflowExecutionTarget, WorkflowPayload,
        WorkflowRegistrationId, WorkflowSourceEvent, WorkflowStateMutation,
        WorkflowTransitionOutput, WorkflowTrigger,
        WorkflowDescribeRequest,
    };

    fn sample_trigger() -> WorkflowTrigger {
        WorkflowTrigger::Callback {
            callback_id: "cb-1".to_string(),
            response: b"approved".to_vec(),
        }
    }

    fn sample_input(task_id: &str) -> WorkflowTransitionInput {
        WorkflowTransitionInput {
            run_id: default_run_id("orders", "order-1", 1),
            target: default_native_target("orders"),
            task_id: WorkflowTaskId::new(task_id).expect("task id"),
            workflow_name: "orders".to_string(),
            instance_id: "order-1".to_string(),
            lifecycle: WorkflowLifecycleState::Scheduled,
            history_len: 0,
            attempt: 1,
            admitted_at_millis: 10,
            state: None,
            trigger: sample_trigger(),
        }
    }

    #[test]
    fn reducer_creates_explicit_run_history_and_running_state() {
        let reducer = WorkflowTransitionReducer::default();
        let input = sample_input("task:1");
        let output = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("queued"),
            },
            ..WorkflowTransitionOutput::default()
        };

        let plan = reducer
            .reduce(None, &input, &output, 20)
            .expect("reduce first transition");

        assert_eq!(plan.run_records.len(), 1);
        assert_eq!(plan.run_records[0].run_id, input.run_id);
        assert_eq!(plan.active_state.lifecycle, WorkflowLifecycleState::Running);
        assert_eq!(
            plan.active_state.current_task_id,
            Some(input.task_id.clone())
        );
        assert_eq!(plan.active_state.history_len, 6);
        assert_eq!(plan.lifecycle_records.len(), 2);
        assert_eq!(
            plan.lifecycle_records[0].lifecycle,
            WorkflowLifecycleState::Scheduled
        );
        assert_eq!(
            plan.lifecycle_records[1].lifecycle,
            WorkflowLifecycleState::Running
        );
        assert_eq!(plan.visibility_records.len(), 1);
        assert_eq!(plan.history.len(), 6);
        assert!(matches!(
            &plan.history[0].event,
            WorkflowHistoryEvent::RunCreated { run_id, .. } if *run_id == input.run_id
        ));
        assert!(matches!(
            &plan.history[2].event,
            WorkflowHistoryEvent::TaskAdmitted { task_id, .. } if *task_id == input.task_id
        ));
        assert!(matches!(
            &plan.history[3].event,
            WorkflowHistoryEvent::TaskApplied { task_id, output: applied_output, .. }
                if *task_id == input.task_id && *applied_output == output
        ));
        assert!(matches!(
            &plan.history[5].event,
            WorkflowHistoryEvent::VisibilityUpdated { .. }
        ));
    }

    #[test]
    fn reducer_switches_active_state_on_continue_as_new() {
        let reducer = WorkflowTransitionReducer::default();
        let input = sample_input("task:1");
        let next_run_id = WorkflowRunId::new("run:orders:order-1:next").expect("next run id");
        let next_target = WorkflowExecutionTarget::NativeRegistration {
            registration_id: WorkflowRegistrationId::new("native:orders:v2")
                .expect("next registration id should be valid"),
        };
        let output = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("done"),
            },
            continue_as_new: Some(WorkflowContinueAsNew {
                next_run_id: next_run_id.clone(),
                next_target: next_target.clone(),
                state: Some(WorkflowPayload::bytes("carry")),
            }),
            ..WorkflowTransitionOutput::default()
        };

        let plan = reducer
            .reduce(None, &input, &output, 30)
            .expect("reduce continue-as-new");

        assert_eq!(plan.run_records.len(), 2);
        assert_eq!(plan.active_state.run_id, next_run_id);
        assert_eq!(plan.active_state.target, next_target);
        assert_eq!(
            plan.active_state.lifecycle,
            WorkflowLifecycleState::Scheduled
        );
        assert_eq!(plan.active_state.history_len, 3);
        assert!(plan.history.iter().any(|record| {
            matches!(
                &record.event,
                WorkflowHistoryEvent::ContinuedAsNew {
                    from_run_id,
                    next_run_id: recorded_next_run_id,
                    ..
                } if *from_run_id == input.run_id && *recorded_next_run_id == next_run_id
            )
        }));
        assert!(plan.history.iter().any(|record| {
            matches!(
                &record.event,
                WorkflowHistoryEvent::RunCompleted { run_id, .. } if *run_id == input.run_id
            )
        }));
    }

    #[test]
    fn in_memory_store_replays_identically_for_same_admitted_sequence() {
        let output_one = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("first"),
            },
            ..WorkflowTransitionOutput::default()
        };
        let output_two = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("second"),
            },
            ..WorkflowTransitionOutput::default()
        };

        let mut left = InMemoryWorkflowRunStore::default();
        let mut right = InMemoryWorkflowRunStore::default();
        let first = sample_input("task:1");
        let second = WorkflowTransitionInput {
            lifecycle: WorkflowLifecycleState::Running,
            history_len: 5,
            state: Some(WorkflowPayload::bytes("first")),
            task_id: WorkflowTaskId::new("task:2").expect("task id"),
            admitted_at_millis: 11,
            ..first.clone()
        };

        let left_first = left
            .apply_transition(&first, &output_one, 20)
            .expect("left first transition");
        let right_first = right
            .apply_transition(&first, &output_one, 20)
            .expect("right first transition");
        let left_second = left
            .apply_transition(&second, &output_two, 21)
            .expect("left second transition");
        let right_second = right
            .apply_transition(&second, &output_two, 21)
            .expect("right second transition");

        assert_eq!(left_first, right_first);
        assert_eq!(left_second, right_second);
        assert_eq!(left.active_state("order-1"), right.active_state("order-1"));
        assert_eq!(left.history(&first.run_id), right.history(&first.run_id));
    }

    #[test]
    fn run_builder_seeds_store_with_deterministic_defaults() {
        let store = WorkflowRunBuilder::new("orders", "order-1")
            .with_state(Some(WorkflowPayload::bytes("seeded")))
            .build_store();
        let state = store.active_state("order-1").expect("seeded active state");
        assert_eq!(state.run_id, default_run_id("orders", "order-1", 1));
        assert_eq!(state.target, default_native_target("orders"));
        assert_eq!(state.state, Some(WorkflowPayload::bytes("seeded")));
    }

    #[test]
    fn builder_can_seed_from_materialized_event_payloads() {
        let event = WorkflowSourceEvent {
            source_table: "orders".to_string(),
            key: b"order-1".to_vec(),
            value: Some(WorkflowPayload::bytes("created")),
            cursor: [0; 16],
            sequence: 1,
            kind: WorkflowChangeKind::Put,
            operation_context: None,
        };
        let input = WorkflowTransitionInput {
            trigger: WorkflowTrigger::Event { event },
            ..sample_input("task:event")
        };
        let plan = WorkflowTransitionReducer::default()
            .reduce(None, &input, &WorkflowTransitionOutput::default(), 15)
            .expect("reduce event transition");
        assert_eq!(plan.active_state.lifecycle, WorkflowLifecycleState::Running);
    }

    #[tokio::test]
    async fn visibility_api_reconstructs_terminal_run_state_after_continue_as_new() {
        let input = sample_input("task:1");
        let next_run_id = WorkflowRunId::new("run:orders:order-1:next").expect("next run id");
        let next_target = WorkflowExecutionTarget::NativeRegistration {
            registration_id: WorkflowRegistrationId::new("native:orders:v2")
                .expect("next target registration id should be valid"),
        };
        let output = WorkflowTransitionOutput {
            state: WorkflowStateMutation::Put {
                state: WorkflowPayload::bytes("done"),
            },
            continue_as_new: Some(WorkflowContinueAsNew {
                next_run_id: next_run_id.clone(),
                next_target,
                state: Some(WorkflowPayload::bytes("carry")),
            }),
            ..WorkflowTransitionOutput::default()
        };

        let mut store = InMemoryWorkflowRunStore::default();
        store
            .apply_transition(&input, &output, 30)
            .expect("apply continue-as-new transition");

        let describe = store
            .describe(WorkflowDescribeRequest {
                run_id: input.run_id.clone(),
            })
            .await
            .expect("describe prior run");
        assert_eq!(describe.state.run_id, input.run_id);
        assert_eq!(describe.state.state, Some(WorkflowPayload::bytes("done")));
        assert_eq!(describe.state.lifecycle, WorkflowLifecycleState::Completed);
        assert_eq!(
            describe.visibility.record.lifecycle,
            WorkflowLifecycleState::Completed
        );
    }
}
