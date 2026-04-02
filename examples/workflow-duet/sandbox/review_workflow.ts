import { camelCase } from "lodash";
import { schema, text, wf } from "@terrace/workflow";

type ReviewStageName = "retry-backoff" | "waiting-approval" | "approved" | "timed-out";
type ReviewStateValue = { stage: ReviewStageName; attempt: number; started_at_millis: number; last_updated_at_millis: number; waiting_for_callback: string | null; deadline_millis: number | null; last_trigger: string };

const START_CALLBACK_ID: string = "start";
const APPROVE_CALLBACK_ID: string = "approve";
const RETRY_DELAY_MS: number = 10;
const APPROVAL_TIMEOUT_MS: number = 120;

const ReviewState = wf.jsonState(
  schema.object({
    stage: schema.string(),
    attempt: schema.number(),
    started_at_millis: schema.number(),
    last_updated_at_millis: schema.number(),
    waiting_for_callback: schema.nullable(schema.string()),
    deadline_millis: schema.nullable(schema.number()),
    last_trigger: schema.string(),
  }),
);

function retryTimerId(instanceId: string): string {
  return `${instanceId}:retry`;
}

function approvalTimerId(instanceId: string): string {
  return `${instanceId}:approval-timeout`;
}

function triggerLabel(trigger: any): string {
  switch (trigger.kind) {
    case "callback":
      return `callback:${trigger.callback_id}`;
    case "timer":
      return `timer:${text(trigger.timer_id)}`;
    case "event":
      return `event:${text(trigger.event.key)}`;
    default:
      return "unknown";
  }
}

function reviewVisibility(workflowName, state, visibility) {
  const summary = {
    status: state.stage,
    attempt: String(state.attempt),
    workflow: workflowName,
    workflow_slug: camelCase(workflowName),
    "waiting-for": state.waiting_for_callback ?? "-",
  };
  if (state.deadline_millis != null) {
    summary["deadline-millis"] = String(state.deadline_millis);
  }
  return visibility(summary, `${camelCase(state.stage)} via ${state.last_trigger}`);
}

function outboxAction(command, workflowName, instanceId, action, attempt, trigger) {
  const idempotencyKey = `${instanceId}:${action}:${attempt}`;
  return command.outboxJson(idempotencyKey, {
    workflow: workflowName,
    instance_id: instanceId,
    action,
    attempt,
    trigger,
  });
}

export default wf.define({
  state: ReviewState,

  async handle({
    input,
    state,
    workflowName,
    instanceId,
    admittedAtMillis,
    command,
    visibility,
    running,
    completed,
    failed,
    stay,
  }) {
    const label = triggerLabel(input.trigger);

    if (!state) {
      if (input.trigger.kind !== "callback" || input.trigger.callback_id !== START_CALLBACK_ID) {
        throw {
          code: "invalid-contract",
          message: `workflow-duet instances must start from callback:${START_CALLBACK_ID}`,
        };
      }
      const nextState: ReviewStateValue = {
        stage: "retry-backoff",
        attempt: 1,
        started_at_millis: admittedAtMillis,
        last_updated_at_millis: admittedAtMillis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return running({
        putState: nextState,
        visibility: reviewVisibility(workflowName, nextState, visibility),
        commands: [
          outboxAction(command, workflowName, instanceId, "accepted-start", 1, label),
          outboxAction(command, workflowName, instanceId, "requested-check", 1, label),
          command.scheduleTimer(
            retryTimerId(instanceId),
            admittedAtMillis + RETRY_DELAY_MS,
            "retry",
          ),
        ],
      });
    }

    if (
      state.stage === "retry-backoff" &&
      input.trigger.kind === "timer" &&
      text(input.trigger.timer_id) === retryTimerId(instanceId)
    ) {
      const nextAttempt = state.attempt + 1;
      const deadline = input.trigger.fire_at_millis + APPROVAL_TIMEOUT_MS;
      const nextState: ReviewStateValue = {
        stage: "waiting-approval",
        attempt: nextAttempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: admittedAtMillis,
        waiting_for_callback: APPROVE_CALLBACK_ID,
        deadline_millis: deadline,
        last_trigger: label,
      };
      return running({
        putState: nextState,
        visibility: reviewVisibility(workflowName, nextState, visibility),
        commands: [
          outboxAction(command, workflowName, instanceId, "requested-approval", nextAttempt, label),
          command.scheduleTimer(approvalTimerId(instanceId), deadline, "approval-timeout"),
        ],
      });
    }

    if (
      state.stage === "waiting-approval" &&
      input.trigger.kind === "callback" &&
      input.trigger.callback_id === APPROVE_CALLBACK_ID
    ) {
      const nextState: ReviewStateValue = {
        stage: "approved",
        attempt: state.attempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: admittedAtMillis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return completed({
        putState: nextState,
        visibility: reviewVisibility(workflowName, nextState, visibility),
        commands: [
          command.cancelTimer(approvalTimerId(instanceId)),
          outboxAction(command, workflowName, instanceId, "approved", state.attempt, label),
        ],
      });
    }

    if (
      state.stage === "waiting-approval" &&
      input.trigger.kind === "timer" &&
      text(input.trigger.timer_id) === approvalTimerId(instanceId)
    ) {
      const nextState: ReviewStateValue = {
        stage: "timed-out",
        attempt: state.attempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: admittedAtMillis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return failed({
        putState: nextState,
        visibility: reviewVisibility(workflowName, nextState, visibility),
        commands: [
          outboxAction(command, workflowName, instanceId, "timed-out", state.attempt, label),
        ],
      });
    }

    return stay({
      visibility: reviewVisibility(workflowName, state, visibility),
    });
  },
});
