const START_CALLBACK_ID = "start";
const APPROVE_CALLBACK_ID = "approve";
const RETRY_DELAY_MS = 10;
const APPROVAL_TIMEOUT_MS = 40;

const bytes = (value) => Array.from(value).map((char) => char.charCodeAt(0));
const text = (value) => String.fromCharCode(...value);

const retryTimerId = (instanceId) => `${instanceId}:retry`;
const approvalTimerId = (instanceId) => `${instanceId}:approval-timeout`;

function decodeState(payload) {
  if (!payload) {
    return null;
  }
  if (payload.encoding !== "application/octet-stream") {
    throw {
      code: "invalid-contract",
      message: `workflow-duet state must use application/octet-stream, got ${payload.encoding}`,
    };
  }
  return JSON.parse(text(payload.bytes));
}

function encodeState(state) {
  return {
    encoding: "application/octet-stream",
    bytes: bytes(JSON.stringify(state)),
  };
}

function triggerLabel(trigger) {
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

function visibility(workflowName, state) {
  const summary = {
    status: state.stage,
    attempt: String(state.attempt),
    workflow: workflowName,
    "waiting-for": state.waiting_for_callback ?? "-",
  };
  if (state.deadline_millis != null) {
    summary["deadline-millis"] = String(state.deadline_millis);
  }
  return {
    summary,
    note: `${state.stage} via ${state.last_trigger}`,
  };
}

function outbox(workflowName, instanceId, action, attempt, trigger) {
  const key = `${instanceId}:${action}:${attempt}`;
  return {
    kind: "outbox",
    entry: {
      outbox_id: bytes(key),
      idempotency_key: key,
      payload: bytes(
        JSON.stringify({
          workflow: workflowName,
          instance_id: instanceId,
          action,
          attempt,
          trigger,
        }),
      ),
    },
  };
}

function scheduleTimer(timerId, fireAtMillis, payload) {
  return {
    kind: "timer",
    command: {
      kind: "schedule",
      timer_id: timerId,
      fire_at_millis: fireAtMillis,
      payload: bytes(payload),
    },
  };
}

function cancelTimer(timerId) {
  return {
    kind: "timer",
    command: {
      kind: "cancel",
      timer_id: timerId,
    },
  };
}

function buildTransition(workflowName, instanceId, state, lifecycle, commands) {
  if (state.stage === "retry-backoff") {
    commands = [
      outbox(workflowName, instanceId, "accepted-start", state.attempt, state.last_trigger),
      ...commands,
    ];
  }
  return {
    state: {
      kind: "put",
      state: encodeState(state),
    },
    lifecycle,
    visibility: visibility(workflowName, state),
    continue_as_new: null,
    commands,
  };
}

export default {
  routeEventV1() {
    throw {
      code: "invalid-contract",
      message: "workflow-duet uses explicit callbacks rather than routed source events",
    };
  },

  async handleTaskV1(request) {
    const input = request.input;
    const state = decodeState(input.state);
    const label = triggerLabel(input.trigger);
    const workflowName = input.workflow_name;
    const instanceId = input.instance_id;

    if (!state) {
      if (input.trigger.kind !== "callback" || input.trigger.callback_id !== START_CALLBACK_ID) {
        throw {
          code: "invalid-contract",
          message: `workflow-duet instances must start from callback:${START_CALLBACK_ID}`,
        };
      }
      const nextState = {
        stage: "retry-backoff",
        attempt: 1,
        started_at_millis: input.admitted_at_millis,
        last_updated_at_millis: input.admitted_at_millis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return {
        abi: request.abi,
        output: buildTransition(workflowName, instanceId, nextState, "running", [
          outbox(workflowName, instanceId, "requested-check", 1, label),
          scheduleTimer(
            bytes(retryTimerId(instanceId)),
            input.admitted_at_millis + RETRY_DELAY_MS,
            "retry",
          ),
        ]),
      };
    }

    if (
      state.stage === "retry-backoff" &&
      input.trigger.kind === "timer" &&
      text(input.trigger.timer_id) === retryTimerId(instanceId)
    ) {
      const nextAttempt = state.attempt + 1;
      const deadline = input.trigger.fire_at_millis + APPROVAL_TIMEOUT_MS;
      const nextState = {
        stage: "waiting-approval",
        attempt: nextAttempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: input.admitted_at_millis,
        waiting_for_callback: APPROVE_CALLBACK_ID,
        deadline_millis: deadline,
        last_trigger: label,
      };
      return {
        abi: request.abi,
        output: buildTransition(workflowName, instanceId, nextState, "running", [
          outbox(workflowName, instanceId, "requested-approval", nextAttempt, label),
          scheduleTimer(bytes(approvalTimerId(instanceId)), deadline, "approval-timeout"),
        ]),
      };
    }

    if (
      state.stage === "waiting-approval" &&
      input.trigger.kind === "callback" &&
      input.trigger.callback_id === APPROVE_CALLBACK_ID
    ) {
      const nextState = {
        stage: "approved",
        attempt: state.attempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: input.admitted_at_millis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return {
        abi: request.abi,
        output: buildTransition(workflowName, instanceId, nextState, "completed", [
          cancelTimer(bytes(approvalTimerId(instanceId))),
          outbox(workflowName, instanceId, "approved", state.attempt, label),
        ]),
      };
    }

    if (
      state.stage === "waiting-approval" &&
      input.trigger.kind === "timer" &&
      text(input.trigger.timer_id) === approvalTimerId(instanceId)
    ) {
      const nextState = {
        stage: "timed-out",
        attempt: state.attempt,
        started_at_millis: state.started_at_millis,
        last_updated_at_millis: input.admitted_at_millis,
        waiting_for_callback: null,
        deadline_millis: null,
        last_trigger: label,
      };
      return {
        abi: request.abi,
        output: buildTransition(workflowName, instanceId, nextState, "failed", [
          outbox(workflowName, instanceId, "timed-out", state.attempt, label),
        ]),
      };
    }

    return {
      abi: request.abi,
      output: {
        state: { kind: "unchanged" },
        lifecycle: null,
        visibility: visibility(workflowName, state),
        continue_as_new: null,
        commands: [],
      },
    };
  },
};
