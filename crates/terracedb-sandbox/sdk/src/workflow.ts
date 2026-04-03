type PrimitiveValue = string | number | boolean | null | undefined;

type WorkflowVisibility = {
  summary: Record<string, string>;
  note: string | null;
};

type WorkflowPayload = {
  encoding: string;
  bytes: number[];
};

type WorkflowStateMutation =
  | { kind: "unchanged" }
  | { kind: "delete" }
  | { kind: "put"; state: WorkflowPayload };

type WorkflowOutboxCommand = {
  kind: "outbox";
  entry: {
    outbox_id: number[];
    idempotency_key: string;
    payload: number[];
  };
};

type WorkflowTimerCommand =
  | {
      kind: "timer";
      command: {
        kind: "schedule";
        timer_id: number[];
        fire_at_millis: number;
        payload: number[];
      };
    }
  | {
      kind: "timer";
      command: {
        kind: "cancel";
        timer_id: number[];
      };
    };

type WorkflowDeliverCallbackCommand = {
  kind: "deliver-callback";
  delivery: {
    target_workflow: string;
    instance_id: string;
    callback_id: string;
    response: number[];
  };
};

export type WorkflowCommand =
  | WorkflowOutboxCommand
  | WorkflowTimerCommand
  | WorkflowDeliverCallbackCommand;

type WorkflowTransitionFields<State> = {
  putState?: State;
  deleteState?: true;
  visibility?: WorkflowVisibility | null;
  continueAsNew?: unknown;
  commands?: WorkflowCommand[];
  lifecycle?: string | null;
};

type WorkflowRequest = {
  abi: string;
  input: {
    workflow_name: string;
    instance_id: string;
    lifecycle: string;
    history_len: number;
    admitted_at_millis: number;
    trigger: unknown;
    state?: WorkflowPayload | null;
  };
  deterministic: unknown;
};

type WorkflowRouteRequest = {
  abi: string;
  event: unknown;
};

export type TerraceSchema<T = unknown> = {
  parse(value: unknown, path?: string): T;
};

type JsonStateDefinition<T> = {
  decode(payload: unknown, path?: string): T | null;
  encode(value: T, path?: string): WorkflowPayload;
};

type WorkflowDefinition<State> = {
  state?: JsonStateDefinition<State>;
  routeEvent?: (ctx: {
    request: WorkflowRouteRequest;
    event: unknown;
    bytes: (value: unknown) => number[];
    text: (value: number[]) => string;
  }) => string | { instanceId?: string; instance_id?: string };
  handle: (ctx: {
    request: WorkflowRequest;
    input: WorkflowRequest["input"];
    state: State | null;
    bytes: (value: unknown) => number[];
    text: (value: number[]) => string;
    trigger: unknown;
    workflowName: string;
    instanceId: string;
    lifecycle: string;
    historyLength: number;
    admittedAtMillis: number;
    deterministic: unknown;
    visibility(summary: Record<string, PrimitiveValue>, note?: PrimitiveValue): WorkflowVisibility;
    command: {
      outboxJson(
        idempotencyKey: string,
        payload: unknown,
        outboxId?: string | number[],
      ): WorkflowOutboxCommand;
      outboxBytes(
        idempotencyKey: string,
        payload: string | number[],
        outboxId?: string | number[],
      ): WorkflowOutboxCommand;
      scheduleTimer(
        timerId: string | number[],
        fireAtMillis: number,
        payload?: string | number[],
      ): WorkflowTimerCommand;
      cancelTimer(timerId: string | number[]): WorkflowTimerCommand;
      deliverCallback(
        targetWorkflow: string,
        targetInstanceId: string,
        callbackId: string,
        response?: string | number[],
      ): WorkflowDeliverCallbackCommand;
    };
    transition(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
    stay(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
    running(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
    completed(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
    failed(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
  }) => Promise<WorkflowTransitionFields<State>> | WorkflowTransitionFields<State>;
};

type CompiledWorkflowDefinition = {
  routeEventV1(request: WorkflowRouteRequest): { abi: string; instance_id: string };
  handleTaskV1(request: WorkflowRequest): Promise<{
    abi: string;
    output: {
      state: WorkflowStateMutation;
      lifecycle: string | null;
      visibility: WorkflowVisibility | null;
      continue_as_new: unknown;
      commands: WorkflowCommand[];
    };
  }>;
};

const invalid = (message: string, metadata: unknown = undefined) => ({
  code: "invalid-contract",
  message,
  ...(metadata === undefined ? {} : { metadata }),
});

export const bytes = (value: unknown): number[] =>
  Array.from(String(value)).map((char) => char.charCodeAt(0));

export const text = (value: number[]): string => String.fromCharCode(...value);

const schemaType = <T>(name: string, parse: (value: unknown, path: string) => T): TerraceSchema<T> => ({
  parse(value: unknown, path = "value"): T {
    return parse(value, path);
  },
});

const parseSchema = <T>(schemaDefinition: TerraceSchema<T>, value: unknown, path = "value"): T => {
  if (!schemaDefinition || typeof schemaDefinition.parse !== "function") {
    throw invalid(`schema at ${path} is not a valid terrace workflow schema`);
  }
  return schemaDefinition.parse(value, path);
};

export const schema = {
  string(): TerraceSchema<string> {
    return schemaType("string", (value, path) => {
      if (typeof value !== "string") {
        throw invalid(`${path} must be a string`);
      }
      return value;
    });
  },

  number(): TerraceSchema<number> {
    return schemaType("number", (value, path) => {
      if (typeof value !== "number" || Number.isNaN(value)) {
        throw invalid(`${path} must be a number`);
      }
      return value;
    });
  },

  boolean(): TerraceSchema<boolean> {
    return schemaType("boolean", (value, path) => {
      if (typeof value !== "boolean") {
        throw invalid(`${path} must be a boolean`);
      }
      return value;
    });
  },

  literal<T>(expected: T): TerraceSchema<T> {
    return schemaType("literal", (value, path) => {
      if (value !== expected) {
        throw invalid(`${path} must equal ${JSON.stringify(expected)}`);
      }
      return expected;
    });
  },

  optional<T>(inner: TerraceSchema<T>): TerraceSchema<T | undefined> {
    return schemaType("optional", (value, path) => {
      if (value === undefined) {
        return undefined;
      }
      return parseSchema(inner, value, path);
    });
  },

  nullable<T>(inner: TerraceSchema<T>): TerraceSchema<T | null> {
    return schemaType("nullable", (value, path) => {
      if (value === null) {
        return null;
      }
      return parseSchema(inner, value, path);
    });
  },

  array<T>(inner: TerraceSchema<T>): TerraceSchema<T[]> {
    return schemaType("array", (value, path) => {
      if (!Array.isArray(value)) {
        throw invalid(`${path} must be an array`);
      }
      return value.map((entry, index) => parseSchema(inner, entry, `${path}[${index}]`));
    });
  },

  object<T extends Record<string, TerraceSchema<unknown>>>(
    shape: T,
  ): TerraceSchema<{ [K in keyof T]: T[K] extends TerraceSchema<infer U> ? U : never }> {
    return schemaType("object", (value, path) => {
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw invalid(`${path} must be an object`);
      }
      const input = value as Record<string, unknown>;
      const result: Record<string, unknown> = {};
      for (const [key, inner] of Object.entries(shape)) {
        result[key] = parseSchema(inner, input[key], `${path}.${key}`);
      }
      for (const key of Object.keys(input)) {
        if (!(key in shape)) {
          throw invalid(`${path}.${key} is not declared in the workflow schema`);
        }
      }
      return result as { [K in keyof T]: T[K] extends TerraceSchema<infer U> ? U : never };
    });
  },
};

const normalizePayloadBytes = (value: string | number[]): number[] => {
  if (Array.isArray(value)) {
    return value.slice();
  }
  if (typeof value === "string") {
    return bytes(value);
  }
  throw invalid("timer or outbox payload bytes must be a string or an array of numbers");
};

const normalizeVisibility = (value: WorkflowVisibility | null | undefined): WorkflowVisibility | null => {
  if (value == null) {
    return null;
  }
  const summary = value.summary ?? {};
  const normalizedSummary: Record<string, string> = {};
  for (const [key, entry] of Object.entries(summary)) {
    normalizedSummary[key] = String(entry);
  }
  return {
    summary: normalizedSummary,
    note: value.note == null ? null : String(value.note),
  };
};

const normalizeStateMutation = <State>(
  definition: WorkflowDefinition<State>,
  result: WorkflowTransitionFields<State>,
): WorkflowStateMutation => {
  if (result.deleteState === true) {
    return { kind: "delete" };
  }
  if ("putState" in result) {
    if (!definition.state) {
      throw invalid("workflow returned putState without declaring wf.jsonState(...)");
    }
    return {
      kind: "put",
      state: definition.state.encode(result.putState as State, "workflow state"),
    };
  }
  return { kind: "unchanged" };
};

const normalizeTransitionOutput = <State>(
  definition: WorkflowDefinition<State>,
  result: WorkflowTransitionFields<State>,
) => {
  if (!result || typeof result !== "object" || Array.isArray(result)) {
    throw invalid("workflow handlers must return a transition object");
  }
  return {
    state: normalizeStateMutation(definition, result),
    lifecycle: result.lifecycle ?? null,
    visibility: normalizeVisibility(result.visibility),
    continue_as_new: result.continueAsNew ?? null,
    commands: Array.isArray(result.commands) ? result.commands : [],
  };
};

const unsupported = (name: string): never => {
  throw invalid(`${name} is not available on workflow-task/v1 yet`);
};

export const wf = {
  define<State>(definition: WorkflowDefinition<State>): CompiledWorkflowDefinition {
    if (!definition || typeof definition !== "object") {
      throw invalid("wf.define(...) requires a workflow definition object");
    }
    if (typeof definition.handle !== "function") {
      throw invalid("wf.define(...) requires an async handle(ctx) function");
    }

    return {
      routeEventV1(request) {
        if (typeof definition.routeEvent !== "function") {
          throw invalid("this workflow only supports explicit callbacks or timers");
        }
        const routed = definition.routeEvent({
          request,
          event: request.event,
          bytes,
          text,
        });
        const instanceId =
          typeof routed === "string" ? routed : routed?.instanceId ?? routed?.instance_id;
        if (typeof instanceId !== "string" || instanceId.length === 0) {
          throw invalid("routeEvent must return a non-empty instance id");
        }
        return {
          abi: request.abi,
          instance_id: instanceId,
        };
      },

      async handleTaskV1(request) {
        const input = request.input;
        const decodedState = definition.state
          ? definition.state.decode(input.state, "workflow state")
          : ((input.state ?? null) as State | null);
        const helpers = {
          bytes,
          text,
          trigger: input.trigger,
          workflowName: input.workflow_name,
          instanceId: input.instance_id,
          lifecycle: input.lifecycle,
          historyLength: input.history_len,
          admittedAtMillis: input.admitted_at_millis,
          deterministic: request.deterministic,
          visibility(summary: Record<string, PrimitiveValue>, note: PrimitiveValue = null): WorkflowVisibility {
            const normalizedSummary: Record<string, string> = {};
            for (const [key, entry] of Object.entries(summary)) {
              normalizedSummary[key] = String(entry);
            }
            return {
              summary: normalizedSummary,
              note: note == null ? null : String(note),
            };
          },
          command: {
            outboxJson(idempotencyKey: string, payload: unknown, outboxId: string | number[] = idempotencyKey): WorkflowOutboxCommand {
              return {
                kind: "outbox",
                entry: {
                  outbox_id: normalizePayloadBytes(outboxId),
                  idempotency_key: String(idempotencyKey),
                  payload: bytes(JSON.stringify(payload)),
                },
              };
            },
            outboxBytes(idempotencyKey: string, payload: string | number[], outboxId: string | number[] = idempotencyKey): WorkflowOutboxCommand {
              return {
                kind: "outbox",
                entry: {
                  outbox_id: normalizePayloadBytes(outboxId),
                  idempotency_key: String(idempotencyKey),
                  payload: normalizePayloadBytes(payload),
                },
              };
            },
            scheduleTimer(timerId: string | number[], fireAtMillis: number, payload: string | number[] = ""): WorkflowTimerCommand {
              return {
                kind: "timer",
                command: {
                  kind: "schedule",
                  timer_id: normalizePayloadBytes(timerId),
                  fire_at_millis: fireAtMillis,
                  payload: normalizePayloadBytes(payload),
                },
              };
            },
            cancelTimer(timerId: string | number[]): WorkflowTimerCommand {
              return {
                kind: "timer",
                command: {
                  kind: "cancel",
                  timer_id: normalizePayloadBytes(timerId),
                },
              };
            },
            deliverCallback(
              targetWorkflow: string,
              targetInstanceId: string,
              callbackId: string,
              response: string | number[] = "",
            ): WorkflowDeliverCallbackCommand {
              return {
                kind: "deliver-callback",
                delivery: {
                  target_workflow: String(targetWorkflow),
                  instance_id: String(targetInstanceId),
                  callback_id: String(callbackId),
                  response: normalizePayloadBytes(response),
                },
              };
            },
          },
          transition(fields: WorkflowTransitionFields<State> = {}): WorkflowTransitionFields<State> {
            return fields;
          },
          stay(fields: WorkflowTransitionFields<State> = {}): WorkflowTransitionFields<State> {
            return { ...fields, lifecycle: null };
          },
          running(fields: WorkflowTransitionFields<State> = {}): WorkflowTransitionFields<State> {
            return { ...fields, lifecycle: "running" };
          },
          completed(fields: WorkflowTransitionFields<State> = {}): WorkflowTransitionFields<State> {
            return { ...fields, lifecycle: "completed" };
          },
          failed(fields: WorkflowTransitionFields<State> = {}): WorkflowTransitionFields<State> {
            return { ...fields, lifecycle: "failed" };
          },
        };

        const output = await definition.handle({
          request,
          input,
          state: decodedState,
          ...helpers,
        });
        return {
          abi: request.abi,
          output: normalizeTransitionOutput(definition, output),
        };
      },
    };
  },

  jsonState<T>(innerSchema: TerraceSchema<T>): JsonStateDefinition<T> {
    return {
      decode(payload: unknown, path = "workflow state"): T | null {
        if (payload == null) {
          return null;
        }
        if (
          typeof payload !== "object" ||
          payload === null ||
          (payload as WorkflowPayload).encoding !== "application/octet-stream"
        ) {
          throw invalid(`${path} must use application/octet-stream encoding`);
        }
        const parsed = JSON.parse(text((payload as WorkflowPayload).bytes));
        return parseSchema(innerSchema, parsed, path);
      },

      encode(value: T, path = "workflow state"): WorkflowPayload {
        const parsed = parseSchema(innerSchema, value, path);
        return {
          encoding: "application/octet-stream",
          bytes: bytes(JSON.stringify(parsed)),
        };
      },
    };
  },

  signal<T = unknown>(name: string, schemaDefinition: TerraceSchema<T> | undefined = undefined) {
    return { kind: "signal" as const, name, schema: schemaDefinition };
  },

  query<T = unknown>(name: string, schemaDefinition: TerraceSchema<T> | undefined = undefined) {
    return { kind: "query" as const, name, schema: schemaDefinition };
  },

  spawn(..._args: unknown[]): never {
    return unsupported("wf.spawn(...)");
  },
};
