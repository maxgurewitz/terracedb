declare module "@terrace/workflow" {
  type PrimitiveValue = string | number | boolean | null | undefined;
  type WorkflowVisibility = {
      summary: Record<string, string>;
      note: string | null;
  };
  type WorkflowPayload = {
      encoding: string;
      bytes: number[];
  };
  type WorkflowStateMutation = {
      kind: "unchanged";
  } | {
      kind: "delete";
  } | {
      kind: "put";
      state: WorkflowPayload;
  };
  type WorkflowOutboxCommand = {
      kind: "outbox";
      entry: {
          outbox_id: number[];
          idempotency_key: string;
          payload: number[];
      };
  };
  type WorkflowTimerCommand = {
      kind: "timer";
      command: {
          kind: "schedule";
          timer_id: number[];
          fire_at_millis: number;
          payload: number[];
      };
  } | {
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
  export type WorkflowCommand = WorkflowOutboxCommand | WorkflowTimerCommand | WorkflowDeliverCallbackCommand;
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
      }) => string | {
          instanceId?: string;
          instance_id?: string;
      };
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
              outboxJson(idempotencyKey: string, payload: unknown, outboxId?: string | number[]): WorkflowOutboxCommand;
              outboxBytes(idempotencyKey: string, payload: string | number[], outboxId?: string | number[]): WorkflowOutboxCommand;
              scheduleTimer(timerId: string | number[], fireAtMillis: number, payload?: string | number[]): WorkflowTimerCommand;
              cancelTimer(timerId: string | number[]): WorkflowTimerCommand;
              deliverCallback(targetWorkflow: string, targetInstanceId: string, callbackId: string, response?: string | number[]): WorkflowDeliverCallbackCommand;
          };
          transition(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
          stay(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
          running(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
          completed(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
          failed(fields?: WorkflowTransitionFields<State>): WorkflowTransitionFields<State>;
      }) => Promise<WorkflowTransitionFields<State>> | WorkflowTransitionFields<State>;
  };
  type CompiledWorkflowDefinition = {
      routeEventV1(request: WorkflowRouteRequest): {
          abi: string;
          instance_id: string;
      };
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
  export declare const bytes: (value: unknown) => number[];
  export declare const text: (value: number[]) => string;
  export declare const schema: {
      string(): TerraceSchema<string>;
      number(): TerraceSchema<number>;
      boolean(): TerraceSchema<boolean>;
      literal<T>(expected: T): TerraceSchema<T>;
      optional<T>(inner: TerraceSchema<T>): TerraceSchema<T | undefined>;
      nullable<T>(inner: TerraceSchema<T>): TerraceSchema<T | null>;
      array<T>(inner: TerraceSchema<T>): TerraceSchema<T[]>;
      object<T extends Record<string, TerraceSchema<unknown>>>(shape: T): TerraceSchema<{ [K in keyof T]: T[K] extends TerraceSchema<infer U> ? U : never; }>;
  };
  export declare const wf: {
      define<State>(definition: WorkflowDefinition<State>): CompiledWorkflowDefinition;
      jsonState<T>(innerSchema: TerraceSchema<T>): JsonStateDefinition<T>;
      signal<T = unknown>(name: string, schemaDefinition?: TerraceSchema<T> | undefined): {
          kind: "signal";
          name: string;
          schema: TerraceSchema<T> | undefined;
      };
      query<T = unknown>(name: string, schemaDefinition?: TerraceSchema<T> | undefined): {
          kind: "query";
          name: string;
          schema: TerraceSchema<T> | undefined;
      };
      spawn(..._args: unknown[]): never;
  };
  export {};
}
