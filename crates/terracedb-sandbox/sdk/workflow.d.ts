declare module "@terrace/workflow" {
  export type TerraceSchema<T = unknown> = {
    parse(value: unknown, path?: string): T;
  };

  export const schema: {
    string(): TerraceSchema<string>;
    number(): TerraceSchema<number>;
    boolean(): TerraceSchema<boolean>;
    literal<T>(expected: T): TerraceSchema<T>;
    optional<T>(inner: TerraceSchema<T>): TerraceSchema<T | undefined>;
    nullable<T>(inner: TerraceSchema<T>): TerraceSchema<T | null>;
    array<T>(inner: TerraceSchema<T>): TerraceSchema<T[]>;
    object<T extends Record<string, TerraceSchema<any>>>(
      shape: T,
    ): TerraceSchema<{ [K in keyof T]: T[K] extends TerraceSchema<infer U> ? U : never }>;
  };

  export const bytes: (value: unknown) => number[];
  export const text: (value: number[]) => string;

  export type WorkflowDefinition = {
    routeEventV1(request: unknown): unknown;
    handleTaskV1(request: unknown): Promise<unknown>;
  };

  export const wf: {
    define(definition: {
      state?: {
        decode(payload: unknown, path?: string): unknown;
        encode(value: unknown, path?: string): unknown;
      };
      routeEvent?: (ctx: Record<string, unknown>) => string | { instanceId?: string; instance_id?: string };
      handle(ctx: Record<string, unknown>): Promise<Record<string, unknown>> | Record<string, unknown>;
    }): WorkflowDefinition;
    jsonState<T>(inner: TerraceSchema<T>): {
      decode(payload: unknown, path?: string): T | null;
      encode(value: T, path?: string): unknown;
    };
    signal<T = unknown>(name: string, schema?: TerraceSchema<T>): { kind: "signal"; name: string; schema?: TerraceSchema<T> };
    query<T = unknown>(name: string, schema?: TerraceSchema<T>): { kind: "query"; name: string; schema?: TerraceSchema<T> };
    spawn(...args: unknown[]): never;
  };
}
