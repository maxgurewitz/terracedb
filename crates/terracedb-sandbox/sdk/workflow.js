const invalid = (message, metadata = undefined) => ({
    code: "invalid-contract",
    message,
    ...(metadata === undefined ? {} : { metadata }),
});
export const bytes = (value) => Array.from(String(value)).map((char) => char.charCodeAt(0));
export const text = (value) => String.fromCharCode(...value);
const schemaType = (name, parse) => ({
    parse(value, path = "value") {
        return parse(value, path);
    },
});
const parseSchema = (schemaDefinition, value, path = "value") => {
    if (!schemaDefinition || typeof schemaDefinition.parse !== "function") {
        throw invalid(`schema at ${path} is not a valid terrace workflow schema`);
    }
    return schemaDefinition.parse(value, path);
};
export const schema = {
    string() {
        return schemaType("string", (value, path) => {
            if (typeof value !== "string") {
                throw invalid(`${path} must be a string`);
            }
            return value;
        });
    },
    number() {
        return schemaType("number", (value, path) => {
            if (typeof value !== "number" || Number.isNaN(value)) {
                throw invalid(`${path} must be a number`);
            }
            return value;
        });
    },
    boolean() {
        return schemaType("boolean", (value, path) => {
            if (typeof value !== "boolean") {
                throw invalid(`${path} must be a boolean`);
            }
            return value;
        });
    },
    literal(expected) {
        return schemaType("literal", (value, path) => {
            if (value !== expected) {
                throw invalid(`${path} must equal ${JSON.stringify(expected)}`);
            }
            return expected;
        });
    },
    optional(inner) {
        return schemaType("optional", (value, path) => {
            if (value === undefined) {
                return undefined;
            }
            return parseSchema(inner, value, path);
        });
    },
    nullable(inner) {
        return schemaType("nullable", (value, path) => {
            if (value === null) {
                return null;
            }
            return parseSchema(inner, value, path);
        });
    },
    array(inner) {
        return schemaType("array", (value, path) => {
            if (!Array.isArray(value)) {
                throw invalid(`${path} must be an array`);
            }
            return value.map((entry, index) => parseSchema(inner, entry, `${path}[${index}]`));
        });
    },
    object(shape) {
        return schemaType("object", (value, path) => {
            if (!value || typeof value !== "object" || Array.isArray(value)) {
                throw invalid(`${path} must be an object`);
            }
            const input = value;
            const result = {};
            for (const [key, inner] of Object.entries(shape)) {
                result[key] = parseSchema(inner, input[key], `${path}.${key}`);
            }
            for (const key of Object.keys(input)) {
                if (!(key in shape)) {
                    throw invalid(`${path}.${key} is not declared in the workflow schema`);
                }
            }
            return result;
        });
    },
};
const normalizePayloadBytes = (value) => {
    if (Array.isArray(value)) {
        return value.slice();
    }
    if (typeof value === "string") {
        return bytes(value);
    }
    throw invalid("timer or outbox payload bytes must be a string or an array of numbers");
};
const normalizeVisibility = (value) => {
    if (value == null) {
        return null;
    }
    const summary = value.summary ?? {};
    const normalizedSummary = {};
    for (const [key, entry] of Object.entries(summary)) {
        normalizedSummary[key] = String(entry);
    }
    return {
        summary: normalizedSummary,
        note: value.note == null ? null : String(value.note),
    };
};
const normalizeStateMutation = (definition, result) => {
    if (result.deleteState === true) {
        return { kind: "delete" };
    }
    if ("putState" in result) {
        if (!definition.state) {
            throw invalid("workflow returned putState without declaring wf.jsonState(...)");
        }
        return {
            kind: "put",
            state: definition.state.encode(result.putState, "workflow state"),
        };
    }
    return { kind: "unchanged" };
};
const normalizeTransitionOutput = (definition, result) => {
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
const unsupported = (name) => {
    throw invalid(`${name} is not available on workflow-task/v1 yet`);
};
export const wf = {
    define(definition) {
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
                const instanceId = typeof routed === "string" ? routed : routed?.instanceId ?? routed?.instance_id;
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
                    : (input.state ?? null);
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
                    visibility(summary, note = null) {
                        const normalizedSummary = {};
                        for (const [key, entry] of Object.entries(summary)) {
                            normalizedSummary[key] = String(entry);
                        }
                        return {
                            summary: normalizedSummary,
                            note: note == null ? null : String(note),
                        };
                    },
                    command: {
                        outboxJson(idempotencyKey, payload, outboxId = idempotencyKey) {
                            return {
                                kind: "outbox",
                                entry: {
                                    outbox_id: normalizePayloadBytes(outboxId),
                                    idempotency_key: String(idempotencyKey),
                                    payload: bytes(JSON.stringify(payload)),
                                },
                            };
                        },
                        outboxBytes(idempotencyKey, payload, outboxId = idempotencyKey) {
                            return {
                                kind: "outbox",
                                entry: {
                                    outbox_id: normalizePayloadBytes(outboxId),
                                    idempotency_key: String(idempotencyKey),
                                    payload: normalizePayloadBytes(payload),
                                },
                            };
                        },
                        scheduleTimer(timerId, fireAtMillis, payload = "") {
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
                        cancelTimer(timerId) {
                            return {
                                kind: "timer",
                                command: {
                                    kind: "cancel",
                                    timer_id: normalizePayloadBytes(timerId),
                                },
                            };
                        },
                        deliverCallback(targetWorkflow, targetInstanceId, callbackId, response = "") {
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
                    transition(fields = {}) {
                        return fields;
                    },
                    stay(fields = {}) {
                        return { ...fields, lifecycle: null };
                    },
                    running(fields = {}) {
                        return { ...fields, lifecycle: "running" };
                    },
                    completed(fields = {}) {
                        return { ...fields, lifecycle: "completed" };
                    },
                    failed(fields = {}) {
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
    jsonState(innerSchema) {
        return {
            decode(payload, path = "workflow state") {
                if (payload == null) {
                    return null;
                }
                if (typeof payload !== "object" ||
                    payload === null ||
                    payload.encoding !== "application/octet-stream") {
                    throw invalid(`${path} must use application/octet-stream encoding`);
                }
                const parsed = JSON.parse(text(payload.bytes));
                return parseSchema(innerSchema, parsed, path);
            },
            encode(value, path = "workflow state") {
                const parsed = parseSchema(innerSchema, value, path);
                return {
                    encoding: "application/octet-stream",
                    bytes: bytes(JSON.stringify(parsed)),
                };
            },
        };
    },
    signal(name, schemaDefinition = undefined) {
        return { kind: "signal", name, schema: schemaDefinition };
    },
    query(name, schemaDefinition = undefined) {
        return { kind: "query", name, schema: schemaDefinition };
    },
    spawn(..._args) {
        return unsupported("wf.spawn(...)");
    },
};
