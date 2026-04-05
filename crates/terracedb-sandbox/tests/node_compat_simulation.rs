use std::time::Duration;

use serde_json::Value as JsonValue;
use terracedb::{
    DomainCpuBudget, ExecutionDomainBudget, ExecutionDomainOwner, ExecutionDomainPath,
    ExecutionDomainPlacement, ExecutionDomainSpec, ExecutionResourceUsage, ExecutionUsageHandle,
    InMemoryResourceManager, ResourceManager,
};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_sandbox::{
    SandboxBatchedDomainMemoryBudget, SandboxRuntimeMemoryBudget,
    SandboxTrackedMemoryBudgetPolicy,
};

#[path = "support/node_compat.rs"]
mod node_compat_support;

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatSimulationCapture {
    stdout: String,
    exit_code: i64,
    module_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatMemorySimulationCapture {
    exit_code: i64,
    managed_bytes: u64,
    gc_bytes: u64,
    accounted_bytes: u64,
    context_runtime_bytes: u64,
    task_queue_bytes: u64,
    compiled_code_bytes: u64,
    parser_retained_bytes: u64,
    module_cache_bytes: u64,
    host_buffer_bytes: u64,
    node_compat_state_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeCompatBudgetFailureCapture {
    message: String,
    charged_bytes: u64,
    current_bytes: u64,
    gc_bytes: u64,
    batch_requests: u64,
    terminated_for_budget: bool,
}

fn run_graceful_fs_simulation(seed: u64) -> turmoil::Result<NodeCompatSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                610 + seed,
                221 + seed,
                node_compat_support::graceful_fs_repro_source(),
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = result.result.clone();
            let report = match report {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            Ok(NodeCompatSimulationCapture {
                stdout: report["stdout"].as_str().unwrap_or_default().to_string(),
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                module_count: result.module_graph.len(),
            })
        })
}

fn run_next_tick_order_simulation(seed: u64) -> turmoil::Result<NodeCompatSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                810 + seed,
                421 + seed,
                r#"
                const out = [];
                process.nextTick(() => out.push("nextTick"));
                Promise.resolve().then(() => out.push("promise"));
                queueMicrotask(() => out.push("queueMicrotask"));
                queueMicrotask(() => console.log(out.join(",")));
                out.push("sync");
                "#,
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = match result.result.clone() {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            Ok(NodeCompatSimulationCapture {
                stdout: report["stdout"].as_str().unwrap_or_default().to_string(),
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                module_count: result.module_graph.len(),
            })
        })
}

fn run_node_memory_tracking_simulation(
    seed: u64,
) -> turmoil::Result<NodeCompatMemorySimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let result = node_compat_support::exec_node_fixture_with_seed(
                910 + seed,
                521 + seed,
                r#"
                const { URLPattern } = require("url");
                const path = require("path");

                globalThis.pattern = new URLPattern({ pathname: "/:id" });
                globalThis.buffer = Buffer.from(path.join("memory", "sim"));
                console.log(globalThis.buffer.toString());
                "#,
            )
            .await;
            let result = match result {
                Ok(value) => value,
                Err(error) => return Err(error.to_string().into()),
            };
            let report = match result.result.clone() {
                Some(value) => value,
                None => return Err("missing node command report".into()),
            };
            let managed = match result.metadata.get("node_runtime_managed_memory") {
                Some(JsonValue::Object(value)) => value,
                _ => return Err("missing node runtime managed memory snapshot".into()),
            };
            let read_bucket = |key: &str| {
                managed
                    .get(key)
                    .and_then(JsonValue::as_u64)
                    .ok_or_else(|| format!("missing managed memory bucket {key}"))
            };
            let context_runtime_bytes = match read_bucket("context_runtime_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let task_queue_bytes = match read_bucket("task_queue_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let compiled_code_bytes = match read_bucket("compiled_code_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let parser_retained_bytes = match read_bucket("parser_retained_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let module_cache_bytes = match read_bucket("module_cache_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let host_buffer_bytes = match read_bucket("host_buffer_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            let node_compat_state_bytes = match read_bucket("node_compat_state_bytes") {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            Ok(NodeCompatMemorySimulationCapture {
                exit_code: report["exitCode"].as_i64().unwrap_or_default(),
                managed_bytes: result
                    .metadata
                    .get("node_runtime_managed_bytes")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                gc_bytes: result
                    .metadata
                    .get("node_runtime_gc_bytes_allocated")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                accounted_bytes: result
                    .metadata
                    .get("node_runtime_accounted_bytes")
                    .and_then(JsonValue::as_u64)
                    .unwrap_or_default(),
                context_runtime_bytes,
                task_queue_bytes,
                compiled_code_bytes,
                parser_retained_bytes,
                module_cache_bytes,
                host_buffer_bytes,
                node_compat_state_bytes,
            })
        })
}

fn run_node_allocator_budget_failure_simulation(
    seed: u64,
) -> turmoil::Result<NodeCompatBudgetFailureCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(50))
        .run_with(move |_context| async move {
            let entrypoint = "/workspace/app/index.cjs";
            let session =
                node_compat_support::open_node_session(1010 + seed, 641 + seed, entrypoint, r#"
                const values = [];
                for (let i = 0; i < 20000; i += 1) {
                  values.push({ index: i, payload: "x".repeat(64) });
                }
                console.log(values.length);
                "#).await;

            let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
                ExecutionDomainBudget::default(),
            ));
            let domain_path =
                ExecutionDomainPath::new(["process", "sandbox-tests", "allocator-budget"]);
            resource_manager.register_domain(ExecutionDomainSpec {
                path: domain_path.clone(),
                owner: ExecutionDomainOwner::Subsystem {
                    database: None,
                    name: "allocator-budget".to_string(),
                },
                budget: ExecutionDomainBudget {
                    cpu: DomainCpuBudget {
                        worker_slots: Some(1),
                        weight: Some(1),
                    },
                    memory: terracedb::DomainMemoryBudget {
                        total_bytes: Some(256 * 1024),
                        cache_bytes: None,
                        mutable_bytes: None,
                    },
                    ..Default::default()
                },
                placement: ExecutionDomainPlacement::Dedicated,
                metadata: Default::default(),
            });
            let usage_handle = ExecutionUsageHandle::acquire(
                resource_manager,
                domain_path,
                ExecutionResourceUsage {
                    cpu_workers: 1,
                    ..Default::default()
                },
            );
            if !usage_handle.admitted() {
                return Err("allocator-budget domain was not admitted".into());
            }
            let budget = std::sync::Arc::new(SandboxBatchedDomainMemoryBudget::new(
                usage_handle,
                SandboxTrackedMemoryBudgetPolicy {
                    budget_bytes: None,
                    allocation_batch_bytes: 8 * 1024,
                },
            ));
            session.set_runtime_memory_budget(budget.clone());

            match session
                .exec_node_command(
                    entrypoint,
                    vec!["/usr/bin/node".to_string(), entrypoint.to_string()],
                    "/workspace/app".to_string(),
                    std::collections::BTreeMap::from([(
                        "HOME".to_string(),
                        "/workspace/home".to_string(),
                    )]),
                )
                .await
            {
                Ok(result) => Err(format!("expected budget failure, got success: {result:#?}").into()),
                Err(error) => {
                    let snapshot = budget.snapshot();
                    Ok(NodeCompatBudgetFailureCapture {
                        message: error.to_string(),
                        charged_bytes: snapshot.charged_bytes,
                        current_bytes: snapshot.current.total_bytes,
                        gc_bytes: snapshot.current.gc_heap_bytes,
                        batch_requests: snapshot.batch_requests,
                        terminated_for_budget: snapshot.terminated_for_budget,
                    })
                }
            }
        })
}

#[test]
fn seeded_graceful_fs_repro_keeps_executing_under_simulation() -> turmoil::Result<()> {
    let capture = run_graceful_fs_simulation(771)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert!(
        capture.stdout.contains("before-require"),
        "expected pre-require marker, got: {capture:#?}"
    );
    assert!(
        capture.stdout.contains("after-gracefulify:function"),
        "expected gracefulify marker, got: {capture:#?}"
    );
    assert!(
        capture.stdout.contains("after-write"),
        "expected post-write marker, got: {capture:#?}"
    );
    assert!(capture.module_count >= 4, "{capture:#?}");
    Ok(())
}

#[test]
fn seeded_next_tick_order_stays_stable_under_simulation() -> turmoil::Result<()> {
    let capture = run_next_tick_order_simulation(913)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert!(
        capture
            .stdout
            .contains("sync,nextTick,promise,queueMicrotask"),
        "expected Node-style nextTick ordering, got: {capture:#?}"
    );
    Ok(())
}

#[test]
fn seeded_node_memory_tracking_reports_named_buckets() -> turmoil::Result<()> {
    let capture = run_node_memory_tracking_simulation(977)?;
    assert_eq!(capture.exit_code, 0, "{capture:#?}");
    assert_eq!(
        capture.accounted_bytes,
        capture.managed_bytes + capture.gc_bytes,
        "{capture:#?}"
    );
    assert_eq!(
        capture.managed_bytes,
        capture.context_runtime_bytes
            + capture.task_queue_bytes
            + capture.compiled_code_bytes
            + capture.parser_retained_bytes
            + capture.module_cache_bytes
            + capture.host_buffer_bytes
            + capture.node_compat_state_bytes,
        "{capture:#?}"
    );
    assert!(capture.gc_bytes > 0, "{capture:#?}");
    assert!(capture.compiled_code_bytes > 0, "{capture:#?}");
    assert!(capture.host_buffer_bytes > 0, "{capture:#?}");
    assert!(capture.node_compat_state_bytes > 0, "{capture:#?}");
    Ok(())
}

#[test]
fn batched_memory_budget_only_grows_on_batch_boundaries() {
    let resource_manager = std::sync::Arc::new(InMemoryResourceManager::new(
        ExecutionDomainBudget::default(),
    ));
    let domain_path = ExecutionDomainPath::new(["process", "sandbox-tests", "batched-memory"]);
    resource_manager.register_domain(ExecutionDomainSpec {
        path: domain_path.clone(),
        owner: ExecutionDomainOwner::Subsystem {
            database: None,
            name: "batched-memory".to_string(),
        },
        budget: ExecutionDomainBudget {
            cpu: DomainCpuBudget {
                worker_slots: Some(1),
                weight: Some(1),
            },
            memory: terracedb::DomainMemoryBudget {
                total_bytes: Some(256 * 1024),
                cache_bytes: None,
                mutable_bytes: None,
            },
            ..Default::default()
        },
        placement: ExecutionDomainPlacement::Dedicated,
        metadata: Default::default(),
    });
    let usage_handle = ExecutionUsageHandle::acquire(
        resource_manager,
        domain_path,
        ExecutionResourceUsage {
            cpu_workers: 1,
            ..Default::default()
        },
    );
    assert!(usage_handle.admitted());
    let budget = SandboxBatchedDomainMemoryBudget::new(
        usage_handle,
        SandboxTrackedMemoryBudgetPolicy {
            budget_bytes: Some(256 * 1024),
            allocation_batch_bytes: 8 * 1024,
        },
    );
    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 1_000,
            ..Default::default()
        })
        .expect("first batch request");
    let first = budget.snapshot();
    assert_eq!(first.charged_bytes, 8 * 1024, "{first:#?}");
    assert_eq!(first.batch_requests, 1, "{first:#?}");

    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 2_000,
            ..Default::default()
        })
        .expect("same batch update");
    let second = budget.snapshot();
    assert_eq!(second.charged_bytes, 8 * 1024, "{second:#?}");
    assert_eq!(second.batch_requests, 1, "{second:#?}");

    budget
        .update_tracked_memory_usage(terracedb_sandbox::SandboxTrackedMemoryUsage {
            total_bytes: 9_000,
            ..Default::default()
        })
        .expect("second batch request");
    let third = budget.snapshot();
    assert_eq!(third.charged_bytes, 16 * 1024, "{third:#?}");
    assert_eq!(third.batch_requests, 2, "{third:#?}");
}

#[test]
fn seeded_node_budget_failure_happens_from_allocator_accounting() -> turmoil::Result<()> {
    let capture = run_node_allocator_budget_failure_simulation(991)?;
    assert!(
        capture.message.contains("domain denied tracked memory batch"),
        "{capture:#?}"
    );
    assert!(capture.gc_bytes > 0, "{capture:#?}");
    assert!(capture.current_bytes > 0, "{capture:#?}");
    assert!(capture.batch_requests > 0, "{capture:#?}");
    assert!(
        capture.current_bytes >= capture.charged_bytes,
        "{capture:#?}"
    );
    assert!(
        capture.current_bytes - capture.charged_bytes <= 8 * 1024,
        "{capture:#?}"
    );
    assert!(capture.terminated_for_budget, "{capture:#?}");
    Ok(())
}
