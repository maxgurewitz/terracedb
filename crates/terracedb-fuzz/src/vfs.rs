use std::{collections::BTreeMap, time::Duration};

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use terracedb::{DeterministicRng, LogCursor, Rng};
use terracedb_simulation::SeededSimulationRunner;
use terracedb_vfs::{
    ActivityOptions, CompletedToolRun, CompletedToolRunOutcome, CreateOptions, InMemoryVfsStore,
    JsonValue, ReadOnlyVfsFileSystem, ReadOnlyVfsKvStore, SnapshotOptions, VolumeConfig, VolumeId,
    VolumeStore,
};

use crate::{GeneratedScenarioHarness, ScenarioOperations, assert_non_decreasing};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsScenarioConfig {
    pub steps: usize,
    pub path_count: usize,
    pub key_count: usize,
    pub max_payload_len: usize,
    pub chunk_size: u32,
}

impl Default for VfsScenarioConfig {
    fn default() -> Self {
        Self {
            steps: 20,
            path_count: 4,
            key_count: 3,
            max_payload_len: 12,
            chunk_size: 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcomeSpec {
    Success,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VfsOperation {
    WriteFile {
        path: String,
        data: Vec<u8>,
    },
    Pwrite {
        path: String,
        offset: u64,
        data: Vec<u8>,
    },
    Rename {
        from: String,
        to: String,
    },
    Unlink {
        path: String,
    },
    ReadFile {
        path: String,
    },
    SetJson {
        key: String,
        value: JsonValue,
    },
    DeleteJson {
        key: String,
    },
    GetJson {
        key: String,
    },
    RecordCompletedTool {
        name: String,
        outcome: ToolOutcomeSpec,
    },
    Flush,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VfsGeneratedScenario {
    pub seed: u64,
    pub volume_id: u128,
    pub chunk_size: u32,
    pub workload: Vec<VfsOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VfsSimulationOutcome {
    pub seed: u64,
    pub scenario: VfsGeneratedScenario,
    pub visible_files: BTreeMap<String, Vec<u8>>,
    pub durable_files: BTreeMap<String, Vec<u8>>,
    pub visible_kv: BTreeMap<String, JsonValue>,
    pub durable_kv: BTreeMap<String, JsonValue>,
    pub visible_tool_runs: usize,
    pub durable_tool_runs: usize,
    pub visible_activity_count: usize,
    pub durable_activity_count: usize,
}

#[derive(Clone, Debug)]
pub struct VfsScenarioHarness {
    config: VfsScenarioConfig,
}

impl VfsScenarioHarness {
    pub fn new(config: VfsScenarioConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &VfsScenarioConfig {
        &self.config
    }
}

impl GeneratedScenarioHarness for VfsScenarioHarness {
    type Scenario = VfsGeneratedScenario;
    type Outcome = VfsSimulationOutcome;
    type Error = Box<dyn std::error::Error>;

    fn generate(&self, seed: u64) -> Self::Scenario {
        generate_vfs_scenario(seed, &self.config)
    }

    fn run(&self, scenario: Self::Scenario) -> Result<Self::Outcome, Self::Error> {
        run_vfs_scenario(scenario, &self.config)
    }
}

impl ScenarioOperations for VfsGeneratedScenario {
    type Operation = VfsOperation;

    fn operations(&self) -> &[Self::Operation] {
        &self.workload
    }

    fn with_operations(&self, operations: Vec<Self::Operation>) -> Self {
        let mut next = self.clone();
        next.workload = operations;
        next
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ShadowState {
    files: BTreeMap<String, Vec<u8>>,
    kv: BTreeMap<String, JsonValue>,
    tool_runs: usize,
    activity_count: usize,
}

fn generate_vfs_scenario(seed: u64, config: &VfsScenarioConfig) -> VfsGeneratedScenario {
    let rng = DeterministicRng::seeded(seed);
    let mut files = BTreeMap::<String, Vec<u8>>::new();
    let mut kv = BTreeMap::<String, JsonValue>::new();
    let mut workload = Vec::with_capacity(config.steps);

    for step in 0..config.steps {
        let path = random_path(&rng, config.path_count);
        let key = random_key(&rng, config.key_count);
        let mut choices = vec![0_u8, 1, 2, 3];
        if !files.is_empty() {
            choices.extend([4, 5, 6, 7]);
        }
        if !kv.is_empty() {
            choices.extend([8, 9]);
        }

        let choice = choices[choose_index(&rng, choices.len())];
        let operation = match choice {
            0 => {
                let data = make_payload(&rng, config.max_payload_len, step);
                files.insert(path.clone(), data.clone());
                VfsOperation::WriteFile { path, data }
            }
            1 => {
                let value = make_json_value(&rng, step);
                kv.insert(key.clone(), value.clone());
                VfsOperation::SetJson { key, value }
            }
            2 => VfsOperation::RecordCompletedTool {
                name: format!("tool-{step}"),
                outcome: if rng.next_u64().is_multiple_of(2) {
                    ToolOutcomeSpec::Success
                } else {
                    ToolOutcomeSpec::Error
                },
            },
            3 => VfsOperation::Flush,
            4 => {
                let from = choose_existing_key(&rng, files.keys().cloned().collect());
                let to = random_distinct_path(&rng, config.path_count, &from);
                let data = files
                    .remove(&from)
                    .expect("generator rename source should exist");
                files.insert(to.clone(), data);
                VfsOperation::Rename { from, to }
            }
            5 => {
                let path = choose_existing_key(&rng, files.keys().cloned().collect());
                let file = files
                    .get_mut(&path)
                    .expect("generator patch path should exist");
                let offset = if file.is_empty() {
                    0
                } else {
                    rng.next_u64() % (file.len() as u64 + 3)
                };
                let data = make_payload(
                    &rng,
                    config.max_payload_len.saturating_sub(2).max(1),
                    step ^ 0x55,
                );
                apply_pwrite(file, offset as usize, &data);
                VfsOperation::Pwrite { path, offset, data }
            }
            6 => {
                let path = choose_existing_key(&rng, files.keys().cloned().collect());
                files.remove(&path);
                VfsOperation::Unlink { path }
            }
            7 => {
                let path = choose_existing_key(&rng, files.keys().cloned().collect());
                VfsOperation::ReadFile { path }
            }
            8 => {
                let key = choose_existing_key(&rng, kv.keys().cloned().collect());
                kv.remove(&key);
                VfsOperation::DeleteJson { key }
            }
            9 => {
                let key = choose_existing_key(&rng, kv.keys().cloned().collect());
                VfsOperation::GetJson { key }
            }
            _ => unreachable!("bounded generator choice"),
        };
        workload.push(operation);
    }

    VfsGeneratedScenario {
        seed,
        volume_id: 0x7900 + seed as u128,
        chunk_size: config.chunk_size.max(1),
        workload,
    }
}

fn run_vfs_scenario(
    scenario: VfsGeneratedScenario,
    config: &VfsScenarioConfig,
) -> turmoil::Result<VfsSimulationOutcome> {
    let seed = scenario.seed;
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(75))
        .run_with({
            let scenario = scenario.clone();
            let config = config.clone();
            move |context| async move {
                let store = InMemoryVfsStore::new(context.clock(), context.rng());
                let volume = store
                    .open_volume(
                        VolumeConfig::new(VolumeId::new(scenario.volume_id))
                            .with_chunk_size(scenario.chunk_size)
                            .with_create_if_missing(true),
                    )
                    .await
                    .expect("open generated vfs volume");
                volume
                    .fs()
                    .mkdir(
                        "/workspace",
                        terracedb_vfs::MkdirOptions {
                            recursive: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("create generated workspace root");
                volume
                    .flush()
                    .await
                    .expect("flush generated workspace root");

                let mut visible = ShadowState::default();
                let mut durable = ShadowState::default();
                visible.activity_count = 1;
                durable.activity_count = 1;

                let mut visible_activity_history = vec![visible.activity_count];
                let mut durable_activity_history = vec![durable.activity_count];

                for operation in &scenario.workload {
                    match operation {
                        VfsOperation::WriteFile { path, data } => {
                            volume
                                .fs()
                                .write_file(path, data.clone(), CreateOptions::default())
                                .await
                                .expect("generated write file");
                            visible.files.insert(path.clone(), data.clone());
                            visible.activity_count += 1;
                        }
                        VfsOperation::Pwrite { path, offset, data } => {
                            volume
                                .fs()
                                .pwrite(path, *offset, data.clone())
                                .await
                                .expect("generated pwrite");
                            let file = visible
                                .files
                                .get_mut(path)
                                .expect("generated pwrite path should exist");
                            apply_pwrite(file, *offset as usize, data);
                            visible.activity_count += 1;
                        }
                        VfsOperation::Rename { from, to } => {
                            volume
                                .fs()
                                .rename(from, to)
                                .await
                                .expect("generated rename");
                            let data = visible
                                .files
                                .remove(from)
                                .expect("generated rename source should exist");
                            visible.files.insert(to.clone(), data);
                            visible.activity_count += 1;
                        }
                        VfsOperation::Unlink { path } => {
                            volume.fs().unlink(path).await.expect("generated unlink");
                            visible.files.remove(path);
                            visible.activity_count += 1;
                        }
                        VfsOperation::ReadFile { path } => {
                            let actual = volume
                                .fs()
                                .read_file(path)
                                .await
                                .expect("generated read file");
                            assert_eq!(actual, visible.files.get(path).cloned());
                        }
                        VfsOperation::SetJson { key, value } => {
                            volume
                                .kv()
                                .set_json(key, value.clone())
                                .await
                                .expect("generated set json");
                            visible.kv.insert(key.clone(), value.clone());
                            visible.activity_count += 1;
                        }
                        VfsOperation::DeleteJson { key } => {
                            volume
                                .kv()
                                .delete(key)
                                .await
                                .expect("generated delete json");
                            visible.kv.remove(key);
                            visible.activity_count += 1;
                        }
                        VfsOperation::GetJson { key } => {
                            let actual =
                                volume.kv().get_json(key).await.expect("generated get json");
                            assert_eq!(actual, visible.kv.get(key).cloned());
                        }
                        VfsOperation::RecordCompletedTool { name, outcome } => {
                            volume
                                .tools()
                                .record_completed(CompletedToolRun {
                                    name: name.clone(),
                                    params: None,
                                    outcome: match outcome {
                                        ToolOutcomeSpec::Success => {
                                            CompletedToolRunOutcome::Success { result: None }
                                        }
                                        ToolOutcomeSpec::Error => CompletedToolRunOutcome::Error {
                                            message: format!("generated-{name}"),
                                        },
                                    },
                                })
                                .await
                                .expect("generated record completed tool");
                            visible.tool_runs += 1;
                            visible.activity_count += 1;
                        }
                        VfsOperation::Flush => {
                            volume.flush().await.expect("generated flush");
                            durable = visible.clone();
                        }
                    }

                    let actual_visible_activity = count_activity(&*volume, false).await;
                    let actual_durable_activity = count_activity(&*volume, true).await;
                    assert_eq!(actual_visible_activity, visible.activity_count);
                    assert_eq!(actual_durable_activity, durable.activity_count);
                    assert!(actual_durable_activity <= actual_visible_activity);

                    visible_activity_history.push(actual_visible_activity);
                    durable_activity_history.push(actual_durable_activity);
                }

                assert_non_decreasing("visible vfs activity count", &visible_activity_history);
                assert_non_decreasing("durable vfs activity count", &durable_activity_history);

                let visible_snapshot = volume
                    .snapshot(SnapshotOptions::default())
                    .await
                    .expect("visible generated vfs snapshot");
                let durable_snapshot = volume
                    .snapshot(SnapshotOptions { durable: true })
                    .await
                    .expect("durable generated vfs snapshot");

                let visible_files = collect_files(visible_snapshot.fs(), config.path_count).await;
                let durable_files = collect_files(durable_snapshot.fs(), config.path_count).await;
                let visible_kv = collect_kv(visible_snapshot.kv(), config.key_count).await;
                let durable_kv = collect_kv(durable_snapshot.kv(), config.key_count).await;
                let visible_tool_runs = visible_snapshot
                    .tools()
                    .recent(None)
                    .await
                    .expect("visible generated tools")
                    .len();
                let durable_tool_runs = durable_snapshot
                    .tools()
                    .recent(None)
                    .await
                    .expect("durable generated tools")
                    .len();

                assert_eq!(visible_files, visible.files);
                assert_eq!(durable_files, durable.files);
                assert_eq!(visible_kv, visible.kv);
                assert_eq!(durable_kv, durable.kv);
                assert_eq!(visible_tool_runs, visible.tool_runs);
                assert_eq!(durable_tool_runs, durable.tool_runs);

                Ok(VfsSimulationOutcome {
                    seed,
                    scenario,
                    visible_files,
                    durable_files,
                    visible_kv,
                    durable_kv,
                    visible_tool_runs,
                    durable_tool_runs,
                    visible_activity_count: *visible_activity_history
                        .last()
                        .expect("visible history always has initial state"),
                    durable_activity_count: *durable_activity_history
                        .last()
                        .expect("durable history always has initial state"),
                })
            }
        })
}

async fn count_activity(volume: &dyn terracedb_vfs::Volume, durable: bool) -> usize {
    volume
        .activity_since(
            LogCursor::beginning(),
            ActivityOptions {
                durable,
                ..Default::default()
            },
        )
        .await
        .expect("open generated activity stream")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect generated activity stream")
        .len()
}

async fn collect_files(
    fs: std::sync::Arc<dyn ReadOnlyVfsFileSystem>,
    path_count: usize,
) -> BTreeMap<String, Vec<u8>> {
    let mut files = BTreeMap::new();
    for index in 0..path_count {
        let path = fixed_path(index);
        if let Some(bytes) = fs.read_file(&path).await.expect("collect generated file") {
            files.insert(path, bytes);
        }
    }
    files
}

async fn collect_kv(
    kv: std::sync::Arc<dyn ReadOnlyVfsKvStore>,
    key_count: usize,
) -> BTreeMap<String, JsonValue> {
    let mut values = BTreeMap::new();
    for index in 0..key_count {
        let key = fixed_key(index);
        if let Some(value) = kv.get_json(&key).await.expect("collect generated kv") {
            values.insert(key, value);
        }
    }
    values
}

fn choose_index(rng: &DeterministicRng, len: usize) -> usize {
    if len == 0 {
        0
    } else {
        (rng.next_u64() as usize) % len
    }
}

fn choose_existing_key(rng: &DeterministicRng, values: Vec<String>) -> String {
    values[choose_index(rng, values.len())].clone()
}

fn random_path(rng: &DeterministicRng, path_count: usize) -> String {
    fixed_path(choose_index(rng, path_count.max(1)))
}

fn random_distinct_path(rng: &DeterministicRng, path_count: usize, current: &str) -> String {
    if path_count <= 1 {
        return current.to_string();
    }
    loop {
        let candidate = random_path(rng, path_count);
        if candidate != current {
            return candidate;
        }
    }
}

fn fixed_path(index: usize) -> String {
    format!("/workspace/file-{index}.txt")
}

fn random_key(rng: &DeterministicRng, key_count: usize) -> String {
    fixed_key(choose_index(rng, key_count.max(1)))
}

fn fixed_key(index: usize) -> String {
    format!("config-{index}")
}

fn make_payload(rng: &DeterministicRng, max_payload_len: usize, salt: usize) -> Vec<u8> {
    let len = ((rng.next_u64() as usize) % max_payload_len.max(1)) + 1;
    (0..len)
        .map(|offset| b'a' + ((salt + offset + choose_index(rng, 26)) % 26) as u8)
        .collect()
}

fn make_json_value(rng: &DeterministicRng, salt: usize) -> JsonValue {
    serde_json::json!({
        "enabled": rng.next_u64().is_multiple_of(2),
        "version": salt as u64,
        "label": format!("cfg-{salt:x}")
    })
}

fn apply_pwrite(target: &mut Vec<u8>, offset: usize, data: &[u8]) {
    if target.len() < offset {
        target.resize(offset, 0);
    }
    let end = offset + data.len();
    if target.len() < end {
        target.resize(end, 0);
    }
    target[offset..end].copy_from_slice(data);
}
