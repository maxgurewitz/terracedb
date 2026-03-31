use std::sync::Arc;

use terracedb::{DbDependencies, StubClock, StubFileSystem, StubObjectStore, StubRng, Timestamp};
use terracedb_sandbox::{
    BashRequest, BashService, DefaultSandboxStore, DeterministicBashService,
    DeterministicTypeScriptService, SandboxConfig, SandboxServices, SandboxStore,
    TERRACE_BASH_SESSION_STATE_PATH, TERRACE_TYPESCRIPT_MIRROR_PATH, TERRACE_TYPESCRIPT_STATE_PATH,
    TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR, TypeCheckRequest, TypeScriptService,
    TypeScriptTranspileRequest,
};
use terracedb_vfs::{CreateOptions, InMemoryVfsStore, VolumeConfig, VolumeId, VolumeStore};

fn sandbox_store(now: u64, seed: u64) -> (InMemoryVfsStore, DefaultSandboxStore<InMemoryVfsStore>) {
    let dependencies = DbDependencies::new(
        Arc::new(StubFileSystem::default()),
        Arc::new(StubObjectStore::default()),
        Arc::new(StubClock::new(Timestamp::new(now))),
        Arc::new(StubRng::seeded(seed)),
    );
    let vfs = InMemoryVfsStore::with_dependencies(dependencies.clone());
    let sandbox = DefaultSandboxStore::new(
        Arc::new(vfs.clone()),
        dependencies.clock,
        SandboxServices::deterministic(),
    );
    (vfs, sandbox)
}

async fn seed_base(store: &InMemoryVfsStore, volume_id: VolumeId) {
    let base = store
        .open_volume(
            VolumeConfig::new(volume_id)
                .with_chunk_size(4096)
                .with_create_if_missing(true),
        )
        .await
        .expect("open base");
    base.fs()
        .write_file(
            "/workspace/src/index.ts",
            br#"import { answer } from "./helper";
export const label: string = "ok";
export const total: number = answer;
"#
            .to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed index");
    base.fs()
        .write_file(
            "/workspace/src/helper.ts",
            b"export const answer: number = 7;\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed helper");
    base.fs()
        .write_file(
            "/workspace/src/bad.ts",
            b"export const broken: number = \"oops\";\n".to_vec(),
            CreateOptions {
                create_parents: true,
                ..Default::default()
            },
        )
        .await
        .expect("seed bad");
}

#[tokio::test]
async fn typescript_service_writes_mirror_cache_and_emit_outputs() {
    let (vfs, sandbox) = sandbox_store(100, 1);
    let base_volume_id = VolumeId::new(0x9000);
    let session_volume_id = VolumeId::new(0x9001);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let ts = DeterministicTypeScriptService::default();

    let first = ts
        .transpile(
            &session,
            TypeScriptTranspileRequest {
                path: "/workspace/src/index.ts".to_string(),
                target: "es2022".to_string(),
                module_kind: "esm".to_string(),
                jsx: None,
            },
        )
        .await
        .expect("transpile");
    assert!(!first.cache_hit);
    let second = ts
        .transpile(
            &session,
            TypeScriptTranspileRequest {
                path: "/workspace/src/index.ts".to_string(),
                target: "es2022".to_string(),
                module_kind: "esm".to_string(),
                jsx: None,
            },
        )
        .await
        .expect("transpile again");
    assert!(second.cache_hit);
    assert_eq!(first.cache_key, second.cache_key);
    assert!(
        session
            .filesystem()
            .read_file(&format!(
                "{TERRACE_TYPESCRIPT_TRANSPILE_CACHE_DIR}/{}.json",
                first.cache_key
            ))
            .await
            .expect("read cache entry")
            .is_some()
    );

    let diagnostics = ts
        .check(
            &session,
            TypeCheckRequest {
                roots: vec!["/workspace/src/bad.ts".to_string()],
                ..Default::default()
            },
        )
        .await
        .expect("check");
    assert_eq!(diagnostics.diagnostics.len(), 1);
    assert_eq!(diagnostics.diagnostics[0].code.as_deref(), Some("TS2322"));

    let emit = ts
        .emit(
            &session,
            TypeCheckRequest {
                roots: vec!["/workspace/src/index.ts".to_string()],
                ..Default::default()
            },
        )
        .await
        .expect("emit");
    assert_eq!(
        emit.emitted_files,
        vec!["/workspace/src/index.js".to_string()]
    );
    let emitted = String::from_utf8(
        session
            .filesystem()
            .read_file("/workspace/src/index.js")
            .await
            .expect("read emit")
            .expect("emitted file"),
    )
    .expect("utf8 emit");
    assert!(emitted.contains("export const label = \"ok\";"));

    assert!(
        session
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_MIRROR_PATH)
            .await
            .expect("read mirror")
            .is_some()
    );
    assert!(
        session
            .filesystem()
            .read_file(TERRACE_TYPESCRIPT_STATE_PATH)
            .await
            .expect("read ts state")
            .is_some()
    );

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tools")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.typescript.transpile".to_string()));
    assert!(tool_names.contains(&"sandbox.typescript.check".to_string()));
    assert!(tool_names.contains(&"sandbox.typescript.emit".to_string()));
}

#[tokio::test]
async fn bash_service_persists_shell_state_and_bridges_npm_and_tsc() {
    let (vfs, sandbox) = sandbox_store(110, 2);
    let base_volume_id = VolumeId::new(0x9010);
    let session_volume_id = VolumeId::new(0x9011);
    seed_base(&vfs, base_volume_id).await;

    let session = sandbox
        .open_session(SandboxConfig::new(base_volume_id, session_volume_id).with_chunk_size(4096))
        .await
        .expect("open session");
    let ts = Arc::new(DeterministicTypeScriptService::default());
    let bash = DeterministicBashService::default().with_typescript_service(ts);

    let first = bash
        .run(
            &session,
            BashRequest {
                command: "mkdir -p src/tmp && cd src/tmp && printf 'hello\\n' > note.txt && pwd"
                    .to_string(),
                cwd: "/workspace".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("bash create");
    assert_eq!(first.cwd, "/workspace/src/tmp");
    assert_eq!(first.stdout, "/workspace/src/tmp\n");

    let second = bash
        .run(
            &session,
            BashRequest {
                command: "cat note.txt".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("bash cat");
    assert_eq!(second.stdout, "hello\n");
    assert_eq!(second.cwd, "/workspace/src/tmp");

    let export = bash
        .run(
            &session,
            BashRequest {
                command: "export NAME=Terrace && echo $NAME".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("bash export");
    assert_eq!(export.stdout, "Terrace\n");
    assert_eq!(export.env.get("NAME").map(String::as_str), Some("Terrace"));

    let npm = bash
        .run(
            &session,
            BashRequest {
                command: "npm install zod lodash".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("bash npm");
    assert!(npm.stdout.contains("installed lodash, zod"));

    let tsc = bash
        .run(
            &session,
            BashRequest {
                command: "tsc check /workspace/src/bad.ts".to_string(),
                ..Default::default()
            },
        )
        .await
        .expect("bash tsc");
    assert_eq!(tsc.exit_code, 2);
    assert!(tsc.stderr.contains("TS2322"));

    assert!(
        session
            .filesystem()
            .read_file(TERRACE_BASH_SESSION_STATE_PATH)
            .await
            .expect("read bash state")
            .is_some()
    );
    let note = String::from_utf8(
        session
            .filesystem()
            .read_file("/workspace/src/tmp/note.txt")
            .await
            .expect("read note")
            .expect("note exists"),
    )
    .expect("utf8 note");
    assert_eq!(note, "hello\n");

    let tool_names = session
        .volume()
        .tools()
        .recent(None)
        .await
        .expect("recent tools")
        .into_iter()
        .map(|run| run.name)
        .collect::<Vec<_>>();
    assert!(tool_names.contains(&"sandbox.bash.exec".to_string()));
    assert!(tool_names.contains(&"sandbox.package.install".to_string()));
    assert!(tool_names.contains(&"sandbox.typescript.check".to_string()));
}
