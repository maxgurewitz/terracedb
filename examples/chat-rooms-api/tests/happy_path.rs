use std::sync::Arc;

use tempfile::tempdir;
use terracedb::{
    DbComponents, DeterministicRng, FailpointMode, LocalDirObjectStore, PhysicalShardId, StubClock,
    TokioFileSystem, VirtualPartitionId,
};
use terracedb_example_chat_rooms_api::{
    ChatRoomsApp, ChatRoomsExampleProfile, CreateRoomRequest, PostMessageRequest,
    chat_rooms_db_settings, find_room_id_for_partition, find_room_id_for_shard,
    room_sharding_config,
};

struct AppFixture {
    _dir: tempfile::TempDir,
    clock: Arc<StubClock>,
    app: ChatRoomsApp,
}

async fn open_app_at(
    data_root: &std::path::Path,
    profile: ChatRoomsExampleProfile,
    clock: Arc<StubClock>,
) -> Result<ChatRoomsApp, Box<dyn std::error::Error>> {
    let deployment = profile.deployment()?;
    let db = deployment
        .open_database(
            terracedb_example_chat_rooms_api::CHAT_ROOMS_DATABASE_NAME,
            chat_rooms_db_settings(data_root.join("ssd").to_string_lossy().as_ref(), "fixture"),
            DbComponents::new(
                Arc::new(TokioFileSystem::new()),
                Arc::new(LocalDirObjectStore::new(data_root.join("object-store"))),
                clock.clone(),
                Arc::new(DeterministicRng::seeded(7)),
            ),
        )
        .await?;
    Ok(ChatRoomsApp::open(deployment, db, profile, clock).await?)
}

async fn open_fixture(
    profile: ChatRoomsExampleProfile,
) -> Result<AppFixture, Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let clock = Arc::new(StubClock::default());
    let app = open_app_at(dir.path(), profile, clock.clone()).await?;
    Ok(AppFixture {
        _dir: dir,
        clock,
        app,
    })
}

#[tokio::test]
async fn room_scoped_operations_restart_and_reshard_cleanly() {
    let fixture = open_fixture(ChatRoomsExampleProfile::DatabaseShared)
        .await
        .expect("open fixture");
    let sharding = room_sharding_config();
    let room_id = find_room_id_for_partition(&sharding, VirtualPartitionId::new(1), "busy-room");
    let shard_zero_room = find_room_id_for_shard(&sharding, PhysicalShardId::new(0), "compat-room");

    let room = fixture
        .app
        .state()
        .create_room(CreateRoomRequest {
            room_id: room_id.clone(),
            title: "Busy room".to_string(),
        })
        .await
        .expect("create hot room");
    assert_eq!(room.message_count, 0);

    for body in ["first", "second", "third", "fourth"] {
        fixture.clock.advance(std::time::Duration::from_millis(1));
        fixture
            .app
            .state()
            .post_message(
                &room_id,
                PostMessageRequest {
                    author: "alice".to_string(),
                    body: body.to_string(),
                },
            )
            .await
            .expect("post room-local message");
    }

    let before = fixture
        .app
        .state()
        .inspect_room_shard(&room_id)
        .await
        .expect("inspect before reshard");
    assert_eq!(before.virtual_partition, 1);
    assert_eq!(before.physical_shard, 0);
    assert_eq!(before.shard_map_revision, 7);

    let recent = fixture
        .app
        .state()
        .read_recent_messages(&room_id, Some(10))
        .await
        .expect("read recent messages");
    assert_eq!(recent.room.message_count, 4);
    assert_eq!(recent.messages.len(), 4);
    assert_eq!(recent.messages[0].body, "fourth");
    assert_eq!(recent.messages[3].body, "first");

    let resharded = fixture
        .app
        .state()
        .reshard_room(
            &room_id,
            terracedb_example_chat_rooms_api::ReshardRoomRequest {
                target_physical_shard: 2,
            },
        )
        .await
        .expect("reshard busy room");
    assert_eq!(resharded.from_physical_shard, 0);
    assert_eq!(resharded.to_physical_shard, 2);
    assert_eq!(resharded.room_state.phase, "completed");
    assert_eq!(resharded.room_messages.phase, "completed");

    fixture.clock.advance(std::time::Duration::from_millis(1));
    fixture
        .app
        .state()
        .post_message(
            &room_id,
            PostMessageRequest {
                author: "alice".to_string(),
                body: "after-reshard".to_string(),
            },
        )
        .await
        .expect("post after reshard");

    let after = fixture
        .app
        .state()
        .inspect_room_shard(&room_id)
        .await
        .expect("inspect after reshard");
    assert_eq!(after.physical_shard, 2);
    assert_eq!(after.shard_map_revision, 8);

    let shard_zero = fixture
        .app
        .state()
        .create_room(CreateRoomRequest {
            room_id: shard_zero_room.clone(),
            title: "Shard zero".to_string(),
        })
        .await
        .expect("create shard-zero room");
    assert_eq!(shard_zero.room_id, shard_zero_room);
    let shard_zero_report = fixture
        .app
        .state()
        .inspect_room_shard(&shard_zero_room)
        .await
        .expect("inspect shard-zero room");
    let observability = fixture
        .app
        .state()
        .observability_report()
        .await
        .expect("observability report");
    assert_eq!(
        shard_zero_report.active_background_domain, observability.database_background_domain,
        "shard 0000 should stay compatible with the database-wide lane",
    );

    let data_root = fixture._dir.path().to_path_buf();
    let clock = fixture.clock.clone();
    fixture.app.shutdown().await.expect("shutdown fixture");

    let reopened = open_app_at(&data_root, ChatRoomsExampleProfile::DatabaseShared, clock)
        .await
        .expect("reopen fixture");
    let reopened_messages = reopened
        .state()
        .read_recent_messages(&room_id, Some(10))
        .await
        .expect("read messages after reopen");
    assert_eq!(reopened_messages.room.message_count, 5);
    assert_eq!(reopened_messages.messages[0].body, "after-reshard");
    assert_eq!(reopened_messages.messages[4].body, "first");
    let reopened_shard = reopened
        .state()
        .inspect_room_shard(&room_id)
        .await
        .expect("inspect after reopen");
    assert_eq!(reopened_shard.physical_shard, 2);
    assert_eq!(reopened_shard.shard_map_revision, 8);
    reopened.shutdown().await.expect("shutdown reopened app");
}

#[tokio::test]
async fn conservative_cutover_temporarily_blocks_room_messages_until_resume() {
    let fixture = open_fixture(ChatRoomsExampleProfile::DatabaseShared)
        .await
        .expect("open fixture");
    let room_id = find_room_id_for_partition(
        &room_sharding_config(),
        VirtualPartitionId::new(1),
        "cutover-room",
    );
    fixture
        .app
        .state()
        .create_room(CreateRoomRequest {
            room_id: room_id.clone(),
            title: "Cutover".to_string(),
        })
        .await
        .expect("create room");
    fixture
        .app
        .state()
        .post_message(
            &room_id,
            PostMessageRequest {
                author: "bob".to_string(),
                body: "before-cutover".to_string(),
            },
        )
        .await
        .expect("post before cutover");
    fixture
        .app
        .state()
        .db()
        .flush()
        .await
        .expect("flush before reshard");

    let target = fixture
        .app
        .state()
        .target_sharding_for_room(&room_id, 2)
        .expect("target sharding");
    let _fail = fixture.app.state().db().__failpoint_registry().arm_timeout(
        terracedb::failpoints::names::DB_RESHARD_MANIFEST_INSTALLED,
        "pause room_state reshard after manifest install",
        FailpointMode::Once,
    );
    fixture
        .app
        .state()
        .tables()
        .room_state_raw()
        .reshard_to(target.clone())
        .await
        .expect_err("room_state reshard should stop at the injected cut point");

    let blocked = fixture
        .app
        .state()
        .post_message(
            &room_id,
            PostMessageRequest {
                author: "bob".to_string(),
                body: "blocked-during-cutover".to_string(),
            },
        )
        .await
        .expect_err("room writes should fail closed while one table is paused");
    assert!(
        blocked.to_string().contains("paused for resharding")
            || blocked
                .to_string()
                .contains("temporarily unavailable while shard maps converge"),
        "unexpected error: {blocked}",
    );

    fixture
        .app
        .state()
        .tables()
        .room_state_raw()
        .run_reshard_plan()
        .await
        .expect("resume room_state cutover");
    fixture
        .app
        .state()
        .tables()
        .room_messages_raw()
        .reshard_to(target)
        .await
        .expect("reshard room_messages after state table");

    fixture
        .app
        .state()
        .post_message(
            &room_id,
            PostMessageRequest {
                author: "bob".to_string(),
                body: "after-cutover".to_string(),
            },
        )
        .await
        .expect("writes should resume after both tables converge");

    let after = fixture
        .app
        .state()
        .inspect_room_shard(&room_id)
        .await
        .expect("inspect after convergence");
    assert_eq!(after.physical_shard, 2);
    assert_eq!(after.shard_map_revision, 8);
    let recent = fixture
        .app
        .state()
        .read_recent_messages(&room_id, Some(10))
        .await
        .expect("read messages after resumed cutover");
    assert_eq!(recent.room.message_count, 2);
    assert_eq!(recent.messages[0].body, "after-cutover");

    fixture.app.shutdown().await.expect("shutdown fixture");
}
