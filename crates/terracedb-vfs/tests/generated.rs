use terracedb_fuzz::{
    SeedCampaign, VfsGeneratedScenario, VfsScenarioConfig, VfsScenarioHarness, assert_seed_replays,
    assert_seed_variation, decode_json_artifact, encode_json_artifact, run_campaign,
};

#[test]
fn generated_vfs_scenarios_replay_and_round_trip_through_artifacts() -> turmoil::Result {
    let harness = VfsScenarioHarness::new(VfsScenarioConfig::default());
    let replay = assert_seed_replays(&harness, 0x3901)?;

    let encoded = encode_json_artifact(&replay.scenario).expect("encode generated vfs scenario");
    let decoded: VfsGeneratedScenario =
        decode_json_artifact(&encoded).expect("decode generated vfs scenario");

    assert_eq!(decoded, replay.scenario);
    assert!(replay.outcome.visible_activity_count >= replay.outcome.durable_activity_count);
    assert!(replay.outcome.visible_files.len() >= replay.outcome.durable_files.len());
    Ok(())
}

#[test]
fn generated_vfs_scenarios_vary_across_seeds() -> turmoil::Result {
    let harness = VfsScenarioHarness::new(VfsScenarioConfig {
        steps: 24,
        ..Default::default()
    });
    let _ = assert_seed_variation(&harness, 0x3902, 0x3903, |left, right| {
        left.scenario != right.scenario || left.outcome != right.outcome
    })?;
    Ok(())
}

#[test]
fn generated_vfs_smoke_campaign_is_replayable() -> turmoil::Result {
    let harness = VfsScenarioHarness::new(VfsScenarioConfig {
        steps: 12,
        ..Default::default()
    });
    let campaign = SeedCampaign::smoke();
    let first = run_campaign(&harness, &campaign)?;
    let second = run_campaign(&harness, &campaign)?;

    assert_eq!(first, second);
    assert!(
        first
            .iter()
            .all(|capture| !capture.scenario.workload.is_empty())
    );
    Ok(())
}
