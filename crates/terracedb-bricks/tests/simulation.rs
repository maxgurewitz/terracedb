use std::time::Duration;

use bytes::Bytes;

use terracedb::{Clock, Rng};
use terracedb_bricks::{
    BlobObjectLayout, BlobPutOptions, BlobStore, BlobStoreFailure, InMemoryBlobStore,
    compute_blob_digest, read_blob_bytes, upload_blob_bytes,
};
use terracedb_simulation::SeededSimulationRunner;

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlobStoreSimulationCapture {
    body: Vec<u8>,
    digest: String,
    object_key: String,
    listed_keys: Vec<String>,
    observed_at: u64,
}

fn run_blob_store_simulation(seed: u64) -> turmoil::Result<BlobStoreSimulationCapture> {
    SeededSimulationRunner::new(seed)
        .with_simulation_duration(Duration::from_millis(25))
        .run_with(move |context| async move {
            let store = InMemoryBlobStore::new();
            let payload =
                format!("blob-sim-{seed:x}-{}", context.rng().next_u64() % 10_000).into_bytes();
            let layout =
                BlobObjectLayout::new(format!("tenant-{seed:04x}"), "sim").expect("layout");
            let digest = compute_blob_digest(&payload);
            let object_key = layout
                .content_addressed_key(&digest)
                .expect("content-addressed key");

            if seed % 2 == 0 {
                store.inject_failure(BlobStoreFailure::lost_put_response(object_key.clone()));
            }

            context
                .clock()
                .sleep(Duration::from_millis((seed % 3) + 1))
                .await;
            let receipt = upload_blob_bytes(
                &store,
                &layout,
                Bytes::from(payload.clone()),
                BlobPutOptions {
                    content_type: Some("application/octet-stream".to_string()),
                    expected_size: Some(payload.len() as u64),
                },
            )
            .await
            .expect("upload blob");

            context
                .clock()
                .sleep(Duration::from_millis((seed % 5) + 1))
                .await;
            let body = read_blob_bytes(&store, receipt.object_key(), Default::default())
                .await
                .expect("read blob");
            let listed_keys = store
                .list_prefix(&layout.object_prefix())
                .await
                .expect("list prefix")
                .into_iter()
                .map(|info| info.key)
                .collect::<Vec<_>>();

            Ok(BlobStoreSimulationCapture {
                body,
                digest: receipt.digest,
                object_key: receipt.object.key,
                listed_keys,
                observed_at: context.clock().now().get(),
            })
        })
}

#[test]
fn blob_store_simulation_replays_same_seed() -> turmoil::Result {
    let first = run_blob_store_simulation(0x4201)?;
    let second = run_blob_store_simulation(0x4201)?;
    assert_eq!(first, second);
    Ok(())
}

#[test]
fn blob_store_simulation_changes_shape_for_different_seeds() -> turmoil::Result {
    let left = run_blob_store_simulation(0x4202)?;
    let right = run_blob_store_simulation(0x4203)?;

    assert_ne!(left.body, right.body);
    assert_ne!(left.object_key, right.object_key);
    assert_ne!(left.digest, right.digest);
    assert_eq!(left.listed_keys, vec![left.object_key.clone()]);
    assert_eq!(right.listed_keys, vec![right.object_key.clone()]);
    Ok(())
}
