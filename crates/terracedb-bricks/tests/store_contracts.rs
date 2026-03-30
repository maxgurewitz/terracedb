use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, stream};

use terracedb::{StorageErrorKind, StubObjectStore};
use terracedb_bricks::{
    BlobByteRange, BlobGetOptions, BlobObjectLayout, BlobPutOptions, BlobStore,
    BlobStoreByteStream, BlobStoreFailure, BlobStoreObjectStoreAdapter, BlobStoreOperation,
    BlobStoreRecoveryHint, BlobStoreStandardObjectStoreAdapter, compute_blob_digest,
    read_blob_bytes, upload_blob_bytes,
};

fn byte_stream(chunks: Vec<&'static [u8]>) -> BlobStoreByteStream {
    Box::pin(stream::iter(
        chunks
            .into_iter()
            .map(|chunk| Ok(Bytes::from_static(chunk))),
    ))
}

async fn exercise_store_contract(store: &dyn BlobStore) {
    let key = "blobs/docs/contracts/blob.txt";
    let body = b"hello terracedb bricks";

    let written = store
        .put(
            key,
            byte_stream(vec![b"hello ", b"terracedb ", b"bricks"]),
            BlobPutOptions {
                content_type: Some("text/plain".to_string()),
                expected_size: Some(body.len() as u64),
            },
        )
        .await
        .expect("put blob");
    assert_eq!(written.key, key);
    assert_eq!(written.size_bytes, body.len() as u64);
    assert_eq!(written.content_type.as_deref(), Some("text/plain"));

    let full = read_blob_bytes(store, key, BlobGetOptions::default())
        .await
        .expect("read full blob");
    assert_eq!(full, body);

    let range = read_blob_bytes(
        store,
        key,
        BlobGetOptions {
            range: Some(BlobByteRange::new(6, 15).expect("range")),
        },
    )
    .await
    .expect("read range");
    assert_eq!(range, b"terracedb");

    let listed = store
        .list_prefix("blobs/docs/contracts/")
        .await
        .expect("list prefix");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, key);

    let stat = store
        .stat(key)
        .await
        .expect("stat")
        .expect("object should exist");
    assert_eq!(stat.size_bytes, body.len() as u64);

    store.delete(key).await.expect("delete");
    assert!(store.stat(key).await.expect("stat after delete").is_none());
}

#[tokio::test]
async fn compatibility_adapters_cover_store_contracts() {
    let compatible = BlobStoreObjectStoreAdapter::new(Arc::new(StubObjectStore::default()));
    exercise_store_contract(&compatible).await;

    let standard = BlobStoreStandardObjectStoreAdapter::new(Arc::new(StubObjectStore::default()))
        .with_multipart_chunk_bytes(4);
    exercise_store_contract(&standard).await;
}

#[tokio::test]
async fn upload_helper_uses_content_addressed_keys_and_recovers_lost_responses() {
    let store = terracedb_bricks::InMemoryBlobStore::new();
    let layout = BlobObjectLayout::new("/tenant-a/db-01/", "photos").expect("layout");
    let payload = Bytes::from_static(b"blob payload");
    let digest = compute_blob_digest(payload.as_ref());
    let object_key = layout
        .content_addressed_key(&digest)
        .expect("content-addressed key");

    store.inject_failure(BlobStoreFailure::lost_put_response(object_key.clone()));

    let receipt = upload_blob_bytes(
        &store,
        &layout,
        payload.clone(),
        BlobPutOptions {
            content_type: Some("application/octet-stream".to_string()),
            expected_size: Some(payload.len() as u64),
        },
    )
    .await
    .expect("upload should recover via stat");

    assert_eq!(receipt.digest, digest);
    assert_eq!(receipt.object.key, object_key);
    assert!(receipt.object.key.contains("/blobs/photos/objects/sha256/"));
    assert_eq!(receipt.object.size_bytes, payload.len() as u64);
}

#[tokio::test]
async fn blob_store_failures_expose_retry_and_fail_closed_hints() {
    let store = terracedb_bricks::InMemoryBlobStore::new();
    let key = "blobs/docs/failures/blob.bin";
    store
        .put(
            key,
            byte_stream(vec![b"abcdefghijk"]),
            BlobPutOptions::default(),
        )
        .await
        .expect("seed object");

    store.inject_failure(BlobStoreFailure::timeout(BlobStoreOperation::Get, key));
    let timeout = match store.get(key, BlobGetOptions::default()).await {
        Ok(_) => panic!("timeout get should fail"),
        Err(error) => error,
    };
    assert_eq!(timeout.kind(), Some(StorageErrorKind::Timeout));
    assert_eq!(
        timeout.recovery_hint(BlobStoreOperation::Get),
        BlobStoreRecoveryHint::Retry
    );

    store.inject_failure(BlobStoreFailure::interrupted_read(
        BlobStoreOperation::Get,
        key,
        4,
    ));
    let mut stream = store
        .get(key, BlobGetOptions::default())
        .await
        .expect("interrupted read stream");
    let first = stream
        .next()
        .await
        .expect("first read event")
        .expect("first bytes");
    assert_eq!(first.as_ref(), b"abcd");
    let interrupted = stream
        .next()
        .await
        .expect("interrupted read event")
        .expect_err("interrupted read should fail closed");
    assert_eq!(interrupted.kind(), Some(StorageErrorKind::Corruption));
    assert_eq!(
        interrupted.recovery_hint(BlobStoreOperation::Get),
        BlobStoreRecoveryHint::FailClosed
    );

    store.inject_failure(BlobStoreFailure::stale_list("blobs/docs/"));
    let stale = store
        .list_prefix("blobs/docs/")
        .await
        .expect_err("stale list should surface durability boundary");
    assert_eq!(stale.kind(), Some(StorageErrorKind::DurabilityBoundary));
    assert_eq!(
        stale.recovery_hint(BlobStoreOperation::List),
        BlobStoreRecoveryHint::RefreshListing
    );
}
