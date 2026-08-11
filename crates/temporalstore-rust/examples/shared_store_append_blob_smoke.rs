// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use matrixobjectstore_rs::StoreOptions;
use temporalstore_rust::shared_store::{
    ReplayReport, SharedStoreReplicator, SharedStoreWalAppendMode, SharedStoreWalEntry,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::ObjectStore;

fn test_engine(root: &std::path::Path, role: &str) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1024,
        root.join(format!("{role}-cache")),
        root.join(format!("{role}-pages")),
        root.join(format!("{role}-index")),
    )
}

#[tokio::main]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(
        MatrixObjectObjectStore::new(
            "temporalstore-shared",
            StoreOptions {
                segment_size: 16,
                max_extent_bytes: 4,
                chunk_size: 4,
                ..StoreOptions::default()
            },
        )
        .unwrap(),
    );
    let replicator = SharedStoreReplicator::new("cluster-a", store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    for (oplog_index, key, value) in [
        (1, "proto-a", b"one".to_vec()),
        (2, "proto-b", b"two".to_vec()),
    ] {
        replicator
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                oplog_index,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            })
            .await
            .unwrap();
    }

    let blob_key = "cluster-a/shards/1/shared/oplog/oplog.protobuf.blob";
    assert_eq!(
        store
            .list("cluster-a/shards/1/shared/oplog/")
            .await
            .unwrap(),
        vec![blob_key.to_string()]
    );
    let matrixobject_blob = store
        .inner()
        .lock()
        .expect("matrixobject lock poisoned")
        .get_object("temporalstore-shared", blob_key)
        .unwrap();
    assert!(matrixobject_blob.metadata.extents.len() > 1);

    let restarted = SharedStoreReplicator::new("cluster-a", store)
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    let follower = test_engine(dir.path(), "follower");
    follower.load_shard(1);
    assert_eq!(
        restarted.replay_wal_strict(1, 0, &follower).await.unwrap(),
        ReplayReport {
            applied: 2,
            last_oplog_index: 2,
            offset_index_reads: 0,
            range_bytes_read: 0,
        }
    );
    for (key, value) in [("proto-a", b"one".to_vec()), ("proto-b", b"two".to_vec())] {
        assert_eq!(
            follower
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: Some(value) }
        );
    }
}
