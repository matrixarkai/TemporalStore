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
                // A PAGE SITS BELOW A CHUNK, so `page_size` must be < `chunk_size`; the old
                // `slab_size` was its own tier and carried no such relation. Kept tiny so the
                // blob still spans several extents, which is what this example asserts.
                page_size: 4,
                max_extent_bytes: 4,
                chunk_size: 16,
                ..StoreOptions::default()
            },
        )
        .unwrap(),
    );
    let replicator = SharedStoreReplicator::new("cluster-a", store.clone())
        .with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);

    for (wal_index, key, value) in [
        (1, "proto-a", b"one".to_vec()),
        (2, "proto-b", b"two".to_vec()),
    ] {
        replicator
            // THESE TWO FIELDS ARE NOT OPTIONAL TO SUPPLY, ONLY OPTIONAL TO CARRY. A shared-store
            // WAL entry gained `staged_blocks` (pages a command cannot rebuild) and `outcomes`
            // (what the write DID, so a successor installs results instead of re-running), and
            // `command` became `Option` because an entry carrying outcomes has no need of it.
            // Both new fields are `serde(default)`, so an older entry still decodes -- but a
            // literal in Rust has to name every field, and this example had not been compiled
            // since they arrived, because nothing builds the matrixobject feature.
            .publish_wal_entry(SharedStoreWalEntry {
                shard_id: 1,
                wal_index,
                command: Some(Command::StringSet {
                    key: key.to_string(),
                    value,
                }),
                staged_blocks: Vec::new(),
                outcomes: Vec::new(),
            })
            .await
            .unwrap();
    }

    let blob_key = "cluster-a/shards/1/shared/wal/wal.protobuf.blob";
    let offset_index_key = "cluster-a/shards/1/shared/wal/wal.offset_index.protobuf.blob";
    // THE APPEND PATH WRITES AN OFFSET INDEX BESIDE THE BLOB. This asserted the blob ALONE, which
    // was true when it was written and has not been since -- another thing an uncompiled feature
    // hid. Both are named rather than the assertion loosened, so a layer going missing still fails.
    assert_eq!(
        store
            .list("cluster-a/shards/1/shared/wal/")
            .await
            .unwrap(),
        vec![offset_index_key.to_string(), blob_key.to_string()]
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
        // THE REPLAY READS THE OFFSET INDEX NOW, where this expected it to read nothing. The
        // third stale expectation in this one example, and all three have the same cause: the
        // `matrixobject` feature was an inert stub, so nothing compiled or ran this, and each
        // layer added since drifted past it unnoticed.
        //
        // Pinned exactly rather than loosened to `>= 0`: these two figures are what the indexed
        // read path is FOR, and a range read that stopped happening should fail here.
        ReplayReport {
            applied: 2,
            last_wal_index: 2,
            offset_index_reads: 2,
            range_bytes_read: 192,
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
