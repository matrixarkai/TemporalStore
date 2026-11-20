// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A LIFECYCLE APPLY LISTS THE SHARD'S LIVE BLOCKS ONCE FOR THE OBJECT LIFECYCLE AND THE SAMPLES.
//!
//! The apply reports the object lifecycle and takes four samples of the same blocks. The two used
//! to list the shard separately, under two reads of it; they now share one listing.
//!
//! TWO CLAIMS, EACH WITH A CONTROL.
//!
//!   1. THE REPORT'S OBJECT LIFECYCLE IS THE STANDALONE SNAPSHOT'S, for the shard as the apply left
//!      it. The control is that the figure is not zero, so two empty reports do not agree.
//!   2. NOTHING IS REMEMBERED BETWEEN APPLIES. A key written between two applies moves the second
//!      report's live block count by exactly one, and it still equals the standalone snapshot.
//!
//! The listing count of one apply is pinned in
//! `part1::one_call_walks_the_live_blocks_a_known_number_of_times`.
#![allow(clippy::all)]
use super::*;

fn engine_with_keys(dir: &std::path::Path, count: usize) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..count {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: format!("k-{index:08}"),
                value: vec![b'v'; 96],
            },
        });
        assert!(response.status.ok, "seed write failed: {:?}", response.status);
    }
    engine
}

fn apply(engine: &TemporalEngine) -> StorageLifecycleReport {
    engine.apply_storage_lifecycle(StorageLifecycleRequest {
        shard_id: 1,
        ..StorageLifecycleRequest::default()
    })
}

#[test]
fn the_applys_object_lifecycle_is_the_standalone_snapshot_and_follows_a_write_between_applies() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_with_keys(dir.path(), 400);

    let first = apply(&engine);
    assert_eq!(
        first.object_lifecycle,
        engine.storage_object_lifecycle_snapshot_for_test(1)
    );
    // CONTROL: two default reports would agree.
    assert!(first.object_lifecycle.live_block_refs >= 400);

    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "k-planted".to_string(),
            value: vec![b'w'; 96],
        },
    });
    assert!(response.status.ok, "planted write failed: {:?}", response.status);

    let second = apply(&engine);
    assert_eq!(
        second.object_lifecycle.live_block_refs,
        first.object_lifecycle.live_block_refs + 1,
        "the planted write did not move the second apply's listing"
    );
    assert_eq!(
        second.object_lifecycle,
        engine.storage_object_lifecycle_snapshot_for_test(1)
    );
}