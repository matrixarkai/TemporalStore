// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::*;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    engine.load_shard(1);
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: (0..128)
            .map(|index| Command::StringSet {
                key: format!("checkpoint-{index}"),
                value: vec![b'x'; 128],
            })
            .collect(),
    });
    assert!(response.status.ok);
    assert!(response.responses.iter().all(|row| row.status.ok));
    engine
}

#[test]
fn checkpoint_capture_rebuilds_exact_state_after_writer_progress() {
    fn assert_send<T: Send>() {}
    assert_send::<ShardCheckpointCapture>();
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let (expected, captured) = {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        for index in 0..4096 {
            shard.wal_resident_blocks.insert(
                index,
                WalResidentBlock {
                    log_id: index + 1,
                    sequence: index + 7,
                },
            );
        }
        shard.context_event_timeline.insert(
            Arc::from("timeline"),
            (0..128).map(|id| (id, id * 3)).collect(),
        );
        shard.live_user_bytes = 123456;
        shard.live_objects_with_logical_size = 128;
        shard.index_flush_countdown = 19;
        shard.bucket_index.bucket_map.dirty_buckets();
        let captured = shard.capture_for_checkpoint();
        assert!(captured.rest.wal_resident_blocks.is_empty());
        assert!(captured.rest.bucket_index.bucket_map.is_empty());
        assert!(captured
            .rest
            .bucket_index
            .object_block_lookup
            .by_model
            .is_empty());
        assert!(captured.rest.dirty_objects.by_key.is_empty());
        assert_eq!(captured.wal.0.len(), shard.wal_resident_blocks.len());
        assert_eq!(
            captured.buckets.nodes.0.len(),
            shard.bucket_index.bucket_map.len()
        );
        (shard.clone(), captured)
    };
    assert!(engine.shards.try_write().is_ok());
    let writer = engine.clone();
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::spawn(move || {
        let result = writer.execute(crate::types::ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "later".into(),
                value: vec![b'y'; 128],
            },
        });
        sent.send(result.status.ok).unwrap();
    });
    assert!(received
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap());
    thread.join().unwrap();
    let actual = captured.finish();
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(&expected).unwrap()
    );
}

#[test]
fn checkpoint_capture_preserves_empty_groups_and_dirty_key_shapes() {
    let mut shard = ShardState::default();
    shard.dirty_objects.by_bucket.insert(1, DirtyKeySet::Empty);
    shard
        .dirty_objects
        .by_bucket
        .insert(2, DirtyKeySet::One(Arc::from("one")));
    shard
        .dirty_objects
        .by_bucket
        .insert(3, DirtyKeySet::Many(Box::new(BTreeSet::new())));
    shard.dirty_objects.by_bucket.insert(
        4,
        DirtyKeySet::Many(Box::new(
            [Arc::from("a"), Arc::from("b")].into_iter().collect(),
        )),
    );
    shard
        .bucket_index
        .object_block_lookup
        .by_model
        .insert(Arc::from("empty"), BTreeMap::new());
    let actual = shard.capture_for_checkpoint().finish();
    assert_eq!(format!("{actual:?}"), format!("{:?}", shard.clone()));
}

#[test]
fn checkpoint_capture_restores_released_buckets_without_changing_live_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let manifest = engine.create_bucket_dump_manifest(1, Vec::new()).unwrap();
    engine.clear_dumped_bucket_dirty_state(1, &manifest);
    let (released, blocks, _) = engine.release_all_releasable_bucket_index_blocks(1);
    assert!(released > 0 && blocks > 0);
    let (mut expected, released_before) = {
        let shards = engine.shards.read().unwrap();
        let shard = &shards[&1];
        let mut expected = shard.clone();
        expected.applied_wal_sequence = Some(shard.checkpoint_wal_sequence().unwrap());
        (expected, shard.bucket_index.released_buckets.clone())
    };
    storage_bucket_internals::restore_released_buckets_in_checkpoint(&mut expected, 1);
    let (actual, _) = engine.capture_shard_checkpoint(1).unwrap();
    assert!(actual.bucket_index.released_buckets.is_empty());
    assert_eq!(
        serde_json::to_value(&actual).unwrap(),
        serde_json::to_value(&expected).unwrap()
    );
    assert_eq!(actual.applied_wal_sequence, expected.applied_wal_sequence);
    assert_eq!(
        engine.shards.read().unwrap()[&1]
            .bucket_index
            .released_buckets,
        released_before
    );
}
