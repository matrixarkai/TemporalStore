// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

const SHARD: u64 = 17;
const KEYS: usize = 24;

fn open(dir: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"), dir.join("blocks"), dir.join("indexes"),
    );
    let response = engine.load_shard_with(LoadShardRequest {
        shard_id: SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    assert!(response.status.ok, "load failed: {:?}", response.status);
    engine
}

fn key(item: usize) -> String {
    format!("released-checkpoint-{item:03}")
}

fn block_count(shard: &ShardState) -> usize {
    shard.bucket_index.bucket_map.values().map(|bucket| bucket.block_index.len()).sum()
}

fn round_trip(stamped: bool) {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    for item in 0..KEYS {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet { key: key(item), value: vec![item as u8; 128] },
        });
        assert!(response.status.ok, "write failed: {:?}", response.status);
    }
    let manifest = engine.create_bucket_dump_manifest(SHARD, Vec::<u32>::new()).unwrap();
    engine.clear_dumped_bucket_dirty_state(SHARD, &manifest);
    {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&SHARD).unwrap();
        let buckets: Vec<_> = shard.bucket_index.bucket_map.keys().copied().collect();
        let release = storage_bucket_internals::release_bucket_blocks(shard, &buckets);
        assert_eq!(release.released_blocks, KEYS, "the fixture must really release its blocks");
        assert_eq!(block_count(shard), 0);
    }
    let bytes = {
        let shards = engine.shards.read().unwrap();
        let shard = shards.get(&SHARD).unwrap();
        // The raw encoder reproduces the incomplete image a checkpoint must not publish.
        let incomplete = decode_index_bytes(&encode_index_bytes(shard)).unwrap();
        assert_eq!(block_count(&incomplete), 0);
        let bytes = if stamped {
            let mut snapshot = shard.clone();
            serialize_index_stamped(&mut snapshot)
        } else {
            serialize_index(shard)
        };
        assert_eq!(block_count(shard), 0, "serialization must not undo live eviction");
        assert_eq!(shard.bucket_index.released_buckets.len(), KEYS);
        bytes
    };
    let complete = decode_index_bytes(&bytes).unwrap();
    assert_eq!(complete.strings().len(), KEYS);
    assert_eq!(block_count(&complete), KEYS);
    engine.persist_index_bytes(SHARD, &bytes).unwrap();
    drop(engine);
    let reopened = open(dir.path());
    for item in 0..KEYS {
        let response = reopened.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet { key: key(item) },
        });
        assert!(response.status.ok, "read failed: {:?}", response.status);
        assert_eq!(response.response, CommandResponse::Bytes { value: Some(vec![item as u8; 128]) });
    }
}

#[test]
fn borrowed_checkpoint_keeps_released_records_without_repopulating_live_buckets() {
    round_trip(false);
}

#[test]
fn stamped_checkpoint_keeps_released_records_without_repopulating_live_buckets() {
    round_trip(true);
}
