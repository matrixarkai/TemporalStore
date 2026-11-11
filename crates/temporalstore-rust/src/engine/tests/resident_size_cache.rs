// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use crate::block_store::ElementEntry;
use crate::engine::state::{BlockIndex, BlockIndexMap, BucketNode, ShardState};
use crate::engine::storage_bucket_internals::{
    bucket_index_resident_bytes, bucket_index_resident_bytes_visits,
    reset_bucket_index_resident_bytes_visits, StoredModelKind,
};

fn node(bucket: u32, count: usize, capacity: usize) -> BucketNode {
    let mut node = BucketNode {
        routing_bucket: bucket,
        ..BucketNode::default()
    };
    let mut blocks = Vec::with_capacity(capacity.max(count));
    for index in 0..count {
        blocks.push((
            index as u64,
            BlockIndex::new(
                StoredModelKind::String,
                ElementEntry::from_parts(3, index as u64 * 64, 64, None, None),
                false,
                false,
            ),
        ));
    }
    node.block_index = match count {
        0 => BlockIndexMap::Empty,
        1 => BlockIndexMap::One(0, Box::new(blocks.pop().unwrap().1)),
        _ => BlockIndexMap::Many(blocks),
    };
    node
}

fn fixture(count: u32) -> ShardState {
    let mut shard = ShardState::default();
    for bucket in 0..count {
        shard
            .bucket_index
            .bucket_map
            .insert(bucket, node(bucket, 3, 16));
    }
    shard
}

fn by_walk(shard: &ShardState) -> u64 {
    shard.bucket_index.bucket_map.values().fold(
        (shard.bucket_index.bucket_map.len() * std::mem::size_of::<BucketNode>()) as u64,
        |bytes, node| bytes.saturating_add(node.block_index.resident_heap_bytes()),
    )
}

fn assert_exact(shard: &ShardState) {
    let expected = by_walk(shard);
    assert_eq!(bucket_index_resident_bytes(shard), expected);
    reset_bucket_index_resident_bytes_visits();
    assert_eq!(bucket_index_resident_bytes(shard), expected);
    assert_eq!(
        bucket_index_resident_bytes_visits(),
        0,
        "unchanged read walked the index"
    );
}

#[test]
fn unchanged_resident_size_reads_do_not_revisit_the_bucket_index() {
    for count in [32, 512] {
        let shard = fixture(count);
        reset_bucket_index_resident_bytes_visits();
        let expected = by_walk(&shard);
        assert_eq!(bucket_index_resident_bytes(&shard), expected);
        assert_eq!(bucket_index_resident_bytes_visits(), count as u64);
        reset_bucket_index_resident_bytes_visits();
        for _ in 0..32 {
            assert_eq!(bucket_index_resident_bytes(&shard), expected);
        }
        assert_eq!(bucket_index_resident_bytes_visits(), 0);
    }
}

#[test]
fn resident_size_stays_exact_through_every_bucket_mutation_door() {
    for held in [false, true] {
        let mut shard = fixture(8);
        if held {
            assert_exact(&shard);
        }
        shard
            .bucket_index
            .bucket_map
            .get_mut(&1)
            .unwrap()
            .block_index = BlockIndexMap::Empty;
        assert_exact(&shard);
        shard
            .bucket_index
            .bucket_map
            .get_mut(&2)
            .unwrap()
            .block_index = node(2, 1, 1).block_index;
        assert_exact(&shard);
        {
            let mut bucket = shard.bucket_index.bucket_map.get_mut(&3).unwrap();
            bucket.set_dirty(true);
            bucket.block_index = node(3, 7, 128).block_index;
            bucket.set_dirty(false);
        }
        assert_exact(&shard);
        shard.bucket_index.bucket_map.insert(4, node(4, 9, 256));
        assert_exact(&shard);
        shard.bucket_index.bucket_map.insert(90, node(90, 1, 1));
        assert_exact(&shard);
        shard
            .bucket_index
            .bucket_map
            .entry(91)
            .or_insert_with(|| node(91, 4, 64));
        assert_exact(&shard);
        shard
            .bucket_index
            .bucket_map
            .entry(91)
            .or_insert_with(|| node(91, 1, 1))
            .block_index = BlockIndexMap::Empty;
        assert_exact(&shard);
        shard.bucket_index.bucket_map.remove(&4);
        shard.bucket_index.bucket_map.remove(&999);
        assert_exact(&shard);
        shard.bucket_index.bucket_map.clear();
        assert_exact(&shard);
    }
}

#[test]
fn bulk_mutations_invalidate_then_rebuild_resident_size_once() {
    let mut shard = fixture(8);
    assert_exact(&shard);
    for bucket in shard.bucket_index.bucket_map.values_mut() {
        bucket.block_index = node(bucket.routing_bucket, 5, 128).block_index;
    }
    assert_exact(&shard);
    for (key, bucket) in shard.bucket_index.bucket_map.iter_mut() {
        bucket.block_index = node(*key, 2, 16).block_index;
    }
    assert_exact(&shard);
    shard.bucket_index.bucket_map.retain(|key, bucket| {
        bucket.block_index = node(*key, 3, 64).block_index;
        key % 2 == 0
    });
    assert_exact(&shard);
}

#[test]
fn cloned_maps_measure_their_own_buffer_capacities() {
    let shard = fixture(8);
    assert_exact(&shard);
    let mut copy = ShardState::default();
    copy.bucket_index.bucket_map = shard.bucket_index.bucket_map.clone();
    assert!(
        by_walk(&copy) < by_walk(&shard),
        "fixture must have capacity slack"
    );
    assert_exact(&copy);
    copy.bucket_index
        .bucket_map
        .get_mut(&0)
        .unwrap()
        .block_index = BlockIndexMap::Empty;
    assert_exact(&copy);
    assert_exact(&shard);
}

#[test]
fn unwinding_a_guard_keeps_resident_accounting_exact() {
    let mut shard = fixture(8);
    assert_exact(&shard);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut bucket = shard.bucket_index.bucket_map.get_mut(&0).unwrap();
        bucket.block_index = node(0, 9, 128).block_index;
        panic!("guard unwind control");
    }));
    assert!(result.is_err());
    assert_exact(&shard);
}

#[test]
fn warm_eviction_pressure_checks_preserve_the_index_wire_image_without_a_census() {
    use crate::{Command, ExecuteRequest, TemporalEngine};
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        65536,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        dir.path().join("indexes"),
    );
    engine.load_shard(1);
    for index in 0..256 {
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: format!("resident-{index}"),
                        value: vec![index as u8; 16],
                    },
                })
                .status
                .ok
        );
    }
    let wire_before = {
        let mut shards = engine.shards.write().unwrap();
        let shard = shards.get_mut(&1).unwrap();
        let _ = shard.bucket_index.bucket_map.values_mut();
        serde_json::to_vec(&shard.bucket_index).unwrap()
    };
    reset_bucket_index_resident_bytes_visits();
    let bytes = engine.bucket_index_resident_bytes(1);
    assert!(bytes > 0);
    assert!(
        bucket_index_resident_bytes_visits() > 0,
        "cold-cache control did not run"
    );
    reset_bucket_index_resident_bytes_visits();
    for _ in 0..32 {
        let report = engine.apply_storage_eviction(1, u64::MAX, 16, false, false);
        assert!(!report.pressure_gate_open);
        assert_eq!(report.pressure_before, bytes);
        assert_eq!(report.pressure_after, bytes);
    }
    assert_eq!(bucket_index_resident_bytes_visits(), 0);
    let shards = engine.shards.read().unwrap();
    let wire_after = serde_json::to_vec(&shards.get(&1).unwrap().bucket_index).unwrap();
    assert_eq!(wire_before, wire_after);
}
