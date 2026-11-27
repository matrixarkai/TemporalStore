// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::state::{measure_block_identity_builds_for_test, ShardState};
use crate::engine::storage_bucket_internals::upsert_bucket_index_block;

fn state(end: u32) -> ShardState {
    let mut shard = ShardState::default();
    shard.set_shard_id(1);
    shard.set_routing_range(0, end);
    shard
}

fn address(offset: u64) -> ElementEntry {
    ElementEntry::from_parts(7, offset * 128, 128, Some(0), Some(0))
}

fn file(shard: &mut ShardState, kind: &str, key: &str, offset: u64) {
    upsert_bucket_index_block(shard, 1, kind, key, None, address(offset), false);
}

#[test]
fn complete_delete_projection_avoids_store_inversion() {
    let mut shard = state(u32::MAX);
    let bucket = block_routing_bucket("victim", 0, u32::MAX);
    let kinds = ["string", "hash", "set", "feature"];
    for (offset, kind) in kinds.iter().enumerate() {
        file(&mut shard, kind, "victim", offset as u64);
    }
    let mut neighbors = Vec::new();
    for index in 0..1_024 {
        let key = format!("neighbor-{index}");
        if block_routing_bucket(&key, 0, u32::MAX) == bucket {
            continue;
        }
        file(&mut shard, "string", &key, 10_000 + index);
        neighbors.push((key, address(10_000 + index)));
    }
    assert!(neighbors.len() >= 1_000);
    assert_eq!(
        shard
            .bucket_index
            .bucket_map
            .get(&bucket)
            .unwrap()
            .block_index
            .len(),
        kinds.len()
    );
    let (removed, builds) = measure_block_identity_builds_for_test(|| {
        crate::engine::mark_bucket_index_object_deleted(&mut shard, "victim")
    });
    assert!(removed);
    assert_eq!(builds, 0, "a fully named bucket must not invert the store");
    let node = shard.bucket_index.bucket_map.get(&bucket).unwrap();
    assert!(node.block_index.is_empty());
    for kind in kinds {
        let id = stable_block_object_id(1, kind, "victim");
        assert!(node.deleted_object_index.contains(&id));
        assert!(node.object_index.contains(&id));
        assert!(shard
            .bucket_index
            .object_block_refs(kind, "victim")
            .is_none());
    }
    for (key, address) in neighbors {
        assert!(shard
            .bucket_index
            .contains_object_block_address("string", &key, None, &address));
    }
}

fn mixed_delete_bucket_case(object_local: bool) {
    let mut shard = state(0);
    file(&mut shard, "hash", "victim", 0);
    file(&mut shard, "hash", "neighbor", 1);
    let victim = stable_block_object_id(1, "hash", "victim");
    let neighbor = stable_block_object_id(1, "hash", "neighbor");
    let bogus = neighbor ^ 1;
    assert_ne!(bogus, victim);
    {
        let mut bucket = shard.bucket_index.bucket_map.get_mut(&0).unwrap();
        bucket.object_index.remove(&neighbor);
        bucket.object_index.insert(bogus);
    }
    crate::engine::disable_object_local_delete_for_test(!object_local);
    let (removed, builds) = measure_block_identity_builds_for_test(|| {
        crate::engine::mark_bucket_index_object_deleted(&mut shard, "victim")
    });
    crate::engine::disable_object_local_delete_for_test(false);
    assert!(removed);
    let bucket = shard.bucket_index.bucket_map.get(&0).unwrap();
    assert_eq!(bucket.block_index.len(), 1);
    assert!(!bucket.object_index.contains(&victim));
    assert!(bucket.deleted_object_index.contains(&victim));
    if object_local {
        // THE GUARANTEE THAT MOVED OUT OF THE DELETE PATH: the delete removes the key's own blocks by
        // handle and does not reconcile the bucket's object list, so a list that had already drifted is
        // left as it was until the load, release, reload, reconstruct or flag-refresh path reconciles.
        assert_eq!(builds, 0, "a delete by handle names no bucket");
        assert!(bucket.object_index.contains(&bogus));
    } else {
        assert_eq!(builds, 1, "mixed buckets require the full projection");
        assert!(bucket.object_index.contains(&neighbor));
        assert!(!bucket.object_index.contains(&bogus));
    }
    assert!(shard.bucket_index.contains_object_block_address(
        "hash",
        "neighbor",
        None,
        &address(1)
    ));
}

#[test]
fn mixed_delete_bucket_retains_full_identity_repair() {
    // The path that names the bucket, used when no object-block lookup is retained.
    mixed_delete_bucket_case(false);
}

#[test]
fn mixed_delete_bucket_by_handle_leaves_a_drifted_object_list_to_the_next_reconcile() {
    mixed_delete_bucket_case(true);
}

fn incomplete_delete_projection_case(object_local: bool) {
    let mut shard = state(0);
    file(&mut shard, "hash", "victim", 0);
    file(&mut shard, "hash", "unnamed", 1);
    shard
        .bucket_index
        .remove_object_from_block_lookup("hash", "unnamed");
    crate::engine::disable_object_local_delete_for_test(!object_local);
    let (removed, builds) = measure_block_identity_builds_for_test(|| {
        crate::engine::mark_bucket_index_object_deleted(&mut shard, "victim")
    });
    crate::engine::disable_object_local_delete_for_test(false);
    assert!(removed);
    assert_eq!(builds, if object_local { 0 } else { 1 });
    let bucket = shard.bucket_index.bucket_map.get(&0).unwrap();
    assert_eq!(bucket.block_index.len(), 1);
    assert!(bucket
        .block_index
        .values()
        .any(|block| block.address == address(1)));
}

#[test]
fn incomplete_delete_projection_preserves_unnamed_blocks() {
    incomplete_delete_projection_case(false);
}

#[test]
fn a_delete_by_handle_preserves_blocks_no_ref_names() {
    incomplete_delete_projection_case(true);
}

fn large_delete_object_case(object_local: bool) {
    let mut shard = state(0);
    let count = crate::engine::MAX_DELETE_IDENTITY_PROJECTION_REFS + 1;
    for offset in 0..count {
        file(&mut shard, "hash", "victim", offset as u64);
    }
    file(&mut shard, "hash", "neighbor", 10_000);
    let keyed_before = crate::engine::state::keyed_block_inversions();
    crate::engine::disable_object_local_delete_for_test(!object_local);
    let (removed, builds) = measure_block_identity_builds_for_test(|| {
        crate::engine::mark_bucket_index_object_deleted(&mut shard, "victim")
    });
    crate::engine::disable_object_local_delete_for_test(false);
    assert!(removed);
    assert_eq!(builds, if object_local { 0 } else { 1 });
    assert_eq!(crate::engine::state::keyed_block_inversions(), keyed_before);
    assert_eq!(
        shard
            .bucket_index
            .bucket_map
            .get(&0)
            .unwrap()
            .block_index
            .len(),
        1
    );
    assert!(shard.bucket_index.contains_object_block_address(
        "hash",
        "neighbor",
        None,
        &address(10_000)
    ));
}

#[test]
fn large_delete_object_skips_the_temporary_keyed_projection() {
    large_delete_object_case(false);
}

#[test]
fn a_delete_by_handle_of_a_large_object_names_no_bucket() {
    large_delete_object_case(true);
}
