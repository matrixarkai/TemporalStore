// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::state::{BlockIndex, BucketNode};
use crate::engine::storage_bucket_internals::StoredModelKind;
use std::sync::Arc;

fn add_live(shard: &mut ShardState, key: &str, ordinal: usize) {
    let key: Arc<str> = Arc::from(key);
    let routing_bucket = (ordinal % 8) as u32;
    let block = BlockIndex::new(
        StoredModelKind::String,
        ElementEntry::from_parts(1, ordinal as u64 * 64, 64, None, None),
        false,
        false,
    );
    let handle = shard
        .bucket_index
        .bucket_map
        .entry(routing_bucket)
        .or_insert_with(|| BucketNode {
            routing_bucket,
            ..BucketNode::default()
        })
        .insert_block(&key, block.clone(), &mut shard.bucket_index.block_slab_live);
    shard
        .bucket_index
        .insert_object_block_lookup(routing_bucket, handle, &key, &block);
}

#[test]
fn deletion_totals_match_full_reports_across_live_and_missing_dirty_keys() {
    for live in [0, 1, 64, 4096] {
        let mut shard = ShardState::default();
        for ordinal in 0..live {
            let key = format!("live-{ordinal}");
            add_live(&mut shard, &key, ordinal);
            shard.dirty_objects.insert(&key, (ordinal % 8) as u32);
        }
        for ordinal in 0..32 {
            let key = format!("deleted-{ordinal}");
            shard.dirty_objects.insert(&key, (ordinal % 8) as u32);
            shard.dirty_objects.insert(&key, (ordinal % 8) as u32);
        }
        let report = storage_object_lifecycle_report(1, &shard);
        assert_eq!(report.live_block_refs, live as u64);
        assert_eq!(report.delete_marked_object_ids, 32);
        assert_eq!(
            storage_delete_marked_object_count(&shard),
            report.delete_marked_object_ids
        );
        shard.dirty_objects.clear();
        assert_eq!(storage_delete_marked_object_count(&shard), 0);
        assert_eq!(
            storage_object_lifecycle_report(1, &shard).delete_marked_object_ids,
            0
        );
    }
}

#[test]
fn associated_records_keep_a_dirty_logical_key_live() {
    let mut shard = ShardState::default();
    let associated = associated_record_keys("logical-key");
    assert!(associated.len() > 1);
    add_live(&mut shard, &associated[1], 0);
    shard.dirty_objects.insert("logical-key", 0);
    shard.dirty_objects.insert("control_state:missing", 0);
    shard.dirty_objects.insert("other-missing", 0);
    assert!(record_exists(&shard, "logical-key"));
    assert_eq!(storage_delete_marked_object_count(&shard), 2);
    assert_eq!(
        storage_object_lifecycle_report(1, &shard).delete_marked_object_ids,
        2
    );
}

#[test]
fn deletion_count_avoids_block_materialization_and_identity_inversion() {
    let mut shard = ShardState::default();
    for ordinal in 0..8192 {
        add_live(&mut shard, &format!("live-{ordinal}"), ordinal);
    }
    for ordinal in 0..16 {
        shard.dirty_objects.insert(&format!("deleted-{ordinal}"), 0);
    }
    reset_live_block_scan_entries();
    let (full, full_inversions) = state::measure_block_identity_builds_for_test(|| {
        storage_object_lifecycle_report(1, &shard)
    });
    let full_visits = live_block_scan_entries();
    assert!(
        full_visits >= 8192,
        "the full-report control did not walk blocks"
    );
    assert!(full_inversions > 0, "the inversion control did not fire");
    reset_live_block_scan_entries();
    let (count, count_inversions) = state::measure_block_identity_builds_for_test(|| {
        storage_delete_marked_object_count(&shard)
    });
    assert_eq!(count, full.delete_marked_object_ids);
    assert_eq!(count, 16);
    assert_eq!(live_block_scan_entries(), 0);
    assert_eq!(count_inversions, 0);
    println!("live=8192 dirty=16 full_block_visits={full_visits} full_inversions={full_inversions} count_block_visits=0 count_inversions=0");
}
