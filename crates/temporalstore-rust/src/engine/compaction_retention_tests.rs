// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::engine::storage_bucket_internals::{
    collect_model_live_block_entries, live_block_scan_entries, model_kind_registry_guards,
    ModelKind,
};
use std::collections::{BTreeMap, HashSet};

fn materialized(shard: &ShardState, shard_id: ShardId, target: u64) -> HashSet<u64> {
    collect_model_live_block_entries(shard)
        .into_iter()
        .filter(|entry| entry.address.block_slab_id() != target)
        .map(|entry| stable_block_object_id(shard_id, entry.kind.as_str(), &entry.object_key))
        .collect()
}

#[test]
fn any_unmoved_address_keeps_its_object_across_every_model_kind() {
    assert!(ModelKind::ALL.len() >= 15);
    let mut shard = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    let real = ElementEntry::from_parts(3, 128, 64, Some(0), None);
    let other = ElementEntry::from_parts(4, 128, 64, Some(0), None);
    shard.bucket_index.hashes.insert_elements_for_test(
        "mixed",
        [
            ("moved".into(), real.clone()),
            ("unmoved".into(), other.clone()),
        ]
        .into_iter()
        .collect(),
    );
    shard.features_mut().insert(
        "packed".into(),
        (0..64)
            .map(|i| {
                (
                    i,
                    if i % 2 == 0 {
                        real.clone()
                    } else {
                        other.clone()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>()
            .into(),
    );
    for (key, slab) in [
        ("wal", crate::wal_record::WAL_LOG_SLAB_ID),
        ("hot", crate::engine::HOT_BLOCK_SLAB_ID),
    ] {
        shard.set_string_address(
            key.into(),
            &ElementEntry::from_parts(slab, 256, 64, Some(0), None),
        );
    }
    for shard_id in [1, 17] {
        for target in [
            3,
            4,
            99,
            crate::wal_record::WAL_LOG_SLAB_ID,
            crate::engine::HOT_BLOCK_SLAB_ID,
        ] {
            let before = live_block_scan_entries();
            let expected = materialized(&shard, shard_id, target);
            let old_visits = live_block_scan_entries() - before;
            let before = live_block_scan_entries();
            let actual = retained_objects_outside_slab(&shard, shard_id, target);
            assert_eq!(actual, expected);
            assert_eq!(live_block_scan_entries() - before, old_visits);
            assert!(old_visits >= ModelKind::ALL.len() as u64);
            assert!(actual.contains(&stable_block_object_id(shard_id, "hash", "mixed")));
            assert!(actual.contains(&stable_block_object_id(shard_id, "feature", "packed")));
            if target == 3 {
                assert_eq!(actual.len(), 4);
            }
        }
    }
    assert_ne!(
        retained_objects_outside_slab(&shard, 1, 3),
        retained_objects_outside_slab(&shard, 17, 3)
    );
}

#[test]
fn empty_and_fully_moved_shards_retain_no_objects() {
    assert!(retained_objects_outside_slab(&ShardState::default(), 1, 3).is_empty());
    let shard = model_kind_registry_guards::shard_holding_one_block_of_every_kind();
    assert!(retained_objects_outside_slab(&shard, 1, 3).is_empty());
    assert_eq!(
        retained_objects_outside_slab(&shard, 1, 4).len(),
        ModelKind::ALL.len()
    );
}

#[cfg(feature = "alloc-probe")]
#[test]
fn wide_object_retention_does_not_materialize_its_fields() {
    let mut shard = ShardState::default();
    let count = 8192;
    shard.bucket_index.hashes.insert_elements_for_test(
        "catalog",
        (0..count)
            .map(|i| {
                (
                    format!("field-{i}"),
                    ElementEntry::from_parts(4, i as u64 * 64, 64, Some(0), None),
                )
            })
            .collect(),
    );
    drop(materialized(&shard, 1, 3));
    drop(retained_objects_outside_slab(&shard, 1, 3));
    let probe = crate::alloc_probe::Probe::start();
    let old = materialized(&shard, 1, 3);
    let old_counts = probe.stop();
    let probe = crate::alloc_probe::Probe::start();
    let current = retained_objects_outside_slab(&shard, 1, 3);
    let new_counts = probe.stop();
    assert_eq!(current, old);
    assert_eq!(
        current,
        HashSet::from([stable_block_object_id(1, "hash", "catalog")])
    );
    println!("fields={count} materialized_rust_alloc_bytes={} borrowed_rust_alloc_bytes={} materialized_calls={} borrowed_calls={}",
        old_counts.alloc_bytes, new_counts.alloc_bytes, old_counts.allocs, new_counts.allocs);
    assert!(
        old_counts.alloc_bytes
            >= new_counts.alloc_bytes
                + (count * std::mem::size_of::<storage_bucket_internals::LiveBlockEntry>()) as u64
    );
}
