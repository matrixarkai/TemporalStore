// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(path: &std::path::Path) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        path.join("cache"),
        path.join("blocks"),
        path.join("indexes"),
    );
    engine.load_shard(1);
    engine
}

#[test]
fn selected_summaries_equal_the_complete_inspection_including_legacy_selectors() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    for index in 0..128 {
        engine.cache.put_memory_only(
            CacheKey::page_with_slot(1, 7, index * 16, 16, Some((index % 7) as u32)),
            vec![7; 16],
        );
    }
    for (shard, selector) in [
        (1, "slot-03:legacy"),
        (1, "slot-+3:legacy"),
        (1, "slot-3"),
        (1, "slot-bad:x"),
        (2, "slot-3:x"),
    ] {
        engine.cache.put_memory_only(
            CacheKey {
                shard_id: shard,
                namespace: "hash".into(),
                record_key: "legacy".into(),
                selector: selector.to_owned().into(),
            },
            vec![3; 11],
        );
    }
    let pinned = CacheKey::page_with_slot(1, 7, 3 * 16, 16, Some(3));
    engine.cache.pin(pinned.clone());
    let full = engine.storage_cache_inspection_report(1);
    let wanted = BTreeSet::from([3, 6]);
    let expected = full
        .bucket_summaries
        .into_iter()
        .filter(|summary| wanted.contains(&summary.routing_bucket))
        .map(|summary| (summary.routing_bucket, summary))
        .collect::<BTreeMap<_, _>>();
    assert!(expected[&3].pinned_entries > 0);
    reset_storage_cache_inspection_listings();
    assert_eq!(
        engine.storage_cache_summaries_for_buckets(1, &wanted),
        expected
    );
    assert_eq!(storage_cache_inspection_listings(), 1);
    assert_eq!(
        storage_cache_inspection_listing_entries(),
        expected
            .values()
            .map(|summary| summary.entry_count as u64)
            .sum::<u64>()
    );
    reset_storage_cache_inspection_listings();
    assert!(engine
        .storage_cache_summaries_for_buckets(1, &BTreeSet::new())
        .is_empty());
    assert_eq!(storage_cache_inspection_listings(), 0);
    engine.cache.unpin(&pinned);
}

#[test]
fn sampled_index_eviction_keeps_cache_and_explicit_deletion_inspects_victims() {
    for delete_drop in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let engine = open(directory.path());
        for index in 0..128 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("candidate-{index:03}"),
                    value: vec![index as u8; 32],
                },
            });
            assert!(response.status.ok);
            let warmed = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("candidate-{index:03}"),
                },
            });
            assert_eq!(
                warmed.response,
                CommandResponse::Bytes {
                    value: Some(vec![index as u8; 32])
                }
            );
        }
        engine.use_sampled_eviction_for_test();
        let full = engine.storage_cache_inspection_report(1);
        assert!(
            full.entries.len() > 16,
            "the fixture must hold unrelated cache entries"
        );
        let invalidations_before = engine.cache.stats().invalidations;
        let summaries = full
            .bucket_summaries
            .into_iter()
            .map(|summary| (summary.routing_bucket, summary))
            .collect::<BTreeMap<_, _>>();
        reset_storage_cache_inspection_listings();
        let report = engine.apply_storage_eviction(1, 0, 4, false, delete_drop);
        assert_eq!(report.selected_victims.len(), 4);
        let mut expected_entries = 0u64;
        for victim in &report.selected_victims {
            let cache = if delete_drop {
                summaries
                    .get(&victim.routing_bucket)
                    .cloned()
                    .unwrap_or_default()
            } else {
                StorageCacheBucketSummary::default()
            };
            expected_entries += cache.entry_count as u64;
            assert_eq!(victim.cache_memory_bytes, cache.memory_bytes);
            assert_eq!(victim.cache_disk_bytes, cache.disk_bytes);
            assert_eq!(
                victim.weight,
                cache
                    .memory_bytes
                    .saturating_mul(4)
                    .saturating_add(cache.disk_bytes.saturating_mul(2))
                    .saturating_add(victim.physical_bytes)
                    .saturating_add(victim.dirty_object_count.saturating_mul(1024))
            );
        }
        if delete_drop {
            assert!(expected_entries > 0 && expected_entries < full.entries.len() as u64);
            assert_eq!(storage_cache_inspection_listings(), 1);
            assert!(report.dropped_object_count > 0);
            assert!(report.cache_entries_removed > 0);
        } else {
            assert_eq!(storage_cache_inspection_listings(), 0);
            assert_eq!(report.cache_entries_removed, 0);
            assert_eq!(report.cache_disk_bytes_removed, 0);
            assert_eq!(report.bucket_index_buckets_released, 4);
            assert!(report.bucket_index_bytes_after < report.bucket_index_bytes_before);
            assert_eq!(engine.cache.stats().invalidations, invalidations_before);
            assert_eq!(
                engine.cache.entries_for_shard(1),
                full.entries,
                "index release must retain the actual cached pages"
            );
        }
        assert_eq!(storage_cache_inspection_listing_entries(), expected_entries);
        for index in 0..128 {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: format!("candidate-{index:03}"),
                },
            });
            assert!(response.status.ok);
            let deleted = delete_drop
                && report.selected_victims.iter().any(|victim| {
                    victim.routing_bucket
                        == engine.routing_bucket_for_key(1, &format!("candidate-{index:03}"))
                });
            assert_eq!(
                response.response,
                CommandResponse::Bytes {
                    value: (!deleted).then(|| vec![index as u8; 32]),
                }
            );
        }
    }
}

#[test]
fn a_sampler_with_no_resident_objects_inspects_no_cache_entries() {
    let directory = tempfile::tempdir().unwrap();
    let engine = open(directory.path());
    engine.use_sampled_eviction_for_test();
    engine
        .cache
        .put_memory_only(CacheKey::page_with_slot(1, 7, 0, 32, Some(3)), vec![7; 32]);
    reset_storage_cache_inspection_listings();
    let report = engine.apply_storage_eviction(1, 0, 4, false, false);
    assert!(report.selected_victims.is_empty());
    assert_eq!(storage_cache_inspection_listings(), 0);
    assert_eq!(storage_cache_inspection_listing_entries(), 0);
}
