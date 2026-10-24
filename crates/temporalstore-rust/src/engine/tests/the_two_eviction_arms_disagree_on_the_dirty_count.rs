// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The two eviction arms must agree on what a victim's dirty count means.
//!
//! WHY THIS EXISTS. `StorageEvictionVictim::dirty_object_count` is filled by two different arms of
//! `apply_storage_eviction`, from two different quantities, and both feed the SAME weight:
//!
//!     weight = cache_memory*4 + cache_disk*2 + physical_bytes + dirty_object_count*1024
//!
//! * the SAMPLED arm -- the shipped default -- walks `bucket.block_index` and counts PAGES whose
//!   own `dirty` bit is set;
//! * the FULL-SCAN arm takes `summary.dirty_object_count`, which comes from the object-keyed dirty
//!   index and counts OBJECTS.
//!
//! An object can hold several pages, so these are not the same number, and the field name says
//! object while one arm puts pages in it. The consequence is not cosmetic: the two arms rank
//! victims differently for a reason that has nothing to do with eviction policy, and a weight is a
//! sort key.
//!
//! WHAT THIS TEST REFUSES TO DO. It does not assert a particular definition is correct -- that is a
//! policy question. It asserts the two arms AGREE, which is the part neither answer can excuse.
//!
//! NON-VACUITY, asserted rather than hoped for. On a fixture where every object happens to hold one
//! page, pages and objects are equal and the two arms agree for the wrong reason -- such a fixture
//! would pass against the defect. So the fixture seeds a multi-page object and the test asserts
//! that some bucket really does hold more dirty pages than dirty objects BEFORE comparing the arms.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 11;

/// A store seeded so that at least one bucket holds a multi-page object.
///
/// `page_size` is deliberately small so one feature series spans several pages: a fixture whose
/// objects are all single-page cannot tell a page count from an object count.
fn seeded(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(SHARD);

    // One wide series: many points under ONE object key, so its pages outnumber it.
    for chunk in 0..6usize {
        let points = (0..64u64)
            .map(|t| crate::types::FeaturePoint {
                timestamp_ms: 1_700_000_000_000 + (chunk as u64) * 64 + t,
                value: vec![b'f'; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::FeatureAppend {
                key: "wide".to_string(),
                points,
            },
        });
        assert!(response.status.ok, "wide seed must ack at {chunk}");
    }
    // And a handful of ordinary single-page records, so the fixture is not one object.
    for i in 0..12usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 48],
            },
        });
        assert!(response.status.ok, "string seed must ack at {i}");
    }
    engine
}

/// Dirty pages and dirty objects per bucket, read straight off the shard.
fn pages_and_objects(
    engine: &TemporalEngine,
) -> (
    std::collections::BTreeMap<u32, u64>,
    std::collections::BTreeMap<u32, u64>,
) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let mut pages: std::collections::BTreeMap<u32, u64> = std::collections::BTreeMap::new();
    for (bucket_id, bucket) in shard.bucket_index.bucket_map.iter() {
        let n = bucket
            .block_index
            .values()
            .filter(|page| !page.deleted && page.dirty)
            .count() as u64;
        if n > 0 {
            pages.insert(*bucket_id, n);
        }
    }
    let objects: std::collections::BTreeMap<u32, u64> = shard
        .dirty_objects
        .bucket_counts()
        .into_iter()
        .map(|(bucket_id, count)| (bucket_id, count as u64))
        .collect();
    (pages, objects)
}

#[test]
fn the_two_eviction_arms_must_agree_on_what_the_dirty_count_means() {
    // ---- THE REGIME, asserted before anything is compared. ----
    let regime_dir = tempfile::tempdir().expect("tempdir");
    let regime = seeded(regime_dir.path());
    let (pages, objects) = pages_and_objects(&regime);
    println!("dirty pages per bucket:   {pages:?}");
    println!("dirty objects per bucket: {objects:?}");
    let total_pages: u64 = pages.values().sum();
    let total_objects: u64 = objects.values().sum();
    println!("totals: {total_pages} dirty pages, {total_objects} dirty objects");
    assert!(
        total_pages > 0 && total_objects > 0,
        "the fixture must be dirty at all: {total_pages} pages, {total_objects} objects"
    );
    assert!(
        total_pages > total_objects,
        "this fixture cannot tell a page count from an object count ({total_pages} pages vs \
         {total_objects} objects) -- every object holds one page, so the two arms would agree \
         for the wrong reason. The fixture needs a multi-page object, not a different assertion."
    );
    drop(regime);

    // ---- THE TWO ARMS, on two stores seeded identically. ----
    let sampled_dir = tempfile::tempdir().expect("tempdir");
    let sampled_engine = seeded(sampled_dir.path());
    sampled_engine.use_sampled_eviction_for_test();
    let sampled = sampled_engine.apply_storage_eviction(SHARD, 1, 64, false, false);

    let full_dir = tempfile::tempdir().expect("tempdir");
    let full_engine = seeded(full_dir.path());
    full_engine.use_full_scan_eviction_for_test();
    let full = full_engine.apply_storage_eviction(SHARD, 1, 64, false, false);

    // Both arms must have actually selected something, or neither says anything.
    assert!(
        !sampled.selected_victims.is_empty(),
        "the sampled arm selected nothing: {sampled:?}"
    );
    assert!(
        !full.selected_victims.is_empty(),
        "the full-scan arm selected nothing: {full:?}"
    );

    let by_bucket = |report: &crate::engine::reports::StorageEvictionReport| {
        report
            .selected_victims
            .iter()
            .map(|victim| (victim.routing_bucket, (victim.dirty_object_count, victim.weight)))
            .collect::<std::collections::BTreeMap<u32, (u64, u64)>>()
    };
    let s = by_bucket(&sampled);
    let f = by_bucket(&full);
    println!("sampled   (bucket -> dirty_object_count, weight): {s:?}");
    println!("full scan (bucket -> dirty_object_count, weight): {f:?}");

    // ---- THE CLAIM: one field, one meaning. ----
    for (bucket_id, (sampled_count, sampled_weight)) in &s {
        if let Some((full_count, full_weight)) = f.get(bucket_id) {
            assert_eq!(
                sampled_count, full_count,
                "bucket {bucket_id}: the two eviction arms disagree on `dirty_object_count` -- \
                 the sampled arm counts PAGES whose dirty bit is set and the full-scan arm counts \
                 OBJECTS from the dirty index. One field, two meanings, and it is multiplied by \
                 1024 into a sort key"
            );
            assert_eq!(
                sampled_weight, full_weight,
                "bucket {bucket_id}: the two arms produced different victim WEIGHTS, so they rank \
                 eviction candidates differently for a reason that is not eviction policy"
            );
        }
    }
}
