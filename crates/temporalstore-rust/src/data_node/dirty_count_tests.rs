// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn assert_counts(tracker: &DirtyTracker) {
    let mut expected = HashMap::new();
    for (shard_id, _) in tracker.by_key.keys() {
        *expected.entry(*shard_id).or_insert(0usize) += 1;
    }
    assert_eq!(tracker.by_shard, expected);
}

#[test]
fn dirty_counts_track_distinct_keys_and_preserve_redirty_identity() {
    let mut tracker = DirtyTracker::default();
    assert_counts(&tracker);
    mark_dirty_in(&mut tracker, 1, "first".into(), 10);
    let object_id = tracker.by_key[&(1, "first".into())].object_id;
    mark_dirty_in(&mut tracker, 1, "first".into(), 20);
    assert_eq!(tracker.by_key[&(1, "first".into())].object_id, object_id);
    assert_eq!(tracker.by_key[&(1, "first".into())].last_dirty_at_ms, 20);
    assert_eq!(tracker.by_shard[&1], 1);
    mark_dirty_in(&mut tracker, 2, "first".into(), 30);
    mark_dirty_in(&mut tracker, 1, "second".into(), 40);
    assert_counts(&tracker);
    assert_eq!(tracker.by_shard[&1], 2);
    assert_eq!(tracker.by_shard[&2], 1);
    assert_eq!(dirty_shards(&Mutex::new(tracker)), vec![1, 2]);
}

#[test]
fn dirty_counts_follow_partial_bucket_clear_and_last_shard_removal() {
    let directory = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        directory.path().join("cache"),
        directory.path().join("blocks"),
        directory.path().join("indexes"),
    );
    engine.load_shard(1);
    engine.load_shard(2);
    let dirty = Mutex::new(DirtyTracker::default());
    for shard in [1, 2] {
        for index in 0..32 {
            mark_dirty_owned(&dirty, shard, format!("object-{index}"));
        }
    }
    assert_eq!(clear_dirty_shard_buckets(&dirty, &engine, 1, &[]), 0);
    let selected = engine.routing_bucket_for_key(1, "object-0");
    let expected_removed = dirty
        .lock()
        .unwrap()
        .by_key
        .keys()
        .filter(|(shard, key)| *shard == 1 && engine.routing_bucket_for_key(1, key) == selected)
        .count();
    assert!(expected_removed > 0);
    assert!(
        expected_removed < 32,
        "the selected bucket must leave dirty objects in this shard"
    );
    assert_eq!(
        clear_dirty_shard_buckets(&dirty, &engine, 1, &[selected]),
        expected_removed
    );
    assert_eq!(
        clear_dirty_shard_buckets(&dirty, &engine, 1, &[selected]),
        0
    );
    {
        let tracker = dirty.lock().unwrap();
        assert_counts(&tracker);
        assert_eq!(tracker.by_shard[&2], 32);
    }
    mark_dirty_owned(&dirty, 1, "object-0".into());
    for shard in [1, 2] {
        let buckets = dirty
            .lock()
            .unwrap()
            .by_key
            .keys()
            .filter(|(id, _)| *id == shard)
            .map(|(_, key)| engine.routing_bucket_for_key(shard, key))
            .collect::<Vec<_>>();
        assert!(clear_dirty_shard_buckets(&dirty, &engine, shard, &buckets) > 0);
        let tracker = dirty.lock().unwrap();
        assert_counts(&tracker);
        assert!(!tracker.by_shard.contains_key(&shard));
    }
    assert!(dirty_shards(&dirty).is_empty());
}

#[test]
fn clearing_dirty_buckets_reads_the_routing_range_once_not_once_per_key() {
    let directory = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        directory.path().join("cache"),
        directory.path().join("blocks"),
        directory.path().join("indexes"),
    );
    engine.load_shard(1);
    let dirty = Mutex::new(DirtyTracker::default());
    const KEYS: u64 = 512;
    for index in 0..KEYS {
        mark_dirty_owned(&dirty, 1, format!("object-{index}"));
    }
    let selected = engine.routing_bucket_for_key(1, "object-0");
    // The control: the per-key call DOES read the info row once per key, so the counter below
    // can fire and the single read after it means something.
    let control_start = crate::engine::routing_range_reads::count();
    let expected_removed = dirty
        .lock()
        .unwrap()
        .by_key
        .keys()
        .filter(|(_, key)| engine.routing_bucket_for_key(1, key) == selected)
        .count();
    assert_eq!(
        crate::engine::routing_range_reads::count() - control_start,
        KEYS,
        "the control must read the routing range once per key, or this guard measures nothing"
    );
    assert!(expected_removed > 0 && (expected_removed as u64) < KEYS);
    let start = crate::engine::routing_range_reads::count();
    let removed = clear_dirty_shard_buckets(&dirty, &engine, 1, &[selected]);
    let reads = crate::engine::routing_range_reads::count() - start;
    assert_eq!(removed, expected_removed, "the resolver must name the same buckets as the per-key call");
    assert_eq!(
        reads, 1,
        "a clear over {KEYS} dirty keys read the routing range {reads} times; it must read it once"
    );
    assert_counts(&dirty.lock().unwrap());
}

// Measurement, not a guard: `cargo test --release ... -- --ignored --nocapture`. Times the clear over
// 100k dirty keys against the old loop (per-key call under the same mutex), both in this binary.
#[test]
#[ignore]
fn measure_clearing_dirty_buckets_per_key_call_against_one_range_read() {
    let directory = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1 << 20,
        directory.path().join("cache"),
        directory.path().join("blocks"),
        directory.path().join("indexes"),
    );
    engine.load_shard(1);
    const KEYS: usize = 100_000;
    let fill = || {
        let dirty = Mutex::new(DirtyTracker::default());
        for index in 0..KEYS {
            mark_dirty_owned(&dirty, 1, format!("object-{index}"));
        }
        dirty
    };
    let selected = engine.routing_bucket_for_key(1, "object-0");
    let buckets = [selected];
    let mut old_ns = Vec::new();
    let mut new_ns = Vec::new();
    for _ in 0..7 {
        let dirty = fill();
        let started = std::time::Instant::now();
        let removed_old = {
            let set = buckets.iter().copied().collect::<BTreeSet<_>>();
            let mut tracker = dirty.lock().unwrap();
            let before = tracker.by_key.len();
            tracker.by_key.retain(|(shard, key), _| {
                *shard != 1 || !set.contains(&engine.routing_bucket_for_key(1, key))
            });
            before - tracker.by_key.len()
        };
        old_ns.push(started.elapsed().as_nanos());
        let dirty = fill();
        let started = std::time::Instant::now();
        let removed_new = clear_dirty_shard_buckets(&dirty, &engine, 1, &buckets);
        new_ns.push(started.elapsed().as_nanos());
        assert_eq!(removed_old, removed_new);
    }
    old_ns.sort();
    new_ns.sort();
    println!(
        "MEASURE keys={KEYS} old_median_ms={:.2} new_median_ms={:.2} old_per_key_ns={} new_per_key_ns={}",
        old_ns[3] as f64 / 1e6,
        new_ns[3] as f64 / 1e6,
        old_ns[3] / KEYS as u128,
        new_ns[3] / KEYS as u128
    );
}

#[test]
fn concurrent_redirty_keeps_one_count_per_shard_and_key() {
    let dirty = std::sync::Arc::new(Mutex::new(DirtyTracker::default()));
    let handles = (0..4)
        .map(|_| {
            let dirty = dirty.clone();
            std::thread::spawn(move || {
                for index in 0..64 {
                    mark_dirty_owned(&dirty, index % 3, format!("object-{index}"));
                }
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }
    let tracker = dirty.lock().unwrap();
    assert_counts(&tracker);
    assert_eq!(tracker.by_key.len(), 64);
}
