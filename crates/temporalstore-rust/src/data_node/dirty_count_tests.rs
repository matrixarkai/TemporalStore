// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn assert_counts(tracker: &DirtyTracker) {
    let mut expected = BTreeMap::new();
    let objects = tracker.objects();
    for object in &objects {
        *expected.entry(object.shard_id).or_insert(0usize) += 1;
    }
    assert_eq!(tracker.counts(), (objects.len(), expected.len()));
    assert_eq!(tracker.counts_by_shard(), expected);
}

#[test]
fn dirty_counts_track_distinct_keys_and_preserve_redirty_identity() {
    let tracker = DirtyTracker::default();
    assert_counts(&tracker);
    tracker.mark(1, ["first".into()], || 10);
    let object_id = tracker.objects()[0].object_id;
    tracker.mark(1, ["first".into()], || 20);
    assert_eq!(tracker.objects()[0].object_id, object_id);
    assert_eq!(tracker.objects()[0].last_dirty_at_ms, 20);
    assert_eq!(tracker.counts_by_shard()[&1], 1);
    tracker.mark(2, ["first".into()], || 30);
    tracker.mark(1, ["second".into()], || 40);
    assert_counts(&tracker);
    assert_eq!(tracker.counts_by_shard()[&1], 2);
    assert_eq!(tracker.counts_by_shard()[&2], 1);
    assert_eq!(dirty_shards(&tracker), vec![1, 2]);
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
    let dirty = DirtyTracker::default();
    for shard in [1, 2] {
        for index in 0..32 {
            mark_dirty_owned(&dirty, shard, format!("object-{index}"));
        }
    }
    assert_eq!(clear_dirty_shard_buckets(&dirty, &engine, 1, &[]), 0);
    let selected = engine.routing_bucket_for_key(1, "object-0");
    let expected_removed = dirty.objects().iter()
        .filter(|row| row.shard_id == 1 && engine.routing_bucket_for_key(1, &row.key) == selected)
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
        let tracker = &dirty;
        assert_counts(&tracker);
        assert_eq!(tracker.counts_by_shard()[&2], 32);
    }
    mark_dirty_owned(&dirty, 1, "object-0".into());
    for shard in [1, 2] {
        let buckets = dirty.objects().iter()
            .filter(|row| row.shard_id == shard)
            .map(|row| engine.routing_bucket_for_key(shard, &row.key))
            .collect::<Vec<_>>();
        assert!(clear_dirty_shard_buckets(&dirty, &engine, shard, &buckets) > 0);
        let tracker = &dirty;
        assert_counts(&tracker);
        assert!(!tracker.counts_by_shard().contains_key(&shard));
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
    let dirty = DirtyTracker::default();
    const KEYS: u64 = 512;
    for index in 0..KEYS {
        mark_dirty_owned(&dirty, 1, format!("object-{index}"));
    }
    let selected = engine.routing_bucket_for_key(1, "object-0");
    // The control: the per-key call DOES read the info row once per key, so the counter below
    // can fire and the single read after it means something.
    let control_start = crate::engine::routing_range_reads::count();
    let expected_removed = dirty.objects().iter()
        .filter(|row| engine.routing_bucket_for_key(1, &row.key) == selected)
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
    assert_counts(&dirty);
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
        let dirty = DirtyTracker::default();
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
        let old = Mutex::new(fill().objects().into_iter()
            .map(|row| ((row.shard_id, row.key), row.object_id)).collect::<HashMap<_, _>>());
        let started = std::time::Instant::now();
        let removed_old = {
            let set = buckets.iter().copied().collect::<BTreeSet<_>>();
            let mut tracker = old.lock().unwrap();
            let before = tracker.len();
            tracker.retain(|(shard, key), _| {
                *shard != 1 || !set.contains(&engine.routing_bucket_for_key(1, key))
            });
            before - tracker.len()
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
    let dirty = std::sync::Arc::new(DirtyTracker::default());
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
    let tracker = &dirty;
    assert_counts(&tracker);
    assert_eq!(tracker.counts().0, 64);
}

#[test]
fn peer_tracker_progresses_while_another_shard_is_locked() {
    use std::sync::mpsc;
    use std::time::Duration;

    let dirty = Arc::new(DirtyTracker::default());
    mark_dirty_owned(&dirty, 1, "held".into());
    mark_dirty_owned(&dirty, 2, "peer".into());
    let held_owner = dirty.owner(1).unwrap();
    let held = held_owner.lock().unwrap();
    let (finished, completion) = mpsc::channel();
    let peer = Arc::clone(&dirty);
    let worker = std::thread::spawn(move || {
        mark_dirty_owned(&peer, 2, "new-peer".into());
        let removed = peer.clear_where(2, |key| key == "peer");
        finished.send((removed, peer.counts(), dirty_shards(&peer))).unwrap();
    });
    let result = completion.recv_timeout(Duration::from_secs(5));
    drop(held);
    worker.join().unwrap();
    assert_eq!(result.expect("peer tracker stalled behind shard 1"), (1, (2, 2), vec![1, 2]));
}

#[test]
fn last_key_clear_retires_the_owner_and_redirty_uses_a_new_identity() {
    let dirty = DirtyTracker::default();
    dirty.mark(1, ["same".into()], || 10);
    dirty.mark(2, ["same".into()], || 11);
    let old_owner = dirty.owner(1).unwrap();
    let before = dirty.objects();
    let first_id = before.iter().find(|row| row.shard_id == 1).unwrap().object_id;
    let peer_id = before.iter().find(|row| row.shard_id == 2).unwrap().object_id;
    assert_ne!(first_id, peer_id);
    assert_eq!(dirty.clear_where(1, |key| key == "same"), 1);
    assert!(dirty.owner(1).is_none());
    assert!(old_owner.lock().unwrap().is_none());
    assert_eq!(dirty.counts(), (1, 1));
    dirty.mark(1, ["same".into()], || 20);
    assert!(!Arc::ptr_eq(&old_owner, &dirty.owner(1).unwrap()));
    let after = dirty.objects();
    let row = after.iter().find(|row| row.shard_id == 1).unwrap();
    assert_eq!(row.key, "same");
    assert_eq!(row.last_dirty_at_ms, 20);
    assert_ne!(row.object_id, first_id);
    assert_ne!(row.object_id, peer_id);
    assert_eq!(after.iter().find(|row| row.shard_id == 2).unwrap().object_id, peer_id);
    assert_eq!(dirty.counts(), (2, 2));
}

#[test]
fn exact_dirty_key_removal_uses_one_point_operation_at_both_populations() {
    for peers in [64, 4_096] {
        let dirty = DirtyTracker::default();
        dirty.mark(1, (0..peers).map(|i| format!("peer-{i:04}")), || 10);
        dirty.mark(1, ["selected".into()], || 11);
        dirty.mark(2, ["selected".into()], || 12);
        DIRTY_REMOVAL_VISITS.with(|visits| visits.set((0, 0)));
        assert_eq!(dirty.clear_where(1, |_| false), 0);
        assert_eq!(DIRTY_REMOVAL_VISITS.with(|visits| visits.replace((0, 0))), (peers + 1, 0));
        assert!(dirty.clear_key(1, "selected"));
        assert_eq!(DIRTY_REMOVAL_VISITS.with(|visits| visits.replace((0, 0))), (0, 1));
        assert_eq!(dirty.counts(), (peers + 1, 2));
        assert_eq!(dirty.counts_by_shard(), BTreeMap::from([(1, peers), (2, 1)]));
        let survivors = dirty.objects();
        assert!(!survivors.iter().any(|row| row.shard_id == 1 && row.key == "selected"));
        assert!(survivors.iter().any(|row| row.shard_id == 2 && row.key == "selected"));
        assert!(!dirty.clear_key(1, "selected"));
        assert_eq!(DIRTY_REMOVAL_VISITS.with(|visits| visits.get()), (0, 1));
    }
}
