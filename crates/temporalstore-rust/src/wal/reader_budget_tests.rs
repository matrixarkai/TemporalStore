// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn reader(root: &Path, shard: ShardId, length: u64) -> Reader {
    let path = root.join(format!("fixture-{shard}"));
    let file = File::create(&path).unwrap();
    file.set_len(length).unwrap();
    let stamp = stamp(&file.metadata().unwrap());
    Reader { shard, base: 0, header: 0, length, sealed: true, path,
        stamp, file, physical: 0, reservation: None }
}

#[test]
fn every_aggregate_dimension_rejects_excess_and_returns_on_drop() {
    let budget = Arc::new(Budget::default());
    for dimension in 0..5 {
        let mut full = [0; 5];
        full[dimension] = LIMITS[dimension];
        let reservation = budget.reserve(full).unwrap();
        let mut excess = [0; 5];
        excess[dimension] = 1;
        assert!(budget.reserve(excess).is_none());
        assert_eq!(budget.snapshot(), full);
        drop(reservation);
        assert_eq!(budget.snapshot(), [0; 5]);
    }
    let mut lease = budget.lease();
    assert!(!lease.resize([u64::MAX; 5]));
    assert_eq!(budget.snapshot(), [0; 5]);
}

#[cfg(unix)]
#[test]
fn idle_descriptors_and_queue_capacity_are_bounded_across_owners() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(Budget::default());
    let mut caches: Vec<_> = (0..192).map(|_| Cache::with_budget(budget.clone())).collect();
    for (shard, cache) in caches.iter_mut().enumerate() {
        cache.put(reader(root.path(), shard as ShardId, 1));
        let used = budget.snapshot();
        assert!(used.iter().zip(LIMITS).all(|(used, limit)| *used <= limit));
    }
    assert_eq!(caches.iter().map(|cache| cache.readers.len()).sum::<usize>(), MAX_READERS);
    let actual = caches.iter().map(|cache| cache.reader_bytes
        + cache.readers.capacity() * std::mem::size_of::<Reader>()).sum::<usize>();
    assert_eq!(budget.snapshot()[1], actual as u64);
    assert_eq!(budget.snapshot()[2], MAX_READERS as u64);
    drop(caches);
    assert_eq!(budget.snapshot(), [0; 5]);
}

#[cfg(unix)]
#[test]
fn represented_extents_do_not_multiply_with_shard_count() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(Budget::default());
    let mut caches: Vec<_> = (0..96).map(|_| Cache::with_budget(budget.clone())).collect();
    for (shard, cache) in caches.iter_mut().enumerate() {
        cache.put(reader(root.path(), shard as ShardId, 1024 * 1024));
    }
    assert_eq!(budget.snapshot()[2], OPEN_FILE_BYTES);
    assert_eq!(caches.iter().map(|cache| cache.readers.len()).sum::<usize>(), 64);
    drop(caches);
    assert_eq!(budget.snapshot(), [0; 5]);
}

#[cfg(unix)]
#[test]
fn denied_reader_bookkeeping_releases_the_new_queue_allocation() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(Budget::default());
    let full = budget.reserve([0, READER_BYTES as u64, 0, 0, 0]).unwrap();
    let mut cache = Cache::with_budget(budget.clone());
    cache.put(reader(root.path(), 1, 1));
    assert!(cache.readers.is_empty());
    assert_eq!(cache.readers.capacity(), 0);
    assert_eq!(budget.snapshot(), [0, READER_BYTES as u64, 0, 0, 0]);
    drop(full);
    cache.put(reader(root.path(), 2, 1));
    assert_eq!(cache.readers.len(), 1);
    cache.clear_shard(2);
    assert_eq!(budget.snapshot(), [0; 5]);
}

#[cfg(unix)]
#[test]
fn directory_reservation_follows_last_arc_and_failed_admission_bypasses() {
    let root = tempfile::tempdir().unwrap();
    let budget = Arc::new(Budget::default());
    let mut caches: Vec<_> = (0..16).map(|_| Cache::with_budget(budget.clone())).collect();
    let mut held = Vec::new();
    for (shard, cache) in caches.iter_mut().enumerate() {
        held.push(cache.directory(root.path(), shard as ShardId, false).unwrap());
    }
    assert_eq!(budget.snapshot()[3], MAX_DIRECTORIES as u64);
    assert_eq!(caches.iter().map(|cache| cache.directories.len()).sum::<usize>(), MAX_DIRECTORIES);
    for (shard, cache) in caches.iter_mut().enumerate() { cache.clear_shard(shard as ShardId); }
    assert_eq!(budget.snapshot()[3], MAX_DIRECTORIES as u64);
    drop(held);
    assert_eq!(budget.snapshot(), [0; 5]);
    let full = budget.reserve([0, 0, 0, 0, DIRECTORY_BYTES as u64]).unwrap();
    let transient = caches[0].directory(root.path(), 0, false).unwrap();
    assert!(caches[0].directories.is_empty());
    assert_eq!(caches[0].directories.capacity(), 0);
    assert!(transient.reservation.is_none());
    drop(full);
    assert_eq!(budget.snapshot(), [0; 5]);
}

#[test]
fn concurrent_reservations_cannot_overbook_the_store() {
    let budget = Arc::new(Budget::default());
    let ready = Arc::new(std::sync::Barrier::new(16));
    let release = Arc::new(std::sync::Barrier::new(16));
    let admitted = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|scope| {
        for _ in 0..16 {
            let budget = budget.clone();
            let ready = ready.clone();
            let release = release.clone();
            let admitted = admitted.clone();
            scope.spawn(move || {
                let lease = budget.reserve([16, 0, 0, 0, 0]);
                if lease.is_some() { admitted.fetch_add(1, AtomicOrdering::SeqCst); }
                ready.wait();
                assert_eq!(budget.snapshot()[0], MAX_READERS as u64);
                release.wait();
                drop(lease);
            });
        }
    });
    assert_eq!(admitted.load(AtomicOrdering::SeqCst), 8);
    assert_eq!(budget.snapshot(), [0; 5]);
}
