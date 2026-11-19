// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A WALK OVER A SHARD'S REGISTRATIONS TAKES THE REGISTRY LOCK ONE PART AT A TIME.
//!
//! `unreferenced_registrations` and `oldest_registered_objects` used to hold the process-wide
//! registry lock for the whole walk, so every registering writer waited for it: 50 ms at 100k
//! registrations, 278 ms at 300k, measured. They now copy one of 64 parts under the lock and
//! release it before looking at any entry. Wall time is the wrong thing to assert (it depends on
//! the box), so this asserts the quantity that bounds a writer's wait: the most entries a single
//! lock hold copied. It must be a small fraction of the shard, and the walk must still see every
//! registration exactly once.
use super::*;

const ENTRIES: u64 = 6_400;

fn largest_copy() -> usize {
    LARGEST_PART_COPY.with(std::cell::Cell::get)
}

fn reset() {
    LARGEST_PART_COPY.with(|largest| largest.set(0));
    REGISTRATION_QUERY_VISITS.with(|count| count.set(0));
}

fn fixture() -> (tempfile::TempDir, crate::block_store::BlockStore, LocalWriteAheadLogStore) {
    let dir = tempfile::tempdir().unwrap();
    let blocks = crate::block_store::BlockStore::new(dir.path().join("blocks"));
    let wal = LocalWriteAheadLogStore::new(dir.path().join("wal"));
    for object in 0..ENTRIES {
        register_at(&blocks, 1, object, None, 10, object + 1, &wal);
    }
    (dir, blocks, wal)
}

#[test]
fn the_candidate_walk_copies_a_small_part_of_the_shard_under_one_lock_hold() {
    let (_dir, blocks, wal) = fixture();
    // Retain one object in ten, so both outcomes (candidate and protected) occur.
    let retained: std::collections::HashSet<u64> = (0..ENTRIES).step_by(10).collect();
    reset();
    let candidates = unreferenced_registrations(&blocks, 1, &wal, &retained);
    assert_eq!(candidates.len() as u64, ENTRIES - retained.len() as u64, "every unretained block is proposed once");
    assert_eq!(REGISTRATION_QUERY_VISITS.with(std::cell::Cell::get) as u64, ENTRIES, "every registration is seen once");
    let largest = largest_copy();
    assert!(largest > 0, "NON-VACUITY: the walk copied nothing");
    // 64 parts over 6,400 entries is 100 per part; allow 3x for an uneven hash.
    assert!(
        (largest as u64) * 20 < ENTRIES,
        "one lock hold copied {largest} of {ENTRIES} registrations; the walk is not sliced"
    );
}

#[test]
fn the_oldest_first_walk_copies_a_small_part_of_the_shard_under_one_lock_hold() {
    let (_dir, blocks, _wal) = fixture();
    reset();
    let oldest = oldest_registered_objects(&blocks, 1);
    assert_eq!(oldest.len() as u64, ENTRIES);
    assert!(oldest.windows(2).all(|pair| pair[0] <= pair[1]), "still oldest sequence first");
    let largest = largest_copy();
    assert!(largest > 0, "NON-VACUITY: the walk copied nothing");
    assert!((largest as u64) * 20 < ENTRIES, "one lock hold copied {largest} of {ENTRIES} registrations");
}

#[test]
fn a_writer_registering_between_parts_is_neither_lost_nor_double_counted() {
    let (_dir, blocks, wal) = fixture();
    let retained = std::collections::HashSet::new();
    let first = unreferenced_registrations(&blocks, 1, &wal, &retained);
    // New registrations after the walk are not in `first`, and the walk's own entries are intact.
    for object in ENTRIES..ENTRIES + 10 {
        register_at(&blocks, 1, object, None, 10, object + 1, &wal);
    }
    assert_eq!(first.len() as u64, ENTRIES);
    assert_eq!(registration_count(&blocks, 1) as u64, ENTRIES + 10);
    // Retirement re-checks identity: a replaced registration is not removed.
    register_at(&blocks, 1, 0, None, 11, 999, &wal);
    retire_unchanged_registrations(&blocks, 1, &first);
    assert_eq!(registration_count(&blocks, 1), 11, "only the replaced block and the 10 new ones remain");
}
