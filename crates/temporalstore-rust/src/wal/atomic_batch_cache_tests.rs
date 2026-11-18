// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct SegmentLimit;

impl SegmentLimit {
    fn set(bytes: u64) -> Self {
        set_wal_segment_bytes_for_test(Some(bytes));
        Self
    }
}

impl Drop for SegmentLimit {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(None);
    }
}

fn commands(round: usize) -> Vec<Command> {
    (0..2)
        .map(|item| Command::StringSet {
            key: format!("batch-{round}-{item}"),
            value: vec![round as u8; 64],
        })
        .collect()
}

fn tail_reads() -> u64 {
    WAL_PIECE_TAIL_READS.with(|reads| reads.get())
}

fn full_scans(store: &LocalWriteAheadLogStore) -> u64 {
    store.lock_inner_raw().stats.append_full_scans
}

fn sequences(store: &LocalWriteAheadLogStore) -> Vec<u64> {
    store
        .scan(1, 0, u64::MAX, u64::MAX)
        .unwrap()
        .iter()
        .map(|(_, bytes)| decode_wal_line(bytes).unwrap().sequence)
        .collect()
}

#[test]
fn warm_atomic_batches_do_not_rescan_existing_records() {
    let _limit = SegmentLimit::set(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store.append_batch_atomic(1, commands(0), true).unwrap();
    let before = (tail_reads(), full_scans(&store));
    for round in 1..17 {
        let records = store.append_batch_atomic(1, commands(round), true).unwrap();
        assert_eq!(records[1].sequence, (round as u64 + 1) * 2);
    }
    assert_eq!(
        tail_reads(),
        before.0,
        "a warm batch read the WAL tail again"
    );
    assert_eq!(full_scans(&store), before.1);
    assert_eq!(sequences(&store), (1..=34).collect::<Vec<_>>());
}

#[test]
fn forced_scan_control_still_reads_each_atomic_batch() {
    let _limit = SegmentLimit::set(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store.rescan_on_every_append_for_test();
    store.append_batch_atomic(1, commands(0), true).unwrap();
    let before = (tail_reads(), full_scans(&store));
    for round in 1..9 {
        store.append_batch_atomic(1, commands(round), true).unwrap();
    }
    assert!(
        tail_reads() >= before.0 + 8,
        "control did not read the tail"
    );
    assert_eq!(full_scans(&store), before.1 + 8);
    assert_eq!(sequences(&store), (1..=18).collect::<Vec<_>>());
}

#[test]
fn reopened_atomic_writer_validates_once_then_uses_the_warm_tail() {
    let _limit = SegmentLimit::set(0);
    let dir = tempfile::tempdir().unwrap();
    {
        let store = LocalWriteAheadLogStore::new(dir.path());
        store.append_batch_atomic(1, commands(0), true).unwrap();
    }
    let store = LocalWriteAheadLogStore::new(dir.path());
    let before = tail_reads();
    let records = store.append_batch_atomic(1, commands(1), true).unwrap();
    assert_eq!(records[0].sequence, 3);
    assert!(tail_reads() > before);
    assert_eq!(full_scans(&store), 1);
    let warmed = tail_reads();
    store.append_batch_atomic(1, commands(2), true).unwrap();
    assert_eq!(tail_reads(), warmed);
    assert_eq!(sequences(&store), (1..=6).collect::<Vec<_>>());
}

#[test]
fn another_atomic_writer_invalidates_the_verified_tail() {
    let _limit = SegmentLimit::set(0);
    let dir = tempfile::tempdir().unwrap();
    let first = LocalWriteAheadLogStore::new(dir.path());
    first.append_batch_atomic(1, commands(0), true).unwrap();
    let second = LocalWriteAheadLogStore::new(dir.path());
    second.append_batch_atomic(1, commands(1), true).unwrap();
    let before = (tail_reads(), full_scans(&first));
    let records = first.append_batch_atomic(1, commands(2), true).unwrap();
    assert_eq!(records[0].sequence, 5);
    assert!(
        tail_reads() > before.0,
        "external append was not revalidated"
    );
    assert_eq!(full_scans(&first), before.1 + 1);
    assert_eq!(sequences(&first), (1..=6).collect::<Vec<_>>());
}

#[test]
fn interrupted_atomic_batch_cannot_publish_a_stale_sequence_cache() {
    let _limit = SegmentLimit::set(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store.append_batch_atomic(1, commands(0), true).unwrap();
    let armed = crate::fault::arm(
        "wal/atomic_batch/after_first_record",
        crate::fault::FaultAction::Stop,
    );
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        store.append_batch_atomic(1, commands(1), false)
    }));
    drop(armed);
    assert!(outcome.is_err(), "the partial-batch fault was not reached");
    // Deliberately resume this same cache after the interrupted write, not a fresh store.
    store.inner.clear_poison();
    assert!(!store
        .lock_inner_raw()
        .last_sequence_by_shard
        .contains_key(&1));
    let before = full_scans(&store);
    let records = store.append_batch_atomic(1, commands(2), true).unwrap();
    assert_eq!(
        records[0].sequence, 4,
        "a written prefix sequence was reused"
    );
    assert_eq!(full_scans(&store), before + 1);
    assert_eq!(store.cached_last_sequence(1), 5);
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let records = reopened.append_batch_atomic(1, commands(3), true).unwrap();
    assert_eq!(records[0].sequence, 6);
}

#[test]
fn atomic_batch_cache_survives_segment_roll_and_restart() {
    let _limit = SegmentLimit::set(1024);
    let dir = tempfile::tempdir().unwrap();
    {
        let store = LocalWriteAheadLogStore::new(dir.path());
        for round in 0..32 {
            let records = store.append_batch_atomic(1, commands(round), true).unwrap();
            assert_eq!(records[0].sequence, round as u64 * 2 + 1);
        }
        assert!(wal_segment_paths(dir.path(), 1).len() > 1);
        assert_eq!(sequences(&store), (1..=64).collect::<Vec<_>>());
    }
    let reopened = LocalWriteAheadLogStore::new(dir.path());
    let records = reopened.append_batch_atomic(1, commands(32), true).unwrap();
    assert_eq!(records[0].sequence, 65);
    assert_eq!(sequences(&reopened), (1..=66).collect::<Vec<_>>());
}
