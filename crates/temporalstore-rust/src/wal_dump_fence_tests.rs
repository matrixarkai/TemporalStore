// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct Appended {
    current_sequence: u64,
    size: u64,
}

fn append(store: &LocalWriteAheadLogStore, shard: ShardId, n: u64) -> Appended {
    let before = store.undumped_len_since_dump(shard);
    let record = store
        .append_with_sync(
            shard,
            Command::StringSet {
                key: format!("growth-{n}"),
                value: n.to_le_bytes().to_vec(),
            },
            false,
        )
        .unwrap();
    Appended {
        current_sequence: record.sequence,
        size: store.undumped_len_since_dump(shard) - before,
    }
}

#[test]
fn dump_fence_keeps_later_appends_and_other_shards_owed() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 1, 1);
    let fence = store.capture_dump_fence(1, first.current_sequence).unwrap();
    let tail = append(&store, 1, 2);
    let unrelated = append(&store, 2, 3);
    assert!(store.mark_dumped_through(&fence));
    assert_eq!(store.undumped_len_since_dump(1), tail.size);
    assert_eq!(store.undumped_objects_since_dump(1), 1);
    assert_eq!(store.undumped_len_since_dump(2), unrelated.size);
    assert!(store.mark_dumped_through(&fence));
    assert_eq!(store.undumped_len_since_dump(1), tail.size);
    // Control: retiring the current tail would hide the write absent from the image.
    store.mark_dumped(1);
    assert_eq!(store.undumped_len_since_dump(1), 0);
    assert_eq!(store.undumped_objects_since_dump(1), 0);
}

#[test]
fn dump_fences_are_idempotent_out_of_order_and_across_successive_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, 1, 1);
    let older = store.capture_dump_fence(1, first.current_sequence).unwrap();
    let second = append(&store, 1, 2);
    let newer = store
        .capture_dump_fence(1, second.current_sequence)
        .unwrap();
    let third = append(&store, 1, 3);
    for fence in [&newer, &older, &newer] {
        assert!(store.mark_dumped_through(fence));
        assert_eq!(store.undumped_len_since_dump(1), third.size);
        assert_eq!(store.undumped_objects_since_dump(1), 1);
    }
    let third_fence = store.capture_dump_fence(1, third.current_sequence).unwrap();
    let fourth = append(&store, 1, 4);
    assert!(store.mark_dumped_through(&third_fence));
    assert_eq!(store.undumped_len_since_dump(1), fourth.size);
    assert_eq!(store.undumped_objects_since_dump(1), 1);
}

#[test]
fn dump_fence_refuses_a_stale_image_and_a_foreign_namespace() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(a.path());
    let foreign = LocalWriteAheadLogStore::new(b.path());
    let report = append(&store, 1, 1);
    let other = append(&foreign, 1, 1);
    assert!(store.capture_dump_fence(1, 0).is_none());
    let fence = store
        .capture_dump_fence(1, report.current_sequence)
        .unwrap();
    assert!(!foreign.mark_dumped_through(&fence));
    assert_eq!(foreign.undumped_len_since_dump(1), other.size);
    assert_eq!(store.undumped_len_since_dump(1), report.size);
}

#[test]
fn dump_fence_does_not_scan_history_or_store_one_row_per_record() {
    fn io_counts() -> ((u64, u64), u64, u64) {
        (
            wal_piece_read_counts_on_this_thread(),
            WAL_READ_FILE_OPENS.with(std::cell::Cell::get),
            WAL_SEGMENT_LISTINGS.with(std::cell::Cell::get),
        )
    }
    for count in [64, 4096] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalWriteAheadLogStore::new(dir.path());
        let mut sequence = 0;
        for n in 0..count {
            sequence = append(&store, 1, n).current_sequence;
        }
        let scans = store.stats_without_rescan(1).stats_full_scans;
        let control = io_counts();
        store.probe_all_segment_bytes(1);
        assert!(
            io_counts().2 > control.2,
            "a real listing must move the I/O control"
        );
        let before = io_counts();
        let fence = store.capture_dump_fence(1, sequence).unwrap();
        assert_eq!(
            io_counts(),
            before,
            "fence capture must not read or list WAL files"
        );
        let tail = append(&store, 1, count);
        let before = io_counts();
        assert!(store.mark_dumped_through(&fence));
        assert_eq!(
            io_counts(),
            before,
            "fence publication must not read or list WAL files"
        );
        assert_eq!(store.stats_without_rescan(1).stats_full_scans, scans);
        assert_eq!(store.undumped_len_since_dump(1), tail.size);
        assert_eq!(store.inner.lock().unwrap().dumped_growth_by_shard.len(), 1);
    }
}
