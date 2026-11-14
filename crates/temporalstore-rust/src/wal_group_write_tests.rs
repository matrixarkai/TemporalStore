// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The group-commit write buffer: concurrent writers' records leave in ONE write, and a crash or a
//! failed write part-way through that batch loses no acknowledged record and promotes no
//! unacknowledged one.

use super::*;
use std::sync::atomic::Ordering;

fn park_flusher() {
    LOOP_INTERVAL_MS_OVERRIDE.store(600_000, Ordering::SeqCst);
}

fn set(key: &str, len: usize) -> Command {
    Command::StringSet { key: key.to_string(), value: vec![7u8; len] }
}

fn read_back(dir: &Path) -> Vec<(u64, Command)> {
    // A FRESH store over the same directory: what a restart would see, with no memory buffer.
    let store = LocalWriteAheadLogStore::new(dir);
    let (records, _) = store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
    records
        .into_iter()
        .map(|(_, record)| (record.sequence, record.command.expect("a command record")))
        .collect()
}

fn keys(records: &[(u64, Command)]) -> Vec<String> {
    records
        .iter()
        .map(|(_, command)| match command {
            Command::StringSet { key, .. } => key.clone(),
            other => panic!("unexpected command {other:?}"),
        })
        .collect()
}

#[test]
fn group_write_one_barrier_writes_every_buffered_record_in_one_call() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut last = 0;
    for index in 0..8 {
        last = store
            .append_for_group_commit(1, set(&format!("k{index}"), 144), vec![], vec![])
            .unwrap()
            .sequence;
    }
    // Buffered, not written: eight appends, no system call to the log file yet.
    assert_eq!(store.group_write_counts(), (0, 8), "appends must only buffer");
    store.commit_barrier(1, last).unwrap();
    assert_eq!(
        store.group_write_counts(),
        (1, 8),
        "one barrier must write all eight records in one write"
    );
    let records = read_back(dir.path());
    assert_eq!(keys(&records), (0..8).map(|i| format!("k{i}")).collect::<Vec<_>>());
    assert_eq!(records.iter().map(|(s, _)| *s).collect::<Vec<_>>(), (1..=8).collect::<Vec<_>>());
}

#[test]
fn group_write_a_record_that_was_never_acknowledged_is_not_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let acked = store.append_for_group_commit(1, set("acked", 64), vec![], vec![]).unwrap();
    store.commit_barrier(1, acked.sequence).unwrap();
    store.append_for_group_commit(1, set("in-flight", 64), vec![], vec![]).unwrap();
    // The process dies here: the second record is only in memory. A restart must not see it.
    let records = read_back(dir.path());
    assert_eq!(keys(&records), vec!["acked".to_string()]);
    std::mem::forget(store);
}

/// One torn-batch scenario. `cut_in_record` cuts the batch write half way through its second
/// record; otherwise exactly after the first. `preallocate` selects the log layout the file is
/// written in (the default is ON; the layout decides how a torn frame reads back).
fn torn_batch_case(preallocate: bool, cut_in_record: bool) -> Option<Vec<String>> {
    park_flusher();
    std::env::set_var("TS_WAL_PREALLOCATE", if preallocate { "1" } else { "0" });
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let acked = store.append_for_group_commit(1, set("a0", 100), vec![], vec![]).unwrap();
    store.commit_barrier(1, acked.sequence).unwrap();
    let before = store.info(1).unwrap().persistent_length_bytes;
    let mut last = 0;
    for name in ["b1", "b2", "b3"] {
        last = store.append_for_group_commit(1, set(name, 100), vec![], vec![]).unwrap().sequence;
    }
    let one_record = before as i64; // the acked record has the same size as each batch record
    let cut = if cut_in_record { one_record + one_record / 2 } else { one_record };
    TORN_BATCH_WRITE_BYTES.store(cut, Ordering::SeqCst);
    assert!(store.commit_barrier(1, last).is_err(), "a torn batch must not be acknowledged");
    // What a restart sees, read BEFORE anything else touches the store (any reader on the store
    // retries the buffered write).
    let restart = {
        let fresh = LocalWriteAheadLogStore::new(dir.path());
        fresh.scan_decoded(1, 0, u64::MAX, u64::MAX)
    };
    assert_eq!(
        store.info(1).unwrap().last_flushed_sequence,
        acked.sequence,
        "a failed batch must not advance the durable sequence"
    );
    assert_eq!(store.info(1).unwrap().persistent_length_bytes, before);
    std::mem::forget(store);
    std::env::remove_var("TS_WAL_PREALLOCATE");
    let (records, _) = restart.ok()?;
    let names: Vec<String> = records
        .iter()
        .map(|(_, r)| match r.command.as_ref().unwrap() {
            Command::StringSet { key, .. } => key.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    Some(names)
}

#[test]
fn group_write_torn_batch_loses_no_acked_record_and_promotes_no_unacked_one() {
    // Cut at a record boundary, in both layouts: the acked record, then only whole records of the
    // batch -- never a record that was not written.
    for preallocate in [false, true] {
        let names = torn_batch_case(preallocate, false).expect("a boundary cut must read back");
        assert_eq!(names.first().map(String::as_str), Some("a0"), "{names:?}");
        assert!(names.len() <= 2 && names.iter().all(|n| ["a0", "b1"].contains(&n.as_str())), "{names:?}");
    }
    // Cut inside a record, growing layout: the partial frame is a torn tail and is dropped.
    let names = torn_batch_case(false, true).expect("a torn tail must be dropped, not refused");
    assert_eq!(names.first().map(String::as_str), Some("a0"), "{names:?}");
    assert!(names.len() <= 2 && names.iter().all(|n| ["a0", "b1"].contains(&n.as_str())), "{names:?}");
}

#[test]
fn group_write_a_torn_frame_in_a_preallocated_reservation_is_never_replayed_as_a_record() {
    // Preallocated layout, cut inside a record: the zeros of the reservation follow the partial
    // frame, so the scan can see a "complete" frame whose digest fails. Whether recovery refuses or
    // drops it is the existing tail logic's business; what this pins is that a torn record NEVER
    // comes back as a record.
    if let Some(names) = torn_batch_case(true, true) {
        assert!(names.iter().all(|n| ["a0", "b1"].contains(&n.as_str())), "{names:?}");
    }
}

#[test]
fn group_write_a_failed_batch_is_retried_whole_and_never_duplicated() {
    park_flusher();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut last = 0;
    for name in ["r1", "r2", "r3"] {
        last = store.append_for_group_commit(1, set(name, 100), vec![], vec![]).unwrap().sequence;
    }
    TORN_BATCH_WRITE_BYTES.store(150, Ordering::SeqCst);
    assert!(store.commit_barrier(1, last).is_err());
    // The next barrier retries the same bytes over the same place: nothing was lost with the
    // failed write, and the torn bytes are overwritten rather than followed by a second copy.
    store.commit_barrier(1, last).unwrap();
    let records = read_back(dir.path());
    assert_eq!(keys(&records), vec!["r1", "r2", "r3"]);
    assert_eq!(records.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[test]
fn group_write_a_failed_batch_does_not_let_a_later_writer_be_told_it_is_durable() {
    park_flusher();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = store.append_for_group_commit(1, set("first", 100), vec![], vec![]).unwrap();
    let second = store.append_for_group_commit(1, set("second", 100), vec![], vec![]).unwrap();
    TORN_BATCH_WRITE_BYTES.store(10, Ordering::SeqCst);
    // The leader's write fails...
    assert!(store.commit_barrier(1, first.sequence).is_err());
    // ...and a follower whose record sat in that same batch must NOT find itself "covered".
    TORN_BATCH_WRITE_BYTES.store(10, Ordering::SeqCst);
    assert!(
        store.commit_barrier(1, second.sequence).is_err(),
        "a follower in a failed batch was acknowledged"
    );
    TORN_BATCH_WRITE_BYTES.store(-1, Ordering::SeqCst);
}

#[test]
fn group_write_readers_see_records_that_are_still_buffered() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    for index in 0..5 {
        store.append_for_group_commit(1, set(&format!("v{index}"), 32), vec![], vec![]).unwrap();
    }
    let (records, _) = store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(records.len(), 5, "a scan must see what was appended, buffered or not");
    // A legacy append after buffered ones lands after them, not on top of them.
    store.append_with_sync(1, set("legacy", 32), true).unwrap();
    let (records, _) = store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
    assert_eq!(records.len(), 6);
    assert_eq!(
        records.iter().map(|(_, r)| r.sequence).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6]
    );
}

#[test]
fn group_write_concurrent_writers_share_writes_and_every_acked_record_is_durable() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let threads = 8;
    let per_thread = 60;
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let store = store.clone();
            std::thread::spawn(move || {
                let mut acked = Vec::new();
                for i in 0..per_thread {
                    let name = format!("t{t}-{i}");
                    let seq = store
                        .append_for_group_commit(1, set(&name, 144), vec![], vec![])
                        .unwrap()
                        .sequence;
                    store.commit_barrier(1, seq).unwrap();
                    acked.push(name);
                }
                acked
            })
        })
        .collect();
    let acked: Vec<String> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    let (writes, records) = store.group_write_counts();
    assert_eq!(records as usize, threads * per_thread);
    assert!(writes < records, "no batching happened: {writes} writes for {records} records");
    let mut on_disk = keys(&read_back(dir.path()));
    on_disk.sort();
    let mut expected = acked;
    expected.sort();
    assert_eq!(on_disk, expected, "every acknowledged record must be on disk exactly once");
    let sequences: Vec<u64> = read_back(dir.path()).iter().map(|(s, _)| *s).collect();
    assert_eq!(sequences, (1..=(threads * per_thread) as u64).collect::<Vec<_>>());
}

#[test]
fn group_write_batches_that_cross_block_boundaries_and_rolls_read_back_whole() {
    let dir = tempfile::tempdir().unwrap();
    set_wal_segment_bytes_for_test(Some(256 * 1024));
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut expected = Vec::new();
    for round in 0..12 {
        let mut last = 0;
        for i in 0..4 {
            let name = format!("big{round}-{i}");
            last = store
                .append_for_group_commit(1, set(&name, 20 * 1024), vec![], vec![])
                .unwrap()
                .sequence;
            expected.push(name);
        }
        store.commit_barrier(1, last).unwrap();
    }
    let (records, _) = store.scan_decoded(1, 0, u64::MAX, u64::MAX).unwrap();
    set_wal_segment_bytes_for_test(None);
    let names: Vec<String> = records
        .iter()
        .map(|(_, r)| match r.command.as_ref().unwrap() {
            Command::StringSet { key, .. } => key.clone(),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(names, expected);
}

#[test]
fn group_write_off_writes_once_per_record() {
    let dir = tempfile::tempdir().unwrap();
    GROUP_WRITE_OVERRIDE.with(|v| v.set(Some(false)));
    let store = LocalWriteAheadLogStore::new(dir.path());
    for index in 0..4 {
        store.append_for_group_commit(1, set(&format!("o{index}"), 32), vec![], vec![]).unwrap();
    }
    GROUP_WRITE_OVERRIDE.with(|v| v.set(None));
    assert_eq!(store.group_write_counts(), (0, 0), "the buffer must be bypassed when off");
    assert_eq!(read_back(dir.path()).len(), 4);
}
