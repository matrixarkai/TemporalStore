// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc;
use std::time::Duration;

struct SegmentPolicy(Option<u64>);

impl SegmentPolicy {
    fn active() -> Self {
        Self(SEGMENT_BYTES_OVERRIDE.with(|value| value.replace(Some(0))))
    }
}

impl Drop for SegmentPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
    }
}

fn staged(seed: u8) -> Vec<StagedBlock> {
    (0..128u64).map(|index| StagedBlock {
        object_id: index,
        element_name: None,
        bytes: (0..1024u64).map(|offset| ((offset + index * 17 + u64::from(seed)) % 251) as u8).collect(),
    }).collect()
}

fn append(store: &LocalWriteAheadLogStore, seed: u8) -> (u64, u64) {
    let (record, id) = store.append_batch_as_one_record_at(1, Vec::new(), staged(seed), false).unwrap();
    (record.sequence, id)
}

fn block(result: Result<Option<CarriedBlockRead>, WriteAheadLogError>) -> Vec<u8> {
    match result.unwrap() {
        Some(CarriedBlockRead::Block(bytes)) => bytes,
        _ => panic!("expected an indexed block"),
    }
}

#[test]
fn prepared_io_and_completion_do_not_wait_for_writer_mutex() {
    let _policy = SegmentPolicy::active();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let (sequence, id) = append(&store, 1);
    let before = store.raw_stats(1);
    let mut prepared = prepare(&store, 1, id, sequence).unwrap().unwrap();
    let shard_writer = store.shard_writer(1);
    let held = shard_writer.lock_inner_raw();
    let (result, bytes) = prepared.load(63, 63, None);
    assert_eq!(block(finish(&store, prepared, result, bytes)), staged(1)[63].bytes);
    assert_eq!(*held.point_reads.active.lock().unwrap(), 0);
    assert_eq!(held.point_read_counts.stats(held.stats).reads, before.reads + 1);
    drop(held);
    assert_eq!(store.block_retention_floor(1), None);
    let after = store.raw_stats(1);
    assert_eq!(after.reads, before.reads + 1);
    assert_eq!(after.bytes_read, before.bytes_read + bytes);
    assert_eq!(store.stats_without_rescan(1).reads, after.reads);
    assert_eq!(store.stats(1).reads, after.reads);
}

#[test]
fn leased_prepared_reads_survive_prefix_rewrite() {
    let _policy = SegmentPolicy::active();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    append(&store, 1);
    let (sequence, id) = append(&store, 2);
    let (tail, _) = append(&store, 3);
    let mut prepared = prepare(&store, 1, id, sequence).unwrap().unwrap();
    let held = store.gc_before_sequence_unchecked(1, tail).unwrap();
    assert!(held.clamped_by_block_retention);
    assert_eq!(held.effective_retain_from_sequence, sequence);
    assert!(store.base_offset(1).unwrap() > 0);
    let (result, bytes) = prepared.load(127, 127, None);
    assert_eq!(block(finish(&store, prepared, result, bytes)), staged(2)[127].bytes);
    assert_eq!(store.block_retention_floor(1), None);
    let released = store.gc_before_sequence_unchecked(1, tail).unwrap();
    assert!(!released.clamped_by_block_retention);
    assert_eq!(released.effective_retain_from_sequence, tail);
    assert!(read(&store, 1, id, sequence, 127, 127, None).unwrap().is_none());
}

#[test]
fn independent_reads_finish_together_while_the_writer_mutex_is_held() {
    let _policy = SegmentPolicy::active();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let (sequence, id) = append(&store, 4);
    let before = store.raw_stats(1).reads;
    let plans: Vec<_> = (0..MAX_IN_FLIGHT).map(|_| prepare(&store, 1, id, sequence).unwrap().unwrap()).collect();
    let ready = Arc::new(std::sync::Barrier::new(MAX_IN_FLIGHT));
    let (sender, receiver) = mpsc::channel();
    let shard_writer = store.shard_writer(1);
    let held = shard_writer.lock_inner_raw();
    let threads: Vec<_> = plans.into_iter().enumerate().map(|(index, mut plan)| {
        let ready = Arc::clone(&ready);
        let sender = sender.clone();
        let reader = store.clone();
        std::thread::spawn(move || {
            ready.wait();
            let (result, bytes) = plan.load(index, index as u64, None);
            let bytes = block(finish(&reader, plan, result, bytes));
            sender.send((index, bytes)).unwrap();
        })
    }).collect();
    let expected = staged(4);
    for _ in 0..MAX_IN_FLIGHT {
        let (index, bytes) = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(bytes, expected[index].bytes);
    }
    assert_eq!(held.point_read_counts.stats(held.stats).reads, before + MAX_IN_FLIGHT as u64);
    assert_eq!(*held.point_reads.active.lock().unwrap(), 0);
    drop(held);
    for thread in threads { thread.join().unwrap(); }
    assert_eq!(store.block_retention_floor(1), None);
}

#[test]
fn admission_is_bounded_and_released_without_the_writer_mutex() {
    let state = Arc::new(State::default());
    let mut permits: Vec<_> = (0..MAX_IN_FLIGHT).map(|_| Arc::clone(&state).enter()).collect();
    let (started_tx, started_rx) = mpsc::channel();
    let (permit_tx, permit_rx) = mpsc::channel();
    let queued = Arc::clone(&state);
    let thread = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        permit_tx.send(queued.enter()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(*state.active.lock().unwrap(), MAX_IN_FLIGHT);
    assert!(permit_rx.try_recv().is_err());
    drop(permits.pop());
    let last = permit_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(*state.active.lock().unwrap(), MAX_IN_FLIGHT);
    drop(permits);
    drop(last);
    thread.join().unwrap();
    assert_eq!(*state.active.lock().unwrap(), 0);
}

#[test]
fn missing_and_invalid_reads_release_admission_and_retention() {
    let _policy = SegmentPolicy::active();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    assert!(read(&store, 1, 0, 1, 0, 0, None).unwrap().is_none());
    let (sequence, id) = append(&store, 1);
    let before = store.raw_stats(1).reads;
    assert!(read(&store, 1, id, sequence, 0, u64::MAX, None).is_err());
    assert_eq!(store.block_retention_floor(1), None);
    let shard_writer = store.shard_writer(1);
    let inner = shard_writer.lock_inner_raw();
    assert_eq!(*inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(inner.point_read_counts.stats(inner.stats).reads, before + 1);
}

#[test]
fn older_record_encodings_keep_the_checked_fallback() {
    let _policy = SegmentPolicy::active();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let expected = vec![7; 64];
    let (record, id) = store.append_batch_as_one_record_at(1, Vec::new(), vec![StagedBlock {
        object_id: 9, element_name: None, bytes: expected.clone(),
    }], false).unwrap();
    let Some(CarriedBlockRead::Record(bytes)) = read(&store, 1, id, record.sequence, 0, 9, None).unwrap() else {
        panic!("expected the existing record fallback");
    };
    assert_eq!(decode_carried_blocks(&bytes).unwrap()[0].bytes, expected);
    assert_eq!(store.block_retention_floor(1), None);
}
