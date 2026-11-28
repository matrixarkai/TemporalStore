// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc;
use std::time::Duration;

struct RollPolicy(Option<u64>);
impl RollPolicy {
    fn sealed() -> Self { Self(SEGMENT_BYTES_OVERRIDE.with(|slot| slot.replace(Some(1)))) }
}
impl Drop for RollPolicy {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(self.0);
        point_readers::FAIL_NEXT_OPEN.with(|slot| slot.set(None));
    }
}

fn seed(store: &LocalWriteAheadLogStore, shard: ShardId) -> (u64, u64) {
    let blocks = (0..128).map(|object_id| StagedBlock {
        object_id, element_name: None,
        bytes: (0..1024).map(|offset| ((offset + object_id * 17 + shard) % 251) as u8).collect(),
    }).collect();
    let (record, id) = store.append_batch_as_one_record_at(shard, Vec::new(), blocks, false).unwrap();
    (record.sequence, id)
}

fn load_exact(store: &LocalWriteAheadLogStore, shard: ShardId, position: (u64, u64)) {
    let result = read(store, shard, position.1, position.0, 63, 63, None).unwrap().unwrap();
    let CarriedBlockRead::Block(bytes) = result else { panic!("expected indexed payload"); };
    let expected: Vec<_> = (0..1024).map(|offset| ((offset + 63 * 17 + shard) % 251) as u8).collect();
    assert_eq!(bytes, expected);
}

#[test]
fn all_shards_share_admission_and_metadata_but_not_completion_counts() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let first = store.shard_writer(1);
    let second = store.shard_writer(2);
    let one = first.lock_inner_raw();
    let two = second.lock_inner_raw();
    assert!(Arc::ptr_eq(&one.point_reads, &two.point_reads));
    assert!(Arc::ptr_eq(&one.point_reads, &store.inner.point_reads));
    assert!(!Arc::ptr_eq(&one.point_read_counts, &two.point_read_counts));
    one.point_read_counts.complete(19);
    assert_eq!(one.point_read_counts.stats(one.stats).reads, 1);
    assert_eq!(two.point_read_counts.stats(two.stats).reads, 0);
}

#[test]
fn ninth_multishard_prepared_read_waits_for_a_store_permit() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let positions: Vec<_> = (1..=9).map(|shard| seed(&store, shard)).collect();
    let mut held: Vec<_> = (1..=8).map(|shard| {
        let position = positions[shard as usize - 1];
        prepare(&store, shard, position.1, position.0).unwrap().unwrap()
    }).collect();
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), MAX_IN_FLIGHT);
    let (started, ready) = mpsc::channel();
    let (done, result) = mpsc::channel();
    let peer = store.clone();
    let position = positions[8];
    let worker = std::thread::spawn(move || {
        started.send(()).unwrap();
        let prepared = prepare(&peer, 9, position.1, position.0).unwrap().unwrap();
        done.send(()).unwrap();
        drop(prepared);
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(result.recv_timeout(Duration::from_millis(100)), Err(mpsc::RecvTimeoutError::Timeout)));
    drop(held.pop());
    result.recv_timeout(Duration::from_secs(5)).unwrap();
    worker.join().unwrap();
    drop(held);
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(9), None);
}

#[cfg(unix)]
#[test]
fn descriptor_pressure_evicts_peer_idle_cache_after_releasing_own_writer() {
    let _policy = RollPolicy::sealed();
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let first = seed(&store, 1);
    let second = seed(&store, 2);
    load_exact(&store, 1, first);
    assert_eq!(store.inner.reader_budget.snapshot()[0], 1);
    point_readers::FAIL_NEXT_OPEN.with(|slot| slot.set(Some(24)));
    load_exact(&store, 2, second);
    // One peer descriptor left, one target descriptor admitted; no growth or leaked permit.
    assert_eq!(store.inner.reader_budget.snapshot()[0], 1);
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(2), None);
    assert_eq!(store.raw_stats(1).reads, 1);
    assert_eq!(store.raw_stats(2).reads, 1);
}

#[cfg(unix)]
#[test]
fn busy_peer_or_directory_is_never_waited_on_and_failed_read_releases_resources() {
    let _policy = RollPolicy::sealed();
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let first = seed(&store, 1);
    let second = seed(&store, 2);
    load_exact(&store, 1, first);
    let owner = store.shard_writer(1);
    let held = owner.lock_inner_raw();
    point_readers::FAIL_NEXT_OPEN.with(|slot| slot.set(Some(24)));
    let error = match prepare(&store, 2, second.1, second.0) {
        Err(error) => error,
        _ => panic!("busy peer must not turn descriptor exhaustion into success"),
    };
    assert!(point_readers::descriptor_pressure(&error));
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(2), None);
    drop(held);
    let directory = store.inner.writers.lock().unwrap();
    assert!(!store.evict_idle_reader_for_open(2));
    drop(directory);
    assert!(store.evict_idle_reader_for_open(2));
    assert_eq!(store.inner.reader_budget.snapshot()[0], 0);
    load_exact(&store, 2, second);
}

#[cfg(unix)]
#[test]
fn unrelated_open_errors_do_not_evict_an_idle_peer() {
    let _policy = RollPolicy::sealed();
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let first = seed(&store, 1);
    let second = seed(&store, 2);
    load_exact(&store, 1, first);
    point_readers::FAIL_NEXT_OPEN.with(|slot| slot.set(Some(13)));
    assert!(prepare(&store, 2, second.1, second.0).is_err());
    assert_eq!(store.inner.reader_budget.snapshot()[0], 1);
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(2), None);
}

#[test]
fn completion_uses_prepared_owner_without_reentering_the_directory() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let position = seed(&store, 1);
    let mut prepared = prepare(&store, 1, position.1, position.0).unwrap().unwrap();
    let (result, bytes) = prepared.load(63, 63, None);
    let held = store.inner.writers.lock().unwrap();
    let result = finish(&store, prepared, result, bytes).unwrap().unwrap();
    drop(held);
    let CarriedBlockRead::Block(actual) = result else { panic!("expected indexed payload"); };
    let expected: Vec<_> = (0..1024).map(|offset| ((offset + 63 * 17 + 1) % 251) as u8).collect();
    assert_eq!(actual, expected);
    assert_eq!(store.raw_stats(1).reads, 1);
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(1), None);
}

#[cfg(unix)]
#[test]
fn prepared_handle_keeps_no_writer_or_append_flock_alive() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    let position = seed(&store, 1);
    let prepared = prepare(&store, 1, position.1, position.0).unwrap().unwrap();
    let owner = prepared.writer.clone();
    let state = store.inner.point_reads.clone();
    assert_eq!(*state.active.lock().unwrap(), 1);
    drop(store);
    assert!(owner.upgrade().is_none());
    let replacement = LocalWriteAheadLogStore::new(root.path());
    assert_eq!(replacement.append_with_sync(1, Command::StringSet {
        key: "replacement-owner".into(), value: vec![1],
    }, false).unwrap().sequence, 2);
    drop(prepared);
    assert_eq!(*state.active.lock().unwrap(), 0);
}

#[cfg(unix)]
#[test]
fn descriptor_pressure_reaches_idle_owner_after_more_than_32_empty_owners() {
    let _policy = RollPolicy::sealed();
    let root = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(root.path());
    for shard in 2..=48 { store.set_block_retention_floor(shard, 1); }
    let target = seed(&store, 1);
    let late = seed(&store, 100);
    load_exact(&store, 100, late);
    assert_eq!(store.inner.reader_budget.snapshot()[0], 1);
    point_readers::FAIL_NEXT_OPEN.with(|slot| slot.set(Some(24)));
    load_exact(&store, 1, target);
    // The late owner's sole idle FD was reclaimed and the exact target read retried.
    assert_eq!(store.inner.reader_budget.snapshot()[0], 1);
    assert!(!store.evict_idle_reader_for_open(1));
    assert_eq!(store.raw_stats(100).reads, 1);
    assert_eq!(store.raw_stats(1).reads, 1);
    assert_eq!(*store.inner.point_reads.active.lock().unwrap(), 0);
    assert_eq!(store.block_retention_floor(1), None);
}
