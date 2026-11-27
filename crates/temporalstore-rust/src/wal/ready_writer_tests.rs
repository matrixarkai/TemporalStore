// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

struct Buffered(Option<bool>);
impl Buffered {
    fn new() -> Self {
        Self(GROUP_WRITE_OVERRIDE.with(|mode| mode.replace(Some(true))))
    }
}
impl Drop for Buffered {
    fn drop(&mut self) {
        GROUP_WRITE_OVERRIDE.with(|mode| mode.set(self.0));
        set_torn_batch_write(-1);
        set_wal_segment_bytes_for_test(None);
        READY_AFTER_TAKE.with(|slot| *slot.borrow_mut() = None);
        READY_BEFORE_IDLE_SKIP.with(|slot| *slot.borrow_mut() = None);
    }
}

fn command(value: u64) -> Command {
    Command::StringSet {
        key: format!("ready-{value}"),
        value: value.to_le_bytes().to_vec(),
    }
}

fn buffer(store: &LocalWriteAheadLogStore, shard: ShardId, value: u64) -> u64 {
    store
        .append_for_group_commit(shard, command(value), vec![], vec![])
        .unwrap()
        .sequence
}

#[test]
fn cold_observations_do_not_publish_or_enroll_writers_with_flusher_enabled() {
    let _buffered = Buffered::new();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    store.append_with_sync(1, command(1), true).unwrap();
    assert!(store.flusher_started.load(AtomicOrdering::Acquire));
    let owners = store.inner.writers.lock().unwrap().len();
    for shard in 2..10_002 {
        assert_eq!(store.cached_last_sequence(shard), 0);
        assert_eq!(store.raw_stats(shard), WriteAheadLogStats::default());
        assert_eq!(store.block_retention_floor(shard), None);
    }
    assert_eq!(store.inner.writers.lock().unwrap().len(), owners);
    assert!(store.inner.ready.lock().unwrap().iter().all(|id| *id == 1));
}

#[test]
fn one_hot_writer_tick_visits_only_ready_work_not_idle_owners() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    // Real existing owners without payloads must not become periodic work either.
    for shard in 2..1002 {
        store.set_block_retention_floor(shard, 1);
    }
    assert_eq!(buffer(&store, 1, 1), 1);
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.flush_ready_once(), 1); // bounded retirement of successful work
    assert_eq!(store.flush_ready_once(), 0);
    assert_eq!(store.inner.writers.lock().unwrap().len(), 1001);
    assert_eq!(store.info(1).unwrap().last_flushed_sequence, 1);
    assert_eq!(buffer(&store, 1, 2), 2);
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.flush_ready_once(), 0);
}

#[test]
fn ready_tick_requeues_busy_writer_and_coordinator_without_waiting() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    buffer(&store, 1, 1);
    let owner = store.shard_writer(1);
    {
        let _held = owner.lock_inner_raw();
        assert_eq!(store.flush_ready_once(), 1);
    }
    assert!(store.inner.ready.lock().unwrap().contains(&1));
    {
        let _held = owner.coordinator.state.lock().unwrap();
        assert_eq!(store.flush_ready_once(), 1);
    }
    assert!(store.inner.ready.lock().unwrap().contains(&1));
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.info(1).unwrap().last_flushed_sequence, 1);
}

#[test]
fn failed_ready_drain_is_retried_without_another_append() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    buffer(&store, 1, 1);
    let owner = store.shard_writer(1);
    let before = owner.lock_inner_raw().pending_total;
    assert!(before > 0);
    set_torn_batch_write(4);
    assert_eq!(store.flush_ready_once(), 1);
    set_torn_batch_write(-1);
    assert_eq!(owner.lock_inner_raw().pending_total, before);
    assert_eq!(store.raw_stats(1).last_flushed_sequence, 0);
    assert!(store.inner.ready.lock().unwrap().contains(&1));
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(owner.lock_inner_raw().pending_total, 0);
    assert_eq!(store.info(1).unwrap().last_flushed_sequence, 1);
    assert_eq!(store.scan(1, 0, u64::MAX, u64::MAX).unwrap().len(), 1);
}

#[test]
fn append_during_ready_dequeue_is_not_lost() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    buffer(&store, 1, 1);
    let append = store.clone();
    READY_AFTER_TAKE.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            assert_eq!(buffer(&append, 1, 2), 2);
            assert_eq!(buffer(&append, 2, 3), 1);
        }))
    });
    assert_eq!(store.flush_ready_once(), 1);
    assert!(store.inner.ready.lock().unwrap().contains(&2));
    assert_eq!(store.flush_ready_once(), 2);
    assert_eq!(store.info(1).unwrap().last_flushed_sequence, 2);
    assert_eq!(store.info(2).unwrap().last_flushed_sequence, 1);
}

#[test]
fn append_after_idle_snapshot_keeps_its_new_enrollment() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    buffer(&store, 1, 1);
    assert_eq!(store.flush_ready_once(), 1);
    let append = store.clone();
    READY_BEFORE_IDLE_SKIP.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            assert_eq!(buffer(&append, 1, 2), 2);
        }))
    });
    assert_eq!(store.flush_ready_once(), 1);
    assert!(store.inner.ready.lock().unwrap().contains(&1));
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.info(1).unwrap().last_flushed_sequence, 2);
}

#[test]
fn ready_callbacks_can_reenter_their_thread_local_slots() {
    let _buffered = Buffered::new();
    set_wal_segment_bytes_for_test(Some(0));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    buffer(&store, 1, 1);
    READY_AFTER_TAKE.with(|slot| *slot.borrow_mut() = Some(Box::new(|| {
        READY_AFTER_TAKE.with(|slot| assert!(slot.borrow().is_none()));
    })));
    assert_eq!(store.flush_ready_once(), 1);
    READY_BEFORE_IDLE_SKIP.with(|slot| *slot.borrow_mut() = Some(Box::new(|| {
        READY_BEFORE_IDLE_SKIP.with(|slot| assert!(slot.borrow().is_none()));
    })));
    assert_eq!(store.flush_ready_once(), 1);
    assert_eq!(store.flush_ready_once(), 0);
}
