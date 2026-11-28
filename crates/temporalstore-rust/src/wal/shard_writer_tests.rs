// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn command(value: u64) -> Command {
    Command::StringSet {
        key: format!("key-{value}"),
        value: value.to_le_bytes().to_vec(),
    }
}

fn decoded_records(store: &LocalWriteAheadLogStore, shard: ShardId) -> Vec<WriteAheadLogRecord> {
    let (rows, truncated) = store.scan_decoded(shard, 0, u64::MAX, u64::MAX).unwrap();
    assert!(
        !truncated,
        "exact verification requires the complete WAL window"
    );
    rows.into_iter().map(|(_, record)| record).collect()
}

fn parked_owner_allows_peer(phase: &'static str, buffered: bool) {
    let directory = tempfile::tempdir().unwrap();
    // Leave real on-disk history for the cold path to reconcile, not an empty warm cache.
    {
        let seed = LocalWriteAheadLogStore::new(directory.path());
        seed.append_with_sync(1, command(0), false).unwrap();
        seed.flush(1).unwrap();
    }
    let store = LocalWriteAheadLogStore::new(directory.path());
    set_wal_segment_bytes_for_test(Some(0));
    if !phase.starts_with("cold_") {
        for value in 1..20 {
            store.append_with_sync(1, command(value), false).unwrap();
        }
    }
    if buffered {
        store
            .append_for_group_commit(1, command(20), Vec::new(), Vec::new())
            .unwrap();
    }
    let (paused, ready) = mpsc::sync_channel(0);
    let (release, resume) = mpsc::channel();
    let first = store.clone();
    let parked = std::thread::spawn(move || {
        set_wal_segment_bytes_for_test(Some(0));
        GROUP_WRITE_OVERRIDE.with(|mode| mode.set(Some(buffered)));
        let hook: Box<dyn FnOnce()> = Box::new(move || {
            paused.send(()).unwrap();
            resume.recv_timeout(Duration::from_secs(15)).unwrap();
        });
        if phase == "sync" {
            SELECTED_FLUSH_BEFORE_SYNC.with(|slot| *slot.borrow_mut() = Some(hook));
        } else {
            SHARD_WRITER_PHASE.with(|slot| *slot.borrow_mut() = Some((phase, hook)));
        }
        if phase.starts_with("reclaim_") {
            first.gc_before_sequence_unchecked(1, 10).unwrap();
        } else if phase == "sync" {
            first.flush(1).unwrap();
        } else if buffered {
            first
                .append_for_group_commit(1, command(20), Vec::new(), Vec::new())
                .unwrap();
        } else {
            first.append_with_sync(1, command(20), false).unwrap();
        }
    });
    ready.recv_timeout(Duration::from_secs(5)).unwrap();
    // Pointer lookup itself must not wait for cold filesystem work or payload work.
    let one = store.shard_writer(1);
    if phase == "encode" && buffered {
        assert!(one.coordinator.buffered_append.try_lock().is_err());
    } else if phase != "sync" {
        assert!(one.inner.try_lock().is_err());
    }
    let (done, receipt) = mpsc::channel();
    let peer = store.clone();
    let peer_thread = std::thread::spawn(move || {
        let record = peer.append_with_sync(2, command(42), true).unwrap();
        let rows = decoded_records(&peer, 2);
        let durable = peer.info(2).unwrap().last_flushed_sequence;
        done.send((record, rows, durable)).unwrap();
    });
    let result = receipt.recv_timeout(Duration::from_secs(5));
    release.send(()).unwrap();
    parked.join().unwrap();
    peer_thread.join().unwrap();
    let (record, rows, durable) =
        result.expect("peer public append/read/barrier blocked by another shard");
    assert_eq!(record.sequence, 1);
    assert_eq!(record.command, Some(command(42)));
    assert_eq!(rows, vec![record]);
    assert_eq!(durable, 1);
    assert!(!Arc::ptr_eq(&one, &store.shard_writer(2)));
    assert!(store.same_log(&store.clone()));
    let rows = decoded_records(&store, 1);
    assert!(rows
        .windows(2)
        .all(|rows| rows[0].sequence + 1 == rows[1].sequence));
    set_wal_segment_bytes_for_test(None);
}

#[test]
fn cold_open_and_scan_are_shard_owned() {
    parked_owner_allows_peer("cold_open", false);
}

#[test]
fn cold_tail_reconciliation_is_shard_owned() {
    parked_owner_allows_peer("cold_scan", false);
}

#[test]
fn unbuffered_encoding_is_shard_owned() {
    parked_owner_allows_peer("encode", false);
}

#[test]
fn buffered_encoding_is_shard_owned() {
    parked_owner_allows_peer("encode", true);
}

#[cfg(unix)]
#[test]
fn syncing_one_owner_does_not_block_a_peer() {
    parked_owner_allows_peer("sync", false);
}

#[test]
fn reclaim_planning_is_shard_owned() {
    parked_owner_allows_peer("reclaim_plan", false);
}

#[test]
fn reclaim_publication_is_shard_owned() {
    parked_owner_allows_peer("reclaim_publish", false);
}

#[test]
fn racing_first_touches_publish_one_owner_and_one_ordered_stream() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(directory.path());
    let start = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|value| {
            let store = store.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                let owner = store.shard_writer(17);
                let row = store.append_with_sync(17, command(value), true).unwrap();
                (owner, row)
            })
        })
        .collect();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert!(results
        .iter()
        .all(|(owner, _)| Arc::ptr_eq(owner, &results[0].0)));
    let mut sequences: Vec<_> = results.iter().map(|(_, row)| row.sequence).collect();
    sequences.sort_unstable();
    assert_eq!(sequences, (1..=8).collect::<Vec<_>>());
    let rows = decoded_records(&store, 17);
    for (_, expected) in &results {
        assert_eq!(&rows[(expected.sequence - 1) as usize], expected);
    }
    assert_eq!(store.info(17).unwrap().last_flushed_sequence, 8);
}

#[test]
fn failed_buffered_write_is_local_and_preserves_order_and_read_lease() {
    struct Mode(Option<bool>);
    impl Drop for Mode {
        fn drop(&mut self) {
            set_torn_batch_write(-1);
            GROUP_WRITE_OVERRIDE.with(|mode| mode.set(self.0));
        }
    }
    let _mode = Mode(GROUP_WRITE_OVERRIDE.with(|mode| mode.replace(Some(true))));
    let directory = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(directory.path());
    store.append_with_sync(1, command(1), false).unwrap();
    let second = store
        .append_for_group_commit(1, command(2), Vec::new(), Vec::new())
        .unwrap();
    assert_eq!(second.sequence, 2);
    let lease = store.retain_read_sequence(1, 1);
    let owner = store.shard_writer(1);
    let before = owner.lock_inner_raw().pending_total;
    assert!(before > 0);
    set_torn_batch_write(4);
    let failed = store.flush(1);
    set_torn_batch_write(-1);
    assert!(failed.is_err());
    assert_eq!(owner.lock_inner_raw().pending_total, before);
    let peer = store.append_with_sync(2, command(30), false).unwrap();
    assert_eq!(decoded_records(&store, 2), vec![peer]);
    assert_eq!(store.flush(2).unwrap().last_sequence, 1);
    assert_eq!(
        owner.lock_inner_raw().pending_total,
        before,
        "peer read and sync must not drain failed owner"
    );
    assert!(lease.same_log(&store));
    assert_eq!(store.flush(1).unwrap().last_sequence, 2);
    assert_eq!(owner.lock_inner_raw().pending_total, 0);
    assert_eq!(decoded_records(&store, 1)[1], second);
    let retained = store.gc_before_sequence_unchecked(1, 2).unwrap();
    assert!(retained.clamped_by_block_retention);
    assert_eq!(retained.effective_retain_from_sequence, 1);
    drop(lease);
    assert_eq!(store.block_retention_floor(1), None);
    assert_eq!(
        store
            .append_with_sync(1, command(3), false)
            .unwrap()
            .sequence,
        3
    );
}

#[test]
fn shard_owner_lifetime_and_log_identity_do_not_cycle() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(directory.path());
    let identity = store.weak_identity();
    let lease = store.retain_read_sequence(19, 2);
    let owner = Arc::downgrade(&store.shard_writer(19));
    assert!(lease.same_log(&store));
    drop(store);
    assert!(!identity.is_live());
    assert!(owner.upgrade().is_none());
    let replacement = LocalWriteAheadLogStore::new(directory.path());
    assert!(!lease.same_log(&replacement));
    drop(lease);
}

#[test]
fn registration_observers_do_not_create_a_cold_owner_or_ready_work() {
    let directory = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(directory.path());
    assert_eq!(store.registered_block_floor(99, 7), None);
    assert!(!store.registered_block_span_has_holes(99, 7));
    assert!(store.inner.writers.lock().unwrap().is_empty());
    assert!(store.inner.ready.lock().unwrap().is_empty());
    store.replace_block_location(99, 7, 12, 1, None);
    assert_eq!(store.registered_block_floor(99, 7), Some(12));
    assert!(!store.registered_block_span_has_holes(99, 7));
    assert_eq!(store.registered_block_floor(99, 8), None);
    assert!(!store.registered_block_span_has_holes(99, 8));
    assert_eq!(store.inner.writers.lock().unwrap().len(), 1);
    assert!(store.inner.ready.lock().unwrap().is_empty());
    store.remove_block_location(99, 7, 12, 1);
    assert_eq!(store.registered_block_floor(99, 7), None);
}
