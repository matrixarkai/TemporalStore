// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn acquiring_and_releasing_a_read_lease_does_not_wait_for_wal_io() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let held = store.inner.lock().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = store.clone();
    let started = Arc::new(std::sync::Barrier::new(2));
    let ready = started.clone();
    let thread = std::thread::spawn(move || {
        ready.wait();
        sender.send(reader.retain_read_sequence(1, 5)).unwrap();
    });
    started.wait();
    let mut lease = receiver
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    lease.retain_earlier(2);
    drop(lease);
    drop(held);
    thread.join().unwrap();
    assert_eq!(store.block_retention_floor(1), None);
}

#[test]
fn read_leases_scope_floors_and_reuse_slots_without_retaining_the_owner() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let identity = store.weak_identity();
    store.set_block_retention_floor(1, 20);
    let mut first = store.retain_read_sequence(1, 10);
    let second = store.retain_read_sequence(1, 5);
    let other = store.retain_read_sequence(2, 2);
    assert_eq!(store.block_retention_floor(1), Some(5));
    assert_eq!(store.block_retention_floor(2), Some(2));
    first.retain_earlier(3);
    first.retain_earlier(15);
    assert_eq!(store.block_retention_floor(1), Some(3));
    store.clear_block_retention_floor(1);
    drop(first);
    assert_eq!(store.block_retention_floor(1), Some(5));
    drop(second);
    assert_eq!(store.block_retention_floor(1), None);
    let coordinator = store.group_commit_coordinator(1);
    let count = coordinator.read_retention.lock().unwrap().slots.len();
    for _ in 0..100 {
        let lease = store.retain_read_sequence(1, 7);
        assert_eq!(store.block_retention_floor(1), Some(7));
        drop(lease);
    }
    assert_eq!(coordinator.read_retention.lock().unwrap().slots.len(), count);
    drop(coordinator);
    drop(store);
    assert!(!identity.is_live());
    drop(other);
}

#[test]
fn reclaim_preserves_a_leased_record_and_advances_after_release() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut positions = Vec::new();
    for seed in 1..=3 {
        let (record, log_id) = store
            .append_batch_as_one_record_at(
                1,
                Vec::new(),
                vec![StagedBlock {
                    object_id: seed,
                    element_name: None,
                    bytes: vec![seed as u8; 1024],
                }],
                false,
            )
            .unwrap();
        positions.push((record.sequence, log_id));
    }
    let lease = store.retain_read_sequence(1, positions[0].0);
    let other_dir = tempfile::tempdir().unwrap();
    let other = LocalWriteAheadLogStore::new(other_dir.path());
    assert!(!lease.same_log(&other));
    assert_eq!(other.block_retention_floor(1), None);
    let held = store
        .gc_before_sequence_unchecked(1, positions[2].0)
        .unwrap();
    assert!(held.clamped_by_block_retention);
    assert_eq!(held.effective_retain_from_sequence, positions[0].0);
    assert!(store
        .read_frame_at_log_id(1, positions[0].1)
        .unwrap()
        .is_some());
    drop(lease);
    let released = store
        .gc_before_sequence_unchecked(1, positions[2].0)
        .unwrap();
    assert!(!released.clamped_by_block_retention);
    assert_eq!(released.effective_retain_from_sequence, positions[2].0);
    assert!(store
        .read_frame_at_log_id(1, positions[0].1)
        .unwrap()
        .is_none());
    assert!(store
        .read_frame_at_log_id(1, positions[2].1)
        .unwrap()
        .is_some());
}

#[test]
fn holding_one_shard_retention_does_not_hold_a_peer_lease_or_registration() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let held_coordinator = store.group_commit_coordinator(1);
    let held = held_coordinator.read_retention.lock().unwrap();
    let (send, receive) = std::sync::mpsc::channel();
    let peer = store.clone();
    let worker = std::thread::spawn(move || {
        let mut lease = peer.retain_read_sequence(2, 8);
        lease.retain_earlier(6);
        peer.replace_block_location(19, 2, 7, 1, None);
        let result = peer.registered_block_floor(19, 2);
        peer.remove_block_location(19, 2, 7, 1);
        drop(lease);
        let _ = send.send(result);
    });
    let result = receive.recv_timeout(std::time::Duration::from_secs(5));
    drop(held);
    worker.join().unwrap();
    assert_eq!(result.unwrap(), Some(7));
    assert_eq!(store.block_retention_floor(2), None);
}

#[test]
fn failed_collection_plan_releases_only_its_shard_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("wal");
    std::fs::write(&root, b"not a directory").unwrap();
    let store = LocalWriteAheadLogStore::new(&root);
    let peer = GcRunning::begin(store.group_commit_coordinator(2)).unwrap();
    assert!(store.gc_before_sequence_unchecked(1, 2).is_err());
    std::fs::remove_file(&root).unwrap();
    let retry = store.gc_before_sequence_unchecked(1, 2).unwrap();
    assert!(!retry.skipped_raced);
    assert!(store.gc_before_sequence_unchecked(2, 2).unwrap().skipped_raced);
    drop(peer);
    assert!(!store.gc_before_sequence_unchecked(2, 2).unwrap().skipped_raced);
}
