// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use sha2::Digest;
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn rewrite_during_barrier_does_not_claim_an_unsynced_tail() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let payload: Vec<u8> = (0..4096u64)
        .flat_map(|value| sha2::Sha256::digest(value.to_le_bytes()))
        .collect();
    store
        .append_for_group_commit(
            1,
            Command::StringSet {
                key: "large".into(),
                value: payload,
            },
            vec![],
            vec![],
        )
        .unwrap();
    let second = store
        .append_for_group_commit(
            1,
            Command::StringSet {
                key: "retained".into(),
                value: vec![2],
            },
            vec![],
            vec![],
        )
        .unwrap();
    let expected = Arc::new(AtomicU64::new(0));
    let third_sequence = Arc::new(AtomicU64::new(0));
    let observer_store = store.clone();
    let observer_expected = expected.clone();
    let observer_sequence = third_sequence.clone();
    AFTER_GROUP_COMMIT_SYNC.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move || {
            let reclaimed = observer_store
                .gc_before_sequence_unchecked(1, second.sequence)
                .unwrap();
            assert!(reclaimed.bytes_after < reclaimed.bytes_before);
            observer_expected.store(reclaimed.bytes_after, Ordering::Relaxed);
            let third = observer_store
                .append_for_group_commit(
                    1,
                    Command::StringSet {
                        key: "not-yet-committed".into(),
                        value: vec![3; 1024],
                    },
                    vec![],
                    vec![],
                )
                .unwrap();
            observer_sequence.store(third.sequence, Ordering::Relaxed);
        }))
    });
    store.commit_barrier(1, second.sequence).unwrap();
    let info = store.info(1).unwrap();
    assert_eq!(
        info.persistent_length_bytes,
        expected.load(Ordering::Relaxed),
        "a stale barrier must not mark the replacement log's unsynced tail durable"
    );
    assert_eq!(info.last_flushed_sequence, second.sequence);
    store
        .commit_barrier(1, third_sequence.load(Ordering::Relaxed))
        .unwrap();
    let committed = store.info(1).unwrap();
    assert!(committed.persistent_length_bytes > expected.load(Ordering::Relaxed));
    assert_eq!(
        committed.last_flushed_sequence,
        third_sequence.load(Ordering::Relaxed)
    );
}
