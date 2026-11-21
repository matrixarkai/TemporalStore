// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The append lock is taken once per store and shard, not once per append.
//!
//! A log directory has one writer process. These tests pin what that buys -- no lock call and no
//! `statx` per acked append after the first -- and what it costs: a second store on the same
//! directory is refused, in this process or another.

use super::*;

fn put(store: &LocalWriteAheadLogStore, index: usize) -> Result<(), WriteAheadLogError> {
    store
        .append(
            5,
            Command::StringSet {
                key: format!("key-{index}"),
                value: vec![b'v'; 48],
            },
        )
        .map(|_| ())
}

fn locks() -> u64 {
    WAL_LOCK_ACQUISITIONS.with(|count| count.get())
}

fn stats() -> u64 {
    WAL_PATH_LEN_ASKS.with(|asks| asks.get())
}

#[test]
fn a_second_store_on_the_same_directory_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let first = LocalWriteAheadLogStore::new(dir.path());
    put(&first, 0).unwrap();
    let second = LocalWriteAheadLogStore::new(dir.path());
    let refused = put(&second, 1).unwrap_err();
    assert!(
        refused.to_string().contains(WAL_DIRECTORY_OWNED_BY_ANOTHER_PROCESS),
        "{refused}"
    );
    // The refused store wrote nothing, and the owner is unharmed.
    put(&first, 2).unwrap();
    assert_eq!(first.stats(5).last_sequence, 2);
    // Reading is not writing: a second store may still scan the log.
    assert_eq!(second.scan(5, 0, u64::MAX, u64::MAX).unwrap().len(), 2);
}

#[test]
fn a_closed_store_hands_the_directory_to_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    {
        let first = LocalWriteAheadLogStore::new(dir.path());
        put(&first, 0).unwrap();
    }
    let second = LocalWriteAheadLogStore::new(dir.path());
    put(&second, 1).unwrap();
    assert_eq!(second.stats(5).last_sequence, 2);
}

#[test]
fn an_acked_append_after_the_first_takes_no_lock_and_asks_no_length() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    put(&store, 0).unwrap();
    // Flat from 1 to 2,000 appends: the same zero at both ends.
    let (locks_before, stats_before) = (locks(), stats());
    for index in 1..=2_000 {
        put(&store, index).unwrap();
    }
    assert_eq!(locks() - locks_before, 0, "no lock acquisition per append");
    assert!(stats() - stats_before <= 4, "no statx per append, only the few roll checks");
    assert_eq!(store.stats(5).last_sequence, 2_001);
}

#[test]
fn the_lock_is_taken_once_per_shard() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let before = locks();
    for shard in [5u64, 6, 7] {
        for index in 0..20 {
            store
                .append(
                    shard,
                    Command::StringSet { key: format!("k{index}"), value: b"v".to_vec() },
                )
                .unwrap();
        }
    }
    assert_eq!(locks() - before, 3);
}
