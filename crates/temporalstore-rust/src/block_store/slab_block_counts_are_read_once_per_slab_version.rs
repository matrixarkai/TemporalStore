// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A SLAB IS COUNTED AGAIN ONLY WHEN ITS FILE CHANGED.
//!
//! The reclaim planner asks `slab_block_counts` every round. It header-walks every byte of every
//! slab file, so a round cost the size of the store whether or not anything had been written, and
//! that cost grew with the corpus while the round itself had nothing to do.
//!
//! THE GUARD counts files read, on the store itself rather than in a process-wide total, so
//! another test's store cannot move it. A second call with nothing written reads zero files; a
//! write to the active slab reads exactly that one slab again, and the answer for it moves.
//!
//! THE CONTROLS are the first call, which must read every slab (a memo that remembered nothing
//! would also read zero files on the second call if it simply returned stale data, so the answer
//! is checked against what was appended), and the write, which must change the answer.
use super::*;

fn counts(store: &BlockStore) -> Vec<(u64, u64, u64)> {
    store.slab_block_counts().expect("slab_block_counts")
}

fn reads(store: &BlockStore) -> u64 {
    store
        .read_view
        .slab_count_file_reads
        .load(std::sync::atomic::Ordering::Relaxed)
}

#[test]
fn a_second_count_reads_no_slab_and_a_write_rereads_only_the_slab_it_touched() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.append(b"sealed-one").unwrap();
    store.append(b"sealed-two").unwrap();
    store.roll_slab().unwrap();
    store.append(b"active-one").unwrap();

    let first = counts(&store);
    assert_eq!(first.len(), 2, "two slabs on disk: {first:?}");
    assert_eq!(first[0].2, 2, "the sealed slab holds two blocks: {first:?}");
    assert_eq!(first[1].2, 1, "the active slab holds one block: {first:?}");
    assert_eq!(reads(&store), 2, "control: the first call reads every slab");

    let before = reads(&store);
    let second = counts(&store);
    assert_eq!(second, first, "an unchanged store answers the same");
    assert_eq!(
        reads(&store) - before,
        0,
        "nothing was written, so no slab file may be read again"
    );

    store.append(b"active-two").unwrap();
    let before = reads(&store);
    let third = counts(&store);
    assert_eq!(
        reads(&store) - before,
        1,
        "one append touched one slab, and only that slab is read again"
    );
    assert_eq!(third[0], first[0], "the sealed slab's answer is unchanged");
    assert_eq!(third[1].2, 2, "the active slab now holds two blocks: {third:?}");
    assert!(third[1].1 > first[1].1, "and its bytes grew: {first:?} -> {third:?}");
}

#[test]
fn a_slab_that_left_the_directory_is_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.append(b"one").unwrap();
    store.roll_slab().unwrap();
    store.append(b"two").unwrap();
    let both = counts(&store);
    assert_eq!(both.len(), 2);
    let gone = both[0].0;
    std::fs::remove_file(paths::slab_path(dir.path(), gone)).unwrap();
    let remaining = counts(&store);
    assert_eq!(remaining.len(), 1, "the removed slab is not reported: {remaining:?}");
    assert_ne!(remaining[0].0, gone);
    let memo = store.read_view.slab_count_memo.lock().unwrap();
    assert!(!memo.contains_key(&gone), "and not remembered either");
}
