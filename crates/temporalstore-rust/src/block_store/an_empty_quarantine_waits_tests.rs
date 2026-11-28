// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A quarantine that a purge round found EMPTY needs no live set until something is set aside
//! again. `quarantine_waits_for_age` is the gate the page stage reads before it walks the shard to
//! build a live set; an empty quarantine used to answer "must look" on every idle round.
//!
//! Planted faults (each must turn a test red): never set `quarantine_known_empty`; do not clear it
//! at the arrival site; set it after a round that left a slab behind.

use super::*;

fn mature_quarantine(store: &BlockStore) {
    let old = now_unix_ms().saturating_sub(DELAYED_DESTROY_MIN_AGE_MS + 1_000);
    let mut inner = store.inner.lock().unwrap();
    for mut slab in inner.slabs.values_mut() {
        if slab.state == BlockStoreSlabState::DelayedDestroy {
            slab.updated_unix_ms = Some(old);
        }
    }
    inner.delayed_destroy_earliest_unix_ms = Some(old);
}

#[test]
fn an_empty_quarantine_waits_and_a_new_arrival_makes_it_look_again() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.install_slab(0, b"dead").unwrap();
    store.install_slab(1, b"current").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(1, [1]).unwrap();
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    mature_quarantine(&store);

    // The round destroys its only entry and reaches the end of the directory.
    let report = store
        .purge_delayed_destroy_slabs_selected(DELAYED_DESTROY_MIN_AGE_MS, [1], None)
        .unwrap();
    assert_eq!(report.purged_block_slab_ids, vec![0]);
    assert!(!store.has_delayed_destroy_slabs().unwrap());
    assert!(
        store.quarantine_waits_for_age(DELAYED_DESTROY_MIN_AGE_MS),
        "an emptied quarantine must not send the next idle round to walk the shard"
    );

    // Something is set aside again: the gate must open, whatever the earlier empty read said.
    store.install_slab(2, b"dead too").unwrap();
    store.install_slab(3, b"current too").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(3, [3]).unwrap();
    assert!(store.has_delayed_destroy_slabs().unwrap());
    assert!(
        !store.quarantine_waits_for_age(DELAYED_DESTROY_MIN_AGE_MS),
        "a slab arrived since the empty read, so a round must look"
    );
}

#[test]
fn a_round_that_left_a_slab_behind_does_not_claim_the_quarantine_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.install_slab(0, b"needed").unwrap();
    store.install_slab(1, b"unnamed").unwrap();
    store.install_slab(2, b"current").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(2, [2]).unwrap();
    mature_quarantine(&store);
    // Restore-only round naming slab 0: slab 1 stays in quarantine.
    let report = store.restore_delayed_destroy_slabs_selected(0, [0], None).unwrap();
    assert_eq!(report.restored_block_slab_ids, vec![0]);
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![1]);
    assert!(
        !store.quarantine_waits_for_age(DELAYED_DESTROY_MIN_AGE_MS),
        "slab 1 is still in quarantine and unexamined; the gate must not read the directory as empty"
    );
}

#[test]
fn a_fresh_process_does_not_know_its_quarantine_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    assert!(
        !store.quarantine_waits_for_age(DELAYED_DESTROY_MIN_AGE_MS),
        "nothing has read the directory in this process, so the first round must look"
    );
}
