// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

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
fn a_restore_only_round_cannot_age_into_deletion() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.install_slab(0, b"unproven").unwrap();
    store.install_slab(1, b"current").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(1, [1]).unwrap();
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    store.inner.lock().unwrap().delayed_destroy_earliest_unix_ms = Some(now_unix_ms());
    assert!(store.quarantine_is_too_young_to_purge(DELAYED_DESTROY_MIN_AGE_MS));

    // The caller already chose the approximate set when its earlier age check was true.
    mature_quarantine(&store);
    assert!(!store.quarantine_is_too_young_to_purge(DELAYED_DESTROY_MIN_AGE_MS));
    let held = store.restore_delayed_destroy_slabs_selected(
        DELAYED_DESTROY_MIN_AGE_MS, [], None,
    ).unwrap();
    assert!(held.purged_block_slab_ids.is_empty());
    assert_eq!(held.processed_block_slabs, 0);
    assert!(held.declined_without_examining);
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);

    let reclaimed = store.purge_delayed_destroy_slabs_selected(
        DELAYED_DESTROY_MIN_AGE_MS, [1], None,
    ).unwrap();
    assert_eq!(reclaimed.purged_block_slab_ids, vec![0]);
    assert_eq!(store.read_slab(1).unwrap(), b"current");
}

#[test]
fn a_restore_only_round_restores_live_slabs_and_keeps_unnamed_mature_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.install_slab(0, b"needed").unwrap();
    store.install_slab(1, b"unknown-to-tally").unwrap();
    store.install_slab(2, b"current").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(2, [2]).unwrap();
    mature_quarantine(&store);
    let restored = store.restore_delayed_destroy_slabs_selected(0, [0], None).unwrap();
    assert_eq!(restored.restored_block_slab_ids, vec![0]);
    assert!(restored.purged_block_slab_ids.is_empty());
    assert_eq!(store.read_slab(0).unwrap(), b"needed");
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![1]);
    let reclaimed = store.purge_delayed_destroy_slabs_selected(0, [0, 2], None).unwrap();
    assert_eq!(reclaimed.purged_block_slab_ids, vec![1]);
    assert_eq!(store.read_slab(0).unwrap(), b"needed");
}

#[test]
fn restore_only_selection_does_not_change_other_quarantined_slabs() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    store.install_slab(0, b"needed").unwrap();
    store.install_slab(1, b"unknown").unwrap();
    store.install_slab(2, b"current").unwrap();
    store.gc_slabs_before_with_live_refs_delayed_destroy(2, [2]).unwrap();
    mature_quarantine(&store);
    let untouched = store.restore_delayed_destroy_slabs_selected(
        0, [0], Some([1].into_iter().collect()),
    ).unwrap();
    assert!(untouched.restored_block_slab_ids.is_empty());
    assert!(untouched.purged_block_slab_ids.is_empty());
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0, 1]);
    let restored = store.restore_delayed_destroy_slabs_selected(
        0, [0], Some([0].into_iter().collect()),
    ).unwrap();
    assert_eq!(restored.restored_block_slab_ids, vec![0]);
    assert!(restored.purged_block_slab_ids.is_empty());
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![1]);
}
