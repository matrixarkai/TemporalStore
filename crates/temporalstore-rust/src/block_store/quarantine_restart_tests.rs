// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::fs;

impl BlockStore {
    pub(crate) fn age_quarantine_for_reclaim_test(&self, arrived_at: u64) {
        let mut inner = self.inner.lock().unwrap();
        for slab in inner.slabs.values_mut() {
            if slab.state == BlockStoreSlabState::DelayedDestroy {
                slab.updated_unix_ms = Some(arrived_at);
            }
        }
        inner.delayed_destroy_earliest_unix_ms = Some(arrived_at);
        inner.persist_slab_manifest_counted().unwrap();
    }
}

fn old_slab(store: &BlockStore, root: &Path) {
    store.install_slab(0, b"old payload").unwrap();
    store.install_slab(1, b"active").unwrap();
    File::open(slab_path(root, 0))
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1))
        .unwrap();
}

#[test]
fn restart_preserves_the_quarantine_arrival_not_the_last_append_time() {
    let dir = tempfile::tempdir().unwrap();
    let arrived_at;
    {
        let store = BlockStore::new(dir.path());
        old_slab(&store, dir.path());
        store
            .gc_slabs_before_with_live_refs_delayed_destroy(1, [1])
            .unwrap();
        arrived_at = store
            .slab_descriptors()
            .into_iter()
            .find(|slab| slab.block_slab_id == 0)
            .unwrap()
            .updated_unix_ms;
    }
    let store = BlockStore::new(dir.path());
    let slab = store
        .slab_descriptors()
        .into_iter()
        .find(|slab| slab.block_slab_id == 0)
        .unwrap();
    assert_eq!(slab.updated_unix_ms, arrived_at);
    let held = store
        .purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert!(held.purged_block_slab_ids.is_empty());
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    assert_eq!(
        store
            .purge_delayed_destroy_slabs_older_than(0)
            .unwrap()
            .purged_block_slab_ids,
        vec![0]
    );
}

#[test]
fn restart_gives_an_interrupted_quarantine_a_full_grace_window() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = BlockStore::new(dir.path());
        old_slab(&store, dir.path());
        // Simulate a crash after rename but before the slab manifest records the transition.
        fs::create_dir_all(delayed_destroy_dir(dir.path())).unwrap();
        fs::rename(
            slab_path(dir.path(), 0),
            delayed_destroy_dir(dir.path())
                .join("block_segment_00000000000000000000.seg.deleted.1"),
        )
        .unwrap();
    }
    let before_open = now_unix_ms();
    let store = BlockStore::new(dir.path());
    let slab = store
        .slab_descriptors()
        .into_iter()
        .find(|slab| slab.block_slab_id == 0)
        .unwrap();
    assert!(slab.updated_unix_ms.unwrap() >= before_open);
    let held = store
        .purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
        .unwrap();
    assert!(held.purged_block_slab_ids.is_empty());
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
}
