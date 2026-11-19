// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

/// Retains distinct slabs until their captured addresses have finished loading.
pub(crate) struct BlockStoreReadSnapshot {
    store: BlockStore,
    first: Option<u64>,
    remaining: BTreeSet<u64>,
}

impl BlockStoreReadSnapshot {
    pub(crate) fn new(store: &BlockStore) -> Self {
        Self {
            store: store.clone(),
            first: None,
            remaining: BTreeSet::new(),
        }
    }

    /// The caller still holds the index scope protecting this address from retirement.
    pub(crate) fn pin(&mut self, slab: u64) {
        if self.first == Some(slab) || self.remaining.contains(&slab) {
            return;
        }
        let mut inner = self.store.inner.lock().expect("block store lock poisoned");
        let readers = inner.read_pins.entry(slab).or_default();
        *readers = readers.checked_add(1).expect("slab reader count exhausted");
        if self.first.is_none() {
            self.first = Some(slab);
        } else {
            self.remaining.insert(slab);
        }
    }
}

impl Drop for BlockStoreReadSnapshot {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.store.inner.lock() {
            for slab in self.first.iter().chain(self.remaining.iter()) {
                if let Some(readers) = inner.read_pins.get_mut(slab) {
                    *readers -= 1;
                    if *readers == 0 {
                        inner.read_pins.remove(slab);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_readers_protect_only_named_slabs_until_last_release() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        store.roll_slab().unwrap();
        let mut outer = BlockStoreReadSnapshot::new(&store);
        outer.pin(first.block_slab_id());
        outer.pin(first.block_slab_id());
        let mut nested = BlockStoreReadSnapshot::new(&store);
        nested.pin(first.block_slab_id());
        assert_eq!(
            store.inner.lock().unwrap().read_pins[&first.block_slab_id()],
            2
        );
        assert!(store
            .gc_utility_candidates(u64::MAX, [])
            .unwrap()
            .iter()
            .all(|candidate| candidate.block_slab_id != first.block_slab_id()));
        let collected = store.gc_slabs_before(u64::MAX).unwrap();
        assert_eq!(collected.removed_block_slab_ids, [second.block_slab_id()]);
        assert_eq!(store.read(&first).unwrap(), b"first");
        drop(outer);
        assert!(store
            .gc_slabs_before(u64::MAX)
            .unwrap()
            .removed_block_slab_ids
            .is_empty());
        drop(nested);
        assert!(store.inner.lock().unwrap().read_pins.is_empty());
        assert_eq!(
            store
                .gc_slabs_before(u64::MAX)
                .unwrap()
                .removed_block_slab_ids,
            [first.block_slab_id()]
        );
    }

    #[test]
    fn read_pins_survive_quarantine_plans_and_restore_before_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"held").unwrap();
        store.roll_slab().unwrap();
        let mut snapshot = BlockStoreReadSnapshot::new(&store);
        snapshot.pin(first.block_slab_id());
        let held = store
            .gc_slabs_before_with_live_refs_delayed_destroy(u64::MAX, [])
            .unwrap();
        assert!(held.delayed_destroy_block_slab_ids.is_empty());
        drop(snapshot);
        let moved = store
            .gc_slabs_before_with_live_refs_delayed_destroy(u64::MAX, [])
            .unwrap();
        assert_eq!(
            moved.delayed_destroy_block_slab_ids,
            [first.block_slab_id()]
        );
        let mut restored = BlockStoreReadSnapshot::new(&store);
        restored.pin(first.block_slab_id());
        let purge = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
        assert_eq!(purge.restored_block_slab_ids, [first.block_slab_id()]);
        assert!(purge.purged_block_slab_ids.is_empty());
        assert_eq!(store.read(&first).unwrap(), b"held");
        drop(restored);
        assert_eq!(
            store
                .gc_slabs_before(u64::MAX)
                .unwrap()
                .removed_block_slab_ids,
            [first.block_slab_id()]
        );
    }
}
