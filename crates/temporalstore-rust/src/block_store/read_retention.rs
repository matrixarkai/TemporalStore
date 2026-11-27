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
        Self::insert_pin(&mut inner, &mut self.first, &mut self.remaining, slab);
    }

    pub(super) fn belongs_to(&self, store: &BlockStore) -> bool {
        Arc::ptr_eq(&self.store.inner, &store.inner)
    }

    /// Called under the lock that makes an imported slab visible to collection.
    pub(super) fn pin_locked(&mut self, inner: &mut BlockStoreInner, slab: u64) {
        if self.first == Some(slab) || self.remaining.contains(&slab) {
            return;
        }
        Self::insert_pin(inner, &mut self.first, &mut self.remaining, slab);
    }

    fn insert_pin(
        inner: &mut BlockStoreInner,
        first: &mut Option<u64>,
        remaining: &mut BTreeSet<u64>,
        slab: u64,
    ) {
        let readers = inner.read_pins.entry(slab).or_default();
        *readers = readers.checked_add(1).expect("slab reader count exhausted");
        if first.is_none() {
            *first = Some(slab);
        } else {
            remaining.insert(slab);
        }
    }
}

impl Drop for BlockStoreReadSnapshot {
    fn drop(&mut self) {
        let Some(first) = self.first else {
            return;
        };
        if let Ok(mut inner) = self.store.inner.lock() {
            for slab in std::iter::once(&first).chain(self.remaining.iter()) {
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

    #[derive(Debug)]
    struct RawSlabSource {
        id: u64,
        contents: &'static [u8],
    }

    impl SharedSlabSource for RawSlabSource {
        fn fetch_slab(&self, id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
            Ok((id == self.id).then(|| self.contents.to_vec()))
        }
    }

    #[test]
    fn dropping_an_empty_read_snapshot_does_not_wait_for_the_catalog() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let snapshot = BlockStoreReadSnapshot::new(&store);
        let held = store.inner.lock().unwrap();
        let (done, completed) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            drop(snapshot);
            done.send(()).unwrap();
        });
        let observed = completed.recv_timeout(std::time::Duration::from_secs(5));
        drop(held);
        reader.join().unwrap();
        observed.expect("empty snapshot teardown waited for the catalog");
    }

    #[test]
    fn replacing_the_default_source_releases_the_registry_before_dropping_it() {
        #[derive(Debug)]
        struct PausedSource {
            entered: std::sync::mpsc::Sender<()>,
            resume: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }

        impl SharedSlabSource for PausedSource {
            fn fetch_slab(&self, _: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
                Ok(None)
            }
        }

        impl Drop for PausedSource {
            fn drop(&mut self) {
                let _ = self.entered.send(());
                let _ = self.resume.get_mut().unwrap().recv();
            }
        }

        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let (entered, paused) = std::sync::mpsc::channel();
        let (release, resume) = std::sync::mpsc::channel();
        let id = store.current_block_slab_id() + 8;
        store.attach_shared_slab_source(Arc::new(PausedSource {
            entered, resume: std::sync::Mutex::new(resume),
        }));
        let writer = store.clone();
        let attach = std::thread::spawn(move || {
            writer.attach_shared_slab_source(Arc::new(RawSlabSource { id, contents: b"new source" }));
        });

        let timeout = std::time::Duration::from_secs(5);
        if let Err(error) = paused.recv_timeout(timeout) {
            let _ = release.send(());
            let _ = attach.join();
            panic!("previous source did not reach teardown: {error}");
        }
        let reader = store.clone();
        let (done, complete) = std::sync::mpsc::channel();
        let fetching = std::thread::spawn(move || done.send(reader.read_slab(id)).unwrap());
        let observed = complete.recv_timeout(timeout);
        release.send(()).unwrap();
        attach.join().unwrap();
        fetching.join().unwrap();
        assert_eq!(observed.unwrap().unwrap(), b"new source");
    }

    #[test]
    fn attaching_a_second_checkpoint_source_preserves_the_first() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let cursor = store.current_block_slab_id();
        let ids = store.reserve_checkpoint_slab_ids(2).unwrap();
        let first = *ids.start();
        let second = *ids.end();
        let source = |id, contents| Arc::new(RawSlabSource { id, contents });
        store
            .attach_checkpoint_slab_source(first, first, "first", source(first, b"first"))
            .unwrap();
        store
            .attach_checkpoint_slab_source(second, second, "second", source(second, b"second"))
            .unwrap();
        assert!(store
            .attach_checkpoint_slab_source(first, second, "overlap", source(first, b"wrong"))
            .is_err());
        assert!(store.slab_ids().unwrap().is_empty());
        assert_eq!(store.read_slab(first).unwrap(), b"first");
        assert_eq!(store.read_slab(second).unwrap(), b"second");
        assert_eq!(store.current_block_slab_id(), cursor);
    }

    #[test]
    fn a_remapped_raw_slab_does_not_become_the_legacy_writer_after_reopen() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let legacy = store.append(b"local").unwrap();
        let incoming = record::encode_slab_owner(19, 42);
        let imported = *store.reserve_checkpoint_slab_ids(1).unwrap().start();
        let mut staging = BlockStoreReadSnapshot::new(&store);
        store
            .stage_transferred_slab(imported, &incoming, None, &mut staging, true)
            .unwrap();
        assert_eq!(store.current_block_slab_id(), legacy.block_slab_id());
        store.sync_durable().unwrap();
        drop(staging);
        drop(store);
        let manifest = slab_manifest_path(root.path());
        if manifest.exists() {
            std::fs::remove_file(manifest).unwrap();
        }

        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.current_block_slab_id(), legacy.block_slab_id());
        assert_eq!(reopened.read_slab(imported).unwrap(), incoming);
        assert_eq!(reopened.exported_slab_owner(imported).unwrap(), None);
        let next = reopened.append(b"next").unwrap();
        assert_eq!(next.block_slab_id(), legacy.block_slab_id());
        assert_eq!(reopened.read_slab(imported).unwrap(), incoming);
    }

    #[test]
    fn staged_checkpoint_slab_is_retained_and_conflicts_leave_its_bytes_intact() {
        let source = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let source = BlockStore::new(source.path());
        let destination = BlockStore::new(destination.path());
        let first = source
            .append_block_of_object_for_shard(42, b"first", None, None, 0)
            .unwrap();
        source.sync_durable().unwrap();
        let id = first.block_slab_id();
        let (bytes, owner) = source.read_slab_for_transfer(id).unwrap();
        let mut staging = BlockStoreReadSnapshot::new(&destination);
        destination
            .stage_transferred_slab(id, &bytes, owner, &mut staging, false)
            .unwrap();
        assert!(destination
            .gc_slabs_before(u64::MAX)
            .unwrap()
            .removed_block_slab_ids
            .is_empty());
        destination
            .stage_transferred_slab(id, &bytes, owner, &mut staging, false)
            .unwrap();
        let mut changed = bytes.clone();
        *changed.last_mut().unwrap() ^= 1;
        assert!(destination
            .stage_transferred_slab(id, &changed, owner, &mut staging, false)
            .is_err());
        assert_eq!(destination.read(&first).unwrap(), b"first");
        drop(staging);
        assert_eq!(
            destination
                .gc_slabs_before(u64::MAX)
                .unwrap()
                .removed_block_slab_ids,
            [id]
        );
    }

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

    #[test]
    fn import_cannot_replace_an_open_slab_under_a_read_pin() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let id = 9;
        store.install_slab(id, b"legacy bytes").unwrap();
        let mut reader = BlockStoreReadSnapshot::new(&store);
        reader.pin(id);
        assert!(store.install_slab(id, b"replacement").is_err());
        assert_eq!(store.read_slab(id).unwrap(), b"legacy bytes");
        drop(reader);
        store.install_slab(id, b"replacement").unwrap();
        assert_eq!(store.read_slab(id).unwrap(), b"replacement");
    }
}
