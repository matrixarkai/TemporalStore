// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};

/// Derived membership only: the values are the canonical descriptor records.
/// Never acquire a descriptor, writer, or catalog guard while holding this lock.
#[derive(Debug, Default)]
struct ActiveLeaseIndex {
    entries: Mutex<BTreeMap<u64, Arc<SlabRecord>>>,
    #[cfg(test)]
    visits: AtomicUsize,
    #[cfg(test)]
    lock_attempts: AtomicUsize,
}

impl ActiveLeaseIndex {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, Arc<SlabRecord>>> {
        #[cfg(test)]
        self.lock_attempts.fetch_add(1, Ordering::Relaxed);
        self.entries.lock().expect("active lease index poisoned")
    }

    fn remove_if_current(entries: &mut BTreeMap<u64, Arc<SlabRecord>>, record: &Arc<SlabRecord>) {
        if entries
            .get(&record.index_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, record))
        {
            entries.remove(&record.index_id);
        }
    }
}

#[derive(Debug)]
pub(super) struct SlabRecord {
    descriptor: RwLock<BlockStoreSlabDescriptor>,
    writers: AtomicUsize,
    index_id: u64,
    index: Weak<ActiveLeaseIndex>,
    // One-way namespace-incarnation fence, not a second active-state authority.
    retired: AtomicBool,
    #[cfg(test)]
    drop_probe: Mutex<Option<RecordDropProbe>>,
}

#[cfg(test)]
#[derive(Debug)]
struct RecordDropProbe {
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
    drops: Arc<AtomicUsize>,
}

#[cfg(test)]
impl Drop for SlabRecord {
    fn drop(&mut self) {
        if let Some(probe) = self.drop_probe.get_mut().unwrap().take() {
            probe.drops.fetch_add(1, Ordering::Relaxed);
            let _ = probe.entered.send(());
            let _ = probe.release.recv();
        }
    }
}

impl SlabRecord {
    fn new(
        id: u64,
        descriptor: BlockStoreSlabDescriptor,
        index: &Arc<ActiveLeaseIndex>,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor: RwLock::new(descriptor),
            writers: AtomicUsize::new(0),
            index_id: id,
            index: Arc::downgrade(index),
            retired: AtomicBool::new(false),
            #[cfg(test)]
            drop_probe: Mutex::new(None),
        })
    }

    fn read(&self) -> RwLockReadGuard<'_, BlockStoreSlabDescriptor> {
        self.descriptor.read().expect("slab record poisoned")
    }

    fn write(&self) -> RwLockWriteGuard<'_, BlockStoreSlabDescriptor> {
        self.descriptor.write().expect("slab record poisoned")
    }

    fn protected(&self) -> bool {
        self.writers.load(Ordering::Acquire) != 0
    }

    fn lease(record: &Arc<Self>) -> Option<SlabWriterLease> {
        let index = record.index.upgrade()?;
        let mut entries = index.lock();
        if record.retired.load(Ordering::Acquire) {
            return None;
        }
        if let Some(entry) = entries.get(&record.index_id) {
            assert!(
                Arc::ptr_eq(entry, record),
                "different active physical incarnation"
            );
        }
        record
            .writers
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                count.checked_add(1)
            })
            .expect("physical writer lease count exhausted");
        entries
            .entry(record.index_id)
            .or_insert_with(|| record.clone());
        Some(SlabWriterLease {
            record: record.clone(),
            index: index.clone(),
        })
    }
}

/// The Arc allocation is this physical incarnation's identity. A stale lease can
/// only release its own record, never a replacement installed under the same ID.
#[derive(Debug)]
pub(super) struct SlabWriterLease {
    record: Arc<SlabRecord>,
    index: Arc<ActiveLeaseIndex>,
}

impl SlabWriterLease {
    pub(super) fn snapshot(&self) -> BlockStoreSlabDescriptor {
        self.record.read().clone()
    }

    pub(super) fn fork(&self) -> Self {
        SlabRecord::lease(&self.record).expect("live lease cannot belong to a retired record")
    }

    pub(super) fn seal(&self) {
        let mut slab = self.record.write();
        slab.state = BlockStoreSlabState::Sealed;
        slab.updated_unix_ms = Some(now_unix_ms());
    }

    pub(super) fn publish_header(&self, offset: u64) {
        let mut slab = self.record.write();
        slab.physical_bytes = offset;
        slab.readable_prefix_physical_bytes = offset;
    }

    pub(super) fn publish_append(&self, offset: u64, logical: u64, block: u64) {
        let mut slab = self.record.write();
        slab.state = BlockStoreSlabState::Active;
        slab.physical_bytes = offset;
        slab.readable_prefix_physical_bytes = offset;
        slab.logical_bytes = slab.logical_bytes.saturating_add(logical);
        slab.has_corruption = false;
        slab.first_error_offset = None;
        slab.first_error = None;
        let now = now_unix_ms();
        slab.created_unix_ms.get_or_insert(now);
        slab.updated_unix_ms = Some(now);
        slab.first_block_id = Some(slab.first_block_id.map_or(block, |first| first.min(block)));
        slab.last_block_id = Some(slab.last_block_id.map_or(block, |last| last.max(block)));
    }

    #[cfg(test)]
    pub(super) fn standalone(descriptor: BlockStoreSlabDescriptor) -> Self {
        let index = Arc::new(ActiveLeaseIndex::default());
        SlabRecord::lease(&SlabRecord::new(0, descriptor, &index)).unwrap()
    }
}

impl Drop for SlabWriterLease {
    fn drop(&mut self) {
        let mut entries = self.index.lock();
        let before = self.record.writers.fetch_sub(1, Ordering::AcqRel);
        assert!(before > 0, "unbalanced physical writer lease");
        if before == 1 {
            ActiveLeaseIndex::remove_if_current(&mut entries, &self.record);
        }
    }
}

/// One topology map; record guards only protect memory and never span I/O.
#[derive(Debug, Default)]
pub(super) struct SlabCatalog {
    records: BTreeMap<u64, Arc<SlabRecord>>,
    active: Arc<ActiveLeaseIndex>,
    #[cfg(test)]
    topology_visits: AtomicUsize,
}

impl SlabCatalog {
    pub(super) fn new() -> Self {
        Self::default()
    }
    pub(super) fn len(&self) -> usize {
        self.records.len()
    }
    pub(super) fn contains_key(&self, id: &u64) -> bool {
        self.records.contains_key(id)
    }
    pub(super) fn keys(&self) -> impl DoubleEndedIterator<Item = &u64> {
        self.records.keys().inspect(|_| self.note_topology_visit())
    }
    pub(super) fn get(&self, id: &u64) -> Option<RwLockReadGuard<'_, BlockStoreSlabDescriptor>> {
        self.records.get(id).map(|record| record.read())
    }
    pub(super) fn get_mut(
        &mut self,
        id: &u64,
    ) -> Option<RwLockWriteGuard<'_, BlockStoreSlabDescriptor>> {
        self.records.get(id).map(|record| record.write())
    }
    pub(super) fn values(
        &self,
    ) -> impl DoubleEndedIterator<Item = RwLockReadGuard<'_, BlockStoreSlabDescriptor>> {
        self.records.values().map(|record| {
            self.note_topology_visit();
            record.read()
        })
    }
    pub(super) fn values_mut(
        &mut self,
    ) -> impl DoubleEndedIterator<Item = RwLockWriteGuard<'_, BlockStoreSlabDescriptor>> {
        self.records.values().map(|record| {
            self.note_topology_visit();
            record.write()
        })
    }
    pub(super) fn iter(
        &self,
    ) -> impl DoubleEndedIterator<Item = (&u64, RwLockReadGuard<'_, BlockStoreSlabDescriptor>)>
    {
        self.records.iter().map(|(id, record)| {
            self.note_topology_visit();
            (id, record.read())
        })
    }
    pub(super) fn iter_mut(
        &mut self,
    ) -> impl Iterator<Item = (&u64, RwLockWriteGuard<'_, BlockStoreSlabDescriptor>)> {
        self.records.iter().map(|(id, record)| {
            self.note_topology_visit();
            (id, record.write())
        })
    }
    pub(super) fn range_mut(
        &mut self,
        range: impl RangeBounds<u64>,
    ) -> impl Iterator<Item = (&u64, RwLockWriteGuard<'_, BlockStoreSlabDescriptor>)> {
        self.records.range(range).map(|(id, record)| {
            self.note_topology_visit();
            (id, record.write())
        })
    }
    pub(super) fn last_key_value(&self) -> Option<(&u64, &Arc<SlabRecord>)> {
        self.records.last_key_value()
    }
    // The caller must own this buffer outside any catalog guard, including on
    // error exits. It holds detached ownership only, never serving authority.
    pub(super) fn insert(
        &mut self,
        id: u64,
        descriptor: BlockStoreSlabDescriptor,
        retired: &mut Vec<Arc<SlabRecord>>,
    ) {
        let replacement = SlabRecord::new(id, descriptor, &self.active);
        let mut entries = self.active.lock();
        if let Some(old) = self.records.get(&id) {
            assert!(!old.protected(), "cannot replace a leased physical slab");
            old.retired.store(true, Ordering::Release);
            ActiveLeaseIndex::remove_if_current(&mut entries, old);
        }
        let detached = self.records.insert(id, replacement);
        drop(entries);
        retired.extend(detached);
    }
    pub(super) fn ensure_with(
        &mut self,
        id: u64,
        create: impl FnOnce() -> BlockStoreSlabDescriptor,
    ) -> RwLockWriteGuard<'_, BlockStoreSlabDescriptor> {
        let active = &self.active;
        self.records
            .entry(id)
            .or_insert_with(|| SlabRecord::new(id, create(), active))
            .write()
    }
    pub(super) fn protected(&self, id: u64) -> bool {
        self.records
            .get(&id)
            .is_some_and(|record| record.protected())
    }
    pub(super) fn protected_ids(&self) -> BTreeSet<u64> {
        self.active
            .lock()
            .keys()
            .map(|id| {
                #[cfg(test)]
                self.active.visits.fetch_add(1, Ordering::Relaxed);
                *id
            })
            .collect()
    }
    pub(super) fn writer_lease(&self, id: u64) -> Option<SlabWriterLease> {
        self.records.get(&id).and_then(SlabRecord::lease)
    }
    pub(super) fn snapshot(&self) -> BTreeMap<u64, BlockStoreSlabDescriptor> {
        self.iter().map(|(id, slab)| (*id, slab.clone())).collect()
    }

    fn note_topology_visit(&self) {
        #[cfg(test)]
        self.topology_visits.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(super) fn active_index_lock_attempts(&self) -> usize {
        self.active.lock_attempts.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(super) fn enumeration_counters(&self) -> (usize, usize) {
        (
            self.active.visits.load(Ordering::Relaxed),
            self.topology_visits.load(Ordering::Relaxed),
        )
    }
}

impl FromIterator<(u64, BlockStoreSlabDescriptor)> for SlabCatalog {
    fn from_iter<T: IntoIterator<Item = (u64, BlockStoreSlabDescriptor)>>(iter: T) -> Self {
        let mut catalog = Self::new();
        let mut retired = Vec::new();
        for (id, slab) in iter {
            catalog.insert(id, slab, &mut retired);
            retired.clear(); // Construction has no shared catalog guard.
        }
        catalog
    }
}

pub(super) trait SlabView {
    fn visit_descriptors(&self, visitor: &mut dyn FnMut(&BlockStoreSlabDescriptor));
    fn descriptor_count(&self) -> usize;
}
impl SlabView for SlabCatalog {
    fn visit_descriptors(&self, visitor: &mut dyn FnMut(&BlockStoreSlabDescriptor)) {
        for slab in self.values() {
            visitor(&slab);
        }
    }
    fn descriptor_count(&self) -> usize {
        self.len()
    }
}
impl SlabView for BTreeMap<u64, BlockStoreSlabDescriptor> {
    fn visit_descriptors(&self, visitor: &mut dyn FnMut(&BlockStoreSlabDescriptor)) {
        for slab in self.values() {
            visitor(slab);
        }
    }
    fn descriptor_count(&self) -> usize {
        self.len()
    }
}

/// Coherent live accounting, never held over file/catalog/record operations.
#[derive(Debug, Default)]
pub(super) struct AppendAccounting {
    totals: Mutex<BlockStoreStats>,
    #[cfg(test)]
    attempt_observer: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

impl AppendAccounting {
    fn lock_totals(&self) -> std::sync::MutexGuard<'_, BlockStoreStats> {
        #[cfg(test)]
        if let Some(observer) = self.attempt_observer.lock().unwrap().as_ref() {
            let _ = observer.send(());
        }
        self.totals.lock().expect("append accounting poisoned")
    }

    #[cfg(test)]
    pub(super) fn park_for_test(
        &self,
        observer: std::sync::mpsc::Sender<()>,
    ) -> std::sync::MutexGuard<'_, BlockStoreStats> {
        let held = self.totals.lock().unwrap();
        *self.attempt_observer.lock().unwrap() = Some(observer);
        held
    }

    pub(super) fn add(&self, writes: u64, bytes: u64, logical: u64, compressed: u64, saved: u64) {
        let mut totals = self.lock_totals();
        totals.writes += writes;
        totals.bytes_written += bytes;
        totals.logical_bytes_written += logical;
        totals.compressed_records_written += compressed;
        totals.compression_bytes_saved += saved;
    }
    pub(super) fn add_batch(
        &self,
        writes: u64,
        bytes: u64,
        logical: u64,
        compressed: u64,
        saved: u64,
    ) {
        let mut totals = self.lock_totals();
        totals.writes = totals.writes.saturating_add(writes);
        totals.bytes_written = totals.bytes_written.saturating_add(bytes);
        totals.logical_bytes_written = totals.logical_bytes_written.saturating_add(logical);
        totals.compressed_records_written =
            totals.compressed_records_written.saturating_add(compressed);
        totals.compression_bytes_saved = totals.compression_bytes_saved.saturating_add(saved);
    }
    pub(super) fn merge_into(&self, stats: &mut BlockStoreStats) {
        let totals = self.lock_totals();
        stats.writes = totals.writes;
        stats.bytes_written = totals.bytes_written;
        stats.logical_bytes_written = totals.logical_bytes_written;
        stats.compressed_records_written = totals.compressed_records_written;
        stats.compression_bytes_saved = totals.compression_bytes_saved;
        stats.bytes_written_at_last_compaction = totals.bytes_written_at_last_compaction;
    }
    pub(super) fn mark_window(&self, baseline: u64) {
        self.lock_totals().bytes_written_at_last_compaction = baseline;
    }
}

#[cfg(all(test, feature = "alloc-probe"))]
impl Clone for SlabCatalog {
    // Only the heap probe deep-clones this diagnostic structure. Count the real
    // map nodes and Arc/record allocations, not an unrepresentative DTO map.
    fn clone(&self) -> Self {
        // Copy descriptors before freezing membership: even this diagnostic must
        // not acquire a descriptor guard while holding the lifecycle index.
        let mut copy = Self::new();
        for (id, record) in &self.records {
            let cloned = SlabRecord::new(*id, record.read().clone(), &copy.active);
            copy.records.insert(*id, cloned);
        }
        let _held = self.active.lock();
        for (id, record) in &self.records {
            let cloned = &copy.records[id];
            let count = record.writers.load(Ordering::Acquire);
            cloned.writers.store(count, Ordering::Release);
            if count != 0 {
                copy.active
                    .entries
                    .lock()
                    .unwrap()
                    .insert(*id, cloned.clone());
            }
        }
        copy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_record_drop_after_real_import_releases_catalog_and_index_on_success_and_error() {
        use std::sync::mpsc;
        use std::time::Duration;
        for fail_persist in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let store = BlockStore::new(root.path());
            let address = store.append(b"same-ID exact legacy payload").unwrap();
            let peer = store
                .append_block_of_object_for_shard(7, b"peer writer anchor", None, None, 0)
                .unwrap();
            let bytes = std::fs::read(slab_path(root.path(), address.block_slab_id())).unwrap();
            let target_id = address.block_slab_id();
            let peer_id = peer.block_slab_id();
            let drops = Arc::new(AtomicUsize::new(0));
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let old_weak = {
                let mut inner = store.inner.lock().unwrap();
                let record = &inner.slabs.records[&address.block_slab_id()];
                record.write().first_error = Some("old descriptor allocation".repeat(65_536));
                *record.drop_probe.lock().unwrap() = Some(RecordDropProbe {
                    entered: entered_tx,
                    release: release_rx,
                    drops: drops.clone(),
                });
                let weak = Arc::downgrade(record);
                if fail_persist {
                    inner.slabs_unwritten = SLABS_UNWRITTEN_BEFORE_PERSIST - 1;
                }
                weak
            };
            if fail_persist {
                std::fs::remove_file(slab_manifest_path(root.path())).unwrap();
                std::fs::create_dir(slab_manifest_path(root.path())).unwrap();
            }
            let outcome = std::thread::scope(|scope| {
                let source = &store;
                let (done_tx, done_rx) = mpsc::channel();
                let import = scope.spawn(move || {
                    done_tx
                        .send(source.install_slab(target_id, &bytes))
                        .unwrap();
                });
                let entered = entered_rx.recv_timeout(Duration::from_secs(5));
                let observed_drops = drops.load(Ordering::Relaxed);
                let no_retained_old_arc = old_weak.upgrade().is_none();
                let (probe_tx, probe_rx) = mpsc::channel();
                let probe = scope.spawn(move || {
                    let inner = source.inner.lock().unwrap();
                    let lease = inner.slabs.writer_lease(peer_id).unwrap();
                    let ids = inner.slabs.protected_ids();
                    drop(inner);
                    let fork = lease.fork();
                    drop(lease);
                    drop(fork);
                    probe_tx.send(ids).unwrap();
                });
                let accessible = probe_rx.recv_timeout(Duration::from_secs(5));
                let waiting = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
                let _ = release_tx.send(());
                import.join().unwrap();
                probe.join().unwrap();
                (
                    entered,
                    observed_drops,
                    no_retained_old_arc,
                    accessible,
                    waiting,
                    done_rx.recv().unwrap(),
                )
            });
            outcome.0.unwrap();
            assert_eq!(
                outcome.1, 1,
                "actual final destructor, not expected budget or detached Arc drop"
            );
            assert!(outcome.2, "no retained old Arc may mask final destruction");
            assert!(outcome.3.unwrap().contains(&peer.block_slab_id()));
            assert!(
                outcome.4,
                "import has reached final destruction and awaits explicit release"
            );
            assert_eq!(outcome.5.is_err(), fail_persist);
            assert_eq!(drops.load(Ordering::Relaxed), 1);
            assert_eq!(
                store.read(&address).unwrap(),
                b"same-ID exact legacy payload"
            );
            assert_eq!(store.read(&peer).unwrap(), b"peer writer anchor");
        }
    }

    #[test]
    fn legacy_file_selection_releases_descriptor_before_real_metadata_probe() {
        let root = tempfile::tempdir().unwrap();
        let mut catalog = SlabCatalog::new();
        for id in 0..4 {
            set_slab_state(&mut catalog, id, BlockStoreSlabState::Sealed);
        }
        catalog.get_mut(&3).unwrap().owner_shard_id = Some(7);
        catalog.get_mut(&0).unwrap().state = BlockStoreSlabState::Active;
        std::fs::write(slab_path(root.path(), 1), b"legacy candidate").unwrap();
        std::fs::write(slab_path(root.path(), 3), b"owned candidate excluded").unwrap();
        let mut probes = Vec::new();
        let selected = latest_sealed_legacy_slab_with(&catalog, |id| {
            let record = &catalog.records[&id];
            let write_guard = record
                .descriptor
                .try_write()
                .expect("selection must release its read guard before the metadata probe");
            drop(write_guard);
            probes.push(id);
            slab_path(root.path(), id).is_file()
        });
        assert_eq!(selected, Some(1));
        assert_eq!(
            probes,
            vec![2, 1],
            "skip missing file but exclude owned/active candidates"
        );
        assert_eq!(latest_sealed_legacy_slab_with(&catalog, |_| false), None);
    }

    #[test]
    fn stale_same_id_cleanup_racing_current_forks_preserves_the_current_record() {
        let mut catalog = SlabCatalog::new();
        set_slab_state(&mut catalog, 17, BlockStoreSlabState::Active);
        let old = catalog.writer_lease(17).unwrap();
        let stale = old.record.clone();
        let replacement = old.snapshot();
        drop(old);
        catalog.insert(17, replacement, &mut Vec::new());
        let current = catalog.writer_lease(17).unwrap();
        let start = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            let stale_worker = scope.spawn(|| {
                start.wait();
                for _ in 0..64 {
                    assert!(SlabRecord::lease(&stale).is_none());
                    // Adversarial old callback uses the same cleanup primitive
                    // as last drop; it must not erase a newer incarnation.
                    ActiveLeaseIndex::remove_if_current(&mut current.index.lock(), &stale);
                }
            });
            let current_worker = scope.spawn(|| {
                start.wait();
                for _ in 0..64 {
                    drop(current.fork());
                }
            });
            start.wait();
            stale_worker.join().unwrap();
            current_worker.join().unwrap();
        });
        assert_eq!(catalog.protected_ids(), BTreeSet::from([17]));
        assert_eq!(current.record.writers.load(Ordering::Acquire), 1);
        assert!(Arc::ptr_eq(&current.index.lock()[&17], &current.record));
        drop(current);
        assert!(catalog.protected_ids().is_empty());
    }

    #[test]
    fn leases_keep_the_index_alive_without_a_record_index_cycle() {
        let mut catalog = SlabCatalog::new();
        set_slab_state(&mut catalog, 17, BlockStoreSlabState::Active);
        let lease = catalog.writer_lease(17).unwrap();
        let weak_index = Arc::downgrade(&catalog.active);
        let retained_record = lease.record.clone();
        drop(catalog);
        let fork = lease.fork();
        drop(lease);
        assert_eq!(retained_record.writers.load(Ordering::Acquire), 1);
        assert!(weak_index.upgrade().is_some());
        drop(fork);
        assert_eq!(retained_record.writers.load(Ordering::Acquire), 0);
        assert!(weak_index.upgrade().is_none());
        assert!(SlabRecord::lease(&retained_record).is_none());
    }

    #[test]
    fn active_enumeration_visits_only_leased_records_not_sealed_history() {
        let mut catalog = SlabCatalog::new();
        for id in 0..8192 {
            set_slab_state(&mut catalog, id, BlockStoreSlabState::Sealed);
        }
        let leases = [3, 97, 8001].map(|id| catalog.writer_lease(id).unwrap());
        let fork = leases[0].fork();
        catalog.topology_visits.store(0, Ordering::Relaxed);
        catalog.active.visits.store(0, Ordering::Relaxed);
        assert_eq!(catalog.protected_ids(), BTreeSet::from([3, 97, 8001]));
        assert_eq!(catalog.active.visits.load(Ordering::Relaxed), 3);
        assert_eq!(catalog.topology_visits.load(Ordering::Relaxed), 0);
        // Positive control exercises the actual topology visitor, not len().
        let mut visited = 0;
        for record in catalog.values() {
            drop(record);
            visited += 1;
        }
        assert_eq!(visited, 8192);
        assert_eq!(catalog.topology_visits.load(Ordering::Relaxed), 8192);
        drop(leases);
        assert_eq!(catalog.protected_ids(), BTreeSet::from([3]));
        drop(fork);
        assert!(catalog.protected_ids().is_empty());
    }

    #[test]
    fn last_drop_racing_reacquire_cannot_remove_the_new_membership() {
        use std::sync::{mpsc, Barrier};
        use std::time::Duration;
        let mut catalog = SlabCatalog::new();
        set_slab_state(&mut catalog, 17, BlockStoreSlabState::Active);
        let initial = catalog.writer_lease(17).unwrap();
        let start = Barrier::new(3);
        std::thread::scope(|scope| {
            let (dropped_tx, dropped_rx) = mpsc::channel();
            let (acquired_tx, acquired_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let barrier = &start;
            let source = &catalog;
            let dropping = scope.spawn(move || {
                barrier.wait();
                drop(initial);
                dropped_tx.send(()).unwrap();
            });
            let acquiring = scope.spawn(move || {
                barrier.wait();
                let current = source.writer_lease(17).unwrap();
                acquired_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                drop(current);
            });
            start.wait();
            let dropped = dropped_rx.recv_timeout(Duration::from_secs(5));
            let acquired = acquired_rx.recv_timeout(Duration::from_secs(5));
            let ids = catalog.protected_ids();
            let count = catalog.records[&17].writers.load(Ordering::Acquire);
            release_tx.send(()).unwrap();
            dropping.join().unwrap();
            acquiring.join().unwrap();
            dropped.unwrap();
            acquired.unwrap();
            assert_eq!(ids, BTreeSet::from([17]));
            assert_eq!(count, 1);
        });
        assert!(catalog.protected_ids().is_empty());
    }

    #[test]
    fn concurrent_fork_drops_preserve_the_anchor_and_exact_index_identity() {
        let mut catalog = SlabCatalog::new();
        set_slab_state(&mut catalog, 17, BlockStoreSlabState::Active);
        let anchor = catalog.writer_lease(17).unwrap();
        let start = std::sync::Barrier::new(9);
        std::thread::scope(|scope| {
            let workers = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let fork = anchor.fork();
                        start.wait();
                        drop(fork);
                    })
                })
                .collect::<Vec<_>>();
            start.wait();
            for worker in workers {
                worker.join().unwrap();
            }
        });
        assert_eq!(anchor.record.writers.load(Ordering::Acquire), 1);
        let entries = catalog.active.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert!(Arc::ptr_eq(&entries[&17], &anchor.record));
        drop(entries);
        drop(anchor);
        assert!(catalog.protected_ids().is_empty());
    }

    #[test]
    fn an_old_incarnation_release_cannot_unprotect_the_replacement() {
        let mut catalog = SlabCatalog::new();
        set_slab_state(&mut catalog, 17, BlockStoreSlabState::Active);
        let first = catalog.writer_lease(17).unwrap();
        let old_record = first.record.clone();
        assert!(catalog.protected(17));
        drop(first);
        let mut replacement = old_record.read().clone();
        replacement.logical_bytes = 991;
        catalog.insert(17, replacement, &mut Vec::new());
        let current = catalog.writer_lease(17).unwrap();
        assert!(!Arc::ptr_eq(&old_record, &current.record));
        assert!(SlabRecord::lease(&old_record).is_none());
        // Exercise the exact cleanup primitive against a replacement entry.
        ActiveLeaseIndex::remove_if_current(
            &mut catalog.active.entries.lock().unwrap(),
            &old_record,
        );
        assert!(catalog.protected(17));
        assert_eq!(catalog.protected_ids(), BTreeSet::from([17]));
        assert_eq!(catalog.get(&17).unwrap().logical_bytes, 991);
        drop(current);
        assert!(!catalog.protected(17));
    }

    #[test]
    fn record_manifest_is_byte_identical_to_the_existing_physical_format() {
        let mut catalog = SlabCatalog::new();
        for id in [0, 1, 255, 65536] {
            set_slab_state(&mut catalog, id, BlockStoreSlabState::Sealed);
            let mut slab = catalog.get_mut(&id).unwrap();
            slab.owner_shard_id = Some(id + 9);
            slab.physical_bytes = id + 193;
            slab.logical_bytes = id + 117;
            slab.first_block_id = Some(2);
            slab.last_block_id = Some(31);
            slab.readable_prefix_physical_bytes = slab.physical_bytes;
        }
        let snapshot = catalog.snapshot();
        let prior = super::super::slab_manifest_wire::encode_slab_manifest(1, snapshot.values());
        let records =
            super::super::slab_manifest_wire::encode_slab_record_manifest(1, catalog.values());
        assert_eq!(prior, records, "no physical manifest migration is intended");
        let decoded = super::super::slab_manifest_wire::decode_slab_manifest(&records).unwrap();
        assert_eq!(decoded.slabs, snapshot.into_values().collect::<Vec<_>>());
    }

    #[test]
    fn descriptor_read_and_write_guards_do_not_acquire_the_owner_mutex() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let id = store
            .prepare_next_slab_for_shard(7, u64::MAX)
            .unwrap()
            .unwrap()
            .new_block_slab_id;
        let writer = store.owned_directory.existing(7).unwrap().unwrap();
        let completed = std::thread::scope(|scope| {
            let held = writer.lock().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            let store_ref = &store;
            let worker = scope.spawn(move || {
                let mut inner = store_ref.inner.lock().unwrap();
                assert!(inner.slabs.get(&id).is_some());
                let mut record = inner.slabs.get_mut(&id).unwrap();
                assert_eq!(record.owner_shard_id, Some(7));
                record.owner_shard_id = Some(7);
                tx.send(()).unwrap();
            });
            let result = rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(held);
            worker.join().unwrap();
            result
        });
        completed.expect("record guards finish before explicit owner-mutex release");
    }
}
