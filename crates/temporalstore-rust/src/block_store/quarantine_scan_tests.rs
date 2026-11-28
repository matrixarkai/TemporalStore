// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use super::quarantine_scan::{with_hook, ScanPoint};
use std::sync::mpsc;
use std::time::Duration;

fn fixture(root: &std::path::Path) -> (BlockStore, ElementEntry, ElementEntry) {
    let store = BlockStore::new(root);
    let victim = store.append(b"quarantined-exact").unwrap();
    store.roll_slab().unwrap();
    let peer = store.append(b"peer-exact").unwrap();
    store.sync_durable().unwrap();
    let moved = store.gc_slabs_before_with_live_refs_delayed_destroy(
        peer.block_slab_id(), [peer.block_slab_id()],
    ).unwrap();
    assert_eq!(moved.delayed_destroy_block_slab_ids, [victim.block_slab_id()]);
    assert_eq!(store.backdate_delayed_destroy_stamps_for_test(DELAYED_DESTROY_MIN_AGE_MS + 10_000).unwrap(), 1);
    (store, victim, peer)
}

struct Paused {
    resume: Option<mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError>>>,
}

impl Paused {
    fn start(store: &BlockStore, point: ScanPoint) -> Self {
        let (entered, ready) = mpsc::channel();
        let (resume, wait) = mpsc::channel();
        let store = store.clone();
        let worker = std::thread::spawn(move || with_hook(point, move || {
            entered.send(()).unwrap();
            wait.recv_timeout(Duration::from_secs(10)).expect("test did not release the scan");
        }, || store.purge_delayed_destroy_slabs_with_report()));
        let paused = Self { resume: Some(resume), worker: Some(worker) };
        if let Err(error) = ready.recv_timeout(Duration::from_secs(5)) {
            drop(paused);
            panic!("scan did not reach its actual boundary: {error}");
        }
        paused
    }

    fn finish(mut self) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        let _ = self.resume.take().unwrap().send(());
        self.worker.take().unwrap().join().expect("scan worker panicked")
    }
}

impl Drop for Paused {
    fn drop(&mut self) {
        if let Some(resume) = self.resume.take() { let _ = resume.send(()); }
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}

fn assert_stale(result: Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError>) {
    assert!(matches!(result, Err(BlockStoreError::Io(ref error))
        if error.kind() == std::io::ErrorKind::WouldBlock), "{result:?}");
}

#[test]
fn paused_quarantine_enumeration_allows_independent_append_and_sync() {
    let dir = tempfile::tempdir().unwrap();
    let (store, victim, peer) = fixture(dir.path());
    let paused = Paused::start(&store, ScanPoint::Opened);
    let writer = store.clone();
    let (done, completed) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result = writer.append_block_of_object_for_shard(42, b"independent", None, None, 0).and_then(|address| {
            writer.sync_durable()?;
            Ok(address)
        });
        let _ = done.send(result);
    });
    let progress = completed.recv_timeout(Duration::from_secs(5));
    let purged = paused.finish();
    writer.join().unwrap();
    let appended = progress.expect("independent writer waited for enumeration").unwrap();
    assert_eq!(store.read(&appended).unwrap(), b"independent");
    assert_eq!(store.read(&peer).unwrap(), b"peer-exact");
    assert_eq!(purged.unwrap().purged_block_slab_ids, [victim.block_slab_id()]);
}

#[test]
fn a_stale_discovery_does_not_destroy_an_entry_twice() {
    let dir = tempfile::tempdir().unwrap();
    let (store, victim, peer) = fixture(dir.path());
    let paused = Paused::start(&store, ScanPoint::Planned);
    let first = store.purge_delayed_destroy_slabs_with_report().unwrap();
    assert_eq!(first.purged_block_slab_ids, [victim.block_slab_id()]);
    assert_stale(paused.finish());
    assert_eq!(store.read(&peer).unwrap(), b"peer-exact");
    assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());
}

#[test]
fn a_concurrent_arrival_invalidates_scan_and_empty_memo() {
    let dir = tempfile::tempdir().unwrap();
    let (store, victim, peer) = fixture(dir.path());
    store.roll_slab().unwrap();
    let current = store.append(b"current-exact").unwrap();
    store.sync_durable().unwrap();
    let paused = Paused::start(&store, ScanPoint::Planned);
    let arrival = store.gc_slabs_before_with_live_refs_delayed_destroy(
        current.block_slab_id(), [current.block_slab_id()],
    ).unwrap();
    assert_eq!(arrival.delayed_destroy_block_slab_ids, [peer.block_slab_id()]);
    assert_stale(paused.finish());
    assert!(store.delayed_destroy_earliest_for_test().is_none());
    assert!(!store.inner.lock().unwrap().quarantine_known_empty);
    let next = store.purge_delayed_destroy_slabs_with_report().unwrap();
    assert_eq!(next.purged_block_slab_ids, [victim.block_slab_id()]);
    assert_eq!(next.retained_too_young_block_slab_ids, [peer.block_slab_id()]);
    assert!(store.delayed_destroy_earliest_for_test().is_some());
    assert_eq!(store.read(&current).unwrap(), b"current-exact");
}

#[test]
fn a_pin_admitted_after_discovery_restores_before_destroying() {
    let dir = tempfile::tempdir().unwrap();
    let (store, victim, peer) = fixture(dir.path());
    let paused = Paused::start(&store, ScanPoint::Planned);
    let mut retained = BlockStoreReadSnapshot::new(&store);
    retained.pin(victim.block_slab_id());
    let result = paused.finish().unwrap();
    assert!(result.purged_block_slab_ids.is_empty());
    assert_eq!(result.restored_block_slab_ids, [victim.block_slab_id()]);
    assert_eq!(store.read(&victim).unwrap(), b"quarantined-exact");
    assert_eq!(store.read(&peer).unwrap(), b"peer-exact");
    assert!(store.inner.lock().unwrap().quarantine_known_empty);
    drop(retained);
}

#[test]
fn a_same_id_install_invalidates_discovery_without_overwriting_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let (store, victim, peer) = fixture(dir.path());
    let paused = Paused::start(&store, ScanPoint::Planned);
    store.install_slab(victim.block_slab_id(), b"replacement-exact").unwrap();
    assert_stale(paused.finish());
    assert_eq!(store.read_slab(victim.block_slab_id()).unwrap(), b"replacement-exact");
    assert_eq!(store.delayed_destroy_slab_ids().unwrap(), [victim.block_slab_id()]);
    assert_eq!(store.read(&peer).unwrap(), b"peer-exact");
}

#[test]
fn scan_keeps_selected_successful_work_cap_and_live_restore_priority() {
    let dir = tempfile::tempdir().unwrap();
    let store = BlockStore::new(dir.path());
    let mut old = Vec::new();
    for _ in 0..6 {
        old.push(store.append(b"retained-exact").unwrap());
        store.roll_slab().unwrap();
    }
    let current = store.append(b"current").unwrap();
    store.sync_durable().unwrap();
    let moved = store.gc_slabs_before_with_live_refs_delayed_destroy(
        current.block_slab_id(), [current.block_slab_id()],
    ).unwrap();
    assert_eq!(moved.delayed_destroy_block_slab_ids.len(), old.len());
    assert_eq!(store.backdate_delayed_destroy_stamps_for_test(DELAYED_DESTROY_MIN_AGE_MS + 10_000).unwrap(), old.len());
    let selected = [old[1].block_slab_id(), old[4].block_slab_id()].into_iter().collect::<BTreeSet<_>>();
    let result = store.purge_delayed_destroy_slabs_capped(
        DELAYED_DESTROY_MIN_AGE_MS, [current.block_slab_id()], Some(selected.clone()), 2,
    ).unwrap();
    assert_eq!(result.processed_block_slabs, 2);
    assert_eq!(result.purged_block_slab_ids.into_iter().collect::<BTreeSet<_>>(), selected);
    assert_eq!(store.delayed_destroy_slab_ids().unwrap().len(), 4);
    let mut retained = BlockStoreReadSnapshot::new(&store);
    retained.pin(old[0].block_slab_id());
    let untouched = store.purge_delayed_destroy_slabs_selected(
        DELAYED_DESTROY_MIN_AGE_MS, [old[0].block_slab_id()], Some(BTreeSet::new()),
    ).unwrap();
    assert_eq!(untouched.processed_block_slabs, 0);
    let restored = store.restore_delayed_destroy_slabs_selected(
        DELAYED_DESTROY_MIN_AGE_MS, [old[0].block_slab_id()],
        Some([old[0].block_slab_id()].into_iter().collect()),
    ).unwrap();
    assert_eq!(restored.restored_block_slab_ids, [old[0].block_slab_id()]);
    assert_eq!(store.read(&old[0]).unwrap(), b"retained-exact");
    assert_eq!(store.read(&current).unwrap(), b"current");
}

#[test]
fn a_failed_directory_sync_does_not_publish_manifest_before_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("store");
    let parked = dir.path().join("parked");
    let (store, victim, peer) = fixture(&root);
    let before = std::fs::read(slab_manifest_path(&root)).unwrap();
    let from = root.clone();
    let to = parked.clone();
    let failed = with_hook(ScanPoint::BeforeSync, move || std::fs::rename(from, to).unwrap(),
        || store.purge_delayed_destroy_slabs_with_report());
    assert!(!root.exists(), "failed directory barrier must not publish a replacement store");
    assert!(matches!(&failed, Err(BlockStoreError::Io(error))
        if error.kind() == std::io::ErrorKind::NotFound));
    std::fs::rename(&parked, &root).unwrap();
    assert!(matches!(failed, Err(BlockStoreError::Io(_))));
    assert_eq!(std::fs::read(slab_manifest_path(&root)).unwrap(), before);
    assert!(!slab_path(&root, victim.block_slab_id()).exists());
    drop(store);
    let reopened = BlockStore::new(&root);
    assert_eq!(reopened.read(&peer).unwrap(), b"peer-exact");
    assert!(reopened.delayed_destroy_slab_ids().unwrap().is_empty());
}

#[test]
fn productive_discovery_next_and_stat_allow_durable_commit_before_release() {
    for point in [ScanPoint::Next, ScanPoint::Stat] {
        let dir = tempfile::tempdir().unwrap();
        let (store, victim, peer) = fixture(dir.path());
        let seed = store.append_block_of_object_for_shard(42, b"writer-seed", None, None, 0).unwrap();
        store.sync_durable().unwrap();
        let paused = Paused::start(&store, point);
        let writing = store.clone();
        let (done, completion) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            let result = writing.append_block_of_object_for_shard(
                42, b"committed-before-discovery-release", None, None, 1,
            ).and_then(|address| {
                writing.sync_durable()?;
                Ok(address)
            });
            let _ = done.send(result);
        });
        // Capture the result BEFORE releasing discovery. A late successful append
        // cannot turn this timeout into a passing observation. Always release/join.
        let before_release = completion.recv_timeout(Duration::from_secs(5));
        let purged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| paused.finish()));
        let writer_joined = writer.join();
        let purged = purged.expect("scan panicked after releasing discovery");
        writer_joined.expect("peer writer panicked");
        let address = before_release.expect("productive discovery held the catalog").unwrap();
        assert_eq!(store.read(&address).unwrap(), b"committed-before-discovery-release");
        assert_eq!(store.read(&seed).unwrap(), b"writer-seed");
        assert_eq!(store.read(&peer).unwrap(), b"peer-exact");
        assert_eq!(purged.unwrap().purged_block_slab_ids, [victim.block_slab_id()]);
        drop(store);
        let reopened = BlockStore::new(dir.path());
        assert_eq!(reopened.read(&address).unwrap(), b"committed-before-discovery-release");
        assert_eq!(reopened.read(&peer).unwrap(), b"peer-exact");
    }
}

#[test]
fn successful_work_reaches_past_a_full_skipped_prefix_without_false_memos() {
    super::quarantine_scan::with_ordered_discovery(|| {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let count = DELAYED_DESTROY_MAX_SLABS_PER_ROUND + 5;
        for id in 0..=count as u64 {
            store.install_slab(id, b"small-real-slab").unwrap();
        }
        store.sync_durable().unwrap();
        let current = count as u64;
        let moved = store.gc_slabs_before_with_live_refs_delayed_destroy(current, [current]).unwrap();
        assert_eq!(moved.delayed_destroy_block_slab_ids.len(), count);
        // Independently enumerate REAL names, then explicitly sort. No assumption
        // about native ReadDir order, ID order, or a separate unsorted listing.
        let mut entries = std::fs::read_dir(delayed_destroy_dir(dir.path())).unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let id = delayed_destroy_slab_id_from_name(&entry.file_name()).unwrap();
                (entry.file_name(), entry.path(), id)
            }).collect::<Vec<_>>();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(entries.len(), count);
        let bytes = entries.iter().map(|(_, path, _)| path.metadata().unwrap().len()).sum::<u64>();
        assert!(bytes <= 64 * count as u64, "bounded physical file contents, not an allocated-block claim");
        let skipped = &entries[..count - 2];
        assert!(skipped.len() > DELAYED_DESTROY_MAX_SLABS_PER_ROUND + 1);
        let victims = &entries[count - 2..];
        let mature = victims.iter().map(|(_, _, id)| *id).collect::<BTreeSet<_>>();
        assert_eq!(store.backdate_named_delayed_destroy_stamps_for_test(
            Some(&mature), DELAYED_DESTROY_MIN_AGE_MS + 10_000,
        ).unwrap(), 2);
        // One selected young entry and >1,001 skipped entries precede any work.
        let young = skipped[0].2;
        let selected = mature.iter().copied().chain([young]).collect::<BTreeSet<_>>();
        let first = store.purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS, [current], Some(selected.clone()), 1,
        ).unwrap();
        assert_eq!(first.processed_block_slabs, 1);
        assert_eq!(first.purged_block_slab_ids, [victims[0].2]);
        assert!(first.examined_block_slabs > DELAYED_DESTROY_MAX_SLABS_PER_ROUND + 1);
        assert!(first.budget_exhausted);
        assert!(store.delayed_destroy_earliest_for_test().is_none());
        assert!(!store.inner.lock().unwrap().quarantine_known_empty);
        assert!(!victims[0].1.exists());
        assert!(victims[1].1.exists());
        assert!(skipped.iter().all(|(_, path, _)| path.exists()));
        let second = store.purge_delayed_destroy_slabs_capped(
            DELAYED_DESTROY_MIN_AGE_MS, [current], Some(selected), 1,
        ).unwrap();
        assert_eq!(second.processed_block_slabs, 1);
        assert_eq!(second.purged_block_slab_ids, [victims[1].2]);
        assert!(second.examined_block_slabs > DELAYED_DESTROY_MAX_SLABS_PER_ROUND + 1);
        assert!(!second.budget_exhausted, "the iterator really reached its end");
        assert_eq!(second.retained_too_young_block_slab_ids, [young]);
        assert!(store.delayed_destroy_earliest_for_test().is_some());
        assert!(!store.inner.lock().unwrap().quarantine_known_empty);
        assert!(skipped.iter().all(|(_, path, _)| path.exists()));
        assert_eq!(std::fs::read(&skipped[0].1).unwrap(), b"small-real-slab");
        assert_eq!(store.read_slab(current).unwrap(), b"small-real-slab");
        assert_eq!(store.delayed_destroy_slab_ids().unwrap().len(), count - 2);
        drop(store);
        let reopened = BlockStore::new(dir.path());
        assert_eq!(reopened.read_slab(current).unwrap(), b"small-real-slab");
        assert_eq!(reopened.delayed_destroy_slab_ids().unwrap().len(), count - 2);
        assert!(mature.is_disjoint(&reopened.delayed_destroy_slab_ids().unwrap().into_iter().collect()));
    });
}
