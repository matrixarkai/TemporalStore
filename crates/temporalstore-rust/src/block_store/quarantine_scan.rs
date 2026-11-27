// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub(super) struct ScannedSlab {
    id: Option<u64>,
    path: PathBuf,
    metadata: Option<std::fs::Metadata>,
    metadata_error: Option<(std::io::ErrorKind, String)>,
    descriptor: Option<BlockStoreSlabDescriptor>,
}

pub(super) enum RoundEntry {
    Cached(ScannedSlab),
    Remaining(std::fs::DirEntry),
}

impl RoundEntry {
    pub(super) fn file_name(&self) -> std::ffi::OsString {
        match self {
            Self::Cached(entry) => entry.path.file_name().expect("scanned name").to_os_string(),
            Self::Remaining(entry) => entry.file_name(),
        }
    }

    pub(super) fn path(&self) -> PathBuf {
        match self {
            Self::Cached(entry) => entry.path.clone(),
            Self::Remaining(entry) => entry.path(),
        }
    }

    pub(super) fn metadata(&self) -> std::io::Result<std::fs::Metadata> {
        match self {
            Self::Cached(entry) => match &entry.metadata {
                Some(metadata) => Ok(metadata.clone()),
                None => {
                    let (kind, message) = entry.metadata_error.as_ref().expect("metadata observation");
                    Err(std::io::Error::new(*kind, message.clone()))
                }
            },
            Self::Remaining(entry) => entry.metadata(),
        }
    }

    pub(super) fn modified_unix_ms(&self) -> Option<u64> {
        match self {
            Self::Cached(entry) => entry.metadata.as_ref()?.modified().ok().and_then(system_time_unix_ms),
            Self::Remaining(entry) => file_modified_unix_ms(&entry.path()),
        }
    }

    pub(super) fn validate_for_action(&self) -> Result<(), BlockStoreError> {
        if let Self::Cached(entry) = self {
            let observed = self.metadata()?;
            let current = std::fs::metadata(&entry.path)?;
            if !same_file(&observed, &current) { return Err(stale_scan()); }
        }
        Ok(())
    }
}

pub(super) struct QuarantineScan {
    entries: Vec<Result<ScannedSlab, BlockStoreError>>,
    directory: DiscoveryDirectory,
    revision: u64,
    store_id: usize,
}

enum DiscoveryDirectory {
    Native(std::fs::ReadDir),
    #[cfg(test)]
    Ordered(std::vec::IntoIter<std::io::Result<std::fs::DirEntry>>),
}

impl DiscoveryDirectory {
    fn open(root: &std::path::Path) -> std::io::Result<Self> {
        let directory = std::fs::read_dir(delayed_destroy_dir(root))?;
        #[cfg(test)]
        if ORDERED_DISCOVERY.with(|ordered| ordered.get()) {
            // Order real entries only in this scoped fixture; never synthesize rows.
            let mut entries = directory.collect::<std::io::Result<Vec<_>>>()?;
            entries.sort_by_key(|entry| entry.file_name());
            return Ok(Self::Ordered(entries.into_iter().map(Ok).collect::<Vec<_>>().into_iter()));
        }
        Ok(Self::Native(directory))
    }
}

impl Iterator for DiscoveryDirectory {
    type Item = std::io::Result<std::fs::DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        #[cfg(test)]
        notify(ScanPoint::Next);
        match self {
            Self::Native(directory) => directory.next(),
            #[cfg(test)]
            Self::Ordered(entries) => entries.next(),
        }
    }
}

fn discovery_metadata(entry: &std::fs::DirEntry) -> std::io::Result<std::fs::Metadata> {
    #[cfg(test)]
    notify(ScanPoint::Stat);
    entry.metadata()
}

fn stale_scan() -> BlockStoreError {
    std::io::Error::new(std::io::ErrorKind::WouldBlock, "quarantine scan changed; retry").into()
}

fn same_file(a: &std::fs::Metadata, b: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if a.dev() != b.dev() || a.ino() != b.ino() { return false; }
    }
    a.len() == b.len() && a.modified().ok() == b.modified().ok()
        && a.created().ok() == b.created().ok() && a.is_file() == b.is_file()
}

impl QuarantineScan {
    pub(super) fn validate(&self, inner: &BlockStoreInner) -> Result<(), BlockStoreError> {
        if inner._identity.0 != self.store_id || inner.quarantine_scan_revision != self.revision {
            return Err(stale_scan());
        }
        for entry in self.entries.iter().filter_map(|entry| entry.as_ref().ok()) {
            if let Some(id) = entry.id {
                if inner.slabs.get(&id) != entry.descriptor.as_ref() { return Err(stale_scan()); }
            }
        }
        Ok(())
    }

    pub(super) fn into_entries(self) -> impl Iterator<Item = Result<RoundEntry, BlockStoreError>> {
        self.entries.into_iter().map(|entry| entry.map(RoundEntry::Cached))
            .chain(self.directory.map(|entry| entry.map(RoundEntry::Remaining).map_err(BlockStoreError::from)))
    }
}

impl BlockStore {
    pub(super) fn scan_delayed_destroy(
        &self, root: &std::path::Path, revision: u64,
    ) -> Result<QuarantineScan, BlockStoreError> {
        crate::durability_metrics::record_scan("block_store_trash_dir_walk", 1);
        let mut directory = DiscoveryDirectory::open(root)?;
        #[cfg(test)]
        notify(ScanPoint::Opened);
        // Look-ahead is not a candidate budget. The original loop consumes this
        // prefix, then the same iterator, until actual successful work fills its cap.
        let mut entries = Vec::new();
        for _ in 0..DELAYED_DESTROY_MAX_SLABS_PER_ROUND.saturating_add(1) {
            let Some(entry) = directory.next() else { break; };
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => { entries.push(Err(error.into())); break; }
            };
            let id = delayed_destroy_slab_id_from_name(&entry.file_name());
            let (metadata, metadata_error) = if id.is_some() {
                match discovery_metadata(&entry) {
                    Ok(metadata) => (Some(metadata), None),
                    Err(error) => (None, Some((error.kind(), error.to_string()))),
                }
            } else { (None, None) };
            let descriptor = if let Some(id) = id {
                let inner = self.inner.lock().expect("block store lock poisoned");
                inner.check_owned_recovery()?;
                if inner.quarantine_scan_revision != revision { return Err(stale_scan()); }
                inner.slabs.get(&id).cloned()
            } else { None };
            entries.push(Ok(ScannedSlab { id, path: entry.path(), metadata, metadata_error, descriptor }));
        }
        #[cfg(test)]
        notify(ScanPoint::Planned);
        Ok(QuarantineScan { entries, directory, revision, store_id: self.store_id() })
    }
}

impl BlockStoreInner {
    pub(super) fn note_quarantine_scan_change(&mut self) {
        self.quarantine_scan_revision = self.quarantine_scan_revision.checked_add(1)
            .expect("quarantine scan revision exhausted");
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ScanPoint { Opened, Next, Stat, Planned, BeforeSync }

#[cfg(test)]
thread_local! {
    static SCAN_HOOK: std::cell::RefCell<Option<(ScanPoint, Box<dyn FnOnce()>)>> =
        std::cell::RefCell::new(None);
    static ORDERED_DISCOVERY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(super) fn with_ordered_discovery<R>(run: impl FnOnce() -> R) -> R {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) { ORDERED_DISCOVERY.with(|ordered| ordered.set(self.0)); }
    }
    let _restore = Restore(ORDERED_DISCOVERY.with(|ordered| ordered.replace(true)));
    run()
}

#[cfg(test)]
pub(super) fn notify(point: ScanPoint) {
    let hook = SCAN_HOOK.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|(at, _)| *at == point) { slot.take() } else { None }
    });
    if let Some((_, hook)) = hook { hook(); }
}

#[cfg(test)]
pub(super) fn with_hook<R>(point: ScanPoint, hook: impl FnOnce() + 'static, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<(ScanPoint, Box<dyn FnOnce()>)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let retired = SCAN_HOOK.with(|slot| slot.replace(self.0.take()));
            drop(retired);
        }
    }
    let restore = Restore(SCAN_HOOK.with(|slot| slot.replace(Some((point, Box::new(hook))))));
    let result = run();
    drop(restore);
    result
}
