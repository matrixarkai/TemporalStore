// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::fs::{self, File};
use std::io::{Error, ErrorKind};
use std::path::Path;

use super::record::{encode_slab_owner, slab_owner, OWNED_SLAB_HEADER_LEN};

#[derive(Debug)]
pub(super) struct OwnedWriterDirectory {
    state: std::sync::RwLock<OwnedWriterDirectoryState>,
    #[cfg(test)]
    reads: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    writes: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    after_cold_miss: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    #[cfg(test)]
    writer_locks: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    next_writer_lock: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

#[derive(Debug)]
struct OwnedWriterDirectoryState {
    writers: BTreeMap<u64, Arc<Mutex<OwnedWriter>>>,
    options: BlockStoreOptions,
    recovery_error: Option<String>,
}

impl OwnedWriterDirectoryState {
    fn check_admission(&self) -> Result<(), BlockStoreError> {
        if let Some(error) = &self.recovery_error {
            return Err(Error::new(ErrorKind::InvalidData, error.clone()).into());
        }
        Ok(())
    }
}

impl OwnedWriterDirectory {
    pub(super) fn new(
        writers: BTreeMap<u64, Arc<Mutex<OwnedWriter>>>,
        options: BlockStoreOptions,
        recovery_error: Option<String>,
    ) -> Self {
        Self {
            state: std::sync::RwLock::new(OwnedWriterDirectoryState {
                writers, options, recovery_error,
            }),
            #[cfg(test)]
            reads: Default::default(),
            #[cfg(test)]
            writes: Default::default(),
            #[cfg(test)]
            after_cold_miss: Mutex::new(None),
            #[cfg(test)]
            writer_locks: Default::default(),
            #[cfg(test)]
            next_writer_lock: Mutex::new(None),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, OwnedWriterDirectoryState> {
        #[cfg(test)]
        self.reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state.read().expect("owned writer directory lock poisoned")
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, OwnedWriterDirectoryState> {
        #[cfg(test)]
        self.writes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.state.write().expect("owned writer directory lock poisoned")
    }

    pub(super) fn check_admission(&self) -> Result<(), BlockStoreError> {
        self.read().check_admission()
    }

    pub(super) fn options(&self) -> BlockStoreOptions {
        self.read().options
    }

    pub(super) fn lock_writer<'a>(
        &self,
        writer: &'a Mutex<OwnedWriter>,
    ) -> std::sync::MutexGuard<'a, OwnedWriter> {
        #[cfg(test)]
        {
            self.writer_locks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(entered) = self.next_writer_lock.lock().unwrap().take() {
                let _ = entered.send(());
            }
        }
        writer.lock().expect("owned slab writer lock poisoned")
    }

    fn get_or_insert(
        &self,
        shard_id: u64,
    ) -> Result<(Arc<Mutex<OwnedWriter>>, BlockStoreOptions), BlockStoreError> {
        {
            let state = self.read();
            state.check_admission()?;
            if let Some(writer) = state.writers.get(&shard_id) {
                return Ok((writer.clone(), state.options));
            }
        }
        #[cfg(test)]
        {
            let pause = self.after_cold_miss.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                entered.send(()).unwrap();
                resume.recv_timeout(std::time::Duration::from_secs(10))
                    .expect("cold insertion requires explicit release");
            }
        }
        // Recheck admission and insertion together after upgrading; never publish a second owner.
        let mut state = self.write();
        state.check_admission()?;
        let options = state.options;
        let writer = state.writers.entry(shard_id).or_default().clone();
        Ok((writer, options))
    }

    pub(super) fn snapshot(&self) -> Result<Vec<Arc<Mutex<OwnedWriter>>>, BlockStoreError> {
        let state = self.read();
        state.check_admission()?;
        Ok(state.writers.values().cloned().collect())
    }

    pub(super) fn existing(
        &self,
        shard_id: u64,
    ) -> Result<Option<Arc<Mutex<OwnedWriter>>>, BlockStoreError> {
        let state = self.read();
        state.check_admission()?;
        Ok(state.writers.get(&shard_id).cloned())
    }
}

#[cfg(test)]
thread_local! {
    static FAIL_CURSOR_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Debug, Default)]
pub(super) struct OwnedWriter {
    current: Option<OwnedSlab>,
    needs_directory_sync: bool,
    pending_retirement: Option<SlabWriterLease>,
    #[cfg(test)]
    after_complete_write: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
    #[cfg(test)]
    after_roll_io: Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>,
}

#[derive(Debug)]
struct OwnedSlab {
    id: u64,
    owner: u64,
    offset: u64,
    file: File,
    dirty: bool,
    needs_truncate: bool,
    lease: SlabWriterLease,
}

fn reservation_path(root: &Path, id: u64) -> PathBuf {
    root.join(format!("slab_reservation_{id:020}"))
}

pub(super) fn has_owned_format(root: &Path, id: u64) -> std::io::Result<bool> {
    reservation_path(root, id).try_exists()
}

pub(super) fn stored_slab_owner(root: &Path, id: u64) -> Result<Option<u64>, BlockStoreError> {
    match fs::read(reservation_path(root, id)) {
        Ok(bytes) => slab_owner(&bytes, id)?
            .map(Some)
            .ok_or_else(|| invalid_cursor().into()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(super) fn owned_format_ids(root: &Path) -> std::io::Result<BTreeSet<u64>> {
    let mut ids = BTreeSet::new();
    if !root.is_dir() {
        return Ok(ids);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("slab_reservation_")?.parse::<u64>().ok())
        {
            ids.insert(id);
        }
    }
    Ok(ids)
}

fn cursor_path(root: &Path, owner: u64) -> PathBuf {
    root.join(format!("slab_writer_{owner:020}"))
}

pub(super) fn check_imported_slab_owner(
    root: &Path,
    id: u64,
    owner: Option<u64>,
) -> Result<(), BlockStoreError> {
    let path = reservation_path(root, id);
    match fs::read(&path) {
        Ok(bytes) => {
            let registered = slab_owner(&bytes, id)?.ok_or_else(invalid_cursor)?;
            if owner != Some(registered) {
                return Err(invalid_cursor().into());
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            if let Some(owner) = owner {
                match File::open(slab_path(root, id)) {
                    Ok(mut file) => {
                        let mut existing = [0; OWNED_SLAB_HEADER_LEN];
                        if file.read_exact(&mut existing).is_err()
                            || slab_owner(&existing, id)? != Some(owner)
                        {
                            return Err(Error::new(
                                ErrorKind::AlreadyExists,
                                "cannot change the ownership of an installed slab",
                            )
                            .into());
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
                publish_new_header(&path, &encode_slab_owner(id, owner), false)?;
                sync_writer_directory(root)?;
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn invalid_cursor() -> Error {
    Error::new(ErrorKind::InvalidData, "invalid slab writer cursor")
}

fn sync_writer_directory(root: &Path) -> std::io::Result<()> {
    File::open(root)?.sync_all()
}

const CURSOR_LEN: usize = OWNED_SLAB_HEADER_LEN + 8 + 4;

fn publish_cursor(root: &Path, owner: u64, id: u64, offset: u64) -> std::io::Result<()> {
    let mut bytes = [0; CURSOR_LEN];
    bytes[..OWNED_SLAB_HEADER_LEN].copy_from_slice(&encode_slab_owner(id, owner));
    bytes[OWNED_SLAB_HEADER_LEN..CURSOR_LEN - 4].copy_from_slice(&offset.to_le_bytes());
    let checksum = crate::checksum::crc32c(&bytes[..CURSOR_LEN - 4]);
    bytes[CURSOR_LEN - 4..].copy_from_slice(&checksum.to_le_bytes());
    let cursor = cursor_path(root, owner);
    let temp = cursor.with_extension("tmp");
    let mut file = File::create(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(temp, cursor)
}

fn durable_cursor_offset(bytes: &[u8]) -> std::io::Result<u64> {
    let bytes: &[u8; CURSOR_LEN] = bytes.try_into().map_err(|_| invalid_cursor())?;
    let checksum = u32::from_le_bytes(bytes[CURSOR_LEN - 4..].try_into().unwrap());
    if checksum != crate::checksum::crc32c(&bytes[..CURSOR_LEN - 4]) {
        return Err(invalid_cursor());
    }
    let offset = u64::from_le_bytes(
        bytes[OWNED_SLAB_HEADER_LEN..CURSOR_LEN - 4]
            .try_into()
            .unwrap(),
    );
    if offset < OWNED_SLAB_HEADER_LEN as u64 {
        return Err(invalid_cursor());
    }
    Ok(offset)
}

fn publish_new_header(path: &Path, header: &[u8], append: bool) -> std::io::Result<File> {
    let temp = path.with_extension("writing");
    let mut options = OpenOptions::new();
    options.write(true).append(append).create_new(true);
    let mut file = options.open(&temp)?;
    let published = (|| {
        file.write_all(header)?;
        file.sync_all()?;
        fs::hard_link(&temp, path)
    })();
    let cleanup = fs::remove_file(temp);
    published?;
    cleanup?;
    Ok(file)
}

impl OwnedWriter {
    pub(super) fn id(&self) -> Option<u64> {
        self.current.as_ref().map(|slab| slab.id)
    }

    pub(super) fn offset(&self) -> u64 {
        self.current.as_ref().map_or(0, |slab| slab.offset)
    }

    #[cfg(test)]
    pub(super) fn roll(
        &mut self,
        root: &Path,
        owner: u64,
        id: u64,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        let mut slabs = SlabCatalog::new();
        set_slab_state(&mut slabs, id, BlockStoreSlabState::Active);
        slabs.get_mut(&id).unwrap().owner_shard_id = Some(owner);
        self.roll_registered(root, owner, id, slabs.writer_lease(id).unwrap())
    }

    fn roll_registered(
        &mut self,
        root: &Path,
        owner: u64,
        id: u64,
        lease: SlabWriterLease,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        self.sync(root)?;
        fs::create_dir_all(root)?;
        let previous = self.id();
        let previous_block_slab_id = previous.unwrap_or(id);
        let header = encode_slab_owner(id, owner);
        let path = slab_path(root, id);
        if path.try_exists()? {
            return Err(Error::new(ErrorKind::AlreadyExists, "slab ID already exists").into());
        }
        let reservation = reservation_path(root, id);
        publish_new_header(&reservation, &header, false)?;
        let file = {
            let directory = File::open(root)?;
            directory.sync_all()?;

            let file = publish_new_header(&path, &header, true)?;
            directory.sync_all()?;
            file
        };

        let initial_offset = OWNED_SLAB_HEADER_LEN as u64;
        publish_cursor(root, owner, id, initial_offset)?;

        // Once the pointer has been renamed, never resume writing to the outgoing
        // slab, even if the directory barrier needs another attempt.
        let outgoing = self.current.replace(OwnedSlab {
            id,
            owner,
            offset: initial_offset,
            file,
            dirty: false,
            needs_truncate: false,
            lease,
        });
        self.needs_directory_sync = true;
        self.current.as_ref().expect("published owner cursor").lease.publish_header(initial_offset);
        self.pending_retirement = outgoing.map(|slab| {
            slab.lease.seal();
            slab.lease
        });
        self.sync(root)?;
        Ok(BlockStoreRollReport {
            previous_block_slab_id,
            new_block_slab_id: id,
        })
    }

    #[cfg(test)]
    pub(super) fn recover(
        root: &Path,
        owner: u64,
        slab: &BlockStoreSlabDescriptor,
    ) -> Result<Self, BlockStoreError> {
        Self::recover_registered(root, owner, SlabWriterLease::standalone(slab.clone()))
    }

    fn recover_registered(
        root: &Path,
        owner: u64,
        lease: SlabWriterLease,
    ) -> Result<Self, BlockStoreError> {
        let slab = lease.snapshot();
        let id = slab.block_slab_id;
        let cursor = fs::read(cursor_path(root, owner))?;
        let durable_offset = durable_cursor_offset(&cursor)?;
        let expected_header = &cursor[..OWNED_SLAB_HEADER_LEN];
        let reservation = fs::read(reservation_path(root, id))?;
        if slab_owner(&cursor, id)? != Some(owner)
            || reservation != expected_header
            || slab.owner_shard_id != Some(owner)
            || slab.readable_prefix_physical_bytes < durable_offset
        {
            return Err(invalid_cursor().into());
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(slab_path(root, id))?;
        let mut actual = [0; OWNED_SLAB_HEADER_LEN];
        file.read_exact(&mut actual)?;
        if actual.as_slice() != expected_header {
            return Err(invalid_cursor().into());
        }
        let offset = slab.readable_prefix_physical_bytes;
        let physical_bytes = file.metadata()?.len();
        if physical_bytes < offset {
            return Err(invalid_cursor().into());
        }
        if physical_bytes > offset {
            let tail_len = physical_bytes - offset;
            // The verified prefix includes the last published cursor. Only a
            // physically incomplete next record outside it is repairable.
            let mut tail = [0; BLOCK_RECORD_HEADER_LEN];
            let tail = &mut tail[..tail_len.min(BLOCK_RECORD_HEADER_LEN as u64) as usize];
            file.seek(std::io::SeekFrom::Start(offset))?;
            file.read_exact(tail)?;
            if !super::record::incomplete_block_record_tail(tail, tail_len, id, offset) {
                return Err(invalid_cursor().into());
            }
            file.set_len(offset)?;
            file.sync_all()?;
        }
        Ok(Self {
            current: Some(OwnedSlab {
                id,
                owner,
                offset,
                file,
                dirty: false,
                needs_truncate: false,
                lease,
            }),
            needs_directory_sync: false,
            pending_retirement: None,
            #[cfg(test)]
            after_complete_write: None,
            #[cfg(test)]
            after_roll_io: None,
        })
    }

    pub(super) fn append(
        &mut self,
        root: &Path,
        bytes: &[u8],
        block_id: u64,
        object_id: Option<u64>,
    ) -> Result<ElementEntry, BlockStoreError> {
        if self.needs_directory_sync {
            self.sync(root)?;
        }
        let slab = self
            .current
            .as_mut()
            .expect("owned slab allocated before append");
        let address = ElementEntry::try_from_parts(
            slab.id,
            slab.offset,
            bytes.len() as u64,
            Some(block_id),
            object_id,
        )?;
        // An interrupted write must never become a hole before a later append.
        // Retry starts by restoring the last fully written offset.
        if slab.needs_truncate {
            slab.file.set_len(slab.offset)?;
        }
        slab.dirty = true;
        slab.needs_truncate = true;
        slab.file.write_all(bytes)?;
        slab.needs_truncate = false;
        slab.offset += address.length();
        #[cfg(test)]
        if let Some((entered, release)) = self.after_complete_write.take() {
            entered.send(()).unwrap();
            release.recv_timeout(std::time::Duration::from_secs(10))
                .expect("append requires explicit release after complete write");
        }
        Ok(address)
    }

    pub(super) fn sync(&mut self, root: &Path) -> Result<(), BlockStoreError> {
        if self.needs_directory_sync {
            #[cfg(test)]
            if FAIL_CURSOR_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
                return Err(Error::other("injected cursor directory sync failure").into());
            }
            sync_writer_directory(root)?;
            self.needs_directory_sync = false;
        }
        if let Some(slab) = self.current.as_mut() {
            if slab.dirty {
                if slab.needs_truncate {
                    slab.file.set_len(slab.offset)?;
                    slab.needs_truncate = false;
                }
                slab.file.sync_data()?;
                // Data precedes the committed cursor so recovery can enforce
                // a lower bound independently of the extent manifest.
                if !slab_path(root, slab.id).try_exists()? {
                    return Err(
                        Error::new(ErrorKind::NotFound, "active owned slab is missing").into(),
                    );
                }
                publish_cursor(root, slab.owner, slab.id, slab.offset)?;
                sync_writer_directory(root)?;
                slab.dirty = false;
            }
        }
        // Only successful cursor/root publication resolves the exact outgoing
        // incarnation. Dropping this lease never looks up a possibly reused ID.
        self.pending_retirement.take();
        Ok(())
    }
}

pub(super) fn recover_owned_writers(
    root: &Path,
    slabs: &mut SlabCatalog,
) -> Result<(BTreeMap<u64, Arc<Mutex<OwnedWriter>>>, u64), BlockStoreError> {
    let mut writers = BTreeMap::new();
    let mut highest = 0;
    let mut quarantined = None;
    if !root.is_dir() {
        return Ok((writers, highest));
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(stem) = name.strip_prefix("slab_reservation_") {
            let pending = stem.strip_suffix(".writing");
            let id = pending.unwrap_or(stem).parse::<u64>().map_err(|_| invalid_cursor())?;
            if id > MAX_ADDRESSABLE_BLOCK_SLAB_ID {
                return Err(invalid_cursor().into());
            }
            highest = highest.max(id);
            // A staging name fences allocation even if its contents were only
            // partly written. Ownership still requires the published name.
            if pending.is_some() {
                continue;
            }
            let reservation = fs::read(entry.path())?;
            let owner = slab_owner(&reservation, id)?.ok_or_else(invalid_cursor)?;
            if let Some(slab) = slabs.get(&id).map(|slab| slab.clone()) {
                if slab.state == BlockStoreSlabState::DelayedDestroy {
                    if quarantined.is_none() {
                        let mut paths = BTreeMap::new();
                        for entry in fs::read_dir(delayed_destroy_dir(root))? {
                            let entry = entry?;
                            if let Some(id) = delayed_destroy_slab_id_from_name(&entry.file_name())
                            {
                                if paths.insert(id, entry.path()).is_some() {
                                    return Err(invalid_cursor().into());
                                }
                            }
                        }
                        quarantined = Some(paths);
                    }
                    let paths = quarantined.as_ref().expect("quarantine directory indexed");
                    let path = paths.get(&id).ok_or_else(invalid_cursor)?;
                    let mut header = [0; OWNED_SLAB_HEADER_LEN];
                    File::open(path)?.read_exact(&mut header)?;
                    if slab_owner(&header, id)? != Some(owner) {
                        return Err(invalid_cursor().into());
                    }
                    slabs.get_mut(&id).expect("recovered quarantined record").owner_shard_id = Some(owner);
                } else if slab.state != BlockStoreSlabState::Purged
                    && slab.owner_shard_id != Some(owner)
                {
                    return Err(invalid_cursor().into());
                }
            } else if slab_path(root, id).exists() {
                return Err(invalid_cursor().into());
            }
        } else if let Some(owner) = name.strip_prefix("slab_writer_") {
            if owner.ends_with(".tmp") {
                continue;
            }
            let owner = owner.parse::<u64>().map_err(|_| invalid_cursor())?;
            let cursor = fs::read(entry.path())?;
            let id = u64::from_le_bytes(
                cursor
                    .get(8..16)
                    .ok_or_else(invalid_cursor)?
                    .try_into()
                    .unwrap(),
            );
            if slabs.protected(id) { return Err(invalid_cursor().into()); }
            let lease = slabs.writer_lease(id).ok_or_else(invalid_cursor)?;
            let writer = OwnedWriter::recover_registered(root, owner, lease)?;
            let mut slab = slabs.get_mut(&id).ok_or_else(invalid_cursor)?;
            slab.physical_bytes = writer.offset();
            slab.has_corruption = false;
            slab.first_error = None;
            slab.first_error_offset = None;
            slab.state = BlockStoreSlabState::Active;
            if writers
                .insert(owner, Arc::new(Mutex::new(writer)))
                .is_some()
            {
                return Err(invalid_cursor().into());
            }
        }
    }
    Ok((writers, highest))
}

impl BlockStoreInner {
    pub(super) fn check_owned_recovery(&self) -> Result<(), BlockStoreError> {
        self.owned_directory.check_admission()
    }

    pub(super) fn protects_writer_slab(&self, id: u64) -> bool {
        self.block_slab_id == id || self.slabs.protected(id)
    }

    pub(super) fn cleanup_purged_reservations(
        &self,
        ids: &[u64],
        purged_from: &Path,
    ) -> Result<(), BlockStoreError> {
        let purged = ids
            .iter()
            .copied()
            .filter(|id| {
                *id < self.owned_highest_id
                    && self.slabs.get(id).is_some_and(|slab| {
                        slab.owner_shard_id.is_some() && slab.state == BlockStoreSlabState::Purged
                    })
            })
            .collect::<Vec<_>>();
        if purged.is_empty() {
            return Ok(());
        }
        // Keep one durable reservation as the allocator's high-water mark.
        let highest = self.owned_highest_id;
        let mark = match fs::read(reservation_path(&self.root, highest)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        if slab_owner(&mark, highest)?.is_none() {
            return Err(invalid_cursor().into());
        }
        sync_writer_directory(purged_from)?;
        if purged_from != self.root.as_path() {
            sync_writer_directory(&self.root)?;
        }
        for id in purged {
            if slab_path(&self.root, id).try_exists()? {
                continue;
            }
            match fs::remove_file(reservation_path(&self.root, id)) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_writer_directory(&self.root)?;
        Ok(())
    }
}

impl BlockStore {
    #[cfg(test)]
    pub(crate) fn owned_slab_position_for_test(&self, shard_id: u64) -> Option<(u64, u64)> {
        let writer = self.owned_directory.existing(shard_id).ok()??;
        let writer = self.owned_directory.lock_writer(&writer);
        Some((writer.id()?, writer.offset()))
    }

    pub(crate) fn active_block_slab_ids(&self) -> BTreeSet<u64> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let mut active = inner.slabs.protected_ids();
        active.insert(inner.block_slab_id);
        active
    }

    fn owned_writer(
        &self,
        shard_id: u64,
    ) -> Result<(Arc<Mutex<OwnedWriter>>, BlockStoreOptions), BlockStoreError> {
        self.owned_directory.get_or_insert(shard_id)
    }

    fn roll_owned_writer(
        &self,
        shard_id: u64,
        writer: &mut OwnedWriter,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        writer.sync(&self.read_view.root)?;
        let (id, lease) = {
            let mut inner = self.inner.lock().expect("block store lock poisoned");
            let highest = inner
                .owned_highest_id
                .max(inner.block_slab_id)
                .max(inner.slabs.last_key_value().map_or(0, |(id, _)| *id));
            let id = highest
                .checked_add(1)
                .filter(|&id| id <= MAX_ADDRESSABLE_BLOCK_SLAB_ID)
                .ok_or(BlockAddressOutOfRange {
                    block_slab_id: highest.saturating_add(1),
                    offset: 0,
                    block_id: None,
                })?;
            inner.owned_highest_id = id;
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Active);
            inner.slabs.get_mut(&id).expect("reserved owned record").owner_shard_id = Some(shard_id);
            (id, inner.slabs.writer_lease(id).expect("reserved owned lease"))
        };
        // Retain the provisional claim through catalog finalization even when
        // roll_registered fails before installing its own current-writer lease.
        let result = writer.roll_registered(&self.read_view.root, shard_id, id, lease.fork());
        #[cfg(test)]
        if let Some((entered, release)) = writer.after_roll_io.take() {
            entered.send(()).unwrap();
            release.recv_timeout(std::time::Duration::from_secs(10))
                .expect("roll completion requires explicit release");
        }
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if writer.id() == Some(id) {
            // The outgoing exact record was sealed before its lease could be
            // released. Do not restamp an ID that GC may already have retired.
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Active);
            let mut slab = inner
                .slabs
                .get_mut(&id)
                .expect("registered active owned slab");
            slab.owner_shard_id = Some(shard_id);
            slab.physical_bytes = writer.offset();
            slab.readable_prefix_physical_bytes = writer.offset();
        } else {
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Sealed);
        }
        drop(inner);
        drop(lease);
        result
    }

    pub(crate) fn roll_slab_for_shard(
        &self,
        shard_id: u64,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        let (writer, _) = self.owned_writer(shard_id)?;
        let mut writer = self.owned_directory.lock_writer(&writer);
        self.roll_owned_writer(shard_id, &mut writer)
    }

    pub(crate) fn prepare_next_slab_for_shard(
        &self,
        shard_id: u64,
        slab_target_bytes: u64,
    ) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        let (writer, _) = self.owned_writer(shard_id)?;
        let mut writer = self.owned_directory.lock_writer(&writer);
        if writer.id().is_some() && !slab_is_at_target(writer.offset(), slab_target_bytes) {
            return Ok(None);
        }
        self.roll_owned_writer(shard_id, &mut writer).map(Some)
    }

    pub(crate) fn append_block_of_object_for_shard(
        &self,
        shard_id: u64,
        bytes: &[u8],
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        block_ordinal: u32,
    ) -> Result<ElementEntry, BlockStoreError> {
        let (writer, options) = self.owned_writer(shard_id)?;
        let mut writer = self.owned_directory.lock_writer(&writer);
        let address = self.append_owned_record(
            shard_id,
            &mut writer,
            options,
            (bytes, object_id, routing_bucket, block_ordinal),
        )?;
        if !bulk_relaxed_durability() && !block_wal_single_barrier() {
            writer.sync(&self.read_view.root)?;
        }
        Ok(address)
    }

    pub(crate) fn append_batch_with_block_metadata_for_shard(
        &self,
        shard_id: u64,
        records: Vec<BlockAppendRecord<'_>>,
    ) -> Result<Vec<ElementEntry>, BlockStoreError> {
        let (writer, options) = self.owned_writer(shard_id)?;
        let mut writer = self.owned_directory.lock_writer(&writer);
        let mut addresses = Vec::with_capacity(records.len());
        for record in records {
            addresses.push(self.append_owned_record(shard_id, &mut writer, options, record)?);
        }
        if !bulk_relaxed_durability() && !block_wal_single_barrier() {
            writer.sync(&self.read_view.root)?;
        }
        Ok(addresses)
    }

    fn append_owned_record(
        &self,
        shard_id: u64,
        writer: &mut OwnedWriter,
        options: BlockStoreOptions,
        (bytes, object_id, routing_bucket, ordinal): BlockAppendRecord<'_>,
    ) -> Result<ElementEntry, BlockStoreError> {
        let block_id = u64::from(ordinal);
        let record = encode_block_record(bytes, block_id, object_id, routing_bucket, options)?;
        if writer.id().is_none()
            || (writer.offset() > OWNED_SLAB_HEADER_LEN as u64
                && should_roll_before_append(
                    writer.offset(),
                    record.bytes.len() as u64,
                    effective_block_slab_target_bytes(),
                ))
        {
            self.roll_owned_writer(shard_id, writer)?;
        }
        let address = writer.append(&self.read_view.root, &record.bytes, block_id, object_id)?;
        writer.current.as_ref().expect("appended owned record").lease.publish_append(
            writer.offset(), record.logical_len as u64, block_id,
        );
        let compressed = record.compression == BlockRecordCompression::Zstd;
        self.write_accounting.add(1, address.length(), record.logical_len as u64,
            u64::from(compressed), if compressed { record.logical_len.saturating_sub(record.stored_len) as u64 } else { 0 });
        Ok(address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append_deferred(store: &BlockStore, owner: u64, bytes: &[u8]) -> ElementEntry {
        let (writer, options) = store.owned_writer(owner).unwrap();
        let mut writer = writer.lock().unwrap();
        store.append_owned_record(owner, &mut writer, options, (bytes, None, None, 0)).unwrap()
    }

    fn assert_warm_append_fits(store: &BlockStore, owner: u64, bytes: &[u8]) {
        let (writer, options) = store.owned_writer(owner).unwrap();
        let writer = writer.lock().unwrap();
        assert!(writer.id().is_some());
        let encoded = encode_block_record(bytes, 4, Some(17), Some(3), options).unwrap();
        assert!(!should_roll_before_append(writer.offset(), encoded.bytes.len() as u64,
            effective_block_slab_target_bytes()), "fixture must exercise a real warm no-roll append");
    }

    #[test]
    fn maintenance_active_api_does_not_walk_sealed_catalog_history() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let left = append_deferred(&store, 7, b"left active");
        let right = append_deferred(&store, 42, b"right active");
        let (before, legacy) = {
            let mut inner = store.inner.lock().unwrap();
            for id in 100_000..108_192 {
                set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Sealed);
            }
            (inner.slabs.enumeration_counters(), inner.block_slab_id)
        };
        let active = store.active_block_slab_ids();
        let after = store.inner.lock().unwrap().slabs.enumeration_counters();
        assert_eq!(active, BTreeSet::from([legacy, left.block_slab_id(), right.block_slab_id()]));
        assert_eq!((after.0 - before.0, after.1 - before.1), (2, 0));
    }

    #[test]
    fn parked_accounting_blocks_both_owners_but_not_catalog_or_record_access() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::with_options(root.path(), BlockStoreOptions {
            compression_enabled: false, ..BlockStoreOptions::default()
        });
        let a = append_deferred(&store, 7, b"seed");
        let b = append_deferred(&store, 42, b"seed");
        assert_warm_append_fits(&store, 7, b"accounting wait");
        assert_warm_append_fits(&store, 42, b"accounting wait");
        let outcome = std::thread::scope(|scope| {
            let (attempt_tx, attempt_rx) = mpsc::channel();
            let held = store.write_accounting.park_for_test(attempt_tx);
            let (done_tx, done_rx) = mpsc::channel();
            let source = &store;
            let first_tx = done_tx.clone();
            let first = scope.spawn(move || first_tx.send(append_deferred(source, 7, b"accounting wait")).unwrap());
            let second = scope.spawn(move || done_tx.send(append_deferred(source, 42, b"accounting wait")).unwrap());
            let attempt_a = attempt_rx.recv_timeout(Duration::from_secs(5));
            let attempt_b = attempt_rx.recv_timeout(Duration::from_secs(5));
            let (probe_tx, probe_rx) = mpsc::channel();
            let probe = scope.spawn(move || {
                let mut catalog = source.inner.lock().unwrap();
                let mut left = catalog.slabs.get_mut(&a.block_slab_id()).unwrap();
                let left_bytes = left.logical_bytes;
                left.updated_unix_ms = Some(now_unix_ms());
                drop(left);
                let right_bytes = catalog.slabs.get(&b.block_slab_id()).unwrap().logical_bytes;
                probe_tx.send((left_bytes, right_bytes)).unwrap();
            });
            let accessible = probe_rx.recv_timeout(Duration::from_secs(5));
            let waiting = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            drop(held);
            first.join().unwrap(); second.join().unwrap(); probe.join().unwrap();
            let addresses = [done_rx.recv().unwrap(), done_rx.recv().unwrap()];
            (attempt_a, attempt_b, accessible, waiting, addresses)
        });
        outcome.0.unwrap(); outcome.1.unwrap();
        let expected_logical_bytes = (b"seed".len() + b"accounting wait".len()) as u64;
        assert_eq!(outcome.2.unwrap(), (expected_logical_bytes, expected_logical_bytes));
        assert!(outcome.3, "both real appends wait on shared accounting until explicit release");
        assert_eq!(store.stats().writes, 4);
        store.sync_durable().unwrap();
        for address in &outcome.4 { assert_eq!(store.read(address).unwrap(), b"accounting wait"); }
        drop(store);
        let reopened = BlockStore::new(root.path());
        for address in &outcome.4 { assert_eq!(reopened.read(address).unwrap(), b"accounting wait"); }
    }

    #[test]
    fn canonical_owned_append_finishes_with_catalog_parked_and_publishes_exact_stats() {
        use std::sync::{atomic::Ordering, mpsc};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::with_options(root.path(), BlockStoreOptions {
            compression_enabled: false, ..BlockStoreOptions::default()
        });
        let first = append_deferred(&store, 7, b"first canonical bytes");
        store.sync_durable().unwrap();
        assert_warm_append_fits(&store, 7, b"second canonical bytes");
        let before = store.stats();
        let outcome = std::thread::scope(|scope| {
            let held = store.inner.lock().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (control_tx, control_rx) = mpsc::channel();
            *store.inner.next_lock_attempt.lock().unwrap() = Some(entered_tx);
            let store_ref = &store;
            let control = scope.spawn(move || control_tx.send(store_ref.stats()).unwrap());
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let attempts = store.inner.lock_attempts.load(Ordering::Relaxed);
            let index_attempts = held.slabs.active_index_lock_attempts();
            let (tx, rx) = mpsc::channel();
            let append = scope.spawn(move || {
                tx.send(store_ref.append_block_of_object_for_shard(7,
                    b"second canonical bytes", Some(17), Some(3), 4)).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(5));
            let catalog_attempts = store.inner.lock_attempts.load(Ordering::Relaxed) - attempts;
            let descriptor = held.slabs.get(&first.block_slab_id()).unwrap().clone();
            let mut totals = BlockStoreStats::default();
            store.write_accounting.merge_into(&mut totals);
            let still_parked = matches!(control_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            let index_delta = held.slabs.active_index_lock_attempts() - index_attempts;
            drop(held);
            append.join().unwrap();
            control.join().unwrap();
            (entered, result, catalog_attempts, descriptor, totals, still_parked, index_delta)
        });
        outcome.0.unwrap();
        let second = outcome.1.unwrap().unwrap();
        assert_eq!(outcome.2, 0, "measured catalog-lock attempts in the actual append actor");
        assert!(outcome.5, "real catalog consumer must await explicit release");
        assert_eq!(outcome.6, 0, "actual warm append takes no active-index lock");
        assert_eq!(outcome.3.physical_bytes, second.offset() + second.length());
        assert_eq!(outcome.3.logical_bytes, (b"first canonical bytes".len() + b"second canonical bytes".len()) as u64);
        assert_eq!(outcome.4.writes, before.writes + 1);
        assert_eq!(outcome.4.bytes_written, before.bytes_written + second.length());
        assert_eq!(store.stats().bytes_written, outcome.4.bytes_written);
        store.sync_durable().unwrap();
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"first canonical bytes");
        assert_eq!(reopened.read(&second).unwrap(), b"second canonical bytes");
    }

    #[test]
    fn active_physical_lease_refuses_same_id_import_and_gc_during_append() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = append_deferred(&store, 7, b"leased first");
        let peer = append_deferred(&store, 42, b"peer first");
        assert_warm_append_fits(&store, 7, b"leased second");
        let writer = store.owned_directory.existing(7).unwrap().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        writer.lock().unwrap().after_complete_write = Some((entered_tx, release_rx));
        let outcome = std::thread::scope(|scope| {
            let store_ref = &store;
            let worker = scope.spawn(move || store_ref.append_block_of_object_for_shard(7,
                b"leased second", None, None, 1));
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let bytes = fs::read(slab_path(root.path(), first.block_slab_id()));
            let import = bytes.as_ref().map(|bytes| store.install_slab_with_format(
                first.block_slab_id(), bytes, Some(Some(7)), None, false));
            let gc = store.gc_slabs_before_with_live_refs(u64::MAX, []);
            let peer_progress = store.append_block_of_object_for_shard(42, b"peer progresses", None, None, 1);
            let released = release_tx.send(());
            let result = worker.join().unwrap();
            (entered, import.map_err(|error| error.kind()), gc, peer_progress, released, result)
        });
        outcome.0.unwrap();
        assert!(matches!(outcome.1.unwrap(), Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::AlreadyExists));
        assert!(outcome.2.unwrap().retained_current_block_slab_ids.contains(&first.block_slab_id()));
        let peer_next = outcome.3.unwrap();
        outcome.4.unwrap();
        let second = outcome.5.unwrap();
        store.sync_durable().unwrap();
        drop(writer);
        drop(store);
        let reopened = BlockStore::new(root.path());
        for (entry, expected) in [(&first, b"leased first".as_slice()),
            (&second, b"leased second".as_slice()), (&peer, b"peer first".as_slice()),
            (&peer_next, b"peer progresses".as_slice())] {
            assert_eq!(reopened.read(entry).unwrap(), expected);
        }
    }

    #[test]
    fn failed_roll_retry_retires_exact_outgoing_lease_without_catalog() {
        use std::sync::{atomic::Ordering, mpsc};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = append_deferred(&store, 7, b"before uncertain cursor");
        store.sync_durable().unwrap();
        FAIL_CURSOR_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert!(store.roll_slab_for_shard(7).is_err());
        let active = store.active_block_slab_ids();
        assert!(active.contains(&first.block_slab_id()));
        let outcome = std::thread::scope(|scope| {
            let held = store.inner.lock().unwrap();
            let attempts = store.inner.lock_attempts.load(Ordering::Relaxed);
            let (tx, rx) = mpsc::channel();
            let store_ref = &store;
            let worker = scope.spawn(move || tx.send(store_ref.append_block_of_object_for_shard(
                7, b"retry after cursor directory sync", None, None, 1)).unwrap());
            let result = rx.recv_timeout(Duration::from_secs(5));
            let retired = !held.slabs.protected(first.block_slab_id());
            let state = held.slabs.get(&first.block_slab_id()).unwrap().state;
            let delta = store.inner.lock_attempts.load(Ordering::Relaxed) - attempts;
            drop(held);
            worker.join().unwrap();
            (result, retired, state, delta)
        });
        let second = outcome.0.unwrap().unwrap();
        assert!(outcome.1);
        assert_eq!(outcome.2, BlockStoreSlabState::Sealed);
        assert_eq!(outcome.3, 0);
        assert_ne!(first.block_slab_id(), second.block_slab_id());
        store.sync_durable().unwrap();
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"before uncertain cursor");
        assert_eq!(reopened.read(&second).unwrap(), b"retry after cursor directory sync");
    }

    #[test]
    fn failed_create_keeps_its_provisional_lease_until_catalog_finalization() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = append_deferred(&store, 7, b"original before failed create");
        store.sync_durable().unwrap();
        let blocked = cursor_path(root.path(), 7).with_extension("tmp");
        fs::create_dir(&blocked).unwrap();
        let expected_id = {
            let inner = store.inner.lock().unwrap();
            inner.owned_highest_id.max(inner.block_slab_id)
                .max(inner.slabs.last_key_value().map_or(0, |(id, _)| *id)) + 1
        };
        let writer = store.owned_directory.existing(7).unwrap().unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        writer.lock().unwrap().after_roll_io = Some((entered_tx, release_rx));
        let outcome = std::thread::scope(|scope| {
            let store_ref = &store;
            let worker = scope.spawn(move || store_ref.roll_slab_for_shard(7));
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let active = store.active_block_slab_ids();
            let gc = store.gc_slabs_before_with_live_refs(u64::MAX, []);
            let released = release_tx.send(());
            let result = worker.join().unwrap();
            (entered, active, gc, released, result)
        });
        outcome.0.unwrap();
        assert!(outcome.1.contains(&expected_id));
        assert!(outcome.2.unwrap().retained_current_block_slab_ids.contains(&expected_id));
        outcome.3.unwrap();
        assert!(outcome.4.is_err());
        assert!(!store.active_block_slab_ids().contains(&expected_id));
        assert_eq!(store.inner.lock().unwrap().slabs.get(&expected_id).unwrap().state, BlockStoreSlabState::Sealed);
        fs::remove_dir(blocked).unwrap();
        let retry = store.roll_slab_for_shard(7).unwrap();
        assert!(retry.new_block_slab_id > expected_id);
        let second = append_deferred(&store, 7, b"after failed create");
        store.sync_durable().unwrap();
        drop(writer);
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"original before failed create");
        assert_eq!(reopened.read(&second).unwrap(), b"after failed create");
    }

    #[test]
    fn read_pin_and_current_lease_join_gc_and_same_id_install() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let old = append_deferred(&store, 7, b"pinned outgoing");
        store.roll_slab_for_shard(7).unwrap();
        let current = append_deferred(&store, 7, b"current survivor");
        store.sync_durable().unwrap();
        let mut retained = BlockStoreReadSnapshot::new(&store);
        retained.pin(old.block_slab_id());
        let bytes = fs::read(slab_path(root.path(), old.block_slab_id())).unwrap();
        assert!(matches!(store.install_slab_with_format(old.block_slab_id(), &bytes,
            Some(Some(7)), None, false), Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::WouldBlock));
        let report = store.gc_slabs_before_with_live_refs(u64::MAX, []).unwrap();
        assert!(!report.removed_block_slab_ids.contains(&old.block_slab_id()));
        assert_eq!(store.read(&old).unwrap(), b"pinned outgoing");
        drop(retained);
        let report = store.gc_slabs_before_with_live_refs(u64::MAX, []).unwrap();
        assert!(report.removed_block_slab_ids.contains(&old.block_slab_id()));
        assert!(!report.removed_block_slab_ids.contains(&current.block_slab_id()));
        drop(store);
        assert_eq!(BlockStore::new(root.path()).read(&current).unwrap(), b"current survivor");
    }

    #[test]
    fn owned_and_legacy_accounting_share_exact_totals_and_window() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::with_options(root.path(), BlockStoreOptions {
            compression_enabled: false, ..BlockStoreOptions::default()
        });
        let mut entries = vec![store.append(b"a").unwrap()];
        entries.extend(store.append_batch_with_block_metadata(vec![(b"bb", None, None, 1),
            (b"ccc", None, None, 2)]).unwrap());
        entries.push(store.append_block_of_object_for_shard(7, b"dddd", None, None, 3).unwrap());
        entries.extend(store.append_batch_with_block_metadata_for_shard(7, vec![
            (b"eeeee", None, None, 4), (b"ffffff", None, None, 5)]).unwrap());
        let stats = store.stats();
        assert_eq!(stats.writes, 6);
        assert_eq!(stats.logical_bytes_written, 21);
        assert_eq!(stats.bytes_written, entries.iter().map(|entry| entry.length()).sum::<u64>());
        assert_eq!((stats.compressed_records_written, stats.compression_bytes_saved), (0, 0));
        store.mark_write_window_at_compaction(stats.bytes_written);
        let last = store.append_block_of_object_for_shard(7, b"window", None, None, 6).unwrap();
        let after = store.stats();
        assert_eq!(after.bytes_written_at_last_compaction, stats.bytes_written);
        assert_eq!(after.bytes_written, stats.bytes_written + last.length());
        store.sync_durable().unwrap();
        drop(store);
        let reopened = BlockStore::new(root.path());
        for (entry, expected) in entries.iter().zip([b"a".as_slice(), b"bb", b"ccc", b"dddd", b"eeeee", b"ffffff"]) {
            assert_eq!(reopened.read(entry).unwrap(), expected);
        }
        assert_eq!(reopened.read(&last).unwrap(), b"window");
    }

    #[test]
    fn warm_prepare_completes_before_the_catalog_is_explicitly_released() {
        use std::sync::{atomic::Ordering, mpsc};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let address = append_deferred(&store, 7, b"warm owner payload");
        store.sync_durable().unwrap();
        let cursor = fs::read(cursor_path(root.path(), 7)).unwrap();
        let outcome = std::thread::scope(|scope| {
            let held = store.inner.lock().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            let (control_tx, control_rx) = mpsc::channel();
            *store.inner.next_lock_attempt.lock().unwrap() = Some(entered_tx);
            let store_ref = &store;
            let control = scope.spawn(move || {
                let _ = store_ref.stats();
                control_tx.send(()).unwrap();
            });
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let before = store.inner.lock_attempts.load(Ordering::Relaxed);
            let reads = store.owned_directory.reads.load(Ordering::Relaxed);
            let writes = store.owned_directory.writes.load(Ordering::Relaxed);
            let writer_locks = store.owned_directory.writer_locks.load(Ordering::Relaxed);
            let (tx, rx) = mpsc::channel();
            let warm = scope.spawn(move || {
                tx.send(store_ref.prepare_next_slab_for_shard(7, u64::MAX)).unwrap();
            });
            let warm_result = rx.recv_timeout(Duration::from_secs(5));
            let delta = (
                store.inner.lock_attempts.load(Ordering::Relaxed) - before,
                store.owned_directory.reads.load(Ordering::Relaxed) - reads,
                store.owned_directory.writes.load(Ordering::Relaxed) - writes,
                store.owned_directory.writer_locks.load(Ordering::Relaxed) - writer_locks,
            );
            let control_still_waiting = matches!(control_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            // Always release before inspecting failures, so a failed control cannot strand the scope.
            drop(held);
            warm.join().unwrap();
            control.join().unwrap();
            (entered, warm_result, delta, control_still_waiting, control_rx.recv())
        });
        outcome.0.unwrap();
        assert!(outcome.1.unwrap().unwrap().is_none());
        assert_eq!(outcome.2, (0, 1, 0, 1), "actual catalog/read-directory/write-directory/writer attempts");
        assert!(outcome.3, "the real catalog consumer must await explicit release");
        outcome.4.unwrap();
        assert_eq!(fs::read(cursor_path(root.path(), 7)).unwrap(), cursor);
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&address).unwrap(), b"warm owner payload");
        assert!(reopened.prepare_next_slab_for_shard(7, u64::MAX).unwrap().is_none());
    }

    #[test]
    fn productive_prepare_rolls_while_an_unrelated_writer_is_parked() {
        use std::sync::{atomic::Ordering, mpsc};
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = append_deferred(&store, 7, b"target owner before roll");
        let other = append_deferred(&store, 42, b"peer stays pending");
        let peer_cursor = fs::read(cursor_path(root.path(), 42)).unwrap();
        let peer = store.owned_directory.existing(42).unwrap().unwrap();
        let completed = std::thread::scope(|scope| {
            let held = peer.lock().unwrap();
            assert!(held.current.as_ref().unwrap().dirty);
            let (tx, rx) = mpsc::channel();
            let before = store.inner.lock_attempts.load(Ordering::Relaxed);
            let reads = store.owned_directory.reads.load(Ordering::Relaxed);
            let store_ref = &store;
            let worker = scope.spawn(move || {
                tx.send(store_ref.prepare_next_slab_for_shard(7, 1)).unwrap();
            });
            let result = rx.recv_timeout(Duration::from_secs(5));
            let attempts = store.inner.lock_attempts.load(Ordering::Relaxed) - before;
            let lookup_reads = store.owned_directory.reads.load(Ordering::Relaxed) - reads;
            let remains_dirty = held.current.as_ref().unwrap().dirty;
            drop(held);
            worker.join().unwrap();
            (result, attempts, lookup_reads, remains_dirty)
        });
        let rolled = completed.0.unwrap().unwrap().expect("target must cause a real roll");
        assert_eq!(rolled.previous_block_slab_id, first.block_slab_id());
        assert_ne!(rolled.new_block_slab_id, first.block_slab_id());
        assert_ne!(rolled.new_block_slab_id, other.block_slab_id());
        assert_eq!((completed.1, completed.2), (2, 1), "only allocator and completion take catalog");
        assert!(completed.3);
        assert_eq!(fs::read(cursor_path(root.path(), 42)).unwrap(), peer_cursor);
        let next = append_deferred(&store, 7, b"target owner after roll");
        assert_eq!(next.block_slab_id(), rolled.new_block_slab_id);
        store.sync_durable().unwrap();
        drop(peer);
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"target owner before roll");
        assert_eq!(reopened.read(&next).unwrap(), b"target owner after roll");
        assert_eq!(reopened.read(&other).unwrap(), b"peer stays pending");
        assert!(reopened.active_block_slab_ids().contains(&rolled.new_block_slab_id));
        assert!(reopened.active_block_slab_ids().contains(&other.block_slab_id()));
    }

    #[test]
    fn concurrent_cold_lookup_has_one_authoritative_writer_and_options() {
        use std::sync::Barrier;
        let root = tempfile::tempdir().unwrap();
        let options = BlockStoreOptions {
            compression_enabled: false, compression_min_bytes: 117, compression_level: 5,
        };
        let store = BlockStore::with_options(root.path(), options);
        let start = Barrier::new(8);
        let writers = std::thread::scope(|scope| {
            let workers = (0..8).map(|_| scope.spawn(|| {
                start.wait();
                store.owned_writer(7).unwrap()
            })).collect::<Vec<_>>();
            workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>()
        });
        for (writer, observed) in &writers {
            assert!(Arc::ptr_eq(writer, &writers[0].0));
            assert_eq!(*observed, options);
            assert!(writer.lock().unwrap().id().is_none());
        }
        assert_eq!(store.owned_directory.snapshot().unwrap().len(), 1);
        let alias = store.clone();
        assert!(Arc::ptr_eq(&store.owned_directory, &alias.owned_directory));
        assert!(Arc::ptr_eq(&store.owned_directory, &store.inner.lock().unwrap().owned_directory));
        let first = store.prepare_next_slab_for_shard(7, u64::MAX).unwrap().unwrap();
        assert!(alias.prepare_next_slab_for_shard(7, u64::MAX).unwrap().is_none());
        assert_eq!(writers[0].0.lock().unwrap().id(), Some(first.new_block_slab_id));
    }

    #[test]
    fn concurrent_cold_prepare_publishes_one_initial_slab() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let start = std::sync::Barrier::new(8);
        let prepared = std::thread::scope(|scope| {
            let workers = (0..8).map(|_| scope.spawn(|| {
                start.wait();
                store.prepare_next_slab_for_shard(7, u64::MAX).unwrap()
            })).collect::<Vec<_>>();
            workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>()
        });
        let initial = prepared.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(initial.len(), 1, "cold contenders must serialize on one authoritative owner");
        assert_eq!(store.owned_directory.snapshot().unwrap().len(), 1);
        let address = append_deferred(&store, 7, b"one initial slab exact bytes");
        assert_eq!(address.block_slab_id(), initial[0].new_block_slab_id);
        store.sync_durable().unwrap();
        drop(store);
        assert_eq!(BlockStore::new(root.path()).read(&address).unwrap(), b"one initial slab exact bytes");
    }

    #[test]
    fn waiting_for_a_writer_does_not_retain_the_directory_guard() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        store.prepare_next_slab_for_shard(7, u64::MAX).unwrap().unwrap();
        let target = store.owned_directory.existing(7).unwrap().unwrap();
        let results = std::thread::scope(|scope| {
            let held = target.lock().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            *store.owned_directory.next_writer_lock.lock().unwrap() = Some(entered_tx);
            let (warm_tx, warm_rx) = mpsc::channel();
            let store_ref = &store;
            let warm = scope.spawn(move || {
                warm_tx.send(store_ref.prepare_next_slab_for_shard(7, u64::MAX)).unwrap();
            });
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let (cold_tx, cold_rx) = mpsc::channel();
            let cold = scope.spawn(move || {
                cold_tx.send(store_ref.owned_writer(42)).unwrap();
            });
            let inserted = cold_rx.recv_timeout(Duration::from_secs(5));
            let waiting = matches!(warm_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            drop(held);
            warm.join().unwrap();
            cold.join().unwrap();
            (entered, inserted, waiting, warm_rx.recv())
        });
        results.0.unwrap();
        assert!(results.1.unwrap().unwrap().0.lock().unwrap().id().is_none());
        assert!(results.2);
        assert!(results.3.unwrap().unwrap().is_none());
        assert_eq!(store.owned_directory.snapshot().unwrap().len(), 2);
    }

    #[test]
    fn full_sync_snapshots_release_directory_before_waiting_for_a_writer() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let address = append_deferred(&store, 7, b"full sync selected bytes");
        let target = store.owned_directory.existing(7).unwrap().unwrap();
        let results = std::thread::scope(|scope| {
            let held = target.lock().unwrap();
            let (entered_tx, entered_rx) = mpsc::channel();
            *store.owned_directory.next_writer_lock.lock().unwrap() = Some(entered_tx);
            let (done_tx, done_rx) = mpsc::channel();
            let store_ref = &store;
            let sync = scope.spawn(move || done_tx.send(store_ref.sync_durable()).unwrap());
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            let (cold_tx, cold_rx) = mpsc::channel();
            let cold = scope.spawn(move || cold_tx.send(store_ref.owned_writer(42)).unwrap());
            let inserted = cold_rx.recv_timeout(Duration::from_secs(5));
            let waiting = matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
            drop(held);
            sync.join().unwrap();
            cold.join().unwrap();
            (entered, inserted, waiting, done_rx.recv())
        });
        results.0.unwrap();
        results.1.unwrap().unwrap();
        assert!(results.2);
        results.3.unwrap().unwrap();
        drop(target);
        drop(store);
        assert_eq!(BlockStore::new(root.path()).read(&address).unwrap(), b"full sync selected bytes");
    }

    #[test]
    fn a_recovery_error_between_cold_read_and_insert_refuses_admission() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *store.owned_directory.after_cold_miss.lock().unwrap() = Some((entered_tx, release_rx));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| store.owned_writer(7));
            let entered = entered_rx.recv_timeout(Duration::from_secs(5));
            store.owned_directory.write().recovery_error = Some("new root error".into());
            release_tx.send(()).unwrap();
            let result = worker.join().unwrap();
            entered.unwrap();
            assert!(matches!(result, Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::InvalidData));
        });
        assert!(store.owned_directory.read().writers.is_empty());
        assert!(store.sync_durable().is_err());
    }

    #[test]
    fn warm_lookup_does_not_cache_past_an_admission_error_or_a_new_open() {
        let root = tempfile::tempdir().unwrap();
        let options = BlockStoreOptions { compression_enabled: false, ..BlockStoreOptions::default() };
        let store = BlockStore::with_options(root.path(), options);
        let address = append_deferred(&store, 7, b"exact prior durable bytes");
        store.sync_durable().unwrap();
        let cached = store.owned_writer(7).unwrap().0;
        store.owned_directory.write().recovery_error = Some("root became unhealthy".into());
        assert!(store.owned_writer(7).is_err());
        assert!(store.prepare_next_slab_for_shard(7, u64::MAX).is_err());
        assert!(store.append_block_of_object_for_shard(7, b"must not append", None, None, 0).is_err());
        assert!(store.sync_durable().is_err());
        assert!(Arc::ptr_eq(&cached, &store.owned_directory.read().writers[&7]));
        let cursor = cursor_path(root.path(), 7);
        let original = fs::read(&cursor).unwrap();
        fs::write(&cursor, b"bad cursor on the same root").unwrap();
        let failed = BlockStore::with_options(root.path(), options);
        assert!(failed.owned_writer(7).is_err());
        assert!(failed.prepare_next_slab_for_shard(7, u64::MAX).is_err());
        assert!(failed.owned_directory.read().writers.is_empty());
        assert!(!Arc::ptr_eq(&failed.owned_directory, &store.owned_directory));
        drop(failed);
        fs::write(&cursor, original).unwrap();
        let new_options = BlockStoreOptions { compression_min_bytes: 999, compression_level: 11, ..options };
        let reopened = BlockStore::with_options(root.path(), new_options);
        let (recovered, observed) = reopened.owned_writer(7).unwrap();
        assert_eq!(observed, new_options);
        assert!(!Arc::ptr_eq(&cached, &recovered));
        assert!(store.owned_writer(7).is_err());
        assert_eq!(reopened.read(&address).unwrap(), b"exact prior durable bytes");
        assert!(reopened.prepare_next_slab_for_shard(7, u64::MAX).unwrap().is_none());
    }

    #[test]
    fn relocation_sync_leaves_unrelated_dirty_writer_locked_and_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let address = append_deferred(&store, 0, b"selected");
        let peer_address = append_deferred(&store, 42, b"unacknowledged peer");
        let peer_cursor = fs::read(cursor_path(root.path(), 42)).unwrap();
        let peer = store.owned_directory.existing(42).unwrap().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let completed = std::thread::scope(|scope| {
            let blocked = peer.lock().unwrap();
            assert!(blocked.current.as_ref().unwrap().dirty);
            let store_ref = &store;
            let address_ref = &address;
            let worker = scope.spawn(move || {
                send.send(store_ref.sync_relocation_destinations(0, [address_ref]).is_ok()).unwrap();
            });
            let result = receive.recv_timeout(std::time::Duration::from_secs(5));
            drop(blocked);
            worker.join().unwrap();
            result
        });
        assert!(completed.unwrap());
        assert!(peer.lock().unwrap().current.as_ref().unwrap().dirty);
        assert_eq!(fs::read(cursor_path(root.path(), 42)).unwrap(), peer_cursor);
        assert_eq!(store.read(&peer_address).unwrap(), b"unacknowledged peer");
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&address).unwrap(), b"selected");
    }

    #[test]
    fn relocation_sync_rejects_foreign_and_legacy_destinations_before_syncing() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let selected = append_deferred(&store, 0, b"selected");
        let peer = append_deferred(&store, 42, b"peer");
        let legacy = store.append(b"legacy").unwrap();
        let before = fs::read(cursor_path(root.path(), 0)).unwrap();
        for invalid in [&peer, &legacy] {
            assert!(store.sync_relocation_destinations(0, [&selected, invalid]).is_err());
            assert_eq!(fs::read(cursor_path(root.path(), 0)).unwrap(), before);
        }
        let writer = store.owned_directory.existing(0).unwrap().unwrap();
        assert!(writer.lock().unwrap().current.as_ref().unwrap().dirty);
    }

    #[test]
    fn relocation_sync_cursor_failure_is_retryable() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let selected = append_deferred(&store, 0, b"retry selected");
        let before = fs::read(cursor_path(root.path(), 0)).unwrap();
        let blocked = cursor_path(root.path(), 0).with_extension("tmp");
        fs::create_dir(&blocked).unwrap();
        assert!(store.sync_relocation_destinations(0, [&selected]).is_err());
        assert_eq!(fs::read(cursor_path(root.path(), 0)).unwrap(), before);
        fs::remove_dir(&blocked).unwrap();
        store.sync_relocation_destinations(0, [&selected]).unwrap();
        drop(store);
        assert_eq!(BlockStore::new(root.path()).read(&selected).unwrap(), b"retry selected");
    }

    #[test]
    fn empty_relocation_sync_does_not_create_a_writer() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        store.sync_relocation_destinations(42, std::iter::empty()).unwrap();
        assert!(store.owned_directory.snapshot().unwrap().is_empty());
    }

    #[test]
    fn selected_relocation_barrier_does_not_commit_unrelated_legacy_data() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let legacy = store.append(b"legacy pending data").unwrap();
        store.inner.lock().unwrap().relaxed_dirty = true;
        let selected = append_deferred(&store, 7, b"selected destination");
        let before_cursor = fs::read(cursor_path(root.path(), 7)).unwrap();
        let legacy_path = slab_path(root.path(), legacy.block_slab_id());
        let held = root.path().join("legacy-held");
        fs::rename(&legacy_path, &held).unwrap();

        store.sync_relocation_destinations(7, [&selected]).unwrap();
        assert!(store.inner.lock().unwrap().relaxed_dirty);
        assert_ne!(fs::read(cursor_path(root.path(), 7)).unwrap(), before_cursor);
        let writer = store.owned_directory.existing(7).unwrap().unwrap();
        assert!(!writer.lock().unwrap().current.as_ref().unwrap().dirty);
        assert!(store.sync_durable().is_err());
        assert!(store.inner.lock().unwrap().relaxed_dirty);

        fs::rename(&held, &legacy_path).unwrap();
        store.sync_durable().unwrap();
        assert!(!store.inner.lock().unwrap().relaxed_dirty);
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&selected).unwrap(), b"selected destination");
        assert_eq!(reopened.read(&legacy).unwrap(), b"legacy pending data");
    }

    #[test]
    fn empty_relocation_barrier_leaves_pending_legacy_data_untouched() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let legacy = store.append(b"legacy pending data").unwrap();
        store.inner.lock().unwrap().relaxed_dirty = true;
        let legacy_path = slab_path(root.path(), legacy.block_slab_id());
        let held = root.path().join("legacy-held");
        fs::rename(&legacy_path, &held).unwrap();

        store.sync_relocation_destinations(99, std::iter::empty()).unwrap();
        {
            let inner = store.inner.lock().unwrap();
            assert!(inner.relaxed_dirty);
            assert!(store.owned_directory.snapshot().unwrap().is_empty());
        }
        assert!(store.sync_durable().is_err());
        fs::rename(&held, &legacy_path).unwrap();
        store.sync_durable().unwrap();
        drop(store);
        assert_eq!(BlockStore::new(root.path()).read(&legacy).unwrap(), b"legacy pending data");
    }

    #[test]
    fn independent_owner_cursor_commits_survive_reopen() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let mut first = OwnedWriter::default();
        let mut second = OwnedWriter::default();
        first.roll(root, 0, 1).unwrap();
        second.roll(root, 42, 2).unwrap();
        first.roll(root, 0, 3).unwrap();
        let store = BlockStore::new(root);
        let slabs = store.slab_descriptors();
        let find = |id| slabs.iter().find(|slab| slab.block_slab_id == id).unwrap();

        assert!(OwnedWriter::recover(root, 0, find(1)).is_err());
        assert_eq!(
            OwnedWriter::recover(root, 0, find(3))
                .unwrap()
                .current
                .unwrap()
                .id,
            3
        );
        assert_eq!(
            OwnedWriter::recover(root, 42, find(2))
                .unwrap()
                .current
                .unwrap()
                .id,
            2
        );
    }

    #[test]
    fn concurrent_writers_sync_roll_reopen_and_keep_each_active_slab() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let (first, second) = std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                store
                    .append_block_of_object_for_shard(0, b"first", None, None, 0)
                    .unwrap()
            });
            let second = scope.spawn(|| {
                store
                    .append_block_of_object_for_shard(42, b"second", None, None, 0)
                    .unwrap()
            });
            (first.join().unwrap(), second.join().unwrap())
        });
        assert_ne!(first.block_slab_id(), second.block_slab_id());
        let rolled = store.roll_slab_for_shard(0).unwrap();
        assert_eq!(rolled.previous_block_slab_id, first.block_slab_id());
        store.sync_durable().unwrap();
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
        let active = reopened.active_block_slab_ids();
        assert!(active.contains(&rolled.new_block_slab_id));
        assert!(active.contains(&second.block_slab_id()));
        assert!(!active.contains(&first.block_slab_id()));
        let retention = reopened
            .gc_slabs_before_with_live_refs(u64::MAX, [first.block_slab_id()])
            .unwrap();
        assert!(retention
            .retained_current_block_slab_ids
            .contains(&rolled.new_block_slab_id));
        assert!(retention
            .retained_current_block_slab_ids
            .contains(&second.block_slab_id()));
        let same = reopened
            .append_block_of_object_for_shard(42, b"after reopen", None, None, 0)
            .unwrap();
        assert_eq!(same.block_slab_id(), second.block_slab_id());
    }

    #[test]
    fn damaged_owned_header_cannot_reopen_as_a_legacy_writer() {
        for keep_manifest in [false, true] {
            for length in 0..=OWNED_SLAB_HEADER_LEN {
                let root = tempfile::tempdir().unwrap();
                let root = root.path();
                let store = BlockStore::new(root);
                let id = store.roll_slab_for_shard(7).unwrap().new_block_slab_id;
                store
                    .inner
                    .lock()
                    .unwrap()
                    .persist_slab_manifest_counted()
                    .unwrap();
                drop(store);
                if !keep_manifest {
                    fs::remove_file(slab_manifest_path(root)).unwrap();
                }
                let path = slab_path(root, id);
                let mut header = fs::read(&path).unwrap();
                header.truncate(length);
                if length == OWNED_SLAB_HEADER_LEN {
                    header[0] ^= 1;
                }
                fs::write(&path, &header).unwrap();
                let reopened = BlockStore::new(root);
                assert!(reopened.append(b"legacy attempt").is_err());
                assert!(reopened
                    .append_block_of_object_for_shard(7, b"owner attempt", None, None, 0,)
                    .is_err());
                assert!(reopened
                    .gc_slabs_before_with_live_refs(u64::MAX, [])
                    .is_err());
                assert_eq!(fs::read(path).unwrap(), header);
            }
        }
    }

    #[test]
    fn owned_reopen_preserves_a_damaged_record_and_every_later_record() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let append = |bytes: &[u8]| {
            store
                .append_block_of_object_for_shard(7, bytes, None, None, 0)
                .unwrap()
        };
        let first = append(b"first");
        let middle = append(b"middle");
        let last = append(b"last");
        store.sync_durable().unwrap();
        drop(store);
        assert_eq!(first.block_slab_id(), last.block_slab_id());
        let path = slab_path(root, middle.block_slab_id());
        let mut damaged = fs::read(&path).unwrap();
        damaged[(middle.offset() + middle.length() - 1) as usize] ^= 1;
        fs::write(&path, &damaged).unwrap();
        let reopened = BlockStore::new(root);
        assert!(reopened.sync_durable().is_err());
        assert!(reopened.read(&middle).is_err());
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&last).unwrap(), b"last");
        assert_eq!(fs::read(path).unwrap(), damaged);
    }

    #[test]
    fn first_owned_append_retries_directory_creation() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        fs::write(&parent, b"temporarily blocked").unwrap();
        let store = BlockStore::new(parent.join("store"));
        assert!(store
            .append_block_of_object_for_shard(0, b"retry", None, None, 0)
            .is_err());
        fs::remove_file(parent).unwrap();
        let address = store
            .append_block_of_object_for_shard(0, b"retry", None, None, 0)
            .unwrap();
        assert_eq!(store.read(&address).unwrap(), b"retry");
    }

    #[test]
    fn damaged_imported_header_is_detected_after_manifest_loss() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let mut header = encode_slab_owner(17, 42);
        store
            .install_transferred_slab(17, &header, Some(42))
            .unwrap();
        drop(store);
        if slab_manifest_path(root).exists() {
            fs::remove_file(slab_manifest_path(root)).unwrap();
        }
        let path = slab_path(root, 17);
        header[0] ^= 1;
        fs::write(&path, header).unwrap();
        let reopened = BlockStore::new(root);
        assert!(reopened.append(b"legacy attempt").is_err());
        assert!(reopened
            .gc_slabs_before_with_live_refs(u64::MAX, [])
            .is_err());
        assert_eq!(fs::read(path).unwrap(), header);
    }

    #[test]
    fn lazy_checkpoint_reservation_refuses_unaddressable_ids() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let first = store.append(b"first").unwrap();
        let active = store.current_block_slab_id();
        let slabs = store.slab_ids().unwrap();
        for limit in [MAX_ADDRESSABLE_BLOCK_SLAB_ID, u64::MAX] {
            assert!(store.reserve_lazy_checkpoint_range(limit, 0).is_err());
            assert_eq!(store.current_block_slab_id(), active);
            assert_eq!(store.slab_ids().unwrap(), slabs);
        }
        let second = store.append(b"second").unwrap();
        assert_eq!(second.block_slab_id(), active);
        store.sync_durable().unwrap();
        drop(store);
        let reopened = BlockStore::new(root.path());
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
    }

    #[test]
    fn same_owner_and_id_cannot_replace_a_different_slab() {
        let root_a = tempfile::tempdir().unwrap();
        let root_b = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let source_a = BlockStore::new(root_a.path());
        let source_b = BlockStore::new(root_b.path());
        let receiver = BlockStore::new(destination.path());
        let first = source_a
            .append_block_of_object_for_shard(42, b"first", None, None, 0)
            .unwrap();
        let second = source_b
            .append_block_of_object_for_shard(42, b"other", None, None, 0)
            .unwrap();
        let id = first.block_slab_id();
        assert_eq!(id, second.block_slab_id());
        source_a.sync_durable().unwrap();
        source_b.sync_durable().unwrap();
        let (first_bytes, owner) = source_a.read_slab_for_transfer(id).unwrap();
        let (second_bytes, second_owner) = source_b.read_slab_for_transfer(id).unwrap();
        assert_eq!(owner, Some(42));
        assert_eq!(owner, second_owner);
        receiver.install_transferred_slab(id, &first_bytes, owner).unwrap();

        let mut snapshot = BlockStoreReadSnapshot::new(&receiver);
        snapshot.pin(id);
        assert!(receiver.install_transferred_slab(id, &second_bytes, owner).is_err());
        assert_eq!(receiver.read(&first).unwrap(), b"first");
        drop(snapshot);
        assert!(receiver.install_transferred_slab(id, &second_bytes, owner).is_err());
        receiver.install_transferred_slab(id, &first_bytes, owner).unwrap();
        drop(receiver);

        let reopened = BlockStore::new(destination.path());
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read_slab(id).unwrap(), first_bytes);
    }

    #[test]
    fn rebind_of_a_sealed_legacy_slab_does_not_write_a_reservation() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let legacy = store.append(b"legacy").unwrap();
        let id = legacy.block_slab_id();
        store.roll_slab().unwrap();
        let path = slab_path(root, id);
        let before = fs::read(&path).unwrap();
        assert!(store
            .install_transferred_slab(id, &encode_slab_owner(id, 42), Some(42))
            .is_err());
        assert!(!reservation_path(root, id).exists());
        assert_eq!(fs::read(path).unwrap(), before);
        drop(store);

        let reopened = BlockStore::new(root);
        assert_eq!(reopened.read(&legacy).unwrap(), b"legacy");
        reopened
            .append_block_of_object_for_shard(42, b"new", None, None, 0)
            .unwrap();
    }

    #[test]
    fn quarantined_slab_keeps_its_owner_until_physical_purge() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let first = store
            .append_block_of_object_for_shard(7, b"retire", None, None, 0)
            .unwrap();
        let old = first.block_slab_id();
        let current = store.roll_slab_for_shard(7).unwrap().new_block_slab_id;
        let removal = store
            .gc_slabs_before_with_live_refs_delayed_destroy(u64::MAX, [])
            .unwrap();
        assert!(removal.delayed_destroy_block_slab_ids.contains(&old));
        assert!(reservation_path(root, old).exists());
        drop(store);
        fs::remove_file(slab_manifest_path(root)).unwrap();

        let reopened = BlockStore::new(root);
        assert!(reopened.slab_descriptors().iter().any(|slab| {
            slab.block_slab_id == old
                && slab.owner_shard_id == Some(7)
                && slab.state == BlockStoreSlabState::DelayedDestroy
        }));
        let purge = reopened.purge_delayed_destroy_slabs_checked(0, []).unwrap();
        assert!(purge.purged_block_slab_ids.contains(&old));
        assert!(!reservation_path(root, old).exists());
        assert!(reservation_path(root, current).exists());
    }

    #[test]
    fn owner_reopen_repairs_only_the_incomplete_tail_beyond_committed_data() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let first = store
            .append_block_of_object_for_shard(0, b"committed", None, None, 0)
            .unwrap();
        store.sync_durable().unwrap();
        drop(store);
        let path = slab_path(root, first.block_slab_id());
        let committed = fs::read(&path).unwrap();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"TB\x00").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reopened = BlockStore::new(root);
        assert_eq!(fs::read(path).unwrap(), committed);
        assert_eq!(reopened.read(&first).unwrap(), b"committed");
        let next = reopened
            .append_block_of_object_for_shard(0, b"next", None, None, 1)
            .unwrap();
        assert_eq!(next.block_slab_id(), first.block_slab_id());
        assert_eq!(next.offset(), committed.len() as u64);
    }

    #[test]
    fn recovery_repairs_short_payloads_but_keeps_complete_corrupt_records() {
        for missing_bytes in [0, 1, 12] {
            let root = tempfile::tempdir().unwrap();
            let root = root.path();
            let store = BlockStore::new(root);
            let first = store
                .append_block_of_object_for_shard(0, b"committed", None, None, 0)
                .unwrap();
            store.sync_durable().unwrap();
            drop(store);
            let path = slab_path(root, first.block_slab_id());
            let committed = fs::read(&path).unwrap();
            let mut incomplete =
                encode_block_record(b"second frame", 1, None, None, BlockStoreOptions::default())
                    .unwrap()
                    .bytes;
            incomplete.truncate(incomplete.len() - missing_bytes);
            if missing_bytes == 0 {
                *incomplete.last_mut().unwrap() ^= 1;
            }
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(&incomplete).unwrap();
            file.sync_all().unwrap();
            drop(file);
            let reopened = BlockStore::new(root);
            assert_eq!(reopened.read(&first).unwrap(), b"committed");
            if missing_bytes == 0 {
                assert!(reopened.sync_durable().is_err());
                assert_eq!(fs::read(path).unwrap(), [committed, incomplete].concat());
            } else {
                assert_eq!(fs::read(path).unwrap(), committed);
                let next = reopened
                    .append_block_of_object_for_shard(0, b"after repair", None, None, 1)
                    .unwrap();
                assert_eq!(next.offset(), committed.len() as u64);
            }
        }
    }

    #[test]
    fn prepare_allocates_initial_owner_cursor_and_idle_syncs_do_no_manifest_work() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let target = 1 << 30;
        let first = store
            .prepare_next_slab_for_shard(0, target)
            .unwrap()
            .unwrap();
        let second = store
            .prepare_next_slab_for_shard(42, target)
            .unwrap()
            .unwrap();
        assert_ne!(first.new_block_slab_id, second.new_block_slab_id);
        assert!(store
            .prepare_next_slab_for_shard(0, target)
            .unwrap()
            .is_none());
        assert!(store
            .prepare_next_slab_for_shard(42, target)
            .unwrap()
            .is_none());
        let first_address = store
            .append_block_of_object_for_shard(0, b"prepared", None, None, 0)
            .unwrap();
        assert_eq!(first_address.block_slab_id(), first.new_block_slab_id);
        store.sync_durable().unwrap();
        let writes = store.stats().slab_manifest_writes;
        store.sync_durable().unwrap();
        store.sync_durable().unwrap();
        assert_eq!(store.stats().slab_manifest_writes, writes);
    }

    #[test]
    fn interrupted_reservation_names_are_never_reused_after_reopen() {
        let id = 1;
        let header = encode_slab_owner(id, 7);
        for interrupted in [&header[..], &header[..3]] {
            let root = tempfile::tempdir().unwrap();
            let temp = reservation_path(root.path(), id).with_extension("writing");
            let mut file = File::create(&temp).unwrap();
            file.write_all(interrupted).unwrap();
            file.sync_all().unwrap();
            sync_writer_directory(root.path()).unwrap();
            drop(file);

            let store = BlockStore::new(root.path());
            let owned = store.append_block_of_object_for_shard(7, b"owned", None, None, 0).unwrap();
            assert!(owned.block_slab_id() > id);
            let legacy = store.append(b"legacy").unwrap();
            assert_ne!(legacy.block_slab_id(), id);
            store.sync_durable().unwrap();
            assert_eq!(fs::read(&temp).unwrap(), interrupted);
            assert!(!reservation_path(root.path(), id).exists());
            assert!(!slab_path(root.path(), id).exists());
            drop(store);

            let reopened = BlockStore::new(root.path());
            assert_eq!(reopened.read(&owned).unwrap(), b"owned");
            assert_eq!(reopened.read(&legacy).unwrap(), b"legacy");
        }
    }

    #[test]
    fn failed_owner_roll_releases_only_resolved_reservations() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let first = store
            .append_block_of_object_for_shard(7, b"first", None, None, 0)
            .unwrap();
        store.sync_durable().unwrap();
        let before = store.active_block_slab_ids();
        let blocked = cursor_path(root, 7).with_extension("tmp");
        fs::create_dir(&blocked).unwrap();
        assert!(store.roll_slab_for_shard(7).is_err());
        assert_eq!(store.active_block_slab_ids(), before);
        fs::remove_dir(blocked).unwrap();

        FAIL_CURSOR_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert!(store.roll_slab_for_shard(7).is_err());
        let unresolved = store.active_block_slab_ids();
        assert_eq!(unresolved.len(), before.len() + 1);
        assert!(unresolved.contains(&first.block_slab_id()));
        store.sync_durable().unwrap();
        let resolved = store.active_block_slab_ids();
        assert_eq!(resolved.len(), before.len());
        assert!(!resolved.contains(&first.block_slab_id()));
        assert_eq!(store.read(&first).unwrap(), b"first");
    }

    #[test]
    fn failed_slab_publication_preserves_reservation_and_writer_cursors() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let store = BlockStore::new(root);
        let outgoing = store
            .append_block_of_object_for_shard(0, b"outgoing", None, None, 0)
            .unwrap();
        let peer = store
            .append_block_of_object_for_shard(42, b"peer", None, None, 0)
            .unwrap();
        store.sync_durable().unwrap();

        let old_cursor = fs::read(cursor_path(root, 0)).unwrap();
        let peer_cursor = fs::read(cursor_path(root, 42)).unwrap();
        let active = store.active_block_slab_ids();
        let blocked_id = {
            let inner = store.inner.lock().unwrap();
            inner.owned_highest_id
                .max(inner.block_slab_id)
                .max(inner.slabs.last_key_value().map_or(0, |(id, _)| *id))
                + 1
        };
        let colliding_temp = slab_path(root, blocked_id).with_extension("writing");
        fs::write(&colliding_temp, b"occupied").unwrap();
        assert!(matches!(
            store.roll_slab_for_shard(0),
            Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::AlreadyExists
        ));
        assert_eq!(fs::read(&colliding_temp).unwrap(), b"occupied");
        assert!(!slab_path(root, blocked_id).exists());
        assert_eq!(stored_slab_owner(root, blocked_id).unwrap(), Some(0));
        assert_eq!(fs::read(cursor_path(root, 0)).unwrap(), old_cursor);
        assert_eq!(fs::read(cursor_path(root, 42)).unwrap(), peer_cursor);
        assert_eq!(store.active_block_slab_ids(), active);
        fs::remove_file(colliding_temp).unwrap();
        drop(store);

        let reopened = BlockStore::new(root);
        assert_eq!(reopened.read(&outgoing).unwrap(), b"outgoing");
        assert_eq!(reopened.read(&peer).unwrap(), b"peer");
        let retry = reopened.roll_slab_for_shard(0).unwrap();
        assert_eq!(retry.previous_block_slab_id, outgoing.block_slab_id());
        assert!(retry.new_block_slab_id > blocked_id);
        assert_eq!(stored_slab_owner(root, blocked_id).unwrap(), Some(0));
        assert_eq!(fs::read(cursor_path(root, 42)).unwrap(), peer_cursor);
        let current = reopened
            .append_block_of_object_for_shard(0, b"current", None, None, 0)
            .unwrap();
        assert_eq!(current.block_slab_id(), retry.new_block_slab_id);
        reopened.sync_durable().unwrap();
        drop(reopened);

        let reopened = BlockStore::new(root);
        assert_eq!(reopened.read(&outgoing).unwrap(), b"outgoing");
        assert_eq!(reopened.read(&current).unwrap(), b"current");
        assert_eq!(reopened.read(&peer).unwrap(), b"peer");
        assert_eq!(fs::read(cursor_path(root, 42)).unwrap(), peer_cursor);
        let inner = reopened.inner.lock().unwrap();
        assert_eq!(inner.slabs.protected_ids(), BTreeSet::from([
            retry.new_block_slab_id, peer.block_slab_id(),
        ]));
    }

    #[test]
    fn colliding_file_prevents_roll_without_reclassifying_legacy_bytes() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let store = BlockStore::new(root);
        let current = store.roll_slab_for_shard(0).unwrap().new_block_slab_id;
        let collision = current + 1;
        let path = slab_path(root, collision);
        fs::write(&path, b"existing legacy file").unwrap();
        assert!(store.roll_slab_for_shard(0).is_err());
        assert!(!reservation_path(root, collision).exists());
        drop(store);

        let reopened = BlockStore::new(root);
        let next = reopened
            .append_block_of_object_for_shard(0, b"new payload", None, None, 0)
            .unwrap();
        assert_eq!(next.block_slab_id(), current);
        assert_eq!(fs::read(path).unwrap(), b"existing legacy file");
    }
}
