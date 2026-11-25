// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::fs::{self, File};
use std::io::{Error, ErrorKind};
use std::path::Path;

use super::record::{encode_slab_owner, slab_owner, OWNED_SLAB_HEADER_LEN};

#[cfg(test)]
thread_local! {
    static FAIL_CURSOR_DIRECTORY_SYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Debug, Default)]
pub(super) struct OwnedWriter {
    current: Option<OwnedSlab>,
    needs_directory_sync: bool,
    pending_retirement: Option<u64>,
}

#[derive(Debug)]
struct OwnedSlab {
    id: u64,
    owner: u64,
    offset: u64,
    file: File,
    dirty: bool,
    needs_truncate: bool,
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

    pub(super) fn take_retired(&mut self) -> Option<u64> {
        if self.needs_directory_sync {
            return None;
        }
        self.pending_retirement.take()
    }

    pub(super) fn roll(
        &mut self,
        root: &Path,
        owner: u64,
        id: u64,
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
        sync_writer_directory(root)?;

        let file = publish_new_header(&path, &header, true)?;
        sync_writer_directory(root)?;

        let initial_offset = OWNED_SLAB_HEADER_LEN as u64;
        publish_cursor(root, owner, id, initial_offset)?;

        // Once the pointer has been renamed, never resume writing to the outgoing
        // slab, even if the directory barrier needs another attempt.
        self.current = Some(OwnedSlab {
            id,
            owner,
            offset: initial_offset,
            file,
            dirty: false,
            needs_truncate: false,
        });
        self.needs_directory_sync = true;
        self.pending_retirement = previous;
        self.sync(root)?;
        Ok(BlockStoreRollReport {
            previous_block_slab_id,
            new_block_slab_id: id,
        })
    }

    pub(super) fn recover(
        root: &Path,
        owner: u64,
        slab: &BlockStoreSlabDescriptor,
    ) -> Result<Self, BlockStoreError> {
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
            }),
            needs_directory_sync: false,
            pending_retirement: None,
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
        Ok(())
    }
}

pub(super) fn recover_owned_writers(
    root: &Path,
    slabs: &mut BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Result<(BTreeMap<u64, Arc<Mutex<OwnedWriter>>>, BTreeSet<u64>, u64), BlockStoreError> {
    let mut writers = BTreeMap::new();
    let mut protected = BTreeSet::new();
    let mut highest = 0;
    let mut quarantined = None;
    if !root.is_dir() {
        return Ok((writers, protected, highest));
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
            if let Some(slab) = slabs.get_mut(&id) {
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
                    slab.owner_shard_id = Some(owner);
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
            let slab = slabs.get_mut(&id).ok_or_else(invalid_cursor)?;
            let writer = OwnedWriter::recover(root, owner, slab)?;
            slab.physical_bytes = writer.offset();
            slab.has_corruption = false;
            slab.first_error = None;
            slab.first_error_offset = None;
            slab.state = BlockStoreSlabState::Active;
            if writers
                .insert(owner, Arc::new(Mutex::new(writer)))
                .is_some()
                || !protected.insert(id)
            {
                return Err(invalid_cursor().into());
            }
        }
    }
    Ok((writers, protected, highest))
}

impl BlockStoreInner {
    pub(super) fn check_owned_recovery(&self) -> Result<(), BlockStoreError> {
        if let Some(error) = &self.owned_recovery_error {
            return Err(Error::new(ErrorKind::InvalidData, error.clone()).into());
        }
        Ok(())
    }

    pub(super) fn protects_writer_slab(&self, id: u64) -> bool {
        self.block_slab_id == id || self.owned_active.contains(&id)
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
        let writer = self.inner.lock().expect("block store lock poisoned")
            .owned_writers.get(&shard_id)?.clone();
        let writer = writer.lock().expect("owned slab writer lock poisoned");
        Some((writer.id()?, writer.offset()))
    }

    pub(crate) fn active_block_slab_ids(&self) -> BTreeSet<u64> {
        let inner = self.inner.lock().expect("block store lock poisoned");
        let mut active = inner.owned_active.clone();
        active.insert(inner.block_slab_id);
        active
    }

    fn owned_writer(
        &self,
        shard_id: u64,
    ) -> Result<(Arc<Mutex<OwnedWriter>>, BlockStoreOptions), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        inner.check_owned_recovery()?;
        let options = inner.options;
        let writer = inner.owned_writers.entry(shard_id).or_default().clone();
        Ok((writer, options))
    }

    fn roll_owned_writer(
        &self,
        shard_id: u64,
        writer: &mut OwnedWriter,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        writer.sync(&self.read_view.root)?;
        let previous = writer.id();
        let id = {
            let mut inner = self.inner.lock().expect("block store lock poisoned");
            if let Some(retired) = writer.take_retired() {
                inner.owned_active.remove(&retired);
            }
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
            inner.owned_active.insert(id);
            id
        };
        let result = writer.roll(&self.read_view.root, shard_id, id);
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if writer.id() == Some(id) {
            if let Some(previous) = previous {
                set_slab_state(&mut inner.slabs, previous, BlockStoreSlabState::Sealed);
            }
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Active);
            let slab = inner
                .slabs
                .get_mut(&id)
                .expect("registered active owned slab");
            slab.owner_shard_id = Some(shard_id);
            slab.physical_bytes = writer.offset();
            slab.readable_prefix_physical_bytes = writer.offset();
        } else {
            inner.owned_active.remove(&id);
        }
        if let Some(retired) = writer.take_retired() {
            inner.owned_active.remove(&retired);
        }
        result
    }

    pub(crate) fn roll_slab_for_shard(
        &self,
        shard_id: u64,
    ) -> Result<BlockStoreRollReport, BlockStoreError> {
        let (writer, _) = self.owned_writer(shard_id)?;
        let mut writer = writer.lock().expect("owned slab writer lock poisoned");
        self.roll_owned_writer(shard_id, &mut writer)
    }

    pub(crate) fn prepare_next_slab_for_shard(
        &self,
        shard_id: u64,
        slab_target_bytes: u64,
    ) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        let (writer, _) = self.owned_writer(shard_id)?;
        let mut writer = writer.lock().expect("owned slab writer lock poisoned");
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
        let mut writer = writer.lock().expect("owned slab writer lock poisoned");
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
        let mut writer = writer.lock().expect("owned slab writer lock poisoned");
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
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if let Some(retired) = writer.take_retired() {
            inner.owned_active.remove(&retired);
        }
        upsert_slab_after_append(
            &mut inner.slabs,
            address.block_slab_id(),
            writer.offset(),
            record.logical_len as u64,
            block_id,
        );
        inner.stats.writes += 1;
        inner.stats.bytes_written += address.length();
        inner.stats.logical_bytes_written += record.logical_len as u64;
        if record.compression == BlockRecordCompression::Zstd {
            inner.stats.compressed_records_written += 1;
            inner.stats.compression_bytes_saved +=
                record.logical_len.saturating_sub(record.stored_len) as u64;
        }
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

    #[test]
    fn relocation_sync_leaves_unrelated_dirty_writer_locked_and_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let store = BlockStore::new(root.path());
        let address = append_deferred(&store, 0, b"selected");
        let peer_address = append_deferred(&store, 42, b"unacknowledged peer");
        let peer_cursor = fs::read(cursor_path(root.path(), 42)).unwrap();
        let peer = store.inner.lock().unwrap().owned_writers[&42].clone();
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
        let writer = store.inner.lock().unwrap().owned_writers[&0].clone();
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
        assert!(store.inner.lock().unwrap().owned_writers.is_empty());
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
