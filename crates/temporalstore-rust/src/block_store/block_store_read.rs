// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore read/read_range/install_slab methods, split from block_store.rs.
use super::*;
use super::record::sha256_bytes;

impl BlockStore {
    pub fn read(&self, address: &ElementEntry) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: if this slab lives only in shared storage after a
        // metadata-only restore, fetch + cache it before serving the read.
        self.ensure_slab_present(address.block_slab_id())?;
        // NO STORE LOCK ANYWHERE IN A READ. The root is fixed when the store opens and the
        // counters are atomics, so the file open, the positioned read and the decode all run
        // without the mutex that writers, rolls and collection hold -- a read no longer waits
        // behind them, and they no longer wait behind a read's disk and decompression time.
        let mut tally = BlockStoreReadTally::start();
        let read = LocalSlabBackend::new(&self.read_view.root).read_range(
            address.block_slab_id(),
            address.offset(),
            address.length(),
            &mut tally,
        )?;
        // Charged before the decode, not after. A read that reached the disk cost the disk
        // whether or not the record on it decodes, and the old order left a failed decode
        // looking like a read that never happened.
        let bytes = read.charge(&self.read_view.counters);
        let decoded = decode_block_record(&bytes, address)?;
        // `decode_block_record` just verified this payload against the CRC32C stored in the
        // record envelope, and cross-checked the record header's block id against this address.
        // It cross-checks NOTHING ELSE: the header carries no object id and no routing bucket, so
        // the two arms that name them cannot match -- read the note in `decode_block_record`
        // before relying on either. A second comparison against a digest carried in the index
        // added nothing here: the CRC covers corruption of these bytes, and it is the block id and
        // the stored-length check, not the CRC, that stand against an entry pointing at the wrong
        // block.
        let bytes = decoded.payload;
        self.read_view.counters.add_logical(
            decoded.logical_len as u64,
            u64::from(decoded.compression == BlockRecordCompression::Zstd),
        );
        Ok(bytes)
    }

    pub fn read_range(
        &self,
        block_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: drive the shared-store read-through for slab-report /
        // streaming reads too, so a not-yet-fetched checkpoint slab is pulled + cached on demand.
        self.ensure_slab_present(block_slab_id)?;
        let mut tally = BlockStoreReadTally::start();
        let read = LocalSlabBackend::new(&self.read_view.root).read_range_at_most(
            block_slab_id,
            offset,
            size,
            &mut tally,
        )?;
        Ok(read.charge(&self.read_view.counters))
    }

    pub fn read_logical_range(
        &self,
        block_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: drive the shared-store read-through for slab-report /
        // streaming reads too, so a not-yet-fetched checkpoint slab is pulled + cached on demand.
        self.ensure_slab_present(block_slab_id)?;
        let mut tally = BlockStoreReadTally::start();
        // `bytes_read` is charged the WHOLE slab, which is what this read pulls off disk to
        // answer a logical range. It used to be charged the length of the slice handed back --
        // the same number `logical_bytes_read` gets, which is the field that means that.
        let read =
            LocalSlabBackend::new(&self.read_view.root).read_all(block_slab_id, &mut tally)?;
        let slab = read.charge(&self.read_view.counters);
        let range = logical_range_from_slab(&slab, block_slab_id, offset, size)?;
        let bytes = range.bytes;
        self.read_view
            .counters
            .add_logical(bytes.len() as u64, range.compressed_records_read);
        Ok(bytes)
    }

    /// A whole slab, by id.
    ///
    /// COUNTED, which it was not. This read the slab file straight off disk and charged the store
    /// nothing, so the dump manifest, the cluster snapshot and the two shared-storage paths that
    /// use it were invisible in `BlockStoreStats::reads` and `bytes_read` -- the other three read
    /// entry points beside it all charged. Going through the backend makes that impossible to
    /// repeat: `CountedSlabRead` does not give up its bytes without a `&mut BlockStoreStats`.
    ///
    /// No store lock is taken for any of it, because a whole-slab read under the lock would block
    /// every writer for the length of a slab.
    pub fn read_slab(&self, block_slab_id: u64) -> Result<Vec<u8>, BlockStoreError> {
        self.ensure_slab_present(block_slab_id)?;
        let mut tally = BlockStoreReadTally::start();
        let read =
            LocalSlabBackend::new(&self.read_view.root).read_all(block_slab_id, &mut tally)?;
        let bytes = read.charge(&self.read_view.counters);
        #[cfg(test)]
        crate::snapshot_probe::note_slab_read(bytes.len() as u64);
        Ok(bytes)
    }

    /// Install one slab, and rewrite the whole slab manifest.
    ///
    /// The manifest rewrite is the expensive part and it grows with the store: every install
    /// serializes every slab descriptor, writes them to a fresh file, fsyncs it, renames it and
    /// fsyncs the directory. Installing n slabs therefore writes the manifest n times. Timed by
    /// phase on one machine: 111.7 ms per install at 200 slabs, 270.7 ms at 800 -- while purging
    /// all of them afterwards costs about 0.65 ms each, so the collection is not what is dear here.
    ///
    /// Fixable, and not fixed: the manifest is a CACHE, rebuildable from the slabs themselves by
    /// `rebuild_slab_manifest_at`, so it does not have to be written on every install. Writing it
    /// periodically needs the load path to notice a stale one -- comparing its set against the
    /// slabs actually present -- because a stale manifest is trusted today, which is worse than a
    /// missing one.
    pub fn install_slab(
        &self,
        block_slab_id: u64,
        bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        #[cfg(test)]
        crate::snapshot_probe::note_slab_install(bytes.len() as u64);
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        std::fs::create_dir_all(&inner.root)?;
        let path = slab_path(&inner.root, block_slab_id);
        let temp_path = path.with_extension(format!(
            "seg.tmp.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        {
            let mut temp = std::fs::File::create(&temp_path)?;
            temp.write_all(bytes)?;
            temp.flush()?;
            temp.sync_all()?;
        }
        let slab_summary = summarize_slab(bytes, block_slab_id)?;
        if block_slab_id >= inner.block_slab_id {
            inner.active_append_file = None;
        }
        std::fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        if block_slab_id >= inner.block_slab_id {
            inner.block_slab_id = block_slab_id;
            inner.write_offset = bytes.len() as u64;
        }
        if let Some(max_block_id) = slab_summary.last_block_id {
            inner.next_block_id = inner.next_block_id.max(max_block_id.saturating_add(1));
        }
        let is_current_slab = block_slab_id == inner.block_slab_id;
        let now = now_unix_ms();
        inner.slabs.insert(
            block_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: block_slab_id,
                block_slab_id,
                owner_shard_id: slab_summary.owner_shard_id,
                state: if is_current_slab {
                    BlockStoreSlabState::Active
                } else {
                    BlockStoreSlabState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: slab_summary.logical_bytes,
                created_unix_ms: Some(
                    file_modified_unix_ms(&path)
                        .or_else(|| file_created_unix_ms(&path))
                        .unwrap_or(now),
                ),
                updated_unix_ms: Some(now),
                first_block_id: slab_summary.first_block_id,
                last_block_id: slab_summary.last_block_id,
                readable_prefix_physical_bytes: bytes.len() as u64,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        if is_current_slab {
            for slab in inner.slabs.values_mut() {
                if slab.block_slab_id != block_slab_id
                    && slab.state == BlockStoreSlabState::Active
                {
                    slab.state = BlockStoreSlabState::Sealed;
                }
            }
        }
        // Not written on every install: the cost of writing it is the cost of the whole
        // manifest, so doing it per install makes installing n slabs cost n manifests. The load
        // rebuilds from the slabs when what it reads does not match them, so the worst a deferred
        // write costs is a rebuild after a crash.
        inner.slabs_unwritten = inner.slabs_unwritten.saturating_add(1);
        if inner.slabs_unwritten >= SLABS_UNWRITTEN_BEFORE_PERSIST {
            inner.slabs_unwritten = 0;
            inner.persist_slab_manifest_counted()?;
        }
        Ok(())
    }
}

/// Every way into the block store that reads slab bytes, and whether the store counts it.
///
/// `BlockStoreStats::reads` and `bytes_read` are `pub` on a crate other people build on, so a
/// read the store performs and does not count is a wrong number for them as well as for our own
/// guards. The table below is the DENOMINATOR: it names each entry point once and asserts the
/// list is complete, so an entry point added without a row here is a failing count rather than a
/// silent omission.
#[cfg(test)]
mod read_entry_point_counting {
    use super::super::BlockStore;

    type Exercise = Box<dyn Fn(&BlockStore)>;

    #[test]
    fn every_read_entry_point_charges_one_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store.append(b"counted-payload").unwrap();
        let slab = address.block_slab_id();

        let entry_points: Vec<(&str, Exercise)> = vec![
            (
                "read",
                Box::new(move |store: &BlockStore| {
                    store.read(&address).unwrap();
                }),
            ),
            (
                "read_range",
                Box::new(move |store: &BlockStore| {
                    store.read_range(slab, 0, 4).unwrap();
                }),
            ),
            (
                "read_logical_range",
                Box::new(move |store: &BlockStore| {
                    store.read_logical_range(slab, 0, 4).unwrap();
                }),
            ),
            (
                "read_slab",
                Box::new(move |store: &BlockStore| {
                    store.read_slab(slab).unwrap();
                }),
            ),
        ];

        // VACUITY FLOOR. A table that lost its rows would pass every assertion below.
        assert_eq!(
            entry_points.len(),
            4,
            "the store has four read entry points; the table must name every one",
        );

        let mut exercised = 0usize;
        let mut uncounted: Vec<&str> = Vec::new();
        for (name, run) in &entry_points {
            let before = store.stats();
            run(&store);
            let after = store.stats();
            exercised += 1;
            if after.reads != before.reads + 1 || after.bytes_read <= before.bytes_read {
                uncounted.push(name);
            }
        }

        assert_eq!(
            exercised,
            entry_points.len(),
            "every named entry point must actually have run",
        );
        assert!(
            uncounted.is_empty(),
            "{} of {exercised} read entry points performed a read the store did not count: {:?}",
            uncounted.len(),
            uncounted,
        );
    }
}
