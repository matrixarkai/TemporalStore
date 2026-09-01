// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalBlockStore append/append_batch methods, split from block_store.rs.
use super::*;
use super::record::sha256_bytes;

impl LocalBlockStore {
    /// Force durability for writes made under relaxed (bulk) mode: fsync the
    /// active segment's data and persist the extent manifest once. No-op when
    /// nothing was deferred (e.g. the live per-append-fsync path).
    pub fn sync_durable(&self) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if !inner.relaxed_dirty {
            return Ok(());
        }
        let path = slab_path(&inner.root, inner.page_slab_id);
        if let Ok(file) = OpenOptions::new().append(true).open(&path) {
            file.sync_data()?;
        }
        persist_band_manifest(&inner.root, &inner.bands)?;
        inner.relaxed_dirty = false;
        Ok(())
    }

    pub fn append(&self, bytes: &[u8]) -> Result<BlockAddress, BlockStoreError> {
        self.append_with_object_id(bytes, None)
    }

    pub fn append_with_object_id(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
    ) -> Result<BlockAddress, BlockStoreError> {
        self.append_with_page_metadata(bytes, object_id, None)
    }

    pub fn append_with_page_metadata(
        &self,
        bytes: &[u8],
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
    ) -> Result<BlockAddress, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let slab_target_bytes = effective_block_slab_target_bytes();
        let mut page_id = inner.next_page_id;
        let mut band_id = band_id_for_slab(inner.page_slab_id);
        let mut record = encode_page_record(
            bytes,
            page_id,
            object_id,
            routing_bucket,
            band_id,
            inner.options,
        )?;
        if should_roll_before_append(
            inner.write_offset,
            record.bytes.len() as u64,
            slab_target_bytes,
        ) {
            roll_slab_inner(&mut inner)?;
            page_id = inner.next_page_id;
            band_id = band_id_for_slab(inner.page_slab_id);
            record = encode_page_record(
                bytes,
                page_id,
                object_id,
                routing_bucket,
                band_id,
                inner.options,
            )?;
        }
        let path = slab_path(&inner.root, inner.page_slab_id);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let address = BlockAddress::from_parts(inner.page_slab_id, inner.write_offset, record.bytes.len() as u64, Some(page_id), object_id, routing_bucket, Some(page_id), Some(band_id), Some(sha256_bytes(bytes)));
        file.write_all(&record.bytes)?;
        file.flush()?;
        // Two INDEPENDENT relaxations:
        //  * `defer_data_sync` skips this record's data-page fdatasync. UNSAFE on the live
        //    path: the served index references pages by (slab,offset,page_id) and recovery
        //    may skip WAL replay when the index anchor is current, leaving dangling refs to
        //    an un-synced page (verified data loss under SIGKILL). Only bulk backfill --
        //    which pins the replay anchor and re-drives the whole chunk -- may defer it.
        //  * `defer_manifest` defers the per-record extent-manifest persist (write-tmp +
        //    fsync + rename EVERY record -- barriers 5/6) to sync_durable()/slab-seal. SAFE
        //    even with pages kept durable: the manifest is band/GC metadata reconstructed
        //    by reconcile-on-open from the durable page records, never a read dependency.
        let defer_data_sync = bulk_relaxed_durability() || page_wal_single_barrier();
        let defer_manifest = bulk_relaxed_durability() || page_wal_only_sync();
        if !defer_data_sync {
            file.sync_data()?;
        }
        inner.next_page_id = inner.next_page_id.saturating_add(1);
        inner.write_offset += address.length;
        let page_slab_id = inner.page_slab_id;
        let write_offset = inner.write_offset;
        upsert_band_after_append(
            &mut inner.bands,
            page_slab_id,
            write_offset,
            record.logical_len as u64,
            page_id,
        );
        if defer_manifest {
            inner.relaxed_dirty = true;
        } else {
            persist_band_manifest(&inner.root, &inner.bands)?;
        }
        inner.stats.writes += 1;
        inner.stats.bytes_written += address.length;
        inner.stats.logical_bytes_written += record.logical_len as u64;
        if record.compression == PageRecordCompression::Zstd {
            inner.stats.compressed_records_written += 1;
            inner.stats.compression_bytes_saved +=
                record.logical_len.saturating_sub(record.stored_len) as u64;
        }
        Ok(address)
    }

    pub fn append_batch_with_page_metadata(
        &self,
        records: Vec<BlockAppendRecord>,
    ) -> Result<Vec<BlockAddress>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if records.is_empty() {
            return Ok(Vec::new());
        }
        fs::create_dir_all(&inner.root)?;
        let slab_target_bytes = effective_block_slab_target_bytes();
        let mut file = None::<File>;
        let mut addresses = Vec::with_capacity(records.len());
        let mut writes = 0u64;
        let mut bytes_written = 0u64;
        let mut logical_bytes_written = 0u64;
        let mut compressed_records_written = 0u64;
        let mut compression_bytes_saved = 0u64;

        for (bytes, object_id, routing_bucket) in records {
            let mut page_id = inner.next_page_id;
            let mut band_id = band_id_for_slab(inner.page_slab_id);
            let mut record = encode_page_record(
                &bytes,
                page_id,
                object_id,
                routing_bucket,
                band_id,
                inner.options,
            )?;
            if should_roll_before_append(
                inner.write_offset,
                record.bytes.len() as u64,
                slab_target_bytes,
            ) {
                if let Some(mut current) = file.take() {
                    current.flush()?;
                    current.sync_data()?;
                }
                roll_slab_inner(&mut inner)?;
                page_id = inner.next_page_id;
                band_id = band_id_for_slab(inner.page_slab_id);
                record = encode_page_record(
                    &bytes,
                    page_id,
                    object_id,
                    routing_bucket,
                    band_id,
                    inner.options,
                )?;
            }
            if file.is_none() {
                let path = slab_path(&inner.root, inner.page_slab_id);
                file = Some(OpenOptions::new().create(true).append(true).open(path)?);
            }
            let address = BlockAddress::from_parts(inner.page_slab_id, inner.write_offset, record.bytes.len() as u64, Some(page_id), object_id, routing_bucket, Some(page_id), Some(band_id), Some(sha256_bytes(&bytes)));
            if let Some(current) = file.as_mut() {
                current.write_all(&record.bytes)?;
            }
            inner.next_page_id = inner.next_page_id.saturating_add(1);
            inner.write_offset += address.length;
            let page_slab_id = inner.page_slab_id;
            let write_offset = inner.write_offset;
            upsert_band_after_append(
                &mut inner.bands,
                page_slab_id,
                write_offset,
                record.logical_len as u64,
                page_id,
            );
            writes = writes.saturating_add(1);
            bytes_written = bytes_written.saturating_add(address.length);
            logical_bytes_written = logical_bytes_written.saturating_add(record.logical_len as u64);
            if record.compression == PageRecordCompression::Zstd {
                compressed_records_written = compressed_records_written.saturating_add(1);
                compression_bytes_saved = compression_bytes_saved
                    .saturating_add(record.logical_len.saturating_sub(record.stored_len) as u64);
            }
            addresses.push(address);
        }
        // The batch already amortizes the data barrier across all its records (one
        // sync_data for the whole batch). Under the single-barrier default both the data-page
        // fdatasync and the extent-manifest persist are deferred (the manifest is reconciled on
        // open); base-only recovery re-derives every post-dump page by WAL replay, so a
        // never-fsync'd page is rebuilt, never left dangling. Under the TS_WAL_LEGACY_RECOVERY
        // escape hatch both barriers stay synchronous on the live path (delta-fold recovery).
        let defer_data_sync = bulk_relaxed_durability() || page_wal_single_barrier();
        let defer_manifest = bulk_relaxed_durability() || page_wal_only_sync();
        if let Some(mut current) = file {
            current.flush()?;
            if !defer_data_sync {
                current.sync_data()?;
            }
        }
        if defer_manifest {
            inner.relaxed_dirty = true;
        } else {
            persist_band_manifest(&inner.root, &inner.bands)?;
        }
        inner.stats.writes = inner.stats.writes.saturating_add(writes);
        inner.stats.bytes_written = inner.stats.bytes_written.saturating_add(bytes_written);
        inner.stats.logical_bytes_written = inner
            .stats
            .logical_bytes_written
            .saturating_add(logical_bytes_written);
        inner.stats.compressed_records_written = inner
            .stats
            .compressed_records_written
            .saturating_add(compressed_records_written);
        inner.stats.compression_bytes_saved = inner
            .stats
            .compression_bytes_saved
            .saturating_add(compression_bytes_saved);
        Ok(addresses)
    }
}
