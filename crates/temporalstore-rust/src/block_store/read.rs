// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalBlockStore read/read_range/install_slab methods, split from block_store.rs.
use super::*;

impl LocalBlockStore {
    pub fn read(&self, address: &BlockAddress) -> Result<Vec<u8>, BlockStoreError> {
        // Reference-parity lazy recovery: if this slab lives only in shared storage after a
        // metadata-only restore, fetch + cache it before serving the read.
        self.ensure_slab_present(address.page_slab_id)?;
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = slab_path(&inner.root, address.page_slab_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(address.offset))?;
        let mut bytes = vec![0; address.length as usize];
        file.read_exact(&mut bytes)?;
        let decoded = decode_page_record(&bytes, address)?;
        let bytes = decoded.payload;
        if let Some(expected) = &address.sha256 {
            let actual = sha256_hex(&bytes);
            if &actual != expected {
                return Err(BlockStoreError::ChecksumMismatch {
                    page_slab_id: address.page_slab_id,
                    offset: address.offset,
                    length: address.length,
                    expected: expected.clone(),
                    actual,
                });
            }
        }
        inner.stats.reads += 1;
        inner.stats.bytes_read += address.length;
        inner.stats.logical_bytes_read += decoded.logical_len as u64;
        if decoded.compression == PageRecordCompression::Zstd {
            inner.stats.compressed_records_read += 1;
        }
        Ok(bytes)
    }

    pub fn read_range(
        &self,
        page_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = slab_path(&inner.root, page_slab_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; size as usize];
        let read = file.read(&mut bytes)?;
        bytes.truncate(read);
        inner.stats.reads += 1;
        inner.stats.bytes_read += read as u64;
        Ok(bytes)
    }

    pub fn read_logical_range(
        &self,
        page_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = slab_path(&inner.root, page_slab_id);
        let slab = fs::read(path)?;
        let range = logical_range_from_slab(&slab, page_slab_id, offset, size)?;
        let bytes = range.bytes;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        inner.stats.logical_bytes_read += bytes.len() as u64;
        inner.stats.compressed_records_read += range.compressed_records_read;
        Ok(bytes)
    }

    pub fn read_slab(&self, page_slab_id: u64) -> Result<Vec<u8>, BlockStoreError> {
        self.ensure_slab_present(page_slab_id)?;
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        Ok(fs::read(slab_path(&root, page_slab_id))?)
    }

    pub fn install_slab(
        &self,
        page_slab_id: u64,
        bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = slab_path(&inner.root, page_slab_id);
        let temp_path = path.with_extension(format!(
            "seg.tmp.{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        ));
        {
            let mut temp = File::create(&temp_path)?;
            temp.write_all(bytes)?;
            temp.flush()?;
            temp.sync_all()?;
        }
        fs::rename(&temp_path, &path)?;
        sync_parent_dir(&path)?;
        if page_slab_id >= inner.page_slab_id {
            inner.page_slab_id = page_slab_id;
            inner.write_offset = bytes.len() as u64;
        }
        let band_summary = summarize_slab(bytes, page_slab_id)?;
        if let Some(max_page_id) = band_summary.last_page_id {
            inner.next_page_id = inner.next_page_id.max(max_page_id.saturating_add(1));
        }
        let is_current_slab = page_slab_id == inner.page_slab_id;
        let now = now_unix_ms();
        inner.bands.insert(
            page_slab_id,
            BlockStoreBandDescriptor {
                band_id: band_id_for_slab(page_slab_id),
                page_slab_id,
                state: if is_current_slab {
                    BlockStoreBandState::Active
                } else {
                    BlockStoreBandState::Sealed
                },
                physical_bytes: bytes.len() as u64,
                logical_bytes: band_summary.logical_bytes,
                created_unix_ms: Some(
                    file_modified_unix_ms(&path)
                        .or_else(|| file_created_unix_ms(&path))
                        .unwrap_or(now),
                ),
                updated_unix_ms: Some(now),
                first_page_id: band_summary.first_page_id,
                last_page_id: band_summary.last_page_id,
                readable_prefix_physical_bytes: bytes.len() as u64,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        if is_current_slab {
            for band in inner.bands.values_mut() {
                if band.page_slab_id != page_slab_id
                    && band.state == BlockStoreBandState::Active
                {
                    band.state = BlockStoreBandState::Sealed;
                }
            }
        }
        persist_band_manifest(&inner.root, &inner.bands)?;
        Ok(())
    }
}
