//! LocalBlockStore read/read_range/install_segment methods, split from block_store.rs.
use super::*;

impl LocalBlockStore {
    pub fn read(&self, address: &BlockAddress) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, address.page_segment_id);
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
                    page_segment_id: address.page_segment_id,
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
        page_segment_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, page_segment_id);
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
        page_segment_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let path = segment_path(&inner.root, page_segment_id);
        let segment = fs::read(path)?;
        let range = logical_range_from_segment(&segment, page_segment_id, offset, size)?;
        let bytes = range.bytes;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        inner.stats.logical_bytes_read += bytes.len() as u64;
        inner.stats.compressed_records_read += range.compressed_records_read;
        Ok(bytes)
    }

    pub fn read_segment(&self, page_segment_id: u64) -> Result<Vec<u8>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        Ok(fs::read(segment_path(&root, page_segment_id))?)
    }

    pub fn install_segment(
        &self,
        page_segment_id: u64,
        bytes: &[u8],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = segment_path(&inner.root, page_segment_id);
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
        if page_segment_id >= inner.page_segment_id {
            inner.page_segment_id = page_segment_id;
            inner.write_offset = bytes.len() as u64;
        }
        let band_summary = summarize_segment(bytes, page_segment_id)?;
        if let Some(max_page_id) = band_summary.last_page_id {
            inner.next_page_id = inner.next_page_id.max(max_page_id.saturating_add(1));
        }
        let is_current_segment = page_segment_id == inner.page_segment_id;
        let now = now_unix_ms();
        inner.bands.insert(
            page_segment_id,
            BlockStoreBandDescriptor {
                band_id: band_id_for_segment(page_segment_id),
                page_segment_id,
                state: if is_current_segment {
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
        if is_current_segment {
            for band in inner.bands.values_mut() {
                if band.page_segment_id != page_segment_id
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
