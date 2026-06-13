use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "page checksum mismatch for segment {page_segment_id} offset {offset} length {length}: expected {expected}, got {actual}"
    )]
    ChecksumMismatch {
        page_segment_id: u64,
        offset: u64,
        length: u64,
        expected: String,
        actual: String,
    },
    #[error("corrupt page envelope for segment {page_segment_id} offset {offset}: {reason}")]
    CorruptPageEnvelope {
        page_segment_id: u64,
        offset: u64,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PageAddress {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreStats {
    pub writes: u64,
    pub reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreGcReport {
    pub retain_from_page_segment_id: u64,
    pub removed_page_segment_ids: Vec<u64>,
    pub retained_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub retained_current_page_segment_ids: Vec<u64>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageStoreRollReport {
    pub previous_page_segment_id: u64,
    pub new_page_segment_id: u64,
}

#[derive(Debug, Clone)]
pub struct LocalPageStore {
    inner: Arc<Mutex<PageStoreInner>>,
}

#[derive(Debug)]
struct PageStoreInner {
    root: PathBuf,
    page_segment_id: u64,
    write_offset: u64,
    stats: PageStoreStats,
}

impl LocalPageStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        let page_segment_id = 0;
        let write_offset = segment_path(&root, page_segment_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        Self {
            inner: Arc::new(Mutex::new(PageStoreInner {
                root,
                page_segment_id,
                write_offset,
                stats: PageStoreStats::default(),
            })),
        }
    }

    pub fn append(&self, bytes: &[u8]) -> Result<PageAddress, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let path = segment_path(&inner.root, inner.page_segment_id);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let record = encode_page_record(bytes);
        let address = PageAddress {
            page_segment_id: inner.page_segment_id,
            offset: inner.write_offset,
            length: record.len() as u64,
            sha256: Some(sha256_hex(bytes)),
        };
        file.write_all(&record)?;
        file.flush()?;
        file.sync_data()?;
        inner.write_offset += address.length;
        inner.stats.writes += 1;
        inner.stats.bytes_written += address.length;
        Ok(address)
    }

    pub fn roll_segment(&self) -> Result<PageStoreRollReport, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let previous_page_segment_id = inner.page_segment_id;
        let next_from_current = inner.page_segment_id.saturating_add(1);
        let next_from_disk = segment_ids_at(&inner.root)?
            .into_iter()
            .max()
            .map(|id| id.saturating_add(1))
            .unwrap_or_default();
        inner.page_segment_id = next_from_current.max(next_from_disk);
        inner.write_offset = 0;
        let path = segment_path(&inner.root, inner.page_segment_id);
        let file = File::create(&path)?;
        file.sync_all()?;
        sync_parent_dir(&path)?;
        Ok(PageStoreRollReport {
            previous_page_segment_id,
            new_page_segment_id: inner.page_segment_id,
        })
    }

    pub fn read(&self, address: &PageAddress) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        let path = segment_path(&inner.root, address.page_segment_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(address.offset))?;
        let mut bytes = vec![0; address.length as usize];
        file.read_exact(&mut bytes)?;
        let bytes = decode_page_record(&bytes, address)?;
        if let Some(expected) = &address.sha256 {
            let actual = sha256_hex(&bytes);
            if &actual != expected {
                return Err(PageStoreError::ChecksumMismatch {
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
        Ok(bytes)
    }

    pub fn read_range(
        &self,
        page_segment_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
    ) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        let path = segment_path(&inner.root, page_segment_id);
        let segment = fs::read(path)?;
        let bytes = logical_range_from_segment(&segment, page_segment_id, offset, size)?;
        inner.stats.reads += 1;
        inner.stats.bytes_read += bytes.len() as u64;
        Ok(bytes)
    }

    pub fn read_segment(&self, page_segment_id: u64) -> Result<Vec<u8>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        Ok(fs::read(segment_path(&root, page_segment_id))?)
    }

    pub fn install_segment(
        &self,
        page_segment_id: u64,
        bytes: &[u8],
    ) -> Result<(), PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
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
        if page_segment_id == inner.page_segment_id {
            inner.write_offset = bytes.len() as u64;
        }
        Ok(())
    }

    pub fn segment_ids(&self) -> Result<Vec<u64>, PageStoreError> {
        let root = self
            .inner
            .lock()
            .expect("page store lock poisoned")
            .root
            .clone();
        let mut ids = Vec::new();
        if !root.exists() {
            return Ok(ids);
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(id) = name
                .strip_prefix("page_segment_")
                .and_then(|name| name.strip_suffix(".seg"))
                .and_then(|id| id.parse::<u64>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub fn gc_segments_before(
        &self,
        retain_from_page_segment_id: u64,
    ) -> Result<PageStoreGcReport, PageStoreError> {
        self.gc_segments_before_with_live_refs(retain_from_page_segment_id, std::iter::empty())
    }

    pub fn gc_segments_before_with_live_refs(
        &self,
        retain_from_page_segment_id: u64,
        live_page_segment_ids: impl IntoIterator<Item = u64>,
    ) -> Result<PageStoreGcReport, PageStoreError> {
        let inner = self.inner.lock().expect("page store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let current_page_segment_id = inner.page_segment_id;
        let live_page_segment_ids = live_page_segment_ids.into_iter().collect::<BTreeSet<_>>();
        let mut removed = Vec::new();
        let mut retained = Vec::new();
        let mut retained_live = Vec::new();
        let mut retained_current = Vec::new();
        for page_segment_id in segment_ids_at(&inner.root)? {
            let below_retention_floor = page_segment_id < retain_from_page_segment_id;
            let is_current = page_segment_id == current_page_segment_id;
            let is_live = live_page_segment_ids.contains(&page_segment_id);
            if below_retention_floor && !is_current && !is_live {
                fs::remove_file(segment_path(&inner.root, page_segment_id))?;
                removed.push(page_segment_id);
            } else {
                if below_retention_floor && is_current {
                    retained_current.push(page_segment_id);
                }
                if below_retention_floor && is_live {
                    retained_live.push(page_segment_id);
                }
                retained.push(page_segment_id);
            }
        }
        Ok(PageStoreGcReport {
            retain_from_page_segment_id,
            removed_page_segment_ids: removed,
            retained_page_segment_ids: retained,
            retained_live_page_segment_ids: retained_live,
            retained_current_page_segment_ids: retained_current,
        })
    }

    pub fn stats(&self) -> PageStoreStats {
        self.inner.lock().expect("page store lock poisoned").stats
    }
}

impl Default for LocalPageStore {
    fn default() -> Self {
        Self::new(unique_temp_path("pages"))
    }
}

fn segment_path(root: &Path, page_segment_id: u64) -> PathBuf {
    root.join(format!("page_segment_{page_segment_id:020}.seg"))
}

const PAGE_RECORD_MAGIC: &[u8; 8] = b"TSPAGE01";
const PAGE_RECORD_VERSION: u8 = 1;
const PAGE_RECORD_HEADER_LEN: usize = 8 + 1 + 1 + 2 + 8 + 8 + 32;

fn encode_page_record(payload: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(payload);
    let mut record = Vec::with_capacity(PAGE_RECORD_HEADER_LEN + payload.len());
    record.extend_from_slice(PAGE_RECORD_MAGIC);
    record.push(PAGE_RECORD_VERSION);
    record.push(0);
    record.extend_from_slice(&(PAGE_RECORD_HEADER_LEN as u16).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    record.extend_from_slice(&digest);
    record.extend_from_slice(payload);
    record
}

fn decode_page_record(record: &[u8], address: &PageAddress) -> Result<Vec<u8>, PageStoreError> {
    if !record.starts_with(PAGE_RECORD_MAGIC) {
        return Ok(record.to_vec());
    }
    if record.len() < PAGE_RECORD_HEADER_LEN {
        return Err(corrupt_page_envelope(address, "short header"));
    }
    let (header_len, payload_len, expected_sha256) = parse_page_record_header(record, address)?;
    if record.len() != header_len + payload_len {
        return Err(corrupt_page_envelope(
            address,
            "payload length mismatch".to_string(),
        ));
    }
    let payload = &record[header_len..];
    verify_page_record_checksum(payload, &expected_sha256, address)?;
    Ok(payload.to_vec())
}

fn logical_range_from_segment(
    segment: &[u8],
    page_segment_id: u64,
    offset: u64,
    size: u64,
) -> Result<Vec<u8>, PageStoreError> {
    if size == 0 {
        return Ok(Vec::new());
    }
    if !segment.starts_with(PAGE_RECORD_MAGIC) {
        let start = offset as usize;
        let end = start.saturating_add(size as usize).min(segment.len());
        return Ok(if start >= segment.len() {
            Vec::new()
        } else {
            segment[start..end].to_vec()
        });
    }

    let requested_start = offset as usize;
    let requested_end = requested_start.saturating_add(size as usize);
    let mut physical_offset = 0usize;
    let mut logical_offset = 0usize;
    let mut out = Vec::with_capacity(size as usize);

    while physical_offset < segment.len() && out.len() < size as usize {
        let remaining = &segment[physical_offset..];
        let address = PageAddress {
            page_segment_id,
            offset: physical_offset as u64,
            length: 0,
            sha256: None,
        };
        if !remaining.starts_with(PAGE_RECORD_MAGIC) {
            return Err(corrupt_page_envelope(
                &address,
                "mixed raw bytes after page envelope",
            ));
        }
        if remaining.len() < PAGE_RECORD_HEADER_LEN {
            return Err(corrupt_page_envelope(&address, "short header"));
        }
        let (header_len, payload_len, expected_sha256) =
            parse_page_record_header(remaining, &address)?;
        let record_len = header_len.saturating_add(payload_len);
        if remaining.len() < record_len {
            return Err(corrupt_page_envelope(
                &address,
                "payload length mismatch".to_string(),
            ));
        }
        let payload = &remaining[header_len..record_len];
        let address = PageAddress {
            length: record_len as u64,
            ..address
        };
        verify_page_record_checksum(payload, &expected_sha256, &address)?;

        let logical_end = logical_offset.saturating_add(payload_len);
        let overlap_start = requested_start.max(logical_offset);
        let overlap_end = requested_end.min(logical_end);
        if overlap_start < overlap_end {
            let payload_start = overlap_start - logical_offset;
            let payload_end = overlap_end - logical_offset;
            out.extend_from_slice(&payload[payload_start..payload_end]);
        }

        physical_offset = physical_offset.saturating_add(record_len);
        logical_offset = logical_end;
    }

    Ok(out)
}

fn parse_page_record_header(
    record: &[u8],
    address: &PageAddress,
) -> Result<(usize, usize, [u8; 32]), PageStoreError> {
    let version = record[8];
    if version != PAGE_RECORD_VERSION {
        return Err(corrupt_page_envelope(
            address,
            format!("unsupported version {version}"),
        ));
    }
    let header_len = u16::from_le_bytes(
        record[10..12]
            .try_into()
            .expect("page envelope header length slice"),
    ) as usize;
    if header_len != PAGE_RECORD_HEADER_LEN {
        return Err(corrupt_page_envelope(
            address,
            format!("unexpected header length {header_len}"),
        ));
    }
    let payload_len = u64::from_le_bytes(
        record[12..20]
            .try_into()
            .expect("page envelope payload length slice"),
    ) as usize;
    let raw_len = u64::from_le_bytes(
        record[20..28]
            .try_into()
            .expect("page envelope raw length slice"),
    ) as usize;
    if raw_len != payload_len {
        return Err(corrupt_page_envelope(
            address,
            format!("raw length {raw_len} does not match payload length {payload_len}"),
        ));
    }
    let expected_sha256 = record[28..60]
        .try_into()
        .expect("page envelope sha256 slice");
    Ok((header_len, payload_len, expected_sha256))
}

fn verify_page_record_checksum(
    payload: &[u8],
    expected_sha256: &[u8; 32],
    address: &PageAddress,
) -> Result<(), PageStoreError> {
    let actual_sha256 = Sha256::digest(payload);
    if &actual_sha256[..] != expected_sha256 {
        return Err(PageStoreError::ChecksumMismatch {
            page_segment_id: address.page_segment_id,
            offset: address.offset,
            length: address.length,
            expected: hex::encode(expected_sha256),
            actual: hex::encode(actual_sha256),
        });
    }
    Ok(())
}

fn corrupt_page_envelope(address: &PageAddress, reason: impl Into<String>) -> PageStoreError {
    PageStoreError::CorruptPageEnvelope {
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        reason: reason.into(),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn segment_ids_at(root: &Path) -> Result<Vec<u64>, PageStoreError> {
    let mut ids = Vec::new();
    if !root.exists() {
        return Ok(ids);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if let Some(id) = name
            .strip_prefix("page_segment_")
            .and_then(|name| name.strip_suffix(".seg"))
            .and_then(|id| id.parse::<u64>().ok())
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
}

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_segments_removes_old_non_current_segments() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        store.install_segment(0, b"current").unwrap();
        store.install_segment(1, b"old").unwrap();
        store.install_segment(2, b"keep").unwrap();

        let report = store.gc_segments_before(2).unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![1]);
        assert_eq!(report.retained_page_segment_ids, vec![0, 2]);
        assert_eq!(report.retained_current_page_segment_ids, vec![0]);
        assert!(report.retained_live_page_segment_ids.is_empty());
        assert_eq!(store.segment_ids().unwrap(), vec![0, 2]);
    }

    #[test]
    fn roll_segment_moves_future_appends_to_fresh_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        assert_eq!(first.page_segment_id, 0);

        let roll = store.roll_segment().unwrap();
        assert_eq!(roll.previous_page_segment_id, 0);
        assert_eq!(roll.new_page_segment_id, 1);
        let second = store.append(b"second").unwrap();
        assert_eq!(second.page_segment_id, 1);
        assert_eq!(second.offset, 0);
        assert_eq!(store.read(&first).unwrap(), b"first");
        assert_eq!(store.read(&second).unwrap(), b"second");
    }

    #[test]
    fn page_address_checksum_rejects_corrupt_segment_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"verified-page").unwrap();
        assert_eq!(address.sha256, Some(sha256_hex(b"verified-page")));
        assert_eq!(store.read(&address).unwrap(), b"verified-page");

        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        fs::write(path, segment).unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::ChecksumMismatch { .. }));
    }

    #[test]
    fn page_segment_records_have_self_describing_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"enveloped-page").unwrap();
        let raw = store.read_segment(address.page_segment_id).unwrap();

        assert!(raw.starts_with(PAGE_RECORD_MAGIC));
        assert_eq!(raw[8], PAGE_RECORD_VERSION);
        assert_eq!(store.read(&address).unwrap(), b"enveloped-page");
    }

    #[test]
    fn logical_page_range_skips_record_envelopes_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        store.append(b"abc").unwrap();
        store.append(b"def").unwrap();

        assert_eq!(store.read_logical_range(0, 1, 4).unwrap(), b"bcde");
    }

    #[test]
    fn page_envelope_rejects_corrupt_header_lengths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"header-checked-page").unwrap();
        let path = segment_path(dir.path(), address.page_segment_id);
        let mut segment = fs::read(&path).unwrap();
        segment[10] = 1;
        segment[11] = 0;
        fs::write(path, segment).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn page_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let legacy_address = PageAddress {
            page_segment_id: 0,
            offset: 0,
            length: b"alteredpage".len() as u64,
            sha256: None,
        };
        fs::write(
            segment_path(dir.path(), legacy_address.page_segment_id),
            b"alteredpage",
        )
        .unwrap();

        assert_eq!(store.read(&legacy_address).unwrap(), b"alteredpage");
    }

    #[test]
    fn gc_segments_retains_live_index_references_below_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        store.install_segment(0, b"current").unwrap();
        store.install_segment(1, b"live").unwrap();
        store.install_segment(2, b"stale").unwrap();
        store.install_segment(3, b"keep").unwrap();

        let report = store.gc_segments_before_with_live_refs(3, [1_u64]).unwrap();
        assert_eq!(report.removed_page_segment_ids, vec![2]);
        assert_eq!(report.retained_page_segment_ids, vec![0, 1, 3]);
        assert_eq!(report.retained_current_page_segment_ids, vec![0]);
        assert_eq!(report.retained_live_page_segment_ids, vec![1]);
        assert_eq!(store.segment_ids().unwrap(), vec![0, 1, 3]);
    }
}
