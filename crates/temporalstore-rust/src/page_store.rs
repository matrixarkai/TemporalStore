use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
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
        let address = PageAddress {
            page_segment_id: inner.page_segment_id,
            offset: inner.write_offset,
            length: bytes.len() as u64,
            sha256: Some(sha256_hex(bytes)),
        };
        file.write_all(bytes)?;
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
        File::create(segment_path(&inner.root, inner.page_segment_id))?;
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

fn segment_path(root: &std::path::Path, page_segment_id: u64) -> PathBuf {
    root.join(format!("page_segment_{page_segment_id:020}.seg"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn segment_ids_at(root: &std::path::Path) -> Result<Vec<u64>, PageStoreError> {
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

        fs::write(
            segment_path(dir.path(), address.page_segment_id),
            b"corrupted-page",
        )
        .unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, PageStoreError::ChecksumMismatch { .. }));
    }

    #[test]
    fn page_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalPageStore::new(dir.path());
        let address = store.append(b"legacy-page").unwrap();
        let legacy_address = PageAddress {
            sha256: None,
            ..address
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
