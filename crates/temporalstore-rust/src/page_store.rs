use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageAddress {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
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
        };
        file.write_all(bytes)?;
        file.flush()?;
        inner.write_offset += address.length;
        inner.stats.writes += 1;
        inner.stats.bytes_written += address.length;
        Ok(address)
    }

    pub fn read(&self, address: &PageAddress) -> Result<Vec<u8>, PageStoreError> {
        let mut inner = self.inner.lock().expect("page store lock poisoned");
        let path = segment_path(&inner.root, address.page_segment_id);
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(address.offset))?;
        let mut bytes = vec![0; address.length as usize];
        file.read_exact(&mut bytes)?;
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
        fs::write(segment_path(&inner.root, page_segment_id), bytes)?;
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
        let inner = self.inner.lock().expect("page store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let current_page_segment_id = inner.page_segment_id;
        let mut removed = Vec::new();
        let mut retained = Vec::new();
        for page_segment_id in segment_ids_at(&inner.root)? {
            if page_segment_id < retain_from_page_segment_id
                && page_segment_id != current_page_segment_id
            {
                fs::remove_file(segment_path(&inner.root, page_segment_id))?;
                removed.push(page_segment_id);
            } else {
                retained.push(page_segment_id);
            }
        }
        Ok(PageStoreGcReport {
            retain_from_page_segment_id,
            removed_page_segment_ids: removed,
            retained_page_segment_ids: retained,
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
        assert_eq!(store.segment_ids().unwrap(), vec![0, 2]);
    }
}
