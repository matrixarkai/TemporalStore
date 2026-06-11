use std::collections::{HashMap, VecDeque};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::ShardId;

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt cache block: {0}")]
    CorruptBlock(String),
    #[error("unsupported cache block codec {0}")]
    UnsupportedCodec(u8),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub shard_id: ShardId,
    pub record_key: String,
    pub namespace: String,
    pub selector: String,
}

impl CacheKey {
    pub fn string(shard_id: ShardId, key: &str) -> Self {
        Self {
            shard_id,
            record_key: key.to_string(),
            namespace: "string".to_string(),
            selector: "value".to_string(),
        }
    }

    pub fn hash(shard_id: ShardId, key: &str, field: &str) -> Self {
        Self {
            shard_id,
            record_key: key.to_string(),
            namespace: "hash".to_string(),
            selector: field.to_string(),
        }
    }

    pub fn set_members(shard_id: ShardId, key: &str) -> Self {
        Self {
            shard_id,
            record_key: key.to_string(),
            namespace: "set".to_string(),
            selector: "members".to_string(),
        }
    }

    pub fn feature_query(
        shard_id: ShardId,
        key: &str,
        start_ms: u64,
        end_ms: u64,
        count: Option<usize>,
    ) -> Self {
        Self {
            shard_id,
            record_key: key.to_string(),
            namespace: "feature".to_string(),
            selector: format!("{start_ms}:{end_ms}:{}", count.unwrap_or(5000)),
        }
    }

    pub fn page(shard_id: ShardId, page_segment_id: u64, offset: u64, length: u64) -> Self {
        Self {
            shard_id,
            record_key: format!("segment-{page_segment_id:020}"),
            namespace: "page".to_string(),
            selector: format!("{offset}:{length}"),
        }
    }

    fn disk_name(&self) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        format!("{:016x}.cache_block", hasher.finish())
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStats {
    pub memory_hits: u64,
    pub disk_hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub invalidations: u64,
    pub memory_evictions: u64,
    pub compressed_puts: u64,
    pub compressed_hits: u64,
    pub compression_bytes_saved: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheCompression {
    None,
    Zstd { level: i32 },
}

impl Default for CacheCompression {
    fn default() -> Self {
        Self::Zstd { level: 1 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBlockOptions {
    pub compression: CacheCompression,
    pub min_compress_bytes: usize,
}

impl Default for CacheBlockOptions {
    fn default() -> Self {
        Self {
            compression: CacheCompression::default(),
            min_compress_bytes: 128,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheGcReport {
    pub shard_id: ShardId,
    pub memory_entries_removed: usize,
    pub disk_bytes_removed: u64,
}

#[derive(Debug, Clone)]
pub struct MultiLayerCache {
    inner: Arc<RwLock<CacheInner>>,
}

#[derive(Debug)]
struct CacheInner {
    memory_capacity_bytes: usize,
    memory_bytes: usize,
    disk_dir: PathBuf,
    block_options: CacheBlockOptions,
    memory: HashMap<CacheKey, Vec<u8>>,
    order: VecDeque<CacheKey>,
    stats: CacheStats,
}

impl MultiLayerCache {
    pub fn new(memory_capacity_bytes: usize, disk_dir: impl Into<PathBuf>) -> Self {
        Self::with_block_options(
            memory_capacity_bytes,
            disk_dir,
            CacheBlockOptions::default(),
        )
    }

    pub fn with_block_options(
        memory_capacity_bytes: usize,
        disk_dir: impl Into<PathBuf>,
        block_options: CacheBlockOptions,
    ) -> Self {
        let disk_dir = disk_dir.into();
        let _ = fs::create_dir_all(&disk_dir);
        Self {
            inner: Arc::new(RwLock::new(CacheInner {
                memory_capacity_bytes,
                memory_bytes: 0,
                disk_dir,
                block_options,
                memory: HashMap::new(),
                order: VecDeque::new(),
                stats: CacheStats::default(),
            })),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
        {
            let mut inner = self.inner.write().expect("cache lock poisoned");
            if let Some(value) = inner.memory.get(key).cloned() {
                inner.stats.memory_hits += 1;
                return Ok(Some(value));
            }
        }

        let path = {
            let inner = self.inner.read().expect("cache lock poisoned");
            inner.disk_path(key)
        };
        match fs::read(path) {
            Ok(block) => {
                let decoded = decode_cache_block(&block)?;
                let mut inner = self.inner.write().expect("cache lock poisoned");
                inner.stats.disk_hits += 1;
                if is_encoded_compressed_block(&block) {
                    inner.stats.compressed_hits += 1;
                }
                inner.put_memory(key.clone(), decoded.clone());
                Ok(Some(decoded))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut inner = self.inner.write().expect("cache lock poisoned");
                inner.stats.misses += 1;
                Ok(None)
            }
            Err(err) => Err(CacheError::Io(err)),
        }
    }

    pub fn get_memory(&self, key: &CacheKey) -> Option<Vec<u8>> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        let value = inner.memory.get(key).cloned();
        if value.is_some() {
            inner.stats.memory_hits += 1;
        } else {
            inner.stats.misses += 1;
        }
        value
    }

    pub fn put(&self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        let path = inner.disk_path(&key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let block = encode_cache_block(&value, inner.block_options)?;
        let compressed = is_encoded_compressed_block(&block);
        if compressed {
            inner.stats.compressed_puts += 1;
            inner.stats.compression_bytes_saved += value.len().saturating_sub(block.len()) as u64;
        }
        fs::write(path, block)?;
        inner.stats.puts += 1;
        inner.stats.disk_bytes = dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes);
        inner.put_memory(key, value);
        Ok(())
    }

    pub fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if let Some(value) = inner.memory.remove(key) {
            inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
        }
        let _ = fs::remove_file(inner.disk_path(key));
        inner.stats.invalidations += 1;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.disk_bytes = dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes);
        Ok(())
    }

    pub fn invalidate_record(
        &self,
        shard_id: ShardId,
        namespace: &str,
        record_key: &str,
    ) -> Result<(), CacheError> {
        let keys = {
            let inner = self.inner.read().expect("cache lock poisoned");
            inner
                .memory
                .keys()
                .filter(|key| {
                    key.shard_id == shard_id
                        && key.namespace == namespace
                        && key.record_key == record_key
                })
                .cloned()
                .collect::<Vec<_>>()
        };
        for key in keys {
            self.invalidate(&key)?;
        }
        Ok(())
    }

    pub fn invalidate_shard(&self, shard_id: ShardId) -> Result<CacheGcReport, CacheError> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        let memory_keys = inner
            .memory
            .keys()
            .filter(|key| key.shard_id == shard_id)
            .cloned()
            .collect::<Vec<_>>();
        let memory_entries_removed = memory_keys.len();
        for key in &memory_keys {
            if let Some(value) = inner.memory.remove(key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
            }
        }
        inner.order.retain(|key| key.shard_id != shard_id);

        let shard_disk_dir = inner.disk_dir.join(format!("shard-{shard_id}"));
        let disk_bytes_before = dir_size(&shard_disk_dir).unwrap_or_default();
        match fs::remove_dir_all(&shard_disk_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(CacheError::Io(err)),
        }
        inner.stats.invalidations += memory_entries_removed as u64;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.disk_bytes = dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes);
        Ok(CacheGcReport {
            shard_id,
            memory_entries_removed,
            disk_bytes_removed: disk_bytes_before,
        })
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().expect("cache lock poisoned");
        CacheStats {
            memory_bytes: inner.memory_bytes as u64,
            disk_bytes: dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes),
            ..inner.stats
        }
    }

    #[cfg(test)]
    pub fn clear_memory_for_test(&self) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.memory.clear();
        inner.order.clear();
        inner.memory_bytes = 0;
        inner.stats.memory_bytes = 0;
    }
}

impl Default for MultiLayerCache {
    fn default() -> Self {
        Self::new(16 * 1024 * 1024, unique_temp_path("cache"))
    }
}

const CACHE_BLOCK_MAGIC: &[u8; 8] = b"TSBCACHE";
const CACHE_BLOCK_VERSION: u8 = 1;
const CACHE_CODEC_RAW: u8 = 0;
const CACHE_CODEC_ZSTD: u8 = 1;
const CACHE_HEADER_LEN: usize = 8 + 1 + 1 + 8 + 8;

fn encode_cache_block(value: &[u8], options: CacheBlockOptions) -> Result<Vec<u8>, CacheError> {
    let (codec, payload) = match options.compression {
        CacheCompression::None if value.len() >= options.min_compress_bytes => {
            (CACHE_CODEC_RAW, value.to_vec())
        }
        CacheCompression::None => (CACHE_CODEC_RAW, value.to_vec()),
        CacheCompression::Zstd { level } if value.len() >= options.min_compress_bytes => {
            let compressed = zstd::stream::encode_all(value, level)?;
            if CACHE_HEADER_LEN + compressed.len() < value.len() {
                (CACHE_CODEC_ZSTD, compressed)
            } else {
                (CACHE_CODEC_RAW, value.to_vec())
            }
        }
        CacheCompression::Zstd { .. } => (CACHE_CODEC_RAW, value.to_vec()),
    };
    let mut block = Vec::with_capacity(CACHE_HEADER_LEN + payload.len());
    block.extend_from_slice(CACHE_BLOCK_MAGIC);
    block.push(CACHE_BLOCK_VERSION);
    block.push(codec);
    block.extend_from_slice(&(value.len() as u64).to_le_bytes());
    block.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    block.extend_from_slice(&payload);
    Ok(block)
}

fn decode_cache_block(block: &[u8]) -> Result<Vec<u8>, CacheError> {
    if !block.starts_with(CACHE_BLOCK_MAGIC) {
        return Ok(block.to_vec());
    }
    if block.len() < CACHE_HEADER_LEN {
        return Err(CacheError::CorruptBlock("short header".to_string()));
    }
    let version = block[8];
    if version != CACHE_BLOCK_VERSION {
        return Err(CacheError::CorruptBlock(format!(
            "unsupported version {version}"
        )));
    }
    let codec = block[9];
    let original_len = u64::from_le_bytes(
        block[10..18]
            .try_into()
            .expect("cache block original length slice"),
    ) as usize;
    let payload_len = u64::from_le_bytes(
        block[18..26]
            .try_into()
            .expect("cache block payload length slice"),
    ) as usize;
    if block.len() != CACHE_HEADER_LEN + payload_len {
        return Err(CacheError::CorruptBlock(
            "payload length mismatch".to_string(),
        ));
    }
    let payload = &block[CACHE_HEADER_LEN..];
    let decoded = match codec {
        CACHE_CODEC_RAW => payload.to_vec(),
        CACHE_CODEC_ZSTD => zstd::stream::decode_all(payload)?,
        other => return Err(CacheError::UnsupportedCodec(other)),
    };
    if decoded.len() != original_len {
        return Err(CacheError::CorruptBlock(
            "original length mismatch".to_string(),
        ));
    }
    Ok(decoded)
}

fn is_encoded_compressed_block(block: &[u8]) -> bool {
    block.starts_with(CACHE_BLOCK_MAGIC)
        && block.len() >= CACHE_HEADER_LEN
        && block[9] == CACHE_CODEC_ZSTD
}

impl CacheInner {
    fn disk_path(&self, key: &CacheKey) -> PathBuf {
        self.disk_dir
            .join(format!("shard-{}", key.shard_id))
            .join(&key.namespace)
            .join(key.disk_name())
    }

    fn put_memory(&mut self, key: CacheKey, value: Vec<u8>) {
        if self.memory_capacity_bytes == 0 || value.len() > self.memory_capacity_bytes {
            return;
        }
        if let Some(old) = self.memory.insert(key.clone(), value.clone()) {
            self.memory_bytes = self.memory_bytes.saturating_sub(old.len());
        } else {
            self.order.push_back(key);
        }
        self.memory_bytes += value.len();
        while self.memory_bytes > self.memory_capacity_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            if let Some(old_value) = self.memory.remove(&oldest) {
                self.memory_bytes = self.memory_bytes.saturating_sub(old_value.len());
                self.stats.memory_evictions += 1;
            }
        }
        self.stats.memory_bytes = self.memory_bytes as u64;
    }
}

fn dir_size(path: &Path) -> Result<u64, std::io::Error> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                total += metadata.len();
            }
        }
    }
    Ok(total)
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
    fn disk_cache_promotes_back_to_memory() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let key = CacheKey::string(1, "record-a");

        cache.put(key.clone(), b"value".to_vec()).unwrap();
        cache.clear_memory_for_test();

        assert_eq!(cache.get(&key).unwrap(), Some(b"value".to_vec()));
        assert_eq!(cache.stats().disk_hits, 1);
        assert_eq!(cache.get_memory(&key), Some(b"value".to_vec()));
        assert_eq!(cache.stats().memory_hits, 1);
    }

    #[test]
    fn memory_cache_evicts_oldest_entries_but_keeps_disk_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(8, dir.path());
        let first = CacheKey::string(1, "first");
        let second = CacheKey::string(1, "second");

        cache.put(first.clone(), b"12345".to_vec()).unwrap();
        cache.put(second.clone(), b"abcde".to_vec()).unwrap();

        assert_eq!(cache.get_memory(&first), None);
        assert_eq!(cache.get_memory(&second), Some(b"abcde".to_vec()));
        assert_eq!(cache.get(&first).unwrap(), Some(b"12345".to_vec()));
        assert_eq!(cache.stats().disk_hits, 1);
        assert!(cache.stats().memory_evictions >= 1);
    }

    #[test]
    fn invalidate_shard_removes_memory_and_disk_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let shard_one = CacheKey::string(1, "a");
        let shard_two = CacheKey::string(2, "b");
        cache.put(shard_one.clone(), b"one".to_vec()).unwrap();
        cache.put(shard_two.clone(), b"two".to_vec()).unwrap();

        let report = cache.invalidate_shard(1).unwrap();
        assert_eq!(report.memory_entries_removed, 1);
        assert!(report.disk_bytes_removed > 0);
        assert_eq!(cache.get(&shard_one).unwrap(), None);
        assert_eq!(cache.get(&shard_two).unwrap(), Some(b"two".to_vec()));
    }

    #[test]
    fn disk_cache_serializes_compresses_and_decodes_block_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::with_block_options(
            1024,
            dir.path(),
            CacheBlockOptions {
                compression: CacheCompression::Zstd { level: 1 },
                min_compress_bytes: 16,
            },
        );
        let key = CacheKey::string(1, "compressible");
        let value = vec![b'x'; 4096];

        cache.put(key.clone(), value.clone()).unwrap();
        cache.clear_memory_for_test();

        assert_eq!(cache.get(&key).unwrap(), Some(value));
        let stats = cache.stats();
        assert_eq!(stats.compressed_puts, 1);
        assert_eq!(stats.compressed_hits, 1);
        assert!(stats.compression_bytes_saved > 0);
    }

    #[test]
    fn disk_cache_can_read_legacy_raw_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let key = CacheKey::string(1, "legacy");
        let legacy_path = {
            let inner = cache.inner.read().expect("cache lock poisoned");
            inner.disk_path(&key)
        };
        fs::create_dir_all(legacy_path.parent().unwrap()).unwrap();
        fs::write(&legacy_path, b"legacy-value").unwrap();

        assert_eq!(cache.get(&key).unwrap(), Some(b"legacy-value".to_vec()));
    }
}
