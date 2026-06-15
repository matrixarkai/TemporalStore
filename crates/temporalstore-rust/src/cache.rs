use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::Write;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    pub fn page_with_slot(
        shard_id: ShardId,
        page_segment_id: u64,
        offset: u64,
        length: u64,
        routing_slot: Option<u32>,
    ) -> Self {
        let selector = match routing_slot {
            Some(slot) => format!("slot-{slot}:{offset}:{length}"),
            None => format!("{offset}:{length}"),
        };
        Self {
            shard_id,
            record_key: format!("segment-{page_segment_id:020}"),
            namespace: "page".to_string(),
            selector,
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
    #[serde(default)]
    pub memory_admission_accepted: u64,
    #[serde(default)]
    pub memory_admission_rejected: u64,
    #[serde(default)]
    pub memory_fills: u64,
    #[serde(default)]
    pub disk_fills: u64,
    #[serde(default)]
    pub refill_failures: u64,
    #[serde(default)]
    pub eviction_capacity: u64,
    #[serde(default)]
    pub eviction_oversize: u64,
    #[serde(default)]
    pub pinned_entries: u64,
    #[serde(default)]
    pub pinned_bytes: u64,
    #[serde(default)]
    pub pin_operations: u64,
    #[serde(default)]
    pub unpin_operations: u64,
    #[serde(default)]
    pub eviction_pinned_skips: u64,
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntryInfo {
    pub shard_id: ShardId,
    pub namespace: String,
    pub record_key: String,
    pub selector: String,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    #[serde(default)]
    pub pinned: bool,
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
    disk_index: HashMap<CacheKey, u64>,
    pinned: HashSet<CacheKey>,
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
                disk_index: HashMap::new(),
                pinned: HashSet::new(),
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
                if !inner.put_memory(key.clone(), decoded.clone()) {
                    inner.stats.refill_failures += 1;
                }
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
        let block_len = block.len();
        write_cache_block_atomic(&path, &block)?;
        inner.stats.puts += 1;
        inner.stats.disk_fills += 1;
        inner.stats.disk_bytes = inner.stats.disk_bytes.saturating_add(block_len as u64);
        inner.disk_index.insert(key.clone(), block_len as u64);
        inner.put_memory(key, value);
        Ok(())
    }

    pub fn put_memory_only(&self, key: CacheKey, value: Vec<u8>) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.stats.puts += 1;
        if !inner.put_memory(key, value) {
            inner.stats.refill_failures += 1;
        }
    }

    pub fn pin(&self, key: CacheKey) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.pinned.insert(key);
        inner.stats.pin_operations = inner.stats.pin_operations.saturating_add(1);
        inner.refresh_pin_stats();
    }

    pub fn unpin(&self, key: &CacheKey) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if inner.pinned.remove(key) {
            inner.stats.unpin_operations = inner.stats.unpin_operations.saturating_add(1);
        }
        inner.refresh_pin_stats();
    }

    pub fn invalidate(&self, key: &CacheKey) -> Result<(), CacheError> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if let Some(value) = inner.memory.remove(key) {
            inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
        }
        let _ = fs::remove_file(inner.disk_path(key));
        inner.disk_index.remove(key);
        inner.pinned.remove(key);
        inner.stats.invalidations += 1;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.refresh_pin_stats();
        Ok(())
    }

    pub fn invalidate_memory_only(&self, key: &CacheKey) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if let Some(value) = inner.memory.remove(key) {
            inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
        }
        inner.pinned.remove(key);
        inner.stats.invalidations += 1;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.refresh_pin_stats();
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
                .chain(inner.disk_index.keys())
                .filter(|key| {
                    key.shard_id == shard_id
                        && key.namespace == namespace
                        && key.record_key == record_key
                })
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
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
        inner.disk_index.retain(|key, _| key.shard_id != shard_id);
        inner.pinned.retain(|key| key.shard_id != shard_id);

        let shard_disk_dir = inner.disk_dir.join(format!("shard-{shard_id}"));
        let disk_bytes_before = dir_size(&shard_disk_dir).unwrap_or_default();
        match fs::remove_dir_all(&shard_disk_dir) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(CacheError::Io(err)),
        }
        inner.stats.invalidations += memory_entries_removed as u64;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.disk_bytes = inner.stats.disk_bytes.saturating_sub(disk_bytes_before);
        inner.refresh_pin_stats();
        Ok(CacheGcReport {
            shard_id,
            memory_entries_removed,
            disk_bytes_removed: disk_bytes_before,
        })
    }

    pub fn invalidate_slot(
        &self,
        shard_id: ShardId,
        routing_slot: u32,
    ) -> Result<CacheGcReport, CacheError> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        let prefix = format!("slot-{routing_slot}:");
        let slot_keys = inner
            .memory
            .keys()
            .chain(inner.disk_index.keys())
            .filter(|key| key.shard_id == shard_id && key.selector.starts_with(&prefix))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let memory_entries_removed = slot_keys
            .iter()
            .filter(|key| inner.memory.contains_key(*key))
            .count();
        let mut disk_bytes_removed = 0u64;
        for key in &slot_keys {
            if let Some(value) = inner.memory.remove(key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
            }
            let path = inner.disk_path(key);
            disk_bytes_removed = disk_bytes_removed.saturating_add(
                inner
                    .disk_index
                    .remove(key)
                    .or_else(|| path.metadata().ok().map(|metadata| metadata.len()))
                    .unwrap_or_default(),
            );
            let _ = fs::remove_file(path);
            inner.pinned.remove(key);
        }
        inner
            .order
            .retain(|key| !(key.shard_id == shard_id && key.selector.starts_with(&prefix)));
        inner.stats.invalidations = inner
            .stats
            .invalidations
            .saturating_add(memory_entries_removed as u64);
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.disk_bytes = dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes);
        inner.refresh_pin_stats();
        Ok(CacheGcReport {
            shard_id,
            memory_entries_removed,
            disk_bytes_removed,
        })
    }

    pub fn entries_for_shard(&self, shard_id: ShardId) -> Vec<CacheEntryInfo> {
        let inner = self.inner.read().expect("cache lock poisoned");
        let keys = inner
            .memory
            .keys()
            .chain(inner.disk_index.keys())
            .filter(|key| key.shard_id == shard_id)
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let mut entries = keys
            .into_iter()
            .map(|key| {
                let pinned = inner.pinned.contains(&key);
                let memory_bytes = inner
                    .memory
                    .get(&key)
                    .map(|value| value.len() as u64)
                    .unwrap_or_default();
                let disk_bytes = inner.disk_index.get(&key).copied().unwrap_or_else(|| {
                    inner
                        .disk_path(&key)
                        .metadata()
                        .map(|metadata| metadata.len())
                        .unwrap_or_default()
                });
                CacheEntryInfo {
                    shard_id: key.shard_id,
                    namespace: key.namespace,
                    record_key: key.record_key,
                    selector: key.selector,
                    memory_bytes,
                    disk_bytes,
                    pinned,
                }
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then(left.record_key.cmp(&right.record_key))
                .then(left.selector.cmp(&right.selector))
        });
        entries
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.read().expect("cache lock poisoned");
        CacheStats {
            memory_bytes: inner.memory_bytes as u64,
            disk_bytes: dir_size(&inner.disk_dir).unwrap_or(inner.stats.disk_bytes),
            pinned_entries: inner.pinned.len() as u64,
            pinned_bytes: inner.pinned_memory_bytes(),
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
        inner.refresh_pin_stats();
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

fn write_cache_block_atomic(path: &Path, block: &[u8]) -> Result<(), CacheError> {
    let temp_path = path.with_extension(format!(
        "cache_block.tmp.{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default()
    ));
    {
        let mut temp = File::create(&temp_path)?;
        temp.write_all(block)?;
        temp.flush()?;
        temp.sync_all()?;
    }
    fs::rename(&temp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = File::open(parent) {
            dir.sync_all()?;
        }
    }
    Ok(())
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

    fn put_memory(&mut self, key: CacheKey, value: Vec<u8>) -> bool {
        if self.memory_capacity_bytes == 0 || value.len() > self.memory_capacity_bytes {
            self.stats.memory_admission_rejected += 1;
            self.stats.eviction_oversize += 1;
            return false;
        }
        self.stats.memory_admission_accepted += 1;
        self.stats.memory_fills += 1;
        if let Some(old) = self.memory.insert(key.clone(), value.clone()) {
            self.memory_bytes = self.memory_bytes.saturating_sub(old.len());
        } else {
            self.order.push_back(key);
        }
        self.memory_bytes += value.len();
        while self.memory_bytes > self.memory_capacity_bytes {
            let mut evicted = false;
            let order_len = self.order.len();
            for _ in 0..order_len {
                let Some(oldest) = self.order.pop_front() else {
                    break;
                };
                if self.pinned.contains(&oldest) {
                    self.stats.eviction_pinned_skips =
                        self.stats.eviction_pinned_skips.saturating_add(1);
                    self.order.push_back(oldest);
                    continue;
                }
                if let Some(old_value) = self.memory.remove(&oldest) {
                    self.memory_bytes = self.memory_bytes.saturating_sub(old_value.len());
                    self.stats.memory_evictions += 1;
                    self.stats.eviction_capacity += 1;
                    evicted = true;
                    break;
                }
            }
            if !evicted {
                self.stats.eviction_pinned_skips =
                    self.stats.eviction_pinned_skips.saturating_add(1);
                break;
            }
        }
        self.stats.memory_bytes = self.memory_bytes as u64;
        self.refresh_pin_stats();
        true
    }

    fn pinned_memory_bytes(&self) -> u64 {
        self.pinned
            .iter()
            .filter_map(|key| self.memory.get(key))
            .map(|value| value.len() as u64)
            .sum()
    }

    fn refresh_pin_stats(&mut self) {
        self.stats.pinned_entries = self.pinned.len() as u64;
        self.stats.pinned_bytes = self.pinned_memory_bytes();
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
        assert!(cache.stats().eviction_capacity >= 1);
    }

    #[test]
    fn cache_records_memory_admission_rejection_for_oversized_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(4, dir.path());
        let key = CacheKey::string(1, "oversized");

        cache.put(key.clone(), b"too-large".to_vec()).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.disk_fills, 1);
        assert_eq!(stats.memory_admission_rejected, 1);
        assert_eq!(stats.eviction_oversize, 1);
        assert_eq!(stats.refill_failures, 0);
        assert_eq!(cache.get_memory(&key), None);
        assert_eq!(cache.get(&key).unwrap(), Some(b"too-large".to_vec()));
        assert_eq!(cache.stats().refill_failures, 1);
    }

    #[test]
    fn pinned_memory_entries_survive_capacity_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(10, dir.path());
        let pinned = CacheKey::string(1, "pinned");
        let first = CacheKey::string(1, "first");
        let second = CacheKey::string(1, "second");

        cache.put(pinned.clone(), b"pin".to_vec()).unwrap();
        cache.pin(pinned.clone());
        cache.put(first.clone(), b"11111".to_vec()).unwrap();
        cache.put(second.clone(), b"22222".to_vec()).unwrap();

        assert_eq!(cache.get_memory(&pinned), Some(b"pin".to_vec()));
        assert_eq!(cache.stats().pinned_entries, 1);
        assert_eq!(cache.stats().pinned_bytes, 3);
        assert!(cache.stats().eviction_pinned_skips > 0);

        cache.unpin(&pinned);
        assert_eq!(cache.stats().pinned_entries, 0);
    }

    #[test]
    fn invalidation_clears_pinned_state() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(16, dir.path());
        let key = CacheKey::page_with_slot(1, 10, 0, 4, Some(7));

        cache.put(key.clone(), b"page".to_vec()).unwrap();
        cache.pin(key.clone());
        assert_eq!(cache.stats().pinned_entries, 1);

        cache.invalidate(&key).unwrap();
        assert_eq!(cache.stats().pinned_entries, 0);
        assert_eq!(cache.stats().pinned_bytes, 0);
        assert!(cache.entries_for_shard(1).is_empty());
    }

    #[test]
    fn cache_inspection_and_slot_invalidation_are_slot_aware() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let slot_five = CacheKey::page_with_slot(1, 10, 20, 4, Some(5));
        let slot_six = CacheKey::page_with_slot(1, 11, 30, 4, Some(6));

        cache.put(slot_five.clone(), b"five".to_vec()).unwrap();
        cache.put(slot_six.clone(), b"six!".to_vec()).unwrap();

        let entries = cache.entries_for_shard(1);
        assert_eq!(entries.len(), 2);
        assert!(entries
            .iter()
            .any(|entry| entry.selector.starts_with("slot-5:")));

        let report = cache.invalidate_slot(1, 5).unwrap();
        assert_eq!(report.memory_entries_removed, 1);
        assert!(report.disk_bytes_removed > 0);
        assert_eq!(cache.get(&slot_five).unwrap(), None);
        assert_eq!(cache.get(&slot_six).unwrap(), Some(b"six!".to_vec()));
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
