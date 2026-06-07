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
    pub memory_bytes: u64,
    pub disk_bytes: u64,
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
    memory: HashMap<CacheKey, Vec<u8>>,
    order: VecDeque<CacheKey>,
    stats: CacheStats,
}

impl MultiLayerCache {
    pub fn new(memory_capacity_bytes: usize, disk_dir: impl Into<PathBuf>) -> Self {
        let disk_dir = disk_dir.into();
        let _ = fs::create_dir_all(&disk_dir);
        Self {
            inner: Arc::new(RwLock::new(CacheInner {
                memory_capacity_bytes,
                memory_bytes: 0,
                disk_dir,
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
            Ok(value) => {
                let mut inner = self.inner.write().expect("cache lock poisoned");
                inner.stats.disk_hits += 1;
                inner.put_memory(key.clone(), value.clone());
                Ok(Some(value))
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
        fs::write(path, &value)?;
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
        "temporalstore-single-node-{kind}-{}-{nanos}-{counter}",
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
}
