use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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
    #[serde(default)]
    pub zero_copy_handle_hits: u64,
    #[serde(default)]
    pub zero_copy_handle_misses: u64,
    #[serde(default)]
    pub async_writeback_enqueued: u64,
    #[serde(default)]
    pub async_writeback_drained: u64,
    #[serde(default)]
    pub async_writeback_backpressure_rejections: u64,
    #[serde(default)]
    pub get_latency_samples: u64,
    #[serde(default)]
    pub put_latency_samples: u64,
    #[serde(default)]
    pub get_latency_total_micros: u64,
    #[serde(default)]
    pub put_latency_total_micros: u64,
    #[serde(default)]
    pub get_latency_max_micros: u64,
    #[serde(default)]
    pub put_latency_max_micros: u64,
    pub compressed_puts: u64,
    pub compressed_hits: u64,
    pub compression_bytes_saved: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheTier {
    Memory,
    Pmem,
    Ssd,
    Reject,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheAdmissionReason {
    HotPage,
    HotObject,
    WarmSlot,
    PersistentMemory,
    LargeColdBlock,
    Oversize,
    MemoryOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheBlockKind {
    Page,
    Object,
    Index,
    Oplog,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheAdmissionRequest {
    pub block_kind: CacheBlockKind,
    pub shard_id: ShardId,
    #[serde(default)]
    pub routing_slot: Option<u32>,
    pub block_bytes: usize,
    #[serde(default)]
    pub hotness: u32,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheTieringPolicy {
    pub memory_capacity_bytes: usize,
    #[serde(default)]
    pub pmem_capacity_bytes: usize,
    pub ssd_capacity_bytes: usize,
    pub memory_hotness_threshold: u32,
    #[serde(default)]
    pub pmem_admit_hotness_threshold: u32,
    pub ssd_admit_hotness_threshold: u32,
    pub max_memory_block_bytes: usize,
    #[serde(default)]
    pub max_pmem_block_bytes: usize,
    pub max_ssd_block_bytes: usize,
}

impl Default for CacheTieringPolicy {
    fn default() -> Self {
        Self {
            memory_capacity_bytes: 64 * 1024 * 1024,
            pmem_capacity_bytes: 512 * 1024 * 1024,
            ssd_capacity_bytes: 16 * 1024 * 1024 * 1024,
            memory_hotness_threshold: 8,
            pmem_admit_hotness_threshold: 4,
            ssd_admit_hotness_threshold: 2,
            max_memory_block_bytes: 1024 * 1024,
            max_pmem_block_bytes: 4 * 1024 * 1024,
            max_ssd_block_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheAdmissionDecision {
    pub tier: CacheTier,
    pub reason: CacheAdmissionReason,
    pub admit_memory: bool,
    #[serde(default)]
    pub admit_pmem: bool,
    pub admit_ssd: bool,
}

impl CacheTieringPolicy {
    pub fn decide(&self, request: &CacheAdmissionRequest) -> CacheAdmissionDecision {
        if request.block_bytes > self.max_ssd_block_bytes
            || request.block_bytes > self.ssd_capacity_bytes
        {
            return CacheAdmissionDecision {
                tier: CacheTier::Reject,
                reason: CacheAdmissionReason::Oversize,
                admit_memory: false,
                admit_pmem: false,
                admit_ssd: false,
            };
        }
        if request.pinned
            || (request.hotness >= self.memory_hotness_threshold
                && request.block_bytes <= self.max_memory_block_bytes
                && request.block_bytes <= self.memory_capacity_bytes)
        {
            return CacheAdmissionDecision {
                tier: CacheTier::Memory,
                reason: if matches!(request.block_kind, CacheBlockKind::Page) {
                    CacheAdmissionReason::HotPage
                } else {
                    CacheAdmissionReason::HotObject
                },
                admit_memory: true,
                admit_pmem: true,
                admit_ssd: true,
            };
        }
        if self.pmem_capacity_bytes > 0
            && request.hotness >= self.pmem_admit_hotness_threshold
            && request.block_bytes <= self.max_pmem_block_bytes
            && request.block_bytes <= self.pmem_capacity_bytes
        {
            return CacheAdmissionDecision {
                tier: CacheTier::Pmem,
                reason: CacheAdmissionReason::PersistentMemory,
                admit_memory: false,
                admit_pmem: true,
                admit_ssd: true,
            };
        }
        if request.routing_slot.is_some()
            && request.hotness >= self.ssd_admit_hotness_threshold
            && request.block_bytes <= self.max_ssd_block_bytes
        {
            return CacheAdmissionDecision {
                tier: CacheTier::Ssd,
                reason: CacheAdmissionReason::WarmSlot,
                admit_memory: false,
                admit_pmem: false,
                admit_ssd: true,
            };
        }
        if request.block_bytes > self.max_memory_block_bytes
            || request.hotness >= self.ssd_admit_hotness_threshold
        {
            return CacheAdmissionDecision {
                tier: CacheTier::Ssd,
                reason: CacheAdmissionReason::LargeColdBlock,
                admit_memory: false,
                admit_pmem: false,
                admit_ssd: true,
            };
        }
        CacheAdmissionDecision {
            tier: CacheTier::Memory,
            reason: CacheAdmissionReason::MemoryOnly,
            admit_memory: true,
            admit_pmem: false,
            admit_ssd: false,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePressureValidationReport {
    pub iterations: usize,
    pub memory_admitted: u64,
    pub pmem_admitted: u64,
    pub ssd_admitted: u64,
    pub rejected: u64,
    pub observed_evictions: u64,
    pub observed_disk_refills: u64,
    pub passed: bool,
    pub reasons: Vec<String>,
}

pub fn validate_cache_pressure_policy(
    policy: CacheTieringPolicy,
    requests: &[CacheAdmissionRequest],
    stats: CacheStats,
) -> CachePressureValidationReport {
    let mut report = CachePressureValidationReport {
        iterations: requests.len(),
        observed_evictions: stats.memory_evictions,
        observed_disk_refills: stats.disk_hits,
        ..CachePressureValidationReport::default()
    };
    for request in requests {
        match policy.decide(request).tier {
            CacheTier::Memory => report.memory_admitted += 1,
            CacheTier::Pmem => report.pmem_admitted += 1,
            CacheTier::Ssd => report.ssd_admitted += 1,
            CacheTier::Reject => report.rejected += 1,
        }
    }
    if report.memory_admitted == 0 {
        report.reasons.push("missing_memory_admission".to_string());
    }
    if report.ssd_admitted == 0 {
        report.reasons.push("missing_ssd_admission".to_string());
    }
    if policy.pmem_capacity_bytes > 0 && report.pmem_admitted == 0 {
        report.reasons.push("missing_pmem_admission".to_string());
    }
    if report.rejected == 0 {
        report.reasons.push("missing_rejection_case".to_string());
    }
    if stats.memory_evictions == 0 {
        report
            .reasons
            .push("missing_eviction_observation".to_string());
    }
    if stats.disk_hits == 0 {
        report
            .reasons
            .push("missing_disk_refill_observation".to_string());
    }
    report.passed = report.reasons.is_empty();
    report
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
pub struct CachePinnedHandle {
    pub key: CacheKey,
    pub value: Arc<[u8]>,
}

impl CachePinnedHandle {
    pub fn as_slice(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheWritebackJob {
    pub key: CacheKey,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheWritebackDrainReport {
    pub requested: usize,
    pub drained: usize,
    pub remaining: usize,
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
    memory: HashMap<CacheKey, Arc<[u8]>>,
    disk_index: HashMap<CacheKey, u64>,
    pinned: HashSet<CacheKey>,
    order: VecDeque<CacheKey>,
    async_writeback_queue: VecDeque<CacheWritebackJob>,
    max_async_writeback_queue: usize,
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
                async_writeback_queue: VecDeque::new(),
                max_async_writeback_queue: 1024,
                stats: CacheStats::default(),
            })),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Result<Option<Vec<u8>>, CacheError> {
        let started = Instant::now();
        {
            let mut inner = self.inner.write().expect("cache lock poisoned");
            if let Some(value) = inner.memory.get(key).cloned() {
                inner.stats.memory_hits += 1;
                inner.record_get_latency(started);
                return Ok(Some(value.to_vec()));
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
                inner.record_get_latency(started);
                Ok(Some(decoded))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut inner = self.inner.write().expect("cache lock poisoned");
                inner.stats.misses += 1;
                inner.record_get_latency(started);
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
        value.map(|value| value.to_vec())
    }

    pub fn get_pinned_handle(
        &self,
        key: &CacheKey,
    ) -> Result<Option<CachePinnedHandle>, CacheError> {
        let started = Instant::now();
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if let Some(value) = inner.memory.get(key).cloned() {
            inner.pinned.insert(key.clone());
            inner.stats.zero_copy_handle_hits = inner.stats.zero_copy_handle_hits.saturating_add(1);
            inner.stats.pin_operations = inner.stats.pin_operations.saturating_add(1);
            inner.refresh_pin_stats();
            inner.record_get_latency(started);
            return Ok(Some(CachePinnedHandle {
                key: key.clone(),
                value,
            }));
        }
        inner.stats.zero_copy_handle_misses = inner.stats.zero_copy_handle_misses.saturating_add(1);
        inner.record_get_latency(started);
        Ok(None)
    }

    pub fn put(&self, key: CacheKey, value: Vec<u8>) -> Result<(), CacheError> {
        let started = Instant::now();
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
        inner.record_put_latency(started);
        Ok(())
    }

    pub fn put_memory_only(&self, key: CacheKey, value: Vec<u8>) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.stats.puts += 1;
        if !inner.put_memory(key, value) {
            inner.stats.refill_failures += 1;
        }
    }

    pub fn enqueue_async_writeback(
        &self,
        key: CacheKey,
        value: Vec<u8>,
    ) -> Result<(), CacheWritebackJob> {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if inner.async_writeback_queue.len() >= inner.max_async_writeback_queue {
            inner.stats.async_writeback_backpressure_rejections = inner
                .stats
                .async_writeback_backpressure_rejections
                .saturating_add(1);
            return Err(CacheWritebackJob { key, value });
        }
        inner
            .async_writeback_queue
            .push_back(CacheWritebackJob { key, value });
        inner.stats.async_writeback_enqueued =
            inner.stats.async_writeback_enqueued.saturating_add(1);
        Ok(())
    }

    pub fn drain_async_writeback(
        &self,
        max_jobs: usize,
    ) -> Result<CacheWritebackDrainReport, CacheError> {
        let jobs = {
            let mut inner = self.inner.write().expect("cache lock poisoned");
            let mut jobs = Vec::new();
            for _ in 0..max_jobs {
                let Some(job) = inner.async_writeback_queue.pop_front() else {
                    break;
                };
                jobs.push(job);
            }
            jobs
        };
        let drained = jobs.len();
        for job in jobs {
            self.put(job.key, job.value)?;
        }
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.stats.async_writeback_drained = inner
            .stats
            .async_writeback_drained
            .saturating_add(drained as u64);
        Ok(CacheWritebackDrainReport {
            requested: max_jobs,
            drained,
            remaining: inner.async_writeback_queue.len(),
        })
    }

    pub fn set_async_writeback_queue_limit_for_test(&self, limit: usize) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.max_async_writeback_queue = limit;
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
        let value = Arc::<[u8]>::from(value);
        if let Some(old) = self.memory.insert(key.clone(), Arc::clone(&value)) {
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

    fn record_get_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.stats.get_latency_samples = self.stats.get_latency_samples.saturating_add(1);
        self.stats.get_latency_total_micros =
            self.stats.get_latency_total_micros.saturating_add(micros);
        self.stats.get_latency_max_micros = self.stats.get_latency_max_micros.max(micros);
    }

    fn record_put_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.stats.put_latency_samples = self.stats.put_latency_samples.saturating_add(1);
        self.stats.put_latency_total_micros =
            self.stats.put_latency_total_micros.saturating_add(micros);
        self.stats.put_latency_max_micros = self.stats.put_latency_max_micros.max(micros);
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
    fn ssd_cache_tiering_policy_admits_hot_warm_and_rejects_oversize_blocks() {
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 64,
            pmem_capacity_bytes: 256,
            ssd_capacity_bytes: 1024,
            memory_hotness_threshold: 8,
            pmem_admit_hotness_threshold: 5,
            ssd_admit_hotness_threshold: 2,
            max_memory_block_bytes: 32,
            max_pmem_block_bytes: 96,
            max_ssd_block_bytes: 256,
        };
        let hot_page = CacheAdmissionRequest {
            block_kind: CacheBlockKind::Page,
            shard_id: 1,
            routing_slot: Some(9),
            block_bytes: 16,
            hotness: 10,
            pinned: false,
        };
        let warm_slot = CacheAdmissionRequest {
            block_kind: CacheBlockKind::Page,
            shard_id: 1,
            routing_slot: Some(9),
            block_bytes: 128,
            hotness: 3,
            pinned: false,
        };
        let oversize = CacheAdmissionRequest {
            block_kind: CacheBlockKind::Object,
            shard_id: 1,
            routing_slot: None,
            block_bytes: 512,
            hotness: 99,
            pinned: false,
        };

        let hot = policy.decide(&hot_page);
        assert_eq!(hot.tier, CacheTier::Memory);
        assert_eq!(hot.reason, CacheAdmissionReason::HotPage);
        assert!(hot.admit_memory);
        assert!(hot.admit_pmem);
        assert!(hot.admit_ssd);

        let warm = policy.decide(&warm_slot);
        assert_eq!(warm.tier, CacheTier::Ssd);
        assert_eq!(warm.reason, CacheAdmissionReason::WarmSlot);
        assert!(!warm.admit_memory);
        assert!(!warm.admit_pmem);
        assert!(warm.admit_ssd);

        let rejected = policy.decide(&oversize);
        assert_eq!(rejected.tier, CacheTier::Reject);
        assert_eq!(rejected.reason, CacheAdmissionReason::Oversize);
        assert!(!rejected.admit_memory);
        assert!(!rejected.admit_pmem);
        assert!(!rejected.admit_ssd);

        let pmem = policy.decide(&CacheAdmissionRequest {
            block_kind: CacheBlockKind::Index,
            shard_id: 1,
            routing_slot: Some(9),
            block_bytes: 64,
            hotness: 5,
            pinned: false,
        });
        assert_eq!(pmem.tier, CacheTier::Pmem);
        assert_eq!(pmem.reason, CacheAdmissionReason::PersistentMemory);
        assert!(!pmem.admit_memory);
        assert!(pmem.admit_pmem);
        assert!(pmem.admit_ssd);
    }

    #[test]
    fn cache_pressure_policy_report_requires_admission_eviction_and_refill_evidence() {
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 64,
            pmem_capacity_bytes: 256,
            ssd_capacity_bytes: 1024,
            memory_hotness_threshold: 8,
            pmem_admit_hotness_threshold: 5,
            ssd_admit_hotness_threshold: 2,
            max_memory_block_bytes: 32,
            max_pmem_block_bytes: 128,
            max_ssd_block_bytes: 256,
        };
        let requests = vec![
            CacheAdmissionRequest {
                block_kind: CacheBlockKind::Page,
                shard_id: 1,
                routing_slot: Some(1),
                block_bytes: 8,
                hotness: 10,
                pinned: false,
            },
            CacheAdmissionRequest {
                block_kind: CacheBlockKind::Index,
                shard_id: 1,
                routing_slot: Some(1),
                block_bytes: 64,
                hotness: 5,
                pinned: false,
            },
            CacheAdmissionRequest {
                block_kind: CacheBlockKind::Index,
                shard_id: 1,
                routing_slot: Some(1),
                block_bytes: 96,
                hotness: 2,
                pinned: false,
            },
            CacheAdmissionRequest {
                block_kind: CacheBlockKind::Oplog,
                shard_id: 1,
                routing_slot: None,
                block_bytes: 512,
                hotness: 10,
                pinned: false,
            },
        ];
        let passing = validate_cache_pressure_policy(
            policy,
            &requests,
            CacheStats {
                memory_evictions: 4,
                disk_hits: 7,
                ..CacheStats::default()
            },
        );
        assert!(passing.passed, "{passing:?}");
        assert_eq!(passing.memory_admitted, 1);
        assert_eq!(passing.pmem_admitted, 1);
        assert_eq!(passing.ssd_admitted, 1);
        assert_eq!(passing.rejected, 1);

        let failing = validate_cache_pressure_policy(policy, &requests[..1], CacheStats::default());
        assert!(!failing.passed);
        assert!(failing
            .reasons
            .contains(&"missing_ssd_admission".to_string()));
        assert!(failing
            .reasons
            .contains(&"missing_eviction_observation".to_string()));
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

    // shared-corpus: storage_cache_refill
    #[test]
    fn pinned_handle_async_writeback_and_latency_metrics_are_reported() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(64, dir.path());
        let key = CacheKey::string(1, "handle");

        cache.put(key.clone(), b"value".to_vec()).unwrap();
        let handle = cache
            .get_pinned_handle(&key)
            .unwrap()
            .expect("pinned handle should exist");
        assert_eq!(handle.as_slice(), b"value");
        assert_eq!(cache.stats().pinned_entries, 1);
        assert_eq!(cache.stats().zero_copy_handle_hits, 1);

        cache.set_async_writeback_queue_limit_for_test(1);
        cache
            .enqueue_async_writeback(CacheKey::string(1, "async-a"), b"a".to_vec())
            .unwrap();
        assert!(cache
            .enqueue_async_writeback(CacheKey::string(1, "async-b"), b"b".to_vec())
            .is_err());
        let drained = cache.drain_async_writeback(8).unwrap();
        assert_eq!(drained.drained, 1);
        assert_eq!(drained.remaining, 0);

        let stats = cache.stats();
        assert_eq!(stats.async_writeback_enqueued, 1);
        assert_eq!(stats.async_writeback_drained, 1);
        assert_eq!(stats.async_writeback_backpressure_rejections, 1);
        assert!(stats.get_latency_samples > 0);
        assert!(stats.put_latency_samples > 0);
        assert!(stats.get_latency_total_micros >= stats.get_latency_max_micros);
        assert!(stats.put_latency_total_micros >= stats.put_latency_max_micros);
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
