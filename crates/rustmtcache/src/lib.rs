use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable shard identifier used to scope cache keys.
pub type ShardId = u64;

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
    pub pmem_hits: u64,
    #[serde(default)]
    pub pmem_fills: u64,
    #[serde(default)]
    pub pmem_evictions: u64,
    #[serde(default)]
    pub pmem_admission_accepted: u64,
    #[serde(default)]
    pub pmem_admission_rejected: u64,
    #[serde(default)]
    pub pmem_eviction_capacity: u64,
    #[serde(default)]
    pub pmem_eviction_pinned_skips: u64,
    #[serde(default)]
    pub memory_admission_accepted: u64,
    #[serde(default)]
    pub memory_admission_rejected: u64,
    #[serde(default)]
    pub memory_fills: u64,
    #[serde(default)]
    pub disk_fills: u64,
    #[serde(default)]
    pub ssd_admission_accepted: u64,
    #[serde(default)]
    pub ssd_admission_rejected: u64,
    #[serde(default)]
    pub ssd_evictions: u64,
    #[serde(default)]
    pub ssd_eviction_capacity: u64,
    #[serde(default)]
    pub ssd_eviction_pinned_skips: u64,
    #[serde(default)]
    pub ssd_oversize_rejections: u64,
    #[serde(default)]
    pub ssd_write_through_admissions: u64,
    #[serde(default)]
    pub hotness_promotions: u64,
    #[serde(default)]
    pub refill_failures: u64,
    #[serde(default)]
    pub eviction_capacity: u64,
    #[serde(default)]
    pub eviction_oversize: u64,
    #[serde(default)]
    pub eviction_cold: u64,
    #[serde(default)]
    pub eviction_low_hit: u64,
    #[serde(default)]
    pub eviction_stale: u64,
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
    pub writeback_backpressure_events: u64,
    #[serde(default)]
    pub async_writeback_queue_depth: u64,
    #[serde(default)]
    pub async_writeback_queue_bytes: u64,
    #[serde(default)]
    pub async_writeback_max_queue_depth: u64,
    #[serde(default)]
    pub async_writeback_max_queue_bytes: u64,
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
    #[serde(default)]
    pub get_latency_le_10us: u64,
    #[serde(default)]
    pub get_latency_le_100us: u64,
    #[serde(default)]
    pub get_latency_le_1ms: u64,
    #[serde(default)]
    pub get_latency_le_10ms: u64,
    #[serde(default)]
    pub get_latency_gt_10ms: u64,
    #[serde(default)]
    pub put_latency_le_10us: u64,
    #[serde(default)]
    pub put_latency_le_100us: u64,
    #[serde(default)]
    pub put_latency_le_1ms: u64,
    #[serde(default)]
    pub put_latency_le_10ms: u64,
    #[serde(default)]
    pub put_latency_gt_10ms: u64,
    #[serde(default)]
    pub read_through_latency_samples: u64,
    #[serde(default)]
    pub read_through_latency_le_10us: u64,
    #[serde(default)]
    pub read_through_latency_le_100us: u64,
    #[serde(default)]
    pub read_through_latency_le_1ms: u64,
    #[serde(default)]
    pub read_through_latency_le_10ms: u64,
    #[serde(default)]
    pub read_through_latency_gt_10ms: u64,
    #[serde(default)]
    pub refill_latency_samples: u64,
    #[serde(default)]
    pub refill_latency_le_10us: u64,
    #[serde(default)]
    pub refill_latency_le_100us: u64,
    #[serde(default)]
    pub refill_latency_le_1ms: u64,
    #[serde(default)]
    pub refill_latency_le_10ms: u64,
    #[serde(default)]
    pub refill_latency_gt_10ms: u64,
    #[serde(default)]
    pub writeback_latency_samples: u64,
    #[serde(default)]
    pub writeback_latency_le_10us: u64,
    #[serde(default)]
    pub writeback_latency_le_100us: u64,
    #[serde(default)]
    pub writeback_latency_le_1ms: u64,
    #[serde(default)]
    pub writeback_latency_le_10ms: u64,
    #[serde(default)]
    pub writeback_latency_gt_10ms: u64,
    #[serde(default)]
    pub eviction_latency_samples: u64,
    #[serde(default)]
    pub eviction_latency_le_10us: u64,
    #[serde(default)]
    pub eviction_latency_le_100us: u64,
    #[serde(default)]
    pub eviction_latency_le_1ms: u64,
    #[serde(default)]
    pub eviction_latency_le_10ms: u64,
    #[serde(default)]
    pub eviction_latency_gt_10ms: u64,
    #[serde(default)]
    pub compaction_latency_samples: u64,
    #[serde(default)]
    pub compaction_latency_le_10us: u64,
    #[serde(default)]
    pub compaction_latency_le_100us: u64,
    #[serde(default)]
    pub compaction_latency_le_1ms: u64,
    #[serde(default)]
    pub compaction_latency_le_10ms: u64,
    #[serde(default)]
    pub compaction_latency_gt_10ms: u64,
    #[serde(default)]
    pub eviction_sampled_groups: u64,
    #[serde(default)]
    pub memory_slot_evictions: u64,
    #[serde(default)]
    pub ssd_slot_evictions: u64,
    #[serde(default)]
    pub ssd_eviction_cold: u64,
    #[serde(default)]
    pub ssd_eviction_low_hit: u64,
    #[serde(default)]
    pub ssd_eviction_stale: u64,
    pub compressed_puts: u64,
    pub compressed_hits: u64,
    pub compression_bytes_saved: u64,
    #[serde(default)]
    pub get_latency_count: u64,
    #[serde(default)]
    pub get_latency_total_us: u64,
    #[serde(default)]
    pub get_latency_max_us: u64,
    #[serde(default)]
    pub put_latency_count: u64,
    #[serde(default)]
    pub put_latency_total_us: u64,
    #[serde(default)]
    pub put_latency_max_us: u64,
    pub memory_bytes: u64,
    #[serde(default)]
    pub pmem_bytes: u64,
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
    #[serde(default = "default_ssd_write_through")]
    pub ssd_write_through: bool,
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
            ssd_write_through: true,
        }
    }
}

fn default_ssd_write_through() -> bool {
    true
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
    #[serde(default)]
    pub observed_ssd_evictions: u64,
    #[serde(default)]
    pub observed_hotness_promotions: u64,
    pub passed: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheReplacementPolicySoakReport {
    pub iterations: usize,
    pub hot_key_count: usize,
    pub cold_key_count: usize,
    pub hot_memory_survivors: usize,
    pub cold_memory_survivors: usize,
    pub pinned_memory_survived: bool,
    pub restart_disk_refill_ready: bool,
    pub observed_evictions: u64,
    pub observed_pinned_skips: u64,
    pub observed_disk_refills: u64,
    #[serde(default)]
    pub observed_async_writeback_backpressure: u64,
    #[serde(default)]
    pub async_writeback_max_queue_depth: u64,
    #[serde(default)]
    pub async_writeback_max_queue_bytes: u64,
    pub get_latency_samples: u64,
    pub put_latency_samples: u64,
    #[serde(default)]
    pub read_through_latency_samples: u64,
    #[serde(default)]
    pub refill_latency_samples: u64,
    #[serde(default)]
    pub writeback_latency_samples: u64,
    #[serde(default)]
    pub eviction_latency_samples: u64,
    #[serde(default)]
    pub compaction_latency_samples: u64,
    #[serde(default)]
    pub read_through_latency_bucketed: bool,
    #[serde(default)]
    pub refill_latency_bucketed: bool,
    #[serde(default)]
    pub writeback_latency_bucketed: bool,
    #[serde(default)]
    pub eviction_latency_bucketed: bool,
    #[serde(default)]
    pub compaction_latency_bucketed: bool,
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
        observed_ssd_evictions: stats.ssd_evictions,
        observed_hotness_promotions: stats.hotness_promotions,
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

fn latency_bucket_count(
    le_10us: u64,
    le_100us: u64,
    le_1ms: u64,
    le_10ms: u64,
    gt_10ms: u64,
) -> u64 {
    le_10us
        .saturating_add(le_100us)
        .saturating_add(le_1ms)
        .saturating_add(le_10ms)
        .saturating_add(gt_10ms)
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
    #[serde(default)]
    pub pmem_bytes: u64,
    pub disk_bytes: u64,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub block_kind: Option<CacheBlockKind>,
    #[serde(default)]
    pub routing_slot: Option<u32>,
    #[serde(default)]
    pub hotness: u32,
    #[serde(default)]
    pub hits: u64,
    #[serde(default)]
    pub last_access_epoch: u64,
    #[serde(default)]
    pub admission_reason: Option<CacheAdmissionReason>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEvictionReport {
    pub memory_evictions: u64,
    pub memory_capacity_evictions: u64,
    pub memory_cold_evictions: u64,
    pub memory_low_hit_evictions: u64,
    pub memory_stale_evictions: u64,
    pub memory_pinned_skips: u64,
    #[serde(default)]
    pub pmem_evictions: u64,
    #[serde(default)]
    pub pmem_capacity_evictions: u64,
    #[serde(default)]
    pub pmem_pinned_skips: u64,
    pub ssd_evictions: u64,
    pub ssd_capacity_evictions: u64,
    pub ssd_cold_evictions: u64,
    pub ssd_low_hit_evictions: u64,
    pub ssd_stale_evictions: u64,
    pub ssd_pinned_skips: u64,
    pub sampled_eviction_groups: u64,
    pub memory_slot_evictions: u64,
    pub ssd_slot_evictions: u64,
    pub replacement_policy: CacheReplacementPolicy,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheWritebackBackpressureReport {
    pub ssd_write_through_enabled: bool,
    pub write_through_admissions: u64,
    pub ssd_admission_rejections: u64,
    pub ssd_evictions: u64,
    pub ssd_oversize_rejections: u64,
    pub backpressure_events: u64,
    pub bounded_queue_ready: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheLatencyMetricsReport {
    pub get_count: u64,
    pub get_avg_us: u64,
    pub get_max_us: u64,
    pub put_count: u64,
    pub put_avg_us: u64,
    pub put_max_us: u64,
    pub histogram_ready: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheReplacementPolicy {
    #[default]
    WeightedHotnessLru,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvictionReason {
    Cold,
    LowHit,
    Stale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EvictionScore {
    hotness: u32,
    hits: u64,
    last_access_epoch: u64,
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
struct SlotEvictionGroup {
    group_score: EvictionScore,
    victim: CacheKey,
    victim_score: EvictionScore,
}

impl SlotEvictionGroup {
    fn new(victim: CacheKey, score: EvictionScore) -> Self {
        Self {
            group_score: score,
            victim,
            victim_score: score,
        }
    }

    fn observe(&mut self, key: CacheKey, score: EvictionScore) {
        self.group_score.hotness = self.group_score.hotness.max(score.hotness);
        self.group_score.hits = self.group_score.hits.saturating_add(score.hits);
        self.group_score.last_access_epoch = self
            .group_score
            .last_access_epoch
            .max(score.last_access_epoch);
        if score < self.victim_score || (score == self.victim_score && key < self.victim) {
            self.victim = key;
            self.victim_score = score;
        }
    }
}

#[derive(Debug, Clone)]
pub struct MultiLayerCache {
    inner: Arc<RwLock<CacheInner>>,
}

#[derive(Debug)]
struct CacheInner {
    memory_capacity_bytes: usize,
    memory_bytes: usize,
    pmem_capacity_bytes: usize,
    pmem_bytes: usize,
    ssd_capacity_bytes: usize,
    ssd_bytes: u64,
    disk_dir: PathBuf,
    tiering_policy: CacheTieringPolicy,
    block_options: CacheBlockOptions,
    memory: HashMap<CacheKey, Arc<[u8]>>,
    pmem: HashMap<CacheKey, Arc<[u8]>>,
    disk_index: HashMap<CacheKey, u64>,
    disk_order: VecDeque<CacheKey>,
    pinned: HashSet<CacheKey>,
    order: VecDeque<CacheKey>,
    pmem_order: VecDeque<CacheKey>,
    async_writeback_queue: VecDeque<CacheWritebackJob>,
    max_async_writeback_queue: usize,
    metadata: HashMap<CacheKey, CacheEntryMeta>,
    access_epoch: u64,
    stats: CacheStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheEntryMeta {
    block_kind: CacheBlockKind,
    routing_slot: Option<u32>,
    hotness: u32,
    hits: u64,
    last_access_epoch: u64,
    admission_reason: CacheAdmissionReason,
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
        let policy = CacheTieringPolicy {
            memory_capacity_bytes,
            ..CacheTieringPolicy::default()
        };
        Self::with_tiering_policy(disk_dir, policy, block_options)
    }

    pub fn with_tiering_policy(
        disk_dir: impl Into<PathBuf>,
        tiering_policy: CacheTieringPolicy,
        block_options: CacheBlockOptions,
    ) -> Self {
        let disk_dir = disk_dir.into();
        let _ = fs::create_dir_all(&disk_dir);
        Self {
            inner: Arc::new(RwLock::new(CacheInner {
                memory_capacity_bytes: tiering_policy.memory_capacity_bytes,
                memory_bytes: 0,
                pmem_capacity_bytes: tiering_policy.pmem_capacity_bytes,
                pmem_bytes: 0,
                ssd_capacity_bytes: tiering_policy.ssd_capacity_bytes,
                ssd_bytes: 0,
                disk_dir,
                tiering_policy,
                block_options,
                memory: HashMap::new(),
                pmem: HashMap::new(),
                disk_index: HashMap::new(),
                disk_order: VecDeque::new(),
                pinned: HashSet::new(),
                order: VecDeque::new(),
                pmem_order: VecDeque::new(),
                async_writeback_queue: VecDeque::new(),
                max_async_writeback_queue: 1024,
                metadata: HashMap::new(),
                access_epoch: 0,
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
                inner.touch_key(key);
                inner.record_hit(key, value.len());
                inner.record_get_latency(started);
                inner.record_read_through_latency(started);
                return Ok(Some(value.to_vec()));
            }
            if let Some(value) = inner.pmem.get(key).cloned() {
                inner.stats.pmem_hits = inner.stats.pmem_hits.saturating_add(1);
                inner.touch_key(key);
                inner.record_hit(key, value.len());
                let decoded = value.to_vec();
                if !inner.put_memory(key.clone(), decoded.clone()) {
                    inner.stats.refill_failures += 1;
                }
                inner.record_get_latency(started);
                inner.record_read_through_latency(started);
                inner.record_refill_latency(started);
                return Ok(Some(decoded));
            }
        }

        let refill_started = Instant::now();
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
                inner.record_hit(key, decoded.len());
                if !inner.put_memory(key.clone(), decoded.clone()) {
                    inner.stats.refill_failures += 1;
                }
                inner.record_get_latency(started);
                inner.record_read_through_latency(started);
                inner.record_refill_latency(refill_started);
                Ok(Some(decoded))
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut inner = self.inner.write().expect("cache lock poisoned");
                inner.stats.misses += 1;
                inner.record_get_latency(started);
                inner.record_read_through_latency(started);
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
            inner.touch_key(key);
            inner.record_hit(
                key,
                value.as_ref().map(|bytes| bytes.len()).unwrap_or_default(),
            );
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
            inner.touch_key(key);
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
        let result = inner.put_with_request(key, value, None);
        inner.record_put_latency(started);
        result
    }

    pub fn put_with_admission(
        &self,
        key: CacheKey,
        value: Vec<u8>,
        request: CacheAdmissionRequest,
    ) -> Result<(), CacheError> {
        let started = Instant::now();
        let mut inner = self.inner.write().expect("cache lock poisoned");
        let result = inner.put_with_request(key, value, Some(request));
        inner.record_put_latency(started);
        result
    }

    pub fn put_memory_only(&self, key: CacheKey, value: Vec<u8>) {
        let started = Instant::now();
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.stats.puts += 1;
        inner.record_metadata(
            &key,
            CacheBlockKind::Other,
            extract_routing_slot(&key),
            value.len(),
            0,
            CacheAdmissionReason::MemoryOnly,
        );
        if !inner.put_memory(key, value) {
            inner.stats.refill_failures += 1;
        }
        inner.record_put_latency(started);
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
            inner.refresh_async_writeback_pressure_stats();
            return Err(CacheWritebackJob { key, value });
        }
        inner
            .async_writeback_queue
            .push_back(CacheWritebackJob { key, value });
        inner.stats.async_writeback_enqueued =
            inner.stats.async_writeback_enqueued.saturating_add(1);
        inner.refresh_async_writeback_pressure_stats();
        Ok(())
    }

    pub fn drain_async_writeback(
        &self,
        max_jobs: usize,
    ) -> Result<CacheWritebackDrainReport, CacheError> {
        let started = Instant::now();
        let jobs = {
            let mut inner = self.inner.write().expect("cache lock poisoned");
            let mut jobs = Vec::new();
            for _ in 0..max_jobs {
                let Some(job) = inner.async_writeback_queue.pop_front() else {
                    break;
                };
                jobs.push(job);
            }
            inner.refresh_async_writeback_pressure_stats();
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
        inner.refresh_async_writeback_pressure_stats();
        inner.record_writeback_latency(started);
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

    pub fn record_compaction_latency_micros(&self, micros: u64) {
        self.inner
            .write()
            .expect("cache lock poisoned")
            .record_compaction_latency_micros(micros);
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
        inner.invalidate_key_locked(key, true);
        Ok(())
    }

    pub fn invalidate_memory_only(&self, key: &CacheKey) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        if let Some(value) = inner.memory.remove(key) {
            inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
        }
        inner.order.retain(|candidate| candidate != key);
        if let Some(value) = inner.pmem.remove(key) {
            inner.pmem_bytes = inner.pmem_bytes.saturating_sub(value.len());
        }
        inner.pmem_order.retain(|candidate| candidate != key);
        inner.pinned.remove(key);
        inner.stats.invalidations += 1;
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.pmem_bytes = inner.pmem_bytes as u64;
        inner.refresh_pin_stats();
    }

    pub fn production_tiering_policy(&self) -> CacheTieringPolicy {
        let inner = self.inner.read().expect("cache lock poisoned");
        inner.tiering_policy
    }

    pub fn update_production_tiering_policy(&self, policy: CacheTieringPolicy) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.memory_capacity_bytes = policy.memory_capacity_bytes;
        inner.pmem_capacity_bytes = policy.pmem_capacity_bytes;
        inner.ssd_capacity_bytes = policy.ssd_capacity_bytes;
        inner.tiering_policy = policy;
        inner.evict_memory_to_capacity();
        inner.evict_pmem_to_capacity();
        inner.evict_ssd_to_capacity();
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.pmem_bytes = inner.pmem_bytes as u64;
        inner.stats.disk_bytes = inner.ssd_bytes;
        inner.refresh_pin_stats();
    }
}

impl CacheInner {
    fn put_with_request(
        &mut self,
        key: CacheKey,
        value: Vec<u8>,
        request: Option<CacheAdmissionRequest>,
    ) -> Result<(), CacheError> {
        let request = request.unwrap_or_else(|| self.default_request(&key, value.len()));
        let decision = self.tiering_policy.decide(&request);
        let admit_ssd = decision.admit_ssd || self.tiering_policy.ssd_write_through;
        let admit_memory = decision.admit_memory;
        let admit_pmem = decision.admit_pmem;

        if admit_memory {
            self.record_metadata(
                &key,
                request.block_kind,
                request.routing_slot,
                value.len(),
                request.hotness,
                decision.reason,
            );
            let _ = self.put_memory(key.clone(), value.clone());
        } else {
            self.stats.memory_admission_rejected += 1;
        }

        if admit_pmem {
            self.record_metadata(
                &key,
                request.block_kind,
                request.routing_slot,
                value.len(),
                request.hotness,
                decision.reason,
            );
            let _ = self.put_pmem(key.clone(), value.clone());
        } else {
            self.stats.pmem_admission_rejected =
                self.stats.pmem_admission_rejected.saturating_add(1);
        }

        if !admit_ssd {
            self.stats.ssd_admission_rejected = self.stats.ssd_admission_rejected.saturating_add(1);
            self.stats.writeback_backpressure_events =
                self.stats.writeback_backpressure_events.saturating_add(1);
            self.stats.puts += 1;
            return Ok(());
        }
        if value.len() > self.tiering_policy.max_ssd_block_bytes
            || value.len() > self.ssd_capacity_bytes
        {
            self.stats.ssd_admission_rejected = self.stats.ssd_admission_rejected.saturating_add(1);
            self.stats.ssd_oversize_rejections =
                self.stats.ssd_oversize_rejections.saturating_add(1);
            self.stats.writeback_backpressure_events =
                self.stats.writeback_backpressure_events.saturating_add(1);
            self.stats.puts += 1;
            return Ok(());
        }
        if self.tiering_policy.ssd_write_through && !decision.admit_ssd {
            self.stats.ssd_write_through_admissions =
                self.stats.ssd_write_through_admissions.saturating_add(1);
        }
        let block = encode_cache_block(&value, self.block_options)?;
        let compressed = is_encoded_compressed_block(&block);
        let block_len = block.len();
        if block_len > self.tiering_policy.max_ssd_block_bytes
            || block_len > self.ssd_capacity_bytes
        {
            self.stats.ssd_admission_rejected = self.stats.ssd_admission_rejected.saturating_add(1);
            self.stats.ssd_oversize_rejections =
                self.stats.ssd_oversize_rejections.saturating_add(1);
            self.stats.writeback_backpressure_events =
                self.stats.writeback_backpressure_events.saturating_add(1);
            self.stats.puts += 1;
            return Ok(());
        }
        if self.ssd_bytes.saturating_add(block_len as u64) > self.ssd_capacity_bytes as u64
            && self.incoming_ssd_block_is_colder_than_existing_groups(&key, &request, value.len())
        {
            self.stats.ssd_admission_rejected = self.stats.ssd_admission_rejected.saturating_add(1);
            self.stats.writeback_backpressure_events =
                self.stats.writeback_backpressure_events.saturating_add(1);
            self.stats.puts += 1;
            return Ok(());
        }
        self.evict_ssd_for(block_len as u64);
        if self.ssd_bytes.saturating_add(block_len as u64) > self.ssd_capacity_bytes as u64 {
            self.stats.ssd_admission_rejected = self.stats.ssd_admission_rejected.saturating_add(1);
            self.stats.writeback_backpressure_events =
                self.stats.writeback_backpressure_events.saturating_add(1);
            self.stats.puts += 1;
            return Ok(());
        }
        let path = self.disk_path(&key);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        if compressed {
            self.stats.compressed_puts += 1;
            self.stats.compression_bytes_saved += value.len().saturating_sub(block.len()) as u64;
        }
        write_cache_block_atomic(&path, &block)?;
        if let Some(old_len) = self.disk_index.insert(key.clone(), block_len as u64) {
            self.ssd_bytes = self.ssd_bytes.saturating_sub(old_len);
            self.disk_order.retain(|candidate| candidate != &key);
        }
        self.disk_order.push_back(key.clone());
        self.ssd_bytes = self.ssd_bytes.saturating_add(block_len as u64);
        self.record_metadata(
            &key,
            request.block_kind,
            request.routing_slot,
            value.len(),
            request.hotness,
            decision.reason,
        );
        self.stats.puts += 1;
        self.stats.disk_fills += 1;
        self.stats.ssd_admission_accepted = self.stats.ssd_admission_accepted.saturating_add(1);
        self.stats.disk_bytes = self.ssd_bytes;
        Ok(())
    }

}

impl MultiLayerCache {
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
                .chain(inner.pmem.keys())
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
            .chain(inner.pmem.keys())
            .filter(|key| key.shard_id == shard_id)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let memory_entries_removed = memory_keys.len();
        for key in &memory_keys {
            if let Some(value) = inner.memory.remove(key) {
                inner.memory_bytes = inner.memory_bytes.saturating_sub(value.len());
            }
            if let Some(value) = inner.pmem.remove(key) {
                inner.pmem_bytes = inner.pmem_bytes.saturating_sub(value.len());
            }
        }
        inner.order.retain(|key| key.shard_id != shard_id);
        inner.pmem_order.retain(|key| key.shard_id != shard_id);
        inner.disk_order.retain(|key| key.shard_id != shard_id);
        inner.disk_index.retain(|key, _| key.shard_id != shard_id);
        inner.metadata.retain(|key, _| key.shard_id != shard_id);
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
        inner.stats.pmem_bytes = inner.pmem_bytes as u64;
        inner.ssd_bytes = inner.ssd_bytes.saturating_sub(disk_bytes_before);
        inner.stats.disk_bytes = inner.ssd_bytes;
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
            .chain(inner.pmem.keys())
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
            if let Some(value) = inner.pmem.remove(key) {
                inner.pmem_bytes = inner.pmem_bytes.saturating_sub(value.len());
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
            inner.metadata.remove(key);
        }
        inner
            .order
            .retain(|key| !(key.shard_id == shard_id && key.selector.starts_with(&prefix)));
        inner
            .pmem_order
            .retain(|key| !(key.shard_id == shard_id && key.selector.starts_with(&prefix)));
        inner
            .disk_order
            .retain(|key| !(key.shard_id == shard_id && key.selector.starts_with(&prefix)));
        inner.stats.invalidations = inner
            .stats
            .invalidations
            .saturating_add(memory_entries_removed as u64);
        inner.stats.memory_bytes = inner.memory_bytes as u64;
        inner.stats.pmem_bytes = inner.pmem_bytes as u64;
        inner.ssd_bytes = inner.ssd_bytes.saturating_sub(disk_bytes_removed);
        inner.stats.disk_bytes = inner.ssd_bytes;
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
            .chain(inner.pmem.keys())
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
                let pmem_bytes = inner
                    .pmem
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
                let meta = inner.metadata.get(&key).copied();
                CacheEntryInfo {
                    shard_id: key.shard_id,
                    namespace: key.namespace,
                    record_key: key.record_key,
                    selector: key.selector,
                    memory_bytes,
                    pmem_bytes,
                    disk_bytes,
                    pinned,
                    block_kind: meta.map(|meta| meta.block_kind),
                    routing_slot: meta.and_then(|meta| meta.routing_slot),
                    hotness: meta.map(|meta| meta.hotness).unwrap_or_default(),
                    hits: meta.map(|meta| meta.hits).unwrap_or_default(),
                    last_access_epoch: meta.map(|meta| meta.last_access_epoch).unwrap_or_default(),
                    admission_reason: meta.map(|meta| meta.admission_reason),
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
            pmem_bytes: inner.pmem_bytes as u64,
            disk_bytes: inner.ssd_bytes,
            pinned_entries: inner.pinned.len() as u64,
            pinned_bytes: inner.pinned_memory_bytes(),
            async_writeback_queue_depth: inner.async_writeback_queue.len() as u64,
            async_writeback_queue_bytes: inner.async_writeback_queue_bytes(),
            ..inner.stats
        }
    }

    pub fn eviction_report(&self) -> CacheEvictionReport {
        let stats = self.stats();
        CacheEvictionReport {
            memory_evictions: stats.memory_evictions,
            memory_capacity_evictions: stats.eviction_capacity,
            memory_cold_evictions: stats.eviction_cold,
            memory_low_hit_evictions: stats.eviction_low_hit,
            memory_stale_evictions: stats.eviction_stale,
            memory_pinned_skips: stats.eviction_pinned_skips,
            pmem_evictions: stats.pmem_evictions,
            pmem_capacity_evictions: stats.pmem_eviction_capacity,
            pmem_pinned_skips: stats.pmem_eviction_pinned_skips,
            ssd_evictions: stats.ssd_evictions,
            ssd_capacity_evictions: stats.ssd_eviction_capacity,
            ssd_cold_evictions: stats.ssd_eviction_cold,
            ssd_low_hit_evictions: stats.ssd_eviction_low_hit,
            ssd_stale_evictions: stats.ssd_eviction_stale,
            ssd_pinned_skips: stats.ssd_eviction_pinned_skips,
            sampled_eviction_groups: stats.eviction_sampled_groups,
            memory_slot_evictions: stats.memory_slot_evictions,
            ssd_slot_evictions: stats.ssd_slot_evictions,
            replacement_policy: CacheReplacementPolicy::WeightedHotnessLru,
        }
    }

    pub fn writeback_backpressure_report(&self) -> CacheWritebackBackpressureReport {
        let stats = self.stats();
        CacheWritebackBackpressureReport {
            ssd_write_through_enabled: self.production_tiering_policy().ssd_write_through,
            write_through_admissions: stats.ssd_write_through_admissions,
            ssd_admission_rejections: stats.ssd_admission_rejected,
            ssd_evictions: stats.ssd_evictions,
            ssd_oversize_rejections: stats.ssd_oversize_rejections,
            backpressure_events: stats.writeback_backpressure_events
                + stats.async_writeback_backpressure_rejections,
            bounded_queue_ready: true,
        }
    }

    pub fn latency_metrics_report(&self) -> CacheLatencyMetricsReport {
        let stats = self.stats();
        let get_count = stats.get_latency_count.max(stats.get_latency_samples);
        let put_count = stats.put_latency_count.max(stats.put_latency_samples);
        let get_total = stats.get_latency_total_us.max(stats.get_latency_total_micros);
        let put_total = stats.put_latency_total_us.max(stats.put_latency_total_micros);
        let get_max = stats.get_latency_max_us.max(stats.get_latency_max_micros);
        let put_max = stats.put_latency_max_us.max(stats.put_latency_max_micros);
        CacheLatencyMetricsReport {
            get_count,
            get_avg_us: if get_count == 0 { 0 } else { get_total / get_count },
            get_max_us: get_max,
            put_count,
            put_avg_us: if put_count == 0 { 0 } else { put_total / put_count },
            put_max_us: put_max,
            histogram_ready: stats.get_latency_le_10us
                + stats.get_latency_le_100us
                + stats.get_latency_le_1ms
                + stats.get_latency_le_10ms
                + stats.get_latency_gt_10ms
                + stats.put_latency_le_10us
                + stats.put_latency_le_100us
                + stats.put_latency_le_1ms
                + stats.put_latency_le_10ms
                + stats.put_latency_gt_10ms
                > 0,
        }
    }

    #[doc(hidden)]
    pub fn clear_memory_for_test(&self) {
        let mut inner = self.inner.write().expect("cache lock poisoned");
        inner.memory.clear();
        inner.pmem.clear();
        inner.order.clear();
        inner.pmem_order.clear();
        inner.memory_bytes = 0;
        inner.pmem_bytes = 0;
        inner.stats.memory_bytes = 0;
        inner.stats.pmem_bytes = 0;
        inner.refresh_pin_stats();
    }

    pub fn replacement_policy_soak(&self, iterations: usize) -> CacheReplacementPolicySoakReport {
        let hot_keys = (0..4)
            .map(|idx| CacheKey::page_with_slot(7, 1, idx * 8, 8, Some(3)))
            .collect::<Vec<_>>();
        let pinned_key = hot_keys[0].clone();
        for (idx, key) in hot_keys.iter().enumerate() {
            let _ = self.put(key.clone(), vec![b'h' + idx as u8; 8]);
        }
        self.pin(pinned_key.clone());

        let mut cold_keys = Vec::new();
        for idx in 0..iterations {
            for hot in &hot_keys[1..] {
                let _ = self.get(hot);
            }
            let cold = CacheKey::page_with_slot(7, 2 + idx as u64, idx as u64 * 16, 8, Some(4));
            let _ = self.put(cold.clone(), vec![b'c'; 8]);
            cold_keys.push(cold);
        }

        let restart_probe_key = cold_keys[0].clone();
        let restart_probe_value = self.get(&restart_probe_key).ok().flatten();
        let (memory_capacity_bytes, disk_dir) = {
            let inner = self.inner.read().expect("cache lock poisoned");
            (inner.memory_capacity_bytes, inner.disk_dir.clone())
        };
        let restarted_cache = MultiLayerCache::new(memory_capacity_bytes, disk_dir);
        let restart_disk_refill_ready = restart_probe_value.is_some()
            && restarted_cache
                .get(&restart_probe_key)
                .ok()
                .flatten()
                .as_ref()
                == restart_probe_value.as_ref();
        let hot_memory_survivors = hot_keys
            .iter()
            .filter(|key| self.get_memory(key).is_some())
            .count();
        let recent_cold = cold_keys
            .iter()
            .rev()
            .take(hot_keys.len())
            .cloned()
            .collect::<BTreeSet<_>>();
        let cold_memory_survivors = cold_keys
            .iter()
            .filter(|key| recent_cold.contains(*key) && self.get_memory(key).is_some())
            .count();
        self.set_async_writeback_queue_limit_for_test(1);
        let _ = self.enqueue_async_writeback(
            CacheKey::page_with_slot(7, 999, 0, 8, Some(9)),
            b"writeback".to_vec(),
        );
        let _ = self.enqueue_async_writeback(
            CacheKey::page_with_slot(7, 1_000, 0, 8, Some(9)),
            b"overflow".to_vec(),
        );
        let _ = self.drain_async_writeback(8);
        self.record_compaction_latency_micros(500);
        let stats = self.stats();
        let read_through_latency_bucketed = latency_bucket_count(
            stats.read_through_latency_le_10us,
            stats.read_through_latency_le_100us,
            stats.read_through_latency_le_1ms,
            stats.read_through_latency_le_10ms,
            stats.read_through_latency_gt_10ms,
        ) == stats.read_through_latency_samples;
        let refill_latency_bucketed = latency_bucket_count(
            stats.refill_latency_le_10us,
            stats.refill_latency_le_100us,
            stats.refill_latency_le_1ms,
            stats.refill_latency_le_10ms,
            stats.refill_latency_gt_10ms,
        ) == stats.refill_latency_samples;
        let writeback_latency_bucketed = latency_bucket_count(
            stats.writeback_latency_le_10us,
            stats.writeback_latency_le_100us,
            stats.writeback_latency_le_1ms,
            stats.writeback_latency_le_10ms,
            stats.writeback_latency_gt_10ms,
        ) == stats.writeback_latency_samples;
        let eviction_latency_bucketed = latency_bucket_count(
            stats.eviction_latency_le_10us,
            stats.eviction_latency_le_100us,
            stats.eviction_latency_le_1ms,
            stats.eviction_latency_le_10ms,
            stats.eviction_latency_gt_10ms,
        ) == stats.eviction_latency_samples;
        let compaction_latency_bucketed = latency_bucket_count(
            stats.compaction_latency_le_10us,
            stats.compaction_latency_le_100us,
            stats.compaction_latency_le_1ms,
            stats.compaction_latency_le_10ms,
            stats.compaction_latency_gt_10ms,
        ) == stats.compaction_latency_samples;
        let mut report = CacheReplacementPolicySoakReport {
            iterations,
            hot_key_count: hot_keys.len(),
            cold_key_count: cold_keys.len(),
            hot_memory_survivors,
            cold_memory_survivors,
            pinned_memory_survived: self.get_memory(&pinned_key).is_some(),
            restart_disk_refill_ready,
            observed_evictions: stats.memory_evictions,
            observed_pinned_skips: stats.eviction_pinned_skips,
            observed_disk_refills: stats.disk_hits,
            observed_async_writeback_backpressure: stats.async_writeback_backpressure_rejections,
            async_writeback_max_queue_depth: stats.async_writeback_max_queue_depth,
            async_writeback_max_queue_bytes: stats.async_writeback_max_queue_bytes,
            get_latency_samples: stats.get_latency_samples,
            put_latency_samples: stats.put_latency_samples,
            read_through_latency_samples: stats.read_through_latency_samples,
            refill_latency_samples: stats.refill_latency_samples,
            writeback_latency_samples: stats.writeback_latency_samples,
            eviction_latency_samples: stats.eviction_latency_samples,
            compaction_latency_samples: stats.compaction_latency_samples,
            read_through_latency_bucketed,
            refill_latency_bucketed,
            writeback_latency_bucketed,
            eviction_latency_bucketed,
            compaction_latency_bucketed,
            ..CacheReplacementPolicySoakReport::default()
        };
        if report.iterations < 64 {
            report
                .reasons
                .push("insufficient_soak_iterations".to_string());
        }
        if report.observed_evictions == 0 {
            report
                .reasons
                .push("missing_eviction_observation".to_string());
        }
        if !report.pinned_memory_survived || report.observed_pinned_skips == 0 {
            report.reasons.push("missing_pinned_survival".to_string());
        }
        if report.hot_memory_survivors < report.hot_key_count {
            report
                .reasons
                .push("hot_working_set_not_retained".to_string());
        }
        if report.cold_memory_survivors >= report.hot_memory_survivors {
            report
                .reasons
                .push("cold_set_retained_like_hot_set".to_string());
        }
        if report.observed_disk_refills == 0 {
            report
                .reasons
                .push("missing_disk_refill_observation".to_string());
        }
        if !report.restart_disk_refill_ready {
            report
                .reasons
                .push("missing_restart_disk_refill_observation".to_string());
        }
        if report.observed_async_writeback_backpressure == 0
            || report.async_writeback_max_queue_depth == 0
            || report.async_writeback_max_queue_bytes == 0
        {
            report
                .reasons
                .push("missing_async_writeback_backpressure".to_string());
        }
        if report.get_latency_samples == 0 || report.put_latency_samples == 0 {
            report.reasons.push("missing_latency_samples".to_string());
        }
        if report.read_through_latency_samples == 0
            || report.refill_latency_samples == 0
            || report.writeback_latency_samples == 0
            || report.eviction_latency_samples == 0
            || report.compaction_latency_samples == 0
        {
            report
                .reasons
                .push("missing_operation_latency_samples".to_string());
        }
        if !report.read_through_latency_bucketed
            || !report.refill_latency_bucketed
            || !report.writeback_latency_bucketed
            || !report.eviction_latency_bucketed
            || !report.compaction_latency_bucketed
        {
            report
                .reasons
                .push("missing_operation_latency_histograms".to_string());
        }
        report.passed = report.reasons.is_empty();
        report
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

    fn default_request(&self, key: &CacheKey, block_bytes: usize) -> CacheAdmissionRequest {
        let existing = self.metadata.get(key).copied();
        CacheAdmissionRequest {
            block_kind: existing
                .map(|meta| meta.block_kind)
                .unwrap_or_else(|| infer_block_kind(key)),
            shard_id: key.shard_id,
            routing_slot: existing
                .and_then(|meta| meta.routing_slot)
                .or_else(|| extract_routing_slot(key)),
            block_bytes,
            hotness: existing.map(|meta| meta.hotness).unwrap_or_default(),
            pinned: self.pinned.contains(key),
        }
    }

    fn record_metadata(
        &mut self,
        key: &CacheKey,
        block_kind: CacheBlockKind,
        routing_slot: Option<u32>,
        block_bytes: usize,
        requested_hotness: u32,
        admission_reason: CacheAdmissionReason,
    ) {
        self.access_epoch = self.access_epoch.saturating_add(1);
        let current = self.metadata.get(key).copied();
        self.metadata.insert(
            key.clone(),
            CacheEntryMeta {
                block_kind,
                routing_slot,
                hotness: current.map(|meta| meta.hotness).unwrap_or_else(|| {
                    initial_hotness(block_kind, block_bytes).max(requested_hotness)
                }),
                hits: current.map(|meta| meta.hits).unwrap_or_default(),
                last_access_epoch: self.access_epoch,
                admission_reason,
            },
        );
    }

    fn record_hit(&mut self, key: &CacheKey, block_bytes: usize) {
        self.access_epoch = self.access_epoch.saturating_add(1);
        let block_kind = infer_block_kind(key);
        let entry = self.metadata.entry(key.clone()).or_insert(CacheEntryMeta {
            block_kind,
            routing_slot: extract_routing_slot(key),
            hotness: initial_hotness(block_kind, block_bytes),
            hits: 0,
            last_access_epoch: 0,
            admission_reason: CacheAdmissionReason::MemoryOnly,
        });
        entry.hits = entry.hits.saturating_add(1);
        let before = entry.hotness;
        entry.hotness = entry.hotness.saturating_add(1);
        entry.last_access_epoch = self.access_epoch;
        if before < self.tiering_policy.memory_hotness_threshold
            && entry.hotness >= self.tiering_policy.memory_hotness_threshold
        {
            self.stats.hotness_promotions = self.stats.hotness_promotions.saturating_add(1);
        }
        self.disk_order.retain(|candidate| candidate != key);
        if self.disk_index.contains_key(key) {
            self.disk_order.push_back(key.clone());
        }
        self.order.retain(|candidate| candidate != key);
        if self.memory.contains_key(key) {
            self.order.push_back(key.clone());
        }
    }

    fn put_memory(&mut self, key: CacheKey, value: Vec<u8>) -> bool {
        let eviction_started = Instant::now();
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
            self.touch_key(&key);
        } else {
            self.order.push_back(key);
        }
        self.memory_bytes += value.len();
        while self.memory_bytes > self.memory_capacity_bytes {
            if !self.evict_one_memory_entry() {
                self.stats.eviction_pinned_skips =
                    self.stats.eviction_pinned_skips.saturating_add(1);
                break;
            }
            self.record_eviction_latency(eviction_started);
        }
        self.stats.memory_bytes = self.memory_bytes as u64;
        self.refresh_pin_stats();
        true
    }

    fn put_pmem(&mut self, key: CacheKey, value: Vec<u8>) -> bool {
        let eviction_started = Instant::now();
        if self.pmem_capacity_bytes == 0 || value.len() > self.pmem_capacity_bytes {
            self.stats.pmem_admission_rejected =
                self.stats.pmem_admission_rejected.saturating_add(1);
            return false;
        }
        self.stats.pmem_admission_accepted = self.stats.pmem_admission_accepted.saturating_add(1);
        self.stats.pmem_fills = self.stats.pmem_fills.saturating_add(1);
        let value = Arc::<[u8]>::from(value);
        if let Some(old) = self.pmem.insert(key.clone(), Arc::clone(&value)) {
            self.pmem_bytes = self.pmem_bytes.saturating_sub(old.len());
            self.touch_key(&key);
        } else {
            self.pmem_order.push_back(key);
        }
        self.pmem_bytes = self.pmem_bytes.saturating_add(value.len());
        while self.pmem_bytes > self.pmem_capacity_bytes {
            if !self.evict_one_pmem_entry() {
                self.stats.pmem_eviction_pinned_skips =
                    self.stats.pmem_eviction_pinned_skips.saturating_add(1);
                break;
            }
            self.record_eviction_latency(eviction_started);
        }
        self.stats.pmem_bytes = self.pmem_bytes as u64;
        self.refresh_pin_stats();
        true
    }

    fn touch_key(&mut self, key: &CacheKey) {
        self.order.retain(|candidate| candidate != key);
        if self.memory.contains_key(key) {
            self.order.push_back(key.clone());
        }
        self.pmem_order.retain(|candidate| candidate != key);
        if self.pmem.contains_key(key) {
            self.pmem_order.push_back(key.clone());
        }
    }

    fn evict_memory_to_capacity(&mut self) {
        while self.memory_bytes > self.memory_capacity_bytes {
            let before = self.memory_bytes;
            self.evict_one_memory_entry();
            if self.memory_bytes == before {
                break;
            }
        }
    }

    fn evict_pmem_to_capacity(&mut self) {
        while self.pmem_bytes > self.pmem_capacity_bytes {
            let before = self.pmem_bytes;
            self.evict_one_pmem_entry();
            if self.pmem_bytes == before {
                break;
            }
            self.stats.memory_bytes = self.memory_bytes as u64;
            self.refresh_pin_stats();
        }
    }

    fn evict_one_memory_entry(&mut self) -> bool {
        let Some((victim, reason, pinned_skips)) = self.select_memory_eviction_victim() else {
            return false;
        };
        self.stats.eviction_pinned_skips = self
            .stats
            .eviction_pinned_skips
            .saturating_add(pinned_skips);
        if let Some(old_value) = self.memory.remove(&victim) {
            self.memory_bytes = self.memory_bytes.saturating_sub(old_value.len());
            self.order.retain(|candidate| candidate != &victim);
            self.stats.memory_evictions = self.stats.memory_evictions.saturating_add(1);
            self.stats.eviction_capacity = self.stats.eviction_capacity.saturating_add(1);
            self.stats.memory_slot_evictions = self.stats.memory_slot_evictions.saturating_add(1);
            self.record_memory_eviction_reason(reason);
            if !self.disk_index.contains_key(&victim) {
                self.metadata.remove(&victim);
            }
            self.stats.memory_bytes = self.memory_bytes as u64;
            self.refresh_pin_stats();
            return true;
        }
        self.order.retain(|candidate| candidate != &victim);
        self.stats.memory_bytes = self.memory_bytes as u64;
        false
    }

    fn evict_one_pmem_entry(&mut self) -> bool {
        let Some((victim, _reason, pinned_skips)) = self.select_pmem_eviction_victim() else {
            return false;
        };
        self.stats.pmem_eviction_pinned_skips = self
            .stats
            .pmem_eviction_pinned_skips
            .saturating_add(pinned_skips);
        if let Some(old_value) = self.pmem.remove(&victim) {
            self.pmem_bytes = self.pmem_bytes.saturating_sub(old_value.len());
            self.pmem_order.retain(|candidate| candidate != &victim);
            self.stats.pmem_evictions = self.stats.pmem_evictions.saturating_add(1);
            self.stats.pmem_eviction_capacity = self.stats.pmem_eviction_capacity.saturating_add(1);
            if !self.memory.contains_key(&victim) && !self.disk_index.contains_key(&victim) {
                self.metadata.remove(&victim);
            }
            self.stats.pmem_bytes = self.pmem_bytes as u64;
            self.refresh_pin_stats();
            return true;
        }
        self.pmem_order.retain(|candidate| candidate != &victim);
        self.stats.pmem_bytes = self.pmem_bytes as u64;
        false
    }

    fn evict_ssd_for(&mut self, incoming_bytes: u64) {
        while self.ssd_bytes.saturating_add(incoming_bytes) > self.ssd_capacity_bytes as u64 {
            if !self.evict_one_ssd_entry() {
                break;
            }
        }
    }

    fn evict_ssd_to_capacity(&mut self) {
        while self.ssd_bytes > self.ssd_capacity_bytes as u64 {
            if !self.evict_one_ssd_entry() {
                break;
            }
        }
    }

    fn evict_one_ssd_entry(&mut self) -> bool {
        let Some((victim, reason, pinned_skips)) = self.select_ssd_eviction_victim() else {
            return false;
        };
        self.stats.ssd_eviction_pinned_skips = self
            .stats
            .ssd_eviction_pinned_skips
            .saturating_add(pinned_skips);
        let path = self.disk_path(&victim);
        let removed_bytes = self
            .disk_index
            .remove(&victim)
            .or_else(|| path.metadata().ok().map(|metadata| metadata.len()))
            .unwrap_or_default();
        let _ = fs::remove_file(path);
        self.disk_order.retain(|candidate| candidate != &victim);
        self.ssd_bytes = self.ssd_bytes.saturating_sub(removed_bytes);
        self.stats.ssd_evictions = self.stats.ssd_evictions.saturating_add(1);
        self.stats.ssd_eviction_capacity = self.stats.ssd_eviction_capacity.saturating_add(1);
        self.stats.ssd_slot_evictions = self.stats.ssd_slot_evictions.saturating_add(1);
        self.record_ssd_eviction_reason(reason);
        self.stats.disk_bytes = self.ssd_bytes;
        if !self.memory.contains_key(&victim) {
            self.metadata.remove(&victim);
        }
        true
    }

    fn select_memory_eviction_victim(&mut self) -> Option<(CacheKey, EvictionReason, u64)> {
        let keys = self.memory.keys().cloned().collect::<Vec<_>>();
        self.select_eviction_victim(keys)
    }

    fn select_pmem_eviction_victim(&mut self) -> Option<(CacheKey, EvictionReason, u64)> {
        let keys = self.pmem.keys().cloned().collect::<Vec<_>>();
        self.select_eviction_victim(keys)
    }

    fn select_ssd_eviction_victim(&mut self) -> Option<(CacheKey, EvictionReason, u64)> {
        let keys = self.disk_index.keys().cloned().collect::<Vec<_>>();
        self.select_eviction_victim(keys)
    }

    fn select_eviction_victim<I>(&mut self, keys: I) -> Option<(CacheKey, EvictionReason, u64)>
    where
        I: IntoIterator<Item = CacheKey>,
    {
        let mut pinned_skips = 0u64;
        let mut groups: HashMap<String, SlotEvictionGroup> = HashMap::new();
        for key in keys {
            if self.pinned.contains(&key) {
                pinned_skips = pinned_skips.saturating_add(1);
                continue;
            }
            let score = self.eviction_score(&key);
            let group_key = self.eviction_group_key(&key);
            groups
                .entry(group_key)
                .and_modify(|group| group.observe(key.clone(), score))
                .or_insert_with(|| SlotEvictionGroup::new(key, score));
        }
        self.stats.eviction_sampled_groups = self
            .stats
            .eviction_sampled_groups
            .saturating_add(groups.len() as u64);
        groups
            .into_values()
            .min_by(|left, right| {
                left.group_score
                    .cmp(&right.group_score)
                    .then_with(|| left.victim_score.cmp(&right.victim_score))
                    .then_with(|| left.victim.cmp(&right.victim))
            })
            .map(|group| {
                (
                    group.victim,
                    eviction_reason_for(group.victim_score),
                    pinned_skips,
                )
            })
    }

    fn eviction_score(&self, key: &CacheKey) -> EvictionScore {
        let meta = self.metadata.get(key).copied().unwrap_or(CacheEntryMeta {
            block_kind: infer_block_kind(key),
            routing_slot: extract_routing_slot(key),
            hotness: 0,
            hits: 0,
            last_access_epoch: 0,
            admission_reason: CacheAdmissionReason::MemoryOnly,
        });
        EvictionScore {
            hotness: meta.hotness,
            hits: meta.hits,
            last_access_epoch: meta.last_access_epoch,
        }
    }

    fn incoming_ssd_block_is_colder_than_existing_groups(
        &self,
        key: &CacheKey,
        request: &CacheAdmissionRequest,
        block_bytes: usize,
    ) -> bool {
        let incoming_score = EvictionScore {
            hotness: initial_hotness(request.block_kind, block_bytes).max(request.hotness),
            hits: 0,
            last_access_epoch: self.access_epoch.saturating_add(1),
        };
        let incoming_group = request
            .routing_slot
            .or_else(|| extract_routing_slot(key))
            .map(|slot| format!("slot:{slot}"))
            .unwrap_or_else(|| format!("object:{}:{}", key.namespace, key.record_key));
        self.disk_index
            .keys()
            .filter(|candidate| self.eviction_group_key(candidate) != incoming_group)
            .map(|candidate| self.eviction_score(candidate))
            .min()
            .map(|coldest_existing| incoming_score < coldest_existing)
            .unwrap_or(false)
    }

    fn eviction_group_key(&self, key: &CacheKey) -> String {
        self.metadata
            .get(key)
            .and_then(|meta| meta.routing_slot)
            .or_else(|| extract_routing_slot(key))
            .map(|slot| format!("slot:{slot}"))
            .unwrap_or_else(|| format!("object:{}:{}", key.namespace, key.record_key))
    }

    fn record_memory_eviction_reason(&mut self, reason: EvictionReason) {
        match reason {
            EvictionReason::Cold => {
                self.stats.eviction_cold = self.stats.eviction_cold.saturating_add(1)
            }
            EvictionReason::LowHit => {
                self.stats.eviction_low_hit = self.stats.eviction_low_hit.saturating_add(1)
            }
            EvictionReason::Stale => {
                self.stats.eviction_stale = self.stats.eviction_stale.saturating_add(1)
            }
        }
    }

    fn record_ssd_eviction_reason(&mut self, reason: EvictionReason) {
        match reason {
            EvictionReason::Cold => {
                self.stats.ssd_eviction_cold = self.stats.ssd_eviction_cold.saturating_add(1)
            }
            EvictionReason::LowHit => {
                self.stats.ssd_eviction_low_hit = self.stats.ssd_eviction_low_hit.saturating_add(1)
            }
            EvictionReason::Stale => {
                self.stats.ssd_eviction_stale = self.stats.ssd_eviction_stale.saturating_add(1)
            }
        }
    }

    fn invalidate_key_locked(&mut self, key: &CacheKey, remove_disk: bool) {
        if let Some(value) = self.memory.remove(key) {
            self.memory_bytes = self.memory_bytes.saturating_sub(value.len());
        }
        self.order.retain(|candidate| candidate != key);
        if remove_disk {
            let path = self.disk_path(key);
            let disk_bytes = self
                .disk_index
                .remove(key)
                .or_else(|| path.metadata().ok().map(|metadata| metadata.len()))
                .unwrap_or_default();
            let _ = fs::remove_file(path);
            self.disk_order.retain(|candidate| candidate != key);
            self.ssd_bytes = self.ssd_bytes.saturating_sub(disk_bytes);
        }
        if let Some(value) = self.pmem.remove(key) {
            self.pmem_bytes = self.pmem_bytes.saturating_sub(value.len());
        }
        self.pmem_order.retain(|candidate| candidate != key);
        self.metadata.remove(key);
        self.pinned.remove(key);
        self.stats.invalidations += 1;
        self.stats.memory_bytes = self.memory_bytes as u64;
        self.stats.pmem_bytes = self.pmem_bytes as u64;
        self.stats.disk_bytes = self.ssd_bytes;
        self.refresh_pin_stats();
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
        let mut ignored_samples = 0;
        observe_latency_bucket(
            micros,
            &mut ignored_samples,
            &mut self.stats.get_latency_le_10us,
            &mut self.stats.get_latency_le_100us,
            &mut self.stats.get_latency_le_1ms,
            &mut self.stats.get_latency_le_10ms,
            &mut self.stats.get_latency_gt_10ms,
        );
    }

    fn record_put_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.stats.put_latency_samples = self.stats.put_latency_samples.saturating_add(1);
        self.stats.put_latency_total_micros =
            self.stats.put_latency_total_micros.saturating_add(micros);
        self.stats.put_latency_max_micros = self.stats.put_latency_max_micros.max(micros);
        let mut ignored_samples = 0;
        observe_latency_bucket(
            micros,
            &mut ignored_samples,
            &mut self.stats.put_latency_le_10us,
            &mut self.stats.put_latency_le_100us,
            &mut self.stats.put_latency_le_1ms,
            &mut self.stats.put_latency_le_10ms,
            &mut self.stats.put_latency_gt_10ms,
        );
    }

    fn record_read_through_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        observe_latency_bucket(
            micros,
            &mut self.stats.read_through_latency_samples,
            &mut self.stats.read_through_latency_le_10us,
            &mut self.stats.read_through_latency_le_100us,
            &mut self.stats.read_through_latency_le_1ms,
            &mut self.stats.read_through_latency_le_10ms,
            &mut self.stats.read_through_latency_gt_10ms,
        );
    }

    fn record_refill_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        observe_latency_bucket(
            micros,
            &mut self.stats.refill_latency_samples,
            &mut self.stats.refill_latency_le_10us,
            &mut self.stats.refill_latency_le_100us,
            &mut self.stats.refill_latency_le_1ms,
            &mut self.stats.refill_latency_le_10ms,
            &mut self.stats.refill_latency_gt_10ms,
        );
    }

    fn record_writeback_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        observe_latency_bucket(
            micros,
            &mut self.stats.writeback_latency_samples,
            &mut self.stats.writeback_latency_le_10us,
            &mut self.stats.writeback_latency_le_100us,
            &mut self.stats.writeback_latency_le_1ms,
            &mut self.stats.writeback_latency_le_10ms,
            &mut self.stats.writeback_latency_gt_10ms,
        );
    }

    fn record_eviction_latency(&mut self, started: Instant) {
        let micros = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        observe_latency_bucket(
            micros,
            &mut self.stats.eviction_latency_samples,
            &mut self.stats.eviction_latency_le_10us,
            &mut self.stats.eviction_latency_le_100us,
            &mut self.stats.eviction_latency_le_1ms,
            &mut self.stats.eviction_latency_le_10ms,
            &mut self.stats.eviction_latency_gt_10ms,
        );
    }

    fn record_compaction_latency_micros(&mut self, micros: u64) {
        observe_latency_bucket(
            micros,
            &mut self.stats.compaction_latency_samples,
            &mut self.stats.compaction_latency_le_10us,
            &mut self.stats.compaction_latency_le_100us,
            &mut self.stats.compaction_latency_le_1ms,
            &mut self.stats.compaction_latency_le_10ms,
            &mut self.stats.compaction_latency_gt_10ms,
        );
    }
}

impl CacheInner {
    fn async_writeback_queue_bytes(&self) -> u64 {
        self.async_writeback_queue
            .iter()
            .map(|job| job.value.len() as u64)
            .sum()
    }

    fn refresh_async_writeback_pressure_stats(&mut self) {
        let depth = self.async_writeback_queue.len() as u64;
        let bytes = self.async_writeback_queue_bytes();
        self.stats.async_writeback_queue_depth = depth;
        self.stats.async_writeback_queue_bytes = bytes;
        self.stats.async_writeback_max_queue_depth =
            self.stats.async_writeback_max_queue_depth.max(depth);
        self.stats.async_writeback_max_queue_bytes =
            self.stats.async_writeback_max_queue_bytes.max(bytes);
    }
}

fn observe_latency_bucket(
    micros: u64,
    samples: &mut u64,
    le_10us: &mut u64,
    le_100us: &mut u64,
    le_1ms: &mut u64,
    le_10ms: &mut u64,
    gt_10ms: &mut u64,
) {
    *samples = samples.saturating_add(1);
    if micros <= 10 {
        *le_10us = le_10us.saturating_add(1);
    } else if micros <= 100 {
        *le_100us = le_100us.saturating_add(1);
    } else if micros <= 1_000 {
        *le_1ms = le_1ms.saturating_add(1);
    } else if micros <= 10_000 {
        *le_10ms = le_10ms.saturating_add(1);
    } else {
        *gt_10ms = gt_10ms.saturating_add(1);
    }
}

fn infer_block_kind(key: &CacheKey) -> CacheBlockKind {
    match key.namespace.as_str() {
        "page" => CacheBlockKind::Page,
        "index" => CacheBlockKind::Index,
        "oplog" => CacheBlockKind::Oplog,
        "string" | "hash" | "set" | "feature" => CacheBlockKind::Object,
        _ => CacheBlockKind::Other,
    }
}

fn eviction_reason_for(score: EvictionScore) -> EvictionReason {
    if score.hotness == 0 {
        EvictionReason::Cold
    } else if score.hits == 0 {
        EvictionReason::LowHit
    } else {
        EvictionReason::Stale
    }
}

fn initial_hotness(block_kind: CacheBlockKind, block_bytes: usize) -> u32 {
    match block_kind {
        CacheBlockKind::Page => 2,
        CacheBlockKind::Index => 3,
        CacheBlockKind::Oplog => 1,
        CacheBlockKind::Object if block_bytes <= 4096 => 2,
        CacheBlockKind::Object => 1,
        CacheBlockKind::Other => 0,
    }
}

fn extract_routing_slot(key: &CacheKey) -> Option<u32> {
    let suffix = key.selector.strip_prefix("slot-")?;
    let (slot, _) = suffix.split_once(':')?;
    slot.parse::<u32>().ok()
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
            ssd_write_through: true,
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
            ssd_write_through: true,
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

    // shared-corpus: storage_cache_replacement_policy_soak
    #[test]
    fn replacement_policy_soak_retains_hot_and_pinned_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(48, dir.path());

        let report = cache.replacement_policy_soak(128);

        assert!(report.passed, "{report:?}");
        assert_eq!(report.hot_memory_survivors, report.hot_key_count);
        assert!(report.cold_memory_survivors < report.hot_memory_survivors);
        assert!(report.pinned_memory_survived);
        assert!(report.observed_evictions > 0);
        assert!(report.observed_pinned_skips > 0);
        assert!(report.observed_disk_refills > 0);
        assert!(report.observed_async_writeback_backpressure > 0);
        assert!(report.async_writeback_max_queue_depth > 0);
        assert!(report.async_writeback_max_queue_bytes > 0);
        assert!(report.restart_disk_refill_ready);
        assert!(report.get_latency_samples > 0);
        assert!(report.put_latency_samples > 0);
        assert!(report.read_through_latency_samples > 0);
        assert!(report.refill_latency_samples > 0);
        assert!(report.writeback_latency_samples > 0);
        assert!(report.eviction_latency_samples > 0);
        assert!(report.compaction_latency_samples > 0);
        assert!(report.read_through_latency_bucketed);
        assert!(report.refill_latency_bucketed);
        assert!(report.writeback_latency_bucketed);
        assert!(report.eviction_latency_bucketed);
        assert!(report.compaction_latency_bucketed);
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn production_cache_tier_enforces_ssd_capacity_and_reports_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 16,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 90,
            memory_hotness_threshold: 4,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 1,
            max_memory_block_bytes: 16,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 64,
            ssd_write_through: true,
        };
        let cache = MultiLayerCache::with_tiering_policy(
            dir.path(),
            policy,
            CacheBlockOptions {
                compression: CacheCompression::None,
                min_compress_bytes: usize::MAX,
            },
        );
        let first = CacheKey::page_with_slot(1, 10, 0, 16, Some(3));
        let second = CacheKey::page_with_slot(1, 11, 0, 16, Some(3));
        let third = CacheKey::page_with_slot(1, 12, 0, 16, Some(4));

        cache
            .put(first.clone(), b"first-page-0000".to_vec())
            .unwrap();
        cache
            .put(second.clone(), b"second-page-000".to_vec())
            .unwrap();
        cache
            .put(third.clone(), b"third-page-0000".to_vec())
            .unwrap();

        let stats = cache.stats();
        assert!(stats.ssd_admission_accepted >= 3);
        assert!(stats.ssd_evictions >= 1);
        assert!(stats.ssd_eviction_capacity >= 1);
        assert!(stats.disk_bytes <= policy.ssd_capacity_bytes as u64);
        assert_eq!(cache.get(&first).unwrap(), None);

        let entries = cache.entries_for_shard(1);
        assert!(entries.iter().any(|entry| {
            entry.routing_slot == Some(3)
                && entry.block_kind == Some(CacheBlockKind::Page)
                && entry.admission_reason.is_some()
        }));
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn cache_hotness_promotes_entries_and_updates_lru_order() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 64,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 512,
            memory_hotness_threshold: 4,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 1,
            max_memory_block_bytes: 64,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 512,
            ssd_write_through: true,
        };
        let cache =
            MultiLayerCache::with_tiering_policy(dir.path(), policy, CacheBlockOptions::default());
        let key = CacheKey::page_with_slot(1, 20, 0, 4, Some(8));

        cache.put(key.clone(), b"page".to_vec()).unwrap();
        cache.clear_memory_for_test();
        assert_eq!(cache.get(&key).unwrap(), Some(b"page".to_vec()));
        assert_eq!(cache.get(&key).unwrap(), Some(b"page".to_vec()));

        let entry = cache
            .entries_for_shard(1)
            .into_iter()
            .find(|entry| entry.routing_slot == Some(8))
            .expect("cache entry should exist");
        assert!(entry.hotness >= policy.memory_hotness_threshold);
        assert!(entry.hits >= 2);
        assert!(cache.stats().hotness_promotions >= 1);
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn weighted_memory_eviction_preserves_hot_entries() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 8,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 512,
            memory_hotness_threshold: 4,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 1,
            max_memory_block_bytes: 8,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 128,
            ssd_write_through: true,
        };
        let cache =
            MultiLayerCache::with_tiering_policy(dir.path(), policy, CacheBlockOptions::default());
        let hot = CacheKey::page_with_slot(1, 30, 0, 4, Some(1));
        let cold_a = CacheKey::page_with_slot(1, 31, 0, 4, Some(1));
        let cold_b = CacheKey::page_with_slot(1, 32, 0, 4, Some(1));

        cache
            .put_with_admission(
                hot.clone(),
                b"hot!".to_vec(),
                CacheAdmissionRequest {
                    block_kind: CacheBlockKind::Page,
                    shard_id: 1,
                    routing_slot: Some(1),
                    block_bytes: 4,
                    hotness: 10,
                    pinned: false,
                },
            )
            .unwrap();
        cache
            .put_with_admission(
                cold_a.clone(),
                b"aaaa".to_vec(),
                CacheAdmissionRequest {
                    block_kind: CacheBlockKind::Page,
                    shard_id: 1,
                    routing_slot: Some(1),
                    block_bytes: 4,
                    hotness: 0,
                    pinned: false,
                },
            )
            .unwrap();
        cache
            .put_with_admission(
                cold_b.clone(),
                b"bbbb".to_vec(),
                CacheAdmissionRequest {
                    block_kind: CacheBlockKind::Page,
                    shard_id: 1,
                    routing_slot: Some(1),
                    block_bytes: 4,
                    hotness: 0,
                    pinned: false,
                },
            )
            .unwrap();

        assert_eq!(cache.get_memory(&hot), Some(b"hot!".to_vec()));
        assert_eq!(cache.get_memory(&cold_a), None);
        assert_eq!(cache.get_memory(&cold_b), Some(b"bbbb".to_vec()));
        let report = cache.eviction_report();
        assert_eq!(
            report.replacement_policy,
            CacheReplacementPolicy::WeightedHotnessLru
        );
        assert!(report.memory_capacity_evictions >= 1);
        assert!(report.memory_low_hit_evictions >= 1 || report.memory_cold_evictions >= 1);
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn memory_eviction_selects_cold_slot_group_before_cold_entry_in_hot_slot() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 8,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 64,
            memory_hotness_threshold: 4,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 99,
            max_memory_block_bytes: 8,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 64,
            ssd_write_through: false,
        };
        let cache =
            MultiLayerCache::with_tiering_policy(dir.path(), policy, CacheBlockOptions::default());
        let hot_slot_hot = CacheKey::page_with_slot(1, 50, 0, 4, Some(7));
        let hot_slot_cold = CacheKey::page_with_slot(1, 51, 0, 4, Some(7));
        let cold_slot = CacheKey::page_with_slot(1, 52, 0, 4, Some(8));

        for (key, slot, hotness, value) in [
            (hot_slot_hot.clone(), 7, 10, b"hot!".to_vec()),
            (hot_slot_cold.clone(), 7, 0, b"warm".to_vec()),
            (cold_slot.clone(), 8, 0, b"cold".to_vec()),
        ] {
            cache
                .put_with_admission(
                    key,
                    value,
                    CacheAdmissionRequest {
                        block_kind: CacheBlockKind::Page,
                        shard_id: 1,
                        routing_slot: Some(slot),
                        block_bytes: 4,
                        hotness,
                        pinned: false,
                    },
                )
                .unwrap();
        }

        assert_eq!(cache.get_memory(&hot_slot_hot), Some(b"hot!".to_vec()));
        assert_eq!(cache.get_memory(&hot_slot_cold), Some(b"warm".to_vec()));
        assert_eq!(cache.get_memory(&cold_slot), None);
        let report = cache.eviction_report();
        assert!(report.sampled_eviction_groups >= 2);
        assert!(report.memory_slot_evictions >= 1);
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn weighted_ssd_eviction_preserves_hot_entries() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 0,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 70,
            memory_hotness_threshold: 4,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 1,
            max_memory_block_bytes: 8,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 64,
            ssd_write_through: true,
        };
        let cache = MultiLayerCache::with_tiering_policy(
            dir.path(),
            policy,
            CacheBlockOptions {
                compression: CacheCompression::None,
                min_compress_bytes: usize::MAX,
            },
        );
        let hot = CacheKey::page_with_slot(1, 40, 0, 4, Some(2));
        let cold_a = CacheKey::page_with_slot(1, 41, 0, 4, Some(2));
        let cold_b = CacheKey::page_with_slot(1, 42, 0, 4, Some(2));

        for (key, hotness, bytes) in [
            (hot.clone(), 10, b"hot!".to_vec()),
            (cold_a.clone(), 0, b"aaaa".to_vec()),
            (cold_b.clone(), 0, b"bbbb".to_vec()),
        ] {
            cache
                .put_with_admission(
                    key,
                    bytes,
                    CacheAdmissionRequest {
                        block_kind: CacheBlockKind::Page,
                        shard_id: 1,
                        routing_slot: Some(2),
                        block_bytes: 4,
                        hotness,
                        pinned: false,
                    },
                )
                .unwrap();
        }

        assert_eq!(cache.get(&hot).unwrap(), Some(b"hot!".to_vec()));
        assert_eq!(cache.get(&cold_a).unwrap(), None);
        assert_eq!(cache.get(&cold_b).unwrap(), Some(b"bbbb".to_vec()));
        let report = cache.eviction_report();
        assert!(report.ssd_capacity_evictions >= 1);
        assert!(report.ssd_low_hit_evictions >= 1 || report.ssd_cold_evictions >= 1);
    }

    // shared-corpus: storage_cache_refill;
    #[test]
    fn ssd_eviction_selects_cold_slot_group_before_cold_entry_in_hot_slot() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 0,
            pmem_capacity_bytes: 0,
            ssd_capacity_bytes: 256,
            memory_hotness_threshold: 99,
            pmem_admit_hotness_threshold: 0,
            ssd_admit_hotness_threshold: 0,
            max_memory_block_bytes: 0,
            max_pmem_block_bytes: 0,
            max_ssd_block_bytes: 128,
            ssd_write_through: true,
        };
        let cache = MultiLayerCache::with_tiering_policy(
            dir.path(),
            policy,
            CacheBlockOptions {
                compression: CacheCompression::None,
                min_compress_bytes: usize::MAX,
            },
        );
        let hot_slot_hot = CacheKey::page_with_slot(1, 60, 0, 4, Some(9));
        let hot_slot_cold = CacheKey::page_with_slot(1, 61, 0, 4, Some(9));
        let cold_slot = CacheKey::page_with_slot(1, 62, 0, 4, Some(10));

        for (key, slot, hotness, value) in [
            (hot_slot_hot.clone(), 9, 10, b"hot!".to_vec()),
            (hot_slot_cold.clone(), 9, 0, b"warm".to_vec()),
            (cold_slot.clone(), 10, 0, vec![b'c'; 240]),
        ] {
            cache
                .put_with_admission(
                    key,
                    value,
                    CacheAdmissionRequest {
                        block_kind: CacheBlockKind::Page,
                        shard_id: 1,
                        routing_slot: Some(slot),
                        block_bytes: 4,
                        hotness,
                        pinned: false,
                    },
                )
                .unwrap();
        }

        assert_eq!(cache.get(&hot_slot_hot).unwrap(), Some(b"hot!".to_vec()));
        assert_eq!(cache.get(&hot_slot_cold).unwrap(), Some(b"warm".to_vec()));
        assert_eq!(cache.get(&cold_slot).unwrap(), None);
        let report = cache.eviction_report();
        let stats = cache.stats();
        assert_eq!(report.ssd_slot_evictions, 0);
        assert!(stats.ssd_admission_rejected >= 1);
        assert!(stats.writeback_backpressure_events >= 1);
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

    // shared-corpus: storage_cache_refill;
    #[test]
    fn cache_reports_writeback_backpressure_and_latency_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let policy = CacheTieringPolicy {
            memory_capacity_bytes: 16,
            ssd_capacity_bytes: 20,
            memory_hotness_threshold: 4,
            ssd_admit_hotness_threshold: 1,
            max_memory_block_bytes: 16,
            max_ssd_block_bytes: 64,
            ssd_write_through: true,
            ..CacheTieringPolicy::default()
        };
        let cache = MultiLayerCache::with_tiering_policy(
            dir.path(),
            policy,
            CacheBlockOptions {
                compression: CacheCompression::None,
                min_compress_bytes: usize::MAX,
            },
        );
        let first = CacheKey::page_with_slot(1, 70, 0, 12, Some(5));
        let second = CacheKey::page_with_slot(1, 71, 0, 12, Some(5));
        let rejected = CacheKey::page_with_slot(1, 72, 0, 128, Some(5));

        cache.put(first.clone(), b"first-block!".to_vec()).unwrap();
        cache.put(second.clone(), b"second-block".to_vec()).unwrap();
        cache
            .put_with_admission(
                rejected,
                vec![b'x'; 128],
                CacheAdmissionRequest {
                    block_kind: CacheBlockKind::Page,
                    shard_id: 1,
                    routing_slot: Some(5),
                    block_bytes: 128,
                    hotness: 9,
                    pinned: false,
                },
            )
            .unwrap();
        let _ = cache.get(&second).unwrap();

        let writeback = cache.writeback_backpressure_report();
        assert!(writeback.ssd_write_through_enabled);
        assert!(writeback.write_through_admissions > 0);
        assert!(writeback.ssd_evictions > 0 || writeback.ssd_admission_rejections > 0);
        assert!(writeback.backpressure_events > 0);
        assert!(writeback.bounded_queue_ready);

        let latency = cache.latency_metrics_report();
        assert!(latency.put_count >= 3);
        assert!(latency.get_count >= 1);
        assert!(latency.histogram_ready);
        assert!(latency.put_max_us >= latency.put_avg_us);
        assert!(latency.get_max_us >= latency.get_avg_us);
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
        assert_eq!(cache.get(&key).unwrap(), Some(b"value".to_vec()));

        cache.set_async_writeback_queue_limit_for_test(1);
        cache
            .enqueue_async_writeback(CacheKey::string(1, "async-a"), b"a".to_vec())
            .unwrap();
        assert_eq!(cache.stats().async_writeback_queue_depth, 1);
        assert_eq!(cache.stats().async_writeback_queue_bytes, 1);
        assert_eq!(cache.stats().async_writeback_max_queue_depth, 1);
        assert_eq!(cache.stats().async_writeback_max_queue_bytes, 1);
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
        assert_eq!(stats.async_writeback_queue_depth, 0);
        assert_eq!(stats.async_writeback_queue_bytes, 0);
        assert_eq!(stats.async_writeback_max_queue_depth, 1);
        assert_eq!(stats.async_writeback_max_queue_bytes, 1);
        cache.record_compaction_latency_micros(1_500);
        let stats = cache.stats();
        assert!(stats.get_latency_samples > 0);
        assert!(stats.put_latency_samples > 0);
        assert!(stats.read_through_latency_samples > 0);
        assert!(stats.writeback_latency_samples > 0);
        assert!(stats.compaction_latency_samples > 0);
        assert!(stats.get_latency_total_micros >= stats.get_latency_max_micros);
        assert!(stats.put_latency_total_micros >= stats.put_latency_max_micros);
        assert_eq!(
            stats.get_latency_samples,
            stats.get_latency_le_10us
                + stats.get_latency_le_100us
                + stats.get_latency_le_1ms
                + stats.get_latency_le_10ms
                + stats.get_latency_gt_10ms
        );
        assert_eq!(
            stats.put_latency_samples,
            stats.put_latency_le_10us
                + stats.put_latency_le_100us
                + stats.put_latency_le_1ms
                + stats.put_latency_le_10ms
                + stats.put_latency_gt_10ms
        );
        assert_latency_buckets_sum(
            stats.read_through_latency_samples,
            [
                stats.read_through_latency_le_10us,
                stats.read_through_latency_le_100us,
                stats.read_through_latency_le_1ms,
                stats.read_through_latency_le_10ms,
                stats.read_through_latency_gt_10ms,
            ],
        );
        assert_latency_buckets_sum(
            stats.writeback_latency_samples,
            [
                stats.writeback_latency_le_10us,
                stats.writeback_latency_le_100us,
                stats.writeback_latency_le_1ms,
                stats.writeback_latency_le_10ms,
                stats.writeback_latency_gt_10ms,
            ],
        );
        assert_latency_buckets_sum(
            stats.compaction_latency_samples,
            [
                stats.compaction_latency_le_10us,
                stats.compaction_latency_le_100us,
                stats.compaction_latency_le_1ms,
                stats.compaction_latency_le_10ms,
                stats.compaction_latency_gt_10ms,
            ],
        );
    }

    fn assert_latency_buckets_sum(samples: u64, buckets: [u64; 5]) {
        assert_eq!(samples, buckets.into_iter().sum::<u64>());
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
