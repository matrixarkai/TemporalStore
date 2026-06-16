use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cache::{CacheEntryInfo, CacheGcReport, CacheKey, CacheStats, MultiLayerCache};
use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ObjectManagerStats,
    PartitionInfoStats, ScanStreamRequest, ScanStreamResponse, SetConfigRequest, ShardInfo,
    ShardStats, StreamKind, StreamReadRequest, StreamReadResponse, StreamRecord,
    UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::oplog::LocalOplogStore;
use crate::page_store::{
    LocalPageStore, PageAddress, PageStoreError, PageStoreOptions, PageStoreSegmentReport,
    PageStoreStats, PageStoreZoneDescriptor, PageStoreZoneSummary,
};
use crate::types::{
    parse_cpp_feature_filters, BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse,
    ExecuteRequest, ExecuteResponse, FeatureFilter, FeatureFilterOp, FeaturePoint,
    FeatureWritePolicy, IpsStats, RiskFamily, RiskFolType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status, StringSetCondition,
};

#[derive(Debug, Clone)]
pub struct TemporalEngine {
    shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    cache: MultiLayerCache,
    page_store: LocalPageStore,
    oplog_store: LocalOplogStore,
    index_log_store: LocalIndexLogStore,
    index_dir: PathBuf,
    configs: Arc<RwLock<HashMap<ShardId, Config>>>,
    infos: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    admissions: Arc<RwLock<HashMap<AdmissionScope, AdmissionState>>>,
}

impl Default for TemporalEngine {
    fn default() -> Self {
        Self::with_cache_and_page_store(MultiLayerCache::default(), LocalPageStore::default())
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ShardState {
    expires_at_ms: HashMap<String, u64>,
    strings: HashMap<String, PageAddress>,
    hashes: HashMap<String, HashMap<String, PageAddress>>,
    sets: HashMap<String, BTreeMap<Vec<u8>, PageAddress>>,
    features: HashMap<String, BTreeMap<u64, PageAddress>>,
    sequences: HashMap<String, BTreeMap<u64, PageAddress>>,
    ips: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    ips_meta: HashMap<String, BTreeMap<u64, IpsPointMeta>>,
    #[serde(default)]
    ips_request_ids: HashMap<String, BTreeSet<String>>,
    risk: HashMap<String, BTreeMap<u64, i64>>,
    #[serde(default)]
    risk_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>,
    #[serde(default)]
    risk_fol: HashMap<String, RiskFolValue>,
    #[serde(skip)]
    dirty_objects: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RiskFolValue {
    occur_time_ms: u64,
    value: Vec<u8>,
    fol_type: RiskFolType,
}

#[derive(Debug, Default, Clone)]
struct AdmissionState {
    window_epoch_sec: u64,
    read_count: u64,
    write_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AdmissionScope {
    Shard(ShardId),
    Table(String),
    Tenant(String),
}

struct AdmissionLimit {
    scope: AdmissionScope,
    limit: u64,
    label: &'static str,
}

const FEATURE_ADD_HARD_MAX_SIZE: usize = 100_000;
const FEATURE_PAGE_MAGIC: &[u8] = b"TSFPG1\n";
const TIMESTAMPED_KV_PAGE_TARGET_BYTES: usize = 64 * 1024;
const HOT_PAGE_SEGMENT_ID: u64 = u64::MAX;
static HOT_PAGE_OFFSET: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IpsPointMeta {
    address: PageAddress,
    action_type: Option<u32>,
    table_id: Option<u64>,
    request_id: Option<String>,
}

struct ExecuteOutcome {
    response: CommandResponse,
    mutated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PackedFeaturePage {
    version: u8,
    points: Vec<FeaturePoint>,
}

#[derive(Debug, Clone, PartialEq)]
enum PackedFeaturePageDecode {
    Legacy,
    Packed(Vec<FeaturePoint>),
    Corrupt(String),
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionReport {
    pub shard_id: ShardId,
    pub previous_page_segment_id: u64,
    pub compacted_page_segment_id: u64,
    pub rewritten_page_refs: usize,
    pub stale_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub before: ShardCompactionUtilityReport,
    #[serde(default)]
    pub after: ShardCompactionUtilityReport,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionUtilityReport {
    pub live_page_segment_count: usize,
    pub total_page_count: u64,
    pub live_page_refs: u64,
    pub stale_page_estimate: u64,
    pub live_ref_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepReport {
    pub shard_id: ShardId,
    pub expired_records_removed: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustStorageObservation {
    pub shard_id: ShardId,
    pub cache: CacheStats,
    pub page_store: PageStoreStats,
    pub observed_memory_hit: bool,
    pub observed_block_cache_hit: bool,
    pub observed_local_file_read: bool,
    pub observed_cache_invalidation: bool,
    pub observed_memory_eviction: bool,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub local_page_bytes_written: u64,
    pub local_page_bytes_read: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryReport {
    pub shard_id: ShardId,
    pub index_bytes: u64,
    #[serde(default)]
    pub index_write_atomic: bool,
    pub oplog_records: usize,
    pub index_log_records: usize,
    pub active_page_segment_ids: Vec<u64>,
    pub live_page_segment_ids: Vec<u64>,
    pub zone_descriptors: Vec<PageStoreZoneDescriptor>,
    #[serde(default)]
    pub zone_summary: PageStoreZoneSummary,
    #[serde(default)]
    pub page_segment_reports: Vec<PageStoreSegmentReport>,
    #[serde(default)]
    pub page_segment_live_reports: Vec<StorageRecoverySegmentLiveReport>,
    pub total_page_refs: usize,
    pub readable_page_refs: usize,
    #[serde(default)]
    pub unreadable_page_refs: Vec<StorageRecoveryPageError>,
    #[serde(default)]
    pub owner_mismatch_page_refs: Vec<StorageRecoveryPageOwnerMismatch>,
    #[serde(default)]
    pub missing_owner_page_refs: usize,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
    pub all_live_pages_readable: bool,
    #[serde(default)]
    pub boundary: StorageRecoveryBoundaryReport,
    #[serde(default)]
    pub segment_integrity: StorageSegmentIntegrityReport,
    #[serde(default)]
    pub feature_page_layout: StorageFeaturePageLayoutReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryPageError {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryPageOwnerMismatch {
    pub object_key: String,
    pub page_segment_id: u64,
    pub offset: u64,
    pub expected_object_id: u64,
    pub actual_object_id: Option<u64>,
    pub expected_routing_slot: u32,
    pub actual_routing_slot: Option<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeaturePageLayoutReport {
    pub indexed_feature_points: usize,
    pub unique_feature_page_refs: usize,
    pub packed_feature_pages: usize,
    pub legacy_feature_value_pages: usize,
    #[serde(default)]
    pub corrupt_packed_feature_pages: Vec<StorageFeaturePageError>,
    #[serde(default)]
    pub missing_indexed_timestamps: Vec<StorageFeaturePageTimestampMismatch>,
    #[serde(default)]
    pub orphan_packed_timestamps: Vec<StorageFeaturePageTimestampMismatch>,
    #[serde(default)]
    pub duplicate_packed_timestamps: Vec<StorageFeaturePageTimestampMismatch>,
}

impl StorageFeaturePageLayoutReport {
    fn has_errors(&self) -> bool {
        !self.corrupt_packed_feature_pages.is_empty()
            || !self.missing_indexed_timestamps.is_empty()
            || !self.orphan_packed_timestamps.is_empty()
            || !self.duplicate_packed_timestamps.is_empty()
    }

    fn mismatch_count(&self) -> usize {
        self.missing_indexed_timestamps.len()
            + self.orphan_packed_timestamps.len()
            + self.duplicate_packed_timestamps.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeaturePageError {
    pub key: String,
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeaturePageTimestampMismatch {
    pub key: String,
    pub timestamp_ms: u64,
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObjectLifecycleReport {
    pub live_object_ids: u64,
    pub live_page_refs: u64,
    pub stale_object_ids: u64,
    pub tombstoned_object_ids: u64,
    pub reused_object_id_conflicts: u64,
    pub missing_owner_page_refs: u64,
    pub owner_mismatch_page_refs: u64,
    #[serde(default)]
    pub reused_object_ids: Vec<u64>,
    #[serde(default)]
    pub tombstoned_object_keys: Vec<String>,
}

impl StorageObjectLifecycleReport {
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoverySegmentLiveReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub page_count: u64,
    pub live_page_refs: u64,
    pub readable_live_page_refs: u64,
    pub unreadable_live_page_refs: u64,
    pub stale_page_estimate: u64,
    pub live_physical_bytes: u64,
    pub live_logical_bytes: u64,
    pub live_object_count: u64,
    pub live_routing_slot_count: u64,
    pub live_ref_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSegmentIntegrityReport {
    pub shard_id: ShardId,
    pub indexed_page_segment_count: usize,
    pub discovered_page_segment_count: usize,
    pub live_page_segment_count: usize,
    pub orphan_page_segment_count: usize,
    pub stale_page_ref_count: usize,
    pub corrupt_page_segment_count: usize,
    pub unreadable_page_ref_count: usize,
    pub unreadable_page_bytes: u64,
    pub owner_mismatch_page_ref_count: usize,
    pub missing_owner_page_ref_count: usize,
    pub reclaim_required: bool,
    pub integrity_ok: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotStorageSummary {
    pub routing_slot: u32,
    pub object_count: u64,
    pub page_ref_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub dirty_object_count: u64,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    #[serde(default)]
    pub page_segment_ids: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_compacted_zone: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpManifest {
    pub version: u32,
    pub shard_id: ShardId,
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dump_generation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    pub created_unix_ms: u64,
    pub slot_ids: Vec<u32>,
    pub page_segment_ids: Vec<u64>,
    pub oplog_sequence: u64,
    pub index_log_sequence: u64,
    pub live_page_refs: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub slot_summaries: Vec<SlotStorageSummary>,
    #[serde(
        default,
        skip_serializing_if = "StorageObjectLifecycleReport::is_empty"
    )]
    pub object_lifecycle: StorageObjectLifecycleReport,
    #[serde(default)]
    pub index_bytes: Vec<u8>,
    #[serde(default)]
    pub index_sha256: String,
    pub checksum: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpInstallMarker {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub phase: String,
    pub oplog_sequence: u64,
    pub index_log_sequence: u64,
    pub created_unix_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpInstallPreflightReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub install_safe: bool,
    pub blockers: Vec<String>,
    pub current_oplog_sequence: u64,
    pub current_index_log_sequence: u64,
    pub manifest_oplog_sequence: u64,
    pub manifest_index_log_sequence: u64,
    pub missing_page_segment_ids: Vec<u64>,
    pub corrupt_page_segment_ids: Vec<u64>,
    pub unreadable_page_ref_count: usize,
    pub unreadable_page_bytes: u64,
    pub stale_manifest: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpManifestChainIssue {
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpManifestPrunePlan {
    pub shard_id: ShardId,
    pub retained_manifest_ids: Vec<String>,
    pub prunable_manifest_ids: Vec<String>,
    pub prunable_marker_manifest_ids: Vec<String>,
    pub blocked_manifest_ids: Vec<String>,
    #[serde(default)]
    pub follower_blocks: Vec<SlotDumpFollowerRetentionBlock>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpManifestPruneReport {
    pub shard_id: ShardId,
    pub plan: SlotDumpManifestPrunePlan,
    pub removed_manifest_ids: Vec<String>,
    pub removed_marker_files: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpInstallRollForwardReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub interrupted_phase: String,
    pub can_roll_forward: bool,
    #[serde(default)]
    pub can_retry_install: bool,
    pub completed_commit: bool,
    #[serde(default)]
    pub completed_install: bool,
    #[serde(default)]
    pub obsolete_marker_files_removed: usize,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpFollowerReplayCursor {
    pub follower_id: String,
    pub shard_id: ShardId,
    pub oplog_sequence: u64,
    pub index_log_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpFollowerRetentionBlock {
    pub follower_id: String,
    pub manifest_id: String,
    pub manifest_oplog_sequence: u64,
    pub manifest_index_log_sequence: u64,
    pub cursor_oplog_sequence: u64,
    pub cursor_index_log_sequence: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecyclePlan {
    pub shard_id: ShardId,
    pub dirty_slots: Vec<u32>,
    pub selected_dump_slots: Vec<u32>,
    #[serde(default)]
    pub undumped_oplog_records: u64,
    #[serde(default)]
    pub dump_delayed: bool,
    pub slot_summaries: Vec<SlotStorageSummary>,
    pub live_page_segment_ids: Vec<u64>,
    pub stale_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub reclaim_candidates: Vec<StorageReclaimCandidate>,
    pub delayed_destroy_page_segment_ids: Vec<u64>,
    pub reclaimable_physical_bytes: u64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReclaimCandidate {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub stale_physical_bytes: u64,
    pub page_count: u64,
    pub live_page_refs: u64,
    pub stale_page_estimate: u64,
    pub live_ref_density_basis_points: u64,
    pub reclaim_score: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecycleReport {
    pub shard_id: ShardId,
    pub plan: StorageLifecyclePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dump_manifest: Option<SlotDumpManifest>,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    #[serde(default)]
    pub cache_warmup_page_refs: usize,
    #[serde(default)]
    pub cache_warmup: StorageCacheWarmupReport,
    pub delayed_destroy_purged_segments: Vec<u64>,
    pub delayed_destroy_purged_bytes: u64,
    #[serde(default)]
    pub manifest_prune_plan: SlotDumpManifestPrunePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_prune_report: Option<SlotDumpManifestPruneReport>,
    #[serde(default)]
    pub install_roll_forward_reports: Vec<SlotDumpInstallRollForwardReport>,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheWarmupReport {
    pub shard_id: ShardId,
    pub selected_slots: Vec<u32>,
    pub considered_page_refs: usize,
    pub skipped_page_refs: usize,
    pub already_cached_page_refs: usize,
    pub page_store_reads: usize,
    pub warmed_page_refs: usize,
    pub failed_page_refs: usize,
    pub warmed_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryBoundaryReport {
    pub shard_id: ShardId,
    pub latest_safe_oplog_sequence: u64,
    pub latest_safe_index_log_sequence: u64,
    pub latest_dump_oplog_sequence: u64,
    pub latest_dump_index_log_sequence: u64,
    pub selected_replay_oplog_sequence: u64,
    pub selected_replay_index_log_sequence: u64,
    pub orphan_page_segment_ids: Vec<u64>,
    pub missing_dump_slot_ids: Vec<u32>,
    pub stale_index_page_refs: Vec<StorageRecoveryPageError>,
    #[serde(default)]
    pub interrupted_slot_dump_installs: Vec<SlotDumpInstallMarker>,
    #[serde(default)]
    pub prepared_slot_dump_install_count: usize,
    #[serde(default)]
    pub installed_slot_dump_install_count: usize,
    #[serde(default)]
    pub unknown_slot_dump_install_count: usize,
    #[serde(default)]
    pub manifest_chain_issues: Vec<SlotDumpManifestChainIssue>,
    #[serde(default)]
    pub owner_mismatch_page_refs: Vec<StorageRecoveryPageOwnerMismatch>,
    #[serde(default)]
    pub missing_owner_page_refs: usize,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
    pub corrupt_page_segment_ids: Vec<u64>,
    pub unreadable_page_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecycleRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub selected_dump_slots: Vec<u32>,
    #[serde(default)]
    pub max_dump_slots_per_round: usize,
    #[serde(default)]
    pub min_undumped_oplog_records: u64,
    #[serde(default)]
    pub purge_delayed_destroy: bool,
    #[serde(default)]
    pub prune_slot_dump_manifests: bool,
    #[serde(default)]
    pub roll_forward_slot_dump_installs: bool,
    #[serde(default)]
    pub follower_replay_cursors: Vec<SlotDumpFollowerReplayCursor>,
    #[serde(default)]
    pub invalidate_cache: bool,
    #[serde(default)]
    pub warm_cache: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProductionReadinessPolicy {
    #[serde(default)]
    pub max_dirty_slots: Option<usize>,
    #[serde(default)]
    pub max_stale_page_segments: Option<usize>,
    #[serde(default)]
    pub max_orphan_page_segments: Option<usize>,
    #[serde(default)]
    pub max_undumped_oplog_records: Option<u64>,
    #[serde(default)]
    pub require_slot_dump_manifest: bool,
    #[serde(default)]
    pub block_on_warnings: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProductionReadinessRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub policy: StorageProductionReadinessPolicy,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProductionReadinessReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub policy: StorageProductionReadinessPolicy,
    pub production_ready: bool,
    pub blockers: Vec<String>,
    pub warnings: Vec<String>,
    pub dirty_slot_count: usize,
    pub stale_page_segment_count: usize,
    pub orphan_page_segment_count: usize,
    #[serde(default)]
    pub undumped_oplog_records: u64,
    pub corrupt_page_segment_count: usize,
    pub unreadable_page_ref_count: usize,
    pub owner_mismatch_page_ref_count: usize,
    pub missing_owner_page_ref_count: u64,
    pub reused_object_id_conflict_count: u64,
    pub interrupted_slot_dump_install_count: usize,
    #[serde(default)]
    pub prepared_slot_dump_install_count: usize,
    #[serde(default)]
    pub installed_slot_dump_install_count: usize,
    #[serde(default)]
    pub unknown_slot_dump_install_count: usize,
    pub slot_dump_manifest_count: usize,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub page_store_bytes_written: u64,
    pub boundary: StorageRecoveryBoundaryReport,
    pub object_lifecycle: StorageObjectLifecycleReport,
    #[serde(default)]
    pub segment_integrity: StorageSegmentIntegrityReport,
    #[serde(default)]
    pub log_compatibility: StorageLogCompatibilityReport,
    #[serde(default)]
    pub page_format_compatibility: StoragePageFormatCompatibilityReport,
    #[serde(default)]
    pub feature_page_layout: StorageFeaturePageLayoutReport,
    pub feature_page_layout_mismatch_count: usize,
    pub corrupt_feature_page_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLogCompatibilityReport {
    pub shard_id: ShardId,
    pub oplog_format: String,
    pub index_log_format: String,
    pub rust_native_replay_safe: bool,
    pub cxx_binary_compatible: bool,
    pub oplog_last_sequence: u64,
    pub index_log_last_sequence: u64,
    pub oplog_records: usize,
    pub index_log_records: usize,
    pub oplog_bytes: u64,
    pub index_log_bytes: u64,
    pub compatibility_gaps: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageFormatCompatibilityReport {
    pub shard_id: ShardId,
    pub page_format: String,
    pub rust_envelope_version: u8,
    pub rust_native_read_safe: bool,
    pub cxx_page_header_compatible: bool,
    pub checksum_protected: bool,
    pub object_ids_embedded: bool,
    pub routing_slots_embedded: bool,
    pub compression_supported: bool,
    pub active_zones: u64,
    pub sealed_zones: u64,
    pub delayed_destroy_zones: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub page_store_writes: u64,
    pub page_store_bytes_written: u64,
    pub logical_bytes_written: u64,
    pub compressed_records_written: u64,
    pub compatibility_gaps: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheSlotSummary {
    pub routing_slot: u32,
    pub entry_count: usize,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
    #[serde(default)]
    pub pinned_entries: usize,
    #[serde(default)]
    pub pinned_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheInspectionReport {
    pub shard_id: ShardId,
    pub stats: CacheStats,
    pub entries: Vec<CacheEntryInfo>,
    pub slot_summaries: Vec<StorageCacheSlotSummary>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheInvalidateSlotRequest {
    pub shard_id: ShardId,
    pub routing_slot: u32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CppGoldenCaseReport {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CppGoldenCorpusReport {
    pub corpus: String,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub cases: Vec<CppGoldenCaseReport>,
}

impl CppGoldenCorpusReport {
    pub fn passed(&self) -> bool {
        self.failed_cases == 0 && self.total_cases == self.passed_cases
    }
}

pub fn cpp_feature_sequence_golden_corpus_report() -> CppGoldenCorpusReport {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let mut cases = Vec::new();

    let matching = SequenceFeatureRow {
        timestamp_ms: 1_000,
        gid: 42,
        action_type: 7,
        duration: 33,
        author_id: 9_001,
    };
    let replacement = SequenceFeatureRow {
        timestamp_ms: 1_001,
        gid: 43,
        action_type: 7,
        duration: 34,
        author_id: 9_002,
    };
    let non_matching = SequenceFeatureRow {
        timestamp_ms: 1_002,
        gid: 44,
        action_type: 8,
        duration: 35,
        author_id: 9_003,
    };

    record_golden_case(
        &mut cases,
        "cpp_feature_proto_roundtrip",
        SequenceFeatureRow::decode_cpp_feature_value(
            matching.timestamp_ms,
            &matching.encode_cpp_feature_value(),
        ) == Some(matching.clone()),
        "C++ feature protobuf fields gid/action_type/duration/author_id round-trip",
    );

    let duplicate_filters = parse_cpp_feature_filters(["gid = 42", "duration > 30", "gid != 42"]);
    record_golden_case(
        &mut cases,
        "cpp_feature_filter_last_field_wins",
        matches!(duplicate_filters, Ok(ref filters) if filters.len() == 2
            && filters[0].field == "gid"
            && filters[0].op == FeatureFilterOp::NotEqual
            && filters[0].value == 42
            && filters[1].field == "duration"),
        "C++ duplicate filter fields replace the previous field predicate",
    );

    let append = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAppend {
            key: "cpp-golden-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: matching.timestamp_ms,
                    value: matching.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: replacement.timestamp_ms,
                    value: replacement.encode_cpp_feature_value(),
                },
                FeaturePoint {
                    timestamp_ms: non_matching.timestamp_ms,
                    value: non_matching.encode_cpp_feature_value(),
                },
            ],
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_append_status",
        append.status.ok,
        "C++ feature points append through the Rust engine",
    );

    let filtered = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureQueryFiltered {
            key: "cpp-golden-feature".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: Some(10),
            filters: parse_cpp_feature_filters(["action_type = 7", "duration <= 34"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_filtered_query",
        matches!(
            filtered.response,
            CommandResponse::FeaturePoints { ref points }
                if points.iter().map(|point| point.timestamp_ms).collect::<Vec<_>>()
                    == vec![matching.timestamp_ms, replacement.timestamp_ms]
        ),
        "C++ protobuf feature filters select matching timestamp/value rows",
    );

    let aggregate = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::FeatureAggQuery {
            key: "cpp-golden-aggregate".to_string(),
            start_ms: 0,
            end_ms: 10,
            aggregator: "sum".to_string(),
            count: None,
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_feature_empty_sum_aggregate",
        aggregate.response == CommandResponse::Aggregate { value: 0 },
        "Empty C++ feature aggregate returns neutral zero",
    );

    let rows = vec![matching.clone(), replacement.clone(), non_matching.clone()];
    let add_rows = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceAdd {
            key: "cpp-golden-sequence".to_string(),
            rows: rows.clone(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_sequence_add_status",
        add_rows.status.ok,
        "C++ sequence rows append through timestamped KV pages",
    );

    let sequence_query = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::SequenceQuery {
            key: "cpp-golden-sequence".to_string(),
            start_ms: 0,
            end_ms: 2_000,
            count: 10,
            filters: parse_cpp_feature_filters(["gid >= 42", "action_type = 7"])
                .unwrap_or_default(),
        },
    });
    record_golden_case(
        &mut cases,
        "cpp_sequence_filtered_query",
        sequence_query.response
            == CommandResponse::SequenceRows {
                rows: vec![matching, replacement],
            },
        "C++ sequence filters reuse the feature predicate semantics",
    );

    let page_layout = engine.storage_recovery_report(1).feature_page_layout;
    record_golden_case(
        &mut cases,
        "cpp_timestamped_kv_shared_page_layout",
        page_layout.packed_feature_pages >= 1
            && page_layout.unique_feature_page_refs < page_layout.indexed_feature_points
            && !page_layout.has_errors(),
        "Timestamped feature/sequence values share packed pages without layout errors",
    );

    let total_cases = cases.len();
    let passed_cases = cases.iter().filter(|case| case.passed).count();
    CppGoldenCorpusReport {
        corpus: "feature_sequence_cpp_proto_v1".to_string(),
        total_cases,
        passed_cases,
        failed_cases: total_cases.saturating_sub(passed_cases),
        cases,
    }
}

fn record_golden_case(
    cases: &mut Vec<CppGoldenCaseReport>,
    name: &str,
    passed: bool,
    detail: &str,
) {
    cases.push(CppGoldenCaseReport {
        name: name.to_string(),
        passed,
        detail: detail.to_string(),
    });
}

impl TemporalEngine {
    pub fn new(cache: MultiLayerCache) -> Self {
        Self::with_cache_and_page_store(cache, LocalPageStore::default())
    }

    pub fn with_cache_and_page_store(cache: MultiLayerCache, page_store: LocalPageStore) -> Self {
        Self::with_cache_page_store_and_index_dir(cache, page_store, unique_temp_path("indexes"))
    }

    pub fn with_cache_page_store_and_index_dir(
        cache: MultiLayerCache,
        page_store: LocalPageStore,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        let index_dir = index_dir.into();
        let oplog_store = LocalOplogStore::new(index_dir.join("oplogs"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        Self {
            shards: Arc::default(),
            cache,
            page_store,
            oplog_store,
            index_log_store,
            index_dir,
            configs: Arc::default(),
            infos: Arc::default(),
            admissions: Arc::default(),
        }
    }

    pub fn cache(&self) -> MultiLayerCache {
        self.cache.clone()
    }

    pub fn page_store(&self) -> LocalPageStore {
        self.page_store.clone()
    }

    pub fn oplog_store(&self) -> LocalOplogStore {
        self.oplog_store.clone()
    }

    pub fn index_log_store(&self) -> LocalIndexLogStore {
        self.index_log_store.clone()
    }

    pub(crate) fn ingestion_dir(&self) -> PathBuf {
        self.index_dir.join("ingestion")
    }

    pub fn with_local_dirs(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        page_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
    ) -> Self {
        Self::with_local_dirs_and_page_store_options(
            memory_capacity_bytes,
            cache_dir,
            page_store_dir,
            index_dir,
            PageStoreOptions::default(),
        )
    }

    pub fn with_local_dirs_and_page_store_options(
        memory_capacity_bytes: usize,
        cache_dir: impl Into<PathBuf>,
        page_store_dir: impl Into<PathBuf>,
        index_dir: impl Into<PathBuf>,
        page_store_options: PageStoreOptions,
    ) -> Self {
        Self::with_cache_page_store_and_index_dir(
            MultiLayerCache::new(memory_capacity_bytes, cache_dir),
            LocalPageStore::with_options(page_store_dir, page_store_options),
            index_dir,
        )
    }

    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_slot: 0,
            end_routing_slot: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        let _ = self.load_shard_with(request);
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return LoadShardResponse {
                status: Status::error("already_exists", "shard already exists"),
            };
        }
        let state = self.load_index(request.shard_id).unwrap_or_default();
        self.shards
            .write()
            .expect("engine lock poisoned")
            .insert(request.shard_id, state);
        self.configs
            .write()
            .expect("config lock poisoned")
            .entry(request.shard_id)
            .or_default();
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .entry(AdmissionScope::Shard(request.shard_id))
            .or_default();
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_slot: request.start_routing_slot,
                end_routing_slot: request.end_routing_slot,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: 0,
                replica_membership_version: 0,
                membership_valid: true,
                replica_node_ids: Vec::new(),
                leader_node_id: None,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn unload_shard(&self, shard_id: ShardId) {
        let _ = self.unload_shard_with(UnloadShardRequest { shard_id });
    }

    pub fn unload_shard_with(&self, request: UnloadShardRequest) -> UnloadShardResponse {
        if !self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
        {
            return UnloadShardResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
            };
        }
        self.shards
            .write()
            .expect("engine lock poisoned")
            .remove(&request.shard_id);
        self.infos
            .write()
            .expect("info lock poisoned")
            .remove(&request.shard_id);
        self.configs
            .write()
            .expect("config lock poisoned")
            .remove(&request.shard_id);
        self.admissions
            .write()
            .expect("admission lock poisoned")
            .remove(&AdmissionScope::Shard(request.shard_id));
        UnloadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn reload_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        let existing = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let Some(existing) = existing else {
            return self.load_shard_with(request);
        };
        if request.load_version < existing.load_version {
            return LoadShardResponse {
                status: Status::error(
                    "stale_load_version",
                    format!(
                        "reload version {} is older than loaded version {}",
                        request.load_version, existing.load_version
                    ),
                ),
            };
        }
        self.infos.write().expect("info lock poisoned").insert(
            request.shard_id,
            ShardInfo {
                shard_id: request.shard_id,
                loaded: true,
                table_name: request.table_name,
                shard_uri: request.shard_uri,
                start_routing_slot: request.start_routing_slot,
                end_routing_slot: request.end_routing_slot,
                readonly: request.readonly,
                load_version: request.load_version,
                local_node_id: request.local_node_id,
                membership_version: existing.membership_version,
                replica_membership_version: existing.replica_membership_version,
                membership_valid: existing.membership_valid,
                replica_node_ids: existing.replica_node_ids,
                leader_node_id: existing.leader_node_id,
            },
        );
        LoadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, None)
    }

    pub fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, Some(false))
    }

    fn execute_with_storage_override(
        &self,
        request: ExecuteRequest,
        async_storage_override: Option<bool>,
    ) -> ExecuteResponse {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            };
        };
        let command = request.command;
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false)
            && is_write_command(&command)
        {
            return ExecuteResponse {
                status: Status::error("readonly_shard", "readonly shard rejects write command"),
                response: CommandResponse::Empty,
            };
        }
        let mut config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if let Some(async_storage) = async_storage_override {
            config.async_storage = async_storage;
        }
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let write_command = is_write_command(&command);
        if let Err(status) = self.check_admission(request.shard_id, write_command, &config, &info) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        if write_command
            && config
                .maxmemory_bytes
                .map(|limit| self.page_store.stats().bytes_written >= limit)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "storage_quota_exceeded",
                    "shard maxmemory_bytes limit has been reached",
                ),
                response: CommandResponse::Empty,
            };
        }
        if let Err(status) = validate_command_preconditions(
            &self.cache,
            &self.page_store,
            request.shard_id,
            shard,
            &command,
        ) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        let outcome = execute_on_shard(
            &self.cache,
            &self.page_store,
            config.feature_max_size,
            config.async_storage,
            request.shard_id,
            info.as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX),
            shard,
            command.clone(),
        );
        if outcome.mutated {
            for object_key in command_object_keys(&command) {
                shard.dirty_objects.insert(object_key);
            }
            if write_command && !config.async_storage {
                let _ = self.oplog_store.append(request.shard_id, command);
            }
            if !config.async_storage {
                let index_bytes = serialize_index(shard);
                let _ = self
                    .index_log_store
                    .append_json(request.shard_id, &index_bytes);
                let _ = self.persist_index_bytes(request.shard_id, &index_bytes);
            }
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        });
        CheckedExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    fn check_admission(
        &self,
        shard_id: ShardId,
        write_command: bool,
        config: &Config,
        info: &Option<ShardInfo>,
    ) -> Result<(), Status> {
        let limits = admission_limits(shard_id, write_command, config, info);
        if limits.is_empty() {
            return Ok(());
        }
        let now_sec = now_epoch_seconds();
        let mut admissions = self.admissions.write().expect("admission lock poisoned");
        for limit in &limits {
            if limit.limit == 0 {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} is zero", limit.label),
                ));
            }
            let admission = admissions.entry(limit.scope.clone()).or_default();
            reset_admission_window(admission, now_sec);
            let count = admission_count(admission, write_command);
            if *count >= limit.limit {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} limit exceeded", limit.label),
                ));
            }
        }
        for limit in limits {
            let admission = admissions.entry(limit.scope).or_default();
            reset_admission_window(admission, now_sec);
            *admission_count(admission, write_command) += 1;
        }
        Ok(())
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        if !self.is_shard_loaded(request.shard_id) {
            return Status::error("shard_not_found", "shard is not loaded");
        }
        let mut configs = self.configs.write().expect("config lock poisoned");
        let current = configs.get(&request.shard_id).cloned().unwrap_or_default();
        if request.config.version < current.version {
            return Status::error("failed_precondition", "legacy config version");
        }
        if request.config.version == current.version {
            return Status::ok();
        }
        configs.insert(request.shard_id, request.config);
        Status::ok()
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
        if !self.is_shard_loaded(shard_id) {
            return GetConfigResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
                config: Config::default(),
            };
        }
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        GetConfigResponse {
            status: Status::ok(),
            config,
        }
    }

    fn is_shard_loaded(&self, shard_id: ShardId) -> bool {
        self.infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
    }

    pub fn get_info(&self, shard_id: ShardId) -> GetInfoResponse {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        GetInfoResponse {
            status: if info.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            info,
        }
    }

    pub fn update_membership(&self, request: MembershipUpdateRequest) -> Status {
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            if request.membership_version < info.membership_version {
                return Status::error("failed_precondition", "legacy membership info");
            }
            let global_update = request.membership_version > info.membership_version;
            if !global_update
                && request.replica_membership_version == info.replica_membership_version
            {
                return Status::ok();
            }
            if request.replica_membership_version < info.replica_membership_version {
                return Status::error("failed_precondition", "legacy membership unit info");
            }
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            info.membership_version = request.membership_version;
            info.replica_membership_version = request.replica_membership_version;
            info.membership_valid = info
                .local_node_id
                .map(|node_id| info.replica_node_ids.contains(&node_id))
                .unwrap_or(true);
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let stats = self.shard_stats(shard_id);
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn rust_storage_observation(&self, shard_id: ShardId) -> Option<RustStorageObservation> {
        self.shard_stats(shard_id)
            .map(|stats| RustStorageObservation {
                shard_id,
                observed_memory_hit: stats.cache.memory_hits > 0,
                observed_block_cache_hit: stats.cache.disk_hits > 0,
                observed_local_file_read: stats.page_store.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_page_bytes_written: stats.page_store.bytes_written,
                local_page_bytes_read: stats.page_store.bytes_read,
                cache: stats.cache,
                page_store: stats.page_store,
            })
    }

    pub fn loaded_shard_stats(&self) -> Vec<ShardStats> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.shard_stats(shard_id))
            .collect()
    }

    pub fn loaded_shard_ids(&self) -> Vec<ShardId> {
        let mut shard_ids = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids
    }

    pub fn slot_storage_summaries(&self, shard_id: ShardId) -> Vec<SlotStorageSummary> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        let summaries = slot_storage_summaries(shard, start, end);
        if let Some(manifest) = latest_slot_dump_manifest_at(&self.index_dir, shard_id) {
            merge_last_dump_sequence(summaries, &manifest)
        } else {
            summaries
        }
    }

    pub fn routing_slot_for_key(&self, shard_id: ShardId, key: &str) -> u32 {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        page_routing_slot(key, start, end)
    }

    pub fn create_slot_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> Result<SlotDumpManifest, Status> {
        let selected_slots = selected_slots.into_iter().collect::<BTreeSet<_>>();
        let summaries = self.slot_storage_summaries(shard_id);
        if summaries.is_empty()
            && !self
                .shards
                .read()
                .expect("engine lock poisoned")
                .contains_key(&shard_id)
        {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        }
        let mut slot_summaries = summaries
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect::<Vec<_>>();
        slot_summaries.sort_by_key(|summary| summary.routing_slot);
        let mut page_segment_ids = slot_summaries
            .iter()
            .flat_map(|summary| summary.page_segment_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        page_segment_ids.sort_unstable();
        let oplog_sequence = self.oplog_store.stats(shard_id).last_sequence;
        let index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let index_bytes = self
            .export_index_bytes(shard_id)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        let index_sha256 = sha256_hex_bytes(&index_bytes);
        let created_unix_ms = now_ms();
        let manifest_id = format!("{shard_id}-{index_log_sequence}-{created_unix_ms}");
        let parent_manifest_id = latest_slot_dump_manifest_at(&self.index_dir, shard_id)
            .map(|manifest| manifest.manifest_id);
        let object_lifecycle = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .get(&shard_id)
            .map(|shard| {
                storage_object_lifecycle_report_for_slots(shard_id, shard, &selected_slots, |key| {
                    self.routing_slot_for_key(shard_id, key)
                })
            })
            .unwrap_or_default();
        let mut manifest = SlotDumpManifest {
            version: 3,
            shard_id,
            manifest_id,
            dump_generation_id: String::new(),
            parent_manifest_id,
            created_unix_ms,
            slot_ids: slot_summaries
                .iter()
                .map(|summary| summary.routing_slot)
                .collect(),
            page_segment_ids,
            oplog_sequence,
            index_log_sequence,
            live_page_refs: slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            logical_bytes: slot_summaries
                .iter()
                .map(|summary| summary.logical_bytes)
                .sum(),
            physical_bytes: slot_summaries
                .iter()
                .map(|summary| summary.physical_bytes)
                .sum(),
            slot_summaries,
            object_lifecycle,
            index_bytes,
            index_sha256,
            checksum: String::new(),
        };
        manifest.dump_generation_id = slot_dump_generation_id(&manifest);
        manifest.checksum = slot_dump_manifest_checksum(&manifest)?;
        self.persist_slot_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        Ok(manifest)
    }

    pub fn list_slot_dump_manifests(&self, shard_id: ShardId) -> Vec<SlotDumpManifest> {
        list_slot_dump_manifests_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn interrupted_slot_dump_installs(&self, shard_id: ShardId) -> Vec<SlotDumpInstallMarker> {
        interrupted_slot_dump_installs_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn slot_dump_manifest_prune_plan(&self, shard_id: ShardId) -> SlotDumpManifestPrunePlan {
        self.slot_dump_manifest_prune_plan_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn slot_dump_manifest_prune_plan_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
    ) -> SlotDumpManifestPrunePlan {
        let follower_cursors = follower_cursors.into_iter().collect::<Vec<_>>();
        slot_dump_manifest_prune_plan_at(&self.index_dir, shard_id, &follower_cursors)
            .unwrap_or_else(|err| SlotDumpManifestPrunePlan {
                shard_id,
                reasons: vec![format!("slot_dump_prune_plan_failed:{err}")],
                ..SlotDumpManifestPrunePlan::default()
            })
    }

    pub fn slot_dump_install_roll_forward_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<SlotDumpInstallRollForwardReport> {
        self.interrupted_slot_dump_installs(shard_id)
            .into_iter()
            .map(|marker| self.slot_dump_install_roll_forward_report(&marker))
            .collect()
    }

    pub fn roll_forward_slot_dump_installs(
        &self,
        shard_id: ShardId,
    ) -> Vec<SlotDumpInstallRollForwardReport> {
        self.interrupted_slot_dump_installs(shard_id)
            .into_iter()
            .map(|marker| {
                let mut report = self.slot_dump_install_roll_forward_report(&marker);
                if report.can_retry_install {
                    match slot_dump_manifest_at(
                        &self.index_dir,
                        marker.shard_id,
                        &marker.manifest_id,
                    )
                    .ok()
                    .flatten()
                    .map(|manifest| self.install_slot_dump_manifest(&manifest))
                    {
                        Some(Ok(())) => {
                            report.completed_install = true;
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_slot_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "install_retried".to_string();
                        }
                        Some(Err(status)) => {
                            report.can_retry_install = false;
                            report.reason = format!("install_retry_failed:{}", status.code);
                        }
                        None => {
                            report.can_retry_install = false;
                            report.reason = "missing_manifest".to_string();
                        }
                    }
                } else if report.can_roll_forward {
                    match self.persist_slot_dump_install_marker_by_fields(
                        marker.shard_id,
                        &marker.manifest_id,
                        "commit",
                        marker.oplog_sequence,
                        marker.index_log_sequence,
                    ) {
                        Ok(()) => {
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_slot_dump_install_markers(
                                    &self.index_dir,
                                    marker.shard_id,
                                    &marker.manifest_id,
                                )
                                .unwrap_or_default();
                            report.reason = "commit_marker_written".to_string();
                        }
                        Err(err) => {
                            report.can_roll_forward = false;
                            report.reason = format!("commit_marker_failed:{err}");
                        }
                    }
                }
                report
            })
            .collect()
    }

    pub fn apply_slot_dump_manifest_prune(&self, shard_id: ShardId) -> SlotDumpManifestPruneReport {
        self.apply_slot_dump_manifest_prune_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn apply_slot_dump_manifest_prune_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
    ) -> SlotDumpManifestPruneReport {
        let plan =
            self.slot_dump_manifest_prune_plan_with_follower_cursors(shard_id, follower_cursors);
        let mut removed_manifest_ids = Vec::new();
        for manifest_id in &plan.prunable_manifest_ids {
            let path = slot_dump_manifest_path(&self.index_dir, shard_id, manifest_id);
            if fs::remove_file(path).is_ok() {
                removed_manifest_ids.push(manifest_id.clone());
            }
        }
        let mut removed_marker_files = 0usize;
        if let Ok(marker_files) = slot_dump_install_marker_files_at(&self.index_dir, shard_id) {
            let prunable_marker_manifest_ids = plan
                .prunable_marker_manifest_ids
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>();
            for (marker, path) in marker_files {
                if prunable_marker_manifest_ids.contains(&marker.manifest_id)
                    && fs::remove_file(path).is_ok()
                {
                    removed_marker_files = removed_marker_files.saturating_add(1);
                }
            }
        }
        SlotDumpManifestPruneReport {
            shard_id,
            plan,
            removed_manifest_ids,
            removed_marker_files,
        }
    }

    fn slot_dump_install_roll_forward_report(
        &self,
        marker: &SlotDumpInstallMarker,
    ) -> SlotDumpInstallRollForwardReport {
        if marker.phase != "install" && marker.phase != "prepare" {
            return SlotDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "unknown_interrupted_phase".to_string(),
            };
        }
        let Some(manifest) =
            slot_dump_manifest_at(&self.index_dir, marker.shard_id, &marker.manifest_id)
                .ok()
                .flatten()
        else {
            return SlotDumpInstallRollForwardReport {
                shard_id: marker.shard_id,
                manifest_id: marker.manifest_id.clone(),
                interrupted_phase: marker.phase.clone(),
                can_roll_forward: false,
                completed_commit: false,
                completed_install: false,
                can_retry_install: false,
                obsolete_marker_files_removed: 0,
                reason: "missing_manifest".to_string(),
            };
        };
        let reason = match self.validate_slot_dump_manifest(&manifest) {
            Ok(()) if marker.phase == "install" => "commit_ready".to_string(),
            Ok(()) => "install_retry_ready".to_string(),
            Err(status) => format!("manifest_invalid:{}", status.code),
        };
        SlotDumpInstallRollForwardReport {
            shard_id: marker.shard_id,
            manifest_id: marker.manifest_id.clone(),
            interrupted_phase: marker.phase.clone(),
            can_roll_forward: reason == "commit_ready",
            can_retry_install: reason == "install_retry_ready",
            completed_commit: false,
            completed_install: false,
            obsolete_marker_files_removed: 0,
            reason,
        }
    }

    pub fn validate_slot_dump_manifest(&self, manifest: &SlotDumpManifest) -> Result<(), Status> {
        let expected = slot_dump_manifest_checksum(manifest)
            .map_err(|_| Status::error("slot_dump_corrupt", "slot dump manifest is corrupt"))?;
        if manifest.checksum != expected {
            return Err(Status::error(
                "slot_dump_checksum_mismatch",
                "slot dump manifest checksum mismatch",
            ));
        }
        if manifest.version >= 2 && manifest.dump_generation_id.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_generation",
                "slot dump manifest is missing dump generation id",
            ));
        }
        if manifest.index_bytes.is_empty() {
            return Err(Status::error(
                "slot_dump_partial_manifest",
                "slot dump manifest is missing index bytes",
            ));
        }
        let actual_index_sha256 = sha256_hex_bytes(&manifest.index_bytes);
        if manifest.index_sha256 != actual_index_sha256 {
            return Err(Status::error(
                "slot_dump_index_checksum_mismatch",
                "slot dump manifest index checksum mismatch",
            ));
        }
        let existing_segments = self
            .page_store
            .segment_ids()
            .map_err(|err| Status::error("slot_dump_invalid", err.to_string()))?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing = manifest
            .page_segment_ids
            .iter()
            .copied()
            .filter(|id| !existing_segments.contains(id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_page_segments",
                format!("slot dump references missing page segments: {missing:?}"),
            ));
        }
        let restored = serde_json::from_slice::<ShardState>(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        if manifest_slots.len() != manifest.slot_ids.len()
            || manifest.slot_ids != manifest_slots.iter().copied().collect::<Vec<_>>()
        {
            return Err(Status::error(
                "slot_dump_slot_ids_not_canonical",
                "slot dump manifest slot ids must be sorted and unique",
            ));
        }
        let canonical_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if manifest.page_segment_ids != canonical_page_segment_ids {
            return Err(Status::error(
                "slot_dump_page_segment_ids_not_canonical",
                "slot dump manifest page segment ids must be sorted and unique",
            ));
        }
        let live_page_entries = collect_live_page_entries(&restored)
            .into_iter()
            .filter(|entry| {
                let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
                    self.routing_slot_for_key(manifest.shard_id, &entry.object_key)
                });
                manifest_slots.is_empty() || manifest_slots.contains(&routing_slot)
            })
            .collect::<Vec<_>>();
        if live_page_entries.len() as u64 != manifest.live_page_refs {
            return Err(Status::error(
                "slot_dump_live_ref_mismatch",
                format!(
                    "slot dump expected {} live page refs but index has {}",
                    manifest.live_page_refs,
                    live_page_entries.len()
                ),
            ));
        }
        let expected_slot_summaries =
            slot_dump_manifest_comparable_summaries(&restored, &manifest_slots);
        let actual_slot_summaries = comparable_slot_dump_summaries(manifest.slot_summaries.clone());
        if actual_slot_summaries != expected_slot_summaries {
            return Err(Status::error(
                "slot_dump_slot_summary_mismatch",
                "slot dump slot summaries do not match restored index page ownership",
            ));
        }
        let expected_logical_bytes = expected_slot_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let expected_physical_bytes = expected_slot_summaries
            .iter()
            .map(|summary| summary.physical_bytes)
            .sum::<u64>();
        if manifest.logical_bytes != expected_logical_bytes
            || manifest.physical_bytes != expected_physical_bytes
        {
            return Err(Status::error(
                "slot_dump_byte_accounting_mismatch",
                format!(
                    "slot dump byte totals logical={} physical={} do not match restored index logical={} physical={}",
                    manifest.logical_bytes,
                    manifest.physical_bytes,
                    expected_logical_bytes,
                    expected_physical_bytes
                ),
            ));
        }
        if manifest.version >= 3 {
            let expected_object_lifecycle = storage_object_lifecycle_report_for_slots(
                manifest.shard_id,
                &restored,
                &manifest_slots,
                |key| self.routing_slot_for_key(manifest.shard_id, key),
            );
            if manifest.object_lifecycle != expected_object_lifecycle {
                return Err(Status::error(
                    "slot_dump_object_lifecycle_mismatch",
                    "slot dump object lifecycle metadata does not match restored index",
                ));
            }
        }
        let referenced_page_segment_ids = live_page_entries
            .iter()
            .map(|entry| entry.address.page_segment_id)
            .collect::<BTreeSet<_>>();
        let manifest_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if referenced_page_segment_ids != manifest_page_segment_ids {
            return Err(Status::error(
                "slot_dump_page_segment_mismatch",
                format!(
                    "slot dump page segment ids {manifest_page_segment_ids:?} do not match live refs {referenced_page_segment_ids:?}"
                ),
            ));
        }
        if !manifest.dump_generation_id.is_empty()
            && manifest.dump_generation_id != slot_dump_generation_id(manifest)
        {
            return Err(Status::error(
                "slot_dump_generation_mismatch",
                "slot dump manifest generation id does not match its sequence, slots, pages, and index checksum",
            ));
        }
        let mut unreadable_page_refs = 0usize;
        let mut unreadable_page_bytes = 0u64;
        for entry in live_page_entries {
            if self.page_store.read(&entry.address).is_err() {
                unreadable_page_refs = unreadable_page_refs.saturating_add(1);
                unreadable_page_bytes = unreadable_page_bytes.saturating_add(entry.address.length);
            }
        }
        if unreadable_page_refs > 0 {
            return Err(Status::error(
                "slot_dump_unreadable_page_refs",
                format!(
                    "slot dump has {unreadable_page_refs} unreadable page refs covering {unreadable_page_bytes} bytes"
                ),
            ));
        }
        Ok(())
    }

    pub fn slot_dump_install_preflight_report(
        &self,
        manifest: &SlotDumpManifest,
    ) -> SlotDumpInstallPreflightReport {
        let current_oplog_sequence = self.oplog_store.stats(manifest.shard_id).last_sequence;
        let current_index_log_sequence =
            self.index_log_store.stats(manifest.shard_id).last_sequence;
        let existing_segments = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .filter(|id| !existing_segments.contains(id))
            .collect::<Vec<_>>();
        let corrupt_page_segment_ids = self
            .page_store
            .segment_reports()
            .unwrap_or_default()
            .into_iter()
            .filter(|report| {
                report.has_corruption && manifest.page_segment_ids.contains(&report.page_segment_id)
            })
            .map(|report| report.page_segment_id)
            .collect::<Vec<_>>();
        let stale_manifest = current_index_log_sequence > manifest.index_log_sequence;
        let mut blockers = Vec::new();
        if stale_manifest {
            blockers.push("stale_manifest_sequence".to_string());
        }
        if !missing_page_segment_ids.is_empty() {
            blockers.push("missing_page_segments".to_string());
        }
        if !corrupt_page_segment_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }

        let mut unreadable_page_ref_count = 0usize;
        let mut unreadable_page_bytes = 0u64;
        if !manifest.index_bytes.is_empty() && missing_page_segment_ids.is_empty() {
            if let Ok(restored) = serde_json::from_slice::<ShardState>(&manifest.index_bytes) {
                let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
                for entry in collect_live_page_entries(&restored) {
                    let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
                        self.routing_slot_for_key(manifest.shard_id, &entry.object_key)
                    });
                    if manifest_slots.is_empty() || manifest_slots.contains(&routing_slot) {
                        if self.page_store.read(&entry.address).is_err() {
                            unreadable_page_ref_count = unreadable_page_ref_count.saturating_add(1);
                            unreadable_page_bytes =
                                unreadable_page_bytes.saturating_add(entry.address.length);
                        }
                    }
                }
            } else {
                blockers.push("invalid_manifest_index".to_string());
            }
        }
        if unreadable_page_ref_count > 0 {
            blockers.push("unreadable_page_refs".to_string());
        }
        blockers.sort();
        blockers.dedup();

        SlotDumpInstallPreflightReport {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            install_safe: blockers.is_empty(),
            blockers,
            current_oplog_sequence,
            current_index_log_sequence,
            manifest_oplog_sequence: manifest.oplog_sequence,
            manifest_index_log_sequence: manifest.index_log_sequence,
            missing_page_segment_ids,
            corrupt_page_segment_ids,
            unreadable_page_ref_count,
            unreadable_page_bytes,
            stale_manifest,
        }
    }

    pub fn install_slot_dump_manifest(&self, manifest: &SlotDumpManifest) -> Result<(), Status> {
        self.validate_slot_dump_manifest(manifest)?;
        let preflight = self.slot_dump_install_preflight_report(manifest);
        if !preflight.install_safe {
            if preflight.stale_manifest {
                return Err(Status::error(
                    "slot_dump_stale_manifest",
                    format!(
                        "manifest index sequence {} is older than current {}",
                        manifest.index_log_sequence, preflight.current_index_log_sequence
                    ),
                ));
            }
            if preflight.unreadable_page_ref_count > 0 {
                return Err(Status::error(
                    "slot_dump_unreadable_page_refs",
                    format!(
                        "slot dump has {} unreadable page refs covering {} bytes",
                        preflight.unreadable_page_ref_count, preflight.unreadable_page_bytes
                    ),
                ));
            }
            return Err(Status::error(
                "slot_dump_install_preflight_failed",
                format!(
                    "slot dump install preflight blockers: {:?}",
                    preflight.blockers
                ),
            ));
        }
        self.validate_slot_dump_generation_for_install(manifest)?;
        let current_index_sequence = self.index_log_store.stats(manifest.shard_id).last_sequence;
        if current_index_sequence > manifest.index_log_sequence {
            return Err(Status::error(
                "slot_dump_stale_manifest",
                format!(
                    "manifest index sequence {} is older than current {}",
                    manifest.index_log_sequence, current_index_sequence
                ),
            ));
        }
        let restored = serde_json::from_slice::<ShardState>(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "prepare")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_index_bytes(manifest.shard_id, &manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "install")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        if self
            .shards
            .read()
            .expect("engine lock poisoned")
            .contains_key(&manifest.shard_id)
        {
            self.shards
                .write()
                .expect("engine lock poisoned")
                .insert(manifest.shard_id, restored);
        }
        self.persist_slot_dump_manifest(manifest)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "commit")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        Ok(())
    }

    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        let slot_summaries = self.slot_storage_summaries(request.shard_id);
        let dirty_slots = slot_summaries
            .iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_slot)
            .collect::<Vec<_>>();
        let latest_dump_oplog_sequence =
            latest_slot_dump_manifest_at(&self.index_dir, request.shard_id)
                .map(|manifest| manifest.oplog_sequence)
                .unwrap_or_default();
        let current_oplog_sequence = self.oplog_store.stats(request.shard_id).last_sequence;
        let undumped_oplog_records =
            current_oplog_sequence.saturating_sub(latest_dump_oplog_sequence);
        let explicit_slots = !request.selected_dump_slots.is_empty();
        let dump_delayed = !explicit_slots
            && request.min_undumped_oplog_records > 0
            && undumped_oplog_records < request.min_undumped_oplog_records;
        let mut selected_dump_slots = if explicit_slots {
            request.selected_dump_slots.clone()
        } else if dump_delayed {
            Vec::new()
        } else {
            dirty_slots.clone()
        };
        if request.max_dump_slots_per_round > 0
            && selected_dump_slots.len() > request.max_dump_slots_per_round
        {
            selected_dump_slots.truncate(request.max_dump_slots_per_round);
        }
        let live_page_segment_ids = self.live_page_segment_ids(request.shard_id);
        let live_page_segment_set = live_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let stale_page_segment_ids = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .filter(|id| !live_page_segment_set.contains(id))
            .collect::<Vec<_>>();
        let recovery = self.storage_recovery_report_without_boundary(request.shard_id);
        let stale_page_segment_set = stale_page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut reclaim_candidates =
            storage_reclaim_candidates_from_recovery(&recovery, &stale_page_segment_set);
        let delayed_destroy_reports = self
            .page_store
            .delayed_destroy_segment_reports()
            .unwrap_or_default();
        reclaim_candidates.extend(delayed_destroy_reports.iter().map(|report| {
            StorageReclaimCandidate {
                page_segment_id: report.page_segment_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: 0,
                stale_physical_bytes: report.physical_bytes,
                reclaim_score: report.physical_bytes.saturating_mul(2),
                reason: "delayed_destroy".to_string(),
                ..StorageReclaimCandidate::default()
            }
        }));
        reclaim_candidates.sort_by(|left, right| {
            right
                .reclaim_score
                .cmp(&left.reclaim_score)
                .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
                .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
        });
        let mut reasons = Vec::new();
        if !selected_dump_slots.is_empty() {
            reasons.push("dirty_slot_dump".to_string());
        } else if dump_delayed && !dirty_slots.is_empty() {
            reasons.push("dirty_slot_dump_delayed".to_string());
        }
        if !stale_page_segment_ids.is_empty() {
            reasons.push("stale_page_segment_gc".to_string());
        }
        if !reclaim_candidates.is_empty() {
            reasons.push("ranked_reclaim_candidates".to_string());
        }
        if request.purge_delayed_destroy && !delayed_destroy_reports.is_empty() {
            reasons.push("delayed_destroy_purge".to_string());
        }
        let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        if !manifest_prune_plan.prunable_manifest_ids.is_empty()
            || !manifest_prune_plan.prunable_marker_manifest_ids.is_empty()
        {
            reasons.push("slot_dump_manifest_prune".to_string());
        }
        if !self
            .interrupted_slot_dump_installs(request.shard_id)
            .is_empty()
        {
            reasons.push("slot_dump_install_roll_forward_check".to_string());
        }
        if request.invalidate_cache {
            reasons.push("cache_invalidation".to_string());
        }
        StorageLifecyclePlan {
            shard_id: request.shard_id,
            dirty_slots,
            selected_dump_slots,
            undumped_oplog_records,
            dump_delayed,
            slot_summaries,
            live_page_segment_ids,
            stale_page_segment_ids,
            reclaim_candidates,
            delayed_destroy_page_segment_ids: delayed_destroy_reports
                .iter()
                .map(|report| report.page_segment_id)
                .collect(),
            reclaimable_physical_bytes: delayed_destroy_reports
                .iter()
                .map(|report| report.physical_bytes)
                .sum(),
            reasons,
        }
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleReport {
        let plan = self.storage_lifecycle_plan(request.clone());
        let dump_manifest = if plan.selected_dump_slots.is_empty() {
            None
        } else {
            self.create_slot_dump_manifest(request.shard_id, plan.selected_dump_slots.clone())
                .ok()
        };
        let (cache_entries_removed, cache_disk_bytes_removed) = if request.invalidate_cache {
            self.cache
                .invalidate_shard(request.shard_id)
                .map(|report| (report.memory_entries_removed, report.disk_bytes_removed))
                .unwrap_or_default()
        } else {
            (0, 0)
        };
        let cache_warmup = if request.warm_cache {
            self.storage_cache_warmup_report(request.shard_id, plan.selected_dump_slots.clone())
        } else {
            StorageCacheWarmupReport {
                shard_id: request.shard_id,
                selected_slots: plan.selected_dump_slots.clone(),
                ..StorageCacheWarmupReport::default()
            }
        };
        let cache_warmup_page_refs = cache_warmup.warmed_page_refs;
        let purge_report = if request.purge_delayed_destroy {
            self.page_store
                .purge_delayed_destroy_segments_with_report()
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let manifest_prune_plan = self.slot_dump_manifest_prune_plan_with_follower_cursors(
            request.shard_id,
            request.follower_replay_cursors.clone(),
        );
        let manifest_prune_report = request.prune_slot_dump_manifests.then(|| {
            self.apply_slot_dump_manifest_prune_with_follower_cursors(
                request.shard_id,
                request.follower_replay_cursors.clone(),
            )
        });
        let install_roll_forward_reports = if request.roll_forward_slot_dump_installs {
            self.roll_forward_slot_dump_installs(request.shard_id)
        } else {
            self.slot_dump_install_roll_forward_reports(request.shard_id)
        };
        let object_lifecycle = self
            .storage_recovery_report_without_boundary(request.shard_id)
            .object_lifecycle;
        StorageLifecycleReport {
            shard_id: request.shard_id,
            plan,
            dump_manifest,
            cache_entries_removed,
            cache_disk_bytes_removed,
            cache_warmup_page_refs,
            cache_warmup,
            delayed_destroy_purged_segments: purge_report.purged_page_segment_ids,
            delayed_destroy_purged_bytes: purge_report.purged_physical_bytes,
            manifest_prune_plan,
            manifest_prune_report,
            install_roll_forward_reports,
            object_lifecycle,
        }
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.storage_production_readiness_report_with_policy(
            shard_id,
            StorageProductionReadinessPolicy::default(),
        )
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        let boundary = self.storage_recovery_boundary_report(shard_id);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let segment_integrity = storage_segment_integrity_report(shard_id, &recovery, &boundary);
        let plan = self.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            follower_replay_cursors: Vec::new(),
            roll_forward_slot_dump_installs: false,
            invalidate_cache: false,
            warm_cache: false,
        });
        let stats = self
            .loaded_shard_stats()
            .into_iter()
            .find(|stats| stats.shard_id == shard_id);
        let cache = stats
            .as_ref()
            .map(|stats| stats.cache.clone())
            .unwrap_or_else(|| self.cache.stats());
        let page_store = stats
            .as_ref()
            .map(|stats| stats.page_store.clone())
            .unwrap_or_else(|| self.page_store.stats());
        let log_compatibility = self.storage_log_compatibility_report(shard_id);
        let page_format_compatibility = self.storage_page_format_compatibility_report(shard_id);
        let slot_dump_manifest_count = self.list_slot_dump_manifests(shard_id).len();
        let interrupted_slot_dump_install_count = boundary.interrupted_slot_dump_installs.len();
        let undumped_oplog_records = boundary
            .latest_safe_oplog_sequence
            .saturating_sub(boundary.latest_dump_oplog_sequence);
        let mut blockers = Vec::new();
        if !boundary.stale_index_page_refs.is_empty() {
            blockers.push("stale_index_page_refs".to_string());
        }
        if !boundary.corrupt_page_segment_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }
        if boundary.unreadable_page_bytes > 0 || !recovery.all_live_pages_readable {
            blockers.push("unreadable_live_page_refs".to_string());
        }
        if !boundary.owner_mismatch_page_refs.is_empty() {
            blockers.push("owner_mismatch_page_refs".to_string());
        }
        if boundary.object_lifecycle.missing_owner_page_refs > 0 {
            blockers.push("missing_owner_page_refs".to_string());
        }
        if boundary.object_lifecycle.reused_object_id_conflicts > 0 {
            blockers.push("reused_object_id_conflicts".to_string());
        }
        if interrupted_slot_dump_install_count > 0 {
            blockers.push("interrupted_slot_dump_installs".to_string());
        }
        if !boundary.manifest_chain_issues.is_empty() {
            blockers.push("broken_slot_dump_manifest_chain".to_string());
        }
        if !segment_integrity.integrity_ok
            && !blockers
                .iter()
                .any(|blocker| blocker == "storage_segment_integrity_failed")
        {
            blockers.push("storage_segment_integrity_failed".to_string());
        }
        if recovery.feature_page_layout.has_errors() {
            blockers.push("feature_page_layout_mismatch".to_string());
        }

        let mut warnings = Vec::new();
        if !plan.dirty_slots.is_empty() {
            warnings.push("dirty_slots_pending_dump".to_string());
        }
        if !plan.stale_page_segment_ids.is_empty() {
            warnings.push("stale_page_segments_pending_gc".to_string());
        }
        if !boundary.orphan_page_segment_ids.is_empty() {
            warnings.push("orphan_page_segments_pending_gc".to_string());
        }
        if slot_dump_manifest_count == 0 && recovery.total_page_refs > 0 {
            warnings.push("no_slot_dump_manifest_for_live_pages".to_string());
        }
        if policy
            .max_dirty_slots
            .map(|limit| plan.dirty_slots.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("dirty_slots_exceed_policy".to_string());
        }
        if policy
            .max_stale_page_segments
            .map(|limit| plan.stale_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("stale_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_orphan_page_segments
            .map(|limit| boundary.orphan_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("orphan_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_undumped_oplog_records
            .map(|limit| undumped_oplog_records > limit)
            .unwrap_or(false)
        {
            blockers.push("undumped_oplog_records_exceed_policy".to_string());
        }
        if policy.require_slot_dump_manifest
            && slot_dump_manifest_count == 0
            && recovery.total_page_refs > 0
        {
            blockers.push("slot_dump_manifest_required".to_string());
        }
        if policy.block_on_warnings && !warnings.is_empty() {
            blockers.push("warnings_exceed_policy".to_string());
        }

        StorageProductionReadinessReport {
            shard_id,
            policy,
            production_ready: blockers.is_empty(),
            blockers,
            warnings,
            dirty_slot_count: plan.dirty_slots.len(),
            stale_page_segment_count: plan.stale_page_segment_ids.len(),
            orphan_page_segment_count: boundary.orphan_page_segment_ids.len(),
            undumped_oplog_records,
            corrupt_page_segment_count: boundary.corrupt_page_segment_ids.len(),
            unreadable_page_ref_count: recovery.unreadable_page_refs.len(),
            owner_mismatch_page_ref_count: boundary.owner_mismatch_page_refs.len(),
            missing_owner_page_ref_count: boundary.object_lifecycle.missing_owner_page_refs,
            reused_object_id_conflict_count: boundary.object_lifecycle.reused_object_id_conflicts,
            interrupted_slot_dump_install_count,
            prepared_slot_dump_install_count: boundary.prepared_slot_dump_install_count,
            installed_slot_dump_install_count: boundary.installed_slot_dump_install_count,
            unknown_slot_dump_install_count: boundary.unknown_slot_dump_install_count,
            slot_dump_manifest_count,
            cache_memory_bytes: cache.memory_bytes,
            cache_disk_bytes: cache.disk_bytes,
            page_store_bytes_written: page_store.bytes_written,
            boundary,
            object_lifecycle: recovery.object_lifecycle,
            segment_integrity,
            log_compatibility,
            page_format_compatibility,
            feature_page_layout_mismatch_count: recovery.feature_page_layout.mismatch_count(),
            corrupt_feature_page_count: recovery
                .feature_page_layout
                .corrupt_packed_feature_pages
                .len(),
            feature_page_layout: recovery.feature_page_layout,
        }
    }

    pub fn storage_log_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StorageLogCompatibilityReport {
        let oplog_stats = self.oplog_store.stats(shard_id);
        let index_log_stats = self.index_log_store.stats(shard_id);
        let oplog_records = self
            .oplog_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        StorageLogCompatibilityReport {
            shard_id,
            oplog_format: "rust-jsonl-command-v1".to_string(),
            index_log_format: "rust-jsonl-shard-index-v1".to_string(),
            rust_native_replay_safe: true,
            cxx_binary_compatible: false,
            oplog_last_sequence: oplog_stats.last_sequence,
            index_log_last_sequence: index_log_stats.last_sequence,
            oplog_records,
            index_log_records,
            oplog_bytes: oplog_stats.bytes_written,
            index_log_bytes: index_log_stats.bytes_written,
            compatibility_gaps: vec![
                "C++ binary/protobuf oplog reader and writer are not implemented".to_string(),
                "C++ binary/protobuf index-log reader and writer are not implemented".to_string(),
                "mixed-format migration and golden-log replay suite are not implemented"
                    .to_string(),
            ],
        }
    }

    pub fn storage_page_format_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StoragePageFormatCompatibilityReport {
        let stats = self.page_store.stats();
        let zones = self.page_store.zone_summary();
        StoragePageFormatCompatibilityReport {
            shard_id,
            page_format: "rust-page-envelope-v6".to_string(),
            rust_envelope_version: 6,
            rust_native_read_safe: true,
            cxx_page_header_compatible: false,
            checksum_protected: true,
            object_ids_embedded: true,
            routing_slots_embedded: true,
            compression_supported: true,
            active_zones: zones.active_zones,
            sealed_zones: zones.sealed_zones,
            delayed_destroy_zones: zones.delayed_destroy_zones,
            live_physical_bytes: zones.live_physical_bytes,
            reclaimable_physical_bytes: zones.reclaimable_physical_bytes,
            page_store_writes: stats.writes,
            page_store_bytes_written: stats.bytes_written,
            logical_bytes_written: stats.logical_bytes_written,
            compressed_records_written: stats.compressed_records_written,
            compatibility_gaps: vec![
                "C++ protobuf page header reader and writer are not implemented".to_string(),
                "C++ slot/page layout and page-id allocation are not byte-compatible".to_string(),
                "mixed Rust-envelope/C++-header migration and golden-page replay suite are not implemented"
                    .to_string(),
            ],
        }
    }

    pub fn warm_cache_from_page_index(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> usize {
        self.storage_cache_warmup_report(shard_id, selected_slots)
            .warmed_page_refs
    }

    pub fn storage_cache_warmup_report(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> StorageCacheWarmupReport {
        let selected_slots = selected_slots.into_iter().collect::<BTreeSet<_>>();
        let mut report = StorageCacheWarmupReport {
            shard_id,
            selected_slots: selected_slots.iter().copied().collect(),
            ..StorageCacheWarmupReport::default()
        };
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return report;
        };
        for entry in collect_live_page_entries(shard) {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| self.routing_slot_for_key(shard_id, &entry.object_key));
            if !selected_slots.is_empty() && !selected_slots.contains(&routing_slot) {
                report.skipped_page_refs = report.skipped_page_refs.saturating_add(1);
                continue;
            }
            report.considered_page_refs = report.considered_page_refs.saturating_add(1);
            let key = CacheKey::page_with_slot(
                shard_id,
                entry.address.page_segment_id,
                entry.address.offset,
                entry.address.length,
                entry.address.routing_slot,
            );
            if self.cache.get(&key).ok().flatten().is_some() {
                report.already_cached_page_refs = report.already_cached_page_refs.saturating_add(1);
                report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
            } else if let Ok(bytes) = self.page_store.read(&entry.address) {
                report.page_store_reads = report.page_store_reads.saturating_add(1);
                let byte_len = bytes.len() as u64;
                match self.cache.put(key, bytes) {
                    Ok(()) => {
                        report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
                        report.warmed_bytes = report.warmed_bytes.saturating_add(byte_len);
                    }
                    Err(_) => {
                        report.failed_page_refs = report.failed_page_refs.saturating_add(1);
                    }
                }
            } else {
                report.failed_page_refs = report.failed_page_refs.saturating_add(1);
            }
        }
        report
    }

    pub fn storage_cache_inspection_report(
        &self,
        shard_id: ShardId,
    ) -> StorageCacheInspectionReport {
        let entries = self.cache.entries_for_shard(shard_id);
        let mut slot_summaries = BTreeMap::<u32, StorageCacheSlotSummary>::new();
        for entry in &entries {
            let Some(routing_slot) = cache_entry_routing_slot(entry) else {
                continue;
            };
            let summary = slot_summaries
                .entry(routing_slot)
                .or_insert(StorageCacheSlotSummary {
                    routing_slot,
                    ..StorageCacheSlotSummary::default()
                });
            summary.entry_count = summary.entry_count.saturating_add(1);
            summary.memory_bytes = summary.memory_bytes.saturating_add(entry.memory_bytes);
            summary.disk_bytes = summary.disk_bytes.saturating_add(entry.disk_bytes);
            if entry.pinned {
                summary.pinned_entries = summary.pinned_entries.saturating_add(1);
                summary.pinned_bytes = summary.pinned_bytes.saturating_add(entry.memory_bytes);
            }
        }
        StorageCacheInspectionReport {
            shard_id,
            stats: self.cache.stats(),
            entries,
            slot_summaries: slot_summaries.into_values().collect(),
        }
    }

    pub fn invalidate_storage_cache_slot(
        &self,
        request: StorageCacheInvalidateSlotRequest,
    ) -> Result<CacheGcReport, Status> {
        self.cache
            .invalidate_slot(request.shard_id, request.routing_slot)
            .map_err(|err| Status::error("cache_slot_invalidation_failed", err.to_string()))
    }

    pub fn storage_recovery_boundary_report(
        &self,
        shard_id: ShardId,
    ) -> StorageRecoveryBoundaryReport {
        let manifests = self.list_slot_dump_manifests(shard_id);
        let latest_dump_oplog_sequence = manifests
            .iter()
            .map(|manifest| manifest.oplog_sequence)
            .max()
            .unwrap_or_default();
        let latest_dump_index_log_sequence = manifests
            .iter()
            .map(|manifest| manifest.index_log_sequence)
            .max()
            .unwrap_or_default();
        let latest_safe_oplog_sequence = self.oplog_store.stats(shard_id).last_sequence;
        let latest_safe_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let live_page_segment_ids = self
            .live_page_segment_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let all_segment_ids = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let orphan_page_segment_ids = all_segment_ids
            .difference(&live_page_segment_ids)
            .copied()
            .collect::<Vec<_>>();
        let latest_dump_slots = manifests
            .last()
            .map(|manifest| manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let missing_dump_slot_ids = self
            .slot_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_slot)
            .filter(|slot| !latest_dump_slots.contains(slot))
            .collect::<Vec<_>>();
        let interrupted_slot_dump_installs = self.interrupted_slot_dump_installs(shard_id);
        let (
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
        ) = slot_dump_install_phase_counts(&interrupted_slot_dump_installs);
        let manifest_chain_issues = slot_dump_manifest_chain_issues(&manifests);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let corrupt_page_segment_ids = recovery
            .page_segment_reports
            .iter()
            .filter(|report| report.has_corruption)
            .map(|report| report.page_segment_id)
            .collect::<Vec<_>>();
        let unreadable_page_bytes = recovery
            .unreadable_page_refs
            .iter()
            .map(|error| error.length)
            .sum();
        let object_lifecycle = recovery.object_lifecycle.clone();
        StorageRecoveryBoundaryReport {
            shard_id,
            latest_safe_oplog_sequence,
            latest_safe_index_log_sequence,
            latest_dump_oplog_sequence,
            latest_dump_index_log_sequence,
            selected_replay_oplog_sequence: latest_dump_oplog_sequence
                .min(latest_safe_oplog_sequence),
            selected_replay_index_log_sequence: latest_dump_index_log_sequence
                .min(latest_safe_index_log_sequence),
            orphan_page_segment_ids,
            missing_dump_slot_ids,
            stale_index_page_refs: recovery.unreadable_page_refs,
            interrupted_slot_dump_installs,
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
            manifest_chain_issues,
            owner_mismatch_page_refs: recovery.owner_mismatch_page_refs,
            missing_owner_page_refs: recovery.missing_owner_page_refs,
            object_lifecycle,
            corrupt_page_segment_ids,
            unreadable_page_bytes,
        }
    }

    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP temporalstore_shard_records Number of records by shard and kind.\n");
        out.push_str("# TYPE temporalstore_shard_records gauge\n");
        out.push_str("# HELP temporalstore_cache_operations_total Cache operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_cache_operations_total counter\n");
        out.push_str("# HELP temporalstore_cache_bytes Cache bytes by shard and tier.\n");
        out.push_str("# TYPE temporalstore_cache_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_operations_total Page store operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_page_store_bytes_total Page store byte counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_bytes_total counter\n");
        out.push_str("# HELP temporalstore_page_store_zone_count Page-store zone counts by shard and lifecycle state.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_count gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_bytes Page-store physical bytes by shard and lifecycle kind.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_unix_ms Oldest page-store zone timestamp by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_unix_ms gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_age_ms Oldest page-store zone age by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_age_ms gauge\n");
        out.push_str("# HELP temporalstore_oplog_records_total Oplog append records by shard.\n");
        out.push_str("# TYPE temporalstore_oplog_records_total counter\n");
        out.push_str("# HELP temporalstore_oplog_bytes_total Oplog appended bytes by shard.\n");
        out.push_str("# TYPE temporalstore_oplog_bytes_total counter\n");
        out.push_str(
            "# HELP temporalstore_object_manager_objects Logical hot objects tracked by shard.\n",
        );
        out.push_str("# TYPE temporalstore_object_manager_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_page_refs Page-address references tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_page_refs gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_objects Dirty logical objects tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_slots Dirty routing slots tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_slots gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_page_refs Live page refs by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_page_refs gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_bytes Live bytes by shard, routing slot, and kind.\n");
        out.push_str("# TYPE temporalstore_storage_slot_bytes gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_dirty_objects Dirty objects by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_dirty_objects gauge\n");
        out.push_str(
            "# HELP temporalstore_partition_routing_slots Routing slots owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_slots gauge\n");
        for stats in self.loaded_shard_stats() {
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "string".into()),
                ],
                stats.string_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "hash".into()),
                ],
                stats.hash_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "set".into()),
                ],
                stats.set_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "feature".into()),
                ],
                stats.feature_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "sequence".into()),
                ],
                stats.sequence_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "ips".into()),
                ],
                stats.ips_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "risk".into()),
                ],
                stats.risk_records as u64,
            );
            for (kind, value) in [
                ("memory_hits", stats.cache.memory_hits),
                ("disk_hits", stats.cache.disk_hits),
                ("misses", stats.cache.misses),
                ("puts", stats.cache.puts),
                ("invalidations", stats.cache.invalidations),
                ("memory_evictions", stats.cache.memory_evictions),
                (
                    "memory_admission_accepted",
                    stats.cache.memory_admission_accepted,
                ),
                (
                    "memory_admission_rejected",
                    stats.cache.memory_admission_rejected,
                ),
                ("memory_fills", stats.cache.memory_fills),
                ("disk_fills", stats.cache.disk_fills),
                ("refill_failures", stats.cache.refill_failures),
                ("eviction_capacity", stats.cache.eviction_capacity),
                ("eviction_oversize", stats.cache.eviction_oversize),
                ("pinned_entries", stats.cache.pinned_entries),
                ("pin_operations", stats.cache.pin_operations),
                ("unpin_operations", stats.cache.unpin_operations),
                ("eviction_pinned_skips", stats.cache.eviction_pinned_skips),
                ("compressed_puts", stats.cache.compressed_puts),
                ("compressed_hits", stats.cache.compressed_hits),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (tier, value) in [
                ("memory", stats.cache.memory_bytes),
                ("disk", stats.cache.disk_bytes),
                ("compression_saved", stats.cache.compression_bytes_saved),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("tier", tier.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("writes", stats.page_store.writes),
                ("reads", stats.page_store.reads),
                (
                    "compressed_writes",
                    stats.page_store.compressed_records_written,
                ),
                ("compressed_reads", stats.page_store.compressed_records_read),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("written", stats.page_store.bytes_written),
                ("read", stats.page_store.bytes_read),
                ("logical_written", stats.page_store.logical_bytes_written),
                ("logical_read", stats.page_store.logical_bytes_read),
                (
                    "compression_saved",
                    stats.page_store.compression_bytes_saved,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_bytes_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (state, value) in [
                ("active", stats.page_store_zones.active_zones),
                ("sealed", stats.page_store_zones.sealed_zones),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_zones,
                ),
                ("purged", stats.page_store_zones.purged_zones),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("active", stats.page_store_zones.active_physical_bytes),
                ("sealed", stats.page_store_zones.sealed_physical_bytes),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_physical_bytes,
                ),
                ("purged", stats.page_store_zones.purged_physical_bytes),
                ("live", stats.page_store_zones.live_physical_bytes),
                (
                    "reclaimable",
                    stats.page_store_zones.reclaimable_physical_bytes,
                ),
                (
                    "total_known",
                    stats.page_store_zones.total_known_physical_bytes,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_zone_unix_ms),
                ("live", stats.page_store_zones.oldest_live_zone_unix_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_zone_unix_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_zone_age_ms),
                ("live", stats.page_store_zones.oldest_live_zone_age_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_zone_age_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_age_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            push_metric(
                &mut out,
                "temporalstore_oplog_records_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.oplog.writes,
            );
            push_metric(
                &mut out,
                "temporalstore_oplog_bytes_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.oplog.bytes_written,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_page_refs",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.page_ref_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_slot_count as u64,
            );
            for summary in self.slot_storage_summaries(stats.shard_id) {
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_page_refs",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.page_ref_count,
                );
                for (kind, value) in [
                    ("logical", summary.logical_bytes),
                    ("physical", summary.physical_bytes),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_storage_slot_bytes",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("slot", summary.routing_slot.to_string()),
                            ("kind", kind.to_string()),
                        ],
                        value,
                    );
                }
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_dirty_objects",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.dirty_object_count,
                );
            }
            push_metric(
                &mut out,
                "temporalstore_partition_routing_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.routing_slot_count as u64,
            );
        }
        out
    }

    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Page => self
                .page_store
                .read_logical_range(request.page_segment_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::Index => fs::read(self.index_path(request.shard_id))
                .map_err(|err| err.to_string())
                .map(|bytes| {
                    let start = request.offset as usize;
                    let end = start.saturating_add(request.size as usize).min(bytes.len());
                    if start >= bytes.len() {
                        Vec::new()
                    } else {
                        bytes[start..end].to_vec()
                    }
                }),
            StreamKind::Oplog => self
                .oplog_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::IndexLog => self
                .index_log_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
        };
        match data {
            Ok(data) => StreamReadResponse {
                status: Status::ok(),
                data,
            },
            Err(err) => StreamReadResponse {
                status: Status::error("stream_read_failed", err.to_string()),
                data: Vec::new(),
            },
        }
    }

    pub fn scan_stream(&self, request: ScanStreamRequest) -> ScanStreamResponse {
        if request.start_offset > request.end_offset {
            return ScanStreamResponse {
                status: Status::error("invalid_stream_range", "start_offset is after end_offset"),
                records: Vec::new(),
                end_of_stream: true,
            };
        }
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(request.max_bytes);
        if request.stream_kind == StreamKind::Oplog || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Oplog => self
                    .oplog_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Page => unreachable!(),
            };
            return match records {
                Ok(records) => ScanStreamResponse {
                    status: Status::ok(),
                    records: records
                        .into_iter()
                        .map(|(offset, data)| StreamRecord { offset, data })
                        .collect(),
                    end_of_stream: true,
                },
                Err(err) => ScanStreamResponse {
                    status: Status::error("stream_scan_failed", err.to_string()),
                    records: Vec::new(),
                    end_of_stream: true,
                },
            };
        }
        let read = self.read_stream(StreamReadRequest {
            shard_id: request.shard_id,
            stream_kind: request.stream_kind,
            page_segment_id: request.page_segment_id,
            offset: request.start_offset,
            size,
        });
        ScanStreamResponse {
            status: read.status.clone(),
            records: if read.status.ok && !read.data.is_empty() {
                vec![StreamRecord {
                    offset: request.start_offset,
                    data: read.data,
                }]
            } else {
                Vec::new()
            },
            end_of_stream: true,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        let responses = request
            .commands
            .into_iter()
            .map(|command| {
                self.execute(ExecuteRequest {
                    shard_id: request.shard_id,
                    command,
                })
            })
            .collect();
        BatchExecuteResponse {
            status: Status::ok(),
            responses,
        }
    }

    pub fn batch_execute_checked(
        &self,
        request: CheckedBatchExecuteRequest,
    ) -> CheckedBatchExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedBatchExecuteResponse {
                status: status.clone(),
                response: BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                },
            };
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: request.shard_id,
            commands: request.commands,
        });
        CheckedBatchExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    pub fn export_index_bytes(&self, shard_id: ShardId) -> Result<Vec<u8>, std::io::Error> {
        fs::read(self.index_path(shard_id))
    }

    pub fn install_index_bytes(
        &self,
        shard_id: ShardId,
        bytes: &[u8],
    ) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        fs::write(self.index_path(shard_id), bytes)
    }

    pub fn storage_recovery_report(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let mut report = self.storage_recovery_report_without_boundary(shard_id);
        report.boundary = self.storage_recovery_boundary_report(shard_id);
        report.segment_integrity =
            storage_segment_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let oplog_records = self
            .oplog_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let active_page_segment_ids = self.page_store.segment_ids().unwrap_or_default();
        let zone_descriptors = self.page_store.zone_descriptors();
        let zone_summary = self.page_store.zone_summary();
        let page_segment_reports = self.page_store.segment_reports().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let total_page_refs = addresses.len();
        let mut readable_page_refs = 0usize;
        let mut unreadable_page_refs = Vec::new();
        let mut owner_mismatch_page_refs = Vec::new();
        let mut missing_owner_page_refs = 0usize;
        let mut object_lifecycle = StorageObjectLifecycleReport::default();
        let mut feature_page_layout = StorageFeaturePageLayoutReport::default();
        let mut page_segment_live_reports = page_segment_reports
            .iter()
            .map(|report| {
                (
                    report.page_segment_id,
                    StorageRecoverySegmentLiveReport {
                        page_segment_id: report.page_segment_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        page_count: report.page_count,
                        ..StorageRecoverySegmentLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_slots = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let segment_report = page_segment_live_reports
                .entry(address.page_segment_id)
                .or_insert(StorageRecoverySegmentLiveReport {
                    page_segment_id: address.page_segment_id,
                    ..StorageRecoverySegmentLiveReport::default()
                });
            segment_report.live_page_refs = segment_report.live_page_refs.saturating_add(1);
            segment_report.live_physical_bytes = segment_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id {
                let objects = live_object_ids.entry(address.page_segment_id).or_default();
                objects.insert(object_id);
                segment_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_slot) = address.routing_slot {
                let slots = live_routing_slots
                    .entry(address.page_segment_id)
                    .or_default();
                slots.insert(routing_slot);
                segment_report.live_routing_slot_count = slots.len() as u64;
            }
            match self.page_store.read(address) {
                Ok(bytes) => {
                    readable_page_refs += 1;
                    segment_report.readable_live_page_refs =
                        segment_report.readable_live_page_refs.saturating_add(1);
                    segment_report.live_logical_bytes = segment_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    segment_report.unreadable_live_page_refs =
                        segment_report.unreadable_live_page_refs.saturating_add(1);
                    unreadable_page_refs.push(StorageRecoveryPageError {
                        page_segment_id: address.page_segment_id,
                        offset: address.offset,
                        length: address.length,
                        error: err.to_string(),
                    });
                }
            }
        }
        if let Some(shard) = shards.get(&shard_id) {
            let ownership = self.validate_shard_page_ownership(shard_id, shard);
            owner_mismatch_page_refs = ownership.mismatches;
            missing_owner_page_refs = ownership.missing_owner_page_refs;
            object_lifecycle = storage_object_lifecycle_report(shard_id, shard);
            object_lifecycle.owner_mismatch_page_refs = owner_mismatch_page_refs.len() as u64;
            object_lifecycle.missing_owner_page_refs = missing_owner_page_refs as u64;
            feature_page_layout = storage_feature_page_layout_report(&self.page_store, shard);
        }
        let page_segment_live_reports = page_segment_live_reports
            .into_values()
            .map(|mut report| {
                report.stale_page_estimate =
                    report.page_count.saturating_sub(report.live_page_refs);
                report.live_ref_density_basis_points = if report.page_count == 0 {
                    0
                } else {
                    report.live_page_refs.saturating_mul(10_000) / report.page_count
                };
                report
            })
            .collect::<Vec<_>>();
        object_lifecycle.stale_object_ids = page_segment_live_reports
            .iter()
            .map(|report| report.stale_page_estimate)
            .sum();
        let mut live_page_segment_ids = addresses
            .iter()
            .map(|address| address.page_segment_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_page_segment_ids.sort_unstable();
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            oplog_records,
            index_log_records,
            active_page_segment_ids,
            live_page_segment_ids,
            zone_descriptors,
            zone_summary,
            page_segment_reports,
            page_segment_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            owner_mismatch_page_refs,
            missing_owner_page_refs,
            object_lifecycle,
            all_live_pages_readable: total_page_refs == readable_page_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            segment_integrity: StorageSegmentIntegrityReport::default(),
            feature_page_layout,
        }
    }

    pub fn live_page_segment_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_page_segment_ids)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn sweep_expired_records(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardExpirySweepReport, Status> {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let now = now_ms();
        let expired_keys = shard
            .expires_at_ms
            .iter()
            .filter_map(|(key, expires_at)| (*expires_at <= now).then(|| key.clone()))
            .collect::<Vec<_>>();
        let mut expired_records_removed = 0;
        for key in expired_keys {
            if delete_record(shard, &key) {
                invalidate_record_all(&self.cache, shard_id, &key);
                expired_records_removed += 1;
            }
        }
        if expired_records_removed > 0 {
            let index_bytes = serde_json::to_vec_pretty(shard)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            self.persist_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            let _ = self.index_log_store.append_json(shard_id, &index_bytes);
        }
        Ok(ShardExpirySweepReport {
            shard_id,
            expired_records_removed,
        })
    }

    pub fn sweep_all_expired_records(&self) -> Vec<ShardExpirySweepReport> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.sweep_expired_records(shard_id).ok())
            .collect()
    }

    fn validate_shard_page_ownership(
        &self,
        shard_id: ShardId,
        shard: &ShardState,
    ) -> StoragePageOwnershipValidation {
        let mut validation = StoragePageOwnershipValidation::default();
        for entry in collect_live_page_entries(shard) {
            let expected_object_id = expected_live_page_object_id(shard_id, &entry);
            let expected_routing_slot = self.routing_slot_for_key(shard_id, &entry.object_key);
            let object_mismatch = entry
                .address
                .object_id
                .is_some_and(|actual| actual != expected_object_id);
            let slot_mismatch = entry
                .address
                .routing_slot
                .is_some_and(|actual| actual != expected_routing_slot);
            if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
                validation.missing_owner_page_refs =
                    validation.missing_owner_page_refs.saturating_add(1);
            }
            if object_mismatch || slot_mismatch {
                validation
                    .mismatches
                    .push(StorageRecoveryPageOwnerMismatch {
                        object_key: entry.object_key,
                        page_segment_id: entry.address.page_segment_id,
                        offset: entry.address.offset,
                        expected_object_id,
                        actual_object_id: entry.address.object_id,
                        expected_routing_slot,
                        actual_routing_slot: entry.address.routing_slot,
                    });
            }
        }
        validation
    }

    pub fn compact_shard_pages(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let ownership = self.validate_shard_page_ownership(shard_id, shard);
        if !ownership.mismatches.is_empty() {
            return Err(Status::error(
                "page_compaction_owner_mismatch",
                format!(
                    "refusing compaction because {} live page refs disagree with object/page/slot ownership",
                    ownership.mismatches.len()
                ),
            ));
        }
        let before_segments = collect_live_page_segment_ids(shard);
        let before = compaction_utility_report(&self.page_store, shard);
        let roll = self
            .page_store
            .roll_segment()
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let mut rewritten_page_refs = 0;

        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.strings.values_mut(),
            &mut rewritten_page_refs,
        )?;
        for fields in shard.hashes.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                fields.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for members in shard.sets.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                members.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.features.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.sequences.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.ips.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for (key, series) in &mut shard.ips_meta {
            for (timestamp, meta) in series {
                let bytes = read_page_bytes(&self.cache, &self.page_store, shard_id, &meta.address)
                    .ok_or_else(|| {
                        Status::error(
                            "page_compaction_failed",
                            format!("missing IPS page for {key}@{timestamp}"),
                        )
                    })?;
                let new_address = self
                    .page_store
                    .append_with_page_metadata(
                        &bytes,
                        meta.address.object_id,
                        meta.address.routing_slot,
                    )
                    .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
                meta.address = new_address.clone();
                let _ = self.cache.put(
                    CacheKey::page_with_slot(
                        shard_id,
                        new_address.page_segment_id,
                        new_address.offset,
                        new_address.length,
                        new_address.routing_slot,
                    ),
                    bytes,
                );
                rewritten_page_refs += 1;
            }
        }

        let after_segments = collect_live_page_segment_ids(shard);
        let after = compaction_utility_report(&self.page_store, shard);
        let stale_page_segment_ids = before_segments
            .difference(&after_segments)
            .copied()
            .collect::<Vec<_>>();
        let index_bytes = serde_json::to_vec_pretty(shard)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        self.persist_index_bytes(shard_id, &index_bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = self.index_log_store.append_json(shard_id, &index_bytes);
        Ok(ShardCompactionReport {
            shard_id,
            previous_page_segment_id: roll.previous_page_segment_id,
            compacted_page_segment_id: roll.new_page_segment_id,
            rewritten_page_refs,
            stale_page_segment_ids,
            before,
            after,
        })
    }

    fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    fn persist_slot_dump_manifest(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), std::io::Error> {
        let path =
            slot_dump_manifest_path(&self.index_dir, manifest.shard_id, &manifest.manifest_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        fs::write(path, bytes)
    }

    fn persist_slot_dump_install_marker(
        &self,
        manifest: &SlotDumpManifest,
        phase: &str,
    ) -> Result<(), std::io::Error> {
        self.persist_slot_dump_install_marker_by_fields(
            manifest.shard_id,
            &manifest.manifest_id,
            phase,
            manifest.oplog_sequence,
            manifest.index_log_sequence,
        )
    }

    fn persist_slot_dump_install_marker_by_fields(
        &self,
        shard_id: ShardId,
        manifest_id: &str,
        phase: &str,
        oplog_sequence: u64,
        index_log_sequence: u64,
    ) -> Result<(), std::io::Error> {
        write_slot_dump_install_marker(
            &self.index_dir,
            &SlotDumpInstallMarker {
                shard_id,
                manifest_id: manifest_id.to_string(),
                phase: phase.to_string(),
                oplog_sequence,
                index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
    }

    fn validate_slot_dump_generation_for_install(
        &self,
        manifest: &SlotDumpManifest,
    ) -> Result<(), Status> {
        if manifest.dump_generation_id.is_empty() {
            return Ok(());
        }
        let requested_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        for existing in self.list_slot_dump_manifests(manifest.shard_id) {
            if existing.manifest_id == manifest.manifest_id
                || existing.dump_generation_id.is_empty()
                || existing.dump_generation_id == manifest.dump_generation_id
            {
                continue;
            }
            let existing_slots = existing.slot_ids.iter().copied().collect::<BTreeSet<_>>();
            let overlaps = requested_slots.is_empty()
                || existing_slots.is_empty()
                || !requested_slots.is_disjoint(&existing_slots);
            if overlaps
                && existing.index_log_sequence >= manifest.index_log_sequence
                && existing.oplog_sequence >= manifest.oplog_sequence
            {
                return Err(Status::error(
                    "slot_dump_generation_conflict",
                    format!(
                        "manifest generation {} conflicts with installed generation {} for overlapping slots",
                        manifest.dump_generation_id, existing.dump_generation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    fn load_index(&self, shard_id: ShardId) -> Option<ShardState> {
        let bytes = fs::read(self.index_path(shard_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn persist_index_bytes(&self, shard_id: ShardId, bytes: &[u8]) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        atomic_write_bytes(&self.index_path(shard_id), bytes)
    }

    fn validate_load_version(&self, shard_id: ShardId, load_version: u64) -> Result<(), Status> {
        let infos = self.infos.read().expect("info lock poisoned");
        let Some(info) = infos.get(&shard_id) else {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        };
        if !info.loaded {
            return Err(Status::error(
                "shard_not_loaded",
                "shard is not loaded on this server",
            ));
        }
        if info.load_version != load_version {
            return Err(Status::error(
                "load_version_mismatch",
                format!(
                    "request load_version {} does not match loaded version {}",
                    load_version, info.load_version
                ),
            ));
        }
        Ok(())
    }

    fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        shards.get(&shard_id).map(|state| {
            let page_store = self.page_store.stats();
            let page_store_zones = self.page_store.zone_summary();
            let string_records = state.strings.len();
            let hash_records = state.hashes.len();
            let set_records = state.sets.len();
            let feature_records = state.features.len();
            let sequence_records = state.sequences.len();
            let ips_records = state.ips.len();
            let risk_records = state.risk.len() + state.risk_changes.len();
            let total_records = string_records
                + hash_records
                + set_records
                + feature_records
                + sequence_records
                + ips_records
                + risk_records;
            let loaded = info.as_ref().map(|info| info.loaded).unwrap_or(true);
            let readonly = info.as_ref().map(|info| info.readonly).unwrap_or(false);
            let load_version = info
                .as_ref()
                .map(|info| info.load_version)
                .unwrap_or_default();
            let table_name = info
                .as_ref()
                .map(|info| info.table_name.clone())
                .unwrap_or_default();
            let shard_uri = info
                .as_ref()
                .map(|info| info.shard_uri.clone())
                .unwrap_or_default();
            let start_routing_slot = info
                .as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default();
            let end_routing_slot = info
                .as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX);
            let object_manager = object_manager_stats(state, start_routing_slot, end_routing_slot);
            let partition_info = PartitionInfoStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                table_name,
                shard_uri,
                start_routing_slot,
                end_routing_slot,
                total_records,
                storage_bytes: page_store.bytes_written,
                object_manager: object_manager.clone(),
            };
            ShardStats {
                shard_id,
                loaded,
                readonly,
                load_version,
                total_records,
                string_records,
                hash_records,
                set_records,
                feature_records,
                sequence_records,
                ips_records,
                risk_records,
                storage_bytes: page_store.bytes_written,
                object_manager,
                partition_info,
                cache: self.cache.stats(),
                page_store,
                page_store_zones,
                oplog: self.oplog_store.stats(shard_id),
            }
        })
    }
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec_pretty(shard).unwrap_or_default()
}

fn push_metric(out: &mut String, name: &str, labels: &[(&str, String)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&escape_metric_label(value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        next_temp_counter()
    ));
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn next_temp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn unique_temp_path(kind: &str) -> PathBuf {
    let counter = next_temp_counter();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

fn slot_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

fn slot_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

fn slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<SlotDumpManifest>, std::io::Error> {
    let path = slot_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<SlotDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn slot_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

fn write_slot_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = slot_dump_install_marker_path(index_dir, marker);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, bytes)
}

fn slot_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(SlotDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut markers = Vec::new();
    if !dir.exists() {
        return Ok(markers);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "marker")
            .unwrap_or(false)
        {
            continue;
        }
        let path = entry.path();
        let marker = serde_json::from_slice::<SlotDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            slot_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

fn list_slot_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    Ok(slot_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

fn interrupted_slot_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, SlotDumpInstallMarker>::new();
    for marker in list_slot_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                slot_dump_install_phase_rank(&marker.phase)
                    > slot_dump_install_phase_rank(&existing.phase)
                    || (slot_dump_install_phase_rank(&marker.phase)
                        == slot_dump_install_phase_rank(&existing.phase)
                        && marker.created_unix_ms > existing.created_unix_ms)
            })
            .unwrap_or(true);
        if replace {
            latest_by_manifest.insert(marker.manifest_id.clone(), marker);
        }
    }
    Ok(latest_by_manifest
        .into_values()
        .filter(|marker| marker.phase != "commit")
        .collect())
}

fn remove_obsolete_slot_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in slot_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn slot_dump_install_phase_counts(markers: &[SlotDumpInstallMarker]) -> (usize, usize, usize) {
    let mut prepared = 0usize;
    let mut installed = 0usize;
    let mut unknown = 0usize;
    for marker in markers {
        match marker.phase.as_str() {
            "prepare" => prepared = prepared.saturating_add(1),
            "install" => installed = installed.saturating_add(1),
            _ => unknown = unknown.saturating_add(1),
        }
    }
    (prepared, installed, unknown)
}

fn slot_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

fn slot_dump_manifest_chain_issues(
    manifests: &[SlotDumpManifest],
) -> Vec<SlotDumpManifestChainIssue> {
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| SlotDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

fn retained_slot_dump_manifest_ids(manifests: &[SlotDumpManifest]) -> BTreeSet<String> {
    let by_id = manifests
        .iter()
        .map(|manifest| (manifest.manifest_id.clone(), manifest))
        .collect::<BTreeMap<_, _>>();
    let mut retained = BTreeSet::new();
    let mut cursor = manifests
        .iter()
        .max_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms))
        .map(|manifest| manifest.manifest_id.clone());
    while let Some(manifest_id) = cursor {
        if !retained.insert(manifest_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&manifest_id)
            .and_then(|manifest| manifest.parent_manifest_id.clone());
    }
    retained
}

fn slot_dump_manifest_prune_plan_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    follower_cursors: &[SlotDumpFollowerReplayCursor],
) -> Result<SlotDumpManifestPrunePlan, std::io::Error> {
    let manifests = list_slot_dump_manifests_at(index_dir, shard_id)?;
    let mut retained = retained_slot_dump_manifest_ids(&manifests);
    let mut follower_blocks = Vec::new();
    for cursor in follower_cursors
        .iter()
        .filter(|cursor| cursor.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= cursor.oplog_sequence
                && manifest.index_log_sequence <= cursor.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            follower_blocks.push(SlotDumpFollowerRetentionBlock {
                follower_id: cursor.follower_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                cursor_oplog_sequence: cursor.oplog_sequence,
                cursor_index_log_sequence: cursor.index_log_sequence,
                reason: "follower_cursor_anchor".to_string(),
            });
        }
    }
    let interrupted = interrupted_slot_dump_installs_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .collect::<BTreeSet<_>>();
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    let mut prunable_manifest_ids = Vec::new();
    let mut blocked_manifest_ids = Vec::new();
    for manifest in &manifests {
        if retained.contains(&manifest.manifest_id) {
            continue;
        }
        if interrupted.contains(&manifest.manifest_id) {
            blocked_manifest_ids.push(manifest.manifest_id.clone());
        } else {
            prunable_manifest_ids.push(manifest.manifest_id.clone());
        }
    }
    let prunable_marker_manifest_ids = list_slot_dump_install_markers_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .filter(|manifest_id| {
            !retained.contains(manifest_id)
                && !interrupted.contains(manifest_id)
                && (prunable_manifest_ids.iter().any(|id| id == manifest_id)
                    || !manifest_ids.contains(manifest_id))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut reasons = Vec::new();
    if !prunable_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_manifest".to_string());
    }
    if !prunable_marker_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_marker".to_string());
    }
    if !blocked_manifest_ids.is_empty() {
        reasons.push("interrupted_install_blocks_prune".to_string());
    }
    if !follower_blocks.is_empty() {
        reasons.push("follower_cursor_blocks_prune".to_string());
    }
    Ok(SlotDumpManifestPrunePlan {
        shard_id,
        retained_manifest_ids: retained.into_iter().collect(),
        prunable_manifest_ids,
        prunable_marker_manifest_ids,
        blocked_manifest_ids,
        follower_blocks,
        reasons,
    })
}

fn list_slot_dump_manifests_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpManifest>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut manifests = Vec::new();
    if !dir.exists() {
        return Ok(manifests);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "json")
            .unwrap_or(false)
        {
            continue;
        }
        let manifest = serde_json::from_slice::<SlotDumpManifest>(&fs::read(entry.path())?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms));
    Ok(manifests)
}

fn latest_slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<SlotDumpManifest> {
    list_slot_dump_manifests_at(index_dir, shard_id)
        .ok()?
        .into_iter()
        .last()
}

fn slot_dump_manifest_checksum(manifest: &SlotDumpManifest) -> Result<String, Status> {
    let mut payload = manifest.clone();
    payload.checksum.clear();
    serde_json::to_vec(&payload)
        .map(|bytes| sha256_hex_bytes(&bytes))
        .map_err(|err| Status::error("slot_dump_checksum_failed", err.to_string()))
}

fn slot_dump_generation_id(manifest: &SlotDumpManifest) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest.shard_id.to_le_bytes());
    digest.update(manifest.oplog_sequence.to_le_bytes());
    digest.update(manifest.index_log_sequence.to_le_bytes());
    for slot_id in &manifest.slot_ids {
        digest.update(slot_id.to_le_bytes());
    }
    for page_segment_id in &manifest.page_segment_ids {
        digest.update(page_segment_id.to_le_bytes());
    }
    digest.update(manifest.index_sha256.as_bytes());
    if manifest.version >= 3 {
        digest.update(manifest.object_lifecycle.live_object_ids.to_le_bytes());
        digest.update(manifest.object_lifecycle.live_page_refs.to_le_bytes());
        digest.update(manifest.object_lifecycle.stale_object_ids.to_le_bytes());
        digest.update(
            manifest
                .object_lifecycle
                .tombstoned_object_ids
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .reused_object_id_conflicts
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .missing_owner_page_refs
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .owner_mismatch_page_refs
                .to_le_bytes(),
        );
        for object_id in &manifest.object_lifecycle.reused_object_ids {
            digest.update(object_id.to_le_bytes());
        }
        for key in &manifest.object_lifecycle.tombstoned_object_keys {
            digest.update(key.as_bytes());
            digest.update([0]);
        }
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn execute_on_shard(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    feature_max_size: usize,
    async_storage: bool,
    shard_id: ShardId,
    start_routing_slot: u32,
    end_routing_slot: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    let mut mutated = false;
    let response = match command {
        Command::CommonDelete { key } => {
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            let expires_at = now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    shard.expires_at_ms.insert(record_key, expires_at);
                }
            }
            mutated = true;
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonTtl { key } => {
            let expired = shard
                .expires_at_ms
                .get(&key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false);
            let value = ttl_ms(shard, &key);
            mutated = expired;
            CommandResponse::Integer { value }
        }
        Command::CommonExists { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_record_all(cache, shard_id, &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: if record_exists(shard, &key) { 1 } else { 0 },
            }
        }
        Command::StringSet { key, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.strings.insert(key.clone(), address);
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.strings.insert(key.clone(), address);
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetConditional {
            key,
            value,
            ttl_ms,
            condition,
            return_old,
        } => {
            remove_if_expired(shard, &key);
            let old_value = shard
                .strings
                .get(&key)
                .and_then(|address| read_page_bytes(cache, page_store, shard_id, address));
            let exists = old_value.is_some();
            let should_set = match condition {
                StringSetCondition::Always => true,
                StringSetCondition::IfExists => exists,
                StringSetCondition::IfNotExists => !exists,
            };
            if should_set {
                let object_id = stable_page_object_id(shard_id, "string", &key, None);
                let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    shard.strings.insert(key.clone(), address);
                    if let Some(ttl_ms) = ttl_ms {
                        shard
                            .expires_at_ms
                            .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                    } else {
                        shard.expires_at_ms.remove(&key);
                    }
                    mutated = true;
                }
                invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            }
            if return_old {
                CommandResponse::Bytes { value: old_value }
            } else {
                CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                }
            }
        }
        Command::StringGet { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::string(shard_id, &key), || {
                CommandResponse::Bytes {
                    value: shard
                        .strings
                        .get(&key)
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address)),
                }
            })
        }
        Command::StringDelete { key } => {
            mutated = shard.strings.remove(&key).is_some();
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::HashGet { key, field } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::hash(shard_id, &key, &field), || {
                CommandResponse::Bytes {
                    value: shard
                        .hashes
                        .get(&key)
                        .and_then(|fields| fields.get(&field))
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address)),
                }
            })
        }
        Command::HashMultiGet { key, fields } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Values {
                        values: vec![None; fields.len()],
                    },
                    mutated,
                };
            }
            let values = fields
                .iter()
                .map(|field| {
                    shard
                        .hashes
                        .get(&key)
                        .and_then(|entries| entries.get(field))
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
                })
                .collect();
            CommandResponse::Values { values }
        }
        Command::HashMultiSet { key, entries } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            for (field, value) in entries {
                let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    shard
                        .hashes
                        .entry(key.clone())
                        .or_default()
                        .insert(field.clone(), address);
                    let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                    mutated = true;
                }
            }
            CommandResponse::Empty
        }
        Command::HashIncrBy {
            key,
            field,
            increment,
        } => {
            remove_if_expired(shard, &key);
            let current = shard
                .hashes
                .get(&key)
                .and_then(|entries| entries.get(&field))
                .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
                .and_then(|bytes| parse_i64(&bytes))
                .unwrap_or_default();
            let value = current.saturating_add(increment);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                value.to_string().as_bytes(),
                Some(stable_page_object_id(shard_id, "hash", &key, Some(&field))),
                Some(page_routing_slot(
                    &key,
                    start_routing_slot,
                    end_routing_slot,
                )),
                async_storage,
            ) {
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                mutated = true;
            }
            CommandResponse::Integer { value }
        }
        Command::HashGetAll { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let entries = shard
                .hashes
                .get(&key)
                .map(|fields| {
                    let mut entries = fields
                        .iter()
                        .filter_map(|(field, address)| {
                            read_page_bytes(cache, page_store, shard_id, address)
                                .map(|value| (field.clone(), value))
                        })
                        .collect::<Vec<_>>();
                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries
                })
                .unwrap_or_default();
            CommandResponse::HashEntries { entries }
        }
        Command::HashLen { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard
                    .hashes
                    .get(&key)
                    .map(|fields| fields.len() as i64)
                    .unwrap_or_default(),
            }
        }
        Command::HashDelete { key, field } => {
            if let Some(fields) = shard.hashes.get_mut(&key) {
                mutated = fields.remove(&field).is_some();
            }
            let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::SetAdd { key, member } => {
            remove_if_expired(shard, &key);
            let member_component = hex::encode(&member);
            let object_id = stable_page_object_id(shard_id, "set", &key, Some(&member_component));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .sets
                    .entry(key.clone())
                    .or_default()
                    .insert(member.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::SetMembers { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "set", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::set_members(shard_id, &key), || {
                let members = shard
                    .sets
                    .get(&key)
                    .map(|set| {
                        set.values()
                            .filter_map(|address| {
                                read_page_bytes(cache, page_store, shard_id, address)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::Members { members }
            })
        }
        Command::SetRemove { key, member } => {
            if let Some(set) = shard.sets.get_mut(&key) {
                mutated = set.remove(&member).is_some();
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppend { key, points } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let mut accepted_points = Vec::new();
            let mut accepted_timestamps = BTreeSet::new();
            for point in sorted_feature_points(points) {
                let exists = series.contains_key(&point.timestamp_ms)
                    || accepted_timestamps.contains(&point.timestamp_ms);
                let should_write = match policy {
                    FeatureWritePolicy::Upsert => true,
                    FeatureWritePolicy::InsertIfAbsent => !exists,
                    FeatureWritePolicy::ReplaceExisting => exists,
                };
                if should_write {
                    accepted_timestamps.insert(point.timestamp_ms);
                    accepted_points.push(point);
                }
            }
            if !accepted_points.is_empty() {
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "feature",
                    &key,
                    accepted_points,
                    routing_slot,
                    async_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => cached_response(
            cache,
            CacheKey::feature_query(shard_id, &key, start_ms, end_ms, count),
            || {
                let points = shard
                    .features
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .take(count.unwrap_or(5000))
                            .filter_map(|(timestamp_ms, address)| {
                                read_feature_point(
                                    cache,
                                    page_store,
                                    shard_id,
                                    *timestamp_ms,
                                    address,
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::FeaturePoints { points }
            },
        ),
        Command::FeatureQueryFiltered {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            let limit = count.unwrap_or(feature_max_size).min(feature_max_size);
            let points = shard
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(limit)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                                .and_then(|point| {
                                    let row = SequenceFeatureRow::decode_cpp_feature_value(
                                        point.timestamp_ms,
                                        &point.value,
                                    )?;
                                    filters
                                        .iter()
                                        .all(|filter| sequence_filter_matches(&row, filter))
                                        .then_some(point)
                                })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::FeatureReplace {
            key,
            start_ms,
            end_ms,
            points,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let replaced = series
                .range(start_ms..=end_ms)
                .map(|(timestamp_ms, _)| *timestamp_ms)
                .collect::<Vec<_>>();
            for timestamp_ms in replaced {
                series.remove(&timestamp_ms);
                mutated = true;
            }
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureDelete { key } => {
            mutated = shard.features.remove(&key).is_some();
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAggQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Aggregate { value: 0 },
                    mutated,
                };
            }
            let values = shard
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(5000))
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                                .map(|point| point.value)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            CommandResponse::Aggregate {
                value: aggregate_feature_values(&values, &aggregator),
            }
        }
        Command::SequenceAdd { key, rows } => {
            remove_if_expired(shard, &key);
            let series = shard.sequences.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = rows
                .into_iter()
                .filter_map(|row| {
                    serde_json::to_vec(&row).ok().map(|value| FeaturePoint {
                        timestamp_ms: row.timestamp_ms,
                        value,
                    })
                })
                .collect::<Vec<_>>();
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "sequence",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            CommandResponse::Empty
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::SequenceRows { rows: Vec::new() },
                    mutated,
                };
            }
            let rows = shard
                .sequences
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_sequence_row(cache, page_store, shard_id, *timestamp_ms, address)
                        })
                        .filter(|row| {
                            filters
                                .iter()
                                .all(|filter| sequence_filter_matches(row, filter))
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::SequenceRows { rows }
        }
        Command::SequenceBatchQuery { queries } => {
            let groups = queries
                .into_iter()
                .map(
                    |SequenceQuerySpec {
                         key,
                         start_ms,
                         end_ms,
                         count,
                         filters,
                     }| {
                        if remove_if_expired(shard, &key) {
                            mutated = true;
                            return (key, Vec::new());
                        }
                        let rows = sequence_rows_in_range(
                            cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                            &filters,
                        );
                        (key, rows)
                    },
                )
                .collect();
            CommandResponse::SequenceRowGroups { groups }
        }
        Command::IpsAdd {
            key,
            timestamp_ms,
            instance,
        } => {
            remove_if_expired(shard, &key);
            let timestamp = timestamp_ms.to_string();
            let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &instance,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type: None,
                        table_id: None,
                        request_id: None,
                    },
                );
                mutated = true;
            }
            CommandResponse::Empty
        }
        Command::IpsAddWithOptions {
            key,
            timestamp_ms,
            instance,
            action_type,
            table_id,
            request_id,
        } => {
            remove_if_expired(shard, &key);
            if let Some(request_id) = &request_id {
                if shard
                    .ips_request_ids
                    .get(&key)
                    .is_some_and(|ids| ids.contains(request_id))
                {
                    return ExecuteOutcome {
                        response: CommandResponse::Integer { value: 0 },
                        mutated: false,
                    };
                }
            }
            let timestamp = timestamp_ms.to_string();
            let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &instance,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key.clone()).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type,
                        table_id,
                        request_id: request_id.clone(),
                    },
                );
                if let Some(request_id) = request_id {
                    shard
                        .ips_request_ids
                        .entry(key)
                        .or_default()
                        .insert(request_id);
                }
                mutated = true;
            }
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsLoad { key, points } => {
            remove_if_expired(shard, &key);
            let mut loaded = 0i64;
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            for point in points {
                let timestamp = point.timestamp_ms.to_string();
                let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &point.value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    shard
                        .ips
                        .entry(key.clone())
                        .or_default()
                        .insert(point.timestamp_ms, address.clone());
                    shard.ips_meta.entry(key.clone()).or_default().insert(
                        point.timestamp_ms,
                        IpsPointMeta {
                            address,
                            action_type: None,
                            table_id: None,
                            request_id: None,
                        },
                    );
                    loaded += 1;
                    mutated = true;
                }
            }
            CommandResponse::Integer { value: loaded }
        }
        Command::IpsQueryLast { key, count } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .ips
                .get(&key)
                .map(|series| {
                    series
                        .iter()
                        .rev()
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_page_bytes(cache, page_store, shard_id, address).map(|value| {
                                FeaturePoint {
                                    timestamp_ms: *timestamp_ms,
                                    value,
                                }
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::IpsQueryRange {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsBatchQueryLast { keys, count } => {
            let groups = keys
                .into_iter()
                .map(|key| {
                    if remove_if_expired(shard, &key) {
                        mutated = true;
                        return (key, Vec::new());
                    }
                    let points = shard
                        .ips
                        .get(&key)
                        .map(|series| {
                            series
                                .iter()
                                .rev()
                                .take(count)
                                .filter_map(|(timestamp_ms, address)| {
                                    read_page_bytes(cache, page_store, shard_id, address).map(
                                        |value| FeaturePoint {
                                            timestamp_ms: *timestamp_ms,
                                            value,
                                        },
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    (key, points)
                })
                .collect();
            CommandResponse::FeaturePointGroups { groups }
        }
        Command::IpsRemove { key, timestamp_ms } => {
            if let Some(series) = shard.ips.get_mut(&key) {
                mutated = series.remove(&timestamp_ms).is_some();
                if series.is_empty() {
                    shard.ips.remove(&key);
                }
            }
            if let Some(series) = shard.ips_meta.get_mut(&key) {
                if let Some(meta) = series.remove(&timestamp_ms) {
                    if let Some(request_id) = meta.request_id {
                        if let Some(ids) = shard.ips_request_ids.get_mut(&key) {
                            ids.remove(&request_id);
                        }
                    }
                }
                if series.is_empty() {
                    shard.ips_meta.remove(&key);
                }
            }
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsDelete { key } => {
            mutated = shard.ips.remove(&key).is_some();
            mutated |= shard.ips_meta.remove(&key).is_some();
            shard.ips_request_ids.remove(&key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .ips
                .get(&key)
                .map(|series| series.range(start_ms..=end_ms).count() as i64)
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::IpsQueryRangeWithOptions {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::IpsSnapshot {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsStat {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::IpsStats {
                        stats: IpsStats {
                            total: 0,
                            first_timestamp_ms: None,
                            last_timestamp_ms: None,
                            action_type_counts: Vec::new(),
                            table_id_counts: Vec::new(),
                        },
                    },
                    mutated,
                };
            }
            CommandResponse::IpsStats {
                stats: ips_stats_in_range(shard, &key, start_ms, end_ms),
            }
        }
        Command::IpsFilter {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::RiskIncrement {
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            *shard
                .risk
                .entry(key)
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskIncrementWithOptions {
            key,
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default() += amount;
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskChangeAdd {
            key,
            timestamp_ms,
            value,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            shard
                .risk_changes
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default()
                .insert(value);
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .map(|(_, value)| *value)
                        .sum()
                })
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::RiskQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskDetail {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(usize::MAX))
                        .map(|(timestamp_ms, amount)| FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value: amount.to_string().into_bytes(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::RiskSet {
            family,
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            *shard
                .risk
                .entry(key)
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskSetAndGet {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            let series = shard.risk.entry(key).or_default();
            *series.entry(timestamp_ms).or_default() += amount;
            let values = series
                .range(start_ms..=end_ms)
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            mutated = true;
            CommandResponse::Integer {
                value: aggregate_risk_values(&values, &aggregator),
            }
        }
        Command::RiskFamilyQuery {
            family,
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let key = risk_family_key(family, &key);
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskFolSet {
            key,
            value,
            occur_time_ms,
            ttl_ms,
            fol_type,
        } => {
            remove_if_expired(shard, &key);
            let should_store = shard
                .risk_fol
                .get(&key)
                .map(|existing| match fol_type {
                    RiskFolType::First => occur_time_ms < existing.occur_time_ms,
                    RiskFolType::Last => occur_time_ms > existing.occur_time_ms,
                })
                .unwrap_or(true);
            if should_store {
                shard.risk_fol.insert(
                    key.clone(),
                    RiskFolValue {
                        occur_time_ms,
                        value,
                        fol_type,
                    },
                );
            }
            if ttl_ms > 0 {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskFolQuery { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard.risk_fol.get(&key).map(|stored| stored.value.clone()),
            }
        }
        Command::RiskManager { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
                let family_key = risk_family_key(family, &key);
                let values = shard
                    .risk
                    .get(&family_key)
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                entries.push((
                    format!("{}_events", risk_family_name(family)),
                    values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{}_sum", risk_family_name(family)),
                    values.iter().sum::<i64>().to_string().into_bytes(),
                ));
            }
            if let Some(fol) = shard.risk_fol.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.fol_type {
                        RiskFolType::First => b"first".to_vec(),
                        RiskFolType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
    };
    ExecuteOutcome { response, mutated }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn ttl_ms(shard: &mut ShardState, key: &str) -> i64 {
    if remove_if_expired(shard, key) {
        return -2;
    }
    if !record_exists(shard, key) {
        return -2;
    }
    associated_record_keys(key)
        .into_iter()
        .filter_map(|record_key| shard.expires_at_ms.get(&record_key).copied())
        .map(|expires_at| expires_at.saturating_sub(now_ms()) as i64)
        .min()
        .unwrap_or(-1)
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    let now = now_ms();
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        if shard
            .expires_at_ms
            .get(&record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false)
        {
            removed |= delete_record_exact(shard, &record_key);
        }
    }
    removed
}

fn delete_record(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        removed |= delete_record_exact(shard, &record_key);
    }
    removed
}

fn delete_record_exact(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.features.remove(key).is_some();
    removed |= shard.sequences.remove(key).is_some();
    removed |= shard.ips.remove(key).is_some();
    removed |= shard.ips_meta.remove(key).is_some();
    removed |= shard.ips_request_ids.remove(key).is_some();
    removed |= shard.risk.remove(key).is_some();
    removed |= shard.risk_changes.remove(key).is_some();
    removed |= shard.risk_fol.remove(key).is_some();
    removed
}

fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("risk:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
        keys.push(risk_family_key(family, key));
    }
    keys
}

fn collect_live_page_segment_ids(shard: &ShardState) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    ids.extend(
        shard
            .strings
            .values()
            .map(|address| address.page_segment_id),
    );
    for fields in shard.hashes.values() {
        ids.extend(fields.values().map(|address| address.page_segment_id));
    }
    for members in shard.sets.values() {
        ids.extend(members.values().map(|address| address.page_segment_id));
    }
    for series in shard.features.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.sequences.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.ips.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.ips_meta.values() {
        ids.extend(series.values().map(|meta| meta.address.page_segment_id));
    }
    ids
}

fn storage_segment_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSegmentIntegrityReport {
    let indexed_page_segment_count = recovery.active_page_segment_ids.len();
    let discovered_page_segment_count = recovery.page_segment_reports.len();
    let live_page_segment_count = recovery.live_page_segment_ids.len();
    let orphan_page_segment_count = boundary.orphan_page_segment_ids.len();
    let stale_page_ref_count = boundary.stale_index_page_refs.len();
    let corrupt_page_segment_count = boundary.corrupt_page_segment_ids.len();
    let unreadable_page_ref_count = recovery.unreadable_page_refs.len();
    let unreadable_page_bytes = boundary.unreadable_page_bytes;
    let owner_mismatch_page_ref_count = boundary.owner_mismatch_page_refs.len();
    let missing_owner_page_ref_count = boundary.missing_owner_page_refs;
    let reclaim_required = orphan_page_segment_count > 0
        || recovery
            .page_segment_live_reports
            .iter()
            .any(|report| report.stale_page_estimate > 0);
    let integrity_ok = stale_page_ref_count == 0
        && corrupt_page_segment_count == 0
        && unreadable_page_ref_count == 0
        && unreadable_page_bytes == 0
        && owner_mismatch_page_ref_count == 0
        && missing_owner_page_ref_count == 0
        && recovery.all_live_pages_readable;

    StorageSegmentIntegrityReport {
        shard_id,
        indexed_page_segment_count,
        discovered_page_segment_count,
        live_page_segment_count,
        orphan_page_segment_count,
        stale_page_ref_count,
        corrupt_page_segment_count,
        unreadable_page_ref_count,
        unreadable_page_bytes,
        owner_mismatch_page_ref_count,
        missing_owner_page_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

fn storage_reclaim_candidates_from_recovery(
    recovery: &StorageRecoveryReport,
    fully_stale_segment_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = recovery
        .page_segment_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_segment_ids.contains(&report.page_segment_id);
            let stale_page_estimate = if fully_stale {
                report.page_count
            } else {
                report.stale_page_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_page_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_page_estimate);
            Some(StorageReclaimCandidate {
                page_segment_id: report.page_segment_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                page_count: report.page_count,
                live_page_refs: report.live_page_refs,
                stale_page_estimate,
                live_ref_density_basis_points: report.live_ref_density_basis_points,
                reclaim_score,
                reason: if fully_stale {
                    "orphan_segment".to_string()
                } else {
                    "low_live_density".to_string()
                },
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .reclaim_score
            .cmp(&left.reclaim_score)
            .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
            .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
    });
    candidates
}

#[derive(Debug, Clone)]
struct LivePageEntry {
    object_key: String,
    kind: &'static str,
    component: Option<String>,
    address: PageAddress,
}

#[derive(Debug, Default)]
struct StoragePageOwnershipValidation {
    mismatches: Vec<StorageRecoveryPageOwnerMismatch>,
    missing_owner_page_refs: usize,
}

fn collect_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    entries.extend(shard.strings.iter().map(|(key, address)| LivePageEntry {
        object_key: key.clone(),
        kind: "string",
        component: None,
        address: address.clone(),
    }));
    for (key, fields) in &shard.hashes {
        entries.extend(fields.iter().map(|(field, address)| LivePageEntry {
            object_key: key.clone(),
            kind: "hash",
            component: Some(field.clone()),
            address: address.clone(),
        }));
    }
    for (key, members) in &shard.sets {
        entries.extend(members.iter().map(|(member, address)| LivePageEntry {
            object_key: key.clone(),
            kind: "set",
            component: Some(hex::encode(member)),
            address: address.clone(),
        }));
    }
    for (key, series) in &shard.features {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| LivePageEntry {
                    object_key: key.clone(),
                    kind: "feature",
                    component: None,
                    address,
                }),
        );
    }
    for (key, series) in &shard.sequences {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| LivePageEntry {
                    object_key: key.clone(),
                    kind: "sequence",
                    component: None,
                    address,
                }),
        );
    }
    for (key, series) in &shard.ips {
        entries.extend(series.iter().map(|(timestamp_ms, address)| LivePageEntry {
            object_key: key.clone(),
            kind: "ips",
            component: Some(timestamp_ms.to_string()),
            address: address.clone(),
        }));
    }
    for (key, series) in &shard.ips_meta {
        entries.extend(series.iter().map(|(timestamp_ms, meta)| LivePageEntry {
            object_key: key.clone(),
            kind: "ips",
            component: Some(timestamp_ms.to_string()),
            address: meta.address.clone(),
        }));
    }
    entries
}

fn expected_live_page_object_id(shard_id: ShardId, entry: &LivePageEntry) -> u64 {
    stable_page_object_id(
        shard_id,
        entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    )
}

fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_slots(shard_id, shard, &BTreeSet::new(), |_| 0)
}

fn storage_object_lifecycle_report_for_slots(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let entries = collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_page_refs = 0u64;
    let mut owner_mismatch_page_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_page_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
            missing_owner_page_refs = missing_owner_page_refs.saturating_add(1);
        }
        match entry.address.object_id {
            Some(actual_object_id) => {
                actual_object_owners
                    .entry(actual_object_id)
                    .or_default()
                    .insert(expected_object_id);
                if actual_object_id != expected_object_id {
                    owner_mismatch_page_refs = owner_mismatch_page_refs.saturating_add(1);
                }
            }
            None => {}
        }
    }

    let reused_object_ids = actual_object_owners
        .into_iter()
        .filter_map(|(actual_object_id, expected_ids)| {
            (expected_ids.len() > 1).then_some(actual_object_id)
        })
        .collect::<Vec<_>>();
    let tombstoned_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_slot = routing_slot_for_key(key);
            (selected_slots.is_empty() || selected_slots.contains(&routing_slot))
                && !record_exists(shard, key)
        })
        .cloned()
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_page_refs: entries.len() as u64,
        stale_object_ids: 0,
        tombstoned_object_ids: tombstoned_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_page_refs,
        owner_mismatch_page_refs,
        reused_object_ids,
        tombstoned_object_keys,
    }
}

fn slot_storage_summaries(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> Vec<SlotStorageSummary> {
    let mut slots = BTreeMap::<u32, SlotStorageSummary>::new();
    let mut objects_by_slot = BTreeMap::<u32, BTreeSet<String>>::new();
    let mut page_segments_by_slot = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length);
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length);
        if let Some(zone_id) = entry.address.zone_id {
            summary.last_compacted_zone = Some(
                summary
                    .last_compacted_zone
                    .map_or(zone_id, |current| current.max(zone_id)),
            );
        }
        objects_by_slot
            .entry(routing_slot)
            .or_default()
            .insert(entry.object_key);
        page_segments_by_slot
            .entry(routing_slot)
            .or_default()
            .insert(entry.address.page_segment_id);
    }
    for key in &shard.dirty_objects {
        let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.dirty_object_count = summary.dirty_object_count.saturating_add(1);
        summary.dirty_generation = summary.dirty_generation.saturating_add(1);
    }
    for (routing_slot, summary) in &mut slots {
        summary.object_count = objects_by_slot
            .get(routing_slot)
            .map(|objects| objects.len() as u64)
            .unwrap_or_default();
        summary.page_segment_ids = page_segments_by_slot
            .get(routing_slot)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    slots.into_values().collect()
}

fn merge_last_dump_sequence(
    mut summaries: Vec<SlotStorageSummary>,
    manifest: &SlotDumpManifest,
) -> Vec<SlotStorageSummary> {
    let dumped_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
    for summary in &mut summaries {
        if dumped_slots.contains(&summary.routing_slot) {
            summary.last_dump_sequence = manifest.index_log_sequence;
        }
    }
    summaries
}

fn slot_dump_manifest_comparable_summaries(
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
) -> Vec<SlotStorageSummary> {
    comparable_slot_dump_summaries(
        slot_storage_summaries(shard, 0, u32::MAX)
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect(),
    )
}

fn comparable_slot_dump_summaries(
    mut summaries: Vec<SlotStorageSummary>,
) -> Vec<SlotStorageSummary> {
    for summary in &mut summaries {
        summary.dirty_object_count = 0;
        summary.dirty_generation = 0;
        summary.last_dump_sequence = 0;
        summary.page_segment_ids.sort_unstable();
        summary.page_segment_ids.dedup();
    }
    summaries.sort_by_key(|summary| summary.routing_slot);
    summaries
}

fn collect_live_page_addresses(shard: &ShardState) -> Vec<PageAddress> {
    let mut addresses = Vec::new();
    addresses.extend(shard.strings.values().cloned());
    for fields in shard.hashes.values() {
        addresses.extend(fields.values().cloned());
    }
    for members in shard.sets.values() {
        addresses.extend(members.values().cloned());
    }
    for series in shard.features.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.sequences.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.ips.values() {
        addresses.extend(series.values().cloned());
    }
    for series in shard.ips_meta.values() {
        addresses.extend(series.values().map(|meta| meta.address.clone()));
    }
    addresses
}

fn unique_timestamped_kv_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    let mut addresses = series
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.page_segment_id
            .cmp(&right.page_segment_id)
            .then(left.offset.cmp(&right.offset))
            .then(left.length.cmp(&right.length))
    });
    addresses
}

fn unique_feature_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    unique_timestamped_kv_page_addresses(series)
}

fn storage_feature_page_layout_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> StorageFeaturePageLayoutReport {
    let mut report = StorageFeaturePageLayoutReport::default();
    for (key, series) in &shard.features {
        report.indexed_feature_points = report.indexed_feature_points.saturating_add(series.len());
        let mut timestamps_by_address = HashMap::<PageAddress, BTreeSet<u64>>::new();
        for (timestamp_ms, address) in series {
            timestamps_by_address
                .entry(address.clone())
                .or_default()
                .insert(*timestamp_ms);
        }
        report.unique_feature_page_refs = report
            .unique_feature_page_refs
            .saturating_add(timestamps_by_address.len());

        for (address, indexed_timestamps) in timestamps_by_address {
            match page_store.read(&address) {
                Ok(bytes) => match decode_feature_page_strict(&bytes) {
                    PackedFeaturePageDecode::Packed(points) => {
                        report.packed_feature_pages = report.packed_feature_pages.saturating_add(1);
                        let mut packed_timestamp_counts = BTreeMap::<u64, usize>::new();
                        for point in &points {
                            let count = packed_timestamp_counts
                                .entry(point.timestamp_ms)
                                .or_default();
                            if *count == 1 {
                                report.duplicate_packed_timestamps.push(
                                    feature_page_timestamp_mismatch(
                                        key,
                                        point.timestamp_ms,
                                        &address,
                                    ),
                                );
                            }
                            *count = (*count).saturating_add(1);
                        }
                        let packed_timestamps = points
                            .into_iter()
                            .map(|point| point.timestamp_ms)
                            .collect::<BTreeSet<_>>();
                        for timestamp_ms in
                            indexed_timestamps.difference(&packed_timestamps).copied()
                        {
                            report
                                .missing_indexed_timestamps
                                .push(feature_page_timestamp_mismatch(key, timestamp_ms, &address));
                        }
                        for timestamp_ms in
                            packed_timestamps.difference(&indexed_timestamps).copied()
                        {
                            report
                                .orphan_packed_timestamps
                                .push(feature_page_timestamp_mismatch(key, timestamp_ms, &address));
                        }
                    }
                    PackedFeaturePageDecode::Corrupt(error) => {
                        report
                            .corrupt_packed_feature_pages
                            .push(feature_page_error(key, &address, error));
                    }
                    PackedFeaturePageDecode::Legacy => {
                        report.legacy_feature_value_pages =
                            report.legacy_feature_value_pages.saturating_add(1);
                        if indexed_timestamps.len() > 1 {
                            report.corrupt_packed_feature_pages.push(feature_page_error(
                                key,
                                &address,
                                "legacy feature page shared by multiple timestamps",
                            ));
                        }
                    }
                },
                Err(err) => report.corrupt_packed_feature_pages.push(feature_page_error(
                    key,
                    &address,
                    err.to_string(),
                )),
            }
        }
    }
    report
}

fn feature_page_error(
    key: &str,
    address: &PageAddress,
    error: impl Into<String>,
) -> StorageFeaturePageError {
    StorageFeaturePageError {
        key: key.to_string(),
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        error: error.into(),
    }
}

fn feature_page_timestamp_mismatch(
    key: &str,
    timestamp_ms: u64,
    address: &PageAddress,
) -> StorageFeaturePageTimestampMismatch {
    StorageFeaturePageTimestampMismatch {
        key: key.to_string(),
        timestamp_ms,
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
    }
}

fn compaction_utility_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    let addresses = collect_live_page_addresses(shard);
    let live_page_segment_ids = addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let total_page_count = live_page_segment_ids
        .iter()
        .map(|page_segment_id| {
            segment_page_counts
                .get(page_segment_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_page_refs = addresses.len() as u64;
    let stale_page_estimate = total_page_count.saturating_sub(live_page_refs);
    let live_ref_density_basis_points = if total_page_count == 0 {
        0
    } else {
        live_page_refs.saturating_mul(10_000) / total_page_count
    };
    ShardCompactionUtilityReport {
        live_page_segment_count: live_page_segment_ids.len(),
        total_page_count,
        live_page_refs,
        stale_page_estimate,
        live_ref_density_basis_points,
    }
}

fn compact_page_addresses<'a>(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    addresses: impl IntoIterator<Item = &'a mut PageAddress>,
    rewritten_page_refs: &mut usize,
) -> Result<(), Status> {
    for address in addresses {
        let bytes = read_page_bytes(cache, page_store, shard_id, address).ok_or_else(|| {
            Status::error(
                "page_compaction_failed",
                "missing page bytes during compaction",
            )
        })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, address.object_id, address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
            ),
            bytes,
        );
        *rewritten_page_refs += 1;
    }
    Ok(())
}

fn compact_feature_page_addresses(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    series: &mut BTreeMap<u64, PageAddress>,
    rewritten_page_refs: &mut usize,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_page_addresses(series);
    let mut rewritten = HashMap::<PageAddress, PageAddress>::new();
    for old_address in unique_addresses {
        let bytes =
            read_page_bytes(cache, page_store, shard_id, &old_address).ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing feature page bytes during compaction",
                )
            })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, old_address.object_id, old_address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
            ),
            bytes,
        );
        rewritten.insert(old_address, new_address);
        *rewritten_page_refs += 1;
    }
    for address in series.values_mut() {
        if let Some(new_address) = rewritten.get(address) {
            *address = new_address.clone();
        }
    }
    Ok(())
}

fn append_value(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    async_storage: bool,
) -> Result<PageAddress, PageStoreError> {
    if !async_storage {
        return page_store.append_with_page_metadata(bytes, object_id, routing_slot);
    }
    let address = PageAddress {
        page_segment_id: HOT_PAGE_SEGMENT_ID,
        offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
        length: bytes.len() as u64,
        page_id: None,
        object_id,
        routing_slot,
        zone_id: None,
        sha256: None,
    };
    let bytes = bytes.to_vec();
    cache.put_memory_only(
        CacheKey::page_with_slot(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
        ),
        bytes,
    );
    Ok(address)
}

fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    if memory_only {
        cache.invalidate_memory_only(&key);
    } else {
        let _ = cache.invalidate(&key);
    }
}

fn record_exists(shard: &ShardState, key: &str) -> bool {
    associated_record_keys(key)
        .iter()
        .any(|record_key| record_exists_exact(shard, record_key))
}

fn record_exists_exact(shard: &ShardState, key: &str) -> bool {
    shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.features.contains_key(key)
        || shard.sequences.contains_key(key)
        || shard.ips.contains_key(key)
        || shard.risk.contains_key(key)
        || shard.risk_changes.contains_key(key)
        || shard.risk_fol.contains_key(key)
}

fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    let _ = cache.invalidate_record(shard_id, "hash", key);
    let _ = cache.invalidate_record(shard_id, "set", key);
    let _ = cache.invalidate_record(shard_id, "feature", key);
}

fn read_sequence_row(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &PageAddress,
) -> Option<SequenceFeatureRow> {
    let bytes = read_page_bytes(cache, page_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .and_then(|point| serde_json::from_slice(&point.value).ok()),
        PackedFeaturePageDecode::Legacy => serde_json::from_slice(&bytes).ok(),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn sequence_filter_matches(row: &SequenceFeatureRow, filter: &FeatureFilter) -> bool {
    let lhs = match filter.field.as_str() {
        "gid" => row.gid,
        "action_type" => row.action_type as u64,
        "duration" => row.duration as u64,
        "author_id" => row.author_id,
        _ => return false,
    };
    match filter.op {
        FeatureFilterOp::Equal => lhs == filter.value,
        FeatureFilterOp::NotEqual => lhs != filter.value,
        FeatureFilterOp::GreaterThan => lhs > filter.value,
        FeatureFilterOp::GreaterOrEqual => lhs >= filter.value,
        FeatureFilterOp::LessThan => lhs < filter.value,
        FeatureFilterOp::LessOrEqual => lhs <= filter.value,
    }
}

fn sequence_rows_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: &[FeatureFilter],
) -> Vec<SequenceFeatureRow> {
    shard
        .sequences
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row(cache, page_store, shard_id, *timestamp_ms, address)
                })
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn aggregate_feature_values(values: &[Vec<u8>], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" => values.iter().filter_map(parse_i64).sum(),
        "min" => values
            .iter()
            .filter_map(parse_i64)
            .min()
            .unwrap_or_default(),
        "max" => values
            .iter()
            .filter_map(parse_i64)
            .max()
            .unwrap_or_default(),
        "count" | "" => values.len() as i64,
        _ => values.len() as i64,
    }
}

fn aggregate_risk_values(values: &[i64], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" | "count" | "" => values.iter().sum(),
        "events" | "len" => values.len() as i64,
        "min" => values.iter().copied().min().unwrap_or_default(),
        "max" => values.iter().copied().max().unwrap_or_default(),
        "first" => values.first().copied().unwrap_or_default(),
        "last" => values.last().copied().unwrap_or_default(),
        _ => values.iter().sum(),
    }
}

fn is_risk_change_aggregator(aggregator: &str) -> bool {
    aggregator.eq_ignore_ascii_case("change")
}

fn count_risk_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let mut unique = BTreeSet::new();
    if let Some(series) = shard.risk_changes.get(key) {
        for (_, values) in series.range(start_ms..=end_ms) {
            unique.extend(values.iter().cloned());
        }
    }
    unique.len() as i64
}

fn risk_family_key(family: RiskFamily, key: &str) -> String {
    format!("risk:{}:{key}", risk_family_name(family))
}

fn risk_family_name(family: RiskFamily) -> &'static str {
    match family {
        RiskFamily::H => "h",
        RiskFamily::Cpc => "cpc",
        RiskFamily::Fol => "fol",
    }
}

fn ips_points_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
) -> Vec<FeaturePoint> {
    shard
        .ips
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count.unwrap_or(usize::MAX))
                .filter_map(|(timestamp_ms, address)| {
                    read_page_bytes(cache, page_store, shard_id, address).map(|value| {
                        FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value,
                        }
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn ips_points_in_range_with_options(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
    action_type: Option<u32>,
    table_id: Option<u64>,
) -> Vec<FeaturePoint> {
    let Some(series) = shard.ips_meta.get(key) else {
        return ips_points_in_range(
            cache, page_store, shard_id, shard, key, start_ms, end_ms, count,
        );
    };
    series
        .range(start_ms..=end_ms)
        .filter(|(_, meta)| {
            action_type
                .map(|expected| meta.action_type == Some(expected))
                .unwrap_or(true)
                && table_id
                    .map(|expected| meta.table_id == Some(expected))
                    .unwrap_or(true)
        })
        .take(count.unwrap_or(usize::MAX))
        .filter_map(|(timestamp_ms, meta)| {
            read_page_bytes(cache, page_store, shard_id, &meta.address).map(|value| FeaturePoint {
                timestamp_ms: *timestamp_ms,
                value,
            })
        })
        .collect()
}

fn ips_stats_in_range(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> IpsStats {
    let mut total = 0u64;
    let mut first_timestamp_ms = None;
    let mut last_timestamp_ms = None;
    let mut action_type_counts = BTreeMap::<u32, u64>::new();
    let mut table_id_counts = BTreeMap::<u64, u64>::new();

    if let Some(series) = shard.ips.get(key) {
        for (timestamp_ms, _) in series.range(start_ms..=end_ms) {
            total += 1;
            first_timestamp_ms.get_or_insert(*timestamp_ms);
            last_timestamp_ms = Some(*timestamp_ms);
        }
    }
    if let Some(series) = shard.ips_meta.get(key) {
        for (_, meta) in series.range(start_ms..=end_ms) {
            if let Some(action_type) = meta.action_type {
                *action_type_counts.entry(action_type).or_default() += 1;
            }
            if let Some(table_id) = meta.table_id {
                *table_id_counts.entry(table_id).or_default() += 1;
            }
        }
    }

    IpsStats {
        total,
        first_timestamp_ms,
        last_timestamp_ms,
        action_type_counts: action_type_counts.into_iter().collect(),
        table_id_counts: table_id_counts.into_iter().collect(),
    }
}

fn read_page_bytes(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    address: &PageAddress,
) -> Option<Vec<u8>> {
    let cache_key = CacheKey::page_with_slot(
        shard_id,
        address.page_segment_id,
        address.offset,
        address.length,
        address.routing_slot,
    );
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
}

fn sorted_feature_points(mut points: Vec<FeaturePoint>) -> Vec<FeaturePoint> {
    let mut by_timestamp = BTreeMap::new();
    for point in points.drain(..) {
        by_timestamp.insert(point.timestamp_ms, point);
    }
    by_timestamp.into_values().collect()
}

fn encode_feature_page(points: &[FeaturePoint]) -> Vec<u8> {
    let page = PackedFeaturePage {
        version: 1,
        points: points.to_vec(),
    };
    let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
    if let Ok(mut payload) = serde_json::to_vec(&page) {
        bytes.append(&mut payload);
    }
    bytes
}

fn append_timestamped_kv_pages(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_slot: u32,
    async_storage: bool,
) -> Result<Vec<(u64, PageAddress)>, PageStoreError> {
    let object_id = stable_page_object_id(shard_id, kind, key, None);
    let mut refs = Vec::new();
    for chunk in chunk_timestamped_kv_points(points) {
        let packed = encode_feature_page(&chunk);
        let address = append_value(
            cache,
            page_store,
            shard_id,
            &packed,
            Some(object_id),
            Some(routing_slot),
            async_storage,
        )?;
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    Ok(refs)
}

fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();

    for point in points {
        current.push(point);
        let encoded_len = encode_feature_page(&current).len();
        if encoded_len > TIMESTAMPED_KV_PAGE_TARGET_BYTES && current.len() > 1 {
            let overflow = current.pop().expect("current chunk is non-empty");
            chunks.push(current);
            current = vec![overflow];
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
fn decode_feature_page(bytes: &[u8]) -> Option<Vec<FeaturePoint>> {
    match decode_feature_page_strict(bytes) {
        PackedFeaturePageDecode::Packed(points) => Some(points),
        PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn decode_feature_page_strict(bytes: &[u8]) -> PackedFeaturePageDecode {
    let Some(payload) = bytes.strip_prefix(FEATURE_PAGE_MAGIC) else {
        return PackedFeaturePageDecode::Legacy;
    };
    let page = match serde_json::from_slice::<PackedFeaturePage>(payload) {
        Ok(page) => page,
        Err(err) => {
            return PackedFeaturePageDecode::Corrupt(format!(
                "invalid packed feature page payload: {err}"
            ));
        }
    };
    if page.version != 1 {
        return PackedFeaturePageDecode::Corrupt(format!(
            "unsupported packed feature page version {}",
            page.version
        ));
    }
    PackedFeaturePageDecode::Packed(page.points)
}

fn read_feature_point(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &PageAddress,
) -> Option<FeaturePoint> {
    let bytes = read_page_bytes(cache, page_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn cache_entry_routing_slot(entry: &CacheEntryInfo) -> Option<u32> {
    entry
        .selector
        .strip_prefix("slot-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn object_manager_stats(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> ObjectManagerStats {
    let object_count = shard.strings.len()
        + shard.hashes.len()
        + shard.sets.len()
        + shard.features.len()
        + shard.sequences.len()
        + shard.ips.len()
        + shard.risk.len();
    let page_ref_count = shard.strings.len()
        + shard.hashes.values().map(HashMap::len).sum::<usize>()
        + shard.sets.values().map(BTreeMap::len).sum::<usize>()
        + shard.features.values().map(BTreeMap::len).sum::<usize>()
        + shard.sequences.values().map(BTreeMap::len).sum::<usize>()
        + shard.ips.values().map(BTreeMap::len).sum::<usize>();
    let routing_slot_count = routing_slot_count(start_routing_slot, end_routing_slot);
    let dirty_slots = shard
        .dirty_objects
        .iter()
        .map(|key| slot_for_object(key, start_routing_slot, routing_slot_count))
        .collect::<BTreeSet<_>>();
    ObjectManagerStats {
        object_count,
        page_ref_count,
        dirty_object_count: shard.dirty_objects.len(),
        dirty_slot_count: dirty_slots.len(),
        routing_slot_count,
    }
}

fn routing_slot_count(start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    if end_routing_slot < start_routing_slot {
        return 0;
    }
    end_routing_slot
        .saturating_sub(start_routing_slot)
        .saturating_add(1)
}

fn slot_for_object(key: &str, start_routing_slot: u32, routing_slot_count: u32) -> u32 {
    if routing_slot_count == 0 {
        return start_routing_slot;
    }
    start_routing_slot + (stable_object_hash(key) % routing_slot_count as u64) as u32
}

fn stable_object_hash(key: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn stable_page_object_id(shard_id: ShardId, kind: &str, key: &str, component: Option<&str>) -> u64 {
    let mut identity = format!("{shard_id}:{kind}:{key}");
    if let Some(component) = component {
        identity.push(':');
        identity.push_str(component);
    }
    stable_object_hash(&identity)
}

fn page_routing_slot(key: &str, start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    slot_for_object(
        key,
        start_routing_slot,
        routing_slot_count(start_routing_slot, end_routing_slot),
    )
}

fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskChangeAdd { key, .. }
        | Command::RiskFolSet { key, .. } => vec![key.clone()],
        Command::RiskSet { family, key, .. } | Command::RiskSetAndGet { family, key, .. } => {
            vec![risk_family_key(*family, key)]
        }
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::IpsQueryLast { .. }
        | Command::IpsQueryRange { .. }
        | Command::IpsBatchQueryLast { .. }
        | Command::IpsCount { .. }
        | Command::IpsQueryRangeWithOptions { .. }
        | Command::IpsSnapshot { .. }
        | Command::IpsStat { .. }
        | Command::IpsFilter { .. }
        | Command::RiskCount { .. }
        | Command::RiskQuery { .. }
        | Command::RiskDetail { .. }
        | Command::RiskFamilyQuery { .. }
        | Command::RiskFolQuery { .. }
        | Command::RiskManager { .. } => Vec::new(),
    }
}

fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskChangeAdd { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFolSet { .. }
    )
}

fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    if let Some(limit) = if write_command {
        config.write_qps
    } else {
        config.read_qps
    } {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
        .map(|info| info.table_name.trim())
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn validate_command_preconditions(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    command: &Command,
) -> Result<(), Status> {
    match command {
        Command::CommonExpire { key, .. } => {
            if shard
                .expires_at_ms
                .get(key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false)
                || !record_exists(shard, key)
            {
                return Err(Status::error("not_found", "key not found"));
            }
        }
        Command::FeatureAppend { key, points }
        | Command::FeatureAppendWithPolicy { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureReplace { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        _ => {}
    }

    if let Command::HashIncrBy {
        key,
        field,
        increment,
    } = command
    {
        if shard
            .expires_at_ms
            .get(key)
            .map(|expires_at| *expires_at <= now_ms())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(bytes) = shard
            .hashes
            .get(key)
            .and_then(|entries| entries.get(field))
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
        else {
            return 0_i64
                .checked_add(*increment)
                .map(|_| ())
                .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"));
        };
        let current = parse_i64(&bytes)
            .ok_or_else(|| Status::error("unmatched", "hash value is not an integer"))?;
        current
            .checked_add(*increment)
            .map(|_| ())
            .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"))?;
    }
    Ok(())
}

fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    if let Ok(Some(bytes)) = cache.get(&key) {
        if let Ok(response) = serde_json::from_slice::<CommandResponse>(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Ok(bytes) = serde_json::to_vec(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page_store::PageStoreZoneState;
    use crate::types::parse_cpp_feature_filters;

    fn wait_for_fresh_admission_second() {
        loop {
            let elapsed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch");
            if elapsed.subsec_millis() < 100 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn live_page_segment_ids_scan_all_index_backed_data_models() {
        let mut shard = ShardState::default();
        shard.strings.insert(
            "string".to_string(),
            PageAddress {
                page_segment_id: 7,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.hashes.entry("hash".to_string()).or_default().insert(
            "field".to_string(),
            PageAddress {
                page_segment_id: 8,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.sets.entry("set".to_string()).or_default().insert(
            b"member".to_vec(),
            PageAddress {
                page_segment_id: 9,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard
            .features
            .entry("feature".to_string())
            .or_default()
            .insert(
                10,
                PageAddress {
                    page_segment_id: 10,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
            );
        shard
            .sequences
            .entry("sequence".to_string())
            .or_default()
            .insert(
                11,
                PageAddress {
                    page_segment_id: 11,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
            );
        shard.ips.entry("ips".to_string()).or_default().insert(
            12,
            PageAddress {
                page_segment_id: 12,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.ips_meta.entry("ips".to_string()).or_default().insert(
            13,
            IpsPointMeta {
                address: PageAddress {
                    page_segment_id: 13,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
                action_type: Some(1),
                table_id: Some(2),
                request_id: Some("r".to_string()),
            },
        );
        shard
            .risk
            .entry("risk".to_string())
            .or_default()
            .insert(14, 1);

        let ids = collect_live_page_segment_ids(&shard)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![7, 8, 9, 10, 11, 12, 13]);
    }

    #[test]
    fn page_compaction_rewrites_live_addresses_and_allows_old_segment_gc() {
        let page_dir = unique_temp_path("compact-pages");
        let index_dir = unique_temp_path("compact-index");
        let page_store = LocalPageStore::new(&page_dir);
        let engine = TemporalEngine::with_cache_page_store_and_index_dir(
            MultiLayerCache::default(),
            page_store.clone(),
            &index_dir,
        );
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v1".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v2".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(engine.live_page_segment_ids(1), vec![0]);

        let report = engine.compact_shard_pages(1).unwrap();
        assert_eq!(report.previous_page_segment_id, 0);
        assert_eq!(report.compacted_page_segment_id, 1);
        assert_eq!(report.rewritten_page_refs, 2);
        assert_eq!(report.stale_page_segment_ids, vec![0]);
        assert_eq!(report.before.live_page_segment_count, 1);
        assert_eq!(report.before.total_page_count, 3);
        assert_eq!(report.before.live_page_refs, 2);
        assert_eq!(report.before.stale_page_estimate, 1);
        assert_eq!(report.before.live_ref_density_basis_points, 6_666);
        assert_eq!(report.after.live_page_segment_count, 1);
        assert_eq!(report.after.total_page_count, 2);
        assert_eq!(report.after.live_page_refs, 2);
        assert_eq!(report.after.stale_page_estimate, 0);
        assert_eq!(report.after.live_ref_density_basis_points, 10_000);
        assert_eq!(engine.live_page_segment_ids(1), vec![1]);
        {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let string_address = shard.strings.get("k").expect("string address");
            let hash_address = shard
                .hashes
                .get("h")
                .and_then(|fields| fields.get("f"))
                .expect("hash address");
            assert_eq!(
                string_address.object_id,
                Some(stable_page_object_id(1, "string", "k", None))
            );
            assert_eq!(
                string_address.routing_slot,
                Some(page_routing_slot("k", 0, u32::MAX))
            );
            assert_eq!(
                hash_address.object_id,
                Some(stable_page_object_id(1, "hash", "h", Some("f")))
            );
            assert_eq!(
                hash_address.routing_slot,
                Some(page_routing_slot("h", 0, u32::MAX))
            );
        }

        let gc = page_store
            .gc_segments_before_with_live_refs(1, engine.live_page_segment_ids(1))
            .unwrap();
        assert_eq!(gc.removed_page_segment_ids, vec![0]);
        assert_eq!(page_store.segment_ids().unwrap(), vec![1]);

        let restarted = TemporalEngine::with_cache_page_store_and_index_dir(
            MultiLayerCache::default(),
            page_store,
            &index_dir,
        );
        restarted.load_shard(1);
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashGet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"hv".to_vec())
            }
        );
    }

    #[test]
    fn recovery_reports_owner_mismatch_and_compaction_refuses_it() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "owned".to_string(),
                        value: b"value".to_vec(),
                    },
                })
                .status
                .ok
        );

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let address = shard.strings.get_mut("owned").expect("string address");
            address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
        }

        let recovery = engine.storage_recovery_report(1);
        assert_eq!(recovery.owner_mismatch_page_refs.len(), 1);
        assert!(!recovery.segment_integrity.integrity_ok);
        assert_eq!(recovery.segment_integrity.owner_mismatch_page_ref_count, 1);
        assert_eq!(recovery.segment_integrity.missing_owner_page_ref_count, 0);
        assert_eq!(recovery.object_lifecycle.live_object_ids, 1);
        assert_eq!(recovery.object_lifecycle.live_page_refs, 1);
        assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
        assert_eq!(
            recovery.owner_mismatch_page_refs[0].expected_object_id,
            stable_page_object_id(1, "string", "owned", None)
        );
        assert_eq!(recovery.boundary.owner_mismatch_page_refs.len(), 1);
        assert_eq!(
            recovery.boundary.object_lifecycle.owner_mismatch_page_refs,
            1
        );

        let err = engine.compact_shard_pages(1).unwrap_err();
        assert_eq!(err.code, "page_compaction_owner_mismatch");
    }

    #[test]
    fn recovery_reports_reused_object_id_conflicts() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for key in ["first", "second"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: key.as_bytes().to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let reused_object_id = {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let first_object_id = shard
                .strings
                .get("first")
                .and_then(|address| address.object_id)
                .expect("first object id");
            let second = shard.strings.get_mut("second").expect("second address");
            second.object_id = Some(first_object_id);
            first_object_id
        };

        let recovery = engine.storage_recovery_report(1);
        assert_eq!(recovery.object_lifecycle.live_object_ids, 2);
        assert_eq!(recovery.object_lifecycle.live_page_refs, 2);
        assert_eq!(recovery.object_lifecycle.reused_object_id_conflicts, 1);
        assert_eq!(
            recovery.object_lifecycle.reused_object_ids,
            vec![reused_object_id]
        );
        assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
        assert_eq!(
            recovery
                .boundary
                .object_lifecycle
                .reused_object_id_conflicts,
            1
        );
    }

    #[test]
    fn crash_recovery_report_covers_oplog_index_page_and_zone_manifest() {
        let cache_dir = unique_temp_path("recovery-cache");
        let page_dir = unique_temp_path("recovery-pages");
        let index_dir = unique_temp_path("recovery-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v1".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );

        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);

        assert!(report.index_bytes > 0);
        assert!(report.index_write_atomic);
        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_segment_ids, vec![0, 1]);
        assert_eq!(report.live_page_segment_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
        assert_eq!(report.readable_page_refs, 2);
        assert!(report.all_live_pages_readable);
        assert!(report.segment_integrity.integrity_ok);
        assert!(!report.segment_integrity.reclaim_required);
        assert_eq!(report.segment_integrity.indexed_page_segment_count, 2);
        assert_eq!(report.segment_integrity.discovered_page_segment_count, 2);
        assert_eq!(report.segment_integrity.live_page_segment_count, 2);
        assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
        assert_eq!(report.zone_descriptors.len(), 2);
        assert_eq!(report.zone_descriptors[0].state, PageStoreZoneState::Sealed);
        assert_eq!(report.zone_descriptors[1].state, PageStoreZoneState::Active);
        assert_eq!(report.zone_summary.sealed_zones, 1);
        assert_eq!(report.zone_summary.active_zones, 1);
        assert_eq!(report.zone_summary.delayed_destroy_zones, 0);
        assert_eq!(
            report.zone_summary.sealed_physical_bytes,
            report.zone_descriptors[0].physical_bytes
        );
        assert_eq!(
            report.zone_summary.active_physical_bytes,
            report.zone_descriptors[1].physical_bytes
        );
        assert_eq!(
            report.zone_summary.live_physical_bytes,
            report.zone_descriptors[0].physical_bytes + report.zone_descriptors[1].physical_bytes
        );
        assert_eq!(report.page_segment_live_reports.len(), 2);
        assert_eq!(report.page_segment_live_reports[0].page_segment_id, 0);
        assert_eq!(report.page_segment_live_reports[0].page_count, 1);
        assert_eq!(report.page_segment_live_reports[0].live_page_refs, 1);
        assert_eq!(
            report.page_segment_live_reports[0].readable_live_page_refs,
            1
        );
        assert_eq!(
            report.page_segment_live_reports[0].unreadable_live_page_refs,
            0
        );
        assert_eq!(report.page_segment_live_reports[0].stale_page_estimate, 0);
        assert_eq!(
            report.page_segment_live_reports[0].live_ref_density_basis_points,
            10_000
        );
        assert_eq!(report.page_segment_live_reports[0].live_object_count, 1);
        assert_eq!(
            report.page_segment_live_reports[0].live_routing_slot_count,
            1
        );
        assert_eq!(report.page_segment_live_reports[0].live_logical_bytes, 2);
        assert!(report.page_segment_live_reports[0].live_physical_bytes > 0);

        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashGet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"hv".to_vec())
            }
        );
    }

    #[test]
    fn crash_recovery_report_marks_stale_segment_density_after_overwrite() {
        let cache_dir = unique_temp_path("recovery-density-cache");
        let page_dir = unique_temp_path("recovery-density-pages");
        let index_dir = unique_temp_path("recovery-density-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "hot".to_string(),
                        value: b"old".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "hot".to_string(),
                        value: b"new".to_vec(),
                    },
                })
                .status
                .ok
        );

        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);
        let segment = report
            .page_segment_live_reports
            .iter()
            .find(|segment| segment.page_segment_id == 0)
            .expect("segment 0 live-density report");

        assert_eq!(segment.page_count, 2);
        assert_eq!(segment.live_page_refs, 1);
        assert_eq!(segment.readable_live_page_refs, 1);
        assert_eq!(segment.stale_page_estimate, 1);
        assert_eq!(segment.live_ref_density_basis_points, 5_000);
        assert_eq!(segment.live_logical_bytes, 3);
        assert_eq!(segment.live_object_count, 1);
        assert_eq!(segment.live_routing_slot_count, 1);
    }

    #[test]
    fn crash_recovery_rebuilds_missing_zone_manifest_from_page_stream() {
        let cache_dir = unique_temp_path("recovery-rebuild-cache");
        let page_dir = unique_temp_path("recovery-rebuild-pages");
        let index_dir = unique_temp_path("recovery-rebuild-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "before".to_string(),
                        value: b"before".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "after".to_string(),
                        value: b"after".to_vec(),
                    },
                })
                .status
                .ok
        );

        fs::remove_file(page_dir.join("page_zone_manifest.json")).unwrap();
        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);

        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_segment_ids, vec![0, 1]);
        assert_eq!(report.live_page_segment_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
        assert!(report.all_live_pages_readable);
        assert_eq!(report.zone_descriptors.len(), 2);
        assert_eq!(report.zone_descriptors[0].state, PageStoreZoneState::Sealed);
        assert_eq!(report.zone_descriptors[1].state, PageStoreZoneState::Active);
        assert_eq!(report.zone_summary.sealed_zones, 1);
        assert_eq!(report.zone_summary.active_zones, 1);
        assert!(report.zone_summary.live_physical_bytes > 0);
        assert!(page_dir.join("page_zone_manifest.json").exists());
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"before".to_vec())
            }
        );
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"after".to_vec())
            }
        );
    }

    #[test]
    fn string_round_trip() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
    }

    #[test]
    fn durable_writes_stamp_stable_object_ids_on_page_addresses() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    table_name: "table".to_string(),
                    shard_uri: "local://1".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    load_version: 1,
                    local_node_id: None,
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let string_address = shard.strings.get("k").expect("string address");
        let hash_address = shard
            .hashes
            .get("h")
            .and_then(|fields| fields.get("f"))
            .expect("hash address");

        assert_eq!(
            string_address.object_id,
            Some(stable_page_object_id(1, "string", "k", None))
        );
        assert_eq!(
            string_address.routing_slot,
            Some(page_routing_slot("k", 10, 20))
        );
        assert_eq!(string_address.zone_id, Some(string_address.page_segment_id));
        assert_eq!(
            hash_address.object_id,
            Some(stable_page_object_id(1, "hash", "h", Some("f")))
        );
        assert_eq!(
            hash_address.routing_slot,
            Some(page_routing_slot("h", 10, 20))
        );
        assert_eq!(hash_address.zone_id, Some(hash_address.page_segment_id));
        assert_ne!(string_address.object_id, hash_address.object_id);
    }

    #[test]
    fn string_setex_sets_value_and_ttl() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                        ttl_ms: 60_000,
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        let ttl = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonTtl {
                key: "k".to_string(),
            },
        });
        let CommandResponse::Integer { value } = ttl.response else {
            panic!("expected ttl integer response");
        };
        assert!(value > 0);
    }

    #[test]
    fn expiry_sweep_removes_expired_records_without_lazy_read() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: "expire-me".to_string(),
                        value: b"gone".to_vec(),
                        ttl_ms: 1,
                    },
                })
                .status
                .ok
        );
        std::thread::sleep(std::time::Duration::from_millis(5));

        let report = engine.sweep_expired_records(1).unwrap();
        assert_eq!(report.expired_records_removed, 1);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "expire-me".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert_eq!(
            engine
                .sweep_expired_records(1)
                .unwrap()
                .expired_records_removed,
            0
        );
    }

    #[test]
    fn string_get_uses_memory_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let engine = TemporalEngine::new(cache.clone());
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert!(stats.memory_hits >= 1);
        assert!(stats.puts >= 2);
    }

    #[test]
    fn memory_miss_reads_local_page_file_using_index_address() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(page_store.stats().writes, 1);

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);

        let second = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            second.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().memory_hits, 1);

        cache.clear_memory_for_test();
        let third = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            third.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().disk_hits, 1);
    }

    #[test]
    fn three_layer_cache_reads_memory_then_block_cache_then_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().puts, 2);
        assert!(cache.stats().memory_bytes > 0);
        assert!(cache.stats().disk_bytes > 0);

        let memory = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            memory.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert!(cache.stats().memory_hits >= 1);
        assert_eq!(page_store.stats().reads, 1);

        cache.clear_memory_for_test();
        let block_cache = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            block_cache.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(cache.stats().disk_hits, 1);
        assert_eq!(page_store.stats().reads, 1);

        cache.invalidate_shard(1).unwrap();
        let local_file = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            local_file.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 2);
        assert!(cache.stats().puts >= 4);
        assert!(cache.stats().memory_bytes > 0);
        assert!(cache.stats().disk_bytes > 0);

        let observation = engine.rust_storage_observation(1).unwrap();
        assert!(observation.observed_memory_hit);
        assert!(observation.observed_block_cache_hit);
        assert!(observation.observed_local_file_read);
        assert!(observation.observed_cache_invalidation);
        assert!(observation.cache_memory_bytes > 0);
        assert!(observation.cache_disk_bytes > 0);
        assert!(observation.local_page_bytes_written > 0);
        assert!(observation.local_page_bytes_read > 0);
    }

    #[test]
    fn tiny_memory_cache_eviction_refills_from_persistence_then_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);

        let target_value = b"target-value-0123456789".to_vec();
        for (key, value) in [
            ("target", target_value.clone()),
            ("evict-a", b"eviction-value-a-0123456789".to_vec()),
            ("evict-b", b"eviction-value-b-0123456789".to_vec()),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        let first_read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            first_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(page_store.stats().reads, 1);

        for key in ["evict-a", "evict-b"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert!(
                response.status.ok,
                "eviction pressure read should pass: {response:?}"
            );
        }
        assert!(
            cache.stats().memory_evictions > 0,
            "reading multiple persisted blocks through a tiny memory cache should evict older blocks"
        );
        assert!(
            cache.stats().disk_bytes > 0,
            "persistent page read should populate block-cache files"
        );

        let target_page_key = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .expect("shard should exist")
                .strings
                .get("target")
                .expect("target address should exist");
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };
        assert_eq!(
            cache.get_memory(&target_page_key),
            None,
            "target page block should have been evicted from memory"
        );

        let disk_hits_before = cache.stats().disk_hits;
        let file_reads_before_block_hit = page_store.stats().reads;
        let second_read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            second_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            page_store.stats().reads,
            file_reads_before_block_hit,
            "memory miss should hit disk block cache instead of rereading page store"
        );
        assert!(
            cache.stats().disk_hits > disk_hits_before,
            "block cache should serve the read and promote it to memory"
        );
        assert_eq!(
            cache.get_memory(&target_page_key),
            Some(target_value),
            "disk block hit should promote the page block into memory"
        );
    }

    #[test]
    fn restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let original =
            TemporalEngine::with_local_dirs(32, dir.path().join("cache-a"), &page_dir, &index_dir);
        original.load_shard(1);
        let target_value = b"restart-target-value-0123456789".to_vec();
        let write = original.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "target".to_string(),
                value: target_value.clone(),
            },
        });
        assert!(write.status.ok, "{write:?}");
        assert_eq!(original.page_store().stats().writes, 1);

        let restarted =
            TemporalEngine::with_local_dirs(32, dir.path().join("cache-b"), &page_dir, &index_dir);
        restarted.load_shard(1);
        let restarted_cache = restarted.cache();
        let restarted_page_store = restarted.page_store();
        let target_page_key = {
            let shards = restarted.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .expect("shard should exist after index replay")
                .strings
                .get("target")
                .expect("target address should be restored from index")
                .clone();
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };

        let first_read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            first_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            restarted_page_store.stats().reads,
            1,
            "restart should miss memory and load the persisted page once"
        );
        assert_eq!(
            restarted_cache.get_memory(&target_page_key),
            Some(target_value.clone()),
            "persistent page read should refill the memory cache"
        );
        assert!(
            restarted_cache.stats().disk_bytes > 0,
            "persistent page read should also write the disk block cache"
        );

        restarted_cache.clear_memory_for_test();
        assert_eq!(restarted_cache.get_memory(&target_page_key), None);
        let disk_hits_before = restarted_cache.stats().disk_hits;
        let page_reads_before = restarted_page_store.stats().reads;
        let second_read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            second_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            restarted_page_store.stats().reads,
            page_reads_before,
            "memory miss after restart should use the disk block cache"
        );
        assert!(
            restarted_cache.stats().disk_hits > disk_hits_before,
            "disk block cache should serve the second read"
        );
        assert_eq!(
            restarted_cache.get_memory(&target_page_key),
            Some(target_value),
            "disk block hit should promote the page block back into memory"
        );
    }

    #[test]
    fn page_reads_fill_compressed_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::with_block_options(
            1024 * 1024,
            dir.path().join("cache"),
            crate::cache::CacheBlockOptions {
                compression: crate::cache::CacheCompression::Zstd { level: 1 },
                min_compress_bytes: 16,
            },
        );
        let engine = TemporalEngine::with_cache_page_store_and_index_dir(
            cache.clone(),
            LocalPageStore::new(dir.path().join("pages")),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let value = vec![b'x'; 4096];
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "large".to_string(),
                value: value.clone(),
            },
        });

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes { value: Some(value) }
        );
        assert!(cache.stats().compressed_puts >= 1);
        assert!(cache.stats().compression_bytes_saved > 0);

        cache.clear_memory_for_test();
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large".to_string(),
            },
        });
        assert!(cache.stats().compressed_hits >= 1);
    }

    #[test]
    fn local_dirs_constructor_applies_page_store_compression_options() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_and_page_store_options(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
            PageStoreOptions {
                compression_enabled: false,
                ..PageStoreOptions::default()
            },
        );
        engine.load_shard(1);
        let value = b"engine-page-policy-".repeat(80);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "large-policy".to_string(),
                value: value.clone(),
            },
        });

        let page_store = engine.page_store();
        let stats = page_store.stats();
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.compressed_records_written, 0);
        assert_eq!(stats.compression_bytes_saved, 0);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large-policy".to_string(),
            },
        });
        assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
    }

    #[test]
    fn write_invalidates_cached_string() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let engine = TemporalEngine::new(cache.clone());
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"old".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"new".to_vec(),
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"new".to_vec())
            }
        );
        assert!(cache.stats().invalidations >= 2);
    }

    #[test]
    fn async_storage_string_write_stays_on_hot_memory_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    },
                })
                .ok
        );

        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "hot".to_string(),
                value: b"value".to_vec(),
            },
        });
        assert!(write.status.ok);
        assert_eq!(engine.page_store().stats().writes, 0);
        assert_eq!(engine.oplog_store().stats(1).writes, 0);
        assert_eq!(engine.index_log_store().stats(1).writes, 0);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "hot".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"value".to_vec())
            }
        );
        assert_eq!(engine.page_store().stats().reads, 0);
        assert!(engine.cache().stats().memory_hits >= 1);
    }

    #[test]
    fn durable_execute_overrides_async_storage_for_raft_local_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    },
                })
                .ok
        );

        let write = engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "raft".to_string(),
                value: b"value".to_vec(),
            },
        });
        assert!(write.status.ok);
        assert_eq!(engine.page_store().stats().writes, 1);
        assert_eq!(engine.oplog_store().stats(1).writes, 1);
        assert_eq!(engine.index_log_store().stats(1).writes, 1);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "raft".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"value".to_vec())
            }
        );
    }

    #[test]
    fn durable_index_survives_restart_and_points_to_page_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache-a");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"persisted".to_vec(),
            },
        });

        let restarted = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache-b"),
            &page_dir,
            &index_dir,
        );
        restarted.load_shard(1);
        let response = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"persisted".to_vec())
            }
        );
        assert_eq!(restarted.page_store().stats().reads, 1);
    }

    #[test]
    fn set_members_round_trip() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "group".to_string(),
                member: b"alice".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "group".to_string(),
                member: b"bob".to_vec(),
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetMembers {
                key: "group".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Members {
                members: vec![b"alice".to_vec(), b"bob".to_vec()]
            }
        );
    }

    #[test]
    fn hash_multi_get_set_and_incrby_match_extension_api() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashMultiSet {
                        key: "h".to_string(),
                        entries: vec![
                            ("f1".to_string(), b"v1".to_vec()),
                            ("f2".to_string(), b"7".to_vec()),
                        ],
                    },
                })
                .response,
            CommandResponse::Empty
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashMultiGet {
                        key: "h".to_string(),
                        fields: vec!["f1".to_string(), "missing".to_string(), "f2".to_string()],
                    },
                })
                .response,
            CommandResponse::Values {
                values: vec![Some(b"v1".to_vec()), None, Some(b"7".to_vec())]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashIncrBy {
                        key: "h".to_string(),
                        field: "f2".to_string(),
                        increment: 5,
                    },
                })
                .response,
            CommandResponse::Integer { value: 12 }
        );
    }

    #[test]
    fn hash_incrby_rejects_non_integer_and_overflow_like_cpp() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashMultiSet {
                key: "h".to_string(),
                entries: vec![
                    ("alpha".to_string(), b"abc".to_vec()),
                    ("mixed".to_string(), b"123abc".to_vec()),
                    ("max".to_string(), i64::MAX.to_string().into_bytes()),
                    ("min".to_string(), i64::MIN.to_string().into_bytes()),
                ],
            },
        });

        for field in ["alpha", "mixed"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashIncrBy {
                    key: "h".to_string(),
                    field: field.to_string(),
                    increment: 1,
                },
            });
            assert_eq!(response.status.code, "unmatched");
            assert_eq!(response.response, CommandResponse::Empty);
        }

        let overflow = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: "max".to_string(),
                increment: 1,
            },
        });
        assert_eq!(overflow.status.code, "out_of_range");
        let underflow = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: "min".to_string(),
                increment: -1,
            },
        });
        assert_eq!(underflow.status.code, "out_of_range");
    }

    #[test]
    fn feature_query_respects_count_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "f".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 1,
                        value: b"a".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"b".to_vec(),
                    },
                ],
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "f".to_string(),
                start_ms: 0,
                end_ms: 10,
                count: Some(1),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"a".to_vec()
                }]
            }
        );
    }

    #[test]
    fn feature_append_packs_many_timestamp_values_into_one_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let first = SequenceFeatureRow {
            timestamp_ms: 10,
            gid: 1,
            action_type: 2,
            duration: 3,
            author_id: 4,
        };
        let second = SequenceFeatureRow {
            timestamp_ms: 20,
            gid: 5,
            action_type: 6,
            duration: 7,
            author_id: 8,
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "packed-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: second.timestamp_ms,
                        value: second.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: first.timestamp_ms,
                        value: first.encode_cpp_feature_value(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let (first_address, second_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("packed-feature"))
                .expect("feature series should exist");
            (
                series.get(&10).expect("first point").clone(),
                series.get(&20).expect("second point").clone(),
            )
        };
        assert_eq!(first_address, second_address);
        assert_eq!(
            first_address.object_id,
            Some(stable_page_object_id(1, "feature", "packed-feature", None))
        );
        let packed_bytes = engine.page_store().read(&first_address).unwrap();
        let packed_points = decode_feature_page(&packed_bytes).expect("packed feature page");
        assert_eq!(packed_points.len(), 2);
        assert_eq!(packed_points[0].timestamp_ms, 10);
        assert_eq!(packed_points[1].timestamp_ms, 20);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: first.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: second.encode_cpp_feature_value(),
                    },
                ]
            }
        );

        let filtered = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
                filters: vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 5,
                }],
            },
        });
        assert_eq!(
            filtered.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 20,
                    value: second.encode_cpp_feature_value(),
                }]
            }
        );

        let agg = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAggQuery {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                aggregator: "count".to_string(),
                count: None,
            },
        });
        assert_eq!(agg.response, CommandResponse::Aggregate { value: 2 });
    }

    #[test]
    fn feature_append_chunks_and_persists_timestamped_kv_pages() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let points = (0..10)
            .map(|offset| FeaturePoint {
                timestamp_ms: 1_000 + offset,
                value: vec![b'a' + offset as u8; 10 * 1024],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "chunked-feature".to_string(),
                points: points.clone(),
            },
        });
        assert!(response.status.ok);

        let addresses = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("chunked-feature"))
                .expect("feature series should exist");
            unique_timestamped_kv_page_addresses(series)
        };
        assert!(
            addresses.len() > 1,
            "large timestamped KV batch should be split into page chunks"
        );
        let mut persisted_timestamps = Vec::new();
        for address in &addresses {
            assert_eq!(
                address.object_id,
                Some(stable_page_object_id(1, "feature", "chunked-feature", None))
            );
            let bytes = engine.page_store().read(address).unwrap();
            let chunk = decode_feature_page(&bytes).expect("persisted packed page chunk");
            assert!(!chunk.is_empty());
            assert!(bytes.len() <= TIMESTAMPED_KV_PAGE_TARGET_BYTES + 12 * 1024);
            persisted_timestamps.extend(chunk.into_iter().map(|point| point.timestamp_ms));
        }
        persisted_timestamps.sort_unstable();
        assert_eq!(
            persisted_timestamps,
            points
                .iter()
                .map(|point| point.timestamp_ms)
                .collect::<Vec<_>>()
        );

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "chunked-feature".to_string(),
                start_ms: 0,
                end_ms: 2_000,
                count: None,
            },
        });
        assert_eq!(query.response, CommandResponse::FeaturePoints { points });
    }

    #[test]
    fn feature_append_keeps_oversized_single_timestamped_value_readable() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let points = vec![FeaturePoint {
            timestamp_ms: 1_000,
            value: vec![b'x'; TIMESTAMPED_KV_PAGE_TARGET_BYTES + 8 * 1024],
        }];
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "oversized-single-feature".to_string(),
                points: points.clone(),
            },
        });
        assert!(response.status.ok);

        let addresses = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("oversized-single-feature"))
                .expect("feature series should exist");
            unique_timestamped_kv_page_addresses(series)
        };
        assert_eq!(addresses.len(), 1);
        let bytes = engine.page_store().read(&addresses[0]).unwrap();
        assert!(bytes.len() > TIMESTAMPED_KV_PAGE_TARGET_BYTES);
        assert_eq!(decode_feature_page(&bytes).unwrap(), points);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "oversized-single-feature".to_string(),
                start_ms: 0,
                end_ms: 2_000,
                count: None,
            },
        });
        assert_eq!(query.response, CommandResponse::FeaturePoints { points });
        assert!(
            engine
                .storage_production_readiness_report(1)
                .production_ready
        );
    }

    #[test]
    fn sequence_add_packs_many_timestamp_values_into_one_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let first = SequenceFeatureRow {
            timestamp_ms: 10,
            gid: 101,
            action_type: 2,
            duration: 30,
            author_id: 400,
        };
        let second = SequenceFeatureRow {
            timestamp_ms: 20,
            gid: 102,
            action_type: 3,
            duration: 40,
            author_id: 500,
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "packed-sequence".to_string(),
                rows: vec![second.clone(), first.clone()],
            },
        });
        assert!(response.status.ok);

        let (first_address, second_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.sequences.get("packed-sequence"))
                .expect("sequence series should exist");
            (
                series.get(&10).expect("first row").clone(),
                series.get(&20).expect("second row").clone(),
            )
        };
        assert_eq!(first_address, second_address);
        assert_eq!(
            first_address.object_id,
            Some(stable_page_object_id(
                1,
                "sequence",
                "packed-sequence",
                None
            ))
        );

        let packed_bytes = engine.page_store().read(&first_address).unwrap();
        let packed_points = decode_feature_page(&packed_bytes).expect("packed sequence page");
        assert_eq!(packed_points.len(), 2);
        assert_eq!(packed_points[0].timestamp_ms, 10);
        assert_eq!(packed_points[1].timestamp_ms, 20);
        assert_eq!(
            serde_json::from_slice::<SequenceFeatureRow>(&packed_points[0].value).unwrap(),
            first
        );
        assert_eq!(
            serde_json::from_slice::<SequenceFeatureRow>(&packed_points[1].value).unwrap(),
            second
        );

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: "packed-sequence".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: 10,
                filters: Vec::new(),
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::SequenceRows {
                rows: vec![first.clone(), second.clone()]
            }
        );

        let filtered = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: "packed-sequence".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: 10,
                filters: vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 102,
                }],
            },
        });
        assert_eq!(
            filtered.response,
            CommandResponse::SequenceRows { rows: vec![second] }
        );

        let shards = engine.shards.read().expect("engine lock poisoned");
        let validation = engine.validate_shard_page_ownership(1, shards.get(&1).unwrap());
        assert!(validation.mismatches.is_empty());
        assert_eq!(validation.missing_owner_page_refs, 0);
    }

    #[test]
    fn feature_recovery_validates_packed_page_layout() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let report = engine.storage_recovery_report(1);
        assert_eq!(report.feature_page_layout.indexed_feature_points, 2);
        assert_eq!(report.feature_page_layout.unique_feature_page_refs, 1);
        assert_eq!(report.feature_page_layout.packed_feature_pages, 1);
        assert_eq!(report.feature_page_layout.legacy_feature_value_pages, 0);
        assert!(report
            .feature_page_layout
            .corrupt_packed_feature_pages
            .is_empty());
        assert!(report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty());
    }

    #[test]
    fn feature_recovery_reports_index_timestamp_missing_from_packed_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let series = shards
                .get_mut(&1)
                .and_then(|shard| shard.features.get_mut("layout-feature"))
                .expect("feature series should exist");
            let address = series.get(&10).expect("packed page").clone();
            series.insert(30, address);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .missing_indexed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![30]
        );
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_packed_timestamp_orphaned_from_index() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let series = shards
                .get_mut(&1)
                .and_then(|shard| shard.features.get_mut("layout-feature"))
                .expect("feature series should exist");
            series.remove(&20);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .orphan_packed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![20]
        );
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_duplicate_timestamps_inside_packed_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let duplicate_page = encode_feature_page(&[
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten-duplicate".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 20,
                value: b"twenty".to_vec(),
            },
        ]);
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &duplicate_page,
                Some(stable_page_object_id(1, "feature", "layout-feature", None)),
                Some(page_routing_slot("layout-feature", 0, u32::MAX)),
            )
            .expect("duplicate packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let series = shard
                .features
                .entry("layout-feature".to_string())
                .or_default();
            series.insert(10, address.clone());
            series.insert(20, address);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .duplicate_packed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert!(report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty());
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_corrupt_packed_timestamped_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut corrupt_page = FEATURE_PAGE_MAGIC.to_vec();
        corrupt_page.extend_from_slice(br#"{"version":1,"points":"not-a-point-list"}"#);
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &corrupt_page,
                Some(stable_page_object_id(1, "feature", "corrupt-feature", None)),
                Some(page_routing_slot("corrupt-feature", 0, u32::MAX)),
            )
            .expect("corrupt packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            shard
                .features
                .entry("corrupt-feature".to_string())
                .or_default()
                .insert(10, address);
        }

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "corrupt-feature".to_string(),
                start_ms: 0,
                end_ms: 20,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints { points: vec![] }
        );

        let readiness = engine.storage_production_readiness_report(1);
        assert!(!readiness.production_ready);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.corrupt_feature_page_count, 1);
        assert!(
            readiness.feature_page_layout.corrupt_packed_feature_pages[0]
                .error
                .contains("invalid packed feature page payload")
        );
    }

    #[test]
    fn feature_recovery_reports_unsupported_packed_timestamped_page_version() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let page = PackedFeaturePage {
            version: 2,
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            }],
        };
        let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(&page).unwrap());
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &bytes,
                Some(stable_page_object_id(
                    1,
                    "feature",
                    "versioned-feature",
                    None,
                )),
                Some(page_routing_slot("versioned-feature", 0, u32::MAX)),
            )
            .expect("unsupported packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            shard
                .features
                .entry("versioned-feature".to_string())
                .or_default()
                .insert(10, address);
        }

        let readiness = engine.storage_production_readiness_report(1);
        assert!(!readiness.production_ready);
        assert_eq!(readiness.corrupt_feature_page_count, 1);
        assert!(
            readiness.feature_page_layout.corrupt_packed_feature_pages[0]
                .error
                .contains("unsupported packed feature page version 2")
        );
    }

    #[test]
    fn feature_compaction_rewrites_shared_packed_page_once() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "compact-packed-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let before = engine.storage_recovery_report(1);
        assert_eq!(before.total_page_refs, 1);
        let report = engine.compact_shard_pages(1).unwrap();
        assert_eq!(report.rewritten_page_refs, 1);
        assert_eq!(report.after.live_page_refs, 1);

        let (first_address, second_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("compact-packed-feature"))
                .expect("feature series should exist");
            (
                series.get(&10).expect("first point").clone(),
                series.get(&20).expect("second point").clone(),
            )
        };
        assert_eq!(first_address, second_address);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "compact-packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ]
            }
        );
        let after = engine.storage_recovery_report(1);
        assert_eq!(after.total_page_refs, 1);
        assert_eq!(after.object_lifecycle.live_page_refs, 1);
        assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
    }

    #[test]
    fn feature_append_rejects_cpp_hard_size_limit_before_mutation() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "huge-feature".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"kept".to_vec(),
                }],
            },
        });

        let oversized_points = (0..FEATURE_ADD_HARD_MAX_SIZE)
            .map(|offset| FeaturePoint {
                timestamp_ms: 10 + offset as u64,
                value: b"x".to_vec(),
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "huge-feature".to_string(),
                points: oversized_points,
            },
        });
        assert_eq!(response.status.ok, false);
        assert_eq!(response.status.code, "invalid_argument");
        assert!(response
            .status
            .message
            .contains("huge-feature size bigger than 100000"));

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "huge-feature".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"kept".to_vec(),
                }]
            }
        );
    }

    #[test]
    fn feature_query_filtered_matches_cpp_protobuf_feature_point() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let matching = SequenceFeatureRow {
            timestamp_ms: 777,
            gid: 1,
            action_type: 2,
            duration: 3,
            author_id: 1,
        };
        let other = SequenceFeatureRow {
            timestamp_ms: 778,
            gid: 2,
            action_type: 2,
            duration: 5,
            author_id: 9,
        };
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "9".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: matching.timestamp_ms,
                        value: matching.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: other.timestamp_ms,
                        value: other.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: 779,
                        value: b"not-protobuf".to_vec(),
                    },
                ],
            },
        });

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters: vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
            },
        });

        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);
        assert_eq!(
            SequenceFeatureRow::decode_cpp_feature_value(points[0].timestamp_ms, &points[0].value),
            Some(matching)
        );

        let filters = parse_cpp_feature_filters(["gid = 1", "duration < 4"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);

        let filters = parse_cpp_feature_filters(["gid >= 1", "duration <= 3"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);

        let filters = parse_cpp_feature_filters(["gid = 1", "gid != 1"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 778);

        assert!(FeatureFilter::parse_cpp_filter("unknown = 1").is_err());
        assert!(FeatureFilter::parse_cpp_filter("gid = nope").is_err());
    }

    #[test]
    fn cpp_feature_sequence_golden_corpus_passes() {
        let report = cpp_feature_sequence_golden_corpus_report();
        assert_eq!(report.corpus, "feature_sequence_cpp_proto_v1");
        assert_eq!(report.total_cases, 8);
        assert_eq!(report.passed_cases, report.total_cases);
        assert_eq!(report.failed_cases, 0);
        assert!(report.passed(), "{report:#?}");
    }

    #[test]
    fn feature_replace_delete_and_agg_query() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "f".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"2".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"3".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"4".to_vec(),
                    },
                ],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "sum".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 9 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureReplace {
                key: "f".to_string(),
                start_ms: 0,
                end_ms: 20,
                points: vec![FeaturePoint {
                    timestamp_ms: 15,
                    value: b"10".to_vec(),
                }],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "sum".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 14 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureDelete {
                key: "f".to_string(),
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "count".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 0 }
        );
    }

    #[test]
    fn common_delete_removes_all_data_types_for_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "k".to_string(),
                member: b"m".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetMembers {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Members {
                members: Vec::new()
            }
        );
    }

    #[test]
    fn common_delete_removes_cpp_risk_family_records_for_logical_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (family, amount) in [
            (RiskFamily::H, 5),
            (RiskFamily::Cpc, 7),
            (RiskFamily::Fol, 11),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskSet {
                    family,
                    key: "risk-cpp".to_string(),
                    timestamp_ms: 10,
                    amount,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExists {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::Empty
        );
        for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFamilyQuery {
                            family,
                            key: "risk-cpp".to_string(),
                            start_ms: 0,
                            end_ms: 20,
                            aggregator: "sum".to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: 0 }
            );
        }
    }

    #[test]
    fn common_expire_and_ttl_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "k".to_string(),
                ttl_ms: 0,
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Integer { value: -2 }
        );
    }

    #[test]
    fn common_expire_missing_key_matches_cpp_not_found() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "missing".to_string(),
                ttl_ms: 1000,
            },
        });
        assert_eq!(response.status.code, "not_found");
    }

    #[test]
    fn common_expire_and_ttl_cover_cpp_risk_family_records_for_logical_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskSet {
                family: RiskFamily::Cpc,
                key: "risk-expire".to_string(),
                timestamp_ms: 10,
                amount: 3,
            },
        });
        assert!(response.status.ok, "{response:?}");

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-expire".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: -1 }
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire {
                        key: "risk-expire".to_string(),
                        ttl_ms: 0,
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-expire".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: -2 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::Cpc,
                        key: "risk-expire".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
    }

    #[test]
    fn sequence_query_filters_typed_rows() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "seq".to_string(),
                rows: vec![
                    SequenceFeatureRow {
                        timestamp_ms: 1,
                        gid: 10,
                        action_type: 1,
                        duration: 30,
                        author_id: 7,
                    },
                    SequenceFeatureRow {
                        timestamp_ms: 2,
                        gid: 11,
                        action_type: 3,
                        duration: 120,
                        author_id: 8,
                    },
                ],
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: "seq".to_string(),
                start_ms: 0,
                end_ms: 10,
                count: 10,
                filters: vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::SequenceRows {
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 2,
                    gid: 11,
                    action_type: 3,
                    duration: 120,
                    author_id: 8,
                }]
            }
        );
    }

    #[test]
    fn long_sequence_query_keeps_timestamp_order_and_applies_random_filters() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let base_ts = 1_700_000_000_000_u64;
        let row_count = 5_000_u64;
        let key = "long-sequence".to_string();

        let ordered_rows = (0..row_count)
            .map(|offset| SequenceFeatureRow {
                timestamp_ms: base_ts + offset,
                gid: 10_000 + offset,
                action_type: (offset % 7) as u32,
                duration: (50 + (offset * 37) % 1_000) as u32,
                author_id: 500 + (offset * 17) % 97,
            })
            .collect::<Vec<_>>();
        let shuffled_rows = (0..row_count)
            .map(|i| ordered_rows[((i * 2_919) % row_count) as usize].clone())
            .collect::<Vec<_>>();

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: key.clone(),
                rows: shuffled_rows,
            },
        });

        for seed in 0..20_u64 {
            let start_offset = (seed * 313) % 4_400;
            let end_offset = (start_offset + 250 + (seed * 97) % 700).min(row_count - 1);
            let count = 25 + (seed as usize % 40);
            let filters = vec![
                FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::NotEqual,
                    value: seed % 7,
                },
                FeatureFilter {
                    field: "duration".to_string(),
                    op: FeatureFilterOp::GreaterOrEqual,
                    value: 100 + (seed * 29) % 500,
                },
                FeatureFilter {
                    field: "author_id".to_string(),
                    op: FeatureFilterOp::LessOrEqual,
                    value: 560 + (seed * 11) % 30,
                },
            ];

            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: key.clone(),
                    start_ms: base_ts + start_offset,
                    end_ms: base_ts + end_offset,
                    count,
                    filters: filters.clone(),
                },
            });
            let CommandResponse::SequenceRows { rows } = response.response else {
                panic!("expected sequence rows");
            };
            let expected = ordered_rows
                .iter()
                .filter(|row| row.timestamp_ms >= base_ts + start_offset)
                .filter(|row| row.timestamp_ms <= base_ts + end_offset)
                .take(count)
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(rows, expected, "seed {seed}");
            assert!(rows
                .windows(2)
                .all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
            assert!(rows.len() <= count);
        }
    }

    #[test]
    fn ips_query_last_returns_recent_instances() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, value) in [(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: "ips".to_string(),
                    timestamp_ms,
                    instance: value,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryLast {
                        key: "ips".to_string(),
                        count: 2,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 3,
                        value: b"c".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"b".to_vec(),
                    }
                ]
            }
        );
    }

    #[test]
    fn risk_count_sums_window() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 1), (20, 2), (30, 4)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskCount {
                        key: "risk".to_string(),
                        start_ms: 15,
                        end_ms: 30,
                    },
                })
                .response,
            CommandResponse::Integer { value: 6 }
        );
    }

    #[test]
    fn control_api_load_config_info_stats_membership_and_unload() {
        let engine = TemporalEngine::default();
        assert_eq!(
            engine.set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 123,
                    ..Config::default()
                },
            }),
            Status::error("shard_not_found", "shard is not loaded")
        );
        assert_eq!(engine.get_config(7).status.code, "shard_not_found");
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 7,
                    load_version: 42,
                    local_node_id: Some(2),
                    shard_uri: "file:///tmp/shard-7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );
        let duplicate_load = engine.load_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 43,
            local_node_id: Some(2),
            shard_uri: "file:///tmp/shard-7-duplicate".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 20,
            readonly: false,
            table_name: "table".to_string(),
        });
        assert!(!duplicate_load.status.ok);
        assert_eq!(duplicate_load.status.code, "already_exists");
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 7,
                    config: Config {
                        version: 2,
                        feature_max_size: 123,
                        maxmemory_bytes: Some(3000),
                        extend_config: BTreeMap::from([(
                            "test_config".to_string(),
                            "test_value".to_string(),
                        )]),
                        ..Config::default()
                    },
                })
                .ok
        );
        let config = engine.get_config(7).config;
        assert_eq!(config.feature_max_size, 123);
        assert_eq!(config.maxmemory_bytes, Some(3000));
        assert_eq!(
            config.extend_config.get("test_config"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            engine.set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 1,
                    feature_max_size: 456,
                    ..Config::default()
                },
            }),
            Status::error("failed_precondition", "legacy config version")
        );
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 7,
                    config: Config {
                        version: 2,
                        feature_max_size: 456,
                        ..Config::default()
                    },
                })
                .ok
        );
        assert_eq!(engine.get_config(7).config.feature_max_size, 123);
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 3,
                    replica_membership_version: 4,
                    replica_node_ids: vec![1, 2, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.load_version, 42);
        assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
        assert_eq!(info.membership_version, 3);
        assert_eq!(info.replica_membership_version, 4);
        assert!(info.membership_valid);
        assert_eq!(
            engine.update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 2,
                replica_membership_version: 5,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            }),
            Status::error("failed_precondition", "legacy membership info")
        );
        assert_eq!(
            engine.update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 3,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            }),
            Status::error("failed_precondition", "legacy membership unit info")
        );
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 4,
                    replica_membership_version: 5,
                    replica_node_ids: vec![1, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.replica_node_ids, vec![1, 3]);
        assert!(!info.membership_valid);

        engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let stats = engine.get_stats(7).stats.unwrap();
        assert_eq!(stats.string_records, 1);
        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.load_version, 42);
        assert!(!stats.readonly);
        assert!(stats.storage_bytes > 0);
        assert_eq!(stats.page_store.writes, 1);

        assert!(
            engine
                .unload_shard_with(UnloadShardRequest { shard_id: 7 })
                .status
                .ok
        );
        let after_unload = engine.get_info(7);
        assert!(!after_unload.status.ok);
        assert_eq!(after_unload.status.code, "shard_not_found");
        assert_eq!(engine.get_config(7).status.code, "shard_not_found");
        let second_unload = engine.unload_shard_with(UnloadShardRequest { shard_id: 7 });
        assert!(!second_unload.status.ok);
        assert_eq!(second_unload.status.code, "shard_not_found");
    }

    #[test]
    fn engine_reload_shard_updates_metadata_and_rejects_stale_version() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 7,
                    load_version: 42,
                    local_node_id: Some(2),
                    shard_uri: "file:///tmp/shard-7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "old_table".to_string(),
                })
                .status
                .ok
        );
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 3,
                    replica_membership_version: 4,
                    replica_node_ids: vec![1, 2, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );

        let stale = engine.reload_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 41,
            local_node_id: Some(9),
            shard_uri: "file:///tmp/stale".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 200,
            readonly: true,
            table_name: "stale_table".to_string(),
        });
        assert!(!stale.status.ok);
        assert_eq!(stale.status.code, "stale_load_version");
        let unchanged = engine.get_info(7).info.unwrap();
        assert_eq!(unchanged.load_version, 42);
        assert_eq!(unchanged.table_name, "old_table");
        assert!(!unchanged.readonly);

        let reload = engine.reload_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 43,
            local_node_id: Some(9),
            shard_uri: "file:///tmp/shard-7-reloaded".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 200,
            readonly: true,
            table_name: "new_table".to_string(),
        });
        assert!(reload.status.ok, "{reload:?}");
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.load_version, 43);
        assert_eq!(info.local_node_id, Some(9));
        assert_eq!(info.table_name, "new_table");
        assert_eq!(info.start_routing_slot, 100);
        assert_eq!(info.end_routing_slot, 200);
        assert!(info.readonly);
        assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
        assert_eq!(info.membership_version, 3);
        assert_eq!(info.replica_membership_version, 4);
        assert!(info.membership_valid);

        let write = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(write.status.code, "readonly_shard");
    }

    #[test]
    fn control_api_reads_page_and_index_streams() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"stream-value".to_vec(),
            },
        });

        let page = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            offset: 0,
            size: 12,
        });
        assert_eq!(page.data, b"stream-value".to_vec());

        let index = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Index,
            page_segment_id: 0,
            offset: 0,
            size: 32,
        });
        assert!(index.status.ok);
        assert!(!index.data.is_empty());

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 12,
            max_bytes: 12,
        });
        assert_eq!(scan.records.len(), 1);
        assert_eq!(scan.records[0].data, b"stream-value".to_vec());

        let invalid = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            start_offset: 12,
            end_offset: 1,
            max_bytes: 12,
        });
        assert_eq!(invalid.status.code, "invalid_stream_range");
        assert!(invalid.records.is_empty());
    }

    #[test]
    fn control_api_reads_and_scans_oplog_stream() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
        });

        let stream = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Oplog,
            page_segment_id: 0,
            offset: 0,
            size: 4096,
        });
        assert!(stream.status.ok);
        let text = String::from_utf8(stream.data).unwrap();
        assert!(text.contains("\"sequence\":1"));
        assert!(text.contains("\"sequence\":2"));

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Oplog,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 4096,
            max_bytes: 4096,
        });
        assert_eq!(scan.records.len(), 2);
        assert_eq!(engine.get_stats(1).stats.unwrap().oplog.last_sequence, 2);
    }

    #[test]
    fn control_api_reads_and_scans_index_log_stream() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"hv".to_vec(),
            },
        });

        let stream = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::IndexLog,
            page_segment_id: 0,
            offset: 0,
            size: 8192,
        });
        assert!(stream.status.ok);
        let text = String::from_utf8(stream.data).unwrap();
        assert!(text.contains("\"sequence\":1"));
        assert!(text.contains("\"sequence\":2"));
        assert!(text.contains("\"strings\""));
        assert!(text.contains("\"hashes\""));

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::IndexLog,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 8192,
            max_bytes: 8192,
        });
        assert_eq!(scan.records.len(), 2);

        let last_record: crate::index_log::IndexLogRecord =
            serde_json::from_slice(&scan.records[1].data).unwrap();
        assert_eq!(last_record.sequence, 2);
        assert_eq!(
            last_record.index["hashes"]["h"]["f"]["page_segment_id"],
            serde_json::json!(0)
        );
        assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
    }

    #[test]
    fn readonly_shard_rejects_writes_but_allows_reads() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    load_version: 1,
                    local_node_id: None,
                    shard_uri: "file:///tmp/readonly".to_string(),
                    start_routing_slot: 0,
                    end_routing_slot: 99,
                    readonly: true,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );

        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(!write.status.ok);
        assert_eq!(write.status.code, "readonly_shard");

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert!(read.status.ok);
        assert_eq!(read.response, CommandResponse::Bytes { value: None });
    }

    #[test]
    fn checked_execute_rejects_stale_load_version() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    load_version: 7,
                    local_node_id: None,
                    shard_uri: "file:///tmp/versioned".to_string(),
                    start_routing_slot: 0,
                    end_routing_slot: 99,
                    readonly: false,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );

        let stale = engine.execute_checked(CheckedExecuteRequest {
            shard_id: 1,
            load_version: 6,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(stale.status.code, "load_version_mismatch");

        let current = engine.execute_checked(CheckedExecuteRequest {
            shard_id: 1,
            load_version: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(current.status.ok);
    }

    #[test]
    fn loaded_shard_stats_reports_per_shard_load() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.load_shard(2);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::HashSet {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"2".to_vec(),
            },
        });

        let stats = engine.loaded_shard_stats();
        assert_eq!(stats.len(), 2);
        assert!(stats
            .iter()
            .any(|stat| stat.shard_id == 1 && stat.string_records == 1));
        assert!(stats
            .iter()
            .any(|stat| stat.shard_id == 2 && stat.hash_records == 1));
    }

    #[test]
    fn string_set_conditional_supports_nx_xx_and_get() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfNotExists,
                return_old: false,
            },
        });
        assert_eq!(first.response, CommandResponse::Integer { value: 1 });

        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfNotExists,
                return_old: false,
            },
        });
        assert_eq!(rejected.response, CommandResponse::Integer { value: 0 });

        let old = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v3".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfExists,
                return_old: true,
            },
        });
        assert_eq!(
            old.response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v3".to_vec())
            }
        );
    }

    #[test]
    fn ips_remove_delete_and_count_are_supported() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for timestamp_ms in [10, 20, 30] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: "ips".to_string(),
                    timestamp_ms,
                    instance: timestamp_ms.to_string().into_bytes(),
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsCount {
                        key: "ips".to_string(),
                        start_ms: 0,
                        end_ms: 25,
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsRemove {
                        key: "ips".to_string(),
                        timestamp_ms: 20,
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsDelete {
                        key: "ips".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
    }

    #[test]
    fn ips_range_and_batch_queries_match_cpp_style_read_shapes() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (key, timestamp_ms) in [
            ("ips-a", 10),
            ("ips-a", 20),
            ("ips-a", 30),
            ("ips-b", 15),
            ("ips-b", 25),
        ] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: key.to_string(),
                    timestamp_ms,
                    instance: format!("{key}-{timestamp_ms}").into_bytes(),
                },
            });
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryRange {
                        key: "ips-a".to_string(),
                        start_ms: 15,
                        end_ms: 35,
                        count: Some(1),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 20,
                    value: b"ips-a-20".to_vec(),
                }]
            }
        );

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsBatchQueryLast {
                        keys: vec!["ips-a".to_string(), "ips-b".to_string()],
                        count: 1,
                    },
                })
                .response,
            CommandResponse::FeaturePointGroups {
                groups: vec![
                    (
                        "ips-a".to_string(),
                        vec![FeaturePoint {
                            timestamp_ms: 30,
                            value: b"ips-a-30".to_vec(),
                        }],
                    ),
                    (
                        "ips-b".to_string(),
                        vec![FeaturePoint {
                            timestamp_ms: 25,
                            value: b"ips-b-25".to_vec(),
                        }],
                    ),
                ],
            }
        );
    }

    #[test]
    fn ips_load_snapshot_stat_and_filter_match_cpp_style_module_shape() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsLoad {
                        key: "ips-load".to_string(),
                        points: vec![
                            FeaturePoint {
                                timestamp_ms: 10,
                                value: b"loaded-10".to_vec(),
                            },
                            FeaturePoint {
                                timestamp_ms: 20,
                                value: b"loaded-20".to_vec(),
                            },
                        ],
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-load".to_string(),
                timestamp_ms: 30,
                instance: b"opt-30".to_vec(),
                action_type: Some(7),
                table_id: Some(42),
                request_id: Some("req-30".to_string()),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-load".to_string(),
                timestamp_ms: 40,
                instance: b"opt-40".to_vec(),
                action_type: Some(7),
                table_id: Some(43),
                request_id: Some("req-40".to_string()),
            },
        });

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsSnapshot {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 35,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"loaded-10".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"loaded-20".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"opt-30".to_vec(),
                    },
                ]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsFilter {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 100,
                        count: Some(10),
                        action_type: Some(7),
                        table_id: Some(42),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 30,
                    value: b"opt-30".to_vec(),
                }]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsStat {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 100,
                    },
                })
                .response,
            CommandResponse::IpsStats {
                stats: IpsStats {
                    total: 4,
                    first_timestamp_ms: Some(10),
                    last_timestamp_ms: Some(40),
                    action_type_counts: vec![(7, 2)],
                    table_id_counts: vec![(42, 1), (43, 1)],
                }
            }
        );
    }

    #[test]
    fn risk_query_supports_sum_min_max_and_event_count() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        for (aggregator, expected) in [("sum", 10), ("min", -2), ("max", 7), ("events", 3)] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskQuery {
                            key: "risk".to_string(),
                            start_ms: 0,
                            end_ms: 40,
                            aggregator: aggregator.to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: expected }
            );
        }
    }

    #[test]
    fn risk_change_matches_cpp_distinct_field_semantics() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, value) in [(10, "device-a"), (20, "device-a"), (30, "device-b")] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskChangeAdd {
                    key: "risk-change".to_string(),
                    timestamp_ms,
                    value: value.as_bytes().to_vec(),
                    precision_ms: Some(10),
                    ttl_ms: None,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskQuery {
                        key: "risk-change".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "change".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );

        for (timestamp_ms, value) in [(10, "buyer-1"), (20, "buyer-1"), (30, "buyer-2")] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskChangeAdd {
                    key: risk_family_key(RiskFamily::H, "risk-change"),
                    timestamp_ms,
                    value: value.as_bytes().to_vec(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::H,
                        key: "risk-change".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "change".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
    }

    #[test]
    fn risk_query_supports_first_last_and_detail_list() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        for (aggregator, expected) in [("first", 5), ("last", 7)] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskQuery {
                            key: "risk".to_string(),
                            start_ms: 0,
                            end_ms: 40,
                            aggregator: aggregator.to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: expected }
            );
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskDetail {
                        key: "risk".to_string(),
                        start_ms: 15,
                        end_ms: 40,
                        count: Some(2),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"-2".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"7".to_vec(),
                    },
                ]
            }
        );
    }

    #[test]
    fn risk_cpp_family_set_query_setandget_and_manager_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (family, timestamp_ms, amount) in [
            (RiskFamily::H, 10, 5),
            (RiskFamily::H, 20, 7),
            (RiskFamily::Cpc, 10, 3),
            (RiskFamily::Fol, 10, 11),
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskSet {
                            family,
                            key: "risk-cpp".to_string(),
                            timestamp_ms,
                            amount,
                        },
                    })
                    .status
                    .ok
            );
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::H,
                        key: "risk-cpp".to_string(),
                        start_ms: 0,
                        end_ms: 30,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 12 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskSetAndGet {
                        family: RiskFamily::Cpc,
                        key: "risk-cpp".to_string(),
                        timestamp_ms: 20,
                        amount: 4,
                        start_ms: 0,
                        end_ms: 30,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 7 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskManager {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::HashEntries {
                entries: vec![
                    ("h_events".to_string(), b"2".to_vec()),
                    ("h_sum".to_string(), b"12".to_vec()),
                    ("cpc_events".to_string(), b"2".to_vec()),
                    ("cpc_sum".to_string(), b"7".to_vec()),
                    ("fol_events".to_string(), b"1".to_vec()),
                    ("fol_sum".to_string(), b"11".to_vec()),
                ],
            }
        );
    }

    #[test]
    fn risk_fol_matches_cpp_first_last_string_semantics() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        for (occur_time_ms, value) in [(20, "middle"), (10, "first"), (30, "last")] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFolSet {
                            key: "risk-fol-first".to_string(),
                            value: value.as_bytes().to_vec(),
                            occur_time_ms,
                            ttl_ms: 60_000,
                            fol_type: RiskFolType::First,
                        },
                    })
                    .status
                    .ok
            );
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFolSet {
                            key: "risk-fol-last".to_string(),
                            value: value.as_bytes().to_vec(),
                            occur_time_ms,
                            ttl_ms: 60_000,
                            fol_type: RiskFolType::Last,
                        },
                    })
                    .status
                    .ok
            );
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolQuery {
                        key: "risk-fol-first".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"first".to_vec()),
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolQuery {
                        key: "risk-fol-last".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"last".to_vec()),
            }
        );
    }

    #[test]
    fn feature_write_policy_sequence_batch_ips_dimensions_and_risk_precision_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "feature-policy".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 10,
                    value: b"old".to_vec(),
                }],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppendWithPolicy {
                        key: "feature-policy".to_string(),
                        points: vec![FeaturePoint {
                            timestamp_ms: 10,
                            value: b"ignored".to_vec(),
                        }],
                        policy: FeatureWritePolicy::InsertIfAbsent,
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppendWithPolicy {
                        key: "feature-policy".to_string(),
                        points: vec![FeaturePoint {
                            timestamp_ms: 10,
                            value: b"new".to_vec(),
                        }],
                        policy: FeatureWritePolicy::ReplaceExisting,
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureQuery {
                        key: "feature-policy".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 10,
                    value: b"new".to_vec(),
                }]
            }
        );

        for (key, gid, action_type) in [("seq-a", 1, 7), ("seq-b", 2, 8)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: key.to_string(),
                    rows: vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid,
                        action_type,
                        duration: 5,
                        author_id: 9,
                    }],
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SequenceBatchQuery {
                        queries: vec![
                            SequenceQuerySpec {
                                key: "seq-a".to_string(),
                                start_ms: 0,
                                end_ms: 200,
                                count: 10,
                                filters: vec![FeatureFilter {
                                    field: "action_type".to_string(),
                                    op: FeatureFilterOp::Equal,
                                    value: 7,
                                }],
                            },
                            SequenceQuerySpec {
                                key: "seq-b".to_string(),
                                start_ms: 0,
                                end_ms: 200,
                                count: 10,
                                filters: Vec::new(),
                            },
                        ],
                    },
                })
                .response,
            CommandResponse::SequenceRowGroups {
                groups: vec![
                    (
                        "seq-a".to_string(),
                        vec![SequenceFeatureRow {
                            timestamp_ms: 100,
                            gid: 1,
                            action_type: 7,
                            duration: 5,
                            author_id: 9,
                        }],
                    ),
                    (
                        "seq-b".to_string(),
                        vec![SequenceFeatureRow {
                            timestamp_ms: 100,
                            gid: 2,
                            action_type: 8,
                            duration: 5,
                            author_id: 9,
                        }],
                    ),
                ],
            }
        );

        for (timestamp_ms, value, action_type, request_id) in [
            (10, b"a10".to_vec(), Some(1), Some("r1".to_string())),
            (20, b"a20".to_vec(), Some(2), Some("r2".to_string())),
            (30, b"a30".to_vec(), Some(1), Some("r3".to_string())),
        ] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAddWithOptions {
                    key: "ips-dim".to_string(),
                    timestamp_ms,
                    instance: value,
                    action_type,
                    table_id: Some(99),
                    request_id,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsAddWithOptions {
                        key: "ips-dim".to_string(),
                        timestamp_ms: 40,
                        instance: b"dup".to_vec(),
                        action_type: Some(1),
                        table_id: Some(99),
                        request_id: Some("r1".to_string()),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryRangeWithOptions {
                        key: "ips-dim".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        count: None,
                        action_type: Some(1),
                        table_id: Some(99),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"a10".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"a30".to_vec(),
                    },
                ]
            }
        );

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrementWithOptions {
                key: "risk-bucket".to_string(),
                timestamp_ms: 1_234,
                amount: 3,
                precision_ms: Some(1_000),
                ttl_ms: Some(60_000),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrementWithOptions {
                key: "risk-bucket".to_string(),
                timestamp_ms: 1_999,
                amount: 4,
                precision_ms: Some(1_000),
                ttl_ms: None,
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskDetail {
                        key: "risk-bucket".to_string(),
                        start_ms: 0,
                        end_ms: 2_000,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1_000,
                    value: b"7".to_vec(),
                }]
            }
        );
        assert!(matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-bucket".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value } if value > 0
        ));
    }

    #[test]
    fn maxmemory_config_rejects_writes_when_storage_budget_is_exhausted() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                maxmemory_bytes: Some(0),
                ..Config::default()
            },
        });

        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "first".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "storage_quota_exceeded");
    }

    #[test]
    fn write_qps_config_rejects_writes_after_admission_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                write_qps: Some(1),
                ..Config::default()
            },
        });
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "second".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "write_qps limit exceeded");
    }

    #[test]
    fn read_qps_config_rejects_reads_after_admission_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                read_qps: Some(1),
                ..Config::default()
            },
        });
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "first".to_string(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "first".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "read_qps limit exceeded");
    }

    #[test]
    fn table_write_qps_config_is_shared_across_loaded_table_shards() {
        let engine = TemporalEngine::default();
        for shard_id in [1, 2] {
            assert!(
                engine
                    .load_shard_with(LoadShardRequest {
                        shard_id,
                        load_version: 1,
                        local_node_id: Some(1),
                        shard_uri: format!("local://feature_table/{shard_id}"),
                        start_routing_slot: 0,
                        end_routing_slot: u32::MAX,
                        readonly: false,
                        table_name: "feature_table".to_string(),
                    })
                    .status
                    .ok
            );
            engine.set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    table_write_qps: Some(1),
                    ..Config::default()
                },
            });
        }
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::StringSet {
                key: "second".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "table_write_qps limit exceeded");
    }

    #[test]
    fn tenant_read_qps_config_is_shared_across_tables() {
        let engine = TemporalEngine::default();
        for (shard_id, table_name, key) in [(1, "feature_table", "k1"), (2, "risk_table", "k2")] {
            assert!(
                engine
                    .load_shard_with(LoadShardRequest {
                        shard_id,
                        load_version: 1,
                        local_node_id: Some(1),
                        shard_uri: format!("local://{table_name}/{shard_id}"),
                        start_routing_slot: 0,
                        end_routing_slot: u32::MAX,
                        readonly: false,
                        table_name: table_name.to_string(),
                    })
                    .status
                    .ok
            );
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: b"value".to_vec(),
                        },
                    })
                    .status
                    .ok
            );
            engine.set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    tenant_name: Some("tenant-a".to_string()),
                    tenant_read_qps: Some(1),
                    ..Config::default()
                },
            });
        }
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k1".to_string(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::StringGet {
                key: "k2".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "tenant_read_qps limit exceeded");
    }

    #[test]
    fn stats_include_cpp_style_partition_and_object_manager_accounting() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 9,
                    load_version: 77,
                    local_node_id: Some(3),
                    shard_uri: "local://table/shard-9".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "feature_table".to_string(),
                })
                .status
                .ok
        );
        for command in [
            Command::StringSet {
                key: "string-key".to_string(),
                value: b"v".to_vec(),
            },
            Command::HashSet {
                key: "hash-key".to_string(),
                field: "a".to_string(),
                value: b"1".to_vec(),
            },
            Command::HashSet {
                key: "hash-key".to_string(),
                field: "b".to_string(),
                value: b"2".to_vec(),
            },
            Command::SetAdd {
                key: "set-key".to_string(),
                member: b"m1".to_vec(),
            },
            Command::SetAdd {
                key: "set-key".to_string(),
                member: b"m2".to_vec(),
            },
            Command::FeatureAppend {
                key: "feature-key".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 1,
                        value: b"f1".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"f2".to_vec(),
                    },
                ],
            },
            Command::SequenceAdd {
                key: "sequence-key".to_string(),
                rows: vec![
                    SequenceFeatureRow {
                        timestamp_ms: 10,
                        gid: 1,
                        action_type: 2,
                        duration: 3,
                        author_id: 4,
                    },
                    SequenceFeatureRow {
                        timestamp_ms: 20,
                        gid: 5,
                        action_type: 6,
                        duration: 7,
                        author_id: 8,
                    },
                ],
            },
            Command::IpsAdd {
                key: "ips-key".to_string(),
                timestamp_ms: 30,
                instance: b"i".to_vec(),
            },
            Command::RiskSet {
                family: RiskFamily::Cpc,
                key: "risk-key".to_string(),
                timestamp_ms: 40,
                amount: 5,
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 9,
                        command,
                    })
                    .status
                    .ok
            );
        }

        let stats = engine.get_stats(9).stats.unwrap();
        assert_eq!(stats.total_records, 7);
        assert_eq!(stats.object_manager.object_count, 7);
        assert_eq!(stats.object_manager.page_ref_count, 10);
        assert_eq!(stats.object_manager.dirty_object_count, 7);
        assert!(stats.object_manager.dirty_slot_count > 0);
        assert!(stats.object_manager.dirty_slot_count <= 7);
        assert_eq!(stats.object_manager.routing_slot_count, 11);
        assert_eq!(stats.partition_info.table_name, "feature_table");
        assert_eq!(stats.partition_info.shard_uri, "local://table/shard-9");
        assert_eq!(stats.partition_info.start_routing_slot, 10);
        assert_eq!(stats.partition_info.end_routing_slot, 20);
        assert_eq!(stats.partition_info.object_manager, stats.object_manager);
        assert!(stats.page_store_zones.active_zones >= 1);
        assert!(stats.page_store_zones.active_physical_bytes > 0);
        assert_eq!(
            stats.page_store_zones.live_physical_bytes,
            stats.page_store_zones.active_physical_bytes
                + stats.page_store_zones.sealed_physical_bytes
        );
    }

    #[test]
    fn prometheus_metrics_include_records_cache_page_and_oplog() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.page_store().roll_segment().unwrap();

        let metrics = engine.prometheus_metrics();
        assert!(metrics.contains("temporalstore_shard_records{shard_id=\"1\",kind=\"string\"} 1"));
        assert!(metrics.contains("temporalstore_cache_operations_total"));
        assert!(metrics.contains(
            "temporalstore_cache_operations_total{shard_id=\"1\",kind=\"memory_evictions\"}"
        ));
        assert!(metrics.contains("temporalstore_page_store_operations_total"));
        assert!(metrics
            .contains("temporalstore_page_store_zone_count{shard_id=\"1\",state=\"sealed\"} 1"));
        assert!(
            metrics.contains("temporalstore_page_store_zone_bytes{shard_id=\"1\",kind=\"live\"}")
        );
        assert!(metrics
            .contains("temporalstore_page_store_zone_bytes{shard_id=\"1\",kind=\"total_known\"}"));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_unix_ms{shard_id=\"1\",scope=\"known\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_unix_ms{shard_id=\"1\",scope=\"live\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_age_ms{shard_id=\"1\",scope=\"known\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_age_ms{shard_id=\"1\",scope=\"live\"}"
        ));
        assert!(metrics.contains("temporalstore_oplog_records_total{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_objects{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_page_refs{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_dirty_objects{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_storage_slot_page_refs{shard_id=\"1\""));
        assert!(metrics.contains("temporalstore_storage_slot_bytes{shard_id=\"1\""));
        assert!(metrics.contains("temporalstore_storage_slot_dirty_objects{shard_id=\"1\""));
        assert!(
            metrics.contains("temporalstore_partition_routing_slots{shard_id=\"1\"} 4294967295")
        );
    }

    #[test]
    fn slot_storage_summaries_track_live_refs_dirty_slots_and_manifest_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard_with(LoadShardRequest {
            shard_id: 1,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_slot: 10,
            end_routing_slot: 12,
            readonly: false,
            table_name: String::new(),
        });
        for key in ["alpha", "beta", "gamma"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: key.as_bytes().to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let summaries = engine.slot_storage_summaries(1);
        assert!(!summaries.is_empty());
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum::<u64>(),
            3
        );
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.dirty_object_count)
                .sum::<u64>(),
            3
        );
        let dirty_slot = summaries
            .iter()
            .find(|summary| summary.dirty_object_count > 0)
            .unwrap()
            .routing_slot;
        let manifest = engine
            .create_slot_dump_manifest(1, [dirty_slot])
            .expect("slot dump manifest should persist");
        engine.validate_slot_dump_manifest(&manifest).unwrap();
        let summaries = engine.slot_storage_summaries(1);
        assert!(summaries
            .iter()
            .filter(|summary| summary.routing_slot == dirty_slot)
            .all(|summary| summary.last_dump_sequence == manifest.index_log_sequence));
    }

    #[test]
    fn slot_dump_manifest_validation_rejects_checksum_and_missing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let mut manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        manifest.logical_bytes = manifest.logical_bytes.saturating_add(1);
        assert!(
            !engine
                .validate_slot_dump_manifest(&manifest)
                .unwrap_err()
                .ok
        );

        let mut missing = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        missing.page_segment_ids.push(999_999);
        missing.checksum = slot_dump_manifest_checksum(&missing).unwrap();
        let missing_preflight = engine.slot_dump_install_preflight_report(&missing);
        assert!(!missing_preflight.install_safe);
        assert_eq!(missing_preflight.missing_page_segment_ids, vec![999_999]);
        assert!(missing_preflight
            .blockers
            .contains(&"missing_page_segments".to_string()));
        assert!(!engine.validate_slot_dump_manifest(&missing).unwrap_err().ok);

        let mut incomplete = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        incomplete.page_segment_ids.clear();
        incomplete.checksum = slot_dump_manifest_checksum(&incomplete).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&incomplete)
                .unwrap_err()
                .code,
            "slot_dump_page_segment_mismatch"
        );

        let corrupt = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        let segment_id = corrupt.page_segment_ids[0];
        let mut segment = engine.page_store().read_segment(segment_id).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        let _ = engine.page_store().install_segment(segment_id, &segment);
        let corrupt_preflight = engine.slot_dump_install_preflight_report(&corrupt);
        assert!(!corrupt_preflight.install_safe);
        assert!(corrupt_preflight
            .corrupt_page_segment_ids
            .contains(&segment_id));
        assert!(corrupt_preflight.unreadable_page_ref_count > 0);
        assert!(corrupt_preflight.unreadable_page_bytes > 0);
        assert!(corrupt_preflight
            .blockers
            .contains(&"unreadable_page_refs".to_string()));
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&corrupt)
                .unwrap_err()
                .code,
            "slot_dump_unreadable_page_refs"
        );
    }

    #[test]
    fn slot_dump_manifest_install_restores_index_and_rejects_partial_or_stale() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "restore-me".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");

        let restore_engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("restore-cache"),
            dir.path().join("pages"),
            dir.path().join("restore-indexes"),
        );
        restore_engine.load_shard(1);
        let safe_preflight = restore_engine.slot_dump_install_preflight_report(&manifest);
        assert!(safe_preflight.install_safe, "{safe_preflight:?}");
        assert!(safe_preflight.blockers.is_empty());
        assert_eq!(
            safe_preflight.manifest_index_log_sequence,
            manifest.index_log_sequence
        );
        restore_engine
            .install_slot_dump_manifest(&manifest)
            .expect("manifest should install");
        assert!(
            fs::read_dir(dir.path().join("restore-indexes"))
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp-")),
            "slot dump install should not leave atomic index temp files"
        );
        assert!(restore_engine.interrupted_slot_dump_installs(1).is_empty());
        let markers = list_slot_dump_install_markers_at(&restore_engine.index_dir, 1).unwrap();
        assert!(markers.iter().any(|marker| marker.phase == "prepare"));
        assert!(markers.iter().any(|marker| marker.phase == "install"));
        assert!(markers.iter().any(|marker| marker.phase == "commit"));
        let response = restore_engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "restore-me".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );

        let mut partial = manifest.clone();
        partial.index_bytes.clear();
        partial.checksum = slot_dump_manifest_checksum(&partial).unwrap();
        assert_eq!(
            restore_engine
                .install_slot_dump_manifest(&partial)
                .unwrap_err()
                .code,
            "slot_dump_partial_manifest"
        );

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "newer".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let stale_preflight = engine.slot_dump_install_preflight_report(&manifest);
        assert!(!stale_preflight.install_safe);
        assert!(stale_preflight.stale_manifest);
        assert!(stale_preflight
            .blockers
            .contains(&"stale_manifest_sequence".to_string()));
        assert_eq!(
            engine
                .install_slot_dump_manifest(&manifest)
                .unwrap_err()
                .code,
            "slot_dump_stale_manifest"
        );
    }

    #[test]
    fn slot_dump_install_markers_report_interrupted_prepare() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "marker".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: "interrupted".to_string(),
                phase: "prepare".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let interrupted = engine.interrupted_slot_dump_installs(1);
        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].phase, "prepare");
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.interrupted_slot_dump_installs, interrupted);
        assert_eq!(boundary.prepared_slot_dump_install_count, 1);
        assert_eq!(boundary.installed_slot_dump_install_count, 0);
        assert_eq!(boundary.unknown_slot_dump_install_count, 0);
        let readiness = engine.storage_production_readiness_report(1);
        assert_eq!(readiness.interrupted_slot_dump_install_count, 1);
        assert_eq!(readiness.prepared_slot_dump_install_count, 1);
        assert_eq!(readiness.installed_slot_dump_install_count, 0);
        assert_eq!(readiness.unknown_slot_dump_install_count, 0);
    }

    #[test]
    fn slot_dump_install_roll_forward_completes_safe_installed_marker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "roll".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                phase: "install".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let dry_run = engine.slot_dump_install_roll_forward_reports(1);
        assert_eq!(dry_run.len(), 1);
        assert!(dry_run[0].can_roll_forward);
        assert_eq!(dry_run[0].reason, "commit_ready");

        let applied = engine.roll_forward_slot_dump_installs(1);
        assert_eq!(applied.len(), 1);
        assert!(applied[0].completed_commit);
        assert!(applied[0].obsolete_marker_files_removed > 0);
        assert!(engine.interrupted_slot_dump_installs(1).is_empty());
        let marker_files =
            slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
        assert!(marker_files
            .iter()
            .all(|(marker, _)| marker.phase == "commit"));
    }

    #[test]
    fn slot_dump_install_roll_forward_retries_safe_prepare_marker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "retry-prepare".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                phase: "prepare".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let dry_run = engine.slot_dump_install_roll_forward_reports(1);
        assert_eq!(dry_run.len(), 1);
        assert!(dry_run[0].can_retry_install);
        assert!(!dry_run[0].can_roll_forward);
        assert_eq!(dry_run[0].reason, "install_retry_ready");

        let applied = engine.roll_forward_slot_dump_installs(1);
        assert_eq!(applied.len(), 1);
        assert!(applied[0].completed_install);
        assert!(applied[0].completed_commit);
        assert!(applied[0].obsolete_marker_files_removed > 0);
        assert!(engine.interrupted_slot_dump_installs(1).is_empty());
        let marker_files =
            slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
        assert!(marker_files
            .iter()
            .all(|(marker, _)| marker.phase == "commit"));
    }

    #[test]
    fn slot_dump_recovery_reports_broken_manifest_parent_chain() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "chain".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("parent manifest should persist");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "chain".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("child manifest should persist");
        assert_eq!(child.parent_manifest_id, Some(parent.manifest_id.clone()));

        fs::remove_file(slot_dump_manifest_path(
            &engine.index_dir,
            1,
            &parent.manifest_id,
        ))
        .unwrap();
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.manifest_chain_issues.len(), 1);
        assert_eq!(
            boundary.manifest_chain_issues[0].manifest_id,
            child.manifest_id
        );
        assert_eq!(
            boundary.manifest_chain_issues[0].reason,
            "missing_parent_manifest"
        );
    }

    #[test]
    fn slot_dump_manifest_prune_keeps_latest_parent_chain_and_removes_obsolete_fork() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "prune".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "prune".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let mut fork = parent.clone();
        fork.manifest_id = format!("{}-fork", fork.manifest_id);
        fork.parent_manifest_id = None;
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        engine.persist_slot_dump_manifest(&fork).unwrap();
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: 1,
                manifest_id: fork.manifest_id.clone(),
                phase: "commit".to_string(),
                oplog_sequence: fork.oplog_sequence,
                index_log_sequence: fork.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let plan = engine.slot_dump_manifest_prune_plan(1);
        assert!(plan.retained_manifest_ids.contains(&parent.manifest_id));
        assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
        assert_eq!(plan.prunable_manifest_ids, vec![fork.manifest_id.clone()]);
        assert_eq!(
            plan.prunable_marker_manifest_ids,
            vec![fork.manifest_id.clone()]
        );

        let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: true,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        let report = lifecycle
            .manifest_prune_report
            .expect("lifecycle should apply manifest prune");
        assert_eq!(report.removed_manifest_ids, vec![fork.manifest_id.clone()]);
        assert_eq!(report.removed_marker_files, 1);
        assert_eq!(
            lifecycle.manifest_prune_plan.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );
        assert!(slot_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
        assert!(slot_dump_manifest_path(&engine.index_dir, 1, &child.manifest_id).exists());
        assert!(!slot_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
    }

    #[test]
    fn slot_dump_manifest_prune_is_blocked_by_lagging_follower_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "cursor".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "cursor".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let mut fork = parent.clone();
        fork.manifest_id = format!("{}-follower-anchor", fork.manifest_id);
        fork.parent_manifest_id = None;
        fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        engine.persist_slot_dump_manifest(&fork).unwrap();

        let no_cursor = engine.slot_dump_manifest_prune_plan(1);
        assert_eq!(
            no_cursor.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );

        let lagging_cursor = SlotDumpFollowerReplayCursor {
            follower_id: "follower-a".to_string(),
            shard_id: 1,
            oplog_sequence: fork.oplog_sequence,
            index_log_sequence: fork.index_log_sequence,
        };
        let blocked = engine
            .slot_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
        assert!(blocked.prunable_manifest_ids.is_empty());
        assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
        assert_eq!(blocked.follower_blocks.len(), 1);
        assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
        assert_eq!(blocked.follower_blocks[0].manifest_id, fork.manifest_id);
        assert!(blocked
            .reasons
            .contains(&"follower_cursor_blocks_prune".to_string()));

        let caught_up = engine.slot_dump_manifest_prune_plan_with_follower_cursors(
            1,
            vec![SlotDumpFollowerReplayCursor {
                oplog_sequence: child.oplog_sequence,
                index_log_sequence: child.index_log_sequence,
                ..lagging_cursor
            }],
        );
        assert_eq!(
            caught_up.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_generation_mismatch_and_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "generation".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        assert_eq!(manifest.version, 3);
        assert!(!manifest.dump_generation_id.is_empty());
        assert_eq!(manifest.object_lifecycle.live_object_ids, 1);
        assert_eq!(manifest.object_lifecycle.live_page_refs, 1);

        let mut legacy_v2 = manifest.clone();
        legacy_v2.version = 2;
        legacy_v2.object_lifecycle = StorageObjectLifecycleReport::default();
        let legacy_generation_id = slot_dump_generation_id(&legacy_v2);
        legacy_v2.object_lifecycle.live_object_ids = 99;
        assert_eq!(slot_dump_generation_id(&legacy_v2), legacy_generation_id);

        let mut mismatched = manifest.clone();
        mismatched.dump_generation_id = "wrong-generation".to_string();
        mismatched.checksum = slot_dump_manifest_checksum(&mismatched).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&mismatched)
                .unwrap_err()
                .code,
            "slot_dump_generation_mismatch"
        );

        let restore_engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("restore-cache"),
            dir.path().join("pages"),
            dir.path().join("restore-indexes"),
        );
        restore_engine.load_shard(1);
        restore_engine
            .install_slot_dump_manifest(&manifest)
            .expect("first generation should install");

        let mut fork = manifest.clone();
        let extra_slot = fork
            .slot_ids
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        fork.slot_ids.push(extra_slot);
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.manifest_id = format!("{}-fork", fork.manifest_id);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        assert_eq!(
            restore_engine
                .install_slot_dump_manifest(&fork)
                .unwrap_err()
                .code,
            "slot_dump_generation_conflict"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_object_lifecycle_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "lifecycle".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_lifecycle = manifest.clone();
        stale_lifecycle.object_lifecycle.live_object_ids = stale_lifecycle
            .object_lifecycle
            .live_object_ids
            .saturating_add(1);
        stale_lifecycle.dump_generation_id = slot_dump_generation_id(&stale_lifecycle);
        stale_lifecycle.checksum = slot_dump_manifest_checksum(&stale_lifecycle).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_lifecycle)
                .unwrap_err()
                .code,
            "slot_dump_object_lifecycle_mismatch"
        );

        let mut reused_owner = manifest.clone();
        {
            let mut restored = serde_json::from_slice::<ShardState>(&reused_owner.index_bytes)
                .expect("manifest index should decode");
            let address = restored
                .strings
                .get_mut("lifecycle")
                .expect("manifest string address");
            address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
            reused_owner.index_bytes = serde_json::to_vec(&restored).unwrap();
            reused_owner.index_sha256 = sha256_hex_bytes(&reused_owner.index_bytes);
            reused_owner.dump_generation_id = slot_dump_generation_id(&reused_owner);
            reused_owner.checksum = slot_dump_manifest_checksum(&reused_owner).unwrap();
        }
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&reused_owner)
                .unwrap_err()
                .code,
            "slot_dump_object_lifecycle_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_slot_summary_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "slot-summary".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_summary = manifest.clone();
        let summary = stale_summary
            .slot_summaries
            .first_mut()
            .expect("slot summary should exist");
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        stale_summary.dump_generation_id = slot_dump_generation_id(&stale_summary);
        stale_summary.checksum = slot_dump_manifest_checksum(&stale_summary).unwrap();

        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_summary)
                .unwrap_err()
                .code,
            "slot_dump_slot_summary_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_byte_accounting_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "byte-accounting".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_bytes = manifest.clone();
        stale_bytes.logical_bytes = stale_bytes.logical_bytes.saturating_add(1);
        stale_bytes.checksum = slot_dump_manifest_checksum(&stale_bytes).unwrap();

        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_bytes)
                .unwrap_err()
                .code,
            "slot_dump_byte_accounting_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_non_canonical_slot_and_page_segment_ids() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "canonical".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut duplicate_slot = manifest.clone();
        duplicate_slot.slot_ids.push(
            duplicate_slot
                .slot_ids
                .first()
                .copied()
                .expect("slot id should exist"),
        );
        duplicate_slot.dump_generation_id = slot_dump_generation_id(&duplicate_slot);
        duplicate_slot.checksum = slot_dump_manifest_checksum(&duplicate_slot).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&duplicate_slot)
                .unwrap_err()
                .code,
            "slot_dump_slot_ids_not_canonical"
        );

        let mut duplicate_page_segment = manifest.clone();
        duplicate_page_segment.page_segment_ids.push(
            duplicate_page_segment
                .page_segment_ids
                .first()
                .copied()
                .expect("page segment id should exist"),
        );
        duplicate_page_segment.dump_generation_id =
            slot_dump_generation_id(&duplicate_page_segment);
        duplicate_page_segment.checksum =
            slot_dump_manifest_checksum(&duplicate_page_segment).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&duplicate_page_segment)
                .unwrap_err()
                .code,
            "slot_dump_page_segment_ids_not_canonical"
        );
    }

    #[test]
    fn storage_lifecycle_plan_and_boundary_report_cover_dirty_and_orphan_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.page_store().roll_segment().unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v2".to_vec(),
            },
        });

        let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!plan.dirty_slots.is_empty());
        assert_eq!(plan.selected_dump_slots, plan.dirty_slots);
        assert!(plan.reasons.contains(&"dirty_slot_dump".to_string()));
        assert!(plan.stale_page_segment_ids.contains(&0));
        assert!(plan
            .reasons
            .contains(&"ranked_reclaim_candidates".to_string()));
        assert!(!plan.reclaim_candidates.is_empty());
        assert_eq!(plan.reclaim_candidates[0].page_segment_id, 0);
        assert_eq!(plan.reclaim_candidates[0].reason, "orphan_segment");
        assert!(plan.reclaim_candidates[0].stale_physical_bytes > 0);
        assert!(plan.reclaim_candidates[0].reclaim_score > 0);

        let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: plan.selected_dump_slots.clone(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: true,
            warm_cache: false,
        });
        assert!(report.dump_manifest.is_some());
        assert_eq!(report.object_lifecycle.live_object_ids, 1);
        assert_eq!(report.object_lifecycle.live_page_refs, 1);
        assert_eq!(report.object_lifecycle.stale_object_ids, 1);
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.latest_safe_oplog_sequence, 2);
        assert_eq!(boundary.latest_dump_oplog_sequence, 2);
        assert!(boundary.orphan_page_segment_ids.contains(&0));
    }

    #[test]
    fn storage_production_readiness_reports_warnings_without_blocking_dirty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "ready-key".to_string(),
                        value: b"ready-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report(1);
        assert!(report.production_ready, "{report:?}");
        assert!(report.blockers.is_empty());
        assert_eq!(report.dirty_slot_count, 1);
        assert!(report
            .warnings
            .contains(&"dirty_slots_pending_dump".to_string()));
        assert!(report.segment_integrity.integrity_ok);
        assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
        assert_eq!(report.unreadable_page_ref_count, 0);
        assert_eq!(report.owner_mismatch_page_ref_count, 0);
        assert!(report.log_compatibility.rust_native_replay_safe);
        assert!(!report.log_compatibility.cxx_binary_compatible);
        assert_eq!(
            report.log_compatibility.oplog_format,
            "rust-jsonl-command-v1"
        );
        assert_eq!(
            report.log_compatibility.index_log_format,
            "rust-jsonl-shard-index-v1"
        );
        assert!(report.page_format_compatibility.rust_native_read_safe);
        assert!(!report.page_format_compatibility.cxx_page_header_compatible);
        assert_eq!(
            report.page_format_compatibility.page_format,
            "rust-page-envelope-v6"
        );
        assert!(report.page_format_compatibility.checksum_protected);
        assert!(report.page_format_compatibility.object_ids_embedded);
        assert!(report.page_store_bytes_written > 0);
    }

    #[test]
    fn storage_log_compatibility_report_counts_jsonl_sequences_and_cxx_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..2 {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: format!("log-key-{index}"),
                            value: format!("log-value-{index}").into_bytes(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let report = engine.storage_log_compatibility_report(1);
        assert_eq!(report.shard_id, 1);
        assert_eq!(report.oplog_last_sequence, 2);
        assert_eq!(report.index_log_last_sequence, 2);
        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert!(report.oplog_bytes > 0);
        assert!(report.index_log_bytes > 0);
        assert!(report.rust_native_replay_safe);
        assert!(!report.cxx_binary_compatible);
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("C++ binary/protobuf oplog")));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("golden-log replay")));
    }

    #[test]
    fn storage_page_format_compatibility_report_counts_zones_and_header_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "page-format-key".to_string(),
                        value: vec![11; 512],
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();

        let report = engine.storage_page_format_compatibility_report(1);
        assert_eq!(report.shard_id, 1);
        assert_eq!(report.page_format, "rust-page-envelope-v6");
        assert_eq!(report.rust_envelope_version, 6);
        assert!(report.rust_native_read_safe);
        assert!(!report.cxx_page_header_compatible);
        assert!(report.checksum_protected);
        assert!(report.object_ids_embedded);
        assert!(report.routing_slots_embedded);
        assert!(report.compression_supported);
        assert_eq!(report.sealed_zones, 1);
        assert_eq!(report.active_zones, 1);
        assert!(report.live_physical_bytes > 0);
        assert!(report.page_store_writes > 0);
        assert!(report.page_store_bytes_written > 0);
        assert!(report.logical_bytes_written >= 512);
        assert!(report.compressed_records_written > 0);
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("C++ protobuf page header")));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("golden-page replay")));
    }

    #[test]
    fn storage_production_readiness_policy_can_block_dirty_dump_lag_and_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "policy-key".to_string(),
                        value: b"policy-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report_with_policy(
            1,
            StorageProductionReadinessPolicy {
                max_dirty_slots: Some(0),
                max_undumped_oplog_records: Some(0),
                require_slot_dump_manifest: true,
                ..StorageProductionReadinessPolicy::default()
            },
        );

        assert!(!report.production_ready, "{report:?}");
        assert_eq!(report.policy.max_dirty_slots, Some(0));
        assert_eq!(report.dirty_slot_count, 1);
        assert!(report.undumped_oplog_records > 0);
        assert!(report
            .blockers
            .contains(&"dirty_slots_exceed_policy".to_string()));
        assert!(report
            .blockers
            .contains(&"undumped_oplog_records_exceed_policy".to_string()));
        assert!(report
            .blockers
            .contains(&"slot_dump_manifest_required".to_string()));
    }

    #[test]
    fn storage_production_readiness_policy_can_promote_warnings_to_blockers() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "warn-key".to_string(),
                        value: b"warn-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report_with_policy(
            1,
            StorageProductionReadinessPolicy {
                block_on_warnings: true,
                ..StorageProductionReadinessPolicy::default()
            },
        );

        assert!(!report.production_ready, "{report:?}");
        assert!(report
            .warnings
            .contains(&"dirty_slots_pending_dump".to_string()));
        assert!(report
            .blockers
            .contains(&"warnings_exceed_policy".to_string()));
    }

    #[test]
    fn storage_production_readiness_blocks_corrupt_live_page_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "corrupt-key".to_string(),
                        value: b"corrupt-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        let segment_id = engine.live_page_segment_ids(1)[0];
        let mut bytes = engine.page_store().read_segment(segment_id).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        engine
            .page_store()
            .install_segment(segment_id, &bytes)
            .unwrap();

        let report = engine.storage_production_readiness_report(1);
        assert!(!report.production_ready, "{report:?}");
        assert!(report
            .blockers
            .contains(&"corrupt_page_segments".to_string()));
        assert!(report
            .blockers
            .contains(&"unreadable_live_page_refs".to_string()));
        assert!(report
            .blockers
            .contains(&"storage_segment_integrity_failed".to_string()));
        assert!(!report.segment_integrity.integrity_ok);
        assert!(report.segment_integrity.corrupt_page_segment_count > 0);
        assert!(report.segment_integrity.unreadable_page_ref_count > 0);
        assert!(report.corrupt_page_segment_count > 0);
        assert!(report.unreadable_page_ref_count > 0);
    }

    #[test]
    fn storage_lifecycle_apply_warms_cache_from_page_index() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "warm-me".to_string(),
                value: vec![7; 128],
            },
        });
        engine.cache().invalidate_shard(1).unwrap();
        let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: plan.selected_dump_slots,
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: true,
        });
        assert!(report.cache_warmup_page_refs >= 1);
        assert_eq!(
            report.cache_warmup.warmed_page_refs,
            report.cache_warmup_page_refs
        );
        assert!(report.cache_warmup.considered_page_refs >= 1);
        assert!(report.cache_warmup.page_store_reads >= 1);
        assert!(report.cache_warmup.warmed_bytes >= 128);
        assert_eq!(report.cache_warmup.failed_page_refs, 0);
        assert!(engine.cache().stats().puts >= 1);
    }

    #[test]
    fn storage_cache_warmup_report_filters_slots_and_counts_cache_hits() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let first_key = "warm-slot-a";
        let first_slot = engine.routing_slot_for_key(1, first_key);
        let second_key = (0..100)
            .map(|index| format!("warm-slot-b-{index}"))
            .find(|key| engine.routing_slot_for_key(1, key) != first_slot)
            .expect("test should find a key in another slot");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: first_key.to_string(),
                value: b"value-a".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: second_key,
                value: b"value-b".to_vec(),
            },
        });
        engine.cache().invalidate_shard(1).unwrap();

        let slot = first_slot;
        let first = engine.storage_cache_warmup_report(1, [slot]);
        assert_eq!(first.selected_slots, vec![slot]);
        assert_eq!(first.considered_page_refs, 1);
        assert_eq!(first.skipped_page_refs, 1);
        assert_eq!(first.page_store_reads, 1);
        assert_eq!(first.already_cached_page_refs, 0);
        assert_eq!(first.failed_page_refs, 0);
        assert!(first.warmed_bytes > 0);

        let second = engine.storage_cache_warmup_report(1, [slot]);
        assert_eq!(second.considered_page_refs, 1);
        assert_eq!(second.skipped_page_refs, 1);
        assert_eq!(second.page_store_reads, 0);
        assert_eq!(second.already_cached_page_refs, 1);
        assert_eq!(second.warmed_page_refs, 1);
    }

    #[test]
    fn storage_cache_inspection_reports_slot_entries_and_invalidates_slot() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let key = "slot-cache-key";
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"slot-cache-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.cache().invalidate_shard(1).unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );

        let slot = engine.routing_slot_for_key(1, key);
        let report = engine.storage_cache_inspection_report(1);
        assert!(report.stats.disk_fills >= 1);
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
        assert!(report
            .slot_summaries
            .iter()
            .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));

        let invalidated = engine
            .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
                shard_id: 1,
                routing_slot: slot,
            })
            .unwrap();
        assert!(invalidated.memory_entries_removed >= 1);
        let after = engine.storage_cache_inspection_report(1);
        assert!(!after
            .entries
            .iter()
            .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
    }

    #[test]
    fn tiny_cache_dump_load_restart_refills_from_disk_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let restore_index_dir = dir.path().join("restore-indexes");
        let engine = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        let target_value = b"dump-load-target-1234".to_vec();
        for (key, value) in [
            ("target", target_value.clone()),
            ("churn-a", b"cache-churn-a-1234".to_vec()),
            ("churn-b", b"cache-churn-b-1234".to_vec()),
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value,
                        },
                    })
                    .status
                    .ok
            );
        }
        let target_page_key = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .unwrap()
                .strings
                .get("target")
                .unwrap()
                .clone();
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "target".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        for key in ["churn-a", "churn-b"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string(),
                        },
                    })
                    .status
                    .ok
            );
        }
        assert!(engine.cache().stats().memory_evictions > 0);
        assert!(engine.cache().stats().disk_bytes > 0);
        assert_eq!(engine.cache().get_memory(&target_page_key), None);
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("slot dump manifest should persist");
        engine.validate_slot_dump_manifest(&manifest).unwrap();

        let restored =
            TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &restore_index_dir);
        restored.load_shard(1);
        restored
            .install_slot_dump_manifest(&manifest)
            .expect("slot dump should install after restart");
        let page_reads_before = restored.page_store().stats().reads;
        let disk_hits_before = restored.cache().stats().disk_hits;
        let response = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(target_value)
            }
        );
        assert_eq!(
            restored.page_store().stats().reads,
            page_reads_before,
            "restored engine should refill from disk block cache before page store"
        );
        assert!(restored.cache().stats().disk_hits > disk_hits_before);

        let slot = restored.routing_slot_for_key(1, "target");
        let cache_report = restored.storage_cache_inspection_report(1);
        assert!(cache_report
            .slot_summaries
            .iter()
            .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));
        let invalidated = restored
            .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
                shard_id: 1,
                routing_slot: slot,
            })
            .unwrap();
        assert!(invalidated.memory_entries_removed >= 1);
        let readiness = restored.storage_production_readiness_report(1);
        assert!(readiness.production_ready, "{readiness:?}");
        assert_eq!(readiness.unreadable_page_ref_count, 0);
        assert_eq!(readiness.corrupt_page_segment_count, 0);
    }

    #[test]
    fn storage_lifecycle_plan_matches_cpp_delayed_and_limited_dirty_slot_dump_policy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..128 {
            let key = format!("slot-{i}");
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.clone(),
                    value: key.as_bytes().to_vec(),
                },
            });
            let observed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1,
                selected_dump_slots: Vec::new(),
                max_dump_slots_per_round: 0,
                min_undumped_oplog_records: 0,
                purge_delayed_destroy: false,
                prune_slot_dump_manifests: false,
                roll_forward_slot_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                invalidate_cache: false,
                warm_cache: false,
            });
            if observed.dirty_slots.len() >= 3 {
                break;
            }
        }

        let delayed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 99,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(delayed.dump_delayed);
        assert!(delayed.selected_dump_slots.is_empty());
        assert!(delayed
            .reasons
            .contains(&"dirty_slot_dump_delayed".to_string()));

        let limited = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 2,
            min_undumped_oplog_records: 1,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!limited.dump_delayed);
        assert!(limited.undumped_oplog_records >= 3);
        assert_eq!(limited.selected_dump_slots.len(), 2);
        assert!(limited.dirty_slots.len() >= limited.selected_dump_slots.len());

        let explicit = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: vec![delayed.dirty_slots[0]],
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 99,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!explicit.dump_delayed);
        assert_eq!(explicit.selected_dump_slots, vec![delayed.dirty_slots[0]]);
    }
}
