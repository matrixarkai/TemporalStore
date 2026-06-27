use serde::{Deserialize, Serialize};

use crate::cache::{CacheEntryInfo, CacheStats};
use crate::page_store::{
    PageStoreSegmentReport, PageStoreStats, PageStoreZoneDescriptor, PageStoreZoneSummary,
};
use crate::types::ShardId;

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
    #[serde(default)]
    pub indexed_timestamped_points: usize,
    #[serde(default)]
    pub unique_timestamped_page_refs: usize,
    #[serde(default)]
    pub packed_timestamped_pages: usize,
    #[serde(default)]
    pub legacy_timestamped_value_pages: usize,
    #[serde(default)]
    pub families: Vec<StorageTimestampedPageFamilyReport>,
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

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTimestampedPageFamilyReport {
    pub kind: String,
    pub indexed_points: usize,
    pub unique_page_refs: usize,
    pub packed_pages: usize,
    pub legacy_value_pages: usize,
    pub corrupt_pages: usize,
    pub mismatch_count: usize,
}

impl StorageFeaturePageLayoutReport {
    pub(crate) fn has_errors(&self) -> bool {
        !self.corrupt_packed_feature_pages.is_empty()
            || !self.missing_indexed_timestamps.is_empty()
            || !self.orphan_packed_timestamps.is_empty()
            || !self.duplicate_packed_timestamps.is_empty()
    }

    pub(crate) fn mismatch_count(&self) -> usize {
        self.missing_indexed_timestamps.len()
            + self.orphan_packed_timestamps.len()
            + self.duplicate_packed_timestamps.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeaturePageError {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    pub key: String,
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFeaturePageTimestampMismatch {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
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
pub struct SlotDumpFaultMatrixReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub production_ready_slice: bool,
    pub scenario_count: usize,
    pub passed_count: usize,
    pub failed_scenarios: Vec<SlotDumpFaultScenarioReport>,
    pub scenarios: Vec<SlotDumpFaultScenarioReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpFaultScenarioReport {
    pub scenario: String,
    pub passed: bool,
    pub expected_code: String,
    pub actual_code: String,
    pub blockers: Vec<String>,
    pub install_safe: bool,
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
    #[serde(default)]
    pub raft_snapshot_blocks: Vec<SlotDumpRaftSnapshotRetentionBlock>,
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
pub struct SlotDumpRaftSnapshotRef {
    pub snapshot_id: String,
    pub shard_id: ShardId,
    pub last_included_index: u64,
    pub last_included_term: u64,
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
pub struct SlotDumpRaftSnapshotRetentionBlock {
    pub snapshot_id: String,
    pub manifest_id: String,
    pub manifest_oplog_sequence: u64,
    pub manifest_index_log_sequence: u64,
    pub snapshot_oplog_sequence: u64,
    pub snapshot_index_log_sequence: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerCycleRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_prepare: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_oplog_reclaim: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_evict: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_expire: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_page_reclaim: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_page_compaction: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_index_gc: bool,
    #[serde(default)]
    pub max_dump_slots_per_round: usize,
    #[serde(default)]
    pub min_undumped_oplog_records: u64,
    #[serde(default)]
    pub warm_cache: bool,
}

impl Default for StorageManagerCycleRequest {
    fn default() -> Self {
        Self {
            shard_id: 0,
            dry_run: false,
            enable_prepare: true,
            enable_oplog_reclaim: true,
            enable_evict: true,
            enable_expire: true,
            enable_page_reclaim: true,
            enable_page_compaction: true,
            enable_index_gc: true,
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            warm_cache: false,
        }
    }
}

fn default_storage_manager_stage_enabled() -> bool {
    true
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerStageReport {
    pub stage: String,
    pub enabled: bool,
    pub applied: bool,
    pub skipped: bool,
    pub reason: String,
    #[serde(default)]
    pub pressure_signal: String,
    #[serde(default)]
    pub pressure_score: u64,
    #[serde(default)]
    pub pressure_threshold: u64,
    #[serde(default)]
    pub pressure_triggered: bool,
    #[serde(default)]
    pub candidate_count: usize,
    #[serde(default)]
    pub skipped_count: usize,
    #[serde(default)]
    pub before_bytes: u64,
    #[serde(default)]
    pub after_bytes: u64,
    #[serde(default)]
    pub live_bytes: u64,
    #[serde(default)]
    pub stale_bytes: u64,
    #[serde(default)]
    pub selected_slots: Vec<u32>,
    #[serde(default)]
    pub selected_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub dirty_slot_count: usize,
    #[serde(default)]
    pub undumped_oplog_records: u64,
    #[serde(default)]
    pub dumped_slot_count: usize,
    #[serde(default)]
    pub expired_records_removed: usize,
    #[serde(default)]
    pub cache_entries_removed: usize,
    #[serde(default)]
    pub cache_disk_bytes_removed: u64,
    #[serde(default)]
    pub page_segments_reclaimed: usize,
    #[serde(default)]
    pub page_bytes_reclaimed: u64,
    #[serde(default)]
    pub manifest_pruned_count: usize,
    #[serde(default)]
    pub install_roll_forward_count: usize,
    #[serde(default)]
    pub compacted_page_segment_id: Option<u64>,
    #[serde(default)]
    pub rewritten_page_refs: usize,
    #[serde(default)]
    pub metrics_slot_count: usize,
    #[serde(default)]
    pub metrics_page_ref_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerCycleReport {
    pub shard_id: ShardId,
    pub dry_run: bool,
    pub cxx_stage_order: Vec<String>,
    pub completed: bool,
    pub production_parity_slice: bool,
    pub stages: Vec<StorageManagerStageReport>,
    pub plan: StorageLifecyclePlan,
    #[serde(default)]
    pub merged_dump_load_policy: StorageMergedDumpLoadPolicyReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_report: Option<StorageLifecycleReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expiry_report: Option<ShardExpirySweepReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_report: Option<ShardCompactionReport>,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyReport {
    pub shard_id: ShardId,
    pub dry_run: bool,
    pub production_slice_ready: bool,
    pub dirty_slot_count: usize,
    pub selected_dump_slot_count: usize,
    pub dumped_slot_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_id: Option<String>,
    pub manifest_checksum_validated: bool,
    pub manifest_generation_validated: bool,
    pub sequence_boundaries_validated: bool,
    pub page_segments_validated: bool,
    pub live_page_refs_validated: bool,
    pub object_lifecycle_validated: bool,
    pub install_preflight_safe: bool,
    pub install_marker_policy_checked: bool,
    pub install_roll_forward_checked: bool,
    pub page_reclaim_policy_applied: bool,
    pub compaction_policy_applied: bool,
    pub index_gc_policy_applied: bool,
    pub cache_policy_applied: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
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
    #[serde(default)]
    pub compatibility_mode: String,
    #[serde(default)]
    pub migration_required: bool,
    #[serde(default)]
    pub cxx_reader_supported: bool,
    #[serde(default)]
    pub cxx_writer_supported: bool,
    #[serde(default)]
    pub golden_conversion_required: bool,
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
    #[serde(default)]
    pub compatibility_mode: String,
    #[serde(default)]
    pub migration_required: bool,
    #[serde(default)]
    pub cxx_page_header_reader_supported: bool,
    #[serde(default)]
    pub cxx_page_header_writer_supported: bool,
    #[serde(default)]
    pub golden_conversion_required: bool,
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
