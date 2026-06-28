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
    #[serde(default)]
    pub model_policies: Vec<ModelCompactionPolicyReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompactionPolicyReport {
    pub model_id: String,
    pub layout_policy: String,
    pub live_page_refs: u64,
    pub deleted_page_refs: u64,
    pub total_segment_pages: u64,
    pub stale_page_estimate: u64,
    pub stale_density_basis_points: u64,
    pub tombstone_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepReport {
    pub shard_id: ShardId,
    pub expired_records_removed: usize,
    #[serde(default)]
    pub hot_slots_scanned: usize,
    #[serde(default)]
    pub cold_slots_scanned: usize,
    #[serde(default)]
    pub scanned_records: usize,
    #[serde(default)]
    pub skipped_records: usize,
    #[serde(default)]
    pub loaded_for_expire: usize,
    #[serde(default)]
    pub next_hot_cursor: Option<String>,
    #[serde(default)]
    pub next_cold_cursor: Option<String>,
    #[serde(default)]
    pub round_limit: usize,
    #[serde(default)]
    pub load_on_expire_only_when_needed: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub hot_cursor: Option<String>,
    #[serde(default)]
    pub cold_cursor: Option<String>,
    #[serde(default)]
    pub max_hot_slots_per_round: usize,
    #[serde(default)]
    pub max_cold_slots_per_round: usize,
    #[serde(default)]
    pub load_cold_slots: bool,
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
pub struct StoragePhysicalPageIndex {
    pub object_key: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    pub routing_slot: u32,
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    pub dirty: bool,
    pub deleted: bool,
    pub log_backed: bool,
    pub cpp_packed_page_index_len: usize,
    pub cpp_packed_page_index_hex: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalSlotNode {
    pub routing_slot: u32,
    pub layout: String,
    pub dirty: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub object_count: u64,
    pub page_ref_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub dirty_generation: u64,
    pub last_dump_sequence: u64,
    pub cpp_packed_slot_node_len: usize,
    pub cpp_packed_slot_node_hex: String,
    #[serde(default)]
    pub page_indexes: Vec<StoragePhysicalPageIndex>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalIndexReport {
    pub shard_id: ShardId,
    pub slot_first: bool,
    pub slot_index_authority: bool,
    pub slot_count: usize,
    pub page_index_count: usize,
    pub dirty_slot_count: usize,
    pub missing_object_id_count: usize,
    pub missing_routing_slot_count: usize,
    pub missing_page_id_count: usize,
    pub missing_checksum_count: usize,
    pub cpp_packed_page_index_size: usize,
    pub cpp_packed_slot_node_size: usize,
    pub cpp_packed_layout_compatible: bool,
    #[serde(default)]
    pub slot_nodes: Vec<StoragePhysicalSlotNode>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpManifest {
    pub version: u32,
    pub shard_id: ShardId,
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub manifest_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dump_generation_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_manifest_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_version_handoff: Option<SlotDumpLoadVersionHandoff>,
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
pub struct SlotDumpLoadVersionHandoff {
    pub previous_load_version: u64,
    pub next_load_version: u64,
    pub applied: bool,
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
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_page_conflict_count: usize,
    #[serde(default)]
    pub stale_object_conflicts: Vec<String>,
    #[serde(default)]
    pub stale_page_conflicts: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotDumpMergedInstallReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub source_manifest_ids: Vec<String>,
    pub slot_ids: Vec<u32>,
    pub preflight: SlotDumpInstallPreflightReport,
    pub rollback_marker_written: bool,
    pub prepare_marker_written: bool,
    pub install_marker_written: bool,
    pub commit_marker_written: bool,
    pub load_version_handoff: Option<SlotDumpLoadVersionHandoff>,
    pub installed: bool,
    pub status_code: String,
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
pub struct StoragePageGcReplayCursor {
    pub cursor_id: String,
    pub shard_id: ShardId,
    pub retain_from_page_segment_id: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageGcDependencyBlock {
    pub page_segment_id: u64,
    pub dependency: String,
    #[serde(default)]
    pub owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_from_page_segment_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_until_unix_ms: Option<u64>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageGcDependencyPlan {
    pub shard_id: ShardId,
    pub safe_to_reclaim: bool,
    pub candidate_page_segment_ids: Vec<u64>,
    pub reclaimable_page_segment_ids: Vec<u64>,
    pub blocked_page_segment_ids: Vec<u64>,
    pub live_page_segment_ids: Vec<u64>,
    pub manifest_page_segment_ids: Vec<u64>,
    pub shared_store_cursor_count: usize,
    pub checkpoint_snapshot_floor: Option<u64>,
    pub raft_snapshot_install_floor: Option<u64>,
    pub delayed_destroy_grace_ms: u64,
    pub dependency_blocks: Vec<StoragePageGcDependencyBlock>,
    pub blocker_reasons: Vec<String>,
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
pub struct StorageWalReclaimPlan {
    pub shard_id: ShardId,
    pub safe_to_reclaim: bool,
    pub durable_slot_generation_frontier_oplog_sequence: u64,
    pub durable_slot_generation_frontier_index_log_sequence: u64,
    pub retain_from_oplog_sequence: u64,
    pub retain_from_index_log_sequence: u64,
    pub current_oplog_sequence: u64,
    pub current_index_log_sequence: u64,
    pub covered_slot_count: usize,
    pub uncovered_slot_count: usize,
    pub follower_cursor_block_count: usize,
    pub raft_snapshot_block_count: usize,
    pub missing_slot_generations: Vec<u32>,
    pub retained_manifest_ids: Vec<String>,
    pub blocker_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWalReclaimReport {
    pub plan: StorageWalReclaimPlan,
    pub applied: bool,
    pub oplog_records_removed: usize,
    pub index_log_records_removed: usize,
    pub oplog_bytes_before: u64,
    pub oplog_bytes_after: u64,
    pub index_log_bytes_before: u64,
    pub index_log_bytes_after: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexGcReport {
    pub shard_id: ShardId,
    pub enabled: bool,
    pub applied: bool,
    pub dirty_slots_committed_before_truncate: bool,
    pub bytes_threshold: u64,
    pub usage_ratio_trigger_basis_points: u64,
    pub usage_ratio_basis_points: u64,
    pub max_entries_per_round: usize,
    pub retain_from_index_log_sequence: u64,
    pub records_before: usize,
    pub records_after: usize,
    pub records_removed: usize,
    pub removable_records_before_budget: usize,
    pub budget_exhausted: bool,
    pub bytes_before: u64,
    pub bytes_after: u64,
    pub threshold_triggered: bool,
    pub usage_ratio_triggered: bool,
    pub safe_to_truncate: bool,
    pub skipped_reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEvictionVictim {
    pub routing_slot: u32,
    pub object_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub dirty_object_count: u64,
    pub weight: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEvictionReport {
    pub shard_id: ShardId,
    pub mode: String,
    pub pressure_before: u64,
    pub pressure_after: u64,
    pub memory_pressure_threshold: u64,
    pub pressure_gate_open: bool,
    pub batch_limit: usize,
    pub dump_before_evict: bool,
    pub dump_manifest_ids: Vec<String>,
    pub selected_victims: Vec<StorageEvictionVictim>,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    pub dropped_object_count: usize,
    pub cooldown: bool,
    pub skipped_reason: String,
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
    pub page_gc_shared_store_cursors: Vec<StoragePageGcReplayCursor>,
    #[serde(default)]
    pub page_gc_raft_snapshot_refs: Vec<SlotDumpRaftSnapshotRef>,
    #[serde(default)]
    pub page_gc_checkpoint_floor_segment_id: Option<u64>,
    #[serde(default)]
    pub page_gc_raft_install_floor_segment_id: Option<u64>,
    #[serde(default)]
    pub page_gc_delayed_destroy_grace_ms: u64,
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
    #[serde(default = "default_storage_manager_eviction_threshold")]
    pub eviction_memory_pressure_threshold: u64,
    #[serde(default)]
    pub eviction_batch_limit: usize,
    #[serde(default)]
    pub eviction_dump_before_evict: bool,
    #[serde(default)]
    pub eviction_delete_drop: bool,
    #[serde(default)]
    pub max_expire_hot_slots_per_round: usize,
    #[serde(default)]
    pub max_expire_cold_slots_per_round: usize,
    #[serde(default)]
    pub expire_hot_cursor: Option<String>,
    #[serde(default)]
    pub expire_cold_cursor: Option<String>,
    #[serde(default)]
    pub load_cold_slots_for_expire: bool,
    #[serde(default)]
    pub follower_replay_cursors: Vec<SlotDumpFollowerReplayCursor>,
    #[serde(default)]
    pub raft_snapshot_refs: Vec<SlotDumpRaftSnapshotRef>,
    #[serde(default)]
    pub page_gc_shared_store_cursors: Vec<StoragePageGcReplayCursor>,
    #[serde(default)]
    pub page_gc_checkpoint_floor_segment_id: Option<u64>,
    #[serde(default)]
    pub page_gc_raft_install_floor_segment_id: Option<u64>,
    #[serde(default)]
    pub page_gc_delayed_destroy_grace_ms: u64,
    #[serde(default)]
    pub index_gc_index_log_bytes_threshold: u64,
    #[serde(default)]
    pub index_gc_usage_ratio_trigger_basis_points: u64,
    #[serde(default)]
    pub index_gc_max_entries_per_round: usize,
    #[serde(default)]
    pub index_gc_commit_dirty_slots_before_truncation: bool,
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
            eviction_memory_pressure_threshold: default_storage_manager_eviction_threshold(),
            eviction_batch_limit: 0,
            eviction_dump_before_evict: false,
            eviction_delete_drop: false,
            max_expire_hot_slots_per_round: 0,
            max_expire_cold_slots_per_round: 0,
            expire_hot_cursor: None,
            expire_cold_cursor: None,
            load_cold_slots_for_expire: false,
            follower_replay_cursors: Vec::new(),
            raft_snapshot_refs: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            index_gc_index_log_bytes_threshold: 0,
            index_gc_usage_ratio_trigger_basis_points: 0,
            index_gc_max_entries_per_round: 0,
            index_gc_commit_dirty_slots_before_truncation: true,
        }
    }
}

fn default_storage_manager_stage_enabled() -> bool {
    true
}

fn default_storage_manager_eviction_threshold() -> u64 {
    1
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
    pub wal_records_removed: usize,
    #[serde(default)]
    pub index_log_records_removed: usize,
    #[serde(default)]
    pub retain_from_wal_sequence: u64,
    #[serde(default)]
    pub retain_from_index_log_sequence: u64,
    #[serde(default)]
    pub expired_records_removed: usize,
    #[serde(default)]
    pub cache_entries_removed: usize,
    #[serde(default)]
    pub cache_disk_bytes_removed: u64,
    #[serde(default)]
    pub eviction_pressure_before: u64,
    #[serde(default)]
    pub eviction_pressure_after: u64,
    #[serde(default)]
    pub eviction_cooldown: bool,
    #[serde(default)]
    pub dropped_object_count: usize,
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
pub struct StorageManagerPressureSignals {
    pub dirty_slot_count: usize,
    pub undumped_wal_records: u64,
    pub wal_bytes: u64,
    pub index_log_bytes: u64,
    pub stale_page_bytes: u64,
    pub live_page_bytes: u64,
    pub page_segment_stale_density_basis_points: u64,
    pub memory_cache_bytes: u64,
    pub disk_cache_bytes: u64,
    pub memory_cache_pressure_score: u64,
    pub expired_slot_object_scan_debt: usize,
    pub delayed_destroy_segment_count: usize,
    pub delayed_destroy_bytes: u64,
    pub follower_cursor_retention_blockers: usize,
    pub raft_snapshot_retention_blockers: usize,
    pub compaction_debt_model_count: usize,
    pub compaction_debt_score: u64,
    pub total_pressure_score: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerCycleReport {
    pub shard_id: ShardId,
    pub dry_run: bool,
    pub cxx_stage_order: Vec<String>,
    pub completed: bool,
    pub production_parity_slice: bool,
    #[serde(default)]
    pub pressure_signals: StorageManagerPressureSignals,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_reclaim_report: Option<StorageWalReclaimReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_gc_report: Option<StorageIndexGcReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eviction_report: Option<StorageEvictionReport>,
    #[serde(default)]
    pub page_gc_dependency_plan: StoragePageGcDependencyPlan,
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
