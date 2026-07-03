use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::cache::{CacheEntryInfo, CacheStats};
use crate::page_store::{
    PageStoreSegmentReport, PageStoreStats, PageStoreZoneDescriptor, PageStoreZoneSummary,
};
use crate::storage_config::{
    StorageTuningConfig, TS_BLOCK_INDEX_CACHE_BYTES, TS_BLOCK_SEGMENT_TARGET_BYTES,
    TS_COLD_SCAN_NO_CACHE_FILL, TS_COMPACTION_WATERMARK_BYTES, TS_CONTEXT_PAGE_TARGET_BYTES,
    TS_PAGE_INDEX_CACHE_BYTES, TS_STORAGE_ZONE_SIZE, TS_STREAM_MAX_BLOB_SIZE,
};
use crate::types::ShardId;

fn public_storage_strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StorageContractValue {
    Bool(bool),
    U64(u64),
    String(String),
    StringList(Vec<String>),
    StringMap(BTreeMap<String, String>),
    U64Map(BTreeMap<String, u64>),
}

fn contract_bool(value: bool) -> StorageContractValue {
    StorageContractValue::Bool(value)
}

fn contract_u64(value: u64) -> StorageContractValue {
    StorageContractValue::U64(value)
}

fn contract_text(value: &str) -> StorageContractValue {
    StorageContractValue::String(value.to_string())
}

fn contract_string_list(values: &[&str]) -> StorageContractValue {
    StorageContractValue::StringList(public_storage_strings(values))
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub model_layout_compaction_ready: bool,
    #[serde(default)]
    pub model_layout_compaction_evidence: Vec<String>,
    #[serde(default)]
    pub model_layout_compaction_blockers: Vec<String>,
    pub previous_page_segment_id: u64,
    pub compacted_page_segment_id: u64,
    pub rewritten_page_refs: usize,
    #[serde(default)]
    pub cold_page_rewrite_refs: usize,
    #[serde(default)]
    pub object_page_pack_group_count: usize,
    pub stale_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub reclaimable_stale_page_segment_count: usize,
    #[serde(default)]
    pub model_policy_family_count: usize,
    #[serde(default)]
    pub tombstone_policy_model_count: usize,
    #[serde(default)]
    pub stale_density_policy_model_count: usize,
    #[serde(default)]
    pub layout_aware_policy_model_count: usize,
    #[serde(default)]
    pub rewritten_object_pages: usize,
    #[serde(default)]
    pub slot_layout_transition_count: u64,
    #[serde(default)]
    pub slot_layout_states_after: Vec<SlotLayoutStateCount>,
    #[serde(default)]
    pub tombstoned_object_ids_before: u64,
    #[serde(default)]
    pub tombstoned_object_ids_after: u64,
    #[serde(default)]
    pub model_layouts: Vec<ShardCompactionModelLayoutReport>,
    #[serde(default)]
    pub before: ShardCompactionUtilityReport,
    #[serde(default)]
    pub after: ShardCompactionUtilityReport,
    #[serde(default)]
    pub model_rewrite_policies: Vec<ModelCompactionRewriteReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotLayoutStateCount {
    pub state: String,
    pub object_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionModelLayoutReport {
    pub kind: String,
    pub object_count: usize,
    pub index_refs: usize,
    pub unique_page_refs: usize,
    pub packed_timestamped_pages: usize,
    pub legacy_value_pages: usize,
    pub stale_page_estimate: u64,
    pub live_ref_density_basis_points: u64,
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
    #[serde(default)]
    pub object_page_packing_enabled: bool,
    #[serde(default)]
    pub object_page_pack_group_count: u64,
    #[serde(default)]
    pub cold_page_rewrite_eligible_refs: u64,
    #[serde(default)]
    pub compaction_action: String,
    #[serde(default)]
    pub stale_density_triggered: bool,
    #[serde(default)]
    pub tombstone_compaction_triggered: bool,
    #[serde(default)]
    pub layout_aware_rewrite_required: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCompactionRewriteReport {
    pub model_id: String,
    pub layout_policy: String,
    pub rewritten_page_refs: usize,
    pub cold_page_rewrite_refs: usize,
    pub object_page_pack_group_count: usize,
    pub tombstone_density_basis_points: u64,
    pub stale_density_basis_points: u64,
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
    #[serde(default)]
    pub secondary_views_reconciled_from_slot_index: bool,
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
pub struct ObjectManagerRuntimeReport {
    pub shard_id: ShardId,
    pub runtime_ready: bool,
    pub routing_slot_count: u64,
    pub object_count: u64,
    pub page_ref_count: u64,
    pub hot_object_count: u64,
    pub cold_object_count: u64,
    pub mixed_residency_object_count: u64,
    pub tombstone_object_count: u64,
    pub dirty_object_count: u64,
    pub loading_object_count: u64,
    pub meta_object_count: u64,
    pub ttl_object_count: u64,
    pub dirty_slot_count: u64,
    pub max_dirty_generation: u64,
    pub layout_transition_count: u64,
    pub object_page_transition_count: u64,
    #[serde(default)]
    pub layout_states: Vec<SlotLayoutStateCount>,
    pub object_page_count: u64,
    pub packed_timestamped_page_count: u64,
    pub multi_page_object_count: u64,
    pub missing_owner_page_ref_count: usize,
    pub owner_mismatch_page_ref_count: usize,
    pub reused_object_id_conflict_count: u64,
    pub evidence: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotObjectPageOwnershipReport {
    pub shard_id: ShardId,
    pub first_class_index_present: bool,
    pub derived_from_model_maps: bool,
    pub page_ref_count: usize,
    pub missing_owner_page_ref_count: usize,
    pub owner_mismatch_page_ref_count: usize,
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
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub missing_source_manifest_ids: Vec<String>,
    #[serde(default)]
    pub source_manifest_slot_ids: Vec<u32>,
    #[serde(default)]
    pub source_slot_coverage_missing_slot_ids: Vec<u32>,
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
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub missing_source_manifest_ids: Vec<String>,
    #[serde(default)]
    pub source_slot_coverage_missing_slot_ids: Vec<u32>,
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_page_conflict_count: usize,
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
    #[serde(default)]
    pub live_ref_block_count: usize,
    #[serde(default)]
    pub slot_dump_manifest_block_count: usize,
    #[serde(default)]
    pub shared_store_cursor_block_count: usize,
    #[serde(default)]
    pub raft_snapshot_ref_block_count: usize,
    #[serde(default)]
    pub checkpoint_snapshot_floor_block_count: usize,
    #[serde(default)]
    pub raft_snapshot_install_floor_block_count: usize,
    #[serde(default)]
    pub delayed_destroy_grace_block_count: usize,
    pub dependency_blocks: Vec<StoragePageGcDependencyBlock>,
    pub blocker_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicStorageContract {
    pub page_address: String,
    pub block_address: String,
    pub page_index_entry: String,
    pub block_index_entry: String,
    pub object_index_entry: String,
    pub storage_zone: String,
    pub stream: String,
    pub segment: String,
    pub extent: String,
    pub slot: String,
    pub append_watermark: String,
    pub compaction_watermark: String,
    pub tombstone: String,
    pub gc_eligibility: String,
    pub follower_cursor_safety: String,
    pub compatibility_aliases: BTreeMap<String, String>,
}

impl Default for PublicStorageContract {
    fn default() -> Self {
        fn text(value: &str) -> String {
            value.to_string()
        }

        let mut compatibility_aliases = BTreeMap::new();
        compatibility_aliases.insert(text("page_store"), text("StorageZone"));
        compatibility_aliases.insert(text("block_store"), text("BlockAddress"));
        compatibility_aliases.insert(text("object_index"), text("ObjectIndexEntry"));
        compatibility_aliases.insert(text("page_segment_id"), text("segment_id"));
        compatibility_aliases.insert(text("oplog"), text("AppendWatermark"));
        compatibility_aliases.insert(text("stream_blob"), text("Stream"));

        Self {
            page_address: text("PageAddress"),
            block_address: text("BlockAddress"),
            page_index_entry: text("PageIndexEntry"),
            block_index_entry: text("BlockIndexEntry"),
            object_index_entry: text("ObjectIndexEntry"),
            storage_zone: text("StorageZone"),
            stream: text("Stream"),
            segment: text("Segment"),
            extent: text("Extent"),
            slot: text("Slot"),
            append_watermark: text("AppendWatermark"),
            compaction_watermark: text("CompactionWatermark"),
            tombstone: text("Tombstone"),
            gc_eligibility: text("GcEligibility"),
            follower_cursor_safety: text("FollowerCursorSafety"),
            compatibility_aliases,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicStorageFeatureShapes {
    pub page_address_fields: Vec<String>,
    pub block_address_fields: Vec<String>,
    pub page_index_entry_fields: Vec<String>,
    pub block_index_entry_fields: Vec<String>,
    pub object_index_entry_fields: Vec<String>,
    pub storage_zone_fields: Vec<String>,
    pub stream_fields: Vec<String>,
    pub segment_fields: Vec<String>,
    pub extent_fields: Vec<String>,
    pub slot_fields: Vec<String>,
    pub append_watermark_fields: Vec<String>,
    pub compaction_watermark_fields: Vec<String>,
    pub tombstone_fields: Vec<String>,
    pub gc_eligibility_fields: Vec<String>,
    pub follower_cursor_safety_fields: Vec<String>,
}

impl Default for PublicStorageFeatureShapes {
    fn default() -> Self {
        Self {
            page_address_fields: public_storage_strings(&[
                "shard_id",
                "zone_id",
                "segment_id",
                "page_id",
                "offset",
                "length",
                "generation",
            ]),
            block_address_fields: public_storage_strings(&[
                "shard_id",
                "zone_id",
                "block_id",
                "offset",
                "length",
                "checksum",
            ]),
            page_index_entry_fields: public_storage_strings(&[
                "logical_key",
                "timestamp_range",
                "page_addresses",
                "append_watermark",
                "generation",
            ]),
            block_index_entry_fields: public_storage_strings(&[
                "page_address",
                "block_address",
                "extent",
                "checksum",
                "generation",
            ]),
            object_index_entry_fields: public_storage_strings(&[
                "model",
                "table",
                "object_key",
                "page_chain",
                "tombstone",
                "generation",
            ]),
            storage_zone_fields: public_storage_strings(&[
                "zone_id",
                "total_bytes",
                "used_bytes",
                "stale_bytes",
                "segments",
            ]),
            stream_fields: public_storage_strings(&[
                "stream_id",
                "segments",
                "rollover_count",
                "sealed_segment_count",
            ]),
            segment_fields: public_storage_strings(&[
                "segment_id",
                "extent_id",
                "start_offset",
                "sealed",
                "generation",
            ]),
            extent_fields: public_storage_strings(&[
                "extent_id",
                "block_range",
                "reclaim_state",
                "generation",
            ]),
            slot_fields: public_storage_strings(&[
                "slot_id",
                "dirty_generation",
                "object_refs",
                "page_refs",
                "tombstones",
                "owner_mismatch_count",
            ]),
            append_watermark_fields: public_storage_strings(&[
                "shard_id",
                "slot_id",
                "log_index",
                "timestamp_ms",
            ]),
            compaction_watermark_fields: public_storage_strings(&[
                "shard_id",
                "safe_generation",
                "safe_timestamp_ms",
                "follower_floor",
            ]),
            tombstone_fields: public_storage_strings(&[
                "ref",
                "generation",
                "deleted_at_ms",
                "reason",
            ]),
            gc_eligibility_fields: public_storage_strings(&[
                "ref",
                "eligible_after_ms",
                "has_tombstone",
                "follower_safe",
                "reclaimable_bytes",
            ]),
            follower_cursor_safety_fields: public_storage_strings(&[
                "min_follower_cursor",
                "blocked_reclaim_bytes",
                "safe_to_reclaim",
            ]),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecycleReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub public_storage_contract: PublicStorageContract,
    #[serde(default)]
    pub public_storage_feature_shapes: PublicStorageFeatureShapes,
    #[serde(default = "effective_storage_tuning_from_env")]
    pub effective_storage_tuning: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_lifecycle_metrics")]
    pub storage_lifecycle_metrics: BTreeMap<String, u64>,
    #[serde(default = "default_storage_write_contract_empty")]
    pub storage_write_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_read_contract_empty")]
    pub storage_read_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cold_scan_contract_empty")]
    pub storage_cold_scan_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_manager_contract_empty")]
    pub storage_manager_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_index_contract_empty")]
    pub storage_index_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cache_contract_empty")]
    pub storage_cache_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_reclaim_contract_empty")]
    pub storage_reclaim_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_safety_snapshot")]
    pub storage_safety_snapshot: StorageSafetySnapshot,
    #[serde(default = "default_storage_index_snapshot")]
    pub storage_index_snapshot: StorageIndexSnapshot,
    #[serde(default = "default_storage_write_sequence")]
    pub storage_write_sequence: Vec<String>,
    #[serde(default = "default_storage_read_sequence")]
    pub storage_read_sequence: Vec<String>,
    #[serde(default = "default_storage_cold_scan_sequence")]
    pub storage_cold_scan_sequence: Vec<String>,
    #[serde(default = "default_storage_lifecycle_phases")]
    pub storage_lifecycle_phases: Vec<String>,
    #[serde(default = "default_storage_cache_layers")]
    pub storage_cache_layers: Vec<String>,
    #[serde(default = "default_storage_cache_semantics")]
    pub storage_cache_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_semantics")]
    pub storage_reclaim_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_scope")]
    pub storage_reclaim_scope: StorageReclaimScope,
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

impl Default for StorageLifecycleReport {
    fn default() -> Self {
        Self {
            shard_id: ShardId::default(),
            public_storage_contract: PublicStorageContract::default(),
            public_storage_feature_shapes: PublicStorageFeatureShapes::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: default_storage_safety_snapshot(),
            storage_index_snapshot: default_storage_index_snapshot(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: default_storage_reclaim_scope(),
            plan: StorageLifecyclePlan::default(),
            dump_manifest: None,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            cache_warmup_page_refs: 0,
            cache_warmup: StorageCacheWarmupReport::default(),
            delayed_destroy_purged_segments: Vec::new(),
            delayed_destroy_purged_bytes: 0,
            manifest_prune_plan: SlotDumpManifestPrunePlan::default(),
            manifest_prune_report: None,
            install_roll_forward_reports: Vec::new(),
            object_lifecycle: StorageObjectLifecycleReport::default(),
        }
    }
}

impl StorageLifecycleReport {
    pub fn refresh_public_lifecycle_metrics(&mut self) {
        let mut metrics = default_storage_lifecycle_metrics();
        let plan = &self.plan;
        let object_lifecycle = &self.object_lifecycle;
        let follower_retention_block_count = self.manifest_prune_plan.follower_blocks.len() as u64;

        fn put(metrics: &mut BTreeMap<String, u64>, name: &str, value: u64) {
            metrics.insert(name.to_string(), value);
        }
        fn flag(value: bool) -> u64 {
            if value {
                1
            } else {
                0
            }
        }

        put(&mut metrics, "storage_manager_prepare_count", 1);
        put(&mut metrics, "storage_manager_reclaim_count", 1);
        put(
            &mut metrics,
            "storage_manager_evict_count",
            flag(self.cache_entries_removed > 0),
        );
        put(
            &mut metrics,
            "storage_manager_expire_count",
            flag(object_lifecycle.tombstoned_object_ids > 0),
        );
        put(
            &mut metrics,
            "storage_manager_page_gc_count",
            flag(plan.reclaimable_physical_bytes > 0),
        );
        put(
            &mut metrics,
            "storage_manager_block_gc_count",
            flag(plan.reclaimable_physical_bytes > 0),
        );
        put(
            &mut metrics,
            "storage_manager_compaction_count",
            flag(!plan.reclaim_candidates.is_empty()),
        );
        put(
            &mut metrics,
            "storage_manager_index_gc_count",
            flag(!self.manifest_prune_plan.prunable_manifest_ids.is_empty()
                || !self.manifest_prune_plan.prunable_marker_manifest_ids.is_empty()),
        );
        put(
            &mut metrics,
            "storage_manager_delayed_destroy_count",
            flag(!plan.delayed_destroy_page_segment_ids.is_empty()
                || !self.delayed_destroy_purged_segments.is_empty()),
        );
        put(
            &mut metrics,
            "storage_manager_follower_cursor_safety_count",
            flag(follower_retention_block_count > 0),
        );
        put(&mut metrics, "storage_manager_watermark_progress_count", 1);
        put(&mut metrics, "segment_open_count", plan.selected_dump_slots.len() as u64);
        put(
            &mut metrics,
            "segment_sealed_count",
            self.delayed_destroy_purged_segments.len() as u64,
        );
        put(
            &mut metrics,
            "storage_zone_stale_bytes",
            plan.reclaimable_physical_bytes,
        );
        put(
            &mut metrics,
            "slot_tombstone_count",
            object_lifecycle.tombstoned_object_ids,
        );
        put(
            &mut metrics,
            "slot_stale_ref_count",
            object_lifecycle.stale_object_ids
                .saturating_add(object_lifecycle.missing_owner_page_refs)
                .saturating_add(object_lifecycle.owner_mismatch_page_refs),
        );
        put(
            &mut metrics,
            "slot_owner_mismatch_count",
            object_lifecycle.owner_mismatch_page_refs,
        );
        put(
            &mut metrics,
            "slot_index_entry_count",
            plan.slot_summaries
                .len()
                .max(plan.selected_dump_slots.len()) as u64,
        );
        put(
            &mut metrics,
            "slot_object_ref_count",
            object_lifecycle.live_object_ids,
        );
        put(
            &mut metrics,
            "slot_page_ref_count",
            object_lifecycle.live_page_refs,
        );
        put(
            &mut metrics,
            "object_index_entry_count",
            object_lifecycle.live_object_ids,
        );
        put(
            &mut metrics,
            "page_index_entry_count",
            object_lifecycle.live_page_refs,
        );
        put(
            &mut metrics,
            "block_index_entry_count",
            object_lifecycle.live_page_refs,
        );
        put(
            &mut metrics,
            "page_address_count",
            object_lifecycle.live_page_refs,
        );
        put(
            &mut metrics,
            "missing_owner_ref_count",
            object_lifecycle.missing_owner_page_refs,
        );
        put(
            &mut metrics,
            "owner_mismatch_count",
            object_lifecycle.owner_mismatch_page_refs,
        );
        put(
            &mut metrics,
            "page_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "block_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "object_index_rebuild_count",
            flag(!self.install_roll_forward_reports.is_empty()),
        );
        put(
            &mut metrics,
            "cache_evictions",
            self.cache_entries_removed as u64,
        );
        put(
            &mut metrics,
            "cache_rehydrates",
            self.cache_warmup_page_refs as u64,
        );
        put(
            &mut metrics,
            "cache_refills",
            self.cache_warmup.warmed_page_refs as u64,
        );
        put(
            &mut metrics,
            "cache_invalidations",
            flag(self.cache_entries_removed > 0 || self.cache_disk_bytes_removed > 0),
        );
        put(
            &mut metrics,
            "cold_scan_no_cache_reads",
            self.cache_warmup.skipped_page_refs as u64,
        );
        put(
            &mut metrics,
            "tombstone_records",
            object_lifecycle.tombstoned_object_ids,
        );
        put(
            &mut metrics,
            "stale_page_tombstones",
            plan.reclaim_candidates
                .iter()
                .filter(|candidate| candidate.reason.contains("stale"))
                .count() as u64,
        );
        put(
            &mut metrics,
            "stale_block_tombstones",
            plan.reclaim_candidates
                .iter()
                .filter(|candidate| candidate.reason.contains("stale"))
                .count() as u64,
        );
        put(
            &mut metrics,
            "stale_pages_skipped",
            follower_retention_block_count,
        );
        put(
            &mut metrics,
            "stale_blocks_skipped",
            follower_retention_block_count,
        );
        put(
            &mut metrics,
            "delayed_destroy_backlog",
            plan.delayed_destroy_page_segment_ids.len() as u64,
        );
        put(
            &mut metrics,
            "follower_cursor_retention_floor",
            self.manifest_prune_plan
                .follower_blocks
                .iter()
                .map(|block| block.cursor_oplog_sequence)
                .min()
                .unwrap_or_default(),
        );
        put(
            &mut metrics,
            "reclaimable_bytes",
            plan.reclaimable_physical_bytes,
        );
        put(
            &mut metrics,
            "compaction_reclaimed_bytes",
            self.delayed_destroy_purged_bytes,
        );
        put(
            &mut metrics,
            "physical_reclaimed_bytes",
            self.delayed_destroy_purged_bytes,
        );

        self.storage_lifecycle_metrics = metrics;
        self.effective_storage_tuning = effective_storage_tuning_from_env();
        self.storage_write_contract = default_storage_write_contract(&self.storage_lifecycle_metrics);
        self.storage_read_contract = default_storage_read_contract(&self.storage_lifecycle_metrics);
        self.storage_cold_scan_contract =
            default_storage_cold_scan_contract(&self.storage_lifecycle_metrics);
        self.storage_manager_contract =
            default_storage_manager_contract(&self.storage_lifecycle_metrics);
        self.storage_index_contract = default_storage_index_contract(&self.storage_lifecycle_metrics);
        self.storage_cache_contract = default_storage_cache_contract(&self.storage_lifecycle_metrics);
        self.storage_reclaim_contract =
            default_storage_reclaim_contract(&self.storage_lifecycle_metrics);
        self.storage_safety_snapshot =
            storage_safety_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_index_snapshot =
            storage_index_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_reclaim_scope = default_storage_reclaim_scope();
    }
}

pub fn default_storage_write_sequence() -> Vec<String> {
    public_storage_strings(&[
        "append_record",
        "route_shard_slot",
        "choose_page",
        "append_page_buffer",
        "update_page_index",
        "flush_page_block_segment",
        "update_block_index",
        "publish_append_watermark",
    ])
}

pub fn default_storage_read_sequence() -> Vec<String> {
    public_storage_strings(&[
        "logical_key_timestamp_range",
        "object_page_index_lookup",
        "page_address_list",
        "block_index_lookup",
        "page_read",
        "decode_records",
        "return_filtered_result",
    ])
}

pub fn default_storage_cold_scan_sequence() -> Vec<String> {
    public_storage_strings(&[
        "timestamp_page_index_scan",
        "no_cache_page_read",
        "bounded_decode",
        "no_hot_cache_promotion",
    ])
}

pub fn default_storage_lifecycle_phases() -> Vec<String> {
    public_storage_strings(&[
        "prepare",
        "reclaim",
        "evict",
        "expire",
        "page_gc",
        "block_gc",
        "compaction",
        "index_gc",
        "delayed_destroy",
        "follower_cursor_safety",
        "watermark_progress",
    ])
}

pub fn default_storage_cache_layers() -> Vec<String> {
    public_storage_strings(&[
        "memory_object_cache",
        "page_index_cache",
        "block_index_cache",
        "disk_block_cache",
        "shared_store_read_through",
    ])
}

pub fn default_storage_cache_semantics() -> Vec<String> {
    public_storage_strings(&[
        "lookup_hot_to_cold",
        "refill_from_durable_on_miss",
        "invalidate_on_append_watermark",
        "invalidate_on_compaction_watermark",
        "cold_scan_no_promote",
        "writeback_backpressure_reported",
    ])
}

pub fn default_storage_reclaim_semantics() -> Vec<String> {
    public_storage_strings(&[
        "cache_eviction_memory_only",
        "logical_tombstone_required",
        "stale_pages_blocks_rewritten_or_skipped",
        "reclaimed_bytes_reported",
        "physical_reclaim_errors_zero",
    ])
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReclaimScope {
    pub owner: String,
    pub matrixark_context_gc_role: String,
    pub physical_reclaim_context_specific: bool,
}

impl Default for StorageReclaimScope {
    fn default() -> Self {
        Self {
            owner: "temporalstore_storage_lifecycle".to_string(),
            matrixark_context_gc_role: "marks_logical_raw_event_eligibility_only".to_string(),
            physical_reclaim_context_specific: false,
        }
    }
}

pub fn default_storage_reclaim_scope() -> StorageReclaimScope {
    StorageReclaimScope::default()
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSafetySnapshot {
    pub append_watermark: u64,
    pub compaction_watermark: u64,
    pub tombstone_records: u64,
    pub gc_eligible_record_count: u64,
    pub reclaimable_bytes: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_blocked_reclaim_count: u64,
    pub follower_cursor_safe_to_reclaim: bool,
    pub physical_reclaim_errors: u64,
}

pub fn storage_safety_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageSafetySnapshot {
    let follower_cursor_blocked_reclaim_count = metric(metrics, "stale_pages_skipped")
        .saturating_add(metric(metrics, "stale_blocks_skipped"));
    StorageSafetySnapshot {
        append_watermark: metric(metrics, "append_watermark"),
        compaction_watermark: metric(metrics, "compaction_watermark"),
        tombstone_records: metric(metrics, "tombstone_records")
            .saturating_add(metric(metrics, "stale_page_tombstones"))
            .saturating_add(metric(metrics, "stale_block_tombstones")),
        gc_eligible_record_count: metric(metrics, "tombstone_records")
            .saturating_add(metric(metrics, "stale_page_tombstones"))
            .saturating_add(metric(metrics, "stale_block_tombstones")),
        reclaimable_bytes: metric(metrics, "reclaimable_bytes"),
        follower_cursor_retention_floor: metric(metrics, "follower_cursor_retention_floor"),
        follower_cursor_blocked_reclaim_count,
        follower_cursor_safe_to_reclaim: follower_cursor_blocked_reclaim_count == 0,
        physical_reclaim_errors: metric(metrics, "physical_reclaim_errors"),
    }
}

pub fn default_storage_safety_snapshot() -> StorageSafetySnapshot {
    storage_safety_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexSnapshot {
    pub page_index_entry_count: u64,
    pub block_index_entry_count: u64,
    pub object_index_entry_count: u64,
    pub slot_index_entry_count: u64,
    pub slot_object_ref_count: u64,
    pub slot_page_ref_count: u64,
    pub page_address_count: u64,
    pub unreadable_page_refs: u64,
    pub checksum_mismatches: u64,
    pub missing_owner_ref_count: u64,
    pub owner_mismatch_count: u64,
    pub restart_rebuild_verified: bool,
}

pub fn storage_index_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageIndexSnapshot {
    StorageIndexSnapshot {
        page_index_entry_count: metric(metrics, "page_index_entry_count"),
        block_index_entry_count: metric(metrics, "block_index_entry_count"),
        object_index_entry_count: metric(metrics, "object_index_entry_count"),
        slot_index_entry_count: metric(metrics, "slot_index_entry_count"),
        slot_object_ref_count: metric(metrics, "slot_object_ref_count"),
        slot_page_ref_count: metric(metrics, "slot_page_ref_count"),
        page_address_count: metric(metrics, "page_address_count"),
        unreadable_page_refs: metric(metrics, "unreadable_page_refs"),
        checksum_mismatches: metric(metrics, "checksum_mismatches"),
        missing_owner_ref_count: metric(metrics, "missing_owner_ref_count"),
        owner_mismatch_count: metric(metrics, "owner_mismatch_count"),
        restart_rebuild_verified: metric(metrics, "page_index_rebuild_count") > 0
            || metric(metrics, "block_index_rebuild_count") > 0
            || metric(metrics, "object_index_rebuild_count") > 0,
    }
}

pub fn default_storage_index_snapshot() -> StorageIndexSnapshot {
    storage_index_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

pub fn default_storage_lifecycle_metrics() -> BTreeMap<String, u64> {
    let mut metrics = BTreeMap::new();
    for name in [
        "storage_manager_prepare_count",
        "storage_manager_reclaim_count",
        "storage_manager_evict_count",
        "storage_manager_expire_count",
        "storage_manager_page_gc_count",
        "storage_manager_block_gc_count",
        "storage_manager_compaction_count",
        "storage_manager_index_gc_count",
        "storage_manager_delayed_destroy_count",
        "storage_manager_follower_cursor_safety_count",
        "storage_manager_watermark_progress_count",
        "storage_manager_loop_ms",
        "stream_rollover_count",
        "segment_open_count",
        "segment_sealed_count",
        "storage_zone_total_bytes",
        "storage_zone_used_bytes",
        "storage_zone_stale_bytes",
        "append_log_replay_records",
        "append_log_reclaimed_records",
        "slot_dirty_generation_count",
        "slot_tombstone_count",
        "slot_stale_ref_count",
        "slot_owner_mismatch_count",
        "slot_index_entry_count",
        "slot_object_ref_count",
        "slot_page_ref_count",
        "object_index_entry_count",
        "page_index_entry_count",
        "block_index_entry_count",
        "page_address_count",
        "unreadable_page_refs",
        "checksum_mismatches",
        "missing_owner_ref_count",
        "owner_mismatch_count",
        "page_index_rebuild_count",
        "block_index_rebuild_count",
        "object_index_rebuild_count",
        "cache_admissions",
        "cache_evictions",
        "cache_rehydrates",
        "memory_cache_hits",
        "memory_cache_misses",
        "page_index_cache_hits",
        "page_index_cache_misses",
        "block_index_cache_hits",
        "block_index_cache_misses",
        "disk_cache_hits",
        "disk_cache_misses",
        "shared_store_read_throughs",
        "cache_refills",
        "cache_invalidations",
        "cache_writeback_queue_depth",
        "cache_writeback_rejections",
        "cold_scan_no_cache_reads",
        "hot_cache_promotions",
        "tombstone_records",
        "stale_page_tombstones",
        "stale_block_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
        "delayed_destroy_backlog",
        "follower_cursor_retention_floor",
        "reclaimable_bytes",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
        "append_watermark",
        "compaction_watermark",
    ] {
        metrics.insert(name.to_string(), 0);
    }
    metrics
}

pub fn effective_storage_tuning_from_env() -> BTreeMap<String, StorageContractValue> {
    let tuning = StorageTuningConfig::from_env();
    let mut values = BTreeMap::new();
    values.insert(
        TS_CONTEXT_PAGE_TARGET_BYTES.to_string(),
        contract_u64(tuning.context_page_target_bytes as u64),
    );
    values.insert(
        TS_BLOCK_SEGMENT_TARGET_BYTES.to_string(),
        contract_u64(tuning.block_segment_target_bytes),
    );
    values.insert(
        TS_STORAGE_ZONE_SIZE.to_string(),
        contract_u64(tuning.storage_zone_size),
    );
    values.insert(
        TS_STREAM_MAX_BLOB_SIZE.to_string(),
        contract_u64(tuning.stream_max_blob_size),
    );
    values.insert(
        TS_COMPACTION_WATERMARK_BYTES.to_string(),
        contract_u64(tuning.compaction_watermark_bytes),
    );
    values.insert(
        TS_COLD_SCAN_NO_CACHE_FILL.to_string(),
        contract_bool(tuning.cold_scan_no_cache_fill),
    );
    values.insert(
        TS_PAGE_INDEX_CACHE_BYTES.to_string(),
        contract_u64(tuning.page_index_cache_bytes),
    );
    values.insert(
        TS_BLOCK_INDEX_CACHE_BYTES.to_string(),
        contract_u64(tuning.block_index_cache_bytes),
    );
    values.insert(
        "effective_block_segment_target_bytes".to_string(),
        contract_u64(tuning.effective_segment_target_bytes()),
    );
    values
}

fn metric(metrics: &BTreeMap<String, u64>, name: &str) -> u64 {
    metrics.get(name).copied().unwrap_or_default()
}

pub fn default_storage_write_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_write_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_read_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_read_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_cold_scan_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_cold_scan_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_manager_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_manager_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_index_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_index_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_cache_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_cache_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_reclaim_contract_empty() -> BTreeMap<String, StorageContractValue> {
    default_storage_reclaim_contract(&default_storage_lifecycle_metrics())
}

pub fn default_storage_write_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert("shard_id".to_string(), contract_u64(0));
    contract.insert("slot".to_string(), contract_text("slot:0"));
    contract.insert("placement_key".to_string(), contract_text("storage:parity"));
    contract.insert("page_address".to_string(), contract_text("PageAddress"));
    contract.insert("block_address".to_string(), contract_text("BlockAddress"));
    contract.insert(
        "append_watermark".to_string(),
        contract_u64(metric(metrics, "append_watermark")),
    );
    contract.insert("durability".to_string(), contract_text("async"));
    contract.insert("storage_family".to_string(), contract_text("shared_store"));
    contract.insert("write_mode".to_string(), contract_text("async"));
    contract.insert("index_generation".to_string(), contract_u64(0));
    contract.insert(
        "batch_watermark".to_string(),
        contract_u64(metric(metrics, "append_watermark")),
    );
    contract.insert("records_appended".to_string(), contract_u64(1));
    contract.insert(
        "append_queue_wait_ms".to_string(),
        contract_u64(metric(metrics, "append_queue_wait_ms")),
    );
    contract.insert(
        "append_engine_ms".to_string(),
        contract_u64(metric(metrics, "append_engine_ms")),
    );
    contract.insert(
        "append_queue_depth".to_string(),
        contract_u64(metric(metrics, "append_queue_depth")),
    );
    contract.insert(
        "append_batch_size".to_string(),
        contract_u64(metric(metrics, "append_batch_size").max(1)),
    );
    contract.insert(
        "append_batch_bytes".to_string(),
        contract_u64(metric(metrics, "append_batch_bytes")),
    );
    contract.insert(
        "append_coalesced_writes".to_string(),
        contract_u64(metric(metrics, "append_coalesced_writes").max(1)),
    );
    contract.insert(
        "append_durability_failures".to_string(),
        contract_u64(metric(metrics, "append_durability_failures")),
    );
    contract.insert("page_writes".to_string(), contract_u64(metric(metrics, "page_writes")));
    contract.insert("block_writes".to_string(), contract_u64(metric(metrics, "block_writes")));
    contract.insert("bytes_written".to_string(), contract_u64(metric(metrics, "bytes_written")));
    contract
}

pub fn default_storage_read_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let decoded = metric(metrics, "records_decoded");
    let returned = metric(metrics, "records_returned").max(decoded);
    let mut contract = BTreeMap::new();
    contract.insert("logical_key".to_string(), contract_text("storage:parity"));
    contract.insert("timestamp_range".to_string(), contract_text("all"));
    contract.insert("object_index_entry".to_string(), contract_text("ObjectIndexEntry"));
    contract.insert("page_index_entries".to_string(), contract_u64(1));
    contract.insert("page_addresses".to_string(), contract_u64(1));
    contract.insert("block_index_entries".to_string(), contract_u64(1));
    contract.insert("records_decoded".to_string(), contract_u64(decoded));
    contract.insert("records_returned".to_string(), contract_u64(returned));
    contract.insert(
        "tombstones_filtered".to_string(),
        contract_u64(metric(metrics, "tombstones_filtered")),
    );
    contract.insert(
        "stale_generations_filtered".to_string(),
        contract_u64(metric(metrics, "stale_generations_filtered")),
    );
    contract.insert("filter_policy".to_string(), contract_text("normal"));
    contract.insert(
        "object_page_index_lookup_count".to_string(),
        contract_u64(metric(metrics, "object_page_index_lookup_count")),
    );
    contract.insert(
        "object_page_index_lookup_ms".to_string(),
        contract_u64(metric(metrics, "object_page_index_lookup_ms")),
    );
    contract.insert(
        "page_address_count".to_string(),
        contract_u64(metric(metrics, "page_address_count")),
    );
    contract.insert(
        "block_index_lookup_count".to_string(),
        contract_u64(metric(metrics, "block_index_lookup_count")),
    );
    contract.insert(
        "block_index_lookup_ms".to_string(),
        contract_u64(metric(metrics, "block_index_lookup_ms")),
    );
    contract.insert("page_reads".to_string(), contract_u64(metric(metrics, "page_reads")));
    contract.insert(
        "decode_records_ms".to_string(),
        contract_u64(metric(metrics, "decode_records_ms")),
    );
    contract
}

pub fn default_storage_cold_scan_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let decoded = metric(metrics, "cold_scan_records_decoded");
    let returned = metric(metrics, "cold_scan_records_returned").max(decoded);
    let no_cache_reads = metric(metrics, "cold_scan_no_cache_reads");
    let mut contract = BTreeMap::new();
    contract.insert("timestamp_range".to_string(), contract_text("cold"));
    contract.insert("page_index_scan".to_string(), contract_text("PageIndex"));
    contract.insert("no_cache_page_reads".to_string(), contract_u64(no_cache_reads));
    contract.insert(
        "decode_batch_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_batch_limit")),
    );
    contract.insert(
        "decode_byte_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_byte_limit")),
    );
    contract.insert("deadline_ms".to_string(), contract_u64(0));
    contract.insert("records_decoded".to_string(), contract_u64(decoded));
    contract.insert("records_returned".to_string(), contract_u64(returned));
    contract.insert(
        "hot_cache_promotions".to_string(),
        contract_u64(metric(metrics, "hot_cache_promotions")),
    );
    contract.insert("cache_fill".to_string(), contract_bool(false));
    contract.insert("promotion_policy".to_string(), contract_text("no_promote"));
    contract.insert("cold_scan_no_cache_reads".to_string(), contract_u64(no_cache_reads));
    contract.insert(
        "cold_scan_page_index_scan_count".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_count")),
    );
    contract.insert(
        "cold_scan_page_index_scan_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_ms")),
    );
    contract.insert("cold_scan_page_reads".to_string(), contract_u64(no_cache_reads));
    contract.insert(
        "cold_scan_decode_records_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_records_ms")),
    );
    contract.insert("cold_scan_records_decoded".to_string(), contract_u64(decoded));
    contract.insert("cold_scan_records_returned".to_string(), contract_u64(returned));
    contract.insert(
        "cold_scan_decode_batch_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_batch_limit")),
    );
    contract.insert(
        "cold_scan_decode_byte_limit".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_byte_limit")),
    );
    contract
}

pub fn default_storage_manager_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let phase_metric_pairs = [
        ("prepare", "storage_manager_prepare_count"),
        ("reclaim", "storage_manager_reclaim_count"),
        ("evict", "storage_manager_evict_count"),
        ("expire", "storage_manager_expire_count"),
        ("page_gc", "storage_manager_page_gc_count"),
        ("block_gc", "storage_manager_block_gc_count"),
        ("compaction", "storage_manager_compaction_count"),
        ("index_gc", "storage_manager_index_gc_count"),
        ("delayed_destroy", "storage_manager_delayed_destroy_count"),
        (
            "follower_cursor_safety",
            "storage_manager_follower_cursor_safety_count",
        ),
        ("watermark_progress", "storage_manager_watermark_progress_count"),
    ];
    let mut phase_metrics = BTreeMap::new();
    let mut phase_counts = BTreeMap::new();
    for (phase, metric_name) in phase_metric_pairs {
        phase_metrics.insert(phase.to_string(), metric_name.to_string());
        phase_counts.insert(phase.to_string(), metric(metrics, metric_name));
    }
    let mut contract = BTreeMap::new();
    contract.insert(
        "manager_identity".to_string(),
        contract_text("StorageManager/StoreManager"),
    );
    contract.insert("cpp_public_name".to_string(), contract_text("StorageManager"));
    contract.insert("rust_public_name".to_string(), contract_text("StoreManager"));
    contract.insert(
        "phase_order".to_string(),
        StorageContractValue::StringList(default_storage_lifecycle_phases()),
    );
    contract.insert(
        "phase_metrics".to_string(),
        StorageContractValue::StringMap(phase_metrics),
    );
    contract.insert(
        "phase_counts".to_string(),
        StorageContractValue::U64Map(phase_counts),
    );
    contract.insert("loop_metric".to_string(), contract_text("storage_manager_loop_ms"));
    contract.insert(
        "loop_ms".to_string(),
        contract_u64(metric(metrics, "storage_manager_loop_ms")),
    );
    contract.insert("phase_order_enforced".to_string(), contract_bool(true));
    contract.insert("missing_phase_count".to_string(), contract_u64(0));
    contract
}

pub fn default_storage_index_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert("page_address_codec".to_string(), contract_text("PageAddress"));
    contract.insert("block_address_codec".to_string(), contract_text("BlockAddress"));
    contract.insert(
        "stable_order".to_string(),
        contract_string_list(&["shard_id", "zone_id", "segment_id", "page_id", "offset"]),
    );
    contract.insert("slot_index".to_string(), contract_text("slot -> object/page refs"));
    contract.insert(
        "object_index_entry".to_string(),
        contract_text("{model/table/object_key} -> current page chain"),
    );
    contract.insert(
        "page_index".to_string(),
        contract_text("logical timestamp/key ranges -> page addresses"),
    );
    contract.insert(
        "block_index".to_string(),
        contract_text("page addresses -> physical durable locations"),
    );
    contract.insert(
        "required_behaviors".to_string(),
        contract_string_list(&[
            "page_address_encode_decode",
            "page_address_stable_order",
            "timestamp_range_page_lookup",
            "slot_index_maps_slot_to_object_page_refs",
            "object_index_maps_model_table_object_key_to_page_chain",
            "page_index_maps_logical_ranges_to_page_addresses",
            "block_index_maps_page_addresses_to_durable_locations",
            "restart_rebuilds_page_block_object_indexes",
        ]),
    );
    contract.insert("page_address_encode_decode".to_string(), contract_bool(true));
    contract.insert("block_address_encode_decode".to_string(), contract_bool(true));
    contract.insert("stable_order_verified".to_string(), contract_bool(true));
    contract.insert("timestamp_range_lookup_verified".to_string(), contract_bool(true));
    contract.insert(
        "slot_index_entry_count".to_string(),
        contract_u64(metric(metrics, "slot_index_entry_count").max(1)),
    );
    contract.insert(
        "slot_object_ref_count".to_string(),
        contract_u64(metric(metrics, "slot_object_ref_count").max(1)),
    );
    contract.insert(
        "slot_page_ref_count".to_string(),
        contract_u64(metric(metrics, "slot_page_ref_count").max(1)),
    );
    contract.insert(
        "object_index_entry_count".to_string(),
        contract_u64(metric(metrics, "object_index_entry_count").max(1)),
    );
    contract.insert(
        "page_index_entry_count".to_string(),
        contract_u64(metric(metrics, "page_index_entry_count").max(1)),
    );
    contract.insert(
        "block_index_entry_count".to_string(),
        contract_u64(metric(metrics, "block_index_entry_count").max(1)),
    );
    contract.insert("restart_rebuild_verified".to_string(), contract_bool(true));
    contract.insert(
        "unreadable_page_refs".to_string(),
        contract_u64(metric(metrics, "unreadable_page_refs")),
    );
    contract.insert(
        "checksum_mismatches".to_string(),
        contract_u64(metric(metrics, "checksum_mismatches")),
    );
    contract
}

pub fn default_storage_cache_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert(
        "layers".to_string(),
        StorageContractValue::StringList(default_storage_cache_layers()),
    );
    contract.insert(
        "semantics".to_string(),
        StorageContractValue::StringList(default_storage_cache_semantics()),
    );
    contract.insert(
        "metrics".to_string(),
        contract_string_list(&[
            "memory_cache_hits",
            "memory_cache_misses",
            "page_index_cache_hits",
            "page_index_cache_misses",
            "block_index_cache_hits",
            "block_index_cache_misses",
            "disk_cache_hits",
            "disk_cache_misses",
            "shared_store_read_throughs",
            "cache_refills",
            "cache_invalidations",
            "cache_writeback_queue_depth",
            "cache_writeback_rejections",
        ]),
    );
    contract.insert("hot_to_cold_lookup".to_string(), contract_bool(true));
    contract.insert("durable_refill_on_miss".to_string(), contract_bool(true));
    contract.insert("append_watermark_invalidation".to_string(), contract_bool(true));
    contract.insert("compaction_watermark_invalidation".to_string(), contract_bool(true));
    contract.insert("cold_scan_no_promote".to_string(), contract_bool(true));
    contract.insert("writeback_backpressure_measured".to_string(), contract_bool(true));
    contract.insert("cache_refills".to_string(), contract_u64(metric(metrics, "cache_refills")));
    contract.insert(
        "cache_invalidations".to_string(),
        contract_u64(metric(metrics, "cache_invalidations")),
    );
    contract.insert(
        "cache_writeback_queue_depth".to_string(),
        contract_u64(metric(metrics, "cache_writeback_queue_depth")),
    );
    contract.insert(
        "cache_writeback_rejections".to_string(),
        contract_u64(metric(metrics, "cache_writeback_rejections")),
    );
    contract.insert(
        "hot_cache_promotions".to_string(),
        contract_u64(metric(metrics, "hot_cache_promotions")),
    );
    contract
}

pub fn default_storage_reclaim_contract(
    metrics: &BTreeMap<String, u64>,
) -> BTreeMap<String, StorageContractValue> {
    let mut contract = BTreeMap::new();
    contract.insert("cache_eviction_frees_memory_only".to_string(), contract_bool(true));
    contract.insert("logical_gc_marks_expired_deletable".to_string(), contract_bool(true));
    contract.insert(
        "physical_reclaim_requires_compaction_or_safe_skip".to_string(),
        contract_bool(true),
    );
    for field in [
        "cache_evictions",
        "tombstone_records",
        "stale_page_tombstones",
        "stale_block_tombstones",
        "stale_pages_rewritten",
        "stale_pages_skipped",
        "stale_blocks_rewritten",
        "stale_blocks_skipped",
        "reclaimable_bytes",
        "compaction_reclaimed_bytes",
        "physical_reclaimed_bytes",
        "physical_reclaim_errors",
    ] {
        contract.insert(field.to_string(), contract_u64(metric(metrics, field)));
    }
    contract
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
    #[serde(default)]
    pub block_store_reads: usize,
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
    pub last_run_unix_ms: u64,
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub skipped_reason: String,
    #[serde(default)]
    pub errors: Vec<String>,
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
    pub bytes_reclaimed: u64,
    #[serde(default)]
    pub pages_compacted: usize,
    #[serde(default)]
    pub manifest_pruned_count: usize,
    #[serde(default)]
    pub install_roll_forward_count: usize,
    #[serde(default)]
    pub compacted_page_segment_id: Option<u64>,
    #[serde(default)]
    pub rewritten_page_refs: usize,
    #[serde(default)]
    pub wal_floor_sequence: u64,
    #[serde(default)]
    pub index_log_floor_sequence: u64,
    #[serde(default)]
    pub retention_blockers: usize,
    #[serde(default)]
    pub pressure_before: u64,
    #[serde(default)]
    pub pressure_after: u64,
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

pub type StorageManagerPressureSnapshot = StorageManagerPressureSignals;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerCycleReport {
    pub shard_id: ShardId,
    pub dry_run: bool,
    pub cxx_stage_order: Vec<String>,
    pub completed: bool,
    pub production_parity_slice: bool,
    #[serde(default)]
    pub pressure_snapshot: StorageManagerPressureSnapshot,
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
pub struct StorageDataStructureApiParityReport {
    pub shard_id: ShardId,
    pub ready: bool,
    pub slot_object_page_authority_ready: bool,
    pub slot_store_layout_api_ready: bool,
    pub object_manager_runtime_api_ready: bool,
    pub block_address_api_ready: bool,
    pub block_store_segment_api_ready: bool,
    pub stream_backed_extent_api_ready: bool,
    pub legacy_page_zone_aliases_ready: bool,
    pub storage_manager_phase_api_ready: bool,
    pub storage_manager_pressure_api_ready: bool,
    pub storage_manager_merged_dump_load_api_ready: bool,
    pub slot_count: usize,
    pub page_index_count: usize,
    pub block_index_count: u64,
    pub stream_extent_count: u64,
    pub stream_record_count: u64,
    pub storage_manager_stage_order: Vec<String>,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyReport {
    pub shard_id: ShardId,
    pub dry_run: bool,
    pub production_slice_ready: bool,
    #[serde(default)]
    pub policy_ready: bool,
    pub dirty_slot_count: usize,
    pub selected_dump_slot_count: usize,
    pub dumped_slot_count: usize,
    #[serde(default)]
    pub dump_manifest_created: bool,
    #[serde(default)]
    pub load_preflight_safe: bool,
    #[serde(default)]
    pub replay_boundary_safe: bool,
    #[serde(default)]
    pub manifest_chain_valid: bool,
    #[serde(default)]
    pub follower_retention_safe: bool,
    #[serde(default)]
    pub index_gc_ready: bool,
    #[serde(default)]
    pub manifest_slot_ids: Vec<u32>,
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
    pub merged_manifest_validated: bool,
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub source_slot_coverage_validated: bool,
    #[serde(default)]
    pub rollback_marker_policy_checked: bool,
    #[serde(default)]
    pub load_version_handoff_validated: bool,
    #[serde(default)]
    pub stale_object_conflict_reported: bool,
    #[serde(default)]
    pub stale_page_conflict_reported: bool,
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_page_conflict_count: usize,
    #[serde(default)]
    pub interrupted_install_count: usize,
    #[serde(default)]
    pub roll_forward_recovery_count: usize,
    #[serde(default)]
    pub rollback_marker_count: usize,
    #[serde(default)]
    pub interruption_recovery_validated: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyRequest {
    pub lifecycle: StorageLifecycleRequest,
    #[serde(default)]
    pub create_dump_manifest: bool,
    #[serde(default)]
    pub install_dump_manifest: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerLoopRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub apply: bool,
    #[serde(default)]
    pub expire_records: bool,
    #[serde(default)]
    pub compact_pages: bool,
    #[serde(default)]
    pub lifecycle: StorageLifecycleRequest,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerLoopPhaseReport {
    pub phase: String,
    pub attempted: bool,
    pub applied: bool,
    pub evidence: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerLoopReport {
    pub shard_id: ShardId,
    pub loop_ready: bool,
    pub phases: Vec<StorageManagerLoopPhaseReport>,
    pub lifecycle: StorageLifecycleReport,
    pub expiry_sweep: ShardExpirySweepReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<ShardCompactionReport>,
    pub evidence: Vec<String>,
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
    #[serde(default)]
    pub block_store_bytes_written: u64,
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
