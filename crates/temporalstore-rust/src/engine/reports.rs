// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::block_store::{
    BlockStoreSlabReport, BlockStoreStats, BlockStoreBandDescriptor, BlockStoreBandSummary,
};
use crate::storage_config::{
    StorageTuningConfig, TS_BLOCK_INDEX_CACHE_BYTES, TS_BLOCK_SLAB_TARGET_BYTES,
    TS_COLD_SCAN_NO_CACHE_FILL, TS_COMPACTION_WATERMARK_BYTES, TS_CONTEXT_PAGE_TARGET_BYTES,
    TS_PAGE_INDEX_CACHE_BYTES, TS_STORAGE_ZONE_SIZE, TS_STREAM_MAX_BLOB_SIZE,
};
use crate::types::{ShardId, Status};
use matrixcache::{CacheEntryInfo, CacheStats};

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
    #[serde(alias = "previous_page_segment_id")]
    pub previous_page_slab_id: u64,
    #[serde(alias = "compacted_page_segment_id")]
    pub compacted_page_slab_id: u64,
    pub rewritten_page_refs: usize,
    #[serde(default)]
    pub cold_page_rewrite_refs: usize,
    #[serde(default)]
    pub object_page_pack_group_count: usize,
    #[serde(alias = "stale_page_segment_ids")]
    pub stale_page_slab_ids: Vec<u64>,
    #[serde(default)]
    #[serde(alias = "reclaimable_stale_page_segment_count")]
    pub reclaimable_stale_page_slab_count: usize,
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
    #[serde(rename = "slot_layout_transition_count")]
    pub bucket_layout_transition_count: u64,
    #[serde(default)]
    #[serde(rename = "slot_layout_states_after")]
    pub bucket_layout_states_after: Vec<BucketLayoutStateCount>,
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
pub struct BucketLayoutStateCount {
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
    #[serde(alias = "live_page_segment_count")]
    pub live_page_slab_count: usize,
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
    #[serde(alias = "total_segment_pages")]
    pub total_slab_pages: u64,
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
    #[serde(rename = "hot_slots_scanned")]
    pub hot_buckets_scanned: usize,
    #[serde(default)]
    #[serde(rename = "cold_slots_scanned")]
    pub cold_buckets_scanned: usize,
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
    #[serde(rename = "max_hot_slots_per_round")]
    pub max_hot_buckets_per_round: usize,
    #[serde(default)]
    #[serde(rename = "max_cold_slots_per_round")]
    pub max_cold_buckets_per_round: usize,
    #[serde(default)]
    #[serde(rename = "load_cold_slots")]
    pub load_cold_buckets: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustStorageObservation {
    pub shard_id: ShardId,
    pub cache: CacheStats,
    pub page_store: BlockStoreStats,
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
    #[serde(rename = "wal_records")]
    pub wal_records: usize,
    pub index_log_records: usize,
    #[serde(alias = "active_page_segment_ids")]
    pub active_page_slab_ids: Vec<u64>,
    #[serde(alias = "live_page_segment_ids")]
    pub live_page_slab_ids: Vec<u64>,
    pub zone_descriptors: Vec<BlockStoreBandDescriptor>,
    #[serde(default)]
    pub zone_summary: BlockStoreBandSummary,
    #[serde(default)]
    #[serde(alias = "page_segment_reports")]
    pub page_slab_reports: Vec<BlockStoreSlabReport>,
    #[serde(default)]
    #[serde(alias = "page_segment_live_reports")]
    pub page_slab_live_reports: Vec<StorageRecoverySlabLiveReport>,
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
    #[serde(alias = "segment_integrity")]
    pub slab_integrity: StorageSlabIntegrityReport,
    #[serde(default)]
    pub feature_page_layout: StorageFeaturePageLayoutReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryPageError {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryPageOwnerMismatch {
    pub object_key: String,
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub offset: u64,
    pub expected_object_id: u64,
    pub actual_object_id: Option<u64>,
    #[serde(rename = "expected_routing_slot")]
    pub expected_routing_bucket: u32,
    #[serde(rename = "actual_routing_slot")]
    pub actual_routing_bucket: Option<u32>,
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
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
pub struct StorageRecoverySlabLiveReport {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
    #[serde(rename = "live_routing_slot_count")]
    pub live_routing_bucket_count: u64,
    pub live_ref_density_basis_points: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlabIntegrityReport {
    pub shard_id: ShardId,
    #[serde(alias = "indexed_page_segment_count")]
    pub indexed_page_slab_count: usize,
    #[serde(alias = "discovered_page_segment_count")]
    pub discovered_page_slab_count: usize,
    #[serde(alias = "live_page_segment_count")]
    pub live_page_slab_count: usize,
    #[serde(alias = "orphan_page_segment_count")]
    pub orphan_page_slab_count: usize,
    pub stale_page_ref_count: usize,
    #[serde(alias = "corrupt_page_segment_count")]
    pub corrupt_page_slab_count: usize,
    pub unreadable_page_ref_count: usize,
    pub unreadable_page_bytes: u64,
    pub owner_mismatch_page_ref_count: usize,
    pub missing_owner_page_ref_count: usize,
    pub reclaim_required: bool,
    pub integrity_ok: bool,
}

/// One summary a bucket, and a dump manifest carries one per bucket.
///
/// The short aliases below were the first half of a two-step change: every reader accepts them,
/// and this still WRITES the long names by default. A dump manifest is read by other engines
/// during install, so a writer that emitted short names unconditionally would hand a document to
/// a reader that has never heard of them -- and these fields carry no serde default, so that
/// reader fails rather than degrades.
///
/// Measured on a 4,000-bucket manifest: the field names are 592,046 bytes of a 1,367,875-byte
/// document -- 43% of it, once the whitespace came out.
///
/// The second step has been taken and the switch that guarded it is gone: the writer emits the
/// short names, which is worth 43% of the document.
///
/// Reading never depended on that switch and still does not. Both spellings deserialize, so a
/// directory holding manifests written either way -- which every fleet that has run both has --
/// loads end to end. What no longer exists is a way to ASK for the long spelling: producing a
/// manifest for a reader older than the release that taught the aliases now means running such a
/// build, not setting a variable on this one.
#[derive(Debug, Default, Clone, PartialEq, Eq, Deserialize)]
pub struct BucketStorageSummary {
    #[serde(rename = "routing_slot", alias = "rs")]
    pub routing_bucket: u32,
    #[serde(alias = "oc")]
    pub object_count: u64,
    #[serde(alias = "prc")]
    pub page_ref_count: u64,
    #[serde(alias = "lb")]
    pub logical_bytes: u64,
    #[serde(alias = "pb")]
    pub physical_bytes: u64,
    #[serde(alias = "doc")]
    pub dirty_object_count: u64,
    #[serde(alias = "dg")]
    pub dirty_generation: u64,
    #[serde(alias = "lds")]
    pub last_dump_sequence: u64,
    #[serde(default)]
    #[serde(alias = "page_segment_ids", alias = "psi")]
    pub page_slab_ids: Vec<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "lcz")]
    pub last_compacted_zone: Option<u64>,
}

/// Whether the manifest writer spells its fields short. **On by default.**
///
/// Shipped opt-in first, because turning it on writes a document an engine older than the
/// release that taught readers the short names cannot load -- those fields carry no serde
/// default, so such a reader fails rather than degrades. Which engines are deployed is the one
/// fact this code cannot check for itself, so the flip was left as an operator decision, and it
/// has now been taken.
///
/// What the code CAN check, and does: every reader in this repository accepts both spellings.
/// The aliases are on the struct, the round-trip is asserted in both directions below, and no
/// Python tool reads this document -- the `logical_bytes`/`physical_bytes` names in `tools/`
/// belong to the band extent manifest, which is a different file with different fields.
///
impl BucketStorageSummary {
    /// Write the summary under one spelling or the other.
    ///
    /// Takes the choice as an argument rather than reading the switch, so a test can exercise
    /// both spellings without touching process environment -- which is shared, and which several
    /// hundred sites in this crate already race over.
    fn serialize_named<S>(&self, serializer: S, short: bool) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut fields = 9;
        if self.last_compacted_zone.is_some() {
            fields += 1;
        }
        let mut out = serializer.serialize_struct("BucketStorageSummary", fields)?;
        out.serialize_field(
            if short { "rs" } else { "routing_slot" },
            &self.routing_bucket,
        )?;
        out.serialize_field(if short { "oc" } else { "object_count" }, &self.object_count)?;
        out.serialize_field(
            if short { "prc" } else { "page_ref_count" },
            &self.page_ref_count,
        )?;
        out.serialize_field(
            if short { "lb" } else { "logical_bytes" },
            &self.logical_bytes,
        )?;
        out.serialize_field(
            if short { "pb" } else { "physical_bytes" },
            &self.physical_bytes,
        )?;
        out.serialize_field(
            if short { "doc" } else { "dirty_object_count" },
            &self.dirty_object_count,
        )?;
        out.serialize_field(
            if short { "dg" } else { "dirty_generation" },
            &self.dirty_generation,
        )?;
        out.serialize_field(
            if short { "lds" } else { "last_dump_sequence" },
            &self.last_dump_sequence,
        )?;
        out.serialize_field(
            if short { "psi" } else { "page_slab_ids" },
            &self.page_slab_ids,
        )?;
        // Absent stays absent: the derived writer skipped this when None, and a manifest that
        // started emitting nulls would be bigger, not smaller.
        match self.last_compacted_zone.as_ref() {
            Some(zone) => {
                out.serialize_field(if short { "lcz" } else { "last_compacted_zone" }, zone)?
            }
            None => out.skip_field(if short { "lcz" } else { "last_compacted_zone" })?,
        }
        out.end()
    }
}

impl serde::Serialize for BucketStorageSummary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.serialize_named(serializer, true)
    }
}

/// A summary written under an explicit spelling, for tests and for anything that must not depend
/// on the process-wide switch.
pub(crate) struct SummaryNamed<'a>(pub(crate) &'a BucketStorageSummary, pub(crate) bool);

impl serde::Serialize for SummaryNamed<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize_named(serializer, self.1)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalPageIndex {
    pub object_key: String,
    pub model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
    pub native_packed_page_index_len: usize,
    pub native_packed_page_index_hex: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalBucketNode {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
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
    #[serde(rename = "native_packed_slot_node_len")]
    pub native_packed_bucket_node_len: usize,
    #[serde(rename = "native_packed_slot_node_hex")]
    pub native_packed_bucket_node_hex: String,
    #[serde(default)]
    pub page_indexes: Vec<StoragePhysicalPageIndex>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePhysicalIndexReport {
    pub shard_id: ShardId,
    #[serde(rename = "slot_first")]
    pub bucket_first: bool,
    #[serde(rename = "slot_index_authority")]
    pub bucket_index_authority: bool,
    #[serde(default)]
    #[serde(rename = "secondary_views_reconciled_from_slot_index")]
    pub secondary_views_reconciled_from_bucket_index: bool,
    #[serde(rename = "slot_count")]
    pub bucket_count: usize,
    pub page_index_count: usize,
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    pub missing_object_id_count: usize,
    #[serde(rename = "missing_routing_slot_count")]
    pub missing_routing_bucket_count: usize,
    pub missing_page_id_count: usize,
    pub missing_checksum_count: usize,
    pub native_packed_page_index_size: usize,
    #[serde(rename = "native_packed_slot_node_size")]
    pub native_packed_bucket_node_size: usize,
    pub native_packed_layout_compatible: bool,
    #[serde(default)]
    #[serde(rename = "slot_nodes")]
    pub bucket_nodes: Vec<StoragePhysicalBucketNode>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketObjectPageOwnershipReport {
    pub shard_id: ShardId,
    pub first_class_index_present: bool,
    pub derived_from_model_maps: bool,
    pub page_ref_count: usize,
    pub missing_owner_page_ref_count: usize,
    pub owner_mismatch_page_ref_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectManagerRuntimeReport {
    pub shard_id: ShardId,
    pub runtime_ready: bool,
    #[serde(rename = "routing_slot_count")]
    pub routing_bucket_count: u64,
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
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: u64,
    pub max_dirty_generation: u64,
    #[serde(default)]
    pub object_page_transition_count: u64,
    pub layout_transition_count: u64,
    #[serde(default)]
    pub layout_states: Vec<BucketLayoutStateCount>,
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
pub struct BucketDumpManifest {
    pub version: u32,
    pub shard_id: ShardId,
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub manifest_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dump_generation_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_manifest_ids: Vec<String>,
    // Self-describing source coverage for a merged dump: each source's
    // (manifest_id, bucket_ids), captured at create time when the sources were
    // validated. Lets a target engine that does not hold the source manifest files
    // preflight source coverage from the manifest itself (falls back to on-disk
    // sources when empty). Rust-native, skip-if-empty so non-merged manifests are
    // byte-for-byte unchanged.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_manifest_coverage: Vec<BucketDumpSourceCoverage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_version_handoff: Option<BucketDumpLoadVersionHandoff>,
    pub created_unix_ms: u64,
    #[serde(rename = "slot_ids")]
    pub bucket_ids: Vec<u32>,
    #[serde(alias = "page_segment_ids")]
    pub page_slab_ids: Vec<u64>,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
    pub live_page_refs: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    #[serde(rename = "slot_summaries")]
    pub bucket_summaries: Vec<BucketStorageSummary>,
    #[serde(
        default,
        skip_serializing_if = "StorageObjectLifecycleReport::is_empty"
    )]
    pub object_lifecycle: StorageObjectLifecycleReport,
    /// The index image itself. Encoded, not written as an array of numbers: the array shape costs
    /// three to four characters per byte, and this field carries the whole image. The reader
    /// accepts either shape, so a manifest written before this still loads.
    #[serde(default, with = "crate::bytes_serde")]
    pub index_bytes: Vec<u8>,
    #[serde(default)]
    pub index_sha256: String,
    pub checksum: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpSourceCoverage {
    pub manifest_id: String,
    #[serde(rename = "slot_ids")]
    pub bucket_ids: Vec<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpLoadVersionHandoff {
    pub previous_load_version: u64,
    pub next_load_version: u64,
    pub applied: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallMarker {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub phase: String,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
    pub created_unix_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallPreflightReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub install_safe: bool,
    pub blockers: Vec<String>,
    #[serde(rename = "current_wal_sequence")]
    pub current_wal_sequence: u64,
    pub current_index_log_sequence: u64,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(alias = "missing_page_segment_ids")]
    pub missing_page_slab_ids: Vec<u64>,
    #[serde(alias = "corrupt_page_segment_ids")]
    pub corrupt_page_slab_ids: Vec<u64>,
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
    #[serde(rename = "source_manifest_slot_ids")]
    pub source_manifest_bucket_ids: Vec<u32>,
    #[serde(default)]
    #[serde(rename = "source_slot_coverage_missing_slot_ids")]
    pub source_bucket_coverage_missing_bucket_ids: Vec<u32>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpMergedInstallReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub source_manifest_ids: Vec<String>,
    #[serde(rename = "slot_ids")]
    pub bucket_ids: Vec<u32>,
    pub preflight: BucketDumpInstallPreflightReport,
    pub rollback_marker_written: bool,
    pub prepare_marker_written: bool,
    pub install_marker_written: bool,
    pub commit_marker_written: bool,
    pub load_version_handoff: Option<BucketDumpLoadVersionHandoff>,
    pub installed: bool,
    pub status_code: String,
    #[serde(default)]
    pub source_manifest_count: usize,
    #[serde(default)]
    pub missing_source_manifest_ids: Vec<String>,
    #[serde(default)]
    #[serde(rename = "source_slot_coverage_missing_slot_ids")]
    pub source_bucket_coverage_missing_bucket_ids: Vec<u32>,
    #[serde(default)]
    pub stale_object_conflict_count: usize,
    #[serde(default)]
    pub stale_page_conflict_count: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFaultMatrixReport {
    pub shard_id: ShardId,
    pub manifest_id: String,
    pub production_ready_slice: bool,
    pub scenario_count: usize,
    pub passed_count: usize,
    pub failed_scenarios: Vec<BucketDumpFaultScenarioReport>,
    pub scenarios: Vec<BucketDumpFaultScenarioReport>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFaultScenarioReport {
    pub scenario: String,
    pub passed: bool,
    pub expected_code: String,
    pub actual_code: String,
    pub blockers: Vec<String>,
    pub install_safe: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestChainIssue {
    pub manifest_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_manifest_id: Option<String>,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestPrunePlan {
    pub shard_id: ShardId,
    pub retained_manifest_ids: Vec<String>,
    pub prunable_manifest_ids: Vec<String>,
    pub prunable_marker_manifest_ids: Vec<String>,
    pub blocked_manifest_ids: Vec<String>,
    #[serde(default)]
    pub follower_blocks: Vec<BucketDumpFollowerRetentionBlock>,
    #[serde(default)]
    pub raft_snapshot_blocks: Vec<BucketDumpRaftSnapshotRetentionBlock>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpManifestPruneReport {
    pub shard_id: ShardId,
    pub plan: BucketDumpManifestPrunePlan,
    pub removed_manifest_ids: Vec<String>,
    pub removed_marker_files: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpInstallRollForwardReport {
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
pub struct BucketDumpFollowerReplayCursor {
    pub follower_id: String,
    pub shard_id: ShardId,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpRaftSnapshotRef {
    pub snapshot_id: String,
    pub shard_id: ShardId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub index_log_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpFollowerRetentionBlock {
    pub follower_id: String,
    pub manifest_id: String,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "cursor_wal_sequence")]
    pub cursor_wal_sequence: u64,
    pub cursor_index_log_sequence: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDumpRaftSnapshotRetentionBlock {
    pub snapshot_id: String,
    pub manifest_id: String,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "snapshot_wal_sequence")]
    pub snapshot_wal_sequence: u64,
    pub snapshot_index_log_sequence: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecyclePlan {
    pub shard_id: ShardId,
    #[serde(rename = "dirty_slots")]
    pub dirty_buckets: Vec<u32>,
    #[serde(rename = "selected_dump_slots")]
    pub selected_dump_buckets: Vec<u32>,
    #[serde(rename = "undumped_wal_records", default)]
    pub undumped_wal_records: u64,
    #[serde(default)]
    pub dump_delayed: bool,
    #[serde(rename = "slot_summaries")]
    pub bucket_summaries: Vec<BucketStorageSummary>,
    #[serde(alias = "live_page_segment_ids")]
    pub live_page_slab_ids: Vec<u64>,
    #[serde(alias = "stale_page_segment_ids")]
    pub stale_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub reclaim_candidates: Vec<StorageReclaimCandidate>,
    #[serde(alias = "delayed_destroy_page_segment_ids")]
    pub delayed_destroy_page_slab_ids: Vec<u64>,
    pub reclaimable_physical_bytes: u64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReclaimCandidate {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
    #[serde(alias = "retain_from_page_segment_id")]
    pub retain_from_page_slab_id: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageGcDependencyBlock {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub dependency: String,
    #[serde(default)]
    pub owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "retain_from_page_segment_id")]
    pub retain_from_page_slab_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain_until_unix_ms: Option<u64>,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageGcDependencyPlan {
    pub shard_id: ShardId,
    pub safe_to_reclaim: bool,
    #[serde(alias = "candidate_page_segment_ids")]
    pub candidate_page_slab_ids: Vec<u64>,
    #[serde(alias = "reclaimable_page_segment_ids")]
    pub reclaimable_page_slab_ids: Vec<u64>,
    #[serde(alias = "blocked_page_segment_ids")]
    pub blocked_page_slab_ids: Vec<u64>,
    #[serde(alias = "live_page_segment_ids")]
    pub live_page_slab_ids: Vec<u64>,
    #[serde(alias = "manifest_page_segment_ids")]
    pub manifest_page_slab_ids: Vec<u64>,
    pub shared_store_cursor_count: usize,
    pub checkpoint_snapshot_floor: Option<u64>,
    pub raft_snapshot_install_floor: Option<u64>,
    pub delayed_destroy_grace_ms: u64,
    #[serde(default)]
    pub live_ref_block_count: usize,
    #[serde(default)]
    #[serde(rename = "slot_dump_manifest_block_count")]
    pub bucket_dump_manifest_block_count: usize,
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
    #[serde(alias = "segment")]
    pub slab: String,
    pub band: String,
    #[serde(rename = "slot")]
    pub bucket: String,
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
        compatibility_aliases.insert(text("wal"), text("AppendWatermark"));
        compatibility_aliases.insert(text("stream_blob"), text("Stream"));

        Self {
            page_address: text("BlockAddress"),
            block_address: text("BlockAddress"),
            page_index_entry: text("PageIndexEntry"),
            block_index_entry: text("BlockIndexEntry"),
            object_index_entry: text("ObjectIndexEntry"),
            storage_zone: text("StorageZone"),
            stream: text("Stream"),
            slab: text("Segment"),
            band: text("Band"),
            bucket: text("Slot"),
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
    #[serde(alias = "segment_fields")]
    pub slab_fields: Vec<String>,
    pub band_fields: Vec<String>,
    #[serde(rename = "slot_fields")]
    pub bucket_fields: Vec<String>,
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
                "shard_id", "zone_id", "block_id", "offset", "length", "checksum",
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
                "band",
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
            slab_fields: public_storage_strings(&[
                "segment_id",
                "band",
                "start_offset",
                "sealed",
                "generation",
            ]),
            band_fields: public_storage_strings(&[
                "band",
                "block_range",
                "reclaim_state",
                "generation",
            ]),
            bucket_fields: public_storage_strings(&[
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
    #[serde(default = "default_storage_watermark_snapshot")]
    pub storage_watermark_snapshot: StorageWatermarkSnapshot,
    #[serde(default = "default_storage_gc_snapshot")]
    pub storage_gc_snapshot: StorageGcSnapshot,
    #[serde(default = "default_storage_index_snapshot")]
    pub storage_index_snapshot: StorageIndexSnapshot,
    #[serde(default = "default_storage_topology_snapshot")]
    pub storage_topology_snapshot: StorageTopologySnapshot,
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
    pub dump_manifest: Option<BucketDumpManifest>,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    #[serde(default)]
    pub cache_warmup_page_refs: usize,
    #[serde(default)]
    pub cache_warmup: StorageCacheWarmupReport,
    #[serde(alias = "delayed_destroy_purged_segments")]
    pub delayed_destroy_purged_slabs: Vec<u64>,
    pub delayed_destroy_purged_bytes: u64,
    #[serde(default)]
    pub manifest_prune_plan: BucketDumpManifestPrunePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_prune_report: Option<BucketDumpManifestPruneReport>,
    #[serde(default)]
    pub install_roll_forward_reports: Vec<BucketDumpInstallRollForwardReport>,
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
            storage_watermark_snapshot: default_storage_watermark_snapshot(),
            storage_gc_snapshot: default_storage_gc_snapshot(),
            storage_index_snapshot: default_storage_index_snapshot(),
            storage_topology_snapshot: default_storage_topology_snapshot(),
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
            delayed_destroy_purged_slabs: Vec::new(),
            delayed_destroy_purged_bytes: 0,
            manifest_prune_plan: BucketDumpManifestPrunePlan::default(),
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
            flag(
                !self.manifest_prune_plan.prunable_manifest_ids.is_empty()
                    || !self
                        .manifest_prune_plan
                        .prunable_marker_manifest_ids
                        .is_empty(),
            ),
        );
        put(
            &mut metrics,
            "storage_manager_delayed_destroy_count",
            flag(
                !plan.delayed_destroy_page_slab_ids.is_empty()
                    || !self.delayed_destroy_purged_slabs.is_empty(),
            ),
        );
        put(
            &mut metrics,
            "storage_manager_follower_cursor_safety_count",
            flag(follower_retention_block_count > 0),
        );
        put(&mut metrics, "storage_manager_watermark_progress_count", 1);
        put(
            &mut metrics,
            "segment_open_count",
            plan.selected_dump_buckets.len() as u64,
        );
        put(
            &mut metrics,
            "segment_sealed_count",
            self.delayed_destroy_purged_slabs.len() as u64,
        );
        put(
            &mut metrics,
            "storage_zone_stale_bytes",
            plan.reclaimable_physical_bytes,
        );
        put(
            &mut metrics,
            "storage_zone_count",
            plan.live_page_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "active_storage_zones",
            plan.live_page_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "stream_segment_count",
            plan.live_page_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "slot_tombstone_count",
            object_lifecycle.tombstoned_object_ids,
        );
        put(
            &mut metrics,
            "slot_stale_ref_count",
            object_lifecycle
                .stale_object_ids
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
            plan.bucket_summaries
                .len()
                .max(plan.selected_dump_buckets.len()) as u64,
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
            plan.delayed_destroy_page_slab_ids.len() as u64,
        );
        put(
            &mut metrics,
            "follower_cursor_retention_floor",
            self.manifest_prune_plan
                .follower_blocks
                .iter()
                .map(|block| block.cursor_wal_sequence)
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
        self.storage_write_contract =
            default_storage_write_contract(&self.storage_lifecycle_metrics);
        self.storage_read_contract = default_storage_read_contract(&self.storage_lifecycle_metrics);
        self.storage_cold_scan_contract =
            default_storage_cold_scan_contract(&self.storage_lifecycle_metrics);
        self.storage_manager_contract =
            default_storage_manager_contract(&self.storage_lifecycle_metrics);
        self.storage_index_contract =
            default_storage_index_contract(&self.storage_lifecycle_metrics);
        self.storage_cache_contract =
            default_storage_cache_contract(&self.storage_lifecycle_metrics);
        self.storage_reclaim_contract =
            default_storage_reclaim_contract(&self.storage_lifecycle_metrics);
        self.storage_safety_snapshot =
            storage_safety_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_watermark_snapshot =
            storage_watermark_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_gc_snapshot =
            storage_gc_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_index_snapshot =
            storage_index_snapshot_from_metrics(&self.storage_lifecycle_metrics);
        self.storage_topology_snapshot =
            storage_topology_snapshot_from_metrics(&self.storage_lifecycle_metrics);
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
pub struct StorageAppendWatermarkSample {
    pub shard_id: ShardId,
    #[serde(rename = "slot_id")]
    pub bucket_id: u32,
    pub log_index: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCompactionWatermarkSample {
    pub shard_id: ShardId,
    pub safe_generation: u64,
    pub safe_timestamp_ms: u64,
    pub follower_floor: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWatermarkSnapshot {
    pub append_watermark: u64,
    pub compaction_watermark: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_safe_watermark: u64,
    pub page_index_rebuild_watermark: u64,
    pub block_index_rebuild_watermark: u64,
    pub object_index_rebuild_watermark: u64,
    #[serde(default)]
    pub append_watermark_samples: Vec<StorageAppendWatermarkSample>,
    #[serde(default)]
    pub compaction_watermark_samples: Vec<StorageCompactionWatermarkSample>,
}

pub fn storage_watermark_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageWatermarkSnapshot {
    let append_watermark = metric(metrics, "append_watermark");
    let compaction_watermark = metric(metrics, "compaction_watermark");
    let follower_cursor_retention_floor = metric(metrics, "follower_cursor_retention_floor");
    let follower_cursor_safe_watermark = if follower_cursor_retention_floor > 0 {
        compaction_watermark.min(follower_cursor_retention_floor)
    } else {
        compaction_watermark
    };
    StorageWatermarkSnapshot {
        append_watermark,
        compaction_watermark,
        follower_cursor_retention_floor,
        follower_cursor_safe_watermark,
        page_index_rebuild_watermark: metric(metrics, "page_index_rebuild_count"),
        block_index_rebuild_watermark: metric(metrics, "block_index_rebuild_count"),
        object_index_rebuild_watermark: metric(metrics, "object_index_rebuild_count"),
        append_watermark_samples: Vec::new(),
        compaction_watermark_samples: Vec::new(),
    }
}

pub fn default_storage_watermark_snapshot() -> StorageWatermarkSnapshot {
    storage_watermark_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTombstoneSample {
    #[serde(rename = "ref")]
    pub ref_id: String,
    pub generation: u64,
    pub deleted_at_ms: u64,
    pub reason: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGcEligibilitySample {
    #[serde(rename = "ref")]
    pub ref_id: String,
    pub eligible_after_ms: u64,
    pub has_tombstone: bool,
    pub follower_safe: bool,
    pub reclaimable_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageFollowerCursorSafetySample {
    pub min_follower_cursor: u64,
    pub blocked_reclaim_bytes: u64,
    pub safe_to_reclaim: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageGcSnapshot {
    pub tombstone_records: u64,
    pub stale_page_tombstones: u64,
    pub stale_block_tombstones: u64,
    pub gc_eligible_record_count: u64,
    pub reclaimable_bytes: u64,
    pub compaction_reclaimed_bytes: u64,
    pub physical_reclaimed_bytes: u64,
    pub physical_reclaim_errors: u64,
    pub follower_cursor_retention_floor: u64,
    pub follower_cursor_blocked_reclaim_count: u64,
    pub follower_cursor_safe_to_reclaim: bool,
    #[serde(default)]
    pub tombstone_samples: Vec<StorageTombstoneSample>,
    #[serde(default)]
    pub gc_eligibility_samples: Vec<StorageGcEligibilitySample>,
    #[serde(default)]
    pub follower_cursor_safety_samples: Vec<StorageFollowerCursorSafetySample>,
}

pub fn storage_gc_snapshot_from_metrics(metrics: &BTreeMap<String, u64>) -> StorageGcSnapshot {
    let stale_page_tombstones = metric(metrics, "stale_page_tombstones");
    let stale_block_tombstones = metric(metrics, "stale_block_tombstones");
    let tombstone_records = metric(metrics, "tombstone_records");
    let follower_cursor_blocked_reclaim_count = metric(metrics, "stale_pages_skipped")
        .saturating_add(metric(metrics, "stale_blocks_skipped"));
    StorageGcSnapshot {
        tombstone_records,
        stale_page_tombstones,
        stale_block_tombstones,
        gc_eligible_record_count: tombstone_records
            .saturating_add(stale_page_tombstones)
            .saturating_add(stale_block_tombstones),
        reclaimable_bytes: metric(metrics, "reclaimable_bytes"),
        compaction_reclaimed_bytes: metric(metrics, "compaction_reclaimed_bytes"),
        physical_reclaimed_bytes: metric(metrics, "physical_reclaimed_bytes"),
        physical_reclaim_errors: metric(metrics, "physical_reclaim_errors"),
        follower_cursor_retention_floor: metric(metrics, "follower_cursor_retention_floor"),
        follower_cursor_blocked_reclaim_count,
        follower_cursor_safe_to_reclaim: follower_cursor_blocked_reclaim_count == 0,
        tombstone_samples: Vec::new(),
        gc_eligibility_samples: Vec::new(),
        follower_cursor_safety_samples: Vec::new(),
    }
}

pub fn default_storage_gc_snapshot() -> StorageGcSnapshot {
    storage_gc_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageAddressSample {
    pub shard_id: u64,
    pub zone_id: u64,
    #[serde(alias = "segment_id")]
    pub slab_id: u64,
    pub page_id: u64,
    pub offset: u64,
    pub length: u64,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockAddressSample {
    pub shard_id: u64,
    pub zone_id: u64,
    pub block_id: u64,
    pub offset: u64,
    pub length: u64,
    pub checksum: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePageIndexEntrySample {
    pub logical_key: String,
    pub timestamp_range: Option<(u64, u64)>,
    pub page_addresses: Vec<StoragePageAddressSample>,
    pub append_watermark: u64,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBlockIndexEntrySample {
    pub page_address: StoragePageAddressSample,
    pub block_address: StorageBlockAddressSample,
    pub band: u64,
    pub checksum: String,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageObjectIndexEntrySample {
    pub model: String,
    pub table: String,
    pub object_key: String,
    pub page_chain: Vec<StoragePageAddressSample>,
    pub tombstone: bool,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexSnapshot {
    pub page_index_entry_count: u64,
    pub block_index_entry_count: u64,
    pub object_index_entry_count: u64,
    #[serde(rename = "slot_index_entry_count")]
    pub bucket_index_entry_count: u64,
    #[serde(rename = "slot_object_ref_count")]
    pub bucket_object_ref_count: u64,
    #[serde(rename = "slot_page_ref_count")]
    pub bucket_page_ref_count: u64,
    pub page_address_count: u64,
    pub unreadable_page_refs: u64,
    pub checksum_mismatches: u64,
    pub missing_owner_ref_count: u64,
    pub owner_mismatch_count: u64,
    pub restart_rebuild_verified: bool,
    #[serde(default)]
    pub page_index_entry_samples: Vec<StoragePageIndexEntrySample>,
    #[serde(default)]
    pub block_index_entry_samples: Vec<StorageBlockIndexEntrySample>,
    #[serde(default)]
    pub object_index_entry_samples: Vec<StorageObjectIndexEntrySample>,
}

pub fn storage_index_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageIndexSnapshot {
    StorageIndexSnapshot {
        page_index_entry_count: metric(metrics, "page_index_entry_count"),
        block_index_entry_count: metric(metrics, "block_index_entry_count"),
        object_index_entry_count: metric(metrics, "object_index_entry_count"),
        bucket_index_entry_count: metric(metrics, "slot_index_entry_count"),
        bucket_object_ref_count: metric(metrics, "slot_object_ref_count"),
        bucket_page_ref_count: metric(metrics, "slot_page_ref_count"),
        page_address_count: metric(metrics, "page_address_count"),
        unreadable_page_refs: metric(metrics, "unreadable_page_refs"),
        checksum_mismatches: metric(metrics, "checksum_mismatches"),
        missing_owner_ref_count: metric(metrics, "missing_owner_ref_count"),
        owner_mismatch_count: metric(metrics, "owner_mismatch_count"),
        restart_rebuild_verified: metric(metrics, "page_index_rebuild_count") > 0
            || metric(metrics, "block_index_rebuild_count") > 0
            || metric(metrics, "object_index_rebuild_count") > 0,
        page_index_entry_samples: Vec::new(),
        block_index_entry_samples: Vec::new(),
        object_index_entry_samples: Vec::new(),
    }
}

pub fn default_storage_index_snapshot() -> StorageIndexSnapshot {
    storage_index_snapshot_from_metrics(&default_storage_lifecycle_metrics())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageZoneSample {
    pub zone_id: u64,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub stale_bytes: u64,
    #[serde(alias = "segments")]
    pub slabs: Vec<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageStreamSample {
    pub stream_id: String,
    #[serde(alias = "segments")]
    pub slabs: Vec<u64>,
    pub rollover_count: u64,
    #[serde(alias = "sealed_segment_count")]
    pub sealed_slab_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSlabSample {
    #[serde(alias = "segment_id")]
    pub slab_id: u64,
    pub band: u64,
    pub start_offset: u64,
    pub sealed: bool,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBandSample {
    pub band: u64,
    pub block_range: Vec<u64>,
    pub reclaim_state: String,
    pub generation: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageBucketSample {
    #[serde(rename = "slot_id")]
    pub bucket_id: u32,
    pub dirty_generation: u64,
    pub object_refs: Vec<u64>,
    pub page_refs: Vec<StoragePageAddressSample>,
    pub tombstones: Vec<String>,
    pub owner_mismatch_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageTopologySnapshot {
    pub storage_zone_count: u64,
    pub active_storage_zones: u64,
    pub sealed_storage_zones: u64,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    #[serde(alias = "segment_open_count")]
    pub slab_open_count: u64,
    #[serde(alias = "segment_sealed_count")]
    pub slab_sealed_count: u64,
    pub delayed_destroy_backlog: u64,
    pub storage_zone_total_bytes: u64,
    pub storage_zone_used_bytes: u64,
    pub storage_zone_stale_bytes: u64,
    pub append_log_replay_records: u64,
    pub append_log_reclaimed_records: u64,
    #[serde(default)]
    pub storage_zone_samples: Vec<StorageZoneSample>,
    #[serde(default)]
    pub stream_samples: Vec<StorageStreamSample>,
    #[serde(default)]
    #[serde(alias = "segment_samples")]
    pub slab_samples: Vec<StorageSlabSample>,
    #[serde(default)]
    pub band_samples: Vec<StorageBandSample>,
    #[serde(default)]
    #[serde(rename = "slot_samples")]
    pub bucket_samples: Vec<StorageBucketSample>,
}

pub fn storage_topology_snapshot_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> StorageTopologySnapshot {
    StorageTopologySnapshot {
        storage_zone_count: metric(metrics, "storage_zone_count"),
        active_storage_zones: metric(metrics, "active_storage_zones"),
        sealed_storage_zones: metric(metrics, "sealed_storage_zones"),
        stream_slab_count: metric(metrics, "stream_segment_count"),
        slab_open_count: metric(metrics, "segment_open_count"),
        slab_sealed_count: metric(metrics, "segment_sealed_count"),
        delayed_destroy_backlog: metric(metrics, "delayed_destroy_backlog"),
        storage_zone_total_bytes: metric(metrics, "storage_zone_total_bytes"),
        storage_zone_used_bytes: metric(metrics, "storage_zone_used_bytes"),
        storage_zone_stale_bytes: metric(metrics, "storage_zone_stale_bytes"),
        append_log_replay_records: metric(metrics, "append_log_replay_records"),
        append_log_reclaimed_records: metric(metrics, "append_log_reclaimed_records"),
        storage_zone_samples: Vec::new(),
        stream_samples: Vec::new(),
        slab_samples: Vec::new(),
        band_samples: Vec::new(),
        bucket_samples: Vec::new(),
    }
}

pub fn default_storage_topology_snapshot() -> StorageTopologySnapshot {
    storage_topology_snapshot_from_metrics(&default_storage_lifecycle_metrics())
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
        "storage_zone_count",
        "active_storage_zones",
        "sealed_storage_zones",
        "stream_segment_count",
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
        TS_BLOCK_SLAB_TARGET_BYTES.to_string(),
        contract_u64(tuning.block_slab_target_bytes),
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
        contract_u64(tuning.effective_slab_target_bytes()),
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
    contract.insert("page_address".to_string(), contract_text("BlockAddress"));
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
    contract.insert(
        "page_writes".to_string(),
        contract_u64(metric(metrics, "page_writes")),
    );
    contract.insert(
        "block_writes".to_string(),
        contract_u64(metric(metrics, "block_writes")),
    );
    contract.insert(
        "bytes_written".to_string(),
        contract_u64(metric(metrics, "bytes_written")),
    );
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
    contract.insert(
        "object_index_entry".to_string(),
        contract_text("ObjectIndexEntry"),
    );
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
    contract.insert(
        "page_reads".to_string(),
        contract_u64(metric(metrics, "page_reads")),
    );
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
    contract.insert(
        "no_cache_page_reads".to_string(),
        contract_u64(no_cache_reads),
    );
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
    contract.insert(
        "cold_scan_no_cache_reads".to_string(),
        contract_u64(no_cache_reads),
    );
    contract.insert(
        "cold_scan_page_index_scan_count".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_count")),
    );
    contract.insert(
        "cold_scan_page_index_scan_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_page_index_scan_ms")),
    );
    contract.insert(
        "cold_scan_page_reads".to_string(),
        contract_u64(no_cache_reads),
    );
    contract.insert(
        "cold_scan_decode_records_ms".to_string(),
        contract_u64(metric(metrics, "cold_scan_decode_records_ms")),
    );
    contract.insert(
        "cold_scan_records_decoded".to_string(),
        contract_u64(decoded),
    );
    contract.insert(
        "cold_scan_records_returned".to_string(),
        contract_u64(returned),
    );
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
        (
            "watermark_progress",
            "storage_manager_watermark_progress_count",
        ),
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
    contract.insert(
        "native_public_name".to_string(),
        contract_text("StorageManager"),
    );
    contract.insert(
        "rust_public_name".to_string(),
        contract_text("StoreManager"),
    );
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
    contract.insert(
        "loop_metric".to_string(),
        contract_text("storage_manager_loop_ms"),
    );
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
    contract.insert(
        "page_address_codec".to_string(),
        contract_text("BlockAddress"),
    );
    contract.insert(
        "block_address_codec".to_string(),
        contract_text("BlockAddress"),
    );
    contract.insert(
        "stable_order".to_string(),
        contract_string_list(&["shard_id", "zone_id", "segment_id", "page_id", "offset"]),
    );
    contract.insert(
        "slot_index".to_string(),
        contract_text("slot -> object/page refs"),
    );
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
    contract.insert(
        "page_address_encode_decode".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "block_address_encode_decode".to_string(),
        contract_bool(true),
    );
    contract.insert("stable_order_verified".to_string(), contract_bool(true));
    contract.insert(
        "timestamp_range_lookup_verified".to_string(),
        contract_bool(true),
    );
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
    contract.insert(
        "append_watermark_invalidation".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "compaction_watermark_invalidation".to_string(),
        contract_bool(true),
    );
    contract.insert("cold_scan_no_promote".to_string(), contract_bool(true));
    contract.insert(
        "writeback_backpressure_measured".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "cache_refills".to_string(),
        contract_u64(metric(metrics, "cache_refills")),
    );
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
    contract.insert(
        "cache_eviction_frees_memory_only".to_string(),
        contract_bool(true),
    );
    contract.insert(
        "logical_gc_marks_expired_deletable".to_string(),
        contract_bool(true),
    );
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
    #[serde(rename = "durable_slot_generation_frontier_wal_sequence")]
    pub durable_bucket_generation_frontier_wal_sequence: u64,
    #[serde(rename = "durable_slot_generation_frontier_index_log_sequence")]
    pub durable_bucket_generation_frontier_index_log_sequence: u64,
    #[serde(rename = "retain_from_wal_sequence")]
    pub retain_from_wal_sequence: u64,
    pub retain_from_index_log_sequence: u64,
    #[serde(rename = "current_wal_sequence")]
    pub current_wal_sequence: u64,
    pub current_index_log_sequence: u64,
    #[serde(rename = "covered_slot_count")]
    pub covered_bucket_count: usize,
    #[serde(rename = "uncovered_slot_count")]
    pub uncovered_bucket_count: usize,
    pub follower_cursor_block_count: usize,
    pub raft_snapshot_block_count: usize,
    #[serde(rename = "missing_slot_generations")]
    pub missing_bucket_generations: Vec<u32>,
    pub retained_manifest_ids: Vec<String>,
    pub blocker_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageWalReclaimReport {
    pub plan: StorageWalReclaimPlan,
    pub applied: bool,
    #[serde(rename = "wal_records_removed")]
    pub wal_records_removed: usize,
    pub index_log_records_removed: usize,
    #[serde(rename = "wal_bytes_before")]
    pub wal_bytes_before: u64,
    #[serde(rename = "wal_bytes_after")]
    pub wal_bytes_after: u64,
    pub index_log_bytes_before: u64,
    pub index_log_bytes_after: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageIndexGcReport {
    pub shard_id: ShardId,
    pub enabled: bool,
    pub applied: bool,
    #[serde(rename = "dirty_slots_committed_before_truncate")]
    pub dirty_buckets_committed_before_truncate: bool,
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

/// What one threshold catalog dump reclaimed from the shard's logs (embedded path).
///
/// After [`dump_index_catalog`](crate::TemporalEngine::dump_index_catalog) durably
/// materializes the base index at `wal_anchor` and folds the catalog anchor, everything the
/// base reflects is redundant: index-log records whose own WAL anchor is at or below
/// `wal_anchor`, and WAL records at or below it (clamped by the block-retention floor, which
/// pins any record still holding the only copy of a served page).
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogDumpReclaimReport {
    pub shard_id: ShardId,
    pub wal_anchor: u64,
    pub index_log_records_removed: usize,
    pub index_log_bytes_before: u64,
    pub index_log_bytes_after: u64,
    pub wal_records_removed: usize,
    pub wal_bytes_before: u64,
    pub wal_bytes_after: u64,
    /// The block-retention floor in force during the WAL sweep (`None` = unconstrained): the
    /// lowest WAL sequence a live block-in-WAL registration still depends on.
    pub wal_retention_floor: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageEvictionVictim {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub object_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub dirty_object_count: u64,
    pub weight: u64,
    /// Wall-clock ms this bucket was last read or written (0 == never touched).
    /// Eviction victims are ranked least-recently-used first.
    #[serde(default)]
    pub last_touched_ms: u64,
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
pub struct StorageMergedDumpLoadPolicyRequest {
    pub lifecycle: StorageLifecycleRequest,
    #[serde(default)]
    pub create_dump_manifest: bool,
    #[serde(default)]
    pub install_dump_manifest: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageMergedDumpLoadPolicyReport {
    pub shard_id: ShardId,
    pub policy_ready: bool,
    pub dump_manifest_created: bool,
    pub load_preflight_safe: bool,
    pub load_installed: bool,
    pub replay_boundary_safe: bool,
    pub manifest_chain_valid: bool,
    pub follower_retention_safe: bool,
    pub index_gc_ready: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest_id: Option<String>,
    #[serde(rename = "manifest_slot_ids")]
    pub manifest_bucket_ids: Vec<u32>,
    #[serde(alias = "manifest_page_segment_ids")]
    pub manifest_page_slab_ids: Vec<u64>,
    #[serde(rename = "manifest_wal_sequence")]
    pub manifest_wal_sequence: u64,
    pub manifest_index_log_sequence: u64,
    #[serde(rename = "selected_replay_wal_sequence")]
    pub selected_replay_wal_sequence: u64,
    pub selected_replay_index_log_sequence: u64,
    pub lifecycle: StorageLifecycleReport,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_preflight: Option<BucketDumpInstallPreflightReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub install_status: Option<Status>,
    pub boundary: StorageRecoveryBoundaryReport,
    pub manifest_prune_plan: BucketDumpManifestPrunePlan,
    pub install_roll_forward_reports: Vec<BucketDumpInstallRollForwardReport>,
    pub evidence: Vec<String>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheWarmupReport {
    pub shard_id: ShardId,
    #[serde(rename = "selected_slots")]
    pub selected_buckets: Vec<u32>,
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
    #[serde(rename = "latest_safe_wal_sequence")]
    pub latest_safe_wal_sequence: u64,
    pub latest_safe_index_log_sequence: u64,
    #[serde(rename = "latest_dump_wal_sequence")]
    pub latest_dump_wal_sequence: u64,
    pub latest_dump_index_log_sequence: u64,
    #[serde(rename = "selected_replay_wal_sequence")]
    pub selected_replay_wal_sequence: u64,
    pub selected_replay_index_log_sequence: u64,
    #[serde(alias = "orphan_page_segment_ids")]
    pub orphan_page_slab_ids: Vec<u64>,
    #[serde(rename = "missing_dump_slot_ids")]
    pub missing_dump_bucket_ids: Vec<u32>,
    pub stale_index_page_refs: Vec<StorageRecoveryPageError>,
    #[serde(default)]
    #[serde(rename = "interrupted_slot_dump_installs")]
    pub interrupted_bucket_dump_installs: Vec<BucketDumpInstallMarker>,
    #[serde(default)]
    #[serde(rename = "prepared_slot_dump_install_count")]
    pub prepared_bucket_dump_install_count: usize,
    #[serde(default)]
    #[serde(rename = "installed_slot_dump_install_count")]
    pub installed_bucket_dump_install_count: usize,
    #[serde(default)]
    #[serde(rename = "unknown_slot_dump_install_count")]
    pub unknown_bucket_dump_install_count: usize,
    #[serde(default)]
    pub manifest_chain_issues: Vec<BucketDumpManifestChainIssue>,
    #[serde(default)]
    pub owner_mismatch_page_refs: Vec<StorageRecoveryPageOwnerMismatch>,
    #[serde(default)]
    pub missing_owner_page_refs: usize,
    #[serde(default)]
    pub object_lifecycle: StorageObjectLifecycleReport,
    #[serde(alias = "corrupt_page_segment_ids")]
    pub corrupt_page_slab_ids: Vec<u64>,
    pub unreadable_page_bytes: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLifecycleRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    #[serde(rename = "selected_dump_slots")]
    pub selected_dump_buckets: Vec<u32>,
    #[serde(default)]
    #[serde(rename = "max_dump_slots_per_round")]
    pub max_dump_buckets_per_round: usize,
    #[serde(rename = "min_undumped_wal_records", default)]
    pub min_undumped_wal_records: u64,
    #[serde(default)]
    pub purge_delayed_destroy: bool,
    #[serde(default)]
    #[serde(rename = "prune_slot_dump_manifests")]
    pub prune_bucket_dump_manifests: bool,
    #[serde(default)]
    #[serde(rename = "roll_forward_slot_dump_installs")]
    pub roll_forward_bucket_dump_installs: bool,
    #[serde(default)]
    pub follower_replay_cursors: Vec<BucketDumpFollowerReplayCursor>,
    #[serde(default)]
    pub page_gc_shared_store_cursors: Vec<StoragePageGcReplayCursor>,
    #[serde(default)]
    pub page_gc_raft_snapshot_refs: Vec<BucketDumpRaftSnapshotRef>,
    #[serde(default)]
    #[serde(alias = "page_gc_checkpoint_floor_segment_id")]
    pub page_gc_checkpoint_floor_slab_id: Option<u64>,
    #[serde(default)]
    #[serde(alias = "page_gc_raft_install_floor_segment_id")]
    pub page_gc_raft_install_floor_slab_id: Option<u64>,
    #[serde(default)]
    pub page_gc_delayed_destroy_grace_ms: u64,
    #[serde(default)]
    pub invalidate_cache: bool,
    #[serde(default)]
    pub warm_cache: bool,
}

/// Index-log truncation waits for dirty buckets by default; see the field this serves.
fn commit_dirty_buckets_before_truncation_default() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerCycleRequest {
    pub shard_id: ShardId,
    /// Dump exactly these buckets instead of the dirty ones.
    ///
    /// Reclaim wants a dump manifest matching each bucket's current generation; a bucket that
    /// went clean without ever being dumped at that generation has none, and the dirty-only
    /// cadence will never dump it again. Naming buckets here is the only way to clear that.
    #[serde(default)]
    pub selected_dump_buckets: Vec<u32>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_prepare: bool,
    #[serde(rename = "enable_wal_reclaim", default = "default_storage_manager_stage_enabled")]
    pub enable_wal_reclaim: bool,
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
    #[serde(rename = "max_dump_slots_per_round")]
    pub max_dump_buckets_per_round: usize,
    #[serde(rename = "min_undumped_wal_records", default)]
    pub min_undumped_wal_records: u64,
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
    #[serde(rename = "max_expire_hot_slots_per_round")]
    pub max_expire_hot_buckets_per_round: usize,
    #[serde(default)]
    #[serde(rename = "max_expire_cold_slots_per_round")]
    pub max_expire_cold_buckets_per_round: usize,
    #[serde(default)]
    pub expire_hot_cursor: Option<String>,
    #[serde(default)]
    pub expire_cold_cursor: Option<String>,
    #[serde(default)]
    #[serde(rename = "load_cold_slots_for_expire")]
    pub load_cold_buckets_for_expire: bool,
    #[serde(default)]
    pub follower_replay_cursors: Vec<BucketDumpFollowerReplayCursor>,
    #[serde(default)]
    pub raft_snapshot_refs: Vec<BucketDumpRaftSnapshotRef>,
    #[serde(default)]
    pub page_gc_shared_store_cursors: Vec<StoragePageGcReplayCursor>,
    #[serde(default)]
    #[serde(alias = "page_gc_checkpoint_floor_segment_id")]
    pub page_gc_checkpoint_floor_slab_id: Option<u64>,
    #[serde(default)]
    #[serde(alias = "page_gc_raft_install_floor_segment_id")]
    pub page_gc_raft_install_floor_slab_id: Option<u64>,
    #[serde(default)]
    pub page_gc_delayed_destroy_grace_ms: u64,
    #[serde(default)]
    pub index_gc_index_log_bytes_threshold: u64,
    #[serde(default)]
    pub index_gc_usage_ratio_trigger_basis_points: u64,
    #[serde(default)]
    pub index_gc_max_entries_per_round: usize,
    /// Whether index-log truncation waits for the buckets it describes to be dumped.
    ///
    /// Defaulted explicitly rather than with a bare `#[serde(default)]`: on a bool that decodes an
    /// ABSENT field to `false`, which is the unsafe order, regardless of what this type's own
    /// `Default` says. The request is parsed from a request body, so an absent field is what every
    /// caller who does not name it gets -- and discarding index-log records before the buckets
    /// they describe are durable loses exactly the record needed to rebuild them.
    #[serde(default = "commit_dirty_buckets_before_truncation_default")]
    #[serde(rename = "index_gc_commit_dirty_slots_before_truncation")]
    pub index_gc_commit_dirty_buckets_before_truncation: bool,
    /// Reclaim only bands whose garbage ratio is at least this many basis points
    /// (garbage = 10_000 - band live-fraction). 0 reclaims every eligible band
    /// (today's behavior). The garbage-ratio GC gate, expressed against bands.
    #[serde(default)]
    pub page_gc_min_band_garbage_basis_points: u64,
}

impl Default for StorageManagerCycleRequest {
    fn default() -> Self {
        Self {
            shard_id: 0,
            selected_dump_buckets: Vec::new(),
            dry_run: false,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_evict: true,
            enable_expire: true,
            enable_page_reclaim: true,
            enable_page_compaction: true,
            enable_index_gc: true,
            max_dump_buckets_per_round: 0,
            min_undumped_wal_records: 0,
            warm_cache: false,
            eviction_memory_pressure_threshold: default_storage_manager_eviction_threshold(),
            eviction_batch_limit: 0,
            eviction_dump_before_evict: false,
            eviction_delete_drop: false,
            max_expire_hot_buckets_per_round: 0,
            max_expire_cold_buckets_per_round: 0,
            expire_hot_cursor: None,
            expire_cold_cursor: None,
            load_cold_buckets_for_expire: false,
            follower_replay_cursors: Vec::new(),
            raft_snapshot_refs: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_checkpoint_floor_slab_id: None,
            page_gc_raft_install_floor_slab_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            index_gc_index_log_bytes_threshold: 0,
            index_gc_usage_ratio_trigger_basis_points: 0,
            index_gc_max_entries_per_round: 0,
            index_gc_commit_dirty_buckets_before_truncation: true,
            page_gc_min_band_garbage_basis_points: 0,
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
    #[serde(rename = "selected_slots")]
    pub selected_buckets: Vec<u32>,
    #[serde(default)]
    #[serde(alias = "selected_page_segment_ids")]
    pub selected_page_slab_ids: Vec<u64>,
    #[serde(default)]
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    #[serde(rename = "undumped_wal_records", default)]
    pub undumped_wal_records: u64,
    #[serde(default)]
    #[serde(rename = "dumped_slot_count")]
    pub dumped_bucket_count: usize,
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
    #[serde(alias = "page_segments_reclaimed")]
    pub page_slabs_reclaimed: usize,
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
    #[serde(alias = "compacted_page_segment_id")]
    pub compacted_page_slab_id: Option<u64>,
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
    #[serde(rename = "metrics_slot_count")]
    pub metrics_bucket_count: usize,
    #[serde(default)]
    pub metrics_page_ref_count: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageManagerPressureSignals {
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    pub undumped_wal_records: u64,
    pub wal_bytes: u64,
    pub index_log_bytes: u64,
    pub stale_page_bytes: u64,
    pub live_page_bytes: u64,
    #[serde(alias = "page_segment_stale_density_basis_points")]
    pub page_slab_stale_density_basis_points: u64,
    pub memory_cache_bytes: u64,
    pub disk_cache_bytes: u64,
    pub memory_cache_pressure_score: u64,
    #[serde(rename = "expired_slot_object_scan_debt")]
    pub expired_bucket_object_scan_debt: usize,
    #[serde(alias = "delayed_destroy_segment_count")]
    pub delayed_destroy_slab_count: usize,
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
    /// How long the whole round took. The per-stage `duration_ms` values are each stage's own
    /// time and tile this; they used to all be a copy of this number.
    #[serde(default)]
    pub duration_ms: u64,
    pub dry_run: bool,
    pub native_stage_order: Vec<String>,
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
    #[serde(rename = "slot_object_page_authority_ready")]
    pub bucket_object_page_authority_ready: bool,
    #[serde(rename = "slot_store_layout_api_ready")]
    pub bucket_store_layout_api_ready: bool,
    pub object_manager_runtime_api_ready: bool,
    pub block_address_api_ready: bool,
    #[serde(alias = "block_store_segment_api_ready")]
    pub block_store_slab_api_ready: bool,
    pub stream_backed_band_api_ready: bool,
    pub legacy_page_zone_aliases_ready: bool,
    pub storage_manager_phase_api_ready: bool,
    pub storage_manager_pressure_api_ready: bool,
    pub storage_manager_merged_dump_load_api_ready: bool,
    #[serde(rename = "slot_count")]
    pub bucket_count: usize,
    pub page_index_count: usize,
    pub block_index_count: u64,
    pub stream_band_count: u64,
    pub stream_record_count: u64,
    pub storage_manager_stage_order: Vec<String>,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
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
    #[serde(rename = "max_dirty_slots")]
    pub max_dirty_buckets: Option<usize>,
    #[serde(default)]
    #[serde(alias = "max_stale_page_segments")]
    pub max_stale_page_slabs: Option<usize>,
    #[serde(default)]
    #[serde(alias = "max_orphan_page_segments")]
    pub max_orphan_page_slabs: Option<usize>,
    #[serde(rename = "max_undumped_wal_records", default)]
    pub max_undumped_wal_records: Option<u64>,
    #[serde(default)]
    #[serde(rename = "require_slot_dump_manifest")]
    pub require_bucket_dump_manifest: bool,
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
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    #[serde(alias = "stale_page_segment_count")]
    pub stale_page_slab_count: usize,
    #[serde(alias = "orphan_page_segment_count")]
    pub orphan_page_slab_count: usize,
    #[serde(rename = "undumped_wal_records", default)]
    pub undumped_wal_records: u64,
    #[serde(alias = "corrupt_page_segment_count")]
    pub corrupt_page_slab_count: usize,
    pub unreadable_page_ref_count: usize,
    pub owner_mismatch_page_ref_count: usize,
    pub missing_owner_page_ref_count: u64,
    pub reused_object_id_conflict_count: u64,
    #[serde(rename = "interrupted_slot_dump_install_count")]
    pub interrupted_bucket_dump_install_count: usize,
    #[serde(default)]
    #[serde(rename = "prepared_slot_dump_install_count")]
    pub prepared_bucket_dump_install_count: usize,
    #[serde(default)]
    #[serde(rename = "installed_slot_dump_install_count")]
    pub installed_bucket_dump_install_count: usize,
    #[serde(default)]
    #[serde(rename = "unknown_slot_dump_install_count")]
    pub unknown_bucket_dump_install_count: usize,
    #[serde(rename = "slot_dump_manifest_count")]
    pub bucket_dump_manifest_count: usize,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub page_store_bytes_written: u64,
    #[serde(default)]
    pub block_store_bytes_written: u64,
    pub boundary: StorageRecoveryBoundaryReport,
    pub object_lifecycle: StorageObjectLifecycleReport,
    #[serde(default)]
    #[serde(alias = "segment_integrity")]
    pub slab_integrity: StorageSlabIntegrityReport,
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
    #[serde(rename = "wal_format")]
    pub wal_format: String,
    pub index_log_format: String,
    #[serde(default)]
    pub compatibility_mode: String,
    #[serde(default)]
    pub migration_required: bool,
    #[serde(default)]
    pub native_reader_supported: bool,
    #[serde(default)]
    pub native_writer_supported: bool,
    #[serde(default)]
    pub golden_conversion_required: bool,
    pub rust_native_replay_safe: bool,
    pub native_binary_compatible: bool,
    #[serde(rename = "wal_last_sequence")]
    pub wal_last_sequence: u64,
    pub index_log_last_sequence: u64,
    #[serde(rename = "wal_records")]
    pub wal_records: usize,
    pub index_log_records: usize,
    #[serde(rename = "wal_bytes")]
    pub wal_bytes: u64,
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
    pub native_page_header_reader_supported: bool,
    #[serde(default)]
    pub native_page_header_writer_supported: bool,
    #[serde(default)]
    pub golden_conversion_required: bool,
    pub rust_native_read_safe: bool,
    pub native_page_header_compatible: bool,
    pub checksum_protected: bool,
    pub object_ids_embedded: bool,
    #[serde(rename = "routing_slots_embedded")]
    pub routing_buckets_embedded: bool,
    pub compression_supported: bool,
    pub active_bands: u64,
    pub sealed_bands: u64,
    pub delayed_destroy_bands: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub page_store_writes: u64,
    pub page_store_bytes_written: u64,
    pub logical_bytes_written: u64,
    pub compressed_records_written: u64,
    pub compatibility_gaps: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheBucketSummary {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
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
    #[serde(rename = "slot_summaries")]
    pub bucket_summaries: Vec<StorageCacheBucketSummary>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageCacheInvalidateBucketRequest {
    pub shard_id: ShardId,
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenCaseReport {
    pub name: String,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenCorpusReport {
    pub corpus: String,
    pub total_cases: usize,
    pub passed_cases: usize,
    pub failed_cases: usize,
    pub cases: Vec<GoldenCaseReport>,
}

impl GoldenCorpusReport {
    pub fn passed(&self) -> bool {
        self.failed_cases == 0 && self.total_cases == self.passed_cases
    }
}


#[cfg(test)]
mod manifest_field_name_tests {
    use super::{BucketStorageSummary, SummaryNamed};

    /// The writer this replaced: the derive, with the long names it emitted.
    ///
    /// Kept as the specification. A hand-written writer that produced a DIFFERENT document under
    /// the default would change every manifest silently, which is the one thing this change must
    /// not do -- the whole point is that nothing moves until an operator asks.
    #[derive(serde::Serialize)]
    struct Derived {
        #[serde(rename = "routing_slot")]
        routing_bucket: u32,
        object_count: u64,
        page_ref_count: u64,
        logical_bytes: u64,
        physical_bytes: u64,
        dirty_object_count: u64,
        dirty_generation: u64,
        last_dump_sequence: u64,
        page_slab_ids: Vec<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_compacted_zone: Option<u64>,
    }

    fn derived_of(summary: &BucketStorageSummary) -> Derived {
        Derived {
            routing_bucket: summary.routing_bucket,
            object_count: summary.object_count,
            page_ref_count: summary.page_ref_count,
            logical_bytes: summary.logical_bytes,
            physical_bytes: summary.physical_bytes,
            dirty_object_count: summary.dirty_object_count,
            dirty_generation: summary.dirty_generation,
            last_dump_sequence: summary.last_dump_sequence,
            page_slab_ids: summary.page_slab_ids.clone(),
            last_compacted_zone: summary.last_compacted_zone,
        }
    }

    fn cases() -> Vec<(&'static str, BucketStorageSummary)> {
        let full = BucketStorageSummary {
            routing_bucket: 8539,
            object_count: 12,
            page_ref_count: 34,
            logical_bytes: 4096,
            physical_bytes: 8192,
            dirty_object_count: 3,
            dirty_generation: 77,
            last_dump_sequence: 909,
            page_slab_ids: vec![1, 2, 3],
            last_compacted_zone: Some(5),
        };
        let mut no_zone = full.clone();
        no_zone.last_compacted_zone = None;
        let mut empty_slabs = full.clone();
        empty_slabs.page_slab_ids = Vec::new();
        vec![
            ("fully populated", full),
            ("no compacted zone", no_zone),
            ("no slab ids", empty_slabs),
            ("all defaults", BucketStorageSummary::default()),
        ]
    }

    /// Off by default: the long spelling is byte for byte what the derive produced.
    #[test]
    fn the_default_spelling_is_unchanged() {
        for (label, summary) in cases() {
            let ours = serde_json::to_vec(&SummaryNamed(&summary, false)).expect("ours");
            let theirs = serde_json::to_vec(&derived_of(&summary)).expect("theirs");
            assert_eq!(
                String::from_utf8_lossy(&ours),
                String::from_utf8_lossy(&theirs),
                "default spelling changed for {label}"
            );
        }
    }

    /// The short spelling reads back as the same summary, because every reader learned the
    /// aliases first. This is the property that makes the switch safe to turn on.
    #[test]
    fn the_short_spelling_round_trips() {
        for (label, summary) in cases() {
            let short = serde_json::to_vec(&SummaryNamed(&summary, true)).expect("short");
            let back: BucketStorageSummary =
                serde_json::from_slice(&short).unwrap_or_else(|err| {
                    panic!("short form did not parse for {label}: {err}");
                });
            assert_eq!(back, summary, "short form lost something for {label}");

            // And the long form still reads, so a directory holding both loads either way.
            let long = serde_json::to_vec(&SummaryNamed(&summary, false)).expect("long");
            let back_long: BucketStorageSummary =
                serde_json::from_slice(&long).expect("long form parses");
            assert_eq!(back_long, summary, "long form lost something for {label}");
        }
    }

    /// The writer emits the short spelling, and nothing can ask it not to.
    ///
    /// Every other test here names the spelling explicitly through `serialize_named`, so all of
    /// them would pass whichever one `Serialize` chose. This is the only test that exercises the
    /// choice `Serialize` actually makes, which is why it asserts the BYTES rather than a
    /// setting -- there is no longer a setting to read.
    #[test]
    fn the_default_is_now_the_short_spelling() {
        let written = serde_json::to_vec(&BucketStorageSummary::default()).expect("serialize");
        let text = String::from_utf8_lossy(&written);
        assert!(
            text.contains("\"rs\""),
            "the default write should use the short names, got {text}"
        );
        assert!(
            !text.contains("routing_slot"),
            "the default write should not use the long names, got {text}"
        );
    }

    /// And it is actually smaller -- the reason for the change.
    #[test]
    fn the_short_spelling_is_smaller() {
        let summary = cases()
            .into_iter()
            .find(|(label, _)| *label == "fully populated")
            .expect("the populated case")
            .1;
        let long = serde_json::to_vec(&SummaryNamed(&summary, false)).expect("long");
        let short = serde_json::to_vec(&SummaryNamed(&summary, true)).expect("short");
        let saved = 100.0 * (long.len() - short.len()) as f64 / long.len() as f64;
        println!(
            "  MANIFEST one summary: long {} B, short {} B, saved {saved:.1}%",
            long.len(),
            short.len()
        );
        assert!(
            short.len() < long.len(),
            "short {} is not smaller than long {}",
            short.len(),
            long.len()
        );
        // A saving this small would mean the names were not what cost the bytes.
        assert!(saved > 25.0, "expected the names to be worth more than 25%, got {saved:.1}%");
    }
}
