// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage_config::{effective_block_slab_target_bytes, storage_zone_size_bytes};

mod paths;
mod band_manifest;
mod band_reports;
mod append;
mod read;
mod gc;
mod slab_ids;
mod record;

use paths::{
    delayed_destroy_dir, delayed_destroy_path, band_manifest_path, file_created_unix_ms,
    file_modified_unix_ms, legacy_zone_manifest_path, now_unix_ms, slab_path, sync_dir,
    sync_parent_dir, system_time_unix_ms,
};
use record::{
    decode_page_record, default_page_record_compression_enabled,
    default_page_record_compression_level, default_page_record_compression_min_bytes,
    encode_page_record, inspect_slab, logical_range_from_slab, sha256_hex, summarize_slab,
    PageRecordCompression,
};
use self::band_manifest::*;
pub(crate) use slab_ids::*;
#[cfg(test)]
use record::{
    PAGE_RECORD_COMPRESSION_NONE, PAGE_RECORD_COMPRESSION_ZSTD, PAGE_RECORD_HEADER_LEN,
    PAGE_RECORD_MAGIC, PAGE_RECORD_VERSION,
};

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "block checksum mismatch for segment {page_slab_id} offset {offset} length {length}: expected {expected}, got {actual}"
    )]
    ChecksumMismatch {
        page_slab_id: u64,
        offset: u64,
        length: u64,
        expected: String,
        actual: String,
    },
    #[error("corrupt block envelope for segment {page_slab_id} offset {offset}: {reason}")]
    CorruptPageEnvelope {
        page_slab_id: u64,
        offset: u64,
        reason: String,
    },
}

/// Which optional parts an address carries.
///
/// Five `Option`s cost 72 bytes to wrap values of 8, 8, 4, 8 and 8. A `u64` has no spare bit
/// pattern to mean "absent", so each one pays a whole extra word for its tag, and every page in
/// the index holds an address for the life of the shard.
///
/// A sentinel would be cheaper and is NOT available here: `0` is a legitimate `band_id` and a
/// legitimate `routing_slot` -- there is a test asserting `band_id == Some(0)` -- so "zero means
/// absent" would silently erase real values. A byte of presence bits costs almost nothing and
/// cannot make that mistake.
///
/// This is the shape the design being followed uses: one byte carrying `dirty`, `page_in_log` and
/// its reserved bits, rather than an optional wrapped around each.
const ADDRESS_HAS_PAGE_ID: u8 = 1 << 0;
const ADDRESS_HAS_OBJECT_ID: u8 = 1 << 1;
const ADDRESS_HAS_ROUTING_BUCKET: u8 = 1 << 2;
const ADDRESS_HAS_GENERATION: u8 = 1 << 3;
const ADDRESS_HAS_BAND_ID: u8 = 1 << 4;

/// The address as it travels on the wire and on disk -- unchanged, field names, renames and
/// aliases included.
///
/// `BlockAddress` converts through this both ways, which is what keeps the packing an in-memory
/// concern rather than a format change: an index written before this loads, and one written after
/// stays readable by anything expecting the old shape.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BlockAddressWire {
    #[serde(rename = "page_segment_id")]
    page_slab_id: u64,
    offset: u64,
    length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "routing_slot")]
    routing_bucket: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    generation: Option<u64>,
    #[serde(
        default,
        alias = "extent_id",
        alias = "zone_id",
        skip_serializing_if = "Option::is_none"
    )]
    band_id: Option<u64>,
    #[serde(
        default,
        alias = "checksum",
        skip_serializing_if = "Option::is_none",
        with = "hex_digest"
    )]
    sha256: Option<[u8; 32]>,
}

impl From<BlockAddressWire> for BlockAddress {
    fn from(wire: BlockAddressWire) -> Self {
        BlockAddress::from_parts(
            wire.page_slab_id,
            wire.offset,
            wire.length,
            wire.page_id,
            wire.object_id,
            wire.routing_bucket,
            wire.generation,
            wire.band_id,
        )
    }
}

impl From<BlockAddress> for BlockAddressWire {
    fn from(address: BlockAddress) -> Self {
        Self {
            page_slab_id: address.page_slab_id,
            offset: address.offset,
            length: address.length,
            page_id: address.page_id(),
            object_id: address.object_id(),
            routing_bucket: address.routing_bucket(),
            generation: address.generation(),
            band_id: address.band_id(),
            // The index no longer holds a digest, so it cannot write one. An index written
            // before this still LOADS -- the field is accepted and ignored -- but one written
            // now omits it. That is a content change, not a schema change: the field was always
            // optional, and the page envelope carries the digest that verifies the bytes.
            sha256: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "BlockAddressWire", into = "BlockAddressWire")]
pub struct BlockAddress {
    pub page_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    page_id: u64,
    object_id: u64,
    generation: u64,
    band_id: u64,
    routing_bucket: u32,
    /// Which of the five above are actually set. See `ADDRESS_HAS_*`.
    present: u8,
}

impl BlockAddress {
    /// Build an address from the parts a caller has. The presence bits are derived here so no
    /// caller has to know they exist.
    ///
    /// There is deliberately no digest parameter: the index does not hold one, and a parameter the
    /// constructor discarded would invite a caller to pass a freshly computed digest believing it
    /// was kept. The page envelope carries the digest that a read verifies against.
    #[allow(clippy::too_many_arguments)]
    pub fn from_parts(
        page_slab_id: u64,
        offset: u64,
        length: u64,
        page_id: Option<u64>,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        generation: Option<u64>,
        band_id: Option<u64>,
    ) -> Self {
        let mut present = 0u8;
        if page_id.is_some() {
            present |= ADDRESS_HAS_PAGE_ID;
        }
        if object_id.is_some() {
            present |= ADDRESS_HAS_OBJECT_ID;
        }
        if routing_bucket.is_some() {
            present |= ADDRESS_HAS_ROUTING_BUCKET;
        }
        if generation.is_some() {
            present |= ADDRESS_HAS_GENERATION;
        }
        if band_id.is_some() {
            present |= ADDRESS_HAS_BAND_ID;
        }
        Self {
            page_slab_id,
            offset,
            length,
            page_id: page_id.unwrap_or_default(),
            object_id: object_id.unwrap_or_default(),
            generation: generation.unwrap_or_default(),
            band_id: band_id.unwrap_or_default(),
            routing_bucket: routing_bucket.unwrap_or_default(),
            present,
        }
    }

    pub fn page_id(&self) -> Option<u64> {
        (self.present & ADDRESS_HAS_PAGE_ID != 0).then_some(self.page_id)
    }

    pub fn object_id(&self) -> Option<u64> {
        (self.present & ADDRESS_HAS_OBJECT_ID != 0).then_some(self.object_id)
    }

    pub fn routing_bucket(&self) -> Option<u32> {
        (self.present & ADDRESS_HAS_ROUTING_BUCKET != 0).then_some(self.routing_bucket)
    }

    pub fn generation(&self) -> Option<u64> {
        (self.present & ADDRESS_HAS_GENERATION != 0).then_some(self.generation)
    }

    pub fn band_id(&self) -> Option<u64> {
        (self.present & ADDRESS_HAS_BAND_ID != 0).then_some(self.band_id)
    }

    pub fn set_page_id(&mut self, value: Option<u64>) {
        self.page_id = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_PAGE_ID, value.is_some());
    }

    pub fn set_object_id(&mut self, value: Option<u64>) {
        self.object_id = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_OBJECT_ID, value.is_some());
    }

    pub fn set_routing_bucket(&mut self, value: Option<u32>) {
        self.routing_bucket = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_ROUTING_BUCKET, value.is_some());
    }

    pub fn set_generation(&mut self, value: Option<u64>) {
        self.generation = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_GENERATION, value.is_some());
    }

    pub fn set_band_id(&mut self, value: Option<u64>) {
        self.band_id = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_BAND_ID, value.is_some());
    }

    fn set_present(&mut self, bit: u8, on: bool) {
        if on {
            self.present |= bit;
        } else {
            self.present &= !bit;
        }
    }
}

/// A digest is 32 bytes in memory and hex on the wire.
///
/// Keeping the wire form makes this change invisible to anything that reads a persisted index, in
/// both directions: the same hex string is written, and a hex string is what is read.
mod hex_digest {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Option<[u8; 32]>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_str(&hex::encode(bytes)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<[u8; 32]>, D::Error> {
        // A digest that is not 32 bytes of hex is not a digest. Reading it as absent rather than
        // failing keeps a malformed one from making a whole index unloadable -- the read path
        // treats a missing digest as "unverified", which is what a corrupt one deserves too.
        let raw = Option::<String>::deserialize(deserializer)?;
        Ok(raw.and_then(|text| {
            let mut bytes = [0u8; 32];
            hex::decode_to_slice(text.as_bytes(), &mut bytes)
                .ok()
                .map(|_| bytes)
        }))
    }
}

impl BlockAddress {
    pub fn compact_slab_id(&self) -> Option<u32> {
        u32::try_from(self.page_slab_id).ok()
    }

    pub fn compact_slab_offset(&self) -> Option<u32> {
        u32::try_from(self.offset).ok()
    }

    pub fn compact_slab_address(&self) -> Option<u64> {
        compact_slab_address_from_parts(self.page_slab_id, self.offset)
    }

    pub fn from_compact_slab_address(compact_slab_address: u64, length: u64) -> Self {
        Self::from_parts(
            compact_extract_band_id(compact_slab_address) as u64,
            compact_extract_band_offset(compact_slab_address) as u64,
            length,
            None,
            None,
            None,
            None,
            None,
        )
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreStats {
    pub writes: u64,
    pub reads: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    #[serde(default)]
    pub logical_bytes_written: u64,
    #[serde(default)]
    pub logical_bytes_read: u64,
    #[serde(default)]
    pub compressed_records_written: u64,
    #[serde(default)]
    pub compressed_records_read: u64,
    #[serde(default)]
    pub compression_bytes_saved: u64,
    /// Times the whole band manifest was written out.
    ///
    /// Writing it costs the whole manifest, so one write per slab install made installing n slabs
    /// cost n manifests -- and each install cost time proportional to how many slabs already
    /// existed. Counted rather than timed, because a count says the same thing on a busy machine.
    #[serde(default)]
    pub band_manifest_writes: u64,
    /// Slabs fetched on-demand from a shared-storage read-through source (conformance
    /// lazy recovery). Each shared slab is fetched at most once, only when a read
    /// misses it locally; a nonzero count proves recovery did not install every slab
    /// up front.
    #[serde(default)]
    pub shared_slab_fetches: u64,
}

/// Lazy read-through source for slabs that live only in shared storage after a
/// metadata-only (index + address map) recovery on the shared-filesystem backend.
/// On a local slab miss the block store asks the source for exactly that slab's
/// bytes, caches them locally, then serves the read, so old pages are read lazily
/// by address rather than eagerly installed at recovery time. Implementations
/// resolve a slab id to its shared object and return its verified bytes, or `None`
/// when the slab is not part of the recovered checkpoint.
pub trait SharedSlabSource: Send + Sync + std::fmt::Debug {
    fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreOptions {
    #[serde(default = "default_page_record_compression_enabled")]
    pub compression_enabled: bool,
    #[serde(default = "default_page_record_compression_min_bytes")]
    pub compression_min_bytes: usize,
    #[serde(default = "default_page_record_compression_level")]
    pub compression_level: i32,
}

pub type BlockAppendRecord = (Vec<u8>, Option<u64>, Option<u32>);

impl Default for BlockStoreOptions {
    fn default() -> Self {
        Self {
            compression_enabled: default_page_record_compression_enabled(),
            compression_min_bytes: default_page_record_compression_min_bytes(),
            compression_level: default_page_record_compression_level(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcReport {
    #[serde(alias = "retain_from_page_segment_id")]
    pub retain_from_page_slab_id: u64,
    #[serde(alias = "removed_page_segment_ids")]
    pub removed_page_slab_ids: Vec<u64>,
    #[serde(alias = "retained_page_segment_ids")]
    pub retained_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub removed_physical_bytes: u64,
    #[serde(default)]
    pub retained_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "delayed_destroy_page_segment_ids")]
    pub delayed_destroy_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub delayed_destroy_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_live_page_segment_ids")]
    pub retained_live_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_current_page_segment_ids")]
    pub retained_current_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_current_physical_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcUtilityCandidate {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub bytes: u64,
    #[serde(default)]
    pub total_bytes: u64,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub stale_bytes: u64,
    #[serde(default)]
    pub utility_basis_points: u64,
    pub utility_score: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicy {
    #[serde(default)]
    #[serde(alias = "max_destroy_segments")]
    pub max_destroy_slabs: usize,
    #[serde(default)]
    pub max_destroy_physical_bytes: u64,
    #[serde(default)]
    pub max_utility_score: Option<u64>,
    #[serde(default)]
    pub min_age_ms: Option<u64>,
    /// Reclaim only bands whose garbage ratio (10_000 - utility_basis_points) is at
    /// least this many basis points. `None`/0 reclaims every eligible band (today's
    /// behavior). The garbage-ratio gate (reclaim the most-garbage zones),
    /// expressed against Rust bands.
    #[serde(default)]
    pub min_band_garbage_basis_points: Option<u64>,
}

impl BlockStoreGcPolicy {
    pub fn max_slabs(max_destroy_slabs: usize) -> Self {
        Self {
            max_destroy_slabs,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms: None,
            min_band_garbage_basis_points: None,
        }
    }

    /// Reclaim eligible bands whose garbage ratio is at least
    /// `min_band_garbage_basis_points`, highest-garbage first, optionally bounded by a
    /// minimum band age. Mirrors selecting the maximum-garbage-rate zone under GC.
    pub fn with_band_garbage_floor(
        min_band_garbage_basis_points: u64,
        min_age_ms: Option<u64>,
    ) -> Self {
        Self {
            max_destroy_slabs: 0,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms,
            min_band_garbage_basis_points: Some(min_band_garbage_basis_points),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicyPlan {
    #[serde(alias = "retain_from_page_segment_id")]
    pub retain_from_page_slab_id: u64,
    #[serde(alias = "selected_page_segment_ids")]
    pub selected_page_slab_ids: Vec<u64>,
    pub selected_physical_bytes: u64,
    #[serde(default)]
    pub candidate_total_bytes: u64,
    #[serde(default)]
    pub candidate_used_bytes: u64,
    #[serde(default)]
    pub candidate_stale_bytes: u64,
    #[serde(default)]
    pub candidate_utility_basis_points: u64,
    pub candidate_count: usize,
    pub candidate_physical_bytes: u64,
    pub skipped_by_policy_count: usize,
    pub skipped_by_policy_physical_bytes: u64,
    pub skipped_by_budget_count: usize,
    pub skipped_by_budget_physical_bytes: u64,
    pub candidates: Vec<BlockStoreGcUtilityCandidate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreDelayedDestroySlabReport {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStorePurgeDelayedDestroyReport {
    #[serde(alias = "purged_page_segment_ids")]
    pub purged_page_slab_ids: Vec<u64>,
    pub purged_physical_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStoreBandState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// Metadata for a SEALED band whose bytes live in shared storage and are restored lazily.
/// Passed to [`LocalBlockStore::install_lazy_checkpoint_bands`] so a lazy-restore installs
/// complete band descriptors before the first on-demand slab fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LazyCheckpointBand {
    pub page_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub first_page_id: Option<u64>,
    pub last_page_id: Option<u64>,
    pub created_unix_ms: Option<u64>,
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreBandDescriptor {
    #[serde(alias = "extent_id", alias = "zone_id")]
    pub band_id: u64,
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub state: BlockStoreBandState,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreBandSummary {
    #[serde(alias = "active_zones")]
    pub active_bands: u64,
    #[serde(alias = "sealed_zones")]
    pub sealed_bands: u64,
    #[serde(alias = "delayed_destroy_zones")]
    pub delayed_destroy_bands: u64,
    #[serde(alias = "purged_zones")]
    pub purged_bands: u64,
    pub active_physical_bytes: u64,
    pub sealed_physical_bytes: u64,
    pub delayed_destroy_physical_bytes: u64,
    pub purged_physical_bytes: u64,
    pub live_physical_bytes: u64,
    pub reclaimable_physical_bytes: u64,
    pub total_known_physical_bytes: u64,
    #[serde(
        default,
        alias = "oldest_known_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_known_band_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_known_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_known_band_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_live_band_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_live_band_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_reclaimable_band_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub oldest_reclaimable_band_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreBandUsage {
    #[serde(alias = "extent_id", alias = "zone_id")]
    pub band_id: u64,
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    #[serde(default)]
    pub storage_zone_id: u64,
    #[serde(default)]
    #[serde(alias = "stream_segment_id")]
    pub stream_slab_id: u64,
    pub state: BlockStoreBandState,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub live_bytes: u64,
    #[serde(default)]
    pub reclaimable_bytes: u64,
    #[serde(default)]
    pub purged_bytes: u64,
    pub page_store_used_bytes: u64,
    pub live_page_store_used_bytes: u64,
    pub reclaimable_page_store_used_bytes: u64,
    pub purged_page_store_used_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBackedBandRuntimeReport {
    pub runtime_ready: bool,
    #[serde(default)]
    pub band_lifecycle_states: Vec<String>,
    #[serde(alias = "extent_count", alias = "zone_count")]
    pub band_count: u64,
    #[serde(alias = "active_zones")]
    pub active_bands: u64,
    #[serde(alias = "sealed_zones")]
    pub sealed_bands: u64,
    #[serde(alias = "delayed_destroy_zones")]
    pub delayed_destroy_bands: u64,
    #[serde(alias = "purged_zones")]
    pub purged_bands: u64,
    #[serde(default)]
    pub zone_stats_ready: bool,
    #[serde(default)]
    pub zone_usage: Vec<BlockStoreBandUsage>,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default)]
    pub stream_record_count: u64,
    #[serde(default)]
    pub first_page_id: Option<u64>,
    #[serde(default)]
    pub last_page_id: Option<u64>,
    #[serde(default)]
    pub page_id_continuity_ready: bool,
    #[serde(default)]
    pub logical_stream_bytes_read: u64,
    #[serde(default)]
    pub band_state_transition_count: u64,
    pub logical_stream_read_ready: bool,
    pub append_roll_ready: bool,
    #[serde(alias = "extent_manifest_ready", alias = "zone_manifest_ready")]
    pub band_manifest_ready: bool,
    #[serde(default)]
    pub band_manifest_rebuild_ready: bool,
    #[serde(default)]
    pub band_manifest_reconciled_on_open: bool,
    #[serde(default)]
    pub band_manifest_disk_consistent: bool,
    #[serde(default)]
    pub manifest_missing_stream_bands: u64,
    #[serde(default)]
    pub manifest_extra_stream_bands: u64,
    #[serde(default)]
    pub corrupt_band_count: u64,
    #[serde(default)]
    pub partial_band_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    pub partial_band_recovery_ready: bool,
    pub envelope_checksum_ready: bool,
    pub compression_stream_ready: bool,
    pub delayed_destroy_ready: bool,
    #[serde(default)]
    pub purge_lifecycle_ready: bool,
    pub blockers: Vec<String>,
    pub evidence: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabReport {
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub page_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    #[serde(default)]
    pub object_count: u64,
    #[serde(default)]
    #[serde(rename = "routing_slot_count")]
    pub routing_bucket_count: u64,
    pub compressed_records: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "first_routing_slot")]
    pub first_routing_bucket: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "last_routing_slot")]
    pub last_routing_bucket: Option<u32>,
    #[serde(default, alias = "page_index_count")]
    pub block_index_count: u64,
    #[serde(default, alias = "page_index_entries")]
    pub block_index_entries: Vec<BlockStoreBlockIndexReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreBlockIndexReport {
    #[serde(alias = "page_segment_id")]
    #[serde(alias = "block_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_address")]
    pub compact_slab_address: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_id")]
    pub compact_slab_id: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(alias = "compact_segment_offset")]
    pub compact_slab_offset: Option<u32>,
    #[serde(
        default,
        alias = "extent_id",
        alias = "zone_id",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(alias = "storage_segment_id")]
    pub storage_slab_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<u8>,
    #[serde(default, alias = "page_id", skip_serializing_if = "Option::is_none")]
    pub block_id: Option<u64>,
    #[serde(alias = "page_size")]
    pub block_size: u64,
    pub stored_size: u64,
    pub dirty: bool,
    pub deleted: bool,
    #[serde(alias = "page_in_log")]
    pub block_in_log: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "routing_slot")]
    pub routing_bucket: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BlockStoreBandManifest {
    version: u32,
    #[serde(alias = "extents", alias = "zones")]
    bands: Vec<BlockStoreBandDescriptor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreRollReport {
    #[serde(alias = "previous_page_segment_id")]
    pub previous_page_slab_id: u64,
    #[serde(alias = "new_page_segment_id")]
    pub new_page_slab_id: u64,
}

#[derive(Debug, Clone)]
pub struct LocalBlockStore {
    inner: Arc<Mutex<BlockStoreInner>>,
}

impl LocalBlockStore {
    /// Which store this is, as a value that can be compared and hashed.
    ///
    /// Clones of one store share their inner handle, so they answer the same; two engines --
    /// which is what an embedded process runs several of -- never do. That distinction is the
    /// only thing separating two engines that both serve shard 1 and both hold a key called
    /// "user:1", so anything keyed per object has to include it.
    pub fn store_id(&self) -> usize {
        Arc::as_ptr(&self.inner) as *const u8 as usize
    }
}

#[derive(Debug)]
struct BlockStoreInner {
    root: PathBuf,
    // Set when a relaxed (bulk) append deferred its fsync + manifest persist;
    // cleared by sync_durable(). See bulk_relaxed_durability().
    relaxed_dirty: bool,
    page_slab_id: u64,
    write_offset: u64,
    next_page_id: u64,
    options: BlockStoreOptions,
    bands: BTreeMap<u64, BlockStoreBandDescriptor>,
    /// Slab installs since the manifest was last written out. Writing it costs the whole manifest,
    /// so it is written every so often rather than every install; the load rebuilds from the slabs
    /// when what it reads does not match them.
    bands_unwritten: usize,
    band_manifest_reconciled_on_open: bool,
    stats: BlockStoreStats,
    // Optional shared-storage read-through (on-demand lazy recovery): set by
    // attach_shared_slab_source() after a metadata-only restore. When present, a
    // read that misses a slab locally fetches it from here, caches it, then serves.
    shared_slab_source: Option<Arc<dyn SharedSlabSource>>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<Arc<crate::scratch::ScratchDirGuard>>,
}

/// Slab installs allowed to go by before the band manifest is written out.
///
/// Writing it costs the whole manifest, so writing it per install makes installing n slabs cost n
/// manifests. Deferring trades that for a rebuild after a crash, which the load does for itself
/// when what it reads does not match the slabs on disk.
const BANDS_UNWRITTEN_BEFORE_PERSIST: usize = 64;

impl LocalBlockStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_options(root, BlockStoreOptions::default())
    }

    pub fn with_options(root: impl Into<PathBuf>, options: BlockStoreOptions) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        let page_slab_id = latest_slab_id_at(&root).unwrap_or_default();
        let mut write_offset = slab_path(&root, page_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let next_page_id = next_page_id_at(&root).unwrap_or_default();
        let manifest_exists =
            band_manifest_path(&root).exists() || legacy_zone_manifest_path(&root).exists();
        let (mut bands, mut manifest_rebuilt) = if manifest_exists {
            match load_band_manifest_at(&root) {
                Ok(bands) => (bands, false),
                Err(_) => (rebuild_band_manifest_at(&root).unwrap_or_default(), true),
            }
        } else {
            (rebuild_band_manifest_at(&root).unwrap_or_default(), true)
        };
        let band_manifest_reconciled_on_open =
            reconcile_band_manifest_with_disk(&root, &mut bands).unwrap_or_default();
        manifest_rebuilt |= band_manifest_reconciled_on_open;
        ensure_band_descriptor(
            &mut bands,
            &root,
            page_slab_id,
            BlockStoreBandState::Active,
        );
        // Fence a torn tail on the ACTIVE slab. After a crash mid-append the raw file length
        // includes uncommitted/partial bytes past the last intact record; reconcile computed
        // the intact `readable_prefix`. Resuming appends at raw EOF would embed the torn record
        // permanently mid-slab and, via the early-halting page-id scan, regress next_page_id ->
        // page-id/generation reuse -> stale reads. Mirror the resume-at-committed-length:
        // physically truncate the active slab to its readable prefix and resume there.
        let active_readable_prefix = bands
            .get(&page_slab_id)
            .map(|band| band.readable_prefix_physical_bytes);
        if let Some(readable_prefix) = active_readable_prefix {
            if readable_prefix < write_offset {
                if let Ok(file) = OpenOptions::new()
                    .write(true)
                    .open(slab_path(&root, page_slab_id))
                {
                    if file.set_len(readable_prefix).is_ok() {
                        crate::durability_metrics::record_barrier("block_store_open");
                        let _ = file.sync_all();
                        if let Ok(dir) = File::open(&root) {
                            let _ = dir.sync_all();
                        }
                        write_offset = readable_prefix;
                        if let Some(band) = bands.get_mut(&page_slab_id) {
                            band.physical_bytes = readable_prefix;
                            band.has_corruption = false;
                            band.first_error_offset = None;
                        }
                        manifest_rebuilt = true;
                    }
                }
            }
        }
        if manifest_rebuilt {
            let _ = persist_band_manifest(&root, &bands);
        }
        Self {
            inner: Arc::new(Mutex::new(BlockStoreInner {
                root,
                relaxed_dirty: false,
                page_slab_id,
                write_offset,
                next_page_id,
                options,
                bands,
                bands_unwritten: 0,
                band_manifest_reconciled_on_open,
                stats: BlockStoreStats::default(),
                shared_slab_source: None,
                scratch: None,
            })),
        }
    }

    /// Attach a shared-storage read-through source (on-demand lazy recovery). After a
    /// metadata-only restore installs the served index and a slab address map, this
    /// lets a later read fetch a missing old slab on demand from shared storage,
    /// cache it locally, and serve it — instead of installing every slab up front.
    pub fn attach_shared_slab_source(&self, source: Arc<dyn SharedSlabSource>) {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .shared_slab_source = Some(source);
    }

    pub fn has_shared_slab_source(&self) -> bool {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .shared_slab_source
            .is_some()
    }

    /// The next free page id this store would assign on the next append. Recorded in a
    /// shared checkpoint so a lazy restore can advance the fresh owner's counter past it.
    pub fn next_page_id(&self) -> u64 {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .next_page_id
    }

    /// Reserve the slab-id (and page-id) range consumed by a lazily-restored checkpoint
    /// so replayed/new appends land in a FRESH slab beyond it, never overwriting a slab
    /// that is still served on-demand from shared storage. Matches the recovery behaviour
    /// model where old pages stay addressable in shared storage while new writes roll
    /// forward. Called right after attaching the shared read-through on a fresh owner.
    pub fn reserve_lazy_checkpoint_range(
        &self,
        through_slab_id: u64,
        next_page_id_floor: u64,
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let existing_max = slab_ids_at(&inner.root)?.into_iter().max();
        let new_slab_id = through_slab_id
            .max(inner.page_slab_id)
            .max(existing_max.unwrap_or_default())
            .saturating_add(1);
        let path = slab_path(&inner.root, new_slab_id);
        let file = File::create(&path)?;
        crate::durability_metrics::record_barrier("block_store_checkpoint_reserve");
        file.sync_all()?;
        sync_parent_dir(&path)?;
        inner.page_slab_id = new_slab_id;
        inner.write_offset = 0;
        inner.next_page_id = inner.next_page_id.max(next_page_id_floor);
        let now = now_unix_ms();
        // Any previously-active local band is now sealed; the reserved slab is active.
        for band in inner.bands.values_mut() {
            if band.state == BlockStoreBandState::Active {
                band.state = BlockStoreBandState::Sealed;
                band.updated_unix_ms = Some(now);
            }
        }
        inner.bands.insert(
            new_slab_id,
            BlockStoreBandDescriptor {
                band_id: band_id_for_slab(new_slab_id),
                page_slab_id: new_slab_id,
                state: BlockStoreBandState::Active,
                physical_bytes: 0,
                logical_bytes: 0,
                created_unix_ms: Some(now),
                updated_unix_ms: Some(now),
                first_page_id: None,
                last_page_id: None,
                readable_prefix_physical_bytes: 0,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        persist_band_manifest(&inner.root, &inner.bands)?;
        Ok(())
    }

    /// Install SEALED band descriptors for the slabs a lazy checkpoint restore backs from shared
    /// storage. The slab bytes are NOT local yet (they are fetched on demand through the attached
    /// shared read-through), but a GC/compaction cycle running between restore and the first fetch
    /// must still see these sealed bands, or it accounts on an incomplete picture and could
    /// reclaim prematurely. Recording them here makes `band_summary()`/`band_descriptors()`
    /// complete immediately after restore. Any slab id that is the current active slab, or already
    /// has a descriptor (e.g. it was fetched or is local), is left untouched. Call AFTER
    /// [`reserve_lazy_checkpoint_range`] so the reserved slab is the active one and every
    /// checkpoint slab is correctly sealed.
    pub fn install_lazy_checkpoint_bands(
        &self,
        bands: &[LazyCheckpointBand],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let root = inner.root.clone();
        let active = inner.page_slab_id;
        let mut changed = false;
        for band in bands {
            // Never touch the active (reserved) slab — it holds live local writes.
            if band.page_slab_id == active {
                continue;
            }
            // If the slab is materialized locally (already fetched / a real local slab), its
            // existing descriptor reflects real on-disk bytes and is authoritative — leave it.
            if slab_path(&root, band.page_slab_id).exists() {
                continue;
            }
            // Lazily-backed checkpoint slab: install (or replace a fresh-store placeholder — a
            // freshly opened block store seeds an empty Active descriptor for slab 0, which
            // `reserve_lazy_checkpoint_range` then seals; that stale empty descriptor must be
            // overwritten with the checkpoint's real band metadata, not skipped) a complete
            // SEALED descriptor so accounting is correct before any fetch.
            let descriptor = BlockStoreBandDescriptor {
                band_id: band_id_for_slab(band.page_slab_id),
                page_slab_id: band.page_slab_id,
                state: BlockStoreBandState::Sealed,
                physical_bytes: band.physical_bytes,
                logical_bytes: band.logical_bytes,
                created_unix_ms: band.created_unix_ms,
                updated_unix_ms: band.updated_unix_ms,
                first_page_id: band.first_page_id,
                last_page_id: band.last_page_id,
                readable_prefix_physical_bytes: band.physical_bytes,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            };
            if inner.bands.get(&band.page_slab_id) != Some(&descriptor) {
                inner.bands.insert(band.page_slab_id, descriptor);
                changed = true;
            }
        }
        if changed {
            persist_band_manifest(&inner.root, &inner.bands)?;
        }
        Ok(())
    }

    /// Ensure `page_slab_id` is present locally, fetching it from the shared
    /// read-through source (if any) on a local miss, caching it, and counting the
    /// on-demand fetch. A no-op when the slab already exists locally or no source is
    /// attached; in the latter case the normal read path surfaces the miss.
    fn ensure_slab_present(&self, page_slab_id: u64) -> Result<(), BlockStoreError> {
        let (root, source) = {
            let inner = self.inner.lock().expect("block store lock poisoned");
            (inner.root.clone(), inner.shared_slab_source.clone())
        };
        if slab_path(&root, page_slab_id).exists() {
            return Ok(());
        }
        let Some(source) = source else {
            return Ok(());
        };
        if let Some(bytes) = source.fetch_slab(page_slab_id)? {
            self.install_slab(page_slab_id, &bytes)?;
            self.inner
                .lock()
                .expect("block store lock poisoned")
                .stats
                .shared_slab_fetches += 1;
        }
        Ok(())
    }

    pub fn roll_slab(&self) -> Result<BlockStoreRollReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        roll_slab_inner(&mut inner)
    }

    /// Roll the active slab ahead of the write that would otherwise have to, off the client
    /// path. Returns the roll report if a roll happened, `None` if the active slab still has
    /// room.
    ///
    /// Rolling is not cheap: `roll_slab_inner` fsyncs the outgoing slab, scans the slab
    /// directory to pick the next id, creates and fsyncs the new file, fsyncs the parent
    /// directory, and persists the band manifest. Run inline from `append` -- which is where
    /// it runs today -- one unlucky client write pays all of that on top of its own
    /// durability barrier, a latency outlier unrelated to the size of the write that
    /// triggered it.
    ///
    /// The reference implementation keeps this off the write path: its background
    /// storage-manager cycle runs a prepare step that no-ops while the active zone is under
    /// target and otherwise rolls to a fresh one, so a client append finds space already
    /// waiting. The inline roll in `append` stays as the fallback for a write that arrives
    /// before the background cycle got here -- the same role the reference's forced-roll path
    /// plays.
    pub fn prepare_next_slab(&self) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        self.prepare_next_slab_with_target(effective_block_slab_target_bytes())
    }

    /// [`prepare_next_slab`] against an explicit target rather than the process-wide
    /// configured one.
    ///
    /// The slab target is only reachable through a global env var, so a test that wanted a
    /// small target would have to mutate process state — and if it then failed an assertion
    /// before restoring it, every later test in the process would inherit the small target.
    /// Taking the target as an argument keeps that class of cross-test failure impossible.
    pub fn prepare_next_slab_with_target(
        &self,
        slab_target_bytes: u64,
    ) -> Result<Option<BlockStoreRollReport>, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        if !slab_is_at_target(inner.write_offset, slab_target_bytes) {
            return Ok(None);
        }
        roll_slab_inner(&mut inner).map(Some)
    }

    /// Whether [`prepare_next_slab`] would roll right now, without doing it. Lets a caller
    /// (a report, or a scheduler deciding whether the phase is worth running) ask about
    /// pressure without paying for it.
    pub fn needs_slab_preparation(&self) -> bool {
        self.needs_slab_preparation_with_target(effective_block_slab_target_bytes())
    }

    /// [`needs_slab_preparation`] against an explicit target.
    pub fn needs_slab_preparation_with_target(&self, slab_target_bytes: u64) -> bool {
        let inner = self.inner.lock().expect("block store lock poisoned");
        slab_is_at_target(inner.write_offset, slab_target_bytes)
    }

    /// Bytes written to the active slab. Exposed so a caller can reason about slab pressure
    /// (and so tests can drive the prepare threshold without touching global config).
    pub fn active_slab_write_offset(&self) -> u64 {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .write_offset
    }

    pub fn slab_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut ids = Vec::new();
        if !root.exists() {
            return Ok(ids);
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if let Some(id) = name
                .strip_prefix("page_segment_")
                .and_then(|name| name.strip_suffix(".seg"))
                .and_then(|id| id.parse::<u64>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub fn zone_summary(&self) -> BlockStoreBandSummary {
        self.band_summary()
    }

    pub fn zone_descriptors(&self) -> Vec<BlockStoreBandDescriptor> {
        self.band_descriptors()
    }

    pub fn zone_usage(&self) -> Vec<BlockStoreBandUsage> {
        band_zone_usage(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .bands,
        )
    }


    pub fn delayed_destroy_slab_ids(&self) -> Result<Vec<u64>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_slab_ids_at(&root)
    }

    pub fn delayed_destroy_slab_reports(
        &self,
    ) -> Result<Vec<BlockStoreDelayedDestroySlabReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        delayed_destroy_slab_reports_at(&root)
    }

    pub fn purge_delayed_destroy_slabs(&self) -> Result<Vec<u64>, BlockStoreError> {
        Ok(self
            .purge_delayed_destroy_slabs_with_report()?
            .purged_page_slab_ids)
    }

    pub fn purge_delayed_destroy_slabs_with_report(
        &self,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let trash_dir = delayed_destroy_dir(&inner.root);
        let mut purged = Vec::new();
        let mut purged_physical_bytes = 0;
        if !trash_dir.exists() {
            return Ok(BlockStorePurgeDelayedDestroyReport::default());
        }
        for entry in fs::read_dir(&trash_dir)? {
            let entry = entry?;
            let Some(id) = delayed_destroy_slab_id_from_name(&entry.file_name()) else {
                continue;
            };
            purged_physical_bytes += entry
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            fs::remove_file(entry.path())?;
            set_band_state(&mut inner.bands, id, BlockStoreBandState::Purged);
            purged.push(id);
        }
        purged.sort_unstable();
        sync_dir(&trash_dir)?;
        persist_band_manifest(&inner.root, &inner.bands)?;
        Ok(BlockStorePurgeDelayedDestroyReport {
            purged_page_slab_ids: purged,
            purged_physical_bytes,
        })
    }


    pub fn slab_reports(&self) -> Result<Vec<BlockStoreSlabReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut reports = Vec::new();
        for page_slab_id in slab_ids_at(&root)? {
            let bytes = fs::read(slab_path(&root, page_slab_id))?;
            reports.push(inspect_slab(&bytes, page_slab_id));
        }
        Ok(reports)
    }

    pub fn stats(&self) -> BlockStoreStats {
        self.inner.lock().expect("block store lock poisoned").stats
    }
}

fn should_roll_before_append(
    write_offset: u64,
    record_len: u64,
    slab_target_bytes: u64,
) -> bool {
    write_offset > 0 && write_offset.saturating_add(record_len) > slab_target_bytes
}

/// Whether the active slab has reached its target and should be rolled ahead of the next
/// append.
///
/// Deliberately NOT the same predicate as `should_roll_before_append`: that one asks "will
/// THIS record overflow the slab", which needs the record length and is only answerable on
/// the write path. This one asks "is the slab full enough to roll now", which is answerable
/// in the background with no pending record. The `write_offset > 0` guard stops a freshly
/// rolled, still-empty slab from rolling again immediately — without it a background cycle
/// would mint an empty slab every pass.
fn slab_is_at_target(write_offset: u64, slab_target_bytes: u64) -> bool {
    write_offset > 0 && write_offset >= slab_target_bytes
}

/// Bulk backfill (MATRIXARK_BULK_INGEST) defers per-append fsync + manifest
/// persistence to an explicit sync_durable(), trading crash-durability *within a
/// resumable/WAL-backed chunk* for far fewer fsyncs. The live path (env unset)
/// keeps full per-append durability.
pub(crate) fn bulk_relaxed_durability() -> bool {
    matches!(
        std::env::var("MATRIXARK_BULK_INGEST")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// On the live path, defer the per-record extent-manifest persist to sync_durable()/slab-seal
/// (the manifest is reconciled from disk on open). Single-barrier default; restored to a
/// synchronous persist only under the TS_WAL_LEGACY_RECOVERY escape hatch. Moves in lockstep
/// with `page_wal_single_barrier` (the intermediate "manifest-only" relaxation is gone).
pub(crate) fn page_wal_only_sync() -> bool {
    page_wal_single_barrier()
}

/// The single-barrier default also defers the per-write data-page fdatasync -- the last non-WAL
/// synchronous barrier. This is safe ONLY because the default also switches recovery to base-only
/// replay: reload trusts only the durable dump checkpoint (whose `flush_shard_index` fsyncs every
/// page BEFORE advancing the watermark) and re-derives every post-watermark page by replaying the
/// WAL tail exactly once. A page that was written but never fsync'd is therefore rebuilt from its
/// WAL command, never left as a dangling reference. The deferred page still becomes durable at the
/// next dump (`sync_durable` fsyncs the active slab; a rolled slab is fsync'd at roll). Restored to
/// a synchronous per-write data-page fdatasync (with delta-fold recovery) only under the
/// TS_WAL_LEGACY_RECOVERY escape hatch.
pub(crate) fn page_wal_single_barrier() -> bool {
    !matches!(
        std::env::var("TS_WAL_LEGACY_RECOVERY")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Bands are neither preallocated nor recycled, and both are deliberate.
///
/// The log does preallocate, and it measured **2.16x** cheaper per append for doing so
/// (1131 us against 2444, ranges [1042-1285] and [2442-2565], six interleaved runs). So the same
/// treatment here looks obviously worth having. It is not, and the reason is the barrier rate
/// rather than anything about files.
///
/// That win comes entirely from not persisting a new file size on every barrier. Measured across
/// group sizes, it is 65.5% at one barrier per record, 42.7% at eight, and **gone by sixty-four**:
/// with no barrier, growing a file is page-cache work and costs nothing worth reclaiming.
///
/// This path has no barrier per write. `defer_data_sync` is
/// `bulk_relaxed_durability() || page_wal_single_barrier()`, and the second is true unless legacy
/// recovery is turned back on -- so by default the per-write page fdatasync is already deferred
/// (see the note on `page_wal_only_sync`). Preallocating would remove a cost that is not being
/// paid.
///
/// Recycling a band rather than creating and unlinking one is the same story from the other end.
/// What it saves is the create and the unlink -- and bands are large, so that turnover is rare
/// against the writes going through them. Reusing already-allocated blocks is the other half of the
/// preallocation argument, and it lapses for the same reason.
///
/// What WOULD change the answer: making page writes synchronous again (turning
/// `TS_WAL_LEGACY_RECOVERY` on, or anything else that stops deferring that fdatasync). Then this
/// path starts paying per barrier for a file that grows per write, and both are worth revisiting
/// together -- with the group-size table above as the guide to how much is there.
fn roll_slab_inner(
    inner: &mut BlockStoreInner,
) -> Result<BlockStoreRollReport, BlockStoreError> {
    fs::create_dir_all(&inner.root)?;
    let previous_page_slab_id = inner.page_slab_id;
    // The outgoing slab may hold relaxed (un-fsynced) bulk appends; make them
    // durable before we seal and stop writing to it.
    {
        let prev_path = slab_path(&inner.root, previous_page_slab_id);
        if let Ok(prev) = OpenOptions::new().append(true).open(&prev_path) {
            crate::durability_metrics::record_barrier("block_store_slab_roll_prev");
            let _ = prev.sync_data();
        }
    }
    let next_from_current = inner.page_slab_id.saturating_add(1);
    let next_from_disk = slab_ids_at(&inner.root)?
        .into_iter()
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or_default();
    inner.page_slab_id = next_from_current.max(next_from_disk);
    inner.write_offset = 0;
    let path = slab_path(&inner.root, inner.page_slab_id);
    let file = File::create(&path)?;
    crate::durability_metrics::record_barrier("block_store_slab_roll");
    file.sync_all()?;
    sync_parent_dir(&path)?;
    let transition_unix_ms = now_unix_ms();
    if let Some(previous) = inner.bands.get_mut(&previous_page_slab_id) {
        previous.state = BlockStoreBandState::Sealed;
        previous.updated_unix_ms = Some(transition_unix_ms);
    }
    let new_band = BlockStoreBandDescriptor {
        band_id: band_id_for_slab(inner.page_slab_id),
        page_slab_id: inner.page_slab_id,
        state: BlockStoreBandState::Active,
        physical_bytes: 0,
        logical_bytes: 0,
        created_unix_ms: Some(transition_unix_ms),
        updated_unix_ms: Some(transition_unix_ms),
        first_page_id: None,
        last_page_id: None,
        readable_prefix_physical_bytes: 0,
        has_corruption: false,
        first_error_offset: None,
        first_error: None,
    };
    let page_slab_id = inner.page_slab_id;
    inner.bands.insert(page_slab_id, new_band);
    persist_band_manifest(&inner.root, &inner.bands)?;
    Ok(BlockStoreRollReport {
        previous_page_slab_id,
        new_page_slab_id: inner.page_slab_id,
    })
}

fn band_lifecycle_states(summary: &BlockStoreBandSummary) -> Vec<String> {
    let mut states = Vec::new();
    if summary.active_bands > 0 {
        states.push("active".to_string());
    }
    if summary.sealed_bands > 0 {
        states.push("sealed".to_string());
    }
    if summary.delayed_destroy_bands > 0 {
        states.push("delayed_destroy".to_string());
    }
    if summary.purged_bands > 0 {
        states.push("purged".to_string());
    }
    states
}

fn band_zone_usage(
    bands: &BTreeMap<u64, BlockStoreBandDescriptor>,
) -> Vec<BlockStoreBandUsage> {
    #[derive(Debug, Clone)]
    struct ZoneUsageAcc {
        usage: BlockStoreBandUsage,
    }

    fn merged_zone_state(
        left: BlockStoreBandState,
        right: BlockStoreBandState,
    ) -> BlockStoreBandState {
        use BlockStoreBandState::*;
        match (left, right) {
            (Active, _) | (_, Active) => Active,
            (Sealed, _) | (_, Sealed) => Sealed,
            (DelayedDestroy, _) | (_, DelayedDestroy) => DelayedDestroy,
            (Purged, Purged) => Purged,
        }
    }

    let mut zones = BTreeMap::<u64, ZoneUsageAcc>::new();
    for band in bands.values() {
        let (live, reclaimable, purged) = match band.state {
            BlockStoreBandState::Active | BlockStoreBandState::Sealed => {
                (band.physical_bytes, 0, 0)
            }
            BlockStoreBandState::DelayedDestroy => (0, band.physical_bytes, 0),
            BlockStoreBandState::Purged => (0, 0, band.physical_bytes),
        };
        let entry = zones
            .entry(band.band_id)
            .or_insert_with(|| ZoneUsageAcc {
                usage: BlockStoreBandUsage {
                    band_id: band.band_id,
                    page_slab_id: band.page_slab_id,
                    storage_zone_id: band.band_id,
                    stream_slab_id: band.page_slab_id,
                    state: band.state,
                    used_bytes: 0,
                    live_bytes: 0,
                    reclaimable_bytes: 0,
                    purged_bytes: 0,
                    page_store_used_bytes: 0,
                    live_page_store_used_bytes: 0,
                    reclaimable_page_store_used_bytes: 0,
                    purged_page_store_used_bytes: 0,
                    first_page_id: None,
                    last_page_id: None,
                },
            });
        let usage = &mut entry.usage;
        usage.page_slab_id = usage.page_slab_id.min(band.page_slab_id);
        usage.stream_slab_id = usage.stream_slab_id.min(band.page_slab_id);
        usage.state = merged_zone_state(usage.state, band.state);
        usage.used_bytes = usage.used_bytes.saturating_add(band.physical_bytes);
        usage.live_bytes = usage.live_bytes.saturating_add(live);
        usage.reclaimable_bytes = usage.reclaimable_bytes.saturating_add(reclaimable);
        usage.purged_bytes = usage.purged_bytes.saturating_add(purged);
        usage.page_store_used_bytes = usage
            .page_store_used_bytes
            .saturating_add(band.physical_bytes);
        usage.live_page_store_used_bytes = usage.live_page_store_used_bytes.saturating_add(live);
        usage.reclaimable_page_store_used_bytes = usage
            .reclaimable_page_store_used_bytes
            .saturating_add(reclaimable);
        usage.purged_page_store_used_bytes =
            usage.purged_page_store_used_bytes.saturating_add(purged);
        usage.first_page_id = match (usage.first_page_id, band.first_page_id) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, right) => right,
            (left, None) => left,
        };
        usage.last_page_id = match (usage.last_page_id, band.last_page_id) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, right) => right,
            (left, None) => left,
        };
    }
    zones.into_values().map(|acc| acc.usage).collect()
}

impl Default for LocalBlockStore {
    fn default() -> Self {
        let scratch = crate::scratch::owned_scratch_dir("pages");
        let store = Self::new(scratch.path());
        store
            .inner
            .lock()
            .expect("block store lock poisoned")
            .scratch = Some(scratch);
        store
    }
}

#[cfg(test)]
mod address_size_tests {
    use super::*;

    /// The shape this replaced, declared here so the comparison is measured rather than argued.
    /// Rust has no spare bit pattern in a `u64` to mean "absent", so each `Option` pays a whole
    /// extra word for its tag; five of them is the cost being removed.
    #[allow(dead_code)]
    struct OptionalShape {
        page_slab_id: u64,
        offset: u64,
        length: u64,
        page_id: Option<u64>,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        generation: Option<u64>,
        band_id: Option<u64>,
        sha256: Option<[u8; 32]>,
    }

    #[test]
    fn address_is_smaller_than_the_optional_shape() {
        let packed = std::mem::size_of::<BlockAddress>();
        let optional = std::mem::size_of::<OptionalShape>();
        assert!(
            packed < optional,
            "packing should shrink the address: {packed} vs {optional}"
        );
        // Guards the win rather than merely observing it: every page in the index holds one of
        // these for the life of the shard, so a regression here is a per-page regression.
        assert!(packed <= 104, "address grew to {packed} bytes");
    }

    /// A presence bit is not the same as a zero value. `0` is a legitimate `band_id` and a
    /// legitimate `routing_slot`, so "zero means absent" would erase real values -- this is why
    /// the byte exists instead of a sentinel.
    #[test]
    fn zero_is_distinguishable_from_absent() {
        let zero = BlockAddress::from_parts(1, 0, 0, None, None, Some(0), None, Some(0));
        let absent = BlockAddress::from_parts(1, 0, 0, None, None, None, None, None);
        assert_eq!(zero.band_id(), Some(0));
        assert_eq!(zero.routing_bucket(), Some(0));
        assert_eq!(absent.band_id(), None);
        assert_eq!(absent.routing_bucket(), None);
        assert_ne!(zero, absent);
    }

    /// Clearing a value must clear its bit, or the next read reports a stale one as present.
    #[test]
    fn setters_track_presence_both_ways() {
        let mut address =
            BlockAddress::from_parts(1, 0, 0, Some(7), None, None, None, None);
        assert_eq!(address.page_id(), Some(7));
        address.set_page_id(None);
        assert_eq!(address.page_id(), None);
        address.set_page_id(Some(9));
        assert_eq!(address.page_id(), Some(9));
        address.set_object_id(Some(3));
        assert_eq!(address.object_id(), Some(3));
        assert_eq!(address.page_id(), Some(9), "one setter disturbed another");
    }

    /// The packing is an in-memory concern: the serialized form must be unchanged, including the
    /// renames an older index on disk was written with.
    #[test]
    fn wire_form_survives_the_packing() {
        let address = BlockAddress::from_parts(
            5,
            64,
            128,
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(0),
        );
        let json = serde_json::to_value(&address).unwrap();
        assert_eq!(json["page_segment_id"], 5);
        assert_eq!(json["routing_slot"], 3);
        assert_eq!(json["band_id"], 0, "a present zero must still be written");
        assert!(json.get("present").is_none(), "presence bits must not reach the wire");
        let round_tripped: BlockAddress = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = LocalBlockStore::default();
        let root = store.inner.lock().unwrap().root.clone();
        assert!(root.exists(), "Default must create its scratch dir");
        let clone = store.clone();
        drop(store);
        assert!(root.exists(), "a live clone must keep the scratch dir");
        drop(clone);
        assert!(!root.exists(), "the last clone's drop must remove the scratch dir");
    }

    #[test]
    fn explicit_root_survives_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        drop(store);
        assert!(dir.path().exists(), "a caller-supplied root must never be deleted");
    }

    #[test]
    fn active_slab_torn_tail_is_fenced_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let a1 = store.append(b"record-one").unwrap();
        let a2 = store.append(b"record-two").unwrap();
        drop(store);
        // Simulate a crash that left a partial/torn record (no valid envelope) on the ACTIVE
        // slab past the last committed record.
        let slab = slab_path(dir.path(), a2.page_slab_id);
        let clean_len = std::fs::metadata(&slab).unwrap().len();
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&slab)
                .unwrap();
            file.write_all(b"\x01\x02 torn partial record without a valid envelope")
                .unwrap();
            file.sync_all().unwrap();
        }
        assert!(std::fs::metadata(&slab).unwrap().len() > clean_len);
        // Reopen: the torn tail must be physically fenced back to the readable prefix (mirrors
        // resume-at-committed-length), not left embedded mid-slab.
        let reopened = LocalBlockStore::new(dir.path());
        assert_eq!(
            std::fs::metadata(&slab).unwrap().len(),
            clean_len,
            "torn active-slab tail must be truncated to the readable prefix on reopen"
        );
        // Committed records survive and remain readable.
        assert_eq!(reopened.read(&a1).unwrap(), b"record-one");
        assert_eq!(reopened.read(&a2).unwrap(), b"record-two");
        // A new append lands right after the fenced prefix and does not reuse a page id.
        let a3 = reopened.append(b"record-three").unwrap();
        assert_ne!(a3.page_id(), a1.page_id());
        assert_ne!(a3.page_id(), a2.page_id());
        assert_eq!(reopened.read(&a3).unwrap(), b"record-three");
    }

    #[test]
    fn per_append_does_not_reserialize_the_band_manifest_on_the_default_path() {
        // MANIFEST-CONFORMANCE FOLD no-O(n) proof: on the default single-barrier path the per-append
        // band-manifest full re-serialize (the measured O(n) aging driver -- ~961 B rewritten per
        // write, growing with the band count) is OFF the write path. Appending many records must
        // NOT rewrite `page_extent_manifest.json` each time; the catalog is deferred and made
        // durable in one shot at sync_durable()/seal. Proven by the manifest file bytes staying
        // byte-identical across a burst of appends, then changing exactly once at sync_durable.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let manifest = band_manifest_path(dir.path());
        // Land one append so the manifest exists at a known state, then snapshot it.
        store.append(b"seed").unwrap();
        store.sync_durable().unwrap();
        let after_seed = std::fs::read(&manifest).unwrap();
        // A burst of appends: NONE of them may rewrite the manifest (deferred off the write path).
        for i in 0..200u32 {
            store.append(format!("record-{i}").as_bytes()).unwrap();
        }
        assert_eq!(
            std::fs::read(&manifest).unwrap(),
            after_seed,
            "the band manifest must NOT be re-serialized per append on the default path"
        );
        // The deferred catalog materializes in one shot; now it reflects the burst (bytes grew).
        store.sync_durable().unwrap();
        assert_ne!(
            std::fs::read(&manifest).unwrap(),
            after_seed,
            "sync_durable must materialize the deferred catalog exactly once"
        );
    }

    #[test]
    fn zone_catalog_folds_bands_and_install_reconstructs_lifecycle() {
        // MANIFEST-CONFORMANCE FOLD round-trip at the block-store layer: project the band catalog into
        // the durable ZoneInfo subset, then reconstruct the band lifecycle from that projection
        // with the band-manifest file deleted -- proving the folded catalog is a lossless source
        // of the durable band state (diagnostics are recomputed from the slab separately).
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.append(b"a").unwrap();
        // Seal the first band by rolling to a new active slab.
        store.roll_slab().unwrap();
        store.append(b"b").unwrap();
        store.sync_durable().unwrap();
        let zones = store.zone_catalog(7);
        // Two bands: the sealed first slab and the active second slab.
        assert_eq!(zones.len(), 2);
        assert!(zones.iter().any(|z| z.state == crate::index_log::ZoneState::Sealed));
        assert!(zones.iter().any(|z| z.state == crate::index_log::ZoneState::Active));
        assert!(zones.iter().all(|z| z.version == 7));
        // Delete the band-manifest file so the reopened store has no cached catalog file; it
        // reconstructs bands from the durable slabs (reconcile-on-open), then we install the
        // folded catalog on top. The lifecycle states must match the pre-crash projection.
        let reopened = LocalBlockStore::new(dir.path());
        std::fs::remove_file(band_manifest_path(dir.path())).ok();
        let changed = reopened.install_zone_catalog(&zones).unwrap();
        let recovered = reopened.zone_catalog(0);
        let state_of = |slab: u64, zs: &[crate::index_log::ZoneInfo]| {
            zs.iter().find(|z| z.page_slab_id == slab).map(|z| z.state)
        };
        for zone in &zones {
            assert_eq!(
                state_of(zone.page_slab_id, &recovered),
                Some(zone.state),
                "band {} lifecycle must reconstruct from the folded catalog",
                zone.page_slab_id
            );
        }
        let _ = changed;
    }

    #[test]
    fn gc_slabs_removes_old_non_current_slabs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"old").unwrap();
        store.install_slab(2, b"keep").unwrap();

        let report = store.gc_slabs_before(2).unwrap();
        assert_eq!(report.removed_page_slab_ids, vec![0, 1]);
        assert_eq!(report.retained_page_slab_ids, vec![2]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"old".len()) as u64
        );
        assert_eq!(report.retained_physical_bytes, b"keep".len() as u64);
        assert!(report.retained_current_page_slab_ids.is_empty());
        assert!(report.retained_live_page_slab_ids.is_empty());
        assert_eq!(store.slab_ids().unwrap(), vec![2]);
    }

    /// The point of pre-allocation: once the background prepare has rolled a full slab, the
    /// next client append lands on the fresh one without rolling inline.
    ///
    /// Drives the threshold through the explicit-target API so the test never touches the
    /// process-wide slab-target env var.
    #[test]
    fn prepare_next_slab_takes_the_roll_off_the_append_path() {
        const TARGET: u64 = 2048;
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());

        let payload = vec![b'x'; 512];
        let mut guard = 0;
        let mut full_slab = 0;
        while !store.needs_slab_preparation_with_target(TARGET) {
            full_slab = store.append(&payload).unwrap().page_slab_id;
            guard += 1;
            assert!(guard < 200, "filled {guard} times without reaching the target");
        }

        let rolled = store
            .prepare_next_slab_with_target(TARGET)
            .unwrap()
            .expect("a slab at target must roll");
        assert_eq!(rolled.previous_page_slab_id, full_slab);
        assert!(rolled.new_page_slab_id > full_slab);

        // The next append lands on the fresh slab, and did not have to roll to get there.
        let after = store.append(&payload).unwrap();
        assert_eq!(after.page_slab_id, rolled.new_page_slab_id);
    }

    /// Prepare must be a no-op while the slab has room, or a background cycle would shred the
    /// store into one slab per pass.
    #[test]
    fn prepare_next_slab_is_a_noop_below_target() {
        const TARGET: u64 = 1 << 20;
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.append(b"small").unwrap();

        assert!(!store.needs_slab_preparation_with_target(TARGET));
        assert!(store.prepare_next_slab_with_target(TARGET).unwrap().is_none());
        // Repeated passes must stay no-ops.
        assert!(store.prepare_next_slab_with_target(TARGET).unwrap().is_none());
        assert_eq!(store.slab_ids().unwrap().len(), 1);
    }

    /// A freshly rolled, still-empty slab must not roll again — otherwise prepare would mint
    /// an empty slab on every cycle forever.
    #[test]
    fn prepare_next_slab_does_not_roll_an_empty_slab() {
        const TARGET: u64 = 2048;
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let payload = vec![b'y'; 512];
        let mut guard = 0;
        while !store.needs_slab_preparation_with_target(TARGET) {
            store.append(&payload).unwrap();
            guard += 1;
            assert!(guard < 200, "filled {guard} times without reaching the target");
        }
        store
            .prepare_next_slab_with_target(TARGET)
            .unwrap()
            .expect("first roll");

        assert!(!store.needs_slab_preparation_with_target(TARGET));
        assert!(store.prepare_next_slab_with_target(TARGET).unwrap().is_none());
    }

    /// Pre-allocation is an optimisation, not a correctness requirement: with prepare never
    /// called, every payload written across an explicit roll must still read back.
    #[test]
    fn data_survives_when_prepare_never_runs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let payload = vec![b'z'; 512];
        let mut addresses = Vec::new();
        for round in 0..8 {
            addresses.push(store.append(&payload).unwrap());
            if round % 3 == 2 {
                store.roll_slab().unwrap();
            }
        }
        assert!(store.slab_ids().unwrap().len() > 1, "rolls must have happened");
        for address in &addresses {
            assert_eq!(store.read(address).unwrap(), payload);
        }
    }

    #[test]
    fn roll_slab_moves_future_appends_to_fresh_slab() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        assert_eq!(first.page_slab_id, 0);

        let roll = store.roll_slab().unwrap();
        assert_eq!(roll.previous_page_slab_id, 0);
        assert_eq!(roll.new_page_slab_id, 1);
        let second = store.append(b"second").unwrap();
        assert_eq!(second.page_slab_id, 1);
        assert_eq!(second.offset, 0);
        assert_eq!(store.read(&first).unwrap(), b"first");
        assert_eq!(store.read(&second).unwrap(), b"second");
    }

    #[test]
    fn reopened_store_appends_to_latest_existing_slab() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let roll = store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(roll.new_page_slab_id, second.page_slab_id);

        let reopened = LocalBlockStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.page_slab_id, second.page_slab_id);
        assert!(third.offset > second.offset);
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    // shared-corpus: storage_stream_manifest_disk_reconciliation;
    #[test]
    fn reopen_reconciles_manifest_missing_existing_stream_band() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        drop(store);

        let manifest_path = band_manifest_path(dir.path());
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["bands"] = serde_json::json!([manifest["bands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|band| band["page_segment_id"] == serde_json::json!(first.page_slab_id))
            .unwrap()
            .clone()]);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let reopened = LocalBlockStore::new(dir.path());
        let descriptors = reopened.band_descriptors();

        assert!(descriptors
            .iter()
            .any(|band| band.page_slab_id == first.page_slab_id
                && band.state == BlockStoreBandState::Sealed));
        assert!(descriptors
            .iter()
            .any(|band| band.page_slab_id == second.page_slab_id
                && band.state == BlockStoreBandState::Active));
        let report = reopened.stream_backed_band_runtime_report().unwrap();
        assert!(report.band_manifest_reconciled_on_open);
        assert!(report.band_manifest_disk_consistent);
        assert_eq!(report.manifest_extra_stream_bands, 0);
        assert_eq!(report.manifest_missing_stream_bands, 0);
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
    }

    // shared-corpus: storage_stream_manifest_disk_reconciliation;
    #[test]
    fn reopen_marks_manifest_band_without_stream_file_as_purged() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        drop(store);

        fs::remove_file(slab_path(dir.path(), first.page_slab_id)).unwrap();

        let reopened = LocalBlockStore::new(dir.path());
        let descriptors = reopened.band_descriptors();

        assert!(descriptors
            .iter()
            .any(|band| band.page_slab_id == first.page_slab_id
                && band.state == BlockStoreBandState::Purged));
        assert!(descriptors
            .iter()
            .any(|band| band.page_slab_id == second.page_slab_id
                && band.state == BlockStoreBandState::Active));
        let report = reopened.stream_backed_band_runtime_report().unwrap();
        assert!(report.band_manifest_reconciled_on_open);
        assert!(report.band_manifest_disk_consistent);
        assert_eq!(report.manifest_extra_stream_bands, 0);
        assert_eq!(report.manifest_missing_stream_bands, 0);
        assert_eq!(reopened.read(&second).unwrap(), b"second");
    }

    #[test]
    fn installed_higher_slab_becomes_current_for_future_appends() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(3, b"restored-segment").unwrap();

        let next = store.append(b"after-restore").unwrap();

        assert_eq!(next.page_slab_id, 3);
        assert_eq!(next.offset, b"restored-segment".len() as u64);
        assert_eq!(next.compact_slab_id(), Some(3));
        assert_eq!(
            next.compact_slab_offset(),
            Some(b"restored-segment".len() as u32)
        );
        assert_eq!(
            next.compact_slab_address(),
            Some((3_u64 << 32) | b"restored-segment".len() as u64)
        );
        let from_compact_slab = BlockAddress::from_compact_slab_address(
            next.compact_slab_address().unwrap(),
            next.length,
        );
        assert_eq!(from_compact_slab.page_slab_id, next.page_slab_id);
        assert_eq!(from_compact_slab.offset, next.offset);
        assert_eq!(from_compact_slab.length, next.length);
        assert_eq!(store.read(&next).unwrap(), b"after-restore");
    }

    /// What the page envelope actually carries, and therefore what removing the index copy costs.
    ///
    /// Written before assuming: a v7 record stores a CRC32C in its checksum field, not a SHA-256
    /// (v6 and earlier stored the full digest). So dropping  from the address does NOT
    /// relocate the digest -- for a current record the SHA-256 is not recoverable at all. What
    /// survives is verification, which is the property reads depend on, and that is asserted by
    /// the corruption test below.
    #[test]
    fn the_envelope_carries_a_crc_not_a_digest() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let payload = b"digest-lives-with-the-page";
        let address = store.append(payload).unwrap();

        let path = slab_path(dir.path(), address.page_slab_id);
        let slab = fs::read(&path).unwrap();
        let start = address.offset as usize;
        let record = &slab[start..start + address.length as usize];
        let field = &record[28..60];

        assert_ne!(
            hex::encode(field),
            record::sha256_hex(payload),
            "if this ever matches, the envelope holds a full digest and the index copy could              genuinely be relocated rather than dropped"
        );
        assert_eq!(
            &field[4..8],
            b"C32C",
            "a v7 record marks its checksum field as a crc32c"
        );
        assert_eq!(std::mem::size_of::<BlockAddress>(), 64);
    }

    #[test]
    fn page_address_checksum_rejects_corrupt_slab_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"verified-page").unwrap();
        // The address no longer carries a digest. The page does, and a read still verifies
        // against it -- corrupting the slab below must still be caught.
        assert_eq!(store.read(&address).unwrap(), b"verified-page");

        let path = slab_path(dir.path(), address.page_slab_id);
        let mut slab = fs::read(&path).unwrap();
        *slab.last_mut().unwrap() ^= 0xff;
        fs::write(path, slab).unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::ChecksumMismatch { .. }));
    }

    // shared-corpus: storage_object_page_bucket_parity_surfaces;
    #[test]
    fn page_address_matches_compact_slab_metadata_contract_and_checksum_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store
            .append_with_page_metadata(b"address-contract", Some(4242), Some(17))
            .unwrap();

        assert_eq!(address.page_slab_id, 0);
        assert_eq!(address.offset, 0);
        assert!(address.length > b"address-contract".len() as u64);
        assert_eq!(address.page_id(), Some(0));
        assert_eq!(address.object_id(), Some(4242));
        assert_eq!(address.routing_bucket(), Some(17));
        assert_eq!(address.band_id(), Some(0));
        assert_eq!(address.compact_slab_id(), Some(0));
        assert_eq!(address.compact_slab_offset(), Some(0));
        assert_eq!(address.compact_slab_address(), Some(0));
        let from_compact_slab = BlockAddress::from_compact_slab_address(
            address.compact_slab_address().unwrap(),
            address.length,
        );
        assert_eq!(
            from_compact_slab.page_slab_id,
            address.page_slab_id
        );
        assert_eq!(from_compact_slab.offset, address.offset);
        assert_eq!(from_compact_slab.length, address.length);
        assert_eq!(store.read(&address).unwrap(), b"address-contract");

        let legacy_alias_json = serde_json::json!({
            "page_segment_id": address.page_slab_id,
            "offset": address.offset,
            "length": address.length,
            "page_id": address.page_id(),
            "object_id": address.object_id(),
            "routing_slot": address.routing_bucket(),
            // generation is a canonical, always-present field on write (append sets
            // Some(page_id)) and on read (record decode derives it), so the legacy
            // alias JSON must carry it or the round-trip deserializes to None.
            "generation": address.generation(),
            "band_id": address.band_id(),
            // A document written before the digest left the index carries it under this
            // alias. It must still LOAD -- accepted and ignored -- which is what this asserts.
            "checksum": sha256_hex(b"address-contract"),
        });
        let from_checksum_alias: BlockAddress = serde_json::from_value(legacy_alias_json).unwrap();
        assert_eq!(from_checksum_alias, address);
        assert_eq!(
            serde_json::to_value(&address).unwrap().get("sha256"),
            None,
            "an index written now omits the digest; the page envelope carries it"
        );
        assert_eq!(
            serde_json::json!(sha256_hex(b"address-contract")),
            serde_json::json!(sha256_hex(b"address-contract"))
        );
    }

    #[test]
    fn page_slab_records_have_self_describing_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"enveloped-page").unwrap();
        let raw = store.read_slab(address.page_slab_id).unwrap();

        assert!(raw.starts_with(PAGE_RECORD_MAGIC));
        assert_eq!(raw[8], PAGE_RECORD_VERSION);
        assert_eq!(address.page_id(), Some(0));
        assert_eq!(store.read(&address).unwrap(), b"enveloped-page");
    }

    #[test]
    fn page_ids_are_persisted_and_continue_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(first.page_id(), Some(0));
        assert_eq!(second.page_id(), Some(1));

        let reopened = LocalBlockStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.page_id(), Some(2));
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    #[test]
    fn installed_slab_page_ids_advance_future_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let source_dir = tempfile::tempdir().unwrap();
        let source = LocalBlockStore::new(source_dir.path());
        let _ = source.append(b"first").unwrap();
        let restored = source.append(b"restored").unwrap();
        let restored_bytes = source.read_slab(restored.page_slab_id).unwrap();

        let store = LocalBlockStore::new(dir.path());
        store.install_slab(4, &restored_bytes).unwrap();
        let next = store.append(b"next").unwrap();

        assert_eq!(next.page_id(), Some(2));
        assert_eq!(store.read(&next).unwrap(), b"next");
    }

    #[test]
    fn page_id_mismatch_rejects_corrupt_address_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let mut address = store.append(b"identity-checked-page").unwrap();
        address.set_page_id(Some(address.page_id().unwrap() + 1));

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn object_ids_are_persisted_and_checked_in_page_envelopes() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let mut address = store
            .append_with_page_metadata(b"object-page", Some(42), Some(7))
            .unwrap();

        assert_eq!(address.object_id(), Some(42));
        assert_eq!(address.routing_bucket(), Some(7));
        assert_eq!(address.band_id(), Some(0));
        assert_eq!(store.read(&address).unwrap(), b"object-page");

        address.set_object_id(Some(43));
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));

        address.set_object_id(Some(42));
        address.set_routing_bucket(Some(8));
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));

        address.set_routing_bucket(Some(7));
        address.set_band_id(Some(1));
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn rolled_slabs_stamp_new_band_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-band").unwrap();
        let roll = store.roll_slab().unwrap();
        let second = store.append(b"second-band").unwrap();

        assert_eq!(first.band_id(), Some(first.page_slab_id));
        assert_eq!(second.band_id(), Some(second.page_slab_id));
        assert_eq!(second.band_id(), Some(roll.new_page_slab_id));
        assert_ne!(first.band_id(), second.band_id());
    }

    #[test]
    fn band_manifest_tracks_roll_reopen_gc_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-band").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second-band").unwrap();

        let bands = store.band_descriptors();
        assert_eq!(bands.len(), 2);
        assert_eq!(bands[0].page_slab_id, first.page_slab_id);
        assert_eq!(bands[0].state, BlockStoreBandState::Sealed);
        assert_eq!(bands[0].first_page_id, first.page_id());
        assert_eq!(bands[0].last_page_id, first.page_id());
        assert!(bands[0].created_unix_ms.is_some());
        assert!(bands[0].updated_unix_ms.is_some());
        assert_eq!(bands[1].page_slab_id, second.page_slab_id);
        assert_eq!(bands[1].state, BlockStoreBandState::Active);
        assert_eq!(bands[1].first_page_id, second.page_id());
        assert_eq!(bands[1].last_page_id, second.page_id());
        assert!(bands[1].created_unix_ms.is_some());
        assert!(bands[1].updated_unix_ms.is_some());
        assert!(band_manifest_path(dir.path()).exists());
        let initial_summary = store.band_summary();
        assert_eq!(initial_summary.sealed_bands, 1);
        assert_eq!(initial_summary.active_bands, 1);
        assert_eq!(initial_summary.delayed_destroy_bands, 0);
        assert_eq!(initial_summary.purged_bands, 0);
        assert_eq!(
            initial_summary.sealed_physical_bytes,
            bands[0].physical_bytes
        );
        assert_eq!(
            initial_summary.active_physical_bytes,
            bands[1].physical_bytes
        );
        assert_eq!(
            initial_summary.live_physical_bytes,
            bands[0].physical_bytes + bands[1].physical_bytes
        );
        assert_eq!(initial_summary.reclaimable_physical_bytes, 0);
        assert!(initial_summary.oldest_known_band_unix_ms.is_some());
        assert!(initial_summary.oldest_known_band_age_ms.is_some());
        assert!(initial_summary.oldest_live_band_unix_ms.is_some());
        assert!(initial_summary.oldest_live_band_age_ms.is_some());
        assert!(initial_summary.oldest_reclaimable_band_unix_ms.is_none());
        assert!(initial_summary.oldest_reclaimable_band_age_ms.is_none());
        let initial_zone_usage = store.zone_usage();
        assert_eq!(initial_zone_usage.len(), 2);
        assert_eq!(initial_zone_usage[0].band_id, bands[0].band_id);
        assert_eq!(
            initial_zone_usage[0].page_slab_id,
            bands[0].page_slab_id
        );
        assert_eq!(
            initial_zone_usage[0].page_store_used_bytes,
            bands[0].physical_bytes
        );
        assert_eq!(
            initial_zone_usage[0].live_page_store_used_bytes,
            bands[0].physical_bytes
        );
        assert_eq!(initial_zone_usage[0].reclaimable_page_store_used_bytes, 0);
        assert_eq!(initial_zone_usage[0].purged_page_store_used_bytes, 0);

        let reopened = LocalBlockStore::new(dir.path());
        let reopened_bands = reopened.band_descriptors();
        assert_eq!(reopened_bands.len(), bands.len());
        assert_eq!(reopened_bands[0], bands[0]);
        assert_eq!(
            reopened_bands[1].page_slab_id,
            bands[1].page_slab_id
        );
        assert_eq!(reopened_bands[1].state, bands[1].state);
        assert_eq!(
            reopened_bands[1].physical_bytes,
            bands[1].physical_bytes
        );
        assert_eq!(reopened_bands[1].logical_bytes, bands[1].logical_bytes);
        assert_eq!(
            reopened_bands[1].created_unix_ms,
            bands[1].created_unix_ms
        );
        assert!(reopened_bands[1].updated_unix_ms >= bands[1].updated_unix_ms);

        let report = reopened
            .gc_slabs_before_with_live_refs_delayed_destroy(1, std::iter::empty())
            .unwrap();
        assert_eq!(report.delayed_destroy_page_slab_ids, vec![0]);
        let delayed = reopened.band_descriptors();
        assert_eq!(delayed[0].state, BlockStoreBandState::DelayedDestroy);
        assert!(delayed[0].physical_bytes > 0);
        assert_eq!(delayed[0].created_unix_ms, bands[0].created_unix_ms);
        assert!(delayed[0].updated_unix_ms >= bands[0].updated_unix_ms);
        assert_eq!(delayed[1].state, BlockStoreBandState::Active);
        let delayed_summary = reopened.band_summary();
        assert_eq!(delayed_summary.delayed_destroy_bands, 1);
        assert_eq!(delayed_summary.active_bands, 1);
        assert_eq!(
            delayed_summary.delayed_destroy_physical_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(
            delayed_summary.reclaimable_physical_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(
            delayed_summary.live_physical_bytes,
            delayed[1].physical_bytes
        );
        assert!(delayed_summary.oldest_known_band_unix_ms.is_some());
        assert!(delayed_summary.oldest_live_band_unix_ms.is_some());
        assert_eq!(
            delayed_summary.oldest_reclaimable_band_unix_ms,
            delayed[0].updated_unix_ms
        );
        assert!(delayed_summary.oldest_reclaimable_band_age_ms.is_some());
        let delayed_zone_usage = reopened.zone_usage();
        let delayed_first = delayed_zone_usage
            .iter()
            .find(|zone| zone.page_slab_id == first.page_slab_id)
            .unwrap();
        assert_eq!(
            delayed_first.reclaimable_page_store_used_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(delayed_first.live_page_store_used_bytes, 0);

        let purge = reopened
            .purge_delayed_destroy_slabs_with_report()
            .unwrap();
        assert_eq!(purge.purged_page_slab_ids, vec![0]);
        assert!(purge.purged_physical_bytes > 0);
        let purged = LocalBlockStore::new(dir.path()).band_descriptors();
        assert_eq!(purged[0].state, BlockStoreBandState::Purged);
        assert_eq!(purged[0].created_unix_ms, bands[0].created_unix_ms);
        assert!(purged[0].updated_unix_ms >= delayed[0].updated_unix_ms);
        assert_eq!(purged[1].state, BlockStoreBandState::Active);
        let purged_summary = LocalBlockStore::new(dir.path()).band_summary();
        assert_eq!(purged_summary.purged_bands, 1);
        assert_eq!(purged_summary.active_bands, 1);
        assert_eq!(
            purged_summary.purged_physical_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_summary.live_physical_bytes, purged[1].physical_bytes);
        assert_eq!(purged_summary.reclaimable_physical_bytes, 0);
        let purged_zone_usage = LocalBlockStore::new(dir.path()).zone_usage();
        let purged_first = purged_zone_usage
            .iter()
            .find(|zone| zone.page_slab_id == first.page_slab_id)
            .unwrap();
        assert_eq!(
            purged_first.purged_page_store_used_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_first.reclaimable_page_store_used_bytes, 0);
        assert!(purged_summary.oldest_known_band_unix_ms.is_some());
        assert!(purged_summary.oldest_live_band_unix_ms.is_some());
        assert!(purged_summary.oldest_reclaimable_band_unix_ms.is_none());
        assert!(purged_summary.oldest_reclaimable_band_age_ms.is_none());
    }

    #[test]
    fn missing_band_manifest_rebuilds_from_existing_slabs() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"first-band").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second-band").unwrap();
        fs::remove_file(band_manifest_path(dir.path())).unwrap();

        let rebuilt = LocalBlockStore::new(dir.path());
        let bands = rebuilt.band_descriptors();

        assert_eq!(bands.len(), 2);
        assert_eq!(bands[0].page_slab_id, first.page_slab_id);
        assert_eq!(bands[0].state, BlockStoreBandState::Sealed);
        assert_eq!(bands[0].first_page_id, first.page_id());
        assert_eq!(bands[0].last_page_id, first.page_id());
        assert!(bands[0].created_unix_ms.is_some());
        assert!(bands[0].updated_unix_ms.is_some());
        assert_eq!(bands[1].page_slab_id, second.page_slab_id);
        assert_eq!(bands[1].state, BlockStoreBandState::Active);
        assert_eq!(bands[1].first_page_id, second.page_id());
        assert_eq!(bands[1].last_page_id, second.page_id());
        assert!(bands[1].created_unix_ms.is_some());
        assert!(bands[1].updated_unix_ms.is_some());
        assert!(band_manifest_path(dir.path()).exists());

        let report = rebuilt.stream_backed_band_runtime_report().unwrap();
        assert!(report.runtime_ready, "{report:?}");
        assert_eq!(report.band_lifecycle_states, vec!["active", "sealed"]);
        assert!(report.band_manifest_ready);
        assert!(report.band_manifest_rebuild_ready);
        assert!(report.zone_stats_ready);
        assert_eq!(report.zone_usage.len(), 2);
        assert_eq!(
            report
                .zone_usage
                .iter()
                .map(|zone| zone.page_store_used_bytes)
                .sum::<u64>(),
            report.physical_bytes
        );
        assert!(!report.band_manifest_reconciled_on_open);
        assert!(report.band_manifest_disk_consistent);
        assert_eq!(report.manifest_missing_stream_bands, 0);
        assert_eq!(report.manifest_extra_stream_bands, 0);
        assert_eq!(report.corrupt_band_count, 0);
        assert_eq!(report.partial_band_count, 0);
        assert!(report.partial_band_recovery_ready);
        assert_eq!(report.readable_prefix_physical_bytes, report.physical_bytes);
    }

    // shared-corpus: storage_stream_partial_band_rebuild;
    #[test]
    fn partial_band_manifest_rebuild_preserves_readable_prefix_and_reports_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"sealed-readable-prefix".repeat(64);
        let first = store.append(&first_payload).unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"active-clean-tail").unwrap();

        let first_slab = slab_path(dir.path(), first.page_slab_id);
        let readable_prefix = fs::metadata(&first_slab).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&first_slab)
            .unwrap()
            .write_all(b"partial-corrupt-tail")
            .unwrap();
        fs::remove_file(band_manifest_path(dir.path())).unwrap();

        let rebuilt = LocalBlockStore::new(dir.path());
        assert_eq!(rebuilt.read(&first).unwrap(), first_payload);
        assert_eq!(rebuilt.read(&second).unwrap(), b"active-clean-tail");

        let bands = rebuilt.band_descriptors();
        let sealed = bands
            .iter()
            .find(|band| band.page_slab_id == first.page_slab_id)
            .unwrap();
        assert_eq!(sealed.state, BlockStoreBandState::Sealed);
        assert!(sealed.has_corruption);
        assert_eq!(sealed.first_error_offset, Some(readable_prefix));
        assert_eq!(sealed.readable_prefix_physical_bytes, readable_prefix);
        assert_eq!(sealed.first_page_id, first.page_id());
        assert_eq!(sealed.last_page_id, first.page_id());
        assert!(sealed
            .first_error
            .as_deref()
            .unwrap_or_default()
            .contains("mixed raw bytes"));
        assert!(band_manifest_path(dir.path()).exists());

        let report = rebuilt.stream_backed_band_runtime_report().unwrap();
        assert!(!report.runtime_ready, "{report:?}");
        assert!(report.band_manifest_ready);
        assert!(report.band_manifest_rebuild_ready);
        assert!(!report.band_manifest_reconciled_on_open);
        assert!(report.band_manifest_disk_consistent);
        assert_eq!(report.manifest_missing_stream_bands, 0);
        assert_eq!(report.manifest_extra_stream_bands, 0);
        assert_eq!(report.band_lifecycle_states, vec!["active", "sealed"]);
        assert_eq!(report.corrupt_band_count, 1);
        assert_eq!(report.partial_band_count, 1);
        assert_eq!(
            report.readable_prefix_physical_bytes,
            readable_prefix + second.length
        );
        assert!(report.partial_band_recovery_ready);
        assert!(!report.envelope_checksum_ready);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("readable prefix was preserved")));
    }

    #[test]
    fn logical_page_range_skips_record_envelopes_across_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.append(b"abc").unwrap();
        store.append(b"def").unwrap();

        assert_eq!(store.read_logical_range(0, 1, 4).unwrap(), b"bcde");
    }

    #[test]
    fn read_range_and_logical_range_drive_shared_slab_read_through() {
        // On-demand lazy recovery must cover band-report / streaming reads too:
        // read_range and read_logical_range on a metadata-only restored node must pull a
        // not-yet-fetched checkpoint slab from shared storage on first access (previously
        // they hit a local File::open miss instead of the shared read-through).
        #[derive(Debug)]
        struct OneSlabSource {
            page_slab_id: u64,
            bytes: Vec<u8>,
        }
        impl SharedSlabSource for OneSlabSource {
            fn fetch_slab(&self, page_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
                Ok((page_slab_id == self.page_slab_id).then(|| self.bytes.clone()))
            }
        }

        // Producer writes a real slab; capture its raw bytes to serve lazily to fresh nodes.
        let producer_dir = tempfile::tempdir().unwrap();
        let producer = LocalBlockStore::new(producer_dir.path());
        producer.append(b"abc").unwrap();
        producer.append(b"def").unwrap();
        let raw = producer.read_slab(0).unwrap();
        drop(producer);

        // read_range: fresh node, slab absent locally, shared source attached.
        let range_dir = tempfile::tempdir().unwrap();
        let range_node = LocalBlockStore::new(range_dir.path());
        range_node.attach_shared_slab_source(Arc::new(OneSlabSource {
            page_slab_id: 0,
            bytes: raw.clone(),
        }));
        assert!(
            !range_node.slab_ids().unwrap().contains(&0),
            "slab must not be installed before the range read"
        );
        assert_eq!(range_node.stats().shared_slab_fetches, 0);
        let raw_prefix = range_node.read_range(0, 0, 3).unwrap();
        assert_eq!(
            range_node.stats().shared_slab_fetches, 1,
            "read_range must fetch the missing slab exactly once"
        );
        assert_eq!(raw_prefix.len(), 3);
        assert_eq!(raw, range_node.read_slab(0).unwrap());
        // Cached now: a second range read must not re-fetch.
        let _ = range_node.read_range(0, 0, 3).unwrap();
        assert_eq!(
            range_node.stats().shared_slab_fetches, 1,
            "cached slab: no re-fetch"
        );

        // read_logical_range: independent fresh node so its fetch count starts at 0.
        let logical_dir = tempfile::tempdir().unwrap();
        let logical_node = LocalBlockStore::new(logical_dir.path());
        logical_node.attach_shared_slab_source(Arc::new(OneSlabSource {
            page_slab_id: 0,
            bytes: raw.clone(),
        }));
        assert_eq!(logical_node.stats().shared_slab_fetches, 0);
        let logical = logical_node.read_logical_range(0, 1, 4).unwrap();
        assert_eq!(
            logical_node.stats().shared_slab_fetches, 1,
            "read_logical_range must fetch the missing slab exactly once"
        );
        assert_eq!(logical, b"bcde");
    }

    #[test]
    fn compressed_page_records_round_trip_and_remain_logical() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();
        let raw = store.read_slab(first.page_slab_id).unwrap();

        assert!(first.length < (PAGE_RECORD_HEADER_LEN + first_payload.len()) as u64);
        assert!(second.length < (PAGE_RECORD_HEADER_LEN + second_payload.len()) as u64);
        assert_eq!(store.read(&first).unwrap(), first_payload);
        assert_eq!(store.read(&second).unwrap(), second_payload);

        let logical_offset = first_payload.len() as u64 - 3;
        let logical = store
            .read_logical_range(first.page_slab_id, logical_offset, 12)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 3..]);
        expected.extend_from_slice(&second_payload[..9]);
        assert_eq!(logical, expected);
        assert_eq!(raw[8], PAGE_RECORD_VERSION);
        assert_eq!(raw[92], PAGE_RECORD_COMPRESSION_ZSTD);

        let stats = store.stats();
        assert_eq!(stats.writes, 2);
        assert_eq!(
            stats.logical_bytes_written,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(stats.compressed_records_written, 2);
        assert_eq!(stats.compressed_records_read, 4);
        assert!(stats.compression_bytes_saved > 0);
        assert!(stats.bytes_written < stats.logical_bytes_written);
        assert!(stats.logical_bytes_read >= stats.bytes_read);
    }

    // shared-corpus: storage_stream_backed_band_runtime;
    #[test]
    fn stream_backed_band_runtime_report_covers_roll_read_manifest_and_delayed_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"band-stream-first-".repeat(96);
        let second_payload = b"band-stream-second-".repeat(96);
        let first = store
            .append_with_page_metadata(&first_payload, Some(11), Some(7))
            .unwrap();
        let second = store
            .append_with_page_metadata(&second_payload, Some(12), Some(7))
            .unwrap();
        assert_eq!(first.page_slab_id, second.page_slab_id);

        let logical_offset = first_payload.len() as u64 - 8;
        let logical = store
            .read_logical_range(first.page_slab_id, logical_offset, 16)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 8..]);
        expected.extend_from_slice(&second_payload[..8]);
        assert_eq!(logical, expected);

        let roll = store.roll_slab().unwrap();
        let third_payload = b"band-stream-third-".repeat(96);
        let third = store
            .append_with_page_metadata(&third_payload, Some(13), Some(8))
            .unwrap();
        assert_eq!(third.page_slab_id, roll.new_page_slab_id);
        let before_gc = store.stream_backed_band_runtime_report().unwrap();
        assert!(before_gc.runtime_ready, "{before_gc:?}");
        assert_eq!(before_gc.active_bands, 1);
        assert_eq!(before_gc.sealed_bands, 1);
        assert_eq!(before_gc.band_lifecycle_states, vec!["active", "sealed"]);
        assert_eq!(before_gc.stream_record_count, 3);
        assert_eq!(before_gc.first_page_id, first.page_id());
        assert_eq!(before_gc.last_page_id, third.page_id());
        assert!(before_gc.page_id_continuity_ready);
        assert!(before_gc.band_manifest_rebuild_ready);
        assert!(before_gc.zone_stats_ready);
        assert_eq!(before_gc.zone_usage.len(), 2);
        assert_eq!(
            before_gc
                .zone_usage
                .iter()
                .map(|zone| zone.page_store_used_bytes)
                .sum::<u64>(),
            before_gc.physical_bytes
        );
        assert!(before_gc.logical_stream_bytes_read >= 16);
        assert!(before_gc.band_state_transition_count >= 2);

        let delayed = store
            .gc_slabs_before_with_live_refs_delayed_destroy(
                roll.new_page_slab_id,
                [roll.new_page_slab_id],
            )
            .unwrap();
        assert_eq!(
            delayed.delayed_destroy_page_slab_ids,
            vec![roll.previous_page_slab_id]
        );

        let reopened = LocalBlockStore::new(dir.path());
        assert_eq!(reopened.read(&third).unwrap(), third_payload);
        let report = reopened.stream_backed_band_runtime_report().unwrap();
        assert!(report.runtime_ready, "{report:?}");
        assert_eq!(report.active_bands, 1);
        assert_eq!(report.delayed_destroy_bands, 1);
        assert_eq!(
            report.band_lifecycle_states,
            vec!["active", "delayed_destroy"]
        );
        assert!(report.band_count >= 2);
        assert!(report.stream_slab_count >= 1);
        assert!(report.logical_stream_read_ready);
        assert!(report.append_roll_ready);
        assert!(report.band_manifest_ready);
        assert!(report.band_manifest_rebuild_ready);
        assert!(report.zone_stats_ready);
        assert!(report
            .zone_usage
            .iter()
            .any(|zone| zone.state == BlockStoreBandState::DelayedDestroy
                && zone.reclaimable_page_store_used_bytes > 0));
        assert!(report
            .zone_usage
            .iter()
            .any(|zone| zone.state == BlockStoreBandState::Active
                && zone.live_page_store_used_bytes > 0));
        assert!(report.envelope_checksum_ready);
        assert!(report.compression_stream_ready);
        assert!(report.delayed_destroy_ready);
        assert!(!report.purge_lifecycle_ready);
        assert!(report.logical_bytes >= third_payload.len() as u64);
        assert_eq!(report.stream_record_count, 1);
        assert_eq!(report.first_page_id, third.page_id());
        assert_eq!(report.last_page_id, third.page_id());
        assert!(report.page_id_continuity_ready);
        assert!(report.blockers.is_empty());
        assert!(report
            .evidence
            .iter()
            .any(|item| item.contains("logical stream reads span records")));
        assert!(report
            .evidence
            .iter()
            .any(|item| item.contains("page-id continuity")));

        let purge = reopened
            .purge_delayed_destroy_slabs_with_report()
            .unwrap();
        assert_eq!(
            purge.purged_page_slab_ids,
            vec![roll.previous_page_slab_id]
        );
        let purged = LocalBlockStore::new(dir.path())
            .stream_backed_band_runtime_report()
            .unwrap();
        assert!(purged.runtime_ready, "{purged:?}");
        assert_eq!(purged.active_bands, 1);
        assert_eq!(purged.delayed_destroy_bands, 0);
        assert_eq!(purged.purged_bands, 1);
        assert_eq!(purged.band_lifecycle_states, vec!["active", "purged"]);
        assert!(purged.zone_stats_ready);
        assert!(purged
            .zone_usage
            .iter()
            .any(|zone| zone.state == BlockStoreBandState::Purged
                && zone.purged_page_store_used_bytes > 0));
        assert!(purged.purge_lifecycle_ready);
        assert!(purged.append_roll_ready);
        assert!(purged.page_id_continuity_ready);
    }

    #[test]
    fn slab_reports_describe_page_counts_bytes_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();

        let reports = store.slab_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_slab_id, first.page_slab_id);
        assert_eq!(reports[0].physical_bytes, first.length + second.length);
        assert_eq!(
            reports[0].logical_bytes,
            (first_payload.len() + second_payload.len()) as u64
        );
        assert_eq!(reports[0].page_count, 2);
        assert_eq!(reports[0].compressed_records, 2);
        assert_eq!(
            reports[0].readable_prefix_physical_bytes,
            reports[0].physical_bytes
        );
        assert!(!reports[0].has_corruption);
        assert_eq!(reports[0].first_error_offset, None);
        assert_eq!(reports[0].first_page_id, first.page_id());
        assert_eq!(reports[0].last_page_id, second.page_id());
        assert_eq!(reports[0].block_index_count, 2);
        assert_eq!(reports[0].block_index_entries.len(), 2);
        assert_eq!(
            reports[0].block_index_entries[0].block_slab_id,
            first.page_slab_id
        );
        assert_eq!(reports[0].block_index_entries[0].offset, first.offset);
        assert_eq!(reports[0].block_index_entries[0].length, first.length);
        assert_eq!(
            reports[0].block_index_entries[0].compact_slab_address,
            first.compact_slab_address()
        );
        assert_eq!(
            reports[0].block_index_entries[0].compact_slab_id,
            first.compact_slab_id()
        );
        assert_eq!(
            reports[0].block_index_entries[0].compact_slab_offset,
            first.compact_slab_offset()
        );
        assert_eq!(reports[0].block_index_entries[0].block_id, first.page_id());
        assert_eq!(
            reports[0].block_index_entries[0].block_size,
            first_payload.len() as u64
        );
        assert!(reports[0].block_index_entries[0].stored_size < first_payload.len() as u64);
        assert!(!reports[0].block_index_entries[0].dirty);
        assert!(!reports[0].block_index_entries[0].deleted);
        assert!(!reports[0].block_index_entries[0].block_in_log);
        assert_eq!(reports[0].block_index_entries[1].offset, second.offset);
        assert_eq!(reports[0].block_index_entries[1].length, second.length);
        assert_eq!(reports[0].block_index_entries[1].block_id, second.page_id());
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn slab_reports_describe_object_and_routing_bucket_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store
            .append_with_page_metadata(b"slot-7-object-100", Some(100), Some(7))
            .unwrap();
        store
            .append_with_page_metadata(b"slot-11-object-101", Some(101), Some(11))
            .unwrap();
        store
            .append_with_page_metadata(b"slot-7-object-100-again", Some(100), Some(7))
            .unwrap();

        let reports = store.slab_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_count, 3);
        assert_eq!(reports[0].object_count, 2);
        assert_eq!(reports[0].routing_bucket_count, 2);
        assert_eq!(reports[0].first_routing_bucket, Some(7));
        assert_eq!(reports[0].last_routing_bucket, Some(11));
        assert_eq!(reports[0].block_index_count, 3);
        assert_eq!(
            reports[0]
                .block_index_entries
                .iter()
                .filter_map(|entry| entry.object_id)
                .collect::<Vec<_>>(),
            vec![100, 101, 100]
        );
        assert_eq!(
            reports[0]
                .block_index_entries
                .iter()
                .filter_map(|entry| entry.routing_bucket)
                .collect::<Vec<_>>(),
            vec![7, 11, 7]
        );
        assert!(reports[0]
            .block_index_entries
            .iter()
            .all(|entry| !entry.dirty && !entry.deleted && !entry.block_in_log));
        assert_eq!(
            reports[0].readable_prefix_physical_bytes,
            reports[0].physical_bytes
        );
        assert!(!reports[0].has_corruption);
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn slab_reports_capture_first_corrupt_record_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let first = store.append(b"healthy").unwrap();
        let second = store.append(b"damaged").unwrap();
        let path = slab_path(dir.path(), second.page_slab_id);
        let mut slab = fs::read(&path).unwrap();
        *slab.last_mut().unwrap() ^= 0xff;
        fs::write(path, slab).unwrap();

        let reports = store.slab_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].page_count, 1);
        assert_eq!(reports[0].logical_bytes, b"healthy".len() as u64);
        assert_eq!(reports[0].readable_prefix_physical_bytes, first.length);
        assert!(reports[0].has_corruption);
        assert_eq!(reports[0].first_error_offset, Some(first.length));
        assert_eq!(reports[0].first_page_id, first.page_id());
        assert_eq!(reports[0].last_page_id, first.page_id());
        let error = reports[0]
            .first_error
            .as_ref()
            .expect("corrupt second record should be reported");
        assert!(error.contains("checksum") || error.contains("corrupt page envelope"));
    }

    #[test]
    fn page_record_compression_policy_can_disable_or_raise_threshold() {
        let payload = b"policy-controlled-".repeat(80);

        let disabled_dir = tempfile::tempdir().unwrap();
        let disabled_store = LocalBlockStore::with_options(
            disabled_dir.path(),
            BlockStoreOptions {
                compression_enabled: false,
                ..BlockStoreOptions::default()
            },
        );
        let disabled_address = disabled_store.append(&payload).unwrap();
        let disabled_raw = disabled_store
            .read_slab(disabled_address.page_slab_id)
            .unwrap();

        assert_eq!(
            disabled_address.length,
            (PAGE_RECORD_HEADER_LEN + payload.len()) as u64
        );
        assert_eq!(disabled_raw[92], PAGE_RECORD_COMPRESSION_NONE);
        assert_eq!(disabled_store.read(&disabled_address).unwrap(), payload);
        assert_eq!(disabled_store.stats().compressed_records_written, 0);
        assert_eq!(disabled_store.stats().compression_bytes_saved, 0);

        let threshold_dir = tempfile::tempdir().unwrap();
        let threshold_store = LocalBlockStore::with_options(
            threshold_dir.path(),
            BlockStoreOptions {
                compression_min_bytes: payload.len() + 1,
                ..BlockStoreOptions::default()
            },
        );
        let threshold_address = threshold_store.append(&payload).unwrap();
        let threshold_raw = threshold_store
            .read_slab(threshold_address.page_slab_id)
            .unwrap();

        assert_eq!(
            threshold_address.length,
            (PAGE_RECORD_HEADER_LEN + payload.len()) as u64
        );
        assert_eq!(threshold_raw[92], PAGE_RECORD_COMPRESSION_NONE);
        assert_eq!(threshold_store.read(&threshold_address).unwrap(), payload);
        assert_eq!(threshold_store.stats().compressed_records_written, 0);
        assert_eq!(threshold_store.stats().compression_bytes_saved, 0);
    }

    #[test]
    fn page_envelope_rejects_corrupt_compressed_payload() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(&b"compress-me-".repeat(80)).unwrap();
        let path = slab_path(dir.path(), address.page_slab_id);
        let mut slab = fs::read(&path).unwrap();
        *slab.last_mut().unwrap() ^= 0xff;
        fs::write(path, slab).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(
            err,
            BlockStoreError::ChecksumMismatch { .. } | BlockStoreError::CorruptPageEnvelope { .. }
        ));
    }

    #[test]
    fn page_envelope_rejects_corrupt_header_lengths() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let address = store.append(b"header-checked-page").unwrap();
        let path = slab_path(dir.path(), address.page_slab_id);
        let mut slab = fs::read(&path).unwrap();
        slab[10] = 1;
        slab[11] = 0;
        fs::write(path, slab).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptPageEnvelope { .. }));
    }

    #[test]
    fn page_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let legacy_address = BlockAddress::from_parts(0, 0, b"alteredpage".len() as u64, None, None, None, None, None);
        fs::write(
            slab_path(dir.path(), legacy_address.page_slab_id),
            b"alteredpage",
        )
        .unwrap();

        assert_eq!(store.read(&legacy_address).unwrap(), b"alteredpage");
    }

    #[test]
    fn gc_slabs_retains_live_index_references_below_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"live").unwrap();
        store.install_slab(2, b"stale").unwrap();
        store.install_slab(3, b"keep").unwrap();

        let report = store.gc_slabs_before_with_live_refs(3, [1_u64]).unwrap();
        assert_eq!(report.removed_page_slab_ids, vec![0, 2]);
        assert_eq!(report.retained_page_slab_ids, vec![1, 3]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.retained_physical_bytes,
            (b"live".len() + b"keep".len()) as u64
        );
        assert!(report.retained_current_page_slab_ids.is_empty());
        assert_eq!(report.retained_live_page_slab_ids, vec![1]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.slab_ids().unwrap(), vec![1, 3]);
    }

    #[test]
    fn delayed_destroy_gc_quarantines_stale_slabs_before_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"stale").unwrap();
        store.install_slab(2, b"live").unwrap();
        store.install_slab(3, b"keep").unwrap();

        let report = store
            .gc_slabs_before_with_live_refs_delayed_destroy(3, [2_u64])
            .unwrap();

        assert_eq!(report.removed_page_slab_ids, vec![0, 1]);
        assert_eq!(report.delayed_destroy_page_slab_ids, vec![0, 1]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            report.removed_physical_bytes
        );
        assert_eq!(report.retained_page_slab_ids, vec![2, 3]);
        assert_eq!(report.retained_live_page_slab_ids, vec![2]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.slab_ids().unwrap(), vec![2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0, 1]);
        let delayed_reports = store.delayed_destroy_slab_reports().unwrap();
        assert_eq!(delayed_reports.len(), 2);
        assert_eq!(delayed_reports[0].page_slab_id, 0);
        assert_eq!(delayed_reports[0].physical_bytes, b"current".len() as u64);
        assert!(delayed_reports[0].modified_unix_ms.is_some());
        assert_eq!(delayed_reports[1].page_slab_id, 1);
        assert_eq!(delayed_reports[1].physical_bytes, b"stale".len() as u64);
        assert!(delayed_reports[1].modified_unix_ms.is_some());

        let purge = store.purge_delayed_destroy_slabs_with_report().unwrap();
        assert_eq!(purge.purged_page_slab_ids, vec![0, 1]);
        assert_eq!(
            purge.purged_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());
        assert!(store.delayed_destroy_slab_reports().unwrap().is_empty());
        assert_eq!(store.slab_ids().unwrap(), vec![2, 3]);
    }

    #[test]
    fn utility_gc_selects_low_utility_stale_slabs_with_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"small").unwrap();
        store.install_slab(1, b"largest-stale-segment").unwrap();
        store.install_slab(2, b"live-segment").unwrap();
        store.install_slab(3, b"current-segment").unwrap();

        let candidates = store.gc_utility_candidates(3, [2_u64]).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.page_slab_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert!(candidates
            .iter()
            .all(|candidate| candidate.utility_score == 0));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.utility_basis_points == 0));
        assert!(candidates.iter().all(|candidate| candidate.used_bytes == 0));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.stale_bytes == candidate.total_bytes));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.created_unix_ms.is_some()));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.updated_unix_ms.is_some()));
        assert!(candidates
            .iter()
            .all(|candidate| candidate.age_ms.is_some()));

        let no_op = store
            .gc_slabs_before_with_live_refs_utility(3, [2_u64], 0, true)
            .unwrap();
        assert!(no_op.removed_page_slab_ids.is_empty());
        assert_eq!(no_op.removed_physical_bytes, 0);
        assert_eq!(store.slab_ids().unwrap(), vec![0, 1, 2, 3]);

        let report = store
            .gc_slabs_before_with_live_refs_utility(3, [2_u64], 1, true)
            .unwrap();
        assert_eq!(report.removed_page_slab_ids, vec![1]);
        assert_eq!(report.delayed_destroy_page_slab_ids, vec![1]);
        assert_eq!(
            report.removed_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(report.retained_page_slab_ids, vec![0, 2, 3]);
        assert_eq!(report.retained_live_page_slab_ids, vec![2]);
        assert_eq!(
            report.retained_live_physical_bytes,
            b"live-segment".len() as u64
        );
        assert_eq!(store.slab_ids().unwrap(), vec![0, 2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![1]);
        let delayed_reports = store.delayed_destroy_slab_reports().unwrap();
        assert_eq!(delayed_reports.len(), 1);
        assert_eq!(delayed_reports[0].page_slab_id, 1);
        assert_eq!(
            delayed_reports[0].physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert!(delayed_reports[0].modified_unix_ms.is_some());
    }

    #[test]
    fn band_garbage_floor_gates_reclaim_by_garbage_ratio() {
        // garbage-ratio GC conformance: reclaim is gated on a minimum band garbage ratio
        // (garbage = 10_000 - band live-fraction). Floor 0 (the default) reclaims every
        // eligible band as before; a floor above a band's garbage ratio excludes it.
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"stale-a").unwrap();
        store.install_slab(1, b"stale-b").unwrap();
        store.install_slab(2, b"kept-above-floor").unwrap();
        let floor_zero = store
            .gc_policy_plan(
                2,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::with_band_garbage_floor(0, None),
            )
            .unwrap();
        assert_eq!(floor_zero.selected_page_slab_ids, vec![0, 1]);
        assert_eq!(floor_zero.skipped_by_policy_count, 0);
        let floor_impossible = store
            .gc_policy_plan(
                2,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::with_band_garbage_floor(10_001, None),
            )
            .unwrap();
        assert!(floor_impossible.selected_page_slab_ids.is_empty());
        assert_eq!(floor_impossible.skipped_by_policy_count, 2);
    }

    #[test]
    fn policy_gc_plans_and_applies_byte_bounded_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        store.install_slab(0, b"small").unwrap();
        store.install_slab(1, b"largest-stale-segment").unwrap();
        store.install_slab(2, b"live-segment").unwrap();
        store.install_slab(3, b"current-segment").unwrap();

        let policy = BlockStoreGcPolicy {
            max_destroy_slabs: 2,
            max_destroy_physical_bytes: b"small".len() as u64,
            max_utility_score: Some(0),
            min_age_ms: Some(0),
            min_band_garbage_basis_points: None,
        };
        let plan = store.gc_policy_plan(3, [2_u64], &policy).unwrap();
        assert_eq!(plan.retain_from_page_slab_id, 3);
        assert_eq!(plan.candidate_count, 2);
        assert_eq!(
            plan.candidate_physical_bytes,
            (b"small".len() + b"largest-stale-segment".len()) as u64
        );
        assert_eq!(plan.candidate_total_bytes, plan.candidate_physical_bytes);
        assert_eq!(plan.candidate_used_bytes, 0);
        assert_eq!(plan.candidate_stale_bytes, plan.candidate_physical_bytes);
        assert_eq!(plan.candidate_utility_basis_points, 0);
        assert_eq!(plan.selected_page_slab_ids, vec![0]);
        assert_eq!(plan.selected_physical_bytes, b"small".len() as u64);
        assert_eq!(plan.skipped_by_policy_count, 0);
        assert_eq!(plan.skipped_by_policy_physical_bytes, 0);
        assert_eq!(plan.skipped_by_budget_count, 1);
        assert_eq!(
            plan.skipped_by_budget_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(
            plan.candidates
                .iter()
                .map(|candidate| candidate.page_slab_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );

        let report = store
            .gc_slabs_before_with_live_refs_policy(3, [2_u64], policy, true)
            .unwrap();
        assert_eq!(report.removed_page_slab_ids, vec![0]);
        assert_eq!(report.delayed_destroy_page_slab_ids, vec![0]);
        assert_eq!(report.retained_page_slab_ids, vec![1, 2, 3]);
        assert_eq!(store.slab_ids().unwrap(), vec![1, 2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    }

    /// Install, quarantine and purge, timed apart.
    ///
    /// A purge unlinks every quarantined slab in one round with the store's lock held, so the round
    /// is unbounded in the amount of work it does. Whether that is the expensive part, or whether
    /// getting there is, is what this separates -- an earlier attempt timed all three together at
    /// twenty thousand slabs and did not finish in an hour.
    #[test]
    fn quarantine_and_purge_timed_by_phase() {
        for slabs in [200u64, 800, 3_200] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalBlockStore::new(dir.path());

            let started = std::time::Instant::now();
            for id in 0..slabs {
                store.install_slab(id, b"slab-contents").unwrap();
            }
            let install = started.elapsed().as_secs_f64() * 1e3;

            let started = std::time::Instant::now();
            store
                .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
                .unwrap();
            let quarantine = started.elapsed().as_secs_f64() * 1e3;

            let started = std::time::Instant::now();
            let report = store.purge_delayed_destroy_slabs_with_report().unwrap();
            let purge = started.elapsed().as_secs_f64() * 1e3;

            println!(
                "  {slabs:>5} slabs: install {install:>9.1} ms ({:>6.3} ms each)   quarantine {quarantine:>9.1} ms   purge {purge:>8.1} ms ({} destroyed)",
                install / slabs as f64,
                report.purged_page_slab_ids.len()
            );
        }
    }

    /// Installing slabs must not cost more as the store fills up.
    ///
    /// Writing the band manifest costs the whole manifest, so writing it per install made
    /// installing n slabs cost n manifests: measured at 111.7 ms per install with two hundred slabs
    /// in the store and 270.7 ms with eight hundred. Written every so often instead, both are about
    /// 5.4 ms and the cost stops tracking the size of the store.
    ///
    /// Counted rather than timed. A duration would assert the right thing on an idle machine and
    /// something else entirely on a busy one; the number of manifest writes is the shape itself.
    #[test]
    fn installing_slabs_does_not_write_a_manifest_each_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalBlockStore::new(dir.path());
        let slabs = 512u64;
        for id in 0..slabs {
            store.install_slab(id, b"slab-contents").unwrap();
        }
        let writes = store.stats().band_manifest_writes;
        assert!(
            writes < slabs / 8,
            "installing {slabs} slabs wrote the manifest {writes} times; one per install is what \
             made each install cost the whole store"
        );
        assert!(
            writes > 0,
            "the manifest should still reach disk periodically, or a crash rebuilds everything"
        );
        // And it is still correct: a reopen sees every slab, whether or not the last write landed.
        drop(store);
        let reopened = LocalBlockStore::new(dir.path());
        assert_eq!(
            reopened.slab_ids().unwrap().len(),
            slabs as usize,
            "every installed slab must still be there after a reopen"
        );
    }
}
