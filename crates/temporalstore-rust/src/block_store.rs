// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::storage_config::effective_block_slab_target_bytes;

mod paths;
mod slab_manifest;
mod slab_reports;
mod append;
mod read;
mod gc;
mod slab_ids;
mod slab_backend;
use slab_backend::{LocalSlabBackend, SlabBackend};
mod record;

/// Bytes a block record spends on its header, before the block's own bytes.
///
/// Re-exported rather than redefined. This was a second constant whose entire body was the
/// first one, written to widen a `pub(super)` definition so a log that CARRIES a block could
/// work out the length its address will hold. Now that both spell the record the same way, two
/// constants of one name in two modules is a thing to trip over rather than a bridge.
pub(crate) use record::BLOCK_RECORD_HEADER_LEN;

pub(crate) use record::block_index_checksums_enabled;

use paths::{
    delayed_destroy_dir, delayed_destroy_path, slab_manifest_path, file_created_unix_ms,
    file_modified_unix_ms, legacy_zone_manifest_path, now_unix_ms, slab_path, sync_dir,
    sync_parent_dir, system_time_unix_ms,
};
use record::{
    decode_block_record, default_block_record_compression_enabled,
    default_block_record_compression_level, default_block_record_compression_min_bytes,
    encode_block_record, inspect_slab, logical_range_from_slab,
    sha256_hex, summarize_slab,
    BlockRecordCompression,
};
use self::slab_manifest::*;
pub(crate) use slab_ids::*;
#[cfg(test)]
use record::{BLOCK_RECORD_COMPRESSION_NONE, BLOCK_RECORD_COMPRESSION_ZSTD};

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "block checksum mismatch for slab {block_slab_id} offset {offset} length {length}: expected {expected}, got {actual}"
    )]
    ChecksumMismatch {
        block_slab_id: u64,
        offset: u64,
        length: u64,
        expected: String,
        actual: String,
    },
    #[error("corrupt block envelope for slab {block_slab_id} offset {offset}: {reason}")]
    CorruptBlockEnvelope {
        block_slab_id: u64,
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
/// A sentinel would be cheaper and is NOT available here: `0` is a legitimate `stored_slab_id` and a
/// legitimate `routing_slot` -- there is a test asserting `stored_slab_id == Some(0)` -- so "zero means
/// absent" would silently erase real values. A byte of presence bits costs almost nothing and
/// cannot make that mistake.
///
/// This is the shape the design being followed uses: one byte carrying `dirty`, `page_in_log` and
/// its reserved bits, rather than an optional wrapped around each.
const ADDRESS_HAS_BLOCK_ID: u8 = 1 << 0;
const ADDRESS_HAS_OBJECT_ID: u8 = 1 << 1;
const ADDRESS_HAS_ROUTING_BUCKET: u8 = 1 << 2;
const ADDRESS_HAS_GENERATION: u8 = 1 << 3;

/// The address as it travels on the wire and on disk.
///
/// `BlockAddress` converts through this both ways, which keeps the in-memory packing separate
/// from the on-disk shape.
///
/// The names are SHORT. An address is written once per index item, and an index-log record was
/// measured at 65.3% field names -- these were the largest remaining group of them, and
/// `page_segment_id` alone cost more than the offset it labels. Every short name carries every
/// spelling this field has ever had as an alias, so anything already written still loads:
/// `block_slab_id` and its `page_segment_id` rename, `routing_bucket` and its `routing_slot`
/// rename, `stored_slab_id` with its older `extent_id`/`zone_id`, and `sha256` with its `checksum`.
///
/// This does change the shape a NEW record is written in, so a binary older than this cannot
/// read one -- the same trade the WAL and the index item made before it. Old to new is safe;
/// new to old is not.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct BlockAddressWire {
    #[serde(rename = "ps", alias = "page_segment_id", alias = "page_slab_id")]
    block_slab_id: u64,
    #[serde(rename = "o", alias = "offset")]
    offset: u64,
    #[serde(rename = "l", alias = "length")]
    length: u64,
    #[serde(
        rename = "pi",
        alias = "page_id",
        default
    )]
    block_id: Option<u64>,
    #[serde(
        rename = "oi",
        alias = "object_id",
        default
    )]
    object_id: Option<u64>,
    #[serde(
        rename = "rs",
        alias = "routing_slot",
        alias = "routing_bucket",
        default
    )]
    routing_bucket: Option<u32>,
    #[serde(
        rename = "g",
        alias = "generation",
        default
    )]
    generation: Option<u64>,
    #[serde(
        rename = "h",
        alias = "sha256",
        alias = "checksum",
        default,
        with = "hex_digest"
    )]
    sha256: Option<[u8; 32]>,
}

impl From<BlockAddressWire> for BlockAddress {
    fn from(wire: BlockAddressWire) -> Self {
        BlockAddress::from_parts(
            wire.block_slab_id,
            wire.offset,
            wire.length,
            wire.block_id,
            wire.object_id,
            wire.routing_bucket,
            wire.generation,
        )
    }
}

impl From<BlockAddress> for BlockAddressWire {
    fn from(address: BlockAddress) -> Self {
        Self {
            block_slab_id: address.block_slab_id,
            offset: address.offset,
            length: address.length,
            block_id: address.block_id(),
            object_id: address.object_id(),
            routing_bucket: address.routing_bucket(),
            generation: address.generation(),
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
    pub block_slab_id: u64,
    pub offset: u64,
    pub length: u64,
    block_id: u64,
    object_id: u64,
    generation: u64,
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
        block_slab_id: u64,
        offset: u64,
        length: u64,
        block_id: Option<u64>,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        generation: Option<u64>,
    ) -> Self {
        let mut present = 0u8;
        if block_id.is_some() {
            present |= ADDRESS_HAS_BLOCK_ID;
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
        Self {
            block_slab_id,
            offset,
            length,
            block_id: block_id.unwrap_or_default(),
            object_id: object_id.unwrap_or_default(),
            generation: generation.unwrap_or_default(),
            routing_bucket: routing_bucket.unwrap_or_default(),
            present,
        }
    }

    pub fn block_id(&self) -> Option<u64> {
        (self.present & ADDRESS_HAS_BLOCK_ID != 0).then_some(self.block_id)
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

    /// The slab this address is in, which is the slab it is in.
    ///
    /// Derived rather than stored. It was a function of the slab AND two configuration sizes,
    /// which is what made it unsafe to derive: a reader whose configuration had moved would
    /// reconstruct a different slab than the writer meant. With one size there is nothing to
    /// disagree about, so the slab is a fact about the address instead of a field beside it.
    ///
    /// Still an `Option` because every caller reads it as one, and it now answers `Some` for
    /// every address -- a slab is always known.
    pub fn slab_id(&self) -> Option<u64> {
        Some(self.block_slab_id)
    }

    pub fn set_block_id(&mut self, value: Option<u64>) {
        self.block_id = value.unwrap_or_default();
        self.set_present(ADDRESS_HAS_BLOCK_ID, value.is_some());
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
        u32::try_from(self.block_slab_id).ok()
    }

    pub fn compact_slab_offset(&self) -> Option<u32> {
        u32::try_from(self.offset).ok()
    }

    pub fn compact_slab_address(&self) -> Option<u64> {
        compact_slab_address_from_parts(self.block_slab_id, self.offset)
    }

    pub fn from_compact_slab_address(compact_slab_address: u64, length: u64) -> Self {
        Self::from_parts(
            compact_extract_slab_id(compact_slab_address) as u64,
            compact_extract_slab_offset(compact_slab_address) as u64,
            length,
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
    /// Times the whole slab manifest was written out.
    ///
    /// Writing it costs the whole manifest, so one write per slab install made installing n slabs
    /// cost n manifests -- and each install cost time proportional to how many slabs already
    /// existed. Counted rather than timed, because a count says the same thing on a busy machine.
    #[serde(default)]
    #[serde(rename = "band_manifest_writes")]
    pub slab_manifest_writes: u64,
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
    fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreOptions {
    #[serde(default = "default_block_record_compression_enabled")]
    pub compression_enabled: bool,
    #[serde(default = "default_block_record_compression_min_bytes")]
    pub compression_min_bytes: usize,
    #[serde(default = "default_block_record_compression_level")]
    pub compression_level: i32,
}

/// The payload is BORROWED. A caller already holds the encoded page -- it keeps it for the
/// cache put, or it holds the buffer it published from -- so owning it here forced every
/// caller to hand over a clone of a page that is several times its own payload.
/// A block to append: its bytes, the object it belongs to, that object's routing bucket, and
/// which block of that object it is.
///
/// The last of those used to be the block store's to invent, from a counter running across the
/// whole store. A block id is now an index INSIDE an object -- block 0, block 1 -- the way the
/// comparison design numbers a block within a slot rather than within a partition. Two objects
/// both having a block 0 is expected: a block is identified by its object and its index, and
/// every key that names one already carries the object key.
///
/// The store-wide counter is gone with it, and so is the walk that recovered the counter by
/// reading every block header in every slab to work out one integer.
pub type BlockAppendRecord<'a> = (&'a [u8], Option<u64>, Option<u32>, u32);

impl Default for BlockStoreOptions {
    fn default() -> Self {
        Self {
            compression_enabled: default_block_record_compression_enabled(),
            compression_min_bytes: default_block_record_compression_min_bytes(),
            compression_level: default_block_record_compression_level(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcReport {
    #[serde(alias = "retain_from_page_segment_id")]
    #[serde(rename = "retain_from_page_slab_id")]
    pub retain_from_block_slab_id: u64,
    #[serde(alias = "removed_page_segment_ids")]
    #[serde(rename = "removed_page_slab_ids")]
    pub removed_block_slab_ids: Vec<u64>,
    #[serde(alias = "retained_page_segment_ids")]
    #[serde(rename = "retained_page_slab_ids")]
    pub retained_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub removed_physical_bytes: u64,
    #[serde(default)]
    pub retained_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "delayed_destroy_page_segment_ids")]
    #[serde(rename = "delayed_destroy_page_slab_ids")]
    pub delayed_destroy_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub delayed_destroy_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_live_page_segment_ids")]
    #[serde(rename = "retained_live_page_slab_ids")]
    pub retained_live_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "retained_current_page_segment_ids")]
    #[serde(rename = "retained_current_page_slab_ids")]
    pub retained_current_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_current_physical_bytes: u64,
    /// Slabs this round declined to reclaim because the PUBLISHED BYTE TALLY still credits them
    /// with live block bytes, even though the caller's live slab-id set did not name them.
    ///
    /// Not the same thing as `retained_live_block_slab_ids`, which is the slabs the caller's id
    /// set kept. These are the ones the two sources DISAGREE about, and reaching this list means
    /// a bug upstream: an id set and a tally derived from the same index should never contradict
    /// each other about the same slab. Non-empty is an alarm, and the point of the check is that
    /// the disagreement costs a round of reclaim instead of the bytes.
    #[serde(default)]
    #[serde(rename = "retained_live_bytes_page_slab_ids")]
    pub retained_live_bytes_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_live_bytes_physical_bytes: u64,
}

/// Live pages on ONE slab, as the INDEX counts them.
///
/// The block store cannot derive this. It sees appends, and it sees whole slabs arrive and leave;
/// an index entry that stopped pointing at an offset reaches it nowhere. So this arrives from the
/// outside, through [`BlockStore::publish_live_block_bytes`], and the store only reads it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabLive {
    #[serde(rename = "live_page_refs")]
    pub live_block_refs: u64,
    /// Sum of the lengths of the live pages on the slab. LOGICAL bytes -- the same quantity the
    /// slab descriptor's `logical_bytes` totals over every page ever appended to it, which is why
    /// that, and not the file size, is the denominator of the fraction below.
    pub live_bytes: u64,
}

/// How much of one slab is still live, for every slab the store holds.
///
/// The read side of the published tally, and the answer to "is `utility_basis_points` uniformly
/// zero". It is, for GC CANDIDATES, and necessarily so -- a candidate is a slab no live page
/// points at. Across the whole store it is not, and this is where that shows.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabLiveFraction {
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    /// Total logical bytes ever appended to this slab. Only ever grows, which is correct FOR A
    /// DENOMINATOR and was the bug when the same field was read as a live figure.
    pub logical_bytes: u64,
    #[serde(rename = "live_page_refs")]
    pub live_block_refs: u64,
    pub live_bytes: u64,
    /// `live_bytes * 10_000 / logical_bytes`, or 0 when the slab has no logical bytes.
    pub live_basis_points: u64,
    /// `10_000 - live_basis_points`. What the page-GC garbage floor compares against.
    pub garbage_basis_points: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcUtilityCandidate {
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
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
    /// Reclaim only slabs whose garbage ratio (10_000 - utility_basis_points) is at
    /// least this many basis points. `None`/0 reclaims every eligible slab (today's
    /// behavior). The garbage-ratio gate (reclaim the most-garbage zones),
    /// expressed against Rust slabs.
    #[serde(default)]
    #[serde(rename = "min_band_garbage_basis_points")]
    pub min_slab_garbage_basis_points: Option<u64>,
}

impl BlockStoreGcPolicy {
    pub fn max_slabs(max_destroy_slabs: usize) -> Self {
        Self {
            max_destroy_slabs,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms: None,
            min_slab_garbage_basis_points: None,
        }
    }

    /// Reclaim eligible slabs whose garbage ratio is at least
    /// `min_slab_garbage_basis_points` (on the wire, `min_band_garbage_basis_points`),
    /// highest-garbage first, optionally bounded by a
    /// minimum slab age. Mirrors selecting the maximum-garbage-rate zone under GC.
    /// A garbage floor that now MEASURES THE RIGHT QUANTITY, and still excludes nothing in a
    /// running store. Both halves measured, neither assumed.
    ///
    /// WHAT CHANGED. `used_bytes` used to sum the file sizes of the slabs grouped under a
    /// candidate's stored id that are NOT collectable. A stored id names exactly one slab, and the
    /// candidate filter is the exact negation of that test, so a candidate could never contribute
    /// to its own used bytes: every candidate reported 0, 10,000 bp of garbage, and the floor
    /// cleared everything at every setting. It was not merely degenerate, it was answering "is
    /// this whole slab collectable" (always yes, by construction) instead of "how much of this
    /// slab is still live".
    ///
    /// It now sums the LIVE PAGE BYTES on the slab itself, taken from the per-slab tally the index
    /// maintains on its own mutation path and publishes through
    /// `BlockStore::publish_live_block_bytes`. The denominator moved with it, from the file
    /// size to the slab descriptor's `logical_bytes` -- the total ever appended there -- because a
    /// live page is counted at its logical length. `a_published_live_tally_makes_used_bytes_mean_
    /// live_page_bytes` shows the floor excluding a 90%-live slab, which is the first time this
    /// knob has excluded anything.
    ///
    /// WHAT DID NOT CHANGE. In a running store the floor still excludes none of the COLLECTOR's
    /// candidates, and `can_the_block_gc_garbage_floor_bind` still asserts that. The reason has
    /// moved, and the new one is the useful one: a collector candidate is a slab that no live page
    /// points at -- `is_live` is checked before candidacy and again before removal -- so its
    /// maintained live bytes are genuinely zero. The floor is now measured against a real figure
    /// that is really zero, rather than against an artefact of two filters contradicting each
    /// other.
    ///
    /// So the remaining obstacle is the CANDIDATE PREDICATE, not the accounting: nothing offers
    /// this floor a partially-live slab, because a slab with one live page is not a candidate at
    /// all. That is the same all-or-nothing rule that lets one live page pin a whole slab, and
    /// widening it means relocating the survivors first -- a compaction decision with its own
    /// measurement, not a change to this constructor.
    ///
    /// (An earlier reading of this blamed the stored id for not grouping several slabs. It does
    /// not group, and it was never going to: the only grouping key ever used was the slab's own
    /// address, and `a_stored_slab_id_that_disagrees_with_its_descriptor_is_normalised_on_load`
    /// shows even a manifest cannot introduce one. Grouping was not the missing piece.)
    pub fn with_slab_garbage_floor(
        min_slab_garbage_basis_points: u64,
        min_age_ms: Option<u64>,
    ) -> Self {
        Self {
            max_destroy_slabs: 0,
            max_destroy_physical_bytes: 0,
            max_utility_score: None,
            min_age_ms,
            min_slab_garbage_basis_points: Some(min_slab_garbage_basis_points),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreGcPolicyPlan {
    #[serde(alias = "retain_from_page_segment_id")]
    #[serde(rename = "retain_from_page_slab_id")]
    pub retain_from_block_slab_id: u64,
    #[serde(alias = "selected_page_segment_ids")]
    #[serde(rename = "selected_page_slab_ids")]
    pub selected_block_slab_ids: Vec<u64>,
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
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u64>,
}

/// How long a reclaimed slab waits in quarantine before it may be destroyed.
///
/// Quarantine existed but nothing ever aged: the purge deleted every file it found, so a slab
/// moved there a second earlier went with the rest, and the "delay" lasted only until something
/// called purge. The hazard is written down a few files away, in the compaction commit path -- a
/// reclaim that quarantines and purges a slab, followed by a reload of an index that still names
/// it, dangles at a deleted slab. An age is what makes a delayed destroy delay anything.
///
/// One hour rather than the twenty-four the comparison design waits. The window this has to cover
/// is a reader holding a stale address, a replica catching up, or an index persist that failed and
/// is retried next cycle -- minutes, not a day -- and quarantine holds real bytes on a box with a
/// measured capacity wall. Long enough for all three, short enough not to hoard a day of garbage.
pub(crate) const DELAYED_DESTROY_MIN_AGE_MS: u64 = 60 * 60 * 1000;

/// How many quarantined slabs one purge round may act on before it stops and leaves the rest for
/// the next round.
///
/// THE PURGE HOLDS THE STORE-WIDE LOCK FOR THE WHOLE ROUND, and before this the round was
/// unbounded in the amount of work it did: it read the trash directory and acted on every slab it
/// found. Measured on this box with the re-checked, list-narrowed purge, and linear in the
/// quarantine size:
///
///   quarantined   purge duration   destroyed / restored / held
///         8,000        5,194.7 ms      6,400 /   800 /   800
///        80,000       58,661.7 ms     64,000 / 8,000 / 8,000
///
/// Nothing else can touch the store for that whole time. A thousand slabs is the largest round
/// that keeps the hold under a second at the per-slab cost measured here, which is the number
/// that matters: the bound is on the LOCK HOLD, not on the reclaim rate, and a caller that wants
/// the quarantine drained faster runs more rounds rather than one longer one.
///
/// THE BUDGET IS SPENT ON WORK DONE, NOT ON ENTRIES LOOKED AT -- see
/// [`BlockStore::purge_delayed_destroy_slabs_capped`], where the difference is what makes
/// the cap advance instead of stalling.
pub(crate) const DELAYED_DESTROY_MAX_SLABS_PER_ROUND: usize = 1_000;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStorePurgeDelayedDestroyReport {
    #[serde(alias = "purged_page_segment_ids")]
    #[serde(rename = "purged_page_slab_ids")]
    pub purged_block_slab_ids: Vec<u64>,
    pub purged_physical_bytes: u64,
    /// Slabs left in quarantine because they had not been there long enough yet.
    ///
    /// Non-zero means the purge ran and DECLINED: the space comes back on a later cycle. Without
    /// it a caller cannot tell "nothing to reclaim" from "not yet".
    #[serde(default)]
    pub retained_too_young_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub retained_too_young_physical_bytes: u64,
    /// Slabs taken BACK OUT of quarantine because the last-chance re-check found them live.
    ///
    /// Non-empty is an alarm, not routine: the collector decided this slab was unreferenced and
    /// the re-check disagreed. The slab is returned to the store and the bytes are not freed.
    #[serde(default)]
    pub restored_block_slab_ids: Vec<u64>,
    #[serde(default)]
    pub restored_physical_bytes: u64,
    /// Slabs the re-check found live but could NOT return, because a file already occupies the
    /// id. They stay in quarantine: not destroyed, not restored.
    #[serde(default)]
    pub restore_blocked_block_slab_ids: Vec<u64>,
    /// Slabs this round destroyed or restored -- what the round's budget was spent on.
    ///
    /// NOT the same as `purged + restored` being nonzero, and not derivable from the lists a
    /// caller can already see once a round can stop early: this is the number compared against
    /// `max_slabs_per_round`, and a caller checking that the cap advances needs the two side by
    /// side.
    #[serde(default)]
    pub processed_block_slabs: usize,
    /// The round stopped on its budget with quarantined slabs still unexamined.
    ///
    /// `true` is NOT an error and NOT a decline: it says the work continues next round. A caller
    /// draining a quarantine loops while this holds. The distinction the `retained_too_young`
    /// list already draws is the one that matters here too -- a slab the round never reached is
    /// not being held back for any reason of its own, it simply was not this round's business.
    ///
    /// A ROUND THAT SPENDS ITS BUDGET EXACTLY AS THE WORK RUNS OUT STILL REPORTS `true`, and the
    /// next round then finds nothing and reports `false`. That costs one extra, empty round, and
    /// it is deliberate: the only way for the round to know the directory holds nothing more is
    /// to walk the rest of it, and not walking the rest of it is the entire point of the cap.
    /// Over-reporting here costs a round that does nothing; under-reporting would stop a caller's
    /// drain with slabs still in quarantine.
    #[serde(default)]
    pub budget_exhausted: bool,
    /// The budget this round ran under. 0 means uncapped.
    ///
    /// Reported so a round that did less than a caller expected can be told apart from a round
    /// that ran under a smaller cap than the caller thought.
    #[serde(default)]
    pub max_slabs_per_round: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockStoreSlabState {
    Active,
    Sealed,
    DelayedDestroy,
    Purged,
}

/// Metadata for a SEALED slab whose bytes live in shared storage and are restored lazily.
/// Passed to [`BlockStore::install_lazy_checkpoint_slabs`] so a lazy-restore installs
/// complete slab descriptors before the first on-demand slab fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LazyCheckpointSlab {
    pub block_slab_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub first_block_id: Option<u64>,
    pub last_block_id: Option<u64>,
    pub created_unix_ms: Option<u64>,
    pub updated_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabDescriptor {
    /// The same number as [`Self::block_slab_id`], always.
    ///
    /// The manifest is the ONE way a second spelling of a slab id enters the process, so this is
    /// the number that arrives from disk rather than one this code computed. It used to divide a
    /// slab's byte range by a separate size, so one of these could have grouped several slabs;
    /// nothing ever configured the two sizes differently and the grouping was never exercised,
    /// which is why the map is keyed by slab id with one descriptor per slab.
    ///
    /// It stays because it SERIALIZES -- under its original key, which the wire names below
    /// record as `zone_id` -> `extent_id` -> `band_id` -- and the compat corpora carry it, so
    /// dropping it is a wire break rather than a cleanup. Read `block_slab_id` in new code; the
    /// two cannot diverge, `reconcile_slab_manifest_with_disk` normalises this one from the map
    /// key on every open, and `a_slab_descriptor_carries_the_same_number_twice` fails if they
    /// ever do.
    #[serde(rename = "band_id", alias = "extent_id", alias = "zone_id")]
    pub stored_slab_id: u64,
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
    pub state: BlockStoreSlabState,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_unix_ms: Option<u64>,
    #[serde(rename = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    /// The file mtime this descriptor was last verified against.
    ///
    /// Every open re-reads and re-hashes every page record in every slab; on a 1.4 GB store that is
    /// ~30 s of CPU, and it is the same work every time for slabs nobody has touched. Recording the
    /// identity the descriptor was verified against makes "has this file changed" answerable from
    /// metadata instead of by decoding the slab again.
    ///
    /// None on a descriptor written before this existed, which simply verifies once and fills it in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_source_mtime_unix_ms: Option<u64>,
    #[serde(default)]
    pub has_corruption: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_error: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabSummary {
    #[serde(alias = "active_zones")]
    #[serde(rename = "active_bands")]
    pub active_slabs: u64,
    #[serde(alias = "sealed_zones")]
    #[serde(rename = "sealed_bands")]
    pub sealed_slabs: u64,
    #[serde(alias = "delayed_destroy_zones")]
    #[serde(rename = "delayed_destroy_bands")]
    pub delayed_destroy_slabs: u64,
    #[serde(alias = "purged_zones")]
    #[serde(rename = "purged_bands")]
    pub purged_slabs: u64,
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
    #[serde(rename = "oldest_known_band_unix_ms")]
    pub oldest_known_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_known_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "oldest_known_band_age_ms")]
    pub oldest_known_slab_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "oldest_live_band_unix_ms")]
    pub oldest_live_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_live_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "oldest_live_band_age_ms")]
    pub oldest_live_slab_age_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_unix_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "oldest_reclaimable_band_unix_ms")]
    pub oldest_reclaimable_slab_unix_ms: Option<u64>,
    #[serde(
        default,
        alias = "oldest_reclaimable_zone_age_ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[serde(rename = "oldest_reclaimable_band_age_ms")]
    pub oldest_reclaimable_slab_age_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreSlabUsage {
    #[serde(rename = "band_id", alias = "extent_id", alias = "zone_id")]
    pub stored_slab_id: u64,
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
    #[serde(rename = "storage_zone_id", default)]
    pub storage_slab_id: u64,
    #[serde(default)]
    #[serde(alias = "stream_segment_id")]
    pub stream_slab_id: u64,
    pub state: BlockStoreSlabState,
    #[serde(default)]
    pub used_bytes: u64,
    #[serde(default)]
    pub live_bytes: u64,
    #[serde(default)]
    pub reclaimable_bytes: u64,
    #[serde(default)]
    pub purged_bytes: u64,
    #[serde(rename = "page_store_used_bytes")]
    pub block_store_used_bytes: u64,
    #[serde(rename = "live_page_store_used_bytes")]
    pub live_block_store_used_bytes: u64,
    #[serde(rename = "reclaimable_page_store_used_bytes")]
    pub reclaimable_block_store_used_bytes: u64,
    #[serde(rename = "purged_page_store_used_bytes")]
    pub purged_block_store_used_bytes: u64,
    #[serde(rename = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamBackedSlabRuntimeReport {
    pub runtime_ready: bool,
    #[serde(default)]
    #[serde(rename = "band_lifecycle_states")]
    pub slab_lifecycle_states: Vec<String>,
    #[serde(rename = "band_count", alias = "extent_count", alias = "zone_count")]
    pub slab_count: u64,
    #[serde(alias = "active_zones")]
    #[serde(rename = "active_bands")]
    pub active_slabs: u64,
    #[serde(alias = "sealed_zones")]
    #[serde(rename = "sealed_bands")]
    pub sealed_slabs: u64,
    #[serde(alias = "delayed_destroy_zones")]
    #[serde(rename = "delayed_destroy_bands")]
    pub delayed_destroy_slabs: u64,
    #[serde(alias = "purged_zones")]
    #[serde(rename = "purged_bands")]
    pub purged_slabs: u64,
    #[serde(rename = "zone_stats_ready", default)]
    pub slab_stats_ready: bool,
    #[serde(rename = "zone_usage", default)]
    pub slab_usage: Vec<BlockStoreSlabUsage>,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    #[serde(default)]
    pub stream_record_count: u64,
    #[serde(rename = "first_page_id", default)]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_page_id", default)]
    pub last_block_id: Option<u64>,
    #[serde(rename = "page_id_continuity_ready", default)]
    pub block_id_continuity_ready: bool,
    #[serde(default)]
    pub logical_stream_bytes_read: u64,
    #[serde(default)]
    #[serde(rename = "band_state_transition_count")]
    pub slab_state_transition_count: u64,
    pub logical_stream_read_ready: bool,
    pub append_roll_ready: bool,
    #[serde(alias = "extent_manifest_ready", alias = "zone_manifest_ready")]
    #[serde(rename = "band_manifest_ready")]
    pub slab_manifest_ready: bool,
    #[serde(default)]
    #[serde(rename = "band_manifest_rebuild_ready")]
    pub slab_manifest_rebuild_ready: bool,
    #[serde(default)]
    #[serde(rename = "band_manifest_reconciled_on_open")]
    pub slab_manifest_reconciled_on_open: bool,
    #[serde(default)]
    #[serde(rename = "band_manifest_disk_consistent")]
    pub slab_manifest_disk_consistent: bool,
    #[serde(default)]
    #[serde(rename = "manifest_missing_stream_bands")]
    pub manifest_missing_stream_slabs: u64,
    #[serde(default)]
    #[serde(rename = "manifest_extra_stream_bands")]
    pub manifest_extra_stream_slabs: u64,
    #[serde(default)]
    #[serde(rename = "corrupt_band_count")]
    pub corrupt_slab_count: u64,
    #[serde(default)]
    #[serde(rename = "partial_band_count")]
    pub partial_slab_count: u64,
    #[serde(default)]
    pub readable_prefix_physical_bytes: u64,
    #[serde(default)]
    #[serde(rename = "partial_band_recovery_ready")]
    pub partial_slab_recovery_ready: bool,
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
    pub block_slab_id: u64,
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
    #[serde(rename = "first_page_id", default, skip_serializing_if = "Option::is_none")]
    pub first_block_id: Option<u64>,
    #[serde(rename = "last_page_id", default, skip_serializing_if = "Option::is_none")]
    pub last_block_id: Option<u64>,
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
    #[serde(rename = "page_slab_id")]
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
struct BlockStoreSlabManifest {
    version: u32,
    /// ON-DISK KEY, NOT THE RUST NAME. `slab_manifest.json` is a format an already-deployed
    /// binary reads, and the descriptor's `band_id` carries no `#[serde(default)]`, so a manifest
    /// written under new keys is unreadable to it rather than merely unfamiliar.
    /// `a_folded_manifest_still_writes_the_keys_on_disk` pins both keys.
    #[serde(rename = "bands", alias = "extents", alias = "zones")]
    slabs: Vec<BlockStoreSlabDescriptor>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockStoreRollReport {
    #[serde(alias = "previous_page_segment_id")]
    #[serde(rename = "previous_page_slab_id")]
    pub previous_block_slab_id: u64,
    #[serde(alias = "new_page_segment_id")]
    #[serde(rename = "new_page_slab_id")]
    pub new_block_slab_id: u64,
}

#[derive(Debug, Clone)]
pub struct BlockStore {
    inner: Arc<Mutex<BlockStoreInner>>,
}

impl BlockStore {
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
    block_slab_id: u64,
    write_offset: u64,
    next_block_id: u64,
    options: BlockStoreOptions,
    slabs: BTreeMap<u64, BlockStoreSlabDescriptor>,
    /// Slab installs since the manifest was last written out. Writing it costs the whole manifest,
    /// so it is written every so often rather than every install; the load rebuilds from the slabs
    /// when what it reads does not match them.
    slabs_unwritten: usize,
    slab_manifest_reconciled_on_open: bool,
    /// Sealed slabs this open kept from the manifest WITHOUT re-reading them. Not part of any
    /// report wire shape; it exists so a guard aimed at that route can prove the route ran.
    slabs_skipped_reinspection_on_open: usize,
    /// Per-slab live page tallies, PUBLISHED by the index that maintains them.
    ///
    /// `None` until an index has published once, and the difference matters: an empty map means
    /// "an index looked and found no live pages anywhere", while `None` means "nobody has told
    /// this store anything" -- and only the second may fall back to the older neighbour-sum
    /// figure.
    ///
    /// A snapshot, so it can be stale -- and staleness here is safe in ONE DIRECTION ONLY, which
    /// is why it is allowed. `used_bytes` feeds the garbage floor, and the floor only ever KEEPS a
    /// slab; it never grants permission to delete one. Deletion is gated by the live slab id set,
    /// computed fresh at the call. So a stale tally that overstates live bytes costs a round of
    /// collection, and one that understates them cannot reach anything the live-set test would
    /// have retained.
    live_block_bytes: Option<BTreeMap<u64, BlockStoreSlabLive>>,
    stats: BlockStoreStats,
    // Optional shared-storage read-through (on-demand lazy recovery): set by
    // attach_shared_slab_source() after a metadata-only restore. When present, a
    // read that misses a slab locally fetches it from here, caches it, then serves.
    shared_slab_source: Option<Arc<dyn SharedSlabSource>>,
    // Set only by Default: the store owns its minted scratch directory, and the last
    // clone's drop removes it. Never set for a caller-supplied root.
    scratch: Option<Arc<crate::scratch::ScratchDirGuard>>,
}

/// Slab installs allowed to go by before the slab manifest is written out.
///
/// Writing it costs the whole manifest, so writing it per install makes installing n slabs cost n
/// manifests. Deferring trades that for a rebuild after a crash, which the load does for itself
/// when what it reads does not match the slabs on disk.
const SLABS_UNWRITTEN_BEFORE_PERSIST: usize = 64;

impl BlockStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_options(root, BlockStoreOptions::default())
    }

    pub fn with_options(root: impl Into<PathBuf>, options: BlockStoreOptions) -> Self {
        let root = root.into();
        let _ = fs::create_dir_all(&root);
        let block_slab_id = latest_slab_id_at(&root).unwrap_or_default();
        let mut write_offset = slab_path(&root, block_slab_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let manifest_exists =
            slab_manifest_path(&root).exists() || legacy_zone_manifest_path(&root).exists();
        let (mut slabs, mut manifest_rebuilt) = if manifest_exists {
            match load_slab_manifest_at(&root) {
                Ok(slabs) => (slabs, false),
                Err(_) => (rebuild_slab_manifest_at(&root).unwrap_or_default(), true),
            }
        } else {
            (rebuild_slab_manifest_at(&root).unwrap_or_default(), true)
        };
        // Loaded BEFORE the page-id scan on purpose: the manifest already records `last_page_id`
        // per slab, and reading it turns a walk over every page record header -- 90% of a
        // steady-state open's reads -- into a few MB. Any slab the manifest cannot prove unchanged
        // is still walked, and the active slab always is.
        // Nothing allocates from a store-wide block counter any more: a block id is an index
        // inside its object, and the object is what knows how many blocks it has. Recovering a
        // counter here used to mean reading every block header in every slab to work out one
        // integer -- on a live-store copy, the bulk of a steady-state open.
        let next_block_id = 0;
        let reconciled = reconcile_slab_manifest_with_disk(&root, &mut slabs).unwrap_or_default();
        let slab_manifest_reconciled_on_open = reconciled.changed;
        let slabs_skipped_reinspection_on_open = reconciled.slabs_skipped_reinspection;
        manifest_rebuilt |= slab_manifest_reconciled_on_open;
        ensure_slab_descriptor(
            &mut slabs,
            &root,
            block_slab_id,
            BlockStoreSlabState::Active,
        );
        // Fence a torn tail on the ACTIVE slab. After a crash mid-append the raw file length
        // includes uncommitted/partial bytes past the last intact record; reconcile computed
        // the intact `readable_prefix`. Resuming appends at raw EOF would embed the torn record
        // permanently mid-slab and, via the early-halting page-id scan, regress next_page_id ->
        // page-id/generation reuse -> stale reads. Mirror the resume-at-committed-length:
        // physically truncate the active slab to its readable prefix and resume there.
        let active_readable_prefix = slabs
            .get(&block_slab_id)
            .map(|slab| slab.readable_prefix_physical_bytes);
        if let Some(readable_prefix) = active_readable_prefix {
            if readable_prefix < write_offset {
                if let Ok(file) = OpenOptions::new()
                    .write(true)
                    .open(slab_path(&root, block_slab_id))
                {
                    if file.set_len(readable_prefix).is_ok() {
                        crate::durability_metrics::record_barrier("block_store_open");
                        let _ = file.sync_all();
                        if let Ok(dir) = File::open(&root) {
                            let _ = dir.sync_all();
                        }
                        write_offset = readable_prefix;
                        if let Some(slab) = slabs.get_mut(&block_slab_id) {
                            slab.physical_bytes = readable_prefix;
                            slab.has_corruption = false;
                            slab.first_error_offset = None;
                        }
                        manifest_rebuilt = true;
                    }
                }
            }
        }
        if manifest_rebuilt {
            let _ = persist_slab_manifest(&root, &slabs);
        }
        Self {
            inner: Arc::new(Mutex::new(BlockStoreInner {
                root,
                relaxed_dirty: false,
                block_slab_id,
                write_offset,
                next_block_id,
                options,
                slabs,
                slabs_unwritten: 0,
                slab_manifest_reconciled_on_open,
                slabs_skipped_reinspection_on_open,
                live_block_bytes: None,
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
    pub fn next_block_id(&self) -> u64 {
        self.inner
            .lock()
            .expect("block store lock poisoned")
            .next_block_id
    }

    /// Reserve the slab-id (and page-id) range consumed by a lazily-restored checkpoint
    /// so replayed/new appends land in a FRESH slab beyond it, never overwriting a slab
    /// that is still served on-demand from shared storage. Matches the recovery behaviour
    /// model where old pages stay addressable in shared storage while new writes roll
    /// forward. Called right after attaching the shared read-through on a fresh owner.
    pub fn reserve_lazy_checkpoint_range(
        &self,
        through_slab_id: u64,
        next_block_id_floor: u64,
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        fs::create_dir_all(&inner.root)?;
        let existing_max = slab_ids_at(&inner.root)?.into_iter().max();
        let new_slab_id = through_slab_id
            .max(inner.block_slab_id)
            .max(existing_max.unwrap_or_default())
            .saturating_add(1);
        let path = slab_path(&inner.root, new_slab_id);
        let file = File::create(&path)?;
        crate::durability_metrics::record_barrier("block_store_checkpoint_reserve");
        file.sync_all()?;
        sync_parent_dir(&path)?;
        inner.block_slab_id = new_slab_id;
        inner.write_offset = 0;
        inner.next_block_id = inner.next_block_id.max(next_block_id_floor);
        let now = now_unix_ms();
        // Any previously-active local slab is now sealed; the reserved slab is active.
        for slab in inner.slabs.values_mut() {
            if slab.state == BlockStoreSlabState::Active {
                slab.state = BlockStoreSlabState::Sealed;
                slab.updated_unix_ms = Some(now);
            }
        }
        inner.slabs.insert(
            new_slab_id,
            BlockStoreSlabDescriptor {
                stored_slab_id: new_slab_id,
                block_slab_id: new_slab_id,
                state: BlockStoreSlabState::Active,
                physical_bytes: 0,
                logical_bytes: 0,
                created_unix_ms: Some(now),
                updated_unix_ms: Some(now),
                first_block_id: None,
                last_block_id: None,
                readable_prefix_physical_bytes: 0,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            },
        );
        persist_slab_manifest(&inner.root, &inner.slabs)?;
        Ok(())
    }

    /// Install SEALED slab descriptors for the slabs a lazy checkpoint restore backs from shared
    /// storage. The slab bytes are NOT local yet (they are fetched on demand through the attached
    /// shared read-through), but a GC/compaction cycle running between restore and the first fetch
    /// must still see these sealed slabs, or it accounts on an incomplete picture and could
    /// reclaim prematurely. Recording them here makes `slab_summary()`/`slab_descriptors()`
    /// complete immediately after restore. Any slab id that is the current active slab, or already
    /// has a descriptor (e.g. it was fetched or is local), is left untouched. Call AFTER
    /// [`reserve_lazy_checkpoint_range`] so the reserved slab is the active one and every
    /// checkpoint slab is correctly sealed.
    pub fn install_lazy_checkpoint_slabs(
        &self,
        slabs: &[LazyCheckpointSlab],
    ) -> Result<(), BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let root = inner.root.clone();
        let active = inner.block_slab_id;
        let mut changed = false;
        for slab in slabs {
            // Never touch the active (reserved) slab — it holds live local writes.
            if slab.block_slab_id == active {
                continue;
            }
            // If the slab is materialized locally (already fetched / a real local slab), its
            // existing descriptor reflects real on-disk bytes and is authoritative — leave it.
            if slab_path(&root, slab.block_slab_id).exists() {
                continue;
            }
            // Lazily-backed checkpoint slab: install (or replace a fresh-store placeholder — a
            // freshly opened block store seeds an empty Active descriptor for slab 0, which
            // `reserve_lazy_checkpoint_range` then seals; that stale empty descriptor must be
            // overwritten with the checkpoint's real slab metadata, not skipped) a complete
            // SEALED descriptor so accounting is correct before any fetch.
            let descriptor = BlockStoreSlabDescriptor {
                stored_slab_id: slab.block_slab_id,
                block_slab_id: slab.block_slab_id,
                state: BlockStoreSlabState::Sealed,
                physical_bytes: slab.physical_bytes,
                logical_bytes: slab.logical_bytes,
                created_unix_ms: slab.created_unix_ms,
                updated_unix_ms: slab.updated_unix_ms,
                first_block_id: slab.first_block_id,
                last_block_id: slab.last_block_id,
                readable_prefix_physical_bytes: slab.physical_bytes,
                verified_source_mtime_unix_ms: None,
                has_corruption: false,
                first_error_offset: None,
                first_error: None,
            };
            if inner.slabs.get(&slab.block_slab_id) != Some(&descriptor) {
                inner.slabs.insert(slab.block_slab_id, descriptor);
                changed = true;
            }
        }
        if changed {
            persist_slab_manifest(&inner.root, &inner.slabs)?;
        }
        Ok(())
    }

    /// Ensure `block_slab_id` is present locally, fetching it from the shared
    /// read-through source (if any) on a local miss, caching it, and counting the
    /// on-demand fetch. A no-op when the slab already exists locally or no source is
    /// attached; in the latter case the normal read path surfaces the miss.
    fn ensure_slab_present(&self, block_slab_id: u64) -> Result<(), BlockStoreError> {
        let (root, source) = {
            let inner = self.inner.lock().expect("block store lock poisoned");
            (inner.root.clone(), inner.shared_slab_source.clone())
        };
        if slab_path(&root, block_slab_id).exists() {
            return Ok(());
        }
        let Some(source) = source else {
            return Ok(());
        };
        if let Some(bytes) = source.fetch_slab(block_slab_id)? {
            self.install_slab(block_slab_id, &bytes)?;
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
    /// directory, and persists the slab manifest. Run inline from `append` -- which is where
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

    pub fn slab_usage(&self) -> Vec<BlockStoreSlabUsage> {
        compute_slab_usage(
            &self
                .inner
                .lock()
                .expect("block store lock poisoned")
                .slabs,
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
            .purged_block_slab_ids)
    }

    pub fn purge_delayed_destroy_slabs_with_report(
        &self,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
    }

    /// Purge quarantined slabs that have waited at least `min_age_ms`.
    ///
    /// The age is a parameter so a caller that wants an immediate purge has to SAY so, rather
    /// than a suite quietly depending on there being no delay at all.
    ///
    /// Purges WITHOUT a liveness re-check. Every caller that can name a live set should call
    /// [`Self::purge_delayed_destroy_slabs_checked`] instead; this spelling remains for the
    /// callers that genuinely have nothing to re-check against.
    pub(crate) fn purge_delayed_destroy_slabs_older_than(
        &self,
        min_age_ms: u64,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_checked(min_age_ms, std::iter::empty())
    }

    /// Purge quarantined slabs, re-checking liveness one last time before each destroy.
    ///
    /// THE DESTROY IS THE ONLY IRREVERSIBLE STEP, AND UNTIL NOW IT CONSULTED NOTHING. The purge
    /// read a directory and a timestamp; it took no live set and re-read no index. Everything it
    /// knew about whether a slab was still needed was decided in an earlier round, by the
    /// collector that quarantined it, against the state of the store at THAT moment. Between the
    /// two, a dump manifest can be written whose embedded index installs pages in a slab that was
    /// unreferenced when the collector looked -- and the purge would unlink it without ever
    /// asking.
    ///
    /// So the destroy asks again, against the live set the caller holds NOW, and a slab that
    /// comes back live is not destroyed. It is taken back out of quarantine: renamed into the
    /// store, its descriptor returned to `Sealed`, and reported in `restored_block_slab_ids`.
    /// Un-quarantining is not a nicety here the way rolling a slab back to its pre-collection
    /// state would be elsewhere -- our phase 1 RENAMES the file, so a slab left sitting in the
    /// trash directory is unreadable by path no matter how long the grace window runs. Declining
    /// to destroy it would leave the reader just as broken. Moving it back is the whole repair.
    ///
    /// A restore is an ALARM. Reaching it means the collector and the re-check disagreed about
    /// the same slab, which is a bug upstream of this function; the point of the re-check is that
    /// the bug costs a log line and a rename instead of the data.
    pub fn purge_delayed_destroy_slabs_checked(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_selected(min_age_ms, live_block_slab_ids, None)
    }

    /// Purge only the quarantined slabs the caller NAMES.
    ///
    /// `None` purges every quarantined slab old enough, which is what this did before it could be
    /// told otherwise. `Some(set)` purges only the intersection, and a quarantined slab left out
    /// of the set is neither destroyed nor reported as too young -- it is simply not this round's
    /// business, and it comes back as a candidate next round.
    ///
    /// THIS EXISTS SO THE RECLAIM GATE CAN STOP BEING ALL-OR-NOTHING. The dependency plan works
    /// out, per slab, which candidates are pinned and which are free; until now the collector and
    /// the purge both consulted a single store-wide boolean derived from that plan, so one pinned
    /// slab suppressed the collection of every other candidate. Letting the plan's answer through
    /// per slab requires the purge to accept a list: otherwise a purge that ran because SOME slab
    /// was free would destroy the quarantined slabs the plan had specifically blocked, which is a
    /// loss path rather than an inefficiency. The two changes only make sense together.
    pub fn purge_delayed_destroy_slabs_selected(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        selected_block_slab_ids: Option<BTreeSet<u64>>,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        self.purge_delayed_destroy_slabs_capped(
            min_age_ms,
            live_block_slab_ids,
            selected_block_slab_ids,
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        )
    }

    /// The purge, bounded to `max_slabs_per_round` slabs of work. 0 means unbounded.
    ///
    /// THE BOUND IS ON THE LOCK HOLD. Everything above funnels into this function, and this
    /// function takes the store-wide lock and keeps it until it returns, so the round's length is
    /// the length of time nothing else can touch the store. Unbounded, that was 58.7 seconds at
    /// eighty thousand quarantined slabs, measured, and linear -- the quarantine has no ceiling of
    /// its own, so neither did the hold. The cap is the shipped default rather than an opt-in
    /// because a caller that forgets it does not get a slower purge, it gets a minute-long stall.
    ///
    /// THE BUDGET IS SPENT ON WORK DONE -- a destroy or a restore -- AND NOT ON ENTRIES EXAMINED.
    /// That is the whole reason this advances rather than stalling, and getting it the other way
    /// round is the failure this has to avoid: a slab that is skipped (not named by the caller's
    /// list, not old enough yet, or live-but-blocked) STAYS IN THE DIRECTORY, so if a skip cost
    /// budget, a quarantine whose first thousand entries were all blocked would spend every
    /// round's whole budget re-skipping the same thousand and destroy nothing, for ever. Spending
    /// budget only on work makes progress unconditional: every slab this round charges to the
    /// budget LEAVES the trash directory -- unlinked, or renamed back into the store -- so the
    /// actionable set is strictly smaller next round, and the skipped prefix the loop walks past
    /// is bounded by the number of slabs that are being skipped for a reason of their own.
    ///
    /// THE RE-CHECK IS NOT WHAT GETS CAPPED. Every slab the round reaches goes through the full
    /// liveness re-check and the un-quarantining restore before anything irreversible happens to
    /// it; the budget decides HOW MANY slabs a round reaches, never what happens to one it did.
    /// A round that stops early leaves no slab half-processed: each iteration destroys or
    /// restores one slab completely, and the manifest written after the loop records exactly the
    /// slabs whose state actually changed. A slab the round never reached is still quarantined,
    /// which is the state it was already in.
    pub fn purge_delayed_destroy_slabs_capped(
        &self,
        min_age_ms: u64,
        live_block_slab_ids: impl IntoIterator<Item = u64>,
        selected_block_slab_ids: Option<BTreeSet<u64>>,
        max_slabs_per_round: usize,
    ) -> Result<BlockStorePurgeDelayedDestroyReport, BlockStoreError> {
        let live_block_slab_ids = live_block_slab_ids.into_iter().collect::<BTreeSet<_>>();
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let trash_dir = delayed_destroy_dir(&inner.root);
        let mut purged = Vec::new();
        let mut purged_physical_bytes = 0;
        let mut retained_too_young = Vec::new();
        let mut retained_too_young_physical_bytes = 0;
        let mut restored = Vec::new();
        let mut restored_physical_bytes = 0;
        let mut restore_blocked = Vec::new();
        let mut processed = 0usize;
        let mut budget_exhausted = false;
        if !trash_dir.exists() {
            return Ok(BlockStorePurgeDelayedDestroyReport {
                max_slabs_per_round,
                ..Default::default()
            });
        }
        let root = inner.root.clone();
        for entry in fs::read_dir(&trash_dir)? {
            // THE BUDGET IS CHECKED BEFORE THE ENTRY IS EVEN NAMED, and it stops the round rather
            // than skipping to the next entry. Continuing would walk the remaining eighty
            // thousand directory entries to do no work; stopping is what makes a capped round
            // cost the budget rather than the directory. The unexamined entries stay exactly as
            // they are, and `budget_exhausted` tells the caller there are more.
            if max_slabs_per_round > 0 && processed >= max_slabs_per_round {
                budget_exhausted = true;
                break;
            }
            let entry = entry?;
            let Some(id) = delayed_destroy_slab_id_from_name(&entry.file_name()) else {
                continue;
            };
            let bytes = entry
                .metadata()
                .map(|metadata| metadata.len())
                .unwrap_or_default();
            // Not this round's business. Checked BEFORE the liveness re-check and before the
            // age, because a slab the caller did not name should be left exactly as it is: not
            // destroyed, not restored, and not reported as too young -- "too young" is a claim
            // about a slab that was considered, and this one was not.
            if selected_block_slab_ids
                .as_ref()
                .map(|selected| !selected.contains(&id))
                .unwrap_or(false)
            {
                continue;
            }
            // THE LAST-CHANCE RE-CHECK, before the age is even consulted: a live slab is not
            // "too young to destroy", it is NOT FOR DESTROYING, and waiting longer would not
            // make it safe. It also must not be left where it is -- quarantine is a rename out
            // of the store, so the reader that needs it cannot reach it until the file is back.
            if live_block_slab_ids.contains(&id) {
                if restore_slab_from_delayed_destroy_unsynced(&root, id, &entry.path())? {
                    set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Sealed);
                    restored.push(id);
                    restored_physical_bytes += bytes;
                    // A restore is WORK: it renamed a file and changed a descriptor, and it is
                    // the expensive half of the measured round. A blocked restore is not -- it
                    // moved nothing, and charging budget for it would let a directory full of
                    // blocked slabs starve the destroys, which is the stall this cap must not
                    // have.
                    processed += 1;
                } else {
                    restore_blocked.push(id);
                }
                continue;
            }
            // Quarantining goes through `set_slab_state`, which stamps `updated_unix_ms`, so the
            // manifest already records WHEN this slab was set aside.
            //
            // A DESCRIPTOR-LESS SLAB FALLS BACK TO THE FILE'S MTIME rather than being destroyed
            // on sight. The two are written at different moments and a crash fits between them:
            // the collector renames each slab into the trash directory inside its loop and
            // persists the manifest only once, after it. Die mid-loop and the restarted process
            // finds files in quarantine that the manifest never learned about -- freshly
            // quarantined slabs with no stamp, which the old reading purged on the very next
            // round with the full hour of grace skipped. The mtime is a weaker clock (a rename
            // keeps the mtime of the last append, so it runs EARLY and the window it grants is
            // shorter than the real one) but it is a clock, and a short window beats none.
            let quarantined_at = inner
                .slabs
                .get(&id)
                .and_then(|slab| slab.updated_unix_ms)
                .or_else(|| file_modified_unix_ms(&entry.path()));
            if let Some(quarantined_at) = quarantined_at {
                if now_unix_ms().saturating_sub(quarantined_at) < min_age_ms {
                    retained_too_young.push(id);
                    retained_too_young_physical_bytes += bytes;
                    continue;
                }
            }
            purged_physical_bytes += bytes;
            fs::remove_file(entry.path())?;
            set_slab_state(&mut inner.slabs, id, BlockStoreSlabState::Purged);
            purged.push(id);
            processed += 1;
        }
        purged.sort_unstable();
        restored.sort_unstable();
        restore_blocked.sort_unstable();
        // BOTH directories, once, and before the manifest.
        //
        // The unlinks always needed the trash directory synced and always got it here -- one
        // fsync for the round, which is the shape the quarantine side has now been moved to. What
        // is new is the store ROOT: a restore renames a slab out of quarantine and back into the
        // store, and that rename used to be made durable by two fsyncs inside
        // `restore_slab_from_delayed_destroy`, once per restored slab. Syncing the root here
        // instead commits every one of this round's restores together. It was in fact already
        // reaching disk, as a side effect of `persist_slab_manifest` fsyncing the root to commit
        // its own rename -- which is exactly the kind of accident that survives until someone
        // reorders the two calls. It is stated here instead of relied upon there.
        //
        // AND ONLY WHEN THE ROUND ACTUALLY DID SOMETHING, which is the guard the collector half of
        // this stage already carries (`a_gc_round_that_reclaimed_nothing_does_not_rewrite_the_
        // manifest`) and this half did not. Both run in the same periodic round, from
        // `apply_storage_lifecycle`, and the reasoning recorded there applies unchanged: the
        // manifest write serialises every slab, fsyncs a temp file, renames it and fsyncs the
        // parent directory.
        //
        // `inner.slabs` is mutated in exactly two places in the loop above, both `set_slab_state`
        // -- one in the branch that pushes onto `restored`, one in the branch that pushes onto
        // `purged` -- and the only filesystem changes are the rename inside those restores and the
        // unlink inside those destroys. A `restore_blocked` entry moves nothing
        // (`restore_slab_from_delayed_destroy_unsynced` returns before its rename when the
        // destination is occupied) and a `retained_too_young` entry touches nothing at all. So with
        // both lists empty there is no rename and no unlink for an fsync to commit, and the
        // manifest would be rewritten with byte-identical content.
        //
        // That is the ordinary shape of a purge round, not a corner: a quarantined slab waits
        // DELAYED_DESTROY_MIN_AGE_MS -- an hour -- and every round in that window walks the whole
        // trash directory and acts on none of it. Measured on a sixteen-slab quarantine: three
        // directory fsyncs plus a full manifest rewrite per round, for no durable change.
        if !purged.is_empty() || !restored.is_empty() {
            sync_delayed_destroy_dirs(&root)?;
            persist_slab_manifest(&inner.root, &inner.slabs)?;
        }
        retained_too_young.sort_unstable();
        Ok(BlockStorePurgeDelayedDestroyReport {
            purged_block_slab_ids: purged,
            purged_physical_bytes,
            retained_too_young_block_slab_ids: retained_too_young,
            retained_too_young_physical_bytes,
            restored_block_slab_ids: restored,
            restored_physical_bytes,
            restore_blocked_block_slab_ids: restore_blocked,
            processed_block_slabs: processed,
            budget_exhausted,
            max_slabs_per_round,
        })
    }

    /// Move every quarantined slab's stamp `age_ms` into the past, so a test can reach the far
    /// side of [`DELAYED_DESTROY_MIN_AGE_MS`] without an hour of wall clock.
    ///
    /// This does NOT shorten the age the purge enforces. The scheduled round still calls
    /// `purge_delayed_destroy_slabs_with_report()` with the shipped one-hour minimum; what moves
    /// is the slab's own arrival time. So a purge observed after this is the real gate firing on
    /// a slab that is genuinely old by its own clock, which is a different and stronger claim
    /// than `purge_delayed_destroy_slabs_older_than(0)` -- that one proves only that a purge with
    /// the delay removed removes things.
    ///
    /// Returns how many descriptors moved, and a caller MUST assert that count before reading a
    /// purge result. A purge reporting zero because nothing was backdated looks exactly like a
    /// purge reporting zero because the mechanism is broken.
    #[cfg(test)]
    pub(crate) fn backdate_delayed_destroy_stamps_for_test(
        &self,
        age_ms: u64,
    ) -> Result<usize, BlockStoreError> {
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let root = inner.root.clone();
        let quarantined = delayed_destroy_slab_ids_at(&root)?;
        let mut moved = 0usize;
        for block_slab_id in quarantined {
            if let Some(slab) = inner.slabs.get_mut(&block_slab_id) {
                let stamp = slab.updated_unix_ms.unwrap_or_else(now_unix_ms);
                slab.updated_unix_ms = Some(stamp.saturating_sub(age_ms));
                moved += 1;
            }
        }
        persist_slab_manifest(&root, &inner.slabs)?;
        Ok(moved)
    }

    /// Per-slab size and block count, without decoding any block.
    ///
    /// What the reclaim planner needs out of `slab_reports()`, at the cost of a header walk
    /// instead of a CRC-and-decompress of every record in the store.
    pub fn slab_block_counts(&self) -> Result<Vec<(u64, u64, u64)>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut out = Vec::new();
        for block_slab_id in slab_ids_at(&root)? {
            let bytes = fs::read(slab_path(&root, block_slab_id))?;
            let (block_count, physical_bytes) =
                crate::block_store::record::count_slab_blocks(&bytes, block_slab_id);
            out.push((block_slab_id, physical_bytes, block_count));
        }
        Ok(out)
    }

    pub fn slab_reports(&self) -> Result<Vec<BlockStoreSlabReport>, BlockStoreError> {
        let root = self
            .inner
            .lock()
            .expect("block store lock poisoned")
            .root
            .clone();
        let mut reports = Vec::new();
        for block_slab_id in slab_ids_at(&root)? {
            let bytes = fs::read(slab_path(&root, block_slab_id))?;
            reports.push(inspect_slab(&bytes, block_slab_id));
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
    crate::engine::bulk_ingest_mode()
}

/// On the live path, defer the per-record extent-manifest persist to sync_durable()/slab-seal
/// (the manifest is reconciled from disk on open). Single-barrier default; restored to a
/// synchronous persist only under the TS_WAL_LEGACY_RECOVERY escape hatch. Moves in lockstep
/// with `block_wal_single_barrier` (the intermediate "manifest-only" relaxation is gone).
pub(crate) fn block_wal_only_sync() -> bool {
    block_wal_single_barrier()
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
pub(crate) fn block_wal_single_barrier() -> bool {
    // One reader for the hatch, in `engine`. This parsed it itself, as did index_log, and the
    // three barriers they gate have to move together -- a copy that drifted would leave one of
    // them on the legacy path and the others on the default.
    !crate::engine::wal_legacy_recovery()
}

/// Slabs are neither preallocated nor recycled, and both are deliberate.
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
/// `bulk_relaxed_durability() || block_wal_single_barrier()`, and the second is true unless legacy
/// recovery is turned back on -- so by default the per-write page fdatasync is already deferred
/// (see the note on `block_wal_only_sync`). Preallocating would remove a cost that is not being
/// paid.
///
/// Recycling a slab rather than creating and unlinking one is the same story from the other end.
/// What it saves is the create and the unlink -- and slabs are large, so that turnover is rare
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
    let previous_block_slab_id = inner.block_slab_id;
    // The outgoing slab may hold relaxed (un-fsynced) bulk appends; make them
    // durable before we seal and stop writing to it.
    {
        let prev_path = slab_path(&inner.root, previous_block_slab_id);
        if let Ok(prev) = OpenOptions::new().append(true).open(&prev_path) {
            crate::durability_metrics::record_barrier("block_store_slab_roll_prev");
            let _ = prev.sync_data();
        }
    }
    let next_from_current = inner.block_slab_id.saturating_add(1);
    let next_from_disk = slab_ids_at(&inner.root)?
        .into_iter()
        .max()
        .map(|id| id.saturating_add(1))
        .unwrap_or_default();
    inner.block_slab_id = next_from_current.max(next_from_disk);
    inner.write_offset = 0;
    let path = slab_path(&inner.root, inner.block_slab_id);
    let file = File::create(&path)?;
    crate::durability_metrics::record_barrier("block_store_slab_roll");
    file.sync_all()?;
    sync_parent_dir(&path)?;
    let transition_unix_ms = now_unix_ms();
    if let Some(previous) = inner.slabs.get_mut(&previous_block_slab_id) {
        previous.state = BlockStoreSlabState::Sealed;
        previous.updated_unix_ms = Some(transition_unix_ms);
    }
    let new_slab = BlockStoreSlabDescriptor {
        stored_slab_id: inner.block_slab_id,
        block_slab_id: inner.block_slab_id,
        state: BlockStoreSlabState::Active,
        physical_bytes: 0,
        logical_bytes: 0,
        created_unix_ms: Some(transition_unix_ms),
        updated_unix_ms: Some(transition_unix_ms),
        first_block_id: None,
        last_block_id: None,
        readable_prefix_physical_bytes: 0,
        verified_source_mtime_unix_ms: None,
        has_corruption: false,
        first_error_offset: None,
        first_error: None,
    };
    let block_slab_id = inner.block_slab_id;
    inner.slabs.insert(block_slab_id, new_slab);
    persist_slab_manifest(&inner.root, &inner.slabs)?;
    Ok(BlockStoreRollReport {
        previous_block_slab_id,
        new_block_slab_id: inner.block_slab_id,
    })
}

fn slab_lifecycle_states(summary: &BlockStoreSlabSummary) -> Vec<String> {
    let mut states = Vec::new();
    if summary.active_slabs > 0 {
        states.push("active".to_string());
    }
    if summary.sealed_slabs > 0 {
        states.push("sealed".to_string());
    }
    if summary.delayed_destroy_slabs > 0 {
        states.push("delayed_destroy".to_string());
    }
    if summary.purged_slabs > 0 {
        states.push("purged".to_string());
    }
    states
}

fn compute_slab_usage(
    slabs: &BTreeMap<u64, BlockStoreSlabDescriptor>,
) -> Vec<BlockStoreSlabUsage> {
    #[derive(Debug, Clone)]
    struct SlabUsageAcc {
        usage: BlockStoreSlabUsage,
    }

    fn merged_slab_state(
        left: BlockStoreSlabState,
        right: BlockStoreSlabState,
    ) -> BlockStoreSlabState {
        use BlockStoreSlabState::*;
        match (left, right) {
            (Active, _) | (_, Active) => Active,
            (Sealed, _) | (_, Sealed) => Sealed,
            (DelayedDestroy, _) | (_, DelayedDestroy) => DelayedDestroy,
            (Purged, Purged) => Purged,
        }
    }

    let mut usage_by_slab = BTreeMap::<u64, SlabUsageAcc>::new();
    for slab in slabs.values() {
        let (live, reclaimable, purged) = match slab.state {
            BlockStoreSlabState::Active | BlockStoreSlabState::Sealed => {
                (slab.physical_bytes, 0, 0)
            }
            BlockStoreSlabState::DelayedDestroy => (0, slab.physical_bytes, 0),
            BlockStoreSlabState::Purged => (0, 0, slab.physical_bytes),
        };
        let entry = usage_by_slab
            .entry(slab.stored_slab_id)
            .or_insert_with(|| SlabUsageAcc {
                usage: BlockStoreSlabUsage {
                    stored_slab_id: slab.stored_slab_id,
                    block_slab_id: slab.block_slab_id,
                    storage_slab_id: slab.stored_slab_id,
                    stream_slab_id: slab.block_slab_id,
                    state: slab.state,
                    used_bytes: 0,
                    live_bytes: 0,
                    reclaimable_bytes: 0,
                    purged_bytes: 0,
                    block_store_used_bytes: 0,
                    live_block_store_used_bytes: 0,
                    reclaimable_block_store_used_bytes: 0,
                    purged_block_store_used_bytes: 0,
                    first_block_id: None,
                    last_block_id: None,
                },
            });
        let usage = &mut entry.usage;
        usage.block_slab_id = usage.block_slab_id.min(slab.block_slab_id);
        usage.stream_slab_id = usage.stream_slab_id.min(slab.block_slab_id);
        usage.state = merged_slab_state(usage.state, slab.state);
        usage.used_bytes = usage.used_bytes.saturating_add(slab.physical_bytes);
        usage.live_bytes = usage.live_bytes.saturating_add(live);
        usage.reclaimable_bytes = usage.reclaimable_bytes.saturating_add(reclaimable);
        usage.purged_bytes = usage.purged_bytes.saturating_add(purged);
        usage.block_store_used_bytes = usage
            .block_store_used_bytes
            .saturating_add(slab.physical_bytes);
        usage.live_block_store_used_bytes = usage.live_block_store_used_bytes.saturating_add(live);
        usage.reclaimable_block_store_used_bytes = usage
            .reclaimable_block_store_used_bytes
            .saturating_add(reclaimable);
        usage.purged_block_store_used_bytes =
            usage.purged_block_store_used_bytes.saturating_add(purged);
        usage.first_block_id = match (usage.first_block_id, slab.first_block_id) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (None, right) => right,
            (left, None) => left,
        };
        usage.last_block_id = match (usage.last_block_id, slab.last_block_id) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (None, right) => right,
            (left, None) => left,
        };
    }
    usage_by_slab.into_values().map(|acc| acc.usage).collect()
}

impl Default for BlockStore {
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
        block_slab_id: u64,
        offset: u64,
        length: u64,
        block_id: Option<u64>,
        object_id: Option<u64>,
        routing_bucket: Option<u32>,
        generation: Option<u64>,
        stored_slab_id: Option<u64>,
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

    /// A presence bit is not the same as a zero value. `0` is a legitimate `routing_slot` and a
    /// legitimate `generation`, so "zero means absent" would erase real values -- this is why the
    /// byte exists instead of a sentinel.
    ///
    /// The grouping id was a third example here and is no longer one: it is the slab's own id, so
    /// it is always known, never absent, and needs no bit.
    #[test]
    fn zero_is_distinguishable_from_absent() {
        let zero = BlockAddress::from_parts(1, 0, 0, None, None, Some(0), Some(0));
        let absent = BlockAddress::from_parts(1, 0, 0, None, None, None, None);
        assert_eq!(zero.routing_bucket(), Some(0));
        assert_eq!(zero.generation(), Some(0));
        assert_eq!(absent.routing_bucket(), None);
        assert_eq!(absent.generation(), None);
        assert_ne!(zero, absent);
        assert_eq!(zero.slab_id(), Some(1), "the slab id is derived, present either way");
        assert_eq!(absent.slab_id(), Some(1));
    }

    /// Clearing a value must clear its bit, or the next read reports a stale one as present.
    #[test]
    fn setters_track_presence_both_ways() {
        let mut address =
            BlockAddress::from_parts(1, 0, 0, Some(7), None, None, None);
        assert_eq!(address.block_id(), Some(7));
        address.set_block_id(None);
        assert_eq!(address.block_id(), None);
        address.set_block_id(Some(9));
        assert_eq!(address.block_id(), Some(9));
        address.set_object_id(Some(3));
        assert_eq!(address.object_id(), Some(3));
        assert_eq!(address.block_id(), Some(9), "one setter disturbed another");
    }

    /// The packing is an in-memory concern, and the field names are a wire concern: an address
    /// writes the short names and still reads the long ones an older index was written with.
    ///
    /// Both halves matter and they pull in opposite directions. Writing the long names again would
    /// undo the saving that shortening them bought -- fifteen characters to label an integer, once
    /// per index item forever. Failing to read them would make every index already on disk
    /// unresolvable, and the blocks it points at unreachable.
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
        );
        let json = serde_json::to_value(&address).unwrap();
        // What is written now: the short names, and nothing else.
        assert_eq!(json["ps"], 5, "the page slab id");
        assert_eq!(json["o"], 64, "the offset");
        assert_eq!(json["l"], 128, "the length");
        assert_eq!(json["rs"], 3, "the routing bucket");
        assert!(
            json.get("b").is_none(),
            "the slab id is derived rather than written"
        );
        for long in ["page_segment_id", "routing_slot", "band_id", "object_id", "generation"] {
            assert!(
                json.get(long).is_none(),
                "{long} is a read alias now, not something to write"
            );
        }
        assert!(json.get("present").is_none(), "presence bits must not reach the wire");

        // What is still read: the same address spelled the old way, which is what an index already
        // on disk looks like.
        let round_tripped: BlockAddress = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, address, "an address written now must read back identical");
        let long_form = serde_json::json!({
            "page_segment_id": 5_u64,
            "offset": 64_u64,
            "length": 128_u64,
            "page_id": 1_u64,
            "object_id": 2_u64,
            "routing_slot": 3_u32,
            "generation": 4_u64,
            "band_id": 0_u64,
        });
        let from_long: BlockAddress = serde_json::from_value(long_form).unwrap();
        assert_eq!(
            from_long, address,
            "an index written with the long names must still load as the same address"
        );
    }
}

#[cfg(test)]
mod tests {

    /// A rename may not quietly drop a durable name.
    ///
    /// The vocabulary migration -- page to block, zone to slab, slot to bucket -- is deliberate and
    /// only half done, and what keeps it safe is that every historical spelling survives as a serde
    /// alias, so a store written by an older build still loads. `BlockAddressWire` states the rule
    /// directly: every short name carries every spelling the field has ever had.
    ///
    /// Nothing checked it. A rename that changed a field AND its alias would compile, pass every
    /// other test, and make existing data unreadable -- with the symptom appearing at load, on
    /// someone else's machine, later.
    ///
    /// The check runs one way. ADDING a durable name is free, which is what keeps this quiet while
    /// the migration proceeds; removing or respelling one fails here with the name in the message.
    /// If a removal is genuinely intended -- a field that no store in the world still carries --
    /// delete it from the list below in the same change, so the decision is written down.
    #[test]
    fn a_durable_name_is_never_quietly_dropped() {
        // Every serde `rename`/`alias` in the crate that carries the old vocabulary, as of the
        // change that added this test.
        const DURABLE_NAMES: &[&str] = &[
        "active_page_segment_ids",
        "active_page_slab_ids",
        "active_storage_zones",
        "active_zones",
        "actual_routing_slot",
        "block_segment_id",
        "block_segment_target_bytes",
        "block_store_segment_api_ready",
        "block_store_zones",
        "blocked_page_segment_ids",
        "blocked_page_slab_ids",
        "bounded_max_dump_slots_per_round",
        "cache_slot_entry_count",
        "candidate_page_segment_ids",
        "candidate_page_slab_ids",
        "cold_slots_scanned",
        "compact_segment_address",
        "compact_segment_id",
        "compact_segment_offset",
        "compacted_page_segment_id",
        "compacted_page_slab_id",
        "configured_page_gc_raft_install_floor_segment_id",
        "corrupt_page_index_wal_snapshot_evidence_ready",
        "corrupt_page_segment_count",
        "corrupt_page_segment_ids",
        "corrupt_page_slab_count",
        "corrupt_page_slab_ids",
        "covered_slot_count",
        "delayed_destroy_page_segment_ids",
        "delayed_destroy_page_slab_ids",
        "delayed_destroy_purged_segments",
        "delayed_destroy_segment_count",
        "delayed_destroy_zones",
        "deleted_slot_count",
        "dirty_slot_count",
        "dirty_slot_pressure",
        "dirty_slots",
        "dirty_slots_committed_before_truncate",
        "discovered_page_segment_count",
        "discovered_page_slab_count",
        "dumped_slot_count",
        "durable_slot_generation_frontier_index_log_sequence",
        "durable_slot_generation_frontier_wal_sequence",
        "empty_slots",
        "end_routing_slot",
        "end_slot",
        "expected_routing_slot",
        "expired_slot_object_scan_debt",
        "extent_count",
        "extent_id",
        "extent_manifest_ready",
        "extents",
        "fanout_segment_count",
        "first_class_slot_object_page_index_evidence",
        "first_class_slot_object_page_index_ready",
        "first_routing_slot",
        "hot_slots_scanned",
        "in_memory_slot_count",
        "index_gc_commit_dirty_slots_before_truncation",
        "indexed_page_segment_count",
        "indexed_page_slab_count",
        "installed_slot_dump_install_count",
        "interrupted_slot_dump_install_count",
        "interrupted_slot_dump_installs",
        "last_compacted_zone",
        "last_routing_slot",
        "last_selected_slots",
        "live_page_segment_count",
        "live_page_segment_ids",
        "live_page_slab_count",
        "live_page_slab_ids",
        "live_routing_slot_count",
        "load_cold_slots",
        "load_cold_slots_for_expire",
        "loading_slot_count",
        "manifest_page_segment_ids",
        "manifest_page_slab_ids",
        "manifest_slot_ids",
        "max_cold_slots_per_round",
        "max_destroy_segments",
        "max_dirty_slots",
        "max_dump_slots_per_round",
        "max_expire_cold_slots_per_round",
        "max_expire_hot_slots_per_round",
        "max_hot_slots_per_round",
        "max_orphan_page_segments",
        "max_orphan_page_slabs",
        "max_stale_page_segments",
        "max_stale_page_slabs",
        "metrics_slot_count",
        "missing_dump_slot_ids",
        "missing_page_segment_ids",
        "missing_page_slab_ids",
        "missing_routing_slot_count",
        "missing_slot_generations",
        "multi_object_slots",
        "multi_page_object_slots",
        "native_packed_slot_node_hex",
        "native_packed_slot_node_len",
        "native_packed_slot_node_size",
        "native_slot_store_layout_transition_evidence",
        "native_slot_store_layout_transition_ready",
        "new_page_segment_id",
        "new_page_slab_id",
        "oldest_known_zone_age_ms",
        "oldest_known_zone_unix_ms",
        "oldest_live_zone_age_ms",
        "oldest_live_zone_unix_ms",
        "oldest_reclaimable_zone_age_ms",
        "oldest_reclaimable_zone_unix_ms",
        "orphan_page_segment_count",
        "orphan_page_segment_ids",
        "orphan_page_slab_count",
        "orphan_page_slab_ids",
        "page",
        "page_gc_checkpoint_floor_segment_id",
        "page_gc_raft_install_floor_segment_id",
        "page_id",
        "page_in_log",
        "page_index_count",
        "page_index_entries",
        "page_ref_key",
        "page_refs",
        "page_segment_id",
        "page_segment_ids",
        "page_segment_live_reports",
        "page_segment_manifest_ready",
        "page_segment_reports",
        "page_segment_stale_density_basis_points",
        "page_segments",
        "page_segments_reclaimed",
        "page_segments_removed",
        "page_segments_removed_physical_bytes",
        "page_segments_retained_live",
        "page_segments_retained_live_physical_bytes",
        "page_segments_retained_physical_bytes",
        "page_size",
        "page_slab_count",
        "page_slab_id",
        "page_slab_ids",
        "page_slab_live_reports",
        "page_slab_manifest_ready",
        "page_slab_reports",
        "page_slab_stale_density_basis_points",
        "page_slabs",
        "page_slabs_reclaimed",
        "page_slabs_removed",
        "page_slabs_removed_physical_bytes",
        "page_slabs_retained_live",
        "page_slabs_retained_live_physical_bytes",
        "page_slabs_retained_physical_bytes",
        "page_store_bytes_written",
        "prepared_slot_dump_install_count",
        "previous_page_segment_id",
        "previous_page_slab_id",
        "prune_slot_dump_manifests",
        "purged_page_segment_ids",
        "purged_page_slab_ids",
        "purged_zones",
        "reclaimable_page_segment_ids",
        "reclaimable_page_slab_ids",
        "reclaimable_stale_page_segment_count",
        "reclaimable_stale_page_slab_count",
        "removed_page_segment_ids",
        "removed_page_slab_ids",
        "require_slot_dump_manifest",
        "retain_from_page_segment_id",
        "retain_from_page_slab_id",
        "retain_page_segments_from_id",
        "retain_page_slabs_from_id",
        "retained_current_page_segment_ids",
        "retained_current_page_slab_ids",
        "retained_live_page_segment_ids",
        "retained_live_page_slab_ids",
        "retained_page_segment_ids",
        "retained_page_slab_ids",
        "roll_forward_slot_dump_installs",
        "routing_slot",
        "routing_slot_count",
        "routing_slots_embedded",
        "sealed_segment_count",
        "sealed_storage_zones",
        "sealed_zones",
        "secondary_views_reconciled_from_slot_index",
        "segment",
        "segment_count",
        "segment_fields",
        "segment_id",
        "segment_integrity",
        "segment_open_count",
        "segment_samples",
        "segment_sealed_count",
        "segments",
        "selected_dirty_slot_count",
        "selected_dump_slots",
        "selected_page_segment_ids",
        "selected_page_slab_ids",
        "selected_routing_slots",
        "selected_slots",
        "single_object_slots",
        "single_page_object_slots",
        "slot",
        "slot_count",
        "slot_dump_manifest",
        "slot_dump_manifest_block_count",
        "slot_dump_manifest_count",
        "slot_dump_manifest_id",
        "slot_entries",
        "slot_fields",
        "slot_first",
        "slot_id",
        "slot_ids",
        "slot_index",
        "slot_index_authority",
        "slot_index_entry_count",
        "slot_layout_states_after",
        "slot_layout_transition_count",
        "slot_map",
        "slot_nodes",
        "slot_object_page_authority_ready",
        "slot_object_ref_count",
        "slot_page_ref_count",
        "slot_samples",
        "slot_store_layout_api_ready",
        "slot_store_runtime_module",
        "slot_summaries",
        "slot_warmup_ready",
        "slots",
        "source_manifest_slot_ids",
        "source_slot_coverage_missing_slot_ids",
        "staged_pages",
        "stale_page_segment_count",
        "stale_page_segment_ids",
        "stale_page_segment_pressure",
        "stale_page_slab_count",
        "stale_page_slab_ids",
        "stale_page_slab_pressure",
        "start_routing_slot",
        "start_slot",
        "storage_segment_id",
        "storage_zone_count",
        "storage_zone_id",
        "storage_zone_stale_bytes",
        "storage_zone_total_bytes",
        "storage_zone_used_bytes",
        "stream_segment_count",
        "stream_segment_id",
        "total_segment_pages",
        "ttl_slot_count",
        "uncovered_slot_count",
        "unknown_slot_dump_install_count",
        "zone_count",
        "zone_descriptors",
        "zone_id",
        "zone_manifest_ready",
        "zone_stats_ready",
        "zone_summary",
        "zone_usage",
        "zone_version",
        "zones",
        ];

        // The list above lives inside the tree this walk reads, so every entry would match its
        // own literal and the check would verify nothing. Excise the declaration from the text
        // before searching: a name must be found because some OTHER site spells it.
        let marker = concat!("const ", "DURABLE_NAMES", ": &[&str] = &[");
        let strip_list_literal = |text: &str| -> (String, usize) {
            let Some(open) = text.find(marker) else {
                return (text.to_string(), 0);
            };
            let Some(close_rel) = text[open..].find("];") else {
                return (text.to_string(), 0);
            };
            let close = open + close_rel + "];".len();
            let mut kept = String::with_capacity(text.len());
            kept.push_str(&text[..open]);
            kept.push_str(&text[close..]);
            (kept, close - open)
        };

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut sources = String::new();
        let mut files_read = 0_usize;
        let mut excised_bytes = 0_usize;
        let mut files_excised = 0_usize;
        let mut pending = vec![root];
        while let Some(path) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&path) else {
                continue;
            };
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_dir() {
                    pending.push(entry_path);
                } else if entry_path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    if let Ok(text) = std::fs::read_to_string(&entry_path) {
                        files_read += 1;
                        let (kept, removed) = strip_list_literal(&text);
                        if removed > 0 {
                            files_excised += 1;
                            excised_bytes += removed;
                        }
                        sources.push_str(&kept);
                        sources.push('\n');
                    }
                }
            }
        }
        assert!(
            sources.len() > 100_000,
            "the source walk found almost nothing ({} bytes over {files_read} files); the guard \
             would pass vacuously",
            sources.len()
        );
        // Vacuity, part one: the excision must have happened, exactly once, and must have taken
        // the whole list with it. If the declaration is ever reshaped so the marker stops
        // matching, this fails instead of quietly restoring the self-matching haystack.
        assert_eq!(
            files_excised, 1,
            "expected exactly one file to hold the durable-name list; excised it from \
             {files_excised} of {files_read} files"
        );
        assert!(
            excised_bytes > 1_000,
            "the list excision removed only {excised_bytes} bytes from {files_excised} file(s); \
             the list holds {} names and cannot be that small -- the haystack would still match \
             each name against its own list entry",
            DURABLE_NAMES.len()
        );
        assert!(
            !DURABLE_NAMES.is_empty(),
            "DURABLE_NAMES is empty; there is nothing to check"
        );

        let mut missing = Vec::new();
        let mut found = 0_usize;
        for name in DURABLE_NAMES {
            let quoted = format!("\"{name}\"");
            if sources.contains(&quoted) {
                found += 1;
            } else {
                missing.push(*name);
            }
        }
        // Vacuity, part two: if the haystack ever loses its content, every name goes missing and
        // the assertion below fires -- but a haystack that matched nothing at all would be a
        // broken walk, not 260 real regressions, so say which it is.
        assert!(
            found > 0,
            "not one of the {} durable names was found outside the list itself across \
             {files_read} files ({} bytes searched, {excised_bytes} excised); the walk is broken, \
             not the vocabulary",
            DURABLE_NAMES.len(),
            sources.len()
        );
        assert!(
            missing.is_empty(),
            "durable name(s) no longer written anywhere outside the list itself: {missing:?}\n\
             ({found} of {} names still have a real site, across {files_read} files.)\n\
             A store written by an older build still carries these. If a rename is intended, keep \
             the old spelling as a `serde(alias = ...)` rather than replacing it; if the name is \
             genuinely dead, remove it from DURABLE_NAMES in the same change.",
            DURABLE_NAMES.len()
        );
    }

    #[test]
    fn an_address_written_with_any_older_field_name_still_loads() {
        // Every spelling this wire form has ever used, in one record: the original names, the
        // `page_segment_id` / `routing_slot` renames, and the older `extent_id` / `zone_id` /
        // `checksum` aliases. All of them must still land, or an index already on disk stops
        // resolving and the blocks it points at become unreachable.
        let legacy = serde_json::json!({
            "page_segment_id": 3_u64,
            "offset": 128_u64,
            "length": 126_u64,
            "page_id": 9_u64,
            "object_id": 122110326161599232_u64,
            "routing_slot": 545210715_u32,
            "generation": 2_u64,
            "zone_id": 4_u64,
            "checksum": "c38c2bf3055c516a98ac5d97f30e7c364e827bc0a1b2c3d4e5f60718293a4b5c"
        });
        let address: BlockAddress = serde_json::from_value(legacy).expect("a legacy address loads");
        assert_eq!(address.block_slab_id, 3);
        assert_eq!(address.offset, 128);
        assert_eq!(address.length, 126);
        // Every other field the legacy record carried, under whichever name it used: this is the
        // "all of them must still land" the comment above promises, and asserting one of them was
        // never enough to keep that promise.
        assert_eq!(address.block_id(), Some(9));
        assert_eq!(address.object_id(), Some(122110326161599232));
        assert_eq!(address.routing_bucket(), Some(545210715));
        assert_eq!(address.generation(), Some(2));
        // A slab is the slab now, so a slab STORED against a different slab is accepted and
        // ignored rather than believed. This record says slab 3 and zone 4, which could only have
        // been written under a configuration that sized slabs and slabs differently -- one the
        // tree never set, and no longer has a knob for.
        assert_eq!(
            address.slab_id(),
            Some(3),
            "the address answers the slab, whatever an older record stored beside it"
        );
        // And the digest is accepted and dropped rather than rejected: an index written before the
        // address stopped carrying one still loads, which is the whole point of keeping the alias.
        // The page envelope holds the digest that verifies the bytes.
    }

    #[test]
    fn an_address_costs_far_less_than_its_field_names_used_to() {
        // `page_segment_id` is fifteen characters spent to label an integer, written once per
        // index item forever.
        let address = BlockAddress::from_parts(
            3,
            128,
            126,
            Some(9),
            Some(122110326161599232),
            Some(545210715),
            Some(2),
        );
        let encoded = serde_json::to_string(&address).unwrap();
        // Not vacuous: the values must still be there before the size claim means anything.
        assert!(encoded.contains("122110326161599232"));
        assert!(encoded.contains("545210715"));
        for gone in ["page_segment_id", "routing_slot", "object_id", "generation"] {
            assert!(!encoded.contains(gone), "{gone} should not be written any more");
        }
        // Ninety-one bytes, where the long-name form was more than twice that. It was 90 until
        // an absent field stopped vanishing: a row is read by position, so a field that
        // disappears when it is empty moves every field behind it -- which shifted `generation`
        // into `object_id` until a round-trip test caught it. The cost of that safety, here, is
        // one `"h":null`; in the packed form an absent field is a single byte.
        assert!(
            encoded.len() < 100,
            "expected a compact address, got {} bytes: {encoded}",
            encoded.len()
        );
        // And it round-trips through its own new form.
        let back: BlockAddress = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back, address);
    }
    use super::*;

    #[test]
    fn default_store_scratch_dir_dies_with_the_last_clone() {
        let store = BlockStore::default();
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
        let store = BlockStore::new(dir.path());
        drop(store);
        assert!(dir.path().exists(), "a caller-supplied root must never be deleted");
    }

    #[test]
    fn active_slab_torn_tail_is_fenced_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let a1 = store.append(b"record-one").unwrap();
        let a2 = store.append(b"record-two").unwrap();
        drop(store);
        // Simulate a crash that left a partial/torn record (no valid envelope) on the ACTIVE
        // slab past the last committed record.
        let slab = slab_path(dir.path(), a2.block_slab_id);
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
        let reopened = BlockStore::new(dir.path());
        assert_eq!(
            std::fs::metadata(&slab).unwrap().len(),
            clean_len,
            "torn active-slab tail must be truncated to the readable prefix on reopen"
        );
        // Committed records survive and remain readable.
        assert_eq!(reopened.read(&a1).unwrap(), b"record-one");
        assert_eq!(reopened.read(&a2).unwrap(), b"record-two");
        // A new append lands right after the fenced prefix rather than on top of a committed
        // record. Checked by OFFSET: a block id is an index inside its object now, so three
        // blocks of three different objects all being block 0 is expected and says nothing
        // about where they landed.
        let a3 = reopened.append(b"record-three").unwrap();
        assert_eq!(a3.offset, clean_len, "a new append starts at the fenced prefix");
        assert_ne!(a3.offset, a1.offset);
        assert_ne!(a3.offset, a2.offset);
        assert_eq!(reopened.read(&a3).unwrap(), b"record-three");
    }

    #[test]
    fn a_gc_round_that_reclaimed_nothing_does_not_rewrite_the_manifest() {
        // The sibling test above keeps the full manifest re-serialize off the APPEND path. The
        // page-GC path had no such guard and rewrote it unconditionally, once per round, on a
        // stage the periodic loop runs whenever page pressure holds.
        //
        // It is not a cheap write: it serialises every slab, fsyncs the temp file, renames it and
        // fsyncs the parent directory -- two fsyncs. A round that reclaimed nothing rewrites it
        // with byte-identical content, so unlike the append test the BYTES cannot tell the two
        // apart and the modification time is the observable.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        for index in 0..8u64 {
            store.append(format!("record-{index}").as_bytes()).unwrap();
        }
        store.sync_durable().unwrap();
        let manifest = slab_manifest_path(dir.path());
        assert!(manifest.exists(), "expected the manifest to exist after a durable sync");
        let before = std::fs::metadata(&manifest).unwrap().modified().unwrap();

        // `retain_from` 0 leaves every slab above the floor, so this round walks and reclaims
        // nothing.
        let report = store.gc_slabs_before(0).unwrap();
        assert!(
            report.removed_block_slab_ids.is_empty(),
            "the fixture was supposed to reclaim nothing, but removed {:?}",
            report.removed_block_slab_ids
        );
        // The denominator: a round that walked no slabs at all would satisfy the assertion below
        // for the wrong reason.
        assert!(
            !report.retained_block_slab_ids.is_empty(),
            "the round walked no slabs, so it proves nothing about skipping the write"
        );

        let after = std::fs::metadata(&manifest).unwrap().modified().unwrap();
        assert_eq!(
            before, after,
            "a page-GC round that reclaimed nothing rewrote the slab manifest"
        );
    }

    #[test]
    fn per_append_does_not_reserialize_the_slab_manifest_on_the_default_path() {
        // MANIFEST-CONFORMANCE FOLD no-O(n) proof: on the default single-barrier path the per-append
        // slab-manifest full re-serialize (the measured O(n) aging driver -- ~961 B rewritten per
        // write, growing with the slab count) is OFF the write path. Appending many records must
        // NOT rewrite `page_extent_manifest.json` each time; the catalog is deferred and made
        // durable in one shot at sync_durable()/seal. Proven by the manifest file bytes staying
        // byte-identical across a burst of appends, then changing exactly once at sync_durable.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let manifest = slab_manifest_path(dir.path());
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
            "the slab manifest must NOT be re-serialized per append on the default path"
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
    fn slab_catalog_folds_slabs_and_install_reconstructs_lifecycle() {
        // MANIFEST-CONFORMANCE FOLD round-trip at the block-store layer: project the slab catalog into
        // the durable SlabCatalogEntry subset, then reconstruct the slab lifecycle from that projection
        // with the slab-manifest file deleted -- proving the folded catalog is a lossless source
        // of the durable slab state (diagnostics are recomputed from the slab separately).
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.append(b"a").unwrap();
        // Seal the first slab by rolling to a new active slab.
        store.roll_slab().unwrap();
        store.append(b"b").unwrap();
        store.sync_durable().unwrap();
        let catalog = store.slab_catalog(7);
        // Two slabs: the sealed first slab and the active second slab.
        assert_eq!(catalog.len(), 2);
        assert!(catalog.iter().any(|z| z.state == crate::index_log::SlabCatalogState::Sealed));
        assert!(catalog.iter().any(|z| z.state == crate::index_log::SlabCatalogState::Active));
        assert!(catalog.iter().all(|z| z.version == 7));
        // Delete the slab-manifest file so the reopened store has no cached catalog file; it
        // reconstructs slabs from the durable slabs (reconcile-on-open), then we install the
        // folded catalog on top. The lifecycle states must match the pre-crash projection.
        let reopened = BlockStore::new(dir.path());
        std::fs::remove_file(slab_manifest_path(dir.path())).ok();
        let changed = reopened.install_slab_catalog(&catalog).unwrap();
        let recovered = reopened.slab_catalog(0);
        let state_of = |slab: u64, zs: &[crate::index_log::SlabCatalogEntry]| {
            zs.iter().find(|z| z.block_slab_id == slab).map(|z| z.state)
        };
        for entry in &catalog {
            assert_eq!(
                state_of(entry.block_slab_id, &recovered),
                Some(entry.state),
                "slab {} lifecycle must reconstruct from the folded catalog",
                entry.block_slab_id
            );
        }
        let _ = changed;
    }

    #[test]
    fn gc_slabs_removes_old_non_current_slabs() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"old").unwrap();
        store.install_slab(2, b"keep").unwrap();

        let report = store.gc_slabs_before(2).unwrap();
        assert_eq!(report.removed_block_slab_ids, vec![0, 1]);
        assert_eq!(report.retained_block_slab_ids, vec![2]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"old".len()) as u64
        );
        assert_eq!(report.retained_physical_bytes, b"keep".len() as u64);
        assert!(report.retained_current_block_slab_ids.is_empty());
        assert!(report.retained_live_block_slab_ids.is_empty());
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
        let store = BlockStore::new(dir.path());

        let payload = vec![b'x'; 512];
        let mut guard = 0;
        let mut full_slab = 0;
        while !store.needs_slab_preparation_with_target(TARGET) {
            full_slab = store.append(&payload).unwrap().block_slab_id;
            guard += 1;
            assert!(guard < 200, "filled {guard} times without reaching the target");
        }

        let rolled = store
            .prepare_next_slab_with_target(TARGET)
            .unwrap()
            .expect("a slab at target must roll");
        assert_eq!(rolled.previous_block_slab_id, full_slab);
        assert!(rolled.new_block_slab_id > full_slab);

        // The next append lands on the fresh slab, and did not have to roll to get there.
        let after = store.append(&payload).unwrap();
        assert_eq!(after.block_slab_id, rolled.new_block_slab_id);
    }

    /// Prepare must be a no-op while the slab has room, or a background cycle would shred the
    /// store into one slab per pass.
    #[test]
    fn prepare_next_slab_is_a_noop_below_target() {
        const TARGET: u64 = 1 << 20;
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
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
        let store = BlockStore::new(dir.path());
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
        let store = BlockStore::new(dir.path());
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
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        assert_eq!(first.block_slab_id, 0);

        let roll = store.roll_slab().unwrap();
        assert_eq!(roll.previous_block_slab_id, 0);
        assert_eq!(roll.new_block_slab_id, 1);
        let second = store.append(b"second").unwrap();
        assert_eq!(second.block_slab_id, 1);
        assert_eq!(second.offset, 0);
        assert_eq!(store.read(&first).unwrap(), b"first");
        assert_eq!(store.read(&second).unwrap(), b"second");
    }

    #[test]
    fn reopened_store_appends_to_latest_existing_slab() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        let roll = store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        assert_eq!(roll.new_block_slab_id, second.block_slab_id);

        let reopened = BlockStore::new(dir.path());
        let third = reopened.append(b"third").unwrap();

        assert_eq!(third.block_slab_id, second.block_slab_id);
        assert!(third.offset > second.offset);
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
        assert_eq!(reopened.read(&third).unwrap(), b"third");
    }

    // shared-corpus: storage_stream_manifest_disk_reconciliation;
    #[test]
    fn reopen_reconciles_manifest_missing_existing_stream_slab() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        drop(store);

        let manifest_path = slab_manifest_path(dir.path());
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest["bands"] = serde_json::json!([manifest["bands"]
            .as_array()
            .unwrap()
            .iter()
            .find(|slab| slab["page_segment_id"] == serde_json::json!(first.block_slab_id))
            .unwrap()
            .clone()]);
        fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();

        let reopened = BlockStore::new(dir.path());
        let descriptors = reopened.slab_descriptors();

        assert!(descriptors
            .iter()
            .any(|slab| slab.block_slab_id == first.block_slab_id
                && slab.state == BlockStoreSlabState::Sealed));
        assert!(descriptors
            .iter()
            .any(|slab| slab.block_slab_id == second.block_slab_id
                && slab.state == BlockStoreSlabState::Active));
        let report = reopened.stream_backed_slab_runtime_report().unwrap();
        assert!(report.slab_manifest_reconciled_on_open);
        assert!(report.slab_manifest_disk_consistent);
        assert_eq!(report.manifest_extra_stream_slabs, 0);
        assert_eq!(report.manifest_missing_stream_slabs, 0);
        assert_eq!(reopened.read(&first).unwrap(), b"first");
        assert_eq!(reopened.read(&second).unwrap(), b"second");
    }

    // shared-corpus: storage_stream_manifest_disk_reconciliation;
    #[test]
    fn reopen_marks_manifest_slab_without_stream_file_as_purged() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second").unwrap();
        drop(store);

        fs::remove_file(slab_path(dir.path(), first.block_slab_id)).unwrap();

        let reopened = BlockStore::new(dir.path());
        let descriptors = reopened.slab_descriptors();

        assert!(descriptors
            .iter()
            .any(|slab| slab.block_slab_id == first.block_slab_id
                && slab.state == BlockStoreSlabState::Purged));
        assert!(descriptors
            .iter()
            .any(|slab| slab.block_slab_id == second.block_slab_id
                && slab.state == BlockStoreSlabState::Active));
        let report = reopened.stream_backed_slab_runtime_report().unwrap();
        assert!(report.slab_manifest_reconciled_on_open);
        assert!(report.slab_manifest_disk_consistent);
        assert_eq!(report.manifest_extra_stream_slabs, 0);
        assert_eq!(report.manifest_missing_stream_slabs, 0);
        assert_eq!(reopened.read(&second).unwrap(), b"second");
    }

    #[test]
    fn installed_higher_slab_becomes_current_for_future_appends() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(3, b"restored-segment").unwrap();

        let next = store.append(b"after-restore").unwrap();

        assert_eq!(next.block_slab_id, 3);
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
        assert_eq!(from_compact_slab.block_slab_id, next.block_slab_id);
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
        let store = BlockStore::new(dir.path());
        let payload = b"digest-lives-with-the-page";
        let address = store.append(payload).unwrap();

        let path = slab_path(dir.path(), address.block_slab_id);
        let slab = fs::read(&path).unwrap();
        let start = address.offset as usize;
        let record = &slab[start..start + address.length as usize];
        let at = record::BLOCK_RECORD_CHECKSUM_OFFSET;
        let field = &record[at..at + record::BLOCK_RECORD_CHECKSUM_LEN];

        assert_ne!(
            hex::encode(field),
            record::sha256_hex(payload),
            "if this ever matches, the envelope holds a full digest and the index copy could              genuinely be relocated rather than dropped"
        );
        assert_eq!(
            field,
            crate::checksum::crc32c(payload).to_le_bytes(),
            "the field is the crc32c of the payload"
        );
        assert_eq!(
            record::BLOCK_RECORD_CHECKSUM_LEN,
            4,
            "the field is the checksum and nothing else: no padding, no marker"
        );
        // Six u64, one u32 and the presence byte. It was 64 while the grouping id was a seventh
        // field; it is the slab's own id, so it is read off `block_slab_id` instead of stored.
        assert_eq!(std::mem::size_of::<BlockAddress>(), 56);
    }

    #[test]
    fn block_address_checksum_rejects_corrupt_slab_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store.append(b"verified-page").unwrap();
        // The address no longer carries a digest. The page does, and a read still verifies
        // against it -- corrupting the slab below must still be caught.
        assert_eq!(store.read(&address).unwrap(), b"verified-page");

        let path = slab_path(dir.path(), address.block_slab_id);
        let mut slab = fs::read(&path).unwrap();
        *slab.last_mut().unwrap() ^= 0xff;
        fs::write(path, slab).unwrap();
        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::ChecksumMismatch { .. }));
    }

    // shared-corpus: storage_object_page_bucket_parity_surfaces;
    #[test]
    fn block_address_matches_compact_slab_metadata_contract_and_checksum_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store
            .append_with_block_metadata(b"address-contract", Some(4242), Some(17))
            .unwrap();

        assert_eq!(address.block_slab_id, 0);
        assert_eq!(address.offset, 0);
        assert!(address.length > b"address-contract".len() as u64);
        assert_eq!(address.block_id(), Some(0));
        assert_eq!(address.object_id(), Some(4242));
        assert_eq!(address.routing_bucket(), Some(17));
        assert_eq!(address.slab_id(), Some(0));
        assert_eq!(address.compact_slab_id(), Some(0));
        assert_eq!(address.compact_slab_offset(), Some(0));
        assert_eq!(address.compact_slab_address(), Some(0));
        let from_compact_slab = BlockAddress::from_compact_slab_address(
            address.compact_slab_address().unwrap(),
            address.length,
        );
        assert_eq!(
            from_compact_slab.block_slab_id,
            address.block_slab_id
        );
        assert_eq!(from_compact_slab.offset, address.offset);
        assert_eq!(from_compact_slab.length, address.length);
        assert_eq!(store.read(&address).unwrap(), b"address-contract");

        let legacy_alias_json = serde_json::json!({
            "page_segment_id": address.block_slab_id,
            "offset": address.offset,
            "length": address.length,
            "page_id": address.block_id(),
            "object_id": address.object_id(),
            "routing_slot": address.routing_bucket(),
            // generation is a canonical, always-present field on write (append sets
            // Some(page_id)) and on read (record decode derives it), so the legacy
            // alias JSON must carry it or the round-trip deserializes to None.
            "generation": address.generation(),
            "band_id": address.slab_id(),
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
    fn block_slab_records_have_self_describing_envelope() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store.append(b"enveloped-page").unwrap();
        let raw = store.read_slab(address.block_slab_id).unwrap();

        assert_eq!(address.block_id(), Some(0));
        assert_eq!(store.read(&address).unwrap(), b"enveloped-page");
    }

    #[test]
    fn block_id_mismatch_rejects_corrupt_address_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let mut address = store.append(b"identity-checked-page").unwrap();
        address.set_block_id(Some(address.block_id().unwrap() + 1));

        let err = store.read(&address).unwrap_err();
        assert!(matches!(err, BlockStoreError::CorruptBlockEnvelope { .. }));
    }

    #[test]
    fn a_slab_descriptor_carries_the_same_number_twice() {
        // A descriptor's `stored_slab_id` and its `block_slab_id` are ONE value under two names.
        // Every construction site says so: each one now writes the slab id itself into both,
        // which is what removing the identity helper made visible.
        //
        // `rolled_slabs_stamp_new_slab_ids` below pins that for an ADDRESS. This pins it for the
        // DESCRIPTOR, which is the struct that actually stores both, and where a caller picks
        // whichever name is nearer without it mattering today.
        //
        // Removing either field is a wire change -- both serialize and the compat corpora carry
        // them -- so the duplication stays. What must not happen is the two diverging quietly.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        store.append(b"second").unwrap();

        let descriptors = store.slab_descriptors();
        assert!(
            descriptors.len() >= 2,
            "the roll should give at least two descriptors, got {}",
            descriptors.len()
        );
        for descriptor in &descriptors {
            assert_eq!(
                descriptor.stored_slab_id, descriptor.block_slab_id,
                "one unit, one id: stored_slab_id and block_slab_id must never diverge"
            );
        }
    }

    /// The stored id IS the slab id -- but across DESERIALIZATION that is trusted, not enforced,
    /// and the route where it is trusted hardest is the one an open deliberately does not look at.
    ///
    /// `a_slab_descriptor_carries_the_same_number_twice` pins the invariant on the paths that
    /// COMPUTE it: every one of them writes the slab id itself, so a descriptor this process
    /// builds cannot diverge. The manifest is the hole. `stored_slab_id` serializes, under the
    /// older `band_id` key, and `load_slab_manifest_at` keeps whatever number the file carried.
    ///
    /// Consumers then read that stored number instead of recomputing it: `gc_utility_candidates`
    /// groups slabs by it in two places, and `compute_slab_usage` keys its per-slab usage rows by
    /// it. Two slabs carrying one id are summed together there, so each one's GC utility is
    /// scored against the other one's bytes.
    ///
    /// THE ROUTE THIS GUARD EXISTS FOR IS THE SKIP. Re-inspecting a sealed slab whose size and
    /// mtime still match what it was verified against is the bulk of a cold open, so by default
    /// (`TS_REVERIFY_ALL_SLABS` unset) it is not done, and the descriptor is carried over from
    /// the manifest untouched. An earlier shape of this test opened the store TWICE and asserted
    /// the property. That reads as a guard and is not one: the stamp that arms the skip is
    /// written by the second open and only persisted afterwards, so that test skipped ZERO slabs
    /// and proved nothing about the route its own comment named. It takes THREE opens, and the
    /// skipped count is asserted non-zero HERE, as a denominator, before any property is.
    ///
    /// No manifest is believed to carry a divergent id today -- the historical grouping value was
    /// the identity too, because the grouping size and the slab size were never configured
    /// differently. This pins the consequence rather than the belief, and it is the guard the
    /// consolidation of the two names is judged against: the code now says slab everywhere, the
    /// manifest still says band, and this is where those two meet.
    #[test]
    fn a_stored_slab_id_that_disagrees_with_its_descriptor_is_normalised_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        store.append(b"second-is-a-little-longer").unwrap();
        store.roll_slab().unwrap();
        store.append(b"third").unwrap();
        drop(store);

        // OPEN TWO. The one that inspects the sealed slabs, stamps
        // `verified_source_mtime_unix_ms` onto their descriptors and writes the manifest out.
        // It skips nothing -- which is exactly why a two-open test exercises nothing.
        let warm = BlockStore::new(dir.path());
        assert_eq!(
            warm.slabs_skipped_reinspection_on_open(),
            0,
            "the second open cannot skip anything yet: it is the one doing the stamping"
        );
        drop(warm);

        let manifest_path = slab_manifest_path(dir.path());
        let raw = fs::read(&manifest_path).unwrap();
        let mut manifest: serde_json::Value = serde_json::from_slice(&raw).unwrap();

        // DENOMINATOR: the manifest really has descriptors, and they really agree to begin with.
        // The KEYS read here are the on-disk spelling and are deliberately not the Rust one:
        // "bands" and "band_id" are the format, `slabs` and `stored_slab_id` are the code.
        let entries = manifest["bands"].as_array_mut().expect("bands array");
        assert!(
            entries.len() >= 3,
            "the manifest must really carry descriptors: {entries:?}"
        );
        for entry in entries.iter() {
            assert_eq!(
                entry["band_id"].as_u64(),
                entry["page_segment_id"].as_u64(),
                "they agree before the edit"
            );
        }

        // Make EVERY descriptor disagree, exactly as a grouping id would have. The active slab is
        // always inspected and would be rewritten whatever this file said; the two sealed ones
        // are the point.
        for entry in entries.iter_mut() {
            entry["band_id"] = serde_json::json!(999_u64);
        }
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        // OPEN THREE. Editing the manifest does not touch the slab files, so every sealed slab
        // still matches what its descriptor was verified against and takes the skip.
        let reopened = BlockStore::new(dir.path());

        // THE DENOMINATOR FOR THE ROUTE, asserted BEFORE the property. Without it, a change that
        // quietly stopped skipping -- or a default that went back to re-verifying everything --
        // would leave everything below passing while covering nothing at all.
        let skipped = reopened.slabs_skipped_reinspection_on_open();
        assert!(
            skipped > 0,
            "this guard must actually exercise the skip route, and it skipped {skipped} slabs"
        );

        let descriptors = reopened.slab_descriptors();
        assert!(
            descriptors.len() >= 3,
            "the reopened store must really have loaded the descriptors: {descriptors:?}"
        );
        let divergent = descriptors
            .iter()
            .filter(|descriptor| descriptor.stored_slab_id != descriptor.block_slab_id)
            .collect::<Vec<_>>();

        // THE ANSWER: the open NORMALISES it. `reconcile_slab_manifest_with_disk` ends with a
        // sweep over EVERY descriptor, not merely the ones it re-read, so a manifest cannot
        // smuggle in a grouping the rest of the code would then honour.
        assert!(
            divergent.is_empty(),
            "a divergent stored band id must not survive the load, and {skipped} slabs took the \
             skip route on this open: {divergent:?}"
        );

        // AND THE CONSEQUENCE, at the consumer that groups by the stored value. Each slab is its
        // own group, so each candidate's group bytes are its own bytes and no one else's. Under a
        // shared id the two collectable slabs report each other's bytes as their group total and
        // their GC utility is scored against the wrong denominator.
        let candidates = reopened
            .gc_utility_candidates(2, Vec::<u64>::new())
            .unwrap();
        assert!(
            candidates.len() >= 2,
            "the consequence needs at least two collectable slabs: {candidates:?}"
        );
        for candidate in &candidates {
            assert_eq!(
                candidate.total_bytes, candidate.bytes,
                "each slab is its own group, so its group bytes are its own: {candidate:?}"
            );
        }
    }

    /// The CODE says slab. The FILE still says band, and that is the point.
    ///
    /// `slab_manifest.json` is read by binaries that are already deployed, and the descriptor's
    /// `band_id` carries no `#[serde(default)]` -- so a manifest written under renamed keys is
    /// not "an older shape with a missing field", it is unreadable. There is no alias mechanism
    /// on the WRITE side to soften that: serde writes exactly one name per field.
    ///
    /// Renaming a Rust field whose wire name was IMPLICIT is the way that happens by accident.
    /// It compiles, every type-level test still passes, and the only thing that changed is the
    /// bytes on disk. This asserts the bytes.
    #[test]
    fn a_folded_manifest_still_writes_the_keys_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.append(b"first").unwrap();
        store.roll_slab().unwrap();
        store.append(b"second").unwrap();
        store.sync_durable().unwrap();
        drop(store);

        let raw = fs::read(slab_manifest_path(dir.path())).expect("the manifest must be on disk");
        let manifest: serde_json::Value = serde_json::from_slice(&raw).expect("valid json");

        // DENOMINATOR FIRST: read the array under the key it must be written at, and prove it is
        // not empty. Without this every per-entry assertion below is vacuously true.
        let entries = manifest
            .get("bands")
            .unwrap_or_else(|| panic!("the slab list must be written under \"bands\": {manifest}"))
            .as_array()
            .expect("an array");
        assert!(
            entries.len() >= 2,
            "this guard needs descriptors to look at, and found {}: {manifest}",
            entries.len()
        );
        assert!(
            manifest.get("slabs").is_none(),
            "the Rust field name must NOT reach the file: {manifest}"
        );

        for entry in entries {
            assert!(
                entry.get("band_id").is_some(),
                "every descriptor keeps its on-disk id key: {entry}"
            );
            assert!(
                entry.get("page_segment_id").is_some(),
                "and the slab-id key it already had: {entry}"
            );
            assert!(
                entry.get("stored_slab_id").is_none(),
                "the Rust field name must NOT reach the file: {entry}"
            );
        }

        // And the file this process wrote is one this process can read: the keys above are not
        // merely present, they are the ones the deserializer binds.
        let reopened = BlockStore::new(dir.path());
        let descriptors = reopened.slab_descriptors();
        assert_eq!(
            descriptors.len(),
            entries.len(),
            "every descriptor on disk must come back: {descriptors:?}"
        );
    }

    #[test]
    fn rolled_slabs_stamp_new_slab_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first-slab").unwrap();
        let roll = store.roll_slab().unwrap();
        let second = store.append(b"second-slab").unwrap();

        assert_eq!(first.slab_id(), Some(first.block_slab_id));
        assert_eq!(second.slab_id(), Some(second.block_slab_id));
        assert_eq!(second.slab_id(), Some(roll.new_block_slab_id));
        assert_ne!(first.slab_id(), second.slab_id());
    }

    #[test]
    fn slab_manifest_tracks_roll_reopen_gc_and_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first-slab").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second-slab").unwrap();

        let slabs = store.slab_descriptors();
        assert_eq!(slabs.len(), 2);
        assert_eq!(slabs[0].block_slab_id, first.block_slab_id);
        assert_eq!(slabs[0].state, BlockStoreSlabState::Sealed);
        assert_eq!(slabs[0].first_block_id, first.block_id());
        assert_eq!(slabs[0].last_block_id, first.block_id());
        assert!(slabs[0].created_unix_ms.is_some());
        assert!(slabs[0].updated_unix_ms.is_some());
        assert_eq!(slabs[1].block_slab_id, second.block_slab_id);
        assert_eq!(slabs[1].state, BlockStoreSlabState::Active);
        assert_eq!(slabs[1].first_block_id, second.block_id());
        assert_eq!(slabs[1].last_block_id, second.block_id());
        assert!(slabs[1].created_unix_ms.is_some());
        assert!(slabs[1].updated_unix_ms.is_some());
        assert!(slab_manifest_path(dir.path()).exists());
        let initial_summary = store.slab_summary();
        assert_eq!(initial_summary.sealed_slabs, 1);
        assert_eq!(initial_summary.active_slabs, 1);
        assert_eq!(initial_summary.delayed_destroy_slabs, 0);
        assert_eq!(initial_summary.purged_slabs, 0);
        assert_eq!(
            initial_summary.sealed_physical_bytes,
            slabs[0].physical_bytes
        );
        assert_eq!(
            initial_summary.active_physical_bytes,
            slabs[1].physical_bytes
        );
        assert_eq!(
            initial_summary.live_physical_bytes,
            slabs[0].physical_bytes + slabs[1].physical_bytes
        );
        assert_eq!(initial_summary.reclaimable_physical_bytes, 0);
        assert!(initial_summary.oldest_known_slab_unix_ms.is_some());
        assert!(initial_summary.oldest_known_slab_age_ms.is_some());
        assert!(initial_summary.oldest_live_slab_unix_ms.is_some());
        assert!(initial_summary.oldest_live_slab_age_ms.is_some());
        assert!(initial_summary.oldest_reclaimable_slab_unix_ms.is_none());
        assert!(initial_summary.oldest_reclaimable_slab_age_ms.is_none());
        let initial_slab_usage = store.slab_usage();
        assert_eq!(initial_slab_usage.len(), 2);
        assert_eq!(initial_slab_usage[0].stored_slab_id, slabs[0].stored_slab_id);
        assert_eq!(
            initial_slab_usage[0].block_slab_id,
            slabs[0].block_slab_id
        );
        assert_eq!(
            initial_slab_usage[0].block_store_used_bytes,
            slabs[0].physical_bytes
        );
        assert_eq!(
            initial_slab_usage[0].live_block_store_used_bytes,
            slabs[0].physical_bytes
        );
        assert_eq!(initial_slab_usage[0].reclaimable_block_store_used_bytes, 0);
        assert_eq!(initial_slab_usage[0].purged_block_store_used_bytes, 0);

        let reopened = BlockStore::new(dir.path());
        let reopened_slabs = reopened.slab_descriptors();
        assert_eq!(reopened_slabs.len(), slabs.len());
        // The slab must survive a reopen unchanged. `verified_source_mtime_unix_ms` is excluded
        // because it is not part of the slab: it records when the descriptor was last checked
        // against the file, and the two sides differ on that for a good reason, asserted below.
        let strip = |slab: &BlockStoreSlabDescriptor| {
            let mut slab = slab.clone();
            slab.verified_source_mtime_unix_ms = None;
            slab
        };
        assert_eq!(strip(&reopened_slabs[0]), strip(&slabs[0]));
        // The original store wrote and sealed this slab without ever inspecting it, so it has
        // nothing verified to record; the reopen reconciled, which inspected it, so it does.
        assert!(
            slabs[0].verified_source_mtime_unix_ms.is_none(),
            "a slab this process only wrote has not been verified by inspection: {:?}",
            slabs[0]
        );
        assert!(
            reopened_slabs[0].verified_source_mtime_unix_ms.is_some(),
            "reopening inspected this slab, so the identity it verified should be recorded: {:?}",
            reopened_slabs[0]
        );
        assert_eq!(
            reopened_slabs[1].block_slab_id,
            slabs[1].block_slab_id
        );
        assert_eq!(reopened_slabs[1].state, slabs[1].state);
        assert_eq!(
            reopened_slabs[1].physical_bytes,
            slabs[1].physical_bytes
        );
        assert_eq!(reopened_slabs[1].logical_bytes, slabs[1].logical_bytes);
        assert_eq!(
            reopened_slabs[1].created_unix_ms,
            slabs[1].created_unix_ms
        );
        assert!(reopened_slabs[1].updated_unix_ms >= slabs[1].updated_unix_ms);

        let report = reopened
            .gc_slabs_before_with_live_refs_delayed_destroy(1, std::iter::empty())
            .unwrap();
        assert_eq!(report.delayed_destroy_block_slab_ids, vec![0]);
        let delayed = reopened.slab_descriptors();
        assert_eq!(delayed[0].state, BlockStoreSlabState::DelayedDestroy);
        assert!(delayed[0].physical_bytes > 0);
        assert_eq!(delayed[0].created_unix_ms, slabs[0].created_unix_ms);
        assert!(delayed[0].updated_unix_ms >= slabs[0].updated_unix_ms);
        assert_eq!(delayed[1].state, BlockStoreSlabState::Active);
        let delayed_summary = reopened.slab_summary();
        assert_eq!(delayed_summary.delayed_destroy_slabs, 1);
        assert_eq!(delayed_summary.active_slabs, 1);
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
        assert!(delayed_summary.oldest_known_slab_unix_ms.is_some());
        assert!(delayed_summary.oldest_live_slab_unix_ms.is_some());
        assert_eq!(
            delayed_summary.oldest_reclaimable_slab_unix_ms,
            delayed[0].updated_unix_ms
        );
        assert!(delayed_summary.oldest_reclaimable_slab_age_ms.is_some());
        let delayed_slab_usage = reopened.slab_usage();
        let delayed_first = delayed_slab_usage
            .iter()
            .find(|slab| slab.block_slab_id == first.block_slab_id)
            .unwrap();
        assert_eq!(
            delayed_first.reclaimable_block_store_used_bytes,
            delayed[0].physical_bytes
        );
        assert_eq!(delayed_first.live_block_store_used_bytes, 0);

        let purge = reopened
            .purge_delayed_destroy_slabs_older_than(0)
            .unwrap();
        assert_eq!(purge.purged_block_slab_ids, vec![0]);
        assert!(purge.purged_physical_bytes > 0);
        let purged = BlockStore::new(dir.path()).slab_descriptors();
        assert_eq!(purged[0].state, BlockStoreSlabState::Purged);
        assert_eq!(purged[0].created_unix_ms, slabs[0].created_unix_ms);
        assert!(purged[0].updated_unix_ms >= delayed[0].updated_unix_ms);
        assert_eq!(purged[1].state, BlockStoreSlabState::Active);
        let purged_summary = BlockStore::new(dir.path()).slab_summary();
        assert_eq!(purged_summary.purged_slabs, 1);
        assert_eq!(purged_summary.active_slabs, 1);
        assert_eq!(
            purged_summary.purged_physical_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_summary.live_physical_bytes, purged[1].physical_bytes);
        assert_eq!(purged_summary.reclaimable_physical_bytes, 0);
        let purged_slab_usage = BlockStore::new(dir.path()).slab_usage();
        let purged_first = purged_slab_usage
            .iter()
            .find(|slab| slab.block_slab_id == first.block_slab_id)
            .unwrap();
        assert_eq!(
            purged_first.purged_block_store_used_bytes,
            purged[0].physical_bytes
        );
        assert_eq!(purged_first.reclaimable_block_store_used_bytes, 0);
        assert!(purged_summary.oldest_known_slab_unix_ms.is_some());
        assert!(purged_summary.oldest_live_slab_unix_ms.is_some());
        assert!(purged_summary.oldest_reclaimable_slab_unix_ms.is_none());
        assert!(purged_summary.oldest_reclaimable_slab_age_ms.is_none());
    }

    #[test]
    fn missing_slab_manifest_rebuilds_from_existing_slabs() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"first-slab").unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"second-slab").unwrap();
        fs::remove_file(slab_manifest_path(dir.path())).unwrap();

        let rebuilt = BlockStore::new(dir.path());
        let slabs = rebuilt.slab_descriptors();

        assert_eq!(slabs.len(), 2);
        assert_eq!(slabs[0].block_slab_id, first.block_slab_id);
        assert_eq!(slabs[0].state, BlockStoreSlabState::Sealed);
        assert_eq!(slabs[0].first_block_id, first.block_id());
        assert_eq!(slabs[0].last_block_id, first.block_id());
        assert!(slabs[0].created_unix_ms.is_some());
        assert!(slabs[0].updated_unix_ms.is_some());
        assert_eq!(slabs[1].block_slab_id, second.block_slab_id);
        assert_eq!(slabs[1].state, BlockStoreSlabState::Active);
        assert_eq!(slabs[1].first_block_id, second.block_id());
        assert_eq!(slabs[1].last_block_id, second.block_id());
        assert!(slabs[1].created_unix_ms.is_some());
        assert!(slabs[1].updated_unix_ms.is_some());
        assert!(slab_manifest_path(dir.path()).exists());

        let report = rebuilt.stream_backed_slab_runtime_report().unwrap();
        assert!(report.runtime_ready, "{report:?}");
        assert_eq!(report.slab_lifecycle_states, vec!["active", "sealed"]);
        assert!(report.slab_manifest_ready);
        assert!(report.slab_manifest_rebuild_ready);
        assert!(report.slab_stats_ready);
        assert_eq!(report.slab_usage.len(), 2);
        assert_eq!(
            report
                .slab_usage
                .iter()
                .map(|slab| slab.block_store_used_bytes)
                .sum::<u64>(),
            report.physical_bytes
        );
        assert!(!report.slab_manifest_reconciled_on_open);
        assert!(report.slab_manifest_disk_consistent);
        assert_eq!(report.manifest_missing_stream_slabs, 0);
        assert_eq!(report.manifest_extra_stream_slabs, 0);
        assert_eq!(report.corrupt_slab_count, 0);
        assert_eq!(report.partial_slab_count, 0);
        assert!(report.partial_slab_recovery_ready);
        assert_eq!(report.readable_prefix_physical_bytes, report.physical_bytes);
    }

    // shared-corpus: storage_stream_partial_band_rebuild;
    #[test]
    fn partial_slab_manifest_rebuild_preserves_readable_prefix_and_reports_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first_payload = b"sealed-readable-prefix".repeat(64);
        let first = store.append(&first_payload).unwrap();
        store.roll_slab().unwrap();
        let second = store.append(b"active-clean-tail").unwrap();

        let first_slab = slab_path(dir.path(), first.block_slab_id);
        let readable_prefix = fs::metadata(&first_slab).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&first_slab)
            .unwrap()
            .write_all(b"partial-corrupt-tail")
            .unwrap();
        fs::remove_file(slab_manifest_path(dir.path())).unwrap();

        let rebuilt = BlockStore::new(dir.path());
        assert_eq!(rebuilt.read(&first).unwrap(), first_payload);
        assert_eq!(rebuilt.read(&second).unwrap(), b"active-clean-tail");

        let slabs = rebuilt.slab_descriptors();
        let sealed = slabs
            .iter()
            .find(|slab| slab.block_slab_id == first.block_slab_id)
            .unwrap();
        assert_eq!(sealed.state, BlockStoreSlabState::Sealed);
        assert!(sealed.has_corruption);
        assert_eq!(sealed.first_error_offset, Some(readable_prefix));
        assert_eq!(sealed.readable_prefix_physical_bytes, readable_prefix);
        assert_eq!(sealed.first_block_id, first.block_id());
        assert_eq!(sealed.last_block_id, first.block_id());
        assert!(sealed
            .first_error
            .as_deref()
            .unwrap_or_default()
            .contains("mixed raw bytes"));
        assert!(slab_manifest_path(dir.path()).exists());

        let report = rebuilt.stream_backed_slab_runtime_report().unwrap();
        assert!(!report.runtime_ready, "{report:?}");
        assert!(report.slab_manifest_ready);
        assert!(report.slab_manifest_rebuild_ready);
        assert!(!report.slab_manifest_reconciled_on_open);
        assert!(report.slab_manifest_disk_consistent);
        assert_eq!(report.manifest_missing_stream_slabs, 0);
        assert_eq!(report.manifest_extra_stream_slabs, 0);
        assert_eq!(report.slab_lifecycle_states, vec!["active", "sealed"]);
        assert_eq!(report.corrupt_slab_count, 1);
        assert_eq!(report.partial_slab_count, 1);
        assert_eq!(
            report.readable_prefix_physical_bytes,
            readable_prefix + second.length
        );
        assert!(report.partial_slab_recovery_ready);
        assert!(!report.envelope_checksum_ready);
        assert!(report
            .blockers
            .iter()
            .any(|blocker| blocker.contains("readable prefix was preserved")));
    }

    #[test]
    fn logical_block_range_skips_record_envelopes_across_blocks() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.append(b"abc").unwrap();
        store.append(b"def").unwrap();

        assert_eq!(store.read_logical_range(0, 1, 4).unwrap(), b"bcde");
    }

    #[test]
    fn read_range_and_logical_range_drive_shared_slab_read_through() {
        // On-demand lazy recovery must cover slab-report / streaming reads too:
        // read_range and read_logical_range on a metadata-only restored node must pull a
        // not-yet-fetched checkpoint slab from shared storage on first access (previously
        // they hit a local File::open miss instead of the shared read-through).
        #[derive(Debug)]
        struct OneSlabSource {
            block_slab_id: u64,
            bytes: Vec<u8>,
        }
        impl SharedSlabSource for OneSlabSource {
            fn fetch_slab(&self, block_slab_id: u64) -> Result<Option<Vec<u8>>, BlockStoreError> {
                Ok((block_slab_id == self.block_slab_id).then(|| self.bytes.clone()))
            }
        }

        // Producer writes a real slab; capture its raw bytes to serve lazily to fresh nodes.
        let producer_dir = tempfile::tempdir().unwrap();
        let producer = BlockStore::new(producer_dir.path());
        producer.append(b"abc").unwrap();
        producer.append(b"def").unwrap();
        let raw = producer.read_slab(0).unwrap();
        drop(producer);

        // read_range: fresh node, slab absent locally, shared source attached.
        let range_dir = tempfile::tempdir().unwrap();
        let range_node = BlockStore::new(range_dir.path());
        range_node.attach_shared_slab_source(Arc::new(OneSlabSource {
            block_slab_id: 0,
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
        let logical_node = BlockStore::new(logical_dir.path());
        logical_node.attach_shared_slab_source(Arc::new(OneSlabSource {
            block_slab_id: 0,
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

        /// A record written by the streaming encoder must still decode, and vice versa.
    ///
    /// The page-record encoder used to build a zstd compressor per call; it now holds one per
    /// thread, which removed about 80% of what a write allocates. The two APIs frame a stream
    /// differently, so the STORED BYTES changed -- and a stored-byte change is only safe if a
    /// record written by either build reads on either.
    ///
    /// This pins that both ways round, which a round-trip test through one encoder cannot: it
    /// would pass just as happily if the format had shifted under it.
    #[test]
    fn block_records_written_by_either_encoder_decode() {
        let payload: Vec<u8> = (0..4096u32).map(|i| ((i * 7 + (i >> 3)) % 251) as u8).collect();
        let level = 3;

        // What the old encoder produced, byte for byte.
        let streaming = zstd::stream::encode_all(std::io::Cursor::new(&payload[..]), level)
            .expect("streaming encode");
        // What the held compressor produces now.
        let bulk = {
            let mut compressor = zstd::bulk::Compressor::new(level).expect("compressor");
            compressor.compress(&payload).expect("bulk encode")
        };

        assert_ne!(
            streaming, bulk,
            "if these ever match, this test is no longer proving anything and should be removed"
        );

        // The decode path a reader takes, given the exact payload length from the header.
        for (label, blob) in [("streaming", &streaming), ("bulk", &bulk)] {
            let mut decompressor = zstd::bulk::Decompressor::new().expect("decompressor");
            let back = decompressor
                .decompress(blob, payload.len())
                .unwrap_or_else(|error| panic!("{label} record failed the bulk decode: {error}"));
            assert_eq!(back, payload, "{label} record decoded to the wrong bytes");

            // And the streaming fallback the decoder uses above its size ceiling.
            let back = zstd::stream::decode_all(std::io::Cursor::new(&blob[..]))
                .unwrap_or_else(|error| panic!("{label} record failed the streaming decode: {error}"));
            assert_eq!(back, payload, "{label} record decoded to the wrong bytes");
        }
    }

    #[test]
    fn compressed_block_records_round_trip_and_remain_logical() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();
        let raw = store.read_slab(first.block_slab_id).unwrap();

        assert!(first.length < (record::BLOCK_RECORD_HEADER_LEN + first_payload.len()) as u64);
        assert!(second.length < (record::BLOCK_RECORD_HEADER_LEN + second_payload.len()) as u64);
        assert_eq!(store.read(&first).unwrap(), first_payload);
        assert_eq!(store.read(&second).unwrap(), second_payload);

        let logical_offset = first_payload.len() as u64 - 3;
        let logical = store
            .read_logical_range(first.block_slab_id, logical_offset, 12)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 3..]);
        expected.extend_from_slice(&second_payload[..9]);
        assert_eq!(logical, expected);
        assert_eq!(record::block_record_compression_byte(&raw), BLOCK_RECORD_COMPRESSION_ZSTD);

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
    fn stream_backed_slab_runtime_report_covers_roll_read_manifest_and_delayed_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first_payload = b"slab-stream-first-".repeat(96);
        let second_payload = b"slab-stream-second-".repeat(96);
        let first = store
            .append_with_block_metadata(&first_payload, Some(11), Some(7))
            .unwrap();
        let second = store
            .append_with_block_metadata(&second_payload, Some(12), Some(7))
            .unwrap();
        assert_eq!(first.block_slab_id, second.block_slab_id);

        let logical_offset = first_payload.len() as u64 - 8;
        let logical = store
            .read_logical_range(first.block_slab_id, logical_offset, 16)
            .unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(&first_payload[first_payload.len() - 8..]);
        expected.extend_from_slice(&second_payload[..8]);
        assert_eq!(logical, expected);

        let roll = store.roll_slab().unwrap();
        let third_payload = b"slab-stream-third-".repeat(96);
        let third = store
            .append_with_block_metadata(&third_payload, Some(13), Some(8))
            .unwrap();
        assert_eq!(third.block_slab_id, roll.new_block_slab_id);
        let before_gc = store.stream_backed_slab_runtime_report().unwrap();
        assert!(before_gc.runtime_ready, "{before_gc:?}");
        assert_eq!(before_gc.active_slabs, 1);
        assert_eq!(before_gc.sealed_slabs, 1);
        assert_eq!(before_gc.slab_lifecycle_states, vec!["active", "sealed"]);
        assert_eq!(before_gc.stream_record_count, 3);
        assert_eq!(before_gc.first_block_id, first.block_id());
        assert_eq!(before_gc.last_block_id, third.block_id());
        assert!(before_gc.block_id_continuity_ready);
        assert!(before_gc.slab_manifest_rebuild_ready);
        assert!(before_gc.slab_stats_ready);
        assert_eq!(before_gc.slab_usage.len(), 2);
        assert_eq!(
            before_gc
                .slab_usage
                .iter()
                .map(|slab| slab.block_store_used_bytes)
                .sum::<u64>(),
            before_gc.physical_bytes
        );
        assert!(before_gc.logical_stream_bytes_read >= 16);
        assert!(before_gc.slab_state_transition_count >= 2);

        let delayed = store
            .gc_slabs_before_with_live_refs_delayed_destroy(
                roll.new_block_slab_id,
                [roll.new_block_slab_id],
            )
            .unwrap();
        assert_eq!(
            delayed.delayed_destroy_block_slab_ids,
            vec![roll.previous_block_slab_id]
        );

        let reopened = BlockStore::new(dir.path());
        assert_eq!(reopened.read(&third).unwrap(), third_payload);
        let report = reopened.stream_backed_slab_runtime_report().unwrap();
        assert!(report.runtime_ready, "{report:?}");
        assert_eq!(report.active_slabs, 1);
        assert_eq!(report.delayed_destroy_slabs, 1);
        assert_eq!(
            report.slab_lifecycle_states,
            vec!["active", "delayed_destroy"]
        );
        assert!(report.slab_count >= 2);
        assert!(report.stream_slab_count >= 1);
        assert!(report.logical_stream_read_ready);
        assert!(report.append_roll_ready);
        assert!(report.slab_manifest_ready);
        assert!(report.slab_manifest_rebuild_ready);
        assert!(report.slab_stats_ready);
        assert!(report
            .slab_usage
            .iter()
            .any(|slab| slab.state == BlockStoreSlabState::DelayedDestroy
                && slab.reclaimable_block_store_used_bytes > 0));
        assert!(report
            .slab_usage
            .iter()
            .any(|slab| slab.state == BlockStoreSlabState::Active
                && slab.live_block_store_used_bytes > 0));
        assert!(report.envelope_checksum_ready);
        assert!(report.compression_stream_ready);
        assert!(report.delayed_destroy_ready);
        assert!(!report.purge_lifecycle_ready);
        assert!(report.logical_bytes >= third_payload.len() as u64);
        assert_eq!(report.stream_record_count, 1);
        assert_eq!(report.first_block_id, third.block_id());
        assert_eq!(report.last_block_id, third.block_id());
        assert!(report.block_id_continuity_ready);
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
            .purge_delayed_destroy_slabs_older_than(0)
            .unwrap();
        assert_eq!(
            purge.purged_block_slab_ids,
            vec![roll.previous_block_slab_id]
        );
        let purged = BlockStore::new(dir.path())
            .stream_backed_slab_runtime_report()
            .unwrap();
        assert!(purged.runtime_ready, "{purged:?}");
        assert_eq!(purged.active_slabs, 1);
        assert_eq!(purged.delayed_destroy_slabs, 0);
        assert_eq!(purged.purged_slabs, 1);
        assert_eq!(purged.slab_lifecycle_states, vec!["active", "purged"]);
        assert!(purged.slab_stats_ready);
        assert!(purged
            .slab_usage
            .iter()
            .any(|slab| slab.state == BlockStoreSlabState::Purged
                && slab.purged_block_store_used_bytes > 0));
        assert!(purged.purge_lifecycle_ready);
        assert!(purged.append_roll_ready);
        assert!(purged.block_id_continuity_ready);
    }

    #[test]
    fn slab_reports_describe_block_counts_bytes_and_compression() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first_payload = b"prefix-".repeat(80);
        let second_payload = b"suffix-".repeat(80);
        let first = store.append(&first_payload).unwrap();
        let second = store.append(&second_payload).unwrap();

        let reports = store.slab_reports().unwrap();

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].block_slab_id, first.block_slab_id);
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
        assert_eq!(reports[0].first_block_id, first.block_id());
        assert_eq!(reports[0].last_block_id, second.block_id());
        assert_eq!(reports[0].block_index_count, 2);
        assert_eq!(reports[0].block_index_entries.len(), 2);
        assert_eq!(
            reports[0].block_index_entries[0].block_slab_id,
            first.block_slab_id
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
        assert_eq!(reports[0].block_index_entries[0].block_id, first.block_id());
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
        assert_eq!(reports[0].block_index_entries[1].block_id, second.block_id());
        assert_eq!(reports[0].first_error, None);
    }

    #[test]
    fn slab_reports_capture_first_corrupt_record_error() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let first = store.append(b"healthy").unwrap();
        let second = store.append(b"damaged").unwrap();
        let path = slab_path(dir.path(), second.block_slab_id);
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
        assert_eq!(reports[0].first_block_id, first.block_id());
        assert_eq!(reports[0].last_block_id, first.block_id());
        let error = reports[0]
            .first_error
            .as_ref()
            .expect("corrupt second record should be reported");
        assert!(error.contains("checksum") || error.contains("corrupt page envelope"));
    }

    #[test]
    fn block_record_compression_policy_can_disable_or_raise_threshold() {
        let payload = b"policy-controlled-".repeat(80);

        let disabled_dir = tempfile::tempdir().unwrap();
        let disabled_store = BlockStore::with_options(
            disabled_dir.path(),
            BlockStoreOptions {
                compression_enabled: false,
                ..BlockStoreOptions::default()
            },
        );
        let disabled_address = disabled_store.append(&payload).unwrap();
        let disabled_raw = disabled_store
            .read_slab(disabled_address.block_slab_id)
            .unwrap();

        // Stated from the values that went in, because a varint header has no fixed length: it
        // is the fixed part, one varint per number, and the compression codec.
        let expected_header = record::BLOCK_RECORD_HEADER_LEN;
        assert_eq!(
            disabled_address.length,
            (expected_header + payload.len()) as u64
        );
        assert_eq!(
            expected_header,
            record::BLOCK_RECORD_HEADER_LEN,
            "one header size, whatever the values"
        );
        assert_eq!(record::block_record_compression_byte(&disabled_raw), BLOCK_RECORD_COMPRESSION_NONE);
        assert_eq!(disabled_store.read(&disabled_address).unwrap(), payload);
        assert_eq!(disabled_store.stats().compressed_records_written, 0);
        assert_eq!(disabled_store.stats().compression_bytes_saved, 0);

        let threshold_dir = tempfile::tempdir().unwrap();
        let threshold_store = BlockStore::with_options(
            threshold_dir.path(),
            BlockStoreOptions {
                compression_min_bytes: payload.len() + 1,
                ..BlockStoreOptions::default()
            },
        );
        let threshold_address = threshold_store.append(&payload).unwrap();
        let threshold_raw = threshold_store
            .read_slab(threshold_address.block_slab_id)
            .unwrap();

        let threshold_header = record::BLOCK_RECORD_HEADER_LEN;
        assert_eq!(
            threshold_address.length,
            (threshold_header + payload.len()) as u64
        );
        assert_eq!(record::block_record_compression_byte(&threshold_raw), BLOCK_RECORD_COMPRESSION_NONE);
        assert_eq!(threshold_store.read(&threshold_address).unwrap(), payload);
        assert_eq!(threshold_store.stats().compressed_records_written, 0);
        assert_eq!(threshold_store.stats().compression_bytes_saved, 0);
    }

    #[test]
    fn block_envelope_rejects_corrupt_compressed_payload() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store.append(&b"compress-me-".repeat(80)).unwrap();
        let path = slab_path(dir.path(), address.block_slab_id);
        let mut slab = fs::read(&path).unwrap();
        *slab.last_mut().unwrap() ^= 0xff;
        fs::write(path, slab).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(matches!(
            err,
            BlockStoreError::ChecksumMismatch { .. } | BlockStoreError::CorruptBlockEnvelope { .. }
        ));
    }

    #[test]
    fn block_envelope_rejects_corrupt_header_lengths() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let address = store.append(b"header-checked-page").unwrap();
        let path = slab_path(dir.path(), address.block_slab_id);
        let mut slab = fs::read(&path).unwrap();
        // Make the block say it is far larger than the record holds. The size sits at a
        // constant offset now, so this corrupts the number itself rather than payload bytes,
        // which would only be caught by the checksum.
        let size_at = record::BLOCK_RECORD_LENGTH_OFFSET;
        slab[size_at] = 0xFF;
        slab[size_at + 1] = 0xFF;
        slab[size_at + 2] = 0xFF;
        slab[size_at + 3] = 0x3F;
        fs::write(path, slab).unwrap();

        let err = store.read(&address).unwrap_err();
        assert!(
            matches!(err, BlockStoreError::CorruptBlockEnvelope { .. }),
            "expected a corrupt envelope, got {err:?}"
        );
    }

    #[test]
    fn block_address_without_checksum_keeps_legacy_read_compatibility() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let legacy_address = BlockAddress::from_parts(0, 0, b"alteredpage".len() as u64, None, None, None, None);
        fs::write(
            slab_path(dir.path(), legacy_address.block_slab_id),
            b"alteredpage",
        )
        .unwrap();

        assert_eq!(store.read(&legacy_address).unwrap(), b"alteredpage");
    }

    #[test]
    fn gc_slabs_retains_live_index_references_below_floor() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"live").unwrap();
        store.install_slab(2, b"stale").unwrap();
        store.install_slab(3, b"keep").unwrap();

        let report = store.gc_slabs_before_with_live_refs(3, [1_u64]).unwrap();
        assert_eq!(report.removed_block_slab_ids, vec![0, 2]);
        assert_eq!(report.retained_block_slab_ids, vec![1, 3]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.retained_physical_bytes,
            (b"live".len() + b"keep".len()) as u64
        );
        assert!(report.retained_current_block_slab_ids.is_empty());
        assert_eq!(report.retained_live_block_slab_ids, vec![1]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.slab_ids().unwrap(), vec![1, 3]);
    }

    /// A freshly quarantined slab is NOT purged; one that has waited long enough is.
    ///
    /// "Delayed destroy" quarantined the file and then deleted every file it found, so the delay
    /// lasted only until something called purge -- a slab set aside a second earlier went with the
    /// rest. The hazard is written down in the compaction commit path: a reclaim that quarantines
    /// and purges, followed by a reload of an index that still names the slab, dangles at a
    /// deleted slab.
    ///
    /// Both directions are asserted because either alone is passable by a broken purge: one that
    /// never deleted anything would satisfy the first, and one that ignored the age would satisfy
    /// the second.
    #[test]
    fn a_quarantined_slab_waits_before_it_is_destroyed() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"stale").unwrap();
        store.install_slab(1, b"live").unwrap();
        store
            .gc_slabs_before_with_live_refs_delayed_destroy(1, [1_u64])
            .unwrap();
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![0],
            "the slab is in quarantine to begin with"
        );

        // An hour has not passed, so the purge declines and SAYS it declined.
        let held = store
            .purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
            .unwrap();
        assert!(
            held.purged_block_slab_ids.is_empty(),
            "a slab quarantined moments ago must not be destroyed"
        );
        assert_eq!(
            held.retained_too_young_block_slab_ids,
            vec![0],
            "and the report must say why it is still there"
        );
        assert_eq!(held.retained_too_young_physical_bytes, b"stale".len() as u64);
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![0],
            "the file is still in quarantine"
        );

        // Old enough -- expressed as a zero age rather than by waiting an hour.
        let purged = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
        assert_eq!(
            purged.purged_block_slab_ids,
            vec![0],
            "a slab past its wait is destroyed"
        );
        assert!(
            purged.retained_too_young_block_slab_ids.is_empty(),
            "and nothing is held back once it is old enough"
        );
        assert!(
            store.delayed_destroy_slab_ids().unwrap().is_empty(),
            "quarantine is empty afterwards"
        );
    }

    /// The destroy re-checks liveness, and a slab that comes back live is UN-QUARANTINED.
    ///
    /// The shape the re-check exists for: the collector quarantines a slab nothing references,
    /// and only afterwards does something -- a dump manifest whose embedded index installs pages
    /// in it -- start needing it again. The purge that runs next is the irreversible step, and
    /// before this it consulted only a directory listing and a timestamp.
    ///
    /// THE THREE OUTCOMES ARE ASSERTED SEPARATELY. A purge that destroyed nothing at all would
    /// satisfy "the live slab survived"; one that restored everything would satisfy it too and
    /// free nothing; and a restore that only declined to unlink, leaving the file in the trash
    /// directory, would satisfy both while the slab stayed unreadable by path. So this asserts
    /// that the dead slab WAS destroyed, that the live slab was NOT, and that the live slab is
    /// back where a reader looks for it.
    #[test]
    fn a_quarantined_slab_that_is_live_again_is_returned_not_destroyed() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"needed-again").unwrap();
        store.install_slab(1, b"really-stale").unwrap();
        store.install_slab(2, b"current").unwrap();
        store
            .gc_slabs_before_with_live_refs_delayed_destroy(2, [2_u64])
            .unwrap();

        // THE DENOMINATOR. Both slabs are really in quarantine, and really out of the store, so
        // the assertions below are about a purge that had two things to act on.
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![0, 1],
            "both slabs were really quarantined"
        );
        assert_eq!(
            store.slab_ids().unwrap(),
            vec![2],
            "and really left the store, so a reader cannot reach either by path"
        );

        // Slab 0 became live again after it was quarantined. Slab 1 did not.
        let report = store
            .purge_delayed_destroy_slabs_checked(0, [0_u64])
            .unwrap();

        assert_eq!(
            report.restored_block_slab_ids,
            vec![0],
            "the slab that came back live is taken out of quarantine"
        );
        assert_eq!(
            report.restored_physical_bytes,
            b"needed-again".len() as u64
        );
        assert_eq!(
            report.purged_block_slab_ids,
            vec![1],
            "and the one that is genuinely dead is still destroyed -- the re-check is not a \
             blanket refusal to reclaim"
        );
        assert_eq!(report.purged_physical_bytes, b"really-stale".len() as u64);
        assert!(report.restore_blocked_block_slab_ids.is_empty());

        // The repair is the rename, not the reprieve: the slab is READABLE BY PATH again.
        assert_eq!(
            store.slab_ids().unwrap(),
            vec![0, 2],
            "the restored slab is back in the store where a reader looks for it"
        );
        assert_eq!(
            store.read_slab(0).unwrap(),
            b"needed-again".to_vec(),
            "and its bytes are intact"
        );
        assert!(
            store.delayed_destroy_slab_ids().unwrap().is_empty(),
            "quarantine is empty: one restored, one destroyed, nothing stranded"
        );
    }

    /// A quarantined slab the manifest does not name still gets a grace window.
    ///
    /// THIS IS A SECOND LINE, NOT THE FIRST. `reconcile_slab_manifest_with_disk` already walks the
    /// trash directory on every open and stamps a `DelayedDestroy` descriptor onto every file it
    /// finds there, described or not -- so the crash window inside the collector's loop (rename
    /// each slab, persist the manifest once at the end, die in between) is repaired at the next
    /// open, and within a live process the rename and the state write happen back to back under
    /// one lock. The descriptor-less slab is therefore not reachable through the ordinary API,
    /// which is why this constructs one directly.
    ///
    /// It is guarded anyway because the purge's own reading of the case was the OPPOSITE policy:
    /// a slab with no descriptor was destroyed on sight, with the whole hour skipped. That is a
    /// dangerous default to leave standing behind a repair in a different module -- the repair
    /// can be narrowed, skipped for speed, or run after the purge, and then the destroy is
    /// immediate again with nothing to say so.
    ///
    /// Asserted in BOTH directions, because one alone is passable: a purge that never destroyed
    /// an undescribed slab at all would satisfy the first assertion and leak the bytes for ever.
    #[test]
    fn a_quarantined_slab_with_no_descriptor_still_waits() {
        // An id no slab in this store uses: a fresh store already opens slab 0 active, and
        // reusing that id would collide with a descriptor that legitimately exists.
        const ORPHAN: u64 = 7;
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(1, b"current").unwrap();

        // A file in quarantine that the manifest never learned about.
        let trash = delayed_destroy_dir(dir.path());
        fs::create_dir_all(&trash).unwrap();
        fs::write(
            trash.join(format!("page_segment_{ORPHAN:020}.seg.deleted.1")),
            b"orphaned-by-a-crash",
        )
        .unwrap();

        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![ORPHAN],
            "DENOMINATOR: the slab is really in quarantine, so the purge has something to act on"
        );
        assert!(
            !store
                .slab_descriptors()
                .iter()
                .any(|descriptor| descriptor.block_slab_id == ORPHAN),
            "DENOMINATOR: and it really has no descriptor, so this exercises the fallback"
        );

        // The hour has not passed by ANY clock, so the purge must decline.
        let held = store
            .purge_delayed_destroy_slabs_older_than(DELAYED_DESTROY_MIN_AGE_MS)
            .unwrap();
        assert!(
            held.purged_block_slab_ids.is_empty(),
            "a slab quarantined moments ago must keep its grace even with no descriptor"
        );
        assert_eq!(
            held.retained_too_young_block_slab_ids,
            vec![ORPHAN],
            "and the report must say it was held, not that there was nothing to do"
        );

        // But it is still reclaimable: the fallback grants a window, it does not grant immunity.
        let purged = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
        assert_eq!(
            purged.purged_block_slab_ids,
            vec![ORPHAN],
            "past its wait, an undescribed slab is destroyed like any other"
        );
        assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());
    }

    /// A purge told which slabs it may destroy destroys those and leaves the rest ALONE.
    ///
    /// The half of the per-slab reclaim gate that lives in this file. Once the collector stops
    /// being suppressed by a single pinned slab, a round runs whenever ANY candidate is free --
    /// and a purge that still swept the whole trash directory would then destroy the quarantined
    /// slabs the dependency plan had specifically blocked. That converts a suppressed reclaim
    /// into lost data, which is why the list and the gate move together.
    ///
    /// The two outcomes are asserted SEPARATELY. A purge that destroyed nothing would satisfy
    /// "the blocked slab survived"; one that destroyed everything would satisfy "the free slab
    /// was reclaimed". Only both together say the list was actually consulted.
    #[test]
    fn a_purge_handed_a_slab_list_leaves_the_rest_in_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"free-to-go").unwrap();
        store.install_slab(1, b"pinned-by-a-follower").unwrap();
        store.install_slab(2, b"current").unwrap();
        store
            .gc_slabs_before_with_live_refs_delayed_destroy(2, [2_u64])
            .unwrap();

        // DENOMINATOR: the purge really has two slabs to act on, so "one survived" is a choice
        // it made rather than a directory that was empty.
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![0, 1],
            "both slabs were really quarantined"
        );

        // Slab 1 is blocked upstream; only slab 0 is free.
        let report = store
            .purge_delayed_destroy_slabs_selected(
                0,
                Vec::<u64>::new(),
                Some([0_u64].into_iter().collect()),
            )
            .unwrap();

        assert_eq!(
            report.purged_block_slab_ids,
            vec![0],
            "the free slab is reclaimed -- one blocked slab no longer suppresses the others"
        );
        assert!(
            report.retained_too_young_block_slab_ids.is_empty(),
            "the blocked slab is not reported as too young: it was never considered, and saying \
             'not yet' about it would describe a wait that is not what is holding it"
        );
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap(),
            vec![1],
            "and the blocked slab is still in quarantine, undestroyed"
        );

        // Unnarrowed, it takes the rest: the list narrows this round, it does not retire a slab.
        let rest = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
        assert_eq!(
            rest.purged_block_slab_ids,
            vec![1],
            "once nothing blocks it, the slab is reclaimed on a later round"
        );
    }

    /// One uncapped purge round against `slabs` quarantined slabs, timed, then one capped round
    /// against the same fixture. Prints; returns nothing. The body of the two scale tests below.
    ///
    /// Split out of the loop it used to be so each size is a test of its own and can be RUN on
    /// its own. The 80,000 arm writes eighty thousand files and is the expensive half; on a box
    /// short of disk, running the 8,000 arm alone is a real measurement, whereas running neither
    /// and scaling the other is not a measurement at all.
    fn purge_at_scale_arm(slabs: u64) {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());

        // The quarantine state is built DIRECTLY rather than by installing and collecting
        // N slabs. Installing is the harness, not the subject: it summarises every slab and
        // periodically rewrites the manifest. What is being measured is the purge -- the round
        // that the re-check and the slab list changed, and the one that holds the store lock
        // while it unlinks.
        let started = std::time::Instant::now();
        let trash = delayed_destroy_dir(dir.path());
        fs::create_dir_all(&trash).unwrap();
        for id in 0..slabs {
            fs::write(
                trash.join(format!("page_segment_{id:020}.seg.deleted.{id}")),
                b"slab",
            )
            .unwrap();
        }
        let setup_ms = started.elapsed().as_secs_f64() * 1e3;

        // DENOMINATOR: the quarantine really is the size claimed, so the counts below are
        // about a purge that had that much to decide.
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "every slab must really be in quarantine before the purge runs"
        );

        // A tenth are live again; a further tenth are blocked upstream. Disjoint, so each
        // outcome has its own denominator and one cannot cover for another.
        let live = (0..slabs).filter(|id| id % 10 == 0).collect::<Vec<_>>();
        let blocked = (0..slabs).filter(|id| id % 10 == 1).collect::<Vec<_>>();
        let selected = (0..slabs).filter(|id| id % 10 != 1).collect::<BTreeSet<_>>();
        assert!(!live.is_empty() && !blocked.is_empty());

        // ONE ROUND WITH THE CAP OFF -- the shape this change is about, kept runnable so the
        // number it replaces can still be produced rather than only quoted.
        let started = std::time::Instant::now();
        let report = store
            .purge_delayed_destroy_slabs_capped(0, live.clone(), Some(selected.clone()), 0)
            .unwrap();
        let purge_ms = started.elapsed().as_secs_f64() * 1e3;

        let expected_purged = slabs - live.len() as u64 - blocked.len() as u64;
        // THREE HALVES, SEPARATELY.
        assert_eq!(
            report.restored_block_slab_ids.len(),
            live.len(),
            "every live slab was restored"
        );
        assert_eq!(
            report.purged_block_slab_ids.len() as u64,
            expected_purged,
            "every slab that was neither live nor blocked was destroyed"
        );
        assert!(
            !report.budget_exhausted,
            "an uncapped round must not report a budget it did not have"
        );
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len(),
            blocked.len(),
            "and exactly the blocked slabs are still in quarantine"
        );
        // The restored slabs are back in the store, readable by path.
        let present = store.slab_ids().unwrap().into_iter().collect::<BTreeSet<_>>();
        assert!(
            live.iter().all(|id| present.contains(id)),
            "every restored slab is readable by path again"
        );

        println!(
            "  {slabs:>6} quarantined: setup {setup_ms:>9.1} ms   UNCAPPED purge {purge_ms:>9.1} ms \
             ({:>6} destroyed, {:>5} restored, {:>5} held)   per slab {:.4} ms",
            report.purged_block_slab_ids.len(),
            report.restored_block_slab_ids.len(),
            blocked.len(),
            purge_ms / slabs as f64,
        );

        // And now ONE ROUND UNDER THE SHIPPED CAP, on a freshly rebuilt quarantine of the same
        // size. This is the number that matters: how long the store lock is held by a round the
        // scheduler actually runs.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let trash = delayed_destroy_dir(dir.path());
        fs::create_dir_all(&trash).unwrap();
        for id in 0..slabs {
            fs::write(
                trash.join(format!("page_segment_{id:020}.seg.deleted.{id}")),
                b"slab",
            )
            .unwrap();
        }
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "the capped round must face the same size quarantine as the uncapped one"
        );
        let started = std::time::Instant::now();
        let capped = store
            .purge_delayed_destroy_slabs_selected(0, live.clone(), Some(selected))
            .unwrap();
        let capped_ms = started.elapsed().as_secs_f64() * 1e3;
        assert_eq!(
            capped.processed_block_slabs, DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
            "a capped round against a quarantine far larger than its budget spends all of it"
        );
        assert!(
            capped.budget_exhausted,
            "and says there is more to do -- otherwise a caller stops draining"
        );
        println!(
            "  {slabs:>6} quarantined:                    CAPPED purge {capped_ms:>9.1} ms \
             ({:>6} destroyed, {:>5} restored) at a budget of {}",
            capped.purged_block_slab_ids.len(),
            capped.restored_block_slab_ids.len(),
            DELAYED_DESTROY_MAX_SLABS_PER_ROUND,
        );
    }

    /// The re-checked, list-narrowed purge at 8,000 slabs, capped and uncapped. Prints.
    ///
    ///   cargo test -p temporalstore-rust --lib the_purge_at_scale \
    ///       -- --ignored --nocapture --test-threads=1
    ///
    /// Ignored because it creates eight thousand files; the correctness guards run in CI, and
    /// this answers the question correctness cannot -- what ONE purge round costs while it holds
    /// the store lock, with the cap off and with the cap on.
    ///
    /// Every count is asserted as a separate half. Five defects in this area each presented as
    /// one half full and the other zero, and a combined total hid all of them.
    #[test]
    #[ignore]
    fn the_purge_at_scale() {
        purge_at_scale_arm(8_000);
    }

    /// The same measurement at 80,000 slabs, where the unbounded round was measured at 58.7 s.
    ///
    ///   cargo test -p temporalstore-rust --lib the_purge_at_eighty_thousand_slabs \
    ///       -- --ignored --nocapture --test-threads=1
    ///
    /// SEPARATE FROM THE 8,000 ARM BECAUSE IT IS THE EXPENSIVE ONE. It writes eighty thousand
    /// files twice over and wants real disk headroom; on a box that cannot spare it, the 8,000
    /// arm above still produces a measurement, and this one should be reported as not run rather
    /// than scaled up from the other.
    #[test]
    #[ignore]
    fn the_purge_at_eighty_thousand_slabs() {
        purge_at_scale_arm(80_000);
    }

    /// Build a quarantine of `slabs` files directly, without installing or collecting anything.
    ///
    /// The collector is not the subject of the purge guards below, and going through it would
    /// make each of them pay for a full install of every slab.
    fn quarantine_fixture(root: &std::path::Path, slabs: u64) {
        let trash = delayed_destroy_dir(root);
        fs::create_dir_all(&trash).unwrap();
        for id in 0..slabs {
            fs::write(
                trash.join(format!("page_segment_{id:020}.seg.deleted.{id}")),
                b"slab",
            )
            .unwrap();
        }
    }

    /// A capped purge must ADVANCE: each round must move to slabs the last one did not touch, and
    /// the whole quarantine must be drained in the number of rounds the budget predicts.
    ///
    /// THE ROUND COUNT IS COMPUTED BEFORE THE RUN AND THE RUN IS GIVEN MORE ROUNDS THAN IT NEEDS.
    /// A loop that stops at exactly the predicted number cannot tell a cap that finished from a
    /// cap that stalled on its last round and was cut off -- both end with the loop exhausted.
    /// Running past the prediction and asserting there was nothing left to do makes those two
    /// outcomes different.
    ///
    /// The per-round sets are asserted DISJOINT as well as exhaustive. Exhaustive alone would be
    /// satisfied by a cap that re-walked the same slabs and happened to get through them; disjoint
    /// alone would be satisfied by a cap that destroyed three slabs and then stopped for ever.
    #[test]
    fn a_capped_purge_advances_and_drains_in_the_rounds_its_budget_predicts() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let slabs = 500u64;
        let budget = 40usize;
        quarantine_fixture(dir.path(), slabs);

        // DENOMINATOR: the quarantine really holds what the arithmetic below assumes.
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "every slab must really be in quarantine before the first round"
        );

        let live = (0..slabs).filter(|id| id % 10 == 0).collect::<Vec<_>>();
        let blocked = (0..slabs).filter(|id| id % 10 == 1).collect::<BTreeSet<_>>();
        let selected = (0..slabs).filter(|id| id % 10 != 1).collect::<BTreeSet<_>>();
        let expected_destroyed = (0..slabs)
            .filter(|id| id % 10 != 0 && id % 10 != 1)
            .collect::<BTreeSet<_>>();
        // Work is a destroy or a restore. The blocked slabs are named by nobody, so they are
        // never work, and a budget spent on them would be a budget spent on nothing.
        let work = expected_destroyed.len() + live.len();
        let predicted_rounds = work.div_ceil(budget);
        assert_eq!(
            (work, predicted_rounds),
            (450, 12),
            "the prediction is arithmetic on the fixture, stated before the run"
        );
        // The budget must not divide the work exactly: a round that spends its budget as the work
        // runs out still reports `budget_exhausted`, so an evenly-dividing fixture takes one extra
        // empty round whose existence depends on `read_dir` order. A partial last round does not.
        assert_ne!(work % budget, 0, "the last round must be a partial one");
        // Deliberately more rounds than predicted, so "finished" and "stalled" look different.
        let allowed_rounds = predicted_rounds + 8;

        let mut rounds = 0usize;
        let mut per_round_touched: Vec<BTreeSet<u64>> = Vec::new();
        let mut all_destroyed = BTreeSet::new();
        let mut all_restored = BTreeSet::new();
        loop {
            let report = store
                .purge_delayed_destroy_slabs_capped(
                    0,
                    live.clone(),
                    Some(selected.clone()),
                    budget,
                )
                .unwrap();
            let touched = report
                .purged_block_slab_ids
                .iter()
                .chain(report.restored_block_slab_ids.iter())
                .copied()
                .collect::<BTreeSet<_>>();
            assert_eq!(
                touched.len(),
                report.processed_block_slabs,
                "the round's own count of what it did must match what it reported doing"
            );
            all_destroyed.extend(report.purged_block_slab_ids.iter().copied());
            all_restored.extend(report.restored_block_slab_ids.iter().copied());
            per_round_touched.push(touched);
            rounds += 1;
            if !report.budget_exhausted {
                break;
            }
            assert_eq!(
                report.processed_block_slabs, budget,
                "a round that says it ran out of budget must have SPENT the budget; anything \
                 less means it stopped for some other reason and is calling it a budget"
            );
            assert!(
                rounds <= allowed_rounds,
                "the drain did not finish in {allowed_rounds} rounds, which is {} more than the \
                 {predicted_rounds} its budget predicts -- that is a stall, not a slow cap",
                allowed_rounds - predicted_rounds
            );
        }

        // IT ADVANCED: no round revisited a slab an earlier round had already dealt with.
        let mut seen = BTreeSet::new();
        for (index, touched) in per_round_touched.iter().enumerate() {
            assert!(
                touched.is_disjoint(&seen),
                "round {index} acted on a slab an earlier round had already finished with; the \
                 set the cap processes must MOVE"
            );
            seen.extend(touched.iter().copied());
        }
        // AND IT FINISHED, in exactly the rounds the budget predicted.
        assert_eq!(
            rounds, predicted_rounds,
            "draining {work} slabs at {budget} a round must take {predicted_rounds} rounds"
        );
        assert_eq!(
            all_destroyed, expected_destroyed,
            "every slab that was neither live nor blocked was destroyed, across the rounds"
        );
        assert_eq!(
            all_restored,
            live.iter().copied().collect::<BTreeSet<_>>(),
            "and every live slab was restored, across the rounds"
        );
        assert_eq!(
            store
                .delayed_destroy_slab_ids()
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            blocked,
            "exactly the blocked slabs are left in quarantine"
        );
        // One more round finds nothing to do -- the drain is over, not merely paused.
        let after = store
            .purge_delayed_destroy_slabs_capped(0, live.clone(), Some(selected), budget)
            .unwrap();
        assert_eq!(
            (after.processed_block_slabs, after.budget_exhausted),
            (0, false),
            "a round after the drain must do nothing and must not claim there is more"
        );
    }

    /// A quarantine that is almost entirely slabs the caller has NOT named must still drain the
    /// few it has.
    ///
    /// THIS IS THE STALL THE CAP HAS TO NOT HAVE. Nine hundred of these thousand slabs are
    /// blocked upstream; they are skipped every round and they stay in the directory, so they are
    /// in front of the loop again next round. A budget charged for every ENTRY THE LOOP LOOKS AT
    /// rather than for every slab it ACTS ON would spend all ten of each round's units on the
    /// same blocked slabs and destroy nothing, for ever -- and from the outside that is
    /// indistinguishable from a cap that is merely conservative.
    ///
    /// A hundred rounds is the entry-counted cost; twelve is the work-counted one. The assertion
    /// is on twelve.
    ///
    /// THE BUDGET DELIBERATELY DOES NOT DIVIDE THE WORK. 100 slabs at 10 a round would finish in
    /// ten full rounds -- and a round that spends its budget exactly as the work runs out still
    /// reports `budget_exhausted`, so the drain takes an eleventh, empty round whose existence
    /// depends on where in directory order the last actionable slab happened to sit. At 9 a round
    /// the last round is always partial, always walks to the end of the directory, and always
    /// reports the drain finished: twelve rounds, whatever order `read_dir` returns.
    #[test]
    fn a_capped_purge_is_not_starved_by_the_slabs_it_must_skip() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let slabs = 1_000u64;
        let budget = 9usize;
        quarantine_fixture(dir.path(), slabs);

        // DENOMINATORS, both halves, before anything runs.
        let selected = (0..slabs).filter(|id| id % 10 == 0).collect::<BTreeSet<_>>();
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "a thousand slabs really are in quarantine"
        );
        assert_eq!(
            selected.len(),
            100,
            "and only a hundred of them are this caller's business -- the other nine hundred are \
             what the loop has to walk past"
        );

        let predicted_rounds = selected.len().div_ceil(budget);
        assert_eq!(predicted_rounds, 12, "stated before the run");
        assert_ne!(
            selected.len() % budget,
            0,
            "the budget must not divide the work, or the last round's report depends on where in \
             directory order the final actionable slab sits"
        );
        let allowed_rounds = predicted_rounds + 5;

        let mut rounds = 0usize;
        let mut destroyed = BTreeSet::new();
        loop {
            let report = store
                .purge_delayed_destroy_slabs_capped(
                    0,
                    Vec::<u64>::new(),
                    Some(selected.clone()),
                    budget,
                )
                .unwrap();
            destroyed.extend(report.purged_block_slab_ids.iter().copied());
            rounds += 1;
            if !report.budget_exhausted {
                break;
            }
            assert!(
                rounds <= allowed_rounds,
                "after {rounds} rounds the drain has destroyed {} of {}; a budget spent on \
                 entries examined instead of work done would look exactly like this",
                destroyed.len(),
                selected.len()
            );
        }
        assert_eq!(
            rounds, predicted_rounds,
            "the skipped slabs must cost the loop a walk, never a unit of budget"
        );
        assert_eq!(
            destroyed, selected,
            "and every slab the caller did name was destroyed"
        );
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len(),
            900,
            "with the nine hundred it did not name still in quarantine"
        );
    }

    /// The cap must bound HOW MANY slabs a round reaches, never what happens to one it reached.
    ///
    /// Every slab here is live, so every slab the round touches must go through the last-chance
    /// re-check and come back OUT of quarantine, readable by path again. A cap that reached a
    /// slab and skipped the re-check to save time would destroy live data, which is the one
    /// failure this whole path exists to prevent.
    #[test]
    fn a_capped_purge_re_checks_liveness_for_every_slab_it_reaches() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let slabs = 60u64;
        let budget = 7usize;
        quarantine_fixture(dir.path(), slabs);
        let live = (0..slabs).collect::<Vec<_>>();

        // DENOMINATOR: the store holds none of these yet, so "readable by path" below is the
        // restore's doing and not a file that was already there.
        assert_eq!(
            store.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs,
            "every slab is in quarantine"
        );
        assert!(
            store.slab_ids().unwrap().is_empty(),
            "and none of them is in the store"
        );

        let mut restored = BTreeSet::new();
        let mut rounds = 0usize;
        loop {
            let report = store
                .purge_delayed_destroy_slabs_capped(0, live.clone(), None, budget)
                .unwrap();
            assert!(
                report.purged_block_slab_ids.is_empty(),
                "a live slab must never be destroyed, capped round or not"
            );
            restored.extend(report.restored_block_slab_ids.iter().copied());
            rounds += 1;
            if !report.budget_exhausted {
                break;
            }
            assert_eq!(
                report.restored_block_slab_ids.len(),
                budget,
                "a full round restores exactly its budget -- the cap limits the count, not the \
                 treatment"
            );
            assert!(rounds <= 20, "60 slabs at 7 a round must not take 20 rounds");
        }
        assert_eq!(
            restored.len() as u64,
            slabs,
            "every live slab came back out of quarantine"
        );
        assert_eq!(
            store.slab_ids().unwrap().len() as u64,
            slabs,
            "and every one of them is readable by path again"
        );
        assert!(
            store.delayed_destroy_slab_ids().unwrap().is_empty(),
            "with nothing left in quarantine"
        );
    }

    /// The collector's victim order is LARGEST SLAB FIRST, not the highest-garbage order its sort
    /// key is written to express -- and that is why the collector's own per-round budget stays
    /// OFF while the purge gets one.
    ///
    /// `can_the_block_gc_garbage_floor_bind` establishes the premise and asserts it in CI: every
    /// candidate reports `used_bytes == 0`, so every candidate reports the same zero live
    /// fraction. This test states the CONSEQUENCE for ordering. With the first sort key uniform
    /// and the second (`utility_score`) uniform too, the first key that can separate two
    /// candidates is physical size, descending. The order the comment above the sort describes is
    /// therefore not the order that happens.
    ///
    /// NOTHING IS PUBLISHED HERE, AND THAT IS THE POINT. `used_bytes` now means live page bytes
    /// on the slab whenever an index has published a tally, and
    /// `a_published_live_tally_makes_used_bytes_mean_live_block_bytes` shows that ordering coming
    /// out highest-garbage first. This store has no publisher, so it exercises the unpublished
    /// arm -- which is still what a bare `BlockStore` does, and still orders by size.
    ///
    /// AN UNBOUNDED COLLECTOR DOES NOT CARE -- it takes every candidate, so the order only
    /// decides what happens first. A BUDGETED ONE DOES: the budget makes the order decide who
    /// SURVIVES, and here the survivors would be the smallest files, chosen by a rule nobody
    /// wrote down and unrelated to how much garbage they hold. Worse, it can starve: small slabs
    /// keep losing to every larger slab that arrives later.
    ///
    /// The purge cap has neither problem, which is the asymmetry behind treating the two
    /// differently. Its budget is spent on work, every slab it charges for LEAVES the trash
    /// directory, and the set it has left to do strictly shrinks -- so nothing it defers can be
    /// deferred for ever, whatever order the directory hands it.
    ///
    /// If this test starts failing because the order changed, the question of whether to budget
    /// the collector is open again and should be re-asked rather than assumed.
    #[test]
    fn the_collector_victim_order_is_size_while_every_candidate_reports_zero_used_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        // Sizes deliberately disagree with id order, so an assertion about the resulting order
        // cannot be satisfied by the slabs merely coming back in the order they were made.
        store.install_slab(0, &vec![b'a'; 200]).unwrap();
        store.install_slab(1, &vec![b'b'; 800]).unwrap();
        store.install_slab(2, &vec![b'c'; 400]).unwrap();
        store.install_slab(3, &vec![b'd'; 100]).unwrap();
        store.install_slab(4, b"current").unwrap();

        let candidates = store.gc_utility_candidates(4, Vec::<u64>::new()).unwrap();
        // DENOMINATOR: there really are four candidates to order.
        assert_eq!(
            candidates.len(),
            4,
            "four slabs are below the retention floor and collectable"
        );
        // THE PREMISE, restated where the consequence is drawn, so this test fails on its own
        // terms if used bytes ever start meaning live page bytes.
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.used_bytes == 0
                    && candidate.utility_basis_points == 0),
            "every candidate reports a zero live fraction, so the garbage key carries no \
             information to sort by: {candidates:?}"
        );
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.utility_score == 0),
            "and the categorical score is uniform too, so it cannot separate them either"
        );

        let order = candidates
            .iter()
            .map(|candidate| candidate.block_slab_id)
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![1, 2, 0, 3],
            "the order that actually results is descending physical size (800, 400, 200, 100 \
             bytes of payload), not anything about garbage"
        );

        // So a budget of two would destroy the two LARGEST and leave the two smallest.
        let plan = store
            .gc_policy_plan(
                4,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::max_slabs(2),
            )
            .unwrap();
        assert_eq!(
            plan.selected_block_slab_ids,
            vec![1, 2],
            "a budgeted collector picks its victims by file size; the survivors are the small \
             slabs, for no reason connected to how much of them is garbage"
        );

        // AND THE SHIPPED POLICY LEAVES THAT BUDGET OFF.
        assert_eq!(
            BlockStoreGcPolicy::with_slab_garbage_floor(
                crate::engine::reports::DEFAULT_BLOCK_GC_MIN_SLAB_GARBAGE_BASIS_POINTS,
                None,
            )
            .max_destroy_slabs,
            0,
            "the collector's per-round budget is off, and stays off until used bytes mean live \
             page bytes within the slab"
        );
    }

    /// Quarantining N slabs must cost a FIXED number of directory fsyncs, not two per slab.
    ///
    /// RUN AT TWO SIZES AND COMPARED, because a single size cannot tell a constant from a
    /// coefficient. Measured on the unhoisted loop with `strace -y -e trace=fsync`: quarantining
    /// 199 slabs issued 199 fsyncs of the trash directory and 199 of the store root; 799 slabs
    /// issued 799 and 799. Hoisted, both sizes must issue the same small number.
    ///
    /// THE NON-ZERO ASSERTION IS NOT DECORATION. Deleting every fsync in the module makes both
    /// counts zero, which satisfies "the count does not grow with the slab count" perfectly --
    /// a mutation that removes the durability this test is standing next to would PASS on the
    /// equality alone. The floor is what makes the guard about hoisting the fsyncs rather than
    /// about having none.
    #[test]
    fn quarantining_a_round_of_slabs_fsyncs_its_directories_once_not_once_per_slab() {
        let mut measured: Vec<(u64, usize, u64)> = Vec::new();
        for slabs in [16u64, 64] {
            let dir = tempfile::tempdir().unwrap();
            let store = BlockStore::new(dir.path());
            for id in 0..slabs {
                store.install_slab(id, b"slab-contents").unwrap();
            }
            let before = super::paths::directory_fsyncs();
            let report = store
                .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
                .unwrap();
            let fsyncs = super::paths::directory_fsyncs() - before;
            // DENOMINATOR: the round really quarantined nearly every slab, so a small fsync
            // count is a hoist and not an empty round.
            assert_eq!(
                report.delayed_destroy_block_slab_ids.len() as u64,
                slabs - 1,
                "the round must really have quarantined {} slabs",
                slabs - 1
            );
            measured.push((slabs, report.delayed_destroy_block_slab_ids.len(), fsyncs));
        }

        let (small, small_quarantined, small_fsyncs) = measured[0];
        let (large, large_quarantined, large_fsyncs) = measured[1];
        assert!(
            large_quarantined > small_quarantined * 3,
            "the two sizes must really differ, or 'the count did not grow' says nothing"
        );
        assert_eq!(
            small_fsyncs, large_fsyncs,
            "quarantining {small_quarantined} slabs cost {small_fsyncs} directory fsyncs and \
             {large_quarantined} cost {large_fsyncs}; a count that tracks the slab count is the \
             per-slab fsync back again"
        );
        // Two for the round's renames, one for the manifest that records them.
        assert_eq!(
            small_fsyncs, 3,
            "a quarantine round syncs the store root and the trash directory once each, and the \
             manifest write syncs the root once more"
        );
        assert!(
            small_fsyncs >= 2,
            "and it must still sync BOTH directories -- a round that syncs nothing would satisfy \
             the equality above while making the renames undurable"
        );
    }

    /// A purge round that DESTROYED AND RESTORED NOTHING must not fsync, and must not rewrite the
    /// slab manifest.
    ///
    /// The collector half of this stage already has the guard --
    /// `a_gc_round_that_reclaimed_nothing_does_not_rewrite_the_manifest` -- and the reasoning it
    /// records applies word for word here: the manifest write "serialises every slab, fsyncs the
    /// temp file, renames it and fsyncs the parent directory", on a stage the periodic loop runs
    /// whenever page pressure holds. The purge runs in the SAME round as the collector, from
    /// `apply_storage_lifecycle`, and had no such guard: it synced both directories and rewrote the
    /// manifest on every round, including the rounds where every quarantined slab was still inside
    /// its grace window and the round therefore touched nothing at all -- which is what a purge
    /// round looks like for the whole hour after a quarantine.
    ///
    /// Counted rather than timed: how many fsyncs a round issues is the shape itself and reads the
    /// same on a loaded box as on an idle one.
    #[test]
    fn a_purge_round_that_acted_on_nothing_does_not_fsync_or_rewrite_the_manifest() {
        fn arm(
            min_age_ms: u64,
            live_block_slab_ids: Vec<u64>,
        ) -> (u64, bool, BlockStorePurgeDelayedDestroyReport) {
            let dir = tempfile::tempdir().unwrap();
            let store = BlockStore::new(dir.path());
            for index in 0..8u64 {
                store.append(format!("record-{index}").as_bytes()).unwrap();
            }
            store.sync_durable().unwrap();
            quarantine_fixture(dir.path(), 16);
            let manifest = slab_manifest_path(dir.path());
            assert!(manifest.exists(), "the fixture needs a manifest to leave alone");
            let mtime_before = std::fs::metadata(&manifest).unwrap().modified().unwrap();
            let fsyncs_before = super::paths::directory_fsyncs();
            let report = store
                .purge_delayed_destroy_slabs_capped(min_age_ms, live_block_slab_ids, None, 0)
                .unwrap();
            let fsyncs = super::paths::directory_fsyncs() - fsyncs_before;
            let mtime_after = std::fs::metadata(&manifest).unwrap().modified().unwrap();
            (fsyncs, mtime_before != mtime_after, report)
        }

        // Nothing is old enough, so the round walks all sixteen entries and acts on none of them.
        let (idle_fsyncs, idle_rewrote, idle) = arm(u64::MAX, Vec::new());
        // THE DENOMINATOR: the round really did walk the quarantine. A round that found an empty
        // directory would satisfy everything below for the wrong reason.
        assert_eq!(
            idle.retained_too_young_block_slab_ids.len(),
            16,
            "the idle round must have examined all sixteen quarantined slabs: {idle:?}"
        );
        assert_eq!(idle.processed_block_slabs, 0, "and charged its budget for none: {idle:?}");
        assert!(idle.purged_block_slab_ids.is_empty(), "{idle:?}");
        assert!(idle.restored_block_slab_ids.is_empty(), "{idle:?}");

        // THE TWO HALVES, ASSERTED SEPARATELY.
        assert_eq!(
            idle_fsyncs, 0,
            "a purge round that destroyed and restored nothing issued {idle_fsyncs} directory \
             fsyncs; there is no rename and no unlink for them to commit"
        );
        assert!(
            !idle_rewrote,
            "and it rewrote the slab manifest with byte-identical content: {idle:?}"
        );

        // THE CONTROL, on the same fixture: a round that DOES act still syncs and still writes.
        // Without this the assertions above are satisfied by a purge that stopped working.
        let (busy_fsyncs, _, busy) = arm(0, Vec::new());
        assert_eq!(
            busy.purged_block_slab_ids.len(),
            16,
            "the control round must really have destroyed the quarantine: {busy:?}"
        );
        assert!(
            busy_fsyncs >= 3,
            "a round that unlinked sixteen slabs must still sync both directories and the \
             manifest rename, but issued {busy_fsyncs}"
        );

        // THE OTHER HALF OF THE CONDITION, ON ITS OWN. A round can do work without destroying
        // anything: a slab that came back live is RENAMED out of quarantine and back into the
        // store, and that rename needs the same two directory fsyncs an unlink does. Without this
        // arm the guard passes while testing only the purged half -- verified by mutation:
        // narrowing the condition to `!purged.is_empty()` alone left all 79 tests green.
        //
        // Nothing is old enough to destroy, and ids 8..16 are named live. Ids 0..8 stay put
        // (too young), so this round restores and destroys nothing.
        let (restore_fsyncs, _, restore) = arm(u64::MAX, (8..16).collect::<Vec<u64>>());
        assert_eq!(
            restore.restored_block_slab_ids,
            (8..16).collect::<Vec<u64>>(),
            "the restore-only round must really have restored eight slabs: {restore:?}"
        );
        assert!(
            restore.purged_block_slab_ids.is_empty(),
            "and destroyed none, which is the whole point of this arm: {restore:?}"
        );
        assert!(
            restore_fsyncs >= 3,
            "a round that renamed eight slabs back into the store must still sync both \
             directories and the manifest rename, but issued {restore_fsyncs}"
        );
    }

    /// A purge round's directory fsyncs must not track the number of slabs it RESTORES.
    ///
    /// The unlinks were already batched to one trash-directory fsync per round. The restores were
    /// not: each one fsynced the trash directory and the store root, so a round that restored
    /// eight thousand slabs issued sixteen thousand fsyncs to commit renames that travel between
    /// the same two directories.
    ///
    /// Compared against a round that restores NOTHING, on the same fixture size, so the number
    /// being held constant is the restore count and not the round.
    #[test]
    fn a_purge_round_fsyncs_its_directories_once_however_many_slabs_it_restores() {
        let mut measured: Vec<(usize, usize, u64)> = Vec::new();
        for live_count in [0u64, 48] {
            let dir = tempfile::tempdir().unwrap();
            let store = BlockStore::new(dir.path());
            let slabs = 64u64;
            quarantine_fixture(dir.path(), slabs);
            assert_eq!(
                store.delayed_destroy_slab_ids().unwrap().len() as u64,
                slabs,
                "the fixture is the same size in both arms"
            );
            let live = (0..live_count).collect::<Vec<_>>();
            let before = super::paths::directory_fsyncs();
            let report = store
                .purge_delayed_destroy_slabs_capped(0, live, None, 0)
                .unwrap();
            let fsyncs = super::paths::directory_fsyncs() - before;
            // DENOMINATORS: both halves of the round really happened.
            assert_eq!(
                report.restored_block_slab_ids.len() as u64,
                live_count,
                "the round restored what this arm asked it to"
            );
            assert_eq!(
                report.purged_block_slab_ids.len() as u64,
                slabs - live_count,
                "and destroyed the rest"
            );
            measured.push((
                report.restored_block_slab_ids.len(),
                report.purged_block_slab_ids.len(),
                fsyncs,
            ));
        }

        let (no_restores, _, no_restore_fsyncs) = measured[0];
        let (many_restores, _, many_restore_fsyncs) = measured[1];
        assert_eq!(no_restores, 0);
        assert_eq!(many_restores, 48);
        assert_eq!(
            no_restore_fsyncs, many_restore_fsyncs,
            "a round restoring 48 slabs cost {many_restore_fsyncs} directory fsyncs against \
             {no_restore_fsyncs} for a round restoring none; the difference is the per-restore \
             fsync"
        );
        assert!(
            no_restore_fsyncs >= 2,
            "and the round must still sync both directories -- zero would satisfy the equality \
             while leaving every rename undurable"
        );
    }

    /// What a quarantine round leaves on disk must not change when the fsyncs move.
    ///
    /// The hoist widens the window in which a crash can leave the batch half-applied; it must not
    /// change the outcome of the round that COMPLETES. A store reopened from the same root has to
    /// see the same thing either way: every quarantined slab gone from the store, every one of
    /// them in the trash directory, and the manifest agreeing.
    #[test]
    fn a_quarantine_round_is_durable_as_a_whole_after_the_fsyncs_are_hoisted() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let slabs = 32u64;
        for id in 0..slabs {
            store.install_slab(id, b"slab-contents").unwrap();
        }
        // DENOMINATOR: everything is in the store before the round.
        assert_eq!(store.slab_ids().unwrap().len() as u64, slabs);
        assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());

        let report = store
            .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
            .unwrap();
        assert_eq!(report.delayed_destroy_block_slab_ids.len() as u64, slabs - 1);
        drop(store);

        let reopened = BlockStore::new(dir.path());
        assert_eq!(
            reopened.slab_ids().unwrap(),
            vec![slabs - 1],
            "only the current slab is left in the store after a reopen"
        );
        assert_eq!(
            reopened.delayed_destroy_slab_ids().unwrap().len() as u64,
            slabs - 1,
            "and every quarantined slab is still in the trash directory"
        );
        // The manifest agrees with the directory: each quarantined slab reads as DelayedDestroy.
        let quarantined = reopened
            .delayed_destroy_slab_ids()
            .unwrap()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let states = reopened
            .inner
            .lock()
            .unwrap()
            .slabs
            .iter()
            .filter(|(id, _)| quarantined.contains(id))
            .map(|(_, slab)| slab.state)
            .collect::<Vec<_>>();
        assert_eq!(
            states.len() as u64,
            slabs - 1,
            "every quarantined slab has a descriptor after the reopen"
        );
        assert!(
            states
                .iter()
                .all(|state| matches!(state, BlockStoreSlabState::DelayedDestroy)),
            "and each one reads as DelayedDestroy -- the manifest written after the renames \
             agrees with the directory the renames produced"
        );
    }

    #[test]
    fn delayed_destroy_gc_quarantines_stale_slabs_before_purge() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"current").unwrap();
        store.install_slab(1, b"stale").unwrap();
        store.install_slab(2, b"live").unwrap();
        store.install_slab(3, b"keep").unwrap();

        let report = store
            .gc_slabs_before_with_live_refs_delayed_destroy(3, [2_u64])
            .unwrap();

        assert_eq!(report.removed_block_slab_ids, vec![0, 1]);
        assert_eq!(report.delayed_destroy_block_slab_ids, vec![0, 1]);
        assert_eq!(
            report.removed_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            report.removed_physical_bytes
        );
        assert_eq!(report.retained_block_slab_ids, vec![2, 3]);
        assert_eq!(report.retained_live_block_slab_ids, vec![2]);
        assert_eq!(report.retained_live_physical_bytes, b"live".len() as u64);
        assert_eq!(store.slab_ids().unwrap(), vec![2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0, 1]);
        let delayed_reports = store.delayed_destroy_slab_reports().unwrap();
        assert_eq!(delayed_reports.len(), 2);
        assert_eq!(delayed_reports[0].block_slab_id, 0);
        assert_eq!(delayed_reports[0].physical_bytes, b"current".len() as u64);
        assert!(delayed_reports[0].modified_unix_ms.is_some());
        assert_eq!(delayed_reports[1].block_slab_id, 1);
        assert_eq!(delayed_reports[1].physical_bytes, b"stale".len() as u64);
        assert!(delayed_reports[1].modified_unix_ms.is_some());

        let purge = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
        assert_eq!(purge.purged_block_slab_ids, vec![0, 1]);
        assert_eq!(
            purge.purged_physical_bytes,
            (b"current".len() + b"stale".len()) as u64
        );
        assert!(store.delayed_destroy_slab_ids().unwrap().is_empty());
        assert!(store.delayed_destroy_slab_reports().unwrap().is_empty());
        assert_eq!(store.slab_ids().unwrap(), vec![2, 3]);
    }

    /// A PUBLISHED LIVE TALLY MAKES `used_bytes` MEAN LIVE PAGE BYTES, AND THE FLOOR THEN BINDS.
    ///
    /// The arithmetic on its own, with the tally supplied directly rather than earned by a
    /// workload, because the question here is whether the MACHINERY works: given a slab that is
    /// 90% live, does the shipped 4,000 basis-point garbage floor exclude it?
    ///
    /// It does, and that is new. The figure `used_bytes` used to carry summed the file sizes of
    /// the slabs grouped under the same stored id that are NOT collectable -- and since the
    /// candidate filter is the exact negation of that test, and a stored id names exactly one
    /// slab, no candidate could ever contribute to its own used bytes. Every candidate read 0,
    /// 10,000 bp of garbage, and the floor excluded nothing at any setting.
    ///
    /// WHAT THIS DOES NOT SAY. It does not say the floor starts excluding slabs in a running
    /// store. `can_the_block_gc_garbage_floor_bind` is where that is measured, and the answer
    /// there is still no -- for a reason that lives in the CANDIDATE PREDICATE and not in this
    /// arithmetic: a collector candidate is a slab that no live page points at, so its maintained
    /// live bytes are genuinely zero. The two tests answer different halves of the same question,
    /// and both are needed: this one that the knob is real, that one that nothing in a running
    /// store currently presents it with a partially-live candidate.
    #[test]
    fn a_published_live_tally_makes_used_bytes_mean_live_block_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        // Equal sizes, so nothing below can be satisfied by the slabs merely differing in size --
        // which is the key the order actually used to fall through to.
        store.install_slab(0, &vec![b'a'; 1_000]).unwrap();
        store.install_slab(1, &vec![b'b'; 1_000]).unwrap();
        store.install_slab(2, &vec![b'c'; 1_000]).unwrap();
        store.install_slab(3, b"current").unwrap();

        // THE PREMISE, restated where it is about to be overturned.
        let before = store.gc_utility_candidates(3, Vec::<u64>::new()).unwrap();
        assert_eq!(
            before.len(),
            3,
            "three slabs are below the retention floor and collectable"
        );
        assert!(
            before
                .iter()
                .all(|candidate| candidate.used_bytes == 0
                    && candidate.utility_basis_points == 0),
            "with nothing published, every candidate reports the old zero: {before:?}"
        );

        store.publish_live_block_bytes(BTreeMap::from([
            (
                0_u64,
                BlockStoreSlabLive {
                    live_block_refs: 2,
                    live_bytes: 200,
                },
            ),
            (
                1_u64,
                BlockStoreSlabLive {
                    live_block_refs: 9,
                    live_bytes: 900,
                },
            ),
        ]));

        let after = store.gc_utility_candidates(3, Vec::<u64>::new()).unwrap();
        assert_eq!(after.len(), 3, "the same three candidates: {after:?}");
        let by_id = |block_slab_id: u64| {
            after
                .iter()
                .find(|candidate| candidate.block_slab_id == block_slab_id)
                .unwrap_or_else(|| panic!("candidate {block_slab_id} missing from {after:?}"))
        };
        assert_eq!(by_id(0).used_bytes, 200);
        assert_eq!(by_id(0).total_bytes, 1_000);
        assert_eq!(by_id(0).stale_bytes, 800);
        assert_eq!(by_id(0).utility_basis_points, 2_000);
        assert_eq!(by_id(1).used_bytes, 900);
        assert_eq!(by_id(1).utility_basis_points, 9_000);
        // Absent from the published tally is ZERO LIVE BYTES, not "unknown": the publisher walks
        // its whole index, so a slab it did not name holds nothing live.
        assert_eq!(by_id(2).used_bytes, 0);
        assert_eq!(by_id(2).utility_basis_points, 0);

        // HIGHEST GARBAGE FIRST, which is what the sort comment has always claimed and what a
        // uniformly zero first key could never deliver.
        assert_eq!(
            after
                .iter()
                .map(|candidate| candidate.block_slab_id)
                .collect::<Vec<_>>(),
            vec![2, 0, 1],
            "ascending live fraction is descending garbage: {after:?}"
        );

        let plan = store
            .gc_policy_plan(
                3,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::with_slab_garbage_floor(4_000, None),
            )
            .unwrap();
        // THE DENOMINATOR: three candidates were offered to the floor.
        assert_eq!(plan.candidate_count, 3, "{plan:?}");
        assert_eq!(
            plan.skipped_by_policy_count, 1,
            "the 90%-live slab is 1,000 bp of garbage and the floor is 4,000, so the floor \
             excludes it -- which it could not do for any slab, at any setting, before a tally \
             was published: {plan:?}"
        );
        assert_eq!(
            plan.selected_block_slab_ids,
            vec![2, 0],
            "and the two above the floor are selected, most-garbage first: {plan:?}"
        );
        assert_eq!(plan.candidate_used_bytes, 1_100);
        assert_eq!(plan.candidate_total_bytes, 3_000);

        // The read-only whole-store view agrees with what the plan was handed.
        let fractions = store.slab_live_fractions().unwrap();
        assert_eq!(fractions.len(), 4, "every slab, not just the candidates");
        let live_points = fractions
            .iter()
            .map(|fraction| fraction.live_basis_points)
            .collect::<Vec<_>>();
        assert_eq!(live_points, vec![2_000, 9_000, 0, 0], "{fractions:?}");
    }

    /// A slab the PUBLISHED TALLY still credits with live bytes is not reclaimed, however little
    /// of it is live.
    ///
    /// The tally's own header says it "only ever KEEPS a slab; it never grants permission to delete
    /// one". Its only consumer was the garbage floor, and a floor is a threshold: at the shipped
    /// 4,000 basis points a slab that is 20% live is 8,000 bp of garbage and clears it. So the
    /// tally could keep a slab only once it was more than 60% live.
    ///
    /// Two arms on ONE fixture, differing only in whether a tally was published, so the number the
    /// assertions move is the tally and not the store.
    #[test]
    fn a_slab_the_tally_still_credits_with_live_bytes_is_not_reclaimed() {
        fn arm(publish: bool) -> (BlockStoreGcReport, Vec<u64>) {
            let dir = tempfile::tempdir().unwrap();
            let store = BlockStore::new(dir.path());
            // Equal sizes: nothing below can be satisfied by the slabs merely differing in length.
            store.install_slab(0, &vec![b'a'; 1_000]).unwrap();
            store.install_slab(1, &vec![b'b'; 1_000]).unwrap();
            store.install_slab(2, b"current").unwrap();
            if publish {
                // Slab 0 is 20% live -- 8,000 bp of garbage, which clears the shipped 4,000 floor.
                // Slab 1 is named nowhere, which the publisher's contract reads as zero live bytes.
                store.publish_live_block_bytes(BTreeMap::from([(
                    0_u64,
                    BlockStoreSlabLive {
                        live_block_refs: 2,
                        live_bytes: 200,
                    },
                )]));
            }
            // The caller's live id set is EMPTY in both arms. That is the disagreement being
            // tested: the walk says nothing is live, the tally says slab 0 is.
            let report = store
                .gc_slabs_before_with_live_refs(2, Vec::<u64>::new())
                .unwrap();
            let left = store.slab_ids().unwrap();
            (report, left)
        }

        let (without, left_without) = arm(false);
        let (with, left_with) = arm(true);

        // THE DENOMINATOR, and the control. With no tally published the check reads zero and the
        // round reclaims both slabs below the floor, exactly as it did before.
        assert_eq!(
            without.removed_block_slab_ids,
            vec![0, 1],
            "the unpublished arm must reclaim both candidates, or the arm below proves nothing: \
             {without:?}"
        );
        assert!(
            without.retained_live_bytes_block_slab_ids.is_empty(),
            "nothing was published, so nothing can be held back by a tally: {without:?}"
        );
        assert_eq!(without.retained_live_bytes_physical_bytes, 0);
        assert_eq!(left_without, vec![2], "only the current slab survives: {left_without:?}");

        // THE TWO HALVES, ASSERTED SEPARATELY.
        //
        // Half one: the slab the tally credits is held back, named, and still on disk.
        assert_eq!(
            with.retained_live_bytes_block_slab_ids,
            vec![0],
            "the 20%-live slab must be refused, not merely under-selected: {with:?}"
        );
        assert_eq!(
            with.retained_live_bytes_physical_bytes, 1_000,
            "and reported at its own physical size: {with:?}"
        );
        assert!(
            with.retained_block_slab_ids.contains(&0),
            "a refused slab is retained: {with:?}"
        );
        assert!(
            left_with.contains(&0),
            "and its file is still in the store: {left_with:?}"
        );

        // Half two: the round still did its work on the slab the tally agrees is dead. A check
        // that refused everything would satisfy half one and be useless.
        assert_eq!(
            with.removed_block_slab_ids,
            vec![1],
            "the slab with no tallied live bytes is still reclaimed: {with:?}"
        );
        assert!(
            !left_with.contains(&1),
            "and its file is gone: {left_with:?}"
        );
        assert_eq!(left_with, vec![0, 2], "{left_with:?}");

        // The refusal is not double-counted as an ordinary live-set retention: the caller's id set
        // was empty in both arms, so that list must stay empty.
        assert!(
            with.retained_live_block_slab_ids.is_empty(),
            "the caller named no live slabs, so `retained_live` must not absorb the refusal: \
             {with:?}"
        );
    }

    #[test]
    fn utility_gc_selects_low_utility_stale_slabs_with_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"small").unwrap();
        store.install_slab(1, b"largest-stale-segment").unwrap();
        store.install_slab(2, b"live-segment").unwrap();
        store.install_slab(3, b"current-segment").unwrap();

        let candidates = store.gc_utility_candidates(3, [2_u64]).unwrap();
        assert_eq!(
            candidates
                .iter()
                .map(|candidate| candidate.block_slab_id)
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
        assert!(no_op.removed_block_slab_ids.is_empty());
        assert_eq!(no_op.removed_physical_bytes, 0);
        assert_eq!(store.slab_ids().unwrap(), vec![0, 1, 2, 3]);

        let report = store
            .gc_slabs_before_with_live_refs_utility(3, [2_u64], 1, true)
            .unwrap();
        assert_eq!(report.removed_block_slab_ids, vec![1]);
        assert_eq!(report.delayed_destroy_block_slab_ids, vec![1]);
        assert_eq!(
            report.removed_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(
            report.delayed_destroy_physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert_eq!(report.retained_block_slab_ids, vec![0, 2, 3]);
        assert_eq!(report.retained_live_block_slab_ids, vec![2]);
        assert_eq!(
            report.retained_live_physical_bytes,
            b"live-segment".len() as u64
        );
        assert_eq!(store.slab_ids().unwrap(), vec![0, 2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![1]);
        let delayed_reports = store.delayed_destroy_slab_reports().unwrap();
        assert_eq!(delayed_reports.len(), 1);
        assert_eq!(delayed_reports[0].block_slab_id, 1);
        assert_eq!(
            delayed_reports[0].physical_bytes,
            b"largest-stale-segment".len() as u64
        );
        assert!(delayed_reports[0].modified_unix_ms.is_some());
    }

    #[test]
    fn slab_garbage_floor_gates_reclaim_by_garbage_ratio() {
        // garbage-ratio GC conformance: reclaim is gated on a minimum slab garbage ratio
        // (garbage = 10_000 - slab live-fraction). Floor 0 (the default) reclaims every
        // eligible slab as before; a floor above a slab's garbage ratio excludes it.
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"stale-a").unwrap();
        store.install_slab(1, b"stale-b").unwrap();
        store.install_slab(2, b"kept-above-floor").unwrap();
        let floor_zero = store
            .gc_policy_plan(
                2,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::with_slab_garbage_floor(0, None),
            )
            .unwrap();
        assert_eq!(floor_zero.selected_block_slab_ids, vec![0, 1]);
        assert_eq!(floor_zero.skipped_by_policy_count, 0);
        let floor_impossible = store
            .gc_policy_plan(
                2,
                Vec::<u64>::new(),
                &BlockStoreGcPolicy::with_slab_garbage_floor(10_001, None),
            )
            .unwrap();
        assert!(floor_impossible.selected_block_slab_ids.is_empty());
        assert_eq!(floor_impossible.skipped_by_policy_count, 2);
    }

    /// The per-round budget is INERT on the production path, and that is now written down.
    ///
    /// `BlockStoreGcPolicy` can bound a round two ways -- a slab count and a physical-byte total
    /// -- and `policy_gc_plans_and_applies_byte_bounded_destroy` proves both work. Neither binds
    /// in production: `with_slab_garbage_floor` is the only constructor the scheduled cycle uses
    /// and it sets both to 0, which means "no limit". The operator path does not even reach the
    /// policy layer, and the purge takes no budget at all.
    ///
    /// A capability that is implemented, tested, and switched off everywhere it would matter is
    /// the hardest kind to notice: the tests are green, the struct looks complete, and nothing
    /// says the shipped path opted out. This test is what says it. It is deliberately an
    /// assertion of the CURRENT state rather than a fix -- changing a shipped default is a policy
    /// call, and the recommendation attached to this work is that the PURGE should be capped
    /// (unbounded unlinking under the store lock, linear in quarantine depth) while the
    /// COLLECTOR's budget should stay off until the victim ordering it would truncate is the
    /// intended one. Capping a wrong order makes the wrong slabs survive.
    ///
    /// So: when someone switches a budget on, this test fails, and the failure is the review
    /// prompt. It cannot go back to being inert quietly.
    #[test]
    fn the_production_gc_policy_ships_with_both_round_budgets_off() {
        let shipped = BlockStoreGcPolicy::with_slab_garbage_floor(
            crate::engine::reports::DEFAULT_BLOCK_GC_MIN_SLAB_GARBAGE_BASIS_POINTS,
            None,
        );
        assert_eq!(
            shipped.max_destroy_slabs, 0,
            "the slab-count budget is off on the only constructor the scheduled cycle uses; \
             turning it on is a policy change that must be made deliberately"
        );
        assert_eq!(
            shipped.max_destroy_physical_bytes, 0,
            "and so is the byte budget"
        );

        // The halves are separate: a budget being CONSTRUCTIBLE is not a budget being APPLIED,
        // and asserting only the first would leave the shipped path unexamined. This is the
        // second half -- the plumbing works, so 0 really does mean "chose not to", not "cannot".
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"a").unwrap();
        store.install_slab(1, b"b").unwrap();
        store.install_slab(2, b"current").unwrap();
        let bounded = store
            .gc_policy_plan(2, Vec::<u64>::new(), &BlockStoreGcPolicy::max_slabs(1))
            .unwrap();
        assert_eq!(
            bounded.selected_block_slab_ids.len(),
            1,
            "a budget of one really does bound a round: {bounded:?}"
        );
        assert_eq!(bounded.skipped_by_budget_count, 1);
        let unbounded = store.gc_policy_plan(2, Vec::<u64>::new(), &shipped).unwrap();
        assert_eq!(
            unbounded.selected_block_slab_ids.len(),
            2,
            "and the shipped policy bounds nothing: {unbounded:?}"
        );
        assert_eq!(unbounded.skipped_by_budget_count, 0);
    }

    #[test]
    fn policy_gc_plans_and_applies_byte_bounded_destroy() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        store.install_slab(0, b"small").unwrap();
        store.install_slab(1, b"largest-stale-segment").unwrap();
        store.install_slab(2, b"live-segment").unwrap();
        store.install_slab(3, b"current-segment").unwrap();

        let policy = BlockStoreGcPolicy {
            max_destroy_slabs: 2,
            max_destroy_physical_bytes: b"small".len() as u64,
            max_utility_score: Some(0),
            min_age_ms: Some(0),
            min_slab_garbage_basis_points: None,
        };
        let plan = store.gc_policy_plan(3, [2_u64], &policy).unwrap();
        assert_eq!(plan.retain_from_block_slab_id, 3);
        assert_eq!(plan.candidate_count, 2);
        assert_eq!(
            plan.candidate_physical_bytes,
            (b"small".len() + b"largest-stale-segment".len()) as u64
        );
        assert_eq!(plan.candidate_total_bytes, plan.candidate_physical_bytes);
        assert_eq!(plan.candidate_used_bytes, 0);
        assert_eq!(plan.candidate_stale_bytes, plan.candidate_physical_bytes);
        assert_eq!(plan.candidate_utility_basis_points, 0);
        assert_eq!(plan.selected_block_slab_ids, vec![0]);
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
                .map(|candidate| candidate.block_slab_id)
                .collect::<Vec<_>>(),
            vec![1, 0]
        );

        let report = store
            .gc_slabs_before_with_live_refs_policy(3, [2_u64], policy, true)
            .unwrap();
        assert_eq!(report.removed_block_slab_ids, vec![0]);
        assert_eq!(report.delayed_destroy_block_slab_ids, vec![0]);
        assert_eq!(report.retained_block_slab_ids, vec![1, 2, 3]);
        assert_eq!(store.slab_ids().unwrap(), vec![1, 2, 3]);
        assert_eq!(store.delayed_destroy_slab_ids().unwrap(), vec![0]);
    }

    /// Install, quarantine and purge, timed apart.
    ///
    /// A purge round used to unlink every quarantined slab with the store's lock held, so the
    /// round was unbounded in the amount of work it did. Whether that is the expensive part, or
    /// whether getting there is, is what this separates -- an earlier attempt timed all three
    /// together at twenty thousand slabs and did not finish in an hour.
    ///
    /// THE PURGE IS NOW DRAINED IN ROUNDS, because one call no longer finishes it. The longest
    /// single round is what the lock hold costs and is printed beside the total; a total alone
    /// would say the cap had made things slower while hiding that the thing it bounds got
    /// shorter. The round count is printed too: at 3,200 slabs and a budget of 1,000 it is the
    /// four that the arithmetic predicts, so a drain that quietly stopped early would show up
    /// here as a count that is too small rather than as a number nobody checks.
    #[test]
    fn quarantine_and_purge_timed_by_phase() {
        for slabs in [200u64, 800, 3_200] {
            let dir = tempfile::tempdir().unwrap();
            let store = BlockStore::new(dir.path());

            let started = std::time::Instant::now();
            for id in 0..slabs {
                store.install_slab(id, b"slab-contents").unwrap();
            }
            let install = started.elapsed().as_secs_f64() * 1e3;

            let started = std::time::Instant::now();
            let quarantined = store
                .gc_slabs_before_with_live_refs_delayed_destroy(slabs - 1, [slabs - 1])
                .unwrap()
                .delayed_destroy_block_slab_ids
                .len();
            let quarantine = started.elapsed().as_secs_f64() * 1e3;
            // DENOMINATOR: the purge below really has this much to drain.
            assert_eq!(
                quarantined as u64,
                slabs - 1,
                "the quarantine phase must really have set aside {} slabs",
                slabs - 1
            );

            let expected_rounds = quarantined.div_ceil(DELAYED_DESTROY_MAX_SLABS_PER_ROUND);
            let started = std::time::Instant::now();
            let mut destroyed = 0usize;
            let mut rounds = 0usize;
            let mut longest_round_ms = 0.0f64;
            loop {
                let round_started = std::time::Instant::now();
                let report = store.purge_delayed_destroy_slabs_older_than(0).unwrap();
                let round_ms = round_started.elapsed().as_secs_f64() * 1e3;
                longest_round_ms = longest_round_ms.max(round_ms);
                destroyed += report.purged_block_slab_ids.len();
                rounds += 1;
                if !report.budget_exhausted {
                    break;
                }
                assert!(
                    rounds <= expected_rounds + 4,
                    "the drain of {quarantined} slabs did not finish in {} rounds",
                    expected_rounds + 4
                );
            }
            let purge = started.elapsed().as_secs_f64() * 1e3;
            assert_eq!(
                destroyed, quarantined,
                "the rounds together must destroy every quarantined slab"
            );
            assert_eq!(
                rounds, expected_rounds,
                "and must take the number of rounds the budget predicts"
            );

            println!(
                "  {slabs:>5} slabs: install {install:>9.1} ms ({:>6.3} ms each)   quarantine \
                 {quarantine:>9.1} ms   purge {purge:>8.1} ms over {rounds} round(s), longest \
                 round {longest_round_ms:>7.1} ms ({destroyed} destroyed)",
                install / slabs as f64,
            );
        }
    }

    /// Installing slabs must not cost more as the store fills up.
    ///
    /// Writing the slab manifest costs the whole manifest, so writing it per install made
    /// installing n slabs cost n manifests: measured at 111.7 ms per install with two hundred slabs
    /// in the store and 270.7 ms with eight hundred. Written every so often instead, both are about
    /// 5.4 ms and the cost stops tracking the size of the store.
    ///
    /// Counted rather than timed. A duration would assert the right thing on an idle machine and
    /// something else entirely on a busy one; the number of manifest writes is the shape itself.
    #[test]
    fn installing_slabs_does_not_write_a_manifest_each_time() {
        let dir = tempfile::tempdir().unwrap();
        let store = BlockStore::new(dir.path());
        let slabs = 512u64;
        for id in 0..slabs {
            store.install_slab(id, b"slab-contents").unwrap();
        }
        let writes = store.stats().slab_manifest_writes;
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
        let reopened = BlockStore::new(dir.path());
        assert_eq!(
            reopened.slab_ids().unwrap().len(),
            slabs as usize,
            "every installed slab must still be there after a reopen"
        );
    }
}
