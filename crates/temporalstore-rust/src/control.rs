// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStoreSlabSummary, BlockStoreStats};
use crate::types::{BatchExecuteResponse, Command, ExecuteResponse};
use crate::types::{ShardId, Status};
use crate::wal::WriteAheadLogStats;
use matrixcache::CacheStats;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub version: u64,
    pub maxmemory_bytes: Option<u64>,
    #[serde(default)]
    pub tenant_name: Option<String>,
    pub read_qps: Option<u64>,
    pub write_qps: Option<u64>,
    #[serde(default)]
    pub table_read_qps: Option<u64>,
    #[serde(default)]
    pub table_write_qps: Option<u64>,
    #[serde(default)]
    pub tenant_read_qps: Option<u64>,
    #[serde(default)]
    pub tenant_write_qps: Option<u64>,
    #[serde(default)]
    pub extend_config: BTreeMap<String, String>,
    pub feature_max_size: usize,
    pub async_storage: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            maxmemory_bytes: None,
            tenant_name: None,
            read_qps: None,
            write_qps: None,
            table_read_qps: None,
            table_write_qps: None,
            tenant_read_qps: None,
            tenant_write_qps: None,
            extend_config: BTreeMap::new(),
            // Default retained points (and default read bound) per feature/sequence timeline.
            // Doubled from the historical 5000 to better serve long-sequence feature use cases
            // out of the box; still overridable per shard via set_config.
            feature_max_size: 10000,
            async_storage: false,
        }
    }
}

impl Config {
    /// Truthy check for an `extend_config` gate flag.
    pub fn flag(&self, name: &str) -> bool {
        self.flag_or(name, false)
    }

    /// `flag`, for a gate whose default is ON. An absent key takes `default`; a key that is
    /// present is read exactly as `flag` reads it, so an explicit false value opts out.
    pub fn flag_or(&self, name: &str, default: bool) -> bool {
        match self.extend_config.get(name) {
            None => default,
            Some(v) => matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes" | "enabled"
            ),
        }
    }

    /// Gate for the Control State rollup ladder (O(levels) sum-family window aggregates).
    /// Off by default so the live scan path is unchanged.
    pub fn control_rollup_enabled(&self) -> bool {
        self.flag("control_rollup")
    }

    /// Gate for coalesced Control State counter persistence: skip the per-write whole-series
    /// page rewrite and rely on the index snapshot + WAL replay (same durability model as
    /// control_state_changes/fol). Effective only with async_storage (WAL) on.
    ///
    /// DEFAULT ON. The rewrite it skips costs the length of the series on every increment --
    /// measured at 313,309 bytes per increment on a 3,200-point series against 2,459 coalesced,
    /// a 127x difference that grows without bound. What it relies on instead is covered by
    /// `control_state_coalesced_write_survives_restart_via_wal_replay`, which writes counters,
    /// reopens the engine and requires the counts to come back.
    ///
    /// Set the key to a false value to opt out; the async_storage gate still applies, so a
    /// deployment without a WAL keeps the per-write page rewrite either way.
    pub fn control_coalesce_persist_enabled(&self) -> bool {
        self.flag_or("control_coalesce_persist", true)
    }

    /// Gate for bounded distinct: convert oversized exact CHANGE sets to fixed-size HLL sketches
    /// (approximate distinct counts past the threshold). Off by default so distinct stays exact.
    pub fn control_distinct_sketch_enabled(&self) -> bool {
        self.flag("control_distinct_sketch")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub readonly: bool,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardRequest {
    pub shard_id: ShardId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetConfigRequest {
    pub shard_id: ShardId,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetConfigResponse {
    pub status: Status,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipUpdateRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub readonly: bool,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    #[serde(default = "default_membership_valid")]
    pub membership_valid: bool,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
    // True only while the shard's WAL is being replayed on load. keeps a partition in
    // PartitionLoadStage::LOADING (not serving) until the shard load and WAL replay
    // finishes; Rust must likewise refuse client commands during replay so a concurrent
    // write cannot interleave with replay and regress the WAL anchor (double-apply on the
    // next restart) or expose a stale mid-replay read. The replay thread itself bypasses
    // this gate via replaying_wal().
    #[serde(default)]
    pub recovering: bool,
}

fn default_membership_valid() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetInfoResponse {
    pub status: Status,
    pub info: Option<ShardInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectManagerStats {
    pub object_count: usize,
    pub block_ref_count: usize,
    pub dirty_object_count: usize,
    pub dirty_bucket_count: usize,
    pub routing_bucket_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatInfo {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_bucket: u32,
    pub end_routing_bucket: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardCanonicalStorageStats {
    pub page_index_entries: u64,
    /// The same live count as `page_index_entries` -- block and page are one concept under two
    /// names, and both are published while the rename is in flight. It is NOT a write counter;
    /// `block_writes` and `page_writes` are.
    pub block_index_entries: u64,
    pub object_index_entries: u64,
    /// The routing RANGE this shard covers -- the hash modulus, `end - start` -- and NOT a count
    /// of buckets that exist. A default shard covers the whole space, so this reads 4,294,967,295
    /// on an empty one. Read `bucket_index_resident_bytes_floor` below if you want a number that
    /// tracks what is actually resident.
    pub bucket_entries: u64,
    /// A FLOOR on what the bucket index costs in memory: one `BucketNode` per RESIDENT bucket,
    /// and nothing else.
    ///
    /// Published because every other memory number this engine emitted read the CACHE only -- the
    /// maintenance cycle's pressure gate and the `ShardLoad.memory_bytes` the metaserver balances
    /// on were both `cache.memory_bytes`. The bucket index was in neither, and it grows one entry
    /// per stored object, so "zero" was the one answer that is certainly wrong. Both of those now
    /// carry `bucket_index_resident_bytes` below as well; this floor stays node-only.
    ///
    /// A FLOOR, not the total: it counts the node structs and not the heap they point at (the
    /// shared object key, the map nodes). Measured over 96-byte values the true resident cost was
    /// roughly 3.5x this; `what_a_bucket_costs` prints both. Do not gate on it as though it were
    /// the whole figure -- it is published to make a growing index visible, not to size it.
    ///
    /// It also cannot move when a bucket is RELEASED, which is now a state a bucket can be in: a
    /// release frees the per-page entries and keeps the node, and this counts only nodes. The
    /// eviction gate uses `TemporalEngine::bucket_index_resident_bytes` instead, which counts both
    /// -- a gate reading this one would see a release free nothing and fire for ever.
    ///
    /// O(1): a count the stats path already holds, times a compile-time size.
    #[serde(default)]
    pub bucket_index_resident_bytes_floor: u64,
    /// What the resident bucket index costs RIGHT NOW: the nodes the floor above counts, PLUS one
    /// entry per page each bucket holds.
    ///
    /// Two numbers, deliberately, because they answer two different questions and one number
    /// cannot do both:
    ///
    ///  - `bucket_index_resident_bytes_floor` is STABLE under a release. It counts nodes, and a
    ///    release keeps every node, so it is the right thing to watch when you want "how big did
    ///    this index get" to be unaffected by whether maintenance has been through.
    ///  - this one MOVES under a release, because the per-page entries are exactly what a release
    ///    frees. That is what makes it usable as a pressure reading: acting on the pressure
    ///    reduces it, so the gate closes and the loop converges. A gate keyed on the floor would
    ///    see a release free nothing and ask again for ever.
    ///
    /// This is the same quantity `TemporalEngine::bucket_index_resident_bytes` returns and the
    /// same one `apply_storage_eviction` gates on; it is published here so the heartbeat and the
    /// metrics path can read it without taking the shard lock a second time.
    ///
    /// O(resident buckets): `page_index.len()` is O(1) per bucket, so this is one walk of the
    /// bucket map and no walk of the pages.
    #[serde(default)]
    pub bucket_index_resident_bytes: u64,
    /// How many buckets are actually resident -- `bucket_map.len()`.
    ///
    /// Distinct from `bucket_entries` above, which is the routing RANGE. The metric
    /// `slot_index_entry_count` was published from that range and therefore read
    /// 4,294,967,295 per shard no matter how large the index really was.
    #[serde(default)]
    pub bucket_index_resident_entries: u64,
    #[serde(alias = "storage_zone_count")]
    pub storage_slab_count: u64,
    #[serde(alias = "active_storage_zones")]
    pub active_storage_slabs: u64,
    #[serde(alias = "sealed_storage_zones")]
    pub sealed_storage_slabs: u64,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    #[serde(alias = "storage_zone_total_bytes")]
    pub storage_slab_total_bytes: u64,
    /// Physical bytes written per LOGICAL byte written, in PARTS PER MILLION (1.0x = 1_000_000).
    ///
    /// THE NAME ON THIS FIELD IS WRONG AND THE VALUE IS KEPT ANYWAY. `bytes_written /
    /// logical_bytes_written` is physical over logical, which is the STORAGE-write ratio. The
    /// logical-write ratio is logical bytes written over LIVE bytes -- a different numerator and a
    /// different denominator, so this is not an approximation of it. Read
    /// `storage_write_amplification_ppm` below, which carries the same value under the name that
    /// describes it, and prefer it in anything new.
    ///
    /// WHY THE WRONG NAME SURVIVES. This struct is `pub`, re-exported from `lib.rs`, and travels
    /// from a data node to the meta server inside `ShardStatInfo`. The field name is a serde key
    /// on that wire, so re-pointing it at a different quantity would leave an older peer's images
    /// decoding into the right shape with the wrong contents -- silently, which is the one failure
    /// mode worth spending a duplicated field to avoid. A `serde(alias)` cannot help here either:
    /// the old spelling and the correct quantity cannot both answer to this key.
    ///
    /// So the value here never changes. A reader that wants the ratio this name PROMISES wants
    /// `lifetime_write_amplification_ppm`, and the note on that field says why it is not spelled
    /// `logical_` anything.
    ///
    /// Zero when nothing logical has been written yet -- the convention the slab descriptor's own
    /// ratio already uses ("or 0 when the slab has no logical bytes"), so a fresh store reports 0
    /// rather than a division or a misleading 1.0.
    ///
    /// WHY IT IS PUBLISHED AT ALL. Amplification was measured in probes and described in prose and
    /// appeared in no report, so nothing watched it between measurements. A figure that is only
    /// ever produced by a probe is a figure nobody sees move.
    #[serde(default)]
    pub logical_write_amplification_ppm: u64,
    /// Physical bytes written per logical byte written, in PARTS PER MILLION -- THE CORRECT NAME
    /// for the quantity `logical_write_amplification_ppm` above has always carried.
    ///
    /// Byte-for-byte the same value as that field, deliberately: this is a renaming with the old
    /// spelling retained, not a second measurement. Below 1_000_000 means the store writes FEWER
    /// physical bytes than the workload handed it, which is what compression does -- a measured
    /// 136_718 ppm (0.137x) on a 2,000-key corpus of 256 B values.
    ///
    /// SO THIS RATIO HAS NO 1.0x FLOOR, and a gate that assumes one will fire on a healthy store.
    /// Its two halves are in different units on purpose: logical bytes are what a caller wrote and
    /// physical bytes are what landed, and the whole point of the ratio is the factor between
    /// them.
    #[serde(default)]
    pub storage_write_amplification_ppm: u64,
    /// Physical bytes written OVER THIS STORE'S LIFETIME per physical byte that is live NOW, in
    /// PARTS PER MILLION -- how many times over the store has written the data it is holding.
    ///
    /// `bytes_written` over the index's own live tally -- NOT over the slab summary's
    /// `live_physical_bytes`, which is slab-granular and cannot fall under rewrite. See the
    /// denominator note below; it is the whole reason this field is computable at all. Zero when
    /// the tally is not ready. Costs O(slabs), not O(objects), and no write-path accounting.
    ///
    /// WHAT IT IS FOR: rewrite pressure. A corpus written once reads 1_000_000 ppm however well it
    /// compresses; writing every key a second time reads 2_000_000. That is the question
    /// "amplification" is asked to answer, and neither write field above answers it --
    /// `storage_write_amplification_ppm` divides out by the compression factor, and
    /// `space_amplification_ppm` below has a numerator that is bytes RESIDENT rather than bytes
    /// written, so it cannot see a rewrite that reclaimed as it went.
    ///
    /// # THE DENOMINATOR IS BLOCK-GRANULAR, AND THE OBVIOUS ONE DOES NOT WORK
    ///
    /// `summarize_slabs` adds a slab's WHOLE `physical_bytes` to `live_physical_bytes` for every
    /// Active or Sealed slab, so "live" there means "in a slab not awaiting destruction". A slab
    /// counts fully live however many of its blocks are dead. That makes it track `bytes_written`
    /// to the byte under rewrite, and any ratio over it is PINNED AT 1.0x: measured at 2,000 keys
    /// overwritten five times, `bytes_written` and `live_physical_bytes` both read 2,680,000 B at
    /// every round while four fifths of those bytes were dead.
    ///
    /// `bucket_index.block_slab_live` sums `BlockAddress::length` over the blocks the INDEX still
    /// points at, and a block leaves the tally when its index entry does, so an overwrite retires
    /// the block it displaced. On that same corpus it holds at 536,000 B -- the live set's true
    /// size -- across all five rounds, and this ratio moves 1.000x -> 5.000x as it should.
    ///
    /// `the_live_denominator_must_fall_when_a_key_is_overwritten` holds both denominators side by
    /// side and fails if this field is ever re-pointed at the slab-granular one.
    ///
    /// # THIS RATIO HAS NO CEILING AND MUST NOT CARRY A FIXED ALERT THRESHOLD
    ///
    /// THE NUMERATOR IS A LIFETIME ACCUMULATOR AND THE DENOMINATOR IS A CURRENT READING. Nothing
    /// resets `bytes_written`: the only assignments to zero in the crate are local accumulators in
    /// `block_store/append.rs`, and no compaction, reclamation or dump path touches the counter. So
    /// on a store whose live set is roughly flat, this figure RISES WITHOUT LIMIT as a function of
    /// uptime, and a gate set at any multiple of 1.0x will eventually fire on a healthy store that
    /// has simply been up a long time. `lifetime_` is in the name for that reason.
    ///
    /// To make it gateable the numerator would have to be WINDOWED -- reset where the live set is
    /// rewritten wholesale, so the ratio means "written since the last compaction per byte live".
    /// That is a change to the write path's accounting, not a change to this arithmetic, and it is
    /// not made here. Read this field as a cumulative history, and compare it against ITSELF over
    /// time rather than against a constant.
    ///
    /// WHY IT IS NOT CALLED `logical_write_amplification_ppm`, which is the spelling the ratio
    /// logical-bytes-written over live-bytes would carry. That ratio needs both halves in the same
    /// unit to mean anything, and the live half we can produce today is COMPRESSED: an object
    /// record carries `address.length()`, the stored length, and no uncompressed size. Dividing
    /// uncompressed bytes written by compressed bytes live multiplies the real figure by the
    /// compression factor -- it reads 7.314x on a write-once corpus that has no write amplification
    /// at all, where this field reads 1.000x. Matching the units the other way, by adding an
    /// uncompressed size to the record, costs 4-8 B per object against a record the footprint
    /// campaign cut to 16, so it is a decision with a price rather than an oversight. Both halves
    /// here are stored bytes, so the compression factor cancels instead of dividing in.
    #[serde(default)]
    pub lifetime_write_amplification_ppm: u64,
    /// Physical bytes the store occupies per physical byte that is live, in PARTS PER MILLION.
    ///
    /// `total_known_physical_bytes / live_physical_bytes` from the slab summary. What pushes it
    /// above 1_000_000 is reclaimable space a compaction has not taken yet, which is the thing this
    /// ratio exists to make visible. Zero when nothing is live.
    ///
    /// NOT THE SAME QUANTITY AS THE SLAB UTILISATION ALREADY ON A DESCRIPTOR, which is per slab and
    /// expressed in basis points; this is the whole store in ppm.
    ///
    /// # ITS DENOMINATOR IS SLAB-GRANULAR, SO IT CANNOT SEE A DEAD BLOCK INSIDE A LIVE SLAB
    ///
    /// This ratio computes what its name says, and its VALUE is nonetheless pinned near 1.0x for
    /// the ordinary shape of space amplification. `live_physical_bytes` counts an Active or Sealed
    /// slab's whole `physical_bytes`, so the figure can only move once an ENTIRE slab reaches
    /// delayed-destroy. Measured at 2,000 keys overwritten five times: 1.000x at every round, with
    /// `reclaimable_physical_bytes` never leaving zero, where the block-granular figure is 5.0x.
    ///
    /// IT CAN MOVE, BUT ONLY AFTER A COMPACTION, NOT AFTER THE REWRITE THAT CAUSED THE WASTE. The
    /// one state that shifts it is a whole slab reaching `DelayedDestroy`, and only the compaction
    /// phase produces that -- so the figure lags the condition it describes by however long it
    /// takes a collector to quarantine a slab, and then by `DELAYED_DESTROY_MIN_AGE_MS` before the
    /// space is actually returned.
    ///
    /// SO DO NOT PUT AN ALERT ON THIS EXPECTING IT TO CATCH RECLAIMABLE SPACE. It reports slab
    /// states, not how much of the data is dead. The block-granular quantity is
    /// `bucket_index.block_slab_live`, which `lifetime_write_amplification_ppm` above uses; moving
    /// THIS field onto it would change a published value that consumers already read, so it is
    /// recorded here rather than done silently.
    #[serde(default)]
    pub space_amplification_ppm: u64,
    /// Bytes still on disk per stored byte the INDEX still points at, in PARTS PER MILLION.
    ///
    /// `non_purged_physical_bytes / live_block_bytes`. This is the figure
    /// [`Self::space_amplification_ppm`] above cannot produce, published beside it rather than
    /// replacing it: both of that field's halves are blind to reclamation, and re-pointing a serde
    /// key that travels to the meta server would decode into the right shape with the wrong
    /// contents on an older peer.
    ///
    /// # BOTH HALVES MOVE IN THE DIRECTION THE RATIO CLAIMS TO MEASURE
    ///
    /// The numerator drops when a purge deletes a slab's files, because it excludes purged slabs.
    /// The denominator drops when an overwrite retires the block its index entry pointed at,
    /// because it is the index's own per-slab tally rather than a slab-file size. Measured over
    /// four rewrites of every key: **1.000x -> 5.000x**, where the pinned field held 1.000x at
    /// every round. See `a_space_ratio_whose_numerator_falls`.
    ///
    /// # ITS FLOOR IS EXACTLY 1.0x, AND BOTH TERMS ARE STORED BYTES
    ///
    /// AN EARLIER VERSION OF THIS NOTE SAID THE FLOOR WAS ABOVE 1.0x, on the reasoning that the
    /// numerator is file bytes and the denominator block payload bytes, so framing would sit in one
    /// half only. **The arms that measure it say otherwise**, and they said so in output that was
    /// already in front of the claim: `a_space_ratio_whose_numerator_falls` reports `after 1
    /// write(s) 268000 268000 1.000x`, and `the_live_and_stale_shares_are_a_partition` reports a
    /// stale share of exactly 0.00% on the same store.
    ///
    /// The terms agree because [`Self::live_block_bytes`] sums `BlockAddress::length` -- what was
    /// STORED, after compression and including the block envelope -- and
    /// `total_known_physical_bytes` is the file size of those same bytes. Both sides are stored
    /// bytes, so this ratio is unit-matched and reads exactly 1.000x on a store with nothing dead
    /// in it.
    ///
    /// FRAMING IS REAL BUT BELONGS TO A DIFFERENT DENOMINATOR: 536,000 B on disk for 512,000 B of
    /// USER bytes, 1.047x, which appears when stored bytes are divided by PRE-COMPRESSION bytes.
    /// That is the mixed-unit division these fields exist to avoid, and it does not arise between
    /// two stored-byte terms.
    ///
    /// Zero when the live tally has not been derived yet, which is the same condition under which
    /// [`Self::lifetime_write_amplification_ppm`] reports 0 ppm, so the two agree about when they
    /// are unavailable instead of one of them reading as a real figure.
    #[serde(default)]
    pub block_space_amplification_ppm: u64,
    #[serde(alias = "storage_zone_used_bytes")]
    pub storage_slab_used_bytes: u64,
    #[serde(alias = "storage_zone_stale_bytes")]
    pub storage_slab_stale_bytes: u64,
    /// How many times the block store was CALLED to read, summed over the shard's lifetime.
    ///
    /// One per call, not one per byte and not one per page: a four-megabyte object read cold
    /// counts one, the same as a five-hundred-byte one, and a read served from the memory or
    /// disk cache counts none because it never reaches the store. Incremented at three sites
    /// in `block_store/read.rs` -- `read`, `read_range`, `read_logical_range`.
    pub page_reads: u64,
    /// How many times the block store was CALLED to append, summed over the shard's lifetime.
    /// Two increment sites, both in `block_store/append.rs`.
    pub page_writes: u64,
    /// The SAME number as `page_reads`, not a second measurement.
    ///
    /// Block and page are one concept under two names while the rename is in flight, and
    /// `engine/persistence.rs` assigns both fields from `block_store.reads` four lines apart.
    /// A committed fixture once recorded `page_reads: 4` beside `block_reads: 2`, which reads
    /// as two measurements and would make a rename halve a real number; no code path can
    /// produce that, and `engine::tests::page_and_block_counter_ratio` asserts the ratio is
    /// one over nine shaped workloads so the claim cannot rot back in.
    pub block_reads: u64,
    /// The SAME number as `page_writes`, on the same terms as `block_reads` above.
    pub block_writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    /// User bytes offered to the store, cumulative: the DENOMINATOR of
    /// [`Self::storage_write_amplification_ppm`].
    ///
    /// PUBLISHED SO THE RATIO CAN BE CHECKED. That ratio is `bytes_written /
    /// logical_bytes_written`, and until now only the numerator was a field -- this quantity
    /// appeared in this file exactly once, inside a doc comment. A consumer reading 136718 ppm
    /// could neither re-derive it nor tell a compression change from an accounting change.
    ///
    /// It is counted before compression, which is what makes it the right denominator for that
    /// ratio and the WRONG one for anything measuring bytes on disk. A block's length is what was
    /// stored, after compression; dividing one by the other yields the compression factor
    /// multiplied by whatever the ratio claimed to measure, which is how a payload write figure
    /// came to read 7.314x where the rewrite pressure was 1.0x.
    #[serde(default)]
    pub logical_bytes_written: u64,
    /// Stored bytes the index still points at: the DENOMINATOR of
    /// [`Self::lifetime_write_amplification_ppm`].
    ///
    /// From the maintained per-slab tally, which retires a block when its index entry is replaced,
    /// so an overwrite decrements it. Zero when that tally has not been derived yet -- the same
    /// condition under which the ratio over it reports 0 ppm, so the two agree about when they are
    /// unavailable rather than one of them reading as a real figure.
    ///
    /// NOT THE SAME AS [`Self::storage_slab_used_bytes`], which is slab-granular: that counts a
    /// live slab's whole file however many of its blocks are dead, and therefore tracks bytes
    /// written rather than bytes live. Both are published because they answer different questions
    /// and neither derives the other.
    #[serde(default)]
    pub live_block_bytes: u64,
    /// Slab-file bytes that have NOT been purged: the NUMERATOR of
    /// [`Self::block_space_amplification_ppm`].
    ///
    /// `total_known_physical_bytes - purged_physical_bytes`. Published for the same reason the two
    /// denominators above it are: a consumer reading a ppm figure it cannot re-derive from
    /// published counters cannot tell a real change from an accounting change.
    ///
    /// NOT THE SAME AS [`Self::storage_slab_total_bytes`], which is `total_known_physical_bytes`
    /// and keeps counting a purged slab whose files are gone. The difference between the two is
    /// exactly the space a purge has already returned.
    #[serde(default)]
    pub non_purged_physical_bytes: u64,
    /// The LIVE share of the stored image, in PARTS PER MILLION.
    ///
    /// `live_block_bytes / non_purged_physical_bytes`. This and
    /// [`Self::stale_bytes_ppm`] are a PARTITION of the bytes on disk, not two independent ratios:
    /// they are computed over the same whole and **sum to 1,000,000** within integer floor
    /// rounding.
    ///
    /// # WHY A PARTITION AND NOT A FIFTH AMPLIFICATION RATIO
    ///
    /// An amplification ratio divides one quantity by another and nothing constrains the answer, so
    /// a term that measures the wrong thing is invisible in the published figure. A partition
    /// constrains itself. Every one of this struct's four amplification ratios could carry a wrong
    /// numerator without any identity over the set failing -- one of them did, for as long as it
    /// did -- and these two cannot.
    ///
    /// Zero when the live tally has not been derived yet, on the same terms as
    /// [`Self::live_block_bytes`].
    #[serde(default)]
    pub live_bytes_ppm: u64,
    /// The STALE share of the stored image, in PARTS PER MILLION.
    ///
    /// `(non_purged_physical_bytes - live_block_bytes) / non_purged_physical_bytes`. Bytes still on
    /// disk that the index no longer points at: the space a compaction could return.
    ///
    /// DERIVED FROM THE TWO PUBLISHED TERMS RATHER THAN COUNTED SEPARATELY, deliberately. A byte on
    /// disk the index does not point at is dead, and deriving it this way means the partition cannot
    /// disagree with the terms it is built from. The separately counted figure already exists as
    /// [`Self::storage_slab_stale_bytes`] and reads zero for the ordinary shape of waste, because it
    /// only moves once an ENTIRE slab is quarantined -- so the two answer different questions and
    /// neither derives the other.
    ///
    /// # THE FLOOR IS EXACTLY ZERO, BECAUSE BOTH TERMS ARE STORED BYTES
    ///
    /// On a store with nothing dead in it this reads **0 ppm** and the live share reads 1,000,000:
    /// [`Self::live_block_bytes`] sums `BlockAddress::length`, which is what was stored, and the
    /// whole is the file size of those same bytes. Measured at exactly 0.00% stale on a freshly
    /// written corpus by `the_live_and_stale_shares_are_a_partition`, which pins the equality.
    ///
    /// So a non-zero reading here is dead data and nothing else -- no framing term to subtract
    /// first, which is what makes this the best-conditioned figure in the set.
    #[serde(default)]
    pub stale_bytes_ppm: u64,
    pub append_watermark: u64,
    pub compaction_watermark: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub total_records: usize,
    pub string_records: usize,
    pub hash_records: usize,
    pub set_records: usize,
    pub feature_records: usize,
    pub sequence_records: usize,
    pub control_state_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
    #[serde(alias = "partition_info")]
    pub shard_stat_info: ShardStatInfo,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    pub cache: CacheStats,
    #[serde(default, alias = "page_store")]
    pub block_store_compat: BlockStoreStats,
    #[serde(default)]
    pub block_store_slabs_compat: BlockStoreSlabSummary,
    pub block_store: BlockStoreStats,
    #[serde(default)]
    pub block_store_slabs: BlockStoreSlabSummary,
    pub write_ahead_log: WriteAheadLogStats,
}

impl ShardStats {
    /// What this shard costs in memory, as reported to the metaserver in `ShardLoad.memory_bytes`.
    ///
    /// Cache bytes PLUS resident bucket-index bytes. It was cache bytes alone, and the metaserver
    /// balances placement on the sum of this across a datanode's shards -- so a shard whose index
    /// is large and whose cache is small read as holding almost no memory. That is not an exotic
    /// shape: it is every cold shard with a big corpus, and it is exactly the shard that most
    /// wants to be moved or asked to evict. The balancer instead read it as unloaded.
    ///
    /// The index term is `bucket_index_resident_bytes`, the MOVING figure, and not
    /// `bucket_index_resident_bytes_floor`, the node-only one. A release frees the per-page
    /// entries and keeps the nodes, so the moving figure falls when maintenance relieves the
    /// shard and the next heartbeat reports the relief; the floor would be a load report that can
    /// only ever rise.
    ///
    /// A method rather than an expression at the heartbeat, so the guard on this arithmetic
    /// covers the code the server runs instead of a copy of it.
    pub fn load_memory_bytes(&self) -> u64 {
        (self.cache.memory_bytes as u64).saturating_add(self.storage.bucket_index_resident_bytes)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetStatsResponse {
    pub status: Status,
    pub stats: Option<ShardStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedExecuteRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedExecuteResponse {
    pub status: Status,
    pub response: ExecuteResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedBatchExecuteRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedBatchExecuteResponse {
    pub status: Status,
    pub response: BatchExecuteResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Index,
    IndexLog,
    Wal,
    Block,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReadRequest {
    pub shard_id: ShardId,
    pub stream_kind: StreamKind,
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReadResponse {
    pub status: Status,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanStreamRequest {
    pub shard_id: ShardId,
    pub stream_kind: StreamKind,
    #[serde(rename = "page_segment_id")]
    pub block_slab_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanStreamResponse {
    pub status: Status,
    pub records: Vec<StreamRecord>,
    pub end_of_stream: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The two stream requests still write, and still read, their durable field name.
    ///
    /// `a_durable_name_is_never_quietly_dropped` watches the NAME `page_segment_id`, and that name
    /// is not what is at risk here: seven other structs in the crate spell it, so the list would
    /// go on finding it even if both of these stopped. What a list of names structurally cannot
    /// say is that a PARTICULAR struct still carries one -- and these two are the ones on the
    /// wire, parsed straight out of a request body by `/read_stream` and `/scan_stream`, where
    /// losing the rename means an existing client's body quietly stops binding the field it names
    /// and the read starts at slab zero.
    ///
    /// The two are asserted SEPARATELY. They are separate structs carrying separate attributes,
    /// and one round trip over both passes while one of them is wrong.
    #[test]
    fn the_stream_requests_keep_their_durable_field_name() {
        // NON-VACUITY: the bodies below are the only place the durable name appears, so a decode
        // that ignored it would leave the slab id at its default rather than at this value.
        const SLAB: u64 = 7;
        assert_ne!(SLAB, 0, "the durable name must carry a value a default cannot produce");

        // --- StreamReadRequest, on its own.
        let read = StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Wal,
            block_slab_id: SLAB,
            offset: 64,
            size: 128,
        };
        let read_json = serde_json::to_value(&read).expect("a request serializes");
        assert_eq!(
            read_json["page_segment_id"], SLAB,
            "StreamReadRequest no longer writes its durable field name: {read_json}"
        );
        assert!(
            read_json.get("block_slab_id").is_none(),
            "StreamReadRequest wrote the in-memory spelling, which no client sends: {read_json}"
        );
        let read_body = serde_json::json!({
            "shard_id": 1,
            "stream_kind": "wal",
            "page_segment_id": SLAB,
            "offset": 64,
            "size": 128,
        });
        assert!(
            read_body.get("block_slab_id").is_none(),
            "the body under test must name the field only the durable way"
        );
        let read_back: StreamReadRequest =
            serde_json::from_value(read_body).expect("a client body parses");
        assert_eq!(
            read_back, read,
            "a /read_stream body naming page_segment_id no longer binds the slab id"
        );

        // --- ScanStreamRequest, asserted on its own.
        let scan = ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Wal,
            block_slab_id: SLAB,
            start_offset: 0,
            end_offset: 4096,
            max_bytes: 1024,
        };
        let scan_json = serde_json::to_value(&scan).expect("a request serializes");
        assert_eq!(
            scan_json["page_segment_id"], SLAB,
            "ScanStreamRequest no longer writes its durable field name: {scan_json}"
        );
        assert!(
            scan_json.get("block_slab_id").is_none(),
            "ScanStreamRequest wrote the in-memory spelling, which no client sends: {scan_json}"
        );
        let scan_body = serde_json::json!({
            "shard_id": 1,
            "stream_kind": "wal",
            "page_segment_id": SLAB,
            "start_offset": 0,
            "end_offset": 4096,
            "max_bytes": 1024,
        });
        assert!(
            scan_body.get("block_slab_id").is_none(),
            "the body under test must name the field only the durable way"
        );
        let scan_back: ScanStreamRequest =
            serde_json::from_value(scan_body).expect("a client body parses");
        assert_eq!(
            scan_back, scan,
            "a /scan_stream body naming page_segment_id no longer binds the slab id"
        );
    }
}
