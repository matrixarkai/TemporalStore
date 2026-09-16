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
