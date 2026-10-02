// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::block_store::BlockAddress;
use crate::types::{CommandResponse, ControlStateSelectionType, FeaturePoint, ShardId};

use super::control_rollup::RollupEntry;
use super::hll::Hll;
use super::hash_field_map::HashFieldMap;

/// In-memory, coalesced summary-dirty entry.
///
/// One entry per dirty object key (`ctx:dirty:{tenant}:{node}`). Repeated
/// `ContextMarkSummaryDirty` commands for the same node update this single entry
/// in place instead of appending a new persisted marker, so the number of dirty
/// records is bounded by the number of distinct dirty nodes rather than the number
/// of events. `propagate_depth` keeps the deepest parent-propagation requested and
/// `event_time_ms` bounds track the earliest/latest events that made the node dirty.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct ContextDirtyEntry {
    // `tenant_hash` is populated for embedding-dirty entries so the all-pending
    // drain scan (which spans every tenant on the shard) can recover each node's
    // tenant. It is left 0 for summary-dirty entries, whose queries are always
    // per-node and already carry the tenant.
    pub(super) tenant_hash: u64,
    pub(super) node_hash: u64,
    pub(super) first_event_time_ms: u64,
    pub(super) last_event_time_ms: u64,
    pub(super) reason: u32,
    pub(super) propagate_depth: u32,
    pub(super) mark_count: u64,
}

/// Where a WAL-resident block's bytes are: the log id of the record carrying it, and that
/// record's sequence (which is what log reclaim reasons about).
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct WalResidentBlock {
    pub(super) log_id: u64,
    pub(super) sequence: u64,
}

/// Two log coordinates. One per block whose only durable copy is a log record.
const _: () = assert!(std::mem::size_of::<WalResidentBlock>() == 16);

/// The key type the three bare-address resident model maps are keyed by.
///
/// `Box<str>` rather than `String`. A model-map key is written once when the key is first seen and
/// is never appended to, so the capacity word a `String` carries is eight bytes per entry that
/// nothing can ever use -- and the table holds a slot for every bucket, not for every live key, so
/// those eight bytes are charged at the table's occupancy rather than at the key count. Measured on
/// the counting allocator at the shipped routing range: 1.434 slots a live key at 40,000 keys, so
/// 13.1 B/key off each map's chunk column (112.4 -> 99.2 B/key on the string map).
///
/// The three are `strings`, `control_state_blocks` and `context_nodes` -- every resident map
/// whose value is a bare [`BlockAddress`] rather than a nested container. `address_footprint`
/// iterates the three together in one array, so they have to agree on their key type; that
/// array is the reason the class is these three and not just the one that was measured.
///
/// EVERY READ SHAPE IS UNCHANGED, which is why this is a key-type change and not a surface change.
/// `Box<str>: Borrow<str>`, so `get`, `get_mut`, `contains_key` and `remove` still take `&str` and
/// every call site that passes one compiles untouched. `keys()` yields `&Box<str>`, which derefs to
/// `&str`. Only the sites that CREATE a key change, and they change from `.clone()` to `.into()`.
///
/// NO STORED BYTES MOVE. The shard index is `serde_json`, where a map key is a string either way;
/// `the_string_map_key_is_the_same_json_either_way` round-trips a snapshot to prove it rather than
/// asserting it, so this carries no index format stamp.
pub(super) type ModelKey = Box<str>;

/// The eight bytes, as a guard rather than as a sentence in the comment above: a `String` put back
/// here would be a silent per-live-key regression, and nothing else in the suite would notice.
const _: () = assert!(std::mem::size_of::<ModelKey>() == 16);
const _: () = assert!(std::mem::size_of::<ModelKey>() + 8 == std::mem::size_of::<String>());

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct ShardState {
    /// On-disk shape of this index. 0 means "written before the stamp existed".
    ///
    /// The index is a serialized ShardState, so a change to what a field MEANS -- not just its
    /// type -- makes an older file decode into the right shape with the wrong contents, silently.
    /// That is exactly what keying context_events by event id did: a pre-rekey index decodes with
    /// timeline keys sitting in the event-id slot and an empty context_event_timeline, so every
    /// time-windowed read returns nothing and no error is raised anywhere.
    ///
    /// Stamped on write by serialize_index; checked on load by load_index_inner, which refuses a
    /// stale index rather than trusting it -- a refusal falls back to WAL replay, which rebuilds
    /// both maps correctly through insert_context_event_views.
    #[serde(default)]
    pub(super) index_format_version: u32,
    /// Blocks whose only durable copy is a WAL record, and which record holds each one.
    ///
    /// The resolver's table is process-local, so after a restart it is empty: the served index
    /// still points at a synthetic address and nothing can turn it back into bytes until a full
    /// replay re-derives the block. Recording the log id HERE means the mapping travels with the
    /// index that depends on it, and a reload hands it straight back.
    ///
    /// A stale entry costs a miss, never wrong bytes. The resolver reads the record at that log
    /// id and looks for the object inside it, so a reclaimed record or a superseded block simply
    /// is not found, and the read falls through exactly as it did before this existed.
    #[serde(default)]
    pub(super) wal_resident_blocks: BTreeMap<u64, WalResidentBlock>,
    /// Routing buckets whose derived runtime flags may be stale, so a write can refresh the
    /// buckets it touched instead of sweeping the shard.
    ///
    /// `refresh_bucket_runtime_flags` recomputes `in_memory` / `deleted` / `dirty` / `ttl_ms` /
    /// `layout` for EVERY bucket, and the single-write path called it on every write. Each bucket
    /// costs `O(its pages)`, so the sweep is `O(total pages)` per write and ingestion is quadratic
    /// in the corpus. Note that the routing-slot range does not soften this: fewer slots means
    /// fewer, larger buckets and the same total block count.
    ///
    /// Recorded where the routing bucket is already known -- the bucket-index upsert, the removal
    /// paths, and the async dirty mark -- rather than inferred from the key, because a stored
    /// address may carry an explicit routing bucket that disagrees with `block_routing_bucket`.
    ///
    /// Not persisted: on load this is empty, and every load and recovery path already runs the
    /// full sweep, so a fresh process starts from fully recomputed flags.
    #[serde(skip)]
    pub(super) buckets_pending_flag_refresh: BTreeSet<u32>,
    /// Deadlines, kept in key order for the point lookup `ttl_ms` needs.
    ///
    /// MUTATE THIS THROUGH `set_expiry` / `clear_expiry`, never directly: `expiry_by_deadline`
    /// below mirrors it and the two must agree. `the_two_expiry_indexes_agree` fails if they
    /// drift.
    pub(super) expires_at_ms: BTreeMap<String, u64>,
    /// The same deadlines, DEADLINE-ordered, so the keys that are due are a PREFIX.
    ///
    /// Walking `expires_at_ms` in key order to find due keys made time-to-expire a function of
    /// the keyspace rather than of how many keys were actually due: measured at
    /// keyspace/scan_budget rounds, so ten expired keys behind 10,000 live ones survived more
    /// than sixty rounds. Ordered by deadline, finding them costs the number that are due.
    ///
    /// The same pair as `SeenSet`'s `by_member`/`by_time`, for the same reason.
    ///
    /// Derived, never persisted: it is rebuilt from `expires_at_ms` on first use after a load,
    /// so no snapshot or wire format changes and an older snapshot needs no migration. The repair
    /// (`ensure_expiry_order`) fires ONLY on an entirely empty map -- a mirror left populated and
    /// wrong is never repaired, and shows up only as keys that silently never expire.
    ///
    /// The `engine::tests::expiry_scale` module holds the guards: that a round finds what is due
    /// at a hundred-thousand-key shard, that this view is rebuilt on load, manifest install and
    /// WAL replay, and -- at length -- why a bounded round-robin expiry cursor should not replace
    /// an ordered index here.
    #[serde(skip)]
    pub(super) expiry_by_deadline: BTreeMap<(u64, String), ()>,
    pub(super) strings: HashMap<ModelKey, BlockAddress>,
    // Rebuildable from the durable bucket/block index on load; do not duplicate in checkpoints.
    //
    // THE INNER CONTAINER IS A SORTED VECTOR, NOT A TABLE AND NOT A B-TREE, and it is the only one
    // of the eighteen nested model maps that is either. Measured on the counting allocator, a hash
    // of ONE field -- which is every context node in a store, because `write_context_node` files
    // its block under the single constant `CONTEXT_NODE_FIELD` -- cost 272 chunk bytes as a table
    // and 560 as a `BTreeMap`, against 64 as an exact-sized vector. Matching the seventeen ordered
    // siblings was measured and LOST, by more than 2x at the occupancy that is 100% of the product
    // write path. See `engine::hash_field_map` for the trade and
    // `engine::tests::model_map_container_cost` for the numbers.
    //
    // The wire shape is unchanged: `HashFieldMap` serializes to and from the same MAP the
    // `HashMap` did, so an index written before this field became `skip_serializing` still decodes.
    #[serde(default)]
    pub(super) hashes: HashMap<String, HashFieldMap>,
    #[serde(default, with = "super::set_index_serde")]
    pub(super) sets: HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>,
    /// Windowed seen-sets backing idempotency keys: member -> when it was last seen, plus
    /// the same entries time-ordered so expiry pops from the front in bounded steps. Like the
    /// buckets, no blocks back this state -- it persists with the shard index snapshot, and a
    /// crash forgetting a window's worth of members re-admits a duplicate rather than
    /// dropping a legitimate first ingest.
    #[serde(default, with = "super::seen_index_serde")]
    pub(super) seen: HashMap<String, SeenSet>,
    /// Token buckets: key -> (tokens remaining, last refill ms). Config rides each command,
    /// never the store -- the caller owns policy, which is exactly what a quota layer wants.
    /// No blocks back this state: it persists only with the shard index snapshot, so a crash
    /// refills every bucket to capacity. That direction is deliberate and documented -- a
    /// limiter that briefly over-admits after a crash beats one that starves recovered
    /// tenants on stale counts.
    #[serde(default)]
    pub(super) buckets: HashMap<String, (f64, u64)>,
    /// Sorted sets: member -> (total-order score bits, element block). The score-ordered view
    /// is derived per query -- V1 accepts the per-range sort; the upgrade path is a second
    /// in-memory map rebuilt at load, never a second persisted structure (the index component
    /// already encodes score-then-member, so recovery has the order for free).
    #[serde(default, with = "super::zset_index_serde")]
    pub(super) zsets: HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>,
    /// Redis-style lists: element blocks keyed by a signed sequence -- left pushes walk the
    /// low end down, right pushes walk the high end up, so both ends are O(log n) and the
    /// BTree's order IS the list's order.
    #[serde(default)]
    pub(super) lists: HashMap<String, BTreeMap<i64, BlockAddress>>,
    pub(super) features: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // Sequence data is now stored in `features` (thin-layer fold: Sequence is Feature
    // with a typed row codec over identical timestamped-KV storage). This field is
    // retained only to fold a pre-fold on-disk index that still carries a `sequences`
    // map into `features` at load time (see load_index); new code never writes it, so
    // it serializes away once empty.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub(super) sequences: HashMap<String, BTreeMap<u64, BlockAddress>>,
    pub(super) control_state: HashMap<String, BTreeMap<u64, i64>>,
    #[serde(default)]
    pub(super) control_state_blocks: HashMap<ModelKey, BlockAddress>,
    #[serde(default)]
    pub(super) control_state_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>,
    // Bounded distinct: per (key, bucket) HyperLogLog sketch. A bucket lives in EITHER
    // control_state_changes (exact set, small) OR here (fixed-size HLL, converted once the
    // exact set exceeds the threshold), so high-cardinality distinct counts stay memory-bounded.
    // Serde-default + durable (rides the index snapshot + WAL, like control_state_changes).
    #[serde(default)]
    pub(super) control_state_change_sketch: HashMap<String, BTreeMap<u64, Hll>>,
    #[serde(default, alias = "control_state_fol")]
    pub(super) control_state_selection: HashMap<String, ControlStateSelectionValue>,
    // UUID idempotency ledger for control-state writes: uuid -> expiry_ms. Mirrors
    // the control_state 300s dedup window so at-least-once queue replays do not
    // double-count. Lazily garbage-collected on write; in-memory + serde-default so
    // it is rebuilt via command replay and never blocks recovery.
    #[serde(default)]
    pub(super) control_state_uuid: HashMap<String, u64>,
    // Derived, in-memory rollup ladder over `control_state` for O(levels) sum-family
    // window aggregates (frequency caps / long-window counts). Serde-skipped: rebuilt
    // lazily from the authoritative counter series and never blocks recovery. Gated by
    // Config.control_rollup_enabled(); empty and inert when the gate is off.
    #[serde(skip)]
    pub(super) control_state_rollups: HashMap<String, RollupEntry>,
    // Transient per-execute hint: when true (async_storage + control_coalesce_persist),
    // control-state counter writes skip the redundant per-write whole-series block rewrite
    // and rely on the index snapshot + WAL replay for durability, exactly like the
    // control_state_changes/fol sub-stores already do. Serde-skipped; set on every execute.
    #[serde(skip)]
    pub(super) control_coalesce_persist: bool,
    // Transient per-execute hint: gate for converting oversized exact distinct sets to HLL
    // sketches on the CHANGE write path. Serde-skipped; set on every execute.
    #[serde(skip)]
    pub(super) control_distinct_sketch: bool,
    // Derived, in-memory feature-aggregate rollup: the numeric view of the feature series
    // (feature_values, decoded via aggregate_feature_values so it is bit-identical to the raw
    // aggregate) plus the shared rollup ladder over it. Serde-skipped; rebuilt lazily on the
    // FeatureAggQuery read path when stale. Gated by Config.control_rollup_enabled(); empty
    // and inert when off.
    #[serde(skip)]
    pub(super) feature_values: HashMap<String, BTreeMap<u64, i64>>,
    #[serde(skip)]
    pub(super) feature_rollups: HashMap<String, RollupEntry>,
    #[serde(default)]
    pub(super) context_nodes: HashMap<ModelKey, BlockAddress>,
    // Keyed by EVENT ID HASH, aligning events with entities/embeddings so update and delete
    // address one event directly in log n instead of scanning the node's whole series (mem0
    // delete carries the event id, not the time, so it previously had no way to locate one).
    #[serde(default, skip_serializing)]
    pub(super) context_events: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // Time index over the same events: timeline_key -> event_id_hash, where timeline_key stays
    // timestamp_ms * CONTEXT_TIMELINE_FANOUT + (id % FANOUT). The primary map above is ordered
    // by hash, which is effectively random, so a time window is no longer a contiguous range in
    // it -- and 13 sites scan events by time window (context_timeline_start/end). Those range
    // over this index and dereference into the primary, keeping time reads at log n + k rather
    // than degrading them to a full series scan to make deletes cheaper.
    //
    // Rebuilt at load from the same block decode the event load path already performs, so it
    // costs no extra on-disk state; it is serialized with the index like the primary because
    // ShardState is snapshotted whole.
    #[serde(default)]
    pub(super) context_event_timeline: HashMap<String, BTreeMap<u64, u64>>,
    #[serde(default, skip_serializing)]
    pub(super) context_indexes: HashMap<String, BTreeMap<u64, BlockAddress>>,
    #[serde(default)]
    pub(super) context_audits: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // Summary-dirty tracking is intentionally in-memory only. Instead of appending a
    // persisted `ctx:dirty` block per event (which produced one dirty node per write and
    // unbounded dirty-block growth: a real e2e capture stored 47 dirty records for only 6
    // events), we keep a coalescing hashmap keyed by dirty object key so repeated edits to
    // the same node collapse into a single entry. This map is `#[serde(skip)]`: it is
    // ephemeral and may be lost on restart, which is acceptable because the async summary
    // worker re-marks nodes on the next event and stale summaries are self-healing.
    #[serde(skip)]
    pub(super) context_dirty_index: HashMap<String, ContextDirtyEntry>,
    // Embedding-dirty tracking, independent of `context_dirty_index` (summary).
    // Keyed by `ctx:embdirty:{tenant}:{node}`; coalescing hashmap so repeated
    // marks for the same node collapse into one entry. Like the summary index it
    // is in-memory only (`#[serde(skip)]`) and ephemeral: on restart the drainer
    // re-derives pending work — a node whose embedding is still missing is simply
    // re-marked by the next ingest, and the hybrid retrieve path keeps it
    // rankable via lexical scoring in the meantime.
    #[serde(skip)]
    pub(super) context_embedding_dirty_index: HashMap<String, ContextDirtyEntry>,
    // Per-node temporal-compression high-water mark: the latest event time already
    // folded into a ContextCompressionEvent for this event object key. In-memory and
    // ephemeral (serde-skipped); on loss the auto-compression trigger re-compresses
    // the oldest pending window idempotently (stable compression id per window).
    #[serde(skip)]
    pub(super) context_compression_watermark: HashMap<String, u64>,
    // Entities are grouped by their OWNING NODE, not one map key per entity: the key is
    // `ctx:entity:{tenant}:{node}` (context_entity_collection_key) and the BTreeMap holds every
    // entity of that node. The inner u64 is the ENTITY HASH, not a timestamp -- unlike
    // context_events/indexes/audits, which are time-keyed. That keeps upsert as an overwrite of
    // one slot (an entity has one current value; its history is the separate
    // context_entity_update_audit series) while making a node's entities enumerable, which the
    // per-entity key shape could not do: a HashMap cannot prefix-scan, so ContextQueryEntities
    // had to be handed every entity_hash by its caller.
    #[serde(default)]
    pub(super) context_entities: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // No migration field is needed: the PERSISTED entry still carries the per-entity key
    // `ctx:entity:{tenant}:{node}:{entity_hash}`, which the load path splits back into
    // (collection key, entity hash). The on-disk shape is unchanged in both directions, so an
    // index written before this fold loads natively and one written after it stays readable by
    // an older binary.
    #[serde(default)]
    pub(super) context_children: HashMap<String, BTreeMap<u64, BlockAddress>>,
    #[serde(default)]
    pub(super) context_summaries: HashMap<String, BTreeMap<u64, BlockAddress>>,
    #[serde(default)]
    pub(super) context_compressions: HashMap<String, BTreeMap<u64, BlockAddress>>,
    #[serde(default)]
    #[serde(rename = "slot_index")]
    pub(super) bucket_index: CoreIndex,
    /// Highest WAL sequence whose effect is already materialized in this
    /// serialized index. On shard load, WAL records with sequence greater than this
    /// are replayed to rebuild in-memory state, matching startup load
    /// replaying the WAL from the dumped-log-id anchor. `None` marks an index
    /// written before this anchor existed (treated as fully authoritative -> no
    /// replay); a missing index file replays the whole retained WAL onto empty state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) applied_wal_sequence: Option<u64>,
    /// Per-bucket LRU recency: wall-clock ms of the last read/write that touched the
    /// bucket. In-memory and ephemeral (serde-skipped, like context_dirty_index); on
    /// restart every bucket resets to 0 (== never touched == evicted first), which
    /// self-corrects as traffic re-warms hot buckets. Mirrors SlotNode last_used
    /// without persisting it.
    #[serde(skip)]
    pub(super) bucket_recency: HashMap<u32, u64>,
    #[serde(skip)]
    pub(super) dirty_objects: DirtyObjectIndex,
    /// Phase-1 flat-append fast-skip flag for the per-execute
    /// `promote_model_maps_to_bucket_index_authority` reconciliation. The live write path already
    /// keeps `bucket_index` authoritative in step with the model maps (each mutating command
    /// upserts its block into `bucket_index` before returning), so once a full promote scan has
    /// confirmed the two are in sync a repeat per-command O(store) scan can only re-confirm it.
    /// Set true after a confirmed/rebuilt reconcile at the hot-path call site; `#[serde(skip)]`
    /// so it is false on every fresh load -> the first live command after any reload pays one
    /// full reconcile and re-establishes it. Only consulted when the log's `flat_append()`.
    #[serde(skip)]
    pub(super) promote_scan_done: bool,
    /// Resume point and candidate pool for sampled eviction. In-memory and ephemeral like
    /// `bucket_recency`: on restart the scan simply restarts from the top, which costs one
    /// pass, not correctness. Only consulted when the engine's `evict_sampled_lru` is on.
    #[serde(skip)]
    pub(super) evict_sampler: super::eviction_sampler::EvictionSamplerState,
    /// THE ROUTING RANGE THIS SHARD IS LOADED ON, travelling with the shard instead of with a
    /// caller. Read through [`ShardState::routing_range`], never directly.
    ///
    /// The two functions that decide WHERE A BLOCK IS FILED -- `upsert_bucket_index_block_inner`
    /// and `sync_bucket_index_object_blocks_with_mode` -- take `&mut ShardState` and a `ShardId`
    /// and never `&self`, so the engine's `shard_routing_range` accessor, which reads the info
    /// rows under `infos.read()`, is not reachable from either. They answered with the WHOLE
    /// range, which on a shard loaded on `0..1023` files an unrouted block in a bucket the shard
    /// does not hold, where nothing scoped to the shard will ever look for it.
    ///
    /// THREE `u32`-WIDE FIELDS RATHER THAN AN `Option<(u32, u32)>`, for the same reason
    /// `LiveBlockEntry` carries a value and a flag: the flag lands in the byte this struct was
    /// already padding out for its three other `bool`s, so the pair costs 8 bytes and not 16.
    ///
    /// `#[serde(skip)]`, SO THE STORED SHAPE DOES NOT MOVE. `ShardState` IS the serialized index
    /// -- see `index_format_version` at the top of this struct for what a change to that shape
    /// has already cost once -- and a skipped field is absent from the Serialize impl entirely,
    /// so an index written before this field reads identically after it and vice versa.
    /// `the_routing_range_field_changes_no_serialized_byte` drives that rather than asserting it.
    ///
    /// THE UNSTAMPED DEFAULT IS THE WHOLE RANGE, deliberately, and it is the same default
    /// `Engine::shard_routing_range` gives a shard whose info row is absent. A `ShardState` that
    /// never entered the engine -- a decoded image, a report's scratch copy -- keeps exactly the
    /// behaviour it had. Every state the engine SERVES is stamped, because there is one function
    /// that installs one (`install_shard_state`) and it stamps;
    /// `every_shard_the_engine_installs_carries_its_routing_range` holds that as a list.
    #[serde(skip)]
    pub(super) routing_range_start: u32,
    #[serde(skip)]
    pub(super) routing_range_end: u32,
    #[serde(skip)]
    pub(super) routing_range_known: bool,
    /// THE SHARD THIS STATE IS SERVED UNDER, travelling with the shard for the same reason the
    /// routing range above does. Read through [`ShardState::shard_id`], never directly.
    ///
    /// A block's object id is `stable_block_object_id(shard, kind, key)`. `BlockIndex` carries the
    /// kind and the key beside the address, so the only term a `&ShardState` could not supply was
    /// the first one, and four functions that take a shard and never `&self` say so in their own
    /// words: `BlockIndex::object_id`, `object_manager::runtime_report`,
    /// `settle_released_bucket_object_delete` -- whose comment reads "recomputing one needs a
    /// shard id this path does not carry" -- and `bucket_generation_fingerprints_by_bucket`. None
    /// of them is changed by this field existing; what changes is that the term is now reachable.
    ///
    /// A `u64` AND A FLAG, NOT AN `Option<ShardId>`, and the flag is not decoration: shard 0 is a
    /// real shard, so a bare zero cannot mean "unstamped" the way `u32::MAX` can stand in for an
    /// unknown range end.
    ///
    /// THE PAIR COSTS SIXTEEN BYTES, MEASURED, AND THIS FLAG DID NOT LAND FREE. `ShardState` goes
    /// from 1,888 to 1,904. The routing range's flag above DID land in padding this struct already
    /// had and its doc says so; that reasoning was written into this field's first draft and the
    /// measurement refuted it, so it is recorded here rather than repeated. An `Option<u64>` would
    /// have cost the same sixteen, so the flag is not what is being paid for -- the `u64` is.
    ///
    /// Paid rather than argued down, because this struct has ONE INSTANCE PER SHARD: that is the
    /// same reason `per_item_byte_budget` carries `ShardState` as a printed control and not as a
    /// budgeted row, and at a count of one, width is not worth trading the flag's clarity for.
    /// `the_shard_carried_range_costs_eight_bytes_on_a_structure_there_is_one_of` pins the number
    /// so a later widening is a named failure rather than a silent one.
    ///
    /// `#[serde(skip)]`, SO THE STORED SHAPE DOES NOT MOVE -- see `index_format_version` at the top
    /// of this struct for what a change to that shape has cost once, and
    /// `the_shard_id_field_changes_no_serialized_byte` for the drive rather than the claim. The
    /// stamp cannot be persisted anyway: it is the id the state is SERVED under, and an index file
    /// is read back by whichever shard the engine is loading.
    ///
    /// THERE IS NO DEFAULT AND THE ACCESSOR SAYS SO. An unstamped state answers `None`, which is
    /// the one behaviour that cannot derive a plausible WRONG object id -- a zero here would hash
    /// to a real-looking value for the wrong shard. Every state the engine SERVES is stamped,
    /// because there is one function that installs one (`install_shard_state`) and it stamps; the
    /// existing `every_shard_the_engine_installs_carries_its_routing_range` holds that there is no
    /// other way in, and `every_served_shard_carries_the_id_its_owner_routes_it_under` holds that
    /// what it stamps equals the key the served map routes under, for every shard it holds.
    #[serde(skip)]
    shard_id: u64,
    #[serde(skip)]
    shard_id_known: bool,
}

impl ShardState {
    /// Record the range this shard is loaded on. Called once, by `install_shard_state`.
    pub(super) fn set_routing_range(&mut self, start_routing_bucket: u32, end_routing_bucket: u32) {
        self.routing_range_start = start_routing_bucket;
        self.routing_range_end = end_routing_bucket;
        self.routing_range_known = true;
    }

    /// The range to file an unrouted block under.
    ///
    /// An unstamped state answers the WHOLE range -- what every caller of these two writers
    /// passed unconditionally before the field existed -- so a state that never entered the
    /// engine is unchanged by this field's existence.
    pub(super) fn routing_range(&self) -> (u32, u32) {
        if self.routing_range_known {
            (self.routing_range_start, self.routing_range_end)
        } else {
            (0, u32::MAX)
        }
    }

    /// Record the shard this state is served under. Called once, by `install_shard_state`.
    pub(super) fn set_shard_id(&mut self, shard_id: crate::types::ShardId) {
        self.shard_id = shard_id;
        self.shard_id_known = true;
    }

    /// The shard this state is served under, or `None` for a state that never entered the engine.
    ///
    /// `None` and not a default, deliberately: there is no shard id that is safe to guess. A
    /// caller that answers with a zero here derives `stable_block_object_id(0, kind, key)`, which
    /// is a well-formed id belonging to a different shard -- worse than having no id at all,
    /// because nothing downstream can tell it from the right one.
    pub(super) fn shard_id(&self) -> Option<crate::types::ShardId> {
        if self.shard_id_known {
            Some(self.shard_id)
        } else {
            None
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct CoreIndex {
    #[serde(default, alias = "slots")]
    pub(super) bucket_map: BucketMap,
    /// Per-slab live block refs and live bytes, maintained by every mutation of the map above.
    ///
    /// HERE, and not one level up on `ShardState`, for two reasons. `fold_delta_block_items` files
    /// blocks through a bare `&mut CoreIndex` and has no shard to reach for -- a tally it could not
    /// see would be a hole in the mutation surface, which is the one thing this must not have. And
    /// two fields of ONE struct are what make the borrows work at every other site:
    /// `bucket_map.get_mut(..)` loans one field while `&mut ..block_slab_live` takes the other,
    /// which the borrow checker accepts because the two places are disjoint.
    ///
    /// Derived, never persisted. Seeded by `seed_block_slab_live` wherever the index is rebuilt
    /// wholesale; until then `is_ready` is false and every consumer falls back to the walk.
    #[serde(skip)]
    pub(super) block_slab_live: BlockSlabLiveIndex,
    // Derived lookup tables rebuilt from the bucket map on load. Persisting them duplicates
    // block references already carried by the bucket index and made large context backfill
    // checkpoints tens of MB larger without adding authoritative recovery state.
    #[serde(default, skip_serializing)]
    pub(super) object_block_lookup: ObjectBlockLookup,
    /// One shared copy of each block kind, so a block holds a pointer rather than its own string.
    ///
    /// Measured over 2700 blocks: `model_id` had 2 distinct values and 2700 copies -- 1350 copies
    /// of each, and an allocation apiece. The object key, by contrast, had 1650 distinct values
    /// for those same 2700 blocks, which is why it is not interned here: sharing something that is
    /// nearly unique saves nothing.
    ///
    /// COMPONENT names share it, and that used to destroy it. The cap exists so that being
    /// bounded is a property of this code rather than a promise about every future caller -- but
    /// components are one per field, member or element of a container, so the FIRST container key
    /// written fills the cap, and after that the pool took nothing new. Including the kinds of
    /// every container written afterwards, which then allocated their own copy of a
    /// four-character string once per block. Measured over a container store of 40,000 blocks in
    /// 400 objects: 300 of the 400 held up to ONE HUNDRED distinct allocations of their model id,
    /// a name with four distinct values in the whole store.
    ///
    /// ONE POOL, TWO CEILINGS, and no second `HashSet`: a set of its own is 48 bytes on a
    /// structure there is one of per shard, and `shard_carried_range` holds `ShardState` at 1,888
    /// for exactly that reason. Components may fill the pool to `KIND_POOL_CAP`; kinds may use
    /// `KIND_RESERVE` slots beyond it, which no component can reach. The pool is still bounded,
    /// by the sum, and a component can no longer crowd out a kind.
    ///
    /// Owned by the index rather than a global, so the write path interns through the `&mut` it
    /// already holds and no lock appears on it. Not serialized -- it is a sharing detail, not part
    /// of the index.
    #[serde(skip)]
    pub(super) kind_pool: std::collections::HashSet<Arc<str>>,
    /// Running total of block refs across `object_component_lookup`, or `None` when not known.
    ///
    /// The stats path reports this number, and computing it as
    /// `object_component_lookup.values().map(BTreeSet::len).sum()` walks every object in the
    /// shard. That runs on a TIMER -- the server heartbeat, every 3s by default -- while holding
    /// the shard read lock that writers need, so its cost grows with the store and lands on the
    /// write path as lock contention. Measured on a 200k-record ingest in five equal phases: with
    /// the heartbeat at 1s the last phase cost 3.0x the first and the datanode's own CPU grew
    /// 3.3-3.7x; with the heartbeat off, 0.84-0.94x -- flat.
    ///
    /// Maintained by the two methods below, which are the only places the lookup is mutated, and
    /// recomputed by `rebuild_object_block_lookup`. `None` means "not established yet" (a
    /// freshly deserialized index, before any rebuild) and the reader falls back to the walk,
    /// so a missing value costs time rather than correctness.
    #[serde(skip)]
    pub(super) object_component_block_refs: Option<usize>,
    /// Buckets whose block list has been released: present in `bucket_map`, `in_memory: false`,
    /// `page_index` empty, reloadable from the model maps on demand.
    ///
    /// A registry rather than a scan, because three hot paths need the answer "is anything
    /// released" in O(1): the per-execute promote reconcile (which would otherwise see a released
    /// bucket as an index that has fallen out of sync and rebuild the whole shard), the
    /// bucket-index block walk (which must supplement released buckets from the model maps rather
    /// than report them as empty), and the write path (which must reload a bucket before filing a
    /// block into it, so a node never ends up half-resident).
    ///
    /// Not serialized: release is a memory state, not a durable one. An index written while a
    /// bucket is released decodes with that bucket simply holding no blocks, and every load path
    /// re-derives `bucket_map` from the model maps anyway -- which is why the release rules above
    /// refuse the three model maps that are themselves rebuilt from the index.
    #[serde(skip)]
    pub(super) released_buckets: BTreeSet<u32>,
}

pub(super) type BucketMap = BTreeMap<u32, BucketNode>;
/// The object ids a bucket holds.
///
/// Almost always exactly one: keys route one to a bucket, and even an object with many components
/// is still one object. A `BTreeSet` holding a single id costs 128 live bytes of node to carry
/// eight bytes of id, once per bucket and so once per key.
///
/// The shape `BlockIndexMap` and `BlockRefs` already use here, for the same reason.
///
/// THE RARE ARM IS A SLOT ARRAY AND NOT A SORTED RUN, so a slot number names the same object
/// for as long as that object is in the bucket. See [`ObjectSlots`] for the three rules that
/// make that true, the bound they give, and what they cost. The tiering above it is unchanged.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) enum ObjectIndex {
    #[default]
    Empty,
    One(u64),
    /// Several ids, held as a SLOT ARRAY behind one pointer.
    ///
    /// Boxed because an enum is as wide as its widest arm: held inline the collection made every
    /// `ObjectIndex` wider than a word whether or not it held anything, and a `BucketNode`
    /// carries two of them. Behind a box the enum is 16, and the box is only allocated by the
    /// buckets that actually hold more than one object.
    ///
    /// AN ARRAY AND NOT A TREE, because this arm is not rare. The measured occupancy is 46.6% of
    /// buckets holding two or more -- a collection key files one object id PER MEMBER into the
    /// one bucket its key routes to, while a string or a series files one -- so what this arm
    /// costs is paid by nearly half the store. A search tree charges a fixed 128 bytes for it: 24
    /// for the collection and 104 for a node sized to hold eleven ids whether it holds two or
    /// eleven.
    ///
    /// NOT SORTED, AND THAT IS THE CHANGE. It was a sorted run, which cannot be indexed: `insert`
    /// placed a new id at its bisection position, so every later position silently repointed. See
    /// [`ObjectSlots`] for the three rules that replace it, the bound they give, and the two things
    /// they cost -- the bisection `contains` used to answer by, and eight bytes a slot.
    Many(Box<ObjectSlots>),
}

/// A pointer-wide arm plus its tag, once per bucket.
///
/// It used to be twice: the tombstone side held the same enum for a case it is in 2.32% of
/// the time, and is now `DeletedObjectIndex` below, at eight.
/// ARITHMETIC AND NOT A LITERAL: the discriminant rounded up to the payload's alignment, plus
/// the payload. A literal 16 passes for the wrong reason the moment an arm widens into padding
/// that was already there, which is how three pins in this campaign under-reached.
/// `the_object_index_is_a_tag_and_one_pointer` measures the same identity with `offset_of!`.
const _: () = assert!(
    std::mem::size_of::<ObjectIndex>()
        == std::mem::align_of::<ObjectIndex>() + std::mem::size_of::<Box<ObjectSlots>>()
);

/// THE IDS OF A BUCKET HOLDING MORE THAN ONE OBJECT, AS A SLOT ARRAY: slot `i` names the same
/// object for as long as that object is in the bucket.
///
/// WHY NOT A SORTED RUN, which is what this was. A sorted run cannot be indexed, and #2023 drove
/// exactly that on twenty-five ids: position 12 named id 1300, inserting one unrelated SMALLER id
/// made the same position name 1200, a removal made it 1400, and `shrink` collapsing the arm to
/// `One` retired position 1 outright. Nothing in the store stored such a position, so nothing was
/// broken -- but nothing could, and that is what closed the route of naming an object by its
/// position instead of by sixteen bytes of `Arc<str>` on every page entry.
///
/// WHAT MAKES A SLOT STABLE, and it is three rules rather than one:
///
///   * `insert` takes the FIRST FREE SLOT, else appends. It never moves an id already filed, so no
///     insert can repoint a slot another id is in. This is the rule the sorted run broke.
///   * `remove` leaves a PLACEHOLDER. The slot keeps its position and `valid` falls; the array
///     never closes a hole, because closing one is exactly what renumbers every id above it.
///   * `valid` is tracked SEPARATELY from `slots.len()`. They are different numbers the moment a
///     placeholder exists, so every reader has to say which it wants: `ObjectIndex::object_count`
///     or `ObjectIndex::slot_count`, and there is no `len` to read by accident.
///
/// AND ONE THING THAT IS GIVEN BACK, which renumbers nothing. A removal that leaves free slots at
/// the END pops them, and the arm collapses to `One` only when a single id is left IN SLOT ZERO with
/// no slot above it. Both are index-preserving by inspection rather than by argument: a live id's
/// slot is never renumbered, and the positions dropped held no id for anyone to be naming.
///
/// SO THE ARRAY IS BOUNDED BY ITS OWN HIGH WATER MARK AND NOT BY CHURN. A bucket that has held at
/// most `n` objects at once is at most `n` slots long however many times it has been added to and
/// deleted from: a hole is refilled before the array grows, and a tail of holes is not kept. An
/// array that gave nothing back would instead grow with the number of OPERATIONS, which is the
/// failure this shape has to be shown to avoid rather than assumed to.
///
/// WHAT IT COSTS, both of which are measured and neither of which is nothing:
///
///   * THE BISECTION. `contains` was `binary_search` over a sorted run -- `floor(log2(n)) + 1`
///     entries examined on the one door every membership question goes through. A slot array is in
///     slot order, so the walk is linear in the SLOTS, placeholders included. At the run lengths
///     this store actually holds the two coincide (`floor(log2(n)) + 1 == n` for n of 1 and 2);
///     they diverge on the long lists a container bucket produces.
///   * EIGHT BYTES A SLOT. A slot is `Option<u64>` and not a `u64` with a reserved value, because
///     object ids are FNV-1a hashes over the shard, kind and key
///     (`hashing::stable_block_object_id`) and so span the whole of `u64`: any value picked to mean
///     free is a value some key hashes to, and the collision would read as a missing object rather
///     than as an error. `Option<u64>` has no niche, so a slot is 16 bytes where the run's element
///     was 8. `what_a_slot_array_costs_under_churn` prints what a reserved value would buy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectSlots {
    /// Slot `i` holds the id filed there, or `None` for a placeholder.
    slots: Vec<Option<u64>>,
    /// How many slots hold an id. NOT `slots.len()`, which counts placeholders too.
    ///
    /// `usize` and not the `u8` a 255-slot ceiling would allow: boxed, this struct is 32 bytes with
    /// either -- a `Vec` is 24 and the tail rounds to 8 -- so the narrow one buys no byte and costs
    /// a truncation to reason about. A ceiling here would also be a ceiling on a bucket, and a
    /// bucket fills with the corpus.
    valid: usize,
}

impl ObjectSlots {
    /// Two ids, in the slots they keep.
    fn pair(first: u64, second: u64) -> Self {
        ObjectSlots { slots: vec![Some(first), Some(second)], valid: 2 }
    }

    fn valid(&self) -> usize {
        self.valid
    }

    fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// A LINEAR WALK, charging what it actually looked at.
    ///
    /// Placeholders are counted: they are words the walk reads, so a bucket whose array has grown
    /// holes pays for them here, and the probe counter shows the waste instead of hiding it behind
    /// a valid count.
    fn locate(&self, id: &u64) -> (Option<usize>, u64) {
        let mut examined = 0u64;
        for (at, slot) in self.slots.iter().enumerate() {
            examined += 1;
            if slot.as_ref() == Some(id) {
                return (Some(at), examined);
            }
        }
        (None, examined)
    }

    fn id_at(&self, slot: usize) -> Option<u64> {
        self.slots.get(slot).copied().flatten()
    }

    /// FIRST FREE SLOT, ELSE APPEND -- and nothing already filed moves.
    fn insert(&mut self, id: u64) -> bool {
        if self.locate(&id).0.is_some() {
            return false;
        }
        match self.slots.iter().position(Option::is_none) {
            Some(free) => self.slots[free] = Some(id),
            None => self.slots.push(Some(id)),
        }
        self.valid += 1;
        true
    }

    /// A PLACEHOLDER, NOT A CLOSED HOLE. The slot keeps its position.
    fn remove(&mut self, id: &u64) -> bool {
        let Some(at) = self.locate(id).0 else {
            return false;
        };
        self.slots[at] = None;
        self.valid -= 1;
        self.drop_trailing_placeholders();
        true
    }

    /// Free slots at the END are given back, which renumbers nothing: every position dropped held
    /// no id, and every id still filed is below the new length. This is what keeps the array bounded
    /// by the bucket high water mark instead of by its churn.
    fn drop_trailing_placeholders(&mut self) {
        while self.slots.last().is_some_and(Option::is_none) {
            self.slots.pop();
        }
    }

    fn iter(&self) -> std::slice::Iter<'_, Option<u64>> {
        self.slots.iter()
    }

    fn into_ids(self) -> Vec<u64> {
        self.slots.into_iter().flatten().collect()
    }
}

pub(super) enum ObjectIndexIter<'a> {
    Empty,
    One(std::iter::Once<&'a u64>),
    Many(std::slice::Iter<'a, Option<u64>>),
}

impl<'a> Iterator for ObjectIndexIter<'a> {
    type Item = &'a u64;
    fn next(&mut self) -> Option<&'a u64> {
        match self {
            ObjectIndexIter::Empty => None,
            ObjectIndexIter::One(once) => once.next(),
            // Placeholders are skipped: a hole is not an object, and `object_count` is what
            // says how many this will yield.
            ObjectIndexIter::Many(slots) => slots.find_map(|slot| slot.as_ref()),
        }
    }
}

impl ObjectIndex {
    /// HOW MANY OBJECTS THIS HOLDS -- which is not how long the slot array is.
    ///
    /// Named rather than spelt `len`, and the rename is half the change. A `len` on a slot array is
    /// ambiguous between the count and the array, and the two differ the moment a placeholder
    /// exists; renaming it made the COMPILER name every reader of the old number, instead of leaving
    /// each one to be guessed at from a grep over a field name five structs share. See `slot_count`
    /// for the other number.
    pub(super) fn object_count(&self) -> usize {
        match self {
            ObjectIndex::Empty => 0,
            ObjectIndex::One(_) => 1,
            ObjectIndex::Many(slots) => slots.valid(),
        }
    }

    /// HOW LONG THE SLOT ARRAY IS, placeholders included.
    ///
    /// Never an object count. It is the bound on a slot number, and the number
    /// `what_a_slot_array_costs_under_churn` divides by `object_count` to report the waste.
    pub(super) fn slot_count(&self) -> usize {
        match self {
            ObjectIndex::Empty => 0,
            ObjectIndex::One(_) => 1,
            ObjectIndex::Many(slots) => slots.slot_count(),
        }
    }

    /// The slot an id is filed in, stable for as long as the id is in the bucket.
    ///
    /// The observable surface of the whole change: without it the three rules hold but nothing can
    /// read them, and a test driving them would be asserting about a private field. A page entry
    /// naming its object by slot -- the step this one is the precondition for -- consumes this and
    /// `id_at` and nothing else.
    pub(super) fn slot_of(&self, id: &u64) -> Option<usize> {
        match self {
            ObjectIndex::Empty => None,
            ObjectIndex::One(held) => (held == id).then_some(0),
            ObjectIndex::Many(slots) => slots.locate(id).0,
        }
    }

    /// The id a slot holds: `None` for a placeholder, and `None` past the end of the array.
    pub(super) fn id_at(&self, slot: usize) -> Option<u64> {
        match self {
            ObjectIndex::Empty => None,
            ObjectIndex::One(held) => (slot == 0).then_some(*held),
            ObjectIndex::Many(slots) => slots.id_at(slot),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, ObjectIndex::Empty)
    }

    /// By a WALK over the slot array, because slot order is not id order.
    ///
    /// This is what the slot array costs on the one door every membership question goes through.
    /// The run answered by bisection -- `floor(log2(n)) + 1` entries -- and the walk charges the
    /// slots it reads, placeholders included. `the_join_a_name_reader_would_pay` prints both models
    /// against the run lengths this store holds, where for n of 1 and 2 they are the same number.
    pub(super) fn contains(&self, id: &u64) -> bool {
        match self {
            ObjectIndex::Empty => {
                note_object_index_entries_examined(0);
                false
            }
            ObjectIndex::One(held) => {
                note_object_index_entries_examined(1);
                held == id
            }
            ObjectIndex::Many(slots) => {
                // Charged here rather than at the callers because this is the one door every
                // membership question goes through.
                let (found, examined) = slots.locate(id);
                note_object_index_entries_examined(examined);
                found.is_some()
            }
        }
    }

    pub(super) fn insert(&mut self, id: u64) -> bool {
        match self {
            ObjectIndex::Empty => {
                *self = ObjectIndex::One(id);
                true
            }
            ObjectIndex::One(held) => {
                if *held == id {
                    return false;
                }
                // The id already here KEEPS SLOT ZERO. The sorted run put the smaller of the two
                // first, which is what made slot zero name a different object after an insert of
                // something smaller than what was already filed.
                *self = ObjectIndex::Many(Box::new(ObjectSlots::pair(*held, id)));
                true
            }
            ObjectIndex::Many(slots) => slots.insert(id),
        }
    }

    pub(super) fn remove(&mut self, id: &u64) -> bool {
        match self {
            ObjectIndex::Empty => false,
            ObjectIndex::One(held) => {
                if held != id {
                    return false;
                }
                *self = ObjectIndex::Empty;
                true
            }
            ObjectIndex::Many(slots) => {
                let removed = slots.remove(id);
                self.settle();
                removed
            }
        }
    }

    /// Give up the array when it no longer names anything -- WITHOUT RENUMBERING A LIVE SLOT.
    ///
    /// THIS IS THE COLLAPSE THAT USED TO RETIRE A SLOT. It was `shrink`, and on reaching one id it
    /// rewrote `Many` to `One(that id)` whichever slot the id was in. A bucket holding
    /// `[placeholder, id]` became `One(id)`: a holder of slot 1 was left naming nothing, and a
    /// holder of slot 0 -- which had held a different object -- was left naming this one. #2023
    /// drove it as "the collapse retired ordinal 1", and it is the third of the three mutations that
    /// closed the route.
    ///
    /// WHAT IT DOES NOW. Exactly two collapses survive, and both are index-preserving by inspection
    /// rather than by argument:
    ///
    ///   * NO IDS LEFT -> `Empty`. There is no live slot to preserve, so there is nothing anyone can
    ///     be naming. `drop_trailing_placeholders` has already emptied the array by then, which is
    ///     why this is the only shape `valid == 0` can be in.
    ///   * ONE ID LEFT, IN SLOT ZERO, WITH NO SLOT ABOVE IT -> `One(id)`. Slot 0 names that id in
    ///     both shapes, and no other slot exists in either, so `slot_of` and `id_at` cannot tell the
    ///     two apart. That indistinguishability is what the test drives.
    ///
    /// A single id in slot 1, or in slot 0 with a placeholder above it, STAYS `Many`. That is
    /// precisely the case the old collapse got wrong, and it is the case a bucket is in whenever the
    /// object deleted is not the one filed first.
    fn settle(&mut self) {
        let (valid, slot_count) = match self {
            ObjectIndex::Many(slots) => (slots.valid(), slots.slot_count()),
            _ => return,
        };
        if valid == 0 {
            *self = ObjectIndex::Empty;
            return;
        }
        if valid == 1 && slot_count == 1 {
            let id = match self {
                ObjectIndex::Many(slots) => slots.id_at(0).expect("one valid id in one slot"),
                _ => unreachable!("just matched Many"),
            };
            *self = ObjectIndex::One(id);
        }
    }

    pub(super) fn clear(&mut self) {
        *self = ObjectIndex::Empty;
    }

    pub(super) fn iter(&self) -> ObjectIndexIter<'_> {
        match self {
            ObjectIndex::Empty => ObjectIndexIter::Empty,
            ObjectIndex::One(id) => ObjectIndexIter::One(std::iter::once(id)),
            ObjectIndex::Many(slots) => ObjectIndexIter::Many(slots.iter()),
        }
    }

    /// The ids this holds, ASCENDING, whatever slots they are in.
    ///
    /// `iter` yields SLOT ORDER, which is an order over slots and not over ids. Exactly one reader
    /// wants them sorted -- the Serialize impl, so that the bytes already on disk do not move -- and
    /// it says so here instead of relying on the container to be a sorted run, which it no longer
    /// is.
    pub(super) fn sorted_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// The ids this holds that `other` does not.
    pub(super) fn difference<'a>(
        &'a self,
        other: &'a ObjectIndex,
    ) -> impl Iterator<Item = &'a u64> + 'a {
        self.iter().filter(move |id| !other.contains(id))
    }
}

impl<'a> IntoIterator for &'a ObjectIndex {
    type Item = &'a u64;
    type IntoIter = ObjectIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for ObjectIndex {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<u64>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ObjectIndex::Empty => Vec::new().into_iter(),
            ObjectIndex::One(id) => vec![id].into_iter(),
            ObjectIndex::Many(slots) => (*slots).into_ids().into_iter(),
        }
    }
}

impl From<BTreeSet<u64>> for ObjectIndex {
    fn from(ids: BTreeSet<u64>) -> Self {
        ids.into_iter().collect()
    }
}

impl Extend<u64> for ObjectIndex {
    fn extend<I: IntoIterator<Item = u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(id);
        }
    }
}

impl<'a> Extend<&'a u64> for ObjectIndex {
    fn extend<I: IntoIterator<Item = &'a u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(*id);
        }
    }
}

impl FromIterator<u64> for ObjectIndex {
    fn from_iter<I: IntoIterator<Item = u64>>(ids: I) -> Self {
        let mut index = ObjectIndex::default();
        index.extend(ids);
        index
    }
}

impl Serialize for ObjectIndex {
    /// The same sequence of ids it has always written, in the same order.
    ///
    /// ASCENDING, which is the order a sorted run wrote and so the order already on disk. The slot
    /// array iterates in SLOT order, so the multi arm SORTS rather than taking the container's word
    /// for it -- which keeps every written byte where it was, and is why this change moves no stored
    /// shape and takes no new `SHARD_INDEX_FORMAT_VERSION`. Placeholders are not written: a hole is
    /// not an id, and a reload re-files what is written through `insert`, which hands out slots
    /// afresh.
    ///
    /// AND THE TWO COMMON ARMS DO NOT ALLOCATE TO SAY SO. A shard index is written on every dump,
    /// once per bucket and twice counting the tombstone side, and `Empty` and `One` are what most
    /// buckets are in -- sorting through a `Vec` there would have added an allocation per bucket per
    /// dump to a path that had none. Only the arm whose order can actually differ from ascending pays
    /// for being put in ascending order.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        match self {
            ObjectIndex::Empty => serializer.serialize_seq(Some(0))?.end(),
            ObjectIndex::One(id) => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                seq.serialize_element(id)?;
                seq.end()
            }
            ObjectIndex::Many(_) => {
                let ids = self.sorted_ids();
                let mut seq = serializer.serialize_seq(Some(ids.len()))?;
                for id in &ids {
                    seq.serialize_element(id)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ObjectIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Through `insert`, so a loaded bucket takes the shape a written one does: one id comes
        // back held inline rather than in a run, and a repeated id collapses.
        //
        // The sequence is read into a `Vec` and not a tree. It accepts exactly what it always
        // accepted -- a sequence of ids, in any order, duplicates allowed -- and `insert` puts
        // each one where it belongs, so the loaded shape is the same one a written bucket holds.
        // The tree that used to stand here was an allocation per bucket for a shape that was
        // thrown away on the next line.
        Ok(Vec::<u64>::deserialize(deserializer)?.into_iter().collect())
    }
}

/// THE TOMBSTONE SIDE OF A BUCKET, held so that ABSENCE costs a pointer and nothing else.
///
/// `deleted_object_index` is the ids a bucket has had deleted and not yet reclaimed. It is
/// written by one path -- a delete that finds blocks to retire -- and cleared by the next write of
/// the same object, so on a store that is not being deleted from it holds nothing at all.
/// MEASURED over a seeded corpus at two sizes with one string key in twenty deleted: 97.68% of
/// buckets carry no tombstone, and the widest bucket that carries one carries a single id.
///
/// Held as `ObjectIndex` that is the sixteen bytes of an enum whether or not it holds anything,
/// twice over on the two arms that never run. Held as one nullable pointer it is eight, and the
/// sixteen it used to spend are only spent by the buckets that are actually carrying a tombstone
/// -- which is the same tiering `ObjectIndex` already applies one level down, with the tier that
/// costs nothing moved to the case this field is actually in.
///
/// WHERE THE TRADE TURNS OVER, because it is not free in the other direction: a bucket that DOES
/// carry a tombstone pays the pointer plus a sixteen-byte allocation for the index behind it,
/// which the allocator serves out of its smallest chunk. So the shape wins while fewer than
/// about a quarter of buckets carry one and loses above that, against a measured 2.32%.
/// `what_the_object_side_of_the_bucket_node_costs` prints the share beside the saving so a
/// workload that moved it would be visible rather than assumed.
///
/// `Some` ALWAYS HOLDS A NON-EMPTY INDEX. `remove` gives the allocation back when it takes the
/// last id, so "carrying nothing" has exactly one spelling and two buckets holding no tombstone
/// cannot compare unequal.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct DeletedObjectIndex(Option<Box<ObjectIndex>>);

/// One nullable pointer, against the sixteen bytes the enum spends inline.
const _: () = assert!(std::mem::size_of::<DeletedObjectIndex>() == 8);

impl DeletedObjectIndex {
    pub(super) fn object_count(&self) -> usize {
        self.0.as_ref().map_or(0, |index| index.object_count())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.as_ref().map_or(true, |index| index.is_empty())
    }

    pub(super) fn contains(&self, id: &u64) -> bool {
        self.0.as_ref().is_some_and(|index| index.contains(id))
    }

    pub(super) fn insert(&mut self, id: u64) -> bool {
        self.0
            .get_or_insert_with(|| Box::new(ObjectIndex::default()))
            .insert(id)
    }

    /// Removing the last id gives the allocation back, which is what keeps the empty state to a
    /// single spelling and keeps a bucket that was briefly deleted from holding a box for ever.
    pub(super) fn remove(&mut self, id: &u64) -> bool {
        let Some(index) = self.0.as_mut() else {
            return false;
        };
        let removed = index.remove(id);
        if index.is_empty() {
            self.0 = None;
        }
        removed
    }

    pub(super) fn iter(&self) -> ObjectIndexIter<'_> {
        match &self.0 {
            None => ObjectIndexIter::Empty,
            Some(index) => index.iter(),
        }
    }

    /// The ids this holds, ASCENDING. See [`ObjectIndex::sorted_ids`]: `iter` is in slot order, and
    /// the Serialize impl is the one reader that needs the other.
    pub(super) fn sorted_ids(&self) -> Vec<u64> {
        self.0.as_ref().map_or_else(Vec::new, |index| index.sorted_ids())
    }
}

impl<'a> IntoIterator for &'a DeletedObjectIndex {
    type Item = &'a u64;
    type IntoIter = ObjectIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for DeletedObjectIndex {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<u64>;
    fn into_iter(self) -> Self::IntoIter {
        match self.0 {
            None => Vec::new().into_iter(),
            Some(index) => (*index).into_iter(),
        }
    }
}

impl Extend<u64> for DeletedObjectIndex {
    fn extend<I: IntoIterator<Item = u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(id);
        }
    }
}

impl<'a> Extend<&'a u64> for DeletedObjectIndex {
    fn extend<I: IntoIterator<Item = &'a u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(*id);
        }
    }
}

impl FromIterator<u64> for DeletedObjectIndex {
    fn from_iter<I: IntoIterator<Item = u64>>(ids: I) -> Self {
        let mut index = DeletedObjectIndex::default();
        index.extend(ids);
        index
    }
}

impl Serialize for DeletedObjectIndex {
    /// The same sequence of ids in the same order, and the same empty sequence when there is
    /// nothing to write -- a bucket carrying no tombstone spells it `[]` exactly as it did when
    /// the field held an empty enum.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // ASCENDING, and allocation-free on the arms that hold nothing or one id -- through the
        // live side's own Serialize impl, where both rules live. This field is serialized, so writing
        // it in slot order would move bytes already on disk for no gain, and it holds a single id on
        // 97.68% of the buckets that carry one at all, so an allocation here would be an allocation
        // on almost all of them.
        //
        // DELEGATED RATHER THAN COPIED: two impls that agreed today would be two impls to keep
        // agreeing, and "carrying nothing" has exactly one spelling precisely so it cannot drift.
        match &self.0 {
            None => {
                use serde::ser::SerializeSeq;
                serializer.serialize_seq(Some(0))?.end()
            }
            Some(index) => index.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for DeletedObjectIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // An empty sequence loads as no allocation at all, which is the whole point of the
        // shape: the common bucket comes back holding one null pointer.
        Ok(Vec::<u64>::deserialize(deserializer)?.into_iter().collect())
    }
}

/// Keyed by a SHARED block-ref key: the same allocation is held by the lookups that point at this
/// block, instead of each of the three keeping its own copy of the same ~117-byte string.
/// Blocks of one bucket, keyed by an id assigned when the block is filed.
///
/// The key used to be a rendered string of the block's identity and address -- 45.6 B a block, and
/// three quarters of what a block cost on the heap. It was never read as a name: every lookup goes
/// through a ref this map handed out, and a rewrite produces a different key while leaving one
/// entry, so identity comes from the lookup rather than from key equality.
///
/// Serializes as the string map it always was. The key is rebuilt from the value, which carries
/// every part of it, and the handle is recomputed from the block on load.
///
/// The handle is NOT free to choose: the lookup's refs hold handles and the lookup is written to
/// disk, so a handle has to mean the same block in every process that reads the file. Assigning
/// them from a counter compiled, round-tripped, and lost an object on the first reload.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(from = "BTreeMap<String, BlockIndex>")]
pub(super) enum BlockIndexMap {
    /// A bucket holding nothing: no list, no node, no allocation.
    ///
    /// KEPT FOR THE NAME, NOT FOR THE BYTE. An empty `Vec` allocates nothing, so this arm saves no
    /// allocation over `Many(vec![])` -- and it costs no width either, because it rides in the
    /// vector pointer's niche and the enum is exactly as wide as the list it holds. What it buys is
    /// that "this bucket holds nothing" has a name at every site that tests for it, including the
    /// one the release lifecycle assigns. `the_empty_arm_is_an_alias_for_no_pages_and_costs_nothing_to_keep`
    /// asserts both halves of "free" and the release/reload round trip that depends on it.
    ///
    /// AN ALIAS, NOT A RELEASE MARKER. A released bucket has an empty block index, but what makes it
    /// RELEASED rather than legitimately empty is `released_buckets` plus its retained
    /// `object_index` -- `release_bucket_blocks` says so where it keeps that index. Nothing reads
    /// this discriminant as a lifecycle state.
    #[default]
    Empty,
    /// One block, held behind a POINTER rather than inline.
    ///
    /// THE ARM SURVIVES; WHAT IT COSTS THE NODE DOES NOT. It used to hold the whole entry inline, so
    /// this enum was `8 + size_of::<BlockIndex>()` and every `BucketNode` in the `BucketMap` paid
    /// that width whether or not its bucket held exactly one block. Now the arm is a handle and a
    /// box, the enum is exactly as wide as the list it also holds -- the tag rides in a pointer
    /// niche -- and the single-block case still has a representation of its own.
    ///
    /// WHY THE WIDTH WAS PAYABLE ONLY NOW. #1964 kept the inline entry on an explicit measurement,
    /// and the measurement was sound on the population it sampled: `load_shard` defaulted
    /// `end_routing_bucket` to the whole `u32` keyspace, which gives EVERY KEY A BUCKET OF ITS OWN
    /// BY CONSTRUCTION, so 100% of buckets were single-block and the arm was free of charge there.
    /// #1973 made 1023 the shipped default. At 1,024 buckets the same routed keys FILL them --
    /// measured 4.510% single-block at 4,000 records and 0.000% at 40,000, p50 39 and MAX 52 -- so
    /// the inline width became a toll on 95.5% to 100% of buckets that could never use it.
    ///
    /// AND WHY BOXING RATHER THAN DROPPING THE ARM ALTOGETHER, which recovers the identical 24-byte
    /// width. Dropping it makes a single-block bucket hold a one-entry LIST, and the list's first
    /// block is a whole growth step -- four entries to carry one. Measured over the real bucket
    /// population in both allocator columns: at the shipped range the two are within 13.7 B a bucket
    /// at 4,000 records and identical at 40,000, but on the WHOLE-KEYSPACE range dropping the arm
    /// costs 482.0 B a bucket against boxing's 178.3 and the inline arm's 284.2 -- a REGRESSION of
    /// 197.9 B a bucket against doing nothing. A store records its routing range beside its index
    /// and a load honours that file, so stores built before #1973 still run the wide range. Boxing
    /// is the only one of the three that wins at both. `inline_arm_trade.rs` is that measurement,
    /// four shapes wide.
    ///
    /// WHAT THE ARM STILL COSTS, AND IT IS REAL. One allocation per single-block bucket that the
    /// inline entry did not take, and one DEPENDENT load on every read of such a block -- the entry's
    /// address is not known until the node has been read. Both are measured; dropping the arm would
    /// have paid both too.
    One(u64, Box<BlockIndex>),
    /// Several blocks -- an object with components, or several keys routed to one bucket -- held
    /// as a FLAT LIST SORTED BY HANDLE rather than as a tree.
    ///
    /// WHY A LIST AND NOT A TREE. A `BTreeMap` leaf holds eleven value slots whether or not it
    /// fills them: at 104 bytes a block that is a 1,248-byte node per eleven blocks, and the fill
    /// was measured at 63%. A block list is short, and the operations it actually takes are a
    /// lookup by handle, an ordered walk, and an insert -- none of which needs a tree's
    /// rebalancing. `the_page_index_of_a_real_store_costs_less_as_a_list_than_as_a_tree` measures
    /// what the node costs against what the list costs at the real length distribution, and
    /// `the_page_list_length_distribution_is_reported_as_a_histogram` publishes that distribution.
    ///
    /// SORTED BY HANDLE, AND THAT IS NOT AN IMPLEMENTATION DETAIL. A `BTreeMap<u64, _>` iterates
    /// in ascending key order, and readers of this index depend on that: the index-log item
    /// builder emits blocks in this order, `collect_live_block_entries` materialises them in it,
    /// the storage-topology sampler TRUNCATES at a sample cap so the order decides which blocks
    /// are reported, `bucket_index_shape_for_test` renders it as the comparison between a shard
    /// built by commands and one rebuilt from records, and the whole-scan address lookup in
    /// `bucket_store` takes the FIRST match. Keeping the list sorted by the same key the tree was
    /// keyed by makes every one of those readers see the identical sequence, which is why this is
    /// a container change and not a behaviour change.
    /// `an_unsorted_page_list_would_reorder_every_walk_of_this_index` is the control: it builds
    /// the same blocks in three different orders and asserts one walk, with a negative control
    /// showing that fill order and handle order genuinely differ.
    ///
    /// DUPLICATES ARE REPLACED, NOT APPENDED. A map deduplicated by construction; a list does
    /// not, so `insert_unaccounted` binary-searches and overwrites in place on a hit, returning
    /// the address it displaced so the live tally can discharge it.
    /// `a_second_insert_of_the_same_page_replaces_it_rather_than_adding_beside_it` is the guard.
    Many(Vec<(u64, BlockIndex)>),
}

/// AS WIDE AS THE BLOCK LIST IT HOLDS, AND NO WIDER -- three arms in the space of one vector header.
/// `Empty` and the boxed single-block arm both ride in pointer niches, so there is no discriminant
/// word and no inline payload to be the widest field of `BucketNode` any more.
///
/// 24, not 80, since the single-block case stopped being held INLINE and started being held behind a
/// pointer. That arm was `8 + size_of::<BlockIndex>()` -- a handle plus a whole entry -- and every
/// `BucketNode` in the `BucketMap` paid its width whether or not its bucket held exactly one block.
/// The 56 bytes leave the structure rather than moving into its tail, which is why they take
/// `BucketNode` with them.
///
/// 56 AND NOT 72, BECAUSE #1974 GOT THERE FIRST AND THE TWO CHANGES HARVEST THE SAME FIELD. It took
/// the entry from 88 to 72 by spending one byte on a model spelling drawn from a seventeen-element
/// set instead of a sixteen-byte fat pointer, which took this arm 96 -> 80 and the node 160 -> 144.
/// Whichever of the two landed second found the other's node saving already banked; the node is 88
/// either way. What #1974 keeps after this change is on the HEAP rather than in the node -- a
/// narrower entry makes every boxed arm and every list element smaller.
///
/// AND BOXING THE ARM RECOVERS EXACTLY WHAT REMOVING IT WOULD. Measured: both shapes are 24 bytes,
/// because the tags fit in the pointer niches either way. So the width is not what decides between
/// them -- the heap is, and the measurement is in `inline_arm_trade.rs`.
///
/// ASSERTED AGAINST THE LIST RATHER THAN A LITERAL ALONE. The literal says which number this is;
/// the derivation says WHY, and it is the derivation that fails if the niches ever stop being
/// available -- which is the failure a literal alone would report as "the width moved".
const _: () = assert!(std::mem::size_of::<BlockIndexMap>() == 24);
const _: () = assert!(
    std::mem::size_of::<BlockIndexMap>() == std::mem::size_of::<Vec<(u64, BlockIndex)>>()
);

/// Entries this process has examined looking a block up, counted under `cfg(test)` only.
///
/// THE READ PATH IS WHERE THIS CHANGE IS WON OR LOST -- it trades a tree descent for a walk --
/// and a duration cannot say so on a box that sits at load 40. This counts the entries the
/// lookup actually touched, which repeats to three significant figures and is what the two
/// strategies differ in. Incremented inside [`find_page`], which is the ONE door every lookup
/// goes through, so a test reading it is reading the copy production calls rather than a model
/// of it.
///
/// `cfg(test)` and not a feature: a counter on the read path is exactly what this campaign is
/// removing, and it must not exist in a shipped binary.
#[cfg(test)]
/// HOW MANY ENTRIES A MEMBERSHIP QUESTION IN AN `ObjectIndex` TOUCHES.
///
/// The number an ordinal proposal turns on. `stable_block_object_id` computes an object's identity
/// from its key with a hash; the alternative is to look the object up in the bucket's own short list,
/// and what that COSTS is this count against the hash's byte count -- the two things
/// `engine::hashing::ROUTING_BUCKET_KEY_BYTES` and this counter make comparable.
///
/// `#[cfg(test)]` for the reason given on the derivation counters: the work being counted is smaller
/// than the atomic that would count it in production.
#[cfg(test)]
pub(super) static OBJECT_INDEX_ENTRIES_EXAMINED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// A bisection of `len` touches `floor(log2(len)) + 1` entries, and zero for an empty list. Written
/// out rather than measured inside `binary_search`, which does not report.
#[cfg(test)]
pub(super) fn entries_a_bisection_examines(len: usize) -> u64 {
    if len == 0 {
        0
    } else {
        u64::from(usize::BITS - len.leading_zeros())
    }
}

#[cfg(not(test))]
fn entries_a_bisection_examines(_len: usize) -> u64 {
    0
}

#[cfg(test)]
fn note_object_index_entries_examined(count: u64) {
    OBJECT_INDEX_ENTRIES_EXAMINED.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_object_index_entries_examined(_count: u64) {}

#[cfg(test)]
pub(super) fn reset_object_index_entries_examined() {
    OBJECT_INDEX_ENTRIES_EXAMINED.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn object_index_entries_examined() -> u64 {
    OBJECT_INDEX_ENTRIES_EXAMINED.load(std::sync::atomic::Ordering::Relaxed)
}

pub(super) static PAGE_LOOKUP_ENTRIES_EXAMINED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(super) fn page_lookup_entries_examined() -> u64 {
    PAGE_LOOKUP_ENTRIES_EXAMINED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(super) fn reset_page_lookup_entries_examined() {
    PAGE_LOOKUP_ENTRIES_EXAMINED.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
fn note_entries_examined(count: usize) {
    PAGE_LOOKUP_ENTRIES_EXAMINED.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_entries_examined(_count: usize) {}

/// Walk a sorted block list from the front. THE STRATEGY THE MEASUREMENT DECLINED, kept for the
/// measurement and compiled only into tests.
///
/// A walk was the obvious candidate here: a block list is short, its entries are contiguous, and a
/// walk has no unpredictable branch. `the_walk_and_the_bisection_cross_over_where_the_measurement_says`
/// counts the entries each strategy touches and the bisection becomes the cheaper of the two at a
/// list of THREE -- while the `Many` arm exists only from TWO, where the two tie exactly. So there
/// is no length this arm ever holds at which the walk is ahead, and the shipped lookup is the
/// bisection. At the measured p50 of 39 the walk touches 20.000 entries a hit against the
/// bisection's 4.538 -- 4.41x, far outside any correction contiguity could make to a count.
///
/// Sorted, so the walk stops at the first handle PAST the one being looked for -- and the
/// position it stops at is the insert position, which is what the bisection's `Err` arm hands
/// back too. Both therefore answer the identical `Result`, which is what lets
/// `the_walk_and_the_bisection_answer_identically_at_every_length` run one against the other.
#[cfg(test)]
pub(super) fn scan_page(pages: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    for (at, (handle, _)) in pages.iter().enumerate() {
        match handle.cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                note_entries_examined(at + 1);
                return Ok(at);
            }
            std::cmp::Ordering::Greater => {
                note_entries_examined(at + 1);
                return Err(at);
            }
        }
    }
    note_entries_examined(pages.len());
    Err(pages.len())
}

/// Bisect a sorted block list. THE SHIPPED LOOKUP.
///
/// Written out rather than delegated to `binary_search_by` so the entries it touches can be
/// COUNTED as they are touched. A count derived from `log2(len)` would be a model of the search
/// rather than the search, and this campaign has already had an instrument report a model.
///
/// It also has no cliff: the widest list measured holds 50 blocks, and ten times that is four more
/// probes. A walk at the same length would be five hundred entries, which is the reason a
/// fallback for the tail is not needed here -- the shipped strategy IS the tail's strategy.
pub(super) fn bisect_page(pages: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    let (mut low, mut high) = (0usize, pages.len());
    let mut examined = 0usize;
    while low < high {
        let mid = low + (high - low) / 2;
        examined += 1;
        match pages[mid].0.cmp(key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                note_entries_examined(examined);
                return Ok(mid);
            }
        }
    }
    note_entries_examined(examined);
    Err(low)
}

/// Find a handle in a sorted block list.
///
/// THE ONE DOOR. Every lookup, removal and insert into the `Many` arm comes through here, which
/// is what makes the entry counter above a reading of the path production runs rather than of a
/// copy written for the test. It is the bisection because that is what the measurement said --
/// see [`scan_page`] for the strategy it declined and the counts that declined it.
pub(super) fn find_page(pages: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    bisect_page(pages, key)
}

/// Room for one more block, taken in fixed steps rather than by doubling.
///
/// `Vec`'s own growth doubles: a list of 39 entries takes a capacity of 64, which is a quarter of
/// its bytes unused -- almost exactly the slack the `BTreeMap` node left, so a change that
/// swapped a tree for a doubling vector would have spent the change and bought nothing. Stepping
/// by four caps the waste at three entries, at the price of a reallocation every four inserts on
/// a list that is tens of entries long.
/// `the_page_list_growth_step_is_what_keeps_the_slack_off_the_measurement` measures both sides.
///
/// FOUR AND NOT EIGHT, because most buckets are short. At 4,000 records on the configured range
/// the measured distribution is p50 4, p90 6, MAX 8, and a step of eight would have charged a
/// list of two the same as a list of eight. Four ties `Vec`'s own first block at every length up
/// to eight and wins from twelve upward, which is where the large corpus lives.
pub(super) const PAGE_LIST_GROWTH_STEP: usize = 4;

fn reserve_one_more(pages: &mut Vec<(u64, BlockIndex)>) {
    if pages.len() == pages.capacity() {
        let want = (pages.len() + 1).div_ceil(PAGE_LIST_GROWTH_STEP) * PAGE_LIST_GROWTH_STEP;
        pages.reserve_exact(want - pages.len());
    }
}

/// Iterating a block index, whichever shape it is in.
pub(super) enum BlockIndexIter<'a> {
    Empty,
    One(std::iter::Once<(&'a u64, &'a BlockIndex)>),
    Many(std::slice::Iter<'a, (u64, BlockIndex)>),
}

impl<'a> Iterator for BlockIndexIter<'a> {
    type Item = (&'a u64, &'a BlockIndex);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BlockIndexIter::Empty => None,
            BlockIndexIter::One(once) => once.next(),
            // The list is kept sorted by handle, so this yields the same sequence the tree's
            // iterator did.
            BlockIndexIter::Many(iter) => iter.next().map(|(handle, page)| (handle, page)),
        }
    }
}

pub(super) enum BlockIndexValuesMut<'a> {
    Empty,
    One(std::iter::Once<&'a mut BlockIndex>),
    Many(std::slice::IterMut<'a, (u64, BlockIndex)>),
}

impl<'a> Iterator for BlockIndexValuesMut<'a> {
    type Item = &'a mut BlockIndex;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BlockIndexValuesMut::Empty => None,
            BlockIndexValuesMut::One(once) => once.next(),
            BlockIndexValuesMut::Many(iter) => iter.next().map(|(_handle, page)| page),
        }
    }
}

/// Live blocks sitting on ONE slab: how many, and how many logical bytes of them.
///
/// `bytes` sums `BlockAddress::length`, which is what every existing per-slab live figure sums --
/// `storage_reclaim_slab_reports` fills `live_physical_bytes` from exactly that. Block refs and
/// bytes are both kept because they answer different questions and neither derives the other: the
/// compaction drain set asks whether ANY block is still there, a garbage fraction asks how MUCH.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SlabLiveTally {
    pub(super) block_refs: u64,
    pub(super) bytes: u64,
}

/// Per-slab live-block tally, MAINTAINED on every index mutation rather than recomputed by a
/// whole-shard walk.
///
/// WHAT IT COUNTS. Exactly the block set `collect_live_block_entries` returns: every block held in
/// the bucket index, plus -- for a RELEASED bucket -- the blocks that bucket held when it was
/// released. Delete-marked blocks are included, because that walk includes them; a block leaves
/// this tally when its index entry does, not when a flag on it changes.
///
/// WHY THE INDEX AND NOT THE BLOCK STORE. The block store never learns that a block died. It sees
/// appends, and it sees whole slabs arrive and leave; the fact that an index entry stopped
/// pointing at an offset reaches it nowhere. A live-byte figure maintained there could only be
/// recomputed from the index anyway, which is the walk this exists to remove.
///
/// RELEASE IS COUNTER-NEUTRAL, DELIBERATELY. `release_bucket_blocks` empties a bucket block index
/// while its blocks stay live -- they are still in the model maps, and
/// `collect_bucket_index_live_block_entries` supplements them back into the walk. So release does
/// NOT decrement, and `reload_released_bucket` does NOT increment: it re-files the same blocks
/// through `insert_released`. The pair cancels, which is why a released-then-reloaded bucket is
/// one of the workloads the drift check is required to cover rather than one it may assume.
///
/// ON DRIFT, RECOMPUTATION WINS. `reconcile_block_slab_live` compares this against the walk,
/// corrects THIS one, and reports the difference. There is no production assert: a counting bug
/// must not become an outage.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BlockSlabLiveIndex {
    by_slab: BTreeMap<u64, SlabLiveTally>,
    /// False until something has derived this from the index it counts.
    ///
    /// A ShardState arrives from serde with this empty, and an empty tally is indistinguishable
    /// from a shard holding no live blocks at all. Every consumer checks this before believing a
    /// zero, and the fallback is the walk -- so a load path that forgets to seed it costs the old
    /// cost rather than reporting a store made entirely of garbage.
    ready: bool,
}

/// Every charge and discharge this process has made against a live tally.
///
/// THE COST ADDED, as a count rather than as a duration: one map lookup apiece, on the write path.
/// A count is what can be compared against the scan it replaces without a clock, and it is what
/// stays true on a box that is busy.
pub static BLOCK_SLAB_LIVE_CHARGES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn block_slab_live_charges() -> u64 {
    BLOCK_SLAB_LIVE_CHARGES.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn reset_block_slab_live_charges() {
    BLOCK_SLAB_LIVE_CHARGES.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl BlockSlabLiveIndex {
    pub(super) fn is_ready(&self) -> bool {
        self.ready
    }

    /// Make the tally read as never-derived, so a consumer takes its fallback.
    ///
    /// The A side of the measurement: the same process, the same shard, the same round, with the
    /// maintained tally withheld -- which is the only way to compare the walk against the tally
    /// without also comparing two different corpora.
    #[cfg(test)]
    pub(super) fn forget_for_test(&mut self) {
        self.ready = false;
    }

    pub(super) fn add_address(&mut self, address: &BlockAddress) {
        BLOCK_SLAB_LIVE_CHARGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tally = self.by_slab.entry(address.block_slab_id()).or_default();
        tally.block_refs = tally.block_refs.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(address.length());
    }

    pub(super) fn remove_address(&mut self, address: &BlockAddress) {
        BLOCK_SLAB_LIVE_CHARGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(tally) = self.by_slab.get_mut(&address.block_slab_id()) else {
            return;
        };
        tally.block_refs = tally.block_refs.saturating_sub(1);
        tally.bytes = tally.bytes.saturating_sub(address.length());
        if tally.block_refs == 0 && tally.bytes == 0 {
            // A slab nothing points at any more is ABSENT, not zero. Keeping the entry would grow
            // this map with every slab the store ever rolled, and the caller that wants a zero for
            // a slab it can name gets one from `tally` regardless.
            self.by_slab.remove(&address.block_slab_id());
        }
    }

    pub(super) fn tally(&self, block_slab_id: u64) -> SlabLiveTally {
        self.by_slab.get(&block_slab_id).copied().unwrap_or_default()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, SlabLiveTally)> + '_ {
        self.by_slab.iter().map(|(id, tally)| (*id, *tally))
    }

    pub(super) fn slab_count(&self) -> usize {
        self.by_slab.len()
    }

    pub(super) fn total_block_refs(&self) -> u64 {
        self.by_slab
            .values()
            .fold(0_u64, |sum, tally| sum.saturating_add(tally.block_refs))
    }

    /// Declare the tally derived, without replacing it.
    ///
    /// For a rebuild that CHARGED every block as it filed it: the tally is already correct and a
    /// walk to confirm that would be the walk this type exists to remove. Callers must have
    /// emptied it first -- `rebuild_bucket_block_ownership` does, right where it clears the map it
    /// counts.
    pub(super) fn mark_ready(&mut self) {
        self.ready = true;
    }

    /// Empty the tally and un-derive it. Pairs with `mark_ready` around a rebuild.
    pub(super) fn clear(&mut self) {
        self.by_slab.clear();
        self.ready = false;
    }

    /// Replace the whole tally and declare it derived.
    ///
    /// For the paths that rebuild the index this counts -- a load, a manifest install, a
    /// bucket-ownership rebuild -- and for the drift check correcting itself.
    pub(super) fn reset_from(&mut self, tallies: BTreeMap<u64, SlabLiveTally>) {
        self.by_slab = tallies;
        self.ready = true;
    }
}

impl BlockIndexMap {
    pub(super) fn get(&self, key: &u64) -> Option<&BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            // ONE COMPARISON AND ONE POINTER LOAD, not a bisection. The arm does not enter
            // `find_page`, which is why `PAGE_LOOKUP_ENTRIES_EXAMINED` counts nothing at all for a
            // single-block bucket.
            BlockIndexMap::One(handle, page) => (handle == key).then_some(&**page),
            BlockIndexMap::Many(pages) => find_page(pages, key).ok().map(|at| &pages[at].1),
        }
    }

    pub(super) fn get_mut(&mut self, key: &u64) -> Option<&mut BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            BlockIndexMap::One(handle, page) => (&*handle == key).then_some(&mut **page),
            BlockIndexMap::Many(pages) => {
                find_page(pages, key).ok().map(|at| &mut pages[at].1)
            }
        }
    }

    /// Drop a block and charge the removal to the live tally.
    pub(super) fn remove(
        &mut self,
        key: &u64,
        live: &mut BlockSlabLiveIndex,
    ) -> Option<BlockIndex> {
        let removed = self.remove_unaccounted(key);
        if let Some(page) = removed.as_ref() {
            live.remove_address(&page.address);
        }
        removed
    }

    fn remove_unaccounted(&mut self, key: &u64) -> Option<BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            BlockIndexMap::One(handle, _) => {
                if handle != key {
                    return None;
                }
                match std::mem::replace(self, BlockIndexMap::Empty) {
                    // The box is UNWRAPPED rather than cloned out of: the caller is handed the entry
                    // it already owned, and the allocation goes back to the allocator with it.
                    BlockIndexMap::One(_, page) => Some(*page),
                    _ => unreachable!("just matched One"),
                }
            }
            BlockIndexMap::Many(pages) => {
                let removed = find_page(pages, key)
                    .ok()
                    .map(|at| pages.remove(at).1);
                self.shrink();
                removed
            }
        }
    }

    /// Install a block, charge it to the live tally, and return its handle.
    ///
    /// A block with the same identity replaces the one already there rather than adding beside it,
    /// which is what the rendered string key used to do by being the key -- so an OVERWRITE both
    /// discharges the address it displaced and charges the new one. Those are different slabs
    /// whenever a rewrite rolled, which is the whole reason a counter has to see the displaced
    /// address rather than assume a replacement is byte-neutral.
    pub(super) fn insert(&mut self, page: BlockIndex, live: &mut BlockSlabLiveIndex) -> u64 {
        let address = page.address.clone();
        let (handle, displaced) = self.insert_unaccounted(page);
        if let Some(displaced) = displaced {
            live.remove_address(&displaced);
        }
        live.add_address(&address);
        handle
    }

    /// Install a block that is ALREADY counted.
    ///
    /// One caller, and it must stay that way: `reload_released_bucket` re-files the blocks a
    /// release took out of the index, and release never discharged them. Counting them here would
    /// double every released bucket the moment it was touched again.
    pub(super) fn insert_released(&mut self, page: BlockIndex) -> u64 {
        self.insert_unaccounted(page).0
    }

    /// The mechanism, with no accounting: the handle assigned, and the address it displaced.
    fn insert_unaccounted(&mut self, page: BlockIndex) -> (u64, Option<BlockAddress>) {
        let handle = block_index_handle(&page);
        let displaced = match self {
            BlockIndexMap::Empty => {
                // THE ONE ALLOCATION THE INLINE ENTRY DID NOT TAKE, and it is taken here. ONE ENTRY
                // and not a growth step: this is a box, so it is sized for exactly the block it holds.
                // That is the whole of boxing's advantage over dropping the arm, which would put the
                // same page in a list whose first block is four entries -- measured at 482.0 B a
                // bucket against 178.3 on the whole-keyspace range.
                *self = BlockIndexMap::One(handle, Box::new(page));
                None
            }
            BlockIndexMap::One(existing, held) => {
                if *existing == handle {
                    // A rewrite of the same block: the entry is replaced INSIDE the box it already
                    // has, so a rewrite of a single-block bucket allocates nothing at all.
                    Some(std::mem::replace(&mut **held, page).address)
                } else {
                    // A second block: this bucket has earned a list. Built SORTED, because every
                    // walk of this index reads it in handle order.
                    let (first_handle, first) = match std::mem::replace(self, BlockIndexMap::Empty) {
                        BlockIndexMap::One(first_handle, first) => (first_handle, *first),
                        _ => unreachable!("just matched One"),
                    };
                    // ONE STEP, not two. A spill takes the same first block `reserve_one_more`
                    // would have taken, so a list of two, three or four costs exactly what
                    // `Vec`'s own first block costs and the step policy is never behind at the
                    // short lengths where most buckets sit. Taking two steps here made a list of
                    // three cost twice what doubling would have;
                    // `the_page_list_growth_step_is_what_keeps_the_slack_off_the_measurement`
                    // is what said so.
                    let mut pages = Vec::with_capacity(PAGE_LIST_GROWTH_STEP);
                    if first_handle < handle {
                        pages.push((first_handle, first));
                        pages.push((handle, page));
                    } else {
                        pages.push((handle, page));
                        pages.push((first_handle, first));
                    }
                    *self = BlockIndexMap::Many(pages);
                    None
                }
            }
            BlockIndexMap::Many(pages) => match find_page(pages, &handle) {
                // A block with this identity is already filed: overwrite it where it sits. A list
                // does not deduplicate by construction the way the tree did, so this is the only
                // thing standing between a rewrite and a bucket holding the same block twice.
                Ok(at) => Some(std::mem::replace(&mut pages[at].1, page).address),
                Err(at) => {
                    reserve_one_more(pages);
                    pages.insert(at, (handle, page));
                    None
                }
            },
        };
        (handle, displaced)
    }

    /// Give the list back once a bucket no longer needs one.
    ///
    /// TWO COLLAPSES, AND BOTH GIVE AN ALLOCATION BACK. At one entry the list becomes a box, which
    /// trades a buffer sized for a whole growth step for one sized for the entry; at zero it becomes
    /// `Empty`, which gives the buffer back outright. Without the second, a bucket emptied by an
    /// expiry sweep would go on holding a list sized for the fifty blocks it used to have; without
    /// the first, a bucket that briefly held two blocks would keep a four-entry buffer for the rest
    /// of its life -- which is the cost this type exists to avoid.
    fn shrink(&mut self) {
        let len = match self {
            BlockIndexMap::Many(pages) => pages.len(),
            _ => return,
        };
        match len {
            0 => *self = BlockIndexMap::Empty,
            1 => {
                let pages = match std::mem::replace(self, BlockIndexMap::Empty) {
                    BlockIndexMap::Many(pages) => pages,
                    _ => unreachable!("just matched Many"),
                };
                let (handle, page) = pages.into_iter().next().expect("length is one");
                *self = BlockIndexMap::One(handle, Box::new(page));
            }
            _ => {}
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            BlockIndexMap::Empty => 0,
            BlockIndexMap::One(..) => 1,
            BlockIndexMap::Many(pages) => pages.len(),
        }
    }

    /// What this block index owns on the heap, in REQUEST bytes.
    ///
    /// HERE, AND NOT AT THE CALL SITE, BECAUSE A CALL SITE CAN ONLY NAME A TYPE. The published
    /// index figure used to spell its own arithmetic as `pages * size_of::<BlockIndex>()`, which is
    /// a reading of the ENTRY where two of the three arms hold something else -- and naming a type
    /// at a call site compiles whatever the field became, so it went on answering a plausible
    /// number after the container underneath it changed shape twice. `pages_per_bucket` charged
    /// `2 * size_of::<Arc<str>>()` for a group that had stopped being a pointer at all for exactly
    /// that reason. Written beside the arms, each term is the `size_of` of the type that arm
    /// actually holds, so a field that changes width moves the figure with it.
    ///
    /// ONE TERM PER ARM, AND THE ARMS GENUINELY DIFFER:
    ///
    ///   * `Empty` owns nothing -- `the_empty_arm_is_an_alias_for_no_pages_and_costs_nothing_to_keep`
    ///     is the standing assertion that this is still true.
    ///   * `One` owns a `Box<BlockIndex>`. The handle rides in the enum, so this allocation is one
    ///     ENTRY wide and the old per-block charge was exactly right for it -- measured 1.0000
    ///     against the allocator at both corpus sizes on the whole-keyspace range, where every
    ///     bucket is single-block. A blanket move to the pair stride would have OVER-charged that
    ///     whole deployment by 12.5%.
    ///   * `Many` owns `Vec<(u64, BlockIndex)>`. Its element is the PAIR, eight bytes wider than
    ///     the entry, and the buffer is owned at CAPACITY -- `reserve_one_more` steps it by
    ///     `PAGE_LIST_GROWTH_STEP` with `reserve_exact`, so a list of five owns a buffer of eight.
    ///
    /// WHAT THIS DELIBERATELY DOES NOT CHARGE, ON A MEASUREMENT AND NOT AN OVERSIGHT. `BlockIndex`
    /// holds `object_key: Arc<str>` and `component: Option<Arc<str>>`, whose bytes live off the
    /// entry. They are not added here because this index does not OWN them: over four fixtures --
    /// routed keys and hash fields, at two corpus sizes, at both routing ranges -- every distinct
    /// name allocation had a strong count higher than the number of index entries holding it, and
    /// dropping every block index in the shard returned the block buffers and not one byte of name.
    /// The model maps own them and a read resolves through those, not through here. Charging them
    /// would double-count, and charging them once per ENTRY rather than once per allocation would
    /// over-count a container store's key names by 100x.
    /// `the_resident_index_report_is_measured_against_the_allocator` is that measurement and fails
    /// if the ownership ever moves.
    ///
    /// AND IT IS A FLOOR, NOT AN EQUALITY -- BY A MEASURED AMOUNT, WHICH IS THE POINT. These are
    /// REQUEST bytes: what the container asked the allocator for. `ALLOC_CHUNK_BYTES` reads
    /// `malloc_usable_size`, which is a floor above the request and not an equality (#1969: a
    /// 104-byte request read 128). Modelling a rounding rule into a shipped figure would make it a
    /// claim about the platform's allocator, so this charges the request and the size of what it
    /// misses is MEASURED rather than left unknown. Replaying this index's own allocation size
    /// distribution through the counting allocator:
    ///
    /// ```text
    ///   population                      allocations   chunk/request   this figure / chunk
    ///   routed, whole keyspace, 4,000         4,000         1.2634x                0.7915
    ///   routed, whole keyspace, 40,000       40,000         1.2543x                0.7972
    ///   routed, 0..1023, 4,000                1,024         1.0449x                0.9571
    ///   routed, 0..1023, 40,000               1,024         1.0055x                0.9945
    ///   hash fields, either range, 4,000         40         1.0022x                0.9978
    ///   hash fields, either range, 40,000   350-400    1.0020-1.0022x         0.9978-0.9980
    /// ```
    ///
    /// SO THE ERROR IS SIGNED AND ITS SIZE IS KNOWN: this reads 0.2% to 4.3% below what the
    /// allocator holds wherever the index is list-shaped, and about 20% below on the whole-keyspace
    /// range, where every bucket is one boxed 64-byte entry and glibc's 16-byte rounding is paid
    /// once per block instead of once per list. It is never HIGH. An eviction gate reading this can
    /// treat it as a lower bound on the index's real footprint, which is the direction that makes
    /// it safe to act on.
    pub(super) fn resident_heap_bytes(&self) -> u64 {
        match self {
            BlockIndexMap::Empty => 0,
            // `Box<BlockIndex>`: one entry, and the handle is inline in the enum.
            BlockIndexMap::One(_handle, _page) => std::mem::size_of::<BlockIndex>() as u64,
            // `Vec<(u64, BlockIndex)>`: the element is the pair, and the buffer is owned at the
            // capacity it was reserved to rather than the length that is filled.
            BlockIndexMap::Many(pages) => (pages.capacity() as u64)
                .saturating_mul(std::mem::size_of::<(u64, BlockIndex)>() as u64),
        }
    }

    /// Whether this bucket holds no blocks.
    ///
    /// READS THE LENGTH, NOT THE DISCRIMINANT. `Empty` and `Many(vec![])` are both "no blocks", and
    /// `shrink` normalises the second into the first -- but a predicate that trusted the
    /// normalisation would answer `false` for an empty list if any path ever missed it, and the
    /// release lifecycle branches on this. Reading the length makes the two indistinguishable by
    /// construction instead of by convention.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn iter(&self) -> BlockIndexIter<'_> {
        match self {
            BlockIndexMap::Empty => BlockIndexIter::Empty,
            BlockIndexMap::One(handle, page) => {
                BlockIndexIter::One(std::iter::once((handle, &**page)))
            }
            BlockIndexMap::Many(pages) => BlockIndexIter::Many(pages.iter()),
        }
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &BlockIndex> {
        self.iter().map(|(_handle, page)| page)
    }

    /// Mutable blocks, UNACCOUNTED.
    ///
    /// Named for the contract rather than for the shape, because the shape cannot express it: a
    /// holder of `&mut BlockIndex` can rewrite `address`, and the live tally keys on
    /// `address.block_slab_id` and sums `address.length`. A mutation through here must change
    /// NEITHER. Everything else is fair game -- today's callers set `dirty`, and two tests
    /// deliberately corrupt `object_id` and `routing_bucket`, none of which the tally reads.
    ///
    /// A block that needs to MOVE goes through `insert`, which discharges the address it displaces
    /// and charges the new one.
    ///
    /// This is a NAMED boundary, not a compiler-enforced one: enforcing it would mean making
    /// `BlockIndex::address` private, and it is read in three figures of places.
    /// `the_maintained_slab_live_tally_matches_the_walk` is what fails if the name stops being
    /// obeyed.
    pub(super) fn blocks_mut_unaccounted(&mut self) -> BlockIndexValuesMut<'_> {
        match self {
            BlockIndexMap::Empty => BlockIndexValuesMut::Empty,
            BlockIndexMap::One(_, page) => BlockIndexValuesMut::One(std::iter::once(&mut **page)),
            BlockIndexMap::Many(pages) => BlockIndexValuesMut::Many(pages.iter_mut()),
        }
    }

    /// Drop the blocks a predicate rejects, discharging each from the live tally as it goes.
    ///
    /// `live` comes first so the predicate stays the trailing argument it was.
    pub(super) fn retain(
        &mut self,
        live: &mut BlockSlabLiveIndex,
        mut keep: impl FnMut(&u64, &mut BlockIndex) -> bool,
    ) {
        match self {
            BlockIndexMap::Empty => {}
            BlockIndexMap::One(handle, page) => {
                let handle = *handle;
                if !keep(&handle, page) {
                    live.remove_address(&page.address);
                    *self = BlockIndexMap::Empty;
                }
            }
            BlockIndexMap::Many(pages) => {
                pages.retain_mut(|(handle, page)| {
                    let kept = keep(&*handle, page);
                    if !kept {
                        live.remove_address(&page.address);
                    }
                    kept
                });
                self.shrink();
            }
        }
    }
}

impl<'a> IntoIterator for &'a BlockIndexMap {
    type Item = (&'a u64, &'a BlockIndex);
    type IntoIter = BlockIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Collecting blocks assigns handles, the same as inserting them one at a time.
impl FromIterator<BlockIndex> for BlockIndexMap {
    fn from_iter<I: IntoIterator<Item = BlockIndex>>(pages: I) -> Self {
        // Unaccounted: this is how a ShardState arrives from serde, and the tally is derived
        // AFTER a load by `reconcile_block_slab_live`. Charging blocks here would count a loaded
        // index twice over -- once on the way in, once when the load seeds the tally.
        let mut map = Self::default();
        for page in pages {
            map.insert_unaccounted(page);
        }
        map
    }
}

impl From<BTreeMap<String, BlockIndex>> for BlockIndexMap {
    fn from(flat: BTreeMap<String, BlockIndex>) -> Self {
        // The handle is recomputed from the block, not read from the file and not assigned by a
        // counter. A counter would hand out different handles than the ones the lookup refs were
        // written with, and those refs are on disk too.
        //
        // Built through `insert`, so a loaded index takes the same shape a written one does: a
        // bucket that loads a single block must not come back holding a map.
        flat.into_values().collect()
    }
}

impl Serialize for BlockIndexMap {
    /// Writes the same map of rendered keys, in the same order, without copying the index first.
    ///
    /// Hand-written rather than `#[serde(into = ...)]`, which is defined as
    /// `T::from(self.clone()).serialize(..)`: that duplicates the whole index twice over -- once
    /// cloning it, once building the converted map -- before a byte is written.
    ///
    /// The sort is not incidental. The map this used to convert into was keyed by the rendered
    /// string, so it emitted entries in string order; this map is keyed by a handle and iterates
    /// in hash order. Writing them as they come would reorder every dump, which is a change to
    /// the bytes on disk rather than to how they are produced.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut entries: Vec<(String, &BlockIndex)> = self
            .values()
            .map(|page| (block_index_written_key(page), page))
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, page) in &entries {
            map.serialize_entry(key, page)?;
        }
        map.end()
    }
}

/// The key this map writes, rebuilt from the block it is stored against.
///
/// The same spelling the map used to hold, so a dump written now reads the same as one written
/// before. Shared with the replay log so the two cannot drift.
/// The in-memory handle for a block: its identity, hashed.
///
/// Derived rather than assigned, because handles are written to disk inside the lookup's refs.
/// Two processes holding the same block must compute the same handle or those refs point at
/// nothing -- which is what a counter did, silently, until a reload lost an object.
///
/// Hashes exactly the fields [`block_index_written_key`] renders, so the handle and the written
/// key always name the same block. Equal identity therefore lands on one slot, which is also how
/// this map keeps a rewrite from accumulating a second entry for the same block.
pub(super) fn block_index_handle(page: &BlockIndex) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    page.model_id.hash(&mut hasher);
    page.object_key.hash(&mut hasher);
    page.component.as_deref().hash(&mut hasher);
    page.address.block_slab_id().hash(&mut hasher);
    page.address.offset().hash(&mut hasher);
    page.address.length().hash(&mut hasher);
    page.address.block_id().unwrap_or_default().hash(&mut hasher);
    page.address.generation().unwrap_or_default().hash(&mut hasher);
    hasher.finish()
}

pub(super) fn block_index_written_key(page: &BlockIndex) -> String {
    crate::index_log::block_ref_key_from_parts(
        page.model_id.as_str(),
        &page.object_key,
        page.component.as_deref(),
        page.address.block_slab_id(),
        page.address.offset(),
        page.address.length(),
        page.address.block_id().unwrap_or_default(),
        page.address.generation().unwrap_or_default(),
    )
}
/// One entry per OBJECT, with its components nested inside.
///
/// This replaced two maps keyed by overlapping composites -- (model, object) and
/// (model, object, component) -- where the shorter key was a byte-for-byte prefix of the longer
/// one, so every record stored the (model, object) head twice: once as a whole key and once as the
/// head of a longer one. Measured across both maps at 4000 records: 223588 B of keys held, 110780
/// nested, a saving of 50.5%. The entry count halves too, because the second map is gone rather
/// than nested.
///
/// It also makes `len()` the count of distinct OBJECTS, which is the number the stats path
/// reports. That question is precisely why the per-component map could not simply be dropped in
/// favour of a range scan over the other: a scan of a map keyed by (object, component) cannot
/// count distinct objects without walking every entry.
/// Blocks by object, nested under the model that owns them.
///
/// Flat, this was keyed by a `model|object` concatenation: a string built for every stored object
/// and rebuilt for every lookup. Measured at 37 B per object -- more than either copy of the object
/// key itself -- and unshareable, because it is a different string from the key it contains.
///
/// Nested, the outer key is the model, which is already a shared pointer, and nothing is
/// concatenated. A lookup walks two maps instead of building a string.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    from = "BTreeMap<String, ObjectBlockRefs>",
    into = "BTreeMap<String, ObjectBlockRefs>"
)]
pub(super) struct ObjectBlockLookup {
    by_model: BTreeMap<Arc<str>, BTreeMap<Arc<str>, ObjectBlockRefs>>,
}

impl ObjectBlockLookup {
    pub(super) fn get(&self, model_id: &str, object_key: &str) -> Option<&ObjectBlockRefs> {
        self.by_model.get(model_id)?.get(object_key)
    }

    pub(super) fn get_mut(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&mut ObjectBlockRefs> {
        self.by_model.get_mut(model_id)?.get_mut(object_key)
    }

    /// The entry for this object, created empty if absent. Takes the model by shared pointer so
    /// the outer key costs nothing to store.
    /// Takes both keys by shared pointer. The object key is the one the block entry already
    /// holds, so filing a block adds a pointer rather than a second copy of its identity.
    pub(super) fn entry(
        &mut self,
        model_id: &Arc<str>,
        object_key: &Arc<str>,
    ) -> &mut ObjectBlockRefs {
        let objects = self.by_model.entry(Arc::clone(model_id)).or_default();
        if !objects.contains_key(object_key.as_ref()) {
            objects.insert(Arc::clone(object_key), ObjectBlockRefs::default());
        }
        objects
            .get_mut(object_key.as_ref())
            .expect("just inserted")
    }

    /// The allocation this map already holds for an object's key, for a block about to be filed
    /// under that object.
    ///
    /// The write path used to build `Arc::from(object_key)` for every block. For a store of keys
    /// that route one to a bucket that is right -- there is one block, and the allocation it makes
    /// is the one the map then keeps. For a CONTAINER it is a hundred allocations of one short
    /// string, because every field, member and element is filed by its own call and each call
    /// started again. Ninety-nine of the hundred carried nothing the first did not, and pointer
    /// identity is the only thing that could ever have told them apart.
    ///
    /// Answers `None` before the object has any blocks, which is the first call for a new object
    /// and the one that must allocate. The caller falls back to allocating then.
    pub(super) fn shared_object_key(&self, model_id: &str, object_key: &str) -> Option<Arc<str>> {
        let (stored, _) = self.by_model.get(model_id)?.get_key_value(object_key)?;
        Some(Arc::clone(stored))
    }

    /// The address of the inner key allocation, so a test can assert that a block and this map
    /// point at one copy of the object identity rather than two equal ones. Contents cannot tell
    /// those apart; pointers can.
    #[cfg(test)]
    pub(super) fn key_ptr(&self, model_id: &str, object_key: &str) -> Option<*const u8> {
        let (stored, _) = self.by_model.get(model_id)?.get_key_value(object_key)?;
        Some(stored.as_ptr())
    }

    pub(super) fn remove(&mut self, model_id: &str, object_key: &str) -> Option<ObjectBlockRefs> {
        let objects = self.by_model.get_mut(model_id)?;
        let removed = objects.remove(object_key);
        if objects.is_empty() {
            self.by_model.remove(model_id);
        }
        removed
    }

    /// The number of objects, which is what the flat map's length meant.
    pub(super) fn len(&self) -> usize {
        self.by_model.values().map(BTreeMap::len).sum()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_model.values().all(BTreeMap::is_empty)
    }

    pub(super) fn clear(&mut self) {
        self.by_model.clear();
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &ObjectBlockRefs> {
        self.by_model.values().flat_map(BTreeMap::values)
    }

    /// Model and object for every entry, for the places that used to read the composite key.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &Arc<str>, &ObjectBlockRefs)> {
        self.by_model.iter().flat_map(|(model, objects)| {
            objects.iter().map(move |(object, refs)| (model, object, refs))
        })
    }
}

/// One part of the flat key: a decimal length, a colon, the bytes, a bar. Length-prefixed, so a
/// value containing the separator cannot be mistaken for a boundary.
fn take_lookup_part(input: &str) -> Option<(&str, &str)> {
    let colon = input.find(':')?;
    let len: usize = input[..colon].parse().ok()?;
    let start = colon + 1;
    let end = start.checked_add(len)?;
    if input.len() <= end || input.as_bytes()[end] != b'|' {
        return None;
    }
    Some((&input[start..end], &input[end + 1..]))
}

impl From<BTreeMap<String, ObjectBlockRefs>> for ObjectBlockLookup {
    fn from(flat: BTreeMap<String, ObjectBlockRefs>) -> Self {
        let mut nested: BTreeMap<Arc<str>, BTreeMap<Arc<str>, ObjectBlockRefs>> = BTreeMap::new();
        for (key, refs) in flat {
            // A key that does not parse is skipped rather than guessed at: inventing a model for
            // it would file the object somewhere no lookup would ever look.
            let Some((model, rest)) = take_lookup_part(&key) else {
                continue;
            };
            let Some((object, tail)) = take_lookup_part(rest) else {
                continue;
            };
            if !tail.is_empty() {
                continue;
            }
            nested
                .entry(Arc::from(model))
                .or_default()
                .insert(Arc::from(object), refs);
        }
        Self { by_model: nested }
    }
}

impl From<ObjectBlockLookup> for BTreeMap<String, ObjectBlockRefs> {
    fn from(nested: ObjectBlockLookup) -> Self {
        let mut flat = BTreeMap::new();
        for (model, objects) in nested.by_model {
            for (object, refs) in objects {
                flat.insert(object_component_lookup_key(&model, &object), refs);
            }
        }
        flat
    }
}

/// The block refs of one object, grouped by component and ordered by it.
///
/// A sorted vector rather than a map because the measured average is 1.0 components per object: a
/// B-tree holding a single entry is a node and an allocation spent to express a list of one.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectBlockRefs {
    #[serde(default)]
    pub(super) by_component: ComponentList,
}

/// One per object in the lookup.
const _: () = assert!(std::mem::size_of::<ObjectBlockRefs>() == 40);

/// The components of one object: none, one, or a sorted vector.
///
/// The measured average is 1.0 components per object, and a `Vec` holding a single element is a
/// heap allocation and its allocator rounding spent to express a list of one. The shape
/// `BlockIndexMap`, `BlockRefs` and `ObjectIndex` already use, for the same reason.
///
/// It carries the slice of `Vec`'s surface the lookup actually uses, so the call sites read as
/// they did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) enum ComponentList {
    #[default]
    Empty,
    One(ComponentBlocks),
    Many(Vec<ComponentBlocks>),
}

/// Its `One` arm is a whole `ComponentBlocks`; the tag rides a niche in the shared name.
const _: () = assert!(std::mem::size_of::<ComponentList>() == 40);

impl ComponentList {
    pub(super) fn len(&self) -> usize {
        match self {
            ComponentList::Empty => 0,
            ComponentList::One(_) => 1,
            ComponentList::Many(list) => list.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, ComponentList::Empty)
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, ComponentBlocks> {
        match self {
            // A const-promoted empty slice, so the iterator does not borrow a temporary.
            ComponentList::Empty => (&[] as &[ComponentBlocks]).iter(),
            ComponentList::One(entry) => std::slice::from_ref(entry).iter(),
            ComponentList::Many(list) => list.iter(),
        }
    }

    /// `Vec::binary_search_by`'s contract: `f` orders the ELEMENT against the target, `Ok` is the
    /// position of a match and `Err` the position it would be inserted at.
    pub(super) fn binary_search_by<F>(&self, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(&ComponentBlocks) -> std::cmp::Ordering,
    {
        match self {
            ComponentList::Empty => Err(0),
            ComponentList::One(entry) => match f(entry) {
                std::cmp::Ordering::Equal => Ok(0),
                // The held entry sorts before the target, so the target goes after it.
                std::cmp::Ordering::Less => Err(1),
                std::cmp::Ordering::Greater => Err(0),
            },
            ComponentList::Many(list) => list.binary_search_by(f),
        }
    }

    pub(super) fn insert(&mut self, at: usize, value: ComponentBlocks) {
        match self {
            ComponentList::Empty => {
                assert_eq!(at, 0, "the only position in an empty list is 0");
                *self = ComponentList::One(value);
            }
            ComponentList::One(_) => {
                let held = match std::mem::replace(self, ComponentList::Empty) {
                    ComponentList::One(held) => held,
                    _ => unreachable!("just matched One"),
                };
                let mut list = Vec::with_capacity(2);
                list.push(held);
                list.insert(at, value);
                *self = ComponentList::Many(list);
            }
            ComponentList::Many(list) => list.insert(at, value),
        }
    }

    /// Removes and returns the entry at `at`, giving up the vector once it no longer earns one so
    /// an object that briefly held two components does not keep it for the rest of its life.
    pub(super) fn remove(&mut self, at: usize) -> ComponentBlocks {
        match self {
            ComponentList::Empty => panic!("removal from an empty component list"),
            ComponentList::One(_) => {
                assert_eq!(at, 0, "the only position in a list of one is 0");
                match std::mem::replace(self, ComponentList::Empty) {
                    ComponentList::One(held) => held,
                    _ => unreachable!("just matched One"),
                }
            }
            ComponentList::Many(list) => {
                let removed = list.remove(at);
                match list.len() {
                    0 => *self = ComponentList::Empty,
                    1 => *self = ComponentList::One(list.pop().expect("length is one")),
                    _ => {}
                }
                removed
            }
        }
    }
}

impl std::ops::Index<usize> for ComponentList {
    type Output = ComponentBlocks;
    fn index(&self, at: usize) -> &ComponentBlocks {
        match self {
            ComponentList::Empty => panic!("index {at} into an empty component list"),
            ComponentList::One(entry) => {
                assert_eq!(at, 0, "the only position in a list of one is 0");
                entry
            }
            ComponentList::Many(list) => &list[at],
        }
    }
}

impl std::ops::IndexMut<usize> for ComponentList {
    fn index_mut(&mut self, at: usize) -> &mut ComponentBlocks {
        match self {
            ComponentList::Empty => panic!("index {at} into an empty component list"),
            ComponentList::One(entry) => {
                assert_eq!(at, 0, "the only position in a list of one is 0");
                entry
            }
            ComponentList::Many(list) => &mut list[at],
        }
    }
}

impl<'a> IntoIterator for &'a ComponentList {
    type Item = &'a ComponentBlocks;
    type IntoIter = std::slice::Iter<'a, ComponentBlocks>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Serialize for ComponentList {
    /// The same sequence it has always written, in the same order.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for entry in self.iter() {
            seq.serialize_element(entry)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for ComponentList {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // A loaded object takes the shape a written one does: one component comes back held
        // inline rather than in a vector.
        let mut list = Vec::<ComponentBlocks>::deserialize(deserializer)?;
        Ok(match list.len() {
            0 => ComponentList::Empty,
            1 => ComponentList::One(list.pop().expect("length is one")),
            _ => ComponentList::Many(list),
        })
    }
}

/// One component of one object, and the blocks holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ComponentBlocks {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<Arc<str>>,
    /// Sorted and deduplicated, and held inline when there is only one -- which is all of them.
    /// Insertion goes through `BlockRefs::insert`, which keeps both invariants.
    #[serde(default)]
    pub(super) refs: BlockRefs,
}

/// One per (object, component), and the measured average is one component per object.
const _: () = assert!(std::mem::size_of::<ComponentBlocks>() == 40);

impl ObjectBlockRefs {
    /// Where this component sits, or where it would be inserted. `None` sorts first, matching
    /// `Option`'s own ordering, so the vector's order is the order a caller would expect.
    pub(super) fn position(&self, component: Option<&str>) -> Result<usize, usize> {
        self.by_component
            .binary_search_by(|entry| entry.component.as_deref().cmp(&component))
    }

    pub(super) fn refs_for(&self, component: Option<&str>) -> Option<&[BlockLookupRef]> {
        self.position(component)
            .ok()
            .map(|at| self.by_component[at].refs.as_slice())
    }

    /// Every block ref of this object, across every component, in component order.
    pub(super) fn all_refs(&self) -> impl Iterator<Item = &BlockLookupRef> {
        self.by_component.iter().flat_map(|entry| entry.refs.iter())
    }

    pub(super) fn total_refs(&self) -> usize {
        self.by_component.iter().map(|entry| entry.refs.len()).sum()
    }
}

/// A kind is drawn from a fixed set of literals in the code, so a pool of them is bounded. A
/// component name is not -- it comes from the caller -- and the cap is what makes sharing it safe
/// anyway: past the cap a name still works, it just allocates as it did before. So the cap is not
/// a tuning knob, it is the thing that lets an unbounded input share a bounded pool.
const KIND_POOL_CAP: usize = 64;

/// One shared copy of `kind`, taken from the pool or added to it.
pub(super) fn intern_shared(pool: &mut std::collections::HashSet<Arc<str>>, kind: &str) -> Arc<str> {
    intern_up_to(pool, kind, KIND_POOL_CAP)
}

/// Headroom in the same pool that only a KIND may use.
///
/// Components are one per field, member or element, so they reach `KIND_POOL_CAP` on the first
/// container key written and then the pool takes nothing new. Kinds are a closed set this engine
/// spells itself -- about five -- so a reserve this size is never the binding constraint for them
/// and the pool stays bounded by `KIND_POOL_CAP + KIND_RESERVE` either way.
const KIND_RESERVE: usize = 16;

/// Intern a KIND. Sees the reserve; a component does not.
pub(super) fn intern_kind(pool: &mut std::collections::HashSet<Arc<str>>, kind: &str) -> Arc<str> {
    intern_up_to(pool, kind, KIND_POOL_CAP + KIND_RESERVE)
}

fn intern_up_to(
    pool: &mut std::collections::HashSet<Arc<str>>,
    name: &str,
    ceiling: usize,
) -> Arc<str> {
    if let Some(shared) = pool.get(name) {
        return Arc::clone(shared);
    }
    let shared: Arc<str> = Arc::from(name);
    if pool.len() < ceiling {
        pool.insert(Arc::clone(&shared));
    }
    shared
}

/// The blocks holding one component, with the single-block case held inline.
///
/// Measured over a corpus mixing single-value objects and multi-field ones: 3600 of 3600
/// components hold exactly one ref. That is not a property of the workload mix the way the
/// component count per object is -- it held for both object shapes -- so the single case is worth
/// having a shape for.
///
/// A `Vec` for one element costs a 24-byte header and, more expensively, its own heap allocation
/// to hold a single 24-byte value. Inline costs 8 bytes more inside the enclosing vector and no
/// allocation at all. The spilled arm keeps the behaviour unchanged for a component that does span
/// several blocks -- nothing in the measured corpus does, but the format allows it, so it stays
/// representable rather than being asserted away.
///
/// Serializes as a sequence exactly as the vector did, so the on-disk index is unchanged.
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<BlockLookupRef>", into = "Vec<BlockLookupRef>")]
pub(super) enum BlockRefs {
    One(BlockLookupRef),
    Many(Vec<BlockLookupRef>),
}

/// As wide as its `Many` arm's vector; the tag rides the padding of the inline ref.
const _: () = assert!(std::mem::size_of::<BlockRefs>() == 24);

impl BlockRefs {
    pub(super) fn as_slice(&self) -> &[BlockLookupRef] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values.as_slice(),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::One(_) => 1,
            Self::Many(values) => values.len(),
        }
    }

    pub(super) fn iter(&self) -> std::slice::Iter<'_, BlockLookupRef> {
        self.as_slice().iter()
    }

    /// Insert keeping the refs sorted and free of duplicates, which is what the set this replaced
    /// did. Reports whether anything was added, which is what the ref counter is kept from.
    pub(super) fn insert(&mut self, value: BlockLookupRef) -> bool {
        match self {
            Self::One(existing) => match value.cmp(existing) {
                std::cmp::Ordering::Equal => false,
                std::cmp::Ordering::Less => {
                    *self = Self::Many(vec![value, existing.clone()]);
                    true
                }
                std::cmp::Ordering::Greater => {
                    *self = Self::Many(vec![existing.clone(), value]);
                    true
                }
            },
            Self::Many(values) => match values.binary_search(&value) {
                Ok(_) => false,
                Err(at) => {
                    values.insert(at, value);
                    true
                }
            },
        }
    }
}

impl Default for BlockRefs {
    fn default() -> Self {
        Self::Many(Vec::new())
    }
}

/// Compared by contents, so a one-element spilled arm and an inline one are the same refs. Without
/// this, a value read back from an index written before this change could compare unequal to the
/// identical value built in memory.
impl PartialEq for BlockRefs {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl From<Vec<BlockLookupRef>> for BlockRefs {
    fn from(mut refs: Vec<BlockLookupRef>) -> Self {
        if refs.len() == 1 {
            Self::One(refs.pop().expect("length just checked"))
        } else {
            Self::Many(refs)
        }
    }
}

impl From<BlockRefs> for Vec<BlockLookupRef> {
    fn from(refs: BlockRefs) -> Self {
        match refs {
            BlockRefs::One(value) => vec![value],
            BlockRefs::Many(values) => values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct BlockLookupRef {
    #[serde(rename = "routing_slot")]
    pub(super) routing_bucket: u32,
    pub(super) block_ref_key: u64,
}

/// TWELVE BYTES OF FIELD IN SIXTEEN. This one is alignment, not width: a `u32` beside a `u64`
/// pads to the `u64`'s alignment, and narrowing either field moves nothing. It is pinned so the
/// waste is visible, not because it can be reclaimed here.
const _: () = assert!(std::mem::size_of::<BlockLookupRef>() == 16);

/// Rust-native core index mirroring the shape:
/// The shard's dirty objects, indexed BY ROUTING BUCKET as well as by key.
///
/// A flat `BTreeSet<String>` of keys answers "is this object dirty" and nothing else, so every
/// consumer that wanted "which buckets are dirty" or "how many dirty objects does this bucket
/// hold" re-derived the answer by hashing every key in the set. `bucket_storage_summaries` does
/// it three times in one `apply_storage_lifecycle` and again on each metrics scrape; the dump
/// drain did it once more. The set grows with the ingest, so all of that grew with the store.
///
/// The bucket is not a fact that has to be recomputed. It is computed ONCE, at the moment an
/// object is marked dirty, by the only two sites that mark one -- and both already held it and
/// threw it away. Keeping it turns each of those questions into a lookup and makes the drain
/// visit the cleared buckets' keys instead of the whole set.
///
/// The key text is stored once: `by_bucket` holds the same `Arc<str>` as `by_key`, so the second
/// index costs a pointer and a tree node per dirty object rather than a second copy of the key.
///
/// NOT serialized, like the set it replaces. A load clears every dirty flag -- reloaded data is
/// durable, hence clean -- so a reloaded shard starts with this empty and fills it from live
/// writes.
#[derive(Debug, Default, Clone)]
pub(super) struct DirtyObjectIndex {
    by_key: BTreeMap<Arc<str>, u32>,
    by_bucket: BTreeMap<u32, DirtyKeySet>,
}

/// The dirty object keys of ONE bucket.
///
/// Almost always exactly one AT THE DEFAULT ROUTING RANGE, and almost never otherwise.
/// `what_a_live_key_costs_in_the_index_at_two_corpus_sizes` measured 40,040 of 40,040 buckets
/// holding exactly one key at 80,000 records and 4,004 of 4,004 at 8,000 -- on `load_shard`'s
/// default of `0..u32::MAX`, where a key lands alone by construction because the placement
/// modulus is the RANGE WIDTH. On the range `docs/runtime_tuning.md` tells an operator to set,
/// `TS_SHARD_END_ROUTING_BUCKET=1023`, it is 54 of 1,024 at 4,000 records: 94.7% of dirty
/// buckets hold more than one key and sit on the `Many` arm below. `bucket_fill.rs` reports
/// both, and drains one bucket to show what that costs a dump.
///
/// So the inline arm is earned by the DEFAULT RANGE rather than by the workload, and the
/// sizing argument that follows holds at that range and is the wrong way round at a narrow one.
///
/// A `BTreeSet<Arc<str>>` holding a single key costs 192 live bytes of node -- a leaf sized for
/// eleven 16-byte pointers -- to carry one of them, once per dirty object. At 40,040 dirty objects
/// that was 7.7 MB of an index whose whole live heap is 42.3 MB, and it made `dirty_objects` the
/// second largest structure in the shard, larger than the `strings` map holding the addresses the
/// reads actually resolve through.
///
/// The shape `ObjectIndex`, `BlockIndexMap`, `ComponentList` and `BlockRefs` already use, for the
/// same reason. `Many` is boxed on the same grounds `ObjectIndex` boxes its own: an enum is as wide
/// as its widest arm, the set arm is the rare one, and this value sits in eleven slots of every
/// node of the map above.
///
/// NO FORMAT CHANGE IS POSSIBLE HERE. `DirtyObjectIndex` is `#[serde(skip)]` on `ShardState` -- a
/// load clears every dirty flag, so this is rebuilt from live writes and never read back off disk.
#[derive(Debug, Default, Clone)]
pub(super) enum DirtyKeySet {
    #[default]
    Empty,
    One(Arc<str>),
    Many(Box<BTreeSet<Arc<str>>>),
}

/// One per dirty bucket, and the measured distribution is one key per bucket.
const _: () = assert!(std::mem::size_of::<DirtyKeySet>() == 24);

impl DirtyKeySet {
    pub(super) fn len(&self) -> usize {
        match self {
            DirtyKeySet::Empty => 0,
            DirtyKeySet::One(_) => 1,
            DirtyKeySet::Many(set) => set.len(),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, DirtyKeySet::Empty)
    }

    pub(super) fn insert(&mut self, key: Arc<str>) -> bool {
        match self {
            DirtyKeySet::Empty => {
                *self = DirtyKeySet::One(key);
                true
            }
            DirtyKeySet::One(held) => {
                if **held == *key {
                    return false;
                }
                let first = match std::mem::replace(self, DirtyKeySet::Empty) {
                    DirtyKeySet::One(first) => first,
                    _ => unreachable!("just matched One"),
                };
                let mut set = BTreeSet::new();
                set.insert(first);
                set.insert(key);
                *self = DirtyKeySet::Many(Box::new(set));
                true
            }
            DirtyKeySet::Many(set) => set.insert(key),
        }
    }

    /// Drop `key` and hand back the `Arc` that held it, so a key MOVING between buckets keeps its
    /// one allocation instead of taking a second. This is what `DirtyObjectIndex::insert` needs,
    /// and it is why removal is spelled `take` rather than returning a bool.
    pub(super) fn take(&mut self, key: &str) -> Option<Arc<str>> {
        match self {
            DirtyKeySet::Empty => None,
            DirtyKeySet::One(held) => {
                if &**held != key {
                    return None;
                }
                match std::mem::replace(self, DirtyKeySet::Empty) {
                    DirtyKeySet::One(held) => Some(held),
                    _ => unreachable!("just matched One"),
                }
            }
            DirtyKeySet::Many(set) => {
                let taken = set.take(key);
                self.shrink();
                taken
            }
        }
    }

    pub(super) fn remove(&mut self, key: &str) -> bool {
        self.take(key).is_some()
    }

    /// Give up the set once it no longer earns one, so a bucket that briefly held two dirty keys
    /// does not keep a node for the rest of the round.
    fn shrink(&mut self) {
        let len = match self {
            DirtyKeySet::Many(set) => set.len(),
            _ => return,
        };
        match len {
            0 => *self = DirtyKeySet::Empty,
            1 => {
                let set = match std::mem::replace(self, DirtyKeySet::Empty) {
                    DirtyKeySet::Many(set) => set,
                    _ => unreachable!("just matched Many"),
                };
                *self = DirtyKeySet::One((*set).into_iter().next().expect("length is one"));
            }
            _ => {}
        }
    }
}

impl IntoIterator for DirtyKeySet {
    type Item = Arc<str>;
    type IntoIter = std::vec::IntoIter<Arc<str>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            DirtyKeySet::Empty => Vec::new().into_iter(),
            DirtyKeySet::One(key) => vec![key].into_iter(),
            DirtyKeySet::Many(set) => (*set).into_iter().collect::<Vec<_>>().into_iter(),
        }
    }
}

impl DirtyObjectIndex {
    pub(super) fn len(&self) -> usize {
        self.by_key.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub(super) fn contains(&self, object_key: &str) -> bool {
        self.by_key.contains_key(object_key)
    }

    /// Every dirty object key, in key order -- what iterating the flat set gave.
    pub(super) fn iter(&self) -> impl Iterator<Item = &str> + '_ {
        self.by_key.keys().map(|key| &**key)
    }

    /// Mark `object_key` dirty under `routing_bucket`.
    ///
    /// Re-marking a key under a DIFFERENT bucket MOVES it rather than leaving it in both, which
    /// is the state a shard whose routing range changed under it would otherwise reach. Both
    /// indexes are updated together, so neither can hold a key the other does not.
    pub(super) fn insert(&mut self, object_key: &str, routing_bucket: u32) {
        // Below both entry paths: the engine's write path calls this index directly and also goes
        // through `mark_async_dirty_object`, so a charge on either wrapper would see one of the
        // two. The same reason the walk counters in `storage_bucket_internals` moved inward.
        crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::DirtyObjects, || {
            self.insert_inner(object_key, routing_bucket)
        })
    }

    fn insert_inner(&mut self, object_key: &str, routing_bucket: u32) {
        if let Some(existing) = self.by_key.get_mut(object_key) {
            if *existing == routing_bucket {
                return;
            }
            let previous = std::mem::replace(existing, routing_bucket);
            let moved = match self.by_bucket.get_mut(&previous) {
                Some(keys) => {
                    let taken = keys.take(object_key);
                    if keys.is_empty() {
                        self.by_bucket.remove(&previous);
                    }
                    taken
                }
                None => None,
            };
            let shared = moved.unwrap_or_else(|| Arc::from(object_key));
            self.by_bucket
                .entry(routing_bucket)
                .or_default()
                .insert(shared);
            return;
        }
        let shared: Arc<str> = Arc::from(object_key);
        self.by_key.insert(Arc::clone(&shared), routing_bucket);
        self.by_bucket
            .entry(routing_bucket)
            .or_default()
            .insert(shared);
    }

    /// Empty both indexes. Used by the resident-memory probe, which measures what a field
    /// holds by dropping it.
    pub(super) fn clear(&mut self) {
        self.by_key.clear();
        self.by_bucket.clear();
    }

    /// Drop `object_key`. Returns whether it was dirty.
    pub(super) fn remove(&mut self, object_key: &str) -> bool {
        let Some(routing_bucket) = self.by_key.remove(object_key) else {
            return false;
        };
        if let Some(keys) = self.by_bucket.get_mut(&routing_bucket) {
            keys.remove(object_key);
            if keys.is_empty() {
                self.by_bucket.remove(&routing_bucket);
            }
        }
        true
    }

    /// Drop every dirty object belonging to any of `buckets`, and report how many were dropped.
    ///
    /// This is the drain a dump runs once its manifest is durable. It looks at the keys of the
    /// named buckets and at nothing else, so a round that dumps a slice of the shard pays for
    /// that slice rather than for the whole set.
    pub(super) fn drain_buckets(&mut self, buckets: &[u32]) -> usize {
        let mut dropped = 0;
        for routing_bucket in buckets {
            let Some(keys) = self.by_bucket.remove(routing_bucket) else {
                continue;
            };
            for key in keys {
                self.by_key.remove(&key);
                dropped += 1;
            }
        }
        dropped
    }

    /// Per-bucket dirty object counts, in bucket order: ONE entry per dirty BUCKET, where the
    /// flat set gave one per dirty OBJECT.
    pub(super) fn bucket_counts(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        self.by_bucket
            .iter()
            .map(|(routing_bucket, keys)| (*routing_bucket, keys.len() as u64))
    }

    /// The routing buckets holding at least one dirty object.
    pub(super) fn bucket_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.by_bucket.keys().copied()
    }

    /// How many buckets sit in each arm of `DirtyKeySet`: (Empty, One, Many).
    ///
    /// The arm is the footprint -- `One` holds a pointer, `Many` holds a B-tree node sized for
    /// eleven of them -- so this is what a guard on the cost of this index has to read. It cannot
    /// be derived from `len()`: a bucket holding one key reports 1 from either arm, which is
    /// exactly the regression worth catching.
    ///
    /// `Empty` should never be observed: a set that empties is dropped from the map by the caller.
    /// It is reported rather than asserted away so a guard can say so.
    pub(super) fn bucket_arms(&self) -> (usize, usize, usize) {
        let mut arms = (0usize, 0usize, 0usize);
        for keys in self.by_bucket.values() {
            match keys {
                DirtyKeySet::Empty => arms.0 += 1,
                DirtyKeySet::One(_) => arms.1 += 1,
                DirtyKeySet::Many(_) => arms.2 += 1,
            }
        }
        arms
    }
}

/// The shortest countdown to expiry left in a bucket, in milliseconds, or absent when nothing
/// the bucket holds expires.
///
/// EIGHT BYTES, NOT SIXTEEN, AND THAT IS THE WHOLE REASON THIS TYPE EXISTS. A `u64` has no value
/// it does not use, so `Option<u64>` cannot put its discriminant inside the number: the
/// discriminant takes a word of its own and the aligner rounds the pair to 16. `NonZeroU64` has
/// exactly one unused value and `Option<NonZeroU64>` spends it on the discriminant, so the same
/// two states fit in 8. On `BucketNode` that is not eight bytes of field traded for eight bytes
/// of padding -- it moves the node from 208 to 200, because the node's eight-byte-aligned group
/// loses a whole word and the six bytes of trailing slack stay exactly where they were.
///
/// WHY NOT JUST READ ZERO AS ABSENT. Zero is a real countdown and a different answer from
/// absent. `refresh_bucket_runtime_flags` computes this as `expires_at.saturating_sub(now)`, so
/// a bucket holding something already past its deadline reports a countdown of 0, and
/// `ttl_bucket_count` counts that bucket. Folding zero into absent would stop counting exactly
/// the buckets whose expiry is due, which is the opposite of what the field is for.
///
/// SO THE COUNTDOWN IS STORED PLUS ONE, and the one value that cannot survive the bias is the
/// top of the range: a countdown of `u64::MAX` ms stores saturated and reads back one
/// millisecond short -- 584,542,046 years after the deadline rather than that plus a
/// millisecond. `saturating_add` is the same shape the narrowed address fields already use, and
/// for the same reason: a value that cannot fit has to come back as something no clock will ever
/// produce, never as its own low bits.
///
/// THE STORED SPELLING DOES NOT MOVE. The index writes this as the `ttl_ms` key it always did,
/// a number or `null`, because the serde impls below unbias on the way out and rebias on the way
/// in. `the_stored_spelling_of_a_bucket_node_did_not_move` drives that in both directions.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct BucketTtl(Option<NonZeroU64>);

/// One word, which is the point.
const _: () = assert!(std::mem::size_of::<BucketTtl>() == 8);

impl BucketTtl {
    /// No countdown: nothing this bucket holds expires.
    pub(super) const ABSENT: Self = Self(None);

    /// Take a countdown in milliseconds and bias it into the non-zero range.
    ///
    /// `saturating_add` is the whole of the saturation: it is what keeps the biased value from
    /// reaching zero, so the `and_then` never actually drops a countdown. Written this way on
    /// purpose rather than with a fallback arm -- a fallback arm is unreachable code that no
    /// test can reach and no mutation can be scored against, and an addition that WRAPPED would
    /// then be absorbed by it silently instead of turning into the absence that
    /// `the_biased_countdown_saturates_at_the_top_and_is_exact_below_it` catches.
    pub(super) fn from_ms(ms: Option<u64>) -> Self {
        Self(ms.and_then(|ms| NonZeroU64::new(ms.saturating_add(1))))
    }

    /// The countdown in milliseconds, as the rest of the engine reads it.
    pub(super) fn ms(self) -> Option<u64> {
        self.0.map(|biased| biased.get() - 1)
    }

    /// Whether this bucket holds anything that expires.
    pub(super) fn is_some(self) -> bool {
        self.0.is_some()
    }
}

impl Serialize for BucketTtl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // The unbiased countdown, spelled exactly as the plain `Option<u64>` was: a number, or
        // `null`. The bias is a resident-layout decision and stops at this boundary.
        self.ms().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BucketTtl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self::from_ms(Option::<u64>::deserialize(deserializer)?))
    }
}

/// The five per-bucket lifecycle flags, in one byte.
///
/// SIX BYTES OF SMALL FIELD, NOT TEN, AND THAT IS THE WHOLE REASON THIS TYPE EXISTS. Rust lays
/// `BucketNode` out as two groups: everything of alignment 8 packs solid, and everything smaller
/// fills the tail, which is then rounded up to the struct's alignment. The tail held
/// `routing_bucket` (4), `layout` (1) and five separate `bool` (5) -- ten bytes, rounded to
/// sixteen. Folding the five bools into one byte makes it six, and six rounds to eight.
///
/// WHY THE EARLIER READING SAID THIS WAS WORTH NOTHING, AND WHY IT WAS RIGHT ABOUT A DIFFERENT
/// QUESTION. Narrowing any ONE field of a ten-byte tail is worth nothing, because 9 and 6 and 4
/// all round back to 16 -- there is no single field here whose removal crosses the step. That is
/// true, it was stated as the general rule, and the general rule is false: PACKING is not
/// narrowing one field, it is removing four of them at once, and four is what it takes to cross.
///
/// THE STORED SPELLING DOES NOT MOVE. The index writes five separate boolean keys, exactly as it
/// always did, because `BucketNode` now carries hand-written `Serialize` and `Deserialize` impls
/// that spell them out. That is the real price of this change and it is paid in one place;
/// `the_stored_spelling_of_a_bucket_node_did_not_move` drives it byte for byte.
///
/// A MASK IS THE FAILURE MODE. Five bits read through five masks is five chances to read the
/// wrong one, and a flag that answers another flag's question is not a crash, it is a bucket that
/// reports itself resident when it is loading. `engine::tests::bucket_flag_masks` holds every
/// accessor to its own bit, and holds a deliberately mis-masked mirror beside it to prove the
/// check can fail.
#[derive(Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct BucketFlags(u8);

impl BucketFlags {
    pub(super) const DIRTY: u8 = 1 << 0;
    pub(super) const DELETED: u8 = 1 << 1;
    pub(super) const META_LOADED: u8 = 1 << 2;
    pub(super) const LOADING: u8 = 1 << 3;
    pub(super) const IN_MEMORY: u8 = 1 << 4;

    /// Every flag this type defines, under the name the engine reads it by.
    ///
    /// Derived from here rather than hand-listed at each use, so a flag added to the struct and
    /// not to this table is a table that has drifted -- which is what the mask guard checks.
    pub(super) const MASKS: [(&'static str, u8); 5] = [
        ("dirty", Self::DIRTY),
        ("deleted", Self::DELETED),
        ("meta_loaded", Self::META_LOADED),
        ("loading", Self::LOADING),
        ("in_memory", Self::IN_MEMORY),
    ];

    pub(super) const fn get(self, mask: u8) -> bool {
        self.0 & mask != 0
    }

    pub(super) fn set(&mut self, mask: u8, on: bool) {
        if on {
            self.0 |= mask;
        } else {
            self.0 &= !mask;
        }
    }

    /// The same, as a value, for the struct literals that used to name a flag inline.
    pub(super) const fn with(self, mask: u8, on: bool) -> Self {
        Self(if on { self.0 | mask } else { self.0 & !mask })
    }

    /// The raw byte. For the mask guard's diagnostics and for nothing else -- every engine reader
    /// goes through a named accessor on `BucketNode`.
    pub(super) const fn bits(self) -> u8 {
        self.0
    }
}

/// Named, not numeric: a debug line reading `BucketFlags(20)` is a line nobody can check.
impl std::fmt::Debug for BucketFlags {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut set = formatter.debug_set();
        for (name, mask) in Self::MASKS {
            if self.get(mask) {
                set.entry(&format_args!("{name}"));
            }
        }
        set.finish()
    }
}

/// One byte, which is the point.
const _: () = assert!(std::mem::size_of::<BucketFlags>() == 1);

/// Index -> BucketMap -> BucketNode -> BlockIndex/ObjectIndex.
#[derive(Debug, Default, Clone)]
pub(super) struct BucketNode {
    pub(super) routing_bucket: u32,
    pub(super) layout: BucketLayoutState,
    /// The three flags of a per-bucket residency lifecycle. They are SET now, by
    /// `release_bucket_blocks` and `reload_released_bucket` in `storage_bucket_internals`.
    ///
    /// A RELEASED bucket is `meta_loaded: true, loading: false, in_memory: false` with an empty
    /// `page_index` and its `object_index` intact: the node stays in `bucket_map`, so the bucket
    /// is still routable, still countable, and still findable -- it simply no longer holds the
    /// per-block entries, which are what the index actually costs (~760 B a record).
    ///
    /// WHERE THE BLOCK LIST COMES BACK FROM. `bucket_map` is derived:
    /// `rebuild_bucket_block_ownership` builds it by walking `collect_model_live_block_entries`,
    /// which iterates `strings`, `zsets`, `lists` and the rest -- the resident address maps. That
    /// used to read as the reason a bucket could not be released, and it is in fact the reason it
    /// CAN be: the model maps, not the bucket index, are what a read resolves through (see
    /// `Command::StringGet`, which goes straight to `shard.strings`), so releasing the derived
    /// per-block view frees memory without touching anything a read needs. Reload re-derives
    /// exactly that bucket's blocks from the same maps.
    ///
    /// WHAT THAT COSTS IN PRECONDITIONS, all checked by `release_bucket_blocks`, none assumed:
    ///
    ///   * the bucket must be clean and undeleted, block by block -- the model maps carry no
    ///     per-block `dirty`/`deleted` bit, so a release that had to restore one could not;
    ///   * every block's address must carry an explicit routing bucket equal to this one, so
    ///     "which model entries are this bucket's" needs no hash fallback to answer;
    ///   * the block set must be EQUAL to what the model maps derive for the bucket right now,
    ///     which is what makes the release reversible rather than hopeful; and
    ///   * no block may belong to `hashes`, `context_events` or `context_indexes`. For
    ///     `context_events` and `context_indexes` the reason is that they are `skip_serializing`
    ///     on `ShardState` and are rebuilt FROM the bucket index on load, so a released bucket of
    ///     one of those kinds would have nothing left to rebuild from once the index was written
    ///     and read back. `hashes` is DURABLE now and is held out by the other precondition
    ///     instead: it is read whole, and `model_map_block_address` has no point lookup for it.
    ///
    /// `loading` is held across the re-derivation, which is the state a concurrent caller would
    /// have to queue behind if reload ever became asynchronous; today the shard write lock covers
    /// it, so it is true only within one critical section.
    pub(super) flags: BucketFlags,
    pub(super) ttl_ms: BucketTtl,
    pub(super) dirty_generation: u64,
    /// The write-ahead log sequence at which this bucket most recently went from clean to dirty,
    /// or 0 when it is clean or the answer is not known.
    ///
    /// This is the floor the bucket holds over the log. Reclaim may free the log below the
    /// oldest sequence any bucket still needs; a dirty bucket with no durable dump manifest is
    /// captured nowhere, so without this the only safe answer for it is 0 -- and one such bucket
    /// pins the whole log for ever. With it, that bucket pins the log only from its own oldest
    /// undumped write.
    ///
    /// 0 means "no claim", which is the safe direction: an unknown value reads as the old
    /// behaviour rather than as permission to reclaim more.
    ///
    /// NOT serialized. A load clears every dirty flag -- reloaded data is durable, hence clean --
    /// and recomputes dirtiness from the live `dirty_objects` set, which is empty on load. So a
    /// reloaded bucket holds no claim by definition, and persisting this would both add a key to
    /// the index wire format and carry a number that is meaningless the moment it is read back.
    /// `the_index_wire_keys_are_what_they_were` is what caught that.
    ///
    /// AND IT STAYS ON THE NODE. Two readers pin it, and neither can be answered by a shard-level
    /// watermark: `storage_wal_reclaim_plan` takes this as the WAL retain FLOOR for a bucket no
    /// manifest covers, and `first_dirty_rank` -- the dump ordering's PRIMARY key -- takes it as
    /// "which bucket is holding the log", so the bucket whose dump moves the floor is dumped
    /// first. A single watermark over the whole index gives the same FLOOR, because that floor is
    /// the minimum of these claims either way; what it cannot give is the ORDER, and a dump order
    /// that cannot see which bucket is pinning the log is the starvation
    /// `dump_selection_prioritizes_the_least_recently_dumped_bucket_not_the_lowest_id` was written
    /// for. `a_wrapped_claim_ring_errs_conservative_only_while_it_keeps_dirtiness` measures what a
    /// BOUNDED side structure does instead, and which direction it errs.
    pub(super) first_dirty_wal_sequence: u64,
    /// The same claim against the INDEX LOG: the index-log sequence at which this bucket most
    /// recently went from clean to dirty, or 0 when it is clean or not known.
    ///
    /// Both are needed, and one cannot stand in for the other: the reclaim plan keeps a WAL
    /// frontier and an index-log frontier, and they count in different sequences. A bucket that
    /// can say where it sits in the log but not in the index log can only hold both at 0.
    ///
    /// Transient for the same reason as its WAL twin -- a load clears every dirty flag and
    /// recomputes from an empty dirty set, so a reloaded bucket holds no claim.
    pub(super) first_dirty_index_log_sequence: u64,
    pub(super) object_index: ObjectIndex,
    pub(super) deleted_object_index: DeletedObjectIndex,
    pub(super) block_index: BlockIndexMap,
}

/// The widest per-item structure in the engine, and the one whose count is the bucket count.
///
/// MOST OF IT USED TO BE THE BLOCK INDEX, AND IS NOT ANY MORE. `block_index` held a block entry
/// INLINE -- 96 of a 160-byte node was one entry plus its handle -- so an accounting of this
/// structure had to start there. The entry moved behind a POINTER and the block index is 24 bytes,
/// so the two 24-byte collections and the three `u64` claims are now the terms that matter, and the
/// widest single field is no longer obvious from the declaration. `every_byte_of_the_bucket_node_is_accounted_for`
/// is what says which it is rather than this comment.
///
/// 202 bytes of field became 194 when `ttl_ms` stopped spending a word on a discriminant, and
/// 186 when the tombstone index stopped spending sixteen on a case it is in 2.32% of the time;
/// the struct went 208 -> 200 -> 192 with them, and 192 -> 184 when the address inside the inline
/// block entry shed its derived `generation`. 184 -> 176 is the five `bool` becoming five BITS.
/// 176 -> 168 is `last_dump_sequence` leaving: a whole word out of the eight-aligned group.
/// 168 -> 160 is that same inline address merging its slab id and its offset into ONE WORD: a
/// SECOND whole word out of that group.
///
/// THOSE THREE MOVE THIS STRUCTURE IN TWO DIFFERENT WAYS, which is why they are worth eight
/// bytes each instead of eight between them. The flags are in the TAIL: packing them took it
/// from ten bytes to six, and six rounds to eight where ten rounded to sixteen. The other two
/// are in the EIGHT-ALIGNED GROUP -- a removed `u64` and, inside `block_index`, an address
/// whose two slab coordinates became one word -- and each takes a whole word out of it. None of
/// them could have been found by looking at another's half.
///
/// THE RULE THAT USED TO BE WRITTEN HERE WAS TRUE OF ONE FIELD AND FALSE IN GENERAL, and it is
/// worth stating plainly because it is why nobody tried this for three changes. It said the six
/// bytes left over were the aligner rounding `routing_bucket`, `layout` and the five flags -- ten
/// bytes of small field -- up to sixteen, and that narrowing any of those ten bytes moved
/// nothing. Every clause of that is correct except the last one's scope. Narrowing ONE of the ten
/// moves nothing: 9, 6 and 4 all round back to 16, and no single field here can cross the step on
/// its own. PACKING is not narrowing one field. It took four bytes off at once, the tail landed
/// on six, and six rounds to eight. MERGING is not narrowing one field either: two 8-byte
/// fields become one, and a whole word leaves the eight-aligned group -- the same shape of
/// change as removing a field, and for the same reason it crosses. The general rule that holds
/// is the one the accounting test states: only a change that takes the tail to eight bytes or
/// fewer, or that takes a whole word out of the eight-aligned group, moves this structure at
/// all.
///
/// THE SECOND OF THOSE TWO SHAPES HAS NOW BEEN TAKEN FOUR TIMES. `last_dump_sequence` was
/// eight-aligned, so removing it took a whole word out of the packed group and the six-byte
/// tail did not move at all. The address merge does the same thing one level in: two 8-byte
/// fields inside the inline block entry become one, so `BlockIndexMap` went 104 -> 96 and the
/// group went 168 -> 160, with the tail still six rounded to eight. The model spelling is the
/// largest of the four: a sixteen-byte fat pointer inside that same inline entry becomes one
/// byte, `BlockIndexMap` goes 96 -> 80, and TWO whole words leave the group at once, so the
/// struct was 144. The fourth is that same address again, shedding its `routing_bucket` beside
/// narrowing its `block_id` to sixteen bits: `BlockIndexMap` goes 80 -> 72, the group 136 -> 128,
/// and the struct is 136. Bytes that LEAVE the group cross where narrowing a sequence would not
/// have: at a six-byte tail the first narrowing lands on ten, ten still rounds to sixteen, and
/// the freed word is handed straight back.
///
/// AND THE FOURTH IS THE ONE THAT SHOWS WHY A PER-FIELD VERDICT IS NOT A VERDICT ON THE FIELD.
/// Six bytes of address payload left in two independent narrowings, NEITHER of which moves this
/// struct on its own -- 25 bytes of payload rounds to 32 and so does 27. The pair is worth a word
/// here and a word in every other structure that holds a block entry.
///
/// AND THE LARGEST STEP OF ALL IS A WHOLE FIELD NARROWING BY FIFTY-SIX BYTES AT ONCE.
/// `block_index` stopped holding a block entry INLINE and started holding it behind a POINTER. That
/// arm was a handle plus a whole `BlockIndex`, so `BlockIndexMap` was `8 + size_of::<BlockIndex>()`
/// and every node in the `BucketMap` carried it whether or not its bucket held exactly one block;
/// the block index is now 24 bytes, the width of the list it also holds, because the tags ride in
/// pointer niches. The eight-aligned group goes 136 -> 80 and the struct 144 -> 88, with the tail
/// still six rounded to eight. This is the same SHAPE of change as the steps above -- whole words
/// leaving the eight-aligned group -- taken seven times over in one field.
///
/// FIFTY-SIX AND NOT SEVENTY-TWO, BECAUSE THE STEP ABOVE HARVESTED THE SAME FIELD FIRST. Narrowing
/// the model spelling took the inline entry 88 -> 72 and so this arm 96 -> 80; taking the entry out
/// of the node then recovers what is left. The two changes are not additive in the NODE and the node
/// is 88 whichever order they land in -- what the narrower entry still buys, after this change, is on
/// the heap: every boxed arm and every list element is sixteen bytes smaller.
///
/// WHY IT WAS AVAILABLE ONLY NOW. The inline entry was kept by #1964 on a measurement over a
/// population where every bucket held exactly one block, which was a property of `load_shard`
/// defaulting the routing range to the whole `u32` keyspace rather than of any workload. #1973 made
/// 1023 the default and the population inverted. `inline_arm_trade.rs` re-derives both halves of
/// that arithmetic at both ranges, and prices dropping the arm altogether beside boxing it: the two
/// recover the identical 24 bytes, and boxing is the one that does not REGRESS a store still on the
/// old range.
///
/// `every_byte_of_the_bucket_node_is_accounted_for` states the whole of it field by field, and
/// asserts the reconstruction -- eight-aligned group plus one rounding of the tail -- rather than
/// a literal.
const _: () = assert!(std::mem::size_of::<BucketNode>() == 88);

impl BucketNode {
    /// The five lifecycle flags, each read through its own mask and nothing else.
    ///
    /// These are ACCESSORS FOR FIELDS THAT USED TO BE PUBLIC, and they exist so that packing the
    /// flags is a change to one declaration rather than to every reader's meaning. Each one is
    /// the same question it was before, asked the same way round.
    pub(super) const fn dirty(&self) -> bool {
        self.flags.get(BucketFlags::DIRTY)
    }

    pub(super) fn set_dirty(&mut self, on: bool) {
        self.flags.set(BucketFlags::DIRTY, on);
    }

    pub(super) const fn deleted(&self) -> bool {
        self.flags.get(BucketFlags::DELETED)
    }

    pub(super) fn set_deleted(&mut self, on: bool) {
        self.flags.set(BucketFlags::DELETED, on);
    }

    pub(super) const fn meta_loaded(&self) -> bool {
        self.flags.get(BucketFlags::META_LOADED)
    }

    pub(super) fn set_meta_loaded(&mut self, on: bool) {
        self.flags.set(BucketFlags::META_LOADED, on);
    }

    pub(super) const fn loading(&self) -> bool {
        self.flags.get(BucketFlags::LOADING)
    }

    pub(super) fn set_loading(&mut self, on: bool) {
        self.flags.set(BucketFlags::LOADING, on);
    }

    pub(super) const fn in_memory(&self) -> bool {
        self.flags.get(BucketFlags::IN_MEMORY)
    }

    pub(super) fn set_in_memory(&mut self, on: bool) {
        self.flags.set(BucketFlags::IN_MEMORY, on);
    }
}

/// THE STORED SPELLING, WRITTEN OUT BY HAND BECAUSE THE DECLARATION NO LONGER MATCHES IT.
///
/// `BucketNode` is written into the shard index, so its field names ARE a stored format. The five
/// flags are five separate boolean keys on the wire and one byte in memory, and the only way to
/// hold both is to stop deriving this and say it. Twelve keys, in this order, with `routing_slot`
/// and `page_index` spelled as they always were -- thirteen until `last_dump_sequence` left the
/// node.
///
/// WHAT A DERIVE WAS DOING THAT THIS HAS TO KEEP DOING, each one a stored-format fact rather than
/// a style choice:
///
///   * the two `first_dirty_*` claims are NOT written -- they were `#[serde(skip)]`, a load
///     clears them, and writing them would add a key carrying a number that is meaningless the
///     moment it is read back;
///   * `last_dump_sequence` is NOT written either, and that one is a FORMAT BREAK rather than a
///     field that was always transient. The node no longer holds it: it was read into two reports
///     and nothing else, and the report takes it from the newest dump manifest now, exactly as
///     `BucketStorageSummary` already did. The key is still RECOGNISED on the way in -- still
///     type-checked as a `u64`, still refused if a node states it twice -- so every index ever
///     written still loads. What it is no longer is REQUIRED, and it cannot be: an index this
///     engine writes does not carry it, and an index this engine writes has to load. An index
///     THIS engine writes will not load in an engine older than this change, which is the whole
///     of the break and is why it is stated here rather than discovered;
///   * `object_ids`, `deleted_object_ids` and `page_refs` are still accepted as aliases, which is
///     what lets the oldest written index load -- `core_index_loads_legacy_bucket_page_field_names`
///     holds that spelling and an alias dropped from here is a store that stops loading;
///   * the fields that had `#[serde(default)]` still default when the key is absent, and the ones
///     that did not still REFUSE a node that omits them. Presence comes from the wire.
///
/// A repeated key is an error rather than a last-one-wins, exactly as the derive had it: two
/// disagreeing statements of the same fact must fail loudly and before anything is built from
/// them.
impl Serialize for BucketNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut node = serializer.serialize_struct("BucketNode", 12)?;
        node.serialize_field("routing_slot", &self.routing_bucket)?;
        node.serialize_field("layout", &self.layout)?;
        node.serialize_field("dirty", &self.dirty())?;
        node.serialize_field("deleted", &self.deleted())?;
        node.serialize_field("meta_loaded", &self.meta_loaded())?;
        node.serialize_field("loading", &self.loading())?;
        node.serialize_field("in_memory", &self.in_memory())?;
        node.serialize_field("ttl_ms", &self.ttl_ms)?;
        node.serialize_field("dirty_generation", &self.dirty_generation)?;
        node.serialize_field("object_index", &self.object_index)?;
        node.serialize_field("deleted_object_index", &self.deleted_object_index)?;
        node.serialize_field("page_index", &self.block_index)?;
        node.end()
    }
}

/// The keys a stored bucket node can carry, including the three older spellings and the one key
/// the node no longer holds.
///
/// Resolved without allocating -- a `String` per key per bucket is a cost the derive did not pay
/// and the load path should not start paying. An unrecognised key is IGNORED, which is what the
/// derive did and what lets an index written by a newer engine load into an older one.
///
/// `LastDumpSequence` is kept as a NAMED variant rather than folded into `Ignore` on purpose. The
/// node discards the value, but the key is still part of the shape every index ever written
/// carries, so it is still resolved by name, still decoded as a `u64` -- a node stating a string
/// there is still refused -- and still refused if a node states it twice. Folding it into
/// `Ignore` would drop all three of those, and it is the difference between a field that has been
/// retired and one that is no longer read.
enum BucketNodeField {
    RoutingSlot,
    Layout,
    Dirty,
    Deleted,
    MetaLoaded,
    Loading,
    InMemory,
    TtlMs,
    DirtyGeneration,
    LastDumpSequence,
    ObjectIndex,
    DeletedObjectIndex,
    PageIndex,
    Ignore,
}

const BUCKET_NODE_FIELDS: &[&str] = &[
    "routing_slot",
    "layout",
    "dirty",
    "deleted",
    "meta_loaded",
    "loading",
    "in_memory",
    "ttl_ms",
    "dirty_generation",
    "last_dump_sequence",
    "object_index",
    "deleted_object_index",
    "page_index",
];

impl<'de> Deserialize<'de> for BucketNodeField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl serde::de::Visitor<'_> for FieldVisitor {
            type Value = BucketNodeField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a bucket node field name")
            }

            fn visit_str<E>(self, value: &str) -> Result<BucketNodeField, E>
            where
                E: serde::de::Error,
            {
                Ok(match value {
                    "routing_slot" => BucketNodeField::RoutingSlot,
                    "layout" => BucketNodeField::Layout,
                    "dirty" => BucketNodeField::Dirty,
                    "deleted" => BucketNodeField::Deleted,
                    "meta_loaded" => BucketNodeField::MetaLoaded,
                    "loading" => BucketNodeField::Loading,
                    "in_memory" => BucketNodeField::InMemory,
                    "ttl_ms" => BucketNodeField::TtlMs,
                    "dirty_generation" => BucketNodeField::DirtyGeneration,
                    "last_dump_sequence" => BucketNodeField::LastDumpSequence,
                    // The three older spellings, each alongside the one that replaced it.
                    "object_index" | "object_ids" => BucketNodeField::ObjectIndex,
                    "deleted_object_index" | "deleted_object_ids" => {
                        BucketNodeField::DeletedObjectIndex
                    }
                    "page_index" | "page_refs" => BucketNodeField::PageIndex,
                    _ => BucketNodeField::Ignore,
                })
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

impl<'de> Deserialize<'de> for BucketNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeVisitor;

        impl<'de> serde::de::Visitor<'de> for NodeVisitor {
            type Value = BucketNode;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a bucket node")
            }

            fn visit_map<M>(self, mut map: M) -> Result<BucketNode, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                use serde::de::Error as _;

                let mut routing_bucket: Option<u32> = None;
                let mut layout: Option<BucketLayoutState> = None;
                let mut dirty: Option<bool> = None;
                let mut deleted: Option<bool> = None;
                let mut meta_loaded: Option<bool> = None;
                let mut loading: Option<bool> = None;
                let mut in_memory: Option<bool> = None;
                let mut ttl_ms: Option<BucketTtl> = None;
                let mut dirty_generation: Option<u64> = None;
                // Decoded and discarded: the node does not hold this any more, and the key is
                // still type-checked and still refused twice. See `BucketNodeField`.
                let mut last_dump_sequence: Option<u64> = None;
                let mut object_index: Option<ObjectIndex> = None;
                let mut deleted_object_index: Option<DeletedObjectIndex> = None;
                let mut block_index: Option<BlockIndexMap> = None;

                // Each arm refuses a SECOND statement of the same fact rather than letting the
                // later one win. `once` names the key in the error so a store that carries both
                // an old and a new spelling of one field says which.
                macro_rules! once {
                    ($slot:ident, $name:literal) => {{
                        if $slot.is_some() {
                            return Err(M::Error::duplicate_field($name));
                        }
                        $slot = Some(map.next_value()?);
                    }};
                }

                while let Some(key) = map.next_key::<BucketNodeField>()? {
                    match key {
                        BucketNodeField::RoutingSlot => once!(routing_bucket, "routing_slot"),
                        BucketNodeField::Layout => once!(layout, "layout"),
                        BucketNodeField::Dirty => once!(dirty, "dirty"),
                        BucketNodeField::Deleted => once!(deleted, "deleted"),
                        BucketNodeField::MetaLoaded => once!(meta_loaded, "meta_loaded"),
                        BucketNodeField::Loading => once!(loading, "loading"),
                        BucketNodeField::InMemory => once!(in_memory, "in_memory"),
                        BucketNodeField::TtlMs => once!(ttl_ms, "ttl_ms"),
                        BucketNodeField::DirtyGeneration => {
                            once!(dirty_generation, "dirty_generation")
                        }
                        BucketNodeField::LastDumpSequence => {
                            once!(last_dump_sequence, "last_dump_sequence")
                        }
                        BucketNodeField::ObjectIndex => once!(object_index, "object_index"),
                        BucketNodeField::DeletedObjectIndex => {
                            once!(deleted_object_index, "deleted_object_index")
                        }
                        BucketNodeField::PageIndex => once!(block_index, "page_index"),
                        BucketNodeField::Ignore => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                // DECODED AND DISCARDED. The value is dropped here rather than never read,
                // because the key is still part of the shape and the arm above is what
                // type-checks it and refuses a second statement of it. Naming the drop is what
                // stops it reading as an oversight.
                let _ = last_dump_sequence;

                // PRESENCE COMES FROM THE WIRE. The six keys that had no `#[serde(default)]` and
                // whose values the node still holds are still required, so a node that omits one
                // is refused rather than filled in with a zero that would read as a real answer.
                // It was seven: `last_dump_sequence` is the one that left, and it left because
                // this engine no longer writes it -- a key an index we write does not carry
                // cannot be one we refuse an index for omitting.
                let mut flags = BucketFlags::default();
                flags.set(
                    BucketFlags::DIRTY,
                    dirty.ok_or_else(|| M::Error::missing_field("dirty"))?,
                );
                flags.set(BucketFlags::DELETED, deleted.unwrap_or_default());
                flags.set(
                    BucketFlags::META_LOADED,
                    meta_loaded.ok_or_else(|| M::Error::missing_field("meta_loaded"))?,
                );
                flags.set(
                    BucketFlags::LOADING,
                    loading.ok_or_else(|| M::Error::missing_field("loading"))?,
                );
                flags.set(
                    BucketFlags::IN_MEMORY,
                    in_memory.ok_or_else(|| M::Error::missing_field("in_memory"))?,
                );

                Ok(BucketNode {
                    routing_bucket: routing_bucket
                        .ok_or_else(|| M::Error::missing_field("routing_slot"))?,
                    layout: layout.unwrap_or_default(),
                    flags,
                    ttl_ms: ttl_ms.unwrap_or_default(),
                    dirty_generation: dirty_generation
                        .ok_or_else(|| M::Error::missing_field("dirty_generation"))?,
                    // Transient by declaration: a load clears every dirty flag, so a reloaded
                    // bucket holds no claim over either log and these start at 0 whatever the
                    // stored index says.
                    first_dirty_wal_sequence: 0,
                    first_dirty_index_log_sequence: 0,
                    object_index: object_index.unwrap_or_default(),
                    deleted_object_index: deleted_object_index.unwrap_or_default(),
                    block_index: block_index.unwrap_or_default(),
                })
            }
        }

        deserializer.deserialize_struct("BucketNode", BUCKET_NODE_FIELDS, NodeVisitor)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum BucketLayoutState {
    #[default]
    Empty,
    SingleObject,
    SingleBlockObject,
    MultiBlockObject,
    MultiObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct BlockIndex {
    pub(super) object_key: Arc<str>,
    pub(super) model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<Arc<str>>,
    pub(super) address: BlockAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
}

/// One per stored block. TWO shared names, a one-byte model spelling, an address, and three
/// flags -- 52 bytes of field in 56.
///
/// 64, not 72, since the address inside it is 24 bytes and not 32: it shed the `routing_bucket`
/// the read path now derives from the key, and narrowed its `block_id` to the sixteen bits the
/// record encoder has always refused to exceed. That is the fourth whole eight-byte step out of
/// this structure: 72, not 88, since the model spelling stopped being a sixteen-byte fat pointer
/// to a string drawn from a seventeen-element set; 88, not 96, since the address merged its slab
/// id and its offset into one word; 96, not 104, since it shed its derived `generation` before
/// that.
///
/// THE FOURTH STEP IS TWO NARROWINGS AND NEITHER IS WORTH ANYTHING ALONE. The address carried 29
/// bytes of payload in 32. Dropping the bucket leaves 25 and narrowing the id leaves 27; both
/// round back to 32 and this structure would have stayed 72 for either one. Together they leave
/// 23, the address is 24, and the step lands. `block_store`'s width assert carries both
/// counterfactuals beside the width, because the arithmetic is the claim and it was the per-field
/// reading of it that declared this blocked.
///
/// AND THE FLAGS STILL DO NOT PAY. At 99 bytes of field the slack was five, at 91 five, at 83
/// five, at 68 four and at 60 it is four again -- the address left in a whole word, which is the
/// only kind of change that moves this number, and it moved the width without touching the
/// rounding the flags sit in. Packing the three flags would reclaim nothing and would move the
/// stored index, which spells each one as its own key.
///
/// AND THE WIRE DID MOVE, IN EXACTLY ONE SLOT. This paragraph said "WHAT DID NOT MOVE IS THE WIRE
/// -- the spelling is still written and read as the string it always was; only the in-memory width
/// changed", and named `the_stored_spelling_of_a_page_entry_did_not_move` as its guard. That guard
/// is what failed on the first suite run after this change, which is how the claim was caught: it
/// had no wrong word in it and it was no longer true.
///
/// `"oi":42` is written `"oi":null` now. `BlockAddressWire::object_id` carries `rename`, `alias`
/// and `default` but no `skip_serializing_if`, so an address holding no id emits the slot as a null
/// rather than dropping it. The slot STAYS because the index log packs the address POSITIONALLY --
/// retiring it would shorten the array and refuse every row already on disk -- and because the
/// decode still cross-checks an old `g` against `block_id.or(object_id)` through it.
///
/// So the stored form moved, `SHARD_INDEX_FORMAT_VERSION` goes to 6 to pay for it, and the guards
/// are `the_stored_spelling_of_a_page_entry_moved_in_exactly_one_slot` (the four goldens),
/// `an_index_written_before_this_change_loads_page_for_page_and_writes_back_the_same_bytes` (the
/// old-row round trip) and `core_index_loads_legacy_bucket_page_field_names` (that the old names
/// still read).
const _: () = assert!(std::mem::size_of::<BlockIndex>() == 56);

impl BlockIndex {
    /// The object this block belongs to, DERIVED from the terms rather than read off the address.
    ///
    /// It was `self.address.object_id()` while the address carried one. The address does not any
    /// more, and this entry holds both remaining terms -- the stored model spelling and the object
    /// key -- so the only term it lacks is the shard.
    ///
    /// THE SHARD IS A PARAMETER AND NOT AN `Option`, deliberately. `ShardState::shard_id` answers
    /// `None` for a state that never entered the engine, and there is no shard id that is safe to
    /// guess: a zero here derives a well-formed id belonging to a different shard, which nothing
    /// downstream can tell from the right one. Taking a definite `ShardId` pushes that decision to
    /// the caller, where a state IS in scope to ask, and makes the compiler name every site that
    /// cannot answer.
    ///
    /// The component is NOT a term. An object id is per (shard, kind, key); which element of that
    /// object a block holds is `component`, beside the id and not inside it.
    pub(super) fn object_id(&self, shard_id: crate::types::ShardId) -> u64 {
        crate::engine::hashing::stable_block_object_id(
            shard_id,
            self.model_id.as_str(),
            &self.object_key,
        )
    }
}

impl CoreIndex {
    pub(super) fn rebuild_object_block_lookup(&mut self) {
        // The rebuild is already O(blocks); establishing the total here costs nothing extra and
        // is what lets the stats path stop walking the shard.
        self.object_component_block_refs = Some(0);
        self.object_block_lookup.clear();
        let refs = self
            .bucket_map
            .iter()
            .flat_map(|(routing_bucket, bucket)| {
                bucket.block_index.iter().map(move |(block_ref_key, page)| {
                    (*routing_bucket, *block_ref_key, page.clone())
                })
            })
            .collect::<Vec<_>>();
        for (routing_bucket, block_ref_key, page) in refs {
            self.insert_object_block_lookup(routing_bucket, block_ref_key, &page);
        }
    }

    /// Takes the handle the block index filed this block under, so the two cannot name different
    /// things. It used to take the rendered key by shared pointer; the key is a number now and
    /// costs nothing to copy.
    pub(super) fn insert_object_block_lookup(
        &mut self,
        routing_bucket: u32,
        block_ref_key: u64,
        page: &BlockIndex,
    ) {
        if page.deleted {
            return;
        }
        let added = {
            // The lookup is keyed by the kind's SHARED name, and the block no longer carries one --
            // it carries the one-byte spelling. `kind_pool` is where that shared name already
            // lives: the kinds are a closed set of about fifteen, so only the first block of each
            // allocates and the reserve in the pool exists for precisely this.
            let kind = crate::engine::state::intern_kind(
                &mut self.kind_pool,
                page.model_id.as_str(),
            );
            let entry = self
                .object_block_lookup
                .entry(&kind, &page.object_key);
            let value = BlockLookupRef {
                routing_bucket,
                block_ref_key,
            };
            match entry.position(page.component.as_deref()) {
                Ok(at) => entry.by_component[at].refs.insert(value),
                Err(at) => {
                    // A component's first block. Build the entry already holding it, so the common
                    // case never allocates and there is no empty state in between.
                    entry.by_component.insert(
                        at,
                        ComponentBlocks {
                            component: page.component.clone(),
                            refs: BlockRefs::One(value),
                        },
                    );
                    true
                }
            }
        };
        if added {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_add(1);
            }
        }
    }

    /// Every block ref this object holds, for one component.
    pub(super) fn block_refs_for(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) -> Option<&[BlockLookupRef]> {
        self.object_block_lookup
            .get(model_id, object_key)
            .and_then(|entry| entry.refs_for(component))
    }

    /// Every component of this object, and the blocks holding each.
    pub(super) fn object_block_refs(
        &self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&ObjectBlockRefs> {
        self.object_block_lookup.get(model_id, object_key)
    }

    pub(super) fn remove_object_block_lookup_entry(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) {

        // One vector element IS this component's entire set of refs. What this replaces had to
        // seek a range with an empty-string sentinel and take_while on the component, because the
        // per-component map flattened every component of an object into one ordered set -- so a
        // component's refs could only be found by range, not by index. Nesting deletes that.
        let mut removed = 0usize;
        let mut now_empty = false;
        if let Some(entry) = self.object_block_lookup.get_mut(model_id, object_key) {
            if let Ok(at) = entry.position(component) {
                removed = entry.by_component.remove(at).refs.len();
            }
            now_empty = entry.by_component.is_empty();
        }
        if now_empty {
            self.object_block_lookup.remove(model_id, object_key);
        }
        if removed > 0 {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_sub(removed);
            }
        }
    }

    /// Drop every ref an object holds under one kind.
    ///
    /// The delete path used to call `rebuild_object_block_lookup` instead, which clears the whole
    /// lookup, clones every block in every bucket into a vector, and re-inserts them -- so one
    /// delete cost work proportional to the entire shard, and deleting a store cost the square of
    /// it. Removing the object's own entry is the same result for a fraction of the work.
    ///
    /// Maintains `object_component_page_refs` exactly as the per-component removal above does,
    /// because that counter is what lets the stats path avoid walking the shard.
    pub(super) fn remove_object_from_block_lookup(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) -> usize {
        let Some(entry) = self.object_block_lookup.remove(model_id, object_key) else {
            return 0;
        };
        let removed: usize = entry
            .by_component
            .iter()
            .map(|component| component.refs.len())
            .sum();
        if removed > 0 {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_sub(removed);
            }
        }
        removed
    }

    pub(super) fn contains_object_block_address(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
        address: &BlockAddress,
    ) -> bool {
        if let Some(block_refs) = self.block_refs_for(model_id, object_key, component) {
            return block_refs.iter().any(|block_ref| {
                self.bucket_map
                    .get(&block_ref.routing_bucket)
                    .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
                    .map(|page| {
                        !page.deleted
                            && page.model_id.as_str() == model_id
                            && &*page.object_key == object_key
                            && page.component.as_deref() == component
                            && same_block_address(&page.address, address)
                    })
                    .unwrap_or(false)
            });
        }

        if !self.object_block_lookup.is_empty() {
            return false;
        }

        self.bucket_map.values().any(|bucket| {
            bucket.block_index.values().any(|page| {
                !page.deleted
                    && page.model_id.as_str() == model_id
                    && &*page.object_key == object_key
                    && page.component.as_deref() == component
                    && same_block_address(&page.address, address)
            })
        })
    }
}

pub(super) fn object_component_lookup_key(model_id: &str, object_key: &str) -> String {
    let mut key = String::new();
    push_lookup_part(&mut key, model_id);
    push_lookup_part(&mut key, object_key);
    key
}

/// The next block index this object may use, which is one past the highest it holds.
///
/// A block id is an index INSIDE its object, which is what keeps it small. It must never be
/// REUSED, and that is a different requirement from being small: this store keeps a rewritten
/// block's predecessor live -- still referenced by the points it holds that were not rewritten --
/// so an object numbering its new blocks from zero again would have two live blocks claiming the
/// same position, and a reload would serve whichever it reached first.
///
/// Read from the blocks the object already has rather than from a counter, so it survives a
/// restart without anything having to be persisted for it.
pub(super) fn next_block_index_for_object(
    bucket_index: &CoreIndex,
    routing_bucket: u32,
    model_id: &str,
    object_key: &str,
) -> u32 {
    bucket_index
        .bucket_map
        .get(&routing_bucket)
        .and_then(|bucket| {
            bucket
                .block_index
                .values()
                .filter(|block| {
                    block.model_id.as_str() == model_id && block.object_key.as_ref() == object_key
                })
                .filter_map(|block| block.address.block_id())
                .max()
        })
        .map_or(0, |highest| {
            u32::try_from(highest).unwrap_or(u32::MAX).saturating_add(1)
        })
}

/// THE ORDINAL A CONTAINER PAGE IS FILED UNDER: STABLE ACROSS AN OVERWRITE, UNASSIGNED PAST THE
/// CEILING.
///
/// `next_block_index_for_object` above is the same derivation, and it is already the ordinal every
/// TIMESTAMPED kind gets -- thirteen call sites covering `feature` and the six `context_*` kinds. No
/// container kind ever called it, so a container page reached `append_block_of_object` through
/// `append_with_block_metadata`, which passes a hardcoded `0`. That is the whole reason every
/// container page carries block id 0: not a format limit, not a missing field, an argument nobody
/// supplied.
///
/// This is that same derivation for the container kinds, and it differs from the series one in
/// exactly two ways, both forced:
///
///   * AN OVERWRITE KEEPS THE ORDINAL IT ALREADY HAS. A container element is addressed by its
///     component, and `HashSet` or `SetAdd` on an existing member REPLACES that member's block.
///     Handing the replacement `max + 1` would make the ordinal climb once per WRITE rather than
///     once per ELEMENT, so a single member rewritten 65,536 times would reach the ceiling on a set
///     of one. Reading the component's own block first bounds the ordinal by the object's live
///     element high-water mark instead, which is what a position means.
///   * PAST THE CEILING IT LEAVES THE ORDINAL AT `0` RATHER THAN PANICKING OR SATURATING.
///     `narrow_block_id` refuses a value above `MAX_ADDRESSABLE_BLOCK_ID`, and refusing is right
///     for a value a caller chose -- but an object's 65,536th element is not a caller's mistake, it
///     is a container this store serves today with no ordinal at all. Saturating is worse still:
///     this tree's own doctrine is that a saturated block id is a legal block id for a DIFFERENT block
///     of the same object. So past the ceiling nothing is assigned and the block keeps the `0` it
///     would have had on `main`, which makes this change a strict no-op for such an object. A
///     container past the ceiling loses the ordinal, never the element.
///
/// IT NAMES A POSITION AMONG AN OBJECT'S LIVE BLOCKS, NOT AN ELEMENT. `max` falls after a delete and
/// the next insert is handed the ordinal that was just freed. That is correct for a position and
/// would be silent corruption for an identity, which is why the element's identity stays in the
/// component: nothing here reads the ordinal to find a row, and deletion still matches by component
/// exactly as before.
///
/// # WHY THE WALK NOW FILTERS `deleted`, AND WHAT IT COST TO FIND OUT
///
/// It used to need no filter, because every delete path REMOVED the block: an entry that existed was a
/// live entry, so "the object's entries" and "the object's live entries" were the same set and the
/// distinction was not expressible. A container removal now leaves a TOMBSTONE ENTRY behind so the
/// block recording it stays reachable, and that breaks the identity in both directions at once:
///
///   * `max` would no longer FALL after a delete, so the ordinal would climb once per element ever
///     written rather than once per live element -- and a container churning distinct members would
///     walk to `MAX_ADDRESSABLE_BLOCK_ID` and fall off the ceiling into the no-ordinal-at-all case,
///     for a set that never held more than a handful of members at a time; and
///   * the component-match early return would match the TOMBSTONE, so a re-add would be handed the
///     dead block's position instead of a free one.
///
/// Neither is corruption -- the ceiling case is a documented graceful loss of the ordinal and not of
/// the element -- and both are a real regression in headroom that nothing else would have reported.
/// `element_ordinal_reuse` is the suite that measures the reuse and is what makes this filter
/// attributable rather than defensive.
pub(super) fn container_page_ordinal(
    bucket_index: &CoreIndex,
    routing_bucket: u32,
    model_id: &str,
    object_key: &str,
    component: &str,
) -> u32 {
    let mut highest: Option<u64> = None;
    if let Some(bucket) = bucket_index.bucket_map.get(&routing_bucket) {
        for page in bucket.block_index.values() {
            if page.deleted {
                continue;
            }
            if page.model_id.as_str() != model_id || page.object_key.as_ref() != object_key {
                continue;
            }
            let Some(held) = page.address.block_id() else {
                continue;
            };
            if page.component.as_deref() == Some(component) {
                // This member already holds a block, and the ordinal on it IS its position. An
                // overwrite is the same element in the same place.
                return u32::try_from(held).unwrap_or(0);
            }
            highest = Some(highest.map_or(held, |current: u64| current.max(held)));
        }
    }
    // The first element of an object is 0, which is also what an object past the ceiling keeps.
    let next = highest.map_or(0, |held| held.saturating_add(1));
    if next > crate::block_store::MAX_ADDRESSABLE_BLOCK_ID {
        return 0;
    }
    u32::try_from(next).unwrap_or(0)
}

pub(super) fn object_block_lookup_key(
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> String {
    let mut key = String::new();
    push_lookup_part(&mut key, model_id);
    push_lookup_part(&mut key, object_key);
    match component {
        Some(component) => {
            key.push_str("1|");
            push_lookup_part(&mut key, component);
        }
        None => key.push_str("0|"),
    }
    key
}

fn push_lookup_part(buffer: &mut String, value: &str) {
    buffer.push_str(&value.len().to_string());
    buffer.push(':');
    buffer.push_str(value);
    buffer.push('|');
}

fn same_block_address(left: &BlockAddress, right: &BlockAddress) -> bool {
    left.block_slab_id() == right.block_slab_id()
        && left.offset() == right.offset()
        && left.length() == right.length()
        && left.block_id() == right.block_id()
    // `routing_bucket` is not compared, because an address no longer holds one. It was the same
    // kind of clause `generation` is: a block's copy of a value the container decides. Two blocks
    // in ONE bucket cannot differ on the bucket, and two blocks in different buckets differ on the
    // map key rather than on anything inside the address.
    //
    // `generation` is not compared either, because it no longer CAN differ here: it is derived as
    // `block_id.or(object_id)` and both of those are compared on the two lines above, so the
    // clause that used to sit here was implied by them and could not fail. A comparison that
    // cannot fail reads like extra safety and provides none.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct ControlStateSelectionValue {
    pub(super) occur_time_ms: u64,
    pub(super) value: Vec<u8>,
    #[serde(alias = "fol_type")]
    pub(super) selection_type: ControlStateSelectionType,
}

#[derive(Debug, Default, Clone)]
pub(super) struct AdmissionState {
    pub(super) window_epoch_sec: u64,
    pub(super) read_count: u64,
    pub(super) write_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum AdmissionScope {
    Shard(ShardId),
    Table(String),
    Tenant(String),
}

pub(super) struct AdmissionLimit {
    pub(super) scope: AdmissionScope,
    pub(super) limit: u64,
    pub(super) label: &'static str,
}

pub(super) struct ExecuteOutcome {
    pub(super) response: CommandResponse,
    pub(super) mutated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PackedFeatureBlock {
    pub(super) version: u8,
    pub(super) points: Vec<FeaturePoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PackedFeatureBlockDecode {
    Legacy,
    Packed(Vec<FeaturePoint>),
    Corrupt(String),
}

/// One windowed seen-set: both views hold exactly the same entries.
#[derive(Debug, Clone, Default)]
pub(super) struct SeenSet {
    pub(super) by_member: BTreeMap<Vec<u8>, u64>,
    pub(super) by_time: BTreeMap<(u64, Vec<u8>), ()>,
}


#[cfg(test)]
mod component_lookup_tests {
    use super::*;

    /// A block carrying nothing but the identity the lookup keys on.
    fn page(object: &str, component: Option<&str>) -> BlockIndex {
        BlockIndex {
            object_key: Arc::from(object.to_string()),
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
            component: component.map(str::to_string).map(Arc::from),
            address: BlockAddress::from_parts(0, 0, 0, None, Some(0)),
            dirty: false,
            deleted: false,
            log_backed: false,
        }
    }

    /// Built through the real insert rather than assembled by hand. These tests turn on the
    /// component ordering that removal binary-searches, and that ordering is the insert's to
    /// maintain -- a fixture that imitates it can agree with an insert that has stopped holding it.
    fn core_with(object: &str, components: &[Option<&str>]) -> CoreIndex {
        let mut index = CoreIndex::default();
        for (i, component) in components.iter().enumerate() {
            index.insert_object_block_lookup(
                i as u32,
                i as u64,
                &page(object, *component),
            );
        }
        index
    }

    fn components_left(index: &CoreIndex, object: &str) -> Vec<Option<String>> {
        index
            .object_block_refs("hash", object)
            .map(|entry| {
                entry
                    .by_component
                    .iter()
                    .map(|component| {
                        component.component.as_ref().map(|name| name.to_string())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    #[test]
    fn removing_one_component_leaves_the_others() {
        let mut index = core_with("k", &[Some("a"), Some("b"), Some("c")]);
        index.remove_object_block_lookup_entry("hash", "k", Some("b"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("c".to_string())]
        );
    }

    #[test]
    fn removing_the_first_and_last_components_works() {
        let mut index = core_with("k", &[Some("a"), Some("b"), Some("c")]);
        index.remove_object_block_lookup_entry("hash", "k", Some("a"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("b".to_string()), Some("c".to_string())]
        );
        index.remove_object_block_lookup_entry("hash", "k", Some("c"));
        assert_eq!(components_left(&index, "k"), vec![Some("b".to_string())]);
    }

    #[test]
    fn a_none_component_is_removable_and_does_not_take_the_others() {
        // `None` sorts before every `Some`, so it is the range's first element -- the case most
        // likely to run off the front of the set.
        let mut index = core_with("k", &[None, Some("a"), Some("b")]);
        index.remove_object_block_lookup_entry("hash", "k", None);
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[test]
    fn every_ref_sharing_a_component_goes() {
        let mut index = CoreIndex::default();
        for i in 0..3u32 {
            index.insert_object_block_lookup(
                i,
                i as u64,
                &page("k", Some("dup")),
            );
        }
        index.insert_object_block_lookup(9, 9, &page("k", Some("keep")));
        index.remove_object_block_lookup_entry("hash", "k", Some("dup"));
        assert_eq!(components_left(&index, "k"), vec![Some("keep".to_string())]);
    }

    #[test]
    fn removing_an_absent_component_changes_nothing() {
        let mut index = core_with("k", &[Some("a"), Some("b")]);
        index.remove_object_block_lookup_entry("hash", "k", Some("zzz"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[test]
    fn emptying_the_set_drops_the_key_entirely() {
        let mut index = core_with("k", &[Some("only")]);
        index.remove_object_block_lookup_entry("hash", "k", Some("only"));
        assert!(index.object_block_refs("hash", "k").is_none());
    }

    #[test]
    fn the_running_ref_total_is_decremented_by_what_was_removed() {
        // main keeps a running total beside the map; `retain` derived the decrement by
        // differencing the length, and this form knows it directly. Same number either way.
        let mut index = core_with("k", &[Some("a"), Some("b"), Some("c")]);
        index.object_component_block_refs = Some(3);
        index.remove_object_block_lookup_entry("hash", "k", Some("b"));
        assert_eq!(index.object_component_block_refs, Some(2));
    }
}
