// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::block_store::BlockAddress;
use crate::types::{CommandResponse, FeaturePoint, ControlStateSelectionType, ShardId};

use super::control_rollup::RollupEntry;
use super::hll::Hll;

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

/// Where a WAL-resident page's bytes are: the log id of the record carrying it, and that
/// record's sequence (which is what log reclaim reasons about).
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct WalResidentPage {
    pub(super) log_id: u64,
    pub(super) sequence: u64,
}

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
    /// Pages whose only durable copy is a WAL record, and which record holds each one.
    ///
    /// The resolver's table is process-local, so after a restart it is empty: the served index
    /// still points at a synthetic address and nothing can turn it back into bytes until a full
    /// replay re-derives the page. Recording the log id HERE means the mapping travels with the
    /// index that depends on it, and a reload hands it straight back.
    ///
    /// A stale entry costs a miss, never wrong bytes. The resolver reads the record at that log
    /// id and looks for the object inside it, so a reclaimed record or a superseded page simply
    /// is not found, and the read falls through exactly as it did before this existed.
    #[serde(default)]
    pub(super) wal_resident_pages: BTreeMap<u64, WalResidentPage>,
    /// Routing buckets whose derived runtime flags may be stale, so a write can refresh the
    /// buckets it touched instead of sweeping the shard.
    ///
    /// `refresh_bucket_runtime_flags` recomputes `in_memory` / `deleted` / `dirty` / `ttl_ms` /
    /// `layout` for EVERY bucket, and the single-write path called it on every write. Each bucket
    /// costs `O(its pages)`, so the sweep is `O(total pages)` per write and ingestion is quadratic
    /// in the corpus. Note that the routing-slot range does not soften this: fewer slots means
    /// fewer, larger buckets and the same total page count.
    ///
    /// Recorded where the routing bucket is already known -- the bucket-index upsert, the removal
    /// paths, and the async dirty mark -- rather than inferred from the key, because a stored
    /// address may carry an explicit routing bucket that disagrees with `page_routing_bucket`.
    ///
    /// Not persisted: on load this is empty, and every load and recovery path already runs the
    /// full sweep, so a fresh process starts from fully recomputed flags.
    #[serde(skip)]
    pub(super) buckets_pending_flag_refresh: BTreeSet<u32>,
    /// Deadlines, kept in key order so a sweep can resume from its cursor and look at the
    /// window rather than at everything.
    pub(super) expires_at_ms: BTreeMap<String, u64>,
    pub(super) strings: HashMap<String, BlockAddress>,
    pub(super) hashes: HashMap<String, HashMap<String, BlockAddress>>,
    #[serde(default, with = "super::set_index_serde")]
    pub(super) sets: HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>,
    /// Windowed seen-sets backing idempotency keys: member -> when it was last seen, plus
    /// the same entries time-ordered so expiry pops from the front in bounded steps. Like the
    /// buckets, no pages back this state -- it persists with the shard index snapshot, and a
    /// crash forgetting a window's worth of members re-admits a duplicate rather than
    /// dropping a legitimate first ingest.
    #[serde(default, with = "super::seen_index_serde")]
    pub(super) seen: HashMap<String, SeenSet>,
    /// Token buckets: key -> (tokens remaining, last refill ms). Config rides each command,
    /// never the store -- the caller owns policy, which is exactly what a quota layer wants.
    /// No pages back this state: it persists only with the shard index snapshot, so a crash
    /// refills every bucket to capacity. That direction is deliberate and documented -- a
    /// limiter that briefly over-admits after a crash beats one that starves recovered
    /// tenants on stale counts.
    #[serde(default)]
    pub(super) buckets: HashMap<String, (f64, u64)>,
    /// Sorted sets: member -> (total-order score bits, element page). The score-ordered view
    /// is derived per query -- V1 accepts the per-range sort; the upgrade path is a second
    /// in-memory map rebuilt at load, never a second persisted structure (the index component
    /// already encodes score-then-member, so recovery has the order for free).
    #[serde(default, with = "super::zset_index_serde")]
    pub(super) zsets: HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>,
    /// Redis-style lists: element pages keyed by a signed sequence -- left pushes walk the
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
    pub(super) control_state_pages: HashMap<String, BlockAddress>,
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
    // control-state counter writes skip the redundant per-write whole-series page rewrite
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
    pub(super) context_nodes: HashMap<String, BlockAddress>,
    // Keyed by EVENT ID HASH, aligning events with entities/embeddings so update and delete
    // address one event directly in log n instead of scanning the node's whole series (mem0
    // delete carries the event id, not the time, so it previously had no way to locate one).
    #[serde(default)]
    pub(super) context_events: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // Time index over the same events: timeline_key -> event_id_hash, where timeline_key stays
    // timestamp_ms * CONTEXT_TIMELINE_FANOUT + (id % FANOUT). The primary map above is ordered
    // by hash, which is effectively random, so a time window is no longer a contiguous range in
    // it -- and 13 sites scan events by time window (context_timeline_start/end). Those range
    // over this index and dereference into the primary, keeping time reads at log n + k rather
    // than degrading them to a full series scan to make deletes cheaper.
    //
    // Rebuilt at load from the same page decode the event load path already performs, so it
    // costs no extra on-disk state; it is serialized with the index like the primary because
    // ShardState is snapshotted whole.
    #[serde(default)]
    pub(super) context_event_timeline: HashMap<String, BTreeMap<u64, u64>>,
    #[serde(default)]
    pub(super) context_indexes: HashMap<String, BTreeMap<u64, BlockAddress>>,
    #[serde(default)]
    pub(super) context_audits: HashMap<String, BTreeMap<u64, BlockAddress>>,
    // Summary-dirty tracking is intentionally in-memory only. Instead of appending a
    // persisted `ctx:dirty` page per event (which produced one dirty node per write and
    // unbounded dirty-page growth: a real e2e capture stored 47 dirty records for only 6
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
    pub(super) dirty_objects: BTreeSet<String>,
    /// Phase-1 flat-append (TS_PHASE1_FLAT) fast-skip flag for the per-execute
    /// `promote_model_maps_to_bucket_index_authority` reconciliation. The live write path already
    /// keeps `bucket_index` authoritative in step with the model maps (each mutating command
    /// upserts its page into `bucket_index` before returning), so once a full promote scan has
    /// confirmed the two are in sync a repeat per-command O(store) scan can only re-confirm it.
    /// Set true after a confirmed/rebuilt reconcile at the hot-path call site; `#[serde(skip)]`
    /// so it is false on every fresh load -> the first live command after any reload pays one
    /// full reconcile and re-establishes it. Only consulted when `phase1_flat_enabled()`.
    #[serde(skip)]
    pub(super) promote_scan_done: bool,
    /// Resume point and candidate pool for sampled eviction. In-memory and ephemeral like
    /// `bucket_recency`: on restart the scan simply restarts from the top, which costs one
    /// pass, not correctness. Only consulted when `evict_sampled_lru_enabled()`.
    #[serde(skip)]
    pub(super) evict_sampler: super::eviction_sampler::EvictionSamplerState,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct CoreIndex {
    #[serde(default, alias = "slots")]
    #[serde(rename = "slot_map")]
    pub(super) bucket_map: BucketMap,
    #[serde(default)]
    pub(super) object_page_lookup: ObjectPageLookup,
    /// One shared copy of each page kind, so a page holds a pointer rather than its own string.
    ///
    /// Measured over 2700 pages: `model_id` had 2 distinct values and 2700 copies -- 1350 copies
    /// of each, and an allocation apiece. The object key, by contrast, had 1650 distinct values
    /// for those same 2700 pages, which is why it is not interned here: sharing something that is
    /// nearly unique saves nothing.
    ///
    /// Owned by the index rather than a global, so the write path interns through the `&mut` it
    /// already holds and no lock appears on it. Capped, so being bounded is a property of this
    /// code rather than a promise about every future caller: past the cap a kind still works, it
    /// just allocates as it did before. Not serialized -- it is a sharing detail, not part of the
    /// index.
    #[serde(skip)]
    pub(super) kind_pool: std::collections::HashSet<Arc<str>>,
    /// Running total of page refs across `object_component_lookup`, or `None` when not known.
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
    /// recomputed by `rebuild_object_page_lookup`. `None` means "not established yet" (a
    /// freshly deserialized index, before any rebuild) and the reader falls back to the walk,
    /// so a missing value costs time rather than correctness.
    #[serde(skip)]
    pub(super) object_component_page_refs: Option<usize>,
}

pub(super) type BucketMap = BTreeMap<u32, BucketNode>;
pub(super) type ObjectIndex = BTreeSet<u64>;
/// Keyed by a SHARED page-ref key: the same allocation is held by the lookups that point at this
/// page, instead of each of the three keeping its own copy of the same ~117-byte string.
/// Pages of one bucket, keyed by an id assigned when the page is filed.
///
/// The key used to be a rendered string of the page's identity and address -- 45.6 B a page, and
/// three quarters of what a page cost on the heap. It was never read as a name: every lookup goes
/// through a ref this map handed out, and a rewrite produces a different key while leaving one
/// entry, so identity comes from the lookup rather than from key equality.
///
/// Serializes as the string map it always was. The key is rebuilt from the value, which carries
/// every part of it, and the handle is recomputed from the page on load.
///
/// The handle is NOT free to choose: the lookup's refs hold handles and the lookup is written to
/// disk, so a handle has to mean the same page in every process that reads the file. Assigning
/// them from a counter compiled, round-tripped, and lost an object on the first reload.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(from = "BTreeMap<String, PageIndex>")]
pub(super) struct PageIndexMap {
    by_key: BTreeMap<u64, PageIndex>,
}

impl PageIndexMap {
    pub(super) fn get(&self, key: &u64) -> Option<&PageIndex> {
        self.by_key.get(key)
    }

    pub(super) fn get_mut(&mut self, key: &u64) -> Option<&mut PageIndex> {
        self.by_key.get_mut(key)
    }

    pub(super) fn remove(&mut self, key: &u64) -> Option<PageIndex> {
        self.by_key.remove(key)
    }

    /// File a page and return the handle it is filed under.
    /// Install a page and return its handle.
    ///
    /// A page with the same identity replaces the one already there rather than adding beside it,
    /// which is what the rendered string key used to do by being the key.
    pub(super) fn insert(&mut self, page: PageIndex) -> u64 {
        let handle = page_index_handle(&page);
        self.by_key.insert(handle, page);
        handle
    }

    pub(super) fn len(&self) -> usize {
        self.by_key.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    pub(super) fn iter(&self) -> std::collections::btree_map::Iter<'_, u64, PageIndex> {
        self.by_key.iter()
    }

    pub(super) fn values(&self) -> std::collections::btree_map::Values<'_, u64, PageIndex> {
        self.by_key.values()
    }

    pub(super) fn values_mut(&mut self) -> std::collections::btree_map::ValuesMut<'_, u64, PageIndex> {
        self.by_key.values_mut()
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&u64, &mut PageIndex) -> bool) {
        self.by_key.retain(|key, page| keep(key, page));
    }
}

impl<'a> IntoIterator for &'a PageIndexMap {
    type Item = (&'a u64, &'a PageIndex);
    type IntoIter = std::collections::btree_map::Iter<'a, u64, PageIndex>;
    fn into_iter(self) -> Self::IntoIter {
        self.by_key.iter()
    }
}

/// Collecting pages assigns handles, the same as inserting them one at a time.
impl FromIterator<PageIndex> for PageIndexMap {
    fn from_iter<I: IntoIterator<Item = PageIndex>>(pages: I) -> Self {
        let mut map = Self::default();
        for page in pages {
            map.insert(page);
        }
        map
    }
}

impl From<BTreeMap<String, PageIndex>> for PageIndexMap {
    fn from(flat: BTreeMap<String, PageIndex>) -> Self {
        // The handle is recomputed from the page, not read from the file and not assigned by a
        // counter. A counter would hand out different handles than the ones the lookup refs were
        // written with, and those refs are on disk too.
        let mut by_key = BTreeMap::new();
        for (_written_key, page) in flat {
            by_key.insert(page_index_handle(&page), page);
        }
        Self { by_key }
    }
}

impl Serialize for PageIndexMap {
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
        let mut entries: Vec<(String, &PageIndex)> = self
            .by_key
            .values()
            .map(|page| (page_index_written_key(page), page))
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));

        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, page) in &entries {
            map.serialize_entry(key, page)?;
        }
        map.end()
    }
}

/// The key this map writes, rebuilt from the page it is stored against.
///
/// The same spelling the map used to hold, so a dump written now reads the same as one written
/// before. Shared with the replay log so the two cannot drift.
/// The in-memory handle for a page: its identity, hashed.
///
/// Derived rather than assigned, because handles are written to disk inside the lookup's refs.
/// Two processes holding the same page must compute the same handle or those refs point at
/// nothing -- which is what a counter did, silently, until a reload lost an object.
///
/// Hashes exactly the fields [`page_index_written_key`] renders, so the handle and the written
/// key always name the same page. Equal identity therefore lands on one slot, which is also how
/// this map keeps a rewrite from accumulating a second entry for the same page.
pub(super) fn page_index_handle(page: &PageIndex) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    page.model_id.hash(&mut hasher);
    page.object_key.hash(&mut hasher);
    page.component.as_deref().hash(&mut hasher);
    page.address.page_slab_id.hash(&mut hasher);
    page.address.offset.hash(&mut hasher);
    page.address.length.hash(&mut hasher);
    page.address.page_id().unwrap_or_default().hash(&mut hasher);
    page.address.generation().unwrap_or_default().hash(&mut hasher);
    hasher.finish()
}

pub(super) fn page_index_written_key(page: &PageIndex) -> String {
    crate::index_log::page_ref_key_from_parts(
        &page.model_id,
        &page.object_key,
        page.component.as_deref(),
        page.address.page_slab_id,
        page.address.offset,
        page.address.length,
        page.address.page_id().unwrap_or_default(),
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
/// Pages by object, nested under the model that owns them.
///
/// Flat, this was keyed by a `model|object` concatenation: a string built for every stored object
/// and rebuilt for every lookup. Measured at 37 B per object -- more than either copy of the object
/// key itself -- and unshareable, because it is a different string from the key it contains.
///
/// Nested, the outer key is the model, which is already a shared pointer, and nothing is
/// concatenated. A lookup walks two maps instead of building a string.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    from = "BTreeMap<String, ObjectPageRefs>",
    into = "BTreeMap<String, ObjectPageRefs>"
)]
pub(super) struct ObjectPageLookup {
    by_model: BTreeMap<Arc<str>, BTreeMap<Arc<str>, ObjectPageRefs>>,
}

impl ObjectPageLookup {
    pub(super) fn get(&self, model_id: &str, object_key: &str) -> Option<&ObjectPageRefs> {
        self.by_model.get(model_id)?.get(object_key)
    }

    pub(super) fn get_mut(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&mut ObjectPageRefs> {
        self.by_model.get_mut(model_id)?.get_mut(object_key)
    }

    /// The entry for this object, created empty if absent. Takes the model by shared pointer so
    /// the outer key costs nothing to store.
    /// Takes both keys by shared pointer. The object key is the one the page entry already
    /// holds, so filing a page adds a pointer rather than a second copy of its identity.
    pub(super) fn entry(
        &mut self,
        model_id: &Arc<str>,
        object_key: &Arc<str>,
    ) -> &mut ObjectPageRefs {
        let objects = self.by_model.entry(Arc::clone(model_id)).or_default();
        if !objects.contains_key(object_key.as_ref()) {
            objects.insert(Arc::clone(object_key), ObjectPageRefs::default());
        }
        objects
            .get_mut(object_key.as_ref())
            .expect("just inserted")
    }

    /// The address of the inner key allocation, so a test can assert that a page and this map
    /// point at one copy of the object identity rather than two equal ones. Contents cannot tell
    /// those apart; pointers can.
    #[cfg(test)]
    pub(super) fn key_ptr(&self, model_id: &str, object_key: &str) -> Option<*const u8> {
        let (stored, _) = self.by_model.get(model_id)?.get_key_value(object_key)?;
        Some(stored.as_ptr())
    }

    pub(super) fn remove(&mut self, model_id: &str, object_key: &str) -> Option<ObjectPageRefs> {
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

    pub(super) fn values(&self) -> impl Iterator<Item = &ObjectPageRefs> {
        self.by_model.values().flat_map(BTreeMap::values)
    }

    /// Model and object for every entry, for the places that used to read the composite key.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &Arc<str>, &ObjectPageRefs)> {
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

impl From<BTreeMap<String, ObjectPageRefs>> for ObjectPageLookup {
    fn from(flat: BTreeMap<String, ObjectPageRefs>) -> Self {
        let mut nested: BTreeMap<Arc<str>, BTreeMap<Arc<str>, ObjectPageRefs>> = BTreeMap::new();
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

impl From<ObjectPageLookup> for BTreeMap<String, ObjectPageRefs> {
    fn from(nested: ObjectPageLookup) -> Self {
        let mut flat = BTreeMap::new();
        for (model, objects) in nested.by_model {
            for (object, refs) in objects {
                flat.insert(object_component_lookup_key(&model, &object), refs);
            }
        }
        flat
    }
}

/// The page refs of one object, grouped by component and ordered by it.
///
/// A sorted vector rather than a map because the measured average is 1.0 components per object: a
/// B-tree holding a single entry is a node and an allocation spent to express a list of one.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ObjectPageRefs {
    #[serde(default)]
    pub(super) by_component: Vec<ComponentPages>,
}

/// One component of one object, and the pages holding it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct ComponentPages {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<Arc<str>>,
    /// Sorted and deduplicated, and held inline when there is only one -- which is all of them.
    /// Insertion goes through `PageRefs::insert`, which keeps both invariants.
    #[serde(default)]
    pub(super) refs: PageRefs,
}

impl ObjectPageRefs {
    /// Where this component sits, or where it would be inserted. `None` sorts first, matching
    /// `Option`'s own ordering, so the vector's order is the order a caller would expect.
    pub(super) fn position(&self, component: Option<&str>) -> Result<usize, usize> {
        self.by_component
            .binary_search_by(|entry| entry.component.as_deref().cmp(&component))
    }

    pub(super) fn refs_for(&self, component: Option<&str>) -> Option<&[PageLookupRef]> {
        self.position(component)
            .ok()
            .map(|at| self.by_component[at].refs.as_slice())
    }

    /// Every page ref of this object, across every component, in component order.
    pub(super) fn all_refs(&self) -> impl Iterator<Item = &PageLookupRef> {
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
    if let Some(shared) = pool.get(kind) {
        return Arc::clone(shared);
    }
    let shared: Arc<str> = Arc::from(kind);
    if pool.len() < KIND_POOL_CAP {
        pool.insert(Arc::clone(&shared));
    }
    shared
}

/// The pages holding one component, with the single-page case held inline.
///
/// Measured over a corpus mixing single-value objects and multi-field ones: 3600 of 3600
/// components hold exactly one ref. That is not a property of the workload mix the way the
/// component count per object is -- it held for both object shapes -- so the single case is worth
/// having a shape for.
///
/// A `Vec` for one element costs a 24-byte header and, more expensively, its own heap allocation
/// to hold a single 24-byte value. Inline costs 8 bytes more inside the enclosing vector and no
/// allocation at all. The spilled arm keeps the behaviour unchanged for a component that does span
/// several pages -- nothing in the measured corpus does, but the format allows it, so it stays
/// representable rather than being asserted away.
///
/// Serializes as a sequence exactly as the vector did, so the on-disk index is unchanged.
#[derive(Debug, Clone, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<PageLookupRef>", into = "Vec<PageLookupRef>")]
pub(super) enum PageRefs {
    One(PageLookupRef),
    Many(Vec<PageLookupRef>),
}

impl PageRefs {
    pub(super) fn as_slice(&self) -> &[PageLookupRef] {
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

    pub(super) fn iter(&self) -> std::slice::Iter<'_, PageLookupRef> {
        self.as_slice().iter()
    }

    /// Insert keeping the refs sorted and free of duplicates, which is what the set this replaced
    /// did. Reports whether anything was added, which is what the ref counter is kept from.
    pub(super) fn insert(&mut self, value: PageLookupRef) -> bool {
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

impl Default for PageRefs {
    fn default() -> Self {
        Self::Many(Vec::new())
    }
}

/// Compared by contents, so a one-element spilled arm and an inline one are the same refs. Without
/// this, a value read back from an index written before this change could compare unequal to the
/// identical value built in memory.
impl PartialEq for PageRefs {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl From<Vec<PageLookupRef>> for PageRefs {
    fn from(mut refs: Vec<PageLookupRef>) -> Self {
        if refs.len() == 1 {
            Self::One(refs.pop().expect("length just checked"))
        } else {
            Self::Many(refs)
        }
    }
}

impl From<PageRefs> for Vec<PageLookupRef> {
    fn from(refs: PageRefs) -> Self {
        match refs {
            PageRefs::One(value) => vec![value],
            PageRefs::Many(values) => values,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct PageLookupRef {
    #[serde(rename = "routing_slot")]
    pub(super) routing_bucket: u32,
    pub(super) page_ref_key: u64,
}

/// Rust-native core index mirroring the shape:
/// Index -> BucketMap -> BucketNode -> PageIndex/ObjectIndex.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct BucketNode {
    #[serde(rename = "routing_slot")]
    pub(super) routing_bucket: u32,
    #[serde(default)]
    pub(super) layout: BucketLayoutState,
    pub(super) dirty: bool,
    #[serde(default)]
    pub(super) deleted: bool,
    pub(super) meta_loaded: bool,
    pub(super) loading: bool,
    pub(super) in_memory: bool,
    pub(super) ttl_ms: Option<u64>,
    pub(super) dirty_generation: u64,
    pub(super) last_dump_sequence: u64,
    #[serde(default, alias = "object_ids")]
    pub(super) object_index: ObjectIndex,
    #[serde(default, alias = "deleted_object_ids")]
    pub(super) deleted_object_index: ObjectIndex,
    #[serde(default, alias = "page_refs")]
    pub(super) page_index: PageIndexMap,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum BucketLayoutState {
    #[default]
    Empty,
    SingleObject,
    SinglePageObject,
    MultiPageObject,
    MultiObject,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PageIndex {
    pub(super) object_key: Arc<str>,
    pub(super) model_id: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<Arc<str>>,
    pub(super) address: BlockAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
}

impl PageIndex {
    /// The object this page belongs to.
    ///
    /// Held once, in the address, rather than beside it. The entry used to carry its own copy and
    /// the two agreed on every page -- necessarily, since the field was assigned from the address.
    /// The write path now puts the computed id into the address, including the fallback used when
    /// an address arrives without one, so this can always answer.
    pub(super) fn object_id(&self) -> u64 {
        self.address.object_id().unwrap_or_default()
    }
}

impl CoreIndex {
    pub(super) fn rebuild_object_page_lookup(&mut self) {
        // The rebuild is already O(pages); establishing the total here costs nothing extra and
        // is what lets the stats path stop walking the shard.
        self.object_component_page_refs = Some(0);
        self.object_page_lookup.clear();
        let refs = self
            .bucket_map
            .iter()
            .flat_map(|(routing_bucket, bucket)| {
                bucket.page_index.iter().map(move |(page_ref_key, page)| {
                    (*routing_bucket, *page_ref_key, page.clone())
                })
            })
            .collect::<Vec<_>>();
        for (routing_bucket, page_ref_key, page) in refs {
            self.insert_object_page_lookup(routing_bucket, page_ref_key, &page);
        }
    }

    /// Takes the handle the page index filed this page under, so the two cannot name different
    /// things. It used to take the rendered key by shared pointer; the key is a number now and
    /// costs nothing to copy.
    pub(super) fn insert_object_page_lookup(
        &mut self,
        routing_bucket: u32,
        page_ref_key: u64,
        page: &PageIndex,
    ) {
        if page.deleted {
            return;
        }
        let added = {
            let entry = self
                .object_page_lookup
                .entry(&page.model_id, &page.object_key);
            let value = PageLookupRef {
                routing_bucket,
                page_ref_key,
            };
            match entry.position(page.component.as_deref()) {
                Ok(at) => entry.by_component[at].refs.insert(value),
                Err(at) => {
                    // A component's first page. Build the entry already holding it, so the common
                    // case never allocates and there is no empty state in between.
                    entry.by_component.insert(
                        at,
                        ComponentPages {
                            component: page.component.clone(),
                            refs: PageRefs::One(value),
                        },
                    );
                    true
                }
            }
        };
        if added {
            if let Some(total) = self.object_component_page_refs.as_mut() {
                *total = total.saturating_add(1);
            }
        }
    }

    /// Every page ref this object holds, for one component.
    pub(super) fn page_refs_for(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) -> Option<&[PageLookupRef]> {
        self.object_page_lookup
            .get(model_id, object_key)
            .and_then(|entry| entry.refs_for(component))
    }

    /// Every component of this object, and the pages holding each.
    pub(super) fn object_page_refs(
        &self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&ObjectPageRefs> {
        self.object_page_lookup.get(model_id, object_key)
    }

    pub(super) fn remove_object_page_lookup_entry(
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
        if let Some(entry) = self.object_page_lookup.get_mut(model_id, object_key) {
            if let Ok(at) = entry.position(component) {
                removed = entry.by_component.remove(at).refs.len();
            }
            now_empty = entry.by_component.is_empty();
        }
        if now_empty {
            self.object_page_lookup.remove(model_id, object_key);
        }
        if removed > 0 {
            if let Some(total) = self.object_component_page_refs.as_mut() {
                *total = total.saturating_sub(removed);
            }
        }
    }

    /// Drop every ref an object holds under one kind.
    ///
    /// The delete path used to call `rebuild_object_page_lookup` instead, which clears the whole
    /// lookup, clones every page in every bucket into a vector, and re-inserts them -- so one
    /// delete cost work proportional to the entire shard, and deleting a store cost the square of
    /// it. Removing the object's own entry is the same result for a fraction of the work.
    ///
    /// Maintains `object_component_page_refs` exactly as the per-component removal above does,
    /// because that counter is what lets the stats path avoid walking the shard.
    pub(super) fn remove_object_from_page_lookup(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) -> usize {
        let Some(entry) = self.object_page_lookup.remove(model_id, object_key) else {
            return 0;
        };
        let removed: usize = entry
            .by_component
            .iter()
            .map(|component| component.refs.len())
            .sum();
        if removed > 0 {
            if let Some(total) = self.object_component_page_refs.as_mut() {
                *total = total.saturating_sub(removed);
            }
        }
        removed
    }

    pub(super) fn contains_object_page_address(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
        address: &BlockAddress,
    ) -> bool {
        if let Some(page_refs) = self.page_refs_for(model_id, object_key, component) {
            return page_refs.iter().any(|page_ref| {
                self.bucket_map
                    .get(&page_ref.routing_bucket)
                    .and_then(|bucket| bucket.page_index.get(&page_ref.page_ref_key))
                    .map(|page| {
                        !page.deleted
                            && &*page.model_id == model_id
                            && &*page.object_key == object_key
                            && page.component.as_deref() == component
                            && same_page_address(&page.address, address)
                    })
                    .unwrap_or(false)
            });
        }

        if !self.object_page_lookup.is_empty() {
            return false;
        }

        self.bucket_map.values().any(|bucket| {
            bucket.page_index.values().any(|page| {
                !page.deleted
                    && &*page.model_id == model_id
                    && &*page.object_key == object_key
                    && page.component.as_deref() == component
                    && same_page_address(&page.address, address)
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

pub(super) fn object_page_lookup_key(
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

fn same_page_address(left: &BlockAddress, right: &BlockAddress) -> bool {
    left.page_slab_id == right.page_slab_id
        && left.offset == right.offset
        && left.length == right.length
        && left.page_id() == right.page_id()
        && left.object_id() == right.object_id()
        && left.routing_bucket() == right.routing_bucket()
        && left.generation() == right.generation()
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
pub(super) struct PackedFeaturePage {
    pub(super) version: u8,
    pub(super) points: Vec<FeaturePoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PackedFeaturePageDecode {
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

    /// A page carrying nothing but the identity the lookup keys on.
    fn page(object: &str, component: Option<&str>) -> PageIndex {
        PageIndex {
            object_key: Arc::from(object.to_string()),
            model_id: Arc::from("hash".to_string()),
            component: component.map(str::to_string).map(Arc::from),
            address: BlockAddress::from_parts(0, 0, 0, None, Some(0), None, None, None),
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
            index.insert_object_page_lookup(
                i as u32,
                i as u64,
                &page(object, *component),
            );
        }
        index
    }

    fn components_left(index: &CoreIndex, object: &str) -> Vec<Option<String>> {
        index
            .object_page_refs("hash", object)
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
        index.remove_object_page_lookup_entry("hash", "k", Some("b"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("c".to_string())]
        );
    }

    #[test]
    fn removing_the_first_and_last_components_works() {
        let mut index = core_with("k", &[Some("a"), Some("b"), Some("c")]);
        index.remove_object_page_lookup_entry("hash", "k", Some("a"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("b".to_string()), Some("c".to_string())]
        );
        index.remove_object_page_lookup_entry("hash", "k", Some("c"));
        assert_eq!(components_left(&index, "k"), vec![Some("b".to_string())]);
    }

    #[test]
    fn a_none_component_is_removable_and_does_not_take_the_others() {
        // `None` sorts before every `Some`, so it is the range's first element -- the case most
        // likely to run off the front of the set.
        let mut index = core_with("k", &[None, Some("a"), Some("b")]);
        index.remove_object_page_lookup_entry("hash", "k", None);
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[test]
    fn every_ref_sharing_a_component_goes() {
        let mut index = CoreIndex::default();
        for i in 0..3u32 {
            index.insert_object_page_lookup(
                i,
                i as u64,
                &page("k", Some("dup")),
            );
        }
        index.insert_object_page_lookup(9, 9, &page("k", Some("keep")));
        index.remove_object_page_lookup_entry("hash", "k", Some("dup"));
        assert_eq!(components_left(&index, "k"), vec![Some("keep".to_string())]);
    }

    #[test]
    fn removing_an_absent_component_changes_nothing() {
        let mut index = core_with("k", &[Some("a"), Some("b")]);
        index.remove_object_page_lookup_entry("hash", "k", Some("zzz"));
        assert_eq!(
            components_left(&index, "k"),
            vec![Some("a".to_string()), Some("b".to_string())]
        );
    }

    #[test]
    fn emptying_the_set_drops_the_key_entirely() {
        let mut index = core_with("k", &[Some("only")]);
        index.remove_object_page_lookup_entry("hash", "k", Some("only"));
        assert!(index.object_page_refs("hash", "k").is_none());
    }

    #[test]
    fn the_running_ref_total_is_decremented_by_what_was_removed() {
        // main keeps a running total beside the map; `retain` derived the decrement by
        // differencing the length, and this form knows it directly. Same number either way.
        let mut index = core_with("k", &[Some("a"), Some("b"), Some("c")]);
        index.object_component_page_refs = Some(3);
        index.remove_object_page_lookup_entry("hash", "k", Some("b"));
        assert_eq!(index.object_component_page_refs, Some(2));
    }
}
