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
    #[serde(default)]
    pub(super) object_component_lookup: ObjectComponentLookup,
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
pub(super) type PageIndexMap = BTreeMap<Arc<str>, PageIndex>;
pub(super) type ObjectPageLookup = BTreeMap<String, BTreeSet<PageLookupRef>>;
pub(super) type ObjectComponentLookup = BTreeMap<String, BTreeSet<ComponentPageLookupRef>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct PageLookupRef {
    #[serde(rename = "routing_slot")]
    pub(super) routing_bucket: u32,
    pub(super) page_ref_key: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct ComponentPageLookupRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<String>,
    #[serde(rename = "routing_slot")]
    pub(super) routing_bucket: u32,
    pub(super) page_ref_key: Arc<str>,
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
    pub(super) object_key: String,
    pub(super) model_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) component: Option<String>,
    pub(super) object_id: u64,
    pub(super) address: BlockAddress,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
    pub(super) log_backed: bool,
}

impl CoreIndex {
    pub(super) fn rebuild_object_page_lookup(&mut self) {
        // The rebuild is already O(pages); establishing the total here costs nothing extra and
        // is what lets the stats path stop walking the shard.
        self.object_component_page_refs = Some(0);
        self.object_page_lookup.clear();
        self.object_component_lookup.clear();
        let refs = self
            .bucket_map
            .iter()
            .flat_map(|(routing_bucket, bucket)| {
                bucket.page_index.iter().map(move |(page_ref_key, page)| {
                    (*routing_bucket, page_ref_key.clone(), page.clone())
                })
            })
            .collect::<Vec<_>>();
        for (routing_bucket, page_ref_key, page) in refs {
            self.insert_object_page_lookup(routing_bucket, page_ref_key, &page);
        }
    }

    /// Takes the key by shared pointer: both lookups clone the `Arc`, not the string, so the
    /// three structures that point at a page hold one allocation between them.
    pub(super) fn insert_object_page_lookup(
        &mut self,
        routing_bucket: u32,
        page_ref_key: Arc<str>,
        page: &PageIndex,
    ) {
        if page.deleted {
            return;
        }
        self.object_page_lookup
            .entry(object_page_lookup_key(
                &page.model_id,
                &page.object_key,
                page.component.as_deref(),
            ))
            .or_default()
            .insert(PageLookupRef {
                routing_bucket,
                page_ref_key: Arc::clone(&page_ref_key),
            });
        let added = self
            .object_component_lookup
            .entry(object_component_lookup_key(
                &page.model_id,
                &page.object_key,
            ))
            .or_default()
            .insert(ComponentPageLookupRef {
                component: page.component.clone(),
                routing_bucket,
                page_ref_key,
            });
        if added {
            if let Some(total) = self.object_component_page_refs.as_mut() {
                *total = total.saturating_add(1);
            }
        }
    }

    pub(super) fn remove_object_page_lookup_entry(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) {
        self.object_page_lookup
            .remove(&object_page_lookup_key(model_id, object_key, component));
        let component_lookup_key = object_component_lookup_key(model_id, object_key);
        if let Some(component_refs) = self.object_component_lookup.get_mut(&component_lookup_key) {
            // `ComponentPageLookupRef` orders on `component` first, so every ref for one component
            // is a contiguous range and can be found by seeking to it. Walking the whole set
            // instead cost time proportional to how many components the object has -- and for a
            // shard hash the components ARE its fields, so writing one field walked every field
            // already in that hash, once per field written.
            let first = ComponentPageLookupRef {
                component: component.map(str::to_string),
                routing_bucket: 0,
                // Only a range START: the empty key sorts below every real page-ref key, so the
                // seek lands on this component's first ref. It became an `Arc<str>` when the key
                // was made shared, and the sentinel has to follow the field.
                page_ref_key: Arc::from(""),
            };
            let doomed: Vec<ComponentPageLookupRef> = component_refs
                .range(first..)
                .take_while(|page_ref| page_ref.component.as_deref() == component)
                .cloned()
                .collect();
            let removed = doomed.len();
            for page_ref in doomed {
                component_refs.remove(&page_ref);
            }
            if removed > 0 {
                if let Some(total) = self.object_component_page_refs.as_mut() {
                    *total = total.saturating_sub(removed);
                }
            }
            if component_refs.is_empty() {
                self.object_component_lookup.remove(&component_lookup_key);
            }
        }
    }

    pub(super) fn contains_object_page_address(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
        address: &BlockAddress,
    ) -> bool {
        let lookup_key = object_page_lookup_key(model_id, object_key, component);
        if let Some(page_refs) = self.object_page_lookup.get(&lookup_key) {
            return page_refs.iter().any(|page_ref| {
                self.bucket_map
                    .get(&page_ref.routing_bucket)
                    .and_then(|bucket| bucket.page_index.get(&page_ref.page_ref_key))
                    .map(|page| {
                        !page.deleted
                            && page.model_id == model_id
                            && page.object_key == object_key
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
                    && page.model_id == model_id
                    && page.object_key == object_key
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
        && left.page_id == right.page_id
        && left.object_id == right.object_id
        && left.routing_bucket == right.routing_bucket
        && left.generation == right.generation
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

    fn core_with(object: &str, components: &[Option<&str>]) -> CoreIndex {
        let mut index = CoreIndex::default();
        let set = index
            .object_component_lookup
            .entry(object_component_lookup_key("hash", object))
            .or_default();
        for (i, component) in components.iter().enumerate() {
            set.insert(ComponentPageLookupRef {
                component: component.map(str::to_string),
                routing_bucket: i as u32,
                page_ref_key: Arc::from(format!("p{i}").as_str()),
            });
        }
        index
    }

    fn components_left(index: &CoreIndex, object: &str) -> Vec<Option<String>> {
        index
            .object_component_lookup
            .get(&object_component_lookup_key("hash", object))
            .map(|refs| refs.iter().map(|r| r.component.clone()).collect())
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
        let set = index
            .object_component_lookup
            .entry(object_component_lookup_key("hash", "k"))
            .or_default();
        for i in 0..3u32 {
            set.insert(ComponentPageLookupRef {
                component: Some("dup".to_string()),
                routing_bucket: i,
                page_ref_key: Arc::from(format!("p{i}").as_str()),
            });
        }
        set.insert(ComponentPageLookupRef {
            component: Some("keep".to_string()),
            routing_bucket: 9,
            page_ref_key: Arc::from("p9"),
        });
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
        assert!(index
            .object_component_lookup
            .get(&object_component_lookup_key("hash", "k"))
            .is_none());
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
