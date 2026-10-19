// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::block_store::ElementEntry;
use super::timestamped_series::OwnedTimestampedSeries;
use crate::types::{CommandResponse, ControlStateSelectionType, FeaturePoint, ShardId};

use super::control_rollup::RollupEntry;
use super::hll::Hll;
use super::hash_field_map::HashFieldMap;

/// WHICH FLAVOUR OF DIRTINESS A MARK IS, as a field rather than as a key prefix.
///
/// One map holds both, and it used to tell them apart by testing whether the key began
/// `ctx:embdirty:`. That worked and was documented as load-bearing, which is the tell: a
/// boolean spelled as a string prefix has to be parsed back out, and the parse is what every
/// reader then depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum DirtyFlavour {
    /// A node whose SUMMARY is stale.
    Summary,
    /// A node whose EMBEDDING is stale.
    Embedding,
}

/// THE KEY OF ONE DIRTY MARK: the two integers it is about, and which flavour it is.
///
/// This replaced `String`. The string was `ctx:dirty:{tenant}:{node}` or
/// `ctx:embdirty:{tenant}:{node}` -- a heap allocation per mark spelling two integers that
/// `ContextDirtyEntry` already carries as fields, plus a prefix standing in for the flavour.
/// Measured at 400 context events: 800 keys holding 14,000 characters, 52% of every key
/// character in the context family, in the one map of that family that could not share an
/// allocation with anything because it was the only String-keyed one.
///
/// ORDERED, because a bounded drain sorts its candidates for a stable pass across calls. The
/// derived order is (tenant, node, flavour), which is not the lexicographic order of the old
/// keys -- a drain's ORDER is an implementation detail it only needs to be STABLE in, and the
/// test over it asserts stability rather than a particular sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct DirtyMark {
    pub(super) tenant_hash: u64,
    pub(super) node_hash: u64,
    pub(super) flavour: DirtyFlavour,
}

impl DirtyMark {
    pub(super) fn summary(tenant_hash: u64, node_hash: u64) -> Self {
        Self { tenant_hash, node_hash, flavour: DirtyFlavour::Summary }
    }

    pub(super) fn embedding(tenant_hash: u64, node_hash: u64) -> Self {
        Self { tenant_hash, node_hash, flavour: DirtyFlavour::Embedding }
    }
}

/// Twenty-four bytes and no heap at all, pinned as an equality so a field added without
/// thought shows up here rather than in a footprint report.
const _: () = assert!(std::mem::size_of::<DirtyMark>() == 24);

/// In-memory, coalesced summary-dirty entry.
///
/// One entry per `DirtyMark` -- a tenant, a node and a flavour. Repeated
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
/// THE CLASS IS A KEY-TYPE CLASS AND NOT A VALUE-TYPE ONE, which matters now that the values have
/// started to diverge: `control_state_blocks` holds an `ObjectRecord` rather than a bare address.
/// The three still agree on `ModelKey`, which is what the shared array needs. And they are not
/// three equal candidates for that record -- `context_nodes` has NO production writer (its only
/// `insert` is `#[cfg(test)]`) and is reached in production only through an `.or_else()` after
/// `hashes` is consulted first, so it is a decode path for blocks written before that move. See
/// `block_store`'s note on `CONTEXT_NODE_FIELD`. Converting it would change the stored shape of
/// exactly the population it exists to read back, for no live value.
///
/// EVERY READ SHAPE IS UNCHANGED, which is why this is a key-type change and not a surface change.
/// `Box<str>: Borrow<str>`, so `get`, `get_mut`, `contains_key` and `remove` still take `&str` and
/// every call site that passes one compiles untouched. `keys()` yields `&Box<str>`, which derefs to
/// `&str`. Only the sites that CREATE a key change, and they change from `.clone()` to `.into()`.
///
/// NO STORED BYTES MOVE. The shard index is `serde_json`, where a map key is a string either way;
/// `the_string_map_key_is_the_same_json_either_way` round-trips a snapshot to prove it rather than
/// asserting it, so this carries no index format stamp.
pub(super) type ModelKey = Arc<str>;

/// Move one map's key onto the canonical allocation, if it is holding its own equal copy.
///
/// Factored out of `adopt_context_event_key` rather than copied per map: the body is identical for
/// every `ModelKey`-keyed per-kind map, and four hand-written copies of a pointer comparison is
/// four places for one of them to drift. The decision is taken before the move so the shared borrow
/// ends before the mutable one.
fn adopt_key_in<V>(
    map: &mut HashMap<ModelKey, V>,
    object_key: &str,
    canonical: &ModelKey,
) -> bool {
    let moves = match map.get_key_value(object_key) {
        Some((held, _)) => !Arc::ptr_eq(held, canonical),
        None => false,
    };
    if !moves {
        return false;
    }
    match map.remove(object_key) {
        Some(value) => {
            map.insert(Arc::clone(canonical), value);
            true
        }
        None => false,
    }
}

/// Why a context-event key adoption did or did not move the pair. Counted rather than reasoned
/// about: a mechanism that is wired and still does nothing looks exactly like one that is not
/// wired, and only the split between "no handle to take" and "took one" tells them apart.
pub(super) static CONTEXT_EVENT_ADOPT_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static CONTEXT_EVENT_ADOPT_NO_HANDLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static CONTEXT_EVENT_ADOPT_MOVED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

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
    ///
    /// THE KEY IS AN `Arc<str>` SO THAT THIS MAP AND ITS MIRROR HOLD ONE COPY OF THE CHARACTERS
    /// BETWEEN THEM, not one each. A deadline used to cost two independent `String`s of the same
    /// text -- this map's key and the mirror's tuple element -- which is two of the five copies a
    /// live object was measured to keep. `set_expiry` allocates once and hands both maps a
    /// refcounted handle to it.
    ///
    /// THE STORED BYTES DO NOT MOVE. `serde`'s `rc` feature is enabled, so an `Arc<str>` key
    /// serializes as the same string a `String` key did, and the index image is byte-identical --
    /// asserted by `the_expiry_key_change_writes_the_same_index_bytes` rather than argued from the
    /// feature flag. So this owes no format stamp.
    ///
    /// AND SHARING DOES NOT SURVIVE A DECODE, which is the part worth knowing. `serde` gives each
    /// occurrence its own allocation on the way back in; what restores the sharing is
    /// `ensure_expiry_order`, whose `key.clone()` on an `Arc` IS the sharing operation rather than
    /// a copy. That is measured on both paths -- fresh, and after a dump and restore -- by
    /// `the_expiry_pair_shares_one_allocation_on_both_paths`, because an argument that a rebuild
    /// shares is not a measurement that it does.
    pub(super) expires_at_ms: BTreeMap<Arc<str>, u64>,
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
    /// THE TUPLE'S SECOND ELEMENT IS A SHARED HANDLE, NOT A SECOND COPY. See
    /// `expires_at_ms` above: the ordering here is the whole point of this map and is untouched,
    /// but the characters it orders are the ones that map already holds.
    pub(super) expiry_by_deadline: BTreeMap<(u64, Arc<str>), ()>,
    pub(super) strings: HashMap<ModelKey, ElementEntry>,
    // DURABLE, AND THIS LINE SAID IT WAS NOT. It read "Rebuildable from the durable bucket/block
    // index on load; do not duplicate in checkpoints", which was true while this field was
    // `skip_serializing` and the element name on a page-index entry was the durable spelling of a
    // hash field. The entry has no element-name field now, so nothing could rebuild this map from
    // the index: `hashes` carries `#[serde(default)]` below, as `sets`, `zsets` and `lists` do, and
    // the four resident maps ARE the authority rather than a derived view of one.
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
    //
    // AND THE WRAPPER AROUND IT MOVES NO BYTES EITHER. `RecordedHashContainer` is
    // `#[serde(transparent)]` over exactly one field, `HashMap<String, HashFieldMap>` -- the type
    // this field had -- so the derived impls forward verbatim and no snapshot, manifest or
    // index-log encoding changes by a byte. `SHARD_INDEX_FORMAT_VERSION` does not move for it, and
    // `engine::tests::recorded_hash_container_invariant` holds the two encodings equal against the
    // bare map with a control that detects a planted difference.
    //
    // WHAT THE WRAPPER IS FOR IS NOT FOOTPRINT. It owns the map together with the durable record of
    // its mutations: the inner map is private to the container's module, so the only way to change
    // membership is to pass a value that could not have been constructed without the record already
    // being emitted. A writer that mutates this map without recording does not compile. The five
    // paths that legitimately mutate WITHOUT a record -- the reconcile, the delta fold, two replay
    // arms and the compactor's address rewrite -- are named methods there, each with its reason,
    // and that set is closed. See `engine::recorded_hash_container`.
    #[serde(default)]
    pub(super) hashes: super::recorded_map::RecordedMap<super::recorded_map::HashKind>,
    // THE WIRE IS UNCHANGED AND THE `with =` MOVED RATHER THAN WENT. `RecordedSetContainer` owns
    // this map together with the durable record of its mutations -- the inner map is private, so the
    // only way to change membership is to pass a value that could not exist unless the record was
    // already emitted, and a writer that mutates without recording does not compile. Its
    // `Serialize`/`Deserialize` impls CALL `set_index_serde`, the same functions this field named,
    // so no snapshot, manifest or index-log encoding moves a byte and
    // `SHARD_INDEX_FORMAT_VERSION` does not change for it.
    //
    // The four paths that legitimately mutate WITHOUT a record -- the reconcile, the delta fold,
    // the replay removal and the compactor's address rewrite -- are named methods there, each with
    // its reason, and that set is closed. FOUR and not hash's five: recovery's `set` arm re-files
    // its block, so unlike hash's `context_node` arm it needs no replay-install exception.
    #[serde(default)]
    pub(super) sets: super::recorded_map::RecordedMap<super::recorded_map::SetKind>,
    /// Windowed seen-sets backing idempotency keys: member -> when it was last seen, plus
    /// the same entries time-ordered so expiry pops from the front in bounded steps. Like the
    /// buckets, no blocks back this state -- it persists with the shard index snapshot, and a
    /// crash forgetting a window's worth of members re-admits a duplicate rather than
    /// dropping a legitimate first ingest.
    #[serde(default, with = "super::seen_index_serde")]
    pub(super) seen: HashMap<ModelKey, SeenSet>,
    /// Token buckets: key -> (tokens remaining, last refill ms). Config rides each command,
    /// never the store -- the caller owns policy, which is exactly what a quota layer wants.
    /// No blocks back this state: it persists only with the shard index snapshot, so a crash
    /// refills every bucket to capacity. That direction is deliberate and documented -- a
    /// limiter that briefly over-admits after a crash beats one that starves recovered
    /// tenants on stale counts.
    #[serde(default)]
    pub(super) buckets: HashMap<ModelKey, (f64, u64)>,
    /// Sorted sets: member -> (total-order score bits, element block). The score-ordered view
    /// is derived per query -- V1 accepts the per-range sort; the upgrade path is a second
    /// in-memory map rebuilt at load, never a second persisted structure (the index component
    /// already encodes score-then-member, so recovery has the order for free).
    // THE WIRE IS UNCHANGED AND THE `with =` MOVED RATHER THAN WENT: `ZSetKind` names
    // `zset_index_serde` as its codec, which is a REQUIRED associated function with no default --
    // so this kind cannot inherit the wrong encoding, which is the defect a blanket impl produced
    // for `sets` and which three reload tests caught.
    #[serde(default)]
    pub(super) zsets: super::recorded_map::RecordedMap<super::recorded_map::ZSetKind>,
    /// Redis-style lists: element blocks keyed by a signed sequence -- left pushes walk the
    /// low end down, right pushes walk the high end up, so both ends are O(log n) and the
    /// BTree's order IS the list's order.
    // A PLAIN MAP, STATED RATHER THAN INHERITED. An `i64` key has a string form a JSON object can
    // carry, so this kind never needed the codec `sets` and `zsets` do -- and `ListKind` says so
    // explicitly instead of defaulting to it.
    #[serde(default)]
    pub(super) lists: super::recorded_map::RecordedMap<super::recorded_map::ListKind>,
    /// Feature series as an OWNED SORTED ARRAY, written in the B-tree shape it has always had.
    ///
    /// Measured: a `BTreeMap` of 1,000 points costs **49.1 B a point** where the payload is 24 --
    /// `(u64, ElementEntry)` with a 16-byte entry -- and the array costs **24.0 B a point, exactly
    /// the payload**. At a 40,040-key corpus that is this row going 49.1 -> 24.0 B/key, which is
    /// 25.0 B/key off the resident index and the largest single row left in it.
    /// `what_the_feature_series_would_cost_as_an_owned_array` is that measurement and asserts the
    /// array stays the smaller of the two.
    ///
    /// The stored image does not move: see [`super::timestamped_series::as_btree_maps`].
    #[serde(with = "super::timestamped_series::as_btree_maps")]
    pub(super) features: HashMap<ModelKey, OwnedTimestampedSeries>,
    /// THE KEY IS A SHARED HANDLE, AND ONLY HALF OF THEM CAN ACTUALLY SHARE.
    ///
    /// This map holds two kinds of key. The counter commands file the RAW object key, which
    /// `strings`, `features` and `control_state_selection` may hold too -- measured at 8 of 8
    /// keys in `which_string_keyed_holders_duplicate_a_key_another_map_holds`, where those three
    /// agreed on one allocation and this map owned a second copy of the same characters. The
    /// family operators file a DECORATED key (`control_state:<family>:<key>`) that no other map
    /// holds, so that half has nothing to share with and converting it buys nothing.
    ///
    /// Both halves are `ModelKey` because one map cannot have two key types; the saving is per
    /// RAW key, not per entry, and that probe asserts the split so the two denominators cannot
    /// drift into each other unnoticed.
    ///
    /// The stored image does not move: `Arc<str>` serializes as the same string a `String` did.
    pub(super) control_state: HashMap<ModelKey, BTreeMap<u64, i64>>,
    /// Counter-series blocks, one per control-state object key.
    ///
    /// THE VALUE IS A RECORD RATHER THAN A BARE ADDRESS, and this is the first map to hold one.
    /// A live object was costing several key-addressed structures, each paying its own node AND
    /// its own copy of the key's characters; the record is the single row those collapse into. It
    /// carries the address this map always held, plus a deadline slot, state bits and a model id
    /// that are PRESENT AND UNUSED here -- so the width goes live with no behaviour change, and
    /// the deadline merge that fills them is a separate change with its own evidence.
    ///
    /// READ IT THROUGH `address()`, WRITE IT THROUGH `set_address()`. The second matters: writing
    /// back by building a fresh record drops the unused slots silently, and would not fail while
    /// they are unused. See `ObjectRecord::set_address`.
    ///
    /// A STORED-FORMAT CHANGE, which is why the format stamp moves with it. An index written
    /// before this carries a bare address here and is refused by the stamp check rather than
    /// decoded into the wrong shape; the refusal falls back to replay, and the reconcile rebuilds
    /// this map from the bucket index.
    /// THE CONSOLIDATED MAP: one durable structure, keyed kind then object key.
    ///
    /// Two levels and not a flat `(kind, key)` map, because a key may hold DIFFERENT KINDS AT
    /// ONCE. That is measured, not assumed: a string and a hash under the same key both answer
    /// ok and both read back their own bytes, in either write order. A flat map keyed by the
    /// object key alone would collide them, and the collision is reachable by any client through
    /// the public surface. Two levels also keep a per-kind walk at exactly today's cost -- one
    /// lookup, then iterate the kind -- where a flat map would make every such walk scan every
    /// kind.
    ///
    /// A `BTreeMap` OUTSIDE, FOR ITS ORDER AND NOT ITS SPEED. Seventeen entries at most, so the
    /// container's lookup cost is irrelevant and the temptation is to reach for a `HashMap`. The
    /// order is the point: the dump manifest feeds its digest from an iteration key by key and
    /// recomputes it on restore, so an unordered outer map makes that digest depend on hash
    /// iteration order and silently invalidates every manifest written before the change. See the
    /// test that pins it, which is the first thing in the tree asserting that digest is
    /// order-sensitive at all.
    ///
    /// ONE RECORD PER KEY, AND THE COMPILER IS WHAT HOLDS THAT -- see #2165. A per-element
    /// tenant needs `ObjectSlot`'s owned element array, which has no stored form, so the
    /// only door that fills it demands a tenancy witness whose durable arm is an uninhabited
    /// type and cannot be spelled: see [`super::object_record::OwnedElementTenancy`], which
    /// carries the reasoning and the condition for deleting it.
    #[serde(default)]
    pub(super) objects: std::collections::BTreeMap<
        super::storage_bucket_internals::StoredModelKind,
        HashMap<ModelKey, super::object_record::ObjectSlot>,
    >,

    /// WHERE A MULTI-PAGE OBJECT'S PAGES LIVE, so they hang off the object's own record.
    ///
    /// A record keeps ONE address word that is either a page address or a handle
    /// (`PageAddress::One` / `PageAddress::Many`). This is what the handle half points into. Until
    /// it existed, a handle was only a number: every production mention of `PageAddress::Many`,
    /// `Deadline::PerElement` and `RecordFlags::ADDRESS_IS_POINTER` sat inside `object_record.rs`
    /// itself, so the arm was reachable from tests and from nothing else.
    ///
    /// SKIPPED IN BOTH DIRECTIONS, AND THAT IS PROVABLY LOSSLESS RIGHT NOW RATHER THAN A BARGAIN.
    /// Nothing writes a layout yet -- `objects` is filled for exactly one kind, `ControlState`,
    /// whose entries take the inline arm -- so this is empty in every image this binary writes.
    /// Skipped in BOTH directions rather than on the write half alone: no image in existence
    /// carries this field, so there is no old row for a deserialize half to deliver, and skipping
    /// both ways leaves the stored bytes byte-identical with no format stamp owed.
    ///
    /// The one-sided spelling is deliberately not named here. `tools/` scans this struct for that
    /// token to find fields whose contents a dump drops, and it carries the match forward across
    /// comment lines so a multi-line attribute still reaches its field -- so PROSE mentioning the
    /// token arms it and the next field is reported as dropping its contents. This field does not;
    /// it is empty in every image. The scan's sensitivity to prose is a defect in the scan and is
    /// filed as one, rather than worked around by making this comment vaguer than it should be.
    ///
    /// THE CONDITION ON THE FIRST WRITER, which is the part that will not be obvious later: the
    /// first path that files a layout here MUST bring a wire form with it, because at that moment
    /// this field becomes the only copy of where those pages are and a reload would drop them.
    /// That is the same trap `ObjectSlot`'s owned element array is held back by -- see #2165 -- and
    /// the reason it is written here is that the emptiness argument above stops being true the
    /// instant a writer lands, silently.
    // Serialized, not skipped. Its own doc above says this field becomes the only
    // copy of where a record's pages are and that a reload would drop them, and the
    // scan meant to catch exactly that could not see it: the scan looks for the long
    // form of the skip attribute, and the short form contains none of that text. The
    // inner types already derive Serialize, so carrying it costs only a `default` so
    // that dumps written before this still decode.
    #[serde(default)]
    pub(super) object_page_layouts: super::object_record::PageLayoutStore,
    /// Per `(key, bucket)` distinct-count state, in whichever representation the bucket holds.
    ///
    /// ONE MAP, WHERE THERE WERE TWO. `control_state_change_sketch` stood beside this holding the
    /// sketched buckets, keyed identically, and `record_change` kept the two mutually exclusive by
    /// removing from one as it inserted into the other. They were one logical map with the
    /// exclusivity maintained by a remove/insert pair; [`super::hll::DistinctBucket`] makes it
    /// unrepresentable instead, and that type's doc records what the merge does and does not buy.
    ///
    /// THE STORED SHAPE MOVED, WHICH IS WHY THE FORMAT STAMP MOVED WITH IT. An index written
    /// before this carries `control_state_changes` whose bucket values are bare member ARRAYS and
    /// a separate `control_state_change_sketch` object. Decoded against this shape the member
    /// array does not match either variant, and the sketch map is a field no reader claims -- so
    /// the stamp refuses the index rather than letting a decode decide, and the load falls back to
    /// WAL replay.
    #[serde(default)]
    /// KEYED BY A SHARED HANDLE, like every other map that holds a raw object key.
    ///
    /// It was `String`-keyed, and `hll::record_change` filled it with
    /// `.entry(key.to_string())` -- a fresh allocation per key. Measured by the default-run
    /// guard: SEVEN holders of one raw key text reporting TWO allocations, at 8 of 8 keys, with
    /// this map owning the second. It was invisible to that guard until it was named in it,
    /// because the guard collects pointers out of a list of map names and this was not on the
    /// list -- the staleness its own doc warns about, arriving.
    ///
    /// The stored shape does not move: `Arc<str>` serialises exactly as `String` does, which is
    /// the same argument `control_state`'s conversion made, so no format stamp is owed.
    pub(super) control_state_changes:
        HashMap<ModelKey, BTreeMap<u64, super::hll::DistinctBucket>>,
    #[serde(default, alias = "control_state_fol")]
    pub(super) control_state_selection: HashMap<ModelKey, ControlStateSelectionValue>,
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
    pub(super) context_nodes: HashMap<ModelKey, ElementEntry>,
    /// THE EVENT SERIES, ON THE OWNED SORTED ARRAY.
    ///
    /// Keyed by EVENT ID HASH, aligning events with entities/embeddings so update and delete
    /// address one event directly in log n instead of scanning the whole series of a node (mem0
    /// delete carries the event id, not the time, so it previously had no way to locate one).
    ///
    /// THE REPRESENTATION IS [`OwnedTimestampedSeries`] AND NO LONGER A `BTreeMap`. One owned
    /// sorted array per key, eight bytes in the map value and two allocations when it holds
    /// anything, against a 24-byte B-tree header and a 280-byte root node allocated for the first
    /// element alone. See that type for the measured pair of numbers and for why an array is the
    /// right shape for an append-mostly series read by single instant.
    ///
    /// NOT SERIALIZED. The stored image is the compatibility field below, under the wire key this
    /// map has always had, so the image is unchanged and no format stamp is owed. This field is
    /// filled from that one by [`ShardState::fold_legacy_context_events`] at decode, which DRAINS
    /// the other to empty -- so there is exactly one live copy of this data at every moment after
    /// the decode returns.
    #[serde(skip)]
    context_events_series: HashMap<ModelKey, OwnedTimestampedSeries>,
    /// THE COMPATIBILITY KEY, AND ITS OWN REMOVAL CONDITION.
    ///
    /// The event series also live in `context_events_series` above, which is authoritative. This
    /// copy exists only so that what a stored image carries under `context_events` is exactly what
    /// it carried before, in both directions: an index written by this binary is readable by an
    /// older one, and one written by an older binary still delivers its rows here.
    ///
    /// IT KEEPS `skip_serializing`, WHICH IS WHAT IT ALREADY HAD. This map is not written into a
    /// new image at all -- it is derived at load from the block decode -- so the stored image is
    /// byte-identical, not merely compatible. `default` is what still lets an image written before
    /// `skip_serializing` landed deliver its rows, and that is the case the decode fold is for.
    ///
    /// WHY IT CANNOT JUST BE DELETED YET. Two reasons, and NEITHER IS A DOOR THAT FORGETS TO
    /// CHECK THE STAMP. Every door that installs a stored index does check it, through
    /// `persistence::refuse_stale_index_stamp`. An argument resting on an unguarded door was
    /// written here first and was wrong; these two do not rest on one, which is why they survive
    /// that door being shut.
    ///
    /// First, THE STAMP CANNOT REFUSE A ROW IT DOES NOT KNOW ABOUT. `index_stamp_is_stale` is
    /// `shard.index_format_version < SHARD_INDEX_FORMAT_VERSION` -- `<` and not `!=` -- so an
    /// image at the CURRENT stamp is accepted by every door, and the stamp is a single integer
    /// that says nothing about which keys an image carries. So an accepted, perfectly current
    /// image can still carry `context_events`, and with this field gone its rows would not be
    /// refused: they would be silently ignored, which is the one outcome worse than a refusal.
    ///
    /// Second, and this is the path the decode fold does NOT reach at all: THE KEY-STATE CAPTURE
    /// CARRIES THIS MAP UNDER ITS OWN NAME. `capture_key_states` writes a `context_events` field
    /// into the index-log blobs and `apply_key_states` reads it back -- written by THIS binary at
    /// THIS stamp, and folded on a path that takes `ShardState::default()` and decodes nothing,
    /// so no staleness test is reached and none could apply if it were. That path is covered by
    /// the `KeyedState` conversion in `engine.rs`, which turns the stored B-tree shape the blob
    /// carries into the array -- a conversion and not a mirror, because there is one live copy.
    ///
    /// REMOVE THIS -- the field, the decode fold and the conversion -- once no stored image and no
    /// captured blob this binary can meet still carries the key under its old name. That is a
    /// statement about what is reachable, not about which doors are
    /// guarded, and it is the one to re-check rather than the stamp coverage.
    ///
    /// PRIVATE, AND THAT IS WHAT MAKES "DRAINED AND NEVER REFILLED" A PROPERTY OF THE CODE
    /// rather than a request. Nothing outside `state.rs` can reach this field, and inside it the
    /// only writer is the fold, which empties it. That is the difference between a decode-only
    /// landing spot and a second live copy, and it is one line of code wide -- the mirror version
    /// of this change refilled it at the end of the fold.
    #[serde(rename = "context_events", default, skip_serializing)]
    context_events_compat: HashMap<String, BTreeMap<u64, ElementEntry>>,
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
    pub(super) context_event_timeline: HashMap<ModelKey, BTreeMap<u64, u64>>,
    // ON THE OWNED SORTED ARRAY, NOT A B-TREE A KEY.
    //
    // 98.1% of this map's keys hold exactly ONE point, measured over 2,000 extracted events in
    // `which_series_maps_the_grouped_shape_would_help`: the index fans each event out across five
    // index kinds whose cardinalities differ by orders of magnitude, and the high-cardinality ones
    // (entity above all) give a key per value. A one-element `BTreeMap` costs 312 B in 4
    // allocations where this array costs 48 B in 2, so that distribution is the worst possible fit
    // for a B-tree a key and the best possible for the array.
    //
    // AND THIS MAP OWES NO COMPATIBILITY FIELD, which is what makes it the first tenant rather
    // than the fifth. `OwnedTimestampedSeries` is deliberately not serializable, so a map adopting
    // it normally keeps its stored image in a compat field under its historical wire key -- but
    // this field was `#[serde(default, skip_serializing)]` and has no stored image at all: the key
    // was never written, so every decode found it absent and took the default. `#[serde(skip)]` is
    // that same pair of behaviours written as one attribute -- nothing on disk changes, no format
    // stamp is owed, and it is what lets the value type stay unserializable.
    //
    // THE ATTRIBUTE AUDIT AGREES, which is worth stating because this looks like the shape it
    // hunts. `a_field_that_stops_being_written` reports a field that GAINS `skip_serializing`
    // WITHOUT a default -- a key that stops being written while a decode still demands it -- and it
    // carries a test asserting that a bare `skip` is NOT reported, because `skip` is
    // `skip_serializing` plus a default and so cannot strand a reader. This moves out of that
    // audit's scope rather than into it.
    #[serde(skip)]
    pub(super) context_indexes: HashMap<ModelKey, OwnedTimestampedSeries>,
    // ON THE OWNED SORTED ARRAY. Measured at 24.0 B a point against the B-tree's 49.1 over 40,040
    // keys (`what_the_feature_series_would_cost_as_an_owned_array`): 24 bytes is EXACTLY the
    // payload -- `(u64, ElementEntry)` with a 16-byte entry -- so the array carries no per-point
    // overhead and the B-tree roughly doubles it. Unlike the grouped shape, which loses by 5% where
    // ten points fit one nearly-full node, the plain array has no occupancy at which it is worse.
    //
    // THE STORED IMAGE DOES NOT MOVE: `as_btree_maps` writes the B-tree map this field has always
    // written and reads it back into the array, exactly as `features` above does. `default` is kept
    // because this field has always had it -- `with` covers a PRESENT field and an absent one must
    // still load as empty rather than fail.
    #[serde(default, with = "super::timestamped_series::as_btree_maps")]
    pub(super) context_audits: HashMap<ModelKey, OwnedTimestampedSeries>,
    // IN-MEMORY ONLY, AND COALESCING, which are two decisions with a measurement behind each.
    //
    // EPHEMERAL: this map is `#[serde(skip)]`, so a restart loses it, and that is acceptable
    // because the async summary worker re-marks nodes on the next event and stale summaries are
    // self-healing. What it replaced was a persisted `ctx:dirty` block per event -- a real
    // end-to-end capture stored 47 dirty records for only 6 events, because nothing coalesced.
    //
    // COALESCING: one entry per (tenant, node, flavour), so repeated edits to one node collapse
    // into a single row and the record count is bounded by distinct dirty nodes rather than by
    // events.
    //
    // ONE MAP FOR BOTH FLAVOURS. Summary-dirty and embedding-dirty were two maps of the same type
    // over the same key shape -- one map per attribute, where the attribute was a flavour of
    // dirtiness -- and they are one map now with the flavour as a field on the key.
    //
    // AND THE KEY IS THE THREE VALUES IT IS ABOUT, not a string of them. It used to be
    // `ctx:dirty:{tenant}:{node}` or `ctx:embdirty:{tenant}:{node}`: a heap allocation per mark
    // spelling two integers `ContextDirtyEntry` already carries as fields, with a prefix standing
    // in for the flavour. Measured at 400 context events, that was 800 keys holding 14,000
    // characters -- 52% of every key character in the context family, in the only String-keyed map
    // among them, so it could never share an allocation with anything. `DirtyMark` is 24 bytes
    // inline and nothing on the heap.
    //
    // IT ALSO REMOVED A TRAP RATHER THAN GUARDING ONE. The summary-mark site never set
    // `tenant_hash` on the row -- it was left ZERO -- and the embedding drain's tenant clause
    // admits everything when asked for all tenants, so a prefix test was the only thing stopping
    // an all-tenant drain handing summary entries out as embedding work. Its own doc called that
    // load-bearing, which was true and is the tell: a boolean spelled as a string prefix has to be
    // parsed back, and every reader then depends on the parse. The key carries the tenant for both
    // flavours now, so the two cannot be confused whether or not a drain filters on flavour.
    #[serde(skip)]
    pub(super) context_dirty_index: HashMap<DirtyMark, ContextDirtyEntry>,
    // Per-node temporal-compression high-water mark: the latest event time already
    // folded into a ContextCompressionEvent for this event object key. In-memory and
    // ephemeral (serde-skipped); on loss the auto-compression trigger re-compresses
    // the oldest pending window idempotently (stable compression id per window).
    #[serde(skip)]
    pub(super) context_compression_watermark: HashMap<ModelKey, u64>,
    // Entities are grouped by their OWNING NODE, not one map key per entity: the key is
    // `ctx:entity:{tenant}:{node}` (context_entity_collection_key) and the BTreeMap holds every
    // entity of that node. The inner u64 is the ENTITY HASH, not a timestamp -- unlike
    // context_events/indexes/audits, which are time-keyed. That keeps upsert as an overwrite of
    // one slot (an entity has one current value; its history is the separate
    // context_entity_update_audit series) while making a node's entities enumerable, which the
    // per-entity key shape could not do: a HashMap cannot prefix-scan, so ContextQueryEntities
    // had to be handed every entity_hash by its caller.
    #[serde(default)]
    pub(super) context_entities: HashMap<ModelKey, BTreeMap<u64, ElementEntry>>,
    // No migration field is needed: the PERSISTED entry still carries the per-entity key
    // `ctx:entity:{tenant}:{node}:{entity_hash}`, which the load path splits back into
    // (collection key, entity hash). The on-disk shape is unchanged in both directions, so an
    // index written before this fold loads natively and one written after it stays readable by
    // an older binary.
    // On the owned sorted array; see `context_audits` above for the measurement and for why
    // `default` is kept beside the adapter.
    #[serde(default, with = "super::timestamped_series::as_btree_maps")]
    pub(super) context_children: HashMap<ModelKey, OwnedTimestampedSeries>,
    // On the owned sorted array; see `context_audits` above for the measurement and for why
    // `default` is kept beside the adapter.
    #[serde(default, with = "super::timestamped_series::as_btree_maps")]
    pub(super) context_summaries: HashMap<ModelKey, OwnedTimestampedSeries>,
    // On the owned sorted array; see `context_audits` above for the measurement and for why
    // `default` is kept beside the adapter.
    #[serde(default, with = "super::timestamped_series::as_btree_maps")]
    pub(super) context_compressions: HashMap<ModelKey, OwnedTimestampedSeries>,
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

// BOTH DIRECTIONS ARE HAND-WRITTEN NOW, AND THE DECODER IS WHY THE ATTRIBUTES ARE GONE.
//
// The encoder has to be hand-written because the page index needs a SIBLING FIELD of this
// struct in scope -- serde hands a `Serialize` impl no context, so a bucket's encoder can see
// nothing but the bucket, and the blanket map impl between them is where the chain breaks.
//
// THE DECODER NEEDS THE SAME THING IN THE OTHER DIRECTION. A page's identity arrives inside the
// page index, and `object_block_lookup` -- a sibling field of this struct -- is keyed by it.
// Rebuilding that lookup after the load by walking the decoded entries is what this replaces, and
// that walk reads the identity off each entry, which is the one place it is about to stop being.
//
// AND THAT COST THE ATTRIBUTES, WHICH WAS THE RECORDED RISK OF DOING IT. They are only valid
// while a derive consumes them, so `alias = "slots"`, the defaults and the skips stop being
// DECLARATIONS the compiler enforces and become code that has to be read to be believed. Each is
// restated below as the arm that implements it, and
// `the_legacy_spelling_of_the_one_wire_field_still_loads_and_an_unknown_one_does_not` is the guard
// that was landed against the derive for exactly this moment: its discriminating pair is that the
// legacy spelling decodes POPULATED and an unrecognised one decodes EMPTY, which is the only form
// of the claim that is not true of any input whatsoever.
#[derive(Debug, Clone)]
pub(super) struct CoreIndex {
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
    ///
    /// NOT ON THE WIRE IN EITHER DIRECTION. This carried `#[serde(skip)]`; the decoder below
    /// names no arm for it, so a stored key of this name is ignored exactly as the derive ignored
    /// it, and the encoder never wrote it.
    pub(super) block_slab_live: BlockSlabLiveIndex,
    // Derived lookup tables rebuilt from the bucket map on load. Persisting them duplicates
    // block references already carried by the bucket index and made large context backfill
    // checkpoints tens of MB larger without adding authoritative recovery state.
    ///
    /// DECODED IF PRESENT AND NEVER WRITTEN, which is what `#[serde(default, skip_serializing)]`
    /// said and what the decoder below still does: it takes a stored value when a store carries
    /// one, defaults when it does not, and files each decoded page on top of whatever it started
    /// from.
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
    /// of the index, and the decoder below names no arm for it.
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
    ///
    /// ESTABLISHED BY THE DECODE NOW, where it used to be established by the rebuild that followed
    /// one. The decoder sets it to zero before it files a single page and lets each filing count,
    /// which is the same bargain `rebuild_object_block_lookup` makes -- a count that starts at
    /// "unknown" cannot be incremented into a right answer.
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
    ///
    /// Not serialized in either direction; the decoder below names no arm for it.
    pub(super) released_buckets: BTreeSet<u32>,
}

/// HAND-WRITTEN FOR ONE FIELD, AND BECAUSE A LIST OF CONSTRUCTION SITES COULD NOT BE EXHAUSTIVE.
///
/// `object_component_block_refs` is `Some(0)` from birth rather than `None`. `None` means "nobody
/// has established this count", and `insert_object_block_lookup` increments only `if let Some`, so
/// a count left at `None` can never be incremented into a right answer -- it has to START at a
/// number. The decoder already said exactly that and set zero before its first filing; this makes
/// the same statement for every other way an index comes into existence.
///
/// # WHY NOT VISIT THE CONSTRUCTION SITES INSTEAD -- ENUMERATED BY THE COMPILER, NOT GREPPED
///
/// Taking `Default` off the derive and compiling names every construction. There are two EXPLICIT
/// production sites -- `rebuild_bucket_first_index` and the decoder -- and THREE IMPLICIT routes
/// that no site list can cover:
///
///   * `ShardState` derives `Default`, so `ShardState::default()` builds one;
///   * `bucket_index` carries `#[serde(default)]`, so a stored index that omits `slot_index`
///     decodes through THIS and not through the hand-written `Deserialize` that sets zero -- which
///     is the route that matters, because it is a real file on a real load; and
///   * any `..Default::default()` in a struct update.
///
/// A site list would also go stale the first time someone adds a fourth route, with nothing
/// failing. Put on the type, the claim holds for every route that exists and every route anyone
/// adds, and a field added to this struct later is a COMPILE ERROR here rather than a silent
/// `Default::default()` nobody chose.
///
/// AN EMPTY INDEX'S REF TOTAL IS NOT UNKNOWN, IT IS ZERO, which is the other half of why this is a
/// correction rather than a convenience. The two production readers test
/// `object_block_lookup.is_empty() || object_component_block_refs.is_none()` and read either half
/// as "needs establishing". The FIRST half still answers for an index holding pages its lookup
/// does not name, so what this removes is only the arm that called a known zero unknown -- and in
/// exchange, an index that files as it goes now carries a maintained total without a rebuild
/// having to establish one.
///
/// `a_default_index_reports_a_ref_total_of_zero_by_every_route_one_is_built` drives all three
/// implicit routes plus both decode paths.
impl Default for CoreIndex {
    fn default() -> Self {
        Self {
            bucket_map: BucketMap::default(),
            block_slab_live: BlockSlabLiveIndex::default(),
            object_block_lookup: ObjectBlockLookup::default(),
            kind_pool: std::collections::HashSet::default(),
            object_component_block_refs: Some(0),
            released_buckets: BTreeSet::default(),
        }
    }
}

/// WHICH KEY OF A STORED INDEX THIS IS, including the one spelling that is only ever read.
enum CoreIndexField {
    /// `bucket_map`, and `slots` -- THE LEGACY SPELLING, which was `alias = "slots"` while a
    /// derive consumed it. An index written before the rename is keyed `slots`, and dropping this
    /// arm is a store that stops loading with no other symptom: a round trip would still pass,
    /// because a round trip only ever writes the CURRENT spelling and reads it back.
    BucketMap,
    /// `object_block_lookup`: decoded if a store carries one, never written.
    ObjectBlockLookup,
    /// Everything else, INCLUDING the four fields that are not on the wire at all. Ignored rather
    /// than refused, which is what lets an index written by a newer engine load into an older one.
    Ignore,
}

impl<'de> Deserialize<'de> for CoreIndexField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl serde::de::Visitor<'_> for FieldVisitor {
            type Value = CoreIndexField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core index field name")
            }

            fn visit_str<E>(self, value: &str) -> Result<CoreIndexField, E>
            where
                E: serde::de::Error,
            {
                Ok(match value {
                    "bucket_map" | "slots" => CoreIndexField::BucketMap,
                    "object_block_lookup" => CoreIndexField::ObjectBlockLookup,
                    _ => CoreIndexField::Ignore,
                })
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

const CORE_INDEX_FIELDS: &[&str] = &["bucket_map", "object_block_lookup"];

impl<'de> Deserialize<'de> for CoreIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IndexVisitor;

        impl<'de> serde::de::Visitor<'de> for IndexVisitor {
            type Value = CoreIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core index")
            }

            fn visit_map<M>(self, mut map: M) -> Result<CoreIndex, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                use serde::de::Error as _;

                let mut buckets: Option<BTreeMap<u32, BucketNodeWithPageKeys>> = None;
                let mut stored_lookup: Option<ObjectBlockLookup> = None;
                while let Some(key) = map.next_key::<CoreIndexField>()? {
                    match key {
                        CoreIndexField::BucketMap => {
                            // A SECOND STATEMENT OF THE SAME FACT IS REFUSED, as every arm of the
                            // node's decoder refuses one: a store carrying both spellings of this
                            // key is two disagreeing statements and must fail loudly.
                            if buckets.is_some() {
                                return Err(M::Error::duplicate_field("bucket_map"));
                            }
                            buckets = Some(map.next_value()?);
                        }
                        CoreIndexField::ObjectBlockLookup => {
                            if stored_lookup.is_some() {
                                return Err(M::Error::duplicate_field("object_block_lookup"));
                            }
                            stored_lookup = Some(map.next_value()?);
                        }
                        CoreIndexField::Ignore => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let mut index = CoreIndex {
                    object_block_lookup: stored_lookup.unwrap_or_default(),
                    ..CoreIndex::default()
                };
                // ZERO BEFORE THE FIRST FILING, not after the last. `insert_object_block_lookup`
                // increments only `if let Some(total)`, so a count left at `None` can never be
                // incremented into a right answer -- it has to START at a number.
                //
                // KEPT THOUGH `Default` NOW SAYS THE SAME THING, because this decoder does not go
                // through `Default` for this field in every arm it could gain, and because the
                // statement belongs where the filing loop below can be read beside it. It is a
                // restatement rather than a second mechanism: both write the same zero, and
                // `impl Default for CoreIndex` carries the argument for why a site list cannot be
                // relied on to.
                index.object_component_block_refs = Some(0);
                for (routing_bucket, decoded) in buckets.unwrap_or_default() {
                    for (handle, object_key) in &decoded.page_keys {
                        // THE PAGE IS LOOKED UP BY THE HANDLE IT WAS FILED UNDER rather than
                        // carried beside its key, so the entry the lookup records is the entry the
                        // map holds. A page whose handle collided with an earlier one is not in
                        // the map twice and must not be in the lookup twice either.
                        if let Some(page) = decoded.node.block_index.get(handle) {
                            index.insert_object_block_lookup(
                                routing_bucket,
                                *handle,
                                object_key,
                                page,
                            );
                        }
                    }
                    index.bucket_map.insert(routing_bucket, decoded.node);
                }
                Ok(index)
            }
        }

        deserializer.deserialize_struct("CoreIndex", CORE_INDEX_FIELDS, IndexVisitor)
    }
}

/// WHERE EACH PAGE'S OBJECT IDENTITY LIVES, INDEXED BY THE POSITION THE PAGE IS HELD AT.
///
/// The object lookup runs object -> refs. This is that map inverted once per encode, so a page can
/// be asked for its identity by the `(routing_bucket, handle)` it is filed under, which is the only
/// thing a page's position actually says about it.
///
/// BUILT IN EVERY BUILD NOW, AND THAT COSTS ONE PASS PER READER THAT NEEDS IT.
///
/// It was `#[cfg(debug_assertions)]`, because while the identity was still on the entry this was
/// only a CHECK on the bytes the entry produced. It is a SOURCE now: the readers that group pages
/// by object have no other way to ask which object a page belongs to, so the inversion has to
/// exist in a release build.
///
/// WHAT THAT COSTS, STATED RATHER THAN GLOSSED. One pass over the lookup per reader that builds
/// one, which is O(block refs) -- the same order as the walk over pages the reader is already
/// doing. It is built ONCE per report and queried per page, never built per page, and never built
/// by a point reader: a reader that starts from an object goes through `block_refs_for` in the
/// forward direction and needs none of this.
///
/// THE DELETED PAGES ARE NOT IN IT, and that is the lookup's rule rather than this type's:
/// `insert_object_block_lookup` returns early on a deleted page. [`page_identity`] is the door
/// that knows about both sources, and every reader goes through it rather than through `get`.
/// WHAT A PAGE'S NEIGHBOURS SAY ITS OBJECT IS.
///
/// The object key is handed back as the SHARED POINTER rather than as characters, because every
/// caller that keeps it keeps a pointer -- the report rows, the filing map and the index-log items
/// all held `page.object_key.clone()` and must go on costing one pointer rather than a copy of the
/// characters.
///
/// THE KIND TERM IS GONE FROM THIS STRUCT, AND THE COMPILER IS WHAT SAID SO.
///
/// It carried `model_id: &'a str` beside the key, "because that is what both sources hold it as:
/// the lookup keys on the shared name and a tombstone row holds the one-byte spelling, whose
/// `as_str` is static". Its only readers were the agreement ties that compared the identity beside
/// a page against the copy ON it -- those ties compared the MODEL term as well as the key, which
/// is what made a neighbour disagreeing about either half fail. The ties went with the field and
/// `dead_code` named this immediately.
///
/// IT IS REMOVED RATHER THAN KEPT FOR A FUTURE READER. Both sources still hold the kind and
/// `identity_of` can hand it back again in one line; what a field with no reader buys is a term
/// every caller has to pattern-match past, and the entry's own `model_id` is in scope at every one
/// of those call sites anyway.
pub(super) struct PageObjectIdentity<'a> {
    pub(super) object_key: &'a Arc<str>,
}

pub(super) struct PageKeysByPosition<'a> {
    by_position: std::collections::HashMap<(u32, u64), (&'a Arc<str>, &'a Arc<str>)>,
    /// THE SAME ANSWER WITHOUT THE BUCKET, FOR THE ONE QUESTION POSITION CANNOT ANSWER.
    ///
    /// `by_position` is the right index for every reader that asks "which object is the page at
    /// this position", because position is what a page's filing states about it. It cannot answer
    /// for a page that is filed in the WRONG bucket -- the lookup names it at the position it was
    /// filed under, so asking at the position it now sits in finds nothing.
    ///
    /// THAT STATE IS NOT HYPOTHETICAL AND IT IS NOT AN ERROR TO SWALLOW. It is precisely what
    /// `validate_bucket_ownership_index_from_entries` exists to find, and
    /// `bucket_filing_range::the_ownership_report_names_a_page_filed_where_its_key_does_not_route`
    /// drives it: a node moved under a bucket its key does not route to must be NAMED, once per
    /// page. A walk that dropped those rows would hand the validator an empty set and the report
    /// would say the index is healthy.
    ///
    /// SO THIS IS A SECOND INDEX AND NOT A REPLACEMENT. Only the entry-collecting walk reads it,
    /// through [`Self::identity_anywhere`], and the doc there says why. A handle is hashed from a
    /// block's identity and its address, so two pages share one only by being the same block --
    /// which is the same assumption `BlockIndexMap` already keys on.
    by_handle: std::collections::HashMap<u64, (&'a Arc<str>, &'a Arc<str>)>,
}

impl<'a> PageKeysByPosition<'a> {
    /// REACHABLE FROM THE SIBLING MODULE THAT DERIVES, not only from this file's encoder.
    ///
    /// It was private while the only builder was `CoreIndex::serialize` one screen below. The
    /// bucket-layout rebuild in `storage_bucket_internals` is a second builder, and it has to
    /// hoist its own: the inversion borrows `object_block_lookup` while the rebuild holds
    /// `bucket_map` mutably, and only the caller can split those two borrows apart by naming the
    /// fields. `CoreIndex::page_identities` cannot be used there for exactly that reason -- it
    /// borrows the whole index.
    pub(super) fn from_lookup(lookup: &'a ObjectBlockLookup) -> Self {
        let mut by_position = std::collections::HashMap::new();
        let mut by_handle = std::collections::HashMap::new();
        for (model_id, object_key, refs) in lookup.iter() {
            for block_ref in refs.iter() {
                by_position.insert(
                    (block_ref.routing_bucket, block_ref.block_ref_key),
                    (model_id, object_key),
                );
                by_handle.insert(block_ref.block_ref_key, (model_id, object_key));
            }
        }
        Self {
            by_position,
            by_handle,
        }
    }

    /// THE IDENTITY OF A LIVE PAGE AT A POSITION, AND ONLY OF A LIVE ONE.
    ///
    /// Named rather than spelled `get` because it answers for ONE of the two sources: the lookup
    /// holds no deleted page, so a caller that may be looking at a tombstone wants
    /// [`Self::identity_of`], which knows about both. A caller that has already filtered
    /// `!page.deleted` wants this, and wants it WITHOUT the node: this borrows the lookup alone,
    /// so it can be asked while the node is held mutably. `identity_of` cannot be -- it hands back
    /// a tombstone row and so borrows the node for as long as the answer lives.
    pub(super) fn live_identity(
        &self,
        routing_bucket: u32,
        handle: u64,
    ) -> Option<(&'a Arc<str>, &'a Arc<str>)> {
        self.by_position.get(&(routing_bucket, handle)).copied()
    }

    /// THE OBJECT A PAGE BELONGS TO, FROM WHICHEVER STRUCTURE BESIDE THE PAGE STATES IT.
    ///
    /// TWO SOURCES AND NOT ONE, because a page has one of two kinds of neighbour. A LIVE page is
    /// named by the object lookup, inverted by the `(routing_bucket, handle)` it is filed under. A
    /// DELETED page is not in that lookup at all -- `insert_object_block_lookup` returns early on
    /// one -- and is named instead by the tombstone row the removal filed in the same bucket,
    /// which is keyed by the page triple and carries both the object key and the kind.
    ///
    /// `None` HAS EXACTLY TWO CAUSES, both named so that a caller's skip is not a shrug. A live
    /// page is missing from the lookup only between a decode and a rebuild, a window the decoder
    /// closed by filing as it decodes. A deleted page is missing a row only when the removal that
    /// made it named no element -- a tombstone with nothing to say about which member went -- and
    /// that is the one page in this store whose object nothing beside it states.
    pub(super) fn identity_of(
        &self,
        node: &'a BucketNode,
        handle: u64,
        page: &BlockIndex,
    ) -> Option<PageObjectIdentity<'a>> {
        if page.deleted {
            return node
                .tombstone_element_at(&page.address)
                .map(|row| PageObjectIdentity { object_key: &row.object_key });
        }
        self.live_identity(node.routing_bucket, handle)
            .map(|(_model_id, object_key)| PageObjectIdentity { object_key })
    }

    /// WHETHER THIS NAMES NO POSITION AT ALL, which is what an UNESTABLISHED lookup looks like.
    ///
    /// The lookup is `skip_serializing` and empty between a decode and the rebuild that fills it,
    /// so an encode taken in that window has no identity beside any page. That is a state with a
    /// NAME, and the check below refuses a missing source unless this is true -- which is the
    /// difference between a skip with a reason and a skip that hides the one case that matters.
    pub(super) fn names_no_position(&self) -> bool {
        self.by_position.is_empty()
    }

    /// THE OBJECT A PAGE BELONGS TO, WHEREVER IT IS FILED -- FOR ONE CALLER AND ONE REASON.
    ///
    /// Every other reader asks by POSITION, because what a page's filing states about it is where
    /// it sits. This asks by HANDLE alone, which answers for a page the lookup names at a
    /// different position than the one it is now in.
    ///
    /// # WHY THAT IS NOT A LOOPHOLE
    ///
    /// The one caller is `collect_bucket_index_live_block_entries`, whose rows feed
    /// `validate_bucket_ownership_index_from_entries` -- a check whose entire content is comparing
    /// where a page IS against where its key ROUTES. A mis-filed page is the subject of that
    /// check, so a walk that could not name one would hand it an empty set and the report would
    /// call a damaged index healthy. Measured: moving one node under a bucket its key does not
    /// route to must produce one mismatch per page, and does.
    ///
    /// IT IS STRICTLY WIDER THAN `identity_of` AND NEVER DISAGREES WITH IT. A page the position
    /// index names is named here with the same pair, because both are filled from the same refs.
    /// What it adds is an answer where the position index has none.
    ///
    /// DELETED PAGES ARE STILL NOT IN IT, for the lookup's own reason:
    /// `insert_object_block_lookup` returns early on one. A caller that must answer for a
    /// tombstone wants [`Self::identity_of`], which knows about the bucket's rows as well.
    pub(super) fn identity_anywhere(&self, handle: u64) -> Option<(&'a Arc<str>, &'a Arc<str>)> {
        self.by_handle.get(&handle).copied()
    }
}

/// THE IDENTITY OF EVERY PAGE IN ONE BUCKET, OWNED, SO A MUTATION CAN HOLD THE BUCKET.
///
/// # WHY THIS EXISTS AND IS NOT JUST `identity_of` IN A LOOP
///
/// `BlockIndexMap::retain` borrows the page index MUTABLY, and [`PageKeysByPosition::identity_of`]
/// needs the NODE -- because a deleted page's identity lives in the node's tombstone rows and not
/// in the lookup. So a `retain` closure cannot ask which object the page it is looking at belongs
/// to: the borrow checker refuses it, and rightly, since the answer is drawn in part from the
/// structure being mutated.
///
/// Taken once BEFORE the mutation, from the same two sources, keyed by the handle the closure is
/// handed. A handle the result does not contain is a page NEITHER source names -- the caller
/// decides what that means, and in every current caller it means "this page is not the one I am
/// matching", which is the conservative direction: a predicate cannot match an object key the
/// page has none of, and widening it would take every sibling page of the bucket with it.
///
/// ONE PASS OVER ONE BUCKET, not over the shard. The inversion it is given is built once per
/// caller and shared across that caller's buckets; this is the per-bucket projection of it.
pub(super) fn page_identities_in(
    node: &BucketNode,
    keys: &PageKeysByPosition<'_>,
) -> std::collections::HashMap<u64, Arc<str>> {
    node.block_index
        .iter()
        .filter_map(|(handle, page)| {
            keys.identity_of(node, *handle, page)
                .map(|beside| (*handle, Arc::clone(beside.object_key)))
        })
        .collect()
}

/// THE ONE FIELD OF THIS STRUCT THAT REACHES THE WIRE, and the context the rest of the encode needs.
///
/// Hand-written for the reason stated at the struct: the page index has to be composed where a
/// SIBLING of the bucket map is in scope, and no derive and no `serialize_with` can offer that,
/// because both see one field at a time.
///
/// THE FIELD COUNT IS PART OF THE ENCODING, not a hint. The served index is msgpack
/// `with_struct_map`, so `serialize_struct` writes a map header of exactly this length -- the same
/// reason `BucketNode` counts its own optional key rather than hard-coding one. One field, because
/// five of the six are not written, and `what_the_core_index_puts_on_the_wire` observes that from a
/// real encode rather than reading it off the attributes.
impl Serialize for CoreIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // BUILT IN EVERY BUILD, BECAUSE IT IS THE SOURCE AND NOT A CHECK.
        //
        // It was `#[cfg(debug_assertions)]` here while the written key was composed from the
        // entry's own copy of the identity and this inversion only CONFIRMED it. The key is
        // composed from the inversion now, so a release build without one would write no key at
        // all. The type's own doc prices the pass.
        let keys = PageKeysByPosition::from_lookup(&self.object_block_lookup);
        let mut index = serializer.serialize_struct("CoreIndex", 1)?;
        index.serialize_field(
            "bucket_map",
            &BucketMapAt {
                buckets: &self.bucket_map,
                keys: &keys,
            },
        )?;
        index.end()
    }
}

/// The bucket map, with each node given the context the derive could not pass it.
///
/// The map is written exactly as the blanket impl wrote it -- same length header, same key order,
/// same key encoding -- because a `BTreeMap` iterates in key order and that is what it did before.
/// What changes is which `Serialize` each VALUE goes through.
struct BucketMapAt<'a> {
    buckets: &'a BucketMap,
    keys: &'a PageKeysByPosition<'a>,
}

impl Serialize for BucketMapAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.buckets.len()))?;
        for (routing_bucket, node) in self.buckets {
            map.serialize_entry(
                routing_bucket,
                &BucketNodeAt {
                    node,
                    keys: self.keys,
                },
            )?;
        }
        map.end()
    }
}

/// One node, with the page-key context in scope.
///
/// ONE PRODUCTION DOOR NOW, AND THE CONTEXT IS NOT OPTIONAL. `impl Serialize for BucketNode`
/// stood beside this as the door for a fixture holding a bare node, passing `None` for the
/// context, and that `None` is the escape hatch this commit closes: a keyless node encode
/// composes every page key from the entry's own copy of the identity, which with the identity off
/// the entry is a key naming nothing, once per page, returned as `Ok`.
///
/// So the context arrives by value rather than as an `Option`, and the type system is what says a
/// node this engine writes cannot be encoded without one.
///
/// THE FIXTURE DOOR SURVIVES AS A `#[cfg(test)]` SCAFFOLD WITH A NAME THAT SAYS WHAT IT IS, and
/// it is NOT reachable from production -- `--lib` compiles with no `Serialize for BucketNode` at
/// all. Twenty of the twenty-six callers of the two fixture doors encode a bare node and six a
/// bare page map, every one of them a byte-shape or byte-budget assertion over a hand-built
/// fixture. They are named by [`PageIndexFromTheEntriesOwnCopies`] and they end when the field
/// does.
struct BucketNodeAt<'a> {
    node: &'a BucketNode,
    keys: &'a PageKeysByPosition<'a>,
}

impl Serialize for BucketNodeAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_bucket_node(
            self.node,
            &PageIndexAt { node: self.node, keys: self.keys },
            serializer,
        )
    }
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

    /// Slots read back from a stored index, IN THE POSITIONS THEY WERE WRITTEN IN.
    ///
    /// NOT through `insert`, and that is the whole change. `insert` hands out the first free slot,
    /// so re-filing a stored array through it reproduces the written ORDER and not the written
    /// POSITIONS -- which is how a slot came back naming a different object after a load, and why
    /// nothing was allowed to store one.
    ///
    /// A REPEATED ID IS PLACEHOLDERED WHERE IT REPEATS, not dropped: dropping it would shorten the
    /// array and move every id above it, which is the renumber this constructor exists to avoid.
    /// The first position wins, because it is the one anything already stored would be naming. The
    /// count is returned so the caller can report it rather than swallow it.
    fn from_stored_slots(mut slots: Vec<Option<u64>>) -> (Self, u64) {
        let mut repeated = 0u64;
        let mut seen: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for slot in slots.iter_mut() {
            if let Some(id) = *slot {
                if !seen.insert(id) {
                    *slot = None;
                    repeated += 1;
                }
            }
        }
        let mut held = ObjectSlots { valid: slots.iter().filter(|slot| slot.is_some()).count(), slots };
        // Trailing placeholders are not kept, exactly as a removal does not keep them: the
        // positions dropped hold no id, so nothing can be naming them.
        held.drop_trailing_placeholders();
        (held, repeated)
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

    /// An index read back from a stored one, IN THE POSITIONS IT WAS WRITTEN IN.
    ///
    /// Goes through the same two collapses `settle` applies, so a loaded index is in the shape a
    /// live one would be in and `slot_of` and `id_at` cannot tell a loaded bucket from a built one.
    /// That indistinguishability is what the reload test drives.
    ///
    /// A repeated id is reported rather than swallowed: it means the stored array disagreed with
    /// itself, which is a thing an operator should be able to see a count of.
    pub(super) fn from_stored_slots(slots: Vec<Option<u64>>) -> Self {
        let (held, repeated) = ObjectSlots::from_stored_slots(slots);
        if repeated > 0 {
            note_stored_slots_repeated_an_id(repeated);
        }
        // THE ARM IS CHOSEN BEFORE ANYTHING IS BOXED, and that is not a tidy-up.
        //
        // This built `Many(Box::new(held))` and then called `settle`, which allocated a box for
        // every bucket on every load and threw it away again for the two arms most buckets are
        // actually in. `a_bucket_holding_one_object_holds_no_node` and
        // `the_tombstone_index_gives_its_allocation_back_when_it_empties` both caught it: a shard
        // index is read once per bucket and twice counting the tombstone side, so that was an
        // allocation per bucket per load on a path that had none.
        //
        // Constructing `held` itself allocates nothing -- it moves the vector the deserializer
        // already built -- so the branch is free and the box is taken only by the arm that needs it.
        // The two collapses are `settle`'s, spelled here against the same two conditions.
        match (held.valid(), held.slot_count()) {
            (0, _) => ObjectIndex::Empty,
            (1, 1) => ObjectIndex::One(held.id_at(0).expect("one valid id in one slot")),
            _ => ObjectIndex::Many(Box::new(held)),
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

impl ObjectIndex {
    /// THE IDS ASCENDING, HOLES OMITTED -- the spelling this wrote before the slot array became
    /// durable, kept for the one side that does not need a durable slot.
    ///
    /// WHY IT STILL EXISTS. Nothing names a TOMBSTONE by slot: the deleted-object side is the ids a
    /// bucket has had removed and not yet reclaimed, read as a set and never indexed into. Writing
    /// it in slot order would move bytes already on disk for no gain at all, on the 2.32% of
    /// buckets that carry one.
    ///
    /// SO THE TWO SIDES NOW DIFFER DELIBERATELY, and that is why this is a named method rather than
    /// a copy. `DeletedObjectIndex` used to delegate to the live side's `Serialize` on the
    /// reasoning that two impls which agreed would be two impls to keep agreeing. They no longer
    /// agree -- one is positional and one is ascending -- so delegation would silently drag the
    /// tombstone side along. One body for each rule, and the caller says which rule it wants.
    ///
    /// Allocation-free on the two arms that hold nothing or one id, which is almost all of them.
    pub(super) fn serialize_ascending<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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

impl Serialize for ObjectIndex {
    /// THE SLOT ARRAY, IN SLOT ORDER, WITH ITS PLACEHOLDERS -- which is the change.
    ///
    /// This wrote `sorted_ids()`: ascending, holes omitted. That kept every byte where a sorted run
    /// had put it and took no format stamp, and it also meant NOTHING PERSISTED A SLOT. A load
    /// re-filed the written ids through `insert`, so positions were handed out afresh on every load
    /// and a slot was a resident fact. A page entry naming its object by slot cannot be built on
    /// that, and this is the step that makes the slot durable instead.
    ///
    /// WHAT MOVES ON DISK, AND IT IS TWO THINGS. The sequence is no longer ascending -- it is slot
    /// order, which is filing order -- and a hole is written as `null` where it used to be left out.
    /// Both change the bytes of every bucket holding more than one object, which the measurement
    /// puts at 46.6% of them, so this takes the next `SHARD_INDEX_FORMAT_VERSION`.
    ///
    /// AN OLDER INDEX STILL DECODES, AND LANDS WHERE IT WOULD HAVE -- AS DEFENCE IN DEPTH AND NOT
    /// AS THE UPGRADE PATH. An ascending sequence of bare ids deserializes as `Some` in the
    /// positions it is written in, and ascending positions are exactly what the old loader's
    /// `insert` would have handed out for it.
    ///
    /// THE UPGRADE PATH IS THE STAMP, THOUGH. `persistence.rs` compares the stored stamp with `<`
    /// and refuses anything below the constant, so an index written before this change is REFUSED
    /// and rebuilt from the log rather than decoded -- which is the established cost of a stamp
    /// here. The tolerance above matters on the one shape where a decode runs before the check, and
    /// it is worth having for exactly that reason, but it is not what an upgrade exercises.
    ///
    /// AND THE TWO COMMON ARMS STILL DO NOT ALLOCATE. A shard index is written on every dump, once
    /// per bucket and twice counting the tombstone side, and `Empty` and `One` are what most buckets
    /// are in. Neither builds a vector: the sort that used to stand here is gone, because slot order
    /// is now the order being written.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        match self {
            ObjectIndex::Empty => serializer.serialize_seq(Some(0))?.end(),
            ObjectIndex::One(id) => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                // `Some(id)` and not `id`: the element type of this sequence is now nullable, and
                // writing the one-object arm as a bare number would make the two arms write
                // different shapes for the same array.
                seq.serialize_element(&Some(*id))?;
                seq.end()
            }
            ObjectIndex::Many(slots) => {
                let mut seq = serializer.serialize_seq(Some(slots.slot_count()))?;
                for slot in slots.iter() {
                    seq.serialize_element(slot)?;
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
        // BY POSITION, NOT THROUGH `insert`. This read `Vec<u64>` and collected it, which put
        // each id in the first free slot and so reproduced the written ORDER rather than the
        // written POSITIONS. That is what made a slot resident-only.
        //
        // `Option<u64>` is what accepts both shapes: a `null` is the placeholder this now writes,
        // and a bare number from an index written before this change comes back as `Some` in the
        // position it was written in. So the tolerance is in the element type and needs no probing
        // of the stored form.
        let slots = Vec::<Option<u64>>::deserialize(deserializer)?;
        Ok(ObjectIndex::from_stored_slots(slots))
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
        // ASCENDING, and allocation-free on the arms that hold nothing or one id -- through
        // `ObjectIndex::serialize_ascending`, which is where that rule now lives. This field is
        // serialized, so writing it in slot order would move bytes already on disk for no gain, and
        // it holds a single id on 97.68% of the buckets that carry one at all.
        //
        // NO LONGER `index.serialize(..)`, AND THAT IS THE POINT. The live side's `Serialize` is
        // POSITIONAL now -- slot order, with a null for a placeholder -- because a page entry has to
        // be able to name its object by a slot that survives a load. Nothing names a tombstone by
        // slot, so delegating to it would have moved this field's bytes for nothing. The two rules
        // are two bodies with one caller each, rather than one body serving a claim that split.
        match &self.0 {
            None => {
                use serde::ser::SerializeSeq;
                serializer.serialize_seq(Some(0))?.end()
            }
            Some(index) => index.serialize_ascending(serializer),
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
#[serde(from = "BTreeMap<String, StoredPageEntry>")]
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

/// How many stored slots named an id another slot in the same array already named.
///
/// PRODUCTION AND NOT `cfg(test)`, unlike the probe counter below: a stored array disagreeing with
/// itself is a thing that happens to a real store and a zero here is a fact worth being able to
/// read. It is a count rather than a refusal because the load path's job is to come up.
static STORED_SLOTS_REPEATED_AN_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_stored_slots_repeated_an_id(count: u64) {
    STORED_SLOTS_REPEATED_AN_ID.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!(
        repeated = count,
        "a stored object list named the same object in more than one slot; the first slot wins and          the rest are left as placeholders so nothing above them renumbers"
    );
}

pub(super) fn stored_slots_repeated_an_id() -> u64 {
    STORED_SLOTS_REPEATED_AN_ID.load(std::sync::atomic::Ordering::Relaxed)
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

    pub(super) fn add_address(&mut self, address: &ElementEntry) {
        BLOCK_SLAB_LIVE_CHARGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tally = self.by_slab.entry(address.block_slab_id()).or_default();
        tally.block_refs = tally.block_refs.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(address.length());
    }

    pub(super) fn remove_address(&mut self, address: &ElementEntry) {
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
    pub(super) fn insert_with_handle(
        &mut self,
        handle: u64,
        page: BlockIndex,
        live: &mut BlockSlabLiveIndex,
    ) -> u64 {
        let address = page.address.clone();
        let (handle, displaced) = self.insert_unaccounted(handle, page);
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
    pub(super) fn insert_released_with_handle(&mut self, handle: u64, page: BlockIndex) -> u64 {
        self.insert_unaccounted(handle, page).0
    }

    /// TEST-ONLY DOOR, AND IT DELEGATES RATHER THAN DUPLICATING.
    ///
    /// The fixtures build a bare `BlockIndexMap` with no bucket around it, so they have nothing to
    /// compute a handle from the bucket level with. This keeps their signature and computes it the
    /// one way it is computed -- `block_index_handle` and then the same install -- so a fixture
    /// cannot exercise a handle derived differently from the one production writes. Two doors, one
    /// implementation; the other arrangement is the second path this change exists to delete.
    #[cfg(test)]
    pub(super) fn insert(
        &mut self,
        object_key: &str,
        page: BlockIndex,
        live: &mut BlockSlabLiveIndex,
    ) -> u64 {
        let handle = block_index_handle(object_key, &page);
        self.insert_with_handle(handle, page, live)
    }

    /// The other test-only door, delegating for the same reason.
    #[cfg(test)]
    pub(super) fn insert_released(&mut self, object_key: &str, page: BlockIndex) -> u64 {
        let handle = block_index_handle(object_key, &page);
        self.insert_released_with_handle(handle, page)
    }

    /// The mechanism, with no accounting: the handle it was GIVEN, and the address it displaced.
    ///
    /// THE HANDLE ARRIVES FROM ABOVE, and that is the whole of this change. It was
    /// `block_index_handle(&page.object_key, &page)` computed right here -- inside the map, from the entry and
    /// nothing else. A map cannot see the sibling fields of the bucket that owns it, so a term
    /// that leaves the entry for a per-bucket structure becomes unreachable from this line, and
    /// every such term was therefore pinned to the entry by this one call.
    ///
    /// Computing it one level up at [`BucketNode::insert_page`] changes WHERE, not WHAT: same
    /// inputs, same order, same hasher, byte-identical `u64`. That matters more than it sounds
    /// like, because handles are written to disk inside the lookup's refs -- two processes holding
    /// the same block must compute the same handle or those refs point at nothing, which is what a
    /// counter did silently until a reload lost an object.
    /// `the_hoisted_handle_is_byte_identical_to_the_one_the_map_used_to_compute` is the check.
    fn insert_unaccounted(&mut self, handle: u64, page: BlockIndex) -> (u64, Option<ElementEntry>) {
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
///
/// THE THIRD SITE WITH NO BUCKET IN SCOPE, AND THE ONE THE HOIST DOES NOT REACH.
///
/// The two mutation doors compute the handle at [`BucketNode::insert_page`], where the bucket's own
/// structures are available. This one cannot: it is how a `BlockIndexMap` arrives from serde, and a
/// `FromIterator` impl is handed pages and nothing else. The enclosing `BucketNode` is being
/// deserialized at the time, so its other fields may not have been read yet -- field order on the
/// wire decides, and the stored form is a map whose keys arrive in whatever order the encoder wrote.
///
/// So a term moved off the entry into a per-bucket structure is not reachable from THIS seam. The
/// handle is therefore still read off the entry on this line: same inputs, same order, same hasher,
/// byte-identical value.
///
/// BUT THIS IS NOT THE SEAM THE LOAD PATH HAS TO USE, AND THAT IS THE IMPORTANT HALF.
/// `BucketNode`'s own visitor accumulates every field as a SEPARATE LOCAL and builds
/// `Ok(BucketNode { .. })` at the end, so `object_index` and the page index are both in scope at
/// that closing construction, order-independently -- which is what a serde map visitor needs, since
/// it cannot assume one key arrives before another.
///
/// WHAT IT COSTS IS A CHANGE OF ACCUMULATION TYPE, not a change of construction site, and the
/// difference is worth stating because the same seam has already been used for a `u32` where moving
/// the construction was enough. A `u32` is STAMPED ONTO entries that already exist. A handle IS THE
/// MAP'S KEY: by the time this impl has run, every entry is already filed under one, and there is
/// nothing left to stamp. So the visitor would hold the STORED form -- a map of rendered key to
/// entry -- and build the page index at the closing construction, where the sibling it needs is in
/// hand. This impl then stays as the standalone door, for deserializing a bare map, which is the
/// same two-doors-one-body shape the install and the written key already have.
/// COLLECTED FROM (IDENTITY, PAGE) PAIRS, NOT FROM PAGES.
///
/// The handle is hashed from the identity and the page, and the identity is leaving the page --
/// so the pair is what this can be built from once it has. Every producer of these pairs has the
/// key in scope already: the decode has it in the stored map key it is iterating, and the
/// fixtures have it in the page they just built.
impl FromIterator<(Arc<str>, BlockIndex)> for BlockIndexMap {
    fn from_iter<I: IntoIterator<Item = (Arc<str>, BlockIndex)>>(pages: I) -> Self {
        // Unaccounted: this is how a ShardState arrives from serde, and the tally is derived
        // AFTER a load by `reconcile_block_slab_live`. Charging blocks here would count a loaded
        // index twice over -- once on the way in, once when the load seeds the tally.
        let mut map = Self::default();
        for (object_key, page) in pages {
            let handle = block_index_handle(&object_key, &page);
            map.insert_unaccounted(handle, page);
        }
        map
    }
}

/// THE PAGE INDEX A STORED BUCKET DECODES TO, AND BESIDE IT WHAT EACH PAGE'S IDENTITY IS.
///
/// The map is keyed by a HANDLE, and the handle is hashed from the identity and the page -- so a
/// decode that cannot see an identity cannot even assign the key it files a page under. The
/// identity is read here from the entry, which still carries one; what matters is that it is read
/// ONCE, in one place, and handed both to the handle and to the caller.
///
/// WHY THE CALLER NEEDS IT. `object_block_lookup` is keyed by (kind, object key) and valued by the
/// `(routing_bucket, handle)` of each of that object's pages. It is `skip_serializing` and was
/// rebuilt after every load by walking the decoded entries and reading the identity off each --
/// which is a walk that stops being possible the moment the entry has none. Handed up from here
/// it is filed as the index decodes, from the only pass that ever has both the identity and the
/// handle in scope.
///
/// Built through the same install as a live write, so a loaded index takes the shape a written one
/// does: a bucket that loads a single block must not come back holding a list.
///
/// UNACCOUNTED, as it always was: this is how a `ShardState` arrives from serde and the live tally
/// is derived AFTER a load by `reconcile_block_slab_live`. Charging here would count a loaded index
/// twice over.
fn page_index_and_identities(
    flat: BTreeMap<String, StoredPageEntry>,
) -> (BlockIndexMap, Vec<(u64, Arc<str>)>) {
    let mut map = BlockIndexMap::default();
    let mut identities = Vec::with_capacity(flat.len());
    for stored in flat.into_values() {
        // READ ONCE, FROM THE ONE PLACE THAT STILL HAS IT, and handed both to the handle and to
        // the caller. The key is MOVED out of the stored entry rather than cloned off it.
        let (object_key, page) = stored.into_parts();
        let handle = block_index_handle(&object_key, &page);
        map.insert_unaccounted(handle, page);
        identities.push((handle, object_key));
    }
    (map, identities)
}

/// THE FIXTURE DOOR onto the same builder, for the bare maps that decode with no node around them.
///
/// It discards the identities because it has nobody to hand them to. One implementation and two
/// doors; two builders that agree today is the hazard this file keeps a record of.
impl From<BTreeMap<String, StoredPageEntry>> for BlockIndexMap {
    fn from(flat: BTreeMap<String, StoredPageEntry>) -> Self {
        page_index_and_identities(flat).0
    }
}

/// THE PAGE INDEX, SERIALIZED AT THE BUCKET LEVEL.
///
/// Holds the whole node rather than just the map, which is the entire reason it exists:
/// `impl Serialize for BlockIndexMap` could only ever see the map, and serde's
/// `serialize(&self)` cannot be handed anything else -- so the written key was pinned to terms
/// living on the entry exactly as the handle was. Composed from the node, a key can draw on
/// anything the bucket owns.
///
/// THE MAP'S OWN `Serialize` IS DELETED RATHER THAN LEFT, and that is not tidying. It had one
/// caller; a second path that agrees with this one today is a second path to keep agreeing, and
/// this tree records what two copies that agree cost when one of them later does not.
///
/// It writes what it always wrote. Hand-written rather than `#[serde(into = ...)]`, which is
/// defined as `T::from(self.clone()).serialize(..)` and duplicates the whole index twice over
/// before a byte is written. And the sort is not incidental: the map this used to convert into was
/// keyed by the rendered string and emitted entries in string order, while this map is keyed by a
/// handle and iterates in hash order, so writing them as they come would reorder every dump.
struct PageIndexAt<'a> {
    node: &'a BucketNode,
    keys: &'a PageKeysByPosition<'a>,
}

impl Serialize for PageIndexAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_page_index(self.node, self.keys, serializer)
    }
}

/// THE ENTRY AS IT REACHES THE WIRE, WITH ITS IDENTITY INJECTED FROM BESIDE IT.
///
/// `BlockIndex` no longer derives `Serialize`, for one reason: `object_key` is a SERIALIZED MEMBER
/// and a derive can only read it off the entry. This writes the same five keys, in the same order,
/// with the same names -- `object_key`, `model_id`, `address`, `dirty`, `deleted` -- taking the
/// first from the caller and the rest from the page.
///
/// THE KEY SET IS OBSERVED, NOT READ OFF THE ATTRIBUTES.
/// `page_entry_names::capture_the_stored_spelling_of_a_page_entry` prints a real encode of an
/// entry and of a whole index; the five keys above are what it printed before this type existed
/// and what it prints now.
///
/// THE FIELD COUNT IS PART OF THE ENCODING. The served index is msgpack `with_struct_map`, so
/// `serialize_struct` writes a map header of exactly this length -- the same reason `BucketNode`
/// counts its own optional key rather than hard-coding one.
pub(super) struct PageEntryAt<'a> {
    pub(super) page: &'a BlockIndex,
    pub(super) object_key: &'a str,
}

/// THE ENTRY AS IT ARRIVES FROM THE WIRE, WITH THE IDENTITY STILL BESIDE IT.
///
/// The counterpart of [`PageEntryAt`], and it exists for a sharper reason than symmetry. A derived
/// `Deserialize` on `BlockIndex` keeps COMPILING once `object_key` is not a field: serde's derive
/// ignores a key it has no field for unless `deny_unknown_fields` is set. So the removal would
/// have taken the identity off the load path without a single error, and `object_block_lookup` --
/// `skip_serializing`, rebuilt from what the decode reads -- would have come back empty on every
/// load. That is the exact state the encoder refuses to write.
///
/// THE FIVE FIELDS ARE THE DERIVE'S, UNCHANGED AND UNATTRIBUTED. `BlockIndex` carried no
/// `#[serde(default)]` and no `rename`, so all five keys are required and spelled as declared;
/// adding an attribute here would be a stored-format change wearing a refactor's clothes.
/// `page_entry_names::an_index_written_before_this_change_loads_page_for_page` is what drives it.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct StoredPageEntry {
    pub(super) object_key: Arc<str>,
    pub(super) model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    pub(super) address: ElementEntry,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
}

impl StoredPageEntry {
    /// The identity, and the entry that no longer holds it.
    ///
    /// Taken apart by VALUE so the shared pointer moves rather than being cloned: every caller
    /// keeps the key -- the filing map and the identities handed up to the decode both do -- so
    /// this has to go on costing one pointer and not a copy of the characters.
    pub(super) fn into_parts(self) -> (Arc<str>, BlockIndex) {
        let StoredPageEntry {
            object_key,
            model_id,
            address,
            dirty,
            deleted,
        } = self;
        let page = BlockIndex {
            // AND THE IDENTITY IS NOT ASSIGNED TO IT, WHICH IS WHY THE RETURN IS A PAIR. The
            // pointer is not cloned either: it is MOVED out as the first element, so the load
            // path is handed the key beside the page at the cost of nothing.
            model_id,
            address,
            dirty,
            deleted,
        };
        (object_key, page)
    }
}

impl Serialize for PageEntryAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // NAMED `BlockIndex`, which is what the derive named it. A `serialize_struct` name reaches
        // some formats and not others, and this one must not move either.
        let mut entry = serializer.serialize_struct("BlockIndex", 5)?;
        // AN `&str` WHERE THE DERIVE WROTE AN `Arc<str>`, which is the same bytes: serde's impl
        // for `Arc<T>` forwards to `T`, and `str` writes a string.
        entry.serialize_field("object_key", self.object_key)?;
        entry.serialize_field("model_id", &self.page.model_id)?;
        entry.serialize_field("address", &self.page.address)?;
        entry.serialize_field("dirty", &self.page.dirty)?;
        entry.serialize_field("deleted", &self.page.deleted)?;
        entry.end()
    }
}

/// EVERY PAGE THIS BUCKET WRITES, NAMED BY THE IDENTITY HELD BESIDE IT.
///
/// # THE SOURCE IS BESIDE THE PAGE NOW, AND A MISSING ONE IS REFUSED
///
/// This was a CHECK (`check_page_keys_against_their_siblings`) walking the bucket a second time
/// and comparing the key the entry's own copy composes against the key the neighbour composes. It
/// is the SOURCE now: the written key below is composed from the neighbour, and the entry's copy
/// is only still compared because it still exists. One walk rather than two, because a check that
/// has become a source cannot usefully be a separate pass over the same pages.
///
/// A PAGE WITH NO IDENTITY BESIDE IT IS A REFUSAL, NOT A SKIP, AND THAT IS A BEHAVIOUR CHANGE ON
/// THE DURABILITY PATH. The skip that stood here was excused by `names_no_position()` -- the
/// decode-then-rebuild window, where a page legitimately has nothing beside it. That hatch excused
/// two states and not one: the legitimate window, and an encoder handed a lookup that does not
/// cover the bucket it is writing. With the identity off the entry the second writes a key naming
/// NOTHING, silently, once per page, and returns `Ok`. A dump therefore goes from "succeeds,
/// having written a key naming nothing" to "refuses", and `names_no_position()` is what the
/// refusal says to tell the two apart rather than what excuses either.
///
/// UNDER A PLANTED EMPTY INVERSION ALL 29 TOMBSTONE TESTS STILL PASSED, which is what says the
/// hatch was load-bearing in the wrong direction: nothing in the suite could tell an encode with
/// no source from an encode with one.
///
/// # WHAT MAKES THE REFUSAL UNREACHABLE: THE FILING SITES, NOT AN ARGUMENT
///
/// The lookup is `skip_serializing` and is filed as pages are placed, at every site that places
/// one. `insert_object_block_lookup` has ELEVEN production callers -- `state.rs` at the decode
/// (`CoreIndex`'s visitor, from the `page_keys` the decode hands up), at
/// `CoreIndex::insert_page_for_bucket` and at two rebuild sites; `storage_bucket_internals.rs` at
/// six (the publish establish, the batch fold, the two reconstructs, the whole-shard rebuild and
/// the bucket-first rebuild); and `engine.rs` at one. Between them and the hand-written
/// `impl Default for CoreIndex` -- which starts the ref total at `Some(0)` rather than `None`, so
/// a count can be incremented into a right answer -- there is no site at which a page is placed
/// and not filed.
///
/// MEASURED RATHER THAN ARGUED: 630 encodes with pages, ZERO unnamed pages and ZERO empty
/// inversions, with the instrument proved able to fire -- a planted empty inversion turned twelve
/// encodes into twelve and eighty-four.
///
/// # THE CHECK THIS REPLACES COMPARED NO DELETED PAGE, AND IT WAS MEASURED DOING SO
///
/// It lived inside the encoder, drew only on the object lookup, and `continue`d whenever the
/// lookup did not name a page. A tombstone is NEVER in that lookup --
/// `insert_object_block_lookup` returns early on a deleted page -- so the skip was unconditional
/// for the whole deleted class and the comparison could not reach it.
///
/// COUNTED RATHER THAN REASONED. Instrumented over the hundred dump tests, the old check compared
/// 1,998,922 pages and 0 of them were deleted, while the encoder was reached by deleted pages 7
/// times under `tombstone` and 3 under `container_tombstone`. The denominator on the class the
/// swap depends on was zero, so zero disagreements said nothing about it.
///
/// # SO THERE ARE TWO SOURCES BESIDE A PAGE AND NOT ONE
///
/// A LIVE page has its identity in the object lookup, inverted by position. A DELETED page has
/// its own in this bucket's tombstone rows, which are keyed by the page triple and carry the
/// object key and the kind -- the two terms the stored spelling is composed from. Measured over
/// `tombstone`, `container_tombstone` and `reload`: 245 deleted pages reached the encoder and all
/// 245 had an agreeing row, with no disagreement and no row missing.
///
/// # AND A MISSING SOURCE IS NOW REFUSED UNLESS THE LOOKUP IS UNESTABLISHED
///
/// That window is real -- an index decoded and not yet rebuilt has an empty lookup, which is
/// where the two live pages measured without a source both came from -- and it is the one state
/// in which a page legitimately has nothing beside it. Naming it is what stops the skip from
/// covering the case the identity's move would break.
/// THE ONE IMPLEMENTATION, AND THERE IS ONE DOOR ONTO IT.
///
/// It took a bare `&BlockIndexMap` and took the identity off each entry, which is why a second
/// door -- `#[cfg(test)] impl Serialize for BlockIndexMap` -- could exist at all. It takes the
/// NODE now, because that is what the two identity sources live beside: the object lookup inverted
/// by position for a live page, and this bucket's own tombstone rows for a deleted one. The
/// test-only door is gone with it, and it had NO CALLERS either.
///
/// THE SORT IS NOT INCIDENTAL. The map this used to convert into was keyed by the rendered string
/// and emitted entries in string order, while this map is keyed by a handle and iterates in hash
/// order, so writing them as they come would reorder every dump.
fn serialize_page_index<'k, S>(
    node: &'k BucketNode,
    keys: &PageKeysByPosition<'k>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::{Error as _, SerializeMap};
    let mut entries: Vec<(String, PageEntryAt<'_>)> =
        Vec::with_capacity(node.block_index.len());
    for (handle, page) in node.block_index.iter() {
        // THROUGH THE ONE DOOR, so the encoder and the readers cannot disagree about where an
        // identity comes from.
        let Some(beside) = keys.identity_of(node, *handle, page) else {
            return Err(S::Error::custom(format!(
                "bucket {} holds a page whose stored key would name no object, so this index is \
                 not writable: deleted={}, and the inversion {}",
                node.routing_bucket,
                page.deleted,
                if keys.names_no_position() {
                    "names NO position at all, so this is the decode-then-rebuild window and the \
                     encode was taken inside it"
                } else {
                    "DOES name positions, so the encoder was handed a lookup that does not cover \
                     this bucket"
                },
            )));
        };
        // THE TIE BACK TO THE ENTRY'S OWN COPY IS GONE WITH THE COPY, AND WHAT IT ESTABLISHED
        // IS WHY THIS ENCODER IS TRUSTED.
        //
        // It asserted, character for character, that the key composed from the NEIGHBOUR is the
        // key the entry's own copy composed -- on every page of every run of this suite while
        // both sources existed. That is the whole of the evidence that moving the identity off
        // the entry did not move the stored bytes, and it is unrepeatable now: there is one
        // source, so anything written here would compare a renderer with itself.
        //
        // IT IS DELETED RATHER THAN POINTED AT THE SURVIVING SOURCE, and the mutation is tempting
        // and wrong: the left side would become `block_index_written_key(beside.object_key, page)`
        // while the right side already renders `beside.object_key`, so the result is a comparison
        // of two RENDERERS of one input wearing the name of a comparison of two SOURCES. A reader
        // would inherit the stronger claim from the prose.
        //
        // WHERE THE CLAIM LIVES NOW, and it is a golden rather than a tie:
        // `page_entry_names::the_stored_spelling_of_a_page_entry_moved_in_exactly_one_slot` pins
        // the four written forms byte for byte, and
        // `an_index_written_before_this_change_loads_page_for_page` pins that a row written by a
        // binary that still had the field decodes.
        entries.push((
            block_index_written_key(beside.object_key, page),
            PageEntryAt { page, object_key: beside.object_key },
        ));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    let mut serialized = serializer.serialize_map(Some(entries.len()))?;
    for (key, entry) in &entries {
        serialized.serialize_entry(key, entry)?;
    }
    serialized.end()
}

/// THE PAGE INDEX KEYED FROM THE ENTRIES' OWN COPIES OF THE IDENTITY -- A SCAFFOLD, AND TESTS ONLY.
///
/// # WHY A SECOND IMPLEMENTATION EXISTS AT ALL, WHICH THIS FILE OTHERWISE REFUSES
///
/// Twenty-six test arms encode a bare `BucketNode` or a bare `BlockIndexMap` and assert something
/// about the BYTES: that two fillings of the same pages agree, that a flag state survives the
/// wire, that a budget is what it was. None of them has an object lookup, because none of them has
/// a `CoreIndex`; the identity they can name is the one on the entry.
///
/// It is `#[cfg(test)]` and it is named rather than reached through an `Option`, which is the
/// difference that matters: the keyless case used to be a `None` any caller could pass, and is now
/// a type production cannot construct. `--lib` compiles with no `Serialize for BucketNode` and no
/// `Serialize for BlockIndexMap` in it at all.
///
/// # IT STOPPED READING THE ENTRY, AND THE FIXTURE NAMES THE OBJECTS INSTEAD
///
/// It was `PageIndexFromTheEntriesOwnCopies`, and it keyed each page from the entry's own copy of
/// the identity -- which is the copy that is going. A bare node has no object lookup to invert,
/// so the only remaining source for a page's object is the FIXTURE, and the fixture is handed in.
///
/// WHAT IS HANDED IN IS THE OBJECT KEYS, NOT A HANDLE MAP, and the match is the map's own. A page
/// is filed under `block_index_handle(object_key, page)`, so for each page exactly one of the
/// named objects reproduces the handle it is filed under. The caller writes the string literals
/// its own fixture wrote -- usually one -- and this recomputes the pairing rather than being told
/// it. A wrong literal does not quietly key a page under the wrong name: it matches nothing.
///
/// A PAGE NO NAMED OBJECT ACCOUNTS FOR IS A PANIC, and so is a list with an object that accounts
/// for no page. The first is the hole this door exists to close -- the keyless encode that wrote
/// a key naming nothing, once per page, and returned `Ok`. The second is a stale list, which is
/// how this kind of hand-written argument rots: a fixture that stops writing an object and leaves
/// its name here would otherwise go on passing.
#[cfg(test)]
pub(super) struct PageIndexNamedBy<'a>(pub(super) &'a BlockIndexMap, pub(super) &'a [&'a str]);

#[cfg(test)]
impl PageIndexNamedBy<'_> {
    /// The object each page is filed under, matched by the handle the map keyed it with.
    fn pairs(&self) -> Vec<(&str, &BlockIndex)> {
        let mut used = vec![false; self.1.len()];
        let paired: Vec<(&str, &BlockIndex)> = self
            .0
            .iter()
            .map(|(handle, page)| {
                let at = self
                    .1
                    .iter()
                    .position(|object| block_index_handle(object, page) == *handle)
                    .unwrap_or_else(|| {
                        panic!(
                            "no object named by this fixture is filed at handle {handle}; the                              named objects are {:?}",
                            self.1
                        )
                    });
                used[at] = true;
                (self.1[at], page)
            })
            .collect();
        if let Some(at) = used.iter().position(|hit| !hit) {
            panic!(
                "this fixture names the object {:?}, which accounts for none of the {} page(s)                  in the map; a name left behind by a fixture that stopped writing it would go on                  passing",
                self.1[at],
                paired.len()
            );
        }
        paired
    }
}

#[cfg(test)]
impl Serialize for PageIndexNamedBy<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut entries: Vec<(String, PageEntryAt<'_>)> = self
            .pairs()
            .into_iter()
            .map(|(object_key, page)| {
                (
                    block_index_written_key(object_key, page),
                    PageEntryAt { page, object_key },
                )
            })
            .collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        let mut serialized = serializer.serialize_map(Some(entries.len()))?;
        for (key, entry) in &entries {
            serialized.serialize_entry(key, entry)?;
        }
        serialized.end()
    }
}

/// TEST-ONLY DOOR for the arms that write a bare NODE, with the objects its fixture wrote.
#[cfg(test)]
pub(super) struct NodeNamedBy<'a>(pub(super) &'a BucketNode, pub(super) &'a [&'a str]);

#[cfg(test)]
impl Serialize for NodeNamedBy<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_bucket_node(
            self.0,
            &PageIndexNamedBy(&self.0.block_index, self.1),
            serializer,
        )
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
pub(super) fn block_index_handle(object_key: &str, page: &BlockIndex) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    page.model_id.hash(&mut hasher);
    // THE IDENTITY ARRIVES AS A PARAMETER. It was read off the entry, which is the one place it
    // is about to stop being. Same bytes into the same hasher in the same order, so the handle
    // is unchanged -- what moves is WHERE the caller gets the key, and every caller has one.
    object_key.hash(&mut hasher);
    // STILL HASHED, AND SEVERAL PRODUCERS NOW FILE IT AS `None`.
    //
    // RESTATED FROM "ONE PRODUCER", which was the set arm alone. Every kind
    // `storage_bucket_internals::index_entry_names_a_page` answers true for -- `set` and `list`
    // today -- emits entries with no component, so for those this term contributes the hash of
    // `None` and the handle is decided by the object key and the address alone.
    //
    // That is safe only because all of them go through ONE emitter,
    // `visit_model_live_blocks::emit_one_entry_a_page`, which emits at most one entry per distinct
    // physical page: two of its entries cannot share an address and therefore cannot share a
    // handle. It is one function rather than one block per arm precisely so that this property is
    // held in a single place -- there is a note at the dedup saying what it is holding. A collision
    // here returns a DIFFERENT page rather than failing, which is why the property is written at
    // both ends rather than inferred at either.
    // THE NAME TERM IS HASHED AS THE CONSTANT IT ALREADY WAS, NOT REMOVED.
    //
    // This hashed `page.component.as_deref()`, and every entry's component was `None` before the
    // field was removed -- so the term contributed one fixed discriminant to every handle and
    // distinguished nothing. It is spelled `None::<&str>` rather than deleted because deleting it
    // MOVES EVERY HANDLE VALUE, and the identity this computes is the key `BlockIndexMap` files an
    // entry under. Handles are re-assigned on every load, so moving them is survivable -- but it
    // is a change to an identity, with its own blast radius, and it is not what a width step is
    // for. Kept constant, the handles this produces are bit-for-bit the ones it produced before.
    None::<&str>.hash(&mut hasher);
    page.address.block_slab_id().hash(&mut hasher);
    page.address.offset().hash(&mut hasher);
    page.address.length().hash(&mut hasher);
    page.address.block_id().unwrap_or_default().hash(&mut hasher);
    page.address.generation().unwrap_or_default().hash(&mut hasher);
    hasher.finish()
}

pub(super) fn block_index_written_key(object_key: &str, page: &BlockIndex) -> String {
    crate::index_log::block_ref_key_from_parts(
        page.model_id.as_str(),
        object_key,
        // `None`, WHICH IS WHAT THIS ALREADY PASSED. The STORED spelling of a page's key is built
        // here, so the value must not move: every entry's component was `None` before the field was
        // removed, and passing the constant keeps every written key character-for-character what it
        // was. That is why removing the field costs no `SHARD_INDEX_FORMAT_VERSION` step -- the
        // slot it filled was already absent from the bytes.
        None,
        page.address.block_slab_id(),
        page.address.offset(),
        page.address.length(),
        page.address.block_id().unwrap_or_default(),
        page.address.generation().unwrap_or_default(),
    )
}
/// One entry per OBJECT, holding that object's block refs directly.
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
///
/// # THE THREE WRAPPERS THAT USED TO SIT BETWEEN THE OBJECT AND ITS REFS
///
/// The value was `ObjectBlockRefs { by_component: ComponentList }`, where `ComponentList` was
/// `Empty | One(ComponentBlocks) | Many(Vec<ComponentBlocks>)` and `ComponentBlocks` was
/// `{ component: Option<Arc<str>>, refs: BlockRefs }`. Every one of them existed to map a
/// CALLER-SUPPLIED ELEMENT NAME onto blocks.
///
/// # THE WRAPPERS WERE FREE IN BYTES AND THE NAME WAS NOT -- 16 PER OBJECT, NOT 96
///
/// WRITTEN DOWN BECAUSE THE PLAN THIS WAS BUILT FROM HAD IT WRONG, and so did the first commit
/// message of the change: "120 bytes of nesting before you reach the 24 that holds anything". That
/// adds up four DECLARATION widths for types that occupy THE SAME BYTES. `ObjectBlockRefs` had one
/// field, `ComponentList::One` held its `ComponentBlocks` INLINE, and `ComponentBlocks` was 16 of
/// name plus 24 of refs with no padding -- so all four were 40 bytes because they WERE the same 40
/// bytes. Each of the base's own const assertions said 40, 40, 40 and 24, and reading them as a sum
/// is the field-sum-against-`size_of` mistake in its most inviting form.
///
/// So the per-object cost was 40 and is now 24. The saving is the 16-byte `Option<Arc<str>>` that
/// held a constant `None`, and the three wrappers around it cost nothing -- which is exactly what
/// `bucket_node_arms::the_component_level_is_not_a_list_of_one` measured and said in as many words,
/// "there are no bytes here to reclaim". That arm was right about the wrapper and looking one level
/// too high: the bytes were in what the wrapper wrapped.
///
/// There is no name left to map. `insert_object_block_lookup` was the only site in the crate that
/// ever built a `ComponentBlocks`, and it wrote `component: None` as a literal because the page
/// entry it files from has no element name to give it. So the list held exactly one entry, that
/// entry's name was a constant, and `ObjectBlockRefs` was a struct with one field. All three are
/// deleted and an object now maps straight to its `BlockRefs`.
///
/// WHAT EACH ONE TURNED OUT TO CARRY, since "it is only nesting" is a claim and not an
/// observation:
///
///   * `ObjectBlockRefs` carried `position`, `refs_for`, `all_refs` and `total_refs`. Every one
///     was a forwarding method over the list; `all_refs` and `total_refs` ARE `BlockRefs::iter`
///     and `BlockRefs::len` once the list is gone.
///   * `ComponentList` carried the most: `iter`, `binary_search_by`, `insert`, `remove`, `Index`,
///     `IndexMut`, `IntoIterator` and a hand-written `Serialize`/`Deserialize` pair. All of it is
///     machinery for holding SEVERAL components -- the inline/spilled trade so a list of one costs
///     no allocation, the demotion on removal so an object that briefly held two does not keep the
///     vector, and the binary search the removals used to find a named slot. At one slot whose
///     name is a constant, each of those is machinery for nothing. The one piece worth keeping is
///     the inline/spilled trade itself, and `BlockRefs` already has it for the refs, which is the
///     level where it now applies.
///   * `ComponentBlocks` carried the name field and nothing else.
///
/// THE EMPTY SLOT STOPPED BEING REPRESENTABLE, AND THAT IS A SIMPLIFICATION AND NOT A LOSS. The
/// list could hold a slot with no refs -- `BlockRefs::remove` leaves `Many(vec![])` behind -- and
/// that state was transient by contract with one holder. The object's absence from this map is now
/// the only way to express "no refs", so the contract has nowhere left to be broken.
///
/// # NO FORMAT STAMP IS OWED, AND IT IS CHECKED RATHER THAN ASSUMED
///
/// The field holding this is `#[serde(default, skip_serializing)]` on `CoreIndex` and is rebuilt
/// by a full bucket walk on load (`persistence`'s two `rebuild_object_block_lookup` calls). An
/// index stamped with the current `SHARD_INDEX_FORMAT_VERSION` cannot carry it, because the writer
/// skips it; an index stamped lower is refused by `persistence`'s `<` comparison. So the wire
/// shape below has no reader a stamp could protect. The one observable difference for a
/// pre-version index that DOES carry the old shape is which counter it lands in --
/// `IndexLoadPath::Undecodable` rather than `RefusedStaleStamp` -- and both return `Ok(None)` and
/// fall back to WAL replay.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    from = "BTreeMap<String, BlockRefs>",
    into = "BTreeMap<String, BlockRefs>"
)]
pub(super) struct ObjectBlockLookup {
    by_model: BTreeMap<Arc<str>, BTreeMap<Arc<str>, BlockRefs>>,
}

/// WHY THIS INDEX IS ADDRESSED BY THE OBJECT KEY AND NOT BY AN INTEGER.
///
/// One `u64` derived from the object's identity, in place of the `Arc<str>` above, is the obvious
/// next width step here. It is REFUSED, on three independent grounds, and the widths below are the
/// first of them -- pinned, so the arithmetic cannot go stale the way a number quoted in prose
/// does.
///
///   1. THE SAVING IS THE KEY AND NOTHING ELSE. An entry is 40 bytes of field today. Addressed by
///      a `u64` with the key DROPPED it is 32 -- eight bytes back. Addressed by a `u64` with the
///      key RETAINED it is 48, which is eight bytes WORSE than the shape it would replace. So the
///      route pays in exactly one arm, the arm that stops holding the object key, and every
///      variant that keeps the key is a regression before a single tree node is counted.
///   2. THE KEY IS NOT A WORKING SET. IT IS THE SOURCE OF A WRITTEN FIELD. `impl Serialize for
///      CoreIndex` builds [`PageKeysByPosition::from_lookup`] IN EVERY BUILD and composes each
///      page's written key from it -- its own note says a release build without the inversion
///      "would write no key at all" -- because the page entry carries no identity of its own.
///      Readers beside the encoder reach the same inversion through `page_identities`,
///      `identity_of`, `live_identity` and `page_identities_in`, and they were counted by the
///      COMPILER rather than by a grep over a method name: taking the object key off that inversion is twenty-four compiler diagnostics across ten files, TWENTY of them in production code and four in tests. The governing
///      rule is spelled out at `index_log.rs::strip_block_ref_key_repeat`: for a field omitted
///      because it is derivable, find WHO READS THE RECONSTRUCTION first. Here this allocation IS
///      the reconstruction, so dropping it writes an index whose pages name no object.
///   3. THE DERIVATION AVAILABLE IS MERGEABLE BY ONE BYTE. `hashing::stable_object_hash` is
///      FNV-1a-64, `h <- (h ^ b) * P` with `P` odd, so two running states take the same value on
///      the next byte if and only if they differ in their low eight bits ALONE. A chosen-key
///      collision therefore needs a 56-bit agreement rather than a 64-bit one: about 2^28
///      candidates, not 2^32. A collision returns ANOTHER OBJECT'S refs with no error anywhere,
///      and the only step that catches one is a comparison against the stored key -- which is
///      ground 1 again. Object keys arrive from the caller, so there is no funnel to hold a
///      uniqueness property the way `block_index_handle`'s single emitter holds its own.
///      `an_integer_addressed_lookup_serves_another_objects_refs` drives both halves.
const _: () = assert!(std::mem::size_of::<(Arc<str>, BlockRefs)>() == 40);
/// Pinned on its own, because a width pin admits every alignment that divides it.
const _: () = assert!(std::mem::align_of::<(Arc<str>, BlockRefs)>() == 8);
/// The integer-addressed arm that pays, and the only one: the object key is GONE from it, which is
/// what ground 2 forbids.
const _: () = assert!(std::mem::size_of::<(u64, BlockRefs)>() == 32);
/// The integer-addressed arm that keeps the key, and so keeps what the encoder reads. Wider than
/// the shape it would replace, which is the whole of ground 1.
const _: () = assert!(std::mem::size_of::<(u64, (Arc<str>, BlockRefs))>() == 48);
/// AND THE TWO LEVELS ARE NOT THE COST EITHER. Folding the model spelling into one composite key
/// -- the shape a single-level map would need -- costs the same eight bytes an integer address
/// would have saved. The outer level is one entry per stored kind for the whole shard, which
/// [`ObjectBlockLookup::model_count`] reports and the footprint probe prints beside the object
/// count, so that level is a constant head and not a per-object tree.
const _: () = assert!(std::mem::size_of::<((u8, Arc<str>), BlockRefs)>() == 48);

/// Whether this component argument names THE ONLY SLOT THERE IS.
///
/// # THIS IS THE ONE PIECE OF THE DELETED LEVELS THAT HAD TO STAY
///
/// The component argument on this lookup's readers and removers is NOT always `None`, and
/// believing otherwise is the way to get this collapse wrong. Two production sites reach it with a
/// real `Some`: `bucket_store::bucket_index_block_address`, which passes a caller's component
/// straight through from the slow read path, and [`CoreIndex::contains_object_block_address`],
/// which `storage_bucket_internals`'s hash-field sync asks `Some(field)` once per field.
///
/// What made the levels dead is the other half of the pair: the one slot was keyed `None`, so a
/// binary search for `Some(x)` always ran off the front and answered nothing. This is that search
/// with the entry's constant folded in -- `None.cmp(&Some(x))` is `Less` for every `x`, so the
/// search returned `Err` and every caller took its miss branch.
///
/// Spelled once, because the consequence of the three callers drifting apart on it is not a bigger
/// index: a `Some` that started hitting would serve an object's whole page as the answer to a
/// field lookup that has always, correctly, found nothing.
fn names_the_only_slot(component: Option<&str>) -> bool {
    component.is_none()
}

impl ObjectBlockLookup {
    pub(super) fn get(&self, model_id: &str, object_key: &str) -> Option<&BlockRefs> {
        self.by_model.get(model_id)?.get(object_key)
    }

    pub(super) fn get_mut(
        &mut self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&mut BlockRefs> {
        self.by_model.get_mut(model_id)?.get_mut(object_key)
    }

    /// File one ref under this object, opening the object's entry if it is absent. Reports whether
    /// anything was added, which is what the ref counter is kept from.
    ///
    /// Takes both keys by shared pointer. The object key is the one the block entry already holds,
    /// so filing a block adds a pointer rather than a second copy of its identity.
    ///
    /// REPLACES `entry()`, WHICH CANNOT EXIST ANY MORE AND IS BETTER FOR IT. That method handed
    /// back an empty `ObjectBlockRefs` for the caller to fill, so the "object present, nothing
    /// filed" state had to be representable. `BlockRefs` has no empty arm to default to, so the
    /// first ref is built INTO the entry that holds it and the intermediate state is gone.
    pub(super) fn file(
        &mut self,
        model_id: &Arc<str>,
        object_key: &Arc<str>,
        value: BlockLookupRef,
    ) -> bool {
        let objects = self.by_model.entry(Arc::clone(model_id)).or_default();
        match objects.get_mut(object_key.as_ref()) {
            Some(refs) => refs.insert(value),
            None => {
                // The object's first block. Built already holding it, so the common case never
                // allocates and there is no empty state in between.
                objects.insert(Arc::clone(object_key), BlockRefs::One(value));
                true
            }
        }
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

    /// The allocation this map holds for `object_key` under ANY kind.
    ///
    /// `DirtyObjectIndex` is keyed by object alone, so it cannot name the kind to look under. The
    /// outer level here is one entry per stored kind for the whole shard -- a constant head, not a
    /// per-object tree -- so this scan is bounded by the KIND count and never by the object count.
    pub(super) fn any_shared_object_key(&self, object_key: &str) -> Option<Arc<str>> {
        self.by_model.values().find_map(|objects| {
            objects
                .get_key_value(object_key)
                .map(|(stored, _)| Arc::clone(stored))
        })
    }

    /// The address of the inner key allocation, so a test can assert that a block and this map
    /// point at one copy of the object identity rather than two equal ones. Contents cannot tell
    /// those apart; pointers can.
    #[cfg(test)]
    pub(super) fn key_ptr(&self, model_id: &str, object_key: &str) -> Option<*const u8> {
        let (stored, _) = self.by_model.get(model_id)?.get_key_value(object_key)?;
        Some(stored.as_ptr())
    }

    pub(super) fn remove(&mut self, model_id: &str, object_key: &str) -> Option<BlockRefs> {
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

    /// How many MODEL levels the outer map holds -- NOT the object count, and reported precisely
    /// so that the two are never read as one figure. One entry per stored kind for the whole
    /// shard, which is what makes the outer level a constant head.
    #[cfg(test)]
    pub(super) fn model_count(&self) -> usize {
        self.by_model.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.by_model.values().all(BTreeMap::is_empty)
    }

    pub(super) fn clear(&mut self) {
        self.by_model.clear();
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &BlockRefs> {
        self.by_model.values().flat_map(BTreeMap::values)
    }

    /// Model and object for every entry, for the places that used to read the composite key.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&Arc<str>, &Arc<str>, &BlockRefs)> {
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

impl From<BTreeMap<String, BlockRefs>> for ObjectBlockLookup {
    fn from(flat: BTreeMap<String, BlockRefs>) -> Self {
        let mut nested: BTreeMap<Arc<str>, BTreeMap<Arc<str>, BlockRefs>> = BTreeMap::new();
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

impl From<ObjectBlockLookup> for BTreeMap<String, BlockRefs> {
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

/// THE LOOKUP'S PER-OBJECT VALUE, AND WHAT IT REPLACED.
///
/// `BlockRefs` is now the value `ObjectBlockLookup` holds for each object, which is where the
/// collapse's saving lands. The value was an `ObjectBlockRefs` holding a `ComponentList` holding a
/// `ComponentBlocks` holding this -- and all four were 40, 40, 40 and 24 because the first three
/// WERE THE SAME 40 BYTES, nested inline. The step is 40 to 24 per object: the 16 bytes of the
/// always-`None` `Option<Arc<str>>`, and nothing for the wrappers. See [`ObjectBlockLookup`] on why
/// reading those four declarations as a 120-byte sum is the mistake it looks like.
///
/// HOW IT IS HELD, which is what decides whether that step is worth anything. This is a
/// `BTreeMap` VALUE, and `std`'s B-tree leaf holds eleven entries in one allocation whatever the
/// value type -- so the 16-byte step is multiplied by eleven before it meets an allocator size
/// class, and the class is irrelevant. That is the opposite of a structure held ONE PER
/// ALLOCATION, where `chunk(40)` and `chunk(32)` fall in the same 48-byte class and a step
/// between them moves nothing at all. Same arithmetic, opposite conclusion, and which one applies
/// is a property of the holder rather than of the width.
///
/// PINNED AT ITS WIDTH AND AT ITS ALIGNMENT, AND THE NEIGHBOUR BRACKET THAT STOOD HERE IS GONE.
/// The note read that a single `==` which compiles tells you the value while the pair tells you
/// the value is not an artefact of the comparison chosen. The pair told you nothing: `!= 23` and
/// `!= 25` sat in the same const context as `== 24`, which entails both, and 23 and 25 are
/// unreachable for an 8-aligned type in any case. The argument is written out in full beside
/// [`BlockIndex`]'s width pin. The alignment is pinned in its place, because a width pin admits
/// every alignment that divides it and this one is the modulus `per_item_byte_budget`'s row for
/// this type pads with.
const _: () = assert!(std::mem::size_of::<BlockRefs>() == 24);
const _: () = assert!(std::mem::align_of::<BlockRefs>() == 8);
/// AND THE DECOMPOSITION, not merely the width -- a width test in this tree once printed a wrong
/// field sum and passed because it asserted only the total. The widest arm is the spilled vector,
/// and the enum is exactly as wide as it, so the tag costs nothing. If the tag ever stops being
/// free this goes red rather than silently adding a word.
const _: () = assert!(
    std::mem::size_of::<BlockRefs>() == std::mem::size_of::<Vec<BlockLookupRef>>()
);
/// The inline arm fits inside that same width with room for the tag, which is the other half of
/// the decomposition: the arm is a `BlockLookupRef` and it is strictly narrower than the vector.
const _: () = assert!(
    std::mem::size_of::<BlockLookupRef>() < std::mem::size_of::<Vec<BlockLookupRef>>()
);

/// AND THE WIDTH THIS REPLACED, DECOMPOSED IN TERMS OF TYPES THAT STILL EXIST.
///
/// `ComponentBlocks` was `Option<Arc<str>>` plus `BlockRefs` with no padding, so its 40 is still
/// checkable with the type itself deleted -- which is the point: a "before" number quoted in a
/// comment goes stale silently, and this one cannot. If the name's width or the refs' width moves,
/// this goes red and the 40-to-24 claim above has to be restated rather than left wrong.
const _: () = assert!(
    std::mem::size_of::<Option<Arc<str>>>() + std::mem::size_of::<BlockRefs>() == 40
);
/// The half of that sum the collapse removed, named on its own so the saving is not inferred from
/// a subtraction: the element name was 16 bytes and held a constant `None`.
const _: () = assert!(std::mem::size_of::<Option<Arc<str>>>() == 16);

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

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove ONE ref, reporting whether it was there -- the counterpart of [`BlockRefs::insert`],
    /// and the counter is kept from the same answer for the same reason.
    ///
    /// # WHY THE INLINE ARM BECOMES AN EMPTY SPILLED ONE
    ///
    /// `One` cannot represent zero refs. Removing the only ref therefore has to change the arm, and
    /// it becomes `Many(vec![])` rather than panicking or leaving the ref behind. That state is
    /// transient by contract: the holder drops the slot when this leaves it empty, and
    /// [`CoreIndex::remove_object_block_lookup_ref`] is the only caller precisely so that contract
    /// has one holder.
    /// Representing it anyway is what keeps the transition from being a panic.
    ///
    /// SINCE THE SECOND LEVEL COLLAPSED THE EMPTY STATE IS NOT EVEN REPRESENTABLE ABOVE. The slot
    /// is the object's presence in `ObjectBlockLookup`, so the remover drops the object in the same
    /// breath that sees the refs empty -- there is no longer a slot that could be left holding this.
    pub(super) fn remove(&mut self, value: &BlockLookupRef) -> bool {
        match self {
            Self::One(existing) => {
                if existing == value {
                    *self = Self::Many(Vec::new());
                    true
                } else {
                    false
                }
            }
            // Sorted by `insert`, so the search that finds it is the search that put it there.
            Self::Many(values) => match values.binary_search(value) {
                Ok(at) => {
                    values.remove(at);
                    true
                }
                Err(_) => false,
            },
        }
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

/// WHY THIS IS TWO TREES AND NOT A BOUNDED RING OF INTEGER IDS.
///
/// A fixed-capacity, pre-allocated ring of `u64` ids beside the dirty bits this engine already
/// carries -- `BucketFlags` is one byte and `BlockIndex` holds `dirty` and `deleted` -- would hold
/// a dirty object in eight bytes where the pair of trees above holds one in 20 plus 28 bytes of
/// field. It is refused on two grounds, and the first is not about width at all.
///
///   1. EVERY READER OF `by_key` READS IT BY KEY, AND ONE READS THE KEY TEXT.
///      `storage_bucket_internals`'s ownership reconstruct asks `contains(entry.object_key)` once
///      per live entry; `engine::object_manager_stats` ITERATES these keys and hands each to
///      `bucket_index_target_buckets_for_object_key`, which needs the text to route. A ring of
///      derived ids answers neither. The text is not recoverable from an id -- the page entry
///      holds no identity, and this index is not the object lookup -- and `remove` by a derived id
///      would clear the dirty mark of any other key sharing that id, which is a dirty object that
///      is never dumped. That is the one failure this index exists to prevent, so the ring trades
///      a footprint for a data loss. The readers were enumerated by the COMPILER, by deleting the
///      two by-key doors rather than by grepping for them: deleting `contains` and `iter` is twelve diagnostics, four of them in production, and THREE of those four consume the key as text -- `compaction.rs` probes seven key-addressed maps with it, `storage_reporting.rs` routes it and hands it out of the engine as a `String` in a report, and `engine::object_manager_stats` routes it into the object lookup. A grep over the field found only the last of the three.
///   2. A BOUND NEEDS AN OVERFLOW BEHAVIOUR, AND THE ONLY SAFE ONE UNDOES THE DRAIN. The dirty set
///      IS the write backlog between dumps, and nothing bounds that: the footprint probe seeds
///      tens of thousands of dirty objects with no dump taken at all. Dropping an id on overflow
///      loses a mark. Forcing a dump from the write path takes the dump's lock under the shard
///      write lock. What is left is to record "overflowed" and treat the whole shard as dirty,
///      which turns every dump under sustained write into a whole-shard dump and gives back
///      exactly what [`DirtyObjectIndex::drain_buckets`] was built to win. A ring that may grow
///      without bound is this `BTreeMap` with a worse `contains`.
///
/// The key TEXT is held once for both trees -- `by_bucket` holds the same `Arc<str>` as `by_key`,
/// which the paragraph above this struct states -- and the footprint probe reports how many of
/// those allocations the bucket index turns out to be sharing, because a saving claimed for
/// dropping a key from one holder is zero if another holder owns the bytes.

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
/// The shape `ObjectIndex`, `BlockIndexMap` and `BlockRefs` already use, for the
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
            self.insert_inner(object_key, routing_bucket, None)
        })
    }

    /// As `insert`, with the key's allocation supplied by the caller.
    ///
    /// `ObjectBlockLookup` already holds an `Arc<str>` for every object that has blocks, and this
    /// index allocated a SECOND copy of the same characters for every object it had not seen since
    /// the last dump. The dirty set is drained per bucket at dump and the lookup is not, so that
    /// was one allocation of the key per object per dump cycle, freed only when the set drained
    /// again. Handing the handle in makes it a refcount bump instead.
    ///
    /// POINTER IDENTITY IS THE ONLY THING THAT DISTINGUISHES THIS FROM THE OLD BEHAVIOUR. The
    /// contents were always equal, which is why a contents comparison cannot witness it and
    /// `key_ptr` exists.
    pub(super) fn insert_shared(&mut self, shared: Arc<str>, routing_bucket: u32) {
        crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::DirtyObjects, || {
            let borrowed = Arc::clone(&shared);
            self.insert_inner(&borrowed, routing_bucket, Some(shared))
        })
    }

    /// Swap this index's OWN copy of a key for the handle the page index holds.
    ///
    /// WHY THIS EXISTS WHEN `insert_shared` ALREADY DOES THE RIGHT THING. It does, and it is wired:
    /// both sites that mark an object dirty read `any_shared_object_key` first and hand the handle
    /// in when there is one. An ORDERING defeats it. On an object's FIRST write the mark runs before
    /// its page is filed, so the lookup has nothing to hand over, the `None` arm allocates a second
    /// copy of the characters, and every later `insert` for that object takes `insert_inner`'s
    /// early return and never reconsiders. The result is permanent: measured at 24.0 B/key across
    /// 40,040 keys, with ZERO of the dirty index's allocations shared with the bucket index.
    ///
    /// So this is called once the page IS filed, when the handle finally exists -- the same
    /// adopt-afterwards shape the ordered deadline map uses, for the same reason.
    ///
    /// POINTER IDENTITY IS THE WHOLE POINT, AND IT IS WHY A CONTENTS CHECK CANNOT WITNESS THIS.
    /// The two keys were always equal as text; what changes is that one allocation is dropped. The
    /// early return below is on `Arc::ptr_eq`, not on equality, because equality is already true.
    ///
    /// Re-keys BOTH inner maps. Leaving the old handle in `by_bucket` would keep the allocation
    /// alive and this would free nothing while appearing to work.
    pub(super) fn adopt_shared_key(&mut self, shared: &Arc<str>) -> bool {
        let Some((held, routing_bucket)) = self.by_key.get_key_value(shared.as_ref()) else {
            return false;
        };
        if Arc::ptr_eq(held, shared) {
            return false;
        }
        let routing_bucket = *routing_bucket;
        self.by_key.remove(shared.as_ref());
        self.by_key.insert(Arc::clone(shared), routing_bucket);
        if let Some(keys) = self.by_bucket.get_mut(&routing_bucket) {
            keys.take(shared.as_ref());
            keys.insert(Arc::clone(shared));
        }
        true
    }

    fn insert_inner(
        &mut self,
        object_key: &str,
        routing_bucket: u32,
        preallocated: Option<Arc<str>>,
    ) {
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
            let shared = moved
                .or(preallocated)
                .unwrap_or_else(|| Arc::from(object_key));
            self.by_bucket
                .entry(routing_bucket)
                .or_default()
                .insert(shared);
            return;
        }
        let shared: Arc<str> = preallocated.unwrap_or_else(|| Arc::from(object_key));
        self.by_key.insert(Arc::clone(&shared), routing_bucket);
        self.by_bucket
            .entry(routing_bucket)
            .or_default()
            .insert(shared);
    }

    /// The address of this index's key allocation, so a test can assert it IS the page
    /// index's rather than an equal copy. Contents cannot tell those apart; pointers can --
    /// the same reason `ObjectBlockLookup::key_ptr` exists.
    #[cfg(test)]
    pub(super) fn key_ptr(&self, object_key: &str) -> Option<*const u8> {
        let (stored, _) = self.by_key.get_key_value(object_key)?;
        Some(stored.as_ptr())
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
    /// WHICH ELEMENT EACH TOMBSTONE ENTRY IN THIS BUCKET IS ABOUT.
    ///
    /// # WHY IT IS NOT ON THE ENTRY
    ///
    /// A LIVE entry is a per-PAGE fact: the page's own frame names every element on it, so the
    /// entry needs no element name and dropping it is what one entry a page means. A TOMBSTONE is
    /// a per-ELEMENT fact -- recording WHICH element was removed is its whole content -- so a
    /// nameless one is not smaller, it is useless. Two consequences, both measured: the re-add
    /// sweep could not tell whose tombstone it was holding, so re-adding Y cleared X's and X came
    /// back (a gated removal served twelve members where the invariant is eleven); and the
    /// retention could not be bounded, so four removals of ONE member left four tombstones.
    ///
    /// A single struct cannot have a field only some of its instances pay for, and the entry is
    /// the structure the index is MADE of -- one per page, millions of them, almost all live. So
    /// the name lives beside the entries instead, where ONLY A REMOVAL PAYS for it.
    ///
    /// # A VEC AND NOT A MAP
    ///
    /// Bounded at one row per distinct element removed, and every reader of it is already walking
    /// this bucket's whole page list in the same call -- the sweep's probe, the two rebuilds'
    /// live-again test, the removal's already-tombstoned test. A linear find over the rows is
    /// strictly smaller than the walk it rides along with, and a map keyed by a tuple would have
    /// had to be given a string spelling to cross the wire.
    ///
    /// # AND IT IS RECONCILED, NOT MERELY MAINTAINED
    ///
    /// [`BucketNode::reconcile_tombstone_elements`] derives the rows back from the deleted entries
    /// at the classification pass every bucket mutation funnels through, so a site that drops a
    /// tombstone entry without saying so cannot leave a row describing a tombstone that is gone.
    /// That is the same doctrine `reconcile_object_index_with_live_pages` already applies to the
    /// object list.
    /// NO `#[serde]` ATTRIBUTE, DELIBERATELY. `BucketNode`'s `Serialize` and `Deserialize` are
    /// written by hand -- the five flags are five wire keys and one byte in memory -- so an
    /// attribute here would be inert text describing behaviour it does not control. The
    /// skip-when-empty and the default-when-absent are in those two impls, where they are real.
    pub(super) tombstone_elements: TombstoneElements,
}

/// The element one tombstone entry is about, and the page that entry names.
///
/// `page` is the tombstone entry's `(slab, offset, length)` -- how a page is identified everywhere
/// in this index now that an entry does not name its element. It is STABLE across a dump and a
/// reload, which the handle `BlockIndexMap` assigns is not: the map re-assigns handles on every
/// load, so a row keyed by one would point at a different entry after a restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TombstoneElement {
    pub(super) object_key: Arc<str>,
    pub(super) model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    pub(super) component: Arc<str>,
    pub(super) page: (u64, u64, u64),
}

/// THE TOMBSTONE ROWS A BUCKET HOLDS, HELD INDIRECTLY SO A BUCKET WITH NO REMOVAL PAYS ONE WORD.
///
/// A bare `Vec` is three words, and the bucket node is the structure whose count is the BUCKET
/// count -- so a vector that is empty in almost every bucket would cost 24 bytes a bucket to carry
/// nothing. `pages_per_bucket` already measures this exact choice for a per-bucket vector, as
/// `MirrorAlwaysMapped` against `MirrorAlwaysIndirect`, and the indirect form is one word.
///
/// THE INDIRECTION IS PAID BY REMOVALS ONLY, which is the whole shape of this change: the element
/// name left the ENTRY -- one per page, millions of them, almost all live -- for a structure only a
/// removal allocates. A bucket that has never had one holds `None` and dereferences nothing.
///
/// `None` and `Some(empty)` are the same state to every reader here, and the reconcile that can
/// empty the vector puts it back to `None` so the two cannot both be reachable with different
/// behaviour.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct TombstoneElements(Option<Box<Vec<TombstoneElement>>>);

impl TombstoneElements {
    pub(super) fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(|rows| rows.is_empty())
    }

    pub(super) fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |rows| rows.len())
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &TombstoneElement> {
        self.0.iter().flat_map(|rows| rows.iter())
    }

    fn iter_mut(&mut self) -> impl Iterator<Item = &mut TombstoneElement> {
        self.0.iter_mut().flat_map(|rows| rows.iter_mut())
    }

    fn push(&mut self, row: TombstoneElement) {
        self.0.get_or_insert_with(Box::default).push(row);
    }

    /// Retain, and DROP THE ALLOCATION when nothing is left, so `None` is the only spelling of
    /// empty that can be observed.
    fn retain(&mut self, keep: impl FnMut(&TombstoneElement) -> bool) {
        if let Some(rows) = self.0.as_mut() {
            rows.retain(keep);
            if rows.is_empty() {
                self.0 = None;
            }
        }
    }
}

/// AS A PLAIN SEQUENCE ON THE WIRE. The indirection is an in-memory decision and must not reach the
/// stored shape: a reader that saw `null` or a one-element wrapper here would be reading the
/// allocation strategy instead of the rows.
impl Serialize for TombstoneElements {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for row in self.iter() {
            seq.serialize_element(row)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for TombstoneElements {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let rows: Vec<TombstoneElement> = Deserialize::deserialize(deserializer)?;
        Ok(if rows.is_empty() {
            Self(None)
        } else {
            Self(Some(Box::new(rows)))
        })
    }
}

/// How a tombstone row and a tombstone entry agree on which page they mean.
///
/// The same triple `engine::live_page_key` computes, spelled here because `state` is below
/// `engine` in the module tree and this is the one place the node itself needs it.
pub(super) fn tombstone_page_key(address: &ElementEntry) -> (u64, u64, u64) {
    (
        address.block_slab_id(),
        address.offset(),
        address.length(),
    )
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
///
/// AND IT IS 96 NOW, NOT 88, BECAUSE A REMOVAL'S ELEMENT NAME MOVED HERE. The node gained
/// `tombstone_elements`, which is ONE WORD -- an `Option<Box<Vec<_>>>` and not a bare `Vec`,
/// because the bucket node's count IS the bucket count and three words a bucket to carry nothing
/// is not a trade. There was no slack to absorb even the one word: the tail was six rounded to
/// eight and the new field lands in a word of its own, so this is eight real bytes a bucket.
///
/// WHAT IT BOUGHT, AND THE DENOMINATORS ARE WHY IT IS A TRADE. `BlockIndex` -- one per PAGE, which
/// is the structure the index is MADE of -- stopped carrying `Option<Arc<str>>` for an element name
/// that only a tombstone ever needed, and went 56 to 40. So eight bytes a BUCKET buys sixteen
/// bytes a PAGE, and a bucket holds many pages: at the 1023-bucket default a shard pays about 8 KiB
/// for it and recovers sixteen bytes on every page entry it holds.
const _: () = assert!(std::mem::size_of::<BucketNode>() == 96);

impl BucketNode {
    /// Install a block in this bucket, charge it to the live tally, and return its handle.
    ///
    /// THE HANDLE IS COMPUTED HERE, one level above the map, and that is the point of this method
    /// existing at all. `self` is the bucket, so every structure the bucket owns is in scope --
    /// which is what a term moved off the entry into a per-bucket structure would need, and what
    /// `BlockIndexMap` can never offer, since a field cannot see its siblings.
    ///
    /// It banks nothing today: `block_index_handle` reads the same terms it always did, in the
    /// same order, through the same hasher, so the value is byte-identical. What changes is that
    /// the computation is no longer pinned to a scope that can only see the entry.
    ///
    /// THE IDENTITY ARRIVES AS A PARAMETER, for the same reason the four derivations in
    /// `070f24334` take it: it is leaving the entry, and a handle computed from
    /// `page.object_key` is a handle no caller can supply once the field is gone. Same bytes into
    /// the same hasher in the same order, so every handle is byte-identical; what moves is where
    /// the key comes from.
    pub(super) fn insert_page(
        &mut self,
        object_key: &str,
        page: BlockIndex,
        live: &mut BlockSlabLiveIndex,
    ) -> u64 {
        let handle = block_index_handle(object_key, &page);
        self.block_index.insert_with_handle(handle, page, live)
    }

    /// Install a block that is ALREADY counted, with the handle computed at this level.
    ///
    /// One caller, and it must stay that way -- see
    /// [`BlockIndexMap::insert_released_with_handle`].
    pub(super) fn insert_released_page(&mut self, object_key: &str, page: BlockIndex) -> u64 {
        let handle = block_index_handle(object_key, &page);
        self.block_index.insert_released_with_handle(handle, page)
    }

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

    /// RECORD WHICH ELEMENT A TOMBSTONE ENTRY IN THIS BUCKET IS ABOUT.
    ///
    /// Keyed on what identifies the element -- object, kind, name -- and valued by the PAGE the
    /// tombstone entry names, so both of the questions the removal machinery asks can be answered:
    /// "is this element already tombstoned" reads the key, and "which entry do I take when this
    /// element is written back" reads the value.
    ///
    /// REPLACES rather than appends on a repeat. The retention bound is ONE ROW PER DISTINCT
    /// ELEMENT REMOVED and not one per removal issued -- four removals of one member must leave
    /// one row, which is the bound a nameless tombstone could not state and which was measured
    /// growing per removal when it could not.
    /// THE IDENTITY ARRIVES AS A HANDLE, BECAUSE TWO OF THE THREE CALLERS ALREADY HELD ONE.
    ///
    /// This took `&str` and finished with `object_key: Arc::from(object_key)`, so the row owned
    /// a second copy of characters something else already had. Measured: with the object still
    /// alive, the page index held the key at one address and the row it wrote held it at
    /// another -- `what_the_tombstone_path_files_and_what_it_allocates`.
    ///
    /// The waste was plainest at the two rebuild callers, which pass `&element.object_key` -- a
    /// block entry's own shared handle -- and had it re-minted from under them. A `&str`
    /// parameter discarded the sharing at the boundary, which is a different failure from not
    /// looking the handle up: here the caller had already paid for it. Those two now hand it
    /// straight through and need no change at the call site. The third caller,
    /// `insert_container_tombstone_entry`, takes one from the lookup before its `&mut` borrow.
    pub(super) fn record_tombstone_element(
        &mut self,
        object_key: &Arc<str>,
        model_id: crate::engine::storage_bucket_internals::StoredModelKind,
        component: &str,
        address: &ElementEntry,
    ) {
        let page = tombstone_page_key(address);
        if let Some(row) = self.tombstone_elements.iter_mut().find(|row| {
            row.model_id == model_id
                && &*row.object_key == &**object_key
                && &*row.component == component
        }) {
            row.page = page;
            return;
        }
        self.tombstone_elements.push(TombstoneElement {
            // CLONING A HANDLE, NOT COPYING CHARACTERS. This was `Arc::from(object_key)`.
            object_key: Arc::clone(object_key),
            model_id,
            component: Arc::from(component),
            page,
        });
    }

    /// The page the tombstone for this element names, if this bucket holds one.
    pub(super) fn tombstone_element_page(
        &self,
        object_key: &str,
        model_id: crate::engine::storage_bucket_internals::StoredModelKind,
        component: &str,
    ) -> Option<(u64, u64, u64)> {
        self.tombstone_elements
            .iter()
            .find(|row| {
                row.model_id == model_id
                    && &*row.object_key == object_key
                    && &*row.component == component
            })
            .map(|row| row.page)
    }

    /// The element the tombstone entry over this page is about.
    ///
    /// The other direction, for the two rebuilds: they hold a tombstone ENTRY and have to decide
    /// whether its element is live again, which is a question about the element and not the page.
    pub(super) fn tombstone_element_at(&self, address: &ElementEntry) -> Option<&TombstoneElement> {
        let page = tombstone_page_key(address);
        self.tombstone_elements.iter().find(|row| row.page == page)
    }

    /// Forget one element's tombstone row. Paired with the retain that drops its entry.
    pub(super) fn forget_tombstone_element(
        &mut self,
        object_key: &str,
        model_id: crate::engine::storage_bucket_internals::StoredModelKind,
        component: &str,
    ) {
        self.tombstone_elements.retain(|row| {
            !(row.model_id == model_id
                && &*row.object_key == object_key
                && &*row.component == component)
        });
    }

    /// Forget every tombstone row for one object, for the whole-object delete that takes its
    /// entries.
    pub(super) fn forget_object_tombstone_elements(&mut self, object_key: &str) {
        self.tombstone_elements
            .retain(|row| &*row.object_key != object_key);
    }

    /// Put the rows back in step with the entries, DERIVED from the entries rather than trusted.
    ///
    /// A row whose page no longer carries a deleted entry in this bucket describes a tombstone that
    /// is not here. That is the drift direction a reconcile can fix, and it is fixed the way
    /// `reconcile_object_index_with_live_pages` fixes the object list: by deriving from the pages,
    /// at the pass every bucket mutation already funnels through, instead of asking each mutation
    /// site to remember. The OTHER direction -- a deleted entry with no row -- is not repairable
    /// here and is not left to chance either: `insert_container_tombstone_entry` is the one
    /// producer of a tombstone entry and it files the row in the same call.
    pub(super) fn reconcile_tombstone_elements(&mut self) {
        if self.tombstone_elements.is_empty() {
            return;
        }
        let pages: std::collections::BTreeSet<(u64, u64, u64)> = self
            .block_index
            .values()
            .filter(|page| page.deleted)
            .map(|page| tombstone_page_key(&page.address))
            .collect();
        self.tombstone_elements
            .retain(|row| pages.contains(&row.page));
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
// THE BLANKET FIXTURE DOORS ARE GONE, AND THAT IS THE POINT RATHER THAN A TIDY-UP.
//
// `#[cfg(test)] impl Serialize for BucketNode` and the matching one for `BlockIndexMap` let a
// test write `serde_json::to_string(&node)` and get bytes whose page keys were composed from the
// entries' own copies of the identity. With that copy gone the only thing such an impl could
// compose is a key naming nothing, once per page, returned as `Ok` -- and an inherent impl on the
// type is reachable from anywhere in the test tree, so nothing would ask the caller for the
// identity it means.
//
// [`NodeNamedBy`] and [`PageIndexNamedBy`] replace them. They are wrappers, so every one of the
// twenty-six arms now states the objects its fixture wrote, and the compiler asked each of them
// to. Their doc carries why the objects are matched by handle rather than listed per page.

/// THE BODY EVERY NODE DOOR GOES THROUGH, PARAMETERISED ON HOW THE PAGE INDEX IS WRITTEN.
///
/// The page index used to be composed inside this function from an `Option` context, which is
/// what let the keyless case exist. It arrives as a value that already knows how to write itself,
/// so there is one body, no `Option`, and exactly two things that can be passed: the production
/// [`PageIndexAt`], which is handed the identity from beside each page, and the `#[cfg(test)]`
/// [`PageIndexFromTheEntriesOwnCopies`], which is not reachable from production code.
fn serialize_bucket_node<S, P>(
    node_ref: &BucketNode,
    page_index: &P,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    P: Serialize,
{
    {
        let self_ = node_ref;
        use serde::ser::SerializeStruct;
        // THE COUNT IS PART OF THE ENCODING, not a hint. The served index is msgpack
        // `with_struct_map`, so `serialize_struct` writes a MAP HEADER of exactly this length --
        // a node that then writes a thirteenth field under a header of twelve is corrupt bytes,
        // not a tolerated mismatch. So the optional key below is counted here rather than
        // hard-coded, and the two cannot disagree.
        let field_count = 12 + usize::from(!self_.tombstone_elements.is_empty());
        let mut node = serializer.serialize_struct("BucketNode", field_count)?;
        node.serialize_field("routing_slot", &self_.routing_bucket)?;
        node.serialize_field("layout", &self_.layout)?;
        node.serialize_field("dirty", &self_.dirty())?;
        node.serialize_field("deleted", &self_.deleted())?;
        node.serialize_field("meta_loaded", &self_.meta_loaded())?;
        node.serialize_field("loading", &self_.loading())?;
        node.serialize_field("in_memory", &self_.in_memory())?;
        node.serialize_field("ttl_ms", &self_.ttl_ms)?;
        node.serialize_field("dirty_generation", &self_.dirty_generation)?;
        node.serialize_field("object_index", &self_.object_index)?;
        node.serialize_field("deleted_object_index", &self_.deleted_object_index)?;
        // AT THE BUCKET LEVEL, through `PageIndexAt`, so the written key is composed where the
        // node's other structures are in scope. The bytes are the ones the map's own impl wrote.
        node.serialize_field("page_index", page_index)?;
        // THE ELEMENT EACH TOMBSTONE IN THIS BUCKET IS ABOUT, which the entries themselves no
        // longer say. Written LAST so the key is appended to the shape rather than inserted into
        // the middle of it, and `skip_serializing_if` keeps it off every bucket that has never had
        // a removal -- which is almost all of them, so the ordinary node's bytes do not move.
        if !self_.tombstone_elements.is_empty() {
            node.serialize_field("tombstone_elements", &self_.tombstone_elements)?;
        }
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
    TombstoneElements,
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
    "tombstone_elements",
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
                    "tombstone_elements" => BucketNodeField::TombstoneElements,
                    _ => BucketNodeField::Ignore,
                })
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

/// A DECODED NODE AND THE IDENTITY OF EVERY PAGE IN IT.
///
/// The pair exists because serde hands a `Deserialize` impl no context and lets it return only
/// `Self`: a node's decoder cannot reach the index's `object_block_lookup`, and the index's
/// decoder cannot see inside a node's page index. What travels between them is this.
///
/// The same shape as `BucketNodeAt` on the encode side, for the same reason and in the other
/// direction.
pub(super) struct BucketNodeWithPageKeys {
    pub(super) node: BucketNode,
    pub(super) page_keys: Vec<(u64, Arc<str>)>,
}

/// THE FIXTURE DOOR: a node on its own, with its page identities dropped.
impl<'de> Deserialize<'de> for BucketNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(BucketNodeWithPageKeys::deserialize(deserializer)?.node)
    }
}

impl<'de> Deserialize<'de> for BucketNodeWithPageKeys {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NodeVisitor;

        impl<'de> serde::de::Visitor<'de> for NodeVisitor {
            type Value = BucketNodeWithPageKeys;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a bucket node")
            }

            fn visit_map<M>(self, mut map: M) -> Result<BucketNodeWithPageKeys, M::Error>
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
                // DECODED FLAT, and that is the change. It was `Option<BlockIndexMap>`, which
                // threw the stored map's keys away inside the conversion; the page identities have
                // to come out of this decode, so the flat form is what is held and the conversion
                // happens where both halves can be kept.
                let mut block_index: Option<BTreeMap<String, StoredPageEntry>> = None;
                // DEFAULTS WHEN ABSENT, and that is not a compatibility affordance: an index
                // written before this key existed is REFUSED by its stamp before it reaches here
                // (`persistence` compares `<` and falls back to WAL replay), so the only node that
                // can arrive without the key is one this engine wrote with no tombstone in it.
                let mut tombstone_elements: Option<TombstoneElements> = None;

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
                        BucketNodeField::TombstoneElements => {
                            once!(tombstone_elements, "tombstone_elements")
                        }
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

                let (block_index, page_keys) =
                    page_index_and_identities(block_index.unwrap_or_default());

                Ok(BucketNodeWithPageKeys {
                    node: BucketNode {
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
                        block_index,
                        tombstone_elements: tombstone_elements.unwrap_or_default(),
                    },
                    page_keys,
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

// NEITHER HALF OF SERDE IS ON THIS DERIVE ANY MORE, AND THAT IS THE STEP.
//
// `object_key` is a SERIALIZED MEMBER of the stored entry, so a derive on this struct is a derive
// that reads the identity off the entry -- the one place it is about to stop being. The entry
// reaches the wire through [`PageEntryAt`] instead, which is handed the identity from BESIDE the
// page and writes the same five keys in the same order.
//
// AND IT ARRIVES THROUGH [`StoredPageEntry`], FOR THE MIRROR-IMAGE REASON. A derived
// `Deserialize` would go on compiling with the field gone -- serde IGNORES an unknown key on a
// derived struct unless `deny_unknown_fields` is set -- and would silently drop the one term the
// load path cannot do without. `object_block_lookup` is `skip_serializing` and is rebuilt from
// what the decode reads, so an `object_key` quietly discarded here is an index that loads with no
// lookup, which is the state the encoder now refuses to write. The stored form still CARRIES the
// key; what changes is that the thing it is read into is not the entry.
#[derive(Debug, Clone)]
pub(super) struct BlockIndex {
    pub(super) model_id: crate::engine::storage_bucket_internals::StoredModelKind,
    pub(super) address: ElementEntry,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
}

/// One per stored block, AND IT CARRIES NO IDENTITY OF ITS OWN: a one-byte model spelling, an
/// address, and two flags -- 19 bytes of field in 24, with FIVE OF SLACK.
///
/// THIS PARAGRAPH CONTRADICTED THE CONST ASSERT THIRTY LINES BELOW IT, TWICE OVER, AND THAT IS WHY
/// IT IS WRITTEN UP RATHER THAN QUIETLY FIXED. It read "TWO shared names ... 40 bytes of field in
/// 40, WITH NO SLACK LEFT" and then, a paragraph later, "the entry is 40 of field in 40". The const
/// block asserted `field_sum == 35` and `size_of - field_sum == 5` on the same revision. Both
/// numbers in the prose were wrong and the const assert was right -- and the prose is the form
/// nothing can fail, so it went on being read as the summary of a block that said something else.
///
/// THE HISTORY IS KEPT BECAUSE IT IS THE WARNING. Earlier drafts read "56 bytes of field in 56",
/// before that "52 of field in 56", and at the flags "at 60 four again -- and at 56 it is ZERO".
/// Every one of those was a decomposition that had stopped describing the type, and each survived
/// because `size_of` steps at a word: a field set can lose five bytes and round to the same
/// number. Every field decision taken here while the entry was believed to be exactly full has to
/// be RE-PRICED rather than inherited.
///
/// 24, NOT 40, SINCE THE IDENTITY LEFT: `Arc<str>` is a sixteen-byte fat pointer, so the field set
/// went 35 -> 19 and the width 40 -> 24 -- a whole two words rather than one word and some
/// rounding. That is the SECOND two-word step out of this structure in two legs; the first was
/// 56 -> 40 when the element name left, which was `Option<Arc<str>>`, sixteen bytes with a niche.
///
/// AND A FIELD UP TO FIVE BYTES WIDE IS STILL FREE HERE. Nineteen is not a multiple of eight, so
/// an eight-aligned struct holding it has five bytes standing empty by construction -- as it did
/// at 40, where the field sum was thirty-five. What changed is the width, not the room in it.
/// `element_ordinal_reuse::the_width_an_element_ordinal_would_take_and_the_ceiling_that_implies`
/// prices both candidate ordinal widths against the OLD width and has to be re-read against this
/// one.
///
/// 56, not 64, since the address inside it is 16 bytes and not 24; 64, not 72, before that, since
/// the address shed the `routing_bucket` the read path now derives from the key and narrowed its
/// `block_id` to the sixteen bits the record encoder has always refused to exceed. That is one
/// whole eight-byte step out of this structure among several: 72, not 88, since the model spelling
/// stopped being a sixteen-byte fat pointer to a string drawn from a seventeen-element set; 88, not
/// 96, since the address merged its slab id and its offset into one word; 96, not 104, since it
/// shed its derived `generation` before that.
///
/// THE ADDRESS STEP WAS TWO NARROWINGS AND NEITHER WAS WORTH ANYTHING ALONE. The address carried 29
/// bytes of payload in 32. Dropping the bucket leaves 25 and narrowing the id leaves 27; both
/// round back to 32 and this structure would have stayed 72 for either one. Together they leave
/// 23, the address is 24, and the step lands. `block_store`'s width assert carries both
/// counterfactuals beside the width, because the arithmetic is the claim and it was the per-field
/// reading of it that declared this blocked.
///
/// AND THE FLAGS STILL DO NOT PAY, THOUGH THE SLACK TABLE IN THIS PARAGRAPH WAS PART OF THE
/// CONTRADICTION. It read "at 56 it was ZERO -- and at 40 it is ZERO still, because 40 packs
/// exactly as 56 did", against a const block asserting five on the same revision. The slack was
/// FIVE at 40 and is FIVE at 24. Packing the two flags that remain would reclaim nothing, because
/// the tail rounds to one word with or without them, and it would still move the stored index,
/// which spells each of them as its own key. The claim is asserted directly in
/// `pages_per_bucket::every_byte_of_the_page_index_is_accounted_for` rather than read off the room
/// beside it.
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
/// `an_index_written_before_this_change_loads_page_for_page` (that an old row still decodes),
/// `an_index_written_before_this_change_is_written_back_without_the_key_the_entry_shed` (what this
/// binary writes back in its place) and `core_index_loads_legacy_bucket_page_field_names` (that
/// the old names still read).
// FORTY, AND THE NEIGHBOUR BRACKET THAT STOOD HERE IS GONE.
//
// IT COULD DISTINGUISH NO CASE. The `!= 39` and `!= 41` lines sat in the SAME const context as
// the `== 40` above them, and `x == 40` already entails `x != 39` and `x != 41`. An inequality
// dominated by an equality over the same expression cannot fail in any state the equality
// admits, so each could only have failed in a build that was already failing one line higher.
// That holds at every width and every alignment, so naming REACHABLE neighbours instead would
// be the same mistake with better arithmetic: it still names no case `== 40` lets through.
//
// THE ALIGNMENT IS WHY IT LOOKED HARMLESS, NOT WHY IT WAS EMPTY. `size_of` is always a multiple
// of `align_of` and this type is 8-aligned, so 39 and 41 are 7 and 1 mod 8 and no layout of it
// can reach either. The bracket was a theorem about Rust layout wearing this type's name. The
// treatment is the one `bucket_node_container_header` carries on main, where it was DRIVEN: a
// planted field that moves a real width reddens the `==` and leaves the `!=` partner GREEN.
//
// WHAT REPLACES IT IS A DIFFERENT CLAIM, NOT A BETTER BRACKET. A width pin admits every
// alignment that DIVIDES it -- `== 40` is equally true at align 1, 2, 4 and 8 -- so the line
// below can fail in a state that every width assertion in this file accepts. It is a PREMISE
// rather than a curiosity: `page_entry_handles`, `pages_per_bucket` and `per_item_byte_budget`
// each reconstruct this width as eight-aligned field plus a tail rounded up to
// `align_of::<BlockIndex>()`, so this value is the modulus of those decompositions.
const _: () = assert!(std::mem::size_of::<BlockIndex>() == 24);
const _: () = assert!(std::mem::align_of::<BlockIndex>() == 8);

// THE WIDTH, MEASURED AT THIS COMMIT -- not a target.
//
// A RECONSTRUCTION rather than a restatement: the groups are added up from the widths
// they are made of, so a field whose own width moves elsewhere fails HERE rather than
// passing silently.
//
// THE `object_key` TERM IS GONE FROM THIS SUM, which is the whole of the step. It was
// `size_of::<Arc<str>>()` -- sixteen bytes, a fat pointer -- so the field set went 35 -> 19 and
// the width 40 -> 24, a whole two words rather than one word and some rounding. The `component`
// term went the same way one leg earlier, from 56 -> 40.
//
// AND THIS BLOCK IS NOW THE ONLY DECOMPOSITION OF THIS STRUCT. `TheEntryWithoutAnElementName`
// stood below it: a second struct listing the same fields minus one, which recorded what the entry
// WOULD measure while the field was still present. It has been folded in here rather than left
// beside the real pin, because two decompositions of one type are two things to keep in step and
// the probe's whole justification -- that the number had to be real before anyone started -- is
// spent the moment the field is actually gone. What the probe asserted and this block keeps is the
// pair of claims it was built to hold apart: the SUM of the field widths is 40, and the TYPE
// measures 40, neither inferred from the other.
const _: () = {
    let field_sum = std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>() // model_id
        + std::mem::size_of::<ElementEntry>()                                // address
        + std::mem::size_of::<bool>()                                        // dirty
        + std::mem::size_of::<bool>();                                       // deleted
    // SPELLED AS TYPES, NOT AS LITERALS. A hand-written `1` for the model spelling goes on being
    // right by luck if the enum grows a payload; read off the type it goes red here instead.
    assert!(field_sum == 19);
    assert!(std::mem::size_of::<BlockIndex>() == (field_sum + 7) / 8 * 8);
    // AND THE WIDTH DID MOVE THIS TIME, WHICH IS WHY THE PAIR ABOVE IS NOT ENOUGH ON ITS OWN.
    //
    // The previous leg removed two fields and `size_of` stayed at FORTY, because thirty-five
    // rounds back to forty; this leg removes sixteen bytes and forty becomes twenty-four. So the
    // two claims this block has always held apart matter in opposite directions: last time the
    // sum moved and the width did not, this time both moved.
    //
    // THE FOUR LINES TOGETHER ADMIT ONLY TWENTY-FOUR, which is the property asked for rather than
    // a literal restated. `field_sum == 19` fixes the sum; `size_of == (field_sum + 7) / 8 * 8`
    // then forces the width to be nineteen rounded up, which is twenty-four and nothing else;
    // `size_of == 24` says it independently; and the slack line says the five bytes are real. A
    // sixteen-byte or thirty-two-byte struct satisfies none of them -- sixteen cannot hold a
    // nineteen-byte field set and thirty-two is not nineteen rounded up -- so no other width can
    // pass this block by coincidence.
    //
    // AND NO NEIGHBOUR INEQUALITY IS WRITTEN BESIDE THEM. An `== 24` entails `!= 23` and `!= 25`
    // over the same expression, so a bracket could fail in no state the equality admits; thirteen
    // such pairs were removed from this tree for exactly that reason. The alignment pin above is
    // NOT dominated the same way, because a width admits every alignment that divides it.
    assert!(std::mem::size_of::<BlockIndex>() == 24);
    assert!(std::mem::size_of::<BlockIndex>() - field_sum == 5);
};

// WHAT THE ENTRY WOULD BE IF ITS ADDRESS FIELD HELD THE BARE WORD: THIRTY-TWO, MEASURED.
//
// # AND THE FIELD CANNOT HOLD IT TODAY. THE NUMBER IS RECORDED, THE STEP IS REFUSED.
//
// The width is real and it is recorded here because the next lane needs it to be real before it
// starts. What blocks it is not the width and not the census: it is that THREE OF THE FOUR TERMS
// THE HOLDER CARRIES ARE READ THROUGH THIS FIELD, and every place that reads them is a place the
// bare word cannot answer. A compiler census over `address: BlockAddress` enumerates 157 errors in
// 39 files -- 48 on `length`, 26 on `block_id`, 7 on `generation` -- and SEVEN production readers
// of `block_id` alone, which is four more than the three this step was scoped against. The four
// that were missed are `BlockIndex::log_backed`, `container_page_ordinal`, and the handle and the
// written key.
//
// THE ONE THAT DECIDES IT IS `next_block_index_for_object`, and the lead that was supposed to free
// it is refuted by the structure it points at. A block id is an ordinal INSIDE its object, and the
// walk takes `max + 1` over the object's entries -- so the question is whether the ordinal is
// recoverable as a POSITION in the object's own block list instead of stored on every entry. It is
// not, on four independent grounds, each read off the list rather than argued:
//
//   * `BlockRefs::insert` keeps the list as a SORTED SET ordered by `BlockLookupRef`'s derived
//     `Ord`, whose second term is `block_ref_key` -- a `u64` HASH. Position in that list is
//     position in hash order, which carries no ordinal.
//   * The list is nested PER COMPONENT (`ComponentBlocks`), while `next_block_index_for_object`
//     filters on `(model_id, object_key)` and NOT on the component. A position inside one
//     component's list is the wrong denominator for a maximum taken across all of them.
//   * `insert_object_block_lookup` returns early on `page.deleted`, so the list omits exactly the
//     entries the walk still counts. Taking the ordinal from the list would hand out a value BELOW
//     a deleted-but-still-reachable block's ordinal, which is the reuse this walk's own doc calls
//     two live blocks claiming one position.
//   * The list is keyed per object across buckets; the walk is scoped to ONE `routing_bucket`.
//
// AND A COUNTER IS NOT THE WAY OUT: this walk reads the blocks rather than a counter precisely so
// the value survives a restart with nothing persisted for it, and `block_index_handle`'s doc
// records a counter having lost an object on reload, silently.
//
// THE RECORD HEADER DOES HOLD IT -- `BlockRecordHeader` carries `block_id` durably -- but reading
// it there turns one in-memory maximum into one disk read per block of the object, on the append
// path of `feature` and all six `context_*` kinds. That is the trade this step would be, and it is
// a serving-path cost paid to save eight bytes of resident width.
//
// # THE SIBLINGS FOLLOW, SO HALF A STEP PAYS NOTHING
//
// Keeping any one of the remaining two as its own field on this entry costs the whole eight bytes
// back. The small-field group is EXACTLY FULL at eight bytes -- `kind` 1, `routing_bucket` 4,
// `model_id` 1, `dirty` 1, `deleted` 1 -- and it is still exactly full after the element name
// left, because that field was never in it. So a `block_id: u16` beside it spills into a second
// word and the entry measures 40 again, with 34 bytes of field instead of 32. A `length: u32` does
// the same. That is the recorded shape of this whole address: the word sheds eight bytes and the
// holder sheds none.
//
// THE NUMBERS IN THIS PARAGRAPH MOVED WITH THE PROBE AND THE CLAIM DID NOT. It read "the entry
// measures 56 again, with 50 bytes of field instead of 56", against a probe at 48 and a live entry
// at 56. Against a probe at 32 and a live entry at 40 the same spill reads 40 and 34 -- the step
// is still a whole word given back, which is the whole point of the paragraph.
//
// # AND THE HANDLE'S UNIQUENESS ARGUMENT DOES NOT SURVIVE DROPPING `length`
//
// `block_index_handle` hashes five address terms, and the reading that frees it is that the bare
// word IS the physical page, so slab and offset alone still tell two entries apart. THE SITE SAYS
// OTHERWISE. `storage_bucket_internals::emit_one_entry_a_page` dedups on the TRIPLE
// `(block_slab_id, offset, length)`, and its own note states the safety direction: deduping on
// that narrow triple is safe only because the handle hashes a WIDER tuple, so the set "can only
// ever emit FEWER entries than the handle is able to tell apart, never more". Dropping `length`
// from the handle takes the handle NARROWER than the dedup, which inverts that direction -- two
// entries that differ only in length survive the dedup and then collide on one handle, and a
// handle collision returns a DIFFERENT page rather than failing. That is the recorded
// 39-of-40 element loss. `validate_bucket_ownership_index_from_entries` identifies a page by the
// same triple plus `block_id`, so nothing in this tree treats slab and offset as a page identity.
//
// Dropping `block_id` and `generation` from the handle alone WOULD stay on the safe side of that
// argument -- both are terms the dedup does not use -- but it moves `block_index_written_key`,
// which is the stored spelling, and the handle is written to disk inside the lookup's refs.
//
// # AND THE FIELD IS NOT SERIALIZABLE AS A BARE WORD
//
// `BlockAddress` derives no `Serialize` and no `Deserialize`, and that is deliberate rather than an
// oversight: its own doc says the serde "lives THERE rather than here -- it has to, because the
// wire row spells all four and only the holder has all four in scope". `BlockIndex` IS the stored
// form of the page index, so the field cannot hold the bare word without the wire row giving up
// three terms durably.
const _: () = {
    #[allow(dead_code)]
    struct TheEntryHoldingOneAddressWord {
        model_id: crate::engine::storage_bucket_internals::StoredModelKind,
        address: crate::block_store::BlockAddress,
        dirty: bool,
        deleted: bool,
    }
    let field_sum = std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>()
        + std::mem::size_of::<crate::block_store::BlockAddress>()
        + std::mem::size_of::<bool>()
        + std::mem::size_of::<bool>();
    // THE PROBE LOST THE TERM THE LIVE ENTRY LOST, FOR THE SECOND TIME, AND THE NUMBER WENT
    // 48 -> 32 -> 16.
    //
    // It carried a `component: Option<Arc<str>>` and measured 48; that field left `BlockIndex` one
    // leg ago and the probe went to 32. It carried an `object_key: Arc<str>` and has now lost that
    // too, which takes it to 16. The probe's whole job is to answer "what would THIS entry measure
    // if its address field held the bare word", so keeping either term would have it measure a
    // structure that cannot exist here -- and the tie at the bottom of this block is what stops
    // that. It is the assertion that went red on the previous merge, and it is why the probe was
    // corrected rather than left describing the old entry.
    //
    // THE REFUSAL ABOVE IS UNTOUCHED BY ANY OF THIS, which is worth saying because only the width
    // moved. Three of the four terms the holder carries are read through this field; the census
    // that enumerates them counts READERS, not bytes, and the seven production readers of
    // `block_id` are the same seven. What this branch changed is the number the step would reach,
    // not whether it can be taken.
    //
    // THE TWO NUMBERS THE STEP IS ABOUT, both asserted and neither inferred from the other.
    assert!(field_sum == 11);
    assert!(std::mem::size_of::<TheEntryHoldingOneAddressWord>() == 16);
    // AND THE SLACK IS FIVE, WHICH IT HAS BEEN THROUGH EVERY CORRECTION OF THIS BLOCK.
    //
    // It once read "ZERO SLACK SURVIVES THE NARROWING: 32 is a multiple of eight, so the step is a
    // whole word and not a word of field plus some rounding", asserted as
    // `size_of::<probe>() == field_sum`. That was true of a probe carrying `kind` and
    // `routing_bucket`, which left `BlockIndex` in `65c7fc562` -- and the width did not move when
    // they went, because 32 and 27 both round to 32, so the width pin and the tie both went on
    // passing while the DECOMPOSITION was false. Eleven rounds to sixteen and the slack is five
    // again.
    //
    // THAT THE SLACK IS FIVE AT 40, AT 32, AT 24 AND AT 16 IS NOT A COINCIDENCE WORTH TRUSTING:
    // it is three bytes of tail -- a one-byte model spelling and two flags -- rounding to a word
    // in every one of those shapes. It is asserted rather than inferred for the same reason the
    // width is.
    assert!(std::mem::size_of::<TheEntryHoldingOneAddressWord>() - field_sum == 5);
    // TIED TO `BlockIndex` ITSELF, so this cannot go on describing a type that moved. The real
    // entry must equal the probe plus exactly what the holder carries over the bare word.
    //
    // THE EXPRESSION IS UNCHANGED AND IT IS THE PROBE THAT MOVED, FOR THE THIRD TIME.
    // 16 + (16 - 8) is 24, which is what the live entry measures; it was 32 + (16 - 8) = 40 before
    // this leg and 48 + (16 - 8) = 56 before the one before. So the tie has now held across three
    // different pairs of widths with no term touched, which is what a reconstruction is for.
    //
    // AND IT IS A TIE BETWEEN TWO WIDTHS, so it could not see the retired fields: the probe's
    // width was 32 whether or not `kind` and `routing_bucket` were declared on it. It cannot
    // check a decomposition, only a relation between two of them. Recorded here rather than left
    // as an apparent success.
    assert!(
        std::mem::size_of::<BlockIndex>()
            == std::mem::size_of::<TheEntryHoldingOneAddressWord>()
                + (std::mem::size_of::<ElementEntry>()
                    - std::mem::size_of::<crate::block_store::BlockAddress>())
    );
};

// WHERE THIS LEG ENDS IF THE REMAINING NARROWINGS LAND: FIFTEEN OF FIELD IN SIXTEEN.
//
// THREE LANES WERE NAMED AND ONE HAS LANDED, so this reads as two. It said "Three lanes have to
// land for this shape to exist, and this one owns NONE of them: the address field holding the bare
// word (refused above, with its reasons), `component` leaving, and `object_key` becoming an ordinal
// rather than a shared name." The element name HAS left `BlockIndex` on this branch -- the entry is
// 40 -- so what stands between the live entry and this shape is the address word and the object
// key. The probe below is unchanged: it was already written without an element name, which is why
// its own three assertions survived the merge untouched while the two that tie back to the live
// entry did not.
//
// It is pinned here so that whoever lands the last of them finds the number already measured
// rather than arriving at it by adjusting an assertion until it passed.
//
// # THE SLACK AT THE END OF THE LEG IS ONE BYTE, AND BOTH EARLIER ANSWERS WERE WRONG
//
// This was scoped as "20 of field, 24 wide, zero slack", then corrected to "20 of field, 24 wide,
// FOUR of slack". Both were arithmetic over a field set that included `kind` and `routing_bucket`,
// and `65c7fc562` retired both from `BlockIndex`. The probe below is the live field set with the
// two remaining narrowings applied, and it measures FIFTEEN BYTES OF FIELD IN SIXTEEN: one byte of
// slack, not four, at a width of sixteen, not twenty-four.
//
// SO THE END OF THIS LEG IS A WORD LOWER THAN RECORDED. 24 was the number to beat; it is the
// number the FIRST of the two narrowings reaches on its own -- the identity leaving, 19 of field
// in 24 with five of slack -- and the address word takes it to 16.
//
// IT STILL INVERTS THE RULE THIS STRUCTURE IS GOVERNED BY, and by more than was thought. The
// block above asserted FIVE bytes of slack at 40, not zero -- it pins the live entry at 24 now, and
// the slack there is five again -- so the next field added here up to five bytes wide already costs
// nothing; at 16 it would be one. Every field decision taken here while the entry was believed to be exactly full has
// to be re-priced rather than inherited.
const _: () = {
    // THIS PROBE HAS FOLDED INTO THE ONE ABOVE, AND THE IDENTITY LEAVING IS WHAT DID IT.
    //
    // It declared `object_key: u32` -- the object key as an ORDINAL rather than a shared name --
    // and measured the shape at the end of the leg: 15 bytes of field in 16, slack one. The
    // ordinal was the narrowing it was waiting for; what happened instead is that the key LEFT,
    // so the shape at the end of the leg has no object term at all and is field for field the
    // `TheEntryHoldingOneAddressWord` declared thirty lines above: 11 of field in 16, slack five.
    //
    // TWO PROBES OF ONE SHAPE ARE TWO THINGS TO KEEP IN STEP, and the block above has already
    // recorded what that costs -- `TheEntryWithoutAnElementName` stood below it for exactly this
    // reason and was folded in for exactly this reason. So the declaration is gone and what is
    // kept here is the half this block alone held: the live entry's own sum and slack, and the
    // relation between the live entry and the end of the leg.
    //
    // THE NUMBERS IT PUBLISHED ARE NOT SILENTLY DROPPED. "15 of field in 16, slack one" was
    // arithmetic about an ordinal that is not being added; the end of the leg is 11 of field in
    // 16 with five of slack, asserted above. A reader who comes looking for the one-byte slack
    // should find this paragraph rather than the assertion.
    // TIED TO `BlockIndex` ITSELF, through its field sum rather than its width -- the widths
    // differ by a rounding and the field sums differ by exactly the narrowings.
    //
    // AND THIS TERM LIST WAS THE WORST OF THE THREE STALE ONES, because it claims to BE the live
    // entry. It read `IndexItemKind + u32 + Arc<str> + StoredModelKind + ElementEntry + bool +
    // bool` -- seven terms summing to 40 -- and asserted `live_field_sum == size_of::<BlockIndex>()`
    // under a note saying "it holds only while the live entry has zero slack, so a field added to
    // or removed from `BlockIndex` breaks this block". TWO FIELDS WERE REMOVED FROM `BlockIndex`
    // AND IT DID NOT BREAK: the live entry fell to 35 bytes of field, the retired terms were worth
    // exactly the five bytes of slack that opened up, and 40 == 40 went on passing. It was
    // arithmetic about two fields that do not exist.
    //
    // SO THE SUM IS THE LIVE FOUR TERMS AND THE RELATION IS A ROUNDING, not an equality. The live
    // entry holds 19 bytes of field in 24, and an equality between a field sum and a width can
    // only be written for a field set that happens to pack exactly -- which is a coincidence to
    // assert, not an invariant.
    // THE END OF THE LEG'S OWN SUM, RECOMPUTED HERE RATHER THAN READ OFF A SECOND STRUCT.
    //
    // It used to come from `TheEntryAtTheEndOfTheNarrowings`, which is folded away: the shape is
    // field for field `TheEntryHoldingOneAddressWord` above. The sum is spelled from the TYPES so
    // the relation below is still a reconstruction, and it is asserted on its own line so that a
    // term whose width moves fails here instead of inside the relation.
    let field_sum = std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>()
        + std::mem::size_of::<crate::block_store::BlockAddress>()
        + std::mem::size_of::<bool>()
        + std::mem::size_of::<bool>();
    assert!(field_sum == 11);
    let live_field_sum =
        std::mem::size_of::<crate::engine::storage_bucket_internals::StoredModelKind>()
            + std::mem::size_of::<ElementEntry>()
            + std::mem::size_of::<bool>()
            + std::mem::size_of::<bool>();
    assert!(live_field_sum == 19);
    assert!(std::mem::size_of::<BlockIndex>() == (live_field_sum + 7) / 8 * 8);
    assert!(std::mem::size_of::<BlockIndex>() - live_field_sum == 5);
    // AND THE DELTA IS ONE NOW, NOT TWO AND NOT THREE. This read `field_sum +
    // (ElementEntry - BlockAddress) + size_of::<Option<Arc<str>>>() + (Arc<str> - u32)`, naming
    // the three narrowings that stood between the live entry and the end of the leg. Two have
    // landed: the element name left one leg ago and the identity left on this one. Naming either
    // here would double-count it.
    //
    // WHAT IS LEFT BETWEEN 19 OF FIELD AND 11 IS THE ADDRESS WORD AND NOTHING ELSE -- eight
    // bytes, the one narrowing this structure still has in front of it, and the one its own block
    // above refuses on three measured grounds rather than on width.
    //
    // SO THE SHAPE OF THIS ASSERTION CHANGED AND THAT IS THE POINT. It used to survive each leg
    // untouched because the retired terms appeared on BOTH sides and cancelled -- which is also
    // why it could never have caught the staleness. The `(Arc<str> - u32)` term does not cancel:
    // it was the ordinal narrowing, and with the key gone there is no object term on either side
    // for it to describe. Removing it is the whole of the fold.
    assert!(
        live_field_sum
            == field_sum
                + (std::mem::size_of::<ElementEntry>()
                    - std::mem::size_of::<crate::block_store::BlockAddress>())
    );
};

// THE DEFERRED DECISION, WITH A TRIGGER RATHER THAN A NOTE.
//
// This type holds the address DIRECTLY instead of the shared payload value the row
// used to carry -- the value whose whole point was that the meaning of where a page
// lives is defined in ONE place, shared with the model map's level-2 value. Holding
// it directly means two places can describe that and DRIFT APART, and this tree has
// a recorded case of two copies that agreed, were both silent on a path, and could
// not be found by comparing them.
//
// WHY IT WAS GIVEN UP. The shared value is `Option`-shaped on the row because absence
// is a DECODE-side requirement: the wire type already carries an optional address for
// rows written before it existed, and the fold already skips an address-less row.
// Every resident reader sits where an address is always present -- 25 of 25
// constructions name one kind, and every absent-address construction in the tree is a
// test fixture. Carrying the Option inward would have put an absence that CANNOT
// OCCUR into 203 resident call sites, as 203 `expect`s on a hot path (a new panic
// surface) or as 203 signatures propagating it outward.
//
// Nothing here collapses an absent address into a defaulted one. Absence stays on the
// wire type where the tolerance is needed, which is where it already lives.
//
// THE TRIGGER. The two are the same width today, so the sharing costs nothing to give
// up. The moment the shared value grows -- its own doc says its end state is one
// packed flags byte over the address -- this goes RED, and whoever hits it has the
// argument above in front of them and has to DECIDE the sharing question rather than
// inherit the answer.
const _: () = assert!(
    std::mem::size_of::<crate::block_store::ElementEntry>()
        == std::mem::size_of::<ElementEntry>()
);

impl BlockIndex {
    /// Whether this block still lives in the log rather than in a slab, DERIVED from the address.
    ///
    /// `BlockAddress::block_id()` is `None` exactly while the block is log-resident, so the address
    /// already answers this and a stored flag could only agree or disagree with it. It did both:
    /// three sites derived it (`storage_bucket_internals.rs:362`, `:3655`, `:3983`) and three wrote
    /// a constant `false` without looking (`:3524`, `:5873`, `state.rs`'s own fixture).
    ///
    /// The disagreement was observable in one place and not in the obvious one. The behavioural
    /// reader -- `object_manager`'s hot/cold classification -- tests `deleted` FIRST, and the
    /// production site that writes the inconsistent flag is the container tombstone, which sets
    /// `deleted: true`; so that reader never reached the flag for exactly the entries where it was
    /// wrong. THAT PROTECTION IS THE BRANCH ORDER OF ONE `if`, not a property of the design, which
    /// is why this is a fix and not a tidy-up: reorder those branches and the defect becomes live.
    ///
    /// What it did cost was reporting. `storage_reporting` computed "any page in log" from the
    /// stored flag, so a bucket whose only log-resident page was a tombstone reported no page in
    /// log while one was in the log. Derived, it reports what is true.
    pub(super) fn log_backed(&self) -> bool {
        self.address.block_id().is_none()
    }

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
    pub(super) fn object_id(&self, shard_id: crate::types::ShardId, object_key: &str) -> u64 {
        crate::engine::hashing::stable_block_object_id(
            shard_id,
            self.model_id.as_str(),
            object_key,
        )
    }
}

impl CoreIndex {
    /// THE LOOKUP ALREADY DESCRIBES THE PAGES, CHECKED WITHOUT REBUILDING IT.
    ///
    /// This is what replaces a `rebuild_object_block_lookup()` at a site whose filing now files
    /// the lookup as it goes. The rebuild is an O(blocks) whole-shard pass; the claim it was
    /// standing in for is that the lookup and the page index agree, and that is checkable without
    /// doing the work. So in a release build the pass is simply gone, and in a debug build the
    /// claim is verified on every run instead of being asserted in a comment.
    ///
    /// DELETED PAGES ARE NOT IN EITHER SIDE. `insert_object_block_lookup` returns early on a
    /// deleted page, so the reconstruction below applies the same filter -- which is also why a
    /// tombstone refile owes the lookup nothing and why the two sites this serves can do their
    /// tombstone pass after their live pass with no filing in between.
    ///
    // AND IT WAS SPENT, EXACTLY AS ITS OWN DOC SAID IT WOULD BE. The paragraph that stood here
    // read: "It reads `page.object_key`, so it is one of the readers that has to go when the
    // identity leaves the entry -- at which point its only possible source would be the lookup it
    // is checking, and the check would be comparing the lookup with itself. It exists to earn the
    // removal of the two rebuild calls, and it is spent once the field is gone."
    //
    // THE FIELD IS GONE AND `debug_assert_lookup_matches_the_pages` IS DELETED, with
    // `lookup_positions_for_comparison`, which had no other caller. It is not pointed at the
    // surviving source for the reason it gave itself: there is one source now, so any version of
    // it would compare that source with itself and pass on every store including a damaged one.
    //
    // WHAT IT BOUGHT AND WHERE THAT OBLIGATION SITS NOW. It licensed the removal of two
    // whole-shard `rebuild_object_block_lookup()` calls on the load path by verifying, on every
    // debug run of every test, that filing-as-you-go leaves the lookup exactly where a rebuild
    // would have. The obligation moved to the producers: every site that places a page files it,
    // every site that drops one un-files it, and `fold_delta_block_items` carries the note that
    // those two are ONE obligation.
    //
    // WHAT IS LOST IS THE WHOLE-INDEX CROSS-CHECK, and that is stated rather than papered over.
    // The checkable half that survives is the converse -- every position the lookup names holds a
    // live page -- and it is NOT written here, because a new assertion has to be driven red
    // before it is worth anything and this commit is not the place to drive one. It is the next
    // guard this area owes.
    //
    // `rebuild_object_block_lookup` GOES WITH THEM, AND IT IS THE SAME ARGUMENT ONE LEVEL UP. It
    // rebuilt the lookup by walking the pages and reading each one's key -- so with the key off
    // the entry there is nothing to rebuild FROM. The load path does not need it: the decode
    // returns `BucketNodeWithPageKeys`, the identities beside the pages, and files as it decodes.
    // It had no production caller left and six test callers, each of which either compared the
    // maintained lookup against a fresh rebuild -- an oracle with only one side now -- or used it
    // to establish a lookup the write path already establishes.

    /// Takes the handle the block index filed this block under, so the two cannot name different
    /// things. It used to take the rendered key by shared pointer; the key is a number now and
    /// costs nothing to copy.
    pub(super) fn insert_object_block_lookup(
        &mut self,
        routing_bucket: u32,
        block_ref_key: u64,
        // THE IDENTITY, PASSED IN. The lookup is keyed by it and the entry is about to stop
        // carrying it, so it arrives from the caller -- which always has it, because filing a page
        // is something done TO an object.
        object_key: &Arc<str>,
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
            let value = BlockLookupRef {
                routing_bucket,
                block_ref_key,
            };
            // THE LOOKUP IS KEYED ON A NAME THE ENTRY DOES NOT HAVE, so every page of an object
            // files under the SAME single slot -- which is what it already did, because an entry's
            // component was `None` for every kind before the field was removed.
            //
            // THAT SLOT USED TO SIT UNDER THREE WRAPPERS. `ObjectBlockRefs` -> `ComponentList` ->
            // `ComponentBlocks` -> `BlockRefs` was three wrappers in front of the 24 that holds
            // the answer, and all of it existed to map a caller-supplied ELEMENT NAME onto blocks.
            // THE WRAPPERS COST NOTHING -- all three were the same 40 bytes, nested inline -- and
            // the 16 the step recovers is the always-`None` name inside the innermost one.
            // This was the only site in the crate that ever built a `ComponentBlocks`, and
            // the name it wrote was a literal `None`. All three wrappers are gone and the object
            // maps straight to its refs -- see [`ObjectBlockLookup`] on what each one carried, and
            // on the one piece that had to stay, because the component ARGUMENT on the readers and
            // the removers still comes from callers and is still sometimes `Some`.
            self.object_block_lookup.file(&kind, object_key, value)
        };
        if added {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_add(1);
            }
        }
    }

    /// EVERY PAGE'S OBJECT IDENTITY, INDEXED BY THE POSITION THE PAGE IS HELD AT.
    ///
    /// Built once by a reader that walks pages and needs to know which object each belongs to --
    /// see [`PageKeysByPosition`] for what it costs and [`PageKeysByPosition::identity_of`] for
    /// the two sources a page can be named by. A reader that starts FROM an object wants
    /// [`Self::block_refs_for`] instead and should not build this.
    pub(super) fn page_identities(&self) -> PageKeysByPosition<'_> {
        PageKeysByPosition::from_lookup(&self.object_block_lookup)
    }

    /// Every block ref this object holds, for one component.
    ///
    /// A named component resolves nothing and never did -- see [`names_the_only_slot`].
    pub(super) fn block_refs_for(
        &self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) -> Option<&[BlockLookupRef]> {
        if !names_the_only_slot(component) {
            return None;
        }
        self.object_block_lookup
            .get(model_id, object_key)
            .map(BlockRefs::as_slice)
    }

    /// Every block ref of this object, whatever its component.
    pub(super) fn object_block_refs(
        &self,
        model_id: &str,
        object_key: &str,
    ) -> Option<&BlockRefs> {
        self.object_block_lookup.get(model_id, object_key)
    }

    pub(super) fn remove_object_block_lookup_entry(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
    ) {

        // ONE SLOT IS THIS OBJECT'S ENTIRE SET OF REFS, so dropping the slot is dropping the
        // object's entry. What this replaces had to seek a range with an empty-string sentinel and
        // take_while on the component, because the per-component map flattened every component of
        // an object into one ordered set -- so a component's refs could only be found by range,
        // not by index. Nesting deleted the range; the collapse deleted the index too, because
        // there is only ever the one slot.
        //
        // A NAMED COMPONENT REMOVES NOTHING, which is what the binary search over a list of one
        // already did -- see [`names_the_only_slot`].
        if !names_the_only_slot(component) {
            return;
        }
        let removed = self
            .object_block_lookup
            .remove(model_id, object_key)
            // By value, so the closure form: `remove` hands back an owned `BlockRefs`.
            .map_or(0, |refs| refs.len());
        if removed > 0 {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_sub(removed);
            }
        }
    }

    /// Drop ONE ref an object holds under one component, leaving that component's other refs and
    /// every other component alone.
    ///
    /// # WHY THIS EXISTS WHEN THE WHOLE-SLOT REMOVAL ABOVE ALREADY DID
    ///
    /// The whole-slot removal is right when an entry's identity is its ELEMENT: a new filing for
    /// that element supersedes the previous one and nothing else shares the slot. It is wrong when
    /// the identity is the PHYSICAL PAGE, which is what one entry per page makes it -- then every
    /// entry of an object shares ONE slot, keyed `None`, and dropping the slot to file one page's
    /// entry takes every other page of the object with it.
    ///
    /// MEASURED, so the cost of not having this is on the record rather than argued: filing a
    /// page-identity entry through the whole-slot removal, on a five-member set folded onto one
    /// page and then written to once, left the listing serving ONE member -- the one just written --
    /// and orphaned four. `write_after_fold` holds that shape.
    ///
    /// # THE SLOT GOES WHEN ITS LAST REF DOES
    ///
    /// An empty component slot is not the same as an absent one, and the difference is load-bearing
    /// rather than tidiness. `position` finds an empty slot, `refs_for` answers `Some(&[])` for it,
    /// and the upsert's third case -- "the lookup is established and names NOTHING for this
    /// component", which is exactly the re-add after a removal -- is distinguished by `refs_for`
    /// answering `None`. Leaving an empty slot behind would send that case down the wrong branch
    /// and leak the tombstone the re-add is supposed to clear.
    ///
    /// Returns whether a ref was removed, so a caller cannot mistake "nothing was there" for
    /// "something was dropped" -- and the ref counter is advanced from that answer and not from an
    /// assumption that the call did something.
    pub(super) fn remove_object_block_lookup_ref(
        &mut self,
        model_id: &str,
        object_key: &str,
        component: Option<&str>,
        block_ref: &BlockLookupRef,
    ) -> bool {
        // A NAMED COMPONENT REMOVES NOTHING -- see [`names_the_only_slot`].
        if !names_the_only_slot(component) {
            return false;
        }
        let mut removed = false;
        let mut now_empty = false;
        if let Some(refs) = self.object_block_lookup.get_mut(model_id, object_key) {
            removed = refs.remove(block_ref);
            // THE SLOT GOES WHEN ITS LAST REF DOES, AND THE EMPTY STATE IS NOT REPRESENTABLE
            // ABOVE ANY MORE. `BlockRefs::remove` turns the inline arm into an empty spilled one
            // rather than panicking, and this is the single holder of the contract that the state
            // is transient -- which is why the object's removal is decided in the same breath.
            now_empty = refs.is_empty();
        }
        // The object's whole entry goes when its last ref does: an object present in the lookup
        // holding nothing is a `get` that succeeds and answers nothing.
        if now_empty {
            self.object_block_lookup.remove(model_id, object_key);
        }
        if removed {
            if let Some(total) = self.object_component_block_refs.as_mut() {
                *total = total.saturating_sub(1);
            }
        }
        removed
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
        let removed = entry.len();
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
        address: &ElementEntry,
    ) -> bool {
        if let Some(block_refs) = self.block_refs_for(model_id, object_key, component) {
            return block_refs.iter().any(|block_ref| {
                self.bucket_map
                    .get(&block_ref.routing_bucket)
                    .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
                    .map(|page| {
                        !page.deleted
                            && page.model_id.as_str() == model_id
                            // THE IDENTITY IS NOT RE-COMPARED. This read
                            // `&*page.object_key == object_key` against a page reached through
                            // THAT object's own ref list, so it asked whether the lookup was
                            // right rather than which object the page belongs to.
                            //
                            // The entry carries no element name; `component.is_none()` is the old
                            // `page.component.as_deref() == component` with that constant folded
                            // in, so the kinds this answers for do not move.
                            && component.is_none()
                            && same_block_address(&page.address, address)
                    })
                    .unwrap_or(false)
            });
        }

        // AND NO SECOND ARM. A whole-shard walk comparing every entry's identity stood here for
        // the case where the lookup held nothing, which is an index with pages and no lookup --
        // the window between a decode and the rebuild that used to fill it. The decode fills it
        // now, so the object having no entry in the lookup means the object has no live page.
        false
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
    let Some(bucket) = bucket_index.bucket_map.get(&routing_bucket) else {
        return 0;
    };

    // THE IDENTITY STILL COMES FROM BESIDE THE BLOCK, FROM BOTH SOURCES -- BUT ASKED OF THE
    // OBJECT RATHER THAN OF THE SHARD.
    //
    // A block id is an ordinal INSIDE its object and must never be reused: this store keeps a
    // rewritten block's predecessor live, so an object numbering from zero again would have two
    // live blocks claiming one position. So the highest ordinal has to be taken over the object's
    // LIVE pages and its TOMBSTONED ones together -- the lookup holds live pages only
    // (`insert_object_block_lookup` returns early on a deleted one), and selecting on it alone
    // would step back over a deleted block's ordinal and hand it out twice.
    //
    // WHAT THIS REPLACED ASKED THE WHOLE SHARD TO ANSWER FOR ONE OBJECT. It built
    // `PageKeysByPosition::from_lookup` over the ENTIRE object lookup -- two `HashMap`s, one
    // insert per block ref of every object in the shard -- and then walked this bucket's whole
    // page index, calling `identity_of` on every page, to choose one integer. Both terms are
    // sized by the shard and neither is sized by the object, so an append to a one-page object in
    // a large shard paid for every other object in it.
    //
    // MEASURED, BEFORE: 475,584 bytes per call on a 3,200-block shard -- 14.5% of a feature
    // append, and after #2232 declared that command's upsert components it is the dominant
    // remaining term. The same call is on the context-event path, where
    // `what_one_context_event_append_costs_at_two_series_lengths` recorded a flat allocation count
    // with bytes still climbing 2.66x over an 8x series: the signature of a buffer sized by
    // something other than the entries held.
    //
    // THE ANSWER IS THE SAME ONE, reached from the object's own two neighbours.
    let live_highest = bucket_index
        .object_block_refs(model_id, object_key)
        .map(BlockRefs::as_slice)
        .unwrap_or_default()
        .iter()
        // The refs span whatever buckets the object is filed in; this asks about one bucket, as
        // the walk it replaces did -- it only ever saw `bucket.block_index`.
        .filter(|block_ref| block_ref.routing_bucket == routing_bucket)
        .filter_map(|block_ref| bucket.block_index.get(&block_ref.block_ref_key))
        // `model_id` is re-tested because one handle is reachable from one ref, and the ref came
        // from this object's slot under this kind -- but the page it names is the authority on
        // what kind it is, which is what the walk this replaces tested.
        .filter(|block| !block.deleted && block.model_id.as_str() == model_id)
        .filter_map(|block| block.address.block_id())
        .max();

    // THE DELETED HALF, AND WHY IT IS STILL A WALK. A tombstone row names its page by the triple
    // `(slab, offset, length)` and that triple carries NO ordinal, so the rows cannot answer this
    // on their own -- the ordinal lives on the deleted page still sitting in the page index. What
    // makes the walk affordable is that it is now CONDITIONAL: a bucket holding no tombstone row
    // at all cannot hold a tombstoned page of this object, and `TombstoneElements` is `None` in
    // the overwhelming majority of buckets -- its own doc measures the case it is in at 2.32%. An
    // append-only object never reaches this arm.
    //
    // It also no longer builds the shard-wide position map even when it does run: a deleted
    // page's object is stated by the tombstone row, which is the only source this half ever
    // needed.
    let deleted_highest = if bucket.tombstone_elements.is_empty() {
        None
    } else {
        bucket
            .block_index
            .iter()
            .filter(|(_, block)| block.deleted && block.model_id.as_str() == model_id)
            .filter(|(_, block)| {
                bucket
                    .tombstone_element_at(&block.address)
                    .is_some_and(|row| &*row.object_key == object_key)
            })
            .filter_map(|(_, block)| block.address.block_id())
            .max()
    };

    // A page whose removal named no element states its object nowhere, so it is counted by
    // neither half -- exactly as `identity_of` answering `None` left it uncounted before.
    match live_highest.max(deleted_highest) {
        Some(highest) => u32::try_from(highest).unwrap_or(u32::MAX).saturating_add(1),
        None => 0,
    }
}

/// THE ADDRESS THE RESIDENT MAP HOLDS FOR ONE ELEMENT, found by the component the index spells it
/// under.
///
/// # ONE SPELLING TABLE, READ BY EVERY SITE THAT NEEDS AN ELEMENT'S IDENTITY
///
/// Three things in this engine need to know WHICH element a write, a removal or a tombstone is
/// about, and none of them can ask the index any more: the filing supersede term, the tombstone
/// sweep, and the ordinal allocator below. They all have a component STRING in hand -- the
/// rendering the index used to be filed under -- and the resident maps are keyed by the element
/// itself, so each of them needs the same four-arm decode. It is written here once rather than
/// three times, because several copies of a condition is how most of them come to agree and the
/// rest do not, which this series has already hit with two filers disagreeing.
///
/// `hex::encode` for a set member and for a zset member, base sixteen plus the two's-complement
/// bias for a list sequence, the field itself for a hash -- each the SAME rendering the write path
/// files. A component this cannot parse answers `None` rather than defaulting to the empty member
/// or to sequence zero: an unreadable name names nothing, which is the rule every arm of the
/// derived view already follows, and the two defaults it replaces are both real elements whose
/// address it would have taken.
pub(super) fn resident_component_address<'a>(
    shard: &'a ShardState,
    model_id: &str,
    object_key: &str,
    component: &str,
) -> Option<&'a ElementEntry> {
    match model_id {
        "hash" => shard.hashes.get(object_key)?.get(component),
        "set" => {
            let member = hex::decode(component).ok()?;
            shard.sets.get(object_key)?.get(&member)
        }
        "zset" => {
            let member = hex::decode(component).ok()?;
            shard
                .zsets
                .get(object_key)?
                .get(&member)
                .map(|(_, address)| address)
        }
        "list" => {
            let biased = u64::from_str_radix(component, 16).ok()?;
            let sequence = biased.wrapping_add(i64::MIN as u64) as i64;
            shard.lists.get(object_key)?.get(&sequence)
        }
        // NOT A CONTAINER. Only the four container kinds have a level-two element at all, so there
        // is nothing for any other kind to be resident under and `None` is the whole answer.
        _ => None,
    }
}

/// The highest block ordinal any LIVE element of this object holds.
///
/// NO COMPONENT IS RENDERED HERE, deliberately: the question is about ORDINALS, and rendering every
/// sibling's name back to a string to ask it would allocate once per element of the object on every
/// container write. The addresses carry the ordinals and the map's keys are not read at all.
pub(super) fn resident_highest_block_id(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Option<u64> {
    fn highest<'a>(addresses: impl Iterator<Item = &'a ElementEntry>) -> Option<u64> {
        addresses.filter_map(ElementEntry::block_id).max()
    }
    match model_id {
        "hash" => highest(shard.hashes.get(object_key)?.values()),
        "set" => highest(shard.sets.get(object_key)?.values()),
        "zset" => highest(
            shard
                .zsets
                .get(object_key)?
                .values()
                .map(|(_, address)| address),
        ),
        "list" => highest(shard.lists.get(object_key)?.values()),
        _ => None,
    }
}

/// THE ORDINAL A CONTAINER PAGE IS FILED UNDER: STABLE ACROSS AN OVERWRITE, UNASSIGNED PAST THE
/// CEILING.
///
/// `next_block_index_for_object` above is the same derivation for the TIMESTAMPED kinds, read off
/// the index entries, and it stays there: those kinds have no level-two element and so no resident
/// map keyed by one. This is the container form, and it differs from the series one in exactly two
/// ways, both forced:
///
///   * AN OVERWRITE KEEPS THE ORDINAL IT ALREADY HAS. A container element is addressed by its
///     component, and `HashSet` or `SetAdd` on an existing member REPLACES that member's block.
///     Handing the replacement `max + 1` would make the ordinal climb once per WRITE rather than
///     once per ELEMENT, so a single member rewritten 65,536 times would reach the ceiling on a set
///     of one. Reading the element's own block first bounds the ordinal by the object's live
///     element high-water mark instead, which is what a position means.
///   * PAST THE CEILING IT LEAVES THE ORDINAL AT `0` RATHER THAN PANICKING OR SATURATING.
///     `narrow_block_id` refuses a value above `MAX_ADDRESSABLE_BLOCK_ID`, and refusing is right
///     for a value a caller chose -- but an object's 65,536th element is not a caller's mistake, it
///     is a container this store serves today with no ordinal at all. Saturating is worse still:
///     this tree's own doctrine is that a saturated block id is a legal block id for a DIFFERENT
///     block of the same object. So past the ceiling nothing is assigned and the block keeps the
///     `0` it would have had before any of this, which makes the ordinal a strict no-op for such
///     an object. A container past the ceiling loses the ordinal, never the element.
///
/// IT NAMES A POSITION AMONG AN OBJECT'S LIVE BLOCKS, NOT AN ELEMENT. `max` falls after a delete and
/// the next insert is handed the ordinal that was just freed. That is correct for a position and
/// would be silent corruption for an identity, which is why nothing here reads the ordinal to find
/// a row.
///
/// # IT ASKS THE RESIDENT MAP NOW, NOT THE INDEX ENTRIES, AND THE FILTER IT NEEDED IS GONE WITH THEM
///
/// This walked `bucket.block_index` and compared `page.component` to pick out the element's own
/// block. That was the THIRD consumer of a per-element name on an index entry, after the filing
/// supersede term and the tombstone sweep -- and the one that is not about removals at all, which
/// is why it was the hardest of the three to see. An entry under one entry a page carries no
/// element name, so the comparison matches nothing and EVERY overwrite falls through to `max + 1`:
/// measured as `an_overwrite_of_a_component_less_hash_entry_reuses_its_ordinal` failing at round one
/// with `[0]` against `[1]`.
///
/// THE RESIDENT MAP ANSWERS BOTH HALVES AND IS THE ONLY PLACE THAT CAN. It is keyed BY the element,
/// so the overwrite's own address is a lookup; and it holds exactly the object's LIVE elements, so
/// the high-water mark it reports is over live blocks by construction.
///
/// THAT RETIRES THE `deleted` FILTER THIS WALK NEEDED, AND THE TWO HAZARDS THE FILTER EXISTED FOR
/// ARE NOW UNREPRESENTABLE RATHER THAN GUARDED. A container removal leaves a TOMBSTONE ENTRY behind
/// so the page recording it stays reachable, and that broke the old derivation in both directions at
/// once: `max` stopped FALLING after a delete, so the ordinal climbed once per element ever written
/// and a container churning distinct members walked to the ceiling for a set that never held more
/// than a handful; and the component match found the TOMBSTONE, so a re-add was handed the dead
/// block's position. Both were about a deleted entry being visible to a walk over entries. A removal
/// drops its element from the resident map, so neither question arises here.
///
/// AND IT IS ASKED BEFORE THE APPEND, WHICH IS WHAT MAKES THE LOOKUP THE RIGHT ANSWER. Every caller
/// computes this before writing the page, because the append stamps the ordinal into the record
/// header AND onto the address it returns from one value and `decode_block_record` refuses a page
/// whose two copies disagree. So the resident map still holds the element's PREVIOUS address here --
/// which is exactly the address whose ordinal an overwrite must keep.
///
/// # THE BUCKET IS NOT A PARAMETER ANY MORE
///
/// It took a `routing_bucket` to pick the bucket to walk. An object's elements are all in the
/// resident map under one key, so there is no bucket to choose, and leaving the argument in place
/// would have let a caller believe the answer depended on it.
///
/// `element_ordinal_reuse` is the suite that measures the reuse.
pub(super) fn container_page_ordinal(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: &str,
) -> u32 {
    if let Some(held) = resident_component_address(shard, model_id, object_key, component)
        .and_then(ElementEntry::block_id)
    {
        // This element already holds a block, and the ordinal on it IS its position. An overwrite
        // is the same element in the same place.
        return u32::try_from(held).unwrap_or(0);
    }
    // The first element of an object is 0, which is also what an object past the ceiling keeps.
    let next = resident_highest_block_id(shard, model_id, object_key)
        .map_or(0, |held| held.saturating_add(1));
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

fn same_block_address(left: &ElementEntry, right: &ElementEntry) -> bool {
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
    //! WHAT HAPPENED TO THE NINE ARMS THIS MODULE HELD, AND WHAT THE EIGHT HERE NOW ARE, so the
    //! collapse is auditable rather than merely smaller. Four of the nine were deleted outright and
    //! two more lost their names to a restatement, and neither a deletion nor a rename leaves
    //! anything for a reviewer to read. The arithmetic is 9 - 4 deleted + 3 ADDED = 8, and every
    //! term is named below; an earlier version of this note said "six of them were deleted", which
    //! counted the two renames as deletions and did not account for the arms added at all.
    //!
    //! GONE, SUBJECT UNREPRESENTABLE -- there is one slot per object now, so there is no second
    //! component to order against, none to sort first, and no sibling to disturb:
    //!
    //!   * `removing_one_component_leaves_the_others`
    //!   * `removing_the_first_and_last_components_works`
    //!   * `a_none_component_is_removable_and_does_not_take_the_others`
    //!   * `every_ref_sharing_a_component_goes` -- its surviving half, that a whole-slot removal
    //!     takes every ref the slot held, is carried by
    //!     `the_running_ref_total_is_decremented_by_what_the_slot_held` below.
    //!
    //! RESTATED, SUBJECT MOVED:
    //!
    //!   * `removing_an_absent_component_changes_nothing` becomes
    //!     `a_removal_by_a_named_component_is_a_no_op`. Same call; the subject is now the
    //!     predicate rather than a binary search that ran off the front of a list.
    //!   * `emptying_the_set_drops_the_key_entirely` becomes
    //!     `dropping_the_only_slot_drops_the_objects_key`, with `Some("only")` replaced by `None`,
    //!     because a removal by a named component is a no-op and the old form would assert over a
    //!     removal that correctly did nothing.
    //!
    //! KEPT, FULL TEETH -- three of them, and their claim is that the hand-maintained
    //! `object_component_block_refs` counter agrees with the refs actually present, which is
    //! independent of how many slots exist. Their FIXTURES still had to move from `Some(x)` to
    //! `None`: left as they were, each one's first `assert!(index.remove_...)` would fail on a
    //! removal that correctly did nothing.
    //!
    //!   * `a_single_ref_removal_keeps_the_counter_equal_to_what_is_present`
    //!   * `the_last_ref_of_the_slot_takes_the_slot_and_the_objects_key`
    //!   * `the_running_ref_total_is_decremented_by_what_the_slot_held`
    //!
    //! ADDED -- three, and they are why the module is eight arms and not six. The first two came
    //! with the collapse and the third with the merge that joined this branch to the element-name
    //! one:
    //!
    //!   * `the_insert_accumulates_an_objects_pages_into_the_one_slot` -- the half of the filing
    //!     tripwire that HAS a discriminator, since the half about what a page carried cannot fail
    //!     here. See the note on [`page`] for why.
    //!   * `a_named_component_resolves_nothing_while_the_unnamed_one_resolves_everything` -- the
    //!     `names_the_only_slot` predicate, asserted BOTH WAYS in one arm so that loosening it
    //!     cannot satisfy a one-sided check.
    //!   * `the_refs_in_the_only_slot_stay_sorted_for_the_search_that_removes_them` -- carried out
    //!     of `components_of_one_object_stay_separate` in `part4.rs`, which the collapse deleted.
    //!     The order claim it held belongs to `BlockRefs`, whose `insert` keeps the refs sorted and
    //!     whose `remove` binary-searches them, and nothing else in the tree asserted it.

    use super::*;

    /// A block carrying nothing but the identity the lookup keys on.
    ///
    /// THE COMPONENT PARAMETER THIS USED TO TAKE IS GONE, AND IT WAS ALREADY DEAD. It was accepted
    /// and then never mentioned in the `BlockIndex` built below, because the field it used to fill
    /// came off the entry in this branch's previous commit. Keeping it would make the arms below
    /// read as though they varied something they did not.
    ///
    /// IT IS ALSO WHY ONE TRIPWIRE IS NOT WRITTEN HERE. "The insert files every page under one
    /// slot WHATEVER THE PAGE CARRIED" cannot fail against this tree: no page can carry an element
    /// name to file, so the clause after "whatever" has no values to range over and the assertion
    /// would pass on any insert at all. The half that can still move is asserted instead -- see
    /// [`the_insert_accumulates_an_objects_pages_into_the_one_slot`].
    /// AND THE `object` PARAMETER IS REMOVED RATHER THAN RENAMED `_object`, which is the
    /// treatment `entry_count_versus_page_count::page_at` already took for a dead `component`
    /// argument and for the same reason: a caller that still tries to name an object here should
    /// be a compile error and not a silent no-op. The entry has no field for it; the callers pass
    /// the key to `insert_object_block_lookup` themselves.
    fn page() -> BlockIndex {
        BlockIndex {
            model_id: crate::engine::storage_bucket_internals::StoredModelKind::Hash,
            address: ElementEntry::from_parts(0, 0, 0, None, Some(0)),
            dirty: false,
            deleted: false,
        }
    }

    /// EVERY ROUTE BY WHICH A `CoreIndex` COMES INTO EXISTENCE REPORTS A REF TOTAL OF ZERO.
    ///
    /// `None` on this field means "nobody has established this count", and an increment against it
    /// is dropped on purpose -- so an index built by a route that leaves it `None` can never
    /// arrive at a right answer by filing. The claim is therefore about the ROUTES, not about one
    /// constructor, and the routes are the ones the compiler named when `Default` came off the
    /// derive: two explicit sites, and three that no site list could have covered.
    ///
    /// THE THIRD ROUTE IS THE ONE THAT MATTERS AND IT IS A REAL FILE. `bucket_index` carries
    /// `#[serde(default)]`, so an index whose stored bytes have no `slot_index` key decodes
    /// through `Default` and never reaches the hand-written `Deserialize` that sets zero. That
    /// arm is driven here by TAKING THE KEY OUT of a serialized shard rather than by hand-writing
    /// JSON, so the input is the shape this engine actually writes minus one key.
    ///
    /// AND BOTH DECODE PATHS ARE ASSERTED, which is what keeps this from passing on half the
    /// claim: an index WITH the key must come back established too, or the arm above would be
    /// satisfied by a decoder that established nothing and a `Default` that did all the work.
    ///
    /// rust-internal: index construction invariant, no product behaviour
    #[test]
    fn a_default_index_reports_a_ref_total_of_zero_by_every_route_one_is_built() {
        assert_eq!(
            Some(0),
            CoreIndex::default().object_component_block_refs,
            "the explicit route left the ref total unestablished"
        );
        assert_eq!(
            Some(0),
            ShardState::default().bucket_index.object_component_block_refs,
            "a default shard's index left the ref total unestablished, so every route through \
             `ShardState::default()` starts with a counter no filing can correct"
        );

        // --- The serde(default) route: the same bytes this engine writes, minus the one key. ---
        let mut stored = serde_json::to_value(ShardState::default())
            .expect("a default shard serializes");
        let object = stored
            .as_object_mut()
            .expect("a serialized shard is a JSON object");
        assert!(
            object.remove("slot_index").is_some(),
            "the serialized shard carried no `slot_index` key, so removing it is not the arm this \
             test means to drive and the decode below would prove nothing"
        );
        let without_the_key: ShardState =
            serde_json::from_value(stored).expect("a shard without the index key decodes");
        assert_eq!(
            Some(0),
            without_the_key.bucket_index.object_component_block_refs,
            "a stored index with no `slot_index` key decoded to a counter no filing can correct, \
             which is the route a site list cannot cover"
        );

        // --- And the decode path that DOES carry the key, so this is not half a claim. ---
        let with_the_key: ShardState = serde_json::from_value(
            serde_json::to_value(ShardState::default()).expect("a default shard serializes"),
        )
        .expect("a shard with the index key decodes");
        assert_eq!(
            Some(0),
            with_the_key.bucket_index.object_component_block_refs,
            "the decoder left the ref total unestablished on an index that carried the key"
        );
    }

    /// Built through the real insert rather than assembled by hand, and that is deliberate rather
    /// than convenient: these arms turn on what the insert does with an object's second and third
    /// page, and a fixture that imitates it can agree with an insert that has stopped doing it.
    ///
    /// Each page gets its own `(routing_bucket, block_ref_key)` -- `(i, i)` -- so `pages` DISTINCT
    /// refs go in, and a count below is not a count of one ref filed repeatedly.
    fn core_with(object: &str, pages: u32) -> CoreIndex {
        let index = CoreIndex::default();
        // THE COUNTER IS ESTABLISHED BY THE TYPE NOW, AND THIS FIXTURE USED TO DO IT BY HAND.
        //
        // It read "`object_component_block_refs` is `None` on a default index, and `None` means
        // UNKNOWN rather than zero", and set `Some(0)` itself with a note that three arms here had
        // asserted `Some(n)` without the line and read back `None`. `impl Default for CoreIndex`
        // writes that zero now, so the line was a no-op -- and a no-op that STILL COMPILED and
        // still read as the thing establishing the state, which is the shape this tree keeps a
        // record of. Removed rather than left, so the arms below depend on the type's claim and go
        // red with it if anyone takes it away.
        //
        // The reasoning itself has not changed and has only moved: `insert_object_block_lookup`
        // increments only `if let Some(total)`, so a count that starts at "unknown" cannot be
        // incremented into a right answer.
        let mut index = index;
        let object_key: Arc<str> = Arc::from(object);
        for i in 0..pages {
            let filed = page();
            index.insert_object_block_lookup(i, u64::from(i), &object_key, &filed);
        }
        index
    }

    fn refs_under_the_only_slot(index: &CoreIndex, object: &str) -> usize {
        index
            .block_refs_for("hash", object, None)
            .map_or(0, <[BlockLookupRef]>::len)
    }

    /// THE HALF OF THE TRIPWIRE THAT HAS A DISCRIMINATOR, and it can still fail.
    ///
    /// Three inserts for one object must ACCUMULATE into the one slot, which is `BlockRefs::One`
    /// growing into `Many` under [`ObjectBlockLookup::file`]. The count is what makes this bite: a
    /// `file` that replaced instead of inserting -- the obvious way to write it once there is only
    /// one slot, and what the whole-slot removal next door does -- would leave ONE ref here, and
    /// every other arm in this module, which asks whether something is there rather than how much,
    /// would still pass.
    #[test]
    fn the_insert_accumulates_an_objects_pages_into_the_one_slot() {
        let index = core_with("k", 3);
        assert_eq!(
            3,
            refs_under_the_only_slot(&index, "k"),
            "three pages of one object did not accumulate in its slot"
        );
        assert_eq!(
            Some(3),
            index.object_component_block_refs,
            "the ref counter disagrees with the three refs that went in"
        );
        // AND ONE SLOT HOLDS ALL THREE. Not implied by the line above: a structure that kept three
        // slots of one ref each would answer 3 to a walk over the object and 1 to the slot read.
        assert_eq!(
            3,
            index
                .object_block_refs("hash", "k")
                .map_or(0, BlockRefs::len),
            "the object's refs are not all reachable through its single slot"
        );
    }
    /// THE REFS IN THE ONE SLOT STAY SORTED, WHICH IS THE ORDER `remove` SEARCHES.
    ///
    /// CARRIED FROM AN ARM THE COLLAPSE DELETED, in the merge that joined the two branches.
    /// `components_of_one_object_stay_separate` in `part4.rs` asserted "removal binary-searches
    /// this vector, so its order is load-bearing" of the SECOND LEVEL's vector, and the
    /// element-name branch's rewrite of that arm moved the claim down onto the refs. The level is
    /// gone and the rewrite with it -- it reads `by_component` and `refs_for(..)`, neither of which
    /// exists -- but the claim belongs here: [`BlockRefs::insert`] keeps the refs sorted and
    /// [`BlockRefs::remove`] binary-searches them, so an `insert` rewritten to append, which is the
    /// obvious way to add to a `Vec`, would leave every other arm in this module green and make
    /// `remove` miss a ref that is present. Nothing else in the tree asserted it.
    ///
    /// THE FIXTURE INSERTS DESCENDING ON PURPOSE. Fed ascending refs, an append and a sorted insert
    /// produce the SAME vector and this arm could not tell them apart. [`core_with`] ascends, so
    /// this builds its own rather than reusing it.
    ///
    /// AND IT COMPARES WHOLE REFS. `BlockLookupRef` derives `Ord` over `(routing_bucket,
    /// block_ref_key)`, so sorting a projection onto either field alone is a DIFFERENT order that
    /// agrees with this one only while the two fields ascend together. The version on the
    /// element-name branch projected onto `block_ref_key`, which is why it is restated here rather
    /// than carried across.
    ///
    /// THE CONSEQUENCE IS ASSERTED AND NOT ONLY THE MECHANISM. The ref inserted LAST is the lowest,
    /// so an append would strand it at the end of an unsorted vector where `binary_search` cannot
    /// reach it; the removal below is the question `remove` actually asks.
    #[test]
    fn the_refs_in_the_only_slot_stay_sorted_for_the_search_that_removes_them() {
        const PAGES: u32 = 4;
        let mut index = CoreIndex::default();
        // The counter must be established before the inserts, for the reason `core_with` gives.
        index.object_component_block_refs = Some(0);
        // DESCENDING: (3,3), (2,2), (1,1), (0,0).
        let object_key: Arc<str> = Arc::from("k");
        for i in (0..PAGES).rev() {
            let filed = page();
            index.insert_object_block_lookup(i, u64::from(i), &object_key, &filed);
        }

        let refs = index
            .object_block_refs("hash", "k")
            .expect("the object is in the lookup");
        assert_eq!(
            PAGES as usize,
            refs.len(),
            "the fixture filed {} refs of {PAGES}, so the order claim below has too little to              range over",
            refs.len()
        );
        let held = refs.as_slice();
        assert!(
            held.windows(2).all(|pair| pair[0] < pair[1]),
            "the refs in the slot are not strictly ascending, so `BlockRefs::remove`'s binary              search can miss a ref that is present: {held:?}"
        );

        // THE REF INSERTED LAST IS AT THE FRONT, which an append cannot achieve.
        let lowest = BlockLookupRef {
            routing_bucket: 0,
            block_ref_key: 0,
        };
        assert_eq!(
            Some(&lowest),
            held.first(),
            "the lowest ref went in last and is not at the front, so the insert is not placing by              order: {held:?}"
        );

        // AND `remove` FINDS IT. This is the failure an append-style insert actually produces: the
        // ref is present, `binary_search` looks for it in the wrong half, and the removal reports
        // that an object's live page was never filed.
        let mut slot = refs.clone();
        assert!(
            slot.remove(&lowest),
            "the slot holds {lowest:?} and `remove` did not find it"
        );
        assert_eq!(
            PAGES as usize - 1,
            slot.len(),
            "the removal took {} refs, not exactly one",
            PAGES as usize - slot.len()
        );
    }

    /// THE OTHER HALF: a lookup BY A NAMED COMPONENT resolves nothing, while the same lookup
    /// unnamed resolves everything.
    ///
    /// # WHAT CAN MAKE THIS FAIL, STATED PLAINLY BECAUSE IT IS NOT A STRUCTURAL DRIFT
    ///
    /// With the second level gone this is [`names_the_only_slot`]'s single
    /// predicate and nothing else's, so the only thing that can redden it is an edit to that
    /// predicate. It is kept anyway, and asserted BOTH WAYS in one arm, for two reasons. A
    /// one-sided check is satisfied on whichever side an edit moves -- loosening the predicate so
    /// every query hits would redden a `Some`-misses assertion and leave a `None`-hits assertion
    /// passing, and only the pair says which way it went. And the consequence of loosening it is
    /// not a bigger index: `bucket_store::bucket_index_block_address` passes a caller's component
    /// straight through, so a `Some` that started hitting would serve an object's whole page as
    /// the answer to a field lookup that has always, correctly, found nothing.
    #[test]
    fn a_named_component_resolves_nothing_while_the_unnamed_one_resolves_everything() {
        let index = core_with("k", 2);
        assert_eq!(
            None,
            index.block_refs_for("hash", "k", Some("f0")),
            "a lookup by element name resolved the object's pages"
        );
        assert_eq!(
            2,
            index
                .block_refs_for("hash", "k", None)
                .map_or(0, <[BlockLookupRef]>::len),
            "the unnamed lookup stopped resolving, so the assertion above passed over an empty \
             lookup rather than over a refused query"
        );
    }

    /// Restated from `removing_an_absent_component_changes_nothing`.
    #[test]
    fn a_removal_by_a_named_component_is_a_no_op() {
        let mut index = core_with("k", 2);
        index.remove_object_block_lookup_entry("hash", "k", Some("f0"));
        assert_eq!(
            2,
            refs_under_the_only_slot(&index, "k"),
            "a removal by element name took the object's pages with it"
        );
        assert_eq!(
            Some(2),
            index.object_component_block_refs,
            "the counter moved for a removal that removed nothing"
        );

        // AND THE SINGLE-REF REMOVER TOO, which is the other caller of the same predicate and the
        // one the upsert's supersede goes through.
        let first = BlockLookupRef {
            routing_bucket: 0,
            block_ref_key: 0,
        };
        assert!(
            !index.remove_object_block_lookup_ref("hash", "k", Some("f0"), &first),
            "a single-ref removal by element name reported a removal"
        );
        assert_eq!(2, refs_under_the_only_slot(&index, "k"));
    }

    /// Restated from `emptying_the_set_drops_the_key_entirely`, with `Some("only")` replaced by
    /// `None`.
    #[test]
    fn dropping_the_only_slot_drops_the_objects_key() {
        let mut index = core_with("k", 1);
        // FLOOR: the object is in the lookup to begin with, so the absence below is a removal and
        // not a fixture that never filed anything.
        assert!(
            index.object_block_refs("hash", "k").is_some(),
            "the fixture filed nothing, so the assertion below would hold vacuously"
        );
        index.remove_object_block_lookup_entry("hash", "k", None);
        assert!(
            index.object_block_refs("hash", "k").is_none(),
            "the object stayed in the lookup with no slot, which is a `get` that succeeds and \
             answers nothing"
        );
        assert_eq!(Some(0), index.object_component_block_refs);
    }

    /// THE COUNTER AGREES WITH THE REFS THAT ARE ACTUALLY THERE, after every single-ref removal.
    ///
    /// Recomputed from the structure rather than compared with a number written here. The failure
    /// this is for is a counter that drifts from the map it is supposed to describe, and an
    /// expected-number assertion cannot see that: it agrees with a counter that is wrong in the
    /// same direction as the expectation. `object_component_block_refs` is maintained BY HAND on
    /// every path that touches the lookup, which is exactly the shape that drifts.
    fn refs_actually_present(index: &CoreIndex) -> usize {
        index
            .object_block_lookup
            .values()
            .map(BlockRefs::len)
            .sum()
    }

    #[test]
    fn a_single_ref_removal_keeps_the_counter_equal_to_what_is_present() {
        let mut index = core_with("k", 3);
        index.object_component_block_refs = Some(refs_actually_present(&index));
        // FLOOR: there is something to remove, so the agreement below is not over an empty map.
        assert_eq!(
            3,
            refs_actually_present(&index),
            "the fixture holds {} refs, so this is not the three-ref shape the removals below \
             assume",
            refs_actually_present(&index)
        );

        // Each ref of `core_with` is (bucket i, key i) in declaration order.
        let b = BlockLookupRef {
            routing_bucket: 1,
            block_ref_key: 1,
        };
        assert!(
            index.remove_object_block_lookup_ref("hash", "k", None, &b),
            "the ref the fixture filed was not found, so every assertion below would hold over a \
             removal that did nothing"
        );
        assert_eq!(
            Some(refs_actually_present(&index)),
            index.object_component_block_refs,
            "the counter says {:?} and the lookup holds {}",
            index.object_component_block_refs,
            refs_actually_present(&index)
        );

        // REMOVING A REF THAT IS NOT THERE MUST NOT MOVE THE COUNTER. A removal that reports
        // nothing and decrements anyway is the drift this guard exists for, and it is silent.
        let before = index.object_component_block_refs;
        assert!(
            !index.remove_object_block_lookup_ref("hash", "k", None, &b),
            "removing the same ref twice reported a second removal"
        );
        assert_eq!(
            before, index.object_component_block_refs,
            "the counter moved for a removal that found nothing"
        );
        assert_eq!(
            Some(refs_actually_present(&index)),
            index.object_component_block_refs
        );
    }

    /// WHERE A COUNT AND A SLOT DISAGREE, restated from
    /// `the_last_ref_of_a_component_takes_the_slot_and_not_just_the_ref`.
    ///
    /// An empty slot is not an absent one: `refs_for` answered `Some(&[])` for the first and
    /// `None` for the second, and the upsert distinguishes its re-add case -- "established but
    /// names nothing for this object" -- by exactly that difference. A removal that left the slot
    /// behind would send the re-add down the wrong branch.
    ///
    /// THE COLLAPSE MADE THE EMPTY STATE UNREPRESENTABLE rather than merely wrong, because the
    /// slot is an `Option` and [`CoreIndex::remove_object_block_lookup_ref`] takes it in the statement that sees
    /// it empty. This arm keeps its teeth anyway, because what it asserts is the OBSERVABLE --
    /// that `block_refs_for` goes from `Some` to `None` and not to `Some(&[])` -- and that is a
    /// claim about the answer rather than about which type expresses it.
    #[test]
    fn the_last_ref_of_the_slot_takes_the_slot_and_the_objects_key() {
        let mut index = core_with("k", 2);
        index.object_component_block_refs = Some(refs_actually_present(&index));
        assert_eq!(
            2,
            refs_actually_present(&index),
            "the fixture is not the two-refs-in-one-slot shape this is about"
        );

        // First of the two: the slot stays, because it still holds one.
        assert!(index.remove_object_block_lookup_ref(
            "hash",
            "k",
            None,
            &BlockLookupRef {
                routing_bucket: 0,
                block_ref_key: 0
            },
        ));
        assert_eq!(
            Some(vec![BlockLookupRef {
                routing_bucket: 1,
                block_ref_key: 1
            }])
            .as_deref(),
            index.block_refs_for("hash", "k", None),
            "the slot's other ref did not survive its sibling's removal"
        );
        assert_eq!(
            Some(refs_actually_present(&index)),
            index.object_component_block_refs
        );

        // Second: the slot must GO, not become empty -- and with it the object's whole entry,
        // because an object present in the lookup with no slot is a `get` that succeeds and
        // answers nothing.
        assert!(index.remove_object_block_lookup_ref(
            "hash",
            "k",
            None,
            &BlockLookupRef {
                routing_bucket: 1,
                block_ref_key: 1
            },
        ));
        assert_eq!(
            None,
            index.block_refs_for("hash", "k", None),
            "the slot survived its last ref as an EMPTY slot, which reads as present"
        );
        assert!(
            index.object_block_refs("hash", "k").is_none(),
            "the object stayed in the lookup with no slot"
        );
        assert_eq!(Some(0), index.object_component_block_refs);
        assert_eq!(0, refs_actually_present(&index));
    }

    /// Restated from `the_running_ref_total_is_decremented_by_what_was_removed`, and it carries
    /// `every_ref_sharing_a_component_goes`'s surviving half: a whole-slot removal takes EVERY ref
    /// the slot held, not one of them. Three went in, so a decrement of three is the claim and a
    /// decrement of one is the failure it is for.
    #[test]
    fn the_running_ref_total_is_decremented_by_what_the_slot_held() {
        let mut index = core_with("k", 3);
        index.object_component_block_refs = Some(3);
        index.remove_object_block_lookup_entry("hash", "k", None);
        assert_eq!(
            Some(0),
            index.object_component_block_refs,
            "the whole-slot removal decremented by one ref rather than by the three it took"
        );
        assert_eq!(0, refs_actually_present(&index));
    }
}

// =================================================================================================
// REPACKING A DECODED INDEX
// =================================================================================================

/// How many entries fit in one B-tree leaf, so a map at or below this size cannot have split yet.
///
/// `std`'s `BTreeMap` leaf holds eleven entries whatever the key and value types are. A map that
/// never split has no half-empty node to recover, so repacking one would buy nothing and cost a
/// transient vector -- and a store holding one point per series would pay that per series. The
/// consequence of this number being wrong in a future `std` is a repack skipped or a pass wasted,
/// never a wrong answer: the repack below preserves contents and order whatever it is applied to.
const ENTRIES_IN_ONE_LEAF: usize = 11;

/// Rebuild `map` so its entries are packed rather than spread across half-empty leaves.
///
/// `collect` into a `BTreeMap` sorts the entries and builds the tree bottom-up, filling every leaf;
/// `insert` one at a time splits each full leaf in half and never returns to the left half. The
/// entries are MOVED through the intermediate vector, not cloned, so the only new allocation is
/// that vector.
pub(super) fn repack_btree_map<K: Ord, V>(map: &mut BTreeMap<K, V>) {
    if map.len() <= ENTRIES_IN_ONE_LEAF {
        return;
    }
    *map = std::mem::take(map).into_iter().collect();
}

/// The same, for a set.
fn repack_btree_set<T: Ord>(set: &mut BTreeSet<T>) {
    if set.len() <= ENTRIES_IN_ONE_LEAF {
        return;
    }
    *set = std::mem::take(set).into_iter().collect();
}

/// Every nested series of one model map.
fn repack_nested<MK, K: Ord, V>(map: &mut HashMap<MK, BTreeMap<K, V>>) {
    for series in map.values_mut() {
        repack_btree_map(series);
    }
}

/// Pack every B-tree a decode filled one entry at a time.
///
/// WHY THIS RIDES THE DECODE AND NOTHING ELSE. `serde`'s `Deserialize` for `BTreeMap` fills the map
/// with `insert` in a loop, and the bytes it reads were written in key order, so every nested series
/// in a decoded index is built by ASCENDING insertion -- the one insertion order that leaves a
/// B-tree half empty, because each full leaf splits and the left half is never filled again. The
/// load is therefore both the cause and the only moment at which the whole structure is in hand and
/// already paid for. There is no timer, no threshold and no flag: a decode that happens repacks,
/// and a decode that does not happen has nothing to repack.
///
/// WHY IT DESTRUCTURES THE WHOLE STRUCT. The set of maps this has to cover is not a list someone
/// keeps up to date, it is every field of `ShardState` -- so the field list is written out in full
/// with no `..`, and a field added to the struct is a COMPILE ERROR here until someone says which
/// of the two groups below it belongs to. A hand-kept list would go stale and nothing would fail.
///
/// WHAT IT COSTS, measured under the counting allocator at three corpus sizes: see
/// `what_a_cold_load_spends_decoding_an_index_at_three_corpus_sizes`. One O(n) pass inside a
/// function already O(n), moving entries rather than copying them.
/// Re-establish the key sharing that a decode undoes.
///
/// THE WRITE PATH SHARES AND `serde` DOES NOT. Every holder of an object key points at one
/// allocation while the shard is live, because each write takes the handle the page index already
/// holds. A decode builds each map independently, so `serde` hands every occurrence its own
/// allocation and a restored shard pays the duplication again -- permanently, since nothing after
/// the load would otherwise bring them back together. Measured at one allocation before a round
/// trip and two after, with the dirty set absent by design.
///
/// THE PAGE INDEX IS THE CANONICAL HOLDER, because it is what the write path reads from. Keys it
/// does not know keep whatever allocation they decoded with: that is the first-write case, where
/// there is nothing to adopt.
///
/// POINTER IDENTITY IS THE WHOLE POINT. The contents were always equal, so a contents comparison
/// cannot see this pass work or fail to.
///
/// ONLY AN ORDERED MAP CAN TAKE PART, AND A GUARD IS WHAT ESTABLISHED THAT. This pass runs inside a
/// function whose stored form must not move -- `a_decode_puts_back_exactly_the_series_that_were
/// _encoded` compares the bytes a repacked state encodes to against the bytes it came from, and
/// reddens with "the stored form moved and this needs an index-format stamp". A `BTreeMap` rebuilt
/// from its own entries serializes in the same key order, so swapping each key for a handle holding
/// the SAME CHARACTERS is byte-neutral. A `HashMap` is not: its serialized order follows iteration
/// order, so rebuilding `strings` here changed the encoded bytes and the guard caught it. A key in a
/// hashed map cannot be replaced in place either, so there is no byte-neutral way to do it in this
/// pass at all -- that map keeps its decoded allocation, and the restored duplication it pays is
/// stated here rather than left to be rediscovered.
/// The same adoption for a HASHED map, re-keyed IN PLACE.
///
/// THE REFUSAL ABOVE WAS ABOUT A REBUILD, NOT ABOUT A RE-KEY, and that distinction is the whole of
/// this function. Draining a `HashMap` into a fresh one changes its capacity and its insertion
/// order, so it changes the serialized bytes -- which is what happened when `strings` was rebuilt
/// in this pass and the format guard caught it. Swapping ONE entry's key for a different handle
/// holding the SAME CHARACTERS is a different operation: the hash is identical, so the probe
/// sequence is identical, and the `remove` leaves a tombstone that the following `insert` of that
/// same hash reclaims -- putting the entry back where it was.
///
/// Measured rather than argued, because the argument above is exactly the kind that sounds right
/// either way: `whether_rekeying_a_hashed_map_in_place_moves_its_bytes` re-keys 512 keys one at a
/// time and finds the encoding **byte-identical**, 9859 bytes before and after, with iteration
/// order unchanged. So this owes no index-format stamp, and the behaviour it restores is the
/// sharing a decode had been discarding -- 0 of 8 keys still shared across a reload before it,
/// measured by `whether_the_shared_key_survives_a_decode`.
fn adopt_canonical_key_allocations_in_hashed<V>(
    map: &mut HashMap<ModelKey, V>,
    lookup: &ObjectBlockLookup,
) -> usize {
    // The keys to move, collected first: the map cannot be read for a canonical handle and written
    // in the same breath, and re-keying while iterating is not something a `HashMap` allows.
    let moves: Vec<(ModelKey, ModelKey)> = map
        .keys()
        .filter_map(|held| {
            lookup
                .any_shared_object_key(held)
                .filter(|canonical| !Arc::ptr_eq(held, canonical))
                .map(|canonical| (Arc::clone(held), canonical))
        })
        .collect();

    let mut adopted = 0usize;
    for (held, canonical) in moves {
        if let Some(value) = map.remove(&*held) {
            map.insert(canonical, value);
            adopted += 1;
        }
    }
    adopted
}

fn adopt_canonical_key_allocations(
    expires_at_ms: &mut BTreeMap<Arc<str>, u64>,
    lookup: &ObjectBlockLookup,
) -> usize {
    let mut adopted = 0usize;

    let decoded = std::mem::take(expires_at_ms);
    for (key, value) in decoded {
        match lookup.any_shared_object_key(&key) {
            Some(canonical) => {
                adopted += 1;
                expires_at_ms.insert(canonical, value);
            }
            None => {
                expires_at_ms.insert(key, value);
            }
        }
    }

    adopted
}

/// HOW MANY TIMES THE DECODE PASS RAN, AND HOW MANY KEYS IT ADOPTED.
///
/// NOT diagnostic scaffolding -- `whether_the_shared_key_survives_a_dump_and_install` asserts on
/// the call count, and without it that test cannot tell a decode it reached from one it did not.
///
/// That distinction is why these exist. "The pass ran and found nothing to adopt" and "the pass
/// never ran" produce the SAME end-to-end figure, and an earlier change read the second as the
/// first: it attributed a reopen's lost sharing to this pass, which a reopen with no dump never
/// reaches at all. A probe that sees only the final count cannot separate those two, so the count
/// is exported and asserted instead of inferred.
pub(super) static REPACK_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
/// How many hash FIELD names a decode pointed at the pool's copy.
///
/// Counted because a sharing mechanism that is wired and never fires looks exactly like one
/// that works: the names are equal either way, every read answers, and only a pointer or this
/// counter can tell the two apart.
pub(super) static REPACK_ADOPTED_FIELDS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(super) static REPACK_ADOPTED_HASHED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(super) fn repack_decoded_btrees(state: &mut ShardState) {
    let ShardState {
        // ----- Packed here. Deserialized, and filled one entry at a time. -----
        wal_resident_blocks,
        expires_at_ms,
        sets,
        seen,
        zsets,
        lists,
        features,
        control_state,
        control_state_changes,
        context_event_timeline,
        context_indexes,
        context_audits,
        context_entities,
        context_children,
        context_summaries,
        context_compressions,

        // ----- Nothing to pack. Each for a stated reason, not by omission. -----
        //
        // Not a B-tree at all: a hashed table has no insertion-order shape, and `HashFieldMap`
        // keeps a sorted vector whose length IS its capacity below its growth threshold.
        //
        // `hashes` IS BOUND, AND NOT FOR PACKING -- there is still nothing in it to pack, for
        // the reason just given. It is bound because its FIELD NAMES come out of a decode as
        // freshly minted handles and have to be pointed at the pool; see the adoption below.
        strings,
        hashes,
        buckets: _,
        objects: _,
        object_page_layouts: _,
        // Not a B-tree at all: the owned sorted array map is `serde(skip)` and is filled by
        // `fold_legacy_context_events`. An array has no half-empty-leaf shape for this pass to
        // repair.
        context_events_series: _,
        // DECODED, AND DELIBERATELY NOT PACKED. This one IS a B-tree that serde filled by
        // ascending insert, so it is exactly what this pass is for -- but the fold runs
        // immediately after this function and DRAINS it, so packing it is a pass over data that is
        // about to be dropped. The only entry in this list whose absence makes a decode cheaper.
        context_events_compat: _,
        control_state_selection: _,
        control_state_uuid: _,
        context_nodes: _,
        context_dirty_index: _,
        context_compression_watermark: _,
        bucket_recency: _,
        // Not deserialized: `#[serde(skip)]`, so a decode leaves it default-empty and there is
        // nothing in it to pack. `expiry_by_deadline` is the one of these that IS refilled by
        // ascending insertion -- from `expires_at_ms`, which is itself in key order -- but that
        // refill happens in the expiry path rather than here, and is left to it.
        buckets_pending_flag_refresh: _,
        expiry_by_deadline: _,
        control_state_rollups: _,
        control_coalesce_persist: _,
        control_distinct_sketch: _,
        feature_values: _,
        feature_rollups: _,
        dirty_objects: _,
        promote_scan_done: _,
        evict_sampler: _,
        routing_range_start: _,
        routing_range_end: _,
        routing_range_known: _,
        shard_id: _,
        shard_id_known: _,
        applied_wal_sequence: _,
        index_format_version: _,
        // The bucket index's own containers are owned elsewhere and are deliberately left alone
        // here, so this pass cannot be blamed for a change in them.
        bucket_index,
    } = state;

    // THE KEY ALLOCATIONS, BROUGHT BACK TOGETHER. The same reasoning as the packing above: the
    // decode is both the cause and the only moment at which the whole structure is in hand.
    let _adopted =
        adopt_canonical_key_allocations(expires_at_ms, &bucket_index.object_block_lookup);
    // AND THE HASHED MAPS, which this pass used to leave with their decoded allocations. See
    // `adopt_canonical_key_allocations_in_hashed`: an in-place re-key is byte-neutral, measured,
    // so there is no format stamp owed and no reason to leave the duplication behind.
    REPACK_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _adopted_hashed = adopt_canonical_key_allocations_in_hashed(
        strings,
        &bucket_index.object_block_lookup,
    ) + adopt_canonical_key_allocations_in_hashed(
        features,
        &bucket_index.object_block_lookup,
    ) + adopt_canonical_key_allocations_in_hashed(
        control_state,
        &bucket_index.object_block_lookup,
    );
    REPACK_ADOPTED_HASHED.fetch_add(_adopted_hashed, std::sync::atomic::Ordering::Relaxed);
    // AND THE HASH FIELD NAMES. The wire shape is a map of owned names, so the decode minted a
    // handle per field with the pool taking no part; without this a reloaded shard holds the
    // duplication the conversion removed and nothing fails, because the names are equal.
    let _adopted_fields = hashes.adopt_element_names(&mut bucket_index.kind_pool);
    REPACK_ADOPTED_FIELDS.fetch_add(_adopted_fields, std::sync::atomic::Ordering::Relaxed);

    repack_btree_map(wal_resident_blocks);
    repack_btree_map(expires_at_ms);
    sets.repack_decoded();
    zsets.repack_decoded();
    lists.repack_decoded();
    // `features` is an owned sorted array now, built by `collect` at decode, so there is no
    // half-empty B-tree node to repack and nothing here to do for it.
    let _ = &features;
    repack_nested(control_state);
    repack_nested(context_event_timeline);
    // `context_indexes` is an owned sorted array now, built by `collect` at decode exactly as
    // `features` above is, so there is no half-empty B-tree node for this pass to repack.
    let _ = &context_indexes;
    // FOUR OF THESE FIVE ARE OWNED SORTED ARRAYS NOW, built by `collect` at decode exactly as
    // `features` and `context_indexes` above are, so there is no half-empty B-tree node for this
    // pass to repack in any of them. `context_entities` is the one that remains: its inner key is
    // an ENTITY HASH rather than a timestamp, so it is a different structure and keeps its nodes.
    let _ = (
        &context_audits,
        &context_children,
        &context_summaries,
        &context_compressions,
    );
    repack_nested(context_entities);
    // Two levels: the series, and the member set each EXACT entry carries. A sketched bucket is
    // a fixed-size register array with nothing to repack, which is why this matches rather than
    // calling `repack_btree_set` on every bucket.
    for series in control_state_changes.values_mut() {
        repack_btree_map(series);
        for bucket in series.values_mut() {
            if let super::hll::DistinctBucket::Exact(members) = bucket {
                repack_btree_set(members);
            }
        }
    }
    for window in seen.values_mut() {
        repack_btree_map(&mut window.by_member);
        repack_btree_map(&mut window.by_time);
    }
}

// =================================================================================================
// THE CONSOLIDATED MAP, AND THE COMPATIBILITY KEY IT HAS TO KEEP WRITING FOR NOW
// =================================================================================================

/// The empty map a kind with no objects answers with, so the reader below can hand out a borrow
/// without the caller learning whether the kind has a bucket yet.
static NO_OBJECTS_OF_THIS_KIND: std::sync::OnceLock<HashMap<ModelKey, super::object_record::ObjectSlot>> =
    std::sync::OnceLock::new();

impl ShardState {
    /// THE OBJECTS OF ONE KIND. The read half of the consolidated map.
    ///
    /// Answers an empty map for a kind that holds nothing, rather than an `Option`, because every
    /// caller of this wants to look a key up or iterate -- and "this kind has no bucket yet" is
    /// not a distinction any of them should have to carry.
    pub(super) fn objects_of_kind(
        &self,
        kind: super::storage_bucket_internals::StoredModelKind,
    ) -> &HashMap<ModelKey, super::object_record::ObjectSlot> {
        self.objects
            .get(&kind)
            .unwrap_or_else(|| NO_OBJECTS_OF_THIS_KIND.get_or_init(HashMap::new))
    }

    /// The control-state blocks, read from the consolidated map.
    ///
    /// A METHOD AND NOT A FIELD, AND IT STAYS ONE NOW THAT THE SECOND COPY IS GONE.
    ///
    /// Its original reason was that a compatibility key held a second live copy and two copies
    /// that can disagree is worse than paying for both. That copy left with the format stamp that
    /// removed it, so that reason has expired -- and the method is kept for a different one that
    /// did not: the consolidated map is keyed by KIND first, so a writer assigning to a field
    /// would have to pick the inner table itself, and picking the wrong kind writes a control
    /// state block under someone else's kind where nothing would notice. The named door picks the
    /// kind once.
    pub(super) fn control_state_blocks(
        &self,
    ) -> &HashMap<ModelKey, super::object_record::ObjectSlot> {
        self.objects_of_kind(super::storage_bucket_internals::StoredModelKind::ControlState)
    }

    /// Install one control-state block, under the control-state kind.
    pub(super) fn insert_control_state_block(
        &mut self,
        key: ModelKey,
        slot: super::object_record::ObjectSlot,
    ) -> Option<super::object_record::ObjectSlot> {
        self.objects
            .entry(super::storage_bucket_internals::StoredModelKind::ControlState)
            .or_default()
            .insert(key, slot)
    }

    /// Remove one control-state block.
    ///
    /// A NAMED METHOD BESIDE ITS INSERT, STILL, FOR A NARROWER REASON THAN IT HAD. It existed
    /// because a mirrored pair diverges on the remove side more often than the insert side, and
    /// the mirror is gone. What is left is that the removal has to reach INTO the kind's inner
    /// table, and a call site doing that by hand is a call site that can name the wrong kind.
    pub(super) fn remove_control_state_block(
        &mut self,
        key: &str,
    ) -> Option<super::object_record::ObjectSlot> {
        self.objects
            .get_mut(&super::storage_bucket_internals::StoredModelKind::ControlState)
            .and_then(|objects| objects.remove(key))
    }
}

impl ShardState {
    /// Apply a change to one control-state slot.
    ///
    /// A CLOSURE AND NOT A `get_mut`, WHICH NOW COSTS NOTHING AND IS KEPT ANYWAY. Its reason was
    /// that a caller holding `&mut` to one copy mutates it while the mirror never learns; with the
    /// mirror gone there is nothing to fall out of step. What the closure still buys is that the
    /// two lookups this needs -- the kind, then the key -- happen in one place, so a caller cannot
    /// hold a borrow into the wrong kind's table.
    pub(super) fn with_control_state_block_mut<R>(
        &mut self,
        key: &str,
        change: impl FnOnce(&mut super::object_record::ObjectSlot) -> R,
    ) -> Option<R> {
        let slot = self
            .objects
            .get_mut(&super::storage_bucket_internals::StoredModelKind::ControlState)?
            .get_mut(key)?;
        Some(change(slot))
    }

    /// Replace EVERY control-state block.
    ///
    /// The reconcile path rebuilds this map wholesale from the bucket index rather than editing
    /// it, so it needs a whole-map write. It used to be the write that would leave a mirror
    /// holding every key the rebuild dropped -- ghost entries arriving all at once -- and with the
    /// mirror gone it is simply the whole-map door, which replaces the kind's inner table rather
    /// than merging into it.
    pub(super) fn set_all_control_state_blocks(
        &mut self,
        blocks: HashMap<ModelKey, super::object_record::ObjectSlot>,
    ) {
        self.objects
            .insert(super::storage_bucket_internals::StoredModelKind::ControlState, blocks);
    }

}

#[cfg(test)]
mod the_dirty_index_shares_the_page_indexs_key {
    use super::{DirtyObjectIndex, ObjectBlockLookup};
    use std::sync::Arc;

    /// ONE COPY OF THE CHARACTERS, ASSERTED BY POINTER.
    ///
    /// `insert_shared` must store the handle it was given, not a copy of its text. A contents
    /// comparison cannot witness this -- the two were always equal -- so both arms compare
    /// addresses, and the second arm exists to prove the first one CAN fail.
    #[test]
    fn a_handed_in_key_is_stored_and_an_unshared_one_is_not() {
        let shared: Arc<str> = Arc::from("ctx:event:4071:915");

        // SHARED ARM: the handle goes in, and the index holds that very allocation.
        let mut handed = DirtyObjectIndex::default();
        handed.insert_shared(Arc::clone(&shared), 7);
        assert_eq!(
            handed.key_ptr("ctx:event:4071:915"),
            Some(shared.as_ptr()),
            "insert_shared stored a copy of the text instead of the handle it was given"
        );

        // CONTROL ARM: the old path allocates its own, so the same assertion must FAIL here.
        // Without this, a key_ptr that always answered `shared.as_ptr()` would pass above.
        let mut owned = DirtyObjectIndex::default();
        owned.insert("ctx:event:4071:915", 7);
        assert_ne!(
            owned.key_ptr("ctx:event:4071:915"),
            Some(shared.as_ptr()),
            "the unshared path returned the handed-in allocation, so this test cannot fail"
        );

        // Both arms hold a key whose TEXT is identical -- which is exactly why the assertions
        // above compare addresses and not contents.
        assert_eq!(handed.key_ptr("ctx:event:4071:915").is_some(), true);
        assert_eq!(owned.key_ptr("ctx:event:4071:915").is_some(), true);
    }

    /// The kind-agnostic reader answers from any kind, and answers `None` before the object has
    /// blocks -- which is the call that must still allocate.
    #[test]
    fn the_lookup_answers_for_any_kind_and_none_before_the_first_block() {
        let lookup = ObjectBlockLookup::default();
        assert!(
            lookup.any_shared_object_key("never:filed").is_none(),
            "an object with no blocks must answer None so the caller allocates"
        );
    }
}

// =================================================================================================
// THE EVENT SERIES ON THE OWNED ARRAY, AND THE DECODE-ONLY LANDING SPOT BESIDE IT
// =================================================================================================
//
// WHAT THIS IS NOT, AND WHY THE DIFFERENCE IS THE WHOLE POINT. An earlier revision of this change
// kept the compatibility field as a LIVE MIRROR: every mutator wrote both copies and the fold
// refilled it on exit. That shape was deleted here rather than shipped, and not on grounds of
// bytes.
//
// It was deleted because nothing outside this file ever read that field and it was never
// serialized, so THE ONLY CONSUMER WAS THE FOLD THAT DRAINS IT -- the dual write bought nothing at
// all. And it cost something real: a bulk operation under a mirror needs a bulk mutator that
// re-mirrors ON EXIT, so between entry and exit the two copies disagree and a concurrent reader can
// be served the stale one. An agreement predicate called after every step cannot see INSIDE a step.
// Shipping that as the template would have propagated the window to `context_indexes`, the one
// other map whose `skip_serializing` lets it follow this shape at all.
//
// So: one live map. The compatibility field receives from a decode and is drained to EMPTY, and no
// mutator touches it. With one live copy there is nothing to diverge, which retires the mirror, the
// agreement predicate, its negative control, and the bulk mutators that existed only to re-mirror.

impl ShardState {
    /// The event series of every object key. The read half.
    ///
    /// A METHOD AND NOT A FIELD, and the reason is the census rather than enforcement: making the
    /// read a method turned every existing access into a compile error, so the compiler enumerated
    /// the call sites and separated them -- a read needs only `()` appended, while a write cannot
    /// compile at all because nothing can be assigned through a method call. That is what caught a
    /// write going through the read accessor in this very change. A name grep does not find a
    /// four-line delegating helper; the compiler does.
    pub(super) fn context_events(&self) -> &HashMap<ModelKey, OwnedTimestampedSeries> {
        &self.context_events_series
    }

    /// The event series, mutably.
    ///
    /// HANDING OUT `&mut` IS SAFE, AND THAT IS WHAT THE DECODE-ONLY SHAPE BUYS. Under a live mirror
    /// this could not exist: a caller holding `&mut` to one of two copies mutates it and the mirror
    /// never learns, so a mirrored map has to take its changes as closures and re-mirror on exit --
    /// which is the bulk-operation window this shape does not have. With one live copy a mutable
    /// borrow cannot create a divergence, because there is nothing to diverge from, and the generic
    /// helpers over `TimestampedSeriesWrite` can be handed this map directly.
    pub(super) fn context_events_mut(
        &mut self,
    ) -> &mut HashMap<ModelKey, OwnedTimestampedSeries> {
        &mut self.context_events_series
    }

    /// Install a whole series for one key.
    ///
    /// Takes the `BTreeMap` because its callers have one: the key-state fold deserializes the
    /// stored shape, which is still the B-tree. The array is built from it here, so the conversion
    /// happens in one place rather than at each caller.
    pub(super) fn set_context_event_series(
        &mut self,
        object_key: &str,
        series: BTreeMap<u64, ElementEntry>,
    ) {
        self.context_events_series
            .insert(ModelKey::from(object_key), OwnedTimestampedSeries::from(&series));
    }

    /// Point every `ModelKey`-keyed per-kind map at the page index's OWN key allocation.
    ///
    /// THE MAPS ARE LISTED HERE AND NOWHERE ELSE, which is the point of naming them at one site: a
    /// map converted to `ModelKey` and not added below keeps allocating its own equal copy of every
    /// key, and nothing fails -- the duplicate is invisible to a clone-based byte probe, because
    /// cloning an `Arc` is a refcount bump. The pointer-identity probes are what catch it.
    ///
    /// CALLED FROM `engine.rs` AFTER THE BUCKET INDEX IS FILLED, not from the write arms. The write
    /// files its maps while the page index does not yet hold the key, so an adoption at the write
    /// site finds no handle to take -- counted, not assumed: the first attempt at the event pair
    /// adopted 0 of 400 keys from there.
    pub(super) fn adopt_object_key_allocations(&mut self, object_key: &str) -> bool {
        CONTEXT_EVENT_ADOPT_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(canonical) = self
            .bucket_index
            .object_block_lookup
            .any_shared_object_key(object_key)
        else {
            CONTEXT_EVENT_ADOPT_NO_HANDLE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return false;
        };
        let mut moved = false;
        // The event pair moves together or not at all: they are keyed by one handle by
        // construction, so moving one and not the other restores the second allocation.
        moved |= adopt_key_in(&mut self.context_events_series, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_event_timeline, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_indexes, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_audits, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_children, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_summaries, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_compressions, object_key, &canonical);
        moved |= adopt_key_in(&mut self.context_entities, object_key, &canonical);
        // `context_nodes` was ALREADY `ModelKey`-keyed before this change and was still
        // missing from this list, which is the failure the note above describes: a map that
        // CAN share and is not listed here keeps its own equal copy and nothing fails.
        moved |= adopt_key_in(&mut self.context_nodes, object_key, &canonical);
        // Keyed by the EVENT object key, so it is another holder of a key the event pair
        // already holds. Found only by grepping from the crate root: the write is a
        // multi-line `insert` in `engine.rs`, which a search rooted at `engine/` misses.
        moved |= adopt_key_in(&mut self.context_compression_watermark, object_key, &canonical);
        // NOT CONTEXT MAPS, AND THEY BELONG HERE ANYWAY. A raw key written as a string, a
        // feature series and a selection lands in all three of those maps -- measured, 64 of 64
        // keys in `why_there_are_many_per_key_maps_and_not_one` -- and only `strings` could
        // share, so the other two each owned a second and third copy of the same characters.
        // `strings` TOO, and leaving it out is what the pointer guard caught. It takes its
        // handle at its own write site, which runs BEFORE the page index holds one -- so it
        // minted, while the maps adopted here later converged on the page index's
        // allocation. Three holders, two allocations, and no byte probe would have said so.
        moved |= adopt_key_in(&mut self.strings, object_key, &canonical);
        // `control_state` TOO, for its RAW-keyed half. The counter commands file the raw object
        // key, so it is a fourth holder of characters `strings`, `features` and
        // `control_state_selection` already agreed on -- measured at 8 of 8 keys, four holders
        // and TWO allocations, with this map owning the second. Its decorated half
        // (`control_state:<family>:<key>`) never matches `object_key`, so this line is a no-op
        // for those and the saving is per raw key rather than per entry.
        moved |= adopt_key_in(&mut self.control_state, object_key, &canonical);
        // THE FOUR RECORDED KINDS. Each was measured holding its OWN copy of this text while
        // the maps above agreed on one -- eight holders, FIVE allocations, 8 of 8 keys. They go
        // through a declared operation rather than `adopt_key_in` because `RecordedMap` keeps its
        // map private, which is the invariant that kept them unconverted: nothing outside that
        // module can re-key them, so the adoption had to be declared there.
        moved |= self.hashes.adopt_key(object_key, &canonical);
        moved |= self.sets.adopt_key(object_key, &canonical);
        moved |= self.zsets.adopt_key(object_key, &canonical);
        moved |= self.lists.adopt_key(object_key, &canonical);
        // `seen` and `buckets` too: a raw key used for a string, a series, a selection, a
        // seen-set and a rate bucket lands in FIVE maps, and the pointer guard measured
        // THREE allocations before these two joined -- each was holding its own copy.
        moved |= adopt_key_in(&mut self.seen, object_key, &canonical);
        moved |= adopt_key_in(&mut self.buckets, object_key, &canonical);
        moved |= adopt_key_in(&mut self.features, object_key, &canonical);
        moved |= adopt_key_in(&mut self.control_state_selection, object_key, &canonical);
        // `control_state_changes` TOO. Seven holders of one raw key text reported TWO
        // allocations before this line, at 8 of 8 keys, with this map owning the second --
        // measured by `a_key_held_by_several_maps_is_one_allocation` once the map was named in
        // it. Nothing failed while it owned that copy, because the characters are equal either
        // way and a clone-based byte probe cannot see a duplicate.
        moved |= adopt_key_in(&mut self.control_state_changes, object_key, &canonical);
        if moved {
            CONTEXT_EVENT_ADOPT_MOVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        moved
    }

    /// Drain the legacy top-level event map into the array map at decode.
    ///
    /// # WHAT THIS IS FOR, AND IT IS NOT AN UNGUARDED DOOR
    ///
    /// Every door that installs a stored index checks the stamp, through
    /// `persistence::refuse_stale_index_stamp`. What the stamp cannot do is refuse a row it does
    /// not know about: the test is `<` rather than `!=`, so an image at the CURRENT stamp is
    /// accepted everywhere, and the stamp is one integer that says nothing about which keys the
    /// image carries. An accepted image can therefore still carry `context_events`, and a reader
    /// looking only at the array map would find it empty -- the rows gone, destroyed exactly as a
    /// deletion would destroy them, with no refusal anywhere because there was nothing for a door
    /// to refuse.
    ///
    /// # A MOVE AND NOT A MERGE, BECAUSE THE DESTINATION IS PROVABLY EMPTY
    ///
    /// `context_events_series` is `#[serde(skip)]`, so a freshly decoded `ShardState` always has it
    /// at `Default` -- empty. This runs on the `decode_index_bytes` wrapper, immediately after the
    /// decode, so every key in the legacy map is an arrival and there is nothing to merge with.
    ///
    /// THAT IS WHY THE DISAGREEMENT COUNTER THIS SHIPPED WITH IS GONE. It counted
    /// both-present-and-different, and under the mirror that was reachable. With one live copy the
    /// destination is empty by construction, so that branch could never be taken: an assertion that
    /// can only ever be true is the third state of an assertion, and a counter nothing can increment
    /// is indistinguishable from a counter that is broken. What replaced it counts the PREMISE
    /// FAILING instead -- see [`legacy_context_event_fold_over_live_map_count`] -- which is
    /// reachable, assertable, and has a negative control.
    ///
    /// UNCONDITIONAL, and reached from the one place that cannot be bypassed: `decode_index_bytes`
    /// wraps every codec arm, and its own note says a step written there "is a thing nobody can
    /// forget" where one written per arm "would be a thing to remember".
    pub(super) fn fold_legacy_context_events(&mut self) {
        if self.context_events_compat.is_empty() {
            return;
        }
        // THE PREMISE, CHECKED RATHER THAN ASSUMED. If the array map is already populated when this
        // runs, this is not a fresh decode -- the fold has been called twice, or called on a live
        // state -- and a blind insert would then silently overwrite live series with whatever an
        // old image happened to carry. Counted and the live entry KEPT, which is the same choice
        // the mirror version made for the same reason: recovery behaviour must not depend on which
        // copy happens to differ.
        let folding_over_live = !self.context_events_series.is_empty();
        let legacy = std::mem::take(&mut self.context_events_compat);
        let mut kept_live = 0u64;
        for (key, points) in legacy {
            if self.context_events_series.contains_key(&*key) {
                kept_live += 1;
                continue;
            }
            self.context_events_series
                .insert(ModelKey::from(&*key), OwnedTimestampedSeries::from(points));
        }
        if folding_over_live && kept_live > 0 {
            LEGACY_CONTEXT_EVENT_FOLD_OVER_LIVE_MAP
                .fetch_add(kept_live, std::sync::atomic::Ordering::Relaxed);
        }
        // AND THE COMPATIBILITY FIELD IS LEFT EMPTY. `std::mem::take` above did that; it is restated
        // here because the mirror version REFILLED it at exactly this point, and that one line was
        // the whole difference between a decode-only landing spot and a second live copy.
        debug_assert!(
            self.context_events_compat.is_empty(),
            "the compatibility field must be empty after the fold -- a non-empty one is a second \
             live copy, which is the shape this was converted away from"
        );
    }
}

/// HOW MANY LEGACY KEYS ARRIVED AT A FOLD WHOSE DESTINATION WAS ALREADY LIVE.
///
/// Zero on every ordinary decode, because `context_events_series` is `#[serde(skip)]` and is
/// therefore empty on a freshly decoded state. A non-zero reading means the fold ran somewhere it
/// was not meant to -- twice over one state, or over a state already serving -- and that each such
/// key was LEFT ALONE rather than overwritten by the old image.
///
/// THIS REPLACED A COUNTER THAT COULD NOT FIRE. The mirror version counted the two copies arriving
/// different, which was reachable while both were live and became unreachable the moment there was
/// one. A counter whose branch no execution can take proves nothing by reading zero, so the
/// quantity was changed to one that can.
///
/// EXPOSED RATHER THAN LOGGED, for the same reason `index_load_path_counts` is: the fold cannot
/// refuse, so a caller cannot tell from its return value that anything was wrong, and a log line is
/// not something a test can assert on.
static LEGACY_CONTEXT_EVENT_FOLD_OVER_LIVE_MAP: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Read the count.
pub(crate) fn legacy_context_event_fold_over_live_map_count() -> u64 {
    LEGACY_CONTEXT_EVENT_FOLD_OVER_LIVE_MAP.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own folds rather than the suite before it.
pub(crate) fn reset_legacy_context_event_fold_over_live_map_count() {
    LEGACY_CONTEXT_EVENT_FOLD_OVER_LIVE_MAP.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// THE TEST-ONLY DOORS INTO THE COMPATIBILITY FIELD.
///
/// The field is private so that no writer outside this file can reach it, which is what makes
/// "drained and never refilled" a property of the code rather than a request. These let a test
/// plant what a stored image carries and read back what the fold left behind.
#[cfg(test)]
impl ShardState {
    /// Plant a series in the compatibility field alone, which is what a decoded image carrying the
    /// legacy key leaves there.
    pub(super) fn plant_legacy_context_event_for_test(
        &mut self,
        object_key: &str,
        series: BTreeMap<u64, ElementEntry>,
    ) {
        self.context_events_compat
            .insert(object_key.to_string(), series);
    }

    /// How many keys the compatibility field holds. The fold must leave this at zero.
    pub(super) fn legacy_context_event_len_for_test(&self) -> usize {
        self.context_events_compat.len()
    }

    /// A copy of the compatibility field alone, so a footprint test can price it rather than the
    /// whole shard.
    pub(super) fn clone_legacy_context_events_for_test(
        &self,
    ) -> HashMap<String, BTreeMap<u64, ElementEntry>> {
        self.context_events_compat.clone()
    }
}

#[cfg(test)]
mod does_the_sharing_survive_a_decode {
    use super::*;
    use std::collections::BTreeSet;

    /// HOW MANY DISTINCT ALLOCATIONS HOLD ONE OBJECT KEY'S CHARACTERS.
    ///
    /// Counted by ADDRESS, because every holder's contents are equal by construction and contents
    /// therefore witness nothing. One shared handle gives 1; independent copies give one per holder.
    fn distinct_key_allocations(shard: &ShardState, key: &str) -> usize {
        let mut seen: BTreeSet<usize> = BTreeSet::new();
        if let Some((k, _)) = shard.strings.get_key_value(key) {
            seen.insert(k.as_ptr() as usize);
        }
        if let Some((k, _)) = shard.expires_at_ms.get_key_value(key) {
            seen.insert(k.as_ptr() as usize);
        }
        if let Some(p) = shard.bucket_index.object_block_lookup.key_ptr("strings", key) {
            seen.insert(p as usize);
        }
        if let Some(p) = shard.dirty_objects.key_ptr(key) {
            seen.insert(p as usize);
        }
        seen.len()
    }

    /// MEASURED, NOT ASSERTED AS A TARGET: what a round trip through the stored image does to the
    /// sharing the write path established.
    #[test]
    fn report_how_many_allocations_hold_one_key_before_and_after_a_round_trip() {
        let key = "ctx:event:12297829382473034410:9876543210987654321";
        let mut shard = ShardState::default();

        // The write path's end state: ONE allocation, every holder a refcount bump.
        let shared: Arc<str> = Arc::from(key);
        shard.strings.insert(Arc::clone(&shared), ElementEntry::default());
        shard.expires_at_ms.insert(Arc::clone(&shared), 1);
        shard.dirty_objects.insert_shared(Arc::clone(&shared), 0);

        let before = distinct_key_allocations(&shard, key);
        println!("  distinct allocations BEFORE a round trip: {before}");

        // Through the stored image and back.
        let bytes = serde_json::to_vec(&shard).expect("encode the shard");
        let decoded: ShardState = serde_json::from_slice(&bytes).expect("decode the shard");
        let after = distinct_key_allocations(&decoded, key);
        println!("  distinct allocations AFTER  a round trip: {after}");
        println!("  (dirty_objects is serde(skip), so it is absent after a decode by design)");

        assert_eq!(before, 1, "the write path must establish exactly one allocation");
        println!("  VERDICT: sharing {} a decode", if after <= 1 { "SURVIVES" } else { "DOES NOT SURVIVE" });

        // AND WITH THE PAGE INDEX POPULATED, THE ADOPTION PASS BRINGS THEM BACK TOGETHER.
        //
        // The arm above has no page index, so there is no canonical handle to adopt and the pass
        // correctly leaves the decoded allocations alone -- which is the first-write case. This arm
        // files a block so the lookup holds a handle, and then one allocation must serve every
        // holder after a round trip.
        let mut shard = ShardState::default();
        let shared: Arc<str> = Arc::from(key);
        let model: Arc<str> = Arc::from("strings");
        shard.bucket_index.object_block_lookup.file(
            &model,
            &shared,
            BlockLookupRef { routing_bucket: 0, block_ref_key: 1 },
        );
        shard.strings.insert(Arc::clone(&shared), ElementEntry::default());
        shard.expires_at_ms.insert(Arc::clone(&shared), 1);
        assert_eq!(distinct_key_allocations(&shard, key), 1, "the write path shares");

        let bytes = serde_json::to_vec(&shard).expect("encode");
        let mut decoded: ShardState = serde_json::from_slice(&bytes).expect("decode");

        // WHAT THIS ARM DOES AND DOES NOT COVER. `object_block_lookup` is
        // `serde(default, skip_serializing)`: it is never written, and a real load rebuilds it by
        // filing each DECODED PAGE. This test hand-assembles a shard with no pages, so after the
        // round trip the lookup is legitimately empty and there is nothing to adopt -- which is the
        // first-write case, not a failure. Filing the page here stands in for that page-driven
        // rebuild, so what follows tests THE PASS's contract: given a populated lookup and keys
        // that decoded independently, exactly one allocation must remain. Whether the rebuild
        // itself happens before the repack is a property of the decode order and is not asserted
        // here.
        decoded.bucket_index.object_block_lookup.file(
            &model,
            &Arc::from(key),
            BlockLookupRef { routing_bucket: 0, block_ref_key: 1 },
        );
        let without_pass = distinct_key_allocations(&decoded, key);
        repack_decoded_btrees(&mut decoded);
        let with_pass = distinct_key_allocations(&decoded, key);
        println!("  with a page index: {without_pass} distinct before the pass, {with_pass} after");

        // THE DISCRIMINATING ARM. If the pass did nothing, these two would be equal -- so this is
        // what stops the assertion below passing on a no-op.
        assert!(
            without_pass > 1,
            "a decode must scatter the allocations, or this test proves nothing about the pass"
        );
        // ONE, AND THIS ASSERTION USED TO SAY TWO.
        //
        // It read: "`strings` is a hashed map and cannot take part -- so it keeps its decoded
        // allocation [..] asserting one here would assert a behaviour this pass deliberately does
        // not have." The premise was that a hashed map cannot be re-keyed without moving the
        // stored bytes. That is true of a REBUILD, which is the operation that was tried and
        // caught; it is not true of an in-place re-key, where the characters are identical so the
        // hash is identical, the probe sequence is identical, and the `remove`'s tombstone is
        // reclaimed by the `insert` that follows it.
        //
        // Measured before changing this: `whether_rekeying_a_hashed_map_in_place_moves_its_bytes`
        // re-keys 512 keys one at a time and the encoding is byte-identical, 9859 bytes either
        // way, iteration order unchanged -- so no index-format stamp is owed. The repository's own
        // byte guard, `a_decode_puts_back_exactly_the_series_that_were_encoded`, passes with the
        // hashed pass wired in, which is the check that would redden with "the stored form moved".
        //
        // So the pass now collapses all three allocations a decode scattered, not two of them.
        assert_eq!(
            with_pass, 1,
            "every holder must adopt the page index's handle: the ordered map and the hashed ones"
        );
        assert!(
            with_pass < without_pass,
            "the pass must remove at least one allocation, or it is doing nothing"
        );
    }
}

#[cfg(test)]
mod a_records_pages_can_live_on_the_shard_beside_the_record {
    use super::ShardState;
    use crate::block_store::ElementEntry;
    use crate::engine::object_record::{ObjectRecord, PageAddress, PageLayout};

    /// The handle arm, driven through the shard rather than through the type's own tests.
    ///
    /// WHY THIS TEST AND NOT ONLY THE STORE'S OWN: a store nothing reaches is the same dead arm it
    /// replaces. `PageAddress::Many` was constructible and tested for some time while every
    /// production mention stayed inside `object_record.rs`, because a handle is only a number until
    /// something maps it to a layout AND that something is somewhere a shard can get at. This
    /// drives the whole path -- shard field, handle, layout, record, and back -- so the field is
    /// reachable state and not a declaration.
    #[test]
    fn a_page_layout_filed_on_the_shard_gives_the_record_its_rows_back() {
        let mut shard = ShardState::default();
        assert!(
            shard.object_page_layouts.is_empty(),
            "a fresh shard should hold no layouts"
        );

        let address = ElementEntry::from_parts(7, 64, 128, Some(3), None);
        let reserved = shard.object_page_layouts.insert(PageLayout::default());
        let (record, layout) =
            ObjectRecord::from_object_rows_with_layout(&address, 9, Some(1_700), true, reserved)
                .expect("the layout arm refused an entry it is documented to hold");
        assert!(
            matches!(record.page_address(), PageAddress::Many(_)),
            "the record should carry a handle rather than an inline address"
        );

        // File the real layout where the record's handle already points.
        shard
            .object_page_layouts
            .remove(reserved)
            .expect("the reserved slot should have held the placeholder");
        let handle = shard.object_page_layouts.insert(layout);
        assert_eq!(1, shard.object_page_layouts.len(), "one layout should be filed");

        let held = shard
            .object_page_layouts
            .get(handle)
            .expect("a layout filed on the shard should resolve");
        let (back_address, back_model, back_deadline, back_dirty) = record
            .into_object_rows(Some(held))
            .expect("unfolding a record through its layout refused");
        assert_eq!(address, back_address, "the address row changed");
        assert_eq!(9, back_model, "the model id changed");
        assert_eq!(Some(1_700), back_deadline, "the deadline row changed");
        assert!(back_dirty, "the dirty bit changed");
    }

    /// A filed layout IS carried in the stored bytes, and comes back.
    ///
    /// THIS ASSERTION WAS INVERTED, AND THE INVERSION IS THE WHOLE POINT. It read "changes
    /// nothing", pinning the claim that the field was skipped in both directions and therefore owed
    /// no format stamp. That claim was true when written and is not any more: the field was made
    /// durable while it was still empty -- the cheapest possible moment, and the condition its own
    /// doc comment said would stop holding "the instant a writer lands, silently".
    ///
    /// So the subject moved from "the image is unchanged" to "the image carries the layouts". Both
    /// are real invariants and only one can hold at a time; the one that matters now is that a
    /// reload does not drop where a multi-page record's pages live.
    ///
    /// AND IT IS READ BACK, which `assert_ne!` on the bytes alone would not show. Differing bytes
    /// are satisfied by a field that encodes something lossy, or something that cannot be read
    /// back -- exactly the failure the durability change exists to prevent -- so the decode half
    /// is what makes this a durability test rather than a "the bytes moved" test.
    #[test]
    fn a_filed_layout_is_carried_in_the_stored_bytes_and_comes_back() {
        let without = ShardState::default();
        let mut with = ShardState::default();
        with.object_page_layouts.insert(PageLayout::default());
        assert_eq!(1, with.object_page_layouts.len(), "the layout did not land");

        let a = serde_json::to_vec(&without).expect("encoding a shard failed");
        let b = serde_json::to_vec(&with).expect("encoding a shard failed");
        assert_ne!(
            a, b,
            "a filed layout did NOT change the stored bytes, so the field is skipped again and a              reload would drop where a multi-page record's pages are"
        );

        let back: ShardState =
            serde_json::from_slice(&b).expect("decoding a shard image failed");
        assert_eq!(
            1,
            back.object_page_layouts.len(),
            "the layout did not survive the round trip, so the field is written but not readable              back -- which is the same loss as not writing it"
        );
    }
}

#[cfg(test)]
mod the_dirty_index_adopts_the_page_indexs_handle_after_the_fact {
    use super::{DirtyObjectIndex, ObjectBlockLookup};
    use std::sync::Arc;

    fn lookup_holding(key: &str) -> (ObjectBlockLookup, Arc<str>) {
        let mut lookup = ObjectBlockLookup::default();
        let kind: Arc<str> = Arc::from("control_state");
        let shared: Arc<str> = Arc::from(key);
        lookup.file(
            &kind,
            &shared,
            super::BlockLookupRef { routing_bucket: 7, block_ref_key: 1 },
        );
        let handle = lookup
            .any_shared_object_key(key)
            .expect("the lookup should hold the key it was just given");
        (lookup, handle)
    }

    /// Adoption replaces the index's own allocation with the page index's handle.
    ///
    /// AND THE CONTROL IS THE SAME INDEX BEFORE THE CALL, not a different one: the assertion below
    /// must be FALSE first and TRUE after, or a `key_ptr` that always answered the handle would
    /// satisfy it either way. Contents are equal throughout -- that is why this is a pointer test.
    #[test]
    fn the_index_holds_its_own_copy_until_it_adopts_and_the_handle_after() {
        let key = "ctx:event:4071:915";
        let (lookup, handle) = lookup_holding(key);

        let mut dirty = DirtyObjectIndex::default();
        dirty.insert(key, 7); // the first-write arm: allocates its own copy
        assert_eq!(
            Some(key),
            dirty.key_ptr(key).map(|_| key),
            "the index should hold the key at all"
        );
        assert_ne!(
            dirty.key_ptr(key),
            Some(handle.as_ptr()),
            "CONTROL: before adopting, the index must hold a DIFFERENT allocation -- if this \
             already matched, the assertion after the call would prove nothing"
        );

        assert!(dirty.adopt_shared_key(&handle), "adoption reported no change");
        assert_eq!(
            Some(handle.as_ptr()),
            dirty.key_ptr(key),
            "after adopting, the index must hold the page index's allocation"
        );
        drop(lookup);
    }

    /// Adopting twice is not a second adoption, and adopting a key it does not hold does nothing.
    #[test]
    fn adoption_is_idempotent_and_declines_a_key_it_does_not_hold() {
        let key = "ctx:event:4071:915";
        let (_lookup, handle) = lookup_holding(key);

        let mut dirty = DirtyObjectIndex::default();
        dirty.insert(key, 7);
        assert!(dirty.adopt_shared_key(&handle));
        assert!(
            !dirty.adopt_shared_key(&handle),
            "the second adoption should report no change -- it is already the same allocation"
        );

        let (_other_lookup, other) = lookup_holding("ctx:event:9999:111");
        assert!(
            !dirty.adopt_shared_key(&other),
            "adopting a key this index does not hold should do nothing"
        );
    }

    /// BOTH inner maps are re-keyed, which is what makes the old allocation actually drop.
    ///
    /// Re-keying only `by_key` would leave `by_bucket` holding the original handle, so the
    /// refcount would never fall to zero and the change would free nothing while every pointer
    /// assertion above still passed. Asserted through the bucket side's own view.
    #[test]
    fn the_bucket_side_is_re_keyed_too_or_nothing_is_freed() {
        let key = "ctx:event:4071:915";
        let (_lookup, handle) = lookup_holding(key);

        let mut dirty = DirtyObjectIndex::default();
        dirty.insert(key, 7);
        let owned = Arc::strong_count(&handle);
        assert!(dirty.adopt_shared_key(&handle));
        assert_eq!(
            owned + 2,
            Arc::strong_count(&handle),
            "both inner maps should now hold the handle, so the strong count should rise by two; \
             a rise of one means only one map was re-keyed and the old allocation is still alive"
        );
    }
}
