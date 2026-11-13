// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Standalone per-shard command execution extracted from engine.rs.
use super::*;
use crate::engine::timestamped_series::TimestampedSeries as _;
use crate::engine::timestamped_series::TimestampedSeriesWrite as _;

#[derive(Clone, PartialEq, prost::Message)]
struct HashTimestamp {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

pub(super) fn decode_hash_timestamp(bytes: &[u8]) -> Option<(i64, i32)> {
    <HashTimestamp as prost::Message>::decode(bytes).ok().map(|value| (value.seconds, value.nanos))
}

#[derive(Clone, PartialEq, prost::Message)]
struct FeatureProtoNumber {
    #[prost(oneof = "feature_proto_number::Value", tags = "3, 4")]
    value: Option<feature_proto_number::Value>,
}

mod feature_proto_number {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Value {
        #[prost(int32, tag = "3")]
        Int32(i32),
        #[prost(int64, tag = "4")]
        Int64(i64),
    }
}

fn parse_feature_proto_number(bytes: &[u8]) -> Option<i64> {
    let number = <FeatureProtoNumber as prost::Message>::decode(bytes).ok()?;
    match number.value? {
        feature_proto_number::Value::Int32(value) => Some(i64::from(value)),
        feature_proto_number::Value::Int64(value) => Some(value),
    }
}

/// THE SERVED-PATH DIVERGENCE COUNTER FOR THE HASH WHOLE-OBJECT READ.
///
/// WHAT IT COUNTS. One increment for each field a served `HashGetAll` found NAMED BY THE BLOCK
/// INDEX and NOT HELD BY the durable container `shard.bucket_index.hashes`. Per (read, field), so one read of
/// an object whose container lacks three of its fields counts three.
///
/// WHY ONLY THAT DIRECTION. It is the direction that loses a read once the index stops being
/// consulted -- which is the whole point of measuring it, because taking `element_name` off the block
/// entry is what makes the container load-bearing. The other direction, a field the container
/// holds that the index does not name, is not a loss: the union in `Command::HashGetAll` already
/// answers it from the container, and counting it would mix a rescued read in with a threatened
/// one.
///
/// WHAT A ZERO MEANS, AND WHAT IT DOES NOT. A zero means NO DIVERGENCE ON THE ROUTES THE TRAFFIC
/// TOOK. IT IS NOT PROOF OF COMPLETENESS. A field installed by a route that no request read is
/// never sampled here and cannot be, because this counter only ever sees reads; an object nobody
/// got is an object nobody checked. Reading a zero from here as "the container holds everything
/// the fold installs" would repeat exactly the substitution #2078 made -- it read a fixture's
/// green as a statement about the engine -- and that is the mistake a correction comment on #2078
/// was written for.
///
/// `reads_served` is reported BESIDE the count for that reason, and it is the figure to read
/// first: a zero over a zero denominator says nothing whatever, and the two numbers are the only
/// way to tell "no divergence" from "nothing was asked".
///
/// WHAT IT IS STRICTLY BETTER AT THAN THE FIXTURE IT REPLACES. It samples whatever route the
/// traffic actually took, including routes no fixture drives, and it cannot be satisfied by one
/// source being derived from the other -- the comparison happens at read time against both
/// sources as they are, with no load path in between to fill one from the other.
///
/// WHY IT IS PROCESS-WIDE AND NOT PER-SHARD. The figure is a property of the engine's read path,
/// not of a shard's contents, and it is read as a trend rather than as an inventory. Carrying a
/// `shard_id` on it would be a label describing something it did not measure, so the report
/// surface does not carry one.
///
/// THE RATE LIMIT ON THE LOG. The first [`LOG_BURST`] divergences are logged, and after that one
/// in every [`LOG_EVERY`]. An unbounded log would be one line per field per read for as long as
/// the divergence lasts, which on a hot object is the loudest possible way to say one thing; a
/// log of only the first would go quiet exactly when the condition spread. `divergences_logged`
/// is reported so the log and the count can be told apart.
pub(super) mod hash_read_divergence {
    use std::sync::atomic::{AtomicU64, Ordering};

    static READS_SERVED: AtomicU64 = AtomicU64::new(0);
    static INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED: AtomicU64 = AtomicU64::new(0);
    static DIVERGENCES_LOGGED: AtomicU64 = AtomicU64::new(0);
    static LAST: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None);

    /// Log every one of the first this many, so a divergence that happens once is still named.
    const LOG_BURST: u64 = 8;
    /// After the burst, log one in this many.
    const LOG_EVERY: u64 = 1_024;

    /// One served whole-object hash read that resolved through both sources.
    ///
    /// Counted after the expiry door, because a read that removed an expired key never consulted
    /// either source and including it would inflate the denominator with reads that could not
    /// have diverged.
    pub(crate) fn note_read() {
        READS_SERVED.fetch_add(1, Ordering::Relaxed);
    }

    /// One field the block index named and the container did not hold.
    pub(crate) fn note(object_key: &str, field: &str) {
        let seen = INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut last) = LAST.lock() {
            *last = Some((object_key.to_string(), field.to_string()));
        }
        if seen <= LOG_BURST || seen % LOG_EVERY == 0 {
            DIVERGENCES_LOGGED.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                object_key,
                field,
                divergences = seen,
                "the block index named a hash field the durable container does not hold; the \
                 union served it from the index"
            );
        }
    }

    /// The four figures, read together so a reader cannot take the count without its denominator.
    pub(crate) fn snapshot() -> (u64, u64, u64, Option<(String, String)>) {
        (
            READS_SERVED.load(Ordering::Relaxed),
            INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED.load(Ordering::Relaxed),
            DIVERGENCES_LOGGED.load(Ordering::Relaxed),
            LAST.lock().ok().and_then(|last| last.clone()),
        )
    }
}

/// Put aside an outcome that no block backs: a deletion, a deadline, or state that lives only
/// in the index snapshot.
///
/// The block path stages its outcome inside `upsert_bucket_index_block`, because that is where a
/// block outcome is produced. These have no such moment, so they say it here.
#[allow(clippy::too_many_arguments)]
/// Record a block or a value that belongs to one ELEMENT NAME of an object.
///
/// `stage_meta_outcome` states something about the object as a whole -- it is gone, its deadline
/// is this. This states what one part of it became: which block backs it, or the bytes for state no
/// block backs at all.
#[allow(clippy::too_many_arguments)]
fn stage_element_name_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    element_name: Option<String>,
    routing_bucket: u32,
    address: Option<crate::block_store::ElementEntry>,
    value: Option<Vec<u8>>,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        // MOVED, NOT CLONED. `element_name` is taken BY VALUE -- it is already the caller's
        // allocation handed over -- and this function ends here, so nothing reads it after.
        // Cloning it allocated a second copy of the same characters on every staged element name
        // outcome: every hash field, set member, zset member, list element and context entity
        // write. `stage_meta_outcome` next door already moves its owned arguments.
        element_name,
        object_id: stable_block_object_id(shard_id, kind, object_key),
        routing_bucket,
        address,
        value,
        ttl: None,
        deleted: false,
        meta: false,
    });
}

fn stage_meta_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    value: Option<Vec<u8>>,
    ttl: Option<u64>,
    deleted: bool,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        element_name: None,
        object_id: stable_block_object_id(shard_id, kind, object_key),
        routing_bucket: block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket),
        address: None,
        value,
        ttl,
        deleted,
        meta: true,
    });
}

/// A counter write's RESULT at one bucket, and the series block when it is due.
///
/// The result, not the delta: an outcome installed twice is the same value, which is what lets a
/// record be replayed more than once. All five counter arms add into one bucket of one series and
/// can state the total they left there, so all five stage it here and let
/// `persist_control_state_block_after_increment` decide whether the whole series is rewritten.
#[allow(clippy::too_many_arguments)]
fn record_counter_result(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
    bucket_ms: u64,
    resulting: i64,
) -> bool {
    stage_element_name_outcome(
        shard_id,
        "control_counter",
        key,
        Some(bucket_ms.to_string()),
        block_routing_bucket(key, start_routing_bucket, end_routing_bucket),
        None,
        Some(resulting.to_le_bytes().to_vec()),
    );
    persist_control_state_block_after_increment(
        cache,
        block_store,
        shard_id,
        shard,
        key,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
        bucket_ms,
        resulting,
    )
}

/// Read a node record back, by the same two lookups every node reader here uses.
fn load_context_node(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Option<ContextNode> {
    // THE BUCKET THE PAGE CACHE IS KEYED BY, derived the way the write path derives it.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket = Some(block_routing_bucket(
        object_key,
        start_routing_bucket,
        end_routing_bucket,
    ));
    // EACH ARM NAMES ITS OWN OBJECT, which is what the identity forced this lookup to say.
    //
    // The two arms are not two places to find one block: a CURRENT node is the `CONTEXT_NODE_FIELD`
    // field of a `hash`, and a pre-hashes node is a whole `context_node` block --
    // `model_map_block_address` spells the second as `("context_node", None)` and is the authority
    // on it. One identity for both is wrong for whichever arm it does not describe, and the comment
    // that used to sit at the read asserted they were the same. Under the old shape only the
    // ELEMENT was wrong, because the object came off the address: the in-log fallback built the block
    // key `(id, Some("meta"))` for a block registered as `(id, None)` and missed it, which reads as
    // MISSING for a durably acknowledged write on that path. The identity cannot be built without
    // naming a kind, so the arms had to separate.
    shard
        .bucket_index
        .hashes
        .get(object_key)
        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
        .map(|address| {
            (
                address,
                BlockIdentity::of(shard_id, "hash", object_key, Some(CONTEXT_NODE_FIELD)),
            )
        })
        .or_else(|| {
            shard.context_nodes().get(object_key).map(|address| {
                (
                    address,
                    BlockIdentity::of(shard_id, "context_node", object_key, None),
                )
            })
        })
        .and_then(|(address, identity)| {
            // Owning, not shared: the summary upsert calls this while writing, where the block
            // is as likely to be a miss as a hit -- and on a miss the shared read wraps an owned
            // buffer in a fresh Arc, which copies a second time. The query commands, which are
            // hit-heavy, do share.
            read_block_bytes(
                cache,
                block_store,
                shard_id,
                address,
                identity,
                routing_bucket,
            )
            .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
        })
}

/// Persist a node record: the one producer of a node block.
///
/// Three commands now write a node -- the upsert, the embedding attach, and the summary upsert
/// keeping the node's copy of its summary vector current -- and each needs the block appended,
/// the outcome staged, the hash field pointed at the new address and the record invalidated, in
/// that order. Held apart, the third copy is where one of those four steps goes missing.
///
/// Returns whether a block was written, which is what the caller records as `mutated`.
#[allow(clippy::too_many_arguments)]
fn write_context_node(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    object_key: &str,
    node: &ContextNode,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let object_id = stable_block_object_id(shard_id, "hash", object_key);
    let routing_bucket = block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    let mut wrote = false;
    // FRAMED AS A HASH BLOCK, because that is what it is. The outcome below records it under
    // `context_node` for a reason of its own -- it is never filed in the block index -- but the block
    // itself lands in `shard.hashes` under `CONTEXT_NODE_FIELD` and is read back through the hash
    // door with that field as its element name. Leaving this one site unframed would leave the node
    // the single hash element a later load could not recover from its block, which is the element
    // #2016 already recorded as the one whose absence reads as the node not existing at all.
    let block = crate::engine::container_blocks::single_element_block(
        "hash",
        CONTEXT_NODE_FIELD,
        &context_bytes(node),
    );
    if let Ok(address) = append_value(
        cache,
        block_store,
        shard_id,
        &block,
        Some(object_id),
        // The field this node is filed under -- the same argument the object id above was
        // derived with. A context node is a hash FIELD, not a whole object.
        Some(CONTEXT_NODE_FIELD),
        Some(routing_bucket),
        async_storage,
    ) {
        // Its own kind, deliberately: this writes a hash block and records the OUTCOME under
        // `context_node` rather than `hash`, so a rebuild cannot add an entry naming a block the
        // outcome never described.
        //
        // CORRECTED: this said the write "never registers it in the bucket index". It does -- the
        // `append_value` above files the block from the object id, `CONTEXT_NODE_FIELD` and the
        // routing bucket, and `context_node_survives_reload` reads that entry back as
        // `("hash", Some("meta"), deleted=false)`. The distinction is between the index entry,
        // which exists, and the outcome record's kind, which is not `hash` on purpose.
        // THE CONSTANT, INTERNED ONCE PER SHARD INSTEAD OF COPIED PER NODE. This is
        // the write path the narrow arm of the measurement is about: every context
        // node in a store files its block under this one four-character name.
        let field_handle =
            super::state::intern_shared(&mut shard.bucket_index.kind_pool, CONTEXT_NODE_FIELD);
        super::recorded_map::install_element_staged_only::<super::recorded_map::HashKind>(
            shard,
            shard_id,
            "context_node",
            object_key,
            CONTEXT_NODE_FIELD,
            routing_bucket,
            field_handle,
            address.clone(),
            address,
        );
        wrote = true;
    }
    invalidate_context_record(cache, shard_id, object_key);
    wrote
}

/// The read-time half of a deadline, at the one spelling every arm uses.
///
/// A deadline is honoured in two places and both have to agree: the background sweep, which
/// collects the key eventually, and this, which makes the key read as gone the instant its
/// deadline passes. Without the second, a key stays visible -- and answers with real data --
/// for the whole interval between its deadline and whichever sweep round collects it.
///
/// It is a PER-CALL-SITE obligation: `execute_on_shard` has one arm per command and each one
/// has to remember. So it is spelled once here and called, and
/// `execute_on_shard_guards_every_arm_that_can_hold_a_deadline` fails when an arm appears
/// that cannot reach this helper and is not listed as exempt with a reason.
///
/// COST. `remove_if_expired` returns on an `is_empty` when the shard holds no deadlines at
/// all, and otherwise examines only the key it is given plus the three control-state families
/// derived from it. It never walks the keyspace, so the O(due) cost of an expiry round is
/// untouched. Cache retirement runs only when a record was actually dropped and uses
/// record-scoped indexes, so unrelated payload pages are not scanned.
///
/// THE CACHE GOES WITH THE RECORD. Dropping the record and leaving its cached copy would let
/// the very next read answer from the copy, which is the bug this exists to close.
fn drop_if_expired(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> bool {
    let (start, end) = shard.routing_range();
    let mut removed_keys = Vec::new();
    let removed = remove_if_expired_with(shard, key, |removed_key| {
        // A write's outcomes must retire the expired incarnation before its new blocks.
        // Unlike a family-wide delete, this does not remove related records that remain live.
        stage_meta_outcome(shard_id, "record", removed_key, start, end, None, None, true);
        removed_keys.push(removed_key.to_owned());
    });
    if removed {
        invalidate_records_all_batched(cache, shard_id, &removed_keys, &CACHE_SWEEP_COUNTS);
        true
    } else {
        false
    }
}

pub(super) struct ContainerRemoval {
    pub(super) index_removed: bool,
    pub(super) tombstone_written: bool,
}

/// Remove one element of a container, RECORDING THE REMOVAL IN THE BLOCKS as well as the index.
///
/// # WHY THERE IS A FUNCTION HERE AT ALL
///
/// Seven call sites removed a container element by calling `mark_bucket_index_block_deleted`, which
/// drops the index entry and stages the outcome. That left the BLOCKS saying nothing: #2028 drove
/// what it costs -- a folded block holding twelve members the index named eleven of -- and the reason
/// is that the block is never rewritten by a removal. So a removal now APPENDS a block that states it,
/// and the seven sites differ in their kind and their element name and in nothing else. Spelling the
/// append at each of them is how the fourth site gets the ordinal wrong.
///
/// Two of the seven are not obviously removals and are the ones this must not miss: a `ZSetAdd` or
/// `ZSetIncrement` that changes a member's score REMOVES the old `(score, member)` element key and
/// writes a new one. Leave the old key untombstoned and a block-derived membership holds the member
/// TWICE, at both scores. `a_rescore_sweeps_its_own_tombstone_because_the_element_name_no_longer_spells_the_score`
/// drives that -- and, since the element name the removal tombstones and the element name the new write
/// files under are now the same string, also drives the sweep that cleans the tombstone up again
/// in the same write.
///
/// # WHAT IT DOES WHEN IT CANNOT WRITE THE BLOCK
///
/// It removes the element anyway and leaves the blocks silent, with the miss counted by
/// `container_blocks::unframed_container_removal_count` or by [`TOMBSTONE_APPEND_FAILURES`]. That is
/// the only honest choice: refusing the removal would keep serving an element the caller deleted,
/// which is worse than a block set that a derivation will decline to install. Both counters are
/// floored at zero by the guards, so neither is a tolerance -- they are how a hole announces itself
/// instead of appearing later as a resurrected member.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_container_element(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    model_id: &str,
    key: &str,
    element_name: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> ContainerRemoval {
    let routing_bucket = block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    let tombstone = crate::engine::container_blocks::tombstone_block(model_id, element_name)
        .and_then(|block| {
            // THE ORDINAL IS THE ELEMENT'S OWN, exactly as it is on the write path: the element
            // already has a position in this object and the tombstone is another block AT that
            // position, not a new one. Computed before the append because the append stamps it into
            // the record header and onto the address from one value -- `decode_block_record` refuses
            // a block whose two copies disagree, which is what #2008 threaded this for.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, model_id, key, element_name);
            append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(stable_block_object_id(shard_id, model_id, key)),
                Some(element_name),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            )
            .ok()
        });
    if tombstone.is_none() {
        TOMBSTONE_APPEND_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    let tombstone_written = tombstone.is_some();
    let index_removed = crate::engine::mark_bucket_index_block_deleted_recording(
        shard,
        shard_id,
        model_id,
        key,
        Some(element_name),
        true,
        tombstone,
    );
    ContainerRemoval { index_removed, tombstone_written }
}

/// Gated listings answered from a fold of the blocks, and gated listings that DECLINED to.
///
/// Two counters because a test must be able to tell which path produced an answer. A gated
/// listing that quietly fell back to the durable map would return the right members and prove
/// nothing about the fold -- the shape that made an earlier measurement in this series report a
/// clean result over a branch nothing reached.
static GATED_LISTING_DERIVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static GATED_LISTING_DECLINED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// How many set listings were answered from the durable member map rather than by a fold.
/// It joins the two above for one reason: a test that asserts the fold served has to be able
/// to tell "the fold served" from "something cheaper served", and without this the map path
/// is indistinguishable from a cache hit -- both leave the fold counters at zero.
static SET_LISTING_FROM_MAP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (derived, declined, from_map) for gated container listings. The third exists so a test can
/// still tell WHICH mechanism answered once the durable map became the preferred one: a map
/// answer and a cache hit both leave the first two at zero, and they are not the same event.
pub fn gated_listing_counts() -> (u64, u64, u64) {
    (
        GATED_LISTING_DERIVED.load(std::sync::atomic::Ordering::Relaxed),
        GATED_LISTING_DECLINED.load(std::sync::atomic::Ordering::Relaxed),
        SET_LISTING_FROM_MAP.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget them, so a test measures its own exercise.
pub fn reset_gated_listing_counts() {
    GATED_LISTING_DERIVED.store(0, std::sync::atomic::Ordering::Relaxed);
    GATED_LISTING_DECLINED.store(0, std::sync::atomic::Ordering::Relaxed);
    SET_LISTING_FROM_MAP.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Removals whose tombstone block could not be APPENDED, as distinct from could not be FRAMED.
///
/// Two counters and not one, because the two say different things about the store: an unframable
/// removal is a bug in this code's spelling of a kind, and a failed append is the block store
/// refusing a write. A guard that floored their sum would pass while one of them was firing.
static TOMBSTONE_APPEND_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many removals could not append the block that would have recorded them.
pub fn tombstone_append_failure_count() -> u64 {
    TOMBSTONE_APPEND_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_tombstone_append_failure_count() {
    TOMBSTONE_APPEND_FAILURES.store(0, std::sync::atomic::Ordering::Relaxed);
}

pub(super) enum ProjectedHashValues {
    Missing(usize),
    Owned(Vec<Option<Vec<u8>>>),
    Shared {
        frame: std::sync::Arc<[u8]>,
        ranges: Vec<Option<std::ops::Range<usize>>>,
    },
}

#[cfg(test)]
thread_local! {
    static PROJECTED_VALUE_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static PROJECTED_FIELD_CACHE_HITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static PROJECTED_FRAME_FETCHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[path = "tests/projected_field_cache.rs"]
mod projected_field_cache_tests;

#[cfg(test)]
pub(crate) fn reset_projected_value_visits_for_test() {
    PROJECTED_VALUE_VISITS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn projected_value_visits_for_test() -> u64 {
    PROJECTED_VALUE_VISITS.with(std::cell::Cell::get)
}

impl ProjectedHashValues {
    pub(super) fn retained_bytes(&self) -> usize {
        match self {
            Self::Missing(_) => 0,
            Self::Owned(values) => values.iter().flatten().fold(
                values.capacity().saturating_mul(std::mem::size_of::<Option<Vec<u8>>>()),
                |bytes, value| bytes.saturating_add(value.capacity()),
            ),
            Self::Shared { frame, ranges } => frame.len()
                .saturating_add(2 * std::mem::size_of::<usize>())
                .saturating_add(ranges.capacity().saturating_mul(
                    std::mem::size_of::<Option<std::ops::Range<usize>>>(),
                )),
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            Self::Missing(field_count) => *field_count,
            Self::Owned(values) => values.len(),
            Self::Shared { ranges, .. } => ranges.len(),
        }
    }

    pub(super) fn value_at(&self, index: usize) -> Option<&[u8]> {
        #[cfg(test)]
        PROJECTED_VALUE_VISITS.with(|count| count.set(count.get().saturating_add(1)));
        match self {
            Self::Missing(_) => None,
            Self::Owned(values) => values.get(index).and_then(|value| value.as_deref()),
            Self::Shared { frame, ranges } => ranges
                .get(index)
                .and_then(|range| range.as_ref())
                .map(|range| &frame[range.clone()]),
        }
    }

    pub(super) fn into_owned(self) -> Vec<Option<Vec<u8>>> {
        match self {
            Self::Missing(field_count) => vec![None; field_count],
            Self::Owned(values) => values,
            Self::Shared { frame, ranges } => ranges
                .into_iter()
                .map(|range| range.map(|range| frame[range].to_vec()))
                .collect(),
        }
    }
}

pub(super) fn execute_hash_multi_get(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    key: &str,
    fields: &[String],
    projection_plan: &container_blocks::Utf8ProjectionPlan<'_>,
    projection_scratch: &mut container_blocks::Utf8ProjectionScratch,
) -> ExecuteOutcome {
    if drop_if_expired(cache, shard_id, shard, key) {
        return ExecuteOutcome {
            response: CommandResponse::Values {
                values: vec![None; fields.len()],
            },
            mutated: true,
        };
    }
    let routing_bucket = block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    let values = execute_hash_multi_get_projected(
        cache,
        block_store,
        shard_id,
        routing_bucket,
        shard,
        key,
        fields,
        projection_plan,
        projection_scratch,
    )
    .into_owned();
    ExecuteOutcome {
        response: CommandResponse::Values { values },
        mutated: false,
    }
}

pub(super) fn execute_hash_multi_get_projected(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    routing_bucket: u32,
    shard: &mut ShardState,
    key: &str,
    fields: &[String],
    projection_plan: &container_blocks::Utf8ProjectionPlan<'_>,
    projection_scratch: &mut container_blocks::Utf8ProjectionScratch,
) -> ProjectedHashValues {
    if let [field] = fields {
        if let Some(bytes) = shard.bucket_index.hashes.cached_timestamp_bytes(key, field) {
            #[cfg(test)]
            PROJECTED_FIELD_CACHE_HITS.with(|count| count.set(count.get().saturating_add(1)));
            return ProjectedHashValues::Owned(vec![Some(bytes.to_vec())]);
        }
    }
    let Some(hash_fields) = shard.bucket_index.hashes.get(key) else {
        return ProjectedHashValues::Missing(fields.len());
    };
    // Point reads and conditional writes share this invalidated-under-lock field cache.
    // An indexed field must still exist; negative, old or malformed cache entries do
    // not replace the authoritative packed-row lookup.
    if let [field] = fields {
        if hash_fields.get(field).is_some() {
            let cached = crate::alloc_probe::in_class(
                crate::alloc_probe::AllocClass::CacheRead,
                || cache.get_shared(&CacheKey::hash(shard_id, key, field)),
            );
            if let Ok(Some(frame)) = cached {
                if let Some(Some(value)) =
                    super::command_validation::decode_borrowed_cached_bytes(&frame)
                {
                    let range = if value.is_empty() {
                        Some(0..0)
                    } else {
                        (value.as_ptr() as usize)
                            .checked_sub(frame.as_ptr() as usize)
                            .and_then(|start| {
                                start.checked_add(value.len())
                                    .filter(|end| *end <= frame.len())
                                    .map(|end| start..end)
                            })
                    };
                    if let Some(range) = range {
                        #[cfg(test)]
                        PROJECTED_FIELD_CACHE_HITS.with(|count| {
                            count.set(count.get().saturating_add(1));
                        });
                        return ProjectedHashValues::Shared {
                            frame,
                            ranges: vec![Some(range)],
                        };
                    }
                }
            }
        }
    }
    // A packed row gives every present field the same address. Detect that common case
    // without allocating a per-command address map, fetch its frame once, and project all
    // requested fields from the retained bytes. Distinct legacy/per-field addresses keep
    // the established independent-read path. Both collections are sorted by field name, so
    // merge them once and retain presence in reusable scratch instead of binary-searching the
    // field map twice per requested slot.
    let mut shared: Option<(&ElementEntry, &str)> = None;
    let mut distinct_addresses = false;
    projection_scratch.reset_index_presence(fields.len());
    let mut stored = hash_fields.entries().peekable();
    for &index in projection_plan.positions_by_name() {
        let field = fields[index].as_str();
        while stored.peek().is_some_and(|(name, _)| &name[..] < field) {
            stored.next();
        }
        let Some(&(name, address)) = stored.peek() else {
            break;
        };
        if &name[..] != field {
            continue;
        }
        projection_scratch.mark_present_in_index(index);
        match shared {
            None => shared = Some((address, field)),
            Some((held, _)) if held == address => {}
            Some(_) => {
                distinct_addresses = true;
                break;
            }
        }
    }
    if !distinct_addresses {
        if let Some((address, first_field)) = shared {
            #[cfg(test)]
            PROJECTED_FRAME_FETCHES.with(|count| count.set(count.get().saturating_add(1)));
            let frame = read_block_frame_shared(
                cache,
                block_store,
                shard_id,
                address,
                BlockIdentity::of(shard_id, "hash", key, Some(first_field)),
                Some(routing_bucket),
            );
            let Some(frame) = frame else {
                return ProjectedHashValues::Missing(fields.len());
            };
            match container_blocks::project_utf8_container_element_ranges_with_plan(
                frame.as_ref(),
                projection_plan,
                projection_scratch,
            ) {
                container_blocks::Utf8ContainerRangeProjection::Values(mut ranges) => {
                    for (index, range) in ranges.iter_mut().enumerate() {
                        if !projection_scratch.is_present_in_index(index) {
                            *range = None;
                        }
                    }
                    ProjectedHashValues::Shared { frame, ranges }
                }
                container_blocks::Utf8ContainerRangeProjection::Corrupt(_) => {
                    note_corrupt_container_block();
                    ProjectedHashValues::Missing(fields.len())
                }
                container_blocks::Utf8ContainerRangeProjection::NotFramed
                | container_blocks::Utf8ContainerRangeProjection::OtherSpelling => {
                    ProjectedHashValues::Owned(
                        fields
                            .iter()
                            .enumerate()
                            .map(|(index, field)| {
                                projection_scratch
                                    .is_present_in_index(index)
                                    .then(|| select_container_value(frame.as_ref(), field))
                                    .flatten()
                            })
                            .collect(),
                    )
                }
            }
        } else {
            ProjectedHashValues::Missing(fields.len())
        }
    } else {
        ProjectedHashValues::Owned(
            fields
                .iter()
                .map(|field| {
                    hash_fields.get(field).and_then(|address| {
                        #[cfg(test)]
                        PROJECTED_FRAME_FETCHES.with(|count| {
                            count.set(count.get().saturating_add(1));
                        });
                        read_block_bytes(
                            cache,
                            block_store,
                            shard_id,
                            address,
                            BlockIdentity::of(shard_id, "hash", key, Some(field.as_str())),
                            Some(routing_bucket),
                        )
                    })
                })
                .collect(),
        )
    }
}

fn deduplicate_hash_entries(mut entries: Vec<(String, Vec<u8>)>) -> Vec<(String, Vec<u8>)> {
    let mut first_positions =
        std::collections::HashMap::<&str, usize>::with_capacity(entries.len());
    let mut has_duplicate = false;
    for (index, (field, _)) in entries.iter().enumerate() {
        match first_positions.entry(field.as_str()) {
            std::collections::hash_map::Entry::Occupied(_) => has_duplicate = true,
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(index);
            }
        }
    }
    if !has_duplicate {
        drop(first_positions);
        return entries;
    }

    // Preserve the first occurrence's position while moving the last occurrence's value into it.
    // The table borrows the names already owned by `entries`; no second copy of every field is
    // needed just to collapse a row before encoding it.
    let mut last_for_first = vec![None; entries.len()];
    for (index, (field, _)) in entries.iter().enumerate() {
        last_for_first[first_positions[field.as_str()]] = Some(index);
    }
    drop(first_positions);

    for (first, last) in last_for_first.iter().copied().enumerate() {
        let Some(last) = last else {
            continue;
        };
        if first != last {
            entries[first].1 = std::mem::take(&mut entries[last].1);
        }
    }
    let mut index = 0;
    entries.retain(|_| {
        let keep = last_for_first[index].is_some();
        index += 1;
        keep
    });
    entries
}

fn release_hash_entry_payloads(entries: &mut [(String, Vec<u8>)]) {
    for (_, value) in entries {
        *value = Vec::new();
    }
}

pub(super) struct HashIndexFiled(());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PackedFrameCache {
    Eager,
    ReadThrough,
}

fn write_packed_hash_entries(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    shard: &mut ShardState,
    key: &str,
    async_storage: bool,
    entries: Vec<(String, Vec<u8>)>,
    fields_are_unique: bool,
    shared_fields: Option<&super::container_blocks::SharedHashFields>,
    borrowed_values: Option<&[crate::types::BorrowedHashValue<'_>]>,
    frame_cache: PackedFrameCache,
) -> (Option<HashIndexFiled>, Vec<(String, Vec<u8>)>) {
    let mut packed_entries = if fields_are_unique {
        entries
    } else {
        deduplicate_hash_entries(entries)
    };
    if packed_entries.is_empty() {
        return (None, packed_entries);
    }
    let block_ordinal = || shard
        .bucket_index
        .hashes
        .get(key)
        .and_then(|fields| {
            packed_entries
                .iter()
                .find_map(|(field, _)| fields.get(field).and_then(ElementEntry::block_id))
                .or_else(|| {
                    fields
                        .values()
                        .filter_map(ElementEntry::block_id)
                        .max()
                        .map(|held| held.saturating_add(1))
                })
        })
        .filter(|ordinal| *ordinal <= crate::block_store::MAX_ADDRESSABLE_BLOCK_ID)
        .and_then(|ordinal| u32::try_from(ordinal).ok())
        .unwrap_or(0);
    let block = match borrowed_values {
        Some(values) => {
            debug_assert!(fields_are_unique && values.len() == packed_entries.len());
            crate::engine::container_blocks::encode_utf8_container_iter(packed_entries.iter().zip(values)
                .map(|((field, _), value)| (field.as_bytes(), value.0.as_ref())))
        }
        None => crate::engine::container_blocks::encode_utf8_container_block(&packed_entries),
    };
    // Probe only the first selected field, even when a new projection has no resident fields.
    // Existing homogeneous rows share its frame; partial projections use best-effort admission.
    let frame_cache = if frame_cache == PackedFrameCache::ReadThrough && block.len() < 8 * 1024
        && shard.bucket_index.hashes.get(key)
            .and_then(|fields| fields.get(&packed_entries[0].0))
            .is_some_and(|address| cache.peek(&CacheKey::page_with_slot(
                shard_id, address.block_slab_id(), address.offset(), address.length(),
                Some(routing_bucket))))
    {
        PackedFrameCache::Eager
    } else {
        frame_cache
    };
    let address = super::append_owned_value_of_object(
        cache,
        block_store,
        shard_id,
        block,
        object_id,
        routing_bucket,
        async_storage,
        frame_cache,
        block_ordinal,
    );
    let filed = match address {
        Ok(address) => {
            super::recorded_map::install_packed_hash_block_at_bucket(
                cache, shard, shard_id, routing_bucket, object_id, key,
                packed_entries.iter().map(|(field, _)| field.as_str()), address, shared_fields,
            );
            let mut invalidator = HashFieldCacheInvalidator::new(cache, shard_id, key);
            for (field, _) in &packed_entries {
                invalidator.invalidate(field);
            }
            Some(HashIndexFiled(()))
        }
        Err(_) => None,
    };
    release_hash_entry_payloads(&mut packed_entries);
    (filed, packed_entries)
}

pub(super) fn execute_hash_multi_set_reusing_metadata(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    async_storage: bool,
    pack_projection: bool,
    key: String,
    entries: Vec<(String, Vec<u8>)>,
) -> (ExecuteOutcome, Command, Option<HashIndexFiled>) {
    let routing_bucket = block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
    let object_id = stable_block_object_id(shard_id, "hash", &key);
    let (outcome, command, filed) = execute_hash_multi_set_reusing_metadata_at_bucket(
        cache,
        block_store,
        shard_id,
        routing_bucket,
        object_id,
        shard,
        async_storage,
        pack_projection,
        key,
        entries,
        false,
        None,
        PackedFrameCache::Eager,
    );
    (outcome, command, filed)
}

pub(super) fn execute_hash_multi_set_reusing_metadata_at_bucket(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    shard: &mut ShardState,
    async_storage: bool,
    pack_projection: bool,
    key: String,
    mut entries: Vec<(String, Vec<u8>)>,
    fields_are_unique: bool,
    shared_fields: Option<&super::container_blocks::SharedHashFields>,
    frame_cache: PackedFrameCache,
) -> (ExecuteOutcome, Command, Option<HashIndexFiled>) {
    let mut mutated = drop_if_expired(cache, shard_id, shard, &key);
    let contains_large_value =
        !pack_projection && entries.iter().any(|(_, value)| value.len() >= 8 * 1024);
    // Shared feature-row batches carry one projection and read it back together, so one packed
    // block avoids repeating the block header and physical append for every field. The packed
    // projection reader fetches that shared address once on both storage modes. Generic hash
    // writes retain their per-field representation; a large value still selects the packed
    // layout on every path so its scalar siblings stay with it under pressure.
    let (wrote, index_filed) = if pack_projection || contains_large_value {
        let (filed, returned_entries) = write_packed_hash_entries(
            cache,
            block_store,
            shard_id,
            routing_bucket,
            object_id,
            shard,
            &key,
            async_storage,
            entries,
            fields_are_unique,
            shared_fields,
            None,
            frame_cache,
        );
        entries = returned_entries;
        (filed.is_some(), filed)
    } else {
        let mut applied = 0usize;
        let mut highest_ordinal: Option<Option<u64>> = None;
        let mut invalidator = None;
        for (field, value) in &entries {
            let existing_ordinal = crate::engine::state::resident_element_name_address(
                shard, "hash", &key, field,
            )
            .and_then(ElementEntry::block_id);
            let block_ordinal = if let Some(existing) = existing_ordinal {
                u32::try_from(existing).unwrap_or(0)
            } else {
                // No deletion occurs inside this batch, so successful appends can advance the
                // live maximum without scanning all siblings again for each new field.
                let highest = highest_ordinal.get_or_insert_with(|| {
                    crate::engine::state::resident_highest_block_id(shard, "hash", &key)
                });
                let next = highest.map_or(0, |held| held.saturating_add(1));
                if next > crate::block_store::MAX_ADDRESSABLE_BLOCK_ID {
                    0
                } else {
                    u32::try_from(next).unwrap_or(0)
                }
            };
            let block = crate::engine::container_blocks::single_element_block("hash", field, value);
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                Some(field.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                if existing_ordinal.is_none() {
                    if let Some(highest) = highest_ordinal.as_mut() {
                        let assigned = u64::from(block_ordinal);
                        *highest = Some(highest.map_or(assigned, |held| held.max(assigned)));
                    }
                }
                let field_handle =
                    super::state::intern_shared(&mut shard.bucket_index.kind_pool, field);
                super::recorded_map::install_appended_element::<super::recorded_map::HashKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(field.clone()),
                    field_handle,
                    address.clone(),
                    true,
                    address,
                );
                invalidator
                    .get_or_insert_with(|| HashFieldCacheInvalidator::new(cache, shard_id, &key))
                    .invalidate(field);
                applied += 1;
            }
        }
        release_hash_entry_payloads(&mut entries);
        // Every applied scalar field was filed by install_element before its resident update.
        (applied > 0, (applied > 0).then_some(HashIndexFiled(())))
    };
    mutated |= wrote;

    (
        ExecuteOutcome {
            response: CommandResponse::Empty,
            mutated,
        },
        Command::HashMultiSet { key, entries },
        index_filed,
    )
}

pub(super) fn execute_packed_conditional_hash_write(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    async_storage: bool,
    key: String,
    timestamp_field: String,
    timestamp_seconds: i64,
    timestamp_nanos: i32,
    ttl_ms: Option<u64>,
    entries: Vec<(String, Vec<u8>)>,
) -> (ExecuteOutcome, Command) {
    let routing_bucket = block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
    let object_id = stable_block_object_id(shard_id, "hash", &key);
    let (outcome, command, _) = execute_packed_conditional_hash_write_at_bucket(
        cache,
        block_store,
        shard_id,
        routing_bucket,
        object_id,
        shard,
        async_storage,
        key,
        timestamp_field,
        timestamp_seconds,
        timestamp_nanos,
        ttl_ms,
        entries,
        false,
        None,
        None,
        PackedFrameCache::Eager,
    );
    (outcome, command)
}

pub(super) fn execute_packed_conditional_hash_write_at_bucket(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    routing_bucket: u32,
    object_id: u64,
    shard: &mut ShardState,
    async_storage: bool,
    key: String,
    timestamp_field: String,
    timestamp_seconds: i64,
    timestamp_nanos: i32,
    ttl_ms: Option<u64>,
    entries: Vec<(String, Vec<u8>)>,
    fields_are_unique: bool,
    shared_fields: Option<&super::container_blocks::SharedHashFields>,
    borrowed_values: Option<&[crate::types::BorrowedHashValue<'_>]>,
    frame_cache: PackedFrameCache,
) -> (ExecuteOutcome, Command, Option<HashIndexFiled>) {
    let mut mutated = drop_if_expired(cache, shard_id, shard, &key);
    let timestamp_hint = shard.bucket_index.hashes.cached_timestamp(&key, &timestamp_field);
    let loaded_timestamp = timestamp_hint.is_none().then(|| shard
        .bucket_index
        .hashes
        .get(&key)
        .and_then(|fields| fields.get(&timestamp_field))
        .and_then(|address| {
            // Consult the field cache only after the resident map confirms the field exists.
            // Ordinary hash writes and deletes invalidate this same key under the shard lock.
            cached_bytes_with(cache, CacheKey::hash(shard_id, &key, &timestamp_field), || {
                    read_block_frame_shared(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        BlockIdentity::of(shard_id, "hash", &key, Some(timestamp_field.as_str())),
                        Some(routing_bucket),
                    ).and_then(|frame| select_container_value(&frame, &timestamp_field))
            }, |timestamp| timestamp.map(super::recorded_map::TimestampHintValue::from_bytes))
        })).flatten();
    let stored_timestamp = timestamp_hint.unwrap_or_else(|| loaded_timestamp.as_ref().and_then(|value| value.parsed()));
    if stored_timestamp.is_some_and(|stored| stored >= (timestamp_seconds, timestamp_nanos)) {
        if let Some(value) = loaded_timestamp {
            shard.bucket_index.hashes.remember_timestamp_value(&key, &timestamp_field, value);
        }
        let mut metadata_entries = entries;
        release_hash_entry_payloads(&mut metadata_entries);
        return (
            ExecuteOutcome {
                response: CommandResponse::Integer { value: 0 },
                mutated,
            },
            Command::HashMultiSetIfTimestampOlder {
                key,
                timestamp_field,
                timestamp_seconds,
                timestamp_nanos,
                ttl_ms,
                entries: metadata_entries,
            },
            None,
        );
    }

    // Preserve the actual stored bytes, not the comparison argument: direct commands may
    // supply different timestamp bytes or duplicate fields (whose last value wins).
    let timestamp_value = match borrowed_values {
        Some(values) => entries.iter().zip(values).rev()
            .find(|((field, _), _)| field == &timestamp_field)
            .map(|(_, value)| value.0.as_ref()).filter(|value| value.len() <= 64)
            .map(|value| (super::recorded_map::TimestampHintValue::from_bytes(value),
                (value.len() > 24).then(|| value.to_vec()))),
        None => entries.iter().rev().find(|(field, _)| field == &timestamp_field)
            .filter(|(_, value)| value.len() <= 64)
            .map(|(_, value)| (super::recorded_map::TimestampHintValue::from_bytes(value),
                (value.len() > 24).then(|| value.clone()))),
    };
    let (index_filed, metadata_entries) = write_packed_hash_entries(
        cache,
        block_store,
        shard_id,
        routing_bucket,
        object_id,
        shard,
        &key,
        async_storage,
        entries,
        fields_are_unique,
        shared_fields,
        borrowed_values,
        frame_cache,
    );
    let packed_mutated = index_filed.is_some();
    mutated |= packed_mutated;
    if packed_mutated {
        if let Some((value, large_raw_value)) = timestamp_value {
            shard.bucket_index.hashes.remember_timestamp_value(&key, &timestamp_field, value);
            if let Some(value) = large_raw_value {
                if let Some(encoded) = encode_cached_response(&CommandResponse::Bytes { value: Some(value) }) {
                    cache.put_memory_only(CacheKey::hash(shard_id, &key, &timestamp_field), encoded);
                }
            }
        }
    }
    if let Some(ttl_ms) = ttl_ms {
        apply_hash_expiry(shard_id, routing_bucket, shard, &key, ttl_ms);
        mutated = true;
    }
    (
        ExecuteOutcome {
            response: CommandResponse::Integer { value: 1 },
            mutated,
        },
        Command::HashMultiSetIfTimestampOlder {
            key,
            timestamp_field,
            timestamp_seconds,
            timestamp_nanos,
            ttl_ms,
            entries: metadata_entries,
        },
        index_filed,
    )
}

pub(super) fn apply_hash_expiry(
    shard_id: ShardId,
    routing_bucket: u32,
    shard: &mut ShardState,
    key: &str,
    ttl_ms: u64,
) {
    let expires_at = resolve_now_ms().saturating_add(ttl_ms);
    for record_key in associated_record_keys(key) {
        if record_exists_exact(shard, &record_key) {
            crate::engine::set_expiry(shard, record_key, expires_at);
        }
    }
    // Reads check the deadline before cache lookup; the write already invalidates changed fields.
    // A deadline refresh alone must not scan every cache entry once per row.
    stage_meta_outcome(
        shard_id,
        "object",
        key,
        routing_bucket,
        routing_bucket,
        None,
        Some(expires_at),
        false,
    );
}

#[cfg(test)]
#[path = "tests/packed_hash_completion.rs"]
mod packed_hash_completion_tests;

#[cfg(test)]
mod packed_hash_entry_tests {
    use super::{deduplicate_hash_entries, release_hash_entry_payloads};

    #[test]
    fn duplicate_fields_keep_first_position_and_last_value() {
        let entries = vec![
            ("a".to_string(), b"old-a".to_vec()),
            ("b".to_string(), b"only-b".to_vec()),
            ("a".to_string(), b"new-a".to_vec()),
        ];

        assert_eq!(
            deduplicate_hash_entries(entries),
            vec![
                ("a".to_string(), b"new-a".to_vec()),
                ("b".to_string(), b"only-b".to_vec()),
            ],
        );
    }

    #[test]
    fn released_metadata_keeps_names_without_retaining_payload_capacity() {
        let mut entries = vec![
            ("event_timestamp".to_string(), vec![1; 32]),
            ("embedding".to_string(), vec![2; 256 * 1024]),
        ];

        release_hash_entry_payloads(&mut entries);

        assert_eq!(
            entries
                .iter()
                .map(|(field, value)| (field.as_str(), value.len(), value.capacity()))
                .collect::<Vec<_>>(),
            vec![("event_timestamp", 0, 0), ("embedding", 0, 0)],
        );
    }
}

/// Replaces every point of `key`'s series stamped in `[start_ms, end_ms]` with `points`, trims
/// to the feature ceiling and restates the bucket index with the live blocks. Returns whether
/// anything changed, OR-ed with `mutated`. `FeatureReplace` and `CellRollup` replace a range the
/// same way.
#[allow(clippy::too_many_arguments)]
fn replace_feature_range(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    points: Vec<FeaturePoint>,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
    feature_max_size: usize,
    trim_boundary_record: bool,
    mut mutated: bool,
) -> bool {
    let routing_bucket =
        block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    // HOISTED ABOVE THE SERIES BORROW. `features_mut()` borrows the whole
    // `ShardState` where `&mut shard.features` borrowed one field, so this read of
    // `shard.bucket_index` can no longer sit inside the append call below -- the
    // two borrows are no longer disjoint. Taken here, before the series is
    // borrowed, which is also the order this block reads best in.
    let next_block_index = crate::engine::state::next_block_index_for_object(
        &shard.bucket_index,
        routing_bucket,
        "feature",
        key,
    );
    let series = shard
        .features_mut()
        .entry(std::sync::Arc::from(key))
        .or_default();
    let replaced = series
        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
        .map(|(timestamp_ms, _)| timestamp_ms)
        .collect::<Vec<_>>();
    mutated |= drop_timestamped_points(
        shard_id,
        "feature",
        key,
        routing_bucket,
        series,
        &replaced,
    );
    let points = sorted_feature_points(points);
    if let Ok(addresses) = append_timestamped_kv_blocks(
        cache,
        block_store,
        shard_id,
        "feature",
        key,
        points,
        routing_bucket,
        async_storage,
        true,
        next_block_index,
    ) {
        for (timestamp_ms, address) in addresses {
            series.set(timestamp_ms, address);
            mutated = true;
        }
    }
    mutated |= trim_timestamped_series(
        shard_id,
        "feature",
        key,
        routing_bucket,
        series,
        feature_max_size,
        trim_boundary_record,
    );
    let live_addresses = series.addresses().cloned().collect::<Vec<_>>();
    sync_bucket_index_object_blocks(
        shard,
        shard_id,
        "feature",
        key,
        live_addresses,
        mutated,
    );
    mutated
}

pub(crate) fn execute_on_shard(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    feature_max_size: usize,
    async_storage: bool,
    control_rollup_enabled: bool,
    control_coalesce_persist: bool,
    control_state_max_size: usize,
    control_distinct_sketch: bool,
    trim_boundary_record: bool,
    set_listing_from_map: bool,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    // Publish per-execute hints so the write helpers don't need the flags threaded through
    // every control-state write arm.
    shard.control_coalesce_persist = control_coalesce_persist;
    shard.control_state_max_size = control_state_max_size;
    shard.control_distinct_sketch = control_distinct_sketch;
    shard.set_listing_from_map = set_listing_from_map;
    let mut mutated = false;
    let command = match command {
        Command::HashMultiSetIfTimestampOlder {
            key,
            timestamp_field,
            timestamp_seconds,
            timestamp_nanos,
            ttl_ms,
            entries,
        } => {
            return execute_packed_conditional_hash_write(
                cache,
                block_store,
                shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                async_storage,
                key,
                timestamp_field,
                timestamp_seconds,
                timestamp_nanos,
                ttl_ms,
                entries,
            )
            .0
        }
        command => command,
    };
    let response = match command {
        // Applied as nothing: `mutated` stays false, so it neither dirties a shard nor
        // invalidates a cache entry.
        Command::LeaderEstablish => CommandResponse::Empty,
        // Blob commands are dispatched before the shard lock (they live beside the engine,
        // not in shard record state); reaching this arm means the early dispatch was skipped.
        Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. } => CommandResponse::Empty,
        Command::CommonDelete { key } => {
            // An outcome with no block: the object is gone. Their log item states the same thing
            // with `object_deleted`, and replay applies it without re-running a delete.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            // Expired-but-unswept is "missing" here, exactly as `CommonPersist` treats it
            // and exactly as reads do. Without this, `record_exists_exact` below still sees
            // the unswept record and the deadline is RE-ARMED, bringing back a key that
            // every read already answers is gone.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let expires_at = resolve_now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    crate::engine::set_expiry(shard, record_key, expires_at);
                }
            }
            mutated = true;
            // The deadline is recorded ALREADY RESOLVED. That is the point: a replay applying
            // this outcome needs no clock of its own, where re-running the command would resolve
            // the TTL against the restart clock and extend every recently-expiring key.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                Some(expires_at),
                false,
            );
            invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
            CommandResponse::Empty
        }
        Command::CommonPersist { key } => {
            // Expired-but-unswept is "missing" here, exactly as reads treat it: removing the
            // sweep's pending work must not resurrect a value whose deadline already passed.
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                // The expiry helper has already recorded each exact deletion.
                CommandResponse::Integer { value: 0 }
            } else {
                let mut removed = false;
                for record_key in associated_record_keys(&key) {
                    if crate::engine::clear_expiry(shard, &record_key)
                        && record_exists_exact(shard, &record_key)
                    {
                        removed = true;
                    }
                }
                if removed {
                    mutated = true;
                    stage_meta_outcome(
                        shard_id,
                        "object",
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        None,
                        None,
                        false,
                    );
                    invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
                }
                CommandResponse::Integer {
                    value: i64::from(removed),
                }
            }
        }
        Command::CommonTtl { key } => {
            // Take the answer from the COLLECTION, the way the other thirty-six arms do, rather
            // than from a second and narrower predicate beside it. `ttl_ms` calls
            // `remove_if_expired`, which collects the key AND its three control-state families
            // (`associated_record_keys`). Asking `expires_at_ms` about the bare key alone
            // answered a narrower question than the collection actually performed, so a TTL that
            // collected an expired `control_state:<family>:<key>` record reported no mutation:
            // the record left memory and no WAL tombstone was ever appended for it, leaving it to
            // come back on the next recovery after the caller had been told it was gone. An
            // expiry deletion is a LOGGED deletion on this path as much as in the sweep.
            //
            // `drop_if_expired` is the one spelling the other arms use. It also drops the cached
            // copy of the record it removes -- which the inline predicate never did, so the next
            // read could answer from the cache of a record just collected -- and it reads the
            // same replay-aware clock as every other deadline check, where the inline test read
            // the live clock instead.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let value = ttl_ms(shard, &key);
            CommandResponse::Integer { value }
        }
        Command::CommonExists { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: if record_exists(shard, &key) { 1 } else { 0 },
            }
        }
        Command::StringSet { key, value } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let object_id = stable_block_object_id(shard_id, "string", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_string_value(
                cache,
                block_store,
                shard_id,
                &value,
                object_id,
                routing_bucket,
                async_storage,
            ) {
                upsert_bucket_index_block(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                // ONE COPY OF THE CHARACTERS. The block was filed on the line above, so the
                // lookup holds a handle for this object now -- take it rather than allocating a
                // second copy of the same text. Allocate only when it has none, which is the
                // shape `stream_batch_methods` already uses for this very map.
                let stored_key = shard
                    .bucket_index
                    .any_shared_object_key(key.as_str())
                    .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                shard.set_string_address(stored_key, &address);
                // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                // carries the STORED length, so a denominator taken from the index would be
                // compressed bytes; this is what the caller handed us, before the codec.
                shard.set_string_logical_size(key.as_str(), value.len() as u64);
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let object_id = stable_block_object_id(shard_id, "string", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_string_value(
                cache,
                block_store,
                shard_id,
                &value,
                object_id,
                routing_bucket,
                async_storage,
            ) {
                upsert_bucket_index_block(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                // ONE COPY OF THE CHARACTERS. The block was filed on the line above, so the
                // lookup holds a handle for this object now -- take it rather than allocating a
                // second copy of the same text. Allocate only when it has none, which is the
                // shape `stream_batch_methods` already uses for this very map.
                let stored_key = shard
                    .bucket_index
                    .any_shared_object_key(key.as_str())
                    .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                shard.set_string_address(stored_key, &address);
                // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                // carries the STORED length, so a denominator taken from the index would be
                // compressed bytes; this is what the caller handed us, before the codec.
                shard.set_string_logical_size(key.as_str(), value.len() as u64);
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                // This write sets a value AND a deadline. Recording only the block passes a probe
                // that asks whether the record said anything, and produces a recovered key that
                // never expires -- so the deadline is recorded too, already resolved, exactly as
                // CommonExpire records its own.
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetConditional {
            key,
            value,
            ttl_ms,
            condition,
            return_old,
            keep_ttl,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let old_value = shard.string_address(key.as_str()).and_then(|address| {
                read_block_bytes(
                    cache,
                    block_store,
                    shard_id,
                    &address,
                    // `shard.strings`: a whole-object block, so the element is `None` and the
                    // object is the key itself.
                    BlockIdentity::of(shard_id, "string", &key, None),
                    Some(block_routing_bucket(
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                )
            });
            let exists = old_value.is_some();
            let should_set = match condition {
                StringSetCondition::Always => true,
                StringSetCondition::IfExists => exists,
                StringSetCondition::IfNotExists => !exists,
            };
            if should_set {
                let object_id = stable_block_object_id(shard_id, "string", &key);
                let routing_bucket =
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                if let Ok(address) = append_string_value(
                    cache,
                    block_store,
                    shard_id,
                    &value,
                    object_id,
                    routing_bucket,
                    async_storage,
                ) {
                    upsert_bucket_index_block(
                        shard,
                        shard_id,
                        "string",
                        &key,
                        None,
                        address.clone(),
                        true,
                    );
                    // ONE COPY OF THE CHARACTERS. The block was filed on the line above, so the
                    // lookup holds a handle for this object now -- take it rather than allocating a
                    // second copy of the same text. Allocate only when it has none, which is the
                    // shape `stream_batch_methods` already uses for this very map.
                    let stored_key = shard
                        .bucket_index
                        .any_shared_object_key(key.as_str())
                        .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                    shard.set_string_address(stored_key, &address);
                    // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                    // carries the STORED length, so a denominator taken from the index would be
                    // compressed bytes; this is what the caller handed us, before the codec.
                    shard.set_string_logical_size(key.as_str(), value.len() as u64);
                    if let Some(ttl_ms) = ttl_ms {
                        let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                        crate::engine::set_expiry(shard, key.clone(), expires_at);
                        // A conditional write that refreshes a deadline records the refreshed
                        // one. Recording only the block leaves a replay installing the value over
                        // a LAPSED deadline from an earlier record, and the key reads as expired
                        // even though the leader kept it alive.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            Some(expires_at),
                            false,
                        );
                    } else if keep_ttl {
                        // KEEPTTL: THE DEADLINE IS NOT TOUCHED, AND THAT IS A RESULT TOO.
                        //
                        // `remove_if_expired` at the top of this arm has already taken the key
                        // away if its deadline had lapsed, so whatever is in `expires_at_ms`
                        // here is a LIVE deadline -- this cannot preserve a dead one.
                        //
                        // What is staged is the deadline the key ALREADY HOLDS, not `None`.
                        // Staging `None` would spell "this key has no deadline" to a replay,
                        // and the replay would install the new value over a cleared deadline --
                        // turning the one option whose whole purpose is to keep the countdown
                        // into the one that loses it, but only after a restart.
                        let kept = shard.expires_at_ms().get(key.as_str()).copied();
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            kept,
                            false,
                        );
                    } else {
                        crate::engine::clear_expiry(shard, &key);
                        // No deadline is equally a result. An object outcome carrying neither a
                        // deadline nor a deletion says exactly that.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            None,
                            false,
                        );
                    }
                    mutated = true;
                }
                invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            }
            if return_old {
                CommandResponse::Bytes { value: old_value }
            } else {
                CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                }
            }
        }
        Command::StringGet { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            // THE PAYLOAD IS NOT CACHED A SECOND TIME UNDER THE LOGICAL KEY.
            //
            // A point read used to fill two cache layers that both hold the payload: this response
            // under `CacheKey::string(shard, key)`, and the block frame under
            // `CacheKey::page_with_slot(...)` inside the read below. That measured
            // `2 * value + envelope` bytes an entry at exactly 2.00 fills per key.
            //
            // THE FRAME IS THE COPY WORTH KEEPING, on three counts. `cached_response` stores with
            // `put_memory_only`, so this copy was memory-only, while the frame goes through
            // `cache.put` and is tiered and persistent -- `part2` asserts a tiny memory cache
            // refills from that tier without a block-store read. The frame key is read by the 23
            // callers of `read_block_bytes` and the two of `read_block_frame_bytes`; this key, for
            // a string, had exactly one reader. And this copy was the LARGER of the two, by the
            // msgpack response envelope.
            //
            // WHAT IT COSTS: a repeat read no longer short-circuits here, so it pays the
            // resident-map lookup and the element selection again. It must pay no extra
            // block-store read -- the frame entry is what it finds -- and that is asserted rather
            // than assumed by the cache arms that measure this path.
            CommandResponse::Bytes {
                value: read_bucket_index_value(
                    cache,
                    block_store,
                    shard_id,
                    shard,
                    "string",
                    &key,
                    None,
                ),
            }
        }
        Command::StringDelete { key } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            stage_meta_outcome(
                shard_id,
                "string",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated |= delete_record(shard, &key);
            let _ = cache.invalidate_record(shard_id, "string", &key);
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let object_id = stable_block_object_id(shard_id, "hash", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a block id mismatch.
            //
            // AND THE QUESTION GOES TO THE MAP THAT IS KEYED BY THE FIELD, inside the function.
            // `container_block_ordinal` used to find an existing field's position by looking for an
            // entry filed under that field's element name; a block-named entry carries none, so the
            // walk fell through to `highest + 1` on every overwrite -- a fresh ordinal, a fresh
            // address, a fresh block, for a write that should land on the same one. Hash is the one
            // container kind that overwrites a field IN PLACE this way.
            //
            // THAT IS ONE BRANCH, NOT TWO, NOW. This arm used to read `shard.bucket_index.hashes` itself under
            // the gate and call the function without it; the function reads the resident map
            // itself, so the two arms of that `if` computed the same value and the gate decided
            // nothing. The rule lives in one place -- see `state::container_block_ordinal`, which
            // also records why the resident map answers the high-water half as well.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, "hash", &key, &field);
            // The block STATES which field it is, rather than leaving that to the entry that names
            // it. Same value, one frame around it -- see `container_blocks`.
            let block =
                crate::engine::container_blocks::single_element_block("hash", &field, &value);
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block, and since the frame
                // above it is also the key INSIDE the block.
                Some(field.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // ONE HANDLE FROM THE POOL, for the name this map keeps. The element name
                // argument below still carries its own owned copy; that position is the
                // bucket index's and is converted separately.
                let field_handle =
                    super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
                super::recorded_map::install_appended_element::<super::recorded_map::HashKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(field.clone()),
                    field_handle,
                    address.clone(),
                    true,
                    address,
                );
                mutated = true;
            }
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::HashGet { key, field } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            if let Some(value) = shard.bucket_index.hashes.cached_timestamp_bytes(&key, &field) {
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: Some(value.to_vec()) },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::hash(shard_id, &key, &field), || {
                CommandResponse::Bytes {
                    // UNDER ONE ENTRY A BLOCK THE INDEX CANNOT NAME THIS FIELD, so the index-only
                    // read would answer `None` for a field that is there. `shard.bucket_index.hashes` is
                    // durable (`#[serde(default)]`, see `RecordedMap`'s unconditional
                    // `Serialize`) and already the address `HashMultiGet` reads a field from --
                    // same lookup, used here as the point read's source of truth under the gate.
                    value: shard
                        .bucket_index
                        .hashes
                        .get(&key)
                        .and_then(|fields| fields.get(&field))
                        .and_then(|address| {
                            read_block_bytes(
                                cache,
                                block_store,
                                shard_id,
                                address,
                                BlockIdentity::of(shard_id, "hash", &key, Some(field.as_str())),
                                Some(block_routing_bucket(
                                    &key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        }),
                }
            })
        }
        Command::HashMultiGet { key, fields } => {
            let projection_plan = container_blocks::Utf8ProjectionPlan::new(&fields);
            let mut projection_scratch = container_blocks::Utf8ProjectionScratch::new();
            return execute_hash_multi_get(
                cache,
                block_store,
                shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                &key,
                &fields,
                &projection_plan,
                &mut projection_scratch,
            );
        }
        Command::HashMultiSet { key, entries } => {
            return execute_hash_multi_set_reusing_metadata(
                cache,
                block_store,
                shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                async_storage,
                false,
                key,
                entries,
            )
            .0;
        }
        Command::HashMultiSetWithTtl { key, entries, ttl_ms } => {
            let (outcome, _, _) = execute_hash_multi_set_reusing_metadata(
                cache,
                block_store,
                shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                async_storage,
                false,
                key.clone(),
                entries,
            );
            if outcome.mutated {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            return outcome;
        }
        Command::HashMultiSetIfTimestampOlder { .. } => unreachable!(
            "conditional hash writes are handled before command dispatch"
        ),
        Command::HashIncrBy {
            key,
            field,
            increment,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // UNDER ONE ENTRY A BLOCK THE INDEX CANNOT NAME THIS FIELD, so the index-only read
            // would miss a present counter through `unwrap_or_default()` and silently restart it
            // at zero -- this is the one HASH surface among the four that WRITES on a miss, so
            // the wrong answer is not just served, it is persisted. `shard.bucket_index.hashes` is durable and
            // already the address `HashMultiGet`/the gated `HashGet` read a field from.
            let current = shard
                .bucket_index
                .hashes
                .get(&key)
                .and_then(|fields| fields.get(&field))
                .and_then(|address| {
                    read_block_bytes(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        BlockIdentity::of(shard_id, "hash", &key, Some(field.as_str())),
                        Some(block_routing_bucket(
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                })
                .and_then(|bytes| parse_i64(&bytes))
                .unwrap_or_default();
            let value = current.saturating_add(increment);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a block id mismatch.
            //
            // AND THE POSITION COMES FROM THE RESIDENT MAP, inside the function -- see `HashSet`'s
            // identical site and `state::container_block_ordinal` for why the index cannot say.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, "hash", &key, &field);
            // The block STATES which field it is -- see `container_blocks`.
            let block = crate::engine::container_blocks::single_element_block(
                "hash",
                &field,
                value.to_string().as_bytes(),
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(stable_block_object_id(shard_id, "hash", &key)),
                // The element this block holds -- the same expression that derived the ordinal
                // above, and now also the key INSIDE the block. The object id beside it no longer
                // names the element: #2019 made it the object's, so the ordinal, the element name and
                // the frame are the three things that say which element this is.
                Some(field.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // ONE HANDLE FROM THE POOL, for the name this map keeps. The element name
                // argument below still carries its own owned copy; that position is the
                // bucket index's and is converted separately.
                let field_handle =
                    super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
                super::recorded_map::install_appended_element::<super::recorded_map::HashKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(field.clone()),
                    field_handle,
                    address.clone(),
                    true,
                    address,
                );
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                mutated = true;
            }
            CommandResponse::Integer { value }
        }
        Command::HashGetAll { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            // THE TWO SOURCES THAT NAME THIS OBJECT'S FIELDS, AND WHY THIS READ NOW ASKS BOTH.
            //
            // The block index has always been the source this read resolves field names from, and
            // the durable container `shard.bucket_index.hashes` names the same fields from the other side.
            // #2078 set out to prove the container holds every field the fold installs, and it
            // does not prove that: BOTH load-path functions in `storage_bucket_internals` derive a
            // hash field map by WALKING THE BLOCK INDEX and merge it into the container through
            // `RecordedHashContainer::reconcile_from_durable` --
            // `rebuild_unserialized_model_maps_from_bucket_index` and
            // `reconcile_secondary_views_from_bucket_index`. So after a reload the container names
            // what the index names BY CONSTRUCTION, the fixture compares the index against a
            // structure derived from it, and disabling the container carry leaves it green.
            //
            // CITED BY FUNCTION AND NOT BY LINE, because this change watched a line citation go
            // stale while it was being written: the second site sat at `:5229` on the commit this
            // was authored against and at `:5248` two commits later, moved by #2085.
            //
            // So the precondition is established HERE instead, at run time against whatever
            // traffic the process serves. That samples every route a request takes rather than
            // the ones a fixture drives, and no load path sits between the two sources to fill
            // one from the other.
            //
            // SERVING THE UNION IS WHAT MAKES THAT SAFE. A field only one source names is still
            // answered, so a divergence is OBSERVED rather than SUFFERED and no read is lost
            // while the measurement is being taken.
            let indexed = bucket_index_element_name_block_addresses(shard, "hash", &key);
            // READ IN PLACE. The block reads below take the cache and the block store and nothing
            // from the shard, so the container is borrowed for the whole answer instead of being
            // copied out first: that copy was a name `String` and an address per field, spent
            // before the first block was read and then spent again to build the answer.
            let container = shard.bucket_index.hashes.get(&key);

            // THE UNION, KEYED ON THE FIELD NAME, AND WHICH SIDE WINS A TIE.
            //
            // The INDEX wins. That makes this a no-op for every field the index names -- which
            // today is every field -- and leaves the container contributing exactly the fields
            // the index does not name. A union that preferred the container would change the
            // address served for every read in the store, and a change whose purpose is to
            // measure is not entitled to do that.
            //
            // BORROWED, NOT COPIED. Each name below is held as a `&str` of the entry or the
            // container it came from, and the only `String` built per field is the one the answer
            // returns. The union used to spell every container field three times -- once to copy it
            // out, once as an `Arc<str>` to name its block, once more to return -- and to key a
            // set with a clone of each index name.
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            let mut page_keys = crate::engine::PageKeys::new();
            let mut entries: Vec<(String, Vec<u8>)> =
                Vec::with_capacity(indexed.len() + container.map_or(0, |fields| fields.len()));
            let mut read_field =
                |name: &str, address: &crate::block_store::ElementEntry| {
                    let cache_key = page_keys.key(shard_id, address, Some(routing_bucket));
                    if let Some(value) = read_block_value_keyed(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        // The element the whole-object door handed back beside this address.
                        BlockIdentity::of(shard_id, "hash", &key, Some(name)),
                        cache_key,
                    ) {
                        entries.push((name.to_string(), value));
                    }
                };
            let mut named_by_index: std::collections::HashSet<&str> =
                std::collections::HashSet::with_capacity(indexed.len());
            for (element_name, address) in &indexed {
                // A `None` ELEMENT NAMES NO FIELD, SO IT IS NOT SERVED AS ONE.
                //
                // THIS USED TO DEFAULT IT TO `""` AND SERVE IT. The paragraph here said a nameless
                // entry "is the whole-object door's answer for a block that belongs to the object
                // rather than to an element of it ... It is served exactly as it always was", and
                // that was survivable only while every hash entry carried a field name. Under one
                // entry a block NO hash entry carries one, so every block of the object was served as
                // a field called `""` whose value was the raw block FRAME.
                //
                // MEASURED, and it is the defect that refused this collapse twice: a two-field hash
                // answered FOUR entries -- `("f1", ..), ("f3", ..)` and two `("", b"TSCPG2\n...")`
                // -- which `conformance_oracle_matches_reference_model` reports against the
                // reference model and `a_command_of_one_type_answers_empty_for_a_key_of_another`
                // reports as a hash that is not the hash that was written. It also served `hash: 41`
                // of 40 on a forty-field corpus, which is one phantom per folded block.
                //
                // AN EMPTY FIELD NAME IS LEGAL, which is exactly why the absent one must not spell
                // it: defaulting here collides with a genuine `""` field and takes its value. This
                // is the same `unwrap_or_default()` correction the four derived-view arms already
                // carry, arriving on the READ path. The field a nameless block holds is named inside
                // the block's own frame and in `shard.bucket_index.hashes`, and the union below serves it from
                // there -- so nothing is lost by skipping, only a name that was never written.
                let Some(name) = element_name.as_deref() else {
                    continue;
                };
                named_by_index.insert(name);
                read_field(name, address);
            }

            // THE MEASUREMENT. See `hash_read_divergence` for what a zero from it does and does
            // not mean; the short form is that a zero is a statement about the routes traffic
            // took and NOT a proof of completeness on the routes it did not.
            {
                for name in named_by_index
                    .iter()
                    .filter(|name| !container.is_some_and(|fields| fields.contains_key(name)))
                {
                    hash_read_divergence::note(&key, name);
                }
                hash_read_divergence::note_read();
            }

            // The container's own fields that the index does not name. These are the reads the
            // union ADDS: without it they are not served at all, which is why the union is the
            // part that makes observing a divergence safe rather than merely informative.
            //
            // The routing bucket is the object's, so it is one value for every field; and the
            // fields of one hash sit in few slabs, so the page keys share their record key.
            if let Some(fields) = container {
                for (name, address) in fields.iter() {
                    if !named_by_index.contains(&**name) {
                        read_field(&**name, address);
                    }
                }
            }
            CommandResponse::HashEntries { entries }
        }
        Command::HashLen { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                // UNDER ONE ENTRY A BLOCK THE INDEX NAMES NO FIELD, so a count of index entries is
                // THE INDEX NAMES NO FIELD, so a count of index entries is a count of BLOCKS, not
                // fields -- a wrong number rather than an empty answer. The resident map is durable
                // and keyed by field, so it is the count. The entry-counting arm this replaces is
                // deleted rather than left behind a condition: there is no state in which an entry
                // names a field for it to count.
                value: shard
                    .bucket_index
                    .hashes
                    .get(&key)
                    .map(|fields| fields.len())
                    .unwrap_or(0) as i64,
            }
        }
        Command::HashDelete { key, field } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // Mirror hash2::Del: deleting the last field removes the whole key (DeleteObject
            // on empty). Leaving an empty field map behind makes the key still report as existing
            // (EXISTS=1, TYPE=hash) -- a phantom hash. That cleanup now lives inside
            // `RecordedHashContainer::remove_field`, so no arm can forget it.
            //
            // AND "THE MAP FORGOT IT" IS NOT "THE KEYSPACE FORGOT IT". The sentence above is true
            // and it is about ONE SOURCE: `record_exists_exact` ORs the bucket index in beside the
            // resident map, so the resident map going empty does not stop the key answering. A
            // gated removal keeps the object's live block entry -- correctly, while the block still
            // holds other members -- and for the LAST member that left the key enumerable with
            // nothing in it across KEYS, SCAN, DBSIZE, EXISTS, TYPE and EXPIRE. The last-element
            // arm in `recorded_map::remove_element` is what empties the second source.
            //
            // THE GENERAL FORM, because this asymmetry has now decided two fixes: a change that
            // touches "the index entry for this object" WITHOUT SAYING WHICH KIND will keep
            // getting it wrong. A LIVE entry claims a block with members on it; a TOMBSTONE records
            // that a named element was removed. The first becomes false when the last member goes;
            // the second stays true, and it is what makes the removal win a fold by append
            // position -- so taking it along undoes the removal it records. The gate itself got
            // this right by naming tombstones and not live entries; the first attempt at the
            // cleanup above got it wrong by marking the whole object deleted, and a churn test
            // caught it at zero tombstones where one was required.
            //
            // THE PARENTHESIS THAT USED TO END THIS NOTE SAID SETS DID NOT NEED THE CLEANUP,
            // and it was wrong about this engine: `record_exists_exact` reads
            // `shard.bucket_index.sets.contains_key(key)` on the line after the one it reads
            // `shard.bucket_index.hashes.contains_key(key)` on, so a set left holding an empty member map is
            // the same phantom by the same reader. `SetRemove` does the cleanup now too.
            // FROM THE POOL RATHER THAN MINTED, even though this name is about to be
            // dropped: the write already interned it, so this is a lookup, and minting
            // here would allocate a copy whose only use is to be compared and discarded.
            let field_handle =
                super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
            let existed = shard.bucket_index.hashes.get(&key)
                .is_some_and(|fields| fields.contains_key(&field));
            mutated |= super::recorded_map::remove_element::<super::recorded_map::HashKind>(
                cache,
                block_store,
                shard,
                shard_id,
                &key,
                field.as_str(),
                field_handle,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Integer { value: i64::from(existed) }
        }
        Command::SetAdd { key, member } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let member_element_name = set_element_name(&member);
            let object_id = stable_block_object_id(shard_id, "set", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append because the
            // append stamps it into the record header, and read off the block index -- which is
            // where the element's own previous block, if it has one, already states its position.
            //
            // AND THE QUESTION GOES TO THE MAP THAT IS KEYED BY THE MEMBER, inside the function.
            // `container_block_ordinal` used to pick an element's existing position out of the INDEX
            // by element name, which a block-named entry does not carry -- so every rewrite fell
            // through to `highest + 1` and a member written five times left five blocks instead of
            // overwriting one (measured: two blocks and ordinals [0, 1] where one and [0] are
            // right). This arm therefore carried its own `shard.bucket_index.sets` lookup under the gate, with
            // the function called only when the map had no answer.
            //
            // THAT IS ONE BRANCH, NOT TWO, NOW. The function asks the resident map itself, so the
            // gated arm here and the ungated one computed the same value and the `if` decided
            // nothing -- a branch whose arms cannot differ, which no suite can report. The lookup
            // lives in the one place that owns the rule; see `state::container_block_ordinal`.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                "set",
                &key,
                &member_element_name,
            );
            // The block STATES which member it is -- see `container_blocks`. For a set the element
            // key IS the member and so is the value, which the frame stores once: #2017 measured
            // that redundancy and this is the stage that stops paying it twice.
            let block = crate::engine::container_blocks::single_element_block(
                "set",
                &member_element_name,
                &member,
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(member_element_name.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                super::recorded_map::install_appended_element::<super::recorded_map::SetKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(member_element_name.clone()),
                    member.clone(),
                    address.clone(),
                    true,
                    address,
                );
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
            CommandResponse::Empty
        }
        Command::ZSetAdd { key, member, score } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let biased = zset_score_bits(score);
            let existed = shard
                .bucket_index
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(old_biased, _)| *old_biased);
            if existed == Some(biased) {
                // Same score: nothing to rewrite, and the answer is still "not new".
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if let Some(old_biased) = existed {
                let old_element_name = zset_element_name(&member);
                remove_container_element(
                    cache,
                    block_store,
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    &old_element_name,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                );
            }
            let element_name = zset_element_name(&member);
            let object_id = stable_block_object_id(shard_id, "zset", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a block id mismatch.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, "zset", &key, &element_name);
            // The block STATES which element it is -- see `container_blocks`. A zset's element key is
            // its biased score followed by its member, and the value IS that member, so the frame
            // stores the member once and says where inside the key it starts.
            let block = crate::engine::container_blocks::single_element_block(
                "zset",
                &element_name,
                &member,
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(element_name.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // THE STANDALONE UPSERT IS GONE BECAUSE THE OPERATION DOES IT -- the same
                // correction `ZSetIncrBy` and `ListPush` already carry (see their own notes). It
                // stood here filing the SAME block under the SAME element name a second time, which
                // staged a second WAL outcome for one write: harmless while the element name carried
                // the score twice over identically, and no longer harmless now that only
                // `install_element`'s own filing carries the score (through
                // `RecordedKind::outcome_value`) -- a replay of the OTHER, standalone-filed
                // outcome would have found no score to install and refused the whole shard load.
                // One call now does both, record first.
                super::recorded_map::install_element::<super::recorded_map::ZSetKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(element_name.clone()),
                    member.clone(),
                    (biased, address.clone()),
                    true,
                    address,
                );
                mutated = true;
            }
            CommandResponse::Integer {
                value: i64::from(existed.is_none()),
            }
        }
        Command::ZSetScore { key, member } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard
                    .bucket_index
                    .zsets
                    .get(&key)
                    .and_then(|members| members.get(&member))
                    .map(|(biased, _)| zset_score_string(*biased).into_bytes()),
            }
        }
        Command::ZSetRemove { key, member } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // THE SCORE IS READ BEFORE THE REMOVAL RATHER THAN TAKEN FROM IT, and that is the
            // one ordering this conversion had to change rather than preserve.
            //
            // This arm used to drop the member from the resident map FIRST, because the removal's
            // own return value carried the biased score that the element name is rendered from -- so
            // the resident mutation preceded the durable record, which is the order a recorded
            // container exists to make unrepresentable. Reading the score with `get` costs one
            // lookup and lets the record go first.
            let biased = shard
                .bucket_index
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| *biased);
            match biased {
                None => CommandResponse::Integer { value: 0 },
                Some(biased) => {
                    let element_name = zset_element_name(&member);
                    mutated |= super::recorded_map::remove_element::<super::recorded_map::ZSetKind>(
                        cache,
                        block_store,
                        shard,
                        shard_id,
                        &key,
                        &element_name,
                        member.clone(),
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                    CommandResponse::Integer { value: 1 }
                }
            }
        }
        Command::ZSetCard { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.bucket_index.zsets.get(&key).map_or(0, BTreeMap::len) as i64,
            }
        }
        Command::ZSetRange {
            key,
            start,
            stop,
            rev,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let mut ordered = zset_ordered_refs(shard, &key);
            if rev {
                ordered.reverse();
            }
            let length = ordered.len() as i64;
            let resolve = |index: i64| if index < 0 { length + index } else { index };
            let from = resolve(start).max(0);
            let to = resolve(stop).min(length - 1);
            let members = if from > to || length == 0 {
                Vec::new()
            } else {
                ordered[from as usize..=to as usize]
                    .iter()
                    .flat_map(|(member, biased)| {
                        [member.to_vec(), zset_score_string(*biased).into_bytes()]
                    })
                    .collect()
            };
            CommandResponse::Members { members }
        }
        Command::ZSetRangeByScore {
            key,
            min,
            max,
            min_exclusive,
            max_exclusive,
            rev,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let min_bits = zset_score_bits(min);
            let max_bits = zset_score_bits(max);
            // The score test runs before the member bytes are copied, so a narrow window copies
            // a narrow window rather than the whole set.
            let mut ordered: Vec<(Vec<u8>, u64)> = zset_members_in_score_range(
                shard,
                &key,
                min_bits,
                max_bits,
                min_exclusive,
                max_exclusive,
            );
            if rev {
                ordered.reverse();
            }
            let members = ordered
                .into_iter()
                .flat_map(|(member, biased)| [member, zset_score_string(biased).into_bytes()])
                .collect();
            CommandResponse::Members { members }
        }
        Command::ZSetScan { key, after, limit } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            let members = shard
                .bucket_index
                .zsets
                .get(&key)
                .map(|members| {
                    if after.is_empty() {
                        members
                            .iter()
                            .take(limit)
                            .flat_map(|(member, (biased, _))| {
                                [member.clone(), zset_score_string(*biased).into_bytes()]
                            })
                            .collect::<Vec<_>>()
                    } else {
                        use std::ops::Bound::{Excluded, Unbounded};
                        members
                            .range::<[u8], _>((Excluded(after.as_slice()), Unbounded))
                            .take(limit)
                            .flat_map(|(member, (biased, _))| {
                                [member.clone(), zset_score_string(*biased).into_bytes()]
                            })
                            .collect::<Vec<_>>()
                    }
                })
                .unwrap_or_default();
            CommandResponse::Members { members }
        }
        Command::SeenCheck {
            key,
            member,
            window_ms,
        } => {
            // Lazy expiry, as every other kind's arms do. Without it a key reads as
            // live for the whole interval between its deadline and whichever sweep
            // round happens to collect it -- and these two fail CLOSED while it lasts:
            // a lapsed seen-set still answers "duplicate" and suppresses work that
            // should run, a lapsed bucket still answers "denied" and keeps rejecting a
            // caller whose limit was meant to have been discarded.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let now = resolve_now_ms();
            let floor = now.saturating_sub(window_ms);
            // No block backs a seen-set; it lives in the index snapshot. So the outcome carries
            // the member and the moment, which is everything an apply needs.
            stage_meta_outcome(
                shard_id,
                "seen",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(member.clone()),
                Some(now),
                false,
            );
            let seen = shard
                .seen
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            // Bounded sweep from the time-ordered front: enough to keep pace with any
            // sustained rate, never enough to stall a hot call on a huge backlog.
            for _ in 0..128 {
                match seen.by_time.first_key_value() {
                    Some(((seen_at, _), ())) if *seen_at < floor => {
                        let ((seen_at, expired), ()) =
                            seen.by_time.pop_first().expect("front exists");
                        if seen.by_member.get(&expired) == Some(&seen_at) {
                            seen.by_member.remove(&expired);
                        }
                    }
                    _ => break,
                }
            }
            let duplicate = seen
                .by_member
                .get(&member)
                .is_some_and(|seen_at| *seen_at >= floor);
            if !duplicate {
                if let Some(previous) = seen.by_member.insert(member.clone(), now) {
                    seen.by_time.remove(&(previous, member.clone()));
                }
                seen.by_time.insert((now, member), ());
            }
            mutated = true;
            CommandResponse::Integer {
                value: i64::from(duplicate),
            }
        }
        Command::SeenCard { key } => {
            // A count is a read like any other: a set whose deadline has passed counts zero,
            // not whatever it held when the deadline arrived. The removal is a real change to
            // the shard, so it is reported as one -- the same shape `StringGet` and
            // `CommonExists` use.
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard
                    .seen
                    .get(&key[..])
                    .map_or(0, |seen| seen.by_member.len()) as i64,
            }
        }
        Command::BucketTake {
            key,
            tokens,
            capacity,
            refill_per_sec,
        } => {
            // Lazy expiry, as every other kind's arms do. Without it a key reads as
            // live for the whole interval between its deadline and whichever sweep
            // round happens to collect it -- and these two fail CLOSED while it lasts:
            // a lapsed seen-set still answers "duplicate" and suppresses work that
            // should run, a lapsed bucket still answers "denied" and keeps rejecting a
            // caller whose limit was meant to have been discarded.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let now = resolve_now_ms();
            let current = shard.buckets.get(&key[..]).copied();
            let (allowed, remaining, retry_after_ms, next) =
                bucket_take(current, now, tokens, capacity, refill_per_sec);
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            shard.buckets.insert(std::sync::Arc::from(&key[..]), next);
            mutated = true;
            CommandResponse::Members {
                members: bucket_answer(allowed, remaining, retry_after_ms),
            }
        }
        Command::BucketPeek {
            key,
            tokens,
            capacity,
            refill_per_sec,
        } => {
            // A peek that drops a lapsed bucket has changed the shard, so it says so -- the
            // answer below then describes a bucket at full capacity, which is what the caller
            // would get from the next take.
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
            }
            let now = resolve_now_ms();
            let current = shard.buckets.get(&key[..]).copied();
            let (allowed, remaining, retry_after_ms, _) =
                bucket_take(current, now, tokens, capacity, refill_per_sec);
            CommandResponse::Members {
                members: bucket_answer(allowed, remaining, retry_after_ms),
            }
        }
        Command::ZSetIncrBy {
            key,
            member,
            increment,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let old = shard
                .bucket_index
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| *biased);
            let score = old.map_or(0.0, zset_score_from_bits) + increment;
            let biased = zset_score_bits(score);
            if let Some(old_biased) = old {
                let old_element_name = zset_element_name(&member);
                remove_container_element(
                    cache,
                    block_store,
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    &old_element_name,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                );
            }
            let element_name = zset_element_name(&member);
            let object_id = stable_block_object_id(shard_id, "zset", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a block id mismatch.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, "zset", &key, &element_name);
            // The block STATES which element it is -- see `container_blocks`.
            let block = crate::engine::container_blocks::single_element_block(
                "zset",
                &element_name,
                &member,
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(element_name.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // THIS ARM HAD THE TWO STATEMENTS IN THE UNSAFE ORDER. It inserted into the
                // resident map and THEN filed the bucket-index entry, so a panic between them left
                // a mutation with no record -- the exact defect class this container removes, found
                // in the engine rather than argued from. One call now does both, record first.
                super::recorded_map::install_element::<super::recorded_map::ZSetKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(element_name.clone()),
                    member.clone(),
                    (biased, address.clone()),
                    true,
                    address.clone(),
                );
                mutated = true;
            }
            CommandResponse::Bytes {
                value: Some(zset_score_string(biased).into_bytes()),
            }
        }
        Command::ZSetCountAdjust {
            key,
            member,
            increment,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let old = shard
                .bucket_index
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| *biased);
            let current = old
                .map(zset_score_from_bits)
                .unwrap_or_default()
                .round()
                .clamp(0.0, i64::MAX as f64) as i64;
            let count = current.saturating_add(increment).max(0);
            if old.is_some() {
                let element_name = zset_element_name(&member);
                let removal = remove_container_element(
                    cache,
                    block_store,
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    &element_name,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                );
                if let Some(address) = super::recorded_map::remove_replayed_element::<super::recorded_map::ZSetKind>(shard, &key, &member) {
                    if removal.tombstone_written {
                        super::storage_bucket_internals::drop_vacated_live_block(shard, "zset", &key, &address);
                    }
                }
                mutated = true;
            }
            if count > 0 {
                let biased = zset_score_bits(count as f64);
                let element_name = zset_element_name(&member);
                let object_id = stable_block_object_id(shard_id, "zset", &key);
                let routing_bucket =
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                let block_ordinal = crate::engine::state::container_block_ordinal(
                    shard,
                    "zset",
                    &key,
                    &element_name,
                );
                let block = crate::engine::container_blocks::single_element_block(
                    "zset",
                    &element_name,
                    &member,
                );
                if let Ok(address) = append_value_of_object(
                    cache,
                    block_store,
                    shard_id,
                    &block,
                    Some(object_id),
                    Some(element_name.as_str()),
                    Some(routing_bucket),
                    async_storage,
                    block_ordinal,
                ) {
                    super::recorded_map::install_element::<super::recorded_map::ZSetKind>(
                        shard,
                        shard_id,
                        &key,
                        Some(element_name),
                        member,
                        (biased, address.clone()),
                        true,
                        address.clone(),
                    );
                    mutated = true;
                }
            }
            CommandResponse::Integer { value: count }
        }
        Command::ZSetPop { key, min, count } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut ordered = zset_ordered_refs(shard, &key);
            if !min {
                ordered.reverse();
            }
            ordered.truncate(count as usize);
            // The borrows end here: the removals below take `&mut shard`, so the few members the
            // pop returns are copied out first. The clone count is the answer size, not the set size.
            let doomed: Vec<(Vec<u8>, u64)> = ordered
                .into_iter()
                .map(|(member, biased)| (member.to_vec(), biased))
                .collect();
            let mut members = Vec::new();
            for (member, biased) in doomed {
                let element_name = zset_element_name(&member);
                let removal = remove_container_element(
                    cache,
                    block_store,
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    &element_name,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                );
                if let Some(address) = super::recorded_map::remove_replayed_element::<super::recorded_map::ZSetKind>(shard, &key, &member) {
                    if removal.tombstone_written {
                        super::storage_bucket_internals::drop_vacated_live_block(shard, "zset", &key, &address);
                    }
                }
                mutated = true;
                members.push(member);
                members.push(zset_score_string(biased).into_bytes());
            }
            if mutated {}
            CommandResponse::Members { members }
        }
        Command::ZSetRank { key, member, rev } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let target = shard
                .bucket_index
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| (*biased, member.clone()));
            CommandResponse::Bytes {
                value: target.map(|(biased, member)| {
                    let before = shard
                        .bucket_index
                        .zsets
                        .get(&key)
                        .map(|members| {
                            members
                                .iter()
                                .filter(|(other, (other_biased, _))| {
                                    // Compare the parts directly. Building two owned tuples here
                                    // copied both member names on every member examined -- exactly
                                    // two allocations per member, for a comparison that needs none.
                                    let ordering = other_biased
                                        .cmp(&biased)
                                        .then_with(|| (*other).as_slice().cmp(member.as_slice()));
                                    if rev {
                                        ordering == std::cmp::Ordering::Greater
                                    } else {
                                        ordering == std::cmp::Ordering::Less
                                    }
                                })
                                .count()
                        })
                        .unwrap_or(0);
                    before.to_string().into_bytes()
                }),
            }
        }
        Command::ListPush { key, member, left } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // READ, NOT `entry().or_default()`, WHICH CLOSES #2087 BY CONSTRUCTION.
            //
            // This arm was the only container command that created its map entry BEFORE the append
            // that justifies it. A failed append therefore left an empty `BTreeMap` under the key,
            // and `record_exists_exact` ORs `shard.bucket_index.lists.contains_key(key)` into its answer -- so
            // the key reported EXISTS=1 and TYPE=list with no block behind it, and `CommonExpire`
            // gating on the same function accepted a deadline for it.
            //
            // The container makes the old shape inexpressible: reading the next sequence is a READ,
            // and the entry now comes into being only through the recorded install below.
            let seq = match shard.bucket_index.lists.get(&key) {
                Some(list) => {
                    if left {
                        list.keys().next().copied().map_or(0, |first| first - 1)
                    } else {
                        list.keys().next_back().copied().map_or(0, |last| last + 1)
                    }
                }
                None => 0,
            };
            // Two's-complement bias makes the hex element name sort lexically in list order,
            // which is what lets recovery and range reads walk the bucket index directly.
            let element_name = format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
            let object_id = stable_block_object_id(shard_id, "list", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a block id mismatch.
            let block_ordinal =
                crate::engine::state::container_block_ordinal(shard, "list", &key, &element_name);
            // The block STATES which element it is -- see `container_blocks`. A list's element key is
            // its biased sequence word, which is eight bytes and is NOT the value beside it, so
            // this is the one container kind whose frame carries both in full.
            let block = crate::engine::container_blocks::single_element_block(
                "list",
                &element_name,
                &member,
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(element_name.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                upsert_bucket_index_block(
                    shard,
                    shard_id,
                    "list",
                    &key,
                    Some(element_name.clone()),
                    address.clone(),
                    true,
                );
                super::recorded_map::install_element::<super::recorded_map::ListKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(element_name.clone()),
                    seq,
                    address.clone(),
                    true,
                    address,
                );
                mutated = true;
            }
            let length = shard.bucket_index.lists.get(&key).map_or(0, BTreeMap::len) as i64;
            CommandResponse::Integer { value: length }
        }
        Command::ListPop { key, left } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            // READ THE ELEMENT, THEN RECORD, THEN DROP -- the same reordering `ZSetRemove`
            // needed and for the same reason: this arm took the address out of the resident map
            // before the removal was recorded.
            let popped = shard.bucket_index.lists.get(&key).and_then(|list| {
                let seq = if left {
                    list.keys().next().copied()
                } else {
                    list.keys().next_back().copied()
                }?;
                list.get(&seq).map(|address| (seq, address.clone()))
            });
            match popped {
                None => CommandResponse::Bytes { value: None },
                Some((seq, address)) => {
                    let element_name =
                        format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64));
                    mutated = true;
                    super::recorded_map::remove_element::<super::recorded_map::ListKind>(
                        cache,
                        block_store,
                        shard,
                        shard_id,
                        &key,
                        &element_name,
                        seq,
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                    CommandResponse::Bytes {
                        value: read_block_bytes(
                            cache,
                            block_store,
                            shard_id,
                            &address,
                            // The element rendered two lines up, which is the one the index removal names.
                            BlockIdentity::of(shard_id, "list", &key, Some(element_name.as_str())),
                            Some(block_routing_bucket(
                                &key,
                                start_routing_bucket,
                                end_routing_bucket,
                            )),
                        ),
                    }
                }
            }
        }
        Command::ListRange { key, start, stop } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            // The length comes from the map itself -- `ListLen` below takes the same number the
            // same way. Copying every address to obtain it, and to index a slice of it, meant a
            // ten-entry block of a four-thousand-entry list copied four thousand addresses.
            let list = shard.bucket_index.lists.get(&key);
            let length = list.map_or(0, |list| list.len()) as i64;
            let resolve = |index: i64| -> i64 {
                if index < 0 {
                    length + index
                } else {
                    index
                }
            };
            let from = resolve(start).max(0);
            let to = resolve(stop).min(length - 1);
            let members = if from > to || length == 0 {
                Vec::new()
            } else {
                // A BTreeMap iterates in key order, which is the list's order -- the same order the
                // materialised Vec had. Only the requested span is READ: `read_block_bytes` runs
                // `wanted` times, not `length` times, which is what stopped a ten-entry block of a
                // four-thousand-entry list from touching four thousand blocks.
                //
                // `skip(from)` still ADVANCES the iterator `from` times, so reaching a late offset
                // walks the nodes before it -- cheap per step and allocating nothing, but not
                // free. A read near the head is O(wanted); one near the tail is O(from + wanted).
                // An allocation probe cannot see that difference, so it is stated here rather
                // than left for a flat byte measurement to imply otherwise.
                let wanted = (to - from + 1) as usize;
                // One routing bucket for the whole range, and one page-key builder: the entries of
                // a list sit in few slabs, so their keys share a record key rather than each
                // spelling it again, and the element name is rendered in place rather than in a
                // `String` per entry.
                let routing_bucket =
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                let mut page_keys = crate::engine::PageKeys::new();
                list.map(|list| {
                    list.iter()
                        .skip(from as usize)
                        .take(wanted)
                        .filter_map(|(seq, address)| {
                            // The same rendering ListPop uses, which is what the index rows
                            // for this list were filed under.
                            let mut name = [0_u8; 16];
                            let element_name = list_element_name(*seq, &mut name);
                            let cache_key = page_keys.key(shard_id, address, Some(routing_bucket));
                            read_block_value_keyed(
                                cache,
                                block_store,
                                shard_id,
                                address,
                                // Re-derived from the sequence the same way ListPop renders it. `.values()` used to
                                // drop that sequence, which is why the iteration above is now `.iter()`.
                                BlockIdentity::of(shard_id, "list", &key, Some(element_name)),
                                cache_key,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
            };
            CommandResponse::Members { members }
        }
        Command::ListLen { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.bucket_index.lists.get(&key).map_or(0, BTreeMap::len) as i64,
            }
        }
        Command::SetMembers { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::set_members(shard_id, &key), || {
                // THE DURABLE MAP ANSWERS FIRST, AND IT READS NO BLOCKS AT ALL.
                //
                // `shard.sets` is keyed BY MEMBER BYTES, so it holds the member identity the
                // bucket index cannot -- which is the whole reason the fold below had to ask
                // the payload. A listing needs only those keys; the element address each one
                // maps to is never consulted, so a stale address after a fold cannot reach
                // this answer.
                //
                // WHAT IT REMOVES. The fold reads one block per block the object has,
                // tombstones included: 1,033 block reads and 71.4 ms to answer 64 live
                // members once 1,024 have been removed, against 614 us unchurned. From the
                // map it is zero block reads and O(live) whatever the churn -- and a churned
                // set is the steady state rather than an edge case.
                //
                // WHY THIS MAY BE PREFERRED, as what is driven rather than what is argued.
                // Three places in this tree already said the map answers correctly, and
                // three claims are not a measurement -- so
                // `does_the_durable_map_answer_what_the_fold_answers` drives BOTH mechanisms
                // on one store across the five shapes where they could part company,
                // including the precedence case (live, tombstoned, live again) where the fold
                // resolves by `append_position` and the map by the key being present. The
                // gate is what makes that comparison possible at all.
                //
                // AND THE FOLD IS STILL REACHED, for a key the map holds no entry for. That
                // matters twice: `derive_membership` does not become a zero-caller function,
                // and a listing can never answer EMPTY for a key whose blocks say otherwise
                // -- which is exactly the `listed=0` against 40 that killed the entries walk.
                if shard.set_listing_from_map {
                    if let Some(members) = shard.bucket_index.sets.get(&key) {
                        SET_LISTING_FROM_MAP.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        return CommandResponse::Members {
                            members: members.iter().map(|(member, _)| member.clone()).collect(),
                        };
                    }
                }
                // IDENTITY IS IN THE PAYLOAD, AND THERE IS NO LONGER ANOTHER WAY.
                //
                // This was the gated arm of an `if`, and the ungated arm walked the entries and
                // answered each from the block it named. That walk's own `None` arm said why it
                // cannot work once an entry names no element -- "an entry naming no element name can
                // only be answered by a block that names no element either" -- so with every entry
                // nameless it dropped every member and the listing returned EMPTY, measured as
                // `listed=0` against 40. It is deleted rather than left unreachable: an entry
                // cannot name an element any more, so there is no state in which it would be right.
                //
                // Asking the PAYLOAD is the direction that resurrected a removed member once, and
                // the two things that make it safe are both driven: the tombstone block is REACHABLE
                // from the index, and `derive_membership` folds every block of the object by
                // `append_position` with a value and a tombstone as the same kind of statement, so
                // the later block wins. A hand-rolled loop over the payload lacks exactly that
                // precedence, which is why one resurrected a member before.
                //
                // THE ENUMERATOR IS THE ONE THAT KEEPS TOMBSTONE ENTRIES, because withholding them
                // would hand the fold only the blocks that say "present".
                let routing_bucket =
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                let addresses =
                    super::bucket_store::bucket_index_all_block_addresses_with_tombstones(
                        shard, "set", &key,
                    );
                let derived = crate::engine::container_membership::derive_membership(
                    "set",
                    addresses,
                    |address| {
                        read_block_bytes(
                            cache,
                            block_store,
                            shard_id,
                            address,
                            BlockIdentity::of(shard_id, "set", &key, None),
                            Some(routing_bucket),
                        )
                    },
                );
                if derived.is_complete() {
                    GATED_LISTING_DERIVED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    return CommandResponse::Members {
                        members: derived.live.into_values().collect(),
                    };
                }
                // AN INCOMPLETE DERIVATION MUST NOT BE SERVED, and the reason is not
                // squeamishness: a block that could not be read may have been the one carrying
                // a tombstone, so an incomplete fold can be OVER-complete as easily as under,
                // and there is no direction to fail safely in. The durable member map is the
                // authority -- `reconcile_from_durable` treats it as such, and a gated removal
                // was already shown to survive a reload in it -- so the answer comes from
                // there and the decline is counted rather than hidden.
                GATED_LISTING_DECLINED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                CommandResponse::Members {
                    members: shard
                        .bucket_index
                        .sets
                        .get(&key)
                        .map(|members| members.iter().map(|(member, _)| member.clone()).collect())
                        .unwrap_or_default(),
                }
            })
        }
        Command::SetScan { key, after, limit } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            let members = shard
                .bucket_index
                .sets
                .get(&key)
                .map(|members| {
                    if after.is_empty() {
                        members.keys().take(limit).cloned().collect::<Vec<_>>()
                    } else {
                        use std::ops::Bound::{Excluded, Unbounded};
                        members
                            .range::<[u8], _>((Excluded(after.as_slice()), Unbounded))
                            .take(limit)
                            .map(|(member, _)| member.clone())
                            .collect::<Vec<_>>()
                    }
                })
                .unwrap_or_default();
            CommandResponse::Members { members }
        }
        Command::SetRemove { key, member } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let member_element_name = set_element_name(&member);
            // REMOVING THE LAST MEMBER REMOVES THE KEY, as it does for the other three container
            // kinds. `record_exists_exact` ORs `shard.bucket_index.sets.contains_key(key)` in beside its
            // bucket-index answer, so an empty member map left under a live key is a key that
            // EXISTS answers 1 for while every listing answers empty for -- and `CommonExpire`
            // gates on the same function, so it accepted a deadline for it and `ttl_ms` then
            // reported that deadline instead of the -2 of a missing key.
            //
            // THAT CLEANUP NOW LIVES INSIDE `RecordedSetContainer::remove_member`, so no arm can
            // forget it. `HashDelete` has done it since it was written and `ZSetRemove` and
            // `ListPop` do it in this shape; the note at `HashDelete` said in a parenthesis that
            // sets did not need it, and they do, for the reader in the very next line of the same
            // expression.
            mutated |= super::recorded_map::remove_element::<super::recorded_map::SetKind>(
                cache,
                block_store,
                shard,
                shard_id,
                &key,
                &member_element_name,
                member.clone(),
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
            CommandResponse::Empty
        }
        append_command @ (Command::FeatureAppend { .. }
        | Command::FeatureAppendWithTtl { .. }) => {
            let (key, points, ttl_ms) = match append_command {
                Command::FeatureAppend { key, points } => (key, points, None),
                Command::FeatureAppendWithTtl { key, points, ttl_ms } => {
                    (key, points, Some(ttl_ms))
                }
                _ => unreachable!(),
            };
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // HOISTED ABOVE THE SERIES BORROW. `features_mut()` borrows the whole
            // `ShardState` where `&mut shard.features` borrowed one field, so this read of
            // `shard.bucket_index` can no longer sit inside the append call below -- the
            // two borrows are no longer disjoint. Taken here, before the series is
            // borrowed, which is also the order this block reads best in.
            let next_block_index = crate::engine::state::next_block_index_for_object(
                &shard.bucket_index,
                routing_bucket,
                "feature",
                &key,
            );
            let series = shard
                .features_mut()
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            let points = sorted_feature_points(points);
            let mut published: Vec<ElementEntry> = Vec::new();
            let mut replaced_any = false;
            // feature_append_chunks_and_persists_timestamped_kv_blocks: append each
            // timestamped feature point through the block-backed KV layout, then
            // publish the resulting block addresses into the bucket index below.
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_bucket,
                async_storage,
                true,
                next_block_index,
            ) {
                for (timestamp_ms, address) in addresses {
                    // A replaced timestamp supersedes a block, and a superseded block must be
                    // dropped rather than joined -- so record it and take the restating path.
                    // `set` returns nothing where `insert` returned the superseded value,
                    // so the question has to be asked BEFORE the write rather than read off it.
                    if series.get(timestamp_ms).is_some() {
                        replaced_any = true;
                    }
                    series.set(timestamp_ms, address.clone());
                    published.push(address);
                    mutated = true;
                }
            }
            let trimmed = trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
                trim_boundary_record,
            );
            mutated |= trimmed;
            if replaced_any || trimmed {
                // Something was superseded or evicted: the live set has to be restated in full.
                let live_addresses = series.addresses().cloned().collect::<Vec<_>>();
                sync_bucket_index_object_blocks(
                    shard,
                    shard_id,
                    "feature",
                    &key,
                    live_addresses,
                    mutated,
                );
            } else {
                // A pure append. Publishing only the new blocks leaves the index in the same
                // state, and stops an append costing the length of the series it joins:
                // restating every address measured 4.07 MB for one point on a 3,200-point series.
                sync_bucket_index_object_blocks_with_mode(
                    shard, shard_id, "feature", &key, published, mutated, false,
                );
            }
            if let Some(ttl_ms) = ttl_ms {
                if shard.features().get(&key[..]).is_some_and(|series| series.len() > 0) {
                    let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                    crate::engine::set_expiry(shard, key.clone(), expires_at);
                    stage_meta_outcome(
                        shard_id,
                        "object",
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        None,
                        Some(expires_at),
                        false,
                    );
                    mutated = true;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // MERGE-REDUCE: the request's cells are merged into what is stored BEFORE the series
            // is borrowed for writing, and what is appended below is the merged snapshot at each
            // timestamp -- an ordinary upsert of it. See `cell_merge_append` for where a violation
            // is refused (validation) and why this plan cannot fail here for a validated command.
            let mut merge_receipt = None;
            let points = if policy == FeatureWritePolicy::MergeReduce {
                match crate::engine::cell_merge_append::plan_cell_merge(
                    cache,
                    block_store,
                    shard_id,
                    shard.features().get(&key[..]),
                    &key,
                    routing_bucket,
                    &points,
                    crate::engine::cell_merge_append::PlanOutput::Values,
                ) {
                    Ok(plan) => {
                        merge_receipt = Some((
                            plan.points.len() as u64,
                            plan.cells_created,
                            plan.cells_merged,
                        ));
                        plan.points
                    }
                    Err(_) => {
                        return ExecuteOutcome {
                            response: CommandResponse::CellMergeReceipt {
                                snapshots_written: 0,
                                cells_created: 0,
                                cells_merged: 0,
                            },
                            mutated,
                        };
                    }
                }
            } else {
                points
            };
            // HOISTED ABOVE THE SERIES BORROW. `features_mut()` borrows the whole
            // `ShardState` where `&mut shard.features` borrowed one field, so this read of
            // `shard.bucket_index` can no longer sit inside the append call below -- the
            // two borrows are no longer disjoint. Taken here, before the series is
            // borrowed, which is also the order this block reads best in.
            let next_block_index = crate::engine::state::next_block_index_for_object(
                &shard.bucket_index,
                routing_bucket,
                "feature",
                &key,
            );
            let series = shard
                .features_mut()
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            let mut accepted_points = Vec::new();
            let mut accepted_timestamps = BTreeSet::new();
            let mut published: Vec<ElementEntry> = Vec::new();
            let mut replaced_any = false;
            // Process points in REQUEST order, NOT pre-collapsed by timestamp. feature ADD
            // FIRST policy walks the point list in order and
            // skips any ts already present, so for an in-batch duplicate timestamp the FIRST
            // value wins. Pre-collapsing here via sorted_feature_points (last-wins) would silently
            // keep the LAST duplicate under InsertIfAbsent. accepted_points is sorted+collapsed
            // just before the append below (UPSERT/ReplaceExisting keep last-wins, matching).
            for point in points {
                let exists = series.get(point.timestamp_ms).is_some()
                    || accepted_timestamps.contains(&point.timestamp_ms);
                let should_write = match policy {
                    FeatureWritePolicy::Upsert => true,
                    FeatureWritePolicy::InsertIfAbsent => !exists,
                    FeatureWritePolicy::ReplaceExisting => exists,
                    FeatureWritePolicy::Block => false,
                    // The point is already the merged snapshot: an upsert of it.
                    FeatureWritePolicy::MergeReduce => true,
                };
                if should_write {
                    accepted_timestamps.insert(point.timestamp_ms);
                    accepted_points.push(point);
                }
            }
            if !accepted_points.is_empty() {
                if let Ok(addresses) = append_timestamped_kv_blocks(
                    cache,
                    block_store,
                    shard_id,
                    "feature",
                    &key,
                    sorted_feature_points(accepted_points),
                    routing_bucket,
                    async_storage,
                    true,
                    next_block_index,
                ) {
                    for (timestamp_ms, address) in addresses {
                        if series.get(timestamp_ms).is_some() {
                            replaced_any = true;
                        }
                        series.set(timestamp_ms, address.clone());
                        published.push(address);
                        mutated = true;
                    }
                }
            }
            let trimmed = trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
                trim_boundary_record,
            );
            mutated |= trimmed;
            if replaced_any || trimmed {
                let live_addresses = series.addresses().cloned().collect::<Vec<_>>();
                sync_bucket_index_object_blocks(
                    shard,
                    shard_id,
                    "feature",
                    &key,
                    live_addresses,
                    mutated,
                );
            } else {
                sync_bucket_index_object_blocks_with_mode(
                    shard, shard_id, "feature", &key, published, mutated, false,
                );
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            match merge_receipt {
                // A plan that was never appended reports nothing landed.
                Some((snapshots_written, cells_created, cells_merged)) if mutated => {
                    CommandResponse::CellMergeReceipt {
                        snapshots_written,
                        cells_created,
                        cells_merged,
                    }
                }
                Some(_) => CommandResponse::CellMergeReceipt {
                    snapshots_written: 0,
                    cells_created: 0,
                    cells_merged: 0,
                },
                None => CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                },
            }
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            // Apply lazy expiry before serving, matching FeatureAggQuery/SequenceQuery and
            // every sibling read: a key past its deadline but not yet swept must read empty,
            // otherwise FeatureQuery and FeatureAggQuery disagree on the same expired key.
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            cached_response(
                cache,
                CacheKey::feature_query(shard_id, &key, start_ms, end_ms, count),
                || {
                    let identity = BlockIdentity::of(shard_id, "feature", &key, None);
                    let routing_bucket = Some(block_routing_bucket(
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                    ));
                    let points = shard
                        .features()
                        .get(&key[..])
                        .map(|series| {
                            let mut block_cache = HashMap::new();
                            // feature_append_keeps_oversized_single_timestamped_value_readable:
                            // range queries rehydrate each timestamp through the packed
                            // block reader, so a large single timestamped value remains
                            // readable when it occupies its own block.
                            series
                                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                                // Default read bound follows feature_max_size so raising the
                                // retention cap (long-sequence use) also lifts the read limit.
                                .take(count.unwrap_or(feature_max_size))
                                .filter_map(|(timestamp_ms, address)| {
                                    read_feature_point_cached(
                                        cache,
                                        block_store,
                                        shard_id,
                                        timestamp_ms,
                                        address,
                                        identity,
                                        &mut block_cache,
                                        routing_bucket,
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    CommandResponse::FeaturePoints { points }
                },
            )
        }
        Command::FeatureQueryFiltered {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            // Apply lazy expiry before serving, matching FeatureAggQuery/SequenceQuery and
            // every sibling read: an expired-but-unswept key must read empty so filtered and
            // aggregate reads stay consistent with each other on the same key.
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let limit = count.unwrap_or(feature_max_size).min(feature_max_size);
            let identity = BlockIdentity::of(shard_id, "feature", &key, None);
            let routing_bucket = Some(block_routing_bucket(
                &key,
                start_routing_bucket,
                end_routing_bucket,
            ));
            let points = shard
                .features()
                .get(&key[..])
                .map(|series| {
                    let mut block_cache = HashMap::new();
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                        .take(limit)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point_cached(
                                cache,
                                block_store,
                                shard_id,
                                timestamp_ms,
                                address,
                                identity,
                                &mut block_cache,
                                routing_bucket,
                            )
                            .and_then(|point| {
                                let row = crate::engine::decode_or_count_sequence_row(
                                    point.timestamp_ms,
                                    &point.value,
                                )?;
                                filters
                                    .iter()
                                    .all(|filter| sequence_filter_matches(&row, filter))
                                    .then_some(point)
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::FeatureReplace {
            key,
            start_ms,
            end_ms,
            points,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            mutated = replace_feature_range(
                cache,
                block_store,
                shard_id,
                shard,
                &key,
                start_ms,
                end_ms,
                points,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
                feature_max_size,
                trim_boundary_record,
                mutated,
            );
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureDelete { key } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // A removal with no element name: the whole series went, not one point.
            stage_meta_outcome(
                shard_id,
                "feature",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            // THE BLOCKS ARE MARKED DELETED BEFORE THE SERIES IS REMOVED, not after, and `StringDelete`
            // above has the same two lines in this order for the same reason.
            //
            // `mark_bucket_index_object_deleted` finds an object's blocks by NAMING each one through
            // the object's row and the block lookup, and keeps any block it cannot name. Removing the
            // series first took the row the names come from, so the marker named nothing, kept every
            // block, and left them LIVE in `bucket_map` with no object naming them: the index writer
            // then refuses the shard ("holds a block whose stored key would name no object"), and
            // that is an error in a release build too, not an assertion. Every
            // `FeatureDelete` in the conformance sequences that leave the index unwritable is this
            // one; no other command is.
            mutated |= mark_bucket_index_object_deleted(shard, &key);
            mutated |= shard.features_mut().remove(&key[..]).is_some();
            // THE ONE FEATURE WRITE THAT DOES NOT PUBLISH BLOCKS, so the invalidation in
            // `sync_bucket_index_object_blocks_with_mode` does not cover it. The expiry path's
            // `forget_object` already drops the view beside its own `features_mut().remove`; this
            // arm removed the series and left the view behind. The length test hid it in the
            // common case -- an emptied series reads as length 0 against a non-empty view -- but
            // not when the key is re-created with the same number of points, which then answers
            // with the sums of the DELETED series.
            control_rollup::feature_forget(shard, &key);
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        aggregate_command @ (Command::FeatureAggQuery { .. }
        | Command::FeatureProtoAggQuery { .. }) => {
            let proto_values = matches!(aggregate_command, Command::FeatureProtoAggQuery { .. });
            let (key, start_ms, end_ms, aggregator, count) = match aggregate_command {
                Command::FeatureAggQuery {
                    key,
                    start_ms,
                    end_ms,
                    aggregator,
                    count,
                }
                | Command::FeatureProtoAggQuery {
                    key,
                    start_ms,
                    end_ms,
                    aggregator,
                    count,
                } => (key, start_ms, end_ms, aggregator, count),
                _ => unreachable!(),
            };
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Aggregate { value: 0 },
                    mutated,
                };
            }
            // The additive "sum" aggregate over the full window can be served in O(levels) via
            // the shared rollup ladder. Only "sum" is summable here: for features "count"/"" mean
            // element count (aggregate_feature_values), and min/max/first/last are non-additive, so
            // those stay on the raw path. Gated to no explicit `count` cap so it covers the whole
            // window, matching the raw scan on the retained series.
            let use_rollup = control_rollup_enabled
                && !proto_values
                && aggregator.trim().eq_ignore_ascii_case("sum")
                && count.is_none();
            if use_rollup {
                // Lazily materialize the numeric view of the feature series. Each point's value
                // is parsed through the SAME aggregate_feature_values path, so the folded values
                // are bit-identical to the raw scan; rebuild when the series length changed.
                let feature_len = shard
                    .features()
                    .get(&key[..])
                    .map(|series| series.len())
                    .unwrap_or(0);
                let stale = shard
                    .feature_values()
                    .get(&key[..])
                    .map(|values| values.len() != feature_len)
                    .unwrap_or(feature_len > 0);
                if stale {
                    // Each value is read IN PLACE and parsed where it sits: a point's number is
                    // `parse_i64` of its bytes, or 0 when it is not a number, which is what summing
                    // the one-element slice came to. No `FeaturePoint` or value copy is built.
                    let mut page_keys = crate::engine::PageKeys::new();
                    let identity = BlockIdentity::of(shard_id, "feature", &key, None);
                    let routing_bucket = Some(block_routing_bucket(
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                    ));
                    let decoded: BTreeMap<u64, i64> = shard
                        .features()
                        .get(&key[..])
                        .map(|series| {
                            series
                                .points()
                                .filter_map(|(timestamp_ms, address)| {
                                    read_feature_value_with(
                                        cache,
                                        block_store,
                                        shard_id,
                                        timestamp_ms,
                                        address,
                                        identity,
                                        routing_bucket,
                                        &mut page_keys,
                                        |value| parse_i64_slice(value).unwrap_or(0),
                                    )
                                    .map(|number| (timestamp_ms, number))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    shard
                        .feature_values_mut()
                        // `Arc<str>`: this map is `ModelKey`-keyed now and joins the key
                        // adoption, so it stops minting its own allocation per key.
                        .insert(std::sync::Arc::from(key.as_str()), decoded);
                    shard.feature_rollups_mut().remove(&key);
                }
                CommandResponse::Aggregate {
                    value: control_rollup::feature_windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let mut page_keys = crate::engine::PageKeys::new();
                let count_only = matches!(
                    crate::engine::window_aggregate::AggName::parse(&aggregator),
                    None | Some(
                        crate::engine::window_aggregate::AggName::Count
                            | crate::engine::window_aggregate::AggName::Events
                            | crate::engine::window_aggregate::AggName::Len
                    )
                );
                let identity = BlockIdentity::of(shard_id, "feature", &key, None);
                let routing_bucket = Some(block_routing_bucket(
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                ));
                let value = shard
                    .features()
                    .get(&key[..])
                    .map(|series| {
                        let mut read_number = |(timestamp_ms, address): (u64, &ElementEntry)| {
                            read_feature_value_with(
                                cache,
                                block_store,
                                shard_id,
                                timestamp_ms,
                                address,
                                identity,
                                routing_bucket,
                                &mut page_keys,
                                |value| {
                                    if count_only {
                                        None
                                    } else if proto_values {
                                        parse_feature_proto_number(value)
                                    } else {
                                        parse_i64_slice(value)
                                    }
                                },
                            )
                        };
                        let limit = count.unwrap_or(feature_max_size);
                        if matches!(
                            crate::engine::window_aggregate::AggName::parse(&aggregator),
                            Some(crate::engine::window_aggregate::AggName::Last)
                        ) {
                            // A count cap selects the earliest points before unreadable blocks
                            // are filtered. Find that prefix's endpoint without reading values.
                            let endpoint = if series.len() <= limit {
                                Some(end_ms)
                            } else {
                                series
                                    .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                                    .take(limit)
                                    .last()
                                    .map(|(timestamp, _)| timestamp)
                            };
                            return endpoint
                                .and_then(|end| {
                                    series
                                        .range(crate::engine::timestamp_range_bounds(start_ms, end))
                                        .rev()
                                        .find_map(&mut read_number)
                                })
                                .flatten()
                                .unwrap_or_default();
                        }
                        crate::engine::window_aggregate::feature_stream(
                            &aggregator,
                            series
                                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                                .take(limit)
                                .filter_map(read_number),
                        )
                    })
                    .unwrap_or_default();
                CommandResponse::Aggregate { value }
            }
        }
        Command::SequenceAdd { key, rows } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // HOISTED ABOVE THE SERIES BORROW. `features_mut()` borrows the whole
            // `ShardState` where `&mut shard.features` borrowed one field, so this read of
            // `shard.bucket_index` can no longer sit inside the append call below -- the
            // two borrows are no longer disjoint. Taken here, before the series is
            // borrowed, which is also the order this block reads best in.
            let next_block_index = crate::engine::state::next_block_index_for_object(
                &shard.bucket_index,
                routing_bucket,
                "feature",
                &key,
            );
            let series = shard
                .features_mut()
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            // PROTOBUF ONLY: the same wire form `FeatureAppend` rows hold, so both read paths decode
            // it. There is no JSON encode path here; see `sequence_row_value`.
            let points = rows
                .into_iter()
                .map(|row| FeaturePoint {
                    timestamp_ms: row.timestamp_ms,
                    value: row.encode_feature_proto_value(),
                })
                .collect::<Vec<_>>();
            let points = sorted_feature_points(points);
            let mut published: Vec<ElementEntry> = Vec::new();
            let mut replaced_any = false;
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_bucket,
                async_storage,
                true,
                next_block_index,
            ) {
                for (timestamp_ms, address) in addresses {
                    // A replaced timestamp supersedes a block, and a superseded block must be
                    // dropped rather than joined -- so record it and take the restating path.
                    // `set` returns nothing where `insert` returned the superseded value, so
                    // the question has to be asked BEFORE the write rather than read off it.
                    if series.get(timestamp_ms).is_some() {
                        replaced_any = true;
                    }
                    series.set(timestamp_ms, address.clone());
                    published.push(address);
                    mutated = true;
                }
            }
            let trimmed = trim_timestamped_series(
                shard_id,
                "feature",
                &key,
                routing_bucket,
                series,
                feature_max_size,
                trim_boundary_record,
            );
            mutated |= trimmed;
            if replaced_any || trimmed {
                // Something was superseded or evicted: the live set has to be restated in full.
                let live_addresses = series.addresses().cloned().collect::<Vec<_>>();
                sync_bucket_index_object_blocks(
                    shard,
                    shard_id,
                    "feature",
                    &key,
                    live_addresses,
                    mutated,
                );
            } else {
                // A pure append. Publishing only the new blocks leaves the index in the same
                // state, and stops a row append costing the length of the series it joins.
                sync_bucket_index_object_blocks_with_mode(
                    shard, shard_id, "feature", &key, published, mutated, false,
                );
            }
            CommandResponse::Empty
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::SequenceRows { rows: Vec::new() },
                    mutated,
                };
            }
            let identity = BlockIdentity::of(shard_id, "feature", &key, None);
            let routing_bucket = Some(block_routing_bucket(
                &key,
                start_routing_bucket,
                end_routing_bucket,
            ));
            let mut packed_block_cache = HashMap::new();
            let rows = shard
                .features()
                .get(&key[..])
                .map(|series| {
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_sequence_row_cached(
                                cache,
                                block_store,
                                shard_id,
                                timestamp_ms,
                                address,
                                identity,
                                routing_bucket,
                                &mut packed_block_cache,
                            )
                        })
                        .filter(|row| {
                            filters
                                .iter()
                                .all(|filter| sequence_filter_matches(row, filter))
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::SequenceRows { rows }
        }
        Command::SequenceBatchQuery { queries } => {
            let groups = queries
                .into_iter()
                .map(
                    |SequenceQuerySpec {
                         key,
                         start_ms,
                         end_ms,
                         count,
                         filters,
                     }| {
                        if drop_if_expired(cache, shard_id, shard, &key) {
                            mutated = true;
                            return (key, Vec::new());
                        }
                        let rows = sequence_rows_in_range(
                            cache,
                            block_store,
                            shard_id,
                            shard,
                            &key,
                            start_ms,
                            end_ms,
                            count,
                            &filters,
                        );
                        (key, rows)
                    },
                )
                .collect();
            CommandResponse::SequenceRowGroups { groups }
        }
        Command::CellRankQuery { key, spec } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::CellRanking {
                        ranked: crate::types::RankedCells::default(),
                    },
                    mutated,
                };
            }
            CommandResponse::CellRanking {
                ranked: cell_rank_read::serve(
                    cache,
                    block_store,
                    shard_id,
                    shard,
                    &key,
                    &spec,
                    start_routing_bucket,
                    end_routing_bucket,
                    resolve_now_ms(),
                ),
            }
        }
        Command::CellRollup { key, spec } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let routing_bucket = block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // THE SAME PLANNER AS ADMISSION, over the same series, so a follower and a replay
            // reach the leader's bytes. A plan that fails here passed validation on the leader:
            // that is a divergence, not a bad request, so it writes nothing and receipts zeros.
            let plan = crate::engine::cell_rollup_apply::plan_cell_rollup(
                cache,
                block_store,
                shard_id,
                shard.features().get(&key[..]),
                &key,
                routing_bucket,
                &spec,
            )
            .ok();
            let receipt = match plan {
                Some(plan) => {
                    // The stored snapshots below the cutoff are exactly `plan.replaced`; the
                    // range from the first to the last of them holds nothing else.
                    if let (true, Some(&first), Some(&last)) =
                        (plan.changed, plan.replaced.first(), plan.replaced.last())
                    {
                        mutated = replace_feature_range(
                            cache,
                            block_store,
                            shard_id,
                            shard,
                            &key,
                            first,
                            last,
                            plan.points,
                            start_routing_bucket,
                            end_routing_bucket,
                            async_storage,
                            feature_max_size,
                            trim_boundary_record,
                            mutated,
                        );
                        // The numeric view of the series is rebuilt only when its length moves; a
                        // rollup can change the values and keep the length.
                        control_rollup::feature_forget(shard, &key);
                        let _ = cache.invalidate_record(shard_id, "feature", &key);
                    }
                    CommandResponse::CellRollupReceipt {
                        snapshots_before: plan.snapshots_before,
                        snapshots_after: plan.snapshots_after,
                        cells_before: plan.cells_before,
                        cells_after: plan.cells_after,
                        cells_dropped: plan.cells_dropped,
                    }
                }
                None => CommandResponse::CellRollupReceipt {
                    snapshots_before: 0,
                    snapshots_after: 0,
                    cells_before: 0,
                    cells_after: 0,
                    cells_dropped: 0,
                },
            };
            receipt
        }
        Command::ControlStateIncrement {
            key,
            timestamp_ms,
            amount,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let resulting = {
                let counter = shard
                    .control_state_mut()
                    .entry(std::sync::Arc::from(&key[..]))
                    .or_default()
                    .entry(timestamp_ms)
                    .or_default();
                *counter += amount;
                *counter
            };
            // The RESULT, not the increment. An increment replayed twice counts twice; a count
            // installed twice is the same count, which is what makes a record idempotent.
            record_counter_result(
                cache,
                block_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
                timestamp_ms,
                resulting,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateIncrementWithOptions {
            key,
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            let resulting = {
                let counter = shard
                    .control_state_mut()
                    .entry(std::sync::Arc::from(&key[..]))
                    .or_default()
                    .entry(bucket_ms)
                    .or_default();
                *counter += amount;
                *counter
            };
            record_counter_result(
                cache,
                block_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
                bucket_ms,
                resulting,
            );
            if let Some(ttl_ms) = ttl_ms {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                // Through `set_expiry`, never straight into `expires_at_ms`. The sweep reads the
                // deadline-ordered mirror and nothing else, and `ensure_expiry_order` rebuilds
                // that mirror only when it is ENTIRELY empty -- so a deadline written past the
                // helper is invisible to every sweep for the life of the shard the moment any
                // other key holds one, and the key is retained forever.
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                // And recorded, already resolved. This arm writes a control-state block, the block
                // write stages an outcome, and a record carrying outcomes is INSTALLED rather
                // than re-executed on replay -- so a deadline that is not in the record is not in
                // the shard after recovery. Measured before this existed: the block came back and
                // the deadline came back as None.
                //
                // AFTER the block, not before. Replay installs a deadline only onto a key that
                // already exists, and outcomes install in the order they were staged; staged
                // first, this deadline meets a shard that has not seen the key yet and is
                // dropped in silence. Both correct siblings here stage their element name first.
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, bucket_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateChangeAdd {
            key,
            timestamp_ms,
            value,
            precision_ms,
            ttl_ms,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            stage_element_name_outcome(
                shard_id,
                "control_change",
                &key,
                Some(bucket_ms.to_string()),
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket),
                None,
                Some(value.clone()),
            );
            hll::record_change(shard, &key, bucket_ms, value);
            if let Some(ttl_ms) = ttl_ms {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateCount {
            key,
            start_ms,
            end_ms,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = if control_rollup_enabled {
                control_rollup::windowed_sum(shard, &key, start_ms, end_ms)
            } else {
                shard
                    .control_state()
                    .get(&key[..])
                    .map(|series| {
                        crate::engine::window_aggregate::sum_legacy(
                            series
                                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                                .map(|(_, value)| *value),
                        )
                    })
                    .unwrap_or_default()
            };
            CommandResponse::Integer { value }
        }
        Command::ControlStateQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if is_control_state_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_control_state_changes(shard, &key, start_ms, end_ms),
                }
            } else if control_rollup_enabled && control_rollup::is_sum_family(&aggregator) {
                CommandResponse::Integer {
                    value: control_rollup::windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .control_state()
                    .get(&key[..])
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_control_state_values(&values, &aggregator),
                }
            }
        }
        Command::ControlStateDetail {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .control_state()
                .get(&key[..])
                .map(|series| {
                    series
                        .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                        .take(count.unwrap_or(usize::MAX))
                        .map(|(timestamp_ms, amount)| FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value: amount.to_string().into_bytes(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::ControlStateSet {
            family,
            key,
            timestamp_ms,
            amount,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let key = control_state_family_key(family, &key);
            let resulting = {
                let counter = shard
                    .control_state_mut()
                    .entry(std::sync::Arc::from(&key[..]))
                    .or_default()
                    .entry(timestamp_ms)
                    .or_default();
                *counter += amount;
                *counter
            };
            record_counter_result(
                cache,
                block_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
                timestamp_ms,
                resulting,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::ControlStateSetAndGet {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let key = control_state_family_key(family, &key);
            let series = shard
                .control_state_mut()
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            let resulting = {
                let counter = series.entry(timestamp_ms).or_default();
                *counter += amount;
                *counter
            };
            let values = series
                .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            record_counter_result(
                cache,
                block_store,
                shard_id,
                shard,
                &key,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
                timestamp_ms,
                resulting,
            );
            if control_rollup_enabled {
                control_rollup::record_increment(shard, &key, timestamp_ms, amount);
            }
            mutated = true;
            CommandResponse::Integer {
                value: aggregate_control_state_values(&values, &aggregator),
            }
        }
        Command::ControlStateSetAndGetWithOptions {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
            precision_ms,
            ttl_ms,
            uuid,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let key = control_state_family_key(family, &key);
            // UUID idempotency: dedup at-least-once replays within a bounded window,
            // mirroring the control_state dedup ledger. A duplicate is a no-op
            // write that still returns the current windowed aggregate (idempotent).
            let now = resolve_now_ms();
            let is_duplicate = if let Some(uuid) = uuid.as_ref().filter(|u| !u.is_empty()) {
                let dedup_key = format!("{key}\u{1}{uuid}");
                gc_control_state_uuid(shard, now);
                let dup = matches!(shard.control_state_uuid().get(&dedup_key[..]), Some(expiry) if *expiry > now);
                if !dup {
                    shard
                        .control_state_uuid_mut()
                        // `Arc<str>`, because the ledger is `ModelKey`-keyed now and joins the
                        // key adoption. One allocation here, at the only site that inserts.
                        .insert(
                            std::sync::Arc::from(dedup_key.as_str()),
                            now.saturating_add(CONTROL_STATE_UUID_DEDUP_MS),
                        );
                }
                dup
            } else {
                false
            };
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            let series = shard
                .control_state_mut()
                .entry(std::sync::Arc::from(&key[..]))
                .or_default();
            // A duplicate adds nothing and so has no result to state; it keeps writing the block,
            // which is what makes the record one a replay installs.
            let resulting = if is_duplicate {
                None
            } else {
                let counter = series.entry(bucket_ms).or_default();
                *counter += amount;
                Some(*counter)
            };
            let value = if is_control_state_change_aggregator(&aggregator) {
                count_control_state_changes(shard, &key, start_ms, end_ms)
            } else {
                let values = shard
                    .control_state()
                    .get(&key[..])
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                aggregate_control_state_values(&values, &aggregator)
            };
            match resulting {
                Some(resulting) => {
                    record_counter_result(
                        cache,
                        block_store,
                        shard_id,
                        shard,
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                        bucket_ms,
                        resulting,
                    );
                }
                None => {
                    persist_control_state_block(
                        cache,
                        block_store,
                        shard_id,
                        shard,
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                }
            }
            if let Some(ttl_ms) = ttl_ms {
                // `key` is the family-prefixed key by this point, which is the key both the
                // record and the sweep have to name. Same two requirements as every other
                // deadline in this file: through `set_expiry` so the deadline-ordered mirror the
                // sweep reads learns about it, and staged -- after the block -- so it survives a
                // replay that installs this record's block outcome instead of re-running the
                // command.
                let expires_at = now.saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            if control_rollup_enabled && !is_duplicate {
                control_rollup::record_increment(shard, &key, bucket_ms, amount);
            }
            mutated = true;
            CommandResponse::Integer { value }
        }
        Command::ControlStateFamilyQuery {
            family,
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let key = control_state_family_key(family, &key);
            if is_control_state_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_control_state_changes(shard, &key, start_ms, end_ms),
                }
            } else if control_rollup_enabled && control_rollup::is_sum_family(&aggregator) {
                CommandResponse::Integer {
                    value: control_rollup::windowed_sum(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .control_state()
                    .get(&key[..])
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_control_state_values(&values, &aggregator),
                }
            }
        }
        Command::ControlStateSelectionSet {
            key,
            value,
            occur_time_ms,
            ttl_ms,
            selection_type,
        } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // FirstOrLastSet substitutes occur_time==0 with the current time BEFORE the
            // FIRST/LAST comparison (an occur_time of 0 is replaced with the current time).
            // occur_time defaults to 0 on the proto, so a caller that omits it must compare as
            // "now" -- taking 0 literally made an omitted-time FIRST set always win (0 < any)
            // and an omitted-time LAST set always lose.
            let occur_time_ms = if occur_time_ms == 0 {
                resolve_now_ms()
            } else {
                occur_time_ms
            };
            let should_store = shard
                .control_state_selection
                .get(&key[..])
                .map(|existing| match selection_type {
                    ControlStateSelectionType::First => occur_time_ms < existing.occur_time_ms,
                    ControlStateSelectionType::Last => occur_time_ms > existing.occur_time_ms,
                })
                .unwrap_or(true);
            if should_store {
                // The value that WON the comparison, so a replay installs the winner instead of
                // re-running a comparison against whatever it happens to hold.
                stage_element_name_outcome(
                    shard_id,
                    "control_selection",
                    &key,
                    Some(match selection_type {
                        ControlStateSelectionType::First => "first".to_string(),
                        ControlStateSelectionType::Last => "last".to_string(),
                    }),
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket),
                    None,
                    Some(value.clone()),
                );
                shard.control_state_selection.insert(
                    std::sync::Arc::from(&key[..]),
                    ControlStateSelectionValue {
                        occur_time_ms,
                        value,
                        selection_type,
                    },
                );
            }
            if ttl_ms > 0 {
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
            }
            // A comparison this value LOST stored nothing and set no deadline, so it changed
            // nothing, unless lazy expiry already removed a related record. Preserve that
            // earlier mutation even when this comparison stores no new value.
            mutated |= should_store || ttl_ms > 0;
            CommandResponse::Empty
        }
        Command::ControlStateSelectionQuery { key } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard
                    .control_state_selection
                    .get(&key[..])
                    .map(|stored| stored.value.clone()),
            }
        }
        Command::ControlStateManager {
            key,
            op_type,
            field_list,
            start_offset,
            end_offset,
            is_distinct,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let entries = control_state_manager_entries(
                shard,
                &key,
                op_type.as_deref(),
                &field_list,
                &start_offset,
                &end_offset,
                is_distinct,
            );
            CommandResponse::HashEntries { entries }
        }
        Command::ControlStateDebug {
            key,
            start_ms,
            end_ms,
        } => {
            if drop_if_expired(cache, shard_id, shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            entries.push(("key".to_string(), key.as_bytes().to_vec()));
            entries.push(("start_ms".to_string(), start_ms.to_string().into_bytes()));
            entries.push(("end_ms".to_string(), end_ms.to_string().into_bytes()));
            for family in [
                ControlStateFamily::Counter,
                ControlStateFamily::Distinct,
                ControlStateFamily::Selection,
            ] {
                let family_key = control_state_family_key(family, &key);
                let name = control_state_family_name(family);
                let series = shard.control_state().get(&family_key[..]);
                let all_values = series
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                let window = series
                    .map(|series| {
                        series
                            .range(crate::engine::timestamp_range_bounds(start_ms, end_ms))
                            .map(|(timestamp_ms, value)| (*timestamp_ms, *value))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                entries.push((
                    format!("{name}_events"),
                    all_values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_sum"),
                    crate::engine::window_aggregate::sum_legacy(all_values.iter().copied())
                        .to_string()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_events"),
                    window.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_sum"),
                    crate::engine::window_aggregate::sum_legacy(
                        window.iter().map(|(_, value)| *value),
                    )
                    .to_string()
                    .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_first_timestamp_ms"),
                    window
                        .first()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_last_timestamp_ms"),
                    window
                        .last()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
            }
            if let Some(fol) = shard.control_state_selection.get(&key[..]) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.selection_type {
                        ControlStateSelectionType::First => b"first".to_vec(),
                        ControlStateSelectionType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::ContextQueryNodeEmbeddings {
            tenant_hash,
            node_hashes,
        } => {
            let node_hashes = dedupe_nonzero_u64_preserve_order(node_hashes);
            for node_hash in node_hashes.iter().copied() {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_node_key(tenant_hash, node_hash),
                );
            }
            // One node read per hash. Decode only the vector: this command feeds embedding
            // backfill and scoring, so allocating node text and the cached summary vector would
            // create short-lived work proportional to the batch without changing the response.
            // Every id asked about is answered for. A limit belongs on a query that searches,
            // where the caller cannot know how many results exist; this batch is a list of ids the
            // caller already holds, so truncating it drops nodes the caller named, with nothing in
            // the response saying so. The sibling `ContextGetNodes` reads its whole batch, and
            // `command_validation` already declares a key for every hash in this one.
            let embeddings = node_hashes
                .into_iter()
                .filter_map(|node_hash| {
                    let vector = load_context_node_vector(
                        cache,
                        block_store,
                        shard_id,
                        shard,
                        tenant_hash,
                        node_hash,
                    )?;
                    if vector.is_empty() {
                        return None;
                    }
                    Some((node_hash, vector))
                })
                .collect();
            CommandResponse::ContextNodeEmbeddings { embeddings }
        }
        Command::ContextQueryNodeVectorProjections {
            tenant_hash,
            node_hashes,
        } => {
            let node_hashes = dedupe_nonzero_u64_preserve_order(node_hashes);
            for node_hash in node_hashes.iter().copied() {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_node_key(tenant_hash, node_hash),
                );
            }
            let projections = load_context_node_vector_projections(
                cache,
                block_store,
                shard_id,
                shard,
                tenant_hash,
                &node_hashes,
            );
            CommandResponse::ContextNodeVectorProjections { projections }
        }
        Command::ContextSetNodeEmbedding {
            tenant_hash,
            node_hash,
            model_hash,
            vector,
            updated_at_ms,
        } => {
            // Read-modify-write of the node record. The vector rides along with the node from
            // here on, so a reader that has fetched the node has already paid for the vector --
            // no second key, no second block, and no hash to invert.
            let object_key = context_node_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let existing = load_context_node(cache, block_store, shard_id, shard, &object_key);
            match existing {
                // No node to attach to. Writing a placeholder here would invent a node that
                // ingest never created, so report it rather than fabricate one.
                None => CommandResponse::ContextObjectKey {
                    object_key: String::new(),
                },
                Some(mut node) => {
                    node.vector = vector;
                    node.embedding_model_hash = model_hash;
                    node.embedding_updated_at_ms = updated_at_ms;
                    // `summary_vector` is left exactly as it was read. It is a copy of the L1
                    // SUMMARY's vector, and re-embedding the node does not re-embed the summary,
                    // so the copy is still the summary's current vector. Clearing it here would
                    // throw away a valid copy; overwriting it with the new node vector would
                    // claim the summary says something it does not.
                    mutated |= write_context_node(
                        cache,
                        block_store,
                        shard_id,
                        shard,
                        &object_key,
                        &node,
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                    CommandResponse::ContextObjectKey { object_key }
                }
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            let object_key = context_node_key(tenant_hash, node.node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            mutated |= write_context_node(
                cache,
                block_store,
                shard_id,
                shard,
                &object_key,
                &node,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            let object_key = context_node_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let node = shard
                .bucket_index
                .hashes
                .get(&object_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .map(|address| {
                    (
                        address,
                        BlockIdentity::of(shard_id, "hash", &object_key, Some(CONTEXT_NODE_FIELD)),
                    )
                })
                .or_else(|| {
                    // THE PRE-HASHES ARM IS A WHOLE `context_node` BLOCK, not the `meta` field of a
                    // hash. `model_map_block_address` spells it `("context_node", None)` and is the
                    // authority; one element named for both arms is wrong for whichever arm it does
                    // not describe, and this chain named the field for both.
                    shard
                        .context_nodes()
                        .get(object_key.as_str())
                        .map(|address| {
                            (
                                address,
                                BlockIdentity::of(shard_id, "context_node", &object_key, None),
                            )
                        })
                })
                .and_then(|(address, identity)| {
                    read_block_shared(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        identity,
                        Some(block_routing_bucket(
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                    .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            CommandResponse::ContextNode { object_key, node }
        }
        Command::ContextGetNodes {
            tenant_hash,
            node_hashes,
        } => {
            // The guard runs over the same hashes the read below uses, in a pass of its own:
            // the read borrows the shard for the whole closure, so the check cannot run
            // inside it.
            let node_hashes = dedupe_nonzero_u64_preserve_order(node_hashes);
            for node_hash in node_hashes.iter().copied() {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_node_key(tenant_hash, node_hash),
                );
            }
            let nodes = node_hashes
                .into_iter()
                .filter_map(|node_hash| {
                    let object_key = context_node_key(tenant_hash, node_hash);
                    shard
                        .bucket_index
                        .hashes
                        .get(&object_key)
                        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                        .map(|address| {
                            (
                                address,
                                BlockIdentity::of(
                                    shard_id,
                                    "hash",
                                    &object_key,
                                    Some(CONTEXT_NODE_FIELD),
                                ),
                            )
                        })
                        .or_else(|| {
                            // THE PRE-HASHES ARM IS A WHOLE `context_node` BLOCK, not the `meta` field of a
                            // hash. `model_map_block_address` spells it `("context_node", None)` and is the
                            // authority; one element named for both arms is wrong for whichever arm it does
                            // not describe, and this chain named the field for both.
                            shard
                                .context_nodes()
                                .get(object_key.as_str())
                                .map(|address| {
                                    (
                                        address,
                                        BlockIdentity::of(
                                            shard_id,
                                            "context_node",
                                            &object_key,
                                            None,
                                        ),
                                    )
                                })
                        })
                        .and_then(|(address, identity)| {
                            read_block_shared(
                                cache,
                                block_store,
                                shard_id,
                                address,
                                identity,
                                Some(block_routing_bucket(
                                    &object_key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                            .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                        })
                })
                .collect();
            CommandResponse::ContextNodes { nodes }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            mut event,
            first_write_only,
            cold_storage,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            normalize_context_event_storage_keys(node_hash, &mut event);
            // CONTEXT_TIMELINE_FANOUT is applied inside context_timeline_key so
            // multiple ContextEvent writes at the same millisecond map to stable,
            // timestamp-keyed blocks instead of overwriting one another.
            // context_models_match_keys_timeline_blocks_and_filters keeps this
            // key shape aligned with the context event timeline contract.
            let timeline_key = context_timeline_key(event.primary_time_ms(), event.event_id_hash);
            let event_id_hash = event.event_id_hash;
            // THE MEMBERSHIP TEST FIRST, THROUGH THE READ ACCESSOR, and the write afterwards. The
            // borrow this replaced held a `&mut` into the map across the whole block append, which
            // the read accessor cannot hand out.
            //
            // AND NO EMPTY SERIES IS CREATED ANY MORE. `entry(..).or_default()` filed an empty
            // series under the object key before the append was even attempted, so a failed append
            // left a key behind that every length and emptiness reader counted. The shared point
            // operation below keeps no empty series by construction: it takes the key out of the
            // map when the series ends up empty.
            let already_written = first_write_only
                && shard
                    .context_events()
                    .get(&*object_key)
                    .is_some_and(|series| series.get(event_id_hash).is_some());
            // Idempotence now tests the EVENT ID, which is what first_write_only actually means:
            // the same event written twice. Testing the timeline key made two different events
            // that collided in the low FANOUT bits of one millisecond look like a rewrite.
            if !already_written {
                let value = context_bytes(&*event);
                let routing_bucket =
                    block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
                // The BLOCK stays timestamp-keyed: blocks pack by time, and the load path recovers
                // the timeline key from the packed point. Only the index key changes.
                if let Ok(addresses) = append_timestamped_kv_blocks_keyed(
                    cache,
                    block_store,
                    shard_id,
                    "context_event",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value,
                    }],
                    routing_bucket,
                    async_storage && !cold_storage,
                    !cold_storage,
                    event_id_hash,
                    crate::engine::state::next_block_index_for_object(
                        &shard.bucket_index,
                        routing_bucket,
                        "context_event",
                        &object_key,
                    ),
                ) {
                    for (stored_timeline_key, address) in addresses {
                        // ONE HANDLE, AND BOTH MAPS TAKE IT. The series map and the time index are filed
                        // under the same object key, so they share one allocation instead of minting two.
                        // Taken here rather than before the append on purpose: the handle this borrows has
                        // to exist already, and the block index is what holds it.
                        let stored_key = shard
                            .bucket_index
                            .any_shared_object_key(&*object_key)
                            .unwrap_or_else(|| std::sync::Arc::from(&*object_key));
                        // THE NON-COLLECTING DOOR. This called `set_point_and_collect_live`
                        // and bound the result to nothing -- and that result is a clone of every
                        // address the node event series holds, so writing one event to a node
                        // copied the whole series and dropped it.
                        super::timestamped_series::set_point(
                            shard.context_events_mut(),
                            std::sync::Arc::clone(&stored_key),
                            event_id_hash,
                            Some(address),
                        );
                        shard
                            .context_event_timeline
                            .entry(std::sync::Arc::clone(&stored_key))
                            .or_default()
                            .insert(stored_timeline_key, event_id_hash);
                        mutated = true;
                    }
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            if !bulk_ingest_mode()
                && maybe_auto_compress_context_node(
                    cache,
                    block_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    node_hash,
                    &object_key,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                )
            {
                mutated = true;
            }
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            mut event,
            indexes,
            first_write_only,
            cold_storage,
        } => {
            let event_object_key = context_event_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &event_object_key);
            normalize_context_event_storage_keys(node_hash, &mut event);
            let primary_time_ms = event.primary_time_ms();
            // Extracted events use the same CONTEXT_TIMELINE_FANOUT timeline as
            // raw context events so index refs, filters, and event blocks share the
            // wire-compatible timestamp key discipline.
            let event_timeline_key = context_timeline_key(primary_time_ms, event.event_id_hash);
            let event_id_hash = event.event_id_hash;
            // Read first, write through the pair afterwards -- see the note on the
            // `ContextWriteEvent` arm above for both halves of why.
            let event_already_written = first_write_only
                && shard
                    .context_events()
                    .get(&*event_object_key)
                    .is_some_and(|series| series.get(event_id_hash).is_some());
            // Same key discipline as ContextWriteEvent since the event rekey: the primary map
            // is keyed by EVENT ID (idempotence tests the id), the block stays timestamp-keyed,
            // and the time index maps the stored timeline key back to the id. This arm was
            // missed by the rekey -- it kept inserting timeline keys into the id-keyed map and
            // never fed the time index, so every extracted event was invisible to time-ranged
            // queries while its write reported success.
            if !event_already_written {
                // `event` is boxed on this variant; the wire encoder takes the value.
                let event_value = &*event;
                let value = context_bytes(event_value);
                let routing_bucket = block_routing_bucket(
                    &event_object_key,
                    start_routing_bucket,
                    end_routing_bucket,
                );
                if let Ok(addresses) = append_timestamped_kv_blocks_keyed(
                    cache,
                    block_store,
                    shard_id,
                    "context_event",
                    &event_object_key,
                    vec![FeaturePoint {
                        timestamp_ms: event_timeline_key,
                        value,
                    }],
                    routing_bucket,
                    async_storage && !cold_storage,
                    !cold_storage,
                    event_id_hash,
                    crate::engine::state::next_block_index_for_object(
                        &shard.bucket_index,
                        routing_bucket,
                        "context_event",
                        &event_object_key,
                    ),
                ) {
                    for (stored_timeline_key, address) in addresses {
                        // ONE HANDLE, AND BOTH MAPS TAKE IT. The series map and the time index are filed
                        // under the same object key, so they share one allocation instead of minting two.
                        // Taken here rather than before the append on purpose: the handle this borrows has
                        // to exist already, and the block index is what holds it.
                        let stored_key = shard
                            .bucket_index
                            .any_shared_object_key(&*event_object_key)
                            .unwrap_or_else(|| std::sync::Arc::from(&*event_object_key));
                        // THE NON-COLLECTING DOOR. This called `set_point_and_collect_live`
                        // and bound the result to nothing -- and that result is a clone of every
                        // address the node event series holds, so writing one event to a node
                        // copied the whole series and dropped it.
                        super::timestamped_series::set_point(
                            shard.context_events_mut(),
                            std::sync::Arc::clone(&stored_key),
                            event_id_hash,
                            Some(address),
                        );
                        shard
                            .context_event_timeline
                            .entry(std::sync::Arc::clone(&stored_key))
                            .or_default()
                            .insert(stored_timeline_key, event_id_hash);
                        mutated = true;
                    }
                }
            }
            invalidate_context_record(cache, shard_id, &event_object_key);

            let index_ref = ContextIndexRef {
                primary_node_hash: node_hash,
                primary_event_time_ms: primary_time_ms,
                event_id_hash: event.event_id_hash,
            };
            let mut index_object_keys = Vec::new();
            let mut write_default_index =
                |index_name: &str, value_hash: u64, index_time_ms: u64| {
                    if value_hash == 0 || index_time_ms == 0 {
                        return;
                    }
                    let object_key =
                        context_index_key(tenant_hash, index_name, value_hash, indexes.scope_hash);
                    let timeline_key = context_timeline_key(index_time_ms, index_ref.event_id_hash);
                    let value = context_bytes(&index_ref);
                    let routing_bucket =
                        block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
                    if let Ok(addresses) = append_timestamped_kv_blocks(
                        cache,
                        block_store,
                        shard_id,
                        "context_index",
                        &object_key,
                        vec![FeaturePoint {
                            timestamp_ms: timeline_key,
                            value,
                        }],
                        routing_bucket,
                        async_storage && !cold_storage,
                        !cold_storage,
                        crate::engine::state::next_block_index_for_object(
                            &shard.bucket_index,
                            routing_bucket,
                            "context_index",
                            &object_key,
                        ),
                    ) {
                        let series = shard
                            .context_indexes_mut()
                            .entry(std::sync::Arc::from(object_key.as_str()))
                            .or_default();
                        for (timestamp_ms, address) in addresses {
                            series.set(timestamp_ms, address);
                            mutated = true;
                        }
                        invalidate_context_record(cache, shard_id, &object_key);
                        index_object_keys.push(object_key);
                    }
                };

            if !context_index_disabled(&indexes, InternalContextIndex::EventKind) {
                write_default_index(
                    "event_kind",
                    context_event_kind_hash(&event),
                    primary_time_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Status) {
                write_default_index("status", indexes.status_hash, primary_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Source) {
                write_default_index("source", indexes.source_hash, primary_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::EventTimeBucket) {
                write_default_index(
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.event_time_bucket_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Entity) {
                for entity_hash in &indexes.entity_hashes {
                    write_default_index("entity", *entity_hash, primary_time_ms);
                }
            }
            CommandResponse::ContextExtractedEventWrite {
                event_object_key,
                written_index_count: index_object_keys.len(),
                index_object_keys,
            }
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            max_scan,
            current_valid_only,
            as_of_ms,
            kinds,
            statuses,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let scan_limit = context_limit(max_scan);
            let events = shard
                .context_events()
                .get(&*object_key)
                .map(|_series| {
                    let mut block_cache = HashMap::new();
                    // Range the TIME INDEX, not the primary map: the primary is keyed by event
                    // id hash now, so a time window is not contiguous in it.
                    context_event_time_range(shard, &object_key, start_time_ms, end_time_ms)
                        .rev()
                        // Bound the SCAN (kMaxLimit), NOT the result: the caller's
                        // `limit` must be applied AFTER filtering (LimitOrDefault runs
                        // post-filter). Scan newest-first so retrieval can find recent,
                        // serving-relevant context without walking cold history first.
                        .take(scan_limit)
                        .filter_map(|(timeline_key, address)| {
                            read_context_value_cached::<ContextEvent>(
                                cache,
                                block_store,
                                shard_id,
                                timeline_key,
                                address,
                                BlockIdentity::of(shard_id, "context_event", &object_key, None),
                                &mut block_cache,
                                Some(block_routing_bucket(
                                    &object_key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        })
                        .filter(|event| {
                            context_event_matches_filter(
                                event,
                                current_valid_only,
                                as_of_ms,
                                end_time_ms,
                                &kinds,
                                &statuses,
                                min_confidence,
                                min_importance,
                            )
                        })
                        .take(context_limit(limit))
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextEvents { object_key, events }
        }
        Command::ContextQueryEventsByIds {
            tenant_hash,
            node_hash,
            event_id_hashes,
            limit,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let requested = dedupe_nonzero_u64_preserve_order(event_id_hashes)
                .into_iter()
                .take(context_limit(limit))
                .collect::<Vec<_>>();
            let requested_set = requested.iter().copied().collect::<HashSet<_>>();
            let mut timeline_by_event = HashMap::with_capacity(requested.len());
            if let Some(timeline) = shard.context_event_timeline.get(&*object_key) {
                for (timeline_key, event_id_hash) in timeline {
                    if requested_set.contains(event_id_hash) {
                        timeline_by_event.insert(*event_id_hash, *timeline_key);
                        if timeline_by_event.len() == requested.len() {
                            break;
                        }
                    }
                }
            }
            let mut block_cache = HashMap::new();
            let events = shard
                .context_events()
                .get(&*object_key)
                .map(|series| {
                    requested
                        .into_iter()
                        .filter_map(|event_id_hash| {
                            let timeline_key = *timeline_by_event.get(&event_id_hash)?;
                            let address = series.get(event_id_hash)?;
                            read_context_value_cached::<ContextEvent>(
                                cache,
                                block_store,
                                shard_id,
                                timeline_key,
                                address,
                                BlockIdentity::of(shard_id, "context_event", &object_key, None),
                                &mut block_cache,
                                Some(block_routing_bucket(
                                    &object_key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextEvents { object_key, events }
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            event_time_ms,
            index_ref,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let timeline_key = context_timeline_key(event_time_ms, index_ref.event_id_hash);
            let value = context_bytes(&index_ref);
            let routing_bucket =
                block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "context_index",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_bucket,
                async_storage,
                true,
                crate::engine::state::next_block_index_for_object(
                    &shard.bucket_index,
                    routing_bucket,
                    "context_index",
                    &object_key,
                ),
            ) {
                let series = shard
                    .context_indexes_mut()
                    .entry(std::sync::Arc::from(object_key.as_str()))
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.set(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let refs = shard
                .context_indexes()
                .get(&object_key[..])
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextIndexRef>(
                                cache,
                                block_store,
                                shard_id,
                                timeline_key,
                                address,
                                BlockIdentity::of(shard_id, "context_index", &object_key, None),
                                Some(block_routing_bucket(
                                    &object_key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextIndexRefs { object_key, refs }
        }
        Command::ContextQueryIndexIntersection {
            tenant_hash,
            predicates,
            limit,
        } => {
            for predicate in &predicates {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_index_key(
                        tenant_hash,
                        &predicate.index_name,
                        predicate.index_value_hash,
                        predicate.scope_hash,
                    ),
                );
            }
            let mut scanned_ref_count = 0usize;
            let mut deduped_ref_count = 0usize;
            let mut candidate_refs: Option<HashMap<(u64, u64, u64), ContextIndexRef>> = None;

            for predicate in &predicates {
                let object_key = context_index_key(
                    tenant_hash,
                    &predicate.index_name,
                    predicate.index_value_hash,
                    predicate.scope_hash,
                );
                let mut seen_for_predicate = HashMap::new();
                if let Some(series) = shard.context_indexes().get(&object_key[..]) {
                    let mut block_cache = HashMap::new();
                    for (timeline_key, address) in series.range(
                        context_timeline_start(predicate.start_time_ms)
                            ..context_timeline_end(predicate.end_time_ms),
                    ) {
                        if let Some(index_ref) = read_context_value_cached::<ContextIndexRef>(
                            cache,
                            block_store,
                            shard_id,
                            timeline_key,
                            address,
                            BlockIdentity::of(shard_id, "context_index", &object_key, None),
                            &mut block_cache,
                            Some(block_routing_bucket(
                                &object_key,
                                start_routing_bucket,
                                end_routing_bucket,
                            )),
                        ) {
                            scanned_ref_count += 1;
                            let key = context_index_ref_identity(&index_ref);
                            if seen_for_predicate.insert(key, index_ref).is_some() {
                                deduped_ref_count += 1;
                            }
                        }
                    }
                }

                candidate_refs = match candidate_refs {
                    None => Some(seen_for_predicate),
                    Some(mut existing) => {
                        existing.retain(|key, _| seen_for_predicate.contains_key(key));
                        Some(existing)
                    }
                };
                if candidate_refs.as_ref().is_some_and(HashMap::is_empty) {
                    break;
                }
            }

            let mut refs: Vec<_> = candidate_refs.unwrap_or_default().into_values().collect();
            refs.sort_by_key(|index_ref| {
                (
                    index_ref.primary_event_time_ms,
                    index_ref.event_id_hash,
                    index_ref.primary_node_hash,
                )
            });
            refs.truncate(context_limit(limit));
            CommandResponse::ContextIndexIntersection {
                refs,
                matched_index_count: predicates.len(),
                scanned_ref_count,
                deduped_ref_count,
            }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            let object_key = context_audit_key(tenant_hash, audit.session_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let timeline_key =
                context_timeline_key(audit.request_time_ms, stable_object_hash(&audit.query_id));
            let value = context_bytes(&audit);
            let routing_bucket =
                block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "context_audit",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_bucket,
                async_storage,
                true,
                crate::engine::state::next_block_index_for_object(
                    &shard.bucket_index,
                    routing_bucket,
                    "context_audit",
                    &object_key,
                ),
            ) {
                let series = shard
                    .context_audits_mut()
                    .entry(std::sync::Arc::from(object_key.as_str()))
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.set(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key = context_audit_key(tenant_hash, session_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let audits = shard
                .context_audits()
                .get(&object_key[..])
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextPackAudit>(
                                cache,
                                block_store,
                                shard_id,
                                timeline_key,
                                address,
                                BlockIdentity::of(shard_id, "context_audit", &object_key, None),
                                Some(block_routing_bucket(
                                    &object_key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextPackAudits { object_key, audits }
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
        } => {
            // In-memory, coalesced summary-dirty tracking. Repeated marks for the same
            // node update a single hashmap entry instead of appending a new persisted
            // block, so dirty records are bounded by distinct dirty nodes, not by events.
            // The map is ephemeral (never written to the block store) and may be lost on
            // restart; the async summary worker re-marks on the next event.
            let object_key = context_dirty_key(tenant_hash, node_hash);
            let entry = shard
                .context_dirty_index
                .entry(crate::engine::state::DirtyMark::summary(
                    tenant_hash,
                    node_hash,
                ))
                .or_default();
            if entry.mark_count == 0 {
                entry.node_hash = node_hash;
                entry.first_event_time_ms = event_time_ms;
                entry.last_event_time_ms = event_time_ms;
            } else {
                entry.first_event_time_ms = entry.first_event_time_ms.min(event_time_ms);
                entry.last_event_time_ms = entry.last_event_time_ms.max(event_time_ms);
            }
            entry.reason = reason;
            entry.propagate_depth = entry.propagate_depth.max(propagate_depth);
            entry.mark_count = entry.mark_count.saturating_add(1);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            // Read the single coalesced in-memory dirty entry for this node and surface it
            // as one marker when it overlaps the requested time window. `limit` is retained
            // for API compatibility but there is at most one coalesced marker per node.
            let _ = limit;
            let object_key = context_dirty_key(tenant_hash, node_hash);
            let nodes = shard
                .context_dirty_index
                .get(&crate::engine::state::DirtyMark::summary(
                    tenant_hash,
                    node_hash,
                ))
                .filter(|entry| {
                    entry.mark_count > 0
                        && entry.last_event_time_ms >= start_time_ms
                        && entry.first_event_time_ms <= end_time_ms
                })
                .map(|entry| {
                    vec![ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    }]
                })
                .unwrap_or_default();
            CommandResponse::ContextSummaryDirtyNodes { object_key, nodes }
        }
        Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            node_hash,
            event_time_ms,
            reason,
            propagate_depth,
            clear,
        } => {
            // In-memory, coalesced embedding-dirty tracking, independent of the
            // summary-dirty index. `clear` removes the entry (the drainer clears a
            // node once it has been successfully embedded); otherwise repeated marks
            // for the same node coalesce into a single entry so dirty records stay
            // bounded by distinct dirty nodes, not by events. The map is ephemeral
            // (never written to the block store) and re-derived after restart.
            let object_key = context_embedding_dirty_key(tenant_hash, node_hash);
            if clear {
                shard
                    .context_dirty_index
                    .remove(&crate::engine::state::DirtyMark::embedding(
                        tenant_hash,
                        node_hash,
                    ));
            } else {
                let entry = shard
                    .context_dirty_index
                    .entry(crate::engine::state::DirtyMark::embedding(
                        tenant_hash,
                        node_hash,
                    ))
                    .or_default();
                if entry.mark_count == 0 {
                    entry.tenant_hash = tenant_hash;
                    entry.node_hash = node_hash;
                    entry.first_event_time_ms = event_time_ms;
                    entry.last_event_time_ms = event_time_ms;
                } else {
                    entry.first_event_time_ms = entry.first_event_time_ms.min(event_time_ms);
                    entry.last_event_time_ms = entry.last_event_time_ms.max(event_time_ms);
                }
                entry.reason = reason;
                entry.propagate_depth = entry.propagate_depth.max(propagate_depth);
                entry.mark_count = entry.mark_count.saturating_add(1);
            }
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryEmbeddingDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            // node_hash == 0 -> the drainer's O(pending) scan over the whole
            // embedding-dirty index (all tenants on the shard). node_hash != 0 ->
            // the single coalesced entry for that node. Either way this touches only
            // the pending set, never the corpus.
            let cap = context_limit(limit);
            let mut nodes = Vec::new();
            let mut tenant_hashes = Vec::new();
            let object_key = if node_hash == 0 {
                // Deterministic order (by object key) so a bounded drain pass is
                // stable across calls.
                let mut entries: Vec<_> = shard
                    .context_dirty_index
                    .iter()
                    .filter(|(mark, entry)| {
                        // THE FLAVOUR IS A FIELD NOW. This read the key's PREFIX, and its comment
                        // explained that the test was load-bearing because `tenant_hash` is
                        // left zero on summary marks, so the tenant clause below would admit
                        // them on an all-tenant drain. The key carries the tenant for BOTH
                        // flavours now, so the two cannot be confused whether or not this
                        // clause is here -- it stays because the drain still wants only one
                        // flavour, not because the other one would leak through.
                        mark.flavour == crate::engine::state::DirtyFlavour::Embedding
                            && entry.mark_count > 0
                            && entry.last_event_time_ms >= start_time_ms
                            && entry.first_event_time_ms <= end_time_ms
                            && (tenant_hash == 0 || entry.tenant_hash == tenant_hash)
                    })
                    .collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                for (_, entry) in entries.into_iter().take(cap) {
                    nodes.push(ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    });
                    tenant_hashes.push(entry.tenant_hash);
                }
                context_embedding_dirty_key(tenant_hash, 0)
            } else {
                let object_key = context_embedding_dirty_key(tenant_hash, node_hash);
                if let Some(entry) = shard
                    .context_dirty_index
                    .get(&crate::engine::state::DirtyMark::embedding(
                        tenant_hash,
                        node_hash,
                    ))
                    .filter(|entry| {
                        entry.mark_count > 0
                            && entry.last_event_time_ms >= start_time_ms
                            && entry.first_event_time_ms <= end_time_ms
                    })
                {
                    nodes.push(ContextDirtyNode {
                        node_hash: entry.node_hash,
                        first_event_time_ms: entry.first_event_time_ms,
                        last_event_time_ms: entry.last_event_time_ms,
                        reason: entry.reason,
                        propagate_depth: entry.propagate_depth,
                        mark_count: entry.mark_count,
                    });
                    tenant_hashes.push(entry.tenant_hash);
                }
                object_key
            };
            CommandResponse::ContextEmbeddingDirtyNodes {
                object_key,
                nodes,
                tenant_hashes,
            }
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            // The BLOCK is still addressed per entity -- the object id must stay unique per
            // entity or two entities of one node would overwrite each other's bytes. Only the
            // INDEX changes shape: the node's collection key owns a BTreeMap keyed by entity
            // hash, so the entity's own key no longer occupies a map slot of its own.
            let object_key = context_entity_key(tenant_hash, entity.node_hash, entity.entity_hash);
            let collection_key = context_entity_collection_key(tenant_hash, entity.node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            mutated |= drop_if_expired(cache, shard_id, shard, &collection_key);
            let collection_key_for_response = collection_key.clone();
            let object_id = stable_block_object_id(shard_id, "context_entity", &object_key);
            let routing_bucket =
                block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            let bytes = context_bytes(&entity);
            if let Ok(address) = append_value(
                cache,
                block_store,
                shard_id,
                &bytes,
                Some(object_id),
                // `None`, matching the object id derived from ("context_entity", &object_key, None):
                // the entity's own key IS the object key, so its block is a whole object.
                None,
                Some(routing_bucket),
                async_storage,
            ) {
                // insert() on the entity hash OVERWRITES: an entity has one current value, and
                // its history is the separate context_entity_update_audit series. Keying this
                // map by time instead would silently turn every upsert into an append.
                stage_element_name_outcome(
                    shard_id,
                    "context_entity",
                    &collection_key,
                    Some(entity.entity_hash.to_string()),
                    routing_bucket,
                    Some(address.clone()),
                    None,
                );
                shard
                    .context_entities_mut()
                    .entry(std::sync::Arc::from(&collection_key[..]))
                    .or_default()
                    .set(entity.entity_hash, address);
                mutated = true;
            }
            invalidate_context_record(cache, shard_id, &object_key);
            invalidate_context_record(cache, shard_id, &collection_key_for_response);
            CommandResponse::ContextObjectKey {
                object_key: collection_key_for_response,
            }
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            let collection_key = context_entity_collection_key(tenant_hash, node_hash);
            let object_key = collection_key.clone();
            mutated |= drop_if_expired(cache, shard_id, shard, &collection_key);
            // THE BLOCK'S OWN KEY IS THE PER-ENTITY KEY, not the collection key this map is keyed
            // by: `ContextUpsertEntity` files the block under `context_entity_key(..)` and derives
            // its routing bucket from that, so a read has to name the same string or it looks in
            // the cache under a key nothing ever wrote. The index groups entities by node; the
            // BLOCK is still per entity, and that split is exactly what this line has to respect.
            let entity = shard
                .context_entities()
                .get(&collection_key[..])
                .and_then(|series| series.get(entity_hash))
                .and_then(|address| {
                    read_block_bytes(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        // THE PER-ENTITY KEY, not the collection key this map is keyed by -- the
                        // same string the bucket below is derived from, and the one
                        // `ContextUpsertEntity` files the block under. Filed with `None`.
                        BlockIdentity::of(
                            shard_id,
                            "context_entity",
                            &context_entity_key(tenant_hash, node_hash, entity_hash),
                            None,
                        ),
                        Some(block_routing_bucket(
                            &context_entity_key(tenant_hash, node_hash, entity_hash),
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                    .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
                });
            CommandResponse::ContextEntity { object_key, entity }
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            let object_key = context_entity_collection_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let series = shard.context_entities().get(&object_key[..]);
            // TAKES THE ENTITY HASH, because the block's routing bucket comes from the per-entity
            // key and this map is keyed by the node's collection key. The "every entity" arm below
            // therefore iterates `iter()` rather than `values()`: the hash it was discarding is
            // what names the block.
            let read_entity = |entity_hash: u64, address: &ElementEntry| {
                read_block_bytes(
                    cache,
                    block_store,
                    shard_id,
                    address,
                    // The per-entity key, as the bucket below. Filed with `None`.
                    BlockIdentity::of(
                        shard_id,
                        "context_entity",
                        &context_entity_key(tenant_hash, node_hash, entity_hash),
                        None,
                    ),
                    Some(block_routing_bucket(
                        &context_entity_key(tenant_hash, node_hash, entity_hash),
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                )
                .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
            };
            // An empty entity_hashes now means "every entity of this node" instead of "nothing".
            // Before the fold the caller had to already know each hash, because the entities of
            // one node were separate HashMap keys and a HashMap cannot be prefix-scanned -- so a
            // node's entity set could not be discovered from the engine at all. Passing explicit
            // hashes still selects exactly those, so existing callers are unaffected.
            let entities = match (series, entity_hashes.is_empty()) {
                (None, _) => Vec::new(),
                (Some(series), true) => series
                    .points()
                    .take(context_limit(limit))
                    .filter_map(|(entity_hash, address)| read_entity(entity_hash, address))
                    .collect(),
                (Some(series), false) => dedupe_nonzero_u64_preserve_order(entity_hashes)
                    .into_iter()
                    .take(context_limit(limit))
                    .filter_map(|entity_hash| {
                        series
                            .get(entity_hash)
                            .and_then(|address| read_entity(entity_hash, address))
                    })
                    .collect(),
            };
            CommandResponse::ContextEntities {
                object_key,
                entities,
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            let object_key = context_child_key(tenant_hash, child_ref.parent_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let existing = load_context_children(cache, block_store, shard_id, shard, &object_key);
            let created = existing
                .iter()
                .all(|stored| stored.child_hash != child_ref.child_hash);
            if created {
                let timeline_key =
                    context_timeline_key(child_ref.updated_at_ms, child_ref.child_hash);
                let routing_bucket =
                    block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
                if let Ok(addresses) = append_timestamped_kv_blocks(
                    cache,
                    block_store,
                    shard_id,
                    "context_child",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&child_ref),
                    }],
                    routing_bucket,
                    async_storage,
                    true,
                    crate::engine::state::next_block_index_for_object(
                        &shard.bucket_index,
                        routing_bucket,
                        "context_child",
                        &object_key,
                    ),
                ) {
                    let series = shard
                        .context_children_mut()
                        .entry(std::sync::Arc::from(&object_key[..]))
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.set(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            CommandResponse::ContextChildRefs {
                object_key,
                refs: Vec::new(),
                created: Some(created),
            }
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            let object_key = context_child_key(tenant_hash, parent_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let mut refs = load_context_children(cache, block_store, shard_id, shard, &object_key);
            refs.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            // Keep the NEWEST `limit`, not the oldest. Sorting ascending and truncating handed back
            // a parent's first children and hid everything recently added -- five children with
            // limit 2 answered with the two oldest, and the newest was unreachable by any query.
            //
            // The same cut, in the same direction, is what traversal was fixed for: the most
            // recently ingested were the first to become invisible, which is the opposite of what
            // a store keyed on time should do. The listing stays in chronological order, which is
            // what an unlimited query already returned.
            let keep = context_limit(limit);
            if refs.len() > keep {
                refs.drain(..refs.len() - keep);
            }
            CommandResponse::ContextChildRefs {
                object_key,
                refs,
                created: None,
            }
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            leaf_only,
        } => {
            // The walk starts at one node and follows the children it finds. Those two keys
            // are what this arm names; a node dropped further down the walk is the sweep's
            // job, not a read's.
            let start_node_key = context_node_key(tenant_hash, start_node_hash);
            let start_child_key = context_child_key(tenant_hash, start_node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &start_node_key);
            mutated |= drop_if_expired(cache, shard_id, shard, &start_child_key);
            let outcome = traverse_context_tree(
                cache,
                block_store,
                shard_id,
                shard,
                tenant_hash,
                start_node_hash,
                &query_vector,
                max_depth,
                top_k_per_depth,
                max_children_scored_per_parent,
                max_candidate_nodes,
                leaf_only,
            );
            CommandResponse::ContextTraversedNodes {
                nodes: outcome.nodes,
                children_dropped_before_scoring: outcome.children_dropped_before_scoring,
            }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            let object_key = context_summary_key(tenant_hash, summary.node_hash, summary.level);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let timeline_key =
                context_timeline_key(summary.valid_from_ms, u64::from(summary.level));
            let routing_bucket =
                block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "context_summary",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&summary),
                }],
                routing_bucket,
                async_storage,
                true,
                crate::engine::state::next_block_index_for_object(
                    &shard.bucket_index,
                    routing_bucket,
                    "context_summary",
                    &object_key,
                ),
            ) {
                let series = shard
                    .context_summaries_mut()
                    .entry(std::sync::Arc::from(&object_key[..]))
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.set(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            // Keep the node's copy of this vector current. The summary record just written
            // remains the owner; the copy exists so the scoring pass, which fetches the node
            // anyway, does not have to come back here for one field.
            //
            // Only the L1 level: that is the only level scoring reads a vector from, and the
            // node has one slot, so copying L0 here would overwrite the answer with a summary
            // nothing asks for.
            if context_node_summary_vector_enabled()
                && summary.level == CONTEXT_SUMMARY_LEVEL_L1
                && !summary.vector.is_empty()
            {
                let node_key = context_node_key(tenant_hash, summary.node_hash);
                if let Some(node) =
                    load_context_node(cache, block_store, shard_id, shard, &node_key)
                {
                    // Never move the copy BACKWARDS. A summary can be written with an older
                    // `valid_from_ms` than one already stored -- a backfill, a replay, a
                    // correction -- and taking it would leave the node claiming a superseded
                    // vector is the newest, which is precisely the read the copy answers.
                    if node.summary_vector_valid_from_ms <= summary.valid_from_ms {
                        let mut updated = node.clone();
                        // Stored form, not the value as computed. The node being compared
                        // against came off a block, and both stored vector forms round, so a raw
                        // vector differs from its own round trip -- which would call every
                        // ingest a change and append a second node block for each one.
                        updated.summary_vector = context_vector_as_stored(&summary.vector);
                        updated.summary_vector_valid_from_ms = summary.valid_from_ms;
                        // The encoder stamp travels with the vector, or the copy becomes the one
                        // scored vector whose encoder nothing checks.
                        updated.summary_vector_model_hash = summary.embedding_model_hash;
                        // Write only when the record would actually change. The ingest already
                        // wrote the node carrying this copy, moments before the summary write
                        // that lands here.
                        if context_bytes(&updated) != context_bytes(&node) {
                            mutated |= write_context_node(
                                cache,
                                block_store,
                                shard_id,
                                shard,
                                &node_key,
                                &updated,
                                start_routing_bucket,
                                end_routing_bucket,
                                async_storage,
                            );
                        }
                    }
                }
            }
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            let object_key = context_summary_key(tenant_hash, node_hash, level);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let mut summaries = load_context_summaries(
                cache,
                block_store,
                shard_id,
                shard,
                &object_key,
                as_of_ms,
                limit,
            );
            summaries.sort_by_key(|summary| summary.valid_from_ms);
            CommandResponse::ContextSummaries {
                object_key,
                summaries,
            }
        }
        Command::ContextQuerySummaryVectors {
            tenant_hash,
            node_hashes,
            level,
            as_of_ms,
        } => {
            let node_hashes = dedupe_nonzero_u64_preserve_order(node_hashes);
            for node_hash in node_hashes.iter().copied() {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_summary_key(tenant_hash, node_hash, level),
                );
            }
            // One summary read per node -- the same per-node cost the separate embedding rows
            // had, minus the second keyspace. Only the newest summary at or before `as_of_ms`
            // is consulted; a summary carrying no vector contributes nothing, so the caller can
            // tell "not embedded" apart from "not summarized" by the node's absence here.
            let vectors = node_hashes
                .into_iter()
                .filter_map(|node_hash| {
                    let object_key = context_summary_key(tenant_hash, node_hash, level);
                    // Newest, not oldest. The series ascends with time, so the previous
                    // `take(1).next()` returned the node's FIRST summary: once a node had been
                    // re-summarised, scoring used the superseded embedding, and a stale vector of
                    // the right width still scores a plausible cosine, so nothing surfaced it.
                    load_newest_context_summary(
                        cache,
                        block_store,
                        shard_id,
                        shard,
                        &object_key,
                        as_of_ms,
                    )
                    .filter(|summary| !summary.vector.is_empty())
                    .map(|summary| ContextSummaryVector {
                        node_hash,
                        embedding_model_hash: summary.embedding_model_hash,
                        vector: summary.vector,
                    })
                })
                .collect();
            CommandResponse::ContextSummaryVectors { vectors }
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            let object_key = context_compression_key(tenant_hash, event.node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let timeline_key =
                context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
            let routing_bucket =
                block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
            if let Ok(addresses) = append_timestamped_kv_blocks(
                cache,
                block_store,
                shard_id,
                "context_compression",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&event),
                }],
                routing_bucket,
                async_storage,
                true,
                crate::engine::state::next_block_index_for_object(
                    &shard.bucket_index,
                    routing_bucket,
                    "context_compression",
                    &object_key,
                ),
            ) {
                let series = shard
                    .context_compressions_mut()
                    .entry(std::sync::Arc::from(&object_key[..]))
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.set(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_context_record(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            for node_hash in node_hashes
                .iter()
                .copied()
                .filter(|node_hash| *node_hash != 0)
            {
                mutated |= drop_if_expired(
                    cache,
                    shard_id,
                    shard,
                    &context_compression_key(tenant_hash, node_hash),
                );
            }
            let mut events = load_context_compression_events(
                cache,
                block_store,
                shard_id,
                shard,
                tenant_hash,
                &node_hashes,
                start_time_ms,
                end_time_ms,
                limit,
            );
            let object_key = node_hashes
                .iter()
                .find(|node_hash| **node_hash != 0)
                .map(|node_hash| context_compression_key(tenant_hash, *node_hash))
                .unwrap_or_else(|| context_compression_key(tenant_hash, 0));
            CommandResponse::ContextCompressionEvents {
                object_key,
                events: {
                    events.truncate(context_limit(limit));
                    events
                },
                source_event_count: None,
                truncated_source_events: None,
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_compression_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &object_key);
            let source_event_key = context_event_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &source_event_key);
            let source_limit = context_limit(max_source_events);
            let mut selected = shard
                .context_events()
                .get(&*context_event_key(tenant_hash, node_hash))
                .map(|_series| {
                    context_event_time_range(
                        shard,
                        &context_event_key(tenant_hash, node_hash),
                        source_start_ms,
                        source_end_ms,
                    )
                    .filter_map(|(timeline_key, address)| {
                        read_context_value_cold::<ContextEvent>(block_store, timeline_key, address)
                    })
                    .filter(|event| {
                        event.confidence >= min_confidence && event.importance >= min_importance
                    })
                    .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            selected.sort_by_key(|event| (event.event_time_ms, event.event_id_hash));
            let truncated = selected.len() > source_limit;
            selected.truncate(source_limit);
            if selected.is_empty() {
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: Vec::new(),
                    source_event_count: Some(0),
                    truncated_source_events: Some(false),
                }
            } else {
                let event = build_context_compression_event(
                    tenant_hash,
                    node_hash,
                    source_start_ms,
                    source_end_ms,
                    compressed_time_ms,
                    &selected,
                    truncated,
                );
                let timeline_key =
                    context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
                let routing_bucket =
                    block_routing_bucket(&object_key, start_routing_bucket, end_routing_bucket);
                if let Ok(addresses) = append_timestamped_kv_blocks(
                    cache,
                    block_store,
                    shard_id,
                    "context_compression",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&event),
                    }],
                    routing_bucket,
                    async_storage,
                    false,
                    crate::engine::state::next_block_index_for_object(
                        &shard.bucket_index,
                        routing_bucket,
                        "context_compression",
                        &object_key,
                    ),
                ) {
                    let series = shard
                        .context_compressions_mut()
                        .entry(std::sync::Arc::from(&object_key[..]))
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.set(timestamp_ms, address);
                        mutated = true;
                    }
                }
                invalidate_context_record(cache, shard_id, &object_key);
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: vec![event],
                    source_event_count: Some(selected.len() as u32),
                    truncated_source_events: Some(truncated),
                }
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            let node_key = context_node_key(tenant_hash, node_hash);
            mutated |= drop_if_expired(cache, shard_id, shard, &node_key);
            let node = shard
                .bucket_index
                .hashes
                .get(&node_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .map(|address| {
                    (
                        address,
                        BlockIdentity::of(shard_id, "hash", &node_key, Some(CONTEXT_NODE_FIELD)),
                    )
                })
                .or_else(|| {
                    // THE PRE-HASHES ARM IS A WHOLE `context_node` BLOCK, not the `meta` field of a
                    // hash. `model_map_block_address` spells it `("context_node", None)` and is the
                    // authority; one element named for both arms is wrong for whichever arm it does
                    // not describe, and this chain named the field for both.
                    shard.context_nodes().get(node_key.as_str()).map(|address| {
                        (
                            address,
                            BlockIdentity::of(shard_id, "context_node", &node_key, None),
                        )
                    })
                })
                .and_then(|(address, identity)| {
                    read_block_shared(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        identity,
                        Some(block_routing_bucket(
                            &node_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                    .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            let level = summary_level.unwrap_or(1).max(1);
            let summary_key = context_summary_key(tenant_hash, node_hash, level);
            let latest_summary = load_latest_context_summary(
                cache,
                block_store,
                shard_id,
                shard,
                &summary_key,
                as_of_ms,
            );
            let cold_window_summaries = if cold_start_time_ms == 0 && cold_end_time_ms == 0 {
                Vec::new()
            } else {
                load_context_compression_events(
                    cache,
                    block_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    &[node_hash],
                    cold_start_time_ms,
                    cold_end_time_ms,
                    compression_limit,
                )
            };
            CommandResponse::ContextNodeContext {
                node_exists: node.is_some(),
                node,
                overall_summary_exists: latest_summary.is_some(),
                overall_summary: latest_summary,
                cold_window_summaries,
            }
        }
    };
    ExecuteOutcome { response, mutated }
}

/// IEEE-754 total-order bias: flip everything for negatives, set the sign for positives, so
/// unsigned comparison of the bits is numeric comparison of the floats.
pub(super) fn zset_score_bits(score: f64) -> u64 {
    let bits = score.to_bits();
    if bits & (1 << 63) != 0 {
        !bits
    } else {
        bits | (1 << 63)
    }
}

pub(super) fn zset_score_from_bits(biased: u64) -> f64 {
    if biased & (1 << 63) != 0 {
        f64::from_bits(biased & !(1 << 63))
    } else {
        f64::from_bits(!biased)
    }
}

pub(super) fn zset_score_string(biased: u64) -> String {
    let score = zset_score_from_bits(biased);
    if score == score.trunc() && score.abs() < 1e17 {
        format!("{}", score as i64)
    } else {
        format!("{score}")
    }
}

/// The persisted element name: the member alone, spelled ONCE.
///
/// USED TO BE SCORE BITS THEN MEMBER -- the comment this replaces said so, and the lexical order
/// it bought was never built on: every zset serving arm that returns members in score order
/// reads the score out of `shard.bucket_index.zsets` and sorts explicitly (`zset_members_in_score_range`,
/// `zset_ordered_refs`), so dropping the score from this string costs that ordering nothing.
/// What it buys is the thing this change is for: `BlockIndex.element_name` for a zset entry is now
/// the same shape as a set's, so zset can collapse to one index entry a block the way set and list
/// already have. The score still rides every write -- see `RecordedKind::outcome_value` and the
/// WAL outcome's `value` field -- it is simply no longer spelled into this string.
/// WAS `hex::encode(member)`, WHICH CHARGED TWO BYTES PER MEMBER BYTE. The element name is the term
/// that scales with member width, so that doubling was the member-width RATE of a zset write and
/// not a fixed overhead on it. [`super::container_blocks::member_element_name`] charges one byte plus
/// the member, and the tag it puts in front is what keeps the two spellings tellable apart on a
/// string that carries no spelling byte of its own.
pub(super) fn zset_element_name(member: &[u8]) -> String {
    super::container_blocks::member_element_name(member)
}

/// The persisted element name for a SET member: the member alone, spelled ONCE.
///
/// Sibling of [`zset_element_name`], and the same rendering as it -- a set element name and a zset
/// element name have been the same shape since the score came off the zset string. They stay two
/// names because the two kinds reach this from different arms, so a per-kind divergence has
/// somewhere to land, and because what this replaces was `hex::encode` spelled INLINE at five
/// separate set write sites. A spelling decided at each site is a spelling that can be flipped at
/// four of them and missed at the fifth.
pub(super) fn set_element_name(member: &[u8]) -> String {
    super::container_blocks::member_element_name(member)
}

/// The members whose score falls in `[min_bits, max_bits]`, in score order.
///
/// Same scan as `zset_ordered_refs` -- the map is keyed by member, so every member has to be
/// looked at either way -- but the score test happens BEFORE the member bytes are copied, so a
/// narrow window copies a narrow window. The full-order helper below clones all n up front, which
/// cost 4,115 allocations and 324 KB to return eight members from a set of 4,096.
fn zset_members_in_score_range(
    shard: &ShardState,
    key: &str,
    min_bits: u64,
    max_bits: u64,
    min_exclusive: bool,
    max_exclusive: bool,
) -> Vec<(Vec<u8>, u64)> {
    let mut ordered: Vec<(Vec<u8>, u64)> = shard
        .bucket_index
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .filter(|(_, (biased, _))| {
                    let above = if min_exclusive {
                        *biased > min_bits
                    } else {
                        *biased >= min_bits
                    };
                    let below = if max_exclusive {
                        *biased < max_bits
                    } else {
                        *biased <= max_bits
                    };
                    above && below
                })
                .map(|(member, (biased, _))| (member.clone(), *biased))
                .collect()
        })
        .unwrap_or_default();
    ordered.sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    ordered
}

/// Every member of a zset in score order, as BORROWS rather than copies.
///
/// The helper that stood here cloned every member before it knew which ones the caller wanted, so
/// a rank range of eight out of 4,096 paid one allocation per member. The ordering needs the bytes
/// only to COMPARE them, and a comparison needs no allocation. One allocation for the buffer of
/// borrows; the caller clones only the members it returns, so the clone count becomes the answer
/// size and not the set size. The time is unchanged at O(n log n); the figure that moves is
/// allocations.
///
/// `sort_unstable_by`: the map is keyed by member, so `(score, member)` is a total order with no
/// ties for a stable sort to break, and a stable sort allocates a scratch buffer. The order is
/// score-then-member, as before.
fn zset_ordered_refs<'a>(shard: &'a ShardState, key: &str) -> Vec<(&'a [u8], u64)> {
    let mut ordered: Vec<(&'a [u8], u64)> = shard
        .bucket_index
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .map(|(member, (biased, _))| (member.as_slice(), *biased))
                .collect()
        })
        .unwrap_or_default();
    ordered.sort_unstable_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));
    ordered
}

/// The whole token-bucket model in one pure function, so its arithmetic is testable with
/// explicit clocks: refill by elapsed time at `refill_per_sec` (capped at capacity, and a
/// clock that moved backwards refills nothing), then take if it fits. Answers
/// (allowed, remaining AFTER the outcome, retry-after ms, state to store). An absent bucket
/// starts full.
pub(super) fn bucket_take(
    current: Option<(f64, u64)>,
    now_ms: u64,
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
) -> (bool, f64, u64, (f64, u64)) {
    let tokens = tokens.max(0.0);
    let capacity = capacity.max(0.0);
    let refill_per_sec = refill_per_sec.max(0.0);
    let filled = match current {
        None => capacity,
        Some((had, last_ms)) => {
            let elapsed_ms = now_ms.saturating_sub(last_ms);
            (had + refill_per_sec * (elapsed_ms as f64 / 1000.0)).min(capacity)
        }
    };
    if tokens <= filled {
        let remaining = filled - tokens;
        (true, remaining, 0, (remaining, now_ms))
    } else {
        let shortfall = tokens - filled;
        let retry_after_ms = if refill_per_sec > 0.0 && tokens <= capacity {
            (shortfall / refill_per_sec * 1000.0).ceil() as u64
        } else {
            // A take larger than capacity can never succeed; answer the sentinel.
            u64::MAX
        };
        (false, filled, retry_after_ms, (filled, now_ms))
    }
}

fn bucket_answer(allowed: bool, remaining: f64, retry_after_ms: u64) -> Vec<Vec<u8>> {
    vec![
        if allowed {
            b"1".to_vec()
        } else {
            b"0".to_vec()
        },
        format!("{remaining:.3}").into_bytes(),
        retry_after_ms.to_string().into_bytes(),
    ]
}

/// The element name a list entry is filed under: its sequence, offset so that the order of the names
/// is the order of the list, as sixteen lowercase hex digits.
///
/// This is `format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64))` written into a caller's
/// buffer, so a read that names one entry per element of a range does not allocate a `String` for
/// each. A test compares it with the formatted spelling.
pub(crate) fn list_element_name(seq: i64, buf: &mut [u8; 16]) -> &str {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut value = (seq as u64).wrapping_sub(i64::MIN as u64);
    for slot in buf.iter_mut().rev() {
        *slot = DIGITS[(value & 0xf) as usize];
        value >>= 4;
    }
    std::str::from_utf8(buf).expect("hex digits are ascii")
}
