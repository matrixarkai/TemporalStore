// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStore, ElementEntry};
use crate::types::ShardId;
use matrixcache::MultiLayerCache;

use super::hashing::BlockIdentity;
use super::read_block_bytes;
use super::state::{
    object_component_lookup_key, object_block_lookup_key, ShardState, BucketLayoutState,
};

/// One bucket's resident runtime state, for the bucket-store runtime report.
///
/// NO `last_dump_sequence`. This row used to carry one, read straight off `BucketNode`, and
/// `runtime_report` is handed a shard and no manifest -- so the only per-bucket figure it could
/// have published was the node's, and the node no longer holds one. The dumped-log watermark is a
/// property of the newest dump manifest, not of a bucket, and the report that has a manifest in
/// hand (`storage_physical_index_report`) is where it is published. Nothing reads this field: the
/// whole of `runtime_report` has one caller, a test helper.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketRuntimeState {
    #[serde(rename = "routing_slot")]
    pub routing_bucket: u32,
    pub layout: String,
    pub object_ids: Vec<u64>,
    pub block_ref_count: usize,
    pub dirty: bool,
    pub deleted: bool,
    pub meta_loaded: bool,
    pub loading: bool,
    pub in_memory: bool,
    pub ttl_ms: Option<u64>,
    pub dirty_generation: u64,
    pub deleted_block_ref_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct BucketStoreRuntimeReport {
    pub bucket_store_runtime_module: bool,
    pub bucket_index_authority: bool,
    pub bucket_count: usize,
    pub block_ref_count: usize,
    pub dirty_bucket_count: usize,
    pub deleted_bucket_count: usize,
    pub empty_buckets: usize,
    pub single_object_buckets: usize,
    pub single_block_object_buckets: usize,
    pub multi_block_object_buckets: usize,
    pub multi_object_buckets: usize,
    pub deleted_block_ref_count: usize,
    pub loading_bucket_count: usize,
    pub in_memory_bucket_count: usize,
    pub ttl_bucket_count: usize,
    pub max_dirty_generation: u64,
    #[serde(rename = "slots")]
    pub buckets: Vec<BucketRuntimeState>,
}

#[allow(dead_code)]
pub(super) fn runtime_report(shard: &ShardState) -> BucketStoreRuntimeReport {
    let mut report = BucketStoreRuntimeReport {
        bucket_store_runtime_module: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        bucket_count: shard.bucket_index.bucket_map.len(),
        block_ref_count: 0,
        dirty_bucket_count: 0,
        deleted_bucket_count: 0,
        empty_buckets: 0,
        single_object_buckets: 0,
        single_block_object_buckets: 0,
        multi_block_object_buckets: 0,
        multi_object_buckets: 0,
        deleted_block_ref_count: 0,
        loading_bucket_count: 0,
        in_memory_bucket_count: 0,
        ttl_bucket_count: 0,
        max_dirty_generation: 0,
        buckets: Vec::new(),
    };

    for bucket in shard.bucket_index.bucket_map.values() {
        report.block_ref_count = report.block_ref_count.saturating_add(bucket.block_index.len());
        if bucket.dirty() {
            report.dirty_bucket_count = report.dirty_bucket_count.saturating_add(1);
        }
        if bucket.deleted() {
            report.deleted_bucket_count = report.deleted_bucket_count.saturating_add(1);
        }
        if bucket.loading() {
            report.loading_bucket_count = report.loading_bucket_count.saturating_add(1);
        }
        if bucket.in_memory() {
            report.in_memory_bucket_count = report.in_memory_bucket_count.saturating_add(1);
        }
        if bucket.ttl_ms.is_some() {
            report.ttl_bucket_count = report.ttl_bucket_count.saturating_add(1);
        }
        report.max_dirty_generation = report.max_dirty_generation.max(bucket.dirty_generation);
        let deleted_block_ref_count = bucket.block_index.values().filter(|page| page.deleted).count();
        report.deleted_block_ref_count = report
            .deleted_block_ref_count
            .saturating_add(deleted_block_ref_count);
        match bucket.layout {
            BucketLayoutState::Empty => report.empty_buckets = report.empty_buckets.saturating_add(1),
            BucketLayoutState::SingleObject => {
                report.single_object_buckets = report.single_object_buckets.saturating_add(1)
            }
            BucketLayoutState::SingleBlockObject => {
                report.single_block_object_buckets = report.single_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiBlockObject => {
                report.multi_block_object_buckets = report.multi_block_object_buckets.saturating_add(1)
            }
            BucketLayoutState::MultiObject => {
                report.multi_object_buckets = report.multi_object_buckets.saturating_add(1)
            }
        }
        report.buckets.push(BucketRuntimeState {
            routing_bucket: bucket.routing_bucket,
            layout: bucket_layout_name(bucket.layout).to_string(),
            object_ids: bucket.object_index.iter().copied().collect(),
            block_ref_count: bucket.block_index.len(),
            dirty: bucket.dirty(),
            deleted: bucket.deleted(),
            meta_loaded: bucket.meta_loaded(),
            loading: bucket.loading(),
            in_memory: bucket.in_memory(),
            ttl_ms: bucket.ttl_ms.ms(),
            dirty_generation: bucket.dirty_generation,
            deleted_block_ref_count,
        });
    }

    report
}

#[allow(dead_code)]
fn bucket_layout_name(layout: BucketLayoutState) -> &'static str {
    match layout {
        BucketLayoutState::Empty => "empty",
        BucketLayoutState::SingleObject => "single_object",
        BucketLayoutState::SingleBlockObject => "single_page_object",
        BucketLayoutState::MultiBlockObject => "multi_page_object",
        BucketLayoutState::MultiObject => "multi_object",
    }
}

// THE LOOKUP IS THE ONLY DOOR ONTO ONE OBJECT'S PAGES, and these readers go through it.
//
// Each of them answered a question about ONE named object by walking every page of every bucket
// and comparing the entry's own copy of the identity. That comparison is the thing that stops
// being expressible when the entry carries no identity, and it was never the cheap way to ask: the
// object lookup maps (kind, object key) straight to the `(routing_bucket, handle)` of each of that
// object's pages, which is the same answer without the walk.
//
// WHAT THE WALK WAS GUARDING, AND WHY IT IS NOT NEEDED. Each site reached the walk only when the
// lookup held NOTHING -- `object_block_lookup.is_empty()` -- which is an index with pages and no
// lookup. That state was reachable between a decode and the rebuild that filled the lookup, and
// the decode fills it now, so the arm is unreachable in production rather than merely rare. The
// write path files every page it adds; `rebuild_bucket_block_ownership` files what it rebuilds.
//
// AND THE RE-COMPARISON AFTER THE WALK IS A CHECK, NOT A SOURCE. A page reached through its own
// object's ref list has already been matched on identity by the list that produced it; comparing
// the entry's copy again asks whether the lookup is right, not which object the page belongs to.
// The kind is still compared, because `block_refs_for` takes the kind as a key and a ref that
// survives a handle collision could name a page of another kind.

/// Where a read resolves an object's page from.
///
/// THIS IS THE READ PATH, not a reporting path: `read_bucket_index_value` is what
/// `Command::StringGet` calls once the response cache misses. It answers from the bucket index,
/// which is why releasing a bucket's page entries is a question about reads at all -- the fast
/// path in `execute_read_only_fast_path` goes straight to `shard.strings` and never noticed, so a
/// released bucket served every warm read and returned None for every cold one.
///
/// Every "cannot answer" now falls through to the released-bucket lookup rather than returning
/// None, which is what makes a released bucket serve reads identically to a resident one.
pub(super) fn bucket_index_block_address(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<ElementEntry> {
    if let Some(block_refs) = shard
        .bucket_index
        .block_refs_for(model_id, object_key, component)
    {
        for block_ref in block_refs {
            let Some(bucket) = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket) else {
                continue;
            };
            let Some(page) = bucket.block_index.get(&block_ref.block_ref_key) else {
                continue;
            };
            if !page.deleted
                && page.model_id.as_str() == model_id
                // THE IDENTITY IS NOT RE-COMPARED -- see the note above this function on why a
                // page reached through its own object's ref list has already been matched on it.
                //
                // THE ENTRY HAS NO NAME TO MATCH either, so the surviving question is whether the
                // caller asked for a named element. This read
                // `page.component.as_deref() == component` against entries whose component was
                // already `None` for every kind, so the answer does not move: a point lookup BY
                // component found nothing here before this change and finds nothing now -- hash's
                // four such readers were moved onto `shard.hashes` for exactly that reason -- and
                // a lookup with no component still resolves the kinds that converge on the object
                // key.
                && component.is_none()
            {
                return Some(page.address.clone());
            }
        }
        return super::storage_bucket_internals::released_bucket_block_address(
            shard, model_id, object_key, component,
        );
    }

    // AND NO SECOND ARM. A whole-shard walk comparing every entry's identity stood here for the
    // case where the lookup held nothing; the decode fills the lookup now, so an object with no
    // entry in it has no resident page and the released-bucket door is the whole of the answer --
    // which is what the arm above already falls through to when the lookup DOES name the object
    // and no page answers.
    super::storage_bucket_internals::released_bucket_block_address(
        shard, model_id, object_key, component,
    )
}

/// Every page this object resolves to, LIVE ENTRIES AND TOMBSTONED ALIKE.
///
/// # WHY THE EXISTING WALK CANNOT BE USED
///
/// [`bucket_index_component_block_addresses`] filters `!page.deleted`, which is right for every
/// reader that answers FROM an entry: a tombstone entry names no live element and
/// `no_index_reader_answers_from_a_tombstone_entry` drives all of them.
///
/// A FOLD IS NOT SUCH A READER. `container_membership::derive_membership` needs the tombstone
/// pages precisely BECAUSE they state removals -- it folds by `append_position` and lets the later
/// page win, so withholding the tombstones would hand it only the pages that say "present" and it
/// would conclude exactly the resurrection the fold exists to prevent.
///
/// So this is deliberately the one enumerator that does not filter the flag, and it is named for
/// that rather than reading like a variant someone can reach for by accident.
pub(super) fn bucket_index_all_block_addresses_with_tombstones(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<ElementEntry> {
    // TWO SOURCES, BECAUSE A TOMBSTONE IS NOT IN THE OBJECT LOOKUP.
    //
    // `insert_object_block_lookup` returns early on a deleted page, so the lookup names exactly
    // this object's LIVE pages. The tombstoned ones are named by the rows a removal files beside
    // the entries in the bucket it removed them from -- keyed by (object, kind, element) and
    // valued by the page triple -- which is the one structure that still states which object a
    // deleted page belonged to.
    //
    // THE ORDER THIS RETURNS IS NOT THE ORDER THE WALK RETURNED, and that is safe for a stated
    // reason rather than by luck: the one caller is `container_membership::derive_membership`,
    // which sorts by `append_position` and dedups on it before reading anything. Its own note
    // says so -- "ORDER FIRST, THEN READ".
    let mut addresses = Vec::new();
    if let Some(block_refs) = shard.bucket_index.object_block_refs(model_id, object_key) {
        for block_ref in block_refs.iter() {
            let Some(page) = shard
                .bucket_index
                .bucket_map
                .get(&block_ref.routing_bucket)
                .and_then(|bucket| bucket.block_index.get(&block_ref.block_ref_key))
            else {
                continue;
            };
            if page.model_id.as_str() == model_id {
                addresses.push(page.address.clone());
            }
        }
    }
    for bucket in shard.bucket_index.bucket_map.values() {
        for row in bucket.tombstone_elements.iter() {
            if row.model_id.as_str() != model_id || &*row.object_key != object_key {
                continue;
            }
            // THE ROW NAMES A PAGE BY ITS TRIPLE, so the entry is found by address and not by
            // identity. Bounded by the removals this bucket has seen, against the whole-shard page
            // walk it replaces.
            if let Some(page) = bucket.block_index.values().find(|page| {
                page.deleted
                    && crate::engine::state::tombstone_page_key(&page.address) == row.page
            }) {
                addresses.push(page.address.clone());
            }
        }
    }
    addresses
}

pub(super) fn bucket_index_component_block_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<Arc<str>>, ElementEntry)> {
    if let Some(object_refs) = shard.bucket_index.object_block_refs(model_id, object_key) {
        let mut refs = object_refs
            .iter()
            .filter_map(|block_ref| {
                let bucket = shard.bucket_index.bucket_map.get(&block_ref.routing_bucket)?;
                let page = bucket.block_index.get(&block_ref.block_ref_key)?;
                if !page.deleted && page.model_id.as_str() == model_id {
                    // `None` IN THE FIRST SLOT, AND THAT SLOT IS NOW DEAD ON BOTH ARMS -- said
                    // here rather than left to be rediscovered. `released_component_block_addresses`
                    // already hardcodes `(None, address)`, and this arm's `page.component` was
                    // already `None` for every kind before the field was removed, so the pair's
                    // first element can no longer distinguish anything.
                    //
                    // THE PAIR IS KEPT ANYWAY, DELIBERATELY. Its shape is pinned by a signature
                    // tripwire -- `element_ordinal_reuse` asserts this function's exact declaration
                    // text, including `-> Vec<(Option<Arc<str>>, ElementEntry)>`, so that a reader
                    // gaining a component cannot do it quietly -- and roughly twenty call sites
                    // destructure the pair. Collapsing it is a separate change with that tripwire
                    // to restate, not a side effect of a width step.
                    Some((None, page.address.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        if !refs.is_empty() {
            refs.sort_by(|left, right| left.0.cmp(&right.0));
            return refs;
        }
        return released_component_block_addresses(shard, model_id, object_key);
    }

    // AND NO SECOND ARM, for the reason given above this function: the walk that stood here
    // compared every entry's identity, and it ran only when the lookup held nothing.
    //
    // THE SORT WENT WITH IT AND WAS ALREADY A NO-OP. It sorted by the pair's first element, which
    // is the same `None` for every row. The ORDERING CONTRACT callers were given
    // (`hash_field_map`'s note and `element_ordinal_reuse` both cite it) is kept by the arm above,
    // which still sorts.
    released_component_block_addresses(shard, model_id, object_key)
}

/// The whole-object form of the released-bucket lookup.
///
/// A released kind is component-less by construction (see `released_model_kind_is_addressable`),
/// so "every component of this object" is at most one page and the point lookup answers it. A
/// kind with real components could not be served this way, which is why none is releasable.
fn released_component_block_addresses(
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
) -> Vec<(Option<Arc<str>>, ElementEntry)> {
    super::storage_bucket_internals::released_bucket_block_address(shard, model_id, object_key, None)
        .map(|address| vec![(None, address)])
        .unwrap_or_default()
}

pub(super) fn read_bucket_index_value(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    model_id: &str,
    object_key: &str,
    component: Option<&str>,
) -> Option<Vec<u8>> {
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    bucket_index_block_address(shard, model_id, object_key, component).and_then(|address| {
        read_block_bytes(
            cache,
            block_store,
            shard_id,
            &address,
            // THE SAME THREE TERMS `bucket_index_block_address` was just asked, two lines up.
            // Anything else here would resolve a record and then look inside it for a page that is
            // not the one that lookup named.
            BlockIdentity::of(shard_id, model_id, object_key, component),
            Some(crate::engine::hashing::block_routing_bucket(
                object_key,
                start_routing_bucket,
                end_routing_bucket,
            )),
        )
    })
}
