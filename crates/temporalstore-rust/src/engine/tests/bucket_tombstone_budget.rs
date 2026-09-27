// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE THIRD STRUCTURE ON THE OBJECT SIDE OF A BUCKET, AND WHY IT CANNOT BECOME THE SECOND.
//!
//! `BucketNode` carries three containers: `object_index` (16 B), `deleted_object_index` (8 B) and
//! `block_index` (24 B). A design that encoded deletion IN THE PAGE ENTRY would carry only the
//! first and the last, and `deleted_object_index` sits INSIDE the eight-aligned group, so removing
//! it moves the node BY ITSELF -- 80 B of eight-aligned field becomes 72, and the node goes
//! 88 -> 80. That is the cheapest arithmetic on this struct: one field, one step, no side
//! structure and no relocation. Which is exactly why it is worth establishing whether the
//! structure is needed at all, rather than assuming the width already settled it. #1961 took this
//! field from 16 B to 8 by making absence cost a pointer; that priced its WIDTH and said nothing
//! about whether the set has to exist.
//!
//! IT HAS TO EXIST, AND THE REASON IS A LIFETIME RATHER THAN A COUNT.
//!
//! A per-page `deleted` bit cannot carry this, and NOT because it is missing -- `BlockIndex.deleted`
//! is already a field of the page entry, so the per-page half of deletion is already recorded. It
//! cannot carry it because BY THE TIME THE DELETION MATTERS THERE IS NO PAGE LEFT TO CARRY IT. The
//! delete path retires the pages and keeps the id:
//!
//! ```text
//!   engine.rs, delete_object_key_from_bucket_index:
//!     bucket.block_index.retain(.., |_, page| { .. false })   <- the pages are REMOVED
//!     bucket.object_index.extend(deleted_object_ids)          <- the id STAYS
//!     bucket.deleted_object_index.extend(deleted_object_ids)  <- the tombstone is written
//! ```
//!
//! So a deleted object is, by construction, an id in `object_index` with NO entry in `block_index`.
//! `storage_bucket_internals`'s own note on `settle_released_bucket_object_delete` states the
//! invariant from the other side: "The resident path keeps the id and records it in
//! `deleted_object_index` BECAUSE THE ID STAYS; here the id goes, and a tombstone for an absent id
//! is an entry nothing would ever read."
//!
//! AND THE READER IS NAMED. `object_manager::runtime_report` walks `bucket.object_index` and asks
//! `bucket.deleted() || bucket.deleted_object_index.contains(object_id)`. For an object whose pages
//! are gone, `page.deleted` is not reachable -- there is no page -- and `bucket.deleted()` is set
//! from `bucket.block_index.is_empty()`, so it is FALSE for any bucket still holding another
//! object's pages. In that state the tombstone set is the ONLY thing that can answer, and the
//! figure it answers is published: `delete_marker_object_count`, which
//! `object_manager_runtime_report` fills from it and `native_persistence_workflow` and
//! `recovery_sweep_compact` both reach.
//!
//! A ZERO-LENGTH SENTINEL IS UNAVAILABLE FOR THE SAME REASON, which is worth stating because it is
//! the obvious alternative and the obvious refutation of it is the wrong one. Whether a zero-length
//! block is legitimate in this format does not arise: the sentinel needs a page to sit in, and the
//! delete has removed the page from `block_index` entirely. The question is not whether the
//! encoding has room for a marker, it is whether there is anything left to mark.
//!
//! THE POPULATION, MEASURED HERE, AND THE NUMBER THAT DECIDES IT IS NOT THE BIG ONE. At 4,000
//! records on `0..1023` with a quarter of the keys deleted, 704 of 1,012 buckets (69.57%) carry a
//! tombstoned id that no page carries, over 1,000 such ids. But only 19 of those buckets hold the
//! orphaned id in `object_index`, and those nineteen are the whole of the load-bearing population:
//! `delete_marker_object_count` reads 19, and removing ONE tombstone takes it to 18.
//!
//! A SECOND READING, REPORTED BECAUSE IT IS NOT WHAT THE FIELD'S OWN NOTE IMPLIES. 981 of those
//! 1,000 tombstoned ids are reachable by NEITHER reader: the id is not in `object_index`, so the
//! walk that consults the tombstone never reaches it, and no page carries it either. On this
//! corpus 98.1% of the entries in this field are never read by anything. That is NOT a second
//! argument for removing the structure -- the nineteen that ARE read cannot be answered any other
//! way, which is what item 2 drives -- but it says the field is carrying far more than it is asked
//! about, and the shape of that excess (a rebuild re-attaches the tombstone set while recomputing
//! `object_index` from live pages only, so the two drift apart) is worth its own look. This module
//! reports the figure and changes nothing.
//!
//! WHAT A PRECONDITION PROXY COST, recorded because it is the failure that reads as a refutation.
//! A first version of item 2 required the chosen bucket to still hold OTHER pages, as a proxy for
//! "whole-bucket `deleted()` is not what answers". It found NOTHING, and a bare `None` from a
//! search is indistinguishable from the state not existing. Counting each clause separately said
//! why in one line: the buckets that hold the orphaned id in `object_index` are exactly the ones
//! whose page list is now EMPTY, so the proxy excluded the entire population it was meant to
//! select. The empty-page case is the STRONGER one -- there is no page at all for a per-page bit to
//! sit in -- and `deleted()` is asserted false on it directly, which is the clause that was
//! actually wanted. Every clause is counted and printed for that reason.
//!
//! The other half of the field's economics is already measured elsewhere and is not restated here:
//! `what_the_object_side_of_the_bucket_node_costs` reports that 97.68% of buckets carry no
//! tombstone at all, which is what makes the eight-byte nullable-pointer shape the right WIDTH and
//! is not an argument that the set is unused.
//!
//! WHAT IS HERE:
//!
//!   1. `what_removing_the_tombstone_index_would_make_the_node` -- the arithmetic, per field, with
//!      every width read FROM THE FIELD via `field_width` and every offset from `offset_of!`, the
//!      live mirror as the control, and the reconstruction asserted rather than the total. 88 -> 80.
//!   2. `a_tombstone_outlives_every_page_that_could_have_carried_it` -- the refutation, DRIVEN. It
//!      finds a bucket in the state above, ENFORCES that the state is the one described (id held,
//!      no page carrying it, bucket not itself deleted), then PERTURBS the tombstone away and
//!      asserts the published count falls by exactly one. The control is the unperturbed report on
//!      the same shard, so a count that was already wrong cannot read as the edit's doing.
//!
//! WHAT THIS MODULE DOES NOT CLAIM. It proposes no change. `deleted_object_index` stays, at eight
//! bytes, on every routing bucket, and the eight bytes the node would have saved are not available.

#![allow(clippy::all)]
use super::*;

use crate::engine::state::{
    BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl, DeletedObjectIndex,
    ObjectIndex,
};
use std::mem::{align_of, offset_of, size_of};

const TOMB_SHARD: ShardId = 1;

/// The documented production routing range, `TS_SHARD_END_ROUTING_BUCKET=1023`.
const CONFIGURED_END_BUCKET: u32 = 1023;

// -----------------------------------------------------------------------------------------------
// 1. THE ARITHMETIC: WHAT THE FIELD IS WORTH, RECONSTRUCTED RATHER THAN QUOTED.
// -----------------------------------------------------------------------------------------------

/// The node as declared. THE CONTROL: if this is not `size_of::<BucketNode>()` then every row
/// below describes a structure this engine does not have.
#[allow(dead_code)]
struct TombMirrorLive {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: BlockIndexMap,
}

/// The tombstone set gone. The bytes LEAVE the structure rather than moving to the tail, which is
/// why one field crosses the step where a narrowing of the same width would not.
#[allow(dead_code)]
struct TombMirrorNoTombstones {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The same eight bytes NARROWED instead of removed, as the control on the claim that removal is
/// what crosses. `DeletedObjectIndex` is already one nullable pointer, so there is nothing to
/// narrow it to that is smaller than a `u32` handle -- and a `u32` here lands in the tail, takes it
/// from 6 to 10, and rounds back to 16. The group gives up a word and hands it straight back.
#[allow(dead_code)]
struct TombMirrorNarrowedToHandle {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index_handle: u32,
    block_index: BlockIndexMap,
}

/// WHAT REMOVING THE TOMBSTONE SET WOULD MAKE THE NODE, and the control that says removing is not
/// the same shape of change as narrowing.
///
/// Every width is read FROM THE FIELD through `field_width`, which infers `T` from the field it is
/// handed: a hand-written table naming the types at the call site still compiles when a field stops
/// being what the table says, and rustc says nothing. Every offset comes from `offset_of!`, because
/// `repr(Rust)` orders by alignment and not by declaration, so the declaration order below is a
/// convention and not a layout.
///
/// THE RECONSTRUCTION IS ASSERTED, NOT THE TOTAL: `eight_aligned + round_up(tail) == size_of` has
/// to hold on every row, so a row that happens to land on the right number for the wrong reason
/// fails.
#[test]
fn what_removing_the_tombstone_index_would_make_the_node() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<TombMirrorLive>(),
        "the mirror of the live declaration is {} B against the declaration's {}; the mirrors have \
         drifted and every price below is fiction",
        size_of::<TombMirrorLive>(),
        size_of::<BucketNode>()
    );

    let align = align_of::<BucketNode>();
    assert_eq!(
        8, align,
        "BucketNode's alignment is {align} and the arithmetic below assumes 8"
    );

    fn field_width<T>(_field: &T) -> usize {
        size_of::<T>()
    }
    let sample = BucketNode::default();
    let fields = [
        (
            "routing_bucket",
            field_width(&sample.routing_bucket),
            offset_of!(BucketNode, routing_bucket),
        ),
        ("layout", field_width(&sample.layout), offset_of!(BucketNode, layout)),
        ("flags", field_width(&sample.flags), offset_of!(BucketNode, flags)),
        ("ttl_ms", field_width(&sample.ttl_ms), offset_of!(BucketNode, ttl_ms)),
        (
            "dirty_generation",
            field_width(&sample.dirty_generation),
            offset_of!(BucketNode, dirty_generation),
        ),
        (
            "first_dirty_wal_sequence",
            field_width(&sample.first_dirty_wal_sequence),
            offset_of!(BucketNode, first_dirty_wal_sequence),
        ),
        (
            "first_dirty_index_log_sequence",
            field_width(&sample.first_dirty_index_log_sequence),
            offset_of!(BucketNode, first_dirty_index_log_sequence),
        ),
        (
            "object_index",
            field_width(&sample.object_index),
            offset_of!(BucketNode, object_index),
        ),
        (
            "deleted_object_index",
            field_width(&sample.deleted_object_index),
            offset_of!(BucketNode, deleted_object_index),
        ),
        (
            "block_index",
            field_width(&sample.block_index),
            offset_of!(BucketNode, block_index),
        ),
    ];

    println!("\n=== BucketNode, field by field at its REAL offset, each width read from the field ===");
    for (name, width, offset) in &fields {
        println!("  {name:<32} {width:>3} B at offset {offset:>3}");
    }

    // The two groups, DERIVED from the offsets rather than stated. A field is in the tail when it
    // is narrower than the alignment and sits with the other sub-word fields; everything of width
    // 8 or more is in the eight-aligned group. Taking it from the widths means a field that
    // changes width moves itself between the groups here.
    let eight_aligned: usize = fields
        .iter()
        .filter(|(_, width, _)| *width >= align)
        .map(|(_, width, _)| *width)
        .sum();
    let tail: usize = fields
        .iter()
        .filter(|(_, width, _)| *width < align)
        .map(|(_, width, _)| *width)
        .sum();
    let field_total: usize = fields.iter().map(|(_, width, _)| *width).sum();

    println!(
        "\n  eight-aligned group {eight_aligned} B, tail {tail} B -> rounds to {}, field total \
         {field_total} B in {} B of node",
        tail.div_ceil(align) * align,
        size_of::<BucketNode>()
    );

    assert_eq!(
        eight_aligned + tail.div_ceil(align) * align,
        size_of::<BucketNode>(),
        "the live node must reconstruct from {eight_aligned} B of eight-aligned field and a \
         {tail} B tail; it does not, so the model every row below uses is wrong"
    );

    let tombstone_width = field_width(&sample.deleted_object_index);
    assert_eq!(
        8, tombstone_width,
        "the tombstone set is {tombstone_width} B, not the 8 this arithmetic is about"
    );
    assert!(
        tombstone_width >= align,
        "the tombstone set is narrower than the alignment, so it is in the TAIL and removing it \
         would not cross a step; the whole premise of this row has changed"
    );

    println!("\n=== removing the tombstone set against narrowing it to a handle ===");
    println!(
        "  {:<34} {:>14} {:>6} {:>8} {:>6} {:>8}",
        "shape", "eight-aligned", "tail", "rounded", "size", "vs live"
    );
    let rows = [
        ("live", eight_aligned, tail, size_of::<TombMirrorLive>()),
        (
            "tombstone set REMOVED",
            eight_aligned - tombstone_width,
            tail,
            size_of::<TombMirrorNoTombstones>(),
        ),
        (
            "tombstone set narrowed to a u32 handle",
            eight_aligned - tombstone_width,
            tail + 4,
            size_of::<TombMirrorNarrowedToHandle>(),
        ),
    ];
    for (label, group, row_tail, size) in rows {
        let rounded = row_tail.div_ceil(align) * align;
        println!(
            "  {label:<34} {group:>14} {row_tail:>6} {rounded:>8} {size:>6} {:>+8}",
            size as i64 - size_of::<BucketNode>() as i64
        );
        assert_eq!(
            group + rounded,
            size,
            "the {label} row is {size} B but reconstructs to {}; the row lands on its number for a \
             reason the model does not describe",
            group + rounded
        );
    }

    // THE FINDING OF THE ARITHMETIC, asserted rather than printed: removal crosses a step and the
    // same eight bytes narrowed do not.
    assert_eq!(
        size_of::<BucketNode>() - 8,
        size_of::<TombMirrorNoTombstones>(),
        "removing the tombstone set takes the node from {} to {}; it was supposed to be worth a \
         whole eight bytes, which is what made it the cheapest candidate on this struct",
        size_of::<BucketNode>(),
        size_of::<TombMirrorNoTombstones>()
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<TombMirrorNarrowedToHandle>(),
        "narrowing the tombstone set to a handle gives {} against the live {}; the control on \
         'removal is what crosses' has stopped holding",
        size_of::<TombMirrorNarrowedToHandle>(),
        size_of::<BucketNode>()
    );

    println!(
        "\n=== the arithmetic says the field is worth a real eight bytes: {} -> {}. Item 2 is why \
         it is not available. ===",
        size_of::<BucketNode>(),
        size_of::<TombMirrorNoTombstones>()
    );
}

// -----------------------------------------------------------------------------------------------
// 2. THE REFUTATION, DRIVEN.
// -----------------------------------------------------------------------------------------------

fn tomb_engine(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        1 << 20,
        dir.join("tomb-cache"),
        dir.join("tomb-pages"),
        dir.join("tomb-indexes"),
    )
}

fn tomb_load(engine: &TemporalEngine, end_routing_bucket: u32) {
    engine.load_shard_with(LoadShardRequest {
        shard_id: TOMB_SHARD,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        table_name: String::new(),
    });
}

/// A TOMBSTONE OUTLIVES EVERY PAGE THAT COULD HAVE CARRIED IT, so a per-page bit cannot replace it
/// and the node's eight bytes are not available.
///
/// The state is produced rather than constructed: strings are written and a quarter of them
/// deleted, which is the only path that writes `deleted_object_index` at all. The delete retires
/// the pages and keeps the id, so the bucket ends up holding an id in `object_index` with nothing
/// in `block_index` carrying it.
///
/// EVERY PRECONDITION IS ENFORCED, NOT PRINTED, because a fixture that failed to reach the state
/// and one that reached it produce the same output otherwise:
///
///   * the shard holds buckets at all (denominator);
///   * at least one bucket holds a tombstoned id with NO page carrying it -- the "outlives" state;
///   * the chosen bucket still holds OTHER pages, so `bucket.deleted()` is false and cannot be
///     what answers; and
///   * the chosen id is in `object_index`, so the walk that reads the tombstone actually reaches it.
///
/// THEN IT PERTURBS. The tombstone for that one id is removed and the published count must fall by
/// exactly one. The control is the same report on the same shard BEFORE the edit: without it a
/// count that was already wrong for an unrelated reason would read as this edit's doing.
#[test]
#[ignore = "seeds 4,000 records and deletes a quarter of them; run by name"]
fn a_tombstone_outlives_every_page_that_could_have_carried_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = tomb_engine(dir.path());
    tomb_load(&engine, CONFIGURED_END_BUCKET);

    const RECORDS: usize = 4_000;
    let keys: Vec<String> = (0..RECORDS).map(|index| format!("tomb-{index:06}")).collect();
    for chunk in keys.chunks(512) {
        let commands = chunk
            .iter()
            .map(|key| Command::StringSet {
                key: key.clone(),
                value: vec![b't'; 64],
            })
            .collect::<Vec<_>>();
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: TOMB_SHARD,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }

    // A quarter deleted, through the single-command delete path, which is the one that retires the
    // pages and keeps the id.
    let mut deleted_keys = 0usize;
    for key in keys.iter().step_by(4) {
        let response = engine.execute(ExecuteRequest {
            shard_id: TOMB_SHARD,
            command: Command::StringDelete { key: key.clone() },
        });
        assert!(
            response.status.ok,
            "delete {key} failed: {:?}",
            response.status
        );
        deleted_keys += 1;
    }
    assert!(
        deleted_keys > 0,
        "denominator: no key was deleted, so nothing would have written a tombstone at all"
    );

    // --- Find the state, and count how common it is while we are here. ---
    //
    // EVERY SUB-CONDITION IS COUNTED SEPARATELY. A search that returns nothing and a search whose
    // state does not exist produce the same `None`, so the fixture has to say WHICH clause failed
    // rather than leave it to be guessed.
    let (
        target,
        buckets_total,
        buckets_with_tombstone,
        buckets_outliving,
        ids_outliving,
        buckets_outliving_with_pages,
        buckets_outliving_not_deleted,
        buckets_outliving_in_object_index,
        ids_unreachable,
    ) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&TOMB_SHARD).expect("shard is loaded");
        let mut target: Option<(u32, u64)> = None;
        let mut buckets_with_tombstone = 0usize;
        let mut buckets_outliving = 0usize;
        let mut ids_outliving = 0usize;
        let mut with_pages = 0usize;
        let mut not_deleted = 0usize;
        let mut in_object_index = 0usize;
        let mut ids_unreachable = 0usize;
        for (routing_bucket, bucket) in shard.bucket_index.bucket_map.iter() {
            if bucket.deleted_object_index.is_empty() {
                continue;
            }
            buckets_with_tombstone += 1;
            let paged: std::collections::BTreeSet<u64> = bucket
                .block_index
                .values()
                .map(|page| page.object_id())
                .collect();
            let orphaned: Vec<u64> = bucket
                .deleted_object_index
                .iter()
                .copied()
                .filter(|id| !paged.contains(id))
                .collect();
            if orphaned.is_empty() {
                continue;
            }
            buckets_outliving += 1;
            ids_outliving += orphaned.len();
            if !bucket.block_index.is_empty() {
                with_pages += 1;
            }
            if !bucket.deleted() {
                not_deleted += 1;
            }
            let held = orphaned
                .iter()
                .copied()
                .find(|id| bucket.object_index.contains(id));
            if held.is_some() {
                in_object_index += 1;
            }
            ids_unreachable += orphaned
                .iter()
                .filter(|id| !bucket.object_index.contains(id))
                .count();
            // THE DISCRIMINATING BUCKET. What has to be true is that `bucket.deleted()` cannot be
            // what answers -- so that is asserted DIRECTLY rather than inferred from the bucket
            // still holding other pages. A first version of this required
            // `!block_index.is_empty()` as a proxy for it and found nothing: the buckets that hold
            // the orphaned id in `object_index` are exactly the ones whose page list is now EMPTY,
            // so the proxy excluded the entire population it was meant to select. The empty-page
            // case is the stronger one anyway -- there is no page at all for a per-page bit to sit
            // in -- and `deleted()` is false on it regardless, which is the clause that matters.
            if target.is_none() && !bucket.deleted() {
                if let Some(id) = held {
                    target = Some((*routing_bucket, id));
                }
            }
        }
        (
            target,
            shard.bucket_index.bucket_map.len(),
            buckets_with_tombstone,
            buckets_outliving,
            ids_outliving,
            with_pages,
            not_deleted,
            in_object_index,
            ids_unreachable,
        )
    };

    println!("\n=== the population of the state this refutation turns on ===");
    println!("  buckets in the map                                         {buckets_total}");
    println!("  buckets carrying any tombstone                              {buckets_with_tombstone}");
    println!(
        "  buckets carrying a tombstoned id with NO page to carry it   {buckets_outliving} \
         ({:.2}% of buckets)",
        100.0 * buckets_outliving as f64 / buckets_total.max(1) as f64
    );
    println!("  such ids in total                                          {ids_outliving}");
    println!("\n  of those {buckets_outliving} buckets, how many satisfy each further clause:");
    println!("    still hold OTHER pages (block_index non-empty)             {buckets_outliving_with_pages}");
    println!("    are NOT themselves marked deleted                          {buckets_outliving_not_deleted}");
    println!("    hold the orphaned id in object_index                        {buckets_outliving_in_object_index}");
    println!(
        "\n  AND A SECOND READING, reported because it is not what the field's own note implies:\n  \
         tombstoned ids reachable by NEITHER reader (not in object_index,\n  \
         and no page carries them)                                  {ids_unreachable} of \
         {ids_outliving}"
    );

    assert!(
        buckets_total > 0,
        "denominator: the shard holds no buckets, so nothing below compares anything"
    );
    assert!(
        buckets_with_tombstone > 0,
        "denominator: {deleted_keys} keys were deleted and not one bucket carries a tombstone, so \
         this fixture is not exercising `deleted_object_index` at all"
    );
    assert!(
        buckets_outliving > 0,
        "the state this module is about was not reached: {buckets_with_tombstone} buckets carry a \
         tombstone but in every one of them a page still carries the id, so a per-page bit COULD \
         have answered and the refutation would be unproven"
    );

    assert!(
        buckets_outliving_in_object_index > 0,
        "not one of the {buckets_outliving} buckets carrying an orphaned tombstone holds that id in \
         `object_index`, so NEITHER reader reaches any of them -- the walk over `object_index` \
         never sees the id and there is no page to carry it. On this corpus the field would be \
         unread, and the refutation this module states would be unproven"
    );

    let (routing_bucket, object_id) = target.expect(
        "no bucket holds a tombstoned id in `object_index` while not itself being marked deleted; \
         without that bucket, whole-bucket `deleted()` could be what answers and the perturbation \
         below would not be discriminating",
    );

    // --- The state, asserted element by element on the chosen bucket. ---
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&TOMB_SHARD).expect("shard is loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("the bucket was just read from this map");
        println!(
            "\n=== the chosen bucket {routing_bucket}, object {object_id} ===\n  \
             object_index holds it        {}\n  \
             pages carrying it            {}\n  \
             bucket.deleted()             {}\n  \
             tombstone names it           {}\n  \
             pages in the bucket          {}",
            bucket.object_index.contains(&object_id),
            bucket
                .block_index
                .values()
                .filter(|page| page.object_id() == object_id)
                .count(),
            bucket.deleted(),
            bucket.deleted_object_index.contains(&object_id),
            bucket.block_index.len()
        );
        assert!(
            bucket.object_index.contains(&object_id),
            "object {object_id} is not in bucket {routing_bucket}'s object_index, so the walk that \
             reads the tombstone never reaches it"
        );
        assert_eq!(
            0,
            bucket
                .block_index
                .values()
                .filter(|page| page.object_id() == object_id)
                .count(),
            "a page in bucket {routing_bucket} still carries object {object_id}, so a per-page \
             `deleted` bit COULD have carried this deletion and this is not the state the module \
             is about"
        );
        assert!(
            !bucket.deleted(),
            "bucket {routing_bucket} is itself marked deleted, so `bucket.deleted()` would answer \
             for this object and the tombstone would not be the only thing that can"
        );
        assert!(
            bucket.deleted_object_index.contains(&object_id),
            "bucket {routing_bucket} does not name object {object_id} in its tombstone set, so \
             there is nothing for the perturbation to remove"
        );
    }

    // --- The control, taken FIRST on the same shard. ---
    let control = engine.object_manager_runtime_report(TOMB_SHARD);
    println!(
        "\n=== CONTROL: the published report before the edit ===\n  \
         delete_marker_object_count={}  object_count={}",
        control.delete_marker_object_count, control.object_count
    );
    assert!(
        control.delete_marker_object_count > 0,
        "the control reports 0 tombstoned objects on a shard where {deleted_keys} keys were \
         deleted; the figure this perturbation moves is not being computed at all"
    );

    // --- The perturbation: take the tombstone for that ONE id away. ---
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&TOMB_SHARD).expect("shard is loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get_mut(&routing_bucket)
            .expect("the bucket this test chose is in the map");
        assert!(
            bucket.deleted_object_index.remove(&object_id),
            "the tombstone for object {object_id} was not there to remove"
        );
    }

    let perturbed = engine.object_manager_runtime_report(TOMB_SHARD);
    println!(
        "=== PERTURBED: one tombstone removed, nothing else touched ===\n  \
         delete_marker_object_count={}  object_count={}",
        perturbed.delete_marker_object_count, perturbed.object_count
    );

    assert_eq!(
        control.delete_marker_object_count - 1,
        perturbed.delete_marker_object_count,
        "removing ONE tombstone moved the published count from {} to {}; it had to fall by exactly \
         one. If it did not move at all, nothing reads this field and the eight bytes ARE \
         available; if it moved by more than one, the perturbation is not the only thing that \
         changed",
        control.delete_marker_object_count,
        perturbed.delete_marker_object_count
    );

    println!(
        "\n=== THE REFUTATION ===\n  Object {object_id} in bucket {routing_bucket} is IN \
         `object_index` and NO page in `block_index` carries it. With the tombstone the published \
         report calls it deleted; with that one entry removed and nothing else touched, LIVE. No \
         per-page `deleted` bit could carry that, because the delete retired the pages -- and \
         `bucket.deleted()` is FALSE here, asserted above, so whole-bucket deletion is not what \
         answers either. `deleted_object_index` stays, and the {} -> {} the arithmetic offers is \
         not available.",
        size_of::<BucketNode>(),
        size_of::<BucketNode>() - 8
    );
}
