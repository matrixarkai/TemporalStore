// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CAN A PAGE ENTRY NAME ITS OBJECT BY ITS SLOT IN THE BUCKET'S OBJECT LIST? NO, AND THE TWO
//! GROUNDS ARE INDEPENDENT.
//!
//! THE PROPOSAL, AND IT IS THE BEST-SHAPED ONE THIS ROUTE HAS HAD. `BlockIndex::object_key` is an
//! `Arc<str>` at sixteen bytes on an entry that is fifty-six with no slack. Swapping it for a
//! four-byte ordinal takes the field sum to forty-four and the width to forty-eight, MEASURED
//! BELOW. Every previous attempt at that paid for the ordinal with a stored object-ordinal table,
//! and a second stored structure is the one thing this leg may not add. So the question asked here
//! is narrower: IS THE ORDINAL DERIVABLE FROM SOMETHING THE INDEX ALREADY HOLDS?
//!
//! AND THE ANSWER TO THAT NARROW QUESTION IS YES, ON THE WIRE -- WHICH IS WHY THE REFUSAL HAD TO
//! BE DRIVEN RATHER THAN ASSUMED. `bucket.object_index` is a slot array whose positions #2057 and
//! the stored form that followed it made durable: `Serialize` writes slot order with `null`
//! placeholders, `Deserialize` reads back `Vec<Option<u64>>` by position, and
//! `ObjectSlots::from_stored_slots` files them where they were written rather than through
//! `insert`. A slot is therefore a WRITTEN position and costs no new structure. The one objection
//! recorded against a bucket-relative number -- that the three deciding sites take a bare
//! `&BlockIndex` with no bucket in scope -- is also gone, because `routing_bucket` is a FIELD on
//! the entry now, so `(routing_bucket, slot)` is readable from the entry alone. Both halves of the
//! lead check out at the wire.
//!
//! IT STILL FAILS, ON TWO GROUNDS THAT DO NOT DEPEND ON EACH OTHER.
//!
//! # GROUND 1 -- THE WIRE KEEPS THE POSITIONS AND THE LOAD THROWS THEM AWAY
//!
//! MEASURED, END TO END. Two objects are stored at slots 0 and 1 -- read off the object list's own
//! `Serialize`, so the positions are known to have been written. After a dump and a restart on the
//! same directories, the surviving LIVE object is at slot 0. `persistence.rs:487` runs
//! `update_bucket_layout` over every bucket once the index-log delta has folded, and that scan is
//! confirmed to have run; which of its arms moved the id is deliberately NOT asserted, for the
//! reason below.
//!
//! SO NO ENTRY'S SLOT SURVIVES A RESTART -- live or tombstoned -- and an ordinal stored on an entry
//! names a different object or none after the first one.
//!
//! THREE DRAFTS OF THIS TEST WERE WRONG AND ALL THREE FAILURES ARE KEPT IN IT, because each was
//! caught by a different part of the apparatus and that is the only reason the claim above is the
//! size it is.
//!
//!   * DRAFT ONE asserted that a `SetRemove` frees the tombstoned object's slot. It does not: the
//!     remove path at `storage_bucket_internals.rs:4086` drops an id only when NO ENTRY names it
//!     -- `block_index.values().any(|page| page.object_id(shard_id) == removed_object_id)`, with
//!     no `!page.deleted` filter -- so a tombstone holds its slot, which is exactly the invariant
//!     a durable ordinal needs. `reconcile_object_index_with_live_pages`'s own doc comment had
//!     said in advance that the load is "the fatal one for anything that stored a slot, and it is
//!     also the quietest, which is why a test driving a write and finding nothing proves nothing
//!     here". A write is what draft one drove.
//!   * DRAFT TWO asserted that the load frees the TOMBSTONE'S slot and leaves the live object's
//!     alone, since `update_bucket_layout` filters `!page.deleted`. THE CONTROL caught it: the
//!     live witness moved too. The ground is wider than a tombstone.
//!   * DRAFT THREE named a RECONSTRUCT as the branch that renumbered it. THE COUNTER caught it:
//!     `rebuilds +5, reconstructs +0, list_held_an_object_no_page_names +1` -- the load
//!     RECONCILED, and the live id moved anyway. The same reading is why no branch is asserted
//!     now: one load scanned five buckets and these counters are SHARD-WIDE, so a delta of +1 is
//!     not attributable to the bucket under test.
//!
//! WHY THE EXISTING STABILITY PROOF DOES NOT COVER THIS. `a_slot_into_the_object_list_survives_
//! the_three_mutations_that_invalidated_an_ordinal` watches a live object across three in-memory
//! mutations, and the tests in `the_slot_survives_a_reload` round-trip `ObjectIndex` through its
//! own serde impls. Both are true and neither drives an ENGINE reload, which is where the
//! reconstruct is. The rule they establish is the documented one -- "slot `i` names the same
//! object for as long as that object is in the bucket" -- and the rule a durable ordinal needs is
//! that a slot names one object for as long as any ENTRY names the slot.
//!
//! THE CONTROL IS THE STORED FORM, read off the object list's own `Serialize` before the dump, so
//! "the positions were written and then discarded" is measured at both ends. Without it, a
//! renumbering reload would be indistinguishable from positions that were never durable.
//!
//! # GROUND 2 -- THE ENTRY IS THE ONLY DURABLE HOME OF THE KEY'S CHARACTERS
//!
//! A slot resolves to a `u64` that is `hashing::stable_block_object_id(shard, kind, key)`, an
//! FNV-1a hash. It is an IDENTITY and it is not invertible, so no ordinal can hand a reader the
//! characters. Of the fifty-nine production sites the compiler names (below), the ones that want
//! characters have exactly one place to get them, and it is the field being removed:
//!
//!   * `BUCKET_NODE_FIELDS` is thirteen names. `object_index` and `deleted_object_index` are bare
//!     ids. `page_index` is the `BlockIndex` rows. Nothing else on a node carries text.
//!   * The block store does not keep a key with a page; `object_key` appears in `block_store.rs`
//!     only in prose about routing.
//!   * The index-log row DOES carry `object_key` as text -- and it is not a fallback, for two
//!     reasons that compose. It is WRITTEN FROM THE ENTRY (`collect_command_index_items_for` sets
//!     `object_key: page.object_key.clone()` at `engine.rs:3328`), so it is downstream of the
//!     field rather than independent of it, and the compiler census is the proof of that
//!     dependency; and `dump_and_reclaim_index_logs_with_min_reclaimable` gives the log back once
//!     a dump has made the base index authoritative, so it is not durable past a dump either.
//!
//! `the_page_entry_is_the_only_durable_home_of_an_object_keys_characters` drives that, with both
//! controls: a key that IS in the store must be found (or the scan is vacuous) and a key of the
//! same shape that is NOT must be found nowhere (or the matcher matches anything). MEASURED: of
//! the twelve members a stored `BucketNode` writes, the key's characters are in `page_index` and
//! in no other, and `object_index`'s 106 bytes are ids and placeholders only.
//!
//! AND IT IS MEASURED ON THE SERIALIZED NODE, NOT ON THE FILE, because of a detector failure worth
//! recording. A first draft scanned the raw bytes of every file under the engine's three
//! directories for the key's characters and found them in NONE of fourteen files, which reads as
//! "the characters are not durable at all" and would have OPENED this leg. The stored index is
//! COMPRESSED -- `shard-1.index.json` begins with `TSIDX` -- so that scan could not have found a
//! key in it at any width. The test's own positive control is what caught it: a key the store
//! certainly holds was found nowhere, which is a statement about the scan and not about the store.
//! The measurement is taken on `BucketNode`'s own `Serialize` output instead, which is what the
//! compressor is handed.
//!
//! A SECOND DETECTOR FAILED THE SAME WAY AND IS ALSO RECORDED. The index-log arm first bounded the
//! log's BYTES against the characters written; its control refused, because the log reads 0 bytes
//! before the dump as well as after -- this fixture's rows are not on disk until the dump writes
//! them, so the bound could not tell the two states apart. That arm now asserts CONTENT, with the
//! matcher controlled on both sides over the same byte search.
//!
//! THE PRECEDENT THAT DOES NOT TRANSFER, and it is worth naming because it is the obvious hope. A
//! tombstone page already stores its own element key, so "ask what already holds the fact" is the
//! right question here. The answer is that nothing does: a tombstone stores a COMPONENT, which is
//! a different field, and it stores it per page rather than per object.
//!
//! # THE READER CENSUS, BY COMPILER AND NOT BY GREP
//!
//! Measured at `2ae24a03d` by replacing the field with `pub(super) object_ordinal: u32` and
//! running `cargo check --lib`: 59 production field sites, 52 reads (E0609) and 7 constructions
//! (E0560). A grep for the field name over-counts, and E0560 SUPPRESSES E0063 on the same literal,
//! so the construction arm is a FLOOR until the first pass is fixed. `--lib` is also not all code:
//! it does not compile examples, the test targets, or `#[cfg(feature)]` regions.
//!
//! Split by what each site actually wants, and the split is what decides the leg:
//!
//!   * 28 sites use the text only AS AN IDENTITY and an ordinal serves them. 14 are
//!     `&*page.object_key == key` against a caller's `&str`; 6 compare the entry against another
//!     in-tree row that could carry the ordinal too; 2 are `contains` over a caller's key set;
//!     1 is `block_index_handle`, which hashes the key to a handle and would hash
//!     `(routing_bucket, ordinal)` instead -- both terms on the entry; and 5 reach
//!     `ObjectBlockLookup`, which could be re-keyed by `(routing_bucket, slot)`.
//!   * 22 sites WANT THE CHARACTERS and have no other source: `block_index_written_key`, the
//!     `IndexItem` row, `LiveBlockEntry`, `runtime_report`'s public `object_keys`,
//!     `StoragePhysicalBlockIndex` (2), `filing_by_object_key`, `released_block_identity`, the
//!     `dropped` triples in the release path, the delete-marker sample, the `dirty_objects` probes
//!     (4), `expires_at_ms` (1), the lifecycle dump, and `bucket_object_count`'s distinct-object
//!     tuples (2).
//!   * 1 site CANNOT BE SERVED AT ALL, and it is `storage_bucket_internals.rs:2247`:
//!     `block_routing_bucket(&block.object_key, start, end) != routing_bucket`. The bucket is
//!     FNV-1a over the key BYTES, so no ordinal produces it -- and the `routing_bucket` field is
//!     not a substitute, because the check exists precisely to compare the bucket a block's KEY
//!     routes to against the bucket HOLDING it. Its own comment says it "compares two INDEPENDENT
//!     things rather than a block's copy of its bucket against the bucket holding it". Feeding it
//!     the field turns a check that can fail into one that cannot.
//!
//! WHAT WOULD REOPEN THIS. Ground 1 is a fixable defect, and the fix is not the one that looks
//! obvious: the load path would have to preserve positions THROUGH the reconcile -- a live set
//! taken over every entry rather than every live page, no collapse that can move a surviving id,
//! and no give-back of trailing positions -- at the cost of an array no longer bounded by the
//! bucket's live high water mark. Ground 2 is not fixable without a durable ordinal-to-characters
//! map, which is the second index this leg exists to avoid. So the leg is refused on Ground 2, and
//! Ground 1 is the reason a partial fix would not rescue it.
//!
//! AND ONE THING THE WIDTH WORK SETTLED ON THE WAY, found by a mutation and not by argument: the
//! ORDINAL'S WIDTH DOES NOT MOVE THE ENTRY. u8, u16, u32 and u64 all land it at 48, because the
//! tail carries four bytes of padding and the address is eight-aligned. A fail-first mutation that
//! widened the field to `u64` left every assertion GREEN, which exposed both that and a real
//! defect in this module: the field sums were hand-written literals standing beside the structs
//! rather than reconstructions of them. They are taken off a real instance's fields with
//! `size_of_val` now, so a field whose type moves moves the sum.
//!
//! WHAT THE 48 IS WORTH, so the refusal is priced rather than merely stated. Eight bytes an entry
//! of resident width, and nothing on disk -- the stored row's `object_key` would become a number,
//! which is smaller, but the characters would then have to be written somewhere else. Against that:
//! a load path that must retain positions, 28 call sites to re-point, 22 readers that need a new
//! source for the characters, and one -- `block_routing_bucket` over the key BYTES -- that has
//! none.
//!
//! NO STORED SHAPE MOVES HERE, SO NO `SHARD_INDEX_FORMAT_VERSION` IS OWED. This module adds
//! measurements and changes no production code.
//!
//! EVERY ASSERTION HERE WAS SHOWN TO FAIL ON CODE THAT DOES NOT HOLD IT, and three of the six
//! demonstrations were accidents rather than planned mutations:
//!
//!   * the three drafts above -- a removal that keeps the slot, a control that caught the live
//!     witness moving, and a counter that refused the named branch;
//!   * widening the mirror's ordinal to `u64`, which FIRST PASSED and exposed the parallel-literal
//!     field sums, and reddens the sum now;
//!   * planting a field in either mirror, which reddens that mirror's width and its tie;
//!   * comparing the stored array against itself instead of against the reloaded one;
//!   * watching the slot the witness actually lands in rather than its stored one;
//!   * pointing the member matcher at the never-written key, which fires the positive control.
//!
//! rust-internal: drives this crate's own engine, index log and layout scan, no external surface

#![allow(clippy::all)]

use super::*;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

// =================================================================================================
// THE WIDTH
// =================================================================================================

/// THE FIELD SET A FOUR-BYTE ORDINAL WOULD LEAVE, with every type taken from the live entry.
///
/// A MIRROR AND NOT A RESTATEMENT. Nothing here is a literal: each field is spelled with the type
/// the real `BlockIndex` spells it with, so a field whose own width moves elsewhere moves this
/// structure too and fails here rather than passing against a stale number. `#[repr]`-free, so the
/// compiler lays it out exactly as it lays out the real entry.
///
/// MEASURED, NOT PROPOSED. `size_of::<BlockIndex>()` with `object_key: Arc<str>` replaced by
/// `object_ordinal: u32` in the real struct was read off rustc at `2ae24a03d` as **48**, and the
/// field sum as **44**, by making the compiler print them: a
/// `const _: [(); 0] = [(); std::mem::size_of::<BlockIndex>()];` reports "expected an array with a
/// size of 0, found one with a size of 48". This mirror reproduces both, which is what makes it
/// usable as a stand-in for a struct this module does not change.
struct OrdinalEntry {
    _kind: crate::index_log::IndexItemKind,
    _routing_bucket: u32,
    _object_ordinal: u32,
    _model_id: StoredModelKind,
    _component: Option<Arc<str>>,
    _address: ElementEntry,
    _dirty: bool,
    _deleted: bool,
}

/// THE FIELD SET ONCE `component` HAS GONE TOO -- the 28/32 this leg of the campaign is aiming at.
///
/// `component` is another lane's change and nothing here touches it. This structure exists so the
/// target is pinned to the LIVE types rather than carried as a number in prose, and so it cannot
/// drift: the assertions below tie it to `BlockIndex` through the widths of the two fields that
/// would have to leave.
struct BothGoneEntry {
    _kind: crate::index_log::IndexItemKind,
    _routing_bucket: u32,
    _object_ordinal: u32,
    _model_id: StoredModelKind,
    _address: ElementEntry,
    _dirty: bool,
    _deleted: bool,
}

fn an_address() -> ElementEntry {
    ElementEntry::from_parts(1, 64, 32, Some(9), Some(11))
}

/// THE SUMS ARE TAKEN OFF AN INSTANCE'S FIELDS, NOT WRITTEN OUT BESIDE THEM -- and a mutation is
/// the reason.
///
/// These were hand-written sums spelling each term as `size_of::<u32>()` and so on. That reads as a
/// reconstruction and is not one: widening `OrdinalEntry::_object_ordinal` from `u32` to `u64`
/// left all three tests GREEN, because the sum went on saying `size_of::<u32>()` while the struct
/// said `u64`. Two parallel statements of one fact, and the assertion was on the one that had not
/// moved.
///
/// `size_of_val` over the fields of a real instance cannot drift that way: the term IS the field.
fn ordinal_entry_field_sum() -> usize {
    let probe = OrdinalEntry {
        _kind: crate::index_log::IndexItemKind::Page,
        _routing_bucket: 0,
        _object_ordinal: 0,
        _model_id: StoredModelKind::String,
        _component: None,
        _address: an_address(),
        _dirty: false,
        _deleted: false,
    };
    std::mem::size_of_val(&probe._kind)
        + std::mem::size_of_val(&probe._routing_bucket)
        + std::mem::size_of_val(&probe._object_ordinal)
        + std::mem::size_of_val(&probe._model_id)
        + std::mem::size_of_val(&probe._component)
        + std::mem::size_of_val(&probe._address)
        + std::mem::size_of_val(&probe._dirty)
        + std::mem::size_of_val(&probe._deleted)
}

fn both_gone_field_sum() -> usize {
    let probe = BothGoneEntry {
        _kind: crate::index_log::IndexItemKind::Page,
        _routing_bucket: 0,
        _object_ordinal: 0,
        _model_id: StoredModelKind::String,
        _address: an_address(),
        _dirty: false,
        _deleted: false,
    };
    std::mem::size_of_val(&probe._kind)
        + std::mem::size_of_val(&probe._routing_bucket)
        + std::mem::size_of_val(&probe._object_ordinal)
        + std::mem::size_of_val(&probe._model_id)
        + std::mem::size_of_val(&probe._address)
        + std::mem::size_of_val(&probe._dirty)
        + std::mem::size_of_val(&probe._deleted)
}

/// THE LIVE ENTRY'S SUM, OFF A REAL `BlockIndex`. Every other figure in this module is anchored on
/// this one, so it is the term that most needs not to be a literal.
fn live_entry_field_sum() -> usize {
    let probe = BlockIndex {
        kind: crate::index_log::IndexItemKind::Page,
        routing_bucket: 0,
        object_key: Arc::from("slot-probe"),
        model_id: StoredModelKind::String,
        component: None,
        address: an_address(),
        dirty: false,
        deleted: false,
    };
    std::mem::size_of_val(&probe.kind)
        + std::mem::size_of_val(&probe.routing_bucket)
        + std::mem::size_of_val(&probe.object_key)
        + std::mem::size_of_val(&probe.model_id)
        + std::mem::size_of_val(&probe.component)
        + std::mem::size_of_val(&probe.address)
        + std::mem::size_of_val(&probe.dirty)
        + std::mem::size_of_val(&probe.deleted)
}

/// rust-internal: arithmetic over this crate's own types, no external surface
#[test]
fn the_ordinal_would_make_the_entry_forty_eight_and_the_target_is_thirty_two() {
    println!("\n=== THE WIDTH, AT THIS COMMIT ===");
    println!(
        "  live BlockIndex      field_sum {:>3}  size_of {:>3}",
        live_entry_field_sum(),
        std::mem::size_of::<BlockIndex>()
    );
    println!(
        "  with a u32 ordinal   field_sum {:>3}  size_of {:>3}",
        ordinal_entry_field_sum(),
        std::mem::size_of::<OrdinalEntry>()
    );
    println!(
        "  and component gone   field_sum {:>3}  size_of {:>3}",
        both_gone_field_sum(),
        std::mem::size_of::<BothGoneEntry>()
    );

    // THE LIVE ENTRY, WHICH IS WHAT EVERYTHING ELSE IS ANCHORED ON.
    assert_eq!(
        56,
        std::mem::size_of::<BlockIndex>(),
        "the live entry is {} bytes, not 56, so every figure below is anchored on a width that \
         has moved",
        std::mem::size_of::<BlockIndex>()
    );
    assert_eq!(
        56,
        live_entry_field_sum(),
        "the live entry's fields add up to {} in 56, so the ZERO SLACK this whole leg was priced \
         against is gone",
        live_entry_field_sum()
    );

    // WHAT THE ORDINAL PRODUCES: 44 of field in 48. MEASURED on the real struct and reproduced
    // here; see `OrdinalEntry`'s own doc comment for the rustc transcript.
    assert_eq!(
        44,
        ordinal_entry_field_sum(),
        "the ordinal field set adds up to {} bytes, not 44 -- the width this module is about is \
         the four-byte one, and the sum is taken off the mirror's own fields, so a field whose \
         type moved is what this is reporting",
        ordinal_entry_field_sum()
    );
    assert_eq!(
        48,
        std::mem::size_of::<OrdinalEntry>(),
        "the ordinal field set lands at {} bytes, not the 48 rustc measured on the real struct",
        std::mem::size_of::<OrdinalEntry>()
    );

    // THE 32 PROBE: 28 of field in 32.
    assert_eq!(
        28,
        both_gone_field_sum(),
        "the post-removal field set adds up to {}, not 28",
        both_gone_field_sum()
    );
    assert_eq!(
        32,
        std::mem::size_of::<BothGoneEntry>(),
        "the post-removal field set lands at {} bytes, not 32",
        std::mem::size_of::<BothGoneEntry>()
    );

    // NO SLACK BEYOND THE ROUNDING, asserted as the claim rather than read off a spare byte. Both
    // mirrors are exactly their field sum rounded up to a word, and the remainder is PRINTED
    // rather than called nothing: 44 in 48 and 28 in 32 each carry four bytes of tail, so the
    // next field added to either costs NOTHING, where the next field added to the live
    // fifty-six-in-fifty-six costs EIGHT. That difference is the thing a reader needs, and the
    // phrase "zero slack" applied to these two would hide it.
    for (what, field_sum, width) in [
        (
            "with a u32 ordinal",
            ordinal_entry_field_sum(),
            std::mem::size_of::<OrdinalEntry>(),
        ),
        (
            "and component gone",
            both_gone_field_sum(),
            std::mem::size_of::<BothGoneEntry>(),
        ),
        (
            "live BlockIndex",
            live_entry_field_sum(),
            std::mem::size_of::<BlockIndex>(),
        ),
    ] {
        assert_eq!(
            (field_sum + 7) / 8 * 8,
            width,
            "{what} is {width} bytes wide for {field_sum} bytes of field, which is not that sum \
             rounded up to a word -- so something in it is padded internally and the arithmetic \
             above is not a reconstruction of it"
        );
        println!("  {what:<20} tail {} bytes", width - field_sum);
    }

    // AND THE TIE TO THE LIVE TYPE, WHICH IS WHAT MAKES THESE CLAIMS ABOUT `BlockIndex` RATHER
    // THAN ARITHMETIC IN A VACUUM. Each term is the width of the field that would have to move,
    // spelled as that field's own type.
    assert_eq!(
        std::mem::size_of::<OrdinalEntry>(),
        std::mem::size_of::<BothGoneEntry>() + std::mem::size_of::<Option<Arc<str>>>(),
        "48 is not 32 plus the component's {} bytes, so the two targets are not one field apart \
         and the second does not follow from the first",
        std::mem::size_of::<Option<Arc<str>>>()
    );
    assert_eq!(
        live_entry_field_sum(),
        ordinal_entry_field_sum() - std::mem::size_of::<u32>() + std::mem::size_of::<Arc<str>>(),
        "the live field sum is not the ordinal field sum with the ordinal taken back out and the \
         key's {} bytes put back, so the mirror is not the live entry with one field swapped",
        std::mem::size_of::<Arc<str>>()
    );
    assert_eq!(
        live_entry_field_sum(),
        both_gone_field_sum() - std::mem::size_of::<u32>()
            + std::mem::size_of::<Arc<str>>()
            + std::mem::size_of::<Option<Arc<str>>>(),
        "28 does not reconstruct to the live 56 by putting the key and the component back, so the \
         32 probe is not pinned to this type at all"
    );

    // THE ORDINAL'S WIDTH DOES NOT MOVE THE ENTRY AT ALL, u8 THROUGH u64 -- and this paragraph is
    // here because a mutation found it rather than because it was reasoned to.
    //
    // It said only that a `u8` buys nothing. A fail-first mutation widened the field to `u64`
    // expecting the 48 pin to redden, and ALL THREE TESTS STAYED GREEN. Two things were wrong at
    // once: the field sum was a parallel literal (fixed above, so the sum now moves to 48 and the
    // `44` assertion reddens), and `size_of` genuinely does NOT move, because the tail has four
    // bytes of padding and the address is eight-aligned, so an ordinal of 1, 4 or 8 bytes all land
    // at 48. The blind spot is now an asserted property instead.
    //
    // WHAT THAT MEANS FOR THE DECISION: the entry's width places NO constraint on the ordinal's
    // width. A 255-object-a-bucket ceiling buys nothing, and neither does a 64-bit ordinal cost
    // anything. The choice is free and should be made on the semantics.
    macro_rules! ordinal_of_width {
        ($name:ident, $width:ty) => {{
            struct $name {
                _kind: crate::index_log::IndexItemKind,
                _routing_bucket: u32,
                _object_ordinal: $width,
                _model_id: StoredModelKind,
                _component: Option<Arc<str>>,
                _address: ElementEntry,
                _dirty: bool,
                _deleted: bool,
            }
            std::mem::size_of::<$name>()
        }};
    }
    let by_width = [
        ("u8", ordinal_of_width!(U8Ordinal, u8)),
        ("u16", ordinal_of_width!(U16Ordinal, u16)),
        ("u32", ordinal_of_width!(U32Ordinal, u32)),
        ("u64", ordinal_of_width!(U64Ordinal, u64)),
    ];
    println!("\n=== THE ENTRY'S WIDTH AGAINST THE ORDINAL'S ===");
    for (spelling, width) in by_width {
        println!("  {spelling:<4} ordinal -> entry {width} bytes");
        assert_eq!(
            48, width,
            "a {spelling} ordinal lands the entry at {width} rather than 48, so the ordinal's \
             width DOES move the entry and the paragraph above is wrong -- which would also mean \
             the 48 in this module is a figure about one choice of width rather than about the \
             field set"
        );
    }
}

// =================================================================================================
// HARNESS
// =================================================================================================

/// EIGHT BUCKETS, so distinct keys collide into one bucket without hunting for a collision.
const START_BUCKET: u32 = 0;
const END_BUCKET: u32 = 7;
const SHARD: crate::types::ShardId = 1;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, table: &str, load_version: u64) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: SHARD,
        table_name: table.to_string(),
        shard_uri: format!("local://{table}/1"),
        start_routing_bucket: START_BUCKET,
        end_routing_bucket: END_BUCKET,
        readonly: false,
        load_version,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "APPARATUS: load failed: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command,
    });
    assert!(response.status.ok, "APPARATUS: write failed: {response:?}");
}

/// The slot array of one bucket, as `(slot, id)` for the slots that hold an id.
fn slots_of(engine: &TemporalEngine, routing_bucket: u32) -> BTreeMap<usize, u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
        return BTreeMap::new();
    };
    (0..bucket.object_index.slot_count())
        .filter_map(|slot| bucket.object_index.id_at(slot).map(|id| (slot, id)))
        .collect()
}

/// The object list of one bucket AS IT SERIALIZES, which is the stored positions.
fn stored_object_index(engine: &TemporalEngine, routing_bucket: u32) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let bucket = shard
        .bucket_index
        .bucket_map
        .get(&routing_bucket)
        .expect("APPARATUS: the bucket under test is not resident");
    serde_json::from_value(
        serde_json::to_value(&bucket.object_index).expect("an object list serializes"),
    )
    .expect("an object list's stored form is a sequence of ids and placeholders")
}

/// How many entries in one bucket name this object, split by whether each is a tombstone.
fn entries_naming(engine: &TemporalEngine, routing_bucket: u32, object_id: u64) -> (usize, usize) {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&SHARD).expect("shard is loaded");
    let Some(bucket) = shard.bucket_index.bucket_map.get(&routing_bucket) else {
        return (0, 0);
    };
    let mut live = 0usize;
    let mut tombstoned = 0usize;
    for page in bucket.block_index.values() {
        if page.object_id(SHARD) != object_id {
            continue;
        }
        if page.deleted {
            tombstoned += 1;
        } else {
            live += 1;
        }
    }
    (live, tombstoned)
}

/// The object id of a `(kind, key)` under the shard this module loads.
fn object_id_of(kind: &str, key: &str) -> u64 {
    crate::engine::hashing::stable_block_object_id(SHARD, kind, key)
}

fn routing_bucket_of(key: &str) -> u32 {
    crate::engine::hashing::block_routing_bucket(key, START_BUCKET, END_BUCKET)
}

/// Three keys that route to one bucket, with the bucket they route to.
fn three_keys_in_one_bucket(prefix: &str) -> (u32, String, String, String) {
    let mut by_bucket: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for index in 0..400usize {
        let key = format!("{prefix}-{index:04}");
        by_bucket
            .entry(routing_bucket_of(&key))
            .or_default()
            .push(key);
    }
    let (routing_bucket, keys) = by_bucket
        .iter()
        .find(|(_, keys)| keys.len() >= 3)
        .map(|(bucket, keys)| (*bucket, keys.clone()))
        .expect("APPARATUS: no bucket in the stamped range took three of four hundred keys");
    (
        routing_bucket,
        keys[0].clone(),
        keys[1].clone(),
        keys[2].clone(),
    )
}

// =================================================================================================
// GROUND 1 -- A SLOT DOES NOT SURVIVE AN ENGINE RELOAD
// =================================================================================================

/// THE STORED FORM CARRIES THE POSITIONS AND THE LOAD THROWS THEM AWAY, and this test is two
/// drafts' worth of being wrong about which step does it.
///
/// DRAFT ONE asserted that a `SetRemove` frees the tombstoned object's slot. It does not, and it
/// does not for a good reason: the remove path at `storage_bucket_internals.rs:4086` drops an
/// object from the list only when NO ENTRY in the bucket names it --
/// `block_index.values().any(|page| page.object_id(shard_id) == removed_object_id)`, with no
/// `!page.deleted` filter -- so while a tombstone entry is filed, the id and its slot stay. That
/// is exactly the invariant a durable ordinal needs, and the in-session path already holds it. The
/// run printed `after the removal: {0: .., 1: ..}`, unchanged, and the assertion failed.
///
/// DRAFT TWO asserted that the LOAD frees the tombstone's slot and leaves the live object's alone,
/// because `persistence.rs:487` runs `update_bucket_layout` over every bucket and that function
/// derives its live set as `block_index.values().filter(|page| !page.deleted)`. The first half is
/// right. The second half is wrong, and the CONTROL is what caught it: the live witness moved from
/// slot 1 to slot 0 across the same reload.
///
/// DRAFT THREE then named the wrong branch for that, and the COUNTER caught it. It asserted a
/// RECONSTRUCT -- `reconcile_object_index_with_live_pages` re-inserting from a `BTreeSet<u64>` in
/// ascending id order -- and the scan reported `rebuilds +5, reconstructs +0,
/// list_held_an_object_no_page_names +1`. So the load RECONCILED, and the live id still moved. The
/// branch is not asserted now, for a reason the same reading gives: one load scanned five buckets
/// and these counters are shard-wide, so a +1 is not attributable to the bucket under test.
///
/// SO THE REFUTATION IS WIDER THAN THE ONE THIS MODULE SET OUT TO MAKE, AND SHALLOWER THAN DRAFT
/// THREE CLAIMED. It is not that a tombstone loses its slot, and it is not a reconstruct. It is
/// that the array a restart hands back differs from the array that was stored, in the position of
/// a LIVE id -- so no entry's slot survives a restart, and an ordinal stored on an entry is wrong
/// after the first one. Both arrays are read directly, which is what the claim rests on.
///
/// AND THE STORED FORM IS THE CONTROL, which is what makes this a claim about the LOAD rather than
/// about the slots being resident after all. `the_slot_survives_a_reload` establishes that the
/// wire preserves positions; this test re-establishes it on its own fixture by reading the
/// serialized object list before the reload, so "the positions were written and then discarded" is
/// measured at both ends rather than at one.
///
/// rust-internal: drives SetAdd/SetRemove, a dump and a reload, no external surface
#[test]
fn no_entrys_slot_survives_an_engine_reload_so_a_stored_ordinal_cannot_name_an_object() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (routing_bucket, victim, witness, successor) = three_keys_in_one_bucket("reuse");
    let victim_id = object_id_of("set", &victim);
    let witness_id = object_id_of("set", &witness);
    let successor_id = object_id_of("set", &successor);
    assert_ne!(
        victim_id, successor_id,
        "APPARATUS: the victim and the successor hash to one object id, so 'a different object' \
         is not what a reused slot would be naming"
    );

    println!("\n=== bucket {routing_bucket} ===");
    println!("  victim    {victim}  id {victim_id:#018x}  (removed, leaves a tombstone entry)");
    println!("  witness   {witness}  id {witness_id:#018x}  (live throughout)");
    println!("  successor {successor}  id {successor_id:#018x}  (added after the reload)");

    let (victim_slot, witness_slot, stored) = {
        let engine = engine_on(dir.path());
        load_on(&engine, "slot-reuse", 1);

        // ONE MEMBER EACH, so the victim's removal takes its last live page and nothing else.
        for key in [&victim, &witness] {
            write(
                &engine,
                Command::SetAdd {
                    key: key.clone(),
                    member: b"m0".to_vec(),
                },
            );
        }
        let filed = slots_of(&engine, routing_bucket);
        println!("  after the two writes:      {filed:?}");
        let victim_slot = *filed
            .iter()
            .find(|(_, id)| **id == victim_id)
            .map(|(slot, _)| slot)
            .expect("APPARATUS: the victim's id is not in the bucket's object list at all");
        let witness_slot = *filed
            .iter()
            .find(|(_, id)| **id == witness_id)
            .map(|(slot, _)| slot)
            .expect("APPARATUS: the witness's id is not in the bucket's object list at all");
        assert_ne!(
            victim_slot, witness_slot,
            "APPARATUS: the two objects share slot {victim_slot}, so there is no slot to watch"
        );

        // THE REMOVAL KEEPS THE SLOT, asserted rather than passed over -- draft one asserted the
        // opposite, and the unfiltered guard that makes this true is the reason the in-session
        // path is NOT the one that breaks an ordinal.
        write(
            &engine,
            Command::SetRemove {
                key: victim.clone(),
                member: b"m0".to_vec(),
            },
        );
        let after_removal = slots_of(&engine, routing_bucket);
        let (live_here, tombstoned_here) = entries_naming(&engine, routing_bucket, victim_id);
        println!("  after the removal:         {after_removal:?}");
        println!("  the victim's entries: {live_here} live, {tombstoned_here} tombstoned");
        assert!(
            tombstoned_here >= 1,
            "APPARATUS: the removal left the victim {tombstoned_here} tombstone entries, so this \
             test is not about a tombstone and every assertion below is about the wrong fixture"
        );
        assert_eq!(
            0, live_here,
            "APPARATUS: the victim still has {live_here} live entries, so its object is still in \
             the live set and nothing below is about an object with no live page"
        );
        assert_eq!(
            Some(victim_id),
            after_removal.get(&victim_slot).copied(),
            "the IN-SESSION removal freed slot {victim_slot}, so the guard at \
             `storage_bucket_internals.rs:4086` no longer holds a slot while a tombstone names it \
             and this test's account of which path breaks an ordinal is wrong"
        );

        // THE CONTROL, AND IT IS THE HALF THAT MAKES THE RELOAD ASSERTION MEAN ANYTHING: the
        // stored form carries the positions. Taken before the dump, off the object list's own
        // `Serialize`, so what the reload discards is known to have been written.
        let stored = stored_object_index(&engine, routing_bucket);
        println!("  the stored object list:    {stored:?}");
        assert_eq!(
            Some(Some(victim_id)),
            stored.get(victim_slot).copied(),
            "the stored object list does not carry the victim at position {victim_slot}, so the \
             positions were never durable here and the reload cannot be said to discard them"
        );
        assert_eq!(
            Some(Some(witness_id)),
            stored.get(witness_slot).copied(),
            "the stored object list does not carry the witness at position {witness_slot}, so the \
             positions were never durable here and the reload cannot be said to discard them"
        );

        // DUMP, so the base index on disk carries the slots and the tombstone entry.
        engine
            .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
            .expect("APPARATUS: the dump did not complete, so the reload reads nothing");

        // AND ONE MORE WRITE, so a delta exists for the load to fold. `update_bucket_layout` runs
        // on the load path only `if applied`, so a reload with nothing to fold would not reach the
        // scan under test and would pass for the wrong reason.
        write(
            &engine,
            Command::SetAdd {
                key: witness.clone(),
                member: b"m1".to_vec(),
            },
        );

        (victim_slot, witness_slot, stored)
    };

    // THE RELOAD, on a second engine over the same directories, with the scan's own counters read
    // as a DELTA around it. These counters are process-global and monotonic, so a level is not
    // attributable to this test and only a subtraction taken around one action is.
    let before = {
        let probe = engine_on(dir.path());
        probe.object_index_divergence_report()
    };
    let reloaded = engine_on(dir.path());
    load_on(&reloaded, "slot-reuse", 2);
    let after = reloaded.object_index_divergence_report();

    let after_reload = slots_of(&reloaded, routing_bucket);
    let (live_after, tombstoned_after) = entries_naming(&reloaded, routing_bucket, victim_id);
    println!("  after the reload:          {after_reload:?}");
    println!("  the victim's entries: {live_after} live, {tombstoned_after} tombstoned");
    println!(
        "  the scan, across the load: rebuilds +{}, reconstructs +{}, list_held_an_object_no_page_names +{}",
        after.rebuilds.saturating_sub(before.rebuilds),
        after.reconstructs.saturating_sub(before.reconstructs),
        after
            .list_held_an_object_no_page_names
            .saturating_sub(before.list_held_an_object_no_page_names)
    );

    // THE ENTRY IS STILL THERE. Without this a freed slot would be free for honest reasons and
    // nothing durable would be naming it.
    assert!(
        tombstoned_after >= 1,
        "the victim's tombstone entry did not survive the reload, so no durable entry is naming \
         slot {victim_slot} and its fate harms nothing"
    );

    // THE REFUTATION, AND IT IS ABOUT THE LIVE OBJECT AS WELL AS THE TOMBSTONED ONE. The stored
    // array put the witness at `witness_slot`; the reload did not.
    assert_ne!(
        Some(witness_id),
        after_reload.get(&witness_slot).copied(),
        "the live witness came back at its stored slot {witness_slot}, so this reload preserved \
         positions and the wider refutation in this test's doc comment does not hold. If this \
         assertion fails, the narrower tombstone-only ground is the one to re-drive"
    );
    assert_ne!(
        Some(victim_id),
        after_reload.get(&victim_slot).copied(),
        "slot {victim_slot} still names the victim after the reload, so the tombstoned object \
         kept its slot too and nothing here moved at all"
    );
    assert_ne!(
        stored,
        stored_object_index(&reloaded, routing_bucket),
        "the object list came back byte-for-byte as it was stored, so the reload preserved it and \
         the symptom above must have another cause"
    );

    // AND THE BRANCH IS *NOT* ASSERTED, WHICH IS ITSELF A MEASUREMENT.
    //
    // A draft asserted `reconstructs` moved, on the reasoning that only a reconstruct re-hands
    // every slot. It read `rebuilds +5, reconstructs +0, list_held_an_object_no_page_names +1`:
    // the load took the RECONCILE branch, and one id was dropped somewhere. FIVE BUCKETS WERE
    // SCANNED BY THE ONE LOAD, and these counters are shard-wide, so a delta of +1 cannot be
    // attributed to the bucket under test -- it may have been any of the five. The counters are
    // printed for the record and nothing is asserted from them, because an assertion on an
    // unattributable delta would be a claim this fixture cannot support.
    //
    // THE REFUTATION DOES NOT NEED THE BRANCH. The stored array and the reloaded array are both
    // read directly above, and they differ in the position of a LIVE id. Whatever combination of
    // the reconcile's `remove`, `drop_trailing_placeholders` and `settle`'s collapse-to-`One`
    // produced it, a stored ordinal is wrong after the restart.
    // THE DENOMINATOR, WHICH IS THE ONE THING THESE COUNTERS CAN SAY HERE: the scan ran at all
    // during the load. Without it, a reload that never reached `update_bucket_layout` would look
    // the same from outside as one that did.
    assert!(
        after.rebuilds > before.rebuilds,
        "the load scanned no bucket at all (rebuilds {} -> {}), so whatever moved the array was \
         not this scan and the account above is about the wrong code",
        before.rebuilds,
        after.rebuilds
    );

    // AND THE CONSEQUENCE: the next object into this bucket is handed a position a stored ordinal
    // could already be naming.
    write(
        &reloaded,
        Command::SetAdd {
            key: successor.clone(),
            member: b"m0".to_vec(),
        },
    );
    let after_add = slots_of(&reloaded, routing_bucket);
    println!("  after the successor write: {after_add:?}");
    let reused: Vec<usize> = after_add
        .iter()
        .filter(|(slot, id)| {
            **id == successor_id && stored.get(**slot).copied().flatten().is_some()
        })
        .map(|(slot, _)| *slot)
        .collect();
    assert!(
        !reused.is_empty(),
        "the successor landed outside every position the stored array had filled, so a stored \
         ordinal would dangle rather than resolve to the wrong object. Both are fatal, but this \
         test's account says the stronger thing"
    );
    println!(
        "  the successor took slot(s) {reused:?}, which the stored array had filled with {:?}",
        reused
            .iter()
            .map(|slot| stored.get(*slot).copied().flatten())
            .collect::<Vec<_>>()
    );

    println!(
        "\n  REFUTED: the stored array was {stored:?} and the restart handed back {:?}. A LIVE id \
         changed position, so no entry's slot survives a restart and an ordinal stored on an entry \
         names a different object or none.",
        stored_object_index(&reloaded, routing_bucket)
    );
}

// =================================================================================================
// GROUND 2 -- THE ENTRY IS THE ONLY DURABLE HOME OF THE KEY'S CHARACTERS
// =================================================================================================

/// A DISTINCTIVE KEY, so a hit is this key and not an incidental byte run.
const PRESENT_KEY: &str = "zq7-sole-home-key-present-zq7";
/// THE SAME SHAPE, NEVER WRITTEN. The matcher's negative control.
const ABSENT_KEY: &str = "zq7-sole-home-key-absent-zq7";

/// rust-internal: serializes this crate's own bucket node and reads its own index log file
#[test]
fn the_page_entry_is_the_only_durable_home_of_an_object_keys_characters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, "sole-home", 1);

    for key in std::iter::once(PRESENT_KEY.to_string())
        .chain((0..40usize).map(|index| format!("sole-home-filler-{index:04}")))
    {
        write(
            &engine,
            Command::StringSet {
                key,
                value: vec![b'v'; 64],
            },
        );
    }

    // ---------------------------------------------------------------------------------------------
    // ARM 1 -- WITHIN THE DURABLE NODE SHAPE, ONLY THE PAGE ROWS CARRY TEXT.
    //
    // MEASURED OFF THE SERIALIZED NODE AND NOT OFF THE FIELD LIST, because the field list is prose.
    // It is also not measured off the FILE: the stored index is COMPRESSED -- `shard-1.index.json`
    // begins `TSIDX` -- so a byte scan of it finds no key at any width, which is how a first draft
    // of this test failed its own positive control while the store certainly held the key. The
    // node's own `Serialize` output is what the compressor is handed, and that is what is measured.
    // ---------------------------------------------------------------------------------------------
    let routing_bucket = routing_bucket_of(PRESENT_KEY);
    let node_wire = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&SHARD).expect("shard is loaded");
        let bucket = shard
            .bucket_index
            .bucket_map
            .get(&routing_bucket)
            .expect("APPARATUS: the bucket holding the key under test is not resident");
        serde_json::to_value(bucket).expect("a bucket node serializes to its stored shape")
    };
    let members = node_wire.as_object().expect(
        "APPARATUS: a bucket node does not serialize as an object, so its members cannot be \
         taken apart",
    );

    println!("\n=== WHICH MEMBER OF A STORED BUCKET NODE CARRIES THE KEY'S CHARACTERS ===");
    let mut carrying: Vec<String> = Vec::new();
    for (name, value) in members {
        let text = serde_json::to_string(value).expect("a node member serializes");
        let has_present = text.contains(PRESENT_KEY);
        assert!(
            !text.contains(ABSENT_KEY),
            "THE MATCHER'S NEGATIVE CONTROL FAILED: member `{name}` contains a key that was never \
             written, so a hit in this scan does not mean the characters are there"
        );
        println!(
            "  {name:<22} {:>7} bytes  key: {}",
            text.len(),
            if has_present { "PRESENT" } else { "absent " }
        );
        if has_present {
            carrying.push(name.clone());
        }
    }

    assert!(
        !carrying.is_empty(),
        "THE POSITIVE CONTROL FAILED: a key that WAS written is in none of the {} members of the \
         node that holds it, so this scan cannot tell 'only the page rows carry it' from 'the \
         scan finds nothing'",
        members.len()
    );
    assert_eq!(
        vec!["page_index".to_string()],
        carrying,
        "the key's characters are carried by {carrying:?} rather than by `page_index` alone, so \
         the page entry is NOT the only member of a stored node that holds them and this ground \
         does not hold"
    );

    // AND WHAT THE OBJECT LIST HOLDS INSTEAD: numbers and nulls. A slot resolves to one of those
    // for nothing, and `stable_block_object_id` is FNV-1a, so it does not invert back to the
    // characters. This is why a position-derived ordinal cannot serve a character reader even
    // where the position is sound.
    let object_index = members
        .get("object_index")
        .expect("APPARATUS: a stored node has no `object_index` member")
        .as_array()
        .expect("APPARATUS: `object_index` does not serialize as an array");
    assert!(
        !object_index.is_empty(),
        "APPARATUS: the object list of the bucket under test is empty, so there is no slot to say \
         anything about"
    );
    for element in object_index {
        assert!(
            element.is_number() || element.is_null(),
            "the object list holds {element}, which is neither an id nor a placeholder, so it may \
             carry something a reader could use for the characters after all"
        );
    }
    let expected = object_id_of("string", PRESENT_KEY);
    assert!(
        slots_of(&engine, routing_bucket)
            .values()
            .any(|id| *id == expected),
        "APPARATUS: object {expected:#018x} is not in bucket {routing_bucket}'s list, so the slot \
         this arm is about does not exist"
    );
    println!(
        "  object_index holds {} slot(s), all ids or placeholders",
        object_index.len()
    );

    // ---------------------------------------------------------------------------------------------
    // ARM 2 -- AND THE INDEX LOG, WHICH IS THE ONLY OTHER DURABLE ARTEFACT THAT CARRIES THE TEXT,
    // DOES NOT CARRY IT HERE.
    //
    // WHAT THIS ARM IS AND IS NOT. `IndexItem::object_key` is an `Arc<str>` and the row really does
    // hold the characters, so the log has to be accounted for. It is not a second home for two
    // reasons, and only the second is measured here:
    //
    //   * IT IS DOWNSTREAM OF THE FIELD. `collect_command_index_items_for` builds every row from
    //     the resident entry -- `object_key: page.object_key.clone()` at `engine.rs:3328` -- so a
    //     row cannot hold what the entry does not. That is a COMPILE-TIME dependency and the
    //     compiler census is its proof: replacing the field reddens that construction (E0560).
    //   * IT DOES NOT OUTLIVE A DUMP. `dump_and_reclaim_index_logs_with_min_reclaimable` gives the
    //     log back once the base index reflects it.
    //
    // MEASURED AS CONTENT, NOT AS SIZE. A first draft bounded the log's BYTES against the
    // characters written and its own control refused the bound: the log read 0 bytes before the
    // dump as well as after, because this fixture's rows are not on disk until the dump writes
    // them. So the claim here is the narrow one that the file supports -- the log on disk does not
    // contain this key -- and the matcher is controlled on both sides: it finds the key in the
    // `page_index` text above, and finds the never-written key nowhere.
    // ---------------------------------------------------------------------------------------------
    engine
        .dump_and_reclaim_index_logs_with_min_reclaimable(SHARD, 0)
        .expect("APPARATUS: the dump did not complete, so no reclaim happened");

    let log_path = dir.path().join("indexes").join("shard-1.indexlog.bin");
    let log = std::fs::read(&log_path).unwrap_or_default();
    println!("\n=== THE INDEX LOG, AFTER THE DUMP AND THE RECLAIM ===");
    println!("  {} is {} bytes", log_path.display(), log.len());

    // AND THE VACUITY IS NAMED RATHER THAN HIDDEN. On this fixture the log reads ZERO bytes after
    // the reclaim, so "it does not carry this key" is satisfied because it carries NOTHING -- which
    // is the stronger statement and the one actually being made. The length is printed with the
    // verdict so a fixture that left rows behind would read differently rather than silently turn
    // this into a content search over a file that happens to be empty.
    let needle = PRESENT_KEY.as_bytes();
    let holds_key =
        needle.len() <= log.len() && log.windows(needle.len()).any(|window| window == needle);
    assert!(
        !holds_key,
        "the index log still carries the key's characters after a reclaim, so it IS a durable \
         second home for them and this ground does not hold"
    );
    println!(
        "  so the log holds {}",
        if log.is_empty() {
            "nothing at all -- the arm above is a vacuity check, which is the stronger claim"
        } else {
            "rows, and none of them carries this key"
        }
    );

    // THE MATCHER'S POSITIVE CONTROL, over the same code path, against text that DOES hold the
    // key: without it "not found in the log" is indistinguishable from a matcher that never finds
    // anything.
    let page_index_text = serde_json::to_string(
        members
            .get("page_index")
            .expect("APPARATUS: a stored node has no `page_index` member"),
    )
    .expect("the page rows serialize");
    let haystack = page_index_text.as_bytes();
    assert!(
        needle.len() <= haystack.len()
            && haystack
                .windows(needle.len())
                .any(|window| window == needle),
        "THE MATCHER'S POSITIVE CONTROL FAILED: the same byte search does not find the key in the \
         page rows that Arm 1 just showed carry it, so its absence from the log says nothing"
    );
    println!("  the same byte search finds the key in the page rows, so the absence above is real");

    println!(
        "\n  REFUSED: the characters have one durable home and it is the field this leg removes. A \
         slot hands a reader {expected:#018x}, which is a hash of them."
    );
}
