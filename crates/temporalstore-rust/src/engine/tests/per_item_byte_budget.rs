// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A BYTE BUDGET for the structures that exist once per stored item.
//!
//! Most of this engine reaches for a 64-bit integer by default and that is the right default: a
//! sequence number, a hash, a timestamp and a byte count all want the full width, and a structure
//! that exists once per shard can be as fat as it likes. `ShardState` is 1,880 bytes and there is
//! one of it. What is different about the handful of structures below is that their count is the
//! CORPUS: one per stored address, one per page-index entry, one per routing bucket. At those
//! counts a byte is a megabyte, and nothing in this tree stopped one of them growing.
//!
//! WHAT THIS MODULE IS FOR. Two things, and the second is the durable one:
//!
//!   * `what_the_per_item_structures_cost_at_two_corpus_sizes` measures `size_of` x COUNT at two
//!     corpus sizes ten times apart, so a structure is ranked by what it actually costs the store
//!     rather than by how wide it looks. A fat struct with one instance is not a finding.
//!   * `every_per_item_structure_states_its_width_and_its_padding` pins each width against a
//!     literal and reports the FIELD SUM beside it. The literals are the budget. A field added to
//!     any of these structures fails this test by name, which is the thing that did not exist
//!     before: the widths below have moved several times and nothing ever failed.
//!
//! WIDTH IS NOT THE SAME PROBLEM AS PADDING, and the two want opposite fixes. Rust reorders
//! fields, so a structure whose size exceeds its field sum is wasting ALIGNMENT, and narrowing a
//! field in it changes nothing at all -- `BlockLookupRef` is 12 bytes of field in 16 bytes of
//! struct and no narrowing of either field moves it. A structure whose fields fill it is wasting
//! WIDTH, and narrowing one helps only if the sum crosses an 8-byte boundary. `BlockAddress` is
//! the only candidate here where it does, and only when TWO fields narrow together: one alone
//! takes 53 bytes of field to 49 and leaves the struct at 56.
//!
//! THE COUNTS ARE MEASURED, NOT ASSUMED. Every denominator below is read off a seeded shard and
//! asserted non-zero before anything is divided by it, because a walk over an empty shard reports
//! "0 bytes over 0 items", which reads exactly like a structure that costs nothing.
#![allow(clippy::all)]
use super::*;
use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::{BlockAddress, BlockStoreSlabDescriptor};
use crate::engine::state::{
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs, BucketLayoutState, BucketNode,
    ComponentBlocks, ComponentList, DirtyKeySet, ObjectBlockRefs, ObjectIndex, WalResidentBlock,
};
use crate::index_log::{IndexItem, IndexItemKind, SlabCatalogEntry};

/// One structure in the budget: what it costs, and what its own fields add up to.
///
/// `fields` is written out as a sum of `size_of` over the DECLARED field types rather than as a
/// number, so it is a statement about the declaration and moves when the declaration does.
struct Budgeted {
    name: &'static str,
    size: usize,
    align: usize,
    /// For a struct, the sum of its field widths. For an ENUM, the width of its widest arm --
    /// so the slack reported beside it is the discriminant plus any alignment, not padding
    /// alone. `ObjectIndex` reads as 8 bytes of arm in 16 for exactly that reason, and it is not
    /// waste that a narrower field could reclaim.
    fields: usize,
    /// Whether the count of this structure grows with the corpus. A `false` row is a CONTROL:
    /// it is here to be ranked last, and it is the reason "rank by width" is the wrong ranking.
    per_item: bool,
}

impl Budgeted {
    fn padding(&self) -> usize {
        self.size.saturating_sub(self.fields)
    }
}

/// THE BUDGET. Each `size` literal is the ceiling; each `fields` expression is what the
/// declaration adds up to today.
fn budget() -> Vec<Budgeted> {
    let arc_str = size_of::<Arc<str>>();
    let opt_arc_str = size_of::<Option<Arc<str>>>();
    let opt_u64 = size_of::<Option<u64>>();
    let string = size_of::<String>();
    vec![
        Budgeted {
            name: "BlockAddress",
            size: size_of::<BlockAddress>(),
            align: align_of::<BlockAddress>(),
            // block_slab_id, offset : u64 x 2
            // length, block_id      : u32 x 2
            // object_id, generation : u64 x 2
            // routing_bucket        : u32
            // present               : u8
            fields: 4 * size_of::<u64>() + 3 * size_of::<u32>() + size_of::<u8>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndex",
            size: size_of::<BlockIndex>(),
            align: align_of::<BlockIndex>(),
            // object_key, model_id : Arc<str> x 2
            // component            : Option<Arc<str>>
            // address              : BlockAddress
            // dirty/deleted/log_backed : bool x 3
            fields: 2 * arc_str + opt_arc_str + size_of::<BlockAddress>() + 3 * size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockIndexMap",
            size: size_of::<BlockIndexMap>(),
            align: align_of::<BlockIndexMap>(),
            // Widest arm: One(u64, BlockIndex). The discriminant rides a niche in the page.
            fields: size_of::<u64>() + size_of::<BlockIndex>(),
            per_item: true,
        },
        Budgeted {
            name: "BucketNode",
            size: size_of::<BucketNode>(),
            align: align_of::<BucketNode>(),
            // routing_bucket u32, layout, five bools, ttl_ms Option<u64>,
            // four u64 sequences, two ObjectIndex, one BlockIndexMap
            fields: size_of::<u32>()
                + size_of::<BucketLayoutState>()
                + 5 * size_of::<bool>()
                + opt_u64
                + 4 * size_of::<u64>()
                + 2 * size_of::<ObjectIndex>()
                + size_of::<BlockIndexMap>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockLookupRef",
            size: size_of::<BlockLookupRef>(),
            align: align_of::<BlockLookupRef>(),
            fields: size_of::<u32>() + size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "BlockRefs",
            size: size_of::<BlockRefs>(),
            align: align_of::<BlockRefs>(),
            // Widest arm: Many(Vec<BlockLookupRef>).
            fields: size_of::<Vec<BlockLookupRef>>(),
            per_item: true,
        },
        Budgeted {
            name: "ComponentBlocks",
            size: size_of::<ComponentBlocks>(),
            align: align_of::<ComponentBlocks>(),
            fields: opt_arc_str + size_of::<BlockRefs>(),
            per_item: true,
        },
        Budgeted {
            name: "ComponentList",
            size: size_of::<ComponentList>(),
            align: align_of::<ComponentList>(),
            // Widest arm: One(ComponentBlocks).
            fields: size_of::<ComponentBlocks>(),
            per_item: true,
        },
        Budgeted {
            name: "ObjectBlockRefs",
            size: size_of::<ObjectBlockRefs>(),
            align: align_of::<ObjectBlockRefs>(),
            fields: size_of::<ComponentList>(),
            per_item: true,
        },
        Budgeted {
            name: "ObjectIndex",
            size: size_of::<ObjectIndex>(),
            align: align_of::<ObjectIndex>(),
            // Widest arm: Many(Box<BTreeSet<u64>>), which is one pointer.
            fields: size_of::<Box<std::collections::BTreeSet<u64>>>(),
            per_item: true,
        },
        Budgeted {
            name: "DirtyKeySet",
            size: size_of::<DirtyKeySet>(),
            align: align_of::<DirtyKeySet>(),
            // Widest arm: One(Arc<str>).
            fields: arc_str,
            per_item: true,
        },
        Budgeted {
            name: "WalResidentBlock",
            size: size_of::<WalResidentBlock>(),
            align: align_of::<WalResidentBlock>(),
            fields: 2 * size_of::<u64>(),
            per_item: true,
        },
        Budgeted {
            name: "IndexItem",
            size: size_of::<IndexItem>(),
            align: align_of::<IndexItem>(),
            // kind, routing_bucket, three String handles, Option<String> component,
            // object_id, block_id, Option<BlockAddress>, size, in_log, deleted
            fields: size_of::<IndexItemKind>()
                + size_of::<u32>()
                + 3 * string
                + size_of::<Option<String>>()
                + 2 * size_of::<u64>()
                + size_of::<Option<BlockAddress>>()
                + size_of::<u64>()
                + 2 * size_of::<bool>(),
            per_item: true,
        },
        Budgeted {
            name: "SlabCatalogEntry",
            size: size_of::<SlabCatalogEntry>(),
            align: align_of::<SlabCatalogEntry>(),
            fields: 4 * size_of::<u64>()
                + size_of::<crate::index_log::SlabCatalogState>()
                + 4 * opt_u64,
            per_item: false,
        },
        Budgeted {
            name: "BlockStoreSlabDescriptor",
            size: size_of::<BlockStoreSlabDescriptor>(),
            align: align_of::<BlockStoreSlabDescriptor>(),
            // Two slab ids, two byte counts and the readable prefix; the lifecycle state; six
            // optional u64s (created, updated, first/last block id, verified mtime, first error
            // offset); the corruption flag; and the first error text.
            fields: 5 * size_of::<u64>()
                + size_of::<crate::block_store::BlockStoreSlabState>()
                + 6 * opt_u64
                + size_of::<bool>()
                + size_of::<Option<String>>(),
            per_item: false,
        },
        // CONTROLS. One instance each, per shard, and the widest things in the engine. They are
        // in the table so the ranking below has something to rank them BELOW: if this module
        // ordered by width instead of by width x count, these two would head the list and the
        // whole exercise would point at the wrong structures.
        Budgeted {
            name: "ShardState (control: one per shard)",
            size: size_of::<crate::engine::state::ShardState>(),
            align: align_of::<crate::engine::state::ShardState>(),
            fields: 0,
            per_item: false,
        },
        Budgeted {
            name: "CoreIndex (control: one per shard)",
            size: size_of::<crate::engine::state::CoreIndex>(),
            align: align_of::<crate::engine::state::CoreIndex>(),
            fields: 0,
            per_item: false,
        },
    ]
}

/// THE BUDGET, PINNED. Every per-item width, with its field sum and its padding beside it.
///
/// The literals here and the `const _: () = assert!(size_of::<T>() == N)` beside each declaration
/// say the same thing twice on purpose. The const assertion is what stops a growth landing: it is
/// a BUILD failure, so it cannot be skipped, filtered out or marked ignored. This test is what
/// says WHY each number is what it is, and separates the two different wastes -- a field that is
/// wider than its range needs, and the bytes the aligner inserted between fields -- because they
/// do not have the same fix and one of them has no fix at this layer at all.
#[test]
fn every_per_item_structure_states_its_width_and_its_padding() {
    let rows = budget();
    assert!(
        rows.len() >= 15,
        "the budget lists {} structures; it was written with 17 and a shrinking list is how a \
         structure stops being watched",
        rows.len()
    );

    println!(
        "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
        "structure", "size", "align", "fields", "slack", "counted per"
    );
    for row in &rows {
        println!(
            "{:<38} {:>6} {:>6} {:>7} {:>8}  {}",
            row.name,
            row.size,
            row.align,
            if row.fields == 0 { 0 } else { row.fields },
            if row.fields == 0 { 0 } else { row.padding() },
            if row.per_item { "stored item" } else { "shard or slab" }
        );
    }

    // --- The pinned widths. ---
    assert_eq!(48, size_of::<BlockAddress>(), "BlockAddress width moved");
    assert_eq!(104, size_of::<BlockIndex>(), "BlockIndex width moved");
    assert_eq!(112, size_of::<BlockIndexMap>(), "BlockIndexMap width moved");
    assert_eq!(208, size_of::<BucketNode>(), "BucketNode width moved");
    assert_eq!(16, size_of::<BlockLookupRef>(), "BlockLookupRef width moved");
    assert_eq!(24, size_of::<BlockRefs>(), "BlockRefs width moved");
    assert_eq!(40, size_of::<ComponentBlocks>(), "ComponentBlocks width moved");
    assert_eq!(40, size_of::<ComponentList>(), "ComponentList width moved");
    assert_eq!(40, size_of::<ObjectBlockRefs>(), "ObjectBlockRefs width moved");
    assert_eq!(16, size_of::<ObjectIndex>(), "ObjectIndex width moved");
    assert_eq!(24, size_of::<DirtyKeySet>(), "DirtyKeySet width moved");
    assert_eq!(16, size_of::<WalResidentBlock>(), "WalResidentBlock width moved");
    assert_eq!(184, size_of::<IndexItem>(), "IndexItem width moved");
    assert_eq!(104, size_of::<SlabCatalogEntry>(), "SlabCatalogEntry width moved");
    assert_eq!(
        168,
        size_of::<BlockStoreSlabDescriptor>(),
        "BlockStoreSlabDescriptor width moved"
    );

    // --- The field sums, so a width change says whether it was a field or the aligner. ---
    for row in &rows {
        if row.fields == 0 {
            continue;
        }
        assert!(
            row.fields <= row.size,
            "{}: its fields add up to {} inside a {}-byte struct, which is impossible -- the \
             field list in this module has drifted from the declaration",
            row.name,
            row.fields,
            row.size
        );
    }

    // --- Non-vacuity: the padding arithmetic must discriminate, not report a constant. ---
    let with_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() > 0).count();
    let without_padding = rows.iter().filter(|r| r.fields > 0 && r.padding() == 0).count();
    assert!(
        with_padding > 0 && without_padding > 0,
        "the padding column reports {with_padding} padded and {without_padding} unpadded \
         structures; with either at zero it is a constant and says nothing"
    );

    // --- The two named cases, so the classification itself is guarded. ---
    // WIDTH-BOUND: the fields fill the struct, so narrowing one can move it.
    let address = rows.iter().find(|r| r.name == "BlockAddress").expect("row");
    assert!(
        address.padding() < 8,
        "BlockAddress carries {} bytes of padding; at 8 or more it is alignment-bound and the \
         narrowing this module justifies would not have moved it",
        address.padding()
    );
    // ALIGNMENT-BOUND: a quarter of it is padding and no narrowing of either field helps.
    let lookup = rows.iter().find(|r| r.name == "BlockLookupRef").expect("row");
    assert_eq!(
        4,
        lookup.padding(),
        "BlockLookupRef is the worked example of an alignment problem: 12 bytes of field in 16. \
         If that is no longer true the module's own explanation is stale"
    );
}

// -------------------------------------------------------------------------------------------
// TRUNCATION. The two narrowed fields, at and past their boundary.
// -------------------------------------------------------------------------------------------

/// A LENGTH THAT DOES NOT FIT SATURATES; IT NEVER WRAPS.
///
/// This is the mutant that matters for this change. `length` was a `u64` and is a `u32`; the
/// conversion sits in `BlockAddress::from_parts`, which every construction goes through. A
/// conversion written `as u32` compiles, passes every existing test, and turns a 4 GiB length
/// into a small one -- a read that then returns the wrong bytes with no error anywhere. The
/// saturating form cannot do that: what comes back is larger than any length the encoder will
/// ever accept, so it reads as broken rather than as plausible.
///
/// `BLOCK_RECORD_LENGTH_MASK` is why this is a boundary and not a cliff: `encode_block_record`
/// REFUSES a record above 0x3FFF_FFFF bytes, so a real length is at most 2^30 plus a header --
/// a quarter of what the field now holds.
#[test]
fn a_length_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let just_under = BlockAddress::from_parts(1, 0, u64::from(u32::MAX) - 1, None, None, None, None);
    assert_eq!(
        u64::from(u32::MAX) - 1,
        just_under.length(),
        "a length inside the field must round-trip exactly"
    );

    for over in [
        u64::from(u32::MAX) + 1,
        u64::from(u32::MAX) + 2,
        1u64 << 32,
        (1u64 << 32) + 7,
        u64::MAX,
    ] {
        let address = BlockAddress::from_parts(1, 0, over, None, None, None, None);
        assert_eq!(
            u64::from(u32::MAX),
            address.length(),
            "a length of {over} must saturate to {}, not wrap to {}",
            u32::MAX,
            over as u32
        );
        // The discriminating half: for every value whose low 32 bits are NOT already the
        // saturation value, truncation and saturation give different answers, and this is the
        // one that fails under `as u32`. `u64::MAX` is excluded because its low bits ARE
        // `u32::MAX`, so it cannot tell the two apart -- a case that passes either way is not a
        // test of which one ran.
        if over as u32 != u32::MAX {
            assert_ne!(
                over as u32 as u64,
                address.length(),
                "a length of {over} came back as its low 32 bits -- that is the truncation this \
                 field was narrowed under the promise of not doing"
            );
        }
    }

    // And the saturated value is outside what the encoder will ever produce, so it cannot be
    // mistaken for a real length.
    assert!(
        u64::from(u32::MAX) > u64::from(crate::block_store::BLOCK_RECORD_LENGTH_MASK),
        "the saturation value must sit above the largest length a record can hold, or a \
         saturated address is indistinguishable from a valid one"
    );
}

/// A BLOCK ID THAT DOES NOT FIT SATURATES TOO, and the field has four billion times the headroom
/// the write path allows: `encode_block_record` refuses a block id above `u16::MAX`.
#[test]
fn a_block_id_that_does_not_fit_the_field_saturates_rather_than_wrapping() {
    let real = BlockAddress::from_parts(1, 0, 64, Some(u64::from(u16::MAX)), None, None, None);
    assert_eq!(
        Some(u64::from(u16::MAX)),
        real.block_id(),
        "the largest block id the encoder accepts must round-trip exactly"
    );

    for over in [u64::from(u32::MAX) + 1, 1u64 << 33, u64::MAX] {
        let address = BlockAddress::from_parts(1, 0, 64, Some(over), None, None, None);
        assert_eq!(
            Some(u64::from(u32::MAX)),
            address.block_id(),
            "a block id of {over} must saturate, not wrap to {}",
            over as u32
        );
    }

    // The setter is the second write path into the same field and has to agree with the first.
    //
    // THE VALUE IS CHOSEN SO THE TWO ANSWERS DIFFER. `u64::MAX` cannot test this: its low 32 bits
    // ARE `u32::MAX`, so truncation and saturation agree on it and a setter written `as u32`
    // passes. A mutation run scored exactly that -- the setter truncating survived every test
    // here -- and this is the value that kills it.
    const OVER: u64 = (1u64 << 33) + 5;
    assert_ne!(
        u64::from(OVER as u32),
        u64::from(u32::MAX),
        "the probe value must distinguish truncation from saturation, or this test cannot fail"
    );
    let mut address = BlockAddress::from_parts(1, 0, 64, None, None, None, None);
    address.set_block_id(Some(OVER));
    assert_eq!(
        Some(u64::from(u32::MAX)),
        address.block_id(),
        "the setter must saturate too; it answered with the low 32 bits"
    );
    address.set_block_id(None);
    assert_eq!(None, address.block_id(), "clearing the field must still clear it");

    // And the constructor, on the same discriminating value.
    let built = BlockAddress::from_parts(1, 0, 64, Some(OVER), None, None, None);
    assert_eq!(Some(u64::from(u32::MAX)), built.block_id(), "the constructor must saturate");
}

/// `set_length` IS A WRITE PATH, and a setter that quietly drops its write is invisible to every
/// test that only ever reads a length back out of a constructor.
///
/// The slab inspector is its one production caller -- it walks a slab and stamps each record's
/// framed length onto the address it reports -- so a no-op here makes an inspection report every
/// record as zero-length. A mutation run found this uncovered.
#[test]
fn setting_a_length_after_the_fact_writes_it_and_saturates_it() {
    let mut address = BlockAddress::from_parts(1, 0, 0, None, None, None, None);
    assert_eq!(0, address.length(), "it starts at the length it was built with");

    address.set_length(4_096);
    assert_eq!(4_096, address.length(), "the setter must actually write");

    address.set_length(17);
    assert_eq!(17, address.length(), "and must overwrite what was there");

    // The same discriminating value as above: low bits that are not the saturation value.
    const OVER: u64 = (1u64 << 33) + 5;
    address.set_length(OVER);
    assert_eq!(
        u64::from(u32::MAX),
        address.length(),
        "the setter must saturate, not keep the low 32 bits"
    );
}

/// THE STORED FORM DID NOT MOVE.
///
/// `BlockAddress` serializes through `BlockAddressWire`, which holds `length` as a `u64` and
/// `block_id` as an `Option<u64>` and is untouched by this change. That is what makes the
/// narrowing resident-only: an index written before it reads back identically, and an index
/// written after it is byte-identical to one written before. If this test fails, a stored format
/// has moved and the change is not what its own pull request says it is.
#[test]
fn narrowing_the_resident_fields_did_not_move_the_stored_form() {
    let address = BlockAddress::from_parts(
        9_876_543_210,
        1_234_567,
        1_048_576,
        Some(7),
        Some(0xDEAD_BEEF_CAFE_F00D),
        Some(4_294_967_290),
        Some(0x0123_4567_89AB_CDEF),
    );
    let json = serde_json::to_string(&address).expect("an address serializes");
    assert_eq!(
        "{\"ps\":9876543210,\"o\":1234567,\"l\":1048576,\"pi\":7,\"oi\":16045690984503111693,\
         \"rs\":4294967290,\"g\":81985529216486895,\"h\":null}",
        json,
        "the stored spelling of an address moved"
    );
    let back: BlockAddress = serde_json::from_str(&json).expect("it reads back");
    assert_eq!(address, back, "an address must round-trip through its stored form");

    // A stored length or block id above the resident field is already outside what the encoder
    // can have written, so reading one is reading a corrupt index. It must come back saturated,
    // never truncated: a truncated length is a plausible number and a saturated one is not.
    let wide = "{\"ps\":1,\"o\":0,\"l\":4294967296,\"pi\":4294967296}";
    let read: BlockAddress = serde_json::from_str(wide).expect("a wide stored value still loads");
    assert_eq!(u64::from(u32::MAX), read.length(), "a wide stored length saturates");
    assert_eq!(Some(u64::from(u32::MAX)), read.block_id(), "a wide stored block id saturates");
}

// -------------------------------------------------------------------------------------------
// WHAT THEY COST, AT TWO CORPUS SIZES.
// -------------------------------------------------------------------------------------------

/// How many of each structure a seeded shard actually holds.
#[derive(Default)]
struct ItemCounts {
    records: usize,
    bucket_nodes: usize,
    block_index_entries: usize,
    model_map_addresses: usize,
    object_block_refs: usize,
    component_blocks: usize,
    block_lookup_refs: usize,
    dirty_key_sets: usize,
    wal_resident_blocks: usize,
}

impl ItemCounts {
    /// Every resident `BlockAddress`: the one each model-map entry holds, plus the one inside
    /// every page-index entry. The two are separate copies of the same address, which is why
    /// this structure's count is roughly twice the record count rather than equal to it.
    fn block_addresses(&self) -> usize {
        self.model_map_addresses + self.block_index_entries
    }
}

fn count_items(shard: &crate::engine::state::ShardState) -> ItemCounts {
    let mut counts = ItemCounts::default();
    counts.model_map_addresses = shard.strings.len()
        + shard.features.values().map(|series| series.len()).sum::<usize>()
        + shard.hashes.values().map(|fields| fields.len()).sum::<usize>()
        + shard.sets.values().map(|members| members.len()).sum::<usize>()
        + shard.zsets.values().map(|members| members.len()).sum::<usize>()
        + shard.lists.values().map(|items| items.len()).sum::<usize>();
    counts.records = shard.strings.len() + shard.features.values().map(|s| s.len()).sum::<usize>();
    counts.bucket_nodes = shard.bucket_index.bucket_map.len();
    counts.block_index_entries = shard
        .bucket_index
        .bucket_map
        .values()
        .map(|bucket| bucket.block_index.len())
        .sum();
    counts.object_block_refs = shard.bucket_index.object_block_lookup.len();
    counts.component_blocks = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(|entry| entry.by_component.len())
        .sum();
    counts.block_lookup_refs = shard
        .bucket_index
        .object_block_lookup
        .values()
        .map(|entry| entry.total_refs())
        .sum();
    // One `DirtyKeySet` per dirty BUCKET, and the measured distribution is one key per bucket at
    // both corpus sizes -- `every_dirty_bucket_holds_its_one_key_inline` walks the shard and
    // reports it. So the dirty-object count is the set count here, and the comment says which
    // measurement that leans on rather than assuming it.
    counts.dirty_key_sets = shard.dirty_objects.len();
    counts.wal_resident_blocks = shard.wal_resident_blocks.len();
    counts
}

/// THE MEASUREMENT. `size_of` x count, at two corpus sizes ten times apart.
///
/// Ranked by the product, which is the only ranking that answers the question this module asks.
/// `BlockStoreSlabDescriptor` is the reason: it is 168 bytes against `BlockLookupRef`'s 16, and a
/// store holding a gigabyte of pages has ONE of it and eighty thousand of the other.
///
/// A per-item figure that is FLAT across the two corpus sizes is what makes the total multiply
/// out to any scale. One that grows is a finding in itself, and the per-structure rows say which
/// structure grew.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT across the two arms and asserted below: `tempfile`
/// names every directory with the same number of characters, and a path length that moved between
/// arms would move allocation counts with it.
#[test]
#[ignore = "seeds 8,000 then 80,000 records; run by name"]
fn what_the_per_item_structures_cost_at_two_corpus_sizes() {
    let mut per_record: Vec<(&'static str, f64, f64)> = Vec::new();
    let mut path_lengths: Vec<usize> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        budget_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let counts = count_items(shard);

        // --- Denominators, asserted before anything divides by them. ---
        assert_eq!(
            strings_n + series_keys * series_points,
            counts.records,
            "denominator: the shard must hold every record seeded"
        );
        assert!(
            counts.bucket_nodes > 0,
            "denominator: no routing buckets, so every per-bucket figure below is a zero that \
             means nothing"
        );
        assert!(
            counts.block_index_entries > 0,
            "denominator: no page-index entries, so the structure this module is about is absent"
        );
        assert!(
            counts.object_block_refs > 0 && counts.block_lookup_refs > 0,
            "denominator: the object lookup is empty"
        );

        let rows: Vec<(&'static str, usize, usize)> = vec![
            ("BlockAddress", size_of::<BlockAddress>(), counts.block_addresses()),
            ("BucketNode", size_of::<BucketNode>(), counts.bucket_nodes),
            ("BlockIndex (inside BucketNode)", size_of::<BlockIndex>(), counts.block_index_entries),
            ("BlockLookupRef", size_of::<BlockLookupRef>(), counts.block_lookup_refs),
            ("ComponentBlocks", size_of::<ComponentBlocks>(), counts.component_blocks),
            ("ObjectBlockRefs", size_of::<ObjectBlockRefs>(), counts.object_block_refs),
            ("DirtyKeySet", size_of::<DirtyKeySet>(), counts.dirty_key_sets),
            ("WalResidentBlock", size_of::<WalResidentBlock>(), counts.wal_resident_blocks),
        ];

        let mut ranked = rows.clone();
        ranked.sort_by_key(|(_, width, count)| std::cmp::Reverse(width * count));

        println!("\n=== {label}: per-item structures ranked by size_of x count ===");
        println!("  records={} buckets={} page entries={} model-map addresses={}",
            counts.records, counts.bucket_nodes, counts.block_index_entries,
            counts.model_map_addresses);
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12}",
            "structure", "width", "count", "total bytes", "per record"
        );
        let mut total = 0usize;
        for (name, width, count) in &ranked {
            let bytes = width * count;
            total += bytes;
            println!(
                "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
                name,
                width,
                count,
                bytes,
                bytes as f64 / counts.records as f64
            );
        }
        println!(
            "  {:<32} {:>6} {:>10} {:>12} {:>12.2}",
            "TOTAL (structure payload only)",
            "",
            "",
            total,
            total as f64 / counts.records as f64
        );

        // BlockIndex is NOT added into the total separately: every page entry lives inside a
        // BucketNode's own `BlockIndexMap`, so its bytes are already inside the BucketNode row.
        // Counting it twice would report a win twice.
        let counted: usize = rows
            .iter()
            .filter(|(name, _, _)| *name != "BlockIndex (inside BucketNode)")
            .map(|(_, width, count)| width * count)
            .sum();
        for (name, width, count) in &rows {
            per_record.push((
                name,
                (width * count) as f64 / counts.records as f64,
                counts.records as f64,
            ));
        }
        println!(
            "  payload without double-counting the nested page entries: {} B, {:.2} B/record",
            counted,
            counted as f64 / counts.records as f64
        );
    }

    assert_eq!(
        2,
        path_lengths.len(),
        "both arms must have run, or the flatness claim below compares one arm with itself"
    );
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation counts move with it",
        path_lengths[0], path_lengths[1]
    );

    // --- Flatness. Every per-item figure must hold across a tenfold corpus. ---
    println!("\n=== flatness across a tenfold corpus ===");
    let half = per_record.len() / 2;
    for index in 0..half {
        let (name, small, _) = per_record[index];
        let (name_big, big, _) = per_record[index + half];
        assert_eq!(name, name_big, "the two arms reported structures in different orders");
        let ratio = if small > 0.0 { big / small } else { 1.0 };
        println!("  {name:<32} {small:>8.2} -> {big:>8.2} B/record  ({ratio:.3}x)");
        assert!(
            ratio < 1.25,
            "{name} costs {small:.2} B/record at 8,000 records and {big:.2} at 80,000 ({ratio:.3}x) \
             -- it is growing with the corpus, which is a finding and not a budget"
        );
    }
}

fn budget_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("pages"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

fn budget_seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }
    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }
}
