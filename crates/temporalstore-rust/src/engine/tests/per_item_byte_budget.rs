// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A BYTE BUDGET for the structures that exist once per stored item.
//!
//! Most of this engine reaches for a 64-bit integer by default and that is the right default: a
//! sequence number, a hash, a timestamp and a byte count all want the full width, and a structure
//! that exists once per shard can be as fat as it likes. `ShardState` is 1,888 bytes and there is
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
//! THERE IS A THIRD THING A FIELD CAN BE WASTING, and `BucketNode` was. An `Option<u64>` is 16
//! bytes for 8 bytes of number, because a `u64` has no value it does not use and the
//! discriminant has nowhere to go but a word of its own. That is neither width nor alignment:
//! the number is exactly as wide as it needs to be and the struct has no slack to reclaim. It is
//! a DISCRIMINANT with no niche to sit in, and the fix is to make one -- `Option<NonZeroU64>` is
//! 8. `every_byte_of_the_bucket_node_is_accounted_for` states the whole of that structure field
//! by field, `what_each_declined_shape_of_the_bucket_node_would_cost` prices the three shapes
//! that were considered for it and declined, and
//! `the_stored_spelling_of_a_bucket_node_did_not_move` drives the constraint that decides which
//! of them can be taken at all.
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
    BlockIndex, BlockIndexMap, BlockLookupRef, BlockRefs, BucketLayoutState, BucketNode, BucketTtl,
    ComponentBlocks, ComponentList, DirtyKeySet, ObjectBlockRefs, ObjectIndex, WalResidentBlock,
};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;
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
            // routing_bucket u32, layout, five bools, ttl_ms BucketTtl,
            // four u64 sequences, two ObjectIndex, one BlockIndexMap
            fields: size_of::<u32>()
                + size_of::<BucketLayoutState>()
                + 5 * size_of::<bool>()
                + size_of::<BucketTtl>()
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
    assert_eq!(200, size_of::<BucketNode>(), "BucketNode width moved");
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

// -------------------------------------------------------------------------------------------
// THE WIDEST PER-ITEM STRUCTURE, BYTE BY BYTE.
//
// `BucketNode` exists once per routing bucket, which is once per key that routes to a bucket of
// its own, and the ranking above puts it first at both corpus sizes. This section says what its
// bytes ARE -- every field with its own width and alignment, the sum beside `size_of`, and the
// slack named separately -- and then prices the three shapes that were considered for it and
// declined, so the decline is a number rather than an opinion.
// -------------------------------------------------------------------------------------------

/// One declared field of `BucketNode`.
///
/// The widths are `size_of` over the DECLARED types, never literals, so this table moves when the
/// declaration does instead of going quietly stale beside it.
struct Field {
    name: &'static str,
    ty: &'static str,
    size: usize,
    align: usize,
}

fn bucket_node_fields() -> Vec<Field> {
    macro_rules! field {
        ($name:literal, $ty:ty) => {
            Field {
                name: $name,
                ty: stringify!($ty),
                size: size_of::<$ty>(),
                align: align_of::<$ty>(),
            }
        };
    }
    vec![
        field!("routing_bucket", u32),
        field!("layout", BucketLayoutState),
        field!("dirty", bool),
        field!("deleted", bool),
        field!("meta_loaded", bool),
        field!("loading", bool),
        field!("in_memory", bool),
        field!("ttl_ms", BucketTtl),
        field!("dirty_generation", u64),
        field!("first_dirty_wal_sequence", u64),
        field!("first_dirty_index_log_sequence", u64),
        field!("last_dump_sequence", u64),
        field!("object_index", ObjectIndex),
        field!("deleted_object_index", ObjectIndex),
        field!("block_index", BlockIndexMap),
    ]
}

/// EVERY BYTE OF THE BUCKET NODE, ACCOUNTED FOR.
///
/// Fifteen fields, their widths summed, and the difference against `size_of` named as what it is:
/// the aligner's, not any field's. The sum is the discriminating half. A width that is stated
/// without its field sum cannot tell a structure that is FULL from one that is half padding, and
/// those two want opposite fixes -- one wants a narrower field, the other cannot be helped by any
/// narrowing at all.
///
/// HOW RUST LAYS THIS OUT, and it is the whole explanation of the number. Fields reorder freely,
/// so the layout is two groups: everything of alignment 8 packs solid, and everything smaller
/// fills the tail, which is then rounded up to the struct's own alignment. Here that is 184 bytes
/// of eight-aligned field and 10 bytes of small field rounded to 16. The consequence is blunt and
/// worth stating in a test rather than a comment: NOTHING in the ten-byte tail can be narrowed to
/// any effect -- not the routing bucket, not the layout, not the five flags -- because the tail
/// is already inside a rounding. Only a change that takes the tail to 8 bytes or fewer, or that
/// takes a whole word out of the eight-aligned group, moves this structure at all.
#[test]
fn every_byte_of_the_bucket_node_is_accounted_for() {
    let fields = bucket_node_fields();
    assert_eq!(
        15,
        fields.len(),
        "the field table lists {} fields; `BucketNode` has fifteen and a table that has drifted \
         from the declaration proves nothing about it",
        fields.len()
    );

    println!("\n=== BucketNode, field by field ===");
    println!("  {:<34} {:<20} {:>5} {:>6}", "field", "declared type", "size", "align");
    let mut eight_aligned = 0usize;
    let mut tail = 0usize;
    for field in &fields {
        println!(
            "  {:<34} {:<20} {:>5} {:>6}",
            field.name, field.ty, field.size, field.align
        );
        if field.align == align_of::<BucketNode>() {
            eight_aligned += field.size;
        } else {
            tail += field.size;
        }
    }
    let sum: usize = fields.iter().map(|field| field.size).sum();
    let size = size_of::<BucketNode>();
    let slack = size - sum;
    println!("  {:<34} {:<20} {:>5}", "SUM OF FIELDS", "", sum);
    println!("  {:<34} {:<20} {:>5}", "size_of::<BucketNode>()", "", size);
    println!(
        "  {:<34} {:<20} {:>5}   <- the aligner, not any field",
        "SLACK", "", slack
    );
    println!(
        "  groups: {eight_aligned} B of eight-aligned field + {tail} B of small field, the tail \
         rounded up to {}",
        size - eight_aligned
    );

    assert_eq!(194, sum, "the fields of BucketNode add up to {sum}, not 194");
    assert_eq!(200, size, "BucketNode is {size} bytes wide, not 200");
    assert_eq!(6, slack, "BucketNode carries {slack} bytes of alignment slack, not 6");

    // The layout rule itself, asserted rather than described: the eight-aligned group packs
    // solid and the rest is one rounding.
    let align = align_of::<BucketNode>();
    assert_eq!(8, align, "BucketNode's alignment moved, and the arithmetic below assumes 8");
    assert_eq!(184, eight_aligned, "the eight-aligned group is {eight_aligned} B, not 184");
    assert_eq!(10, tail, "the tail group is {tail} B, not 10");
    assert_eq!(
        eight_aligned + tail.div_ceil(align) * align,
        size,
        "the two groups plus one rounding must reconstruct the width exactly, or the layout is \
         not what this test says it is"
    );

    // --- WIDTH OR ALIGNMENT, field by field, and the verdict is arithmetic. ---
    //
    // A field is a WIDTH candidate only if taking bytes off it takes bytes off the struct.
    // Inside the tail that is false until the tail itself drops past a multiple of the
    // alignment, and every field in the tail here is one byte or four.
    println!("\n=== which fields could move this structure, and which could not ===");
    for field in &fields {
        let verdict = if field.align < align {
            // In the rounding. Narrowing it cannot cross the boundary on its own.
            "ALIGNMENT -- inside a 10-in-16 rounding; narrowing it moves nothing"
        } else if field.size % align == 0 && field.size > align {
            "WIDTH -- a whole number of words, and every word of it is paid per bucket"
        } else {
            "WIDTH -- one word, and losing it would take a word off the struct"
        };
        println!("  {:<34} {}", field.name, verdict);
    }
    let tail_fields = fields.iter().filter(|field| field.align < align).count();
    assert_eq!(
        7,
        tail_fields,
        "seven fields sit in the tail rounding -- the routing bucket, the layout and the five \
         flags -- and this test found {tail_fields}"
    );
    let tail_bytes: usize = fields
        .iter()
        .filter(|field| field.align < align)
        .map(|field| field.size)
        .sum();
    assert!(
        tail_bytes.div_ceil(align) * align > tail_bytes,
        "the tail is {tail_bytes} B and fills its rounding exactly; the claim that narrowing a \
         tail field changes nothing is only true while it does not"
    );

    // --- The biggest single field, because an accounting that does not say so misleads. ---
    let page = fields
        .iter()
        .find(|field| field.name == "block_index")
        .expect("block_index is a field of BucketNode");
    assert!(
        page.size * 2 > size,
        "the inline page entry is {} of {size} bytes; if it is no longer more than half the \
         structure, the accounting above leads with the wrong field",
        page.size
    );
}

// -------------------------------------------------------------------------------------------
// THE THREE SHAPES CONSIDERED AND DECLINED, PRICED.
//
// Each is a MIRROR: a struct built from the same field types, so it is a statement about widths
// and not a guess. `mirror_is_faithful` is the control -- it fails if a mirror stops describing
// the declaration, which is the only way these numbers could quietly become fiction.
// -------------------------------------------------------------------------------------------

/// The node as it stands. If this is not `size_of::<BucketNode>()` every mirror below is fiction.
#[allow(dead_code)]
struct MirrorLive {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The node as it was, with the countdown spending a word on its discriminant.
#[allow(dead_code)]
struct MirrorWideTtl {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: Option<u64>,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The five flags folded into one byte.
#[allow(dead_code)]
struct MirrorPackedFlags {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: u8,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The two transient log claims moved out of the node into a side map of dirty buckets.
#[allow(dead_code)]
struct MirrorHoistedClaims {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: BlockIndexMap,
}

/// The inline page entry held behind a pointer instead.
#[allow(dead_code)]
enum MirrorBoxedBlockIndexMap {
    Empty,
    One(u64, Box<BlockIndex>),
    Many(std::collections::BTreeMap<u64, BlockIndex>),
}

#[allow(dead_code)]
struct MirrorBoxedPage {
    routing_bucket: u32,
    layout: BucketLayoutState,
    dirty: bool,
    deleted: bool,
    meta_loaded: bool,
    loading: bool,
    in_memory: bool,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    last_dump_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: ObjectIndex,
    block_index: MirrorBoxedBlockIndexMap,
}

/// WHAT EACH DECLINED SHAPE WOULD ACTUALLY BUY, IN BYTES PER BUCKET.
///
/// THE CONTROL COMES FIRST. `MirrorLive` is built from the same field types as the declaration
/// and has to agree with it exactly; if it does not, every other row here is describing a
/// structure this engine does not have, and the test says so rather than printing numbers.
///
/// THE ONE THAT WAS TAKEN. `MirrorWideTtl` is the shape before this change and `MirrorLive` is
/// the shape after: eight bytes, and they came off because the countdown stopped needing a word
/// for its discriminant. It is the only one of the four whose stored spelling does not move --
/// the countdown is one JSON key either way, and the serde impls unbias across it.
///
/// THE ONE THAT WAS DECLINED ON BLAST RADIUS. Folding the five flags into a byte is worth the
/// same eight bytes, and it is a different kind of change: `dirty`, `deleted`, `meta_loaded`,
/// `loading` and `in_memory` are five keys of the stored index and 137 read sites across the
/// engine, and holding the stored spelling still would mean a hand-written serializer for the
/// node. Eight bytes a bucket does not buy that here, and a mechanical rewrite of 137 sites is
/// how a guard that reads one of those names stops seeing anything.
///
/// THE ONE THAT WAS DECLINED ON WHERE THE COST WOULD GO. The two `#[serde(skip)]` log claims are
/// sixteen bytes and touch no stored shape at all, which makes them the cheapest bytes here to
/// reach -- but only if they move somewhere cheaper, and a side map keyed by bucket costs its own
/// key, its own pair and its own B-tree node for every bucket that is dirty. The measurement that
/// decides it is the dirty FRACTION, which the corpus probe prints; at the fraction this store
/// runs at it is not a win.
///
/// THE ONE THAT IS A LOSS, AND THE ONLY ONE WHOSE SIGN IS NOT OBVIOUS. Putting the inline page
/// entry behind a pointer takes the most off the struct of anything here -- and then pays it
/// back with interest, because almost every bucket holds exactly one page and would allocate for
/// it. The allocation is rounded up by the allocator, so the pair costs MORE than the inline form
/// it replaced, plus an indirection on every page read and an allocation on every bucket.
#[test]
fn what_each_declined_shape_of_the_bucket_node_would_cost() {
    // --- The control. Nothing below means anything without it. ---
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorLive>(),
        "the mirror of the live declaration is {} bytes against the declaration's {}; the mirrors \
         have drifted and every price below is fiction",
        size_of::<MirrorLive>(),
        size_of::<BucketNode>()
    );

    let live = size_of::<BucketNode>();
    let wide_ttl = size_of::<MirrorWideTtl>();
    let packed = size_of::<MirrorPackedFlags>();
    let hoisted = size_of::<MirrorHoistedClaims>();
    let boxed = size_of::<MirrorBoxedPage>();

    println!("\n=== what each shape makes the node ===");
    println!("  {:<44} {:>5} {:>9}", "shape", "bytes", "vs live");
    for (name, bytes) in [
        ("the node as it stands", live),
        ("with the countdown back at two words (before)", wide_ttl),
        ("with the five flags folded into one byte", packed),
        ("with the two transient log claims hoisted out", hoisted),
        ("with the inline page entry behind a pointer", boxed),
    ] {
        println!(
            "  {:<44} {:>5} {:>+9}",
            name,
            bytes,
            bytes as i64 - live as i64
        );
    }

    // --- The change this module documents. ---
    assert_eq!(
        208, wide_ttl,
        "the shape before this change was 208 bytes; it reads as {wide_ttl}, so the eight bytes \
         this change claims are not the eight bytes it took"
    );
    assert_eq!(200, live, "the node is {live} bytes, not 200");

    // --- The two declined savings, each a real eight and sixteen. ---
    assert_eq!(
        8,
        live - packed,
        "folding the flags is priced at eight bytes a bucket; it measured {}",
        live - packed
    );
    assert_eq!(
        16,
        live - hoisted,
        "hoisting the two transient claims is priced at sixteen bytes a bucket; it measured {}",
        live - hoisted
    );

    // --- The loss, and why the struct width alone would have read as a win. ---
    //
    // The pointer takes eighty bytes off the struct and then buys a 104-byte allocation for the
    // entry it moved out -- and it buys one for very nearly every bucket, because the page
    // entries and the buckets are within a tenth of a percent of each other in count. A 104-byte
    // request is served from a 112-byte chunk once the allocator has taken its header and rounded
    // to a class, so the pair is WIDER than the inline form it replaced, before counting the
    // allocation itself or the pointer chase on every page read.
    const ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY: usize = 112;
    assert!(
        boxed < live,
        "the boxed shape must make the STRUCT smaller, or the point of the row is lost"
    );
    let boxed_pair = boxed + ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY;
    println!(
        "\n  the boxed shape: {boxed} B of struct + {ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY} B of \
         allocation = {boxed_pair} B for the bucket that holds one page, against {live} B inline \
         -- {:+} B, one allocation and one indirection",
        boxed_pair as i64 - live as i64
    );
    assert!(
        boxed_pair > live,
        "the boxed shape costs {boxed_pair} B against {live} B inline; if that has become a win \
         the decline recorded here is stale and should be revisited"
    );
    assert!(
        size_of::<BlockIndex>() <= ALLOCATOR_CHUNK_FOR_A_PAGE_ENTRY,
        "the chunk size assumed for a page entry is smaller than the entry ({} B), which would \
         under-price the decline",
        size_of::<BlockIndex>()
    );
}

// -------------------------------------------------------------------------------------------
// THE STORED SHAPE, DRIVEN IN BOTH DIRECTIONS.
// -------------------------------------------------------------------------------------------

/// A bucket node with every field set to something distinguishable, for the wire tests.
fn wire_fixture(ttl_ms: Option<u64>) -> BucketNode {
    BucketNode {
        routing_bucket: 7,
        layout: BucketLayoutState::SingleBlockObject,
        dirty: false,
        deleted: false,
        meta_loaded: true,
        loading: false,
        in_memory: true,
        ttl_ms: BucketTtl::from_ms(ttl_ms),
        dirty_generation: 3,
        first_dirty_wal_sequence: 41,
        first_dirty_index_log_sequence: 42,
        last_dump_sequence: 11,
        object_index: [42u64].into_iter().collect(),
        deleted_object_index: ObjectIndex::default(),
        block_index: BlockIndexMap::default(),
    }
}

/// THE STORED SPELLING OF A BUCKET NODE DID NOT MOVE.
///
/// This is the constraint the whole subject runs into, and it is why the accounting above ends
/// where it does: `BucketNode` is written into the shard index, so its field names ARE a stored
/// format and a resident-layout change that alters one of them is not a resident-layout change.
///
/// The countdown is the one field this change touched, and it is held differently in memory and
/// written identically to disk. Both directions are DRIVEN rather than argued:
///
///   * NEW WRITE. The node serializes to the exact bytes below -- `ttl_ms` as a bare number or
///     `null`, in the same position, with no key added and none removed.
///   * OLD READ. The spellings an index written before this change can contain -- a number, a
///     zero, `null`, and the key absent entirely -- all load, and load to the value they meant.
///
/// THE ZERO IS THE CASE THAT MATTERS. A countdown of zero is a bucket whose expiry is due, and
/// the biased representation has to keep it distinct from absent; a version of this change that
/// read zero as absent passes every other assertion in this module.
#[test]
fn the_stored_spelling_of_a_bucket_node_did_not_move() {
    // --- NEW WRITE: the exact bytes. ---
    let with_ttl = serde_json::to_string(&wire_fixture(Some(5_000))).expect("a node serializes");
    assert_eq!(
        "{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\"deleted\":false,\
         \"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\"ttl_ms\":5000,\
         \"dirty_generation\":3,\"last_dump_sequence\":11,\"object_index\":[42],\
         \"deleted_object_index\":[],\"page_index\":{}}",
        with_ttl,
        "the stored spelling of a bucket node moved"
    );
    let without = serde_json::to_string(&wire_fixture(None)).expect("a node serializes");
    assert!(
        without.contains("\"ttl_ms\":null"),
        "an absent countdown must still be written as null, not omitted: {without}"
    );
    let at_zero = serde_json::to_string(&wire_fixture(Some(0))).expect("a node serializes");
    assert!(
        at_zero.contains("\"ttl_ms\":0"),
        "a countdown of zero must be written as 0, not as null and not as 1: {at_zero}"
    );

    // --- The two sides must not agree by accident. ---
    assert_ne!(
        without, at_zero,
        "absent and zero must not write the same bytes, or the wire has lost the distinction \
         this representation was built to keep"
    );

    // --- OLD READ: every spelling an index written before this change can hold. ---
    for (stored, expected) in [
        ("5000", Some(5_000u64)),
        ("0", Some(0)),
        ("null", None),
        ("18446744073709551615", Some(u64::MAX - 1)),
    ] {
        let json = format!(
            "{{\"routing_slot\":7,\"layout\":\"SingleBlockObject\",\"dirty\":false,\
             \"deleted\":false,\"meta_loaded\":true,\"loading\":false,\"in_memory\":true,\
             \"ttl_ms\":{stored},\"dirty_generation\":3,\"last_dump_sequence\":11,\
             \"object_ids\":[42],\"page_refs\":{{}}}}"
        );
        let node: BucketNode = serde_json::from_str(&json).expect("a stored node loads");
        assert_eq!(
            expected,
            node.ttl_ms.ms(),
            "a stored countdown of {stored} loaded as {:?}",
            node.ttl_ms.ms()
        );
    }

    // The key absent entirely: an index older than the field.
    let missing = "{\"routing_slot\":7,\"layout\":\"Empty\",\"dirty\":false,\"deleted\":false,\
                   \"meta_loaded\":true,\"loading\":false,\"in_memory\":false,\
                   \"dirty_generation\":0,\"last_dump_sequence\":0}";
    let node: BucketNode = serde_json::from_str(missing).expect("a node without the key loads");
    assert_eq!(None, node.ttl_ms.ms(), "a missing countdown key must load as absent");

    // --- ROUND TRIP, at the values that discriminate. ---
    for ms in [None, Some(0), Some(1), Some(5_000), Some(u64::MAX - 1)] {
        let node = wire_fixture(ms);
        let json = serde_json::to_string(&node).expect("a node serializes");
        let back: BucketNode = serde_json::from_str(&json).expect("a node loads");
        assert_eq!(ms, back.ttl_ms.ms(), "a countdown of {ms:?} did not round-trip");
    }
}

/// THE BIAS SATURATES AT THE TOP, AND NOWHERE ELSE.
///
/// A representation that gains a byte by giving up a value has to say which value, and has to
/// come back wrong in a direction that reads as wrong. Every countdown below `u64::MAX` survives
/// exactly; `u64::MAX` alone comes back one millisecond short, which is 584,542,046 years after
/// the deadline either way.
///
/// THE PROBE VALUES ARE CHOSEN SO TRUNCATION AND SATURATION DISAGREE. A wrapping bias would turn
/// `u64::MAX` into a countdown of zero -- an expiry that is due -- which is both plausible and
/// wrong, and is the mutant this test exists to kill.
#[test]
fn the_biased_countdown_saturates_at_the_top_and_is_exact_below_it() {
    for ms in [0u64, 1, 2, 5_000, 1 << 32, u64::MAX - 2, u64::MAX - 1] {
        assert_eq!(
            Some(ms),
            BucketTtl::from_ms(Some(ms)).ms(),
            "a countdown of {ms} must round-trip through the biased representation exactly"
        );
    }

    let top = BucketTtl::from_ms(Some(u64::MAX));
    assert_eq!(
        Some(u64::MAX - 1),
        top.ms(),
        "the one countdown the bias cannot hold must saturate one below the top"
    );
    assert_ne!(
        Some(0),
        top.ms(),
        "a countdown of u64::MAX came back as zero -- that is a wrapping bias, and zero means an \
         expiry that is due"
    );
    assert!(top.is_some(), "a saturated countdown is still a countdown, not an absence");

    // Absent and zero are two states, not one.
    assert_eq!(None, BucketTtl::ABSENT.ms());
    assert!(!BucketTtl::ABSENT.is_some());
    assert_eq!(Some(0), BucketTtl::from_ms(Some(0)).ms());
    assert!(
        BucketTtl::from_ms(Some(0)).is_some(),
        "a countdown of zero must report as present; a bucket whose expiry is due is exactly \
         what `ttl_bucket_count` is counting"
    );
    assert_ne!(
        BucketTtl::ABSENT,
        BucketTtl::from_ms(Some(0)),
        "absent and zero must not compare equal"
    );
    assert_eq!(BucketTtl::ABSENT, BucketTtl::default(), "a fresh node holds no countdown");
}

/// THE COUNTDOWN REACHES BOTH REPORTS, AND AN EMPTY EXPIRY TABLE CLEARS IT.
///
/// The representation is held by the tests above; this is its other half, and it exists because a
/// mutation run said it did not. Three production readers carry the countdown out of the node --
/// `refresh_bucket_runtime_flags`, which computes it, and the two reports that publish it -- and
/// a mutant in each of the three survived every test in the tree that names a bucket flag:
///
///   * `refresh_bucket_runtime_flags` leaving a countdown of ZERO where it should clear. Zero is
///     "this bucket's expiry is due", so this one does not read as a bug anywhere; it reads as a
///     store whose every bucket is about to expire.
///   * `bucket_store::runtime_report` publishing `ttl_ms: None` on every row. `ttl_bucket_count`
///     is computed from the node and not from the row, so the count stays right while every row
///     says the opposite.
///   * `storage_physical_index_report` doing the same.
///
/// `bucket_store_reports_all_layout_states_and_runtime_flags` builds its nodes by hand and never
/// runs a refresh; `bucket_runtime_flags_match_full_sweep` compares the targeted refresh against
/// a full sweep, so a mutation in the shared computation moves both sides and cancels. Neither
/// could have caught any of the three, which is why the value is asserted here and not the flag.
#[test]
fn the_countdown_reaches_both_reports_and_an_empty_expiry_table_clears_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = budget_engine(dir.path());
    for key in ["countdown-a", "countdown-b"] {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: key.to_string(),
                value: vec![b'v'; 16],
            },
        });
        assert!(response.status.ok, "the seed must ack: {:?}", response.status);
    }

    // --- NOTHING EXPIRES. Every bucket must report ABSENT, not a countdown of zero. ---
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        assert!(
            !shard.bucket_index.bucket_map.is_empty(),
            "denominator: no buckets, so every assertion below is vacuous"
        );
        for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
            assert_eq!(
                None,
                bucket.ttl_ms.ms(),
                "bucket {routing_bucket} holds nothing that expires and reports a countdown of \
                 {:?}; a countdown of zero here means every bucket in the store is due",
                bucket.ttl_ms.ms()
            );
            assert!(!bucket.ttl_ms.is_some(), "bucket {routing_bucket} reports a countdown it does not have");
        }
        let report = crate::engine::bucket_store::runtime_report(shard);
        assert_eq!(
            0, report.ttl_bucket_count,
            "no key has an expiry, so no bucket should be counted as holding one"
        );
        assert!(
            report.buckets.iter().all(|row| row.ttl_ms.is_none()),
            "a published row carries a countdown no bucket holds"
        );
    }

    // --- ONE KEY IS ARMED. The VALUE has to arrive, not just the flag. ---
    const ARMED_MS: u64 = 600_000;
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::CommonExpire {
            key: "countdown-a".to_string(),
            ttl_ms: ARMED_MS,
        },
    });
    assert!(response.status.ok, "arming must ack: {:?}", response.status);

    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let report = crate::engine::bucket_store::runtime_report(shard);
        assert_eq!(1, report.ttl_bucket_count, "exactly one bucket now holds an expiry");
        let armed: Vec<u64> = report.buckets.iter().filter_map(|row| row.ttl_ms).collect();
        assert_eq!(
            1,
            armed.len(),
            "exactly one published row must carry the countdown; {} did",
            armed.len()
        );
        // The BOUNDS are what make this a test of the value rather than of its presence: zero is
        // what a cleared countdown looks like, and ARMED_MS + 1 is what the stored bias looks
        // like if it ever escapes the accessor.
        assert!(
            armed[0] > 0 && armed[0] <= ARMED_MS,
            "the published countdown is {}; it must be a real number of milliseconds, neither \
             zero nor the biased value the node holds internally",
            armed[0]
        );
    }

    let physical = engine.storage_physical_index_report(1);
    let published: Vec<u64> = physical
        .bucket_nodes
        .iter()
        .filter_map(|bucket| bucket.ttl_ms)
        .collect();
    assert_eq!(
        1,
        published.len(),
        "the physical index report must publish the countdown too; {} of its {} bucket rows \
         carried one",
        published.len(),
        physical.bucket_nodes.len()
    );
    assert!(
        published[0] > 0 && published[0] <= ARMED_MS,
        "the physical report published a countdown of {}",
        published[0]
    );
}

// -------------------------------------------------------------------------------------------
// WHAT IT COSTS AT TWO LARGE CORPUS SIZES, AND WHAT AN INSTRUMENT THAT IS NOT `size_of` SEES.
// -------------------------------------------------------------------------------------------

/// The counting allocator, used here as the SECOND instrument.
///
/// `Clone` on a `BTreeMap` rebuilds the tree node for node, so the bytes charged across one
/// `clone()` are the map's own nodes -- the key array, the value array, the node header and every
/// slot a node has not filled. That number is measured by the allocator and owes nothing to
/// `size_of` x count, which is what makes the difference between them a reading rather than an
/// identity. A residual computed from one instrument twice cannot notice anything.
#[cfg(feature = "alloc-probe")]
fn clone_alloc_bytes<T: Clone>(value: &T) -> u64 {
    let probe = Probe::start();
    let copy = value.clone();
    let counts = probe.stop();
    std::hint::black_box(&copy);
    drop(copy);
    counts.alloc_bytes
}

/// THE PLANTED MARKER. Recovered exactly, or every residual below is noise.
///
/// The failure this guards against is the one that reads as good news: an instrument that reports
/// near zero makes a structure look free. A megabyte is planted and has to come back as a
/// megabyte -- not less, which would be blindness, and not much more, which would be the probe
/// charging for something other than the clone.
#[cfg(feature = "alloc-probe")]
#[test]
fn the_clone_instrument_recovers_a_planted_megabyte_exactly() {
    const PLANTED: usize = 1 << 20;
    let marker: Vec<u8> = vec![0xA5; PLANTED];
    let measured = clone_alloc_bytes(&marker);
    println!(
        "planted {PLANTED} B, instrument charged {measured} B ({:.4}x)",
        measured as f64 / PLANTED as f64
    );
    assert_eq!(
        PLANTED as u64, measured,
        "the clone instrument charged {measured} B for a planted {PLANTED} B; a residual taken \
         with it would be measuring the instrument"
    );
}

/// WHAT THE BUCKET NODE COSTS AT TWO LARGE CORPUS SIZES, IN TOTAL AND PER RECORD.
///
/// Two corpora three times apart, both large, because the question this module exists to answer
/// is what the structure costs a STORE and not what it costs one bucket. The per-record figure is
/// the one that multiplies out to any scale, and it is only usable if it is flat -- a per-record
/// cost that grows with the corpus is a finding, not a budget, and the flatness check below says
/// which of the two it is.
///
/// THE STORE PATH LENGTH IS HELD CONSTANT and asserted: `tempfile` names every directory with the
/// same number of characters, and allocation bytes move with the path at about six bytes per
/// character, so an arm whose path was one character longer would report a different residual for
/// a reason that has nothing to do with the structure.
///
/// THE DIRTY FRACTION IS PRINTED because it is what prices the one decline this module could not
/// settle from widths alone: the two `#[serde(skip)]` log claims are sixteen bytes that touch no
/// stored shape, and moving them into a side map keyed by bucket is a win exactly when few
/// buckets are dirty and a loss when most are.
///
/// THE RESIDUAL IS THE POINT OF THE SECOND INSTRUMENT. `size_of` x count is what the node
/// occupies; the map that holds the nodes charges more than that, and the difference -- key
/// arrays, node headers, unfilled slots and whatever each node owns on the heap -- is reported
/// rather than assumed away, so a change in it is visible.
#[test]
#[ignore = "seeds 80,000 then 240,000 records; run by name"]
fn what_the_bucket_node_costs_at_two_large_corpus_sizes() {
    let mut path_lengths: Vec<usize> = Vec::new();
    let mut per_record: Vec<(usize, f64)> = Vec::new();

    for (label, strings_n, series_keys, series_points) in [
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
        ("240,000 records", 120_000usize, 120usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        path_lengths.push(dir.path().as_os_str().len());
        let engine = budget_engine(dir.path());
        budget_seed(&engine, strings_n, series_keys, series_points);

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let counts = count_items(shard);

        assert_eq!(
            strings_n + series_keys * series_points,
            counts.records,
            "denominator: the shard must hold every record seeded"
        );
        assert!(
            counts.bucket_nodes > 0,
            "denominator: no routing buckets, so every figure below divides by nothing"
        );

        let width = size_of::<BucketNode>();
        let accounted = width * counts.bucket_nodes;
        // The width this structure carried before the countdown stopped spending a word on a
        // discriminant. A literal, because the shape it names no longer exists to be measured;
        // `what_each_declined_shape_of_the_bucket_node_would_cost` holds a mirror of it to 208
        // so this literal cannot drift away from what it claims.
        const WIDTH_BEFORE: usize = 208;
        let before = WIDTH_BEFORE * counts.bucket_nodes;

        let dirty_buckets = shard
            .bucket_index
            .bucket_map
            .values()
            .filter(|bucket| bucket.dirty)
            .count();

        println!("\n=== {label} ===");
        println!(
            "  records={} buckets={} page entries={}",
            counts.records, counts.bucket_nodes, counts.block_index_entries
        );
        println!(
            "  BucketNode: {width} B x {} = {accounted} B ({:.2} MiB), {:.2} B/record",
            counts.bucket_nodes,
            accounted as f64 / (1024.0 * 1024.0),
            accounted as f64 / counts.records as f64
        );
        println!(
            "  before this change: {WIDTH_BEFORE} B x {} = {before} B ({:.2} MiB), {:.2} B/record",
            counts.bucket_nodes,
            before as f64 / (1024.0 * 1024.0),
            before as f64 / counts.records as f64
        );
        println!(
            "  SAVED: {} B ({:.2} MiB), {:.2} B/record, {:.1}% of the structure",
            before - accounted,
            (before - accounted) as f64 / (1024.0 * 1024.0),
            (before - accounted) as f64 / counts.records as f64,
            100.0 * (before - accounted) as f64 / before as f64
        );
        println!(
            "  dirty buckets: {dirty_buckets} of {} ({:.1}%)",
            counts.bucket_nodes,
            100.0 * dirty_buckets as f64 / counts.bucket_nodes as f64
        );

        #[cfg(feature = "alloc-probe")]
        {
            let measured = clone_alloc_bytes(&shard.bucket_index.bucket_map);
            let residual = measured as i64 - accounted as i64;
            println!(
                "  the map that holds them, charged by the allocator: {measured} B ({:.2} MiB)",
                measured as f64 / (1024.0 * 1024.0)
            );
            println!(
                "  RESIDUAL outside the node widths: {residual} B, {:.2} B/bucket -- key arrays, \
                 node headers, unfilled slots and what each node owns on the heap",
                residual as f64 / counts.bucket_nodes as f64
            );
            assert!(
                residual > 0,
                "the allocator charged {measured} B for a map of {} nodes whose widths add up to \
                 {accounted} B; a residual at or below zero means the two instruments are not \
                 independent and the subtraction is an identity",
                counts.bucket_nodes
            );
        }

        per_record.push((
            counts.records,
            accounted as f64 / counts.records as f64,
        ));
    }

    assert_eq!(2, path_lengths.len(), "both arms must have run");
    assert_eq!(
        path_lengths[0], path_lengths[1],
        "the store path length moved between arms ({} then {}); allocation bytes move with it at \
         about six bytes a character",
        path_lengths[0], path_lengths[1]
    );

    let (small_records, small) = per_record[0];
    let (big_records, big) = per_record[1];
    assert!(
        big_records > small_records,
        "the second arm must be the larger corpus, or the flatness claim compares nothing"
    );
    let ratio = big / small;
    println!(
        "\n=== flatness: {small:.2} B/record at {small_records} -> {big:.2} B/record at \
         {big_records} ({ratio:.3}x) ==="
    );
    assert!(
        ratio < 1.25,
        "BucketNode costs {small:.2} B/record at {small_records} records and {big:.2} at \
         {big_records} ({ratio:.3}x) -- it is growing with the corpus, which is a finding and not \
         a budget"
    );
}
